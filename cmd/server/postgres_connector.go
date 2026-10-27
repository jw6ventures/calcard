package main

import (
	"context"
	"database/sql/driver"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/lib/pq"
)

// lib/pq honors a context only by sending PostgreSQL a CancelRequest over a
// second connection: the statement itself stays blocked reading its own
// connection until the server answers, and the startup handshake does not
// watch the context at all. A server that is hung rather than down (SIGSTOPped,
// or a primary stalled mid-failover) still has its kernel accept connections
// and data, so neither ever returns while the hang lasts. postgresConnector
// cuts the connection's socket instead when the server does not answer.

// cancelGrace is how long a statement whose context has ended waits for the
// server to answer lib/pq's CancelRequest before its connection is cut. A
// server that is up answers within a round trip, and the statement then
// reports the outcome the server reached, which matters for a COMMIT.
const cancelGrace = time.Second

// cancelRequestTimeout bounds each CancelRequest connection. lib/pq reads it
// until the server closes it, which a hung server never does, and it applies
// no deadline of its own unless connect_timeout is set.
const cancelRequestTimeout = 10 * time.Second

type postgresConnector struct {
	cfg pq.Config
}

func (c postgresConnector) Driver() driver.Driver { return noticeRelayDriver{} }

// Connect opens one connection through a dialer of its own, so the dialer
// knows which socket a later CancelRequest is for.
func (c postgresConnector) Connect(ctx context.Context) (driver.Conn, error) {
	connector, err := pq.NewConnectorConfig(c.cfg)
	if err != nil {
		return nil, err
	}
	dialer := &interruptingDialer{}
	connector.Dialer(dialer)

	stop := context.AfterFunc(ctx, dialer.interruptConnecting)
	conn, err := pq.ConnectorWithNoticeHandler(connector, logPostgresNotice).Connect(ctx)
	if !stop() {
		if err == nil {
			_ = conn.Close()
		}
		return nil, ctx.Err()
	}
	if err != nil {
		return nil, err
	}
	dialer.markEstablished()
	return conn, nil
}

// interruptingDialer dials the sockets of one lib/pq connection. Every dial
// before the connection is established is an attempt at it; every dial after
// is a CancelRequest for the statement in progress on it.
type interruptingDialer struct {
	mu          sync.Mutex
	conn        *interruptibleConn
	interrupted bool
	established bool
}

func (d *interruptingDialer) Dial(network, address string) (net.Conn, error) {
	return d.DialContext(context.Background(), network, address)
}

func (d *interruptingDialer) DialTimeout(network, address string, timeout time.Duration) (net.Conn, error) {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	return d.DialContext(ctx, network, address)
}

func (d *interruptingDialer) DialContext(ctx context.Context, network, address string) (net.Conn, error) {
	d.mu.Lock()
	established, conn := d.established, d.conn
	d.mu.Unlock()
	if established {
		conn.interruptIfStalled(cancelGrace)
		return dialCancelRequest(ctx, network, address)
	}

	var dialer net.Dialer
	raw, err := dialer.DialContext(ctx, network, address)
	if err != nil {
		return nil, err
	}
	conn = &interruptibleConn{Conn: raw}
	d.mu.Lock()
	defer d.mu.Unlock()
	d.conn = conn
	if d.interrupted {
		conn.interrupt()
	}
	return conn, nil
}

func dialCancelRequest(ctx context.Context, network, address string) (net.Conn, error) {
	var dialer net.Dialer
	conn, err := dialer.DialContext(ctx, network, address)
	if err != nil {
		return nil, err
	}
	if err := conn.SetDeadline(time.Now().Add(cancelRequestTimeout)); err != nil {
		_ = conn.Close()
		return nil, err
	}
	return conn, nil
}

// interruptConnecting cuts the connection attempt in progress and any later
// one, since lib/pq bounds the startup handshake only by connect_timeout.
func (d *interruptingDialer) interruptConnecting() {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.interrupted = true
	if d.conn != nil {
		d.conn.interrupt()
	}
}

func (d *interruptingDialer) markEstablished() {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.established = true
}

// interruptibleConn is a socket that can be cut from another goroutine,
// releasing a read or write blocked on it. A cut is final: lib/pq clears its
// handshake deadline once startup completes, and that must not undo one.
type interruptibleConn struct {
	net.Conn

	// pending counts reads and writes in progress; progress counts those that
	// moved data.
	pending  atomic.Int32
	progress atomic.Uint64

	mu          sync.Mutex
	interrupted bool
}

func (c *interruptibleConn) Read(b []byte) (int, error) {
	c.pending.Add(1)
	n, err := c.Conn.Read(b)
	c.pending.Add(-1)
	if n > 0 {
		c.progress.Add(1)
	}
	return n, err
}

func (c *interruptibleConn) Write(b []byte) (int, error) {
	c.pending.Add(1)
	n, err := c.Conn.Write(b)
	c.pending.Add(-1)
	if n > 0 {
		c.progress.Add(1)
	}
	return n, err
}

func (c *interruptibleConn) SetDeadline(t time.Time) error {
	return c.setDeadline(c.Conn.SetDeadline, t)
}

func (c *interruptibleConn) SetReadDeadline(t time.Time) error {
	return c.setDeadline(c.Conn.SetReadDeadline, t)
}

func (c *interruptibleConn) SetWriteDeadline(t time.Time) error {
	return c.setDeadline(c.Conn.SetWriteDeadline, t)
}

func (c *interruptibleConn) setDeadline(set func(time.Time) error, t time.Time) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.interrupted {
		return nil
	}
	return set(t)
}

func (c *interruptibleConn) interrupt() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.interrupted = true
	_ = c.Conn.SetDeadline(time.Unix(1, 0))
}

// interruptIfStalled cuts the connection after grace if a read or write is
// still blocked on it and nothing has moved since. A server that answered the
// cancel has sent something; a connection with nothing in progress is idle,
// which happens when a prepared statement's cancel races its completion and
// lib/pq keeps the connection.
func (c *interruptibleConn) interruptIfStalled(grace time.Duration) {
	before := c.progress.Load()
	time.AfterFunc(grace, func() {
		if c.pending.Load() > 0 && c.progress.Load() == before {
			c.interrupt()
		}
	})
}
