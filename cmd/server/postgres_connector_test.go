package main

import (
	"context"
	"database/sql"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/lib/pq"
)

const (
	protocolVersion3  = 196608
	cancelRequestCode = 80877102
)

// hungPostgres accepts connections the way a SIGSTOPped PostgreSQL does: the
// kernel completes the TCP handshake and takes whatever is sent, and nothing
// ever answers. With answerStartup it first completes the startup handshake,
// so the hang is met by a statement on an established connection instead.
type hungPostgres struct {
	ln             net.Listener
	answerStartup  bool
	startups       atomic.Int32
	cancelRequests atomic.Int32

	mu    sync.Mutex
	conns []net.Conn
}

func startHungPostgres(t *testing.T, answerStartup bool) *hungPostgres {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	s := &hungPostgres{ln: ln, answerStartup: answerStartup}
	go s.serve()
	t.Cleanup(s.close)
	return s
}

func (s *hungPostgres) dsn() string {
	return fmt.Sprintf("postgres://u:p@%s/db?sslmode=disable", s.ln.Addr())
}

func (s *hungPostgres) serve() {
	for {
		conn, err := s.ln.Accept()
		if err != nil {
			return
		}
		s.mu.Lock()
		s.conns = append(s.conns, conn)
		s.mu.Unlock()
		go s.handle(conn)
	}
}

func (s *hungPostgres) handle(conn net.Conn) {
	var header [8]byte
	if _, err := io.ReadFull(conn, header[:]); err != nil {
		return
	}
	length := binary.BigEndian.Uint32(header[:4])
	if _, err := io.CopyN(io.Discard, conn, int64(length)-8); err != nil {
		return
	}
	switch binary.BigEndian.Uint32(header[4:]) {
	case cancelRequestCode:
		s.cancelRequests.Add(1)
	case protocolVersion3:
		s.startups.Add(1)
		if s.answerStartup {
			var reply []byte
			reply = appendMessage(reply, 'R', binary.BigEndian.AppendUint32(nil, 0))
			reply = appendMessage(reply, 'K', binary.BigEndian.AppendUint32(binary.BigEndian.AppendUint32(nil, 42), 7))
			reply = appendMessage(reply, 'Z', []byte{'I'})
			if _, err := conn.Write(reply); err != nil {
				return
			}
		}
	}
	_, _ = io.Copy(io.Discard, conn)
}

func appendMessage(b []byte, kind byte, body []byte) []byte {
	b = append(b, kind)
	b = binary.BigEndian.AppendUint32(b, uint32(len(body)+4))
	return append(b, body...)
}

func (s *hungPostgres) close() {
	_ = s.ln.Close()
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, conn := range s.conns {
		_ = conn.Close()
	}
}

// execWithin runs one statement with a context that ends after budget and
// fails the test if the statement is still blocked well past that.
func execWithin(t *testing.T, db *sql.DB, budget, limit time.Duration) (time.Duration, error) {
	t.Helper()
	type result struct {
		elapsed time.Duration
		err     error
	}
	done := make(chan result, 1)
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), budget)
		defer cancel()
		start := time.Now()
		_, err := db.ExecContext(ctx, "SELECT 1")
		done <- result{time.Since(start), err}
	}()
	select {
	case r := <-done:
		return r.elapsed, r.err
	case <-time.After(limit):
		t.Fatalf("statement with a %s context still blocked after %s", budget, limit)
		return 0, nil
	}
}

func TestPostgresConnectorAbandonsStartupWhenTheContextEnds(t *testing.T) {
	server := startHungPostgres(t, false)
	db, err := sql.Open(postgresDriverName, server.dsn())
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()

	elapsed, err := execWithin(t, db, 200*time.Millisecond, 5*time.Second)
	if err == nil {
		t.Fatal("statement against a server that never answered startup succeeded")
	}
	if elapsed > time.Second {
		t.Fatalf("startup was abandoned after %s, want close to the 200ms context", elapsed)
	}
}

func TestPostgresConnectorReleasesAStatementTheServerNeverAnswers(t *testing.T) {
	server := startHungPostgres(t, true)
	db, err := sql.Open(postgresDriverName, server.dsn())
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()

	elapsed, err := execWithin(t, db, 200*time.Millisecond, 5*time.Second)
	if err == nil {
		t.Fatal("statement the server never answered succeeded")
	}
	if want := 200*time.Millisecond + cancelGrace + 500*time.Millisecond; elapsed > want {
		t.Fatalf("statement was released after %s, want within %s", elapsed, want)
	}
	if got := server.cancelRequests.Load(); got != 1 {
		t.Fatalf("server received %d CancelRequests, want the 1 lib/pq sends", got)
	}
	if got := server.startups.Load(); got != 1 {
		t.Fatalf("server saw %d connections start up, want 1: the released statement must not be retried", got)
	}
}

func TestInterruptibleConnCutsOnlyAStalledExchange(t *testing.T) {
	const grace = 20 * time.Millisecond

	waitPending := func(t *testing.T, c *interruptibleConn) {
		t.Helper()
		deadline := time.Now().Add(time.Second)
		for c.pending.Load() == 0 {
			if time.Now().After(deadline) {
				t.Fatal("read never started")
			}
			time.Sleep(time.Millisecond)
		}
	}
	readAsync := func(c *interruptibleConn) <-chan error {
		done := make(chan error, 1)
		go func() {
			_, err := c.Read(make([]byte, 1))
			done <- err
		}()
		return done
	}

	t.Run("a read the server never answers is cut", func(t *testing.T) {
		client, server := net.Pipe()
		defer server.Close()
		c := &interruptibleConn{Conn: client}
		read := readAsync(c)
		waitPending(t, c)
		c.interruptIfStalled(grace)
		select {
		case err := <-read:
			if !errors.Is(err, os.ErrDeadlineExceeded) {
				t.Fatalf("read error = %v, want a deadline error", err)
			}
		case <-time.After(time.Second):
			t.Fatal("stalled read was not cut")
		}
		if err := c.SetDeadline(time.Time{}); err != nil {
			t.Fatalf("SetDeadline: %v", err)
		}
		if _, err := c.Read(make([]byte, 1)); !errors.Is(err, os.ErrDeadlineExceeded) {
			t.Fatalf("read after clearing the deadline = %v, want the cut to stand", err)
		}
	})

	t.Run("an answer within the grace keeps the connection", func(t *testing.T) {
		client, server := net.Pipe()
		defer server.Close()
		c := &interruptibleConn{Conn: client}
		read := readAsync(c)
		waitPending(t, c)
		c.interruptIfStalled(grace)
		if _, err := server.Write([]byte{1}); err != nil {
			t.Fatalf("write: %v", err)
		}
		if err := <-read; err != nil {
			t.Fatalf("answered read: %v", err)
		}
		next := readAsync(c)
		waitPending(t, c)
		time.Sleep(5 * grace)
		if _, err := server.Write([]byte{2}); err != nil {
			t.Fatalf("write: %v", err)
		}
		if err := <-next; err != nil {
			t.Fatalf("read after an answered cancel = %v, want the connection kept", err)
		}
	})

	t.Run("an idle connection is kept", func(t *testing.T) {
		client, server := net.Pipe()
		defer server.Close()
		c := &interruptibleConn{Conn: client}
		c.interruptIfStalled(grace)
		time.Sleep(5 * grace)
		read := readAsync(c)
		if _, err := server.Write([]byte{1}); err != nil {
			t.Fatalf("write: %v", err)
		}
		if err := <-read; err != nil {
			t.Fatalf("read on an idle connection = %v, want it kept", err)
		}
	})
}

// A server that is up answers lib/pq's CancelRequest within the grace, so a
// statement whose context ends reports the server's own outcome, and the
// connection is not cut out from under it.
func TestPostgresConnectorKeepsLibPQCancellationOnAHealthyServer(t *testing.T) {
	dsn := os.Getenv("CALCARD_TEST_POSTGRES_DSN")
	if dsn == "" {
		t.Skip("CALCARD_TEST_POSTGRES_DSN is unset; set it to run the PostgreSQL cancellation test")
	}
	db, err := sql.Open(postgresDriverName, dsn)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()
	start := time.Now()
	_, err = db.ExecContext(ctx, "SELECT pg_sleep(5)")
	elapsed := time.Since(start)
	var pqErr *pq.Error
	if !errors.As(err, &pqErr) || pqErr.Code != "57014" {
		t.Fatalf("canceled statement error = %v, want PostgreSQL's query_canceled", err)
	}
	if elapsed > cancelGrace {
		t.Fatalf("canceled statement took %s, want the server's answer well within the %s grace", elapsed, cancelGrace)
	}
	if _, err := db.ExecContext(context.Background(), "SELECT 1"); err != nil {
		t.Fatalf("statement after a cancellation: %v", err)
	}
}
