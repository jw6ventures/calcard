package dav

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"fmt"
	"sort"
	"strings"
	"sync"
	"testing"

	"github.com/jw6ventures/calcard/internal/store"
	"github.com/lib/pq"
)

// davQueryCounter counts the statements a store sends to PostgreSQL, keyed by
// their first line, so a test can pin how many round trips a request costs.
// Transaction control goes through BeginTx, Commit and Rollback, which it does
// not count.
type davQueryCounter struct {
	mu      sync.Mutex
	total   int
	byQuery map[string]int
}

func (c *davQueryCounter) record(query string) {
	key := strings.TrimSpace(query)
	if i := strings.IndexByte(key, '\n'); i >= 0 {
		key = key[:i]
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	c.total++
	if c.byQuery == nil {
		c.byQuery = make(map[string]int)
	}
	c.byQuery[key]++
}

func (c *davQueryCounter) reset() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.total = 0
	c.byQuery = nil
}

func (c *davQueryCounter) count() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.total
}

// summary lists the counted statements, most frequent first.
func (c *davQueryCounter) summary() string {
	c.mu.Lock()
	defer c.mu.Unlock()
	type row struct {
		query string
		n     int
	}
	rows := make([]row, 0, len(c.byQuery))
	for query, n := range c.byQuery {
		rows = append(rows, row{query, n})
	}
	sort.Slice(rows, func(i, j int) bool {
		if rows[i].n != rows[j].n {
			return rows[i].n > rows[j].n
		}
		return rows[i].query < rows[j].query
	})
	var b strings.Builder
	for _, r := range rows {
		fmt.Fprintf(&b, "  %5d  %s\n", r.n, r.query)
	}
	return b.String()
}

// newDAVPostgresStoreCountingQueries is newDAVPostgresStore over a pool whose
// connections count every statement they send.
func newDAVPostgresStoreCountingQueries(t *testing.T) (*store.Store, *davQueryCounter) {
	t.Helper()
	counter := &davQueryCounter{}
	database := openDAVPostgresStore(t, func(dsn string) (*sql.DB, error) {
		connector, err := pq.NewConnector(dsn)
		if err != nil {
			return nil, err
		}
		return sql.OpenDB(countingConnector{base: connector, counter: counter}), nil
	})
	counter.reset()
	return database, counter
}

type countingConnector struct {
	base    driver.Connector
	counter *davQueryCounter
}

func (c countingConnector) Connect(ctx context.Context) (driver.Conn, error) {
	conn, err := c.base.Connect(ctx)
	if err != nil {
		return nil, err
	}
	return &countingConn{Conn: conn, counter: c.counter}, nil
}

func (c countingConnector) Driver() driver.Driver { return c.base.Driver() }

type countingConn struct {
	driver.Conn
	counter *davQueryCounter
}

func (c *countingConn) QueryContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Rows, error) {
	c.counter.record(query)
	return c.Conn.(driver.QueryerContext).QueryContext(ctx, query, args)
}

func (c *countingConn) ExecContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Result, error) {
	c.counter.record(query)
	return c.Conn.(driver.ExecerContext).ExecContext(ctx, query, args)
}

func (c *countingConn) PrepareContext(ctx context.Context, query string) (driver.Stmt, error) {
	stmt, err := c.Conn.(driver.ConnPrepareContext).PrepareContext(ctx, query)
	if err != nil {
		return nil, err
	}
	return &countingStmt{Stmt: stmt, query: query, counter: c.counter}, nil
}

func (c *countingConn) BeginTx(ctx context.Context, opts driver.TxOptions) (driver.Tx, error) {
	return c.Conn.(driver.ConnBeginTx).BeginTx(ctx, opts)
}

func (c *countingConn) CheckNamedValue(nv *driver.NamedValue) error {
	return c.Conn.(driver.NamedValueChecker).CheckNamedValue(nv)
}

func (c *countingConn) ResetSession(ctx context.Context) error {
	return c.Conn.(driver.SessionResetter).ResetSession(ctx)
}

func (c *countingConn) IsValid() bool {
	return c.Conn.(driver.Validator).IsValid()
}

func (c *countingConn) Ping(ctx context.Context) error {
	return c.Conn.(driver.Pinger).Ping(ctx)
}

type countingStmt struct {
	driver.Stmt
	query   string
	counter *davQueryCounter
}

func (s *countingStmt) ExecContext(ctx context.Context, args []driver.NamedValue) (driver.Result, error) {
	s.counter.record(s.query)
	return s.Stmt.(driver.StmtExecContext).ExecContext(ctx, args)
}

func (s *countingStmt) QueryContext(ctx context.Context, args []driver.NamedValue) (driver.Rows, error) {
	s.counter.record(s.query)
	return s.Stmt.(driver.StmtQueryContext).QueryContext(ctx, args)
}
