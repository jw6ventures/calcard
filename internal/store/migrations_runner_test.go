package store

import (
	"context"
	"fmt"
	"regexp"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

func TestApplyMigrationsEmptyDatabase(t *testing.T) {
	tx := &mockTx{execs: []execExpectation{
		{expect: regexp.MustCompile("-- Initial schema for CalCard"), args: nil},
		{expect: regexp.MustCompile("INSERT INTO schema_migrations"), args: []any{"001_init.sql"}},
	}}

	pool := &mockPool{
		t: t,
		queries: []queryExpectation{
			{expect: regexp.MustCompile("schema_migrations"), value: false},
			{expect: regexp.MustCompile("COUNT\\(\\*\\) FROM information_schema.tables"), value: 0},
			{expect: regexp.MustCompile("schema_migrations WHERE version=\\$1"), args: []any{"001_init.sql"}, value: false},
		},
		execs: []execExpectation{
			{expect: regexp.MustCompile("CREATE TABLE IF NOT EXISTS schema_migrations"), args: nil},
		},
		tx: tx,
	}

	if err := ApplyMigrations(context.Background(), pool); err != nil {
		t.Fatalf("expected migrations to apply, got error: %v", err)
	}

	pool.assertDone()
	tx.assertDone()
}

func TestApplyMigrationsPopulatedWithoutTracking(t *testing.T) {
	pool := &mockPool{
		t: t,
		queries: []queryExpectation{
			{expect: regexp.MustCompile("schema_migrations"), value: false},
			{expect: regexp.MustCompile("COUNT\\(\\*\\) FROM information_schema.tables"), value: 3},
			{expect: regexp.MustCompile("schema_migrations WHERE version=\\$1"), args: []any{"001_init.sql"}, value: true},
		},
		execs: []execExpectation{
			{expect: regexp.MustCompile("CREATE TABLE IF NOT EXISTS schema_migrations"), args: nil},
			{expect: regexp.MustCompile("INSERT INTO schema_migrations"), args: []any{"001_init.sql"}},
		},
	}

	if err := ApplyMigrations(context.Background(), pool); err != nil {
		t.Fatalf("expected migrations to apply without replaying init, got error: %v", err)
	}

	pool.assertDone()
}

type queryExpectation struct {
	expect *regexp.Regexp
	args   []any
	value  any
	err    error
}

type execExpectation struct {
	expect *regexp.Regexp
	args   []any
	err    error
}

type mockPool struct {
	t       *testing.T
	queries []queryExpectation
	execs   []execExpectation
	tx      *mockTx
}

func (m *mockPool) QueryRow(ctx context.Context, sql string, args ...any) pgx.Row {
	if len(m.queries) == 0 {
		m.t.Fatalf("unexpected query: %s", sql)
	}
	exp := m.queries[0]
	m.queries = m.queries[1:]
	if !exp.expect.MatchString(sql) {
		m.t.Fatalf("query mismatch: %s", sql)
	}
	assertArgs(m.t, exp.args, args)
	return mockRow{value: exp.value, err: exp.err}
}

func (m *mockPool) Exec(ctx context.Context, sql string, arguments ...any) (pgconn.CommandTag, error) {
	if len(m.execs) == 0 {
		m.t.Fatalf("unexpected exec: %s", sql)
	}
	exp := m.execs[0]
	m.execs = m.execs[1:]
	if !exp.expect.MatchString(sql) {
		m.t.Fatalf("exec mismatch: %s", sql)
	}
	assertArgs(m.t, exp.args, arguments)
	return pgconn.NewCommandTag("MOCK"), exp.err
}

func (m *mockPool) BeginTx(ctx context.Context, txOptions pgx.TxOptions) (pgx.Tx, error) {
	if m.tx == nil {
		m.t.Fatalf("unexpected begin tx")
	}
	if m.tx.started {
		m.t.Fatalf("transaction already started")
	}
	m.tx.started = true
	return m.tx, nil
}

func (m *mockPool) assertDone() {
	if len(m.queries) != 0 {
		m.t.Fatalf("pending queries: %v", m.queries)
	}
	if len(m.execs) != 0 {
		m.t.Fatalf("pending execs: %v", m.execs)
	}
}

type mockRow struct {
	value any
	err   error
}

func (m mockRow) Scan(dest ...any) error {
	if m.err != nil {
		return m.err
	}
	if len(dest) != 1 {
		return fmt.Errorf("unexpected dest count: %d", len(dest))
	}
	switch v := m.value.(type) {
	case bool:
		ptr, ok := dest[0].(*bool)
		if !ok {
			return fmt.Errorf("expected *bool destination")
		}
		*ptr = v
	case int:
		ptr, ok := dest[0].(*int)
		if !ok {
			return fmt.Errorf("expected *int destination")
		}
		*ptr = v
	default:
		return fmt.Errorf("unsupported value type %T", v)
	}
	return nil
}

type mockTx struct {
	execs     []execExpectation
	started   bool
	committed bool
	rolled    bool
}

func (m *mockTx) Begin(ctx context.Context) (pgx.Tx, error) {
	return nil, fmt.Errorf("unexpected nested begin")
}
func (m *mockTx) Commit(ctx context.Context) error {
	m.committed = true
	return nil
}
func (m *mockTx) Rollback(ctx context.Context) error {
	m.rolled = true
	return nil
}
func (m *mockTx) CopyFrom(ctx context.Context, tableName pgx.Identifier, columnNames []string, rowSrc pgx.CopyFromSource) (int64, error) {
	return 0, fmt.Errorf("unexpected CopyFrom")
}
func (m *mockTx) SendBatch(ctx context.Context, b *pgx.Batch) pgx.BatchResults {
	return emptyBatchResults{}
}
func (m *mockTx) LargeObjects() pgx.LargeObjects { return pgx.LargeObjects{} }
func (m *mockTx) Prepare(ctx context.Context, name, sql string) (*pgconn.StatementDescription, error) {
	return nil, fmt.Errorf("unexpected Prepare")
}
func (m *mockTx) Exec(ctx context.Context, sql string, arguments ...any) (pgconn.CommandTag, error) {
	if len(m.execs) == 0 {
		return pgconn.CommandTag{}, fmt.Errorf("unexpected tx exec: %s", sql)
	}
	exp := m.execs[0]
	m.execs = m.execs[1:]
	if !exp.expect.MatchString(sql) {
		return pgconn.CommandTag{}, fmt.Errorf("exec mismatch: %s", sql)
	}
	if err := assertArgs(nil, exp.args, arguments); err != nil {
		return pgconn.CommandTag{}, err
	}
	return pgconn.NewCommandTag("MOCK"), exp.err
}
func (m *mockTx) Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error) {
	return nil, fmt.Errorf("unexpected query")
}
func (m *mockTx) QueryRow(ctx context.Context, sql string, args ...any) pgx.Row {
	return mockRow{err: fmt.Errorf("unexpected queryrow")}
}
func (m *mockTx) Conn() *pgx.Conn { return nil }

func (m *mockTx) assertDone() {
	if len(m.execs) != 0 {
		panic(fmt.Sprintf("pending tx execs: %v", m.execs))
	}
	if !m.committed && !m.rolled {
		panic("transaction not finished")
	}
}

func assertArgs(t *testing.T, expected, actual []any) error {
	if len(expected) == 0 {
		return nil
	}
	if len(expected) != len(actual) {
		if t != nil {
			t.Fatalf("argument length mismatch: expected %d got %d", len(expected), len(actual))
		}
		return fmt.Errorf("argument length mismatch")
	}
	for i, exp := range expected {
		if exp == nil {
			continue
		}
		if exp != actual[i] {
			if t != nil {
				t.Fatalf("argument mismatch at %d: expected %v got %v", i, exp, actual[i])
			}
			return fmt.Errorf("argument mismatch")
		}
	}
	return nil
}

type emptyBatchResults struct{}

func (emptyBatchResults) Exec() (pgconn.CommandTag, error) {
	return pgconn.CommandTag{}, fmt.Errorf("unexpected batch exec")
}
func (emptyBatchResults) Query() (pgx.Rows, error) { return nil, fmt.Errorf("unexpected batch query") }
func (emptyBatchResults) QueryRow() pgx.Row {
	return mockRow{err: fmt.Errorf("unexpected batch queryrow")}
}
func (emptyBatchResults) Close() error { return nil }
