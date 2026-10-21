package store

import (
	"database/sql"
	"os"
	"testing"

	"github.com/lib/pq"
)

// The upgrade helpers, for the tests in package store_test: those also call
// packages that import this one, such as the DAV validator.
var (
	NewV119ReleasePool = newV119ReleasePool
	MigrateWithRunner  = migrateWithRunner
	ApplyV120Migration = applyV120Migration
)

// OpenWithNotices opens a second pool on pool's schema whose connections hand
// handler every notice and warning the server sends them, which database/sql
// otherwise drops.
func OpenWithNotices(t *testing.T, pool *sql.DB, handler func(*pq.Error)) *sql.DB {
	t.Helper()
	var schema string
	if err := pool.QueryRow(`SELECT current_schema()`).Scan(&schema); err != nil {
		t.Fatalf("read schema: %v", err)
	}
	connector, err := pq.NewConnector(withSearchPath(t, os.Getenv(postgresDSNEnv), schema))
	if err != nil {
		t.Fatalf("open %s: %v", postgresDSNEnv, err)
	}
	noticed := sql.OpenDB(pq.ConnectorWithNoticeHandler(connector, handler))
	t.Cleanup(func() { noticed.Close() })
	return noticed
}
