package store

import (
	"context"
	"database/sql"
	"os"
	"testing"
	"time"
)

// After a PostgreSQL restart every idle connection in the pool is dead. A
// readiness probe that fails until it has discarded each of them in turn
// holds an idle replica out of service for MaxIdleConns probe periods.
func TestHealthCheckSucceedsOnceAfterTheServerDropsEveryIdleConnection(t *testing.T) {
	const idleConns = 10
	pool := newPostgresPool(t)
	pool.SetMaxIdleConns(idleConns)
	ctx := context.Background()

	held := make([]*sql.Conn, 0, idleConns)
	for range idleConns {
		conn, err := pool.Conn(ctx)
		if err != nil {
			t.Fatalf("open connection: %v", err)
		}
		held = append(held, conn)
	}
	for _, conn := range held {
		if err := conn.Close(); err != nil {
			t.Fatalf("return connection to the pool: %v", err)
		}
	}
	if idle := pool.Stats().Idle; idle != idleConns {
		t.Fatalf("idle connections = %d, want %d", idle, idleConns)
	}

	var applicationName string
	if err := pool.QueryRowContext(ctx, `SELECT current_setting('application_name')`).Scan(&applicationName); err != nil {
		t.Fatalf("read application_name: %v", err)
	}
	admin, err := sql.Open("postgres", os.Getenv(postgresDSNEnv))
	if err != nil {
		t.Fatalf("open admin connection: %v", err)
	}
	defer admin.Close()
	if _, err := admin.ExecContext(ctx, `SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE application_name = $1`, applicationName); err != nil {
		t.Fatalf("terminate backends: %v", err)
	}
	deadline := time.Now().Add(10 * time.Second)
	for {
		var remaining int
		if err := admin.QueryRowContext(ctx, `SELECT count(*) FROM pg_stat_activity WHERE application_name = $1`, applicationName).Scan(&remaining); err != nil {
			t.Fatalf("count backends: %v", err)
		}
		if remaining == 0 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("%d backends still running after pg_terminate_backend", remaining)
		}
		time.Sleep(20 * time.Millisecond)
	}

	if err := New(pool).HealthCheck(ctx); err != nil {
		t.Fatalf("HealthCheck() after the server dropped the pool's connections = %v, want nil", err)
	}
}
