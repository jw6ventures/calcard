package store

import (
	"context"
	"fmt"
	"io/fs"
	"sort"
	"strings"

	"github.com/example/calcard/internal/migrations"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

// PgxPool represents the subset of pgxpool.Pool used by migration helpers.
//
// This allows tests to supply a lightweight mock implementation without
// changing the public interface of the store package.
type PgxPool interface {
	Exec(ctx context.Context, sql string, arguments ...any) (pgconn.CommandTag, error)
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
	BeginTx(ctx context.Context, txOptions pgx.TxOptions) (pgx.Tx, error)
}

// ApplyMigrations ensures all embedded SQL migrations have been applied. If
// the database is empty, the initial migration (and any subsequent ones) are
// executed. If the database is already populated but lacks migration tracking,
// we assume the first migration is present and only apply newer migrations.
func ApplyMigrations(ctx context.Context, pool PgxPool) error {
	migrationNames, err := listMigrationFiles()
	if err != nil {
		return err
	}
	if len(migrationNames) == 0 {
		return nil
	}

	hasTable, err := migrationTableExists(ctx, pool)
	if err != nil {
		return err
	}

	if !hasTable {
		empty, err := databaseIsEmpty(ctx, pool)
		if err != nil {
			return err
		}

		if err := ensureMigrationTable(ctx, pool); err != nil {
			return err
		}

		if !empty {
			// The database already has objects, so treat the first migration as
			// already applied to avoid replaying schema creation statements.
			if err := recordMigration(ctx, pool, migrationNames[0]); err != nil {
				return err
			}
		}
	}

	for _, name := range migrationNames {
		applied, err := migrationApplied(ctx, pool, name)
		if err != nil {
			return err
		}
		if applied {
			continue
		}

		if err := applyMigration(ctx, pool, name); err != nil {
			return err
		}
	}

	return nil
}

func listMigrationFiles() ([]string, error) {
	entries, err := fs.ReadDir(migrations.Files, ".")
	if err != nil {
		return nil, fmt.Errorf("list migrations: %w", err)
	}

	var names []string
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		if !strings.HasSuffix(entry.Name(), ".sql") {
			continue
		}
		names = append(names, entry.Name())
	}
	sort.Strings(names)
	return names, nil
}

func migrationTableExists(ctx context.Context, pool PgxPool) (bool, error) {
	const q = `SELECT EXISTS (
        SELECT 1 FROM information_schema.tables
        WHERE table_schema='public' AND table_name='schema_migrations'
)`
	var exists bool
	if err := pool.QueryRow(ctx, q).Scan(&exists); err != nil {
		return false, fmt.Errorf("check migration table: %w", err)
	}
	return exists, nil
}

func databaseIsEmpty(ctx context.Context, pool PgxPool) (bool, error) {
	const q = `SELECT COUNT(*) FROM information_schema.tables
WHERE table_schema NOT IN ('pg_catalog', 'information_schema')`
	var count int
	if err := pool.QueryRow(ctx, q).Scan(&count); err != nil {
		return false, fmt.Errorf("count tables: %w", err)
	}
	return count == 0, nil
}

func ensureMigrationTable(ctx context.Context, pool PgxPool) error {
	const q = `CREATE TABLE IF NOT EXISTS schema_migrations (
        version TEXT PRIMARY KEY,
        applied_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
)`
	if _, err := pool.Exec(ctx, q); err != nil {
		return fmt.Errorf("create schema_migrations: %w", err)
	}
	return nil
}

func migrationApplied(ctx context.Context, pool PgxPool, name string) (bool, error) {
	const q = `SELECT EXISTS (SELECT 1 FROM schema_migrations WHERE version=$1)`
	var exists bool
	if err := pool.QueryRow(ctx, q, name).Scan(&exists); err != nil {
		return false, fmt.Errorf("check migration %s: %w", name, err)
	}
	return exists, nil
}

func applyMigration(ctx context.Context, pool PgxPool, name string) error {
	contents, err := migrations.Files.ReadFile(name)
	if err != nil {
		return fmt.Errorf("read migration %s: %w", name, err)
	}

	tx, err := pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return fmt.Errorf("begin migration %s: %w", name, err)
	}
	if _, err := tx.Exec(ctx, string(contents)); err != nil {
		_ = tx.Rollback(ctx)
		return fmt.Errorf("apply migration %s: %w", name, err)
	}
	if err := recordMigrationInTx(ctx, tx, name); err != nil {
		_ = tx.Rollback(ctx)
		return err
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit migration %s: %w", name, err)
	}
	return nil
}

func recordMigration(ctx context.Context, pool PgxPool, name string) error {
	const q = `INSERT INTO schema_migrations (version) VALUES ($1) ON CONFLICT (version) DO NOTHING`
	if _, err := pool.Exec(ctx, q, name); err != nil {
		return fmt.Errorf("record migration %s: %w", name, err)
	}
	return nil
}

func recordMigrationInTx(ctx context.Context, tx pgx.Tx, name string) error {
	const q = `INSERT INTO schema_migrations (version) VALUES ($1) ON CONFLICT (version) DO NOTHING`
	if _, err := tx.Exec(ctx, q, name); err != nil {
		return fmt.Errorf("record migration %s: %w", name, err)
	}
	return nil
}
