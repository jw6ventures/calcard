package store

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
)

func TestUserRepoSetWeekStartRefusesNonWeekdays(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New() error = %v", err)
	}
	defer db.Close()

	repo := &userRepo{pool: db}
	for _, day := range []time.Weekday{-1, 7} {
		if err := repo.SetWeekStart(context.Background(), 1, day); err == nil {
			t.Errorf("SetWeekStart(%d) error = nil, want an error", day)
		}
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("a refused week start reached the database: %v", err)
	}
}

func TestUserWeekStartRoundTripsAndDefaultsToSunday(t *testing.T) {
	pool := newPostgresPool(t)
	repo := &userRepo{pool: pool}
	ctx := context.Background()

	user, err := repo.UpsertOAuthUser(ctx, "subject", "dana@example.com", "Dana Lee", "Dana")
	if err != nil {
		t.Fatalf("UpsertOAuthUser() error = %v", err)
	}
	if user.WeekStart != time.Sunday {
		t.Fatalf("new user week start = %v, want Sunday", user.WeekStart)
	}

	for day := time.Sunday; day <= time.Saturday; day++ {
		if err := repo.SetWeekStart(ctx, user.ID, day); err != nil {
			t.Fatalf("SetWeekStart(%v) error = %v", day, err)
		}
		got, err := repo.GetByID(ctx, user.ID)
		if err != nil {
			t.Fatalf("GetByID() error = %v", err)
		}
		if got.WeekStart != day {
			t.Fatalf("GetByID() week start = %v, want %v", got.WeekStart, day)
		}
	}

	// The OAuth upsert at each sign-in must not reset the choice.
	if _, err := repo.UpsertOAuthUser(ctx, "subject", "dana@example.com", "Dana Lee", "Dana"); err != nil {
		t.Fatalf("UpsertOAuthUser() error = %v", err)
	}
	if got, _ := repo.GetByEmail(ctx, "dana@example.com"); got == nil || got.WeekStart != time.Saturday {
		t.Fatalf("week start after sign-in = %#v, want Saturday", got)
	}

	if _, err := pool.Exec(`UPDATE users SET week_start = 7 WHERE id = $1`, user.ID); err == nil {
		t.Fatal("the schema stored week_start 7")
	}
}

// A v1.2.0 database gains week_start through the startup migration runner,
// with every existing user on Sunday, and the file can run again unchanged.
func TestV130MigrationAddsWeekStart(t *testing.T) {
	pool := newPostgresPool(t)
	if _, err := pool.Exec(`
ALTER TABLE users DROP COLUMN week_start;
UPDATE application SET value = 'v1.2.0' WHERE key = 'version';
INSERT INTO users (oauth_subject, primary_email) VALUES ('existing', 'existing@example.com');
`); err != nil {
		t.Fatalf("roll the schema back to v1.2.0: %v", err)
	}

	migrations := t.TempDir()
	contents, err := os.ReadFile(filepath.Join("..", "..", "migrations", "v1.3.0.sql"))
	if err != nil {
		t.Fatalf("read migration: %v", err)
	}
	if err := os.WriteFile(filepath.Join(migrations, "v1.3.0.sql"), contents, 0o600); err != nil {
		t.Fatal(err)
	}
	migrateWithRunner(t, pool, migrations, "v1.3.0")

	var weekStart int
	if err := pool.QueryRow(`SELECT week_start FROM users WHERE oauth_subject = 'existing'`).Scan(&weekStart); err != nil {
		t.Fatalf("read week_start: %v", err)
	}
	if weekStart != int(time.Sunday) {
		t.Fatalf("existing user week_start = %d, want 0", weekStart)
	}
	if _, err := pool.Exec(`UPDATE users SET week_start = 7`); err == nil {
		t.Fatal("the migrated schema stored week_start 7")
	}
	if _, err := pool.Exec(string(contents)); err != nil {
		t.Fatalf("migration is not idempotent: %v", err)
	}
	var version string
	if err := pool.QueryRow(`SELECT value FROM application WHERE key = 'version'`).Scan(&version); err != nil || version != "v1.3.0" {
		t.Fatalf("version = %q, %v; want v1.3.0", version, err)
	}
	var constraints int
	if err := pool.QueryRow(`SELECT count(*) FROM pg_constraint WHERE conrelid = 'users'::regclass AND conname = 'users_week_start_check'`).Scan(&constraints); err != nil || constraints != 1 {
		t.Fatalf("users_week_start_check constraints = %d, %v; want 1", constraints, err)
	}
}
