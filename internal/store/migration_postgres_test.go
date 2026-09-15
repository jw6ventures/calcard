package store

import (
	"context"
	"database/sql"
	"math"
	"os"
	"path/filepath"
	"testing"
	"time"
)

// applyV120Migration runs the upgrade the way MigrateDatabase
// (github.com/jw6ventures/jw6-go-utils/database/migration) actually runs this
// file. hasExplicitTransactionStatements strips dollar-quoted bodies before
// scanning for BEGIN/COMMIT/ROLLBACK, so the BEGIN inside this file's
// DO $$ ... END $$ block does not count, and the runner takes the branch that
// wraps the file exec and its own version-row update in one explicit
// transaction, rather than the implicit transaction a bare multi-statement
// exec would use.
func applyV120Migration(t *testing.T, pool *sql.DB) {
	t.Helper()
	migration, err := os.ReadFile(filepath.Join("..", "..", "migrations", "v1.2.0.sql"))
	if err != nil {
		t.Fatalf("read migration: %v", err)
	}

	tx, err := pool.Begin()
	if err != nil {
		t.Fatalf("begin migration transaction: %v", err)
	}
	if _, err := tx.Exec(string(migration)); err != nil {
		tx.Rollback()
		t.Fatalf("apply migration: %v", err)
	}
	if _, err := tx.Exec(`UPDATE application SET value = $1 WHERE key = 'version'`, "1.2.0"); err != nil {
		tx.Rollback()
		t.Fatalf("update database version: %v", err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatalf("commit migration transaction: %v", err)
	}
}

// newV119Pool builds the database an installation running v1.1.9 actually has:
// the baseline that release shipped, with none of the objects later versions add.
// Seeding from the current db.sql instead would hand the migration every column
// it needs for reasons the upgrade path does not supply.
func newV119Pool(t *testing.T) *sql.DB {
	t.Helper()
	return newPostgresSchemaPool(t, filepath.Join("testdata", "schema_v1.1.9.sql"))
}

// Before ace_order existed a denial suppressed every grant on the resource, no
// matter which was written first. Ordered evaluation answers with the first
// matching ACE instead, so the upgrade decides what the stored rows mean: order
// them by age alone and a DAV:all grant written before a user-specific denial
// starts answering first, turning a principal the installation had denied into
// one it admits.
func TestPostgres_V120MigrationPreservesExistingDenials(t *testing.T) {
	pool := newV119Pool(t)

	const (
		resourcePath = "/dav/calendars/1"
		principal    = "/dav/principals/2/"
	)
	if _, err := pool.Exec(`
INSERT INTO acl_entries (resource_path, principal_href, is_grant, privilege, created_at) VALUES
    ($1, 'DAV:all', TRUE,  'read', NOW() - INTERVAL '2 days'),
    ($1, $2,        FALSE, 'read', NOW() - INTERVAL '1 day')`, resourcePath, principal); err != nil {
		t.Fatalf("seed legacy ACL: %v", err)
	}

	applyV120Migration(t, pool)

	// The decision the collection predicates make, spelled as they spell it:
	// the whole principal list this user matches, first ACE wins. HasPrivilege
	// cannot stand in for it -- it narrows to a single principal_href, so it
	// never sees the DAV:all grant that competes with the user's denial.
	const decision = `
SELECT a.is_grant
FROM acl_entries a
WHERE a.resource_path = $1
  AND a.principal_href IN ('DAV:all', 'DAV:authenticated', $2)
  AND a.privilege IN ('read', 'all')
ORDER BY a.ace_order, a.id
LIMIT 1`
	var allowed bool
	if err := pool.QueryRow(decision, resourcePath, principal).Scan(&allowed); err != nil {
		t.Fatalf("ordered ACL decision: %v", err)
	}
	if allowed {
		t.Fatal("the upgrade turned an existing denial into a grant")
	}
}

// The object-level predicates join on resource_path_norm, so an ACE stored with
// the .ics suffix and one stored without it are one ordered group at evaluation
// time. The backfill has to order them against each other for the same reason.
func TestPostgres_V120MigrationPreservesDenialsAcrossPathSpellings(t *testing.T) {
	pool := newV119Pool(t)

	const principal = "/dav/principals/2/"
	if _, err := pool.Exec(`
INSERT INTO acl_entries (resource_path, principal_href, is_grant, privilege, created_at) VALUES
    ('/dav/calendars/1/event',     'DAV:all', TRUE,  'read', NOW() - INTERVAL '2 days'),
    ('/dav/calendars/1/event.ics', $1,        FALSE, 'read', NOW() - INTERVAL '1 day')`, principal); err != nil {
		t.Fatalf("seed legacy ACL: %v", err)
	}

	applyV120Migration(t, pool)

	var denyOrder, grantOrder int
	if err := pool.QueryRow(
		`SELECT ace_order FROM acl_entries WHERE is_grant = FALSE`).Scan(&denyOrder); err != nil {
		t.Fatalf("read deny order: %v", err)
	}
	if err := pool.QueryRow(
		`SELECT ace_order FROM acl_entries WHERE is_grant = TRUE`).Scan(&grantOrder); err != nil {
		t.Fatalf("read grant order: %v", err)
	}
	if denyOrder >= grantOrder {
		t.Fatalf("deny ace_order = %d, grant ace_order = %d: the denial no longer answers first", denyOrder, grantOrder)
	}
}

// A prerelease installation already carries ace_order values an ACL request
// established. Reordering those would discard an ordering a client chose, so
// the backfill has to recognise that it has already run.
func TestPostgres_V120MigrationKeepsEstablishedACLOrdering(t *testing.T) {
	pool := newPostgresPool(t)

	const resourcePath = "/dav/calendars/1"
	if _, err := pool.Exec(`
INSERT INTO acl_entries (resource_path, principal_href, is_grant, privilege, ace_order, created_at) VALUES
    ($1, 'DAV:all',              TRUE,  'read', 1, NOW() - INTERVAL '2 days'),
    ($1, '/dav/principals/2/',   FALSE, 'read', 0, NOW() - INTERVAL '1 day')`, resourcePath); err != nil {
		t.Fatalf("seed ordered ACL: %v", err)
	}

	applyV120Migration(t, pool)

	rows, err := pool.Query(`SELECT principal_href, ace_order FROM acl_entries ORDER BY ace_order`)
	if err != nil {
		t.Fatalf("read ACL: %v", err)
	}
	defer rows.Close()
	got := map[string]int{}
	for rows.Next() {
		var principal string
		var order int
		if err := rows.Scan(&principal, &order); err != nil {
			t.Fatalf("scan ACL: %v", err)
		}
		got[principal] = order
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("read ACL: %v", err)
	}
	if got["DAV:all"] != 1 || got["/dav/principals/2/"] != 0 {
		t.Fatalf("ace_order = %v, want the established ordering preserved", got)
	}
}

// The upgrade has to be repeatable: the runner execs the whole file again if a
// run failed part way, and an operator may run it by hand.
func TestPostgres_V120MigrationIsRepeatable(t *testing.T) {
	pool := newV119Pool(t)

	const resourcePath = "/dav/calendars/1"
	if _, err := pool.Exec(`
INSERT INTO acl_entries (resource_path, principal_href, is_grant, privilege, created_at) VALUES
    ($1, 'DAV:all',            TRUE,  'read', NOW() - INTERVAL '2 days'),
    ($1, '/dav/principals/2/', FALSE, 'read', NOW() - INTERVAL '1 day')`, resourcePath); err != nil {
		t.Fatalf("seed legacy ACL: %v", err)
	}

	applyV120Migration(t, pool)
	var afterFirst []int
	readOrders := func() []int {
		rows, err := pool.Query(`SELECT ace_order FROM acl_entries ORDER BY id`)
		if err != nil {
			t.Fatalf("read ACL: %v", err)
		}
		defer rows.Close()
		var orders []int
		for rows.Next() {
			var order int
			if err := rows.Scan(&order); err != nil {
				t.Fatalf("scan ACL: %v", err)
			}
			orders = append(orders, order)
		}
		if err := rows.Err(); err != nil {
			t.Fatalf("read ACL: %v", err)
		}
		return orders
	}
	afterFirst = readOrders()

	applyV120Migration(t, pool)

	afterSecond := readOrders()
	if len(afterFirst) != len(afterSecond) {
		t.Fatalf("row count changed: %d then %d", len(afterFirst), len(afterSecond))
	}
	for i := range afterFirst {
		if afterFirst[i] != afterSecond[i] {
			t.Fatalf("ace_order = %v after one run and %v after two", afterFirst, afterSecond)
		}
	}
}

// The Digest replay guard writes here on every authenticated request, so the
// upgrade has to leave the table claimable and the primary key has to be what
// rejects the second claim.
func TestPostgres_V120MigrationCreatesClaimableDigestNonceTable(t *testing.T) {
	pool := newV119Pool(t)
	applyV120Migration(t, pool)

	ctx := context.Background()
	store := New(pool)
	user, err := store.Users.UpsertOAuthUser(ctx, "subject-digest", "digest@example.test", "Test User", "Test")
	if err != nil {
		t.Fatalf("create user: %v", err)
	}
	token, err := store.AppPasswords.Create(ctx, AppPassword{UserID: user.ID, Label: "digest", TokenHash: "hash"})
	if err != nil {
		t.Fatalf("create app password: %v", err)
	}

	expiresAt := time.Now().UTC().Add(5 * time.Minute)
	claimed, err := store.DigestNonces.Consume(ctx, token.ID, "nonce-value", 1, expiresAt)
	if err != nil {
		t.Fatalf("Consume() error = %v", err)
	}
	if !claimed {
		t.Fatal("the first claim of a nonce count was refused")
	}

	replayed, err := store.DigestNonces.Consume(ctx, token.ID, "nonce-value", 1, expiresAt)
	if err != nil {
		t.Fatalf("Consume() replay error = %v", err)
	}
	if replayed {
		t.Fatal("the same nonce count was claimed twice")
	}

	// nc is eight hex digits, so validateDAVDigest parses it as a uint32 and the
	// whole of that range reaches here. A column that cannot hold the upper half
	// turns a well-formed request into a store error, which the replay guard can
	// only read as a replay.
	const maxNonceCount = math.MaxUint32
	claimed, err = store.DigestNonces.Consume(ctx, token.ID, "nonce-value", maxNonceCount, expiresAt)
	if err != nil {
		t.Fatalf("Consume(nc=%d) error = %v", uint32(maxNonceCount), err)
	}
	if !claimed {
		t.Fatalf("the first claim of nonce count %d was refused", uint32(maxNonceCount))
	}
	replayed, err = store.DigestNonces.Consume(ctx, token.ID, "nonce-value", maxNonceCount, expiresAt)
	if err != nil {
		t.Fatalf("Consume(nc=%d) replay error = %v", uint32(maxNonceCount), err)
	}
	if replayed {
		t.Fatalf("nonce count %d was claimed twice", uint32(maxNonceCount))
	}
}
