package store

import (
	"context"
	"database/sql"
	"math"
	"os"
	"path/filepath"
	"testing"
	"time"

	icalpkg "github.com/jw6ventures/calcard/internal/ical"
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

// A resource made only of detached instances -- RECURRENCE-ID components with
// no RRULE or RDATE anywhere -- is recurring as far as the recurrence matcher is
// concerned (ical.ConservativeRecurrenceBounds counts a RECURRENCE-ID), but the
// v1.1.7 backfill recognised only RRULE and RDATE and left its bounds NULL. The
// candidate filter then COALESCEs to the first component's dtstart/dtend and
// drops the row before the matcher ever sees the other instances, so an
// installation that upgraded through v1.1.7 has June instances it cannot find.
func TestPostgres_V120MigrationRepairsDetachedRecurrenceBounds(t *testing.T) {
	pool := newV119Pool(t)
	ctx := context.Background()
	store := New(pool)

	// Seeded through SQL rather than the repositories: these rows have to exist
	// before the upgrade runs, and the repositories write columns v1.1.9 does
	// not have yet.
	var calendarID int64
	if err := pool.QueryRow(`
WITH seeded_user AS (
    INSERT INTO users (oauth_subject, primary_email) VALUES ('subject-detached', 'detached@example.test')
    RETURNING id
)
INSERT INTO calendars (user_id, name) SELECT id, 'Detached' FROM seeded_user RETURNING id`).
		Scan(&calendarID); err != nil {
		t.Fatalf("seed user and calendar: %v", err)
	}

	// Written the way the v1.1.7 backfill left it: the columns exist, the first
	// component's dates are stored, and both bounds are NULL.
	const detached = "BEGIN:VCALENDAR\r\nVERSION:2.0\r\n" +
		"BEGIN:VEVENT\r\nUID:detached\r\nRECURRENCE-ID:20260115T100000Z\r\nDTSTART:20260115T100000Z\r\nDTEND:20260115T110000Z\r\nSUMMARY:January\r\nEND:VEVENT\r\n" +
		"BEGIN:VEVENT\r\nUID:detached\r\nRECURRENCE-ID:20260615T100000Z\r\nDTSTART:20260615T100000Z\r\nDTEND:20260615T110000Z\r\nSUMMARY:June\r\nEND:VEVENT\r\n" +
		"END:VCALENDAR\r\n"
	if _, err := pool.Exec(`
INSERT INTO events (calendar_id, uid, resource_name, raw_ical, etag, summary, dtstart, dtend, all_day, recurrence_start, recurrence_until, last_modified)
VALUES ($1, 'detached', 'detached.ics', $2, 'etag', 'January',
        '2026-01-15T10:00:00Z', '2026-01-15T11:00:00Z', FALSE, NULL, NULL, NOW())`,
		calendarID, detached); err != nil {
		t.Fatalf("seed detached resource: %v", err)
	}

	applyV120Migration(t, pool)

	var start, until *time.Time
	if err := pool.QueryRow(`SELECT recurrence_start, recurrence_until FROM events WHERE uid = 'detached'`).
		Scan(&start, &until); err != nil {
		t.Fatalf("read recurrence bounds: %v", err)
	}
	if start == nil || until == nil {
		t.Fatalf("recurrence bounds after upgrade = %v, %v, want both backfilled", start, until)
	}
	if !start.UTC().Equal(icalpkg.RecurrenceStartSentinel) || !until.UTC().Equal(icalpkg.RecurrenceUntilSentinel) {
		t.Fatalf("recurrence bounds = %s, %s, want the open-ended sentinels %s, %s",
			start.UTC(), until.UTC(), icalpkg.RecurrenceStartSentinel, icalpkg.RecurrenceUntilSentinel)
	}

	// The bounds exist so the SQL filter stops excluding the row; the June
	// instance has to survive the filter for the matcher to be able to find it.
	june := time.Date(2026, 6, 15, 0, 0, 0, 0, time.UTC)
	juneEnd := june.AddDate(0, 0, 1)
	events, err := store.Events.ListForCalendarFiltered(ctx, calendarID, EventFilter{
		Start: &june,
		End:   &juneEnd,
	})
	if err != nil {
		t.Fatalf("ListForCalendarFiltered() error = %v", err)
	}
	if len(events) != 1 || events[0].UID != "detached" {
		t.Fatalf("June candidates = %#v, want the detached resource", events)
	}

	// Repeatable, and it must not overwrite a precise bound a later write set.
	if _, err := pool.Exec(`UPDATE events SET recurrence_start = '2026-01-15T10:00:00Z', recurrence_until = '2026-06-15T11:00:00Z' WHERE uid = 'detached'`); err != nil {
		t.Fatalf("tighten bounds: %v", err)
	}
	applyV120Migration(t, pool)
	if err := pool.QueryRow(`SELECT recurrence_start, recurrence_until FROM events WHERE uid = 'detached'`).
		Scan(&start, &until); err != nil {
		t.Fatalf("re-read recurrence bounds: %v", err)
	}
	if !until.UTC().Equal(time.Date(2026, 6, 15, 11, 0, 0, 0, time.UTC)) {
		t.Fatalf("a second run replaced a precise bound with a sentinel: %s", until.UTC())
	}
}

// An installation still below v1.1.7 upgrades through the shipped file and then
// this release, in that order. v1.1.7 has shipped and is left exactly as it ran,
// so it still skips the detached-only resource; what matters is that the repair
// in v1.2.0 catches what it missed, which is the path this covers by applying
// both files the way the runner does.
func TestPostgres_UpgradeFromBeforeV117RepairsDetachedRecurrenceBounds(t *testing.T) {
	pool := newV119Pool(t)

	var calendarID int64
	if err := pool.QueryRow(`
WITH seeded_user AS (
    INSERT INTO users (oauth_subject, primary_email) VALUES ('subject-v117', 'v117@example.test')
    RETURNING id
)
INSERT INTO calendars (user_id, name) SELECT id, 'Detached' FROM seeded_user RETURNING id`).
		Scan(&calendarID); err != nil {
		t.Fatalf("seed user and calendar: %v", err)
	}

	seed := func(uid, raw string) {
		t.Helper()
		if _, err := pool.Exec(`
INSERT INTO events (calendar_id, uid, resource_name, raw_ical, etag, dtstart, dtend, all_day, recurrence_start, recurrence_until, last_modified)
VALUES ($1, $2, $2 || '.ics', $3, 'etag', '2026-01-15T10:00:00Z', '2026-01-15T11:00:00Z', FALSE, NULL, NULL, NOW())`,
			calendarID, uid, raw); err != nil {
			t.Fatalf("seed %s: %v", uid, err)
		}
	}
	seed("detached", "BEGIN:VCALENDAR\r\nVERSION:2.0\r\n"+
		"BEGIN:VEVENT\r\nUID:detached\r\nRECURRENCE-ID:20260115T100000Z\r\nDTSTART:20260115T100000Z\r\nDTEND:20260115T110000Z\r\nEND:VEVENT\r\n"+
		"BEGIN:VEVENT\r\nUID:detached\r\nRECURRENCE-ID:20260615T100000Z\r\nDTSTART:20260615T100000Z\r\nDTEND:20260615T110000Z\r\nEND:VEVENT\r\n"+
		"END:VCALENDAR\r\n")
	// A plain one-off must stay NULL through both files: the columns mean
	// "recurring", and filling them for every row would widen every candidate
	// scan for nothing.
	seed("single", "BEGIN:VCALENDAR\r\nVERSION:2.0\r\n"+
		"BEGIN:VEVENT\r\nUID:single\r\nDTSTART:20260115T100000Z\r\nDTEND:20260115T110000Z\r\nEND:VEVENT\r\n"+
		"END:VCALENDAR\r\n")

	// Ascending version order, which is the order findMigrations applies them in.
	migration, err := os.ReadFile(filepath.Join("..", "..", "migrations", "v1.1.7.sql"))
	if err != nil {
		t.Fatalf("read v1.1.7: %v", err)
	}
	if _, err := pool.Exec(string(migration)); err != nil {
		t.Fatalf("apply v1.1.7: %v", err)
	}

	bounds := func(uid string) (*time.Time, *time.Time) {
		t.Helper()
		var start, until *time.Time
		if err := pool.QueryRow(`SELECT recurrence_start, recurrence_until FROM events WHERE uid = $1`, uid).
			Scan(&start, &until); err != nil {
			t.Fatalf("read %s bounds: %v", uid, err)
		}
		return start, until
	}
	// The shipped file leaves the detached resource unbounded; that is the
	// state this release inherits and has to repair rather than rewrite.
	if start, until := bounds("detached"); start != nil || until != nil {
		t.Fatalf("v1.1.7 bounds = %v, %v, want the shipped file to leave both NULL", start, until)
	}

	applyV120Migration(t, pool)

	start, until := bounds("detached")
	if start == nil || until == nil {
		t.Fatalf("detached bounds after the upgrade = %v, %v, want both repaired", start, until)
	}
	if !start.UTC().Equal(icalpkg.RecurrenceStartSentinel) || !until.UTC().Equal(icalpkg.RecurrenceUntilSentinel) {
		t.Fatalf("detached bounds = %s, %s, want the open-ended sentinels", start.UTC(), until.UTC())
	}
	if start, until := bounds("single"); start != nil || until != nil {
		t.Fatalf("non-recurring bounds = %v, %v, want both NULL", start, until)
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
