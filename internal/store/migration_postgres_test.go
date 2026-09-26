package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"strings"
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

// legacyRecurrenceRepairPredicate is the repair predicate's WHERE clause with no
// filtering stage ahead of the component scan. It decides which stored resources
// count as recurring, and the staged predicate the migration runs is only correct
// if it decides identically, so the two are compared row for row rather than each
// being checked against expectations alone.
const legacyRecurrenceRepairPredicate = `
    (recurrence_start IS NULL OR recurrence_until IS NULL)
      AND (
          EXISTS (
              SELECT 1
              FROM regexp_matches(regexp_replace(events.raw_ical, E'(?:\\r\\n?|\\n)[ \\t]', '', 'g'), $re$BEGIN:VEVENT(.*?)END:VEVENT$re$, 'gi') AS component(match)
              WHERE component.match[1] ~* $re$(^|\r|\n)(RRULE|RDATE|RECURRENCE-ID)[;:]$re$
          )
          OR EXISTS (
              SELECT 1
              FROM regexp_matches(regexp_replace(events.raw_ical, E'(?:\\r\\n?|\\n)[ \\t]', '', 'g'), $re$BEGIN:VTODO(.*?)END:VTODO$re$, 'gi') AS component(match)
              WHERE component.match[1] ~* $re$(^|\r|\n)(RRULE|RDATE|RECURRENCE-ID)[;:]$re$
          )
          OR EXISTS (
              SELECT 1
              FROM regexp_matches(regexp_replace(events.raw_ical, E'(?:\\r\\n?|\\n)[ \\t]', '', 'g'), $re$BEGIN:VJOURNAL(.*?)END:VJOURNAL$re$, 'gi') AS component(match)
              WHERE component.match[1] ~* $re$(^|\r|\n)(RRULE|RDATE|RECURRENCE-ID)[;:]$re$
          )
      )`

// recurrenceRepairCase is one distinction the repair predicate has to draw:
// whether this stored body makes the resource recurring.
type recurrenceRepairCase struct {
	uid       string
	raw       string
	start     *string
	until     *string
	recurring bool
}

func recurrenceRepairCases() []recurrenceRepairCase {
	precise := "2026-01-15T10:00:00Z"
	preciseUntil := "2026-06-15T11:00:00Z"
	return []recurrenceRepairCase{
		// A recurrence property on each component the predicate scans.
		{uid: "vevent-rrule", raw: "BEGIN:VCALENDAR\r\nBEGIN:VEVENT\r\nUID:vevent-rrule\r\nDTSTART:20260115T100000Z\r\nRRULE:FREQ=DAILY\r\nEND:VEVENT\r\nEND:VCALENDAR\r\n", recurring: true},
		{uid: "vevent-rdate", raw: "BEGIN:VCALENDAR\r\nBEGIN:VEVENT\r\nUID:vevent-rdate\r\nDTSTART:20260115T100000Z\r\nRDATE:20260615T100000Z\r\nEND:VEVENT\r\nEND:VCALENDAR\r\n", recurring: true},
		{uid: "vevent-recid", raw: "BEGIN:VCALENDAR\r\nBEGIN:VEVENT\r\nUID:vevent-recid\r\nRECURRENCE-ID:20260615T100000Z\r\nDTSTART:20260615T100000Z\r\nEND:VEVENT\r\nEND:VCALENDAR\r\n", recurring: true},
		{uid: "vtodo-rrule", raw: "BEGIN:VCALENDAR\r\nBEGIN:VTODO\r\nUID:vtodo-rrule\r\nRRULE:FREQ=WEEKLY\r\nEND:VTODO\r\nEND:VCALENDAR\r\n", recurring: true},
		{uid: "vtodo-rdate", raw: "BEGIN:VCALENDAR\r\nBEGIN:VTODO\r\nUID:vtodo-rdate\r\nRDATE;VALUE=DATE:20260615\r\nEND:VTODO\r\nEND:VCALENDAR\r\n", recurring: true},
		{uid: "vjournal-recid", raw: "BEGIN:VCALENDAR\r\nBEGIN:VJOURNAL\r\nUID:vjournal-recid\r\nRECURRENCE-ID;TZID=UTC:20260615T100000\r\nEND:VJOURNAL\r\nEND:VCALENDAR\r\n", recurring: true},

		// Component and property names are matched without regard to case.
		{uid: "lower-property", raw: "BEGIN:VCALENDAR\r\nBEGIN:VEVENT\r\nUID:lower-property\r\nrrule:FREQ=DAILY\r\nEND:VEVENT\r\nEND:VCALENDAR\r\n", recurring: true},
		{uid: "lower-component", raw: "BEGIN:VCALENDAR\r\nbegin:vevent\r\nUID:lower-component\r\nRRULE:FREQ=DAILY\r\nend:vevent\r\nEND:VCALENDAR\r\n", recurring: true},

		// Content lines are unfolded before matching, so a fold may fall inside
		// the property name or the component name, use a tab continuation, or
		// come with bare LF or bare CR endings.
		{uid: "fold-property", raw: "BEGIN:VCALENDAR\r\nBEGIN:VEVENT\r\nUID:fold-property\r\nRRU\r\n LE:FREQ=DAILY\r\nEND:VEVENT\r\nEND:VCALENDAR\r\n", recurring: true},
		{uid: "fold-component", raw: "BEGIN:VCALENDAR\r\nBEGIN:VE\r\n VENT\r\nUID:fold-component\r\nRRULE:FREQ=DAILY\r\nEND:VE\r\n VENT\r\nEND:VCALENDAR\r\n", recurring: true},
		{uid: "fold-tab", raw: "BEGIN:VCALENDAR\r\nBEGIN:VTODO\r\nUID:fold-tab\r\nRDA\r\n\tTE:20260615T100000Z\r\nEND:VTODO\r\nEND:VCALENDAR\r\n", recurring: true},
		{uid: "fold-lf", raw: "BEGIN:VCALENDAR\nBEGIN:VEVENT\nUID:fold-lf\nRRU\n LE:FREQ=DAILY\nEND:VEVENT\nEND:VCALENDAR\n", recurring: true},
		{uid: "fold-cr", raw: "BEGIN:VCALENDAR\rBEGIN:VEVENT\rUID:fold-cr\rRRU\r LE:FREQ=DAILY\rEND:VEVENT\rEND:VCALENDAR\r", recurring: true},

		// A nested VALARM lies inside the VEVENT span, so a recurrence property
		// there counts exactly as one on the VEVENT itself does.
		{uid: "valarm-nested", raw: "BEGIN:VCALENDAR\r\nBEGIN:VEVENT\r\nUID:valarm-nested\r\nDTSTART:20260115T100000Z\r\nBEGIN:VALARM\r\nTRIGGER:-PT15M\r\nRRULE:FREQ=DAILY\r\nEND:VALARM\r\nEND:VEVENT\r\nEND:VCALENDAR\r\n", recurring: true},

		// Text outside ASCII anywhere in the component, ahead of the recurrence
		// property or after it. Whether a regex character class admits it can
		// depend on the collation, and a component the match cannot span is one
		// the repair never sees.
		{uid: "non-ascii-recurring", raw: "BEGIN:VCALENDAR\r\nBEGIN:VEVENT\r\nUID:non-ascii-recurring\r\nSUMMARY:Café à la crème 😀\r\nRRULE:FREQ=WEEKLY\r\nLOCATION:Zürich\r\nEND:VEVENT\r\nEND:VCALENDAR\r\n", recurring: true},
		{uid: "non-ascii-plain", raw: "BEGIN:VCALENDAR\r\nBEGIN:VEVENT\r\nUID:non-ascii-plain\r\nSUMMARY:Café 😀\r\nDTSTART:20260115T100000Z\r\nEND:VEVENT\r\nEND:VCALENDAR\r\n"},

		// One bound already precise still leaves the other to fill.
		{uid: "partial-bound", raw: "BEGIN:VCALENDAR\r\nBEGIN:VEVENT\r\nUID:partial-bound\r\nRRULE:FREQ=DAILY\r\nEND:VEVENT\r\nEND:VCALENDAR\r\n", start: &precise, recurring: true},

		// VTIMEZONE DST rules are RRULEs outside every scanned component. The
		// placement between two VEVENTs is what separates a span reaching from
		// the first BEGIN to the last END from the per-component match: the
		// former covers this RRULE, and the resource is still not recurring.
		{uid: "tz-before", raw: "BEGIN:VCALENDAR\r\nBEGIN:VTIMEZONE\r\nTZID:America/New_York\r\nBEGIN:DAYLIGHT\r\nRRULE:FREQ=YEARLY;BYMONTH=3;BYDAY=2SU\r\nEND:DAYLIGHT\r\nEND:VTIMEZONE\r\nBEGIN:VEVENT\r\nUID:tz-before\r\nDTSTART:20260115T100000Z\r\nEND:VEVENT\r\nEND:VCALENDAR\r\n"},
		{uid: "tz-between", raw: "BEGIN:VCALENDAR\r\nBEGIN:VEVENT\r\nUID:tz-between\r\nDTSTART:20260115T100000Z\r\nEND:VEVENT\r\nBEGIN:VTIMEZONE\r\nTZID:America/New_York\r\nBEGIN:STANDARD\r\nRRULE:FREQ=YEARLY;BYMONTH=11;BYDAY=1SU\r\nEND:STANDARD\r\nEND:VTIMEZONE\r\nBEGIN:VEVENT\r\nUID:tz-between-2\r\nDTSTART:20260116T100000Z\r\nEND:VEVENT\r\nEND:VCALENDAR\r\n"},

		// The literal text appears, but never at the start of a content line:
		// once inside an X- property value, once as the tail of a folded
		// DESCRIPTION, where unfolding joins it onto the line before it.
		{uid: "x-prop-literal", raw: "BEGIN:VCALENDAR\r\nBEGIN:VEVENT\r\nUID:x-prop-literal\r\nX-ALT-DESC:see RRULE:FREQ=DAILY for details\r\nEND:VEVENT\r\nEND:VCALENDAR\r\n"},
		{uid: "fold-desc-value", raw: "BEGIN:VCALENDAR\r\nBEGIN:VEVENT\r\nUID:fold-desc-value\r\nDESCRIPTION:hello\r\n RRULE:FREQ=DAILY\r\nEND:VEVENT\r\nEND:VCALENDAR\r\n"},

		// Inside a component that carries no recurrence for the matcher, after
		// the component closed, and inside one that never closes.
		{uid: "vfreebusy-rdate", raw: "BEGIN:VCALENDAR\r\nBEGIN:VFREEBUSY\r\nUID:vfreebusy-rdate\r\nRDATE:20260615T100000Z\r\nEND:VFREEBUSY\r\nBEGIN:VEVENT\r\nUID:vfreebusy-rdate-2\r\nDTSTART:20260115T100000Z\r\nEND:VEVENT\r\nEND:VCALENDAR\r\n"},
		{uid: "rrule-after-end", raw: "BEGIN:VCALENDAR\r\nBEGIN:VEVENT\r\nUID:rrule-after-end\r\nDTSTART:20260115T100000Z\r\nEND:VEVENT\r\nRRULE:FREQ=DAILY\r\nEND:VCALENDAR\r\n"},
		{uid: "unterminated", raw: "BEGIN:VCALENDAR\r\nBEGIN:VEVENT\r\nUID:unterminated\r\nRRULE:FREQ=DAILY\r\nEND:VCALENDAR\r\n"},
		{uid: "plain", raw: "BEGIN:VCALENDAR\r\nBEGIN:VEVENT\r\nUID:plain\r\nDTSTART:20260115T100000Z\r\nDTEND:20260115T110000Z\r\nSUMMARY:Lunch\r\nEND:VEVENT\r\nEND:VCALENDAR\r\n"},

		// Recurring, but both bounds are already precise, so the NULL guard
		// keeps the repair away from them.
		{uid: "already-bound", raw: "BEGIN:VCALENDAR\r\nBEGIN:VEVENT\r\nUID:already-bound\r\nRRULE:FREQ=DAILY\r\nEND:VEVENT\r\nEND:VCALENDAR\r\n", start: &precise, until: &preciseUntil},
	}
}

// The repair has to reach exactly the resources the matcher treats as recurring:
// a row it skips keeps NULL bounds and drops out of the candidate filter, and a
// row it repairs needlessly widens every range scan that resource appears in.
//
// The regexes evaluate under the collation of raw_ical, which is the database's
// unless the column names one. Running the cases again with the column under
// "C" stands in for an installation whose database was created with
// LC_COLLATE=C, where locale-defined character classes admit ASCII only.
func TestPostgres_V120MigrationRepairsExactlyTheRecurringRowSet(t *testing.T) {
	t.Run("database collation", func(t *testing.T) {
		testV120MigrationRepairsExactlyTheRecurringRowSet(t, "")
	})
	t.Run("C collation", func(t *testing.T) {
		testV120MigrationRepairsExactlyTheRecurringRowSet(t, "C")
	})
}

func testV120MigrationRepairsExactlyTheRecurringRowSet(t *testing.T, collation string) {
	pool := newV119Pool(t)
	if collation != "" {
		if _, err := pool.Exec(fmt.Sprintf(`ALTER TABLE events ALTER COLUMN raw_ical TYPE TEXT COLLATE %q`, collation)); err != nil {
			t.Fatalf("set raw_ical collation: %v", err)
		}
	}

	var calendarID int64
	if err := pool.QueryRow(`
WITH seeded_user AS (
    INSERT INTO users (oauth_subject, primary_email) VALUES ('subject-rowset', 'rowset@example.test')
    RETURNING id
)
INSERT INTO calendars (user_id, name) SELECT id, 'Row set' FROM seeded_user RETURNING id`).
		Scan(&calendarID); err != nil {
		t.Fatalf("seed user and calendar: %v", err)
	}

	cases := recurrenceRepairCases()
	for _, test := range cases {
		if _, err := pool.Exec(`
INSERT INTO events (calendar_id, uid, resource_name, raw_ical, etag, dtstart, dtend, all_day, recurrence_start, recurrence_until, last_modified)
VALUES ($1, $2, $2 || '.ics', $3, 'etag', '2026-01-15T10:00:00Z', '2026-01-15T11:00:00Z', FALSE, $4, $5, NOW())`,
			calendarID, test.uid, test.raw, test.start, test.until); err != nil {
			t.Fatalf("seed %s: %v", test.uid, err)
		}
	}

	readUIDs := func(query string, args ...any) map[string]bool {
		t.Helper()
		rows, err := pool.Query(query, args...)
		if err != nil {
			t.Fatalf("query: %v", err)
		}
		defer rows.Close()
		got := map[string]bool{}
		for rows.Next() {
			var uid string
			if err := rows.Scan(&uid); err != nil {
				t.Fatalf("scan uid: %v", err)
			}
			got[uid] = true
		}
		if err := rows.Err(); err != nil {
			t.Fatalf("read uids: %v", err)
		}
		return got
	}

	// Taken before the repair runs, because the predicate reads the bounds the
	// repair is about to write.
	legacy := readUIDs(`SELECT uid FROM events WHERE ` + legacyRecurrenceRepairPredicate)

	applyV120Migration(t, pool)

	// A row the repair touched is one whose bounds are no longer what they were
	// seeded as, which is the only reading that treats a partially bounded row
	// and a fully unbounded one alike.
	repaired := readUIDs(`
SELECT uid FROM events
WHERE recurrence_start IS DISTINCT FROM $1::timestamptz
   OR recurrence_until IS DISTINCT FROM $2::timestamptz`, nil, nil)
	for _, test := range cases {
		if test.start != nil || test.until != nil {
			delete(repaired, test.uid)
		}
	}
	// The two seeded with a bound of their own are judged on the bound that was
	// NULL, which the repair fills only for a recurring resource.
	for _, test := range cases {
		if test.start == nil && test.until == nil {
			continue
		}
		var until *time.Time
		if err := pool.QueryRow(`SELECT recurrence_until FROM events WHERE uid = $1`, test.uid).Scan(&until); err != nil {
			t.Fatalf("read %s bounds: %v", test.uid, err)
		}
		if test.until == nil && until != nil {
			repaired[test.uid] = true
		}
	}

	expected := map[string]bool{}
	for _, test := range cases {
		if test.recurring {
			expected[test.uid] = true
		}
	}

	for _, test := range cases {
		if repaired[test.uid] != expected[test.uid] {
			t.Errorf("%s: repaired = %t, want %t", test.uid, repaired[test.uid], expected[test.uid])
		}
		if repaired[test.uid] != legacy[test.uid] {
			t.Errorf("%s: repaired = %t, but the unstaged predicate selects %t: the staged predicate changed which rows are repaired",
				test.uid, repaired[test.uid], legacy[test.uid])
		}
	}
	if len(repaired) != len(expected) {
		t.Errorf("repaired %d rows, want %d: %v vs %v", len(repaired), len(expected), repaired, expected)
	}
}

// migrationStatement is the statement of the v1.2.0 migration that starts with
// prefix, exactly as the file carries it. The plan tests read the plan of that
// statement, so it comes from the file rather than from a copy that can drift.
func migrationStatement(t *testing.T, prefix string) string {
	t.Helper()
	migration, err := os.ReadFile(filepath.Join("..", "..", "migrations", "v1.2.0.sql"))
	if err != nil {
		t.Fatalf("read migration: %v", err)
	}
	from := strings.Index(string(migration), prefix)
	if from < 0 {
		t.Fatalf("the migration no longer carries a statement starting %q", prefix)
	}
	statement := string(migration)[from:]
	to := strings.Index(statement, ";\n")
	if to < 0 {
		t.Fatalf("the statement starting %q is unterminated", prefix)
	}
	return statement[:to+1]
}

func recurrenceRepairStatement(t *testing.T) string {
	t.Helper()
	return migrationStatement(t, "UPDATE events\n")
}

// explainStatement returns the plan tree of statement, read
// from EXPLAIN's JSON form: its keys are a stable interface across PostgreSQL
// releases, where the text form's labels and subplan spellings are not. With
// analyze set the statement executes, so it runs inside a transaction that is
// rolled back and the plan carries the actual loop count of every node.
func explainStatement(t *testing.T, pool *sql.DB, statement string, analyze bool) map[string]any {
	t.Helper()
	options := "VERBOSE, COSTS OFF, FORMAT JSON"
	if analyze {
		options = "ANALYZE, TIMING OFF, " + options
	}
	tx, err := pool.Begin()
	if err != nil {
		t.Fatalf("begin explain transaction: %v", err)
	}
	defer tx.Rollback()

	var raw []byte
	if err := tx.QueryRow("EXPLAIN (" + options + ") " + statement).Scan(&raw); err != nil {
		t.Fatalf("explain %.40q: %v", statement, err)
	}
	var plans []struct {
		Plan map[string]any `json:"Plan"`
	}
	if err := json.Unmarshal(raw, &plans); err != nil {
		t.Fatalf("decode plan: %v\n%s", err, raw)
	}
	if len(plans) != 1 || plans[0].Plan == nil {
		t.Fatalf("plan = %s, want one statement plan", raw)
	}
	return plans[0].Plan
}

func explainRecurrenceRepair(t *testing.T, pool *sql.DB, analyze bool) map[string]any {
	t.Helper()
	return explainStatement(t, pool, recurrenceRepairStatement(t), analyze)
}

// walkPlan visits node and every node beneath it.
func walkPlan(node map[string]any, visit func(map[string]any)) {
	visit(node)
	children, _ := node["Plans"].([]any)
	for _, child := range children {
		if childNode, ok := child.(map[string]any); ok {
			walkPlan(childNode, visit)
		}
	}
}

// componentMatch is one regexp_matches scan in the repair plan. The greedy
// matches only filter; the non-greedy ones decide the row set.
type componentMatch struct {
	call   string
	greedy bool
	loops  float64
}

func recurrenceRepairComponentMatches(t *testing.T, plan map[string]any) []componentMatch {
	t.Helper()
	var matches []componentMatch
	walkPlan(plan, func(node map[string]any) {
		call, _ := node["Function Call"].(string)
		if !strings.HasPrefix(call, "regexp_matches(") {
			return
		}
		loops, _ := node["Actual Loops"].(float64)
		matches = append(matches, componentMatch{call: call, greedy: !strings.Contains(call, "*?)"), loops: loops})
	})
	greedy := 0
	for _, match := range matches {
		if match.greedy {
			greedy++
		}
	}
	if len(matches) != 6 || greedy != 3 {
		t.Fatalf("the plan carries %d component matches, %d of them greedy, want three greedy and three non-greedy: %#v", len(matches), greedy, matches)
	}
	return matches
}

// Unfolding a body is a full pass over it, and PostgreSQL does not eliminate a
// repeated subexpression: a predicate naming the unfold in each of its tests
// unfolds the same body once per test. The repair names it once, as a function
// scan whose column every test reads, and the plan is what settles that.
func TestPostgres_V120MigrationUnfoldsEachBodyOnce(t *testing.T) {
	pool := newV119Pool(t)
	applyV120Migration(t, pool)

	if unfolds := countInPlan(explainRecurrenceRepair(t, pool, false), "regexp_replace("); unfolds != 1 {
		t.Errorf("the repair plan unfolds each body %d times, want once", unfolds)
	}
}

// countInPlan counts occurrences of needle across every expression the plan
// prints, which is how often the executor evaluates it per row that reaches it.
func countInPlan(plan map[string]any, needle string) int {
	count := 0
	walkPlan(plan, func(node map[string]any) {
		for key, value := range node {
			if key != "Plans" {
				count += strings.Count(fmt.Sprint(value), needle)
			}
		}
	})
	return count
}

// seedRepairCalendar creates the calendar the plan tests seed their events in.
func seedRepairCalendar(t *testing.T, pool *sql.DB, subject string) int64 {
	t.Helper()
	var calendarID int64
	if err := pool.QueryRow(`
WITH seeded_user AS (
    INSERT INTO users (oauth_subject, primary_email) VALUES ($1, $1 || '@example.test')
    RETURNING id
)
INSERT INTO calendars (user_id, name) SELECT id, 'Repair' FROM seeded_user RETURNING id`, subject).
		Scan(&calendarID); err != nil {
		t.Fatalf("seed user and calendar: %v", err)
	}
	return calendarID
}

// The two filtering tests are cheaper than the component match only when they run
// ahead of it, and the planner does not put them there: it costs the greedy span
// match and the non-greedy component match identically, so in an AND list their
// order is only the order they were written in. The CASE cascade is what holds
// it, and how often each match actually ran is where that shows. Each body stops
// at a different stage, and no match past that stage may run for it.
func TestPostgres_V120MigrationRunsTheCheapRecurrenceFiltersFirst(t *testing.T) {
	pool := newV119Pool(t)
	applyV120Migration(t, pool)
	calendarID := seedRepairCalendar(t, pool, "subject-stages")

	for _, test := range []struct {
		name                 string
		raw                  string
		spansRun, matchesRun bool
	}{
		{
			name: "no recurrence property anywhere",
			raw:  "BEGIN:VCALENDAR\r\nBEGIN:VEVENT\r\nUID:plain\r\nDTSTART:20260115T100000Z\r\nEND:VEVENT\r\nEND:VCALENDAR\r\n",
		},
		{
			name:     "recurrence property outside every component",
			raw:      "BEGIN:VCALENDAR\r\nBEGIN:VTIMEZONE\r\nTZID:America/New_York\r\nBEGIN:DAYLIGHT\r\nRRULE:FREQ=YEARLY;BYMONTH=3;BYDAY=2SU\r\nEND:DAYLIGHT\r\nEND:VTIMEZONE\r\nBEGIN:VEVENT\r\nUID:tz\r\nDTSTART:20260115T100000Z\r\nEND:VEVENT\r\nEND:VCALENDAR\r\n",
			spansRun: true,
		},
		{
			name:       "recurrence property inside a component",
			raw:        "BEGIN:VCALENDAR\r\nBEGIN:VEVENT\r\nUID:rrule\r\nDTSTART:20260115T100000Z\r\nRRULE:FREQ=DAILY\r\nEND:VEVENT\r\nEND:VCALENDAR\r\n",
			spansRun:   true,
			matchesRun: true,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			if _, err := pool.Exec(`DELETE FROM events`); err != nil {
				t.Fatalf("clear events: %v", err)
			}
			if _, err := pool.Exec(`
INSERT INTO events (calendar_id, uid, resource_name, raw_ical, etag, dtstart, dtend, all_day, last_modified)
VALUES ($1, 'stage', 'stage.ics', $2, 'etag', '2026-01-15T10:00:00Z', '2026-01-15T11:00:00Z', FALSE, NOW())`,
				calendarID, test.raw); err != nil {
				t.Fatalf("seed event: %v", err)
			}

			for _, match := range recurrenceRepairComponentMatches(t, explainRecurrenceRepair(t, pool, true)) {
				want := test.spansRun
				if !match.greedy {
					want = test.matchesRun
				}
				// The VEVENT scan of each stage runs first within its stage, so
				// it is the one that runs whenever the stage is reached at all.
				if strings.Contains(match.call, "BEGIN:VEVENT") && (match.loops > 0) != want {
					t.Errorf("%s ran %v times, want it to run: %t", match.call, match.loops, want)
				}
				if !want && match.loops > 0 {
					t.Errorf("%s ran %v times for a body the earlier stages reject", match.call, match.loops)
				}
			}
		})
	}
}

// The repair scans every row of events inside the transaction that holds
// ACCESS EXCLUSIVE on the table, so its per-row cost is startup time during
// which the table is unavailable. The non-greedy component match is the part
// whose cost grows faster than the body, and this pins that the common
// non-recurring shape never reaches it.
//
// The bodies carry a VTIMEZONE whose DST rules are RRULEs. That is what most
// clients write, and it is the shape no property-name filter can exclude: only a
// component-scoped test can tell it from a genuinely recurring resource. The
// assertion is on how often each match ran rather than on elapsed time, which
// the load on the database server decides as much as the predicate does.
func TestPostgres_V120MigrationBackfillKeepsTimezoneRulesOffTheComponentMatch(t *testing.T) {
	pool := newV119Pool(t)
	calendarID := seedRepairCalendar(t, pool, "subject-backfill")

	const seededRows = 200
	if _, err := pool.Exec(`
INSERT INTO events (calendar_id, uid, resource_name, raw_ical, etag, dtstart, dtend, all_day, recurrence_start, recurrence_until, last_modified)
SELECT $1, 'bulk-' || g, 'bulk-' || g || '.ics',
       'BEGIN:VCALENDAR' || E'\r\n' || 'VERSION:2.0' || E'\r\n' ||
       'BEGIN:VTIMEZONE' || E'\r\n' || 'TZID:America/New_York' || E'\r\n' ||
       'BEGIN:DAYLIGHT' || E'\r\n' || 'TZOFFSETFROM:-0500' || E'\r\n' ||
       'RRULE:FREQ=YEARLY;BYMONTH=3;BYDAY=2SU' || E'\r\n' || 'END:DAYLIGHT' || E'\r\n' ||
       'BEGIN:STANDARD' || E'\r\n' || 'TZOFFSETFROM:-0400' || E'\r\n' ||
       'RRULE:FREQ=YEARLY;BYMONTH=11;BYDAY=1SU' || E'\r\n' || 'END:STANDARD' || E'\r\n' ||
       'END:VTIMEZONE' || E'\r\n' ||
       'BEGIN:VEVENT' || E'\r\n' || 'UID:bulk-' || g || E'\r\n' ||
       'DTSTART;TZID=America/New_York:20260115T100000' || E'\r\n' ||
       'DTEND;TZID=America/New_York:20260115T110000' || E'\r\n' ||
       'DESCRIPTION:' || repeat('a', 2200) || E'\r\n' ||
       'END:VEVENT' || E'\r\n' || 'END:VCALENDAR' || E'\r\n',
       'etag', '2026-01-15T10:00:00Z', '2026-01-15T11:00:00Z', FALSE, NULL, NULL, NOW()
FROM generate_series(1, $2) g`, calendarID, seededRows); err != nil {
		t.Fatalf("seed events: %v", err)
	}

	// Every row passes the property-name search, so each greedy span runs once
	// per row and rejects it; the component match must never run.
	for _, match := range recurrenceRepairComponentMatches(t, explainRecurrenceRepair(t, pool, true)) {
		if match.greedy && match.loops != seededRows {
			t.Errorf("%s ran %v times, want once per row (%d)", match.call, match.loops, seededRows)
		}
		if !match.greedy && match.loops != 0 {
			t.Errorf("%s ran %v times over %d non-recurring rows, want never", match.call, match.loops, seededRows)
		}
	}

	applyV120Migration(t, pool)

	var repaired int
	if err := pool.QueryRow(
		`SELECT count(*) FROM events WHERE recurrence_start IS NOT NULL OR recurrence_until IS NOT NULL`).Scan(&repaired); err != nil {
		t.Fatalf("count repaired: %v", err)
	}
	if repaired != 0 {
		t.Fatalf("repaired %d rows, want 0: the migration treated a VTIMEZONE DST rule as a recurrence", repaired)
	}
}

func TestPostgres_UpgradeRepairsFoldedRecurrenceBounds(t *testing.T) {
	pool := newPostgresSchemaPool(t, "testdata/schema_v1.0.13.sql")
	var calendarID int64
	if err := pool.QueryRow(`WITH u AS (INSERT INTO users(oauth_subject, primary_email) VALUES ('folded','folded@example.test') RETURNING id) INSERT INTO calendars(user_id,name) SELECT id,'Folded' FROM u RETURNING id`).Scan(&calendarID); err != nil {
		t.Fatal(err)
	}
	for _, test := range []struct{ uid, component, property string }{
		{"rrule", "VE\r\n VENT", "RRU\r\n LE:FREQ=DAILY;COUNT=365"},
		{"rdate", "VT\r\n\tODO", "RDA\r\n\tTE:20260615T100000Z"},
		{"detached", "VJOU\r\n RNAL", "RECURRENCE-\r\n ID:20260615T100000Z"},
		{"lf", "VE\n VENT", "RRU\n LE:FREQ=DAILY;COUNT=365"},
	} {
		raw := "BEGIN:VCALENDAR\r\nVERSION:2.0\r\nBEGIN:" + test.component + "\r\nUID:" + test.uid + "\r\nDTSTART:20260115T100000Z\r\nDTEND:20260115T110000Z\r\n" + test.property + "\r\nEND:" + test.component + "\r\nEND:VCALENDAR\r\n"
		if _, err := pool.Exec(`INSERT INTO events(calendar_id,uid,resource_name,raw_ical,etag,dtstart,dtend) VALUES ($1,$2,$2,$3,'e','2026-01-15T10:00:00Z','2026-01-15T11:00:00Z')`, calendarID, test.uid, raw); err != nil {
			t.Fatal(err)
		}
	}
	for _, version := range []string{"v1.1.3.sql", "v1.1.4.sql", "v1.1.6.sql", "v1.1.7.sql", "v1.1.8.sql", "v1.1.9.sql"} {
		body, err := os.ReadFile(filepath.Join("..", "..", "migrations", version))
		if err != nil {
			t.Fatal(err)
		}
		if _, err := pool.Exec(string(body)); err != nil {
			t.Fatalf("%s: %v", version, err)
		}
	}
	applyV120Migration(t, pool)
	start := time.Date(2026, 6, 15, 0, 0, 0, 0, time.UTC)
	end := start.AddDate(0, 0, 1)
	events, err := New(pool).Events.ListForCalendarFiltered(t.Context(), calendarID, EventFilter{Start: &start, End: &end})
	if err != nil {
		t.Fatal(err)
	}
	if len(events) != 4 {
		t.Fatalf("June candidates = %d, want 4", len(events))
	}
	var repaired int
	if err := pool.QueryRow(`SELECT count(*) FROM events WHERE recurrence_start = $1 AND recurrence_until = $2`, icalpkg.RecurrenceStartSentinel, icalpkg.RecurrenceUntilSentinel).Scan(&repaired); err != nil {
		t.Fatal(err)
	}
	if repaired != 4 {
		t.Fatalf("repaired bounds = %d, want 4", repaired)
	}
}

// v120IndexDefinitions reads the definitions of the indexes this migration
// builds or drops, keyed by name, with the schema qualifier removed so an
// upgraded database and a fresh one compare directly.
func v120IndexDefinitions(t *testing.T, pool *sql.DB) map[string]string {
	t.Helper()
	rows, err := pool.Query(`
SELECT indexname, replace(indexdef, schemaname || '.', '')
FROM pg_indexes
WHERE schemaname = current_schema()
  AND indexname IN ('idx_events_recurrence_start', 'idx_events_recurrence_until',
                    'idx_events_calendar_keyset', 'idx_contacts_book_keyset',
                    'idx_deleted_resources_keyset',
                    'idx_events_calendar_id', 'idx_contacts_address_book_id')`)
	if err != nil {
		t.Fatalf("read index definitions: %v", err)
	}
	defer rows.Close()
	definitions := map[string]string{}
	for rows.Next() {
		var name, definition string
		if err := rows.Scan(&name, &definition); err != nil {
			t.Fatalf("scan index definition: %v", err)
		}
		definitions[name] = definition
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("read index definitions: %v", err)
	}
	return definitions
}

// Both upgrade paths have to arrive at the indexes a fresh install creates: a
// v1.1.9 database, which has none of the keyset indexes, and a database that ran
// a v1.2.0 release candidate, which built the events and contacts keyset indexes
// on (collection, id) alone.
func TestPostgres_V120MigrationIndexesMatchBaselineSchema(t *testing.T) {
	want := v120IndexDefinitions(t, newPostgresPool(t))
	if len(want) != 5 {
		t.Fatalf("db.sql carries %d of the migrated indexes, want 5: %v", len(want), want)
	}

	for _, test := range []struct {
		name  string
		setup string
	}{
		{name: "from v1.1.9"},
		{name: "from a release candidate", setup: `
CREATE INDEX idx_events_calendar_keyset ON events (calendar_id, id);
CREATE INDEX idx_contacts_book_keyset ON contacts (address_book_id, id);`},
	} {
		t.Run(test.name, func(t *testing.T) {
			pool := newV119Pool(t)
			if test.setup != "" {
				if _, err := pool.Exec(test.setup); err != nil {
					t.Fatalf("build release candidate indexes: %v", err)
				}
			}
			applyV120Migration(t, pool)

			got := v120IndexDefinitions(t, pool)
			for name, definition := range want {
				if got[name] != definition {
					t.Errorf("%s = %q, want %q", name, got[name], definition)
				}
			}
			for name := range got {
				if _, ok := want[name]; !ok {
					t.Errorf("the upgrade leaves %s, which a fresh install does not have: %s", name, got[name])
				}
			}
		})
	}
}

// Everything in this file runs under ACCESS EXCLUSIVE inside the startup budget,
// so no index is built twice, and the recurrence repair runs while no index
// reading the columns it writes exists: each repaired row would otherwise
// maintain every one of them.
func TestV120MigrationBuildsEachIndexOnceAfterTheRepair(t *testing.T) {
	contents, err := os.ReadFile(filepath.Join("..", "..", "migrations", "v1.2.0.sql"))
	if err != nil {
		t.Fatalf("read migration: %v", err)
	}
	var statements []string
	for _, statement := range strings.Split(string(contents), ";\n") {
		var code []string
		for _, line := range strings.Split(statement, "\n") {
			if !strings.HasPrefix(strings.TrimSpace(line), "--") {
				code = append(code, line)
			}
		}
		statements = append(statements, strings.TrimSpace(strings.Join(code, "\n")))
	}

	repair := -1
	for i, statement := range statements {
		if strings.HasPrefix(statement, "UPDATE events\n") {
			repair = i
		}
	}
	if repair < 0 {
		t.Fatal("the migration no longer carries the recurrence repair")
	}

	built := map[string]int{}
	for i, statement := range statements {
		if !strings.HasPrefix(statement, "CREATE INDEX") {
			continue
		}
		fields := strings.Fields(strings.TrimPrefix(statement, "CREATE INDEX IF NOT EXISTS"))
		name := fields[0]
		if fields[0] == "CREATE" {
			name = strings.Fields(statement)[2]
		}
		built[name]++
		if built[name] > 1 {
			t.Errorf("%s is built more than once", name)
		}
		if strings.Contains(statement, "ON events") && strings.Contains(statement, "recurrence_") && i < repair {
			t.Errorf("%s reads the recurrence columns and is built before the repair writes them", name)
		}
	}
}

// syncStampDefinitions reads the trigger functions and triggers that stamp
// members, tombstones and collections for sync, with the per-test schema name
// taken out so two databases can be compared.
func syncStampDefinitions(t *testing.T, pool *sql.DB) map[string]string {
	t.Helper()
	var schema string
	if err := pool.QueryRow(`SELECT current_schema()`).Scan(&schema); err != nil {
		t.Fatalf("read schema: %v", err)
	}
	rows, err := pool.Query(`
SELECT 'function ' || p.proname, pg_get_functiondef(p.oid)
FROM pg_proc p
WHERE p.pronamespace = current_schema()::regnamespace
  AND p.proname IN ('touch_last_modified', 'stamp_collection_updated_at', 'increment_calendar_ctag', 'increment_address_book_ctag', 'stamp_deleted_resource')
UNION ALL
SELECT 'trigger ' || t.tgname, pg_get_triggerdef(t.oid)
FROM pg_trigger t
JOIN pg_class c ON c.oid = t.tgrelid
WHERE NOT t.tgisinternal
  AND c.relnamespace = current_schema()::regnamespace
  AND c.relname IN ('events', 'contacts', 'calendars', 'address_books', 'deleted_resources')`)
	if err != nil {
		t.Fatalf("read sync stamp definitions: %v", err)
	}
	defer rows.Close()
	definitions := make(map[string]string)
	for rows.Next() {
		var name, definition string
		if err := rows.Scan(&name, &definition); err != nil {
			t.Fatalf("scan sync stamp definition: %v", err)
		}
		definitions[name] = strings.ReplaceAll(definition, schema+".", "")
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("read sync stamp definitions: %v", err)
	}
	return definitions
}

// An upgraded installation has to stamp sync changes exactly as a fresh one
// does: a trigger the upgrade left on NOW() would stamp a write behind a token
// handed out before it committed, and that change would never be reported.
func TestPostgres_V120MigrationStampsSyncChangesLikeBaselineSchema(t *testing.T) {
	want := syncStampDefinitions(t, newPostgresPool(t))
	if len(want) != 12 {
		t.Fatalf("db.sql carries %d sync stamp functions and triggers, want 12: %v", len(want), want)
	}
	for name, definition := range want {
		if strings.HasPrefix(name, "function ") && strings.Contains(definition, "now()") {
			t.Errorf("db.sql %s stamps with NOW(): %s", name, definition)
		}
	}

	pool := newV119Pool(t)
	applyV120Migration(t, pool)
	applyV120Migration(t, pool)
	got := syncStampDefinitions(t, pool)
	for name, definition := range want {
		if got[name] != definition {
			t.Errorf("%s after the upgrade = %q, want %q", name, got[name], definition)
		}
	}
	for name := range got {
		if _, ok := want[name]; !ok {
			t.Errorf("the upgrade leaves %s, which a fresh install does not have: %s", name, got[name])
		}
	}
}

// Earlier releases stored a year-less birthday in year 1, which is not a leap
// year: --02-29 came out as March 1. The upgrade re-derives those rows from
// the card itself into NoYearBirthdayYear, moves the address book's CTag so
// clients notice, and leaves every birthday that carries a year alone.
func TestPostgres_V120MigrationRederivesYearlessBirthdays(t *testing.T) {
	pool := newV119Pool(t)
	card := func(bday string) string {
		return "BEGIN:VCARD\r\nVERSION:3.0\r\nFN:Someone\r\nBDAY" + bday + "\r\nEND:VCARD\r\n"
	}
	var bookID int64
	if err := pool.QueryRow(`
WITH seeded_user AS (
    INSERT INTO users (oauth_subject, primary_email) VALUES ('subject-birthdays', 'birthdays@example.test')
    RETURNING id
)
INSERT INTO address_books (user_id, name) SELECT id, 'Birthdays' FROM seeded_user RETURNING id`).Scan(&bookID); err != nil {
		t.Fatalf("seed user and address book: %v", err)
	}
	seeded := []struct {
		uid, card, stored, want string
	}{
		{uid: "leap", card: card(":--02-29"), stored: "0001-03-01", want: "0004-02-29"},
		{uid: "basic", card: card(":--1231"), stored: "0001-12-31", want: "0004-12-31"},
		{uid: "folded", card: "BEGIN:VCARD\r\nVERSION:3.0\r\nFN:Someone\r\nBDAY;VALUE=date:--0\r\n 7-04\r\nEND:VCARD\r\n", stored: "0001-07-04", want: "0004-07-04"},
		{uid: "dated", card: card(":1990-05-15"), stored: "1990-05-15", want: "1990-05-15"},
		{uid: "year-one", card: card(":0001-05-05"), stored: "0001-05-05", want: "0001-05-05"},

		// Apple clients write a placeholder year and name it in
		// X-APPLE-OMIT-YEAR; that year is no year at all.
		{uid: "apple", card: card(";X-APPLE-OMIT-YEAR=1604:1604-03-15"), stored: "1604-03-15", want: "0004-03-15"},
		{uid: "apple-quoted", card: card(";X-APPLE-OMIT-YEAR=\"1604\";VALUE=date:1604-02-29"), stored: "1604-02-29", want: "0004-02-29"},
		{uid: "apple-real-year", card: card(";X-APPLE-OMIT-YEAR=1604:1990-03-15"), stored: "1990-03-15", want: "1990-03-15"},

		// Bare CR line endings, and a fold behind one.
		{uid: "cr-only", card: "BEGIN:VCARD\rVERSION:3.0\rFN:Someone\rBDAY:--02-29\rEND:VCARD\r", stored: "0001-03-01", want: "0004-02-29"},
		{uid: "cr-fold", card: "BEGIN:VCARD\rVERSION:3.0\rFN:Someone\rBDAY:--0\r 2-29\rEND:VCARD\r", stored: "0001-03-01", want: "0004-02-29"},

		// The first BDAY line decides, as it does for parseVCardFields, whether
		// or not a later one would parse.
		{uid: "dated-then-yearless", card: "BEGIN:VCARD\r\nVERSION:3.0\r\nFN:Someone\r\nBDAY:1990-01-01\r\nBDAY:--02-29\r\nEND:VCARD\r\n", stored: "0001-03-01", want: "1990-01-01"},
		{uid: "grouped-first", card: "BEGIN:VCARD\r\nVERSION:3.0\r\nFN:Someone\r\nitem1.BDAY;VALUE=date:--0229\r\nBDAY:--01-01\r\nEND:VCARD\r\n", stored: "0001-03-01", want: "0004-02-29"},
		{uid: "valid-then-invalid", card: "BEGIN:VCARD\r\nVERSION:3.0\r\nFN:Someone\r\nBDAY:--02-29\r\nBDAY:not a date\r\nEND:VCARD\r\n", stored: "0001-03-01", want: "0004-02-29"},

		// A date-time keeps its date; a text value is no date at all.
		{uid: "yearless-date-time", card: card(":--02-29T00:00:00"), stored: "0001-03-01", want: "0004-02-29"},
		{uid: "apple-date-time", card: card(";X-APPLE-OMIT-YEAR=1604:1604-02-29T00:00:00Z"), stored: "1604-02-29", want: "0004-02-29"},
		{uid: "value-text", card: card(";VALUE=text:--02-29"), stored: "0001-03-01", want: "0001-03-01"},

		// When the first BDAY does not parse there is no birthday to derive, and
		// the stored one is left rather than discarded.
		{uid: "invalid-then-valid", card: "BEGIN:VCARD\r\nVERSION:3.0\r\nFN:Someone\r\nBDAY:--13-45\r\nBDAY:--02-29\r\nEND:VCARD\r\n", stored: "0001-03-01", want: "0001-03-01"},

		// Nothing in the card parses, so there is nothing to re-derive from.
		{uid: "unparseable", card: card(":--02-30"), stored: "0001-03-02", want: "0001-03-02"},
	}
	// The upgrade has to land on the birthday a write of the same card stores
	// today, or the next PUT of an unchanged card would move it again.
	for _, contact := range seeded {
		_, _, parsed := parseVCardFields(contact.card)
		want := contact.stored
		if parsed != nil {
			want = parsed.Format("2006-01-02")
		}
		if contact.want != want {
			t.Fatalf("%s: case wants %s, but parseVCardFields stores %s", contact.uid, contact.want, want)
		}
	}
	for _, contact := range seeded {
		if _, err := pool.Exec(`INSERT INTO contacts (address_book_id, uid, resource_name, raw_vcard, etag, birthday) VALUES ($1, $2, $2, $3, 'etag', $4)`,
			bookID, contact.uid, contact.card, contact.stored); err != nil {
			t.Fatalf("seed %s: %v", contact.uid, err)
		}
	}
	ctag := func() int64 {
		t.Helper()
		var value int64
		if err := pool.QueryRow(`SELECT ctag FROM address_books WHERE id=$1`, bookID).Scan(&value); err != nil {
			t.Fatalf("read ctag: %v", err)
		}
		return value
	}
	birthdays := func() map[string]string {
		t.Helper()
		rows, err := pool.Query(`SELECT uid, to_char(birthday, 'YYYY-MM-DD') FROM contacts WHERE address_book_id=$1`, bookID)
		if err != nil {
			t.Fatalf("read birthdays: %v", err)
		}
		defer rows.Close()
		got := make(map[string]string)
		for rows.Next() {
			var uid, birthday string
			if err := rows.Scan(&uid, &birthday); err != nil {
				t.Fatalf("scan birthday: %v", err)
			}
			got[uid] = birthday
		}
		return got
	}

	syncToken := func() time.Time {
		t.Helper()
		var value time.Time
		if err := pool.QueryRow(`SELECT updated_at FROM address_books WHERE id=$1`, bookID).Scan(&value); err != nil {
			t.Fatalf("read sync token: %v", err)
		}
		return value
	}

	seededCTag := ctag()
	seededToken := syncToken()
	applyV120Migration(t, pool)
	before := ctag()
	if token := syncToken(); !token.After(seededToken) {
		t.Errorf("address book sync token after re-deriving birthdays = %s, want it moved past %s", token, seededToken)
	}
	var unreported int
	if err := pool.QueryRow(`SELECT count(*) FROM contacts WHERE address_book_id=$1 AND uid IN ('leap', 'apple', 'cr-only', 'dated-then-yearless') AND last_modified <= $2`,
		bookID, seededToken).Scan(&unreported); err != nil {
		t.Fatalf("read last_modified: %v", err)
	}
	if unreported != 0 {
		t.Errorf("%d re-derived contacts kept a last_modified at or before the old sync token, so a sync from it misses them", unreported)
	}
	got := birthdays()
	for _, contact := range seeded {
		if got[contact.uid] != contact.want {
			t.Errorf("%s birthday after upgrade = %s, want %s", contact.uid, got[contact.uid], contact.want)
		}
	}
	if before <= seededCTag {
		t.Errorf("address book CTag after re-deriving birthdays = %d, want it moved past %d", before, seededCTag)
	}

	applyV120Migration(t, pool)
	if after := ctag(); after != before {
		t.Errorf("repeating the upgrade moved the CTag from %d to %d", before, after)
	}
	if again := birthdays(); fmt.Sprint(again) != fmt.Sprint(got) {
		t.Errorf("repeating the upgrade changed birthdays from %v to %v", got, again)
	}
}

// The birthday backfill reads every contact with a birthday inside the upgrade
// transaction. Each card has to be unfolded, split and matched once: PostgreSQL
// flattens a subquery with no FROM into the expressions that read it, and a
// regex column referenced from ten places is then evaluated ten times per row.
func TestPostgres_V120MigrationParsesEachBirthdayOnce(t *testing.T) {
	pool := newV119Pool(t)
	applyV120Migration(t, pool)

	plan := explainStatement(t, pool, migrationStatement(t, "UPDATE contacts AS contact\n"), false)
	for _, call := range []string{"regexp_replace(", "regexp_split_to_table(unfolded.body", "regexp_match(line.text", "regexp_match(first_bday.value"} {
		if evaluations := countInPlan(plan, call); evaluations != 1 {
			t.Errorf("the birthday backfill plan evaluates %s %d times, want once", call, evaluations)
		}
	}
}
