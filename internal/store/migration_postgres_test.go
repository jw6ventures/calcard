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

	"github.com/jw6ventures/jw6-go-utils/database/migration"

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

// migrateWithRunner migrates pool the way the server does at startup:
// MigrateDatabase applies, in version order and each in its own transaction,
// the files in migrationsDir above the database's stamped version and up to
// appVersion, then stamps appVersion.
func migrateWithRunner(t *testing.T, pool *sql.DB, migrationsDir, appVersion string) {
	t.Helper()
	runner := migration.NewManager(pool, migrationsDir, appVersion)
	if err := runner.Initialize(); err != nil {
		t.Fatalf("initialize migrations for %s: %v", appVersion, err)
	}
	if err := runner.MigrateDatabase(); err != nil {
		t.Fatalf("migrate to %s: %v", appVersion, err)
	}
}

// newReleaseInstallPool installs a release the way its server did on first
// start: its baseline schema, then the migrations it shipped, through the
// runner, which stamps the release's own version whatever those files reached.
// Every release up to v1.1.9 ships migrations only through v1.1.4.sql.
func newReleaseInstallPool(t *testing.T, schemaPath, releaseVersion string) *sql.DB {
	t.Helper()
	pool := newPostgresSchemaPool(t, schemaPath)
	shipped := t.TempDir()
	for _, name := range []string{"v1.0.11.sql", "v1.0.12.sql", "v1.1.3.sql", "v1.1.4.sql"} {
		contents, err := os.ReadFile(filepath.Join("..", "..", "migrations", name))
		if err != nil {
			t.Fatalf("read %s: %v", name, err)
		}
		if err := os.WriteFile(filepath.Join(shipped, name), contents, 0o600); err != nil {
			t.Fatalf("write %s: %v", name, err)
		}
	}
	migrateWithRunner(t, pool, shipped, releaseVersion)
	return pool
}

// newV119ReleasePool builds the database an installation of the v1.1.9 release
// has: stamped v1.1.9 over a schema that stops at v1.1.4.sql.
func newV119ReleasePool(t *testing.T) *sql.DB {
	t.Helper()
	return newReleaseInstallPool(t, filepath.Join("testdata", "schema_v1.1.9_release.sql"), "v1.1.9")
}

// schemaCatalog lists every table, column, constraint, index, trigger and
// function in the pool's schema, one whitespace-normalized definition each with
// the schema qualifier removed, so an upgraded database and a fresh install
// compare directly. Column position is left out: an upgrade appends a column
// that a fresh install declares in place.
func schemaCatalog(t *testing.T, pool *sql.DB) map[string]bool {
	t.Helper()
	var schema string
	if err := pool.QueryRow(`SELECT current_schema()`).Scan(&schema); err != nil {
		t.Fatalf("read schema: %v", err)
	}
	rows, err := pool.Query(`
SELECT 'table ' || c.relname
FROM pg_class c
WHERE c.relnamespace = current_schema()::regnamespace AND c.relkind = 'r'
UNION ALL
SELECT 'column ' || c.relname || '.' || a.attname || ' ' || format_type(a.atttypid, a.atttypmod)
       || CASE WHEN a.attnotnull THEN ' NOT NULL' ELSE '' END
       || COALESCE(CASE WHEN a.attgenerated = 's' THEN ' GENERATED ' ELSE ' DEFAULT ' END
                   || pg_get_expr(d.adbin, d.adrelid), '')
FROM pg_attribute a
JOIN pg_class c ON c.oid = a.attrelid
LEFT JOIN pg_attrdef d ON d.adrelid = a.attrelid AND d.adnum = a.attnum
WHERE c.relnamespace = current_schema()::regnamespace AND c.relkind = 'r'
  AND a.attnum > 0 AND NOT a.attisdropped
UNION ALL
SELECT 'constraint ' || c.relname || '.' || con.conname || ' ' || pg_get_constraintdef(con.oid)
FROM pg_constraint con
JOIN pg_class c ON c.oid = con.conrelid
WHERE con.connamespace = current_schema()::regnamespace
UNION ALL
SELECT 'index ' || pg_get_indexdef(i.indexrelid)
FROM pg_index i
JOIN pg_class c ON c.oid = i.indexrelid
WHERE c.relnamespace = current_schema()::regnamespace
UNION ALL
SELECT 'trigger ' || pg_get_triggerdef(t.oid)
FROM pg_trigger t
JOIN pg_class c ON c.oid = t.tgrelid
WHERE c.relnamespace = current_schema()::regnamespace AND NOT t.tgisinternal
UNION ALL
SELECT 'function ' || pg_get_functiondef(p.oid)
FROM pg_proc p
WHERE p.pronamespace = current_schema()::regnamespace`)
	if err != nil {
		t.Fatalf("read schema catalog: %v", err)
	}
	defer rows.Close()
	catalog := make(map[string]bool)
	for rows.Next() {
		var definition string
		if err := rows.Scan(&definition); err != nil {
			t.Fatalf("scan schema catalog: %v", err)
		}
		definition = strings.ReplaceAll(definition, schema+".", "")
		catalog[strings.Join(strings.Fields(definition), " ")] = true
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("read schema catalog: %v", err)
	}
	return catalog
}

// Every installation a release can have left behind has to reach, through the
// runner the server starts with, the schema a fresh install creates. The runner
// applies only the files above the stamped version, and the stamp does not say
// which files a release carried: v1.1.6, v1.1.7 and v1.1.9 stamp their own
// version over a schema that stops at v1.1.4.sql.
func TestPostgres_UpgradeThroughRunnerMatchesFreshInstall(t *testing.T) {
	const migrationsDir = "../../migrations"
	fresh := newPostgresPool(t)
	migrateWithRunner(t, fresh, migrationsDir, "v1.2.0")
	want := schemaCatalog(t, fresh)

	for _, test := range []struct {
		name    string
		install func(*testing.T) *sql.DB
	}{
		{name: "v1.1.5", install: func(t *testing.T) *sql.DB {
			return newReleaseInstallPool(t, filepath.Join("testdata", "schema_v1.0.13.sql"), "v1.1.5")
		}},
		{name: "v1.1.6", install: func(t *testing.T) *sql.DB {
			return newReleaseInstallPool(t, filepath.Join("testdata", "schema_v1.1.9_release.sql"), "v1.1.6")
		}},
		{name: "v1.1.7", install: func(t *testing.T) *sql.DB {
			return newReleaseInstallPool(t, filepath.Join("testdata", "schema_v1.1.9_release.sql"), "v1.1.7")
		}},
		{name: "v1.1.9", install: newV119ReleasePool},
	} {
		t.Run(test.name, func(t *testing.T) {
			pool := test.install(t)
			migrateWithRunner(t, pool, migrationsDir, "v1.2.0")

			got := schemaCatalog(t, pool)
			for definition := range want {
				if !got[definition] {
					t.Errorf("the upgrade lacks %s", definition)
				}
			}
			for definition := range got {
				if !want[definition] {
					t.Errorf("the upgrade leaves %s, which a fresh install does not have", definition)
				}
			}
		})
	}
}

// Before ace_order existed a denial suppressed every grant on the resource, no
// matter which was written first. Ordered evaluation answers with the first
// matching ACE instead, so the upgrade decides what the stored rows mean: order
// them by age alone and a DAV:all grant written before a user-specific denial
// starts answering first, turning a principal the installation had denied into
// one it admits.
func TestPostgres_V120MigrationPreservesExistingDenials(t *testing.T) {
	pool := newV119ReleasePool(t)

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
	pool := newV119ReleasePool(t)

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
	pool := newV119ReleasePool(t)

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
// concerned (ical.ConservativeRecurrenceBounds counts a RECURRENCE-ID). Left
// with NULL bounds, the candidate filter COALESCEs to the first component's
// dtstart/dtend and drops the row before the matcher ever sees the other
// instances, so the upgrade has to bound it or its June instance is lost.
func TestPostgres_V120MigrationRepairsDetachedRecurrenceBounds(t *testing.T) {
	pool := newV119ReleasePool(t)
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

	// Stored the way v1.1.9 stores it: the first component's dates, and no
	// recurrence bounds, because v1.1.9 has no columns for them.
	const detached = "BEGIN:VCALENDAR\r\nVERSION:2.0\r\n" +
		"BEGIN:VEVENT\r\nUID:detached\r\nRECURRENCE-ID:20260115T100000Z\r\nDTSTART:20260115T100000Z\r\nDTEND:20260115T110000Z\r\nSUMMARY:January\r\nEND:VEVENT\r\n" +
		"BEGIN:VEVENT\r\nUID:detached\r\nRECURRENCE-ID:20260615T100000Z\r\nDTSTART:20260615T100000Z\r\nDTEND:20260615T110000Z\r\nSUMMARY:June\r\nEND:VEVENT\r\n" +
		"END:VCALENDAR\r\n"
	if _, err := pool.Exec(`
INSERT INTO events (calendar_id, uid, resource_name, raw_ical, etag, summary, dtstart, dtend, all_day, last_modified)
VALUES ($1, 'detached', 'detached.ics', $2, 'etag', 'January',
        '2026-01-15T10:00:00Z', '2026-01-15T11:00:00Z', FALSE, NOW())`,
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

// An installation of the v1.1.9 release has neither the recurrence columns nor
// the ACL join key, so the upgrade adds both and fills what they hold: every
// resource that recurs -- through RRULE, RDATE, or detached instances alone --
// gets the open-ended bounds while a one-off stays NULL, and the stored ACL is
// ordered deny-first across its path spellings. The first release carrying
// display names is this one too, so the columns an OAuth login writes exist.
func TestPostgres_UpgradeFromV119ReleaseFillsTheColumnsItAdds(t *testing.T) {
	pool := newV119ReleasePool(t)

	var calendarID int64
	if err := pool.QueryRow(`
WITH seeded_user AS (
    INSERT INTO users (oauth_subject, primary_email) VALUES ('subject-v119', 'v119@example.test')
    RETURNING id
)
INSERT INTO calendars (user_id, name) SELECT id, 'Release' FROM seeded_user RETURNING id`).
		Scan(&calendarID); err != nil {
		t.Fatalf("seed user and calendar: %v", err)
	}

	seed := func(uid, component string) {
		t.Helper()
		raw := "BEGIN:VCALENDAR\r\nVERSION:2.0\r\n" + component + "END:VCALENDAR\r\n"
		if _, err := pool.Exec(`
INSERT INTO events (calendar_id, uid, resource_name, raw_ical, etag, dtstart, dtend, all_day, last_modified)
VALUES ($1, $2, $2 || '.ics', $3, 'etag', '2026-01-15T10:00:00Z', '2026-01-15T11:00:00Z', FALSE, NOW())`,
			calendarID, uid, raw); err != nil {
			t.Fatalf("seed %s: %v", uid, err)
		}
	}
	seed("rrule", "BEGIN:VEVENT\r\nUID:rrule\r\nDTSTART:20260115T100000Z\r\nDTEND:20260115T110000Z\r\nRRULE:FREQ=MONTHLY\r\nEND:VEVENT\r\n")
	seed("rdate", "BEGIN:VTODO\r\nUID:rdate\r\nDTSTART:20260115T100000Z\r\nRDATE:20260615T100000Z\r\nEND:VTODO\r\n")
	seed("detached",
		"BEGIN:VEVENT\r\nUID:detached\r\nRECURRENCE-ID:20260115T100000Z\r\nDTSTART:20260115T100000Z\r\nDTEND:20260115T110000Z\r\nEND:VEVENT\r\n"+
			"BEGIN:VEVENT\r\nUID:detached\r\nRECURRENCE-ID:20260615T100000Z\r\nDTSTART:20260615T100000Z\r\nDTEND:20260615T110000Z\r\nEND:VEVENT\r\n")
	seed("single", "BEGIN:VEVENT\r\nUID:single\r\nDTSTART:20260115T100000Z\r\nDTEND:20260115T110000Z\r\nEND:VEVENT\r\n")

	objectPath := fmt.Sprintf("/dav/calendars/%d/rrule", calendarID)
	if _, err := pool.Exec(`
INSERT INTO acl_entries (resource_path, principal_href, is_grant, privilege, created_at) VALUES
    ($1 || '.ics', 'DAV:all',            TRUE,  'read', NOW() - INTERVAL '2 days'),
    ($1,           '/dav/principals/2/', FALSE, 'read', NOW() - INTERVAL '1 day')`, objectPath); err != nil {
		t.Fatalf("seed ACL: %v", err)
	}

	migrateWithRunner(t, pool, "../../migrations", "v1.2.0")

	for _, uid := range []string{"rrule", "rdate", "detached"} {
		var start, until *time.Time
		if err := pool.QueryRow(`SELECT recurrence_start, recurrence_until FROM events WHERE uid = $1`, uid).
			Scan(&start, &until); err != nil {
			t.Fatalf("read %s bounds: %v", uid, err)
		}
		if start == nil || until == nil ||
			!start.UTC().Equal(icalpkg.RecurrenceStartSentinel) || !until.UTC().Equal(icalpkg.RecurrenceUntilSentinel) {
			t.Errorf("%s bounds = %v, %v, want the open-ended sentinels", uid, start, until)
		}
	}
	var singleStart, singleUntil *time.Time
	if err := pool.QueryRow(`SELECT recurrence_start, recurrence_until FROM events WHERE uid = 'single'`).
		Scan(&singleStart, &singleUntil); err != nil {
		t.Fatalf("read single bounds: %v", err)
	}
	if singleStart != nil || singleUntil != nil {
		t.Errorf("non-recurring bounds = %v, %v, want both NULL", singleStart, singleUntil)
	}

	start := time.Date(2026, 6, 15, 0, 0, 0, 0, time.UTC)
	end := start.AddDate(0, 0, 1)
	events, err := New(pool).Events.ListForCalendarFiltered(t.Context(), calendarID, EventFilter{Start: &start, End: &end})
	if err != nil {
		t.Fatalf("ListForCalendarFiltered() error = %v", err)
	}
	if len(events) != 3 {
		t.Errorf("June candidates = %d, want the three recurring resources", len(events))
	}

	orders := map[string]int{}
	rows, err := pool.Query(`SELECT principal_href, ace_order FROM acl_entries WHERE resource_path_norm = $1`, objectPath)
	if err != nil {
		t.Fatalf("read ACL: %v", err)
	}
	defer rows.Close()
	for rows.Next() {
		var principal string
		var order int
		if err := rows.Scan(&principal, &order); err != nil {
			t.Fatalf("scan ACL: %v", err)
		}
		orders[principal] = order
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("read ACL: %v", err)
	}
	if len(orders) != 2 || orders["/dav/principals/2/"] != 0 || orders["DAV:all"] != 1 {
		t.Errorf("ace_order = %v, want the denial first across both path spellings", orders)
	}

	user, err := New(pool).Users.UpsertOAuthUser(t.Context(), "subject-v119", "v119@example.test", "Dana Lee", "Dana")
	if err != nil {
		t.Fatalf("UpsertOAuthUser() after the upgrade: %v", err)
	}
	if user.FullName != "Dana Lee" || user.FirstName != "Dana" {
		t.Errorf("UpsertOAuthUser() = %#v, want the profile names stored", user)
	}
}

// The Digest replay guard writes here on every authenticated request, so the
// upgrade has to leave the table claimable and the primary key has to be what
// rejects the second claim.
func TestPostgres_V120MigrationCreatesClaimableDigestNonceTable(t *testing.T) {
	pool := newV119ReleasePool(t)
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
	recurring bool
}

func recurrenceRepairCases() []recurrenceRepairCase {
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
	pool := newV119ReleasePool(t)
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
INSERT INTO events (calendar_id, uid, resource_name, raw_ical, etag, dtstart, dtend, all_day, last_modified)
VALUES ($1, $2, $2 || '.ics', $3, 'etag', '2026-01-15T10:00:00Z', '2026-01-15T11:00:00Z', FALSE, NOW())`,
			calendarID, test.uid, test.raw); err != nil {
			t.Fatalf("seed %s: %v", test.uid, err)
		}
	}

	readUIDs := func(q interface {
		Query(string, ...any) (*sql.Rows, error)
	}, query string) map[string]bool {
		t.Helper()
		rows, err := q.Query(query)
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
	// repair is about to write. v1.1.9 has no bound columns, so they are added the
	// way the migration first adds them, in a transaction that is rolled back.
	tx, err := pool.Begin()
	if err != nil {
		t.Fatalf("begin legacy predicate transaction: %v", err)
	}
	if _, err := tx.Exec(`ALTER TABLE events ADD COLUMN recurrence_start TIMESTAMPTZ, ADD COLUMN recurrence_until TIMESTAMPTZ`); err != nil {
		tx.Rollback()
		t.Fatalf("add recurrence bounds: %v", err)
	}
	legacy := readUIDs(tx, `SELECT uid FROM events WHERE `+legacyRecurrenceRepairPredicate)
	if err := tx.Rollback(); err != nil {
		t.Fatalf("roll back legacy predicate transaction: %v", err)
	}

	applyV120Migration(t, pool)

	// Every row enters the upgrade without bounds, so a row the repair touched is
	// one that has any.
	repaired := readUIDs(pool, `
SELECT uid FROM events
WHERE recurrence_start IS NOT NULL OR recurrence_until IS NOT NULL`)

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
	pool := newV119ReleasePool(t)
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
	pool := newV119ReleasePool(t)
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
	pool := newV119ReleasePool(t)
	calendarID := seedRepairCalendar(t, pool, "subject-backfill")

	const seededRows = 200
	if _, err := pool.Exec(`
INSERT INTO events (calendar_id, uid, resource_name, raw_ical, etag, dtstart, dtend, all_day, last_modified)
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
       'etag', '2026-01-15T10:00:00Z', '2026-01-15T11:00:00Z', FALSE, NOW()
FROM generate_series(1, $2) g`, calendarID, seededRows); err != nil {
		t.Fatalf("seed events: %v", err)
	}

	applyV120Migration(t, pool)

	// The repair reads the bound columns the migration adds, so its plan is taken
	// once they exist. The rows it rejected keep NULL bounds and reach it again.
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
	migrateWithRunner(t, pool, "../../migrations", "v1.2.0")
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

// A stored recurrence_until may be the end of the series' last instance as an
// expansion that ignores skipped local times and date UNTILs reads it. Two
// series end later under RFC 5545 than that:
// a DAILY or WEEKLY COUNT series in a zone whose transitions skip some of its
// local times, which leave those instances out uncounted (§3.3.10) and so push
// the COUNT-th instance a step later per skipped one, and a timed series whose
// UNTIL is a date, which includes that whole day. The migration reopens those
// bounds, or the time-range filter drops the instances past them, and leaves
// every bound both readings agree on alone.
func TestPostgres_V120MigrationReopensBoundsTheSeriesOutlast(t *testing.T) {
	t.Run("database collation", func(t *testing.T) {
		testV120MigrationReopensBoundsTheSeriesOutlast(t, "")
	})
	t.Run("C collation", func(t *testing.T) {
		testV120MigrationReopensBoundsTheSeriesOutlast(t, "C")
	})
}

func testV120MigrationReopensBoundsTheSeriesOutlast(t *testing.T, collation string) {
	pool := newV119ReleasePool(t)
	if collation != "" {
		if _, err := pool.Exec(fmt.Sprintf(`ALTER TABLE events ALTER COLUMN raw_ical TYPE TEXT COLLATE %q`, collation)); err != nil {
			t.Fatalf("set raw_ical collation: %v", err)
		}
	}
	// Stored bounds exist only where the columns do, as on a schema that
	// carries them.
	if _, err := pool.Exec(`ALTER TABLE events ADD COLUMN recurrence_start TIMESTAMPTZ, ADD COLUMN recurrence_until TIMESTAMPTZ`); err != nil {
		t.Fatalf("add recurrence bounds: %v", err)
	}
	calendarID := seedRepairCalendar(t, pool, "subject-outlast-"+collation)

	const newYork = "BEGIN:VTIMEZONE\r\nTZID:America/New_York\r\n" +
		"BEGIN:DAYLIGHT\r\nDTSTART:20070311T020000\r\nTZOFFSETFROM:-0500\r\nTZOFFSETTO:-0400\r\nRRULE:FREQ=YEARLY;BYMONTH=3;BYDAY=2SU\r\nEND:DAYLIGHT\r\n" +
		"BEGIN:STANDARD\r\nDTSTART:20071104T020000\r\nTZOFFSETFROM:-0400\r\nTZOFFSETTO:-0500\r\nRRULE:FREQ=YEARLY;BYMONTH=11;BYDAY=1SU\r\nEND:STANDARD\r\n" +
		"END:VTIMEZONE\r\n"
	body := func(uid, timezone, properties string) string {
		return "BEGIN:VCALENDAR\r\nVERSION:2.0\r\n" + timezone +
			"BEGIN:VEVENT\r\nUID:" + uid + "\r\n" + properties + "END:VEVENT\r\nEND:VCALENDAR\r\n"
	}
	finiteStart := time.Date(2026, 3, 1, 0, 0, 0, 0, time.UTC)
	finiteUntil := time.Date(2026, 5, 10, 0, 0, 0, 0, time.UTC)

	cases := []struct {
		uid     string
		raw     string
		bounded bool
		reopen  bool
	}{
		{uid: "count-weekly-tzid", bounded: true, reopen: true, raw: body("count-weekly-tzid", newYork,
			"DTSTART;TZID=America/New_York:20260301T023000\r\nDTEND;TZID=America/New_York:20260301T033000\r\nRRULE:FREQ=WEEKLY;COUNT=10\r\n")},
		{uid: "count-daily-tzid", bounded: true, reopen: true, raw: body("count-daily-tzid", newYork,
			"DTSTART;TZID=America/New_York:20260301T023000\r\nRRULE:FREQ=DAILY;INTERVAL=2;COUNT=30\r\n")},
		// Folds inside the property names, the TZID parameter and the rule.
		{uid: "count-weekly-tzid-folded", bounded: true, reopen: true, raw: body("count-weekly-tzid-folded", newYork,
			"DTST\r\n ART;TZ\r\n\tID=America/New_York:20260301T023000\r\nRRU\r\n LE:FREQ=WEE\r\n KLY;CO\r\n UNT=10\r\n")},
		// Lowercase names and values, the parts in another order, and a quoted
		// TZID after a quoted parameter holding a colon and a semicolon.
		{uid: "count-weekly-tzid-lower-quoted", bounded: true, reopen: true, raw: body("count-weekly-tzid-lower-quoted", newYork,
			"dtstart;x-label=\"a:b;c\";tzid=\"America/New_York\":20260301T023000\r\nrrule:count=10;freq=weekly\r\n")},
		// Text outside ASCII ahead of the TZID and of the rule parts.
		{uid: "count-weekly-tzid-non-ascii", bounded: true, reopen: true, raw: body("count-weekly-tzid-non-ascii", newYork,
			"SUMMARY:Café 😀\r\nDTSTART;X-LABEL=Zürich;TZID=America/New_York:20260301T023000\r\nRRULE;X-NOTE=Été:FREQ=WEEKLY;COUNT=10\r\n")},
		// A date UNTIL on a timed series, zoned or UTC.
		{uid: "date-until-tzid", bounded: true, reopen: true, raw: body("date-until-tzid", newYork,
			"DTSTART;TZID=America/New_York:20260301T100000\r\nRRULE:FREQ=WEEKLY;UNTIL=20260503\r\n")},
		{uid: "date-until-utc", bounded: true, reopen: true, raw: body("date-until-utc", "",
			"DTSTART:20260301T100000Z\r\nRRULE:FREQ=MONTHLY;UNTIL=20260501;BYMONTHDAY=1\r\n")},

		// No zone can skip a UTC or floating time.
		{uid: "count-weekly-utc", bounded: true, raw: body("count-weekly-utc", "",
			"DTSTART:20260301T073000Z\r\nRRULE:FREQ=WEEKLY;COUNT=10\r\n")},
		{uid: "count-weekly-floating", bounded: true, raw: body("count-weekly-floating", "",
			"DTSTART:20260301T023000\r\nRRULE:FREQ=WEEKLY;COUNT=10\r\n")},
		// A skipped instance only removes an instance from a series UNTIL ends.
		{uid: "until-tzid", bounded: true, raw: body("until-tzid", newYork,
			"DTSTART;TZID=America/New_York:20260301T023000\r\nRRULE:FREQ=WEEKLY;UNTIL=20260503T063000Z\r\n")},
		// A date UNTIL on an all-day series, beside a VTIMEZONE whose
		// observances start at a time of day.
		{uid: "date-until-all-day", bounded: true, raw: body("date-until-all-day", newYork,
			"DTSTART;VALUE=DATE:20260301\r\nRRULE:FREQ=WEEKLY;UNTIL=20260503\r\n")},
		{uid: "plain", raw: body("plain", newYork,
			"DTSTART;TZID=America/New_York:20260301T100000\r\nDTEND;TZID=America/New_York:20260301T110000\r\n")},
	}
	for _, test := range cases {
		var start, until any
		if test.bounded {
			start, until = finiteStart, finiteUntil
		}
		if _, err := pool.Exec(`
INSERT INTO events (calendar_id, uid, resource_name, raw_ical, etag, dtstart, dtend, all_day, last_modified, recurrence_start, recurrence_until)
VALUES ($1, $2, $2 || '.ics', $3, 'etag', '2026-03-01T07:30:00Z', '2026-03-01T08:30:00Z', FALSE, NOW(), $4, $5)`,
			calendarID, test.uid, test.raw, start, until); err != nil {
			t.Fatalf("seed %s: %v", test.uid, err)
		}
	}

	// The second run is the by-hand application to a database already stamped
	// v1.2.0, which must leave what the first run wrote as it is.
	for run := 1; run <= 2; run++ {
		applyV120Migration(t, pool)

		for _, test := range cases {
			var start, until *time.Time
			if err := pool.QueryRow(`SELECT recurrence_start, recurrence_until FROM events WHERE uid = $1`, test.uid).
				Scan(&start, &until); err != nil {
				t.Fatalf("run %d: read %s bounds: %v", run, test.uid, err)
			}
			switch {
			case !test.bounded:
				if start != nil || until != nil {
					t.Errorf("run %d: %s bounds = %v, %v, want NULL for a resource that does not recur", run, test.uid, start, until)
				}
			case test.reopen:
				if until == nil || !until.UTC().Equal(icalpkg.RecurrenceUntilSentinel) {
					t.Errorf("run %d: %s recurrence_until = %v, want the open-ended %s", run, test.uid, until, icalpkg.RecurrenceUntilSentinel)
				}
				if start == nil || !start.UTC().Equal(finiteStart) {
					t.Errorf("run %d: %s recurrence_start = %v, want %s kept", run, test.uid, start, finiteStart)
				}
			default:
				if start == nil || until == nil || !start.UTC().Equal(finiteStart) || !until.UTC().Equal(finiteUntil) {
					t.Errorf("run %d: %s bounds = %v, %v, want %s, %s kept", run, test.uid, start, until, finiteStart, finiteUntil)
				}
			}
		}
	}

	// The statement reads every row of events under ACCESS EXCLUSIVE, so a body
	// whose row holds no finite bound must not be unfolded at all.
	finite := 0
	for _, test := range cases {
		if test.bounded && !test.reopen {
			finite++
		}
	}
	unfolds := 0
	walkPlan(explainStatement(t, pool, migrationStatement(t, "UPDATE events\n    SET recurrence_until"), true), func(node map[string]any) {
		call, _ := node["Function Call"].(string)
		if node["Node Type"] != "Function Scan" || !strings.HasPrefix(call, "regexp_replace(") {
			return
		}
		loops, _ := node["Actual Loops"].(float64)
		unfolds++
		if int(loops) != finite {
			t.Errorf("%s ran %v times, want once per row holding a finite bound (%d)", call, loops, finite)
		}
	})
	if unfolds != 1 {
		t.Errorf("the plan unfolds each body %d times, want once", unfolds)
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
                    'idx_deleted_resources_keyset', 'idx_deleted_resources_lookup',
                    'idx_events_last_modified', 'idx_contacts_last_modified',
                    'idx_events_change_stream', 'idx_contacts_change_stream',
                    'idx_deleted_resources_change_stream',
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
// v1.1.9 database, which has none of the keyset indexes and leads its stamp
// indexes on the stamp alone, and a database that ran a v1.2.0 release
// candidate, which built the keyset indexes on other columns.
func TestPostgres_V120MigrationIndexesMatchBaselineSchema(t *testing.T) {
	want := v120IndexDefinitions(t, newPostgresPool(t))
	if len(want) != 7 {
		t.Fatalf("db.sql carries %d of the migrated indexes, want 7: %v", len(want), want)
	}

	for _, test := range []struct {
		name  string
		setup string
	}{
		{name: "from v1.1.9"},
		{name: "from a release candidate", setup: `
CREATE INDEX idx_events_calendar_keyset ON events (calendar_id, id);
CREATE INDEX idx_contacts_book_keyset ON contacts (address_book_id, id, last_modified);
CREATE INDEX idx_deleted_resources_keyset ON deleted_resources (resource_type, collection_id, id);`},
	} {
		t.Run(test.name, func(t *testing.T) {
			pool := newV119ReleasePool(t)
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
// so no index costs more than one build. None is built twice. An index the
// upgrade retires goes before any table is rewritten or updated, so no rewrite
// rebuilds it and no backfill maintains it. The recurrence repair and the
// ace_order backfill run while no index they would maintain per row and the file
// builds anyway exists yet.
func TestV120MigrationMaintainsNoIndexItRebuildsOrRetires(t *testing.T) {
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

	repair, aceOrderBackfill, firstWrite := -1, -1, -1
	for i, statement := range statements {
		if strings.HasPrefix(statement, "UPDATE events\n") {
			repair = i
		}
		if strings.Contains(statement, "UPDATE acl_entries") {
			aceOrderBackfill = i
		}
		if firstWrite < 0 && (strings.HasPrefix(statement, "ALTER TABLE") || strings.HasPrefix(statement, "UPDATE ")) {
			firstWrite = i
		}
	}
	if repair < 0 {
		t.Fatal("the migration no longer carries the recurrence repair")
	}
	if aceOrderBackfill < 0 {
		t.Fatal("the migration no longer carries the ace_order backfill")
	}

	dropped := map[string]int{}
	for i, statement := range statements {
		if name, ok := strings.CutPrefix(statement, "DROP INDEX IF EXISTS "); ok {
			dropped[name] = i
		}
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
		if strings.Contains(statement, "ON acl_entries") && i < aceOrderBackfill {
			t.Errorf("%s is built before the ace_order backfill updates every row it covers", name)
		}
	}

	for name, at := range dropped {
		if _, rebuilt := built[name]; !rebuilt && at > firstWrite {
			t.Errorf("%s is retired only after a table is rewritten or updated, which maintains it until then", name)
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

	pool := newV119ReleasePool(t)
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
	pool := newV119ReleasePool(t)
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
	pool := newV119ReleasePool(t)
	applyV120Migration(t, pool)

	plan := explainStatement(t, pool, migrationStatement(t, "UPDATE contacts AS contact\n"), false)
	for _, call := range []string{"regexp_replace(", "regexp_split_to_table(unfolded.body", "regexp_match(line.text", "regexp_match(first_bday.value"} {
		if evaluations := countInPlan(plan, call); evaluations != 1 {
			t.Errorf("the birthday backfill plan evaluates %s %d times, want once", call, evaluations)
		}
	}
}

// A stored dtend may be a DTEND as written, even one that cannot end the
// event, which the reports read as though it were not there, or none for an
// event without a DTEND. A dtend before dtstart drops the row from the
// time-range narrowing before the RFC 4791 §9.9 test can match it, and a
// missing one keeps the row a candidate for every range. The migration stores
// the end a write of the same body stores (icalpkg.StoredEventEnd): a
// day after an all-day start, the timed start itself, and none where a
// DURATION decides the end. A row whose body carries a DTEND its dtend does not
// hold keeps none. Every other row keeps its dtend, and a second run changes
// nothing.
func TestPostgres_V120MigrationStoresTheEndAnUnusableDTENDLeaves(t *testing.T) {
	pool := newV119ReleasePool(t)
	calendarID := seedRepairCalendar(t, pool, "subject-unusable-dtend")
	at := func(value string) time.Time {
		parsed, err := time.Parse(time.RFC3339, value)
		if err != nil {
			t.Fatal(err)
		}
		return parsed
	}
	body := func(uid, properties string) string {
		return "BEGIN:VCALENDAR\r\nVERSION:2.0\r\nBEGIN:VEVENT\r\nUID:" + uid + "\r\n" + properties + "END:VEVENT\r\nEND:VCALENDAR\r\n"
	}
	cases := []struct {
		uid            string
		properties     string
		allDay         bool
		dtstart, dtend *time.Time
		want           *time.Time
	}{
		{uid: "timed-before", properties: "DTSTART:20250301T100000Z\r\nDTEND:20250301T080000Z\r\n",
			dtstart: ptr(at("2025-03-01T10:00:00Z")), dtend: ptr(at("2025-03-01T08:00:00Z")), want: ptr(at("2025-03-01T10:00:00Z"))},
		{uid: "date-equal", properties: "DTSTART;VALUE=DATE:20250301\r\nDTEND;VALUE=DATE:20250301\r\n", allDay: true,
			dtstart: ptr(at("2025-03-01T00:00:00Z")), dtend: ptr(at("2025-03-01T00:00:00Z")), want: ptr(at("2025-03-02T00:00:00Z"))},
		{uid: "date-before", properties: "DTSTART:20250301\r\nDTEND:20250226\r\n", allDay: true,
			dtstart: ptr(at("2025-03-01T00:00:00Z")), dtend: ptr(at("2025-02-26T00:00:00Z")), want: ptr(at("2025-03-02T00:00:00Z"))},
		// A folded DURATION decides the end, so none is stored.
		{uid: "timed-before-duration", properties: "DTSTART:20250301T100000Z\r\nDTEND:20250301T080000Z\r\nDURA\r\n TION:PT3H\r\n",
			dtstart: ptr(at("2025-03-01T10:00:00Z")), dtend: ptr(at("2025-03-01T08:00:00Z"))},
		{uid: "recurring-date-equal", properties: "DTSTART;VALUE=DATE:20250301\r\nDTEND;VALUE=DATE:20250301\r\nRRULE:FREQ=WEEKLY;COUNT=3\r\n", allDay: true,
			dtstart: ptr(at("2025-03-01T00:00:00Z")), dtend: ptr(at("2025-03-01T00:00:00Z")), want: ptr(at("2025-03-02T00:00:00Z"))},
		// No DTEND at all leaves the same end, stored as none.
		{uid: "no-dtend", properties: "DTSTART:20250301T100000Z\r\n",
			dtstart: ptr(at("2025-03-01T10:00:00Z")), want: ptr(at("2025-03-01T10:00:00Z"))},
		{uid: "date-no-dtend", properties: "DTSTART;VALUE=DATE:20250301\r\n", allDay: true,
			dtstart: ptr(at("2025-03-01T00:00:00Z")), want: ptr(at("2025-03-02T00:00:00Z"))},
		{uid: "recurring-date-no-dtend", properties: "DTSTART;VALUE=DATE:20250301\r\nRRULE:FREQ=WEEKLY\r\n", allDay: true,
			dtstart: ptr(at("2025-03-01T00:00:00Z")), want: ptr(at("2025-03-02T00:00:00Z"))},

		// Rows a write stores as they are.
		{uid: "timed", properties: "DTSTART:20250301T100000Z\r\nDTEND:20250301T110000Z\r\n",
			dtstart: ptr(at("2025-03-01T10:00:00Z")), dtend: ptr(at("2025-03-01T11:00:00Z")), want: ptr(at("2025-03-01T11:00:00Z"))},
		{uid: "timed-equal", properties: "DTSTART:20250301T100000Z\r\nDTEND:20250301T100000Z\r\n",
			dtstart: ptr(at("2025-03-01T10:00:00Z")), dtend: ptr(at("2025-03-01T10:00:00Z")), want: ptr(at("2025-03-01T10:00:00Z"))},
		{uid: "date", properties: "DTSTART;VALUE=DATE:20250301\r\nDTEND;VALUE=DATE:20250302\r\n", allDay: true,
			dtstart: ptr(at("2025-03-01T00:00:00Z")), dtend: ptr(at("2025-03-02T00:00:00Z")), want: ptr(at("2025-03-02T00:00:00Z"))},
		{uid: "duration", properties: "DTSTART:20250301T100000Z\r\nDURATION:PT1H\r\n", dtstart: ptr(at("2025-03-01T10:00:00Z"))},
		{uid: "date-duration", properties: "DTSTART;VALUE=DATE:20250301\r\nDURA\r\n TION:P2D\r\n", allDay: true,
			dtstart: ptr(at("2025-03-01T00:00:00Z"))},
		// A DTEND whose dtend is missing was not read when the row was
		// stored, so the row keeps no bound, and the §9.9 test, which can
		// read it, decides.
		{uid: "unread-dtend", properties: "DTSTART:20250301T100000Z\r\nDTEND;TZID=\"(UTC-05:00) Eastern\":20250301T080000\r\n",
			dtstart: ptr(at("2025-03-01T10:00:00Z"))},
		{uid: "folded-unread-dtend", properties: "DTSTART;VALUE=DATE:20250301\r\nDT\r\n END;VALUE=DATE:2025-03-03\r\n", allDay: true,
			dtstart: ptr(at("2025-03-01T00:00:00Z"))},
		{uid: "recurring", properties: "DTSTART:20250301T100000Z\r\nDTEND:20250301T110000Z\r\nRRULE:FREQ=DAILY\r\n",
			dtstart: ptr(at("2025-03-01T10:00:00Z")), dtend: ptr(at("2025-03-01T11:00:00Z")), want: ptr(at("2025-03-01T11:00:00Z"))},
		{uid: "no-dtstart", properties: "DTEND:20250301T080000Z\r\n", dtend: ptr(at("2025-03-01T08:00:00Z")), want: ptr(at("2025-03-01T08:00:00Z"))},
	}
	for _, test := range cases {
		if _, err := pool.Exec(`
INSERT INTO events (calendar_id, uid, resource_name, raw_ical, etag, dtstart, dtend, all_day, last_modified)
VALUES ($1, $2, $2 || '.ics', $3, 'etag', $4, $5, $6, NOW())`,
			calendarID, test.uid, body(test.uid, test.properties), test.dtstart, test.dtend, test.allDay); err != nil {
			t.Fatalf("seed %s: %v", test.uid, err)
		}
	}

	// From 08:30 on 1 March only the rows ending after it are candidates: the
	// ones a dtend before dtstart dropped are among them once repaired.
	rangeStart := at("2025-03-01T08:30:00Z")
	repaired := []string{"timed-before", "date-equal", "date-before", "timed-before-duration"}
	candidates := func() map[string]bool {
		t.Helper()
		events, err := New(pool).Events.ListForCalendarFiltered(t.Context(), calendarID, EventFilter{Start: &rangeStart})
		if err != nil {
			t.Fatalf("ListForCalendarFiltered() error = %v", err)
		}
		found := map[string]bool{}
		for _, event := range events {
			found[event.UID] = true
		}
		return found
	}
	// This schema has no recurrence bounds, so the narrowing reads dtend
	// alone, as it does for a row that does not recur.
	for _, uid := range repaired {
		var candidate bool
		if err := pool.QueryRow(`SELECT COALESCE(dtend, 'infinity'::timestamptz) >= $2 FROM events WHERE uid = $1`, uid, rangeStart).
			Scan(&candidate); err != nil {
			t.Fatalf("read %s: %v", uid, err)
		}
		if candidate {
			t.Fatalf("%s is a candidate before the upgrade; the fixture no longer shows the defect", uid)
		}
	}

	migrateWithRunner(t, pool, "../../migrations", "v1.2.0")
	for run := 1; run <= 2; run++ {
		if run == 2 {
			applyV120Migration(t, pool)
		}
		for _, test := range cases {
			var dtend *time.Time
			if err := pool.QueryRow(`SELECT dtend FROM events WHERE uid = $1`, test.uid).Scan(&dtend); err != nil {
				t.Fatalf("run %d: read %s: %v", run, test.uid, err)
			}
			if (dtend == nil) != (test.want == nil) || dtend != nil && !dtend.Equal(*test.want) {
				t.Errorf("run %d: %s dtend = %v, want %v", run, test.uid, dtend, test.want)
			}
		}
		after := candidates()
		for _, uid := range repaired {
			if !after[uid] {
				t.Errorf("run %d: %s is not a time-range candidate from %s", run, uid, rangeStart)
			}
		}
	}
}

// Every update of a member row moves its collection's CTag through a row
// trigger, and each move writes a new version of the one collection row the
// next move has to reach, so repairing n rows of one calendar row by row costs
// time quadratic in n. The repairs move each collection they change once, after
// its last row, and leave its sync token at or past every member stamp they
// wrote, so a client syncing from that token is not told of the repaired rows
// again. A collection they do not change keeps its CTag and token.
func TestPostgres_V120MigrationMovesEachRepairedCollectionOnce(t *testing.T) {
	const perRepair = 40
	pool := newV119ReleasePool(t)
	repairedID := seedRepairCalendar(t, pool, "subject-move-once")
	var untouchedID, bookID int64
	if err := pool.QueryRow(`INSERT INTO calendars (user_id, name) SELECT user_id, 'Untouched' FROM calendars WHERE id = $1 RETURNING id`,
		repairedID).Scan(&untouchedID); err != nil {
		t.Fatalf("seed untouched calendar: %v", err)
	}
	if err := pool.QueryRow(`INSERT INTO address_books (user_id, name) SELECT user_id, 'Birthdays' FROM calendars WHERE id = $1 RETURNING id`,
		repairedID).Scan(&bookID); err != nil {
		t.Fatalf("seed address book: %v", err)
	}

	const eventBody = `E'BEGIN:VCALENDAR\r\nVERSION:2.0\r\nBEGIN:VEVENT\r\nUID:' || $2 || n || E'\r\n' || $3 || E'END:VEVENT\r\nEND:VCALENDAR\r\n'`
	seedEvents := func(calendarID int64, prefix, properties string, dtend any) {
		t.Helper()
		if _, err := pool.Exec(`
INSERT INTO events (calendar_id, uid, resource_name, raw_ical, etag, dtstart, dtend, all_day, last_modified)
SELECT $1, $2 || n, $2 || n || '.ics', `+eventBody+`, 'etag', '2025-03-01T10:00:00Z', $4, FALSE, NOW()
FROM generate_series(1, $5) AS n`,
			calendarID, prefix, properties, dtend, perRepair); err != nil {
			t.Fatalf("seed %s events: %v", prefix, err)
		}
	}
	seedEvents(repairedID, "recurring-", "DTSTART:20250301T100000Z\r\nDTEND:20250301T110000Z\r\nRRULE:FREQ=DAILY\r\n", "2025-03-01T11:00:00Z")
	seedEvents(repairedID, "no-dtend-", "DTSTART:20250301T100000Z\r\n", nil)
	seedEvents(repairedID, "dtend-before-", "DTSTART:20250301T100000Z\r\nDTEND:20250301T080000Z\r\n", "2025-03-01T08:00:00Z")
	seedEvents(untouchedID, "plain-", "DTSTART:20250301T100000Z\r\nDTEND:20250301T110000Z\r\n", "2025-03-01T11:00:00Z")
	if _, err := pool.Exec(`
INSERT INTO contacts (address_book_id, uid, resource_name, raw_vcard, etag, birthday)
SELECT $1, 'yearless-' || n, 'yearless-' || n, E'BEGIN:VCARD\r\nVERSION:3.0\r\nFN:Someone\r\nBDAY:--02-29\r\nEND:VCARD\r\n', 'etag', DATE '0001-03-01'
FROM generate_series(1, $2) AS n`, bookID, perRepair); err != nil {
		t.Fatalf("seed contacts: %v", err)
	}

	type collectionState struct {
		ctag       int64
		syncToken  time.Time
		lastMember time.Time
	}
	collections := []struct {
		name, table, members, key string
		id                        int64
	}{
		{name: "repaired calendar", table: "calendars", members: "events", key: "calendar_id", id: repairedID},
		{name: "untouched calendar", table: "calendars", members: "events", key: "calendar_id", id: untouchedID},
		{name: "address book", table: "address_books", members: "contacts", key: "address_book_id", id: bookID},
	}
	states := func() []collectionState {
		t.Helper()
		states := make([]collectionState, len(collections))
		for i, collection := range collections {
			if err := pool.QueryRow(fmt.Sprintf(`SELECT ctag, updated_at, (SELECT max(last_modified) FROM %s WHERE %s = $1) FROM %s WHERE id = $1`,
				collection.members, collection.key, collection.table), collection.id).
				Scan(&states[i].ctag, &states[i].syncToken, &states[i].lastMember); err != nil {
				t.Fatalf("read %s: %v", collection.name, err)
			}
		}
		return states
	}
	upgrade := func(run string, apply func(), wantMoves ...int64) {
		t.Helper()
		before := states()
		apply()
		after := states()
		for i, collection := range collections {
			if moves := after[i].ctag - before[i].ctag; moves != wantMoves[i] {
				t.Errorf("%s: %s CTag moved %d times, want %d", run, collection.name, moves, wantMoves[i])
			}
			if wantMoves[i] == 0 {
				if !after[i].syncToken.Equal(before[i].syncToken) {
					t.Errorf("%s: %s sync token moved from %s to %s", run, collection.name, before[i].syncToken, after[i].syncToken)
				}
				continue
			}
			if !after[i].syncToken.After(before[i].syncToken) {
				t.Errorf("%s: %s sync token = %s, want it moved past %s", run, collection.name, after[i].syncToken, before[i].syncToken)
			}
			if after[i].lastMember.After(after[i].syncToken) {
				t.Errorf("%s: %s has a member stamped %s, after the sync token %s, so a sync from that token reports it again",
					run, collection.name, after[i].lastMember, after[i].syncToken)
			}
		}
		var disabled string
		if err := pool.QueryRow(`
SELECT COALESCE(string_agg(t.tgname, ', '), '')
FROM pg_trigger t
JOIN pg_class c ON c.oid = t.tgrelid
WHERE NOT t.tgisinternal
  AND c.relnamespace = current_schema()::regnamespace
  AND t.tgenabled <> 'O'`).Scan(&disabled); err != nil {
			t.Fatalf("read trigger states: %v", err)
		}
		if disabled != "" {
			t.Errorf("%s: the upgrade leaves triggers disabled: %s", run, disabled)
		}
	}

	upgrade("upgrade from v1.1.9", func() { migrateWithRunner(t, pool, "../../migrations", "v1.2.0") }, 1, 0, 1)

	// Bounds a release candidate stored that the series outlasts are reopened.
	if _, err := pool.Exec(`
INSERT INTO events (calendar_id, uid, resource_name, raw_ical, etag, dtstart, dtend, all_day, last_modified, recurrence_start, recurrence_until)
SELECT $1, $2 || n, $2 || n || '.ics', `+eventBody+`, 'etag', '2026-03-01T07:30:00Z', '2026-03-01T08:30:00Z', FALSE, NOW(),
       '2026-03-01T07:30:00Z', '2026-03-10T08:30:00Z'
FROM generate_series(1, $4) AS n`,
		repairedID, "outlasted-", "DTSTART;TZID=America/New_York:20260301T023000\r\nRRULE:FREQ=DAILY;COUNT=10\r\n", perRepair); err != nil {
		t.Fatalf("seed outlasted bounds: %v", err)
	}
	upgrade("reopening outlasted bounds", func() { applyV120Migration(t, pool) }, 1, 0, 0)
	var reopened int
	if err := pool.QueryRow(`SELECT count(*) FROM events WHERE uid LIKE 'outlasted-%' AND recurrence_until = $1`, icalpkg.RecurrenceUntilSentinel).
		Scan(&reopened); err != nil {
		t.Fatalf("read reopened bounds: %v", err)
	}
	if reopened != perRepair {
		t.Errorf("%d of %d outlasted bounds reopened", reopened, perRepair)
	}

	upgrade("repeated upgrade", func() { applyV120Migration(t, pool) }, 0, 0, 0)
}

func ptr[T any](value T) *T {
	return &value
}
