package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/lib/pq"
)

// postgresDSNEnv names the connection string these tests run against. They are
// the only tests in the repository that need a live server: the guarantees they
// cover are properties of one PostgreSQL transaction, and sqlmock can assert
// which statements were sent but not that two of them cannot interleave.
const postgresDSNEnv = "CALCARD_TEST_POSTGRES_DSN"

// newPostgresStore applies db.sql to a schema of this test's own and returns a
// Store over it. The schema is dropped afterwards, so a run leaves the target
// database as it found it and two runs cannot collide.
func newPostgresStore(t *testing.T) *Store {
	t.Helper()
	return New(newPostgresPool(t))
}

// newPostgresPool is newPostgresStore's schema setup, handed back as the pool
// itself for the tests that run schema statements rather than repository calls.
func newPostgresPool(t *testing.T) *sql.DB {
	t.Helper()
	return newPostgresSchemaPool(t, filepath.Join("..", "..", "db.sql"))
}

// newPostgresSchemaPool builds the pool over the named schema file. Upgrade tests
// pass a baseline an earlier release shipped, which is the schema a migration
// actually meets.
func newPostgresSchemaPool(t *testing.T, schemaPath string) *sql.DB {
	t.Helper()
	dsn := os.Getenv(postgresDSNEnv)
	if dsn == "" {
		t.Skipf("%s is unset; set it to a PostgreSQL connection string to run the concurrency tests", postgresDSNEnv)
	}

	schema := fmt.Sprintf("calcard_test_%d", time.Now().UnixNano())
	admin, err := sql.Open("postgres", dsn)
	if err != nil {
		t.Fatalf("open %s: %v", postgresDSNEnv, err)
	}
	if err := admin.Ping(); err != nil {
		admin.Close()
		t.Fatalf("ping %s: %v", postgresDSNEnv, err)
	}
	if _, err := admin.Exec(`CREATE SCHEMA ` + schema); err != nil {
		admin.Close()
		t.Fatalf("create schema %s: %v", schema, err)
	}

	pool, err := sql.Open("postgres", withSearchPath(t, dsn, schema))
	if err != nil {
		admin.Close()
		t.Fatalf("open schema %s: %v", schema, err)
	}
	t.Cleanup(func() {
		pool.Close()
		if _, err := admin.Exec(`DROP SCHEMA ` + schema + ` CASCADE`); err != nil {
			t.Errorf("drop schema %s: %v", schema, err)
		}
		admin.Close()
	})

	schemaSQL, err := os.ReadFile(schemaPath)
	if err != nil {
		t.Fatalf("read %s: %v", schemaPath, err)
	}
	if _, err := pool.Exec(string(schemaSQL)); err != nil {
		t.Fatalf("apply %s: %v", schemaPath, err)
	}
	return pool
}

// blockedByQuery reports whether a session of this test's own pool, which the
// test names after its schema, waits for a lock the session $1 holds. Test
// packages share one database, so a session of another test's could otherwise
// be taken for the one under test.
const blockedByQuery = `SELECT EXISTS (
    SELECT 1 FROM pg_stat_activity
    WHERE application_name = current_setting('application_name')
      AND $1 = ANY(pg_blocking_pids(pid)))`

func withSearchPath(t *testing.T, dsn, schema string) string {
	t.Helper()
	if strings.HasPrefix(dsn, "postgres://") || strings.HasPrefix(dsn, "postgresql://") {
		parsed, err := url.Parse(dsn)
		if err != nil {
			t.Fatalf("parse %s: %v", postgresDSNEnv, err)
		}
		query := parsed.Query()
		query.Set("search_path", schema)
		query.Set("application_name", schema)
		parsed.RawQuery = query.Encode()
		return parsed.String()
	}
	return dsn + " search_path=" + schema + " application_name=" + schema
}

// newPostgresCalendar creates the user and calendar the concurrency tests write
// into, returning the calendar's ID.
func newPostgresCalendar(t *testing.T, s *Store, name string) (int64, int64) {
	t.Helper()
	ctx := context.Background()
	user, err := s.Users.UpsertOAuthUser(ctx, "subject-"+name, name+"@example.test", "Test User", "Test")
	if err != nil {
		t.Fatalf("create user: %v", err)
	}
	cal, err := s.Calendars.Create(ctx, Calendar{UserID: user.ID, Name: name})
	if err != nil {
		t.Fatalf("create calendar: %v", err)
	}
	return user.ID, cal.ID
}

func postgresCalendarObject(uid, summary string) string {
	return "BEGIN:VCALENDAR\r\nVERSION:2.0\r\nPRODID:-//CalCard//Test//EN\r\n" +
		"BEGIN:VEVENT\r\nUID:" + uid + "\r\nDTSTAMP:20240601T000000Z\r\n" +
		"DTSTART:20240601T100000Z\r\nSUMMARY:" + summary + "\r\nEND:VEVENT\r\nEND:VCALENDAR\r\n"
}

// RFC 4791 §4.1 makes a UID unique within a calendar collection. Two PUTs of one
// UID at different resource names, issued together, must therefore produce one
// stored resource and one CALDAV:no-uid-conflict — not two rows, and not one row
// whose resource name the loser silently rewrote.
func TestPostgres_ConcurrentPutsOneUIDTwoResources(t *testing.T) {
	s := newPostgresStore(t)
	_, calendarID := newPostgresCalendar(t, s, "uid-race")

	const writers = 8
	results := runConcurrently(writers, func(i int) error {
		result, err := s.PutCalendarObject(context.Background(), CalendarObjectWrite{
			CalendarID:   calendarID,
			UID:          "shared-uid",
			ResourceName: fmt.Sprintf("resource-%d", i),
			RawICAL:      postgresCalendarObject("shared-uid", fmt.Sprintf("Writer %d", i)),
			ETag:         fmt.Sprintf("etag-%d", i),
		})
		if errors.Is(err, ErrUIDConflict) && (result == nil || result.Conflict == nil) {
			return fmt.Errorf("ErrUIDConflict returned without the conflicting resource")
		}
		return err
	})

	winners, conflicts := 0, 0
	for _, err := range results {
		switch {
		case err == nil:
			winners++
		case errors.Is(err, ErrUIDConflict):
			conflicts++
		default:
			t.Fatalf("unexpected error from a competing PUT: %v", err)
		}
	}
	if winners != 1 || conflicts != writers-1 {
		t.Fatalf("competing PUTs = %d stored, %d refused; want exactly 1 stored", winners, conflicts)
	}

	stored, err := s.Events.ListForCalendar(context.Background(), calendarID)
	if err != nil {
		t.Fatalf("list events: %v", err)
	}
	if len(stored) != 1 {
		t.Fatalf("stored events = %d, want 1", len(stored))
	}
	if stored[0].UID != "shared-uid" || !strings.HasPrefix(stored[0].ResourceName, "resource-") {
		t.Fatalf("stored event = %+v, want the one writer that won at its own resource name", stored[0])
	}
}

func TestPostgres_ConcurrentUIDChangesReturnConflicts(t *testing.T) {
	s := newPostgresStore(t)
	_, calendarID := newPostgresCalendar(t, s, "uid-change-race")
	ctx := context.Background()

	for _, seed := range []CalendarObjectWrite{
		{CalendarID: calendarID, UID: "uid-a", ResourceName: "a", RawICAL: postgresCalendarObject("uid-a", "A"), ETag: "etag-a"},
		{CalendarID: calendarID, UID: "uid-b", ResourceName: "b", RawICAL: postgresCalendarObject("uid-b", "B"), ETag: "etag-b"},
	} {
		if _, err := s.PutCalendarObject(ctx, seed); err != nil {
			t.Fatalf("seed event: %v", err)
		}
	}

	const rounds = 20
	for round := 0; round < rounds; round++ {
		results := runConcurrently(2, func(i int) error {
			writes := []CalendarObjectWrite{
				{CalendarID: calendarID, UID: "uid-b", ResourceName: "a", RawICAL: postgresCalendarObject("uid-b", "A to B"), ETag: "etag-a2"},
				{CalendarID: calendarID, UID: "uid-a", ResourceName: "b", RawICAL: postgresCalendarObject("uid-a", "B to A"), ETag: "etag-b2"},
			}
			result, err := s.PutCalendarObject(ctx, writes[i])
			if errors.Is(err, ErrUIDConflict) && (result == nil || result.Conflict == nil) {
				return fmt.Errorf("ErrUIDConflict returned without the target resource")
			}
			return err
		})
		for i, err := range results {
			if !errors.Is(err, ErrUIDConflict) {
				t.Fatalf("round %d writer %d error = %v, want ErrUIDConflict", round, i, err)
			}
		}
	}

	for resourceName, wantUID := range map[string]string{"a": "uid-a", "b": "uid-b"} {
		stored, err := s.Events.GetByResourceName(ctx, calendarID, resourceName)
		if err != nil || stored == nil {
			t.Fatalf("load resource %q: %v", resourceName, err)
		}
		if stored.UID != wantUID {
			t.Fatalf("resource %q UID = %q, want unchanged %q", resourceName, stored.UID, wantUID)
		}
	}
}

// RFC 7232 §3.2: "If-None-Match: *" creates only where nothing is mapped yet.
// Issued together against one unmapped resource name, exactly one succeeds.
func TestPostgres_ConcurrentIfNoneMatchStarCreatesOnce(t *testing.T) {
	s := newPostgresStore(t)
	_, calendarID := newPostgresCalendar(t, s, "if-none-match")

	const writers = 8
	results := runConcurrently(writers, func(i int) error {
		_, err := s.PutCalendarObject(context.Background(), CalendarObjectWrite{
			CalendarID:   calendarID,
			UID:          "exclusive",
			ResourceName: "exclusive",
			RawICAL:      postgresCalendarObject("exclusive", fmt.Sprintf("Writer %d", i)),
			ETag:         fmt.Sprintf("etag-%d", i),
			Precondition: CalendarObjectPrecondition{IfNoneMatch: &ETagCondition{Any: true}},
		})
		return err
	})

	created, refused := 0, 0
	for _, err := range results {
		switch {
		case err == nil:
			created++
		case errors.Is(err, ErrPreconditionFailed):
			refused++
		default:
			t.Fatalf("unexpected error from a competing exclusive create: %v", err)
		}
	}
	if created != 1 || refused != writers-1 {
		t.Fatalf("exclusive creates = %d created, %d refused; want exactly 1 created", created, refused)
	}
}

// RFC 7232 §3.1: an If-Match naming an entity tag that is no longer current
// fails. Two updates racing from the same starting ETag must not both apply:
// the loser's validator is stale by the time its write runs, however fresh it
// was when the request arrived.
func TestPostgres_ConcurrentIfMatchAppliesOneUpdate(t *testing.T) {
	s := newPostgresStore(t)
	_, calendarID := newPostgresCalendar(t, s, "if-match")
	ctx := context.Background()

	if _, err := s.PutCalendarObject(ctx, CalendarObjectWrite{
		CalendarID:   calendarID,
		UID:          "contested",
		ResourceName: "contested",
		RawICAL:      postgresCalendarObject("contested", "Original"),
		ETag:         "etag-original",
	}); err != nil {
		t.Fatalf("seed event: %v", err)
	}

	// Whether two conditional updates actually overlap is a matter of timing, so
	// the race is run several times and the invariant demanded of every round.
	const rounds, writers = 5, 8
	current := "etag-original"
	for round := 0; round < rounds; round++ {
		expected := current
		results := runConcurrently(writers, func(i int) error {
			_, err := s.PutCalendarObject(ctx, CalendarObjectWrite{
				CalendarID:   calendarID,
				UID:          "contested",
				ResourceName: "contested",
				RawICAL:      postgresCalendarObject("contested", fmt.Sprintf("Round %d writer %d", round, i)),
				ETag:         fmt.Sprintf("etag-%d-%d", round, i),
				Precondition: CalendarObjectPrecondition{IfMatch: &ETagCondition{ETags: []string{expected}}},
			})
			return err
		})

		applied, stale := 0, 0
		for _, err := range results {
			switch {
			case err == nil:
				applied++
			case errors.Is(err, ErrPreconditionFailed):
				stale++
			default:
				t.Fatalf("unexpected error from a competing conditional update: %v", err)
			}
		}
		if applied != 1 || stale != writers-1 {
			t.Fatalf("round %d conditional updates = %d applied, %d refused; want exactly 1 applied", round, applied, stale)
		}

		stored, err := s.Events.GetByResourceName(ctx, calendarID, "contested")
		if err != nil || stored == nil {
			t.Fatalf("reload contested event: %v", err)
		}
		if stored.ETag == expected {
			t.Fatalf("round %d left the ETag at %q, so no update was applied", round, expected)
		}
		current = stored.ETag
	}
}

// RFC 4791 §5.3.1.1 (DAV:resource-must-be-null): no resource may exist at the
// Request-URI of a MKCALENDAR. Issued together for one URI, exactly one creates
// the collection and the rest fail without leaving a second one behind.
func TestPostgres_ConcurrentMkcalendarCreatesOneCollection(t *testing.T) {
	s := newPostgresStore(t)
	userID, _ := newPostgresCalendar(t, s, "mkcalendar")
	slug := "contested-collection"

	const writers = 8
	results := runConcurrently(writers, func(i int) error {
		_, err := s.CreateCalendarAndState(context.Background(), Calendar{
			UserID: userID,
			Name:   fmt.Sprintf("Attempt %d", i),
			Slug:   &slug,
		}, nil, nil, "", func(int64) string { return "" })
		return err
	})

	created, refused := 0, 0
	for _, err := range results {
		switch {
		case err == nil:
			created++
		case isPostgresUniqueViolation(err):
			refused++
		default:
			t.Fatalf("unexpected error from a competing MKCALENDAR: %v", err)
		}
	}
	if created != 1 || refused != writers-1 {
		t.Fatalf("simultaneous MKCALENDAR = %d created, %d refused; want exactly 1 created", created, refused)
	}

	calendars, err := s.Calendars.ListByUser(context.Background(), userID)
	if err != nil {
		t.Fatalf("list calendars: %v", err)
	}
	matching := 0
	for _, cal := range calendars {
		if cal.Slug != nil && *cal.Slug == slug {
			matching++
		}
	}
	if matching != 1 {
		t.Fatalf("collections at the contested slug = %d, want 1", matching)
	}
}

func TestPostgres_CalendarObjectTransfersRollBackEventAndDAVStateTogether(t *testing.T) {
	for _, operation := range []CalendarObjectTransferOperation{CalendarObjectCopy, CalendarObjectMove} {
		t.Run(string(operation), func(t *testing.T) {
			s := newPostgresStore(t)
			_, sourceCalendarID := newPostgresCalendar(t, s, "object-transfer-source-"+string(operation))
			_, destinationCalendarID := newPostgresCalendar(t, s, "object-transfer-destination-"+string(operation))
			ctx := context.Background()
			raw := postgresCalendarObject("source", "Source")
			if _, err := s.PutCalendarObject(ctx, CalendarObjectWrite{
				CalendarID: sourceCalendarID, UID: "source", ResourceName: "source", RawICAL: raw, ETag: "source-etag",
			}); err != nil {
				t.Fatalf("seed source event: %v", err)
			}
			sourcePath := fmt.Sprintf("/dav/calendars/%d/source", sourceCalendarID)
			destinationPath := fmt.Sprintf("/dav/calendars/%d/destination", destinationCalendarID)
			if err := s.DeadProperties.Apply(ctx, sourcePath, []DeadPropertyMutation{{
				NamespaceURI: "urn:calcard:test", LocalName: "marker", InnerXML: "source-state",
			}}); err != nil {
				t.Fatalf("seed source dead property: %v", err)
			}

			triggerOperation := "INSERT"
			if operation == CalendarObjectMove {
				triggerOperation = "UPDATE"
			}
			triggerSQL := fmt.Sprintf(`
CREATE FUNCTION reject_object_state_change() RETURNS trigger AS $$
BEGIN
    IF NEW.resource_path = %s THEN
        RAISE EXCEPTION 'forced DAV state failure';
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;
CREATE TRIGGER reject_object_state_change
BEFORE %s ON dav_dead_properties
FOR EACH ROW EXECUTE FUNCTION reject_object_state_change()`, pq.QuoteLiteral(destinationPath), triggerOperation)
			if _, err := s.Calendars.(*calendarRepo).pool.ExecContext(ctx, triggerSQL); err != nil {
				t.Fatalf("install failure trigger: %v", err)
			}

			_, err := s.TransferCalendarObject(ctx, CalendarObjectTransfer{
				Operation:               operation,
				SourceCalendarID:        sourceCalendarID,
				SourceUID:               "source",
				SourceResourceName:      "source",
				ExpectedSourceETag:      "source-etag",
				ExpectedSourceRaw:       raw,
				DestinationCalendarID:   destinationCalendarID,
				DestinationResourceName: "destination",
				ExpectedDestination:     CalendarObjectTransferState{},
				Overwrite:               true,
				RawICAL:                 raw,
				ETag:                    "destination-etag",
				SourceStatePath:         sourcePath,
				DestinationStatePath:    destinationPath,
			})
			if err == nil {
				t.Fatal("TransferCalendarObject() error = nil, want forced rollback")
			}

			source, loadErr := s.Events.GetByResourceName(ctx, sourceCalendarID, "source")
			if loadErr != nil || source == nil || source.ETag != "source-etag" {
				t.Fatalf("source after rollback = %#v, %v", source, loadErr)
			}
			destination, loadErr := s.Events.GetByResourceName(ctx, destinationCalendarID, "destination")
			if loadErr != nil || destination != nil {
				t.Fatalf("destination after rollback = %#v, %v", destination, loadErr)
			}
			properties, loadErr := s.DeadProperties.ListByResources(ctx, []string{sourcePath, destinationPath})
			if loadErr != nil {
				t.Fatalf("list dead properties: %v", loadErr)
			}
			if len(properties) != 1 || properties[0].ResourcePath != sourcePath || properties[0].InnerXML != "source-state" {
				t.Fatalf("DAV state after rollback = %#v", properties)
			}
		})
	}
}

func TestPostgres_CalendarObjectMovePublishesSourceRemoval(t *testing.T) {
	for _, crossCalendar := range []bool{false, true} {
		name := "same-calendar rename"
		if crossCalendar {
			name = "cross-calendar move"
		}
		t.Run(name, func(t *testing.T) {
			s := newPostgresStore(t)
			_, sourceCalendarID := newPostgresCalendar(t, s, "move-sync-source")
			destinationCalendarID := sourceCalendarID
			if crossCalendar {
				_, destinationCalendarID = newPostgresCalendar(t, s, "move-sync-destination")
			}
			ctx := context.Background()
			raw := postgresCalendarObject("source", "Source")
			if _, err := s.PutCalendarObject(ctx, CalendarObjectWrite{
				CalendarID: sourceCalendarID, UID: "source", ResourceName: "source", RawICAL: raw, ETag: "source-etag",
			}); err != nil {
				t.Fatalf("seed source event: %v", err)
			}

			database := s.Calendars.(*calendarRepo).pool
			var sourceCTagBefore int64
			if err := database.QueryRowContext(ctx, `SELECT ctag FROM calendars WHERE id=$1`, sourceCalendarID).Scan(&sourceCTagBefore); err != nil {
				t.Fatalf("load source ctag: %v", err)
			}

			result, err := s.TransferCalendarObject(ctx, CalendarObjectTransfer{
				Operation:               CalendarObjectMove,
				SourceCalendarID:        sourceCalendarID,
				SourceUID:               "source",
				SourceResourceName:      "source",
				ExpectedSourceETag:      "source-etag",
				ExpectedSourceRaw:       raw,
				DestinationCalendarID:   destinationCalendarID,
				DestinationResourceName: "renamed",
				ExpectedDestination:     CalendarObjectTransferState{},
				Overwrite:               true,
				RawICAL:                 raw,
				ETag:                    "source-etag",
				SourceStatePath:         fmt.Sprintf("/dav/calendars/%d/source", sourceCalendarID),
				DestinationStatePath:    fmt.Sprintf("/dav/calendars/%d/renamed", destinationCalendarID),
			})
			if err != nil {
				t.Fatalf("TransferCalendarObject() error = %v", err)
			}
			if result == nil || result.Event == nil || result.Event.CalendarID != destinationCalendarID || result.Event.ResourceName != "renamed" {
				t.Fatalf("move result = %#v", result)
			}

			var tombstones int
			if err := database.QueryRowContext(ctx, `
SELECT COUNT(*) FROM deleted_resources
WHERE resource_type='event' AND collection_id=$1 AND uid='source' AND resource_name='source'`, sourceCalendarID).Scan(&tombstones); err != nil {
				t.Fatalf("load source tombstone: %v", err)
			}
			if tombstones != 1 {
				t.Fatalf("source tombstones = %d, want 1", tombstones)
			}

			var sourceCTagAfter int64
			if err := database.QueryRowContext(ctx, `SELECT ctag FROM calendars WHERE id=$1`, sourceCalendarID).Scan(&sourceCTagAfter); err != nil {
				t.Fatalf("reload source ctag: %v", err)
			}
			if sourceCTagAfter <= sourceCTagBefore {
				t.Fatalf("source ctag = %d after move, want greater than %d", sourceCTagAfter, sourceCTagBefore)
			}
		})
	}
}

func TestPostgres_CalendarCollectionTransferRebindsPendingDestinationLock(t *testing.T) {
	for _, operation := range []CalendarCollectionTransferOperation{CalendarCollectionCopy, CalendarCollectionMove} {
		t.Run(string(operation), func(t *testing.T) {
			s := newPostgresStore(t)
			userID, sourceID := newPostgresCalendar(t, s, "collection-lock-"+string(operation))
			ctx := context.Background()
			source, err := s.Calendars.GetByID(ctx, sourceID)
			if err != nil || source == nil {
				t.Fatalf("load source calendar: %#v, %v", source, err)
			}
			pendingPath := fmt.Sprintf("/dav/calendars/.pending/%d/transferred", userID)
			sourcePath := fmt.Sprintf("/dav/calendars/%d", sourceID)
			sourceToken := "collection-source-" + string(operation)
			if _, err := s.Locks.Create(ctx, Lock{
				Token: sourceToken, ResourcePath: sourcePath, UserID: userID,
				LockScope: "exclusive", LockType: "write", Depth: "0",
				TimeoutSeconds: 3600, ExpiresAt: time.Now().Add(time.Hour),
			}); err != nil {
				t.Fatalf("seed source lock: %v", err)
			}
			token := "collection-transfer-" + string(operation)
			if _, err := s.Locks.Create(ctx, Lock{
				Token: token, ResourcePath: pendingPath, UserID: userID,
				LockScope: "exclusive", LockType: "write", Depth: "0",
				TimeoutSeconds: 3600, ExpiresAt: time.Now().Add(time.Hour),
			}); err != nil {
				t.Fatalf("seed pending destination lock: %v", err)
			}

			lockPreconditions := []LockPrecondition{
				{ResourcePath: pendingPath, LookupPaths: []string{pendingPath}, Tokens: []string{token}},
			}
			if operation == CalendarCollectionMove {
				lockPreconditions = append(lockPreconditions, LockPrecondition{ResourcePath: sourcePath, LookupPaths: []string{sourcePath}, Tokens: []string{sourceToken}})
			}
			result, err := s.TransferCalendarCollection(ctx, CalendarCollectionTransfer{
				Operation:           operation,
				Depth:               "infinity",
				Overwrite:           true,
				SourceID:            sourceID,
				ExpectedSourceCTag:  source.CTag,
				SourceStatePath:     sourcePath,
				DestinationOwnerID:  userID,
				DestinationSlug:     "transferred",
				DestinationLockPath: pendingPath,
				ExpectedDestination: CalendarCollectionState{},
				LockPreconditions:   lockPreconditions,
			})
			if err != nil {
				t.Fatalf("TransferCalendarCollection() error = %v", err)
			}
			if result == nil || result.Calendar == nil {
				t.Fatalf("transfer result = %#v", result)
			}
			lock, err := s.Locks.GetByToken(ctx, token)
			if err != nil || lock == nil {
				t.Fatalf("load rebound lock: %#v, %v", lock, err)
			}
			wantPath := fmt.Sprintf("/dav/calendars/%d", result.Calendar.ID)
			if lock.ResourcePath != wantPath {
				t.Fatalf("rebound lock path = %q, want %q", lock.ResourcePath, wantPath)
			}
			sourceLock, err := s.Locks.GetByToken(ctx, sourceToken)
			if err != nil {
				t.Fatalf("load source lock: %v", err)
			}
			if operation == CalendarCollectionCopy {
				if sourceLock == nil || sourceLock.ResourcePath != sourcePath {
					t.Fatalf("COPY source lock = %#v, want it unchanged at %q", sourceLock, sourcePath)
				}
			} else if sourceLock != nil {
				t.Fatalf("MOVE source lock = %#v, want RFC 4918 section 7.6 to remove it", sourceLock)
			}
			if operation == CalendarCollectionMove {
				oldSource, err := s.Calendars.GetByID(ctx, sourceID)
				if err != nil || oldSource != nil {
					t.Fatalf("MOVE source binding still resolves: %#v, %v", oldSource, err)
				}
				if result.Calendar.ID == sourceID {
					t.Fatalf("MOVE destination reused advertised source identity %d", sourceID)
				}
			}
		})
	}
}

func TestPostgres_CalendarObjectTransferUsesRFC4918MoveLockSemantics(t *testing.T) {
	for _, operation := range []CalendarObjectTransferOperation{CalendarObjectCopy, CalendarObjectMove} {
		t.Run(string(operation), func(t *testing.T) {
			s := newPostgresStore(t)
			userID, sourceCalendarID := newPostgresCalendar(t, s, "object-lock-"+string(operation))
			ctx := context.Background()
			destinationCalendar, err := s.Calendars.Create(ctx, Calendar{UserID: userID, Name: "Object Lock Destination " + string(operation)})
			if err != nil {
				t.Fatal(err)
			}
			sourceRaw := postgresCalendarObject("source", "Source")
			sourceResult, err := s.PutCalendarObject(ctx, CalendarObjectWrite{
				CalendarID: sourceCalendarID, UID: "source", ResourceName: "source", RawICAL: sourceRaw, ETag: "source-etag",
			})
			if err != nil || sourceResult == nil || sourceResult.Event == nil {
				t.Fatalf("seed source: %#v, %v", sourceResult, err)
			}
			destinationResult, err := s.PutCalendarObject(ctx, CalendarObjectWrite{
				CalendarID: destinationCalendar.ID, UID: "old-destination", ResourceName: "destination",
				RawICAL: postgresCalendarObject("old-destination", "Destination"), ETag: "destination-etag",
			})
			if err != nil || destinationResult == nil || destinationResult.Event == nil {
				t.Fatalf("seed destination: %#v, %v", destinationResult, err)
			}
			sourcePath := fmt.Sprintf("/dav/calendars/%d/source", sourceCalendarID)
			destinationPath := fmt.Sprintf("/dav/calendars/%d/destination", destinationCalendar.ID)
			sourceToken := "object-source-" + string(operation)
			destinationToken := "object-destination-" + string(operation)
			for token, resourcePath := range map[string]string{sourceToken: sourcePath, destinationToken: destinationPath} {
				if _, err := s.Locks.Create(ctx, Lock{
					Token: token, ResourcePath: resourcePath, UserID: userID, LockScope: "exclusive", LockType: "write",
					Depth: "0", TimeoutSeconds: 3600, ExpiresAt: time.Now().Add(time.Hour),
				}); err != nil {
					t.Fatalf("seed lock %q: %v", token, err)
				}
			}
			preconditions := []LockPrecondition{{
				ResourcePath: destinationPath, LookupPaths: []string{destinationPath}, Tokens: []string{destinationToken},
			}}
			if operation == CalendarObjectMove {
				preconditions = append(preconditions, LockPrecondition{
					ResourcePath: sourcePath, LookupPaths: []string{sourcePath}, Tokens: []string{sourceToken},
				})
			}
			_, err = s.TransferCalendarObject(ctx, CalendarObjectTransfer{
				Operation: operation, SourceCalendarID: sourceCalendarID, SourceUID: "source", SourceResourceName: "source",
				ExpectedSourceETag: "source-etag", ExpectedSourceRaw: sourceRaw,
				DestinationCalendarID: destinationCalendar.ID, DestinationResourceName: "destination",
				ExpectedDestination: CalendarObjectTransferState{Exists: true, UID: "old-destination", ETag: "destination-etag"},
				Overwrite:           true, RawICAL: sourceRaw, ETag: "copied-etag", SourceStatePath: sourcePath,
				DestinationStatePath: destinationPath, LockPreconditions: preconditions,
			})
			if err != nil {
				t.Fatalf("TransferCalendarObject() error = %v", err)
			}
			destinationLock, err := s.Locks.GetByToken(ctx, destinationToken)
			if err != nil || destinationLock == nil || destinationLock.ResourcePath != destinationPath {
				t.Fatalf("destination lock = %#v, %v", destinationLock, err)
			}
			sourceLock, err := s.Locks.GetByToken(ctx, sourceToken)
			if err != nil {
				t.Fatal(err)
			}
			if operation == CalendarObjectCopy {
				if sourceLock == nil || sourceLock.ResourcePath != sourcePath {
					t.Fatalf("COPY source lock = %#v", sourceLock)
				}
			} else if sourceLock != nil {
				t.Fatalf("MOVE source lock = %#v, want nil", sourceLock)
			}
		})
	}
}

func TestPostgres_CalendarCollectionTransferPreservesDestinationLockTree(t *testing.T) {
	for _, operation := range []CalendarCollectionTransferOperation{CalendarCollectionCopy, CalendarCollectionMove} {
		t.Run(string(operation), func(t *testing.T) {
			s := newPostgresStore(t)
			userID, sourceID := newPostgresCalendar(t, s, "collection-tree-lock-"+string(operation))
			ctx := context.Background()
			destinationSlug := "tree-lock-destination-" + string(operation)
			destination, err := s.Calendars.Create(ctx, Calendar{UserID: userID, Name: "Tree Lock Destination", Slug: &destinationSlug})
			if err != nil {
				t.Fatal(err)
			}
			sourceRaw := postgresCalendarObject("source", "Source")
			sourceResult, err := s.PutCalendarObject(ctx, CalendarObjectWrite{
				CalendarID: sourceID, UID: "source", ResourceName: "member", RawICAL: sourceRaw, ETag: "source-etag",
			})
			if err != nil || sourceResult == nil || sourceResult.Event == nil {
				t.Fatalf("seed source: %#v, %v", sourceResult, err)
			}
			if _, err := s.PutCalendarObject(ctx, CalendarObjectWrite{
				CalendarID: destination.ID, UID: "destination", ResourceName: "member",
				RawICAL: postgresCalendarObject("destination", "Destination"), ETag: "destination-etag",
			}); err != nil {
				t.Fatal(err)
			}
			source, _ := s.Calendars.GetByID(ctx, sourceID)
			destination, _ = s.Calendars.GetByID(ctx, destination.ID)
			sourceRoot := calendarCollectionStatePath(sourceID)
			destinationRoot := calendarCollectionStatePath(destination.ID)
			tokens := map[string]string{
				"source-root-" + string(operation):        sourceRoot,
				"source-member-" + string(operation):      sourceRoot + "/member",
				"destination-root-" + string(operation):   destinationRoot,
				"destination-member-" + string(operation): destinationRoot + "/member",
			}
			for token, resourcePath := range tokens {
				if _, err := s.Locks.Create(ctx, Lock{
					Token: token, ResourcePath: resourcePath, UserID: userID, LockScope: "exclusive", LockType: "write",
					Depth: "0", TimeoutSeconds: 3600, ExpiresAt: time.Now().Add(time.Hour),
				}); err != nil {
					t.Fatalf("seed lock %q: %v", token, err)
				}
			}
			preconditions := []LockPrecondition{
				{ResourcePath: destinationRoot, LookupPaths: []string{destinationRoot}, Tokens: []string{"destination-root-" + string(operation)}},
				{ResourcePath: destinationRoot + "/member", LookupPaths: []string{destinationRoot + "/member"}, Tokens: []string{"destination-member-" + string(operation)}},
			}
			if operation == CalendarCollectionMove {
				preconditions = append(preconditions,
					LockPrecondition{ResourcePath: sourceRoot, LookupPaths: []string{sourceRoot}, Tokens: []string{"source-root-" + string(operation)}},
					LockPrecondition{ResourcePath: sourceRoot + "/member", LookupPaths: []string{sourceRoot + "/member"}, Tokens: []string{"source-member-" + string(operation)}},
				)
			}
			result, err := s.TransferCalendarCollection(ctx, CalendarCollectionTransfer{
				Operation: operation, Depth: "infinity", Overwrite: true, SourceID: sourceID,
				ExpectedSourceCTag: source.CTag, SourceStatePath: sourceRoot, DestinationOwnerID: userID,
				DestinationSlug: destinationSlug, ExpectedDestination: CalendarCollectionState{Exists: true, ID: destination.ID, CTag: destination.CTag},
				DestinationStatePath: destinationRoot, DestinationLockPath: destinationRoot,
				Members: []CalendarCollectionMember{{
					ID: sourceResult.Event.ID, UID: "source", ResourceName: "member", RawICAL: sourceRaw,
					ETag: "source-etag", NewETag: "copied-etag",
				}},
				LockPreconditions: preconditions,
			})
			if err != nil {
				t.Fatalf("TransferCalendarCollection() error = %v", err)
			}
			newRoot := calendarCollectionStatePath(result.Calendar.ID)
			for token, suffix := range map[string]string{
				"destination-root-" + string(operation):   "",
				"destination-member-" + string(operation): "/member",
			} {
				lock, err := s.Locks.GetByToken(ctx, token)
				if err != nil || lock == nil || lock.ResourcePath != newRoot+suffix {
					t.Fatalf("destination lock %q = %#v, %v; want %q", token, lock, err, newRoot+suffix)
				}
			}
			for _, token := range []string{"source-root-" + string(operation), "source-member-" + string(operation)} {
				lock, err := s.Locks.GetByToken(ctx, token)
				if err != nil {
					t.Fatal(err)
				}
				if operation == CalendarCollectionCopy {
					if lock == nil {
						t.Fatalf("COPY removed source lock %q", token)
					}
				} else if lock != nil {
					t.Fatalf("MOVE retained source lock %q: %#v", token, lock)
				}
			}
		})
	}
}

func TestPostgres_CalendarCollectionCopyRollsBackCreatedCollectionAndMembers(t *testing.T) {
	s := newPostgresStore(t)
	userID, sourceID := newPostgresCalendar(t, s, "collection-copy-source")
	ctx := context.Background()
	for _, uid := range []string{"first", "second"} {
		if _, err := s.PutCalendarObject(ctx, CalendarObjectWrite{
			CalendarID: sourceID, UID: uid, ResourceName: uid, RawICAL: postgresCalendarObject(uid, uid), ETag: "etag-" + uid,
		}); err != nil {
			t.Fatalf("seed %s: %v", uid, err)
		}
	}
	source, err := s.Calendars.GetByID(ctx, sourceID)
	if err != nil || source == nil {
		t.Fatalf("load source calendar: %#v, %v", source, err)
	}
	events, err := s.Events.ListForCalendar(ctx, sourceID)
	if err != nil {
		t.Fatalf("list source events: %v", err)
	}
	members := make([]CalendarCollectionMember, 0, len(events))
	for _, event := range events {
		members = append(members, CalendarCollectionMember{
			ID: event.ID, UID: event.UID, ResourceName: event.ResourceName, RawICAL: event.RawICAL, ETag: event.ETag, NewETag: "copy-" + event.ETag,
		})
	}
	sourcePath := calendarCollectionStatePath(sourceID)
	if err := s.DeadProperties.Apply(ctx, sourcePath, []DeadPropertyMutation{{
		NamespaceURI: "urn:calcard:test", LocalName: "marker", InnerXML: "collection-state",
	}}); err != nil {
		t.Fatalf("seed collection state: %v", err)
	}
	triggerSQL := fmt.Sprintf(`
CREATE FUNCTION reject_member_copy() RETURNS trigger AS $$
BEGIN
    IF NEW.calendar_id <> %d THEN
        RAISE EXCEPTION 'forced member copy failure';
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;
CREATE TRIGGER reject_member_copy
BEFORE INSERT ON events
FOR EACH ROW EXECUTE FUNCTION reject_member_copy()`, sourceID)
	if _, err := s.Calendars.(*calendarRepo).pool.ExecContext(ctx, triggerSQL); err != nil {
		t.Fatalf("install failure trigger: %v", err)
	}

	_, err = s.TransferCalendarCollection(ctx, CalendarCollectionTransfer{
		Operation:            CalendarCollectionCopy,
		Depth:                "infinity",
		Overwrite:            true,
		SourceID:             sourceID,
		ExpectedSourceCTag:   source.CTag,
		SourceStatePath:      sourcePath,
		DestinationOwnerID:   userID,
		DestinationSlug:      "rolled-back-copy",
		DestinationStatePath: "/dav/calendars/rolled-back-copy",
		ExpectedDestination:  CalendarCollectionState{},
		Members:              members,
	})
	if err == nil {
		t.Fatal("TransferCalendarCollection() error = nil, want forced rollback")
	}

	calendars, err := s.Calendars.ListByUser(ctx, userID)
	if err != nil {
		t.Fatalf("list calendars: %v", err)
	}
	if len(calendars) != 1 || calendars[0].ID != sourceID {
		t.Fatalf("calendars after rollback = %#v", calendars)
	}
	stored, err := s.Events.ListForCalendar(ctx, sourceID)
	if err != nil || len(stored) != len(members) {
		t.Fatalf("source members after rollback = %#v, %v", stored, err)
	}
	properties, err := s.DeadProperties.ListByResources(ctx, []string{sourcePath})
	if err != nil || len(properties) != 1 || properties[0].InnerXML != "collection-state" {
		t.Fatalf("source DAV state after rollback = %#v, %v", properties, err)
	}
}

func TestPostgres_CalendarCollectionDeleteRollsBackTreeState(t *testing.T) {
	s := newPostgresStore(t)
	_, sourceID := newPostgresCalendar(t, s, "collection-delete-source")
	ctx := context.Background()
	raw := postgresCalendarObject("member", "Member")
	seeded, err := s.PutCalendarObject(ctx, CalendarObjectWrite{
		CalendarID: sourceID, UID: "member", ResourceName: "member", RawICAL: raw, ETag: "member-etag",
	})
	if err != nil || seeded == nil || seeded.Event == nil {
		t.Fatalf("seed member: %#v, %v", seeded, err)
	}
	source, err := s.Calendars.GetByID(ctx, sourceID)
	if err != nil || source == nil {
		t.Fatalf("load source: %#v, %v", source, err)
	}
	sourcePath := calendarCollectionStatePath(sourceID)
	if err := s.DeadProperties.Apply(ctx, sourcePath, []DeadPropertyMutation{{
		NamespaceURI: "urn:calcard:test", LocalName: "marker", InnerXML: "delete-state",
	}}); err != nil {
		t.Fatalf("seed state: %v", err)
	}
	triggerSQL := fmt.Sprintf(`
CREATE FUNCTION reject_calendar_delete() RETURNS trigger AS $$
BEGIN
    IF OLD.id = %d THEN
        RAISE EXCEPTION 'forced calendar delete failure';
    END IF;
    RETURN OLD;
END;
$$ LANGUAGE plpgsql;
CREATE TRIGGER reject_calendar_delete
BEFORE DELETE ON calendars
FOR EACH ROW EXECUTE FUNCTION reject_calendar_delete()`, sourceID)
	if _, err := s.Calendars.(*calendarRepo).pool.ExecContext(ctx, triggerSQL); err != nil {
		t.Fatalf("install failure trigger: %v", err)
	}

	_, err = s.TransferCalendarCollection(ctx, CalendarCollectionTransfer{
		Operation:          CalendarCollectionDelete,
		SourceID:           sourceID,
		ExpectedSourceCTag: source.CTag,
		SourceStatePath:    sourcePath,
		Members: []CalendarCollectionMember{{
			ID: seeded.Event.ID, UID: "member", ResourceName: "member", RawICAL: raw, ETag: "member-etag",
		}},
	})
	if err == nil {
		t.Fatal("TransferCalendarCollection(delete) error = nil, want forced rollback")
	}
	if calendar, loadErr := s.Calendars.GetByID(ctx, sourceID); loadErr != nil || calendar == nil {
		t.Fatalf("calendar after rollback = %#v, %v", calendar, loadErr)
	}
	if member, loadErr := s.Events.GetByResourceName(ctx, sourceID, "member"); loadErr != nil || member == nil {
		t.Fatalf("member after rollback = %#v, %v", member, loadErr)
	}
	properties, loadErr := s.DeadProperties.ListByResources(ctx, []string{sourcePath})
	if loadErr != nil || len(properties) != 1 || properties[0].InnerXML != "delete-state" {
		t.Fatalf("tree state after rollback = %#v, %v", properties, loadErr)
	}
}

func TestPostgres_ACLDecisionsFollowACEOrder(t *testing.T) {
	s := newPostgresStore(t)
	_, calendarID := newPostgresCalendar(t, s, "ordered-acl-owner")
	ctx := context.Background()
	viewer, err := s.Users.UpsertOAuthUser(ctx, "ordered-acl-viewer", "viewer@example.test", "Viewer", "Viewer")
	if err != nil {
		t.Fatalf("create viewer: %v", err)
	}
	resourcePath := fmt.Sprintf("/dav/calendars/%d", calendarID)
	principal := fmt.Sprintf("/dav/principals/%d/", viewer.ID)

	assertVisible := func(want bool) {
		t.Helper()
		calendars, err := s.Calendars.ListAccessible(ctx, viewer.ID)
		if err != nil {
			t.Fatalf("ListAccessible() error = %v", err)
		}
		got := false
		for _, calendar := range calendars {
			got = got || calendar.ID == calendarID
		}
		if got != want {
			t.Fatalf("calendar visibility = %v, want %v; calendars = %#v", got, want, calendars)
		}
	}

	if err := s.ACLEntries.SetACL(ctx, resourcePath, []ACLEntry{
		{PrincipalHref: principal, IsGrant: true, Privilege: "read", Position: 0},
		{PrincipalHref: principal, IsGrant: false, Privilege: "read", Position: 1},
	}); err != nil {
		t.Fatalf("set grant-first ACL: %v", err)
	}
	assertVisible(true)
	allowed, err := s.ACLEntries.HasPrivilege(ctx, resourcePath, principal, "read")
	if err != nil || !allowed {
		t.Fatalf("grant-first HasPrivilege() = %v, %v", allowed, err)
	}

	if err := s.ACLEntries.SetACL(ctx, resourcePath, []ACLEntry{
		{PrincipalHref: principal, IsGrant: false, Privilege: "read", Position: 0},
		{PrincipalHref: principal, IsGrant: true, Privilege: "read", Position: 1},
	}); err != nil {
		t.Fatalf("set deny-first ACL: %v", err)
	}
	assertVisible(false)
	allowed, err = s.ACLEntries.HasPrivilege(ctx, resourcePath, principal, "read")
	if err != nil || allowed {
		t.Fatalf("deny-first HasPrivilege() = %v, %v", allowed, err)
	}
}

func TestPostgres_ConcurrentACLReplacementsDoNotMerge(t *testing.T) {
	s := newPostgresStore(t)
	_, calendarID := newPostgresCalendar(t, s, "concurrent-acl-replacement")
	resourcePath := fmt.Sprintf("/dav/calendars/%d", calendarID)

	const writers = 12
	errs := runConcurrently(writers, func(i int) error {
		return s.ACLEntries.SetACL(context.Background(), resourcePath, []ACLEntry{{
			PrincipalHref: fmt.Sprintf("/dav/principals/%d/", i+100),
			IsGrant:       true,
			Privilege:     "read",
			Position:      0,
		}})
	})
	for _, err := range errs {
		if err != nil {
			t.Fatalf("concurrent SetACL() error = %v", err)
		}
	}
	entries, err := s.ACLEntries.ListByResource(context.Background(), resourcePath)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 1 {
		t.Fatalf("concurrent ACL replacements left %d entries, want exactly one complete replacement: %#v", len(entries), entries)
	}
}

// TestPostgres_ConcurrentACLUpdatesDoNotLoseGrants pins the lost update that a
// read-modify-write over the whole ACL allows: every writer derives its result
// from a snapshot, so without the read and the write being serialized against
// each other the last writer silently erases the others' grants.
func TestPostgres_ConcurrentACLUpdatesDoNotLoseGrants(t *testing.T) {
	s := newPostgresStore(t)
	_, calendarID := newPostgresCalendar(t, s, "concurrent-acl-update")
	resourcePath := fmt.Sprintf("/dav/calendars/%d", calendarID)

	const writers = 12
	errs := runConcurrently(writers, func(i int) error {
		return s.ACLEntries.UpdateACL(context.Background(), resourcePath, func(entries []ACLEntry) ([]ACLEntry, error) {
			return append(entries, ACLEntry{
				PrincipalHref: fmt.Sprintf("/dav/principals/%d/", i+100),
				IsGrant:       true,
				Privilege:     "read",
				Position:      i,
			}), nil
		})
	})
	for _, err := range errs {
		if err != nil {
			t.Fatalf("concurrent UpdateACL() error = %v", err)
		}
	}

	entries, err := s.ACLEntries.ListByResource(context.Background(), resourcePath)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != writers {
		t.Fatalf("concurrent UpdateACL() left %d entries, want %d: %#v", len(entries), writers, entries)
	}
}

// TestPostgres_ConcurrentShareAndUnshareKeepBothWrites runs the interleaving
// that makes a revocation and a grant clobber one another: a sharee leaving
// must not resurrect itself, and the grant landing alongside it must survive.
func TestPostgres_ConcurrentShareAndUnshareKeepBothWrites(t *testing.T) {
	s := newPostgresStore(t)
	_, calendarID := newPostgresCalendar(t, s, "concurrent-share-unshare")
	collectionPath := fmt.Sprintf("/dav/calendars/%d", calendarID)
	const leaving = "/dav/principals/200/"
	const joining = "/dav/principals/300/"
	ctx := context.Background()

	for round := 0; round < 8; round++ {
		if err := s.ACLEntries.SetACL(ctx, collectionPath, []ACLEntry{
			{PrincipalHref: leaving, IsGrant: true, Privilege: "read", Position: 0},
		}); err != nil {
			t.Fatalf("SetACL() error = %v", err)
		}

		errs := runConcurrently(2, func(i int) error {
			if i == 0 {
				return s.ACLEntries.RevokePrincipalGrants(ctx, collectionPath, leaving, []string{"read", "write"})
			}
			return s.ACLEntries.UpdateACL(ctx, collectionPath, func(entries []ACLEntry) ([]ACLEntry, error) {
				return append(entries, ACLEntry{PrincipalHref: joining, IsGrant: true, Privilege: "read", Position: 1}), nil
			})
		})
		for _, err := range errs {
			if err != nil {
				t.Fatalf("round %d: error = %v", round, err)
			}
		}

		entries, err := s.ACLEntries.ListByResource(ctx, collectionPath)
		if err != nil {
			t.Fatal(err)
		}
		var sawJoining, sawLeaving bool
		for _, entry := range entries {
			switch entry.PrincipalHref {
			case joining:
				sawJoining = true
			case leaving:
				sawLeaving = true
			}
		}
		if !sawJoining {
			t.Fatalf("round %d: the concurrent grant was lost: %#v", round, entries)
		}
		if sawLeaving {
			t.Fatalf("round %d: the revoked grant came back: %#v", round, entries)
		}
	}
}

// TestPostgres_RevokePrincipalGrantsCoversMembers pins the revocation's scope:
// member grants go with the share, denies and other principals stay.
func TestPostgres_RevokePrincipalGrantsCoversMembers(t *testing.T) {
	s := newPostgresStore(t)
	_, calendarID := newPostgresCalendar(t, s, "revoke-principal-grants")
	collectionPath := fmt.Sprintf("/dav/calendars/%d", calendarID)
	ctx := context.Background()

	const target = "/dav/principals/200/"
	seed := []ACLEntry{
		{ResourcePath: collectionPath, PrincipalHref: target, IsGrant: true, Privilege: "read"},
		{ResourcePath: collectionPath, PrincipalHref: target, IsGrant: true, Privilege: "read-acl"},
		{ResourcePath: collectionPath, PrincipalHref: target, IsGrant: false, Privilege: "write-content"},
		{ResourcePath: collectionPath + "/secret", PrincipalHref: target, IsGrant: true, Privilege: "read"},
		{ResourcePath: collectionPath + "/secret", PrincipalHref: target, IsGrant: true, Privilege: "read-acl"},
		{ResourcePath: collectionPath + "/secret", PrincipalHref: target, IsGrant: false, Privilege: "write-content"},
		{ResourcePath: collectionPath + "/secret", PrincipalHref: "/dav/principals/300/", IsGrant: true, Privilege: "read"},
		// A sibling collection whose path shares the prefix must not be swept up.
		{ResourcePath: collectionPath + "0/other", PrincipalHref: target, IsGrant: true, Privilege: "read"},
	}
	byPath := map[string][]ACLEntry{}
	for _, entry := range seed {
		byPath[entry.ResourcePath] = append(byPath[entry.ResourcePath], entry)
	}
	for resourcePath, entries := range byPath {
		if err := s.ACLEntries.SetACL(ctx, resourcePath, entries); err != nil {
			t.Fatalf("SetACL(%q) error = %v", resourcePath, err)
		}
	}

	if err := s.ACLEntries.RevokePrincipalGrants(ctx, collectionPath, target, []string{"read", "read-free-busy", "write"}); err != nil {
		t.Fatalf("RevokePrincipalGrants() error = %v", err)
	}

	for resourcePath, want := range map[string][]string{
		collectionPath:             {target + ":true:read-acl", target + ":false:write-content"},
		collectionPath + "/secret": {target + ":false:write-content", "/dav/principals/300/:true:read"},
		collectionPath + "0/other": {target + ":true:read"},
	} {
		entries, err := s.ACLEntries.ListByResource(ctx, resourcePath)
		if err != nil {
			t.Fatalf("ListByResource(%q) error = %v", resourcePath, err)
		}
		got := map[string]struct{}{}
		for _, entry := range entries {
			got[fmt.Sprintf("%s:%t:%s", entry.PrincipalHref, entry.IsGrant, entry.Privilege)] = struct{}{}
		}
		if len(got) != len(want) {
			t.Fatalf("ListByResource(%q) = %#v, want %v", resourcePath, entries, want)
		}
		for _, key := range want {
			if _, ok := got[key]; !ok {
				t.Fatalf("ListByResource(%q) lost %q: %#v", resourcePath, key, entries)
			}
		}
	}
}

// TestPostgres_RevokePrincipalGrantsMatchesEitherSlashSpelling covers the
// stored spellings the ACL evaluator folds together. Missing either would leave
// a live grant behind.
func TestPostgres_RevokePrincipalGrantsMatchesEitherSlashSpelling(t *testing.T) {
	for _, tt := range []struct {
		name, stored, revoked string
	}{
		{name: "unslashed grant, slashed revocation", stored: "/dav/principals/200", revoked: "/dav/principals/200/"},
		{name: "slashed grant, unslashed revocation", stored: "/dav/principals/200/", revoked: "/dav/principals/200"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			s := newPostgresStore(t)
			_, calendarID := newPostgresCalendar(t, s, "revoke-principal-spelling")
			collectionPath := fmt.Sprintf("/dav/calendars/%d", calendarID)
			ctx := context.Background()

			if err := s.ACLEntries.SetACL(ctx, collectionPath+"/secret", []ACLEntry{
				{PrincipalHref: tt.stored, IsGrant: true, Privilege: "read"},
			}); err != nil {
				t.Fatalf("SetACL() error = %v", err)
			}
			if err := s.ACLEntries.RevokePrincipalGrants(ctx, collectionPath, tt.revoked, []string{"read"}); err != nil {
				t.Fatalf("RevokePrincipalGrants() error = %v", err)
			}
			entries, err := s.ACLEntries.ListByResource(ctx, collectionPath+"/secret")
			if err != nil {
				t.Fatal(err)
			}
			if len(entries) != 0 {
				t.Fatalf("ListByResource() = %#v, want the %q grant revoked", entries, tt.stored)
			}
		})
	}
}

// UpdateACL rewrites every stored spelling of the resource, so the entries it
// hands mutate have to come from all of them: a grant stored under the legacy
// extension-suffixed path would otherwise be dropped by an unrelated update.
func TestPostgres_UpdateACLKeepsEntriesOfEverySpelling(t *testing.T) {
	s := newPostgresStore(t)
	_, calendarID := newPostgresCalendar(t, s, "update-acl-spellings")
	resourcePath := fmt.Sprintf("/dav/calendars/%d/meeting", calendarID)
	ctx := context.Background()

	pool := s.Calendars.(*calendarRepo).pool
	if _, err := pool.ExecContext(ctx, `INSERT INTO acl_entries (resource_path, principal_href, is_grant, privilege, ace_order) VALUES ($1, '/dav/principals/300/', TRUE, 'read', 0)`, resourcePath+".ics"); err != nil {
		t.Fatalf("store legacy-spelled grant: %v", err)
	}

	var seen []ACLEntry
	err := s.ACLEntries.UpdateACL(ctx, resourcePath, func(current []ACLEntry) ([]ACLEntry, error) {
		seen = current
		return append(current, ACLEntry{PrincipalHref: "/dav/principals/301/", IsGrant: true, Privilege: "read", Position: 1}), nil
	})
	if err != nil {
		t.Fatalf("UpdateACL() error = %v", err)
	}
	if len(seen) != 1 || seen[0].PrincipalHref != "/dav/principals/300/" {
		t.Fatalf("UpdateACL() handed mutate %#v, want the legacy-spelled grant", seen)
	}

	stored, err := s.ACLEntries.ListByResources(ctx, davStatePaths(resourcePath))
	if err != nil {
		t.Fatal(err)
	}
	granted := make(map[string]bool, len(stored))
	for _, entry := range stored {
		granted[entry.PrincipalHref] = true
	}
	if !granted["/dav/principals/300/"] || !granted["/dav/principals/301/"] || len(stored) != 2 {
		t.Fatalf("stored ACL = %#v, want the legacy grant kept beside the new one", stored)
	}
}

func isPostgresUniqueViolation(err error) bool {
	var pqErr *pq.Error
	return errors.As(err, &pqErr) && string(pqErr.Code) == "23505"
}

// runConcurrently releases n goroutines together and collects their errors in
// order, so a test asserts on the outcome of the whole race rather than on one
// arbitrary interleaving.
func runConcurrently(n int, run func(i int) error) []error {
	results := make([]error, n)
	start := make(chan struct{})
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			<-start
			results[i] = run(i)
		}(i)
	}
	close(start)
	wg.Wait()
	return results
}

// davPathLockHolder opens a transaction, takes the advisory locks the DAV path
// helper assigns to resourcePaths, and hands the transaction back still holding
// them. lockTimeout turns a wait that should not happen into an error instead of
// a hung test.
func davPathLockHolder(pool txPool, lockTimeout string, resourcePaths ...string) (*sql.Tx, error) {
	ctx := context.Background()
	tx, err := pool.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	if _, err := tx.ExecContext(ctx, `SET LOCAL lock_timeout = `+pq.QuoteLiteral(lockTimeout)); err != nil {
		tx.Rollback()
		return nil, err
	}
	if err := acquireDAVPathLocks(ctx, tx, resourcePaths...); err != nil {
		tx.Rollback()
		return nil, err
	}
	return tx, nil
}

func isPostgresLockTimeout(err error) bool {
	var pqErr *pq.Error
	return errors.As(err, &pqErr) && string(pqErr.Code) == "55P03"
}

// Advisory locks are shared by every session on one database, and test
// packages run concurrently against the same one, so tests that assert on
// blocking need paths no other run can collide with.
func uniqueDAVCollectionPath(prefix string) string {
	return fmt.Sprintf("%s/%d", prefix, time.Now().UnixNano())
}

// Every DAV path serialization set contains "/dav" and the collection kind's
// root. Taking those exclusively would cap the whole server at one DAV write at
// a time, so they must be shared: resources in different collections cannot
// conflict under RFC 4918 and must not wait on each other.
func TestPostgres_DAVPathLocksDoNotSerializeIndependentResources(t *testing.T) {
	pool := newPostgresPool(t)
	collection := uniqueDAVCollectionPath("/dav/calendars")
	holder, err := davPathLockHolder(pool, "10s", collection+"/a.ics")
	if err != nil {
		t.Fatalf("hold member advisory locks: %v", err)
	}
	defer holder.Rollback()

	for _, resourcePaths := range [][]string{
		{uniqueDAVCollectionPath("/dav/calendars") + "/a.ics"},
		{uniqueDAVCollectionPath("/dav/calendars")},
		{uniqueDAVCollectionPath("/dav/addressbooks") + "/a.vcf"},
	} {
		tx, err := davPathLockHolder(pool, "2s", resourcePaths...)
		if err != nil {
			t.Fatalf("acquiring %q while %q is held: %v; independent resources must not serialize", resourcePaths, collection+"/a.ics", err)
		}
		tx.Rollback()
	}
}

// RFC 4918 §9.10.4: a depth-infinity lock on a collection covers its members, so
// a write to a member has to observe it, and an operation on the collection
// itself has to exclude every member write.
func TestPostgres_DAVPathLocksKeepCollectionScopeExclusive(t *testing.T) {
	pool := newPostgresPool(t)
	collection := uniqueDAVCollectionPath("/dav/calendars")

	holder, err := davPathLockHolder(pool, "10s", collection)
	if err != nil {
		t.Fatalf("hold collection advisory locks: %v", err)
	}
	if _, err := davPathLockHolder(pool, "2s", collection+"/a.ics"); !isPostgresLockTimeout(err) {
		holder.Rollback()
		t.Fatalf("member write while the collection is held = %v, want a lock wait", err)
	}
	if _, err := davPathLockHolder(pool, "2s", collection); !isPostgresLockTimeout(err) {
		holder.Rollback()
		t.Fatalf("second collection-scoped operation = %v, want a lock wait", err)
	}
	holder.Rollback()

	tx, err := davPathLockHolder(pool, "2s", collection+"/a.ics")
	if err != nil {
		t.Fatalf("member write after the collection lock released: %v", err)
	}
	tx.Rollback()
}

// An ACL transaction serializes on a resource's state paths and can name an
// enclosing collection in the same pass. Acquiring a path shared as an ancestor
// and then exclusively as a target would deadlock two transactions against each
// other on the upgrade.
func TestPostgres_DAVPathLocksSurviveNestedTargetSets(t *testing.T) {
	pool := newPostgresPool(t)
	collection := uniqueDAVCollectionPath("/dav/addressbooks")
	targets := append(davStatePaths(collection+"/alice.vcf"), collection)

	errs := runConcurrently(8, func(int) error {
		for round := 0; round < 10; round++ {
			tx, err := davPathLockHolder(pool, "10s", targets...)
			if err != nil {
				return err
			}
			if err := tx.Rollback(); err != nil {
				return err
			}
		}
		return nil
	})
	for i, err := range errs {
		if err != nil {
			t.Fatalf("worker %d acquiring nested targets %q: %v", i, targets, err)
		}
	}
}

// LOCK takes the same path set as any other write to the member, so it waits
// for a write to a sibling member and for an operation on the collection, but
// not for anything in another collection.
func TestPostgres_MemberLocksSerializeOnTheirCollection(t *testing.T) {
	s := newPostgresStore(t)
	userID, _ := newPostgresCalendar(t, s, "member-lock-scope")
	collection := uniqueDAVCollectionPath("/dav/calendars")
	ctx := context.Background()

	newLock := func(resourcePath string) Lock {
		return Lock{
			Token:          "urn:uuid:member-" + resourcePath,
			ResourcePath:   resourcePath,
			UserID:         userID,
			LockScope:      "exclusive",
			LockType:       "write",
			Depth:          "0",
			TimeoutSeconds: 300,
			ExpiresAt:      time.Now().Add(5 * time.Minute),
		}
	}
	// waitsWhileHeld reports whether a LOCK on lockPath waits for holderPaths.
	// A LOCK that has to wait is seen waiting on the holder in pg_locks before
	// the holder lets go; one that does not finishes while the holder still
	// holds its locks.
	waitsWhileHeld := func(holderPaths []string, lockPath string) bool {
		t.Helper()
		holder, err := davPathLockHolder(s.pool, "30s", holderPaths...)
		if err != nil {
			t.Fatalf("hold %q: %v", holderPaths, err)
		}
		created := make(chan error, 1)
		go func() {
			_, err := s.Locks.Create(ctx, newLock(lockPath))
			created <- err
		}()
		var holderPID int
		if err := holder.QueryRowContext(ctx, `SELECT pg_backend_pid()`).Scan(&holderPID); err != nil {
			t.Fatalf("read holder backend: %v", err)
		}
		pool := s.Calendars.(*calendarRepo).pool
		for {
			select {
			case err := <-created:
				holder.Rollback()
				if err != nil {
					t.Fatalf("LOCK on %s: %v", lockPath, err)
				}
				return false
			default:
			}
			var waiting bool
			if err := pool.QueryRowContext(ctx, blockedByQuery, holderPID).Scan(&waiting); err != nil {
				t.Fatalf("read lock waits: %v", err)
			}
			if waiting {
				holder.Rollback()
				if err := <-created; err != nil {
					t.Fatalf("LOCK on %s after the holder released: %v", lockPath, err)
				}
				return true
			}
			time.Sleep(5 * time.Millisecond)
		}
	}

	if !waitsWhileHeld([]string{collection + "/a.ics"}, collection+"/b.ics") {
		t.Fatal("LOCK on a member while a sibling member write is held did not wait for it")
	}
	if !waitsWhileHeld([]string{collection}, collection+"/c.ics") {
		t.Fatal("LOCK on a member while the collection is held did not wait for it")
	}
	if waitsWhileHeld([]string{collection + "/a.ics"}, uniqueDAVCollectionPath("/dav/calendars")+"/a.ics") {
		t.Fatal("LOCK in another collection waited for an unrelated member write")
	}
}

// lockPreconditionHolder opens a transaction, runs the lock-precondition gate a
// member write runs, and hands the transaction back still holding its advisory
// locks.
func lockPreconditionHolder(pool txPool, lockTimeout string, resourcePaths ...string) (*sql.Tx, error) {
	ctx := context.Background()
	tx, err := pool.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	if _, err := tx.ExecContext(ctx, `SET LOCAL lock_timeout = `+pq.QuoteLiteral(lockTimeout)); err != nil {
		tx.Rollback()
		return nil, err
	}
	preconditions := make([]LockPrecondition, 0, len(resourcePaths))
	for _, resourcePath := range resourcePaths {
		preconditions = append(preconditions, LockPrecondition{ResourcePath: resourcePath})
	}
	if err := validateLockPreconditionsTx(ctx, tx, preconditions); err != nil {
		tx.Rollback()
		return nil, err
	}
	return tx, nil
}

// The paths a caller hands the gate to serialize on its own behalf are the ones
// a later step of the same transaction writes, so the transaction has to hold
// them however few lock preconditions the request carried: a request with none
// still writes that state, and two of them would otherwise write it together.
func TestPostgres_LockPreconditionGateSerializesCallerPathsWithoutPreconditions(t *testing.T) {
	pool := newPostgresPool(t)
	collection := uniqueDAVCollectionPath("/dav/addressbooks")
	statePaths := davStatePaths(collection + "/alice.vcf")
	ctx := context.Background()

	holder, err := pool.BeginTx(ctx, nil)
	if err != nil {
		t.Fatalf("begin: %v", err)
	}
	defer holder.Rollback()
	if err := validateLockPreconditionsTx(ctx, holder, nil, statePaths...); err != nil {
		t.Fatalf("validateLockPreconditionsTx() error = %v", err)
	}

	for _, statePath := range statePaths {
		tx, err := davPathLockHolder(pool, "2s", statePath)
		if err == nil {
			tx.Rollback()
			t.Fatalf("acquiring %q while the gate holds it = nil, want a lock wait", statePath)
		}
		if !isPostgresLockTimeout(err) {
			t.Fatalf("acquiring %q while the gate holds it = %v, want a lock wait", statePath, err)
		}
	}
}

// A year-less February 29 has to survive the round trip through the DATE
// column, and a row an earlier release stored in year 1 reads back as year-less.
func TestPostgres_YearlessBirthdaysRoundTrip(t *testing.T) {
	s := newPostgresStore(t)
	ctx := context.Background()
	user, err := s.Users.UpsertOAuthUser(ctx, "birthday-round-trip", "birthday@example.test", "", "")
	if err != nil {
		t.Fatalf("create user: %v", err)
	}
	book, err := s.AddressBooks.Create(ctx, AddressBook{UserID: user.ID, Name: "Birthdays"})
	if err != nil {
		t.Fatalf("create address book: %v", err)
	}
	for _, tt := range []struct {
		uid, bday string
		want      time.Time
	}{
		{uid: "leap", bday: "--02-29", want: time.Date(NoYearBirthdayYear, 2, 29, 0, 0, 0, 0, time.UTC)},
		{uid: "basic", bday: "--1231", want: time.Date(NoYearBirthdayYear, 12, 31, 0, 0, 0, 0, time.UTC)},
		{uid: "dated", bday: "19900515", want: time.Date(1990, 5, 15, 0, 0, 0, 0, time.UTC)},
	} {
		if _, err := s.Contacts.Upsert(ctx, Contact{
			AddressBookID: book.ID, UID: tt.uid, ResourceName: tt.uid, ETag: tt.uid,
			RawVCard: "BEGIN:VCARD\r\nVERSION:4.0\r\nUID:" + tt.uid + "\r\nFN:X\r\nBDAY:" + tt.bday + "\r\nEND:VCARD\r\n",
		}); err != nil {
			t.Fatalf("store %s: %v", tt.uid, err)
		}
		stored, err := s.Contacts.GetByUID(ctx, book.ID, tt.uid)
		if err != nil || stored == nil || stored.Birthday == nil {
			t.Fatalf("load %s: %v %+v", tt.uid, err, stored)
		}
		if !stored.Birthday.Equal(tt.want) {
			t.Errorf("BDAY:%s read back as %v, want %v", tt.bday, stored.Birthday, tt.want)
		}
	}

	pool := s.Calendars.(*calendarRepo).pool
	if _, err := pool.ExecContext(ctx, `UPDATE contacts SET birthday = DATE '0001-07-04' WHERE address_book_id=$1 AND uid='basic'`, book.ID); err != nil {
		t.Fatalf("store a legacy year-less birthday: %v", err)
	}
	legacy, err := s.Contacts.GetByUID(ctx, book.ID, "basic")
	if err != nil || legacy == nil || legacy.Birthday == nil {
		t.Fatalf("load legacy birthday: %v", err)
	}
	if want := time.Date(NoYearBirthdayYear, 7, 4, 0, 0, 0, 0, time.UTC); !legacy.Birthday.Equal(want) {
		t.Errorf("legacy year-1 birthday read back as %v, want %v", legacy.Birthday, want)
	}
}
