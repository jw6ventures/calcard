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

	schemaSQL, err := os.ReadFile(filepath.Join("..", "..", "db.sql"))
	if err != nil {
		t.Fatalf("read db.sql: %v", err)
	}
	if _, err := pool.Exec(string(schemaSQL)); err != nil {
		t.Fatalf("apply db.sql: %v", err)
	}
	return New(pool)
}

func withSearchPath(t *testing.T, dsn, schema string) string {
	t.Helper()
	if strings.HasPrefix(dsn, "postgres://") || strings.HasPrefix(dsn, "postgresql://") {
		parsed, err := url.Parse(dsn)
		if err != nil {
			t.Fatalf("parse %s: %v", postgresDSNEnv, err)
		}
		query := parsed.Query()
		query.Set("search_path", schema)
		parsed.RawQuery = query.Encode()
		return parsed.String()
	}
	return dsn + " search_path=" + schema
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
