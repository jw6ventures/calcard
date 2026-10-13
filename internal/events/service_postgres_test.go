package events

import (
	"database/sql"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	_ "github.com/lib/pq"

	"github.com/jw6ventures/calcard/internal/auth"
	"github.com/jw6ventures/calcard/internal/dav"
	"github.com/jw6ventures/calcard/internal/store"
)

func newEventsPostgresStore(t *testing.T) *store.Store {
	t.Helper()
	dsn := os.Getenv("CALCARD_TEST_POSTGRES_DSN")
	if dsn == "" {
		t.Skip("CALCARD_TEST_POSTGRES_DSN is unset; set it to run the PostgreSQL event access test")
	}
	schema := fmt.Sprintf("calcard_events_test_%d", time.Now().UnixNano())
	admin, err := sql.Open("postgres", dsn)
	if err != nil {
		t.Fatalf("open PostgreSQL: %v", err)
	}
	if _, err := admin.Exec(`CREATE SCHEMA ` + schema); err != nil {
		admin.Close()
		t.Fatalf("create schema %s: %v", schema, err)
	}
	parsed, err := url.Parse(dsn)
	if err != nil {
		admin.Close()
		t.Fatalf("parse CALCARD_TEST_POSTGRES_DSN: %v", err)
	}
	query := parsed.Query()
	query.Set("search_path", schema)
	parsed.RawQuery = query.Encode()
	pool, err := sql.Open("postgres", parsed.String())
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
	pool.SetMaxOpenConns(5)
	return store.New(pool)
}

// A user the store finds no grant for on a calendar or any of its events
// reads, changes and removes none of them, and learns nothing of which UIDs it
// holds; one the store finds a grant for still reaches what it grants.
func TestPostgres_EventsAreHiddenFromAUserWithNoAccessToTheirCalendar(t *testing.T) {
	database := newEventsPostgresStore(t)
	ctx := t.Context()
	newUser := func(name string) *store.User {
		user, err := database.Users.UpsertOAuthUser(ctx, name, name+"@example.test", name, name)
		if err != nil {
			t.Fatalf("create user %s: %v", name, err)
		}
		return user
	}
	owner, stranger, reader, objectReader := newUser("owner"), newUser("stranger"), newUser("reader"), newUser("object-reader")
	cal, err := database.Calendars.Create(ctx, store.Calendar{UserID: owner.ID, Name: "Private"})
	if err != nil {
		t.Fatalf("create calendar: %v", err)
	}
	svc := NewService(database, dav.ValidateCalendarObject)
	if _, err := svc.CreateEvent(ctx, owner, cal.ID, UpsertInput{RawICS: validICS("stored")}); err != nil {
		t.Fatalf("CreateEvent() by the owner: %v", err)
	}
	collection := fmt.Sprintf("/dav/calendars/%d", cal.ID)
	if err := database.ACLEntries.SetACL(ctx, collection, []store.ACLEntry{{PrincipalHref: fmt.Sprintf("/dav/principals/%d/", reader.ID), IsGrant: true, Privilege: "read"}}); err != nil {
		t.Fatalf("grant read on the calendar: %v", err)
	}
	if err := database.ACLEntries.SetACL(ctx, collection+"/stored.ics", []store.ACLEntry{{PrincipalHref: fmt.Sprintf("/dav/principals/%d/", objectReader.ID), IsGrant: true, Privilege: "read"}}); err != nil {
		t.Fatalf("grant read on the event: %v", err)
	}

	for _, uid := range []string{"stored", "missing"} {
		if ev, err := svc.GetEvent(ctx, stranger, cal.ID, uid); !errors.Is(err, ErrNotFound) {
			t.Errorf("GetEvent(%q) by a stranger = %v, %v; want ErrNotFound", uid, ev != nil, err)
		}
		if _, _, err := svc.UpdateEvent(ctx, stranger, cal.ID, uid, UpsertInput{RawICS: validICS(uid), IfNoneMatch: "*"}); !errors.Is(err, ErrNotFound) {
			t.Errorf("UpdateEvent(%q) by a stranger error = %v, want ErrNotFound", uid, err)
		}
		if err := svc.DeleteEvent(ctx, stranger, cal.ID, uid, `"etag"`, ""); !errors.Is(err, ErrNotFound) {
			t.Errorf("DeleteEvent(%q) by a stranger error = %v, want ErrNotFound", uid, err)
		}
	}
	for _, user := range []*store.User{owner, reader, objectReader} {
		if _, err := svc.GetEvent(ctx, user, cal.ID, "stored"); err != nil {
			t.Errorf("GetEvent() by %s error = %v, want the event", user.PrimaryEmail, err)
		}
	}
	if err := svc.DeleteEvent(ctx, reader, cal.ID, "stored", "", ""); !errors.Is(err, ErrForbidden) {
		t.Errorf("DeleteEvent() by a reader error = %v, want ErrForbidden", err)
	}

	// A create is refused before the event is built or validated: 404 to a
	// user who can see nothing of the calendar, and 403 to one who can see it
	// but lacks DAV:bind on it.
	validations := 0
	counted := NewService(database, countingValidator(&validations))
	for _, tc := range []struct {
		user *store.User
		want error
	}{{stranger, ErrNotFound}, {reader, ErrForbidden}, {objectReader, ErrForbidden}} {
		if _, err := counted.CreateEvent(ctx, tc.user, cal.ID, UpsertInput{RawICS: validICS("new")}); !errors.Is(err, tc.want) {
			t.Errorf("CreateEvent() by %s error = %v, want %v", tc.user.PrimaryEmail, err, tc.want)
		}
	}
	if validations != 0 {
		t.Errorf("validator ran %d times for users who may not create", validations)
	}
}

// Concurrent creates of one UID store exactly one event: every other is
// refused with a conflict instead of overwriting the one that won.
func TestPostgres_ConcurrentCreatesOfAUIDStoreOneEvent(t *testing.T) {
	database := newEventsPostgresStore(t)
	ctx := t.Context()
	owner, err := database.Users.UpsertOAuthUser(ctx, "racer", "racer@example.test", "Racer", "Racer")
	if err != nil {
		t.Fatalf("create user: %v", err)
	}
	cal, err := database.Calendars.Create(ctx, store.Calendar{UserID: owner.ID, Name: "Race"})
	if err != nil {
		t.Fatalf("create calendar: %v", err)
	}
	svc := NewService(database, dav.ValidateCalendarObject)

	const writers = 8
	type outcome struct {
		event *store.Event
		err   error
	}
	outcomes := make([]outcome, writers)
	start := make(chan struct{})
	var wg sync.WaitGroup
	for i := range writers {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			<-start
			body := strings.Replace(validICS("raced"), "SUMMARY:Planning", fmt.Sprintf("SUMMARY:Writer %d", i), 1)
			event, err := svc.CreateEvent(ctx, owner, cal.ID, UpsertInput{RawICS: body})
			outcomes[i] = outcome{event, err}
		}(i)
	}
	close(start)
	wg.Wait()

	var winner *store.Event
	for i, o := range outcomes {
		switch {
		case o.err == nil && winner == nil:
			winner = o.event
		case o.err == nil:
			t.Errorf("writer %d also created the event", i)
		case !errors.Is(o.err, ErrConflict):
			t.Errorf("writer %d error = %v, want ErrConflict", i, o.err)
		}
	}
	if winner == nil {
		t.Fatal("no writer created the event")
	}
	stored, err := database.Events.GetByUID(ctx, cal.ID, "raced")
	if err != nil || stored == nil {
		t.Fatalf("load the event: %v", err)
	}
	if stored.ETag != winner.ETag || stored.RawICAL != winner.RawICAL {
		t.Fatalf("stored %q, want the winner's %q", stored.RawICAL, winner.RawICAL)
	}
}

// eventWriteAccessFixture is eventWriteAccessCases over a PostgreSQL store: a
// calendar holding the event "stored" and a user for each case holding the
// ACEs it names.
type eventWriteAccessFixture struct {
	database *store.Store
	owner    *store.User
	users    []*store.User
	calendar *store.Calendar
	acl      map[string][]store.ACLEntry
	etag     string
}

func newEventWriteAccessFixture(t *testing.T) *eventWriteAccessFixture {
	t.Helper()
	database := newEventsPostgresStore(t)
	ctx := t.Context()
	newUser := func(name string) *store.User {
		user, err := database.Users.UpsertOAuthUser(ctx, name, name+"@example.test", name, name)
		if err != nil {
			t.Fatalf("create user %s: %v", name, err)
		}
		return user
	}
	f := &eventWriteAccessFixture{database: database, owner: newUser("owner"), acl: map[string][]store.ACLEntry{}}
	cal, err := database.Calendars.Create(ctx, store.Calendar{UserID: f.owner.ID, Name: "Private"})
	if err != nil {
		t.Fatalf("create calendar: %v", err)
	}
	f.calendar = cal
	for i, tc := range eventWriteAccessCases {
		user := newUser(fmt.Sprintf("principal-%d", i))
		f.users = append(f.users, user)
		for path, entries := range eventWriteAccessEntries(cal.ID, user.ID, tc.aces) {
			f.acl[path] = append(f.acl[path], entries...)
		}
	}
	f.reset(t)
	return f
}

// reset restores the event "stored" and every ACE, which deleting the event
// removes with it.
func (f *eventWriteAccessFixture) reset(t *testing.T) {
	t.Helper()
	ctx := t.Context()
	stored, err := f.database.Events.GetByUID(ctx, f.calendar.ID, "stored")
	if err != nil {
		t.Fatalf("load the stored event: %v", err)
	}
	if stored == nil {
		svc := NewService(f.database, dav.ValidateCalendarObject)
		if stored, err = svc.CreateEvent(ctx, f.owner, f.calendar.ID, UpsertInput{RawICS: validICS("stored")}); err != nil {
			t.Fatalf("restore the stored event: %v", err)
		}
	}
	f.etag = stored.ETag
	for path, entries := range f.acl {
		if err := f.database.ACLEntries.SetACL(ctx, path, entries); err != nil {
			t.Fatalf("set the ACL of %s: %v", path, err)
		}
	}
}

// storedUnchanged reports whether the event "stored" is as reset left it.
func (f *eventWriteAccessFixture) storedUnchanged(t *testing.T) bool {
	t.Helper()
	stored, err := f.database.Events.GetByUID(t.Context(), f.calendar.ID, "stored")
	if err != nil {
		t.Fatalf("load the stored event: %v", err)
	}
	return stored != nil && stored.ETag == f.etag
}

// The access rules eventWriteAccessCases pins hold against the store's own
// reading of who may reach the calendar, not only against the fakes.
func TestPostgres_EventUpdatesAndDeletesFollowTheAccessRules(t *testing.T) {
	f := newEventWriteAccessFixture(t)
	svc := NewService(f.database, dav.ValidateCalendarObject)
	for i, tc := range eventWriteAccessCases {
		for _, op := range []string{"update", "delete"} {
			outcomes := tc.update
			if op == "delete" {
				outcomes = tc.delete
			}
			for h, header := range eventWriteAccessHeaders {
				t.Run(tc.principal+"/"+op+"/"+header.name, func(t *testing.T) {
					got := map[string]error{}
					for _, uid := range []string{"stored", "missing"} {
						f.reset(t)
						ifMatch := header.ifMatch
						if ifMatch != "" {
							ifMatch = `"` + f.etag + `"`
						}
						var err error
						if op == "update" {
							_, _, err = svc.UpdateEvent(t.Context(), f.users[i], f.calendar.ID, uid, UpsertInput{
								RawICS: validICS(uid), IfMatch: ifMatch, IfNoneMatch: header.ifNoneMatch,
							})
						} else {
							err = svc.DeleteEvent(t.Context(), f.users[i], f.calendar.ID, uid, ifMatch, header.ifNoneMatch)
						}
						got[uid] = err
						if err != nil && !f.storedUnchanged(t) {
							t.Errorf("%s of %q answered %v but changed the stored event", op, uid, err)
						}
					}
					assertEventWriteOutcome(t, tc, outcomes[h], got)
				})
			}
		}
	}
}

// A REST delete is decided as a DAV DELETE of the same event is
// (internal/dav/delete.go): DAV:unbind on the calendar and some privilege on
// the event, with a principal holding no privilege on the event answered as
// for a UID the calendar does not hold. They differ, deliberately, for a
// principal holding no privilege on the calendar either, only ACEs that grant
// none or deny every one: DAV answers 403, which confirms the calendar exists,
// and REST answers 404, as REST GetEvent does. Neither answer depends on
// whether the event exists. eventWriteAccessCase.davDelete names each such
// case and DAV's answer, which this test asserts.
func TestPostgres_RESTAndDAVDeletesAgree(t *testing.T) {
	f := newEventWriteAccessFixture(t)
	svc := NewService(f.database, dav.ValidateCalendarObject)
	davServer := dav.NewDavServer(dav.Options{Store: f.database})
	for i, tc := range eventWriteAccessCases {
		for h, header := range eventWriteAccessHeaders {
			t.Run(tc.principal+"/"+header.name, func(t *testing.T) {
				// davWant holds DAV's answer for each UID where it is not REST's.
				davWant := map[string]int{}
				if tc.davDelete != nil {
					if davAnswer := tc.davDelete[h].stored; davAnswer != tc.delete[h].stored {
						davWant["stored"] = StatusCode(davAnswer)
					}
					if davAnswer := tc.davDelete[h].missing; davAnswer != tc.delete[h].missing {
						davWant["missing"] = StatusCode(davAnswer)
					}
				}
				rest := map[string]int{}
				for _, uid := range []string{"stored", "missing"} {
					f.reset(t)
					ifMatch := header.ifMatch
					if ifMatch != "" {
						ifMatch = `"` + f.etag + `"`
					}
					restStatus := http.StatusNoContent
					if err := svc.DeleteEvent(t.Context(), f.users[i], f.calendar.ID, uid, ifMatch, header.ifNoneMatch); err != nil {
						restStatus = StatusCode(err)
					}
					rest[uid] = restStatus

					f.reset(t)
					req := httptest.NewRequest(http.MethodDelete, fmt.Sprintf("/dav/calendars/%d/%s.ics", f.calendar.ID, uid), nil)
					if ifMatch != "" {
						req.Header.Set("If-Match", ifMatch)
					}
					if header.ifNoneMatch != "" {
						req.Header.Set("If-None-Match", header.ifNoneMatch)
					}
					req = req.WithContext(auth.WithUser(req.Context(), f.users[i]))
					rec := httptest.NewRecorder()
					davServer.Delete(rec, req)

					want, differs := davWant[uid]
					switch {
					case differs && rec.Code != want:
						t.Errorf("DELETE of %q: DAV answered %d, want %d (REST %d)", uid, rec.Code, want, restStatus)
					case differs && restStatus == want:
						t.Errorf("DELETE of %q: REST answered %d, as DAV does, where it conceals the calendar", uid, restStatus)
					case !differs && rec.Code != restStatus:
						t.Errorf("DELETE of %q: DAV answered %d, REST %d", uid, rec.Code, restStatus)
					}
				}
				if tc.concealed && rest["stored"] != rest["missing"] {
					t.Errorf("REST answers differ by existence: %d for the stored UID, %d for a missing one", rest["stored"], rest["missing"])
				}
			})
		}
	}
}
