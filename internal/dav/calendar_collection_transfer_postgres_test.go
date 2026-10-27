package dav

import (
	"context"
	"database/sql"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jw6ventures/calcard/internal/acl"
	"github.com/jw6ventures/calcard/internal/auth"
	"github.com/jw6ventures/calcard/internal/store"
)

// A collection COPY by a sharee reads and writes storage a fixed number of
// times however many members the collection holds: the preflight decides every
// member's read from one read of the collection's ACLs, and the store copies
// the members in bulk.
func TestPostgres_CalendarCollectionCopyQueriesDoNotGrowWithMembers(t *testing.T) {
	queriesFor := func(t *testing.T, members int) int {
		t.Helper()
		const method = "COPY"
		database, queries := newDAVPostgresStoreCountingQueries(t)
		ctx := t.Context()
		owner, err := database.Users.UpsertOAuthUser(ctx, "owner", "owner@example.test", "Owner", "Owner")
		if err != nil {
			t.Fatal(err)
		}
		sharee, err := database.Users.UpsertOAuthUser(ctx, "sharee", "sharee@example.test", "Sharee", "Sharee")
		if err != nil {
			t.Fatal(err)
		}
		slug := "source"
		cal, err := database.Calendars.Create(ctx, store.Calendar{UserID: owner.ID, Name: "Source", Slug: &slug})
		if err != nil {
			t.Fatal(err)
		}
		root := calendarCollectionResourcePath(cal.ID)
		if err := database.ACLEntries.SetACL(ctx, root, []store.ACLEntry{{PrincipalHref: acl.PrincipalHref(sharee.ID), IsGrant: true, Privilege: "read"}}); err != nil {
			t.Fatal(err)
		}
		for i := 0; i < members; i++ {
			// Members are named as DAV clients name them, as UI-created objects
			// are, and with a dot of their own; every fifth carries its own ACL.
			name := fmt.Sprintf("member-%03d", i)
			switch i % 3 {
			case 1:
				name += ".ics"
			case 2:
				name += "@example.com"
			}
			raw := buildCalendarObject(buildVEvent("uid-"+name, "SUMMARY:x"))
			if _, err := database.Events.Upsert(ctx, store.Event{CalendarID: cal.ID, UID: "uid-" + name, ResourceName: name, RawICAL: raw, ETag: "e-" + name}); err != nil {
				t.Fatal(err)
			}
			if i%5 == 0 {
				if err := database.ACLEntries.SetACL(ctx, root+"/"+name, []store.ACLEntry{{PrincipalHref: acl.PrincipalHref(sharee.ID), IsGrant: true, Privilege: "read"}}); err != nil {
					t.Fatal(err)
				}
			}
		}
		user := sharee
		h := NewDavServer(Options{Store: database})
		req := httptest.NewRequest(method, root+"/", nil)
		req.Header.Set("Destination", "/dav/calendars/transferred/")
		req = req.WithContext(auth.WithUser(req.Context(), user))
		rr := httptest.NewRecorder()
		queries.reset()
		h.ServeHTTP(rr, req)
		if rr.Code != http.StatusCreated {
			t.Fatalf("%s = %d: %s", method, rr.Code, rr.Body.String())
		}
		transferred, err := database.Calendars.ListByUser(ctx, user.ID)
		if err != nil {
			t.Fatal(err)
		}
		for _, calendar := range transferred {
			if calendar.Slug == nil || *calendar.Slug != "transferred" {
				continue
			}
			events, err := database.Events.ListForCalendar(ctx, calendar.ID)
			if err != nil {
				t.Fatal(err)
			}
			if len(events) != members {
				t.Fatalf("%s carried %d members, want %d", method, len(events), members)
			}
			t.Logf("%s of %d members: %d queries\n%s", method, members, queries.count(), queries.summary())
			return queries.count()
		}
		t.Fatalf("%s left no transferred calendar", method)
		return 0
	}
	if few, many := queriesFor(t, 4), queriesFor(t, 40); few != many {
		t.Errorf("COPY: %d queries for 4 members, %d for 40", few, many)
	}
}

// A collection MOVE or DELETE writes every member, and a COPY or MOVE over an
// existing collection removes every member of that one, so a lock on any such
// member stops the request unless its token is submitted, whatever the member
// is named. So does a lock on an unmapped member, a lock-null resource the
// request would remove with the collection (RFC 4918 §9.6.1, §9.9.2).
func TestPostgres_CalendarCollectionWritesHonourEveryMembersLock(t *testing.T) {
	const unmapped = "unmapped"
	for _, request := range []struct {
		method          string
		lockDestination bool
	}{{"MOVE", false}, {"DELETE", false}, {"MOVE", true}, {"COPY", true}} {
		for _, name := range []string{"plain", "uid@example.com", "dav.ics", "Shout.ICS", unmapped} {
			label := request.method + " " + name
			if request.lockDestination {
				label = request.method + " over " + name
			}
			t.Run(label, func(t *testing.T) {
				database := newDAVPostgresStore(t)
				ctx := t.Context()
				owner, err := database.Users.UpsertOAuthUser(ctx, "owner", "owner@example.test", "Owner", "Owner")
				if err != nil {
					t.Fatal(err)
				}
				newCalendar := func(slug string, members ...string) *store.Calendar {
					t.Helper()
					cal, err := database.Calendars.Create(ctx, store.Calendar{UserID: owner.ID, Name: slug, Slug: &slug})
					if err != nil {
						t.Fatal(err)
					}
					for _, member := range members {
						if _, err := database.Events.Upsert(ctx, store.Event{CalendarID: cal.ID, UID: "uid-" + member, ResourceName: member, RawICAL: buildCalendarObject(buildVEvent("uid-"+member, "SUMMARY:x")), ETag: "e-" + member}); err != nil {
							t.Fatal(err)
						}
					}
					return cal
				}
				members := []string{name, "other"}
				if name == unmapped {
					members = []string{"other"}
				}
				source := newCalendar("source", members...)
				locked := source
				if request.lockDestination {
					locked = newCalendar("dest", members...)
				}
				h := NewDavServer(Options{Store: database})
				do := func(method, target string, header map[string]string, body string) *httptest.ResponseRecorder {
					req := httptest.NewRequest(method, target, strings.NewReader(body))
					for key, value := range header {
						req.Header.Set(key, value)
					}
					req = req.WithContext(auth.WithUser(req.Context(), owner))
					rr := httptest.NewRecorder()
					h.ServeHTTP(rr, req)
					return rr
				}
				const exclusive = `<?xml version="1.0" encoding="utf-8"?><d:lockinfo xmlns:d="DAV:"><d:lockscope><d:exclusive/></d:lockscope><d:locktype><d:write/></d:locktype></d:lockinfo>`
				lockedHref := calendarCollectionResourcePath(locked.ID) + "/" + url.PathEscape(name) + ".ics"
				wantLock := http.StatusOK
				if name == unmapped {
					wantLock = http.StatusCreated
				}
				lock := do("LOCK", lockedHref, map[string]string{"Depth": "0"}, exclusive)
				if lock.Code != wantLock {
					t.Fatalf("LOCK = %d, want %d: %s", lock.Code, wantLock, lock.Body.String())
				}
				token := strings.Trim(lock.Header().Get("Lock-Token"), "<>")
				header := map[string]string{}
				switch {
				case request.lockDestination:
					header["Destination"] = "/dav/calendars/dest/"
					header["Overwrite"] = "T"
				case request.method == "MOVE":
					header["Destination"] = "/dav/calendars/moved/"
				}
				if rr := do(request.method, calendarCollectionResourcePath(source.ID)+"/", header, ""); rr.Code != http.StatusLocked {
					t.Errorf("%s with member %q locked = %d, want 423: %s", label, name, rr.Code, rr.Body.String())
				}
				if events, err := database.Events.ListForCalendar(ctx, locked.ID); err != nil || len(events) != len(members) {
					t.Errorf("locked collection's members after the refused %s = %d, %v", label, len(events), err)
				}
				if held, err := database.Locks.GetByToken(ctx, token); err != nil || held == nil {
					t.Errorf("the refused %s left lock %s = %#v, %v", label, token, held, err)
				}
				header["If"] = "(<" + token + ">)"
				if rr := do(request.method, calendarCollectionResourcePath(source.ID)+"/", header, ""); rr.Code >= 300 {
					t.Errorf("%s with member %q locked and its token = %d: %s", label, name, rr.Code, rr.Body.String())
				}
			})
		}
	}
}

// An ACE names its principal by any href that denotes it, and a deny stored
// under another spelling of the sharee's principal URL than the canonical one
// still refuses the sharee: GET and every listing hide the member, and a
// collection COPY that would carry it is refused.
func TestPostgres_MemberDenyHoldsWhateverSpellingNamesThePrincipal(t *testing.T) {
	database := newDAVPostgresStore(t)
	ctx := t.Context()
	owner, err := database.Users.UpsertOAuthUser(ctx, "owner", "owner@example.test", "Owner", "Owner")
	if err != nil {
		t.Fatal(err)
	}
	sharee, err := database.Users.UpsertOAuthUser(ctx, "sharee", "sharee@example.test", "Sharee", "Sharee")
	if err != nil {
		t.Fatal(err)
	}
	cal, err := database.Calendars.Create(ctx, store.Calendar{UserID: owner.ID, Name: "Shared"})
	if err != nil {
		t.Fatal(err)
	}
	root := calendarCollectionResourcePath(cal.ID)
	shareeHref := acl.PrincipalHref(sharee.ID)
	if err := database.ACLEntries.SetACL(ctx, root, []store.ACLEntry{{PrincipalHref: shareeHref, IsGrant: true, Privilege: "read"}}); err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"open", "secret"} {
		if _, err := database.Events.Upsert(ctx, store.Event{CalendarID: cal.ID, UID: "uid-" + name, ResourceName: name, RawICAL: buildCalendarObject(buildVEvent("uid-"+name, "SUMMARY:x")), ETag: "e-" + name}); err != nil {
			t.Fatal(err)
		}
	}
	if err := database.ACLEntries.SetACL(ctx, root+"/secret", []store.ACLEntry{{PrincipalHref: strings.TrimSuffix(shareeHref, "/"), IsGrant: false, Privilege: "read"}}); err != nil {
		t.Fatal(err)
	}
	h := NewDavServer(Options{Store: database})
	do := func(method, target, depth, body string, header map[string]string) *httptest.ResponseRecorder {
		req := httptest.NewRequest(method, target, strings.NewReader(body))
		if depth != "" {
			req.Header.Set("Depth", depth)
		}
		for key, value := range header {
			req.Header.Set(key, value)
		}
		req = req.WithContext(auth.WithUser(req.Context(), sharee))
		rr := httptest.NewRecorder()
		h.ServeHTTP(rr, req)
		return rr
	}
	secretHref := root + "/secret.ics"
	if get := do(http.MethodGet, secretHref, "", "", nil); get.Code == http.StatusOK {
		t.Fatalf("GET of the denied member = 200")
	}
	for _, listing := range []struct{ method, body string }{
		{"PROPFIND", memberDecisionPropfindBody},
		{"REPORT", memberDecisionSyncBody},
		{"REPORT", memberDecisionCalendarQueryBody},
	} {
		rr := do(listing.method, root+"/", "1", listing.body, nil)
		if strings.Contains(rr.Body.String(), secretHref) {
			t.Errorf("%s %.40q lists the denied member: %s", listing.method, listing.body, rr.Body.String())
		}
	}
	copied := do("COPY", root+"/", "infinity", "", map[string]string{"Destination": "/dav/calendars/mine/"})
	if copied.Code != http.StatusMultiStatus {
		t.Fatalf("COPY = %d, want 207 refusing the denied member: %s", copied.Code, copied.Body.String())
	}
	decodeMultistatus(t, copied).assertHrefs(t, secretHref)
}

// A collection DELETE or MOVE holds the collection itself exclusively, which
// every write to one of its members waits on, so it takes a fixed number of
// advisory locks however many members it removes: one per member would run
// PostgreSQL's shared lock table out for a large collection.
func TestPostgres_CalendarCollectionWritesTakeAdvisoryLocksIndependentOfMembers(t *testing.T) {
	for _, method := range []string{"DELETE", "MOVE"} {
		t.Run(method, func(t *testing.T) {
			locksFor := func(members int) int {
				t.Helper()
				var pool *sql.DB
				database := openDAVPostgresStore(t, func(dsn string) (*sql.DB, error) {
					var err error
					pool, err = sql.Open("postgres", dsn)
					return pool, err
				})
				ctx := t.Context()
				if _, err := pool.ExecContext(ctx, `
CREATE TABLE advisory_lock_counts (held integer NOT NULL);
CREATE FUNCTION record_advisory_locks() RETURNS trigger AS $$
BEGIN
    INSERT INTO advisory_lock_counts SELECT count(*) FROM pg_locks WHERE locktype = 'advisory' AND pid = pg_backend_pid();
    RETURN OLD;
END;
$$ LANGUAGE plpgsql;
CREATE TRIGGER record_advisory_locks BEFORE DELETE ON calendars FOR EACH ROW EXECUTE FUNCTION record_advisory_locks()`); err != nil {
					t.Fatal(err)
				}
				owner, err := database.Users.UpsertOAuthUser(ctx, "owner", "owner@example.test", "Owner", "Owner")
				if err != nil {
					t.Fatal(err)
				}
				slug := "source"
				cal, err := database.Calendars.Create(ctx, store.Calendar{UserID: owner.ID, Name: slug, Slug: &slug})
				if err != nil {
					t.Fatal(err)
				}
				for i := range members {
					uid := fmt.Sprintf("member-%d", i)
					if _, err := database.Events.Upsert(ctx, store.Event{CalendarID: cal.ID, UID: uid, ResourceName: uid, RawICAL: buildCalendarObject(buildVEvent(uid, "SUMMARY:x")), ETag: "e-" + uid}); err != nil {
						t.Fatal(err)
					}
				}
				req := httptest.NewRequest(method, calendarCollectionResourcePath(cal.ID)+"/", nil)
				if method == "MOVE" {
					req.Header.Set("Destination", "/dav/calendars/moved/")
				}
				rr := httptest.NewRecorder()
				NewDavServer(Options{Store: database}).ServeHTTP(rr, req.WithContext(auth.WithUser(req.Context(), owner)))
				if rr.Code >= 300 {
					t.Fatalf("%s of %d members = %d: %s", method, members, rr.Code, rr.Body.String())
				}
				var held int
				if err := pool.QueryRowContext(ctx, `SELECT held FROM advisory_lock_counts`).Scan(&held); err != nil {
					t.Fatal(err)
				}
				return held
			}
			if few, many := locksFor(3), locksFor(60); few != many {
				t.Errorf("%s held %d advisory locks for 3 members and %d for 60", method, few, many)
			}
		})
	}
}

// writtenMember is one member a writer created, with the instants that bound
// when it existed: it was certainly stored from created on, and certainly gone
// from deleted on, while a delete sent at deleteSent may have removed it any
// time after.
type writtenMember struct {
	body       string
	created    time.Time
	deleteSent time.Time
	deleted    time.Time
}

func writtenMemberBody(name string) string {
	return fmt.Sprintf("BEGIN:VCALENDAR\r\nVERSION:2.0\r\nPRODID:-//Review//EN\r\nBEGIN:VEVENT\r\nUID:%s\r\nSUMMARY:%s\r\nDTSTAMP:20260101T000000Z\r\nDTSTART:20260101T100000Z\r\nDTEND:20260101T110000Z\r\nEND:VEVENT\r\nEND:VCALENDAR\r\n", name, name)
}

func serveCalendarRequest(ctx context.Context, user *store.User, h *DavServer, method, target, payload string) *httptest.ResponseRecorder {
	req := httptest.NewRequest(method, target, strings.NewReader(payload))
	if payload != "" {
		req.Header.Set("Content-Type", "text/calendar")
	}
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, req.WithContext(auth.WithUser(ctx, user)))
	return rr
}

// seedWrittenMembers PUTs count members named prefix-0 onwards into the
// calendar at root.
func seedWrittenMembers(t *testing.T, db *store.Store, user *store.User, root, prefix string, count int) map[string]*writtenMember {
	t.Helper()
	h := NewDavServer(Options{Store: db})
	members := make(map[string]*writtenMember, count)
	for i := 0; i < count; i++ {
		name := fmt.Sprintf("%s-%d", prefix, i)
		if rr := serveCalendarRequest(t.Context(), user, h, http.MethodPut, root+"/"+name+".ics", writtenMemberBody(name)); rr.Code != http.StatusCreated {
			t.Fatalf("seed %s: %d %s", name, rr.Code, rr.Body.String())
		}
		members[name] = &writtenMember{body: writtenMemberBody(name), created: time.Now()}
	}
	return members
}

// memberWriters are clients, each on a replica of its own, that never pause
// between creating a member of one calendar and deleting the one they created
// before it. members holds every member a writer was told it created.
type memberWriters struct {
	t            *testing.T
	mu           sync.Mutex
	members      map[string]*writtenMember
	serverErrors []string
	ops          atomic.Int64
	stop         chan struct{}
	wg           sync.WaitGroup
}

func startMemberWriters(t *testing.T, db *store.Store, user *store.User, root string, writers int) *memberWriters {
	w := &memberWriters{t: t, members: make(map[string]*writtenMember), stop: make(chan struct{})}
	ctx := t.Context()
	for g := 0; g < writers; g++ {
		w.wg.Add(1)
		go func(g int) {
			defer w.wg.Done()
			replica := NewDavServer(Options{Store: db})
			previous := ""
			for k := 0; ; k++ {
				select {
				case <-w.stop:
					return
				default:
				}
				name := fmt.Sprintf("w%d-%d", g, k)
				rr := serveCalendarRequest(ctx, user, replica, http.MethodPut, root+"/"+name+".ics", writtenMemberBody(name))
				acked := time.Now()
				w.mu.Lock()
				w.recordServerError(rr, "PUT "+name)
				if rr.Code == http.StatusCreated {
					w.members[name] = &writtenMember{body: writtenMemberBody(name), created: acked}
				}
				if previous != "" {
					w.members[previous].deleteSent = time.Now()
				}
				w.mu.Unlock()
				if previous != "" {
					rr := serveCalendarRequest(ctx, user, replica, http.MethodDelete, root+"/"+previous+".ics", "")
					acked := time.Now()
					w.mu.Lock()
					w.recordServerError(rr, "DELETE "+previous)
					if rr.Code == http.StatusNoContent {
						w.members[previous].deleted = acked
					}
					w.mu.Unlock()
				}
				previous = ""
				if rr.Code == http.StatusCreated {
					previous = name
				}
				w.ops.Add(1)
			}
		}(g)
	}
	return w
}

func (w *memberWriters) recordServerError(rr *httptest.ResponseRecorder, request string) {
	if rr.Code >= 500 {
		w.serverErrors = append(w.serverErrors, fmt.Sprintf("%s = %d %s", request, rr.Code, rr.Body.String()))
	}
}

// waitForMoreOps waits for the writers to make at least n more operations.
func (w *memberWriters) waitForMoreOps(n int64) {
	w.t.Helper()
	target := w.ops.Load() + n
	deadline := time.Now().Add(30 * time.Second)
	for w.ops.Load() < target {
		if time.Now().After(deadline) {
			w.t.Fatalf("writers made %d operations, want %d", w.ops.Load(), target)
		}
		time.Sleep(time.Millisecond)
	}
}

// finish stops the writers and fails the test for any write the server
// answered with a 5xx.
func (w *memberWriters) finish() {
	w.t.Helper()
	close(w.stop)
	w.wg.Wait()
	for _, failure := range w.serverErrors {
		w.t.Errorf("member write failed on the server: %s", failure)
	}
}

func storedMembersByName(t *testing.T, db *store.Store, calendarID int64) map[string]store.Event {
	t.Helper()
	events, err := db.Events.ListForCalendar(t.Context(), calendarID)
	if err != nil {
		t.Fatal(err)
	}
	byName := make(map[string]store.Event, len(events))
	for _, event := range events {
		if _, duplicate := byName[eventResourceName(event)]; duplicate {
			t.Errorf("calendar %d holds %q twice", calendarID, eventResourceName(event))
		}
		byName[eventResourceName(event)] = event
	}
	return byName
}

func calendarWithSlug(t *testing.T, db *store.Store, userID int64, slug string) *store.Calendar {
	t.Helper()
	calendars, err := db.Calendars.ListByUser(t.Context(), userID)
	if err != nil {
		t.Fatal(err)
	}
	for i := range calendars {
		if calendars[i].Slug != nil && *calendars[i].Slug == slug {
			return &calendars[i]
		}
	}
	return nil
}

const (
	continuousWriters    = 4
	opsAroundTheTransfer = 10
)

// A whole-collection COPY or MOVE holds its collection while it reads and
// writes the members, so writers on other replicas that never pause queue
// behind it rather than leaving it to retry until it gives up. The transferred
// collection holds exactly the members stored at the instant it committed, and
// no write a writer was told succeeded is lost.
func TestPostgresCalendarCollectionTransferUnderContinuousMemberWrites(t *testing.T) {
	for _, method := range []string{"COPY", "MOVE"} {
		t.Run(method, func(t *testing.T) {
			db := newDAVPostgresStore(t)
			ctx := t.Context()
			owner, err := db.Users.UpsertOAuthUser(ctx, "owner", "owner@example.test", "", "")
			if err != nil {
				t.Fatal(err)
			}
			source, err := db.Calendars.Create(ctx, store.Calendar{UserID: owner.ID, Name: "Source"})
			if err != nil {
				t.Fatal(err)
			}
			root := calendarCollectionResourcePath(source.ID)
			seeded := seedWrittenMembers(t, db, owner, root, "seed", 20)
			writers := startMemberWriters(t, db, owner, root, continuousWriters)
			writers.waitForMoreOps(continuousWriters * opsAroundTheTransfer)

			slug := strings.ToLower(method) + "-target"
			req := httptest.NewRequest(method, root+"/", nil)
			req.Header.Set("Destination", "/dav/calendars/"+slug+"/")
			transferStarted := time.Now()
			rr := httptest.NewRecorder()
			NewDavServer(Options{Store: db}).ServeHTTP(rr, req.WithContext(auth.WithUser(ctx, owner)))
			transferEnded := time.Now()
			writers.waitForMoreOps(continuousWriters * opsAroundTheTransfer)
			writers.finish()

			if rr.Code != http.StatusCreated {
				t.Fatalf("%s under continuous member writes = %d, want 201: %s", method, rr.Code, rr.Body.String())
			}
			target := calendarWithSlug(t, db, owner.ID, slug)
			if target == nil {
				t.Fatalf("no calendar at %q after %s", slug, method)
			}
			if remains, err := db.Calendars.GetByID(ctx, source.ID); (err == nil && remains != nil) != (method == "COPY") {
				t.Fatalf("source after %s = %#v, %v", method, remains, err)
			}
			members := writers.members
			for name, member := range seeded {
				members[name] = member
			}

			transferred := storedMembersByName(t, db, target.ID)
			for name, event := range transferred {
				member, known := members[name]
				if !known {
					t.Errorf("%s target holds %q, which no writer was told it created", method, name)
					continue
				}
				if event.RawICAL != member.body {
					t.Errorf("%s target holds %q with a body it was never written with", method, name)
				}
			}
			for name, member := range members {
				_, present := transferred[name]
				storedThroughout := member.created.Before(transferStarted) && (member.deleteSent.IsZero() || member.deleteSent.After(transferEnded))
				goneBefore := !member.deleted.IsZero() && member.deleted.Before(transferStarted)
				if storedThroughout && !present {
					t.Errorf("%s target is missing %q, stored throughout the transfer", method, name)
				}
				if goneBefore && present {
					t.Errorf("%s target holds %q, deleted before the transfer began", method, name)
				}
			}

			final := transferred
			if method == "COPY" {
				final = storedMembersByName(t, db, source.ID)
			}
			for name, member := range members {
				_, present := final[name]
				if member.deleted.IsZero() && !present {
					t.Errorf("write of %q was acknowledged but is lost after %s", name, method)
				}
				if !member.deleted.IsZero() && present {
					t.Errorf("delete of %q was acknowledged but it remains after %s", name, method)
				}
			}
		})
	}
}

// A COPY or MOVE over an existing collection that writers on other replicas
// keep changing holds that collection too, and replaces it as it stands: the
// collection at the destination afterwards holds the source's members and
// nothing a writer put in the one it replaced.
func TestPostgresCalendarCollectionOverwriteUnderContinuousDestinationWrites(t *testing.T) {
	for _, method := range []string{"COPY", "MOVE"} {
		t.Run(method, func(t *testing.T) {
			db := newDAVPostgresStore(t)
			ctx := t.Context()
			owner, err := db.Users.UpsertOAuthUser(ctx, "owner", "owner@example.test", "", "")
			if err != nil {
				t.Fatal(err)
			}
			source, err := db.Calendars.Create(ctx, store.Calendar{UserID: owner.ID, Name: "Source"})
			if err != nil {
				t.Fatal(err)
			}
			slug := strings.ToLower(method) + "-target"
			destination, err := db.Calendars.Create(ctx, store.Calendar{UserID: owner.ID, Name: "Replaced", Slug: &slug})
			if err != nil {
				t.Fatal(err)
			}
			sourceRoot, destinationRoot := calendarCollectionResourcePath(source.ID), calendarCollectionResourcePath(destination.ID)
			seeded := seedWrittenMembers(t, db, owner, sourceRoot, "seed", 20)
			seedWrittenMembers(t, db, owner, destinationRoot, "replaced", 5)
			writers := startMemberWriters(t, db, owner, destinationRoot, continuousWriters)
			writers.waitForMoreOps(continuousWriters * opsAroundTheTransfer)

			req := httptest.NewRequest(method, sourceRoot+"/", nil)
			req.Header.Set("Destination", "/dav/calendars/"+slug+"/")
			req.Header.Set("Overwrite", "T")
			rr := httptest.NewRecorder()
			NewDavServer(Options{Store: db}).ServeHTTP(rr, req.WithContext(auth.WithUser(ctx, owner)))
			writers.waitForMoreOps(continuousWriters * opsAroundTheTransfer)
			writers.finish()

			if rr.Code != http.StatusNoContent {
				t.Fatalf("%s over a collection under continuous member writes = %d, want 204: %s", method, rr.Code, rr.Body.String())
			}
			target := calendarWithSlug(t, db, owner.ID, slug)
			if target == nil || target.ID == destination.ID {
				t.Fatalf("calendar at %q after %s = %#v, want a new one", slug, method, target)
			}
			if replaced, err := db.Calendars.GetByID(ctx, destination.ID); err == nil && replaced != nil {
				t.Fatalf("replaced calendar remains after %s: %#v", method, replaced)
			}
			if remaining := storedMembersByName(t, db, destination.ID); len(remaining) != 0 {
				t.Errorf("%d members of the replaced calendar remain after %s", len(remaining), method)
			}
			transferred := storedMembersByName(t, db, target.ID)
			if len(transferred) != len(seeded) {
				t.Errorf("%s target holds %d members, want the source's %d", method, len(transferred), len(seeded))
			}
			for name, member := range seeded {
				if event, present := transferred[name]; !present || event.RawICAL != member.body {
					t.Errorf("%s target holds %q as %#v, want the source's member", method, name, event)
				}
			}
		})
	}
}

// A collection DELETE holds its collection while it removes it, so writers on
// other replicas that never pause queue behind it rather than leaving it to
// fail. Nothing of the collection survives it: no member a writer was told it
// created, and none of the state kept beside the members.
func TestPostgresCalendarCollectionDeleteUnderContinuousMemberWrites(t *testing.T) {
	var pool *sql.DB
	db := openDAVPostgresStore(t, func(dsn string) (*sql.DB, error) {
		var err error
		pool, err = sql.Open("postgres", dsn)
		return pool, err
	})
	ctx := t.Context()
	owner, err := db.Users.UpsertOAuthUser(ctx, "owner", "owner@example.test", "", "")
	if err != nil {
		t.Fatal(err)
	}
	calendar, err := db.Calendars.Create(ctx, store.Calendar{UserID: owner.ID, Name: "Doomed"})
	if err != nil {
		t.Fatal(err)
	}
	root := calendarCollectionResourcePath(calendar.ID)
	seedWrittenMembers(t, db, owner, root, "seed", 20)
	if err := db.ACLEntries.SetACL(ctx, root+"/seed-0", []store.ACLEntry{{PrincipalHref: acl.PrincipalHref(owner.ID), IsGrant: true, Privilege: "read"}}); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.ExecContext(ctx, `INSERT INTO dav_dead_properties (resource_path, namespace_uri, local_name, inner_xml, created_at, updated_at) VALUES ($1, 'urn:review', 'note', 'x', NOW(), NOW())`, root+"/seed-1"); err != nil {
		t.Fatal(err)
	}
	writers := startMemberWriters(t, db, owner, root, continuousWriters)
	writers.waitForMoreOps(continuousWriters * opsAroundTheTransfer)

	rr := serveCalendarRequest(ctx, owner, NewDavServer(Options{Store: db}), http.MethodDelete, root+"/", "")
	writers.waitForMoreOps(continuousWriters * opsAroundTheTransfer)
	writers.finish()

	if rr.Code != http.StatusNoContent {
		t.Fatalf("DELETE under continuous member writes = %d, want 204: %s", rr.Code, rr.Body.String())
	}
	assertNothingRemainsOfCalendar(t, pool, calendar.ID)
}

// assertNothingRemainsOfCalendar fails the test for any row a removed calendar
// left behind.
func assertNothingRemainsOfCalendar(t *testing.T, pool *sql.DB, calendarID int64) {
	t.Helper()
	root := calendarCollectionResourcePath(calendarID)
	for _, check := range []struct {
		table, query string
		args         []any
	}{
		{"calendars", `SELECT count(*) FROM calendars WHERE id=$1`, []any{calendarID}},
		{"events", `SELECT count(*) FROM events WHERE calendar_id=$1`, []any{calendarID}},
		{"deleted_resources", `SELECT count(*) FROM deleted_resources WHERE resource_type='event' AND collection_id=$1`, []any{calendarID}},
		{"dav_dead_properties", `SELECT count(*) FROM dav_dead_properties WHERE resource_path=$1 OR resource_path LIKE $2`, []any{root, root + "/%"}},
		{"acl_entries", `SELECT count(*) FROM acl_entries WHERE resource_path=$1 OR resource_path LIKE $2`, []any{root, root + "/%"}},
		{"locks", `SELECT count(*) FROM locks WHERE resource_path=$1 OR resource_path LIKE $2`, []any{root, root + "/%"}},
	} {
		var count int
		if err := pool.QueryRowContext(t.Context(), check.query, check.args...).Scan(&count); err != nil {
			t.Fatal(err)
		}
		if count != 0 {
			t.Errorf("%d %s rows remain for removed calendar %d", count, check.table, calendarID)
		}
	}
}

// calendarListChurn adds a member to calendarID right after every listing of
// it, so each preflight a collection transfer makes is stale by one member
// before its transaction begins.
type calendarListChurn struct {
	store.EventRepository
	calendarID int64
	add        func(n int) error
	added      int
}

func (r *calendarListChurn) ListForCalendar(ctx context.Context, calendarID int64) ([]store.Event, error) {
	events, err := r.EventRepository.ListForCalendar(ctx, calendarID)
	if err == nil && calendarID == r.calendarID {
		r.added++
		if err := r.add(r.added); err != nil {
			return nil, err
		}
	}
	return events, err
}

// A guest's COPY of a shared calendar whose members change after every
// preflight transfers the members stored once its transaction holds the
// collection, and decides the guest's read privilege on each member it did
// not preflight from the ACL stored at that member then.
func TestPostgresCalendarCollectionCopyChecksMembersAddedAfterThePreflight(t *testing.T) {
	for _, denied := range []bool{false, true} {
		t.Run(fmt.Sprintf("denied=%t", denied), func(t *testing.T) {
			db := newDAVPostgresStore(t)
			ctx := t.Context()
			owner, err := db.Users.UpsertOAuthUser(ctx, "owner", "owner@example.test", "", "")
			if err != nil {
				t.Fatal(err)
			}
			guest, err := db.Users.UpsertOAuthUser(ctx, "guest", "guest@example.test", "", "")
			if err != nil {
				t.Fatal(err)
			}
			calendar, err := db.Calendars.Create(ctx, store.Calendar{UserID: owner.ID, Name: "Shared"})
			if err != nil {
				t.Fatal(err)
			}
			root := fmt.Sprintf("/dav/calendars/%d", calendar.ID)
			if err := db.ACLEntries.SetACL(ctx, root, []store.ACLEntry{{PrincipalHref: acl.PrincipalHref(guest.ID), IsGrant: true, Privilege: "read"}}); err != nil {
				t.Fatal(err)
			}
			repo := db.Events
			db.Events = &calendarListChurn{EventRepository: repo, calendarID: calendar.ID, add: func(n int) error {
				name := fmt.Sprintf("late-%d", n)
				raw := fmt.Sprintf("BEGIN:VCALENDAR\r\nVERSION:2.0\r\nPRODID:-//Review//EN\r\nBEGIN:VEVENT\r\nUID:%s\r\nDTSTAMP:20260101T000000Z\r\nDTSTART:20260101T100000Z\r\nDTEND:20260101T110000Z\r\nEND:VEVENT\r\nEND:VCALENDAR\r\n", name)
				if _, err := repo.Upsert(ctx, store.Event{CalendarID: calendar.ID, UID: name, ResourceName: name, ETag: name, RawICAL: raw}); err != nil {
					return err
				}
				if !denied {
					return nil
				}
				return db.ACLEntries.SetACL(ctx, root+"/"+name, []store.ACLEntry{{PrincipalHref: acl.PrincipalHref(guest.ID), IsGrant: false, Privilege: "read"}})
			}}
			h := NewDavServer(Options{Store: db})

			req := httptest.NewRequest("COPY", root+"/", nil)
			req.Header.Set("Destination", "/dav/calendars/guest-copy/")
			req.Header.Set("Depth", "infinity")
			rr := httptest.NewRecorder()
			h.ServeHTTP(rr, req.WithContext(auth.WithUser(ctx, guest)))
			copies, err := db.Calendars.ListByUser(ctx, guest.ID)
			if err != nil {
				t.Fatal(err)
			}
			if denied {
				if rr.Code != http.StatusMultiStatus || !strings.Contains(rr.Body.String(), root+"/late-1.ics") {
					t.Fatalf("COPY of a member the guest may not read = %d, want 207 naming it: %s", rr.Code, rr.Body.String())
				}
				if len(copies) != 0 {
					t.Fatalf("guest holds %d calendars copied with a member it may not read", len(copies))
				}
				return
			}
			if rr.Code != http.StatusCreated {
				t.Fatalf("COPY of a calendar changing after each preflight = %d, want 201: %s", rr.Code, rr.Body.String())
			}
			if len(copies) != 1 {
				t.Fatalf("guest holds %d calendars, want the copy", len(copies))
			}
			copied, err := repo.ListForCalendar(ctx, copies[0].ID)
			if err != nil {
				t.Fatal(err)
			}
			if len(copied) != 1 || copied[0].ResourceName != "late-1" || copied[0].ETag == "late-1" {
				t.Fatalf("copy holds %+v, want late-1 under an ETag of its own", copied)
			}
		})
	}
}

// A whole-collection COPY or MOVE takes the collection's own properties from
// the row its transaction locks, which a PROPPATCH holding the collection
// waits on, so a PROPPATCH that lands after the preflight is carried to the
// destination rather than reverted.
func TestPostgresCalendarCollectionTransferKeepsAPropertyPatchAfterThePreflight(t *testing.T) {
	for _, method := range []string{"COPY", "MOVE"} {
		t.Run(method, func(t *testing.T) {
			db := newDAVPostgresStore(t)
			ctx := t.Context()
			owner, err := db.Users.UpsertOAuthUser(ctx, "owner", "owner@example.test", "", "")
			if err != nil {
				t.Fatal(err)
			}
			source, err := db.Calendars.Create(ctx, store.Calendar{UserID: owner.ID, Name: "Before"})
			if err != nil {
				t.Fatal(err)
			}
			root := calendarCollectionResourcePath(source.ID)
			seedWrittenMembers(t, db, owner, root, "seed", 3)
			description, timezone, color := "Patched description", "Europe/Berlin", "#123456"
			patched := store.CalendarProperties{Name: "Patched", Description: &description, Timezone: &timezone, Color: &color}
			db.Events = &calendarListMutation{EventRepository: db.Events, calendarID: source.ID, mutate: func() error {
				return db.PatchCalendarProperties(ctx, source.ID, patched, root, nil, nil, nil)
			}}

			slug := strings.ToLower(method) + "-target"
			req := httptest.NewRequest(method, root+"/", nil)
			req.Header.Set("Destination", "/dav/calendars/"+slug+"/")
			rr := httptest.NewRecorder()
			NewDavServer(Options{Store: db}).ServeHTTP(rr, req.WithContext(auth.WithUser(ctx, owner)))
			if rr.Code != http.StatusCreated {
				t.Fatalf("%s = %d, want 201: %s", method, rr.Code, rr.Body.String())
			}
			target := calendarWithSlug(t, db, owner.ID, slug)
			if target == nil {
				t.Fatalf("no calendar at %q after %s", slug, method)
			}
			if target.Name != patched.Name || target.Description == nil || *target.Description != description ||
				target.Timezone == nil || *target.Timezone != timezone || target.Color == nil || *target.Color != color {
				t.Errorf("%s target properties = %q %v %v %v, want the patched ones", method, target.Name, target.Description, target.Timezone, target.Color)
			}
		})
	}
}
