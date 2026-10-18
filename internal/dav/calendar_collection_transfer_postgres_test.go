package dav

import (
	"database/sql"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"

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
