package dav

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/jw6ventures/calcard/internal/acl"
	"github.com/jw6ventures/calcard/internal/auth"
	"github.com/jw6ventures/calcard/internal/store"
)

// eventCreatedAfterRead is an event repository whose first read of a member
// is followed at once by create, as a PUT committing just after that read
// would be.
type eventCreatedAfterRead struct {
	store.EventRepository
	create  func() error
	created bool
}

func (r *eventCreatedAfterRead) GetByResourceName(ctx context.Context, calendarID int64, name string) (*store.Event, error) {
	event, err := r.EventRepository.GetByResourceName(ctx, calendarID, name)
	if err == nil && !r.created {
		r.created = true
		if err := r.create(); err != nil {
			return nil, err
		}
	}
	return event, err
}

// A LOCK is authorized and stored against one read of its target. A member
// created after that read leaves DAV:bind, which authorized locking an absent
// member, short of what locking the existing one needs, so the lock must not
// be taken.
func TestPostgresLockIsTakenOnlyOnTheStateItWasAuthorizedAgainst(t *testing.T) {
	const lockBody = `<?xml version="1.0" encoding="utf-8"?><D:lockinfo xmlns:D="DAV:"><D:lockscope><D:exclusive/></D:lockscope><D:locktype><D:write/></D:locktype></D:lockinfo>`
	db := newDAVPostgresStore(t)
	ctx := t.Context()
	owner, err := db.Users.UpsertOAuthUser(ctx, "owner", "owner@example.test", "", "")
	if err != nil {
		t.Fatal(err)
	}
	binder, err := db.Users.UpsertOAuthUser(ctx, "binder", "binder@example.test", "", "")
	if err != nil {
		t.Fatal(err)
	}
	calendar, err := db.Calendars.Create(ctx, store.Calendar{UserID: owner.ID, Name: "Calendar"})
	if err != nil {
		t.Fatal(err)
	}
	collectionPath := fmt.Sprintf("/dav/calendars/%d", calendar.ID)
	if err := db.ACLEntries.SetACL(ctx, collectionPath, []store.ACLEntry{
		{PrincipalHref: acl.PrincipalHref(binder.ID), IsGrant: true, Privilege: "bind"},
	}); err != nil {
		t.Fatal(err)
	}
	repo := db.Events
	db.Events = &eventCreatedAfterRead{EventRepository: repo, create: func() error {
		_, err := repo.Upsert(ctx, store.Event{CalendarID: calendar.ID, UID: "target", ResourceName: "target", ETag: "target",
			RawICAL: "BEGIN:VCALENDAR\r\nVERSION:2.0\r\nBEGIN:VEVENT\r\nUID:target\r\nDTSTART:20260101T090000Z\r\nEND:VEVENT\r\nEND:VCALENDAR\r\n"})
		return err
	}}
	h := NewDavServer(Options{Store: db})

	req := httptest.NewRequest("LOCK", collectionPath+"/target.ics", strings.NewReader(lockBody))
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, req.WithContext(auth.WithUser(ctx, binder)))

	if rr.Code != http.StatusPreconditionFailed {
		t.Fatalf("LOCK = %d, want 412: %s", rr.Code, rr.Body.String())
	}
	locks, err := db.Locks.ListByResources(ctx, []string{collectionPath + "/target"})
	if err != nil {
		t.Fatal(err)
	}
	if len(locks) != 0 {
		t.Fatalf("LOCK stored %d locks on a member created after its authorization", len(locks))
	}
}

// RFC 4918 §7.5 has a depth 0 lock on a collection cover its membership: a
// PUT or LOCK creating a member needs its token, held by the lock's creator
// (§6.4), while rewriting or locking a member that exists does not.
func TestPostgresDepthZeroCollectionLockCoversMembershipOnly(t *testing.T) {
	const lockBody = `<?xml version="1.0" encoding="utf-8"?><D:lockinfo xmlns:D="DAV:"><D:lockscope><D:exclusive/></D:lockscope><D:locktype><D:write/></D:locktype></D:lockinfo>`
	db := newDAVPostgresStore(t)
	ctx := t.Context()
	owner, err := db.Users.UpsertOAuthUser(ctx, "owner", "owner@example.test", "", "")
	if err != nil {
		t.Fatal(err)
	}
	editor, err := db.Users.UpsertOAuthUser(ctx, "editor", "editor@example.test", "", "")
	if err != nil {
		t.Fatal(err)
	}
	calendar, err := db.Calendars.Create(ctx, store.Calendar{UserID: owner.ID, Name: "Calendar"})
	if err != nil {
		t.Fatal(err)
	}
	collectionPath := fmt.Sprintf("/dav/calendars/%d", calendar.ID)
	h := NewDavServer(Options{Store: db})
	do := func(method, uri, body string, user *store.User, headers ...string) *httptest.ResponseRecorder {
		t.Helper()
		req := httptest.NewRequest(method, uri, strings.NewReader(body))
		if method == http.MethodPut {
			req.Header.Set("Content-Type", "text/calendar")
		}
		for i := 0; i+1 < len(headers); i += 2 {
			req.Header.Set(headers[i], headers[i+1])
		}
		rr := httptest.NewRecorder()
		h.ServeHTTP(rr, req.WithContext(auth.WithUser(ctx, user)))
		return rr
	}
	if rr := do(http.MethodPut, collectionPath+"/event.ics", buildCalendarObject(buildVEvent("event")), owner); rr.Code != http.StatusCreated {
		t.Fatalf("seed PUT = %d: %s", rr.Code, rr.Body.String())
	}
	if err := db.ACLEntries.SetACL(ctx, collectionPath, []store.ACLEntry{
		{PrincipalHref: acl.PrincipalHref(editor.ID), IsGrant: true, Privilege: "read"},
		{PrincipalHref: acl.PrincipalHref(editor.ID), IsGrant: true, Privilege: "write-content"},
		{PrincipalHref: acl.PrincipalHref(editor.ID), IsGrant: true, Privilege: "bind"},
	}); err != nil {
		t.Fatal(err)
	}

	collectionLock := do("LOCK", collectionPath, lockBody, owner, "Depth", "0")
	if collectionLock.Code != http.StatusOK {
		t.Fatalf("owner LOCK of the collection = %d: %s", collectionLock.Code, collectionLock.Body.String())
	}
	token := strings.Trim(collectionLock.Header().Get("Lock-Token"), "<>")
	ownersToken := []string{"If", "<" + collectionPath + "> (<" + token + ">)"}

	for _, step := range []struct {
		name, method, uri, body string
		user                    *store.User
		headers                 []string
		want                    int
	}{
		{name: "editor rewrites a member", method: http.MethodPut, uri: collectionPath + "/event.ics", body: buildCalendarObject(buildVEvent("event", "SUMMARY:edited")), user: editor, want: http.StatusNoContent},
		{name: "editor locks a member", method: "LOCK", uri: collectionPath + "/event.ics", body: lockBody, user: editor, want: http.StatusOK},
		{name: "editor creates a member", method: http.MethodPut, uri: collectionPath + "/new.ics", body: buildCalendarObject(buildVEvent("new")), user: editor, want: http.StatusLocked},
		{name: "editor locks an unmapped member", method: "LOCK", uri: collectionPath + "/unmapped.ics", body: lockBody, user: editor, want: http.StatusLocked},
		{name: "editor creates a member with the owner's token", method: http.MethodPut, uri: collectionPath + "/new.ics", body: buildCalendarObject(buildVEvent("new")), user: editor, headers: ownersToken, want: http.StatusLocked},
		{name: "editor locks an unmapped member with the owner's token", method: "LOCK", uri: collectionPath + "/unmapped.ics", body: lockBody, user: editor, headers: ownersToken, want: http.StatusLocked},
		{name: "owner locks an unmapped member with the token", method: "LOCK", uri: collectionPath + "/unmapped.ics", body: lockBody, user: owner, headers: ownersToken, want: http.StatusCreated},
	} {
		if rr := do(step.method, step.uri, step.body, step.user, step.headers...); rr.Code != step.want {
			t.Fatalf("%s = %d, want %d: %s", step.name, rr.Code, step.want, rr.Body.String())
		}
	}
}

// LOCK and ACL on a member are authorized against that member's state, not the
// whole collection's. A write to a sibling member that commits between the
// handler's read of the target and its own write changes the collection CTag
// and nothing else the request depends on, so it must not fail the request.
func TestPostgresMemberLockAndACLIgnoreSiblingWrites(t *testing.T) {
	const lockBody = `<?xml version="1.0" encoding="utf-8"?><D:lockinfo xmlns:D="DAV:"><D:lockscope><D:exclusive/></D:lockscope><D:locktype><D:write/></D:locktype></D:lockinfo>`

	for _, method := range []string{"LOCK", "ACL"} {
		t.Run(method, func(t *testing.T) {
			db := newDAVPostgresStore(t)
			ctx := t.Context()
			owner, err := db.Users.UpsertOAuthUser(ctx, "owner", "owner@example.test", "", "")
			if err != nil {
				t.Fatal(err)
			}
			reader, err := db.Users.UpsertOAuthUser(ctx, "reader", "reader@example.test", "", "")
			if err != nil {
				t.Fatal(err)
			}
			calendar, err := db.Calendars.Create(ctx, store.Calendar{UserID: owner.ID, Name: "Calendar"})
			if err != nil {
				t.Fatal(err)
			}
			seed := func(uid string) store.Event {
				event := store.Event{CalendarID: calendar.ID, UID: uid, ResourceName: uid, ETag: uid,
					RawICAL: "BEGIN:VCALENDAR\r\nVERSION:2.0\r\nBEGIN:VEVENT\r\nUID:" + uid + "\r\nDTSTART:20260101T090000Z\r\nEND:VEVENT\r\nEND:VCALENDAR\r\n"}
				if _, err := db.Events.Upsert(ctx, event); err != nil {
					t.Fatal(err)
				}
				return event
			}
			seed("target")
			sibling := seed("sibling")

			repo := db.Events
			wrapper := &eventReadMutation{EventRepository: repo}
			// Every read of the target is followed by a sibling write, so one
			// lands after whichever read the handler bases its expectation on.
			wrapper.mutate = func(current *store.Event) error {
				if current.UID != "target" {
					return nil
				}
				sibling.ETag = fmt.Sprintf("sibling-%d", wrapper.calls)
				_, err := repo.Upsert(ctx, sibling)
				return err
			}
			db.Events = wrapper
			h := NewDavServer(Options{Store: db})

			body := lockBody
			if method == "ACL" {
				body = `<?xml version="1.0" encoding="utf-8"?><D:acl xmlns:D="DAV:"><D:ace><D:principal><D:href>` +
					acl.PrincipalHref(reader.ID) + `</D:href></D:principal><D:grant><D:privilege><D:read/></D:privilege></D:grant></D:ace></D:acl>`
			}
			req := httptest.NewRequest(method, fmt.Sprintf("/dav/calendars/%d/target.ics", calendar.ID), strings.NewReader(body))
			rr := httptest.NewRecorder()
			h.ServeHTTP(rr, req.WithContext(auth.WithUser(ctx, owner)))
			if rr.Code != http.StatusOK {
				t.Fatalf("%s on a member after a sibling write = %d, want 200: %s", method, rr.Code, rr.Body.String())
			}
			if wrapper.calls == 0 {
				t.Fatal("the sibling write never ran between the handler's read and its write")
			}
		})
	}
}
