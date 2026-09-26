package dav

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/jw6ventures/calcard/internal/acl"
	"github.com/jw6ventures/calcard/internal/auth"
	"github.com/jw6ventures/calcard/internal/store"
)

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
