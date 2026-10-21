package dav

import (
	"net/http"
	"net/http/httptest"
	"net/url"
	"reflect"
	"strings"
	"testing"

	"github.com/jw6ventures/calcard/internal/auth"
	"github.com/jw6ventures/calcard/internal/store"
)

// DAV:lockdiscovery of a member reports the lock taken on the member and the
// depth-infinity lock taken on its collection, whatever the member is named,
// and a Depth: 1 PROPFIND reports them as a Depth: 0 PROPFIND of the member
// does.
func TestPostgres_LockDiscoveryReportsEachMembersLocks(t *testing.T) {
	database := newDAVPostgresStore(t)
	ctx := t.Context()
	owner, err := database.Users.UpsertOAuthUser(ctx, "owner", "owner@example.test", "Owner", "Owner")
	if err != nil {
		t.Fatal(err)
	}
	h := NewDavServer(Options{Store: database})
	do := func(method, target, depth, body string) *httptest.ResponseRecorder {
		req := httptest.NewRequest(method, target, strings.NewReader(body))
		if depth != "" {
			req.Header.Set("Depth", depth)
		}
		req = req.WithContext(auth.WithUser(req.Context(), owner))
		rr := httptest.NewRecorder()
		h.ServeHTTP(rr, req)
		return rr
	}
	const propfind = `<?xml version="1.0" encoding="utf-8"?><d:propfind xmlns:d="DAV:"><d:prop><d:lockdiscovery/></d:prop></d:propfind>`
	names := []string{"plain", "dav.ics", "Shout.ICS", "uid@example.com"}
	for _, collectionLocked := range []bool{false, true} {
		cal, err := database.Calendars.Create(ctx, store.Calendar{UserID: owner.ID, Name: "Locks"})
		if err != nil {
			t.Fatal(err)
		}
		collectionHref := calendarCollectionResourcePath(cal.ID) + "/"
		memberHref := func(name string) string { return collectionHref + url.PathEscape(name) + ".ics" }
		for _, name := range names {
			if _, err := database.Events.Upsert(ctx, store.Event{CalendarID: cal.ID, UID: "uid-" + name, ResourceName: name, RawICAL: buildCalendarObject(buildVEvent("uid-"+name, "SUMMARY:x")), ETag: "e-" + name}); err != nil {
				t.Fatal(err)
			}
		}
		if collectionLocked {
			if lock := do("LOCK", collectionHref, "infinity", memberDecisionSharedLockBody); lock.Code != http.StatusOK {
				t.Fatalf("LOCK %s = %d: %s", collectionHref, lock.Code, lock.Body.String())
			}
		}
		for _, name := range names {
			if lock := do("LOCK", memberHref(name), "0", memberDecisionSharedLockBody); lock.Code != http.StatusOK {
				t.Fatalf("LOCK %s = %d: %s", memberHref(name), lock.Code, lock.Body.String())
			}
		}
		wantLocks := 1
		if collectionLocked {
			wantLocks = 2
		}

		listing := decodeMultistatus(t, do("PROPFIND", collectionHref, "1", propfind))
		for _, name := range names {
			single := decodeMultistatus(t, do("PROPFIND", memberHref(name), "0", propfind))
			if len(single.Responses) != 1 {
				t.Fatalf("Depth 0 PROPFIND of %q gave %d responses", name, len(single.Responses))
			}
			locks, _ := grantedPropertyOf(single.Responses[0], "lockdiscovery")
			if len(locks.Children) != wantLocks {
				t.Errorf("collection locked %v: Depth 0 reports %d locks on %q, want %d: %s", collectionLocked, len(locks.Children), name, wantLocks, describeElement(locks))
			}
			var listed *davResponse
			for i := range listing.Responses {
				if canonicalHref(listing.Responses[i].Hrefs[0]) == canonicalHref(memberHref(name)) {
					listed = &listing.Responses[i]
				}
			}
			if listed == nil {
				t.Errorf("Depth 1 does not list %q", name)
				continue
			}
			if listedLocks, _ := grantedPropertyOf(*listed, "lockdiscovery"); !reflect.DeepEqual(listedLocks, locks) {
				t.Errorf("collection locked %v: %q's locks differ\nDepth 1: %s\nDepth 0: %s", collectionLocked, name, describeElement(listedLocks), describeElement(locks))
			}
		}
	}
}
