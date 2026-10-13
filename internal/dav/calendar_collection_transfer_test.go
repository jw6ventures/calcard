package dav

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/jw6ventures/calcard/internal/auth"
	"github.com/jw6ventures/calcard/internal/store"
)

// The preflight decides each member's read from the ACL stored for the member
// by name, so a member whose href has to escape its name is refused as GET
// refuses it.
func TestCalendarCollectionCopyRefusesAnUnreadableMemberWhateverItsName(t *testing.T) {
	owner := store.User{ID: 1}
	delegate := store.User{ID: 2}
	source := store.Calendar{ID: 1, UserID: owner.ID, Name: "Shared", CTag: 7}
	events := &fakeEventRepo{events: map[string]*store.Event{
		"1:visible":     {ID: 11, CalendarID: 1, UID: "visible", ResourceName: "visible", RawICAL: buildCalendarObject(buildVEvent("visible")), ETag: "visible-etag"},
		"1:secret note": {ID: 12, CalendarID: 1, UID: "secret", ResourceName: "secret note", RawICAL: buildCalendarObject(buildVEvent("secret")), ETag: "secret-etag"},
	}}
	backend := &recordingCalendarCollectionBackend{}
	h := NewDavServer(Options{Store: &store.Store{
		Calendars: &fakeCalendarRepo{calendars: map[int64]*store.Calendar{source.ID: &source}},
		Events:    events,
		ACLEntries: &fakeACLRepo{entries: []store.ACLEntry{
			{ResourcePath: "/dav/calendars/1", PrincipalHref: "/dav/principals/2/", IsGrant: true, Privilege: "read"},
			{ResourcePath: "/dav/calendars/1/secret note", PrincipalHref: "/dav/principals/2/", IsGrant: false, Privilege: "read"},
		}},
		CalendarCollections: backend,
	}})
	get := httptest.NewRequest(http.MethodGet, "/dav/calendars/1/secret%20note.ics", nil)
	get = get.WithContext(auth.WithUser(get.Context(), &delegate))
	getRR := httptest.NewRecorder()
	h.ServeHTTP(getRR, get)
	if getRR.Code == http.StatusOK {
		t.Fatalf("GET of the denied member = 200, want it refused")
	}

	req := httptest.NewRequest("COPY", "/dav/calendars/1/", nil)
	req.Header.Set("Destination", "/dav/calendars/copied")
	req = req.WithContext(auth.WithUser(req.Context(), &delegate))
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, req)

	if rr.Code != http.StatusMultiStatus {
		t.Fatalf("COPY status = %d, want 207: %s", rr.Code, rr.Body.String())
	}
	if len(backend.transfers) != 0 {
		t.Fatalf("COPY reached backend despite an unreadable source member: %#v", backend.transfers)
	}
	decodeMultistatus(t, rr).assertHrefs(t, "/dav/calendars/1/secret%20note.ics")
}
