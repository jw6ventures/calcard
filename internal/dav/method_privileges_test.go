package dav

import (
	"context"
	"encoding/xml"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/jw6ventures/calcard/internal/auth"
	"github.com/jw6ventures/calcard/internal/store"
)

// RFC 3744 Appendix B fixes which privilege each WebDAV and CalDAV method
// requires, and §7.1.1 requires the refusal to name the resource and privilege
// rather than answering a bare 403.

// assertNeedPrivileges asserts the RFC 3744 §7.1.1 refusal: a 403 whose body is
// a DAV:error carrying DAV:need-privileges, and inside it one DAV:resource
// naming the resource and the privilege that was missing.
func assertNeedPrivileges(t *testing.T, rr *httptest.ResponseRecorder, href string, privilege xml.Name) {
	t.Helper()
	if rr.Code != http.StatusForbidden {
		t.Fatalf("status = %d, want 403: %s", rr.Code, rr.Body.String())
	}
	root, err := parseRootElement(rr.Body.Bytes(), davQN("error"))
	if err != nil {
		t.Fatalf("decode DAV:error: %v; body: %s", err, rr.Body.String())
	}
	needPrivileges := assertSoleChild(t, root, davQN("need-privileges"))
	resource := assertSoleChild(t, needPrivileges, davQN("resource"))
	if got := qnList(resource.childNames()); got != qnList([]xml.Name{davQN("href"), davQN("privilege")}) {
		t.Fatalf("DAV:resource children = %s, want DAV:href then DAV:privilege", got)
	}
	gotHref, err := hrefText(resource.child(t, davQN("href")))
	if err != nil {
		t.Fatalf("DAV:resource href: %v", err)
	}
	if canonicalHref(gotHref) != canonicalHref(href) {
		t.Errorf("need-privileges href = %q, want %q", gotHref, href)
	}
	if got := assertSoleChild(t, resource.child(t, davQN("privilege")), privilege); got.Name != privilege {
		t.Errorf("need-privileges privilege = %s, want %s", qnString(got.Name), qnString(privilege))
	}
}

func TestRFC3744AppendixBMethodPrivilegeFailures(t *testing.T) {
	user := &store.User{ID: 2, PrimaryEmail: "delegate@example.com"}
	principal := "/dav/principals/2/"
	bookPath := "/dav/addressbooks/5"
	objectPath := bookPath + "/alice"
	lockBody := `<?xml version="1.0" encoding="utf-8"?><D:lockinfo xmlns:D="DAV:"><D:lockscope><D:exclusive/></D:lockscope><D:locktype><D:write/></D:locktype></D:lockinfo>`
	propertyUpdate := `<?xml version="1.0" encoding="utf-8"?><D:propertyupdate xmlns:D="DAV:"><D:set><D:prop><D:displayname>Renamed</D:displayname></D:prop></D:set></D:propertyupdate>`
	addressBookQuery := `<?xml version="1.0" encoding="utf-8"?><C:addressbook-query xmlns:C="urn:ietf:params:xml:ns:carddav" xmlns:D="DAV:"><D:prop><D:getetag/></D:prop><C:filter/></C:addressbook-query>`
	vcard := buildVCard("3.0", "UID:alice", "FN:Alice")

	tests := []struct {
		name       string
		method     string
		requestURI string
		body       string
		href       string
		privilege  xml.Name
		configure  func(*http.Request)
	}{
		{name: "PROPFIND requires read", method: "PROPFIND", requestURI: bookPath + "/", href: bookPath, privilege: davQN("read")},
		{name: "REPORT requires read", method: "REPORT", requestURI: bookPath + "/", body: addressBookQuery, href: bookPath, privilege: davQN("read"), configure: func(r *http.Request) { r.Header.Set("Depth", "0") }},
		{name: "new PUT requires bind on parent", method: http.MethodPut, requestURI: bookPath + "/new.vcf", body: buildVCard("3.0", "UID:new", "FN:New"), href: bookPath, privilege: davQN("bind"), configure: func(r *http.Request) { r.Header.Set("Content-Type", "text/vcard") }},
		{name: "existing PUT requires write-content on target", method: http.MethodPut, requestURI: bookPath + "/alice.vcf", body: vcard, href: bookPath + "/alice.vcf", privilege: davQN("write-content"), configure: func(r *http.Request) { r.Header.Set("Content-Type", "text/vcard") }},
		{name: "DELETE requires unbind on parent", method: http.MethodDelete, requestURI: bookPath + "/alice.vcf", href: bookPath, privilege: davQN("unbind")},
		{name: "PROPPATCH requires write-properties", method: "PROPPATCH", requestURI: bookPath + "/", body: propertyUpdate, href: bookPath, privilege: davQN("write-properties")},
		{name: "ACL requires write-acl", method: "ACL", requestURI: bookPath + "/", href: bookPath, privilege: davQN("write-acl")},
		{name: "existing LOCK requires write-content", method: "LOCK", requestURI: bookPath + "/alice.vcf", body: lockBody, href: objectPath, privilege: davQN("write-content")},
		{name: "new LOCK requires bind on parent", method: "LOCK", requestURI: bookPath + "/new.vcf", body: lockBody, href: bookPath, privilege: davQN("bind")},
		{name: "UNLOCK requires unlock on target", method: "UNLOCK", requestURI: bookPath + "/alice.vcf", href: objectPath, privilege: davQN("unlock"), configure: func(r *http.Request) { r.Header.Set("Lock-Token", "<opaquelocktoken:test>") }},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			deniedPath := tt.href
			if strings.HasSuffix(deniedPath, ".vcf") {
				deniedPath = strings.TrimSuffix(deniedPath, ".vcf")
			}
			aclRepo := &fakeACLRepo{entries: []store.ACLEntry{{
				ResourcePath: deniedPath, PrincipalHref: principal, IsGrant: false, Privilege: tt.privilege.Local,
			}}}
			locks := map[string]*store.Lock{}
			if tt.method == "UNLOCK" {
				locks["opaquelocktoken:test"] = &store.Lock{Token: "opaquelocktoken:test", ResourcePath: objectPath, UserID: 1, ExpiresAt: time.Now().Add(time.Hour)}
			}
			h := NewDavServer(Options{Store: &store.Store{
				AddressBooks: &fakeAddressBookRepo{books: map[int64]*store.AddressBook{
					5: {ID: 5, UserID: 1, Name: "Contacts"},
				}},
				Contacts: &fakeContactRepo{contacts: map[string]*store.Contact{
					"5:alice": {AddressBookID: 5, UID: "alice", ResourceName: "alice", RawVCard: vcard, ETag: "alice-etag"},
				}},
				Locks:      &fakeLockRepo{locks: locks},
				ACLEntries: aclRepo,
			}})
			var body io.Reader
			if tt.body != "" {
				body = strings.NewReader(tt.body)
			}
			req := httptest.NewRequest(tt.method, tt.requestURI, body)
			req = req.WithContext(auth.WithUser(req.Context(), user))
			if tt.configure != nil {
				tt.configure(req)
			}
			rr := httptest.NewRecorder()

			h.ServeHTTP(rr, req)

			assertNeedPrivileges(t, rr, tt.href, tt.privilege)
		})
	}
}

func TestUnlockAllowsLockOwnerOrPrincipalWithUnlockPrivilege(t *testing.T) {
	book := store.AddressBook{ID: 5, UserID: 1, Name: "Shared"}
	resourcePath := "/dav/addressbooks/5/alice"

	for _, tc := range []struct {
		name       string
		user       store.User
		lockUserID int64
		entries    []store.ACLEntry
	}{
		{name: "lock owner without unlock ACE", user: store.User{ID: 2}, lockUserID: 2},
		{
			name: "nonowner with unlock ACE", user: store.User{ID: 3}, lockUserID: 2,
			entries: []store.ACLEntry{{
				ResourcePath: resourcePath, PrincipalHref: "/dav/principals/3/", IsGrant: true, Privilege: "unlock",
			}},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			token := "opaquelocktoken:" + strings.ReplaceAll(tc.name, " ", "-")
			locks := &fakeLockRepo{locks: map[string]*store.Lock{token: {
				Token: token, ResourcePath: resourcePath, UserID: tc.lockUserID, LockScope: "exclusive", LockType: "write",
				Depth: "0", ExpiresAt: time.Now().Add(time.Hour),
			}}}
			h := NewDavServer(Options{Store: &store.Store{
				AddressBooks: &fakeAddressBookRepo{books: map[int64]*store.AddressBook{book.ID: &book}},
				Locks:        locks,
				ACLEntries:   &fakeACLRepo{entries: tc.entries},
			}})
			req := httptest.NewRequest("UNLOCK", resourcePath+".vcf", nil)
			req.Header.Set("Lock-Token", "<"+token+">")
			req = req.WithContext(auth.WithUser(req.Context(), &tc.user))
			rr := httptest.NewRecorder()

			h.ServeHTTP(rr, req)

			if rr.Code != http.StatusNoContent {
				t.Fatalf("UNLOCK status = %d, want 204: %s", rr.Code, rr.Body.String())
			}
			if lock, _ := locks.GetByToken(context.Background(), token); lock != nil {
				t.Fatalf("UNLOCK retained lock: %#v", lock)
			}
		})
	}
}

func TestOptionsDoesNotRequireDAVRead(t *testing.T) {
	calendar := store.Calendar{ID: 5, UserID: 1, Name: "Private"}
	delegate := &store.User{ID: 2}
	h := NewDavServer(Options{Store: &store.Store{
		Calendars: &fakeCalendarRepo{calendars: map[int64]*store.Calendar{5: &calendar}},
		ACLEntries: &fakeACLRepo{entries: []store.ACLEntry{{
			ResourcePath: "/dav/calendars/5", PrincipalHref: "/dav/principals/2/", IsGrant: false, Privilege: "read",
		}}},
	}})
	req := httptest.NewRequest(http.MethodOptions, "/dav/calendars/5/", nil)
	req = req.WithContext(auth.WithUser(req.Context(), delegate))
	rr := httptest.NewRecorder()

	h.ServeHTTP(rr, req)

	if rr.Code != http.StatusNoContent {
		t.Fatalf("OPTIONS status = %d, want 204: %s", rr.Code, rr.Body.String())
	}
	if rr.Header().Get("DAV") == "" || rr.Header().Get("Allow") == "" {
		t.Fatalf("OPTIONS omitted discovery headers: DAV=%q Allow=%q", rr.Header().Get("DAV"), rr.Header().Get("Allow"))
	}
}

func TestCalendarMultiGetRequiresReadOnEveryReferencedObject(t *testing.T) {
	owner := &store.User{ID: 1}
	delegate := &store.User{ID: 2}
	calendars := &fakeCalendarRepo{calendars: map[int64]*store.Calendar{
		1: {ID: 1, UserID: owner.ID, Name: "Shared"},
	}}
	events := &fakeEventRepo{events: map[string]*store.Event{
		"1:visible": {CalendarID: 1, UID: "visible", ResourceName: "visible", RawICAL: buildCalendarObject(buildVEvent("visible")), ETag: "visible-etag"},
		"1:hidden":  {CalendarID: 1, UID: "hidden", ResourceName: "hidden", RawICAL: buildCalendarObject(buildVEvent("hidden")), ETag: "hidden-etag"},
	}}
	h := NewDavServer(Options{Store: &store.Store{
		Calendars: calendars,
		Events:    events,
		ACLEntries: &fakeACLRepo{entries: []store.ACLEntry{
			{ResourcePath: "/dav/calendars/1", PrincipalHref: "/dav/principals/2/", IsGrant: true, Privilege: "read"},
			{ResourcePath: "/dav/calendars/1/hidden", PrincipalHref: "/dav/principals/2/", IsGrant: false, Privilege: "read"},
		}},
	}})
	body := `<C:calendar-multiget xmlns:C="urn:ietf:params:xml:ns:caldav" xmlns:D="DAV:"><D:prop><D:getetag/><C:calendar-data/></D:prop><D:href>/dav/calendars/1/visible.ics</D:href><D:href>/dav/calendars/1/hidden.ics</D:href></C:calendar-multiget>`
	req := httptest.NewRequest("REPORT", "/dav/calendars/1/", strings.NewReader(body))
	req = req.WithContext(auth.WithUser(req.Context(), delegate))
	rr := httptest.NewRecorder()

	h.ServeHTTP(rr, req)

	ms := decodeMultistatus(t, rr)
	ms.responseForHref(t, "/dav/calendars/1/visible.ics").assertPropValue(t, davQN("getetag"), http.StatusOK, `"visible-etag"`)
	hidden := ms.responseForHref(t, "/dav/calendars/1/hidden.ics")
	if code := statusCodeFromLine(t, hidden.Status); code != http.StatusNotFound {
		t.Fatalf("denied referenced object status = %d, want 404", code)
	}
	hidden.assertPropAbsent(t, davQN("getetag"))
	hidden.assertPropAbsent(t, calQN("calendar-data"))
}
