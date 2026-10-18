package dav

import (
	"context"
	"encoding/xml"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"path"
	"slices"
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
		// readsCollection grants DAV:read on the address book: a write refused on
		// an existing object names the precise privilege only to a principal
		// that may already see the object exists.
		readsCollection bool
	}{
		{name: "PROPFIND requires read", method: "PROPFIND", requestURI: bookPath + "/", href: bookPath, privilege: davQN("read")},
		{name: "REPORT requires read", method: "REPORT", requestURI: bookPath + "/", body: addressBookQuery, href: bookPath, privilege: davQN("read"), configure: func(r *http.Request) { r.Header.Set("Depth", "0") }},
		{name: "new PUT requires bind on parent", method: http.MethodPut, requestURI: bookPath + "/new.vcf", body: buildVCard("3.0", "UID:new", "FN:New"), href: bookPath, privilege: davQN("bind"), configure: func(r *http.Request) { r.Header.Set("Content-Type", "text/vcard") }},
		{name: "existing PUT requires write-content on target", method: http.MethodPut, requestURI: bookPath + "/alice.vcf", body: vcard, href: bookPath + "/alice.vcf", privilege: davQN("write-content"), configure: func(r *http.Request) { r.Header.Set("Content-Type", "text/vcard") }, readsCollection: true},
		{name: "DELETE requires unbind on parent", method: http.MethodDelete, requestURI: bookPath + "/alice.vcf", href: bookPath, privilege: davQN("unbind")},
		{name: "PROPPATCH requires write-properties", method: "PROPPATCH", requestURI: bookPath + "/", body: propertyUpdate, href: bookPath, privilege: davQN("write-properties")},
		{name: "ACL requires write-acl", method: "ACL", requestURI: bookPath + "/", href: bookPath, privilege: davQN("write-acl")},
		{name: "existing LOCK requires write-content", method: "LOCK", requestURI: bookPath + "/alice.vcf", body: lockBody, href: bookPath + "/alice.vcf", privilege: davQN("write-content"), readsCollection: true},
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
			if tt.readsCollection {
				aclRepo.entries = append(aclRepo.entries, store.ACLEntry{ResourcePath: bookPath, PrincipalHref: principal, IsGrant: true, Privilege: "read"})
			}
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

const exclusiveWriteLockBody = `<?xml version="1.0" encoding="utf-8"?><D:lockinfo xmlns:D="DAV:"><D:lockscope><D:exclusive/></D:lockscope><D:locktype><D:write/></D:locktype></D:lockinfo>`

type objectWriteDomain struct {
	name           string
	collectionPath string
	extension      string
	contentType    string
	body           func(uid string) string
}

var objectWriteDomains = []objectWriteDomain{
	{name: "calendar", collectionPath: "/dav/calendars/5", extension: ".ics", contentType: "text/calendar", body: func(uid string) string { return buildCalendarObject(buildVEvent(uid)) }},
	{name: "address book", collectionPath: "/dav/addressbooks/6", extension: ".vcf", contentType: "text/vcard", body: func(uid string) string { return buildVCard("3.0", "UID:"+uid, "FN:"+uid) }},
}

// objectWriteServer serves calendar 5 and address book 6, each owned by user 1
// and holding one object named "event", with locks and entries as given.
func objectWriteServer(locks *fakeLockRepo, entries []store.ACLEntry) *DavServer {
	return NewDavServer(Options{Store: &store.Store{
		Calendars: &fakeCalendarRepo{calendars: map[int64]*store.Calendar{5: {ID: 5, UserID: 1, Name: "Owner"}}},
		Events: &fakeEventRepo{events: map[string]*store.Event{
			"5:event": {CalendarID: 5, UID: "event", ResourceName: "event", RawICAL: buildCalendarObject(buildVEvent("event")), ETag: "event-etag"},
		}},
		AddressBooks: &fakeAddressBookRepo{books: map[int64]*store.AddressBook{6: {ID: 6, UserID: 1, Name: "Owner"}}},
		Contacts: &fakeContactRepo{contacts: map[string]*store.Contact{
			"6:event": {AddressBookID: 6, UID: "event", ResourceName: "event", RawVCard: buildVCard("3.0", "UID:event", "FN:Event"), ETag: "event-etag"},
		}},
		Locks:      locks,
		ACLEntries: &fakeACLRepo{entries: entries},
	}})
}

// objectWriteRequest builds a LOCK, or a PUT of an object whose UID is the
// resource name requestURI names.
func objectWriteRequest(d objectWriteDomain, method, requestURI string, user *store.User) *http.Request {
	body := exclusiveWriteLockBody
	if method == http.MethodPut {
		resourcePart := path.Base(requestURI)
		body = d.body(strings.TrimSuffix(resourcePart, path.Ext(resourcePart)))
	}
	req := httptest.NewRequest(method, requestURI, strings.NewReader(body))
	if method == http.MethodPut {
		req.Header.Set("Content-Type", d.contentType)
	}
	return req.WithContext(auth.WithUser(req.Context(), user))
}

// RFC 3744 §7.1.1 has a refusal name the privilege the principal lacked, and
// Appendix B makes that DAV:write-content on an existing object but DAV:bind on
// the parent of an absent one. Told apart, the two would let a principal that
// may not read a collection probe which names exist in it with PUT or LOCK, so
// such a principal gets the absent object's refusal either way, as GET and
// DELETE already keep an unreadable object's existence from it.
func TestRefusedWriteDisclosesObjectExistenceOnlyToAReader(t *testing.T) {
	delegate := &store.User{ID: 2}
	for _, d := range objectWriteDomains {
		for _, method := range []string{http.MethodPut, "LOCK"} {
			existingHref := d.collectionPath + "/event" + d.extension
			for _, principal := range []struct {
				name              string
				collectionGrants  []string
				objectDenies      []string
				existingHref      string
				existingPrivilege string
				absentStatus      int
				sameAsAbsent      bool
			}{
				{name: "reader", collectionGrants: []string{"read"}, existingHref: existingHref, existingPrivilege: "write-content", absentStatus: http.StatusForbidden},
				{name: "reader with write-properties", collectionGrants: []string{"read", "write-properties"}, existingHref: existingHref, existingPrivilege: "write-content", absentStatus: http.StatusForbidden},
				{name: "stranger", existingHref: d.collectionPath, existingPrivilege: "bind", absentStatus: http.StatusForbidden, sameAsAbsent: true},
				{name: "non-reader with write-properties", collectionGrants: []string{"write-properties"}, existingHref: d.collectionPath, existingPrivilege: "bind", absentStatus: http.StatusForbidden, sameAsAbsent: true},
				// The collection's read grant does not reach an object whose own
				// ACL denies read, so the collection's listing hides it as well.
				{name: "reader denied read on the object", collectionGrants: []string{"read"}, objectDenies: []string{"read"}, existingHref: d.collectionPath, existingPrivilege: "bind", absentStatus: http.StatusForbidden, sameAsAbsent: true},
				// Creating the absent object succeeds, which tells this principal
				// whether it existed however the refusal is worded.
				{name: "non-reader with bind", collectionGrants: []string{"bind"}, existingHref: existingHref, existingPrivilege: "write-content", absentStatus: http.StatusCreated},
			} {
				t.Run(d.name+"/"+method+"/"+principal.name, func(t *testing.T) {
					var entries []store.ACLEntry
					for _, privilege := range principal.collectionGrants {
						entries = append(entries, store.ACLEntry{ResourcePath: d.collectionPath, PrincipalHref: "/dav/principals/2/", IsGrant: true, Privilege: privilege})
					}
					for _, privilege := range principal.objectDenies {
						entries = append(entries, store.ACLEntry{ResourcePath: d.collectionPath + "/event", PrincipalHref: "/dav/principals/2/", IsGrant: false, Privilege: privilege})
					}

					absent := httptest.NewRecorder()
					objectWriteServer(&fakeLockRepo{}, entries).ServeHTTP(absent, objectWriteRequest(d, method, d.collectionPath+"/missing"+d.extension, delegate))
					existing := httptest.NewRecorder()
					objectWriteServer(&fakeLockRepo{}, entries).ServeHTTP(existing, objectWriteRequest(d, method, d.collectionPath+"/event"+d.extension, delegate))

					if principal.absentStatus == http.StatusForbidden {
						assertNeedPrivileges(t, absent, d.collectionPath, davQN("bind"))
					} else if absent.Code != principal.absentStatus {
						t.Fatalf("absent object status = %d, want %d: %s", absent.Code, principal.absentStatus, absent.Body.String())
					}
					assertNeedPrivileges(t, existing, principal.existingHref, davQN(principal.existingPrivilege))
					if principal.sameAsAbsent && existing.Body.String() != absent.Body.String() {
						t.Fatalf("refusal differs by existence:\nexisting: %s\nabsent:   %s", existing.Body.String(), absent.Body.String())
					}
				})
			}
		}
	}
}

// A DELETE needs DAV:unbind on the member's collection (RFC 3744 Appendix B)
// and, by CalCard's concealment rule, some privilege on the member. A principal that may not read an existing member is
// answered as for a member that is not there, so the member's own ACL entries,
// which refuse it where an absent name would not be refused, do not disclose
// that the member exists. One that may read it is told what it lacks.
func TestRefusedDeleteDisclosesMemberExistenceOnlyToAReader(t *testing.T) {
	delegate := &store.User{ID: 2}
	for _, d := range objectWriteDomains {
		for _, principal := range []struct {
			name             string
			collectionGrants []string
			objectGrants     []string
			objectDenies     []string
			concealed        bool
		}{
			{name: "stranger", concealed: true},
			{name: "reader", collectionGrants: []string{"read"}},
			{name: "reader the member is hidden from", collectionGrants: []string{"read"}, objectDenies: []string{"all"}, concealed: true},
			{name: "writer the member is hidden from", collectionGrants: []string{"read", "write-content", "unbind"}, objectDenies: []string{"all"}, concealed: true},
			{name: "member grant", objectGrants: []string{"read", "write-content", "unbind"}},
			{name: "writer", collectionGrants: []string{"read", "write-content", "unbind"}},
		} {
			for _, header := range []struct{ name, key, value string }{
				{name: "unconditional"},
				{name: "If-Match", key: "If-Match", value: `"event-etag"`},
				{name: "If-None-Match *", key: "If-None-Match", value: "*"},
			} {
				t.Run(d.name+"/"+principal.name+"/"+header.name, func(t *testing.T) {
					var entries []store.ACLEntry
					for _, privilege := range principal.collectionGrants {
						entries = append(entries, store.ACLEntry{ResourcePath: d.collectionPath, PrincipalHref: "/dav/principals/2/", IsGrant: true, Privilege: privilege})
					}
					for _, privilege := range principal.objectGrants {
						entries = append(entries, store.ACLEntry{ResourcePath: d.collectionPath + "/event", PrincipalHref: "/dav/principals/2/", IsGrant: true, Privilege: privilege})
					}
					for _, privilege := range principal.objectDenies {
						entries = append(entries, store.ACLEntry{ResourcePath: d.collectionPath + "/event", PrincipalHref: "/dav/principals/2/", IsGrant: false, Privilege: privilege})
					}
					deleteOf := func(name string) *httptest.ResponseRecorder {
						req := httptest.NewRequest(http.MethodDelete, d.collectionPath+"/"+name+d.extension, nil)
						if header.key != "" {
							req.Header.Set(header.key, header.value)
						}
						rr := httptest.NewRecorder()
						objectWriteServer(&fakeLockRepo{}, entries).ServeHTTP(rr, req.WithContext(auth.WithUser(req.Context(), delegate)))
						return rr
					}
					existing, absent := deleteOf("event"), deleteOf("missing")

					// RFC 9110 §13.2.1: a DELETE that would fail anyway does not
					// evaluate its preconditions, so a missing member is 404 even
					// under If-Match.
					absentWhenPermitted := http.StatusNotFound
					wantExisting, wantAbsent := http.StatusForbidden, http.StatusForbidden
					switch principal.name {
					case "stranger":
						wantExisting, wantAbsent = http.StatusNotFound, http.StatusNotFound
					case "writer the member is hidden from":
						wantExisting, wantAbsent = absentWhenPermitted, absentWhenPermitted
					case "member grant":
						wantAbsent = http.StatusNotFound
					case "writer":
						wantExisting, wantAbsent = http.StatusNoContent, absentWhenPermitted
						if header.key == "If-None-Match" {
							wantExisting = http.StatusPreconditionFailed
						}
					}
					if existing.Code != wantExisting || absent.Code != wantAbsent {
						t.Fatalf("existing %d, absent %d; want %d, %d\nexisting: %s\nabsent:   %s", existing.Code, absent.Code, wantExisting, wantAbsent, existing.Body.String(), absent.Body.String())
					}
					if wantAbsent == http.StatusForbidden {
						assertNeedPrivileges(t, absent, d.collectionPath, davQN("unbind"))
					}
					if principal.concealed && existing.Body.String() != absent.Body.String() {
						t.Fatalf("refusal differs by existence:\nexisting: %s\nabsent:   %s", existing.Body.String(), absent.Body.String())
					}
				})
			}
		}
	}
}

// RFC 3744 §8.1 refuses an ACL request without DAV:write-acl. Refusing an
// existing object by that privilege while answering 404 for an absent one
// would tell a principal that may not read the object that it exists, so such
// a principal is answered as for an absent object. One that may read it is
// told what it lacks.
func TestRefusedACLDisclosesObjectExistenceOnlyToAReader(t *testing.T) {
	const body = `<?xml version="1.0" encoding="utf-8"?><D:acl xmlns:D="DAV:"/>`
	delegate := &store.User{ID: 2}
	for _, d := range objectWriteDomains {
		for _, principal := range []struct {
			name             string
			collectionGrants []string
			objectDenies     []string
			concealed        bool
		}{
			{name: "stranger", concealed: true},
			{name: "non-reader with write-properties", collectionGrants: []string{"write-properties"}, concealed: true},
			{name: "reader denied read on the object", collectionGrants: []string{"read"}, objectDenies: []string{"read"}, concealed: true},
			{name: "reader", collectionGrants: []string{"read"}},
		} {
			t.Run(d.name+"/"+principal.name, func(t *testing.T) {
				var entries []store.ACLEntry
				for _, privilege := range principal.collectionGrants {
					entries = append(entries, store.ACLEntry{ResourcePath: d.collectionPath, PrincipalHref: "/dav/principals/2/", IsGrant: true, Privilege: privilege})
				}
				for _, privilege := range principal.objectDenies {
					entries = append(entries, store.ACLEntry{ResourcePath: d.collectionPath + "/event", PrincipalHref: "/dav/principals/2/", IsGrant: false, Privilege: privilege})
				}
				aclOf := func(name string) *httptest.ResponseRecorder {
					req := httptest.NewRequest("ACL", d.collectionPath+"/"+name+d.extension, strings.NewReader(body))
					rr := httptest.NewRecorder()
					objectWriteServer(&fakeLockRepo{}, entries).ServeHTTP(rr, req.WithContext(auth.WithUser(req.Context(), delegate)))
					return rr
				}
				existing, absent := aclOf("event"), aclOf("missing")

				if absent.Code != http.StatusNotFound {
					t.Fatalf("absent object status = %d, want 404: %s", absent.Code, absent.Body.String())
				}
				if !principal.concealed {
					assertNeedPrivileges(t, existing, d.collectionPath+"/event", davQN("write-acl"))
					return
				}
				if existing.Code != absent.Code || existing.Body.String() != absent.Body.String() {
					t.Fatalf("refusal differs by existence:\nexisting: %d %s\nabsent:   %d %s", existing.Code, existing.Body.String(), absent.Code, absent.Body.String())
				}
			})
		}
	}
}

// RFC 4791 §5.3.2.1 and RFC 6352 §6.3.2.1 have the no-uid-conflict refusal
// name the resource already holding the UID. It is named to a principal that
// may create members of the collection but not read them too: that principal
// supplied the UID, and learns one href in a collection it can write to.
func TestUIDConflictNamesTheHolderToAWriterThatMayNotReadIt(t *testing.T) {
	delegate := &store.User{ID: 2}
	for _, d := range objectWriteDomains {
		t.Run(d.name, func(t *testing.T) {
			entries := []store.ACLEntry{{ResourcePath: d.collectionPath, PrincipalHref: "/dav/principals/2/", IsGrant: true, Privilege: "bind"}}
			req := httptest.NewRequest(http.MethodPut, d.collectionPath+"/other"+d.extension, strings.NewReader(d.body("event")))
			req.Header.Set("Content-Type", d.contentType)
			rr := httptest.NewRecorder()
			objectWriteServer(&fakeLockRepo{}, entries).ServeHTTP(rr, req.WithContext(auth.WithUser(req.Context(), delegate)))
			if rr.Code != http.StatusConflict || !strings.Contains(rr.Body.String(), "no-uid-conflict") || !strings.Contains(rr.Body.String(), d.collectionPath+"/event"+d.extension) {
				t.Fatalf("PUT reusing a held UID = %d, want 409 no-uid-conflict naming %s: %s", rr.Code, d.collectionPath+"/event"+d.extension, rr.Body.String())
			}
		})
	}
}

// transferServer is objectWriteServer with a calendar 7 and an address book 8
// that user 2 owns, each holding one object named "mine" for user 2 to COPY or
// MOVE into calendar 5 and address book 6, which also hold members named
// "v1.2" and "a b". shares lists the calendars shared with user 2 beside its
// own.
func transferServer(entries []store.ACLEntry, shares ...store.CalendarAccess) *DavServer {
	events := &fakeEventRepo{events: map[string]*store.Event{
		"7:mine": {CalendarID: 7, UID: "mine", ResourceName: "mine", RawICAL: buildCalendarObject(buildVEvent("mine")), ETag: "mine-etag"},
	}}
	contacts := &fakeContactRepo{contacts: map[string]*store.Contact{
		"8:mine": {AddressBookID: 8, UID: "mine", ResourceName: "mine", RawVCard: buildVCard("3.0", "UID:mine", "FN:Mine"), ETag: "mine-etag"},
	}}
	for _, name := range []string{"event", "v1.2", "a b"} {
		events.events["5:"+name] = &store.Event{CalendarID: 5, UID: name, ResourceName: name, RawICAL: buildCalendarObject(buildVEvent(name)), ETag: name + "-etag"}
		contacts.contacts["6:"+name] = &store.Contact{AddressBookID: 6, UID: name, ResourceName: name, RawVCard: buildVCard("3.0", "UID:"+name, "FN:"+name), ETag: name + "-etag"}
	}
	calendars := &fakeCalendarRepo{calendars: map[int64]*store.Calendar{
		5: {ID: 5, UserID: 1, Name: "Owner"},
		7: {ID: 7, UserID: 2, Name: "Mine"},
	}}
	if len(shares) > 0 {
		calendars.accessibleByUser = map[int64][]store.CalendarAccess{2: append([]store.CalendarAccess{{Calendar: *calendars.calendars[7]}}, shares...)}
	}
	return NewDavServer(Options{Store: &store.Store{
		Calendars:         calendars,
		Events:            events,
		CalendarTransfers: events,
		AddressBooks: &fakeAddressBookRepo{books: map[int64]*store.AddressBook{
			6: {ID: 6, UserID: 1, Name: "Owner"},
			8: {ID: 8, UserID: 2, Name: "Mine"},
		}},
		Contacts:   contacts,
		Locks:      &fakeLockRepo{},
		ACLEntries: &fakeACLRepo{entries: entries},
	}})
}

// A principal that may not read a member gets the answer an absent member gets
// to an ACL request on it and to a COPY or MOVE onto it, however the request
// spells the member: without an extension, with a dot in its name, percent
// encoded, or in a collection named rather than numbered.
func TestConcealmentHoldsForEverySpellingOfTheMember(t *testing.T) {
	delegate := &store.User{ID: 2}
	sources := map[string]string{"calendar": "/dav/calendars/7/mine.ics", "address book": "/dav/addressbooks/8/mine.vcf"}
	for _, d := range objectWriteDomains {
		type spelling struct {
			name, existing, absent, member string
			// named addresses the collection by name, which resolves only
			// among the calendars shared with the principal.
			named bool
		}
		spellings := []spelling{
			{name: "extensionless", existing: d.collectionPath + "/event", absent: d.collectionPath + "/missing", member: "event"},
			{name: "dotted name", existing: d.collectionPath + "/v1.2" + d.extension, absent: d.collectionPath + "/v9.9" + d.extension, member: "v1.2"},
			{name: "percent-encoded name", existing: d.collectionPath + "/a%20b" + d.extension, absent: d.collectionPath + "/c%20d" + d.extension, member: "a b"},
		}
		if d.name == "calendar" {
			spellings = append(spellings, spelling{name: "named collection", existing: "/dav/calendars/Owner/event.ics", absent: "/dav/calendars/Owner/missing.ics", member: "event", named: true})
		}
		for _, sp := range spellings {
			for _, principal := range []struct {
				name     string
				readable bool
			}{{name: "stranger"}, {name: "reader denied read on the member", readable: true}} {
				if sp.named && !principal.readable {
					continue
				}
				var shares []store.CalendarAccess
				if sp.named {
					shares = []store.CalendarAccess{{Calendar: store.Calendar{ID: 5, UserID: 1, Name: "Owner"}, Shared: true}}
				}
				var entries []store.ACLEntry
				if principal.readable {
					entries = []store.ACLEntry{
						{ResourcePath: d.collectionPath, PrincipalHref: "/dav/principals/2/", IsGrant: true, Privilege: "read"},
						{ResourcePath: d.collectionPath + "/" + sp.member, PrincipalHref: "/dav/principals/2/", IsGrant: false, Privilege: "read"},
					}
				}
				for _, method := range []string{"ACL", "COPY", "MOVE"} {
					t.Run(d.name+"/"+sp.name+"/"+principal.name+"/"+method, func(t *testing.T) {
						answer := func(target string) *httptest.ResponseRecorder {
							var req *http.Request
							if method == "ACL" {
								req = httptest.NewRequest(method, target, strings.NewReader(`<?xml version="1.0" encoding="utf-8"?><D:acl xmlns:D="DAV:"/>`))
							} else {
								req = httptest.NewRequest(method, sources[d.name], nil)
								req.Header.Set("Destination", target)
							}
							rr := httptest.NewRecorder()
							transferServer(entries, shares...).ServeHTTP(rr, req.WithContext(auth.WithUser(req.Context(), delegate)))
							return rr
						}
						existing, absent := answer(sp.existing), answer(sp.absent)
						if existing.Code < 400 {
							t.Fatalf("%s of an unreadable member = %d, want a refusal: %s", method, existing.Code, existing.Body.String())
						}
						if existing.Code != absent.Code || existing.Body.String() != absent.Body.String() {
							t.Fatalf("refusal differs by existence:\nexisting: %d %s\nabsent:   %d %s", existing.Code, existing.Body.String(), absent.Code, absent.Body.String())
						}
					})
				}
			}
		}
	}
}

// RFC 3744 Appendix B has a COPY or MOVE onto an existing destination need
// DAV:write-content and DAV:write-properties on it or DAV:unbind on its
// collection, and one onto an absent destination DAV:bind on the collection.
// Told apart, the two would let a principal that may not read the destination
// probe which names exist, so such a principal gets the absent destination's
// refusal either way, as PUT does. One that may read the destination, or that
// the absent destination would admit, is told what it lacks.
func TestRefusedTransferDisclosesDestinationExistenceOnlyToAReader(t *testing.T) {
	type refusal struct{ href, privilege string }
	delegate := &store.User{ID: 2}
	sources := map[string]string{"calendar": "/dav/calendars/7/mine.ics", "address book": "/dav/addressbooks/8/mine.vcf"}
	for _, d := range objectWriteDomains {
		destination := d.collectionPath + "/event" + d.extension
		for _, method := range []string{"COPY", "MOVE"} {
			for _, overwrite := range []string{"T", "F"} {
				for _, principal := range []struct {
					name             string
					collectionGrants []string
					objectDenies     []string
					absentStatus     int
					// existing is the refusal of the existing destination by
					// domain and method, or nil where it matches the absent one.
					existing map[string]refusal
				}{
					{name: "stranger", absentStatus: http.StatusForbidden},
					{name: "non-reader with write-properties", collectionGrants: []string{"write-properties"}, absentStatus: http.StatusForbidden},
					{name: "reader denied read on the destination", collectionGrants: []string{"read"}, objectDenies: []string{"read"}, absentStatus: http.StatusForbidden},
					{name: "non-reader with write-content denied write-properties on the destination", collectionGrants: []string{"write-content"}, objectDenies: []string{"read", "write-properties"}, absentStatus: http.StatusForbidden},
					{name: "reader", collectionGrants: []string{"read"}, absentStatus: http.StatusForbidden, existing: map[string]refusal{
						"calendar/COPY":     {destination, "write-content"},
						"calendar/MOVE":     {d.collectionPath, "unbind"},
						"address book/COPY": {destination, "write-content"},
						"address book/MOVE": {d.collectionPath, "unbind"},
					}},
					// Creating the absent destination succeeds, which tells this
					// principal whether it existed however the refusal is worded.
					{name: "non-reader with bind", collectionGrants: []string{"bind"}, absentStatus: http.StatusCreated, existing: map[string]refusal{
						"calendar/COPY":     {destination, "write-content"},
						"calendar/MOVE":     {d.collectionPath, "unbind"},
						"address book/COPY": {destination, "write-content"},
						"address book/MOVE": {d.collectionPath, "unbind"},
					}},
				} {
					t.Run(d.name+"/"+method+"/Overwrite "+overwrite+"/"+principal.name, func(t *testing.T) {
						var entries []store.ACLEntry
						for _, privilege := range principal.collectionGrants {
							entries = append(entries, store.ACLEntry{ResourcePath: d.collectionPath, PrincipalHref: "/dav/principals/2/", IsGrant: true, Privilege: privilege})
						}
						for _, privilege := range principal.objectDenies {
							entries = append(entries, store.ACLEntry{ResourcePath: d.collectionPath + "/event", PrincipalHref: "/dav/principals/2/", IsGrant: false, Privilege: privilege})
						}
						transferTo := func(name string) *httptest.ResponseRecorder {
							req := httptest.NewRequest(method, sources[d.name], nil)
							req.Header.Set("Destination", d.collectionPath+"/"+name+d.extension)
							req.Header.Set("Overwrite", overwrite)
							rr := httptest.NewRecorder()
							transferServer(entries).ServeHTTP(rr, req.WithContext(auth.WithUser(req.Context(), delegate)))
							return rr
						}
						existing, absent := transferTo("event"), transferTo("missing")

						switch {
						case principal.absentStatus != http.StatusForbidden:
							if absent.Code != principal.absentStatus {
								t.Fatalf("absent destination status = %d, want %d: %s", absent.Code, principal.absentStatus, absent.Body.String())
							}
						case slices.Contains(principal.collectionGrants, "read"):
							assertNeedPrivileges(t, absent, d.collectionPath, davQN("bind"))
						default:
							// This pins current behaviour rather than a requirement: a
							// principal that may not read the collection is still told
							// it exists by a refusal naming it.
							t.Run("current behaviour/refusal names an unreadable collection", func(t *testing.T) {
								assertNeedPrivileges(t, absent, d.collectionPath, davQN("bind"))
							})
						}
						want, named := principal.existing[d.name+"/"+method]
						if named {
							assertNeedPrivileges(t, existing, want.href, davQN(want.privilege))
							return
						}
						if existing.Code != absent.Code || existing.Body.String() != absent.Body.String() {
							t.Fatalf("refusal differs by existence:\nexisting: %d %s\nabsent:   %d %s", existing.Code, existing.Body.String(), absent.Code, absent.Body.String())
						}
					})
				}
			}
		}
	}
}

// RFC 3744 Appendix B has a COPY onto an existing destination need DAV:read,
// DAV:write-content and DAV:write-properties on it, a PUT replacing it only
// DAV:write-content, and a MOVE onto one DAV:bind and DAV:unbind on its
// collection, in address books as in calendars: a principal denied any of the
// three on the destination may not overwrite it by COPY, which replaces its
// body and its properties, whatever it holds on the collection, but may by
// MOVE. One denied read that may create members of the collection is told
// what it lacks, as it learns whether the destination exists by creating it;
// one that may not create members is answered as for an absent destination.
func TestCopyOntoAnExistingObjectNeedsReadWriteContentAndWritePropertiesOnIt(t *testing.T) {
	delegate := &store.User{ID: 2}
	sources := map[string]string{"calendar": "/dav/calendars/7/mine.ics", "address book": "/dav/addressbooks/8/mine.vcf"}
	for _, d := range objectWriteDomains {
		destination := d.collectionPath + "/event" + d.extension
		for _, tt := range []struct {
			denied string
			// collectionGrants are what the principal holds on the collection
			// beside DAV:read.
			collectionGrants []string
			// wantRefusal is the COPY's refusal, as privilege and href.
			wantRefusal, wantRefusalHref string
			putAllowed                   bool
		}{
			{denied: "write-content", collectionGrants: []string{"write"}, wantRefusal: "write-content", wantRefusalHref: destination},
			{denied: "write-properties", collectionGrants: []string{"write"}, wantRefusal: "write-properties", wantRefusalHref: destination, putAllowed: true},
			{denied: "read", collectionGrants: []string{"write"}, wantRefusal: "read", wantRefusalHref: destination, putAllowed: true},
			{denied: "read", collectionGrants: []string{"write-content", "write-properties", "unbind"}, wantRefusal: "bind", wantRefusalHref: d.collectionPath, putAllowed: true},
		} {
			entries := []store.ACLEntry{
				{ResourcePath: d.collectionPath, PrincipalHref: "/dav/principals/2/", IsGrant: true, Privilege: "read"},
				{ResourcePath: d.collectionPath + "/event", PrincipalHref: "/dav/principals/2/", IsGrant: false, Privilege: tt.denied},
			}
			for _, privilege := range tt.collectionGrants {
				entries = append(entries, store.ACLEntry{ResourcePath: d.collectionPath, PrincipalHref: "/dav/principals/2/", IsGrant: true, Privilege: privilege})
			}
			for _, method := range []string{http.MethodPut, "COPY", "MOVE"} {
				t.Run(d.name+"/"+tt.denied+" denied, "+strings.Join(tt.collectionGrants, " ")+" held/"+method, func(t *testing.T) {
					var req *http.Request
					if method == http.MethodPut {
						req = httptest.NewRequest(method, destination, strings.NewReader(d.body("event")))
						req.Header.Set("Content-Type", d.contentType)
					} else {
						req = httptest.NewRequest(method, sources[d.name], nil)
						req.Header.Set("Destination", destination)
						req.Header.Set("Overwrite", "T")
					}
					rr := httptest.NewRecorder()
					transferServer(entries).ServeHTTP(rr, req.WithContext(auth.WithUser(req.Context(), delegate)))
					switch {
					case method == "MOVE" && slices.Contains(tt.collectionGrants, "write"), method == http.MethodPut && tt.putAllowed:
						if rr.Code != http.StatusNoContent {
							t.Fatalf("%s onto the existing object = %d, want 204: %s", method, rr.Code, rr.Body.String())
						}
					case method == "COPY":
						assertNeedPrivileges(t, rr, tt.wantRefusalHref, davQN(tt.wantRefusal))
					case method == "MOVE":
						assertNeedPrivileges(t, rr, d.collectionPath, davQN("bind"))
					default:
						assertNeedPrivileges(t, rr, destination, davQN(tt.denied))
					}
				})
			}
		}
	}
}

// A COPY or MOVE of a missing object is answered 404 for its source before
// its destination is looked at, in address books as in calendars, so a
// destination in a collection that does not exist does not change the answer.
func TestTransferOfAMissingSourceIsNotFoundWhateverItsDestination(t *testing.T) {
	owner := &store.User{ID: 2}
	sources := map[string]string{"calendar": "/dav/calendars/7/missing.ics", "address book": "/dav/addressbooks/8/missing.vcf"}
	destinations := map[string]string{"calendar": "/dav/calendars/nosuch/x.ics", "address book": "/dav/addressbooks/nosuch/x.vcf"}
	for _, d := range objectWriteDomains {
		for _, method := range []string{"COPY", "MOVE"} {
			t.Run(d.name+"/"+method, func(t *testing.T) {
				req := httptest.NewRequest(method, sources[d.name], nil)
				req.Header.Set("Destination", destinations[d.name])
				rr := httptest.NewRecorder()
				transferServer(nil).ServeHTTP(rr, req.WithContext(auth.WithUser(req.Context(), owner)))
				if rr.Code != http.StatusNotFound {
					t.Fatalf("%s of a missing source to a missing collection = %d, want 404: %s", method, rr.Code, rr.Body.String())
				}
			})
		}
	}
}

// A lock on a member the principal may not read belongs to that member, not
// to an unmapped URL the lock reserves (RFC 4918 §7.3). A Depth 0 PROPFIND of
// the member therefore answers as it does for the member unlocked, without the
// activelock and its owner, just as a Depth 1 listing of the collection leaves
// the member out. A locked collection a stranger may not read is answered as
// it is unlocked too. A lock on an unmapped URL is still listed to a reader of
// the collection, and the lock of a member the principal may read is reported.
func TestPropfindHidesTheLockOfAnUnreadableResource(t *testing.T) {
	const (
		token = "opaquelocktoken:owner-lock"
		owner = "owner-lock-holder@example.com"
		body  = `<?xml version="1.0" encoding="utf-8"?><D:propfind xmlns:D="DAV:"><D:prop><D:displayname/><D:resourcetype/><D:lockdiscovery/></D:prop></D:propfind>`
	)
	delegate := &store.User{ID: 2}
	lockedAt := func(resourcePath string) *fakeLockRepo {
		return &fakeLockRepo{locks: map[string]*store.Lock{token: {
			Token: token, ResourcePath: resourcePath, UserID: 1, LockScope: "exclusive", LockType: "write", Depth: "0",
			OwnerInfo: "<D:href>" + owner + "</D:href>", TimeoutSeconds: 3600, ExpiresAt: time.Now().Add(time.Hour),
		}}}
	}
	for _, d := range objectWriteDomains {
		propfind := func(locks *fakeLockRepo, entries []store.ACLEntry, target, depth string) *httptest.ResponseRecorder {
			req := httptest.NewRequest("PROPFIND", target, strings.NewReader(body))
			req.Header.Set("Depth", depth)
			rr := httptest.NewRecorder()
			objectWriteServer(locks, entries).ServeHTTP(rr, req.WithContext(auth.WithUser(req.Context(), delegate)))
			return rr
		}
		readGrant := store.ACLEntry{ResourcePath: d.collectionPath, PrincipalHref: "/dav/principals/2/", IsGrant: true, Privilege: "read"}
		readDeny := store.ACLEntry{ResourcePath: d.collectionPath + "/event", PrincipalHref: "/dav/principals/2/", IsGrant: false, Privilege: "read"}
		member := d.collectionPath + "/event" + d.extension

		t.Run(d.name+"/unreadable member", func(t *testing.T) {
			entries := []store.ACLEntry{readGrant, readDeny}
			locked := propfind(lockedAt(d.collectionPath+"/event"), entries, member, "0")
			unlocked := propfind(&fakeLockRepo{}, entries, member, "0")
			if locked.Code != unlocked.Code || locked.Body.String() != unlocked.Body.String() {
				t.Fatalf("Depth 0 answer differs by the member's lock:\nlocked:   %d %s\nunlocked: %d %s", locked.Code, locked.Body.String(), unlocked.Code, unlocked.Body.String())
			}
			absent := propfind(&fakeLockRepo{}, entries, d.collectionPath+"/missing"+d.extension, "0")
			if locked.Code != absent.Code || locked.Body.String() != strings.ReplaceAll(absent.Body.String(), "/missing", "/event") {
				t.Fatalf("Depth 0 answer differs from an absent member's:\nunreadable: %d %s\nabsent:     %d %s", locked.Code, locked.Body.String(), absent.Code, absent.Body.String())
			}
			listing := propfind(lockedAt(d.collectionPath+"/event"), entries, d.collectionPath+"/", "1")
			for _, hidden := range []string{"activelock", owner, "/event" + d.extension} {
				if strings.Contains(listing.Body.String(), hidden) {
					t.Errorf("Depth 1 listing disclosed %q of an unreadable member: %s", hidden, listing.Body.String())
				}
			}
		})
		t.Run(d.name+"/locked collection a stranger may not read", func(t *testing.T) {
			locked := propfind(lockedAt(d.collectionPath), nil, d.collectionPath+"/", "0")
			unlocked := propfind(&fakeLockRepo{}, nil, d.collectionPath+"/", "0")
			if locked.Code != http.StatusNotFound || locked.Code != unlocked.Code || locked.Body.String() != unlocked.Body.String() {
				t.Fatalf("Depth 0 answer differs by the collection's lock:\nlocked:   %d %s\nunlocked: %d %s", locked.Code, locked.Body.String(), unlocked.Code, unlocked.Body.String())
			}
			for _, hidden := range []string{"activelock", owner} {
				if strings.Contains(locked.Body.String(), hidden) {
					t.Errorf("PROPFIND disclosed %q of an unreadable collection: %s", hidden, locked.Body.String())
				}
			}
		})
		t.Run(d.name+"/lock on an unmapped URL", func(t *testing.T) {
			rr := propfind(lockedAt(d.collectionPath+"/reserved"), []store.ACLEntry{readGrant}, d.collectionPath+"/reserved"+d.extension, "0")
			if rr.Code != http.StatusMultiStatus || !strings.Contains(rr.Body.String(), "activelock") {
				t.Fatalf("Depth 0 PROPFIND of a locked unmapped URL = %d, want its activelock: %s", rr.Code, rr.Body.String())
			}
		})
		t.Run(d.name+"/readable member", func(t *testing.T) {
			rr := propfind(lockedAt(d.collectionPath+"/event"), []store.ACLEntry{readGrant}, member, "0")
			if rr.Code != http.StatusMultiStatus || !strings.Contains(rr.Body.String(), "activelock") || !strings.Contains(rr.Body.String(), owner) {
				t.Fatalf("Depth 0 PROPFIND of a readable locked member = %d, want its activelock and owner: %s", rr.Code, rr.Body.String())
			}
		})
	}
}

// A write is checked against locks only once the principal is known to hold
// the privilege it needs, so a refused principal's answer cannot tell it
// whether the owner holds a lock on the object, on the collection, or on
// nothing. A principal that may write meets each lock as RFC 4918 has it: a
// lock on the object or a depth-infinity lock above it covers the object
// (§7.4), while a depth 0 lock on the collection covers its membership only
// (§7.5), so it stops creating a member, by PUT or by a LOCK of the unmapped URL
// (§9.10.4), but not rewriting or locking one that exists. The lock's creator
// passes with the token.
func TestWriteMeetsLocksOnlyOnceAuthorized(t *testing.T) {
	const ownerToken = "opaquelocktoken:owner"
	stranger := &store.User{ID: 2}
	editor := &store.User{ID: 3}
	owner := &store.User{ID: 1}
	type lockState struct {
		name  string
		path  func(collectionPath, objectPath string) string
		depth string
	}
	lockStates := []lockState{
		{name: "unlocked"},
		{name: "object locked", path: func(_, objectPath string) string { return objectPath }, depth: "0"},
		{name: "collection locked to depth infinity", path: func(collectionPath, _ string) string { return collectionPath }, depth: "infinity"},
		{name: "collection locked to depth 0", path: func(collectionPath, _ string) string { return collectionPath }, depth: "0"},
	}
	locksFor := func(state lockState, collectionPath, objectPath string) *fakeLockRepo {
		locks := &fakeLockRepo{locks: map[string]*store.Lock{}}
		if state.path != nil {
			locks.locks[ownerToken] = &store.Lock{
				Token: ownerToken, ResourcePath: state.path(collectionPath, objectPath), UserID: owner.ID,
				LockScope: "exclusive", LockType: "write", Depth: state.depth, ExpiresAt: time.Now().Add(time.Hour),
			}
		}
		return locks
	}
	for _, d := range objectWriteDomains {
		editorGrants := []store.ACLEntry{
			{ResourcePath: d.collectionPath, PrincipalHref: "/dav/principals/3/", IsGrant: true, Privilege: "read"},
			{ResourcePath: d.collectionPath, PrincipalHref: "/dav/principals/3/", IsGrant: true, Privilege: "write-content"},
			{ResourcePath: d.collectionPath, PrincipalHref: "/dav/principals/3/", IsGrant: true, Privilege: "bind"},
		}
		for _, method := range []string{http.MethodPut, "LOCK"} {
			for _, name := range []string{"missing", "event"} {
				exists := name == "event"
				objectPath := d.collectionPath + "/" + name
				requestURI := objectPath + d.extension

				unlockedRefusal := httptest.NewRecorder()
				objectWriteServer(locksFor(lockStates[0], d.collectionPath, objectPath), nil).ServeHTTP(unlockedRefusal, objectWriteRequest(d, method, requestURI, stranger))
				t.Run(d.name+"/"+method+"/"+name+"/stranger refused", func(t *testing.T) {
					assertNeedPrivileges(t, unlockedRefusal, d.collectionPath, davQN("bind"))
				})

				// What succeeds for an unlocked object: PUT creates (201) or
				// replaces (204); LOCK creates (201) or locks the object (200).
				succeeds := http.StatusCreated
				if exists && method == http.MethodPut {
					succeeds = http.StatusNoContent
				} else if exists {
					succeeds = http.StatusOK
				}
				for _, state := range lockStates {
					prefix := d.name + "/" + method + "/" + name + "/" + state.name
					t.Run(prefix+"/stranger", func(t *testing.T) {
						rr := httptest.NewRecorder()
						objectWriteServer(locksFor(state, d.collectionPath, objectPath), nil).ServeHTTP(rr, objectWriteRequest(d, method, requestURI, stranger))
						if rr.Code != unlockedRefusal.Code || rr.Body.String() != unlockedRefusal.Body.String() {
							t.Fatalf("refusal depends on lock state:\nlocked:   %d %s\nunlocked: %d %s", rr.Code, rr.Body.String(), unlockedRefusal.Code, unlockedRefusal.Body.String())
						}
					})

					membershipLockOnly := state.name == "collection locked to depth 0"
					want := http.StatusLocked
					if state.path == nil || (membershipLockOnly && exists) {
						want = succeeds
					}
					t.Run(prefix+"/authorized without the token", func(t *testing.T) {
						rr := httptest.NewRecorder()
						objectWriteServer(locksFor(state, d.collectionPath, objectPath), editorGrants).ServeHTTP(rr, objectWriteRequest(d, method, requestURI, editor))
						if rr.Code != want {
							t.Fatalf("status = %d, want %d: %s", rr.Code, want, rr.Body.String())
						}
					})

					// A LOCK with a body asks for a new lock (RFC 4918 §9.10.2),
					// so its untagged If list submits the collection's token as
					// any other write's does.
					if state.path == nil || method == "LOCK" && !(membershipLockOnly && !exists) {
						continue
					}
					ifHeader := "(<" + ownerToken + ">)"
					t.Run(prefix+"/lock creator with the token", func(t *testing.T) {
						req := objectWriteRequest(d, method, requestURI, owner)
						req.Header.Set("If", ifHeader)
						rr := httptest.NewRecorder()
						objectWriteServer(locksFor(state, d.collectionPath, objectPath), nil).ServeHTTP(rr, req)
						if rr.Code != succeeds {
							t.Fatalf("status = %d, want %d: %s", rr.Code, succeeds, rr.Body.String())
						}
					})
					// RFC 4918 §6.4: a token only counts in its creator's hands.
					t.Run(prefix+"/authorized with the creator's token", func(t *testing.T) {
						req := objectWriteRequest(d, method, requestURI, editor)
						req.Header.Set("If", ifHeader)
						rr := httptest.NewRecorder()
						objectWriteServer(locksFor(state, d.collectionPath, objectPath), editorGrants).ServeHTTP(rr, req)
						if rr.Code != want {
							t.Fatalf("status = %d, want %d: %s", rr.Code, want, rr.Body.String())
						}
					})
				}
			}
		}
	}
}

// RFC 4918 §9.10.2 makes a LOCK without a body a refresh of the lock its If
// header names, and one with a body a request for a new lock. A new lock on an
// unmapped member of a collection locked to depth 0 submits the collection's
// token in either list form. A LOCK of the collection that submits the
// requester's own lock on it refreshes that lock, with no body as RFC 4918
// has it, or with the lockinfo body some clients resend.
func TestLockBodyDecidesBetweenCreatingAndRefreshing(t *testing.T) {
	const collectionToken = "opaquelocktoken:collection"
	owner := &store.User{ID: 1}
	for _, d := range objectWriteDomains {
		locks := func() *fakeLockRepo {
			return &fakeLockRepo{locks: map[string]*store.Lock{collectionToken: {
				Token: collectionToken, ResourcePath: d.collectionPath, UserID: owner.ID, LockScope: "exclusive", LockType: "write",
				Depth: "0", TimeoutSeconds: 60, ExpiresAt: time.Now().Add(time.Minute),
			}}}
		}
		for _, tt := range []struct {
			name, target, ifHeader, body string
			want                         int
		}{
			{name: "create with the collection's token untagged", target: d.collectionPath + "/new" + d.extension, ifHeader: "(<" + collectionToken + ">)", body: exclusiveWriteLockBody, want: http.StatusCreated},
			{name: "create with the collection's token tagged", target: d.collectionPath + "/new" + d.extension, ifHeader: "<" + d.collectionPath + "/> (<" + collectionToken + ">)", body: exclusiveWriteLockBody, want: http.StatusCreated},
			{name: "create without the collection's token", target: d.collectionPath + "/new" + d.extension, body: exclusiveWriteLockBody, want: http.StatusLocked},
			{name: "refresh without a body", target: d.collectionPath + "/", ifHeader: "(<" + collectionToken + ">)", want: http.StatusOK},
			{name: "refresh with a body", target: d.collectionPath + "/", ifHeader: "(<" + collectionToken + ">)", body: exclusiveWriteLockBody, want: http.StatusOK},
			{name: "refresh with a body and the token tagged", target: d.collectionPath + "/", ifHeader: "<" + d.collectionPath + "/> (<" + collectionToken + ">)", body: exclusiveWriteLockBody, want: http.StatusOK},
		} {
			t.Run(d.name+"/"+tt.name, func(t *testing.T) {
				repo := locks()
				req := httptest.NewRequest("LOCK", tt.target, strings.NewReader(tt.body))
				if tt.ifHeader != "" {
					req.Header.Set("If", tt.ifHeader)
				}
				req.Header.Set("Timeout", "Second-3600")
				rr := httptest.NewRecorder()
				objectWriteServer(repo, nil).ServeHTTP(rr, req.WithContext(auth.WithUser(req.Context(), owner)))
				if rr.Code != tt.want {
					t.Fatalf("LOCK status = %d, want %d: %s", rr.Code, tt.want, rr.Body.String())
				}
				collectionLock, _ := repo.GetByToken(t.Context(), collectionToken)
				refreshed := collectionLock != nil && collectionLock.TimeoutSeconds == 3600
				if refreshed != (tt.want == http.StatusOK) {
					t.Errorf("collection lock refreshed = %v after a %d: %#v", refreshed, rr.Code, collectionLock)
				}
				wantLocks := 1
				if tt.want == http.StatusCreated {
					wantLocks = 2
				}
				if len(repo.locks) != wantLocks {
					t.Errorf("%d locks after a %d, want %d: %#v", len(repo.locks), rr.Code, wantLocks, repo.locks)
				}
			})
		}
	}
}

// A LOCK with a body that submits the requester's own lock on the resource and
// asks for that lock's scope again refreshes it, for clients that resend the
// lockinfo on refresh; a refresh ignores the Depth header (RFC 4918 §9.10.2).
// Any other LOCK with a body is a request for a new lock: one asking for
// another scope meets the existing lock as a conflict, a malformed one is
// refused, and another principal's token refreshes nothing, its shared lock
// sharing the resource with a new one and its exclusive lock refusing it.
func TestLockWithABodyRefreshesOnlyTheRequestersOwnLockAskedForAgain(t *testing.T) {
	const token = "opaquelocktoken:held"
	sharedBody := `<?xml version="1.0" encoding="utf-8"?><D:lockinfo xmlns:D="DAV:"><D:lockscope><D:shared/></D:lockscope><D:locktype><D:write/></D:locktype></D:lockinfo>`
	owner := &store.User{ID: 1}
	for _, d := range objectWriteDomains {
		for _, tt := range []struct {
			name          string
			holder        int64
			heldScope     string
			body, depth   string
			want          int
			wantRefreshed bool
			wantLocks     int
		}{
			{name: "own exclusive lock asked for again", holder: 1, heldScope: "exclusive", body: exclusiveWriteLockBody, want: http.StatusOK, wantRefreshed: true, wantLocks: 1},
			{name: "own exclusive lock asked for again with another Depth", holder: 1, heldScope: "exclusive", body: exclusiveWriteLockBody, depth: "infinity", want: http.StatusOK, wantRefreshed: true, wantLocks: 1},
			{name: "own shared lock asked for again", holder: 1, heldScope: "shared", body: sharedBody, want: http.StatusOK, wantRefreshed: true, wantLocks: 1},
			{name: "own shared lock and an exclusive lockinfo", holder: 1, heldScope: "shared", body: exclusiveWriteLockBody, want: http.StatusLocked, wantLocks: 1},
			{name: "own exclusive lock and a body that is not XML", holder: 1, heldScope: "exclusive", body: "<not xml", want: http.StatusBadRequest, wantLocks: 1},
			{name: "own exclusive lock and a lockinfo without a type", holder: 1, heldScope: "exclusive", body: `<?xml version="1.0" encoding="utf-8"?><D:lockinfo xmlns:D="DAV:"><D:lockscope><D:exclusive/></D:lockscope></D:lockinfo>`, want: http.StatusBadRequest, wantLocks: 1},
			{name: "another principal's shared lock and a shared lockinfo", holder: 3, heldScope: "shared", body: sharedBody, want: http.StatusOK, wantLocks: 2},
			{name: "another principal's shared lock and an exclusive lockinfo", holder: 3, heldScope: "shared", body: exclusiveWriteLockBody, want: http.StatusLocked, wantLocks: 1},
		} {
			t.Run(d.name+"/"+tt.name, func(t *testing.T) {
				repo := &fakeLockRepo{locks: map[string]*store.Lock{token: {
					Token: token, ResourcePath: d.collectionPath + "/event", UserID: tt.holder, LockScope: tt.heldScope, LockType: "write",
					Depth: "0", TimeoutSeconds: 60, ExpiresAt: time.Now().Add(time.Minute),
				}}}
				req := httptest.NewRequest("LOCK", d.collectionPath+"/event"+d.extension, strings.NewReader(tt.body))
				req.Header.Set("If", "(<"+token+">)")
				req.Header.Set("Timeout", "Second-3600")
				if tt.depth != "" {
					req.Header.Set("Depth", tt.depth)
				}
				rr := httptest.NewRecorder()
				objectWriteServer(repo, nil).ServeHTTP(rr, req.WithContext(auth.WithUser(req.Context(), owner)))
				if rr.Code != tt.want {
					t.Fatalf("LOCK status = %d, want %d: %s", rr.Code, tt.want, rr.Body.String())
				}
				held, _ := repo.GetByToken(t.Context(), token)
				if refreshed := held != nil && held.TimeoutSeconds == 3600; refreshed != tt.wantRefreshed {
					t.Errorf("held lock refreshed = %v, want %v: %#v", refreshed, tt.wantRefreshed, held)
				}
				if len(repo.locks) != tt.wantLocks {
					t.Errorf("%d locks, want %d: %#v", len(repo.locks), tt.wantLocks, repo.locks)
				}
				// §9.10.2: a refresh returns no Lock-Token; a new lock does.
				if newToken := rr.Header().Get("Lock-Token"); (newToken != "") != (tt.wantLocks == 2) || strings.Contains(newToken, token) {
					t.Errorf("Lock-Token = %q after a %d", newToken, rr.Code)
				}
			})
		}
	}
}

// Finding the requester's own lock among the tokens a LOCK with a body
// submits takes as many lock repository calls however many tokens the If
// header carries.
func TestLockWithABodyReadsLocksOnceWhateverItsTokens(t *testing.T) {
	const token = "opaquelocktoken:held"
	owner := &store.User{ID: 1}
	d := objectWriteDomains[0]
	queries := func(decoys int) int {
		repo := &fakeLockRepo{locks: map[string]*store.Lock{token: {
			Token: token, ResourcePath: d.collectionPath + "/event", UserID: owner.ID, LockScope: "exclusive", LockType: "write",
			Depth: "0", TimeoutSeconds: 60, ExpiresAt: time.Now().Add(time.Minute),
		}}}
		var ifHeader strings.Builder
		for i := range decoys {
			fmt.Fprintf(&ifHeader, "(<opaquelocktoken:decoy-%d>) ", i)
		}
		ifHeader.WriteString("(<" + token + ">)")
		req := httptest.NewRequest("LOCK", d.collectionPath+"/event"+d.extension, strings.NewReader(exclusiveWriteLockBody))
		req.Header.Set("If", ifHeader.String())
		rr := httptest.NewRecorder()
		objectWriteServer(repo, nil).ServeHTTP(rr, req.WithContext(auth.WithUser(req.Context(), owner)))
		if rr.Code != http.StatusOK {
			t.Fatalf("LOCK refresh with %d decoy tokens = %d: %s", decoys, rr.Code, rr.Body.String())
		}
		return repo.calls
	}
	if few, many := queries(1), queries(2000); many != few {
		t.Fatalf("lock queries grew with the tokens submitted: %d with 1 decoy, %d with 2000", few, many)
	}
}

// An If header is read once per request, however many write targets check it.
func TestIfHeaderIsParsedOncePerRequest(t *testing.T) {
	req := httptest.NewRequest("LOCK", "/dav/calendars/5/event.ics", nil)
	req = req.WithContext(withDAVRequestState(req.Context()))
	req.Header.Set("If", "(<opaquelocktoken:first>)")
	h := &DavServer{}
	first := h.parseIfHeader(req)
	req.Header.Set("If", "(<opaquelocktoken:second>)")
	if again := h.parseIfHeader(req); again != first || !again.submitsLockToken("opaquelocktoken:first") {
		t.Fatalf("If header parsed again within one request: %#v", again)
	}
}

// Reading an If header's tokens is linear in their number, each kept once.
func TestIfHeaderParsingScalesLinearlyWithItsTokens(t *testing.T) {
	assertScalesLinearly(t, 20000, func(n int) func() {
		var header strings.Builder
		for i := range n {
			fmt.Fprintf(&header, "(<opaquelocktoken:%d>) ", i)
		}
		raw := header.String()
		return func() {
			state := parseIfHeaderState(raw)
			if len(state.tokens) != n {
				t.Fatalf("parsed %d tokens, want %d", len(state.tokens), n)
			}
		}
	})
}

// lockCheckingCalendarCollectionBackend answers a collection transfer as the
// store does for its locks, checking the transfer's lock preconditions against
// every lock held, and changes nothing.
type lockCheckingCalendarCollectionBackend struct {
	locks *fakeLockRepo
}

func (b *lockCheckingCalendarCollectionBackend) TransferCalendarCollection(_ context.Context, transfer store.CalendarCollectionTransfer) (*store.CalendarCollectionTransferResult, error) {
	locks := make([]store.Lock, 0, len(b.locks.locks))
	for _, lock := range b.locks.locks {
		locks = append(locks, *lock)
	}
	if !store.LockPreconditionsSatisfied(transfer.LockPreconditions, locks) {
		return nil, store.ErrLockConflict
	}
	return &store.CalendarCollectionTransferResult{}, nil
}

// A collection DELETE checks every member's locks against the If header's
// tokens and has the store check them again, in work linear in the members
// and the tokens together: neither the check nor the preconditions it hands
// the store go through the header's tokens once per member.
func TestCollectionDeleteLockChecksScaleLinearlyWithMembersAndTokens(t *testing.T) {
	const token = "opaquelocktoken:collection"
	owner := &store.User{ID: 1}
	assertScalesLinearly(t, 20000, func(n int) func() {
		events := &fakeEventRepo{events: map[string]*store.Event{}}
		for i := range n {
			name := fmt.Sprintf("member-%d", i)
			events.events["5:"+name] = &store.Event{ID: int64(i + 1), CalendarID: 5, UID: name, ResourceName: name, RawICAL: buildCalendarObject(buildVEvent(name)), ETag: name}
		}
		locks := &fakeLockRepo{locks: map[string]*store.Lock{token: {
			Token: token, ResourcePath: "/dav/calendars/5", UserID: owner.ID, LockScope: "exclusive", LockType: "write",
			Depth: "infinity", ExpiresAt: time.Now().Add(time.Hour),
		}}}
		h := NewDavServer(Options{Store: &store.Store{
			Calendars:           &fakeCalendarRepo{calendars: map[int64]*store.Calendar{5: {ID: 5, UserID: owner.ID, Name: "Owner"}}},
			Events:              events,
			Locks:               locks,
			ACLEntries:          &fakeACLRepo{},
			CalendarCollections: &lockCheckingCalendarCollectionBackend{locks: locks},
		}})
		var ifHeader strings.Builder
		for i := range n {
			fmt.Fprintf(&ifHeader, "(<opaquelocktoken:decoy-%d>) ", i)
		}
		ifHeader.WriteString("(<" + token + ">)")
		header := ifHeader.String()
		return func() {
			req := httptest.NewRequest(http.MethodDelete, "/dav/calendars/5/", nil)
			req.Header.Set("If", header)
			rr := httptest.NewRecorder()
			h.ServeHTTP(rr, req.WithContext(auth.WithUser(req.Context(), owner)))
			if rr.Code != http.StatusNoContent {
				t.Fatalf("DELETE of %d members with %d tokens = %d: %s", n, n+1, rr.Code, rr.Body.String())
			}
		}
	})
}

// RFC 4918 §9.10.2 refreshes a lock by a LOCK without a body sent to any
// resource within the lock's scope: a member of a collection locked to depth
// infinity, but not one of a collection locked to depth 0. The response names
// the lock's own root. The If header submits the lock's token whatever
// resource a list's tag names (§10.4.1), so a client may tag it with the lock
// root, as it does for a write below a collection lock. A refresh whose If
// header submits no token is malformed. A token the server cannot look up is a
// server error, not a token out of scope, and a lock that UNLOCK or its expiry
// removes before the refresh lands names no lock, as §9.10.6 has it.
func TestLockRefreshWithoutABodyReachesTheLocksWholeScope(t *testing.T) {
	const token = "opaquelocktoken:collection"
	owner := &store.User{ID: 1}
	for _, d := range objectWriteDomains {
		for _, tt := range []struct {
			name        string
			depth       string
			ifHeader    string
			lookupErr   error
			refreshErr  error
			want        int
			wantRefresh bool
		}{
			{name: "member of a collection locked to depth infinity", depth: "infinity", want: http.StatusOK, wantRefresh: true},
			{name: "member of a collection locked to depth 0", depth: "0", want: http.StatusPreconditionFailed},
			{name: "token tagged with the lock root", depth: "infinity", ifHeader: "<" + d.collectionPath + "/> (<" + token + ">)", want: http.StatusOK, wantRefresh: true},
			{name: "token tagged with the lock root by absolute URL", depth: "infinity", ifHeader: "<http://example.com" + d.collectionPath + "/> (<" + token + ">)", want: http.StatusOK, wantRefresh: true},
			{name: "token tagged with another resource", depth: "infinity", ifHeader: "<" + d.collectionPath + "/other" + d.extension + "> (<" + token + ">)", want: http.StatusOK, wantRefresh: true},
			{name: "no If header", depth: "infinity", ifHeader: "-", want: http.StatusBadRequest},
			{name: "token only negated", depth: "infinity", ifHeader: "(Not <" + token + ">)", want: http.StatusBadRequest},
			{name: "lookup failure", depth: "infinity", lookupErr: errors.New("lock store unavailable"), want: http.StatusInternalServerError},
			{name: "lock removed before the refresh lands", depth: "infinity", refreshErr: store.ErrNotFound, want: http.StatusPreconditionFailed},
		} {
			t.Run(d.name+"/"+tt.name, func(t *testing.T) {
				repo := &fakeLockRepo{getByTokenErr: tt.lookupErr, refreshErr: tt.refreshErr, locks: map[string]*store.Lock{token: {
					Token: token, ResourcePath: d.collectionPath, UserID: owner.ID, LockScope: "exclusive", LockType: "write",
					Depth: tt.depth, TimeoutSeconds: 60, ExpiresAt: time.Now().Add(time.Minute),
				}}}
				req := httptest.NewRequest("LOCK", d.collectionPath+"/event"+d.extension, nil)
				switch tt.ifHeader {
				case "":
					req.Header.Set("If", "(<"+token+">)")
				case "-":
				default:
					req.Header.Set("If", tt.ifHeader)
				}
				req.Header.Set("Timeout", "Second-3600")
				rr := httptest.NewRecorder()
				objectWriteServer(repo, nil).ServeHTTP(rr, req.WithContext(auth.WithUser(req.Context(), owner)))
				if rr.Code != tt.want {
					t.Fatalf("LOCK refresh = %d, want %d: %s", rr.Code, tt.want, rr.Body.String())
				}
				switch tt.want {
				case http.StatusPreconditionFailed:
					assertErrorConditions(t, rr, http.StatusPreconditionFailed, davQN("lock-token-matches-request-uri"))
				case http.StatusBadRequest:
					if !strings.Contains(rr.Body.String(), "If header") {
						t.Errorf("refusal does not say the If header names no lock token: %s", rr.Body.String())
					}
				}
				held := repo.locks[token]
				if refreshed := held.TimeoutSeconds == 3600; refreshed != tt.wantRefresh {
					t.Errorf("lock refreshed = %v, want %v", refreshed, tt.wantRefresh)
				}
				if tt.wantRefresh && !strings.Contains(rr.Body.String(), "<d:lockroot><d:href>"+d.collectionPath+"</d:href></d:lockroot>") {
					t.Errorf("refresh does not name the lock's root %s: %s", d.collectionPath, rr.Body.String())
				}
			})
		}
	}
}

// A LOCK with a body that cannot read the resource's locks, to find whether it
// refreshes the requester's own one, fails rather than asking for a new lock.
// One whose refresh finds the lock already removed answers as for a token that
// names no lock (RFC 4918 §9.10.6).
func TestLockWithABodyFailsWhenItCannotReadOrRefreshItsLock(t *testing.T) {
	const token = "opaquelocktoken:held"
	owner := &store.User{ID: 1}
	for _, d := range objectWriteDomains {
		for _, tt := range []struct {
			name       string
			listErr    error
			refreshErr error
			want       int
		}{
			{name: "locks cannot be read", listErr: errors.New("lock store unavailable"), want: http.StatusInternalServerError},
			{name: "lock removed before the refresh lands", refreshErr: store.ErrNotFound, want: http.StatusPreconditionFailed},
		} {
			t.Run(d.name+"/"+tt.name, func(t *testing.T) {
				repo := &fakeLockRepo{listByResourcesErr: tt.listErr, refreshErr: tt.refreshErr, locks: map[string]*store.Lock{token: {
					Token: token, ResourcePath: d.collectionPath + "/event", UserID: owner.ID, LockScope: "exclusive", LockType: "write",
					Depth: "0", TimeoutSeconds: 60, ExpiresAt: time.Now().Add(time.Minute),
				}}}
				req := httptest.NewRequest("LOCK", d.collectionPath+"/event"+d.extension, strings.NewReader(exclusiveWriteLockBody))
				req.Header.Set("If", "(<"+token+">)")
				req.Header.Set("Timeout", "Second-3600")
				rr := httptest.NewRecorder()
				objectWriteServer(repo, nil).ServeHTTP(rr, req.WithContext(auth.WithUser(req.Context(), owner)))
				if rr.Code != tt.want {
					t.Fatalf("LOCK = %d, want %d: %s", rr.Code, tt.want, rr.Body.String())
				}
				if tt.want == http.StatusPreconditionFailed {
					assertErrorConditions(t, rr, http.StatusPreconditionFailed, davQN("lock-token-matches-request-uri"))
				}
				if len(repo.locks) != 1 || rr.Header().Get("Lock-Token") != "" {
					t.Errorf("LOCK left %d locks and Lock-Token %q, want the held lock alone and none", len(repo.locks), rr.Header().Get("Lock-Token"))
				}
			})
		}
	}
}

// RFC 4918 §9.11 has UNLOCK's Request-URI identify a resource within the
// scope of the lock: a member of a collection locked to depth infinity, but
// not one of a collection locked to depth 0, nor a sibling of a locked
// member. A Request-URI outside the scope is refused with 409 (§9.11.1) and
// leaves the lock in place.
func TestUnlockReachesTheLocksWholeScope(t *testing.T) {
	const token = "opaquelocktoken:held"
	owner := &store.User{ID: 1}
	for _, d := range objectWriteDomains {
		for _, tt := range []struct {
			name, lockPath, depth, requestURI string
			wantUnlocked                      bool
		}{
			{name: "member of a collection locked to depth infinity", lockPath: d.collectionPath, depth: "infinity", requestURI: d.collectionPath + "/event" + d.extension, wantUnlocked: true},
			{name: "absent member of a collection locked to depth infinity", lockPath: d.collectionPath, depth: "infinity", requestURI: d.collectionPath + "/missing" + d.extension, wantUnlocked: true},
			{name: "the lock root", lockPath: d.collectionPath, depth: "infinity", requestURI: d.collectionPath + "/", wantUnlocked: true},
			{name: "member of a collection locked to depth 0", lockPath: d.collectionPath, depth: "0", requestURI: d.collectionPath + "/event" + d.extension},
			{name: "sibling of a locked member", lockPath: d.collectionPath + "/event", depth: "0", requestURI: d.collectionPath + "/missing" + d.extension},
			{name: "collection of a locked member", lockPath: d.collectionPath + "/event", depth: "infinity", requestURI: d.collectionPath + "/"},
		} {
			t.Run(d.name+"/"+tt.name, func(t *testing.T) {
				repo := &fakeLockRepo{locks: map[string]*store.Lock{token: {
					Token: token, ResourcePath: tt.lockPath, UserID: owner.ID, LockScope: "exclusive", LockType: "write",
					Depth: tt.depth, TimeoutSeconds: 60, ExpiresAt: time.Now().Add(time.Minute),
				}}}
				req := httptest.NewRequest("UNLOCK", tt.requestURI, nil)
				req.Header.Set("Lock-Token", "<"+token+">")
				rr := httptest.NewRecorder()
				objectWriteServer(repo, nil).ServeHTTP(rr, req.WithContext(auth.WithUser(req.Context(), owner)))
				if tt.wantUnlocked {
					if rr.Code != http.StatusNoContent || len(repo.locks) != 0 {
						t.Fatalf("UNLOCK = %d leaving %d locks, want 204 and none: %s", rr.Code, len(repo.locks), rr.Body.String())
					}
					return
				}
				assertErrorConditions(t, rr, http.StatusConflict, davQN("lock-token-matches-request-uri"))
				if len(repo.locks) != 1 {
					t.Fatal("UNLOCK outside the lock's scope removed it")
				}
			})
		}
	}
}

// Another principal's lock is removed by DAV:unlock on the resource the lock
// is on (RFC 3744 §3.5), so UNLOCK sent to a member within a collection's lock
// needs the privilege on the collection, not on the member it was sent to.
func TestUnlockOfAnotherPrincipalsLockNeedsUnlockOnTheLockRoot(t *testing.T) {
	const token = "opaquelocktoken:owner"
	delegate := &store.User{ID: 2}
	for _, d := range objectWriteDomains {
		for _, tt := range []struct {
			name      string
			grantedAt string
			want      int
		}{
			{name: "unlock granted on the member", grantedAt: d.collectionPath + "/event", want: http.StatusForbidden},
			{name: "unlock granted on the lock root", grantedAt: d.collectionPath, want: http.StatusNoContent},
		} {
			t.Run(d.name+"/"+tt.name, func(t *testing.T) {
				repo := &fakeLockRepo{locks: map[string]*store.Lock{token: {
					Token: token, ResourcePath: d.collectionPath, UserID: 1, LockScope: "exclusive", LockType: "write",
					Depth: "infinity", TimeoutSeconds: 60, ExpiresAt: time.Now().Add(time.Minute),
				}}}
				entries := []store.ACLEntry{
					{ResourcePath: d.collectionPath, PrincipalHref: "/dav/principals/2/", IsGrant: true, Privilege: "read"},
					{ResourcePath: tt.grantedAt, PrincipalHref: "/dav/principals/2/", IsGrant: true, Privilege: "unlock"},
				}
				req := httptest.NewRequest("UNLOCK", d.collectionPath+"/event"+d.extension, nil)
				req.Header.Set("Lock-Token", "<"+token+">")
				rr := httptest.NewRecorder()
				objectWriteServer(repo, entries).ServeHTTP(rr, req.WithContext(auth.WithUser(req.Context(), delegate)))
				if tt.want == http.StatusForbidden {
					assertNeedPrivileges(t, rr, d.collectionPath, davQN("unlock"))
					if len(repo.locks) != 1 {
						t.Fatal("refused UNLOCK removed the lock")
					}
					return
				}
				if rr.Code != tt.want || len(repo.locks) != 0 {
					t.Fatalf("UNLOCK = %d leaving %d locks, want %d and none: %s", rr.Code, len(repo.locks), tt.want, rr.Body.String())
				}
			})
		}
	}
}

// A COPY or MOVE checks its source, its source's locks and its conditional
// headers before the destination's name, in address books as in calendars.
func TestTransferChecksItsSourceBeforeTheDestinationName(t *testing.T) {
	delegate := &store.User{ID: 2}
	sources := map[string]string{"calendar": "/dav/calendars/7/mine.ics", "address book": "/dav/addressbooks/8/mine.vcf"}
	long := strings.Repeat("n", store.MaxIdentifierOctets+1)
	for _, d := range objectWriteDomains {
		for _, tt := range []struct {
			method, name string
			ifMatch      string
			sourceLocked bool
			want         int
		}{
			{method: "COPY", name: "failed If-Match", ifMatch: `"stale"`, want: http.StatusPreconditionFailed},
			{method: "MOVE", name: "failed If-Match", ifMatch: `"stale"`, want: http.StatusPreconditionFailed},
			{method: "MOVE", name: "locked source", sourceLocked: true, want: http.StatusLocked},
		} {
			t.Run(d.name+"/"+tt.method+"/"+tt.name, func(t *testing.T) {
				h := transferServer([]store.ACLEntry{{ResourcePath: d.collectionPath, PrincipalHref: "/dav/principals/2/", IsGrant: true, Privilege: "all"}})
				if tt.sourceLocked {
					h.store.Locks.(*fakeLockRepo).locks = map[string]*store.Lock{"opaquelocktoken:source": {
						Token: "opaquelocktoken:source", ResourcePath: strings.TrimSuffix(sources[d.name], d.extension), UserID: 1,
						LockScope: "exclusive", LockType: "write", Depth: "0", ExpiresAt: time.Now().Add(time.Hour),
					}}
				}
				req := httptest.NewRequest(tt.method, sources[d.name], nil)
				req.Header.Set("Destination", d.collectionPath+"/"+long+d.extension)
				if tt.ifMatch != "" {
					req.Header.Set("If-Match", tt.ifMatch)
				}
				rr := httptest.NewRecorder()
				h.ServeHTTP(rr, req.WithContext(auth.WithUser(req.Context(), delegate)))
				if rr.Code != tt.want {
					t.Fatalf("%s = %d, want %d: %s", tt.method, rr.Code, tt.want, rr.Body.String())
				}
			})
		}
	}
}

// A path names its stored object by the resource name left once the last
// extension is dropped, so every such spelling is the same existing resource,
// and RFC 3744 Appendix B makes locking it need DAV:write-content on it.
func TestLockOfAnExistingObjectNeedsWriteContentWhateverItsExtension(t *testing.T) {
	delegate := &store.User{ID: 2}
	for _, d := range objectWriteDomains {
		for _, suffix := range []string{"", ".ics", ".ICS", ".vcf", ".VCF", ".vcs", ".txt", "."} {
			requestURI := d.collectionPath + "/event" + suffix
			t.Run(requestURI, func(t *testing.T) {
				locks := &fakeLockRepo{locks: map[string]*store.Lock{}}
				h := objectWriteServer(locks, []store.ACLEntry{
					{ResourcePath: d.collectionPath, PrincipalHref: "/dav/principals/2/", IsGrant: true, Privilege: "bind"},
				})
				rr := httptest.NewRecorder()

				h.ServeHTTP(rr, objectWriteRequest(d, "LOCK", requestURI, delegate))

				assertNeedPrivileges(t, rr, requestURI, davQN("write-content"))
				if len(locks.locks) != 0 {
					t.Fatalf("refused LOCK stored %d locks", len(locks.locks))
				}
			})
		}
	}
}

// A refused PUT is refused before its body is read or checked, so neither the
// body's size nor its content can make a refusal differ by whether the object
// exists; a principal that may write meets the lock before the body.
func TestPutIsAuthorizedAndLockCheckedBeforeItsBody(t *testing.T) {
	const ownerToken = "opaquelocktoken:owner"
	stranger := &store.User{ID: 2}
	editor := &store.User{ID: 3}
	for _, d := range objectWriteDomains {
		editorGrants := []store.ACLEntry{
			{ResourcePath: d.collectionPath, PrincipalHref: "/dav/principals/3/", IsGrant: true, Privilege: "read"},
			{ResourcePath: d.collectionPath, PrincipalHref: "/dav/principals/3/", IsGrant: true, Privilege: "write-content"},
			{ResourcePath: d.collectionPath, PrincipalHref: "/dav/principals/3/", IsGrant: true, Privilege: "bind"},
		}
		for _, name := range []string{"missing", "event"} {
			requestURI := d.collectionPath + "/" + name + d.extension
			for _, body := range []struct {
				name      string
				configure func(*http.Request)
			}{
				{name: "oversized", configure: func(r *http.Request) { r.ContentLength = maxDAVBodyBytes + 1 }},
				{name: "unsupported media type", configure: func(r *http.Request) { r.Header.Set("Content-Type", "application/x-unsupported") }},
				{name: "invalid body", configure: func(r *http.Request) {
					r.Body = io.NopCloser(strings.NewReader("not an object"))
					r.ContentLength = int64(len("not an object"))
				}},
			} {
				t.Run(d.name+"/"+name+"/"+body.name+"/stranger", func(t *testing.T) {
					req := objectWriteRequest(d, http.MethodPut, requestURI, stranger)
					body.configure(req)
					rr := httptest.NewRecorder()

					objectWriteServer(&fakeLockRepo{}, nil).ServeHTTP(rr, req)

					assertNeedPrivileges(t, rr, d.collectionPath, davQN("bind"))
				})
				if body.name == "invalid body" {
					continue
				}
				t.Run(d.name+"/"+name+"/"+body.name+"/authorized on a locked object", func(t *testing.T) {
					locks := &fakeLockRepo{locks: map[string]*store.Lock{ownerToken: {
						Token: ownerToken, ResourcePath: d.collectionPath + "/" + name, UserID: 1,
						LockScope: "exclusive", LockType: "write", Depth: "0", ExpiresAt: time.Now().Add(time.Hour),
					}}}
					req := objectWriteRequest(d, http.MethodPut, requestURI, editor)
					body.configure(req)
					rr := httptest.NewRecorder()

					objectWriteServer(locks, editorGrants).ServeHTTP(rr, req)

					if rr.Code != http.StatusLocked {
						t.Fatalf("status = %d, want 423: %s", rr.Code, rr.Body.String())
					}
				})
			}
		}
	}
}
