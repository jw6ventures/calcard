package dav

import (
	"context"
	"encoding/xml"
	"io"
	"net/http"
	"net/http/httptest"
	"path"
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

// A DELETE needs some privilege on the member and DAV:unbind on its collection
// (RFC 3744 Appendix B). A principal that may not read an existing member is
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

					// A LOCK naming a token in an untagged list asks to refresh
					// that lock, so a LOCK can only offer the collection's token
					// tagged with the collection's URL.
					var ifHeader string
					switch {
					case state.path == nil, method == "LOCK" && !(membershipLockOnly && !exists):
						continue
					case method == "LOCK":
						ifHeader = "<" + d.collectionPath + "> (<" + ownerToken + ">)"
					default:
						ifHeader = "(<" + ownerToken + ">)"
					}
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
