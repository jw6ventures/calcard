package dav

import (
	"context"
	"encoding/xml"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/jw6ventures/calcard/internal/auth"
	"github.com/jw6ventures/calcard/internal/store"
)

// The RFC 3744 ACL method and the live ACL properties it writes: the protected
// owner ACE, ordered ACEs, inheritance, and the preconditions that must leave
// the stored ACL untouched.

func TestRFC3744RequiredACLPropertiesAndProtectedOwnerACE(t *testing.T) {
	calendar := store.Calendar{ID: 5, UserID: 1, Name: "Work"}
	h := NewDavServer(Options{Store: &store.Store{
		Calendars: &fakeCalendarRepo{
			accessible: []store.CalendarAccess{{Calendar: calendar, Editor: true}},
			calendars:  map[int64]*store.Calendar{5: &calendar},
		},
		Events:     &fakeEventRepo{events: map[string]*store.Event{}},
		ACLEntries: &fakeACLRepo{},
	}})
	body := `<d:propfind xmlns:d="DAV:"><d:prop>
		<d:owner/><d:group/><d:supported-privilege-set/><d:current-user-privilege-set/>
		<d:acl/><d:acl-restrictions/><d:inherited-acl-set/><d:principal-collection-set/>
	</d:prop></d:propfind>`
	req := httptest.NewRequest("PROPFIND", "/dav/calendars/5/", strings.NewReader(body))
	req.Header.Set("Depth", "0")
	req = req.WithContext(auth.WithUser(req.Context(), &store.User{ID: 1}))
	rr := httptest.NewRecorder()

	h.ServeHTTP(rr, req)

	if rr.Code != http.StatusMultiStatus {
		t.Fatalf("PROPFIND status = %d, want 207: %s", rr.Code, rr.Body.String())
	}
	resp := decodeMultistatus(t, rr).responseForHref(t, "/dav/calendars/5/")
	resp.assertPropHrefs(t, davQN("owner"), "/dav/principals/1/")
	resp.assertPropHrefs(t, davQN("principal-collection-set"), "/dav/principals/")
	// DAV:group is defined and empty rather than absent: CalCard has no groups,
	// and §5.2 makes the property present with no href in that case.
	resp.assertPropChildNames(t, davQN("group"))
	resp.assertPropChildNames(t, davQN("inherited-acl-set"))

	// The owner ACE is protected and grants DAV:all, and DAV:acl-restrictions
	// advertises that inverted ACEs are refused.
	owner := resp.aces(t)[0]
	if got := qnList(acePrincipals(t, owner)); got != qnList([]xml.Name{davQN("href")}) {
		t.Errorf("owner ACE principal = %s, want DAV:href", got)
	}
	assertSoleHref(t, owner.child(t, davQN("principal")), "/dav/principals/1/")
	if got := qnList(acePrivileges(t, owner, davQN("grant"))); got != qnList([]xml.Name{davQN("all")}) {
		t.Errorf("owner ACE grants %s, want DAV:all", got)
	}
	// §5.5.3 marks an ACE the server will not let a client remove DAV:protected.
	owner.child(t, davQN("protected"))
	restrictions := resp.assertPropStatus(t, davQN("acl-restrictions"), http.StatusOK)
	if got := qnList(restrictions.childNames()); got != qnList([]xml.Name{davQN("no-invert")}) {
		t.Errorf("DAV:acl-restrictions = %s, want DAV:no-invert", got)
	}
	// §5.3 requires every advertised privilege to carry a human-readable
	// DAV:description, which DAV:all heads.
	supported := resp.assertPropStatus(t, davQN("supported-privilege-set"), http.StatusOK)
	all := assertSoleChild(t, supported, davQN("supported-privilege"))
	if got := all.child(t, davQN("privilege")); qnList(got.childNames()) != qnList([]xml.Name{davQN("all")}) {
		t.Errorf("supported-privilege-set root privilege = %s, want DAV:all", qnList(got.childNames()))
	}
	description := all.child(t, davQN("description"))
	if description.Value() != "All privileges" {
		t.Errorf("DAV:all description = %q, want %q", description.Value(), "All privileges")
	}
	if got := xmlLangOf(description); got != "en" {
		t.Errorf("DAV:all description xml:lang = %q, want %q", got, "en")
	}
}

func TestVirtualDAVRootACLsAreProtectedAndDoNotLeakBetweenUsers(t *testing.T) {
	aclRepo := &fakeACLRepo{}
	h := NewDavServer(Options{Store: &store.Store{ACLEntries: aclRepo}})
	body := `<d:acl xmlns:d="DAV:"><d:ace><d:principal><d:all/></d:principal><d:grant><d:privilege><d:read/></d:privilege></d:grant></d:ace></d:acl>`

	writeReq := httptest.NewRequest("ACL", "/dav/calendars/", strings.NewReader(body))
	writeReq = writeReq.WithContext(auth.WithUser(writeReq.Context(), &store.User{ID: 1}))
	writeRR := httptest.NewRecorder()
	h.ServeHTTP(writeRR, writeReq)

	assertErrorConditions(t, writeRR, http.StatusForbidden, davQN("no-protected-ace-conflict"))
	if len(aclRepo.entries) != 0 {
		t.Fatalf("protected root ACL write persisted entries: %#v", aclRepo.entries)
	}

	aclRepo.entries = append(aclRepo.entries, store.ACLEntry{
		ResourcePath: "/dav/calendars", PrincipalHref: "/dav/principals/1/", IsGrant: true, Privilege: "read",
	})
	readBody := `<d:propfind xmlns:d="DAV:"><d:prop><d:acl/></d:prop></d:propfind>`
	readReq := httptest.NewRequest("PROPFIND", "/dav/calendars/", strings.NewReader(readBody))
	readReq.Header.Set("Depth", "0")
	readReq = readReq.WithContext(auth.WithUser(readReq.Context(), &store.User{ID: 2}))
	readRR := httptest.NewRecorder()
	h.ServeHTTP(readRR, readReq)

	if readRR.Code != http.StatusMultiStatus {
		t.Fatalf("PROPFIND status = %d, want 207: %s", readRR.Code, readRR.Body.String())
	}
	// Each user sees only their own protected owner ACE on the virtual root.
	rootACEs := decodeMultistatus(t, readRR).responseForHref(t, "/dav/calendars/").aces(t)
	if len(rootACEs) != 1 {
		t.Fatalf("root DAV:acl carries %d ACEs, want only the protected owner ACE", len(rootACEs))
	}
	assertSoleHref(t, rootACEs[0].child(t, davQN("principal")), "/dav/principals/2/")
}

func TestObjectACLPropertiesExposeInheritedCollectionACEs(t *testing.T) {
	owner := store.User{ID: 1}
	calendar := store.Calendar{ID: 5, UserID: owner.ID, Name: "Work"}
	h := NewDavServer(Options{Store: &store.Store{
		Calendars: &fakeCalendarRepo{
			accessible: []store.CalendarAccess{{Calendar: calendar, Editor: true}},
			calendars:  map[int64]*store.Calendar{calendar.ID: &calendar},
		},
		Events: &fakeEventRepo{events: map[string]*store.Event{
			"5:event": {CalendarID: 5, UID: "event", ResourceName: "event", RawICAL: buildCalendarObject(buildVEvent("event")), ETag: "etag"},
		}},
		ACLEntries: &fakeACLRepo{entries: []store.ACLEntry{
			{ResourcePath: "/dav/calendars/5/event", PrincipalHref: "/dav/principals/2/", IsGrant: false, Privilege: "read", Position: 0},
			{ResourcePath: "/dav/calendars/5", PrincipalHref: "/dav/principals/2/", IsGrant: true, Privilege: "read", Position: 0},
		}},
	}})
	body := `<d:propfind xmlns:d="DAV:"><d:prop><d:acl/><d:inherited-acl-set/></d:prop></d:propfind>`
	req := httptest.NewRequest("PROPFIND", "/dav/calendars/5/event.ics", strings.NewReader(body))
	req.Header.Set("Depth", "0")
	req = req.WithContext(auth.WithUser(req.Context(), &owner))
	rr := httptest.NewRecorder()

	h.ServeHTTP(rr, req)

	if rr.Code != http.StatusMultiStatus {
		t.Fatalf("PROPFIND status = %d: %s", rr.Code, rr.Body.String())
	}
	resp := decodeMultistatus(t, rr).responseForHref(t, "/dav/calendars/5/event.ics")
	resp.assertPropHrefs(t, davQN("inherited-acl-set"), "/dav/calendars/5/")

	// The object's own deny comes first, then the collection ACE marked
	// DAV:inherited and naming the collection it came from.
	aces := resp.aces(t)
	if len(aces) < 2 {
		t.Fatalf("DAV:acl carries %d ACEs, want the object deny and the inherited grant", len(aces))
	}
	var objectDeny, inheritedGrant *davElement
	for i := range aces {
		switch {
		case hasACEChild(aces[i], davQN("deny")) && !hasACEChild(aces[i], davQN("inherited")):
			objectDeny = &aces[i]
		case hasACEChild(aces[i], davQN("inherited")):
			inheritedGrant = &aces[i]
		}
	}
	if objectDeny == nil || inheritedGrant == nil {
		t.Fatalf("DAV:acl = %v, want an object deny and an inherited grant", aces)
	}
	if got := qnList(acePrivileges(t, *objectDeny, davQN("deny"))); got != qnList([]xml.Name{davQN("read")}) {
		t.Errorf("object ACE denies %s, want DAV:read", got)
	}
	if got := qnList(acePrivileges(t, *inheritedGrant, davQN("grant"))); got != qnList([]xml.Name{davQN("read")}) {
		t.Errorf("inherited ACE grants %s, want DAV:read", got)
	}
	assertSoleHref(t, inheritedGrant.child(t, davQN("inherited")), "/dav/calendars/5/")
}

func TestObjectInheritedACLSetIsEmptyWithoutInheritedACEs(t *testing.T) {
	owner := store.User{ID: 1}
	calendar := store.Calendar{ID: 5, UserID: owner.ID, Name: "Work"}
	h := NewDavServer(Options{Store: &store.Store{
		Calendars: &fakeCalendarRepo{
			accessible: []store.CalendarAccess{{Calendar: calendar, Editor: true}},
			calendars:  map[int64]*store.Calendar{calendar.ID: &calendar},
		},
		Events: &fakeEventRepo{events: map[string]*store.Event{
			"5:event": {CalendarID: 5, UID: "event", ResourceName: "event", RawICAL: buildCalendarObject(buildVEvent("event")), ETag: "etag"},
		}},
		ACLEntries: &fakeACLRepo{},
	}})
	body := `<d:propfind xmlns:d="DAV:"><d:prop><d:inherited-acl-set/></d:prop></d:propfind>`
	req := httptest.NewRequest("PROPFIND", "/dav/calendars/5/event.ics", strings.NewReader(body))
	req.Header.Set("Depth", "0")
	req = req.WithContext(auth.WithUser(req.Context(), &owner))
	rr := httptest.NewRecorder()

	h.ServeHTTP(rr, req)

	if rr.Code != http.StatusMultiStatus {
		t.Fatalf("PROPFIND status = %d: %s", rr.Code, rr.Body.String())
	}
	decodeMultistatus(t, rr).
		responseForHref(t, "/dav/calendars/5/event.ics").
		assertPropChildNames(t, davQN("inherited-acl-set"))
}

func TestRFC3744ACLPropertiesRequireTheirReadPrivileges(t *testing.T) {
	calendar := store.Calendar{ID: 5, UserID: 1, Name: "Work"}
	delegate := &store.User{ID: 2, PrimaryEmail: "delegate@example.com"}
	h := NewDavServer(Options{Store: &store.Store{
		Calendars: &fakeCalendarRepo{
			accessible: []store.CalendarAccess{{
				Calendar: calendar, Shared: true, PrivilegesResolved: true,
				Privileges: store.CalendarPrivileges{ReadFreeBusy: true},
			}},
			calendars: map[int64]*store.Calendar{5: &calendar},
		},
		Events: &fakeEventRepo{events: map[string]*store.Event{}},
		ACLEntries: &fakeACLRepo{entries: []store.ACLEntry{
			{ResourcePath: "/dav/calendars/5", PrincipalHref: "/dav/principals/2/", IsGrant: true, Privilege: "read"},
			{ResourcePath: "/dav/calendars/5", PrincipalHref: "/dav/principals/2/", IsGrant: true, Privilege: "read-free-busy"},
		}},
	}})
	body := `<d:propfind xmlns:d="DAV:"><d:prop><d:acl/><d:current-user-privilege-set/><d:supported-privilege-set/></d:prop></d:propfind>`
	req := httptest.NewRequest("PROPFIND", "/dav/calendars/5/", strings.NewReader(body))
	req.Header.Set("Depth", "0")
	req = req.WithContext(auth.WithUser(req.Context(), delegate))
	rr := httptest.NewRecorder()

	h.ServeHTTP(rr, req)

	response := decodeMultistatus(t, rr).responseForHref(t, "/dav/calendars/5/")
	response.assertPropStatus(t, davQN("acl"), http.StatusForbidden)
	response.assertPropStatus(t, davQN("current-user-privilege-set"), http.StatusForbidden)
	response.assertPropStatus(t, davQN("supported-privilege-set"), http.StatusOK)
}

func TestACLStoresReadFreeBusyAndACEOrder(t *testing.T) {
	calendar := store.Calendar{ID: 5, UserID: 1, Name: "Work"}
	aclRepo := &fakeACLRepo{}
	h := NewDavServer(Options{Store: &store.Store{
		Calendars: &fakeCalendarRepo{
			accessible: []store.CalendarAccess{{Calendar: calendar, Editor: true}},
			calendars:  map[int64]*store.Calendar{5: &calendar},
		},
		ACLEntries: aclRepo,
	}})
	body := `<d:acl xmlns:d="DAV:" xmlns:cal="urn:ietf:params:xml:ns:caldav">
		<d:ace><d:principal><d:href>/dav/principals/2/</d:href></d:principal><d:grant><d:privilege><cal:read-free-busy/></d:privilege></d:grant></d:ace>
		<d:ace><d:principal><d:href>/dav/principals/3/</d:href></d:principal><d:deny><d:privilege><d:unlock/></d:privilege></d:deny></d:ace>
	</d:acl>`
	req := httptest.NewRequest("ACL", "/dav/calendars/5/", strings.NewReader(body))
	req = req.WithContext(auth.WithUser(req.Context(), &store.User{ID: 1}))
	rr := httptest.NewRecorder()

	h.ServeHTTP(rr, req)

	if rr.Code != http.StatusOK {
		t.Fatalf("ACL status = %d, want 200: %s", rr.Code, rr.Body.String())
	}
	entries, err := aclRepo.ListByResource(context.Background(), "/dav/calendars/5")
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 2 || entries[0].Privilege != "read-free-busy" || entries[0].Position != 0 || entries[1].Privilege != "unlock" || entries[1].Position != 1 {
		t.Fatalf("stored ACL entries = %#v", entries)
	}
}

func TestACLMethodReturnsRFC3744PreconditionsWithoutMutatingACL(t *testing.T) {
	owner := store.User{ID: 1, PrimaryEmail: "owner@example.com"}
	delegate := store.User{ID: 2, PrimaryEmail: "delegate@example.com"}
	calendar := store.Calendar{ID: 5, UserID: owner.ID, Name: "Work"}

	tests := []struct {
		name      string
		body      string
		condition string
	}{
		{
			name: "unsupported privilege",
			body: `<d:acl xmlns:d="DAV:"><d:ace>
				<d:principal><d:href>/dav/principals/2/</d:href></d:principal>
				<d:grant><d:privilege><d:administer/></d:privilege></d:grant>
			</d:ace></d:acl>`,
			condition: "not-supported-privilege",
		},
		{
			name: "unrecognized principal URL",
			body: `<d:acl xmlns:d="DAV:"><d:ace>
				<d:principal><d:href>/dav/principals/999/</d:href></d:principal>
				<d:grant><d:privilege><d:read/></d:privilege></d:grant>
			</d:ace></d:acl>`,
			condition: "recognized-principal",
		},
		{
			name: "cross-authority principal URL",
			body: `<d:acl xmlns:d="DAV:"><d:ace>
				<d:principal><d:href>https://other.example/dav/principals/2/</d:href></d:principal>
				<d:grant><d:privilege><d:read/></d:privilege></d:grant>
			</d:ace></d:acl>`,
			condition: "recognized-principal",
		},
		{
			name: "inverted principal prohibited by advertised restriction",
			body: `<d:acl xmlns:d="DAV:"><d:ace>
				<d:invert><d:principal><d:href>/dav/principals/2/</d:href></d:principal></d:invert>
				<d:grant><d:privilege><d:read/></d:privilege></d:grant>
			</d:ace></d:acl>`,
			condition: "no-invert",
		},
		{
			name: "deny conflicts with protected owner ACE",
			body: `<d:acl xmlns:d="DAV:"><d:ace>
				<d:principal><d:property><d:owner/></d:property></d:principal>
				<d:deny><d:privilege><d:write/></d:privilege></d:deny>
			</d:ace></d:acl>`,
			condition: "no-protected-ace-conflict",
		},
		{
			name: "submitted protected ACE",
			body: `<d:acl xmlns:d="DAV:"><d:ace>
				<d:principal><d:href>/dav/principals/2/</d:href></d:principal>
				<d:grant><d:privilege><d:read/></d:privilege></d:grant><d:protected/>
			</d:ace></d:acl>`,
			condition: "no-protected-ace-conflict",
		},
		{
			name: "submitted inherited ACE",
			body: `<d:acl xmlns:d="DAV:"><d:ace>
				<d:principal><d:href>/dav/principals/2/</d:href></d:principal>
				<d:grant><d:privilege><d:read/></d:privilege></d:grant>
				<d:inherited><d:href>/dav/calendars/</d:href></d:inherited>
			</d:ace></d:acl>`,
			condition: "no-inherited-ace-conflict",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			original := store.ACLEntry{
				ResourcePath:  "/dav/calendars/5",
				PrincipalHref: "/dav/principals/2/",
				IsGrant:       true,
				Privilege:     "read-acl",
				Position:      0,
			}
			aclRepo := &fakeACLRepo{entries: []store.ACLEntry{original}}
			h := NewDavServer(Options{Store: &store.Store{
				Users: &aclReportUserRepo{users: map[int64]store.User{
					owner.ID:    owner,
					delegate.ID: delegate,
				}},
				Calendars: &fakeCalendarRepo{
					accessible: []store.CalendarAccess{{Calendar: calendar, Editor: true}},
					calendars:  map[int64]*store.Calendar{calendar.ID: &calendar},
				},
				ACLEntries: aclRepo,
			}})
			req := httptest.NewRequest("ACL", "/dav/calendars/5/", strings.NewReader(tc.body))
			req = req.WithContext(auth.WithUser(req.Context(), &owner))
			rr := httptest.NewRecorder()

			h.ServeHTTP(rr, req)

			assertErrorConditions(t, rr, http.StatusForbidden, davQN(tc.condition))
			entries, err := aclRepo.ListByResource(context.Background(), original.ResourcePath)
			if err != nil {
				t.Fatal(err)
			}
			if len(entries) != 1 || entries[0] != original {
				t.Fatalf("failed ACL request mutated entries: %#v", entries)
			}
		})
	}
}

func TestACLMethodRoundTripsUnauthenticatedAndPropertyPrincipals(t *testing.T) {
	owner := store.User{ID: 1, PrimaryEmail: "owner@example.com"}
	calendar := store.Calendar{ID: 5, UserID: owner.ID, Name: "Work"}
	aclRepo := &fakeACLRepo{}
	h := NewDavServer(Options{Store: &store.Store{
		Calendars: &fakeCalendarRepo{
			accessible: []store.CalendarAccess{{Calendar: calendar, Editor: true}},
			calendars:  map[int64]*store.Calendar{calendar.ID: &calendar},
		},
		Events:     &fakeEventRepo{events: map[string]*store.Event{}},
		ACLEntries: aclRepo,
	}})
	body := `<d:acl xmlns:d="DAV:">
		<d:ace><d:principal><d:unauthenticated/></d:principal><d:grant><d:privilege><d:read/></d:privilege></d:grant></d:ace>
		<d:ace><d:principal><d:property><d:owner/></d:property></d:principal><d:grant><d:privilege><d:read-acl/></d:privilege></d:grant></d:ace>
	</d:acl>`
	aclReq := httptest.NewRequest("ACL", "/dav/calendars/5/", strings.NewReader(body))
	aclReq = aclReq.WithContext(auth.WithUser(aclReq.Context(), &owner))
	aclRR := httptest.NewRecorder()
	h.ServeHTTP(aclRR, aclReq)
	if aclRR.Code != http.StatusOK {
		t.Fatalf("ACL status = %d, want 200: %s", aclRR.Code, aclRR.Body.String())
	}
	entries, err := aclRepo.ListByResource(context.Background(), "/dav/calendars/5")
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 2 || entries[0].PrincipalHref != "DAV:unauthenticated" || entries[1].PrincipalHref != "DAV:property:owner" {
		t.Fatalf("stored principal forms = %#v", entries)
	}

	propfindBody := `<d:propfind xmlns:d="DAV:"><d:prop><d:acl/></d:prop></d:propfind>`
	propfindReq := httptest.NewRequest("PROPFIND", "/dav/calendars/5/", strings.NewReader(propfindBody))
	propfindReq.Header.Set("Depth", "0")
	propfindReq = propfindReq.WithContext(auth.WithUser(propfindReq.Context(), &owner))
	propfindRR := httptest.NewRecorder()
	h.ServeHTTP(propfindRR, propfindReq)
	if propfindRR.Code != http.StatusMultiStatus {
		t.Fatalf("PROPFIND status = %d, want 207: %s", propfindRR.Code, propfindRR.Body.String())
	}
	resp := decodeMultistatus(t, propfindRR).responseForHref(t, "/dav/calendars/5/")
	aces := resp.aces(t)
	if len(aces) != 3 {
		t.Fatalf("DAV:acl carries %d ACEs, want the protected owner ACE plus the two submitted", len(aces))
	}
	// The protected owner ACE leads, then the two submitted principal forms in
	// the order they were sent.
	for i, want := range []xml.Name{davQN("unauthenticated"), davQN("property")} {
		if got := qnList(acePrincipals(t, aces[i+1])); got != qnList([]xml.Name{want}) {
			t.Errorf("ACE %d principal = %s, want %s", i+1, got, qnString(want))
		}
	}
	property := aces[2].child(t, davQN("principal")).child(t, davQN("property"))
	if got := qnList(property.childNames()); got != qnList([]xml.Name{davQN("owner")}) {
		t.Errorf("DAV:property children = %s, want DAV:owner", got)
	}
}

// hasACEChild reports whether an ACE carries the named child, which is how the
// DAV:grant/DAV:deny alternation and the optional DAV:inherited marker are told
// apart without depending on ACE ordering.
func hasACEChild(ace davElement, name xml.Name) bool {
	for _, child := range ace.Children {
		if child.Name == name {
			return true
		}
	}
	return false
}

// xmlLangOf returns an element's xml:lang, which lives in the XML namespace and
// so is not reachable through the unqualified attribute lookup.
func xmlLangOf(el davElement) string {
	for _, a := range el.Attr {
		if a.Name.Local == "lang" && a.Name.Space == xmlNamespaceURI {
			return a.Value
		}
	}
	return ""
}
