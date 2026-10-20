package dav

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	aclutil "github.com/jw6ventures/calcard/internal/acl"
	"github.com/jw6ventures/calcard/internal/auth"
	"github.com/jw6ventures/calcard/internal/store"
)

// privilegeSetPropfindResponse issues a Depth: 0 PROPFIND for
// current-user-privilege-set and returns the multistatus response element for
// href so a test can assert which propstat the property landed in.
func privilegeSetPropfindResponse(t *testing.T, h *DavServer, user *store.User, requestPath, href string) string {
	t.Helper()
	body := `<?xml version="1.0" encoding="utf-8"?>
<d:propfind xmlns:d="DAV:"><d:prop><d:current-user-privilege-set/></d:prop></d:propfind>`
	req := httptest.NewRequest("PROPFIND", requestPath, strings.NewReader(body))
	req.Header.Set("Depth", "0")
	req = req.WithContext(auth.WithUser(req.Context(), user))
	rr := httptest.NewRecorder()

	h.Propfind(rr, req)

	if rr.Code != http.StatusMultiStatus {
		t.Fatalf("PROPFIND %s: expected 207, got %d: %s", requestPath, rr.Code, rr.Body.String())
	}
	return davResponseForHref(t, rr.Body.String(), href)
}

// privilegeKindsHandler builds a server with one owned calendar and one owned
// address book, each holding a single object, plus the given grants to a
// delegate principal on both collections.
func privilegeKindsHandler(delegateGrants ...string) *DavServer {
	calRepo := &fakeCalendarRepo{
		calendars: map[int64]*store.Calendar{
			5: {ID: 5, UserID: 1, Name: "Work"},
		},
	}
	eventRepo := &fakeEventRepo{
		events: map[string]*store.Event{
			"5:event": {
				CalendarID:   5,
				UID:          "event",
				ResourceName: "event",
				RawICAL:      "BEGIN:VCALENDAR\r\nBEGIN:VEVENT\r\nUID:event\r\nEND:VEVENT\r\nEND:VCALENDAR\r\n",
				ETag:         "etag-event",
			},
		},
	}
	bookRepo := &fakeAddressBookRepo{
		books: map[int64]*store.AddressBook{
			6: {ID: 6, UserID: 1, Name: "Contacts"},
		},
	}
	contactRepo := &fakeContactRepo{
		contacts: map[string]*store.Contact{
			"6:alice": {
				AddressBookID: 6,
				UID:           "alice",
				ResourceName:  "alice",
				RawVCard:      buildVCard("3.0", "UID:alice", "FN:Alice Example"),
				ETag:          "etag-alice",
			},
		},
	}
	aclRepo := &fakeACLRepo{}
	for _, grant := range delegateGrants {
		aclRepo.entries = append(aclRepo.entries,
			store.ACLEntry{ResourcePath: "/dav/calendars/5", PrincipalHref: "/dav/principals/2/", IsGrant: true, Privilege: grant},
			store.ACLEntry{ResourcePath: "/dav/addressbooks/6", PrincipalHref: "/dav/principals/2/", IsGrant: true, Privilege: grant},
		)
	}
	return &DavServer{store: &store.Store{
		Calendars:    calRepo,
		Events:       eventRepo,
		AddressBooks: bookRepo,
		Contacts:     contactRepo,
		ACLEntries:   aclRepo,
	}}
}

// TestPropfindCurrentUserPrivilegeSetResourceKinds pins that RFC 3744 §5.4
// makes current-user-privilege-set the property clients use to discover the
// privileges they hold on a resource, so address-book collections and both object
// kinds must answer 200 with the computed privileges instead of a property-level
// 404.
func TestPropfindCurrentUserPrivilegeSetResourceKinds(t *testing.T) {
	owner := &store.User{ID: 1, PrimaryEmail: "owner@example.com"}
	delegate := &store.User{ID: 2, PrimaryEmail: "delegate@example.com"}

	// DAV:write aggregates write-content, write-properties, bind and unbind
	// (RFC 3744 §3.2), so an editor holds each of them beside it.
	writePrivileges := []string{"<d:write>", "<d:write-content>", "<d:write-properties>", "<d:bind>", "<d:unbind>"}
	allPrivileges := append([]string{"<d:read>"}, writePrivileges...)

	cases := []struct {
		name        string
		user        *store.User
		grants      []string
		requestPath string
		href        string
		want        []string
		unwanted    []string
	}{
		{
			name: "owned calendar collection", user: owner,
			requestPath: "/dav/calendars/5/", href: "/dav/calendars/5/",
			want: allPrivileges,
		},
		{
			name: "owned calendar object", user: owner,
			requestPath: "/dav/calendars/5/event.ics", href: "/dav/calendars/5/event.ics",
			want: allPrivileges,
		},
		{
			name: "owned address book collection", user: owner,
			requestPath: "/dav/addressbooks/6/", href: "/dav/addressbooks/6/",
			want: allPrivileges,
		},
		{
			name: "owned address object", user: owner,
			requestPath: "/dav/addressbooks/6/alice.vcf", href: "/dav/addressbooks/6/alice.vcf",
			want: allPrivileges,
		},
		{
			name: "shared read-only calendar collection", user: delegate, grants: []string{"read", "read-current-user-privilege-set"},
			requestPath: "/dav/calendars/5/", href: "/dav/calendars/5/",
			want: []string{"<d:read>"}, unwanted: writePrivileges,
		},
		{
			name: "shared read-only calendar object", user: delegate, grants: []string{"read", "read-current-user-privilege-set"},
			requestPath: "/dav/calendars/5/event.ics", href: "/dav/calendars/5/event.ics",
			want: []string{"<d:read>"}, unwanted: writePrivileges,
		},
		{
			name: "shared read-only address book collection", user: delegate, grants: []string{"read", "read-current-user-privilege-set"},
			requestPath: "/dav/addressbooks/6/", href: "/dav/addressbooks/6/",
			want: []string{"<d:read>"}, unwanted: writePrivileges,
		},
		{
			name: "shared read-only address object", user: delegate, grants: []string{"read", "read-current-user-privilege-set"},
			requestPath: "/dav/addressbooks/6/alice.vcf", href: "/dav/addressbooks/6/alice.vcf",
			want: []string{"<d:read>"}, unwanted: writePrivileges,
		},
		{
			name: "shared writable calendar collection", user: delegate, grants: []string{"read", "read-current-user-privilege-set", "write", "bind", "unbind"},
			requestPath: "/dav/calendars/5/", href: "/dav/calendars/5/",
			want: allPrivileges,
		},
		{
			name: "shared writable calendar object", user: delegate, grants: []string{"read", "read-current-user-privilege-set", "write", "bind", "unbind"},
			requestPath: "/dav/calendars/5/event.ics", href: "/dav/calendars/5/event.ics",
			want: allPrivileges,
		},
		{
			name: "shared writable address book collection", user: delegate, grants: []string{"read", "read-current-user-privilege-set", "write", "bind", "unbind"},
			requestPath: "/dav/addressbooks/6/", href: "/dav/addressbooks/6/",
			want: allPrivileges,
		},
		{
			name: "shared writable address object", user: delegate, grants: []string{"read", "read-current-user-privilege-set", "write", "bind", "unbind"},
			requestPath: "/dav/addressbooks/6/alice.vcf", href: "/dav/addressbooks/6/alice.vcf",
			want: allPrivileges,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			resp := privilegeSetPropfindResponse(t, privilegeKindsHandler(tc.grants...), tc.user, tc.requestPath, tc.href)
			if !strings.Contains(resp, "<d:current-user-privilege-set>") {
				t.Fatalf("expected current-user-privilege-set with privileges, got %s", resp)
			}
			if strings.Contains(resp, httpStatusNotFound) {
				t.Fatalf("current-user-privilege-set must not be answered with a 404 propstat, got %s", resp)
			}
			for _, want := range tc.want {
				if !strings.Contains(resp, want) {
					t.Fatalf("expected privilege %s in %s", want, resp)
				}
			}
			for _, unwanted := range tc.unwanted {
				if strings.Contains(resp, unwanted) {
					t.Fatalf("unexpected privilege %s in %s", unwanted, resp)
				}
			}
		})
	}
}

// TestPropfindBirthdayCalendarObjectPrivilegeSet covers the virtual birthday
// calendar: its objects are real PROPFIND resources, so they must report the
// read-only privilege set their collection advertises rather than a 404.
func TestPropfindBirthdayCalendarObjectPrivilegeSet(t *testing.T) {
	user := &store.User{ID: 1, PrimaryEmail: "owner@example.com"}
	birthday := time.Date(1990, time.June, 15, 0, 0, 0, 0, time.UTC)
	displayName := "Alice Example"
	contactRepo := &fakeContactRepo{
		contacts: map[string]*store.Contact{
			"6:alice": {
				AddressBookID: 6,
				UID:           "alice",
				ResourceName:  "alice",
				DisplayName:   &displayName,
				Birthday:      &birthday,
				RawVCard:      buildVCard("3.0", "UID:alice", "FN:Alice Example", "BDAY:19900615"),
				ETag:          "etag-alice",
			},
		},
	}
	h := &DavServer{store: &store.Store{Calendars: &fakeCalendarRepo{}, Contacts: contactRepo}}

	body := `<?xml version="1.0" encoding="utf-8"?>
<d:propfind xmlns:d="DAV:"><d:prop><d:current-user-privilege-set/></d:prop></d:propfind>`
	req := httptest.NewRequest("PROPFIND", "/dav/calendars/-1/", strings.NewReader(body))
	req.Header.Set("Depth", "1")
	req = req.WithContext(auth.WithUser(req.Context(), user))
	rr := httptest.NewRecorder()

	h.Propfind(rr, req)

	if rr.Code != http.StatusMultiStatus {
		t.Fatalf("expected 207, got %d: %s", rr.Code, rr.Body.String())
	}
	objectResponse := davResponseForHref(t, rr.Body.String(), calendarObjectHref(birthdayCalendarHref(), birthdayEventUID(6, "alice")))
	if !strings.Contains(objectResponse, "<d:read>") {
		t.Fatalf("expected read privilege on a birthday object, got %s", objectResponse)
	}
	if strings.Contains(objectResponse, httpStatusNotFound) {
		t.Fatalf("current-user-privilege-set must not be a 404 on a birthday object, got %s", objectResponse)
	}
	if strings.Contains(objectResponse, "<d:write>") {
		t.Fatalf("birthday calendar is read-only, got %s", objectResponse)
	}
}

func TestPropfindObjectPrivilegeSetRequiresReadCurrentUserPrivilegeSet(t *testing.T) {
	stranger := &store.User{ID: 3, PrimaryEmail: "stranger@example.com"}
	h := privilegeKindsHandler("read")
	req := &propfindRequest{Prop: &propfindPropQuery{
		propertySelection: propertySelection{CurrentUserPrivilegeSet: &struct{}{}},
	}}

	cases := []struct {
		name     string
		href     string
		propstat propstat
	}{
		{"calendar object", "/dav/calendars/5/event.ics", etagProp("etag-event", "BEGIN:VCALENDAR\r\nEND:VCALENDAR\r\n", true)},
		{"address object", "/dav/addressbooks/6/alice.vcf", addressBookResourcePropstat("etag-alice", buildVCard("3.0", "UID:alice", "FN:Alice Example"))},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			ctx := auth.WithUser(context.Background(), stranger)
			responses := []response{resourceResponse(tc.href, tc.propstat)}
			if err := h.decoratePropfindResponses(ctx, nil, stranger, responses, decorationMaskFor(req)); err != nil {
				t.Fatalf("decorate responses: %v", err)
			}
			filtered := filterPropfindResponse(responses[0], req)
			forbiddenXML := marshalPropstatXML(t, propstatWithStatus(filtered.Propstat, httpStatusForbidden))
			if !strings.Contains(forbiddenXML, "<d:current-user-privilege-set") {
				t.Fatalf("expected current-user-privilege-set in the 403 propstat, got %q", forbiddenXML)
			}
		})
	}
}

// TestCurrentUserPrivilegeSetForObjectPathsIsPresent verifies the producer side
// for object paths: the property is present (non-nil) for calendar and address
// objects so the filter can answer 200 rather than reporting it absent.
func TestCurrentUserPrivilegeSetForObjectPathsIsPresent(t *testing.T) {
	h := privilegeKindsHandler()
	owner := &store.User{ID: 1, PrimaryEmail: "owner@example.com"}

	for _, path := range []string{
		"/dav/calendars/5/event.ics",
		"/dav/addressbooks/6/alice.vcf",
		calendarObjectHref(birthdayCalendarHref(), birthdayEventUID(6, "alice")),
	} {
		privs, err := h.currentUserPrivilegeSetForPath(context.Background(), owner, path)
		if err != nil {
			t.Fatal(err)
		}
		if privs == nil {
			t.Fatalf("%s: expected a present privilege set (non-nil), got nil", path)
		}
		if len(privs.Privileges) == 0 {
			t.Fatalf("%s: expected privileges for a reachable object, got none", path)
		}
	}
}

// DAV:write is the aggregate of write-content, write-properties, bind and
// unbind (RFC 3744 section 3.2), so a member whose ACL denies the delegate
// unbind does not report DAV:write, though its collection grants it, and
// still reports write-content, which only the collection decides.
func TestCurrentUserPrivilegeSetOmitsWriteWhenTheMemberDeniesAPart(t *testing.T) {
	delegate := &store.User{ID: 2, PrimaryEmail: "delegate@example.com"}
	h := privilegeKindsHandler("read", "write", "read-current-user-privilege-set")
	acls := h.store.ACLEntries.(*fakeACLRepo)
	acls.entries = append(acls.entries, store.ACLEntry{ResourcePath: "/dav/calendars/5/event", PrincipalHref: "/dav/principals/2/", IsGrant: false, Privilege: "unbind"})

	response := privilegeSetPropfindResponse(t, h, delegate, "/dav/calendars/5/event.ics", "/dav/calendars/5/event.ics")
	if strings.Contains(response, "<d:write>") || strings.Contains(response, "<d:write/>") {
		t.Errorf("current-user-privilege-set reports DAV:write despite the member's unbind deny: %s", response)
	}
	if !strings.Contains(response, "<d:write-content") {
		t.Errorf("current-user-privilege-set lost write-content, which the collection grants: %s", response)
	}
}

// RFC 3744 section 3.12 makes an aggregate privilege equivalent to the set of
// its parts, so a member whose own ACL grants one part of DAV:write, leaving
// the rest to its collection's grant of DAV:write, still reports DAV:write.
func TestCurrentUserPrivilegeSetReportsWriteWhosePartsTheMemberAndCollectionGrant(t *testing.T) {
	delegate := &store.User{ID: 2, PrimaryEmail: "delegate@example.com"}
	for _, member := range []struct{ statePath, href string }{
		{"/dav/calendars/5/event", "/dav/calendars/5/event.ics"},
		{"/dav/addressbooks/6/alice", "/dav/addressbooks/6/alice.vcf"},
	} {
		h := privilegeKindsHandler("read", "write", "read-current-user-privilege-set")
		acls := h.store.ACLEntries.(*fakeACLRepo)
		acls.entries = append(acls.entries, store.ACLEntry{ResourcePath: member.statePath, PrincipalHref: "/dav/principals/2/", IsGrant: true, Privilege: "bind"})

		response := privilegeSetPropfindResponse(t, h, delegate, member.href, member.href)
		if !strings.Contains(response, "<d:write>") && !strings.Contains(response, "<d:write/>") {
			t.Errorf("%s: current-user-privilege-set omits DAV:write though every part of it is granted: %s", member.href, response)
		}
	}
}

// RFC 3744 section 3.12 makes an aggregate privilege equivalent to the set of
// the privileges it contains, so DAV:all is reported only while every
// privilege it contains is held, and DAV:read only with read-free-busy, which
// CalCard's DAV:read contains. A collection's grant of DAV:all and a member's
// refusal of one contained privilege leave both the refused privilege and the
// aggregates containing it unreported, and the rest reported.
func TestCurrentUserPrivilegeSetReportsAnAggregateOnlyWithEveryPrivilegeItContains(t *testing.T) {
	delegate := &store.User{ID: 2, PrimaryEmail: "delegate@example.com"}
	cases := []struct {
		denied         string
		want, unwanted []string
	}{
		{"unbind", []string{"read", "read-free-busy", "write-content", "bind", "write-acl"}, []string{"all", "write", "unbind"}},
		{"read-free-busy", []string{"read-acl", "write", "unbind"}, []string{"all", "read", "read-free-busy"}},
		{"write-acl", []string{"read", "write"}, []string{"all", "write-acl"}},
	}
	for _, member := range []struct{ statePath, href string }{
		{"/dav/calendars/5/event", "/dav/calendars/5/event.ics"},
		{"/dav/addressbooks/6/alice", "/dav/addressbooks/6/alice.vcf"},
	} {
		for _, tc := range cases {
			h := privilegeKindsHandler("all")
			acls := h.store.ACLEntries.(*fakeACLRepo)
			acls.entries = append(acls.entries, store.ACLEntry{ResourcePath: member.statePath, PrincipalHref: "/dav/principals/2/", IsGrant: false, Privilege: tc.denied})

			response := privilegeSetPropfindResponse(t, h, delegate, member.href, member.href)
			for _, name := range tc.want {
				if !reportsPrivilege(response, name) {
					t.Errorf("%s, member denying %s: %s is not reported: %s", member.href, tc.denied, name, response)
				}
			}
			for _, name := range tc.unwanted {
				if reportsPrivilege(response, name) {
					t.Errorf("%s, member denying %s: %s is reported: %s", member.href, tc.denied, name, response)
				}
			}
		}
	}

	// Every privilege DAV:all contains, granted one by one, is DAV:all.
	h := privilegeKindsHandler("read", "write", "read-acl", "read-current-user-privilege-set", "write-acl", "unlock")
	if response := privilegeSetPropfindResponse(t, h, delegate, "/dav/calendars/5/event.ics", "/dav/calendars/5/event.ics"); !reportsPrivilege(response, "all") {
		t.Errorf("DAV:all is not reported though each privilege it contains is granted: %s", response)
	}
}

func reportsPrivilege(response, name string) bool {
	return strings.Contains(response, ":"+name+">") || strings.Contains(response, ":"+name+"/>")
}

func TestCurrentUserPrivilegeSetForDelegatedPrincipalReportsGrantedPrivileges(t *testing.T) {
	delegate := &store.User{ID: 1, PrimaryEmail: "delegate@example.com"}
	h := &DavServer{store: &store.Store{ACLEntries: &fakeACLRepo{entries: []store.ACLEntry{
		{ResourcePath: "/dav/principals/2", PrincipalHref: "/dav/principals/1/", IsGrant: true, Privilege: "read"},
		{ResourcePath: "/dav/principals/2", PrincipalHref: "/dav/principals/1/", IsGrant: true, Privilege: "read-current-user-privilege-set"},
	}}}}

	privileges, err := h.currentUserPrivilegeSetForPath(context.Background(), delegate, "/dav/principals/2/")
	if err != nil {
		t.Fatal(err)
	}
	if privileges == nil {
		t.Fatal("expected a present privilege set")
	}
	encoded := marshalPropstatXML(t, &propstat{Prop: prop{CurrentUserPrivilegeSet: privileges}})
	for _, want := range []string{"<d:read>", "<d:current-user-privilege-set>"} {
		if !strings.Contains(encoded, want) {
			t.Fatalf("expected %s in delegated principal privilege set, got %s", want, encoded)
		}
	}
}

func TestCurrentUserPrivilegeSetForAddressBookIncludesSupportedReadFreeBusy(t *testing.T) {
	delegate := &store.User{ID: 2, PrimaryEmail: "delegate@example.com"}
	h := privilegeKindsHandler("read-free-busy", "read-current-user-privilege-set")

	privileges, err := h.currentUserPrivilegeSetForPath(context.Background(), delegate, "/dav/addressbooks/6/")
	if err != nil {
		t.Fatal(err)
	}
	if privileges == nil {
		t.Fatal("expected a present privilege set")
	}
	encoded := marshalPropstatXML(t, &propstat{Prop: prop{CurrentUserPrivilegeSet: privileges}})
	if !strings.Contains(encoded, "<cal:read-free-busy>") {
		t.Fatalf("expected supported read-free-busy privilege, got %s", encoded)
	}
}

// A calendar shared read-only through an ACL grant must appear in the owner's
// calendar-home listing alongside calendars they own, or clients cannot
// discover it (RFC 3744 §5.5 read privilege driving RFC 4791 §6.2.1 discovery).
func TestSharedCalendarsAppearInCalendarHomeListing(t *testing.T) {
	now := store.Now()
	calRepo := &fakeCalendarRepo{
		accessible: []store.CalendarAccess{
			{Calendar: store.Calendar{ID: 1, UserID: 1, Name: "My Calendar", UpdatedAt: now}, Editor: true},
			{Calendar: store.Calendar{ID: 2, UserID: 2, Name: "Shared With Me", UpdatedAt: now}, Editor: false},
		},
	}
	aclRepo := &fakeACLRepo{entries: []store.ACLEntry{
		{ResourcePath: "/dav/calendars/2", PrincipalHref: "/dav/principals/1/", IsGrant: true, Privilege: "read"},
	}}
	h := &DavServer{store: &store.Store{Calendars: calRepo, Events: &fakeEventRepo{}, ACLEntries: aclRepo}}
	user := &store.User{ID: 1}

	req := httptest.NewRequest("PROPFIND", "/dav/calendars/", nil)
	req.Header.Set("Depth", "1")
	req = req.WithContext(auth.WithUser(req.Context(), user))
	rr := httptest.NewRecorder()

	h.Propfind(rr, req)

	decodeMultistatus(t, rr).assertHrefs(t,
		"/dav/calendars/",
		"/dav/calendars/-1/", // the virtual birthday collection
		"/dav/calendars/1/",
		"/dav/calendars/2/",
	)
}

// RFC 3744 §5.5.1 resolves DAV:self and DAV:property against the resource, not
// against the requesting user, so the principal set an ACL decision evaluates
// has to be built per resource. CalCard's owner policy grants a resource's owner
// every privilege ahead of the stored ACEs, so today these forms decide nothing
// the owner short-circuit had not already decided; resolving them here is what
// keeps that a property of the policy rather than of an unresolved sentinel.
func TestApplicablePrincipalsForPathResolvesResourceDependentForms(t *testing.T) {
	owner := &store.User{ID: 1, PrimaryEmail: "owner@example.com"}
	delegate := &store.User{ID: 2, PrimaryEmail: "delegate@example.com"}
	calendar := store.Calendar{ID: 5, UserID: owner.ID, Name: "Work"}
	needsResolution := []store.ACLEntry{{PrincipalHref: aclutil.PrincipalSelf}, {PrincipalHref: aclutil.PrincipalPropertyOwner}}

	h := NewDavServer(Options{Store: &store.Store{
		Calendars: &fakeCalendarRepo{
			accessible: []store.CalendarAccess{{Calendar: calendar, Editor: true}},
			calendars:  map[int64]*store.Calendar{calendar.ID: &calendar},
		},
		Events:     &fakeEventRepo{events: map[string]*store.Event{}},
		ACLEntries: &fakeACLRepo{},
	}})

	tests := []struct {
		name         string
		user         *store.User
		resourcePath string
		want         []string
		absent       []string
	}{
		{
			name: "own principal resolves both self and owner", user: delegate,
			resourcePath: "/dav/principals/2/",
			want:         []string{aclutil.PrincipalSelf, aclutil.PrincipalPropertyOwner},
			absent:       []string{aclutil.PrincipalPropertyGroup},
		},
		{
			name: "another principal resolves neither", user: delegate,
			resourcePath: "/dav/principals/1/",
			absent:       []string{aclutil.PrincipalSelf, aclutil.PrincipalPropertyOwner, aclutil.PrincipalPropertyGroup},
		},
		{
			name: "owned calendar resolves owner but never self", user: owner,
			resourcePath: "/dav/calendars/5",
			want:         []string{aclutil.PrincipalPropertyOwner},
			absent:       []string{aclutil.PrincipalSelf, aclutil.PrincipalPropertyGroup},
		},
		{
			name: "shared calendar resolves neither for the sharee", user: delegate,
			resourcePath: "/dav/calendars/5",
			absent:       []string{aclutil.PrincipalSelf, aclutil.PrincipalPropertyOwner, aclutil.PrincipalPropertyGroup},
		},
		{
			name: "calendar object inherits its collection's owner", user: owner,
			resourcePath: "/dav/calendars/5/event.ics",
			want:         []string{aclutil.PrincipalPropertyOwner},
			absent:       []string{aclutil.PrincipalSelf},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, err := h.applicablePrincipalsForPath(context.Background(), tc.user, tc.resourcePath, needsResolution)
			if err != nil {
				t.Fatalf("applicablePrincipalsForPath() error = %v", err)
			}
			for _, principal := range tc.want {
				if _, ok := got[principal]; !ok {
					t.Errorf("missing %q: %#v", principal, got)
				}
			}
			for _, principal := range tc.absent {
				if _, ok := got[principal]; ok {
					t.Errorf("unexpectedly applied %q: %#v", principal, got)
				}
			}
		})
	}
}

// Resolving a resource's owner costs a lookup, so an ACL naming only hrefs and
// the DAV: sentinels must not pay it. A server with no repositories at all
// answers only if the ordinary path never reaches one.
func TestApplicablePrincipalsForPathSkipsOwnerLookupWithoutResourceForms(t *testing.T) {
	user := &store.User{ID: 2}
	ordinary := []store.ACLEntry{
		{PrincipalHref: aclutil.PrincipalAll, Privilege: "read", IsGrant: true},
		{PrincipalHref: aclutil.PrincipalHref(2), Privilege: "write", IsGrant: true},
	}
	h := NewDavServer(Options{Store: &store.Store{}})

	got, err := h.applicablePrincipalsForPath(context.Background(), user, "/dav/calendars/5", ordinary)
	if err != nil {
		t.Fatalf("applicablePrincipalsForPath() error = %v", err)
	}
	if _, ok := got[aclutil.PrincipalHref(2)]; !ok {
		t.Fatalf("resolved set dropped the user's own principal: %#v", got)
	}
	for _, principal := range []string{aclutil.PrincipalSelf, aclutil.PrincipalPropertyOwner, aclutil.PrincipalPropertyGroup} {
		if _, ok := got[principal]; ok {
			t.Errorf("unexpectedly applied %q: %#v", principal, got)
		}
	}
}

type unavailablePrivilegeCalendarRepo struct {
	store.CalendarRepository
	err error
}

func (r unavailablePrivilegeCalendarRepo) GetByID(context.Context, int64) (*store.Calendar, error) {
	return nil, r.err
}

func TestDecoratePrivilegeSetPropagatesRepositoryFailure(t *testing.T) {
	failure := errors.New("calendar repository unavailable")
	h := &DavServer{store: &store.Store{Calendars: unavailablePrivilegeCalendarRepo{err: failure}}}
	p := &prop{}
	err := h.decorateDAVProp(context.Background(), &store.User{ID: 1}, "/dav/calendars/5", "/dav/calendars/5/", p, propDecorationMask{currentUserPrivilegeSet: true})
	if !errors.Is(err, failure) {
		t.Fatalf("error = %v, want repository error instead of an absent property", err)
	}
}

func (r unavailablePrivilegeCalendarRepo) GetAccessible(context.Context, int64, int64) (*store.CalendarAccess, error) {
	return nil, r.err
}

// A store with ACL entries configured is the production shape: the virtual
// birthday calendar has no stored row to own, so an owner check against the
// calendars table denies every privilege on it and the property comes back
// 403 Forbidden. DAVx5 then falls back to assuming the collection is writable.
func TestPropfindBirthdayCalendarPrivilegeSetWithACLStore(t *testing.T) {
	user := &store.User{ID: 1, PrimaryEmail: "owner@example.com"}
	h := &DavServer{store: &store.Store{
		Calendars:  &fakeCalendarRepo{},
		Contacts:   &fakeContactRepo{},
		ACLEntries: &fakeACLRepo{},
	}}

	body := `<?xml version="1.0" encoding="utf-8"?>
<d:propfind xmlns:d="DAV:"><d:prop><d:current-user-privilege-set/></d:prop></d:propfind>`
	req := httptest.NewRequest("PROPFIND", "/dav/calendars/-1/", strings.NewReader(body))
	req.Header.Set("Depth", "0")
	req = req.WithContext(auth.WithUser(req.Context(), user))
	rr := httptest.NewRecorder()

	h.Propfind(rr, req)

	if rr.Code != http.StatusMultiStatus {
		t.Fatalf("expected 207, got %d: %s", rr.Code, rr.Body.String())
	}
	resp := rr.Body.String()
	if strings.Contains(resp, httpStatusForbidden) {
		t.Fatalf("the birthday calendar owner must be able to read its privilege set, got %s", resp)
	}
	for _, want := range []string{"<d:read>", "<cal:read-free-busy>", "<d:read-acl>", "<d:read-current-user-privilege-set>"} {
		if !strings.Contains(resp, want) {
			t.Fatalf("expected %s in the birthday privilege set, got %s", want, resp)
		}
	}
	for _, unwanted := range []string{"<d:write>", "<d:write-content>", "<d:bind>", "<d:unbind>", "<d:write-acl>", "<d:all>"} {
		if strings.Contains(resp, unwanted) {
			t.Fatalf("the birthday calendar is read-only but advertised %s: %s", unwanted, resp)
		}
	}
}

// The DAV:acl the birthday collection reports has to agree with the privileges
// it actually enforces, or a client is told the owner may write to a
// collection every mutating method refuses.
func TestPropfindBirthdayCalendarACLMatchesEnforcedPrivileges(t *testing.T) {
	user := &store.User{ID: 1, PrimaryEmail: "owner@example.com"}
	h := &DavServer{store: &store.Store{
		Calendars:  &fakeCalendarRepo{},
		Contacts:   &fakeContactRepo{},
		ACLEntries: &fakeACLRepo{},
	}}

	body := `<?xml version="1.0" encoding="utf-8"?>
<d:propfind xmlns:d="DAV:"><d:prop><d:acl/></d:prop></d:propfind>`
	req := httptest.NewRequest("PROPFIND", "/dav/calendars/-1/", strings.NewReader(body))
	req.Header.Set("Depth", "0")
	req = req.WithContext(auth.WithUser(req.Context(), user))
	rr := httptest.NewRecorder()

	h.Propfind(rr, req)

	resp := rr.Body.String()
	if strings.Contains(resp, httpStatusForbidden) {
		t.Fatalf("the birthday calendar owner may read its ACL, got %s", resp)
	}
	if !strings.Contains(resp, "<d:read>") {
		t.Fatalf("expected the owner ACE to grant read, got %s", resp)
	}
	if strings.Contains(resp, "<d:all>") {
		t.Fatalf("the owner ACE must not grant privileges the collection refuses: %s", resp)
	}
}

// The privileges the generated collection grants stop at reading: every
// mutating method refuses it, so the check must not report otherwise.
func TestBirthdayCalendarPrivilegeCheckDeniesWrites(t *testing.T) {
	user := &store.User{ID: 1}
	h := &DavServer{store: &store.Store{Calendars: &fakeCalendarRepo{}, ACLEntries: &fakeACLRepo{}}}
	ctx := auth.WithUser(context.Background(), user)

	// The collection is reached with and without the trailing slash (a parent
	// lookup strips it) and by its zero-padded alias.
	for _, path := range []string{"/dav/calendars/-1/", "/dav/calendars/-1", "/dav/calendars/-01/", calendarObjectHref(birthdayCalendarHref(), birthdayEventUID(6, "alice"))} {
		for _, privilege := range []string{"read", "read-free-busy", "read-acl", "read-current-user-privilege-set"} {
			allowed, err := h.checkACLPrivilege(ctx, user, path, privilege)
			if err != nil {
				t.Fatalf("checkACLPrivilege(%s, %s): %v", path, privilege, err)
			}
			if !allowed {
				t.Errorf("%s must be granted on %s", privilege, path)
			}
		}
		for _, privilege := range []string{"write", "write-content", "write-properties", "bind", "unbind", "write-acl", "unlock", "all"} {
			allowed, err := h.checkACLPrivilege(ctx, user, path, privilege)
			if err != nil {
				t.Fatalf("checkACLPrivilege(%s, %s): %v", path, privilege, err)
			}
			if allowed {
				t.Errorf("%s must be denied on %s", privilege, path)
			}
		}
		allowed, err := h.checkACLPrivilege(context.Background(), nil, path, "read")
		if err != nil {
			t.Fatalf("checkACLPrivilege(no user, %s): %v", path, err)
		}
		if allowed {
			t.Errorf("an unauthenticated request must not be granted read on %s", path)
		}
	}
}
