package dav

import (
	"context"
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

// TestPropfindCurrentUserPrivilegeSetResourceKinds is the H3 regression: RFC 3744
// §5.4 makes current-user-privilege-set the property clients use to discover the
// privileges they hold on a resource, so address-book collections and both object
// kinds must answer 200 with the computed privileges instead of a property-level
// 404.
func TestPropfindCurrentUserPrivilegeSetResourceKinds(t *testing.T) {
	owner := &store.User{ID: 1, PrimaryEmail: "owner@example.com"}
	delegate := &store.User{ID: 2, PrimaryEmail: "delegate@example.com"}

	// DAV:write aggregates write-content and write-properties. Bind and unbind
	// remain independent privileges and are granted explicitly for editors.
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
	objectResponse := davResponseForHref(t, rr.Body.String(), "/dav/calendars/-1/birthday-alice@calcard.ics")
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
			filtered := filterNonPrincipalPropfindResponse(responses[0], req)
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
		"/dav/calendars/-1/birthday-alice@calcard.ics",
	} {
		privs := h.currentUserPrivilegeSetForPath(context.Background(), owner, path)
		if privs == nil {
			t.Fatalf("%s: expected a present privilege set (non-nil), got nil", path)
		}
		if len(privs.Privileges) == 0 {
			t.Fatalf("%s: expected privileges for a reachable object, got none", path)
		}
	}
}

func TestCurrentUserPrivilegeSetForDelegatedPrincipalReportsGrantedPrivileges(t *testing.T) {
	delegate := &store.User{ID: 1, PrimaryEmail: "delegate@example.com"}
	h := &DavServer{store: &store.Store{ACLEntries: &fakeACLRepo{entries: []store.ACLEntry{
		{ResourcePath: "/dav/principals/2", PrincipalHref: "/dav/principals/1/", IsGrant: true, Privilege: "read"},
		{ResourcePath: "/dav/principals/2", PrincipalHref: "/dav/principals/1/", IsGrant: true, Privilege: "read-current-user-privilege-set"},
	}}}}

	privileges := h.currentUserPrivilegeSetForPath(context.Background(), delegate, "/dav/principals/2/")
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

	privileges := h.currentUserPrivilegeSetForPath(context.Background(), delegate, "/dav/addressbooks/6/")
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
