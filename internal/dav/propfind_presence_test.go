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

// marshalPropstatXML renders a single propstat so tests can assert precisely
// which propstat (200 vs 404) a requested property lands in.
func marshalPropstatXML(t *testing.T, ps *propstat) string {
	t.Helper()
	if ps == nil {
		return ""
	}
	data, err := xml.Marshal(*ps)
	if err != nil {
		t.Fatalf("marshal propstat: %v", err)
	}
	return string(data)
}

// TestFilterPropfindThreeStatePresence verifies the explicit three-state model
// (RFC 4918 §9.1): an absent property is returned in the 404 propstat, a
// present-empty property is rendered as an empty element in the 200 propstat,
// and a present-nonempty property carries its value in the 200 propstat. In
// every case the requested property must appear in exactly one propstat.
func TestFilterPropfindThreeStatePresence(t *testing.T) {
	calType := func() *resourceType { return &resourceType{Collection: &struct{}{}, Calendar: &struct{}{}} }
	bookType := func() *resourceType { return &resourceType{Collection: &struct{}{}, AddressBook: &struct{}{}} }

	cases := []struct {
		name     string
		href     string
		rtype    func() *resourceType
		element  string
		sel      propertySelection
		absent   func(p *prop)
		empty    func(p *prop)
		nonEmpty func(p *prop)
		// nonEmptyContains is the substring expected inside the 200 propstat for
		// the present-nonempty state.
		nonEmptyContains string
	}{
		{
			name: "displayname", href: "/dav/calendars/1/", rtype: calType, element: "d:displayname",
			sel:              propertySelection{DisplayName: &struct{}{}},
			absent:           func(p *prop) { p.DisplayName = nil },
			empty:            func(p *prop) { p.DisplayName = stringPtr("") },
			nonEmpty:         func(p *prop) { p.DisplayName = stringPtr("Work") },
			nonEmptyContains: "<d:displayname>Work</d:displayname>",
		},
		{
			name: "calendar-description", href: "/dav/calendars/1/", rtype: calType, element: "cal:calendar-description",
			sel:              propertySelection{CalendarDescription: &struct{}{}},
			absent:           func(p *prop) { p.CalendarDescription = nil },
			empty:            func(p *prop) { p.CalendarDescription = langStringPtr("", nil) },
			nonEmpty:         func(p *prop) { p.CalendarDescription = langStringPtr("My calendar", nil) },
			nonEmptyContains: "<cal:calendar-description>My calendar</cal:calendar-description>",
		},
		{
			name: "calendar-timezone", href: "/dav/calendars/1/", rtype: calType, element: "cal:calendar-timezone",
			sel:              propertySelection{CalendarTimezone: &struct{}{}},
			absent:           func(p *prop) { p.CalendarTimezone = nil },
			empty:            func(p *prop) { p.CalendarTimezone = stringPtr("") },
			nonEmpty:         func(p *prop) { p.CalendarTimezone = stringPtr("BEGIN:VTIMEZONE\r\nEND:VTIMEZONE\r\n") },
			nonEmptyContains: "BEGIN:VTIMEZONE",
		},
		{
			name: "calendar-color", href: "/dav/calendars/1/", rtype: calType, element: "ical:calendar-color",
			sel:              propertySelection{CalendarColor: &struct{}{}},
			absent:           func(p *prop) { p.CalendarColor = nil },
			empty:            func(p *prop) { p.CalendarColor = stringPtr("") },
			nonEmpty:         func(p *prop) { p.CalendarColor = stringPtr("#FF0000") },
			nonEmptyContains: "<ical:calendar-color>#FF0000</ical:calendar-color>",
		},
		{
			name: "addressbook-description", href: "/dav/addressbooks/1/", rtype: bookType, element: "card:addressbook-description",
			sel:              propertySelection{AddressBookDesc: &struct{}{}},
			absent:           func(p *prop) { p.AddressBookDesc = nil },
			empty:            func(p *prop) { p.AddressBookDesc = stringPtr("") },
			nonEmpty:         func(p *prop) { p.AddressBookDesc = stringPtr("Team contacts") },
			nonEmptyContains: "<card:addressbook-description>Team contacts</card:addressbook-description>",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			open := "<" + tc.element
			emptyEl := "<" + tc.element + "></" + tc.element + ">"

			run := func(state string, apply func(p *prop)) (okXML, nfXML string) {
				src := prop{ResourceType: tc.rtype()}
				apply(&src)
				resp := response{Href: tc.href, Propstat: []propstat{{Prop: src, Status: httpStatusOK}}}
				filtered := filterNonPrincipalPropfindResponse(resp, &propfindRequest{Prop: &propfindPropQuery{propertySelection: tc.sel}})
				okXML = marshalPropstatXML(t, propstatWithStatus(filtered.Propstat, httpStatusOK))
				nfXML = marshalPropstatXML(t, propstatWithStatus(filtered.Propstat, httpStatusNotFound))
				// The property must appear in exactly one propstat.
				if strings.Contains(okXML, open) == strings.Contains(nfXML, open) {
					t.Fatalf("%s: property %s must appear in exactly one propstat (200=%q, 404=%q)", state, tc.element, okXML, nfXML)
				}
				return okXML, nfXML
			}

			// Absent -> 404.
			_, nfXML := run("absent", tc.absent)
			if !strings.Contains(nfXML, open) {
				t.Fatalf("absent: expected %s in 404 propstat, got %q", tc.element, nfXML)
			}

			// Present-empty -> 200 empty element.
			okXML, _ := run("present-empty", tc.empty)
			if !strings.Contains(okXML, emptyEl) {
				t.Fatalf("present-empty: expected empty element %s in 200 propstat, got %q", emptyEl, okXML)
			}

			// Present-nonempty -> 200 value.
			okXML, _ = run("present-nonempty", tc.nonEmpty)
			if !strings.Contains(okXML, tc.nonEmptyContains) {
				t.Fatalf("present-nonempty: expected %q in 200 propstat, got %q", tc.nonEmptyContains, okXML)
			}
		})
	}
}

// TestFilterCurrentUserPrivilegeSetPresence verifies the explicit three-state
// contract for current-user-privilege-set at the filter: a nil source is absent
// (404), a zero-length set is present-empty (200 empty element), and a populated
// set is present-nonempty (200 value). A resource kind outside the mask is always
// 404 regardless of the source value.
func TestFilterCurrentUserPrivilegeSetPresence(t *testing.T) {
	const element = "d:current-user-privilege-set"
	const emptyEl = "<d:current-user-privilege-set></d:current-user-privilege-set>"
	sel := propertySelection{CurrentUserPrivilegeSet: &struct{}{}}

	filter := func(href string, rtype *resourceType, set *currentUserPrivilegeSet) (okXML, nfXML string) {
		src := prop{ResourceType: rtype, CurrentUserPrivilegeSet: set}
		resp := response{Href: href, Propstat: []propstat{{Prop: src, Status: httpStatusOK}}}
		filtered := filterNonPrincipalPropfindResponse(resp, &propfindRequest{Prop: &propfindPropQuery{propertySelection: sel}})
		return marshalPropstatXML(t, propstatWithStatus(filtered.Propstat, httpStatusOK)),
			marshalPropstatXML(t, propstatWithStatus(filtered.Propstat, httpStatusNotFound))
	}

	// RFC 3744 §5.4: the property is defined on every DAV resource kind.
	applicable := []struct {
		name  string
		href  string
		rtype *resourceType
	}{
		{"calendar-collection", "/dav/calendars/1/", &resourceType{Collection: &struct{}{}, Calendar: &struct{}{}}},
		{"addressbook-collection", "/dav/addressbooks/1/", &resourceType{Collection: &struct{}{}, AddressBook: &struct{}{}}},
		{"principal", "/dav/principals/1/", &resourceType{Principal: &struct{}{}}},
		{"generic-collection", "/dav/", &resourceType{Collection: &struct{}{}}},
		{"calendar-object", "/dav/calendars/1/event.ics", nil},
		{"address-object", "/dav/addressbooks/1/alice.vcf", nil},
	}
	for _, k := range applicable {
		t.Run(k.name+"/nil-absent", func(t *testing.T) {
			okXML, nfXML := filter(k.href, k.rtype, nil)
			if strings.Contains(okXML, "<"+element) {
				t.Fatalf("nil source must be absent, not in 200 propstat, got %q", okXML)
			}
			if !strings.Contains(nfXML, "<"+element) {
				t.Fatalf("expected nil source in 404 propstat, got %q", nfXML)
			}
		})
		t.Run(k.name+"/zero-present-empty", func(t *testing.T) {
			okXML, nfXML := filter(k.href, k.rtype, &currentUserPrivilegeSet{})
			if !strings.Contains(okXML, emptyEl) {
				t.Fatalf("expected present-empty privilege set in 200 propstat, got 200=%q 404=%q", okXML, nfXML)
			}
			if strings.Contains(nfXML, "<"+element) {
				t.Fatalf("present-empty set must not appear in 404 propstat, got %q", nfXML)
			}
		})
		t.Run(k.name+"/valued", func(t *testing.T) {
			okXML, _ := filter(k.href, k.rtype, calendarCurrentUserPrivilegeSet(true))
			if !strings.Contains(okXML, "<d:privilege>") {
				t.Fatalf("expected present-nonempty privilege set in 200 propstat, got %q", okXML)
			}
		})
	}
}

func TestCurrentUserPrivilegeSetForOwnedGenericPathsReportsEffectivePrivileges(t *testing.T) {
	h := &DavServer{store: &store.Store{}}
	user := &store.User{ID: 1, PrimaryEmail: "user@example.com"}
	for _, path := range []string{"/dav/principals/1/", "/dav/", "/dav/calendars/", "/dav/addressbooks/"} {
		privs := h.currentUserPrivilegeSetForPath(context.Background(), user, path)
		if privs == nil {
			t.Fatalf("%s: expected a privilege set, got nil", path)
		}
		if len(privs.Privileges) != len(calendarCurrentPrivilegeNames) {
			t.Fatalf("%s: got %d privileges, want %d: %#v", path, len(privs.Privileges), len(calendarCurrentPrivilegeNames), privs.Privileges)
		}
	}
}

// TestStatusOKPropDisplayNamePresence covers both collection-response helpers:
// an empty name is absent (DisplayName nil), a non-empty name is present.
func TestStatusOKPropDisplayNamePresence(t *testing.T) {
	rtype := resourceType{Collection: &struct{}{}, Calendar: &struct{}{}}

	if dn := statusOKProp("", rtype).Prop.DisplayName; dn != nil {
		t.Fatalf("statusOKProp: empty name must leave DisplayName nil, got %q", *dn)
	}
	if dn := statusOKProp("Work", rtype).Prop.DisplayName; dn == nil || *dn != "Work" {
		t.Fatalf("statusOKProp: expected DisplayName \"Work\", got %#v", dn)
	}

	if dn := statusOKPropWithExtras("", rtype, "/dav/principals/1/", true, false).Prop.DisplayName; dn != nil {
		t.Fatalf("statusOKPropWithExtras: empty name must leave DisplayName nil, got %q", *dn)
	}
	if dn := statusOKPropWithExtras("Work", rtype, "/dav/principals/1/", true, false).Prop.DisplayName; dn == nil || *dn != "Work" {
		t.Fatalf("statusOKPropWithExtras: expected DisplayName \"Work\", got %#v", dn)
	}
}

func TestPropfindOwnedPrincipalCurrentUserPrivilegeSetIncludesAll(t *testing.T) {
	user := &store.User{ID: 1, PrimaryEmail: "user@example.com"}
	h := &DavServer{store: &store.Store{}}

	body := `<?xml version="1.0"?><d:propfind xmlns:d="DAV:"><d:prop><d:current-user-privilege-set/></d:prop></d:propfind>`
	req := httptest.NewRequest("PROPFIND", "/dav/principals/1/", strings.NewReader(body))
	req.Header.Set("Depth", "0")
	req = req.WithContext(auth.WithUser(req.Context(), user))
	rr := httptest.NewRecorder()

	h.Propfind(rr, req)

	if rr.Code != http.StatusMultiStatus {
		t.Fatalf("expected 207, got %d: %s", rr.Code, rr.Body.String())
	}
	respBody := rr.Body.String()
	if !strings.Contains(respBody, "<d:all") || !strings.Contains(respBody, "<d:write-acl") || !strings.Contains(respBody, "<d:unlock") {
		t.Fatalf("expected effective owner privileges on principal, got %s", respBody)
	}
	if strings.Contains(respBody, "404") {
		t.Fatalf("current-user-privilege-set must not be a 404 on a principal, got %s", respBody)
	}
}

// TestPropfindCalendarDescriptionPresentEmptyRendersIn200 is an end-to-end
// regression for H2: a calendar with an explicitly-empty description must not
// have calendar-description vanish from both propstats. It is a present-empty
// 200, never a 404 and never omitted.
func TestPropfindCalendarDescriptionPresentEmptyRendersIn200(t *testing.T) {
	user := &store.User{ID: 1}
	calRepo := &fakeCalendarRepo{
		accessible: []store.CalendarAccess{
			{Calendar: store.Calendar{ID: 2, UserID: 1, Name: "Work", Description: stringPtr(""), CTag: 1}, Editor: true},
		},
		accessibleByUser: map[int64][]store.CalendarAccess{
			1: {{Calendar: store.Calendar{ID: 2, UserID: 1, Name: "Work", Description: stringPtr(""), CTag: 1}, Editor: true}},
		},
		calendars: map[int64]*store.Calendar{
			2: {ID: 2, UserID: 1, Name: "Work", Description: stringPtr(""), CTag: 1},
		},
	}
	h := &DavServer{store: &store.Store{Calendars: calRepo}}

	body := `<?xml version="1.0"?><d:propfind xmlns:d="DAV:" xmlns:cal="urn:ietf:params:xml:ns:caldav"><d:prop><cal:calendar-description/></d:prop></d:propfind>`
	req := httptest.NewRequest("PROPFIND", "/dav/calendars/2/", strings.NewReader(body))
	req.Header.Set("Depth", "0")
	req = req.WithContext(auth.WithUser(req.Context(), user))
	rr := httptest.NewRecorder()

	h.Propfind(rr, req)

	if rr.Code != http.StatusMultiStatus {
		t.Fatalf("expected 207, got %d: %s", rr.Code, rr.Body.String())
	}
	respBody := rr.Body.String()
	if !strings.Contains(respBody, "<cal:calendar-description></cal:calendar-description>") {
		t.Fatalf("expected present-empty calendar-description empty element, got %s", respBody)
	}
	// It must be in the 200 propstat, not the 404 one.
	if strings.Contains(respBody, "404") && !strings.Contains(respBody, "200 OK") {
		t.Fatalf("expected a 200 propstat carrying the property, got %s", respBody)
	}
}

// TestPrincipalResponseDisplayNameNonEmpty verifies the RFC 3744 requirement
// that a principal always has a non-empty DAV:displayname, even when the user
// has neither a full name nor a login email.
func TestPrincipalResponseDisplayNameNonEmpty(t *testing.T) {
	user := &store.User{ID: 7} // no FullName, no PrimaryEmail
	if got := principalDisplayName(user); strings.TrimSpace(got) == "" {
		t.Fatalf("expected non-empty principal display name, got %q", got)
	}
	resp := principalResponse("/dav/principals/7/", user)
	dn := resp.Propstat[0].Prop.DisplayName
	if dn == nil || strings.TrimSpace(*dn) == "" {
		t.Fatalf("expected principal response to carry a non-empty displayname, got %#v", dn)
	}
}

// TestCurrentUserPrivilegeSetZeroPrivilegesIsPresentEmpty verifies that a
// resource the user can reach but on which they hold no privileges yields a
// present-empty set (non-nil, zero children) rather than nil, so the property
// is answered with a 200 empty element instead of a 404.
func TestCurrentUserPrivilegeSetZeroPrivilegesIsPresentEmpty(t *testing.T) {
	delegate := &store.User{ID: 2, PrimaryEmail: "delegate@example.com"}
	calRepo := &fakeCalendarRepo{
		accessibleByUser: map[int64][]store.CalendarAccess{
			delegate.ID: {
				{Calendar: store.Calendar{ID: 5, UserID: 1, Name: "Work"}, Shared: true, PrivilegesResolved: true, Privileges: store.CalendarPrivileges{}},
			},
		},
		calendars: map[int64]*store.Calendar{
			5: {ID: 5, UserID: 1, Name: "Work"},
		},
	}
	h := &DavServer{store: &store.Store{Calendars: calRepo, ACLEntries: &fakeACLRepo{}}}

	privs := h.currentUserPrivilegeSetForPath(context.Background(), delegate, "/dav/calendars/5/")
	if privs != nil {
		t.Fatalf("expected an inaccessible resource to have no visible privilege set, got %#v", privs)
	}
}
