package dav

import (
	"context"
	"encoding/xml"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/jw6ventures/calcard/internal/acl"
	"github.com/jw6ventures/calcard/internal/auth"
	"github.com/jw6ventures/calcard/internal/store"
)

func propstatWithStatus(stats []propstat, status string) *propstat {
	for i := range stats {
		if stats[i].Status == status {
			return &stats[i]
		}
	}
	return nil
}

func TestFilterReadableAddressBookContactsFiltersDeniedContacts(t *testing.T) {
	book := &store.AddressBook{ID: 5, UserID: 1, Name: "Contacts"}
	h := &DavServer{store: &store.Store{ACLEntries: &fakeACLRepo{entries: []store.ACLEntry{
		{ResourcePath: "/dav/addressbooks/5/public", PrincipalHref: "/dav/principals/2/", Privilege: "read", IsGrant: true},
		{ResourcePath: "/dav/addressbooks/5/secret", PrincipalHref: "/dav/principals/2/", Privilege: "read", IsGrant: false},
	}}}}

	contacts := []store.Contact{
		{AddressBookID: 5, UID: "public", ResourceName: "public"},
		{AddressBookID: 5, UID: "secret", ResourceName: "secret"},
	}

	filtered, err := h.filterReadableAddressBookContacts(context.Background(), &store.User{ID: 2}, book, contacts)
	if err != nil {
		t.Fatalf("filterReadableAddressBookContacts returned error: %v", err)
	}
	if len(filtered) != 1 {
		t.Fatalf("expected 1 visible contact, got %d", len(filtered))
	}
	if got := contactResourceName(filtered[0]); got != "public" {
		t.Fatalf("expected public contact to remain, got %q", got)
	}
}

func TestAddressBookPrivilegeDecisionFromEntries(t *testing.T) {
	book := &store.AddressBook{ID: 5, UserID: 1, Name: "Contacts"}
	user := &store.User{ID: 2}

	t.Run("owner is always allowed", func(t *testing.T) {
		allowed, denied := addressBookPrivilegeDecisionFromEntries(&store.User{ID: 1}, book, "alice", "read", nil)
		if !allowed || denied {
			t.Fatalf("expected owner allowed, got allowed=%v denied=%v", allowed, denied)
		}
	})

	t.Run("collection grant allows", func(t *testing.T) {
		entries := map[string][]store.ACLEntry{
			"/dav/addressbooks/5": {{ResourcePath: "/dav/addressbooks/5", PrincipalHref: "/dav/principals/2/", Privilege: "read", IsGrant: true}},
		}
		allowed, denied := addressBookPrivilegeDecisionFromEntries(user, book, "alice", "read", entries)
		if !allowed || denied {
			t.Fatalf("expected collection grant to allow, got allowed=%v denied=%v", allowed, denied)
		}
	})

	t.Run("object deny overrides collection grant", func(t *testing.T) {
		entries := map[string][]store.ACLEntry{
			"/dav/addressbooks/5":        {{ResourcePath: "/dav/addressbooks/5", PrincipalHref: "/dav/principals/2/", Privilege: "read", IsGrant: true}},
			"/dav/addressbooks/5/secret": {{ResourcePath: "/dav/addressbooks/5/secret", PrincipalHref: "/dav/principals/2/", Privilege: "read", IsGrant: false}},
		}
		allowed, denied := addressBookPrivilegeDecisionFromEntries(user, book, "secret", "read", entries)
		if allowed || !denied {
			t.Fatalf("expected object deny to win, got allowed=%v denied=%v", allowed, denied)
		}
	})

	t.Run("no applicable ACL is not readable", func(t *testing.T) {
		allowed, denied := addressBookPrivilegeDecisionFromEntries(user, book, "alice", "read", nil)
		if allowed || denied {
			t.Fatalf("expected no decision, got allowed=%v denied=%v", allowed, denied)
		}
	})
}

func TestFilterReadableAddressBookContactsPrefetchesOnce(t *testing.T) {
	book := &store.AddressBook{ID: 5, UserID: 1, Name: "Contacts"}
	repo := &fakeACLRepo{entries: []store.ACLEntry{
		{ResourcePath: "/dav/addressbooks/5", PrincipalHref: "/dav/principals/2/", Privilege: "read", IsGrant: true},
	}}
	h := &DavServer{store: &store.Store{ACLEntries: repo}}

	contacts := make([]store.Contact, 0, 50)
	for i := 0; i < 50; i++ {
		name := fmt.Sprintf("c%d", i)
		contacts = append(contacts, store.Contact{AddressBookID: 5, UID: name, ResourceName: name})
	}

	filtered, err := h.filterReadableAddressBookContacts(context.Background(), &store.User{ID: 2}, book, contacts)
	if err != nil {
		t.Fatalf("filterReadableAddressBookContacts returned error: %v", err)
	}
	if len(filtered) != len(contacts) {
		t.Fatalf("expected all %d contacts visible via collection grant, got %d", len(contacts), len(filtered))
	}
	// One sweep over the user's principals (DAV:all, DAV:authenticated, /dav/principals/2/)
	// rather than a per-contact ListByResource lookup.
	if repo.listByResourceCalls != 0 {
		t.Fatalf("expected no per-resource ACL lookups, got %d", repo.listByResourceCalls)
	}
	if repo.listByPrincipalCalls != len(acl.PrincipalHrefs(&store.User{ID: 2})) {
		t.Fatalf("expected one prefetch sweep (%d principal lookups), got %d", len(acl.PrincipalHrefs(&store.User{ID: 2})), repo.listByPrincipalCalls)
	}
}

func propstatNotFoundNameSet(stats []propstat) map[xml.Name]bool {
	names := map[xml.Name]bool{}
	for i := range stats {
		if stats[i].Status != httpStatusNotFound {
			continue
		}
		for _, name := range stats[i].PropNames {
			names[name] = true
		}
	}
	return names
}

func TestFilterAddressObjectPropfindCoversSupportedAndMissingProperties(t *testing.T) {
	resp := response{
		Href: "/dav/addressbooks/5/alice.vcf",
		Propstat: []propstat{{
			Prop: prop{
				GetETag:                `"etag-alice"`,
				GetContentType:         "text/vcard; charset=utf-8",
				AddressData:            cdataString(buildVCard("3.0", "UID:alice", "FN:Alice Example", "EMAIL:alice@example.com")),
				SupportedReportSet:     addressbookSupportedReports(),
				LockDiscovery:          &lockDiscoveryProp{},
				SupportedLock:          defaultSupportedLock(),
				ACL:                    &aclProp{},
				SupportedPrivilegeSet:  defaultSupportedPrivilegeSet(),
				PrincipalCollectionSet: &hrefListProp{Href: []string{"/dav/principals/"}},
			},
			Status: httpStatusOK,
		}},
	}

	filtered := filterAddressObjectPropfindResponse(resp, &propfindRequest{Prop: &propfindPropQuery{
		GetETag:                &struct{}{},
		GetContentType:         &struct{}{},
		AddressData:            &addressDataQuery{Prop: []addressDataProp{{Name: "FN"}}},
		SupportedReportSet:     &struct{}{},
		LockDiscovery:          &struct{}{},
		SupportedLock:          &struct{}{},
		ACLProp:                &struct{}{},
		SupportedPrivilegeSet:  &struct{}{},
		PrincipalCollectionSet: &struct{}{},
		DisplayName:            &struct{}{},
	}})
	if len(filtered.Propstat) != 2 {
		t.Fatalf("expected 2 propstats, got %#v", filtered.Propstat)
	}
	okStat := propstatWithStatus(filtered.Propstat, httpStatusOK)
	if okStat == nil {
		t.Fatal("expected 200 propstat")
	}
	if string(okStat.Prop.AddressData) == "" || okStat.Prop.SupportedLock == nil || okStat.Prop.ACL == nil {
		t.Fatalf("expected supported properties in 200 propstat, got %#v", okStat.Prop)
	}
	notFound := propstatNotFoundNameSet(filtered.Propstat)
	if !notFound[xml.Name{Local: "d:displayname"}] {
		t.Fatalf("expected displayname in 404 propstat names, got %#v", notFound)
	}

	empty := filterAddressObjectPropfindResponse(resp, &propfindRequest{Prop: &propfindPropQuery{}})
	if len(empty.Propstat) != 1 || empty.Propstat[0].Status != httpStatusOK {
		t.Fatalf("expected empty request to produce empty 200 propstat, got %#v", empty.Propstat)
	}
}

func TestFilterAddressObjectPropfindResponseBranches(t *testing.T) {
	base := response{
		Href: "/dav/addressbooks/5/alice.vcf",
		Propstat: []propstat{{
			Prop: prop{
				GetETag:        `"etag-alice"`,
				GetContentType: "text/vcard; charset=utf-8",
				AddressData:    cdataString(buildVCard("4.0", "UID:alice", "FN:Alice Example", "EMAIL:alice@example.com")),
			},
			Status: httpStatusOK,
		}},
		Status: httpStatusNotFound,
		Error:  &responseError{SupportedAddressDataConversion: &struct{}{}},
	}

	if got := filterAddressObjectPropfindResponse(base, nil); got.Status != httpStatusNotFound {
		t.Fatalf("expected nil request to return response unchanged, got %#v", got)
	}

	notAcceptable := filterAddressObjectPropfindResponse(base, &propfindRequest{Prop: &propfindPropQuery{
		AddressData: &addressDataQuery{ContentType: "text/vcard", Version: "3.0"},
	}})
	if notAcceptable.Status != httpStatusNotAcceptable || notAcceptable.Error == nil || len(notAcceptable.Propstat) != 0 {
		t.Fatalf("expected 406 response with conversion error, got %#v", notAcceptable)
	}

	filtered := filterAddressObjectPropfindResponse(base, &propfindRequest{Prop: &propfindPropQuery{
		GetETag:     &struct{}{},
		AddressData: &addressDataQuery{Prop: []addressDataProp{{Name: "FN"}}},
	}})
	if filtered.Status != "" || filtered.Error != nil {
		t.Fatalf("expected successful filtered response to clear status/error, got %#v", filtered)
	}
	okStat := propstatWithStatus(filtered.Propstat, httpStatusOK)
	if okStat == nil || okStat.Prop.GetETag == "" {
		t.Fatalf("expected 200 propstat with getetag, got %#v", filtered.Propstat)
	}
	if got := string(okStat.Prop.AddressData); got == "" || got == string(base.Propstat[0].Prop.AddressData) {
		t.Fatalf("expected filtered address-data payload, got %q", got)
	}
}

func TestFilterPrincipalPropfindResponseSupportsMixedRequests(t *testing.T) {
	resp := response{Href: "/dav/principals/1/", Propstat: []propstat{{
		Prop: prop{
			DisplayName:             "User One",
			ResourceType:            resourceType{Principal: &struct{}{}},
			CurrentUserPrincipal:    &expandableHrefProp{Href: "/dav/principals/1/"},
			CurrentUserPrincipalURL: &hrefProp{Href: "/dav/principals/1/"},
			PrincipalURL:            &expandableHrefProp{Href: "/dav/principals/1/"},
			CalendarHomeSet:         &hrefListProp{Href: []string{"/dav/calendars/"}},
			AddressbookHomeSet:      &hrefListProp{Href: []string{"/dav/addressbooks/"}},
			SupportedReportSet:      &supportedReportSet{},
			LockDiscovery:           &lockDiscoveryProp{},
			SupportedLock:           defaultSupportedLock(),
			ACL:                     &aclProp{},
			SupportedPrivilegeSet:   defaultSupportedPrivilegeSet(),
			PrincipalCollectionSet:  &hrefListProp{Href: []string{"/dav/principals/"}},
		},
		Status: httpStatusOK,
	}}}

	filtered := filterNonPrincipalPropfindResponse(resp, &propfindRequest{Prop: &propfindPropQuery{
		DisplayName:             &struct{}{},
		ResourceType:            &struct{}{},
		PrincipalURL:            &struct{}{},
		ACLProp:                 &struct{}{},
		GetETag:                 &struct{}{},
		CalendarTimezone:        &struct{}{},
		ScheduleCalendarTransp:  &struct{}{},
		CurrentUserPrivilegeSet: &struct{}{},
		Owner:                   &struct{}{},
	}})

	if len(filtered.Propstat) != 2 {
		t.Fatalf("expected mixed principal response to include 200 and 404 propstats, got %#v", filtered.Propstat)
	}
	okStat := propstatWithStatus(filtered.Propstat, httpStatusOK)
	if okStat == nil || okStat.Prop.PrincipalURL == nil || okStat.Prop.ACL == nil {
		t.Fatalf("expected supported principal props in 200 propstat, got %#v", okStat)
	}
	notFound := propstatNotFoundNameSet(filtered.Propstat)
	for _, name := range []string{"d:getetag", "cal:calendar-timezone", "cal:schedule-calendar-transp", "d:owner"} {
		if !notFound[xml.Name{Local: name}] {
			t.Fatalf("expected %s in 404 propstat names, got %#v", name, notFound)
		}
	}
}

func TestFilterAddressBookCollectionPropfindResponseSupportsMixedRequests(t *testing.T) {
	resp := response{Href: "/dav/addressbooks/5/", Propstat: []propstat{{
		Prop: prop{
			DisplayName:                "Contacts",
			ResourceType:               resourceType{Collection: &struct{}{}, AddressBook: &struct{}{}},
			AddressBookDesc:            "Shared contacts",
			SupportedAddressData:       supportedAddressDataProp(),
			AddressBookMaxResourceSize: "1024",
			SupportedCollationSet:      supportedCollationSetProp(),
			SyncToken:                  "sync-token",
			CTag:                       "5",
			SupportedReportSet:         addressbookSupportedReports(),
			ACL:                        &aclProp{},
		},
		Status: httpStatusOK,
	}}}

	filtered := filterNonPrincipalPropfindResponse(resp, &propfindRequest{Prop: &propfindPropQuery{
		DisplayName:          &struct{}{},
		SupportedAddressData: &struct{}{},
		ACLProp:              &struct{}{},
		GetETag:              &struct{}{},
		PrincipalURL:         &struct{}{},
		CalendarTimezone:     &struct{}{},
		CalendarHomeSet:      &struct{}{},
	}})

	if len(filtered.Propstat) != 2 {
		t.Fatalf("expected mixed address book response to include 200 and 404 propstats, got %#v", filtered.Propstat)
	}
	okStat := propstatWithStatus(filtered.Propstat, httpStatusOK)
	if okStat == nil || okStat.Prop.SupportedAddressData == nil || okStat.Prop.ACL == nil {
		t.Fatalf("expected supported address book props in 200 propstat, got %#v", okStat)
	}
	notFound := propstatNotFoundNameSet(filtered.Propstat)
	for _, name := range []string{"d:getetag", "d:principal-URL", "cal:calendar-timezone", "cal:calendar-home-set"} {
		if !notFound[xml.Name{Local: name}] {
			t.Fatalf("expected %s in 404 propstat names, got %#v", name, notFound)
		}
	}
}

func TestFilterCalendarCollectionPropfindResponseSupportsMixedRequests(t *testing.T) {
	resp := response{Href: "/dav/calendars/1/", Propstat: []propstat{{
		Prop: prop{
			DisplayName:             "Calendar",
			ResourceType:            resourceType{Collection: &struct{}{}, Calendar: &struct{}{}},
			CalendarDescription:     "Primary calendar",
			CalendarTimezone:        stringPtr("BEGIN:VTIMEZONE\r\nEND:VTIMEZONE\r\n"),
			SyncToken:               "sync-token",
			CTag:                    "9",
			ScheduleCalendarTransp:  &scheduleCalendarTransp{Opaque: &struct{}{}},
			CurrentUserPrivilegeSet: calendarCurrentUserPrivilegeSet(true),
			ACL:                     &aclProp{},
		},
		Status: httpStatusOK,
	}}}

	filtered := filterNonPrincipalPropfindResponse(resp, &propfindRequest{Prop: &propfindPropQuery{
		DisplayName:             &struct{}{},
		CalendarDescription:     &struct{}{},
		CalendarTimezone:        &struct{}{},
		CalendarColor:           &struct{}{},
		ScheduleCalendarTransp:  &struct{}{},
		CurrentUserPrivilegeSet: &struct{}{},
		ACLProp:                 &struct{}{},
		GetETag:                 &struct{}{},
		AddressBookDesc:         &struct{}{},
		PrincipalURL:            &struct{}{},
		Owner:                   &struct{}{},
	}})

	if len(filtered.Propstat) != 2 {
		t.Fatalf("expected mixed calendar response to include 200 and 404 propstats, got %#v", filtered.Propstat)
	}
	okStat := propstatWithStatus(filtered.Propstat, httpStatusOK)
	if okStat == nil || okStat.Prop.ScheduleCalendarTransp == nil || okStat.Prop.CurrentUserPrivilegeSet == nil {
		t.Fatalf("expected supported calendar props in 200 propstat, got %#v", okStat)
	}
	notFound := propstatNotFoundNameSet(filtered.Propstat)
	// calendar-color was requested but is unset on this calendar: 404, not junk.
	for _, name := range []string{"d:getetag", "card:addressbook-description", "d:principal-URL", "d:owner", "ical:calendar-color"} {
		if !notFound[xml.Name{Local: name}] {
			t.Fatalf("expected %s in 404 propstat names, got %#v", name, notFound)
		}
	}
}

// A17 regression: 404 propstats must contain empty property elements, not
// sentinel values like <d:getetag>getetag</d:getetag>.
func TestPropfindNotFoundPropstatHasNoSentinelValues(t *testing.T) {
	now := store.Now()
	calRepo := &fakeCalendarRepo{
		accessible: []store.CalendarAccess{
			{Calendar: store.Calendar{ID: 2, UserID: 1, Name: "Work", UpdatedAt: now}, Editor: true},
		},
		calendars: map[int64]*store.Calendar{
			2: {ID: 2, UserID: 1, Name: "Work", UpdatedAt: now},
		},
	}
	h := &DavServer{store: &store.Store{Calendars: calRepo}}

	body := `<?xml version="1.0"?><d:propfind xmlns:d="DAV:"><d:prop><d:displayname/><d:getetag/></d:prop></d:propfind>`
	req := httptest.NewRequest("PROPFIND", "/dav/calendars/2/", strings.NewReader(body))
	req.Header.Set("Depth", "0")
	req = req.WithContext(auth.WithUser(req.Context(), &store.User{ID: 1}))
	rr := httptest.NewRecorder()
	h.Propfind(rr, req)

	if rr.Code != http.StatusMultiStatus {
		t.Fatalf("expected 207, got %d: %s", rr.Code, rr.Body.String())
	}
	got := rr.Body.String()
	if !strings.Contains(got, "<d:getetag></d:getetag>") {
		t.Fatalf("expected empty getetag element in 404 propstat, got %s", got)
	}
	if strings.Contains(got, ">getetag</d:getetag>") && strings.Contains(got, "<d:getetag>getetag") {
		t.Fatalf("404 propstat still carries sentinel value, got %s", got)
	}
	if !strings.Contains(got, "Work") {
		t.Fatalf("expected displayname value in 200 propstat, got %s", got)
	}
}

// A4 regression: <propname/> must return property names without values.
func TestPropfindPropnameReturnsNamesWithoutValues(t *testing.T) {
	now := store.Now()
	calRepo := &fakeCalendarRepo{
		accessible: []store.CalendarAccess{
			{Calendar: store.Calendar{ID: 2, UserID: 1, Name: "Work", UpdatedAt: now}, Editor: true},
		},
		calendars: map[int64]*store.Calendar{
			2: {ID: 2, UserID: 1, Name: "Work", UpdatedAt: now},
		},
	}
	h := &DavServer{store: &store.Store{Calendars: calRepo}}

	body := `<?xml version="1.0"?><d:propfind xmlns:d="DAV:"><d:propname/></d:propfind>`
	req := httptest.NewRequest("PROPFIND", "/dav/calendars/2/", strings.NewReader(body))
	req.Header.Set("Depth", "0")
	req = req.WithContext(auth.WithUser(req.Context(), &store.User{ID: 1}))
	rr := httptest.NewRecorder()
	h.Propfind(rr, req)

	if rr.Code != http.StatusMultiStatus {
		t.Fatalf("expected 207, got %d: %s", rr.Code, rr.Body.String())
	}
	got := rr.Body.String()
	if !strings.Contains(got, "<d:displayname></d:displayname>") {
		t.Fatalf("expected empty displayname element for propname, got %s", got)
	}
	if strings.Contains(got, "Work") {
		t.Fatalf("propname response must not contain property values, got %s", got)
	}
}
