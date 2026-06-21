package dav

import (
	"context"
	"fmt"
	"testing"

	"github.com/jw6ventures/calcard/internal/acl"
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

func TestPropstatForAddressObjectPropfindCoversSupportedAndMissingProperties(t *testing.T) {
	src := prop{
		GetETag:                `"etag-alice"`,
		GetContentType:         "text/vcard; charset=utf-8",
		AddressData:            cdataString(buildVCard("3.0", "UID:alice", "FN:Alice Example", "EMAIL:alice@example.com")),
		SupportedReportSet:     addressbookSupportedReports(),
		LockDiscovery:          &lockDiscoveryProp{},
		SupportedLock:          defaultSupportedLock(),
		ACL:                    &aclProp{},
		SupportedPrivilegeSet:  defaultSupportedPrivilegeSet(),
		PrincipalCollectionSet: &hrefListProp{Href: []string{"/dav/principals/"}},
	}

	req := &propfindPropQuery{
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
	}

	stats := propstatForAddressObjectPropfind(req, src)
	if len(stats) != 2 {
		t.Fatalf("expected 2 propstats, got %d", len(stats))
	}
	okStat := propstatWithStatus(stats, httpStatusOK)
	if okStat == nil {
		t.Fatal("expected 200 propstat")
	}
	if string(okStat.Prop.AddressData) == "" || okStat.Prop.SupportedLock == nil || okStat.Prop.ACL == nil {
		t.Fatalf("expected supported properties in 200 propstat, got %#v", okStat.Prop)
	}
	notFound := propstatWithStatus(stats, httpStatusNotFound)
	if notFound == nil || notFound.Prop.DisplayName != "displayname" {
		t.Fatalf("expected displayname in 404 propstat, got %#v", notFound)
	}

	emptyStats := propstatForAddressObjectPropfind(&propfindPropQuery{}, src)
	if len(emptyStats) != 1 || emptyStats[0].Status != httpStatusOK {
		t.Fatalf("expected empty request to produce empty 200 propstat, got %#v", emptyStats)
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

	filtered := filterPrincipalPropfindResponse(resp, &propfindRequest{Prop: &propfindPropQuery{
		DisplayName:                   &struct{}{},
		ResourceType:                  &struct{}{},
		CurrentUserPrincipal:          &struct{}{},
		CurrentUserPrincipalURL:       &struct{}{},
		PrincipalURL:                  &struct{}{},
		CalendarHomeSet:               &struct{}{},
		AddressbookHomeSet:            &struct{}{},
		SupportedReportSet:            &struct{}{},
		LockDiscovery:                 &struct{}{},
		SupportedLock:                 &struct{}{},
		ACLProp:                       &struct{}{},
		SupportedPrivilegeSet:         &struct{}{},
		PrincipalCollectionSet:        &struct{}{},
		GetETag:                       &struct{}{},
		GetContentType:                &struct{}{},
		CalendarData:                  &struct{}{},
		AddressData:                   &addressDataQuery{},
		CalendarDescription:           &struct{}{},
		CalendarTimezone:              &struct{}{},
		AddressBookDesc:               &struct{}{},
		SupportedAddressData:          &struct{}{},
		AddressBookMaxResourceSize:    &struct{}{},
		SupportedCollationSet:         &struct{}{},
		SyncToken:                     &struct{}{},
		CTag:                          &struct{}{},
		PrincipalAddress:              &struct{}{},
		SupportedCalendarComponentSet: &struct{}{},
		MaxResourceSize:               &struct{}{},
		MinDateTime:                   &struct{}{},
		MaxDateTime:                   &struct{}{},
		MaxInstances:                  &struct{}{},
		MaxAttendeesPerInstance:       &struct{}{},
		ScheduleCalendarTransp:        &struct{}{},
		SupportedCalendarData:         &struct{}{},
		CalendarServerReadOnly:        &struct{}{},
		CurrentUserPrivilegeSet:       &struct{}{},
		Owner:                         &struct{}{},
	}})

	if len(filtered.Propstat) != 2 {
		t.Fatalf("expected mixed principal response to include 200 and 404 propstats, got %#v", filtered.Propstat)
	}
	okStat := propstatWithStatus(filtered.Propstat, httpStatusOK)
	if okStat == nil || okStat.Prop.PrincipalURL == nil || okStat.Prop.ACL == nil {
		t.Fatalf("expected supported principal props in 200 propstat, got %#v", okStat)
	}
	notFound := propstatWithStatus(filtered.Propstat, httpStatusNotFound)
	if notFound == nil || notFound.Prop.CalendarTimezone == nil || notFound.Prop.ScheduleCalendarTransp == nil {
		t.Fatalf("expected unsupported principal props in 404 propstat, got %#v", notFound)
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
			CurrentUserPrincipal:       &expandableHrefProp{Href: "/dav/principals/1/"},
			CurrentUserPrincipalURL:    &hrefProp{Href: "/dav/principals/1/"},
			AddressbookHomeSet:         &hrefListProp{Href: []string{"/dav/addressbooks/"}},
			SupportedReportSet:         addressbookSupportedReports(),
			LockDiscovery:              &lockDiscoveryProp{},
			SupportedLock:              defaultSupportedLock(),
			ACL:                        &aclProp{},
			SupportedPrivilegeSet:      defaultSupportedPrivilegeSet(),
			PrincipalCollectionSet:     &hrefListProp{Href: []string{"/dav/principals/"}},
		},
		Status: httpStatusOK,
	}}}

	filtered := filterAddressBookCollectionPropfindResponse(resp, &propfindRequest{Prop: &propfindPropQuery{
		DisplayName:                   &struct{}{},
		ResourceType:                  &struct{}{},
		AddressBookDesc:               &struct{}{},
		SupportedAddressData:          &struct{}{},
		AddressBookMaxResourceSize:    &struct{}{},
		SupportedCollationSet:         &struct{}{},
		SyncToken:                     &struct{}{},
		CTag:                          &struct{}{},
		CurrentUserPrincipal:          &struct{}{},
		CurrentUserPrincipalURL:       &struct{}{},
		AddressbookHomeSet:            &struct{}{},
		SupportedReportSet:            &struct{}{},
		LockDiscovery:                 &struct{}{},
		SupportedLock:                 &struct{}{},
		ACLProp:                       &struct{}{},
		SupportedPrivilegeSet:         &struct{}{},
		PrincipalCollectionSet:        &struct{}{},
		GetETag:                       &struct{}{},
		GetContentType:                &struct{}{},
		CalendarData:                  &struct{}{},
		AddressData:                   &addressDataQuery{},
		CalendarDescription:           &struct{}{},
		CalendarTimezone:              &struct{}{},
		PrincipalURL:                  &struct{}{},
		CalendarHomeSet:               &struct{}{},
		PrincipalAddress:              &struct{}{},
		SupportedCalendarComponentSet: &struct{}{},
		MaxResourceSize:               &struct{}{},
		MinDateTime:                   &struct{}{},
		MaxDateTime:                   &struct{}{},
		MaxInstances:                  &struct{}{},
		MaxAttendeesPerInstance:       &struct{}{},
		ScheduleCalendarTransp:        &struct{}{},
		SupportedCalendarData:         &struct{}{},
		CalendarServerReadOnly:        &struct{}{},
		CurrentUserPrivilegeSet:       &struct{}{},
		Owner:                         &struct{}{},
	}})

	if len(filtered.Propstat) != 2 {
		t.Fatalf("expected mixed address book response to include 200 and 404 propstats, got %#v", filtered.Propstat)
	}
	okStat := propstatWithStatus(filtered.Propstat, httpStatusOK)
	if okStat == nil || okStat.Prop.SupportedAddressData == nil || okStat.Prop.ACL == nil {
		t.Fatalf("expected supported address book props in 200 propstat, got %#v", okStat)
	}
	notFound := propstatWithStatus(filtered.Propstat, httpStatusNotFound)
	if notFound == nil || notFound.Prop.PrincipalURL == nil || notFound.Prop.CalendarTimezone == nil {
		t.Fatalf("expected unsupported address book props in 404 propstat, got %#v", notFound)
	}
}

func TestFilterCalendarCollectionPropfindResponseSupportsMixedRequests(t *testing.T) {
	resp := response{Href: "/dav/calendars/1/", Propstat: []propstat{{
		Prop: prop{
			DisplayName:                   "Calendar",
			ResourceType:                  resourceType{Collection: &struct{}{}, Calendar: &struct{}{}},
			CalendarDescription:           "Primary calendar",
			CalendarTimezone:              stringPtr("BEGIN:VTIMEZONE\r\nEND:VTIMEZONE\r\n"),
			SyncToken:                     "sync-token",
			CTag:                          "9",
			CurrentUserPrincipal:          &expandableHrefProp{Href: "/dav/principals/1/"},
			CurrentUserPrincipalURL:       &hrefProp{Href: "/dav/principals/1/"},
			SupportedReportSet:            calendarSupportedReports(),
			SupportedCalendarComponentSet: supportedCalendarComponents(),
			MaxResourceSize:               "10485760",
			MinDateTime:                   caldavMinDateTime,
			MaxDateTime:                   caldavMaxDateTime,
			MaxInstances:                  "1000",
			MaxAttendeesPerInstance:       "100",
			ScheduleCalendarTransp:        &scheduleCalendarTransp{Opaque: &struct{}{}},
			SupportedCalendarData:         supportedCalendarDataProp(),
			CalendarServerReadOnly:        &struct{}{},
			CurrentUserPrivilegeSet:       calendarCurrentUserPrivilegeSet(true),
			LockDiscovery:                 &lockDiscoveryProp{},
			SupportedLock:                 defaultSupportedLock(),
			ACL:                           &aclProp{},
			SupportedPrivilegeSet:         defaultSupportedPrivilegeSet(),
			PrincipalCollectionSet:        &hrefListProp{Href: []string{"/dav/principals/"}},
		},
		Status: httpStatusOK,
	}}}

	filtered := filterCalendarCollectionPropfindResponse(resp, &propfindRequest{Prop: &propfindPropQuery{
		DisplayName:                   &struct{}{},
		ResourceType:                  &struct{}{},
		CalendarDescription:           &struct{}{},
		CalendarTimezone:              &struct{}{},
		SyncToken:                     &struct{}{},
		CTag:                          &struct{}{},
		CurrentUserPrincipal:          &struct{}{},
		CurrentUserPrincipalURL:       &struct{}{},
		SupportedReportSet:            &struct{}{},
		SupportedCalendarComponentSet: &struct{}{},
		MaxResourceSize:               &struct{}{},
		MinDateTime:                   &struct{}{},
		MaxDateTime:                   &struct{}{},
		MaxInstances:                  &struct{}{},
		MaxAttendeesPerInstance:       &struct{}{},
		ScheduleCalendarTransp:        &struct{}{},
		SupportedCalendarData:         &struct{}{},
		CalendarServerReadOnly:        &struct{}{},
		CurrentUserPrivilegeSet:       &struct{}{},
		LockDiscovery:                 &struct{}{},
		SupportedLock:                 &struct{}{},
		ACLProp:                       &struct{}{},
		SupportedPrivilegeSet:         &struct{}{},
		PrincipalCollectionSet:        &struct{}{},
		GetETag:                       &struct{}{},
		GetContentType:                &struct{}{},
		CalendarData:                  &struct{}{},
		AddressData:                   &addressDataQuery{},
		AddressBookDesc:               &struct{}{},
		SupportedAddressData:          &struct{}{},
		AddressBookMaxResourceSize:    &struct{}{},
		SupportedCollationSet:         &struct{}{},
		PrincipalURL:                  &struct{}{},
		CalendarHomeSet:               &struct{}{},
		AddressbookHomeSet:            &struct{}{},
		PrincipalAddress:              &struct{}{},
		Owner:                         &struct{}{},
	}})

	if len(filtered.Propstat) != 2 {
		t.Fatalf("expected mixed calendar response to include 200 and 404 propstats, got %#v", filtered.Propstat)
	}
	okStat := propstatWithStatus(filtered.Propstat, httpStatusOK)
	if okStat == nil || okStat.Prop.ScheduleCalendarTransp == nil || okStat.Prop.CurrentUserPrivilegeSet == nil {
		t.Fatalf("expected supported calendar props in 200 propstat, got %#v", okStat)
	}
	notFound := propstatWithStatus(filtered.Propstat, httpStatusNotFound)
	if notFound == nil || notFound.Prop.AddressBookDesc == "" || notFound.Prop.PrincipalURL == nil {
		t.Fatalf("expected unsupported calendar props in 404 propstat, got %#v", notFound)
	}
}
