package dav

import (
	"context"
	"testing"

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

func TestCanReadAddressBookContactBranches(t *testing.T) {
	book := &store.AddressBook{ID: 5, UserID: 1, Name: "Contacts"}

	t.Run("blank resource name is skipped", func(t *testing.T) {
		h := &Handler{}
		allowed, err := h.canReadAddressBookContact(context.Background(), &store.User{ID: 2}, book, " ")
		if err != nil {
			t.Fatalf("canReadAddressBookContact returned error: %v", err)
		}
		if allowed {
			t.Fatal("expected blank resource name to be rejected")
		}
	})

	t.Run("owner is allowed", func(t *testing.T) {
		h := &Handler{}
		allowed, err := h.canReadAddressBookContact(context.Background(), &store.User{ID: 1}, book, "alice")
		if err != nil {
			t.Fatalf("canReadAddressBookContact returned error: %v", err)
		}
		if !allowed {
			t.Fatal("expected owner to be allowed")
		}
	})

	t.Run("explicit deny returns false", func(t *testing.T) {
		h := &Handler{store: &store.Store{ACLEntries: &fakeACLRepo{entries: []store.ACLEntry{
			{ResourcePath: "/dav/addressbooks/5/alice", PrincipalHref: "/dav/principals/2/", Privilege: "read", IsGrant: false},
		}}}}
		allowed, err := h.canReadAddressBookContact(context.Background(), &store.User{ID: 2}, book, "alice")
		if err != nil {
			t.Fatalf("canReadAddressBookContact returned error: %v", err)
		}
		if allowed {
			t.Fatal("expected denied contact read to be filtered")
		}
	})
}

func TestFilterReadableAddressBookContactsFiltersDeniedContacts(t *testing.T) {
	book := &store.AddressBook{ID: 5, UserID: 1, Name: "Contacts"}
	h := &Handler{store: &store.Store{ACLEntries: &fakeACLRepo{entries: []store.ACLEntry{
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
