package dav

import (
	"context"
	"sort"
	"testing"

	"github.com/jw6ventures/calcard/internal/store"
)

func (f *fakeACLRepo) ListByResources(_ context.Context, paths []string) ([]store.ACLEntry, error) {
	f.listByResourcesCalls++
	if f.listByResourceErr != nil {
		return nil, f.listByResourceErr
	}
	return davTestBatchEntries(f.entries, paths, nil), nil
}

func (f *fakeACLRepo) ListByResourcesAndPrincipals(_ context.Context, paths, principals []string) ([]store.ACLEntry, error) {
	f.listScopedACLCalls++
	if f.listByPrincipalErr != nil {
		return nil, f.listByPrincipalErr
	}
	return davTestBatchEntries(f.entries, paths, principals), nil
}

func davTestBatchEntries(entries []store.ACLEntry, paths, principals []string) []store.ACLEntry {
	pathSet := make(map[string]bool, len(paths))
	for _, value := range paths {
		pathSet[value] = true
	}
	principalSet := make(map[string]bool, len(principals))
	for _, value := range principals {
		principalSet[value] = true
	}
	var result []store.ACLEntry
	for _, entry := range entries {
		if !pathSet[entry.ResourcePath] || (len(principalSet) != 0 && !principalSet[entry.PrincipalHref]) {
			continue
		}
		result = append(result, entry)
	}
	return result
}

func (f *fakeAddressBookRepo) ListAccessible(ctx context.Context, userID int64) ([]store.AddressBook, error) {
	f.listAccessibleCalls++
	if f.accessibleByUser != nil {
		books := f.accessibleByUser[userID]
		return append([]store.AddressBook(nil), books...), nil
	}
	return f.ListByUser(ctx, userID)
}

func TestDiscoverySeedsCollectionRequestCaches(t *testing.T) {
	user := &store.User{ID: 1}
	calendars := &fakeCalendarRepo{accessible: []store.CalendarAccess{
		{Calendar: store.Calendar{ID: 2, UserID: 1, Name: "Work"}, Editor: true},
	}}
	books := &fakeAddressBookRepo{accessibleByUser: map[int64][]store.AddressBook{
		1: {{ID: 3, UserID: 1, Name: "Contacts"}},
	}}
	h := &DavServer{store: &store.Store{Calendars: calendars, AddressBooks: books}}
	ctx := withDAVRequestState(context.Background())

	if _, err := h.accessibleCalendars(ctx, user); err != nil {
		t.Fatalf("accessibleCalendars() error = %v", err)
	}
	if _, err := h.accessibleAddressBooks(ctx, user); err != nil {
		t.Fatalf("accessibleAddressBooks() error = %v", err)
	}
	for i := 0; i < 3; i++ {
		if _, err := h.getCalendar(ctx, 2); err != nil {
			t.Fatalf("getCalendar() error = %v", err)
		}
		if _, err := h.getAddressBook(ctx, 3); err != nil {
			t.Fatalf("getAddressBook() error = %v", err)
		}
	}
	if calendars.listAccessibleCalls != 1 || calendars.getByIDCalls != 0 {
		t.Fatalf("calendar discovery calls: ListAccessible=%d GetByID=%d", calendars.listAccessibleCalls, calendars.getByIDCalls)
	}
	if books.listAccessibleCalls != 1 || books.getByIDCalls != 0 {
		t.Fatalf("address-book discovery calls: ListAccessible=%d GetByID=%d", books.listAccessibleCalls, books.getByIDCalls)
	}
}

func TestPropfindACLPrefetchIncludesLegacyObjectPaths(t *testing.T) {
	user := &store.User{ID: 1}
	acls := &fakeACLRepo{entries: []store.ACLEntry{
		{
			ResourcePath:  "/dav/calendars/2/meeting.ics",
			PrincipalHref: "/dav/principals/3/",
			IsGrant:       true,
			Privilege:     "read",
		},
	}}
	h := &DavServer{store: &store.Store{
		Calendars: &fakeCalendarRepo{calendars: map[int64]*store.Calendar{
			2: {ID: 2, UserID: user.ID, Name: "Work"},
		}},
		ACLEntries: acls,
	}}
	ctx := withDAVRequestState(context.Background())

	if err := h.prefetchPropfindACLEntries(ctx, user, []response{{Href: "/dav/calendars/2/meeting.ics"}}); err != nil {
		t.Fatalf("prefetchPropfindACLEntries() error = %v", err)
	}
	entries, err := h.aclEntriesForResource(ctx, "/dav/calendars/2/meeting")
	if err != nil {
		t.Fatalf("aclEntriesForResource() error = %v", err)
	}
	if len(entries) != 1 || entries[0].ResourcePath != "/dav/calendars/2/meeting.ics" {
		t.Fatalf("aclEntriesForResource() = %#v, want legacy-path entry", entries)
	}
	if acls.listByResourcesCalls != 1 || acls.listByResourceCalls != 0 {
		t.Fatalf("ACL calls: batch=%d single=%d, want 1 and 0", acls.listByResourcesCalls, acls.listByResourceCalls)
	}
}

func TestBatchedCalendarFilteringFallsBackToResolvedCollectionPrivileges(t *testing.T) {
	cal := &store.CalendarAccess{
		Calendar:           store.Calendar{ID: 2, UserID: 9, Name: "Shared"},
		Shared:             true,
		Privileges:         store.CalendarPrivileges{Read: true},
		PrivilegesResolved: true,
	}
	events := []store.Event{{CalendarID: 2, UID: "visible", ResourceName: "visible"}}
	h := &DavServer{store: &store.Store{}}

	visible, err := h.filterReadableCalendarEvents(context.Background(), &store.User{ID: 1}, cal, events)
	if err != nil {
		t.Fatalf("filterReadableCalendarEvents() error = %v", err)
	}
	if len(visible) != 1 || visible[0].UID != "visible" {
		t.Fatalf("filterReadableCalendarEvents() = %#v, want collection-granted event", visible)
	}
}

func TestCalendarMultigetFallsBackToResolvedCollectionPrivileges(t *testing.T) {
	cal := &store.CalendarAccess{
		Calendar:           store.Calendar{ID: 2, UserID: 9, Name: "Shared"},
		Shared:             true,
		Privileges:         store.CalendarPrivileges{Read: true},
		PrivilegesResolved: true,
	}
	events := &fakeEventRepo{events: map[string]*store.Event{
		"2:visible": {CalendarID: 2, UID: "visible", ResourceName: "visible", ETag: "etag"},
	}}
	h := &DavServer{store: &store.Store{Events: events}}

	responses, err := h.calendarMultiGet(context.Background(), &store.User{ID: 1}, cal, []string{"/dav/calendars/2/visible.ics"}, "/dav/calendars/2/", "/dav/calendars/2/", nil, nil)
	if err != nil {
		t.Fatalf("calendarMultiGet() error = %v", err)
	}
	if len(responses) != 1 || responses[0].Status == httpStatusNotFound {
		t.Fatalf("calendarMultiGet() = %#v, want collection-granted event", responses)
	}
}

func (f *fakeContactRepo) ListForBookPageAfter(ctx context.Context, bookID, afterID int64, limit int) ([]store.Contact, error) {
	f.pageLookupCount++
	contacts, err := f.ListForBook(ctx, bookID)
	if err != nil {
		return nil, err
	}
	sort.Slice(contacts, func(i, j int) bool {
		if contacts[i].ID != contacts[j].ID {
			return contacts[i].ID < contacts[j].ID
		}
		return contactResourceName(contacts[i]) < contactResourceName(contacts[j])
	})
	var result []store.Contact
	for i := range contacts {
		if contacts[i].ID == 0 {
			contacts[i].ID = int64(i + 1)
		}
		if contacts[i].ID <= afterID {
			continue
		}
		result = append(result, contacts[i])
		if len(result) == limit {
			break
		}
	}
	return result, nil
}
