package dav

import (
	"context"
	"net/http/httptest"
	"path"
	"sort"
	"strings"
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

// The PROPFIND prefetch serves each member the entries at its own state path.
// The entries at meeting.ics belong to the member of that name, not to the
// member meeting a request for /meeting.ics names.
func TestPropfindACLPrefetchServesEachMemberItsOwnPath(t *testing.T) {
	user := &store.User{ID: 1}
	acls := &fakeACLRepo{entries: []store.ACLEntry{
		{
			ResourcePath:  "/dav/calendars/2/meeting.ics",
			PrincipalHref: "/dav/principals/3/",
			IsGrant:       true,
			Privilege:     "read",
		},
		{
			ResourcePath:  "/dav/calendars/2/meeting",
			PrincipalHref: "/dav/principals/4/",
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
	if len(entries) != 1 || entries[0].ResourcePath != "/dav/calendars/2/meeting" {
		t.Fatalf("aclEntriesForResource() = %#v, want only the entry at meeting's own path", entries)
	}
	if acls.listByResourcesCalls != 1 || acls.listByResourceCalls != 0 {
		t.Fatalf("ACL calls: batch=%d single=%d, want 1 and 0", acls.listByResourcesCalls, acls.listByResourceCalls)
	}
}

// A member's ACL is the entries stored under its canonical path, the path the
// ACL method writes, and under no other spelling of its name: with or without
// an extension, in any case, trimmed or not, each names another resource. The
// batched listing decision and a request naming the member both read exactly
// that path, so they agree with each other and with the ACL the method set.
func TestMemberDecisionsReadOnlyTheMembersOwnPath(t *testing.T) {
	user := &store.User{ID: 1}
	principal := "/dav/principals/1/"
	cal := &store.Calendar{ID: 2, UserID: 9}
	book := &store.AddressBook{ID: 3, UserID: 9}
	domains := []struct {
		collectionPath string
		ext            string
		perRequest     func(h *DavServer, requestPath, privilege string) (bool, bool)
		batched        func(decider *batchedObjectACLDecider, name, privilege string) (bool, bool)
		// collectionOnly is the decision on write-content when only the
		// collection's read grant applies.
		collectionOnly [2]bool
	}{
		{
			"/dav/calendars/2", ".ics",
			func(h *DavServer, requestPath, privilege string) (bool, bool) {
				pc, err := h.calendarPrivilegeContextFor(context.Background(), user, cal, requestPath)
				if err != nil {
					t.Fatal(err)
				}
				return pc.decide(privilege)
			},
			func(decider *batchedObjectACLDecider, name, privilege string) (bool, bool) {
				return calendarPrivilegeDecisionWithDecider(&store.CalendarAccess{Calendar: *cal}, name, privilege, decider)
			},
			[2]bool{false, true},
		},
		{
			"/dav/addressbooks/3", ".vcf",
			func(h *DavServer, requestPath, privilege string) (bool, bool) {
				pc, err := h.addressBookPrivilegeContextFor(context.Background(), user, book, requestPath)
				if err != nil {
					t.Fatal(err)
				}
				return pc.decide(privilege)
			},
			func(decider *batchedObjectACLDecider, name, privilege string) (bool, bool) {
				allowed, denied, _ := decider.decide(name, privilege)
				return allowed, denied
			},
			[2]bool{false, false},
		},
	}
	for _, domain := range domains {
		ext, upper := domain.ext, strings.ToUpper(domain.ext)
		members := []string{
			"plain", "dav" + ext, "dav" + upper, "uid@example.com", "dotted.name", "trailing ", " leading", "a.b.c" + ext,
			// No request can name a member whose stored name holds path
			// segments, but its listing must not decide by another resource.
			"x/../../5",
		}
		for _, name := range members {
			base := strings.TrimSuffix(name, path.Ext(name))
			spellings := []string{name, name + ext, name + upper, base, base + ext, base + upper, strings.TrimSpace(name), "../5", "5"}
			for _, spelling := range spellings {
				for _, isGrant := range []bool{true, false} {
					entries := []store.ACLEntry{
						{ID: 1, ResourcePath: domain.collectionPath, PrincipalHref: principal, IsGrant: true, Privilege: "read"},
						{ID: 2, ResourcePath: domain.collectionPath + "/" + spelling, PrincipalHref: principal, IsGrant: isGrant, Privilege: "read"},
						{ID: 3, ResourcePath: domain.collectionPath + "/" + spelling, PrincipalHref: principal, IsGrant: isGrant, Privilege: "write-content", Position: 1},
						{ID: 4, ResourcePath: path.Dir(domain.collectionPath) + "/5", PrincipalHref: principal, IsGrant: false, Privilege: "read"},
					}
					applies := spelling == name
					want := map[string][2]bool{"read": {true, false}, "write-content": domain.collectionOnly}
					if applies {
						want["read"] = [2]bool{isGrant, !isGrant}
						want["write-content"] = [2]bool{isGrant, !isGrant}
					}
					h := &DavServer{store: &store.Store{ACLEntries: &fakeACLRepo{entries: entries}}}
					prefetched, err := h.prefetchACLEntries(context.Background(), user, []string{domain.collectionPath, objectStatePath(domain.collectionPath, name)})
					if err != nil {
						t.Fatal(err)
					}
					decider := newBatchedObjectACLDecider(user, 9, domain.collectionPath, prefetched)
					for privilege, decision := range want {
						if allowed, denied := domain.batched(decider, name, privilege); allowed != decision[0] || denied != decision[1] {
							t.Errorf("%s member %q with grant=%v ACL at %q: batched %s = (%v, %v), want (%v, %v)",
								domain.collectionPath, name, isGrant, spelling, privilege, allowed, denied, decision[0], decision[1])
						}
						if strings.Contains(name, "/") {
							continue
						}
						requestPath := domain.collectionPath + "/" + name + ext
						if allowed, denied := domain.perRequest(h, requestPath, privilege); allowed != decision[0] || denied != decision[1] {
							t.Errorf("%s member %q with grant=%v ACL at %q: a request decides %s (%v, %v), want (%v, %v)",
								domain.collectionPath, name, isGrant, spelling, privilege, allowed, denied, decision[0], decision[1])
						}
					}
				}
			}
		}
	}
}

// A member's ACL is the entries at its own path in ACE order, however the rows
// were saved, and the entries of its siblings X, X.ics and X.ics's other
// spellings take no part. Both the batched listing decision and a request
// naming the member read it so.
func TestMemberACLReadsOnlyItsOwnEntriesInACEOrder(t *testing.T) {
	user := &store.User{ID: 1}
	principal := "/dav/principals/1/"
	cal := &store.Calendar{ID: 2, UserID: 9}
	entries := []store.ACLEntry{
		{ID: 1, ResourcePath: "/dav/calendars/2/X.ics", PrincipalHref: principal, IsGrant: true, Privilege: "read", Position: 0},
		{ID: 2, ResourcePath: "/dav/calendars/2/X.ICS", PrincipalHref: principal, IsGrant: true, Privilege: "read", Position: 2},
		{ID: 9, ResourcePath: "/dav/calendars/2/X.ICS", PrincipalHref: principal, IsGrant: false, Privilege: "read", Position: 1},
		{ID: 3, ResourcePath: "/dav/calendars/2/X", PrincipalHref: principal, IsGrant: true, Privilege: "write-content", Position: 0},
		{ID: 4, ResourcePath: "/dav/calendars/2/X.ics", PrincipalHref: principal, IsGrant: false, Privilege: "write-content", Position: 1},
	}
	h := &DavServer{store: &store.Store{ACLEntries: &fakeACLRepo{entries: entries}}}
	prefetched, err := h.prefetchACLEntries(context.Background(), user, []string{"/dav/calendars/2", objectStatePath("/dav/calendars/2", "X.ICS")})
	if err != nil {
		t.Fatal(err)
	}
	decider := newBatchedObjectACLDecider(user, 9, "/dav/calendars/2", prefetched)
	pc, err := h.calendarPrivilegeContextFor(context.Background(), user, cal, "/dav/calendars/2/X.ICS.ics")
	if err != nil {
		t.Fatal(err)
	}
	// read: X.ICS's own ACEs decide, the deny at position 1 ahead of its
	// grant at 2, whatever the sibling's position-0 grant says. write-content:
	// only the siblings X and X.ics decide it, so X.ICS's ACL leaves it
	// refused.
	for privilege, want := range map[string][2]bool{"read": {false, true}, "write-content": {false, true}} {
		if allowed, denied := calendarPrivilegeDecisionWithDecider(&store.CalendarAccess{Calendar: *cal}, "X.ICS", privilege, decider); allowed != want[0] || denied != want[1] {
			t.Errorf("batched %s = (%v, %v), want (%v, %v)", privilege, allowed, denied, want[0], want[1])
		}
		if allowed, denied := pc.decide(privilege); allowed != want[0] || denied != want[1] {
			t.Errorf("a request decides %s (%v, %v), want (%v, %v)", privilege, allowed, denied, want[0], want[1])
		}
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

func TestBatchedCalendarFilteringUsesFirstMatchingACE(t *testing.T) {
	cal := &store.CalendarAccess{
		Calendar: store.Calendar{ID: 2, UserID: 9, Name: "Shared"},
		Shared:   true, PrivilegesResolved: true,
	}
	entries := []store.ACLEntry{
		{ResourcePath: "/dav/calendars/2/visible", PrincipalHref: "/dav/principals/1/", IsGrant: true, Privilege: "read", Position: 0},
		{ResourcePath: "/dav/calendars/2/visible", PrincipalHref: "/dav/principals/1/", IsGrant: false, Privilege: "read", Position: 1},
	}
	h := &DavServer{store: &store.Store{ACLEntries: &fakeACLRepo{entries: entries}}}
	events := []store.Event{{CalendarID: 2, UID: "visible", ResourceName: "visible"}}

	visible, err := h.filterReadableCalendarEvents(context.Background(), &store.User{ID: 1}, cal, events)
	if err != nil {
		t.Fatalf("filterReadableCalendarEvents() error = %v", err)
	}
	if len(visible) != 1 || visible[0].UID != "visible" {
		t.Fatalf("filterReadableCalendarEvents() = %#v, want the first granting ACE to decide", visible)
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

	request := httptest.NewRequest("REPORT", "http://example.com/dav/calendars/2/", nil)
	responses, err := h.calendarMultiGet(context.Background(), calendarReportRequest{
		user:           &store.User{ID: 1},
		cal:            cal,
		collectionPath: "/dav/calendars/2/",
		request:        request,
	}, []string{"/dav/calendars/2/visible.ics"})
	if err != nil {
		t.Fatalf("calendarMultiGet() error = %v", err)
	}
	if len(responses) != 1 || responses[0].Status == httpStatusNotFound {
		t.Fatalf("calendarMultiGet() = %#v, want collection-granted event", responses)
	}
}

func (f *fakeContactRepo) ListForBookPageAfter(ctx context.Context, bookID, afterID int64, limit int) ([]store.Contact, error) {
	f.pageLookupCount++
	if f.listPageErr != nil {
		return nil, f.listPageErr
	}
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
