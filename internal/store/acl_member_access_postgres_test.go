package store

import (
	"context"
	"fmt"
	"testing"
)

// ListByPrincipals reads the entries naming any of the hrefs it is given in
// one query, in the order ListByPrincipal reads one principal's.
func TestPostgres_ListByPrincipalsReadsEveryNamedPrincipal(t *testing.T) {
	s := newPostgresStore(t)
	ctx := context.Background()
	for _, set := range []struct {
		path    string
		entries []ACLEntry
	}{
		{"/dav/calendars/1", []ACLEntry{
			{PrincipalHref: "/dav/principals/2", IsGrant: true, Privilege: "read"},
			{PrincipalHref: "/dav/principals/3/", IsGrant: true, Privilege: "read"},
			{PrincipalHref: "/dav/principals/2/", IsGrant: false, Privilege: "write", Position: 1},
		}},
		{"/dav/addressbooks/1", []ACLEntry{{PrincipalHref: "DAV:all", IsGrant: true, Privilege: "read"}}},
	} {
		if err := s.ACLEntries.SetACL(ctx, set.path, set.entries); err != nil {
			t.Fatal(err)
		}
	}
	got, err := s.ACLEntries.ListByPrincipals(ctx, []string{"/dav/principals/2/", "/dav/principals/2", "DAV:all"})
	if err != nil {
		t.Fatal(err)
	}
	var listed []string
	for _, entry := range got {
		listed = append(listed, fmt.Sprintf("%s %s %t %s", entry.ResourcePath, entry.PrincipalHref, entry.IsGrant, entry.Privilege))
	}
	want := []string{
		"/dav/addressbooks/1 DAV:all true read",
		"/dav/calendars/1 /dav/principals/2 true read",
		"/dav/calendars/1 /dav/principals/2/ false write",
	}
	if fmt.Sprint(listed) != fmt.Sprint(want) {
		t.Fatalf("ListByPrincipals() = %q, want %q", listed, want)
	}
	if none, err := s.ACLEntries.ListByPrincipals(ctx, nil); err != nil || len(none) != 0 {
		t.Fatalf("ListByPrincipals(nil) = %v, %v, want nothing", none, err)
	}
}

// A calendar the user holds nothing on is listed as accessible through a
// grant on one of its events, and that event's grant is read from the entries
// at its own state path alone: an entry at another member's path, a sibling
// spelling of its name, neither refuses nor grants it, whichever entry is
// older.
func TestPostgres_ACalendarSharedThroughAnEventReadsOnlyThatEventsEntries(t *testing.T) {
	s := newPostgresStore(t)
	ctx := context.Background()
	owner, err := s.Users.UpsertOAuthUser(ctx, "owner", "owner@example.test", "Owner", "Owner")
	if err != nil {
		t.Fatal(err)
	}
	sharee, err := s.Users.UpsertOAuthUser(ctx, "sharee", "sharee@example.test", "Sharee", "Sharee")
	if err != nil {
		t.Fatal(err)
	}
	shareeHref := fmt.Sprintf("/dav/principals/%d/", sharee.ID)
	for _, tc := range []struct{ granted, denied string }{
		{granted: "X", denied: "X.ics"},
		{granted: "X.ics", denied: "X"},
	} {
		t.Run("grant on "+tc.granted, func(t *testing.T) {
			cal, err := s.Calendars.Create(ctx, Calendar{UserID: owner.ID, Name: "Shared through " + tc.granted})
			if err != nil {
				t.Fatal(err)
			}
			collectionPath := fmt.Sprintf("/dav/calendars/%d", cal.ID)
			for _, name := range []string{"X", "X.ics"} {
				uid := name + "-uid"
				if _, err := s.Events.Upsert(ctx, Event{CalendarID: cal.ID, UID: uid, ResourceName: name, RawICAL: postgresCalendarObject(uid, name), ETag: uid}); err != nil {
					t.Fatal(err)
				}
			}
			// The refusal is stored first, so it is the older entry and would
			// decide first were it read as the granted event's.
			if err := s.ACLEntries.SetACL(ctx, MemberStatePath(collectionPath, tc.denied), []ACLEntry{{PrincipalHref: shareeHref, IsGrant: false, Privilege: "all"}}); err != nil {
				t.Fatal(err)
			}
			if err := s.ACLEntries.SetACL(ctx, MemberStatePath(collectionPath, tc.granted), []ACLEntry{{PrincipalHref: shareeHref, IsGrant: true, Privilege: "read"}}); err != nil {
				t.Fatal(err)
			}
			access, err := s.Calendars.GetAccessible(ctx, cal.ID, sharee.ID)
			if err != nil {
				t.Fatal(err)
			}
			if access == nil {
				t.Fatalf("GetAccessible() = nil, want the calendar its event %q is shared through", tc.granted)
			}
			listed, err := s.Calendars.ListAccessible(ctx, sharee.ID)
			if err != nil {
				t.Fatal(err)
			}
			found := false
			for _, candidate := range listed {
				found = found || candidate.ID == cal.ID
			}
			if !found {
				t.Fatalf("ListAccessible() omits the calendar its event %q is shared through", tc.granted)
			}
		})
	}
}
