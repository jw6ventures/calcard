package store

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"
)

// An ACL write for a collection that no longer exists is refused, and leaves no
// entries behind for it.
func TestPostgres_UpdateACLAndStateRefusesADeletedCollection(t *testing.T) {
	ctx := context.Background()
	s := newPostgresStore(t)
	userID, calendarID := newPostgresCalendar(t, s, "acl-deleted")
	book, err := s.AddressBooks.Create(ctx, AddressBook{UserID: userID, Name: "Doomed"})
	if err != nil {
		t.Fatal(err)
	}
	if err := s.Calendars.Delete(ctx, userID, calendarID); err != nil {
		t.Fatal(err)
	}
	if err := s.AddressBooks.Delete(ctx, userID, book.ID); err != nil {
		t.Fatal(err)
	}
	for name, collectionPath := range map[string]string{
		"calendar":     fmt.Sprintf("/dav/calendars/%d", calendarID),
		"address book": fmt.Sprintf("/dav/addressbooks/%d", book.ID),
	} {
		t.Run(name, func(t *testing.T) {
			err := s.UpdateACLAndState(ctx, collectionPath, nil, func([]ACLEntry) ([]ACLEntry, error) {
				return []ACLEntry{{ResourcePath: collectionPath, PrincipalHref: "/dav/principals/9/", IsGrant: true, Privilege: "read"}}, nil
			})
			if !errors.Is(err, ErrNotFound) {
				t.Fatalf("UpdateACLAndState() error = %v, want ErrNotFound", err)
			}
			if entries, err := s.ACLEntries.ListByResource(ctx, collectionPath); err != nil || len(entries) != 0 {
				t.Errorf("ACL entries for the deleted collection: %v, %v", entries, err)
			}
		})
	}
}

// Revoking a principal's access deletes the locks the principal holds on the
// collection and beneath it, and leaves every other lock; a lock on the
// collection does not stop the revocation.
func TestPostgres_RevokePrincipalGrantsDeletesThePrincipalsLocks(t *testing.T) {
	ctx := context.Background()
	s := newPostgresStore(t)
	ownerID, calendarID := newPostgresCalendar(t, s, "revoke-locks-owner")
	revoked, err := s.Users.UpsertOAuthUser(ctx, "revoked", "revoked@example.test", "R", "R")
	if err != nil {
		t.Fatal(err)
	}
	collectionPath := fmt.Sprintf("/dav/calendars/%d", calendarID)
	href := fmt.Sprintf("/dav/principals/%d/", revoked.ID)
	if err := s.ACLEntries.SetACL(ctx, collectionPath, []ACLEntry{{PrincipalHref: href, IsGrant: true, Privilege: "read"}}); err != nil {
		t.Fatal(err)
	}
	lock := func(token, resourcePath string, userID int64) {
		t.Helper()
		if _, err := s.Locks.Create(ctx, Lock{Token: token, ResourcePath: resourcePath, UserID: userID, LockScope: "exclusive", LockType: "write",
			Depth: "0", TimeoutSeconds: 300, ExpiresAt: time.Now().Add(time.Hour)}); err != nil {
			t.Fatal(err)
		}
	}
	lock("opaquelocktoken:on-collection", collectionPath, revoked.ID)
	lock("opaquelocktoken:below", collectionPath+"/member", revoked.ID)
	lock("opaquelocktoken:elsewhere", fmt.Sprintf("/dav/calendars/%d", calendarID+1000), revoked.ID)
	lock("opaquelocktoken:owners", collectionPath+"/other", ownerID)

	if err := s.ACLEntries.RevokePrincipalGrants(ctx, collectionPath, href, []string{"read"}); err != nil {
		t.Fatalf("RevokePrincipalGrants() past the revoked principal's lock: %v", err)
	}
	for token, want := range map[string]bool{
		"opaquelocktoken:on-collection": false,
		"opaquelocktoken:below":         false,
		"opaquelocktoken:elsewhere":     true,
		"opaquelocktoken:owners":        true,
	} {
		held, err := s.Locks.GetByToken(ctx, token)
		if err != nil {
			t.Fatal(err)
		}
		if (held != nil) != want {
			t.Errorf("lock %s held = %t, want %t", token, held != nil, want)
		}
	}
}

// A principal's locks are deleted only when the revocation leaves them no access
// to the collection: an owner who revokes their own principal, or a principal
// another entry still lets in, keeps theirs.
func TestPostgres_RevokePrincipalGrantsKeepsTheLocksOfAPrincipalWhoStillHasAccess(t *testing.T) {
	ctx := context.Background()
	for _, tc := range []struct {
		name string
		// others are the entries at the collection besides the principal's own read grant.
		others func(principalHref string) []ACLEntry
		// ownCollection has the principal revoke themselves on a collection they own.
		ownCollection bool
		wantLockKept  bool
	}{
		{name: "a sharee nothing else admits", wantLockKept: false},
		{name: "the owner revoking their own principal", ownCollection: true, wantLockKept: true},
		{name: "a principal DAV:authenticated still admits", wantLockKept: true,
			others: func(string) []ACLEntry {
				return []ACLEntry{{PrincipalHref: "DAV:authenticated", IsGrant: true, Privilege: "read", Position: 1}}
			}},
		{name: "a principal DAV:all still admits for writing", wantLockKept: true,
			others: func(string) []ACLEntry {
				return []ACLEntry{{PrincipalHref: "DAV:all", IsGrant: true, Privilege: "write", Position: 1}}
			}},
		{name: "a principal a deny keeps out anyway", wantLockKept: false,
			others: func(string) []ACLEntry {
				return []ACLEntry{{PrincipalHref: "DAV:authenticated", IsGrant: false, Privilege: "read", Position: 1}}
			}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s := newPostgresStore(t)
			ownerID, calendarID := newPostgresCalendar(t, s, "keeps-locks-owner")
			holder, err := s.Users.UpsertOAuthUser(ctx, "holder", "holder@example.test", "H", "H")
			if err != nil {
				t.Fatal(err)
			}
			holderID := holder.ID
			if tc.ownCollection {
				holderID = ownerID
			}
			collectionPath := fmt.Sprintf("/dav/calendars/%d", calendarID)
			href := fmt.Sprintf("/dav/principals/%d/", holderID)
			entries := []ACLEntry{{PrincipalHref: href, IsGrant: true, Privilege: "read", Position: 0}}
			if tc.others != nil {
				entries = append(entries, tc.others(href)...)
			}
			if err := s.ACLEntries.SetACL(ctx, collectionPath, entries); err != nil {
				t.Fatal(err)
			}
			if _, err := s.Locks.Create(ctx, Lock{Token: "opaquelocktoken:held", ResourcePath: collectionPath, UserID: holderID, LockScope: "exclusive", LockType: "write",
				Depth: "0", TimeoutSeconds: 300, ExpiresAt: time.Now().Add(time.Hour)}); err != nil {
				t.Fatal(err)
			}

			if err := s.ACLEntries.RevokePrincipalGrants(ctx, collectionPath, href, []string{"read"}); err != nil {
				t.Fatal(err)
			}

			held, err := s.Locks.GetByToken(ctx, "opaquelocktoken:held")
			if err != nil {
				t.Fatal(err)
			}
			if (held != nil) != tc.wantLockKept {
				t.Errorf("lock kept = %t, want %t", held != nil, tc.wantLockKept)
			}
		})
	}
}
