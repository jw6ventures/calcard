package store

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"
)

// An ACL replacement under lock preconditions is refused by an active lock on
// the resource, leaves the ACL and the lock as they were, and goes through once
// the lock has expired.
func TestPostgres_ACLWritesMeetLockPreconditions(t *testing.T) {
	ctx := context.Background()
	grant := []ACLEntry{{PrincipalHref: "/dav/principals/9/", IsGrant: true, Privilege: "read"}}
	writes := map[string]func(s *Store, collectionPath string, preconditions []LockPrecondition) error{
		"update": func(s *Store, collectionPath string, preconditions []LockPrecondition) error {
			return s.UpdateACLAndState(ctx, collectionPath, preconditions, func([]ACLEntry) ([]ACLEntry, error) { return nil, nil })
		},
	}
	for name, write := range writes {
		t.Run(name, func(t *testing.T) {
			s := newPostgresStore(t)
			userID, calendarID := newPostgresCalendar(t, s, "acl-lock-"+name)
			collectionPath := fmt.Sprintf("/dav/calendars/%d", calendarID)
			if err := s.ACLEntries.SetACL(ctx, collectionPath, grant); err != nil {
				t.Fatal(err)
			}
			lock := func(token string, expires time.Time) {
				t.Helper()
				if _, err := s.Locks.Create(ctx, Lock{Token: token, ResourcePath: collectionPath, UserID: userID, LockScope: "exclusive", LockType: "write",
					Depth: "0", TimeoutSeconds: 300, ExpiresAt: expires}); err != nil {
					t.Fatal(err)
				}
			}
			preconditions := []LockPrecondition{NewLockPrecondition(collectionPath)}

			lock("opaquelocktoken:held", time.Now().Add(time.Hour))
			if err := write(s, collectionPath, preconditions); !errors.Is(err, ErrLockConflict) {
				t.Fatalf("write error = %v, want ErrLockConflict", err)
			}
			if entries, err := s.ACLEntries.ListByResource(ctx, collectionPath); err != nil || len(entries) != 1 {
				t.Errorf("the refused write changed the ACL: %v, %v", entries, err)
			}
			if held, err := s.Locks.GetByToken(ctx, "opaquelocktoken:held"); err != nil || held == nil {
				t.Errorf("the refused write dropped the lock: %v, %v", held, err)
			}

			if err := s.Locks.DeleteByResourcePath(ctx, collectionPath); err != nil {
				t.Fatal(err)
			}
			lock("opaquelocktoken:expired", time.Now().Add(-time.Minute))
			if err := write(s, collectionPath, preconditions); err != nil {
				t.Fatalf("write past an expired lock: %v", err)
			}
			if entries, err := s.ACLEntries.ListByResource(ctx, collectionPath); err != nil || len(entries) != 0 {
				t.Errorf("the ACL after the write = %v, %v, want none", entries, err)
			}
		})
	}
}

// A revocation, which only removes grants, is refused by no lock: not one on the
// collection and not one on a member the grants are stored at. The locks stay.
func TestPostgres_RevokePrincipalGrantsIgnoresLocksOnTheCollectionAndItsMembers(t *testing.T) {
	ctx := context.Background()
	s := newPostgresStore(t)
	userID, calendarID := newPostgresCalendar(t, s, "revoke-member-lock")
	collectionPath := fmt.Sprintf("/dav/calendars/%d", calendarID)
	memberPath := collectionPath + "/member"
	grant := []ACLEntry{{PrincipalHref: "/dav/principals/9/", IsGrant: true, Privilege: "read"}}
	for _, resourcePath := range []string{collectionPath, memberPath} {
		if err := s.ACLEntries.SetACL(ctx, resourcePath, grant); err != nil {
			t.Fatal(err)
		}
		if _, err := s.Locks.Create(ctx, Lock{Token: "opaquelocktoken:" + resourcePath, ResourcePath: resourcePath, UserID: userID, LockScope: "exclusive", LockType: "write",
			Depth: "0", TimeoutSeconds: 300, ExpiresAt: time.Now().Add(time.Hour)}); err != nil {
			t.Fatal(err)
		}
	}

	if err := s.ACLEntries.RevokePrincipalGrants(ctx, collectionPath, "/dav/principals/9/", []string{"read"}); err != nil {
		t.Fatalf("a revocation past locks: %v", err)
	}
	for _, resourcePath := range []string{collectionPath, memberPath} {
		if entries, err := s.ACLEntries.ListByResource(ctx, resourcePath); err != nil || len(entries) != 0 {
			t.Errorf("grants left at %s: %v, %v", resourcePath, entries, err)
		}
		if held, err := s.Locks.GetByToken(ctx, "opaquelocktoken:"+resourcePath); err != nil || held == nil {
			t.Errorf("the revocation dropped the lock at %s: %v, %v", resourcePath, held, err)
		}
	}
}
