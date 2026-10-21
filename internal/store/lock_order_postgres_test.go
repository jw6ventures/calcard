package store

import (
	"context"
	"testing"
)

// Locks listed for a set of paths come in creation order, and locks created at
// the same instant in id order, whatever order their rows were stored in, so
// every reader of the same locks lists them alike.
func TestPostgres_LocksListInCreationThenIDOrder(t *testing.T) {
	s := newPostgresStore(t)
	ctx := context.Background()
	userID, calendarID := newPostgresCalendar(t, s, "lock-order")
	pool := s.Calendars.(*calendarRepo).pool
	collection := calendarCollectionLockPath(calendarID)
	for _, lock := range []struct {
		id    int64
		path  string
		token string
	}{
		{900, collection + "/member", "opaquelocktoken:second"},
		{100, collection, "opaquelocktoken:first"},
	} {
		if _, err := pool.ExecContext(ctx, `
INSERT INTO locks (id, token, resource_path, user_id, lock_scope, lock_type, depth, owner_info, timeout_seconds, created_at, expires_at)
VALUES ($1, $2, $3, $4, 'shared', 'write', 'infinity', '', 3600, '2026-01-01T00:00:00Z', NOW() + interval '1 hour')`,
			lock.id, lock.token, lock.path, userID); err != nil {
			t.Fatal(err)
		}
	}
	locks, err := s.Locks.ListByResources(ctx, []string{collection + "/member", collection})
	if err != nil {
		t.Fatal(err)
	}
	if len(locks) != 2 || locks[0].ID != 100 || locks[1].ID != 900 {
		t.Fatalf("locks = %#v, want ids 100 then 900", locks)
	}
}
