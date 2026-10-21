package store

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"
)

// moveGuardFixture is an event in one calendar, the calendar it moves to, and
// the state of the event as a caller that decided the move read it.
type moveGuardFixture struct {
	s          *Store
	userID     int64
	from, to   int64
	uid        string
	expected   DAVResourceState
	fromPath   string
	toPath     string
	movedEvent Event
}

func newMoveGuardFixture(t *testing.T) *moveGuardFixture {
	t.Helper()
	s := newPostgresStore(t)
	userID, from := newPostgresCalendar(t, s, "guard-from")
	_, to := newPostgresCalendar(t, s, "guard-to")
	const uid = "guarded"
	seedCalendarObject(t, s, from, uid)
	stored, err := s.Events.GetByUID(context.Background(), from, uid)
	if err != nil || stored == nil {
		t.Fatalf("read the seeded event: %v, %v", stored, err)
	}
	return &moveGuardFixture{
		s: s, userID: userID, from: from, to: to, uid: uid,
		expected: EventDAVResourceState(stored),
		fromPath: fmt.Sprintf("/dav/calendars/%d/%s", from, uid),
		toPath:   fmt.Sprintf("/dav/calendars/%d/%s", to, uid),
		movedEvent: Event{
			CalendarID: to, UID: uid, ResourceName: uid,
			RawICAL: postgresCalendarObject(uid, "moved"), ETag: "moved",
		},
	}
}

func (f *moveGuardFixture) move() error {
	return f.moveUnder(nil)
}

func (f *moveGuardFixture) moveUnder(lockPreconditions []LockPrecondition) error {
	return f.s.UpdateAndMoveEventAndState(context.Background(), f.from, f.expected, f.movedEvent, f.fromPath, f.toPath, lockPreconditions)
}

func (f *moveGuardFixture) requireNotMoved(t *testing.T, wantETag string) {
	t.Helper()
	ctx := context.Background()
	if moved, err := f.s.Events.GetByUID(ctx, f.to, f.uid); err != nil || moved != nil {
		t.Errorf("the event reached the destination: %+v, %v", moved, err)
	}
	stored, err := f.s.Events.GetByUID(ctx, f.from, f.uid)
	if err != nil || stored == nil || stored.ETag != wantETag {
		t.Errorf("the source event = %+v, %v; want it kept under ETag %q", stored, err, wantETag)
	}
}

// A move built from the source as it was read does not apply to the source as
// another client has since changed it.
func TestPostgres_UpdateAndMoveEventRefusesASourceChangedSinceItWasRead(t *testing.T) {
	f := newMoveGuardFixture(t)
	if _, err := f.s.Events.Upsert(context.Background(), Event{
		CalendarID: f.from, UID: f.uid, ResourceName: f.uid, RawICAL: postgresCalendarObject(f.uid, "changed by a client"), ETag: "client",
	}); err != nil {
		t.Fatal(err)
	}
	if err := f.move(); !errors.Is(err, ErrResourceStateChanged) {
		t.Fatalf("UpdateAndMoveEventAndState() error = %v, want ErrResourceStateChanged", err)
	}
	f.requireNotMoved(t, "client")
}

// A source another client has removed is not recreated at the destination.
func TestPostgres_UpdateAndMoveEventRefusesASourceRemovedSinceItWasRead(t *testing.T) {
	f := newMoveGuardFixture(t)
	if err := f.s.Events.DeleteByUID(context.Background(), f.from, f.uid); err != nil {
		t.Fatal(err)
	}
	if err := f.move(); !errors.Is(err, ErrResourceStateChanged) {
		t.Fatalf("UpdateAndMoveEventAndState() error = %v, want ErrResourceStateChanged", err)
	}
	if moved, err := f.s.Events.GetByUID(context.Background(), f.to, f.uid); err != nil || moved != nil {
		t.Errorf("the removed event reached the destination: %+v, %v", moved, err)
	}
}

// The ACL the caller decided the move on is checked again under the move's
// locks: a grant changed since is not moved through.
func TestPostgres_UpdateAndMoveEventRefusesAnACLChangedSinceItWasRead(t *testing.T) {
	f := newMoveGuardFixture(t)
	ctx := context.Background()
	fromCollection := fmt.Sprintf("/dav/calendars/%d", f.from)
	toCollection := fmt.Sprintf("/dav/calendars/%d", f.to)
	entries, err := f.s.ACLEntries.ListByResources(ctx, []string{fromCollection, toCollection})
	if err != nil {
		t.Fatal(err)
	}
	f.expected.ACL = NewACLGuard([]string{fromCollection, toCollection}, entries)

	if err := f.move(); err != nil {
		t.Fatalf("a move on unchanged ACLs: %v", err)
	}

	f = newMoveGuardFixture(t)
	fromCollection = fmt.Sprintf("/dav/calendars/%d", f.from)
	toCollection = fmt.Sprintf("/dav/calendars/%d", f.to)
	f.expected.ACL = NewACLGuard([]string{fromCollection, toCollection}, nil)
	if err := f.s.ACLEntries.SetACL(ctx, toCollection, []ACLEntry{{PrincipalHref: "/dav/principals/9/", IsGrant: true, Privilege: "bind"}}); err != nil {
		t.Fatal(err)
	}
	if err := f.move(); !errors.Is(err, ErrResourceStateChanged) {
		t.Fatalf("UpdateAndMoveEventAndState() error = %v, want ErrResourceStateChanged", err)
	}
	f.requireNotMoved(t, f.expected.ETag)
}

func (f *moveGuardFixture) lock(t *testing.T, token, resourcePath string, expires time.Time) {
	t.Helper()
	if _, err := f.s.Locks.Create(context.Background(), Lock{
		Token: token, ResourcePath: resourcePath, UserID: f.userID, LockScope: "exclusive", LockType: "write",
		Depth: "0", TimeoutSeconds: 300, ExpiresAt: expires,
	}); err != nil {
		t.Fatal(err)
	}
}

func (f *moveGuardFixture) preconditions() []LockPrecondition {
	return []LockPrecondition{
		NewLockPrecondition(f.fromPath), NewLockPrecondition(fmt.Sprintf("/dav/calendars/%d", f.from)),
		NewLockPrecondition(f.toPath), NewLockPrecondition(fmt.Sprintf("/dav/calendars/%d", f.to)),
	}
}

// A move that submits no lock token is refused by an active lock on the event,
// on either calendar, or on the name it takes, and the refusal leaves the lock
// row where it is; a lock that has expired refuses nothing.
func TestPostgres_UpdateAndMoveEventRefusesAnActiveLock(t *testing.T) {
	for _, tc := range []struct {
		name string
		path func(f *moveGuardFixture) string
	}{
		{"the event", func(f *moveGuardFixture) string { return f.fromPath }},
		{"the source calendar", func(f *moveGuardFixture) string { return fmt.Sprintf("/dav/calendars/%d", f.from) }},
		{"the destination calendar", func(f *moveGuardFixture) string { return fmt.Sprintf("/dav/calendars/%d", f.to) }},
		{"the destination name", func(f *moveGuardFixture) string { return f.toPath }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newMoveGuardFixture(t)
			f.lock(t, "opaquelocktoken:held", tc.path(f), time.Now().Add(time.Hour))
			if err := f.moveUnder(f.preconditions()); !errors.Is(err, ErrLockConflict) {
				t.Fatalf("UpdateAndMoveEventAndState() error = %v, want ErrLockConflict", err)
			}
			f.requireNotMoved(t, f.expected.ETag)
			if held, err := f.s.Locks.GetByToken(context.Background(), "opaquelocktoken:held"); err != nil || held == nil {
				t.Errorf("the refused move dropped the lock: %v, %v", held, err)
			}
		})
	}

	f := newMoveGuardFixture(t)
	f.lock(t, "opaquelocktoken:expired", f.fromPath, time.Now().Add(-time.Minute))
	if err := f.moveUnder(f.preconditions()); err != nil {
		t.Fatalf("a move past an expired lock: %v", err)
	}
}

// A move that renames the event leaves a tombstone in the source calendar under
// the name the event had there, which is what a sync client removes.
func TestPostgres_UpdateAndMoveEventTombstonesTheSourceName(t *testing.T) {
	f := newMoveGuardFixture(t)
	f.movedEvent.ResourceName = "renamed"
	f.toPath = fmt.Sprintf("/dav/calendars/%d/renamed", f.to)
	if err := f.move(); err != nil {
		t.Fatal(err)
	}
	changes, err := f.s.Events.ListChangesAfter(context.Background(), f.from, SyncCursor{}, true, 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(changes.Removals) != 1 || changes.Removals[0].ResourceName != f.uid {
		t.Errorf("source tombstones = %+v, want one under the source name %q", changes.Removals, f.uid)
	}
}
