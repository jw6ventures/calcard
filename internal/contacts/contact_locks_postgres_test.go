package contacts

import (
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/jw6ventures/calcard/internal/store"
)

type contactLockFixture struct {
	t        *testing.T
	database *store.Store
	service  *Service
	user     *store.User
	fromBook int64
	toBook   int64
}

func newContactLockFixture(t *testing.T) *contactLockFixture {
	t.Helper()
	database, user, fromBook, toBook := newPostgresMoveFixture(t)
	f := &contactLockFixture{t: t, database: database, service: NewService(database), user: user, fromBook: fromBook, toBook: toBook}
	for _, uid := range []string{"alice", "bob"} {
		if _, _, err := f.service.CreateContact(t.Context(), user, fromBook, UpsertInput{RawVCard: lockTestCard(uid, uid)}); err != nil {
			t.Fatalf("create %s: %v", uid, err)
		}
	}
	return f
}

func lockTestCard(uid, name string) string {
	return "BEGIN:VCARD\r\nVERSION:3.0\r\nUID:" + uid + "\r\nFN:" + name + "\r\nEND:VCARD\r\n"
}

func (f *contactLockFixture) bookPath(bookID int64) string {
	return addressBookACLCollectionPath(bookID)
}

func (f *contactLockFixture) memberPath(bookID int64, name string) string {
	return addressBookACLResourcePaths(bookID, name)[0]
}

// lock stores a lock another client holds, as a DAV LOCK would.
func (f *contactLockFixture) lock(resourcePath, depth string, expiresIn time.Duration) string {
	f.t.Helper()
	token := fmt.Sprintf("opaquelocktoken:contact-lock-%d", time.Now().UnixNano())
	if _, err := f.database.Locks.Create(f.t.Context(), store.Lock{
		Token: token, ResourcePath: resourcePath, UserID: f.user.ID,
		LockScope: "exclusive", LockType: "write", Depth: depth, TimeoutSeconds: 3600, ExpiresAt: time.Now().Add(expiresIn),
	}); err != nil {
		f.t.Fatalf("lock %s: %v", resourcePath, err)
	}
	return token
}

func (f *contactLockFixture) stored(bookID int64, uid string) *store.Contact {
	f.t.Helper()
	contact, err := f.database.Contacts.GetByUID(f.t.Context(), bookID, uid)
	if err != nil {
		f.t.Fatal(err)
	}
	return contact
}

// A contact write DAV would refuse for want of a lock token is refused with
// ErrLocked, since a UI or REST request cannot submit one: an update or delete
// of a locked contact, an update or delete under a depth-infinity lock on its
// book, a delete or create under a depth-0 lock on the book, a create at a name
// a lock-null resource holds, and a move out of a locked contact or into a
// locked book or name. The write leaves the contact, and the lock, as they were.
func TestPostgres_ContactWritesRefuseWhatDAVWouldFindLocked(t *testing.T) {
	ctx := t.Context()
	tests := []struct {
		name string
		// lock is the lock another client holds, as (resource path, depth).
		lock func(f *contactLockFixture) (string, string)
		do   func(f *contactLockFixture) error
		// refused is whether DAV answers 423 without a token.
		refused bool
	}{
		{
			name: "update of a locked contact",
			lock: func(f *contactLockFixture) (string, string) { return f.memberPath(f.fromBook, "alice"), "0" },
			do: func(f *contactLockFixture) error {
				_, _, err := f.service.UpdateContact(ctx, f.user, f.fromBook, "alice", UpsertInput{RawVCard: lockTestCard("alice", "Changed")})
				return err
			},
			refused: true,
		},
		{
			name: "update under a depth-infinity lock on the book",
			lock: func(f *contactLockFixture) (string, string) { return f.bookPath(f.fromBook), "infinity" },
			do: func(f *contactLockFixture) error {
				_, _, err := f.service.UpdateContact(ctx, f.user, f.fromBook, "alice", UpsertInput{RawVCard: lockTestCard("alice", "Changed")})
				return err
			},
			refused: true,
		},
		{
			name: "update under a depth-0 lock on the book",
			lock: func(f *contactLockFixture) (string, string) { return f.bookPath(f.fromBook), "0" },
			do: func(f *contactLockFixture) error {
				_, _, err := f.service.UpdateContact(ctx, f.user, f.fromBook, "alice", UpsertInput{RawVCard: lockTestCard("alice", "Changed")})
				return err
			},
		},
		{
			name: "update beside a locked contact",
			lock: func(f *contactLockFixture) (string, string) { return f.memberPath(f.fromBook, "bob"), "0" },
			do: func(f *contactLockFixture) error {
				_, _, err := f.service.UpdateContact(ctx, f.user, f.fromBook, "alice", UpsertInput{RawVCard: lockTestCard("alice", "Changed")})
				return err
			},
		},
		{
			name: "delete of a locked contact",
			lock: func(f *contactLockFixture) (string, string) { return f.memberPath(f.fromBook, "alice"), "0" },
			do: func(f *contactLockFixture) error {
				return f.service.DeleteContact(ctx, f.user, f.fromBook, "alice", "", "")
			},
			refused: true,
		},
		{
			name: "delete under a depth-0 lock on the book",
			lock: func(f *contactLockFixture) (string, string) { return f.bookPath(f.fromBook), "0" },
			do: func(f *contactLockFixture) error {
				return f.service.DeleteContact(ctx, f.user, f.fromBook, "alice", "", "")
			},
			refused: true,
		},
		{
			name: "create under a depth-0 lock on the book",
			lock: func(f *contactLockFixture) (string, string) { return f.bookPath(f.fromBook), "0" },
			do: func(f *contactLockFixture) error {
				_, _, err := f.service.CreateContact(ctx, f.user, f.fromBook, UpsertInput{RawVCard: lockTestCard("carol", "Carol")})
				return err
			},
			refused: true,
		},
		{
			name: "create under a depth-infinity lock on the book",
			lock: func(f *contactLockFixture) (string, string) { return f.bookPath(f.fromBook), "infinity" },
			do: func(f *contactLockFixture) error {
				_, _, err := f.service.CreateContact(ctx, f.user, f.fromBook, UpsertInput{Structured: &StructuredInput{UID: "carol", DisplayName: "Carol"}})
				return err
			},
			refused: true,
		},
		{
			name: "create at a name a lock-null resource holds",
			lock: func(f *contactLockFixture) (string, string) { return f.memberPath(f.fromBook, "carol"), "0" },
			do: func(f *contactLockFixture) error {
				_, _, err := f.service.CreateContact(ctx, f.user, f.fromBook, UpsertInput{RawVCard: lockTestCard("carol", "Carol")})
				return err
			},
			refused: true,
		},
		{
			name: "create beside a lock-null resource",
			lock: func(f *contactLockFixture) (string, string) { return f.memberPath(f.fromBook, "dave"), "0" },
			do: func(f *contactLockFixture) error {
				_, _, err := f.service.CreateContact(ctx, f.user, f.fromBook, UpsertInput{RawVCard: lockTestCard("carol", "Carol")})
				return err
			},
		},
		{
			name: "move of a locked contact",
			lock: func(f *contactLockFixture) (string, string) { return f.memberPath(f.fromBook, "alice"), "0" },
			do: func(f *contactLockFixture) error {
				return f.service.MoveContact(ctx, f.user, f.fromBook, f.toBook, "alice")
			},
			refused: true,
		},
		{
			name: "move out of a book locked at depth 0",
			lock: func(f *contactLockFixture) (string, string) { return f.bookPath(f.fromBook), "0" },
			do: func(f *contactLockFixture) error {
				return f.service.MoveContact(ctx, f.user, f.fromBook, f.toBook, "alice")
			},
			refused: true,
		},
		{
			name: "move into a book locked at depth 0",
			lock: func(f *contactLockFixture) (string, string) { return f.bookPath(f.toBook), "0" },
			do: func(f *contactLockFixture) error {
				return f.service.MoveContact(ctx, f.user, f.fromBook, f.toBook, "alice")
			},
			refused: true,
		},
		{
			name: "move onto a name locked in the destination",
			lock: func(f *contactLockFixture) (string, string) { return f.memberPath(f.toBook, "alice"), "0" },
			do: func(f *contactLockFixture) error {
				return f.service.MoveContact(ctx, f.user, f.fromBook, f.toBook, "alice")
			},
			refused: true,
		},
		{
			name: "move beside a locked contact",
			lock: func(f *contactLockFixture) (string, string) { return f.memberPath(f.fromBook, "bob"), "0" },
			do: func(f *contactLockFixture) error {
				return f.service.MoveContact(ctx, f.user, f.fromBook, f.toBook, "alice")
			},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			f := newContactLockFixture(t)
			resourcePath, depth := tc.lock(f)
			token := f.lock(resourcePath, depth, time.Hour)
			err := tc.do(f)
			if tc.refused {
				if !errors.Is(err, ErrLocked) {
					t.Fatalf("error = %v, want ErrLocked", err)
				}
				if got := StatusCode(err); got != 423 {
					t.Errorf("StatusCode = %d, want 423", got)
				}
				if alice := f.stored(f.fromBook, "alice"); alice == nil || !strings.Contains(alice.RawVCard, "FN:alice") {
					t.Errorf("the refused write changed the contact: %#v", alice)
				}
				if f.stored(f.toBook, "alice") != nil || f.stored(f.fromBook, "carol") != nil {
					t.Error("the refused write stored a contact")
				}
			} else if err != nil {
				t.Fatalf("error = %v, want the write to go through", err)
			}
			held, listErr := f.database.Locks.GetByToken(ctx, token)
			if listErr != nil || held == nil {
				t.Errorf("the lock is gone after the write: %v, %v", held, listErr)
			}
		})
	}
}

// A lock that has expired is no lock, as DAV treats it.
func TestPostgres_ContactWritesIgnoreAnExpiredLock(t *testing.T) {
	ctx := t.Context()
	f := newContactLockFixture(t)
	f.lock(f.memberPath(f.fromBook, "alice"), "0", -time.Minute)
	f.lock(f.bookPath(f.fromBook), "infinity", -time.Minute)
	f.lock(f.bookPath(f.toBook), "infinity", -time.Minute)
	f.lock(f.memberPath(f.fromBook, "carol"), "0", -time.Minute)

	if _, _, err := f.service.UpdateContact(ctx, f.user, f.fromBook, "alice", UpsertInput{RawVCard: lockTestCard("alice", "Changed")}); err != nil {
		t.Fatalf("UpdateContact: %v", err)
	}
	if _, _, err := f.service.CreateContact(ctx, f.user, f.fromBook, UpsertInput{RawVCard: lockTestCard("carol", "Carol")}); err != nil {
		t.Fatalf("CreateContact: %v", err)
	}
	if err := f.service.MoveContact(ctx, f.user, f.fromBook, f.toBook, "alice"); err != nil {
		t.Fatalf("MoveContact: %v", err)
	}
	if err := f.service.DeleteContact(ctx, f.user, f.fromBook, "bob", "", ""); err != nil {
		t.Fatalf("DeleteContact: %v", err)
	}
}

// An import meets the locks a DAV PUT of each card would: a card for a locked
// contact, or any new card into a locked book, is skipped as locked, and the
// cards that are not covered by a lock are stored.
func TestPostgres_ImportSkipsCardsThatAreLocked(t *testing.T) {
	ctx := t.Context()
	f := newContactLockFixture(t)
	f.lock(f.memberPath(f.fromBook, "alice"), "0", time.Hour)

	file := lockTestCard("alice", "Replaced") + lockTestCard("bob", "Replaced") + lockTestCard("carol", "Carol")
	result, err := f.service.ImportVCards(ctx, f.user, f.fromBook, file)
	if err != nil {
		t.Fatalf("ImportVCards: %v", err)
	}
	if result.Imported != 2 || len(result.Skipped) != 1 || result.Skipped[0].Card != 1 || result.Skipped[0].Code != ImportSkipLocked {
		t.Fatalf("result = %+v, want card 1 skipped as locked and the other two imported", result)
	}

	f.lock(f.bookPath(f.fromBook), "0", time.Hour)
	result, err = f.service.ImportVCards(ctx, f.user, f.fromBook, lockTestCard("erin", "Erin")+lockTestCard("bob", "Again"))
	if err != nil {
		t.Fatalf("ImportVCards into a locked book: %v", err)
	}
	if result.Imported != 1 || len(result.Skipped) != 1 || result.Skipped[0].Card != 1 || result.Skipped[0].Code != ImportSkipLocked {
		t.Fatalf("result = %+v, want the new card skipped as locked and the update of bob imported", result)
	}
	if f.stored(f.fromBook, "erin") != nil {
		t.Error("a new card was stored in a locked book")
	}
}

// A write that carries a precondition meets the lock first, as DAV decides
// them: a stale If-Match on a locked contact is 423, not 412, so a client is
// told to wait for the lock rather than to refetch.
func TestPostgres_ContactWriteLockedAnswersAheadOfAFailedPrecondition(t *testing.T) {
	ctx := t.Context()
	f := newContactLockFixture(t)
	f.lock(f.memberPath(f.fromBook, "alice"), "0", time.Hour)
	f.lock(f.bookPath(f.fromBook), "0", time.Hour)
	card := UpsertInput{RawVCard: lockTestCard("alice", "Changed"), IfMatch: `"stale"`}

	if _, _, err := f.service.UpdateContact(ctx, f.user, f.fromBook, "alice", card); !errors.Is(err, ErrLocked) {
		t.Errorf("UpdateContact with a stale If-Match error = %v, want ErrLocked", err)
	}
	if err := f.service.DeleteContact(ctx, f.user, f.fromBook, "alice", `"stale"`, ""); !errors.Is(err, ErrLocked) {
		t.Errorf("DeleteContact with a stale If-Match error = %v, want ErrLocked", err)
	}
	if _, _, err := f.service.CreateContact(ctx, f.user, f.fromBook, UpsertInput{RawVCard: lockTestCard("alice", "Again"), IfNoneMatch: "*"}); !errors.Is(err, ErrLocked) {
		t.Errorf("CreateContact over a contact with If-None-Match: * error = %v, want ErrLocked", err)
	}

	unlocked := newContactLockFixture(t)
	if _, _, err := unlocked.service.UpdateContact(ctx, unlocked.user, unlocked.fromBook, "alice", card); !errors.Is(err, ErrPreconditionFailed) {
		t.Errorf("UpdateContact with a stale If-Match and no lock error = %v, want ErrPreconditionFailed", err)
	}
}
