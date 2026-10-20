package store

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"
)

type contactBatchFixture struct {
	s      *Store
	userID int64
	bookID int64
}

func newContactBatchFixture(t *testing.T) contactBatchFixture {
	t.Helper()
	s := newPostgresStore(t)
	user, err := s.Users.UpsertOAuthUser(t.Context(), "batch", "batch@example.test", "Batch", "Test")
	if err != nil {
		t.Fatalf("create user: %v", err)
	}
	book, err := s.AddressBooks.Create(t.Context(), AddressBook{UserID: user.ID, Name: "Batch"})
	if err != nil {
		t.Fatalf("create address book: %v", err)
	}
	return contactBatchFixture{s: s, userID: user.ID, bookID: book.ID}
}

func (f contactBatchFixture) write(uid string) ContactObjectWrite {
	body := "BEGIN:VCARD\r\nVERSION:3.0\r\nUID:" + uid + "\r\nFN:" + uid + "\r\nEND:VCARD\r\n"
	return ContactObjectWrite{
		AddressBookID: f.bookID,
		UID:           uid,
		ResourceName:  uid,
		RawVCard:      body,
		ETag:          "etag-" + uid,
		StatePath:     f.memberPath(uid),
		LockPreconditions: []LockPrecondition{
			NewLockPrecondition(f.memberPath(uid)),
			NewLockPrecondition(addressBookCollectionLockPath(f.bookID)),
		},
	}
}

func (f contactBatchFixture) memberPath(name string) string {
	return MemberStatePath(addressBookCollectionLockPath(f.bookID), name)
}

func (f contactBatchFixture) lock(t *testing.T, resourcePath, depth string, expiresIn time.Duration) {
	t.Helper()
	if _, err := f.s.Locks.Create(t.Context(), Lock{
		Token: fmt.Sprintf("opaquelocktoken:%s-%d", strings.ReplaceAll(resourcePath, "/", "-"), time.Now().UnixNano()), ResourcePath: resourcePath, UserID: f.userID,
		LockScope: "exclusive", LockType: "write", Depth: depth, TimeoutSeconds: 3600, ExpiresAt: time.Now().Add(expiresIn),
	}); err != nil {
		t.Fatalf("lock %s: %v", resourcePath, err)
	}
}

func (f contactBatchFixture) stored(t *testing.T, uid string) bool {
	t.Helper()
	contact, err := f.s.Contacts.GetByUID(t.Context(), f.bookID, uid)
	if err != nil {
		t.Fatal(err)
	}
	return contact != nil
}

// A batch stores every card a single PutContactObject would, each with its own
// ETag, and moves the book's CTag once per card as separate writes do.
func TestPostgres_PutContactObjectsStoresEveryCardAsASingleWriteDoes(t *testing.T) {
	f := newContactBatchFixture(t)
	before, err := f.s.AddressBooks.GetByID(t.Context(), f.bookID)
	if err != nil {
		t.Fatal(err)
	}
	writes := []ContactObjectWrite{f.write("a"), f.write("b"), f.write("c")}
	outcomes, err := f.s.PutContactObjects(t.Context(), writes)
	if err != nil {
		t.Fatalf("PutContactObjects: %v", err)
	}
	if len(outcomes) != len(writes) {
		t.Fatalf("outcomes = %d, want %d", len(outcomes), len(writes))
	}
	for i, outcome := range outcomes {
		if outcome.Err != nil || outcome.Result == nil || !outcome.Result.Created || outcome.Result.Contact.ETag != writes[i].ETag {
			t.Errorf("outcome %d = %+v, want a created contact with its ETag", i, outcome)
		}
	}
	after, err := f.s.AddressBooks.GetByID(t.Context(), f.bookID)
	if err != nil {
		t.Fatal(err)
	}
	if after.CTag != before.CTag+int64(len(writes)) || !after.UpdatedAt.After(before.UpdatedAt) {
		t.Errorf("ctag %d -> %d, updated %s -> %s, want the ctag moved once per card", before.CTag, after.CTag, before.UpdatedAt, after.UpdatedAt)
	}
	page, err := f.s.Contacts.ListChangesAfter(t.Context(), f.bookID, SyncCursorAt(before.UpdatedAt), false, 100)
	if err != nil || len(page.Members) != len(writes) {
		t.Fatalf("sync changes after the batch = %+v, %v, want %d members", page, err, len(writes))
	}
}

// A card PostgreSQL cannot store costs that card and not the batch around it:
// the cards before and after it commit.
func TestPostgres_PutContactObjectsKeepsTheCardsAroundOneItRefuses(t *testing.T) {
	f := newContactBatchFixture(t)
	oversized := incompressibleText(4096)
	writes := []ContactObjectWrite{f.write("first"), f.write("second"), f.write(oversized), f.write("fourth"), f.write("fifth")}
	outcomes, err := f.s.PutContactObjects(t.Context(), writes)
	if err != nil {
		t.Fatalf("PutContactObjects: %v", err)
	}
	for i, outcome := range outcomes {
		if i == 2 {
			if !IsDataError(outcome.Err) {
				t.Errorf("outcome 2 error = %v, want a data error", outcome.Err)
			}
			continue
		}
		if outcome.Err != nil || outcome.Result == nil {
			t.Errorf("outcome %d = %+v, want stored", i, outcome)
		}
	}
	for _, uid := range []string{"first", "second", "fourth", "fifth"} {
		if !f.stored(t, uid) {
			t.Errorf("contact %q was lost with the refused card", uid)
		}
	}
	if f.stored(t, oversized) {
		t.Error("the refused card was stored")
	}
}

// Each card meets the locks a DAV PUT of it would: an active lock on the card
// or on its book at any depth refuses a card it would create there, a
// depth-0 lock on the book leaves an existing card writable, and an expired
// lock refuses nothing. A refused card leaves the batch around it intact.
func TestPostgres_PutContactObjectsRefusesCardsDAVWouldFindLocked(t *testing.T) {
	f := newContactBatchFixture(t)
	if _, err := f.s.PutContactObject(t.Context(), f.write("existing")); err != nil {
		t.Fatal(err)
	}
	f.lock(t, f.memberPath("locked"), "0", time.Hour)
	f.lock(t, f.memberPath("lapsed"), "0", -time.Minute)

	update := f.write("existing")
	update.RawVCard = strings.Replace(update.RawVCard, "FN:existing", "FN:Renamed", 1)
	update.ETag = "etag-renamed"
	update.ExpectedState = DAVResourceState{Exists: true, UID: "existing", ResourceName: "existing", ETag: "etag-existing", RawData: f.write("existing").RawVCard}
	update.LockPreconditions = update.LockPreconditions[:1]
	f.lock(t, addressBookCollectionLockPath(f.bookID), "0", time.Hour)

	outcomes, err := f.s.PutContactObjects(t.Context(), []ContactObjectWrite{f.write("locked"), f.write("lapsed"), update, f.write("after")})
	if err != nil {
		t.Fatalf("PutContactObjects: %v", err)
	}
	if !errors.Is(outcomes[0].Err, ErrLockConflict) {
		t.Errorf("locked card error = %v, want ErrLockConflict", outcomes[0].Err)
	}
	if !errors.Is(outcomes[1].Err, ErrLockConflict) {
		t.Errorf("card created in a book locked at depth 0 error = %v, want ErrLockConflict", outcomes[1].Err)
	}
	if outcomes[2].Err != nil {
		t.Errorf("update of a card under a depth-0 book lock error = %v, want it stored", outcomes[2].Err)
	}
	if !errors.Is(outcomes[3].Err, ErrLockConflict) {
		t.Errorf("card created in a locked book error = %v, want ErrLockConflict", outcomes[3].Err)
	}
	if f.stored(t, "locked") || f.stored(t, "lapsed") || f.stored(t, "after") {
		t.Error("a refused card was stored")
	}
}

func TestPostgres_PutContactObjectsIgnoresAnExpiredLock(t *testing.T) {
	f := newContactBatchFixture(t)
	f.lock(t, f.memberPath("lapsed"), "0", -time.Minute)
	f.lock(t, addressBookCollectionLockPath(f.bookID), "infinity", -time.Minute)
	outcomes, err := f.s.PutContactObjects(t.Context(), []ContactObjectWrite{f.write("lapsed"), f.write("other")})
	if err != nil {
		t.Fatalf("PutContactObjects: %v", err)
	}
	for i, outcome := range outcomes {
		if outcome.Err != nil {
			t.Errorf("outcome %d error = %v, want stored", i, outcome.Err)
		}
	}
}

// A batch waits for the identity locks it needs in one sorted order, as every
// DAV write does, so it cannot hold one that a writer needing an earlier one
// is waiting on. The batch is made to stop at the name lock of its second
// card; a writer taking the UID lock of its first card then has to get it.
func TestPostgres_PutContactObjectsTakesIdentityLocksInSortedOrder(t *testing.T) {
	f := newContactBatchFixture(t)
	ctx, cancel := context.WithTimeout(t.Context(), 20*time.Second)
	defer cancel()
	holder, err := f.s.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer holder.Rollback()
	nameKey := fmt.Sprintf("contact-object:%d:name:zname", f.bookID)
	uidKey := fmt.Sprintf("contact-object:%d:uid:a", f.bookID)
	if _, err := holder.ExecContext(ctx, `SELECT pg_advisory_xact_lock(hashtext($1))`, nameKey); err != nil {
		t.Fatal(err)
	}

	done := make(chan error, 1)
	go func() {
		_, err := f.s.PutContactObjects(ctx, []ContactObjectWrite{f.write("a"), f.write("zname")})
		done <- err
	}()
	waitForAdvisoryWaiter(t, ctx, f.s)

	probeCtx, probeCancel := context.WithTimeout(ctx, 5*time.Second)
	defer probeCancel()
	if _, err := holder.ExecContext(probeCtx, `SELECT pg_advisory_xact_lock(hashtext($1))`, uidKey); err != nil {
		t.Fatalf("a writer taking %q while the batch waits for %q: %v; the batch holds a later lock than the one it waits on", uidKey, nameKey, err)
	}
	if err := holder.Commit(); err != nil {
		t.Fatal(err)
	}
	if err := <-done; err != nil {
		t.Fatalf("PutContactObjects: %v", err)
	}
	if !f.stored(t, "a") || !f.stored(t, "zname") {
		t.Error("the batch did not store both cards")
	}
}

func waitForAdvisoryWaiter(t *testing.T, ctx context.Context, s *Store) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		var waiting int
		const query = `SELECT count(*) FROM pg_locks l JOIN pg_stat_activity a USING (pid)
WHERE l.locktype = 'advisory' AND NOT l.granted AND a.application_name = current_setting('application_name')`
		if err := s.Contacts.(*contactRepo).pool.QueryRowContext(ctx, query).Scan(&waiting); err != nil {
			t.Fatal(err)
		}
		if waiting > 0 {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("the batch never waited for the held lock")
}

// incompressibleText is n octets PostgreSQL cannot compress below its btree
// entry limit, as a real value of that length would be.
func incompressibleText(n int) string {
	var b strings.Builder
	seed := sha256.Sum256([]byte("calcard"))
	for b.Len() < n {
		b.WriteString(hex.EncodeToString(seed[:]))
		seed = sha256.Sum256(seed[:])
	}
	return b.String()[:n]
}

// Deleting an address book removes the state stored at its paths with it: the
// ACL entries, dead properties and lock rows of the book and of its contacts,
// as deleting a calendar does, and nothing stored at another book's paths.
func TestPostgres_DeleteAddressBookRemovesTheStateAtItsPaths(t *testing.T) {
	f := newContactBatchFixture(t)
	ctx := t.Context()
	other, err := f.s.AddressBooks.Create(ctx, AddressBook{UserID: f.userID, Name: "Other"})
	if err != nil {
		t.Fatal(err)
	}
	book := addressBookCollectionLockPath(f.bookID)
	member := f.memberPath("a")
	otherBook := addressBookCollectionLockPath(other.ID)
	if _, err := f.s.PutContactObject(ctx, f.write("a")); err != nil {
		t.Fatal(err)
	}
	for _, path := range []string{book, member, otherBook} {
		if err := f.s.ACLEntries.SetACL(ctx, path, []ACLEntry{{PrincipalHref: "/dav/principals/9/", IsGrant: true, Privilege: "read"}}); err != nil {
			t.Fatal(err)
		}
		if err := f.s.DeadProperties.Apply(ctx, path, []DeadPropertyMutation{{NamespaceURI: "urn:test", LocalName: "note", InnerXML: "kept"}}); err != nil {
			t.Fatal(err)
		}
	}
	f.lock(t, member, "0", -time.Minute)
	f.lock(t, book, "0", -time.Minute)
	f.lock(t, otherBook, "0", time.Hour)

	if err := f.s.DeleteAddressBookAndState(ctx, f.userID, f.bookID, []LockPrecondition{NewLockPrecondition(book)}); err != nil {
		t.Fatalf("DeleteAddressBookAndState: %v", err)
	}
	for _, path := range []string{book, member} {
		entries, err := f.s.ACLEntries.ListByResource(ctx, path)
		properties, propErr := f.s.DeadProperties.ListByResources(ctx, []string{path})
		var locks int
		lockErr := f.s.Contacts.(*contactRepo).pool.QueryRowContext(ctx, `SELECT count(*) FROM locks WHERE resource_path=$1`, path).Scan(&locks)
		if err != nil || propErr != nil || lockErr != nil {
			t.Fatal(err, propErr, lockErr)
		}
		if len(entries) != 0 || len(properties) != 0 || locks != 0 {
			t.Errorf("left at %s: %d ACL entries, %d dead properties, %d lock rows", path, len(entries), len(properties), locks)
		}
	}
	entries, _ := f.s.ACLEntries.ListByResource(ctx, otherBook)
	properties, _ := f.s.DeadProperties.ListByResources(ctx, []string{otherBook})
	locks, _ := f.s.Locks.ListByResources(ctx, []string{otherBook})
	if len(entries) != 1 || len(properties) != 1 || len(locks) != 1 {
		t.Errorf("another book's state = %d ACL entries, %d dead properties, %d locks, want 1 each", len(entries), len(properties), len(locks))
	}
}

// A batch holds one subtransaction ID per write, and PostgreSQL caches 64 per
// backend, so the store refuses a batch past its bound and stores nothing of
// it, while a batch of exactly the bound is stored whole.
func TestPostgres_PutContactObjectsRefusesABatchPastTheSubtransactionBound(t *testing.T) {
	f := newContactBatchFixture(t)
	batch := func(n int) []ContactObjectWrite {
		writes := make([]ContactObjectWrite, n)
		for i := range writes {
			writes[i] = f.write(fmt.Sprintf("card-%03d", i))
		}
		return writes
	}
	if MaxContactObjectBatch >= 64 {
		t.Fatalf("MaxContactObjectBatch = %d, want fewer than the 64 subtransaction IDs a backend caches", MaxContactObjectBatch)
	}
	if outcomes, err := f.s.PutContactObjects(t.Context(), batch(MaxContactObjectBatch+1)); !errors.Is(err, ErrContactBatchTooLarge) || outcomes != nil {
		t.Fatalf("PutContactObjects over the bound = %v, %v, want ErrContactBatchTooLarge", outcomes, err)
	}
	if f.stored(t, "card-000") {
		t.Error("a refused batch stored a card")
	}
	outcomes, err := f.s.PutContactObjects(t.Context(), batch(MaxContactObjectBatch))
	if err != nil {
		t.Fatalf("PutContactObjects at the bound: %v", err)
	}
	for i, outcome := range outcomes {
		if outcome.Err != nil {
			t.Errorf("outcome %d error = %v", i, outcome.Err)
		}
	}
	if !f.stored(t, fmt.Sprintf("card-%03d", MaxContactObjectBatch-1)) {
		t.Error("the batch at the bound was not stored whole")
	}
}
