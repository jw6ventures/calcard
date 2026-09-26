package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"
)

// memberWrite is the calendar object PUT the DAV layer issues for a new member
// of calendarID, lock preconditions included.
func memberWrite(calendarID int64, name string) CalendarObjectWrite {
	collection := fmt.Sprintf("/dav/calendars/%d", calendarID)
	return CalendarObjectWrite{
		CalendarID:    calendarID,
		UID:           name,
		ResourceName:  name,
		RawICAL:       postgresCalendarObject(name, name),
		ETag:          "etag-" + name,
		ExpectedState: &CalendarObjectResourceState{},
		LockPreconditions: []LockPrecondition{
			{ResourcePath: collection + "/" + name + ".ics"},
			{ResourcePath: collection},
		},
	}
}

func calendarSyncToken(t *testing.T, s *Store, calendarID int64) time.Time {
	t.Helper()
	calendar, err := s.Calendars.GetByID(context.Background(), calendarID)
	if err != nil {
		t.Fatalf("load calendar %d: %v", calendarID, err)
	}
	return calendar.UpdatedAt
}

func reportedSince(t *testing.T, s *Store, calendarID int64, token time.Time) map[string]bool {
	t.Helper()
	changed, err := s.Events.ListModifiedSincePageAfter(context.Background(), calendarID, 0, token, 100)
	if err != nil {
		t.Fatalf("list changes since %v: %v", token, err)
	}
	reported := make(map[string]bool, len(changed))
	for _, event := range changed {
		reported[event.UID] = true
	}
	return reported
}

// Two writes to members of one collection must commit in the order their sync
// stamps were taken. A sibling that could commit while an earlier-stamped write
// was still open would let a client take a token newer than that write's stamp,
// and sync-collection would never report the write.
func TestPostgres_SiblingMemberWritesSerializeOnTheirCollection(t *testing.T) {
	pool := newPostgresPool(t)
	collection := uniqueDAVCollectionPath("/dav/calendars")

	holder, err := lockPreconditionHolder(pool, "10s", collection+"/a.ics", collection)
	if err != nil {
		t.Fatalf("hold member write preconditions: %v", err)
	}
	defer holder.Rollback()

	sibling, err := lockPreconditionHolder(pool, "1s", collection+"/b.ics", collection)
	if err == nil {
		sibling.Rollback()
		t.Fatal("sibling member write proceeded while another member write held the collection; want a lock wait")
	}
	if !isPostgresLockTimeout(err) {
		t.Fatalf("sibling member write = %v, want a lock wait", err)
	}

	if _, err := lockPreconditionHolder(pool, "1s", collection+"/a.ics", collection); !isPostgresLockTimeout(err) {
		t.Fatalf("second write to the same member = %v, want a lock wait", err)
	}
	if _, err := lockPreconditionHolder(pool, "1s", collection); !isPostgresLockTimeout(err) {
		t.Fatalf("collection-scoped operation while a member write is held = %v, want a lock wait", err)
	}

	otherCollection := uniqueDAVCollectionPath("/dav/calendars")
	other, err := lockPreconditionHolder(pool, "1s", otherCollection+"/a.ics", otherCollection)
	if err != nil {
		t.Fatalf("member write in another collection = %v; collections must not serialize on each other", err)
	}
	other.Rollback()
}

// The same race through the repository: a member write that has its locks but
// has not yet written stays open while a sibling write starts. The sibling has
// to wait for it, so the two commit in the order they took the collection and
// their stamps follow that order.
func TestPostgres_SyncTokenReportsSiblingWriteCommittedAfterIt(t *testing.T) {
	s := newPostgresStore(t)
	_, calendarID := newPostgresCalendar(t, s, "sync-sibling-order")
	ctx := context.Background()

	first := memberWrite(calendarID, "first")
	open, err := s.BeginTx(ctx, nil)
	if err != nil {
		t.Fatalf("begin: %v", err)
	}
	defer open.Rollback()
	if err := validateLockPreconditionsTx(ctx, open, first.LockPreconditions); err != nil {
		t.Fatalf("lock first write: %v", err)
	}

	sibling := make(chan error, 1)
	go func() {
		_, err := s.PutCalendarObject(ctx, memberWrite(calendarID, "second"))
		sibling <- err
	}()
	waitUntilBlockedBy(t, s, open)

	if _, err := putCalendarObjectTx(ctx, open, first); err != nil {
		t.Fatalf("write first member: %v", err)
	}
	token := committedToken(t, open, "calendars", calendarID)
	if err := open.Commit(); err != nil {
		t.Fatalf("commit first write: %v", err)
	}
	if err := <-sibling; err != nil {
		t.Fatalf("sibling write: %v", err)
	}
	reported := reportedSince(t, s, calendarID, token)
	if reported["first"] || !reported["second"] {
		t.Fatalf("changes since the first write's token %v = %v, want only the sibling that committed after it", token, reported)
	}
}

// stallingPool holds the first transaction it begins until released, after the
// transaction has started and so after its NOW() is fixed.
type stallingPool struct {
	txPool
	once    sync.Once
	begun   chan struct{}
	release chan struct{}
}

func (p *stallingPool) BeginTx(ctx context.Context, opts *sql.TxOptions) (*sql.Tx, error) {
	tx, err := p.txPool.BeginTx(ctx, opts)
	if err != nil {
		return nil, err
	}
	if _, err := tx.ExecContext(ctx, `SELECT NOW()`); err != nil {
		tx.Rollback()
		return nil, err
	}
	p.once.Do(func() {
		close(p.begun)
		<-p.release
	})
	return tx, nil
}

// A write's transaction can start before a sibling write to its collection
// commits and reach the collection only after it. Its stamps must still come
// out later than any token the sibling's commit let a client take.
func TestPostgres_MemberWriteStartedBeforeASiblingCommitIsStampedAfterIt(t *testing.T) {
	s := newPostgresStore(t)
	_, calendarID := newPostgresCalendar(t, s, "sync-start-order")
	ctx := context.Background()

	pool := &stallingPool{txPool: s.pool, begun: make(chan struct{}), release: make(chan struct{})}
	stalled := *s
	stalled.pool = pool
	late := make(chan error, 1)
	go func() {
		_, err := stalled.PutCalendarObject(ctx, memberWrite(calendarID, "late"))
		late <- err
	}()
	<-pool.begun

	if _, err := s.PutCalendarObject(ctx, memberWrite(calendarID, "early")); err != nil {
		close(pool.release)
		t.Fatalf("sibling write: %v", err)
	}
	token := calendarSyncToken(t, s, calendarID)
	close(pool.release)

	if err := <-late; err != nil {
		t.Fatalf("stalled write = %v, want success", err)
	}
	if !reportedSince(t, s, calendarID, token)["late"] {
		t.Fatalf("the stalled write is not reported since sync token %v taken before it committed", token)
	}
}

// A collection DELETE names the collection and every member it removes. A PUT
// creating a new member of that collection concurrently must wait for it and
// then find the collection gone, which the DAV layer retries into a 404, rather
// than fail its insert on the collection's foreign key.
func TestPostgres_MemberCreateRacingCollectionDeleteIsAStateChange(t *testing.T) {
	s := newPostgresStore(t)
	_, calendarID := newPostgresCalendar(t, s, "create-vs-collection-delete")
	ctx := context.Background()
	if _, err := s.PutCalendarObject(ctx, memberWrite(calendarID, "existing")); err != nil {
		t.Fatalf("seed member: %v", err)
	}
	collection := calendarCollectionLockPath(calendarID)

	deleting, err := s.BeginTx(ctx, nil)
	if err != nil {
		t.Fatalf("begin: %v", err)
	}
	defer deleting.Rollback()
	if err := validateLockPreconditionsTx(ctx, deleting, []LockPrecondition{
		{ResourcePath: collection},
		{ResourcePath: "/dav/calendars"},
		{ResourcePath: collection + "/existing.ics"},
	}); err != nil {
		t.Fatalf("lock collection delete: %v", err)
	}
	if _, err := deleting.ExecContext(ctx, `DELETE FROM calendars WHERE id=$1`, calendarID); err != nil {
		t.Fatalf("delete collection: %v", err)
	}

	created := make(chan error, 1)
	go func() {
		_, err := s.PutCalendarObject(ctx, memberWrite(calendarID, "new"))
		created <- err
	}()
	waitUntilBlockedBy(t, s, deleting)
	if err := deleting.Commit(); err != nil {
		t.Fatalf("commit collection delete: %v", err)
	}
	if err := <-created; !errors.Is(err, ErrResourceStateChanged) {
		t.Fatalf("PUT racing the collection's DELETE = %v, want ErrResourceStateChanged", err)
	}
}

// Whatever the locks let through, an insert whose collection is gone is a
// state change the DAV layer can retry, not a server error.
func TestPostgres_MemberInsertIntoMissingCollectionIsAStateChange(t *testing.T) {
	s := newPostgresStore(t)
	ctx := context.Background()
	const missingID = int64(987654321)

	t.Run("calendar", func(t *testing.T) {
		tx, err := s.BeginTx(ctx, nil)
		if err != nil {
			t.Fatalf("begin: %v", err)
		}
		defer tx.Rollback()
		if _, err := putCalendarObjectTx(ctx, tx, memberWrite(missingID, "orphan")); !errors.Is(err, ErrResourceStateChanged) {
			t.Fatalf("putCalendarObjectTx() into a missing calendar = %v, want ErrResourceStateChanged", err)
		}
	})
	t.Run("address book", func(t *testing.T) {
		tx, err := s.BeginTx(ctx, nil)
		if err != nil {
			t.Fatalf("begin: %v", err)
		}
		defer tx.Rollback()
		_, err = putContactObjectTx(ctx, tx, ContactObjectWrite{
			AddressBookID: missingID,
			UID:           "orphan",
			ResourceName:  "orphan",
			RawVCard:      "BEGIN:VCARD\r\nVERSION:3.0\r\nUID:orphan\r\nFN:Orphan\r\nEND:VCARD\r\n",
			ETag:          "orphan",
		})
		if !errors.Is(err, ErrResourceStateChanged) {
			t.Fatalf("putContactObjectTx() into a missing address book = %v, want ErrResourceStateChanged", err)
		}
	})
}

// A write carries the ACL entries its privilege check was decided on. The
// store re-reads them under the write's locks, so an ACL change landing between
// the check and the write refuses it for the HTTP layer to re-authorize, while
// a sibling member's write, which changes the collection CTag but no ACL, does
// not.
func TestPostgres_MemberWritesRefuseAChangedACL(t *testing.T) {
	s := newPostgresStore(t)
	_, calendarID := newPostgresCalendar(t, s, "acl-guard")
	ctx := context.Background()
	collection := calendarCollectionLockPath(calendarID)
	guardPaths := []string{collection, collection + "/guarded", collection + "/guarded.ics"}
	grant := func(privilege string) {
		t.Helper()
		if err := s.ACLEntries.SetACL(ctx, collection, []ACLEntry{{PrincipalHref: "/dav/principals/77/", IsGrant: true, Privilege: privilege}}); err != nil {
			t.Fatalf("SetACL(%s): %v", privilege, err)
		}
	}
	snapshot := func() *ACLGuard {
		t.Helper()
		entries, err := s.ACLEntries.ListByResources(ctx, guardPaths)
		if err != nil {
			t.Fatalf("read ACL: %v", err)
		}
		return NewACLGuard(guardPaths, entries)
	}

	grant("all")
	authorized := snapshot()
	if _, err := s.PutCalendarObject(ctx, memberWrite(calendarID, "sibling")); err != nil {
		t.Fatalf("sibling write: %v", err)
	}
	write := memberWrite(calendarID, "guarded")
	write.ExpectedACL = authorized
	if _, err := s.PutCalendarObject(ctx, write); err != nil {
		t.Fatalf("PUT under an unchanged ACL after a sibling write = %v, want success", err)
	}
	stored, err := s.Events.GetByResourceName(ctx, calendarID, "guarded")
	if err != nil || stored == nil {
		t.Fatalf("load guarded member: %v", err)
	}

	grant("read")
	write.RawICAL = postgresCalendarObject("guarded", "changed")
	write.ExpectedState = &CalendarObjectResourceState{Exists: true}
	if _, err := s.PutCalendarObject(ctx, write); !errors.Is(err, ErrResourceStateChanged) {
		t.Fatalf("PUT authorized against a since-changed ACL = %v, want ErrResourceStateChanged", err)
	}
	expected := EventDAVResourceState(stored)
	expected.ACL = authorized
	if err := s.DeleteEventAndState(ctx, calendarID, expected, collection+"/guarded.ics", nil); !errors.Is(err, ErrResourceStateChanged) {
		t.Fatalf("DELETE authorized against a since-changed ACL = %v, want ErrResourceStateChanged", err)
	}

	expected.ACL = snapshot()
	if err := s.DeleteEventAndState(ctx, calendarID, expected, collection+"/guarded.ics", nil); err != nil {
		t.Fatalf("DELETE re-authorized against the current ACL = %v, want success", err)
	}
}

// Every kind of member change is stamped for sync, by the triggers or by the
// tombstone it leaves. Each case starts a transaction, lets a sibling write to
// the same collection commit and a client take the resulting token, and only
// then lets the first transaction make its change. The change must be reported
// since that token, which it cannot be if any stamp is the transaction's start
// time rather than a time taken once the collection is held.
func TestPostgres_EveryMemberChangeIsStampedAfterTheCollectionIsHeld(t *testing.T) {
	const vcard = "BEGIN:VCARD\r\nVERSION:3.0\r\nUID:%[1]s\r\nFN:%[1]s\r\nEND:VCARD\r\n"
	type fixture struct {
		s            *Store
		calendarID   int64
		addressBook  int64
		seededEvent  *Event
		seededPerson *Contact
	}
	contactWrite := func(bookID int64, uid, name string) ContactObjectWrite {
		return ContactObjectWrite{
			AddressBookID: bookID, UID: uid, ResourceName: uid, RawVCard: fmt.Sprintf(vcard, name), ETag: "etag-" + name,
			StatePath: fmt.Sprintf("/dav/addressbooks/%d/%s.vcf", bookID, uid),
		}
	}
	cases := []struct {
		name     string
		sibling  func(f fixture) error
		change   func(s *Store, f fixture) error
		token    func(t *testing.T, f fixture) time.Time
		reported func(t *testing.T, f fixture, token time.Time) bool
	}{
		{
			name: "event update",
			sibling: func(f fixture) error {
				_, err := f.s.PutCalendarObject(context.Background(), memberWrite(f.calendarID, "sibling"))
				return err
			},
			change: func(s *Store, f fixture) error {
				write := memberWrite(f.calendarID, "seeded")
				write.RawICAL = postgresCalendarObject("seeded", "changed")
				write.ExpectedState = &CalendarObjectResourceState{Exists: true}
				_, err := s.PutCalendarObject(context.Background(), write)
				return err
			},
			token: func(t *testing.T, f fixture) time.Time { return calendarSyncToken(t, f.s, f.calendarID) },
			reported: func(t *testing.T, f fixture, token time.Time) bool {
				return reportedSince(t, f.s, f.calendarID, token)["seeded"]
			},
		},
		{
			name: "event delete",
			sibling: func(f fixture) error {
				_, err := f.s.PutCalendarObject(context.Background(), memberWrite(f.calendarID, "sibling"))
				return err
			},
			change: func(s *Store, f fixture) error {
				return s.DeleteEventAndState(context.Background(), f.calendarID, EventDAVResourceState(f.seededEvent),
					fmt.Sprintf("/dav/calendars/%d/seeded.ics", f.calendarID), nil)
			},
			token: func(t *testing.T, f fixture) time.Time { return calendarSyncToken(t, f.s, f.calendarID) },
			reported: func(t *testing.T, f fixture, token time.Time) bool {
				removed, err := f.s.DeletedResources.ListDeletedSincePageAfter(context.Background(), "event", f.calendarID, 0, token, 100)
				if err != nil {
					t.Fatalf("list removals: %v", err)
				}
				return len(removed) == 1 && removed[0].UID == "seeded"
			},
		},
		{
			name: "contact create",
			sibling: func(f fixture) error {
				_, err := f.s.PutContactObject(context.Background(), contactWrite(f.addressBook, "sibling", "sibling"))
				return err
			},
			change: func(s *Store, f fixture) error {
				_, err := s.PutContactObject(context.Background(), contactWrite(f.addressBook, "late", "late"))
				return err
			},
			token: func(t *testing.T, f fixture) time.Time {
				book, err := f.s.AddressBooks.GetByID(context.Background(), f.addressBook)
				if err != nil {
					t.Fatalf("load address book: %v", err)
				}
				return book.UpdatedAt
			},
			reported: func(t *testing.T, f fixture, token time.Time) bool {
				changed, err := f.s.Contacts.ListModifiedSincePageAfter(context.Background(), f.addressBook, 0, token, 100)
				if err != nil {
					t.Fatalf("list contact changes: %v", err)
				}
				return len(changed) == 1 && changed[0].UID == "late"
			},
		},
		{
			name: "contact delete",
			sibling: func(f fixture) error {
				_, err := f.s.PutContactObject(context.Background(), contactWrite(f.addressBook, "sibling", "sibling"))
				return err
			},
			change: func(s *Store, f fixture) error {
				return s.DeleteContactAndState(context.Background(), f.addressBook, ContactDAVResourceState(f.seededPerson),
					fmt.Sprintf("/dav/addressbooks/%d/seeded.vcf", f.addressBook), nil)
			},
			token: func(t *testing.T, f fixture) time.Time {
				book, err := f.s.AddressBooks.GetByID(context.Background(), f.addressBook)
				if err != nil {
					t.Fatalf("load address book: %v", err)
				}
				return book.UpdatedAt
			},
			reported: func(t *testing.T, f fixture, token time.Time) bool {
				removed, err := f.s.DeletedResources.ListDeletedSincePageAfter(context.Background(), "contact", f.addressBook, 0, token, 100)
				if err != nil {
					t.Fatalf("list removals: %v", err)
				}
				return len(removed) == 1 && removed[0].UID == "seeded"
			},
		},
	}
	for _, tt := range cases {
		t.Run(tt.name, func(t *testing.T) {
			s := newPostgresStore(t)
			ctx := context.Background()
			userID, calendarID := newPostgresCalendar(t, s, "stamp-order")
			book, err := s.AddressBooks.Create(ctx, AddressBook{UserID: userID, Name: "Stamps"})
			if err != nil {
				t.Fatalf("create address book: %v", err)
			}
			seededEvent, err := s.PutCalendarObject(ctx, memberWrite(calendarID, "seeded"))
			if err != nil {
				t.Fatalf("seed event: %v", err)
			}
			seededContact, err := s.PutContactObject(ctx, contactWrite(book.ID, "seeded", "seeded"))
			if err != nil {
				t.Fatalf("seed contact: %v", err)
			}
			f := fixture{s: s, calendarID: calendarID, addressBook: book.ID, seededEvent: seededEvent.Event, seededPerson: seededContact.Contact}

			pool := &stallingPool{txPool: s.pool, begun: make(chan struct{}), release: make(chan struct{})}
			stalled := *s
			stalled.pool = pool
			done := make(chan error, 1)
			go func() { done <- tt.change(&stalled, f) }()
			<-pool.begun

			if err := tt.sibling(f); err != nil {
				close(pool.release)
				t.Fatalf("sibling write: %v", err)
			}
			token := tt.token(t, f)
			close(pool.release)
			if err := <-done; err != nil {
				t.Fatalf("stalled change: %v", err)
			}
			if !tt.reported(t, f, token) {
				t.Fatalf("a %s committed after sync token %v was taken is not reported since it", tt.name, token)
			}
		})
	}
}

// waitUntilBlockedBy returns once some session is waiting for a lock the
// session running holder holds. It lets a test order a step after a concurrent
// write has reached the lock it has to wait for, rather than after a guessed
// delay, and it fails the test if no session ever does.
func waitUntilBlockedBy(t *testing.T, s *Store, holder *sql.Tx) {
	t.Helper()
	ctx := context.Background()
	var holderPID int
	if err := holder.QueryRowContext(ctx, `SELECT pg_backend_pid()`).Scan(&holderPID); err != nil {
		t.Fatalf("read holder backend: %v", err)
	}
	pool := s.Calendars.(*calendarRepo).pool
	deadline := time.Now().Add(30 * time.Second)
	for {
		var waiting bool
		if err := pool.QueryRowContext(ctx, blockedByQuery, holderPID).Scan(&waiting); err != nil {
			t.Fatalf("read lock waits: %v", err)
		}
		if waiting {
			return
		}
		if time.Now().After(deadline) {
			t.Fatal("no session ever waited for the holder's locks")
		}
		time.Sleep(5 * time.Millisecond)
	}
}

// committedToken reads, inside a write that is about to commit, the sync
// token its commit hands out: the collection's updated_at as the write leaves
// it.
func committedToken(t *testing.T, tx *sql.Tx, table string, id int64) time.Time {
	t.Helper()
	var token time.Time
	if err := tx.QueryRowContext(context.Background(), `SELECT updated_at FROM `+table+` WHERE id=$1`, id).Scan(&token); err != nil {
		t.Fatalf("read the token the write commits: %v", err)
	}
	return token
}

// Writes that do not come through DAV, such as the web UI's, change the same
// collections and have to hold them the same way. A member row stamped before
// its writer waited for the collection could commit behind the token the DAV
// write that held the collection handed out.
func TestPostgres_RepositoryWritesHoldTheCollectionBeforeStamping(t *testing.T) {
	const vcard = "BEGIN:VCARD\r\nVERSION:3.0\r\nUID:%[1]s\r\nFN:%[1]s\r\nEND:VCARD\r\n"
	type fixture struct {
		s                *Store
		calendarID       int64
		sourceCalendarID int64
		bookID           int64
	}
	calendarLock := func(f fixture) ([]LockPrecondition, string, int64) {
		return memberWrite(f.calendarID, "dav").LockPreconditions, "calendars", f.calendarID
	}
	calendarDAVWrite := func(ctx context.Context, tx *sql.Tx, f fixture) error {
		_, err := putCalendarObjectTx(ctx, tx, memberWrite(f.calendarID, "dav"))
		return err
	}
	for _, tt := range []struct {
		name     string
		write    func(f fixture) error
		lock     func(f fixture) ([]LockPrecondition, string, int64)
		davWrite func(ctx context.Context, tx *sql.Tx, f fixture) error
		reported func(t *testing.T, f fixture, token time.Time) bool
	}{
		{
			name: "event upsert",
			write: func(f fixture) error {
				_, err := f.s.Events.Upsert(context.Background(), Event{CalendarID: f.calendarID, UID: "ui", ResourceName: "ui", RawICAL: postgresCalendarObject("ui", "ui"), ETag: "ui"})
				return err
			},
			lock:     calendarLock,
			davWrite: calendarDAVWrite,
			reported: func(t *testing.T, f fixture, token time.Time) bool {
				return reportedSince(t, f.s, f.calendarID, token)["ui"]
			},
		},
		{
			name: "event copy with its DAV state",
			write: func(f fixture) error {
				_, err := f.s.CopyEventAndState(context.Background(), f.sourceCalendarID, f.calendarID, "copied", "copied", "etag-copy",
					fmt.Sprintf("/dav/calendars/%d/copied", f.sourceCalendarID), fmt.Sprintf("/dav/calendars/%d/copied", f.calendarID), "")
				return err
			},
			lock:     calendarLock,
			davWrite: calendarDAVWrite,
			reported: func(t *testing.T, f fixture, token time.Time) bool {
				return reportedSince(t, f.s, f.calendarID, token)["copied"]
			},
		},
		{
			name: "contact upsert",
			write: func(f fixture) error {
				_, err := f.s.Contacts.Upsert(context.Background(), Contact{AddressBookID: f.bookID, UID: "ui", ResourceName: "ui", RawVCard: fmt.Sprintf(vcard, "ui"), ETag: "ui"})
				return err
			},
			lock: func(f fixture) ([]LockPrecondition, string, int64) {
				collection := addressBookCollectionLockPath(f.bookID)
				return []LockPrecondition{{ResourcePath: collection + "/dav.vcf"}, {ResourcePath: collection}}, "address_books", f.bookID
			},
			davWrite: func(ctx context.Context, tx *sql.Tx, f fixture) error {
				_, err := putContactObjectTx(ctx, tx, ContactObjectWrite{AddressBookID: f.bookID, UID: "dav", ResourceName: "dav", RawVCard: fmt.Sprintf(vcard, "dav"), ETag: "dav"})
				return err
			},
			reported: func(t *testing.T, f fixture, token time.Time) bool {
				changed, err := f.s.Contacts.ListModifiedSincePageAfter(context.Background(), f.bookID, 0, token, 100)
				if err != nil {
					t.Fatalf("list contact changes: %v", err)
				}
				for _, contact := range changed {
					if contact.UID == "ui" {
						return true
					}
				}
				return false
			},
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			s := newPostgresStore(t)
			ctx := context.Background()
			userID, calendarID := newPostgresCalendar(t, s, "repository-order")
			source, err := s.Calendars.Create(ctx, Calendar{UserID: userID, Name: "Source"})
			if err != nil {
				t.Fatalf("create source calendar: %v", err)
			}
			if _, err := s.PutCalendarObject(ctx, memberWrite(source.ID, "copied")); err != nil {
				t.Fatalf("seed copy source: %v", err)
			}
			book, err := s.AddressBooks.Create(ctx, AddressBook{UserID: userID, Name: "Repository"})
			if err != nil {
				t.Fatalf("create address book: %v", err)
			}
			f := fixture{s: s, calendarID: calendarID, sourceCalendarID: source.ID, bookID: book.ID}

			dav, err := s.BeginTx(ctx, nil)
			if err != nil {
				t.Fatalf("begin: %v", err)
			}
			defer dav.Rollback()
			preconditions, table, id := tt.lock(f)
			if err := validateLockPreconditionsTx(ctx, dav, preconditions, preconditions[1].ResourcePath); err != nil {
				t.Fatalf("lock DAV write: %v", err)
			}
			if err := lockCollectionRowsTx(ctx, dav, table, id); err != nil {
				t.Fatalf("lock collection row: %v", err)
			}

			repository := make(chan error, 1)
			go func() { repository <- tt.write(f) }()
			waitUntilBlockedBy(t, s, dav)
			if err := tt.davWrite(ctx, dav, f); err != nil {
				t.Fatalf("DAV write: %v", err)
			}
			token := committedToken(t, dav, table, id)
			if err := dav.Commit(); err != nil {
				t.Fatalf("commit DAV write: %v", err)
			}
			if err := <-repository; err != nil {
				t.Fatalf("repository write: %v", err)
			}
			if !tt.reported(t, f, token) {
				t.Fatalf("the %s committed after sync token %v was taken is not reported since it", tt.name, token)
			}
		})
	}
}
