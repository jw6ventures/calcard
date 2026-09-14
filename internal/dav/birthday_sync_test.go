package dav

import (
	"context"
	"encoding/xml"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/jw6ventures/calcard/internal/store"
)

// birthdaySyncServer holds one address book whose state versions the generated
// birthday collection, and one contact in it carrying a birthday.
func birthdaySyncServer(book store.AddressBook, contacts map[string]*store.Contact) *DavServer {
	return &DavServer{store: &store.Store{
		Calendars:        &fakeCalendarRepo{},
		Events:           &fakeEventRepo{},
		DeletedResources: &fakeDeletedResourceRepo{},
		AddressBooks:     &fakeAddressBookRepo{books: map[int64]*store.AddressBook{book.ID: &book}},
		Contacts:         &fakeContactRepo{contacts: contacts},
	}}
}

func birthdayCollectionProps(t *testing.T, h *DavServer, user *store.User) prop {
	t.Helper()
	res, err := h.calendarResponses(context.Background(), birthdayCalendarHref(), "0", user)
	if err != nil {
		t.Fatalf("calendarResponses on the birthday collection returned error: %v", err)
	}
	if len(res) != 1 {
		t.Fatalf("birthday collection PROPFIND returned %d responses, want 1", len(res))
	}
	if len(res[0].Propstat) == 0 {
		t.Fatalf("birthday collection response carried no propstat")
	}
	return res[0].Propstat[0].Prop
}

func birthdayContact(uid string, birthday time.Time, lastModified time.Time) *store.Contact {
	name := uid
	return &store.Contact{
		AddressBookID: 5,
		UID:           uid,
		ResourceName:  uid,
		DisplayName:   &name,
		Birthday:      &birthday,
		LastModified:  lastModified,
	}
}

// A client re-reads a collection only when its version moves, so the generated
// birthday collection has to version itself from the contacts it is built from.
func TestBirthdayCalendarVersionTracksContactState(t *testing.T) {
	user := &store.User{ID: 1}
	created := time.Date(2026, 9, 1, 10, 0, 0, 0, time.UTC)
	birthday := time.Date(1990, 5, 15, 0, 0, 0, 0, time.UTC)

	book := store.AddressBook{ID: 5, UserID: 1, Name: "Contacts", CTag: 4, UpdatedAt: created}
	h := birthdaySyncServer(book, map[string]*store.Contact{
		"5:alice": birthdayContact("alice", birthday, created),
	})

	before := birthdayCollectionProps(t, h, user)
	if before.CTag == "" {
		t.Fatal("birthday collection advertised no getctag")
	}
	if before.SyncToken == "" {
		t.Fatal("birthday collection advertised no sync-token")
	}

	// Adding a contact bumps the owning book's ctag and updated_at, which is
	// the only record the generated collection has that its content moved.
	books := h.store.AddressBooks.(*fakeAddressBookRepo)
	books.books[5].CTag = 5
	books.books[5].UpdatedAt = created.Add(time.Hour)
	contacts := h.store.Contacts.(*fakeContactRepo)
	contacts.contacts["5:bob"] = birthdayContact("bob", birthday, created.Add(time.Hour))

	after := birthdayCollectionProps(t, h, user)
	if after.CTag == before.CTag {
		t.Errorf("getctag stayed %q after a birthday contact was added; a client never re-reads the collection", after.CTag)
	}
	if after.SyncToken == before.SyncToken {
		t.Errorf("sync-token stayed %q after a birthday contact was added; a client never re-reads the collection", after.SyncToken)
	}
}

// An address book removed whole takes its contacts with it without touching any
// surviving book, so the count of books is part of the collection's version.
func TestBirthdayCalendarVersionTracksRemovedAddressBook(t *testing.T) {
	user := &store.User{ID: 1}
	created := time.Date(2026, 9, 1, 10, 0, 0, 0, time.UTC)
	birthday := time.Date(1990, 5, 15, 0, 0, 0, 0, time.UTC)

	book := store.AddressBook{ID: 5, UserID: 1, Name: "Contacts", CTag: 4, UpdatedAt: created}
	h := birthdaySyncServer(book, map[string]*store.Contact{
		"5:alice": birthdayContact("alice", birthday, created),
	})
	books := h.store.AddressBooks.(*fakeAddressBookRepo)
	books.books[6] = &store.AddressBook{ID: 6, UserID: 1, Name: "Work", CTag: 1, UpdatedAt: created}

	before := birthdayCollectionProps(t, h, user)

	delete(books.books, 6)

	after := birthdayCollectionProps(t, h, user)
	if after.CTag == before.CTag {
		t.Errorf("getctag stayed %q after an address book was removed", after.CTag)
	}
	if after.SyncToken == before.SyncToken {
		t.Errorf("sync-token stayed %q after an address book was removed", after.SyncToken)
	}
}

func birthdaySyncReport(t *testing.T, h *DavServer, user *store.User, token string) ([]response, string, error) {
	t.Helper()
	report := reportRequest{
		XMLName:   xml.Name{Local: "sync-collection"},
		SyncToken: token,
		Prop:      &reportProp{},
	}
	return h.birthdayCalendarReportResponses(context.Background(), user, "/dav/principals/1/", birthdayCalendarHref(), "", report, nil)
}

// RFC 6578 §3.2: a collection that keeps no change history cannot answer a
// token naming an older state, so it refuses the token and the client
// resynchronizes against the collection whole. Without the refusal a contact
// removed since the token was issued would never be reported gone.
func TestBirthdayCalendarSyncCollectionRefusesStaleToken(t *testing.T) {
	user := &store.User{ID: 1}
	created := time.Date(2026, 9, 1, 10, 0, 0, 0, time.UTC)
	birthday := time.Date(1990, 5, 15, 0, 0, 0, 0, time.UTC)

	book := store.AddressBook{ID: 5, UserID: 1, Name: "Contacts", CTag: 4, UpdatedAt: created}
	h := birthdaySyncServer(book, map[string]*store.Contact{
		"5:alice": birthdayContact("alice", birthday, created),
		"5:bob":   birthdayContact("bob", birthday, created),
	})

	_, token, err := birthdaySyncReport(t, h, user, "")
	if err != nil {
		t.Fatalf("initial sync-collection returned error: %v", err)
	}
	if token == "" {
		t.Fatal("initial sync-collection returned no sync-token")
	}

	books := h.store.AddressBooks.(*fakeAddressBookRepo)
	books.books[5].CTag = 5
	books.books[5].UpdatedAt = created.Add(time.Hour)
	contacts := h.store.Contacts.(*fakeContactRepo)
	delete(contacts.contacts, "5:bob")

	if _, _, err := birthdaySyncReport(t, h, user, token); !errors.Is(err, errInvalidSyncToken) {
		t.Fatalf("sync-collection with a stale token = %v, want errInvalidSyncToken", err)
	}
}

// A token naming the current state has nothing to report: the collection is
// answered alone, without re-listing every generated resource.
func TestBirthdayCalendarSyncCollectionCurrentTokenReportsNoChanges(t *testing.T) {
	user := &store.User{ID: 1}
	created := time.Date(2026, 9, 1, 10, 0, 0, 0, time.UTC)
	birthday := time.Date(1990, 5, 15, 0, 0, 0, 0, time.UTC)

	book := store.AddressBook{ID: 5, UserID: 1, Name: "Contacts", CTag: 4, UpdatedAt: created}
	h := birthdaySyncServer(book, map[string]*store.Contact{
		"5:alice": birthdayContact("alice", birthday, created),
	})

	first, token, err := birthdaySyncReport(t, h, user, "")
	if err != nil {
		t.Fatalf("initial sync-collection returned error: %v", err)
	}
	if len(first) != 2 {
		t.Fatalf("initial sync-collection returned %d responses, want the collection plus one birthday", len(first))
	}

	second, again, err := birthdaySyncReport(t, h, user, token)
	if err != nil {
		t.Fatalf("sync-collection with the current token returned error: %v", err)
	}
	if again != token {
		t.Errorf("sync-token changed to %q with no change to the collection, want %q", again, token)
	}
	if len(second) != 1 {
		t.Fatalf("sync-collection with the current token returned %d responses, want the collection alone", len(second))
	}
	if !strings.HasSuffix(second[0].Href, "/") {
		t.Errorf("collection response href = %q, want a collection href", second[0].Href)
	}
}

// A token for another collection, or one the server never spelled, is refused
// rather than answered against the birthday collection.
func TestBirthdayCalendarSyncCollectionRefusesForeignToken(t *testing.T) {
	user := &store.User{ID: 1}
	created := time.Date(2026, 9, 1, 10, 0, 0, 0, time.UTC)
	book := store.AddressBook{ID: 5, UserID: 1, Name: "Contacts", CTag: 4, UpdatedAt: created}
	h := birthdaySyncServer(book, nil)

	for name, token := range map[string]string{
		"other calendar": buildSyncToken("cal", 2, created),
		"address book":   buildSyncToken("card", birthdayCalendarID, created),
		"malformed":      "not-a-sync-token",
	} {
		if _, _, err := birthdaySyncReport(t, h, user, token); !errors.Is(err, errInvalidSyncToken) {
			t.Errorf("sync-collection with a %s token = %v, want errInvalidSyncToken", name, err)
		}
	}
}
