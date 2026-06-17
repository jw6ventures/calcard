package contacts

import (
	"context"
	"errors"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/jw6ventures/calcard/internal/store"
)

// --- in-memory fakes -------------------------------------------------------

type fakeAB struct{ books map[int64]*store.AddressBook }

func (f *fakeAB) GetByID(_ context.Context, id int64) (*store.AddressBook, error) {
	if b, ok := f.books[id]; ok {
		cp := *b
		return &cp, nil
	}
	return nil, nil
}
func (f *fakeAB) ListByUser(_ context.Context, userID int64) ([]store.AddressBook, error) {
	var out []store.AddressBook
	for _, b := range f.books {
		if b.UserID == userID {
			out = append(out, *b)
		}
	}
	return out, nil
}
func (f *fakeAB) Create(context.Context, store.AddressBook) (*store.AddressBook, error) {
	return nil, nil
}
func (f *fakeAB) Update(context.Context, int64, int64, string, *string) error    { return nil }
func (f *fakeAB) UpdateProperties(context.Context, int64, string, *string) error { return nil }
func (f *fakeAB) Rename(context.Context, int64, int64, string) error             { return nil }
func (f *fakeAB) Delete(context.Context, int64, int64) error                     { return nil }

type fakeContacts struct{ items map[string]store.Contact } // key bookID:uid

func ckey(bookID int64, uid string) string { return strconv.FormatInt(bookID, 10) + ":" + uid }

func (f *fakeContacts) Upsert(_ context.Context, c store.Contact) (*store.Contact, error) {
	if c.ResourceName == "" {
		c.ResourceName = c.UID
	}
	c.LastModified = time.Now().UTC()
	f.items[ckey(c.AddressBookID, c.UID)] = c
	cp := c
	return &cp, nil
}
func (f *fakeContacts) DeleteByUID(_ context.Context, bookID int64, uid string) error {
	delete(f.items, ckey(bookID, uid))
	return nil
}
func (f *fakeContacts) GetByUID(_ context.Context, bookID int64, uid string) (*store.Contact, error) {
	if c, ok := f.items[ckey(bookID, uid)]; ok {
		cp := c
		return &cp, nil
	}
	return nil, nil
}
func (f *fakeContacts) GetByResourceName(_ context.Context, bookID int64, rn string) (*store.Contact, error) {
	for _, c := range f.items {
		if c.AddressBookID == bookID && c.ResourceName == rn {
			cp := c
			return &cp, nil
		}
	}
	return nil, nil
}
func (f *fakeContacts) ListForBook(_ context.Context, bookID int64) ([]store.Contact, error) {
	var out []store.Contact
	for _, c := range f.items {
		if c.AddressBookID == bookID {
			out = append(out, c)
		}
	}
	return out, nil
}
func (f *fakeContacts) ListForBookFiltered(ctx context.Context, bookID int64, _ store.ContactFilter) ([]store.Contact, error) {
	return f.ListForBook(ctx, bookID)
}
func (f *fakeContacts) ListForBookPaginated(context.Context, int64, int, int) (*store.PaginatedResult[store.Contact], error) {
	return nil, nil
}
func (f *fakeContacts) ListByUIDs(context.Context, int64, []string) ([]store.Contact, error) {
	return nil, nil
}
func (f *fakeContacts) ListModifiedSince(context.Context, int64, time.Time) ([]store.Contact, error) {
	return nil, nil
}
func (f *fakeContacts) ListRecentByUser(context.Context, int64, int) ([]store.Contact, error) {
	return nil, nil
}
func (f *fakeContacts) MaxLastModified(context.Context, int64) (time.Time, error) {
	return time.Time{}, nil
}
func (f *fakeContacts) ListWithBirthdaysByUser(context.Context, int64) ([]store.Contact, error) {
	return nil, nil
}
func (f *fakeContacts) MoveToAddressBook(context.Context, int64, int64, string, string) error {
	return nil
}
func (f *fakeContacts) CopyToAddressBook(context.Context, int64, int64, string, string, string) (*store.Contact, error) {
	return nil, nil
}

type fakeACL struct{ entries []store.ACLEntry }

func (f *fakeACL) SetACL(_ context.Context, resourcePath string, entries []store.ACLEntry) error {
	kept := f.entries[:0:0]
	for _, e := range f.entries {
		if e.ResourcePath != resourcePath {
			kept = append(kept, e)
		}
	}
	for _, e := range entries {
		e.ResourcePath = resourcePath
		if e.CreatedAt.IsZero() {
			e.CreatedAt = time.Now().UTC()
		}
		kept = append(kept, e)
	}
	f.entries = kept
	return nil
}
func (f *fakeACL) ListByResource(_ context.Context, resourcePath string) ([]store.ACLEntry, error) {
	var out []store.ACLEntry
	for _, e := range f.entries {
		if e.ResourcePath == resourcePath {
			out = append(out, e)
		}
	}
	return out, nil
}
func (f *fakeACL) ListByPrincipal(_ context.Context, principalHref string) ([]store.ACLEntry, error) {
	var out []store.ACLEntry
	for _, e := range f.entries {
		if e.PrincipalHref == principalHref {
			out = append(out, e)
		}
	}
	return out, nil
}
func (f *fakeACL) HasPrivilege(context.Context, string, string, string) (bool, error) {
	return false, nil
}
func (f *fakeACL) DeletePrincipalEntriesByResourcePrefix(_ context.Context, principalHref, prefix string) error {
	kept := f.entries[:0:0]
	for _, e := range f.entries {
		if e.PrincipalHref == principalHref && strings.HasPrefix(e.ResourcePath, prefix) {
			continue
		}
		kept = append(kept, e)
	}
	f.entries = kept
	return nil
}
func (f *fakeACL) MoveResourcePath(context.Context, string, string) error { return nil }
func (f *fakeACL) Delete(context.Context, string) error                   { return nil }

type fakeUsers struct{ users map[int64]*store.User }

func (f *fakeUsers) UpsertOAuthUser(context.Context, string, string) (*store.User, error) {
	return nil, nil
}
func (f *fakeUsers) GetByID(_ context.Context, id int64) (*store.User, error) {
	if u, ok := f.users[id]; ok {
		cp := *u
		return &cp, nil
	}
	return nil, nil
}
func (f *fakeUsers) GetByEmail(context.Context, string) (*store.User, error) { return nil, nil }
func (f *fakeUsers) ListActive(context.Context) ([]store.User, error)        { return nil, nil }
func (f *fakeUsers) MarkOnboardingComplete(context.Context, int64) error     { return nil }

// --- helpers ---------------------------------------------------------------

func newTestService() (*Service, *fakeACL) {
	acl := &fakeACL{}
	st := &store.Store{
		AddressBooks: &fakeAB{books: map[int64]*store.AddressBook{
			1: {ID: 1, UserID: 1, Name: "Owner book"},
		}},
		Contacts: &fakeContacts{items: map[string]store.Contact{
			"1:c1": {AddressBookID: 1, UID: "c1", ResourceName: "c1", ETag: "e1", RawVCard: "BEGIN:VCARD\r\nUID:c1\r\nFN:Alice\r\nEND:VCARD\r\n"},
		}},
		ACLEntries: acl,
		Users: &fakeUsers{users: map[int64]*store.User{
			1: {ID: 1, PrimaryEmail: "owner@example.com"},
			2: {ID: 2, PrimaryEmail: "sharee@example.com"},
			3: {ID: 3, PrimaryEmail: "stranger@example.com"},
		}},
	}
	return NewService(st), acl
}

func uvCard(uid string) UpsertInput {
	return UpsertInput{Structured: &StructuredInput{UID: uid, DisplayName: "New " + uid}}
}

var (
	owner    = &store.User{ID: 1, PrimaryEmail: "owner@example.com"}
	sharee   = &store.User{ID: 2, PrimaryEmail: "sharee@example.com"}
	stranger = &store.User{ID: 3, PrimaryEmail: "stranger@example.com"}
)

// --- tests -----------------------------------------------------------------

func TestStrangerCannotSeeBook(t *testing.T) {
	svc, _ := newTestService()
	if _, err := svc.GetAddressBook(context.Background(), stranger, 1); !errors.Is(err, ErrNotFound) {
		t.Fatalf("stranger GetAddressBook err=%v, want ErrNotFound", err)
	}
	books, err := svc.ListAccessibleAddressBooks(context.Background(), stranger)
	if err != nil {
		t.Fatal(err)
	}
	if len(books) != 0 {
		t.Fatalf("stranger sees %d books, want 0", len(books))
	}
}

func TestReadOnlyShareGrantsReadNotWrite(t *testing.T) {
	svc, _ := newTestService()
	if err := svc.ShareAddressBook(context.Background(), owner, 1, 2, false); err != nil {
		t.Fatal(err)
	}

	if _, err := svc.GetAddressBook(context.Background(), sharee, 1); err != nil {
		t.Fatalf("sharee GetAddressBook err=%v, want nil", err)
	}
	contactsList, err := svc.ListContacts(context.Background(), sharee, 1, store.ContactFilter{})
	if err != nil {
		t.Fatalf("sharee ListContacts err=%v", err)
	}
	if len(contactsList) != 1 {
		t.Fatalf("sharee sees %d contacts, want 1", len(contactsList))
	}

	if _, _, err := svc.CreateContact(context.Background(), sharee, 1, uvCard("new1")); !errors.Is(err, ErrForbidden) {
		t.Fatalf("read-only sharee CreateContact err=%v, want ErrForbidden", err)
	}
	if _, _, err := svc.UpdateContact(context.Background(), sharee, 1, "c1", uvCard("c1")); !errors.Is(err, ErrForbidden) {
		t.Fatalf("read-only sharee UpdateContact err=%v, want ErrForbidden", err)
	}
	if err := svc.DeleteContact(context.Background(), sharee, 1, "c1", "", ""); !errors.Is(err, ErrForbidden) {
		t.Fatalf("read-only sharee DeleteContact err=%v, want ErrForbidden", err)
	}

	access, err := svc.AddressBookAccessFor(context.Background(), sharee, store.AddressBook{ID: 1, UserID: 1})
	if err != nil {
		t.Fatal(err)
	}
	if !access.Shared || access.Editor {
		t.Fatalf("read-only access = %+v, want shared+non-editor", access)
	}
}

func TestEditorShareGrantsWrite(t *testing.T) {
	svc, _ := newTestService()
	if err := svc.ShareAddressBook(context.Background(), owner, 1, 2, true); err != nil {
		t.Fatal(err)
	}

	if _, _, err := svc.CreateContact(context.Background(), sharee, 1, uvCard("new1")); err != nil {
		t.Fatalf("editor CreateContact err=%v", err)
	}
	if _, _, err := svc.UpdateContact(context.Background(), sharee, 1, "c1", uvCard("c1")); err != nil {
		t.Fatalf("editor UpdateContact err=%v", err)
	}
	if err := svc.DeleteContact(context.Background(), sharee, 1, "c1", "", ""); err != nil {
		t.Fatalf("editor DeleteContact err=%v", err)
	}

	access, err := svc.AddressBookAccessFor(context.Background(), sharee, store.AddressBook{ID: 1, UserID: 1})
	if err != nil {
		t.Fatal(err)
	}
	if !access.Shared || !access.Editor {
		t.Fatalf("editor access = %+v, want shared+editor", access)
	}
}

func TestShareListAndUnshareRoundTrip(t *testing.T) {
	svc, _ := newTestService()
	if err := svc.ShareAddressBook(context.Background(), owner, 1, 2, true); err != nil {
		t.Fatal(err)
	}
	shares, err := svc.ListAddressBookShares(context.Background(), owner, 1)
	if err != nil {
		t.Fatal(err)
	}
	if len(shares) != 1 || shares[0].UserID != 2 || !shares[0].Editor {
		t.Fatalf("shares=%+v, want one editor share for user 2", shares)
	}

	// Re-sharing as read-only should replace the editor grant, not duplicate it.
	if err := svc.ShareAddressBook(context.Background(), owner, 1, 2, false); err != nil {
		t.Fatal(err)
	}
	shares, _ = svc.ListAddressBookShares(context.Background(), owner, 1)
	if len(shares) != 1 || shares[0].Editor {
		t.Fatalf("after downgrade shares=%+v, want one read-only share", shares)
	}

	if err := svc.UnshareAddressBook(context.Background(), owner, 1, 2); err != nil {
		t.Fatal(err)
	}
	shares, _ = svc.ListAddressBookShares(context.Background(), owner, 1)
	if len(shares) != 0 {
		t.Fatalf("after unshare shares=%+v, want none", shares)
	}
}

func TestShareRejectsSelfAndUnknownTarget(t *testing.T) {
	svc, _ := newTestService()
	if err := svc.ShareAddressBook(context.Background(), owner, 1, 1, true); !errors.Is(err, ErrBadRequest) {
		t.Fatalf("self-share err=%v, want ErrBadRequest", err)
	}
	if err := svc.ShareAddressBook(context.Background(), owner, 1, 999, true); !errors.Is(err, ErrBadRequest) {
		t.Fatalf("unknown-target err=%v, want ErrBadRequest", err)
	}
	// Non-owner cannot share.
	if err := svc.ShareAddressBook(context.Background(), sharee, 1, 3, true); !errors.Is(err, ErrNotFound) {
		t.Fatalf("non-owner share err=%v, want ErrNotFound", err)
	}
}

func TestShareeCanLeaveButNotRemoveOthers(t *testing.T) {
	svc, _ := newTestService()
	if err := svc.ShareAddressBook(context.Background(), owner, 1, 2, false); err != nil {
		t.Fatal(err)
	}
	// Sharee removing another principal is rejected.
	if err := svc.UnshareAddressBook(context.Background(), sharee, 1, 3); !errors.Is(err, ErrNotFound) {
		t.Fatalf("sharee removing other err=%v, want ErrNotFound", err)
	}
	// Sharee leaving their own share succeeds.
	if err := svc.UnshareAddressBook(context.Background(), sharee, 1, 2); err != nil {
		t.Fatalf("sharee leave err=%v", err)
	}
	if _, err := svc.GetAddressBook(context.Background(), sharee, 1); !errors.Is(err, ErrNotFound) {
		t.Fatalf("after leave GetAddressBook err=%v, want ErrNotFound", err)
	}
}

func TestListAccessibleIncludesSharedBook(t *testing.T) {
	svc, _ := newTestService()
	if err := svc.ShareAddressBook(context.Background(), owner, 1, 2, true); err != nil {
		t.Fatal(err)
	}
	books, err := svc.ListAccessibleAddressBooks(context.Background(), sharee)
	if err != nil {
		t.Fatal(err)
	}
	if len(books) != 1 || books[0].ID != 1 || !books[0].Shared || !books[0].Editor {
		t.Fatalf("sharee accessible=%+v, want shared editor book 1", books)
	}
}
