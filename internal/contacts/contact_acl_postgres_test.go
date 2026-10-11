package contacts

import (
	"context"
	"errors"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/jw6ventures/calcard/internal/store"
)

// A sharee's page of a book leaves out a contact whose own ACL denies them
// read, from its contacts and from its count, while the owner's page and a
// sharee the contact's ACL does not name see every contact.
func TestPostgres_ListContactsPageLeavesOutContactsTheShareeMayNotRead(t *testing.T) {
	database, owner, bookID, _ := newPostgresMoveFixture(t)
	ctx := t.Context()
	svc := NewService(database)
	var sharees []*store.User
	for _, subject := range []string{"denied", "plain"} {
		sharee, err := database.Users.UpsertOAuthUser(ctx, "sharee-"+subject, subject+"@example.test", "Sharee", subject)
		if err != nil {
			t.Fatalf("create sharee: %v", err)
		}
		if err := svc.ShareAddressBook(ctx, owner, bookID, sharee.ID, false); err != nil {
			t.Fatalf("ShareAddressBook: %v", err)
		}
		sharees = append(sharees, sharee)
	}
	denied, plain := sharees[0], sharees[1]
	for _, fn := range []string{"Alpha", "Secret", "Zulu"} {
		if _, _, err := svc.CreateContact(ctx, owner, bookID, UpsertInput{RawVCard: "BEGIN:VCARD\r\nVERSION:3.0\r\nUID:" + fn + "\r\nFN:" + fn + "\r\nEND:VCARD\r\n"}); err != nil {
			t.Fatalf("CreateContact(%s): %v", fn, err)
		}
	}
	secret := storedContact(t, svc, bookID, "Secret")
	if err := database.ACLEntries.SetACL(ctx, addressBookACLResourcePaths(bookID, contactResourceName(secret))[0], []store.ACLEntry{
		{PrincipalHref: sharePrincipalHref(denied.ID), IsGrant: false, Privilege: "read"},
	}); err != nil {
		t.Fatalf("set the contact's ACL: %v", err)
	}

	page := func(user *store.User, offset int) *store.PaginatedResult[store.Contact] {
		t.Helper()
		result, err := svc.ListContactsPage(ctx, user, store.AddressBook{ID: bookID, UserID: owner.ID}, 1, offset)
		if err != nil {
			t.Fatalf("ListContactsPage(user %d, offset %d): %v", user.ID, offset, err)
		}
		return result
	}
	names := func(result *store.PaginatedResult[store.Contact]) []string {
		var out []string
		for _, c := range result.Items {
			out = append(out, c.UID)
		}
		return out
	}
	for offset, want := range []string{"Alpha", "Zulu"} {
		if got := page(denied, offset); got.TotalCount != 2 || len(got.Items) != 1 || got.Items[0].UID != want {
			t.Errorf("denied sharee, offset %d: %v of %d, want [%s] of 2", offset, names(got), got.TotalCount, want)
		}
	}
	if got := page(denied, 2); got.TotalCount != 2 || len(got.Items) != 0 {
		t.Errorf("denied sharee, offset 2: %v of %d, want an empty page of 2", names(got), got.TotalCount)
	}
	for _, user := range []*store.User{owner, plain} {
		if got := page(user, 1); got.TotalCount != 3 || len(got.Items) != 1 || got.Items[0].UID != "Secret" {
			t.Errorf("user %d, offset 1: %v of %d, want [Secret] of 3", user.ID, names(got), got.TotalCount)
		}
	}
	// A page of no more than zero contacts is empty and still counts them,
	// however the user's page is read.
	for user, count := range map[*store.User]int{owner: 3, plain: 3, denied: 2} {
		for _, limit := range []int{0, -1} {
			got, err := svc.ListContactsPage(ctx, user, store.AddressBook{ID: bookID, UserID: owner.ID}, limit, 0)
			if err != nil || len(got.Items) != 0 || got.TotalCount != count {
				t.Errorf("user %d, limit %d: %v of %d, %v; want an empty page of %d", user.ID, limit, names(got), got.TotalCount, err, count)
			}
		}
	}
}

// The contacts a listing shows each user are exactly those GetContact lets the
// user read, whatever the ACL: entries on a contact's path with or without its
// .vcf, an extension in capitals, a name padded with white space, a principal
// stored without its trailing slash, group principals, ACE order, a grant on
// the contact alone, and entries on another book's contact of the same name.
func TestPostgres_ListingsShowTheContactsGetContactLetsTheUserRead(t *testing.T) {
	database, owner, bookID, otherBookID := newPostgresMoveFixture(t)
	ctx := t.Context()
	svc := NewService(database)
	users := map[string]*store.User{"owner": owner}
	for _, name := range []string{"editor", "viewer", "stranger"} {
		user, err := database.Users.UpsertOAuthUser(ctx, "listing-"+name, name+"@example.test", "Listing", name)
		if err != nil {
			t.Fatalf("create %s: %v", name, err)
		}
		users[name] = user
	}
	if err := svc.ShareAddressBook(ctx, owner, bookID, users["editor"].ID, true); err != nil {
		t.Fatal(err)
	}
	if err := svc.ShareAddressBook(ctx, owner, bookID, users["viewer"].ID, false); err != nil {
		t.Fatal(err)
	}
	for _, c := range []struct{ uid, resourceName string }{
		{"a1", "a1"}, {"b2", "b2.vcf"}, {"c3", "C3.VCF"}, {"d4", "d4"},
		{"e5", "e5"}, {"f6", "f6"}, {"g7", "g7"}, {"h8", "h8"}, {"i9", "i9"},
		{"j10", " j10\u00a0"}, {"k11", "k11"}, {"m13", "m13.vcf.VCF"}, {"n14", "legacy/n14"},
	} {
		card := "BEGIN:VCARD\r\nVERSION:3.0\r\nUID:" + c.uid + "\r\nFN:Contact " + c.uid + "\r\nEND:VCARD\r\n"
		if _, err := database.Contacts.Upsert(ctx, store.Contact{AddressBookID: bookID, UID: c.uid, ResourceName: c.resourceName, RawVCard: card, ETag: c.uid}); err != nil {
			t.Fatalf("store %s: %v", c.uid, err)
		}
	}
	editor := sharePrincipalHref(users["editor"].ID)
	viewer := sharePrincipalHref(users["viewer"].ID)
	stranger := sharePrincipalHref(users["stranger"].ID)
	member := func(name string) string { return addressBookACLCollectionPath(bookID) + "/" + name }
	acls := map[string][]store.ACLEntry{
		member("a1"):         {{PrincipalHref: editor, Privilege: "read"}},
		member("b2"):         {{PrincipalHref: editor, Privilege: "read"}},
		member("C3.VCF"):     {{PrincipalHref: editor, Privilege: "all"}, {PrincipalHref: "DAV:authenticated", IsGrant: true, Privilege: "read", Position: 1}},
		member("d4"):         {{PrincipalHref: strings.TrimSuffix(editor, "/"), Privilege: "read"}},
		member("e5"):         {{PrincipalHref: "DAV:all", IsGrant: true, Privilege: "read"}, {PrincipalHref: editor, Privilege: "read", Position: 1}},
		member("e5.vcf"):     {{PrincipalHref: viewer, Privilege: "read"}},
		member("f6"):         {{PrincipalHref: editor, Privilege: "write"}},
		member("g7"):         {{PrincipalHref: "DAV:authenticated", Privilege: "read"}},
		member("h8"):         {{PrincipalHref: stranger, IsGrant: true, Privilege: "read"}},
		member("j10"):        {{PrincipalHref: editor, Privilege: "read"}},
		member("k11.vcf"):    {{PrincipalHref: editor, Privilege: "read"}},
		member("m13.vcf"):    {{PrincipalHref: editor, Privilege: "read"}},
		member("legacy/n14"): {{PrincipalHref: editor, Privilege: "read"}},
		addressBookACLCollectionPath(otherBookID) + "/i9": {{PrincipalHref: editor, Privilege: "all"}},
	}
	for resourcePath, entries := range acls {
		if err := database.ACLEntries.SetACL(ctx, resourcePath, entries); err != nil {
			t.Fatalf("set the ACL of %s: %v", resourcePath, err)
		}
	}
	all, err := database.Contacts.ListForBookFiltered(ctx, bookID, store.ContactFilter{})
	if err != nil {
		t.Fatal(err)
	}
	book := store.AddressBook{ID: bookID, UserID: owner.ID}

	for name, user := range users {
		t.Run(name, func(t *testing.T) {
			var want []string
			for _, c := range all {
				if _, err := svc.GetContact(ctx, user, bookID, c.UID); err == nil {
					want = append(want, c.UID)
				}
			}
			var paged []string
			for offset := 0; ; offset += 2 {
				page, err := svc.ListContactsPage(ctx, user, book, 2, offset)
				if err != nil {
					t.Fatalf("ListContactsPage(offset %d): %v", offset, err)
				}
				if page.TotalCount != len(want) {
					t.Fatalf("ListContactsPage(offset %d) count = %d, want %d", offset, page.TotalCount, len(want))
				}
				if len(page.Items) == 0 {
					break
				}
				for _, c := range page.Items {
					paged = append(paged, c.UID)
				}
			}
			if !slices.Equal(paged, want) {
				t.Errorf("ListContactsPage shows %v, GetContact allows %v", paged, want)
			}
			listed, err := svc.ListContacts(ctx, user, bookID, store.ContactFilter{})
			if name == "stranger" {
				if !errors.Is(err, ErrNotFound) {
					t.Fatalf("ListContacts() error = %v, want ErrNotFound for a user who cannot read the book", err)
				}
				return
			}
			if err != nil {
				t.Fatalf("ListContacts: %v", err)
			}
			var listedUIDs []string
			for _, c := range listed {
				listedUIDs = append(listedUIDs, c.UID)
			}
			if !slices.Equal(listedUIDs, want) {
				t.Errorf("ListContacts shows %v, GetContact allows %v", listedUIDs, want)
			}
		})
	}
}

func newPostgresMoveFixture(t *testing.T) (*store.Store, *store.User, int64, int64) {
	t.Helper()
	database := newContactsPostgresStore(t)
	ctx := t.Context()
	user, err := database.Users.UpsertOAuthUser(ctx, "contact-move", "move@example.test", "Move", "Test")
	if err != nil {
		t.Fatalf("create user: %v", err)
	}
	var bookIDs []int64
	for _, name := range []string{"From", "To"} {
		book, err := database.AddressBooks.Create(ctx, store.AddressBook{UserID: user.ID, Name: name})
		if err != nil {
			t.Fatalf("create address book: %v", err)
		}
		bookIDs = append(bookIDs, book.ID)
	}
	return database, user, bookIDs[0], bookIDs[1]
}

// The DAV state bound to a contact's path goes with the contact when it moves,
// as a DAV MOVE takes it: its dead properties and its own ACL are found at the
// new path and nowhere else (RFC 3744 section 7.3), and no lock is left on the
// path it left.
func TestPostgres_MoveContactTakesItsDAVStateToTheNewPath(t *testing.T) {
	database, user, fromBookID, toBookID := newPostgresMoveFixture(t)
	ctx := t.Context()
	svc := NewService(database)
	contact, _, err := svc.CreateContact(ctx, user, fromBookID, UpsertInput{RawVCard: "BEGIN:VCARD\r\nVERSION:3.0\r\nUID:alice\r\nFN:Alice\r\nEND:VCARD\r\n"})
	if err != nil {
		t.Fatalf("CreateContact: %v", err)
	}
	oldPath := addressBookACLResourcePaths(fromBookID, contactResourceName(*contact))[0]
	newPath := addressBookACLResourcePaths(toBookID, contactResourceName(*contact))[0]
	if err := database.DeadProperties.Apply(ctx, oldPath, []store.DeadPropertyMutation{{NamespaceURI: "urn:test", LocalName: "note", InnerXML: "kept"}}); err != nil {
		t.Fatalf("set dead property: %v", err)
	}
	if err := database.ACLEntries.SetACL(ctx, oldPath, []store.ACLEntry{{PrincipalHref: "/dav/principals/999/", IsGrant: true, Privilege: "read"}}); err != nil {
		t.Fatalf("set ACL: %v", err)
	}
	if _, err := database.Locks.Create(ctx, store.Lock{
		Token: "opaquelocktoken:contact-move", ResourcePath: oldPath, UserID: user.ID,
		LockScope: "exclusive", LockType: "write", Depth: "0", TimeoutSeconds: 300, ExpiresAt: time.Now().Add(5 * time.Minute),
	}); err != nil {
		t.Fatalf("create lock: %v", err)
	}

	if err := svc.MoveContact(ctx, user, fromBookID, toBookID, "alice"); err != nil {
		t.Fatalf("MoveContact: %v", err)
	}

	if moved, _ := database.Contacts.GetByUID(ctx, toBookID, "alice"); moved == nil {
		t.Fatal("the contact is not in the destination book")
	}
	if left, _ := database.Contacts.GetByUID(ctx, fromBookID, "alice"); left != nil {
		t.Fatal("the contact is still in the source book")
	}
	properties, err := database.DeadProperties.ListByResources(ctx, []string{oldPath, newPath})
	if err != nil {
		t.Fatalf("list dead properties: %v", err)
	}
	if len(properties) != 1 || properties[0].ResourcePath != newPath || properties[0].InnerXML != "kept" {
		t.Fatalf("dead properties = %#v, want the note at %s only", properties, newPath)
	}
	for resourcePath, want := range map[string]int{oldPath: 0, newPath: 1} {
		entries, err := database.ACLEntries.ListByResource(ctx, resourcePath)
		if err != nil {
			t.Fatalf("list ACL of %s: %v", resourcePath, err)
		}
		if len(entries) != want {
			t.Errorf("ACL of %s = %#v, want %d entries", resourcePath, entries, want)
		}
	}
	locks, err := database.Locks.ListByResource(ctx, oldPath)
	if err != nil {
		t.Fatalf("list locks: %v", err)
	}
	if len(locks) != 0 {
		t.Fatalf("locks left on the old path: %#v", locks)
	}
}

// destinationRaceContacts commits another client's contact into the
// destination book right after the move's first lookup there by resource
// name, which is its check that the destination is free.
type destinationRaceContacts struct {
	store.ContactRepository
	bookID int64
	race   func() error
}

func (r *destinationRaceContacts) GetByResourceName(ctx context.Context, bookID int64, name string) (*store.Contact, error) {
	contact, err := r.ContactRepository.GetByResourceName(ctx, bookID, name)
	if err == nil && bookID == r.bookID && r.race != nil {
		race := r.race
		r.race = nil
		err = race()
	}
	return contact, err
}

func TestPostgres_MoveContactKeepsADestinationContactCreatedWhileItRuns(t *testing.T) {
	database, user, fromBookID, toBookID := newPostgresMoveFixture(t)
	ctx := t.Context()
	if _, _, err := NewService(database).CreateContact(ctx, user, fromBookID, UpsertInput{RawVCard: "BEGIN:VCARD\r\nVERSION:3.0\r\nUID:alice\r\nFN:Mine\r\nEND:VCARD\r\n"}); err != nil {
		t.Fatalf("CreateContact: %v", err)
	}
	racing := *database
	racing.Contacts = &destinationRaceContacts{ContactRepository: database.Contacts, bookID: toBookID, race: func() error {
		_, _, err := NewService(database).CreateContact(ctx, user, toBookID, UpsertInput{RawVCard: "BEGIN:VCARD\r\nVERSION:3.0\r\nUID:alice\r\nFN:Theirs\r\nEND:VCARD\r\n"})
		return err
	}}

	if err := NewService(&racing).MoveContact(ctx, user, fromBookID, toBookID, "alice"); !errors.Is(err, ErrConflict) || errors.Is(err, ErrChangedConcurrently) {
		t.Fatalf("MoveContact() error = %v, want the conflict of a contact already there", err)
	}
	theirs, err := database.Contacts.GetByUID(ctx, toBookID, "alice")
	if err != nil || theirs == nil || !containsContentLine(theirs.RawVCard, "FN:Theirs") {
		t.Fatalf("the destination contact created during the move was replaced: %#v, %v", theirs, err)
	}
	if mine, _ := database.Contacts.GetByUID(ctx, fromBookID, "alice"); mine == nil {
		t.Fatal("the source contact was removed although the move could not complete")
	}
}
