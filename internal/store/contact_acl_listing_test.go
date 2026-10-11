package store

import (
	"slices"
	"testing"
)

// A listing's reads share one snapshot: a contact committed after its ACL read
// is not on its page, nor in its count, so the page is cut by the decisions
// made over the same state.
func TestPostgres_ContactListingReadsOneSnapshot(t *testing.T) {
	s := newPostgresStore(t)
	ctx := t.Context()
	user, err := s.Users.UpsertOAuthUser(ctx, "listing-snapshot", "snapshot@example.test", "Listing", "Snapshot")
	if err != nil {
		t.Fatal(err)
	}
	book, err := s.AddressBooks.Create(ctx, AddressBook{UserID: user.ID, Name: "Snapshot"})
	if err != nil {
		t.Fatal(err)
	}
	card := func(uid string) Contact {
		return Contact{AddressBookID: book.ID, UID: uid, RawVCard: "BEGIN:VCARD\r\nVERSION:3.0\r\nUID:" + uid + "\r\nFN:" + uid + "\r\nEND:VCARD\r\n", ETag: uid}
	}
	if _, err := s.Contacts.Upsert(ctx, card("before")); err != nil {
		t.Fatal(err)
	}
	var page *PaginatedResult[Contact]
	err = s.ReadContactListing(ctx, func(listing *ContactListing) error {
		if _, err := listing.CollectionACLEntries(ctx, "/dav/addressbooks/1", []string{"DAV:all"}); err != nil {
			return err
		}
		if _, err := s.Contacts.Upsert(ctx, card("after")); err != nil {
			return err
		}
		page, err = listing.Page(ctx, book.ID, ContactSelection{Except: true}, 10, 0)
		return err
	})
	if err != nil {
		t.Fatal(err)
	}
	if page.TotalCount != 1 || len(page.Items) != 1 || page.Items[0].UID != "before" {
		t.Fatalf("page = %+v of %d, want the one contact there when the listing began", page.Items, page.TotalCount)
	}
}

// A book's ACL read holds the entries on the book and on every path beneath
// it, and none on another book whose path shares the book's as a prefix, nor
// on a path that only starts with the book's path text.
func TestPostgres_CollectionACLEntriesKeepToTheBook(t *testing.T) {
	s := newPostgresStore(t)
	ctx := t.Context()
	principal := "/dav/principals/7/"
	paths := []string{
		"/dav/addressbooks/1", "/dav/addressbooks/1/a", "/dav/addressbooks/1/b.vcf", "/dav/addressbooks/1/a/b",
		"/dav/addressbooks/10", "/dav/addressbooks/10/a", "/dav/addressbooks/1-a", "/dav/addressbooks/1.vcf", "/dav/addressbooks/2/a",
	}
	for _, resourcePath := range paths {
		if err := s.ACLEntries.SetACL(ctx, resourcePath, []ACLEntry{{PrincipalHref: principal, IsGrant: true, Privilege: "read"}}); err != nil {
			t.Fatalf("set the ACL of %s: %v", resourcePath, err)
		}
	}
	// Stored without its trailing slash, the principal is the same one.
	if err := s.ACLEntries.SetACL(ctx, "/dav/addressbooks/1/c", []ACLEntry{{PrincipalHref: "/dav/principals/7", IsGrant: false, Privilege: "read"}}); err != nil {
		t.Fatal(err)
	}
	if err := s.ACLEntries.SetACL(ctx, "/dav/addressbooks/1/d", []ACLEntry{{PrincipalHref: "/dav/principals/8/", IsGrant: true, Privilege: "read"}}); err != nil {
		t.Fatal(err)
	}

	var got []string
	err := s.ReadContactListing(ctx, func(listing *ContactListing) error {
		entries, err := listing.CollectionACLEntries(ctx, "/dav/addressbooks/1", []string{"DAV:all", principal})
		for _, entry := range entries {
			got = append(got, entry.ResourcePath)
		}
		return err
	})
	if err != nil {
		t.Fatal(err)
	}
	want := []string{"/dav/addressbooks/1", "/dav/addressbooks/1/a", "/dav/addressbooks/1/b.vcf", "/dav/addressbooks/1/a/b", "/dav/addressbooks/1/c"}
	slices.Sort(got)
	slices.Sort(want)
	if !slices.Equal(got, want) {
		t.Fatalf("entries on %v, want %v", got, want)
	}
}
