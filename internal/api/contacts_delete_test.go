package api

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/jw6ventures/calcard/internal/acl"
	"github.com/jw6ventures/calcard/internal/config"
	"github.com/jw6ventures/calcard/internal/dav"
	"github.com/jw6ventures/calcard/internal/store"
)

// churningACLRepo commits a grant to an unrelated principal right after each
// read of entries by path, for the first changes reads, so the entries a
// delete pinned have changed by the time the store re-checks them.
type churningACLRepo struct {
	*fakeACLRepo
	changes int
}

func (c *churningACLRepo) ListByResources(ctx context.Context, paths []string) ([]store.ACLEntry, error) {
	entries, err := c.fakeACLRepo.ListByResources(ctx, paths)
	if c.changes > 0 {
		c.changes--
		c.entries = append(c.entries, store.ACLEntry{ResourcePath: "/dav/addressbooks/2", PrincipalHref: acl.PrincipalHref(int64(1000 + len(c.entries))), IsGrant: true, Privilege: "read"})
	}
	return entries, err
}

// DELETE of a contact answers as a CardDAV DELETE of it does: 403 for a user
// the book refuses DAV:unbind, 404 for a contact the user cannot see or is
// denied everything on, and 409 when the contact's access kept changing.
func TestDeleteContactAnswersAsCardDAVDoes(t *testing.T) {
	user := acl.PrincipalHref(1)
	grant := func(resourcePath string, privileges ...string) []store.ACLEntry {
		var entries []store.ACLEntry
		for _, privilege := range privileges {
			entries = append(entries, store.ACLEntry{ResourcePath: resourcePath, PrincipalHref: user, IsGrant: true, Privilege: privilege})
		}
		return entries
	}
	denyAll := store.ACLEntry{ResourcePath: "/dav/addressbooks/2/u2", PrincipalHref: user, IsGrant: false, Privilege: "all"}
	cases := []struct {
		name    string
		bookID  string
		entries []store.ACLEntry
		churn   bool
		want    int
	}{
		{name: "read on the book", bookID: "2", entries: grant("/dav/addressbooks/2", "read"), want: http.StatusForbidden},
		{name: "no entry on the book", bookID: "2", want: http.StatusNotFound},
		{name: "everything denied on the contact", bookID: "2", entries: append(grant("/dav/addressbooks/2", "read", "write"), denyAll), want: http.StatusNotFound},
		{name: "access kept changing", bookID: "2", entries: grant("/dav/addressbooks/2", "read", "write"), churn: true, want: http.StatusConflict},
		{name: "write on the book", bookID: "2", entries: grant("/dav/addressbooks/2", "read", "write"), want: http.StatusNoContent},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			contacts := &fakeContactRepo{contacts: map[string]store.Contact{
				contactKey(2, "u2"): {ID: 2, AddressBookID: 2, UID: "u2", ResourceName: "u2", ETag: "e2", RawVCard: "BEGIN:VCARD\r\nVERSION:3.0\r\nUID:u2\r\nFN:U2\r\nEND:VCARD\r\n"},
			}}
			var aclRepo store.ACLRepository = &fakeACLRepo{entries: tc.entries}
			if tc.churn {
				aclRepo = &churningACLRepo{fakeACLRepo: aclRepo.(*fakeACLRepo), changes: 10}
			}
			h := NewHandler(&config.Config{}, &store.Store{
				AddressBooks: &fakeAddressBookRepo{books: map[int64]*store.AddressBook{2: {ID: 2, UserID: 99, Name: "Shared"}}},
				Contacts:     contacts,
				ACLEntries:   aclRepo,
			}, dav.ValidateCalendarObject)

			rec := httptest.NewRecorder()
			h.DeleteContact(rec, withUserAndRoute(httptest.NewRequest(http.MethodDelete, "/api/addressbooks/"+tc.bookID+"/contacts/u2", nil), tc.bookID, "u2"))
			if rec.Code != tc.want {
				t.Fatalf("DELETE status = %d (%s), want %d", rec.Code, rec.Body.String(), tc.want)
			}
			if _, kept := contacts.contacts[contactKey(2, "u2")]; kept == (tc.want == http.StatusNoContent) {
				t.Fatalf("contact kept = %v after status %d", kept, rec.Code)
			}
		})
	}
}
