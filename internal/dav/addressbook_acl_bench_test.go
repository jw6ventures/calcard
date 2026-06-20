package dav

import (
	"context"
	"fmt"
	"testing"

	"github.com/jw6ventures/calcard/internal/store"
)

// BenchmarkFilterReadableAddressBookContacts exercises the shared-address-book
// read path: a non-owner reading a book whose access is granted once at the
// collection level. The prefetch makes this O(principals) in ACL repository
// lookups regardless of the contact count, instead of O(contacts).
func BenchmarkFilterReadableAddressBookContacts(b *testing.B) {
	book := &store.AddressBook{ID: 1, UserID: 1, Name: "Shared"}
	user := &store.User{ID: 2}
	entries := []store.ACLEntry{
		{ResourcePath: "/dav/addressbooks/1", PrincipalHref: "DAV:authenticated", Privilege: "read", IsGrant: true},
	}

	for _, n := range []int{16, 128, 1000, 10000} {
		contacts := make([]store.Contact, 0, n)
		for i := 0; i < n; i++ {
			name := fmt.Sprintf("contact-%d", i)
			contacts = append(contacts, store.Contact{AddressBookID: 1, UID: name, ResourceName: name})
		}
		h := &Handler{store: &store.Store{ACLEntries: &fakeACLRepo{entries: entries}}}
		b.Run(fmt.Sprintf("contacts=%d", n), func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				if _, err := h.filterReadableAddressBookContacts(context.Background(), user, book, contacts); err != nil {
					b.Fatalf("filter failed: %v", err)
				}
			}
		})
	}
}
