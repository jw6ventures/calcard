package dav

import (
	"context"
	"strings"

	"github.com/jw6ventures/calcard/internal/store"
)

func addressBookCollectionPath(cleanPath string) string {
	return collectionPathForPrefix(cleanPath, addressBookPrefix)
}

func addressBookContactPath(bookID int64, resourceName string) string {
	return objectResourcePath(addressBookPrefix, bookID, resourceName)
}

func addressBookCollectionResourcePath(bookID int64) string {
	return collectionResourcePath(addressBookPrefix, bookID)
}

func addressBookObjectACLPaths(bookID int64, resourceName string) []string {
	return objectACLPaths(addressBookPrefix, bookID, resourceName, ".vcf")
}

func (h *DavServer) getAddressBook(ctx context.Context, id int64) (*store.AddressBook, error) {
	if h == nil || h.store == nil || h.store.AddressBooks == nil {
		return nil, store.ErrNotFound
	}
	book, err := h.store.AddressBooks.GetByID(ctx, id)
	if err != nil {
		return nil, err
	}
	if book == nil {
		return nil, store.ErrNotFound
	}
	return book, nil
}

func (h *DavServer) loadAddressBook(ctx context.Context, user *store.User, id int64) (*store.AddressBook, error) {
	book, err := h.getAddressBook(ctx, id)
	if err != nil {
		return nil, err
	}
	if book.UserID != user.ID {
		return nil, store.ErrNotFound
	}
	return book, nil
}

func (h *DavServer) loadAddressBookByName(ctx context.Context, user *store.User, name string) (*store.AddressBook, error) {
	if h.store == nil || h.store.AddressBooks == nil {
		return nil, store.ErrNotFound
	}
	books, err := h.store.AddressBooks.ListByUser(ctx, user.ID)
	if err != nil {
		return nil, err
	}
	var match *store.AddressBook
	for _, book := range books {
		if book.Name != name {
			continue
		}
		if match != nil {
			return nil, errAmbiguousAddressBook
		}
		copy := book
		match = &copy
	}
	if match == nil {
		return nil, store.ErrNotFound
	}
	return match, nil
}

func (h *DavServer) loadAddressBookWithPrivilege(ctx context.Context, user *store.User, id int64, cleanPath, privilege string) (*store.AddressBook, error) {
	book, err := h.getAddressBook(ctx, id)
	if err != nil {
		return nil, err
	}
	if err := h.requireAddressBookPrivilege(ctx, user, book, cleanPath, privilege); err != nil {
		return nil, err
	}
	return book, nil
}

func (h *DavServer) addressBookPrivilegeDecision(ctx context.Context, user *store.User, book *store.AddressBook, cleanPath, privilege string) (bool, bool, error) {
	if book == nil {
		return false, false, nil
	}
	if canonicalPath, err := h.canonicalDAVPath(ctx, user, cleanPath); err == nil && canonicalPath != "" {
		cleanPath = canonicalPath
	} else if err != nil {
		return false, false, err
	}
	if user != nil && book.UserID == user.ID {
		return true, false, nil
	}
	if granted, decided, err := h.aclDecisionMatchingPrivilege(ctx, user, cleanPath, privilege); err != nil {
		return false, false, err
	} else if decided {
		return granted, !granted, nil
	}
	collectionPath := addressBookCollectionPath(cleanPath)
	if collectionPath != cleanPath {
		if granted, decided, err := h.aclDecisionMatchingPrivilege(ctx, user, collectionPath, privilege); err != nil {
			return false, false, err
		} else if decided {
			return granted, !granted, nil
		}
	}
	return false, false, nil
}

func (h *DavServer) requireAddressBookPrivilege(ctx context.Context, user *store.User, book *store.AddressBook, cleanPath, privilege string) error {
	return requirePrivilegeDecision(h.addressBookPrivilegeDecision(ctx, user, book, cleanPath, privilege))
}

// addressBookPrivilegeDecisionFromEntries evaluates contact visibility from a
// prefetched ACL map, preserving the object-deny-over-collection-fallback
// semantics of addressBookPrivilegeDecision. Unlike calendars, address books
// have no EffectivePrivileges fallback: a non-owner with no applicable ACL is
// not allowed.
func addressBookPrivilegeDecisionFromEntries(user *store.User, book *store.AddressBook, resourceName, privilege string, entriesByPath map[string][]store.ACLEntry) (bool, bool) {
	if book == nil || user == nil {
		return false, false
	}
	objectPaths := addressBookObjectACLPaths(book.ID, resourceName)
	collectionPaths := []string{addressBookCollectionResourcePath(book.ID)}
	granted, denied, _ := aclEntriesPrivilegeDecision(entriesByPath, user, book.UserID, objectPaths, collectionPaths, privilege)
	return granted, denied
}

// prefetchAddressBookACLEntries loads the ACL entries relevant to a book's
// collection and the supplied contact resource names in a single sweep over the
// user's principals, mirroring prefetchCalendarACLEntries. This replaces the
// per-contact ListByResource lookups that otherwise make a single REPORT/sync
// O(N) in ACL repository queries.
func (h *DavServer) prefetchAddressBookACLEntries(ctx context.Context, user *store.User, bookID int64, resourceNames []string) (map[string][]store.ACLEntry, error) {
	relevantPaths := map[string]struct{}{
		normalizeDAVHref(addressBookCollectionResourcePath(bookID)): {},
	}
	for _, resourceName := range resourceNames {
		for _, resourcePath := range addressBookObjectACLPaths(bookID, resourceName) {
			relevantPaths[normalizeDAVHref(resourcePath)] = struct{}{}
		}
	}
	return h.prefetchACLEntries(ctx, user, relevantPaths)
}

func canReadAddressBookContactWithEntries(user *store.User, book *store.AddressBook, resourceName string, entriesByPath map[string][]store.ACLEntry) bool {
	if strings.TrimSpace(resourceName) == "" {
		return false
	}
	allowed, _ := addressBookPrivilegeDecisionFromEntries(user, book, resourceName, "read", entriesByPath)
	return allowed
}

func (h *DavServer) filterReadableAddressBookContacts(ctx context.Context, user *store.User, book *store.AddressBook, contacts []store.Contact) ([]store.Contact, error) {
	resourceNames := make([]string, 0, len(contacts))
	for _, contact := range contacts {
		resourceNames = append(resourceNames, contactResourceName(contact))
	}
	entriesByPath, err := h.prefetchAddressBookACLEntries(ctx, user, book.ID, resourceNames)
	if err != nil {
		return nil, err
	}
	visible := make([]store.Contact, 0, len(contacts))
	for _, contact := range contacts {
		if canReadAddressBookContactWithEntries(user, book, contactResourceName(contact), entriesByPath) {
			visible = append(visible, contact)
		}
	}
	return visible, nil
}
