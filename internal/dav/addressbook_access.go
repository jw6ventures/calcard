package dav

import (
	"context"
	"strings"

	"github.com/jw6ventures/calcard/internal/acl"
	"github.com/jw6ventures/calcard/internal/store"
)

func addressBookCollectionPath(cleanPath string) string {
	return collectionPathForPrefix(cleanPath, addressBookPrefix)
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
	if state := davRequestStateFromContext(ctx); state != nil {
		if book, ok := state.addressBook(id); ok {
			return book, nil
		}
	}
	book, err := h.store.AddressBooks.GetByID(ctx, id)
	if err != nil {
		return nil, err
	}
	if book == nil {
		return nil, store.ErrNotFound
	}
	if state := davRequestStateFromContext(ctx); state != nil {
		state.putAddressBook(book)
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

// addressBookPrivilegeNames is every privilege an address-book access decision
// can resolve, in DAV:acl document order.
var addressBookPrivilegeNames = []string{"read", "write", "write-content", "write-properties", "bind", "unbind"}

// addressBookPrivilegeContext mirrors calendarPrivilegeContext: one resolution
// of canonical path, ownership, and ACL entries that several privilege
// decisions can share. Unlike calendars there is no applicable-principal
// fallback — a non-owner with no applicable decision is simply not allowed.
type addressBookPrivilegeContext struct {
	owner             bool
	principals        map[string]struct{}
	resourceEntries   []store.ACLEntry
	collectionEntries []store.ACLEntry
	sameCollection    bool
}

func (h *DavServer) addressBookPrivilegeContextFor(ctx context.Context, user *store.User, book *store.AddressBook, cleanPath string) (*addressBookPrivilegeContext, error) {
	if book == nil {
		return nil, nil
	}
	if canonicalPath, err := h.canonicalDAVPath(ctx, user, cleanPath); err == nil && canonicalPath != "" {
		cleanPath = canonicalPath
	} else if err != nil {
		return nil, err
	}
	pc := &addressBookPrivilegeContext{principals: acl.ApplicablePrincipals(user)}
	if user != nil && book.UserID == user.ID {
		pc.owner = true
		return pc, nil
	}

	hasACLStore := h != nil && h.store != nil && h.store.ACLEntries != nil
	if hasACLStore {
		entries, err := h.aclEntriesForResource(ctx, cleanPath)
		if err != nil {
			return nil, err
		}
		pc.resourceEntries = entries
	}

	collectionPath := addressBookCollectionPath(cleanPath)
	pc.sameCollection = collectionPath == cleanPath
	if !pc.sameCollection && hasACLStore {
		entries, err := h.aclEntriesForResource(ctx, collectionPath)
		if err != nil {
			return nil, err
		}
		pc.collectionEntries = entries
	}
	return pc, nil
}

func (pc *addressBookPrivilegeContext) decide(privilege string) (allowed, denied bool) {
	if pc.owner {
		return true, false
	}
	if granted, applicable := acl.DecisionForPrivilege(pc.resourceEntries, pc.principals, privilege); applicable {
		return granted, !granted
	}
	if !pc.sameCollection {
		if granted, applicable := acl.DecisionForPrivilege(pc.collectionEntries, pc.principals, privilege); applicable {
			return granted, !granted
		}
	}
	return false, false
}

func (h *DavServer) addressBookPrivilegeDecision(ctx context.Context, user *store.User, book *store.AddressBook, cleanPath, privilege string) (bool, bool, error) {
	pc, err := h.addressBookPrivilegeContextFor(ctx, user, book, cleanPath)
	if err != nil || pc == nil {
		return false, false, err
	}
	allowed, denied := pc.decide(privilege)
	return allowed, denied, nil
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
	decider := newBatchedObjectACLDecider(user, book.UserID, addressBookCollectionResourcePath(book.ID), entriesByPath)
	granted, denied, _ := decider.decide(resourceName, ".vcf", privilege)
	return granted, denied
}

// prefetchAddressBookACLEntries loads the ACL entries relevant to a book's
// collection and the supplied contact resource names in a single sweep over the
// user's principals, mirroring prefetchCalendarACLEntries. This replaces the
// per-contact ListByResource lookups that otherwise make a single REPORT/sync
// O(N) in ACL repository queries.
func (h *DavServer) prefetchAddressBookACLEntries(ctx context.Context, user *store.User, bookID int64, resourceNames []string) (map[string][]store.ACLEntry, error) {
	collectionPath := addressBookCollectionResourcePath(bookID)
	relevantPaths := make([]string, 0, 1+2*len(resourceNames))
	relevantPaths = append(relevantPaths, collectionPath)
	for _, resourceName := range resourceNames {
		relevantPaths = appendObjectACLPaths(relevantPaths, collectionPath, resourceName, ".vcf")
	}
	return h.prefetchACLEntries(ctx, user, relevantPaths)
}

func canReadAddressBookContactWithEntries(user *store.User, book *store.AddressBook, resourceName string, entriesByPath map[string][]store.ACLEntry) bool {
	if strings.TrimSpace(resourceName) == "" {
		return false
	}
	if book == nil {
		return false
	}
	decider := newBatchedObjectACLDecider(user, book.UserID, addressBookCollectionResourcePath(book.ID), entriesByPath)
	return canReadAddressBookContactWithDecider(resourceName, decider)
}

func canReadAddressBookContactWithDecider(resourceName string, decider *batchedObjectACLDecider) bool {
	allowed, _, _ := decider.decide(resourceName, ".vcf", "read")
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
	decider := newBatchedObjectACLDecider(user, book.UserID, addressBookCollectionResourcePath(book.ID), entriesByPath)
	visible := make([]store.Contact, 0, len(contacts))
	for _, contact := range contacts {
		if canReadAddressBookContactWithDecider(contactResourceName(contact), decider) {
			visible = append(visible, contact)
		}
	}
	return visible, nil
}
