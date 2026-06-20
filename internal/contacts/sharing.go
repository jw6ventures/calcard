package contacts

import (
	"context"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/jw6ventures/calcard/internal/acl"
	"github.com/jw6ventures/calcard/internal/store"
)

// AddressBookShare describes a single principal an address book is shared with.
type AddressBookShare struct {
	UserID    int64
	Editor    bool
	CreatedAt time.Time
}

// AddressBookAccess augments an address book with how the current user reaches it.
type AddressBookAccess struct {
	store.AddressBook
	Shared bool // true when the current user is not the owner
	Editor bool // true when the current user may modify contacts
}

// shareManagedPrivileges are the grants the sharing UI/API own and replace when a
// share is updated. Editor adds "write" (which subsumes the write-* / bind / unbind
// privileges via the shared ACL matcher); read-only shares grant only "read".
func sharePresetPrivileges(editor bool) []string {
	if editor {
		return []string{"read", "write"}
	}
	return []string{"read"}
}

func shareManagedPrivilege(privilege string) bool {
	switch privilege {
	case "read", "write":
		return true
	default:
		return false
	}
}

func shareVisiblePrivilege(privilege string) bool {
	switch privilege {
	case "read", "write", "write-content", "write-properties", "bind", "unbind", "all":
		return true
	default:
		return false
	}
}

func shareEditorFromEntries(entries []store.ACLEntry) bool {
	for _, entry := range entries {
		if !entry.IsGrant {
			continue
		}
		switch entry.Privilege {
		case "write", "write-content", "write-properties", "bind", "unbind", "all":
			return true
		}
	}
	return false
}

func addressBookACLCollectionPath(bookID int64) string {
	return fmt.Sprintf("/dav/addressbooks/%d", bookID)
}

func sharePrincipalHref(userID int64) string {
	return acl.PrincipalHref(userID)
}

func addressBookACLResourcePaths(bookID int64, resourceName string) []string {
	resourceName = strings.TrimSpace(resourceName)
	if resourceName == "" {
		return nil
	}
	base := addressBookACLCollectionPath(bookID) + "/" + resourceName
	paths := []string{base}
	if strings.EqualFold(pathExt(resourceName), ".vcf") {
		paths = append(paths, strings.TrimSuffix(base, pathExt(resourceName)))
	} else {
		paths = append(paths, base+".vcf")
	}
	return paths
}

func pathExt(resourceName string) string {
	idx := strings.LastIndex(resourceName, ".")
	if idx < 0 {
		return ""
	}
	return resourceName[idx:]
}

func contactResourceName(c store.Contact) string {
	if c.ResourceName != "" {
		return c.ResourceName
	}
	return c.UID
}

func (s *Service) aclEntriesForPaths(ctx context.Context, resourcePaths []string) ([]store.ACLEntry, error) {
	if s == nil || s.store == nil || s.store.ACLEntries == nil {
		return nil, nil
	}
	seen := make(map[string]struct{}, len(resourcePaths))
	var result []store.ACLEntry
	for _, resourcePath := range resourcePaths {
		if resourcePath == "" {
			continue
		}
		if _, ok := seen[resourcePath]; ok {
			continue
		}
		seen[resourcePath] = struct{}{}
		entries, err := s.store.ACLEntries.ListByResource(ctx, resourcePath)
		if err != nil {
			return nil, err
		}
		result = append(result, entries...)
	}
	return result, nil
}

func (s *Service) aclDecision(ctx context.Context, user *store.User, resourcePaths []string, privilege string) (bool, bool, error) {
	entries, err := s.aclEntriesForPaths(ctx, resourcePaths)
	if err != nil {
		return false, false, err
	}
	granted, applicable := acl.DecisionForPrivilege(entries, acl.ApplicablePrincipals(user), privilege)
	return granted, applicable, nil
}

func (s *Service) aclHasApplicablePrincipal(ctx context.Context, user *store.User, resourcePaths []string) (bool, error) {
	entries, err := s.aclEntriesForPaths(ctx, resourcePaths)
	if err != nil {
		return false, err
	}
	return acl.HasApplicablePrincipal(entries, acl.ApplicablePrincipals(user)), nil
}

// privilegeDecision evaluates whether user holds privilege on a contact (when
// resourceName is set) or on the address book collection. It returns
// (granted, applicable): applicable is true when an ACL entry exists for a
// principal that applies to the user, which the caller uses to distinguish
// "forbidden" (applicable) from "not found" (not applicable).
func (s *Service) privilegeDecision(ctx context.Context, user *store.User, bookID int64, resourceName, privilege string) (bool, bool, error) {
	if s == nil || s.store == nil || s.store.ACLEntries == nil || user == nil {
		return false, false, nil
	}

	resourcePaths := addressBookACLResourcePaths(bookID, resourceName)
	if len(resourcePaths) > 0 {
		if granted, applicable, err := s.aclDecision(ctx, user, resourcePaths, privilege); err != nil {
			return false, false, err
		} else if applicable {
			return granted, true, nil
		}
	}

	collectionPaths := []string{addressBookACLCollectionPath(bookID)}
	if granted, applicable, err := s.aclDecision(ctx, user, collectionPaths, privilege); err != nil {
		return false, false, err
	} else if applicable {
		return granted, true, nil
	}

	applicable, err := s.aclHasApplicablePrincipal(ctx, user, append(append([]string{}, resourcePaths...), collectionPaths...))
	if err != nil {
		return false, false, err
	}
	return false, applicable, nil
}

// prefetchACLEntries loads, in a small fixed number of queries, every ACL entry
// for the collection and the given contacts that applies to the user, keyed by
// resource path. This avoids a per-contact query when filtering a list.
func (s *Service) prefetchACLEntries(ctx context.Context, user *store.User, bookID int64, contacts []store.Contact) (map[string][]store.ACLEntry, error) {
	if s == nil || s.store == nil || s.store.ACLEntries == nil || user == nil {
		return nil, nil
	}

	relevant := map[string]struct{}{addressBookACLCollectionPath(bookID): {}}
	for _, c := range contacts {
		for _, p := range addressBookACLResourcePaths(bookID, contactResourceName(c)) {
			relevant[p] = struct{}{}
		}
	}

	result := make(map[string][]store.ACLEntry, len(relevant))
	for _, principalHref := range acl.PrincipalHrefs(user) {
		entries, err := s.store.ACLEntries.ListByPrincipal(ctx, principalHref)
		if err != nil {
			return nil, err
		}
		for _, entry := range entries {
			path := strings.TrimSpace(entry.ResourcePath)
			if _, ok := relevant[path]; !ok {
				continue
			}
			result[path] = append(result[path], entry)
		}
	}
	return result, nil
}

func canReadContactFromEntries(user *store.User, bookID int64, resourceName string, entriesByPath map[string][]store.ACLEntry) bool {
	applicable := acl.ApplicablePrincipals(user)
	for _, p := range addressBookACLResourcePaths(bookID, resourceName) {
		if granted, decided := acl.DecisionForPrivilege(entriesByPath[p], applicable, "read"); decided {
			return granted
		}
	}
	if granted, decided := acl.DecisionForPrivilege(entriesByPath[addressBookACLCollectionPath(bookID)], applicable, "read"); decided {
		return granted
	}
	return false
}

// ShareAddressBook grants another user read-only or editor access to an owned
// address book. It is owner-only.
func (s *Service) ShareAddressBook(ctx context.Context, owner *store.User, bookID, targetUserID int64, editor bool) error {
	if _, err := s.requireOwnedBook(ctx, owner, bookID); err != nil {
		return err
	}
	if targetUserID == 0 || targetUserID == owner.ID {
		return fmt.Errorf("%w: invalid target user", ErrBadRequest)
	}
	target, err := s.store.Users.GetByID(ctx, targetUserID)
	if err != nil {
		return err
	}
	if target == nil {
		return fmt.Errorf("%w: target user not found", ErrBadRequest)
	}

	resourcePath := addressBookACLCollectionPath(bookID)
	entries, err := s.store.ACLEntries.ListByResource(ctx, resourcePath)
	if err != nil {
		return err
	}
	principalHref := sharePrincipalHref(targetUserID)
	filtered := make([]store.ACLEntry, 0, len(entries))
	for _, entry := range entries {
		if entry.PrincipalHref == principalHref && entry.IsGrant && shareManagedPrivilege(entry.Privilege) {
			continue
		}
		filtered = append(filtered, entry)
	}
	for _, privilege := range sharePresetPrivileges(editor) {
		filtered = append(filtered, store.ACLEntry{
			ResourcePath:  resourcePath,
			PrincipalHref: principalHref,
			IsGrant:       true,
			Privilege:     privilege,
		})
	}
	return s.store.ACLEntries.SetACL(ctx, resourcePath, filtered)
}

// UnshareAddressBook removes a share. The owner may remove any principal; a
// sharee may remove only their own grant (i.e. leave the shared book).
func (s *Service) UnshareAddressBook(ctx context.Context, user *store.User, bookID, targetUserID int64) error {
	book, err := s.store.AddressBooks.GetByID(ctx, bookID)
	if err != nil {
		return err
	}
	if book == nil {
		return ErrNotFound
	}
	isOwner := user != nil && book.UserID == user.ID
	if !isOwner {
		// A non-owner may only remove their own share, and only if they actually
		// have one (otherwise the book stays hidden).
		if user == nil || targetUserID != user.ID {
			return ErrNotFound
		}
		applicable, err := s.aclHasApplicablePrincipal(ctx, user, []string{addressBookACLCollectionPath(bookID)})
		if err != nil {
			return err
		}
		if !applicable {
			return ErrNotFound
		}
	}
	return s.store.ACLEntries.DeletePrincipalEntriesByResourcePrefix(ctx, sharePrincipalHref(targetUserID), addressBookACLCollectionPath(bookID))
}

// ListAddressBookShares returns the principals an owned address book is shared
// with. It is owner-only.
func (s *Service) ListAddressBookShares(ctx context.Context, owner *store.User, bookID int64) ([]AddressBookShare, error) {
	if _, err := s.requireOwnedBook(ctx, owner, bookID); err != nil {
		return nil, err
	}
	entries, err := s.store.ACLEntries.ListByResource(ctx, addressBookACLCollectionPath(bookID))
	if err != nil {
		return nil, err
	}

	grouped := map[int64][]store.ACLEntry{}
	createdAt := map[int64]time.Time{}
	for _, entry := range entries {
		if !entry.IsGrant || !shareVisiblePrivilege(entry.Privilege) {
			continue
		}
		if !strings.HasPrefix(entry.PrincipalHref, "/dav/principals/") || !strings.HasSuffix(entry.PrincipalHref, "/") {
			continue
		}
		rawID := strings.TrimSuffix(strings.TrimPrefix(entry.PrincipalHref, "/dav/principals/"), "/")
		userID, err := strconv.ParseInt(rawID, 10, 64)
		if err != nil {
			continue
		}
		grouped[userID] = append(grouped[userID], entry)
		if createdAt[userID].IsZero() || entry.CreatedAt.Before(createdAt[userID]) {
			createdAt[userID] = entry.CreatedAt
		}
	}

	shares := make([]AddressBookShare, 0, len(grouped))
	for userID, group := range grouped {
		shares = append(shares, AddressBookShare{
			UserID:    userID,
			Editor:    shareEditorFromEntries(group),
			CreatedAt: createdAt[userID],
		})
	}
	sort.Slice(shares, func(i, j int) bool { return shares[i].UserID < shares[j].UserID })
	return shares, nil
}
