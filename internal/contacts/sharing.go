package contacts

import (
	"context"
	"fmt"
	"slices"
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

// sharePresetPrivileges are the grants a share of each role writes. Editor adds
// "write" (which subsumes the write-* / bind / unbind privileges via the shared
// ACL matcher); read-only shares grant only "read".
func sharePresetPrivileges(editor bool) []string {
	if editor {
		return []string{"read", "write"}
	}
	return []string{"read"}
}

// sharePrincipalPrivileges is every privilege a collection ACE can grant. A
// share is the whole of a principal's grants on the book, however they were
// written, so setting a role or removing a share replaces or revokes them all;
// a grant left behind, such as "all" or "write-acl", would keep access the
// share no longer shows.
var sharePrincipalPrivileges = []string{
	"all", "read", "read-free-busy", "read-acl", "read-current-user-privilege-set",
	"write", "write-content", "write-properties", "write-acl", "bind", "unbind", "unlock",
}

// viewerRevokedPrivileges are the grants a viewer does not hold.
var viewerRevokedPrivileges = []string{
	"all", "write", "write-content", "write-properties", "write-acl", "bind", "unbind", "unlock",
}

// revokeContactWriteGrants takes the principal's write-type grants off every
// contact in the book, each contact's ACL updated under its own lock. A grant
// of "all" becomes a grant of "read" in the same position, so read access the
// owner gave on a contact survives; other grants and every deny are kept.
func (s *Service) revokeContactWriteGrants(ctx context.Context, bookID int64, principalHref string) error {
	memberPrefix := addressBookACLCollectionPath(bookID) + "/"
	seen := map[string]struct{}{}
	for _, spelling := range []string{principalHref, strings.TrimSuffix(principalHref, "/")} {
		entries, err := s.store.ACLEntries.ListByPrincipal(ctx, spelling)
		if err != nil {
			return err
		}
		for _, entry := range entries {
			path := entry.ResourcePath
			if !entry.IsGrant || !strings.HasPrefix(path, memberPrefix) || !slices.Contains(viewerRevokedPrivileges, entry.Privilege) {
				continue
			}
			if _, done := seen[path]; done {
				continue
			}
			seen[path] = struct{}{}
			if err := s.store.ACLEntries.UpdateACL(ctx, path, func(current []store.ACLEntry) ([]store.ACLEntry, error) {
				return withoutWriteGrants(current, principalHref), nil
			}); err != nil {
				return err
			}
		}
	}
	return nil
}

func withoutWriteGrants(entries []store.ACLEntry, principalHref string) []store.ACLEntry {
	kept := make([]store.ACLEntry, 0, len(entries))
	readAt := map[int]bool{}
	for _, entry := range entries {
		if entry.IsGrant && acl.NormalizePrincipalHref(entry.PrincipalHref) == principalHref && entry.Privilege == "read" {
			readAt[entry.Position] = true
		}
	}
	for _, entry := range entries {
		if !entry.IsGrant || acl.NormalizePrincipalHref(entry.PrincipalHref) != principalHref || !slices.Contains(viewerRevokedPrivileges, entry.Privilege) {
			kept = append(kept, entry)
			continue
		}
		if entry.Privilege == "all" && !readAt[entry.Position] {
			entry.Privilege = "read"
			readAt[entry.Position] = true
			kept = append(kept, entry)
		}
	}
	return kept
}

func sharePrincipalPrivilege(privilege string) bool {
	return slices.Contains(sharePrincipalPrivileges, privilege)
}

func shareVisiblePrivilege(privilege string) bool {
	switch privilege {
	case "read", "write", "write-content", "write-properties", "bind", "unbind", "all":
		return true
	default:
		return false
	}
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
	acl.SortEntries(result)
	return result, nil
}

func (s *Service) aclDecision(ctx context.Context, user *store.User, bookID int64, resourcePaths []string, privilege string) (bool, bool, error) {
	entries, err := s.aclEntriesForPaths(ctx, resourcePaths)
	if err != nil {
		return false, false, err
	}
	principals, err := s.addressBookApplicablePrincipals(ctx, user, bookID, entries)
	if err != nil {
		return false, false, err
	}
	granted, applicable := acl.DecisionForPrivilege(entries, principals, privilege)
	return granted, applicable, nil
}

func (s *Service) aclHasApplicablePrincipal(ctx context.Context, user *store.User, bookID int64, resourcePaths []string) (bool, error) {
	entries, err := s.aclEntriesForPaths(ctx, resourcePaths)
	if err != nil {
		return false, err
	}
	principals, err := s.addressBookApplicablePrincipals(ctx, user, bookID, entries)
	if err != nil {
		return false, err
	}
	return acl.HasApplicablePrincipal(entries, principals), nil
}

// addressBookApplicablePrincipals names the principals an ACE on an address
// book or one of its contacts can apply to. An address book is never a
// principal resource, so DAV:self cannot resolve against it. The owner is only
// looked up when some entry names a form that needs it, so the ordinary ACL
// costs no extra query.
func (s *Service) addressBookApplicablePrincipals(ctx context.Context, user *store.User, bookID int64, entries []store.ACLEntry) (map[string]struct{}, error) {
	if !acl.NeedsResourcePrincipals(entries) {
		return acl.ApplicablePrincipals(user), nil
	}
	if s == nil || s.store == nil || s.store.AddressBooks == nil {
		return acl.ApplicablePrincipals(user), nil
	}
	book, err := s.store.AddressBooks.GetByID(ctx, bookID)
	if err != nil {
		return nil, err
	}
	if book == nil {
		return acl.ApplicablePrincipals(user), nil
	}
	return acl.ApplicablePrincipalsFor(user, acl.ResourcePrincipals{OwnerHref: acl.PrincipalHref(book.UserID)}), nil
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
		if granted, applicable, err := s.aclDecision(ctx, user, bookID, resourcePaths, privilege); err != nil {
			return false, false, err
		} else if applicable {
			return granted, true, nil
		}
	}

	collectionPaths := []string{addressBookACLCollectionPath(bookID)}
	if granted, applicable, err := s.aclDecision(ctx, user, bookID, collectionPaths, privilege); err != nil {
		return false, false, err
	} else if applicable {
		return granted, true, nil
	}

	applicable, err := s.aclHasApplicablePrincipal(ctx, user, bookID, append(append([]string{}, resourcePaths...), collectionPaths...))
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
	for resourcePath := range result {
		acl.SortEntries(result[resourcePath])
	}
	return result, nil
}

func canReadContactFromEntries(user *store.User, bookID, ownerID int64, resourceName string, entriesByPath map[string][]store.ACLEntry) bool {
	applicable := acl.ApplicablePrincipalsFor(user, acl.ResourcePrincipals{OwnerHref: acl.PrincipalHref(ownerID)})
	var resourceEntries []store.ACLEntry
	for _, p := range addressBookACLResourcePaths(bookID, resourceName) {
		resourceEntries = append(resourceEntries, entriesByPath[p]...)
	}
	acl.SortEntries(resourceEntries)
	if granted, decided := acl.DecisionForPrivilege(resourceEntries, applicable, "read"); decided {
		return granted
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
	principalHref := sharePrincipalHref(targetUserID)
	if !editor {
		// A contact's own grant is evaluated ahead of the book's, so a viewer
		// keeps no write grant on any contact. This runs before the role is
		// written so that a failure part way leaves less access, never more.
		if err := s.revokeContactWriteGrants(ctx, bookID, principalHref); err != nil {
			return err
		}
	}
	err = s.store.ACLEntries.UpdateACL(ctx, resourcePath, func(entries []store.ACLEntry) ([]store.ACLEntry, error) {
		filtered := make([]store.ACLEntry, 0, len(entries))
		sharePosition := -1
		maxPosition := -1
		for _, entry := range entries {
			if entry.Position > maxPosition {
				maxPosition = entry.Position
			}
			if acl.NormalizePrincipalHref(entry.PrincipalHref) == principalHref && entry.IsGrant && sharePrincipalPrivilege(entry.Privilege) {
				if sharePosition == -1 || entry.Position < sharePosition {
					sharePosition = entry.Position
				}
				continue
			}
			filtered = append(filtered, entry)
		}
		if sharePosition == -1 {
			sharePosition = maxPosition + 1
		}
		for _, privilege := range sharePresetPrivileges(editor) {
			filtered = append(filtered, store.ACLEntry{
				ResourcePath:  resourcePath,
				PrincipalHref: principalHref,
				IsGrant:       true,
				Privilege:     privilege,
				Position:      sharePosition,
			})
		}
		return filtered, nil
	})
	if err != nil || editor {
		return err
	}
	// A contact-level write grant committed while the role was being written
	// would outlive the downgrade, so the contacts are swept once more.
	return s.revokeContactWriteGrants(ctx, bookID, principalHref)
}

// UnshareAddressBook removes a share. The owner may remove any principal; a
// sharee may remove only their own (leave the book). It revokes every grant the
// principal holds on the book and its contacts, since a contact's own grant is
// evaluated ahead of the book's; deny entries stay.
func (s *Service) UnshareAddressBook(ctx context.Context, user *store.User, bookID, targetUserID int64) error {
	book, err := s.store.AddressBooks.GetByID(ctx, bookID)
	if err != nil {
		return err
	}
	if book == nil {
		return ErrNotFound
	}
	resourcePath := addressBookACLCollectionPath(bookID)
	if user == nil || book.UserID != user.ID {
		// A non-owner may only remove their own share, and only if they actually
		// have one (otherwise the book stays hidden).
		if user == nil || targetUserID != user.ID {
			return ErrNotFound
		}
		entries, err := s.store.ACLEntries.ListByResource(ctx, resourcePath)
		if err != nil {
			return err
		}
		acl.SortEntries(entries)
		if !hasEffectiveShare(entries, user.ID) {
			return ErrNotFound
		}
	}
	return s.store.ACLEntries.RevokePrincipalGrants(ctx, resourcePath, sharePrincipalHref(targetUserID), sharePrincipalPrivileges)
}

// hasEffectiveShare reports whether the user reads the book through a grant
// naming them, which is what ListAddressBookShares lists as a share. A grant to
// a broader principal alone is not theirs to remove.
func hasEffectiveShare(entries []store.ACLEntry, userID int64) bool {
	principalHref := sharePrincipalHref(userID)
	applicable := acl.ApplicablePrincipals(&store.User{ID: userID})
	if read, _ := acl.DecisionForPrivilege(entries, applicable, "read"); !read {
		return false
	}
	for _, entry := range entries {
		if entry.IsGrant && acl.NormalizePrincipalHref(entry.PrincipalHref) == principalHref && acl.PrivilegeMatches(entry.Privilege, "read") {
			return true
		}
	}
	return false
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

	acl.SortEntries(entries)
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
		if createdAt[userID].IsZero() || entry.CreatedAt.Before(createdAt[userID]) {
			createdAt[userID] = entry.CreatedAt
		}
	}

	shares := make([]AddressBookShare, 0, len(createdAt))
	for userID := range createdAt {
		applicable := acl.ApplicablePrincipals(&store.User{ID: userID})
		read, decided := acl.DecisionForPrivilege(entries, applicable, "read")
		if !decided || !read {
			continue
		}
		editor, _ := acl.DecisionForPrivilege(entries, applicable, "write")
		bind, _ := acl.DecisionForPrivilege(entries, applicable, "bind")
		unbind, _ := acl.DecisionForPrivilege(entries, applicable, "unbind")
		shares = append(shares, AddressBookShare{
			UserID:    userID,
			Editor:    editor && bind && unbind,
			CreatedAt: createdAt[userID],
		})
	}
	sort.Slice(shares, func(i, j int) bool { return shares[i].UserID < shares[j].UserID })
	return shares, nil
}
