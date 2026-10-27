package dav

import (
	"context"
	"encoding/xml"
	"errors"
	"fmt"
	"net/http"
	"path"
	"strconv"
	"strings"
	"time"

	aclutil "github.com/jw6ventures/calcard/internal/acl"
	"github.com/jw6ventures/calcard/internal/auth"
	"github.com/jw6ventures/calcard/internal/store"
)

// propDecorationMask controls which expensive DAV properties decorateDAVProp
// computes. Each field gates a property that requires a DB query or repeated
// privilege checks, so unrequested properties can be skipped entirely.
type propDecorationMask struct {
	lockDiscovery           bool
	acl                     bool
	currentUserPrivilegeSet bool
	owner                   bool
}

// decorationMaskFor derives the set of expensive properties worth computing for
// a PROPFIND request. A specific <prop> request only needs the properties it
// names; allprop and compatibility (nil) requests compute every value.
func decorationMaskFor(req *propfindRequest) propDecorationMask {
	if req != nil && req.PropName != nil {
		return propDecorationMask{}
	}
	if req == nil || req.Prop == nil {
		return propDecorationMask{lockDiscovery: true, acl: true, currentUserPrivilegeSet: true, owner: true}
	}
	return propDecorationMask{
		lockDiscovery:           req.Prop.LockDiscovery != nil,
		acl:                     req.Prop.ACLProp != nil,
		currentUserPrivilegeSet: req.Prop.CurrentUserPrivilegeSet != nil,
		owner:                   req.Prop.Owner != nil || req.Prop.ACLProp != nil,
	}
}

func (h *DavServer) decoratePropfindResponses(ctx context.Context, r *http.Request, user *store.User, responses []response, mask propDecorationMask) error {
	if _, exceeded := h.capMultistatusResponses(responses); exceeded {
		return nil
	}
	deadByPath, err := h.deadPropertiesForResponses(ctx, user, responses)
	if err != nil {
		return err
	}
	if (mask.acl || mask.currentUserPrivilegeSet) && len(responses) > 1 {
		if err := h.prefetchPropfindACLEntries(ctx, user, responses); err != nil {
			return err
		}
	}
	// When lock discovery is requested across multiple responses, prefetch every
	// relevant lock in one query so each response's lockDiscoveryForPath reads
	// from the batch index rather than issuing its own query.
	if mask.lockDiscovery && len(responses) > 1 {
		batchCtx, err := h.prefetchLockBatchIndex(ctx, responses)
		if err != nil {
			return err
		}
		ctx = batchCtx
	}
	for i := range responses {
		if len(responses[i].Propstat) == 0 {
			continue
		}
		resourcePath := normalizeDAVHref(responses[i].Href)
		requestPath := resourcePath
		canonicalPath, canonicalErr := h.canonicalDAVPath(ctx, user, resourcePath)
		if canonicalErr == nil && canonicalPath != "" {
			resourcePath = canonicalPath
			requestPath = requestPathForCanonical(canonicalPath)
		}
		for j := range responses[i].Propstat {
			if responses[i].Propstat[j].Status != httpStatusOK {
				continue
			}
			if err := h.decorateDAVProp(ctx, user, resourcePath, requestPath, &responses[i].Propstat[j].Prop, mask); err != nil {
				return err
			}
			for _, property := range deadByPath[resourcePath] {
				responses[i].Propstat[j].Prop.setCustomXMLProperty(XMLProperty{
					Name:  xml.Name{Space: property.NamespaceURI, Local: property.LocalName},
					Value: rawXMLValue(property.InnerXML),
				})
			}
			if r != nil {
				if err := h.davRegistry().decoratePropfind(RequestContext{
					Context: ctx,
					User:    user,
					Request: r,
					Path:    resourcePath,
				}, &PropfindProperties{
					href: resourcePath,
					prop: &responses[i].Propstat[j].Prop,
				}); err != nil {
					return err
				}
			}
		}
	}
	return nil
}

func (h *DavServer) prefetchPropfindACLEntries(ctx context.Context, user *store.User, responses []response) error {
	if h == nil || h.store == nil || h.store.ACLEntries == nil {
		return nil
	}
	canonicalSeen := make(map[string]struct{})
	var canonicalPaths []string
	addCanonical := func(resourcePath string) {
		resourcePath = cleanDAVPath(resourcePath)
		if resourcePath == "" {
			return
		}
		if _, ok := canonicalSeen[resourcePath]; ok {
			return
		}
		canonicalSeen[resourcePath] = struct{}{}
		canonicalPaths = append(canonicalPaths, resourcePath)
	}
	for _, response := range responses {
		resourcePath, err := h.canonicalDAVHref(ctx, user, response.Href)
		if err != nil {
			return err
		}
		addCanonical(resourcePath)
		if target := parseDAVTarget(resourcePath); target.Valid && target.Resource {
			addCanonical(path.Dir(resourcePath))
		}
	}

	// Cache each resource under the key aclEntriesForResource looks it up by.
	queryPaths := make([]string, 0, len(canonicalPaths))
	for _, resourcePath := range canonicalPaths {
		if !isProtectedVirtualDAVRoot(resourcePath) {
			queryPaths = append(queryPaths, resourcePath)
		}
	}
	entries, err := h.store.ACLEntries.ListByResources(ctx, queryPaths)
	if err != nil {
		return err
	}
	byPath := make(map[string][]store.ACLEntry, len(queryPaths))
	for _, entry := range entries {
		byPath[entry.ResourcePath] = append(byPath[entry.ResourcePath], entry)
	}
	cache := aclEntryCacheFromContext(ctx)
	if cache == nil {
		return nil
	}
	for _, resourcePath := range canonicalPaths {
		resourceEntries := byPath[resourcePath]
		aclutil.SortEntries(resourceEntries)
		cache.put(resourcePath, resourceEntries)
	}
	return nil
}

func (h *DavServer) deadPropertiesForResponses(ctx context.Context, user *store.User, responses []response) (map[string][]store.DeadProperty, error) {
	result := make(map[string][]store.DeadProperty)
	if h == nil || h.store == nil || h.store.DeadProperties == nil {
		return result, nil
	}
	seen := make(map[string]struct{})
	paths := make([]string, 0, len(responses))
	for _, response := range responses {
		resourcePath := normalizeDAVHref(response.Href)
		if !strings.HasPrefix(resourcePath, calendarPrefix+"/") && !strings.HasPrefix(resourcePath, addressBookPrefix+"/") {
			continue
		}
		canonicalPath, err := h.canonicalDAVPath(ctx, user, resourcePath)
		if err != nil {
			return nil, err
		}
		if _, ok := seen[canonicalPath]; ok {
			continue
		}
		seen[canonicalPath] = struct{}{}
		paths = append(paths, canonicalPath)
	}
	properties, err := h.store.DeadProperties.ListByResources(ctx, paths)
	if err != nil {
		return nil, err
	}
	for _, property := range properties {
		key := cleanDAVPath(property.ResourcePath)
		result[key] = append(result[key], property)
	}
	return result, nil
}

// decorateDAVProp fills the computed properties of the resource at the
// canonical resourcePath, which a request names by requestPath. Checks that
// canonicalize the path they are given take requestPath: a canonical object
// path would lose its name's own extension to them.
func (h *DavServer) decorateDAVProp(ctx context.Context, user *store.User, resourcePath, requestPath string, p *prop, mask propDecorationMask) error {
	if p == nil || resourcePath == "" || !strings.HasPrefix(resourcePath, "/dav") {
		return nil
	}

	// These are static or cheap to build, so always populate them.
	p.SupportedLock = defaultSupportedLock()
	p.SupportedPrivilegeSet = defaultSupportedPrivilegeSet()
	p.PrincipalCollectionSet = &hrefListProp{Href: []string{"/dav/principals/"}}
	p.Group = &hrefProp{}
	p.ACLRestrictions = &aclRestrictionsProp{NoInvert: &struct{}{}}
	p.InheritedACLSet = &hrefListProp{}
	inheritedACLPath := inheritedACLSourcePath(resourcePath)
	var inheritedEntries []store.ACLEntry
	if inheritedACLPath != "" && h != nil && h.store != nil && h.store.ACLEntries != nil {
		var err error
		inheritedEntries, err = h.aclEntriesForResource(ctx, inheritedACLPath)
		if err != nil {
			return err
		}
		if len(inheritedEntries) != 0 {
			p.InheritedACLSet.Href = []string{ensureCollectionHref(inheritedACLPath)}
		}
	}
	if strings.HasPrefix(cleanDAVPath(resourcePath), "/dav/principals/") {
		p.AlternateURISet = &hrefListProp{}
		p.GroupMembership = &hrefListProp{}
	}
	ownerHref := ""
	if mask.owner && user != nil {
		var err error
		ownerHref, err = h.ownerPrincipalForPath(ctx, user, resourcePath)
		if err != nil {
			return err
		}
		if ownerHref != "" {
			p.Owner = &hrefProp{Href: ownerHref}
		}
	}

	if mask.lockDiscovery {
		lockDiscovery, err := h.lockDiscoveryForPath(ctx, requestPath)
		if err != nil {
			return err
		}
		p.LockDiscovery = lockDiscovery
	}

	if mask.acl {
		allowed, err := h.checkACLPrivilege(ctx, user, requestPath, "read-acl")
		if err != nil {
			return err
		}
		if !allowed {
			p.aclForbidden = true
		} else {
			var entries []store.ACLEntry
			if h != nil && h.store != nil && h.store.ACLEntries != nil {
				entries, err = h.aclEntriesForResource(ctx, resourcePath)
				if err != nil {
					return err
				}
			}
			p.ACL = buildACLPropFromEntries(entries)
			if len(inheritedEntries) != 0 {
				inherited := buildACLPropFromEntries(inheritedEntries)
				for i := range inherited.ACE {
					inherited.ACE[i].Inherited = &aceInheritedResp{Href: ensureCollectionHref(inheritedACLPath)}
				}
				p.ACL.ACE = append(p.ACL.ACE, inherited.ACE...)
			}
			if ownerHref != "" {
				p.ACL.ACE = append([]aceResp{protectedOwnerACEForPath(ctx, resourcePath, ownerHref)}, p.ACL.ACE...)
			}
		}
	}

	if user != nil && p.CurrentUserPrincipal == nil {
		principalHref := h.principalURL(user)
		p.CurrentUserPrincipal = &hrefProp{Href: principalHref}
		p.CurrentUserPrincipalURL = &hrefProp{Href: principalHref}
	}
	if mask.currentUserPrivilegeSet && user != nil {
		allowed, err := h.checkACLPrivilege(ctx, user, requestPath, "read-current-user-privilege-set")
		if err != nil {
			return err
		}
		if !allowed {
			p.CurrentUserPrivilegeSet = nil
			p.currentUserPrivilegesForbidden = true
		} else {
			resolveSet := h.store == nil || h.store.ACLEntries != nil || p.CurrentUserPrivilegeSet == nil
			if !resolveSet {
				owner, err := h.isResourceOwner(ctx, user, resourcePath)
				if err != nil {
					return err
				}
				resolveSet = owner
			}
			if resolveSet {
				// CalendarPrivileges omits ACL-management privileges, so stored ACLs
				// must still supply the complete property even when a preset exists.
				privileges, err := h.currentUserPrivilegeSetForPath(ctx, user, requestPath)
				if err != nil {
					return err
				}
				p.CurrentUserPrivilegeSet = privileges
			}
		}
	}

	return nil
}

func inheritedACLSourcePath(resourcePath string) string {
	cleanPath := cleanDAVPath(resourcePath)
	if strings.HasPrefix(cleanPath, calendarPrefix+"/") {
		collectionPath := calendarCollectionPath(cleanPath)
		if collectionPath != cleanPath {
			return collectionPath
		}
	}
	if strings.HasPrefix(cleanPath, addressBookPrefix+"/") {
		collectionPath := addressBookCollectionPath(cleanPath)
		if collectionPath != cleanPath {
			return collectionPath
		}
	}
	return ""
}

func (h *DavServer) ownerPrincipalForPath(ctx context.Context, user *store.User, resourcePath string) (string, error) {
	cleanPath := cleanDAVPath(resourcePath)
	if strings.HasPrefix(cleanPath, "/dav/principals/") {
		segment := strings.Split(strings.Trim(strings.TrimPrefix(cleanPath, "/dav/principals/"), "/"), "/")[0]
		if id, err := strconv.ParseInt(segment, 10, 64); err == nil {
			return fmt.Sprintf("/dav/principals/%d/", id), nil
		}
	}
	if strings.HasPrefix(cleanPath, "/dav/calendars/") {
		if h == nil || h.store == nil || h.store.Calendars == nil {
			return h.principalURL(user), nil
		}
		segment := strings.Split(strings.Trim(strings.TrimPrefix(cleanPath, "/dav/calendars/"), "/"), "/")[0]
		id, ok, err := h.resolveCalendarID(ctx, user, segment)
		if err != nil {
			if errors.Is(err, store.ErrNotFound) && user != nil {
				return h.principalURL(user), nil
			}
			return "", err
		}
		if ok {
			calendar, err := h.getCalendar(ctx, id)
			if err == nil {
				return fmt.Sprintf("/dav/principals/%d/", calendar.UserID), nil
			}
			if !errors.Is(err, store.ErrNotFound) {
				return "", err
			}
		}
	}
	if strings.HasPrefix(cleanPath, "/dav/addressbooks/") {
		if h == nil || h.store == nil || h.store.AddressBooks == nil {
			return h.principalURL(user), nil
		}
		segment := strings.Split(strings.Trim(strings.TrimPrefix(cleanPath, "/dav/addressbooks/"), "/"), "/")[0]
		id, ok, err := h.resolveAddressBookID(ctx, user, segment)
		if err != nil {
			if errors.Is(err, store.ErrNotFound) && user != nil {
				return h.principalURL(user), nil
			}
			return "", err
		}
		if ok {
			book, err := h.getAddressBook(ctx, id)
			if err == nil {
				return fmt.Sprintf("/dav/principals/%d/", book.UserID), nil
			}
			if !errors.Is(err, store.ErrNotFound) {
				return "", err
			}
		}
	}
	return h.principalURL(user), nil
}

func (h *DavServer) currentUserPrivilegeSetForPath(ctx context.Context, user *store.User, resourcePath string) (*currentUserPrivilegeSet, error) {
	if user == nil {
		return nil, nil
	}

	cleanPath := cleanDAVPath(resourcePath)
	if isGenericDAVPrivilegePath(cleanPath) {
		owner, err := h.isResourceOwner(ctx, user, cleanPath)
		if err != nil {
			return nil, err
		}
		if owner {
			return currentUserPrivilegeSetForNames(calendarCurrentPrivilegeNames), nil
		}
	}
	if strings.HasPrefix(cleanPath, "/dav/calendars/") {
		if isBirthdayCalendarPath(ctx, cleanPath) {
			// The birthday calendar is virtual: there is no stored calendar to
			// resolve privileges from, so its collection and objects report the
			// read-only set their collection response advertises rather than
			// reporting the property absent.
			return birthdayCalendarCurrentUserPrivilegeSet(), nil
		}
		segment := singleCollectionSegment(cleanPath, "/dav/calendars/")
		if segment == "" {
			if target := parsedDAVTarget(ctx, cleanPath); target.Valid && target.Domain == davPathCalendar && target.Resource {
				segment = target.CollectionSegment
			}
		}
		if segment == "" {
			// The calendar home collection (/dav/calendars/) is a generic
			// collection the property applies to: present-empty, not a 404.
			return &currentUserPrivilegeSet{}, nil
		}

		calendarID, ok, err := h.resolveCalendarID(ctx, user, segment)
		if err != nil {
			return nil, err
		}
		if !ok {
			return nil, nil
		}
		access, err := h.loadCalendarWithAnyPrivilege(ctx, user, calendarID, cleanPath)
		if errors.Is(err, store.ErrNotFound) || errors.Is(err, errForbidden) {
			return nil, nil
		}
		if err != nil {
			return nil, err
		}
		if access == nil {
			return nil, nil
		}
		cal := &access.Calendar

		// Resolve the ACL state once and decide every privilege from it
		// instead of one full path/entry resolution per privilege name.
		pc, err := h.calendarPrivilegeContextFor(ctx, user, cal, cleanPath)
		if err != nil {
			return nil, err
		}
		if pc == nil {
			return nil, nil
		}
		privileges := make([]privilege, 0, len(calendarCurrentPrivilegeNames))
		for _, name := range calendarCurrentPrivilegeNames {
			if pc.reports(name) {
				privileges = append(privileges, privilegeElementForName(name))
			}
		}
		// The property applies to this resource, so it is present even when the
		// user holds no privileges. RFC 3744 defines the content model as
		// privilege*, so zero privileges is a present-empty 200, not a 404.
		return &currentUserPrivilegeSet{Privileges: privileges}, nil
	}

	if !strings.HasPrefix(cleanPath, "/dav/addressbooks/") {
		return h.genericCurrentUserPrivilegeSet(ctx, user, cleanPath)
	}

	segment := singleCollectionSegment(cleanPath, "/dav/addressbooks/")
	if segment == "" {
		if target := parsedDAVTarget(ctx, cleanPath); target.Valid && target.Domain == davPathAddressBook && target.Resource {
			segment = target.CollectionSegment
		}
	}
	if segment == "" {
		// The address-book home collection (/dav/addressbooks/) is a generic
		// collection the property applies to: present-empty, not a 404.
		return &currentUserPrivilegeSet{}, nil
	}

	bookID, ok, err := h.resolveAddressBookID(ctx, user, segment)
	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, nil
	}
	book, err := h.getAddressBook(ctx, bookID)
	if errors.Is(err, store.ErrNotFound) || errors.Is(err, errForbidden) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	if book == nil {
		return nil, nil
	}

	pc, err := h.addressBookPrivilegeContextFor(ctx, user, book, cleanPath)
	if err != nil {
		return nil, err
	}
	if pc == nil {
		return nil, nil
	}
	privileges := make([]privilege, 0, len(addressBookCurrentPrivilegeNames))
	for _, name := range addressBookCurrentPrivilegeNames {
		if pc.reports(name) {
			privileges = append(privileges, privilegeElementForName(name))
		}
	}
	// Present even with zero privileges (privilege* content model): return a
	// present-empty set rather than a property-level 404.
	return &currentUserPrivilegeSet{Privileges: privileges}, nil
}

func (h *DavServer) genericCurrentUserPrivilegeSet(ctx context.Context, user *store.User, resourcePath string) (*currentUserPrivilegeSet, error) {
	if h == nil || h.store == nil || h.store.ACLEntries == nil {
		return &currentUserPrivilegeSet{}, nil
	}
	entries, err := h.aclEntriesForResource(ctx, resourcePath)
	if err != nil {
		return nil, err
	}
	principals, err := h.applicablePrincipalsForPath(ctx, user, resourcePath, entries)
	if err != nil {
		return nil, err
	}
	privileges := make([]privilege, 0, len(calendarCurrentPrivilegeNames))
	for _, name := range calendarCurrentPrivilegeNames {
		if aclutil.HeldThroughLayers(principals, name, entries) {
			privileges = append(privileges, privilegeElementForName(name))
		}
	}
	return &currentUserPrivilegeSet{Privileges: privileges}, nil
}

func isGenericDAVPrivilegePath(resourcePath string) bool {
	cleanPath := path.Clean(resourcePath)
	if cleanPath == "/dav" || cleanPath == "/dav/calendars" || cleanPath == "/dav/addressbooks" || cleanPath == "/dav/principals" {
		return true
	}
	if strings.HasPrefix(cleanPath, "/dav/principals/") {
		return len(strings.Split(strings.Trim(strings.TrimPrefix(cleanPath, "/dav/principals/"), "/"), "/")) == 1
	}
	return false
}

func currentUserPrivilegeSetForNames(names []string) *currentUserPrivilegeSet {
	privileges := make([]privilege, 0, len(names))
	for _, name := range names {
		privileges = append(privileges, privilegeElementForName(name))
	}
	return &currentUserPrivilegeSet{Privileges: privileges}
}

// privilegeElementForName maps a privilege name to its XML response element.
func privilegeElementForName(name string) privilege {
	switch name {
	case "all":
		return privilege{All: &struct{}{}}
	case "read":
		return privilege{Read: &readPrivilege{}}
	case "read-free-busy":
		return privilege{ReadFreeBusy: &struct{}{}}
	case "read-acl":
		return privilege{ReadACL: &struct{}{}}
	case "read-current-user-privilege-set":
		return privilege{ReadCurrentUserPrivilegeSet: &struct{}{}}
	case "write":
		return privilege{Write: &struct{}{}}
	case "write-content":
		return privilege{WriteContent: &struct{}{}}
	case "write-properties":
		return privilege{WriteProperties: &struct{}{}}
	case "bind":
		return privilege{Bind: &struct{}{}}
	case "unbind":
		return privilege{Unbind: &struct{}{}}
	case "write-acl":
		return privilege{WriteACL: &struct{}{}}
	case "unlock":
		return privilege{Unlock: &struct{}{}}
	}
	return privilege{}
}

func (h *DavServer) lockDiscoveryForPath(ctx context.Context, resourcePath string) (*lockDiscoveryProp, error) {
	if h == nil || h.store == nil || h.store.Locks == nil {
		return &lockDiscoveryProp{}, nil
	}
	resourcePath, paths := h.lockLookupPathsForResource(ctx, resourcePath)

	locks, err := h.locksForLookupPaths(ctx, paths)
	if err != nil {
		return nil, err
	}

	// Only the creator is told the token. Every other reader still sees the
	// activelock element, so a client can tell the resource is locked and that
	// it does not hold the lock.
	user, _ := auth.UserFromContext(ctx)

	now := time.Now()
	activeLocks := make([]activeLock, 0, len(locks))
	for i := range locks {
		lock := locks[i]
		if lock.ExpiresAt.Before(now) {
			continue
		}
		if !sameLockRoot(lock.ResourcePath, resourcePath) && lock.Depth != "infinity" {
			continue
		}
		activeLocks = append(activeLocks, activeLockFromStoreLock(&lock, user != nil && lock.UserID == user.ID))
	}

	return &lockDiscoveryProp{ActiveLocks: activeLocks}, nil
}

// lockLookupPathsForResource canonicalizes resourcePath (best effort) and
// returns the canonical path together with the set of paths whose locks could
// apply to it: the resource itself and its ancestors.
func (h *DavServer) lockLookupPathsForResource(ctx context.Context, resourcePath string) (string, []string) {
	if user, ok := auth.UserFromContext(ctx); ok {
		if canonicalPath, err := h.canonicalDAVPath(ctx, user, resourcePath); err == nil && canonicalPath != "" {
			resourcePath = canonicalPath
		}
	}
	return resourcePath, lockLookupPaths(resourcePath)
}

// locksForLookupPaths returns the active locks for the given lookup paths,
// served from the request's lock index for the paths it has read and read from
// the store, then indexed, for the rest. A request without a lock index state
// reads the store each time.
func (h *DavServer) locksForLookupPaths(ctx context.Context, paths []string) ([]store.Lock, error) {
	idx := lockBatchIndexFromContext(ctx)
	if idx == nil || idx.isStale() {
		locks, err := h.store.Locks.ListByResources(ctx, paths)
		if err != nil {
			return nil, err
		}
		if state := davRequestStateFromContext(ctx); state != nil {
			state.setLockIndex(newLockBatchIndex(paths, locks))
		}
		return locks, nil
	}
	if missing := idx.unfetched(paths); len(missing) > 0 {
		locks, err := h.store.Locks.ListByResources(ctx, missing)
		if err != nil {
			return nil, err
		}
		idx.add(missing, locks)
	}
	return idx.locksForPaths(paths), nil
}

// prefetchLockBatchIndex fetches, in a single query, every lock that could
// apply to any response in the batch and returns a context carrying the
// resulting index. Per-response lockDiscoveryForPath calls then read from the
// index instead of issuing a query each, collapsing a Depth: 1 N+1 to one query.
func (h *DavServer) prefetchLockBatchIndex(ctx context.Context, responses []response) (context.Context, error) {
	if h == nil || h.store == nil || h.store.Locks == nil {
		return ctx, nil
	}
	seen := make(map[string]struct{})
	var union []string
	for i := range responses {
		if len(responses[i].Propstat) == 0 {
			continue
		}
		resourcePath := normalizeDAVHref(responses[i].Href)
		if resourcePath == "" || !strings.HasPrefix(resourcePath, "/dav") {
			continue
		}
		_, paths := h.lockLookupPathsForResource(ctx, resourcePath)
		for _, p := range paths {
			if _, ok := seen[p]; ok {
				continue
			}
			seen[p] = struct{}{}
			union = append(union, p)
		}
	}
	if len(union) == 0 {
		return ctx, nil
	}
	locks, err := h.store.Locks.ListByResources(ctx, union)
	if err != nil {
		return nil, err
	}
	return withLockBatchIndex(ctx, newLockBatchIndex(union, locks)), nil
}

func (h *DavServer) accessibleAddressBooks(ctx context.Context, user *store.User) ([]store.AddressBook, error) {
	if h == nil || h.store == nil || h.store.AddressBooks == nil || user == nil {
		return nil, nil
	}
	books, err := h.store.AddressBooks.ListAccessible(ctx, user.ID)
	if err != nil {
		return nil, err
	}
	if state := davRequestStateFromContext(ctx); state != nil {
		for i := range books {
			state.putAddressBook(&books[i])
		}
	}
	return books, nil
}

func stringPtr(v string) *string {
	return &v
}
