package dav

import (
	"context"
	"net/http"
	"strconv"
	"strings"
	"time"

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
}

// decorationMaskFor derives the set of expensive properties worth computing for
// a PROPFIND request. A specific <prop> request only needs the properties it
// names; allprop, propname, and compat (nil) requests retain the previous
// behavior of computing everything.
func decorationMaskFor(req *propfindRequest) propDecorationMask {
	if req == nil || req.Prop == nil {
		return propDecorationMask{lockDiscovery: true, acl: true, currentUserPrivilegeSet: true}
	}
	return propDecorationMask{
		lockDiscovery:           req.Prop.LockDiscovery != nil,
		acl:                     req.Prop.ACLProp != nil,
		currentUserPrivilegeSet: req.Prop.CurrentUserPrivilegeSet != nil,
	}
}

func (h *DavServer) decoratePropfindResponses(ctx context.Context, r *http.Request, user *store.User, responses []response, mask propDecorationMask) error {
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
		for j := range responses[i].Propstat {
			if responses[i].Propstat[j].Status != httpStatusOK {
				continue
			}
			if err := h.decorateDAVProp(ctx, user, resourcePath, &responses[i].Propstat[j].Prop, mask); err != nil {
				return err
			}
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
	return nil
}

func (h *DavServer) decorateDAVProp(ctx context.Context, user *store.User, resourcePath string, p *prop, mask propDecorationMask) error {
	if p == nil || resourcePath == "" || !strings.HasPrefix(resourcePath, "/dav") {
		return nil
	}

	// These are static or cheap to build, so always populate them.
	p.SupportedLock = defaultSupportedLock()
	p.SupportedPrivilegeSet = defaultSupportedPrivilegeSet()
	p.PrincipalCollectionSet = &hrefListProp{Href: []string{"/dav/principals/"}}

	if mask.lockDiscovery {
		lockDiscovery, err := h.lockDiscoveryForPath(ctx, resourcePath)
		if err != nil {
			return err
		}
		p.LockDiscovery = lockDiscovery
	}

	if mask.acl && h != nil && h.store != nil && h.store.ACLEntries != nil {
		entries, err := h.aclEntriesForResource(ctx, resourcePath)
		if err != nil {
			return err
		}
		p.ACL = buildACLPropFromEntries(entries)
	}

	if user != nil && p.CurrentUserPrincipal == nil {
		principalHref := h.principalURL(user)
		p.CurrentUserPrincipal = &expandableHrefProp{Href: principalHref}
		p.CurrentUserPrincipalURL = &hrefProp{Href: principalHref}
	}
	if mask.currentUserPrivilegeSet && user != nil && p.CurrentUserPrivilegeSet == nil {
		p.CurrentUserPrivilegeSet = h.currentUserPrivilegeSetForPath(ctx, user, resourcePath)
	}

	return nil
}

func (h *DavServer) currentUserPrivilegeSetForPath(ctx context.Context, user *store.User, resourcePath string) *currentUserPrivilegeSet {
	if user == nil {
		return nil
	}

	cleanPath := normalizeDAVHref(resourcePath)
	if strings.HasPrefix(cleanPath, "/dav/calendars/") {
		segment := singleCollectionSegment(cleanPath, "/dav/calendars/")
		if segment == "" {
			if target := parsedDAVTarget(ctx, cleanPath); target.Valid && target.Domain == davPathCalendar && target.Resource {
				segment = target.CollectionSegment
			}
		}
		if segment == "" {
			return nil
		}

		calendarID, ok, err := h.resolveCalendarID(ctx, user, segment)
		if err != nil || !ok {
			return nil
		}
		cal, err := h.getCalendar(ctx, calendarID)
		if err != nil || cal == nil {
			return nil
		}

		// Resolve the ACL state once and decide every privilege from it
		// instead of one full path/entry resolution per privilege name.
		pc, err := h.calendarPrivilegeContextFor(ctx, user, cal, cleanPath)
		if err != nil || pc == nil {
			return nil
		}
		privileges := make([]privilege, 0, len(calendarPrivilegeNames))
		for _, name := range calendarPrivilegeNames {
			if allowed, _ := pc.decide(name); allowed {
				privileges = append(privileges, privilegeElementForName(name))
			}
		}
		if len(privileges) == 0 {
			return nil
		}
		return &currentUserPrivilegeSet{Privileges: privileges}
	}

	if !strings.HasPrefix(cleanPath, "/dav/addressbooks/") {
		return nil
	}

	segment := singleCollectionSegment(cleanPath, "/dav/addressbooks/")
	if segment == "" {
		if target := parsedDAVTarget(ctx, cleanPath); target.Valid && target.Domain == davPathAddressBook && target.Resource {
			segment = target.CollectionSegment
		}
	}
	if segment == "" {
		return nil
	}

	bookID, ok, err := h.resolveAddressBookID(ctx, user, segment)
	if err != nil || !ok {
		return nil
	}
	book, err := h.getAddressBook(ctx, bookID)
	if err != nil || book == nil {
		return nil
	}

	pc, err := h.addressBookPrivilegeContextFor(ctx, user, book, cleanPath)
	if err != nil || pc == nil {
		return nil
	}
	privileges := make([]privilege, 0, len(addressBookPrivilegeNames))
	for _, name := range addressBookPrivilegeNames {
		if allowed, _ := pc.decide(name); allowed {
			privileges = append(privileges, privilegeElementForName(name))
		}
	}
	if len(privileges) == 0 {
		return nil
	}
	return &currentUserPrivilegeSet{Privileges: privileges}
}

// privilegeElementForName maps a privilege name to its XML response element.
func privilegeElementForName(name string) privilege {
	switch name {
	case "read":
		return privilege{Read: &readPrivilege{}}
	case "read-free-busy":
		return privilege{ReadFreeBusy: &struct{}{}}
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

	now := time.Now()
	activeLocks := make([]activeLock, 0, len(locks))
	for i := range locks {
		lock := locks[i]
		if lock.ExpiresAt.Before(now) {
			continue
		}
		lockPath := normalizeDAVResourceIdentity(lock.ResourcePath)
		if lockPath != resourcePath && lock.Depth != "infinity" {
			continue
		}
		activeLocks = append(activeLocks, activeLockFromStoreLock(&lock))
	}

	return &lockDiscoveryProp{ActiveLocks: activeLocks}, nil
}

// lockLookupPathsForResource canonicalizes resourcePath (best effort) and
// returns the canonical path together with the set of paths whose locks could
// apply to it (the resource itself plus its ancestors and legacy aliases).
func (h *DavServer) lockLookupPathsForResource(ctx context.Context, resourcePath string) (string, []string) {
	if user, ok := auth.UserFromContext(ctx); ok {
		if canonicalPath, err := h.canonicalDAVPath(ctx, user, resourcePath); err == nil && canonicalPath != "" {
			resourcePath = canonicalPath
		}
	}
	return resourcePath, lockLookupPaths(resourcePath)
}

// locksForLookupPaths returns the active locks for the given lookup paths. When
// a prefetched batch index is installed (see prefetchLockBatchIndex) it serves
// from that index to avoid one lock query per PROPFIND response; otherwise it
// queries directly.
func (h *DavServer) locksForLookupPaths(ctx context.Context, paths []string) ([]store.Lock, error) {
	if idx := lockBatchIndexFromContext(ctx); idx != nil && !idx.isStale() {
		return idx.locksForPaths(paths), nil
	}
	return h.store.Locks.ListByResources(ctx, paths)
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
	byPath := make(map[string][]store.Lock, len(locks))
	for i := range locks {
		key := normalizeDAVHref(locks[i].ResourcePath)
		byPath[key] = append(byPath[key], locks[i])
	}
	return withLockBatchIndex(ctx, &lockBatchIndex{byPath: byPath}), nil
}

func (h *DavServer) accessibleAddressBooks(ctx context.Context, user *store.User) ([]store.AddressBook, error) {
	if h == nil || h.store == nil || h.store.AddressBooks == nil || user == nil {
		return nil, nil
	}
	owned, err := h.store.AddressBooks.ListByUser(ctx, user.ID)
	if err != nil {
		return nil, err
	}
	if h.store.ACLEntries == nil {
		return owned, nil
	}

	seen := make(map[int64]struct{}, len(owned))
	for _, book := range owned {
		seen[book.ID] = struct{}{}
	}

	principals := []string{"DAV:all", "DAV:authenticated", h.principalURL(user)}
	for _, principal := range principals {
		entries, err := h.store.ACLEntries.ListByPrincipal(ctx, principal)
		if err != nil {
			return nil, err
		}
		for _, entry := range entries {
			collectionPath := addressBookCollectionPath(entry.ResourcePath)
			if collectionPath == "/dav/addressbooks" || !strings.HasPrefix(collectionPath, "/dav/addressbooks/") {
				continue
			}
			granted, err := h.checkACLPrivilege(ctx, user, collectionPath, "read")
			if err != nil {
				return nil, err
			}
			if !granted {
				continue
			}

			segment := strings.Trim(strings.TrimPrefix(collectionPath, "/dav/addressbooks/"), "/")
			if segment == "" || strings.Contains(segment, "/") {
				continue
			}

			bookID, err := strconv.ParseInt(segment, 10, 64)
			if err != nil {
				continue
			}
			if _, ok := seen[bookID]; ok {
				continue
			}
			book, err := h.getAddressBook(ctx, bookID)
			if err != nil {
				if err == store.ErrNotFound {
					continue
				}
				return nil, err
			}
			seen[bookID] = struct{}{}
			owned = append(owned, *book)
		}
	}

	return owned, nil
}

func stringPtr(v string) *string {
	return &v
}
