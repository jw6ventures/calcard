package dav

import (
	"context"
	"encoding/xml"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"path"
	"strconv"
	"strings"

	"github.com/jw6ventures/calcard/internal/acl"
	"github.com/jw6ventures/calcard/internal/auth"
	"github.com/jw6ventures/calcard/internal/store"
)

func (h *DavServer) acl(w http.ResponseWriter, r *http.Request) {
	h.logger().Trace("Acl", "ACL %s", r.URL.Path)
	user, ok := auth.UserFromContext(r.Context())
	if !ok {
		http.Error(w, "missing user", http.StatusUnauthorized)
		return
	}

	cleanPath := path.Clean(r.URL.Path)
	canonicalPath, err := h.canonicalDAVPath(r.Context(), user, cleanPath)
	if err != nil {
		if errors.Is(err, store.ErrNotFound) {
			http.Error(w, "not found", http.StatusNotFound)
			return
		}
		if errors.Is(err, errAmbiguousAddressBook) || errors.Is(err, errAmbiguousCalendar) {
			http.Error(w, "ambiguous path", http.StatusConflict)
			return
		}
		http.Error(w, "failed to resolve path", http.StatusInternalServerError)
		return
	}
	if isProtectedVirtualDAVRoot(canonicalPath) {
		writeDAVError(w, http.StatusForbidden, "no-protected-ace-conflict")
		return
	}
	expected, err := h.aclResourceExpectation(r.Context(), user, cleanPath)
	if err != nil {
		if errors.Is(err, store.ErrNotFound) {
			http.Error(w, "not found", http.StatusNotFound)
		} else {
			http.Error(w, "failed to resolve resource", http.StatusInternalServerError)
		}
		return
	}

	// Verify user has write-acl privilege
	allowed, err := h.checkACLPrivilege(r.Context(), user, canonicalPath, "write-acl")
	if err != nil {
		http.Error(w, "failed to evaluate ACL", http.StatusInternalServerError)
		return
	}
	if !allowed {
		writeNeedPrivileges(w, canonicalPath, "write-acl")
		return
	}
	if !h.requireLock(w, r, cleanPath, "resource is locked") {
		return
	}

	body, err := readDAVBody(w, r, maxDAVBodyBytes)
	if err != nil {
		if errors.Is(err, errRequestTooLarge) {
			http.Error(w, "request too large", http.StatusRequestEntityTooLarge)
		} else {
			http.Error(w, "failed to read body", http.StatusBadRequest)
		}
		return
	}

	var req aclRequest
	if err := safeUnmarshalXML(body, &req); err != nil {
		http.Error(w, "invalid ACL request", http.StatusBadRequest)
		return
	}
	if len(req.Unknown) != 0 {
		http.Error(w, "invalid ACL request", http.StatusBadRequest)
		return
	}

	var entries []store.ACLEntry
	ownerPrincipal, err := h.ownerPrincipalForPath(r.Context(), user, canonicalPath)
	if err != nil {
		http.Error(w, "failed to resolve resource owner", http.StatusInternalServerError)
		return
	}
	for acePosition, a := range req.ACE {
		if err := validateACE(a); err != nil {
			http.Error(w, "invalid ACL request", http.StatusBadRequest)
			return
		}
		if len(a.Protected) != 0 {
			writeDAVError(w, http.StatusForbidden, "no-protected-ace-conflict")
			return
		}
		if len(a.Inherited) != 0 {
			writeDAVError(w, http.StatusForbidden, "no-inherited-ace-conflict")
			return
		}
		if len(a.Invert) != 0 {
			writeDAVError(w, http.StatusForbidden, "no-invert")
			return
		}
		principalHref, condition, err := h.resolveACLPrincipal(r, a.Principal[0])
		if err != nil {
			http.Error(w, "failed to resolve principal", http.StatusInternalServerError)
			return
		}
		if condition != "" {
			writeDAVError(w, http.StatusForbidden, condition)
			return
		}
		if len(a.Deny) != 0 && aclPrincipalMatchesProtectedOwner(principalHref, ownerPrincipal, canonicalPath) {
			writeDAVError(w, http.StatusForbidden, "no-protected-ace-conflict")
			return
		}

		if len(a.Grant) != 0 {
			for _, priv := range a.Grant[0].Privileges {
				if err := validateACEPrivilege(priv); err != nil {
					if errors.Is(err, errUnsupportedACEPrivilege) {
						writeDAVError(w, http.StatusForbidden, "not-supported-privilege")
						return
					}
					http.Error(w, "invalid privilege in ACE", http.StatusBadRequest)
					return
				}
				for _, name := range extractACEPrivilegeNames(priv) {
					entries = append(entries, store.ACLEntry{
						ResourcePath:  canonicalPath,
						PrincipalHref: principalHref,
						IsGrant:       true,
						Privilege:     name,
						Position:      acePosition,
					})
				}
			}
		}
		if len(a.Deny) != 0 {
			for _, priv := range a.Deny[0].Privileges {
				if err := validateACEPrivilege(priv); err != nil {
					if errors.Is(err, errUnsupportedACEPrivilege) {
						writeDAVError(w, http.StatusForbidden, "not-supported-privilege")
						return
					}
					http.Error(w, "invalid privilege in ACE", http.StatusBadRequest)
					return
				}
				for _, name := range extractACEPrivilegeNames(priv) {
					entries = append(entries, store.ACLEntry{
						ResourcePath:  canonicalPath,
						PrincipalHref: principalHref,
						IsGrant:       false,
						Privilege:     name,
						Position:      acePosition,
					})
				}
			}
		}
	}

	if err := h.store.SetACLAndState(r.Context(), canonicalPath, entries, expected, h.lockPreconditions(r, cleanPath)); err != nil {
		if errors.Is(err, store.ErrLockConflict) {
			http.Error(w, "resource is locked", http.StatusLocked)
			return
		}
		if errors.Is(err, store.ErrResourceStateChanged) || errors.Is(err, store.ErrNotFound) {
			http.Error(w, "resource state changed", http.StatusPreconditionFailed)
			return
		}
		http.Error(w, "failed to set ACL", http.StatusInternalServerError)
		return
	}
	invalidateDAVRequestState(r.Context())

	w.WriteHeader(http.StatusOK)
}

func (h *DavServer) aclResourceExpectation(ctx context.Context, user *store.User, cleanPath string) (store.ACLResourceExpectation, error) {
	target := parsedDAVTarget(ctx, cleanPath)
	if !target.Valid || target.CollectionSegment == "" {
		return store.ACLResourceExpectation{}, nil
	}
	switch target.Domain {
	case davPathCalendar:
		calendarID, ok, err := h.resolveCalendarID(ctx, user, target.CollectionSegment)
		if err != nil {
			return store.ACLResourceExpectation{}, err
		}
		if !ok || calendarID == birthdayCalendarID {
			return store.ACLResourceExpectation{}, store.ErrNotFound
		}
		cal, err := h.getCalendar(ctx, calendarID)
		if errors.Is(err, store.ErrNotFound) && h.store != nil && h.store.Calendars != nil && user != nil {
			if access, accessErr := h.store.Calendars.GetAccessible(ctx, calendarID, user.ID); accessErr != nil {
				return store.ACLResourceExpectation{}, accessErr
			} else if access != nil {
				cal = &access.Calendar
				err = nil
			}
		}
		if err != nil {
			return store.ACLResourceExpectation{}, err
		}
		expected := store.ACLResourceExpectation{CollectionKind: "calendar", CollectionID: calendarID, CollectionCTag: &cal.CTag}
		if target.Resource {
			event, err := h.store.Events.GetByResourceName(ctx, calendarID, target.ResourceName)
			if err != nil {
				return store.ACLResourceExpectation{}, err
			}
			if event == nil {
				return store.ACLResourceExpectation{}, store.ErrNotFound
			}
			state := store.EventDAVResourceState(event)
			expected.ResourceState = &state
		}
		return expected, nil
	case davPathAddressBook:
		addressBookID, ok, err := h.resolveAddressBookID(ctx, user, target.CollectionSegment)
		if err != nil {
			return store.ACLResourceExpectation{}, err
		}
		if !ok {
			return store.ACLResourceExpectation{}, store.ErrNotFound
		}
		book, err := h.getAddressBook(ctx, addressBookID)
		if err != nil {
			return store.ACLResourceExpectation{}, err
		}
		expected := store.ACLResourceExpectation{CollectionKind: "addressbook", CollectionID: addressBookID, CollectionCTag: &book.CTag}
		if target.Resource {
			contact, err := h.store.Contacts.GetByResourceName(ctx, addressBookID, target.ResourceName)
			if err != nil {
				return store.ACLResourceExpectation{}, err
			}
			if contact == nil {
				return store.ACLResourceExpectation{}, store.ErrNotFound
			}
			state := store.ContactDAVResourceState(contact)
			expected.ResourceState = &state
		}
		return expected, nil
	default:
		return store.ACLResourceExpectation{}, nil
	}
}

func validateACE(a ace) error {
	if len(a.Unknown) != 0 || len(a.Principal)+len(a.Invert) != 1 {
		return fmt.Errorf("expected exactly one principal")
	}
	if len(a.Principal) == 1 {
		if err := validateACEPrincipal(a.Principal[0]); err != nil {
			return err
		}
	}
	if len(a.Invert) == 1 {
		invert := a.Invert[0]
		if len(invert.Principal) != 1 || len(invert.Unknown) != 0 {
			return fmt.Errorf("invert must contain exactly one principal")
		}
		if err := validateACEPrincipal(invert.Principal[0]); err != nil {
			return err
		}
	}
	if (len(a.Grant) == 1) == (len(a.Deny) == 1) || len(a.Grant) > 1 || len(a.Deny) > 1 {
		return fmt.Errorf("expected exactly one of grant or deny")
	}
	if len(a.Grant) == 1 && (len(a.Grant[0].Privileges) == 0 || len(a.Grant[0].Unknown) != 0) {
		return fmt.Errorf("grant must contain at least one privilege")
	}
	if len(a.Deny) == 1 && (len(a.Deny[0].Privileges) == 0 || len(a.Deny[0].Unknown) != 0) {
		return fmt.Errorf("deny must contain at least one privilege")
	}
	if len(a.Protected) > 1 || len(a.Inherited) > 1 {
		return fmt.Errorf("duplicate ACE metadata")
	}
	if len(a.Inherited) == 1 && (len(a.Inherited[0].Href) != 1 || len(a.Inherited[0].Unknown) != 0) {
		return fmt.Errorf("inherited must contain exactly one href")
	}
	if len(a.Sequence) < 2 || (a.Sequence[0] != (xml.Name{Space: "DAV:", Local: "principal"}) &&
		a.Sequence[0] != (xml.Name{Space: "DAV:", Local: "invert"})) ||
		(a.Sequence[1] != (xml.Name{Space: "DAV:", Local: "grant"}) && a.Sequence[1] != (xml.Name{Space: "DAV:", Local: "deny"})) {
		return fmt.Errorf("invalid ACE child order")
	}
	next := 2
	if next < len(a.Sequence) && a.Sequence[next] == (xml.Name{Space: "DAV:", Local: "protected"}) {
		next++
	}
	if next < len(a.Sequence) && a.Sequence[next] == (xml.Name{Space: "DAV:", Local: "inherited"}) {
		next++
	}
	if next != len(a.Sequence) {
		return fmt.Errorf("invalid ACE child order")
	}
	return nil
}

func validateACEPrincipal(principal acePrincipal) error {
	if len(principal.Unknown) != 0 {
		return fmt.Errorf("unsupported principal element")
	}
	count := len(principal.Href) + len(principal.All) + len(principal.Authenticated) + len(principal.Unauthenticated) + len(principal.Property) + len(principal.Self)
	if count != 1 {
		return fmt.Errorf("expected exactly one principal form")
	}
	if len(principal.Property) == 1 {
		property := principal.Property[0]
		if len(property.Unknown) != 0 || len(property.Owner)+len(property.Group) != 1 {
			return fmt.Errorf("unsupported property principal")
		}
	}
	return nil
}

var errUnsupportedACEPrivilege = errors.New("unsupported ACE privilege")

func validateACEPrivilege(priv acePrivilege) error {
	count := 0
	for _, present := range []bool{
		priv.Read != nil,
		priv.Write != nil,
		priv.WriteContent != nil,
		priv.WriteProperties != nil,
		priv.ReadACL != nil,
		priv.ReadCurrentUserPrivilegeSet != nil,
		priv.WriteACL != nil,
		priv.Unlock != nil,
		priv.Bind != nil,
		priv.Unbind != nil,
		priv.ReadFreeBusy != nil,
		priv.All != nil,
	} {
		if present {
			count++
		}
	}
	if len(priv.Unknown) > 0 {
		return fmt.Errorf("%w %q", errUnsupportedACEPrivilege, xmlNameString(priv.Unknown[0].XMLName))
	}
	if priv.Elements > 0 {
		count = priv.Elements
	}
	if count != 1 {
		return fmt.Errorf("expected exactly one privilege element")
	}
	return nil
}

func (h *DavServer) resolveACLPrincipal(r *http.Request, principal acePrincipal) (string, string, error) {
	switch {
	case len(principal.Href) != 0:
		return h.resolveACLPrincipalHref(r, principal.Href[0])
	case len(principal.All) != 0:
		return acl.PrincipalAll, "", nil
	case len(principal.Authenticated) != 0:
		return acl.PrincipalAuthenticated, "", nil
	case len(principal.Unauthenticated) != 0:
		return acl.PrincipalUnauthenticated, "", nil
	case len(principal.Property) != 0 && len(principal.Property[0].Owner) != 0:
		return acl.PrincipalPropertyOwner, "", nil
	case len(principal.Property) != 0 && len(principal.Property[0].Group) != 0:
		return acl.PrincipalPropertyGroup, "", nil
	case len(principal.Self) != 0:
		return acl.PrincipalSelf, "", nil
	default:
		return "", "", fmt.Errorf("unsupported principal")
	}
}

func (h *DavServer) resolveACLPrincipalHref(r *http.Request, raw string) (string, string, error) {
	u, err := url.ParseRequestURI(strings.TrimSpace(raw))
	if err != nil || u.User != nil || u.RawQuery != "" || u.Fragment != "" {
		return "", "recognized-principal", nil
	}
	if u.IsAbs() {
		if (u.Scheme != "http" && u.Scheme != "https") || !strings.EqualFold(u.Host, r.Host) {
			return "", "recognized-principal", nil
		}
	} else if u.Host != "" || !strings.HasPrefix(u.Path, "/") {
		return "", "recognized-principal", nil
	}
	cleanPath := path.Clean(u.Path)
	if cleanPath == "." || !strings.HasPrefix(cleanPath, "/dav/principals/") {
		return "", "recognized-principal", nil
	}
	principalIDText := strings.TrimSuffix(strings.TrimPrefix(cleanPath, "/dav/principals/"), "/")
	if principalIDText == "" || strings.Contains(principalIDText, "/") {
		return "", "recognized-principal", nil
	}
	principalID, err := strconv.ParseInt(principalIDText, 10, 64)
	if err != nil || principalID <= 0 {
		return "", "recognized-principal", nil
	}
	if h != nil && h.store != nil && h.store.Users != nil {
		principal, err := h.store.Users.GetByID(r.Context(), principalID)
		if err != nil {
			return "", "", err
		}
		if principal == nil {
			return "", "recognized-principal", nil
		}
	}
	return acl.PrincipalHref(principalID), "", nil
}

func aclPrincipalMatchesProtectedOwner(principal, ownerHref, resourcePath string) bool {
	principal = acl.NormalizePrincipalHref(principal)
	ownerHref = acl.NormalizePrincipalHref(ownerHref)
	switch principal {
	case acl.PrincipalAll, acl.PrincipalAuthenticated, acl.PrincipalPropertyOwner:
		return ownerHref != ""
	case acl.PrincipalSelf:
		return ownerHref != "" && acl.NormalizePrincipalHref(resourcePath) == ownerHref
	default:
		return ownerHref != "" && principal == ownerHref
	}
}

func extractACEPrivilegeNames(priv acePrivilege) []string {
	var names []string
	if priv.Read != nil {
		names = append(names, "read")
	}
	if priv.Write != nil {
		names = append(names, "write")
	}
	if priv.WriteContent != nil {
		names = append(names, "write-content")
	}
	if priv.WriteProperties != nil {
		names = append(names, "write-properties")
	}
	if priv.ReadACL != nil {
		names = append(names, "read-acl")
	}
	if priv.ReadCurrentUserPrivilegeSet != nil {
		names = append(names, "read-current-user-privilege-set")
	}
	if priv.WriteACL != nil {
		names = append(names, "write-acl")
	}
	if priv.Unlock != nil {
		names = append(names, "unlock")
	}
	if priv.Bind != nil {
		names = append(names, "bind")
	}
	if priv.Unbind != nil {
		names = append(names, "unbind")
	}
	if priv.ReadFreeBusy != nil {
		names = append(names, "read-free-busy")
	}
	if priv.All != nil {
		names = append(names, "all")
	}
	return names
}

// checkACLPrivilege verifies a user has a specific privilege on a resource.
func (h *DavServer) checkACLPrivilege(ctx context.Context, user *store.User, resourcePath, privilege string) (bool, error) {
	if h == nil || h.store == nil || h.store.ACLEntries == nil {
		return true, nil
	}
	if canonicalPath, err := h.canonicalDAVPath(ctx, user, resourcePath); err == nil && canonicalPath != "" {
		resourcePath = canonicalPath
	} else if err != nil {
		return false, err
	}

	// Check if user is the resource owner — owners always have all privileges
	if h.isResourceOwner(ctx, user, resourcePath) {
		return true, nil
	}

	if granted, applicable, err := h.aclDecision(ctx, user, resourcePath, privilege); err != nil {
		return false, err
	} else if applicable {
		return granted, nil
	}

	switch {
	case strings.HasPrefix(resourcePath, "/dav/calendars/"):
		collectionPath := calendarCollectionPath(resourcePath)
		if collectionPath != resourcePath {
			if granted, applicable, err := h.aclDecision(ctx, user, collectionPath, privilege); err != nil {
				return false, err
			} else if applicable {
				return granted, nil
			}
		}
	case strings.HasPrefix(resourcePath, "/dav/addressbooks/"):
		collectionPath := addressBookCollectionPath(resourcePath)
		if collectionPath != resourcePath {
			if granted, applicable, err := h.aclDecision(ctx, user, collectionPath, privilege); err != nil {
				return false, err
			} else if applicable {
				return granted, nil
			}
		}
	}

	return false, nil
}

func (h *DavServer) requireACLPrivilege(ctx context.Context, user *store.User, resourcePath, privilege string) error {
	allowed, err := h.checkACLPrivilege(ctx, user, resourcePath, privilege)
	if err != nil {
		return err
	}
	if !allowed {
		return errForbidden
	}
	return nil
}

func (h *DavServer) aclDecision(ctx context.Context, user *store.User, resourcePath, privilege string) (bool, bool, error) {
	if h == nil || h.store == nil || h.store.ACLEntries == nil {
		return false, false, nil
	}

	entries, err := h.aclEntriesForResource(ctx, resourcePath)
	if err != nil || len(entries) == 0 {
		return false, false, err
	}

	applicablePrincipals, err := h.applicablePrincipalsForPath(ctx, user, resourcePath, entries)
	if err != nil {
		return false, false, err
	}
	granted, applicable := acl.DecisionForPrivilege(entries, applicablePrincipals, privilege)
	return granted, applicable, nil
}

// applicablePrincipalsForPath resolves the principal set an ACL decision over
// resourcePath evaluates against. The resource's owner is only looked up when
// some entry names a principal form that needs it, so the common ACL naming
// only hrefs and DAV:all keeps costing no extra query.
func (h *DavServer) applicablePrincipalsForPath(ctx context.Context, user *store.User, resourcePath string, entries []store.ACLEntry) (map[string]struct{}, error) {
	if !acl.NeedsResourcePrincipals(entries) {
		return acl.ApplicablePrincipals(user), nil
	}
	owner, err := h.ownerPrincipalForPath(ctx, user, resourcePath)
	if err != nil {
		return nil, err
	}
	return acl.ApplicablePrincipalsFor(user, acl.ResourcePrincipals{
		OwnerHref: owner,
		SelfHref:  principalResourceHref(resourcePath),
	}), nil
}

// principalResourceHref returns the principal a principal resource identifies,
// which is what DAV:self resolves to there, and an empty string for every other
// resource.
func principalResourceHref(resourcePath string) string {
	cleanPath := normalizeDAVHref(resourcePath)
	if !strings.HasPrefix(cleanPath, "/dav/principals/") {
		return ""
	}
	segments := strings.Split(strings.Trim(strings.TrimPrefix(cleanPath, "/dav/principals"), "/"), "/")
	if len(segments) != 1 {
		return ""
	}
	id, err := strconv.ParseInt(segments[0], 10, 64)
	if err != nil || id <= 0 {
		return ""
	}
	return acl.PrincipalHref(id)
}

func (h *DavServer) aclEntriesForResource(ctx context.Context, resourcePath string) ([]store.ACLEntry, error) {
	resourcePath = normalizeDAVResourceIdentity(resourcePath)
	if isProtectedVirtualDAVRoot(resourcePath) {
		return nil, nil
	}

	// Within a read request the same resource path is resolved once per
	// privilege; reuse the cached entries to avoid repeating the query.
	cache := aclEntryCacheFromContext(ctx)
	if cache != nil {
		if entries, ok := cache.get(resourcePath); ok {
			return entries, nil
		}
	}

	candidates := append([]string{resourcePath}, legacyDAVResourcePaths(resourcePath)...)
	seen := make(map[string]struct{}, len(candidates))
	var result []store.ACLEntry
	for _, candidate := range candidates {
		if candidate == "" {
			continue
		}
		if _, ok := seen[candidate]; ok {
			continue
		}
		seen[candidate] = struct{}{}
		entries, err := h.store.ACLEntries.ListByResource(ctx, candidate)
		if err != nil {
			return nil, err
		}
		result = append(result, entries...)
	}
	acl.SortEntries(result)

	if cache != nil {
		cache.put(resourcePath, result)
	}
	return result, nil
}

func isProtectedVirtualDAVRoot(resourcePath string) bool {
	switch normalizeDAVHref(resourcePath) {
	case "/dav", "/dav/calendars", "/dav/addressbooks", "/dav/principals":
		return true
	default:
		return false
	}
}

func (h *DavServer) isResourceOwner(ctx context.Context, user *store.User, resourcePath string) bool {
	if h == nil || h.store == nil || user == nil {
		return false
	}
	cleanPath := path.Clean(resourcePath)
	if cleanPath == "/dav" || cleanPath == "/dav/calendars" || cleanPath == "/dav/addressbooks" || cleanPath == "/dav/principals" {
		return true
	}

	if strings.HasPrefix(cleanPath, "/dav/calendars/") {
		trimmed := strings.Trim(strings.TrimPrefix(cleanPath, "/dav/calendars/"), "/")
		if trimmed == "" {
			return false
		}
		segment := strings.Split(trimmed, "/")[0]
		calID, ok, err := h.resolveCalendarID(ctx, user, segment)
		if err == nil && ok && h.store.Calendars != nil {
			if cal, err := h.store.Calendars.GetByID(ctx, calID); err == nil && cal != nil {
				return cal.UserID == user.ID
			}
		}
		return false
	}

	if strings.HasPrefix(cleanPath, "/dav/addressbooks/") {
		trimmed := strings.Trim(strings.TrimPrefix(cleanPath, "/dav/addressbooks/"), "/")
		if trimmed == "" {
			return false
		}
		segment := strings.Split(trimmed, "/")[0]
		bookID, ok, err := h.resolveAddressBookID(ctx, user, segment)
		if err == nil && ok && h.store.AddressBooks != nil {
			if book, err := h.store.AddressBooks.GetByID(ctx, bookID); err == nil && book != nil {
				return book.UserID == user.ID
			}
		}
		return false
	}

	if strings.HasPrefix(cleanPath, "/dav/principals/") {
		trimmed := strings.Trim(strings.TrimPrefix(cleanPath, "/dav/principals/"), "/")
		if trimmed == "" {
			return false
		}
		segment := strings.Split(trimmed, "/")[0]
		principalID, err := strconv.ParseInt(segment, 10, 64)
		if err != nil {
			return false
		}
		return principalID == user.ID
	}

	return false
}

// PROPFIND ACL property helpers

func defaultSupportedPrivilegeSet() *supportedPrivilegeSetProp {
	return &supportedPrivilegeSetProp{
		SupportedPrivileges: []supportedPrivilege{
			{
				Privilege:   supportedPrivilegeType{All: &struct{}{}},
				Description: aclPrivilegeDescription("All privileges"),
				SubPrivs: []supportedPrivilege{
					{
						Privilege:   supportedPrivilegeType{Read: &struct{}{}},
						Description: aclPrivilegeDescription("Read access"),
						SubPrivs: []supportedPrivilege{
							{Privilege: supportedPrivilegeType{ReadFreeBusy: &struct{}{}}, Description: aclPrivilegeDescription("Read free-busy data")},
						},
					},
					{
						Privilege:   supportedPrivilegeType{Write: &struct{}{}},
						Description: aclPrivilegeDescription("Write access"),
						SubPrivs: []supportedPrivilege{
							{Privilege: supportedPrivilegeType{WriteContent: &struct{}{}}, Description: aclPrivilegeDescription("Write resource content")},
							{Privilege: supportedPrivilegeType{WriteProperties: &struct{}{}}, Description: aclPrivilegeDescription("Write properties")},
							{Privilege: supportedPrivilegeType{Bind: &struct{}{}}, Description: aclPrivilegeDescription("Create child resources")},
							{Privilege: supportedPrivilegeType{Unbind: &struct{}{}}, Description: aclPrivilegeDescription("Delete child resources")},
						},
					},
					{Privilege: supportedPrivilegeType{ReadACL: &struct{}{}}, Description: aclPrivilegeDescription("Read ACL")},
					{Privilege: supportedPrivilegeType{ReadCurrentUserPrivilegeSet: &struct{}{}}, Description: aclPrivilegeDescription("Read current user privilege set")},
					{
						Privilege:   supportedPrivilegeType{WriteACL: &struct{}{}},
						Description: aclPrivilegeDescription("Write ACL"),
					},
					{Privilege: supportedPrivilegeType{Unlock: &struct{}{}}, Description: aclPrivilegeDescription("Remove write locks")},
				},
			},
		},
	}
}

func aclPrivilegeDescription(value string) langString {
	return langString{Value: value, Lang: "en"}
}

func buildACLPropFromEntries(entries []store.ACLEntry) *aclProp {
	if len(entries) == 0 {
		return &aclProp{}
	}

	var aces []aceResp
	currentPosition := -1
	var currentPrincipal string
	var currentGrant bool
	for _, entry := range entries {
		privilege := privilegeNameToResp(entry.Privilege)
		normalizedPrincipal := acl.NormalizePrincipalHref(entry.PrincipalHref)
		if len(aces) == 0 || entry.Position != currentPosition || normalizedPrincipal != currentPrincipal || entry.IsGrant != currentGrant {
			ace := aceResp{Principal: principalRespFromStored(entry.PrincipalHref)}
			if entry.IsGrant {
				ace.Grant = &aceGrantResp{}
			} else {
				ace.Deny = &aceDenyResp{}
			}
			aces = append(aces, ace)
			currentPrincipal = normalizedPrincipal
			currentGrant = entry.IsGrant
			currentPosition = entry.Position
		}
		last := &aces[len(aces)-1]
		if entry.IsGrant {
			last.Grant.Privileges = append(last.Grant.Privileges, privilege)
		} else {
			last.Deny.Privileges = append(last.Deny.Privileges, privilege)
		}
	}

	return &aclProp{ACE: aces}
}

func protectedOwnerACE(ownerHref string) aceResp {
	return aceResp{
		Principal: acePrincipalResp{Href: ownerHref},
		Grant:     &aceGrantResp{Privileges: []acePrivilegeResp{{All: &struct{}{}}}},
		Protected: &struct{}{},
	}
}

func principalRespFromStored(principal string) acePrincipalResp {
	switch acl.NormalizePrincipalHref(principal) {
	case acl.PrincipalAll:
		return acePrincipalResp{All: &struct{}{}}
	case acl.PrincipalAuthenticated:
		return acePrincipalResp{Authenticated: &struct{}{}}
	case acl.PrincipalUnauthenticated:
		return acePrincipalResp{Unauthenticated: &struct{}{}}
	case acl.PrincipalPropertyOwner:
		return acePrincipalResp{Property: &acePrincipalPropertyResp{Owner: &struct{}{}}}
	case acl.PrincipalPropertyGroup:
		return acePrincipalResp{Property: &acePrincipalPropertyResp{Group: &struct{}{}}}
	case acl.PrincipalSelf:
		return acePrincipalResp{Self: &struct{}{}}
	default:
		return acePrincipalResp{Href: principal}
	}
}

func privilegeNameToResp(name string) acePrivilegeResp {
	var p acePrivilegeResp
	switch name {
	case "read":
		p.Read = &struct{}{}
	case "write":
		p.Write = &struct{}{}
	case "write-content":
		p.WriteContent = &struct{}{}
	case "write-properties":
		p.WriteProperties = &struct{}{}
	case "read-acl":
		p.ReadACL = &struct{}{}
	case "read-current-user-privilege-set":
		p.ReadCurrentUserPrivilegeSet = &struct{}{}
	case "write-acl":
		p.WriteACL = &struct{}{}
	case "unlock":
		p.Unlock = &struct{}{}
	case "bind":
		p.Bind = &struct{}{}
	case "unbind":
		p.Unbind = &struct{}{}
	case "read-free-busy":
		p.ReadFreeBusy = &struct{}{}
	case "all":
		p.All = &struct{}{}
	}
	return p
}
