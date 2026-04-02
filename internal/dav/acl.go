package dav

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"path"
	"strconv"
	"strings"

	"github.com/jw6ventures/calcard/internal/auth"
	"github.com/jw6ventures/calcard/internal/store"
)

var allPrivileges = []string{"read", "write", "write-content", "write-properties", "read-acl", "write-acl", "bind", "unbind"}

func (h *Handler) Acl(w http.ResponseWriter, r *http.Request) {
	user, ok := auth.UserFromContext(r.Context())
	if !ok {
		http.Error(w, "missing user", http.StatusUnauthorized)
		return
	}

	cleanPath := path.Clean(r.URL.Path)
	if !h.requireLock(w, r, cleanPath, "resource is locked") {
		return
	}
	canonicalPath, err := h.canonicalDAVPath(r.Context(), user, cleanPath)
	if err != nil {
		if err == store.ErrNotFound {
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

	// Verify user has write-acl privilege
	allowed, err := h.checkACLPrivilege(r.Context(), user, canonicalPath, "write-acl")
	if err != nil {
		http.Error(w, "failed to evaluate ACL", http.StatusInternalServerError)
		return
	}
	if !allowed {
		http.Error(w, "forbidden", http.StatusForbidden)
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

	var entries []store.ACLEntry
	for _, a := range req.ACE {
		if err := validateACE(a); err != nil {
			http.Error(w, "invalid ACL request", http.StatusBadRequest)
			return
		}
		principalHref := normalizeACLPrincipalHref(a.Principal.Href)
		if principalHref == "" {
			if a.Principal.Self != nil {
				principalHref = h.principalURL(user)
			} else if a.Principal.All != nil || a.Principal.Authenticated != nil {
				// Use special sentinel values
				if a.Principal.All != nil {
					principalHref = "DAV:all"
				} else {
					principalHref = "DAV:authenticated"
				}
			} else {
				http.Error(w, "invalid principal in ACE", http.StatusBadRequest)
				return
			}
		}

		if a.Grant != nil {
			for _, priv := range a.Grant.Privileges {
				if err := validateACEPrivilege(priv); err != nil {
					http.Error(w, "invalid privilege in ACE", http.StatusBadRequest)
					return
				}
				for _, name := range extractACEPrivilegeNames(priv) {
					entries = append(entries, store.ACLEntry{
						ResourcePath:  canonicalPath,
						PrincipalHref: principalHref,
						IsGrant:       true,
						Privilege:     name,
					})
				}
			}
		}
		if a.Deny != nil {
			for _, priv := range a.Deny.Privileges {
				if err := validateACEPrivilege(priv); err != nil {
					http.Error(w, "invalid privilege in ACE", http.StatusBadRequest)
					return
				}
				for _, name := range extractACEPrivilegeNames(priv) {
					entries = append(entries, store.ACLEntry{
						ResourcePath:  canonicalPath,
						PrincipalHref: principalHref,
						IsGrant:       false,
						Privilege:     name,
					})
				}
			}
		}
	}

	if err := h.store.ACLEntries.SetACL(r.Context(), canonicalPath, entries); err != nil {
		http.Error(w, "failed to set ACL", http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusOK)
}

func validateACE(a ace) error {
	if (a.Grant == nil) == (a.Deny == nil) {
		return fmt.Errorf("expected exactly one of grant or deny")
	}
	if a.Grant != nil && len(a.Grant.Privileges) == 0 {
		return fmt.Errorf("grant must contain at least one privilege")
	}
	if a.Deny != nil && len(a.Deny.Privileges) == 0 {
		return fmt.Errorf("deny must contain at least one privilege")
	}
	return nil
}

func validateACEPrivilege(priv acePrivilege) error {
	count := 0
	for _, present := range []bool{
		priv.Read != nil,
		priv.Write != nil,
		priv.WriteContent != nil,
		priv.WriteProperties != nil,
		priv.ReadACL != nil,
		priv.WriteACL != nil,
		priv.Bind != nil,
		priv.Unbind != nil,
		priv.All != nil,
	} {
		if present {
			count++
		}
	}
	if len(priv.Unknown) > 0 {
		return fmt.Errorf("unsupported privilege %q", xmlNameString(priv.Unknown[0].XMLName))
	}
	if count != 1 {
		return fmt.Errorf("expected exactly one privilege element")
	}
	return nil
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
	if priv.WriteACL != nil {
		names = append(names, "write-acl")
	}
	if priv.Bind != nil {
		names = append(names, "bind")
	}
	if priv.Unbind != nil {
		names = append(names, "unbind")
	}
	if priv.All != nil {
		names = append(names, "all")
	}
	return names
}

// checkACLPrivilege verifies a user has a specific privilege on a resource.
func (h *Handler) checkACLPrivilege(ctx context.Context, user *store.User, resourcePath, privilege string) (bool, error) {
	if h.store.ACLEntries == nil {
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

	if granted, decided, err := h.aclDecision(ctx, user, resourcePath, privilege); err != nil {
		return false, err
	} else if decided {
		return granted, nil
	}

	// Fall back to existing calendar share model
	return h.hasCalendarSharePrivilege(ctx, user, resourcePath, privilege), nil
}

func (h *Handler) aclDecision(ctx context.Context, user *store.User, resourcePath, privilege string) (bool, bool, error) {
	if h == nil || h.store == nil || h.store.ACLEntries == nil {
		return false, false, nil
	}

	entries, err := h.aclEntriesForResource(ctx, resourcePath)
	if err != nil || len(entries) == 0 {
		return false, false, err
	}

	applicablePrincipals := map[string]struct{}{"DAV:all": {}}
	if user != nil {
		principalHref := fmt.Sprintf("/dav/principals/%d/", user.ID)
		applicablePrincipals[principalHref] = struct{}{}
		applicablePrincipals["DAV:authenticated"] = struct{}{}
	}

	// RFC 3744: deny ACEs MUST be evaluated before grant ACEs.
	// Two-pass: first check for any applicable deny, then for any grant.
	hasGrant := false
	for _, entry := range entries {
		if _, ok := applicablePrincipals[normalizeACLPrincipalHref(entry.PrincipalHref)]; !ok {
			continue
		}
		if !aclPrivilegeMatches(entry.Privilege, privilege) {
			continue
		}
		if !entry.IsGrant {
			return false, true, nil
		}
		hasGrant = true
	}
	if hasGrant {
		return true, true, nil
	}
	return false, false, nil
}

func (h *Handler) aclEntriesForResource(ctx context.Context, resourcePath string) ([]store.ACLEntry, error) {
	resourcePath = normalizeDAVResourceIdentity(resourcePath)
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
	return result, nil
}

func normalizeACLPrincipalHref(raw string) string {
	raw = strings.TrimSpace(raw)
	switch raw {
	case "", "DAV:all", "DAV:authenticated":
		return raw
	}

	normalized := normalizeDAVHref(raw)
	if strings.HasPrefix(normalized, "/dav/principals/") {
		if !strings.HasSuffix(normalized, "/") {
			normalized += "/"
		}
		return normalized
	}
	return raw
}

func aclPrivilegeMatches(granted, requested string) bool {
	if granted == requested || granted == "all" {
		return true
	}
	return granted == "write" && (requested == "write-content" || requested == "write-properties" || requested == "bind" || requested == "unbind")
}

func (h *Handler) isResourceOwner(ctx context.Context, user *store.User, resourcePath string) bool {
	cleanPath := path.Clean(resourcePath)

	if strings.HasPrefix(cleanPath, "/dav/calendars/") {
		trimmed := strings.Trim(strings.TrimPrefix(cleanPath, "/dav/calendars/"), "/")
		if trimmed == "" {
			return false
		}
		segment := strings.Split(trimmed, "/")[0]
		calID, ok, err := h.resolveCalendarID(ctx, user, segment)
		if err == nil && ok {
			if cal, err := h.store.Calendars.GetByID(ctx, calID); err == nil {
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
		if err == nil && ok {
			if book, err := h.store.AddressBooks.GetByID(ctx, bookID); err == nil {
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

func (h *Handler) hasCalendarSharePrivilege(ctx context.Context, user *store.User, resourcePath, privilege string) bool {
	cleanPath := path.Clean(resourcePath)
	if !strings.HasPrefix(cleanPath, "/dav/calendars/") {
		return false
	}

	calID, _, matched, err := h.parseCalendarResourcePath(ctx, user, cleanPath)
	if err != nil || !matched {
		return false
	}

	cal, err := h.loadCalendar(ctx, user, calID)
	if err != nil {
		return false
	}

	switch privilege {
	case "read", "read-acl":
		return true // Shared calendars always grant read
	case "write", "write-content", "write-properties", "bind", "unbind":
		return cal.Editor
	case "write-acl":
		return !cal.Shared // Only owner can modify ACL
	}
	return false
}

// PROPFIND ACL property helpers

func defaultSupportedPrivilegeSet() *supportedPrivilegeSetProp {
	return &supportedPrivilegeSetProp{
		SupportedPrivileges: []supportedPrivilege{
			{
				Privilege:   supportedPrivilegeType{All: &struct{}{}},
				Description: "All privileges",
				SubPrivs: []supportedPrivilege{
					{
						Privilege:   supportedPrivilegeType{Read: &struct{}{}},
						Description: "Read access",
					},
					{
						Privilege:   supportedPrivilegeType{Write: &struct{}{}},
						Description: "Write access",
						SubPrivs: []supportedPrivilege{
							{Privilege: supportedPrivilegeType{WriteContent: &struct{}{}}, Description: "Write resource content"},
							{Privilege: supportedPrivilegeType{WriteProperties: &struct{}{}}, Description: "Write properties"},
							{Privilege: supportedPrivilegeType{Bind: &struct{}{}}, Description: "Create child resources"},
							{Privilege: supportedPrivilegeType{Unbind: &struct{}{}}, Description: "Delete child resources"},
						},
					},
					{
						Privilege:   supportedPrivilegeType{ReadACL: &struct{}{}},
						Description: "Read ACL",
					},
					{
						Privilege:   supportedPrivilegeType{WriteACL: &struct{}{}},
						Description: "Write ACL",
					},
				},
			},
		},
	}
}

func buildACLPropFromEntries(entries []store.ACLEntry) *aclProp {
	if len(entries) == 0 {
		return &aclProp{}
	}

	var aces []aceResp
	var currentPrincipal string
	var currentGrant bool
	for _, entry := range entries {
		privilege := privilegeNameToResp(entry.Privilege)
		normalizedPrincipal := normalizeACLPrincipalHref(entry.PrincipalHref)
		if len(aces) == 0 || normalizedPrincipal != currentPrincipal || entry.IsGrant != currentGrant {
			ace := aceResp{Principal: principalRespFromStored(entry.PrincipalHref)}
			if entry.IsGrant {
				ace.Grant = &aceGrantResp{}
			} else {
				ace.Deny = &aceDenyResp{}
			}
			aces = append(aces, ace)
			currentPrincipal = normalizedPrincipal
			currentGrant = entry.IsGrant
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

func principalRespFromStored(principal string) acePrincipalResp {
	switch normalizeACLPrincipalHref(principal) {
	case "DAV:all":
		return acePrincipalResp{All: &struct{}{}}
	case "DAV:authenticated":
		return acePrincipalResp{Authenticated: &struct{}{}}
	case "DAV:self":
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
	case "write-acl":
		p.WriteACL = &struct{}{}
	case "bind":
		p.Bind = &struct{}{}
	case "unbind":
		p.Unbind = &struct{}{}
	case "all":
		p.All = &struct{}{}
	}
	return p
}
