package acl

import (
	"fmt"
	"net/url"
	"path"
	"sort"
	"strings"

	"github.com/jw6ventures/calcard/internal/store"
)

const (
	PrincipalAll             = "DAV:all"
	PrincipalAuthenticated   = "DAV:authenticated"
	PrincipalUnauthenticated = "DAV:unauthenticated"
	PrincipalSelf            = "DAV:self"
	PrincipalPropertyOwner   = "DAV:property:owner"
	PrincipalPropertyGroup   = "DAV:property:group"
)

func PrincipalHref(userID int64) string {
	return fmt.Sprintf("/dav/principals/%d/", userID)
}

func PrincipalHrefs(user *store.User) []string {
	principals := []string{PrincipalAll}
	if user != nil {
		principals = append(principals, PrincipalAuthenticated, PrincipalHref(user.ID))
	} else {
		principals = append(principals, PrincipalUnauthenticated)
	}
	return principals
}

func ApplicablePrincipals(user *store.User) map[string]struct{} {
	principals := map[string]struct{}{PrincipalAll: {}}
	if user != nil {
		principals[PrincipalHref(user.ID)] = struct{}{}
		principals[PrincipalAuthenticated] = struct{}{}
	} else {
		principals[PrincipalUnauthenticated] = struct{}{}
	}
	return principals
}

// ResourcePrincipals carries the facts about the resource under evaluation that
// RFC 3744 section 5.5.1 resolves the DAV:self and DAV:property principal forms
// against. Those two name a principal indirectly, through the resource rather
// than through the ACE, so a decision over them cannot be made from the
// requesting user alone.
type ResourcePrincipals struct {
	// OwnerHref is the resource's DAV:owner value, empty when it has none.
	OwnerHref string
	// SelfHref is the principal a principal resource identifies. It is empty
	// for every resource that is not itself a principal.
	SelfHref string
}

// ApplicablePrincipalsFor extends ApplicablePrincipals with the principal forms
// that depend on the resource being accessed.
//
// DAV:property:group never becomes applicable: CalCard defines no DAV:group
// property, and section 5.5.1 makes an ACE naming a property the resource does
// not define match nothing. Storing such an ACE is therefore harmless, and
// leaving it unmatched is the specified behavior rather than an omission.
func ApplicablePrincipalsFor(user *store.User, resource ResourcePrincipals) map[string]struct{} {
	principals := ApplicablePrincipals(user)
	if user == nil {
		return principals
	}
	current := NormalizePrincipalHref(PrincipalHref(user.ID))
	if resource.OwnerHref != "" && NormalizePrincipalHref(resource.OwnerHref) == current {
		principals[PrincipalPropertyOwner] = struct{}{}
	}
	if resource.SelfHref != "" && NormalizePrincipalHref(resource.SelfHref) == current {
		principals[PrincipalSelf] = struct{}{}
	}
	return principals
}

// NeedsResourcePrincipals reports whether any entry names a principal form that
// only ApplicablePrincipalsFor can resolve. Callers use it to skip resolving a
// resource's owner — which generally costs a query — for the ordinary ACL that
// names none.
func NeedsResourcePrincipals(entries []store.ACLEntry) bool {
	for _, entry := range entries {
		switch NormalizePrincipalHref(entry.PrincipalHref) {
		case PrincipalSelf, PrincipalPropertyOwner, PrincipalPropertyGroup:
			return true
		}
	}
	return false
}

func SortEntries(entries []store.ACLEntry) {
	sort.SliceStable(entries, func(i, j int) bool {
		if entries[i].Position != entries[j].Position {
			return entries[i].Position < entries[j].Position
		}
		return entries[i].ID < entries[j].ID
	})
}

func NormalizePrincipalHref(raw string) string {
	raw = strings.TrimSpace(raw)
	switch raw {
	case "", PrincipalAll, PrincipalAuthenticated, PrincipalUnauthenticated, PrincipalSelf, PrincipalPropertyOwner, PrincipalPropertyGroup:
		return raw
	}
	if isCanonicalPrincipalHref(raw) {
		if !strings.HasSuffix(raw, "/") {
			return raw + "/"
		}
		return raw
	}

	normalized := normalizeHref(raw)
	if strings.HasPrefix(normalized, "/dav/principals/") {
		if !strings.HasSuffix(normalized, "/") {
			normalized += "/"
		}
		return normalized
	}
	return raw
}

func isCanonicalPrincipalHref(raw string) bool {
	const prefix = "/dav/principals/"
	if !strings.HasPrefix(raw, prefix) {
		return false
	}
	id := strings.TrimSuffix(strings.TrimPrefix(raw, prefix), "/")
	if id == "" || strings.ContainsRune(id, '/') {
		return false
	}
	for _, r := range id {
		if r < '0' || r > '9' {
			return false
		}
	}
	return true
}

func PrivilegeMatches(granted, requested string) bool {
	if granted == requested || granted == "all" {
		return true
	}
	if granted == "read" && requested == "read-free-busy" {
		return true
	}
	return granted == "write" && (requested == "write-content" || requested == "write-properties" || requested == "bind" || requested == "unbind")
}

func DecisionForPrivilege(entries []store.ACLEntry, applicablePrincipals map[string]struct{}, privilege string) (bool, bool) {
	if privilege == "write" {
		return aggregateWriteDecision(entries, applicablePrincipals)
	}
	return decidePrivilege(entries, applicablePrincipals, privilege)
}

func HasApplicablePrincipal(entries []store.ACLEntry, applicablePrincipals map[string]struct{}) bool {
	for _, entry := range entries {
		if _, ok := applicablePrincipals[NormalizePrincipalHref(entry.PrincipalHref)]; ok {
			return true
		}
	}
	return false
}

func decidePrivilege(entries []store.ACLEntry, applicablePrincipals map[string]struct{}, privilege string) (bool, bool) {
	for _, entry := range entries {
		if _, ok := applicablePrincipals[NormalizePrincipalHref(entry.PrincipalHref)]; !ok {
			continue
		}
		if !PrivilegeMatches(entry.Privilege, privilege) {
			continue
		}
		return entry.IsGrant, true
	}
	return false, false
}

func aggregateWriteDecision(entries []store.ACLEntry, applicablePrincipals map[string]struct{}) (bool, bool) {
	applicable := false
	for _, privilege := range []string{"write-content", "write-properties", "bind", "unbind"} {
		granted, decided := decidePrivilege(entries, applicablePrincipals, privilege)
		if decided {
			applicable = true
		}
		if !granted {
			return false, applicable
		}
	}
	return applicable, applicable
}

func normalizeHref(raw string) string {
	trimmed := strings.TrimSpace(raw)
	if trimmed == "" {
		return ""
	}
	if u, err := url.Parse(trimmed); err == nil {
		if u.Path != "" {
			trimmed = u.Path
		}
	}
	cleaned := path.Clean(trimmed)
	if cleaned == "." {
		cleaned = "/"
	}
	if !strings.HasPrefix(cleaned, "/") {
		cleaned = "/" + strings.TrimPrefix(cleaned, "/")
	}
	return cleaned
}
