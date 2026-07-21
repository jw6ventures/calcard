package acl

import (
	"fmt"
	"net/url"
	"path"
	"strings"

	"github.com/jw6ventures/calcard/internal/store"
)

func PrincipalHref(userID int64) string {
	return fmt.Sprintf("/dav/principals/%d/", userID)
}

func PrincipalHrefs(user *store.User) []string {
	principals := []string{"DAV:all"}
	if user != nil {
		principals = append(principals, "DAV:authenticated", PrincipalHref(user.ID))
	}
	return principals
}

func ApplicablePrincipals(user *store.User) map[string]struct{} {
	principals := map[string]struct{}{"DAV:all": {}}
	if user != nil {
		principals[PrincipalHref(user.ID)] = struct{}{}
		principals["DAV:authenticated"] = struct{}{}
	}
	return principals
}

func NormalizePrincipalHref(raw string) string {
	raw = strings.TrimSpace(raw)
	switch raw {
	case "", "DAV:all", "DAV:authenticated":
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
	// CalDAV read grants include free-busy visibility; non-calendar callers do
	// not request read-free-busy.
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
	hasGrant := false
	for _, entry := range entries {
		if _, ok := applicablePrincipals[NormalizePrincipalHref(entry.PrincipalHref)]; !ok {
			continue
		}
		if !PrivilegeMatches(entry.Privilege, privilege) {
			continue
		}
		if !entry.IsGrant {
			return false, true
		}
		hasGrant = true
	}
	if hasGrant {
		return true, true
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
