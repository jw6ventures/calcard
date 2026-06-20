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

	normalized := normalizeHref(raw)
	if strings.HasPrefix(normalized, "/dav/principals/") {
		if !strings.HasSuffix(normalized, "/") {
			normalized += "/"
		}
		return normalized
	}
	return raw
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

type applicableACE struct {
	privilege string
	isGrant   bool
}

func filterApplicableACEs(entries []store.ACLEntry, applicablePrincipals map[string]struct{}) []applicableACE {
	aces := make([]applicableACE, 0, len(entries))
	for _, entry := range entries {
		if _, ok := applicablePrincipals[NormalizePrincipalHref(entry.PrincipalHref)]; !ok {
			continue
		}
		aces = append(aces, applicableACE{privilege: entry.Privilege, isGrant: entry.IsGrant})
	}
	return aces
}

func DecisionForPrivilege(entries []store.ACLEntry, applicablePrincipals map[string]struct{}, privilege string) (bool, bool) {
	aces := filterApplicableACEs(entries, applicablePrincipals)
	if privilege == "write" {
		return aggregateWriteDecision(aces)
	}
	return decidePrivilege(aces, privilege)
}

func HasApplicablePrincipal(entries []store.ACLEntry, applicablePrincipals map[string]struct{}) bool {
	for _, entry := range entries {
		if _, ok := applicablePrincipals[NormalizePrincipalHref(entry.PrincipalHref)]; ok {
			return true
		}
	}
	return false
}

func decidePrivilege(aces []applicableACE, privilege string) (bool, bool) {
	hasGrant := false
	for _, ace := range aces {
		if !PrivilegeMatches(ace.privilege, privilege) {
			continue
		}
		if !ace.isGrant {
			return false, true
		}
		hasGrant = true
	}
	if hasGrant {
		return true, true
	}
	return false, false
}

func aggregateWriteDecision(aces []applicableACE) (bool, bool) {
	applicable := false
	for _, privilege := range []string{"write-content", "write-properties", "bind", "unbind"} {
		granted, decided := decidePrivilege(aces, privilege)
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
