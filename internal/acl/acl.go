package acl

import (
	"fmt"
	"net/url"
	"path"
	"sort"
	"strconv"
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

// PrincipalHrefs lists the principal_href values a stored ACL entry that
// applies to user can hold, for a lookup by principal: the principals
// ApplicablePrincipals names, and the user's href also without its trailing
// slash, which NormalizePrincipalHref folds into it. Every writer stores a
// principal href as NormalizePrincipalHref leaves it, so these are all a
// decision that normalizes the entries it reads can match.
func PrincipalHrefs(user *store.User) []string {
	principals := []string{PrincipalAll}
	if user != nil {
		href := PrincipalHref(user.ID)
		principals = append(principals, PrincipalAuthenticated, href, strings.TrimSuffix(href, "/"))
	} else {
		principals = append(principals, PrincipalUnauthenticated)
	}
	return principals
}

// PrincipalUserID is the user a principal href names, with or without its
// trailing slash or in any other spelling NormalizePrincipalHref folds into
// PrincipalHref's, and whether it names one.
func PrincipalUserID(href string) (int64, bool) {
	normalized := NormalizePrincipalHref(href)
	if !isCanonicalPrincipalHref(normalized) {
		return 0, false
	}
	id, err := strconv.ParseInt(strings.TrimSuffix(strings.TrimPrefix(normalized, "/dav/principals/"), "/"), 10, 64)
	if err != nil {
		return 0, false
	}
	return id, true
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

// CalendarPrivilegeNames is every privilege a calendar access decision can
// resolve, in DAV:acl document order. Holding any of them on a calendar or
// event is what DELETE's any-privilege check asks of a member.
var CalendarPrivilegeNames = []string{"read", "read-free-busy", "write", "write-content", "write-properties", "bind", "unbind"}

func PrivilegeMatches(granted, requested string) bool {
	if granted == requested || granted == "all" {
		return true
	}
	if granted == "read" && requested == "read-free-busy" {
		return true
	}
	return granted == "write" && (requested == "write-content" || requested == "write-properties" || requested == "bind" || requested == "unbind")
}

// DecisionForPrivilege decides privilege from one ACL: whether it is granted,
// and whether the entries decide it at all.
func DecisionForPrivilege(entries []store.ACLEntry, applicablePrincipals map[string]struct{}, privilege string) (bool, bool) {
	return DecisionThroughLayers(applicablePrincipals, privilege, entries)
}

// DecisionThroughLayers decides privilege from the ACLs of a resource and of
// the collections it inherits from, the resource's own first: the first layer
// whose entries decide a privilege decides it. DAV:write, which RFC 3744
// section 3.2 makes the aggregate of DAV:write-content, DAV:write-properties,
// DAV:bind and DAV:unbind, is decided part by part through the layers, so a
// part the member's ACL leaves undecided is decided by its collection's, as a
// request for that part alone would be. It is granted when every part is,
// refused when any part is refused, and decided unheld when only some parts
// are granted.
func DecisionThroughLayers(applicablePrincipals map[string]struct{}, privilege string, layers ...[]store.ACLEntry) (granted, decided bool) {
	if privilege != "write" {
		return decideThroughLayers(applicablePrincipals, privilege, layers)
	}
	grantedAll := true
	for _, part := range writeParts {
		partGranted, partDecided := decideThroughLayers(applicablePrincipals, part, layers)
		if partDecided && !partGranted {
			return false, true
		}
		decided = decided || partDecided
		grantedAll = grantedAll && partGranted
	}
	return decided && grantedAll, decided
}

// HeldThroughLayers reports whether privilege is held through layers, as
// current-user-privilege-set reports it: RFC 3744 section 3.12 makes an
// aggregate privilege equivalent to the set of the privileges it contains, so
// DAV:all is held when every privilege it contains is, and DAV:read when
// read-free-busy, which PrivilegeMatches has it contain, is held as well.
// DAV:write is decided part by part already. A method's own check decides the
// privilege it needs with DecisionThroughLayers, not this.
func HeldThroughLayers(applicablePrincipals map[string]struct{}, privilege string, layers ...[]store.ACLEntry) bool {
	parts := []string{privilege}
	switch privilege {
	case "all":
		parts = allParts
	case "read":
		parts = []string{"read", "read-free-busy"}
	}
	for _, part := range parts {
		if granted, _ := DecisionThroughLayers(applicablePrincipals, part, layers...); !granted {
			return false
		}
	}
	return true
}

// allParts are the privileges DAV:all contains that a decision can resolve,
// DAV:write among them standing for its own parts.
var allParts = []string{"read", "read-free-busy", "read-acl", "read-current-user-privilege-set", "write", "write-acl", "unlock"}

// writeParts are the privileges DAV:write aggregates (RFC 3744 section 3.2).
var writeParts = []string{"write-content", "write-properties", "bind", "unbind"}

func decideThroughLayers(applicablePrincipals map[string]struct{}, privilege string, layers [][]store.ACLEntry) (bool, bool) {
	for _, entries := range layers {
		if granted, decided := decidePrivilege(entries, applicablePrincipals, privilege); decided {
			return granted, true
		}
	}
	return false, false
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
