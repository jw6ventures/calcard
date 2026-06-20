package dav

import (
	"fmt"
	"testing"

	"github.com/jw6ventures/calcard/internal/acl"
	"github.com/jw6ventures/calcard/internal/store"
)

// benchACLEntries builds a representative ACL set: one grant for the user's own
// principal plus n unrelated entries, mimicking a shared resource with a long
// access list. The unrelated entries exercise the normalize + principal-match
// path that runs for every entry on every privilege check.
func benchACLEntries(userID int64, n int) []store.ACLEntry {
	entries := []store.ACLEntry{
		{ResourcePath: "/dav/calendars/1/", PrincipalHref: fmt.Sprintf("/dav/principals/%d/", userID), IsGrant: true, Privilege: "write"},
		{ResourcePath: "/dav/calendars/1/", PrincipalHref: "DAV:authenticated", IsGrant: true, Privilege: "read"},
	}
	for i := 0; i < n; i++ {
		entries = append(entries, store.ACLEntry{
			ResourcePath:  "/dav/calendars/1/",
			PrincipalHref: fmt.Sprintf("/dav/principals/%d/", int64(1000+i)),
			IsGrant:       true,
			Privilege:     "read",
		})
	}
	return entries
}

func BenchmarkApplicableACLPrincipals(b *testing.B) {
	user := &store.User{ID: 42}
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_ = acl.ApplicablePrincipals(user)
	}
}

func BenchmarkACLDecisionForPrivilege_Read(b *testing.B) {
	user := &store.User{ID: 42}
	principals := acl.ApplicablePrincipals(user)
	entries := benchACLEntries(user.ID, 16)
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_, _ = acl.DecisionForPrivilege(entries, principals, "read")
	}
}

// Write is the costliest decision: it fans out across the four sub-privileges,
// re-scanning every entry each time. This is the hot path the ACL performance
// work targeted, so it's the most informative thing to profile.
func BenchmarkACLDecisionForPrivilege_Write(b *testing.B) {
	user := &store.User{ID: 42}
	principals := acl.ApplicablePrincipals(user)
	for _, n := range []int{0, 16, 128, 348, 998, 9998} {
		entries := benchACLEntries(user.ID, n)
		b.Run(fmt.Sprintf("entries=%d", n+2), func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				_, _ = acl.DecisionForPrivilege(entries, principals, "write")
			}
		})
	}
}

func BenchmarkNormalizeACLPrincipalHref(b *testing.B) {
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_ = acl.NormalizePrincipalHref("/dav/principals/42")
	}
}
