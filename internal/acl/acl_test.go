package acl

import (
	"reflect"
	"testing"

	"github.com/jw6ventures/calcard/internal/store"
)

func TestPrincipalSetsUseCanonicalDAVPrincipals(t *testing.T) {
	user := &store.User{ID: 42}

	if got, want := PrincipalHref(user.ID), "/dav/principals/42/"; got != want {
		t.Fatalf("PrincipalHref() = %q, want %q", got, want)
	}

	if got, want := PrincipalHrefs(nil), []string{"DAV:all"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("PrincipalHrefs(nil) = %#v, want %#v", got, want)
	}
	if got, want := PrincipalHrefs(user), []string{"DAV:all", "DAV:authenticated", "/dav/principals/42/"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("PrincipalHrefs(user) = %#v, want %#v", got, want)
	}

	applicable := ApplicablePrincipals(user)
	for _, principal := range PrincipalHrefs(user) {
		if _, ok := applicable[principal]; !ok {
			t.Fatalf("ApplicablePrincipals() missing %q from PrincipalHrefs()", principal)
		}
	}
}

func TestNormalizePrincipalHrefCanonicalizesEquivalentPrincipalURLs(t *testing.T) {
	tests := []struct {
		name string
		raw  string
		want string
	}{
		{name: "empty", raw: "  ", want: ""},
		{name: "all sentinel", raw: "DAV:all", want: "DAV:all"},
		{name: "authenticated sentinel", raw: "DAV:authenticated", want: "DAV:authenticated"},
		{name: "adds trailing slash", raw: "/dav/principals/42", want: "/dav/principals/42/"},
		{name: "absolute url", raw: "https://example.test/dav/principals/42", want: "/dav/principals/42/"},
		{name: "cleaned path", raw: "/dav/users/../principals/42", want: "/dav/principals/42/"},
		{name: "relative principal path", raw: "dav/principals/42", want: "/dav/principals/42/"},
		{name: "non principal href unchanged", raw: "https://example.test/not-principals/42", want: "https://example.test/not-principals/42"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := NormalizePrincipalHref(tc.raw); got != tc.want {
				t.Fatalf("NormalizePrincipalHref(%q) = %q, want %q", tc.raw, got, tc.want)
			}
		})
	}
}

func TestDecisionForPrivilegeUsesSharedPrivilegeRules(t *testing.T) {
	user := &store.User{ID: 42}
	principals := ApplicablePrincipals(user)

	tests := []struct {
		name           string
		entries        []store.ACLEntry
		privilege      string
		wantGranted    bool
		wantApplicable bool
	}{
		{
			name: "read grant satisfies CalDAV read-free-busy",
			entries: []store.ACLEntry{
				{PrincipalHref: "/dav/principals/42/", IsGrant: true, Privilege: "read"},
			},
			privilege:      "read-free-busy",
			wantGranted:    true,
			wantApplicable: true,
		},
		{
			name: "specific deny wins over broader read grant",
			entries: []store.ACLEntry{
				{PrincipalHref: "/dav/principals/42/", IsGrant: true, Privilege: "read"},
				{PrincipalHref: "/dav/principals/42/", IsGrant: false, Privilege: "read-free-busy"},
			},
			privilege:      "read-free-busy",
			wantGranted:    false,
			wantApplicable: true,
		},
		{
			name: "write grant satisfies aggregate write",
			entries: []store.ACLEntry{
				{PrincipalHref: "/dav/principals/42/", IsGrant: true, Privilege: "write"},
			},
			privilege:      "write",
			wantGranted:    true,
			wantApplicable: true,
		},
		{
			name: "aggregate write requires every write component",
			entries: []store.ACLEntry{
				{PrincipalHref: "/dav/principals/42/", IsGrant: true, Privilege: "write-content"},
				{PrincipalHref: "/dav/principals/42/", IsGrant: true, Privilege: "write-properties"},
			},
			privilege:      "write",
			wantGranted:    false,
			wantApplicable: true,
		},
		{
			name: "canonicalized applicable principal",
			entries: []store.ACLEntry{
				{PrincipalHref: "https://example.test/dav/users/../principals/42", IsGrant: true, Privilege: "read"},
			},
			privilege:      "read",
			wantGranted:    true,
			wantApplicable: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			gotGranted, gotApplicable := DecisionForPrivilege(tc.entries, principals, tc.privilege)
			if gotGranted != tc.wantGranted || gotApplicable != tc.wantApplicable {
				t.Fatalf("DecisionForPrivilege() = (%t, %t), want (%t, %t)", gotGranted, gotApplicable, tc.wantGranted, tc.wantApplicable)
			}
		})
	}
}

func TestHasApplicablePrincipalUsesCanonicalNormalization(t *testing.T) {
	entries := []store.ACLEntry{
		{PrincipalHref: "https://example.test/dav/users/../principals/42", IsGrant: true, Privilege: "read"},
	}
	if !HasApplicablePrincipal(entries, ApplicablePrincipals(&store.User{ID: 42})) {
		t.Fatal("expected equivalent principal URL to apply to user")
	}
}
