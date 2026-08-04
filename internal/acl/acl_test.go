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

	if got, want := PrincipalHrefs(nil), []string{"DAV:all", "DAV:unauthenticated"}; !reflect.DeepEqual(got, want) {
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

func TestApplicablePrincipalsForResolvesSelfAndOwnerForms(t *testing.T) {
	user := &store.User{ID: 42}
	other := PrincipalHref(7)

	tests := []struct {
		name     string
		user     *store.User
		resource ResourcePrincipals
		want     []string
		absent   []string
	}{
		{
			name:   "no resource forms without a resource",
			user:   user,
			want:   []string{PrincipalAll, PrincipalAuthenticated, PrincipalHref(42)},
			absent: []string{PrincipalSelf, PrincipalPropertyOwner, PrincipalPropertyGroup},
		},
		{
			name:     "owner resolves when the user owns the resource",
			user:     user,
			resource: ResourcePrincipals{OwnerHref: PrincipalHref(42)},
			want:     []string{PrincipalPropertyOwner},
			absent:   []string{PrincipalSelf, PrincipalPropertyGroup},
		},
		{
			name:     "owner does not resolve for another principal",
			user:     user,
			resource: ResourcePrincipals{OwnerHref: other},
			absent:   []string{PrincipalSelf, PrincipalPropertyOwner, PrincipalPropertyGroup},
		},
		{
			name:     "self resolves on the user's own principal resource",
			user:     user,
			resource: ResourcePrincipals{SelfHref: PrincipalHref(42), OwnerHref: PrincipalHref(42)},
			want:     []string{PrincipalSelf, PrincipalPropertyOwner},
			absent:   []string{PrincipalPropertyGroup},
		},
		{
			name:     "self does not resolve on another principal's resource",
			user:     user,
			resource: ResourcePrincipals{SelfHref: other, OwnerHref: other},
			absent:   []string{PrincipalSelf, PrincipalPropertyOwner, PrincipalPropertyGroup},
		},
		{
			name:     "unauthenticated requests resolve no resource form",
			user:     nil,
			resource: ResourcePrincipals{SelfHref: PrincipalHref(42), OwnerHref: PrincipalHref(42)},
			want:     []string{PrincipalAll, PrincipalUnauthenticated},
			absent:   []string{PrincipalSelf, PrincipalPropertyOwner, PrincipalPropertyGroup},
		},
		{
			name:     "unnormalized owner href still resolves",
			user:     user,
			resource: ResourcePrincipals{OwnerHref: "https://example.test/dav/principals/42"},
			want:     []string{PrincipalPropertyOwner},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := ApplicablePrincipalsFor(tc.user, tc.resource)
			for _, principal := range tc.want {
				if _, ok := got[principal]; !ok {
					t.Errorf("ApplicablePrincipalsFor() missing %q: %#v", principal, got)
				}
			}
			for _, principal := range tc.absent {
				if _, ok := got[principal]; ok {
					t.Errorf("ApplicablePrincipalsFor() unexpectedly applied %q: %#v", principal, got)
				}
			}
		})
	}
}

func TestNeedsResourcePrincipalsDetectsResourceDependentForms(t *testing.T) {
	tests := []struct {
		name    string
		entries []store.ACLEntry
		want    bool
	}{
		{name: "no entries", entries: nil, want: false},
		{
			name:    "href and sentinel principals only",
			entries: []store.ACLEntry{{PrincipalHref: PrincipalAll}, {PrincipalHref: PrincipalHref(1)}},
			want:    false,
		},
		{name: "self", entries: []store.ACLEntry{{PrincipalHref: PrincipalSelf}}, want: true},
		{name: "property owner", entries: []store.ACLEntry{{PrincipalHref: PrincipalPropertyOwner}}, want: true},
		{name: "property group", entries: []store.ACLEntry{{PrincipalHref: PrincipalPropertyGroup}}, want: true},
		{
			name:    "mixed",
			entries: []store.ACLEntry{{PrincipalHref: PrincipalAll}, {PrincipalHref: PrincipalSelf}},
			want:    true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := NeedsResourcePrincipals(tc.entries); got != tc.want {
				t.Fatalf("NeedsResourcePrincipals() = %t, want %t", got, tc.want)
			}
		})
	}
}

// A DAV:property principal naming a property the resource does not define
// matches nothing (RFC 3744 section 5.5.1). CalCard defines no DAV:group, so a
// group ACE must never decide a privilege even for the resource's own owner.
func TestPropertyGroupPrincipalNeverDecidesAPrivilege(t *testing.T) {
	user := &store.User{ID: 42}
	principals := ApplicablePrincipalsFor(user, ResourcePrincipals{
		OwnerHref: PrincipalHref(42),
		SelfHref:  PrincipalHref(42),
	})
	entries := []store.ACLEntry{{PrincipalHref: PrincipalPropertyGroup, IsGrant: true, Privilege: "read"}}

	granted, applicable := DecisionForPrivilege(entries, principals, "read")
	if granted || applicable {
		t.Fatalf("DecisionForPrivilege() = (%t, %t), want (false, false)", granted, applicable)
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
		{name: "cleans traversal", raw: "/dav/principals/42/../7", want: "/dav/principals/7/"},
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
			name: "earlier aggregate grant completes evaluation before later deny",
			entries: []store.ACLEntry{
				{PrincipalHref: "/dav/principals/42/", IsGrant: true, Privilege: "read"},
				{PrincipalHref: "/dav/principals/42/", IsGrant: false, Privilege: "read-free-busy"},
			},
			privilege:      "read-free-busy",
			wantGranted:    true,
			wantApplicable: true,
		},
		{
			name: "earlier specific deny stops evaluation before later grant",
			entries: []store.ACLEntry{
				{PrincipalHref: "/dav/principals/42/", IsGrant: false, Privilege: "read-free-busy"},
				{PrincipalHref: "/dav/principals/42/", IsGrant: true, Privilege: "read"},
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
