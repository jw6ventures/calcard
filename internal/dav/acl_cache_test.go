package dav

import (
	"context"
	"testing"

	"github.com/jw6ventures/calcard/internal/store"
)

func TestACLEntriesForResourceMemoizesWithinRequest(t *testing.T) {
	aclRepo := &fakeACLRepo{entries: []store.ACLEntry{
		{ResourcePath: "/dav/calendars/3", PrincipalHref: "/dav/principals/4/", IsGrant: true, Privilege: "read"},
	}}
	h := &Handler{store: &store.Store{ACLEntries: aclRepo}}

	// With a request-scoped cache, resolving the same path repeatedly (as the
	// per-privilege current-user-privilege-set computation does) hits the store
	// only on the first call.
	ctx := withACLEntryCache(context.Background())
	if _, err := h.aclEntriesForResource(ctx, "/dav/calendars/3"); err != nil {
		t.Fatalf("aclEntriesForResource() error = %v", err)
	}
	firstCalls := aclRepo.listByResourceCalls
	if firstCalls == 0 {
		t.Fatal("expected the first lookup to query the store")
	}
	for i := 0; i < 6; i++ {
		if _, err := h.aclEntriesForResource(ctx, "/dav/calendars/3"); err != nil {
			t.Fatalf("aclEntriesForResource() error = %v", err)
		}
	}
	if aclRepo.listByResourceCalls != firstCalls {
		t.Fatalf("cached lookups queried the store again: calls = %d, want %d", aclRepo.listByResourceCalls, firstCalls)
	}

	// Without a cache in the context (e.g. on ACL-mutating methods), every call
	// reads fresh.
	aclRepo.listByResourceCalls = 0
	for i := 0; i < 3; i++ {
		if _, err := h.aclEntriesForResource(context.Background(), "/dav/calendars/3"); err != nil {
			t.Fatalf("aclEntriesForResource() error = %v", err)
		}
	}
	if aclRepo.listByResourceCalls < 3 {
		t.Fatalf("uncached lookups should each hit the store: calls = %d, want >= 3", aclRepo.listByResourceCalls)
	}
}
