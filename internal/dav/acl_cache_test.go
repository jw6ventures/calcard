package dav

import (
	"context"
	"net/http/httptest"
	"testing"

	"github.com/jw6ventures/calcard/internal/store"
)

func TestACLEntriesForResourceMemoizesWithinRequest(t *testing.T) {
	aclRepo := &fakeACLRepo{entries: []store.ACLEntry{
		{ResourcePath: "/dav/calendars/3", PrincipalHref: "/dav/principals/4/", IsGrant: true, Privilege: "read"},
	}}
	h := &DavServer{store: &store.Store{ACLEntries: aclRepo}}

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

	// Without a cache in the context (e.g. direct handler calls in tests),
	// every call reads fresh.
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

func TestEnsureRequestCachesInstallsOnceAndInvalidates(t *testing.T) {
	req := httptest.NewRequest("PUT", "/dav/calendars/1/e.ics", nil)

	cached := ensureRequestCaches(req)
	if aclEntryCacheFromContext(cached.Context()) == nil {
		t.Fatal("ensureRequestCaches must install the ACL entry cache")
	}
	if davPathMemoFromContext(cached.Context()) == nil {
		t.Fatal("ensureRequestCaches must install the canonical-path memo")
	}
	// A second call (ServeHTTP followed by the method handler) must reuse the
	// installed caches instead of replacing them mid-request.
	again := ensureRequestCaches(cached)
	if again != cached {
		t.Fatal("ensureRequestCaches must be idempotent for a request that already carries the caches")
	}

	// Mutation sites clear the caches so later reads in the same request see
	// fresh state.
	cache := aclEntryCacheFromContext(cached.Context())
	cache.put("/dav/calendars/1", []store.ACLEntry{{ResourcePath: "/dav/calendars/1"}})
	invalidateACLEntryCache(cached.Context())
	if entries, ok := cache.get("/dav/calendars/1"); ok || len(entries) != 0 {
		t.Fatal("invalidateACLEntryCache must drop cached entries")
	}
	memo := davPathMemoFromContext(cached.Context())
	memo.put(davPathMemoKey{userID: 1, path: "/dav/calendars/work"}, "/dav/calendars/1")
	invalidateDAVPathMemo(cached.Context())
	if _, ok := memo.get(davPathMemoKey{userID: 1, path: "/dav/calendars/work"}); ok {
		t.Fatal("invalidateDAVPathMemo must drop memoized resolutions")
	}
}
