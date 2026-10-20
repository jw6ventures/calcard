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
	ctx := withDAVRequestState(context.Background())
	if _, err := h.aclEntriesForResource(ctx, "/dav/calendars/3"); err != nil {
		t.Fatalf("aclEntriesForResource() error = %v", err)
	}
	firstCalls := aclRepo.listByResourcesCalls
	if firstCalls == 0 {
		t.Fatal("expected the first lookup to query the store")
	}
	for i := 0; i < 6; i++ {
		if _, err := h.aclEntriesForResource(ctx, "/dav/calendars/3"); err != nil {
			t.Fatalf("aclEntriesForResource() error = %v", err)
		}
	}
	if aclRepo.listByResourcesCalls != firstCalls {
		t.Fatalf("cached lookups queried the store again: calls = %d, want %d", aclRepo.listByResourcesCalls, firstCalls)
	}

	// Without a cache in the context (e.g. direct handler calls in tests),
	// every call reads fresh.
	aclRepo.listByResourcesCalls = 0
	for i := 0; i < 3; i++ {
		if _, err := h.aclEntriesForResource(context.Background(), "/dav/calendars/3"); err != nil {
			t.Fatalf("aclEntriesForResource() error = %v", err)
		}
	}
	if aclRepo.listByResourcesCalls < 3 {
		t.Fatalf("uncached lookups should each hit the store: calls = %d, want >= 3", aclRepo.listByResourcesCalls)
	}
}

func TestEnsureRequestCachesInstallsOnce(t *testing.T) {
	req := httptest.NewRequest("PUT", "/dav/calendars/1/e.ics", nil)

	cached := ensureRequestCaches(req)
	if aclEntryCacheFromContext(cached.Context()) == nil {
		t.Fatal("ensureRequestCaches must install the ACL entry cache")
	}
	// Calling it again on a request that already carries the caches must reuse
	// them instead of replacing them mid-request.
	again := ensureRequestCaches(cached)
	if again != cached {
		t.Fatal("ensureRequestCaches must be idempotent for a request that already carries the caches")
	}
}

func TestInvalidateDAVRequestStateInvalidatesEveryRequestCache(t *testing.T) {
	req := ensureRequestCaches(httptest.NewRequest("GET", "/dav/calendars/work/event.ics", nil))
	aclCache := aclEntryCacheFromContext(req.Context())
	aclCache.put("/dav/calendars/2/event", []store.ACLEntry{{Privilege: "read"}})
	state := davRequestStateFromContext(req.Context())
	resolutionKey := collectionResolutionKey{userID: 1, prefix: calendarPrefix, segment: "work"}
	state.putCollectionResolution(resolutionKey, collectionResolutionResult{id: 2, ok: true})
	accessKey := calendarAccessKey{calendarID: 2, userID: 1}
	state.memoizeCalendarAccess(accessKey, &store.CalendarAccess{Calendar: store.Calendar{ID: 2}})
	lockIndex := &lockBatchIndex{byPath: map[string][]store.Lock{"/dav/calendars/2/event": nil}}
	ctx := withLockBatchIndex(req.Context(), lockIndex)

	invalidateDAVRequestState(ctx)

	if _, ok := aclCache.get("/dav/calendars/2/event"); ok {
		t.Fatal("ACL cache survived request-state invalidation")
	}
	if _, ok := state.collectionResolution(resolutionKey); ok {
		t.Fatal("collection resolution survived request-state invalidation")
	}
	if _, ok := state.memoizedCalendarAccess(accessKey); ok {
		t.Fatal("stored calendar access survived request-state invalidation")
	}
	if !lockIndex.isStale() {
		t.Fatal("lock batch index was not marked stale")
	}
}
