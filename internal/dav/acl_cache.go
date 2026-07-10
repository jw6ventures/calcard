package dav

import (
	"context"
	"sync"

	"github.com/jw6ventures/calcard/internal/store"
)

// aclEntryCache memoizes ACL entry lookups for the duration of a single
// request. Computing a resource's current-user-privilege-set evaluates every
// privilege independently, and each evaluation re-reads the same acl_entries
// rows; for a shared collection that turns one PROPFIND into ~18 identical
// acl.list_by_resource queries. The entries cannot change within a request
// except at the few DAV-internal mutation sites (ACL, MOVE rebind, DELETE
// cleanup), each of which calls invalidateACLEntryCache, so caching them
// collapses those to one query per distinct path.
type aclEntryCache struct {
	mu      sync.Mutex
	entries map[string][]store.ACLEntry
}

type aclEntryCacheKeyType struct{}

var aclEntryCacheKey = aclEntryCacheKeyType{}

func withACLEntryCache(ctx context.Context) context.Context {
	return context.WithValue(ctx, aclEntryCacheKey, &aclEntryCache{entries: make(map[string][]store.ACLEntry)})
}

func aclEntryCacheFromContext(ctx context.Context) *aclEntryCache {
	cache, _ := ctx.Value(aclEntryCacheKey).(*aclEntryCache)
	return cache
}

func (c *aclEntryCache) get(key string) ([]store.ACLEntry, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	entries, ok := c.entries[key]
	return entries, ok
}

func (c *aclEntryCache) put(key string, entries []store.ACLEntry) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.entries[key] = entries
}

func (c *aclEntryCache) invalidate() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.entries = make(map[string][]store.ACLEntry)
}

// invalidateACLEntryCache drops every cached ACL entry for the current request.
// Any code path that writes acl_entries must call it so later reads within the
// same request see the mutation.
func invalidateACLEntryCache(ctx context.Context) {
	if cache := aclEntryCacheFromContext(ctx); cache != nil {
		cache.invalidate()
	}
}
