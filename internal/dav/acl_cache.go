package dav

import (
	"context"
	"sync"

	"github.com/jw6ventures/calcard/internal/store"
)

// aclEntryCache memoizes ACL entry lookups for the duration of a single read
// request. Computing a resource's current-user-privilege-set evaluates every
// privilege independently, and each evaluation re-reads the same acl_entries
// rows; for a shared collection that turns one PROPFIND into ~18 identical
// acl.list_by_resource queries. The entries cannot change within a read request,
// so caching them collapses those to one query per distinct path.
//
// The cache is only installed for read methods (see ServeHTTP); ACL-mutating
// methods never see it, so they always read fresh.
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
