package dav

import (
	"context"

	"github.com/jw6ventures/calcard/internal/store"
)

// lockBatchIndex holds the locks relevant to a batch of PROPFIND responses,
// fetched in a single locks.list_by_resources query and grouped by their
// normalized resource path. A Depth: 1 PROPFIND that requests lockdiscovery
// would otherwise issue one lock query per child (an N+1); prefetching the
// union of every response's lock-lookup paths collapses that to one query.
//
// The index is keyed the same way lockLookupPaths produces paths
// (normalizeDAVHref) so a lookup is exactly the set of locks that
// store.Locks.ListByResources would have returned for that resource.
type lockBatchIndex struct {
	byPath map[string][]store.Lock
}

type lockBatchIndexKeyType struct{}

var lockBatchIndexKey = lockBatchIndexKeyType{}

func withLockBatchIndex(ctx context.Context, idx *lockBatchIndex) context.Context {
	return context.WithValue(ctx, lockBatchIndexKey, idx)
}

func lockBatchIndexFromContext(ctx context.Context) *lockBatchIndex {
	idx, _ := ctx.Value(lockBatchIndexKey).(*lockBatchIndex)
	return idx
}

// locksForPaths returns the locks whose resource path matches one of paths,
// reproducing store.Locks.ListByResources from the prefetched batch.
func (idx *lockBatchIndex) locksForPaths(paths []string) []store.Lock {
	var result []store.Lock
	for _, p := range paths {
		result = append(result, idx.byPath[p]...)
	}
	return result
}
