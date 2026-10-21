package dav

import (
	"cmp"
	"context"
	"slices"
	"sync"

	"github.com/jw6ventures/calcard/internal/store"
)

// lockBatchIndex holds the locks a request has read, by the lookup path they
// were read for, so the checks of one request read each path's locks once: a
// Depth: 1 PROPFIND that requests lockdiscovery prefetches every response's
// lookup paths in one locks.list_by_resources query instead of one per child,
// and a write's lock check and the preconditions it hands the store read the
// same paths.
//
// The index is keyed the way lockLookupPaths produces paths (cleanDAVPath),
// so the locks it holds for a path are exactly the set
// store.Locks.ListByResources returns for it. It answers only for the paths it
// has read; any other path is read from the store.
type lockBatchIndex struct {
	mu      sync.Mutex
	byPath  map[string][]store.Lock
	fetched map[string]struct{}
	stale   bool
}

// newLockBatchIndex indexes locks, the result of reading the locks at paths.
func newLockBatchIndex(paths []string, locks []store.Lock) *lockBatchIndex {
	idx := &lockBatchIndex{byPath: make(map[string][]store.Lock), fetched: make(map[string]struct{})}
	idx.add(paths, locks)
	return idx
}

// add records locks as the locks read at paths.
func (idx *lockBatchIndex) add(paths []string, locks []store.Lock) {
	idx.mu.Lock()
	defer idx.mu.Unlock()
	for _, p := range paths {
		idx.fetched[p] = struct{}{}
	}
	for i := range locks {
		key := cleanDAVPath(locks[i].ResourcePath)
		idx.byPath[key] = append(idx.byPath[key], locks[i])
	}
}

// unfetched lists the paths among paths the index has not read.
func (idx *lockBatchIndex) unfetched(paths []string) []string {
	idx.mu.Lock()
	defer idx.mu.Unlock()
	var missing []string
	for _, p := range paths {
		if _, ok := idx.fetched[p]; !ok {
			missing = append(missing, p)
		}
	}
	return missing
}

func (idx *lockBatchIndex) markStale() {
	idx.mu.Lock()
	defer idx.mu.Unlock()
	idx.stale = true
}

func (idx *lockBatchIndex) isStale() bool {
	idx.mu.Lock()
	defer idx.mu.Unlock()
	return idx.stale
}

func withLockBatchIndex(ctx context.Context, idx *lockBatchIndex) context.Context {
	ctx = withDAVRequestState(ctx)
	davRequestStateFromContext(ctx).setLockIndex(idx)
	return ctx
}

func lockBatchIndexFromContext(ctx context.Context) *lockBatchIndex {
	if state := davRequestStateFromContext(ctx); state != nil {
		return state.currentLockIndex()
	}
	return nil
}

// locksForPaths returns the locks whose resource path matches one of paths,
// reproducing store.Locks.ListByResources from the index, order included. The
// caller reads any path the index has not fetched from the store first.
func (idx *lockBatchIndex) locksForPaths(paths []string) []store.Lock {
	idx.mu.Lock()
	defer idx.mu.Unlock()
	var result []store.Lock
	for _, p := range paths {
		result = append(result, idx.byPath[p]...)
	}
	slices.SortFunc(result, func(a, b store.Lock) int {
		if order := a.CreatedAt.Compare(b.CreatedAt); order != 0 {
			return order
		}
		return cmp.Compare(a.ID, b.ID)
	})
	return result
}
