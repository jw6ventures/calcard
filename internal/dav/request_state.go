package dav

import (
	"context"
	"sync"

	"github.com/jw6ventures/calcard/internal/store"
)

type davRequestState struct {
	aclEntries *aclEntryCache

	mu                    sync.Mutex
	lockIndex             *lockBatchIndex
	collectionResolutions map[collectionResolutionKey]collectionResolutionResult
	primaryTarget         *davTarget
}

type collectionResolutionKey struct {
	userID  int64
	prefix  string
	segment string
}

type collectionResolutionResult struct {
	id  int64
	ok  bool
	err error
}

type davRequestStateKeyType struct{}

var davRequestStateKey = davRequestStateKeyType{}

func withDAVRequestState(ctx context.Context) context.Context {
	if davRequestStateFromContext(ctx) != nil {
		return ctx
	}
	return context.WithValue(ctx, davRequestStateKey, &davRequestState{
		aclEntries:            &aclEntryCache{entries: make(map[string][]store.ACLEntry)},
		collectionResolutions: make(map[collectionResolutionKey]collectionResolutionResult),
	})
}

func davRequestStateFromContext(ctx context.Context) *davRequestState {
	state, _ := ctx.Value(davRequestStateKey).(*davRequestState)
	return state
}

func (s *davRequestState) setLockIndex(index *lockBatchIndex) {
	if s == nil {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.lockIndex = index
}

func (s *davRequestState) currentLockIndex() *lockBatchIndex {
	if s == nil {
		return nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.lockIndex
}

func (s *davRequestState) collectionResolution(key collectionResolutionKey) (collectionResolutionResult, bool) {
	if s == nil {
		return collectionResolutionResult{}, false
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	result, ok := s.collectionResolutions[key]
	return result, ok
}

func (s *davRequestState) putCollectionResolution(key collectionResolutionKey, result collectionResolutionResult) {
	if s == nil {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.collectionResolutions[key] = result
}

func (s *davRequestState) invalidateCollectionResolutions() {
	if s == nil {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.collectionResolutions = make(map[collectionResolutionKey]collectionResolutionResult)
}

func (s *davRequestState) setPrimaryDAVTarget(target davTarget) {
	if s == nil {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	copy := target
	s.primaryTarget = &copy
}

func (s *davRequestState) primaryDAVTarget(cleanPath string) (davTarget, bool) {
	if s == nil {
		return davTarget{}, false
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.primaryTarget == nil || s.primaryTarget.CleanPath != cleanPath {
		return davTarget{}, false
	}
	return *s.primaryTarget, true
}

func invalidateDAVRequestState(ctx context.Context) {
	state := davRequestStateFromContext(ctx)
	if state == nil {
		return
	}
	state.aclEntries.invalidate()
	state.invalidateCollectionResolutions()
	if index := state.currentLockIndex(); index != nil {
		index.markStale()
	}
}
