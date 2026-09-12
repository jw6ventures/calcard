package store

import (
	"context"
	"sync"
	"testing"
	"time"
)

type recordingDeletedResourceRepo struct {
	mu        sync.Mutex
	cutoffs   []time.Duration
	pruned    chan struct{}
	returnErr error
}

func (r *recordingDeletedResourceRepo) ListDeletedSincePageAfter(context.Context, string, int64, int64, time.Time, int) ([]DeletedResource, error) {
	return nil, nil
}

func (r *recordingDeletedResourceRepo) DeleteByIdentity(context.Context, string, int64, string, string) error {
	return nil
}

func (r *recordingDeletedResourceRepo) Cleanup(_ context.Context, olderThan time.Duration) (int64, error) {
	r.mu.Lock()
	r.cutoffs = append(r.cutoffs, olderThan)
	r.mu.Unlock()
	select {
	case r.pruned <- struct{}{}:
	default:
	}
	if r.returnErr != nil {
		return 0, r.returnErr
	}
	return 1, nil
}

func (r *recordingDeletedResourceRepo) calls() []time.Duration {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]time.Duration(nil), r.cutoffs...)
}

// Nothing pruned deleted_resources, so the table grew for the life of the
// deployment while every incremental sync read from it. The job prunes once on
// start rather than waiting out the first interval, since a process restarted
// more often than the interval would otherwise never prune at all.
func TestStartDeletedResourceCleanupPrunesAtTheRetentionWindow(t *testing.T) {
	repo := &recordingDeletedResourceRepo{pruned: make(chan struct{}, 1)}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	done := make(chan struct{})
	go func() {
		StartDeletedResourceCleanup(ctx, repo, time.Hour, 48*time.Hour)
		close(done)
	}()

	select {
	case <-repo.pruned:
	case <-time.After(2 * time.Second):
		t.Fatal("cleanup did not run its first pass")
	}
	cancel()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("cleanup did not stop when its context was cancelled")
	}

	calls := repo.calls()
	if len(calls) == 0 || calls[0] != 48*time.Hour {
		t.Fatalf("cleanup cutoffs = %v, want the retention window", calls)
	}
}

// A retention of zero turns pruning off, on the terms every other resource knob
// in this server uses. An operator who does not want tombstones pruned must not
// silently get the default window, because pruning is what makes a sync token
// past the window unanswerable.
func TestStartDeletedResourceCleanupOffPrunesNothing(t *testing.T) {
	repo := &recordingDeletedResourceRepo{pruned: make(chan struct{}, 1)}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	done := make(chan struct{})
	go func() {
		StartDeletedResourceCleanup(ctx, repo, time.Millisecond, 0)
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("cleanup with retention off did not return")
	}
	if calls := repo.calls(); len(calls) != 0 {
		t.Fatalf("cleanup with retention off pruned %v", calls)
	}
}
