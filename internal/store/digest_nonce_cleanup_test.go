package store

import (
	"context"
	"sync"
	"testing"
	"time"
)

type recordingDigestNonceRepo struct {
	mu        sync.Mutex
	calls     int
	pruned    chan struct{}
	returnErr error
}

func (r *recordingDigestNonceRepo) Consume(context.Context, int64, string, uint32, time.Time) (bool, error) {
	return true, nil
}

func (r *recordingDigestNonceRepo) DeleteExpired(_ context.Context) (int64, error) {
	r.mu.Lock()
	r.calls++
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

func (r *recordingDigestNonceRepo) callCount() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.calls
}

// Every authenticated Digest request writes a row here, a far higher rate than
// the tombstones StartDeletedResourceCleanup prunes, so the same argument that
// gives that job an immediate first pass applies at least as strongly: a
// process restarted more often than the interval would otherwise never prune.
func TestStartDigestNonceCleanupPrunesImmediately(t *testing.T) {
	repo := &recordingDigestNonceRepo{pruned: make(chan struct{}, 1)}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	done := make(chan struct{})
	go func() {
		StartDigestNonceCleanup(ctx, repo, time.Hour)
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

	if calls := repo.callCount(); calls == 0 {
		t.Fatalf("cleanup calls = %d, want at least one immediate pass", calls)
	}
}

func TestStartDigestNonceCleanupWithNilRepository(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	StartDigestNonceCleanup(ctx, nil, time.Hour)
}
