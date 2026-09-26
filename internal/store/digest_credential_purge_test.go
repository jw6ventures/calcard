package store

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jw6ventures/calcard/internal/logging"
	jw6_utils "github.com/jw6ventures/jw6-go-utils"
)

// stubAppPasswordRepo answers PurgeDigestCredentials from purgeFn and
// everything else with the zero value; the purge calls nothing else.
type stubAppPasswordRepo struct {
	mu      sync.Mutex
	purges  int
	purgeFn func(ctx context.Context, call int) (int64, error)
}

func (r *stubAppPasswordRepo) PurgeDigestCredentials(ctx context.Context) (int64, error) {
	r.mu.Lock()
	r.purges++
	call := r.purges
	r.mu.Unlock()
	if r.purgeFn != nil {
		return r.purgeFn(ctx, call)
	}
	return 0, nil
}

func (r *stubAppPasswordRepo) purgeCalls() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.purges
}

func (r *stubAppPasswordRepo) Create(context.Context, AppPassword) (*AppPassword, error) {
	return nil, nil
}
func (r *stubAppPasswordRepo) FindValidByUser(context.Context, int64) ([]AppPassword, error) {
	return nil, nil
}
func (r *stubAppPasswordRepo) ListByUser(context.Context, int64) ([]AppPassword, error) {
	return nil, nil
}
func (r *stubAppPasswordRepo) GetByID(context.Context, int64) (*AppPassword, error) { return nil, nil }
func (r *stubAppPasswordRepo) Revoke(context.Context, int64) error                  { return nil }
func (r *stubAppPasswordRepo) DeleteRevoked(context.Context, int64) error           { return nil }
func (r *stubAppPasswordRepo) TouchLastUsed(context.Context, int64) error           { return nil }
func (r *stubAppPasswordRepo) ReplaceDigestCredentials(context.Context, int64, *string, *string, *string, *string) (bool, error) {
	return false, nil
}

type recordedLogLine struct {
	level   jw6_utils.LogLevel
	message string
}

type recordingLogSink struct {
	mu    sync.Mutex
	lines []recordedLogLine
}

func (s *recordingLogSink) Log(_, _ string, level jw6_utils.LogLevel, message string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.lines = append(s.lines, recordedLogLine{level: level, message: message})
}

func (s *recordingLogSink) recorded() []recordedLogLine {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]recordedLogLine(nil), s.lines...)
}

// A deployment that has not opted into Digest must not be left holding the
// HA1s an earlier run wrote. One that has opted in needs them.
func TestStartDigestCredentialPurgeFollowsTheDigestSetting(t *testing.T) {
	t.Run("enabled", func(t *testing.T) {
		// Already cancelled, so a missing gate returns after one pass instead
		// of looping forever.
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		repo := &stubAppPasswordRepo{}
		StartDigestCredentialPurge(ctx, repo, true, time.Millisecond)
		if got := repo.purgeCalls(); got != 0 {
			t.Fatalf("PurgeDigestCredentials calls = %d, want 0", got)
		}
	})
	t.Run("disabled", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		repo := &stubAppPasswordRepo{purgeFn: func(context.Context, int) (int64, error) {
			cancel()
			return 0, nil
		}}
		StartDigestCredentialPurge(ctx, repo, false, time.Hour)
		if got := repo.purgeCalls(); got != 1 {
			t.Fatalf("PurgeDigestCredentials calls = %d, want 1 immediate pass", got)
		}
	})
}

// A failed pass must not be the last word: the purge is retried on the next
// tick instead of waiting for a restart that may never come.
func TestStartDigestCredentialPurgeRetriesAfterAFailure(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	repo := &stubAppPasswordRepo{purgeFn: func(_ context.Context, call int) (int64, error) {
		if call == 1 {
			return 0, errors.New("database unreachable")
		}
		cancel()
		return 1, nil
	}}

	done := make(chan struct{})
	go func() {
		defer close(done)
		StartDigestCredentialPurge(ctx, repo, false, time.Millisecond)
	}()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("StartDigestCredentialPurge did not retry after a failed pass")
	}
	if got := repo.purgeCalls(); got != 2 {
		t.Fatalf("PurgeDigestCredentials calls = %d, want 2", got)
	}
}

// The purge is a table-wide UPDATE; one that hangs on a lock must not hold a
// connection for the life of the process.
func TestPurgeDigestCredentialsRunsUnderADeadline(t *testing.T) {
	var deadline time.Time
	var hasDeadline bool
	repo := &stubAppPasswordRepo{purgeFn: func(ctx context.Context, _ int) (int64, error) {
		deadline, hasDeadline = ctx.Deadline()
		return 0, nil
	}}
	purgeDigestCredentials(context.Background(), repo, logging.New(nil, "Store"))
	if !hasDeadline {
		t.Fatal("PurgeDigestCredentials ran without a deadline")
	}
	if remaining := time.Until(deadline); remaining <= 0 || remaining > digestCredentialPurgeTimeout {
		t.Fatalf("purge deadline in %s, want within %s", remaining, digestCredentialPurgeTimeout)
	}
}

// A failure is reported, not fatal, and a pass that clears rows says how many.
func TestPurgeDigestCredentialsReportsItsOutcome(t *testing.T) {
	for _, tc := range []struct {
		name      string
		purged    int64
		err       error
		wantLevel jw6_utils.LogLevel
		wantText  string
	}{
		{name: "failure", err: errors.New("database unreachable"), wantLevel: jw6_utils.Error, wantText: "database unreachable"},
		{name: "cleared rows", purged: 3, wantLevel: jw6_utils.Warn, wantText: "from 3 app passwords"},
		{name: "nothing to clear"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			sink := &recordingLogSink{}
			repo := &stubAppPasswordRepo{purgeFn: func(context.Context, int) (int64, error) { return tc.purged, tc.err }}
			purgeDigestCredentials(context.Background(), repo, logging.New(sink, "Store"))

			lines := sink.recorded()
			if tc.wantText == "" {
				if len(lines) != 0 {
					t.Fatalf("logged %v, want nothing", lines)
				}
				return
			}
			if len(lines) != 1 || lines[0].level != tc.wantLevel || !strings.Contains(lines[0].message, tc.wantText) {
				t.Fatalf("logged %v, want one level-%v line containing %q", lines, tc.wantLevel, tc.wantText)
			}
		})
	}
}
