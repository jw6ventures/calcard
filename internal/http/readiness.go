package httpserver

import (
	"context"
	"sync"
	"time"
)

// readinessTimeout is how long /readyz waits for the database before
// answering 503.
const readinessTimeout = 2 * time.Second

// readinessProbe runs at most one database check at a time. A probe that
// outlasts its budget answers unready and leaves the check running, and every
// probe that arrives while it runs waits on that same check rather than
// starting another, so a database that never answers holds one check, and the
// one connection it is blocked on, however often readiness is polled.
type readinessProbe struct {
	check   func(context.Context) error
	timeout time.Duration

	mu      sync.Mutex
	running *readinessCheck
}

type readinessCheck struct {
	done chan struct{}
	err  error
}

func (p *readinessProbe) ready(ctx context.Context) error {
	ctx, cancel := context.WithTimeout(ctx, p.timeout)
	defer cancel()
	check := p.start(ctx)
	select {
	case <-check.done:
		return check.err
	case <-ctx.Done():
		return ctx.Err()
	}
}

// start joins the check in progress or begins one. A check is shared, so it
// keeps the starting request's values, for metrics, but not its cancellation.
func (p *readinessProbe) start(ctx context.Context) *readinessCheck {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.running != nil {
		return p.running
	}
	check := &readinessCheck{done: make(chan struct{})}
	p.running = check
	go func() {
		checkCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), p.timeout)
		defer cancel()
		check.err = p.check(checkCtx)
		p.mu.Lock()
		p.running = nil
		p.mu.Unlock()
		close(check.done)
	}()
	return check
}
