package ical

import (
	"context"
	"testing"
)

// An allowance spends its own units before it borrows from its pool, leaves
// the pool untouched while its own units cover a draw, and on a refund returns
// what it borrowed to the pool before keeping any itself.
func TestAllowanceSpendsItsOwnUnitsBeforeThePoolsAndReturnsWhatItBorrowed(t *testing.T) {
	pool := NewRequestWork(nil, 1000)
	allowance := pool.Allowance(100)

	if got := allowance.draw(80, 80); got != 80 {
		t.Fatalf("drew %d, want 80 from the allowance", got)
	}
	if got := pool.remaining.Load(); got != 1000 {
		t.Fatalf("pool holds %d after a draw the allowance covered, want 1000", got)
	}
	if got := allowance.draw(50, 50); got != 50 {
		t.Fatalf("drew %d, want 50", got)
	}
	if got, own := pool.remaining.Load(), allowance.remaining.Load(); got != 970 || own != 0 {
		t.Fatalf("pool holds %d and the allowance %d, want 970 and 0", got, own)
	}

	allowance.refund(50)
	if got, own := pool.remaining.Load(), allowance.remaining.Load(); got != 1000 || own != 20 {
		t.Fatalf("after refunding 50: pool holds %d and the allowance %d, want 1000 and 20", got, own)
	}
	if pool.Exhausted() || allowance.Exhausted() {
		t.Error("a pool that gave everything asked is exhausted")
	}
}

// An allowance of a pool that has run out still gives its own units without
// being exhausted, and reports itself exhausted only once it is refused work it
// needed.
func TestAllowanceOutlastsASpentPoolButNotItsOwnUnits(t *testing.T) {
	pool := NewRequestWork(nil, 100)
	if got := pool.draw(500, 500); got != 100 || !pool.Exhausted() {
		t.Fatalf("drew %d, exhausted %t; want the 100 the pool held, and exhausted", got, pool.Exhausted())
	}

	allowance := pool.Allowance(40)
	if got := allowance.draw(30, 30); got != 30 {
		t.Fatalf("drew %d from a spent pool's allowance, want 30", got)
	}
	if allowance.Exhausted() {
		t.Error("an allowance that gave everything asked is exhausted because its pool is")
	}
	if got := allowance.draw(50, 20); got != 10 {
		t.Fatalf("drew %d with 10 left and none to borrow, want 10", got)
	}
	if !allowance.Exhausted() {
		t.Error("an allowance refused work it needed is not exhausted")
	}
}

// A draw larger than an allowance holds takes the rest from the pool, and is
// refused, with both reporting exhausted, only when the pool cannot give it.
func TestAllowanceDrawsWhatItNeedsBeyondItsUnitsFromThePool(t *testing.T) {
	pool := NewRequestWork(nil, 100)
	allowance := pool.Allowance(10)
	if got := allowance.draw(60, 60); got != 60 {
		t.Fatalf("drew %d, want 10 of its own and 50 of the pool's", got)
	}
	if got := pool.remaining.Load(); got != 50 {
		t.Fatalf("pool holds %d, want 50", got)
	}
	if got := allowance.draw(80, 80); got != 50 || !allowance.Exhausted() || !pool.Exhausted() {
		t.Fatalf("drew %d; want the 50 the pool had left, refused and exhausted", got)
	}
}

// An allowance grants nothing once its pool's context has ended, and reports
// itself exhausted.
func TestAllowanceEndsWithThePoolsContext(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	allowance := NewRequestWork(ctx, 100).Allowance(100)
	cancel()
	if got := allowance.draw(10, 10); got != 0 || !allowance.Exhausted() {
		t.Fatalf("drew %d after cancellation, exhausted %t; want none and exhausted", got, allowance.Exhausted())
	}
}

// An allowance of a nil pool is nil, which the functions that take a pool read
// as no pool.
func TestAllowanceOfNoPoolIsNoPool(t *testing.T) {
	var pool *RequestWork
	if pool.Allowance(100) != nil {
		t.Error("an allowance of a nil pool is a pool")
	}
}
