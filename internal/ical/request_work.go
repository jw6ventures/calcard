package ical

import (
	"context"
	"sync/atomic"
)

// requestWorkChunk is how many units a recurrenceWork claims from a
// RequestWork at a time. It is small enough that one evaluation refused by
// the pool wastes little of what another evaluation in the same request
// could still have spent, and large enough -- 1/40 of recurrenceWorkLimit --
// that an evaluation spending its whole per-evaluation cap touches the
// pool's atomic only a few dozen times, keeping the per-candidate hot path
// free of contention.
const requestWorkChunk = 50_000

// RequestWork is the recurrence and observance budget shared by every
// evaluation and reading that answers one request: a DAV REPORT over many
// resources, a UI events listing, one collection transfer and its retries, or
// a single write's two readings of the object it validates. Each evaluation
// still carries its own recurrenceWorkLimit cap -- one resource's single walk
// can never exceed that on its own -- and the pool additionally bounds what
// every evaluation and reading of the request spend together.
//
// An evaluation draws from the pool in chunks (requestWorkChunk units at a
// time), one atomic add each, rather than through a lock or by taking
// whatever remains in one step, so charging the pool costs no more than the
// per-evaluation cap already did and adds no lock traffic to the hot
// per-candidate path. Create one with NewRequestWork; it is safe for
// concurrent use.
type RequestWork struct {
	remaining atomic.Int64
	ctx       context.Context
	// denied is set once an evaluation asked for work the pool could not give.
	denied atomic.Bool
	// parent, when set, is the pool this one's evaluations fall back on once
	// remaining cannot give what they ask for; borrowed is how much of what
	// they hold came from there, which refund returns first.
	parent   *RequestWork
	borrowed atomic.Int64
}

// NewRequestWork returns a pool with units to spend across every evaluation
// and reading that answers one request. ctx, when not nil, ends the pool
// early: once it reports an error, the pool grants nothing further, so a walk
// drawing on it stops soon after the request's deadline passes or it is
// canceled, rather than only between resources. A nil ctx never ends it. The
// functions that take a pool read a nil one as no pool, which leaves each
// evaluation only its own recurrenceWorkLimit.
func NewRequestWork(ctx context.Context, units int) *RequestWork {
	pool := &RequestWork{ctx: ctx}
	pool.remaining.Store(int64(units))
	return pool
}

// Allowance returns a pool holding units of its own for one of the resources
// p's request answers. Its evaluations spend those first and draw on p only for
// what they need beyond them, so a resource that costs no more than its
// allowance is answered whole however far the resources before it have run p
// down, and one that costs more is cut short only when p can no longer give
// the rest. What every allowance spends beyond p is the units it was given, so
// the request as a whole is bounded by p's units and the allowances together.
// The returned pool ends with p's context, and its Exhausted reports only that
// its own evaluations were refused. An allowance of a nil pool is nil.
func (p *RequestWork) Allowance(units int) *RequestWork {
	if p == nil {
		return nil
	}
	allowance := &RequestWork{ctx: p.ctx, parent: p}
	allowance.remaining.Store(int64(units))
	return allowance
}

// take removes up to want from the pool's own units in one step, returning how
// many it removed.
func (p *RequestWork) take(want int) int {
	for {
		current := p.remaining.Load()
		grant := int64(want)
		if grant > current {
			grant = max(current, 0)
		}
		if grant == 0 || p.remaining.CompareAndSwap(current, current-grant) {
			return int(grant)
		}
	}
}

// draw removes up to want from the pool in one step, returning how many it
// actually granted: fewer than asked when the pool is running low, and none
// once it is empty or its context has ended. A grant of less than need is a
// denial, which Exhausted reports from then on.
func (p *RequestWork) draw(want, need int) int {
	if p.ctx != nil && p.ctx.Err() != nil {
		p.denied.Store(true)
		return 0
	}
	grant := p.take(want)
	if p.parent != nil && grant < want {
		lent := p.parent.draw(want-grant, max(need-grant, 0))
		p.borrowed.Add(int64(lent))
		grant += lent
	}
	// Only a grant that stands decides the denial: a take that failed because
	// the pool changed under it was read again.
	if grant < need {
		p.denied.Store(true)
	}
	return grant
}

// refund returns units a recurrenceWork drew and did not spend, to the pool
// they were lent by before the pool's own.
func (p *RequestWork) refund(units int) {
	if p.parent != nil {
		for units > 0 {
			borrowed := p.borrowed.Load()
			back := min(int64(units), borrowed)
			if back == 0 {
				break
			}
			if p.borrowed.CompareAndSwap(borrowed, borrowed-back) {
				p.parent.refund(int(back))
				units -= int(back)
			}
		}
	}
	if units > 0 {
		p.remaining.Add(int64(units))
	}
}

// short reports that the pool refused a draw work it needed.
func (p *RequestWork) short() bool {
	return p.Exhausted()
}

// Exhausted reports that the pool has refused an evaluation work it needed, or
// that its context has ended: some answer drawn on it was cut short. A caller
// that owes an answer for each of many resources uses it to answer the
// remaining ones as it answers one it ran out of work on, without starting
// evaluations that would be refused at their first unit, and to tell an answer
// that was judged on work that ran out from one that was not. A pool drawn
// down to exactly nothing by work that was all granted is not exhausted. A nil
// pool is never exhausted.
func (p *RequestWork) Exhausted() bool {
	if p == nil {
		return false
	}
	return p.denied.Load() || (p.ctx != nil && p.ctx.Err() != nil)
}
