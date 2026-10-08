package dav

import (
	"strings"
	"sync"
	"sync/atomic"
	"time"
	"unsafe"

	"github.com/jw6ventures/calcard/internal/ical"
	"github.com/jw6ventures/calcard/internal/lru"
)

const (
	// maxListedOnsetsPerRule bounds the onsets each listing of one observance
	// rule keeps. A yearly rule from Exchange's 1601 DTSTART reaches past 2100
	// well inside it; a denser rule keeps what fits, and an instant past that
	// is answered from a window listed around it (listedRule.latest).
	maxListedOnsetsPerRule = 2048
	// maxHeldOnsets bounds what the cache keeps across every rule, counted in
	// onsets, about three megabytes. Real zone definitions share a few hundred
	// rules, each a few hundred onsets long.
	maxHeldOnsets = 1 << 17
	// onsetRuleOverhead is what one cached rule is charged beyond its onsets
	// and its RRULE text, so rules that list nothing still count against
	// maxHeldOnsets and cannot grow the cache without bound.
	onsetRuleOverhead = 16
	// onsetBytes is the size of one listed onset, the unit a rule's RRULE text
	// is charged in.
	onsetBytes = int(unsafe.Sizeof(time.Time{}))
	// onsetListingHorizonYears is how far past the instant that asked a rule's
	// onsets are listed, so the next instant resolved through it -- usually the
	// next instance of a series -- is answered from the same listing.
	onsetListingHorizonYears = 50
	// onsetListingMarginYears is how far before the instant that asked a
	// listing started again from an instant keeps onsets, so the instants
	// just before it -- the same instant read on the other scale a
	// definition's offsets are read on, or an earlier instance -- are answered
	// from the same listing.
	onsetListingMarginYears = 1
)

// sharedTimezoneOnsets serves every VTIMEZONE observance lookup the server
// makes. RFC 4791 §4.1 has every resource carry its own copy of each zone it
// uses, so a collection repeats the same few rules in every resource, and
// without the cache each instance of each series would walk them again from the
// observance's DTSTART -- four centuries for the 1601 DTSTART Exchange writes.
// A rule's onsets depend on nothing but its DTSTART and RRULE, so one listing
// serves every report, PUT and COPY or MOVE that meets the rule, whichever
// resource or zone name it arrives under.
var sharedTimezoneOnsets = newTimezoneOnsets(maxHeldOnsets)

// SharedTimezoneOnsets is the observance onset cache the DAV layer reads every
// VTIMEZONE through, for the process's other readers of stored calendar
// objects to share, so each rule is listed once however many readers meet it.
func SharedTimezoneOnsets() ical.ObservanceOnsets {
	return sharedTimezoneOnsets
}

// timezoneOnsets is a least-recently-used cache of observance rule listings,
// bounded by the onsets it holds. An entry pushed out is listed again the next
// time it is asked for, so a burst of distinct rules costs listings rather than
// locking out the rules every other resource shares.
//
// A nil cache asks every question of the rule directly.
type timezoneOnsets struct {
	rules *lru.Cache[observanceRuleKey, *listedRule]

	// lookups and walks count the questions asked and those the listings could
	// not answer, and relists the listings started again from an instant, so
	// tests can tell the work done apart from the time it took.
	lookups atomic.Int64
	walks   atomic.Int64
	relists atomic.Int64
}

// observanceRuleKey identifies an observance rule by its DTSTART and RRULE. The
// DTSTART is held as Unix seconds and nanoseconds, which unlike a nanosecond
// count reach back to 1601; observance values are always read as UTC.
type observanceRuleKey struct {
	seconds     int64
	nanoseconds int
	rrule       string
}

// listedRule is one cached rule. Its lister is created and extended under its
// own lock, so concurrent lookups of one rule list it once and lookups of
// different rules do not wait on each other. A lookup holding a rule's lock may
// take the cache's, never the reverse.
type listedRule struct {
	key observanceRuleKey

	mu     sync.Mutex
	lister *ical.RecurrenceStartLister
	// window lists a counted rule from shortly before an instant lister
	// cannot reach, when one has been asked about; windowless reports that a
	// window kept from such an instant could not reach it either, so instants
	// past lister are walked.
	window     *ical.RecurrenceStartLister
	windowless bool
	// walked reports that the rule keeps no listing and every lookup walks
	// it: a rule without COUNT whose listing from the DTSTART stopped before
	// the present, which ical.LatestRecurrenceOnOrBefore answers by searching
	// back from the instant, in a few microseconds.
	walked bool
}

// listed is how many onsets the rule's listings hold. The caller holds the
// rule's lock, or has not yet handed the rule to any other lookup.
func (r *listedRule) listed() int {
	held := 0
	if r.lister != nil {
		held += r.lister.Len()
	}
	if r.window != nil {
		held += r.window.Len()
	}
	return held
}

func newTimezoneOnsets(budget int) *timezoneOnsets {
	// The RRULE was read out of a calendar object and shares its memory, so
	// the key a rule is cached under, and listed from, is a copy: holding the
	// original would keep the whole object.
	cloneKey := func(key observanceRuleKey) observanceRuleKey {
		key.rrule = strings.Clone(key.rrule)
		return key
	}
	// The cache charges a rule when it is added, before any lookup holds it,
	// and again from charge, under the rule's lock; listed is read in both.
	cost := func(key observanceRuleKey, rule *listedRule) int {
		return ruleCost(key.rrule, rule.listed())
	}
	return &timezoneOnsets{rules: lru.New(budget, cost, cloneKey)}
}

var _ ical.ObservanceOnsets = (*timezoneOnsets)(nil)

// LatestOnOrBefore answers what ical.LatestRecurrenceOnOrBefore answers for
// the same arguments, from a listing of the rule's onsets where one reaches
// before.
func (o *timezoneOnsets) LatestOnOrBefore(dtstart, before time.Time, rrule string) (latest time.Time, found, complete bool) {
	if o == nil || before.Before(dtstart) {
		return ical.LatestRecurrenceOnOrBefore(dtstart, before, rrule)
	}
	o.lookups.Add(1)
	if ruleCost(rrule, 1) > o.rules.Budget() {
		// The rule could not be cached with even its DTSTART listed without
		// exceeding the budget on its own.
		o.walks.Add(1)
		return ical.LatestRecurrenceOnOrBefore(dtstart, before, rrule)
	}
	rule := o.rule(observanceRuleKey{seconds: dtstart.Unix(), nanoseconds: dtstart.Nanosecond(), rrule: rrule})
	latest, found, listed := rule.latest(o, dtstart, before)
	if !listed {
		o.walks.Add(1)
		return ical.LatestRecurrenceOnOrBefore(dtstart, before, rrule)
	}
	return latest, found, true
}

// rule is the cache entry for key, created if absent, marked most recently
// used.
func (o *timezoneOnsets) rule(key observanceRuleKey) *listedRule {
	return o.rules.GetOrAdd(key, func(key observanceRuleKey) *listedRule {
		return &listedRule{key: key}
	})
}

// charge records what rule's listing now holds, and evicts the least recently
// used rules until the cache is back within its budget. The rule is marked most
// recently used again, since lookups of other rules may have marked theirs
// since it was marked, so the rules evicted to make room for it are the ones
// used least recently besides it; it is evicted itself only if it alone exceeds
// the budget, which a listing kept to listLimit never does. A rule that was
// evicted while it was being listed is not charged again. The caller holds the
// rule's lock.
func (o *timezoneOnsets) charge(rule *listedRule) {
	o.rules.Recharge(rule.key, rule)
}

// listLimit is the most onsets a listing of rule may hold beside held, what
// the rule's other listing holds: what one listing may keep, and no more than
// fits in the budget beside the rule's text and that listing, so that a rule
// listed from a distant DTSTART cannot hold the cache past its budget.
// Instants past the listings are asked of the rule directly.
func (o *timezoneOnsets) listLimit(rule *listedRule, held int) int {
	return min(maxListedOnsetsPerRule, o.rules.Budget()-ruleCost(rule.key.rrule, held))
}

// countedRule reports whether rrule ends by COUNT.
func countedRule(rrule string) bool {
	_, counted := ical.RRuleParam(rrule, "COUNT")
	return counted
}

// ruleCost is what a cached rule with this RRULE text and held onsets is
// charged against the cache's budget.
func ruleCost(rrule string, held int) int {
	return onsetRuleOverhead + (len(rrule)+onsetBytes-1)/onsetBytes + held
}

// latest answers from the rule's listings, extending them to before and some
// way past where they can. listed is false when none answers before, which the
// caller then walks the rule for.
//
// A listing from the DTSTART keeps at most listLimit onsets, which for a rule
// denser than yearly from Exchange's 1601 DTSTART stops short of the present.
// Past it, a rule without COUNT is walked back from the instant in a few
// microseconds, so a listing that stops before the present is dropped and the
// rule held at the cost of its text; with many such rules the cache would
// otherwise list and evict them in turn. A counted rule can only be walked
// forward from its DTSTART, so its instants past the listing are answered from
// a window listed from shortly before the first of them, replaced when a later
// instant passes it; an instant before the window's start is walked.
//
// A listing that changed is charged before the rule's lock is let go, so two
// lookups growing it in turn cannot record their sizes out of order.
func (r *listedRule) latest(cache *timezoneOnsets, dtstart, before time.Time) (latest time.Time, found, listed bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.walked {
		return time.Time{}, false, false
	}
	// A new listing can start out holding the DTSTART, which the cache has not
	// yet counted.
	previous := 0
	if r.lister == nil {
		r.lister = ical.NewRecurrenceStartLister(dtstart, r.key.rrule)
	} else {
		previous = r.listed()
	}
	// listing reports that this lookup listed, replaced or dropped a listing,
	// which marks the rule most recently used again even where it ends up
	// holding what it held before.
	listing := false
	defer func() {
		if listing || r.listed() != previous {
			cache.charge(r)
		}
	}()
	horizon := before.AddDate(onsetListingHorizonYears, 0, 0)
	windowHeld := 0
	if r.window != nil {
		windowHeld = r.window.Len()
	}
	if !r.lister.Covers(before) {
		listing = true
		r.lister.ListThrough(horizon, cache.listLimit(r, windowHeld))
	}
	if latest, found, listed = r.lister.LatestOnOrBefore(before); listed || r.windowless {
		return latest, found, listed
	}
	if !countedRule(r.key.rrule) {
		if !r.lister.Covers(time.Now()) {
			r.lister, r.walked = nil, true
		}
		return time.Time{}, false, false
	}
	if r.window != nil {
		if !r.window.Covers(before) {
			listing = true
			r.window.ListThrough(horizon, cache.listLimit(r, r.lister.Len()))
		}
		if latest, found, listed = r.window.LatestOnOrBefore(before); listed || r.window.Covers(before) {
			return latest, found, listed
		}
	}
	listing = true
	cache.relists.Add(1)
	r.window = ical.NewRecurrenceStartListerFrom(dtstart, r.key.rrule, before.AddDate(-onsetListingMarginYears, 0, 0))
	r.window.ListThrough(horizon, cache.listLimit(r, r.lister.Len()))
	if latest, found, listed = r.window.LatestOnOrBefore(before); !listed {
		r.window, r.windowless = nil, true
	}
	return latest, found, listed
}
