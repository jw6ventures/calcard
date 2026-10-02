package dav

import (
	"container/list"
	"strings"
	"sync"
	"sync/atomic"
	"time"
	"unsafe"

	"github.com/jw6ventures/calcard/internal/ical"
)

const (
	// maxListedOnsetsPerRule bounds the onsets kept for one observance rule. A
	// yearly rule from Exchange's 1601 DTSTART reaches past 2100 well inside
	// it; a denser rule keeps what fits, and an instant past that is asked of
	// the rule directly.
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

// timezoneOnsets is a least-recently-used cache of observance rule listings,
// bounded by the onsets it holds. An entry pushed out is listed again the next
// time it is asked for, so a burst of distinct rules costs listings rather than
// locking out the rules every other resource shares.
//
// A nil cache asks every question of the rule directly.
type timezoneOnsets struct {
	mu      sync.Mutex
	budget  int
	held    int
	rules   map[observanceRuleKey]*list.Element
	recency *list.List

	// lookups and walks count the questions asked and those the listings could
	// not answer, so tests can tell the work done apart from the time it took.
	lookups atomic.Int64
	walks   atomic.Int64
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
	// charged is what the cache has counted against its budget for the rule,
	// guarded by the cache's lock.
	charged int

	mu     sync.Mutex
	lister *ical.RecurrenceStartLister
}

func newTimezoneOnsets(budget int) *timezoneOnsets {
	return &timezoneOnsets{
		budget:  budget,
		rules:   make(map[observanceRuleKey]*list.Element),
		recency: list.New(),
	}
}

// latest answers what ical.LatestRecurrenceOnOrBefore answers for the same
// arguments, from a listing of the rule's onsets where one reaches before.
func (o *timezoneOnsets) latest(dtstart, before time.Time, rrule string) (latest time.Time, found, complete bool) {
	if o == nil || before.Before(dtstart) {
		return ical.LatestRecurrenceOnOrBefore(dtstart, before, rrule)
	}
	o.lookups.Add(1)
	if ruleCost(rrule, 0) > o.budget {
		// Caching the rule would push out everything else and still exceed
		// the budget on its own.
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
	o.mu.Lock()
	defer o.mu.Unlock()
	if element, ok := o.rules[key]; ok {
		o.recency.MoveToFront(element)
		return element.Value.(*listedRule)
	}
	// The RRULE was read out of a calendar object and shares its memory, so
	// the key the rule is cached under, and listed from, is a copy: holding
	// the original would keep the whole object.
	key.rrule = strings.Clone(key.rrule)
	rule := &listedRule{key: key}
	o.rules[key] = o.recency.PushFront(rule)
	o.chargeLocked(rule, 0)
	return rule
}

// charge records that rule now holds held onsets, and evicts the least
// recently used rules until the cache is back within its budget. A rule that
// was evicted while it was being listed is not charged again.
func (o *timezoneOnsets) charge(rule *listedRule, held int) {
	o.mu.Lock()
	defer o.mu.Unlock()
	if element, ok := o.rules[rule.key]; !ok || element.Value != rule {
		return
	}
	o.chargeLocked(rule, held)
}

func (o *timezoneOnsets) chargeLocked(rule *listedRule, held int) {
	cost := ruleCost(rule.key.rrule, held)
	o.held += cost - rule.charged
	rule.charged = cost
	for o.held > o.budget {
		oldest := o.recency.Back()
		if oldest == nil || oldest.Value == rule {
			return
		}
		evicted := o.recency.Remove(oldest).(*listedRule)
		delete(o.rules, evicted.key)
		o.held -= evicted.charged
	}
}

// ruleCost is what a cached rule with this RRULE text and held onsets is
// charged against the cache's budget.
func ruleCost(rrule string, held int) int {
	return onsetRuleOverhead + (len(rrule)+onsetBytes-1)/onsetBytes + held
}

// latest answers from the rule's listing, extending it to before and some way
// past where it can. listed is false when the listing cannot reach before.
//
// A listing that changed is charged to cache before the rule's lock is let go:
// two lookups that grew it one after the other would otherwise be free to
// record their sizes in the opposite order, leaving the cache counting the
// smaller listing while holding the larger.
func (r *listedRule) latest(cache *timezoneOnsets, dtstart, before time.Time) (latest time.Time, found, listed bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.lister == nil {
		r.lister = ical.NewRecurrenceStartLister(dtstart, r.key.rrule)
	}
	previous := r.lister.Len()
	if !r.lister.Covers(before) {
		r.lister.ListThrough(before.AddDate(onsetListingHorizonYears, 0, 0), maxListedOnsetsPerRule)
	}
	if held := r.lister.Len(); held != previous {
		cache.charge(r, held)
	}
	return r.lister.LatestOnOrBefore(before)
}
