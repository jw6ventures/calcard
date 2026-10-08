package ical

import (
	"slices"
	"sort"
	"sync"
	"time"
)

// IndexedTimezoneObservances is a definition read for many lookups. It answers
// OffsetAtWall and OffsetAtInstant exactly as TimezoneObservances does, but
// sorts the onsets the observances state outright -- each DTSTART and RDATE --
// once per scale, so a lookup searches them rather than reading every
// observance. The onsets an RRULE generates are asked of the rule, through the
// ObservanceOnsets the caller passes, as TimezoneObservances asks them. It is
// safe for concurrent use.
type IndexedTimezoneObservances struct {
	TimezoneObservances

	wall, instant observanceScale
}

// IndexTimezoneObservances prepares observances for many lookups.
func IndexTimezoneObservances(observances TimezoneObservances) *IndexedTimezoneObservances {
	return &IndexedTimezoneObservances{TimezoneObservances: observances}
}

// OffsetAtWall is TimezoneObservances.OffsetAtWall. A nil definition reads no
// wall clock.
func (x *IndexedTimezoneObservances) OffsetAtWall(wall time.Time, onsets ObservanceOnsets) (time.Duration, bool) {
	if x == nil {
		return 0, false
	}
	return x.wall.offsetBefore(x.TimezoneObservances, wall, wallScaleShift, onsets)
}

// OffsetAtInstant is TimezoneObservances.OffsetAtInstant. A nil definition
// reads no instant.
func (x *IndexedTimezoneObservances) OffsetAtInstant(instant time.Time, onsets ObservanceOnsets) (time.Duration, bool) {
	if x == nil {
		return 0, false
	}
	return x.instant.offsetBefore(x.TimezoneObservances, instant, instantScaleShift, onsets)
}

// observanceScale is the onsets of a definition placed on the scale one kind
// of lookup measures its bound on, listed on first use.
//
// TimezoneObservances.offsetBefore reads the transitions in the order the
// observances list them -- each one's DTSTART, its RDATEs, then its RRULEs --
// and keeps the latest at or before the bound, the first read of several at
// one instant. Each transition carries its place in that order, so the stated
// ones can be sorted by instant and the first of a tie still found.
type observanceScale struct {
	once sync.Once
	// indexed is false when the scale cannot be searched with the same
	// result the scan gives, and every lookup scans.
	indexed bool
	// stated holds the DTSTART and RDATE transitions in ascending instant,
	// the first read of a tie last, so the last one at or before a bound is
	// the one the scan keeps.
	stated []indexedTransition
	// rules holds each RRULE in the order the scan reads them.
	rules []indexedRule
	// earliest is the first transition of the earliest observance, whose
	// TZOFFSETFROM answers a bound before every transition.
	earliest           time.Time
	earliestOffsetFrom time.Duration
}

type indexedTransition struct {
	at       time.Time
	order    int
	offsetTo time.Duration
}

type indexedRule struct {
	start    time.Time
	rrule    string
	shift    time.Duration
	order    int
	offsetTo time.Duration
}

func wallScaleShift(observance TimezoneObservance) time.Duration {
	return max(0, observance.OffsetTo-observance.OffsetFrom)
}

func instantScaleShift(observance TimezoneObservance) time.Duration {
	return -observance.OffsetFrom
}

func (s *observanceScale) build(observances TimezoneObservances, shift func(TimezoneObservance) time.Duration) {
	// The scan takes a transition at the zero time for no transition at
	// all, which no ordering by instant reproduces, so a definition placing
	// one there is scanned.
	s.indexed = true
	order := 0
	for _, observance := range observances {
		toScale := shift(observance)
		first := observance.Start.Add(toScale)
		if s.earliest.IsZero() || first.Before(s.earliest) {
			s.earliest, s.earliestOffsetFrom = first, observance.OffsetFrom
		}
		state := func(onset time.Time) {
			at := onset.Add(toScale)
			if at.IsZero() {
				s.indexed = false
			}
			s.stated = append(s.stated, indexedTransition{at: at, order: order, offsetTo: observance.OffsetTo})
			order++
		}
		state(observance.Start)
		for _, onset := range observance.RDates {
			state(onset)
		}
		for _, rule := range observance.RRules {
			// A rule's onsets are its DTSTART and those after it, so they
			// reach the zero time only from a DTSTART at or before it.
			if !first.After(time.Time{}) {
				s.indexed = false
			}
			s.rules = append(s.rules, indexedRule{start: observance.Start, rrule: rule, shift: toScale, order: order, offsetTo: observance.OffsetTo})
			order++
		}
	}
	slices.SortFunc(s.stated, func(a, b indexedTransition) int {
		if c := a.at.Compare(b.at); c != 0 {
			return c
		}
		return b.order - a.order
	})
}

func (s *observanceScale) offsetBefore(observances TimezoneObservances, bound time.Time, shift func(TimezoneObservance) time.Duration, onsets ObservanceOnsets) (time.Duration, bool) {
	s.once.Do(func() { s.build(observances, shift) })
	if !s.indexed {
		return observances.offsetBefore(bound, shift, onsets)
	}
	var latest indexedTransition
	found := false
	if after := sort.Search(len(s.stated), func(i int) bool { return s.stated[i].at.After(bound) }); after > 0 {
		latest, found = s.stated[after-1], true
	}
	for _, rule := range s.rules {
		onset, generated, complete := latestOnset(onsets, rule.start, bound.Add(-rule.shift), rule.rrule)
		if !complete {
			return 0, false
		}
		if !generated {
			continue
		}
		at := onset.Add(rule.shift)
		if at.After(bound) {
			continue
		}
		if !found || at.After(latest.at) || (at.Equal(latest.at) && rule.order < latest.order) {
			latest, found = indexedTransition{at: at, order: rule.order, offsetTo: rule.offsetTo}, true
		}
	}
	if found {
		return latest.offsetTo, true
	}
	if !s.earliest.IsZero() {
		return s.earliestOffsetFrom, true
	}
	return 0, false
}
