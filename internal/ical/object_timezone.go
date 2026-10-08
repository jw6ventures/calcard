package ical

import (
	"slices"
	"time"
)

// ObjectTimezone is the zone a TZID names within one calendar object, read as
// the DAV layer's matcher reads it: through the VTIMEZONE the object ships for
// it, unless that is a definition the server wrote for a zone the host
// defines (TimezoneComponent.HostDefinition), and otherwise through the host's zone of that name. It is not safe
// for concurrent use.
type ObjectTimezone struct {
	definition tzidDefinition
}

// ResolveObjectTimezone resolves tzid within the calendar object whose
// unfolded content lines are lines. With no lines it is the host's zone of
// that name.
func ResolveObjectTimezone(lines []string, tzid string) ObjectTimezone {
	return ObjectTimezone{definition: resolveTZID(lines, tzid)}
}

// Defined reports whether the object or the host defines the zone.
func (z ObjectTimezone) Defined() bool {
	return z.definition.observances != nil || z.definition.host != nil
}

// Instant is the instant a wall-clock reading in the zone names, held as the
// UTC time with the same fields as ParseDateTime returns it. A reading a
// transition skips is read with the offset before the gap, and one it repeats
// is its first occurrence (RFC 5545 §3.3.5). A zone neither the object nor the
// host defines reads the wall clock as UTC.
func (z ObjectTimezone) Instant(wall time.Time) time.Time {
	return z.definition.instant(wall)
}

// ZoneOffset is the UTC offset a zone keeps from the instant From on.
type ZoneOffset struct {
	From   time.Time
	Offset time.Duration
}

// Offsets lists the offsets the object's own definition of the zone keeps
// from from through through, as OffsetAtInstant reads them: the one in force
// at from, then one for each transition after from and at or before through
// that changes it, in order. ok is false when the zone is not read through the
// object's own definition, or its rules cannot be followed that far.
func (z ObjectTimezone) Offsets(from, through time.Time) (offsets []ZoneOffset, ok bool) {
	observances, onsets := z.definition.observances, z.definition.onsets
	if observances == nil {
		return nil, false
	}
	initial, ok := observances.OffsetAtInstant(from, onsets)
	if !ok {
		return nil, false
	}
	var transitions []ZoneOffset
	for _, observance := range observances.TimezoneObservances {
		// Onsets are wall clocks in the offset the observance leaves.
		add := func(onset time.Time) {
			if at := onset.Add(-observance.OffsetFrom); at.After(from) && !at.After(through) {
				transitions = append(transitions, ZoneOffset{From: at, Offset: observance.OffsetTo})
			}
		}
		add(observance.Start)
		for _, onset := range observance.RDates {
			add(onset)
		}
		after := from.Add(observance.OffsetFrom)
		for _, rule := range observance.RRules {
			before := through.Add(observance.OffsetFrom)
			for listed := 0; ; listed++ {
				if listed == maxListedObservanceOnsets {
					return nil, false
				}
				onset, found, complete := latestOnset(onsets, observance.Start, before, rule)
				if !complete {
					return nil, false
				}
				if !found || !onset.After(after) || !onset.After(observance.Start) {
					break
				}
				add(onset)
				before = onset.Add(-time.Nanosecond)
			}
		}
	}
	// Of transitions at one instant the first observance's governs, as
	// OffsetAtInstant reads them.
	slices.SortStableFunc(transitions, func(a, b ZoneOffset) int { return a.From.Compare(b.From) })
	offsets = []ZoneOffset{{From: from, Offset: initial}}
	for i, transition := range transitions {
		if i > 0 && transition.From.Equal(transitions[i-1].From) {
			continue
		}
		if transition.Offset != offsets[len(offsets)-1].Offset {
			offsets = append(offsets, transition)
		}
	}
	return offsets, true
}
