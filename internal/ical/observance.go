package ical

import (
	"strings"
	"time"
)

// TimezoneObservance is one STANDARD or DAYLIGHT component of a VTIMEZONE
// (RFC 5545 §3.6.5). Its onsets -- the DTSTART, each RDATE, and the starts each
// RRULE generates from the DTSTART -- are wall clocks in the offset the
// observance leaves, OffsetFrom, held as UTC fields.
type TimezoneObservance struct {
	Start      time.Time
	OffsetFrom time.Duration
	OffsetTo   time.Duration
	RDates     []time.Time
	// RRules holds each RRULE, an UNTIL naming a moment restated on the
	// onsets' scale per observanceRuleOnWallClock.
	RRules []string
}

// TimezoneObservances are the observances of one VTIMEZONE, which together
// define its zone.
type TimezoneObservances []TimezoneObservance

// ObservanceOnsets answers what LatestRecurrenceOnOrBefore answers for an
// observance's DTSTART and RRULE, typically from listings shared across the
// many objects that carry the same rules. A nil ObservanceOnsets walks the rule
// for every question.
type ObservanceOnsets interface {
	LatestOnOrBefore(dtstart, before time.Time, rrule string) (latest time.Time, found, complete bool)
}

func latestOnset(onsets ObservanceOnsets, dtstart, before time.Time, rrule string) (time.Time, bool, bool) {
	if onsets == nil {
		return LatestRecurrenceOnOrBefore(dtstart, before, rrule)
	}
	return onsets.LatestOnOrBefore(dtstart, before, rrule)
}

// onsetListings answers onset questions for one evaluation from a listing of
// each rule, so resolving many wall clocks through a definition walks each of
// its rules once rather than once per wall clock. A rule its listing cannot
// reach is walked, as LatestRecurrenceOnOrBefore walks it.
type onsetListings map[onsetListingKey]*RecurrenceStartLister

type onsetListingKey struct {
	seconds     int64
	nanoseconds int
	rrule       string
}

// onsetListingHorizonYears is how far past the wall clock that asked a rule's
// onsets are listed, so the wall clocks after it -- one evaluation asks about
// a series' instances in ascending order -- are answered from the same
// listing, and a listing is extended from where it stopped.
const onsetListingHorizonYears = 1

func (l onsetListings) LatestOnOrBefore(dtstart, before time.Time, rrule string) (time.Time, bool, bool) {
	if before.Before(dtstart) {
		return LatestRecurrenceOnOrBefore(dtstart, before, rrule)
	}
	key := onsetListingKey{seconds: dtstart.Unix(), nanoseconds: dtstart.Nanosecond(), rrule: rrule}
	lister, ok := l[key]
	if !ok {
		lister = NewRecurrenceStartLister(dtstart, rrule)
		l[key] = lister
	}
	if !lister.Covers(before) {
		lister.ListThrough(before.AddDate(onsetListingHorizonYears, 0, 0), maxListedObservanceOnsets)
	}
	if latest, found, listed := lister.LatestOnOrBefore(before); listed {
		return latest, found, true
	}
	return LatestRecurrenceOnOrBefore(dtstart, before, rrule)
}

// ReadTimezoneObservances reads the observances of one VTIMEZONE, each given as
// the content lines of its STANDARD or DAYLIGHT component. ok is false when
// there is no observance, or one lacks a DTSTART or a UTC offset this can read,
// or names an onset that is not a DATE-TIME: no zone can be followed through
// such a definition.
func ReadTimezoneObservances(components [][]PropertyValue) (TimezoneObservances, bool) {
	if len(components) == 0 {
		return nil, false
	}
	observances := make(TimezoneObservances, 0, len(components))
	for _, properties := range components {
		observance, ok := readTimezoneObservance(properties)
		if !ok {
			return nil, false
		}
		observances = append(observances, observance)
	}
	return observances, true
}

// readTimezoneObservance reads one observance. Of DTSTART, TZOFFSETFROM and
// TZOFFSETTO the first is read; every RDATE and RRULE is.
func readTimezoneObservance(properties []PropertyValue) (TimezoneObservance, bool) {
	var start, offsetFrom, offsetTo *PropertyValue
	var observance TimezoneObservance
	for i := range properties {
		property := &properties[i]
		switch name := PropertyName(property.KeyPart); {
		case strings.EqualFold(name, "DTSTART"):
			if start == nil {
				start = property
			}
		case strings.EqualFold(name, "TZOFFSETFROM"):
			if offsetFrom == nil {
				offsetFrom = property
			}
		case strings.EqualFold(name, "TZOFFSETTO"):
			if offsetTo == nil {
				offsetTo = property
			}
		case strings.EqualFold(name, "RDATE"):
			for _, value := range strings.Split(property.Value, ",") {
				onset, err := ParseDateTime(strings.TrimSpace(value))
				if err != nil {
					return TimezoneObservance{}, false
				}
				observance.RDates = append(observance.RDates, onset)
			}
		}
	}
	if start == nil || offsetFrom == nil || offsetTo == nil {
		return TimezoneObservance{}, false
	}
	var err error
	var okFrom, okTo bool
	if observance.Start, err = ParseDateTime(strings.TrimSpace(start.Value)); err != nil {
		return TimezoneObservance{}, false
	}
	observance.OffsetFrom, okFrom = ParseUTCOffset(offsetFrom.Value)
	observance.OffsetTo, okTo = ParseUTCOffset(offsetTo.Value)
	if !okFrom || !okTo {
		return TimezoneObservance{}, false
	}
	for _, property := range properties {
		if strings.EqualFold(PropertyName(property.KeyPart), "RRULE") {
			observance.RRules = append(observance.RRules, observanceRuleOnWallClock(property.Value, observance.OffsetFrom))
		}
	}
	return observance, true
}

// observanceRuleOnWallClock restates an observance RRULE's UNTIL on the scale
// its onsets are generated on. Onsets are wall clocks in the offset each one
// leaves, TZOFFSETFROM (RFC 5545 §3.6.5), while a UTC UNTIL names a moment
// (§3.3.10), so the last onset is the one whose wall clock is at or before
// UNTIL read in TZOFFSETFROM. Written as a floating value, UNTIL is compared on
// the onsets' own scale wherever the rule is expanded, and the rule is listed
// and cached under the text that says where it ends. An UNTIL that names no
// moment is already on that scale, and the rule is returned as it is.
func observanceRuleOnWallClock(rrule string, offsetFrom time.Duration) string {
	start := 0
	for start <= len(rrule) {
		part, _, _ := strings.Cut(rrule[start:], ";")
		name, value, found := strings.Cut(part, "=")
		if !found || !strings.EqualFold(strings.TrimSpace(name), "UNTIL") {
			start += len(part) + 1
			continue
		}
		value = strings.ToUpper(strings.TrimSpace(value))
		if !strings.Contains(value, "T") || !hasZoneSuffix(value) {
			return rrule
		}
		until, err := ParseDateTime(value)
		if err != nil {
			return rrule
		}
		valueStart := start + len(name) + 1
		return rrule[:valueStart] + until.Add(offsetFrom).Format("20060102T150405") + rrule[start+len(part):]
	}
	return rrule
}

// ParseUTCOffset reads a UTC-OFFSET value (RFC 5545 §3.3.14), which does not
// admit "-0000" or "-000000".
func ParseUTCOffset(value string) (time.Duration, bool) {
	value = strings.TrimSpace(value)
	if len(value) != 5 && len(value) != 7 {
		return 0, false
	}
	sign := time.Duration(1)
	switch value[0] {
	case '+':
	case '-':
		sign = -1
	default:
		return 0, false
	}
	if !isDigits(value[1:]) {
		return 0, false
	}
	hours := time.Duration(value[1]-'0')*10 + time.Duration(value[2]-'0')
	minutes := time.Duration(value[3]-'0')*10 + time.Duration(value[4]-'0')
	var seconds time.Duration
	if len(value) == 7 {
		seconds = time.Duration(value[5]-'0')*10 + time.Duration(value[6]-'0')
	}
	if hours > 23 || minutes > 59 || seconds > 59 {
		return 0, false
	}
	offset := hours*time.Hour + minutes*time.Minute + seconds*time.Second
	if sign < 0 && offset == 0 {
		return 0, false
	}
	return sign * offset, true
}

// OffsetAtWall resolves the offset a wall-clock reading in the zone is read
// with. A reading a transition repeats is its first occurrence, which the
// offset the transition leaves gives, and RFC 5545 §3.3.5 reads one it skips
// with the offset before the gap, so a transition that moves the clock forward
// governs readings only from the end of its gap, TZOFFSETTO less TZOFFSETFROM
// past the value it is stated at.
func (o TimezoneObservances) OffsetAtWall(wall time.Time, onsets ObservanceOnsets) (time.Duration, bool) {
	return o.offsetBefore(wall, wallScaleShift, onsets)
}

// OffsetAtInstant resolves the offset in force at an absolute instant. A
// transition happens at the moment its outgoing offset, TZOFFSETFROM, makes
// the wall clock it is stated at.
func (o TimezoneObservances) OffsetAtInstant(instant time.Time, onsets ObservanceOnsets) (time.Duration, bool) {
	return o.offsetBefore(instant, instantScaleShift, onsets)
}

// offsetBefore is the TZOFFSETTO of the last transition at or before bound, or
// the TZOFFSETFROM of the first one when bound precedes them all -- a moment
// before a zone's earliest observance is still in the offset that observance
// says it is moving away from.
//
// shift is how far an observance's transitions sit from the scale bound is
// measured on. It applies to every RDATE and RRULE onset as well, so an
// observance is judged the same way however its transitions are spelled.
func (o TimezoneObservances) offsetBefore(bound time.Time, shift func(TimezoneObservance) time.Duration, onsets ObservanceOnsets) (time.Duration, bool) {
	var latest, earliest time.Time
	var latestOffset, earliestOffsetFrom time.Duration
	for _, observance := range o {
		toBoundScale := shift(observance)
		first := observance.Start.Add(toBoundScale)
		if earliest.IsZero() || first.Before(earliest) {
			earliest, earliestOffsetFrom = first, observance.OffsetFrom
		}
		consider := func(candidate time.Time) {
			transition := candidate.Add(toBoundScale)
			if transition.After(bound) {
				return
			}
			if latest.IsZero() || transition.After(latest) {
				latest, latestOffset = transition, observance.OffsetTo
			}
		}
		consider(observance.Start)
		for _, candidate := range observance.RDates {
			consider(candidate)
		}
		for _, rule := range observance.RRules {
			// The rule generates local values, so it is walked up to the bound
			// moved onto that same scale.
			candidate, found, complete := latestOnset(onsets, observance.Start, bound.Add(-toBoundScale), rule)
			if !complete {
				// The rule describes more than this server will generate, so
				// which observance is in force cannot be established. Answering
				// from the transitions that did resolve would apply an offset
				// this definition does not support.
				return 0, false
			}
			if found {
				consider(candidate)
			}
		}
	}
	if !latest.IsZero() {
		return latestOffset, true
	}
	if !earliest.IsZero() {
		return earliestOffsetFrom, true
	}
	return 0, false
}

// Skips reports whether the zone skips the wall-clock reading wall: whether
// it falls in the gap an observance that moves the clock forward opens, which
// runs from each of its onsets for TZOFFSETTO less TZOFFSETFROM (RFC 5545
// §3.6.5). known is false when an observance's onsets cannot be followed as
// far as wall. It answers from the same onsets Gaps lists.
func (o TimezoneObservances) Skips(wall time.Time, onsets ObservanceOnsets) (skipped, known bool) {
	for _, observance := range o {
		length := observance.OffsetTo - observance.OffsetFrom
		if length <= 0 {
			continue
		}
		var latest time.Time
		consider := func(onset time.Time) {
			if !onset.After(wall) && (latest.IsZero() || onset.After(latest)) {
				latest = onset
			}
		}
		consider(observance.Start)
		for _, onset := range observance.RDates {
			consider(onset)
		}
		for _, rule := range observance.RRules {
			onset, found, complete := latestOnset(onsets, observance.Start, wall, rule)
			if !complete {
				return false, false
			}
			if found {
				consider(onset)
			}
		}
		if !latest.IsZero() && wall.Before(latest.Add(length)) {
			return true, true
		}
	}
	return false, true
}

// maxListedObservanceOnsets bounds the onsets one rule may list for one call
// to Gaps, and the onsets onsetListings keeps for one rule. A rule whose
// transitions come closer together than any zone's do would otherwise list
// without end.
const maxListedObservanceOnsets = 2048

// Gaps lists the gaps the observances that move the clock forward open --
// the stretches of local time Skips reports skipped -- that start after after
// and at or before through, both wall clocks held as UTC fields. It reads each
// rule's onsets through onsets, as Skips does, so the two agree on every wall
// clock. ok is false when an observance's onsets cannot be followed that far.
func (o TimezoneObservances) Gaps(after, through time.Time, onsets ObservanceOnsets) (gaps []LocalTimeGap, ok bool) {
	for _, observance := range o {
		length := observance.OffsetTo - observance.OffsetFrom
		if length <= 0 {
			continue
		}
		add := func(onset time.Time) {
			if onset.After(after) && !onset.After(through) {
				gaps = append(gaps, LocalTimeGap{Start: onset, Length: length})
			}
		}
		add(observance.Start)
		for _, onset := range observance.RDates {
			add(onset)
		}
		for _, rule := range observance.RRules {
			// The rule's onsets are found from the latest backward, and the
			// DTSTART, the first of them, is among the observance's own.
			before := through
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
	return gaps, true
}
