package ical

import (
	"strings"
	"sync"
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

// ObservanceOnsets answers questions about an observance's DTSTART and RRULE
// from listings of the rule, typically shared across the many objects that
// carry the same rules. LatestOnOrBefore answers what
// LatestRecurrenceOnOrBefore does, and StartsBetween lists the rule's starts
// after after and at or before through, leaving out the DTSTART; the caller
// must not modify them. listed is false when no listing holds the answer; the
// reader then follows the rule itself, on the ObservanceWork of the object it
// is reading, so what following one object's definitions costs that reading
// is bounded by the reading.
//
// work is the ObservanceWork of the reading asking. Building or extending a
// listing to answer is charged to it, and through it to the request pool, so a
// listing costs the reading that causes it and a lookup a listing already
// answers costs nothing. Each build or extension is one evaluation of the rule,
// capped at what one evaluation may spend. A listing that cannot be built or
// extended on what work has left is not held against the rule: a later reading
// with budget of its own lists it, and this question is answered
// listed == false.
type ObservanceOnsets interface {
	LatestOnOrBefore(dtstart, before time.Time, rrule string, work *ObservanceWork) (latest time.Time, found, listed bool)
	StartsBetween(dtstart, after, through time.Time, rrule string, work *ObservanceWork) (starts []time.Time, listed bool)
}

// observanceWalkLimit bounds the work one reading of one object may spend
// following VTIMEZONE observance rules where no listing answers them, summed
// over every rule of every definition the object holds; no one rule may take
// more than recurrenceWorkLimit of it. Walking back from an instant costs a
// rule real zones use a few thousand units. A rule too sparse for a listing to
// reach the present, the 29th of February when it is a Monday, costs about
// 30,000 a lookup, and a definition of 200 such observances, each from a
// DTSTART of its own, about 5 million for the 200; the limit admits that with
// three times to spare. A definition of a thousand of them spends the whole
// limit and is refused after about 2 seconds. A single rule dense enough to
// cost more than recurrenceWorkLimit, a transition every second, is given up
// on after about a quarter of a second.
const observanceWalkLimit = 16_000_000

// observanceListingLimit bounds the work one reading may spend building and
// extending listings of its definitions' rules, and onsetListingWorkLimit what
// one build or extension may spend. A rule real zones use lists from Exchange's
// 1601 DTSTART through the present in a few thousand units, and a rule too
// sparse or too dense for a listing to be worth making is found out in the
// first fifty thousand: a definition of 200 observances, each from a DTSTART
// of its own, costs the reading ten million in listings that are not made, and
// is walked rule by rule instead. A listing is shared by every later reading
// of the rule, so what a rule costs here is paid once.
const (
	observanceListingLimit = 16_000_000
	onsetListingWorkLimit  = 50_000
)

// ObjectWorkBudget is the pool one write validating a single object, or one
// member of a collection transfer, draws on: the walks and listings of the
// object's zones, and the walk of its recurrence rule beside them. The write
// reads the object twice, the parsed tree's reading and the ObjectTimes the
// analysis goes through, each with observanceWalkLimit of walks of its own, so
// the pool holds both: a pool smaller than the readings' limits together would
// end one of them for want of work it was entitled to, and an object whose
// answer does not depend on the pool would be refused for it.
const ObjectWorkBudget = 2*observanceWalkLimit + observanceListingLimit + recurrenceWorkLimit

// ObservanceWork is the work one reading of a calendar object may spend on its
// VTIMEZONE observance rules, in two parts.
//
// Walks follow a rule where no listing answers: every walk, and every pass over
// a span, through any of the definitions the reading holds draws on
// observanceWalkLimit, and the walks of any one rule on no more than one
// evaluation may spend. A definition whose rules are costly to follow -- one
// generating a transition every second costs a few milliseconds a walk, and a
// series asks for one per instance -- costs the reading at most what one
// LatestRecurrenceOnOrBefore may spend for each such rule. Once that runs out,
// a question no listing answers is incomplete, as a walk that ran out of work
// is, and the reader falls back as it does for any definition that cannot be
// followed.
//
// Listings are built and extended where a lookup finds none that answers: each
// is one evaluation of its rule, capped at onsetListingWorkLimit, and all of
// them draw on observanceListingLimit. A question a listing answers costs
// nothing, and the listings answer every question about the rules real zones
// use.
//
// Both parts also draw on the pool of the request the reading answers, when
// there is one. Create one with NewObservanceWork; it is safe for concurrent
// use.
type ObservanceWork struct {
	mu       sync.Mutex
	walks    recurrenceWork
	listings recurrenceWork
	// ruleWalks is what the walks of each rule have spent so far.
	ruleWalks map[onsetListingKey]int
	// walkStarved reports that a walk was refused for want of the request's
	// pool, rather than for want of the reading's own walks.
	walkStarved bool
}

// Starved reports that a walk of one of the reading's rules was refused because
// the pool of its request had run out, not because the rule was too large. A
// question that walk would have answered was then answered as a definition that
// cannot be followed is -- for a zone the object defines, as UTC -- so the times
// the reading resolved may be wrong.
func (w *ObservanceWork) Starved() bool {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.walkStarved
}

// NewObservanceWork returns the budget for one reading of one object. Its walks
// and listings also draw on pool, when it is not nil, which bounds what every
// reading and evaluation of the same request spend together.
func NewObservanceWork(pool *RequestWork) *ObservanceWork {
	work := &ObservanceWork{
		walks:    recurrenceWork{remaining: observanceWalkLimit, limit: observanceWalkLimit},
		listings: recurrenceWork{remaining: observanceListingLimit, limit: observanceListingLimit},
	}
	if pool != nil {
		work.walks.source = pool
		work.listings.source = pool
	}
	return work
}

// useWalk runs follow on a budget for a walk of the rule key names, capped at
// what one evaluation may spend less what walks of the rule have spent, and
// drawn from the reading's walks, which it holds until follow returns. A nil
// budget gives follow one of its own.
func (w *ObservanceWork) useWalk(key onsetListingKey, follow func(*recurrenceWork)) {
	if w == nil {
		follow(newRecurrenceWork(nil))
		return
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	allowed := recurrenceWorkLimit - w.ruleWalks[key]
	evaluation := w.walks.evaluation(max(allowed, 0))
	if allowed <= 0 {
		evaluation.remaining = -1
	}
	defer evaluation.release()
	follow(evaluation)
	if used := evaluation.used(); used > 0 || evaluation.remaining < 0 {
		if w.ruleWalks == nil {
			w.ruleWalks = make(map[onsetListingKey]int)
		}
		w.ruleWalks[key] += used
		if evaluation.remaining < 0 && !evaluation.starved {
			// The rule asked for more than its allowance. A budget that has
			// refused once stays exhausted, so later lookups of the rule are
			// refused before they generate the candidates they would be
			// refused for.
			w.ruleWalks[key] = recurrenceWorkLimit
		}
	}
	w.refuse(&w.walks, evaluation)
	if evaluation.requestStarved {
		w.walkStarved = true
	}
}

// useListing is useWalk for building or extending a listing, capped at
// onsetListingWorkLimit and drawn from the reading's listings.
func (w *ObservanceWork) useListing(follow func(*recurrenceWork)) {
	if w == nil {
		follow(newRecurrenceWork(nil))
		return
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	evaluation := w.listings.evaluation(onsetListingWorkLimit)
	defer evaluation.release()
	follow(evaluation)
	w.refuse(&w.listings, evaluation)
}

// refuse spends budget when evaluation, drawn from it, was refused by it. A
// budget that has refused once stays exhausted, so later lookups are refused
// before they generate the candidates they would be refused for.
func (w *ObservanceWork) refuse(budget, evaluation *recurrenceWork) {
	if evaluation.starved {
		budget.remaining = -1
	}
}

// walk is LatestRecurrenceOnOrBefore drawing on the budget.
func (w *ObservanceWork) walk(dtstart, before time.Time, rrule string) (latest time.Time, found, complete bool) {
	w.useWalk(ruleKey(dtstart, rrule), func(work *recurrenceWork) {
		latest, found, complete = latestRecurrenceOnOrBefore(dtstart, before, rrule, work)
	})
	return latest, found, complete
}

// startsBetween is recurrenceStartsBetween drawing on the budget.
func (w *ObservanceWork) startsBetween(dtstart time.Time, rrule string, after, through time.Time, limit int) (starts []time.Time, ok bool) {
	w.useWalk(ruleKey(dtstart, rrule), func(work *recurrenceWork) {
		starts, ok = recurrenceStartsBetween(dtstart, rrule, after, through, limit, work)
	})
	return starts, ok
}

func ruleKey(dtstart time.Time, rrule string) onsetListingKey {
	return onsetListingKey{seconds: dtstart.Unix(), nanoseconds: dtstart.Nanosecond(), rrule: rrule}
}

// latestOnset answers from onsets where a listing does, and walks the rule on
// work where none does.
func latestOnset(onsets ObservanceOnsets, work *ObservanceWork, dtstart, before time.Time, rrule string) (time.Time, bool, bool) {
	if onsets != nil {
		if latest, found, listed := onsets.LatestOnOrBefore(dtstart, before, rrule, work); listed {
			return latest, found, true
		}
	}
	return work.walk(dtstart, before, rrule)
}

// startsBetween lists a rule's starts after after and at or before through,
// leaving out the DTSTART, from onsets where a listing holds them and on work
// otherwise. ok is false when they number more than maxListedObservanceOnsets
// -- a rule whose transitions come closer together than any zone's would
// otherwise list without end -- or the rule cannot be followed that far.
func startsBetween(onsets ObservanceOnsets, work *ObservanceWork, dtstart time.Time, rrule string, after, through time.Time) ([]time.Time, bool) {
	if onsets != nil {
		if starts, listed := onsets.StartsBetween(dtstart, after, through, rrule, work); listed {
			return starts, len(starts) <= maxListedObservanceOnsets
		}
	}
	return work.startsBetween(dtstart, rrule, after, through, maxListedObservanceOnsets)
}

// onsetListings answers onset questions for one reading from a listing of
// each rule, so resolving many wall clocks through a definition walks each of
// its rules once rather than once per wall clock. An instant past what a
// rule's listing can reach is left to the reader to walk. Building or
// extending a listing draws on the reading's work, not a budget of its own.
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

func (l onsetListings) LatestOnOrBefore(dtstart, before time.Time, rrule string, work *ObservanceWork) (time.Time, bool, bool) {
	if before.Before(dtstart) {
		return time.Time{}, false, true
	}
	key := onsetListingKey{seconds: dtstart.Unix(), nanoseconds: dtstart.Nanosecond(), rrule: rrule}
	lister, ok := l[key]
	if !ok {
		if lister = NewRecurrenceStartLister(dtstart, rrule, work); lister == nil {
			return time.Time{}, false, false
		}
		l[key] = lister
	}
	if !lister.Covers(before) {
		lister.ListThrough(before.AddDate(onsetListingHorizonYears, 0, 0), maxListedObservanceOnsets, work)
	}
	return lister.LatestOnOrBefore(before)
}

func (l onsetListings) StartsBetween(dtstart, after, through time.Time, rrule string, work *ObservanceWork) ([]time.Time, bool) {
	if through.Before(dtstart) {
		return nil, true
	}
	if _, _, listed := l.LatestOnOrBefore(dtstart, through, rrule, work); !listed {
		return nil, false
	}
	return l[onsetListingKey{seconds: dtstart.Unix(), nanoseconds: dtstart.Nanosecond(), rrule: rrule}].StartsBetween(after, through)
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
//
// Each walk of a rule no listing in onsets answers has a budget of its own;
// IndexedTimezoneObservances shares one across a reading's lookups.
func (o TimezoneObservances) OffsetAtWall(wall time.Time, onsets ObservanceOnsets) (time.Duration, bool) {
	return o.offsetBefore(wall, wallScaleShift, onsets, nil)
}

// OffsetAtInstant resolves the offset in force at an absolute instant. A
// transition happens at the moment its outgoing offset, TZOFFSETFROM, makes
// the wall clock it is stated at.
//
// Each walk of a rule no listing in onsets answers has a budget of its own.
func (o TimezoneObservances) OffsetAtInstant(instant time.Time, onsets ObservanceOnsets) (time.Duration, bool) {
	return o.offsetBefore(instant, instantScaleShift, onsets, nil)
}

// offsetBefore is the TZOFFSETTO of the last transition at or before bound, or
// the TZOFFSETFROM of the first one when bound precedes them all -- a moment
// before a zone's earliest observance is still in the offset that observance
// says it is moving away from.
//
// shift is how far an observance's transitions sit from the scale bound is
// measured on. It applies to every RDATE and RRULE onset as well, so an
// observance is judged the same way however its transitions are spelled.
func (o TimezoneObservances) offsetBefore(bound time.Time, shift func(TimezoneObservance) time.Duration, onsets ObservanceOnsets, work *ObservanceWork) (time.Duration, bool) {
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
			candidate, found, complete := latestOnset(onsets, work, observance.Start, bound.Add(-toBoundScale), rule)
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
// far as wall. It answers from the same onsets Gaps lists. Each walk of a rule
// no listing in onsets answers has a budget of its own.
func (o TimezoneObservances) Skips(wall time.Time, onsets ObservanceOnsets) (skipped, known bool) {
	return o.skips(wall, onsets, nil)
}

func (o TimezoneObservances) skips(wall time.Time, onsets ObservanceOnsets, work *ObservanceWork) (skipped, known bool) {
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
			onset, found, complete := latestOnset(onsets, work, observance.Start, wall, rule)
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
// to Gaps or ObjectTimezone.Offsets, and the onsets onsetListings keeps for one
// rule.
const maxListedObservanceOnsets = 2048

// Gaps lists the gaps the observances that move the clock forward open --
// the stretches of local time Skips reports skipped -- that start after after
// and at or before through, both wall clocks held as UTC fields. Each rule's
// onsets in that span are read at once, from a listing in onsets that holds
// them or else in one pass of the rule, rather than looked up one at a time:
// an onset Skips looks up is one of them, so the two agree on every wall
// clock, but a rule dense enough to reach maxListedObservanceOnsets would
// otherwise cost that many walks of it. ok is false when an observance's
// onsets cannot be followed that far. Each rule's pass has a budget of its
// own; IndexedTimezoneObservances draws them all from a reading's.
func (o TimezoneObservances) Gaps(after, through time.Time, onsets ObservanceOnsets) (gaps []LocalTimeGap, ok bool) {
	return o.gaps(after, through, onsets, nil)
}

func (o TimezoneObservances) gaps(after, through time.Time, onsets ObservanceOnsets, work *ObservanceWork) (gaps []LocalTimeGap, ok bool) {
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
			starts, ok := startsBetween(onsets, work, observance.Start, rule, after, through)
			if !ok {
				return nil, false
			}
			for _, onset := range starts {
				add(onset)
			}
		}
	}
	return gaps, true
}
