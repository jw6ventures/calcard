// Package ical parses iCalendar content lines and expands recurrence rules
// (RRULE, RDATE, EXDATE, RECURRENCE-ID overrides). It has no knowledge of
// DAV or storage types; callers hand it raw iCalendar text and times.
package ical

import (
	"errors"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"
)

// BusyPeriod is one concrete busy interval of an event occurrence.
// RecurrenceID identifies its slot when the period came from a recurrence set.
type BusyPeriod struct {
	RecurrenceID time.Time
	Start        time.Time
	End          time.Time
}

// RecurrenceInstance is one generated occurrence of a recurrence set: the slot
// it occupies in the master's pattern, and where that occurrence actually
// falls. The two differ only when a RANGE=THISANDFUTURE override has moved it.
//
// The distinction is what RFC 5545 §3.8.4.4 needs: a RECURRENCE-ID names the
// slot rather than the moved time, so a caller writing one out cannot derive it
// from the occurrence's own DTSTART.
type RecurrenceInstance struct {
	RecurrenceID time.Time
	Start        time.Time
	End          time.Time
}

// RecurrenceSlot is one identity in a master's recurrence pattern together
// with its original and effective placement. GoverningRangeRecurrenceID names
// the RANGE=THISANDFUTURE override that supplied the effective placement.
// ExactOverride is true when a separate ordinary override replaces this slot.
type RecurrenceSlot struct {
	RecurrenceID               time.Time
	Original                   BusyPeriod
	Effective                  BusyPeriod
	Suppressed                 bool
	ExactOverride              bool
	GoverningRangeRecurrenceID time.Time
}

// recurrenceOccurrence is the internal form: the slot paired with the period
// the expansion actually produced for it.
type recurrenceOccurrence struct {
	recurrenceID  time.Time
	original      BusyPeriod
	period        BusyPeriod
	suppressed    bool
	exactOverride bool
	governing     time.Time
}

// PropertyTimeResolver resolves one date-valued content line to the absolute
// instant it names. It exists so a caller holding a timezone this package cannot
// see -- RFC 4791 §7.3 gives a report the request's CALDAV:timezone, else the
// collection's CALDAV:calendar-timezone -- resolves every date in a recurrence
// set the same way it resolved the DTSTART it passes in. A nil resolver means
// ParsePropertyDateTimeLocal, which reads a floating value as UTC.
type PropertyTimeResolver func(keyPart, value string) (time.Time, bool)

func (resolve PropertyTimeResolver) or(keyPart, value string) (time.Time, bool) {
	if resolve == nil {
		return ParsePropertyDateTimeLocal(keyPart, value)
	}
	return resolve(keyPart, value)
}

// recurrenceExpansion selects which component the recurrence set belongs to,
// how its dates resolve, and how an occurrence is judged to reach the requested
// window. Consumers need different answers to the last: free-busy wants the
// half-open overlap a busy period has, time-range matching needs an inclusive
// candidate set for the stricter §9.9 condition, and recurrence limiting needs
// both the original and effective placements, including suppressed slots.
type recurrenceExpansion struct {
	componentName     string
	includeOverrides  bool
	includeSuppressed bool
	maxInstances      int
	resolve           PropertyTimeResolver
	reaches           func(original, effective BusyPeriod, suppressed bool, rangeStart, rangeEnd time.Time) bool
}

// RecurringBusyPeriods expands a recurring event (RRULE, RDATE, EXDATE and
// RECURRENCE-ID overrides, including RANGE=THISANDFUTURE) into the concrete
// busy periods that overlap [rangeStart, rangeEnd). dtstart and duration are
// the event's resolved start and occurrence length; maxInstances caps the
// expansion and resolve reads the recurrence dates, per PropertyTimeResolver.
//
// Exhausting maxInstances returns ErrRecurrenceExpansionLimit, and exhausting
// the rule's scan work returns ErrRecurrenceScanLimit, which wraps it. Either
// comes with the periods collected before the budget ran out. Each of those
// reaches the requested range, but together they are neither a prefix of the
// set nor in any particular order: rule-generated instances are collected
// before RDATEs and overrides, so an interrupted rule leaves those out
// entirely, and a RANGE=THISANDFUTURE override can move an instance past a
// later one. A caller that owes the complete set -- free-busy does, since a
// dropped period publishes an hour as free that is not -- has to treat the
// error as fatal rather than publish them.
func RecurringBusyPeriods(raw string, dtstart time.Time, duration time.Duration, rangeStart, rangeEnd time.Time, maxInstances int, resolve PropertyTimeResolver) ([]BusyPeriod, error) {
	occurrences, err := expandRecurrenceSet(raw, dtstart, duration, rangeStart, rangeEnd, recurrenceExpansion{
		componentName:    "VEVENT",
		includeOverrides: true,
		maxInstances:     maxInstances,
		resolve:          resolve,
		reaches: func(_ BusyPeriod, effective BusyPeriod, suppressed bool, rangeStart, rangeEnd time.Time) bool {
			return !suppressed && periodOverlaps(effective.Start, effective.End, rangeStart, rangeEnd)
		},
	})
	if err != nil && !errors.Is(err, ErrRecurrenceExpansionLimit) {
		return nil, err
	}
	periods := make([]BusyPeriod, 0, len(occurrences))
	for _, occurrence := range occurrences {
		period := occurrence.period
		period.RecurrenceID = occurrence.recurrenceID
		periods = append(periods, period)
	}
	return periods, err
}

// RecurrenceInstances expands the recurrence set of the named component and
// returns every *generated* instance whose occurrence window touches
// [rangeStart, rangeEnd], each paired with the slot of the master's pattern it
// belongs to. Overridden instances are left out: RFC 4791 §9.7.1 scopes a
// comp-filter to each component of the resource, so an override is matched as
// the component it is rather than through its master's pattern. Both bounds are
// inclusive, because the caller re-applies the exact §9.9 condition and an
// exclusive bound here would drop an occurrence that starts precisely at the end
// of the range.
//
// Work budget exhaustion returns ErrRecurrenceExpansionLimit (or the
// ErrRecurrenceScanLimit wrapping it) alongside the instances collected before
// the budget ran out. Those are not a prefix of the set in ascending order --
// RDATE instances are collected after the rule's, and a RANGE=THISANDFUTURE
// override can reorder placements -- but each of them does reach the range, so
// a caller asking only whether the set reaches it can answer from them; one
// that owes the complete set has to treat the error as fatal.
func RecurrenceInstances(raw, componentName string, dtstart time.Time, duration time.Duration, rangeStart, rangeEnd time.Time, maxInstances int, resolve PropertyTimeResolver) ([]RecurrenceInstance, error) {
	occurrences, err := expandRecurrenceSet(raw, dtstart, duration, rangeStart, rangeEnd, recurrenceExpansion{
		componentName:    componentName,
		includeOverrides: false,
		maxInstances:     maxInstances,
		resolve:          resolve,
		reaches: func(_ BusyPeriod, effective BusyPeriod, suppressed bool, rangeStart, rangeEnd time.Time) bool {
			return !suppressed && periodTouches(effective.Start, effective.End, rangeStart, rangeEnd)
		},
	})
	if err != nil && !errors.Is(err, ErrRecurrenceExpansionLimit) {
		return nil, err
	}
	instances := make([]RecurrenceInstance, 0, len(occurrences))
	for _, occurrence := range occurrences {
		instances = append(instances, RecurrenceInstance{
			RecurrenceID: occurrence.recurrenceID,
			Start:        occurrence.period.Start,
			End:          occurrence.period.End,
		})
	}
	return instances, err
}

// RecurrenceSlots expands only the identities generated by the master. It
// retains suppressed slots and both placements so callers can decide which
// RANGE=THISANDFUTURE component impacts a requested window without reimplementing
// recurrence rules or override precedence. Work budget exhaustion returns
// ErrRecurrenceExpansionLimit.
func RecurrenceSlots(raw, componentName string, dtstart time.Time, duration time.Duration, rangeStart, rangeEnd time.Time, maxInstances int, resolve PropertyTimeResolver) ([]RecurrenceSlot, error) {
	occurrences, err := expandRecurrenceSet(raw, dtstart, duration, rangeStart, rangeEnd, recurrenceExpansion{
		componentName:     componentName,
		includeSuppressed: true,
		maxInstances:      maxInstances,
		resolve:           resolve,
		reaches: func(original, effective BusyPeriod, suppressed bool, rangeStart, rangeEnd time.Time) bool {
			return periodTouches(original.Start, original.End, rangeStart, rangeEnd) ||
				periodTouches(effective.Start, effective.End, rangeStart, rangeEnd)
		},
	})
	if err != nil {
		return nil, err
	}
	slots := make([]RecurrenceSlot, 0, len(occurrences))
	for _, occurrence := range occurrences {
		slots = append(slots, RecurrenceSlot{
			RecurrenceID:               occurrence.recurrenceID,
			Original:                   occurrence.original,
			Effective:                  occurrence.period,
			Suppressed:                 occurrence.suppressed,
			ExactOverride:              occurrence.exactOverride,
			GoverningRangeRecurrenceID: occurrence.governing,
		})
	}
	return slots, nil
}

func expandRecurrenceSet(raw string, dtstart time.Time, duration time.Duration, rangeStart, rangeEnd time.Time, expansion recurrenceExpansion) ([]recurrenceOccurrence, error) {
	// One unfold serves the master, the overrides and the RDATEs. This runs once
	// per candidate component of every resource a report considers, so a
	// per-reader scan of the payload would be paid that many times over.
	components := componentsNamedFromLines(UnfoldLines(raw), expansion.componentName)
	component := primaryOf(components)
	exdates := eventExDates(component, expansion.resolve)
	overrides := componentRecurrenceOverrides(components, duration, expansion.resolve)
	overrideIndex := newRecurrenceOverrideIndex(overrides)
	rdates := componentRDatePeriods(component, expansion.resolve)
	periodEnds := make(map[time.Time]time.Time)
	for _, rdate := range rdates {
		if !rdate.End.IsZero() {
			periodEnds[rdate.Start.UTC()] = rdate.End
		}
	}
	seen := make(map[string]struct{})
	periods := make([]recurrenceOccurrence, 0)
	var limitErr error
	// recurrenceID is the slot the occurrence belongs to, which a
	// RANGE=THISANDFUTURE shift moves the period away from. EXDATE and an
	// ordinary RECURRENCE-ID override both name that slot, not the shifted start.
	addPeriod := func(recurrenceID time.Time, original BusyPeriod, generated, applyExDates bool) {
		if limitErr != nil {
			return
		}
		if applyExDates && exdates.contains(recurrenceID) {
			return
		}
		if generated {
			if end, ok := periodEnds[recurrenceID.UTC()]; ok {
				original.End = end
			}
		}
		effective := original
		suppressed := false
		exactOverride := false
		var governing time.Time
		if generated {
			effective, suppressed, governing = overrideIndex.applyThisAndFuture(original)
			exactOverride = overrideIndex.replaces(recurrenceID)
			suppressed = suppressed || exactOverride
		}
		if suppressed && !expansion.includeSuppressed {
			return
		}
		if !expansion.reaches(original, effective, suppressed, rangeStart, rangeEnd) {
			return
		}
		key := recurrenceID.UTC().Format(time.RFC3339Nano)
		if _, ok := seen[key]; ok {
			return
		}
		if len(periods) >= expansion.maxInstances {
			limitErr = ErrRecurrenceExpansionLimit
			return
		}
		seen[key] = struct{}{}
		periods = append(periods, recurrenceOccurrence{
			recurrenceID:  recurrenceID,
			original:      original,
			period:        effective,
			suppressed:    suppressed,
			exactOverride: exactOverride,
			governing:     governing,
		})
	}

	if rrule := componentPropertyValue(component, "RRULE"); rrule != "" {
		scanStart, scanEnd := rangeStart, rangeEnd
		if overrideIndex.hasThisAndFuture() {
			if shift := overrideIndex.maxThisAndFutureShiftSeconds(); shift > 0 {
				scanStart = addSeconds(scanStart, -shift)
				scanEnd = addSeconds(scanEnd, shift)
			}
		}
		scanExpansion := expansion
		scanExpansion.reaches = func(_ BusyPeriod, effective BusyPeriod, _ bool, rangeStart, rangeEnd time.Time) bool {
			return periodTouches(effective.Start, effective.End, rangeStart, rangeEnd)
		}
		ok, err := rruleBusyPeriods(component, dtstart, duration, rrule, exdates, scanStart, scanEnd, scanExpansion, func(id time.Time, period BusyPeriod) bool {
			addPeriod(id, period, true, true)
			return limitErr != nil
		})
		if err != nil {
			// The occurrences already collected are kept for the same reason the
			// output budget keeps them: each one reaches the range, so a caller
			// asking whether the set reaches it can still answer. The RDATEs and
			// overrides below are never collected, which is why what is returned
			// is not a prefix of the set.
			return periods, err
		}
		if !ok {
			addPeriod(dtstart, BusyPeriod{Start: dtstart, End: dtstart.Add(duration)}, true, true)
		}
	} else if len(rdates) > 0 {
		addPeriod(dtstart, BusyPeriod{Start: dtstart, End: dtstart.Add(duration)}, true, true)
	}

	for _, rdate := range rdates {
		end := rdate.End
		if end.IsZero() {
			end = rdate.Start.Add(duration)
		}
		addPeriod(rdate.Start, BusyPeriod{Start: rdate.Start, End: end}, true, true)
	}

	if expansion.includeOverrides {
		for _, override := range overrides {
			if override.cancelled {
				continue
			}
			addPeriod(override.recurrenceID, override.period, false, false)
		}
	}

	// The periods collected before the budget ran out are returned with the
	// error rather than dropped. They are an arbitrary subset of the set rather
	// than a prefix, but every one of them reaches the range, so a caller testing
	// whether the set reaches it at all can answer from them; the callers that
	// owe a complete set discard them by refusing on the error.
	return periods, limitErr
}

type rdatePeriod struct {
	Start time.Time
	End   time.Time
}

func EventHasRecurrence(ical string) bool {
	return componentHasRecurrence(ical, "VEVENT")
}

// componentHasRecurrence reports whether the named component's master carries a
// recurrence pattern this package can expand into instances.
func componentHasRecurrence(ical, componentName string) bool {
	component := PrimaryComponent(ical, componentName)
	return componentPropertyValue(component, "RRULE") != "" ||
		len(componentRDatePeriods(component, nil)) > 0
}

// SupportedRecurrenceRule reports whether an RRULE value uses a frequency this
// package expands. An unsupported one is not an error: the caller keeps the
// resource rather than silently filtering it out. An empty value is a component
// with no rule to expand, which is trivially supported.
func SupportedRecurrenceRule(rrule string) bool {
	if strings.TrimSpace(rrule) == "" {
		return true
	}
	return supportedRecurrenceFreq(extractRRuleParam(rrule, "FREQ"))
}

// ValidRecurrenceRule reports whether value uses the recurrence grammar this
// package can expand. It is intentionally independent of a DTSTART timezone;
// callers that expand the rule parse it again with the DTSTART location.
func ValidRecurrenceRule(value string) bool {
	_, ok := parseRecurrenceRule(value, time.UTC, nil)
	return ok
}

// maxRecurrenceOverrideComponents bounds the RECURRENCE-ID components one
// resource may carry. A cancelled override adds no instance to the set, so
// CALDAV:max-instances does not bound them, yet every expansion of the resource
// reads each one; ten times the instance limit leaves room for a long-running
// series with many modified or cancelled occurrences.
const maxRecurrenceOverrideComponents = 10 * MaxRecurrenceInstances

// RecurrenceSetExceedsLimit counts the instance identities in the recurring
// VEVENT, VTODO, or VJOURNAL set carried by raw. The second result is false
// when a recurrence value cannot be parsed.
func RecurrenceSetExceedsLimit(raw string, limit int) (bool, bool) {
	components := topLevelComponents(raw, func(name string) bool {
		switch strings.ToUpper(strings.TrimSpace(name)) {
		case "VEVENT", "VTODO", "VJOURNAL":
			return true
		default:
			return false
		}
	})
	if len(components) == 0 {
		return false, true
	}

	var master *Component
	// overrides maps each overridden slot to whether its override is
	// STATUS:CANCELLED. A cancelled override replaces the instance it names
	// rather than adding one -- only EXDATE removes an instance (RFC 5545
	// §3.8.5.1) -- so it is counted as the generated slot it names, and one
	// naming no generated slot adds nothing.
	overrides := make(map[string]bool)
	overrideComponents := 0
	instances := make(map[string]struct{})
	for i := range components {
		component := &components[i]
		recurrenceID, hasRecurrenceID := ComponentProperty(component, "RECURRENCE-ID")
		if !hasRecurrenceID {
			if master == nil {
				master = component
			}
			continue
		}
		parsed, ok := ParsePropertyDateTimeLocal(recurrenceID.KeyPart, recurrenceID.Value)
		if !ok {
			return false, false
		}
		overrideComponents++
		key := recurrenceInstantKey(parsed)
		cancelled := strings.EqualFold(componentPropertyValue(component, "STATUS"), "CANCELLED")
		if previous, seen := overrides[key]; !seen || previous {
			overrides[key] = cancelled
		}
		if !cancelled {
			instances["override:"+key] = struct{}{}
		}
	}
	if overrideComponents > maxRecurrenceOverrideComponents {
		return true, true
	}
	if master == nil {
		return len(instances) > limit, true
	}

	excluded := make(map[string]struct{})
	for _, property := range componentProperties(master, "EXDATE") {
		values, ok := recurrencePropertyInstants(property)
		if !ok {
			return false, false
		}
		for _, value := range values {
			excluded[recurrenceInstantKey(value)] = struct{}{}
		}
	}
	add := func(value time.Time) bool {
		key := recurrenceInstantKey(value)
		if _, skip := excluded[key]; skip {
			return false
		}
		if cancelled, replaced := overrides[key]; replaced && !cancelled {
			return false
		}
		instances["generated:"+key] = struct{}{}
		return len(instances) > limit
	}

	dtstartProperty, hasDTStart := ComponentProperty(master, "DTSTART")
	var dtstart time.Time
	if hasDTStart {
		var ok bool
		dtstart, ok = ParsePropertyDateTimeLocal(dtstartProperty.KeyPart, dtstartProperty.Value)
		if !ok {
			return false, false
		}
	}

	hasRecurrence := false
	for _, property := range componentProperties(master, "RDATE") {
		hasRecurrence = true
		values, ok := recurrencePropertyInstants(property)
		if !ok {
			return false, false
		}
		for _, value := range values {
			if add(value) {
				return true, true
			}
		}
	}

	rrule := strings.TrimSpace(componentPropertyValue(master, "RRULE"))
	if rrule == "" {
		if hasDTStart {
			if add(dtstart) {
				return true, true
			}
		} else if !hasRecurrence {
			instances["master"] = struct{}{}
		}
		return len(instances) > limit, true
	}
	if !hasDTStart {
		return false, false
	}
	rule, ok := parseRecurrenceRule(rrule, dtstart.Location(), nil)
	if !ok {
		return false, false
	}
	if add(dtstart) {
		return true, true
	}
	if !recurrenceBySetPosMaySelect(dtstart, rule) {
		return len(instances) > limit, true
	}
	generatesStart, overBudget := recurrenceRuleGeneratesStart(dtstart, rule)
	if overBudget {
		return true, true
	}
	// countLimit is how many instances the rule itself contributes toward
	// COUNT once the DTSTART, counted above, has taken its place.
	countLimit := rule.Count
	if !generatesStart && rule.Count > 0 {
		if rule.Count == 1 {
			return len(instances) > limit, true
		}
		countLimit--
	}

	// A rule with neither COUNT nor UNTIL never stops, so the limit cannot be a
	// count of the whole set without refusing every "repeats weekly, no end
	// date" event -- the default shape of every major client. It is measured
	// instead over one year from DTSTART, which admits those rules and still
	// refuses the sub-daily ones whose expansion is genuinely expensive.
	// Expansion at read time is bounded separately, by the requested range.
	unbounded := rule.Count == 0 && rule.Until == nil
	horizon := dtstart.AddDate(unboundedRecurrenceHorizonYears, 0, 0)

	periodStart := recurrencePeriodStart(dtstart, rule)
	occurrences := 0
	for scanned := 0; scanned < recurrenceScanLimit; scanned++ {
		bounded, exceeded := false, false
		_, overBudget := visitRecurrenceCandidates(periodStart, dtstart, rule, func(current time.Time) bool {
			if current.Before(dtstart) {
				return false
			}
			if rule.Until != nil && current.After(*rule.Until) {
				bounded = true
				return true
			}
			if unbounded && current.After(horizon) {
				bounded = true
				return true
			}
			occurrences++
			if countLimit > 0 && occurrences > countLimit {
				bounded = true
				return true
			}
			if add(current) {
				exceeded = true
				return true
			}
			return false
		})
		// A period too large to generate is a period too large to be inside the
		// advertised instance limit, so it answers the question on its own.
		if overBudget || exceeded {
			return true, true
		}
		if bounded {
			return len(instances) > limit, true
		}
		next := advanceRecurrencePeriod(periodStart, rule)
		if !next.After(periodStart) {
			return len(instances) > limit, true
		}
		periodStart = next
		if rule.Until != nil && periodStart.After(*rule.Until) {
			return len(instances) > limit, true
		}
		if unbounded && periodStart.After(horizon) {
			return len(instances) > limit, true
		}
	}
	mayProduceCandidate := occurrences > 0 || recurrenceRuleMayProduceCandidate(dtstart, rule)
	if rule.Count > 0 && mayProduceCandidate {
		if rule.Count-len(excluded) > limit {
			return true, true
		}
	}
	if !mayProduceCandidate {
		return len(instances) > limit, true
	}
	countedRule := rule
	countedRule.Count = countLimit
	if exceeds, handled := finishSparseRecurrence(periodStart, dtstart, countedRule, &occurrences, add); handled {
		return exceeds, true
	}
	// The scan guard ran out before the horizon above was reached, so how many
	// instances this rule generates in a year is still unknown. Only a rule
	// dense enough to spend 100000 periods inside one year gets here, and for
	// those the conservative answer is the right one.
	if unbounded {
		return true, true
	}
	return len(instances) > limit, true
}

// recurrenceRuleMayProduceCandidate distinguishes a merely sparse bounded rule
// from one whose filters can never select a recurrence period. Gregorian dates
// repeat every 400 years. Reducing the recurrence interval against that cycle
// also accounts for sub-daily clock alignment without visiting every second in
// the cycle.
func recurrenceRuleMayProduceCandidate(dtstart time.Time, rule recurrenceRule) bool {
	if !recurrenceBySetPosMaySelect(dtstart, rule) {
		return false
	}
	const daysInGregorianCycle = int64(146097)
	base := recurrencePeriodStart(dtstart, rule)

	var subDailyResidues map[int64]struct{}
	var subDailyDivisor int64
	if _, subDaily := recurrenceFrequencySeconds(rule.Freq); subDaily {
		stepSeconds, ok := subDailyRecurrenceSeconds(rule)
		if !ok {
			return false
		}
		subDailyDivisor = greatestCommonDivisor(stepSeconds, daysInGregorianCycle*24*60*60)
		subDailyResidues = make(map[int64]struct{})
		for _, offset := range allowedSubDailyPeriodOffsets(rule) {
			subDailyResidues[positiveRemainder(offset, subDailyDivisor)] = struct{}{}
		}
		if len(subDailyResidues) == 0 {
			return false
		}
	}

	for year := 2000; year < 2400; year++ {
		for dayNumber := 1; dayNumber <= daysInYear(year); dayNumber++ {
			day := time.Date(year, 1, dayNumber, 0, 0, 0, 0, dtstart.Location())
			if !dateMatchesRule(day, rule) {
				continue
			}
			switch rule.Freq {
			case "SECONDLY", "MINUTELY", "HOURLY":
				required := positiveRemainder(base.Unix()-day.Unix(), subDailyDivisor)
				if _, ok := subDailyResidues[required]; ok {
					return true
				}
			case "DAILY":
				divisor := greatestCommonDivisor(int64(rule.Interval), daysInGregorianCycle)
				if positiveRemainder(civilDayNumber(day)-civilDayNumber(base), divisor) == 0 {
					return true
				}
			default:
				// The normal scan covers far more than a complete calendar cycle for
				// weekly and coarser frequencies. No candidate there means the rule
				// cannot justify treating COUNT as an attainable lower bound.
				return false
			}
		}
	}
	return false
}

func recurrenceFrequencySeconds(freq string) (int64, bool) {
	switch freq {
	case "SECONDLY":
		return 1, true
	case "MINUTELY":
		return 60, true
	case "HOURLY":
		return 60 * 60, true
	default:
		return 0, false
	}
}

func allowedSubDailyPeriodOffsets(rule recurrenceRule) []int64 {
	var offsets []int64
	for hour := 0; hour < 24; hour++ {
		if !intMatchesIfPresent(hour, rule.ByHour) {
			continue
		}
		if rule.Freq == "HOURLY" {
			offsets = append(offsets, int64(hour*60*60))
			continue
		}
		for minute := 0; minute < 60; minute++ {
			if !intMatchesIfPresent(minute, rule.ByMinute) {
				continue
			}
			if rule.Freq == "MINUTELY" {
				offsets = append(offsets, int64((hour*60+minute)*60))
				continue
			}
			for second := 0; second < 60; second++ {
				if intMatchesIfPresent(second, rule.BySecond) || (second == 59 && intInSlice(60, rule.BySecond)) {
					offsets = append(offsets, int64((hour*60+minute)*60+second))
				}
			}
		}
	}
	return offsets
}

func greatestCommonDivisor(a, b int64) int64 {
	if a < 0 {
		a = -a
	}
	if b < 0 {
		b = -b
	}
	for b != 0 {
		a, b = b, a%b
	}
	return a
}

func positiveRemainder(value, divisor int64) int64 {
	result := value % divisor
	if result < 0 {
		result += divisor
	}
	return result
}

// recurrenceBySetPosMaySelect reports whether the rule can select anything at
// all. RFC 5545 §3.3.10 numbers BYSETPOS within one period, so an ordinal
// larger than any period of the rule can ever offer selects nothing however
// many periods are generated -- and generating them to find that out costs
// seconds of CPU for a two-hundred byte object, on the validation gate and
// again on every expansion of the stored resource.
func recurrenceBySetPosMaySelect(dtstart time.Time, rule recurrenceRule) bool {
	maxCandidates := recurrenceMaxCandidatesPerPeriod(dtstart, rule)
	if maxCandidates == 0 {
		return false
	}
	if len(rule.BySetPos) == 0 {
		return true
	}
	for _, position := range rule.BySetPos {
		if position >= -maxCandidates && position <= maxCandidates {
			return true
		}
	}
	return false
}

// recurrenceCandidateBoundCeiling saturates the candidate-count arithmetic
// below. Every BYSETPOS value parses within ±366, so any bound at or above that
// answers identically and the exact figure past it carries no information.
const recurrenceCandidateBoundCeiling = 1 << 20

// recurrenceMaxCandidatesPerPeriod is an upper bound on the candidate starts
// one period of the rule offers, derived from the rule rather than by
// generating a period. It mirrors visitRecurrenceCandidates: the days the
// frequency's day list can hold, times the clock values each of those days
// expands to. Over-estimating only makes the BYSETPOS guard weaker, so each
// part is the widest count its generator can produce.
func recurrenceMaxCandidatesPerPeriod(dtstart time.Time, rule recurrenceRule) int {
	seconds := validSecondCandidateCount(rule.BySecond)
	minutes := len(defaultedInts(rule.ByMinute, dtstart.Minute()))
	switch rule.Freq {
	case "SECONDLY":
		// A second is one instant, so the period offers itself or nothing.
		return 1
	case "MINUTELY":
		return seconds
	case "HOURLY":
		return saturatingProduct(minutes, seconds)
	}
	hours := len(defaultedInts(rule.ByHour, dtstart.Hour()))
	perDay := saturatingProduct(saturatingProduct(hours, minutes), seconds)
	return saturatingProduct(recurrenceMaxDaysPerPeriod(dtstart, rule), perDay)
}

// recurrenceMaxDaysPerPeriod is an upper bound on the entries the day list of
// one period can hold, after the deduplication by calendar date that
// visitRecurrenceCandidates applies to it.
func recurrenceMaxDaysPerPeriod(dtstart time.Time, rule recurrenceRule) int {
	switch rule.Freq {
	case "DAILY":
		return 1
	case "WEEKLY":
		if len(rule.ByDay) == 0 {
			return 1
		}
		return weekdayDayCountBound(rule.ByDay, 7)
	case "MONTHLY":
		return monthlyDayCountBound(rule.ByMonthDay, rule.ByDay)
	case "YEARLY":
		return yearlyDayCountBound(rule)
	default:
		return 1
	}
}

// monthlyDayCountBound bounds monthlyDays. Each BYMONTHDAY value names at most
// one date of a month, and a BYDAY list is bounded over the longest month.
func monthlyDayCountBound(byMonthDay []int, byDay []weekdaySpecifier) int {
	if len(byMonthDay) > 0 {
		return len(byMonthDay)
	}
	if len(byDay) > 0 {
		return weekdayDayCountBound(byDay, 31)
	}
	return 1
}

// yearlyDayCountBound bounds yearlyDays, following the same branches it takes.
func yearlyDayCountBound(rule recurrenceRule) int {
	if len(rule.ByWeekNo) > 0 {
		perWeek := 7
		// A whole-week list is narrowed afterwards by an unnumbered BYDAY, which
		// selects the same weekdays out of every one of those weeks.
		if len(rule.ByDay) > 0 && !hasOrdinalWeekday(rule.ByDay) {
			perWeek = weekdayDayCountBound(rule.ByDay, 7)
		}
		return saturatingProduct(len(rule.ByWeekNo), perWeek)
	}
	if len(rule.ByYearDay) > 0 {
		return len(rule.ByYearDay)
	}
	if len(rule.ByMonth) == 0 && len(rule.ByDay) > 0 && len(rule.ByMonthDay) == 0 {
		// The whole year is searched, whether the days are taken from it
		// directly or month by month.
		return weekdayDayCountBound(rule.ByDay, 366)
	}
	months := len(rule.ByMonth)
	if months == 0 {
		if len(rule.ByMonthDay) > 0 {
			months = 12
		} else {
			months = 1
		}
	}
	return saturatingProduct(months, monthlyDayCountBound(rule.ByMonthDay, rule.ByDay))
}

// weekdayDayCountBound bounds the dates a BYDAY list names inside a period
// spanning at most periodDays days. A numbered specifier names one date.
//
// The unnumbered ones are counted together rather than one at a time, because
// they share the period's days: a stretch of periodDays holds exactly
// periodDays/7 complete weeks, so each named weekday occurs that often, and
// only the periodDays%7 days left over can add one more apiece. Summing a
// per-weekday maximum instead would claim 25 weekdays in a 31-day month, which
// no month has, and hand a BYSETPOS the scan then has to disprove by
// generating every period of the rule.
func weekdayDayCountBound(specifiers []weekdaySpecifier, periodDays int) int {
	numbered := 0
	var unnumbered [7]bool
	distinct := 0
	for _, spec := range specifiers {
		if spec.Ordinal != 0 {
			numbered++
			continue
		}
		if unnumbered[spec.Day] {
			continue
		}
		unnumbered[spec.Day] = true
		distinct++
	}
	total := numbered
	if distinct > 0 {
		total = saturatingSum(total, distinct*(periodDays/7)+min(distinct, periodDays%7))
	}
	return total
}

func saturatingProduct(a, b int) int {
	if a <= 0 || b <= 0 {
		return 0
	}
	if a > recurrenceCandidateBoundCeiling/b {
		return recurrenceCandidateBoundCeiling
	}
	return a * b
}

func saturatingSum(a, b int) int {
	if a > recurrenceCandidateBoundCeiling-b {
		return recurrenceCandidateBoundCeiling
	}
	return a + b
}

func validSecondCandidateCount(values []int) int {
	if len(values) == 0 {
		return 1
	}
	candidates := make(map[int]struct{}, len(values))
	for _, value := range values {
		if value >= 0 && value <= 60 {
			if value == 60 {
				value = 59
			}
			candidates[value] = struct{}{}
		}
	}
	return len(candidates)
}

// ErrRecurrenceExpansionLimit means a recurrence could not be fully evaluated
// within the work budget.
var ErrRecurrenceExpansionLimit = errors.New("recurrence expansion work limit exceeded")

// ErrRecurrenceScanLimit is the ErrRecurrenceExpansionLimit raised when a rule
// needs more candidate generation than the scan budgets allow, as opposed to
// generating more instances than the caller's output budget holds. It is a
// property of the stored rule over the range rather than of how many instances
// the range contains, and wraps ErrRecurrenceExpansionLimit so a caller that
// treats every incomplete expansion alike need not tell the two apart.
var ErrRecurrenceScanLimit = fmt.Errorf("%w: recurrence rule scan work exhausted", ErrRecurrenceExpansionLimit)

// Sparse rules are evaluated exactly while work remains. Validation fails
// closed on max-instances; reads return ErrRecurrenceExpansionLimit.
const sparseRecurrenceWorkLimit = 10_000_000

func finishSparseRecurrence(periodStart, dtstart time.Time, rule recurrenceRule, occurrences *int, add func(time.Time) bool) (bool, bool) {
	if rule.Until == nil && (rule.Count == 0 || *occurrences == 0) {
		return false, false
	}
	switch rule.Freq {
	case "SECONDLY", "MINUTELY", "HOURLY", "DAILY":
	default:
		return false, false
	}

	var until time.Time
	if rule.Until != nil {
		until = *rule.Until
	}
	exceeds := false
	complete := false
	addCandidate := func(current time.Time) bool {
		if rule.Until != nil && current.After(until) {
			return false
		}
		if rule.Count > 0 {
			if *occurrences >= rule.Count {
				complete = true
				return true
			}
			(*occurrences)++
		}
		if add(current) {
			exceeds = true
			complete = true
			return true
		}
		if rule.Count > 0 && *occurrences >= rule.Count {
			complete = true
			return true
		}
		return false
	}

	stopped, overBudget := visitSparseRecurrence(periodStart, dtstart, until, rule, addCandidate)
	if overBudget {
		return true, true
	}
	if stopped {
		return exceeds, complete
	}
	return false, true
}

func visitSparseRecurrence(periodStart, dtstart, until time.Time, rule recurrenceRule, add func(time.Time) bool) (stopped, overBudget bool) {
	remainingWork := sparseRecurrenceWorkLimit
	for year := periodStart.Year(); until.IsZero() || year <= until.Year(); year++ {
		for dayNumber := 1; dayNumber <= daysInYear(year); dayNumber++ {
			remainingWork--
			if remainingWork <= 0 {
				return true, true
			}
			day := time.Date(year, 1, dayNumber, 0, 0, 0, 0, dtstart.Location())
			if !until.IsZero() && day.After(until) {
				return false, false
			}
			if !dateMatchesRule(day, rule) {
				continue
			}
			if rule.Freq == "DAILY" {
				if day.Before(periodStart) || !dailyRecurrencePeriodAligned(day, dtstart, rule.Interval) {
					continue
				}
				stopped, overBudget := visitRecurrenceCandidates(day, dtstart, rule, func(current time.Time) bool {
					if current.Before(dtstart) || (!until.IsZero() && current.After(until)) {
						return false
					}
					return add(current)
				})
				// A day over budget leaves its candidates only partly visited, so
				// it is failed closed rather than treated as producing none, the
				// same way every other visitRecurrenceCandidates caller handles it.
				if overBudget {
					return true, true
				}
				if stopped {
					return true, false
				}
				continue
			}

			if stop, overBudget := visitSparseSubDailyPeriods(day, periodStart, dtstart, until, rule, &remainingWork, add); stop {
				if overBudget {
					return true, true
				}
				if remainingWork <= 0 {
					return true, true
				}
				return true, false
			}
		}
	}
	return false, false
}

func visitSparseSubDailyPeriods(day, periodStart, dtstart, until time.Time, rule recurrenceRule, remainingWork *int, add func(time.Time) bool) (stop, overBudget bool) {
	stepSeconds, ok := subDailyRecurrenceSeconds(rule)
	if !ok {
		return false, false
	}
	base := recurrencePeriodStart(dtstart, rule)
	dayEnd := day.AddDate(0, 0, 1)
	firstUnix := day.Unix()
	if periodStart.After(day) {
		firstUnix = periodStart.Unix()
	}
	remainder := positiveRemainder(firstUnix-base.Unix(), stepSeconds)
	if remainder != 0 {
		firstUnix += stepSeconds - remainder
	}
	lastUnix := dayEnd.Unix()
	if !until.IsZero() && until.Before(dayEnd) {
		lastUnix = until.Unix() + 1
	}

	for periodUnix := firstUnix; periodUnix < lastUnix; {
		(*remainingWork)--
		if *remainingWork <= 0 {
			return true, false
		}
		period := time.Unix(periodUnix, 0).In(day.Location())
		stopped, periodOverBudget := visitRecurrenceCandidates(period, dtstart, rule, func(current time.Time) bool {
			if current.Before(dtstart) || (!until.IsZero() && current.After(until)) {
				return false
			}
			return add(current)
		})
		if periodOverBudget {
			return true, true
		}
		if stopped {
			return true, false
		}
		if periodUnix > int64(^uint64(0)>>1)-stepSeconds {
			break
		}
		periodUnix += stepSeconds
	}
	return false, false
}

func dailyRecurrencePeriodAligned(day, dtstart time.Time, interval int) bool {
	if interval <= 0 {
		return false
	}
	base := recurrencePeriodStart(dtstart, recurrenceRule{Freq: "DAILY"})
	delta := civilDayNumber(day) - civilDayNumber(base)
	return delta >= 0 && delta%int64(interval) == 0
}

func civilDayNumber(value time.Time) int64 {
	year := int64(value.Year())
	month := int64(value.Month())
	if month <= 2 {
		year--
	}
	era := year / 400
	yearOfEra := year - era*400
	adjustedMonth := month - 3
	if adjustedMonth < 0 {
		adjustedMonth += 12
	}
	dayOfYear := (153*adjustedMonth+2)/5 + int64(value.Day()) - 1
	dayOfEra := yearOfEra*365 + yearOfEra/4 - yearOfEra/100 + dayOfYear
	return era*146097 + dayOfEra
}

func recurrencePropertyInstants(property PropertyValue) ([]time.Time, bool) {
	parts := strings.Split(property.Value, ",")
	values := make([]time.Time, 0, len(parts))
	for _, part := range parts {
		start := strings.TrimSpace(part)
		if slash := strings.IndexByte(start, '/'); slash >= 0 {
			start = strings.TrimSpace(start[:slash])
		}
		parsed, ok := ParsePropertyDateTimeLocal(property.KeyPart, start)
		if !ok {
			return nil, false
		}
		values = append(values, parsed)
	}
	return values, true
}

func recurrenceInstantKey(value time.Time) string {
	return value.UTC().Format(time.RFC3339Nano)
}

// LatestRecurrenceOnOrBefore returns the latest occurrence of the recurrence
// set -- the DTSTART or a start the rule generates -- at or before the supplied
// wall-clock value. It is used to apply the observance rules in a submitted
// VTIMEZONE definition.
//
// found reports that such a start exists. complete reports that the search
// reached the end of what the rule describes; it is false when a period was too
// large to generate within recurrencePeriodWorkLimit, or when either scan bound
// ran out. A latest returned alongside complete == false is at best the maximum
// over the part that was searched, which is not the rule's, so the caller has to
// refuse the value rather than apply it -- an observance chosen from a
// half-generated period names the wrong UTC offset, and nothing downstream can
// tell.
func LatestRecurrenceOnOrBefore(dtstart, before time.Time, rrule string) (latest time.Time, found, complete bool) {
	rule, ok := parseRecurrenceRule(rrule, dtstart.Location(), nil)
	if !ok || before.Before(dtstart) {
		return time.Time{}, false, true
	}
	// RFC 5545 §3.8.5.3 makes the DTSTART the first occurrence whether or not
	// the rule generates it, and whether or not it falls past UNTIL; a
	// VTIMEZONE observance's DTSTART is its first onset (§3.6.5). A rule that
	// selects nothing leaves it the only one.
	if !recurrenceBySetPosMaySelect(dtstart, rule) {
		return dtstart, true, true
	}
	generatesStart, overBudget := recurrenceRuleGeneratesStart(dtstart, rule)
	if overBudget {
		return dtstart, true, false
	}
	pastUntil := func(candidate time.Time) bool {
		return rule.Until != nil && candidate.After(*rule.Until) && !candidate.Equal(dtstart)
	}
	threshold := before
	if rule.Until != nil && rule.Until.Before(threshold) {
		threshold = *rule.Until
	}

	if rule.Count > 0 {
		occurrences := 0
		if !generatesStart {
			latest = dtstart
			occurrences = 1
		}
		periodStart := recurrencePeriodStart(dtstart, rule)
		for scanned := 0; scanned < recurrenceScanLimit; scanned++ {
			done := occurrences >= rule.Count
			if done {
				return latest, true, true
			}
			_, overBudget := visitRecurrenceCandidates(periodStart, dtstart, rule, func(candidate time.Time) bool {
				if candidate.Before(dtstart) {
					return false
				}
				if pastUntil(candidate) {
					done = true
					return true
				}
				occurrences++
				if occurrences > rule.Count || candidate.After(before) {
					done = true
					return true
				}
				latest = candidate
				return false
			})
			if overBudget {
				return latest, !latest.IsZero(), false
			}
			if done {
				return latest, !latest.IsZero(), true
			}
			periodStart = advanceRecurrencePeriod(periodStart, rule)
		}
		// The scan bound ran out with the rule still generating, so what was
		// reached is not the end of it.
		return latest, !latest.IsZero(), false
	}

	// The DTSTART answers only when no generated occurrence does, since every
	// generated one is at or after it.
	fallback := func() (time.Time, bool) {
		if generatesStart {
			return time.Time{}, false
		}
		return dtstart, true
	}
	periodStart := fastForwardRecurrencePeriod(recurrencePeriodStart(dtstart, rule), threshold, rule)
	for scanned := 0; scanned < 1000; scanned++ {
		// The whole period is wanted here, so a period too large to generate
		// leaves no usable answer: without BYSETPOS the maximum reached is only
		// the maximum of the days that were generated, and with it nothing is
		// selected at all, because BYSETPOS resolves only once the period ends.
		_, overBudget := visitRecurrenceCandidates(periodStart, dtstart, rule, func(candidate time.Time) bool {
			if candidate.Before(dtstart) || candidate.After(before) || pastUntil(candidate) {
				return false
			}
			if latest.IsZero() || candidate.After(latest) {
				latest = candidate
			}
			return false
		})
		if overBudget {
			return latest, !latest.IsZero(), false
		}
		if !latest.IsZero() {
			return latest, true, true
		}
		previous := retreatRecurrencePeriod(periodStart, rule)
		if !previous.Before(periodStart) || previous.Before(dtstart) {
			start, found := fallback()
			return start, found, true
		}
		periodStart = previous
	}
	return time.Time{}, false, false
}

func retreatRecurrencePeriod(periodStart time.Time, rule recurrenceRule) time.Time {
	interval := rule.Interval
	if interval <= 0 {
		interval = 1
	}
	switch rule.Freq {
	case "SECONDLY":
		return periodStart.Add(-time.Duration(interval) * time.Second)
	case "MINUTELY":
		return periodStart.Add(-time.Duration(interval) * time.Minute)
	case "HOURLY":
		return periodStart.Add(-time.Duration(interval) * time.Hour)
	case "DAILY":
		return periodStart.AddDate(0, 0, -interval)
	case "WEEKLY":
		return periodStart.AddDate(0, 0, -7*interval)
	case "MONTHLY":
		return periodStart.AddDate(0, -interval, 0)
	case "YEARLY":
		return periodStart.AddDate(-interval, 0, 0)
	default:
		return periodStart
	}
}

// recurrenceCivilScanPad bounds how far a civil-time scan window is widened
// past the absolute request bounds, since a valid UTC offset is strictly less
// than 24 hours.
const recurrenceCivilScanPad = 24 * time.Hour

func rruleBusyPeriods(component *Component, dtstart time.Time, duration time.Duration, rrule string, exdates recurrenceInstantSet, rangeStart, rangeEnd time.Time, expansion recurrenceExpansion, add func(time.Time, BusyPeriod) bool) (bool, error) {
	recurrenceStart, resolveCandidate, civil := recurrenceGenerationStart(component, dtstart, expansion.resolve)
	rule, ok := parseRecurrenceRule(rrule, recurrenceStart.Location(), expansion.resolve)
	if !ok {
		return false, nil
	}

	occurrences := 0
	done := false
	visit := func(current time.Time) bool {
		if current.Before(recurrenceStart) {
			return false
		}
		resolvedCurrent, resolved := resolveCandidate(current)
		if !resolved {
			return false
		}
		// The DTSTART is an instance even past UNTIL (RFC 5545 §3.8.5.3).
		if rule.Until != nil && resolvedCurrent.After(*rule.Until) && !current.Equal(recurrenceStart) {
			done = true
			return true
		}
		occurrences++
		if rule.Count > 0 && occurrences > rule.Count {
			done = true
			return true
		}
		period := BusyPeriod{Start: resolvedCurrent, End: resolvedCurrent.Add(duration)}
		// resolvedCurrent is the slot the rule generated; the shared
		// expansion applies any RANGE=THISANDFUTURE transform afterwards.
		if expansion.reaches(period, period, false, rangeStart, rangeEnd) && !exdates.contains(resolvedCurrent) {
			if add(resolvedCurrent, period) {
				done = true
				return true
			}
		}
		return false
	}

	// RFC 5545 §3.8.5.3 makes the DTSTART the first instance of the set, and
	// the first toward COUNT, whether or not the rule generates it. A rule that
	// does generate it visits it below like any other candidate.
	selects := recurrenceBySetPosMaySelect(recurrenceStart, rule)
	generatesStart := false
	if selects {
		var overBudget bool
		generatesStart, overBudget = recurrenceRuleGeneratesStart(recurrenceStart, rule)
		if overBudget {
			// Whether or not the rule generates it, the DTSTART is an
			// instance, so it is collected with whatever else is returned.
			visit(recurrenceStart)
			return false, ErrRecurrenceScanLimit
		}
	}
	if !generatesStart {
		visit(recurrenceStart)
		if done {
			return true, nil
		}
	}
	// A rule whose BYSETPOS names an ordinal no period of it can reach generates
	// nothing past the DTSTART. That is an answer, and one the rule gives on its
	// own: scanning for it would burn the whole work budget on every report the
	// resource appears in and still end with nothing to return.
	if !selects {
		return true, nil
	}

	scanStart, scanEnd := rangeStart, rangeEnd
	if civil {
		// A valid UTC offset is strictly less than 24 hours. Padding the absolute
		// request bounds by that amount produces a safe civil-time scan window;
		// every candidate is still resolved and checked against the exact bounds.
		scanStart = scanStart.Add(-recurrenceCivilScanPad)
		scanEnd = scanEnd.Add(recurrenceCivilScanPad)
	}
	periodStart := recurrencePeriodStart(recurrenceStart, rule)
	if rule.Count == 0 {
		periodStart = fastForwardRecurrencePeriod(periodStart, scanStart.Add(-duration), rule)
	} else if fastForwardedStart, skipped, ok := fastForwardCountedSubDailyRecurrence(periodStart, scanStart.Add(-duration), rule); ok {
		if occurrences+skipped >= rule.Count {
			return true, nil
		}
		periodStart = fastForwardedStart
		occurrences += skipped
	}
	for scanned := 0; scanned < recurrenceScanLimit; scanned++ {
		_, overBudget := visitRecurrenceCandidates(periodStart, recurrenceStart, rule, visit)
		if overBudget {
			return false, ErrRecurrenceScanLimit
		}
		if done {
			return true, nil
		}
		next := advanceRecurrencePeriod(periodStart, rule)
		if !next.After(periodStart) {
			return false, ErrRecurrenceScanLimit
		}
		periodStart = next
		if periodStart.After(scanEnd) || (rule.Count > 0 && occurrences >= rule.Count) {
			return true, nil
		}
	}
	switch rule.Freq {
	case "SECONDLY", "MINUTELY", "HOURLY", "DAILY":
		_, overBudget := visitSparseRecurrence(periodStart, recurrenceStart, scanEnd, rule, visit)
		if !overBudget {
			return true, nil
		}
	}
	return false, ErrRecurrenceScanLimit
}

// recurrenceRuleGeneratesStart reports whether rule itself generates dtstart,
// which only the period containing it can. overBudget reports that the period
// was too large to decide within recurrencePeriodWorkLimit.
func recurrenceRuleGeneratesStart(dtstart time.Time, rule recurrenceRule) (generates, overBudget bool) {
	// A rule with no BY part derives every candidate from the DTSTART itself.
	if !recurrenceRuleHasOtherByPart(rule) && len(rule.BySetPos) == 0 {
		return true, false
	}
	_, overBudget = visitRecurrenceCandidates(recurrencePeriodStart(dtstart, rule), dtstart, rule, func(candidate time.Time) bool {
		if candidate.Before(dtstart) {
			return false
		}
		generates = candidate.Equal(dtstart)
		return true
	})
	return generates && !overBudget, overBudget
}

func recurrenceGenerationStart(component *Component, fallback time.Time, resolve PropertyTimeResolver) (time.Time, func(time.Time) (time.Time, bool), bool) {
	property, ok := ComponentProperty(component, "DTSTART")
	if !ok || hasZoneSuffix(strings.TrimSpace(property.Value)) {
		return fallback, func(candidate time.Time) (time.Time, bool) { return candidate, true }, false
	}
	wall, err := ParseDateTime(strings.TrimSpace(property.Value))
	if err != nil {
		return fallback, func(candidate time.Time) (time.Time, bool) { return candidate, true }, false
	}
	dateOnly := !strings.Contains(property.Value, "T")
	return wall, func(candidate time.Time) (time.Time, bool) {
		layout := "20060102T150405"
		if dateOnly {
			layout = "20060102"
		}
		return resolve.or(property.KeyPart, candidate.Format(layout))
	}, true
}

func fastForwardCountedSubDailyRecurrence(periodStart, threshold time.Time, rule recurrenceRule) (time.Time, int, bool) {
	stepSeconds, ok := subDailyRecurrenceSeconds(rule)
	if !ok || !fixedStepSubDailyRecurrence(rule) {
		return periodStart, 0, false
	}
	if !threshold.After(periodStart) {
		return periodStart, 0, true
	}
	// Whole seconds rather than a time.Duration, which saturates at about 292
	// years and would stop the fast-forward short for an early DTSTART.
	elapsed := threshold.Unix() - periodStart.Unix()
	if threshold.Nanosecond() < periodStart.Nanosecond() {
		elapsed--
	}
	steps := elapsed / stepSeconds
	if steps <= 0 {
		return periodStart, 0, true
	}
	return addSeconds(periodStart, steps*stepSeconds), int(steps), true
}

func fixedStepSubDailyRecurrence(rule recurrenceRule) bool {
	if rule.Freq != "SECONDLY" && rule.Freq != "MINUTELY" && rule.Freq != "HOURLY" {
		return false
	}
	return len(rule.BySecond) == 0 &&
		len(rule.ByMinute) == 0 &&
		len(rule.ByHour) == 0 &&
		len(rule.ByMonth) == 0 &&
		len(rule.ByMonthDay) == 0 &&
		len(rule.ByYearDay) == 0 &&
		len(rule.ByWeekNo) == 0 &&
		len(rule.ByDay) == 0 &&
		len(rule.BySetPos) == 0
}

func supportedRecurrenceFreq(freq string) bool {
	switch strings.ToUpper(freq) {
	case "SECONDLY", "MINUTELY", "HOURLY", "DAILY", "WEEKLY", "MONTHLY", "YEARLY":
		return true
	default:
		return false
	}
}

func periodOverlaps(start, end, rangeStart, rangeEnd time.Time) bool {
	return start.Before(rangeEnd) && end.After(rangeStart)
}

// periodTouches is periodOverlaps with both bounds inclusive, so an occurrence
// that only meets the range at an endpoint still reaches it.
func periodTouches(start, end, rangeStart, rangeEnd time.Time) bool {
	return !start.After(rangeEnd) && !end.Before(rangeStart)
}

// recurrenceInstant is a comparable form of an absolute instant, so a set of
// them can be looked up rather than scanned. Seconds and nanoseconds are held
// apart rather than combined: RFC 5545 §3.3.5 admits a DATE-TIME centuries
// before 1678, which a nanosecond count cannot represent.
type recurrenceInstant struct {
	seconds     int64
	nanoseconds int
}

// recurrenceInstantSet answers the exclusion test the expansion applies once
// per generated instance. RFC 5545 §3.8.5.1 puts no bound on how many EXDATEs a
// component may carry, so scanning them would make one expansion cost the
// product of the two counts.
type recurrenceInstantSet map[recurrenceInstant]struct{}

func (s recurrenceInstantSet) add(value time.Time) {
	s[recurrenceInstant{seconds: value.Unix(), nanoseconds: value.Nanosecond()}] = struct{}{}
}

func (s recurrenceInstantSet) contains(value time.Time) bool {
	_, ok := s[recurrenceInstant{seconds: value.Unix(), nanoseconds: value.Nanosecond()}]
	return ok
}

func componentRDatePeriods(component *Component, resolve PropertyTimeResolver) []rdatePeriod {
	var periods []rdatePeriod
	for _, prop := range componentProperties(component, "RDATE") {
		for _, value := range strings.Split(prop.Value, ",") {
			value = strings.TrimSpace(value)
			if value == "" {
				continue
			}
			if strings.Contains(value, "/") {
				parts := strings.SplitN(value, "/", 2)
				start, ok := resolve.or(prop.KeyPart, strings.TrimSpace(parts[0]))
				if !ok {
					continue
				}
				var end time.Time
				periodEnd := strings.TrimSpace(parts[1])
				if strings.HasPrefix(strings.ToUpper(periodEnd), "P") {
					if duration, ok := ParseDuration(periodEnd); ok {
						end = start.Add(duration)
					}
				} else {
					if parsedEnd, ok := resolve.or(prop.KeyPart, periodEnd); ok {
						end = parsedEnd
					}
				}
				periods = append(periods, rdatePeriod{Start: start, End: end})
				continue
			}
			if start, ok := resolve.or(prop.KeyPart, value); ok {
				periods = append(periods, rdatePeriod{Start: start})
			}
		}
	}
	return periods
}

func eventExDates(component *Component, resolve PropertyTimeResolver) recurrenceInstantSet {
	dates := make(recurrenceInstantSet)
	for _, prop := range componentProperties(component, "EXDATE") {
		for _, value := range strings.Split(prop.Value, ",") {
			if parsed, ok := resolve.or(prop.KeyPart, strings.TrimSpace(value)); ok {
				dates.add(parsed)
			}
		}
	}
	return dates
}

type recurrenceOverride struct {
	recurrenceID       time.Time
	period             BusyPeriod
	cancelled          bool
	rangeThisAndFuture bool
}

func componentRecurrenceOverrides(components []Component, fallbackDuration time.Duration, resolve PropertyTimeResolver) []recurrenceOverride {
	var overrides []recurrenceOverride
	for _, component := range components {
		recurrenceIDProp, ok := ComponentProperty(&component, "RECURRENCE-ID")
		if !ok {
			continue
		}
		recurrenceID, ok := resolve.or(recurrenceIDProp.KeyPart, recurrenceIDProp.Value)
		if !ok {
			continue
		}

		start := recurrenceID
		if prop, ok := ComponentProperty(&component, "DTSTART"); ok {
			if parsed, ok := resolve.or(prop.KeyPart, prop.Value); ok {
				start = parsed
			}
		}

		cancelled := false
		if prop, ok := ComponentProperty(&component, "STATUS"); ok {
			cancelled = strings.EqualFold(strings.TrimSpace(prop.Value), "CANCELLED")
		}

		end := start.Add(fallbackDuration)
		if prop, ok := ComponentProperty(&component, "DTEND"); ok {
			if parsed, ok := resolve.or(prop.KeyPart, prop.Value); ok && parsed.After(start) {
				end = parsed
			}
		} else if prop, ok := ComponentProperty(&component, "DURATION"); ok {
			if duration, ok := ParseDuration(prop.Value); ok && duration > 0 {
				end = start.Add(duration)
			}
		}

		overrides = append(overrides, recurrenceOverride{
			recurrenceID:       recurrenceID,
			period:             BusyPeriod{Start: start, End: end},
			cancelled:          cancelled,
			rangeThisAndFuture: PropertyParamEquals(recurrenceIDProp.KeyPart, "RANGE", "THISANDFUTURE"),
		})
	}
	return overrides
}

// recurrenceOverrideIndex answers the two override questions the expansion
// asks of every generated instance: whether an ordinary override replaces its
// slot, and which RANGE=THISANDFUTURE override governs it. RFC 5545 §3.8.4.4
// bounds neither how many overrides a resource may carry, so both are lookups
// rather than scans; otherwise one expansion would cost the product of the
// instance and override counts.
type recurrenceOverrideIndex struct {
	exact recurrenceInstantSet
	// thisAndFuture is ordered by RECURRENCE-ID. Among overrides naming the same
	// slot the first in the resource comes first, and is the one that governs.
	thisAndFuture []recurrenceOverride
}

func newRecurrenceOverrideIndex(overrides []recurrenceOverride) recurrenceOverrideIndex {
	index := recurrenceOverrideIndex{exact: make(recurrenceInstantSet)}
	for _, override := range overrides {
		if override.rangeThisAndFuture {
			index.thisAndFuture = append(index.thisAndFuture, override)
			continue
		}
		index.exact.add(override.recurrenceID)
	}
	sort.SliceStable(index.thisAndFuture, func(i, j int) bool {
		return index.thisAndFuture[i].recurrenceID.Before(index.thisAndFuture[j].recurrenceID)
	})
	return index
}

// replaces reports whether an ordinary override names the slot.
func (index recurrenceOverrideIndex) replaces(recurrenceID time.Time) bool {
	return index.exact.contains(recurrenceID)
}

// governing returns the RANGE=THISANDFUTURE override with the latest
// RECURRENCE-ID at or before start.
func (index recurrenceOverrideIndex) governing(start time.Time) (recurrenceOverride, bool) {
	overrides := index.thisAndFuture
	after := sort.Search(len(overrides), func(i int) bool {
		return overrides[i].recurrenceID.After(start)
	})
	if after == 0 {
		return recurrenceOverride{}, false
	}
	latest := overrides[after-1].recurrenceID
	first := sort.Search(after, func(i int) bool {
		return !overrides[i].recurrenceID.Before(latest)
	})
	return overrides[first], true
}

func (index recurrenceOverrideIndex) hasThisAndFuture() bool {
	return len(index.thisAndFuture) > 0
}

// maxThisAndFutureShiftSeconds is the furthest any RANGE=THISANDFUTURE
// override moves its instances, in whole seconds rounded up. It is counted in
// seconds rather than as a time.Duration because the distance can exceed the
// ~292 years a Duration holds.
func (index recurrenceOverrideIndex) maxThisAndFutureShiftSeconds() int64 {
	var max int64
	for _, override := range index.thisAndFuture {
		if override.cancelled {
			continue
		}
		shift := override.period.Start.Unix() - override.recurrenceID.Unix()
		if shift < 0 {
			shift = -shift
		}
		if shift+1 > max {
			max = shift + 1
		}
	}
	return max
}

// addSeconds moves t by seconds, which may exceed what a time.Duration holds.
func addSeconds(t time.Time, seconds int64) time.Time {
	return time.Unix(t.Unix()+seconds, int64(t.Nanosecond())).In(t.Location())
}

// moveInstant moves t by the distance from from to to, which may exceed what
// a time.Duration holds.
func moveInstant(t, from, to time.Time) time.Time {
	return time.Unix(t.Unix()+(to.Unix()-from.Unix()),
		int64(t.Nanosecond())+int64(to.Nanosecond()-from.Nanosecond())).In(t.Location())
}

func (index recurrenceOverrideIndex) applyThisAndFuture(period BusyPeriod) (BusyPeriod, bool, time.Time) {
	selected, ok := index.governing(period.Start)
	if !ok {
		return period, false, time.Time{}
	}
	if selected.cancelled {
		return period, true, selected.recurrenceID
	}

	shiftedStart := moveInstant(period.Start, selected.recurrenceID, selected.period.Start)
	duration := selected.period.End.Sub(selected.period.Start)
	if duration <= 0 {
		duration = period.End.Sub(period.Start)
	}
	return BusyPeriod{Start: shiftedStart, End: shiftedStart.Add(duration)}, false, selected.recurrenceID
}

// Component is one component block's parsed content lines, of whatever type the
// BEGIN named.
type Component struct {
	properties          []PropertyValue
	malformedProperties []string
}

// PropertyValue is one content line of a component: the part before the
// first colon (name plus parameters) and the value after it.
type PropertyValue struct {
	KeyPart string
	Value   string
}

// PrimaryVEventComponent returns the master VEVENT of the resource, the
// component the denormalized event columns are derived from.
func PrimaryVEventComponent(ical string) *Component {
	return PrimaryComponent(ical, "VEVENT")
}

// PrimaryComponent returns the master component of the named type: the first
// one carrying no RECURRENCE-ID, since RFC 4791 §4.1 permits a resource made
// only of overridden instances and every one of those carries the property.
func PrimaryComponent(ical, componentName string) *Component {
	return primaryOf(componentsNamed(ical, componentName))
}

func primaryOf(components []Component) *Component {
	for i := range components {
		if !componentHasProperty(&components[i], "RECURRENCE-ID") {
			return &components[i]
		}
	}
	if len(components) == 0 {
		return nil
	}
	return &components[0]
}

func componentsNamed(ical, componentName string) []Component {
	return componentsNamedFromLines(UnfoldLines(ical), componentName)
}

func componentsNamedFromLines(lines []string, componentName string) []Component {
	return topLevelComponentsFromLines(lines, func(name string) bool {
		return strings.EqualFold(name, componentName)
	})
}

func topLevelComponents(ical string, accept func(string) bool) []Component {
	return topLevelComponentsFromLines(UnfoldLines(ical), accept)
}

func topLevelComponentsFromLines(lines []string, accept func(string) bool) []Component {
	var components []Component
	depth := 0
	componentDepth := 0
	componentName := ""
	var current *Component
	for _, rawLine := range lines {
		controlLine := strings.TrimSpace(rawLine)
		upper := strings.ToUpper(controlLine)
		switch {
		case strings.HasPrefix(upper, "BEGIN:"):
			depth++
			name := strings.TrimSpace(controlLine[len("BEGIN:"):])
			if current == nil && depth == 2 && accept(name) {
				componentDepth = depth
				componentName = name
				current = &Component{}
			}
			continue
		case strings.HasPrefix(upper, "END:"):
			if current != nil && componentDepth == depth && strings.EqualFold(strings.TrimSpace(controlLine[len("END:"):]), componentName) {
				components = append(components, *current)
				current = nil
				componentDepth = 0
				componentName = ""
			}
			depth--
			if depth < 0 {
				depth = 0
			}
			continue
		}
		if current == nil || componentDepth != depth {
			continue
		}
		keyPart, value, ok := SplitContentLine(rawLine)
		if !ok {
			current.malformedProperties = append(current.malformedProperties, rawLine)
			continue
		}
		current.properties = append(current.properties, PropertyValue{KeyPart: keyPart, Value: value})
	}
	return components
}

func componentProperties(component *Component, name string) []PropertyValue {
	var values []PropertyValue
	if component == nil {
		return values
	}
	for _, prop := range component.properties {
		if !strings.EqualFold(PropertyName(prop.KeyPart), name) {
			continue
		}
		values = append(values, prop)
	}
	return values
}

func ComponentProperty(component *Component, name string) (PropertyValue, bool) {
	values := componentProperties(component, name)
	if len(values) == 0 {
		return PropertyValue{}, false
	}
	return values[0], true
}

func componentPropertyValue(component *Component, name string) string {
	prop, ok := ComponentProperty(component, name)
	if !ok {
		return ""
	}
	return strings.TrimSpace(prop.Value)
}

func componentHasProperty(component *Component, name string) bool {
	_, ok := ComponentProperty(component, name)
	return ok
}

// PropertyParam returns the value of the named parameter on a content line's
// key part, and whether the line carries it at all.
//
// RFC 5545 §3.2 makes a parameter value a quoted string whenever it contains a
// colon, a semicolon or a comma, and some clients quote unconditionally --
// Exchange writes every TZID that way. The quotes delimit the value rather than
// belonging to it, so a separator inside them does not start another parameter
// and they are stripped from what is returned: a caller handed
// `"America/New_York"` resolves no zone at all and silently places every value
// it reads a whole UTC offset away from where the property named.
func PropertyParam(keyPart, param string) (string, bool) {
	for _, part := range propertyParameterParts(keyPart) {
		name, value, found := strings.Cut(part, "=")
		if !found {
			continue
		}
		if strings.EqualFold(strings.TrimSpace(name), param) {
			return unquotePropertyParam(strings.TrimSpace(value)), true
		}
	}
	return "", false
}

// SplitContentLine splits an unfolded content line into its key part (name and
// parameters) and its value. RFC 5545 §3.1 lets a quoted parameter value hold a
// colon, so the value starts at the first colon outside double quotes: Exchange
// writes zone names such as "(UTC-05:00) Eastern Time (US & Canada)" as TZIDs.
func SplitContentLine(line string) (keyPart, value string, ok bool) {
	colon := IndexOutsideQuotes(line, ':')
	if colon < 0 {
		return "", "", false
	}
	return line[:colon], line[colon+1:], true
}

// IndexOutsideQuotes returns the index of the first delimiter in a content line
// or key part that is not inside a double-quoted parameter value, or -1.
func IndexOutsideQuotes(s string, delimiter byte) int {
	quoted := false
	for i := 0; i < len(s); i++ {
		switch s[i] {
		case '"':
			quoted = !quoted
		case delimiter:
			if !quoted {
				return i
			}
		}
	}
	return -1
}

// propertyParameterParts splits a content line's key part into its parameters,
// leaving out the property name that precedes the first separator.
func propertyParameterParts(keyPart string) []string {
	separator := IndexOutsideQuotes(keyPart, ';')
	if separator < 0 {
		return nil
	}
	var parts []string
	rest := keyPart[separator+1:]
	for {
		next := IndexOutsideQuotes(rest, ';')
		if next < 0 {
			return append(parts, rest)
		}
		parts = append(parts, rest[:next])
		rest = rest[next+1:]
	}
}

func unquotePropertyParam(value string) string {
	if len(value) >= 2 && value[0] == '"' && value[len(value)-1] == '"' {
		return value[1 : len(value)-1]
	}
	return value
}

func PropertyParamEquals(keyPart, param, value string) bool {
	got, ok := PropertyParam(keyPart, param)
	return ok && strings.EqualFold(got, value)
}

// PropertyName returns the property name from a content line or its key part,
// i.e. everything before the first parameter (";") or value (":") delimiter.
// A key part carrying neither delimiter is returned unchanged.
func PropertyName(keyPart string) string {
	if idx := strings.IndexAny(keyPart, ";:"); idx >= 0 {
		return keyPart[:idx]
	}
	return keyPart
}

// ParsePropertyDateTimeLocal resolves one date-valued content line to an
// absolute instant, reading a TZID from the property's parameters when it names
// a zone the host knows. A value carrying neither a resolvable TZID nor its own
// zone suffix is floating, and this reads it as UTC -- the fallback RFC 4791
// §7.3 reaches when a request carries no CALDAV:timezone and the collection
// defines no CALDAV:calendar-timezone. A caller holding either of those resolves
// the value against it instead.
func ParsePropertyDateTimeLocal(keyPart, value string) (time.Time, bool) {
	value = strings.TrimSpace(value)
	if value == "" {
		return time.Time{}, false
	}
	if tzid, ok := PropertyParam(keyPart, "TZID"); ok {
		if loc, err := time.LoadLocation(tzid); err == nil {
			if parsed, err := ParseDateTimeInLocation(value, loc); err == nil {
				return parsed, true
			}
		}
	}
	parsed, err := ParseDateTime(value)
	return parsed, err == nil
}

func ParseDuration(value string) (time.Duration, bool) {
	value = strings.TrimSpace(strings.ToUpper(value))
	if value == "" {
		return 0, false
	}
	sign := time.Duration(1)
	if strings.HasPrefix(value, "-") {
		sign = -1
		value = strings.TrimPrefix(value, "-")
	} else {
		value = strings.TrimPrefix(value, "+")
	}
	if !strings.HasPrefix(value, "P") {
		return 0, false
	}
	value = strings.TrimPrefix(value, "P")
	if value == "" {
		return 0, false
	}

	var total time.Duration
	var number strings.Builder
	inTime := false
	consume := func(unit byte) bool {
		if number.Len() == 0 {
			return false
		}
		n, err := strconv.Atoi(number.String())
		number.Reset()
		if err != nil {
			return false
		}
		switch unit {
		case 'W':
			total += time.Duration(n) * 7 * 24 * time.Hour
		case 'D':
			total += time.Duration(n) * 24 * time.Hour
		case 'H':
			total += time.Duration(n) * time.Hour
		case 'M':
			if !inTime {
				return false
			}
			total += time.Duration(n) * time.Minute
		case 'S':
			total += time.Duration(n) * time.Second
		default:
			return false
		}
		return true
	}

	for i := 0; i < len(value); i++ {
		ch := value[i]
		if ch >= '0' && ch <= '9' {
			number.WriteByte(ch)
			continue
		}
		if ch == 'T' {
			if inTime || number.Len() != 0 {
				return 0, false
			}
			inTime = true
			continue
		}
		if !consume(ch) {
			return 0, false
		}
	}
	if number.Len() != 0 || total < 0 {
		return 0, false
	}
	return sign * total, true
}

const (
	recurrenceScanLimit = 100000
	maxICalendarInteger = 1<<31 - 1

	// unboundedRecurrenceHorizonYears is the window over which the instance
	// limit is measured for a rule with neither COUNT nor UNTIL. A year is
	// long enough that every calendar frequency a client sends stays well
	// inside the limit, and short enough that a sub-daily rule is past it.
	unboundedRecurrenceHorizonYears = 1
)

type recurrenceRule struct {
	Freq       string
	Interval   int
	Count      int
	Until      *time.Time
	WKST       time.Weekday
	BySecond   []int
	ByMinute   []int
	ByHour     []int
	ByMonth    []int
	ByMonthDay []int
	ByYearDay  []int
	ByWeekNo   []int
	ByDay      []weekdaySpecifier
	BySetPos   []int
}

type weekdaySpecifier struct {
	Ordinal int
	Day     time.Weekday
}

func parseRecurrenceRule(rrule string, loc *time.Location, resolve PropertyTimeResolver) (recurrenceRule, bool) {
	params := make(map[string]string)
	for _, part := range strings.Split(rrule, ";") {
		kv := strings.SplitN(strings.TrimSpace(part), "=", 2)
		if len(kv) != 2 {
			return recurrenceRule{}, false
		}
		key := strings.ToUpper(strings.TrimSpace(kv[0]))
		value := strings.TrimSpace(kv[1])
		if !knownRecurrenceRulePart(key) || value == "" {
			return recurrenceRule{}, false
		}
		if _, duplicate := params[key]; duplicate {
			return recurrenceRule{}, false
		}
		params[key] = value
	}
	freq := strings.ToUpper(params["FREQ"])
	if !supportedRecurrenceFreq(freq) {
		return recurrenceRule{}, false
	}
	rule := recurrenceRule{
		Freq:     freq,
		Interval: 1,
		WKST:     time.Monday,
	}
	if intervalStr := params["INTERVAL"]; intervalStr != "" {
		interval, err := strconv.Atoi(intervalStr)
		if err != nil || interval <= 0 || interval > maxICalendarInteger {
			return recurrenceRule{}, false
		}
		rule.Interval = interval
	}
	if countStr := params["COUNT"]; countStr != "" {
		count, err := strconv.Atoi(countStr)
		if err != nil || count <= 0 || count > maxICalendarInteger {
			return recurrenceRule{}, false
		}
		rule.Count = count
	}
	if params["COUNT"] != "" && params["UNTIL"] != "" {
		return recurrenceRule{}, false
	}
	if untilStr := params["UNTIL"]; untilStr != "" {
		until, ok := parseRecurrenceUntil(untilStr, loc, resolve)
		if !ok {
			return recurrenceRule{}, false
		}
		rule.Until = &until
	}
	if wkst := params["WKST"]; wkst != "" {
		day, ok := parseWeekday(wkst)
		if !ok {
			return recurrenceRule{}, false
		}
		rule.WKST = day
	}

	var ok bool
	if rule.BySecond, ok = parseIntList(params["BYSECOND"], 0, 60); !ok {
		return recurrenceRule{}, false
	}
	if rule.ByMinute, ok = parseIntList(params["BYMINUTE"], 0, 59); !ok {
		return recurrenceRule{}, false
	}
	if rule.ByHour, ok = parseIntList(params["BYHOUR"], 0, 23); !ok {
		return recurrenceRule{}, false
	}
	if rule.ByMonth, ok = parseIntList(params["BYMONTH"], 1, 12); !ok {
		return recurrenceRule{}, false
	}
	if rule.ByMonthDay, ok = parseIntListAllowNegative(params["BYMONTHDAY"], -31, 31); !ok {
		return recurrenceRule{}, false
	}
	if rule.ByYearDay, ok = parseIntListAllowNegative(params["BYYEARDAY"], -366, 366); !ok {
		return recurrenceRule{}, false
	}
	if rule.ByWeekNo, ok = parseIntListAllowNegative(params["BYWEEKNO"], -53, 53); !ok {
		return recurrenceRule{}, false
	}
	if rule.BySetPos, ok = parseIntListAllowNegative(params["BYSETPOS"], -366, 366); !ok {
		return recurrenceRule{}, false
	}
	if rule.ByDay, ok = parseWeekdayList(params["BYDAY"]); !ok {
		return recurrenceRule{}, false
	}
	ordinalWeekday := false
	for _, day := range rule.ByDay {
		if day.Ordinal != 0 {
			ordinalWeekday = true
			break
		}
	}
	if ordinalWeekday && rule.Freq != "MONTHLY" && rule.Freq != "YEARLY" {
		return recurrenceRule{}, false
	}
	if ordinalWeekday && rule.Freq == "YEARLY" && len(rule.ByWeekNo) != 0 {
		return recurrenceRule{}, false
	}
	if rule.Freq == "WEEKLY" && len(rule.ByMonthDay) != 0 {
		return recurrenceRule{}, false
	}
	if (rule.Freq == "DAILY" || rule.Freq == "WEEKLY" || rule.Freq == "MONTHLY") && len(rule.ByYearDay) != 0 {
		return recurrenceRule{}, false
	}
	if len(rule.ByWeekNo) != 0 && rule.Freq != "YEARLY" {
		return recurrenceRule{}, false
	}
	if len(rule.BySetPos) != 0 && !recurrenceRuleHasOtherByPart(rule) {
		return recurrenceRule{}, false
	}
	return rule, true
}

func recurrenceRuleHasOtherByPart(rule recurrenceRule) bool {
	return len(rule.BySecond) != 0 || len(rule.ByMinute) != 0 || len(rule.ByHour) != 0 ||
		len(rule.ByDay) != 0 || len(rule.ByMonthDay) != 0 || len(rule.ByYearDay) != 0 ||
		len(rule.ByWeekNo) != 0 || len(rule.ByMonth) != 0
}

func knownRecurrenceRulePart(name string) bool {
	switch name {
	case "FREQ", "UNTIL", "COUNT", "INTERVAL", "BYSECOND", "BYMINUTE", "BYHOUR",
		"BYDAY", "BYMONTHDAY", "BYYEARDAY", "BYWEEKNO", "BYMONTH", "BYSETPOS", "WKST":
		return true
	default:
		return false
	}
}

// parseRecurrenceUntil reads the UNTIL rule part. RFC 5545 §3.3.10 requires a
// floating UNTIL exactly where DTSTART is floating, so a value carrying no zone
// of its own belongs to whatever zone resolved DTSTART: the caller's resolver
// first, since that is the only thing holding a zone the observances of the
// resource itself define, then the DTSTART location.
func parseRecurrenceUntil(value string, loc *time.Location, resolve PropertyTimeResolver) (time.Time, bool) {
	if !hasZoneSuffix(value) {
		if resolve != nil {
			if parsed, ok := resolve("UNTIL", value); ok {
				return parsed, true
			}
		}
		if loc != nil {
			if parsed, err := ParseDateTimeInLocation(value, loc); err == nil {
				return parsed, true
			}
		}
	}
	parsed, err := ParseDateTime(value)
	return parsed, err == nil
}

func parseIntList(value string, min, max int) ([]int, bool) {
	return parseIntListWithZero(value, min, max, true)
}

func parseIntListAllowNegative(value string, min, max int) ([]int, bool) {
	return parseIntListWithZero(value, min, max, false)
}

func parseIntListWithZero(value string, min, max int, allowZero bool) ([]int, bool) {
	if strings.TrimSpace(value) == "" {
		return nil, true
	}
	seen := make(map[int]struct{})
	var result []int
	for _, part := range strings.Split(value, ",") {
		n, err := strconv.Atoi(strings.TrimSpace(part))
		if err != nil || n < min || n > max || (!allowZero && n == 0) {
			return nil, false
		}
		if _, ok := seen[n]; ok {
			continue
		}
		seen[n] = struct{}{}
		result = append(result, n)
	}
	sort.Ints(result)
	return result, true
}

func parseWeekdayList(value string) ([]weekdaySpecifier, bool) {
	if strings.TrimSpace(value) == "" {
		return nil, true
	}
	var result []weekdaySpecifier
	for _, part := range strings.Split(value, ",") {
		part = strings.ToUpper(strings.TrimSpace(part))
		if len(part) < 2 {
			return nil, false
		}
		dayPart := part[len(part)-2:]
		day, ok := parseWeekday(dayPart)
		if !ok {
			return nil, false
		}
		ordinal := 0
		if prefix := part[:len(part)-2]; prefix != "" {
			n, err := strconv.Atoi(prefix)
			if err != nil || n == 0 || n < -53 || n > 53 {
				return nil, false
			}
			ordinal = n
		}
		result = append(result, weekdaySpecifier{Ordinal: ordinal, Day: day})
	}
	return result, true
}

func parseWeekday(value string) (time.Weekday, bool) {
	switch strings.ToUpper(strings.TrimSpace(value)) {
	case "SU":
		return time.Sunday, true
	case "MO":
		return time.Monday, true
	case "TU":
		return time.Tuesday, true
	case "WE":
		return time.Wednesday, true
	case "TH":
		return time.Thursday, true
	case "FR":
		return time.Friday, true
	case "SA":
		return time.Saturday, true
	default:
		return time.Sunday, false
	}
}

func recurrencePeriodStart(dtstart time.Time, rule recurrenceRule) time.Time {
	switch rule.Freq {
	case "SECONDLY":
		return dtstart
	case "MINUTELY":
		return time.Date(dtstart.Year(), dtstart.Month(), dtstart.Day(), dtstart.Hour(), dtstart.Minute(), 0, 0, dtstart.Location())
	case "HOURLY":
		return time.Date(dtstart.Year(), dtstart.Month(), dtstart.Day(), dtstart.Hour(), 0, 0, 0, dtstart.Location())
	case "DAILY":
		return time.Date(dtstart.Year(), dtstart.Month(), dtstart.Day(), 0, 0, 0, 0, dtstart.Location())
	case "WEEKLY":
		return startOfWeek(dtstart, rule.WKST)
	case "MONTHLY":
		return time.Date(dtstart.Year(), dtstart.Month(), 1, 0, 0, 0, 0, dtstart.Location())
	case "YEARLY":
		return time.Date(dtstart.Year(), 1, 1, 0, 0, 0, 0, dtstart.Location())
	default:
		return dtstart
	}
}

func fastForwardRecurrencePeriod(periodStart, threshold time.Time, rule recurrenceRule) time.Time {
	// No instance exists past UNTIL, so the scan never has to be carried
	// beyond it however far out the requested range starts.
	if rule.Until != nil && rule.Until.Before(threshold) {
		threshold = *rule.Until
	}
	// The jump is measured in Unix seconds rather than through time.Duration,
	// which saturates at roughly 292 years. A DTSTART further back than that --
	// 16010101, the value Exchange writes into a VTIMEZONE observance, is one --
	// would otherwise under-shoot by the whole excess and leave the loop below
	// stepping a second at a time across four centuries.
	if seconds, ok := subDailyRecurrenceSeconds(rule); ok && seconds > 0 && threshold.After(periodStart) {
		if steps := (threshold.Unix() - periodStart.Unix()) / seconds; steps > 0 {
			periodStart = time.Unix(periodStart.Unix()+steps*seconds, int64(periodStart.Nanosecond())).In(periodStart.Location())
		}
	}
	for scanned := 0; scanned < recurrenceScanLimit; scanned++ {
		next := advanceRecurrencePeriod(periodStart, rule)
		// advanceRecurrencePeriod returns its input unchanged on overflow and
		// for a frequency it does not recognize; either leaves the scan unable
		// to move, which without this guard is an unbounded loop.
		if next.After(threshold) || !next.After(periodStart) {
			return periodStart
		}
		periodStart = next
	}
	return periodStart
}

func subDailyRecurrenceStep(rule recurrenceRule) (time.Duration, bool) {
	seconds, ok := subDailyRecurrenceSeconds(rule)
	if !ok || seconds > int64(time.Duration(1<<63-1)/time.Second) {
		return 0, false
	}
	return time.Duration(seconds) * time.Second, true
}

func subDailyRecurrenceSeconds(rule recurrenceRule) (int64, bool) {
	interval := rule.Interval
	if interval <= 0 {
		interval = 1
	}
	unitSeconds, ok := recurrenceFrequencySeconds(rule.Freq)
	if !ok || int64(interval) > int64(^uint64(0)>>1)/unitSeconds {
		return 0, false
	}
	return int64(interval) * unitSeconds, true
}

func advanceRecurrencePeriod(periodStart time.Time, rule recurrenceRule) time.Time {
	interval := rule.Interval
	if interval <= 0 {
		interval = 1
	}
	switch rule.Freq {
	case "SECONDLY", "MINUTELY", "HOURLY":
		seconds, ok := subDailyRecurrenceSeconds(rule)
		if !ok || periodStart.Unix() > int64(^uint64(0)>>1)-seconds {
			return periodStart
		}
		return time.Unix(periodStart.Unix()+seconds, int64(periodStart.Nanosecond())).In(periodStart.Location())
	case "DAILY":
		return periodStart.AddDate(0, 0, interval)
	case "WEEKLY":
		return periodStart.AddDate(0, 0, 7*interval)
	case "MONTHLY":
		return periodStart.AddDate(0, interval, 0)
	case "YEARLY":
		return periodStart.AddDate(interval, 0, 0)
	default:
		return periodStart
	}
}

// recurrencePeriodWorkLimit bounds the date/time combinations one recurrence
// period may generate.
//
// A period's candidate set is the product of its days and its BYHOUR, BYMINUTE
// and BYSECOND lists, so a yearly rule naming every day, hour, minute and
// second describes about 31.5 million of them. Generating that set to answer a
// question about its first thousand entries is work no answer needs, and the
// PUT validation path reaches it. Exhausting the bound fails closed on
// max-instances, as sparseRecurrenceWorkLimit does: a period this large is over
// any instance limit the server advertises.
//
// One day cannot exceed the bound on its own -- 24 hours, 60 minutes and the 61
// second values BYSECOND admits come to 87,840 -- so a rule is only ever refused
// for spanning several such days. The sparse path is therefore always inside the
// bound: it generates a DAILY period, which is one day, or a sub-daily one, which
// is at most an hour.
const recurrencePeriodWorkLimit = 100_000

// visitRecurrenceCandidates hands visit each of the period's candidate starts in
// ascending order, stopping as soon as visit returns true. Candidates are
// streamed rather than collected so a caller that needs the first few -- every
// caller with a COUNT, an UNTIL or an instance limit -- pays for the first few.
//
// stopped reports that visit asked to stop. overBudget reports that the period
// exceeded recurrencePeriodWorkLimit, which leaves the candidate set only
// partly visited and is never a set the caller may treat as complete.
func visitRecurrenceCandidates(periodStart, dtstart time.Time, rule recurrenceRule, visit func(time.Time) bool) (stopped, overBudget bool) {
	selector := newBySetPosSelector(rule.BySetPos, visit)

	switch rule.Freq {
	case "SECONDLY", "MINUTELY", "HOURLY":
		// At most one hour of minutes and seconds, so the whole period is
		// already small enough to hold.
		candidates := subDailyCandidatesForPeriod(periodStart, dtstart, rule)
		sortTimes(candidates)
		for _, candidate := range uniqueTimes(candidates) {
			if selector.offer(candidate) {
				return true, false
			}
			if selector.exhausted() {
				break
			}
		}
		return selector.finish(), false
	}

	var days []time.Time
	switch rule.Freq {
	case "DAILY":
		days = []time.Time{periodStart}
	case "WEEKLY":
		days = weeklyDays(periodStart, dtstart, rule)
	case "MONTHLY":
		days = monthlyDays(periodStart.Year(), periodStart.Month(), dtstart, rule)
	case "YEARLY":
		days = yearlyDays(periodStart.Year(), dtstart, rule)
	default:
		return false, false
	}
	// The day list is bounded by the length of the period, so ordering it here
	// is what lets the times below stream out already sorted. It is deduplicated
	// by calendar date rather than by instant: a DST transition can land two
	// day-list entries on the same date at different instants (e.g. a gap that
	// normalizes one entry onto the previous day), and generating that date's
	// times twice would break both the ascending stream and BYSETPOS's count.
	sortTimes(days)
	days = uniqueDates(days)

	hours := defaultedInts(rule.ByHour, dtstart.Hour())
	minutes := defaultedInts(rule.ByMinute, dtstart.Minute())
	seconds := defaultedInts(rule.BySecond, dtstart.Second())
	perDay := len(hours) * len(minutes) * len(seconds)

	budget := recurrencePeriodWorkLimit
	dayTimes := make([]time.Time, 0, perDay)
	for _, day := range days {
		if !dateMatchesRule(day, rule) {
			continue
		}
		if perDay > budget {
			return false, true
		}
		budget -= perDay

		// A day is sorted rather than assumed ordered: a UTC offset change can
		// reorder one day's wall-clock times against each other, and never
		// reaches across the day boundary the outer loop steps over.
		dayTimes = dayTimes[:0]
		for _, hour := range hours {
			for _, minute := range minutes {
				for _, second := range seconds {
					dayTimes = appendValidTime(dayTimes, day.Year(), day.Month(), day.Day(), hour, minute, second, day.Location())
				}
			}
		}
		sortTimes(dayTimes)
		for _, candidate := range uniqueTimes(dayTimes) {
			if selector.offer(candidate) {
				return true, false
			}
			if selector.exhausted() {
				return selector.finish(), false
			}
		}
	}
	return selector.finish(), false
}

// bySetPosSelector applies BYSETPOS to a streamed period.
//
// Without BYSETPOS it is a pass-through. With it, RFC 5545 section 3.3.10
// numbers positions within the period, so a positive one is known as it streams
// past and a negative one needs only the tail: the selector holds
// max(|negative position|) candidates, never the period. Once the largest
// positive position has gone by and no negative position is waiting on the
// tail, nothing further can be selected and the period stops early.
type bySetPosSelector struct {
	positions []int
	visit     func(time.Time) bool

	seen      int
	maxPos    int
	tail      []time.Time
	tailStart int
	selected  []time.Time
	// lastOffered is how the ascending stream is made unique without holding
	// what came before it: a repeat can only ever be the value just seen.
	lastOffered time.Time
	offered     bool
}

func newBySetPosSelector(positions []int, visit func(time.Time) bool) *bySetPosSelector {
	selector := &bySetPosSelector{positions: positions, visit: visit}
	if len(positions) == 0 {
		return selector
	}
	tailLen := 0
	for _, position := range positions {
		switch {
		case position > 0 && position > selector.maxPos:
			selector.maxPos = position
		case position < 0 && -position > tailLen:
			tailLen = -position
		}
	}
	if tailLen > 0 {
		selector.tail = make([]time.Time, tailLen)
	}
	return selector
}

// offer takes the next candidate of the period in ascending order and reports
// whether the caller asked to stop.
func (s *bySetPosSelector) offer(candidate time.Time) bool {
	if s.offered && candidate.Equal(s.lastOffered) {
		return false
	}
	s.lastOffered = candidate
	s.offered = true
	if len(s.positions) == 0 {
		return s.visit(candidate)
	}
	s.seen++
	if s.seen <= s.maxPos {
		for _, position := range s.positions {
			if position == s.seen {
				s.selected = append(s.selected, candidate)
				break
			}
		}
	}
	if len(s.tail) > 0 {
		s.tail[s.tailStart] = candidate
		s.tailStart = (s.tailStart + 1) % len(s.tail)
	}
	return false
}

// finish selects the negative positions, which only the end of the period
// resolves, and hands the whole selection over in ascending order.
func (s *bySetPosSelector) finish() bool {
	if len(s.positions) == 0 {
		return false
	}
	for _, position := range s.positions {
		if position >= 0 {
			continue
		}
		// -1 names the period's last candidate, which is the slot before the
		// ring's next write. A position naming more than the period produced, or
		// more than the ring holds, selects nothing.
		offset := -position
		if offset > s.seen || offset > len(s.tail) {
			continue
		}
		s.selected = append(s.selected, s.tail[(s.tailStart-offset+2*len(s.tail))%len(s.tail)])
	}
	sortTimes(s.selected)
	for _, candidate := range uniqueTimes(s.selected) {
		if s.visit(candidate) {
			return true
		}
	}
	return false
}

// exhausted reports that nothing the period has left to offer can be selected,
// so the caller may stop generating it.
func (s *bySetPosSelector) exhausted() bool {
	return len(s.positions) > 0 && len(s.tail) == 0 && s.seen >= s.maxPos
}

func sortTimes(values []time.Time) {
	if len(values) < 2 {
		return
	}
	sort.Slice(values, func(i, j int) bool { return values[i].Before(values[j]) })
}

func subDailyCandidatesForPeriod(periodStart, dtstart time.Time, rule recurrenceRule) []time.Time {
	if !dateMatchesRule(periodStart, rule) {
		return nil
	}
	var candidates []time.Time
	switch rule.Freq {
	case "SECONDLY":
		if timeMatchesRule(periodStart, dtstart, rule) {
			candidates = append(candidates, periodStart)
		}
	case "MINUTELY":
		if !intMatchesIfPresent(periodStart.Hour(), rule.ByHour) || !intMatchesIfPresent(periodStart.Minute(), rule.ByMinute) {
			return nil
		}
		for _, second := range defaultedInts(rule.BySecond, dtstart.Second()) {
			candidates = appendValidTime(candidates, periodStart.Year(), periodStart.Month(), periodStart.Day(), periodStart.Hour(), periodStart.Minute(), second, periodStart.Location())
		}
	case "HOURLY":
		if !intMatchesIfPresent(periodStart.Hour(), rule.ByHour) {
			return nil
		}
		for _, minute := range defaultedInts(rule.ByMinute, dtstart.Minute()) {
			for _, second := range defaultedInts(rule.BySecond, dtstart.Second()) {
				candidates = appendValidTime(candidates, periodStart.Year(), periodStart.Month(), periodStart.Day(), periodStart.Hour(), minute, second, periodStart.Location())
			}
		}
	}
	return candidates
}

func defaultedInts(values []int, fallback int) []int {
	if len(values) > 0 {
		return values
	}
	return []int{fallback}
}

func appendValidTime(values []time.Time, year int, month time.Month, day, hour, minute, second int, loc *time.Location) []time.Time {
	if second == 60 {
		second = 59
	}
	if second < 0 || second > 59 {
		return values
	}
	t := time.Date(year, month, day, hour, minute, second, 0, loc)
	if t.Year() != year || t.Month() != month || t.Day() != day || t.Hour() != hour || t.Minute() != minute || t.Second() != second {
		return values
	}
	return append(values, t)
}

func timeMatchesRule(t, dtstart time.Time, rule recurrenceRule) bool {
	switch rule.Freq {
	case "SECONDLY":
		return intMatchesIfPresent(t.Hour(), rule.ByHour) &&
			intMatchesIfPresent(t.Minute(), rule.ByMinute) &&
			intMatchesIfPresent(t.Second(), rule.BySecond)
	case "MINUTELY":
		return intMatchesIfPresent(t.Hour(), rule.ByHour) &&
			intMatchesIfPresent(t.Minute(), rule.ByMinute) &&
			intMatchesOrDefault(t.Second(), rule.BySecond, dtstart.Second())
	case "HOURLY":
		return intMatchesIfPresent(t.Hour(), rule.ByHour) &&
			intMatchesOrDefault(t.Minute(), rule.ByMinute, dtstart.Minute()) &&
			intMatchesOrDefault(t.Second(), rule.BySecond, dtstart.Second())
	default:
		return intMatchesOrDefault(t.Hour(), rule.ByHour, dtstart.Hour()) &&
			intMatchesOrDefault(t.Minute(), rule.ByMinute, dtstart.Minute()) &&
			intMatchesOrDefault(t.Second(), rule.BySecond, dtstart.Second())
	}
}

func dateMatchesRule(day time.Time, rule recurrenceRule) bool {
	if len(rule.ByMonth) > 0 && !intInSlice(int(day.Month()), rule.ByMonth) {
		return false
	}
	if len(rule.ByMonthDay) > 0 && !monthDayMatches(day, rule.ByMonthDay) {
		return false
	}
	if len(rule.ByYearDay) > 0 && !yearDayMatches(day, rule.ByYearDay) {
		return false
	}
	if len(rule.ByDay) > 0 && !weekdayMatchesForRule(day, rule) {
		return false
	}
	return true
}

func intMatchesOrDefault(value int, allowed []int, fallback int) bool {
	if len(allowed) == 0 {
		return value == fallback
	}
	return intInSlice(value, allowed)
}

func intMatchesIfPresent(value int, allowed []int) bool {
	return len(allowed) == 0 || intInSlice(value, allowed)
}

func intInSlice(value int, allowed []int) bool {
	for _, candidate := range allowed {
		if candidate == value {
			return true
		}
	}
	return false
}

func startOfWeek(t time.Time, wkst time.Weekday) time.Time {
	day := time.Date(t.Year(), t.Month(), t.Day(), 0, 0, 0, 0, t.Location())
	offset := (int(day.Weekday()) - int(wkst) + 7) % 7
	return day.AddDate(0, 0, -offset)
}

func weeklyDays(periodStart, dtstart time.Time, rule recurrenceRule) []time.Time {
	if len(rule.ByDay) == 0 {
		offset := (int(dtstart.Weekday()) - int(rule.WKST) + 7) % 7
		return []time.Time{periodStart.AddDate(0, 0, offset)}
	}
	var days []time.Time
	for _, byDay := range rule.ByDay {
		offset := (int(byDay.Day) - int(rule.WKST) + 7) % 7
		days = append(days, periodStart.AddDate(0, 0, offset))
	}
	return days
}

func monthlyDays(year int, month time.Month, dtstart time.Time, rule recurrenceRule) []time.Time {
	loc := dtstart.Location()
	if len(rule.ByMonth) > 0 && !intInSlice(int(month), rule.ByMonth) {
		return nil
	}
	if len(rule.ByMonthDay) > 0 {
		var days []time.Time
		for _, monthDay := range rule.ByMonthDay {
			if day, ok := resolveMonthDay(year, month, monthDay, loc); ok {
				days = append(days, day)
			}
		}
		return days
	}
	if len(rule.ByDay) > 0 {
		return daysByWeekdayInMonth(year, month, rule.ByDay, loc)
	}
	if day, ok := resolveMonthDay(year, month, dtstart.Day(), loc); ok {
		return []time.Time{day}
	}
	return nil
}

func yearlyDays(year int, dtstart time.Time, rule recurrenceRule) []time.Time {
	loc := dtstart.Location()
	if len(rule.ByWeekNo) > 0 {
		return daysByWeekNoInYear(year, rule.ByWeekNo, rule.WKST, loc)
	}
	if len(rule.ByYearDay) > 0 {
		var days []time.Time
		yearLen := daysInYear(year)
		for _, yearDay := range rule.ByYearDay {
			dayNum := yearDay
			if dayNum < 0 {
				dayNum = yearLen + dayNum + 1
			}
			if dayNum < 1 || dayNum > yearLen {
				continue
			}
			days = append(days, time.Date(year, 1, dayNum, 0, 0, 0, 0, loc))
		}
		return days
	}

	if len(rule.ByMonth) == 0 && len(rule.ByDay) > 0 && len(rule.ByMonthDay) == 0 {
		if hasOrdinalWeekday(rule.ByDay) {
			return daysByWeekdayInYear(year, rule.ByDay, loc)
		}
		var days []time.Time
		for month := 1; month <= 12; month++ {
			days = append(days, monthlyDays(year, time.Month(month), dtstart, recurrenceRule{ByDay: rule.ByDay})...)
		}
		return days
	}

	months := rule.ByMonth
	if len(months) == 0 && len(rule.ByMonthDay) > 0 {
		months = []int{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12}
	}
	if len(months) == 0 {
		months = []int{int(dtstart.Month())}
	}
	var days []time.Time
	for _, month := range months {
		days = append(days, monthlyDays(year, time.Month(month), dtstart, recurrenceRule{
			ByMonthDay: rule.ByMonthDay,
			ByDay:      rule.ByDay,
		})...)
	}
	return days
}

func daysByWeekNoInYear(year int, weekNumbers []int, wkst time.Weekday, loc *time.Location) []time.Time {
	weeks := weeksInYear(year, wkst)
	var days []time.Time
	for _, weekNo := range weekNumbers {
		resolved := weekNo
		if resolved < 0 {
			resolved = weeks + resolved + 1
		}
		if resolved < 1 || resolved > weeks {
			continue
		}
		weekStart := firstWeekStart(year, wkst, loc).AddDate(0, 0, (resolved-1)*7)
		for day := 0; day < 7; day++ {
			days = append(days, weekStart.AddDate(0, 0, day))
		}
	}
	sortTimes(days)
	return uniqueTimes(days)
}

func weeksInYear(year int, wkst time.Weekday) int {
	start := firstWeekStart(year, wkst, time.UTC)
	next := firstWeekStart(year+1, wkst, time.UTC)
	return int(next.Sub(start).Hours() / (24 * 7))
}

func firstWeekStart(year int, wkst time.Weekday, loc *time.Location) time.Time {
	jan1 := time.Date(year, 1, 1, 0, 0, 0, 0, loc)
	weekStart := startOfWeek(jan1, wkst)
	daysInYearInWeek := 7 - int(jan1.Sub(weekStart).Hours()/24)
	if daysInYearInWeek >= 4 {
		return weekStart
	}
	return weekStart.AddDate(0, 0, 7)
}

func hasOrdinalWeekday(specifiers []weekdaySpecifier) bool {
	for _, spec := range specifiers {
		if spec.Ordinal != 0 {
			return true
		}
	}
	return false
}

func daysByWeekdayInYear(year int, specifiers []weekdaySpecifier, loc *time.Location) []time.Time {
	return daysByWeekday(daysInYear(year), specifiers, func(day int) time.Time {
		return time.Date(year, 1, day, 0, 0, 0, 0, loc)
	})
}

func nthWeekdayInYear(year int, spec weekdaySpecifier, loc *time.Location) (time.Time, bool) {
	return nthWeekday(daysInYear(year), spec, func(day int) time.Time {
		return time.Date(year, 1, day, 0, 0, 0, 0, loc)
	})
}

func resolveMonthDay(year int, month time.Month, monthDay int, loc *time.Location) (time.Time, bool) {
	days := daysInMonth(year, month)
	day := monthDay
	if day < 0 {
		day = days + day + 1
	}
	if day < 1 || day > days {
		return time.Time{}, false
	}
	return time.Date(year, month, day, 0, 0, 0, 0, loc), true
}

func daysByWeekdayInMonth(year int, month time.Month, specifiers []weekdaySpecifier, loc *time.Location) []time.Time {
	return daysByWeekday(daysInMonth(year, month), specifiers, func(day int) time.Time {
		return time.Date(year, month, day, 0, 0, 0, 0, loc)
	})
}

func nthWeekdayInMonth(year int, month time.Month, spec weekdaySpecifier, loc *time.Location) (time.Time, bool) {
	return nthWeekday(daysInMonth(year, month), spec, func(day int) time.Time {
		return time.Date(year, month, day, 0, 0, 0, 0, loc)
	})
}

func daysByWeekday(dayCount int, specifiers []weekdaySpecifier, dayAt func(int) time.Time) []time.Time {
	var days []time.Time
	for _, spec := range specifiers {
		if spec.Ordinal != 0 {
			if day, ok := nthWeekday(dayCount, spec, dayAt); ok {
				days = append(days, day)
			}
			continue
		}
		for day := 1; day <= dayCount; day++ {
			candidate := dayAt(day)
			if candidate.Weekday() == spec.Day {
				days = append(days, candidate)
			}
		}
	}
	return days
}

func nthWeekday(dayCount int, spec weekdaySpecifier, dayAt func(int) time.Time) (time.Time, bool) {
	if spec.Ordinal == 0 {
		return time.Time{}, false
	}
	day, step, countStep := 1, 1, 1
	if spec.Ordinal < 0 {
		day, step, countStep = dayCount, -1, -1
	}
	count := 0
	for ; day >= 1 && day <= dayCount; day += step {
		candidate := dayAt(day)
		if candidate.Weekday() != spec.Day {
			continue
		}
		count += countStep
		if count == spec.Ordinal {
			return candidate, true
		}
	}
	return time.Time{}, false
}

func monthDayMatches(day time.Time, allowed []int) bool {
	monthDay := day.Day()
	negativeDay := day.Day() - daysInMonth(day.Year(), day.Month()) - 1
	return intInSlice(monthDay, allowed) || intInSlice(negativeDay, allowed)
}

func yearDayMatches(day time.Time, allowed []int) bool {
	yearDay := day.YearDay()
	negativeDay := day.YearDay() - daysInYear(day.Year()) - 1
	return intInSlice(yearDay, allowed) || intInSlice(negativeDay, allowed)
}

func weekdayMatches(day time.Time, allowed []weekdaySpecifier) bool {
	for _, spec := range allowed {
		if spec.Ordinal == 0 && day.Weekday() == spec.Day {
			return true
		}
		if spec.Ordinal != 0 {
			if t, ok := nthWeekdayInMonth(day.Year(), day.Month(), spec, day.Location()); ok && sameDate(t, day) {
				return true
			}
		}
	}
	return false
}

func weekdayMatchesForRule(day time.Time, rule recurrenceRule) bool {
	if rule.Freq == "YEARLY" && len(rule.ByMonth) == 0 {
		for _, spec := range rule.ByDay {
			if spec.Ordinal == 0 && day.Weekday() == spec.Day {
				return true
			}
			if spec.Ordinal != 0 {
				if t, ok := nthWeekdayInYear(day.Year(), spec, day.Location()); ok && sameDate(t, day) {
					return true
				}
			}
		}
		return false
	}
	return weekdayMatches(day, rule.ByDay)
}

func uniqueTimes(values []time.Time) []time.Time {
	if len(values) < 2 {
		return values
	}
	unique := values[:1]
	for _, value := range values[1:] {
		if value.Equal(unique[len(unique)-1]) {
			continue
		}
		unique = append(unique, value)
	}
	return unique
}

// uniqueDates compacts a sorted slice down to one entry per calendar date. A
// DST transition can map two different instants onto the same date, and
// uniqueTimes' instant comparison would keep both.
func uniqueDates(values []time.Time) []time.Time {
	if len(values) < 2 {
		return values
	}
	unique := values[:1]
	for _, value := range values[1:] {
		if sameDate(value, unique[len(unique)-1]) {
			continue
		}
		unique = append(unique, value)
	}
	return unique
}

func daysInMonth(year int, month time.Month) int {
	return time.Date(year, month+1, 0, 0, 0, 0, 0, time.UTC).Day()
}

func daysInYear(year int) int {
	if time.Date(year, 12, 31, 0, 0, 0, 0, time.UTC).YearDay() == 366 {
		return 366
	}
	return 365
}

func sameDate(a, b time.Time) bool {
	return a.Year() == b.Year() && a.Month() == b.Month() && a.Day() == b.Day()
}
