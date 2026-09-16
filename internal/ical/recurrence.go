// Package ical parses iCalendar content lines and expands recurrence rules
// (RRULE, RDATE, EXDATE, RECURRENCE-ID overrides). It has no knowledge of
// DAV or storage types; callers hand it raw iCalendar text and times.
package ical

import (
	"errors"
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
// It returns ErrRecurrenceExpansionLimit if the work budget is exhausted.
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
	if err != nil {
		return nil, err
	}
	periods := make([]BusyPeriod, 0, len(occurrences))
	for _, occurrence := range occurrences {
		period := occurrence.period
		period.RecurrenceID = occurrence.recurrenceID
		periods = append(periods, period)
	}
	return periods, nil
}

// RecurrenceInstances expands the recurrence set of the named component and
// returns every *generated* instance whose occurrence window touches
// [rangeStart, rangeEnd], each paired with the slot of the master's pattern it
// belongs to. Overridden instances are left out: RFC 4791 §9.7.1 scopes a
// comp-filter to each component of the resource, so an override is matched as
// the component it is rather than through its master's pattern. Both bounds are
// inclusive, because the caller re-applies the exact §9.9 condition and an
// exclusive bound here would drop an occurrence that starts precisely at the end
// of the range. Work budget exhaustion returns ErrRecurrenceExpansionLimit.
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
	if err != nil {
		return nil, err
	}
	instances := make([]RecurrenceInstance, 0, len(occurrences))
	for _, occurrence := range occurrences {
		instances = append(instances, RecurrenceInstance{
			RecurrenceID: occurrence.recurrenceID,
			Start:        occurrence.period.Start,
		})
	}
	return instances, nil
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
	rdates := componentRDatePeriods(component, expansion.resolve)
	seen := make(map[string]struct{})
	periods := make([]recurrenceOccurrence, 0)
	// recurrenceID is the slot the occurrence belongs to, which a
	// RANGE=THISANDFUTURE shift moves the period away from. EXDATE and an
	// ordinary RECURRENCE-ID override both name that slot, not the shifted start.
	addPeriod := func(recurrenceID time.Time, original BusyPeriod, generated, applyExDates bool) {
		if len(periods) >= expansion.maxInstances {
			return
		}
		if applyExDates && isExcludedDate(recurrenceID, exdates) {
			return
		}
		effective := original
		suppressed := false
		exactOverride := false
		var governing time.Time
		if generated {
			effective, suppressed, governing = applyThisAndFutureOverrides(original, overrides)
			exactOverride = isOverrideRecurrenceID(recurrenceID, overrides)
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
		if hasThisAndFutureOverrides(overrides) {
			if shift := maxThisAndFutureShift(overrides); shift > 0 {
				scanStart = scanStart.Add(-shift)
				scanEnd = scanEnd.Add(shift)
			}
		}
		scanExpansion := expansion
		scanExpansion.reaches = func(_ BusyPeriod, effective BusyPeriod, _ bool, rangeStart, rangeEnd time.Time) bool {
			return periodTouches(effective.Start, effective.End, rangeStart, rangeEnd)
		}
		generated, ok, err := rruleBusyPeriods(component, dtstart, duration, rrule, exdates, scanStart, scanEnd, scanExpansion)
		if err != nil {
			return nil, err
		}
		if !ok {
			addPeriod(dtstart, BusyPeriod{Start: dtstart, End: dtstart.Add(duration)}, true, true)
		}
		for _, occurrence := range generated {
			addPeriod(occurrence.recurrenceID, occurrence.period, true, true)
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

	return periods, nil
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
	overrides := make(map[string]bool)
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
		key := recurrenceInstantKey(parsed)
		cancelled := strings.EqualFold(componentPropertyValue(component, "STATUS"), "CANCELLED")
		overrides[key] = cancelled
		if !cancelled {
			instances["override:"+key] = struct{}{}
		}
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
		if _, replaced := overrides[key]; replaced {
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
			occurrences++
			if rule.Count > 0 && occurrences > rule.Count {
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
	}
	mayProduceCandidate := occurrences > 0 || recurrenceRuleMayProduceCandidate(dtstart, rule)
	if rule.Count > 0 && mayProduceCandidate {
		maxSuppressed := len(excluded)
		for _, cancelled := range overrides {
			if cancelled {
				maxSuppressed++
			}
		}
		if rule.Count-maxSuppressed > limit {
			return true, true
		}
	}
	if !mayProduceCandidate {
		return len(instances) > limit, true
	}
	if exceeds, handled := finishSparseRecurrence(periodStart, dtstart, rule, &occurrences, add); handled {
		return exceeds, true
	}
	// An unbounded rule that could not produce limit+1 candidates within the
	// scan guard is conservatively over the advertised server limit.
	if rule.Count == 0 && rule.Until == nil {
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

func recurrenceBySetPosMaySelect(dtstart time.Time, rule recurrenceRule) bool {
	clockCandidates := len(defaultedInts(rule.ByHour, dtstart.Hour())) *
		len(defaultedInts(rule.ByMinute, dtstart.Minute())) *
		validSecondCandidateCount(rule.BySecond)
	maxCandidates := clockCandidates
	switch rule.Freq {
	case "SECONDLY":
		maxCandidates = 1
	case "MINUTELY":
		maxCandidates = validSecondCandidateCount(rule.BySecond)
	case "HOURLY":
		maxCandidates = len(defaultedInts(rule.ByMinute, dtstart.Minute())) *
			validSecondCandidateCount(rule.BySecond)
	case "WEEKLY":
		maxCandidates *= 7
	case "MONTHLY":
		maxCandidates *= 31
	case "YEARLY":
		maxCandidates *= 366
	}
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

// LatestRecurrenceOnOrBefore returns the latest generated recurrence start at
// or before the supplied wall-clock value. It is used to apply the observance
// rules in a submitted VTIMEZONE definition.
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
	threshold := before
	if rule.Until != nil && rule.Until.Before(threshold) {
		threshold = *rule.Until
	}
	periodStart := recurrencePeriodStart(dtstart, rule)
	if rule.Count == 0 {
		periodStart = fastForwardRecurrencePeriod(periodStart, threshold, rule)
	}

	occurrences := 0
	if rule.Count > 0 {
		periodStart = recurrencePeriodStart(dtstart, rule)
		for scanned := 0; scanned < recurrenceScanLimit; scanned++ {
			done := false
			_, overBudget := visitRecurrenceCandidates(periodStart, dtstart, rule, func(candidate time.Time) bool {
				if candidate.Before(dtstart) {
					return false
				}
				if rule.Until != nil && candidate.After(*rule.Until) {
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

	for scanned := 0; scanned < 1000; scanned++ {
		// The whole period is wanted here, so a period too large to generate
		// leaves no usable answer: without BYSETPOS the maximum reached is only
		// the maximum of the days that were generated, and with it nothing is
		// selected at all, because BYSETPOS resolves only once the period ends.
		_, overBudget := visitRecurrenceCandidates(periodStart, dtstart, rule, func(candidate time.Time) bool {
			if candidate.Before(dtstart) || candidate.After(before) {
				return false
			}
			if rule.Until != nil && candidate.After(*rule.Until) {
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
			return time.Time{}, false, true
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

func rruleBusyPeriods(component *Component, dtstart time.Time, duration time.Duration, rrule string, exdates []time.Time, rangeStart, rangeEnd time.Time, expansion recurrenceExpansion) ([]recurrenceOccurrence, bool, error) {
	recurrenceStart, resolveCandidate, civil := recurrenceGenerationStart(component, dtstart, expansion.resolve)
	rule, ok := parseRecurrenceRule(rrule, recurrenceStart.Location(), expansion.resolve)
	if !ok {
		return nil, false, nil
	}
	var periods []recurrenceOccurrence

	scanStart, scanEnd := rangeStart, rangeEnd
	if civil {
		// A valid UTC offset is strictly less than 24 hours. Padding the absolute
		// request bounds by that amount produces a safe civil-time scan window;
		// every candidate is still resolved and checked against the exact bounds.
		scanStart = scanStart.Add(-recurrenceCivilScanPad)
		scanEnd = scanEnd.Add(recurrenceCivilScanPad)
	}
	periodStart := recurrencePeriodStart(recurrenceStart, rule)
	occurrences := 0
	if rule.Count == 0 {
		periodStart = fastForwardRecurrencePeriod(periodStart, scanStart.Add(-duration), rule)
	} else if fastForwardedStart, skipped, ok := fastForwardCountedSubDailyRecurrence(periodStart, scanStart.Add(-duration), rule); ok {
		if skipped >= rule.Count {
			return periods, true, nil
		}
		periodStart = fastForwardedStart
		occurrences = skipped
	}
	done := false
	visit := func(current time.Time) bool {
		if current.Before(recurrenceStart) {
			return false
		}
		resolvedCurrent, resolved := resolveCandidate(current)
		if !resolved {
			return false
		}
		if rule.Until != nil && resolvedCurrent.After(*rule.Until) {
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
		if expansion.reaches(period, period, false, rangeStart, rangeEnd) && !isExcludedDate(resolvedCurrent, exdates) {
			periods = append(periods, recurrenceOccurrence{recurrenceID: resolvedCurrent, original: period, period: period})
		}
		if len(periods) >= expansion.maxInstances {
			done = true
			return true
		}
		return false
	}
	for scanned := 0; scanned < recurrenceScanLimit; scanned++ {
		_, overBudget := visitRecurrenceCandidates(periodStart, recurrenceStart, rule, visit)
		if overBudget {
			return nil, false, ErrRecurrenceExpansionLimit
		}
		if done {
			return periods, true, nil
		}
		next := advanceRecurrencePeriod(periodStart, rule)
		if !next.After(periodStart) {
			return nil, false, ErrRecurrenceExpansionLimit
		}
		periodStart = next
		if periodStart.After(scanEnd) || (rule.Count > 0 && occurrences >= rule.Count) {
			return periods, true, nil
		}
	}
	switch rule.Freq {
	case "SECONDLY", "MINUTELY", "HOURLY", "DAILY":
		_, overBudget := visitSparseRecurrence(periodStart, recurrenceStart, scanEnd, rule, visit)
		if !overBudget {
			return periods, true, nil
		}
	}
	return nil, false, ErrRecurrenceExpansionLimit
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
	step, ok := subDailyRecurrenceStep(rule)
	if !ok || !fixedStepSubDailyRecurrence(rule) {
		return periodStart, 0, false
	}
	if !threshold.After(periodStart) {
		return periodStart, 0, true
	}
	steps := int(threshold.Sub(periodStart) / step)
	if steps <= 0 {
		return periodStart, 0, true
	}
	return periodStart.Add(time.Duration(steps) * step), steps, true
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

func isExcludedDate(start time.Time, exdates []time.Time) bool {
	for _, exdate := range exdates {
		if start.Equal(exdate) {
			return true
		}
	}
	return false
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

func eventExDates(component *Component, resolve PropertyTimeResolver) []time.Time {
	var dates []time.Time
	for _, prop := range componentProperties(component, "EXDATE") {
		for _, value := range strings.Split(prop.Value, ",") {
			if parsed, ok := resolve.or(prop.KeyPart, strings.TrimSpace(value)); ok {
				dates = append(dates, parsed)
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

func applyThisAndFutureOverrides(period BusyPeriod, overrides []recurrenceOverride) (BusyPeriod, bool, time.Time) {
	var selected *recurrenceOverride
	for i := range overrides {
		override := &overrides[i]
		if !override.rangeThisAndFuture || period.Start.Before(override.recurrenceID) {
			continue
		}
		if selected == nil || override.recurrenceID.After(selected.recurrenceID) {
			selected = override
		}
	}
	if selected == nil {
		return period, false, time.Time{}
	}
	if selected.cancelled {
		return period, true, selected.recurrenceID
	}

	delta := selected.period.Start.Sub(selected.recurrenceID)
	shiftedStart := period.Start.Add(delta)
	duration := selected.period.End.Sub(selected.period.Start)
	if duration <= 0 {
		duration = period.End.Sub(period.Start)
	}
	return BusyPeriod{Start: shiftedStart, End: shiftedStart.Add(duration)}, false, selected.recurrenceID
}

func maxThisAndFutureShift(overrides []recurrenceOverride) time.Duration {
	var max time.Duration
	for _, override := range overrides {
		if !override.rangeThisAndFuture || override.cancelled {
			continue
		}
		shift := override.period.Start.Sub(override.recurrenceID)
		if shift < 0 {
			shift = -shift
		}
		if shift > max {
			max = shift
		}
	}
	return max
}

func hasThisAndFutureOverrides(overrides []recurrenceOverride) bool {
	for _, override := range overrides {
		if override.rangeThisAndFuture {
			return true
		}
	}
	return false
}

func isOverrideRecurrenceID(start time.Time, overrides []recurrenceOverride) bool {
	for _, override := range overrides {
		if !override.rangeThisAndFuture && start.Equal(override.recurrenceID) {
			return true
		}
	}
	return false
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
		colonIdx := strings.IndexByte(rawLine, ':')
		if colonIdx < 0 {
			current.malformedProperties = append(current.malformedProperties, rawLine)
			continue
		}
		current.properties = append(current.properties, PropertyValue{
			KeyPart: rawLine[:colonIdx],
			Value:   rawLine[colonIdx+1:],
		})
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
func PropertyParam(keyPart, param string) (string, bool) {
	parts := strings.Split(keyPart, ";")
	if len(parts) < 2 {
		return "", false
	}
	for _, part := range parts[1:] {
		kv := strings.SplitN(strings.TrimSpace(part), "=", 2)
		if len(kv) != 2 {
			continue
		}
		if strings.EqualFold(strings.TrimSpace(kv[0]), param) {
			return strings.TrimSpace(kv[1]), true
		}
	}
	return "", false
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
	for _, param := range strings.Split(keyPart, ";")[1:] {
		if strings.HasPrefix(strings.ToUpper(param), "TZID=") {
			tzid := strings.TrimSpace(param[len("TZID="):])
			if loc, err := time.LoadLocation(tzid); err == nil {
				if parsed, err := ParseDateTimeInLocation(value, loc); err == nil {
					return parsed, true
				}
			}
			break
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
	if step, ok := subDailyRecurrenceStep(rule); ok && threshold.After(periodStart) {
		steps := int(threshold.Sub(periodStart) / step)
		if steps > 0 {
			periodStart = periodStart.Add(time.Duration(steps) * step)
		}
	}
	for {
		next := advanceRecurrencePeriod(periodStart, rule)
		if next.After(threshold) {
			return periodStart
		}
		periodStart = next
	}
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
