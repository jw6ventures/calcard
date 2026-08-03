// Package ical parses iCalendar content lines and expands recurrence rules
// (RRULE, RDATE, EXDATE, RECURRENCE-ID overrides). It has no knowledge of
// DAV or storage types; callers hand it raw iCalendar text and times.
package ical

import (
	"sort"
	"strconv"
	"strings"
	"time"
)

// BusyPeriod is one concrete busy interval of an event occurrence.
type BusyPeriod struct {
	Start time.Time
	End   time.Time
}

// RecurringBusyPeriods expands a recurring event (RRULE, RDATE, EXDATE and
// RECURRENCE-ID overrides, including RANGE=THISANDFUTURE) into the concrete
// busy periods that overlap [rangeStart, rangeEnd). dtstart and duration are
// the event's resolved start and occurrence length; maxInstances caps the
// expansion.
func RecurringBusyPeriods(raw string, dtstart time.Time, duration time.Duration, rangeStart, rangeEnd time.Time, maxInstances int) []BusyPeriod {
	component := PrimaryVEventComponent(raw)
	exdates := eventExDates(component)
	overrides := eventRecurrenceOverrides(raw, duration)
	seen := make(map[string]struct{})
	periods := make([]BusyPeriod, 0)
	addPeriod := func(period BusyPeriod, suppressGeneratedOverride bool, applyExDates bool) {
		if len(periods) >= maxInstances {
			return
		}
		if suppressGeneratedOverride && isOverrideRecurrenceID(period.Start, overrides) {
			return
		}
		if applyExDates && isExcludedDate(period.Start, exdates) {
			return
		}
		if !periodOverlaps(period.Start, period.End, rangeStart, rangeEnd) {
			return
		}
		key := period.Start.UTC().Format(time.RFC3339Nano) + "/" + period.End.UTC().Format(time.RFC3339Nano)
		if _, ok := seen[key]; ok {
			return
		}
		seen[key] = struct{}{}
		periods = append(periods, period)
	}

	if rrule := componentPropertyValue(component, "RRULE"); rrule != "" {
		scanStart, scanEnd := rangeStart, rangeEnd
		var transform func(BusyPeriod) (BusyPeriod, bool)
		if hasThisAndFutureOverrides(overrides) {
			if shift := maxThisAndFutureShift(overrides); shift > 0 {
				scanStart = scanStart.Add(-shift)
				scanEnd = scanEnd.Add(shift)
			}
			transform = func(period BusyPeriod) (BusyPeriod, bool) {
				return applyThisAndFutureOverrides(period, overrides)
			}
		}
		periods, ok := rruleBusyPeriods(dtstart, duration, rrule, exdates, scanStart, scanEnd, maxInstances, transform)
		if !ok {
			addPeriod(BusyPeriod{Start: dtstart, End: dtstart.Add(duration)}, true, true)
		}
		for _, period := range periods {
			addPeriod(period, true, true)
		}
	} else if len(eventRDatePeriods(raw)) > 0 {
		addPeriod(BusyPeriod{Start: dtstart, End: dtstart.Add(duration)}, true, true)
	}

	for _, rdate := range eventRDatePeriods(raw) {
		end := rdate.End
		if end.IsZero() {
			end = rdate.Start.Add(duration)
		}
		addPeriod(BusyPeriod{Start: rdate.Start, End: end}, true, true)
	}

	for _, override := range overrides {
		if override.cancelled {
			continue
		}
		addPeriod(override.period, false, false)
	}

	return periods
}

type rdatePeriod struct {
	Start time.Time
	End   time.Time
}

func EventHasRecurrence(ical string) bool {
	component := PrimaryVEventComponent(ical)
	return componentPropertyValue(component, "RRULE") != "" || len(eventRDatePeriods(ical)) > 0
}

func SupportedEventRecurrence(ical string) bool {
	rrule := componentPropertyValue(PrimaryVEventComponent(ical), "RRULE")
	if rrule == "" {
		return true
	}
	return supportedRecurrenceFreq(extractRRuleParam(rrule, "FREQ"))
}

// ValidRecurrenceRule reports whether value uses the recurrence grammar this
// package can expand. It is intentionally independent of a DTSTART timezone;
// callers that expand the rule parse it again with the DTSTART location.
func ValidRecurrenceRule(value string) bool {
	_, ok := parseRecurrenceRule(value, time.UTC)
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

	var master *VEventComponent
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
		parsed, ok := parsePropertyDateTime(recurrenceID.KeyPart, recurrenceID.Value)
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
		dtstart, ok = parsePropertyDateTime(dtstartProperty.KeyPart, dtstartProperty.Value)
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
	rule, ok := parseRecurrenceRule(rrule, dtstart.Location())
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
		for _, current := range recurrenceCandidatesForPeriod(periodStart, dtstart, rule) {
			if current.Before(dtstart) {
				continue
			}
			if rule.Until != nil && current.After(*rule.Until) {
				return len(instances) > limit, true
			}
			occurrences++
			if rule.Count > 0 && occurrences > rule.Count {
				return len(instances) > limit, true
			}
			if add(current) {
				return true, true
			}
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

// Sparse rules are evaluated exactly while work remains. Exhausting the bound
// fails closed on max-instances instead of letting one PUT monopolize a worker.
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
	remainingWork := sparseRecurrenceWorkLimit
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

	for year := periodStart.Year(); rule.Until == nil || year <= until.Year(); year++ {
		for dayNumber := 1; dayNumber <= daysInYear(year); dayNumber++ {
			remainingWork--
			if remainingWork <= 0 {
				return true, true
			}
			day := time.Date(year, 1, dayNumber, 0, 0, 0, 0, dtstart.Location())
			if !dateMatchesRule(day, rule) {
				continue
			}
			if rule.Freq == "DAILY" {
				if day.Before(periodStart) || !dailyRecurrencePeriodAligned(day, dtstart, rule.Interval) {
					continue
				}
				for _, current := range recurrenceCandidatesForPeriod(day, dtstart, rule) {
					if current.Before(dtstart) {
						continue
					}
					if addCandidate(current) {
						return exceeds, complete
					}
				}
				continue
			}

			if visitSparseSubDailyPeriods(day, periodStart, dtstart, until, rule, &remainingWork, addCandidate) {
				if remainingWork <= 0 {
					return true, true
				}
				return exceeds, complete
			}
		}
	}
	return false, true
}

func visitSparseSubDailyPeriods(day, periodStart, dtstart, until time.Time, rule recurrenceRule, remainingWork *int, add func(time.Time) bool) bool {
	stepSeconds, ok := subDailyRecurrenceSeconds(rule)
	if !ok {
		return false
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
			return true
		}
		period := time.Unix(periodUnix, 0).In(day.Location())
		for _, current := range recurrenceCandidatesForPeriod(period, dtstart, rule) {
			if current.Before(dtstart) || (!until.IsZero() && current.After(until)) {
				continue
			}
			if add(current) {
				return true
			}
		}
		if periodUnix > int64(^uint64(0)>>1)-stepSeconds {
			break
		}
		periodUnix += stepSeconds
	}
	return false
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
		parsed, ok := parsePropertyDateTime(property.KeyPart, start)
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
func LatestRecurrenceOnOrBefore(dtstart, before time.Time, rrule string) (time.Time, bool) {
	rule, ok := parseRecurrenceRule(rrule, dtstart.Location())
	if !ok || before.Before(dtstart) {
		return time.Time{}, false
	}
	threshold := before
	if rule.Until != nil && rule.Until.Before(threshold) {
		threshold = *rule.Until
	}
	periodStart := recurrencePeriodStart(dtstart, rule)
	if rule.Count == 0 {
		periodStart = fastForwardRecurrencePeriod(periodStart, threshold, rule)
	}

	var latest time.Time
	occurrences := 0
	if rule.Count > 0 {
		periodStart = recurrencePeriodStart(dtstart, rule)
		for scanned := 0; scanned < recurrenceScanLimit; scanned++ {
			for _, candidate := range recurrenceCandidatesForPeriod(periodStart, dtstart, rule) {
				if candidate.Before(dtstart) {
					continue
				}
				if rule.Until != nil && candidate.After(*rule.Until) {
					return latest, !latest.IsZero()
				}
				occurrences++
				if occurrences > rule.Count || candidate.After(before) {
					return latest, !latest.IsZero()
				}
				latest = candidate
			}
			periodStart = advanceRecurrencePeriod(periodStart, rule)
		}
		return latest, !latest.IsZero()
	}

	for scanned := 0; scanned < 1000; scanned++ {
		for _, candidate := range recurrenceCandidatesForPeriod(periodStart, dtstart, rule) {
			if candidate.Before(dtstart) || candidate.After(before) {
				continue
			}
			if rule.Until != nil && candidate.After(*rule.Until) {
				continue
			}
			if latest.IsZero() || candidate.After(latest) {
				latest = candidate
			}
		}
		if !latest.IsZero() {
			return latest, true
		}
		previous := retreatRecurrencePeriod(periodStart, rule)
		if !previous.Before(periodStart) || previous.Before(dtstart) {
			break
		}
		periodStart = previous
	}
	return time.Time{}, false
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

func rruleBusyPeriods(dtstart time.Time, duration time.Duration, rrule string, exdates []time.Time, rangeStart, rangeEnd time.Time, maxInstances int, transform func(BusyPeriod) (BusyPeriod, bool)) ([]BusyPeriod, bool) {
	rule, ok := parseRecurrenceRule(rrule, dtstart.Location())
	if !ok {
		return nil, false
	}
	var periods []BusyPeriod

	periodStart := recurrencePeriodStart(dtstart, rule)
	occurrences := 0
	if rule.Count == 0 {
		periodStart = fastForwardRecurrencePeriod(periodStart, rangeStart.Add(-duration), rule)
	} else if fastForwardedStart, skipped, ok := fastForwardCountedSubDailyRecurrence(periodStart, rangeStart.Add(-duration), rule); ok {
		if skipped >= rule.Count {
			return periods, true
		}
		periodStart = fastForwardedStart
		occurrences = skipped
	}
	for scanned := 0; scanned < recurrenceScanLimit; scanned++ {
		candidates := recurrenceCandidatesForPeriod(periodStart, dtstart, rule)
		for _, current := range candidates {
			if current.Before(dtstart) {
				continue
			}
			if rule.Until != nil && current.After(*rule.Until) {
				return periods, true
			}
			occurrences++
			if rule.Count > 0 && occurrences > rule.Count {
				return periods, true
			}
			period := BusyPeriod{Start: current, End: current.Add(duration)}
			if transform != nil {
				var skip bool
				period, skip = transform(period)
				if skip {
					continue
				}
			}
			if periodOverlaps(period.Start, period.End, rangeStart, rangeEnd) && !isExcludedDate(current, exdates) {
				periods = append(periods, period)
			}
			if len(periods) >= maxInstances {
				return periods, true
			}
		}

		next := advanceRecurrencePeriod(periodStart, rule)
		if !next.After(periodStart) {
			return periods, true
		}
		periodStart = next
		if rule.Count == 0 && periodStart.After(rangeEnd) {
			return periods, true
		}
	}
	return periods, true
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

func isExcludedDate(start time.Time, exdates []time.Time) bool {
	for _, exdate := range exdates {
		if start.Equal(exdate) {
			return true
		}
	}
	return false
}

func eventRDatePeriods(ical string) []rdatePeriod {
	var periods []rdatePeriod
	for _, prop := range eventPropertyValues(ical, "RDATE") {
		for _, value := range strings.Split(prop.Value, ",") {
			value = strings.TrimSpace(value)
			if value == "" {
				continue
			}
			if strings.Contains(value, "/") {
				parts := strings.SplitN(value, "/", 2)
				start, ok := parsePropertyDateTime(prop.KeyPart, strings.TrimSpace(parts[0]))
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
					if parsedEnd, ok := parsePropertyDateTime(prop.KeyPart, periodEnd); ok {
						end = parsedEnd
					}
				}
				periods = append(periods, rdatePeriod{Start: start, End: end})
				continue
			}
			if start, ok := parsePropertyDateTime(prop.KeyPart, value); ok {
				periods = append(periods, rdatePeriod{Start: start})
			}
		}
	}
	return periods
}

func eventExDates(component *VEventComponent) []time.Time {
	var dates []time.Time
	for _, prop := range componentProperties(component, "EXDATE") {
		for _, value := range strings.Split(prop.Value, ",") {
			if parsed, ok := parsePropertyDateTime(prop.KeyPart, strings.TrimSpace(value)); ok {
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

func eventRecurrenceOverrides(ical string, fallbackDuration time.Duration) []recurrenceOverride {
	var overrides []recurrenceOverride
	for _, component := range vEventComponents(ical) {
		recurrenceIDProp, ok := ComponentProperty(&component, "RECURRENCE-ID")
		if !ok {
			continue
		}
		recurrenceID, ok := parsePropertyDateTime(recurrenceIDProp.KeyPart, recurrenceIDProp.Value)
		if !ok {
			continue
		}

		start := recurrenceID
		if prop, ok := ComponentProperty(&component, "DTSTART"); ok {
			if parsed, ok := ParsePropertyDateTimeLocal(prop.KeyPart, prop.Value); ok {
				start = parsed
			}
		}

		cancelled := false
		if prop, ok := ComponentProperty(&component, "STATUS"); ok {
			cancelled = strings.EqualFold(strings.TrimSpace(prop.Value), "CANCELLED")
		}

		end := start.Add(fallbackDuration)
		if prop, ok := ComponentProperty(&component, "DTEND"); ok {
			if parsed, ok := ParsePropertyDateTimeLocal(prop.KeyPart, prop.Value); ok && parsed.After(start) {
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

func applyThisAndFutureOverrides(period BusyPeriod, overrides []recurrenceOverride) (BusyPeriod, bool) {
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
		return period, false
	}
	if selected.cancelled {
		return period, true
	}

	delta := selected.period.Start.Sub(selected.recurrenceID)
	shiftedStart := period.Start.Add(delta)
	duration := selected.period.End.Sub(selected.period.Start)
	if duration <= 0 {
		duration = period.End.Sub(period.Start)
	}
	return BusyPeriod{Start: shiftedStart, End: shiftedStart.Add(duration)}, false
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

// VEventComponent is one VEVENT block's parsed content lines.
type VEventComponent struct {
	properties          []PropertyValue
	malformedProperties []string
}

// PropertyValue is one content line of a component: the part before the
// first colon (name plus parameters) and the value after it.
type PropertyValue struct {
	KeyPart string
	Value   string
}

func eventPropertyValues(ical, name string) []PropertyValue {
	return componentProperties(PrimaryVEventComponent(ical), name)
}

func PrimaryVEventComponent(ical string) *VEventComponent {
	components := vEventComponents(ical)
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

func vEventComponents(ical string) []VEventComponent {
	return topLevelComponents(ical, func(name string) bool {
		return strings.EqualFold(name, "VEVENT")
	})
}

func topLevelComponents(ical string, accept func(string) bool) []VEventComponent {
	return topLevelComponentsFromLines(UnfoldLines(ical), accept)
}

func topLevelComponentsFromLines(lines []string, accept func(string) bool) []VEventComponent {
	var components []VEventComponent
	depth := 0
	componentDepth := 0
	componentName := ""
	var current *VEventComponent
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
				current = &VEventComponent{}
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

func componentProperties(component *VEventComponent, name string) []PropertyValue {
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

func ComponentProperty(component *VEventComponent, name string) (PropertyValue, bool) {
	values := componentProperties(component, name)
	if len(values) == 0 {
		return PropertyValue{}, false
	}
	return values[0], true
}

func componentPropertyValue(component *VEventComponent, name string) string {
	prop, ok := ComponentProperty(component, name)
	if !ok {
		return ""
	}
	return strings.TrimSpace(prop.Value)
}

func componentHasProperty(component *VEventComponent, name string) bool {
	_, ok := ComponentProperty(component, name)
	return ok
}

func PropertyParamEquals(keyPart, param, value string) bool {
	parts := strings.Split(keyPart, ";")
	if len(parts) < 2 {
		return false
	}
	for _, part := range parts[1:] {
		kv := strings.SplitN(strings.TrimSpace(part), "=", 2)
		if len(kv) != 2 {
			continue
		}
		if strings.EqualFold(strings.TrimSpace(kv[0]), param) && strings.EqualFold(strings.TrimSpace(kv[1]), value) {
			return true
		}
	}
	return false
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

func parsePropertyDateTime(keyPart, value string) (time.Time, bool) {
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

func parseRecurrenceRule(rrule string, loc *time.Location) (recurrenceRule, bool) {
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
		until, ok := parseRecurrenceUntil(untilStr, loc)
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

func parseRecurrenceUntil(value string, loc *time.Location) (time.Time, bool) {
	if loc != nil && !hasZoneSuffix(value) {
		if parsed, err := ParseDateTimeInLocation(value, loc); err == nil {
			return parsed, true
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

func recurrenceCandidatesForPeriod(periodStart, dtstart time.Time, rule recurrenceRule) []time.Time {
	hours := defaultedInts(rule.ByHour, dtstart.Hour())
	minutes := defaultedInts(rule.ByMinute, dtstart.Minute())
	seconds := defaultedInts(rule.BySecond, dtstart.Second())
	var candidates []time.Time
	addTimesForDay := func(day time.Time) {
		if !dateMatchesRule(day, rule) {
			return
		}
		for _, hour := range hours {
			for _, minute := range minutes {
				for _, second := range seconds {
					candidates = appendValidTime(candidates, day.Year(), day.Month(), day.Day(), hour, minute, second, day.Location())
				}
			}
		}
	}

	switch rule.Freq {
	case "SECONDLY", "MINUTELY", "HOURLY":
		candidates = subDailyCandidatesForPeriod(periodStart, dtstart, rule)
	case "DAILY":
		addTimesForDay(periodStart)
	case "WEEKLY":
		days := weeklyDays(periodStart, dtstart, rule)
		for _, day := range days {
			addTimesForDay(day)
		}
	case "MONTHLY":
		for _, day := range monthlyDays(periodStart.Year(), periodStart.Month(), dtstart, rule) {
			addTimesForDay(day)
		}
	case "YEARLY":
		for _, day := range yearlyDays(periodStart.Year(), dtstart, rule) {
			addTimesForDay(day)
		}
	}

	sort.Slice(candidates, func(i, j int) bool { return candidates[i].Before(candidates[j]) })
	candidates = uniqueTimes(candidates)
	if len(rule.BySetPos) > 0 {
		candidates = applyBySetPos(candidates, rule.BySetPos)
	}
	return candidates
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
	sort.Slice(days, func(i, j int) bool { return days[i].Before(days[j]) })
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

func applyBySetPos(candidates []time.Time, positions []int) []time.Time {
	var selected []time.Time
	for _, pos := range positions {
		idx := pos
		if idx > 0 {
			idx--
		} else {
			idx = len(candidates) + idx
		}
		if idx < 0 || idx >= len(candidates) {
			continue
		}
		selected = append(selected, candidates[idx])
	}
	sort.Slice(selected, func(i, j int) bool { return selected[i].Before(selected[j]) })
	return uniqueTimes(selected)
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
