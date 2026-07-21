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
		if !strings.EqualFold(propertyName(prop.KeyPart), name) {
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

func propertyName(keyPart string) string {
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
				for _, format := range icalLocalFormats {
					if parsed, err := time.ParseInLocation(format, value, loc); err == nil {
						return parsed, true
					}
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

const recurrenceScanLimit = 100000

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
			continue
		}
		params[strings.ToUpper(strings.TrimSpace(kv[0]))] = strings.TrimSpace(kv[1])
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
		if err != nil || interval <= 0 {
			return recurrenceRule{}, false
		}
		rule.Interval = interval
	}
	if countStr := params["COUNT"]; countStr != "" {
		count, err := strconv.Atoi(countStr)
		if err != nil || count <= 0 {
			return recurrenceRule{}, false
		}
		rule.Count = count
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
	if rule.BySecond, ok = parseIntList(params["BYSECOND"], 0, 59); !ok {
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
	return rule, true
}

func parseRecurrenceUntil(value string, loc *time.Location) (time.Time, bool) {
	if loc != nil && !hasZoneSuffix(value) {
		for _, format := range icalLocalFormats {
			if parsed, err := time.ParseInLocation(format, value, loc); err == nil {
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
	interval := rule.Interval
	if interval <= 0 {
		interval = 1
	}
	switch rule.Freq {
	case "SECONDLY":
		return time.Duration(interval) * time.Second, true
	case "MINUTELY":
		return time.Duration(interval) * time.Minute, true
	case "HOURLY":
		return time.Duration(interval) * time.Hour, true
	default:
		return 0, false
	}
}

func advanceRecurrencePeriod(periodStart time.Time, rule recurrenceRule) time.Time {
	interval := rule.Interval
	if interval <= 0 {
		interval = 1
	}
	switch rule.Freq {
	case "SECONDLY":
		return periodStart.Add(time.Duration(interval) * time.Second)
	case "MINUTELY":
		return periodStart.Add(time.Duration(interval) * time.Minute)
	case "HOURLY":
		return periodStart.Add(time.Duration(interval) * time.Hour)
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
