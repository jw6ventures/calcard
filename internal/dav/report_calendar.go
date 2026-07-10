package dav

import (
	"context"
	"crypto/sha256"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/jw6ventures/calcard/internal/store"
)

func (h *DavServer) calendarReportResponses(ctx context.Context, user *store.User, cal *store.CalendarAccess, principalHref, resolvePath, responsePath string, report reportRequest) ([]response, string, error) {
	calData := reportCalendarData(report)
	switch report.XMLName.Local {
	case "calendar-multiget":
		res, err := h.calendarMultiGet(ctx, user, cal, report.Hrefs, resolvePath, responsePath, calData)
		return res, "", err
	case "calendar-query":
		res, err := h.calendarQuery(ctx, user, cal, responsePath, report.Filter, calData)
		return res, "", err
	case "free-busy-query":
		res, err := h.freeBusyQuery(ctx, user, cal, responsePath, report.Filter, report.TimeRange)
		return res, "", err
	case "sync-collection":
		return h.calendarSyncCollection(ctx, user, cal, principalHref, responsePath, report, calData)
	default:
		// RFC 3253 §3.6: unknown report types must be refused, not answered
		// with a full dump of the collection.
		return nil, "", errUnsupportedReport
	}
}

func (h *DavServer) applyCalendarFilter(events []store.Event, filter *calFilter) []store.Event {
	if filter == nil {
		return events
	}

	var filtered []store.Event
	for _, event := range events {
		if h.eventMatchesFilter(event, filter) {
			filtered = append(filtered, event)
		}
	}
	return filtered
}

func (h *DavServer) eventMatchesFilter(event store.Event, filter *calFilter) bool {
	return h.matchesCompFilter(event, &filter.CompFilter)
}

func (h *DavServer) matchesCompFilter(event store.Event, compFilter *compFilter) bool {
	compType := compFilter.Name
	if compType != "" && !h.hasComponent(event.RawICAL, compType) {
		return false
	}

	if compFilter.TimeRange != nil {
		if !h.eventInTimeRange(event, compFilter.TimeRange) {
			return false
		}
	}

	for _, nestedFilter := range compFilter.CompFilter {
		if !h.matchesCompFilter(event, &nestedFilter) {
			return false
		}
	}

	for _, propFilter := range compFilter.PropFilter {
		if !h.matchesPropFilter(event, &propFilter) {
			return false
		}
	}

	if compFilter.TextMatch != nil {
		if !h.matchesTextMatch(event.RawICAL, compFilter.TextMatch) {
			return false
		}
	}

	return true
}

func (h *DavServer) matchesPropFilter(event store.Event, propFilter *propFilter) bool {
	propName := strings.ToUpper(propFilter.Name)
	hasProp := strings.Contains(strings.ToUpper(event.RawICAL), propName+":")

	if propFilter.IsNotDefined != nil {
		return !hasProp
	}

	if !hasProp {
		return false
	}

	if propFilter.TextMatch != nil {
		return h.matchesTextMatch(event.RawICAL, propFilter.TextMatch)
	}

	return true
}

func (h *DavServer) matchesTextMatch(icalData string, textMatch *textMatch) bool {
	text := strings.TrimSpace(textMatch.Text)
	if text == "" {
		return true
	}

	// Case-insensitive contains check (simplified - RFC 4790 has more complex rules)
	matches := strings.Contains(strings.ToUpper(icalData), strings.ToUpper(text))

	if textMatch.NegateCondition == "yes" {
		return !matches
	}

	return matches
}

func (h *DavServer) hasComponent(icalData, componentType string) bool {
	componentType = strings.ToUpper(componentType)
	beginMarker := "BEGIN:" + componentType
	return strings.Contains(strings.ToUpper(icalData), beginMarker)
}

func (h *DavServer) eventInTimeRange(event store.Event, tr *timeRange) bool {
	start, end, ok := calendarTimeRangeBounds(tr)
	if !ok {
		return false
	}

	if eventHasRecurrence(event.RawICAL) {
		return h.recurringEventInTimeRange(event, start, end)
	}

	if event.DTStart != nil {
		eventEnd := event.DTEnd
		if eventEnd == nil {
			// If no end time, use start time
			eventEnd = event.DTStart
		}

		return event.DTStart.Before(end) && eventEnd.After(start)
	}

	return true
}

// effectiveTimeRange walks the comp-filter tree (VCALENDAR -> VEVENT -> ...) and
// returns the innermost time-range, which is the one that bounds matching
// components. It returns nil when no level carries a time-range.
func effectiveTimeRange(filter *calFilter) *timeRange {
	if filter == nil {
		return nil
	}
	return compFilterTimeRange(&filter.CompFilter)
}

func compFilterTimeRange(filter *compFilter) *timeRange {
	if filter == nil {
		return nil
	}
	for i := range filter.CompFilter {
		if tr := compFilterTimeRange(&filter.CompFilter[i]); tr != nil {
			return tr
		}
	}
	return filter.TimeRange
}

func calendarQueryVEventTimeRange(filter *calFilter) *timeRange {
	if filter == nil || !strings.EqualFold(filter.CompFilter.Name, "VCALENDAR") {
		return nil
	}
	// The store only derives dtstart/dtend metadata from VEVENT rows today.
	for i := range filter.CompFilter.CompFilter {
		child := &filter.CompFilter.CompFilter[i]
		if strings.EqualFold(child.Name, "VEVENT") {
			return compFilterTimeRange(child)
		}
	}
	return nil
}

// eventFilterFromCalFilter derives the SQL pushdown for a calendar-query
// time-range. It returns ok=false when there is no usable (valid) time-range, so
// the caller falls back to an unfiltered fetch. Only the time-range is pushed;
// prop-filter/text-match semantics are left to the in-memory pass. The result is
// a superset -- the store's recurrence_until bound keeps potentially-recurring
// rows, and applyCalendarFilter still runs to produce the exact set.
func eventFilterFromCalFilter(filter *calFilter) (store.EventFilter, bool) {
	return eventFilterFromTimeRange(calendarQueryVEventTimeRange(filter))
}

func eventFilterFromTimeRange(tr *timeRange) (store.EventFilter, bool) {
	if tr == nil {
		return store.EventFilter{}, false
	}
	start, end, ok := calendarTimeRangeBounds(tr)
	if !ok {
		return store.EventFilter{}, false
	}
	ef := store.EventFilter{}
	if !start.IsZero() {
		s := start
		ef.Start = &s
	}
	if !end.IsZero() {
		e := end
		ef.End = &e
	}
	if ef.Start == nil && ef.End == nil {
		return store.EventFilter{}, false
	}
	return ef, true
}

// listCalendarEventsForTimeRange narrows the database read using the given
// time-range when one is present, otherwise falls back to listing every event.
// The returned rows are a superset; callers must still apply exact filtering.
func (h *DavServer) listCalendarEventsForTimeRange(ctx context.Context, calendarID int64, tr *timeRange) ([]store.Event, error) {
	if ef, ok := eventFilterFromTimeRange(tr); ok {
		return h.store.Events.ListForCalendarFiltered(ctx, calendarID, ef)
	}
	return h.store.Events.ListForCalendar(ctx, calendarID)
}

// listCalendarEventsForFilter narrows the database read using the calendar-query
// time-range when one is present, otherwise falls back to listing every event.
// The returned rows are a superset; callers must still apply applyCalendarFilter
// for an exact match.
func (h *DavServer) listCalendarEventsForFilter(ctx context.Context, calendarID int64, filter *calFilter) ([]store.Event, error) {
	if ef, ok := eventFilterFromCalFilter(filter); ok {
		return h.store.Events.ListForCalendarFiltered(ctx, calendarID, ef)
	}
	return h.store.Events.ListForCalendar(ctx, calendarID)
}

func validCalendarFilterTimeRanges(filter *calFilter) bool {
	if filter == nil {
		return true
	}
	return validCompFilterTimeRanges(&filter.CompFilter)
}

func validTimeRange(tr *timeRange) bool {
	if tr == nil {
		return true
	}
	_, _, ok := calendarTimeRangeBounds(tr)
	return ok
}

func validCompFilterTimeRanges(filter *compFilter) bool {
	if filter.TimeRange != nil {
		if _, _, ok := calendarTimeRangeBounds(filter.TimeRange); !ok {
			return false
		}
	}
	for i := range filter.CompFilter {
		if !validCompFilterTimeRanges(&filter.CompFilter[i]) {
			return false
		}
	}
	return true
}

func calendarTimeRangeBounds(tr *timeRange) (time.Time, time.Time, bool) {
	if tr == nil {
		return time.Time{}, time.Time{}, false
	}

	var start time.Time
	var err error
	if strings.TrimSpace(tr.Start) != "" {
		start, err = parseICalDateTime(tr.Start)
		if err != nil {
			return time.Time{}, time.Time{}, false
		}
	}

	end := time.Date(9999, 12, 31, 23, 59, 59, 0, time.UTC)
	if strings.TrimSpace(tr.End) != "" {
		end, err = parseICalDateTime(tr.End)
		if err != nil {
			return time.Time{}, time.Time{}, false
		}
	}
	if start.IsZero() && strings.TrimSpace(tr.End) == "" {
		return time.Time{}, time.Time{}, false
	}
	if !start.IsZero() && !end.After(start) {
		return time.Time{}, time.Time{}, false
	}
	return start, end, true
}

func (h *DavServer) recurringEventInTimeRange(event store.Event, rangeStart, rangeEnd time.Time) bool {
	if event.DTStart == nil {
		return true
	}

	if !supportedEventRecurrence(event.RawICAL) {
		return true
	}
	return len(h.recurringFreeBusyPeriods(event, rangeStart, rangeEnd)) > 0
}

func (h *DavServer) freeBusyQuery(ctx context.Context, user *store.User, cal *store.CalendarAccess, cleanPath string, filter *calFilter, tr *timeRange) ([]response, error) {
	events, err := h.listCalendarEventsForTimeRange(ctx, cal.ID, freeBusyTimeRange(filter, tr))
	if err != nil {
		return nil, fmt.Errorf("failed to list events")
	}

	if filter != nil {
		events = h.applyCalendarFilter(events, filter)
	}
	if tr != nil {
		events = h.filterCalendarEventsByTimeRange(events, tr)
	}
	events, err = h.filterCalendarEventsByPrivilege(ctx, user, cal, events, "read-free-busy")
	if err != nil {
		return nil, err
	}

	freeBusyData := h.generateFreeBusy(events, filter, tr)

	href := strings.TrimSuffix(cleanPath, "/") + "/freebusy.ics"
	etag := fmt.Sprintf("%x", sha256.Sum256([]byte(freeBusyData)))

	return []response{
		resourceResponse(href, etagProp(etag, freeBusyData, true)),
	}, nil
}

func freeBusyTimeRange(filter *calFilter, tr *timeRange) *timeRange {
	if tr != nil {
		return tr
	}
	return effectiveTimeRange(filter)
}

func (h *DavServer) filterCalendarEventsByTimeRange(events []store.Event, tr *timeRange) []store.Event {
	if tr == nil {
		return events
	}
	filtered := make([]store.Event, 0, len(events))
	for _, event := range events {
		if h.eventInTimeRange(event, tr) {
			filtered = append(filtered, event)
		}
	}
	return filtered
}

func (h *DavServer) generateFreeBusy(events []store.Event, filter *calFilter, tr *timeRange) string {
	var sb strings.Builder
	sb.WriteString("BEGIN:VCALENDAR\r\n")
	sb.WriteString("VERSION:2.0\r\n")
	sb.WriteString("PRODID:-//CalCard//CalDAV Server//EN\r\n")
	sb.WriteString("BEGIN:VFREEBUSY\r\n")
	sb.WriteString(fmt.Sprintf("DTSTAMP:%s\r\n", time.Now().UTC().Format("20060102T150405Z")))

	freeBusyTR := freeBusyTimeRange(filter, tr)
	rangeStart, rangeEnd, hasRange := calendarTimeRangeBounds(freeBusyTR)
	if freeBusyTR != nil {
		if freeBusyTR.Start != "" {
			sb.WriteString(fmt.Sprintf("DTSTART:%s\r\n", freeBusyTR.Start))
		}
		if freeBusyTR.End != "" {
			sb.WriteString(fmt.Sprintf("DTEND:%s\r\n", freeBusyTR.End))
		}
	}

	for _, event := range events {
		for _, period := range h.freeBusyPeriods(event, rangeStart, rangeEnd, hasRange) {
			startStr := period.start.UTC().Format("20060102T150405Z")
			endStr := period.end.UTC().Format("20060102T150405Z")
			sb.WriteString(fmt.Sprintf("FREEBUSY:%s/%s\r\n", startStr, endStr))
		}
	}

	sb.WriteString("END:VFREEBUSY\r\n")
	sb.WriteString("END:VCALENDAR\r\n")

	return sb.String()
}

type busyPeriod struct {
	start time.Time
	end   time.Time
}

func (h *DavServer) freeBusyPeriods(event store.Event, rangeStart, rangeEnd time.Time, hasRange bool) []busyPeriod {
	if event.DTStart == nil {
		return nil
	}

	endTime := event.DTEnd
	if endTime == nil {
		endTime = event.DTStart
	}
	if !hasRange {
		return []busyPeriod{{start: *event.DTStart, end: *endTime}}
	}

	if !eventHasRecurrence(event.RawICAL) {
		if event.DTStart.Before(rangeEnd) && endTime.After(rangeStart) {
			return []busyPeriod{{start: *event.DTStart, end: *endTime}}
		}
		return nil
	}

	return h.recurringFreeBusyPeriods(event, rangeStart, rangeEnd)
}

func (h *DavServer) recurringFreeBusyPeriods(event store.Event, rangeStart, rangeEnd time.Time) []busyPeriod {
	component := primaryVEventComponent(event.RawICAL)
	dtstart, ok := recurringEventStart(event, component)
	if !ok {
		return nil
	}

	duration := recurringEventDuration(event, component, dtstart)
	exdates := eventExDates(component)
	overrides := eventRecurrenceOverrides(event.RawICAL, duration)
	seen := make(map[string]struct{})
	periods := make([]busyPeriod, 0)
	addPeriod := func(period busyPeriod, suppressGeneratedOverride bool, applyExDates bool) {
		if len(periods) >= caldavMaxInstances {
			return
		}
		if suppressGeneratedOverride && isOverrideRecurrenceID(period.start, overrides) {
			return
		}
		if applyExDates && isExcludedDate(period.start, exdates) {
			return
		}
		if !periodOverlaps(period.start, period.end, rangeStart, rangeEnd) {
			return
		}
		key := period.start.UTC().Format(time.RFC3339Nano) + "/" + period.end.UTC().Format(time.RFC3339Nano)
		if _, ok := seen[key]; ok {
			return
		}
		seen[key] = struct{}{}
		periods = append(periods, period)
	}

	if rrule := componentPropertyValue(component, "RRULE"); rrule != "" {
		scanStart, scanEnd := rangeStart, rangeEnd
		var transform func(busyPeriod) (busyPeriod, bool)
		if hasThisAndFutureOverrides(overrides) {
			if shift := maxThisAndFutureShift(overrides); shift > 0 {
				scanStart = scanStart.Add(-shift)
				scanEnd = scanEnd.Add(shift)
			}
			transform = func(period busyPeriod) (busyPeriod, bool) {
				return applyThisAndFutureOverrides(period, overrides)
			}
		}
		periods, ok := rruleBusyPeriods(dtstart, duration, rrule, exdates, scanStart, scanEnd, transform)
		if !ok {
			addPeriod(busyPeriod{start: dtstart, end: dtstart.Add(duration)}, true, true)
		}
		for _, period := range periods {
			addPeriod(period, true, true)
		}
	} else if len(eventRDatePeriods(event.RawICAL)) > 0 {
		addPeriod(busyPeriod{start: dtstart, end: dtstart.Add(duration)}, true, true)
	}

	for _, rdate := range eventRDatePeriods(event.RawICAL) {
		end := rdate.end
		if end.IsZero() {
			end = rdate.start.Add(duration)
		}
		addPeriod(busyPeriod{start: rdate.start, end: end}, true, true)
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
	start time.Time
	end   time.Time
}

func eventHasRecurrence(ical string) bool {
	component := primaryVEventComponent(ical)
	return componentPropertyValue(component, "RRULE") != "" || len(eventRDatePeriods(ical)) > 0
}

func supportedEventRecurrence(ical string) bool {
	rrule := componentPropertyValue(primaryVEventComponent(ical), "RRULE")
	if rrule == "" {
		return true
	}
	return supportedRecurrenceFreq(extractRRuleParam(rrule, "FREQ"))
}

func rruleBusyPeriods(dtstart time.Time, duration time.Duration, rrule string, exdates []time.Time, rangeStart, rangeEnd time.Time, transform func(busyPeriod) (busyPeriod, bool)) ([]busyPeriod, bool) {
	rule, ok := parseRecurrenceRule(rrule, dtstart.Location())
	if !ok {
		return nil, false
	}
	var periods []busyPeriod

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
			period := busyPeriod{start: current, end: current.Add(duration)}
			if transform != nil {
				var skip bool
				period, skip = transform(period)
				if skip {
					continue
				}
			}
			if periodOverlaps(period.start, period.end, rangeStart, rangeEnd) && !isExcludedDate(current, exdates) {
				periods = append(periods, period)
			}
			if len(periods) >= caldavMaxInstances {
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

func recurringEventStart(event store.Event, component *vEventComponent) (time.Time, bool) {
	if prop, ok := componentProperty(component, "DTSTART"); ok {
		if dtstart, ok := parseICalPropertyDateTimeLocal(prop.keyPart, prop.value); ok {
			return dtstart, true
		}
	}
	if event.DTStart != nil {
		return *event.DTStart, true
	}
	return time.Time{}, false
}

func recurringEventDuration(event store.Event, component *vEventComponent, dtstart time.Time) time.Duration {
	if prop, ok := componentProperty(component, "DTEND"); ok {
		if dtend, ok := parseICalPropertyDateTimeLocal(prop.keyPart, prop.value); ok {
			if d := dtend.Sub(dtstart); d > 0 {
				return d
			}
		}
	}
	if prop, ok := componentProperty(component, "DURATION"); ok {
		if d, ok := parseICalDuration(prop.value); ok && d > 0 {
			return d
		}
	}
	if event.DTEnd != nil {
		if d := event.DTEnd.Sub(dtstart); d > 0 {
			return d
		}
	}
	if event.AllDay {
		return 24 * time.Hour
	}
	if prop, ok := componentProperty(component, "DTSTART"); ok && propertyParamEquals(prop.keyPart, "VALUE", "DATE") {
		return 24 * time.Hour
	}
	return time.Hour
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
		for _, value := range strings.Split(prop.value, ",") {
			value = strings.TrimSpace(value)
			if value == "" {
				continue
			}
			if strings.Contains(value, "/") {
				parts := strings.SplitN(value, "/", 2)
				start, ok := parseICalPropertyDateTime(prop.keyPart, strings.TrimSpace(parts[0]))
				if !ok {
					continue
				}
				var end time.Time
				periodEnd := strings.TrimSpace(parts[1])
				if strings.HasPrefix(strings.ToUpper(periodEnd), "P") {
					if duration, ok := parseICalDuration(periodEnd); ok {
						end = start.Add(duration)
					}
				} else {
					if parsedEnd, ok := parseICalPropertyDateTime(prop.keyPart, periodEnd); ok {
						end = parsedEnd
					}
				}
				periods = append(periods, rdatePeriod{start: start, end: end})
				continue
			}
			if start, ok := parseICalPropertyDateTime(prop.keyPart, value); ok {
				periods = append(periods, rdatePeriod{start: start})
			}
		}
	}
	return periods
}

func eventExDates(component *vEventComponent) []time.Time {
	var dates []time.Time
	for _, prop := range componentProperties(component, "EXDATE") {
		for _, value := range strings.Split(prop.value, ",") {
			if parsed, ok := parseICalPropertyDateTime(prop.keyPart, strings.TrimSpace(value)); ok {
				dates = append(dates, parsed)
			}
		}
	}
	return dates
}

type recurrenceOverride struct {
	recurrenceID       time.Time
	period             busyPeriod
	cancelled          bool
	rangeThisAndFuture bool
}

func eventRecurrenceOverrides(ical string, fallbackDuration time.Duration) []recurrenceOverride {
	var overrides []recurrenceOverride
	for _, component := range vEventComponents(ical) {
		recurrenceIDProp, ok := componentProperty(&component, "RECURRENCE-ID")
		if !ok {
			continue
		}
		recurrenceID, ok := parseICalPropertyDateTime(recurrenceIDProp.keyPart, recurrenceIDProp.value)
		if !ok {
			continue
		}

		start := recurrenceID
		if prop, ok := componentProperty(&component, "DTSTART"); ok {
			if parsed, ok := parseICalPropertyDateTimeLocal(prop.keyPart, prop.value); ok {
				start = parsed
			}
		}

		cancelled := false
		if prop, ok := componentProperty(&component, "STATUS"); ok {
			cancelled = strings.EqualFold(strings.TrimSpace(prop.value), "CANCELLED")
		}

		end := start.Add(fallbackDuration)
		if prop, ok := componentProperty(&component, "DTEND"); ok {
			if parsed, ok := parseICalPropertyDateTimeLocal(prop.keyPart, prop.value); ok && parsed.After(start) {
				end = parsed
			}
		} else if prop, ok := componentProperty(&component, "DURATION"); ok {
			if duration, ok := parseICalDuration(prop.value); ok && duration > 0 {
				end = start.Add(duration)
			}
		}

		overrides = append(overrides, recurrenceOverride{
			recurrenceID:       recurrenceID,
			period:             busyPeriod{start: start, end: end},
			cancelled:          cancelled,
			rangeThisAndFuture: propertyParamEquals(recurrenceIDProp.keyPart, "RANGE", "THISANDFUTURE"),
		})
	}
	return overrides
}

func applyThisAndFutureOverrides(period busyPeriod, overrides []recurrenceOverride) (busyPeriod, bool) {
	var selected *recurrenceOverride
	for i := range overrides {
		override := &overrides[i]
		if !override.rangeThisAndFuture || period.start.Before(override.recurrenceID) {
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

	delta := selected.period.start.Sub(selected.recurrenceID)
	shiftedStart := period.start.Add(delta)
	duration := selected.period.end.Sub(selected.period.start)
	if duration <= 0 {
		duration = period.end.Sub(period.start)
	}
	return busyPeriod{start: shiftedStart, end: shiftedStart.Add(duration)}, false
}

func maxThisAndFutureShift(overrides []recurrenceOverride) time.Duration {
	var max time.Duration
	for _, override := range overrides {
		if !override.rangeThisAndFuture || override.cancelled {
			continue
		}
		shift := override.period.start.Sub(override.recurrenceID)
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

type vEventComponent struct {
	properties []eventPropertyValue
}

type eventPropertyValue struct {
	keyPart string
	value   string
}

func eventPropertyValues(ical, name string) []eventPropertyValue {
	return componentProperties(primaryVEventComponent(ical), name)
}

func primaryVEventComponent(ical string) *vEventComponent {
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

func vEventComponents(ical string) []vEventComponent {
	var components []vEventComponent
	depth := 0
	componentDepth := 0
	var current *vEventComponent
	for _, rawLine := range unfoldICalLines(ical) {
		line := strings.TrimSpace(rawLine)
		upper := strings.ToUpper(line)
		switch {
		case strings.HasPrefix(upper, "BEGIN:"):
			depth++
			if current == nil && depth == 2 && strings.EqualFold(strings.TrimSpace(line[len("BEGIN:"):]), "VEVENT") {
				componentDepth = depth
				current = &vEventComponent{}
			}
			continue
		case strings.HasPrefix(upper, "END:"):
			if current != nil && componentDepth == depth && strings.EqualFold(strings.TrimSpace(line[len("END:"):]), "VEVENT") {
				components = append(components, *current)
				current = nil
				componentDepth = 0
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
		colonIdx := strings.IndexByte(line, ':')
		if colonIdx < 0 {
			continue
		}
		current.properties = append(current.properties, eventPropertyValue{
			keyPart: line[:colonIdx],
			value:   strings.TrimSpace(line[colonIdx+1:]),
		})
	}
	return components
}

func componentProperties(component *vEventComponent, name string) []eventPropertyValue {
	var values []eventPropertyValue
	if component == nil {
		return values
	}
	for _, prop := range component.properties {
		if !strings.EqualFold(icalPropertyName(prop.keyPart), name) {
			continue
		}
		values = append(values, prop)
	}
	return values
}

func componentProperty(component *vEventComponent, name string) (eventPropertyValue, bool) {
	values := componentProperties(component, name)
	if len(values) == 0 {
		return eventPropertyValue{}, false
	}
	return values[0], true
}

func componentPropertyValue(component *vEventComponent, name string) string {
	prop, ok := componentProperty(component, name)
	if !ok {
		return ""
	}
	return prop.value
}

func componentHasProperty(component *vEventComponent, name string) bool {
	_, ok := componentProperty(component, name)
	return ok
}

func propertyParamEquals(keyPart, param, value string) bool {
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

func icalPropertyName(keyPart string) string {
	if idx := strings.IndexAny(keyPart, ";:"); idx >= 0 {
		return keyPart[:idx]
	}
	return keyPart
}

func parseICalPropertyDateTime(keyPart, value string) (time.Time, bool) {
	value = strings.TrimSpace(value)
	if value == "" {
		return time.Time{}, false
	}
	for _, param := range strings.Split(keyPart, ";")[1:] {
		if strings.HasPrefix(strings.ToUpper(param), "TZID=") {
			tzid := strings.TrimSpace(param[len("TZID="):])
			if loc, err := time.LoadLocation(tzid); err == nil {
				if parsed, err := parseICalDateTimeInLocation(value, loc); err == nil {
					return parsed, true
				}
			}
			break
		}
	}
	parsed, err := parseICalDateTime(value)
	return parsed, err == nil
}

func parseICalPropertyDateTimeLocal(keyPart, value string) (time.Time, bool) {
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
	parsed, err := parseICalDateTime(value)
	return parsed, err == nil
}

func parseICalDuration(value string) (time.Duration, bool) {
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
	if loc != nil && !hasICalZoneSuffix(value) {
		for _, format := range icalLocalFormats {
			if parsed, err := time.ParseInLocation(format, value, loc); err == nil {
				return parsed, true
			}
		}
	}
	parsed, err := parseICalDateTime(value)
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
	var days []time.Time
	for _, spec := range specifiers {
		if spec.Ordinal == 0 {
			for day := 1; day <= daysInYear(year); day++ {
				t := time.Date(year, 1, day, 0, 0, 0, 0, loc)
				if t.Weekday() == spec.Day {
					days = append(days, t)
				}
			}
			continue
		}
		if t, ok := nthWeekdayInYear(year, spec, loc); ok {
			days = append(days, t)
		}
	}
	return days
}

func nthWeekdayInYear(year int, spec weekdaySpecifier, loc *time.Location) (time.Time, bool) {
	if spec.Ordinal > 0 {
		count := 0
		for day := 1; day <= daysInYear(year); day++ {
			t := time.Date(year, 1, day, 0, 0, 0, 0, loc)
			if t.Weekday() != spec.Day {
				continue
			}
			count++
			if count == spec.Ordinal {
				return t, true
			}
		}
		return time.Time{}, false
	}
	count := 0
	for day := daysInYear(year); day >= 1; day-- {
		t := time.Date(year, 1, day, 0, 0, 0, 0, loc)
		if t.Weekday() != spec.Day {
			continue
		}
		count--
		if count == spec.Ordinal {
			return t, true
		}
	}
	return time.Time{}, false
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
	var days []time.Time
	for _, spec := range specifiers {
		if spec.Ordinal == 0 {
			for day := 1; day <= daysInMonth(year, month); day++ {
				t := time.Date(year, month, day, 0, 0, 0, 0, loc)
				if t.Weekday() == spec.Day {
					days = append(days, t)
				}
			}
			continue
		}
		if t, ok := nthWeekdayInMonth(year, month, spec, loc); ok {
			days = append(days, t)
		}
	}
	return days
}

func nthWeekdayInMonth(year int, month time.Month, spec weekdaySpecifier, loc *time.Location) (time.Time, bool) {
	if spec.Ordinal > 0 {
		count := 0
		for day := 1; day <= daysInMonth(year, month); day++ {
			t := time.Date(year, month, day, 0, 0, 0, 0, loc)
			if t.Weekday() != spec.Day {
				continue
			}
			count++
			if count == spec.Ordinal {
				return t, true
			}
		}
		return time.Time{}, false
	}
	count := 0
	for day := daysInMonth(year, month); day >= 1; day-- {
		t := time.Date(year, month, day, 0, 0, 0, 0, loc)
		if t.Weekday() != spec.Day {
			continue
		}
		count--
		if count == spec.Ordinal {
			return t, true
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

func (h *DavServer) calendarQuery(ctx context.Context, user *store.User, cal *store.CalendarAccess, cleanPath string, filter *calFilter, calData *calendarDataEl) ([]response, error) {
	events, err := h.listCalendarEventsForFilter(ctx, cal.ID, filter)
	if err != nil {
		return nil, fmt.Errorf("failed to list events")
	}

	if filter != nil {
		events = h.applyCalendarFilter(events, filter)
	}
	events, err = h.filterReadableCalendarEvents(ctx, user, cal, events)
	if err != nil {
		return nil, err
	}

	return calendarResourceResponsesFiltered(cleanPath, events, calData), nil
}

func (h *DavServer) calendarMultiGet(ctx context.Context, user *store.User, cal *store.CalendarAccess, hrefs []string, resolvePath, responsePath string, calData *calendarDataEl) ([]response, error) {
	if len(hrefs) == 0 {
		return h.calendarQuery(ctx, user, cal, responsePath, nil, calData)
	}
	responseBase := strings.TrimSuffix(responsePath, "/") + "/"
	var responses []response
	for _, href := range hrefs {
		cleanHref := resolveDAVHref(resolvePath, href)
		if cleanHref == "" {
			continue
		}
		segment, uid, ok := parseCalendarResourceSegments(cleanHref)
		if !ok || !calendarSegmentMatches(cal, segment) {
			continue
		}
		responseHref := responseBase + uid + ".ics"
		ev, err := h.store.Events.GetByResourceName(ctx, cal.ID, uid)
		if err != nil {
			return nil, fmt.Errorf("failed to fetch event")
		}
		if ev == nil {
			responses = append(responses, response{Href: responseHref, Status: httpStatusNotFound})
			continue
		}
		allowed, err := h.canReadCalendarObject(ctx, user, cal, uid)
		if err != nil {
			return nil, err
		}
		if !allowed {
			responses = append(responses, response{Href: responseHref, Status: httpStatusNotFound})
			continue
		}
		rawData := filterICalendarData(ev.RawICAL, calData)
		responses = append(responses, resourceResponse(responseHref, etagProp(ev.ETag, rawData, true)))
	}
	return responses, nil
}

func calendarSegmentMatches(cal *store.CalendarAccess, segment string) bool {
	if segment == "" {
		return false
	}
	if segment == strconv.FormatInt(cal.ID, 10) {
		return true
	}
	normalizedSegment := strings.ToLower(segment)
	if cal.Slug != nil && *cal.Slug == normalizedSegment {
		return true
	}
	return cal.Name == segment
}

func (h *DavServer) calendarSyncCollection(ctx context.Context, user *store.User, cal *store.CalendarAccess, principalHref, cleanPath string, report reportRequest, calData *calendarDataEl) ([]response, string, error) {
	syncToken, _ := h.calendarSyncTokenValue(ctx, cal)
	collectionHref := strings.TrimSuffix(cleanPath, "/") + "/"

	var since time.Time
	if report.SyncToken != "" {
		info, err := parseSyncToken(report.SyncToken)
		if err != nil || info.Kind != "cal" || info.ID != cal.ID {
			return nil, "", errInvalidSyncToken
		}
		since = info.Timestamp
	}

	var events []store.Event
	var err error
	if since.IsZero() {
		events, err = h.store.Events.ListForCalendar(ctx, cal.ID)
	} else {
		events, err = h.store.Events.ListModifiedSince(ctx, cal.ID, since)
	}
	if err != nil {
		return nil, "", fmt.Errorf("failed to list events")
	}
	allEvents := events
	events, err = h.filterReadableCalendarEvents(ctx, user, cal, events)
	if err != nil {
		return nil, "", err
	}

	responses := []response{
		calendarCollectionResponseWithPrivileges(collectionHref, cal.Name, cal.Description, cal.Timezone, cal.Color, principalHref, syncToken, strconv.FormatInt(cal.CTag, 10), cal.EffectivePrivileges()),
	}
	responses = append(responses, calendarResourceResponsesFiltered(collectionHref, events, calData)...)

	// Include deleted resources if this is an incremental sync
	if !since.IsZero() {
		deletedHrefs := make(map[string]struct{})
		visible := make(map[string]struct{}, len(events))
		for _, event := range events {
			visible[eventResourceName(event)] = struct{}{}
		}
		for _, event := range allEvents {
			if !event.LastModified.After(since) {
				continue
			}
			resourceName := eventResourceName(event)
			if _, ok := visible[resourceName]; ok {
				continue
			}
			href := collectionHref + resourceName + ".ics"
			responses = append(responses, deletedResponse(href))
			deletedHrefs[href] = struct{}{}
		}
		deleted, err := h.store.DeletedResources.ListDeletedSince(ctx, "event", cal.ID, since)
		if err != nil {
			return nil, "", fmt.Errorf("failed to list deleted events")
		}
		for _, d := range deleted {
			resourceName := d.ResourceName
			if resourceName == "" {
				resourceName = d.UID
			}
			href := collectionHref + resourceName + ".ics"
			if _, ok := deletedHrefs[href]; ok {
				continue
			}
			responses = append(responses, deletedResponse(href))
			deletedHrefs[href] = struct{}{}
		}
	}

	return responses, syncToken, nil
}
