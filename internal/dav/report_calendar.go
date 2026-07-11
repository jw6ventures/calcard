package dav

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/jw6ventures/calcard/internal/ical"
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
	// Uppercase the body once per event; the matchers below run once per
	// filter node and would otherwise each re-uppercase the full iCal text.
	return h.matchesCompFilter(event, strings.ToUpper(event.RawICAL), &filter.CompFilter)
}

func (h *DavServer) matchesCompFilter(event store.Event, upperICAL string, compFilter *compFilter) bool {
	compType := compFilter.Name
	if compType != "" && !h.hasComponent(upperICAL, compType) {
		return false
	}

	if compFilter.TimeRange != nil {
		if !h.eventInTimeRange(event, compFilter.TimeRange) {
			return false
		}
	}

	for _, nestedFilter := range compFilter.CompFilter {
		if !h.matchesCompFilter(event, upperICAL, &nestedFilter) {
			return false
		}
	}

	for _, propFilter := range compFilter.PropFilter {
		if !h.matchesPropFilter(upperICAL, &propFilter) {
			return false
		}
	}

	if compFilter.TextMatch != nil {
		if !h.matchesTextMatch(upperICAL, compFilter.TextMatch) {
			return false
		}
	}

	return true
}

func (h *DavServer) matchesPropFilter(upperICAL string, propFilter *propFilter) bool {
	propName := strings.ToUpper(propFilter.Name)
	hasProp := strings.Contains(upperICAL, propName+":")

	if propFilter.IsNotDefined != nil {
		return !hasProp
	}

	if !hasProp {
		return false
	}

	if propFilter.TextMatch != nil {
		return h.matchesTextMatch(upperICAL, propFilter.TextMatch)
	}

	return true
}

// matchesTextMatch expects icalData already uppercased by the caller.
func (h *DavServer) matchesTextMatch(upperICAL string, textMatch *textMatch) bool {
	text := strings.TrimSpace(textMatch.Text)
	if text == "" {
		return true
	}

	// Case-insensitive contains check (simplified - RFC 4790 has more complex rules)
	matches := strings.Contains(upperICAL, strings.ToUpper(text))

	if textMatch.NegateCondition == "yes" {
		return !matches
	}

	return matches
}

// hasComponent expects icalData already uppercased by the caller.
func (h *DavServer) hasComponent(upperICAL, componentType string) bool {
	return strings.Contains(upperICAL, "BEGIN:"+strings.ToUpper(componentType))
}

func (h *DavServer) eventInTimeRange(event store.Event, tr *timeRange) bool {
	start, end, ok := calendarTimeRangeBounds(tr)
	if !ok {
		return false
	}

	if ical.EventHasRecurrence(event.RawICAL) {
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
		start, err = ical.ParseDateTime(tr.Start)
		if err != nil {
			return time.Time{}, time.Time{}, false
		}
	}

	end := time.Date(9999, 12, 31, 23, 59, 59, 0, time.UTC)
	if strings.TrimSpace(tr.End) != "" {
		end, err = ical.ParseDateTime(tr.End)
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

	if !ical.SupportedEventRecurrence(event.RawICAL) {
		return true
	}
	return len(h.recurringFreeBusyPeriods(event, rangeStart, rangeEnd)) > 0
}

// freeBusyQuery returns the free-busy iCalendar text for the calendar's
// events visible to user within the requested filter/time range.
func (h *DavServer) freeBusyQuery(ctx context.Context, user *store.User, cal *store.CalendarAccess, filter *calFilter, tr *timeRange) (string, error) {
	events, err := h.listCalendarEventsForTimeRange(ctx, cal.ID, freeBusyTimeRange(filter, tr))
	if err != nil {
		return "", fmt.Errorf("failed to list events")
	}

	if filter != nil {
		events = h.applyCalendarFilter(events, filter)
	}
	if tr != nil {
		events = h.filterCalendarEventsByTimeRange(events, tr)
	}
	events, err = h.filterCalendarEventsByPrivilege(ctx, user, cal, events, "read-free-busy")
	if err != nil {
		return "", err
	}

	return h.generateFreeBusy(events, filter, tr), nil
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
			startStr := period.Start.UTC().Format("20060102T150405Z")
			endStr := period.End.UTC().Format("20060102T150405Z")
			sb.WriteString(fmt.Sprintf("FREEBUSY:%s/%s\r\n", startStr, endStr))
		}
	}

	sb.WriteString("END:VFREEBUSY\r\n")
	sb.WriteString("END:VCALENDAR\r\n")

	return sb.String()
}

func (h *DavServer) freeBusyPeriods(event store.Event, rangeStart, rangeEnd time.Time, hasRange bool) []ical.BusyPeriod {
	if event.DTStart == nil {
		return nil
	}

	endTime := event.DTEnd
	if endTime == nil {
		endTime = event.DTStart
	}
	if !hasRange {
		return []ical.BusyPeriod{{Start: *event.DTStart, End: *endTime}}
	}

	if !ical.EventHasRecurrence(event.RawICAL) {
		if event.DTStart.Before(rangeEnd) && endTime.After(rangeStart) {
			return []ical.BusyPeriod{{Start: *event.DTStart, End: *endTime}}
		}
		return nil
	}

	return h.recurringFreeBusyPeriods(event, rangeStart, rangeEnd)
}

func (h *DavServer) recurringFreeBusyPeriods(event store.Event, rangeStart, rangeEnd time.Time) []ical.BusyPeriod {
	component := ical.PrimaryVEventComponent(event.RawICAL)
	dtstart, ok := recurringEventStart(event, component)
	if !ok {
		return nil
	}
	duration := recurringEventDuration(event, component, dtstart)
	return ical.RecurringBusyPeriods(event.RawICAL, dtstart, duration, rangeStart, rangeEnd, caldavMaxInstances)
}

func recurringEventStart(event store.Event, component *ical.VEventComponent) (time.Time, bool) {
	if prop, ok := ical.ComponentProperty(component, "DTSTART"); ok {
		if dtstart, ok := ical.ParsePropertyDateTimeLocal(prop.KeyPart, prop.Value); ok {
			return dtstart, true
		}
	}
	if event.DTStart != nil {
		return *event.DTStart, true
	}
	return time.Time{}, false
}

func recurringEventDuration(event store.Event, component *ical.VEventComponent, dtstart time.Time) time.Duration {
	if prop, ok := ical.ComponentProperty(component, "DTEND"); ok {
		if dtend, ok := ical.ParsePropertyDateTimeLocal(prop.KeyPart, prop.Value); ok {
			if d := dtend.Sub(dtstart); d > 0 {
				return d
			}
		}
	}
	if prop, ok := ical.ComponentProperty(component, "DURATION"); ok {
		if d, ok := ical.ParseDuration(prop.Value); ok && d > 0 {
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
	if prop, ok := ical.ComponentProperty(component, "DTSTART"); ok && ical.PropertyParamEquals(prop.KeyPart, "VALUE", "DATE") {
		return 24 * time.Hour
	}
	return time.Hour
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

	// Apple clients multiget hundreds of hrefs after a sync; fetch the events
	// and the relevant ACL entries in one batch each instead of one event
	// query plus one full privilege evaluation per href.
	uids := make([]string, 0, len(hrefs))
	seen := make(map[string]struct{}, len(hrefs))
	for _, href := range hrefs {
		cleanHref := resolveDAVHref(resolvePath, href)
		if cleanHref == "" {
			continue
		}
		segment, uid, ok := parseCalendarResourceSegments(cleanHref)
		if !ok || !calendarSegmentMatches(cal, segment) {
			continue
		}
		if _, ok := seen[uid]; ok {
			continue
		}
		seen[uid] = struct{}{}
		uids = append(uids, uid)
	}
	if len(uids) == 0 {
		return nil, nil
	}

	events, err := h.store.Events.ListByResourceNames(ctx, cal.ID, uids)
	if err != nil {
		return nil, fmt.Errorf("failed to fetch event")
	}
	eventsByName := make(map[string]*store.Event, len(events))
	for i := range events {
		eventsByName[eventResourceName(events[i])] = &events[i]
	}
	prefetchedACLEntries, err := h.prefetchCalendarACLEntries(ctx, user, cal.ID, events)
	if err != nil {
		return nil, err
	}

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
		ev := eventsByName[uid]
		if ev == nil {
			responses = append(responses, response{Href: responseHref, Status: httpStatusNotFound})
			continue
		}
		allowed, err := h.canAccessCalendarObjectWithEntries(user, cal, uid, "read", prefetchedACLEntries)
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
