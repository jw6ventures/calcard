package dav

import (
	"context"
	"crypto/sha256"
	"fmt"
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
		res, err := h.freeBusyQuery(ctx, user, cal, responsePath, report.Filter)
		return res, "", err
	case "sync-collection":
		return h.calendarSyncCollection(ctx, user, cal, principalHref, responsePath, report, calData)
	default:
		// Fallback: return all events to keep clients moving even if they send unsupported report types.
		res, err := h.calendarQuery(ctx, user, cal, responsePath, nil, calData)
		return res, "", err
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

	if strings.Contains(strings.ToUpper(event.RawICAL), "RRULE:") {
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

func validCalendarFilterTimeRanges(filter *calFilter) bool {
	if filter == nil {
		return true
	}
	return validCompFilterTimeRanges(&filter.CompFilter)
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

	rrule := extractRRule(event.RawICAL)
	if rrule == "" {
		return true // Malformed, be permissive
	}

	// Parse RRULE parameters
	freq := extractRRuleParam(rrule, "FREQ")
	countStr := extractRRuleParam(rrule, "COUNT")
	untilStr := extractRRuleParam(rrule, "UNTIL")
	intervalStr := extractRRuleParam(rrule, "INTERVAL")

	interval := 1
	if intervalStr != "" {
		if i, err := strconv.Atoi(intervalStr); err == nil && i > 0 {
			interval = i
		}
	}

	maxOccurrences := 500 // Default limit to prevent infinite loops
	if countStr != "" {
		if c, err := strconv.Atoi(countStr); err == nil && c > 0 {
			maxOccurrences = c
		}
	}

	recurrenceEnd := rangeEnd.AddDate(0, 0, 1) // Default to just past query range
	if untilStr != "" {
		if until, err := parseICalDateTime(untilStr); err == nil {
			recurrenceEnd = until
		}
	}

	eventDuration := time.Hour // Default 1 hour
	if event.DTEnd != nil {
		eventDuration = event.DTEnd.Sub(*event.DTStart)
	}

	current := *event.DTStart
	for i := 0; i < maxOccurrences; i++ {
		if current.After(recurrenceEnd) {
			break
		}

		if current.After(rangeEnd.AddDate(0, 0, 7)) {
			break
		}

		instanceEnd := current.Add(eventDuration)
		if current.Before(rangeEnd) && instanceEnd.After(rangeStart) {
			return true
		}

		switch strings.ToUpper(freq) {
		case "DAILY":
			current = current.AddDate(0, 0, interval)
		case "WEEKLY":
			current = current.AddDate(0, 0, 7*interval)
		case "MONTHLY":
			current = current.AddDate(0, interval, 0)
		case "YEARLY":
			current = current.AddDate(interval, 0, 0)
		default:
			// Unknown frequency, be permissive
			return true
		}

		// Safety check
		if current.After(event.DTStart.AddDate(3, 0, 0)) && i > 100 {
			break
		}
	}

	return false
}

func (h *DavServer) freeBusyQuery(ctx context.Context, user *store.User, cal *store.CalendarAccess, cleanPath string, filter *calFilter) ([]response, error) {
	events, err := h.store.Events.ListForCalendar(ctx, cal.ID)
	if err != nil {
		return nil, fmt.Errorf("failed to list events")
	}

	if filter != nil {
		events = h.applyCalendarFilter(events, filter)
	}
	events, err = h.filterCalendarEventsByPrivilege(ctx, user, cal, events, "read-free-busy")
	if err != nil {
		return nil, err
	}

	freeBusyData := h.generateFreeBusy(events, filter)

	href := strings.TrimSuffix(cleanPath, "/") + "/freebusy.ics"
	etag := fmt.Sprintf("%x", sha256.Sum256([]byte(freeBusyData)))

	return []response{
		resourceResponse(href, etagProp(etag, freeBusyData, true)),
	}, nil
}

func (h *DavServer) generateFreeBusy(events []store.Event, filter *calFilter) string {
	var sb strings.Builder
	sb.WriteString("BEGIN:VCALENDAR\r\n")
	sb.WriteString("VERSION:2.0\r\n")
	sb.WriteString("PRODID:-//CalCard//CalDAV Server//EN\r\n")
	sb.WriteString("BEGIN:VFREEBUSY\r\n")
	sb.WriteString(fmt.Sprintf("DTSTAMP:%s\r\n", time.Now().UTC().Format("20060102T150405Z")))

	if filter != nil && filter.CompFilter.TimeRange != nil {
		if filter.CompFilter.TimeRange.Start != "" {
			sb.WriteString(fmt.Sprintf("DTSTART:%s\r\n", filter.CompFilter.TimeRange.Start))
		}
		if filter.CompFilter.TimeRange.End != "" {
			sb.WriteString(fmt.Sprintf("DTEND:%s\r\n", filter.CompFilter.TimeRange.End))
		}
	}

	for _, event := range events {
		if event.DTStart != nil {
			endTime := event.DTEnd
			if endTime == nil {
				endTime = event.DTStart
			}

			startStr := event.DTStart.UTC().Format("20060102T150405Z")
			endStr := endTime.UTC().Format("20060102T150405Z")
			sb.WriteString(fmt.Sprintf("FREEBUSY:%s/%s\r\n", startStr, endStr))
		}
	}

	sb.WriteString("END:VFREEBUSY\r\n")
	sb.WriteString("END:VCALENDAR\r\n")

	return sb.String()
}

func (h *DavServer) calendarQuery(ctx context.Context, user *store.User, cal *store.CalendarAccess, cleanPath string, filter *calFilter, calData *calendarDataEl) ([]response, error) {
	events, err := h.store.Events.ListForCalendar(ctx, cal.ID)
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
		calendarCollectionResponseWithPrivileges(collectionHref, cal.Name, cal.Description, cal.Timezone, cal.Color, principalHref, syncToken, fmt.Sprintf("%d", cal.CTag), cal.EffectivePrivileges()),
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
