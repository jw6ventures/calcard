package dav

import (
	"context"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/jw6ventures/calcard/internal/ical"
	"github.com/jw6ventures/calcard/internal/store"
)

// calendarReportResponses runs one REPORT against a calendar collection.
// targetResource is the resource name when the Request-URI is a calendar object
// resource rather than the collection, which RFC 4791 §7 supports for
// calendar-query and calendar-multiget; it is empty for a collection target.
func (h *DavServer) calendarReportResponses(ctx context.Context, user *store.User, cal *store.CalendarAccess, principalHref, responsePath, targetResource string, report reportRequest, request *http.Request) ([]response, string, error) {
	// RFC 4791 §7: a report whose Request-URI names a calendar object resource
	// runs against that resource, so a URI naming none has nothing to report on.
	// That is a request-level 404, not a 207 saying the resource matched nothing.
	if targetResource != "" {
		event, err := h.store.Events.GetByResourceName(ctx, cal.ID, targetResource)
		if err != nil {
			return nil, "", fmt.Errorf("failed to fetch event")
		}
		if event == nil {
			return nil, "", store.ErrNotFound
		}
	}
	calData := reportCalendarData(report)
	switch report.XMLName.Local {
	case "calendar-multiget":
		res, err := h.calendarMultiGet(ctx, user, cal, report.Hrefs, responsePath, targetResource, calData, report.selector, request)
		return res, "", err
	case "calendar-query":
		res, err := h.calendarQuery(ctx, user, cal, responsePath, targetResource, report.Filter, calData, report.selector)
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

// eventMatchesFilter applies one CALDAV:filter to a calendar object resource.
// RFC 4791 §9.7 scopes each filter element to a component, a property of that
// component, or a parameter of that property, so matching runs over the parsed
// tree rather than over the object's text: a substring search cannot tell a
// SUMMARY value from a DESCRIPTION that quotes one. An object whose stored
// octets do not parse matches nothing rather than failing the whole report.
func (h *DavServer) eventMatchesFilter(event store.Event, filter *calFilter) bool {
	root, err := parseICalendarObject(event.RawICAL)
	if err != nil {
		return false
	}
	// RFC 4791 §9.7.1: the outermost comp-filter is scoped to the calendar
	// object resource itself.
	return h.matchesCompFilter(event, []*icalNode{root}, &filter.CompFilter)
}

// matchesCompFilter applies one CALDAV:comp-filter to the components it is
// scoped to: the calendar object at the filter root, and the children of the
// enclosing component when nested. An empty filter matches the component's
// existence and every child filter is conjunctive (RFC 4791 §9.7.1).
func (h *DavServer) matchesCompFilter(event store.Event, candidates []*icalNode, filter *compFilter) bool {
	want := asciiCasemapFold(filter.Name)
	defined := false
	for _, node := range candidates {
		if want != "" && node.name != want {
			continue
		}
		defined = true
		if filter.IsNotDefined != nil {
			return false
		}
		if h.compFilterBodyMatches(event, node, filter) {
			return true
		}
	}
	if filter.IsNotDefined != nil {
		return !defined
	}
	return false
}

func (h *DavServer) compFilterBodyMatches(event store.Event, node *icalNode, filter *compFilter) bool {
	if filter.TimeRange != nil && !h.eventInTimeRange(event, filter.TimeRange) {
		return false
	}
	for i := range filter.PropFilter {
		if !matchesPropFilter(node, &filter.PropFilter[i]) {
			return false
		}
	}
	for i := range filter.CompFilter {
		if !h.matchesCompFilter(event, node.children, &filter.CompFilter[i]) {
			return false
		}
	}
	return true
}

// matchesPropFilter applies one CALDAV:prop-filter to the named property of the
// enclosing component. An empty filter matches the property's existence, and
// its time-range or text-match result is conjoined with every param-filter
// (RFC 4791 §9.7.2).
func matchesPropFilter(node *icalNode, filter *propFilter) bool {
	want := asciiCasemapFold(filter.Name)
	defined := false
	for i := range node.properties {
		property := &node.properties[i]
		if property.name != want {
			continue
		}
		defined = true
		if filter.IsNotDefined != nil {
			return false
		}
		if propFilterBodyMatches(property, filter) {
			return true
		}
	}
	if filter.IsNotDefined != nil {
		return !defined
	}
	return false
}

func propFilterBodyMatches(property *icalProperty, filter *propFilter) bool {
	if filter.TextMatch != nil && !matchesCalendarText(calendarTextMatchValue(property), filter.TextMatch) {
		return false
	}
	if filter.TimeRange != nil && !propertyInTimeRange(property, filter.TimeRange) {
		return false
	}
	for i := range filter.ParamFilter {
		if !matchesParamFilter(property, &filter.ParamFilter[i]) {
			return false
		}
	}
	return true
}

var calendarTextProperties = nameSet(
	"ACTION", "CALSCALE", "CATEGORIES", "CLASS", "COMMENT", "CONTACT", "DESCRIPTION",
	"LOCATION", "METHOD", "PRODID", "RELATED-TO", "REQUEST-STATUS", "RESOURCES",
	"STATUS", "SUMMARY", "TRANSP", "TZID", "TZNAME", "UID", "VERSION",
)

func calendarTextMatchValue(property *icalProperty) string {
	if valueType, explicit := property.parameters["VALUE"]; explicit {
		if strings.EqualFold(valueType, "TEXT") {
			return unescapeCalendarText(property.value)
		}
		return property.value
	}
	if calendarTextProperties.contains(property.name) {
		return unescapeCalendarText(property.value)
	}
	if _, standard := knownICalendarProperties[property.name]; !standard {
		return unescapeCalendarText(property.value)
	}
	return property.value
}

// matchesParamFilter applies one CALDAV:param-filter to the named parameter of
// the enclosing property. An empty filter matches the parameter's existence and
// an optional text-match matches its value (RFC 4791 §9.7.3).
func matchesParamFilter(property *icalProperty, filter *paramFilter) bool {
	value, defined := property.parameters[asciiCasemapFold(filter.Name)]
	if filter.IsNotDefined != nil {
		return !defined
	}
	if !defined {
		return false
	}
	if filter.TextMatch != nil {
		return matchesCalendarText(value, filter.TextMatch)
	}
	return true
}

// matchesCalendarText applies one CALDAV:text-match to the single value it is
// scoped to: a substring test under the named collation, inverted when
// negate-condition is "yes" (RFC 4791 §9.7.5).
func matchesCalendarText(value string, match *textMatch) bool {
	fold, ok := calendarCollationFolder(match.Collation)
	if !ok {
		return false
	}
	matched := strings.Contains(fold(value), fold(match.Text))
	if match.NegateCondition == "yes" {
		return !matched
	}
	return matched
}

// propertyInTimeRange applies the RFC 4791 §9.9 overlap test every date-valued
// property shares: start <= value AND end > value. This helper uses the shared
// parser's current timezone resolution; request and collection timezone
// selection belong to the higher-level time-range evaluator.
func propertyInTimeRange(property *icalProperty, tr *timeRange) bool {
	start, end, ok := calendarTimeRangeBounds(tr)
	if !ok {
		return false
	}
	value, ok := ical.ParsePropertyDateTimeLocal(property.keyPart, property.value)
	if !ok {
		return false
	}
	return !value.Before(start) && value.Before(end)
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

		return eventOverlapsTimeRange(*event.DTStart, *eventEnd, start, end)
	}

	return true
}

// eventOverlapsTimeRange applies the RFC 4791 §9.9 overlap tests. A
// zero-duration event matches (start <= DTSTART && end > DTSTART); anything
// with a duration uses the ordinary half-open overlap.
func eventOverlapsTimeRange(eventStart, eventEnd, rangeStart, rangeEnd time.Time) bool {
	if eventEnd.Equal(eventStart) {
		return !eventStart.Before(rangeStart) && eventStart.Before(rangeEnd)
	}
	return eventStart.Before(rangeEnd) && eventEnd.After(rangeStart)
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

func validCalendarFilterTimeRanges(filter *calFilter) bool {
	if filter == nil {
		return true
	}
	return validCompFilterTimeRanges(&filter.CompFilter)
}

// validCalendarFilterCollations reports whether every CALDAV:text-match in the
// filter names a collation the matcher implements. RFC 4791 §7.8.7
// (CALDAV:supported-collation) forbids answering a request that asks for one
// the server does not: silently matching under a different collation returns
// results the client did not ask for.
func validCalendarFilterCollations(filter *calFilter) bool {
	if filter == nil {
		return true
	}
	return validCompFilterCollations(&filter.CompFilter)
}

func validCompFilterCollations(filter *compFilter) bool {
	for i := range filter.PropFilter {
		if !validPropFilterCollations(&filter.PropFilter[i]) {
			return false
		}
	}
	for i := range filter.CompFilter {
		if !validCompFilterCollations(&filter.CompFilter[i]) {
			return false
		}
	}
	return true
}

func validPropFilterCollations(filter *propFilter) bool {
	if filter.TextMatch != nil && !calendarCollationSupported(filter.TextMatch.Collation) {
		return false
	}
	for i := range filter.ParamFilter {
		param := &filter.ParamFilter[i]
		if param.TextMatch != nil && !calendarCollationSupported(param.TextMatch.Collation) {
			return false
		}
	}
	return true
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
	for i := range filter.PropFilter {
		if tr := filter.PropFilter[i].TimeRange; tr != nil {
			if _, _, ok := calendarTimeRangeBounds(tr); !ok {
				return false
			}
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

// freeBusyHasEffectiveTimeRange reports whether a free-busy-query carries a
// usable time-range from either source. RFC 4791 §7.10 requires exactly one
// CALDAV:time-range; without it the report degenerates into a full-collection
// read and an unbounded text/calendar response.
func freeBusyHasEffectiveTimeRange(filter *calFilter, tr *timeRange) bool {
	_, _, ok := calendarTimeRangeBounds(freeBusyTimeRange(filter, tr))
	return ok
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
		if eventOverlapsTimeRange(*event.DTStart, *endTime, rangeStart, rangeEnd) {
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

func (h *DavServer) calendarQuery(ctx context.Context, user *store.User, cal *store.CalendarAccess, cleanPath, targetResource string, filter *calFilter, calData *calendarDataEl, selector propertySelector) ([]response, error) {
	if targetResource != "" {
		return h.calendarObjectQuery(ctx, user, cal, cleanPath, targetResource, filter, calData, selector)
	}
	databaseFilter, _ := eventFilterFromCalFilter(filter)
	buildLimit := h.multistatusBuildLimit()
	responses := make([]response, 0, buildLimit)
	afterID := int64(0)
	for len(responses) < buildLimit {
		events, err := h.store.Events.ListForCalendarPageAfter(ctx, cal.ID, afterID, multistatusPageSize, databaseFilter)
		if err != nil {
			return nil, fmt.Errorf("failed to list events")
		}
		if len(events) == 0 {
			break
		}

		matching := events
		if filter != nil {
			matching = h.applyCalendarFilter(matching, filter)
		}
		matching, err = h.filterReadableCalendarEvents(ctx, user, cal, matching)
		if err != nil {
			return nil, err
		}
		responses = append(responses, rawCalendarResourceReportResponsesLimit(cleanPath, matching, calData, buildLimit-len(responses))...)

		lastID := events[len(events)-1].ID
		if lastID <= afterID || len(events) < multistatusPageSize {
			break
		}
		afterID = lastID
	}

	return h.finishCalendarReportResponses(ctx, user, responses, selector, calData != nil)
}

func (h *DavServer) calendarMultiGet(ctx context.Context, user *store.User, cal *store.CalendarAccess, hrefs []string, responsePath, targetResource string, calData *calendarDataEl, selector propertySelector, request *http.Request) ([]response, error) {
	// RFC 4791 §9.10 requires at least one DAV:href, so the grammar pass has
	// already refused a body carrying none: there is no hrefless multiget to
	// answer with a dump of the collection.
	responseBase := strings.TrimSuffix(responsePath, "/") + "/"

	// Every href owes a DAV:response, so the build limit is reached after
	// exactly that many hrefs and nothing past it is ever looked at.
	buildLimit := h.multistatusBuildLimit()
	if len(hrefs) > buildLimit {
		hrefs = hrefs[:buildLimit]
	}
	// Apple clients multiget hundreds of hrefs after a sync, so each href is
	// resolved once and the events and their ACL entries are read in one batch
	// each, instead of one event query plus one full privilege evaluation per
	// href.
	resolved := make([]resolvedObjectHref, len(hrefs))
	inScope := make([]bool, len(hrefs))
	uids := make([]string, 0, len(hrefs))
	seen := make(map[string]struct{}, len(hrefs))
	for i, href := range hrefs {
		target, ok := h.resolveCalendarHrefForRequest(href, request)
		resolved[i] = target
		inScope[i] = ok && calendarSegmentMatches(cal, target.Segment) &&
			multigetHrefInScope(targetResource, target.ResourceName)
		if !inScope[i] {
			continue
		}
		if _, duplicate := seen[target.ResourceName]; duplicate {
			continue
		}
		seen[target.ResourceName] = struct{}{}
		uids = append(uids, target.ResourceName)
	}
	// A multiget whose hrefs all fail to resolve still owes the client a
	// response per href, so only the repository reads are skipped here.
	var events []store.Event
	var prefetchedACLEntries map[string][]store.ACLEntry
	if len(uids) > 0 {
		var err error
		events, err = h.store.Events.ListByResourceNames(ctx, cal.ID, uids)
		if err != nil {
			return nil, fmt.Errorf("failed to fetch event")
		}
		prefetchedACLEntries, err = h.prefetchCalendarACLEntries(ctx, user, cal.ID, events)
		if err != nil {
			return nil, err
		}
	}
	eventsByName := make(map[string]*store.Event, len(events))
	for i := range events {
		eventsByName[eventResourceName(events[i])] = &events[i]
	}
	decider := newBatchedObjectACLDecider(user, cal.UserID, calendarCollectionResourcePath(cal.ID), prefetchedACLEntries)

	responses := make([]response, 0, len(hrefs))
	for i, href := range hrefs {
		uid := resolved[i].ResourceName
		// RFC 4791 §7.9: every requested href needs a DAV:response, so an
		// unresolvable or out-of-scope one reports 404 under the best href the
		// request gives us instead of being dropped.
		if !inScope[i] {
			responses = append(responses, response{Href: multiGetFallbackHref(href, resolved[i].Path, responsePath), Status: httpStatusNotFound})
			continue
		}
		responseHref := calendarObjectHref(responseBase, uid)
		ev := eventsByName[uid]
		if ev == nil {
			responses = append(responses, response{Href: responseHref, Status: httpStatusNotFound})
			continue
		}
		allowed, denied := calendarPrivilegeDecisionWithDecider(cal, uid, "read", decider)
		if !allowed && !denied {
			allowed = cal.EffectivePrivileges().Allows("read")
		}
		if !allowed {
			responses = append(responses, response{Href: responseHref, Status: httpStatusNotFound})
			continue
		}
		responses = append(responses, rawCalendarResourceReportResponse(responseHref, *ev, calData))
	}
	return h.finishCalendarReportResponses(ctx, user, responses, selector, calData != nil)
}

// eventsWithResourceName narrows a generated event set to the one resource an
// object-resource Request-URI names.
func eventsWithResourceName(events []store.Event, resourceName string) []store.Event {
	for i := range events {
		if eventResourceName(events[i]) == resourceName {
			return []store.Event{events[i]}
		}
	}
	return nil
}

// multigetHrefInScope reports whether a resolved multiget href names the
// resource the Request-URI names. RFC 4791 §7.9 scopes a multiget run against a
// calendar object resource to that resource, so an href naming a sibling is out
// of scope; a collection target (empty targetResource) admits every member.
func multigetHrefInScope(targetResource, uid string) bool {
	return targetResource == "" || targetResource == uid
}

// calendarObjectQuery answers a calendar-query whose Request-URI is a single
// calendar object resource (RFC 4791 §7). The filter still decides whether the
// resource is reported, so a non-matching resource yields an empty multistatus.
func (h *DavServer) calendarObjectQuery(ctx context.Context, user *store.User, cal *store.CalendarAccess, collectionPath, resourceName string, filter *calFilter, calData *calendarDataEl, selector propertySelector) ([]response, error) {
	event, err := h.store.Events.GetByResourceName(ctx, cal.ID, resourceName)
	if err != nil {
		return nil, fmt.Errorf("failed to fetch event")
	}
	var matching []store.Event
	if event != nil {
		matching = []store.Event{*event}
		if filter != nil {
			matching = h.applyCalendarFilter(matching, filter)
		}
		matching, err = h.filterReadableCalendarEvents(ctx, user, cal, matching)
		if err != nil {
			return nil, err
		}
	}
	responses := rawCalendarResourceReportResponsesLimit(collectionPath, matching, calData, h.multistatusBuildLimit())
	return h.finishCalendarReportResponses(ctx, user, responses, selector, calData != nil)
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
	syncToken, _ := h.calendarSyncTokenValue(cal)
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
		calendarCollectionResponseWithPrivileges(collectionHref, cal.Name, cal.Calendar, principalHref, syncToken, strconv.FormatInt(cal.CTag, 10), cal.EffectivePrivileges()),
	}
	resourceResponses := rawCalendarResourceReportResponsesLimit(collectionHref, events, calData, h.multistatusBuildLimit()-len(responses))
	responses = h.appendMultistatusResponses(responses, resourceResponses)

	// Include deleted resources if this is an incremental sync
	if !since.IsZero() && !h.multistatusBuildComplete(responses) {
		deletedHrefs := make(map[string]struct{})
		visible := make(map[string]struct{}, len(events))
		for _, event := range events {
			visible[eventResourceName(event)] = struct{}{}
		}
		for _, event := range allEvents {
			if h.multistatusBuildComplete(responses) {
				break
			}
			if !event.LastModified.After(since) {
				continue
			}
			resourceName := eventResourceName(event)
			if _, ok := visible[resourceName]; ok {
				continue
			}
			href := calendarObjectHref(collectionHref, resourceName)
			responses = h.appendMultistatusResponses(responses, []response{deletedResponse(href)})
			deletedHrefs[href] = struct{}{}
		}
		if h.multistatusBuildComplete(responses) {
			responses, err = h.finishCalendarReportResponses(ctx, user, responses, propertySelector{Prop: report.Prop}, calData != nil)
			return responses, syncToken, err
		}
		deleted, err := h.store.DeletedResources.ListDeletedSince(ctx, "event", cal.ID, since)
		if err != nil {
			return nil, "", fmt.Errorf("failed to list deleted events")
		}
		for _, d := range deleted {
			if h.multistatusBuildComplete(responses) {
				break
			}
			resourceName := d.ResourceName
			if resourceName == "" {
				resourceName = d.UID
			}
			href := calendarObjectHref(collectionHref, resourceName)
			if _, ok := deletedHrefs[href]; ok {
				continue
			}
			responses = h.appendMultistatusResponses(responses, []response{deletedResponse(href)})
			deletedHrefs[href] = struct{}{}
		}
	}

	responses, err = h.finishCalendarReportResponses(ctx, user, responses, propertySelector{Prop: report.Prop}, calData != nil)
	if err != nil {
		return nil, "", err
	}
	return responses, syncToken, nil
}
