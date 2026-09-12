package dav

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"

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
	// The §7.3 zone every floating value in this report resolves against, the
	// filter's and the projection's alike -- carried on the projection so the
	// two cannot be given different readings of the same request. Only
	// calendar-query carries a CALDAV:timezone of its own; the other reports
	// fall back to the collection property, as §7.3 orders.
	projection := newCalendarDataProjection(reportCalendarData(report), reportFloatingZone(report.Timezone, cal.Timezone))
	req := calendarReportRequest{
		user:           user,
		cal:            cal,
		principalHref:  principalHref,
		collectionPath: responsePath,
		targetResource: targetResource,
		projection:     projection,
		selector:       report.selector,
		request:        request,
	}
	switch report.XMLName.Local {
	case "calendar-multiget":
		res, err := h.calendarMultiGet(ctx, req, report.Hrefs)
		return res, "", err
	case "calendar-query":
		res, err := h.calendarQuery(ctx, req, report.Filter)
		return res, "", err
	case "sync-collection":
		return h.calendarSyncCollection(ctx, req, report)
	default:
		// RFC 3253 §3.6: unknown report types must be refused, not answered
		// with a full dump of the collection.
		return nil, "", errUnsupportedReport
	}
}

// calendarReportRequest is what every calendar REPORT entry point needs to know
// about one request: who is asking, the collection it runs against, where the
// responses it builds are rooted, and how a matching resource is projected and
// selected from. It is built once per report so a value that has to reach all
// four entry points -- as the §7.3 zone on the projection already does --
// arrives through one field rather than a fourth positional argument.
type calendarReportRequest struct {
	user *store.User
	cal  *store.CalendarAccess
	// principalHref names the owner sync-collection reports on the collection.
	principalHref string
	// collectionPath roots every response href. It names the collection even
	// when the Request-URI is one of its object resources.
	collectionPath string
	// targetResource is the resource name when the Request-URI names a calendar
	// object resource rather than the collection, which RFC 4791 §7 allows for
	// calendar-query and calendar-multiget. It is empty for a collection target.
	targetResource string
	projection     calendarDataProjection
	selector       propertySelector
	// request is the HTTP request the report arrived on, which multiget needs to
	// resolve a DAV:href naming an absolute URI against.
	request *http.Request
}

// applyCalendarFilter keeps the events a CALDAV:filter matches, resolving
// floating values through zone, which RFC 4791 §7.3 orders ahead of UTC.
func applyCalendarFilter(events []store.Event, filter *calFilter, zone floatingZone) []store.Event {
	if filter == nil {
		return events
	}

	var filtered []store.Event
	for _, event := range events {
		if eventMatchesFilter(event, filter, zone) {
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
func eventMatchesFilter(event store.Event, filter *calFilter, zone floatingZone) bool {
	matcher, ok := newEventTimeRangeMatcher(event, zone)
	if !ok {
		return false
	}
	return matcherMatchesFilter(matcher, filter)
}

// newEventTimeRangeMatcher parses one stored object so the filter walk, the
// §9.9 time-range test and the free-busy period derivation can share a single
// parse. ok is false for octets that do not parse; the matcher is still
// returned, since a caller may fall back to the denormalized store columns.
func newEventTimeRangeMatcher(event store.Event, zone floatingZone) (calendarTimeRangeMatcher, bool) {
	root, err := parseICalendarObject(event.RawICAL)
	if err != nil {
		return newCalendarTimeRangeMatcher(event.RawICAL, nil, zone), false
	}
	return newCalendarTimeRangeMatcher(event.RawICAL, root, zone), true
}

// matcherMatchesFilter is eventMatchesFilter over an object already parsed.
func matcherMatchesFilter(matcher calendarTimeRangeMatcher, filter *calFilter) bool {
	// RFC 4791 §9.7.1: the outermost comp-filter is scoped to the calendar
	// object resource itself.
	return matchesCompFilter(matcher, nil, []*icalNode{matcher.root}, &filter.CompFilter)
}

// matchesCompFilter applies one CALDAV:comp-filter to the components it is
// scoped to: the calendar object at the filter root, and the children of the
// enclosing component when nested. An empty filter matches the component's
// existence and every child filter is conjunctive (RFC 4791 §9.7.1). parent is
// the component holding candidates, which a VALARM time-range needs to resolve
// a relative TRIGGER against.
func matchesCompFilter(matcher calendarTimeRangeMatcher, parent *icalNode, candidates []*icalNode, filter *compFilter) bool {
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
		if compFilterBodyMatches(matcher, parent, node, filter) {
			return true
		}
	}
	if filter.IsNotDefined != nil {
		return !defined
	}
	return false
}

func compFilterBodyMatches(matcher calendarTimeRangeMatcher, parent, node *icalNode, filter *compFilter) bool {
	if filter.TimeRange != nil {
		start, end, ok := calendarTimeRangeBounds(filter.TimeRange)
		if !ok {
			return false
		}
		if !matcher.componentSetInTimeRange(node, parent, start, end) {
			return false
		}
	}
	for i := range filter.PropFilter {
		if !matchesPropFilter(matcher, parent, node, &filter.PropFilter[i]) {
			return false
		}
	}
	for i := range filter.CompFilter {
		if !matchesCompFilter(matcher, node, node.children, &filter.CompFilter[i]) {
			return false
		}
	}
	return true
}

// matchesPropFilter applies one CALDAV:prop-filter to the named property of the
// enclosing component. An empty filter matches the property's existence, and
// its time-range or text-match result is conjoined with every param-filter
// (RFC 4791 §9.7.2). parent is the component holding node, which a time-range
// needs to find the recurrence set an effective property value moves with.
func matchesPropFilter(matcher calendarTimeRangeMatcher, parent, node *icalNode, filter *propFilter) bool {
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
		if propFilterBodyMatches(matcher, parent, node, property, filter) {
			return true
		}
	}
	if filter.IsNotDefined != nil {
		return !defined
	}
	if !defined && filter.TimeRange != nil && len(filter.ParamFilter) == 0 {
		// RFC 4791 §9.9 closes by directing the test at the effective DTEND of a
		// VEVENT carrying DURATION instead of one, and at the effective DUE of a
		// VTODO in the same shape. Only a time-range reads such a value: an
		// inferred property has no parameters for a param-filter to match, and
		// §9.7.4's is-not-defined asks whether the component spells the property,
		// which it still does not.
		start, end, ok := calendarTimeRangeBounds(filter.TimeRange)
		return ok && matcher.inferredPropertyInTimeRange(filter.Name, node, parent, start, end)
	}
	return false
}

func propFilterBodyMatches(matcher calendarTimeRangeMatcher, parent, node *icalNode, property *icalProperty, filter *propFilter) bool {
	if filter.TextMatch != nil && !matchesCalendarText(calendarTextMatchValue(property), filter.TextMatch) {
		return false
	}
	if filter.TimeRange != nil {
		start, end, ok := calendarTimeRangeBounds(filter.TimeRange)
		if !ok {
			return false
		}
		if !matcher.propertyInTimeRange(*property, node, parent, start, end) {
			return false
		}
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

// calendarQueryVEventTimeRange picks the time-range a calendar-query may narrow
// its database read with: the one spelled directly on the VEVENT comp-filter,
// since the store derives its dtstart/dtend metadata from VEVENT rows alone.
//
// A range nested deeper is not usable. It bounds the sub-component it is scoped
// to -- a VALARM whose TRIGGER fires days from the event it belongs to, say --
// and narrowing on the enclosing VEVENT's own columns would drop a resource that
// RFC 4791 §9.9 matches. Absent a range on the VEVENT itself the report reads
// the collection unnarrowed and the in-memory pass decides.
func calendarQueryVEventTimeRange(filter *calFilter) *timeRange {
	if filter == nil || !strings.EqualFold(filter.CompFilter.Name, "VCALENDAR") {
		return nil
	}
	for i := range filter.CompFilter.CompFilter {
		child := &filter.CompFilter.CompFilter[i]
		if strings.EqualFold(child.Name, "VEVENT") {
			return child.TimeRange
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

// maxStoredInstantSkew bounds how far the instant the RFC 4791 §9.9 test judges
// can sit from the denormalized dtstart/dtend the narrowing predicates read.
//
// The two resolve the same property against different zones. A column is
// ical.ParsePropertyDateTimeLocal: a TZID resolves against the host's zone
// database, and a TZID the host does not know reads as UTC, as a floating value
// does. The §9.9 evaluator resolves a TZID against the VTIMEZONE the resource
// ships, which RFC 4791 §4.1 makes authoritative for the TZIDs it uses, and a
// floating value against the §7.3 zone. A shipped observance offset reaches
// ±23:59:59 and a host zone reaches ±16:00 once the pre-1900 local-mean-time
// entries CALDAV:min-date-time admits are in play, so 48 hours covers every
// pairing of the two.
const maxStoredInstantSkew = 48 * time.Hour

// eventFilterFromTimeRange turns a request time-range into the narrowing the
// database read applies. The rows it keeps are a superset; the exact RFC 4791
// §9.9 test runs in memory afterwards.
//
// Both bounds carry maxStoredInstantSkew. Narrowing to the range as spelled
// drops rows whose column and resolved instant disagree, and no later pass can
// recover a row the query did not return.
func eventFilterFromTimeRange(tr *timeRange) (store.EventFilter, bool) {
	if tr == nil {
		return store.EventFilter{}, false
	}
	start, end, ok := calendarTimeRangeBounds(tr)
	if !ok {
		return store.EventFilter{}, false
	}
	ef := store.EventFilter{}
	if strings.TrimSpace(tr.Start) != "" {
		s := start.Add(-maxStoredInstantSkew)
		ef.Start = &s
	}
	if strings.TrimSpace(tr.End) != "" {
		e := end.Add(maxStoredInstantSkew)
		ef.End = &e
	}
	if ef.Start == nil && ef.End == nil {
		return store.EventFilter{}, false
	}
	return ef, true
}

// listBoundedCalendarEvents reads a calendar in keyset pages rather than whole,
// so a collection larger than the report will examine costs one page instead of
// its full size in memory.
func (h *DavServer) listBoundedCalendarEvents(ctx context.Context, calendarID int64, filter store.EventFilter) ([]store.Event, error) {
	return collectBoundedPages(ctx, h.reportCandidateRowLimit(),
		func(ctx context.Context, afterID int64) ([]store.Event, error) {
			return h.store.Events.ListForCalendarPageAfter(ctx, calendarID, afterID, multistatusPageSize, filter)
		}, eventID)
}

// listBoundedModifiedCalendarEvents is the same read narrowed to the rows an
// incremental sync reports on. The narrowing is the client's sync token rather
// than the server's, so it bounds nothing on its own: a token from before the
// collection existed selects every row in it.
func (h *DavServer) listBoundedModifiedCalendarEvents(ctx context.Context, calendarID int64, since time.Time) ([]store.Event, error) {
	return collectBoundedPages(ctx, h.reportCandidateRowLimit(),
		func(ctx context.Context, afterID int64) ([]store.Event, error) {
			return h.store.Events.ListModifiedSincePageAfter(ctx, calendarID, afterID, since, multistatusPageSize)
		}, eventID)
}

func (h *DavServer) calendarQuery(ctx context.Context, req calendarReportRequest, filter *calFilter) ([]response, error) {
	if req.targetResource != "" {
		return h.calendarObjectQuery(ctx, req, filter)
	}
	databaseFilter, _ := eventFilterFromCalFilter(filter)
	buildLimit := h.multistatusBuildLimit()
	rowLimit := h.reportCandidateRowLimit()
	// The build limit is a ceiling, not a size: an operator may set the response
	// limit to unlimited, and a collection is read one page at a time whatever
	// the limit says. Preallocating a page keeps the growth cheap without
	// letting a configured value decide an allocation.
	responses := make([]response, 0, min(buildLimit, multistatusPageSize))
	afterID := int64(0)
	scanned := 0
	for len(responses) < buildLimit {
		events, err := h.store.Events.ListForCalendarPageAfter(ctx, req.cal.ID, afterID, multistatusPageSize, databaseFilter)
		if err != nil {
			return nil, fmt.Errorf("failed to list events")
		}
		if len(events) == 0 {
			break
		}
		scanned += len(events)
		if scanned > rowLimit {
			return nil, errNumberOfMatchesExceeded
		}

		matching := events
		if filter != nil {
			matching = applyCalendarFilter(matching, filter, req.projection.zone)
		}
		matching, err = h.filterReadableCalendarEvents(ctx, req.user, req.cal, matching)
		if err != nil {
			return nil, err
		}
		responses = append(responses, rawCalendarResourceReportResponsesLimit(req.collectionPath, matching, req.projection, buildLimit-len(responses))...)

		lastID := events[len(events)-1].ID
		if lastID <= afterID || len(events) < multistatusPageSize {
			break
		}
		afterID = lastID
	}
	// The build runs one response past the limit precisely so the overflow is a
	// fact rather than an inference: a set that stopped exactly at the limit is
	// a complete answer, and one response more is not.
	if len(responses) > h.maxReportResponses() {
		return nil, errNumberOfMatchesExceeded
	}

	return h.finishCalendarReportResponses(ctx, req.user, responses, req.selector, req.projection.requested())
}

func (h *DavServer) calendarMultiGet(ctx context.Context, req calendarReportRequest, hrefs []string) ([]response, error) {
	// RFC 4791 §9.10 requires at least one DAV:href, so the grammar pass has
	// already refused a body carrying none: there is no hrefless multiget to
	// answer with a dump of the collection.
	responseBase := strings.TrimSuffix(req.collectionPath, "/") + "/"

	// §7.9 owes one DAV:response per href, so an href list past what the server
	// will answer is refused rather than trimmed: a trimmed list would present
	// the leading hrefs as the whole answer.
	if len(hrefs) > h.multigetHrefLimit() {
		return nil, errTooManyHrefs
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
		target, ok := h.resolveCalendarHrefForRequest(href, req.request)
		resolved[i] = target
		inScope[i] = ok && calendarSegmentMatches(req.cal, target.Segment) &&
			multigetHrefInScope(req.targetResource, target.ResourceName)
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
		events, err = h.store.Events.ListByResourceNames(ctx, req.cal.ID, uids)
		if err != nil {
			return nil, fmt.Errorf("failed to fetch event")
		}
		prefetchedACLEntries, err = h.prefetchCalendarACLEntries(ctx, req.user, req.cal.ID, events)
		if err != nil {
			return nil, err
		}
	}
	eventsByName := make(map[string]*store.Event, len(events))
	for i := range events {
		eventsByName[eventResourceName(events[i])] = &events[i]
	}
	decider := newBatchedObjectACLDecider(req.user, req.cal.UserID, calendarCollectionResourcePath(req.cal.ID), prefetchedACLEntries)

	responses := make([]response, 0, len(hrefs))
	for i, href := range hrefs {
		uid := resolved[i].ResourceName
		// RFC 4791 §7.9: every requested href needs a DAV:response, so an
		// unresolvable or out-of-scope one reports 404 under the best href the
		// request gives us instead of being dropped.
		if !inScope[i] {
			responses = append(responses, response{Href: multiGetFallbackHref(href, resolved[i].Path, req.collectionPath), Status: httpStatusNotFound})
			continue
		}
		responseHref := calendarObjectHref(responseBase, uid)
		ev := eventsByName[uid]
		if ev == nil {
			responses = append(responses, response{Href: responseHref, Status: httpStatusNotFound})
			continue
		}
		allowed, denied := calendarPrivilegeDecisionWithDecider(req.cal, uid, "read", decider)
		if !allowed && !denied {
			allowed = req.cal.EffectivePrivileges().Allows("read")
		}
		if !allowed {
			responses = append(responses, response{Href: responseHref, Status: httpStatusNotFound})
			continue
		}
		responses = append(responses, rawCalendarResourceReportResponse(responseHref, *ev, req.projection))
	}
	return h.finishCalendarReportResponses(ctx, req.user, responses, req.selector, req.projection.requested())
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
func (h *DavServer) calendarObjectQuery(ctx context.Context, req calendarReportRequest, filter *calFilter) ([]response, error) {
	event, err := h.store.Events.GetByResourceName(ctx, req.cal.ID, req.targetResource)
	if err != nil {
		return nil, fmt.Errorf("failed to fetch event")
	}
	var matching []store.Event
	if event != nil {
		matching = []store.Event{*event}
		if filter != nil {
			matching = applyCalendarFilter(matching, filter, req.projection.zone)
		}
		matching, err = h.filterReadableCalendarEvents(ctx, req.user, req.cal, matching)
		if err != nil {
			return nil, err
		}
	}
	responses := rawCalendarResourceReportResponsesLimit(req.collectionPath, matching, req.projection, h.multistatusBuildLimit())
	return h.finishCalendarReportResponses(ctx, req.user, responses, req.selector, req.projection.requested())
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

func (h *DavServer) calendarSyncCollection(ctx context.Context, req calendarReportRequest, report reportRequest) ([]response, string, error) {
	syncToken, _ := h.calendarSyncTokenValue(req.cal)
	collectionHref := strings.TrimSuffix(req.collectionPath, "/") + "/"

	var since time.Time
	if report.SyncToken != "" {
		info, err := parseSyncToken(report.SyncToken)
		if err != nil || info.Kind != "cal" || info.ID != req.cal.ID {
			return nil, "", errInvalidSyncToken
		}
		if !h.syncTokenAnswerable(info.Timestamp, req.cal.UpdatedAt) {
			return nil, "", errInvalidSyncToken
		}
		since = info.Timestamp
	}

	var events []store.Event
	var err error
	if since.IsZero() {
		events, err = h.listBoundedCalendarEvents(ctx, req.cal.ID, store.EventFilter{})
	} else {
		events, err = h.listBoundedModifiedCalendarEvents(ctx, req.cal.ID, since)
	}
	if err != nil {
		if errors.Is(err, errTooManyCandidateRows) {
			return nil, "", err
		}
		return nil, "", errors.New("failed to list events")
	}
	allEvents := events
	events, err = h.filterReadableCalendarEvents(ctx, req.user, req.cal, events)
	if err != nil {
		return nil, "", err
	}

	responses := []response{
		calendarCollectionResponseWithPrivileges(collectionHref, req.cal.Name, req.cal.Calendar, req.principalHref, syncToken, strconv.FormatInt(req.cal.CTag, 10), req.cal.EffectivePrivileges()),
	}
	resourceResponses := rawCalendarResourceReportResponsesLimit(collectionHref, events, req.projection, h.multistatusBuildLimit()-len(responses))
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
			responses, err = h.finishCalendarReportResponses(ctx, req.user, responses, propertySelector{Prop: report.Prop}, req.projection.requested())
			return responses, syncToken, err
		}
		deleted, err := h.listBoundedDeletedResources(ctx, "event", req.cal.ID, since)
		if err != nil {
			if errors.Is(err, errTooManyCandidateRows) {
				return nil, "", err
			}
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

	responses, err = h.finishCalendarReportResponses(ctx, req.user, responses, propertySelector{Prop: report.Prop}, req.projection.requested())
	if err != nil {
		return nil, "", err
	}
	if !h.syncTokenAnswerable(since, req.cal.UpdatedAt) {
		return nil, "", errInvalidSyncToken
	}
	return responses, syncToken, nil
}
