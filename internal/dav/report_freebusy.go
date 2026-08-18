package dav

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"strings"
	"time"

	"github.com/jw6ventures/calcard/internal/ical"
	"github.com/jw6ventures/calcard/internal/store"
)

// freeBusyQuery returns the free-busy iCalendar text for the calendar's
// events visible to user within the requested filter/time range.
func (h *DavServer) freeBusyQuery(ctx context.Context, user *store.User, cal *store.CalendarAccess, filter *calFilter, tr *timeRange) (string, error) {
	// §7.3 gives free-busy only the collection's timezone: the report carries
	// no CALDAV:timezone element of its own.
	zone := reportFloatingZone("", cal.Timezone)
	events, err := h.listCalendarEventsForTimeRange(ctx, cal.ID, freeBusyPushdownTimeRange(filter, tr))
	if err != nil {
		return "", fmt.Errorf("failed to list events")
	}

	candidates := freeBusyCandidates(events, zone)
	if filter != nil {
		candidates = filterFreeBusyCandidates(candidates, filter)
	}
	if tr != nil {
		candidates = filterFreeBusyCandidatesByTimeRange(candidates, tr)
	}
	candidates, err = h.filterFreeBusyCandidatesByPrivilege(ctx, user, cal, candidates)
	if err != nil {
		return "", err
	}

	return h.generateFreeBusy(candidates, filter, tr), nil
}

// freeBusyCandidate is one calendar object under consideration, parsed once.
// The report reads each object up to three times -- the CALDAV:filter, the
// §9.9 time-range test and the period derivation each need its components -- so
// the parse and the matcher holding it are made once and carried through.
type freeBusyCandidate struct {
	event   store.Event
	matcher calendarTimeRangeMatcher
	// parsed is false for octets that do not parse. Such an object matches no
	// filter and intersects no range; the matcher is still built so the period
	// derivation can fall back to the denormalized columns.
	parsed bool
}

func freeBusyCandidates(events []store.Event, zone floatingZone) []freeBusyCandidate {
	candidates := make([]freeBusyCandidate, 0, len(events))
	for _, event := range events {
		matcher, parsed := newEventTimeRangeMatcher(event, zone)
		candidates = append(candidates, freeBusyCandidate{event: event, matcher: matcher, parsed: parsed})
	}
	return candidates
}

func filterFreeBusyCandidates(candidates []freeBusyCandidate, filter *calFilter) []freeBusyCandidate {
	if filter == nil {
		return candidates
	}
	kept := make([]freeBusyCandidate, 0, len(candidates))
	for _, candidate := range candidates {
		if candidate.parsed && matcherMatchesFilter(candidate.matcher, filter) {
			kept = append(kept, candidate)
		}
	}
	return kept
}

func filterFreeBusyCandidatesByTimeRange(candidates []freeBusyCandidate, tr *timeRange) []freeBusyCandidate {
	if tr == nil {
		return candidates
	}
	kept := make([]freeBusyCandidate, 0, len(candidates))
	for _, candidate := range candidates {
		if matcherInTimeRange(candidate.matcher, tr) {
			kept = append(kept, candidate)
		}
	}
	return kept
}

// filterFreeBusyCandidatesByPrivilege applies the CALDAV:read-free-busy check
// the stored-event path already implements, keeping the parse each surviving
// candidate carries. It runs after the content filters so the ACL prefetch
// covers only the objects the report would otherwise publish.
func (h *DavServer) filterFreeBusyCandidatesByPrivilege(ctx context.Context, user *store.User, cal *store.CalendarAccess, candidates []freeBusyCandidate) ([]freeBusyCandidate, error) {
	events := make([]store.Event, 0, len(candidates))
	for _, candidate := range candidates {
		events = append(events, candidate.event)
	}
	visible, err := h.filterCalendarEventsByPrivilege(ctx, user, cal, events, "read-free-busy")
	if err != nil {
		return nil, err
	}
	// A resource name is unique within a collection, so it identifies which
	// candidates survived without re-deriving the privilege decision.
	allowed := make(map[string]struct{}, len(visible))
	for _, event := range visible {
		allowed[eventResourceName(event)] = struct{}{}
	}
	kept := make([]freeBusyCandidate, 0, len(visible))
	for _, candidate := range candidates {
		if _, ok := allowed[eventResourceName(candidate.event)]; ok {
			kept = append(kept, candidate)
		}
	}
	return kept, nil
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

// freeBusyPushdownTimeRange is the range free-busy narrows its database read
// with: its own direct time-range, else whatever calendarQueryVEventTimeRange
// finds, so both reports derive their candidate set under the same rule.
func freeBusyPushdownTimeRange(filter *calFilter, tr *timeRange) *timeRange {
	if tr != nil {
		return tr
	}
	return calendarQueryVEventTimeRange(filter)
}

// freeBusyTimeRange is the range the reported VFREEBUSY covers, which is a
// different question from the one freeBusyPushdownTimeRange answers: this one
// bounds the periods written into the response, that one bounds the rows read
// out of the database. It takes the innermost range the filter carries, since
// that is the narrowest bound the request expressed.
func freeBusyTimeRange(filter *calFilter, tr *timeRange) *timeRange {
	if tr != nil {
		return tr
	}
	return effectiveTimeRange(filter)
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

// matcherInTimeRange reports whether any component of the parsed resource
// intersects tr under the RFC 4791 §9.9 tables. It is the resource-level test
// free-busy selects with, so it applies the tables to every component §9.9
// defines one for; the calendar-query filter walk scopes the same tables to the
// single component the comp-filter names instead. Octets that did not parse
// carry no component to judge and intersect nothing.
func matcherInTimeRange(matcher calendarTimeRangeMatcher, tr *timeRange) bool {
	start, end, ok := calendarTimeRangeBounds(tr)
	if !ok || matcher.root == nil {
		return false
	}
	for _, component := range matcher.root.children {
		if !calendarTimeRangeComponents.contains(component.name) {
			continue
		}
		if matcher.componentSetInTimeRange(component, matcher.root, start, end) {
			return true
		}
	}
	return false
}

// busyPeriodOverlapsRange reports whether one generated busy period reaches the
// range the reported VFREEBUSY covers. A period of zero length is an instant, so
// it is tested with an inclusive start and an exclusive end rather than through
// the half-open overlap an interval uses.
func busyPeriodOverlapsRange(periodStart, periodEnd, rangeStart, rangeEnd time.Time) bool {
	if periodEnd.Equal(periodStart) {
		return !periodStart.Before(rangeStart) && periodStart.Before(rangeEnd)
	}
	return periodStart.Before(rangeEnd) && periodEnd.After(rangeStart)
}

// freeBusyHasEffectiveTimeRange reports whether a free-busy-query carries a
// usable time-range from either source. RFC 4791 §7.10 requires exactly one
// CALDAV:time-range; without it the report degenerates into a full-collection
// read and an unbounded text/calendar response.
func freeBusyHasEffectiveTimeRange(filter *calFilter, tr *timeRange) bool {
	_, _, ok := calendarTimeRangeBounds(freeBusyTimeRange(filter, tr))
	return ok
}

// generateFreeBusy builds the §7.10 response body. Each candidate resolves its
// periods through the zone it was parsed with, which RFC 4791 §7.3 makes the
// collection's CALDAV:calendar-timezone for this report.
func (h *DavServer) generateFreeBusy(candidates []freeBusyCandidate, filter *calFilter, tr *timeRange) string {
	var sb strings.Builder
	sb.WriteString("BEGIN:VCALENDAR\r\n")
	sb.WriteString("VERSION:2.0\r\n")
	sb.WriteString("PRODID:-//CalCard//CalDAV Server//EN\r\n")
	sb.WriteString("BEGIN:VFREEBUSY\r\n")
	now := time.Now().UTC()
	// RFC 5545 §3.6.4 makes UID as REQUIRED in a VFREEBUSY as DTSTAMP is, so a
	// response omitting it is not a valid iCalendar object. The report builds a
	// fresh component per request rather than identifying a stored resource, so
	// the value only has to be unique.
	sb.WriteString(fmt.Sprintf("UID:%s-%s@calcard\r\n", now.Format("20060102T150405Z"), freeBusyUIDSuffix()))
	sb.WriteString(fmt.Sprintf("DTSTAMP:%s\r\n", now.Format("20060102T150405Z")))

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

	for _, candidate := range candidates {
		for _, period := range freeBusyPeriods(candidate, rangeStart, rangeEnd, hasRange) {
			startStr := period.Start.UTC().Format("20060102T150405Z")
			endStr := period.End.UTC().Format("20060102T150405Z")
			sb.WriteString(fmt.Sprintf("FREEBUSY:%s/%s\r\n", startStr, endStr))
		}
	}

	sb.WriteString("END:VFREEBUSY\r\n")
	sb.WriteString("END:VCALENDAR\r\n")

	return sb.String()
}

// freeBusyUIDSuffix makes the generated VFREEBUSY UID unique even for two
// requests answered inside the same second. A failure to read the entropy pool
// is not worth failing the report over, so the timestamp alone carries it.
func freeBusyUIDSuffix() string {
	b := make([]byte, 8)
	if _, err := rand.Read(b); err != nil {
		return "freebusy"
	}
	return hex.EncodeToString(b)
}

func freeBusyPeriods(candidate freeBusyCandidate, rangeStart, rangeEnd time.Time, hasRange bool) []ical.BusyPeriod {
	extent, ok := candidate.extent()
	if !ok {
		return nil
	}
	end := extent.start.Add(extent.length)
	if !hasRange {
		return []ical.BusyPeriod{{Start: extent.start, End: end}}
	}
	if ical.EventHasRecurrence(candidate.event.RawICAL) {
		// The expansion reads its EXDATEs and RDATEs through the same resolver
		// that placed the start, so an exception still names an occurrence the
		// zone moved.
		return ical.RecurringBusyPeriods(candidate.event.RawICAL, extent.start, extent.length,
			rangeStart, rangeEnd, ical.MaxRecurrenceInstances, extent.resolve)
	}
	if busyPeriodOverlapsRange(extent.start, end, rangeStart, rangeEnd) {
		return []ical.BusyPeriod{{Start: extent.start, End: end}}
	}
	return nil
}

// freeBusyExtent is where a calendar object starts, how long one occurrence of
// it lasts, and the resolver that placed both -- so the recurrence expansion
// resolves the rest of the set the same way, and the recurring and
// non-recurring paths cannot disagree about the same event.
type freeBusyExtent struct {
	start   time.Time
	length  time.Duration
	resolve ical.PropertyTimeResolver
}

// extent resolves the candidate's occupied interval from its own components.
//
// Values resolve through the zone the candidate was parsed with, which RFC 4791
// §7.3 makes the collection's CALDAV:calendar-timezone for this report. A
// floating DTSTART names a wall clock rather than an instant, so reading it as
// UTC publishes a period hours away from the one the client asked about. The
// denormalized store columns are the fallback for a resource whose own octets
// carry no usable DTSTART, which is the only case where they say more than the
// component does.
func (c freeBusyCandidate) extent() (freeBusyExtent, bool) {
	if c.parsed {
		if master := freeBusyMasterComponent(c.matcher.root); master != nil {
			if dtstart, ok := c.matcher.dateValue(master, "DTSTART", 0); ok {
				return freeBusyExtent{
					start:   dtstart.instant,
					length:  freeBusyOccurrenceLength(c.matcher, master, dtstart, c.event),
					resolve: c.matcher.resolveContentLine,
				}, true
			}
		}
	}
	if c.event.DTStart == nil {
		return freeBusyExtent{}, false
	}
	return freeBusyExtent{
		start:   *c.event.DTStart,
		length:  freeBusyColumnLength(c.event, *c.event.DTStart),
		resolve: c.matcher.resolveContentLine,
	}, true
}

// freeBusyMasterComponent is the VEVENT a free-busy period is derived from: the
// first carrying no RECURRENCE-ID, since RFC 4791 §4.1 permits a resource made
// only of overridden instances and every one of those carries the property.
func freeBusyMasterComponent(root *icalNode) *icalNode {
	var firstEvent *icalNode
	for _, child := range root.children {
		if child.name != "VEVENT" {
			continue
		}
		if child.count("RECURRENCE-ID") == 0 {
			return child
		}
		if firstEvent == nil {
			firstEvent = child
		}
	}
	return firstEvent
}

func freeBusyOccurrenceLength(matcher calendarTimeRangeMatcher, master *icalNode, dtstart icalTimeValue, event store.Event) time.Duration {
	if dtend, ok := matcher.dateValue(master, "DTEND", 0); ok {
		if length := dtend.instant.Sub(dtstart.instant); length > 0 {
			return length
		}
	}
	// A DURATION written as zero is an instant, which busyPeriodOverlapsRange
	// tests on a different condition than an interval, so it is kept rather
	// than treated as missing.
	if length, ok := componentDuration(master, "DURATION"); ok && length >= 0 {
		return length
	}
	if dtstart.isDate {
		// Nominal rather than a fixed 24 hours, so the day a DATE implies still
		// ends at midnight on a date the zone changes offset.
		return matcher.zone.addDays(dtstart.instant, 1).Sub(dtstart.instant)
	}
	return freeBusyColumnLength(event, dtstart.instant)
}

// freeBusyColumnLength reads the length off the denormalized columns, for an
// event whose own octets bound nothing. RFC 5545 §3.6.1 ends an event carrying
// a DATE-TIME DTSTART and no DTEND at that same instant, so nothing left to read
// means zero rather than a guessed length.
func freeBusyColumnLength(event store.Event, start time.Time) time.Duration {
	if event.DTEnd != nil {
		if length := event.DTEnd.Sub(start); length > 0 {
			return length
		}
	}
	if event.AllDay {
		return 24 * time.Hour
	}
	return 0
}
