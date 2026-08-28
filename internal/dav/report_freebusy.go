package dav

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"slices"
	"strings"
	"sync/atomic"
	"time"

	"github.com/jw6ventures/calcard/internal/ical"
	"github.com/jw6ventures/calcard/internal/store"
)

// freeBusyQuery returns the free-busy iCalendar text for the calendar objects
// visible to user and intersecting the report's required time range.
func (h *DavServer) freeBusyQuery(ctx context.Context, user *store.User, cal *store.CalendarAccess, tr *timeRange) (string, error) {
	// §7.3 gives free-busy only the collection's timezone: the report carries
	// no CALDAV:timezone element of its own.
	zone := reportFloatingZone("", cal.Timezone)
	events, err := h.listCalendarEventsForTimeRange(ctx, cal.ID, tr)
	if err != nil {
		// §7.10 gives this report the §7.8 postcondition, so a row budget the
		// generic reports answer with a capacity status is named here.
		if errors.Is(err, errTooManyCandidateRows) {
			return "", errNumberOfMatchesExceeded
		}
		return "", errors.New("failed to list events")
	}

	candidates := filterFreeBusyCandidatesByTimeRange(freeBusyCandidates(events, zone), tr)
	candidates, err = h.filterFreeBusyCandidatesByPrivilege(ctx, user, cal, candidates)
	if err != nil {
		return "", err
	}

	return h.generateFreeBusy(candidates, tr), nil
}

// freeBusyCandidate is one calendar object under consideration, parsed once so
// its time-range test and period derivation read the same component tree.
type freeBusyCandidate struct {
	event   store.Event
	matcher calendarTimeRangeMatcher
}

// freeBusyCandidates parses each stored object once. Octets that do not parse
// carry no component to read a period, a TRANSP or a STATUS off, so such an
// object is left out here rather than carried forward as a candidate every
// later stage would have to keep excluding.
func freeBusyCandidates(events []store.Event, zone floatingZone) []freeBusyCandidate {
	candidates := make([]freeBusyCandidate, 0, len(events))
	for _, event := range events {
		matcher, parsed := newEventTimeRangeMatcher(event, zone)
		if !parsed {
			continue
		}
		candidates = append(candidates, freeBusyCandidate{event: event, matcher: matcher})
	}
	return candidates
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
// time-range when one is present. The returned rows are a superset; callers
// must still apply exact filtering.
func (h *DavServer) listCalendarEventsForTimeRange(ctx context.Context, calendarID int64, tr *timeRange) ([]store.Event, error) {
	databaseFilter, _ := eventFilterFromTimeRange(tr)
	return h.listBoundedCalendarEvents(ctx, calendarID, databaseFilter)
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

// busyPeriodOverlapsRange reports whether one generated candidate reaches the
// range the reported VFREEBUSY covers. A zero-length VEVENT is selected at its
// instant here, then removed before serialization because it cannot form a
// valid PERIOD.
func busyPeriodOverlapsRange(periodStart, periodEnd, rangeStart, rangeEnd time.Time) bool {
	if periodEnd.Equal(periodStart) {
		return !periodStart.Before(rangeStart) && periodStart.Before(rangeEnd)
	}
	return periodStart.Before(rangeEnd) && periodEnd.After(rangeStart)
}

// freeBusyHasTimeRange reports whether the range the §9.11 grammar required
// carries usable bounds. Without them the report degenerates into a
// full-collection read and an unbounded text/calendar response.
func freeBusyHasTimeRange(tr *timeRange) bool {
	_, _, ok := calendarTimeRangeBounds(tr)
	return ok
}

// generateFreeBusy builds the §7.10 response body. Each candidate resolves its
// periods through the zone it was parsed with, which RFC 4791 §7.3 makes the
// collection's CALDAV:calendar-timezone for this report.
func (h *DavServer) generateFreeBusy(candidates []freeBusyCandidate, tr *timeRange) string {
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

	rangeStart, rangeEnd, hasRange := calendarTimeRangeBounds(tr)
	if tr != nil {
		if tr.Start != "" {
			sb.WriteString(fmt.Sprintf("DTSTART:%s\r\n", tr.Start))
		}
		if tr.End != "" {
			sb.WriteString(fmt.Sprintf("DTEND:%s\r\n", tr.End))
		}
	}

	var intervals []freeBusyInterval
	for _, candidate := range candidates {
		intervals = append(intervals, freeBusyIntervals(candidate, rangeStart, rangeEnd, hasRange)...)
	}
	// §7.10 asks for duplicates to be dropped and consecutive or overlapping
	// periods of the same type to be coalesced. Both are collection-wide
	// questions, so the merge runs once over every candidate rather than per
	// resource: two calendar objects can perfectly well cover the same hour.
	for _, interval := range mergeFreeBusyPeriods(intervals) {
		sb.WriteString(fmt.Sprintf("FREEBUSY%s:%s/%s\r\n",
			freeBusyTypeParameter(interval.fbType),
			interval.start.UTC().Format("20060102T150405Z"),
			interval.end.UTC().Format("20060102T150405Z")))
	}

	sb.WriteString("END:VFREEBUSY\r\n")
	sb.WriteString("END:VCALENDAR\r\n")

	return sb.String()
}

var freeBusyUIDFallbackSequence atomic.Uint64

func freeBusyUIDSuffix() string {
	return freeBusyUIDSuffixFrom(rand.Reader, time.Now().UTC())
}

func freeBusyUIDSuffixFrom(reader io.Reader, now time.Time) string {
	b := make([]byte, 8)
	if _, err := io.ReadFull(reader, b); err == nil {
		return hex.EncodeToString(b)
	}
	return fmt.Sprintf("fallback-%x-%x", now.UnixNano(), freeBusyUIDFallbackSequence.Add(1))
}

// freeBusyInterval is one published busy period and the FBTYPE it is published
// under. RFC 4791 §7.10 lets periods of different types overlap, so the type is
// carried through the merge rather than resolved at the end.
type freeBusyInterval struct {
	start  time.Time
	end    time.Time
	fbType string
}

// freeBusyBusy is the FBTYPE RFC 5545 §3.2.9 defaults to, which is why the
// parameter is written out only for the other types.
const freeBusyBusy = "BUSY"

func freeBusyTypeParameter(fbType string) string {
	if fbType == "" || fbType == freeBusyBusy {
		return ""
	}
	return ";FBTYPE=" + fbType
}

// freeBusyIntervals is every busy period one calendar object publishes.
// RFC 4791 §7.10 considers a VEVENT that is absent TRANSP or names OPAQUE, plus
// every VFREEBUSY, so an object can contribute through both routes.
func freeBusyIntervals(candidate freeBusyCandidate, rangeStart, rangeEnd time.Time, hasRange bool) []freeBusyInterval {
	var intervals []freeBusyInterval
	// A recurrence set belongs to the resource rather than to any one of its
	// components, so it is expanded once from the master and each period is then
	// typed by whichever component actually describes the instance it came from.
	//
	// That route requires a master defining a set. Without one -- a resource of
	// overridden instances alone, which §4.1 permits, or a master carrying no
	// recurrence property beside a RECURRENCE-ID sibling -- every VEVENT stands
	// for itself and is typed by itself, which is also what the §9.6.5 expansion
	// returns for the same octets.
	master := freeBusyMasterComponent(candidate.matcher.root)
	if master != nil && ical.EventHasRecurrence(candidate.event.RawICAL) {
		intervals = append(intervals, freeBusyEventIntervals(candidate, master, rangeStart, rangeEnd, hasRange)...)
	} else {
		for _, child := range candidate.matcher.root.children {
			if child.name != "VEVENT" {
				continue
			}
			if interval, ok := freeBusyStandaloneEventInterval(candidate, child, rangeStart, rangeEnd, hasRange); ok {
				intervals = append(intervals, interval)
			}
		}
	}
	for _, child := range candidate.matcher.root.children {
		if child.name == "VFREEBUSY" {
			intervals = append(intervals, freeBusyStoredPeriods(candidate.matcher, child, rangeStart, rangeEnd, hasRange)...)
		}
	}
	return intervals
}

// freeBusyOverride links a recurrence slot to the component describing it.
// §7.10 derives FBTYPE from that component's TRANSP and STATUS.
type freeBusyOverride struct {
	recurrenceID  time.Time
	node          *icalNode
	thisAndFuture bool
}

func freeBusyOverrides(m calendarTimeRangeMatcher, root, master *icalNode) []freeBusyOverride {
	var overrides []freeBusyOverride
	for _, child := range root.children {
		if child.name != master.name || child.count("RECURRENCE-ID") == 0 {
			continue
		}
		recurrenceID, ok := m.dateValue(child, "RECURRENCE-ID", 0)
		if !ok {
			continue
		}
		overrides = append(overrides, freeBusyOverride{
			recurrenceID:  recurrenceID.instant,
			node:          child,
			thisAndFuture: thisAndFutureOverride(child),
		})
	}
	return overrides
}

// freeBusyDescribingComponent is the component that describes the recurrence
// slot: its exact override, else the nearest preceding RANGE=THISANDFUTURE
// override, else the master.
func freeBusyDescribingComponent(master *icalNode, overrides []freeBusyOverride, recurrenceID time.Time) *icalNode {
	var governing *freeBusyOverride
	for i := range overrides {
		override := &overrides[i]
		if override.recurrenceID.Equal(recurrenceID) {
			return override.node
		}
		if !override.thisAndFuture || override.recurrenceID.After(recurrenceID) {
			continue
		}
		if governing == nil || override.recurrenceID.After(governing.recurrenceID) {
			governing = override
		}
	}
	if governing != nil {
		return governing.node
	}
	return master
}

// freeBusyEventType maps a VEVENT's TRANSP and STATUS to the FBTYPE its periods
// carry, per the RFC 4791 §7.10 table. ok is false for a component publishing no
// busy time at all: TRANSP:TRANSPARENT under any status, and STATUS:CANCELLED
// under OPAQUE. Both map to FREE, and this report returns only busy time.
func freeBusyEventType(node *icalNode) (string, bool) {
	if strings.EqualFold(strings.TrimSpace(node.value("TRANSP")), "TRANSPARENT") {
		return "", false
	}
	switch strings.ToUpper(strings.TrimSpace(node.value("STATUS"))) {
	case "CANCELLED":
		return "", false
	case "TENTATIVE":
		return "BUSY-TENTATIVE", true
	default:
		// CONFIRMED, absent, and the x-name row, which §7.10 lets a server
		// answer as BUSY.
		return freeBusyBusy, true
	}
}

// freeBusyEventIntervals derives the periods a resource's VEVENT recurrence set
// occupies, each tagged with the FBTYPE of the component that describes its
// instance. master is the component defining that set, which the caller has
// established does define one.
func freeBusyEventIntervals(candidate freeBusyCandidate, master *icalNode, rangeStart, rangeEnd time.Time, hasRange bool) []freeBusyInterval {
	extent, ok := candidate.extent(master)
	if !ok {
		return nil
	}
	periods := []ical.BusyPeriod{{Start: extent.start, End: extent.start.Add(extent.length)}}
	if hasRange {
		// The expansion reads its EXDATEs and RDATEs through the same resolver
		// that placed the start, so an exception still names an occurrence the
		// zone moved.
		periods = ical.RecurringBusyPeriods(candidate.event.RawICAL, extent.start, extent.length,
			rangeStart, rangeEnd, ical.MaxRecurrenceInstances, extent.resolve)
	}

	overrides := freeBusyOverrides(candidate.matcher, candidate.matcher.root, master)
	intervals := make([]freeBusyInterval, 0, len(periods))
	for _, period := range periods {
		recurrenceID := period.RecurrenceID
		if recurrenceID.IsZero() {
			recurrenceID = period.Start
		}
		describing := freeBusyDescribingComponent(master, overrides, recurrenceID)
		fbType, publishes := freeBusyEventType(describing)
		if !publishes {
			continue
		}
		intervals = append(intervals, freeBusyInterval{start: period.Start, end: period.End, fbType: fbType})
	}
	return intervals
}

func freeBusyStandaloneEventInterval(candidate freeBusyCandidate, node *icalNode, rangeStart, rangeEnd time.Time, hasRange bool) (freeBusyInterval, bool) {
	dtstart, ok := candidate.matcher.dateValue(node, "DTSTART", 0)
	if !ok {
		return freeBusyInterval{}, false
	}
	end := dtstart.instant.Add(freeBusyOccurrenceLength(candidate.matcher, node, dtstart, candidate.event))
	if hasRange && !busyPeriodOverlapsRange(dtstart.instant, end, rangeStart, rangeEnd) {
		return freeBusyInterval{}, false
	}
	fbType, publishes := freeBusyEventType(node)
	if !publishes {
		return freeBusyInterval{}, false
	}
	return freeBusyInterval{start: dtstart.instant, end: end, fbType: fbType}, true
}

// freeBusyStoredPeriods reads the FREEBUSY properties of a stored VFREEBUSY,
// each under the FBTYPE its own parameter names. RFC 5545 §3.2.9 defaults that
// to BUSY, and FREE names free time, which this report does not publish.
func freeBusyStoredPeriods(matcher calendarTimeRangeMatcher, node *icalNode, rangeStart, rangeEnd time.Time, hasRange bool) []freeBusyInterval {
	var intervals []freeBusyInterval
	for _, property := range node.properties {
		if property.name != "FREEBUSY" {
			continue
		}
		fbType := strings.ToUpper(strings.TrimSpace(property.parameters["FBTYPE"]))
		if fbType == "" {
			fbType = freeBusyBusy
		}
		if fbType == "FREE" {
			continue
		}
		for _, value := range strings.Split(property.value, ",") {
			start, end, ok := matcher.freeBusyPeriod(property, strings.TrimSpace(value))
			if !ok {
				continue
			}
			if hasRange && !busyPeriodOverlapsRange(start, end, rangeStart, rangeEnd) {
				continue
			}
			intervals = append(intervals, freeBusyInterval{start: start, end: end, fbType: fbType})
		}
	}
	return intervals
}

// mergeFreeBusyPeriods removes values that cannot form an RFC 5545 PERIOD,
// then de-duplicates and coalesces the remaining periods per FBTYPE. The result
// is in the ascending start order RFC 5545 §3.8.2.6 asks it to publish in.
//
// The argument is reordered in place: grouping by type is what lets the single
// pass below coalesce, so the caller passes ownership of the slice.
func mergeFreeBusyPeriods(intervals []freeBusyInterval) []freeBusyInterval {
	valid := intervals[:0]
	for _, interval := range intervals {
		if interval.end.After(interval.start) {
			valid = append(valid, interval)
		}
	}
	intervals = valid
	if len(intervals) < 2 {
		return intervals
	}
	// Only periods of the same FBTYPE may merge, and §7.10 lets two types cover
	// one interval, so the grouping is internal to the merge rather than the
	// order the result is published in.
	slices.SortFunc(intervals, func(a, b freeBusyInterval) int {
		if order := strings.Compare(a.fbType, b.fbType); order != 0 {
			return order
		}
		return compareFreeBusyPlacement(a, b)
	})

	merged := make([]freeBusyInterval, 0, len(intervals))
	for _, interval := range intervals {
		if len(merged) == 0 {
			merged = append(merged, interval)
			continue
		}
		last := &merged[len(merged)-1]
		if last.fbType != interval.fbType || interval.start.After(last.end) {
			merged = append(merged, interval)
			continue
		}
		if interval.end.After(last.end) {
			last.end = interval.end
		}
	}

	slices.SortFunc(merged, func(a, b freeBusyInterval) int {
		if order := compareFreeBusyPlacement(a, b); order != 0 {
			return order
		}
		// Two types covering one interval need a tie-break, or the order two
		// equally-placed periods come out in depends on candidate order.
		return strings.Compare(a.fbType, b.fbType)
	})
	return merged
}

// compareFreeBusyPlacement orders two periods by where they fall, which is the
// start-then-end order RFC 5545 §3.8.2.6 publishes in.
func compareFreeBusyPlacement(a, b freeBusyInterval) int {
	if order := a.start.Compare(b.start); order != 0 {
		return order
	}
	return a.end.Compare(b.end)
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

// extent resolves the interval one occurrence of master occupies.
//
// Values resolve through the zone the candidate was parsed with, which RFC 4791
// §7.3 makes the collection's CALDAV:calendar-timezone for this report. A
// floating DTSTART names a wall clock rather than an instant, so reading it as
// UTC publishes a period hours away from the one the client asked about. The
// denormalized store columns are the fallback for a master whose own DTSTART
// does not resolve, which is the only case where they say more than the
// component does.
//
// master is the component freeBusyIntervals expanded the recurrence set from,
// so the extent and the periods derived from it cannot come to describe two
// different components of one resource.
func (c freeBusyCandidate) extent(master *icalNode) (freeBusyExtent, bool) {
	if dtstart, ok := c.matcher.dateValue(master, "DTSTART", 0); ok {
		return freeBusyExtent{
			start:   dtstart.instant,
			length:  freeBusyOccurrenceLength(c.matcher, master, dtstart, c.event),
			resolve: c.matcher.resolveContentLine,
		}, true
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

// freeBusyMasterComponent returns the VEVENT defining the recurrence set.
// A resource containing only overridden instances has no master.
func freeBusyMasterComponent(root *icalNode) *icalNode {
	for _, child := range root.children {
		if child.name != "VEVENT" {
			continue
		}
		if child.count("RECURRENCE-ID") == 0 {
			return child
		}
	}
	return nil
}

func freeBusyOccurrenceLength(matcher calendarTimeRangeMatcher, master *icalNode, dtstart icalTimeValue, event store.Event) time.Duration {
	if dtend, ok := matcher.dateValue(master, "DTEND", 0); ok {
		if length := dtend.instant.Sub(dtstart.instant); length > 0 {
			return length
		}
	}
	// A non-positive duration occupies no time and is removed before the report
	// serializes its intervals.
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
