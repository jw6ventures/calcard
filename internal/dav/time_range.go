package dav

import (
	"errors"
	"strconv"
	"strings"
	"time"

	"github.com/jw6ventures/calcard/internal/ical"
)

// recurringTimeRangeComponents are the components whose recurrence set a
// time-range test expands. §9.9 requires every instance to be considered;
// VFREEBUSY and VALARM carry no recurrence pattern of their own.
var recurringTimeRangeComponents = nameSet("VEVENT", "VTODO", "VJOURNAL")

// floatingZone resolves a floating iCalendar value -- one carrying neither a
// TZID parameter nor a zone suffix -- to an absolute instant, which RFC 4791
// §7.3 makes the request's CALDAV:timezone, else the collection's
// CALDAV:calendar-timezone, else UTC.
//
// The observances the definition ships are what answer, because the definition
// is the reference §7.3 names. A TZID that also happens to name an IANA zone
// does not make the host's copy authoritative: the two can disagree, and
// answering from the host's would resolve the value against a zone the request
// never described.
//
// loc holds the host's location only as the fallback for a definition this
// server cannot walk -- one carrying no usable STANDARD or DAYLIGHT observance.
// The two are never both in force, so whichever is set is the zone.
type floatingZone struct {
	loc  *time.Location
	root *icalNode
	tzid string
}

// newFloatingZone reads the first VTIMEZONE out of an iCalendar object. An
// empty or unusable definition yields the zero value, which resolves as UTC.
func newFloatingZone(icalText string) floatingZone {
	if strings.TrimSpace(icalText) == "" {
		return floatingZone{}
	}
	root, err := parseICalendarObject(icalText)
	if err != nil {
		return floatingZone{}
	}
	for _, child := range root.children {
		if child.name != "VTIMEZONE" {
			continue
		}
		tzid := strings.TrimSpace(child.value("TZID"))
		if tzid == "" {
			return floatingZone{}
		}
		if observances, ok := submittedTimezoneObservances(root, tzid); ok && len(observances) > 0 {
			return floatingZone{root: root, tzid: tzid}
		}
		// Nothing walkable was submitted. The host's database is the only
		// remaining source of rules for this name.
		if loc, err := time.LoadLocation(tzid); err == nil {
			return floatingZone{loc: loc, root: root, tzid: tzid}
		}
		return floatingZone{root: root, tzid: tzid}
	}
	return floatingZone{}
}

// reportFloatingZone selects the zone a calendaring REPORT resolves floating
// values against, which RFC 4791 §7.3 orders: the CALDAV:timezone the request
// carries, else the CALDAV:calendar-timezone the targeted collection defines,
// else UTC. requestTimezone is empty for free-busy, which has no such element.
func reportFloatingZone(requestTimezone string, collectionTimezone *string) floatingZone {
	if zone := newFloatingZone(requestTimezone); zone.root != nil {
		return zone
	}
	if collectionTimezone != nil {
		if zone := newFloatingZone(wrapCalendarTimezone(*collectionTimezone)); zone.root != nil {
			return zone
		}
	}
	return floatingZone{}
}

func (z floatingZone) resolve(value string) (time.Time, bool) {
	if z.loc != nil {
		parsed, err := ical.ParseDateTimeInLocation(value, z.loc)
		return parsed, err == nil
	}
	wall, err := ical.ParseDateTime(value)
	if err != nil {
		return time.Time{}, false
	}
	if z.root != nil {
		if offset, ok := submittedTimezoneOffset(z.root, z.tzid, wall); ok {
			return wall.Add(-offset).UTC(), true
		}
	}
	return wall, true
}

// addDays advances by a nominal number of days: the same wall-clock time on a
// later date rather than a fixed multiple of 24 hours, which is what the +P1D
// the §9.9 tables imply for a DATE value means across a zone transition.
//
// The submitted observances answer here for the same reason they answer in
// resolve. Reading them for the offset and then doing the arithmetic in UTC
// would put the two on different zones, and the day that spans a transition --
// the only day whose length this function exists to get right -- is exactly
// where they would disagree.
func (z floatingZone) addDays(instant time.Time, days int) time.Time {
	if z.root != nil {
		if offset, ok := submittedTimezoneOffsetAtInstant(z.root, z.tzid, instant); ok {
			wall := instant.Add(offset).UTC().AddDate(0, 0, days)
			if shifted, ok := submittedTimezoneOffset(z.root, z.tzid, wall); ok {
				return wall.Add(-shifted).UTC()
			}
		}
	}
	if z.loc == nil {
		return instant.AddDate(0, 0, days)
	}
	return instant.In(z.loc).AddDate(0, 0, days).UTC()
}

// wallClock renders an instant as the wall-clock reading in the zone, which is
// the spelling a DATE or floating value is written back out with.
func (z floatingZone) wallClock(instant time.Time) time.Time {
	if z.loc != nil {
		return instant.In(z.loc)
	}
	if z.root != nil {
		if offset, ok := submittedTimezoneOffsetAtInstant(z.root, z.tzid, instant); ok {
			return instant.Add(offset).UTC()
		}
	}
	return instant.UTC()
}

// icalTimeValue is one resolved date-valued property: the instant it names and
// whether it was written as a DATE, which several §9.9 rows switch on.
type icalTimeValue struct {
	instant time.Time
	isDate  bool
}

// calendarTimeRangeMatcher evaluates RFC 4791 §9.9 against one calendar object
// resource. It holds the resource octets so a recurring component's instance set
// can be expanded, the parsed root so a TZID can be resolved against the
// VTIMEZONE the resource ships, and the zone floating values fall back to.
//
// §9.9 is a per-component intersection test rather than one rule over a
// resource's start and end: each component type gets its own table and the
// tables disagree on which bound is inclusive. The section also leaves the
// semantic "not defined for any other calendar components and properties", so a
// time-range scoped anywhere else matches nothing here. The filter grammar
// refuses those names too, but this evaluator answers independently rather than
// trusting that, since it also runs over stored data the grammar never saw.
type calendarTimeRangeMatcher struct {
	// Copies made during the component walk share expansion failures.
	expansionError *error
	raw            string
	root           *icalNode
	zone           floatingZone
}

func newCalendarTimeRangeMatcher(raw string, root *icalNode, zone floatingZone) calendarTimeRangeMatcher {
	return calendarTimeRangeMatcher{raw: raw, root: root, zone: zone, expansionError: new(error)}
}

// dateValue resolves one date-valued property of node, shifting it by shift so
// a generated recurrence instance can be tested against the master's property
// set. §9.9 requires the effective DTSTART, DTEND, DURATION and DUE of an
// instance to be inferred; the other date properties are metadata of the
// component as a whole and are read unshifted.
func (m calendarTimeRangeMatcher) dateValue(node *icalNode, name string, shift time.Duration) (icalTimeValue, bool) {
	property, ok := firstICalProperty(node, name)
	if !ok {
		return icalTimeValue{}, false
	}
	value := strings.TrimSpace(property.value)
	form, ok := parseICalDateForm(value)
	if !ok {
		return icalTimeValue{}, false
	}
	instant, ok := m.resolveInstant(property.parameters["TZID"], value, form)
	if !ok {
		return icalTimeValue{}, false
	}
	return icalTimeValue{instant: instant.Add(shift), isDate: form == icalDateOnly}, true
}

func (m calendarTimeRangeMatcher) resolveInstant(tzid, value string, form icalDateForm) (time.Time, bool) {
	if form == icalUTCDateTime {
		parsed, err := ical.ParseDateTime(value)
		return parsed, err == nil
	}
	if tzid = strings.TrimSpace(tzid); tzid != "" {
		// RFC 4791 §4.1 requires the resource to carry a VTIMEZONE for every
		// TZID it uses, so that definition is authoritative over the host's.
		if wall, err := ical.ParseDateTime(value); err == nil && m.root != nil {
			if offset, ok := submittedTimezoneOffset(m.root, tzid, wall); ok {
				return wall.Add(-offset).UTC(), true
			}
		}
		if loc, err := time.LoadLocation(tzid); err == nil {
			parsed, err := ical.ParseDateTimeInLocation(value, loc)
			return parsed, err == nil
		}
	}
	return m.zone.resolve(value)
}

// resolveContentLine answers one date-valued content line for the recurrence
// expansion, which reaches EXDATE, RDATE, RECURRENCE-ID and a floating UNTIL --
// the dates a component's DTSTART has to agree with. Handing the expansion this
// rather than letting it read them itself is what keeps one zone answering for
// the whole recurrence set.
func (m calendarTimeRangeMatcher) resolveContentLine(keyPart, value string) (time.Time, bool) {
	value = strings.TrimSpace(value)
	form, ok := parseICalDateForm(value)
	if !ok {
		return time.Time{}, false
	}
	tzid, _ := ical.PropertyParam(keyPart, "TZID")
	return m.resolveInstant(tzid, value, form)
}

func componentDuration(node *icalNode, name string) (time.Duration, bool) {
	property, ok := firstICalProperty(node, name)
	if !ok {
		return 0, false
	}
	return ical.ParseDuration(property.value)
}

// componentSetInTimeRange reports whether the component intersects
// [start, end). §9.9 requires every recurrence instance to be considered and
// makes one match enough.
func (m calendarTimeRangeMatcher) componentSetInTimeRange(node, parent *icalNode, start, end time.Time) bool {
	if hasAbsoluteAlarmTrigger(node) {
		// RFC 5545 §3.8.6.3: an absolute TRIGGER names a fixed instant, so it
		// fires once however often the enclosing component recurs. Every
		// instance would answer identically, and an instance scan that reaches
		// none of them must not suppress a trigger that does fall in the range.
		return m.alarmInTimeRange(node, parent, start, end, 0)
	}
	master := m.recurrenceMaster(node, parent)
	instances, expandable, settled := m.recurrenceInstances(node, master, start, end)
	if !expandable {
		// A frequency this server cannot expand is kept rather than filtered
		// out: reporting nothing would hide a resource that does occur in the
		// range.
		return true
	}
	for _, instance := range instances {
		content, enclosing := node, parent
		shift := instance.shift
		if master != nil {
			occurrence := expandedInstance(m, m.root, master, instance)
			if node == master {
				content = occurrence
			} else {
				enclosing = occurrence
			}
			shift = 0
		}
		if m.componentInTimeRange(content, enclosing, start, end, shift) {
			return true
		}
	}
	// No instance of the prefix matched, which settles the question only when the
	// prefix was the whole set. Otherwise the resource is kept on the same ground
	// an inexpressible frequency is.
	return !settled
}

// recurrenceMaster is the component whose recurrence set governs node: node
// itself when it carries a pattern, and the component enclosing it otherwise,
// since a VALARM defines no pattern of its own but fires once per instance of
// the VEVENT or VTODO holding it. nil means nothing here recurs.
func (m calendarTimeRangeMatcher) recurrenceMaster(node, parent *icalNode) *icalNode {
	if m.hasRecurrenceSet(node) {
		return node
	}
	if parent != nil && m.hasRecurrenceSet(parent) {
		return parent
	}
	return nil
}

// recurrenceInstance is one generated occurrence expressed as offsets from the
// master's DTSTART: where the occurrence falls, and which slot of the pattern it
// belongs to. The two differ only when a RANGE=THISANDFUTURE override moved it,
// and a caller writing a RECURRENCE-ID out needs the slot rather than the
// occurrence (RFC 5545 §3.8.4.4).
type recurrenceInstance struct {
	shift     time.Duration
	slotShift time.Duration
	duration  time.Duration
}

// openTimeRangeEnd reports whether the end of a range is the +infinity RFC 4791
// §9.9 gives an omitted CALDAV:time-range attribute rather than an instant the
// client spelled. The two are told apart by the value alone because every
// spelled endpoint is first checked against CALDAV:max-date-time, which the
// sentinel calendarTimeRangeBounds substitutes sits far beyond: no request can
// name this instant itself.
func openTimeRangeEnd(end time.Time) bool {
	_, maxTime := ical.DateLimits()
	return end.After(maxTime)
}

// recurrenceInstances retains the effective duration and original identity of
// each occurrence. expandable is false when nothing here can be enumerated at
// all. settled is false when the instances returned are a prefix of the set and
// that prefix does not answer the question asked of it, which a caller deciding
// a §9.9 match has to read as "no answer" rather than "no match".
func (m calendarTimeRangeMatcher) recurrenceInstances(node, master *icalNode, start, end time.Time) (_ []recurrenceInstance, expandable, settled bool) {
	if master == nil {
		return []recurrenceInstance{{}}, true, true
	}
	if !ical.SupportedRecurrenceRule(master.value("RRULE")) {
		return nil, false, false
	}
	dtstart, ok := m.dateValue(master, "DTSTART", 0)
	if !ok {
		return []recurrenceInstance{{}}, true, true
	}
	window := m.occurrenceWindow(master, dtstart)
	scanStart, scanEnd := start, end
	if lead := m.alarmScanLead(node, window); lead > 0 {
		scanStart, scanEnd = start.Add(-lead), end.Add(lead)
	}
	// Inclusive candidate bounds can add an instance at each endpoint that the
	// component-specific test excludes. The output budget is checked after that test.
	generated, err := ical.RecurrenceInstances(m.raw, master.name, dtstart.instant, window,
		scanStart, scanEnd, ical.MaxRecurrenceInstances+2, m.resolveContentLine)
	// Exhausting the budget against an open end is not a failure. No budget
	// covers an infinity, so refusing would fail every report a client scopes
	// that way rather than the one resource -- and the prefix settles the match
	// without the rest of the set, since an instance had to reach the range to
	// be generated at all. A spelled end asks a finite question the server
	// undertook to answer exactly, so there the budget still refuses.
	truncated := errors.Is(err, ical.ErrRecurrenceExpansionLimit)
	if err != nil && !truncated {
		*m.expansionError = err
		return nil, false, false
	}
	if truncated && !openTimeRangeEnd(end) {
		*m.expansionError = err
		return nil, false, false
	}
	instances := make([]recurrenceInstance, 0, len(generated))
	for _, instance := range generated {
		instances = append(instances, recurrenceInstance{
			shift:     instance.Start.Sub(dtstart.instant),
			slotShift: instance.RecurrenceID.Sub(dtstart.instant),
			duration:  instance.End.Sub(instance.Start),
		})
	}
	// A truncated prefix reaching here came from an open end, so it settles a
	// match it contains. Whether it settles the absence of one is the caller's
	// question, and truncated is what tells it apart.
	return instances, true, !truncated
}

// alarmScanLead is how far outside the requested range an instance can start
// while still firing an alarm inside it, so the instance scan can be widened by
// it. Only a VALARM has any: a relative TRIGGER fires away from the occurrence
// it belongs to, RELATED=END anchors past the end of that occurrence, and every
// REPEAT carries it further still. The widest reading is taken here and the
// exact §9.9 condition judges each candidate afterwards.
func (m calendarTimeRangeMatcher) alarmScanLead(node *icalNode, window time.Duration) time.Duration {
	if node.name != "VALARM" {
		return 0
	}
	// Nothing storable lies outside the CALDAV:min-date-time to
	// CALDAV:max-date-time span, so a lead wider than that span cannot reach an
	// instance that exists. The cap is also what keeps the arithmetic honest: a
	// DURATION near the time.Duration ceiling, repeated, would otherwise wrap to
	// a negative lead and narrow the scan instead of widening it.
	minDate, maxDate := ical.DateLimits()
	limit := maxDate.Sub(minDate)

	lead := addScanLead(0, window, limit)
	if property, ok := firstICalProperty(node, "TRIGGER"); ok {
		if trigger, ok := ical.ParseDuration(strings.TrimSpace(property.value)); ok {
			if trigger < 0 {
				trigger = -trigger
			}
			lead = addScanLead(lead, trigger, limit)
		}
	}
	if repeat, every, ok := alarmRepeats(node); ok && repeat > 0 && every > 0 {
		if every >= limit/time.Duration(repeat) {
			return limit
		}
		lead = addScanLead(lead, time.Duration(repeat)*every, limit)
	}
	return lead
}

// addScanLead sums two scan-widening durations, saturating at limit rather than
// wrapping past the time.Duration ceiling.
func addScanLead(lead, add, limit time.Duration) time.Duration {
	if add <= 0 || lead >= limit {
		return lead
	}
	if add >= limit || lead > limit-add {
		return limit
	}
	return lead + add
}

func (m calendarTimeRangeMatcher) hasRecurrenceSet(node *icalNode) bool {
	return recurringTimeRangeComponents.contains(node.name) &&
		(node.count("RRULE") > 0 || node.count("RDATE") > 0)
}

// occurrenceWindow is how long one occurrence of node lasts, used to bound the
// recurrence scan. It is deliberately the widest reading each component admits,
// because the exact §9.9 condition is applied to every candidate afterwards.
func (m calendarTimeRangeMatcher) occurrenceWindow(node *icalNode, dtstart icalTimeValue) time.Duration {
	switch node.name {
	case "VEVENT":
		if dtend, ok := m.dateValue(node, "DTEND", 0); ok {
			if d := dtend.instant.Sub(dtstart.instant); d > 0 {
				return d
			}
		}
	case "VTODO":
		if due, ok := m.dateValue(node, "DUE", 0); ok {
			if d := due.instant.Sub(dtstart.instant); d > 0 {
				return d
			}
		}
	}
	if d, ok := componentDuration(node, "DURATION"); ok && d > 0 {
		return d
	}
	if dtstart.isDate {
		return m.zone.addDays(dtstart.instant, 1).Sub(dtstart.instant)
	}
	return 0
}

func (m calendarTimeRangeMatcher) componentInTimeRange(node, parent *icalNode, start, end time.Time, shift time.Duration) bool {
	switch node.name {
	case "VEVENT":
		return m.eventInTimeRange(node, start, end, shift)
	case "VTODO":
		return m.todoInTimeRange(node, start, end, shift)
	case "VJOURNAL":
		return m.journalInTimeRange(node, start, end, shift)
	case "VFREEBUSY":
		return m.freeBusyInTimeRange(node, start, end)
	case "VALARM":
		return m.alarmInTimeRange(node, parent, start, end, shift)
	default:
		return false
	}
}

// eventInTimeRange implements the §9.9 VEVENT table. RFC 5545 makes DTSTART
// required, so a VEVENT without one cannot be placed on a timeline at all.
func (m calendarTimeRangeMatcher) eventInTimeRange(node *icalNode, start, end time.Time, shift time.Duration) bool {
	dtstart, ok := m.dateValue(node, "DTSTART", shift)
	if !ok {
		return false
	}
	if dtend, ok := m.dateValue(node, "DTEND", shift); ok {
		return start.Before(dtend.instant) && end.After(dtstart.instant)
	}
	if duration, ok := componentDuration(node, "DURATION"); ok {
		if duration > 0 {
			return start.Before(dtstart.instant.Add(duration)) && end.After(dtstart.instant)
		}
		return !start.After(dtstart.instant) && end.After(dtstart.instant)
	}
	if !dtstart.isDate {
		return !start.After(dtstart.instant) && end.After(dtstart.instant)
	}
	return start.Before(m.zone.addDays(dtstart.instant, 1)) && end.After(dtstart.instant)
}

// todoInTimeRange implements the eight-row §9.9 VTODO table, whose last row
// makes a VTODO carrying none of the five properties match every range.
func (m calendarTimeRangeMatcher) todoInTimeRange(node *icalNode, start, end time.Time, shift time.Duration) bool {
	dtstart, hasStart := m.dateValue(node, "DTSTART", shift)
	duration, hasDuration := componentDuration(node, "DURATION")
	due, hasDue := m.dateValue(node, "DUE", shift)

	if hasStart && hasDuration && !hasDue {
		effectiveDue := dtstart.instant.Add(duration)
		return !start.After(effectiveDue) && (end.After(dtstart.instant) || !end.Before(effectiveDue))
	}
	if hasStart && !hasDuration && hasDue {
		return (start.Before(due.instant) || !start.After(dtstart.instant)) &&
			(end.After(dtstart.instant) || !end.Before(due.instant))
	}
	if hasStart && !hasDuration && !hasDue {
		return !start.After(dtstart.instant) && end.After(dtstart.instant)
	}
	if !hasStart && !hasDuration && hasDue {
		return start.Before(due.instant) && !end.Before(due.instant)
	}

	completed, hasCompleted := m.dateValue(node, "COMPLETED", 0)
	created, hasCreated := m.dateValue(node, "CREATED", 0)
	switch {
	case hasCompleted && hasCreated:
		return (!start.After(created.instant) || !start.After(completed.instant)) &&
			(!end.Before(created.instant) || !end.Before(completed.instant))
	case hasCompleted:
		return !start.After(completed.instant) && !end.Before(completed.instant)
	case hasCreated:
		return end.After(created.instant)
	default:
		return true
	}
}

// journalInTimeRange implements the three-row §9.9 VJOURNAL table.
func (m calendarTimeRangeMatcher) journalInTimeRange(node *icalNode, start, end time.Time, shift time.Duration) bool {
	dtstart, ok := m.dateValue(node, "DTSTART", shift)
	if !ok {
		return false
	}
	if dtstart.isDate {
		return start.Before(m.zone.addDays(dtstart.instant, 1)) && end.After(dtstart.instant)
	}
	return !start.After(dtstart.instant) && end.After(dtstart.instant)
}

// freeBusyInTimeRange implements the three-row §9.9 VFREEBUSY table. A DURATION
// is ignored here, as §9.9 says outright, because it carries a different meaning
// inside a VFREEBUSY.
func (m calendarTimeRangeMatcher) freeBusyInTimeRange(node *icalNode, start, end time.Time) bool {
	dtstart, hasStart := m.dateValue(node, "DTSTART", 0)
	dtend, hasEnd := m.dateValue(node, "DTEND", 0)
	if hasStart && hasEnd {
		return !start.After(dtend.instant) && end.After(dtstart.instant)
	}
	for _, property := range node.properties {
		if property.name != "FREEBUSY" {
			continue
		}
		for _, period := range strings.Split(property.value, ",") {
			periodStart, periodEnd, ok := m.freeBusyPeriod(property, strings.TrimSpace(period))
			if !ok {
				continue
			}
			if start.Before(periodEnd) && end.After(periodStart) {
				return true
			}
		}
	}
	return false
}

// freeBusyPeriod reads one RFC 5545 §3.3.9 PERIOD value, in either its
// explicit-end or its start-plus-duration form.
func (m calendarTimeRangeMatcher) freeBusyPeriod(property icalProperty, value string) (time.Time, time.Time, bool) {
	rawStart, rawEnd, found := strings.Cut(value, "/")
	if !found {
		return time.Time{}, time.Time{}, false
	}
	form, ok := parseICalDateForm(strings.TrimSpace(rawStart))
	if !ok {
		return time.Time{}, time.Time{}, false
	}
	start, ok := m.resolveInstant(property.parameters["TZID"], strings.TrimSpace(rawStart), form)
	if !ok {
		return time.Time{}, time.Time{}, false
	}
	rawEnd = strings.TrimSpace(rawEnd)
	if strings.HasPrefix(strings.ToUpper(rawEnd), "P") || strings.HasPrefix(strings.ToUpper(rawEnd), "-P") {
		duration, ok := ical.ParseDuration(rawEnd)
		if !ok {
			return time.Time{}, time.Time{}, false
		}
		return start, start.Add(duration), true
	}
	endForm, ok := parseICalDateForm(rawEnd)
	if !ok {
		return time.Time{}, time.Time{}, false
	}
	end, ok := m.resolveInstant(property.parameters["TZID"], rawEnd, endForm)
	if !ok {
		return time.Time{}, time.Time{}, false
	}
	return start, end, true
}

// alarmInTimeRange applies the §9.9 VALARM condition to every trigger the alarm
// fires, which is the initial one plus each REPEAT.
func (m calendarTimeRangeMatcher) alarmInTimeRange(node, parent *icalNode, start, end time.Time, shift time.Duration) bool {
	trigger, ok := m.alarmTriggerTime(node, parent, shift)
	if !ok {
		return false
	}
	repeat, repeatEvery, ok := alarmRepeats(node)
	if !ok {
		return false
	}
	for i := 0; i <= repeat; i++ {
		fires := trigger.Add(time.Duration(i) * repeatEvery)
		if !start.After(fires) && end.After(fires) {
			return true
		}
	}
	return false
}

// alarmRepeats is how many further times an alarm fires after its initial
// trigger, and how far apart. RFC 5545 §3.8.5.3 pairs REPEAT with DURATION, so
// one without the other fires once. ok is false for a REPEAT value that is no
// count at all, which is a malformed alarm rather than one that never fires.
func alarmRepeats(node *icalNode) (int, time.Duration, bool) {
	repeat := 0
	if value := strings.TrimSpace(node.value("REPEAT")); value != "" {
		parsed, err := strconv.Atoi(value)
		if err != nil || parsed < 0 {
			return 0, 0, false
		}
		repeat = parsed
	}
	every, hasEvery := componentDuration(node, "DURATION")
	if repeat > 0 && (!hasEvery || every <= 0) {
		repeat = 0
	}
	if repeat > ical.MaxRecurrenceInstances {
		repeat = ical.MaxRecurrenceInstances
	}
	return repeat, every, true
}

// hasAbsoluteAlarmTrigger reports whether node is a VALARM whose TRIGGER names
// an instant outright rather than a duration relative to the enclosing
// component. RFC 5545 §3.8.6.3 admits both forms and the duration is the
// default, so the value itself is what distinguishes them.
func hasAbsoluteAlarmTrigger(node *icalNode) bool {
	if node.name != "VALARM" {
		return false
	}
	property, ok := firstICalProperty(node, "TRIGGER")
	if !ok {
		return false
	}
	value := strings.TrimSpace(property.value)
	if _, ok := ical.ParseDuration(value); ok {
		return false
	}
	_, ok = parseICalDateForm(value)
	return ok
}

// alarmTriggerTime resolves TRIGGER in both RFC 5545 §3.8.6.3 forms: an
// absolute DATE-TIME, or a duration relative to the enclosing component's start
// or end. RELATED defaults to START.
func (m calendarTimeRangeMatcher) alarmTriggerTime(node, parent *icalNode, shift time.Duration) (time.Time, bool) {
	property, ok := firstICalProperty(node, "TRIGGER")
	if !ok {
		return time.Time{}, false
	}
	value := strings.TrimSpace(property.value)
	if duration, ok := ical.ParseDuration(value); ok {
		if parent == nil {
			return time.Time{}, false
		}
		anchor, ok := m.alarmAnchor(parent, property, shift)
		if !ok {
			return time.Time{}, false
		}
		return anchor.Add(duration), true
	}
	form, ok := parseICalDateForm(value)
	if !ok {
		return time.Time{}, false
	}
	return m.resolveInstant(property.parameters["TZID"], value, form)
}

func (m calendarTimeRangeMatcher) alarmAnchor(parent *icalNode, trigger icalProperty, shift time.Duration) (time.Time, bool) {
	if !strings.EqualFold(strings.TrimSpace(trigger.parameters["RELATED"]), "END") {
		dtstart, ok := m.dateValue(parent, "DTSTART", shift)
		return dtstart.instant, ok
	}
	if dtend, ok := m.dateValue(parent, "DTEND", shift); ok {
		return dtend.instant, true
	}
	if due, ok := m.dateValue(parent, "DUE", shift); ok {
		return due.instant, true
	}
	dtstart, ok := m.dateValue(parent, "DTSTART", shift)
	if !ok {
		return time.Time{}, false
	}
	if duration, ok := componentDuration(parent, "DURATION"); ok {
		return dtstart.instant.Add(duration), true
	}
	return dtstart.instant, true
}

// recurrenceShiftedProperties are the date-valued properties §9.9 requires a
// server to infer an effective value for on each recurrence instance. The rest
// of the properties its table names describe the component as a whole and read
// the same for every instance of it.
var recurrenceShiftedProperties = nameSet("DTSTART", "DTEND", "DUE")

// propertyInTimeRange applies the §9.9 property-level overlap. The seven
// properties it names are the only ones the test is defined for; a time-range
// on any other property matches nothing. node holds the property and parent
// holds node, which together say whose recurrence set the value moves with.
func (m calendarTimeRangeMatcher) propertyInTimeRange(property icalProperty, node, parent *icalNode, start, end time.Time) bool {
	if !calendarTimeRangeProperties.contains(property.name) {
		return false
	}
	value := strings.TrimSpace(property.value)
	form, ok := parseICalDateForm(value)
	if !ok {
		return false
	}
	instant, ok := m.resolveInstant(property.parameters["TZID"], value, form)
	if !ok {
		return false
	}
	if !recurrenceShiftedProperties.contains(property.name) {
		return !start.After(instant) && end.After(instant)
	}
	return m.shiftedInstantInTimeRange(instant, property.name, node, parent, start, end)
}

// inferredTimeRangeProperties are the two properties §9.9 directs the
// property-level test at an inferred value for when the component carries no
// such property: a VEVENT's DTEND and a VTODO's DUE both become
// DTSTART+DURATION.
var inferredTimeRangeProperties = map[string]string{
	"VEVENT": "DTEND",
	"VTODO":  "DUE",
}

// inferredPropertyInTimeRange applies the §9.9 property-level overlap to the
// effective DTEND or DUE of a component that spells a DURATION instead. It
// reports false for any other property, and for a component whose own
// properties already answer, which the caller has tested first.
func (m calendarTimeRangeMatcher) inferredPropertyInTimeRange(name string, node, parent *icalNode, start, end time.Time) bool {
	if inferredTimeRangeProperties[node.name] != strings.ToUpper(name) {
		return false
	}
	dtstart, ok := m.dateValue(node, "DTSTART", 0)
	if !ok {
		return false
	}
	duration, ok := componentDuration(node, "DURATION")
	if !ok {
		return false
	}
	return m.shiftedInstantInTimeRange(dtstart.instant.Add(duration), name, node, parent, start, end)
}

// shiftedInstantInTimeRange applies `start <= value AND end > value` to the
// value as each generated recurrence instance carries it, since §9.9 requires
// every instance to be considered and makes one match enough.
func (m calendarTimeRangeMatcher) shiftedInstantInTimeRange(instant time.Time, name string, node, parent *icalNode, start, end time.Time) bool {
	master := m.recurrenceMaster(node, parent)
	instances, expandable, settled := m.recurrenceInstances(node, master, start, end)
	if !expandable {
		return true
	}
	for _, instance := range instances {
		effective := instant.Add(instance.shift)
		if master == node {
			occurrence := expandedInstance(m, m.root, master, instance)
			if value, ok := m.dateValue(occurrence, name, 0); ok {
				effective = value.instant
			} else if name == "DTEND" || name == "DUE" {
				if start, ok := m.dateValue(occurrence, "DTSTART", 0); ok {
					effective = start.instant.Add(instance.duration)
				}
			}
		}
		if !start.After(effective) && end.After(effective) {
			return true
		}
	}
	return !settled
}
