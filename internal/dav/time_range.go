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
// The two are never both in force, so whichever is set is the zone. defined
// records that a VTIMEZONE was supplied at all, which §7.3 selects even when
// neither source can resolve through it and its values are read as UTC.
type floatingZone struct {
	defined     bool
	loc         *time.Location
	observances *ical.IndexedTimezoneObservances
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
		if observances, ok := submittedTimezoneObservances(root, tzid); ok {
			return floatingZone{defined: true, observances: observances}
		}
		// Nothing walkable was submitted. The host's database is the only
		// remaining source of rules for this name.
		if loc, ok := ical.HostLocation(tzid); ok {
			return floatingZone{defined: true, loc: loc}
		}
		return floatingZone{defined: true}
	}
	return floatingZone{}
}

// reportFloatingZone selects the zone a calendaring REPORT resolves floating
// values against, which RFC 4791 §7.3 orders: the CALDAV:timezone the request
// carries, else the CALDAV:calendar-timezone the targeted collection defines,
// else UTC. requestTimezone is empty for free-busy, which has no such element.
func reportFloatingZone(requestTimezone string, collectionTimezone *string) floatingZone {
	zone := newFloatingZone(requestTimezone)
	if !zone.defined && collectionTimezone != nil {
		zone = newFloatingZone(wrapCalendarTimezone(*collectionTimezone))
	}
	return zone
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
	if offset, ok := observanceOffsetAtWall(z.observances, wall); ok {
		return wall.Add(-offset).UTC(), true
	}
	return wall, true
}

// resolveWall is resolve for a floating wall clock already read, held as the
// UTC time with the same fields.
func (z floatingZone) resolveWall(wall time.Time) time.Time {
	if z.loc != nil {
		if z.loc == time.UTC {
			return wall
		}
		return ical.WallClockIn(z.loc, wall).UTC()
	}
	if offset, ok := observanceOffsetAtWall(z.observances, wall); ok {
		return wall.Add(-offset).UTC()
	}
	return wall
}

// wallClock renders an instant as the wall-clock reading in the zone, which is
// the spelling a DATE or floating value is written back out with.
func (z floatingZone) wallClock(instant time.Time) time.Time {
	if z.loc != nil {
		return instant.In(z.loc)
	}
	if offset, ok := observanceOffsetAtInstant(z.observances, instant); ok {
		return instant.Add(offset).UTC()
	}
	return instant.UTC()
}

// icalTimeValue is one resolved date-valued property: the instant it names and
// whether it was written as a DATE, which several §9.9 rows switch on.
type icalTimeValue struct {
	instant time.Time
	isDate  bool
	// date is the date a DATE value names, as its UTC midnight; tzid the
	// zone its property names, which resolves it.
	date time.Time
	tzid string
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
	// Copies made during the component walk share expansion failures and the
	// per-resource indexes, whose maps are filled on first use.
	expansionError *error
	indexes        *matcherIndexes
	raw            string
	root           *icalNode
	zone           floatingZone
	// pool, when set, is the request-wide recurrence and observance budget
	// every resource this matcher evaluates draws on in addition to its own
	// per-evaluation cap.
	pool *ical.RequestWork
}

// matcherIndexes holds what a matcher derives from its resource once and
// consults per instance. Each map is allocated on first use, so a resource that
// recurs through no override and names no zone pays for none of them.
type matcherIndexes struct {
	overrides map[recurrenceOverrideIndexKey]recurrenceOverrideIndex
	masters   map[*icalNode]map[string]*icalNode
	// series holds each recurrence set expanded over a range, with what the
	// expansion found of its RANGE=THISANDFUTURE overrides.
	series map[seriesRangeKey]*seriesExpansion
	// observances is the definition the resource ships for each TZID asked
	// about, nil for one it ships none of. A matcher serves one request on
	// one goroutine, so it needs none of the locking the parsed tree's own
	// index takes.
	observances map[string]*ical.IndexedTimezoneObservances
}

func newCalendarTimeRangeMatcher(raw string, root *icalNode, zone floatingZone) calendarTimeRangeMatcher {
	return calendarTimeRangeMatcher{
		raw: raw, root: root, zone: zone,
		expansionError: new(error),
		indexes:        &matcherIndexes{},
	}
}

// withPool returns m sharing every other field, with pool as the request-wide
// budget every resource it evaluates draws on besides its own per-evaluation
// cap, and the tree's zones are read on it as well. A copy made during the
// component walk carries it on unchanged. It is called before m's tree is
// first read.
func (m calendarTimeRangeMatcher) withPool(pool *ical.RequestWork) calendarTimeRangeMatcher {
	m.pool = pool
	if m.root != nil && pool != nil {
		m.root.drawOn(pool)
	}
	return m
}

// instantShift is the distance between two instants, carried as whole seconds
// and nanoseconds rather than as a time.Duration. RFC 5545 §3.3.4 admits any
// four-digit year, so a recurrence instance can lie further from its DTSTART
// than a time.Duration's ~292 years can measure, and a saturated shift would
// place the instance centuries from where it falls.
type instantShift struct {
	seconds     int64
	nanoseconds int64
}

// noShift reads a component at the dates it was written with.
var noShift = instantShift{}

// shiftBetween is the shift that moves from onto to.
func shiftBetween(from, to time.Time) instantShift {
	return instantShift{seconds: to.Unix() - from.Unix(), nanoseconds: int64(to.Nanosecond() - from.Nanosecond())}
}

// durationShift is a shift no longer than a time.Duration, such as the length
// of one occurrence.
func durationShift(d time.Duration) instantShift {
	return instantShift{seconds: int64(d / time.Second), nanoseconds: int64(d % time.Second)}
}

// apply moves t by the shift, keeping t's location.
func (s instantShift) apply(t time.Time) time.Time {
	if s == noShift {
		return t
	}
	return time.Unix(t.Unix()+s.seconds, int64(t.Nanosecond())+s.nanoseconds).In(t.Location())
}

func (s instantShift) plus(other instantShift) instantShift {
	return instantShift{seconds: s.seconds + other.seconds, nanoseconds: s.nanoseconds + other.nanoseconds}
}

// dateValue resolves one date-valued property of node, shifting it by shift so
// a generated recurrence instance can be tested against the master's property
// set. §9.9 requires the effective DTSTART, DTEND, DURATION and DUE of an
// instance to be inferred; the other date properties are metadata of the
// component as a whole and are read unshifted.
func (m calendarTimeRangeMatcher) dateValue(node *icalNode, name string, shift instantShift) (icalTimeValue, bool) {
	property, ok := firstICalProperty(node, name)
	if !ok {
		return icalTimeValue{}, false
	}
	value := strings.TrimSpace(property.value)
	form, ok := parseICalDateForm(value)
	if !ok {
		return icalTimeValue{}, false
	}
	tzid := strings.TrimSpace(property.parameters["TZID"])
	instant, ok := m.resolveInstant(tzid, value, form)
	if !ok {
		return icalTimeValue{}, false
	}
	resolved := icalTimeValue{instant: shift.apply(instant), isDate: form == icalDateOnly, tzid: tzid}
	if resolved.isDate {
		resolved.date, _ = ical.DateOf(value)
		if shift != noShift {
			resolved.date = m.LocalDate(tzid, resolved.instant)
		}
	}
	return resolved, true
}

// dateMidnight is the instant date begins where value, a DATE, is read: the
// midnight a DATE naming date resolves to in the zone value's TZID names, else
// the matcher's, as RFC 4791 §9.9 places one. An all-day occurrence spans the
// dates from its own to the one its length counts on, each placed here,
// however many hours they hold -- 23 or 25 where the zone changes offset, none
// for a date it skips -- and wherever the zone's day begins on a date whose
// clocks change at midnight.
func (m calendarTimeRangeMatcher) dateMidnight(value icalTimeValue, date time.Time) time.Time {
	return m.midnight(value.tzid, date)
}

// DateMidnight is the instant date begins where a DATE property carrying the
// TZID tzid is read, as ResolvePropertyTime reads one naming date. It is the
// ical.DateMidnightResolver a recurrence expansion places the days of an
// all-day series through, once for each date it reaches.
func (m calendarTimeRangeMatcher) DateMidnight(tzid string, date time.Time) time.Time {
	return m.midnight(tzid, date)
}

// midnight is resolveInstant for a DATE naming date in the zone tzid names,
// read from its fields rather than its spelling.
func (m calendarTimeRangeMatcher) midnight(tzid string, date time.Time) time.Time {
	if tzid = strings.TrimSpace(tzid); tzid != "" {
		if instant, ok := zonedWallInstant(m.timezoneObservances(tzid), tzid, date); ok {
			return instant
		}
	}
	return m.zone.resolveWall(date)
}

// LocalDate is the date instant falls on, as its UTC midnight: its wall
// clock's date in the zone resolveInstant reads the TZID tzid through -- the
// definition the resource ships, else the host's -- and in the matcher's zone
// for an empty tzid or one neither defines. It is the ical.LocalDateReader a
// recurrence expansion dates its slots by.
func (m calendarTimeRangeMatcher) LocalDate(tzid string, instant time.Time) time.Time {
	if tzid = strings.TrimSpace(tzid); tzid != "" {
		if offset, ok := observanceOffsetAtInstant(m.timezoneObservances(tzid), instant); ok {
			return ical.CivilDate(instant.Add(offset).UTC())
		}
		if loc, ok := ical.HostLocation(tzid); ok {
			return ical.CivilDate(instant.In(loc))
		}
	}
	return ical.CivilDate(m.zone.wallClock(instant))
}

func (m calendarTimeRangeMatcher) resolveInstant(tzid, value string, form icalDateForm) (time.Time, bool) {
	if form == icalUTCDateTime {
		parsed, err := ical.ParseDateTime(value)
		return parsed, err == nil
	}
	if tzid = strings.TrimSpace(tzid); tzid != "" {
		// RFC 4791 §4.1 requires the resource to carry a VTIMEZONE for every
		// TZID it uses, so that definition is authoritative over the host's,
		// unless the server generated it from the host's
		// (hostGeneratedTimezone).
		if wall, err := ical.ParseDateTime(value); err == nil {
			if instant, ok := zonedWallInstant(m.timezoneObservances(tzid), tzid, wall); ok {
				return instant, true
			}
		}
	}
	return m.zone.resolve(value)
}

// timezoneObservances is the definition the resource ships for tzid, read once
// per resource however many of its values name it, or nil when it ships none
// that can be read.
func (m calendarTimeRangeMatcher) timezoneObservances(tzid string) *ical.IndexedTimezoneObservances {
	if m.indexes == nil {
		observances, _ := submittedTimezoneObservances(m.root, tzid)
		return observances
	}
	if observances, read := m.indexes.observances[tzid]; read {
		return observances
	}
	observances, _ := submittedTimezoneObservances(m.root, tzid)
	if m.indexes.observances == nil {
		m.indexes.observances = make(map[string]*ical.IndexedTimezoneObservances)
	}
	m.indexes.observances[tzid] = observances
	return observances
}

// The matcher is the ical.RecurrenceTimeResolver its recurrence expansions read
// their dates through, so one zone answers for the whole recurrence set.
var _ ical.RecurrenceTimeResolver = calendarTimeRangeMatcher{}

// ResolvePropertyTime answers one date-valued content line for the recurrence
// expansion, which reaches EXDATE, RDATE, RECURRENCE-ID and a floating UNTIL --
// the dates a component's DTSTART has to agree with. Handing the expansion this
// rather than letting it read them itself is what keeps one zone answering for
// the whole recurrence set.
func (m calendarTimeRangeMatcher) ResolvePropertyTime(keyPart, value string) (time.Time, bool) {
	value = strings.TrimSpace(value)
	form, ok := parseICalDateForm(value)
	if !ok {
		return time.Time{}, false
	}
	tzid, _ := ical.PropertyParam(keyPart, "TZID")
	return m.resolveInstant(tzid, value, form)
}

// LocalTimeExists reports whether wall is a local time the zone tzid names has.
// It asks the definition resolveInstant reads the TZID through, in the same
// order: the VTIMEZONE the resource ships, else the host's zone of that name. A
// gap the shipped definition cannot be followed to is not assumed: the wall
// clock is reported to exist. A TZID neither defines is read in the RFC 4791
// §7.3 zone, which is no zone the object defines, so it skips nothing.
func (m calendarTimeRangeMatcher) LocalTimeExists(tzid string, wall time.Time) bool {
	tzid = strings.TrimSpace(tzid)
	if observances := m.timezoneObservances(tzid); observances != nil {
		skipped, known := observances.Skips(wall, sharedTimezoneOnsets)
		return !known || !skipped
	}
	if loc, ok := ical.HostLocation(tzid); ok {
		return ical.WallClockExists(loc, wall)
	}
	return true
}

var _ ical.LocalTimeGapLister = calendarTimeRangeMatcher{}

// LocalTimeGaps lists where the zone tzid names skips local times, from the
// definition LocalTimeExists answers from.
func (m calendarTimeRangeMatcher) LocalTimeGaps(tzid string, after, through time.Time) ([]ical.LocalTimeGap, bool) {
	tzid = strings.TrimSpace(tzid)
	observances := m.timezoneObservances(tzid)
	if observances == nil {
		return ical.PropertyTimeResolver(nil).LocalTimeGaps(tzid, after, through)
	}
	return observances.Gaps(after, through, sharedTimezoneOnsets)
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
		return m.alarmInTimeRange(node, parent, start, end, noShift)
	}
	master := m.recurrenceMaster(node, parent)
	matched := false
	expandable, settled := m.visitRecurrenceInstances(node, master, start, end, func(instance recurrenceInstance) bool {
		content, enclosing := node, parent
		shift := instance.shift
		if master != nil {
			occurrence := expandedInstance(m, m.root, master, instance)
			if node == master {
				content = occurrence
			} else {
				enclosing = occurrence
			}
			shift = noShift
		}
		matched = m.componentInTimeRange(content, enclosing, start, end, shift)
		return matched
	})
	if !expandable {
		// A frequency this server cannot expand is kept rather than filtered
		// out: reporting nothing would hide a resource that does occur in the
		// range.
		return true
	}
	// No instance visited matched, which settles the question only when they
	// were the whole set. Otherwise the resource is kept on the same ground
	// an inexpressible frequency is.
	return matched || !settled
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
// occurrence (RFC 5545 §3.8.4.4). Where either starts on a DATE, it is named
// by its date instead, as ical.RecurrenceInstance has it: slotDate is the date
// a slot of a DATE series names, and startDate and days the date an
// occurrence of DATE content starts on and the days it spans.
type recurrenceInstance struct {
	shift     instantShift
	slotShift instantShift
	duration  time.Duration
	slotDate  time.Time
	startDate time.Time
	days      int
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

// seriesRangeKey names one recurrence set expanded over one range.
type seriesRangeKey struct {
	master     *icalNode
	start, end time.Time
}

// seriesExpansion is every instance of a recurrence set over [start, end], as
// visitRecurrenceInstances reports them, for a caller that owes the whole set
// rather than one instance of it, and what the expansion found of each of the
// set's RANGE=THISANDFUTURE overrides. settled is false as well when the slot
// of an override the range places is left unanswered, since the set may then
// leave that override standing for itself, which a caller owing the whole set
// cannot rule out.
type seriesExpansion struct {
	start, end          time.Time
	instances           []recurrenceInstance
	expandable, settled bool
	thisAndFuture       map[*icalNode]*thisAndFutureSlot
}

// thisAndFutureSlot is what an expansion over a range found of one
// RANGE=THISANDFUTURE override: whether the set generates the slot its
// RECURRENCE-ID names, which carries the override, and, once read, whether
// the range places it. An answer of ical.RecurrenceSlotNotGenerated alone
// says the set carries no instance of it; every other answer is read as
// carried.
type thisAndFutureSlot struct {
	override          recurrenceOverride
	answer            ical.RecurrenceSlotAnswer
	placed, readPlace bool
}

// standsAlone reports whether override is a RANGE=THISANDFUTURE override of
// series that stands for itself in the range series was expanded over: the
// range places it, by the §9.9 test of its own component, and the set does
// not carry it. One naming a slot the set never generates -- a date its
// EXDATEs leave out, or one no period of the rule reaches -- is carried by no
// instance, and stands for itself as calendar-query and free-busy read it.
func (m calendarTimeRangeMatcher) standsAlone(root *icalNode, series *seriesExpansion, override *icalNode) bool {
	slot, ok := series.thisAndFuture[override]
	return ok && slot.answer == ical.RecurrenceSlotNotGenerated && m.slotPlaced(root, series, slot)
}

// slotPlaced reports whether the §9.9 test of the override's own component
// places it in the range series was expanded over, reading it once; one of a
// component §9.9 defines no test for is placed in every range. A VEVENT
// starting at or after the range's end is passed over before its end is read.
func (m calendarTimeRangeMatcher) slotPlaced(root *icalNode, series *seriesExpansion, slot *thisAndFutureSlot) bool {
	if slot.readPlace {
		return slot.placed
	}
	slot.readPlace = true
	start, end := series.start, series.end
	switch override := slot.override; {
	case !calendarTimeRangeComponents.contains(override.node.name):
		slot.placed = true
	case override.node.name != "VEVENT":
		slot.placed = m.componentInTimeRange(override.node, root, start, end, noShift)
	default:
		if dtstart, ok := overrideStart(m, override); ok && dtstart.instant.Before(end) {
			slot.placed = periodInTimeRange(dtstart.instant, m.eventOccurrenceEnd(override.node, dtstart, noShift), start, end)
		}
	}
	return slot.placed
}

// expandSeries is master's recurrence set, root's master of its type,
// expanded over [start, end], once per matcher however many components ask.
// The expansion's walk is widened by as far as the RANGE=THISANDFUTURE
// overrides move their instances, so it passes the slot every override it
// places in the range names and answers whether the set carries it. One it
// does not pass is looked up, on the walk's work budget, only when the range
// places it, the one case a caller writing it out has to know; the slot of
// one the walk passes is matched without the override's own dates being read.
func (m calendarTimeRangeMatcher) expandSeries(root, master *icalNode, start, end time.Time) *seriesExpansion {
	key := seriesRangeKey{master: master, start: start, end: end}
	if m.indexes != nil {
		if expanded, ok := m.indexes.series[key]; ok {
			return expanded
		}
	}
	// The override index reads every override's RECURRENCE-ID, which a set
	// carrying none of RANGE=THISANDFUTURE has no question to spend on.
	var overrides []recurrenceOverride
	for _, child := range root.children {
		if child.name == master.name && thisAndFutureOverride(child) {
			overrides = m.overrideIndex(root, master).thisAndFuture
			break
		}
	}
	expanded := &seriesExpansion{start: start, end: end, thisAndFuture: make(map[*icalNode]*thisAndFutureSlot, len(overrides))}
	slots := make([]*thisAndFutureSlot, len(overrides))
	ids := make([]ical.RecurrenceSlotID, len(overrides))
	for i, override := range overrides {
		slots[i] = &thisAndFutureSlot{override: override}
		ids[i] = ical.RecurrenceSlotID{Instant: override.recurrenceID, Date: override.date}
		if _, seen := expanded.thisAndFuture[override.node]; !seen {
			expanded.thisAndFuture[override.node] = slots[i]
		}
	}
	placed := func(i int) bool { return m.slotPlaced(root, expanded, slots[i]) }
	var answers []ical.RecurrenceSlotAnswer
	answers, expanded.expandable, expanded.settled = m.visitRecurrenceSet(master, master, start, end, ids, placed, func(instance recurrenceInstance) bool {
		expanded.instances = append(expanded.instances, instance)
		return false
	})
	if !expanded.expandable {
		expanded.instances, expanded.settled = nil, false
	}
	for i, answer := range answers {
		slots[i].answer = answer
		if answer == ical.RecurrenceSlotUnanswered {
			expanded.settled = false
		}
	}
	if m.indexes != nil {
		if m.indexes.series == nil {
			m.indexes.series = make(map[seriesRangeKey]*seriesExpansion)
		}
		m.indexes.series[key] = expanded
	}
	return expanded
}

// visitRecurrenceInstances hands visit each instance of the recurrence set that
// reaches the range, carrying its effective duration and original identity, and
// stops as soon as visit returns true. expandable is false when nothing here can
// be enumerated at all. settled is false when the instances visited are only
// part of the set and none of them ended the visit, which a caller deciding a
// §9.9 match has to read as "no answer" rather than "no match".
func (m calendarTimeRangeMatcher) visitRecurrenceInstances(node, master *icalNode, start, end time.Time, visit func(recurrenceInstance) bool) (expandable, settled bool) {
	_, expandable, settled = m.visitRecurrenceSet(node, master, start, end, nil, nil, visit)
	return expandable, settled
}

// visitRecurrenceSet is visitRecurrenceInstances, answering as well, per
// ical.VisitRecurrenceInstancesAndSlots, whether master's recurrence set
// generates the slot each of ids names, looking up one its walk does not pass
// only when needed has it. A set this server cannot enumerate is read as the
// expansion reads one, as its DTSTART and RDATEs alone, and one without a
// start answers nothing, leaving answers short of ids.
func (m calendarTimeRangeMatcher) visitRecurrenceSet(node, master *icalNode, start, end time.Time, ids []ical.RecurrenceSlotID, needed func(int) bool, visit func(recurrenceInstance) bool) (answers []ical.RecurrenceSlotAnswer, expandable, settled bool) {
	if master == nil {
		visit(recurrenceInstance{})
		return nil, true, true
	}
	dtstart, ok := m.dateValue(master, "DTSTART", noShift)
	if !ical.SupportedRecurrenceRule(master.value("RRULE")) {
		if ok && len(ids) > 0 {
			answers, _ = ical.VisitRecurrenceInstancesAndSlots(m.raw, master.name, dtstart.instant, m.occurrenceWindow(master, dtstart),
				start, end, ical.MaxRecurrenceInstances+2, m, ids, needed, m.pool, func(ical.RecurrenceInstance) bool { return false })
		}
		return answers, false, false
	}
	if !ok {
		visit(recurrenceInstance{})
		return nil, true, true
	}
	window := m.occurrenceWindow(master, dtstart)
	scanStart, scanEnd := start, end
	if lead := m.alarmScanLead(node, window); lead > 0 {
		scanStart, scanEnd = start.Add(-lead), end.Add(lead)
	}
	// Inclusive candidate bounds can add an instance at each endpoint that the
	// component-specific test excludes. The output budget is checked after that test.
	answers, err := ical.VisitRecurrenceInstancesAndSlots(m.raw, master.name, dtstart.instant, window,
		scanStart, scanEnd, ical.MaxRecurrenceInstances+2, m, ids, needed, m.pool,
		func(instance ical.RecurrenceInstance) bool {
			return visit(recurrenceInstance{
				shift:     shiftBetween(dtstart.instant, instance.Start),
				slotShift: shiftBetween(dtstart.instant, instance.RecurrenceID),
				duration:  instance.End.Sub(instance.Start),
				slotDate:  instance.RecurrenceDate,
				startDate: instance.StartDate,
				days:      instance.Days,
			})
		})
	// Exhausting the budget is not a failure of the report. The instances
	// visited settle a match on their own, since an instance had to reach the
	// range to be generated at all, and their silence about the rest of the set
	// is reported as unsettled rather than as an error: the callers answer that
	// the way they answer a frequency this server cannot expand, by keeping the
	// resource. Refusing instead would lose every other resource of the
	// collection over one "repeats daily, no end date" event, and a range
	// holding more instances than one resource may store is an ordinary
	// request, not a malformed one.
	truncated := errors.Is(err, ical.ErrRecurrenceExpansionLimit)
	if err != nil && !truncated {
		*m.expansionError = err
		return answers, false, false
	}
	// A truncated set settles a match it contains. Whether it settles the
	// absence of one is the caller's question, and truncated is what tells it
	// apart: §9.9 needs only one instance, while the §9.6.5 expansion owes the
	// whole set and refuses the resource on the same signal.
	return answers, true, !truncated
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
// A VEVENT's is the extent eventOccurrenceEnd gives it.
//
// An occurrence starting on a DATE spans whole days, and its window is those
// days at 24 hours each: ical's recurrence expansion reads the count from it
// and spans each instance of a DATE series that many dates from its own. A
// VTODO counts them to the date of its DUE, else in the days its DURATION
// reaches into, as an event does.
func (m calendarTimeRangeMatcher) occurrenceWindow(node *icalNode, dtstart icalTimeValue) time.Duration {
	if dtstart.isDate {
		days := 1
		switch node.name {
		case "VEVENT":
			days = m.eventEnd(node, dtstart, noShift).Days()
		case "VTODO":
			if due, ok := m.dateValue(node, "DUE", noShift); ok && due.isDate && !due.date.Before(dtstart.date) {
				days = ical.CivilDaysBetween(dtstart.date, due.date)
			} else if d, ok := componentDuration(node, "DURATION"); ok && d >= 0 {
				days = ical.DurationDays(d)
			}
		}
		return time.Duration(days) * 24 * time.Hour
	}
	switch node.name {
	case "VEVENT":
		return m.eventOccurrenceEnd(node, dtstart, noShift).Sub(dtstart.instant)
	case "VTODO":
		if due, ok := m.dateValue(node, "DUE", noShift); ok {
			if d := due.instant.Sub(dtstart.instant); d >= 0 {
				return d
			}
		}
	}
	if d, ok := componentDuration(node, "DURATION"); ok && d >= 0 {
		return d
	}
	return 0
}
func (m calendarTimeRangeMatcher) componentInTimeRange(node, parent *icalNode, start, end time.Time, shift instantShift) bool {
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
func (m calendarTimeRangeMatcher) eventInTimeRange(node *icalNode, start, end time.Time, shift instantShift) bool {
	dtstart, ok := m.dateValue(node, "DTSTART", shift)
	if !ok {
		return false
	}
	return periodInTimeRange(dtstart.instant, m.eventOccurrenceEnd(node, dtstart, shift), start, end)
}

// eventEnd is what a VEVENT starting at dtstart writes about where it ends,
// its DTEND moved by shift, for ical.EventEnd to read. An override writing
// neither DTEND nor DURATION lasts as long as an occurrence of its series,
// read from the resource's master as the recurrence expansion reads it, when
// ical.EventEnd.InheritSeriesLength gives it that length.
func (m calendarTimeRangeMatcher) eventEnd(node *icalNode, dtstart icalTimeValue, shift instantShift) ical.EventEnd {
	extent := ical.EventEnd{Start: dtstart.instant, StartIsDate: dtstart.isDate, StartDate: dtstart.date}
	if dtend, ok := m.dateValue(node, "DTEND", shift); ok {
		extent.End, extent.HasEnd, extent.EndDate = dtend.instant, true, dtend.date
	}
	extent.Duration, extent.HasDuration = componentDuration(node, "DURATION")
	if node.name == "VEVENT" && node.count("RECURRENCE-ID") > 0 && node.count("DTEND") == 0 && node.count("DURATION") == 0 {
		if master := m.seriesMaster(node); master != nil {
			if start, ok := m.dateValue(master, "DTSTART", noShift); ok {
				extent.InheritSeriesLength(start.isDate, m.occurrenceWindow(master, start))
			}
		}
	}
	return extent
}

// seriesMaster is the component of the resource an override of node's type
// overrides an occurrence of: the first of that type with no RECURRENCE-ID.
func (m calendarTimeRangeMatcher) seriesMaster(node *icalNode) *icalNode {
	if m.root == nil {
		return nil
	}
	return m.recurrenceSetMaster(m.root, node)
}

// eventOccurrenceEnd is where an occurrence of a VEVENT starting at dtstart
// ends, by the ical.EventEnd rule every reader of an occurrence shares, with a
// DATE start's dates placed in the matcher's zone.
func (m calendarTimeRangeMatcher) eventOccurrenceEnd(node *icalNode, dtstart icalTimeValue, shift instantShift) time.Time {
	return m.eventEnd(node, dtstart, shift).OccurrenceEnd(func(date time.Time) time.Time {
		return m.dateMidnight(dtstart, date)
	})
}

// periodInTimeRange reports whether a period intersects [start, end). The
// DTEND row of the §9.9 VEVENT table (start < DTEND AND end > DTSTART) was
// written for events that occupy time, and read literally it would place a
// period ending where it starts nowhere a range can begin. Such a period is an
// instant, the one the table's DURATION = 0 row places with start <= DTSTART
// AND end > DTSTART, and it is matched by that row however its end is written.
func periodInTimeRange(periodStart, periodEnd, start, end time.Time) bool {
	if periodEnd.Equal(periodStart) {
		return !start.After(periodStart) && end.After(periodStart)
	}
	return start.Before(periodEnd) && end.After(periodStart)
}

// todoInTimeRange implements the eight-row §9.9 VTODO table, whose last row
// makes a VTODO carrying none of the five properties match every range.
func (m calendarTimeRangeMatcher) todoInTimeRange(node *icalNode, start, end time.Time, shift instantShift) bool {
	dtstart, hasStart := m.dateValue(node, "DTSTART", shift)
	duration, hasDuration := componentDuration(node, "DURATION")
	due, hasDue := m.dateValue(node, "DUE", shift)

	if hasStart && hasDuration && !hasDue {
		effectiveDue := dtstart.instant.Add(duration)
		if dtstart.isDate {
			// RFC 5545 §3.8.2.5 has a DURATION beside a DATE count days, and
			// one counting hours is read as the days it reaches into, as an
			// event's is and the task's recurrence set is.
			effectiveDue = m.dateMidnight(dtstart, dtstart.date.AddDate(0, 0, ical.DurationDays(duration)))
		}
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

	completed, hasCompleted := m.dateValue(node, "COMPLETED", noShift)
	created, hasCreated := m.dateValue(node, "CREATED", noShift)
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
func (m calendarTimeRangeMatcher) journalInTimeRange(node *icalNode, start, end time.Time, shift instantShift) bool {
	dtstart, ok := m.dateValue(node, "DTSTART", shift)
	if !ok {
		return false
	}
	if dtstart.isDate {
		return start.Before(m.dateMidnight(dtstart, dtstart.date.AddDate(0, 0, 1))) && end.After(dtstart.instant)
	}
	return !start.After(dtstart.instant) && end.After(dtstart.instant)
}

// freeBusyInTimeRange implements the three-row §9.9 VFREEBUSY table. A DURATION
// is ignored here, as §9.9 says outright, because it carries a different meaning
// inside a VFREEBUSY.
func (m calendarTimeRangeMatcher) freeBusyInTimeRange(node *icalNode, start, end time.Time) bool {
	dtstart, hasStart := m.dateValue(node, "DTSTART", noShift)
	dtend, hasEnd := m.dateValue(node, "DTEND", noShift)
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
func (m calendarTimeRangeMatcher) alarmInTimeRange(node, parent *icalNode, start, end time.Time, shift instantShift) bool {
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
// trigger, and how far apart. RFC 5545 §3.6.6 pairs REPEAT with DURATION, so
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
func (m calendarTimeRangeMatcher) alarmTriggerTime(node, parent *icalNode, shift instantShift) (time.Time, bool) {
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

func (m calendarTimeRangeMatcher) alarmAnchor(parent *icalNode, trigger icalProperty, shift instantShift) (time.Time, bool) {
	if !strings.EqualFold(strings.TrimSpace(trigger.parameters["RELATED"]), "END") {
		dtstart, ok := m.dateValue(parent, "DTSTART", shift)
		return dtstart.instant, ok
	}
	if parent.name == "VEVENT" {
		dtstart, ok := m.dateValue(parent, "DTSTART", shift)
		if !ok {
			return time.Time{}, false
		}
		return m.eventOccurrenceEnd(parent, dtstart, shift), true
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
	dtstart, ok := m.dateValue(node, "DTSTART", noShift)
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
	matched := false
	expandable, settled := m.visitRecurrenceInstances(node, master, start, end, func(instance recurrenceInstance) bool {
		effective := instance.shift.apply(instant)
		if master == node {
			occurrence := expandedInstance(m, m.root, master, instance)
			if value, ok := m.dateValue(occurrence, name, noShift); ok {
				effective = value.instant
			} else if name == "DTEND" || name == "DUE" {
				if start, ok := m.dateValue(occurrence, "DTSTART", noShift); ok {
					effective = start.instant.Add(instance.duration)
				}
			}
		}
		matched = !start.After(effective) && end.After(effective)
		return matched
	})
	if !expandable {
		return true
	}
	return matched || !settled
}
