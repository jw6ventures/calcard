package dav

import (
	"slices"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/jw6ventures/calcard/internal/ical"
)

// The RFC 4791 §9.6.5–§9.6.7 transforms of a returned calendar object resource.
// Each rewrites the recurrence set the projection then selects from, and each
// judges intersection with the §9.9 tables the filter already uses, because
// §9.6.5 and §9.6.6 both say to use "the same logic as defined for
// CALDAV:time-range".

// The iCalendar spellings an expanded DATE-TIME can be written back in, the
// two RFC 5545 §3.3.5 admits beside one with a TZID; a DATE is written as
// ical.DateLayout spells it.
const (
	utcDateTimeLayout      = "20060102T150405Z"
	floatingDateTimeLayout = "20060102T150405"
)

// recurrencePropertyNames are the properties §9.6.5 forbids expanded output to
// carry, since each of them describes a set rather than the one instance the
// component now defines.
var recurrencePropertyNames = nameSet("RRULE", "RDATE", "EXDATE", "EXRULE")

// recurrenceIDName is the property expansion synthesizes: dropped from the
// content an instance inherits, then added back naming the slot, and kept
// through the CALDAV:comp selection that follows.
var recurrenceIDName = nameSet("RECURRENCE-ID")

// shiftedDateProperties move with a recurrence instance; the remaining
// date-valued properties describe the component as a whole and read the same
// for every instance of it. This is the split §9.9 already makes.
var shiftedDateProperties = nameSet("DTSTART", "DTEND", "DUE")

// utcDateProperties are the date-valued properties an expanded component
// rewrites, so nothing is left referring to a VTIMEZONE the output no longer
// carries.
var utcDateProperties = nameSet("DTSTART", "DTEND", "DUE", "COMPLETED", "CREATED", "DTSTAMP", "LAST-MODIFIED", "RECURRENCE-ID")

// expandCalendarData replaces every recurring component of root with the
// individual instances whose scheduled time intersects the range, per RFC 4791
// §9.6.5. The output carries no recurrence property and no VTIMEZONE, and every
// zoned value is rewritten as a date with UTC time.
//
// The instance cap bounds the whole object rather than each component of it, so
// a resource carrying several masters cannot multiply the limit by how many it
// carries.
func expandCalendarData(m calendarTimeRangeMatcher, root *icalNode, r calendarRange) *icalNode {
	expanded := &icalNode{name: root.name, properties: expandedProperties(m, root.properties, noShift)}
	for _, child := range root.children {
		// §9.6.5 forbids the output referring to a VTIMEZONE, and every value
		// that referred to one has been rewritten as UTC by now.
		if child.name == "VTIMEZONE" {
			continue
		}
		budget := ical.MaxRecurrenceInstances - len(expanded.children)
		children := expandComponent(m, root, child, r, budget)
		if *m.expansionError != nil {
			return expanded
		}
		if len(children) > budget {
			*m.expansionError = ical.ErrRecurrenceExpansionLimit
			return expanded
		}
		expanded.children = append(expanded.children, children...)
	}
	return expanded
}

// expandComponent is the instances one top-level component contributes, at most
// budget of them.
func expandComponent(m calendarTimeRangeMatcher, root, node *icalNode, r calendarRange, budget int) []*icalNode {
	if node.count("RECURRENCE-ID") != 0 {
		// A RANGE=THISANDFUTURE override governs a run of instances rather than
		// standing for one, so the master's expansion already carries its times
		// and its content on every instance from its slot onward. Emitting it
		// here as well would return two components for the slot it names, which
		// §9.6.5 does not admit: each component defines exactly one instance.
		// Without a master there is no expansion to carry it, and §4.1 permits a
		// resource made only of overridden instances, so it stands for its own;
		// so does one naming a slot its master never generates.
		if thisAndFutureOverride(node) {
			if master := m.recurrenceSetMaster(root, node); master != nil {
				if !m.standsAlone(root, m.expandSeries(root, master, r.Start, r.End), node) {
					return nil
				}
				return []*icalNode{writtenSingleton(m, node)}
			}
		}
		if calendarTimeRangeComponents.contains(node.name) && !m.componentInTimeRange(node, root, r.Start, r.End, noShift) {
			return nil
		}
		// An ordinary overridden instance already defines exactly one instance
		// and carries the RECURRENCE-ID naming it, so it is emitted as itself.
		return []*icalNode{writtenSingleton(m, node)}
	}

	master := m.recurrenceMaster(node, root)
	if master == nil {
		// Nothing here recurs, so the component already defines exactly one
		// instance and there is no slot for a RECURRENCE-ID to name. Adding one
		// would present a one-off as an override of a set that does not exist.
		return expandedSingleton(m, root, node, r)
	}

	series := m.expandSeries(root, master, r.Start, r.End)
	instances, expandable, settled := series.instances, series.expandable, series.settled
	if expandable && !settled {
		// §9.6.5 owes every instance of the range, which both attributes of the
		// element bound, so a prefix of the set is not an answer it can give.
		*m.expansionError = ical.ErrRecurrenceExpansionLimit
		return nil
	}
	if !expandable {
		// A frequency this server cannot enumerate yields no instances to
		// return. Keeping the component with its RRULE would break the §9.6.5
		// MUST NOT, and dropping it would hide a resource that does occur, so
		// the master stands for the set it could not be expanded into.
		return expandedSingleton(m, root, node, r)
	}

	var expanded []*icalNode
	for _, instance := range instances {
		occurrence := expandedInstance(m, root, node, instance)
		if !m.componentInTimeRange(occurrence, root, r.Start, r.End, noShift) {
			continue
		}
		if len(expanded) >= budget {
			*m.expansionError = ical.ErrRecurrenceExpansionLimit
			return expanded
		}
		expanded = append(expanded, occurrence)
	}
	return expanded
}

// expandedSingleton is a component that stands for one instance already, so
// expansion has only to return it once -- stripped of the recurrence properties
// and VTIMEZONE references §9.6.5 forbids -- when it intersects the range.
//
// §9.9 defines a time-range test for five component names and no others, so a
// component outside them is returned rather than dropped: §5.3.3 requires a
// non-standard component stored by PUT to be preserved, and discarding one on
// the strength of a test that never ran would lose it.
func expandedSingleton(m calendarTimeRangeMatcher, root, node *icalNode, r calendarRange) []*icalNode {
	if calendarTimeRangeComponents.contains(node.name) && !m.componentInTimeRange(node, root, r.Start, r.End, noShift) {
		return nil
	}
	return []*icalNode{writtenSingleton(m, node)}
}

// writtenSingleton is node as expanded output writes a component standing for
// one instance, its end written as it is read.
func writtenSingleton(m calendarTimeRangeMatcher, node *icalNode) *icalNode {
	expanded := utcComponent(m, node, noShift)
	expanded.properties = dropRecurrenceRange(expanded.properties)
	if node.name == "VEVENT" {
		if start, ok := m.dateValue(node, "DTSTART", noShift); ok {
			switch extent := m.eventEnd(node, start, noShift); {
			case start.isDate:
				spanExpandedDays(expanded, node.name, extent.Days(), !m.eventOccurrenceEnd(node, start, noShift).After(start.instant))
			case extent.InheritsLength:
				// An override taking its series' length writes no end of
				// its own, so it is given the one it occupies, which a
				// client would otherwise read as RFC 5545 §3.6.1 reads a
				// VEVENT without one.
				property, _ := firstICalProperty(node, "DTSTART")
				if end, ok := timedEndProperty(m, property, noShift, extent.SeriesLength); ok {
					expanded.properties = append(expanded.properties, end)
				}
			default:
				endInstantAsInstant(m, expanded)
			}
		}
	}
	return expanded
}

// dropRecurrenceRange removes the RANGE parameter from a RECURRENCE-ID. RFC 5545
// §3.8.4.4 makes RANGE say the identifier covers every later instance as well,
// which a component standing for exactly one of them cannot claim -- and in
// expanded output there is no recurrence set left for it to reach.
func dropRecurrenceRange(properties []icalProperty) []icalProperty {
	for i, property := range properties {
		if property.name != "RECURRENCE-ID" {
			continue
		}
		properties[i].keyPart, properties[i].parameters = propertyWithoutParameter(property, "RANGE")
	}
	return properties
}

// expandedInstance is one generated instance: the content that governs it, with
// its dates moved onto the occurrence, plus a RECURRENCE-ID naming which slot of
// the master's pattern it is.
//
// The content is the master unless a RANGE=THISANDFUTURE override reaches this
// slot, in which case RFC 5545 §3.8.4.4 makes that override describe the
// instance and the master no longer does. Content starting on a DATE is
// written on the date the expansion placed the instance on, spanning the days
// it gave it.
//
// §9.6.5 requires the RECURRENCE-ID on every instance but the first; putting it
// on all of them satisfies that and matches the §7.8.6 example. It is added
// after the CALDAV:comp selection would have run, because §9.6.5's requirement
// is unconditional while §9.6's permission to omit properties covers only those
// the media type requires.
func expandedInstance(m calendarTimeRangeMatcher, root, master *icalNode, instance recurrenceInstance) *icalNode {
	content, contentShift := master, instance.shift
	if override := governingThisAndFutureOverride(m, root, master, instance); override != nil {
		content = override
		contentShift = instanceShiftFrom(m, master, override, instance.shift)
	}

	expanded := utcComponent(m, content, contentShift)
	start, ok := m.dateValue(content, "DTSTART", noShift)
	if ok && start.isDate {
		date, days := instance.startDate, instance.days
		if date.IsZero() {
			occurrenceStart := contentShift.apply(start.instant)
			date, days = m.LocalDate(start.tzid, occurrenceStart), ical.DurationDays(instance.duration)
		}
		for i, property := range expanded.properties {
			if property.name == "DTSTART" {
				expanded.properties[i].value = date.Format(ical.DateLayout)
			}
		}
		spanExpandedDays(expanded, master.name, days, instance.duration <= 0)
	} else if ok {
		delta := instance.duration - writtenOccurrenceLength(m, content, start)
		// An override taking its series' length writes no end of its own, so
		// the instance is given one, which a client would otherwise read as
		// RFC 5545 §3.6.1 reads a VEVENT without one.
		if delta != 0 || m.eventEnd(content, start, noShift).InheritsLength {
			adjusted := false
			for i, property := range expanded.properties {
				switch property.name {
				case "DTEND", "DUE":
					if end, ok := utcProperty(m, property, durationShift(delta)); ok {
						expanded.properties[i] = end
						adjusted = true
					}
				case "DURATION":
					expanded.properties[i].value = "PT" + strconv.FormatInt(int64(instance.duration/time.Second), 10) + "S"
					adjusted = true
				}
			}
			if !adjusted && master.name == "VEVENT" {
				property, _ := firstICalProperty(content, "DTSTART")
				if end, ok := timedEndProperty(m, property, contentShift, instance.duration); ok {
					expanded.properties = append(expanded.properties, end)
				}
			} else if !adjusted && master.name == "VTODO" {
				property, _ := firstICalProperty(content, "DTSTART")
				if end, ok := utcProperty(m, property, contentShift.plus(durationShift(instance.duration))); ok {
					end.name = "DUE"
					end.keyPart = end.name + propertyParameterPart(end.keyPart)
					expanded.properties = append(expanded.properties, end)
				}
			}
		}
		if master.name == "VEVENT" && instance.duration <= 0 {
			dtendAsZeroDuration(expanded)
		}
	}
	expanded.properties = dropProperties(expanded.properties, recurrenceIDName)
	if id, ok := recurrenceIDProperty(m, master, instance); ok {
		expanded.properties = append(expanded.properties, id)
	}
	return expanded
}

// endProperties name the property that writes where an occurrence of a
// component ends.
var endProperties = map[string]string{"VEVENT": "DTEND", "VTODO": "DUE"}

// timedEndProperty is the DTEND an expanded occurrence of a VEVENT starting at
// the DATE-TIME start, moved by shift, and writing no end of its own is given,
// so it ends length later. One lasting no time is given none: RFC 5545
// §3.8.2.2 requires a DTEND to be later than its DTSTART, and §3.6.1 reads a
// DATE-TIME start alone as that instant. ok is false when there is no end to
// write, or for a start no zone places.
func timedEndProperty(m calendarTimeRangeMatcher, start icalProperty, shift instantShift, length time.Duration) (icalProperty, bool) {
	if length <= 0 {
		return icalProperty{}, false
	}
	end, ok := utcProperty(m, start, shift.plus(durationShift(length)))
	if !ok {
		return icalProperty{}, false
	}
	end.name = "DTEND"
	end.keyPart = end.name + propertyParameterPart(end.keyPart)
	return end, true
}

// zeroDuration is the DURATION of an occurrence lasting no time, spelled as
// value: PT0S beside a DATE-TIME, P0D beside a DATE, whose DURATION RFC 5545
// §3.8.2.5 has count days.
func zeroDuration(value string) icalProperty {
	return icalProperty{name: "DURATION", keyPart: "DURATION", value: value}
}

// endInstantAsInstant rewrites the DTEND of expanded, an expanded VEVENT
// starting at a DATE-TIME, as a zero DURATION when it is not later than the
// DTSTART: §3.8.2.2 admits no such DTEND, and every reader here places the
// occurrence at the instant it starts, which the zero DURATION says.
func endInstantAsInstant(m calendarTimeRangeMatcher, expanded *icalNode) {
	start, ok := m.dateValue(expanded, "DTSTART", noShift)
	if !ok {
		return
	}
	if end, ok := m.dateValue(expanded, "DTEND", noShift); ok && !end.instant.After(start.instant) {
		dtendAsZeroDuration(expanded)
	}
}

// dtendAsZeroDuration replaces the DTEND of expanded, an occurrence lasting
// no time, with the zero DURATION endInstantAsInstant explains.
func dtendAsZeroDuration(expanded *icalNode) {
	for i, property := range expanded.properties {
		if property.name == "DTEND" {
			expanded.properties[i] = zeroDuration("PT0S")
			return
		}
	}
}

// spanExpandedDays writes the end of expanded, an expanded occurrence of a
// componentName component starting on a DATE, as the days it spans, in the
// form its content wrote it: a DTEND or DUE as the date days on, a DURATION
// as the days it counts -- kept as written when it already counts them in
// days, and rewritten in days when it counts hours, which RFC 5545 §3.8.2.5
// does not admit beside a DATE -- and no end as none, the one day §3.6.1 gives
// a VEVENT. A VEVENT lasting no time -- spanning no day, or only a day its
// zone skips, as zeroLength reports -- has its DTEND given as a zero
// DURATION, since §3.8.2.2 has a DTEND come later than its DTSTART; and a
// VEVENT writing no end is given one when it spans other than one day.
func spanExpandedDays(expanded *icalNode, componentName string, days int, zeroLength bool) {
	endName, ok := endProperties[componentName]
	if !ok {
		return
	}
	startProperty, ok := firstICalProperty(expanded, "DTSTART")
	if !ok {
		return
	}
	startDate, ok := ical.DateOf(startProperty.value)
	if !ok {
		return
	}
	days = max(days, 0)
	endDate := startDate.AddDate(0, 0, days).Format(ical.DateLayout)
	for i, property := range expanded.properties {
		switch property.name {
		case endName:
			if (days == 0 || zeroLength) && componentName == "VEVENT" {
				expanded.properties[i] = zeroDuration("P0D")
			} else {
				expanded.properties[i].value = endDate
			}
			return
		case "DURATION":
			value := strings.TrimSpace(property.value)
			if duration, ok := ical.ParseDuration(value); !ok || strings.ContainsAny(value, "Tt") || ical.DurationDays(duration) != days {
				expanded.properties[i].value = "P" + strconv.Itoa(days) + "D"
			}
			return
		}
	}
	switch {
	case componentName != "VEVENT" || days == 1:
	case days == 0:
		expanded.properties = append(expanded.properties, zeroDuration("P0D"))
	default:
		expanded.properties = append(expanded.properties, icalProperty{name: "DTEND", keyPart: "DTEND;VALUE=DATE",
			value: endDate, parameters: map[string]string{"VALUE": "DATE"}})
	}
}

// writtenOccurrenceLength is how long an occurrence of node lasts as its end is
// written. It differs from occurrenceWindow where that end is one the window
// reads past; an expanded instance then has its end rewritten to the one it
// occupies.
func writtenOccurrenceLength(m calendarTimeRangeMatcher, node *icalNode, dtstart icalTimeValue) time.Duration {
	if name, ok := endProperties[node.name]; ok {
		if end, ok := m.dateValue(node, name, noShift); ok {
			return end.instant.Sub(dtstart.instant)
		}
	}
	return m.occurrenceWindow(node, dtstart)
}

// instanceShiftFrom re-expresses an occurrence offset so it moves the override's
// own dates rather than the master's. The override describes the slot it names;
// a later slot is that description moved by the distance between the two.
func instanceShiftFrom(m calendarTimeRangeMatcher, master, override *icalNode, shift instantShift) instantShift {
	masterStart, ok := m.dateValue(master, "DTSTART", shift)
	if !ok {
		return noShift
	}
	overrideStart, ok := m.dateValue(override, "DTSTART", noShift)
	if !ok {
		return noShift
	}
	return shiftBetween(overrideStart.instant, masterStart.instant)
}

// recurrenceIDProperty is the master's DTSTART moved to the slot and renamed,
// which is exactly what RFC 5545 §3.8.4.4 makes a RECURRENCE-ID: the identifier
// of a slot in the master's pattern, not the time the occurrence was moved to.
// Deriving it from the master's own property also keeps its DATE, floating or
// UTC spelling, whatever the instance's dates ended up written as. A DATE
// slot is named by its date, which a zone skipping one would resolve to the
// next date's midnight.
func recurrenceIDProperty(m calendarTimeRangeMatcher, master *icalNode, instance recurrenceInstance) (icalProperty, bool) {
	dtstart, ok := firstICalProperty(master, "DTSTART")
	if !ok {
		// Nothing to expand around, so there is one instance and no identifier
		// that would distinguish it from another.
		return icalProperty{}, false
	}
	moved, ok := utcProperty(m, dtstart, instance.slotShift)
	if !ok {
		return icalProperty{}, false
	}
	if !instance.slotDate.IsZero() {
		moved.value = instance.slotDate.Format(ical.DateLayout)
	}
	moved.name = "RECURRENCE-ID"
	moved.keyPart = "RECURRENCE-ID" + propertyParameterPart(moved.keyPart)
	return moved, true
}

// governingThisAndFutureOverride is the override that describes the slot
// instance fills: the latest one whose RECURRENCE-ID is at or before it. RFC
// 5545 §3.8.4.4 makes a RANGE=THISANDFUTURE override apply to the instance it
// names and every later one, so the nearest preceding override wins.
func governingThisAndFutureOverride(m calendarTimeRangeMatcher, root, master *icalNode, instance recurrenceInstance) *icalNode {
	slot, ok := m.dateValue(master, "DTSTART", instance.slotShift)
	if !ok {
		return nil
	}
	return m.overrideIndex(root, master).governing(slot.instant, instance.slotDate)
}

// recurrenceOverride links a recurrence slot to the component overriding it:
// recurrenceID is the instant its RECURRENCE-ID names, and date the date it
// names the slot by, when ical.RecurrenceIDNamesDate has it name one.
type recurrenceOverride struct {
	recurrenceID time.Time
	date         time.Time
	node         *icalNode
}

// recurrenceOverrideIndex finds the component describing a slot of a master's
// recurrence set. It is consulted once per expanded instance by free-busy, by
// §9.6.5 expansion and by every §9.9 test that visits instances, and RFC 5545
// §3.8.4.4 bounds neither how many overrides a resource may carry, so both of
// its questions are lookups rather than walks over the resource's components.
// It matches an override to a slot as ical's expansion does: by date when its
// RECURRENCE-ID names the slot by date (ical.RecurrenceIDNamesDate), else by
// instant, and it orders the RANGE=THISANDFUTURE ones by date only when every
// one of them names its slot by date (byDate), as the expansion does.
type recurrenceOverrideIndex struct {
	byDate bool
	// overrides holds the first override naming each slot, which exactDates
	// keys by date and exact by instant.
	overrides  []recurrenceOverride
	exactDates map[time.Time]recurrenceOverride
	exact      map[time.Time]recurrenceOverride
	// thisAndFuture is ordered by key; among overrides naming the same slot
	// the first in the resource comes first, and is the one that governs.
	thisAndFuture []recurrenceOverride
}

type recurrenceOverrideIndexKey struct {
	root, master *icalNode
}

// overrideIndex is the override index of master's recurrence set within root,
// built once per matcher and shared by every copy of it.
func (m calendarTimeRangeMatcher) overrideIndex(root, master *icalNode) recurrenceOverrideIndex {
	key := recurrenceOverrideIndexKey{root: root, master: master}
	if m.indexes == nil {
		return newRecurrenceOverrideIndex(m, root, master)
	}
	if index, ok := m.indexes.overrides[key]; ok {
		return index
	}
	index := newRecurrenceOverrideIndex(m, root, master)
	if m.indexes.overrides == nil {
		m.indexes.overrides = make(map[recurrenceOverrideIndexKey]recurrenceOverrideIndex)
	}
	m.indexes.overrides[key] = index
	return index
}

func newRecurrenceOverrideIndex(m calendarTimeRangeMatcher, root, master *icalNode) recurrenceOverrideIndex {
	masterStart, _ := firstICalProperty(master, "DTSTART")
	seriesStart := ical.PropertyValue{KeyPart: masterStart.keyPart, Value: masterStart.value}
	index := recurrenceOverrideIndex{
		byDate:     true,
		exactDates: make(map[time.Time]recurrenceOverride),
		exact:      make(map[time.Time]recurrenceOverride),
	}
	for _, child := range root.children {
		if child.name != master.name || child.count("RECURRENCE-ID") == 0 {
			continue
		}
		recurrenceID, ok := m.dateValue(child, "RECURRENCE-ID", noShift)
		if !ok {
			continue
		}
		override := recurrenceOverride{recurrenceID: recurrenceID.instant.UTC(), node: child}
		property, _ := firstICalProperty(child, "RECURRENCE-ID")
		if ical.RecurrenceIDNamesDate(seriesStart, ical.PropertyValue{KeyPart: property.keyPart, Value: property.value}) {
			override.date, _ = ical.DateOf(property.value)
		}
		named, key := index.exact, override.recurrenceID
		if !override.date.IsZero() {
			named, key = index.exactDates, override.date
		}
		if _, seen := named[key]; !seen {
			named[key] = override
			index.overrides = append(index.overrides, override)
		}
		if thisAndFutureOverride(child) {
			index.thisAndFuture = append(index.thisAndFuture, override)
			index.byDate = index.byDate && !override.date.IsZero()
		}
	}
	slices.SortStableFunc(index.thisAndFuture, func(a, b recurrenceOverride) int {
		return index.key(a).Compare(index.key(b))
	})
	return index
}

// key is what a RANGE=THISANDFUTURE override is ordered and matched by.
func (index recurrenceOverrideIndex) key(override recurrenceOverride) time.Time {
	if index.byDate {
		return override.date
	}
	return override.recurrenceID
}

// governing is the RANGE=THISANDFUTURE override with the latest RECURRENCE-ID
// at or before the slot at instant, on date in a series starting on a DATE,
// or nil.
func (index recurrenceOverrideIndex) governing(instant, date time.Time) *icalNode {
	if override, ok := index.governingOverride(instant, date); ok {
		return override.node
	}
	return nil
}

// governingOverride is governing's override, with the slot its RECURRENCE-ID
// names.
func (index recurrenceOverrideIndex) governingOverride(instant, date time.Time) (recurrenceOverride, bool) {
	slot := instant.UTC()
	if index.byDate && !date.IsZero() {
		slot = date
	}
	overrides := index.thisAndFuture
	after := sort.Search(len(overrides), func(i int) bool {
		return index.key(overrides[i]).After(slot)
	})
	if after == 0 {
		return recurrenceOverride{}, false
	}
	latest := index.key(overrides[after-1])
	first := sort.Search(after, func(i int) bool {
		return !index.key(overrides[i]).Before(latest)
	})
	return overrides[first], true
}

// describing is the component that describes the slot at instant, on date in
// a series starting on a DATE: its exact override, else the governing
// RANGE=THISANDFUTURE override, else the master.
func (index recurrenceOverrideIndex) describing(master *icalNode, instant, date time.Time) *icalNode {
	if override, ok := index.exactDates[date]; ok && !date.IsZero() {
		return override.node
	}
	if override, ok := index.exact[instant.UTC()]; ok {
		return override.node
	}
	if node := index.governing(instant, date); node != nil {
		return node
	}
	return master
}

// thisAndFutureOverride reports whether the component overrides the instance it
// names and every later one, rather than that instance alone.
func thisAndFutureOverride(node *icalNode) bool {
	property, ok := firstICalProperty(node, "RECURRENCE-ID")
	return ok && ical.PropertyParamEquals(property.keyPart, "RANGE", "THISANDFUTURE")
}

// utcComponent is one component with the recurrence properties §9.6.5 forbids
// removed and every date-valued property rewritten so it refers to no
// VTIMEZONE. Sub-components come along, since a VALARM is part of the instance.
func utcComponent(m calendarTimeRangeMatcher, node *icalNode, shift instantShift) *icalNode {
	projected := &icalNode{name: node.name, properties: expandedProperties(m, node.properties, shift)}
	for _, child := range node.children {
		// A sub-component defines no recurrence set of its own, so its dates
		// are read as written rather than moved onto the instance.
		projected.children = append(projected.children, utcComponent(m, child, noShift))
	}
	return projected
}

func expandedProperties(m calendarTimeRangeMatcher, properties []icalProperty, shift instantShift) []icalProperty {
	projected := make([]icalProperty, 0, len(properties))
	for _, property := range properties {
		if recurrencePropertyNames.contains(property.name) {
			continue
		}
		projected = append(projected, expandedProperty(m, property, shift))
	}
	return projected
}

// expandedProperty is one property as expanded output carries it. §9.6.5 puts
// two requirements on it, and they reach different sets: a date and local time
// with a time zone reference becomes a date with UTC time, while *nothing* may
// reference a VTIMEZONE, which the output no longer carries. The second is
// keyed on the TZID parameter rather than on the date-valued names, because
// RFC 5545 §3.2.19 admits one on any DATE-TIME value -- an alarm's absolute
// TRIGGER and an X- property among them.
func expandedProperty(m calendarTimeRangeMatcher, property icalProperty, shift instantShift) icalProperty {
	zoned := strings.TrimSpace(property.parameters["TZID"]) != ""
	if !zoned && !utcDateProperties.contains(property.name) {
		return property
	}
	propertyShift := noShift
	if shiftedDateProperties.contains(property.name) {
		propertyShift = shift
	}
	if rewritten, ok := utcProperty(m, property, propertyShift); ok {
		return rewritten
	}
	if !zoned {
		return property
	}
	// A value no zone places cannot be rewritten as an instant, but the
	// reference still has to go: there is no VTIMEZONE left to resolve it
	// against, so keeping it would name a definition the client cannot read.
	property.keyPart, property.parameters = propertyWithoutParameter(property, "TZID")
	return property
}

// utcProperty rewrites one date-valued property onto the instance shift,
// dropping the TZID reference §9.6.5 forbids. A DATE stays a DATE and a
// floating value stays floating: §9.6.5 mandates conversion only for a date and
// local time carrying a time zone reference, and rewriting an all-day value as
// an instant would destroy the fact that it is all-day. A DATE left where it is
// keeps the date it names, which a zone skipping that date would resolve onto
// the next one's midnight. ok is false for a value no zone resolves, which is
// left exactly as it was written.
func utcProperty(m calendarTimeRangeMatcher, property icalProperty, shift instantShift) (icalProperty, bool) {
	value := strings.TrimSpace(property.value)
	form, ok := parseICalDateForm(value)
	if !ok {
		return icalProperty{}, false
	}
	instant, ok := m.resolveInstant(property.parameters["TZID"], value, form)
	if !ok {
		return icalProperty{}, false
	}
	instant = shift.apply(instant)

	rewritten := property
	switch {
	case form == icalDateOnly && shift == noShift:
		rewritten.value = value
	case form == icalDateOnly:
		rewritten.value = m.zone.wallClock(instant).Format(ical.DateLayout)
	case form == icalUTCDateTime, strings.TrimSpace(property.parameters["TZID"]) != "":
		rewritten.value = instant.UTC().Format(utcDateTimeLayout)
	default:
		rewritten.value = m.zone.wallClock(instant).Format(floatingDateTimeLayout)
	}
	rewritten.keyPart, rewritten.parameters = propertyWithoutParameter(property, "TZID")
	return rewritten, true
}

// propertyWithoutParameter rebuilds a content line's name-and-parameter part
// without the named parameter, preserving the spelling of everything else, and
// returns the parsed parameter map to match. The map is copied rather than
// edited in place, because it belongs to the node the projection is reading.
func propertyWithoutParameter(property icalProperty, drop string) (string, map[string]string) {
	parts, ok := splitOutsideQuotes(property.keyPart, ';')
	if !ok || len(parts) == 0 {
		return property.keyPart, property.parameters
	}
	kept := make([]string, 0, len(parts))
	kept = append(kept, parts[0])
	parameters := make(map[string]string, len(property.parameters))
	for name, value := range property.parameters {
		parameters[name] = value
	}
	for _, part := range parts[1:] {
		name, _, _ := strings.Cut(part, "=")
		name = strings.ToUpper(strings.TrimSpace(name))
		if strings.EqualFold(name, drop) {
			delete(parameters, name)
			continue
		}
		kept = append(kept, part)
	}
	return strings.Join(kept, ";"), parameters
}

// propertyParameterPart is everything after the property name in a content
// line's key part, including the leading semicolon, so a synthesized property
// can carry the same parameters as the one it was derived from.
func propertyParameterPart(keyPart string) string {
	parts, ok := splitOutsideQuotes(keyPart, ';')
	if !ok || len(parts) < 2 {
		return ""
	}
	return ";" + strings.Join(parts[1:], ";")
}

func dropProperties(properties []icalProperty, names icalNameSet) []icalProperty {
	kept := properties[:0:0]
	for _, property := range properties {
		if names.contains(property.name) {
			continue
		}
		kept = append(kept, property)
	}
	return kept
}

// limitCalendarRecurrenceSet keeps the master component and only the overrides
// impacting the range, per RFC 4791 §9.6.6. An override impacts the range when
// its current scheduled time overlaps it, when the time it would have had
// unoverridden overlaps it, or when a RANGE parameter makes it govern instances
// that do. Nothing is converted and no recurrence property is dropped: this
// element narrows the set, it does not expand it.
func limitCalendarRecurrenceSet(m calendarTimeRangeMatcher, root *icalNode, r calendarRange) *icalNode {
	limited := &icalNode{name: root.name, properties: root.properties}
	slots := newRangeSlots(m, r)
	for _, child := range root.children {
		if child.count("RECURRENCE-ID") == 0 || overrideImpactsRange(m, root, child, r, slots) {
			limited.children = append(limited.children, child)
		}
	}
	return limited
}

// rangeSlots expands a master's recurrence slots over one range at most once.
// Every RANGE=THISANDFUTURE override of the master asks the same question of
// them, and a resource may carry any number of overrides.
type rangeSlots struct {
	m      calendarTimeRangeMatcher
	r      calendarRange
	byNode map[*icalNode][]ical.RecurrenceSlot
}

func newRangeSlots(m calendarTimeRangeMatcher, r calendarRange) *rangeSlots {
	return &rangeSlots{m: m, r: r, byNode: make(map[*icalNode][]ical.RecurrenceSlot)}
}

// of returns the slots of master's recurrence set touching the range. An
// expansion failure is recorded on the matcher, which ends the transform.
func (s *rangeSlots) of(master *icalNode) ([]ical.RecurrenceSlot, bool) {
	if slots, ok := s.byNode[master]; ok {
		return slots, true
	}
	if *s.m.expansionError != nil {
		return nil, false
	}
	dtstart, ok := s.m.dateValue(master, "DTSTART", noShift)
	if !ok {
		return nil, false
	}
	window := s.m.occurrenceWindow(master, dtstart)
	slots, err := ical.RecurrenceSlots(s.m.raw, master.name, dtstart.instant, window,
		s.r.Start, s.r.End, ical.MaxRecurrenceInstances, s.m)
	if err != nil {
		*s.m.expansionError = err
		return nil, false
	}
	s.byNode[master] = slots
	return slots, true
}

func overrideImpactsRange(m calendarTimeRangeMatcher, root, override *icalNode, r calendarRange, rangeSlots *rangeSlots) bool {
	// The current scheduled time: the override's own dates, judged by the same
	// §9.9 table the filter uses.
	if m.componentInTimeRange(override, root, r.Start, r.End, noShift) {
		return true
	}
	property, ok := firstICalProperty(override, "RECURRENCE-ID")
	if !ok {
		return false
	}
	original, ok := m.dateValue(override, "RECURRENCE-ID", noShift)
	if !ok {
		return false
	}
	// The original scheduled time occupies one occurrence of the master's
	// window, which for a DATE slot of a DATE series is its days from midnight
	// to midnight. A masterless override still names an instant, which is
	// enough to retain it when that instant falls within the requested range.
	master := m.recurrenceSetMaster(root, override)
	originalEnd := original.instant
	if master != nil {
		if dtstart, ok := m.dateValue(master, "DTSTART", noShift); ok {
			window := m.occurrenceWindow(master, dtstart)
			originalEnd = original.instant.Add(window)
			if dtstart.isDate && original.isDate {
				originalEnd = m.dateMidnight(original, original.date.AddDate(0, 0, int(window/(24*time.Hour))))
			}
		}
	}
	if periodInTimeRange(original.instant, originalEnd, r.Start, r.End) {
		return true
	}
	if ical.PropertyParamEquals(property.keyPart, "RANGE", "THISANDFUTURE") {
		if master == nil {
			return false
		}
		slots, ok := rangeSlots.of(master)
		if !ok {
			return false
		}
		// The override governing a slot is found as expansion finds it, by
		// date in a series starting on a DATE.
		index := m.overrideIndex(root, master)
		for _, slot := range slots {
			if slot.ExactOverride || index.governing(slot.RecurrenceID, slot.RecurrenceDate) != override {
				continue
			}
			if periodInTimeRange(slot.Original.Start, slot.Original.End, r.Start, r.End) ||
				(!slot.Suppressed && periodInTimeRange(slot.Effective.Start, slot.Effective.End, r.Start, r.End)) {
				return true
			}
		}
		return false
	}
	return false
}

// recurrenceSetMaster is the component an override belongs to: the first
// sibling of the same type carrying no RECURRENCE-ID. A resource made only of
// overridden instances has none, which RFC 4791 §4.1 permits.
//
// It is asked once per override, and a master may follow every one of its
// overrides, so the masters of root are found in one walk and kept with the
// matcher's other per-resource indexes.
func (m calendarTimeRangeMatcher) recurrenceSetMaster(root, override *icalNode) *icalNode {
	var masters map[string]*icalNode
	ok := false
	if m.indexes != nil {
		masters, ok = m.indexes.masters[root]
	}
	if !ok {
		masters = make(map[string]*icalNode)
		for _, child := range root.children {
			if _, seen := masters[child.name]; !seen && child.count("RECURRENCE-ID") == 0 {
				masters[child.name] = child
			}
		}
		if m.indexes != nil {
			if m.indexes.masters == nil {
				m.indexes.masters = make(map[*icalNode]map[string]*icalNode)
			}
			m.indexes.masters[root] = masters
		}
	}
	return masters[override.name]
}

// limitCalendarFreeBusySet drops the FREEBUSY period values that do not
// intersect the range, and the properties left holding none, per RFC 4791
// §9.6.7. Everything else about the VFREEBUSY, and every other component, is
// left as it stands.
func limitCalendarFreeBusySet(m calendarTimeRangeMatcher, root *icalNode, r calendarRange) *icalNode {
	limited := &icalNode{name: root.name, properties: root.properties}
	for _, child := range root.children {
		if child.name != "VFREEBUSY" {
			limited.children = append(limited.children, child)
			continue
		}
		limited.children = append(limited.children, limitFreeBusyComponent(m, child, r))
	}
	return limited
}

func limitFreeBusyComponent(m calendarTimeRangeMatcher, node *icalNode, r calendarRange) *icalNode {
	limited := &icalNode{name: node.name, children: node.children}
	for _, property := range node.properties {
		if property.name != "FREEBUSY" {
			limited.properties = append(limited.properties, property)
			continue
		}
		var kept []string
		for _, period := range strings.Split(property.value, ",") {
			period = strings.TrimSpace(period)
			start, end, ok := m.freeBusyPeriod(property, period)
			if !ok {
				// A period this server cannot read is not one it can rule out
				// of the range either, so it stays.
				kept = append(kept, period)
				continue
			}
			if r.Start.Before(end) && r.End.After(start) {
				kept = append(kept, period)
			}
		}
		if len(kept) == 0 {
			continue
		}
		property.value = strings.Join(kept, ",")
		limited.properties = append(limited.properties, property)
	}
	return limited
}
