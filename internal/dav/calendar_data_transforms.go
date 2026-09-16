package dav

import (
	"strings"
	"time"

	"github.com/jw6ventures/calcard/internal/ical"
)

// The RFC 4791 §9.6.5–§9.6.7 transforms of a returned calendar object resource.
// Each rewrites the recurrence set the projection then selects from, and each
// judges intersection with the §9.9 tables the filter already uses, because
// §9.6.5 and §9.6.6 both say to use "the same logic as defined for
// CALDAV:time-range".

// The three iCalendar spellings an expanded value can be written back in, which
// are the three RFC 5545 §3.3.5 admits for a date-time property.
const (
	utcDateTimeLayout      = "20060102T150405Z"
	floatingDateTimeLayout = "20060102T150405"
	dateLayout             = "20060102"
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
	expanded := &icalNode{name: root.name, properties: expandedProperties(m, root.properties, 0)}
	for _, child := range root.children {
		// §9.6.5 forbids the output referring to a VTIMEZONE, and every value
		// that referred to one has been rewritten as UTC by now.
		if child.name == "VTIMEZONE" {
			continue
		}
		budget := ical.MaxRecurrenceInstances - len(expanded.children)
		if budget <= 0 {
			break
		}
		expanded.children = append(expanded.children, expandComponent(m, root, child, r, budget)...)
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
		// resource made only of overridden instances, so it stands for its own.
		if thisAndFutureOverride(node) && recurrenceSetMaster(root, node) != nil {
			return nil
		}
		// An ordinary overridden instance already defines exactly one instance
		// and carries the RECURRENCE-ID naming it, so it is emitted as itself.
		return expandedSingleton(m, root, node, r)
	}

	master := m.recurrenceMaster(node, root)
	if master == nil {
		// Nothing here recurs, so the component already defines exactly one
		// instance and there is no slot for a RECURRENCE-ID to name. Adding one
		// would present a one-off as an override of a set that does not exist.
		return expandedSingleton(m, root, node, r)
	}

	instances, expandable := m.recurrenceInstances(node, master, r.Start, r.End)
	if !expandable {
		// A frequency this server cannot enumerate yields no instances to
		// return. Keeping the component with its RRULE would break the §9.6.5
		// MUST NOT, and dropping it would hide a resource that does occur, so
		// the master stands for the set it could not be expanded into.
		return expandedSingleton(m, root, node, r)
	}

	var expanded []*icalNode
	for _, instance := range instances {
		if len(expanded) >= budget {
			break
		}
		if !m.componentInTimeRange(node, root, r.Start, r.End, instance.shift) {
			continue
		}
		expanded = append(expanded, expandedInstance(m, root, node, instance))
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
	if calendarTimeRangeComponents.contains(node.name) && !m.componentInTimeRange(node, root, r.Start, r.End, 0) {
		return nil
	}
	expanded := utcComponent(m, node, 0)
	expanded.properties = dropRecurrenceRange(expanded.properties)
	return []*icalNode{expanded}
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
// instance and the master no longer does.
//
// §9.6.5 requires the RECURRENCE-ID on every instance but the first; putting it
// on all of them satisfies that and matches the §7.8.6 example. It is added
// after the CALDAV:comp selection would have run, because §9.6.5's requirement
// is unconditional while §9.6's permission to omit properties covers only those
// the media type requires.
func expandedInstance(m calendarTimeRangeMatcher, root, master *icalNode, instance recurrenceInstance) *icalNode {
	content, contentShift := master, instance.shift
	if override := governingThisAndFutureOverride(m, root, master, instance.slotShift); override != nil {
		content = override
		contentShift = instanceShiftFrom(m, master, override, instance.shift)
	}

	expanded := utcComponent(m, content, contentShift)
	expanded.properties = dropProperties(expanded.properties, recurrenceIDName)
	if id, ok := recurrenceIDProperty(m, master, instance.slotShift); ok {
		expanded.properties = append(expanded.properties, id)
	}
	return expanded
}

// instanceShiftFrom re-expresses an occurrence offset so it moves the override's
// own dates rather than the master's. The override describes the slot it names;
// a later slot is that description moved by the distance between the two.
func instanceShiftFrom(m calendarTimeRangeMatcher, master, override *icalNode, shift time.Duration) time.Duration {
	masterStart, ok := m.dateValue(master, "DTSTART", shift)
	if !ok {
		return 0
	}
	overrideStart, ok := m.dateValue(override, "DTSTART", 0)
	if !ok {
		return 0
	}
	return masterStart.instant.Sub(overrideStart.instant)
}

// recurrenceIDProperty is the master's DTSTART moved to the slot and renamed,
// which is exactly what RFC 5545 §3.8.4.4 makes a RECURRENCE-ID: the identifier
// of a slot in the master's pattern, not the time the occurrence was moved to.
// Deriving it from the master's own property also keeps its DATE, floating or
// UTC spelling, whatever the instance's dates ended up written as.
func recurrenceIDProperty(m calendarTimeRangeMatcher, master *icalNode, slotShift time.Duration) (icalProperty, bool) {
	dtstart, ok := firstICalProperty(master, "DTSTART")
	if !ok {
		// Nothing to expand around, so there is one instance and no identifier
		// that would distinguish it from another.
		return icalProperty{}, false
	}
	moved, ok := utcProperty(m, dtstart, slotShift)
	if !ok {
		return icalProperty{}, false
	}
	moved.name = "RECURRENCE-ID"
	moved.keyPart = "RECURRENCE-ID" + propertyParameterPart(moved.keyPart)
	return moved, true
}

// governingThisAndFutureOverride is the override that describes the slot at
// slotShift: the latest one whose RECURRENCE-ID is at or before it. RFC 5545
// §3.8.4.4 makes a RANGE=THISANDFUTURE override apply to the instance it names
// and every later one, so the nearest preceding override wins.
func governingThisAndFutureOverride(m calendarTimeRangeMatcher, root, master *icalNode, slotShift time.Duration) *icalNode {
	slot, ok := m.dateValue(master, "DTSTART", slotShift)
	if !ok {
		return nil
	}
	var governing *icalNode
	var governingID time.Time
	for _, child := range root.children {
		if child.name != master.name || !thisAndFutureOverride(child) {
			continue
		}
		recurrenceID, ok := m.dateValue(child, "RECURRENCE-ID", 0)
		if !ok || recurrenceID.instant.After(slot.instant) {
			continue
		}
		if governing == nil || recurrenceID.instant.After(governingID) {
			governing, governingID = child, recurrenceID.instant
		}
	}
	return governing
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
func utcComponent(m calendarTimeRangeMatcher, node *icalNode, shift time.Duration) *icalNode {
	projected := &icalNode{name: node.name, properties: expandedProperties(m, node.properties, shift)}
	for _, child := range node.children {
		// A sub-component defines no recurrence set of its own, so its dates
		// are read as written rather than moved onto the instance.
		projected.children = append(projected.children, utcComponent(m, child, 0))
	}
	return projected
}

func expandedProperties(m calendarTimeRangeMatcher, properties []icalProperty, shift time.Duration) []icalProperty {
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
func expandedProperty(m calendarTimeRangeMatcher, property icalProperty, shift time.Duration) icalProperty {
	zoned := strings.TrimSpace(property.parameters["TZID"]) != ""
	if !zoned && !utcDateProperties.contains(property.name) {
		return property
	}
	propertyShift := time.Duration(0)
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
// an instant would destroy the fact that it is all-day. ok is false for a value
// no zone resolves, which is left exactly as it was written.
func utcProperty(m calendarTimeRangeMatcher, property icalProperty, shift time.Duration) (icalProperty, bool) {
	value := strings.TrimSpace(property.value)
	form, ok := parseICalDateForm(value)
	if !ok {
		return icalProperty{}, false
	}
	instant, ok := m.resolveInstant(property.parameters["TZID"], value, form)
	if !ok {
		return icalProperty{}, false
	}
	instant = instant.Add(shift)

	rewritten := property
	switch {
	case form == icalDateOnly:
		rewritten.value = m.zone.wallClock(instant).Format(dateLayout)
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
	for _, child := range root.children {
		if child.count("RECURRENCE-ID") == 0 || overrideImpactsRange(m, root, child, r) {
			limited.children = append(limited.children, child)
		}
	}
	return limited
}

func overrideImpactsRange(m calendarTimeRangeMatcher, root, override *icalNode, r calendarRange) bool {
	// The current scheduled time: the override's own dates, judged by the same
	// §9.9 table the filter uses.
	if m.componentInTimeRange(override, root, r.Start, r.End, 0) {
		return true
	}
	property, ok := firstICalProperty(override, "RECURRENCE-ID")
	if !ok {
		return false
	}
	original, ok := m.dateValue(override, "RECURRENCE-ID", 0)
	if !ok {
		return false
	}
	// The original scheduled time occupies one occurrence of the master's
	// window. A masterless override still names an instant, which is enough to
	// retain it when that instant falls strictly inside the requested range.
	master := recurrenceSetMaster(root, override)
	window := time.Duration(0)
	if master != nil {
		if dtstart, ok := m.dateValue(master, "DTSTART", 0); ok {
			window = m.occurrenceWindow(master, dtstart)
		}
	}
	if r.Start.Before(original.instant.Add(window)) && r.End.After(original.instant) {
		return true
	}
	if ical.PropertyParamEquals(property.keyPart, "RANGE", "THISANDFUTURE") {
		if master == nil {
			return false
		}
		dtstart, ok := m.dateValue(master, "DTSTART", 0)
		if !ok {
			return false
		}
		window := m.occurrenceWindow(master, dtstart)
		slots, err := ical.RecurrenceSlots(m.raw, master.name, dtstart.instant, window,
			r.Start, r.End, ical.MaxRecurrenceInstances, m.resolveContentLine)
		if err != nil {
			*m.expansionError = err
			return false
		}
		for _, slot := range slots {
			if slot.ExactOverride || !slot.GoverningRangeRecurrenceID.Equal(original.instant) {
				continue
			}
			if recurrencePeriodIntersects(slot.Original, r.Start, r.End) ||
				(!slot.Suppressed && recurrencePeriodIntersects(slot.Effective, r.Start, r.End)) {
				return true
			}
		}
		return false
	}
	return false
}

func recurrencePeriodIntersects(period ical.BusyPeriod, start, end time.Time) bool {
	return period.Start.Before(end) && period.End.After(start)
}

// recurrenceSetMaster is the component an override belongs to: the sibling of
// the same type carrying no RECURRENCE-ID. A resource made only of overridden
// instances has none, which RFC 4791 §4.1 permits.
func recurrenceSetMaster(root, override *icalNode) *icalNode {
	for _, child := range root.children {
		if child.name == override.name && child.count("RECURRENCE-ID") == 0 {
			return child
		}
	}
	return nil
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
