package dav

import (
	"slices"
	"strings"
	"time"

	"github.com/jw6ventures/calcard/internal/calobject"
	"github.com/jw6ventures/calcard/internal/ical"
)

var _ calobject.Expander = EventInstances

// EventInstances lists the instances of the recurring VEVENT of raw, a stored
// calendar object, that RFC 4791 §9.9 places in [start, end), as a
// calendar-query with CALDAV:expand over that range returns them (§9.6.5): read
// as the reports read the object, with floating times in UTC, each placed
// where the RANGE=THISANDFUTURE override governing it puts it, and with the
// instances an override replaces left out. It lists at most
// ical.MaxRecurrenceInstances of them and of the overrides placed beside them
// together, the most one expansion returns, and is incomplete when it left any
// out. It is a calobject.Expander. Its work draws on work, when it is not nil,
// and an expansion that runs out of it is incomplete.
//
// Each instance is placed from the period the expansion generates for it,
// which is the extent the §9.6.5 expansion gives the instance it writes out,
// rather than by writing it out: a view lists every instance of a month for
// each series it shows.
func EventInstances(raw string, start, end time.Time, work *ical.RequestWork) calobject.Expansion {
	root, err := parseStoredICalendarObject(raw)
	if err != nil {
		return calobject.Expansion{}
	}
	m := newCalendarTimeRangeMatcher(raw, root, floatingZone{}).withPool(work)
	var master *icalNode
	for _, child := range root.children {
		if child.name == "VEVENT" && child.count("RECURRENCE-ID") == 0 {
			master = child
			break
		}
	}
	if master == nil || !m.hasRecurrenceSet(master) {
		return calobject.Expansion{}
	}
	dtstart, ok := m.dateValue(master, "DTSTART", noShift)
	if !ok {
		return calobject.Expansion{}
	}
	series := m.expandSeries(root, master, start, end)
	if !series.expandable {
		return calobject.Expansion{}
	}
	instances, settled := series.instances, series.settled

	overrides := m.overrideIndex(root, master)
	expansion := calobject.Expansion{Recurs: true, Complete: settled}
	for _, instance := range instances {
		recurrenceID := instance.slotShift.apply(dtstart.instant)
		occurrenceStart := instance.shift.apply(dtstart.instant)
		occurrenceEnd := occurrenceStart.Add(instance.duration)
		if !periodInTimeRange(occurrenceStart, occurrenceEnd, start, end) {
			continue
		}
		listed := calobject.Instance{
			RecurrenceID: recurrenceID,
			Start:        occurrenceStart,
			End:          occurrenceEnd,
		}
		if override, ok := overrides.governingOverride(recurrenceID, instance.slotDate); ok {
			listed.Override = override.recurrenceID
		}
		expansion.Instances = append(expansion.Instances, listed)
	}
	slices.SortStableFunc(expansion.Instances, func(a, b calobject.Instance) int {
		if c := a.Start.Compare(b.Start); c != 0 {
			return c
		}
		return a.RecurrenceID.Compare(b.RecurrenceID)
	})
	// The overrides share the instances' budget, as the §9.6.5 expansion counts
	// the components it writes out, so one expansion lists at most
	// ical.MaxRecurrenceInstances of the two together: the earliest of them.
	expansion.Overrides = placedOverrides(m, root, overrides, series, start, end)
	if len(expansion.Instances)+len(expansion.Overrides) > ical.MaxRecurrenceInstances {
		expansion.Instances, expansion.Overrides = earliest(expansion.Instances, expansion.Overrides, ical.MaxRecurrenceInstances)
		expansion.Complete = false
	}
	if work.Exhausted() {
		expansion.Complete = false
	}
	return expansion
}

// earliest keeps the first limit of instances and overrides together, in the
// order each is sorted in, an instance before an override placed at the same
// instant.
func earliest(instances, overrides []calobject.Instance, limit int) ([]calobject.Instance, []calobject.Instance) {
	i, j := 0, 0
	for i+j < limit && (i < len(instances) || j < len(overrides)) {
		if j == len(overrides) || i < len(instances) && instances[i].Start.Compare(overrides[j].Start) <= 0 {
			i++
		} else {
			j++
		}
	}
	return instances[:i], overrides[:j]
}

// placedOverrides are the overrides of the recurrence set series expands over
// [start, end) that RFC 4791 §9.6.5 expansion writes as themselves and §9.9
// places in that range, by start: each replacing one instance, and each
// RANGE=THISANDFUTURE one standing alone. Each is placed where the expansion
// writes it, from its DTSTART to the end ical.EventEnd gives it, its series'
// length included. index already holds each override's slot, and one
// starting at or after end is passed over before its end is read.
func placedOverrides(m calendarTimeRangeMatcher, root *icalNode, index recurrenceOverrideIndex, series *seriesExpansion, start, end time.Time) []calobject.Instance {
	var placed []calobject.Instance
	for _, override := range index.overrides {
		if thisAndFutureOverride(override.node) && !m.standsAlone(root, series, override.node) {
			continue
		}
		dtstart, ok := overrideStart(m, override)
		if !ok || !dtstart.instant.Before(end) {
			continue
		}
		occurrenceEnd := m.eventOccurrenceEnd(override.node, dtstart, noShift)
		if !periodInTimeRange(dtstart.instant, occurrenceEnd, start, end) {
			continue
		}
		placed = append(placed, calobject.Instance{RecurrenceID: override.recurrenceID, Start: dtstart.instant, End: occurrenceEnd})
	}
	slices.SortFunc(placed, func(a, b calobject.Instance) int {
		if c := a.Start.Compare(b.Start); c != 0 {
			return c
		}
		return a.RecurrenceID.Compare(b.RecurrenceID)
	})
	return placed
}

// overrideStart is where override starts. One whose DTSTART repeats its
// RECURRENCE-ID -- an override changing its instance but not when it starts --
// starts at the instant already resolved, which spares resolving it again for
// each of the overrides a series may carry.
func overrideStart(m calendarTimeRangeMatcher, override recurrenceOverride) (icalTimeValue, bool) {
	start, startOK := firstICalProperty(override.node, "DTSTART")
	id, idOK := firstICalProperty(override.node, "RECURRENCE-ID")
	startTZID, idTZID := strings.TrimSpace(start.parameters["TZID"]), strings.TrimSpace(id.parameters["TZID"])
	if value := strings.TrimSpace(start.value); startOK && idOK && value == strings.TrimSpace(id.value) &&
		startTZID == idTZID {
		resolved := icalTimeValue{instant: override.recurrenceID, tzid: startTZID}
		if date, ok := ical.DateOf(value); ok && len(value) == len(ical.DateLayout) {
			resolved.isDate, resolved.date = true, date
		}
		return resolved, true
	}
	return m.dateValue(override.node, "DTSTART", noShift)
}
