package dav

import (
	"slices"
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
// ical.MaxRecurrenceInstances of them, the most one expansion returns. It is a
// calobject.Expander.
//
// Each instance is placed from the period the expansion generates for it,
// which is the extent the §9.6.5 expansion gives the instance it writes out,
// rather than by writing it out: a view lists every instance of a month for
// each series it shows.
func EventInstances(raw string, start, end time.Time) calobject.Expansion {
	root, err := parseStoredICalendarObject(raw)
	if err != nil {
		return calobject.Expansion{}
	}
	m := newCalendarTimeRangeMatcher(raw, root, floatingZone{})
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
	instances, expandable, settled := m.recurrenceInstances(master, master, start, end)
	if !expandable {
		return calobject.Expansion{}
	}

	overrides := m.overrideIndex(root, master)
	expansion := calobject.Expansion{Recurs: true, Complete: settled}
	for _, instance := range instances {
		occurrenceStart := instance.shift.apply(dtstart.instant)
		occurrenceEnd := occurrenceStart.Add(instance.duration)
		if !periodInTimeRange(occurrenceStart, occurrenceEnd, start, end) {
			continue
		}
		listed := calobject.Instance{
			RecurrenceID: instance.slotShift.apply(dtstart.instant),
			Start:        occurrenceStart,
			End:          occurrenceEnd,
		}
		if override, ok := overrides.governingOverride(listed.RecurrenceID); ok {
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
	if len(expansion.Instances) > ical.MaxRecurrenceInstances {
		expansion.Instances = expansion.Instances[:ical.MaxRecurrenceInstances]
		expansion.Complete = false
	}
	return expansion
}
