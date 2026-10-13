package ical

import (
	"strings"
	"time"
)

// ObjectTimes resolves the date-valued properties of one calendar object to the
// instants they name, as the DAV layer's matcher reads them, so the columns a
// writer stores for an object agree with the reports that read it. A TZID is
// read through the VTIMEZONE the object ships for it, which RFC 4791 §4.1 makes
// authoritative, unless that is a definition the server wrote for a zone the
// host defines (TimezoneComponent.HostDefinition); else through the host's zone of that name. A value carrying a
// TZID neither defines, and a floating one, are read as UTC, the RFC 4791 §7.3
// fallback for a reader with no calendar timezone to read them in.
//
// Each TZID's definition is read once, and every definition's rules are
// walked, where no listing answers them, on one ObservanceWork. It is not safe
// for concurrent use.
type ObjectTimes struct {
	lines     []string
	onsets    ObservanceOnsets
	work      *ObservanceWork
	timezones map[string]objectTimezone
	zones     map[string]tzidDefinition
	// components holds the object's top-level VEVENT, VTODO and VJOURNAL
	// components once recurringComponents has read them.
	components []Component
	read       bool
}

// NewObjectTimes resolves the dates of the object whose unfolded content lines
// are lines.
func NewObjectTimes(lines []string) *ObjectTimes {
	return &ObjectTimes{lines: lines, work: NewObservanceWork()}
}

// NewObjectTimesSharingOnsets is NewObjectTimes reading the rules of the
// object's VTIMEZONEs through onsets, a listing shared beyond this object, such
// as one serving every object a process reads. Definitions starting centuries
// back, as Exchange's do from 1601, are then listed once rather than once per
// object.
func NewObjectTimesSharingOnsets(lines []string, onsets ObservanceOnsets) *ObjectTimes {
	return &ObjectTimes{lines: lines, onsets: onsets, work: NewObservanceWork()}
}

// ResolvePropertyTime resolves one date-valued content line of the object. It
// is a PropertyTimeResolver.
func (o *ObjectTimes) ResolvePropertyTime(keyPart, value string) (time.Time, bool) {
	value = strings.TrimSpace(value)
	tzid, _ := PropertyParam(keyPart, "TZID")
	tzid = strings.TrimSpace(tzid)
	if tzid == "" || hasZoneSuffix(value) {
		parsed, err := ParseDateTime(value)
		return parsed, err == nil
	}
	wall, err := ParseDateTime(value)
	if err != nil {
		return time.Time{}, false
	}
	return o.zone(tzid).instant(wall), true
}

// recurringComponents is the object's top-level VEVENT, VTODO and VJOURNAL
// components, read once for every reader of the object's recurrence. The
// readers do not modify them.
func (o *ObjectTimes) recurringComponents() []Component {
	if !o.read {
		o.components = topLevelComponentsFromLines(o.lines, isRecurringComponentName)
		o.read = true
	}
	return o.components
}

func (o *ObjectTimes) zone(tzid string) tzidDefinition {
	if zone, ok := o.zones[tzid]; ok {
		return zone
	}
	if o.timezones == nil {
		o.timezones = objectTimezones(o.lines)
	}
	zone := resolveTZIDIn(o.timezones, tzid, o.onsets, o.work)
	if o.zones == nil {
		o.zones = make(map[string]tzidDefinition)
	}
	o.zones[tzid] = zone
	return zone
}

// StoredEventEnd is the end a writer stores for an event whose DTSTART resolves
// to start, a DATE when startIsDate, and whose DTEND resolves to end; either is
// nil when the event has none that resolves. It is the end the reports read the
// event with, per EventEnd: the DTEND when it ends the event, else -- whether
// the DTEND is absent or cannot end the event -- the end of a DATE start's day
// or the DATE-TIME start itself. Where a DURATION decides instead, which the
// stored columns do not read, there is no end to store, and a reader narrowing
// by the column keeps the event for every range.
func StoredEventEnd(start *time.Time, startIsDate bool, end *time.Time, hasDuration bool) *time.Time {
	if start == nil {
		return end
	}
	extent := EventEnd{Start: *start, StartIsDate: startIsDate}
	if end != nil {
		extent.End, extent.HasEnd = *end, true
	}
	if extent.UsableEnd() {
		return end
	}
	if hasDuration {
		return nil
	}
	stored := extent.OccurrenceEnd(func(date time.Time) time.Time {
		return start.AddDate(0, 0, CivilDaysBetween(CivilDate(*start), date))
	})
	return &stored
}
