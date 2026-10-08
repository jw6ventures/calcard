package dav

import (
	"testing"

	"github.com/jw6ventures/calcard/internal/store"
)

// A zero-length RANGE=THISANDFUTURE override makes every instance it moves an
// instant, rather than giving them the master's length.
func TestCalendarQueryPlacesInstancesMovedByAZeroLengthOverride(t *testing.T) {
	for name, length := range map[string]string{
		"DTEND equal to DTSTART": "DTEND:20260112T110000Z",
		"zero DURATION":          "DURATION:PT0S",
	} {
		raw := buildCalendarObject(
			buildComponent("VEVENT", "UID:e", "DTSTART:20260105T090000Z", "DTEND:20260105T100000Z", "RRULE:FREQ=WEEKLY;COUNT=4"),
			buildComponent("VEVENT", "UID:e", "RECURRENCE-ID;RANGE=THISANDFUTURE:20260112T090000Z", "DTSTART:20260112T110000Z", length),
		)
		event := store.Event{UID: "e", RawICAL: raw}
		if !filterMatches(event, calQueryWithTimeRange("20260119T103000Z", "20260119T113000Z"), floatingZone{}) {
			t.Errorf("%s: calendar-query missed the moved 19 January instance", name)
		}
		if filterMatches(event, calQueryWithTimeRange("20260119T113000Z", "20260119T120000Z"), floatingZone{}) {
			t.Errorf("%s: the moved 19 January instance still lasts the master's hour", name)
		}
	}
}
