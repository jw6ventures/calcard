package dav

import (
	"testing"

	"github.com/jw6ventures/calcard/internal/store"
)

// RFC 5545 §3.3.10 makes UNTIL inclusive, and a DATE UNTIL on a timed series
// names the whole of its day, so the series' instance on that date is in the
// set.
func TestCalendarQueryMatchesTheInstanceOnADateUntil(t *testing.T) {
	raw := producerObject("VEVENT", "UID:e", "DTSTART:20260105T090000Z", "DTEND:20260105T100000Z", "RRULE:FREQ=WEEKLY;UNTIL=20260302")
	event := store.Event{UID: "e", RawICAL: raw}
	if !filterMatches(t, event, calQueryWithTimeRange("20260302T000000Z", "20260303T000000Z"), floatingZone{}) {
		t.Error("calendar-query missed the Monday 2 March instance")
	}
	if filterMatches(t, event, calQueryWithTimeRange("20260303T000000Z", "20260401T000000Z"), floatingZone{}) {
		t.Error("calendar-query matched an instance after the UNTIL date")
	}
}
