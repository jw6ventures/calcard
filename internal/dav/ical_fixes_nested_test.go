package dav

import (
	"testing"

	"github.com/jw6ventures/calcard/internal/store"
)

// A stored object whose VCALENDAR is opened again inside itself is read as one
// calendar, so the recurring VEVENT inside the nested opening is expanded like
// any other.
func TestReportsExpandARecurringEventInsideANestedVCalendar(t *testing.T) {
	raw := "BEGIN:VCALENDAR\r\nVERSION:2.0\r\nBEGIN:VCALENDAR\r\n" +
		"BEGIN:VEVENT\r\nUID:a\r\nDTSTART:20260105T090000Z\r\nDTEND:20260105T100000Z\r\nRRULE:FREQ=WEEKLY;COUNT=3\r\nEND:VEVENT\r\n" +
		"END:VCALENDAR\r\nEND:VCALENDAR\r\n"
	event := store.Event{UID: "a", RawICAL: raw}
	if !filterMatches(event, calQueryWithTimeRange("20260112T000000Z", "20260113T000000Z"), floatingZone{}) {
		t.Error("calendar-query missed the 12 January instance")
	}
	if filterMatches(event, calQueryWithTimeRange("20260126T000000Z", "20260127T000000Z"), floatingZone{}) {
		t.Error("calendar-query matched past the rule's COUNT")
	}
	body := mustReportText((&DavServer{}).generateFreeBusy(t.Context(), freeBusyCandidates([]store.Event{event}, floatingZone{}),
		&timeRange{Start: "20260112T000000Z", End: "20260113T000000Z"}))
	assertPublishedFreeBusy(t, parsedFreeBusyLines(t, body), []string{"FREEBUSY:20260112T090000Z/20260112T100000Z"})
}
