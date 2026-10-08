package dav

import (
	"strings"
	"testing"

	"github.com/jw6ventures/calcard/internal/ical"
)

// An object stored before PUT stripped byte order marks, or kept with one by a
// COPY or MOVE, is read past its leading mark by every report, as the
// validator reads it: calendar-query matches it by its times and components,
// free-busy publishes its busy time, and an expansion writes its instances.
func TestReportsReadAStoredObjectPastItsByteOrderMark(t *testing.T) {
	raw := ical.ByteOrderMark + zeroLengthCalendar(componentLines("VEVENT",
		"UID:instant", "DTSTAMP:20250101T000000Z", "SUMMARY:daily",
		"DTSTART:20250301T100000Z", "DTEND:20250301T120000Z", "RRULE:FREQ=DAILY;COUNT=3"))

	code, body := zeroLengthReport(t, raw, timeRangeQuery("20250302T103000Z", "20250302T110000Z"))
	if code != 207 || !strings.Contains(body, "instant.ics") {
		t.Errorf("calendar-query missed the 2 March instance: %d\n%s", code, body)
	}
	code, body = zeroLengthReport(t, raw, timeRangeQuery("20250310T000000Z", "20250311T000000Z"))
	if code != 207 || strings.Contains(body, "instant.ics") {
		t.Errorf("calendar-query matched a range past the series: %d\n%s", code, body)
	}
	todos := `<C:calendar-query xmlns:D="DAV:" xmlns:C="urn:ietf:params:xml:ns:caldav"><D:prop><D:getetag/></D:prop>` +
		`<C:filter><C:comp-filter name="VCALENDAR"><C:comp-filter name="VTODO"/></C:comp-filter></C:filter></C:calendar-query>`
	code, body = zeroLengthReport(t, raw, todos)
	if code != 207 || strings.Contains(body, "instant.ics") {
		t.Errorf("a VTODO calendar-query matched the event: %d\n%s", code, body)
	}

	code, body = zeroLengthReport(t, raw, freeBusyQuery("20250301T000000Z", "20250305T000000Z"))
	if code != 200 {
		t.Fatalf("free-busy status = %d, body %s", code, body)
	}
	assertPublishedFreeBusy(t, parsedFreeBusyLines(t, body), []string{
		"FREEBUSY:20250301T100000Z/20250301T120000Z",
		"FREEBUSY:20250302T100000Z/20250302T120000Z",
		"FREEBUSY:20250303T100000Z/20250303T120000Z",
	})

	code, body = zeroLengthReport(t, raw, calendarDataQuery(`<C:expand start="20250302T000000Z" end="20250303T000000Z"/>`))
	if code != 207 {
		t.Fatalf("expand status = %d, body %s", code, body)
	}
	if !strings.Contains(body, "RECURRENCE-ID:20250302T100000Z") || strings.Contains(body, "RRULE") || strings.Contains(body, ical.ByteOrderMark) {
		t.Errorf("the expansion did not write the 2 March instance alone:\n%s", body)
	}
}
