package dav

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/jw6ventures/calcard/internal/store"
)

// quotedColonTZIDObject names its zone the way Exchange does: a quoted TZID
// holding a colon, which RFC 5545 §3.2 requires to be quoted precisely because
// of that colon. The EXDATE carries the same TZID, so a reader splitting the
// content line at the first colon of any kind loses the exception silently.
const quotedColonTZIDObject = "BEGIN:VCALENDAR\r\nVERSION:2.0\r\nPRODID:-//Test//EN\r\n" +
	"BEGIN:VTIMEZONE\r\nTZID:(UTC-05:00) Eastern\r\n" +
	"BEGIN:STANDARD\r\nDTSTART:19701101T020000\r\nRRULE:FREQ=YEARLY;BYMONTH=11;BYDAY=1SU\r\n" +
	"TZOFFSETFROM:-0400\r\nTZOFFSETTO:-0500\r\nTZNAME:EST\r\nEND:STANDARD\r\n" +
	"BEGIN:DAYLIGHT\r\nDTSTART:19700308T020000\r\nRRULE:FREQ=YEARLY;BYMONTH=3;BYDAY=2SU\r\n" +
	"TZOFFSETFROM:-0500\r\nTZOFFSETTO:-0400\r\nTZNAME:EDT\r\nEND:DAYLIGHT\r\nEND:VTIMEZONE\r\n" +
	"BEGIN:VEVENT\r\nUID:standup\r\nDTSTAMP:20240101T000000Z\r\n" +
	"DTSTART;TZID=\"(UTC-05:00) Eastern\":20240108T090000\r\n" +
	"DTEND;TZID=\"(UTC-05:00) Eastern\":20240108T093000\r\n" +
	"RRULE:FREQ=DAILY;COUNT=3\r\n" +
	"EXDATE;TZID=\"(UTC-05:00) Eastern\":20240109T090000\r\n" +
	"END:VEVENT\r\nEND:VCALENDAR\r\n"

func TestPutAcceptsAQuotedTZIDContainingAColon(t *testing.T) {
	validated, fault := validateCalendarObjectForStorage(quotedColonTZIDObject, nil)
	if fault != nil {
		t.Fatalf("validateCalendarObjectForStorage() fault = %+v, want the object accepted", fault)
	}
	if validated.Analysis.Metadata.DTStart == nil {
		t.Fatal("the DTSTART carrying the quoted TZID was not read into the stored metadata")
	}
	if validated.Analysis.Metadata.DTEnd == nil {
		t.Fatal("the DTEND carrying the quoted TZID was not read into the stored metadata")
	}
}

func TestFreeBusyAppliesAnExDateWithAQuotedTZIDContainingAColon(t *testing.T) {
	calRepo := &fakeCalendarRepo{
		accessible: []store.CalendarAccess{
			{Calendar: store.Calendar{ID: 1, UserID: 1, Name: "Work"}, Editor: true},
		},
	}
	h := &DavServer{store: &store.Store{Calendars: calRepo, Events: &fakeEventRepo{
		events: map[string]*store.Event{"1:standup": {
			CalendarID: 1, UID: "standup", ResourceName: "standup", ETag: "e1",
			RawICAL: quotedColonTZIDObject,
		}},
	}}}
	body := `<C:free-busy-query xmlns:C="urn:ietf:params:xml:ns:caldav">
  <C:time-range start="20240101T000000Z" end="20240201T000000Z"/>
</C:free-busy-query>`

	rr := httptest.NewRecorder()
	h.Report(rr, freeBusyRequestFor("/dav/calendars/1/", body, &store.User{ID: 1}))

	if rr.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200: %s", rr.Code, rr.Body.String())
	}
	// EST is UTC-5 in January, and the EXDATE removes the second occurrence.
	assertPublishedFreeBusy(t, parsedFreeBusyLines(t, rr.Body.String()), []string{
		"FREEBUSY:20240108T140000Z/20240108T143000Z",
		"FREEBUSY:20240110T140000Z/20240110T143000Z",
	})
}
