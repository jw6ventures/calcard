package dav

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/jw6ventures/calcard/internal/store"
)

// ancientYearlyEvent is a yearly all-afternoon event first held on 1 June of
// year. RFC 5545 §3.3.4 admits any four-digit year, so an anniversary recorded
// from its historical origin lies centuries before every instance a client
// asks about -- further than time.Duration's ~292 years can measure.
func ancientYearlyEvent(year string) string {
	return "BEGIN:VCALENDAR\r\nVERSION:2.0\r\nPRODID:-//Test//EN\r\nBEGIN:VEVENT\r\nUID:ancient-" + year + "\r\n" +
		"DTSTAMP:20240101T000000Z\r\nDTSTART:" + year + "0601T130000Z\r\nDTEND:" + year + "0601T170000Z\r\n" +
		"RRULE:FREQ=YEARLY\r\n" +
		"BEGIN:VALARM\r\nACTION:DISPLAY\r\nDESCRIPTION:soon\r\nTRIGGER:-PT1H\r\nEND:VALARM\r\n" +
		"END:VEVENT\r\nEND:VCALENDAR\r\n"
}

var ancientYears = []string{"1604", "1700", "0002"}

func ancientEventServer(year string) *DavServer {
	calRepo := &fakeCalendarRepo{accessible: []store.CalendarAccess{
		{Calendar: store.Calendar{ID: 1, UserID: 1, Name: "History"}, Editor: true},
	}}
	return &DavServer{store: &store.Store{Calendars: calRepo, Events: &fakeEventRepo{events: map[string]*store.Event{
		"1:ancient": {CalendarID: 1, UID: "ancient-" + year, ResourceName: "ancient", ETag: "e1", RawICAL: ancientYearlyEvent(year)},
	}}}}
}

func TestCalendarQueryMatchesAYearlyEventCenturiesAfterItsDTStart(t *testing.T) {
	filters := map[string]string{
		"VEVENT time-range": `<C:comp-filter name="VEVENT">
        <C:time-range start="20260601T120000Z" end="20260601T140000Z"/>
      </C:comp-filter>`,
		"VALARM time-range": `<C:comp-filter name="VEVENT"><C:comp-filter name="VALARM">
        <C:time-range start="20260601T115000Z" end="20260601T121000Z"/>
      </C:comp-filter></C:comp-filter>`,
		"DTEND prop-filter time-range": `<C:comp-filter name="VEVENT"><C:prop-filter name="DTEND">
        <C:time-range start="20260601T165000Z" end="20260601T171000Z"/>
      </C:prop-filter></C:comp-filter>`,
	}
	for _, year := range ancientYears {
		for name, filter := range filters {
			t.Run(year+"/"+name, func(t *testing.T) {
				body := `<C:calendar-query xmlns:D="DAV:" xmlns:C="urn:ietf:params:xml:ns:caldav">
  <D:prop><D:getetag/></D:prop>
  <C:filter><C:comp-filter name="VCALENDAR">` + filter + `</C:comp-filter></C:filter>
</C:calendar-query>`
				rr := httptest.NewRecorder()
				ancientEventServer(year).Report(rr, freeBusyRequestFor("/dav/calendars/1/", body, &store.User{ID: 1}))
				if rr.Code != http.StatusMultiStatus {
					t.Fatalf("status = %d, want 207: %.500s", rr.Code, rr.Body.String())
				}
				if !strings.Contains(rr.Body.String(), "ancient.ics") {
					t.Fatalf("the 2026 instance of an event first held in %s was not matched: %.500s", year, rr.Body.String())
				}
			})
		}
	}
}

func TestCalendarDataExpandsAYearlyEventCenturiesAfterItsDTStart(t *testing.T) {
	for _, year := range ancientYears {
		t.Run(year, func(t *testing.T) {
			got, err := filterICalendarData(ancientYearlyEvent(year), newCalendarDataProjection(&calendarDataEl{Expand: &calendarRange{
				Start: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC),
				End:   time.Date(2027, 1, 1, 0, 0, 0, 0, time.UTC),
			}}, floatingZone{}))
			if err != nil {
				t.Fatalf("filterICalendarData() err = %v", err)
			}
			for _, want := range []string{"DTSTART:20260601T130000Z", "DTEND:20260601T170000Z", "RECURRENCE-ID:20260601T130000Z"} {
				if !strings.Contains(got, want) {
					t.Fatalf("expanded 2026 instance lacks %s:\n%s", want, got)
				}
			}
		})
	}
}

func TestFreeBusyPublishesAYearlyEventCenturiesAfterItsDTStart(t *testing.T) {
	for _, year := range ancientYears {
		t.Run(year, func(t *testing.T) {
			body := `<C:free-busy-query xmlns:C="urn:ietf:params:xml:ns:caldav">
  <C:time-range start="20260101T000000Z" end="20270101T000000Z"/>
</C:free-busy-query>`
			rr := httptest.NewRecorder()
			ancientEventServer(year).Report(rr, freeBusyRequestFor("/dav/calendars/1/", body, &store.User{ID: 1}))
			if rr.Code != http.StatusOK {
				t.Fatalf("status = %d, want 200: %.500s", rr.Code, rr.Body.String())
			}
			assertPublishedFreeBusy(t, parsedFreeBusyLines(t, rr.Body.String()), []string{
				"FREEBUSY:20260601T130000Z/20260601T170000Z",
			})
		})
	}
}
