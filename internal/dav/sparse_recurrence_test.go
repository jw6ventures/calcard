package dav

import (
	"fmt"
	"github.com/jw6ventures/calcard/internal/auth"
	"github.com/jw6ventures/calcard/internal/store"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestSparseRecurrenceTimeRange(t *testing.T) {
	raw := "BEGIN:VCALENDAR\r\nVERSION:2.0\r\nPRODID:-//Audit//EN\r\nBEGIN:VEVENT\r\nUID:sparse\r\nDTSTAMP:20260101T000000Z\r\nDTSTART:20260101T090000Z\r\nDTEND:20260101T100000Z\r\nRRULE:FREQ=MINUTELY;BYMONTH=1;BYMONTHDAY=1;BYHOUR=9;BYMINUTE=0;COUNT=2\r\nEND:VEVENT\r\nEND:VCALENDAR\r\n"
	if _, fault := validateCalendarObjectForStorage(raw, nil); fault != nil {
		t.Fatalf("valid payload refused: %+v", fault)
	}
	root, err := parseICalendarObject(raw)
	if err != nil {
		t.Fatal(err)
	}
	m := newCalendarTimeRangeMatcher(raw, root, floatingZone{})
	start := time.Date(2027, 1, 1, 9, 0, 0, 0, time.UTC)
	if !m.componentSetInTimeRange(root.children[0], root, start, start.Add(time.Hour)) {
		t.Fatal("accepted two-occurrence series loses its 2027 occurrence in the time-range matcher")
	}
}

func TestSparseRecurrenceReports(t *testing.T) {
	const raw = "BEGIN:VCALENDAR\r\nVERSION:2.0\r\nPRODID:-//CalCard//EN\r\nBEGIN:VEVENT\r\nUID:sparse\r\nDTSTAMP:20260101T000000Z\r\nDTSTART:20260101T090000Z\r\nDTEND:20260101T100000Z\r\nRRULE:FREQ=MINUTELY;BYMONTH=1;BYMONTHDAY=1;BYHOUR=9;BYMINUTE=0;COUNT=2\r\nEND:VEVENT\r\nEND:VCALENDAR\r\n"
	for _, test := range []struct {
		name, body, want string
		status           int
	}{
		{"query", `<C:calendar-query xmlns:D="DAV:" xmlns:C="urn:ietf:params:xml:ns:caldav"><D:prop><D:getetag/></D:prop><C:filter><C:comp-filter name="VCALENDAR"><C:comp-filter name="VEVENT"><C:time-range start="20270101T090000Z" end="20270101T100000Z"/></C:comp-filter></C:comp-filter></C:filter></C:calendar-query>`, "sparse.ics", 207},
		{"free-busy", `<C:free-busy-query xmlns:C="urn:ietf:params:xml:ns:caldav"><C:time-range start="20270101T090000Z" end="20270101T100000Z"/></C:free-busy-query>`, "FREEBUSY:20270101T090000Z/20270101T100000Z", 200},
		{"expand", `<C:calendar-query xmlns:D="DAV:" xmlns:C="urn:ietf:params:xml:ns:caldav"><D:prop><C:calendar-data><C:expand start="20270101T090000Z" end="20270101T100000Z"/></C:calendar-data></D:prop><C:filter><C:comp-filter name="VCALENDAR"><C:comp-filter name="VEVENT"/></C:comp-filter></C:filter></C:calendar-query>`, "DTSTART:20270101T090000Z", 207},
	} {
		t.Run(test.name, func(t *testing.T) {
			h := hostileRecurrenceServer()
			h.store.Events = &fakeEventRepo{events: map[string]*store.Event{"1:sparse": {ID: 1, CalendarID: 1, UID: "sparse", ResourceName: "sparse", ETag: "e", RawICAL: raw}}}
			req := httptest.NewRequest("REPORT", "/dav/calendars/1/", strings.NewReader(test.body))
			req.Header.Set("Depth", "1")
			rr := httptest.NewRecorder()
			h.Report(rr, req.WithContext(auth.WithUser(req.Context(), &store.User{ID: 1})))
			if rr.Code != test.status || !strings.Contains(rr.Body.String(), test.want) {
				t.Fatalf("%d %s", rr.Code, rr.Body.String())
			}
		})
	}
}

func TestRecurrenceReportsRejectIncompleteExpansion(t *testing.T) {
	values := func(n int) string {
		parts := make([]string, n)
		for i := range parts {
			parts[i] = fmt.Sprint(i)
		}
		return strings.Join(parts, ",")
	}
	raw := "BEGIN:VCALENDAR\r\nVERSION:2.0\r\nBEGIN:VEVENT\r\nUID:large\r\nDTSTART:20260101T090000Z\r\nDTEND:20260101T100000Z\r\nRRULE:FREQ=YEARLY;BYDAY=MO,TU,WE,TH,FR,SA,SU;BYHOUR=" + values(24) + ";BYMINUTE=" + values(60) + ";BYSECOND=" + values(60) + ";BYSETPOS=-1;COUNT=2\r\nEND:VEVENT\r\nEND:VCALENDAR\r\n"
	for _, kind := range []string{"query", "free-busy", "expand"} {
		t.Run(kind, func(t *testing.T) {
			h := hostileRecurrenceServer()
			h.store.Events = &fakeEventRepo{events: map[string]*store.Event{"1:large": {ID: 1, CalendarID: 1, UID: "large", ResourceName: "large", ETag: "e", RawICAL: raw}}}
			timeRange := `<C:time-range start="20260101T000000Z" end="20270101T000000Z"/>`
			body := `<C:calendar-query xmlns:D="DAV:" xmlns:C="urn:ietf:params:xml:ns:caldav"><D:prop><D:getetag/></D:prop><C:filter><C:comp-filter name="VCALENDAR"><C:comp-filter name="VEVENT">` + timeRange + `</C:comp-filter></C:comp-filter></C:filter></C:calendar-query>`
			if kind == "free-busy" {
				body = `<C:free-busy-query xmlns:C="urn:ietf:params:xml:ns:caldav">` + timeRange + `</C:free-busy-query>`
			}
			if kind == "expand" {
				body = `<C:calendar-query xmlns:D="DAV:" xmlns:C="urn:ietf:params:xml:ns:caldav"><D:prop><C:calendar-data><C:expand start="20260101T000000Z" end="20270101T000000Z"/></C:calendar-data></D:prop><C:filter><C:comp-filter name="VCALENDAR"><C:comp-filter name="VEVENT"/></C:comp-filter></C:filter></C:calendar-query>`
			}
			req := httptest.NewRequest("REPORT", "/dav/calendars/1/", strings.NewReader(body))
			req.Header.Set("Depth", "1")
			rr := httptest.NewRecorder()
			h.Report(rr, req.WithContext(auth.WithUser(req.Context(), &store.User{ID: 1})))
			if kind == "expand" {
				if rr.Code != 207 || !strings.Contains(rr.Body.String(), "500 Internal Server Error") || strings.Contains(rr.Body.String(), "BEGIN:VEVENT") {
					t.Fatalf("incomplete expansion published: %d %s", rr.Code, rr.Body.String())
				}
			} else if rr.Code != 500 {
				t.Fatalf("incomplete %s succeeded: %d %s", kind, rr.Code, rr.Body.String())
			}
		})
	}
}
