package dav

import (
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/jw6ventures/calcard/internal/auth"
	"github.com/jw6ventures/calcard/internal/ical"
	"github.com/jw6ventures/calcard/internal/store"
)

// openEndedQuery is a calendar-query whose VEVENT comp-filter carries the given
// time-range attributes verbatim, so a case can omit one the way RFC 4791 §9.9
// permits rather than spell an infinity the grammar would then have to reject.
func openEndedQuery(attributes string) string {
	return `<?xml version="1.0" encoding="utf-8"?>
<C:calendar-query xmlns:D="DAV:" xmlns:C="urn:ietf:params:xml:ns:caldav">
  <D:prop><D:getetag/></D:prop>
  <C:filter>
    <C:comp-filter name="VCALENDAR">
      <C:comp-filter name="VEVENT">
        <C:time-range ` + attributes + `/>
      </C:comp-filter>
    </C:comp-filter>
  </C:filter>
</C:calendar-query>`
}

// reportOnCalendar runs one REPORT against a single-resource calendar and
// returns the recorder, so a case states the resource and the filter rather
// than the fixture wiring they share.
func reportOnCalendar(t *testing.T, raw, body string) *httptest.ResponseRecorder {
	t.Helper()
	h := hostileRecurrenceServer()
	h.store.Events = &fakeEventRepo{events: map[string]*store.Event{
		"1:subject": {ID: 1, CalendarID: 1, UID: "subject", ResourceName: "subject", ETag: "e", RawICAL: raw},
	}}
	req := httptest.NewRequest("REPORT", "/dav/calendars/1/", strings.NewReader(body))
	req.Header.Set("Depth", "1")
	rr := httptest.NewRecorder()
	h.Report(rr, req.WithContext(auth.WithUser(req.Context(), &store.User{ID: 1})))
	return rr
}

func unboundedDailyEvent(dtstart string) string {
	return "BEGIN:VCALENDAR\r\nVERSION:2.0\r\nPRODID:-//Test//EN\r\nBEGIN:VEVENT\r\nUID:subject\r\n" +
		"DTSTAMP:20240101T000000Z\r\nDTSTART:" + dtstart + "\r\nDURATION:PT1H\r\nRRULE:FREQ=DAILY\r\n" +
		"END:VEVENT\r\nEND:VCALENDAR\r\n"
}

// RFC 4791 §9.9 states that either attribute of CALDAV:time-range may be
// omitted, meaning an infinity on that side. A recurring resource in scope must
// not turn that into a failure of the whole report: every response the REPORT
// would otherwise carry is lost, so a client that syncs with a half-open range
// cannot read the collection at all.
func TestOpenEndedTimeRangeAnswersTheReport(t *testing.T) {
	counted := "BEGIN:VCALENDAR\r\nVERSION:2.0\r\nPRODID:-//Test//EN\r\nBEGIN:VEVENT\r\nUID:subject\r\n" +
		"DTSTAMP:20200101T000000Z\r\nDTSTART:20200101T090000Z\r\nDURATION:PT1H\r\nRRULE:FREQ=DAILY;COUNT=3\r\n" +
		"END:VEVENT\r\nEND:VCALENDAR\r\n"

	for _, test := range []struct {
		name       string
		raw        string
		attributes string
		want       bool
	}{
		{name: "open end reaches an unbounded rule", raw: unboundedDailyEvent("20240101T090000Z"), attributes: `start="20240601T000000Z"`, want: true},
		{name: "open end reaches a rule starting after it", raw: unboundedDailyEvent("20300101T090000Z"), attributes: `start="20240601T000000Z"`, want: true},
		{name: "open start reaches an unbounded rule", raw: unboundedDailyEvent("20240101T090000Z"), attributes: `end="20240601T000000Z"`, want: true},
		{name: "open end excludes a finished rule", raw: counted, attributes: `start="20240601T000000Z"`, want: false},
		{name: "open start excludes a rule starting after it", raw: unboundedDailyEvent("20300101T090000Z"), attributes: `end="20240601T000000Z"`, want: false},
	} {
		t.Run(test.name, func(t *testing.T) {
			rr := reportOnCalendar(t, test.raw, openEndedQuery(test.attributes))
			if rr.Code != 207 {
				t.Fatalf("REPORT = %d, want 207: %s", rr.Code, rr.Body.String())
			}
			if got := strings.Contains(rr.Body.String(), "subject.ics"); got != test.want {
				t.Fatalf("resource reported = %t, want %t: %s", got, test.want, rr.Body.String())
			}
		})
	}
}

// §7.10 answers with periods rather than with hrefs, so an open end is not a
// wider question there but an unanswerable one. It is refused on the request,
// before any collection is read, rather than failing partway through building a
// body that could never be finished.
func TestOpenEndedFreeBusyIsRefusedRatherThanAttempted(t *testing.T) {
	raw := unboundedDailyEvent("20240101T090000Z")
	for _, test := range []struct {
		name       string
		attributes string
		status     int
	}{
		{name: "open end is refused", attributes: `start="20240101T000000Z"`, status: 400},
		{name: "open start is answered", attributes: `end="20240201T000000Z"`, status: 200},
		{name: "a closed range is answered", attributes: `start="20240101T000000Z" end="20240201T000000Z"`, status: 200},
	} {
		t.Run(test.name, func(t *testing.T) {
			body := `<C:free-busy-query xmlns:C="urn:ietf:params:xml:ns:caldav"><C:time-range ` + test.attributes + `/></C:free-busy-query>`
			rr := reportOnCalendar(t, raw, body)
			if rr.Code != test.status {
				t.Fatalf("free-busy = %d, want %d: %s", rr.Code, test.status, rr.Body.String())
			}
		})
	}
}

// openTimeRangeEnd tells an omitted attribute from a spelled one by value alone,
// which holds only while no spelled value can reach the sentinel. RFC 4791 §7.9
// bounds every spelled endpoint by CALDAV:max-date-time, so this is a property
// of the two constants and is asserted rather than assumed.
func TestOpenTimeRangeEndIsUnreachableFromASpelledValue(t *testing.T) {
	minTime, maxTime := ical.DateLimits()
	if !openTimeRangeEnd(ical.RecurrenceUntilSentinel) {
		t.Fatalf("the substituted end %s is within CALDAV:max-date-time %s, so an omitted attribute reads as a spelled one", ical.RecurrenceUntilSentinel, maxTime)
	}
	if openTimeRangeEnd(maxTime) {
		t.Fatalf("CALDAV:max-date-time %s reads as an omitted attribute", maxTime)
	}
	if openTimeRangeEnd(minTime) {
		t.Fatalf("CALDAV:min-date-time %s reads as an omitted attribute", minTime)
	}
}

// The same range against the frequency that makes expansion expensive. The
// budget still has to hold: an omitted attribute widens the range, so it must
// not become a way to ask for work a spelled range would be refused.
func TestOpenEndedTimeRangeStaysBoundedForSubSecondRecurrence(t *testing.T) {
	raw := "BEGIN:VCALENDAR\r\nVERSION:2.0\r\nPRODID:-//Test//EN\r\nBEGIN:VEVENT\r\nUID:subject\r\n" +
		"DTSTAMP:20240101T000000Z\r\nDTSTART:20240101T000000Z\r\nDTEND:20240101T000001Z\r\nRRULE:FREQ=SECONDLY\r\n" +
		"END:VEVENT\r\nEND:VCALENDAR\r\n"

	done := make(chan *httptest.ResponseRecorder, 1)
	go func() { done <- reportOnCalendar(t, raw, openEndedQuery(`start="20240101T000000Z"`)) }()
	select {
	case rr := <-done:
		if rr.Code != 207 {
			t.Fatalf("REPORT = %d, want 207: %s", rr.Code, rr.Body.String())
		}
		if !strings.Contains(rr.Body.String(), "subject.ics") {
			t.Fatalf("a resource that does occur in the range was not reported: %s", rr.Body.String())
		}
	case <-time.After(10 * time.Second):
		t.Fatal("REPORT did not return within 10s")
	}
}
