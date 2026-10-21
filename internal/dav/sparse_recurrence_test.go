package dav

import (
	"fmt"
	"github.com/jw6ventures/calcard/internal/auth"
	"github.com/jw6ventures/calcard/internal/store"
	"net/http"
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

// A period too large to generate leaves the recurrence set only partly known.
// Each report answers that within its own contract: §9.9 needs one intersecting
// instance and keeps a resource it cannot rule out, §7.10 owes every busy period
// of the range and refuses with the §7.8 postcondition rather than publish a
// prefix that would report the rest of the range as free, and §9.6.5 owes the
// whole instance set and fails that one resource inside the multistatus.
func TestRecurrenceReportsHandleIncompleteExpansion(t *testing.T) {
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
			switch kind {
			case "expand":
				if rr.Code != 207 || !strings.Contains(rr.Body.String(), "500 Internal Server Error") || strings.Contains(rr.Body.String(), "BEGIN:VEVENT") {
					t.Fatalf("incomplete expansion published: %d %s", rr.Code, rr.Body.String())
				}
			case "free-busy":
				if rr.Code != 403 || !strings.Contains(rr.Body.String(), "number-of-matches-within-limits") {
					t.Fatalf("incomplete free-busy answered %d %s, want a 403 carrying the §7.8 postcondition", rr.Code, rr.Body.String())
				}
				if strings.Contains(rr.Body.String(), "FREEBUSY") {
					t.Fatalf("a refused free-busy published busy time: %s", rr.Body.String())
				}
			default:
				// BYSETPOS=-1 does select the last candidate of 2026, so the
				// resource genuinely matches the range and keeping it is the
				// §9.9 answer as well as the conservative one.
				if rr.Code != 207 || !strings.Contains(rr.Body.String(), "large.ics") {
					t.Fatalf("a resource that could not be fully expanded was dropped: %d %s", rr.Code, rr.Body.String())
				}
			}
		})
	}
}

// requirePrompt runs evaluate and fails the test unless it returns within half
// a second, the most one evaluation of a recurrence or zone may cost. A
// race-detector build is held only to requirePromptWithin's hard limit.
func requirePrompt(t *testing.T, evaluate func()) {
	t.Helper()
	requirePromptWithin(t, 500*time.Millisecond, evaluate)
}

// requirePromptWithin runs evaluate and fails the test unless it returns
// within budget. evaluate runs on its own goroutine, which may outlive the
// test, so it leaves its assertions to the caller. The budget is not asserted
// under the race detector, whose per-access cost would decide it; a hard limit
// is.
func requirePromptWithin(t *testing.T, budget time.Duration, evaluate func()) {
	t.Helper()
	const hardLimit = 10 * time.Second
	done := make(chan time.Duration, 1)
	go func() {
		started := time.Now()
		evaluate()
		done <- time.Since(started)
	}()
	select {
	case elapsed := <-done:
		if elapsed > budget && !raceDetectorEnabled {
			t.Errorf("took %v, want at most %v", elapsed, budget)
		}
	case <-time.After(hardLimit):
		t.Fatalf("did not return within %v", hardLimit)
	}
}

// A rule whose BY parts can never hold together -- the twenty-sixth weekday of
// a year always falls after the 23rd of June -- describes the DTSTART alone. On
// the PUT gate, as a VTIMEZONE observance, and stored under free-busy and an
// open-ended calendar-query, that has to be answered exactly and promptly: a
// few hundred bytes of such a rule can cost minutes of CPU on each when every
// candidate day is tried, and repeating its BYDAY list multiplies that. A daily or hourly rule for a date that does not exist,
// the 30th of February, is the same answer reached through the scan that
// follows a rule day by day.
func TestNeverMatchingRecurrenceIsAnsweredPromptly(t *testing.T) {
	monthDays := make([]string, 0, 22)
	for day := 2; day <= 23; day++ {
		monthDays = append(monthDays, fmt.Sprint(day))
	}
	ordinals := "26MO,26TU,26WE,26TH,26FR,26SA,26SU"
	never := "FREQ=YEARLY;BYMONTHDAY=" + strings.Join(monthDays, ",")
	rule := never + ";COUNT=2;BYDAY=" + ordinals
	repeated := never + ";COUNT=2;BYDAY=" + strings.Repeat(ordinals+",", 99) + ordinals
	stored := map[string]string{
		"yearly": rule,
		"daily":  "FREQ=DAILY;COUNT=2;BYMONTH=2;BYMONTHDAY=30",
		"hourly": "FREQ=HOURLY;BYMONTH=2;BYMONTHDAY=30",
	}

	event := func(rrule string) string {
		return "BEGIN:VCALENDAR\r\nVERSION:2.0\r\nPRODID:-//Review//EN\r\nBEGIN:VEVENT\r\nUID:never\r\nDTSTAMP:20260101T000000Z\r\n" +
			"DTSTART:20260101T090000Z\r\nDTEND:20260101T100000Z\r\nRRULE:" + rrule + "\r\nEND:VEVENT\r\nEND:VCALENDAR\r\n"
	}
	// RFC 5545 §3.6.5 gives an observance no COUNT, so its rule runs forever.
	observance := "BEGIN:VCALENDAR\r\nVERSION:2.0\r\nPRODID:-//Review//EN\r\n" +
		"BEGIN:VTIMEZONE\r\nTZID:Review/Never\r\n" +
		"BEGIN:STANDARD\r\nDTSTART:19700101T000000\r\nTZOFFSETFROM:+0000\r\nTZOFFSETTO:+0000\r\nEND:STANDARD\r\n" +
		"BEGIN:DAYLIGHT\r\nDTSTART:19700102T000000\r\nTZOFFSETFROM:+0000\r\nTZOFFSETTO:+0100\r\nRRULE:" + never + ";BYDAY=" + ordinals + "\r\nEND:DAYLIGHT\r\n" +
		"END:VTIMEZONE\r\n" +
		"BEGIN:VEVENT\r\nUID:never\r\nDTSTAMP:20260101T000000Z\r\n" +
		"DTSTART;TZID=Review/Never:20260101T090000\r\nDTEND;TZID=Review/Never:20260101T100000\r\nEND:VEVENT\r\nEND:VCALENDAR\r\n"

	putBodies := map[string]string{
		"repeated BYDAY":       event(repeated),
		"VTIMEZONE observance": observance,
	}
	for name, rrule := range stored {
		putBodies[name+" rule"] = event(rrule)
	}
	for name, raw := range putBodies {
		t.Run("PUT validation/"+name, func(t *testing.T) {
			var fault *calendarObjectFault
			requirePrompt(t, func() {
				_, fault = validateCalendarObjectForStorage(raw, nil)
			})
			if fault != nil {
				t.Errorf("refused: %+v", fault)
			}
		})
	}

	for _, test := range []struct {
		name, body, want, absent string
		status                   int
	}{
		{
			name:   "free-busy",
			body:   `<C:free-busy-query xmlns:C="urn:ietf:params:xml:ns:caldav"><C:time-range start="20260101T000000Z" end="20360101T000000Z"/></C:free-busy-query>`,
			want:   "FREEBUSY:20260101T090000Z/20260101T100000Z",
			absent: "FREEBUSY:2027",
			status: 200,
		},
		{
			name:   "open-ended calendar-query",
			body:   `<C:calendar-query xmlns:D="DAV:" xmlns:C="urn:ietf:params:xml:ns:caldav"><D:prop><D:getetag/></D:prop><C:filter><C:comp-filter name="VCALENDAR"><C:comp-filter name="VEVENT"><C:time-range start="20260102T000000Z"/></C:comp-filter></C:comp-filter></C:filter></C:calendar-query>`,
			absent: "never.ics",
			status: 207,
		},
	} {
		for name, rrule := range stored {
			t.Run(test.name+"/"+name, func(t *testing.T) {
				h := hostileRecurrenceServer()
				h.store.Events = &fakeEventRepo{events: map[string]*store.Event{"1:never": {ID: 1, CalendarID: 1, UID: "never", ResourceName: "never", ETag: "e", RawICAL: event(rrule)}}}
				req := httptest.NewRequest("REPORT", "/dav/calendars/1/", strings.NewReader(test.body))
				req.Header.Set("Depth", "1")
				rr := httptest.NewRecorder()
				requirePrompt(t, func() {
					h.Report(rr, req.WithContext(auth.WithUser(req.Context(), &store.User{ID: 1})))
				})
				body := rr.Body.String()
				if rr.Code != test.status || !strings.Contains(body, test.want) || strings.Contains(body, test.absent) {
					t.Fatalf("%d %s", rr.Code, body)
				}
			})
		}
	}
}

// denseObservanceObject is a calendar object whose zone moves its clock forward
// one second at every second: a DAYLIGHT rule listing every hour, minute and
// second of every day from Exchange's 1601 DTSTART. PUT admits it without
// COUNT; a counted variant can only be stored data, since PUT refuses COUNT in
// an observance. Its event recurs by eventRule.
func denseObservanceObject(observanceRule, eventRule string) string {
	every := func(from, to int) string {
		values := make([]string, 0, to-from+1)
		for value := from; value <= to; value++ {
			values = append(values, fmt.Sprint(value))
		}
		return strings.Join(values, ",")
	}
	return strings.Join([]string{
		"BEGIN:VCALENDAR", "VERSION:2.0", "PRODID:-//Review//EN",
		"BEGIN:VTIMEZONE", "TZID:Review/Dense",
		"BEGIN:STANDARD", "DTSTART:16010101T000000", "TZOFFSETFROM:+0000", "TZOFFSETTO:+0000", "END:STANDARD",
		"BEGIN:DAYLIGHT", "DTSTART:16010101T020000",
		"RRULE:" + observanceRule + ";BYHOUR=" + every(0, 23) + ";BYMINUTE=" + every(0, 59) + ";BYSECOND=" + every(0, 59),
		"TZOFFSETFROM:+0000", "TZOFFSETTO:+000001", "END:DAYLIGHT",
		"END:VTIMEZONE",
		"BEGIN:VEVENT", "UID:dense", "DTSTAMP:20240101T000000Z",
		"DTSTART;TZID=Review/Dense:20240105T093000", "DURATION:PT1H", "RRULE:" + eventRule,
		"END:VEVENT", "END:VCALENDAR", "",
	}, "\r\n")
}

// A zone that skips a local time at every second makes every lookup of its
// transitions a walk of a day's 86,400 of them, and its gaps cannot be listed
// for any span longer than about half an hour. Every walk and pass a reading
// of the object needs draws on one budget for that reading, so PUT and every
// report over the stored object answer promptly however many instances the
// series has, rather than paying a walk per instance or per listed gap. A
// report reads the object twice, once to match it and once to project it.
func TestDenseObservanceRuleIsAnsweredPromptly(t *testing.T) {
	useTimezoneOnsets(t, maxHeldOnsets)
	for _, eventRule := range []string{"FREQ=WEEKLY;COUNT=2", "FREQ=DAILY;COUNT=1000", "FREQ=HOURLY;COUNT=1000", "FREQ=WEEKLY"} {
		t.Run("PUT validation/"+eventRule, func(t *testing.T) {
			var fault *calendarObjectFault
			requirePrompt(t, func() {
				_, fault = validateCalendarObjectForStorage(denseObservanceObject("FREQ=DAILY", eventRule), nil)
			})
			if fault != nil {
				t.Errorf("refused: %+v", fault)
			}
		})
	}

	stored := map[string]string{
		"open-ended zone": denseObservanceObject("FREQ=DAILY", "FREQ=DAILY;COUNT=1000"),
		"counted zone":    denseObservanceObject("FREQ=DAILY;COUNT=100000000", "FREQ=DAILY;COUNT=1000"),
	}
	for _, test := range []struct {
		name, body, want string
		status           int
	}{
		{
			name:   "free-busy",
			body:   `<C:free-busy-query xmlns:C="urn:ietf:params:xml:ns:caldav"><C:time-range start="20240101T000000Z" end="20270101T000000Z"/></C:free-busy-query>`,
			want:   "FREEBUSY:202401",
			status: http.StatusOK,
		},
		{
			name:   "open-ended calendar-query",
			body:   `<C:calendar-query xmlns:D="DAV:" xmlns:C="urn:ietf:params:xml:ns:caldav"><D:prop><D:getetag/></D:prop><C:filter><C:comp-filter name="VCALENDAR"><C:comp-filter name="VEVENT"><C:time-range start="20250101T000000Z"/></C:comp-filter></C:comp-filter></C:filter></C:calendar-query>`,
			want:   "dense.ics",
			status: http.StatusMultiStatus,
		},
		{
			name: "expanded calendar-query",
			body: `<C:calendar-query xmlns:D="DAV:" xmlns:C="urn:ietf:params:xml:ns:caldav"><D:prop><C:calendar-data><C:expand start="20240101T000000Z" end="20270101T000000Z"/></C:calendar-data></D:prop>` +
				`<C:filter><C:comp-filter name="VCALENDAR"><C:comp-filter name="VEVENT"><C:time-range start="20240101T000000Z" end="20270101T000000Z"/></C:comp-filter></C:comp-filter></C:filter></C:calendar-query>`,
			want:   "RECURRENCE-ID:2024",
			status: http.StatusMultiStatus,
		},
	} {
		for name, raw := range stored {
			t.Run(test.name+"/"+name, func(t *testing.T) {
				h := hostileRecurrenceServer()
				// The stored bounds the database narrows by are written at PUT,
				// so the fake does not derive them again on every read.
				h.store.Events = &fakeEventRepo{ignoreFilter: true, events: map[string]*store.Event{"1:dense": {ID: 1, CalendarID: 1, UID: "dense", ResourceName: "dense", ETag: "e", RawICAL: raw}}}
				report := func() *httptest.ResponseRecorder {
					req := httptest.NewRequest("REPORT", "/dav/calendars/1/", strings.NewReader(test.body))
					req.Header.Set("Depth", "1")
					rr := httptest.NewRecorder()
					h.Report(rr, req.WithContext(auth.WithUser(req.Context(), &store.User{ID: 1})))
					return rr
				}
				// The zone's rules are listed once for the whole process, so
				// the second report pays what every later one does.
				first := report()
				var rr *httptest.ResponseRecorder
				requirePromptWithin(t, time.Second, func() { rr = report() })
				if rr.Code != test.status || first.Code != test.status || !strings.Contains(rr.Body.String(), test.want) {
					t.Fatalf("%d then %d, want %d with %q: %s", first.Code, rr.Code, test.status, test.want, rr.Body.String())
				}
			})
		}
	}
}
