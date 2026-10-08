package dav

import (
	"strings"
	"testing"
	"time"

	"github.com/jw6ventures/calcard/internal/store"
)

// oneDayRange is a range starting partway through 2025-03-01 and ending with
// it, so only an event occupying the rest of that day reaches it: an instant at
// its midnight does not.
const (
	oneDayRangeStart = "20250301T080000Z"
	oneDayRangeEnd   = "20250302T000000Z"
)

func timeRangeQuery(start, end string) string {
	return `<C:calendar-query xmlns:D="DAV:" xmlns:C="urn:ietf:params:xml:ns:caldav"><D:prop><D:getetag/></D:prop>` +
		`<C:filter><C:comp-filter name="VCALENDAR"><C:comp-filter name="VEVENT"><C:time-range start="` + start + `" end="` + end + `"/>` +
		`</C:comp-filter></C:comp-filter></C:filter></C:calendar-query>`
}

func freeBusyQuery(start, end string) string {
	return `<C:free-busy-query xmlns:C="urn:ietf:params:xml:ns:caldav"><C:time-range start="` + start + `" end="` + end + `"/></C:free-busy-query>`
}

// oneDayEventSpellings are VEVENTs a calendar application exports as an event
// on 2025-03-01 by writing that date as both its ends, which RFC 5545 §3.8.2.2
// gives no meaning: DTEND has to be later than DTSTART. Each occupies that day,
// as the same DTSTART with no end does.
var oneDayEventSpellings = map[string][]string{
	"DATE DTEND equal to DTSTART":  {"UID:instant", "DTSTAMP:20250101T000000Z", "DTSTART;VALUE=DATE:20250301", "DTEND;VALUE=DATE:20250301"},
	"DATE DTEND equal, unmarked":   {"UID:instant", "DTSTAMP:20250101T000000Z", "DTSTART:20250301", "DTEND:20250301"},
	"series instance, DTEND equal": {"UID:instant", "DTSTAMP:20250101T000000Z", "DTSTART;VALUE=DATE:20250227", "DTEND;VALUE=DATE:20250227", "RRULE:FREQ=DAILY;COUNT=5"},
}

func TestOneDayEventWrittenWithAnEndNotAfterItsDateOccupiesTheDay(t *testing.T) {
	for name, properties := range oneDayEventSpellings {
		raw := zeroLengthCalendar(componentLines("VEVENT", properties...))
		t.Run(name+"/calendar-query", func(t *testing.T) {
			code, body := zeroLengthReport(t, raw, timeRangeQuery(oneDayRangeStart, oneDayRangeEnd))
			if code != 207 || !strings.Contains(body, "instant.ics") {
				t.Fatalf("calendar-query [%s, %s) = %d without the event:\n%s", oneDayRangeStart, oneDayRangeEnd, code, body)
			}
		})
		t.Run(name+"/free-busy", func(t *testing.T) {
			code, body := zeroLengthReport(t, raw, freeBusyQuery(oneDayRangeStart, oneDayRangeEnd))
			if code != 200 {
				t.Fatalf("free-busy status = %d, body %s", code, body)
			}
			assertPublishedFreeBusy(t, parsedFreeBusyLines(t, body), []string{"FREEBUSY:20250301T000000Z/" + oneDayRangeEnd})
		})
		t.Run(name+"/next day", func(t *testing.T) {
			// The day ends at the next midnight; the event does not run on.
			code, body := zeroLengthReport(t, raw, timeRangeQuery("20250302T080000Z", "20250303T000000Z"))
			if code != 207 {
				t.Fatalf("status = %d, body %s", code, body)
			}
			if strings.Contains(body, "instant.ics") && !strings.Contains(name, "series") {
				t.Fatalf("calendar-query matched the day after the event:\n%s", body)
			}
		})
	}
}

// A DATE DTEND before its DTSTART, which PUT refuses but the store may hold,
// is read as unwritten too: the event occupies its DTSTART's day, and the
// dtend column holds the end of that day (ical.StoredEventEnd).
func TestDateEventWithEndBeforeStartOccupiesItsDay(t *testing.T) {
	raw := zeroLengthCalendar(componentLines("VEVENT", "UID:instant", "DTSTAMP:20250101T000000Z",
		"DTSTART;VALUE=DATE:20250301", "DTEND;VALUE=DATE:20250226"))
	if _, fault := validateCalendarObjectForStorage(raw, nil); fault == nil {
		t.Fatal("PUT accepts a DTEND before DTSTART")
	}
	if !assertComponentInRange(t, raw, "VEVENT", oneDayRangeStart, oneDayRangeEnd, floatingZone{}) {
		t.Errorf("VEVENT not in [%s, %s)", oneDayRangeStart, oneDayRangeEnd)
	}
	if assertComponentInRange(t, raw, "VEVENT", "20250226T000000Z", "20250301T000000Z", floatingZone{}) {
		t.Error("VEVENT reaches back to its DTEND")
	}
	tr := &timeRange{Start: oneDayRangeStart, End: oneDayRangeEnd}
	body := mustReportText((&DavServer{}).generateFreeBusy(t.Context(),
		freeBusyCandidates([]store.Event{{UID: "instant", RawICAL: raw}}, floatingZone{}), tr))
	assertPublishedFreeBusy(t, parsedFreeBusyLines(t, body), []string{"FREEBUSY:20250301T000000Z/" + oneDayRangeEnd})
}

// The DATE-TIME spelling of an end equal to the start is the instant §9.9's
// zero-duration row places, and stays one.
func TestDateTimeEventWithEndEqualToStartStaysAnInstant(t *testing.T) {
	raw := zeroLengthCalendar(componentLines("VEVENT", "UID:instant", "DTSTAMP:20250101T000000Z",
		"DTSTART:20250301T000000Z", "DTEND:20250301T000000Z"))
	code, body := zeroLengthReport(t, raw, timeRangeQuery(oneDayRangeStart, oneDayRangeEnd))
	if code != 207 || strings.Contains(body, "instant.ics") {
		t.Fatalf("calendar-query [%s, %s) = %d, matching a midnight instant:\n%s", oneDayRangeStart, oneDayRangeEnd, code, body)
	}
}

// A DATE-TIME DTEND before DTSTART is read as unwritten too, which RFC 5545
// §3.6.1 makes the instant at DTSTART, as free-busy already publishes it.
func TestDateTimeEventWithEndBeforeStartIsItsStartInstant(t *testing.T) {
	raw := wrapCalendar(componentLines("VEVENT", "UID:instant", "DTSTART:20250301T100000Z", "DTEND:20250301T080000Z")...)
	for _, tt := range []struct {
		start, end string
		want       bool
	}{
		{"20250301T100000Z", "20250301T110000Z", true},
		{"20250301T070000Z", "20250301T090000Z", false},
		{"20250301T090000Z", "20250301T100000Z", false},
	} {
		if got := assertComponentInRange(t, raw, "VEVENT", tt.start, tt.end, floatingZone{}); got != tt.want {
			t.Errorf("VEVENT in [%s, %s) = %v, want %v", tt.start, tt.end, got, tt.want)
		}
	}
}

// An override written as a one-day event with an end equal to its date lasts
// that one day, whatever the length of the occurrences of its series.
func TestOneDayOverrideWrittenWithItsDateAsBothEndsOccupiesTheDay(t *testing.T) {
	master := []string{"UID:instant", "DTSTAMP:20250101T000000Z", "SUMMARY:series",
		"DTSTART;VALUE=DATE:20250206", "DTEND;VALUE=DATE:20250209", "RRULE:FREQ=WEEKLY;COUNT=3"}
	override := []string{"UID:instant", "DTSTAMP:20250101T000000Z", "SUMMARY:moved",
		"RECURRENCE-ID;VALUE=DATE:20250213", "DTSTART;VALUE=DATE:20250301", "DTEND;VALUE=DATE:20250301"}
	raw := zeroLengthCalendar(componentLines("VEVENT", master...), componentLines("VEVENT", override...))

	code, body := zeroLengthReport(t, raw, timeRangeQuery(oneDayRangeStart, oneDayRangeEnd))
	if code != 207 || !strings.Contains(body, "instant.ics") {
		t.Fatalf("calendar-query [%s, %s) = %d without the override:\n%s", oneDayRangeStart, oneDayRangeEnd, code, body)
	}
	code, body = zeroLengthReport(t, raw, freeBusyQuery(oneDayRangeStart, "20250304T000000Z"))
	if code != 200 {
		t.Fatalf("free-busy status = %d, body %s", code, body)
	}
	assertPublishedFreeBusy(t, parsedFreeBusyLines(t, body), []string{"FREEBUSY:20250301T000000Z/" + oneDayRangeEnd})
}

// §9.6.5 writes each expanded instance with the end it occupies, so a client
// reading the expansion places the day where the server matched it.
func TestExpandedOneDayInstanceCarriesTheEndOfItsDay(t *testing.T) {
	raw := zeroLengthCalendar(componentLines("VEVENT", oneDayEventSpellings["series instance, DTEND equal"]...))
	code, body := zeroLengthReport(t, raw, calendarDataQuery(`<C:expand start="`+oneDayRangeStart+`" end="`+oneDayRangeEnd+`"/>`))
	if code != 207 {
		t.Fatalf("status = %d, body %s", code, body)
	}
	for _, want := range []string{"DTSTART;VALUE=DATE:20250301", "DTEND;VALUE=DATE:20250302"} {
		if !strings.Contains(body, want) {
			t.Errorf("expanded instance lacks %s:\n%s", want, body)
		}
	}
	if strings.Contains(body, "DTEND;VALUE=DATE:20250301") {
		t.Errorf("expanded instance ends where it starts:\n%s", body)
	}
}

// §9.6.6 keeps an override whose original slot impacts the range; a slot of a
// series of one-day events written with their date as both ends is that day.
func TestOneDaySeriesLimitsOverridesByTheirDays(t *testing.T) {
	master := []string{"UID:instant", "DTSTAMP:20260101T000000Z", "RRULE:FREQ=DAILY;COUNT=10",
		"DTSTART;VALUE=DATE:20260105", "DTEND;VALUE=DATE:20260105"}
	override := []string{"UID:instant", "DTSTAMP:20260101T000000Z", "SUMMARY:moved",
		"RECURRENCE-ID;VALUE=DATE:20260107", "DTSTART;VALUE=DATE:20260120", "DTEND;VALUE=DATE:20260120"}
	raw := zeroLengthCalendar(componentLines("VEVENT", master...), componentLines("VEVENT", override...))
	for _, tt := range []struct {
		rangeStart, rangeEnd string
		want                 bool
	}{
		{rangeStart: "20260107T120000Z", rangeEnd: "20260107T130000Z", want: true},
		{rangeStart: "20260120T120000Z", rangeEnd: "20260120T130000Z", want: true},
		{rangeStart: "20260108T120000Z", rangeEnd: "20260108T130000Z", want: false},
	} {
		code, body := zeroLengthReport(t, raw, calendarDataQuery(`<C:limit-recurrence-set start="`+tt.rangeStart+`" end="`+tt.rangeEnd+`"/>`))
		if code != 207 {
			t.Fatalf("status = %d, body %s", code, body)
		}
		if got := strings.Contains(body, "SUMMARY:moved"); got != tt.want {
			t.Errorf("[%s, %s): override kept = %v, want %v:\n%s", tt.rangeStart, tt.rangeEnd, got, tt.want, body)
		}
	}
}

// An alarm relative to the end of a one-day event fires relative to the end of
// that day.
func TestAlarmRelatedToTheEndOfAOneDayEvent(t *testing.T) {
	for name, ends := range map[string][]string{
		"DTEND equal to DTSTART": {"DTEND;VALUE=DATE:20250301"},
		"no DTEND":               nil,
	} {
		t.Run(name, func(t *testing.T) {
			properties := append([]string{"UID:alarm", "DTSTART;VALUE=DATE:20250301"}, ends...)
			properties = append(properties, "BEGIN:VALARM", "ACTION:DISPLAY", "DESCRIPTION:x", "TRIGGER;RELATED=END:-PT1H", "END:VALARM")
			raw := wrapCalendar(componentLines("VEVENT", properties...)...)
			if !assertComponentInRange(t, raw, "VALARM", "20250301T225900Z", "20250301T230100Z", floatingZone{}) {
				t.Error("alarm an hour before the end of 2025-03-01 does not fire at 23:00")
			}
			if assertComponentInRange(t, raw, "VALARM", "20250228T225900Z", "20250228T230100Z", floatingZone{}) {
				t.Error("alarm fires an hour before the event's start")
			}
		})
	}
}

// The columns the store narrows a time-range read by hold the same end: the
// day after the event's date.
func TestStoredEndOfAOneDayEventIsTheEndOfItsDay(t *testing.T) {
	for name, properties := range oneDayEventSpellings {
		if strings.Contains(name, "series") {
			continue
		}
		t.Run(name, func(t *testing.T) {
			validated, fault := validateCalendarObjectForStorage(producerObject("VEVENT", properties...), nil)
			if fault != nil {
				t.Fatalf("refused: %+v", fault)
			}
			want := time.Date(2025, 3, 2, 0, 0, 0, 0, time.UTC)
			if got := validated.Analysis.Metadata.DTEnd; got == nil || !got.Equal(want) {
				t.Errorf("stored dtend = %v, want %v", got, want)
			}
		})
	}
}

// An override whose DTEND or DURATION cannot end it -- a DTEND before its
// DTSTART, which PUT refuses but the store may hold, or a negative DURATION --
// is read as RFC 5545 §3.6.1 reads any VEVENT: a DATE-TIME start is the
// instant it names. It does not take its series' length, so calendar-query
// matches it only at that instant and free-busy publishes no busy time for it.
func TestOverrideWithNoUsableEndIsAnInstantToQueryAndFreeBusyAlike(t *testing.T) {
	master := componentLines("VEVENT", "UID:instant", "DTSTAMP:20250101T000000Z",
		"DTSTART:20250203T090000Z", "DTEND:20250203T100000Z", "RRULE:FREQ=WEEKLY;COUNT=4")
	for name, ends := range map[string]func(date string) []string{
		"DTEND before DTSTART": func(date string) []string { return []string{"DTEND:" + date + "T080000Z"} },
		"negative DURATION":    func(string) []string { return []string{"DURATION:-PT1H"} },
	} {
		for _, shape := range []struct {
			name, recurrenceID, date, day string
		}{
			{"exact", "RECURRENCE-ID:20250210T090000Z", "20250301", "20250301"},
			// The override moves this and every later instance to 10:00, so
			// 24 February is one of the instances it describes.
			{"this and future", "RECURRENCE-ID;RANGE=THISANDFUTURE:20250210T090000Z", "20250210", "20250224"},
		} {
			t.Run(name+"/"+shape.name, func(t *testing.T) {
				override := append([]string{"UID:instant", "DTSTAMP:20250101T000000Z", shape.recurrenceID,
					"DTSTART:" + shape.date + "T100000Z"}, ends(shape.date)...)
				raw := zeroLengthCalendar(master, componentLines("VEVENT", override...))
				assertOverrideQueried(t, raw, shape.day+"T100000Z", shape.day+"T103000Z", true)
				assertOverrideQueried(t, raw, shape.day+"T103000Z", shape.day+"T110000Z", false)
				code, body := zeroLengthReport(t, raw, freeBusyQuery(shape.day+"T000000Z", shape.day+"T235959Z"))
				if code != 200 {
					t.Fatalf("free-busy status = %d, body %s", code, body)
				}
				assertPublishedFreeBusy(t, parsedFreeBusyLines(t, body), nil)
			})
		}
	}
}

// An override writing neither DTEND nor DURATION changes its instance's start
// and keeps the rest of it, its length included: it lasts as long as an
// occurrence of its series, whether it overrides one instance or, with
// RANGE=THISANDFUTURE, that instance and every later one. Calendar-query,
// free-busy and expansion place it alike.
func TestOverrideWithNeitherEndTakesItsSeriesLength(t *testing.T) {
	master := componentLines("VEVENT", "UID:instant", "DTSTAMP:20250101T000000Z",
		"DTSTART:20260302T100000Z", "DTEND:20260302T110000Z", "RRULE:FREQ=DAILY;COUNT=5")
	for _, shape := range []struct {
		name, recurrenceID string
		busy               []string
	}{
		{"exact", "RECURRENCE-ID:20260304T100000Z", []string{
			"FREEBUSY:20260302T100000Z/20260302T110000Z", "FREEBUSY:20260303T100000Z/20260303T110000Z",
			"FREEBUSY:20260304T120000Z/20260304T130000Z", "FREEBUSY:20260305T100000Z/20260305T110000Z",
			"FREEBUSY:20260306T100000Z/20260306T110000Z"}},
		{"this and future", "RECURRENCE-ID;RANGE=THISANDFUTURE:20260303T100000Z", []string{
			"FREEBUSY:20260302T100000Z/20260302T110000Z", "FREEBUSY:20260303T120000Z/20260303T130000Z",
			"FREEBUSY:20260304T120000Z/20260304T130000Z", "FREEBUSY:20260305T120000Z/20260305T130000Z",
			"FREEBUSY:20260306T120000Z/20260306T130000Z"}},
	} {
		t.Run(shape.name, func(t *testing.T) {
			start := "20260304T120000Z"
			if shape.name == "this and future" {
				start = "20260303T120000Z"
			}
			override := []string{"UID:instant", "DTSTAMP:20250101T000000Z", shape.recurrenceID, "DTSTART:" + start}
			raw := zeroLengthCalendar(master, componentLines("VEVENT", override...))
			assertOverrideQueried(t, raw, "20260304T123000Z", "20260304T124500Z", true)
			assertOverrideQueried(t, raw, "20260304T130000Z", "20260304T140000Z", false)
			code, body := zeroLengthReport(t, raw, freeBusyQuery("20260301T000000Z", "20260310T000000Z"))
			if code != 200 {
				t.Fatalf("free-busy status = %d, body %s", code, body)
			}
			assertPublishedFreeBusy(t, parsedFreeBusyLines(t, body), shape.busy)
			code, body = zeroLengthReport(t, raw, calendarDataQuery(`<C:expand start="20260304T000000Z" end="20260305T000000Z"/>`))
			if code != 207 || !strings.Contains(body, "DTSTART:20260304T120000Z") || !strings.Contains(body, "DTEND:20260304T130000Z") {
				t.Errorf("expanded 4 March = %d without 12:00-13:00:\n%s", code, body)
			}
			code, body = zeroLengthReport(t, raw, calendarDataQuery(`<C:limit-recurrence-set start="20260304T123000Z" end="20260304T124500Z"/>`))
			if code != 207 || !strings.Contains(body, "DTSTART:"+start) {
				t.Errorf("limit-recurrence-set over 4 March 12:30 = %d without the override:\n%s", code, body)
			}
			// The stored columns are the master's, which no override changes.
			analysis, err := analyzeICalendar(raw)
			if err != nil {
				t.Fatal(err)
			}
			if end := analysis.Metadata.DTEnd; end == nil || !end.Equal(time.Date(2026, 3, 2, 11, 0, 0, 0, time.UTC)) {
				t.Errorf("stored dtend = %v, want the master's 11:00 on 2 March", end)
			}
		})
	}
}

// A DATE override whose DTEND cannot be read at all is read as though it had
// none: a DURATION beside it sets its length, and without one it occupies its
// day. Calendar-query and free-busy place it on the same days.
func TestDateOverrideWithAnUnreadableEndIsReadAlikeByQueryAndFreeBusy(t *testing.T) {
	master := componentLines("VEVENT", "UID:instant", "DTSTAMP:20250101T000000Z",
		"DTSTART;VALUE=DATE:20250203", "DTEND;VALUE=DATE:20250204", "RRULE:FREQ=WEEKLY;COUNT=4")
	for name, tt := range map[string]struct {
		extra    []string
		busy     string
		secondOn bool
	}{
		"with a DURATION": {extra: []string{"DURATION:P2D"}, busy: "FREEBUSY:20250301T000000Z/20250303T000000Z", secondOn: true},
		"alone":           {busy: "FREEBUSY:20250301T000000Z/20250302T000000Z"},
	} {
		t.Run(name, func(t *testing.T) {
			override := append([]string{"UID:instant", "DTSTAMP:20250101T000000Z", "RECURRENCE-ID;VALUE=DATE:20250210",
				"DTSTART;VALUE=DATE:20250301", "DTEND;VALUE=DATE:2025030X"}, tt.extra...)
			raw := zeroLengthCalendar(master, componentLines("VEVENT", override...))
			assertOverrideQueried(t, raw, "20250301T080000Z", "20250301T090000Z", true)
			assertOverrideQueried(t, raw, "20250302T080000Z", "20250302T090000Z", tt.secondOn)
			code, body := zeroLengthReport(t, raw, freeBusyQuery("20250301T000000Z", "20250304T000000Z"))
			if code != 200 {
				t.Fatalf("free-busy status = %d, body %s", code, body)
			}
			assertPublishedFreeBusy(t, parsedFreeBusyLines(t, body), []string{tt.busy})
		})
	}
}

func assertOverrideQueried(t *testing.T, raw, start, end string, want bool) {
	t.Helper()
	code, body := zeroLengthReport(t, raw, timeRangeQuery(start, end))
	if code != 207 {
		t.Fatalf("calendar-query status = %d, body %s", code, body)
	}
	if got := strings.Contains(body, "instant.ics"); got != want {
		t.Errorf("calendar-query [%s, %s) matched = %v, want %v:\n%s", start, end, got, want, body)
	}
}

// todoQuery is a calendar-query for VTODOs, returning their calendar-data
// through transform and, when start is set, selecting those in [start, end).
func todoQuery(transform, start, end string) string {
	timeRange := ""
	if start != "" {
		timeRange = `<C:time-range start="` + start + `" end="` + end + `"/>`
	}
	return `<C:calendar-query xmlns:D="DAV:" xmlns:C="urn:ietf:params:xml:ns:caldav"><D:prop><C:calendar-data>` + transform +
		`</C:calendar-data></D:prop><C:filter><C:comp-filter name="VCALENDAR"><C:comp-filter name="VTODO">` + timeRange +
		`</C:comp-filter></C:comp-filter></C:filter></C:calendar-query>`
}

// A task override's own DUE ends it, and one with neither DUE nor DURATION is
// the instant RFC 4791 §9.9 places a VTODO with a DTSTART alone at: neither
// takes its series' length, which only an event override does, and expansion
// writes no DUE the override does not have.
func TestTaskOverrideEndsAtItsOwnDue(t *testing.T) {
	master := componentLines("VTODO", "UID:instant", "DTSTAMP:20250101T000000Z",
		"DTSTART:20260302T100000Z", "DUE:20260303T100000Z", "RRULE:FREQ=WEEKLY;COUNT=5")
	t.Run("own DUE, this and future", func(t *testing.T) {
		override := componentLines("VTODO", "UID:instant", "DTSTAMP:20250101T000000Z",
			"RECURRENCE-ID;RANGE=THISANDFUTURE:20260309T100000Z", "DTSTART:20260309T100000Z", "DUE:20260312T100000Z")
		raw := zeroLengthCalendar(master, override)
		code, body := zeroLengthReport(t, raw, todoQuery(`<C:expand start="20260316T000000Z" end="20260317T000000Z"/>`, "20260316T000000Z", "20260317T000000Z"))
		if code != 207 || !strings.Contains(body, "DTSTART:20260316T100000Z") || !strings.Contains(body, "DUE:20260319T100000Z") {
			t.Errorf("expanded 16 March = %d without its three-day DUE:\n%s", code, body)
		}
		code, body = zeroLengthReport(t, raw, todoQuery("", "20260318T000000Z", "20260318T120000Z"))
		if code != 207 || !strings.Contains(body, "instant.ics") {
			t.Errorf("calendar-query on 18 March = %d without the task due on the 19th:\n%s", code, body)
		}
	})
	for _, recurrenceID := range []string{"RECURRENCE-ID:20260309T100000Z", "RECURRENCE-ID;RANGE=THISANDFUTURE:20260309T100000Z"} {
		t.Run("neither DUE nor DURATION/"+recurrenceID, func(t *testing.T) {
			override := componentLines("VTODO", "UID:instant", "DTSTAMP:20250101T000000Z", recurrenceID, "DTSTART:20260309T120000Z")
			raw := zeroLengthCalendar(master, override)
			code, body := zeroLengthReport(t, raw, todoQuery(`<C:expand start="20260309T000000Z" end="20260310T000000Z"/>`, "20260309T000000Z", "20260310T000000Z"))
			if code != 207 || !strings.Contains(body, "DTSTART:20260309T120000Z") {
				t.Fatalf("expanded 9 March = %d without the override:\n%s", code, body)
			}
			if strings.Contains(body, "DUE:") {
				t.Errorf("expanded override carries a DUE it does not have:\n%s", body)
			}
		})
	}
}
