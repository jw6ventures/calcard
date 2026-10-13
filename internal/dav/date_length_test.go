package dav

import (
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/jw6ventures/calcard/internal/auth"
	"github.com/jw6ventures/calcard/internal/calobject"
	"github.com/jw6ventures/calcard/internal/ical"
	"github.com/jw6ventures/calcard/internal/store"
)

// zonedReport runs a REPORT over a collection whose CALDAV:calendar-timezone
// is timezone, holding raw as instant.ics.
func zonedReport(t *testing.T, timezone, raw, body string) (int, string) {
	t.Helper()
	h := &DavServer{store: &store.Store{
		Calendars: &fakeCalendarRepo{accessible: []store.CalendarAccess{{
			Calendar: store.Calendar{ID: 1, UserID: 1, Name: "Test", Timezone: &timezone},
			Editor:   true,
		}}},
		Events: &fakeEventRepo{events: map[string]*store.Event{
			"1:instant": {ID: 1, CalendarID: 1, UID: "instant", ResourceName: "instant", ETag: "e", RawICAL: raw},
		}},
	}}
	req := httptest.NewRequest("REPORT", "/dav/calendars/1/", strings.NewReader(body))
	req.Header.Set("Depth", "1")
	rr := httptest.NewRecorder()
	h.Report(rr, req.WithContext(auth.WithUser(req.Context(), &store.User{ID: 1})))
	return rr.Code, rr.Body.String()
}

func assertZonedQuery(t *testing.T, timezone, raw, start, end string, want bool) {
	t.Helper()
	code, body := zonedReport(t, timezone, raw, timeRangeQuery(start, end))
	if code != 207 {
		t.Fatalf("calendar-query status = %d, body %s", code, body)
	}
	if got := strings.Contains(body, "instant.ics"); got != want {
		t.Errorf("calendar-query [%s, %s) matched = %v, want %v:\n%s", start, end, got, want, body)
	}
}

func assertZonedExpansion(t *testing.T, timezone, raw, start, end string, wanted, unwanted []string) {
	t.Helper()
	code, body := zonedReport(t, timezone, raw, calendarDataQuery(`<C:expand start="`+start+`" end="`+end+`"/>`))
	if code != 207 {
		t.Fatalf("expand status = %d, body %s", code, body)
	}
	for _, text := range wanted {
		if !strings.Contains(body, text) {
			t.Errorf("expanded output lacks %s:\n%s", text, body)
		}
	}
	for _, text := range unwanted {
		if strings.Contains(body, text) {
			t.Errorf("expanded output carries %s:\n%s", text, body)
		}
	}
}

// dateSeriesTransitionDays are a daily all-day series in Chicago ending on a
// day the zone changes its offset, whose last instance is overridden by an
// all-day override writing no end: it lasts its series' one day, which is 25
// hours on the day the clocks go back and 23 on the day they go forward.
var dateSeriesTransitionDays = []struct {
	name, day, nextDay  string
	inLastHour          [2]string
	pastTheDay          [2]string
	expandStart, expEnd string
}{
	{
		name: "the day the clocks go back", day: "20261101", nextDay: "20261102",
		inLastHour:  [2]string{"20261102T051500Z", "20261102T054500Z"},
		pastTheDay:  [2]string{"20261102T061500Z", "20261102T064500Z"},
		expandStart: "20261101T050000Z", expEnd: "20261102T060000Z",
	},
	{
		name: "the day the clocks go forward", day: "20260308", nextDay: "20260309",
		inLastHour:  [2]string{"20260309T041500Z", "20260309T044500Z"},
		pastTheDay:  [2]string{"20260309T051500Z", "20260309T054500Z"},
		expandStart: "20260308T060000Z", expEnd: "20260309T050000Z",
	},
}

func dateSeriesEndingOn(day string) string {
	return zeroLengthCalendar(
		componentLines("VEVENT", "UID:instant", "DTSTAMP:20250101T000000Z",
			"DTSTART;VALUE=DATE:20260301", "DTEND;VALUE=DATE:20260302", "RRULE:FREQ=DAILY;UNTIL="+day),
		componentLines("VEVENT", "UID:instant", "DTSTAMP:20250101T000000Z",
			"RECURRENCE-ID;VALUE=DATE:"+day, "DTSTART;VALUE=DATE:"+day, "SUMMARY:Moved"))
}

// An all-day override of an all-day series takes the series' length in days,
// which the collection's zone gives their hours: the override ends at the
// midnight ending its day. Calendar-query places it so, and expansion writes
// it as the one-day event RFC 5545 §3.6.1 reads a DATE start alone as.
func TestDateOverrideTakesItsDateSeriesLengthInTheZonesDays(t *testing.T) {
	chicago := chicagoVTimezoneObject()
	for _, tt := range dateSeriesTransitionDays {
		t.Run(tt.name, func(t *testing.T) {
			raw := dateSeriesEndingOn(tt.day)
			assertZonedQuery(t, chicago, raw, tt.inLastHour[0], tt.inLastHour[1], true)
			assertZonedQuery(t, chicago, raw, tt.pastTheDay[0], tt.pastTheDay[1], false)
			assertZonedExpansion(t, chicago, raw, tt.expandStart, tt.expEnd,
				[]string{"DTSTART;VALUE=DATE:" + tt.day + "\r\nSUMMARY:Moved\r\nEND:VEVENT"},
				[]string{"DURATION", "DTEND"})
		})
	}
}

// An all-day instance of a series lasting two days keeps both its days when it
// falls across the night the clocks go back: §9.6.5 writes its end as the date
// two days after its start, and calendar-query places it through that date.
func TestExpandedDateInstanceKeepsItsDaysAcrossATransition(t *testing.T) {
	chicago := chicagoVTimezoneObject()
	raw := zeroLengthCalendar(componentLines("VEVENT", "UID:instant", "DTSTAMP:20250101T000000Z",
		"DTSTART;VALUE=DATE:20260307", "DTEND;VALUE=DATE:20260309", "RRULE:FREQ=WEEKLY"))
	assertZonedExpansion(t, chicago, raw, "20261031T120000Z", "20261101T120000Z",
		[]string{"DTSTART;VALUE=DATE:20261031\r\nDTEND;VALUE=DATE:20261102"}, nil)
	assertZonedQuery(t, chicago, raw, "20261101T120000Z", "20261101T130000Z", true)
	assertZonedQuery(t, chicago, raw, "20261102T061500Z", "20261102T064500Z", false)
}

// RFC 5545 §3.8.2.5 has a DURATION beside a DATE DTSTART count whole days or
// weeks. One that counts hours is read as the whole days it reaches into, so
// the event occupies every day it was written to touch, and an override taking
// its length is written with an end in days.
func TestDateStartWithAnHourDurationLastsTheWholeDaysItReaches(t *testing.T) {
	for _, tt := range []struct {
		name, duration, rule string
		inMaster             [2]string
		overrideRecurrenceID string
		overrideStart        string
		overrideEnd          string
		inOverride           [2]string
		overrideWritten      string
	}{
		{
			name: "an hour", duration: "PT1H", rule: "FREQ=DAILY;COUNT=3",
			inMaster:             [2]string{"20260105T120000Z", "20260105T130000Z"},
			overrideRecurrenceID: "20260106", overrideStart: "20260110", overrideEnd: "20260111",
			inOverride:      [2]string{"20260110T120000Z", "20260110T130000Z"},
			overrideWritten: "DTSTART;VALUE=DATE:20260110\r\nEND:VEVENT",
		},
		{
			name: "a day and a half", duration: "PT36H", rule: "FREQ=WEEKLY;COUNT=3",
			inMaster:             [2]string{"20260106T130000Z", "20260106T140000Z"},
			overrideRecurrenceID: "20260112", overrideStart: "20260115", overrideEnd: "20260117",
			inOverride:      [2]string{"20260116T130000Z", "20260116T140000Z"},
			overrideWritten: "DTSTART;VALUE=DATE:20260115\r\nDTEND;VALUE=DATE:20260117",
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			raw := zeroLengthCalendar(
				componentLines("VEVENT", "UID:instant", "DTSTAMP:20250101T000000Z",
					"DTSTART;VALUE=DATE:20260105", "DURATION:"+tt.duration, "RRULE:"+tt.rule),
				componentLines("VEVENT", "UID:instant", "DTSTAMP:20250101T000000Z",
					"RECURRENCE-ID;VALUE=DATE:"+tt.overrideRecurrenceID, "DTSTART;VALUE=DATE:"+tt.overrideStart))
			assertOverrideQueried(t, raw, tt.inMaster[0], tt.inMaster[1], true)
			assertOverrideQueried(t, raw, tt.inOverride[0], tt.inOverride[1], true)
			start := mustUTC(t, tt.overrideStart+"T000000Z")
			assertExpandedExtents(t, raw, tt.overrideStart+"T000000Z", tt.overrideEnd+"T000000Z", expandedExtents{
				wanted:   []string{tt.overrideWritten},
				unwanted: []string{"DURATION:PT"},
				want: []calobject.Instance{{RecurrenceID: mustUTC(t, tt.overrideRecurrenceID+"T000000Z"),
					Start: start, End: mustUTC(t, tt.overrideEnd+"T000000Z")}},
			})
		})
	}
}

// An expanded all-day instance whose length differs from the one its DURATION
// writes has the DURATION rewritten in the whole days it lasts.
func TestExpandedDateInstanceRewritesItsDurationInDays(t *testing.T) {
	raw := zeroLengthCalendar(componentLines("VEVENT", "UID:instant", "DTSTAMP:20250101T000000Z",
		"DTSTART;VALUE=DATE:20260105", "DURATION:P1D", "RRULE:FREQ=WEEKLY;COUNT=3"))
	root, err := parseICalendarObject(raw)
	if err != nil {
		t.Fatal(err)
	}
	m := newCalendarTimeRangeMatcher(raw, root, floatingZone{})
	master := root.children[0]
	occurrence := expandedInstance(m, root, master, recurrenceInstance{duration: 48 * time.Hour})
	if got := occurrence.value("DURATION"); got != "P2D" {
		t.Errorf("expanded DURATION = %q, want P2D", got)
	}
}

// Expansion writes a DURATION beside a DATE in the days the event spans, as
// RFC 5545 §3.8.2.5 requires, whether its instance comes from the series or
// is an override, and on an ordinary day as on one a transition shortens.
func TestExpandedDateDurationIsWrittenInDays(t *testing.T) {
	raw := zeroLengthCalendar(
		componentLines("VEVENT", "UID:instant", "DTSTAMP:20250101T000000Z",
			"DTSTART;VALUE=DATE:20260105", "DURATION:PT1H", "RRULE:FREQ=DAILY;COUNT=3"),
		componentLines("VEVENT", "UID:instant", "DTSTAMP:20250101T000000Z",
			"RECURRENCE-ID;VALUE=DATE:20260106", "DTSTART;VALUE=DATE:20260106", "DURATION:PT36H"))
	assertExpandedExtents(t, raw, "20260105T000000Z", "20260108T000000Z", expandedExtents{
		wanted:   []string{"DTSTART;VALUE=DATE:20260105\r\nDURATION:P1D", "DTSTART;VALUE=DATE:20260106\r\nDURATION:P2D"},
		unwanted: []string{"DURATION:PT"},
		want: []calobject.Instance{
			{RecurrenceID: mustUTC(t, "20260105T000000Z"), Start: mustUTC(t, "20260105T000000Z"), End: mustUTC(t, "20260106T000000Z")},
			{RecurrenceID: mustUTC(t, "20260106T000000Z"), Start: mustUTC(t, "20260106T000000Z"), End: mustUTC(t, "20260108T000000Z")},
			{RecurrenceID: mustUTC(t, "20260107T000000Z"), Start: mustUTC(t, "20260107T000000Z"), End: mustUTC(t, "20260108T000000Z")},
		},
	})
}

// Expansion writes an all-day instance's end in the form its content writes
// it: a DURATION counting its days as written, in weeks among them, a DTEND
// as the date it reaches, and no end as none, the one day RFC 5545 §3.6.1
// gives a DATE start alone. An instance spanning no day is given a zero
// DURATION, since §3.8.2.2 has a DTEND come later than its DTSTART.
func TestExpandedDateEndKeepsTheFormItIsWrittenIn(t *testing.T) {
	series := func(lines ...string) []string {
		return componentLines("VEVENT", append([]string{"UID:instant", "DTSTAMP:20250101T000000Z", "DTSTART;VALUE=DATE:20260105"}, lines...)...)
	}
	for _, tt := range []struct {
		name             string
		components       [][]string
		wanted, unwanted []string
	}{
		{
			name:       "a DURATION in weeks",
			components: [][]string{series("DURATION:P1W", "RRULE:FREQ=WEEKLY;COUNT=2")},
			wanted:     []string{"DTSTART;VALUE=DATE:20260112\r\nDURATION:P1W\r\nRECURRENCE-ID;VALUE=DATE:20260112"},
			unwanted:   []string{"DURATION:P7D", "DTEND"},
		},
		{
			name:       "a DTEND",
			components: [][]string{series("DTEND;VALUE=DATE:20260107", "RRULE:FREQ=WEEKLY;COUNT=2")},
			wanted:     []string{"DTSTART;VALUE=DATE:20260112\r\nDTEND;VALUE=DATE:20260114\r\nRECURRENCE-ID;VALUE=DATE:20260112"},
			unwanted:   []string{"DURATION"},
		},
		{
			name: "an override writing no end",
			components: [][]string{series("RRULE:FREQ=DAILY;COUNT=3"), componentLines("VEVENT", "UID:instant", "DTSTAMP:20250101T000000Z",
				"RECURRENCE-ID;VALUE=DATE:20260106", "DTSTART;VALUE=DATE:20260108", "SUMMARY:Moved")},
			wanted:   []string{"DTSTART;VALUE=DATE:20260108\r\nSUMMARY:Moved\r\nEND:VEVENT"},
			unwanted: []string{"DTEND", "DURATION"},
		},
		{
			name:       "a DTEND, on a PERIOD lasting no time",
			components: [][]string{series("DTEND;VALUE=DATE:20260106", "RRULE:FREQ=WEEKLY;COUNT=2", "RDATE;VALUE=PERIOD:20260108T120000Z/PT0S")},
			wanted:     []string{"DTSTART;VALUE=DATE:20260108\r\nDURATION:P0D\r\nRECURRENCE-ID;VALUE=DATE:20260108"},
		},
		{
			name:       "no end, on a PERIOD lasting no time",
			components: [][]string{series("RRULE:FREQ=WEEKLY;COUNT=2", "RDATE;VALUE=PERIOD:20260108T120000Z/PT0S")},
			wanted:     []string{"DTSTART;VALUE=DATE:20260108\r\nDURATION:P0D\r\nRECURRENCE-ID;VALUE=DATE:20260108"},
			unwanted:   []string{"DTEND"},
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			code, body := zeroLengthReport(t, zeroLengthCalendar(tt.components...),
				calendarDataQuery(`<C:expand start="20260101T000000Z" end="20260201T000000Z"/>`))
			if code != 207 {
				t.Fatalf("expand status = %d, body %s", code, body)
			}
			for _, text := range tt.wanted {
				if !strings.Contains(body, text) {
					t.Errorf("expanded output lacks %s:\n%s", text, body)
				}
			}
			for _, text := range tt.unwanted {
				if strings.Contains(body, text) {
					t.Errorf("expanded output carries %s:\n%s", text, body)
				}
			}
		})
	}
}

// A timed event whose DTEND repeats its DTSTART lasts no time, and expansion
// writes it with a zero DURATION, since RFC 5545 §3.8.2.2 has a DTEND come
// later than its DTSTART.
func TestExpandedInstantEventIsWrittenWithAZeroDuration(t *testing.T) {
	raw := zeroLengthCalendar(componentLines("VEVENT", "UID:instant", "DTSTAMP:20250101T000000Z",
		"DTSTART:20260105T100000Z", "DTEND:20260105T100000Z"))
	code, body := zeroLengthReport(t, raw, calendarDataQuery(`<C:expand start="20260101T000000Z" end="20260201T000000Z"/>`))
	if code != 207 {
		t.Fatalf("expand status = %d, body %s", code, body)
	}
	if !strings.Contains(body, "DTSTART:20260105T100000Z\r\nDURATION:PT0S") || strings.Contains(body, "DTEND") {
		t.Errorf("expanded output does not write the instant with a zero DURATION:\n%s", body)
	}
}

// A task starting on a DATE with a DURATION counting hours lasts the whole
// days it reaches into, as an event does, alone or as an instance of a series.
func TestDateTaskWithAnHourDurationLastsTheDayItReaches(t *testing.T) {
	for _, tt := range []struct {
		name  string
		extra []string
		noon  string
	}{
		{name: "alone", noon: "20260105T120000Z"},
		{name: "a later instance", extra: []string{"RRULE:FREQ=DAILY;COUNT=3"}, noon: "20260107T120000Z"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			raw := zeroLengthCalendar(componentLines("VTODO", append([]string{"UID:instant", "DTSTAMP:20250101T000000Z",
				"DTSTART;VALUE=DATE:20260105", "DURATION:PT1H"}, tt.extra...)...))
			code, body := zeroLengthReport(t, raw, todoQuery("", tt.noon, strings.Replace(tt.noon, "T12", "T13", 1)))
			if code != 207 {
				t.Fatalf("calendar-query status = %d, body %s", code, body)
			}
			if !strings.Contains(body, "instant.ics") {
				t.Errorf("noon of the task's day %s is not matched:\n%s", tt.noon, body)
			}
		})
	}
}

// sundaySeries is an all-day series of Sundays in Chicago, which take in the
// days the clocks go forward (8 March 2026) and back (1 November 2026).
var sundaySeries = zeroLengthCalendar(componentLines("VEVENT", "UID:instant", "DTSTAMP:20250101T000000Z",
	"DTSTART;VALUE=DATE:20260301", "DTEND;VALUE=DATE:20260302", "RRULE:FREQ=WEEKLY;COUNT=40"))

// Each instance of an all-day series spans its own local day, however many
// hours that day holds: calendar-query, free-busy and expansion all end it at
// the midnight ending its day, not 24 hours after it began.
func TestDateSeriesInstancesSpanTheirOwnDays(t *testing.T) {
	chicago := chicagoVTimezoneObject()
	for _, tt := range []struct {
		name, day, nextDay string
		lastHour, nextHour [2]string
		freeBusy, wantBusy string
		freeBusyEnd        string
	}{
		{
			name: "25 hours the day the clocks go back", day: "20261101", nextDay: "20261102",
			lastHour: [2]string{"20261102T051500Z", "20261102T054500Z"}, nextHour: [2]string{"20261102T061500Z", "20261102T064500Z"},
			freeBusy: "20261101T000000Z", freeBusyEnd: "20261103T000000Z", wantBusy: "FREEBUSY:20261101T050000Z/20261102T060000Z",
		},
		{
			name: "23 hours the day the clocks go forward", day: "20260308", nextDay: "20260309",
			lastHour: [2]string{"20260309T041500Z", "20260309T044500Z"}, nextHour: [2]string{"20260309T051500Z", "20260309T054500Z"},
			freeBusy: "20260308T000000Z", freeBusyEnd: "20260310T000000Z", wantBusy: "FREEBUSY:20260308T060000Z/20260309T050000Z",
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			assertZonedQuery(t, chicago, sundaySeries, tt.lastHour[0], tt.lastHour[1], true)
			assertZonedQuery(t, chicago, sundaySeries, tt.nextHour[0], tt.nextHour[1], false)
			code, body := zonedReport(t, chicago, sundaySeries, freeBusyQuery(tt.freeBusy, tt.freeBusyEnd))
			if code != 200 {
				t.Fatalf("free-busy status = %d, body %s", code, body)
			}
			assertPublishedFreeBusy(t, parsedFreeBusyLines(t, body), []string{tt.wantBusy})
			assertZonedExpansion(t, chicago, sundaySeries, tt.freeBusy, tt.freeBusyEnd,
				[]string{"DTSTART;VALUE=DATE:" + tt.day + "\r\nDTEND;VALUE=DATE:" + tt.nextDay}, nil)
		})
	}
}

// An all-day RANGE=THISANDFUTURE override moves the instances it governs by
// whole days and gives each the days it spans in its own week: its slot falls
// on the day the clocks go forward, yet the Tuesday after keeps all 24 of its
// hours. Calendar-query, free-busy and expansion place it alike.
func TestThisAndFutureDateOverrideMovesItsInstancesByDays(t *testing.T) {
	chicago := chicagoVTimezoneObject()
	for _, end := range []string{"", "DURATION:P1D", "DTEND;VALUE=DATE:20260309"} {
		t.Run("end "+end, func(t *testing.T) {
			override := []string{"UID:instant", "DTSTAMP:20250101T000000Z",
				"RECURRENCE-ID;RANGE=THISANDFUTURE;VALUE=DATE:20260308", "DTSTART;VALUE=DATE:20260308", "SUMMARY:Later"}
			if end != "" {
				override = append(override, end)
			}
			raw := zeroLengthCalendar(
				componentLines("VEVENT", "UID:instant", "DTSTAMP:20250101T000000Z",
					"DTSTART;VALUE=DATE:20260301", "DTEND;VALUE=DATE:20260302", "RRULE:FREQ=WEEKLY;BYDAY=SU,TU;COUNT=6"),
				componentLines("VEVENT", override...))
			// Tuesday 10 March, 23:30 to midnight CDT.
			assertZonedQuery(t, chicago, raw, "20260311T043000Z", "20260311T050000Z", true)
			assertZonedQuery(t, chicago, raw, "20260311T051500Z", "20260311T054500Z", false)
			code, body := zonedReport(t, chicago, raw, freeBusyQuery("20260310T000000Z", "20260312T000000Z"))
			if code != 200 {
				t.Fatalf("free-busy status = %d, body %s", code, body)
			}
			assertPublishedFreeBusy(t, parsedFreeBusyLines(t, body), []string{"FREEBUSY:20260310T050000Z/20260311T050000Z"})
			assertZonedExpansion(t, chicago, raw, "20260310T050000Z", "20260311T050000Z",
				[]string{"DTSTART;VALUE=DATE:20260310\r\nSUMMARY:Later", "RECURRENCE-ID;VALUE=DATE:20260310"}, []string{"DURATION:PT", "DTEND;VALUE=DATE:20260310"})
		})
	}
}

// The views' instances of a series in a zone with a transition, governed by an
// all-day RANGE=THISANDFUTURE override, fall on the days CalDAV expands them
// to: whole days from their own midnights, not the series' slots moved by the
// hours between them, which the transition makes 23.
func TestEventInstancesOfADateOverrideAcrossATransitionAreTheExpandedOnes(t *testing.T) {
	raw := instancesObject("",
		"DTSTART;TZID=America/New_York:20260305T023000\r\nDTEND;TZID=America/New_York:20260305T033000\r\nRRULE:FREQ=DAILY;COUNT=7\r\n",
		"RECURRENCE-ID;RANGE=THISANDFUTURE;TZID=America/New_York:20260306T023000\r\nDTSTART;VALUE=DATE:20260306\r\nSUMMARY:Later\r\n")
	start, end := mustUTC(t, "20260305T000000Z"), mustUTC(t, "20260315T000000Z")
	got := EventInstances(raw, start, end)
	want := expandedInstances(t, raw, start, end)
	if len(got.Instances) != len(want) {
		t.Fatalf("EventInstances() = %v, CalDAV expands %v", got.Instances, want)
	}
	for i, w := range want {
		g := got.Instances[i]
		if !g.Start.Equal(w.Start) || !g.End.Equal(w.End) {
			t.Errorf("instance %d = %v, CalDAV expands %v", i, g, w)
		}
		if i > 0 && (g.Start.Hour() != 0 || g.End.Sub(g.Start) != 24*time.Hour) {
			t.Errorf("governed instance %d = %v, want a whole UTC day", i, g)
		}
	}
}

// apiaTimezone is Pacific/Apia over the turn of 2011, when it crossed the date
// line by skipping 30 December.
func apiaTimezone(t *testing.T) string {
	t.Helper()
	apia, ok := ical.VTimezone("Pacific/Apia", time.Date(2011, 11, 1, 0, 0, 0, 0, time.UTC), time.Date(2012, 2, 1, 0, 0, 0, 0, time.UTC))
	if !ok {
		t.Skip("Pacific/Apia is not available")
	}
	return "BEGIN:VCALENDAR\r\nVERSION:2.0\r\nPRODID:-//CalCard//EN\r\n" + apia + "END:VCALENDAR\r\n"
}

// A DATE is floating, so an all-day series has the same dates in every zone:
// in Pacific/Apia it still has an instance on 30 December 2011, the day the
// zone skipped, and that instance spans no time, both its midnights falling on
// one instant, which expansion writes as a zero DURATION. Each instance is
// named and ended by its own dates.
func TestDateSeriesKeepsADayItsZoneSkips(t *testing.T) {
	timezone := apiaTimezone(t)
	raw := zeroLengthCalendar(componentLines("VEVENT", "UID:instant", "DTSTAMP:20250101T000000Z",
		"DTSTART;VALUE=DATE:20111226", "DTEND;VALUE=DATE:20111227", "RRULE:FREQ=DAILY;COUNT=7"))
	assertZonedExpansion(t, timezone, raw, "20111220T000000Z", "20120110T000000Z", []string{
		"DTSTART;VALUE=DATE:20111229\r\nDTEND;VALUE=DATE:20111230",
		"DTSTART;VALUE=DATE:20111230\r\nDURATION:P0D\r\nRECURRENCE-ID;VALUE=DATE:20111230",
		"DTSTART;VALUE=DATE:20120101\r\nDTEND;VALUE=DATE:20120102\r\nRECURRENCE-ID;VALUE=DATE:20120101",
	}, []string{"DTSTART;VALUE=DATE:20120102"})
	code, body := zonedReport(t, timezone, raw, freeBusyQuery("20111220T000000Z", "20120110T000000Z"))
	if code != 200 {
		t.Fatalf("free-busy status = %d, body %s", code, body)
	}
	assertPublishedFreeBusy(t, parsedFreeBusyLines(t, body), []string{"FREEBUSY:20111226T100000Z/20120101T100000Z"})
}

// A series starting on a day the clocks go forward measures 23 hours from its
// first midnight to the next, which is its one day: an all-day override
// writing no end takes that day, not the hours it held.
func TestDateOverrideTakesTheDayOfASeriesStartingOnAShortDay(t *testing.T) {
	chicago := chicagoVTimezoneObject()
	raw := zeroLengthCalendar(
		componentLines("VEVENT", "UID:instant", "DTSTAMP:20250101T000000Z",
			"DTSTART;VALUE=DATE:20260308", "RRULE:FREQ=DAILY;COUNT=3"),
		componentLines("VEVENT", "UID:instant", "DTSTAMP:20250101T000000Z",
			"RECURRENCE-ID;VALUE=DATE:20260309", "DTSTART;VALUE=DATE:20260312"))
	// Thursday 12 March, 23:30 to midnight CDT.
	assertZonedQuery(t, chicago, raw, "20260313T043000Z", "20260313T050000Z", true)
	code, body := zonedReport(t, chicago, raw, freeBusyQuery("20260312T000000Z", "20260314T000000Z"))
	if code != 200 {
		t.Fatalf("free-busy status = %d, body %s", code, body)
	}
	assertPublishedFreeBusy(t, parsedFreeBusyLines(t, body), []string{"FREEBUSY:20260312T050000Z/20260313T050000Z"})
}

// Floating times are read on the clock of the zone the report gives, so in
// Pacific/Kiritimati, which skipped 31 December 1994, 01:00 that day is the
// instant of 01:00 on 1 January and the two slots are one instance. A
// THISANDFUTURE override moving the series onto dates places that instance by
// the date the first of them is written on, as the override itself is written,
// however narrow the range asked. Writing no end, it spans that skipped day,
// which takes no time.
func TestTimedSlotsOnOneInstantMoveOntoOneDateWhateverTheRange(t *testing.T) {
	kiritimati, ok := ical.VTimezone("Pacific/Kiritimati", time.Date(1994, 11, 1, 0, 0, 0, 0, time.UTC), time.Date(1995, 2, 1, 0, 0, 0, 0, time.UTC))
	if !ok {
		t.Skip("Pacific/Kiritimati is not available")
	}
	timezone := "BEGIN:VCALENDAR\r\nVERSION:2.0\r\nPRODID:-//CalCard//EN\r\n" + kiritimati + "END:VCALENDAR\r\n"
	raw := zeroLengthCalendar(
		componentLines("VEVENT", "UID:instant", "DTSTAMP:20250101T000000Z",
			"DTSTART:19941228T010000", "DTEND:19941228T020000", "RRULE:FREQ=DAILY;COUNT=8"),
		componentLines("VEVENT", "UID:instant", "DTSTAMP:20250101T000000Z",
			"RECURRENCE-ID;RANGE=THISANDFUTURE:19941231T010000", "DTSTART;VALUE=DATE:19941231"))
	assertZonedExpansion(t, timezone, raw, "19941220T000000Z", "19950110T000000Z",
		[]string{"DTSTART;VALUE=DATE:19941231\r\nRECURRENCE-ID:", "DTSTART;VALUE=DATE:19950102"},
		[]string{"DTSTART;VALUE=DATE:19950101"})
	// Noon and the last second of 1 January in Kiritimati.
	assertZonedQuery(t, timezone, raw, "19941231T220000Z", "19941231T220100Z", false)
	assertZonedQuery(t, timezone, raw, "19950101T095959Z", "19950101T100000Z", false)
	code, body := zonedReport(t, timezone, raw, freeBusyQuery("19941220T000000Z", "19950110T000000Z"))
	if code != 200 {
		t.Fatalf("free-busy status = %d, body %s", code, body)
	}
	assertPublishedFreeBusy(t, parsedFreeBusyLines(t, body), []string{
		"FREEBUSY:19941228T110000Z/19941228T120000Z", "FREEBUSY:19941229T110000Z/19941229T120000Z",
		"FREEBUSY:19941230T110000Z/19941230T120000Z", "FREEBUSY:19950101T100000Z/19950104T100000Z",
	})
}
