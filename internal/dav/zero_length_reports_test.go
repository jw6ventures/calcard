package dav

import (
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/jw6ventures/calcard/internal/auth"
	"github.com/jw6ventures/calcard/internal/store"
)

// zeroLengthReport runs one REPORT against a calendar holding raw alone.
func zeroLengthReport(t *testing.T, raw, body string) (int, string) {
	t.Helper()
	h := hostileRecurrenceServer()
	h.store.Events = &fakeEventRepo{events: map[string]*store.Event{
		"1:instant": {ID: 1, CalendarID: 1, UID: "instant", ResourceName: "instant", ETag: "e", RawICAL: raw},
	}}
	req := httptest.NewRequest("REPORT", "/dav/calendars/1/", strings.NewReader(body))
	req.Header.Set("Depth", "1")
	rr := httptest.NewRecorder()
	h.Report(rr, req.WithContext(auth.WithUser(req.Context(), &store.User{ID: 1})))
	return rr.Code, rr.Body.String()
}

func calendarDataQuery(transform string) string {
	return `<C:calendar-query xmlns:D="DAV:" xmlns:C="urn:ietf:params:xml:ns:caldav"><D:prop><C:calendar-data>` +
		transform + `</C:calendar-data></D:prop><C:filter><C:comp-filter name="VCALENDAR"><C:comp-filter name="VEVENT"/></C:comp-filter></C:filter></C:calendar-query>`
}

func zeroLengthCalendar(components ...[]string) string {
	lines := []string{"VERSION:2.0", "PRODID:-//CalCard//EN"}
	for _, component := range components {
		lines = append(lines, component...)
	}
	return wrapCalendar(lines...)
}

// §9.6.5 selects expanded instances with the §9.9 logic, so an instant series
// yields the occurrence at the range start and leaves out the one at its end,
// whichever way the zero length is written.
func TestZeroLengthSeriesExpandsWithTheInstantAtRangeStart(t *testing.T) {
	spellings := []struct {
		name                 string
		properties           []string
		rangeStart, rangeEnd string
		included, excluded   []string
	}{
		{
			name:       "DTEND equal to DTSTART",
			properties: []string{"DTSTART:20260105T100000Z", "DTEND:20260105T100000Z"},
			rangeStart: "20260107T100000Z", rangeEnd: "20260109T100000Z",
			included: []string{"RECURRENCE-ID:20260107T100000Z", "RECURRENCE-ID:20260108T100000Z"},
			excluded: []string{"RECURRENCE-ID:20260106T100000Z", "RECURRENCE-ID:20260109T100000Z"},
		},
		{
			name:       "DURATION PT0S",
			properties: []string{"DTSTART:20260105T100000Z", "DURATION:PT0S"},
			rangeStart: "20260107T100000Z", rangeEnd: "20260109T100000Z",
			included: []string{"RECURRENCE-ID:20260107T100000Z", "RECURRENCE-ID:20260108T100000Z"},
			excluded: []string{"RECURRENCE-ID:20260106T100000Z", "RECURRENCE-ID:20260109T100000Z"},
		},
		{
			name:       "DATE DTSTART with DURATION P0D",
			properties: []string{"DTSTART;VALUE=DATE:20260105", "DURATION:P0D"},
			rangeStart: "20260107T000000Z", rangeEnd: "20260109T000000Z",
			included: []string{"RECURRENCE-ID;VALUE=DATE:20260107", "RECURRENCE-ID;VALUE=DATE:20260108"},
			excluded: []string{"RECURRENCE-ID;VALUE=DATE:20260106", "RECURRENCE-ID;VALUE=DATE:20260109"},
		},
	}
	for _, spelling := range spellings {
		t.Run(spelling.name, func(t *testing.T) {
			master := append([]string{"UID:instant", "DTSTAMP:20260101T000000Z", "RRULE:FREQ=DAILY;COUNT=10"}, spelling.properties...)
			raw := zeroLengthCalendar(componentLines("VEVENT", master...))
			code, body := zeroLengthReport(t, raw, calendarDataQuery(`<C:expand start="`+spelling.rangeStart+`" end="`+spelling.rangeEnd+`"/>`))
			if code != 207 {
				t.Fatalf("status = %d, body %s", code, body)
			}
			for _, want := range spelling.included {
				if !strings.Contains(body, want) {
					t.Errorf("expanded output lacks %s:\n%s", want, body)
				}
			}
			for _, unwanted := range spelling.excluded {
				if strings.Contains(body, unwanted) {
					t.Errorf("expanded output carries %s:\n%s", unwanted, body)
				}
			}
		})
	}
}

// §9.6.6 keeps an override when its original or current time impacts the
// range, judged as §9.9 judges the instance. For an instant series that is
// start <= instant < end, for an ordinary override and for the instances a
// RANGE=THISANDFUTURE override governs.
func TestZeroLengthSeriesLimitsOverridesByTheirInstants(t *testing.T) {
	type zeroLengthSpelling struct {
		name string
		end  func(start string) string
	}
	spellings := []zeroLengthSpelling{
		{name: "DTEND equal to DTSTART", end: func(start string) string { return "DTEND:" + start }},
		{name: "DURATION PT0S", end: func(string) string { return "DURATION:PT0S" }},
	}
	tests := []struct {
		name       string
		override   []string
		rangeStart string
		rangeEnd   string
		want       bool
	}{
		{
			name:       "original instant at range start",
			override:   []string{"RECURRENCE-ID:20260107T100000Z", "DTSTART:20260107T150000Z"},
			rangeStart: "20260107T100000Z", rangeEnd: "20260107T110000Z", want: true,
		},
		{
			name:       "current instant at range start",
			override:   []string{"RECURRENCE-ID:20260107T100000Z", "DTSTART:20260107T150000Z"},
			rangeStart: "20260107T150000Z", rangeEnd: "20260107T160000Z", want: true,
		},
		{
			name:       "original and current instants at range end",
			override:   []string{"RECURRENCE-ID:20260107T100000Z", "DTSTART:20260107T150000Z"},
			rangeStart: "20260107T090000Z", rangeEnd: "20260107T100000Z", want: false,
		},
		{
			name:       "governed slot's original instant at range start",
			override:   []string{"RECURRENCE-ID;RANGE=THISANDFUTURE:20260106T100000Z", "DTSTART:20260106T120000Z"},
			rangeStart: "20260108T100000Z", rangeEnd: "20260108T110000Z", want: true,
		},
		{
			name:       "governed slot's moved instant at range start",
			override:   []string{"RECURRENCE-ID;RANGE=THISANDFUTURE:20260106T100000Z", "DTSTART:20260106T120000Z"},
			rangeStart: "20260108T120000Z", rangeEnd: "20260108T130000Z", want: true,
		},
		{
			name:       "governed slot's moved instant at range end",
			override:   []string{"RECURRENCE-ID;RANGE=THISANDFUTURE:20260106T100000Z", "DTSTART:20260106T120000Z"},
			rangeStart: "20260108T110000Z", rangeEnd: "20260108T120000Z", want: false,
		},
	}
	for _, spelling := range spellings {
		for _, tt := range tests {
			t.Run(spelling.name+"/"+tt.name, func(t *testing.T) {
				master := []string{"UID:instant", "DTSTAMP:20260101T000000Z", "RRULE:FREQ=DAILY;COUNT=10",
					"DTSTART:20260105T100000Z", spelling.end("20260105T100000Z")}
				overrideStart := strings.TrimPrefix(tt.override[1], "DTSTART:")
				override := append([]string{"UID:instant", "DTSTAMP:20260101T000000Z", "SUMMARY:moved"}, tt.override...)
				override = append(override, spelling.end(overrideStart))
				raw := zeroLengthCalendar(componentLines("VEVENT", master...), componentLines("VEVENT", override...))
				code, body := zeroLengthReport(t, raw, calendarDataQuery(`<C:limit-recurrence-set start="`+tt.rangeStart+`" end="`+tt.rangeEnd+`"/>`))
				if code != 207 {
					t.Fatalf("status = %d, body %s", code, body)
				}
				if got := strings.Contains(body, "SUMMARY:moved"); got != tt.want {
					t.Fatalf("override kept = %v, want %v:\n%s", got, tt.want, body)
				}
			})
		}
	}
}

// An all-day instant written with a zero DURATION is the midnight its DATE
// names, not the day that follows: a series of them governs no override for a
// range later that day.
func TestZeroLengthDateSeriesLimitsOverridesByTheirInstants(t *testing.T) {
	master := []string{"UID:instant", "DTSTAMP:20260101T000000Z", "RRULE:FREQ=DAILY;COUNT=10", "DTSTART;VALUE=DATE:20260105", "DURATION:P0D"}
	override := []string{"UID:instant", "DTSTAMP:20260101T000000Z", "SUMMARY:moved",
		"RECURRENCE-ID;VALUE=DATE:20260107", "DTSTART;VALUE=DATE:20260120", "DURATION:P0D"}
	raw := zeroLengthCalendar(componentLines("VEVENT", master...), componentLines("VEVENT", override...))
	for _, tt := range []struct {
		rangeStart, rangeEnd string
		want                 bool
	}{
		{rangeStart: "20260107T000000Z", rangeEnd: "20260107T010000Z", want: true},
		{rangeStart: "20260107T120000Z", rangeEnd: "20260107T130000Z", want: false},
	} {
		code, body := zeroLengthReport(t, raw, calendarDataQuery(`<C:limit-recurrence-set start="`+tt.rangeStart+`" end="`+tt.rangeEnd+`"/>`))
		if code != 207 {
			t.Fatalf("status = %d, body %s", code, body)
		}
		if got := strings.Contains(body, "SUMMARY:moved"); got != tt.want {
			t.Fatalf("[%s, %s): override kept = %v, want %v:\n%s", tt.rangeStart, tt.rangeEnd, got, tt.want, body)
		}
	}
}

// A zero-length instance occupies no time, so §7.10 publishes no busy period
// for it in any spelling, alone or as an occurrence of a series.
func TestZeroLengthEventsPublishNoBusyTime(t *testing.T) {
	spellings := map[string][]string{
		"DTEND equal to DTSTART":         {"DTSTART:20260107T100000Z", "DTEND:20260107T100000Z"},
		"DURATION PT0S":                  {"DTSTART:20260107T100000Z", "DURATION:PT0S"},
		"DATE DTSTART with DURATION P0D": {"DTSTART;VALUE=DATE:20260107", "DURATION:P0D"},
	}
	const query = `<C:free-busy-query xmlns:C="urn:ietf:params:xml:ns:caldav"><C:time-range start="20260106T000000Z" end="20260109T000000Z"/></C:free-busy-query>`
	for name, properties := range spellings {
		for shape, component := range map[string][]string{
			"single": append([]string{"UID:instant", "DTSTAMP:20260101T000000Z"}, properties...),
			"series": append([]string{"DTSTAMP:20260101T000000Z"}, zeroLengthSeries("instant", properties)...),
		} {
			t.Run(name+"/"+shape, func(t *testing.T) {
				code, body := zeroLengthReport(t, zeroLengthCalendar(componentLines("VEVENT", component...)), query)
				if code != 200 {
					t.Fatalf("status = %d, body %s", code, body)
				}
				if !strings.Contains(body, "BEGIN:VFREEBUSY") || strings.Contains(body, "FREEBUSY:") || strings.Contains(body, "FREEBUSY;") {
					t.Fatalf("free-busy for %v publishes busy time:\n%s", component, body)
				}
			})
		}
	}
}
