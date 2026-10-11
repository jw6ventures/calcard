package ui

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/jw6ventures/calcard/internal/auth"
	"github.com/jw6ventures/calcard/internal/config"
	"github.com/jw6ventures/calcard/internal/dav"
	"github.com/jw6ventures/calcard/internal/store"
)

// seriesObject wraps components, each the lines of a VEVENT between BEGIN and
// END, in a VCALENDAR carrying timezones.
func seriesObject(timezones string, components ...string) string {
	var b strings.Builder
	b.WriteString("BEGIN:VCALENDAR\r\nVERSION:2.0\r\nPRODID:-//Client//EN\r\n" + timezones)
	for _, component := range components {
		b.WriteString("BEGIN:VEVENT\r\nUID:series\r\nDTSTAMP:20240101T000000Z\r\nSUMMARY:Series\r\n" + component + "END:VEVENT\r\n")
	}
	b.WriteString("END:VCALENDAR\r\n")
	return b.String()
}

// servedSeries is the events JSON one calendar view gets for the dates
// [start, end) from a calendar holding raw alone, and the payload of raw. raw
// is stored as a write stores it when the validator accepts it, and as it is,
// as data stored before the validator existed, when it does not.
func servedSeries(t *testing.T, raw, start, end string) (string, map[string]any) {
	t.Helper()
	stored := &store.Event{ID: 1, CalendarID: 1, UID: "series", ResourceName: "series.ics", RawICAL: raw, ETag: "e"}
	if object, err := dav.ValidateCalendarObject(raw, nil); err == nil {
		stored.RawICAL = object.Body
		stored.DTStart, stored.DTEnd, stored.AllDay = object.Metadata.DTStart, object.Metadata.DTEnd, object.Metadata.AllDay
	}
	s := &store.Store{
		Calendars: &fakeCalendarRepo{calendars: map[int64]*store.Calendar{1: {ID: 1, UserID: 100, Name: "Work"}}},
		Events:    &fakeEventRepo{events: map[string]*store.Event{"1:series": stored}},
	}
	req := httptest.NewRequest(http.MethodGet, "/calendars/1/events.json?start="+start+"&end="+end, nil)
	req = withRouteID(req.WithContext(auth.WithUser(req.Context(), &store.User{ID: 100})), "1")
	w := httptest.NewRecorder()
	NewHandler(&config.Config{}, s, nil, dav.ValidateCalendarObject, dav.EventInstances).GetCalendarEventsJSON(w, req)
	var payload []map[string]any
	if err := json.Unmarshal(w.Body.Bytes(), &payload); err != nil || w.Code != http.StatusOK {
		t.Fatalf("events JSON = %d %s (%v)", w.Code, w.Body.String(), err)
	}
	if len(payload) != 1 {
		t.Fatalf("events JSON holds %d events, want the series: %s", len(payload), w.Body.String())
	}
	return w.Body.String(), payload[0]
}

// instanceStarts are the starts of the instances payload lists, failing when
// it lists none or calls the listing partial.
func instanceStarts(t *testing.T, payload map[string]any) []string {
	t.Helper()
	listed, ok := payload["instances"].([]any)
	if !ok {
		t.Fatalf("the series' view model lists no instances: %v", payload)
	}
	if complete, _ := payload["instancesComplete"].(bool); !complete {
		t.Fatalf("the series' instances are listed as partial: %v", payload)
	}
	starts := make([]string, 0, len(listed))
	for _, instance := range listed {
		starts = append(starts, instance.(map[string]any)["start"].(string))
	}
	return starts
}

// The calendar views show a series on the instances the view model lists, so
// the listing has to be the series' instances as CalDAV expands them: for
// ordinal BYDAY, BYSETPOS, a day of the month some months lack, RDATE, series
// started years before the view, and every reading of a zone, UNTIL, COUNT
// and EXDATE. The view asks for its own local dates, so the listing covers the
// instants any browser's midnights of those dates begin and end.
func TestEventsJSONListsTheInstancesCalDAVExpands(t *testing.T) {
	berlin := func(day, wall string) string { return "DTSTART;TZID=Europe/Berlin:" + day + "T" + wall + "\r\n" }
	eastern := func(day, wall string) string {
		return "DTSTART;TZID=\"Eastern Standard Time\":" + day + "T" + wall + "\r\n"
	}
	tests := []struct {
		name       string
		timezones  string
		components []string
		start, end string
		want       []string
	}{
		{
			name:       "the second Monday of each month",
			components: []string{"DTSTART:20260112T090000Z\r\nRRULE:FREQ=MONTHLY;BYDAY=2MO\r\n"},
			start:      "2026-03-01", end: "2026-05-01",
			want: []string{"2026-03-09T09:00:00Z", "2026-04-13T09:00:00Z"},
		},
		{
			name:       "the last weekday of each month",
			components: []string{"DTSTART:20260130T090000Z\r\nRRULE:FREQ=MONTHLY;BYDAY=MO,TU,WE,TH,FR;BYSETPOS=-1\r\n"},
			start:      "2026-10-01", end: "2026-11-01",
			want: []string{"2026-10-30T09:00:00Z"},
		},
		{
			name:       "the 31st, in months that have one",
			components: []string{"DTSTART:20260131T090000Z\r\nRRULE:FREQ=MONTHLY\r\n"},
			start:      "2026-03-01", end: "2026-05-01",
			want: []string{"2026-03-31T09:00:00Z"},
		},
		{
			name: "RDATE and EXDATE",
			components: []string{"DTSTART:20261005T090000Z\r\nRRULE:FREQ=WEEKLY;COUNT=4\r\n" +
				"RDATE:20261008T150000Z\r\nEXDATE:20261012T090000Z\r\n"},
			start: "2026-10-01", end: "2026-11-01",
			want: []string{"2026-10-05T09:00:00Z", "2026-10-08T15:00:00Z", "2026-10-19T09:00:00Z", "2026-10-26T09:00:00Z"},
		},
		{
			name:       "RDATE alone",
			components: []string{"DTSTART:20261005T090000Z\r\nRDATE:20261205T090000Z\r\n"},
			start:      "2026-12-01", end: "2027-01-01",
			want: []string{"2026-12-05T09:00:00Z"},
		},
		{
			// 09:00 UTC on 1 November is still 31 October west of UTC.
			name:       "a daily series started two years before the month",
			components: []string{"DTSTART:20240101T090000Z\r\nRRULE:FREQ=DAILY\r\n"},
			start:      "2026-10-01", end: "2026-11-01",
			want: dailyStarts("2026-10-01T09:00:00Z", 32),
		},
		{
			name:       "a weekly series started a decade before the month",
			components: []string{"DTSTART;TZID=America/New_York:20150105T090000\r\nRRULE:FREQ=WEEKLY\r\n"},
			start:      "2026-10-01", end: "2026-11-01",
			want: []string{"2026-10-05T13:00:00Z", "2026-10-12T13:00:00Z", "2026-10-19T13:00:00Z", "2026-10-26T13:00:00Z"},
		},
		{
			name:       "an EXDATE removes an instance COUNT already bounded",
			components: []string{berlin("20260601", "090000") + "RRULE:FREQ=DAILY;COUNT=5\r\nEXDATE;TZID=Europe/Berlin:20260603T090000\r\n"},
			start:      "2026-06-01", end: "2026-07-01",
			want: []string{"2026-06-01T07:00:00Z", "2026-06-02T07:00:00Z", "2026-06-04T07:00:00Z", "2026-06-05T07:00:00Z"},
		},
		{
			name:       "an EXDATE on the DTSTART",
			components: []string{"DTSTART:20260601T090000Z\r\nRRULE:FREQ=DAILY;COUNT=3\r\nEXDATE:20260601T090000Z\r\n"},
			start:      "2026-06-01", end: "2026-07-01",
			want: []string{"2026-06-02T09:00:00Z", "2026-06-03T09:00:00Z"},
		},
		{
			name:       "an all-day series names its EXDATE by date",
			components: []string{"DTSTART;VALUE=DATE:20260601\r\nRRULE:FREQ=DAILY;COUNT=5\r\nEXDATE;VALUE=DATE:20260603\r\n"},
			start:      "2026-06-01", end: "2026-07-01",
			want: []string{"2026-06-01T00:00:00Z", "2026-06-02T00:00:00Z", "2026-06-04T00:00:00Z", "2026-06-05T00:00:00Z"},
		},
		{
			// 29 March 2026 has no 02:30 in Berlin, so it is no instance and
			// does not count, while 22 March is one the EXDATE removes.
			name:       "a skipped local time and an EXDATE in one series",
			components: []string{berlin("20260315", "023000") + "RRULE:FREQ=WEEKLY;COUNT=4\r\nEXDATE;TZID=Europe/Berlin:20260322T023000\r\n"},
			start:      "2026-03-01", end: "2026-04-30",
			want: []string{"2026-03-15T01:30:00Z", "2026-04-05T00:30:00Z", "2026-04-12T00:30:00Z"},
		},
		{
			name:       "a repeated local time is its first occurrence",
			components: []string{berlin("20261018", "023000") + "RRULE:FREQ=WEEKLY;COUNT=3\r\n"},
			start:      "2026-10-01", end: "2026-11-30",
			want: []string{"2026-10-18T00:30:00Z", "2026-10-25T00:30:00Z", "2026-11-01T01:30:00Z"},
		},
		{
			name:       "daily 02:30 across the New York spring change",
			components: []string{"DTSTART;TZID=America/New_York:20260307T023000\r\nRRULE:FREQ=DAILY;COUNT=3\r\n"},
			start:      "2026-03-01", end: "2026-04-01",
			want: []string{"2026-03-07T07:30:00Z", "2026-03-09T06:30:00Z", "2026-03-10T06:30:00Z"},
		},
		{
			name:       "a UTC UNTIL bounds the series on its instants",
			components: []string{berlin("20260315", "023000") + "RRULE:FREQ=WEEKLY;UNTIL=20260405T003000Z\r\n"},
			start:      "2026-03-01", end: "2026-04-30",
			want: []string{"2026-03-15T01:30:00Z", "2026-03-22T01:30:00Z", "2026-04-05T00:30:00Z"},
		},
		{
			// A DATE UNTIL ends where its day ends in the series' zone: 22:00
			// in Los Angeles on 10 January is 11 January in UTC.
			name:       "a DATE UNTIL in a zone west of UTC",
			components: []string{"DTSTART;TZID=America/Los_Angeles:20260105T220000\r\nRRULE:FREQ=DAILY;UNTIL=20260110\r\n"},
			start:      "2026-01-01", end: "2026-02-01",
			want: dailyStarts("2026-01-06T06:00:00Z", 6),
		},
		{
			name:       "a DATE UNTIL in a zone east of UTC",
			components: []string{"DTSTART;TZID=Pacific/Auckland:20260226T090000\r\nRRULE:FREQ=DAILY;UNTIL=20260302\r\n"},
			start:      "2026-02-01", end: "2026-03-31",
			want: dailyStarts("2026-02-25T20:00:00Z", 5),
		},
		{
			// Santiago skips midnight on 6 September 2026, so 5 September ends
			// at 04:00 UTC.
			name:       "a DATE UNTIL whose next midnight is skipped",
			components: []string{"DTSTART;TZID=America/Santiago:20260901T233000\r\nRRULE:FREQ=DAILY;UNTIL=20260905\r\n"},
			start:      "2026-09-01", end: "2026-10-01",
			want: dailyStarts("2026-09-02T03:30:00Z", 5),
		},
		{
			// ...and reads 23:00-24:00 on 4 April 2026 twice.
			name:       "a DATE UNTIL on a day whose last hour repeats",
			components: []string{"DTSTART;TZID=America/Santiago:20260401T233000\r\nRRULE:FREQ=DAILY;UNTIL=20260404\r\n"},
			start:      "2026-04-01", end: "2026-05-01",
			want: dailyStarts("2026-04-02T02:30:00Z", 4),
		},
		{
			name:       "a floating UNTIL read in the series' zone",
			components: []string{berlin("20260105", "090000") + "RRULE:FREQ=WEEKLY;UNTIL=20260302T090000\r\n"},
			start:      "2026-03-01", end: "2026-04-01",
			want: []string{"2026-03-02T08:00:00Z"},
		},
		{
			name:       "the UTC UNTIL the editor writes",
			components: []string{berlin("20260105", "090000") + "RRULE:FREQ=WEEKLY;UNTIL=20260302T225959Z\r\n"},
			start:      "2026-03-01", end: "2026-04-01",
			want: []string{"2026-03-02T08:00:00Z"},
		},
		{
			name:       "an all-day series ending on a DATE UNTIL",
			components: []string{"DTSTART;VALUE=DATE:20260105\r\nRRULE:FREQ=WEEKLY;UNTIL=20260302\r\n"},
			start:      "2026-03-01", end: "2026-04-01",
			want: []string{"2026-03-02T00:00:00Z"},
		},
		{
			name:       "a zone only the object defines, across its spring change",
			timezones:  outlookEastern,
			components: []string{eastern("20240304", "090000") + "RRULE:FREQ=WEEKLY;BYDAY=MO;COUNT=4\r\n"},
			start:      "2024-03-01", end: "2024-04-01",
			want: []string{"2024-03-04T14:00:00Z", "2024-03-11T13:00:00Z", "2024-03-18T13:00:00Z", "2024-03-25T13:00:00Z"},
		},
		{
			name:       "a zone only the object defines leaves out its skipped hour",
			timezones:  outlookEastern,
			components: []string{eastern("20240309", "023000") + "RRULE:FREQ=DAILY;COUNT=4\r\n"},
			start:      "2024-03-01", end: "2024-04-01",
			want: []string{"2024-03-09T07:30:00Z", "2024-03-11T06:30:00Z", "2024-03-12T06:30:00Z", "2024-03-13T06:30:00Z"},
		},
		{
			name:       "a zone only the object defines repeats an hour",
			timezones:  outlookEastern,
			components: []string{eastern("20241102", "013000") + "RRULE:FREQ=DAILY;COUNT=3\r\n"},
			start:      "2024-11-01", end: "2024-12-01",
			want: []string{"2024-11-02T05:30:00Z", "2024-11-03T05:30:00Z", "2024-11-04T06:30:00Z"},
		},
		{
			// The server reads a TZID nothing defines as UTC.
			name:       "a zone nothing defines",
			components: []string{"DTSTART;TZID=Not/AZone:20260302T233000\r\nRRULE:FREQ=WEEKLY;BYDAY=MO;COUNT=3\r\n"},
			start:      "2026-03-01", end: "2026-04-01",
			want: []string{"2026-03-02T23:30:00Z", "2026-03-09T23:30:00Z", "2026-03-16T23:30:00Z"},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, payload := servedSeries(t, seriesObject(tt.timezones, tt.components...), tt.start, tt.end)
			if got := instanceStarts(t, payload); !slices.Equal(got, tt.want) {
				t.Errorf("instances = %v, want %v", got, tt.want)
			}
		})
	}
}

// dailyStarts is n starts a day apart from first, an RFC 3339 UTC instant.
func dailyStarts(first string, n int) []string {
	start, err := time.Parse(time.RFC3339, first)
	if err != nil {
		panic(err)
	}
	starts := make([]string, n)
	for i := range starts {
		starts[i] = start.AddDate(0, 0, i).Format("2006-01-02T15:04:05Z")
	}
	return starts
}

// A view shows the dates the browser has locally, so an instance at 20:00 UTC
// on 10 March is on the Tokyo day view of 11 March, and one at 02:00 UTC on
// 11 March on the New York day view of 10 March. Both the first instance of a
// series and an event that does not recur have to reach those views, as does
// an event in a zone east of UTC that the far west sees two dates earlier
// than its own zone does.
func TestEventsJSONServesTheDatesTheBrowserHasLocally(t *testing.T) {
	tests := []struct {
		name, component, start, end string
	}{
		{"an event on the next UTC date east of UTC", "DTSTART:20260310T200000Z\r\nDTEND:20260310T210000Z\r\n", "2026-03-11", "2026-03-12"},
		{"an event on the previous UTC date west of UTC", "DTSTART:20260311T020000Z\r\nDTEND:20260311T030000Z\r\n", "2026-03-10", "2026-03-11"},
		{"a series starting on the next UTC date east of UTC", "DTSTART:20260310T200000Z\r\nRRULE:FREQ=WEEKLY\r\n", "2026-03-11", "2026-03-12"},
		{"a series starting on the previous UTC date west of UTC", "DTSTART:20260311T020000Z\r\nRRULE:FREQ=WEEKLY\r\n", "2026-03-10", "2026-03-11"},
		{"a floating event a viewer west of UTC sees the day before", "DTSTART:20261101T050000\r\nDTEND:20261101T060000\r\n", "2026-10-31", "2026-11-01"},
		{"a floating series a viewer west of UTC sees the day before", "DTSTART:20261101T050000\r\nRRULE:FREQ=DAILY;UNTIL=20261110T050000\r\n", "2026-10-31", "2026-11-01"},
		{
			// 01:00 on 12 March in Kiritimati (+14) is 23:00 on 10 March at -12.
			"an event two dates earlier in the far west than in its own zone",
			"DTSTART;TZID=Pacific/Kiritimati:20260312T010000\r\nDTEND;TZID=Pacific/Kiritimati:20260312T020000\r\n",
			"2026-03-10", "2026-03-11",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, payload := servedSeries(t, seriesObject("", tt.component), tt.start, tt.end)
			if _, recurs := payload["rrule"]; recurs && len(instanceStarts(t, payload)) == 0 {
				t.Errorf("the series lists no instance for the view: %v", payload)
			}
		})
	}
}

// A series' UNTIL bounds the instances its rule generates, not the ones an
// RDATE adds or an override moves, and an instance that starts before a view
// can last into it, so none of those keeps a series out of a view CalDAV
// shows it in.
func TestEventsJSONServesSeriesInstancesPastTheirUntil(t *testing.T) {
	tests := []struct {
		name       string
		components []string
		start, end string
		want       []string
	}{
		{"an RDATE after the UNTIL", []string{"DTSTART:20261005T090000Z\r\nRRULE:FREQ=WEEKLY;UNTIL=20261020T000000Z\r\nRDATE:20261105T090000Z\r\n"},
			"2026-11-01", "2026-12-01", []string{"2026-11-05T09:00:00Z"}},
		{"an override moved past the UNTIL", []string{"DTSTART:20261005T090000Z\r\nRRULE:FREQ=WEEKLY;UNTIL=20261020T000000Z\r\n",
			"RECURRENCE-ID:20261019T090000Z\r\nDTSTART:20261103T090000Z\r\nSUMMARY:Moved\r\n"},
			"2026-11-01", "2026-12-01", []string{}},
		{"an all-day instance lasting into the view", []string{"DTSTART;VALUE=DATE:20261029\r\nDTEND;VALUE=DATE:20261101\r\nRRULE:FREQ=WEEKLY;UNTIL=20261030\r\n"},
			"2026-10-31", "2026-11-07", []string{"2026-10-29T00:00:00Z"}},
		{"a timed instance lasting into the view", []string{"DTSTART:20261028T090000Z\r\nDURATION:P4D\r\nRRULE:FREQ=WEEKLY;UNTIL=20261029T000000Z\r\n"},
			"2026-11-01", "2026-11-07", []string{"2026-10-28T09:00:00Z"}},
		{"an RDATE period lasting into the view", []string{"DTSTART:20261005T090000Z\r\nDTEND:20261005T100000Z\r\n" +
			"RRULE:FREQ=WEEKLY;UNTIL=20261020T000000Z\r\nRDATE;VALUE=PERIOD:20261025T090000Z/20261103T090000Z\r\n"},
			"2026-11-01", "2026-11-07", []string{"2026-10-25T09:00:00Z"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, payload := servedSeries(t, seriesObject("", tt.components...), tt.start, tt.end)
			if got := instanceStarts(t, payload); !slices.Equal(got, tt.want) {
				t.Errorf("instances = %v, want %v", got, tt.want)
			}
		})
	}
}

// A series is left out of a view once nothing of it can be seen there: its
// rule's UNTIL has passed, and so have the dates its RDATEs and overrides
// place instances on. A calendar holds many such series for every one still
// running.
func TestEventsJSONLeavesOutEndedSeries(t *testing.T) {
	for name, components := range map[string][]string{
		"an ended series with an override": {"DTSTART:20200106T090000Z\r\nDTEND:20200106T100000Z\r\nRRULE:FREQ=WEEKLY;UNTIL=20200301T000000Z\r\n",
			"RECURRENCE-ID:20200113T090000Z\r\nDTSTART:20200114T090000Z\r\nDTEND:20200114T100000Z\r\nSUMMARY:Moved\r\n"},
		"an ended series with an RDATE": {"DTSTART:20200106T090000Z\r\nRRULE:FREQ=WEEKLY;UNTIL=20200301T000000Z\r\nRDATE:20200401T090000Z\r\n"},
		"a series of past dates":        {"DTSTART:20200106T090000Z\r\nRDATE:20200401T090000Z,20200501T090000Z\r\n"},
	} {
		t.Run(name, func(t *testing.T) {
			s := &store.Store{
				Calendars: &fakeCalendarRepo{calendars: map[int64]*store.Calendar{1: {ID: 1, UserID: 100, Name: "Work"}}},
				Events: &fakeEventRepo{events: map[string]*store.Event{
					"1:series": {ID: 1, CalendarID: 1, UID: "series", ResourceName: "series.ics", RawICAL: seriesObject("", components...), ETag: "e"},
				}},
			}
			req := httptest.NewRequest(http.MethodGet, "/calendars/1/events.json?start=2026-10-01&end=2026-11-01", nil)
			req = withRouteID(req.WithContext(auth.WithUser(req.Context(), &store.User{ID: 100})), "1")
			w := httptest.NewRecorder()
			NewHandler(&config.Config{}, s, nil, dav.ValidateCalendarObject, dav.EventInstances).GetCalendarEventsJSON(w, req)
			if body := strings.TrimSpace(w.Body.String()); w.Code != http.StatusOK || body != "[]" {
				t.Errorf("events JSON = %d %s, want the series left out", w.Code, body)
			}
		})
	}
}

// Only RANGE=THISANDFUTURE makes an override govern the instances after its
// own. One whose RECURRENCE-ID carries another RANGE replaces its instance
// and stands for itself, as CalDAV expands it.
func TestEventsJSONGivesOnlyThisAndFutureOverridesARange(t *testing.T) {
	_, payload := servedSeries(t, seriesObject("",
		"DTSTART:20261005T090000Z\r\nRRULE:FREQ=WEEKLY;COUNT=4\r\n",
		"RECURRENCE-ID;RANGE=THISANDPRIOR:20261012T090000Z\r\nDTSTART:20261012T150000Z\r\nSUMMARY:Moved\r\n"), "2026-10-01", "2026-11-01")
	overrides, _ := payload["overrides"].([]any)
	if len(overrides) != 1 {
		t.Fatalf("overrides = %v, want one", payload["overrides"])
	}
	if r, set := overrides[0].(map[string]any)["recurrenceRange"]; set {
		t.Errorf("a THISANDPRIOR override has recurrenceRange %v, which the views read as governing later instances", r)
	}
	if got, want := instanceStarts(t, payload), []string{"2026-10-05T09:00:00Z", "2026-10-19T09:00:00Z", "2026-10-26T09:00:00Z"}; !slices.Equal(got, want) {
		t.Errorf("instances = %v, want %v", got, want)
	}
}

// The browser places a series by the instances the server lists and its
// overrides as themselves, whatever its own zone: an override stands for the
// instance it replaces, which the server leaves out, and an instance a
// RANGE=THISANDFUTURE override governs carries that override's content, in
// that override's value type, while its RECURRENCE-ID keeps the series'. An
// override names its slot as its RECURRENCE-ID does, whatever its DTSTART.
func TestCalendarViewsShowTheInstancesTheServerLists(t *testing.T) {
	node := requireNode(t)
	scenarios := []struct {
		name, raw, start, end, want string
	}{
		{
			name: "a timed series with an override and one of every later instance",
			raw: seriesObject("",
				"DTSTART:20261005T090000Z\r\nDTEND:20261005T100000Z\r\nRRULE:FREQ=WEEKLY;COUNT=4\r\n",
				"RECURRENCE-ID:20261012T090000Z\r\nDTSTART:20261013T150000Z\r\nDTEND:20261013T160000Z\r\nSUMMARY:Moved\r\n",
				"RECURRENCE-ID;RANGE=THISANDFUTURE:20261019T090000Z\r\nDTSTART:20261019T110000Z\r\nDTEND:20261019T130000Z\r\nSUMMARY:Later\r\n"),
			start: "2026-10-01", end: "2026-11-01",
			want: `[` +
				`{"summary":"Series","allDay":false,"start":"2026-10-05T09:00:00.000Z","end":"2026-10-05T10:00:00.000Z","recurrenceId":"2026-10-05T09:00:00.000Z","recurrenceIdAllDay":false,"recurrenceTimezone":"UTC"},` +
				`{"summary":"Moved","allDay":false,"start":"2026-10-13T15:00:00.000Z","end":"2026-10-13T16:00:00.000Z","recurrenceId":"2026-10-12T09:00:00.000Z","recurrenceIdAllDay":false,"recurrenceTimezone":"UTC"},` +
				`{"summary":"Later","allDay":false,"start":"2026-10-19T11:00:00.000Z","end":"2026-10-19T13:00:00.000Z","recurrenceId":"2026-10-19T09:00:00.000Z","recurrenceIdAllDay":false,"recurrenceTimezone":"UTC"},` +
				`{"summary":"Later","allDay":false,"start":"2026-10-26T11:00:00.000Z","end":"2026-10-26T13:00:00.000Z","recurrenceId":"2026-10-26T09:00:00.000Z","recurrenceIdAllDay":false,"recurrenceTimezone":"UTC"}]`,
		},
		{
			name:  "an all-day series",
			raw:   seriesObject("", "DTSTART;VALUE=DATE:20261005\r\nRRULE:FREQ=MONTHLY;BYDAY=1MO;COUNT=2\r\n"),
			start: "2026-10-01", end: "2026-12-01",
			want: `[` +
				`{"summary":"Series","allDay":true,"start":"2026-10-05","end":"2026-10-06","recurrenceId":"2026-10-05","recurrenceIdAllDay":true,"recurrenceTimezone":""},` +
				`{"summary":"Series","allDay":true,"start":"2026-11-02","end":"2026-11-03","recurrenceId":"2026-11-02","recurrenceIdAllDay":true,"recurrenceTimezone":""}]`,
		},
		{
			name: "a timed series with all-day overrides",
			raw: seriesObject("",
				"DTSTART;TZID=America/New_York:20261005T090000\r\nDTEND;TZID=America/New_York:20261005T100000\r\nRRULE:FREQ=WEEKLY;COUNT=3\r\n",
				"RECURRENCE-ID;TZID=America/New_York:20261005T090000\r\nDTSTART;VALUE=DATE:20261006\r\nDTEND;VALUE=DATE:20261007\r\nSUMMARY:Moved\r\n",
				"RECURRENCE-ID;TZID=America/New_York;RANGE=THISANDFUTURE:20261012T090000\r\nDTSTART;VALUE=DATE:20261012\r\nDTEND;VALUE=DATE:20261013\r\nSUMMARY:Later\r\n"),
			start: "2026-10-01", end: "2026-11-01",
			want: `[` +
				`{"summary":"Moved","allDay":true,"start":"2026-10-06","end":"2026-10-07","recurrenceId":"2026-10-05T13:00:00.000Z","recurrenceIdAllDay":false,"recurrenceTimezone":"America/New_York"},` +
				`{"summary":"Later","allDay":true,"start":"2026-10-12","end":"2026-10-13","recurrenceId":"2026-10-12T13:00:00.000Z","recurrenceIdAllDay":false,"recurrenceTimezone":"America/New_York"},` +
				`{"summary":"Later","allDay":true,"start":"2026-10-19","end":"2026-10-20","recurrenceId":"2026-10-19T13:00:00.000Z","recurrenceIdAllDay":false,"recurrenceTimezone":"America/New_York"}]`,
		},
		{
			// 01:00 repeats in Los Angeles on 1 November, so the slot is
			// named on the series' clock, where it reads back as itself.
			name: "a series with an override of every later instance in another zone",
			raw: seriesObject("",
				"DTSTART:20261018T090000Z\r\nDTEND:20261018T100000Z\r\nRRULE:FREQ=WEEKLY;COUNT=3\r\n",
				"RECURRENCE-ID;RANGE=THISANDFUTURE:20261025T090000Z\r\nDTSTART;TZID=America/Los_Angeles:20261025T020000\r\n"+
					"DTEND;TZID=America/Los_Angeles:20261025T030000\r\nSUMMARY:Later\r\n"),
			start: "2026-10-01", end: "2026-11-15",
			want: `[` +
				`{"summary":"Series","allDay":false,"start":"2026-10-18T09:00:00.000Z","end":"2026-10-18T10:00:00.000Z","recurrenceId":"2026-10-18T09:00:00.000Z","recurrenceIdAllDay":false,"recurrenceTimezone":"UTC"},` +
				`{"summary":"Later","allDay":false,"start":"2026-10-25T09:00:00.000Z","end":"2026-10-25T10:00:00.000Z","recurrenceId":"2026-10-25T09:00:00.000Z","recurrenceIdAllDay":false,"recurrenceTimezone":"UTC"},` +
				`{"summary":"Later","allDay":false,"start":"2026-11-01T09:00:00.000Z","end":"2026-11-01T10:00:00.000Z","recurrenceId":"2026-11-01T09:00:00.000Z","recurrenceIdAllDay":false,"recurrenceTimezone":"UTC"}]`,
		},
		{
			name: "an all-day series with a timed override of every later instance",
			raw: seriesObject("",
				"DTSTART;VALUE=DATE:20261005\r\nRRULE:FREQ=WEEKLY;COUNT=3\r\n",
				"RECURRENCE-ID;VALUE=DATE;RANGE=THISANDFUTURE:20261012\r\nDTSTART:20261012T150000Z\r\nDTEND:20261012T160000Z\r\nSUMMARY:Later\r\n"),
			start: "2026-10-01", end: "2026-11-01",
			want: `[` +
				`{"summary":"Series","allDay":true,"start":"2026-10-05","end":"2026-10-06","recurrenceId":"2026-10-05","recurrenceIdAllDay":true,"recurrenceTimezone":""},` +
				`{"summary":"Later","allDay":false,"start":"2026-10-12T15:00:00.000Z","end":"2026-10-12T16:00:00.000Z","recurrenceId":"2026-10-12","recurrenceIdAllDay":true,"recurrenceTimezone":""},` +
				`{"summary":"Later","allDay":false,"start":"2026-10-19T15:00:00.000Z","end":"2026-10-19T16:00:00.000Z","recurrenceId":"2026-10-19","recurrenceIdAllDay":true,"recurrenceTimezone":""}]`,
		},
	}
	dir := t.TempDir()
	var files []string
	var want strings.Builder
	for i, scenario := range scenarios {
		served, _ := servedSeries(t, scenario.raw, scenario.start, scenario.end)
		file := filepath.Join(dir, strconv.Itoa(i)+".json")
		if err := os.WriteFile(file, []byte(served), 0o600); err != nil {
			t.Fatal(err)
		}
		files = append(files, file)
		want.WriteString(scenario.want + "\n")
	}
	for _, name := range []string{"calendar_view.html", "all_calendars_view.html"} {
		rendered := renderedPage(t, name)
		for _, zone := range []string{"UTC", "America/Los_Angeles", "Asia/Tokyo"} {
			t.Run(name+"/"+zone, func(t *testing.T) {
				cmd := exec.Command(node, append([]string{filepath.Join("testdata", "series_instances.mjs"), rendered}, files...)...)
				cmd.Env = append(os.Environ(), "TZ="+zone)
				output, err := cmd.CombinedOutput()
				if err != nil {
					t.Fatalf("%v\n%s", err, output)
				}
				got, wantLines := strings.Split(string(output), "\n"), strings.Split(want.String(), "\n")
				for i, scenario := range scenarios {
					if i >= len(got) || got[i] != wantLines[i] {
						t.Errorf("%s: the view shows\n%s\nwant\n%s", scenario.name, got[min(i, len(got)-1)], wantLines[i])
					}
				}
			})
		}
	}
}
