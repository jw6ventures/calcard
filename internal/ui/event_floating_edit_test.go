package ui

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"testing"

	"github.com/jw6ventures/calcard/internal/store"
)

// floatingSeries is a daily 09:00 series of five days whose times are floating:
// no TZID and no Z. The server reads a floating time as UTC, so its instances
// are 09:00 UTC on 5 to 9 January 2026.
const floatingSeries = "BEGIN:VEVENT\r\nUID:float\r\nDTSTAMP:20260101T000000Z\r\nSUMMARY:Floating\r\n" +
	"DTSTART:20260105T090000\r\nDTEND:20260105T100000\r\nRRULE:FREQ=DAILY;COUNT=5\r\nEND:VEVENT\r\n"

// floatingPeriods are the free-busy periods of hour-long instances starting at
// each of starts, given as "DD HH": a day of January 2026 and an hour, read in
// UTC.
func floatingPeriods(starts ...string) []string {
	var periods []string
	for _, start := range starts {
		day, hour, _ := strings.Cut(start, " ")
		next, _ := strconv.Atoi(hour)
		periods = append(periods, "202601"+day+"T"+hour+"0000Z/202601"+day+"T"+strconv.Itoa(next+1)+"0000Z")
	}
	slices.Sort(periods)
	return periods
}

func storedFloating(t *testing.T, events *fakeEventRepoWithUpsert) string {
	t.Helper()
	stored := events.events["1:float"]
	// The fake store assigns no ID, and the DAV server pages rows by it.
	stored.ID = 1
	return stored.RawICAL
}

// requireFloating fails when a DTSTART, DTEND, RECURRENCE-ID, EXDATE or RDATE
// of raw names a zone or is in UTC.
func requireFloating(t *testing.T, raw string) {
	t.Helper()
	for _, line := range strings.Split(raw, "\r\n") {
		name, value, found := strings.Cut(line, ":")
		if !found {
			continue
		}
		switch strings.ToUpper(strings.SplitN(name, ";", 2)[0]) {
		case "DTSTART", "DTEND", "RECURRENCE-ID", "EXDATE", "RDATE":
		default:
			continue
		}
		if strings.Contains(strings.ToUpper(name), "TZID") || strings.HasSuffix(value, "Z") {
			t.Errorf("%q is not floating:\n%s", line, raw)
		}
	}
}

// A series whose times are floating stays floating when one of its occurrences
// is edited: the override is written in floating times and names its instance
// by the floating RECURRENCE-ID the server's expansion generates, so the
// instance is replaced rather than duplicated.
func TestFloatingSeriesOccurrenceEditReplacesItsInstance(t *testing.T) {
	handler, s, events, user := storedEventHandler(t, "float", calendarObject(floatingSeries))
	occurrence := url.Values{
		"summary": {"Moved"}, "dtstart": {"2026-01-07T11:00"}, "dtend": {"2026-01-07T12:00"},
		"edit_scope": {"occurrence"}, "recurrence_id": {"2026-01-07T09:00"}, "recurrence_all_day": {"false"},
		"recurrence_timezone": {""},
	}
	updateEvent(t, handler, events, user, "float", "", occurrence)

	raw := storedFloating(t, events)
	requireFloating(t, raw)
	if !strings.Contains(raw, "RECURRENCE-ID:20260107T090000\r\n") || !strings.Contains(raw, "DTSTART:20260107T110000\r\n") {
		t.Errorf("the override is not written in floating times:\n%s", raw)
	}
	want := floatingPeriods("05 09", "06 09", "07 11", "08 09", "09 09")
	if got := davFreeBusyPeriods(t, s, user, "20260101T000000Z", "20260201T000000Z"); !slices.Equal(got, want) {
		t.Errorf("CalDAV free-busy = %v, want %v, with the 7th moved to 11:00 and not duplicated\n%s", got, want, raw)
	}

	// Editing the occurrence again edits the override, rather than adding one.
	occurrence.Del("etag")
	occurrence.Set("dtstart", "2026-01-07T13:00")
	occurrence.Set("dtend", "2026-01-07T14:00")
	updateEvent(t, handler, events, user, "float", "", occurrence)
	raw = storedFloating(t, events)
	if got := strings.Count(raw, "RECURRENCE-ID"); got != 1 {
		t.Errorf("stored %d overrides, want the one:\n%s", got, raw)
	}
	want = floatingPeriods("05 09", "06 09", "07 13", "08 09", "09 09")
	if got := davFreeBusyPeriods(t, s, user, "20260101T000000Z", "20260201T000000Z"); !slices.Equal(got, want) {
		t.Errorf("CalDAV free-busy = %v, want %v\n%s", got, want, raw)
	}
}

// Deleting an occurrence of a floating series excludes it by a floating
// EXDATE, which the series' floating instance matches; an override of it goes
// too.
func TestFloatingSeriesOccurrenceDeleteRemovesItsInstance(t *testing.T) {
	handler, s, events, user := storedEventHandler(t, "float", calendarObject(floatingSeries))
	deleted := func(t *testing.T) {
		t.Helper()
		w := httptest.NewRecorder()
		handler.DeleteEvent(w, deleteOccurrenceRequest(t, "float", url.Values{
			"etag": {storedETag(t, events, "float")}, "recurrence_id": {"2026-01-07T09:00"},
			"recurrence_all_day": {"false"}, "recurrence_timezone": {""}, "timezone": {""},
		}))
		if location := w.Header().Get("Location"); w.Code != http.StatusFound || !strings.Contains(location, "status=occurrence_deleted") {
			t.Fatalf("DeleteEvent() status = %d location = %s", w.Code, location)
		}
	}

	deleted(t)
	raw := storedFloating(t, events)
	requireFloating(t, raw)
	if !strings.Contains(raw, "EXDATE:20260107T090000\r\n") {
		t.Errorf("the occurrence is not excluded by a floating EXDATE:\n%s", raw)
	}
	want := floatingPeriods("05 09", "06 09", "08 09", "09 09")
	if got := davFreeBusyPeriods(t, s, user, "20260101T000000Z", "20260201T000000Z"); !slices.Equal(got, want) {
		t.Errorf("CalDAV free-busy = %v, want %v, without the deleted 7th\n%s", got, want, raw)
	}

	t.Run("an edited occurrence", func(t *testing.T) {
		handler, s, events, user := storedEventHandler(t, "float", calendarObject(floatingSeries))
		updateEvent(t, handler, events, user, "float", "", url.Values{
			"summary": {"Moved"}, "dtstart": {"2026-01-07T11:00"}, "dtend": {"2026-01-07T12:00"},
			"edit_scope": {"occurrence"}, "recurrence_id": {"2026-01-07T09:00"}, "recurrence_timezone": {""},
		})
		w := httptest.NewRecorder()
		handler.DeleteEvent(w, deleteOccurrenceRequest(t, "float", url.Values{
			"etag": {storedETag(t, events, "float")}, "recurrence_id": {"2026-01-07T09:00"},
			"recurrence_all_day": {"false"}, "recurrence_timezone": {""}, "timezone": {""},
		}))
		if location := w.Header().Get("Location"); w.Code != http.StatusFound || !strings.Contains(location, "status=occurrence_deleted") {
			t.Fatalf("DeleteEvent() status = %d location = %s", w.Code, location)
		}
		raw := storedFloating(t, events)
		requireFloating(t, raw)
		if strings.Contains(raw, "RECURRENCE-ID") {
			t.Errorf("the deleted occurrence's override was kept:\n%s", raw)
		}
		if got := davFreeBusyPeriods(t, s, user, "20260101T000000Z", "20260201T000000Z"); !slices.Equal(got, want) {
			t.Errorf("CalDAV free-busy = %v, want %v\n%s", got, want, raw)
		}
	})
}

// A whole-series edit of a floating series keeps its times floating: the
// editor shows the series' zone as none, and the series, its exclusions and its
// overrides are written back in floating times, moved with the series.
func TestFloatingSeriesEditStaysFloating(t *testing.T) {
	const series = "BEGIN:VEVENT\r\nUID:float\r\nDTSTAMP:20260101T000000Z\r\nSUMMARY:Floating\r\n" +
		"DTSTART:20260105T090000\r\nDTEND:20260105T100000\r\nRRULE:FREQ=DAILY;COUNT=5\r\nEXDATE:20260106T090000\r\nEND:VEVENT\r\n" +
		"BEGIN:VEVENT\r\nUID:float\r\nDTSTAMP:20260101T000000Z\r\nSUMMARY:Moved\r\nRECURRENCE-ID:20260108T090000\r\n" +
		"DTSTART:20260108T110000\r\nDTEND:20260108T120000\r\nEND:VEVENT\r\n"
	tests := []struct {
		name string
		edit url.Values
		// rule is the RRULE the stored master carries afterwards.
		rule  string
		lines []string
		want  []string
	}{
		{
			name: "renamed",
			edit: url.Values{
				"summary": {"Renamed"}, "dtstart": {"2026-01-05T09:00"}, "dtend": {"2026-01-05T10:00"},
				"recurrence": {"DAILY"}, "recurrence_end_type": {"after"}, "recurrence_count": {"5"},
			},
			rule:  "RRULE:FREQ=DAILY;COUNT=5",
			lines: []string{"DTSTART:20260105T090000", "DTEND:20260105T100000", "EXDATE:20260106T090000", "RECURRENCE-ID:20260108T090000"},
			want:  floatingPeriods("05 09", "07 09", "08 11", "09 09"),
		},
		{
			name: "moved an hour later",
			edit: url.Values{
				"summary": {"Floating"}, "dtstart": {"2026-01-05T10:00"}, "dtend": {"2026-01-05T11:00"},
				"recurrence": {"DAILY"}, "recurrence_end_type": {"after"}, "recurrence_count": {"5"},
			},
			rule:  "RRULE:FREQ=DAILY;COUNT=5",
			lines: []string{"DTSTART:20260105T100000", "DTEND:20260105T110000", "EXDATE:20260106T100000", "RECURRENCE-ID:20260108T100000"},
			want:  floatingPeriods("05 10", "07 10", "08 11", "09 10"),
		},
		{
			name: "ended on a day",
			edit: url.Values{
				"summary": {"Floating"}, "dtstart": {"2026-01-05T09:00"}, "dtend": {"2026-01-05T10:00"},
				"recurrence": {"DAILY"}, "recurrence_end_type": {"on"}, "recurrence_until": {"2026-01-08"},
			},
			rule:  "RRULE:FREQ=DAILY;UNTIL=20260108T235959",
			lines: []string{"DTSTART:20260105T090000", "EXDATE:20260106T090000"},
			want:  floatingPeriods("05 09", "07 09", "08 11"),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			handler, s, events, user := storedEventHandler(t, "float", calendarObject(series))
			updateEvent(t, handler, events, user, "float", "", tt.edit)

			raw := storedFloating(t, events)
			requireFloating(t, raw)
			if !strings.Contains(raw, tt.rule+"\r\n") {
				t.Errorf("stored object lacks %q:\n%s", tt.rule, raw)
			}
			for _, line := range tt.lines {
				if !strings.Contains(raw, line+"\r\n") {
					t.Errorf("stored object lacks %q:\n%s", line, raw)
				}
			}
			if got := davFreeBusyPeriods(t, s, user, "20260101T000000Z", "20260201T000000Z"); !slices.Equal(got, tt.want) {
				t.Errorf("CalDAV free-busy = %v, want %v\n%s", got, tt.want, raw)
			}
		})
	}
}

// A series of dates is edited and has its occurrences edited and deleted in
// dates, the value type of its DTSTART.
func TestFloatingAllDaySeriesKeepsDatesForOccurrences(t *testing.T) {
	const series = "BEGIN:VEVENT\r\nUID:float\r\nDTSTAMP:20260101T000000Z\r\nSUMMARY:Days\r\n" +
		"DTSTART;VALUE=DATE:20260105\r\nDTEND;VALUE=DATE:20260106\r\nRRULE:FREQ=DAILY;COUNT=5\r\nEND:VEVENT\r\n"
	handler, _, events, user := storedEventHandler(t, "float", calendarObject(series))

	updateEvent(t, handler, events, user, "float", "", url.Values{
		"summary": {"Moved"}, "all_day": {"on"}, "dtstart": {"2026-01-08"}, "dtend": {"2026-01-09"},
		"edit_scope": {"occurrence"}, "recurrence_id": {"2026-01-07"}, "recurrence_all_day": {"true"},
	})
	w := httptest.NewRecorder()
	handler.DeleteEvent(w, deleteOccurrenceRequest(t, "float", url.Values{
		"etag": {storedETag(t, events, "float")}, "recurrence_id": {"2026-01-06"}, "recurrence_all_day": {"true"},
	}))
	if location := w.Header().Get("Location"); w.Code != http.StatusFound || !strings.Contains(location, "status=occurrence_deleted") {
		t.Fatalf("DeleteEvent() status = %d location = %s", w.Code, location)
	}
	raw := storedFloating(t, events)
	for _, want := range []string{"RECURRENCE-ID;VALUE=DATE:20260107\r\n", "EXDATE;VALUE=DATE:20260106\r\n"} {
		if !strings.Contains(raw, want) {
			t.Errorf("stored object lacks %q:\n%s", want, raw)
		}
	}
}

// The view model tells the editors which times are floating, so they submit no
// zone for them rather than UTC: the series' own, and each override's start and
// the RECURRENCE-ID naming its instance.
func TestEventsJSONMarksFloatingTimes(t *testing.T) {
	override := func(recurrenceID, start string) string { return recurrenceID + start }
	tests := []struct {
		name       string
		components []string
		floating   bool
		// overrides are the floating and recurrenceFloating of the override.
		overrideFloating, recurrenceFloating bool
	}{
		{"floating series and override", []string{"DTSTART:20260105T090000\r\nRRULE:FREQ=DAILY;COUNT=3\r\n",
			override("RECURRENCE-ID:20260106T090000\r\n", "DTSTART:20260106T100000\r\n")}, true, true, true},
		{"UTC series and override", []string{"DTSTART:20260105T090000Z\r\nRRULE:FREQ=DAILY;COUNT=3\r\n",
			override("RECURRENCE-ID:20260106T090000Z\r\n", "DTSTART:20260106T100000Z\r\n")}, false, false, false},
		{"floating series, UTC override", []string{"DTSTART:20260105T090000\r\nRRULE:FREQ=DAILY;COUNT=3\r\n",
			override("RECURRENCE-ID:20260106T090000\r\n", "DTSTART:20260106T100000Z\r\n")}, true, false, true},
		{"zoned series", []string{"DTSTART;TZID=Europe/Berlin:20260105T090000\r\nRRULE:FREQ=DAILY;COUNT=3\r\n",
			override("RECURRENCE-ID;TZID=Europe/Berlin:20260106T090000\r\n", "DTSTART;TZID=Europe/Berlin:20260106T100000\r\n")}, false, false, false},
		{"all-day series", []string{"DTSTART;VALUE=DATE:20260105\r\nRRULE:FREQ=DAILY;COUNT=3\r\n",
			override("RECURRENCE-ID;VALUE=DATE:20260106\r\n", "DTSTART;VALUE=DATE:20260106\r\n")}, false, false, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, payload := servedSeries(t, seriesObject("", tt.components...), "2026-01-01", "2026-02-01")
			if got, _ := payload["floating"].(bool); got != tt.floating {
				t.Errorf("floating = %v, want %v: %v", payload["floating"], tt.floating, payload)
			}
			overrides, _ := payload["overrides"].([]any)
			if len(overrides) != 1 {
				t.Fatalf("overrides = %v, want one", payload["overrides"])
			}
			o := overrides[0].(map[string]any)
			if got, present := o["floating"].(bool); !present || got != tt.overrideFloating {
				t.Errorf("override floating = %v, want %v", o["floating"], tt.overrideFloating)
			}
			if got, present := o["recurrenceFloating"].(bool); !present || got != tt.recurrenceFloating {
				t.Errorf("override recurrenceFloating = %v, want %v", o["recurrenceFloating"], tt.recurrenceFloating)
			}
		})
	}
}

// The editors leave the time zone of a floating series' fields blank, and the
// zone of the RECURRENCE-ID naming one of its instances, whether they are
// opened on the series, an instance, or an override: the server keeps a series
// floating when the zone is blank. A zoned or UTC series keeps its zone.
func TestCalendarViewsEditAFloatingSeriesWithNoZone(t *testing.T) {
	const march = "BEGIN:VEVENT\r\nUID:%s\r\nDTSTAMP:20240101T000000Z\r\nSUMMARY:Series\r\n%sRRULE:FREQ=DAILY;COUNT=5\r\nEND:VEVENT\r\n" +
		"BEGIN:VEVENT\r\nUID:%s\r\nDTSTAMP:20240101T000000Z\r\nSUMMARY:Override\r\n%s%sEND:VEVENT\r\n"
	series := func(uid, start, id, moved string) string {
		return fmt.Sprintf("BEGIN:VCALENDAR\r\nVERSION:2.0\r\nPRODID:-//Client//EN\r\n"+march+"END:VCALENDAR\r\n", uid, start, uid, id, moved)
	}
	raws := map[string]string{
		"floating": series("floating", "DTSTART:20240304T090000\r\nDTEND:20240304T100000\r\n",
			"RECURRENCE-ID:20240306T090000\r\n", "DTSTART:20240306T110000\r\nDTEND:20240306T120000\r\n"),
		"utc": series("utc", "DTSTART:20240304T090000Z\r\nDTEND:20240304T100000Z\r\n",
			"RECURRENCE-ID:20240306T090000Z\r\n", "DTSTART:20240306T110000Z\r\nDTEND:20240306T120000Z\r\n"),
		"zoned": series("zoned", "DTSTART;TZID=Europe/Berlin:20240304T090000\r\nDTEND;TZID=Europe/Berlin:20240304T100000\r\n",
			"RECURRENCE-ID;TZID=Europe/Berlin:20240306T090000\r\n", "DTSTART;TZID=Europe/Berlin:20240306T110000\r\nDTEND;TZID=Europe/Berlin:20240306T120000\r\n"),
	}
	node := requireNode(t)
	user := &store.User{ID: 100}
	const placeholderFloating = "Floating (no time zone)"
	tests := []struct {
		uid, occurrence  string
		values           []string
		zone, recurrence string
		floating         bool
	}{
		{"floating", "", []string{"2024-03-04T09:00", "2024-03-04T10:00"}, "", "", true},
		{"floating", "2024-03-05T09:00:00.000Z", []string{"2024-03-05T09:00", "2024-03-05T10:00", "2024-03-05T09:00"}, "", "", true},
		{"floating", "2024-03-06T11:00:00.000Z", []string{"2024-03-06T11:00", "2024-03-06T12:00", "2024-03-06T09:00"}, "", "", true},
		{"utc", "2024-03-05T09:00:00.000Z", []string{"2024-03-05T09:00", "2024-03-05T10:00", "2024-03-05T09:00"}, "UTC", "UTC", false},
		{"utc", "2024-03-06T11:00:00.000Z", []string{"2024-03-06T11:00", "2024-03-06T12:00", "2024-03-06T09:00"}, "UTC", "UTC", false},
		{"zoned", "2024-03-05T08:00:00.000Z", []string{"2024-03-05T09:00", "2024-03-05T10:00", "2024-03-05T09:00"}, "Europe/Berlin", "Europe/Berlin", false},
	}
	for _, template := range []string{"calendar_view.html", "all_calendars_view.html"} {
		rendered := renderedPage(t, template)
		for _, browserZone := range []string{"UTC", "America/New_York", "Asia/Tokyo"} {
			served := servedEventsJSON(t, servedStoredHandler(raws), user, template)
			for _, tt := range tests {
				t.Run(template+"/"+browserZone+"/"+tt.uid+"/"+tt.occurrence, func(t *testing.T) {
					args := []string{filepath.Join("testdata", "served_event_editor.mjs"), rendered, served, tt.uid}
					if tt.occurrence != "" {
						args = append(args, tt.occurrence)
					}
					cmd := exec.Command(node, args...)
					cmd.Env = append(os.Environ(), "TZ="+browserZone)
					output, err := cmd.CombinedOutput()
					if err != nil {
						t.Fatalf("view helpers: %v: %s", err, output)
					}
					var result struct {
						Values                                                                                                            []string
						Zone, RecurrenceZone, Placeholder, DeleteZone, DeleteRecurrenceZone, RecurrenceFloating, DeleteRecurrenceFloating string
					}
					if err := json.Unmarshal(output, &result); err != nil {
						t.Fatalf("%v: %s", err, output)
					}
					if len(result.Values) < 2 || !slices.Equal(result.Values[:2], tt.values[:2]) {
						t.Errorf("the editor shows %v, want %v", result.Values, tt.values[:2])
					}
					if len(tt.values) > 2 && result.Values[2] != tt.values[2] {
						t.Errorf("the editor names the occurrence %q, want %q", result.Values[2], tt.values[2])
					}
					if result.Zone != tt.zone || result.DeleteZone != tt.zone {
						t.Errorf("zone fields = %q (delete %q), want %q", result.Zone, result.DeleteZone, tt.zone)
					}
					if tt.occurrence != "" && (result.RecurrenceZone != tt.recurrence || result.DeleteRecurrenceZone != tt.recurrence) {
						t.Errorf("occurrence zone fields = %q (delete %q), want %q", result.RecurrenceZone, result.DeleteRecurrenceZone, tt.recurrence)
					}
					if tt.occurrence != "" {
						marker := map[bool]string{true: "1", false: ""}[tt.floating]
						if result.RecurrenceFloating != marker || result.DeleteRecurrenceFloating != marker {
							t.Errorf("floating markers = %q (delete %q), want %q", result.RecurrenceFloating, result.DeleteRecurrenceFloating, marker)
						}
					}
					if shown := result.Placeholder == placeholderFloating; shown != tt.floating {
						t.Errorf("zone placeholder = %q, want the floating hint only for a floating series", result.Placeholder)
					}
				})
			}
		}
	}
}

// Both views place and label a floating series, its instances, its exclusions
// and its overrides at their wall clocks, as every CalDAV client does and the
// editor does, whatever zone the browser is in: the events JSON names them as
// instants, which are not theirs.
func TestCalendarViewsShowFloatingEventsAtTheirWallClock(t *testing.T) {
	const object = "BEGIN:VCALENDAR\r\nVERSION:2.0\r\nPRODID:-//Client//EN\r\n" +
		"BEGIN:VEVENT\r\nUID:floating\r\nDTSTAMP:20240101T000000Z\r\nSUMMARY:Floating\r\n" +
		"DTSTART:20240304T090000\r\nDTEND:20240304T100000\r\nRRULE:FREQ=DAILY;COUNT=8\r\nEXDATE:20240305T090000\r\nEND:VEVENT\r\n" +
		"BEGIN:VEVENT\r\nUID:floating\r\nDTSTAMP:20240101T000000Z\r\nSUMMARY:Moved\r\nRECURRENCE-ID:20240306T090000\r\n" +
		"DTSTART:20240306T110000\r\nDTEND:20240306T120000\r\nEND:VEVENT\r\nEND:VCALENDAR\r\n"
	const utc = "BEGIN:VCALENDAR\r\nVERSION:2.0\r\nPRODID:-//Client//EN\r\n" +
		"BEGIN:VEVENT\r\nUID:utc\r\nDTSTAMP:20240101T000000Z\r\nSUMMARY:UTC\r\nDTSTART:20240304T090000Z\r\nDTEND:20240304T100000Z\r\nEND:VEVENT\r\nEND:VCALENDAR\r\n"
	node := requireNode(t)
	user := &store.User{ID: 100}
	for _, template := range []string{"calendar_view.html", "all_calendars_view.html"} {
		rendered := renderedPage(t, template)
		served := servedEventsJSON(t, servedStoredHandler(map[string]string{"floating": object, "utc": utc}), user, template)
		for _, browserZone := range []string{"UTC", "America/Chicago", "America/New_York", "Europe/Berlin", "Asia/Tokyo", "Pacific/Kiritimati"} {
			t.Run(template+"/"+browserZone, func(t *testing.T) {
				cmd := exec.Command(node, filepath.Join("testdata", "floating_display.mjs"), rendered, served)
				cmd.Env = append(os.Environ(), "TZ="+browserZone)
				if output, err := cmd.CombinedOutput(); err != nil {
					t.Fatalf("view helpers: %v: %s", err, output)
				}
			})
		}
	}
}

// A floating time whose wall clock the browser's zone skips (02:30 on the day
// its clocks go forward) is shown on the grid at the nearest time the browser
// has, but the editor shows and submits the time it is stored at, so saving a
// series in the gap changes none of its times, and an occurrence is named by
// its own RECURRENCE-ID.
func TestCalendarViewsKeepAFloatingTimeInTheBrowsersDSTGap(t *testing.T) {
	const object = "BEGIN:VCALENDAR\r\nVERSION:2.0\r\nPRODID:-//Client//EN\r\n" +
		"BEGIN:VEVENT\r\nUID:gap\r\nDTSTAMP:20240101T000000Z\r\nSUMMARY:Gap\r\n" +
		"DTSTART:20240309T023000\r\nDTEND:20240309T033000\r\nRRULE:FREQ=DAILY;COUNT=25\r\nEND:VEVENT\r\nEND:VCALENDAR\r\n"
	starting := func(uid, day string) string {
		return "BEGIN:VCALENDAR\r\nVERSION:2.0\r\nPRODID:-//Client//EN\r\n" +
			"BEGIN:VEVENT\r\nUID:" + uid + "\r\nDTSTAMP:20240101T000000Z\r\nSUMMARY:Gap\r\n" +
			"DTSTART:" + day + "T023000\r\nDTEND:" + day + "T033000\r\nRRULE:FREQ=DAILY;COUNT=3\r\nEND:VEVENT\r\nEND:VCALENDAR\r\n"
	}
	node := requireNode(t)
	user := &store.User{ID: 100}
	tests := []struct {
		browserZone, uid, occurrence string
		values                       []string
	}{
		{"America/Chicago", "gap10", "", []string{"2024-03-10T02:30", "2024-03-10T03:30"}},
		{"Europe/Berlin", "gap31", "", []string{"2024-03-31T02:30", "2024-03-31T03:30"}},
		{"America/Chicago", "gap", "", []string{"2024-03-09T02:30", "2024-03-09T03:30"}},
		{"America/Chicago", "gap", "2024-03-10T02:30:00.000Z", []string{"2024-03-10T02:30", "2024-03-10T03:30", "2024-03-10T02:30"}},
		{"Europe/Berlin", "gap", "2024-03-31T02:30:00.000Z", []string{"2024-03-31T02:30", "2024-03-31T03:30", "2024-03-31T02:30"}},
		{"Europe/Berlin", "gap", "2024-03-30T02:30:00.000Z", []string{"2024-03-30T02:30", "2024-03-30T03:30", "2024-03-30T02:30"}},
	}
	for _, template := range []string{"calendar_view.html", "all_calendars_view.html"} {
		rendered := renderedPage(t, template)
		served := servedEventsJSON(t, servedStoredHandler(map[string]string{"gap": object, "gap10": starting("gap10", "20240310"), "gap31": starting("gap31", "20240331")}), user, template)
		for _, tt := range tests {
			t.Run(template+"/"+tt.browserZone+"/"+tt.uid+"/"+tt.occurrence, func(t *testing.T) {
				args := []string{filepath.Join("testdata", "served_event_editor.mjs"), rendered, served, tt.uid}
				if tt.occurrence != "" {
					args = append(args, tt.occurrence)
				}
				cmd := exec.Command(node, args...)
				cmd.Env = append(os.Environ(), "TZ="+tt.browserZone)
				output, err := cmd.CombinedOutput()
				if err != nil {
					t.Fatalf("view helpers: %v: %s", err, output)
				}
				var result struct {
					Values                   []string
					Zone, RecurrenceFloating string
				}
				if err := json.Unmarshal(output, &result); err != nil {
					t.Fatalf("%v: %s", err, output)
				}
				if len(result.Values) < 2 || !slices.Equal(result.Values[:2], tt.values[:2]) {
					t.Errorf("the editor shows %v, want %v", result.Values, tt.values[:2])
				}
				if len(tt.values) > 2 && result.Values[2] != tt.values[2] {
					t.Errorf("the editor names the occurrence %q, want %q", result.Values[2], tt.values[2])
				}
				if result.Zone != "" {
					t.Errorf("the zone field = %q, want none for a floating series", result.Zone)
				}
			})
		}
	}
}
