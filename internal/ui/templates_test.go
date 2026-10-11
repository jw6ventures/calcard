package ui

import (
	"context"
	"encoding/json"
	"io/fs"
	"maps"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/jw6ventures/calcard/internal/auth"
	"github.com/jw6ventures/calcard/internal/config"
	"github.com/jw6ventures/calcard/internal/dav"
	"github.com/jw6ventures/calcard/internal/ical"
	"github.com/jw6ventures/calcard/internal/store"
	"github.com/jw6ventures/calcard/internal/ui/utils"
)

func TestTemplatesEmbedded(t *testing.T) {
	names := []string{
		"base.html",
		"dashboard.html",
	}
	for _, name := range names {
		if _, err := templateFS.Open("templates/" + name); err != nil {
			t.Fatalf("expected embedded template %s, got error: %v", name, err)
		}
	}
}

func TestFormatMonthAbbrevAndDayOfMonth(t *testing.T) {
	when := time.Date(2026, time.September, 7, 13, 30, 0, 0, time.UTC)
	tests := []struct {
		name      string
		value     any
		wantMonth string
		wantDay   string
	}{
		{name: "value", value: when, wantMonth: "SEP", wantDay: "7"},
		{name: "pointer", value: &when, wantMonth: "SEP", wantDay: "7"},
		{name: "nil", value: nil},
		{name: "nil pointer", value: (*time.Time)(nil)},
		{name: "zero time", value: time.Time{}},
		{name: "wrong type", value: "2026-09-07"},
	}

	month := funcMap["formatMonthAbbrev"].(func(any) string)
	day := funcMap["formatDayOfMonth"].(func(any) string)
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := month(tt.value); got != tt.wantMonth {
				t.Errorf("formatMonthAbbrev(%v) = %q, want %q", tt.value, got, tt.wantMonth)
			}
			if got := day(tt.value); got != tt.wantDay {
				t.Errorf("formatDayOfMonth(%v) = %q, want %q", tt.value, got, tt.wantDay)
			}
		})
	}
}

// The recent-events badge shows the event's own date: a fixed 📅 glyph renders
// as a July 17 page on most platforms regardless of when the event is.
func TestDashboardEventBadgeReadsTheEventDate(t *testing.T) {
	source, err := templateFS.ReadFile("templates/dashboard.html")
	if err != nil {
		t.Fatalf("read dashboard.html: %v", err)
	}
	for _, want := range []string{
		"formatMonthAbbrev .DTStart",
		"formatDayOfMonth .DTStart",
	} {
		if !strings.Contains(string(source), want) {
			t.Errorf("dashboard.html is missing %q: the event badge is not built from the event date", want)
		}
	}
	// U+1F4C5 renders as a July 17 calendar page on most platforms, so it can
	// never stand next to a real date badge.
	if strings.Contains(string(source), "&#128197;") {
		t.Error("dashboard.html still renders the fixed calendar glyph for a recent event")
	}
}

// formatTime renders RFC3339, which is what put "2026-02-05T03:16:24Z" in the
// CREATED columns while the rest of the UI showed "Feb 5, 2026".
func TestTemplatesDoNotRenderRawTimestamps(t *testing.T) {
	files, err := fs.Glob(templateFS, "templates/*.html")
	if err != nil {
		t.Fatal(err)
	}
	for _, file := range files {
		source, err := templateFS.ReadFile(file)
		if err != nil {
			t.Fatal(err)
		}
		if strings.Contains(string(source), "formatTime ") {
			t.Errorf("%s renders a timestamp with formatTime; use formatDate or formatDateTime", file)
		}
	}
}

// A CN that only repeats the address renders as "x@y.z <x@y.z>" without this.
func TestCalendarViewsCollapseRedundantAttendeeName(t *testing.T) {
	source, err := templateFS.ReadFile(calendarEventsPartialFile)
	if err != nil {
		t.Fatalf("read the shared event helpers: %v", err)
	}
	if !strings.Contains(string(source), "name.toLowerCase() === email.toLowerCase()") {
		t.Error("the calendar views do not collapse a CN that repeats the address")
	}
}

// A backwards range is the one rejection the browser can catch before the
// redirect drops the form, so both editors have to check it themselves.
func TestCalendarViewsRejectABackwardsRangeBeforeSubmitting(t *testing.T) {
	for _, name := range []string{"calendar_view.html", "all_calendars_view.html"} {
		source, err := templateFS.ReadFile("templates/" + name)
		if err != nil {
			t.Fatalf("read %s: %v", name, err)
		}
		if !strings.Contains(string(source), "function createEventDateError(startValue, endValue, allDay)") {
			t.Errorf("%s does not check the entered range before submitting", name)
		}
	}
}

// The create modal is wiped by the redirect a server-side rejection performs,
// so it has to both catch the bad range itself and put the draft back when the
// server rejects for some other reason.
func TestCalendarViewPreservesCreateEventDraft(t *testing.T) {
	source, err := templateFS.ReadFile("templates/calendar_view.html")
	if err != nil {
		t.Fatalf("read calendar_view.html: %v", err)
	}
	for _, want := range []string{
		"function createEventDateError(startValue, endValue, allDay)",
		"function saveCreateEventDraft(",
		"function restoreCreateEventDraft(",
		"id=\"create-event-form\"",
	} {
		if !strings.Contains(string(source), want) {
			t.Errorf("calendar_view.html is missing %q: a rejected create loses the form", want)
		}
	}
	// The view bootstrap replaces the URL with one carrying only the view and
	// date before the create wiring runs, so reading the rejection from the
	// query string would always find nothing.
	if !strings.Contains(string(source), "var CREATE_EVENT_REJECTION = {{if .FlashError}}") {
		t.Error("calendar_view.html does not take the rejection from the rendered flash")
	}
	if strings.Contains(string(source), "URLSearchParams(window.location.search).get('error')") {
		t.Error("calendar_view.html reads the rejection from a URL the bootstrap has already rewritten")
	}
}

func TestEventFormHelpers(t *testing.T) {
	node := requireNode(t)
	for _, name := range []string{"calendar_view.html", "all_calendars_view.html"} {
		t.Run(name, func(t *testing.T) {
			cmd := exec.Command(node, filepath.Join("testdata", "event_form_helpers.mjs"), renderedPage(t, name))
			if output, err := cmd.CombinedOutput(); err != nil {
				t.Fatalf("template helpers: %v\n%s", err, output)
			}
		})
	}
}

// The aggregate editor's all-day end conversion lives in template JavaScript,
// which no Go test can call. Two things stand in for that: this assertion that
// the conversion is still applied where the end input is filled, and the node
// script below, which runs the template's own helpers.
func TestAggregateEditorConvertsAllDayEndForDisplay(t *testing.T) {
	source, err := templateFS.ReadFile("templates/all_calendars_view.html")
	if err != nil {
		t.Fatalf("read all_calendars_view.html: %v", err)
	}
	for _, want := range []string{
		"function inclusiveAllDayEnd(start, exclusiveEnd)",
		"function exclusiveAllDayEnd(start, lastDay)",
		"allDay ? formatDateInput(inclusiveAllDayEnd(startOfDay(start), end)) : formatDateTimeForInput(end, clock)",
	} {
		if !strings.Contains(string(source), want) {
			t.Errorf("all_calendars_view.html is missing %q: an all-day end reaches the form unconverted", want)
		}
	}
}

// Both calendar views read the same server payload, so an all-day date has to be
// moved onto the local day in both. Leaving either on the instant the server
// sends puts the event a day early everywhere west of UTC.
func TestCalendarViewsNormalizeAllDayServerDates(t *testing.T) {
	partial, err := templateFS.ReadFile(calendarEventsPartialFile)
	if err != nil {
		t.Fatalf("read the shared event helpers: %v", err)
	}
	for _, want := range []string{
		"function parseServerDate(value, allDay)",
		"parsed.dtstart = parseServerDate(raw.dtstart, allDay);",
		"parsed.dtend = parseServerDate(raw.dtend, allDay);",
		"raw.recurrenceIdAllDay !== undefined ? raw.recurrenceIdAllDay : allDay);",
	} {
		if !strings.Contains(string(partial), want) {
			t.Errorf("the shared event helpers are missing %q: an all-day date keeps the instant the server sent", want)
		}
	}
	for _, file := range []string{calendarEventsPartialFile, "templates/all_calendars_view.html", "templates/calendar_view.html"} {
		source, err := templateFS.ReadFile(file)
		if err != nil {
			t.Fatalf("read %s: %v", file, err)
		}
		// map would hand parseServerDate the array index as its all-day flag,
		// which is false for the first EXDATE and true for every one after it.
		if strings.Contains(string(source), ".map(parseServerDate)") {
			t.Errorf("%s passes parseServerDate to map, so the index becomes the all-day flag", file)
		}
	}
}

// Runs the template's own date helpers over the save round-trip, which must not
// extend an all-day event by a day each time, and over the server payload,
// which must not move it back a day. Skipped where node is absent outside CI,
// which the assertions above partly cover for.
//
// The zones are the point of the table: an all-day date the server sends as UTC
// midnight only lands on the wrong day where the browser's offset is negative,
// so a run in one zone proves nothing about the others.
func TestAggregateEditorAllDayEndRoundTrip(t *testing.T) {
	node := requireNode(t)
	zones := []string{
		"UTC",
		"America/New_York",   // -04:00/-05:00, where the drift appeared
		"America/Anchorage",  // -08:00/-09:00
		"Pacific/Kiritimati", // +14:00, the far side
		"Asia/Kolkata",       // +05:30, not a whole hour
	}
	rendered := renderedPage(t, "all_calendars_view.html")
	for _, zone := range zones {
		t.Run(zone, func(t *testing.T) {
			cmd := exec.Command(node, filepath.Join("testdata", "all_day_end_roundtrip.mjs"), rendered)
			cmd.Env = append(os.Environ(), "TZ="+zone)
			output, err := cmd.CombinedOutput()
			if err != nil {
				t.Fatalf("all-day round-trip failed in %s: %v\n%s", zone, err, output)
			}
		})
	}
}

// calendar_view.html converts the same exclusive DTEND by hand in two places
// (the edit modal and the create-modal prefill) instead of through a shared
// helper. Both must clamp against start the same way the aggregate editor's
// inclusiveAllDayEnd does, or a stored DTEND equal to DTSTART shows an end
// date before the start.
func TestCalendarViewConvertsAllDayEndForDisplay(t *testing.T) {
	source, err := templateFS.ReadFile("templates/calendar_view.html")
	if err != nil {
		t.Fatalf("read calendar_view.html: %v", err)
	}
	for _, want := range []string{
		"function inclusiveAllDayEnd(start, exclusiveEnd)",
		"function exclusiveAllDayEnd(start, lastDay)",
		"formatDateForInput(window.inclusiveAllDayEnd(allDayStart, event.dtend))",
		"formatDateForInput(window.inclusiveAllDayEnd(window.startOfDay(prefill.start), prefill.end))",
	} {
		if !strings.Contains(string(source), want) {
			t.Errorf("calendar_view.html is missing %q: an all-day end reaches the form unconverted or unclamped", want)
		}
	}
}

// Runs calendar_view.html's own showEditEventModal, showCreateEventModal and
// toggleAllDay over the same all-day end scenarios as
// TestAggregateEditorAllDayEndRoundTrip, including a stored DTEND equal to
// DTSTART -- degenerate, but storable, and the shape that showed an end
// before the start. calendar_view.html has no equivalent to the aggregate
// editor's single setEditorDateValues, so this drives a sibling script
// (calendar_view_all_day_end_roundtrip.mjs) rather than the shared one; see
// that file for why.
func TestCalendarViewAllDayEndRoundTrip(t *testing.T) {
	node := requireNode(t)
	zones := []string{
		"UTC",
		"America/New_York",   // -04:00/-05:00, where the drift appeared
		"America/Anchorage",  // -08:00/-09:00
		"Pacific/Kiritimati", // +14:00, the far side
		"Asia/Kolkata",       // +05:30, not a whole hour
	}
	rendered := renderedPage(t, "calendar_view.html")
	for _, zone := range zones {
		t.Run(zone, func(t *testing.T) {
			cmd := exec.Command(node, filepath.Join("testdata", "calendar_view_all_day_end_roundtrip.mjs"), rendered)
			cmd.Env = append(os.Environ(), "TZ="+zone)
			output, err := cmd.CombinedOutput()
			if err != nil {
				t.Fatalf("all-day round-trip failed in %s: %v\n%s", zone, err, output)
			}
		})
	}
}

func TestTimeGridDST(t *testing.T) {
	node := requireNode(t)
	for _, name := range []string{"calendar_view.html", "all_calendars_view.html"} {
		t.Run(name, func(t *testing.T) {
			cmd := exec.Command(node, "testdata/time_grid_dst.mjs", filepath.Join("templates", name))
			cmd.Env = append(os.Environ(), "TZ=America/New_York")
			if output, err := cmd.CombinedOutput(); err != nil {
				t.Fatalf("%v\n%s", err, output)
			}
		})
	}
}

// The edit forms show a timed event's times, and the day its series ends, in
// the event's zone, and the server reads what they submit in that zone: a zone
// neither the browser nor the server knows is UTC to both. Each submitted
// value has to name the instant, and rebuild the UNTIL, the event already had.
// The create form prefills a slot on the browser's grid, so it has to submit
// the browser's zone for the slot to be saved where it was picked.
func TestTimedEditorTimezoneRoundTrip(t *testing.T) {
	node := requireNode(t)
	for _, template := range []string{"all_calendars_view.html", "calendar_view.html"} {
		rendered := renderedPage(t, template)
		for _, browserZone := range []string{"America/Los_Angeles", "UTC", "Europe/Berlin", "Pacific/Kiritimati"} {
			t.Run(template+"/"+browserZone, func(t *testing.T) {
				cmd := exec.Command(node, filepath.Join("testdata", "timed_editor_roundtrip.mjs"), rendered)
				cmd.Env = append(os.Environ(), "TZ="+browserZone)
				output, err := cmd.CombinedOutput()
				if err != nil {
					t.Fatalf("template helpers: %v: %s", err, output)
				}
				var result struct {
					Cases []struct {
						Start, End, Zone, RecurrenceZone string
						Values                           []string
					}
					Create struct {
						Start, End, Zone string
						Values           []string
					}
				}
				if err := json.Unmarshal(output, &result); err != nil {
					t.Fatal(err)
				}
				for _, c := range result.Cases {
					for i, prop := range []string{"DTSTART", "DTEND", "RECURRENCE-ID"} {
						zone := c.Zone
						if i == 2 {
							zone = c.RecurrenceZone
						}
						original := c.Start
						if i == 1 {
							original = c.End
						}
						assertSubmittedInstant(t, prop, c.Values[i], zone, original)
					}
				}
				if result.Create.Zone != browserZone {
					t.Errorf("the create form submits time zone %q beside times it prefilled in %s", result.Create.Zone, browserZone)
				}
				assertSubmittedInstant(t, "DTSTART", result.Create.Values[0], result.Create.Zone, result.Create.Start)
				assertSubmittedInstant(t, "DTEND", result.Create.Values[1], result.Create.Zone, result.Create.End)
			})
		}
	}
}

// assertSubmittedInstant reports whether value, submitted from a form with
// time zone zone, is written as prop naming the instant want (RFC 3339).
func assertSubmittedInstant(t *testing.T, prop, value, zone, want string) {
	t.Helper()
	line, err := utils.FormatICalDateTime(value, false, false, prop, zone)
	if err != nil {
		t.Fatal(err)
	}
	key, raw, _ := strings.Cut(line, ":")
	got, ok := ical.ParsePropertyDateTimeLocal(key, raw)
	wantInstant, err := time.Parse(time.RFC3339, want)
	if err != nil {
		t.Fatal(err)
	}
	if !ok || !got.Equal(wantInstant) {
		t.Errorf("%s: %s submitted as %q in %q is written %s, want instant %s", prop, want, value, zone, line, wantInstant)
	}
}

// servedOutlookCalendar holds two series in Outlook's "Eastern Standard Time",
// a zone only the object defines, both crossing the switch to daylight time
// on 10 March 2024: a Monday 09:00 meeting ending on 25 March, with UNTIL
// written as the server writes it, whose 11 March instance moved to 11:00; and
// a daily 02:30 one whose 10 March instance falls in the skipped hour.
var servedOutlookCalendar = map[string]string{
	"outlook-standup": "BEGIN:VCALENDAR\r\nPRODID:Microsoft Exchange Server 2010\r\nVERSION:2.0\r\n" + outlookEastern +
		"BEGIN:VEVENT\r\nUID:outlook-standup\r\nDTSTAMP:20240101T000000Z\r\nSUMMARY:Standup\r\n" +
		"DTSTART;TZID=\"Eastern Standard Time\":20240304T090000\r\nDTEND;TZID=\"Eastern Standard Time\":20240304T100000\r\n" +
		"RRULE:FREQ=WEEKLY;UNTIL=20240326T035959Z;BYDAY=MO\r\nEND:VEVENT\r\n" +
		"BEGIN:VEVENT\r\nUID:outlook-standup\r\nDTSTAMP:20240101T000000Z\r\nSUMMARY:Moved standup\r\n" +
		"RECURRENCE-ID;TZID=\"Eastern Standard Time\":20240311T090000\r\n" +
		"DTSTART;TZID=\"Eastern Standard Time\":20240311T110000\r\nDTEND;TZID=\"Eastern Standard Time\":20240311T120000\r\nEND:VEVENT\r\n" +
		"END:VCALENDAR\r\n",
	// A series running since decades before the month viewed, whose zone's
	// offsets the view still reads where the series starts and in the month.
	"outlook-decades": "BEGIN:VCALENDAR\r\nPRODID:Microsoft Exchange Server 2010\r\nVERSION:2.0\r\n" + outlookEastern +
		"BEGIN:VEVENT\r\nUID:outlook-decades\r\nDTSTAMP:20240101T000000Z\r\nSUMMARY:Monthly review\r\n" +
		"DTSTART;TZID=\"Eastern Standard Time\":19900305T150000\r\nDTEND;TZID=\"Eastern Standard Time\":19900305T160000\r\n" +
		"RRULE:FREQ=MONTHLY\r\nEND:VEVENT\r\n" +
		"END:VCALENDAR\r\n",
	"outlook-early": "BEGIN:VCALENDAR\r\nPRODID:Microsoft Exchange Server 2010\r\nVERSION:2.0\r\n" + outlookEastern +
		"BEGIN:VEVENT\r\nUID:outlook-early\r\nDTSTAMP:20240101T000000Z\r\nSUMMARY:Early\r\n" +
		"DTSTART;TZID=\"Eastern Standard Time\":20240309T023000\r\nDTEND;TZID=\"Eastern Standard Time\":20240309T033000\r\n" +
		"RRULE:FREQ=DAILY;COUNT=4\r\nEND:VEVENT\r\n" +
		"END:VCALENDAR\r\n",
}

// An event in a zone only the object defines reaches the browser with the
// offsets the object's definition keeps, which the views have to read its
// times through wherever the browser is: each view's expansion of the events
// its own endpoint serves has to publish CalDAV's free-busy periods, its
// editor has to show the zone's wall clocks, and saving what it shows has to
// leave the stored series, and an occurrence's identity, as they were.
func TestCalendarViewsEditAnEventInAZoneOnlyTheObjectDefines(t *testing.T) {
	const zone = "Eastern Standard Time"
	if _, ok := ical.HostLocation(zone); ok {
		t.Skipf("the host defines %q", zone)
	}
	node := requireNode(t)
	user := &store.User{ID: 100}
	for _, template := range []string{"calendar_view.html", "all_calendars_view.html"} {
		rendered := renderedPage(t, template)
		for _, browserZone := range []string{"UTC", "America/New_York", "America/Los_Angeles", "Europe/Berlin", "Asia/Tokyo"} {
			t.Run(template+"/"+browserZone, func(t *testing.T) {
				handler, s, events := servedOutlookHandler(t)
				served := servedEventsJSON(t, handler, user, template)

				cmd := exec.Command(node, filepath.Join("testdata", "timed_editor_roundtrip.mjs"), rendered, served)
				cmd.Env = append(os.Environ(), "TZ="+browserZone)
				output, err := cmd.CombinedOutput()
				if err != nil {
					t.Fatalf("view helpers: %v: %s", err, output)
				}
				type form struct {
					Values               []string
					Zone, RecurrenceZone string
					Until                string
				}
				var result struct {
					Instances            []string
					Summary, SummaryWant string
					Forms                struct{ Series, Override, Occurrence, Decades, DecadesOccurrence form }
				}
				if err := json.Unmarshal(output, &result); err != nil {
					t.Fatalf("%v: %s", err, output)
				}

				freeBusy := func() []string {
					// The fake store assigns no ID to an event it saves, and
					// the DAV server pages rows by it.
					for i, uid := range slices.Sorted(maps.Keys(servedOutlookCalendar)) {
						events.events["1:"+uid].ID = int64(i + 1)
					}
					return davFreeBusyPeriods(t, s, user, "20240301T000000Z", "20240401T000000Z")
				}
				published := freeBusy()
				if !slices.Equal(result.Instances, published) {
					t.Errorf("the view places %v, CalDAV publishes %v", result.Instances, published)
				}
				if result.Summary != result.SummaryWant {
					t.Errorf("recurrence summary = %q, want %q", result.Summary, result.SummaryWant)
				}
				for _, check := range []struct {
					name string
					got  form
					want form
				}{
					{"series", result.Forms.Series, form{Values: []string{"2024-03-04T09:00", "2024-03-04T10:00", ""}, Zone: zone, Until: "2024-03-25"}},
					{"moved occurrence", result.Forms.Override, form{Values: []string{"2024-03-11T11:00", "2024-03-11T12:00", "2024-03-11T09:00"}, Zone: zone, RecurrenceZone: zone, Until: "2024-03-25"}},
					{"occurrence", result.Forms.Occurrence, form{Values: []string{"2024-03-18T09:00", "2024-03-18T10:00", "2024-03-18T09:00"}, Zone: zone, RecurrenceZone: zone, Until: "2024-03-25"}},
					// A series without UNTIL leaves the end field as an earlier
					// edit of the aggregate view's one form left it.
					{"series since 1990", result.Forms.Decades, form{Values: []string{"1990-03-05T15:00", "1990-03-05T16:00", ""}, Zone: zone}},
					{"occurrence of the series since 1990", result.Forms.DecadesOccurrence, form{Values: []string{"2024-03-05T15:00", "2024-03-05T16:00", "2024-03-05T15:00"}, Zone: zone, RecurrenceZone: zone}},
				} {
					want := check.want
					if check.name == "series" || check.name == "series since 1990" {
						want.RecurrenceZone = check.got.RecurrenceZone
					}
					if !slices.Equal(check.got.Values, want.Values) || check.got.Zone != want.Zone ||
						check.got.RecurrenceZone != want.RecurrenceZone || want.Until != "" && check.got.Until != want.Until {
						t.Errorf("%s editor shows %+v, want %+v", check.name, check.got, want)
					}
				}

				stored := func() *ical.Component {
					return ical.PrimaryVEventComponent(events.events["1:outlook-standup"].RawICAL)
				}
				original := stored()
				series := result.Forms.Series
				updateServedEvent(t, handler, user, series.Zone, url.Values{
					"dtstart": {series.Values[0]}, "dtend": {series.Values[1]},
					"recurrence": {"WEEKLY"}, "recurrence_byday": {"MO"},
					"recurrence_end_type": {"on"}, "recurrence_until": {series.Until},
				})
				saved := stored()
				for _, name := range []string{"DTSTART", "DTEND", "RRULE"} {
					before, _ := ical.ComponentProperty(original, name)
					after, _ := ical.ComponentProperty(saved, name)
					beforeZone, _ := ical.PropertyParam(before.KeyPart, "TZID")
					afterZone, _ := ical.PropertyParam(after.KeyPart, "TZID")
					if after.Value != before.Value || afterZone != beforeZone {
						t.Errorf("saving the series unchanged wrote %s;TZID=%s:%s, want %s;TZID=%s:%s", name, afterZone, after.Value, name, beforeZone, before.Value)
					}
				}
				if got := freeBusy(); !slices.Equal(got, published) {
					t.Errorf("after saving the series unchanged, CalDAV publishes %v, want %v", got, published)
				}

				occurrence := result.Forms.Occurrence
				updateServedEvent(t, handler, user, occurrence.Zone, url.Values{
					"dtstart": {occurrence.Values[0]}, "dtend": {occurrence.Values[1]}, "summary": {"Renamed standup"},
					"edit_scope": {"occurrence"}, "recurrence_id": {occurrence.Values[2]}, "recurrence_timezone": {occurrence.RecurrenceZone},
				})
				if got := freeBusy(); !slices.Equal(got, published) {
					t.Errorf("after renaming one occurrence, CalDAV publishes %v, want %v\n%s", got, published, events.events["1:outlook-standup"].RawICAL)
				}
				if raw := events.events["1:outlook-standup"].RawICAL; !strings.Contains(raw, "RECURRENCE-ID;TZID=Eastern Standard Time:20240318T090000") &&
					!strings.Contains(raw, "RECURRENCE-ID;TZID=\"Eastern Standard Time\":20240318T090000") {
					t.Errorf("the renamed occurrence is not the 18 March instance:\n%s", raw)
				}
			})
		}
	}
}

// servedOutlookHandler is a handler over calendar 1, owned by user 100, holding
// servedOutlookCalendar as the validator stores it.
func servedOutlookHandler(t *testing.T) (*Handler, *store.Store, *fakeEventRepoWithUpsert) {
	t.Helper()
	stored := make(map[string]*store.Event)
	for i, uid := range slices.Sorted(maps.Keys(servedOutlookCalendar)) {
		raw := servedOutlookCalendar[uid]
		validated, err := dav.ValidateCalendarObject(raw, nil)
		if err != nil {
			t.Fatalf("%s should pass the validator: %v", uid, err)
		}
		stored["1:"+uid] = &store.Event{ID: int64(i + 1), CalendarID: 1, UID: uid, ResourceName: uid + ".ics", RawICAL: validated.Body, ETag: "e"}
	}
	calendar := store.Calendar{ID: 1, UserID: 100, Name: "Work"}
	events := &fakeEventRepoWithUpsert{fakeEventRepo: fakeEventRepo{events: stored}}
	s := &store.Store{
		Calendars: &fakeCalendarRepo{
			calendars:      map[int64]*store.Calendar{1: &calendar},
			listAccessible: []store.CalendarAccess{{Calendar: calendar}},
		},
		Events: events,
	}
	return NewHandler(&config.Config{}, s, nil, dav.ValidateCalendarObject, dav.EventInstances), s, events
}

// servedEventsJSON writes to a file the March 2024 events JSON the endpoint
// template loads its events from serves, and returns the file's path.
func servedEventsJSON(t *testing.T, handler *Handler, user *store.User, template string) string {
	t.Helper()
	w := httptest.NewRecorder()
	switch template {
	case "calendar_view.html":
		req := httptest.NewRequest(http.MethodGet, "/calendars/1/events.json?start=2024-03-01&end=2024-04-01", nil)
		rctx := chi.NewRouteContext()
		rctx.URLParams.Add("id", "1")
		handler.GetCalendarEventsJSON(w, req.WithContext(context.WithValue(auth.WithUser(req.Context(), user), chi.RouteCtxKey, rctx)))
	case "all_calendars_view.html":
		req := httptest.NewRequest(http.MethodGet, "/calendars/all/events.json?start=2024-03-01&end=2024-04-01", nil)
		handler.GetAllCalendarEventsJSON(w, req.WithContext(auth.WithUser(req.Context(), user)))
	default:
		t.Fatalf("no events endpoint for %s", template)
	}
	if w.Code != http.StatusOK {
		t.Fatalf("events JSON for %s: status %d: %s", template, w.Code, w.Body.String())
	}
	path := filepath.Join(t.TempDir(), "events.json")
	if err := os.WriteFile(path, w.Body.Bytes(), 0o600); err != nil {
		t.Fatal(err)
	}
	return path
}

// updateServedEvent submits the edit form for the "outlook-standup" series.
func updateServedEvent(t *testing.T, handler *Handler, user *store.User, zone string, fields url.Values) {
	t.Helper()
	w := httptest.NewRecorder()
	handler.UpdateEvent(w, eventFormRequest(t, "/calendars/1/events/outlook-standup", "outlook-standup", user, zone, fields))
	if location := w.Header().Get("Location"); w.Code != http.StatusFound || !strings.Contains(location, "status=event_updated") {
		t.Fatalf("UpdateEvent(%v) status = %d location = %s", fields, w.Code, location)
	}
}

// The event modal builds a markup string and assigns it to innerHTML, so every
// value interpolated into it has to be escaped at the point of interpolation.
// The recurrence summary reads its parts straight out of the stored RRULE, which arrives
// over CalDAV PUT or an ICS import and is never sanitized on the way in.
func TestEventModalEscapesRecurrenceSummary(t *testing.T) {
	for _, name := range []string{"calendar_view.html", "all_calendars_view.html"} {
		source, err := templateFS.ReadFile("templates/" + name)
		if err != nil {
			t.Fatalf("read %s: %v", name, err)
		}
		if !strings.Contains(string(source), "escapeHtml(recurrenceSummary(event.seriesEvent || event))") {
			t.Errorf("%s interpolates the recurrence summary into the modal unescaped", name)
		}
	}
}

// An on* attribute is a program the HTML parser hands to the JavaScript engine
// after decoding the character references in it, so a value escaped for markup
// arrives there unescaped. No template may build one out of data.
func TestTemplatesDoNotBuildEventHandlersByConcatenation(t *testing.T) {
	node := requireNode(t)
	files, err := fs.Glob(templateFS, "templates/*.html")
	if err != nil {
		t.Fatal(err)
	}
	for _, file := range files {
		source, err := templateFS.ReadFile(file)
		if err != nil {
			t.Fatal(err)
		}
		if !strings.Contains(string(source), "<script") {
			continue
		}
		t.Run(filepath.Base(file), func(t *testing.T) {
			cmd := exec.Command(node, filepath.Join("testdata", "no_concatenated_handlers.mjs"), file)
			if output, err := cmd.CombinedOutput(); err != nil {
				t.Fatalf("%v\n%s", err, output)
			}
		})
	}
	// The calendar views' shared event helpers are part of their scripts only
	// once the page is rendered.
	for _, name := range []string{"calendar_view.html", "all_calendars_view.html"} {
		t.Run("rendered "+name, func(t *testing.T) {
			cmd := exec.Command(node, filepath.Join("testdata", "no_concatenated_handlers.mjs"), renderedPage(t, name))
			if output, err := cmd.CombinedOutput(); err != nil {
				t.Fatalf("%v\n%s", err, output)
			}
		})
	}
}

// The contact modal's edit button is attached with addEventListener: an
// onclick attribute assembled as markup carries the contact UID as a
// JavaScript string literal the attribute parser hands over with its escaping
// already undone.
func TestContactModalAttachesTheEditHandler(t *testing.T) {
	node := requireNode(t)
	cmd := exec.Command(node, filepath.Join("testdata", "contact_modal_wiring.mjs"),
		filepath.Join("templates", "addressbook_view.html"), safeHTMLPartial)
	if output, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("contact modal wiring: %v\n%s", err, output)
	}
}

// sanitizeHtml is handed the X-ALT-DESC of an event, which arrives over CalDAV
// PUT or an ICS import as attacker-authored HTML and is never sanitized on the
// way in. Each assertion stands for a way the sanitizer can be defeated without
// ever tripping its allow list, checked where it matters: in the sanitizer's
// own body, and in how the pages hand it the description.
func TestCalendarViewsSanitizeHTMLDescriptionsInertly(t *testing.T) {
	partial, err := templateFS.ReadFile("templates/partials/safe_html_js.tmpl")
	if err != nil {
		t.Fatalf("read the shared helpers: %v", err)
	}
	body, ok := javaScriptFunction(string(partial), "sanitizeHtml")
	if !ok {
		t.Fatal("the shared helpers have no sanitizeHtml")
	}
	// A node of the live document starts loading an <img onerror> as soon as
	// innerHTML is assigned -- before the allow list has looked at anything. A
	// document with no browsing context loads nothing.
	if !strings.Contains(body, "document.implementation.createHTMLDocument(") {
		t.Error("sanitizeHtml does not parse the description into an inert document")
	}
	for _, liveParse := range []string{"document.createElement('div')", "document.createElement(\"div\")", "document.body.innerHTML"} {
		if strings.Contains(body, liveParse) {
			t.Errorf("sanitizeHtml parses in the live document (%s)", liveParse)
		}
	}
	if strings.Contains(body, ".innerHTML;") || strings.Contains(body, "return inert") {
		t.Error("sanitizeHtml returns markup or the parsed tree rather than a node built afresh")
	}
	// trim removes no interior whitespace, but the URL parser removes tab and
	// newline from a scheme, so java&#9;script: passes a startsWith test and
	// still navigates as javascript:.
	if strings.Contains(body, "startsWith('javascript:')") {
		t.Error("sanitizeHtml decides an href scheme from the unparsed text")
	}
	if !strings.Contains(body, "safeUrl(node.getAttribute('href'))") {
		t.Error("sanitizeHtml does not put an href through the parsing scheme allow list")
	}

	for _, name := range []string{"calendar_view.html", "all_calendars_view.html"} {
		source, err := templateFS.ReadFile("templates/" + name)
		if err != nil {
			t.Fatalf("read %s: %v", name, err)
		}
		// Re-serializing the sanitized tree and parsing it a second time
		// reopens the hole wherever the parser and the serializer disagree.
		if strings.Contains(string(source), "+ sanitizeHtml(") {
			t.Errorf("%s concatenates the sanitized description back into markup that is parsed again", name)
		}
		if !strings.Contains(string(source), ".appendChild(sanitizeHtml(event.htmlDescription))") {
			t.Errorf("%s does not append the sanitized description as a node", name)
		}
	}
}

// Runs the shared sanitizeHtml over the descriptions it has to defuse. The
// attribute filter has to be an allow list: a filter that refuses attributes
// by name keeps every attribute nobody named, and on the tags this sanitizer
// allows that includes id, which is enough to take over the modal without
// executing anything.
func TestEventDescriptionSanitizerKeepsOnlyAllowedAttributes(t *testing.T) {
	node := requireNode(t)
	cmd := exec.Command(node, filepath.Join("testdata", "description_sanitizer.mjs"), safeHTMLPartial)
	if output, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("description sanitizer: %v\n%s", err, output)
	}
}

// The helpers that stand between stored calendar and contact data and the page
// live in one partial. A page carrying its own copy of any of them is a copy a
// hardening applied to the partial does not reach.
func TestPagesShareOneCopyOfTheSafeHTMLHelpers(t *testing.T) {
	helpers := []string{"escapeHtml", "unescapeText", "safeUrl", "safeHref", "sanitizeHtml"}
	partial, err := templateFS.ReadFile("templates/partials/safe_html_js.tmpl")
	if err != nil {
		t.Fatalf("read the shared helpers: %v", err)
	}
	for _, helper := range helpers {
		if _, ok := javaScriptFunction(string(partial), helper); !ok {
			t.Errorf("the shared helpers have no %s", helper)
		}
	}
	for _, name := range []string{"calendar_view.html", "all_calendars_view.html", "addressbook_view.html", "birthdays.html"} {
		source, err := templateFS.ReadFile("templates/" + name)
		if err != nil {
			t.Fatalf("read %s: %v", name, err)
		}
		if !strings.Contains(string(source), "<script>\n{{template \"safe_html_js\"}}\n</script>") {
			t.Errorf("%s does not include the shared helpers in a script of their own", name)
		}
		if templates[name].Lookup("safe_html_js") == nil {
			t.Errorf("%s is parsed without the shared helpers", name)
		}
		for _, helper := range append(helpers, "unescapeICAL", "unescapeVCard") {
			if strings.Contains(string(source), "function "+helper+"(") {
				t.Errorf("%s defines its own %s", name, helper)
			}
		}
	}
}

// Both calendar views read events and expand series through one partial, which
// each includes inside its own script's function scope. A view carrying its own
// copy of any of the partial's functions is a copy that can come to disagree
// with the other view about which occurrences a series has.
func TestCalendarViewsShareOneCopyOfTheEventHelpers(t *testing.T) {
	partial, err := templateFS.ReadFile(calendarEventsPartialFile)
	if err != nil {
		t.Fatalf("read the shared event helpers: %v", err)
	}
	var shared []string
	for _, match := range regexp.MustCompile(`(?m)^    function (\w+)\(`).FindAllStringSubmatch(string(partial), -1) {
		shared = append(shared, match[1])
	}
	for _, want := range []string{
		"parseICAL", "parseICALDate", "listedOffsets", "intlOffsets", "timeZoneClock",
		"rruleUntilDay", "expandRecurringEvents", "overrideRecurrenceIdAllDay", "recurrenceSummary", "parseServerDate", "normalizeServerEvent", "formatRRule",
		"formatDateTimeForInput", "browserTimeZone",
	} {
		if !slices.Contains(shared, want) {
			t.Errorf("the shared event helpers have no %s", want)
		}
	}

	const include = `{{template "calendar_events_js"}}`
	for _, name := range []string{"calendar_view.html", "all_calendars_view.html"} {
		t.Run(name, func(t *testing.T) {
			raw, err := templateFS.ReadFile("templates/" + name)
			if err != nil {
				t.Fatalf("read %s: %v", name, err)
			}
			source := string(raw)
			if templates[name].Lookup("calendar_events_js") == nil {
				t.Error("the view is parsed without the shared event helpers")
			}
			// Included as a script of its own, the helpers would become globals
			// and stop seeing the view's own scope.
			at := strings.Index(source, include)
			if at < 0 {
				t.Fatal("the view does not include the shared event helpers")
			}
			iife := strings.LastIndex(source[:at], "<script>\n(function() {")
			if iife < 0 || strings.Contains(source[iife:at], "})();") || !strings.Contains(source[at:], "\n})();") {
				t.Error("the view includes the shared event helpers outside its script's function")
			}
			if strings.Count(source, include) != 1 {
				t.Error("the view includes the shared event helpers more than once")
			}
			for _, fn := range shared {
				if strings.Contains(source, "function "+fn+"(") {
					t.Errorf("the view defines its own %s", fn)
				}
			}

			body, err := os.ReadFile(renderedPage(t, name))
			if err != nil {
				t.Fatal(err)
			}
			for _, fn := range shared {
				if got := strings.Count(string(body), "function "+fn+"("); got != 1 {
					t.Errorf("the rendered view defines %s %d times, want once", fn, got)
				}
			}
		})
	}
}

// The text of a named JavaScript function declaration, from the keyword through
// the brace that closes its body.
func javaScriptFunction(source, name string) (string, bool) {
	start := strings.Index(source, "function "+name+"(")
	if start < 0 {
		return "", false
	}
	depth := 0
	for i := start; i < len(source); i++ {
		switch source[i] {
		case '{':
			depth++
		case '}':
			depth--
			if depth == 0 {
				return source[start : i+1], true
			}
		}
	}
	return "", false
}

// escapeHtml, safeUrl and safeHref are the guards between stored data and the
// modals' innerHTML.
func TestEventModalEscapingHelpers(t *testing.T) {
	node := requireNode(t)
	cmd := exec.Command(node, filepath.Join("testdata", "modal_escaping.mjs"), safeHTMLPartial)
	if output, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("escaping helpers: %v\n%s", err, output)
	}
}

// A 29 February birthday built as a Date in a common year is 1 March, so the
// birthdays page places, labels and counts down to it through its own helpers.
func TestBirthdaysPageKeepsLeapDayBirthdays(t *testing.T) {
	node := requireNode(t)
	for _, zone := range []string{"UTC", "America/New_York"} {
		t.Run(zone, func(t *testing.T) {
			cmd := exec.Command(node, filepath.Join("testdata", "birthdays_leap_day.mjs"),
				filepath.Join("templates", "birthdays.html"), safeHTMLPartial)
			cmd.Env = append(os.Environ(), "TZ="+zone)
			if output, err := cmd.CombinedOutput(); err != nil {
				t.Fatalf("%v\n%s", err, output)
			}
		})
	}
}

// A DATE-TIME ending in Z is UTC. Read as local time it moves by the browser's
// offset, which only shows where that offset is not zero, so the zones are the
// point of the table.
func TestCalendarViewsReadUTCDateTimesAsUTC(t *testing.T) {
	node := requireNode(t)
	for _, name := range []string{"calendar_view.html", "all_calendars_view.html"} {
		rendered := renderedPage(t, name)
		for _, zone := range []string{"UTC", "America/New_York", "Asia/Kolkata", "Pacific/Kiritimati"} {
			t.Run(name+"/"+zone, func(t *testing.T) {
				cmd := exec.Command(node, filepath.Join("testdata", "ical_date_parsing.mjs"), rendered)
				cmd.Env = append(os.Environ(), "TZ="+zone)
				if output, err := cmd.CombinedOutput(); err != nil {
					t.Fatalf("%v\n%s", err, output)
				}
			})
		}
	}
}

// A series ends on its UNTIL's day: a DATE UNTIL includes the day, which ends
// in the zone the series' DTSTART is read in, and the UTC UNTIL the server
// writes for a timed series is the picked day's last second in the event's
// zone, or in UTC for an event with no zone or one the browser does not know.
// The node script runs each view's own reading of that day, which the
// recurrence summary names. The editors show the day the server reads
// (TestShownRecurrenceEndsOnTheDayTheEditorWrites).
func TestCalendarViewsEndSeriesOnTheUntilDay(t *testing.T) {
	node := requireNode(t)
	for _, name := range []string{"calendar_view.html", "all_calendars_view.html"} {
		rendered := renderedPage(t, name)
		for _, zone := range []string{"UTC", "America/New_York", "Europe/Berlin", "Asia/Kolkata", "Asia/Tokyo", "Pacific/Kiritimati"} {
			t.Run(name+"/"+zone, func(t *testing.T) {
				cmd := exec.Command(node, filepath.Join("testdata", "recurrence_until.mjs"), rendered)
				cmd.Env = append(os.Environ(), "TZ="+zone)
				if output, err := cmd.CombinedOutput(); err != nil {
					t.Fatalf("%v\n%s", err, output)
				}
			})
		}
	}
}

// The birthday controls spell a date out of three fields, and only the page
// decides how long the day list is. A date no month has is refused by the
// server, and an empty field removes the stored birthday, so the form has to
// decline to spell the first and refuse to submit the second.
func TestBirthdayDayOptionsFollowTheMonth(t *testing.T) {
	node := requireNode(t)
	cmd := exec.Command(node, "testdata/birthday_day_options.mjs", filepath.Join("templates", "addressbook_view.html"))
	if output, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("%v\n%s", err, output)
	}
}

// A stored date goes into the edit form and back out as the value the server
// writes, so the page's reading of it has to be exact in every zone: a
// date-time read through Date lands on the previous day west of UTC.
func TestContactVCardParsing(t *testing.T) {
	node := requireNode(t)
	for _, zone := range []string{"UTC", "America/Los_Angeles", "Pacific/Kiritimati"} {
		t.Run(zone, func(t *testing.T) {
			cmd := exec.Command(node, filepath.Join("testdata", "contact_vcard_parsing.mjs"),
				filepath.Join("templates", "addressbook_view.html"), safeHTMLPartial)
			cmd.Env = append(os.Environ(), "TZ="+zone)
			if output, err := cmd.CombinedOutput(); err != nil {
				t.Fatalf("%v\n%s", err, output)
			}
		})
	}
}

// The handler-attribute check is only as good as the shapes it recognizes, so
// it is run over a catalogue of them -- each concatenation shape it must flag
// and each inert one it must pass.
func TestNoConcatenatedHandlersDetector(t *testing.T) {
	node := requireNode(t)
	cmd := exec.Command(node, filepath.Join("testdata", "no_concatenated_handlers.mjs"), "--self-test")
	if output, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("%v\n%s", err, output)
	}
}

// safeHTMLPartial is the shared helper partial, relative to the package.
var safeHTMLPartial = filepath.Join("templates", "partials", "safe_html_js.tmpl")

// calendarEventsPartialFile is the calendar views' shared event reading and
// recurrence partial, as a path in templateFS.
const calendarEventsPartialFile = "templates/partials/calendar_events_js.tmpl"

// requireNode returns the node binary the template JavaScript tests run on. A
// developer machine without node skips them; CI must not, or the helpers these
// tests guard would go unchecked without anyone noticing.
func requireNode(t *testing.T) string {
	t.Helper()
	node, err := exec.LookPath("node")
	if err != nil {
		if os.Getenv("CI") != "" {
			t.Fatal("node is not installed, and CI must run the template JavaScript tests")
		}
		t.Skip("node is not installed; skipping the template JavaScript tests")
	}
	return node
}

// renderedPage serves the named page through its handler to a user who owns
// one calendar and one address book, and returns the path of a file holding
// the page as the browser gets it: with its partials included and its
// template actions executed.
func renderedPage(t *testing.T, name string) string {
	t.Helper()
	withUser := func(req *http.Request, params map[string]string) *http.Request {
		rctx := chi.NewRouteContext()
		for key, value := range params {
			rctx.URLParams.Add(key, value)
		}
		req = req.WithContext(context.WithValue(req.Context(), chi.RouteCtxKey, rctx))
		return req.WithContext(auth.WithUser(req.Context(), &store.User{ID: 100, PrimaryEmail: "owner@example.com"}))
	}
	handler := NewHandler(&config.Config{}, &store.Store{
		Calendars: &fakeCalendarRepo{calendars: map[int64]*store.Calendar{1: {ID: 1, UserID: 100, Name: "Work"}}},
		AddressBooks: &fakeAddressBookRepo{books: map[int64]*store.AddressBook{
			1: {ID: 1, UserID: 100, Name: "Contacts"},
		}},
		Contacts: &fakeContactRepoWithBirthdays{fakeContactRepo: fakeContactRepo{contacts: map[string]*store.Contact{}}},
	}, nil, dav.ValidateCalendarObject, dav.EventInstances)

	var serve http.HandlerFunc
	var req *http.Request
	switch name {
	case "calendar_view.html":
		serve, req = handler.ViewCalendar, withUser(httptest.NewRequest(http.MethodGet, "/calendars/1", nil), map[string]string{"id": "1"})
	case "all_calendars_view.html":
		serve, req = handler.ViewAllCalendars, withUser(httptest.NewRequest(http.MethodGet, "/calendars/all", nil), nil)
	case "addressbook_view.html":
		serve, req = handler.ViewAddressBook, withUser(httptest.NewRequest(http.MethodGet, "/addressbooks/1", nil), map[string]string{"id": "1"})
	case "birthdays.html":
		serve, req = handler.ViewBirthdays, withUser(httptest.NewRequest(http.MethodGet, "/birthdays", nil), nil)
	default:
		t.Fatalf("no handler is set up to render %s", name)
	}

	w := httptest.NewRecorder()
	serve(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("render %s: status = %d, want %d: %s", name, w.Code, http.StatusOK, w.Body.String())
	}
	rendered := filepath.Join(t.TempDir(), name)
	if err := os.WriteFile(rendered, w.Body.Bytes(), 0o600); err != nil {
		t.Fatal(err)
	}
	return rendered
}

// The partial reaches the browser through html/template, which parses the
// script it sits in. Running the shared helpers' tests over each rendered page
// proves every page serves them, once, and unaltered.
func TestRenderedPagesServeTheSharedSafeHTMLHelpers(t *testing.T) {
	node := requireNode(t)
	for _, name := range []string{"calendar_view.html", "all_calendars_view.html", "addressbook_view.html", "birthdays.html"} {
		t.Run(name, func(t *testing.T) {
			rendered := renderedPage(t, name)
			source, err := os.ReadFile(rendered)
			if err != nil {
				t.Fatal(err)
			}
			body := string(source)
			for _, helper := range []string{"escapeHtml", "unescapeText", "safeUrl", "safeHref", "sanitizeHtml"} {
				if got := strings.Count(body, "function "+helper+"("); got != 1 {
					t.Errorf("the page defines %s %d times, want once", helper, got)
				}
			}
			for _, script := range []string{"modal_escaping.mjs", "description_sanitizer.mjs"} {
				cmd := exec.Command(node, filepath.Join("testdata", script), rendered)
				if output, err := cmd.CombinedOutput(); err != nil {
					t.Errorf("%s over the rendered page: %v\n%s", script, err, output)
				}
			}
		})
	}
}
