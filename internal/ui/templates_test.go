package ui

import (
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/jw6ventures/calcard/internal/ical"
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
		"allDay ? formatDateInput(inclusiveAllDayEnd(startOfDay(start), end)) : formatDateTimeInput(end, timezone)",
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
	for _, name := range []string{"all_calendars_view.html", "calendar_view.html"} {
		source, err := templateFS.ReadFile("templates/" + name)
		if err != nil {
			t.Fatalf("read %s: %v", name, err)
		}
		for _, want := range []string{
			"function parseServerDate(value, allDay)",
			"parsed.dtstart = parseServerDate(raw.dtstart, allDay);",
			"parsed.dtend = parseServerDate(raw.dtend, allDay);",
			"parsed.recurrenceId = parseServerDate(raw.recurrenceId, raw.recurrenceIdAllDay || allDay);",
		} {
			if !strings.Contains(string(source), want) {
				t.Errorf("%s is missing %q: an all-day date keeps the instant the server sent", name, want)
			}
		}
		// map would hand parseServerDate the array index as its all-day flag,
		// which is false for the first EXDATE and true for every one after it.
		if strings.Contains(string(source), ".map(parseServerDate)") {
			t.Errorf("%s passes parseServerDate to map, so the index becomes the all-day flag", name)
		}
	}
}

// Runs the template's own date helpers over the save round-trip that used to
// extend an all-day event by a day each time, and over the server payload that
// used to move it back a day each time. Skipped where node is absent, which the
// assertions above partly cover for.
//
// The zones are the point of the table: an all-day date the server sends as UTC
// midnight only lands on the wrong day where the browser's offset is negative,
// so a run in one zone proves nothing about the others.
func TestAggregateEditorAllDayEndRoundTrip(t *testing.T) {
	node, err := exec.LookPath("node")
	if err != nil {
		t.Skip("node is not installed; skipping the template JavaScript round-trip")
	}
	zones := []string{
		"UTC",
		"America/New_York",   // -04:00/-05:00, where the drift appeared
		"America/Anchorage",  // -08:00/-09:00
		"Pacific/Kiritimati", // +14:00, the far side
		"Asia/Kolkata",       // +05:30, not a whole hour
	}
	for _, zone := range zones {
		t.Run(zone, func(t *testing.T) {
			cmd := exec.Command(node, filepath.Join("testdata", "all_day_end_roundtrip.mjs"),
				filepath.Join("templates", "all_calendars_view.html"))
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
	node, err := exec.LookPath("node")
	if err != nil {
		t.Skip("node is not installed; skipping the template JavaScript round-trip")
	}
	zones := []string{
		"UTC",
		"America/New_York",   // -04:00/-05:00, where the drift appeared
		"America/Anchorage",  // -08:00/-09:00
		"Pacific/Kiritimati", // +14:00, the far side
		"Asia/Kolkata",       // +05:30, not a whole hour
	}
	for _, zone := range zones {
		t.Run(zone, func(t *testing.T) {
			cmd := exec.Command(node, filepath.Join("testdata", "calendar_view_all_day_end_roundtrip.mjs"),
				filepath.Join("templates", "calendar_view.html"))
			cmd.Env = append(os.Environ(), "TZ="+zone)
			output, err := cmd.CombinedOutput()
			if err != nil {
				t.Fatalf("all-day round-trip failed in %s: %v\n%s", zone, err, output)
			}
		})
	}
}

func TestTimeGridDST(t *testing.T) {
	node, err := exec.LookPath("node")
	if err != nil {
		t.Skip("node is not installed")
	}
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

func TestTimedEditorTimezoneRoundTrip(t *testing.T) {
	node, err := exec.LookPath("node")
	if err != nil {
		t.Skip("node is not installed")
	}
	for _, template := range []string{"all_calendars_view.html", "calendar_view.html"} {
		for _, browserZone := range []string{"America/Los_Angeles", "UTC", "Pacific/Kiritimati"} {
			t.Run(template+"/"+browserZone, func(t *testing.T) {
				cmd := exec.Command(node, filepath.Join("testdata", "timed_editor_roundtrip.mjs"), filepath.Join("templates", template))
				cmd.Env = append(os.Environ(), "TZ="+browserZone)
				output, err := cmd.CombinedOutput()
				if err != nil {
					t.Fatalf("template helpers: %v: %s", err, output)
				}
				var cases []struct {
					Start, End, Zone, RecurrenceZone string
					Values                           []string
				}
				if err := json.Unmarshal(output, &cases); err != nil {
					t.Fatal(err)
				}
				for _, c := range cases {
					for i, prop := range []string{"DTSTART", "DTEND", "RECURRENCE-ID"} {
						zone := c.Zone
						if i == 2 {
							zone = c.RecurrenceZone
						}
						line, err := utils.FormatICalDateTime(c.Values[i], false, false, prop, zone)
						if err != nil {
							t.Fatal(err)
						}
						key, value, _ := strings.Cut(line, ":")
						got, ok := ical.ParsePropertyDateTimeLocal(key, value)
						original := c.Start
						if i == 1 {
							original = c.End
						}
						want, err := time.Parse(time.RFC3339, original)
						if err != nil {
							t.Fatal(err)
						}
						if !ok || !got.Equal(want) {
							t.Errorf("%s: %s submitted as %s, want instant %s", prop, original, line, want)
						}
					}
				}
			})
		}
	}
}
