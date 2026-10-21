package ui

import (
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"testing"

	"github.com/jw6ventures/calcard/internal/config"
	"github.com/jw6ventures/calcard/internal/dav"
	"github.com/jw6ventures/calcard/internal/ical"
	"github.com/jw6ventures/calcard/internal/store"
)

// misspeltZoneSeries is a daily 09:00 series in "america/new_york", a name the
// host's zone database, which is case-sensitive, does not define and the
// object does not define either: a 1.1.x release stored it, and CalDAV reads
// its times in UTC, as the server does for any TZID neither defines.
const misspeltZoneSeries = "BEGIN:VCALENDAR\r\nVERSION:2.0\r\nPRODID:-//Client//EN\r\n" +
	"BEGIN:VEVENT\r\nUID:misspelt\r\nDTSTAMP:20240101T000000Z\r\nSUMMARY:Misspelt\r\n" +
	"DTSTART;TZID=america/new_york:20240304T090000\r\nDTEND;TZID=america/new_york:20240304T100000\r\n" +
	"RRULE:FREQ=DAILY;COUNT=3\r\nEND:VEVENT\r\nEND:VCALENDAR\r\n"

// servedStoredHandler is a handler over calendar 1 holding each of raws as
// stored, whether or not the validator accepts it.
func servedStoredHandler(raws map[string]string) *Handler {
	stored := make(map[string]*store.Event)
	id := int64(1)
	for uid, raw := range raws {
		stored["1:"+uid] = &store.Event{ID: id, CalendarID: 1, UID: uid, ResourceName: uid + ".ics", RawICAL: raw, ETag: "e"}
		id++
	}
	calendar := store.Calendar{ID: 1, UserID: 100, Name: "Work"}
	s := &store.Store{
		Calendars: &fakeCalendarRepo{
			calendars:      map[int64]*store.Calendar{1: &calendar},
			listAccessible: []store.CalendarAccess{{Calendar: calendar}},
		},
		Events: &fakeEventRepoWithUpsert{fakeEventRepo: fakeEventRepo{events: stored}},
	}
	return NewHandler(&config.Config{}, s, nil, dav.ValidateCalendarObject, dav.EventInstances)
}

// The view model lists, for a TZID neither the object nor the host defines,
// the UTC offset the server reads it with, so the browser, whose Intl accepts
// the name in any case, does not read it in a zone the server does not.
func TestEventsJSONListsUTCForAZoneNeitherObjectNorHostDefines(t *testing.T) {
	if _, ok := ical.HostLocation("america/new_york"); ok {
		t.Skip("the host defines america/new_york")
	}
	_, payload := servedSeries(t, misspeltZoneSeries, "2024-03-01", "2024-04-01")
	if got := payload["dtstart"]; got != "2024-03-04T09:00:00Z" {
		t.Fatalf("dtstart = %v, want the wall clock read as UTC", got)
	}
	listings, _ := payload["timezoneOffsets"].(map[string]any)
	listing, _ := listings["america/new_york"].([]any)
	if len(listing) != 1 || listing[0].(map[string]any)["offset"] != float64(0) {
		t.Errorf("timezoneOffsets = %v, want america/new_york at a UTC offset throughout", payload["timezoneOffsets"])
	}
}

// A zone the host defines is left to the browser, which shares its database.
func TestEventsJSONLeavesAZoneTheHostDefinesToTheBrowser(t *testing.T) {
	if _, ok := ical.HostLocation("America/New_York"); !ok {
		t.Skip("the host does not define America/New_York")
	}
	raw := "BEGIN:VCALENDAR\r\nVERSION:2.0\r\nPRODID:-//Client//EN\r\n" +
		"BEGIN:VEVENT\r\nUID:series\r\nDTSTAMP:20240101T000000Z\r\nSUMMARY:Series\r\n" +
		"DTSTART;TZID=America/New_York:20240304T090000\r\nRRULE:FREQ=DAILY;COUNT=3\r\nEND:VEVENT\r\nEND:VCALENDAR\r\n"
	_, payload := servedSeries(t, raw, "2024-03-01", "2024-04-01")
	if listings, listed := payload["timezoneOffsets"]; listed {
		t.Errorf("timezoneOffsets = %v, want none for a zone the host defines", listings)
	}
}

// Both views show the times of a series in a misspelt zone as the server reads
// them, in UTC, in whatever zone the browser is in, and submit them back the
// way the server reads them.
func TestCalendarViewsReadAMisspeltZoneAsTheServerDoes(t *testing.T) {
	if _, ok := ical.HostLocation("america/new_york"); ok {
		t.Skip("the host defines america/new_york")
	}
	node := requireNode(t)
	user := &store.User{ID: 100}
	for _, template := range []string{"calendar_view.html", "all_calendars_view.html"} {
		rendered := renderedPage(t, template)
		for _, browserZone := range []string{"UTC", "America/New_York", "Europe/Berlin", "Asia/Tokyo"} {
			t.Run(template+"/"+browserZone, func(t *testing.T) {
				handler := servedStoredHandler(map[string]string{"misspelt": misspeltZoneSeries})
				served := servedEventsJSON(t, handler, user, template)
				cmd := exec.Command(node, filepath.Join("testdata", "served_event_editor.mjs"), rendered, served, "misspelt")
				cmd.Env = append(os.Environ(), "TZ="+browserZone)
				output, err := cmd.CombinedOutput()
				if err != nil {
					t.Fatalf("view helpers: %v: %s", err, output)
				}
				var result struct{ Values []string }
				if err := json.Unmarshal(output, &result); err != nil {
					t.Fatalf("%v: %s", err, output)
				}
				if len(result.Values) < 2 || result.Values[0] != "2024-03-04T09:00" || result.Values[1] != "2024-03-04T10:00" {
					t.Errorf("the editor shows %v, want 09:00 to 10:00, the wall clocks the server reads as UTC", result.Values)
				}
			})
		}
	}
}
