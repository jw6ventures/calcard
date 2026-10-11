package ui

import (
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/jw6ventures/calcard/internal/auth"
	"github.com/jw6ventures/calcard/internal/config"
	"github.com/jw6ventures/calcard/internal/dav"
	"github.com/jw6ventures/calcard/internal/ical"
	"github.com/jw6ventures/calcard/internal/store"
)

// The calendar view reads a TZID the object does not define as the DAV layer
// and the stored columns do: through ical.HostLocation, which knows every zone
// the server's embedded database names, backward-compatible names included,
// and otherwise as UTC. It keeps no aliases of its own that would read a name
// the others cannot place.
func TestCalendarViewReadsAnUndefinedTZIDAsTheDAVLayerDoes(t *testing.T) {
	lines := ical.UnfoldLines("BEGIN:VCALENDAR\r\nVERSION:2.0\r\nBEGIN:VEVENT\r\nUID:x\r\nDTSTART:20260105T090000Z\r\nEND:VEVENT\r\nEND:VCALENDAR\r\n")
	wall := time.Date(2026, 1, 5, 9, 0, 0, 0, time.UTC)
	zones := &calendarEventZones{times: ical.NewObjectTimes(lines)}
	for _, tzid := range []string{"US/Eastern", "America/New_York", "Nowhere/Zone"} {
		want, ok := ical.NewObjectTimes(lines).ResolvePropertyTime("DTSTART;TZID="+tzid, "20260105T090000")
		if !ok {
			t.Fatalf("%s: the DAV layer's reading does not resolve", tzid)
		}
		if got := zones.instant(tzid, wall); !got.Equal(want) {
			t.Errorf("%s: the calendar view places 09:00 at %v, the DAV layer at %v", tzid, got, want)
		}
	}
}

// The calendar view reads an RRULE's parts as the DAV layer does, through
// ical.RRuleParam: names in any case, white space around them, and the first
// of a repeated part.
func TestCalendarViewReadsRRulePartsAsTheDAVLayerDoes(t *testing.T) {
	for _, rrule := range []string{
		"freq=daily;until=20260301T000000Z",
		"FREQ=DAILY ; UNTIL = 20260301T000000Z",
		"FREQ=DAILY;UNTIL=20260301T000000Z;UNTIL=20270301T000000Z",
	} {
		want, _ := ical.RRuleParam(rrule, "UNTIL")
		if got := rruleUntil(rrule); got != want || got != "20260301T000000Z" {
			t.Errorf("rruleUntil(%q) = %q, want %q", rrule, got, want)
		}
		if got := ical.RRuleParts(rrule)["UNTIL"]; got != want {
			t.Errorf("ical.RRuleParts(%q)[UNTIL] = %q, want %q", rrule, got, want)
		}
		if !rruleUntilEndsBefore(rruleUntil(rrule), time.Date(2026, 4, 1, 0, 0, 0, 0, time.UTC), "DTSTART:20260105T090000Z", nil) {
			t.Errorf("the month filter keeps %q in April", rrule)
		}
	}
}

// A DATE UNTIL ends a series with its day's last instance, a wall clock in the
// series' zone that can fall on a later UTC date: the Los Angeles series' 31
// January instance at 20:00 is 04:00Z on 1 February, so a February view keeps
// it, while a view starting days later does not.
func TestMonthFilterKeepsTheLastInstanceOfADateUntilInAWesternZone(t *testing.T) {
	raw := "BEGIN:VCALENDAR\r\nVERSION:2.0\r\nPRODID:-//Client//EN\r\n" +
		"BEGIN:VEVENT\r\nUID:evening\r\nDTSTAMP:20260101T000000Z\r\n" +
		"DTSTART;TZID=America/Los_Angeles:20260101T200000\r\nDTEND;TZID=America/Los_Angeles:20260101T210000\r\n" +
		"RRULE:FREQ=DAILY;UNTIL=20260131\r\nEND:VEVENT\r\nEND:VCALENDAR\r\n"
	events := []store.Event{{UID: "evening", RawICAL: raw}}
	february := time.Date(2026, 2, 1, 0, 0, 0, 0, time.UTC)
	if got := filterEventsForMonth(events, february, february.AddDate(0, 1, 0)); len(got) != 1 {
		t.Error("the February view leaves out the series whose last instance is 1 February 04:00Z")
	}
	later := time.Date(2026, 2, 5, 0, 0, 0, 0, time.UTC)
	if got := filterEventsForMonth(events, later, later.AddDate(0, 1, 0)); len(got) != 0 {
		t.Error("a view starting 5 February keeps the series")
	}
}

// The event form's start and end have the shape its all-day box says, and a
// value of the other shape is refused rather than dropped from the event.
func TestCreateEventRefusesDatesOfTheWrongShape(t *testing.T) {
	for name, form := range map[string]url.Values{
		"all-day with a timed end": {"summary": {"Trip"}, "all_day": {"on"}, "dtstart": {"2026-03-20"}, "dtend": {"2026-03-25T10:00"}},
		"timed with a date end":    {"summary": {"Call"}, "dtstart": {"2026-03-20T10:00"}, "dtend": {"2026-03-21"}},
	} {
		t.Run(name, func(t *testing.T) {
			eventRepo := &fakeEventRepoWithUpsert{fakeEventRepo: fakeEventRepo{events: make(map[string]*store.Event)}}
			handler := NewHandler(&config.Config{}, &store.Store{
				Calendars: &fakeCalendarRepo{calendars: map[int64]*store.Calendar{1: {ID: 1, UserID: 100, Name: "Work"}}},
				Events:    eventRepo,
			}, nil, dav.ValidateCalendarObject, dav.EventInstances)
			req := httptest.NewRequest(http.MethodPost, "/calendars/1/events", strings.NewReader(form.Encode()))
			req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
			req = withRouteID(req, "1")
			req = req.WithContext(auth.WithUser(req.Context(), &store.User{ID: 100, PrimaryEmail: "owner@example.com"}))
			w := httptest.NewRecorder()
			handler.CreateEvent(w, req)
			if location := w.Header().Get("Location"); w.Code != http.StatusFound || !strings.Contains(location, "error=") {
				t.Errorf("CreateEvent() = %d to %s, want the form refused", w.Code, location)
			}
			if len(eventRepo.events) != 0 {
				t.Errorf("stored %v", eventRepo.events)
			}
		})
	}
}
