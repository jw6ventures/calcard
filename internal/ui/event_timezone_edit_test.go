package ui

import (
	"context"
	"encoding/json"
	"maps"
	"net/http"
	"net/http/httptest"
	"net/url"
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

// An edit that moves an event out of the years its stored VTIMEZONE was
// generated for leaves CalDAV reading the edited times as the UI wrote them:
// in the host's zone, at the instant the stored dtstart column holds.
func TestUpdateEventKeepsTheZoneDefinitionCoveringTheEditedTimes(t *testing.T) {
	tests := []struct {
		name string
		zone string
		// create and edit are the form fields the event is created and
		// then edited with, over the fields every request carries.
		create, edit url.Values
		// from and to bound the free-busy query, and starts are the
		// wall clocks, in zone, of the hour-long periods it publishes.
		from, to string
		starts   []string
	}{
		{
			name:   "whole event moved before the definition's first observance",
			zone:   "America/New_York",
			create: url.Values{"dtstart": {"2026-06-15T10:00"}, "dtend": {"2026-06-15T11:00"}},
			edit:   url.Values{"dtstart": {"2025-01-15T10:00"}, "dtend": {"2025-01-15T11:00"}},
			from:   "20250101T000000Z", to: "20250201T000000Z",
			starts: []string{"2025-01-15T10:00"},
		},
		{
			name:   "whole event moved past the definition's listed transitions",
			zone:   "Africa/Cairo",
			create: url.Values{"dtstart": {"2026-06-15T10:00"}, "dtend": {"2026-06-15T11:00"}},
			edit:   url.Values{"dtstart": {"2027-12-10T10:00"}, "dtend": {"2027-12-10T11:00"}},
			from:   "20271201T000000Z", to: "20280101T000000Z",
			starts: []string{"2027-12-10T10:00"},
		},
		{
			name:   "single event turned into a series running past the definition",
			zone:   "Africa/Cairo",
			create: url.Values{"dtstart": {"2026-06-15T10:00"}, "dtend": {"2026-06-15T11:00"}},
			edit: url.Values{
				"dtstart": {"2026-06-15T10:00"}, "dtend": {"2026-06-15T11:00"},
				"recurrence": {"WEEKLY"}, "recurrence_byday": {"MO"},
			},
			from: "20271201T000000Z", to: "20280101T000000Z",
			starts: []string{"2027-12-06T10:00", "2027-12-13T10:00", "2027-12-20T10:00", "2027-12-27T10:00"},
		},
		{
			name: "occurrence moved before the definition's first observance",
			zone: "America/New_York",
			create: url.Values{
				"dtstart": {"2026-06-15T10:00"}, "dtend": {"2026-06-15T11:00"},
				"recurrence": {"WEEKLY"}, "recurrence_byday": {"MO"},
				"recurrence_end_type": {"after"}, "recurrence_count": {"4"},
			},
			edit: url.Values{
				"dtstart": {"2025-01-15T10:00"}, "dtend": {"2025-01-15T11:00"},
				"edit_scope": {"occurrence"}, "recurrence_id": {"2026-06-22T10:00"},
				"recurrence_timezone": {"America/New_York"},
			},
			from: "20250101T000000Z", to: "20250201T000000Z",
			starts: []string{"2025-01-15T10:00"},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			loc, err := time.LoadLocation(tt.zone)
			if err != nil {
				t.Skipf("%s is not available: %v", tt.zone, err)
			}
			calRepo := &fakeCalendarRepo{calendars: map[int64]*store.Calendar{1: {ID: 1, UserID: 100, Name: "Work"}}}
			eventRepo := &fakeEventRepoWithUpsert{fakeEventRepo: fakeEventRepo{events: make(map[string]*store.Event)}}
			s := &store.Store{Calendars: calRepo, Events: eventRepo}
			handler := NewHandler(&config.Config{}, s, nil, dav.ValidateCalendarObject)
			user := &store.User{ID: 100, PrimaryEmail: "owner@example.com"}

			w := httptest.NewRecorder()
			handler.CreateEvent(w, eventFormRequest(t, "/calendars/1/events", "", user, tt.zone, tt.create))
			if location := w.Header().Get("Location"); w.Code != http.StatusFound || !strings.Contains(location, "status=event_created") {
				t.Fatalf("CreateEvent() status = %d location = %s", w.Code, location)
			}
			if len(eventRepo.events) != 1 {
				t.Fatalf("stored %d events, want 1", len(eventRepo.events))
			}
			var uid string
			for _, created := range eventRepo.events {
				uid = created.UID
			}

			w = httptest.NewRecorder()
			handler.UpdateEvent(w, eventFormRequest(t, "/calendars/1/events/"+uid, uid, user, tt.zone, tt.edit))
			if location := w.Header().Get("Location"); w.Code != http.StatusFound || !strings.Contains(location, "status=event_updated") {
				t.Fatalf("UpdateEvent() status = %d location = %s", w.Code, location)
			}
			stored := eventRepo.events["1:"+uid]
			// The fake store assigns no ID, and the DAV server pages rows by it.
			stored.ID = 1

			var want []string
			for _, start := range tt.starts {
				wall, err := time.ParseInLocation("2006-01-02T15:04", start, loc)
				if err != nil {
					t.Fatalf("bad start %q: %v", start, err)
				}
				want = append(want, wall.UTC().Format("20060102T150405Z")+"/"+wall.Add(time.Hour).UTC().Format("20060102T150405Z"))
			}
			if got := davFreeBusyPeriods(t, s, user, tt.from, tt.to); !slices.Equal(got, want) {
				t.Errorf("CalDAV free-busy = %v, want %v, as the host's zone places the edited times\n%s", got, want, stored.RawICAL)
			}

			if tt.edit.Get("edit_scope") == "occurrence" {
				return
			}
			wantStart, _ := time.ParseInLocation("2006-01-02T15:04", tt.edit.Get("dtstart"), loc)
			if stored.WriteMetadata == nil || stored.WriteMetadata.DTStart == nil || !stored.WriteMetadata.DTStart.Equal(wantStart) {
				t.Errorf("stored dtstart = %+v, want %s", stored.WriteMetadata, wantStart.UTC())
			}
		})
	}
}

func eventFormRequest(t *testing.T, target, uid string, user *store.User, zone string, fields url.Values) *http.Request {
	t.Helper()
	form := url.Values{"summary": {"Meeting"}, "timezone": {zone}}
	for key, values := range fields {
		form[key] = values
	}
	req := httptest.NewRequest(http.MethodPost, target, strings.NewReader(form.Encode()))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	rctx := chi.NewRouteContext()
	rctx.URLParams.Add("id", "1")
	if uid != "" {
		rctx.URLParams.Add("uid", uid)
	}
	return req.WithContext(context.WithValue(auth.WithUser(req.Context(), user), chi.RouteCtxKey, rctx))
}

// davFreeBusyPeriods asks the CalDAV server for calendar 1's free-busy between
// from and to, and returns the periods it publishes in order.
func davFreeBusyPeriods(t *testing.T, s *store.Store, user *store.User, from, to string) []string {
	t.Helper()
	body := `<C:free-busy-query xmlns:C="urn:ietf:params:xml:ns:caldav"><C:time-range start="` + from + `" end="` + to + `"/></C:free-busy-query>`
	req := httptest.NewRequest("REPORT", "/dav/calendars/1/", strings.NewReader(body))
	req.Header.Set("Depth", "1")
	req = req.WithContext(auth.WithUser(req.Context(), user))
	w := httptest.NewRecorder()
	dav.NewDavServer(dav.Options{Config: &config.Config{}, Store: s}).ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("free-busy REPORT status = %d: %s", w.Code, w.Body.String())
	}
	var periods []string
	for _, line := range utils.UnfoldLines(w.Body.String()) {
		keyPart, value, found := strings.Cut(line, ":")
		if !found || !strings.HasPrefix(strings.ToUpper(keyPart), "FREEBUSY") {
			continue
		}
		periods = append(periods, strings.Split(value, ",")...)
	}
	slices.Sort(periods)
	return periods
}

// A definition a client wrote is the one CalDAV reads the object's times
// through (RFC 4791 §4.1), whether or not the host knows the zone, so an edit
// keeps it: the editor showed the times through it and reads the edited ones
// through it too.
func TestUpdateEventKeepsTheClientsDefinitions(t *testing.T) {
	const clientNewYork = "BEGIN:VTIMEZONE\r\nTZID:America/New_York\r\nX-LIC-LOCATION:America/New_York\r\n" +
		"BEGIN:STANDARD\r\nDTSTART:19701101T020000\r\nRRULE:FREQ=YEARLY;BYMONTH=11;BYDAY=1SU\r\nTZOFFSETFROM:-0400\r\nTZOFFSETTO:-0500\r\nEND:STANDARD\r\n" +
		"BEGIN:DAYLIGHT\r\nDTSTART:19700308T020000\r\nRRULE:FREQ=YEARLY;BYMONTH=3;BYDAY=2SU\r\nTZOFFSETFROM:-0500\r\nTZOFFSETTO:-0400\r\nEND:DAYLIGHT\r\nEND:VTIMEZONE\r\n"
	const clientZone = "BEGIN:VTIMEZONE\r\nTZID:Custom Eastern\r\n" +
		"BEGIN:STANDARD\r\nDTSTART:19701101T020000\r\nRRULE:FREQ=YEARLY;BYMONTH=11;BYDAY=1SU\r\nTZOFFSETFROM:-0400\r\nTZOFFSETTO:-0500\r\nEND:STANDARD\r\n" +
		"BEGIN:DAYLIGHT\r\nDTSTART:19700308T020000\r\nRRULE:FREQ=YEARLY;BYMONTH=3;BYDAY=2SU\r\nTZOFFSETFROM:-0500\r\nTZOFFSETTO:-0400\r\nEND:DAYLIGHT\r\nEND:VTIMEZONE\r\n"
	const original = "BEGIN:VCALENDAR\r\nVERSION:2.0\r\nPRODID:-//Client//EN\r\n" + clientNewYork + clientZone +
		"BEGIN:VEVENT\r\nUID:series\r\nDTSTAMP:20260101T000000Z\r\nSUMMARY:Series\r\n" +
		"DTSTART;TZID=America/New_York:20260615T100000\r\nDTEND;TZID=America/New_York:20260615T110000\r\nRRULE:FREQ=WEEKLY;COUNT=4\r\nEND:VEVENT\r\n" +
		"BEGIN:VEVENT\r\nUID:series\r\nDTSTAMP:20260101T000000Z\r\nSUMMARY:Moved\r\nRECURRENCE-ID;TZID=America/New_York:20260622T100000\r\n" +
		"DTSTART;TZID=Custom Eastern:20260622T120000\r\nDTEND;TZID=Custom Eastern:20260622T130000\r\nEND:VEVENT\r\n" +
		"END:VCALENDAR\r\n"
	if _, err := dav.ValidateCalendarObject(original, nil); err != nil {
		t.Fatalf("the stored series should pass the validator: %v", err)
	}
	calRepo := &fakeCalendarRepo{calendars: map[int64]*store.Calendar{1: {ID: 1, UserID: 100, Name: "Work"}}}
	eventRepo := &fakeEventRepoWithUpsert{fakeEventRepo: fakeEventRepo{events: map[string]*store.Event{
		"1:series": {ID: 1, CalendarID: 1, UID: "series", ResourceName: "series.ics", RawICAL: original, ETag: "e"},
	}}}
	handler := NewHandler(&config.Config{}, &store.Store{Calendars: calRepo, Events: eventRepo}, nil, dav.ValidateCalendarObject)
	user := &store.User{ID: 100}

	w := httptest.NewRecorder()
	handler.UpdateEvent(w, eventFormRequest(t, "/calendars/1/events/series", "series", user, "America/New_York", url.Values{
		"dtstart": {"2026-06-15T10:00"}, "dtend": {"2026-06-15T11:00"},
		"recurrence": {"WEEKLY"}, "recurrence_end_type": {"after"}, "recurrence_count": {"4"},
	}))
	if location := w.Header().Get("Location"); w.Code != http.StatusFound || !strings.Contains(location, "status=event_updated") {
		t.Fatalf("UpdateEvent() status = %d location = %s", w.Code, location)
	}
	stored := eventRepo.events["1:series"].RawICAL
	if !strings.Contains(stored, clientZone) {
		t.Errorf("the client's definition of a zone the host does not know was not kept:\n%s", stored)
	}
	if got := strings.Count(stored, "TZID:America/New_York\r\n"); got != 1 || !strings.Contains(stored, clientNewYork) {
		t.Errorf("stored %d definitions of America/New_York, want the client's alone:\n%s", got, stored)
	}
}

// outlookEastern is the definition Outlook ships with every event in its
// Windows zone "Eastern Standard Time", a name the host's zone database lacks.
const outlookEastern = "BEGIN:VTIMEZONE\r\nTZID:Eastern Standard Time\r\n" +
	"BEGIN:STANDARD\r\nDTSTART:16010101T020000\r\nTZOFFSETFROM:-0400\r\nTZOFFSETTO:-0500\r\nRRULE:FREQ=YEARLY;INTERVAL=1;BYDAY=1SU;BYMONTH=11\r\nEND:STANDARD\r\n" +
	"BEGIN:DAYLIGHT\r\nDTSTART:16010101T020000\r\nTZOFFSETFROM:-0500\r\nTZOFFSETTO:-0400\r\nRRULE:FREQ=YEARLY;INTERVAL=1;BYDAY=2SU;BYMONTH=3\r\nEND:DAYLIGHT\r\n" +
	"END:VTIMEZONE\r\n"

// An event in a zone only the object itself defines is placed by that
// definition everywhere. The view model gives the browser, whose time zone
// database lacks the zone, the offsets the definition keeps, and the instants
// CalDAV clients see; the editor shows and submits wall clocks read through
// those offsets, and the server reads them in the object's zone. So saving
// the event with its times untouched leaves every instance where it was and
// keeps the object's zone, and a changed time is read in that zone. The series
// starts in standard time and runs past the switch to daylight time on
// 10 March 2024, so a reading that ignores the definition's rules shows.
func TestEventInAZoneOnlyTheObjectDefinesKeepsItsInstants(t *testing.T) {
	const zone = "Eastern Standard Time"
	const original = "BEGIN:VCALENDAR\r\nPRODID:Microsoft Exchange Server 2010\r\nVERSION:2.0\r\n" + outlookEastern +
		"BEGIN:VEVENT\r\nUID:outlook-series\r\nDTSTAMP:20240101T000000Z\r\nSUMMARY:Standup\r\n" +
		"DTSTART;TZID=\"Eastern Standard Time\":20240304T090000\r\nDTEND;TZID=\"Eastern Standard Time\":20240304T100000\r\n" +
		"RRULE:FREQ=WEEKLY;UNTIL=20240325T130000Z;INTERVAL=1;BYDAY=MO;WKST=SU\r\nEND:VEVENT\r\n" +
		"BEGIN:VEVENT\r\nUID:outlook-series\r\nDTSTAMP:20240101T000000Z\r\nSUMMARY:Moved standup\r\n" +
		"RECURRENCE-ID;TZID=\"Eastern Standard Time\":20240311T090000\r\n" +
		"DTSTART;TZID=\"Eastern Standard Time\":20240311T110000\r\nDTEND;TZID=\"Eastern Standard Time\":20240311T120000\r\nEND:VEVENT\r\n" +
		"END:VCALENDAR\r\n"
	if _, ok := ical.HostLocation(zone); ok {
		t.Skipf("the host defines %q", zone)
	}
	validated, err := dav.ValidateCalendarObject(original, nil)
	if err != nil {
		t.Fatalf("the Outlook series should pass the validator: %v", err)
	}
	calRepo := &fakeCalendarRepo{calendars: map[int64]*store.Calendar{1: {ID: 1, UserID: 100, Name: "Work"}}}
	eventRepo := &fakeEventRepoWithUpsert{fakeEventRepo: fakeEventRepo{events: map[string]*store.Event{
		"1:outlook-series": {ID: 1, CalendarID: 1, UID: "outlook-series", ResourceName: "outlook-series.ics", RawICAL: validated.Body, ETag: "e"},
	}}}
	s := &store.Store{Calendars: calRepo, Events: eventRepo}
	handler := NewHandler(&config.Config{}, s, nil, dav.ValidateCalendarObject)
	user := &store.User{ID: 100}
	stored := func() *store.Event {
		event := eventRepo.events["1:outlook-series"]
		// The fake store assigns no ID, and the DAV server pages rows by it.
		event.ID = 1
		return event
	}
	freeBusy := func() []string {
		stored()
		return davFreeBusyPeriods(t, s, user, "20240301T000000Z", "20240401T000000Z")
	}
	wantUnchanged := []string{
		"20240304T140000Z/20240304T150000Z", // 09:00 EST
		"20240311T150000Z/20240311T160000Z", // the override, 11:00 EDT
		"20240318T130000Z/20240318T140000Z", // 09:00 EDT
		"20240325T130000Z/20240325T140000Z",
	}
	if got := freeBusy(); !slices.Equal(got, wantUnchanged) {
		t.Fatalf("CalDAV free-busy of the stored series = %v, want %v", got, wantUnchanged)
	}

	view := march2024ViewModel(t, handler, user)
	if len(view.Overrides) != 1 {
		t.Fatalf("view model overrides = %+v, want one", view.Overrides)
	}
	for _, check := range []struct{ field, got, want string }{
		{"dtstart", view.DTStart, "2024-03-04T14:00:00Z"},
		{"dtend", view.DTEnd, "2024-03-04T15:00:00Z"},
		{"timezone", view.Timezone, zone},
		{"override recurrenceId", view.Overrides[0].RecurrenceID, "2024-03-11T13:00:00Z"},
		{"override dtstart", view.Overrides[0].DTStart, "2024-03-11T15:00:00Z"},
		{"override recurrenceTimezone", view.Overrides[0].RecurrenceTimezone, zone},
	} {
		if check.got != check.want {
			t.Errorf("view model %s = %s, want %s", check.field, check.got, check.want)
		}
	}
	if _, ok := view.TimezoneOffsets[zone]; !ok {
		t.Fatalf("view model gives no offsets for %q: %+v", zone, view.TimezoneOffsets)
	}

	update := func(fields url.Values) {
		t.Helper()
		w := httptest.NewRecorder()
		handler.UpdateEvent(w, eventFormRequest(t, "/calendars/1/events/outlook-series", "outlook-series", user, zone, fields))
		if location := w.Header().Get("Location"); w.Code != http.StatusFound || !strings.Contains(location, "status=event_updated") {
			t.Fatalf("UpdateEvent(%v) status = %d location = %s", fields, w.Code, location)
		}
	}
	// What the series editor shows, and submits unchanged.
	seriesForm := func(view calendarEventView) url.Values {
		return url.Values{
			"dtstart": {view.editorWallClock(t, view.DTStart, zone)}, "dtend": {view.editorWallClock(t, view.DTEnd, zone)},
			"recurrence": {"WEEKLY"}, "recurrence_byday": {"MO"},
			"recurrence_end_type": {"on"}, "recurrence_until": {view.editorUntilDay(t, zone)},
		}
	}
	series := seriesForm(view)
	if want := (url.Values{
		"dtstart": {"2024-03-04T09:00"}, "dtend": {"2024-03-04T10:00"},
		"recurrence": {"WEEKLY"}, "recurrence_byday": {"MO"},
		"recurrence_end_type": {"on"}, "recurrence_until": {"2024-03-25"},
	}); !maps.EqualFunc(series, want, slices.Equal) {
		t.Fatalf("the editor shows %v, want the zone's own wall clocks %v", series, want)
	}

	update(series)
	if got := freeBusy(); !slices.Equal(got, wantUnchanged) {
		t.Errorf("after saving the series unchanged, CalDAV free-busy = %v, want %v\n%s", got, wantUnchanged, stored().RawICAL)
	}
	raw := stored().RawICAL
	if !strings.Contains(raw, outlookEastern) {
		t.Errorf("the object's definition of %q was not kept:\n%s", zone, raw)
	}
	master := ical.PrimaryVEventComponent(raw)
	for _, name := range []string{"DTSTART", "DTEND"} {
		property, _ := ical.ComponentProperty(master, name)
		if tzid, _ := ical.PropertyParam(property.KeyPart, "TZID"); tzid != zone {
			t.Errorf("%s = %s:%s, want it written in %q", name, property.KeyPart, property.Value, zone)
		}
	}
	view = march2024ViewModel(t, handler, user)
	if again := seriesForm(view); !maps.EqualFunc(again, series, slices.Equal) {
		t.Errorf("after saving, the editor shows %v, want %v as before", again, series)
	}

	// The 18 March occurrence, which the browser places at 13:00Z by the
	// offsets it was given; the occurrence editor shows its wall clock.
	occurrenceAt := "2024-03-18T13:00:00Z"
	occurrenceWall := view.editorWallClock(t, occurrenceAt, zone)
	occurrenceEnd := view.editorWallClock(t, "2024-03-18T14:00:00Z", zone)
	update(url.Values{
		"dtstart": {occurrenceWall}, "dtend": {occurrenceEnd}, "summary": {"Renamed standup"},
		"edit_scope": {"occurrence"}, "recurrence_id": {occurrenceWall}, "recurrence_timezone": {zone},
	})
	if got := freeBusy(); !slices.Equal(got, wantUnchanged) {
		t.Errorf("after renaming one occurrence, CalDAV free-busy = %v, want %v\n%s", got, wantUnchanged, stored().RawICAL)
	}

	w := httptest.NewRecorder()
	handler.DeleteEvent(w, deleteOccurrenceRequest(t, "outlook-series", url.Values{
		"recurrence_id": {view.editorWallClock(t, "2024-03-25T13:00:00Z", zone)}, "recurrence_timezone": {zone},
	}))
	if location := w.Header().Get("Location"); w.Code != http.StatusFound || !strings.Contains(location, "status=occurrence_deleted") {
		t.Fatalf("DeleteEvent() status = %d location = %s", w.Code, location)
	}
	if got, want := freeBusy(), wantUnchanged[:3]; !slices.Equal(got, want) {
		t.Errorf("after deleting the last occurrence, CalDAV free-busy = %v, want %v\n%s", got, want, stored().RawICAL)
	}

	moved := maps.Clone(series)
	moved["dtstart"], moved["dtend"] = []string{"2024-03-04T11:00"}, []string{"2024-03-04T12:00"}
	update(moved)
	want := []string{"20240304T160000Z/20240304T170000Z"} // 11:00 EST
	stored()
	if got := davFreeBusyPeriods(t, s, user, "20240304T000000Z", "20240305T000000Z"); !slices.Equal(got, want) {
		t.Errorf("after moving the series to 11:00, CalDAV free-busy on its first day = %v, want %v\n%s", got, want, stored().RawICAL)
	}
}

// calendarEventView is the part of an event's view model the editor reads.
type calendarEventView struct {
	DTStart            string `json:"dtstart"`
	DTEnd              string `json:"dtend"`
	Timezone           string `json:"timezone"`
	RecurrenceID       string `json:"recurrenceId"`
	RecurrenceTimezone string `json:"recurrenceTimezone"`
	RRuleParsed        map[string]string
	TimezoneOffsets    map[string][]calendarTimezoneOffset `json:"timezoneOffsets"`
	Overrides          []calendarEventView                 `json:"overrides"`
}

// march2024ViewModel is the view model the calendar page loads for calendar 1
// in March 2024, which holds one event.
func march2024ViewModel(t *testing.T, handler *Handler, user *store.User) calendarEventView {
	t.Helper()
	return calendarViewModel(t, handler, user, "2024-03-01", "2024-04-01")
}

// calendarViewModel is the view model the calendar page loads for calendar 1
// from start through the day before end, which holds one event.
func calendarViewModel(t *testing.T, handler *Handler, user *store.User, start, end string) calendarEventView {
	t.Helper()
	req := httptest.NewRequest(http.MethodGet, "/calendars/1/events.json?start="+start+"&end="+end, nil)
	rctx := chi.NewRouteContext()
	rctx.URLParams.Add("id", "1")
	req = req.WithContext(context.WithValue(auth.WithUser(req.Context(), user), chi.RouteCtxKey, rctx))
	w := httptest.NewRecorder()
	handler.GetCalendarEventsJSON(w, req)
	var events []calendarEventView
	if err := json.Unmarshal(w.Body.Bytes(), &events); err != nil || len(events) != 1 {
		t.Fatalf("events JSON = %d %s (%v), want one event", w.Code, w.Body.String(), err)
	}
	return events[0]
}

// editorWallClock is the wall clock the editor shows for an instant in zone,
// read through the offsets the view model gives for it, as the editor's
// datetime-local inputs hold it.
func (v calendarEventView) editorWallClock(t *testing.T, instant, zone string) string {
	t.Helper()
	parsed, err := time.Parse(time.RFC3339, instant)
	if err != nil {
		t.Fatal(err)
	}
	wall := parsed.Add(v.offsetAt(t, parsed, zone))
	if wall.Second() != 0 {
		return wall.Format("2006-01-02T15:04:05")
	}
	return wall.Format("2006-01-02T15:04")
}

// editorUntilDay is the day the editor shows a series ending on: the date of
// its UNTIL's wall clock in zone.
func (v calendarEventView) editorUntilDay(t *testing.T, zone string) string {
	t.Helper()
	until, err := time.Parse("20060102T150405Z", v.RRuleParsed["UNTIL"])
	if err != nil {
		t.Fatalf("UNTIL %q: %v", v.RRuleParsed["UNTIL"], err)
	}
	return until.Add(v.offsetAt(t, until, zone)).Format("2006-01-02")
}

// offsetAt is the offset the view model's listing for zone gives instant: the
// last entry starting at or before it, or the first.
func (v calendarEventView) offsetAt(t *testing.T, instant time.Time, zone string) time.Duration {
	t.Helper()
	listing := v.TimezoneOffsets[zone]
	if len(listing) == 0 {
		t.Fatalf("view model gives no offsets for %q", zone)
	}
	offset := listing[0].Offset
	for _, entry := range listing {
		from, err := time.Parse(time.RFC3339, entry.From)
		if err != nil {
			t.Fatal(err)
		}
		if !from.After(instant) {
			offset = entry.Offset
		}
	}
	return time.Duration(offset) * time.Second
}

// New York skips 02:00-03:00 on 10 March 2024. RFC 5545 §3.3.5 reads a wall
// clock in that gap with the offset before it, so 02:30 names 03:30 EDT, the
// instant 03:30 names: the editor writes the times as entered, and the event's
// end is checked against its start as the DAV layer reads both.
func TestEventInASkippedLocalTimeKeepsTheEnteredWallClock(t *testing.T) {
	const zone = "America/New_York"
	if _, err := time.LoadLocation(zone); err != nil {
		t.Skipf("%s is not available: %v", zone, err)
	}
	calRepo := &fakeCalendarRepo{calendars: map[int64]*store.Calendar{1: {ID: 1, UserID: 100, Name: "Work"}}}
	eventRepo := &fakeEventRepoWithUpsert{fakeEventRepo: fakeEventRepo{events: make(map[string]*store.Event)}}
	handler := NewHandler(&config.Config{}, &store.Store{Calendars: calRepo, Events: eventRepo}, nil, dav.ValidateCalendarObject)
	user := &store.User{ID: 100}

	w := httptest.NewRecorder()
	handler.CreateEvent(w, eventFormRequest(t, "/calendars/1/events", "", user, zone, url.Values{
		"dtstart": {"2024-03-10T02:30"}, "dtend": {"2024-03-10T03:15"},
	}))
	if location := w.Header().Get("Location"); w.Code != http.StatusFound || !strings.Contains(location, "error=end+date+must+not+be+before+start+date") {
		t.Fatalf("CreateEvent() of an event ending at 03:15 EDT, before 02:30 read as 03:30 EDT: status = %d location = %s", w.Code, location)
	}

	w = httptest.NewRecorder()
	handler.CreateEvent(w, eventFormRequest(t, "/calendars/1/events", "", user, zone, url.Values{
		"dtstart": {"2024-03-10T02:30"}, "dtend": {"2024-03-10T03:30"},
	}))
	if location := w.Header().Get("Location"); w.Code != http.StatusFound || !strings.Contains(location, "status=event_created") {
		t.Fatalf("CreateEvent() status = %d location = %s", w.Code, location)
	}
	if len(eventRepo.events) != 1 {
		t.Fatalf("stored %d events, want 1", len(eventRepo.events))
	}
	var stored *store.Event
	for _, event := range eventRepo.events {
		stored = event
	}
	for _, want := range []string{"DTSTART;TZID=America/New_York:20240310T023000\r\n", "DTEND;TZID=America/New_York:20240310T033000\r\n"} {
		if !strings.Contains(stored.RawICAL, want) {
			t.Errorf("stored event lacks %q:\n%s", want, stored.RawICAL)
		}
	}
	want := time.Date(2024, time.March, 10, 7, 30, 0, 0, time.UTC)
	metadata := stored.WriteMetadata
	if metadata == nil || metadata.DTStart == nil || metadata.DTEnd == nil || !metadata.DTStart.Equal(want) || !metadata.DTEnd.Equal(want) {
		t.Errorf("the DAV layer reads the stored times as %+v, want both at %s", metadata, want)
	}
	payload := unfoldedEventJSON(unfoldedEvent{event: *stored, lines: utils.UnfoldLines(stored.RawICAL)}, time.Time{}, time.Time{}, nil)
	if payload["dtstart"] != "2024-03-10T07:30:00Z" || payload["dtend"] != "2024-03-10T07:30:00Z" {
		t.Errorf("view model dtstart = %v dtend = %v, want both at %s", payload["dtstart"], payload["dtend"], want.Format(time.RFC3339))
	}
}

// The editor writes no definition of the zone an event's times are in; the
// validator storing the event adds the host's, spanning every year the series
// runs through, and one with no end for as long as the zone keeps its rules,
// so a client reading the object by that definition places every instance
// where the host's zone does.
func TestCreatedEventDefinesItsZoneForEveryInstance(t *testing.T) {
	const zone = "America/New_York"
	loc, err := time.LoadLocation(zone)
	if err != nil {
		t.Skipf("%s is not available: %v", zone, err)
	}
	tests := []struct {
		name       string
		recurrence url.Values
		// walls are wall clocks of instances, the last of them in the
		// series' last year.
		walls []string
	}{
		{name: "one-off", walls: []string{"20250115T140000"}},
		{
			name:       "until",
			recurrence: url.Values{"recurrence": {"WEEKLY"}, "recurrence_end_type": {"on"}, "recurrence_until": {"2026-12-31"}},
			walls:      []string{"20250115T140000", "20250716T140000", "20261230T140000"},
		},
		{
			name:       "count",
			recurrence: url.Values{"recurrence": {"WEEKLY"}, "recurrence_end_type": {"after"}, "recurrence_count": {"200"}},
			walls:      []string{"20250115T140000", "20280712T140000"},
		},
		{
			name:       "no end",
			recurrence: url.Values{"recurrence": {"WEEKLY"}},
			walls:      []string{"20250115T140000", "20300717T140000", "20451213T140000"},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			calRepo := &fakeCalendarRepo{calendars: map[int64]*store.Calendar{1: {ID: 1, UserID: 100, Name: "Work"}}}
			eventRepo := &fakeEventRepoWithUpsert{fakeEventRepo: fakeEventRepo{events: make(map[string]*store.Event)}}
			handler := NewHandler(&config.Config{}, &store.Store{Calendars: calRepo, Events: eventRepo}, nil, dav.ValidateCalendarObject)
			fields := url.Values{"dtstart": {"2025-01-15T14:00"}, "dtend": {"2025-01-15T15:00"}}
			maps.Copy(fields, tt.recurrence)

			w := httptest.NewRecorder()
			handler.CreateEvent(w, eventFormRequest(t, "/calendars/1/events", "", &store.User{ID: 100}, zone, fields))
			if location := w.Header().Get("Location"); w.Code != http.StatusFound || !strings.Contains(location, "status=event_created") {
				t.Fatalf("CreateEvent() status = %d location = %s", w.Code, location)
			}
			var raw string
			for _, event := range eventRepo.events {
				raw = event.RawICAL
			}
			if got := strings.Count(raw, "BEGIN:VTIMEZONE"); got != 1 || !strings.Contains(raw, "TZID:"+zone+"\r\n") {
				t.Fatalf("stored %d definitions, want the host's of %s alone:\n%s", got, zone, raw)
			}
			// A client knows nothing of the host: it reads the definition.
			client := ical.ResolveObjectTimezone(utils.UnfoldLines(strings.Replace(raw, ical.HostDefinitionProperty+":TRUE\r\n", "", 1)), zone)
			for _, value := range tt.walls {
				wall, err := ical.ParseDateTime(value)
				if err != nil {
					t.Fatal(err)
				}
				if got, want := client.Instant(wall), ical.WallClockIn(loc, wall); !got.Equal(want) {
					t.Errorf("the stored definition places %s at %s, the host's zone at %s\n%s", value, got.UTC(), want.UTC(), raw)
				}
			}
		})
	}
}

// clientSaoPaulo is a client's definition of America/Sao_Paulo from before
// Brazil abolished daylight time in 2019: it still keeps -02 from the third
// Sunday of October to the third Sunday of February, where the host's zone
// keeps -03 all year.
const clientSaoPaulo = "BEGIN:VTIMEZONE\r\nTZID:America/Sao_Paulo\r\n" +
	"BEGIN:DAYLIGHT\r\nDTSTART:20081019T000000\r\nRRULE:FREQ=YEARLY;BYMONTH=10;BYDAY=3SU\r\nTZOFFSETFROM:-0300\r\nTZOFFSETTO:-0200\r\nTZNAME:-02\r\nEND:DAYLIGHT\r\n" +
	"BEGIN:STANDARD\r\nDTSTART:20090215T000000\r\nRRULE:FREQ=YEARLY;BYMONTH=2;BYDAY=3SU\r\nTZOFFSETFROM:-0200\r\nTZOFFSETTO:-0300\r\nTZNAME:-03\r\nEND:STANDARD\r\n" +
	"END:VTIMEZONE\r\n"

// Editing one occurrence of a series in a zone the host knows, whose object
// carries the client's own definition of it, leaves the series where that
// definition places it: the master, the client's override and the edited
// occurrence alike.
func TestOccurrenceEditKeepsTheClientsDefinitionOfAHostZone(t *testing.T) {
	const zone = "America/Sao_Paulo"
	loc, ok := ical.HostLocation(zone)
	if !ok {
		t.Skipf("the host does not define %s", zone)
	}
	if _, offset := time.Date(2026, time.January, 5, 10, 0, 0, 0, loc).Zone(); offset != -3*60*60 {
		t.Skipf("the host's %s keeps %d in January 2026, not -03", zone, offset)
	}
	const original = "BEGIN:VCALENDAR\r\nVERSION:2.0\r\nPRODID:-//Client//EN\r\n" + clientSaoPaulo +
		"BEGIN:VEVENT\r\nUID:sp-series\r\nDTSTAMP:20251201T000000Z\r\nSUMMARY:Weekly\r\n" +
		"DTSTART;TZID=America/Sao_Paulo:20260105T100000\r\nDTEND;TZID=America/Sao_Paulo:20260105T110000\r\n" +
		"RRULE:FREQ=WEEKLY;COUNT=4\r\nEND:VEVENT\r\n" +
		"BEGIN:VEVENT\r\nUID:sp-series\r\nDTSTAMP:20251201T000000Z\r\nSUMMARY:Moved\r\n" +
		"RECURRENCE-ID;TZID=America/Sao_Paulo:20260112T100000\r\n" +
		"DTSTART;TZID=America/Sao_Paulo:20260112T140000\r\nDTEND;TZID=America/Sao_Paulo:20260112T150000\r\nEND:VEVENT\r\n" +
		"END:VCALENDAR\r\n"
	validated, err := dav.ValidateCalendarObject(original, nil)
	if err != nil {
		t.Fatalf("the client's series should pass the validator: %v", err)
	}
	calRepo := &fakeCalendarRepo{calendars: map[int64]*store.Calendar{1: {ID: 1, UserID: 100, Name: "Work"}}}
	eventRepo := &fakeEventRepoWithUpsert{fakeEventRepo: fakeEventRepo{events: map[string]*store.Event{
		"1:sp-series": {ID: 1, CalendarID: 1, UID: "sp-series", ResourceName: "sp-series.ics", RawICAL: validated.Body, ETag: "e"},
	}}}
	s := &store.Store{Calendars: calRepo, Events: eventRepo}
	handler := NewHandler(&config.Config{}, s, nil, dav.ValidateCalendarObject)
	user := &store.User{ID: 100}
	freeBusy := func() []string {
		eventRepo.events["1:sp-series"].ID = 1
		return davFreeBusyPeriods(t, s, user, "20260101T000000Z", "20260201T000000Z")
	}
	// 10:00 and the override's 14:00 at -02, as the client's definition reads them.
	want := []string{
		"20260105T120000Z/20260105T130000Z",
		"20260112T160000Z/20260112T170000Z",
		"20260119T120000Z/20260119T130000Z",
		"20260126T120000Z/20260126T130000Z",
	}
	if got := freeBusy(); !slices.Equal(got, want) {
		t.Fatalf("CalDAV free-busy of the stored series = %v, want %v", got, want)
	}

	view := calendarViewModel(t, handler, user, "2026-01-01", "2026-02-01")
	occurrence := view.editorWallClock(t, "2026-01-19T12:00:00Z", zone)
	if occurrence != "2026-01-19T10:00" {
		t.Fatalf("the editor shows the 19 January occurrence at %s, want 2026-01-19T10:00", occurrence)
	}
	w := httptest.NewRecorder()
	handler.UpdateEvent(w, eventFormRequest(t, "/calendars/1/events/sp-series", "sp-series", user, zone, url.Values{
		"summary": {"Renamed"}, "dtstart": {occurrence}, "dtend": {view.editorWallClock(t, "2026-01-19T13:00:00Z", zone)},
		"edit_scope": {"occurrence"}, "recurrence_id": {occurrence}, "recurrence_timezone": {zone},
	}))
	if location := w.Header().Get("Location"); w.Code != http.StatusFound || !strings.Contains(location, "status=event_updated") {
		t.Fatalf("UpdateEvent() status = %d location = %s", w.Code, location)
	}
	stored := eventRepo.events["1:sp-series"].RawICAL
	if got := freeBusy(); !slices.Equal(got, want) {
		t.Errorf("after renaming one occurrence, CalDAV free-busy = %v, want %v\n%s", got, want, stored)
	}
	if !strings.Contains(stored, clientSaoPaulo) || strings.Contains(stored, ical.HostDefinitionProperty) {
		t.Errorf("the client's definition of %s was not kept as the object's only one:\n%s", zone, stored)
	}
}

// legacyNewYork2026 is the definition the v1.1 editor generated for an event
// in America/New_York in 2026: the host's transitions in that year alone,
// after a marker restating the offset kept on 1 January, and unmarked.
const legacyNewYork2026 = "BEGIN:VTIMEZONE\r\nTZID:America/New_York\r\n" +
	"BEGIN:STANDARD\r\nDTSTART:20260101T000000\r\nTZOFFSETFROM:-0500\r\nTZOFFSETTO:-0500\r\nTZNAME:EST\r\nEND:STANDARD\r\n" +
	"BEGIN:DAYLIGHT\r\nDTSTART:20260308T020000\r\nTZOFFSETFROM:-0500\r\nTZOFFSETTO:-0400\r\nTZNAME:EDT\r\nEND:DAYLIGHT\r\n" +
	"BEGIN:STANDARD\r\nDTSTART:20261101T020000\r\nTZOFFSETFROM:-0400\r\nTZOFFSETTO:-0500\r\nTZNAME:EST\r\nEND:STANDARD\r\n" +
	"END:VTIMEZONE\r\n"

// A definition of the v1.1 editor's shape defines the host's zone for one year
// alone, and reads every later year as the standard time it ends in. An edit leaves the zone to the host, whose definition the validator
// stores, and reads the edited times -- the series' end included -- as the
// host's zone does. The series ends on 10 July 2027 at midnight; read in the
// generated definition, that midnight would be an hour later, and the series
// would take in an eleventh instance at 00:30 on 11 July.
func TestEditReplacesADefinitionTheV11EditorGenerated(t *testing.T) {
	const zone = "America/New_York"
	loc, err := time.LoadLocation(zone)
	if err != nil {
		t.Skipf("%s is not available: %v", zone, err)
	}
	const original = "BEGIN:VCALENDAR\r\nVERSION:2.0\r\nPRODID:-//CalCard//EN\r\n" + legacyNewYork2026 +
		"BEGIN:VEVENT\r\nUID:legacy\r\nDTSTAMP:20260101T000000Z\r\n" +
		"DTSTART;TZID=America/New_York:20260615T003000\r\nDTEND;TZID=America/New_York:20260615T013000\r\n" +
		"SUMMARY:Night shift\r\nEND:VEVENT\r\nEND:VCALENDAR\r\n"
	if _, err := dav.ValidateCalendarObject(original, nil); err != nil {
		t.Fatalf("the v1.1 event should pass the validator: %v", err)
	}
	calRepo := &fakeCalendarRepo{calendars: map[int64]*store.Calendar{1: {ID: 1, UserID: 100, Name: "Work"}}}
	eventRepo := &fakeEventRepoWithUpsert{fakeEventRepo: fakeEventRepo{events: map[string]*store.Event{
		"1:legacy": {ID: 1, CalendarID: 1, UID: "legacy", ResourceName: "legacy.ics", RawICAL: original, ETag: "e"},
	}}}
	s := &store.Store{Calendars: calRepo, Events: eventRepo}
	handler := NewHandler(&config.Config{}, s, nil, dav.ValidateCalendarObject)
	user := &store.User{ID: 100}

	w := httptest.NewRecorder()
	handler.UpdateEvent(w, eventFormRequest(t, "/calendars/1/events/legacy", "legacy", user, zone, url.Values{
		"summary": {"Night shift"}, "dtstart": {"2027-07-01T00:30"}, "dtend": {"2027-07-01T01:30"},
		"recurrence": {"DAILY"}, "recurrence_end_type": {"on"}, "recurrence_until": {"2027-07-10"},
	}))
	if location := w.Header().Get("Location"); w.Code != http.StatusFound || !strings.Contains(location, "status=event_updated") {
		t.Fatalf("UpdateEvent() status = %d location = %s", w.Code, location)
	}
	stored := eventRepo.events["1:legacy"]
	stored.ID = 1
	if strings.Contains(stored.RawICAL, legacyNewYork2026) || !strings.Contains(stored.RawICAL, ical.HostDefinitionProperty) {
		t.Errorf("the v1.1 definition was not replaced by the host's:\n%s", stored.RawICAL)
	}
	var walls []string
	for day := 1; day <= 10; day++ {
		walls = append(walls, time.Date(2027, time.July, day, 0, 30, 0, 0, time.UTC).Format("2006-01-02T15:04"))
	}
	if got, want := davFreeBusyPeriods(t, s, user, "20270601T000000Z", "20270801T000000Z"), hourPeriods(t, loc, walls...); !slices.Equal(got, want) {
		t.Errorf("CalDAV free-busy = %v, want %v\n%s", got, want, stored.RawICAL)
	}
}

func TestV11GeneratedDefinitionsAreRecognized(t *testing.T) {
	tests := []struct {
		name       string
		definition string
		want       bool
	}{
		{"one year with transitions", legacyNewYork2026, true},
		{
			"two years",
			"BEGIN:VTIMEZONE\r\nTZID:America/New_York\r\n" +
				"BEGIN:STANDARD\r\nDTSTART:20260101T000000\r\nTZOFFSETFROM:-0500\r\nTZOFFSETTO:-0500\r\nTZNAME:EST\r\nEND:STANDARD\r\n" +
				"BEGIN:DAYLIGHT\r\nDTSTART:20260308T020000\r\nTZOFFSETFROM:-0500\r\nTZOFFSETTO:-0400\r\nTZNAME:EDT\r\nEND:DAYLIGHT\r\n" +
				"BEGIN:STANDARD\r\nDTSTART:20261101T020000\r\nTZOFFSETFROM:-0400\r\nTZOFFSETTO:-0500\r\nTZNAME:EST\r\nEND:STANDARD\r\n" +
				"BEGIN:STANDARD\r\nDTSTART:20270101T000000\r\nTZOFFSETFROM:-0500\r\nTZOFFSETTO:-0500\r\nTZNAME:EST\r\nEND:STANDARD\r\n" +
				"BEGIN:DAYLIGHT\r\nDTSTART:20270314T020000\r\nTZOFFSETFROM:-0500\r\nTZOFFSETTO:-0400\r\nTZNAME:EDT\r\nEND:DAYLIGHT\r\n" +
				"BEGIN:STANDARD\r\nDTSTART:20271107T020000\r\nTZOFFSETFROM:-0400\r\nTZOFFSETTO:-0500\r\nTZNAME:EST\r\nEND:STANDARD\r\n" +
				"END:VTIMEZONE\r\n",
			true,
		},
		{
			"southern hemisphere, starting in daylight time",
			"BEGIN:VTIMEZONE\r\nTZID:America/Sao_Paulo\r\n" +
				"BEGIN:DAYLIGHT\r\nDTSTART:20180101T000000\r\nTZOFFSETFROM:-0200\r\nTZOFFSETTO:-0200\r\nTZNAME:-02\r\nEND:DAYLIGHT\r\n" +
				"BEGIN:STANDARD\r\nDTSTART:20180218T000000\r\nTZOFFSETFROM:-0200\r\nTZOFFSETTO:-0300\r\nTZNAME:-03\r\nEND:STANDARD\r\n" +
				"BEGIN:DAYLIGHT\r\nDTSTART:20181104T000000\r\nTZOFFSETFROM:-0300\r\nTZOFFSETTO:-0200\r\nTZNAME:-02\r\nEND:DAYLIGHT\r\n" +
				"END:VTIMEZONE\r\n",
			true,
		},
		{
			"a zone without transitions",
			"BEGIN:VTIMEZONE\r\nTZID:Asia/Tokyo\r\nBEGIN:STANDARD\r\nDTSTART:20260101T000000\r\nTZOFFSETFROM:+0900\r\nTZOFFSETTO:+0900\r\nTZNAME:JST\r\nEND:STANDARD\r\nEND:VTIMEZONE\r\n",
			true,
		},
		{
			"the zone's TZID after its observances",
			strings.Replace(strings.Replace(legacyNewYork2026, "TZID:America/New_York\r\n", "", 1), "END:VTIMEZONE", "TZID:America/New_York\r\nEND:VTIMEZONE", 1),
			true,
		},
		{
			"a fixed offset in the conventional form",
			"BEGIN:VTIMEZONE\r\nTZID:Asia/Tokyo\r\nBEGIN:STANDARD\r\nDTSTART:19700101T000000\r\nTZOFFSETFROM:+0900\r\nTZOFFSETTO:+0900\r\nTZNAME:JST\r\nEND:STANDARD\r\nEND:VTIMEZONE\r\n",
			false,
		},
		{"a client's rules", clientSaoPaulo, false},
		{"Outlook's", outlookEastern, false},
		{
			"listed transitions without the 1 January marker",
			"BEGIN:VTIMEZONE\r\nTZID:America/New_York\r\n" +
				"BEGIN:DAYLIGHT\r\nDTSTART:20260308T020000\r\nTZOFFSETFROM:-0500\r\nTZOFFSETTO:-0400\r\nTZNAME:EDT\r\nEND:DAYLIGHT\r\n" +
				"BEGIN:STANDARD\r\nDTSTART:20261101T020000\r\nTZOFFSETFROM:-0400\r\nTZOFFSETTO:-0500\r\nTZNAME:EST\r\nEND:STANDARD\r\n" +
				"END:VTIMEZONE\r\n",
			false,
		},
		{
			"a year missing between markers",
			"BEGIN:VTIMEZONE\r\nTZID:Asia/Tokyo\r\n" +
				"BEGIN:STANDARD\r\nDTSTART:20260101T000000\r\nTZOFFSETFROM:+0900\r\nTZOFFSETTO:+0900\r\nTZNAME:JST\r\nEND:STANDARD\r\n" +
				"BEGIN:STANDARD\r\nDTSTART:20280101T000000\r\nTZOFFSETFROM:+0900\r\nTZOFFSETTO:+0900\r\nTZNAME:JST\r\nEND:STANDARD\r\n" +
				"END:VTIMEZONE\r\n",
			false,
		},
		{
			"another property on the zone",
			strings.Replace(legacyNewYork2026, "TZID:America/New_York\r\n", "TZID:America/New_York\r\nX-LIC-LOCATION:America/New_York\r\n", 1),
			false,
		},
		{
			"another property on an observance",
			strings.Replace(legacyNewYork2026, "TZNAME:EDT\r\n", "TZNAME:EDT\r\nCOMMENT:Summer\r\n", 1),
			false,
		},
		{
			"observances out of order",
			"BEGIN:VTIMEZONE\r\nTZID:America/New_York\r\n" +
				"BEGIN:STANDARD\r\nDTSTART:20260101T000000\r\nTZOFFSETFROM:-0500\r\nTZOFFSETTO:-0500\r\nTZNAME:EST\r\nEND:STANDARD\r\n" +
				"BEGIN:STANDARD\r\nDTSTART:20261101T020000\r\nTZOFFSETFROM:-0400\r\nTZOFFSETTO:-0500\r\nTZNAME:EST\r\nEND:STANDARD\r\n" +
				"BEGIN:DAYLIGHT\r\nDTSTART:20260308T020000\r\nTZOFFSETFROM:-0500\r\nTZOFFSETTO:-0400\r\nTZNAME:EDT\r\nEND:DAYLIGHT\r\n" +
				"END:VTIMEZONE\r\n",
			false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			lines := utils.UnfoldLines(strings.TrimSuffix(tt.definition, "\r\n"))
			if got := ical.V11GeneratedDefinition(lines); got != tt.want {
				t.Errorf("ical.V11GeneratedDefinition() = %v, want %v\n%s", got, tt.want, tt.definition)
			}
		})
	}
}

// The definitions an edit drops are the VTIMEZONEs beneath the object's
// VCALENDAR that the server wrote, each through the line that closes it as
// every reader of the object takes it -- an END:VTIMEZONE closing an
// observance left open closes the definition too -- wherever in the object it
// stands, and nothing else.
func TestEditDropsOnlyTheServersDefinitions(t *testing.T) {
	if _, err := time.LoadLocation("Europe/Berlin"); err != nil {
		t.Skipf("Europe/Berlin is not available: %v", err)
	}
	unclosed := []string{"BEGIN:VTIMEZONE", "TZID:Europe/Berlin", ical.HostDefinitionProperty + ":TRUE",
		"BEGIN:STANDARD", "DTSTART:19701025T030000", "TZOFFSETFROM:+0200", "TZOFFSETTO:+0100", "END:VTIMEZONE"}
	client := []string{"BEGIN:VTIMEZONE", "TZID:Custom/Zone",
		"BEGIN:STANDARD", "DTSTART:19700101T000000", "TZOFFSETFROM:+0300", "TZOFFSETTO:+0300", "END:STANDARD", "END:VTIMEZONE"}
	event := []string{"BEGIN:VEVENT", "UID:x", "DTSTART;TZID=Europe/Berlin:20260105T090000", "END:VEVENT"}
	join := func(parts ...[]string) []string { return slices.Concat(parts...) }
	head, tail := []string{"BEGIN:VCALENDAR", "VERSION:2.0"}, []string{"END:VCALENDAR"}

	tests := map[string]struct{ lines, want []string }{
		"observance left open": {join(head, unclosed, client, event, tail), join(head, client, event, tail)},
		"after the events":     {join(head, event, client, unclosed, tail), join(head, event, client, tail)},
	}
	for name, tt := range tests {
		t.Run(name, func(t *testing.T) {
			if got := withoutServerZoneDefinitions(tt.lines); !slices.Equal(got, tt.want) {
				t.Errorf("withoutServerZoneDefinitions() =\n%s\nwant\n%s", strings.Join(got, "\n"), strings.Join(tt.want, "\n"))
			}
		})
	}
}

// An edit splits the stored event's components as every reader of a stored
// object reads them (ical.ReadComponents): an alarm stored without its END is
// closed where the event ends, and an END naming no open
// component is skipped. The edit then writes balanced components, which the
// validator accepts, rather than carrying the defect into a write it refuses.
func TestEditSplitsAStoredEventAsItsReadersDo(t *testing.T) {
	for name, alarm := range map[string]string{
		"alarm without its END": "BEGIN:VALARM\r\nACTION:DISPLAY\r\nDESCRIPTION:Reminder\r\nTRIGGER:-PT15M\r\n",
		"END naming no open component": "BEGIN:VALARM\r\nACTION:DISPLAY\r\nDESCRIPTION:Reminder\r\nEND:VTODO\r\n" +
			"TRIGGER:-PT15M\r\nEND:VALARM\r\n",
	} {
		t.Run(name, func(t *testing.T) {
			stored := "BEGIN:VCALENDAR\r\nVERSION:2.0\r\nPRODID:-//Legacy//EN\r\n" +
				"BEGIN:VEVENT\r\nUID:legacy\r\nDTSTAMP:20260101T000000Z\r\nDTSTART:20260105T090000Z\r\nDTEND:20260105T100000Z\r\n" +
				"SUMMARY:Meeting\r\n" + alarm + "END:VEVENT\r\nEND:VCALENDAR\r\n"
			calRepo := &fakeCalendarRepo{calendars: map[int64]*store.Calendar{1: {ID: 1, UserID: 100, Name: "Work"}}}
			eventRepo := &fakeEventRepoWithUpsert{fakeEventRepo: fakeEventRepo{events: map[string]*store.Event{
				"1:legacy": {ID: 1, CalendarID: 1, UID: "legacy", ResourceName: "legacy.ics", RawICAL: stored, ETag: "e"},
			}}}
			handler := NewHandler(&config.Config{}, &store.Store{Calendars: calRepo, Events: eventRepo}, nil, dav.ValidateCalendarObject)

			w := httptest.NewRecorder()
			handler.UpdateEvent(w, eventFormRequest(t, "/calendars/1/events/legacy", "legacy", &store.User{ID: 100}, "UTC", url.Values{
				"summary": {"Moved"}, "dtstart": {"2026-01-06T09:00"}, "dtend": {"2026-01-06T10:00"},
			}))
			if location := w.Header().Get("Location"); w.Code != http.StatusFound || !strings.Contains(location, "status=event_updated") {
				t.Fatalf("UpdateEvent() status = %d location = %s", w.Code, location)
			}
			updated := eventRepo.events["1:legacy"].RawICAL
			if !strings.Contains(updated, "SUMMARY:Moved") || strings.Count(updated, "BEGIN:VALARM") != strings.Count(updated, "END:VALARM") ||
				strings.Contains(updated, "END:VTODO") {
				t.Errorf("stored\n%s\nwant the edit with balanced components", updated)
			}
		})
	}
}

// Deleting one occurrence stores the zone definitions an edit of that
// occurrence would: the definitions the server wrote, a stale one the v1.1
// editor left unmarked and a marked one for a zone the object no longer
// names, give way to the host's definition of each zone the object names, and
// the occurrence is read in the host's zone.
func TestDeleteOccurrenceStoresTheZoneDefinitionsAnEditWould(t *testing.T) {
	const zone = "America/New_York"
	if _, err := time.LoadLocation(zone); err != nil {
		t.Skipf("%s is not available: %v", zone, err)
	}
	staleBerlin, ok := ical.VTimezone("Europe/Berlin", time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC), time.Date(2024, 12, 31, 0, 0, 0, 0, time.UTC))
	if !ok {
		t.Skip("Europe/Berlin is not available")
	}
	legacy := strings.ReplaceAll(legacyNewYork2026, "2026", "2025")
	original := "BEGIN:VCALENDAR\r\nVERSION:2.0\r\nPRODID:-//CalCard//EN\r\n" + legacy + staleBerlin +
		"BEGIN:VEVENT\r\nUID:legacy\r\nDTSTAMP:20250701T000000Z\r\nSUMMARY:Weekly\r\n" +
		"DTSTART;TZID=America/New_York:20250706T100000\r\nDTEND;TZID=America/New_York:20250706T110000\r\n" +
		"RRULE:FREQ=WEEKLY\r\nEND:VEVENT\r\nEND:VCALENDAR\r\n"
	stored := func(write func(handler *Handler, w *httptest.ResponseRecorder)) string {
		t.Helper()
		calRepo := &fakeCalendarRepo{calendars: map[int64]*store.Calendar{1: {ID: 1, UserID: 100, Name: "Work"}}}
		eventRepo := &fakeEventRepoWithUpsert{fakeEventRepo: fakeEventRepo{events: map[string]*store.Event{
			"1:legacy": {ID: 1, CalendarID: 1, UID: "legacy", ResourceName: "legacy.ics", RawICAL: original, ETag: "e"},
		}}}
		w := httptest.NewRecorder()
		write(NewHandler(&config.Config{}, &store.Store{Calendars: calRepo, Events: eventRepo}, nil, dav.ValidateCalendarObject), w)
		if location := w.Header().Get("Location"); w.Code != http.StatusFound || !strings.Contains(location, "status=occurrence_") && !strings.Contains(location, "status=event_updated") {
			t.Fatalf("status = %d location = %s", w.Code, location)
		}
		return eventRepo.events["1:legacy"].RawICAL
	}
	definitions := func(raw string) []string {
		lines := utils.UnfoldLines(raw)
		var found []string
		for _, timezone := range ical.TimezoneComponents(lines) {
			found = append(found, strings.Join(lines[timezone.Start:timezone.End], "\r\n"))
		}
		return found
	}

	deleted := stored(func(handler *Handler, w *httptest.ResponseRecorder) {
		handler.DeleteEvent(w, deleteOccurrenceRequest(t, "legacy", url.Values{
			"recurrence_id": {"2026-07-05T10:00"}, "recurrence_timezone": {zone},
		}))
	})
	edited := stored(func(handler *Handler, w *httptest.ResponseRecorder) {
		handler.UpdateEvent(w, eventFormRequest(t, "/calendars/1/events/legacy", "legacy", &store.User{ID: 100}, zone, url.Values{
			"summary": {"Renamed"}, "dtstart": {"2026-07-05T10:00"}, "dtend": {"2026-07-05T11:00"},
			"edit_scope": {"occurrence"}, "recurrence_id": {"2026-07-05T10:00"}, "recurrence_timezone": {zone},
		}))
	})
	if got, want := definitions(deleted), definitions(edited); !slices.Equal(got, want) {
		t.Errorf("deleting the occurrence stored the definitions\n%s\nwant those an edit stores\n%s", strings.Join(got, "\n\n"), strings.Join(want, "\n\n"))
	}
	if strings.Contains(deleted, legacy) || strings.Contains(deleted, "TZID:Europe/Berlin") {
		t.Errorf("a stale definition the server wrote was kept:\n%s", deleted)
	}
	if !strings.Contains(deleted, "EXDATE;TZID=America/New_York:20260705T100000\r\n") {
		t.Errorf("the occurrence was not excluded at 10:00 in %s:\n%s", zone, deleted)
	}
}
