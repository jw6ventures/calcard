package dav

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/jw6ventures/calcard/internal/auth"
	"github.com/jw6ventures/calcard/internal/ical"
	"github.com/jw6ventures/calcard/internal/store"
)

// postgresCalendarServer is a server over a fresh PostgreSQL schema holding one
// user and the given number of calendars.
func postgresCalendarServer(t *testing.T, calendars int) (*DavServer, *store.Store, *store.User, []store.Calendar) {
	t.Helper()
	database := newDAVPostgresStore(t)
	user, err := database.Users.UpsertOAuthUser(t.Context(), "reader", "reader@example.test", "Reader", "Test")
	if err != nil {
		t.Fatalf("create user: %v", err)
	}
	created := make([]store.Calendar, 0, calendars)
	for i := range calendars {
		calendar, err := database.Calendars.Create(t.Context(), store.Calendar{UserID: user.ID, Name: fmt.Sprintf("Calendar %d", i)})
		if err != nil {
			t.Fatalf("create calendar: %v", err)
		}
		created = append(created, *calendar)
	}
	return NewDavServer(Options{Store: database}), database, user, created
}

func requestAsUser(h *DavServer, user *store.User, method, target, body string, headers map[string]string) *httptest.ResponseRecorder {
	req := httptest.NewRequest(method, target, strings.NewReader(body))
	if method == http.MethodPut {
		req.Header.Set("Content-Type", "text/calendar")
	}
	for name, value := range headers {
		req.Header.Set(name, value)
	}
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, req.WithContext(auth.WithUser(req.Context(), user)))
	return rr
}

// The SQL narrowing of a time-range read keeps the one-day event, and the
// stored end is the end of its day.
func TestPostgres_OneDayEventWithItsDateAsBothEndsOccupiesTheDay(t *testing.T) {
	h, database, user, calendars := postgresCalendarServer(t, 1)
	collection := fmt.Sprintf("/dav/calendars/%d/", calendars[0].ID)
	raw := producerObject("VEVENT", "UID:one-day", "DTSTAMP:20250101T000000Z", "DTSTART;VALUE=DATE:20250301", "DTEND;VALUE=DATE:20250301")
	if rr := requestAsUser(h, user, http.MethodPut, collection+"one-day.ics", raw, nil); rr.Code != http.StatusCreated {
		t.Fatalf("PUT = %d: %s", rr.Code, rr.Body.String())
	}
	stored, err := database.Events.GetByUID(t.Context(), calendars[0].ID, "one-day")
	if err != nil {
		t.Fatalf("read stored row: %v", err)
	}
	if want := time.Date(2025, 3, 2, 0, 0, 0, 0, time.UTC); stored.DTEnd == nil || !stored.DTEnd.Equal(want) {
		t.Errorf("stored dtend = %v, want %s", stored.DTEnd, want)
	}
	depth := map[string]string{"Depth": "1"}
	rr := requestAsUser(h, user, "REPORT", collection, timeRangeQuery(oneDayRangeStart, oneDayRangeEnd), depth)
	if rr.Code != http.StatusMultiStatus || !strings.Contains(rr.Body.String(), "one-day.ics") {
		t.Errorf("calendar-query [%s, %s) = %d without the event:\n%s", oneDayRangeStart, oneDayRangeEnd, rr.Code, rr.Body.String())
	}
	rr = requestAsUser(h, user, "REPORT", collection, freeBusyQuery(oneDayRangeStart, oneDayRangeEnd), depth)
	if rr.Code != http.StatusOK {
		t.Fatalf("free-busy = %d: %s", rr.Code, rr.Body.String())
	}
	assertPublishedFreeBusy(t, parsedFreeBusyLines(t, rr.Body.String()), []string{"FREEBUSY:20250301T000000Z/" + oneDayRangeEnd})
}

// An event read back with the definition the server stored for its zone,
// moved to a year that definition does not place as the host does and written
// back, is stored with a definition for its new year, and its stored dtstart,
// calendar-query and free-busy all agree with the host's zone.
func TestPostgres_HostDefinitionWrittenBackFollowsTheEventIntoAnotherYear(t *testing.T) {
	requireHostZones(t, "Africa/Casablanca")
	loc, _ := ical.HostLocation("Africa/Casablanca")
	to := wallClockWhereHostOffsetIs(t, loc, 2030, 0)
	hostInstant := ical.WallClockIn(loc, to).UTC()
	from := time.Date(2025, 6, 16, 9, 0, 0, 0, time.UTC)

	h, database, user, calendars := postgresCalendarServer(t, 1)
	collection := fmt.Sprintf("/dav/calendars/%d/", calendars[0].ID)
	raw := producerObject("VEVENT", "UID:moved", "DTSTAMP:20250101T000000Z",
		"DTSTART;TZID=Africa/Casablanca:"+wallClockValue(from), "DTEND;TZID=Africa/Casablanca:"+wallClockValue(from.Add(time.Hour)))
	if rr := requestAsUser(h, user, http.MethodPut, collection+"moved.ics", raw, nil); rr.Code != http.StatusCreated {
		t.Fatalf("first PUT = %d: %s", rr.Code, rr.Body.String())
	}
	got := requestAsUser(h, user, http.MethodGet, collection+"moved.ics", "", nil)
	if got.Code != http.StatusOK {
		t.Fatalf("GET = %d: %s", got.Code, got.Body.String())
	}
	written := strings.NewReplacer(wallClockValue(from), wallClockValue(to),
		wallClockValue(from.Add(time.Hour)), wallClockValue(to.Add(time.Hour))).Replace(got.Body.String())
	if stale := readsAt(t, clientReading(written)); stale.Equal(hostInstant) {
		t.Fatalf("the definition written back already places %s where the host does; the case tests nothing", to)
	}
	if rr := requestAsUser(h, user, http.MethodPut, collection+"moved.ics", written, map[string]string{"If-Match": got.Header().Get("ETag")}); rr.Code != http.StatusNoContent {
		t.Fatalf("second PUT = %d: %s", rr.Code, rr.Body.String())
	}

	stored, err := database.Events.GetByUID(t.Context(), calendars[0].ID, "moved")
	if err != nil {
		t.Fatalf("read stored row: %v", err)
	}
	if stored.DTStart == nil || !stored.DTStart.Equal(hostInstant) {
		t.Errorf("stored dtstart = %v, want the host's %s", stored.DTStart, hostInstant)
	}
	if at := readsAt(t, clientReading(stored.RawICAL)); !at.Equal(hostInstant) {
		t.Errorf("the stored definition places the event at %s for a client, want the host's %s", at, hostInstant)
	}
	start, end := hostInstant.Format("20060102T150405Z"), hostInstant.Add(time.Hour).Format("20060102T150405Z")
	depth := map[string]string{"Depth": "1"}
	rr := requestAsUser(h, user, "REPORT", collection, timeRangeQuery(start, end), depth)
	if rr.Code != http.StatusMultiStatus || !strings.Contains(rr.Body.String(), "moved.ics") {
		t.Errorf("calendar-query [%s, %s) = %d without the event:\n%s", start, end, rr.Code, rr.Body.String())
	}
	rr = requestAsUser(h, user, "REPORT", collection, freeBusyQuery(hostInstant.Add(-24*time.Hour).Format("20060102T150405Z"), hostInstant.Add(24*time.Hour).Format("20060102T150405Z")), depth)
	if rr.Code != http.StatusOK {
		t.Fatalf("free-busy = %d: %s", rr.Code, rr.Body.String())
	}
	assertPublishedFreeBusy(t, parsedFreeBusyLines(t, rr.Body.String()), []string{"FREEBUSY:" + start + "/" + end})
}

// A series whose UNTIL falls before its DTSTART still holds the DTSTART as its
// one instance (RFC 5545 §3.8.5.3), and the SQL narrowing of a time-range read
// keeps the object for a range around that instance, so calendar-query and
// free-busy both report it.
func TestPostgres_UntilBeforeDTStartStillReportsTheFirstInstance(t *testing.T) {
	h, _, user, calendars := postgresCalendarServer(t, 1)
	collection := fmt.Sprintf("/dav/calendars/%d/", calendars[0].ID)
	raw := producerObject("VEVENT", "UID:until-before-start", "DTSTAMP:20240101T000000Z",
		"DTSTART:20240110T100000Z", "DTEND:20240110T110000Z", "RRULE:FREQ=DAILY;UNTIL=20240101T000000Z")
	if rr := requestAsUser(h, user, http.MethodPut, collection+"until.ics", raw, nil); rr.Code != http.StatusCreated {
		t.Fatalf("PUT = %d: %s", rr.Code, rr.Body.String())
	}
	const start, end = "20240110T000000Z", "20240111T000000Z"
	depth := map[string]string{"Depth": "1"}
	rr := requestAsUser(h, user, "REPORT", collection, timeRangeQuery(start, end), depth)
	if rr.Code != http.StatusMultiStatus || !strings.Contains(rr.Body.String(), "until.ics") {
		t.Errorf("calendar-query [%s, %s) = %d without the event:\n%s", start, end, rr.Code, rr.Body.String())
	}
	rr = requestAsUser(h, user, "REPORT", collection, freeBusyQuery(start, end), depth)
	if rr.Code != http.StatusOK {
		t.Fatalf("free-busy = %d: %s", rr.Code, rr.Body.String())
	}
	assertPublishedFreeBusy(t, parsedFreeBusyLines(t, rr.Body.String()), []string{"FREEBUSY:20240110T100000Z/20240110T110000Z"})
}

// PostgreSQL stores the vertical tabs and form feeds Outlook writes, and the
// object carrying them is copied and moved as stored.
func TestPostgres_ControlCharactersSurviveCopyAndMove(t *testing.T) {
	h, _, user, calendars := postgresCalendarServer(t, 3)
	path := func(calendar int) string {
		return fmt.Sprintf("/dav/calendars/%d/outlook-controls.ics", calendars[calendar].ID)
	}
	if rr := requestAsUser(h, user, http.MethodPut, path(0), outlookControlCharacters, nil); rr.Code != http.StatusCreated {
		t.Fatalf("PUT = %d: %s", rr.Code, rr.Body.String())
	}
	if rr := requestAsUser(h, user, "COPY", path(0), "", map[string]string{"Destination": path(1)}); rr.Code != http.StatusCreated {
		t.Fatalf("COPY = %d: %s", rr.Code, rr.Body.String())
	}
	if rr := requestAsUser(h, user, "MOVE", path(0), "", map[string]string{"Destination": path(2)}); rr.Code != http.StatusCreated {
		t.Fatalf("MOVE = %d: %s", rr.Code, rr.Body.String())
	}
	for _, calendar := range []int{1, 2} {
		got := requestAsUser(h, user, http.MethodGet, path(calendar), "", nil)
		if got.Code != http.StatusOK || got.Body.String() != outlookControlCharacters {
			t.Errorf("GET %s = %d %q, want the object as sent", path(calendar), got.Code, got.Body.String())
		}
	}
}
