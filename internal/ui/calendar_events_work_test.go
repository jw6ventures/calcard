package ui

import (
	"cmp"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/jw6ventures/calcard/internal/auth"
	"github.com/jw6ventures/calcard/internal/config"
	"github.com/jw6ventures/calcard/internal/dav"
	"github.com/jw6ventures/calcard/internal/ical"
	"github.com/jw6ventures/calcard/internal/store"
	"github.com/jw6ventures/calcard/internal/ui/utils"
)

// denseZoneSeriesCalendar is a calendar of n weekly series, each in a zone of
// 40 observances of its own that moves the clock on a 29th of February that is
// a Monday. Following one such zone costs a few million units of work, and
// none shares a rule with another.
func denseZoneSeriesCalendar(n int) *store.Store {
	epoch := time.Date(1601, 1, 1, 0, 0, 0, 0, time.UTC)
	events := make(map[string]*store.Event, n)
	for i := range n {
		uid := fmt.Sprintf("dense-%02d", i)
		lines := []string{"BEGIN:VCALENDAR", "VERSION:2.0", "PRODID:-//Test//EN", "BEGIN:VTIMEZONE", "TZID:Dense",
			"BEGIN:STANDARD", "DTSTART:16010101T000000", "TZOFFSETFROM:+0200", "TZOFFSETTO:+0200", "END:STANDARD"}
		for j := range 40 {
			lines = append(lines, "BEGIN:DAYLIGHT", "DTSTART:"+epoch.Add(time.Duration(i*40+j)*time.Second).Format("20060102T150405"),
				"RRULE:FREQ=DAILY;BYMONTH=2;BYMONTHDAY=29;BYDAY=MO", "TZOFFSETFROM:+0200", "TZOFFSETTO:+0300", "END:DAYLIGHT")
		}
		lines = append(lines, "END:VTIMEZONE", "BEGIN:VEVENT", "UID:"+uid, "DTSTAMP:20240101T000000Z", "SUMMARY:Dense",
			"DTSTART;TZID=Dense:20240301T093000", "DTEND;TZID=Dense:20240301T103000", "RRULE:FREQ=WEEKLY", "END:VEVENT", "END:VCALENDAR", "")
		events["1:"+uid] = &store.Event{ID: int64(i + 1), CalendarID: 1, UID: uid, ResourceName: uid + ".ics", RawICAL: strings.Join(lines, "\r\n"), ETag: "e"}
	}
	calRepo := &fakeCalendarRepo{calendars: map[int64]*store.Calendar{1: {ID: 1, UserID: 100, Name: "Work"}}}
	return &store.Store{Calendars: calRepo, Events: &fakeEventRepo{events: events}}
}

// The events JSON for a month of series whose zones each cost seconds to follow
// is bounded as a whole: the request's pool runs out after a few of them, and
// the rest are listed with their instances marked incomplete, instead of each
// costing its own seconds. Thirty such series would take over a minute without
// it.
func TestCalendarEventsJSONOverDenseZonesIsBoundedAsAWhole(t *testing.T) {
	const n = 30
	handler := NewHandler(&config.Config{}, denseZoneSeriesCalendar(n), nil, dav.ValidateCalendarObject, dav.EventInstances)
	handler.ShareTimezoneOnsets(dav.SharedTimezoneOnsets())
	w := httptest.NewRecorder()
	started := time.Now()
	handler.GetCalendarEventsJSON(w, march2024EventsRequest(&store.User{ID: 100}))
	elapsed := time.Since(started)
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d: %s", w.Code, w.Body.String())
	}
	var events []map[string]any
	if err := json.Unmarshal(w.Body.Bytes(), &events); err != nil {
		t.Fatal(err)
	}
	if len(events) != n {
		t.Fatalf("%d of %d series listed", len(events), n)
	}
	incomplete := 0
	for _, event := range events {
		if complete, ok := event["instancesComplete"].(bool); ok && !complete {
			incomplete++
		}
	}
	if incomplete == 0 || incomplete == n {
		t.Errorf("%d of %d series marked incomplete, want the first few expanded and the rest marked", incomplete, n)
	}
	if elapsed > 8*time.Second && !raceDetectorEnabled {
		t.Errorf("took %v, want the request's pool to end it within 8s", elapsed)
	}
	t.Logf("%d of %d series incomplete after %v", incomplete, n, elapsed)
}

// idOrderedEvents lists a calendar's events in ID order, so a test decides which
// the events JSON reads first.
type idOrderedEvents struct{ *fakeEventRepo }

func (r idOrderedEvents) ListForCalendar(ctx context.Context, calendarID int64) ([]store.Event, error) {
	events, err := r.fakeEventRepo.ListForCalendar(ctx, calendarID)
	slices.SortFunc(events, func(a, b store.Event) int { return cmp.Compare(a.ID, b.ID) })
	return events, err
}

// standupEvent is a weekly UTC series from Monday 4 March 2024 whose
// expansion costs a few hundred units of work.
func standupEvent(calendarID int64, id int64, uid string) *store.Event {
	raw := "BEGIN:VCALENDAR\r\nVERSION:2.0\r\nPRODID:-//Test//EN\r\nBEGIN:VEVENT\r\nUID:" + uid +
		"\r\nDTSTAMP:20240101T000000Z\r\nSUMMARY:Standup\r\nDTSTART:20240304T090000Z\r\nDTEND:20240304T091500Z\r\nRRULE:FREQ=WEEKLY\r\nEND:VEVENT\r\nEND:VCALENDAR\r\n"
	return &store.Event{ID: id, CalendarID: calendarID, UID: uid, ResourceName: uid + ".ics", RawICAL: raw, ETag: "e"}
}

type listedSeries struct {
	UID       string                  `json:"uid"`
	Instances []calendarEventInstance `json:"instances"`
	Complete  *bool                   `json:"instancesComplete"`
}

func decodeListedSeries(t *testing.T, w *httptest.ResponseRecorder) []listedSeries {
	t.Helper()
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d: %s", w.Code, w.Body.String())
	}
	var listed []listedSeries
	if err := json.Unmarshal(w.Body.Bytes(), &listed); err != nil {
		t.Fatal(err)
	}
	return listed
}

// requireStandupsServedAsAlone checks the events JSON with, served after
// series in costly zones, against alone, served for the standups by
// themselves: some costly series must have been marked incomplete, so the
// pool did run out, and each standup must still be served every instance it
// is served alone, complete. The browser places a series by the instances it
// is served, so a series served none is not shown at all.
func requireStandupsServedAsAlone(t *testing.T, with, alone *httptest.ResponseRecorder) {
	t.Helper()
	want := map[string]int{}
	for _, series := range decodeListedSeries(t, alone) {
		if len(series.Instances) == 0 || series.Complete == nil || !*series.Complete {
			t.Fatalf("%s alone is served %d instances, complete %v", series.UID, len(series.Instances), series.Complete)
		}
		want[series.UID] = len(series.Instances)
	}
	spent, seen := false, 0
	for _, series := range decodeListedSeries(t, with) {
		complete := series.Complete != nil && *series.Complete
		if !strings.HasPrefix(series.UID, "standup-") {
			spent = spent || !complete
			continue
		}
		seen++
		if len(series.Instances) != want[series.UID] || !complete {
			t.Errorf("%s is served %d instances, complete %t; want the %d it is served alone, complete", series.UID, len(series.Instances), complete, want[series.UID])
		}
	}
	if seen != len(want) {
		t.Fatalf("%d of %d standups listed", seen, len(want))
	}
	if !spent {
		t.Fatal("no costly series was marked incomplete, so the pool never ran out; the test no longer reaches the case")
	}
}

// standupsWithDenseSeries is denseZoneSeriesCalendar(5) with five standups
// in calendar standupCalendar, listed after the costly series, and the
// standups by themselves. Neither shares the process's onset listings, so
// every request pays to follow its zones whatever another test listed before.
func standupsWithDenseSeries(standupCalendar int64) (with, alone *store.Store) {
	with = denseZoneSeriesCalendar(5)
	repo := with.Events.(*fakeEventRepo)
	standups := map[string]*store.Event{}
	for i := range 5 {
		uid := fmt.Sprintf("standup-%d", i)
		key := fmt.Sprintf("%d:%s", standupCalendar, uid)
		repo.events[key] = standupEvent(standupCalendar, int64(1000+i), uid)
		standups[key] = repo.events[key]
	}
	with.Events = idOrderedEvents{repo}
	alone = &store.Store{Calendars: with.Calendars, Events: idOrderedEvents{&fakeEventRepo{events: standups}}}
	return with, alone
}

// Series whose zones each cost millions of units spend the events JSON's
// shared pool, but a series listed after them that costs little to expand is
// still served every instance it has in the view: each listed series keeps an
// allowance of its own, so the pool's running out marks only the series that
// need more than that incomplete, rather than serving the cheap ones nothing.
func TestCalendarEventsJSONServesCheapSeriesListedAfterThePoolIsSpent(t *testing.T) {
	with, alone := standupsWithDenseSeries(1)
	serve := func(s *store.Store) *httptest.ResponseRecorder {
		w := httptest.NewRecorder()
		NewHandler(&config.Config{}, s, nil, dav.ValidateCalendarObject, dav.EventInstances).
			GetCalendarEventsJSON(w, march2024EventsRequest(&store.User{ID: 100}))
		return w
	}
	requireStandupsServedAsAlone(t, serve(with), serve(alone))
}

// The view of every calendar shares one pool across them, so a calendar of
// series in costly zones listed first -- one shared with the viewer, say --
// must not leave the viewer's cheap series in a later calendar unshown.
func TestAllCalendarsEventsJSONServesCheapSeriesOfALaterCalendarAfterThePoolIsSpent(t *testing.T) {
	with, alone := standupsWithDenseSeries(2)
	calendars := &fakeCalendarRepo{listAccessible: []store.CalendarAccess{
		{Calendar: store.Calendar{ID: 1, UserID: 100, Name: "Dense"}, Editor: true},
		{Calendar: store.Calendar{ID: 2, UserID: 100, Name: "Work"}, Editor: true},
	}}
	with.Calendars, alone.Calendars = calendars, calendars
	serve := func(s *store.Store) *httptest.ResponseRecorder {
		req := httptest.NewRequest(http.MethodGet, "/calendars/all/events.json?start=2024-03-01&end=2024-04-01", nil)
		w := httptest.NewRecorder()
		NewHandler(&config.Config{}, s, nil, dav.ValidateCalendarObject, dav.EventInstances).
			GetAllCalendarEventsJSON(w, req.WithContext(auth.WithUser(req.Context(), &store.User{ID: 100})))
		return w
	}
	requireStandupsServedAsAlone(t, serve(with), serve(alone))
}

// A stored series with more overrides than an expansion lists, which PUT no
// longer admits but a 1.1.x release stored, is listed with the instances and
// overrides the budget holds, marked incomplete, and the view model copes with
// the overrides the view then places by themselves.
func TestCalendarEventsJSONListsNoMoreThanTheInstanceBudgetOfASeriesOverrides(t *testing.T) {
	first := time.Date(2024, 3, 1, 9, 0, 0, 0, time.UTC)
	var b strings.Builder
	b.WriteString("BEGIN:VCALENDAR\r\nVERSION:2.0\r\nPRODID:-//Test//EN\r\nBEGIN:VEVENT\r\nUID:legacy\r\nDTSTAMP:20240101T000000Z\r\n" +
		"DTSTART:20240301T090000Z\r\nDTEND:20240301T100000Z\r\nRRULE:FREQ=YEARLY;COUNT=2\r\nEND:VEVENT\r\n")
	const overrides = 1500
	for i := range overrides {
		slot := first.Add(time.Duration(i) * time.Minute)
		b.WriteString("BEGIN:VEVENT\r\nUID:legacy\r\nDTSTAMP:20240101T000000Z\r\nRECURRENCE-ID:" + slot.Format("20060102T150405Z") +
			"\r\nDTSTART:" + slot.Add(time.Hour).Format("20060102T150405Z") + "\r\nDTEND:" + slot.Add(2*time.Hour).Format("20060102T150405Z") + "\r\nEND:VEVENT\r\n")
	}
	b.WriteString("END:VCALENDAR\r\n")
	events := map[string]*store.Event{"1:legacy": {ID: 1, CalendarID: 1, UID: "legacy", ResourceName: "legacy.ics", RawICAL: b.String(), ETag: "e"}}
	s := &store.Store{
		Calendars: &fakeCalendarRepo{calendars: map[int64]*store.Calendar{1: {ID: 1, UserID: 100, Name: "Work"}}},
		Events:    &fakeEventRepo{events: events},
	}
	handler := NewHandler(&config.Config{}, s, nil, dav.ValidateCalendarObject, dav.EventInstances)
	w := httptest.NewRecorder()
	handler.GetCalendarEventsJSON(w, march2024EventsRequest(&store.User{ID: 100}))
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d: %s", w.Code, w.Body.String())
	}
	var listed []struct {
		Instances         []calendarEventInstance `json:"instances"`
		Complete          *bool                   `json:"instancesComplete"`
		OverrideInstances []calendarEventInstance `json:"overrideInstances"`
	}
	if err := json.Unmarshal(w.Body.Bytes(), &listed); err != nil || len(listed) != 1 {
		t.Fatalf("decoded %d events, %v", len(listed), err)
	}
	event := listed[0]
	if got := len(event.Instances) + len(event.OverrideInstances); got != 1000 {
		t.Errorf("%d instances and %d overrides listed, want 1000 together", len(event.Instances), len(event.OverrideInstances))
	}
	if event.Complete == nil || *event.Complete {
		t.Errorf("instancesComplete = %v, want false", event.Complete)
	}
}

// plainZoneEvent is a single event in a zone only its own object defines, whose
// rules start from a second of 1601 no other test shares, so reading it lists
// them afresh. The zone is four hours behind UTC in March, so 09:30 there is
// 13:30Z.
func plainZoneEvent(second int) store.Event {
	start := time.Date(1601, 1, 1, 2, 0, second, 0, time.UTC).Format("20060102T150405")
	raw := "BEGIN:VCALENDAR\r\nVERSION:2.0\r\nPRODID:-//Test//EN\r\nBEGIN:VTIMEZONE\r\nTZID:Plain\r\n" +
		"BEGIN:STANDARD\r\nDTSTART:" + start + "\r\nTZOFFSETFROM:-0400\r\nTZOFFSETTO:-0500\r\nRRULE:FREQ=YEARLY;BYDAY=1SU;BYMONTH=11\r\nEND:STANDARD\r\n" +
		"BEGIN:DAYLIGHT\r\nDTSTART:" + start + "\r\nTZOFFSETFROM:-0500\r\nTZOFFSETTO:-0400\r\nRRULE:FREQ=YEARLY;BYDAY=2SU;BYMONTH=3\r\nEND:DAYLIGHT\r\nEND:VTIMEZONE\r\n" +
		"BEGIN:VEVENT\r\nUID:plain\r\nDTSTAMP:20240101T000000Z\r\nSUMMARY:Plain\r\nDTSTART;TZID=Plain:20240315T093000\r\nDTEND;TZID=Plain:20240315T103000\r\nEND:VEVENT\r\nEND:VCALENDAR\r\n"
	return store.Event{ID: 9999, CalendarID: 1, UID: "plain", ResourceName: "plain.ics", RawICAL: raw, ETag: "e"}
}

// An event read on a pool that has run out cannot be placed by its own zone, and
// is read as UTC. Its payload says so, as a series' does with instancesComplete,
// rather than giving the wrong time as the right one; read on a pool with work
// left it carries no such flag and the right time.
func TestUnfoldedEventJSONSaysWhenItWasReadOnASpentPool(t *testing.T) {
	event := plainZoneEvent(11)
	unfolded := unfoldedEvent{event: event, lines: utils.UnfoldLines(event.RawICAL)}
	window := time.Date(2024, 3, 1, 0, 0, 0, 0, time.UTC)

	spent := unfoldedEventJSON(unfolded, window, window.AddDate(0, 1, 0), dav.SharedTimezoneOnsets(), ical.NewRequestWork(nil, 0))
	if spent["zoneComplete"] != false {
		t.Errorf("zoneComplete = %v on a spent pool, want false: dtstart %v", spent["zoneComplete"], spent["dtstart"])
	}

	ample := unfoldedEventJSON(unfolded, window, window.AddDate(0, 1, 0), dav.SharedTimezoneOnsets(), ical.NewRequestWork(nil, 20_000_000))
	if _, flagged := ample["zoneComplete"]; flagged || ample["dtstart"] != "2024-03-15T13:30:00Z" {
		t.Errorf("on a pool with work left: zoneComplete %v, dtstart %v; want unflagged and 2024-03-15T13:30:00Z", ample["zoneComplete"], ample["dtstart"])
	}

	// An event needing no zone is not flagged for the pool's being spent.
	utc := store.Event{UID: "utc", RawICAL: "BEGIN:VCALENDAR\r\nVERSION:2.0\r\nBEGIN:VEVENT\r\nUID:utc\r\nDTSTART:20240315T093000Z\r\nDTEND:20240315T103000Z\r\nEND:VEVENT\r\nEND:VCALENDAR\r\n"}
	if payload := unfoldedEventJSON(unfoldedEvent{event: utc, lines: utils.UnfoldLines(utc.RawICAL)}, window, window.AddDate(0, 1, 0), nil, ical.NewRequestWork(nil, 0)); payload["zoneComplete"] != nil {
		t.Errorf("a UTC event is flagged on a spent pool: %v", payload["zoneComplete"])
	}
}

// End to end, an events.json whose series spend the request's pool flags the
// plain event it then cannot place, and the page's script shows the flag.
func TestCalendarEventsJSONFlagsThePlainEventsASpentPoolMisplaced(t *testing.T) {
	s := denseZoneSeriesCalendar(30)
	plain := plainZoneEvent(13)
	s.Events.(*fakeEventRepo).events["1:plain"] = &plain
	handler := NewHandler(&config.Config{}, s, nil, dav.ValidateCalendarObject, dav.EventInstances)
	handler.ShareTimezoneOnsets(dav.SharedTimezoneOnsets())
	w := httptest.NewRecorder()
	handler.GetCalendarEventsJSON(w, march2024EventsRequest(&store.User{ID: 100}))
	var events []map[string]any
	if err := json.Unmarshal(w.Body.Bytes(), &events); err != nil {
		t.Fatal(err)
	}
	for _, event := range events {
		if event["uid"] != "plain" {
			continue
		}
		if event["dtstart"] != "2024-03-15T13:30:00Z" && event["zoneComplete"] != false {
			t.Fatalf("the plain event is placed at %v with no flag; the right time is 2024-03-15T13:30:00Z", event["dtstart"])
		}
		return
	}
	t.Fatal("the plain event is not listed")
}

// Both calendar pages show the notice the payload's zoneComplete asks for, from
// the one function they share.
func TestCalendarPagesShowTheNoticeForAZoneReadOnASpentPool(t *testing.T) {
	for _, page := range []string{"templates/calendar_view.html", "templates/all_calendars_view.html"} {
		source, err := templateFS.ReadFile(page)
		if err != nil {
			t.Fatal(err)
		}
		if !strings.Contains(string(source), "zoneNotice(event)") {
			t.Errorf("%s does not show zoneNotice(event) among an event's details", page)
		}
	}
	shared, err := templateFS.ReadFile("templates/partials/calendar_events_js.tmpl")
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(shared), "function zoneNotice(event)") || !strings.Contains(string(shared), "event.zoneComplete !== false") {
		t.Error("the shared script does not define zoneNotice over zoneComplete")
	}
}
