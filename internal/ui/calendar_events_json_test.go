package ui

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"reflect"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/go-chi/chi/v5"

	"github.com/jw6ventures/calcard/internal/auth"
	"github.com/jw6ventures/calcard/internal/config"
	"github.com/jw6ventures/calcard/internal/dav"
	"github.com/jw6ventures/calcard/internal/store"
)

// outlookSeriesCalendar is a calendar of n weekly Outlook series, each
// shipping its own copy of Outlook's "Eastern Standard Time", whose rules run
// from 1601, and each with one moved occurrence in March 2024.
func outlookSeriesCalendar(n int) *store.Store {
	return outlookSeriesCalendarFrom(n, 2024)
}

// outlookSeriesCalendarFrom is outlookSeriesCalendar with each series starting
// in January of year.
func outlookSeriesCalendarFrom(n, year int) *store.Store {
	events := make(map[string]*store.Event, n)
	for i := range n {
		uid := fmt.Sprintf("outlook-%d", i)
		hour := 8 + i%9
		raw := "BEGIN:VCALENDAR\r\nPRODID:Microsoft Exchange Server 2010\r\nVERSION:2.0\r\n" + outlookEastern +
			"BEGIN:VEVENT\r\nUID:" + uid + "\r\nDTSTAMP:20240101T000000Z\r\nSUMMARY:Standup\r\n" +
			fmt.Sprintf("DTSTART;TZID=\"Eastern Standard Time\":%04d01%02dT%02d0000\r\n", year, 1+i%28, hour) +
			fmt.Sprintf("DTEND;TZID=\"Eastern Standard Time\":%04d01%02dT%02d3000\r\n", year, 1+i%28, hour) +
			"RRULE:FREQ=WEEKLY;INTERVAL=1;WKST=SU\r\nEND:VEVENT\r\n" +
			"BEGIN:VEVENT\r\nUID:" + uid + "\r\nDTSTAMP:20240101T000000Z\r\nSUMMARY:Moved standup\r\n" +
			fmt.Sprintf("RECURRENCE-ID;TZID=\"Eastern Standard Time\":202403%02dT%02d0000\r\n", 4+i%7, hour) +
			fmt.Sprintf("DTSTART;TZID=\"Eastern Standard Time\":202403%02dT%02d0000\r\n", 4+i%7, hour+1) +
			fmt.Sprintf("DTEND;TZID=\"Eastern Standard Time\":202403%02dT%02d3000\r\n", 4+i%7, hour+1) +
			"END:VEVENT\r\nEND:VCALENDAR\r\n"
		events["1:"+uid] = &store.Event{ID: int64(i + 1), CalendarID: 1, UID: uid, ResourceName: uid + ".ics", RawICAL: raw, ETag: "e"}
	}
	calRepo := &fakeCalendarRepo{calendars: map[int64]*store.Calendar{1: {ID: 1, UserID: 100, Name: "Work"}}}
	return &store.Store{Calendars: calRepo, Events: &fakeEventRepo{events: events}}
}

func march2024EventsRequest(user *store.User) *http.Request {
	req := httptest.NewRequest(http.MethodGet, "/calendars/1/events.json?start=2024-03-01&end=2024-04-01", nil)
	rctx := chi.NewRouteContext()
	rctx.URLParams.Add("id", "1")
	return req.WithContext(context.WithValue(auth.WithUser(req.Context(), user), chi.RouteCtxKey, rctx))
}

// BenchmarkCalendarEventsJSONOutlookZone measures one month view of a calendar
// of Outlook series, the view model resolving each event's zone, with each
// object listing its zone's rules itself and with the listings the process
// shares.
func BenchmarkCalendarEventsJSONOutlookZone(b *testing.B) {
	for _, n := range []int{50, 500} {
		for _, shared := range []bool{false, true} {
			b.Run(fmt.Sprintf("events=%d/shared=%t", n, shared), func(b *testing.B) {
				handler := NewHandler(&config.Config{}, outlookSeriesCalendar(n), nil, dav.ValidateCalendarObject, dav.EventInstances)
				if shared {
					handler.ShareTimezoneOnsets(dav.SharedTimezoneOnsets())
				}
				user := &store.User{ID: 100}
				b.ReportAllocs()
				for b.Loop() {
					w := httptest.NewRecorder()
					handler.GetCalendarEventsJSON(w, march2024EventsRequest(user))
					if w.Code != http.StatusOK {
						b.Fatalf("events JSON status = %d", w.Code)
					}
				}
			})
		}
	}
}

// BenchmarkCalendarEventsJSONLargeCalendar measures one month view of a
// calendar of 5000 single events, one a day from 2014, of which the month
// holds 31: the view's cost should follow what it shows rather than what the
// calendar holds.
func BenchmarkCalendarEventsJSONLargeCalendar(b *testing.B) {
	const n = 5000
	events := make(map[string]*store.Event, n)
	first := time.Date(2014, 1, 1, 9, 0, 0, 0, time.UTC)
	for i := range n {
		uid := fmt.Sprintf("event-%d", i)
		day := first.AddDate(0, 0, i)
		raw := "BEGIN:VCALENDAR\r\nVERSION:2.0\r\nPRODID:-//Client//EN\r\n" +
			"BEGIN:VEVENT\r\nUID:" + uid + "\r\nDTSTAMP:20140101T000000Z\r\nSUMMARY:Daily note\r\n" +
			"DESCRIPTION:" + strings.Repeat("A line of notes for the day. ", 20) + "\r\n" +
			"DTSTART:" + day.Format("20060102T150405Z") + "\r\nDTEND:" + day.Add(time.Hour).Format("20060102T150405Z") + "\r\n" +
			"END:VEVENT\r\nEND:VCALENDAR\r\n"
		start, end := day, day.Add(time.Hour)
		events["1:"+uid] = &store.Event{ID: int64(i + 1), CalendarID: 1, UID: uid, ResourceName: uid + ".ics", RawICAL: raw, ETag: "e", DTStart: &start, DTEnd: &end}
	}
	s := &store.Store{
		Calendars: &fakeCalendarRepo{calendars: map[int64]*store.Calendar{1: {ID: 1, UserID: 100, Name: "Work"}}},
		Events:    &fakeEventRepo{events: events},
	}
	handler := NewHandler(&config.Config{}, s, nil, dav.ValidateCalendarObject, dav.EventInstances)
	user := &store.User{ID: 100}
	b.ReportAllocs()
	for b.Loop() {
		w := httptest.NewRecorder()
		handler.GetCalendarEventsJSON(w, march2024EventsRequest(user))
		if w.Code != http.StatusOK {
			b.Fatalf("events JSON status = %d", w.Code)
		}
	}
}

// BenchmarkCalendarEventsJSONSeriesSince2000 is the same month view of series
// started in 2000, whose zone offsets the view model lists around each series'
// start and the month viewed.
func BenchmarkCalendarEventsJSONSeriesSince2000(b *testing.B) {
	handler := NewHandler(&config.Config{}, outlookSeriesCalendarFrom(500, 2000), nil, dav.ValidateCalendarObject, dav.EventInstances)
	handler.ShareTimezoneOnsets(dav.SharedTimezoneOnsets())
	user := &store.User{ID: 100}
	b.ReportAllocs()
	for b.Loop() {
		w := httptest.NewRecorder()
		handler.GetCalendarEventsJSON(w, march2024EventsRequest(user))
		if w.Code != http.StatusOK {
			b.Fatalf("events JSON status = %d", w.Code)
		}
		b.ReportMetric(float64(w.Body.Len()), "B/response")
	}
}

// The view model reads the same times and offsets whether an object lists its
// zones' rules itself or reads them through the listings the process shares.
func TestCalendarEventsJSONIsTheSameThroughSharedOnsets(t *testing.T) {
	s := outlookSeriesCalendar(20)
	events := s.Events.(*fakeEventRepo).events
	add := func(uid, raw string) {
		events["1:"+uid] = &store.Event{ID: int64(len(events) + 1), CalendarID: 1, UID: uid, ResourceName: uid + ".ics", RawICAL: raw, ETag: "e"}
	}
	add("sao-paulo", "BEGIN:VCALENDAR\r\nVERSION:2.0\r\nPRODID:-//Client//EN\r\n"+clientSaoPaulo+
		"BEGIN:VEVENT\r\nUID:sao-paulo\r\nDTSTAMP:20240101T000000Z\r\nSUMMARY:Weekly\r\n"+
		"DTSTART;TZID=America/Sao_Paulo:20240205T100000\r\nDTEND;TZID=America/Sao_Paulo:20240205T110000\r\n"+
		"RRULE:FREQ=WEEKLY;UNTIL=20240401T120000Z\r\nEXDATE;TZID=America/Sao_Paulo:20240311T100000\r\nEND:VEVENT\r\n"+
		"BEGIN:VEVENT\r\nUID:sao-paulo\r\nDTSTAMP:20240101T000000Z\r\nSUMMARY:Moved\r\n"+
		"RECURRENCE-ID;TZID=America/Sao_Paulo:20240318T100000\r\n"+
		"DTSTART;TZID=\"Eastern Standard Time\":20240318T090000\r\nDTEND;TZID=\"Eastern Standard Time\":20240318T100000\r\nEND:VEVENT\r\n"+
		outlookEastern+"END:VCALENDAR\r\n")
	add("legacy", "BEGIN:VCALENDAR\r\nVERSION:2.0\r\nPRODID:-//CalCard//EN\r\n"+
		strings.ReplaceAll(legacyNewYork2026, "2026", "2024")+
		"BEGIN:VEVENT\r\nUID:legacy\r\nDTSTAMP:20240101T000000Z\r\nSUMMARY:Legacy\r\n"+
		"DTSTART;TZID=America/New_York:20240312T090000\r\nDTEND;TZID=America/New_York:20240312T100000\r\nEND:VEVENT\r\nEND:VCALENDAR\r\n")
	add("host", "BEGIN:VCALENDAR\r\nVERSION:2.0\r\nPRODID:-//Client//EN\r\n"+
		"BEGIN:VEVENT\r\nUID:host\r\nDTSTAMP:20240101T000000Z\r\nSUMMARY:Host zone\r\n"+
		"DTSTART;TZID=Europe/Berlin:20240320T090000\r\nDTEND;TZID=Europe/Berlin:20240320T100000\r\nEND:VEVENT\r\nEND:VCALENDAR\r\n")
	add("all-day", "BEGIN:VCALENDAR\r\nVERSION:2.0\r\nPRODID:-//Client//EN\r\n"+
		"BEGIN:VEVENT\r\nUID:all-day\r\nDTSTAMP:20240101T000000Z\r\nSUMMARY:Holiday\r\n"+
		"DTSTART;VALUE=DATE:20240329\r\nDTEND;VALUE=DATE:20240330\r\nEND:VEVENT\r\nEND:VCALENDAR\r\n")

	user := &store.User{ID: 100}
	monthView := func(shared bool) []map[string]any {
		t.Helper()
		handler := NewHandler(&config.Config{}, s, nil, dav.ValidateCalendarObject, dav.EventInstances)
		if shared {
			handler.ShareTimezoneOnsets(dav.SharedTimezoneOnsets())
		}
		w := httptest.NewRecorder()
		handler.GetCalendarEventsJSON(w, march2024EventsRequest(user))
		var payload []map[string]any
		if err := json.Unmarshal(w.Body.Bytes(), &payload); err != nil || w.Code != http.StatusOK {
			t.Fatalf("events JSON = %d %s (%v)", w.Code, w.Body.String(), err)
		}
		slices.SortFunc(payload, func(a, b map[string]any) int { return strings.Compare(a["uid"].(string), b["uid"].(string)) })
		return payload
	}
	own, shared := monthView(false), monthView(true)
	if len(own) != len(events) {
		t.Fatalf("the month view shows %d events, want %d", len(own), len(events))
	}
	if !reflect.DeepEqual(own, shared) {
		t.Errorf("the view model differs through shared onsets:\nown    %v\nshared %v", own, shared)
	}

	byUID := make(map[string]map[string]any)
	for _, event := range shared {
		byUID[event["uid"].(string)] = event
	}
	offsets := func(uid, zone string) []any {
		listing, _ := byUID[uid]["timezoneOffsets"].(map[string]any)[zone].([]any)
		return listing
	}
	// From two days before the series' 08:00 EST start through two days past
	// the view, across the second Sunday of March, when Outlook's rules move
	// to daylight time.
	wantEastern := []any{
		map[string]any{"from": "2023-12-30T13:00:00Z", "offset": float64(-5 * 3600)},
		map[string]any{"from": "2024-03-10T07:00:00Z", "offset": float64(-4 * 3600)},
	}
	if got := offsets("outlook-0", "Eastern Standard Time"); !reflect.DeepEqual(got, wantEastern) {
		t.Errorf("offsets of Eastern Standard Time = %v, want %v", got, wantEastern)
	}
	// The client's Sao Paulo keeps -02 until the third Sunday of February,
	// between the two days either side of the series' start and those before
	// the view, which is listed at -03 from two days before the first instant
	// a browser shows in March: the far east's midnight, 14 hours before UTC's.
	wantSaoPaulo := []any{
		map[string]any{"from": "2024-02-03T12:00:00Z", "offset": float64(-2 * 3600)},
		map[string]any{"from": "2024-02-27T10:00:00Z", "offset": float64(-3 * 3600)},
	}
	if got := offsets("sao-paulo", "America/Sao_Paulo"); !reflect.DeepEqual(got, wantSaoPaulo) {
		t.Errorf("offsets of America/Sao_Paulo = %v, want %v", got, wantSaoPaulo)
	}
	if _, listed := byUID["host"]["timezoneOffsets"]; listed {
		t.Errorf("a zone only the host defines is listed: %v", byUID["host"]["timezoneOffsets"])
	}
}

// A series the v1.1 editor stored with its definition of America/New_York for
// 2025 alone is read through the host's zone, which the browser shares, so the
// view model lists no offsets of that definition for the browser to place the
// summer 2026 instances by: read in it, 10:00 would be standard time, and the
// browser would show them at 11:00.
func TestCalendarEventsJSONReadsAV11DefinitionAsTheHostZone(t *testing.T) {
	if _, err := time.LoadLocation("America/New_York"); err != nil {
		t.Skipf("America/New_York is not available: %v", err)
	}
	raw := "BEGIN:VCALENDAR\r\nVERSION:2.0\r\nPRODID:-//CalCard//EN\r\n" + strings.ReplaceAll(legacyNewYork2026, "2026", "2025") +
		"BEGIN:VEVENT\r\nUID:legacy\r\nDTSTAMP:20250701T000000Z\r\nSUMMARY:Weekly\r\n" +
		"DTSTART;TZID=America/New_York:20250706T100000\r\nDTEND;TZID=America/New_York:20250706T110000\r\n" +
		"RRULE:FREQ=WEEKLY\r\nEND:VEVENT\r\nEND:VCALENDAR\r\n"
	s := &store.Store{
		Calendars: &fakeCalendarRepo{calendars: map[int64]*store.Calendar{1: {ID: 1, UserID: 100, Name: "Work"}}},
		Events: &fakeEventRepo{events: map[string]*store.Event{
			"1:legacy": {ID: 1, CalendarID: 1, UID: "legacy", ResourceName: "legacy.ics", RawICAL: raw, ETag: "e"},
		}},
	}
	user := &store.User{ID: 100}
	req := httptest.NewRequest(http.MethodGet, "/calendars/1/events.json?start=2026-07-01&end=2026-08-01", nil)
	rctx := chi.NewRouteContext()
	rctx.URLParams.Add("id", "1")
	req = req.WithContext(context.WithValue(auth.WithUser(req.Context(), user), chi.RouteCtxKey, rctx))
	w := httptest.NewRecorder()
	NewHandler(&config.Config{}, s, nil, dav.ValidateCalendarObject, dav.EventInstances).GetCalendarEventsJSON(w, req)
	var payload []map[string]any
	if err := json.Unmarshal(w.Body.Bytes(), &payload); err != nil || w.Code != http.StatusOK || len(payload) != 1 {
		t.Fatalf("events JSON = %d %s (%v)", w.Code, w.Body.String(), err)
	}
	if offsets, listed := payload[0]["timezoneOffsets"]; listed {
		t.Errorf("the v1.1 definition's offsets are listed for the browser: %v", offsets)
	}
}

// Each event the view model lists carries the ETag of the stored event, which
// the editor sends back with an edit or a delete to show what version it was
// opened on.
func TestCalendarEventsJSONCarriesTheETagOfEachEvent(t *testing.T) {
	handler, _, events := servedOutlookHandler(t)
	events.events["1:outlook-standup"].ETag = "standup-version"
	user := &store.User{ID: 100}

	endpoints := map[string]func(http.ResponseWriter, *http.Request){
		"one calendar": func(w http.ResponseWriter, r *http.Request) {
			rctx := chi.NewRouteContext()
			rctx.URLParams.Add("id", "1")
			handler.GetCalendarEventsJSON(w, r.WithContext(context.WithValue(r.Context(), chi.RouteCtxKey, rctx)))
		},
		"all calendars": func(w http.ResponseWriter, r *http.Request) { handler.GetAllCalendarEventsJSON(w, r) },
	}
	for name, serve := range endpoints {
		t.Run(name, func(t *testing.T) {
			req := httptest.NewRequest(http.MethodGet, "/events.json?start=2024-03-01&end=2024-04-01", nil)
			w := httptest.NewRecorder()
			serve(w, req.WithContext(auth.WithUser(req.Context(), user)))
			var payload []map[string]any
			if err := json.Unmarshal(w.Body.Bytes(), &payload); err != nil || w.Code != http.StatusOK || len(payload) == 0 {
				t.Fatalf("events JSON = %d %s (%v)", w.Code, w.Body.String(), err)
			}
			for _, event := range payload {
				want := "e"
				if event["uid"] == "outlook-standup" {
					want = "standup-version"
				}
				if event["etag"] != want {
					t.Errorf("event %v carries etag %v, want %q", event["uid"], event["etag"], want)
				}
			}
		})
	}
}

// A date range the events listings refuse is answered with a plain message of
// the listing's own, never the text of an error.
func TestCalendarEventsJSONRefusesABadRangeInPlainWords(t *testing.T) {
	handler, _, _ := servedOutlookHandler(t)
	user := &store.User{ID: 100}
	tests := map[string]string{
		"start=2024-03-01":                "start and end are required",
		"start=soon&end=2024-04-01":       "invalid start date",
		"start=2024-03-01&end=later":      "invalid end date",
		"start=2024-04-01&end=2024-03-01": "invalid date range",
	}
	for query, want := range tests {
		for name, serve := range map[string]func(http.ResponseWriter, *http.Request){
			"one calendar": func(w http.ResponseWriter, r *http.Request) {
				rctx := chi.NewRouteContext()
				rctx.URLParams.Add("id", "1")
				handler.GetCalendarEventsJSON(w, r.WithContext(context.WithValue(r.Context(), chi.RouteCtxKey, rctx)))
			},
			"all calendars": func(w http.ResponseWriter, r *http.Request) { handler.GetAllCalendarEventsJSON(w, r) },
		} {
			req := httptest.NewRequest(http.MethodGet, "/events.json?"+query, nil)
			w := httptest.NewRecorder()
			serve(w, req.WithContext(auth.WithUser(req.Context(), user)))
			if got := strings.TrimSpace(w.Body.String()); w.Code != http.StatusBadRequest || got != want {
				t.Errorf("%s ?%s: status %d body %q, want 400 %q", name, query, w.Code, got, want)
			}
		}
	}
}
