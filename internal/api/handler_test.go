package api

import (
	"context"
	"encoding/json"
	"errors"
	"maps"
	"net/http"
	"net/http/httptest"
	"reflect"
	"slices"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/jw6ventures/calcard/internal/auth"
	"github.com/jw6ventures/calcard/internal/calobject"
	"github.com/jw6ventures/calcard/internal/config"
	"github.com/jw6ventures/calcard/internal/dav"
	"github.com/jw6ventures/calcard/internal/events"
	"github.com/jw6ventures/calcard/internal/store"
)

func TestCreateEventStructured(t *testing.T) {
	eventRepo := &fakeEventRepo{events: map[string]store.Event{}}
	handler := NewHandler(&config.Config{}, &store.Store{
		Calendars: &fakeCalendarRepo{
			calendars: map[int64]*store.CalendarAccess{
				1: {Calendar: store.Calendar{ID: 1, UserID: 1, Name: "Work"}, Editor: true},
			},
		},
		Events: eventRepo,
	}, dav.ValidateCalendarObject)

	req := httptest.NewRequest(http.MethodPost, "/api/calendars/1/events", strings.NewReader(`{
		"inputMode":"structured",
		"structured":{
			"summary":"Planning",
			"dtstart":"2026-03-20T10:00",
			"dtend":"2026-03-20T11:00"
		}
	}`))
	req.Header.Set("Content-Type", "application/json")
	req = withUserAndRoute(req, "1", "")

	rec := httptest.NewRecorder()
	handler.CreateEvent(rec, req)

	if rec.Code != http.StatusCreated {
		t.Fatalf("CreateEvent() status = %d, want %d, body=%s", rec.Code, http.StatusCreated, rec.Body.String())
	}
	if rec.Header().Get("ETag") == "" {
		t.Fatal("expected ETag header")
	}
	if len(eventRepo.events) != 1 {
		t.Fatalf("expected 1 stored event, got %d", len(eventRepo.events))
	}
	for _, ev := range eventRepo.events {
		if !strings.Contains(ev.RawICAL, "SUMMARY:Planning") {
			t.Fatalf("expected stored ICS to contain summary, got %s", ev.RawICAL)
		}
	}
}

func TestCreateEventRawICSRejectsMethod(t *testing.T) {
	handler := NewHandler(&config.Config{}, &store.Store{
		Calendars: &fakeCalendarRepo{
			calendars: map[int64]*store.CalendarAccess{
				1: {Calendar: store.Calendar{ID: 1, UserID: 1, Name: "Work"}, Editor: true},
			},
		},
		Events: &fakeEventRepo{events: map[string]store.Event{}},
	}, dav.ValidateCalendarObject)

	rawICS := "BEGIN:VCALENDAR\r\nVERSION:2.0\r\nMETHOD:REQUEST\r\nBEGIN:VEVENT\r\nUID:test-1\r\nSUMMARY:Planning\r\nDTSTART:20260320T100000Z\r\nDTEND:20260320T110000Z\r\nEND:VEVENT\r\nEND:VCALENDAR\r\n"
	req := httptest.NewRequest(http.MethodPost, "/api/calendars/1/events", strings.NewReader(rawICS))
	req.Header.Set("Content-Type", "text/calendar")
	req = withUserAndRoute(req, "1", "")

	rec := httptest.NewRecorder()
	handler.CreateEvent(rec, req)

	if rec.Code != http.StatusConflict {
		t.Fatalf("CreateEvent() status = %d, want %d, body=%s", rec.Code, http.StatusConflict, rec.Body.String())
	}
}

// The iCalendar a JSON body carries in rawIcal under inputMode raw_ical is the
// same payload a text/calendar body is: it meets the same validator, is
// refused with the same status, and is stored the same way.
func TestRawICalJSONIsAnsweredAsATextCalendarBody(t *testing.T) {
	ics := func(uid string, extra ...string) string {
		lines := []string{"BEGIN:VCALENDAR", "VERSION:2.0", "PRODID:-//test//EN"}
		lines = append(lines, extra...)
		lines = append(lines, "BEGIN:VEVENT")
		if uid != "" {
			lines = append(lines, "UID:"+uid)
		}
		lines = append(lines, "DTSTAMP:20260101T000000Z", "SUMMARY:New", "DTSTART:20260320T100000Z", "DTEND:20260320T110000Z", "END:VEVENT", "END:VCALENDAR")
		return strings.Join(lines, "\r\n") + "\r\n"
	}
	tests := []struct {
		name   string
		method string
		uid    string
		ics    string
		want   int
	}{
		{name: "create", method: http.MethodPost, ics: ics("new-1"), want: http.StatusCreated},
		{name: "create a scheduling message", method: http.MethodPost, ics: ics("new-1", "METHOD:REQUEST"), want: http.StatusConflict},
		{name: "create without a UID", method: http.MethodPost, ics: ics(""), want: http.StatusBadRequest},
		{name: "update", method: http.MethodPut, uid: "event-1", ics: ics("event-1"), want: http.StatusOK},
		{name: "update a scheduling message", method: http.MethodPut, uid: "event-1", ics: ics("event-1", "METHOD:REQUEST"), want: http.StatusConflict},
		{name: "update naming another UID", method: http.MethodPut, uid: "event-1", ics: ics("event-2"), want: http.StatusBadRequest},
	}
	for _, tt := range tests {
		jsonBody, err := json.Marshal(map[string]string{"inputMode": "raw_ical", "rawIcal": tt.ics})
		if err != nil {
			t.Fatal(err)
		}
		stored := map[string]string{}
		for _, mode := range []struct{ contentType, body string }{
			{contentType: "text/calendar", body: tt.ics},
			{contentType: "application/json", body: string(jsonBody)},
		} {
			t.Run(tt.name+" "+mode.contentType, func(t *testing.T) {
				eventRepo := &fakeEventRepo{events: map[string]store.Event{
					"1:event-1": {CalendarID: 1, UID: "event-1", ResourceName: "event-1", RawICAL: ics("event-1"), ETag: "current", LastModified: time.Now().UTC()},
				}}
				handler := NewHandler(&config.Config{}, &store.Store{
					Calendars: &fakeCalendarRepo{calendars: map[int64]*store.CalendarAccess{
						1: {Calendar: store.Calendar{ID: 1, UserID: 1, Name: "Work"}, Editor: true},
					}},
					Events: eventRepo,
				}, dav.ValidateCalendarObject)

				req := httptest.NewRequest(tt.method, "/api/calendars/1/events/"+tt.uid, strings.NewReader(mode.body))
				req.Header.Set("Content-Type", mode.contentType)
				req = withUserAndRoute(req, "1", tt.uid)
				rec := httptest.NewRecorder()
				if tt.method == http.MethodPost {
					handler.CreateEvent(rec, req)
				} else {
					handler.UpdateEvent(rec, req)
				}

				if rec.Code != tt.want {
					t.Fatalf("status = %d, want %d, body=%s", rec.Code, tt.want, rec.Body.String())
				}
				var bodies []string
				for _, key := range slices.Sorted(maps.Keys(eventRepo.events)) {
					bodies = append(bodies, key+"="+eventRepo.events[key].RawICAL)
				}
				stored[mode.contentType] = strings.Join(bodies, "\n")
			})
		}
		if stored["text/calendar"] != stored["application/json"] {
			t.Errorf("%s: stored events differ between the modes:\ntext/calendar:\n%s\napplication/json:\n%s", tt.name, stored["text/calendar"], stored["application/json"])
		}
	}
}

func TestUpdateEventRequiresMatchingIfMatch(t *testing.T) {
	existing := store.Event{
		CalendarID:   1,
		UID:          "event-1",
		ResourceName: "event-1",
		RawICAL:      "BEGIN:VCALENDAR\r\nVERSION:2.0\r\nBEGIN:VEVENT\r\nUID:event-1\r\nSUMMARY:Old\r\nDTSTART:20260320T100000Z\r\nDTEND:20260320T110000Z\r\nEND:VEVENT\r\nEND:VCALENDAR\r\n",
		ETag:         "current",
		LastModified: time.Now().UTC(),
	}
	handler := NewHandler(&config.Config{}, &store.Store{
		Calendars: &fakeCalendarRepo{
			calendars: map[int64]*store.CalendarAccess{
				1: {Calendar: store.Calendar{ID: 1, UserID: 1, Name: "Work"}, Editor: true},
			},
		},
		Events: &fakeEventRepo{events: map[string]store.Event{"1:event-1": existing}},
	}, dav.ValidateCalendarObject)

	req := httptest.NewRequest(http.MethodPut, "/api/calendars/1/events/event-1", strings.NewReader(`{
		"inputMode":"structured",
		"structured":{
			"summary":"Updated",
			"dtstart":"2026-03-20T10:00",
			"dtend":"2026-03-20T11:00",
			"uid":"event-1"
		}
	}`))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("If-Match", `"wrong"`)
	req = withUserAndRoute(req, "1", "event-1")

	rec := httptest.NewRecorder()
	handler.UpdateEvent(rec, req)

	if rec.Code != http.StatusPreconditionFailed {
		t.Fatalf("UpdateEvent() status = %d, want %d, body=%s", rec.Code, http.StatusPreconditionFailed, rec.Body.String())
	}
}

func TestGetEventReturnsJSON(t *testing.T) {
	summary := "Planning"
	start := time.Date(2026, 3, 20, 10, 0, 0, 0, time.UTC)
	end := start.Add(time.Hour)
	ev := store.Event{
		CalendarID:   1,
		UID:          "event-1",
		ResourceName: "event-1",
		RawICAL:      "BEGIN:VCALENDAR\r\nVERSION:2.0\r\nBEGIN:VEVENT\r\nUID:event-1\r\nSUMMARY:Planning\r\nDTSTART:20260320T100000Z\r\nDTEND:20260320T110000Z\r\nEND:VEVENT\r\nEND:VCALENDAR\r\n",
		ETag:         "etag-1",
		Summary:      &summary,
		DTStart:      &start,
		DTEnd:        &end,
		LastModified: time.Now().UTC(),
	}
	handler := NewHandler(&config.Config{}, &store.Store{
		Calendars: &fakeCalendarRepo{
			calendars: map[int64]*store.CalendarAccess{
				1: {Calendar: store.Calendar{ID: 1, UserID: 1, Name: "Work"}, Editor: true},
			},
		},
		Events: &fakeEventRepo{events: map[string]store.Event{"1:event-1": ev}},
	}, dav.ValidateCalendarObject)

	req := httptest.NewRequest(http.MethodGet, "/api/calendars/1/events/event-1", nil)
	req = withUserAndRoute(req, "1", "event-1")
	rec := httptest.NewRecorder()

	handler.GetEvent(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("GetEvent() status = %d, want %d, body=%s", rec.Code, http.StatusOK, rec.Body.String())
	}
	var body eventResponse
	if err := json.NewDecoder(rec.Body).Decode(&body); err != nil {
		t.Fatalf("failed to decode response: %v", err)
	}
	if body.UID != "event-1" {
		t.Fatalf("expected uid event-1, got %s", body.UID)
	}
	if body.ETag != "etag-1" {
		t.Fatalf("expected etag etag-1, got %s", body.ETag)
	}
}

func TestListCalendarsUnauthorized(t *testing.T) {
	handler := NewHandler(&config.Config{}, &store.Store{}, dav.ValidateCalendarObject)
	req := httptest.NewRequest(http.MethodGet, "/api/calendars", nil)
	rec := httptest.NewRecorder()

	handler.ListCalendars(rec, req)

	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("ListCalendars() status = %d, want %d", rec.Code, http.StatusUnauthorized)
	}
}

func TestListCalendarsSuccess(t *testing.T) {
	handler := NewHandler(&config.Config{}, &store.Store{
		Calendars: &fakeCalendarRepo{calendars: map[int64]*store.CalendarAccess{
			1: {Calendar: store.Calendar{ID: 1, UserID: 1, Name: "Work"}, Editor: true, OwnerEmail: "owner@example.com"},
		}},
	}, dav.ValidateCalendarObject)

	req := httptest.NewRequest(http.MethodGet, "/api/calendars", nil)
	req = req.WithContext(auth.WithUser(req.Context(), &store.User{ID: 1}))
	rec := httptest.NewRecorder()

	handler.ListCalendars(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("ListCalendars() status = %d, want %d", rec.Code, http.StatusOK)
	}
}

func TestListCalendarsIncludesObjectOnlyGrantWithoutCollectionCapabilities(t *testing.T) {
	handler := NewHandler(&config.Config{}, &store.Store{
		Calendars: &fakeCalendarRepo{calendars: map[int64]*store.CalendarAccess{
			1: {
				Calendar:           store.Calendar{ID: 1, UserID: 9, Name: "Object Shared"},
				OwnerEmail:         "owner@example.com",
				Shared:             true,
				PrivilegesResolved: true,
			},
		}},
	}, dav.ValidateCalendarObject)

	req := httptest.NewRequest(http.MethodGet, "/api/calendars", nil)
	req = req.WithContext(auth.WithUser(req.Context(), &store.User{ID: 1}))
	rec := httptest.NewRecorder()

	handler.ListCalendars(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("ListCalendars() status = %d, want %d, body=%s", rec.Code, http.StatusOK, rec.Body.String())
	}

	var body []map[string]any
	if err := json.NewDecoder(rec.Body).Decode(&body); err != nil {
		t.Fatalf("failed to decode response: %v", err)
	}
	if len(body) != 1 {
		t.Fatalf("expected one calendar, got %#v", body)
	}
	if body[0]["name"] != "" {
		t.Fatalf("expected object-only grant to redact calendar name, got %#v", body[0]["name"])
	}
	if body[0]["ownerEmail"] != "" {
		t.Fatalf("expected object-only grant to redact owner email, got %#v", body[0]["ownerEmail"])
	}
	for _, key := range []string{"description", "timezone", "color"} {
		if _, ok := body[0][key]; ok {
			t.Fatalf("expected object-only grant to omit %s, got %#v", key, body[0][key])
		}
	}
	capabilities, ok := body[0]["capabilities"].(map[string]any)
	if !ok {
		t.Fatalf("expected capabilities object, got %#v", body[0]["capabilities"])
	}
	for _, privilege := range []string{"read", "readFreeBusy", "write", "writeContent", "writeProperties", "bind", "unbind"} {
		if capabilities[privilege] != false {
			t.Fatalf("expected %s=false for object-only grant discovery row, got %#v", privilege, capabilities)
		}
	}
}

func TestListCalendarsInternalError(t *testing.T) {
	handler := NewHandler(&config.Config{}, &store.Store{
		Calendars: &fakeCalendarRepo{listAccessibleErr: errors.New("boom")},
	}, dav.ValidateCalendarObject)

	req := httptest.NewRequest(http.MethodGet, "/api/calendars", nil)
	req = req.WithContext(auth.WithUser(req.Context(), &store.User{ID: 1}))
	rec := httptest.NewRecorder()

	handler.ListCalendars(rec, req)

	if rec.Code != http.StatusInternalServerError {
		t.Fatalf("ListCalendars() status = %d, want %d", rec.Code, http.StatusInternalServerError)
	}
}

func TestGetCalendarUnauthorizedAndInvalidID(t *testing.T) {
	handler := NewHandler(&config.Config{}, &store.Store{}, dav.ValidateCalendarObject)

	req := httptest.NewRequest(http.MethodGet, "/api/calendars/1", nil)
	rec := httptest.NewRecorder()
	handler.GetCalendar(rec, req)
	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("GetCalendar() unauthorized status = %d", rec.Code)
	}

	req = httptest.NewRequest(http.MethodGet, "/api/calendars/x", nil)
	req = req.WithContext(auth.WithUser(req.Context(), &store.User{ID: 1}))
	routeCtx := chi.NewRouteContext()
	routeCtx.URLParams.Add("id", "x")
	req = req.WithContext(context.WithValue(req.Context(), chi.RouteCtxKey, routeCtx))
	rec = httptest.NewRecorder()
	handler.GetCalendar(rec, req)
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("GetCalendar() invalid id status = %d", rec.Code)
	}
}

func TestGetCalendarSuccess(t *testing.T) {
	handler := NewHandler(&config.Config{}, &store.Store{
		Calendars: &fakeCalendarRepo{calendars: map[int64]*store.CalendarAccess{
			1: {Calendar: store.Calendar{ID: 1, UserID: 1, Name: "Work"}, Editor: true, OwnerEmail: "owner@example.com"},
		}},
	}, dav.ValidateCalendarObject)

	req := httptest.NewRequest(http.MethodGet, "/api/calendars/1", nil)
	req = withUserAndRoute(req, "1", "")
	rec := httptest.NewRecorder()

	handler.GetCalendar(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("GetCalendar() status = %d, want %d", rec.Code, http.StatusOK)
	}
}

func TestGetCalendarIncludesPartialWriteCapabilities(t *testing.T) {
	handler := NewHandler(&config.Config{}, &store.Store{
		Calendars: &fakeCalendarRepo{calendars: map[int64]*store.CalendarAccess{
			1: {
				Calendar:   store.Calendar{ID: 1, UserID: 9, Name: "Shared"},
				OwnerEmail: "owner@example.com",
				Shared:     true,
				Editor:     false,
				Privileges: store.CalendarPrivileges{Read: true, Bind: true},
			},
		}},
	}, dav.ValidateCalendarObject)

	req := httptest.NewRequest(http.MethodGet, "/api/calendars/1", nil)
	req = withUserAndRoute(req, "1", "")
	rec := httptest.NewRecorder()

	handler.GetCalendar(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("GetCalendar() status = %d, want %d, body=%s", rec.Code, http.StatusOK, rec.Body.String())
	}

	var body map[string]any
	if err := json.NewDecoder(rec.Body).Decode(&body); err != nil {
		t.Fatalf("failed to decode response: %v", err)
	}
	if body["name"] != "Shared" {
		t.Fatalf("expected calendar metadata to remain visible for collection grant, got %#v", body["name"])
	}
	if _, ok := body["editor"]; ok {
		t.Fatalf("did not expect legacy editor field, got %#v", body["editor"])
	}
	capabilities, ok := body["capabilities"].(map[string]any)
	if !ok {
		t.Fatalf("expected capabilities object, got %#v", body["capabilities"])
	}
	if capabilities["bind"] != true {
		t.Fatalf("expected bind capability, got %#v", capabilities)
	}
	if capabilities["writeContent"] != false {
		t.Fatalf("expected writeContent=false for bind-only access, got %#v", capabilities)
	}
}

func TestGetCalendarDoesNotAdvertiseDeniedWriteSubPrivileges(t *testing.T) {
	handler := NewHandler(&config.Config{}, &store.Store{
		Calendars: &fakeCalendarRepo{calendars: map[int64]*store.CalendarAccess{
			1: {
				Calendar:   store.Calendar{ID: 1, UserID: 9, Name: "Shared"},
				OwnerEmail: "owner@example.com",
				Shared:     true,
				Editor:     true,
				Privileges: store.CalendarPrivileges{
					Read:            true,
					ReadFreeBusy:    true,
					Write:           true,
					WriteContent:    false,
					WriteProperties: true,
					Bind:            true,
					Unbind:          true,
				},
			},
		}},
	}, dav.ValidateCalendarObject)

	req := httptest.NewRequest(http.MethodGet, "/api/calendars/1", nil)
	req = withUserAndRoute(req, "1", "")
	rec := httptest.NewRecorder()

	handler.GetCalendar(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("GetCalendar() status = %d, want %d, body=%s", rec.Code, http.StatusOK, rec.Body.String())
	}

	var body map[string]any
	if err := json.NewDecoder(rec.Body).Decode(&body); err != nil {
		t.Fatalf("failed to decode response: %v", err)
	}
	if body["name"] != "Shared" {
		t.Fatalf("expected calendar metadata to remain visible for collection grant, got %#v", body["name"])
	}
	capabilities, ok := body["capabilities"].(map[string]any)
	if !ok {
		t.Fatalf("expected capabilities object, got %#v", body["capabilities"])
	}
	if capabilities["write"] != false {
		t.Fatalf("expected write=false when write-content is denied, got %#v", capabilities)
	}
	if capabilities["writeContent"] != false {
		t.Fatalf("expected writeContent=false when explicitly denied, got %#v", capabilities)
	}
	if capabilities["bind"] != true || capabilities["unbind"] != true {
		t.Fatalf("expected bind/unbind to remain true, got %#v", capabilities)
	}
}

func TestGetCalendarIncludesObjectOnlyGrantWithoutCollectionCapabilities(t *testing.T) {
	handler := NewHandler(&config.Config{}, &store.Store{
		Calendars: &fakeCalendarRepo{calendars: map[int64]*store.CalendarAccess{
			1: {
				Calendar:           store.Calendar{ID: 1, UserID: 9, Name: "Object Shared"},
				OwnerEmail:         "owner@example.com",
				Shared:             true,
				PrivilegesResolved: true,
			},
		}},
	}, dav.ValidateCalendarObject)

	req := httptest.NewRequest(http.MethodGet, "/api/calendars/1", nil)
	req = withUserAndRoute(req, "1", "")
	rec := httptest.NewRecorder()

	handler.GetCalendar(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("GetCalendar() status = %d, want %d, body=%s", rec.Code, http.StatusOK, rec.Body.String())
	}

	var body map[string]any
	if err := json.NewDecoder(rec.Body).Decode(&body); err != nil {
		t.Fatalf("failed to decode response: %v", err)
	}
	capabilities, ok := body["capabilities"].(map[string]any)
	if !ok {
		t.Fatalf("expected capabilities object, got %#v", body["capabilities"])
	}
	for _, privilege := range []string{"read", "readFreeBusy", "write", "writeContent", "writeProperties", "bind", "unbind"} {
		if capabilities[privilege] != false {
			t.Fatalf("expected %s=false for object-only grant calendar, got %#v", privilege, capabilities)
		}
	}
}

func TestGetCalendarNotFound(t *testing.T) {
	handler := NewHandler(&config.Config{}, &store.Store{
		Calendars: &fakeCalendarRepo{calendars: map[int64]*store.CalendarAccess{}},
	}, dav.ValidateCalendarObject)

	req := httptest.NewRequest(http.MethodGet, "/api/calendars/1", nil)
	req = withUserAndRoute(req, "1", "")
	rec := httptest.NewRecorder()

	handler.GetCalendar(rec, req)

	if rec.Code != http.StatusNotFound {
		t.Fatalf("GetCalendar() status = %d, want %d", rec.Code, http.StatusNotFound)
	}
}

func TestListEventsSuccessAndUnauthorized(t *testing.T) {
	handler := NewHandler(&config.Config{}, &store.Store{
		Calendars: &fakeCalendarRepo{calendars: map[int64]*store.CalendarAccess{
			1: {Calendar: store.Calendar{ID: 1, UserID: 1, Name: "Work"}, Editor: true},
		}},
		Events: &fakeEventRepo{events: map[string]store.Event{
			"1:event-1": {CalendarID: 1, UID: "event-1", ResourceName: "event-1", ETag: "e1", LastModified: time.Now().UTC()},
		}},
	}, dav.ValidateCalendarObject)

	req := httptest.NewRequest(http.MethodGet, "/api/calendars/1/events", nil)
	rec := httptest.NewRecorder()
	handler.ListEvents(rec, req)
	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("ListEvents() unauthorized status = %d", rec.Code)
	}

	req = httptest.NewRequest(http.MethodGet, "/api/calendars/1/events", nil)
	req = withUserAndRoute(req, "1", "")
	rec = httptest.NewRecorder()
	handler.ListEvents(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("ListEvents() status = %d", rec.Code)
	}
}

func TestListEventsFilter(t *testing.T) {
	strptr := func(s string) *string { return &s }
	tm := func(s string) *time.Time {
		v, err := time.Parse(time.RFC3339, s)
		if err != nil {
			t.Fatalf("bad time %q: %v", s, err)
		}
		return &v
	}
	handler := NewHandler(&config.Config{}, &store.Store{
		Calendars: &fakeCalendarRepo{calendars: map[int64]*store.CalendarAccess{
			1: {Calendar: store.Calendar{ID: 1, UserID: 1, Name: "Work"}, Editor: true},
		}},
		Events: &fakeEventRepo{events: map[string]store.Event{
			"1:a": {CalendarID: 1, UID: "a", ResourceName: "a", ETag: "e", Summary: strptr("Team standup"), Location: strptr("Room 1"), DTStart: tm("2026-06-10T09:00:00Z")},
			"1:b": {CalendarID: 1, UID: "b", ResourceName: "b", ETag: "e", Summary: strptr("Budget review"), Description: strptr("Discuss Q3 budget"), DTStart: tm("2026-06-20T09:00:00Z")},
			"1:c": {CalendarID: 1, UID: "c", ResourceName: "c", ETag: "e", Summary: strptr("Lunch"), Location: strptr("Cafe Paris"), DTStart: tm("2026-07-01T12:00:00Z")},
		}},
	}, dav.ValidateCalendarObject)

	listUIDs := func(query string) []string {
		req := httptest.NewRequest(http.MethodGet, "/api/calendars/1/events?"+query, nil)
		req = withUserAndRoute(req, "1", "")
		rec := httptest.NewRecorder()
		handler.ListEvents(rec, req)
		if rec.Code != http.StatusOK {
			t.Fatalf("ListEvents(%q) status = %d, body=%s", query, rec.Code, rec.Body.String())
		}
		var out []eventResponse
		if err := json.Unmarshal(rec.Body.Bytes(), &out); err != nil {
			t.Fatalf("decode: %v", err)
		}
		uids := make([]string, len(out))
		for i, ev := range out {
			uids[i] = ev.UID
		}
		return uids
	}

	tests := []struct {
		name  string
		query string
		want  []string
	}{
		{"title", "title=team", []string{"a"}},
		{"description", "description=Q3", []string{"b"}},
		{"location", "location=paris", []string{"c"}},
		{"q across fields", "q=budget", []string{"b"}},
		{"date range", "start=2026-06-15&end=2026-06-30", []string{"b"}},
		{"date-only start orders by dtstart", "start=2026-06-01", []string{"a", "b", "c"}},
		{"no match", "title=nonexistent", []string{}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := listUIDs(tc.query)
			if !reflect.DeepEqual(got, tc.want) {
				t.Fatalf("query %q = %v, want %v", tc.query, got, tc.want)
			}
		})
	}
}

func TestListEventsFilterInvalidParams(t *testing.T) {
	handler := NewHandler(&config.Config{}, &store.Store{
		Calendars: &fakeCalendarRepo{calendars: map[int64]*store.CalendarAccess{
			1: {Calendar: store.Calendar{ID: 1, UserID: 1, Name: "Work"}, Editor: true},
		}},
		Events: &fakeEventRepo{events: map[string]store.Event{}},
	}, dav.ValidateCalendarObject)

	for _, query := range []string{"start=not-a-date", "end=2026-13-40", "limit=-1", "start=2026-06-10&end=2026-06-01"} {
		req := httptest.NewRequest(http.MethodGet, "/api/calendars/1/events?"+query, nil)
		req = withUserAndRoute(req, "1", "")
		rec := httptest.NewRecorder()
		handler.ListEvents(rec, req)
		if rec.Code != http.StatusBadRequest {
			t.Fatalf("ListEvents(%q) status = %d, want 400", query, rec.Code)
		}
	}
}

func TestListEventsInvalidCalendarID(t *testing.T) {
	handler := NewHandler(&config.Config{}, &store.Store{}, dav.ValidateCalendarObject)
	req := httptest.NewRequest(http.MethodGet, "/api/calendars/x/events", nil)
	req = req.WithContext(auth.WithUser(req.Context(), &store.User{ID: 1}))
	routeCtx := chi.NewRouteContext()
	routeCtx.URLParams.Add("id", "x")
	req = req.WithContext(context.WithValue(req.Context(), chi.RouteCtxKey, routeCtx))
	rec := httptest.NewRecorder()

	handler.ListEvents(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("ListEvents() status = %d, want %d", rec.Code, http.StatusBadRequest)
	}
}

func TestDeleteEventInvalidUID(t *testing.T) {
	handler := NewHandler(&config.Config{}, &store.Store{}, dav.ValidateCalendarObject)
	req := httptest.NewRequest(http.MethodDelete, "/api/calendars/1/events/", nil)
	req = req.WithContext(auth.WithUser(req.Context(), &store.User{ID: 1}))
	routeCtx := chi.NewRouteContext()
	routeCtx.URLParams.Add("id", "1")
	routeCtx.URLParams.Add("uid", "")
	req = req.WithContext(context.WithValue(req.Context(), chi.RouteCtxKey, routeCtx))
	rec := httptest.NewRecorder()

	handler.DeleteEvent(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("DeleteEvent() status = %d, want %d", rec.Code, http.StatusBadRequest)
	}
}

func TestGetEventUnauthorizedAndNotFound(t *testing.T) {
	handler := NewHandler(&config.Config{}, &store.Store{
		Calendars: &fakeCalendarRepo{calendars: map[int64]*store.CalendarAccess{
			1: {Calendar: store.Calendar{ID: 1, UserID: 1, Name: "Work"}, Editor: true},
		}},
		Events: &fakeEventRepo{events: map[string]store.Event{}},
	}, dav.ValidateCalendarObject)

	req := httptest.NewRequest(http.MethodGet, "/api/calendars/1/events/event-1", nil)
	rec := httptest.NewRecorder()
	handler.GetEvent(rec, req)
	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("GetEvent() unauthorized status = %d", rec.Code)
	}

	req = httptest.NewRequest(http.MethodGet, "/api/calendars/1/events/event-1", nil)
	req = withUserAndRoute(req, "1", "event-1")
	rec = httptest.NewRecorder()
	handler.GetEvent(rec, req)
	if rec.Code != http.StatusNotFound {
		t.Fatalf("GetEvent() not found status = %d", rec.Code)
	}
}

func TestCreateEventUnauthorizedAndBadJSON(t *testing.T) {
	handler := NewHandler(&config.Config{}, &store.Store{}, dav.ValidateCalendarObject)

	req := httptest.NewRequest(http.MethodPost, "/api/calendars/1/events", strings.NewReader(`{}`))
	rec := httptest.NewRecorder()
	handler.CreateEvent(rec, req)
	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("CreateEvent() unauthorized status = %d", rec.Code)
	}

	req = httptest.NewRequest(http.MethodPost, "/api/calendars/1/events", strings.NewReader(`{`))
	req.Header.Set("Content-Type", "application/json")
	req = withUserAndRoute(req, "1", "")
	rec = httptest.NewRecorder()
	handler.CreateEvent(rec, req)
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("CreateEvent() bad json status = %d", rec.Code)
	}
}

func TestCreateEventRawICSSuccess(t *testing.T) {
	eventRepo := &fakeEventRepo{events: map[string]store.Event{}}
	handler := NewHandler(&config.Config{}, &store.Store{
		Calendars: &fakeCalendarRepo{
			calendars: map[int64]*store.CalendarAccess{
				1: {Calendar: store.Calendar{ID: 1, UserID: 1, Name: "Work"}, Editor: true},
			},
		},
		Events: eventRepo,
	}, dav.ValidateCalendarObject)

	rawICS := "BEGIN:VCALENDAR\r\nVERSION:2.0\r\nBEGIN:VEVENT\r\nUID:raw-1\r\nSUMMARY:Raw\r\nDTSTART:20260320T100000Z\r\nDTEND:20260320T110000Z\r\nEND:VEVENT\r\nEND:VCALENDAR\r\n"
	req := httptest.NewRequest(http.MethodPost, "/api/calendars/1/events", strings.NewReader(rawICS))
	req.Header.Set("Content-Type", "text/calendar")
	req = withUserAndRoute(req, "1", "")
	rec := httptest.NewRecorder()

	handler.CreateEvent(rec, req)

	if rec.Code != http.StatusCreated {
		t.Fatalf("CreateEvent() status = %d, want %d body=%s", rec.Code, http.StatusCreated, rec.Body.String())
	}
	if _, ok := eventRepo.events["1:raw-1"]; !ok {
		t.Fatal("expected raw event stored")
	}
}

// Apple Calendar gives every VALARM its own UID (RFC 9074 §4); only the UID on
// the top-level VEVENT names the object.
func TestRawICSUpsertUsesTheEventUIDNotTheAlarmUIDs(t *testing.T) {
	const eventUID = "6C4E3B5F-2A7D-4E3C-9F1B-8D2A6E7C4B10"
	alarm := func(uid string) string {
		return "BEGIN:VALARM\r\nACTION:DISPLAY\r\nDESCRIPTION:Reminder\r\nTRIGGER:-PT15M\r\nUID:" + uid + "\r\nX-WR-ALARMUID:" + uid + "\r\nEND:VALARM\r\n"
	}
	rawICS := "BEGIN:VCALENDAR\r\nVERSION:2.0\r\nPRODID:-//Apple Inc.//macOS 15.0//EN\r\n" +
		"BEGIN:VEVENT\r\nUID:" + eventUID + "\r\nSUMMARY:Standup\r\nDTSTART:20260320T100000Z\r\nDTEND:20260320T103000Z\r\nRRULE:FREQ=WEEKLY;COUNT=10\r\n" +
		alarm("D1B2C3E4-F5A6-4B7C-8D9E-0F1A2B3C4D5E") + "END:VEVENT\r\n" +
		"BEGIN:VEVENT\r\n" + alarm("B2B2B2B2-0000-4000-8000-000000000002") +
		"UID:" + eventUID + "\r\nRECURRENCE-ID:20260327T100000Z\r\nSUMMARY:Standup (moved)\r\nDTSTART:20260327T110000Z\r\nDTEND:20260327T113000Z\r\nEND:VEVENT\r\n" +
		"END:VCALENDAR\r\n"

	eventRepo := &fakeEventRepo{events: map[string]store.Event{}}
	handler := NewHandler(&config.Config{}, &store.Store{
		Calendars: &fakeCalendarRepo{
			calendars: map[int64]*store.CalendarAccess{
				1: {Calendar: store.Calendar{ID: 1, UserID: 1, Name: "Work"}, Editor: true},
			},
		},
		Events: eventRepo,
	}, dav.ValidateCalendarObject)

	req := httptest.NewRequest(http.MethodPost, "/api/calendars/1/events", strings.NewReader(rawICS))
	req.Header.Set("Content-Type", "text/calendar")
	req = withUserAndRoute(req, "1", "")
	rec := httptest.NewRecorder()
	handler.CreateEvent(rec, req)
	if rec.Code != http.StatusCreated {
		t.Fatalf("CreateEvent() status = %d, want %d body=%s", rec.Code, http.StatusCreated, rec.Body.String())
	}
	stored, ok := eventRepo.events["1:"+eventUID]
	if !ok || len(eventRepo.events) != 1 {
		t.Fatalf("expected one event stored under %q, got %v", eventUID, eventRepo.events)
	}

	req = httptest.NewRequest(http.MethodPut, "/api/calendars/1/events/"+eventUID, strings.NewReader(rawICS))
	req.Header.Set("Content-Type", "text/calendar")
	req.Header.Set("If-Match", `"`+stored.ETag+`"`)
	req = withUserAndRoute(req, "1", eventUID)
	rec = httptest.NewRecorder()
	handler.UpdateEvent(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("UpdateEvent() status = %d, want %d body=%s", rec.Code, http.StatusOK, rec.Body.String())
	}
}

func TestUpdateEventUnauthorizedAndSuccess(t *testing.T) {
	handler := NewHandler(&config.Config{}, &store.Store{}, dav.ValidateCalendarObject)
	req := httptest.NewRequest(http.MethodPut, "/api/calendars/1/events/event-1", strings.NewReader(`{}`))
	rec := httptest.NewRecorder()
	handler.UpdateEvent(rec, req)
	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("UpdateEvent() unauthorized status = %d", rec.Code)
	}

	existing := store.Event{
		CalendarID:   1,
		UID:          "event-1",
		ResourceName: "event-1",
		RawICAL:      "BEGIN:VCALENDAR\r\nVERSION:2.0\r\nBEGIN:VEVENT\r\nUID:event-1\r\nSUMMARY:Old\r\nDTSTART:20260320T100000Z\r\nDTEND:20260320T110000Z\r\nEND:VEVENT\r\nEND:VCALENDAR\r\n",
		ETag:         "current",
		LastModified: time.Now().UTC(),
	}
	handler = NewHandler(&config.Config{}, &store.Store{
		Calendars: &fakeCalendarRepo{calendars: map[int64]*store.CalendarAccess{
			1: {Calendar: store.Calendar{ID: 1, UserID: 1, Name: "Work"}, Editor: true},
		}},
		Events: &fakeEventRepo{events: map[string]store.Event{"1:event-1": existing}},
	}, dav.ValidateCalendarObject)

	req = httptest.NewRequest(http.MethodPut, "/api/calendars/1/events/event-1", strings.NewReader(`{
		"inputMode":"structured",
		"structured":{"uid":"event-1","summary":"Updated","dtstart":"2026-03-20T10:00","dtend":"2026-03-20T11:00"}
	}`))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("If-Match", `"current"`)
	req = withUserAndRoute(req, "1", "event-1")
	rec = httptest.NewRecorder()
	handler.UpdateEvent(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("UpdateEvent() success status = %d body=%s", rec.Code, rec.Body.String())
	}
}

func TestDeleteEventSuccess(t *testing.T) {
	handler := NewHandler(&config.Config{}, &store.Store{
		Calendars: &fakeCalendarRepo{
			calendars: map[int64]*store.CalendarAccess{
				1: {Calendar: store.Calendar{ID: 1, UserID: 1, Name: "Work"}, Editor: true},
			},
		},
		Events: &fakeEventRepo{events: map[string]store.Event{
			"1:event-1": {CalendarID: 1, UID: "event-1", ResourceName: "event-1", ETag: "e1"},
		}},
	}, dav.ValidateCalendarObject)

	req := httptest.NewRequest(http.MethodDelete, "/api/calendars/1/events/event-1", nil)
	req = withUserAndRoute(req, "1", "event-1")
	rec := httptest.NewRecorder()

	handler.DeleteEvent(rec, req)

	if rec.Code != http.StatusNoContent {
		t.Fatalf("DeleteEvent() status = %d, want %d", rec.Code, http.StatusNoContent)
	}
}

func TestDeleteEventUnauthorizedAndNotFound(t *testing.T) {
	handler := NewHandler(&config.Config{}, &store.Store{}, dav.ValidateCalendarObject)
	req := httptest.NewRequest(http.MethodDelete, "/api/calendars/1/events/event-1", nil)
	rec := httptest.NewRecorder()
	handler.DeleteEvent(rec, req)
	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("DeleteEvent() unauthorized status = %d", rec.Code)
	}

	handler = NewHandler(&config.Config{}, &store.Store{
		Calendars: &fakeCalendarRepo{calendars: map[int64]*store.CalendarAccess{
			1: {Calendar: store.Calendar{ID: 1, UserID: 1, Name: "Work"}, Editor: true},
		}},
		Events: &fakeEventRepo{events: map[string]store.Event{}},
	}, dav.ValidateCalendarObject)

	req = httptest.NewRequest(http.MethodDelete, "/api/calendars/1/events/missing", nil)
	req = withUserAndRoute(req, "1", "missing")
	rec = httptest.NewRecorder()
	handler.DeleteEvent(rec, req)
	if rec.Code != http.StatusNotFound {
		t.Fatalf("DeleteEvent() not found status = %d", rec.Code)
	}
}

// A delegate who may read a calendar but not change it is answered 403 for
// every write whose body decodes, whatever it holds: the request is refused
// before the event is built from the body or validated, and nothing is stored.
func TestEventWritesAnswer403BeforeBuildingTheEventForAPrincipalWhoCannotWrite(t *testing.T) {
	delegate := &store.User{ID: 2, PrimaryEmail: "delegate@example.com"}
	storedICS := "BEGIN:VCALENDAR\r\nVERSION:2.0\r\nBEGIN:VEVENT\r\nUID:event-1\r\nDTSTAMP:20260101T000000Z\r\nSUMMARY:Old\r\nDTSTART:20260320T100000Z\r\nDTEND:20260320T110000Z\r\nEND:VEVENT\r\nEND:VCALENDAR\r\n"
	methodICS := "BEGIN:VCALENDAR\r\nVERSION:2.0\r\nMETHOD:REQUEST\r\nBEGIN:VEVENT\r\nUID:event-2\r\nSUMMARY:Planning\r\nDTSTART:20260320T100000Z\r\nDTEND:20260320T110000Z\r\nEND:VEVENT\r\nEND:VCALENDAR\r\n"
	tests := map[string]struct {
		method, uid, contentType, body string
		header                         map[string]string
	}{
		"create structured in an unknown zone": {method: http.MethodPost, contentType: "application/json", body: `{"structured":{"summary":"x","dtstart":"2026-03-20T10:00","dtend":"2026-03-20T11:00","timezone":"Nowhere/Zone"}}`},
		"create structured without a summary":  {method: http.MethodPost, contentType: "application/json", body: `{"structured":{"dtstart":"2026-03-20T10:00","dtend":"2026-03-20T11:00"}}`},
		"create raw iCalendar":                 {method: http.MethodPost, contentType: "text/calendar", body: methodICS},
		"create raw iCalendar in JSON":         {method: http.MethodPost, contentType: "application/json", body: `{"inputMode":"raw_ical","rawIcal":"BEGIN:VCALENDAR"}`},
		"create over a stored UID":             {method: http.MethodPost, contentType: "text/calendar", body: storedICS, header: map[string]string{"If-None-Match": "*"}},
		"update structured with a bad until":   {method: http.MethodPut, uid: "event-1", contentType: "application/json", body: `{"structured":{"summary":"x","dtstart":"2026-03-20T10:00","dtend":"2026-03-20T11:00","recurrence":{"frequency":"DAILY","until":"June"}}}`},
		"update raw iCalendar":                 {method: http.MethodPut, uid: "event-1", contentType: "text/calendar", body: storedICS, header: map[string]string{"If-Match": `"current"`}},
		"update raw iCalendar in JSON":         {method: http.MethodPut, uid: "event-1", contentType: "application/json", body: `{"inputMode":"raw_ical","rawIcal":"BEGIN:VCALENDAR"}`, header: map[string]string{"If-Match": `"current"`}},
		"delete":                               {method: http.MethodDelete, uid: "event-1", header: map[string]string{"If-Match": `"current"`}},
	}
	for name, tt := range tests {
		t.Run(name, func(t *testing.T) {
			validations := 0
			validate := func(raw string, cal *store.CalendarAccess) (*calobject.Object, error) {
				validations++
				return dav.ValidateCalendarObject(raw, cal)
			}
			eventRepo := &fakeEventRepo{events: map[string]store.Event{
				"1:event-1": {CalendarID: 1, UID: "event-1", ResourceName: "event-1", RawICAL: storedICS, ETag: "current", LastModified: time.Now().UTC()},
			}}
			handler := NewHandler(&config.Config{}, &store.Store{
				Calendars: &fakeCalendarRepo{calendars: map[int64]*store.CalendarAccess{
					1: {Calendar: store.Calendar{ID: 1, UserID: 1, Name: "Shared"}, Shared: true, PrivilegesResolved: true, Privileges: store.CalendarPrivileges{Read: true, ReadFreeBusy: true}},
				}},
				Events: eventRepo,
				ACLEntries: &fakeACLRepo{entries: []store.ACLEntry{
					{ResourcePath: "/dav/calendars/1", PrincipalHref: "/dav/principals/2/", IsGrant: true, Privilege: "read"},
				}},
			}, validate)

			req := httptest.NewRequest(tt.method, "/api/calendars/1/events/"+tt.uid, strings.NewReader(tt.body))
			if tt.contentType != "" {
				req.Header.Set("Content-Type", tt.contentType)
			}
			for header, value := range tt.header {
				req.Header.Set(header, value)
			}
			req = withUserAndRoute(req, "1", tt.uid)
			req = req.WithContext(auth.WithUser(req.Context(), delegate))
			rec := httptest.NewRecorder()
			switch tt.method {
			case http.MethodPost:
				handler.CreateEvent(rec, req)
			case http.MethodPut:
				handler.UpdateEvent(rec, req)
			case http.MethodDelete:
				handler.DeleteEvent(rec, req)
			}

			if rec.Code != http.StatusForbidden {
				t.Fatalf("status = %d, want %d, body=%s", rec.Code, http.StatusForbidden, rec.Body.String())
			}
			if validations != 0 {
				t.Fatalf("validator ran %d times for a principal who cannot write", validations)
			}
			if stored, ok := eventRepo.events["1:event-1"]; len(eventRepo.events) != 1 || !ok || stored.ETag != "current" {
				t.Fatalf("stored events = %#v, want the stored event alone and unchanged", eventRepo.events)
			}
		})
	}
}

// The body is decoded before any privilege is checked, so a body that does not
// decode is answered 400 even for a principal who can see nothing of the
// calendar; one that decodes is answered as that principal's access decides.
// Decoding reveals nothing of the calendar, and costs far less than building
// and validating the event, which comes after the privilege check.
func TestEventWriteBodiesThatDoNotDecodeAnswer400BeforeAnyPrivilegeCheck(t *testing.T) {
	stranger := &store.User{ID: 3}
	valid := `{"structured":{"summary":"x","dtstart":"2026-03-20T10:00","dtend":"2026-03-20T11:00"}}`
	tests := map[string]struct {
		body string
		want int
	}{
		"empty body":                    {body: "", want: http.StatusBadRequest},
		"not JSON":                      {body: "{", want: http.StatusBadRequest},
		"unknown field":                 {body: `{"structured":{"summary":"x"},"extra":1}`, want: http.StatusBadRequest},
		"unknown input mode":            {body: `{"inputMode":"vcard"}`, want: http.StatusBadRequest},
		"raw mode without iCalendar":    {body: `{"inputMode":"raw_ical","rawIcal":" "}`, want: http.StatusBadRequest},
		"structured mode without input": {body: `{"inputMode":"structured"}`, want: http.StatusBadRequest},
		"a body that decodes":           {body: valid, want: http.StatusNotFound},
	}
	for name, tt := range tests {
		for _, method := range []string{http.MethodPost, http.MethodPut} {
			t.Run(name+" "+method, func(t *testing.T) {
				handler := NewHandler(&config.Config{}, &store.Store{
					Calendars: &fakeCalendarRepo{calendars: map[int64]*store.CalendarAccess{
						1: {Calendar: store.Calendar{ID: 1, UserID: 1, Name: "Private"}},
					}},
					Events: &fakeEventRepo{events: map[string]store.Event{
						"1:event-1": {CalendarID: 1, UID: "event-1", ResourceName: "event-1", ETag: "current", LastModified: time.Now().UTC()},
					}},
					ACLEntries: &fakeACLRepo{},
				}, dav.ValidateCalendarObject)
				uid := ""
				if method == http.MethodPut {
					uid = "event-1"
				}
				req := httptest.NewRequest(method, "/api/calendars/1/events/"+uid, strings.NewReader(tt.body))
				req.Header.Set("Content-Type", "application/json")
				req = withUserAndRoute(req, "1", uid)
				req = req.WithContext(auth.WithUser(req.Context(), stranger))
				rec := httptest.NewRecorder()
				if method == http.MethodPost {
					handler.CreateEvent(rec, req)
				} else {
					handler.UpdateEvent(rec, req)
				}
				if rec.Code != tt.want {
					t.Fatalf("status = %d, want %d, body=%s", rec.Code, tt.want, rec.Body.String())
				}
			})
		}
	}
}

// Updates and deletes check the privileges first and the precondition after,
// and a principal the event is hidden from gets the same status and body
// whether or not the UID exists. The principals and the answers are those of
// eventWriteAccessCases in internal/events/service_test.go, which says why
// each answer is what it is and which of them DAV DELETE answers otherwise;
// test packages cannot share it, so a change to one belongs in both.
func TestEventUpdatesAndDeletesAnswerAPrincipalWhoCannotSeeTheEventAlikeWhetherOrNotItExists(t *testing.T) {
	const (
		ok          = http.StatusOK
		noContent   = http.StatusNoContent
		forbidden   = http.StatusForbidden
		notFound    = http.StatusNotFound
		failed      = http.StatusPreconditionFailed
		collection  = "/dav/calendars/1"
		eventObject = "/dav/calendars/1/stored"
	)
	storedICS := func(uid string) string {
		return "BEGIN:VCALENDAR\r\nVERSION:2.0\r\nBEGIN:VEVENT\r\nUID:" + uid + "\r\nDTSTAMP:20260101T000000Z\r\nSUMMARY:Planning\r\nDTSTART:20260320T100000Z\r\nDTEND:20260320T110000Z\r\nEND:VEVENT\r\nEND:VCALENDAR\r\n"
	}
	acl := []store.ACLEntry{
		{ResourcePath: collection, PrincipalHref: "/dav/principals/2/", IsGrant: true, Privilege: "read"},
		{ResourcePath: eventObject, PrincipalHref: "/dav/principals/4/", IsGrant: true, Privilege: "read"},
		{ResourcePath: eventObject, PrincipalHref: "/dav/principals/4/", IsGrant: true, Privilege: "write-content"},
		{ResourcePath: eventObject, PrincipalHref: "/dav/principals/4/", IsGrant: true, Privilege: "unbind"},
		{ResourcePath: collection, PrincipalHref: "/dav/principals/5/", IsGrant: true, Privilege: "read"},
		{ResourcePath: collection, PrincipalHref: "/dav/principals/5/", IsGrant: true, Privilege: "write-content"},
		{ResourcePath: collection, PrincipalHref: "/dav/principals/5/", IsGrant: true, Privilege: "unbind"},
		{ResourcePath: eventObject, PrincipalHref: "/dav/principals/6/", IsGrant: false, Privilege: "all"},
		{ResourcePath: collection, PrincipalHref: "/dav/principals/6/", IsGrant: true, Privilege: "read"},
		{ResourcePath: eventObject, PrincipalHref: "/dav/principals/7/", IsGrant: false, Privilege: "all"},
		{ResourcePath: collection, PrincipalHref: "/dav/principals/7/", IsGrant: true, Privilege: "read"},
		{ResourcePath: collection, PrincipalHref: "/dav/principals/7/", IsGrant: true, Privilege: "write-content"},
		{ResourcePath: collection, PrincipalHref: "/dav/principals/7/", IsGrant: true, Privilege: "unbind"},
		{ResourcePath: eventObject, PrincipalHref: "/dav/principals/8/", IsGrant: false, Privilege: "unbind"},
		{ResourcePath: collection, PrincipalHref: "/dav/principals/8/", IsGrant: true, Privilege: "all"},
		{ResourcePath: collection, PrincipalHref: "/dav/principals/9/", IsGrant: false, Privilege: "all"},
		{ResourcePath: collection, PrincipalHref: "/dav/principals/10/", IsGrant: true, Privilege: "read-acl"},
		{ResourcePath: eventObject, PrincipalHref: "/dav/principals/11/", IsGrant: true, Privilege: "read"},
		{ResourcePath: collection, PrincipalHref: "/dav/principals/11/", IsGrant: false, Privilege: "all"},
	}
	headers := []struct{ name, header, value string }{
		{name: "unconditional"},
		{name: "If-Match", header: "If-Match", value: `"etag"`},
		{name: "If-None-Match *", header: "If-None-Match", value: "*"},
	}
	type statuses struct{ stored, missing int }
	tests := []struct {
		principal string
		userID    int64
		concealed bool
		// update and delete list statuses in the order of headers.
		update, delete [3]statuses
	}{
		{principal: "no access", userID: 3, concealed: true,
			update: [3]statuses{{notFound, notFound}, {notFound, notFound}, {notFound, notFound}},
			delete: [3]statuses{{notFound, notFound}, {notFound, notFound}, {notFound, notFound}}},
		{principal: "read-only", userID: 2,
			update: [3]statuses{{forbidden, forbidden}, {forbidden, forbidden}, {forbidden, forbidden}},
			delete: [3]statuses{{forbidden, forbidden}, {forbidden, forbidden}, {forbidden, forbidden}}},
		{principal: "calendar reader the event is hidden from", userID: 6, concealed: true,
			update: [3]statuses{{forbidden, forbidden}, {forbidden, forbidden}, {forbidden, forbidden}},
			delete: [3]statuses{{forbidden, forbidden}, {forbidden, forbidden}, {forbidden, forbidden}}},
		{principal: "calendar writer the event is hidden from", userID: 7, concealed: true,
			update: [3]statuses{{notFound, notFound}, {notFound, notFound}, {notFound, notFound}},
			delete: [3]statuses{{notFound, notFound}, {notFound, notFound}, {notFound, notFound}}},
		{principal: "event grant", userID: 4,
			update: [3]statuses{{ok, notFound}, {ok, notFound}, {failed, notFound}},
			delete: [3]statuses{{forbidden, notFound}, {forbidden, notFound}, {forbidden, notFound}}},
		{principal: "write", userID: 5,
			update: [3]statuses{{ok, notFound}, {ok, notFound}, {failed, notFound}},
			delete: [3]statuses{{noContent, notFound}, {noContent, notFound}, {failed, notFound}}},
		{principal: "calendar all, event unbind denied", userID: 8,
			update: [3]statuses{{ok, notFound}, {ok, notFound}, {failed, notFound}},
			delete: [3]statuses{{noContent, notFound}, {noContent, notFound}, {failed, notFound}}},
		{principal: "calendar deny all", userID: 9, concealed: true,
			update: [3]statuses{{notFound, notFound}, {notFound, notFound}, {notFound, notFound}},
			delete: [3]statuses{{notFound, notFound}, {notFound, notFound}, {notFound, notFound}}},
		{principal: "calendar read-acl only", userID: 10, concealed: true,
			update: [3]statuses{{notFound, notFound}, {notFound, notFound}, {notFound, notFound}},
			delete: [3]statuses{{notFound, notFound}, {notFound, notFound}, {notFound, notFound}}},
		{principal: "event read, calendar deny all", userID: 11,
			update: [3]statuses{{forbidden, notFound}, {forbidden, notFound}, {forbidden, notFound}},
			delete: [3]statuses{{forbidden, notFound}, {forbidden, notFound}, {forbidden, notFound}}},
	}
	for _, tt := range tests {
		for _, method := range []string{http.MethodPut, http.MethodDelete} {
			want := tt.update
			if method == http.MethodDelete {
				want = tt.delete
			}
			for i, header := range headers {
				t.Run(tt.principal+"/"+method+"/"+header.name, func(t *testing.T) {
					status := map[string]int{}
					body := map[string]string{}
					for _, uid := range []string{"stored", "missing"} {
						eventRepo := &fakeEventRepo{events: map[string]store.Event{
							"1:stored": {CalendarID: 1, UID: "stored", ResourceName: "stored", RawICAL: storedICS("stored"), ETag: "etag", LastModified: time.Now().UTC()},
						}}
						handler := NewHandler(&config.Config{}, &store.Store{
							Calendars: &fakeCalendarRepo{calendars: map[int64]*store.CalendarAccess{
								1: {Calendar: store.Calendar{ID: 1, UserID: 1, Name: "Private"}},
							}},
							Events:     eventRepo,
							ACLEntries: &fakeACLRepo{entries: slices.Clone(acl)},
						}, dav.ValidateCalendarObject)

						var req *http.Request
						if method == http.MethodPut {
							req = httptest.NewRequest(method, "/api/calendars/1/events/"+uid, strings.NewReader(storedICS(uid)))
							req.Header.Set("Content-Type", "text/calendar")
						} else {
							req = httptest.NewRequest(method, "/api/calendars/1/events/"+uid, nil)
						}
						if header.header != "" {
							req.Header.Set(header.header, header.value)
						}
						req = withUserAndRoute(req, "1", uid)
						req = req.WithContext(auth.WithUser(req.Context(), &store.User{ID: tt.userID}))
						rec := httptest.NewRecorder()
						if method == http.MethodPut {
							handler.UpdateEvent(rec, req)
						} else {
							handler.DeleteEvent(rec, req)
						}
						status[uid], body[uid] = rec.Code, rec.Body.String()
						if rec.Code >= 300 {
							if stored, found := eventRepo.events["1:stored"]; !found || stored.ETag != "etag" || len(eventRepo.events) != 1 {
								t.Errorf("%s %s answered %d but changed the calendar: %#v", method, uid, rec.Code, eventRepo.events)
							}
						}
					}
					if got := (statuses{status["stored"], status["missing"]}); got != want[i] {
						t.Fatalf("statuses for the stored and a missing UID = %v, want %v; bodies %q", got, want[i], body)
					}
					if tt.concealed && body["stored"] != body["missing"] {
						t.Fatalf("bodies differ by existence: %q for the stored UID, %q for a missing one", body["stored"], body["missing"])
					}
				})
			}
		}
	}
}

// A structured write answers 400 for a timezone the server's database does not
// define or a recurrence until that is not a YYYY-MM-DD date, rather than
// storing the event in UTC or as a series without an end.
func TestStructuredEventWritesAnswer400ForAZoneOrUntilTheServerCannotRead(t *testing.T) {
	existing := store.Event{CalendarID: 1, UID: "event-1", ResourceName: "event-1", RawICAL: "BEGIN:VCALENDAR\r\nVERSION:2.0\r\nBEGIN:VEVENT\r\nUID:event-1\r\nDTSTAMP:20260101T000000Z\r\nSUMMARY:Old\r\nDTSTART:20260320T100000Z\r\nDTEND:20260320T110000Z\r\nEND:VEVENT\r\nEND:VCALENDAR\r\n", ETag: "current", LastModified: time.Now().UTC()}
	for name, structured := range map[string]string{
		"unknown timezone": `{"summary":"x","dtstart":"2026-03-20T10:00","dtend":"2026-03-20T11:00","timezone":"Eastern Standard Time"}`,
		"malformed until":  `{"summary":"x","dtstart":"2026-03-20T10:00","dtend":"2026-03-20T11:00","recurrence":{"frequency":"WEEKLY","until":"2026-06-31"}}`,
	} {
		for _, method := range []string{http.MethodPost, http.MethodPut} {
			t.Run(name+" "+method, func(t *testing.T) {
				eventRepo := &fakeEventRepo{events: map[string]store.Event{"1:event-1": existing}}
				handler := NewHandler(&config.Config{}, &store.Store{
					Calendars: &fakeCalendarRepo{calendars: map[int64]*store.CalendarAccess{
						1: {Calendar: store.Calendar{ID: 1, UserID: 1, Name: "Work"}, Editor: true},
					}},
					Events: eventRepo,
				}, dav.ValidateCalendarObject)

				uid := ""
				if method == http.MethodPut {
					uid = "event-1"
				}
				req := httptest.NewRequest(method, "/api/calendars/1/events/"+uid, strings.NewReader(`{"inputMode":"structured","structured":`+structured+`}`))
				req.Header.Set("Content-Type", "application/json")
				req = withUserAndRoute(req, "1", uid)
				rec := httptest.NewRecorder()
				if method == http.MethodPost {
					handler.CreateEvent(rec, req)
				} else {
					handler.UpdateEvent(rec, req)
				}

				if rec.Code != http.StatusBadRequest {
					t.Fatalf("status = %d, want %d, body=%s", rec.Code, http.StatusBadRequest, rec.Body.String())
				}
				if len(eventRepo.events) != 1 || eventRepo.events["1:event-1"].ETag != "current" {
					t.Fatalf("stored events = %#v, want the stored event alone and unchanged", eventRepo.events)
				}
			})
		}
	}
}

func TestDecodeUpsertInputBranches(t *testing.T) {
	t.Run("invalid json", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodPost, "/", strings.NewReader("{"))
		req.Header.Set("Content-Type", "application/json")
		_, err := decodeUpsertInput(req)
		if err == nil {
			t.Fatal("expected error")
		}
	})

	t.Run("invalid input mode", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodPost, "/", strings.NewReader(`{"inputMode":"weird"}`))
		req.Header.Set("Content-Type", "application/json")
		_, err := decodeUpsertInput(req)
		if err == nil {
			t.Fatal("expected error")
		}
	})

	t.Run("missing structured", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodPost, "/", strings.NewReader(`{"inputMode":"structured"}`))
		req.Header.Set("Content-Type", "application/json")
		_, err := decodeUpsertInput(req)
		if err == nil {
			t.Fatal("expected error")
		}
	})

	t.Run("missing raw", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodPost, "/", strings.NewReader(`{"inputMode":"raw_ical","rawIcal":"   "}`))
		req.Header.Set("Content-Type", "application/json")
		_, err := decodeUpsertInput(req)
		if err == nil {
			t.Fatal("expected error")
		}
	})

	t.Run("raw content type", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodPost, "/", strings.NewReader("BEGIN:VCALENDAR\r\nEND:VCALENDAR\r\n"))
		req.Header.Set("Content-Type", "text/calendar")
		input, err := decodeUpsertInput(req)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if input.RawICS == "" {
			t.Fatal("expected raw ICS")
		}
	})

	t.Run("empty body", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodPost, "/", nil)
		req.Header.Set("Content-Type", "application/json")
		_, err := decodeUpsertInput(req)
		if err == nil {
			t.Fatal("expected error")
		}
	})

	t.Run("structured success default mode", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodPost, "/", strings.NewReader(`{"structured":{"summary":"x","dtstart":"2026-03-20T10:00","dtend":"2026-03-20T11:00"}}`))
		req.Header.Set("Content-Type", "application/json")
		input, err := decodeUpsertInput(req)
		if err != nil || input.Structured == nil {
			t.Fatalf("unexpected result err=%v input=%+v", err, input)
		}
	})

	t.Run("body too large", func(t *testing.T) {
		huge := strings.Repeat("a", int(events.MaxBodyBytes)+2)
		req := httptest.NewRequest(http.MethodPost, "/", strings.NewReader(huge))
		req.Header.Set("Content-Type", "text/calendar")
		_, err := decodeUpsertInput(req)
		if err == nil {
			t.Fatal("expected error")
		}
	})
}

func TestHelperFunctions(t *testing.T) {
	t.Run("parse uid fallback", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/", nil)
		req = req.WithContext(auth.WithUser(req.Context(), &store.User{ID: 1}))
		routeCtx := chi.NewRouteContext()
		routeCtx.URLParams.Add("id", "1")
		routeCtx.URLParams.Add("uid", "%zz")
		req = req.WithContext(context.WithValue(req.Context(), chi.RouteCtxKey, routeCtx))
		rec := httptest.NewRecorder()

		_, uid, ok := parseCalendarIDAndUID(rec, req)
		if !ok || uid != "%zz" {
			t.Fatalf("unexpected parse result ok=%v uid=%q", ok, uid)
		}
	})

	t.Run("write event error internal", func(t *testing.T) {
		rec := httptest.NewRecorder()
		writeEventError(rec, errors.New("boom"))
		if rec.Code != http.StatusInternalServerError {
			t.Fatalf("writeEventError() status = %d", rec.Code)
		}
	})

	t.Run("parse calendar id success", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/", nil)
		req = req.WithContext(auth.WithUser(req.Context(), &store.User{ID: 1}))
		routeCtx := chi.NewRouteContext()
		routeCtx.URLParams.Add("id", "1")
		req = req.WithContext(context.WithValue(req.Context(), chi.RouteCtxKey, routeCtx))
		rec := httptest.NewRecorder()
		id, ok := parseCalendarID(rec, req)
		if !ok || id != 1 {
			t.Fatalf("unexpected parse result ok=%v id=%d", ok, id)
		}
	})

	t.Run("to event response nil dates", func(t *testing.T) {
		resp := toEventResponse(store.Event{UID: "u1", CalendarID: 1, ResourceName: "u1", ETag: "e1", LastModified: time.Unix(0, 0).UTC()})
		if resp.DTStart != nil || resp.DTEnd != nil {
			t.Fatal("expected nil date fields")
		}
	})

	t.Run("write json", func(t *testing.T) {
		rec := httptest.NewRecorder()
		writeJSON(rec, http.StatusCreated, map[string]string{"ok": "true"})
		if rec.Code != http.StatusCreated {
			t.Fatalf("writeJSON() status = %d", rec.Code)
		}
		if ct := rec.Header().Get("Content-Type"); !strings.Contains(ct, "application/json") {
			t.Fatalf("unexpected content type %q", ct)
		}
	})

	t.Run("fmt bad request", func(t *testing.T) {
		err := fmtBadRequest(errors.New("x"))
		if !errors.Is(err, events.ErrBadRequest) {
			t.Fatalf("expected bad request wrapper, got %v", err)
		}
	})
}

func withUserAndRoute(req *http.Request, calendarID, uid string) *http.Request {
	req = req.WithContext(auth.WithUser(req.Context(), &store.User{ID: 1, PrimaryEmail: "user@example.com"}))
	routeCtx := chi.NewRouteContext()
	routeCtx.URLParams.Add("id", calendarID)
	if uid != "" {
		routeCtx.URLParams.Add("uid", uid)
	}
	return req.WithContext(context.WithValue(req.Context(), chi.RouteCtxKey, routeCtx))
}

type fakeCalendarRepo struct {
	calendars         map[int64]*store.CalendarAccess
	listAccessibleErr error
	getAccessibleErr  error
}

func (f *fakeCalendarRepo) GetByID(ctx context.Context, id int64) (*store.Calendar, error) {
	if cal, ok := f.calendars[id]; ok {
		copy := cal.Calendar
		return &copy, nil
	}
	return nil, nil
}
func (f *fakeCalendarRepo) ListByUser(ctx context.Context, userID int64) ([]store.Calendar, error) {
	var out []store.Calendar
	for _, cal := range f.calendars {
		if cal.UserID == userID {
			out = append(out, cal.Calendar)
		}
	}
	return out, nil
}
func (f *fakeCalendarRepo) ListAccessible(ctx context.Context, userID int64) ([]store.CalendarAccess, error) {
	if f.listAccessibleErr != nil {
		return nil, f.listAccessibleErr
	}
	var out []store.CalendarAccess
	for _, cal := range f.calendars {
		if cal.UserID == userID || cal.Shared {
			out = append(out, *cal)
		}
	}
	return out, nil
}
func (f *fakeCalendarRepo) GetAccessible(ctx context.Context, calendarID, userID int64) (*store.CalendarAccess, error) {
	if f.getAccessibleErr != nil {
		return nil, f.getAccessibleErr
	}
	if cal, ok := f.calendars[calendarID]; ok && (cal.UserID == userID || cal.Shared) {
		copy := *cal
		return &copy, nil
	}
	return nil, nil
}
func (f *fakeCalendarRepo) Create(ctx context.Context, cal store.Calendar) (*store.Calendar, error) {
	return nil, nil
}
func (f *fakeCalendarRepo) Update(ctx context.Context, userID, id int64, name string, description, timezone, color *string) error {
	return nil
}
func (f *fakeCalendarRepo) UpdateProperties(ctx context.Context, id int64, props store.CalendarProperties) error {
	return nil
}
func (f *fakeCalendarRepo) Rename(ctx context.Context, userID, id int64, name string) error {
	return nil
}
func (f *fakeCalendarRepo) Delete(ctx context.Context, userID, id int64) error {
	return nil
}

type fakeEventRepo struct {
	events map[string]store.Event
}

func (f *fakeEventRepo) Upsert(ctx context.Context, event store.Event) (*store.Event, error) {
	event.LastModified = time.Now().UTC()
	if event.ResourceName == "" {
		event.ResourceName = event.UID
	}
	f.events[key(event.CalendarID, event.UID)] = event
	copy := event
	return &copy, nil
}
func (f *fakeEventRepo) DeleteByUID(ctx context.Context, calendarID int64, uid string) error {
	delete(f.events, key(calendarID, uid))
	return nil
}
func (f *fakeEventRepo) GetByUID(ctx context.Context, calendarID int64, uid string) (*store.Event, error) {
	ev, ok := f.events[key(calendarID, uid)]
	if !ok {
		return nil, nil
	}
	copy := ev
	return &copy, nil
}
func (f *fakeEventRepo) GetByResourceName(ctx context.Context, calendarID int64, resourceName string) (*store.Event, error) {
	for _, ev := range f.events {
		if ev.CalendarID == calendarID && ev.ResourceName == resourceName {
			copy := ev
			return &copy, nil
		}
	}
	return nil, nil
}
func (f *fakeEventRepo) ListByResourceNames(ctx context.Context, calendarID int64, resourceNames []string) ([]store.Event, error) {
	var out []store.Event
	for _, name := range resourceNames {
		ev, err := f.GetByResourceName(ctx, calendarID, name)
		if err != nil {
			return nil, err
		}
		if ev != nil {
			out = append(out, *ev)
		}
	}
	return out, nil
}
func (f *fakeEventRepo) ListForCalendar(ctx context.Context, calendarID int64) ([]store.Event, error) {
	var out []store.Event
	for _, ev := range f.events {
		if ev.CalendarID == calendarID {
			out = append(out, ev)
		}
	}
	return out, nil
}
func (f *fakeEventRepo) ListForCalendarFiltered(ctx context.Context, calendarID int64, filter store.EventFilter) ([]store.Event, error) {
	all, _ := f.ListForCalendar(ctx, calendarID)
	contains := func(p *string, term string) bool {
		return p != nil && strings.Contains(strings.ToLower(*p), strings.ToLower(term))
	}
	var out []store.Event
	for _, ev := range all {
		if filter.Start != nil {
			end := ev.DTEnd
			if end == nil {
				end = ev.DTStart
			}
			if end == nil || end.Before(*filter.Start) {
				continue
			}
		}
		if filter.End != nil && (ev.DTStart == nil || ev.DTStart.After(*filter.End)) {
			continue
		}
		if filter.Title != "" && !contains(ev.Summary, filter.Title) {
			continue
		}
		if filter.Description != "" && !contains(ev.Description, filter.Description) {
			continue
		}
		if filter.Location != "" && !contains(ev.Location, filter.Location) {
			continue
		}
		if filter.Query != "" &&
			!contains(ev.Summary, filter.Query) &&
			!contains(ev.Description, filter.Query) &&
			!contains(ev.Location, filter.Query) {
			continue
		}
		out = append(out, ev)
	}
	sort.SliceStable(out, func(i, j int) bool {
		switch {
		case out[i].DTStart == nil && out[j].DTStart == nil:
			return out[i].LastModified.After(out[j].LastModified)
		case out[i].DTStart == nil:
			return false
		case out[j].DTStart == nil:
			return true
		default:
			return out[i].DTStart.Before(*out[j].DTStart)
		}
	})
	return out, nil
}

func (f *fakeEventRepo) ListForCalendarPageAfter(ctx context.Context, calendarID, afterID int64, limit int, filter store.EventFilter) ([]store.Event, error) {
	events, err := f.ListForCalendarFiltered(ctx, calendarID, filter)
	if err != nil {
		return nil, err
	}
	var result []store.Event
	for _, event := range events {
		if event.ID > afterID {
			result = append(result, event)
		}
	}
	if limit >= 0 && len(result) > limit {
		result = result[:limit]
	}
	return result, nil
}
func (f *fakeEventRepo) ListForCalendarPaginated(ctx context.Context, calendarID int64, limit, offset int) (*store.PaginatedResult[store.Event], error) {
	return nil, nil
}
func (f *fakeEventRepo) ListByUIDs(ctx context.Context, calendarID int64, uids []string) ([]store.Event, error) {
	return nil, nil
}
func (f *fakeEventRepo) ListChangesAfter(ctx context.Context, collectionID int64, after store.SyncCursor, removals bool, limit int) (store.SyncChanges[store.Event], error) {
	return store.SyncChanges[store.Event]{Last: after}, nil
}
func (f *fakeEventRepo) ListRecentByUser(ctx context.Context, userID int64, limit int) ([]store.Event, error) {
	return nil, nil
}
func (f *fakeEventRepo) MaxLastModified(ctx context.Context, calendarID int64) (time.Time, error) {
	return time.Time{}, nil
}
func (f *fakeEventRepo) MoveToCalendar(ctx context.Context, fromCalendarID, toCalendarID int64, uid, destResourceName string) error {
	return nil
}
func key(calendarID int64, uid string) string {
	return strconv.FormatInt(calendarID, 10) + ":" + uid
}

// chi routes on r.URL.RawPath when it is set and on the already-decoded
// r.URL.Path when it is not, so a {uid} reaches the handler encoded only in the
// first case. Unescaping it unconditionally decodes the second case twice, and
// "literal%2541" then names "literalA" -- a different resource, which the
// update and delete routes below would mutate.
func TestUIDRouteParamsDecodeExactlyOnceThroughChiRouting(t *testing.T) {
	tests := []struct {
		name    string
		segment string
		want    string
	}{
		{name: "escaped percent", segment: "literal%2541", want: `literal%41`},
		{name: "decoded twin", segment: "literalA", want: "literalA"},
		{name: "escaped at sign", segment: "a%40b.com", want: "a@b.com"},
		{name: "literal at sign", segment: "a@b.com", want: "a@b.com"},
		{name: "escaped space", segment: "sp%20ace", want: "sp ace"},
		{name: "plain", segment: "plain-uid", want: "plain-uid"},
	}

	// Both readers, behind the route shapes the server mounts them on.
	readers := map[string]struct {
		pattern string
		read    func(http.ResponseWriter, *http.Request) (int64, string, bool)
	}{
		"events":   {pattern: "/calendars/{id}/events/{uid}", read: parseCalendarIDAndUID},
		"contacts": {pattern: "/addressbooks/{id}/contacts/{uid}", read: parseBookIDAndUID},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			for name, reader := range readers {
				var got string
				var ok bool
				router := chi.NewRouter()
				router.Get(reader.pattern, func(w http.ResponseWriter, r *http.Request) {
					_, got, ok = reader.read(w, r)
				})

				target := strings.Replace(reader.pattern, "{id}", "1", 1)
				target = strings.Replace(target, "{uid}", tt.segment, 1)
				router.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(http.MethodGet, target, nil))

				if !ok {
					t.Errorf("%s rejected %q", name, tt.segment)
					continue
				}
				if got != tt.want {
					t.Errorf("%s with %q resolved to %q, want %q", name, tt.segment, got, tt.want)
				}
			}
		})
	}
}

// A structured event's start and end have the shape allDay says: dates for an
// all-day event, date-times for a timed one. A value of the other shape is
// refused with 400 rather than dropped from the event written.
func TestCreateEventStructuredRefusesDatesOfTheWrongShape(t *testing.T) {
	for name, body := range map[string]string{
		"all-day with a timed end":   `{"inputMode":"structured","structured":{"summary":"Trip","allDay":true,"dtstart":"2026-03-20","dtend":"2026-03-25T10:00"}}`,
		"all-day with a timed start": `{"inputMode":"structured","structured":{"summary":"Trip","allDay":true,"dtstart":"2026-03-20T10:00","dtend":"2026-03-25"}}`,
		"timed with a date end":      `{"inputMode":"structured","structured":{"summary":"Call","allDay":false,"dtstart":"2026-03-20T10:00","dtend":"2026-03-21"}}`,
	} {
		t.Run(name, func(t *testing.T) {
			eventRepo := &fakeEventRepo{events: map[string]store.Event{}}
			handler := NewHandler(&config.Config{}, &store.Store{
				Calendars: &fakeCalendarRepo{calendars: map[int64]*store.CalendarAccess{
					1: {Calendar: store.Calendar{ID: 1, UserID: 1, Name: "Work"}, Editor: true},
				}},
				Events: eventRepo,
			}, dav.ValidateCalendarObject)
			req := httptest.NewRequest(http.MethodPost, "/api/calendars/1/events", strings.NewReader(body))
			req.Header.Set("Content-Type", "application/json")
			rec := httptest.NewRecorder()
			handler.CreateEvent(rec, withUserAndRoute(req, "1", ""))
			if rec.Code != http.StatusBadRequest {
				t.Fatalf("CreateEvent() status = %d, want 400, body=%s", rec.Code, rec.Body.String())
			}
			if len(eventRepo.events) != 0 {
				t.Errorf("stored %v", eventRepo.events)
			}
		})
	}
}

// Calendar data the validator refuses is a bad request, which the client fixes
// by sending other data. Only a body that is a scheduling message (METHOD) or
// names more than one UID, and so cannot be the one resource the request
// writes, conflicts with the target.
func TestCreateEventRawICSRefusalStatuses(t *testing.T) {
	event := func(lines ...string) string {
		return "BEGIN:VCALENDAR\r\nVERSION:2.0\r\nPRODID:-//Test//EN\r\n" + strings.Join(lines, "") + "END:VCALENDAR\r\n"
	}
	vevent := func(lines ...string) string {
		return "BEGIN:VEVENT\r\n" + strings.Join(lines, "\r\n") + "\r\nEND:VEVENT\r\n"
	}
	tests := map[string]struct {
		body string
		want int
	}{
		"no UID":          {event(vevent("DTSTAMP:20260101T000000Z", "DTSTART:20260320T100000Z")), http.StatusBadRequest},
		"empty UID":       {event(vevent("UID:", "DTSTAMP:20260101T000000Z", "DTSTART:20260320T100000Z")), http.StatusBadRequest},
		"two masters":     {event(vevent("UID:a", "DTSTART:20260320T100000Z"), vevent("UID:a", "DTSTART:20260321T100000Z")), http.StatusBadRequest},
		"event with task": {event(vevent("UID:a", "DTSTART:20260320T100000Z"), "BEGIN:VTODO\r\nUID:a\r\nEND:VTODO\r\n"), http.StatusBadRequest},
		"METHOD":          {strings.Replace(event(vevent("UID:a", "DTSTART:20260320T100000Z")), "VERSION:2.0", "METHOD:REQUEST\r\nVERSION:2.0", 1), http.StatusConflict},
		"two UIDs":        {event(vevent("UID:a", "DTSTART:20260320T100000Z"), vevent("UID:b", "DTSTART:20260321T100000Z")), http.StatusConflict},
	}
	for name, tt := range tests {
		t.Run(name, func(t *testing.T) {
			handler := NewHandler(&config.Config{}, &store.Store{
				Calendars: &fakeCalendarRepo{calendars: map[int64]*store.CalendarAccess{
					1: {Calendar: store.Calendar{ID: 1, UserID: 1, Name: "Work"}, Editor: true},
				}},
				Events: &fakeEventRepo{events: map[string]store.Event{}},
			}, dav.ValidateCalendarObject)
			req := httptest.NewRequest(http.MethodPost, "/api/calendars/1/events", strings.NewReader(tt.body))
			req.Header.Set("Content-Type", "text/calendar")
			rec := httptest.NewRecorder()
			handler.CreateEvent(rec, withUserAndRoute(req, "1", ""))
			if rec.Code != tt.want {
				t.Errorf("CreateEvent() status = %d, want %d, body=%s", rec.Code, tt.want, rec.Body.String())
			}
		})
	}
}
