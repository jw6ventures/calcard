package api

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/jw6ventures/calcard/internal/auth"
	"github.com/jw6ventures/calcard/internal/config"
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
	})

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
	})

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
	})

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
	})

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
	handler := NewHandler(&config.Config{}, &store.Store{})
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
	})
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
	})
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
	})
	req := httptest.NewRequest(http.MethodGet, "/api/calendars", nil)
	req = req.WithContext(auth.WithUser(req.Context(), &store.User{ID: 1}))
	rec := httptest.NewRecorder()

	handler.ListCalendars(rec, req)

	if rec.Code != http.StatusInternalServerError {
		t.Fatalf("ListCalendars() status = %d, want %d", rec.Code, http.StatusInternalServerError)
	}
}

func TestGetCalendarUnauthorizedAndInvalidID(t *testing.T) {
	handler := NewHandler(&config.Config{}, &store.Store{})

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
	})
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
	})
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
	})
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
	})
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
	})
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
	})

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

func TestListEventsInvalidCalendarID(t *testing.T) {
	handler := NewHandler(&config.Config{}, &store.Store{})
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
	handler := NewHandler(&config.Config{}, &store.Store{})
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
	})

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
	handler := NewHandler(&config.Config{}, &store.Store{})

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
	})
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

func TestUpdateEventUnauthorizedAndSuccess(t *testing.T) {
	handler := NewHandler(&config.Config{}, &store.Store{})
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
	})
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
	})
	req := httptest.NewRequest(http.MethodDelete, "/api/calendars/1/events/event-1", nil)
	req = withUserAndRoute(req, "1", "event-1")
	rec := httptest.NewRecorder()

	handler.DeleteEvent(rec, req)

	if rec.Code != http.StatusNoContent {
		t.Fatalf("DeleteEvent() status = %d, want %d", rec.Code, http.StatusNoContent)
	}
}

func TestDeleteEventUnauthorizedAndNotFound(t *testing.T) {
	handler := NewHandler(&config.Config{}, &store.Store{})
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
	})
	req = httptest.NewRequest(http.MethodDelete, "/api/calendars/1/events/missing", nil)
	req = withUserAndRoute(req, "1", "missing")
	rec = httptest.NewRecorder()
	handler.DeleteEvent(rec, req)
	if rec.Code != http.StatusNotFound {
		t.Fatalf("DeleteEvent() not found status = %d", rec.Code)
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
func (f *fakeCalendarRepo) UpdateProperties(ctx context.Context, id int64, name string, description, timezone, color *string) error {
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
func (f *fakeEventRepo) ListForCalendar(ctx context.Context, calendarID int64) ([]store.Event, error) {
	var out []store.Event
	for _, ev := range f.events {
		if ev.CalendarID == calendarID {
			out = append(out, ev)
		}
	}
	return out, nil
}
func (f *fakeEventRepo) ListForCalendarPaginated(ctx context.Context, calendarID int64, limit, offset int) (*store.PaginatedResult[store.Event], error) {
	return nil, nil
}
func (f *fakeEventRepo) ListByUIDs(ctx context.Context, calendarID int64, uids []string) ([]store.Event, error) {
	return nil, nil
}
func (f *fakeEventRepo) ListModifiedSince(ctx context.Context, calendarID int64, since time.Time) ([]store.Event, error) {
	return nil, nil
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
func (f *fakeEventRepo) CopyToCalendar(ctx context.Context, fromCalendarID, toCalendarID int64, uid, destResourceName, newETag string) (*store.Event, error) {
	return nil, nil
}

func key(calendarID int64, uid string) string {
	return strconv.FormatInt(calendarID, 10) + ":" + uid
}
