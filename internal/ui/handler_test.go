package ui

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"mime/multipart"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/jw6ventures/calcard/internal/auth"
	"github.com/jw6ventures/calcard/internal/config"
	"github.com/jw6ventures/calcard/internal/store"
)

func TestViewCalendarHandler(t *testing.T) {
	testCases := []struct {
		name           string
		calendarID     string
		userID         int64
		calendar       *store.Calendar
		events         []store.Event
		wantStatusCode int
	}{
		{
			name:       "valid calendar with events",
			calendarID: "1",
			userID:     100,
			calendar:   &store.Calendar{ID: 1, UserID: 100, Name: "Test Calendar"},
			events: []store.Event{
				{
					ID:         1,
					CalendarID: 1,
					UID:        "event-1",
					RawICAL:    "BEGIN:VCALENDAR\r\nBEGIN:VEVENT\r\nUID:event-1\r\nSUMMARY:Test Event\r\nDTSTART:20240101T100000Z\r\nEND:VEVENT\r\nEND:VCALENDAR\r\n",
				},
			},
			wantStatusCode: http.StatusOK,
		},
		{
			name:           "calendar not found",
			calendarID:     "999",
			userID:         100,
			calendar:       nil,
			events:         nil,
			wantStatusCode: http.StatusNotFound,
		},
		{
			name:           "calendar belongs to different user",
			calendarID:     "1",
			userID:         100,
			calendar:       &store.Calendar{ID: 1, UserID: 200, Name: "Other User Calendar"},
			events:         nil,
			wantStatusCode: http.StatusNotFound,
		},
		{
			name:           "invalid calendar id",
			calendarID:     "invalid",
			userID:         100,
			calendar:       nil,
			events:         nil,
			wantStatusCode: http.StatusBadRequest,
		},
		{
			name:           "empty calendar",
			calendarID:     "1",
			userID:         100,
			calendar:       &store.Calendar{ID: 1, UserID: 100, Name: "Empty Calendar"},
			events:         []store.Event{},
			wantStatusCode: http.StatusOK,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			calRepo := &fakeCalendarRepo{
				calendars: make(map[int64]*store.Calendar),
			}
			if tc.calendar != nil {
				calRepo.calendars[tc.calendar.ID] = tc.calendar
			}

			eventRepo := &fakeEventRepo{
				events: make(map[string]*store.Event),
			}
			for i := range tc.events {
				key := fmt.Sprintf("%d:%s", tc.events[i].CalendarID, tc.events[i].UID)
				eventRepo.events[key] = &tc.events[i]
			}

			s := &store.Store{
				Calendars: calRepo,
				Events:    eventRepo,
			}

			handler := NewHandler(&config.Config{}, s, nil)

			req := httptest.NewRequest(http.MethodGet, "/calendars/"+tc.calendarID, nil)
			rctx := chi.NewRouteContext()
			rctx.URLParams.Add("id", tc.calendarID)
			req = req.WithContext(context.WithValue(req.Context(), chi.RouteCtxKey, rctx))

			// Add user to context
			user := &store.User{ID: tc.userID, PrimaryEmail: "test@example.com"}
			req = req.WithContext(auth.WithUser(req.Context(), user))

			w := httptest.NewRecorder()
			handler.ViewCalendar(w, req)

			if w.Code != tc.wantStatusCode {
				t.Errorf("ViewCalendar() status = %d, want %d", w.Code, tc.wantStatusCode)
			}

			if tc.wantStatusCode == http.StatusOK {
				body := w.Body.String()
				if tc.calendar != nil && len(body) == 0 {
					t.Error("expected non-empty response body")
				}
			}
		})
	}
}

func TestViewCalendarUsesExplicitCapabilitiesForActions(t *testing.T) {
	t.Run("bind access shows create and import actions", func(t *testing.T) {
		handler := NewHandler(&config.Config{}, &store.Store{
			Calendars: &fakeCalendarRepo{
				accessible: map[string]*store.CalendarAccess{
					"1:100": {
						Calendar:   store.Calendar{ID: 1, UserID: 200, Name: "Shared"},
						Shared:     true,
						OwnerEmail: "owner@example.com",
						Privileges: store.CalendarPrivileges{Read: true, Bind: true},
					},
				},
			},
		}, nil)

		req := httptest.NewRequest(http.MethodGet, "/calendars/1", nil)
		rctx := chi.NewRouteContext()
		rctx.URLParams.Add("id", "1")
		req = req.WithContext(context.WithValue(req.Context(), chi.RouteCtxKey, rctx))
		req = req.WithContext(auth.WithUser(req.Context(), &store.User{ID: 100, PrimaryEmail: "delegate@example.com"}))
		w := httptest.NewRecorder()

		handler.ViewCalendar(w, req)

		if w.Code != http.StatusOK {
			t.Fatalf("ViewCalendar() status = %d, want %d", w.Code, http.StatusOK)
		}
		body := w.Body.String()
		if !strings.Contains(body, "+ New Event") {
			t.Fatalf("expected create action for bind access, got %s", body)
		}
		if !strings.Contains(body, ">Import ICS<") {
			t.Fatalf("expected import action for bind access, got %s", body)
		}
	})

	t.Run("read-only access hides write actions", func(t *testing.T) {
		handler := NewHandler(&config.Config{}, &store.Store{
			Calendars: &fakeCalendarRepo{
				accessible: map[string]*store.CalendarAccess{
					"1:100": {
						Calendar:   store.Calendar{ID: 1, UserID: 200, Name: "Shared"},
						Shared:     true,
						OwnerEmail: "owner@example.com",
						Privileges: store.CalendarPrivileges{Read: true},
					},
				},
			},
		}, nil)

		req := httptest.NewRequest(http.MethodGet, "/calendars/1", nil)
		rctx := chi.NewRouteContext()
		rctx.URLParams.Add("id", "1")
		req = req.WithContext(context.WithValue(req.Context(), chi.RouteCtxKey, rctx))
		req = req.WithContext(auth.WithUser(req.Context(), &store.User{ID: 100, PrimaryEmail: "delegate@example.com"}))
		w := httptest.NewRecorder()

		handler.ViewCalendar(w, req)

		if w.Code != http.StatusOK {
			t.Fatalf("ViewCalendar() status = %d, want %d", w.Code, http.StatusOK)
		}
		body := w.Body.String()
		if strings.Contains(body, "+ New Event") {
			t.Fatalf("did not expect create action for read-only access, got %s", body)
		}
		if strings.Contains(body, ">Import ICS<") {
			t.Fatalf("did not expect import action for read-only access, got %s", body)
		}
	})
}

func TestViewCalendarRendersDayAndWeekViews(t *testing.T) {
	handler := NewHandler(&config.Config{}, &store.Store{
		Calendars: &fakeCalendarRepo{
			calendars: map[int64]*store.Calendar{
				1: {ID: 1, UserID: 100, Name: "Work"},
			},
		},
	}, nil)

	req := httptest.NewRequest(http.MethodGet, "/calendars/1", nil)
	rctx := chi.NewRouteContext()
	rctx.URLParams.Add("id", "1")
	req = req.WithContext(context.WithValue(req.Context(), chi.RouteCtxKey, rctx))
	req = req.WithContext(auth.WithUser(req.Context(), &store.User{ID: 100, PrimaryEmail: "owner@example.com"}))
	w := httptest.NewRecorder()

	handler.ViewCalendar(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("ViewCalendar() status = %d, want %d", w.Code, http.StatusOK)
	}
	body := w.Body.String()
	for _, want := range []string{`data-view="day"`, `data-view="week"`, `id="time-grid-view"`, `id="prev-time-range"`} {
		if !strings.Contains(body, want) {
			t.Fatalf("expected calendar view to contain %q, got %s", want, body)
		}
	}
}

func TestViewAddressBookHandler(t *testing.T) {
	testCases := []struct {
		name           string
		bookID         string
		userID         int64
		book           *store.AddressBook
		contacts       []store.Contact
		wantStatusCode int
	}{
		{
			name:   "valid address book with contacts",
			bookID: "1",
			userID: 100,
			book:   &store.AddressBook{ID: 1, UserID: 100, Name: "Test Contacts"},
			contacts: []store.Contact{
				{
					ID:            1,
					AddressBookID: 1,
					UID:           "contact-1",
					RawVCard:      "BEGIN:VCARD\r\nVERSION:3.0\r\nFN:John Doe\r\nEMAIL:john@example.com\r\nEND:VCARD\r\n",
				},
			},
			wantStatusCode: http.StatusOK,
		},
		{
			name:           "address book not found",
			bookID:         "999",
			userID:         100,
			book:           nil,
			contacts:       nil,
			wantStatusCode: http.StatusNotFound,
		},
		{
			name:           "address book belongs to different user",
			bookID:         "1",
			userID:         100,
			book:           &store.AddressBook{ID: 1, UserID: 200, Name: "Other User Contacts"},
			contacts:       nil,
			wantStatusCode: http.StatusNotFound,
		},
		{
			name:           "invalid address book id",
			bookID:         "invalid",
			userID:         100,
			book:           nil,
			contacts:       nil,
			wantStatusCode: http.StatusBadRequest,
		},
		{
			name:           "empty address book",
			bookID:         "1",
			userID:         100,
			book:           &store.AddressBook{ID: 1, UserID: 100, Name: "Empty Contacts"},
			contacts:       []store.Contact{},
			wantStatusCode: http.StatusOK,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			bookRepo := &fakeAddressBookRepo{
				books: make(map[int64]*store.AddressBook),
			}
			if tc.book != nil {
				bookRepo.books[tc.book.ID] = tc.book
			}

			contactRepo := &fakeContactRepo{
				contacts: make(map[string]*store.Contact),
			}
			for i := range tc.contacts {
				key := fmt.Sprintf("%d:%s", tc.contacts[i].AddressBookID, tc.contacts[i].UID)
				contactRepo.contacts[key] = &tc.contacts[i]
			}

			s := &store.Store{
				AddressBooks: bookRepo,
				Contacts:     contactRepo,
			}

			handler := NewHandler(&config.Config{}, s, nil)

			req := httptest.NewRequest(http.MethodGet, "/addressbooks/"+tc.bookID, nil)
			rctx := chi.NewRouteContext()
			rctx.URLParams.Add("id", tc.bookID)
			req = req.WithContext(context.WithValue(req.Context(), chi.RouteCtxKey, rctx))

			// Add user to context
			user := &store.User{ID: tc.userID, PrimaryEmail: "test@example.com"}
			req = req.WithContext(auth.WithUser(req.Context(), user))

			w := httptest.NewRecorder()
			handler.ViewAddressBook(w, req)

			if w.Code != tc.wantStatusCode {
				t.Errorf("ViewAddressBook() status = %d, want %d", w.Code, tc.wantStatusCode)
			}

			if tc.wantStatusCode == http.StatusOK {
				body := w.Body.String()
				if tc.book != nil && len(body) == 0 {
					t.Error("expected non-empty response body")
				}
			}
		})
	}
}

func TestTemplatesIncludeNewViews(t *testing.T) {
	names := []string{
		"calendar_view.html",
		"addressbook_view.html",
		"sessions.html",
		"birthdays.html",
	}
	for _, name := range names {
		if _, err := templateFS.Open("templates/" + name); err != nil {
			t.Errorf("expected embedded template %s, got error: %v", name, err)
		}
	}
}

func TestCreateEventHandler(t *testing.T) {
	calRepo := &fakeCalendarRepo{
		calendars: map[int64]*store.Calendar{
			1: {ID: 1, UserID: 100, Name: "Test Calendar"},
		},
	}
	eventRepo := &fakeEventRepoWithUpsert{
		fakeEventRepo: fakeEventRepo{events: make(map[string]*store.Event)},
	}

	s := &store.Store{
		Calendars: calRepo,
		Events:    eventRepo,
	}

	handler := NewHandler(&config.Config{}, s, nil)

	testCases := []struct {
		name           string
		calendarID     string
		userID         int64
		formValues     map[string]string
		wantStatusCode int
	}{
		{
			name:       "create event success",
			calendarID: "1",
			userID:     100,
			formValues: map[string]string{
				"summary": "Test Event",
				"dtstart": "2024-01-01T10:00",
				"dtend":   "2024-01-01T11:00",
			},
			wantStatusCode: http.StatusFound,
		},
		{
			name:       "missing summary",
			calendarID: "1",
			userID:     100,
			formValues: map[string]string{
				"dtstart": "2024-01-01T10:00",
			},
			wantStatusCode: http.StatusFound, // Redirects with error
		},
		{
			name:       "missing start",
			calendarID: "1",
			userID:     100,
			formValues: map[string]string{
				"summary": "Test Event",
				"dtend":   "2024-01-01T11:00",
			},
			wantStatusCode: http.StatusFound, // Redirects with error
		},
		{
			name:       "missing end",
			calendarID: "1",
			userID:     100,
			formValues: map[string]string{
				"summary": "Test Event",
				"dtstart": "2024-01-01T10:00",
			},
			wantStatusCode: http.StatusFound, // Redirects with error
		},
		{
			name:           "calendar not found",
			calendarID:     "999",
			userID:         100,
			formValues:     map[string]string{"summary": "Test"},
			wantStatusCode: http.StatusNotFound,
		},
		{
			name:           "calendar belongs to different user",
			calendarID:     "1",
			userID:         200,
			formValues:     map[string]string{"summary": "Test"},
			wantStatusCode: http.StatusNotFound,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			form := make(url.Values)
			for k, v := range tc.formValues {
				form.Set(k, v)
			}

			req := httptest.NewRequest(http.MethodPost, "/calendars/"+tc.calendarID+"/events", strings.NewReader(form.Encode()))
			req.Header.Set("Content-Type", "application/x-www-form-urlencoded")

			rctx := chi.NewRouteContext()
			rctx.URLParams.Add("id", tc.calendarID)
			req = req.WithContext(context.WithValue(req.Context(), chi.RouteCtxKey, rctx))

			user := &store.User{ID: tc.userID, PrimaryEmail: "test@example.com"}
			req = req.WithContext(auth.WithUser(req.Context(), user))

			w := httptest.NewRecorder()
			handler.CreateEvent(w, req)

			if w.Code != tc.wantStatusCode {
				t.Errorf("CreateEvent() status = %d, want %d", w.Code, tc.wantStatusCode)
			}
		})
	}
}

func TestCreateEventReturnsToAllCalendars(t *testing.T) {
	eventRepo := &fakeEventRepoWithUpsert{
		fakeEventRepo: fakeEventRepo{events: make(map[string]*store.Event)},
	}
	handler := NewHandler(&config.Config{}, &store.Store{
		Calendars: &fakeCalendarRepo{calendars: map[int64]*store.Calendar{
			1: {ID: 1, UserID: 100, Name: "Work"},
		}},
		Events: eventRepo,
	}, nil)

	form := url.Values{
		"summary":   {"Planning"},
		"dtstart":   {"2026-07-24T10:00"},
		"dtend":     {"2026-07-24T10:30"},
		"return_to": {"/calendars/all?view=week&date=2026-07-24"},
	}
	req := httptest.NewRequest(http.MethodPost, "/calendars/1/events", strings.NewReader(form.Encode()))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	req = withRouteID(req, "1")
	req = req.WithContext(auth.WithUser(req.Context(), &store.User{ID: 100, PrimaryEmail: "owner@example.com"}))
	w := httptest.NewRecorder()

	handler.CreateEvent(w, req)

	if w.Code != http.StatusFound {
		t.Fatalf("CreateEvent() status = %d, want %d", w.Code, http.StatusFound)
	}
	location := w.Header().Get("Location")
	if !strings.HasPrefix(location, "/calendars/all?") ||
		!strings.Contains(location, "view=week") ||
		!strings.Contains(location, "date=2026-07-24") ||
		!strings.Contains(location, "status=event_created") {
		t.Fatalf("CreateEvent() redirect = %q, want all-calendars state and status", location)
	}
}

func TestEventReturnPathRejectsExternalTargets(t *testing.T) {
	form := url.Values{"return_to": {"https://example.com/calendars/all?view=week"}}
	req := httptest.NewRequest(http.MethodPost, "/calendars/1/events", strings.NewReader(form.Encode()))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	if got := eventReturnPath(req, 1); got != "/calendars/1" {
		t.Fatalf("eventReturnPath() = %q, want local calendar fallback", got)
	}
}

func TestEventReturnPathPreservesOwnCalendarView(t *testing.T) {
	form := url.Values{"return_to": {"/calendars/1?view=week&date=2026-07-24"}}
	req := httptest.NewRequest(http.MethodPost, "/calendars/1/events", strings.NewReader(form.Encode()))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	if got := eventReturnPath(req, 1); got != "/calendars/1?date=2026-07-24&view=week" {
		t.Fatalf("eventReturnPath() = %q, want own calendar view/date preserved", got)
	}
}

func TestEventReturnPathRejectsOtherCalendarTargets(t *testing.T) {
	form := url.Values{"return_to": {"/calendars/2?view=week&date=2026-07-24"}}
	req := httptest.NewRequest(http.MethodPost, "/calendars/1/events", strings.NewReader(form.Encode()))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	if got := eventReturnPath(req, 1); got != "/calendars/1" {
		t.Fatalf("eventReturnPath() = %q, want fallback for a different calendar target", got)
	}
}

func TestDeleteEventHandler(t *testing.T) {
	calRepo := &fakeCalendarRepo{
		calendars: map[int64]*store.Calendar{
			1: {ID: 1, UserID: 100, Name: "Test Calendar"},
		},
	}
	eventRepo := &fakeEventRepoWithDelete{
		fakeEventRepo: fakeEventRepo{
			events: map[string]*store.Event{
				"1:event-1": {ID: 1, CalendarID: 1, UID: "event-1"},
			},
		},
	}
	deadProperties := &fakeDeadPropertyRepo{properties: map[string][]store.DeadProperty{
		"/dav/calendars/1/event-1": {{ResourcePath: "/dav/calendars/1/event-1", NamespaceURI: "urn:test", LocalName: "note"}},
	}}

	s := &store.Store{
		Calendars:      calRepo,
		Events:         eventRepo,
		DeadProperties: deadProperties,
	}

	handler := NewHandler(&config.Config{}, s, nil)

	req := httptest.NewRequest(http.MethodDelete, "/calendars/1/events/event-1", nil)
	rctx := chi.NewRouteContext()
	rctx.URLParams.Add("id", "1")
	rctx.URLParams.Add("uid", "event-1")
	req = req.WithContext(context.WithValue(req.Context(), chi.RouteCtxKey, rctx))

	user := &store.User{ID: 100, PrimaryEmail: "test@example.com"}
	req = req.WithContext(auth.WithUser(req.Context(), user))

	w := httptest.NewRecorder()
	handler.DeleteEvent(w, req)

	if w.Code != http.StatusFound {
		t.Errorf("DeleteEvent() status = %d, want %d", w.Code, http.StatusFound)
	}
	if len(deadProperties.properties["/dav/calendars/1/event-1"]) != 0 {
		t.Fatalf("DeleteEvent() retained dead properties: %#v", deadProperties.properties)
	}
}

func TestDeleteContactHandlerRemovesDAVState(t *testing.T) {
	bookRepo := &fakeAddressBookRepo{books: map[int64]*store.AddressBook{
		1: {ID: 1, UserID: 100, Name: "Contacts"},
	}}
	contactRepo := &fakeContactRepo{contacts: map[string]*store.Contact{
		"1:contact-1": {ID: 1, AddressBookID: 1, UID: "contact-1", ResourceName: "alice"},
	}}
	deadProperties := &fakeDeadPropertyRepo{properties: map[string][]store.DeadProperty{
		"/dav/addressbooks/1/alice": {{ResourcePath: "/dav/addressbooks/1/alice", NamespaceURI: "urn:test", LocalName: "note"}},
	}}
	handler := NewHandler(&config.Config{}, &store.Store{
		AddressBooks:   bookRepo,
		Contacts:       contactRepo,
		DeadProperties: deadProperties,
	}, nil)
	req := httptest.NewRequest(http.MethodDelete, "/addressbooks/1/contacts/contact-1", nil)
	rctx := chi.NewRouteContext()
	rctx.URLParams.Add("id", "1")
	rctx.URLParams.Add("uid", "contact-1")
	req = req.WithContext(context.WithValue(req.Context(), chi.RouteCtxKey, rctx))
	req = req.WithContext(auth.WithUser(req.Context(), &store.User{ID: 100}))
	response := httptest.NewRecorder()

	handler.DeleteContact(response, req)

	if response.Code != http.StatusFound {
		t.Fatalf("DeleteContact() status = %d, want %d", response.Code, http.StatusFound)
	}
	if len(deadProperties.properties["/dav/addressbooks/1/alice"]) != 0 {
		t.Fatalf("DeleteContact() retained dead properties: %#v", deadProperties.properties)
	}
}

func TestCalendarEventHandlersRequireSpecificACLPrivileges(t *testing.T) {
	t.Run("create_requires_bind", func(t *testing.T) {
		calRepo := &fakeCalendarRepo{
			accessible: map[string]*store.CalendarAccess{
				"1:100": {Calendar: store.Calendar{ID: 1, UserID: 200, Name: "Shared"}, Shared: true, Editor: true},
			},
		}
		eventRepo := &fakeEventRepoWithUpsert{
			fakeEventRepo: fakeEventRepo{events: make(map[string]*store.Event)},
		}
		aclRepo := &fakeACLRepo{entries: []store.ACLEntry{
			{ResourcePath: "/dav/calendars/1", PrincipalHref: "/dav/principals/100/", IsGrant: true, Privilege: "read"},
			{ResourcePath: "/dav/calendars/1", PrincipalHref: "/dav/principals/100/", IsGrant: true, Privilege: "write-content"},
		}}
		handler := NewHandler(&config.Config{}, &store.Store{
			Calendars:  calRepo,
			Events:     eventRepo,
			ACLEntries: aclRepo,
		}, nil)

		form := url.Values{
			"summary": {"Test Event"},
			"dtstart": {"2024-01-01T10:00"},
			"dtend":   {"2024-01-01T11:00"},
		}
		req := httptest.NewRequest(http.MethodPost, "/calendars/1/events", strings.NewReader(form.Encode()))
		req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
		req = withRouteID(req, "1")
		req = req.WithContext(auth.WithUser(req.Context(), &store.User{ID: 100, PrimaryEmail: "delegate@example.com"}))

		w := httptest.NewRecorder()
		handler.CreateEvent(w, req)

		if w.Code != http.StatusForbidden {
			t.Fatalf("CreateEvent() status = %d, want %d", w.Code, http.StatusForbidden)
		}
		if len(eventRepo.events) != 0 {
			t.Fatalf("expected no created events, got %#v", eventRepo.events)
		}
	})

	t.Run("update_requires_write_content", func(t *testing.T) {
		calRepo := &fakeCalendarRepo{
			accessible: map[string]*store.CalendarAccess{
				"1:100": {Calendar: store.Calendar{ID: 1, UserID: 200, Name: "Shared"}, Shared: true, Editor: true},
			},
		}
		eventRepo := &fakeEventRepoWithUpsert{
			fakeEventRepo: fakeEventRepo{
				events: map[string]*store.Event{
					"1:event-1": {
						ID:           1,
						CalendarID:   1,
						UID:          "event-1",
						ResourceName: "event-1",
						RawICAL:      "BEGIN:VCALENDAR\r\nVERSION:2.0\r\nBEGIN:VEVENT\r\nUID:event-1\r\nSUMMARY:Original\r\nDTSTART:20240101T100000Z\r\nDTEND:20240101T110000Z\r\nEND:VEVENT\r\nEND:VCALENDAR\r\n",
						ETag:         "etag-1",
					},
				},
			},
		}
		aclRepo := &fakeACLRepo{entries: []store.ACLEntry{
			{ResourcePath: "/dav/calendars/1", PrincipalHref: "/dav/principals/100/", IsGrant: true, Privilege: "read"},
			{ResourcePath: "/dav/calendars/1", PrincipalHref: "/dav/principals/100/", IsGrant: true, Privilege: "bind"},
		}}
		handler := NewHandler(&config.Config{}, &store.Store{
			Calendars:  calRepo,
			Events:     eventRepo,
			ACLEntries: aclRepo,
		}, nil)

		form := url.Values{
			"summary":    {"Updated Summary"},
			"dtstart":    {"2024-01-02T10:00"},
			"dtend":      {"2024-01-02T11:00"},
			"edit_scope": {"series"},
		}
		req := httptest.NewRequest(http.MethodPost, "/calendars/1/events/event-1", strings.NewReader(form.Encode()))
		req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
		req = req.WithContext(auth.WithUser(req.Context(), &store.User{ID: 100, PrimaryEmail: "delegate@example.com"}))
		rctx := chi.NewRouteContext()
		rctx.URLParams.Add("id", "1")
		rctx.URLParams.Add("uid", "event-1")
		req = req.WithContext(context.WithValue(req.Context(), chi.RouteCtxKey, rctx))

		w := httptest.NewRecorder()
		handler.UpdateEvent(w, req)

		if w.Code != http.StatusForbidden {
			t.Fatalf("UpdateEvent() status = %d, want %d", w.Code, http.StatusForbidden)
		}
		if strings.Contains(eventRepo.events["1:event-1"].RawICAL, "Updated Summary") {
			t.Fatalf("expected event update to be blocked, got %s", eventRepo.events["1:event-1"].RawICAL)
		}
	})

	t.Run("update_respects_canonical_object_deny_for_ics_resource", func(t *testing.T) {
		calRepo := &fakeCalendarRepo{
			accessible: map[string]*store.CalendarAccess{
				"1:100": {Calendar: store.Calendar{ID: 1, UserID: 200, Name: "Shared"}, Shared: true, Editor: true},
			},
		}
		eventRepo := &fakeEventRepoWithUpsert{
			fakeEventRepo: fakeEventRepo{
				events: map[string]*store.Event{
					"1:event-1": {
						ID:           1,
						CalendarID:   1,
						UID:          "event-1",
						ResourceName: "event-1.ics",
						RawICAL:      "BEGIN:VCALENDAR\r\nVERSION:2.0\r\nBEGIN:VEVENT\r\nUID:event-1\r\nSUMMARY:Original\r\nDTSTART:20240101T100000Z\r\nDTEND:20240101T110000Z\r\nEND:VEVENT\r\nEND:VCALENDAR\r\n",
						ETag:         "etag-1",
					},
				},
			},
		}
		aclRepo := &fakeACLRepo{entries: []store.ACLEntry{
			{ResourcePath: "/dav/calendars/1", PrincipalHref: "/dav/principals/100/", IsGrant: true, Privilege: "read"},
			{ResourcePath: "/dav/calendars/1", PrincipalHref: "/dav/principals/100/", IsGrant: true, Privilege: "write-content"},
			{ResourcePath: "/dav/calendars/1/event-1", PrincipalHref: "/dav/principals/100/", IsGrant: false, Privilege: "write-content"},
		}}
		handler := NewHandler(&config.Config{}, &store.Store{
			Calendars:  calRepo,
			Events:     eventRepo,
			ACLEntries: aclRepo,
		}, nil)

		form := url.Values{
			"summary":    {"Updated Summary"},
			"dtstart":    {"2024-01-02T10:00"},
			"dtend":      {"2024-01-02T11:00"},
			"edit_scope": {"series"},
		}
		req := httptest.NewRequest(http.MethodPost, "/calendars/1/events/event-1", strings.NewReader(form.Encode()))
		req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
		req = req.WithContext(auth.WithUser(req.Context(), &store.User{ID: 100, PrimaryEmail: "delegate@example.com"}))
		rctx := chi.NewRouteContext()
		rctx.URLParams.Add("id", "1")
		rctx.URLParams.Add("uid", "event-1")
		req = req.WithContext(context.WithValue(req.Context(), chi.RouteCtxKey, rctx))

		w := httptest.NewRecorder()
		handler.UpdateEvent(w, req)

		if w.Code != http.StatusForbidden {
			t.Fatalf("UpdateEvent() status = %d, want %d", w.Code, http.StatusForbidden)
		}
		if strings.Contains(eventRepo.events["1:event-1"].RawICAL, "Updated Summary") {
			t.Fatalf("expected canonical object deny to block update, got %s", eventRepo.events["1:event-1"].RawICAL)
		}
	})

	t.Run("delete_requires_unbind", func(t *testing.T) {
		calRepo := &fakeCalendarRepo{
			accessible: map[string]*store.CalendarAccess{
				"1:100": {Calendar: store.Calendar{ID: 1, UserID: 200, Name: "Shared"}, Shared: true, Editor: true},
			},
		}
		eventRepo := &fakeEventRepoWithDelete{
			fakeEventRepo: fakeEventRepo{
				events: map[string]*store.Event{
					"1:event-1": {ID: 1, CalendarID: 1, UID: "event-1", ResourceName: "event-1"},
				},
			},
		}
		aclRepo := &fakeACLRepo{entries: []store.ACLEntry{
			{ResourcePath: "/dav/calendars/1", PrincipalHref: "/dav/principals/100/", IsGrant: true, Privilege: "read"},
			{ResourcePath: "/dav/calendars/1", PrincipalHref: "/dav/principals/100/", IsGrant: true, Privilege: "write-content"},
		}}
		handler := NewHandler(&config.Config{}, &store.Store{
			Calendars:  calRepo,
			Events:     eventRepo,
			ACLEntries: aclRepo,
		}, nil)

		req := httptest.NewRequest(http.MethodDelete, "/calendars/1/events/event-1", nil)
		rctx := chi.NewRouteContext()
		rctx.URLParams.Add("id", "1")
		rctx.URLParams.Add("uid", "event-1")
		req = req.WithContext(context.WithValue(req.Context(), chi.RouteCtxKey, rctx))
		req = req.WithContext(auth.WithUser(req.Context(), &store.User{ID: 100, PrimaryEmail: "delegate@example.com"}))

		w := httptest.NewRecorder()
		handler.DeleteEvent(w, req)

		if w.Code != http.StatusForbidden {
			t.Fatalf("DeleteEvent() status = %d, want %d", w.Code, http.StatusForbidden)
		}
		if len(eventRepo.deleted) != 0 {
			t.Fatalf("expected no deleted events, got %#v", eventRepo.deleted)
		}
	})

	t.Run("delete_occurrence_requires_write_content_not_unbind", func(t *testing.T) {
		calRepo := &fakeCalendarRepo{
			accessible: map[string]*store.CalendarAccess{
				"1:100": {Calendar: store.Calendar{ID: 1, UserID: 200, Name: "Shared"}, Shared: true, Editor: true},
			},
		}
		eventRepo := &fakeEventRepoWithUpsert{
			fakeEventRepo: fakeEventRepo{
				events: map[string]*store.Event{
					"1:event-1": {
						ID:           1,
						CalendarID:   1,
						UID:          "event-1",
						ResourceName: "event-1",
						RawICAL:      "BEGIN:VCALENDAR\r\nVERSION:2.0\r\nBEGIN:VEVENT\r\nUID:event-1\r\nRRULE:FREQ=DAILY\r\nDTSTART:20240101T100000Z\r\nDTEND:20240101T110000Z\r\nEND:VEVENT\r\nEND:VCALENDAR\r\n",
						ETag:         "etag-1",
					},
				},
			},
		}
		aclRepo := &fakeACLRepo{entries: []store.ACLEntry{
			{ResourcePath: "/dav/calendars/1", PrincipalHref: "/dav/principals/100/", IsGrant: true, Privilege: "read"},
			{ResourcePath: "/dav/calendars/1", PrincipalHref: "/dav/principals/100/", IsGrant: true, Privilege: "write-content"},
		}}
		handler := NewHandler(&config.Config{}, &store.Store{
			Calendars:  calRepo,
			Events:     eventRepo,
			ACLEntries: aclRepo,
		}, nil)

		form := url.Values{
			"edit_scope":         {"occurrence"},
			"recurrence_id":      {"2024-01-02T10:00"},
			"recurrence_all_day": {"false"},
		}
		req := httptest.NewRequest(http.MethodPost, "/calendars/1/events/event-1/delete", strings.NewReader(form.Encode()))
		req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
		rctx := chi.NewRouteContext()
		rctx.URLParams.Add("id", "1")
		rctx.URLParams.Add("uid", "event-1")
		req = req.WithContext(context.WithValue(req.Context(), chi.RouteCtxKey, rctx))
		req = req.WithContext(auth.WithUser(req.Context(), &store.User{ID: 100, PrimaryEmail: "delegate@example.com"}))

		w := httptest.NewRecorder()
		handler.DeleteEvent(w, req)

		if w.Code != http.StatusFound {
			t.Fatalf("DeleteEvent() status = %d, want %d", w.Code, http.StatusFound)
		}
		updated := eventRepo.events["1:event-1"]
		if updated == nil || !strings.Contains(updated.RawICAL, "EXDATE:") {
			t.Fatalf("expected occurrence delete to update series with EXDATE, got %#v", updated)
		}
	})
}

func TestCreateContactHandler(t *testing.T) {
	bookRepo := &fakeAddressBookRepo{
		books: map[int64]*store.AddressBook{
			1: {ID: 1, UserID: 100, Name: "Test Contacts"},
		},
	}
	contactRepo := &fakeContactRepoWithUpsert{
		fakeContactRepo: fakeContactRepo{contacts: make(map[string]*store.Contact)},
	}

	s := &store.Store{
		AddressBooks: bookRepo,
		Contacts:     contactRepo,
	}

	handler := NewHandler(&config.Config{}, s, nil)

	testCases := []struct {
		name           string
		bookID         string
		userID         int64
		formValues     map[string]string
		wantStatusCode int
	}{
		{
			name:   "create contact success",
			bookID: "1",
			userID: 100,
			formValues: map[string]string{
				"display_name": "John Doe",
				"email":        "john@example.com",
				"phone":        "+1234567890",
				"birthday":     "1990-05-15",
			},
			wantStatusCode: http.StatusFound,
		},
		{
			name:   "missing display name",
			bookID: "1",
			userID: 100,
			formValues: map[string]string{
				"email": "john@example.com",
			},
			wantStatusCode: http.StatusFound, // Redirects with error
		},
		{
			name:           "address book not found",
			bookID:         "999",
			userID:         100,
			formValues:     map[string]string{"display_name": "John"},
			wantStatusCode: http.StatusNotFound,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			form := make(url.Values)
			for k, v := range tc.formValues {
				form.Set(k, v)
			}

			req := httptest.NewRequest(http.MethodPost, "/addressbooks/"+tc.bookID+"/contacts", strings.NewReader(form.Encode()))
			req.Header.Set("Content-Type", "application/x-www-form-urlencoded")

			rctx := chi.NewRouteContext()
			rctx.URLParams.Add("id", tc.bookID)
			req = req.WithContext(context.WithValue(req.Context(), chi.RouteCtxKey, rctx))

			user := &store.User{ID: tc.userID, PrimaryEmail: "test@example.com"}
			req = req.WithContext(auth.WithUser(req.Context(), user))

			w := httptest.NewRecorder()
			handler.CreateContact(w, req)

			if w.Code != tc.wantStatusCode {
				t.Errorf("CreateContact() status = %d, want %d", w.Code, tc.wantStatusCode)
			}
		})
	}
}

func TestViewBirthdaysHandler(t *testing.T) {
	bday := time.Date(1990, 5, 15, 0, 0, 0, 0, time.UTC)
	displayName := "John Doe"
	contactRepo := &fakeContactRepoWithBirthdays{
		fakeContactRepo: fakeContactRepo{contacts: make(map[string]*store.Contact)},
		birthdays: []store.Contact{
			{ID: 1, UID: "contact-1", DisplayName: &displayName, Birthday: &bday},
		},
	}

	s := &store.Store{
		Contacts: contactRepo,
	}

	handler := NewHandler(&config.Config{}, s, nil)

	req := httptest.NewRequest(http.MethodGet, "/birthdays", nil)
	user := &store.User{ID: 100, PrimaryEmail: "test@example.com"}
	req = req.WithContext(auth.WithUser(req.Context(), user))

	w := httptest.NewRecorder()
	handler.ViewBirthdays(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("ViewBirthdays() status = %d, want %d", w.Code, http.StatusOK)
	}

	body := w.Body.String()
	if len(body) == 0 {
		t.Error("expected non-empty response body")
	}
}

func TestImportCalendarHandler(t *testing.T) {
	t.Run("imports grouped ICS resources and injects missing uid", func(t *testing.T) {
		calRepo := &fakeCalendarRepo{
			accessible: map[string]*store.CalendarAccess{
				"1:100": {Calendar: store.Calendar{ID: 1, UserID: 200, Name: "Shared"}, Editor: true, Shared: true},
			},
		}
		eventRepo := &fakeEventRepoWithUpsert{
			fakeEventRepo: fakeEventRepo{events: make(map[string]*store.Event)},
		}
		s := &store.Store{Calendars: calRepo, Events: eventRepo}
		handler := NewHandler(&config.Config{}, s, nil)

		ics := "BEGIN:VCALENDAR\r\nVERSION:2.0\r\nBEGIN:VTIMEZONE\r\nTZID:America/Chicago\r\nEND:VTIMEZONE\r\nBEGIN:VEVENT\r\nUID:series@example.com\r\nSUMMARY:Series\r\nDTSTART;TZID=America/Chicago:20250115T140000\r\nEND:VEVENT\r\nBEGIN:VEVENT\r\nUID:series@example.com\r\nRECURRENCE-ID;TZID=America/Chicago:20250122T140000\r\nSUMMARY:Override\r\nDTSTART;TZID=America/Chicago:20250122T150000\r\nEND:VEVENT\r\nBEGIN:VEVENT\r\nSUMMARY:No UID\r\nDTSTART:20250116T140000Z\r\nEND:VEVENT\r\nEND:VCALENDAR\r\n"
		req := newICSImportRequest(t, "/calendars/1/import", "calendar.ics", ics)
		req = withRouteID(req, "1")
		req = req.WithContext(auth.WithUser(req.Context(), &store.User{ID: 100, PrimaryEmail: "editor@example.com"}))

		w := httptest.NewRecorder()
		handler.ImportCalendar(w, req)

		if w.Code != http.StatusFound {
			t.Fatalf("ImportCalendar() status = %d, want %d", w.Code, http.StatusFound)
		}
		location := w.Header().Get("Location")
		if !strings.Contains(location, "status=Imported+") {
			t.Fatalf("expected success status in redirect, got %s", location)
		}
		if len(eventRepo.events) != 2 {
			t.Fatalf("expected 2 stored resources, got %d", len(eventRepo.events))
		}

		series := eventRepo.events["1:series@example.com"]
		if series == nil {
			t.Fatalf("expected recurring series resource to be stored")
		}
		if strings.Count(series.RawICAL, "BEGIN:VEVENT") != 2 {
			t.Fatalf("expected grouped recurring resource, got: %s", series.RawICAL)
		}
		if !strings.Contains(series.RawICAL, "BEGIN:VTIMEZONE") {
			t.Fatalf("expected VTIMEZONE to be preserved, got: %s", series.RawICAL)
		}
		if series.ResourceName != "series_example.com.ics" {
			t.Fatalf("unexpected resource name: %s", series.ResourceName)
		}

		var generated *store.Event
		for key, ev := range eventRepo.events {
			if key == "1:series@example.com" {
				continue
			}
			generated = ev
		}
		if generated == nil || generated.UID == "" {
			t.Fatalf("expected generated UID event to be stored")
		}
		if !strings.Contains(generated.RawICAL, "UID:"+generated.UID) {
			t.Fatalf("expected generated UID to be injected into ICS, got: %s", generated.RawICAL)
		}
	})

	t.Run("rejects malformed ICS", func(t *testing.T) {
		calRepo := &fakeCalendarRepo{
			calendars: map[int64]*store.Calendar{
				1: {ID: 1, UserID: 100, Name: "Test Calendar"},
			},
		}
		eventRepo := &fakeEventRepoWithUpsert{
			fakeEventRepo: fakeEventRepo{events: make(map[string]*store.Event)},
		}
		s := &store.Store{Calendars: calRepo, Events: eventRepo}
		handler := NewHandler(&config.Config{}, s, nil)

		req := newICSImportRequest(t, "/calendars/1/import", "bad.ics", "BEGIN:VEVENT\r\nEND:VEVENT\r\n")
		req = withRouteID(req, "1")
		req = req.WithContext(auth.WithUser(req.Context(), &store.User{ID: 100, PrimaryEmail: "owner@example.com"}))

		w := httptest.NewRecorder()
		handler.ImportCalendar(w, req)

		if w.Code != http.StatusFound {
			t.Fatalf("ImportCalendar() status = %d, want %d", w.Code, http.StatusFound)
		}
		if location := w.Header().Get("Location"); !strings.Contains(location, "error=invalid+ICS+file") {
			t.Fatalf("expected invalid ICS redirect, got %s", location)
		}
		if len(eventRepo.events) != 0 {
			t.Fatalf("expected no stored events, got %d", len(eventRepo.events))
		}
	})

	t.Run("forbids read only shared calendars", func(t *testing.T) {
		calRepo := &fakeCalendarRepo{
			accessible: map[string]*store.CalendarAccess{
				"1:100": {Calendar: store.Calendar{ID: 1, UserID: 200, Name: "Shared"}, Editor: false, Shared: true},
			},
		}
		s := &store.Store{Calendars: calRepo, Events: &fakeEventRepoWithUpsert{fakeEventRepo: fakeEventRepo{events: make(map[string]*store.Event)}}}
		handler := NewHandler(&config.Config{}, s, nil)

		req := newICSImportRequest(t, "/calendars/1/import", "calendar.ics", "BEGIN:VCALENDAR\r\nBEGIN:VEVENT\r\nUID:test\r\nEND:VEVENT\r\nEND:VCALENDAR\r\n")
		req = withRouteID(req, "1")
		req = req.WithContext(auth.WithUser(req.Context(), &store.User{ID: 100, PrimaryEmail: "viewer@example.com"}))

		w := httptest.NewRecorder()
		handler.ImportCalendar(w, req)

		if w.Code != http.StatusForbidden {
			t.Fatalf("ImportCalendar() status = %d, want %d", w.Code, http.StatusForbidden)
		}
	})

	t.Run("requires_write_content_when_import_overwrites_existing_resource", func(t *testing.T) {
		calRepo := &fakeCalendarRepo{
			accessible: map[string]*store.CalendarAccess{
				"1:100": {Calendar: store.Calendar{ID: 1, UserID: 200, Name: "Shared"}, Editor: true, Shared: true},
			},
		}
		eventRepo := &fakeEventRepoWithUpsert{
			fakeEventRepo: fakeEventRepo{
				events: map[string]*store.Event{
					"1:existing@example.com": {
						CalendarID:   1,
						UID:          "existing@example.com",
						ResourceName: "existing_example.com.ics",
						RawICAL:      "BEGIN:VCALENDAR\r\nBEGIN:VEVENT\r\nUID:existing@example.com\r\nSUMMARY:Original\r\nEND:VEVENT\r\nEND:VCALENDAR\r\n",
						ETag:         "etag-existing",
					},
				},
			},
		}
		aclRepo := &fakeACLRepo{entries: []store.ACLEntry{
			{ResourcePath: "/dav/calendars/1", PrincipalHref: "/dav/principals/100/", IsGrant: true, Privilege: "read"},
			{ResourcePath: "/dav/calendars/1", PrincipalHref: "/dav/principals/100/", IsGrant: true, Privilege: "bind"},
		}}
		handler := NewHandler(&config.Config{}, &store.Store{
			Calendars:  calRepo,
			Events:     eventRepo,
			ACLEntries: aclRepo,
		}, nil)

		ics := "BEGIN:VCALENDAR\r\nVERSION:2.0\r\nBEGIN:VEVENT\r\nUID:existing@example.com\r\nSUMMARY:Updated\r\nDTSTART:20250115T140000Z\r\nEND:VEVENT\r\nEND:VCALENDAR\r\n"
		req := newICSImportRequest(t, "/calendars/1/import", "calendar.ics", ics)
		req = withRouteID(req, "1")
		req = req.WithContext(auth.WithUser(req.Context(), &store.User{ID: 100, PrimaryEmail: "delegate@example.com"}))

		w := httptest.NewRecorder()
		handler.ImportCalendar(w, req)

		if w.Code != http.StatusForbidden {
			t.Fatalf("ImportCalendar() status = %d, want %d", w.Code, http.StatusForbidden)
		}
		if strings.Contains(eventRepo.events["1:existing@example.com"].RawICAL, "SUMMARY:Updated") {
			t.Fatalf("expected existing event to remain unchanged, got %s", eventRepo.events["1:existing@example.com"].RawICAL)
		}
	})

	t.Run("does not partially import before a later authorization failure", func(t *testing.T) {
		calRepo := &fakeCalendarRepo{
			accessible: map[string]*store.CalendarAccess{
				"1:100": {Calendar: store.Calendar{ID: 1, UserID: 200, Name: "Shared"}, Editor: true, Shared: true},
			},
		}
		eventRepo := &fakeEventRepoWithUpsert{
			fakeEventRepo: fakeEventRepo{
				events: map[string]*store.Event{
					"1:existing@example.com": {
						CalendarID:   1,
						UID:          "existing@example.com",
						ResourceName: "existing_example.com.ics",
						RawICAL:      "BEGIN:VCALENDAR\r\nBEGIN:VEVENT\r\nUID:existing@example.com\r\nSUMMARY:Original\r\nDTSTART:20250115T140000Z\r\nEND:VEVENT\r\nEND:VCALENDAR\r\n",
						ETag:         "etag-existing",
					},
				},
			},
		}
		aclRepo := &fakeACLRepo{entries: []store.ACLEntry{
			{ResourcePath: "/dav/calendars/1", PrincipalHref: "/dav/principals/100/", IsGrant: true, Privilege: "read"},
			{ResourcePath: "/dav/calendars/1", PrincipalHref: "/dav/principals/100/", IsGrant: true, Privilege: "bind"},
		}}
		handler := NewHandler(&config.Config{}, &store.Store{
			Calendars:  calRepo,
			Events:     eventRepo,
			ACLEntries: aclRepo,
		}, nil)

		ics := "BEGIN:VCALENDAR\r\nVERSION:2.0\r\nBEGIN:VEVENT\r\nUID:new@example.com\r\nSUMMARY:New Event\r\nDTSTART:20250110T120000Z\r\nEND:VEVENT\r\nBEGIN:VEVENT\r\nUID:existing@example.com\r\nSUMMARY:Updated Existing\r\nDTSTART:20250115T140000Z\r\nEND:VEVENT\r\nEND:VCALENDAR\r\n"
		req := newICSImportRequest(t, "/calendars/1/import", "calendar.ics", ics)
		req = withRouteID(req, "1")
		req = req.WithContext(auth.WithUser(req.Context(), &store.User{ID: 100, PrimaryEmail: "delegate@example.com"}))

		w := httptest.NewRecorder()
		handler.ImportCalendar(w, req)

		if w.Code != http.StatusForbidden {
			t.Fatalf("ImportCalendar() status = %d, want %d", w.Code, http.StatusForbidden)
		}
		if _, ok := eventRepo.events["1:new@example.com"]; ok {
			t.Fatalf("expected preflight failure to prevent partial import, got %#v", eventRepo.events)
		}
		if strings.Contains(eventRepo.events["1:existing@example.com"].RawICAL, "Updated Existing") {
			t.Fatalf("expected existing event to remain unchanged, got %s", eventRepo.events["1:existing@example.com"].RawICAL)
		}
	})
}

func TestDashboardHidesDeniedRecentEvents(t *testing.T) {
	visibleSummary := "Visible Event"
	hiddenSummary := "Hidden Event"
	start := time.Date(2026, 3, 20, 10, 0, 0, 0, time.UTC)

	handler := NewHandler(&config.Config{}, &store.Store{
		Calendars: &fakeCalendarRepo{listAccessible: []store.CalendarAccess{
			{Calendar: store.Calendar{ID: 1, UserID: 200, Name: "Shared"}, Shared: true, Editor: false, OwnerEmail: "owner@example.com"},
		}},
		AddressBooks: &fakeAddressBookRepo{books: map[int64]*store.AddressBook{}},
		AppPasswords: &fakeAppPasswordRepo{},
		Events: &fakeEventRepo{recent: []store.Event{
			{CalendarID: 1, UID: "visible", ResourceName: "visible", Summary: &visibleSummary, DTStart: &start},
			{CalendarID: 1, UID: "hidden", ResourceName: "hidden", Summary: &hiddenSummary, DTStart: &start},
		}},
		Contacts: &fakeContactRepo{contacts: map[string]*store.Contact{}},
		ACLEntries: &fakeACLRepo{entries: []store.ACLEntry{
			{ResourcePath: "/dav/calendars/1", PrincipalHref: "/dav/principals/100/", IsGrant: true, Privilege: "read"},
			{ResourcePath: "/dav/calendars/1/hidden", PrincipalHref: "/dav/principals/100/", IsGrant: false, Privilege: "read"},
		}},
	}, nil)

	req := httptest.NewRequest(http.MethodGet, "/", nil)
	req = req.WithContext(auth.WithUser(req.Context(), &store.User{ID: 100, PrimaryEmail: "delegate@example.com"}))
	w := httptest.NewRecorder()

	handler.Dashboard(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("Dashboard() status = %d, want %d", w.Code, http.StatusOK)
	}
	body := w.Body.String()
	if !strings.Contains(body, visibleSummary) {
		t.Fatalf("expected visible event in dashboard, got %s", body)
	}
	if !strings.Contains(body, `href="/calendars/1?event=visible&amp;month=3&amp;year=2026"`) {
		t.Fatalf("expected dashboard event link to include event month, got %s", body)
	}
	if strings.Contains(body, hiddenSummary) {
		t.Fatalf("expected denied event to be hidden from dashboard, got %s", body)
	}
}

func TestDashboardEventURLFallsBackWithoutStartDate(t *testing.T) {
	got := dashboardEventURL(store.Event{CalendarID: 7, UID: "event with space"})
	want := "/calendars/7?event=event+with+space"
	if got != want {
		t.Fatalf("dashboardEventURL() = %q, want %q", got, want)
	}
}

func newDashboardTestStore() *store.Store {
	return &store.Store{
		Calendars:    &fakeCalendarRepo{listAccessible: []store.CalendarAccess{}},
		AddressBooks: &fakeAddressBookRepo{books: map[int64]*store.AddressBook{}},
		AppPasswords: &fakeAppPasswordRepo{},
		Events:       &fakeEventRepo{recent: []store.Event{}},
		Contacts:     &fakeContactRepo{contacts: map[string]*store.Contact{}},
		ACLEntries:   &fakeACLRepo{entries: []store.ACLEntry{}},
	}
}

func TestDashboardShowsWelcomeTourOnFirstLogin(t *testing.T) {
	handler := NewHandler(&config.Config{}, newDashboardTestStore(), nil)

	req := httptest.NewRequest(http.MethodGet, "/", nil)
	req = req.WithContext(auth.WithUser(req.Context(), &store.User{
		ID:           100,
		PrimaryEmail: "new@example.com",
		FullName:     "Dana Lee",
		FirstName:    "Dana",
	}))
	w := httptest.NewRecorder()

	handler.Dashboard(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("Dashboard() status = %d, want %d", w.Code, http.StatusOK)
	}
	if !strings.Contains(w.Body.String(), "Welcome to CalCard") {
		t.Fatalf("expected welcome tour for first-login user, got %s", w.Body.String())
	}
	if !strings.Contains(w.Body.String(), "Welcome, Dana") || !strings.Contains(w.Body.String(), "Hi Dana") {
		t.Fatalf("expected dashboard to use OAuth first name, got %s", w.Body.String())
	}
}

func TestDashboardHidesWelcomeTourAfterOnboarding(t *testing.T) {
	handler := NewHandler(&config.Config{}, newDashboardTestStore(), nil)

	completed := time.Now()
	req := httptest.NewRequest(http.MethodGet, "/", nil)
	req = req.WithContext(auth.WithUser(req.Context(), &store.User{ID: 100, PrimaryEmail: "old@example.com", OnboardingCompletedAt: &completed}))
	w := httptest.NewRecorder()

	handler.Dashboard(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("Dashboard() status = %d, want %d", w.Code, http.StatusOK)
	}
	if strings.Contains(w.Body.String(), "Welcome to CalCard") {
		t.Fatalf("expected no welcome tour for onboarded user, got %s", w.Body.String())
	}
}

func TestCompleteOnboardingMarksUser(t *testing.T) {
	userRepo := &fakeUserRepo{users: map[int64]*store.User{100: {ID: 100, PrimaryEmail: "user@example.com"}}}
	handler := NewHandler(&config.Config{}, &store.Store{Users: userRepo}, nil)

	req := httptest.NewRequest(http.MethodPost, "/onboarding/complete", nil)
	req = req.WithContext(auth.WithUser(req.Context(), &store.User{ID: 100, PrimaryEmail: "user@example.com"}))
	w := httptest.NewRecorder()

	handler.CompleteOnboarding(w, req)

	if w.Code != http.StatusNoContent {
		t.Fatalf("CompleteOnboarding() status = %d, want %d", w.Code, http.StatusNoContent)
	}
	if userRepo.users[100].OnboardingCompletedAt == nil {
		t.Fatalf("expected onboarding to be marked complete")
	}
}

func TestCreateCalendarPersistsSelectedColor(t *testing.T) {
	calRepo := &fakeCalendarRepo{calendars: map[int64]*store.Calendar{}}
	handler := NewHandler(&config.Config{}, &store.Store{Calendars: calRepo}, nil)

	form := url.Values{}
	form.Set("name", "Work")
	form.Set("color", "#22cc88")
	form.Set("color_alpha", "75")
	req := httptest.NewRequest(http.MethodPost, "/calendars", strings.NewReader(form.Encode()))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	req = req.WithContext(auth.WithUser(req.Context(), &store.User{ID: 100, PrimaryEmail: "user@example.com"}))
	w := httptest.NewRecorder()

	handler.CreateCalendar(w, req)

	if w.Code != http.StatusFound {
		t.Fatalf("CreateCalendar() status = %d, want %d", w.Code, http.StatusFound)
	}
	cal := calRepo.calendars[1]
	if cal == nil || cal.Color == nil || *cal.Color != "#22CC88BF" {
		t.Fatalf("expected selected color to be persisted, got %#v", cal)
	}
}

func TestRenameCalendarPersistsSelectedColor(t *testing.T) {
	calRepo := &fakeCalendarRepo{calendars: map[int64]*store.Calendar{
		1: {ID: 1, UserID: 100, Name: "Work"},
	}}
	handler := NewHandler(&config.Config{}, &store.Store{Calendars: calRepo}, nil)

	form := url.Values{}
	form.Set("name", "Personal")
	form.Set("color", "#336699")
	form.Set("color_alpha", "50")
	req := httptest.NewRequest(http.MethodPost, "/calendars/1", strings.NewReader(form.Encode()))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	req = withRouteID(req, "1")
	req = req.WithContext(auth.WithUser(req.Context(), &store.User{ID: 100, PrimaryEmail: "user@example.com"}))
	w := httptest.NewRecorder()

	handler.RenameCalendar(w, req)

	if w.Code != http.StatusFound {
		t.Fatalf("RenameCalendar() status = %d, want %d", w.Code, http.StatusFound)
	}
	cal := calRepo.calendars[1]
	if cal == nil || cal.Name != "Personal" || cal.Color == nil || *cal.Color != "#33669980" {
		t.Fatalf("expected renamed calendar color to be persisted, got %#v", cal)
	}
}

func TestCalendarsPageRendersEditModalWithCurrentValues(t *testing.T) {
	color := "#33669980"
	calRepo := &fakeCalendarRepo{calendars: map[int64]*store.Calendar{
		1: {ID: 1, UserID: 100, Name: "Work", Color: &color},
	}}
	handler := NewHandler(&config.Config{}, &store.Store{
		Calendars:  calRepo,
		Users:      &fakeUserRepo{users: map[int64]*store.User{100: {ID: 100, PrimaryEmail: "user@example.com"}}},
		ACLEntries: &fakeACLRepo{},
	}, nil)

	req := httptest.NewRequest(http.MethodGet, "/calendars", nil)
	req = req.WithContext(auth.WithUser(req.Context(), &store.User{ID: 100, PrimaryEmail: "user@example.com"}))
	w := httptest.NewRecorder()

	handler.Calendars(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("Calendars() status = %d, want %d: %s", w.Code, http.StatusOK, w.Body.String())
	}
	body := w.Body.String()
	expected := []string{
		`<div class="calendar-card-header" style="background: #33669980;">`,
		`onclick="openModal('edit-calendar-1')"`,
		`id="edit-calendar-1"`,
		`<input type="text" name="name" value="Work" required>`,
		`<input type="color" name="color" value="#336699">`,
		`<input type="range" name="color_alpha" min="0" max="100" value="50" class="color-alpha-input">`,
		`>Cancel</button>`,
		`>Save</button>`,
	}
	for _, want := range expected {
		if !strings.Contains(body, want) {
			t.Fatalf("expected calendars page to contain %q, got %s", want, body)
		}
	}
	if strings.Contains(body, `class="rename-form"`) {
		t.Fatalf("expected inline rename form to be removed, got %s", body)
	}
}

func TestAppPasswordsUsesConfiguredDAVEndpoint(t *testing.T) {
	handler := NewHandler(&config.Config{BaseURL: "https://calcard.example/"}, &store.Store{
		AppPasswords: &fakeAppPasswordRepo{},
	}, nil)

	req := httptest.NewRequest(http.MethodGet, "/app-passwords", nil)
	req = req.WithContext(auth.WithUser(req.Context(), &store.User{ID: 100, PrimaryEmail: "user@example.com"}))
	w := httptest.NewRecorder()

	handler.AppPasswords(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("AppPasswords() status = %d, want %d: %s", w.Code, http.StatusOK, w.Body.String())
	}
	body := w.Body.String()
	if !strings.Contains(body, "https://calcard.example/dav") {
		t.Fatalf("expected configured DAV endpoint in response, got %s", body)
	}
	if strings.Contains(body, "example.app.calcard.app") {
		t.Fatalf("expected app password guidance not to contain hard-coded example host, got %s", body)
	}
}

func TestDashboardBackfillsVisibleRecentEventsAfterFiltering(t *testing.T) {
	start := time.Date(2026, 3, 20, 10, 0, 0, 0, time.UTC)
	recent := make([]store.Event, 0, 30)
	for i := 1; i <= 25; i++ {
		summary := fmt.Sprintf("Hidden Event %d", i)
		s := summary
		recent = append(recent, store.Event{
			CalendarID:   1,
			UID:          fmt.Sprintf("hidden-%d", i),
			ResourceName: fmt.Sprintf("hidden-%d", i),
			Summary:      &s,
			DTStart:      &start,
		})
	}
	for i := 1; i <= 5; i++ {
		summary := fmt.Sprintf("Visible Event %d", i)
		s := summary
		recent = append(recent, store.Event{
			CalendarID:   1,
			UID:          fmt.Sprintf("visible-%d", i),
			ResourceName: fmt.Sprintf("visible-%d", i),
			Summary:      &s,
			DTStart:      &start,
		})
	}

	handler := NewHandler(&config.Config{}, &store.Store{
		Calendars: &fakeCalendarRepo{listAccessible: []store.CalendarAccess{
			{Calendar: store.Calendar{ID: 1, UserID: 200, Name: "Shared"}, Shared: true, Editor: false, OwnerEmail: "owner@example.com"},
		}},
		AddressBooks: &fakeAddressBookRepo{books: map[int64]*store.AddressBook{}},
		AppPasswords: &fakeAppPasswordRepo{},
		Events:       &fakeEventRepo{recent: recent},
		Contacts:     &fakeContactRepo{contacts: map[string]*store.Contact{}},
		ACLEntries:   buildHiddenRecentEventACLs(25),
	}, nil)

	req := httptest.NewRequest(http.MethodGet, "/", nil)
	req = req.WithContext(auth.WithUser(req.Context(), &store.User{ID: 100, PrimaryEmail: "delegate@example.com"}))
	w := httptest.NewRecorder()

	handler.Dashboard(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("Dashboard() status = %d, want %d", w.Code, http.StatusOK)
	}
	body := w.Body.String()
	for _, summary := range []string{"Visible Event 1", "Visible Event 2", "Visible Event 3", "Visible Event 4", "Visible Event 5"} {
		if !strings.Contains(body, summary) {
			t.Fatalf("expected %q in dashboard, got %s", summary, body)
		}
	}
	if strings.Contains(body, "Hidden Event") {
		t.Fatalf("expected denied recent event to be replaced, got %s", body)
	}
}

func TestDashboardShowsRecentEventsWithDirectObjectReadGrant(t *testing.T) {
	summary := "Direct Object Event"
	start := time.Date(2026, 3, 20, 10, 0, 0, 0, time.UTC)

	handler := NewHandler(&config.Config{}, &store.Store{
		Calendars: &fakeCalendarRepo{
			calendars: map[int64]*store.Calendar{
				1: {ID: 1, UserID: 200, Name: "Object Shared"},
			},
			listAccessible: []store.CalendarAccess{},
		},
		AddressBooks: &fakeAddressBookRepo{books: map[int64]*store.AddressBook{}},
		AppPasswords: &fakeAppPasswordRepo{},
		Events: &fakeEventRepo{recent: []store.Event{
			{CalendarID: 1, UID: "direct", ResourceName: "direct", Summary: &summary, DTStart: &start},
		}},
		Contacts: &fakeContactRepo{contacts: map[string]*store.Contact{}},
		ACLEntries: &fakeACLRepo{entries: []store.ACLEntry{
			{ResourcePath: "/dav/calendars/1/direct", PrincipalHref: "/dav/principals/100/", IsGrant: true, Privilege: "read"},
		}},
	}, nil)

	req := httptest.NewRequest(http.MethodGet, "/", nil)
	req = req.WithContext(auth.WithUser(req.Context(), &store.User{ID: 100, PrimaryEmail: "delegate@example.com"}))
	w := httptest.NewRecorder()

	handler.Dashboard(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("Dashboard() status = %d, want %d", w.Code, http.StatusOK)
	}
	body := w.Body.String()
	if !strings.Contains(body, summary) {
		t.Fatalf("expected direct object grant event in dashboard, got %s", body)
	}
	if !strings.Contains(body, "Object Shared") {
		t.Fatalf("expected dashboard to resolve calendar name for object grant event, got %s", body)
	}
}

func TestCalendarEventJSONHidesDeniedEvents(t *testing.T) {
	start := time.Date(2026, 3, 20, 10, 0, 0, 0, time.UTC)

	handler := NewHandler(&config.Config{}, &store.Store{
		Calendars: &fakeCalendarRepo{
			accessible: map[string]*store.CalendarAccess{
				"1:100": {Calendar: store.Calendar{ID: 1, UserID: 200, Name: "Shared"}, Shared: true, Editor: false},
			},
		},
		Events: &fakeEventRepo{events: map[string]*store.Event{
			"1:visible": {CalendarID: 1, UID: "visible", ResourceName: "visible", RawICAL: "BEGIN:VCALENDAR\r\nBEGIN:VEVENT\r\nUID:visible\r\nEND:VEVENT\r\nEND:VCALENDAR\r\n", DTStart: &start},
			"1:hidden":  {CalendarID: 1, UID: "hidden", ResourceName: "hidden", RawICAL: "BEGIN:VCALENDAR\r\nBEGIN:VEVENT\r\nUID:hidden\r\nEND:VEVENT\r\nEND:VCALENDAR\r\n", DTStart: &start},
		}},
		ACLEntries: &fakeACLRepo{entries: []store.ACLEntry{
			{ResourcePath: "/dav/calendars/1", PrincipalHref: "/dav/principals/100/", IsGrant: true, Privilege: "read"},
			{ResourcePath: "/dav/calendars/1/hidden", PrincipalHref: "/dav/principals/100/", IsGrant: false, Privilege: "read"},
		}},
	}, nil)

	req := httptest.NewRequest(http.MethodGet, "/calendars/1/events.json?year=2026&month=3", nil)
	req = withRouteID(req, "1")
	req = req.WithContext(auth.WithUser(req.Context(), &store.User{ID: 100, PrimaryEmail: "delegate@example.com"}))
	w := httptest.NewRecorder()

	handler.GetCalendarEventsJSON(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("GetCalendarEventsJSON() status = %d, want %d", w.Code, http.StatusOK)
	}
	body := w.Body.String()
	if !strings.Contains(body, `"uid":"visible"`) {
		t.Fatalf("expected visible event in JSON response, got %s", body)
	}
	if strings.Contains(body, `"uid":"hidden"`) {
		t.Fatalf("expected denied event to be omitted from JSON response, got %s", body)
	}
}

func TestCalendarEventJSONReturnsEmptyArrayWhenNoEventsMatch(t *testing.T) {
	handler := NewHandler(&config.Config{}, &store.Store{
		Calendars: &fakeCalendarRepo{
			accessible: map[string]*store.CalendarAccess{
				"1:100": {Calendar: store.Calendar{ID: 1, UserID: 100, Name: "Empty"}, Shared: false, Editor: true},
			},
		},
		Events: &fakeEventRepo{events: map[string]*store.Event{}},
	}, nil)

	req := httptest.NewRequest(http.MethodGet, "/calendars/1/events.json?year=2026&month=4", nil)
	req = withRouteID(req, "1")
	req = req.WithContext(auth.WithUser(req.Context(), &store.User{ID: 100, PrimaryEmail: "owner@example.com"}))
	w := httptest.NewRecorder()

	handler.GetCalendarEventsJSON(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("GetCalendarEventsJSON() status = %d, want %d", w.Code, http.StatusOK)
	}
	if body := strings.TrimSpace(w.Body.String()); body != "[]" {
		t.Fatalf("expected empty JSON array, got %s", body)
	}
}

func TestCalendarEventJSONSupportsExplicitDateRange(t *testing.T) {
	inRange := time.Date(2026, 3, 20, 10, 0, 0, 0, time.UTC)
	outOfRange := time.Date(2026, 5, 20, 10, 0, 0, 0, time.UTC)

	handler := NewHandler(&config.Config{}, &store.Store{
		Calendars: &fakeCalendarRepo{
			accessible: map[string]*store.CalendarAccess{
				"1:100": {Calendar: store.Calendar{ID: 1, UserID: 100, Name: "Work"}, Editor: true},
			},
		},
		Events: &fakeEventRepo{events: map[string]*store.Event{
			"1:in":  {CalendarID: 1, UID: "in", ResourceName: "in", RawICAL: "BEGIN:VCALENDAR\r\nBEGIN:VEVENT\r\nUID:in\r\nEND:VEVENT\r\nEND:VCALENDAR\r\n", DTStart: &inRange},
			"1:out": {CalendarID: 1, UID: "out", ResourceName: "out", RawICAL: "BEGIN:VCALENDAR\r\nBEGIN:VEVENT\r\nUID:out\r\nEND:VEVENT\r\nEND:VCALENDAR\r\n", DTStart: &outOfRange},
		}},
	}, nil)

	req := httptest.NewRequest(http.MethodGet, "/calendars/1/events.json?start=2026-03-01&end=2026-04-01", nil)
	req = withRouteID(req, "1")
	req = req.WithContext(auth.WithUser(req.Context(), &store.User{ID: 100, PrimaryEmail: "owner@example.com"}))
	w := httptest.NewRecorder()

	handler.GetCalendarEventsJSON(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("GetCalendarEventsJSON() status = %d, want %d", w.Code, http.StatusOK)
	}
	body := w.Body.String()
	if !strings.Contains(body, `"uid":"in"`) {
		t.Fatalf("expected in-range event in JSON response, got %s", body)
	}
	if strings.Contains(body, `"uid":"out"`) {
		t.Fatalf("expected out-of-range event to be omitted, got %s", body)
	}
}

func TestCalendarEventJSONRejectsInvalidDateRange(t *testing.T) {
	handler := NewHandler(&config.Config{}, &store.Store{
		Calendars: &fakeCalendarRepo{
			accessible: map[string]*store.CalendarAccess{
				"1:100": {Calendar: store.Calendar{ID: 1, UserID: 100, Name: "Work"}, Editor: true},
			},
		},
		Events: &fakeEventRepo{events: map[string]*store.Event{}},
	}, nil)

	req := httptest.NewRequest(http.MethodGet, "/calendars/1/events.json?start=2026-04-01&end=2026-03-01", nil)
	req = withRouteID(req, "1")
	req = req.WithContext(auth.WithUser(req.Context(), &store.User{ID: 100, PrimaryEmail: "owner@example.com"}))
	w := httptest.NewRecorder()

	handler.GetCalendarEventsJSON(w, req)

	if w.Code != http.StatusBadRequest {
		t.Fatalf("GetCalendarEventsJSON() status = %d, want %d", w.Code, http.StatusBadRequest)
	}
}

func TestExportCalendarDownloadsReadableEventsAsICS(t *testing.T) {
	handler := NewHandler(&config.Config{}, &store.Store{
		Calendars: &fakeCalendarRepo{
			accessible: map[string]*store.CalendarAccess{
				"1:100": {Calendar: store.Calendar{ID: 1, UserID: 100, Name: "Work Calendar"}, Shared: false, Editor: true},
			},
		},
		Events: &fakeEventRepo{events: map[string]*store.Event{
			"1:event-1": {
				CalendarID:   1,
				UID:          "event-1",
				ResourceName: "event-1",
				RawICAL: "BEGIN:VCALENDAR\r\nVERSION:2.0\r\nPRODID:-//Import//EN\r\nBEGIN:VTIMEZONE\r\nTZID:America/Chicago\r\nEND:VTIMEZONE\r\n" +
					"BEGIN:VEVENT\r\nUID:event-1\r\nSUMMARY:Planning\r\nDTSTART;TZID=America/Chicago:20260401T090000\r\nEND:VEVENT\r\nEND:VCALENDAR\r\n",
			},
			"1:event-2": {
				CalendarID:   1,
				UID:          "event-2",
				ResourceName: "event-2",
				RawICAL:      "BEGIN:VCALENDAR\r\nVERSION:2.0\r\nBEGIN:VEVENT\r\nUID:event-2\r\nSUMMARY:Review\r\nEND:VEVENT\r\nEND:VCALENDAR\r\n",
			},
		}},
	}, nil)

	req := httptest.NewRequest(http.MethodGet, "/calendars/1/export", nil)
	req = withRouteID(req, "1")
	req = req.WithContext(auth.WithUser(req.Context(), &store.User{ID: 100, PrimaryEmail: "owner@example.com"}))
	w := httptest.NewRecorder()

	handler.ExportCalendar(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("ExportCalendar() status = %d, want %d", w.Code, http.StatusOK)
	}
	if got := w.Header().Get("Content-Type"); got != "text/calendar; charset=utf-8" {
		t.Fatalf("Content-Type = %q, want text/calendar; charset=utf-8", got)
	}
	if got := w.Header().Get("Content-Disposition"); got != `attachment; filename="work-calendar.ics"` {
		t.Fatalf("Content-Disposition = %q, want export filename", got)
	}
	body := w.Body.String()
	for _, want := range []string{
		"BEGIN:VCALENDAR\r\n",
		"PRODID:-//CalCard//Calendar Export//EN\r\n",
		"X-WR-CALNAME:Work Calendar\r\n",
		"BEGIN:VTIMEZONE\r\nTZID:America/Chicago\r\nEND:VTIMEZONE\r\n",
		"BEGIN:VEVENT\r\nUID:event-1\r\nSUMMARY:Planning\r\nDTSTART;TZID=America/Chicago:20260401T090000\r\nEND:VEVENT\r\n",
		"BEGIN:VEVENT\r\nUID:event-2\r\nSUMMARY:Review\r\nEND:VEVENT\r\n",
		"END:VCALENDAR\r\n",
	} {
		if !strings.Contains(body, want) {
			t.Fatalf("expected export to contain %q, got %s", want, body)
		}
	}
	if strings.Count(body, "BEGIN:VCALENDAR") != 1 {
		t.Fatalf("expected one VCALENDAR wrapper, got %s", body)
	}
}

func TestExportCalendarHidesDeniedEvents(t *testing.T) {
	handler := NewHandler(&config.Config{}, &store.Store{
		Calendars: &fakeCalendarRepo{
			accessible: map[string]*store.CalendarAccess{
				"1:100": {Calendar: store.Calendar{ID: 1, UserID: 200, Name: "Shared"}, Shared: true, Editor: false},
			},
		},
		Events: &fakeEventRepo{events: map[string]*store.Event{
			"1:visible": {CalendarID: 1, UID: "visible", ResourceName: "visible", RawICAL: "BEGIN:VCALENDAR\r\nBEGIN:VEVENT\r\nUID:visible\r\nSUMMARY:Visible\r\nEND:VEVENT\r\nEND:VCALENDAR\r\n"},
			"1:hidden":  {CalendarID: 1, UID: "hidden", ResourceName: "hidden", RawICAL: "BEGIN:VCALENDAR\r\nBEGIN:VEVENT\r\nUID:hidden\r\nSUMMARY:Hidden\r\nEND:VEVENT\r\nEND:VCALENDAR\r\n"},
		}},
		ACLEntries: &fakeACLRepo{entries: []store.ACLEntry{
			{ResourcePath: "/dav/calendars/1", PrincipalHref: "/dav/principals/100/", IsGrant: true, Privilege: "read"},
			{ResourcePath: "/dav/calendars/1/hidden", PrincipalHref: "/dav/principals/100/", IsGrant: false, Privilege: "read"},
		}},
	}, nil)

	req := httptest.NewRequest(http.MethodGet, "/calendars/1/export", nil)
	req = withRouteID(req, "1")
	req = req.WithContext(auth.WithUser(req.Context(), &store.User{ID: 100, PrimaryEmail: "delegate@example.com"}))
	w := httptest.NewRecorder()

	handler.ExportCalendar(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("ExportCalendar() status = %d, want %d", w.Code, http.StatusOK)
	}
	body := w.Body.String()
	if !strings.Contains(body, "UID:visible") {
		t.Fatalf("expected export to contain visible event, got %s", body)
	}
	if strings.Contains(body, "UID:hidden") || strings.Contains(body, "SUMMARY:Hidden") {
		t.Fatalf("expected export to omit denied event, got %s", body)
	}
}

func TestCalendarEventJSONIncludesImportedTimezoneEventForSourceMonth(t *testing.T) {
	storedStart := time.Date(2026, 4, 1, 4, 0, 0, 0, time.UTC)

	handler := NewHandler(&config.Config{}, &store.Store{
		Calendars: &fakeCalendarRepo{
			accessible: map[string]*store.CalendarAccess{
				"1:100": {Calendar: store.Calendar{ID: 1, UserID: 100, Name: "Shared"}, Shared: false, Editor: true},
			},
		},
		Events: &fakeEventRepo{events: map[string]*store.Event{
			"1:imported": {
				CalendarID:   1,
				UID:          "imported",
				ResourceName: "imported",
				RawICAL: "BEGIN:VCALENDAR\r\nBEGIN:VEVENT\r\nUID:imported\r\nSUMMARY:Late Night Import\r\n" +
					"DTSTART;TZID=America/Chicago:20260331T230000\r\nDTEND;TZID=America/Chicago:20260401T000000\r\nEND:VEVENT\r\nEND:VCALENDAR\r\n",
				DTStart: &storedStart,
			},
		}},
	}, nil)

	req := httptest.NewRequest(http.MethodGet, "/calendars/1/events.json?year=2026&month=3", nil)
	req = withRouteID(req, "1")
	req = req.WithContext(auth.WithUser(req.Context(), &store.User{ID: 100, PrimaryEmail: "owner@example.com"}))
	w := httptest.NewRecorder()

	handler.GetCalendarEventsJSON(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("GetCalendarEventsJSON() status = %d, want %d", w.Code, http.StatusOK)
	}
	if body := w.Body.String(); !strings.Contains(body, `"uid":"imported"`) {
		t.Fatalf("expected imported timezone event in March JSON response, got %s", body)
	}
}

func TestCalendarEventJSONIncludesAppleImportedCurrentMonthEvents(t *testing.T) {
	handler := NewHandler(&config.Config{}, &store.Store{
		Calendars: &fakeCalendarRepo{
			accessible: map[string]*store.CalendarAccess{
				"1:100": {Calendar: store.Calendar{ID: 1, UserID: 100, Name: "Default"}, Shared: false, Editor: true},
			},
		},
		Events: &fakeEventRepo{events: map[string]*store.Event{
			"1:apple-april-15": {
				CalendarID:   1,
				UID:          "apple-april-15",
				ResourceName: "apple-april-15.ics",
				RawICAL: "BEGIN:VCALENDAR\r\nVERSION:2.0\r\nPRODID:-//Apple Inc.//macOS 15.3.2//EN\r\n" +
					"BEGIN:VEVENT\r\nSUMMARY:Apple April 15\r\nUID:apple-april-15\r\nCREATED:20260415T225311Z\r\n" +
					"DTSTART;TZID=America/Chicago:20260415T174500\r\nDTEND;TZID=America/Chicago:20260415T184500\r\nEND:VEVENT\r\n" +
					"BEGIN:VTIMEZONE\r\nTZID:America/Chicago\r\nEND:VTIMEZONE\r\nEND:VCALENDAR\r\n",
			},
			"1:apple-april-16": {
				CalendarID:   1,
				UID:          "apple-april-16",
				ResourceName: "apple-april-16.ics",
				RawICAL: "BEGIN:VCALENDAR\r\nVERSION:2.0\r\nPRODID:-//Apple Inc.//macOS 15.3.2//EN\r\n" +
					"BEGIN:VEVENT\r\nSUMMARY:Apple April 16\r\nUID:apple-april-16\r\nCREATED:20260415T225315Z\r\n" +
					"DTSTART;TZID=America/Chicago:20260416T193000\r\nDTEND;TZID=America/Chicago:20260416T200500\r\nEND:VEVENT\r\n" +
					"BEGIN:VTIMEZONE\r\nTZID:America/Chicago\r\nEND:VTIMEZONE\r\nEND:VCALENDAR\r\n",
			},
			"1:apple-april-14": {
				CalendarID:   1,
				UID:          "apple-april-14",
				ResourceName: "apple-april-14.ics",
				RawICAL: "BEGIN:VCALENDAR\r\nVERSION:2.0\r\nPRODID:-//Apple Inc.//macOS 15.3.2//EN\r\n" +
					"BEGIN:VEVENT\r\nSUMMARY:Apple April 14\r\nUID:apple-april-14\r\nCREATED:20260415T225319Z\r\n" +
					"DTSTART;TZID=America/Chicago:20260414T191500\r\nDTEND;TZID=America/Chicago:20260414T195500\r\nEND:VEVENT\r\n" +
					"BEGIN:VTIMEZONE\r\nTZID:America/Chicago\r\nEND:VTIMEZONE\r\nEND:VCALENDAR\r\n",
			},
		}},
	}, nil)

	req := httptest.NewRequest(http.MethodGet, "/calendars/1/events.json?year=2026&month=4", nil)
	req = withRouteID(req, "1")
	req = req.WithContext(auth.WithUser(req.Context(), &store.User{ID: 100, PrimaryEmail: "owner@example.com"}))
	w := httptest.NewRecorder()

	handler.GetCalendarEventsJSON(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("GetCalendarEventsJSON() status = %d, want %d", w.Code, http.StatusOK)
	}
	body := w.Body.String()
	for _, want := range []string{
		`"summary":"Apple April 14"`,
		`"summary":"Apple April 15"`,
		`"summary":"Apple April 16"`,
		`"timezone":"America/Chicago"`,
	} {
		if !strings.Contains(body, want) {
			t.Fatalf("expected JSON to contain %s, got %s", want, body)
		}
	}
}

func TestCalendarEventJSONIgnoresTimezoneRRuleWhenFilteringMonth(t *testing.T) {
	handler := NewHandler(&config.Config{}, &store.Store{
		Calendars: &fakeCalendarRepo{
			accessible: map[string]*store.CalendarAccess{
				"1:100": {Calendar: store.Calendar{ID: 1, UserID: 100, Name: "Default"}, Shared: false, Editor: true},
			},
		},
		Events: &fakeEventRepo{events: map[string]*store.Event{
			"1:apple-timezone-rrule": {
				CalendarID:   1,
				UID:          "apple-timezone-rrule",
				ResourceName: "apple-timezone-rrule.ics",
				RawICAL: "BEGIN:VCALENDAR\r\nVERSION:2.0\r\nBEGIN:VTIMEZONE\r\nTZID:America/Chicago\r\n" +
					"BEGIN:DAYLIGHT\r\nDTSTART:19180331T020000\r\nRRULE:FREQ=YEARLY;UNTIL=19190330T080000Z;BYMONTH=3;BYDAY=-1SU\r\nEND:DAYLIGHT\r\n" +
					"BEGIN:STANDARD\r\nDTSTART:19181027T020000\r\nRRULE:FREQ=YEARLY;UNTIL=19191026T070000Z;BYMONTH=10;BYDAY=-1SU\r\nEND:STANDARD\r\n" +
					"END:VTIMEZONE\r\nBEGIN:VEVENT\r\nUID:apple-timezone-rrule\r\nSUMMARY:Apple Timezone RRule\r\n" +
					"DTSTART;TZID=America/Chicago:20260415T174500\r\nDTEND;TZID=America/Chicago:20260415T184500\r\nEND:VEVENT\r\nEND:VCALENDAR\r\n",
			},
		}},
	}, nil)

	req := httptest.NewRequest(http.MethodGet, "/calendars/1/events.json?year=2026&month=4", nil)
	req = withRouteID(req, "1")
	req = req.WithContext(auth.WithUser(req.Context(), &store.User{ID: 100, PrimaryEmail: "owner@example.com"}))
	w := httptest.NewRecorder()

	handler.GetCalendarEventsJSON(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("GetCalendarEventsJSON() status = %d, want %d", w.Code, http.StatusOK)
	}
	if body := w.Body.String(); !strings.Contains(body, `"summary":"Apple Timezone RRule"`) {
		t.Fatalf("expected VTIMEZONE RRULE not to exclude event, got %s", body)
	}
}

func TestCalendarEventJSONPrefersRawTimezoneDateOverStoredDate(t *testing.T) {
	storedStart := time.Date(2026, 4, 14, 19, 15, 0, 0, time.UTC)
	storedEnd := time.Date(2026, 4, 14, 19, 55, 0, 0, time.UTC)

	payload := calendarEventJSON(store.Event{
		CalendarID:   1,
		UID:          "apple-timezone",
		ResourceName: "apple-timezone.ics",
		RawICAL: "BEGIN:VCALENDAR\r\nVERSION:2.0\r\nBEGIN:VEVENT\r\nUID:apple-timezone\r\nSUMMARY:Apple Timezone\r\n" +
			"DTSTART;TZID=America/Chicago:20260414T191500\r\nDTEND;TZID=America/Chicago:20260414T195500\r\nEND:VEVENT\r\nEND:VCALENDAR\r\n",
		DTStart: &storedStart,
		DTEnd:   &storedEnd,
	})

	if got := payload["dtstart"]; got != "2026-04-15T00:15:00Z" {
		t.Fatalf("dtstart = %v, want raw timezone-adjusted time", got)
	}
	if got := payload["dtend"]; got != "2026-04-15T00:55:00Z" {
		t.Fatalf("dtend = %v, want raw timezone-adjusted time", got)
	}
}

func TestCalendarEventJSONIncludesCrossMonthEventForEndMonth(t *testing.T) {
	start := time.Date(2026, 3, 31, 23, 0, 0, 0, time.UTC)
	end := time.Date(2026, 4, 1, 1, 0, 0, 0, time.UTC)

	handler := NewHandler(&config.Config{}, &store.Store{
		Calendars: &fakeCalendarRepo{
			accessible: map[string]*store.CalendarAccess{
				"1:100": {Calendar: store.Calendar{ID: 1, UserID: 100, Name: "Test"}, Shared: false, Editor: true},
			},
		},
		Events: &fakeEventRepo{events: map[string]*store.Event{
			"1:cross-month": {
				CalendarID:   1,
				UID:          "cross-month",
				ResourceName: "cross-month",
				RawICAL: "BEGIN:VCALENDAR\r\nBEGIN:VEVENT\r\nUID:cross-month\r\nSUMMARY:Cross Month\r\n" +
					"DTSTART:20260331T230000Z\r\nDTEND:20260401T010000Z\r\nEND:VEVENT\r\nEND:VCALENDAR\r\n",
				DTStart: &start,
				DTEnd:   &end,
			},
		}},
	}, nil)

	req := httptest.NewRequest(http.MethodGet, "/calendars/1/events.json?year=2026&month=4", nil)
	req = withRouteID(req, "1")
	req = req.WithContext(auth.WithUser(req.Context(), &store.User{ID: 100, PrimaryEmail: "owner@example.com"}))
	w := httptest.NewRecorder()

	handler.GetCalendarEventsJSON(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("GetCalendarEventsJSON() status = %d, want %d", w.Code, http.StatusOK)
	}
	if body := w.Body.String(); !strings.Contains(body, `"uid":"cross-month"`) {
		t.Fatalf("expected cross-month event in April JSON response, got %s", body)
	}
}

func TestCalendarEventJSONIncludesServerDateFields(t *testing.T) {
	start := time.Date(2026, 3, 28, 17, 0, 0, 0, time.UTC)
	end := time.Date(2026, 3, 28, 18, 0, 0, 0, time.UTC)
	summary := "Imported Event"

	handler := NewHandler(&config.Config{}, &store.Store{
		Calendars: &fakeCalendarRepo{
			accessible: map[string]*store.CalendarAccess{
				"1:100": {Calendar: store.Calendar{ID: 1, UserID: 100, Name: "Test"}, Shared: false, Editor: true},
			},
		},
		Events: &fakeEventRepo{events: map[string]*store.Event{
			"1:imported": {
				CalendarID:   1,
				UID:          "imported",
				ResourceName: "imported",
				RawICAL:      "BEGIN:VCALENDAR\r\nBEGIN:VEVENT\r\nUID:imported\r\nEND:VEVENT\r\nEND:VCALENDAR\r\n",
				Summary:      &summary,
				DTStart:      &start,
				DTEnd:        &end,
				AllDay:       false,
			},
		}},
	}, nil)

	req := httptest.NewRequest(http.MethodGet, "/calendars/1/events.json?year=2026&month=3", nil)
	req = withRouteID(req, "1")
	req = req.WithContext(auth.WithUser(req.Context(), &store.User{ID: 100, PrimaryEmail: "owner@example.com"}))
	w := httptest.NewRecorder()

	handler.GetCalendarEventsJSON(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("GetCalendarEventsJSON() status = %d, want %d", w.Code, http.StatusOK)
	}
	body := w.Body.String()
	if !strings.Contains(body, `"dtstart":"2026-03-28T17:00:00Z"`) {
		t.Fatalf("expected dtstart in JSON response, got %s", body)
	}
	if !strings.Contains(body, `"dtend":"2026-03-28T18:00:00Z"`) {
		t.Fatalf("expected dtend in JSON response, got %s", body)
	}
	if !strings.Contains(body, `"summary":"Imported Event"`) {
		t.Fatalf("expected summary in JSON response, got %s", body)
	}
}

func TestCalendarEventJSONIncludesParsedICalMetadata(t *testing.T) {
	start := time.Date(2026, 3, 28, 17, 0, 0, 0, time.UTC)
	end := time.Date(2026, 3, 28, 18, 0, 0, 0, time.UTC)

	handler := NewHandler(&config.Config{}, &store.Store{
		Calendars: &fakeCalendarRepo{
			accessible: map[string]*store.CalendarAccess{
				"1:100": {Calendar: store.Calendar{ID: 1, UserID: 100, Name: "Test"}, Shared: false, Editor: true},
			},
		},
		Events: &fakeEventRepo{events: map[string]*store.Event{
			"1:imported": {
				CalendarID:   1,
				UID:          "imported",
				ResourceName: "imported",
				RawICAL: "BEGIN:VCALENDAR\r\nBEGIN:VEVENT\r\nUID:imported\r\nSUMMARY:Imported Metadata\r\n" +
					"DESCRIPTION:Line\\nTwo\r\nLOCATION:Room 1\r\nDTSTART;TZID=US/Central:20260328T120000\r\nDTEND;TZID=US/Central:20260328T130000\r\n" +
					"RRULE:FREQ=WEEKLY;COUNT=2\r\nEXDATE;TZID=US/Central:20260404T120000\r\nSTATUS:CONFIRMED\r\nCLASS:PRIVATE\r\nTRANSP:OPAQUE\r\n" +
					"CATEGORIES:Team,Planning\r\nORGANIZER;CN=Alice Example:mailto:alice@example.com\r\nATTENDEE;CN=Bob Example:mailto:bob@example.com\r\n" +
					"ATTACH:https://example.com/agenda.pdf\r\nBEGIN:VALARM\r\nTRIGGER:-PT15M\r\nEND:VALARM\r\nEND:VEVENT\r\n" +
					"BEGIN:VEVENT\r\nUID:imported\r\nRECURRENCE-ID;TZID=US/Central:20260411T120000\r\nSUMMARY:Override\r\nDTSTART;TZID=US/Central:20260411T123000\r\nEND:VEVENT\r\nEND:VCALENDAR\r\n",
				DTStart: &start,
				DTEnd:   &end,
			},
		}},
	}, nil)

	req := httptest.NewRequest(http.MethodGet, "/calendars/1/events.json?year=2026&month=3", nil)
	req = withRouteID(req, "1")
	req = req.WithContext(auth.WithUser(req.Context(), &store.User{ID: 100, PrimaryEmail: "owner@example.com"}))
	w := httptest.NewRecorder()

	handler.GetCalendarEventsJSON(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("GetCalendarEventsJSON() status = %d, want %d", w.Code, http.StatusOK)
	}
	body := w.Body.String()
	for _, want := range []string{
		`"summary":"Imported Metadata"`,
		`"description":"Line\nTwo"`,
		`"location":"Room 1"`,
		`"timezone":"US/Central"`,
		`"rrule":"FREQ=WEEKLY;COUNT=2"`,
		`"COUNT":"2"`,
		`"FREQ":"WEEKLY"`,
		`"exdates":["2026-04-04T17:00:00Z"]`,
		`"status":"CONFIRMED"`,
		`"class":"PRIVATE"`,
		`"transp":"OPAQUE"`,
		`"categories":["Team","Planning"]`,
		`"organizer":"Alice Example \u003calice@example.com\u003e"`,
		`"attendees":["Bob Example \u003cbob@example.com\u003e"]`,
		`"attachments":["https://example.com/agenda.pdf"]`,
		`"reminders":[15]`,
		`"overrides":[`,
		`"recurrenceId":"2026-04-11T17:00:00Z"`,
	} {
		if !strings.Contains(body, want) {
			t.Fatalf("expected JSON to contain %s, got %s", want, body)
		}
	}
}

func TestAllCalendarEventsJSONHidesDeniedEvents(t *testing.T) {
	start := time.Date(2026, 3, 20, 10, 0, 0, 0, time.UTC)

	handler := NewHandler(&config.Config{}, &store.Store{
		Calendars: &fakeCalendarRepo{listAccessible: []store.CalendarAccess{
			{Calendar: store.Calendar{ID: 1, UserID: 200, Name: "Shared"}, Shared: true, Editor: false},
		}},
		Events: &fakeEventRepo{events: map[string]*store.Event{
			"1:visible": {CalendarID: 1, UID: "visible", ResourceName: "visible", RawICAL: "BEGIN:VCALENDAR\r\nBEGIN:VEVENT\r\nUID:visible\r\nEND:VEVENT\r\nEND:VCALENDAR\r\n", DTStart: &start},
			"1:hidden":  {CalendarID: 1, UID: "hidden", ResourceName: "hidden", RawICAL: "BEGIN:VCALENDAR\r\nBEGIN:VEVENT\r\nUID:hidden\r\nEND:VEVENT\r\nEND:VCALENDAR\r\n", DTStart: &start},
		}},
		ACLEntries: &fakeACLRepo{entries: []store.ACLEntry{
			{ResourcePath: "/dav/calendars/1", PrincipalHref: "/dav/principals/100/", IsGrant: true, Privilege: "read"},
			{ResourcePath: "/dav/calendars/1/hidden", PrincipalHref: "/dav/principals/100/", IsGrant: false, Privilege: "read"},
		}},
	}, nil)

	req := httptest.NewRequest(http.MethodGet, "/calendars/all/events.json?year=2026&month=3", nil)
	req = req.WithContext(auth.WithUser(req.Context(), &store.User{ID: 100, PrimaryEmail: "delegate@example.com"}))
	w := httptest.NewRecorder()

	handler.GetAllCalendarEventsJSON(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("GetAllCalendarEventsJSON() status = %d, want %d", w.Code, http.StatusOK)
	}
	body := w.Body.String()
	if !strings.Contains(body, `"uid":"visible"`) {
		t.Fatalf("expected visible event in all-calendars JSON, got %s", body)
	}
	if strings.Contains(body, `"uid":"hidden"`) {
		t.Fatalf("expected denied event to be omitted from all-calendars JSON, got %s", body)
	}
}

func TestAllCalendarEventsJSONReturnsEmptyArrayWhenNoEventsMatch(t *testing.T) {
	handler := NewHandler(&config.Config{}, &store.Store{
		Calendars: &fakeCalendarRepo{listAccessible: []store.CalendarAccess{
			{Calendar: store.Calendar{ID: 1, UserID: 100, Name: "Empty"}, Shared: false, Editor: true},
		}},
		Events: &fakeEventRepo{events: map[string]*store.Event{}},
	}, nil)

	req := httptest.NewRequest(http.MethodGet, "/calendars/all/events.json?year=2026&month=4", nil)
	req = req.WithContext(auth.WithUser(req.Context(), &store.User{ID: 100, PrimaryEmail: "owner@example.com"}))
	w := httptest.NewRecorder()

	handler.GetAllCalendarEventsJSON(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("GetAllCalendarEventsJSON() status = %d, want %d", w.Code, http.StatusOK)
	}
	if body := strings.TrimSpace(w.Body.String()); body != "[]" {
		t.Fatalf("expected empty JSON array, got %s", body)
	}
}

func TestAllCalendarEventsJSONSupportsDateRangeAndCapabilities(t *testing.T) {
	inRange := time.Date(2026, 8, 1, 9, 0, 0, 0, time.UTC)
	outOfRange := time.Date(2026, 8, 3, 9, 0, 0, 0, time.UTC)
	handler := NewHandler(&config.Config{}, &store.Store{
		Calendars: &fakeCalendarRepo{listAccessible: []store.CalendarAccess{
			{Calendar: store.Calendar{ID: 1, UserID: 100, Name: "Work"}},
		}},
		Events: &fakeEventRepo{events: map[string]*store.Event{
			"1:included": {
				CalendarID:   1,
				UID:          "included",
				ResourceName: "included",
				RawICAL:      "BEGIN:VCALENDAR\r\nBEGIN:VEVENT\r\nUID:included\r\nSUMMARY:Included\r\nDTSTART:20260801T090000Z\r\nDTEND:20260801T100000Z\r\nEND:VEVENT\r\nEND:VCALENDAR\r\n",
				DTStart:      &inRange,
			},
			"1:excluded": {
				CalendarID:   1,
				UID:          "excluded",
				ResourceName: "excluded",
				RawICAL:      "BEGIN:VCALENDAR\r\nBEGIN:VEVENT\r\nUID:excluded\r\nSUMMARY:Excluded\r\nDTSTART:20260803T090000Z\r\nDTEND:20260803T100000Z\r\nEND:VEVENT\r\nEND:VCALENDAR\r\n",
				DTStart:      &outOfRange,
			},
		}},
	}, nil)

	req := httptest.NewRequest(http.MethodGet, "/calendars/all/events.json?start=2026-07-27&end=2026-08-03", nil)
	req = req.WithContext(auth.WithUser(req.Context(), &store.User{ID: 100, PrimaryEmail: "owner@example.com"}))
	w := httptest.NewRecorder()

	handler.GetAllCalendarEventsJSON(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("GetAllCalendarEventsJSON() status = %d, want %d: %s", w.Code, http.StatusOK, w.Body.String())
	}
	var payload []map[string]any
	if err := json.Unmarshal(w.Body.Bytes(), &payload); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if len(payload) != 1 || payload[0]["uid"] != "included" {
		t.Fatalf("GetAllCalendarEventsJSON() payload = %#v, want only included event", payload)
	}
	for _, key := range []string{"canEdit", "canDelete", "canMove"} {
		if value, ok := payload[0][key].(bool); !ok || !value {
			t.Fatalf("event capability %q = %#v, want true", key, payload[0][key])
		}
	}
}

func TestAllCalendarEventsJSONRejectsInvalidDateRange(t *testing.T) {
	handler := NewHandler(&config.Config{}, &store.Store{
		Calendars: &fakeCalendarRepo{},
		Events:    &fakeEventRepo{events: map[string]*store.Event{}},
	}, nil)
	req := httptest.NewRequest(http.MethodGet, "/calendars/all/events.json?start=2026-08-01&end=2026-07-31", nil)
	req = req.WithContext(auth.WithUser(req.Context(), &store.User{ID: 100, PrimaryEmail: "owner@example.com"}))
	w := httptest.NewRecorder()

	handler.GetAllCalendarEventsJSON(w, req)

	if w.Code != http.StatusBadRequest {
		t.Fatalf("GetAllCalendarEventsJSON() status = %d, want %d", w.Code, http.StatusBadRequest)
	}
}

// runAllCalendarEventsForCount drives GetAllCalendarEventsJSON against a single
// shared calendar populated with eventCount in-window events and returns the fake
// ACL repository (for call-count inspection) and the number of events in the
// response body.
func runAllCalendarEventsForCount(t *testing.T, eventCount int) (*fakeACLRepo, int) {
	t.Helper()
	start := time.Date(2026, 3, 15, 10, 0, 0, 0, time.UTC)
	events := make(map[string]*store.Event, eventCount)
	for i := 0; i < eventCount; i++ {
		name := fmt.Sprintf("ev-%d", i)
		events[fmt.Sprintf("1:%s", name)] = &store.Event{
			CalendarID:   1,
			UID:          name,
			ResourceName: name,
			RawICAL:      "BEGIN:VCALENDAR\r\nBEGIN:VEVENT\r\nUID:" + name + "\r\nEND:VEVENT\r\nEND:VCALENDAR\r\n",
			DTStart:      &start,
		}
	}
	aclRepo := &fakeACLRepo{entries: []store.ACLEntry{
		{ResourcePath: "/dav/calendars/1", PrincipalHref: "/dav/principals/100/", IsGrant: true, Privilege: "read"},
		{ResourcePath: "/dav/calendars/1", PrincipalHref: "/dav/principals/100/", IsGrant: true, Privilege: "write"},
	}}
	handler := NewHandler(&config.Config{}, &store.Store{
		Calendars: &fakeCalendarRepo{listAccessible: []store.CalendarAccess{
			{Calendar: store.Calendar{ID: 1, UserID: 200, Name: "Shared"}, Shared: true},
		}},
		Events:     &fakeEventRepo{events: events},
		ACLEntries: aclRepo,
	}, nil)

	req := httptest.NewRequest(http.MethodGet, "/calendars/all/events.json?year=2026&month=3", nil)
	req = req.WithContext(auth.WithUser(req.Context(), &store.User{ID: 100, PrimaryEmail: "delegate@example.com"}))
	w := httptest.NewRecorder()
	handler.GetAllCalendarEventsJSON(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("GetAllCalendarEventsJSON() status = %d, want %d: %s", w.Code, http.StatusOK, w.Body.String())
	}
	var payload []map[string]any
	if err := json.Unmarshal(w.Body.Bytes(), &payload); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	return aclRepo, len(payload)
}

func TestAllCalendarEventsJSONBatchesSharedCalendarACLLookups(t *testing.T) {
	small, retSmall := runAllCalendarEventsForCount(t, 10)
	large, retLarge := runAllCalendarEventsForCount(t, 100)

	for _, tc := range []struct {
		name string
		repo *fakeACLRepo
	}{{"small", small}, {"large", large}} {
		if tc.repo.listByResourceCalls != 0 {
			t.Errorf("%s: expected no per-candidate ListByResource calls, got %d", tc.name, tc.repo.listByResourceCalls)
		}
		if tc.repo.listByPrincipalCalls != 0 {
			t.Errorf("%s: expected no whole-principal ListByPrincipal sweeps, got %d", tc.name, tc.repo.listByPrincipalCalls)
		}
	}
	if small.listByResourcesAndPrincipalsCalls != large.listByResourcesAndPrincipalsCalls {
		t.Fatalf("scoped ACL batch calls scaled with event count: small=%d large=%d",
			small.listByResourcesAndPrincipalsCalls, large.listByResourcesAndPrincipalsCalls)
	}
	if small.listByResourcesAndPrincipalsCalls != 1 {
		t.Fatalf("expected exactly one scoped ACL batch per shared calendar, got %d", small.listByResourcesAndPrincipalsCalls)
	}
	if retSmall != 10 || retLarge != 100 {
		t.Fatalf("returned events small=%d large=%d, want 10 and 100", retSmall, retLarge)
	}
}

func TestAllCalendarEventsJSONOwnedCalendarSkipsACLLookups(t *testing.T) {
	start := time.Date(2026, 3, 15, 10, 0, 0, 0, time.UTC)
	events := make(map[string]*store.Event, 20)
	for i := 0; i < 20; i++ {
		name := fmt.Sprintf("ev-%d", i)
		events[fmt.Sprintf("1:%s", name)] = &store.Event{
			CalendarID:   1,
			UID:          name,
			ResourceName: name,
			RawICAL:      "BEGIN:VCALENDAR\r\nBEGIN:VEVENT\r\nUID:" + name + "\r\nEND:VEVENT\r\nEND:VCALENDAR\r\n",
			DTStart:      &start,
		}
	}
	aclRepo := &fakeACLRepo{}
	handler := NewHandler(&config.Config{}, &store.Store{
		Calendars: &fakeCalendarRepo{listAccessible: []store.CalendarAccess{
			{Calendar: store.Calendar{ID: 1, UserID: 100, Name: "Owned"}},
		}},
		Events:     &fakeEventRepo{events: events},
		ACLEntries: aclRepo,
	}, nil)

	req := httptest.NewRequest(http.MethodGet, "/calendars/all/events.json?year=2026&month=3", nil)
	req = req.WithContext(auth.WithUser(req.Context(), &store.User{ID: 100, PrimaryEmail: "owner@example.com"}))
	w := httptest.NewRecorder()
	handler.GetAllCalendarEventsJSON(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("GetAllCalendarEventsJSON() status = %d, want %d: %s", w.Code, http.StatusOK, w.Body.String())
	}
	if aclRepo.listByResourceCalls != 0 || aclRepo.listByPrincipalCalls != 0 || aclRepo.listByResourcesAndPrincipalsCalls != 0 {
		t.Fatalf("owned calendar made ACL repository calls: resource=%d principal=%d batched=%d",
			aclRepo.listByResourceCalls, aclRepo.listByPrincipalCalls, aclRepo.listByResourcesAndPrincipalsCalls)
	}
}

func TestAllCalendarEventsJSONWindowFilteringKeepsACLCallsConstant(t *testing.T) {
	inRange := time.Date(2026, 3, 15, 10, 0, 0, 0, time.UTC)
	outOfRange := time.Date(2026, 5, 15, 10, 0, 0, 0, time.UTC)
	events := map[string]*store.Event{}
	for i := 0; i < 30; i++ {
		when := inRange
		if i%2 == 0 {
			when = outOfRange
		}
		name := fmt.Sprintf("ev-%d", i)
		events[fmt.Sprintf("1:%s", name)] = &store.Event{
			CalendarID:   1,
			UID:          name,
			ResourceName: name,
			RawICAL:      "BEGIN:VCALENDAR\r\nBEGIN:VEVENT\r\nUID:" + name + "\r\nEND:VEVENT\r\nEND:VCALENDAR\r\n",
			DTStart:      &when,
		}
	}
	aclRepo := &fakeACLRepo{entries: []store.ACLEntry{
		{ResourcePath: "/dav/calendars/1", PrincipalHref: "/dav/principals/100/", IsGrant: true, Privilege: "read"},
	}}
	handler := NewHandler(&config.Config{}, &store.Store{
		Calendars: &fakeCalendarRepo{listAccessible: []store.CalendarAccess{
			{Calendar: store.Calendar{ID: 1, UserID: 200, Name: "Shared"}, Shared: true},
		}},
		Events:     &fakeEventRepo{events: events},
		ACLEntries: aclRepo,
	}, nil)

	req := httptest.NewRequest(http.MethodGet, "/calendars/all/events.json?year=2026&month=3", nil)
	req = req.WithContext(auth.WithUser(req.Context(), &store.User{ID: 100, PrimaryEmail: "delegate@example.com"}))
	w := httptest.NewRecorder()
	handler.GetAllCalendarEventsJSON(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("GetAllCalendarEventsJSON() status = %d, want %d: %s", w.Code, http.StatusOK, w.Body.String())
	}
	var payload []map[string]any
	if err := json.Unmarshal(w.Body.Bytes(), &payload); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if len(payload) != 15 {
		t.Fatalf("returned %d events, want 15 in-window events", len(payload))
	}
	if aclRepo.listByResourceCalls != 0 || aclRepo.listByPrincipalCalls != 0 {
		t.Fatalf("expected no unscoped ACL reads, got resource=%d principal=%d", aclRepo.listByResourceCalls, aclRepo.listByPrincipalCalls)
	}
	if aclRepo.listByResourcesAndPrincipalsCalls != 1 {
		t.Fatalf("expected exactly one scoped ACL batch, got %d", aclRepo.listByResourcesAndPrincipalsCalls)
	}

	// The ACL prefetch must be scoped to the requested window: out-of-window
	// events must not contribute resource paths to the batch query.
	prefetched := make(map[string]bool, len(aclRepo.batchedResourcePaths))
	for _, path := range aclRepo.batchedResourcePaths {
		prefetched[path] = true
	}
	for i := 0; i < 30; i++ {
		name := fmt.Sprintf("ev-%d", i)
		path := "/dav/calendars/1/" + name
		if i%2 == 0 { // out-of-window events
			if prefetched[path] {
				t.Errorf("out-of-window event %q was included in the ACL prefetch", name)
			}
		} else if !prefetched[path] {
			t.Errorf("in-window event %q was missing from the ACL prefetch", name)
		}
	}
}

func TestAllCalendarEventsJSONPerResourceCapabilities(t *testing.T) {
	start := time.Date(2026, 3, 15, 10, 0, 0, 0, time.UTC)
	mkEvent := func(uid, resourceName string) *store.Event {
		return &store.Event{
			CalendarID:   1,
			UID:          uid,
			ResourceName: resourceName,
			RawICAL:      "BEGIN:VCALENDAR\r\nBEGIN:VEVENT\r\nUID:" + uid + "\r\nEND:VEVENT\r\nEND:VCALENDAR\r\n",
			DTStart:      &start,
		}
	}
	events := map[string]*store.Event{
		"1:full":        mkEvent("full", "full"),
		"1:read-denied": mkEvent("read-denied", "read-denied"),
		"1:edit-denied": mkEvent("edit-denied", "edit-denied"),
		"1:del-denied":  mkEvent("del-denied", "del-denied"),
		"1:both-denied": mkEvent("both-denied", "both-denied"),
		"1:meeting":     mkEvent("meeting", "meeting.ics"),
	}
	const principal = "/dav/principals/100/"
	aclRepo := &fakeACLRepo{entries: []store.ACLEntry{
		// The collection grants read, write, bind, and unbind, so an event is editable and
		// deletable unless a resource-level deny overrides that grant. This is
		// what makes the deny assertions below meaningful: a regression that
		// ignored resource denies would fall back to these grants and pass a
		// missing-grant test, but not this one.
		{ResourcePath: "/dav/calendars/1", PrincipalHref: principal, IsGrant: true, Privilege: "read"},
		{ResourcePath: "/dav/calendars/1", PrincipalHref: principal, IsGrant: true, Privilege: "write"},
		{ResourcePath: "/dav/calendars/1", PrincipalHref: principal, IsGrant: true, Privilege: "bind"},
		{ResourcePath: "/dav/calendars/1", PrincipalHref: principal, IsGrant: true, Privilege: "unbind"},
		{ResourcePath: "/dav/calendars/1/read-denied", PrincipalHref: principal, IsGrant: false, Privilege: "read"},
		{ResourcePath: "/dav/calendars/1/edit-denied", PrincipalHref: principal, IsGrant: false, Privilege: "write-content"},
		{ResourcePath: "/dav/calendars/1/del-denied", PrincipalHref: principal, IsGrant: false, Privilege: "unbind"},
		{ResourcePath: "/dav/calendars/1/both-denied", PrincipalHref: principal, IsGrant: false, Privilege: "write-content"},
		{ResourcePath: "/dav/calendars/1/both-denied", PrincipalHref: principal, IsGrant: false, Privilege: "unbind"},
		// Deny addressed to the extension-alternate path of "meeting.ics".
		{ResourcePath: "/dav/calendars/1/meeting", PrincipalHref: principal, IsGrant: false, Privilege: "write-content"},
	}}
	handler := NewHandler(&config.Config{}, &store.Store{
		Calendars: &fakeCalendarRepo{listAccessible: []store.CalendarAccess{
			{Calendar: store.Calendar{ID: 1, UserID: 200, Name: "Shared"}, Shared: true},
		}},
		Events:     &fakeEventRepo{events: events},
		ACLEntries: aclRepo,
	}, nil)

	req := httptest.NewRequest(http.MethodGet, "/calendars/all/events.json?year=2026&month=3", nil)
	req = req.WithContext(auth.WithUser(req.Context(), &store.User{ID: 100, PrimaryEmail: "delegate@example.com"}))
	w := httptest.NewRecorder()
	handler.GetAllCalendarEventsJSON(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("GetAllCalendarEventsJSON() status = %d, want %d: %s", w.Code, http.StatusOK, w.Body.String())
	}
	var payload []map[string]any
	if err := json.Unmarshal(w.Body.Bytes(), &payload); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	byUID := make(map[string]map[string]any, len(payload))
	for _, item := range payload {
		uid, _ := item["uid"].(string)
		byUID[uid] = item
	}

	if _, ok := byUID["read-denied"]; ok {
		t.Fatalf("resource-level read deny did not hide event: %#v", byUID["read-denied"])
	}

	want := map[string]struct{ canEdit, canDelete, canMove bool }{
		"full":        {true, true, true},   // collection write grant
		"edit-denied": {false, true, false}, // explicit write-content deny over collection write
		"del-denied":  {true, false, false}, // explicit unbind deny over collection write
		"both-denied": {false, false, false},
		"meeting":     {false, true, false}, // write-content deny on the .ics alternate path
	}
	for uid, expected := range want {
		item, ok := byUID[uid]
		if !ok {
			t.Fatalf("event %q missing from response", uid)
		}
		if got := item["canEdit"].(bool); got != expected.canEdit {
			t.Errorf("event %q canEdit = %v, want %v", uid, got, expected.canEdit)
		}
		if got := item["canDelete"].(bool); got != expected.canDelete {
			t.Errorf("event %q canDelete = %v, want %v", uid, got, expected.canDelete)
		}
		if got := item["canMove"].(bool); got != expected.canMove {
			t.Errorf("event %q canMove = %v, want %v", uid, got, expected.canMove)
		}
	}

	if aclRepo.listByResourceCalls != 0 || aclRepo.listByPrincipalCalls != 0 {
		t.Fatalf("expected no unscoped ACL reads, got resource=%d principal=%d", aclRepo.listByResourceCalls, aclRepo.listByPrincipalCalls)
	}
	if aclRepo.listByResourcesAndPrincipalsCalls != 1 {
		t.Fatalf("expected exactly one scoped ACL batch, got %d", aclRepo.listByResourcesAndPrincipalsCalls)
	}
}

func TestViewAllCalendarsIncludesWritableCalendarMetadata(t *testing.T) {
	handler := NewHandler(&config.Config{}, &store.Store{
		Calendars: &fakeCalendarRepo{listAccessible: []store.CalendarAccess{
			{
				Calendar:           store.Calendar{ID: 1, UserID: 100, Name: "Owned"},
				Privileges:         store.FullCalendarPrivileges(),
				PrivilegesResolved: true,
			},
			{
				Calendar:           store.Calendar{ID: 2, UserID: 200, Name: "Read only"},
				Shared:             true,
				Privileges:         store.CalendarPrivileges{Read: true},
				PrivilegesResolved: true,
			},
		}},
	}, nil)
	req := httptest.NewRequest(http.MethodGet, "/calendars/all", nil)
	req = req.WithContext(auth.WithUser(req.Context(), &store.User{ID: 100, PrimaryEmail: "owner@example.com"}))
	w := httptest.NewRecorder()

	handler.ViewAllCalendars(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("ViewAllCalendars() status = %d, want %d", w.Code, http.StatusOK)
	}
	body := w.Body.String()
	if !strings.Contains(body, `"canCreate":true`) || !strings.Contains(body, `"canCreate":false`) {
		t.Fatalf("ViewAllCalendars() missing writable metadata: %s", body)
	}
	for _, marker := range []string{
		`data-view="day"`,
		`data-view="week"`,
		`id="time-grid-view"`,
		`id="event-editor-modal"`,
		`name="destination_calendar_id"`,
	} {
		if !strings.Contains(body, marker) {
			t.Fatalf("ViewAllCalendars() missing interactive UI marker %q", marker)
		}
	}
}

func TestShareCalendarStoresACLEntries(t *testing.T) {
	calRepo := &fakeCalendarRepo{
		calendars: map[int64]*store.Calendar{
			1: {ID: 1, UserID: 100, Name: "Work"},
		},
	}
	userRepo := &fakeUserRepo{
		users: map[int64]*store.User{
			200: {ID: 200, PrimaryEmail: "viewer@example.com"},
		},
	}
	aclRepo := &fakeACLRepo{}
	s := &store.Store{
		Calendars:  calRepo,
		Users:      userRepo,
		ACLEntries: aclRepo,
	}
	handler := NewHandler(&config.Config{}, s, nil)

	form := url.Values{"user_id": {"200"}}
	req := httptest.NewRequest(http.MethodPost, "/calendars/1/shares", strings.NewReader(form.Encode()))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	req = withRouteID(req, "1")
	req = req.WithContext(auth.WithUser(req.Context(), &store.User{ID: 100, PrimaryEmail: "owner@example.com"}))

	w := httptest.NewRecorder()
	handler.ShareCalendar(w, req)

	if w.Code != http.StatusFound {
		t.Fatalf("ShareCalendar() status = %d, want %d", w.Code, http.StatusFound)
	}

	entries, err := aclRepo.ListByResource(context.Background(), "/dav/calendars/1")
	if err != nil {
		t.Fatalf("ListByResource() error = %v", err)
	}
	if len(entries) != 3 {
		t.Fatalf("expected 3 ACL entries, got %#v", entries)
	}

	got := map[string]bool{}
	for _, entry := range entries {
		if entry.PrincipalHref != "/dav/principals/200/" || !entry.IsGrant {
			t.Fatalf("unexpected ACL entry %#v", entry)
		}
		got[entry.Privilege] = true
	}
	for _, privilege := range []string{"read", "read-free-busy", "write"} {
		if !got[privilege] {
			t.Fatalf("expected %q ACL grant, got %#v", privilege, entries)
		}
	}
}

func TestUnshareCalendarRemovesACLEntries(t *testing.T) {
	calRepo := &fakeCalendarRepo{
		calendars: map[int64]*store.Calendar{
			1: {ID: 1, UserID: 100, Name: "Work"},
		},
	}
	aclRepo := &fakeACLRepo{
		entries: []store.ACLEntry{
			{ResourcePath: "/dav/calendars/1", PrincipalHref: "/dav/principals/200/", IsGrant: true, Privilege: "read"},
			{ResourcePath: "/dav/calendars/1", PrincipalHref: "/dav/principals/200/", IsGrant: true, Privilege: "read-free-busy"},
			{ResourcePath: "/dav/calendars/1", PrincipalHref: "/dav/principals/200/", IsGrant: true, Privilege: "write"},
			{ResourcePath: "/dav/calendars/1", PrincipalHref: "/dav/principals/300/", IsGrant: true, Privilege: "read"},
		},
	}
	s := &store.Store{
		Calendars:  calRepo,
		ACLEntries: aclRepo,
	}
	handler := NewHandler(&config.Config{}, s, nil)

	req := httptest.NewRequest(http.MethodPost, "/calendars/1/shares/200/delete", nil)
	rctx := chi.NewRouteContext()
	rctx.URLParams.Add("id", "1")
	rctx.URLParams.Add("userId", "200")
	req = req.WithContext(context.WithValue(req.Context(), chi.RouteCtxKey, rctx))
	req = req.WithContext(auth.WithUser(req.Context(), &store.User{ID: 100, PrimaryEmail: "owner@example.com"}))

	w := httptest.NewRecorder()
	handler.UnshareCalendar(w, req)

	if w.Code != http.StatusFound {
		t.Fatalf("UnshareCalendar() status = %d, want %d", w.Code, http.StatusFound)
	}

	entries, err := aclRepo.ListByResource(context.Background(), "/dav/calendars/1")
	if err != nil {
		t.Fatalf("ListByResource() error = %v", err)
	}
	if len(entries) != 1 {
		t.Fatalf("expected only unrelated ACL entry to remain, got %#v", entries)
	}
	if entries[0].PrincipalHref != "/dav/principals/300/" {
		t.Fatalf("unexpected ACL entries after unshare: %#v", entries)
	}
}

func TestUnshareCalendarAllowsSharedUserToLeaveWithoutRemovingCustomACLs(t *testing.T) {
	calRepo := &fakeCalendarRepo{
		accessible: map[string]*store.CalendarAccess{
			"1:200": {Calendar: store.Calendar{ID: 1, UserID: 100, Name: "Shared"}, Shared: true, Editor: true},
		},
	}
	aclRepo := &fakeACLRepo{
		entries: []store.ACLEntry{
			{ResourcePath: "/dav/calendars/1", PrincipalHref: "/dav/principals/200/", IsGrant: true, Privilege: "read"},
			{ResourcePath: "/dav/calendars/1", PrincipalHref: "/dav/principals/200/", IsGrant: true, Privilege: "write"},
			{ResourcePath: "/dav/calendars/1/private-event", PrincipalHref: "/dav/principals/200/", IsGrant: true, Privilege: "read"},
			{ResourcePath: "/dav/calendars/1/private-event", PrincipalHref: "/dav/principals/200/", IsGrant: false, Privilege: "write-content"},
			{ResourcePath: "/dav/calendars/1", PrincipalHref: "/dav/principals/300/", IsGrant: true, Privilege: "read"},
			{ResourcePath: "/dav/calendars/1/private-event", PrincipalHref: "/dav/principals/300/", IsGrant: true, Privilege: "read"},
		},
	}
	s := &store.Store{
		Calendars:  calRepo,
		ACLEntries: aclRepo,
	}
	handler := NewHandler(&config.Config{}, s, nil)

	req := httptest.NewRequest(http.MethodPost, "/calendars/1/shares/200/delete", nil)
	rctx := chi.NewRouteContext()
	rctx.URLParams.Add("id", "1")
	rctx.URLParams.Add("userId", "200")
	req = req.WithContext(context.WithValue(req.Context(), chi.RouteCtxKey, rctx))
	req = req.WithContext(auth.WithUser(req.Context(), &store.User{ID: 200, PrimaryEmail: "viewer@example.com"}))

	w := httptest.NewRecorder()
	handler.UnshareCalendar(w, req)

	if w.Code != http.StatusFound {
		t.Fatalf("UnshareCalendar() status = %d, want %d", w.Code, http.StatusFound)
	}

	entries, err := aclRepo.ListByResource(context.Background(), "/dav/calendars/1")
	if err != nil {
		t.Fatalf("ListByResource() error = %v", err)
	}
	if len(entries) != 1 || entries[0].PrincipalHref != "/dav/principals/300/" {
		t.Fatalf("unexpected ACL entries after leave: %#v", entries)
	}

	descendantEntries, err := aclRepo.ListByResource(context.Background(), "/dav/calendars/1/private-event")
	if err != nil {
		t.Fatalf("ListByResource() error = %v", err)
	}
	if len(descendantEntries) != 3 {
		t.Fatalf("expected descendant custom ACLs to remain unchanged, got %#v", descendantEntries)
	}
}

func TestShareCalendarPreservesCustomPrincipalACLs(t *testing.T) {
	calRepo := &fakeCalendarRepo{
		calendars: map[int64]*store.Calendar{
			1: {ID: 1, UserID: 100, Name: "Work"},
		},
	}
	userRepo := &fakeUserRepo{
		users: map[int64]*store.User{
			200: {ID: 200, PrimaryEmail: "viewer@example.com"},
		},
	}
	aclRepo := &fakeACLRepo{
		entries: []store.ACLEntry{
			{ResourcePath: "/dav/calendars/1", PrincipalHref: "/dav/principals/200/", IsGrant: true, Privilege: "read-acl"},
			{ResourcePath: "/dav/calendars/1", PrincipalHref: "/dav/principals/200/", IsGrant: false, Privilege: "write-content"},
			{ResourcePath: "/dav/calendars/1", PrincipalHref: "/dav/principals/300/", IsGrant: true, Privilege: "read"},
		},
	}
	handler := NewHandler(&config.Config{}, &store.Store{
		Calendars:  calRepo,
		Users:      userRepo,
		ACLEntries: aclRepo,
	}, nil)

	form := url.Values{"user_id": {"200"}}
	req := httptest.NewRequest(http.MethodPost, "/calendars/1/shares", strings.NewReader(form.Encode()))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	req = withRouteID(req, "1")
	req = req.WithContext(auth.WithUser(req.Context(), &store.User{ID: 100, PrimaryEmail: "owner@example.com"}))

	w := httptest.NewRecorder()
	handler.ShareCalendar(w, req)

	if w.Code != http.StatusFound {
		t.Fatalf("ShareCalendar() status = %d, want %d", w.Code, http.StatusFound)
	}

	entries, err := aclRepo.ListByResource(context.Background(), "/dav/calendars/1")
	if err != nil {
		t.Fatalf("ListByResource() error = %v", err)
	}

	got := map[string]struct{}{}
	for _, entry := range entries {
		if entry.PrincipalHref == "/dav/principals/200/" {
			got[fmt.Sprintf("%t:%s", entry.IsGrant, entry.Privilege)] = struct{}{}
		}
	}
	for _, want := range []string{
		"true:read",
		"true:read-free-busy",
		"true:write",
		"true:read-acl",
		"false:write-content",
	} {
		if _, ok := got[want]; !ok {
			t.Fatalf("expected preserved/shared ACL %q, got %#v", want, entries)
		}
	}
}

func TestUnshareCalendarRemovesOnlyManagedCollectionGrants(t *testing.T) {
	calRepo := &fakeCalendarRepo{
		calendars: map[int64]*store.Calendar{
			1: {ID: 1, UserID: 100, Name: "Work"},
		},
	}
	aclRepo := &fakeACLRepo{
		entries: []store.ACLEntry{
			{ResourcePath: "/dav/calendars/1", PrincipalHref: "/dav/principals/200/", IsGrant: true, Privilege: "read"},
			{ResourcePath: "/dav/calendars/1", PrincipalHref: "/dav/principals/200/", IsGrant: true, Privilege: "read-free-busy"},
			{ResourcePath: "/dav/calendars/1", PrincipalHref: "/dav/principals/200/", IsGrant: true, Privilege: "write"},
			{ResourcePath: "/dav/calendars/1", PrincipalHref: "/dav/principals/200/", IsGrant: true, Privilege: "read-acl"},
			{ResourcePath: "/dav/calendars/1", PrincipalHref: "/dav/principals/200/", IsGrant: false, Privilege: "write-content"},
			{ResourcePath: "/dav/calendars/1/private-event", PrincipalHref: "/dav/principals/200/", IsGrant: true, Privilege: "read"},
			{ResourcePath: "/dav/calendars/1/private-event", PrincipalHref: "/dav/principals/200/", IsGrant: false, Privilege: "write-content"},
			{ResourcePath: "/dav/calendars/1", PrincipalHref: "/dav/principals/300/", IsGrant: true, Privilege: "read"},
			{ResourcePath: "/dav/calendars/1/private-event", PrincipalHref: "/dav/principals/300/", IsGrant: true, Privilege: "read"},
		},
	}
	handler := NewHandler(&config.Config{}, &store.Store{
		Calendars:  calRepo,
		ACLEntries: aclRepo,
	}, nil)

	req := httptest.NewRequest(http.MethodPost, "/calendars/1/shares/200/delete", nil)
	rctx := chi.NewRouteContext()
	rctx.URLParams.Add("id", "1")
	rctx.URLParams.Add("userId", "200")
	req = req.WithContext(context.WithValue(req.Context(), chi.RouteCtxKey, rctx))
	req = req.WithContext(auth.WithUser(req.Context(), &store.User{ID: 100, PrimaryEmail: "owner@example.com"}))

	w := httptest.NewRecorder()
	handler.UnshareCalendar(w, req)

	if w.Code != http.StatusFound {
		t.Fatalf("UnshareCalendar() status = %d, want %d", w.Code, http.StatusFound)
	}

	entries, err := aclRepo.ListByResource(context.Background(), "/dav/calendars/1")
	if err != nil {
		t.Fatalf("ListByResource() error = %v", err)
	}

	got := map[string]struct{}{}
	for _, entry := range entries {
		if entry.PrincipalHref == "/dav/principals/200/" {
			got[fmt.Sprintf("%t:%s", entry.IsGrant, entry.Privilege)] = struct{}{}
		}
	}
	for _, want := range []string{"true:read-acl", "false:write-content"} {
		if _, ok := got[want]; !ok {
			t.Fatalf("expected custom ACL %q to remain, got %#v", want, entries)
		}
	}
	for _, removed := range []string{"true:read", "true:read-free-busy", "true:write"} {
		if _, ok := got[removed]; ok {
			t.Fatalf("expected managed ACL %q to be removed, got %#v", removed, entries)
		}
	}

	descendantEntries, err := aclRepo.ListByResource(context.Background(), "/dav/calendars/1/private-event")
	if err != nil {
		t.Fatalf("ListByResource() error = %v", err)
	}
	if len(descendantEntries) != 3 {
		t.Fatalf("expected descendant custom ACLs to remain unchanged, got %#v", descendantEntries)
	}
}

func TestUnshareCalendarPreservesACLsWhenRepositoryRemovalFails(t *testing.T) {
	calRepo := &fakeCalendarRepo{
		calendars: map[int64]*store.Calendar{
			1: {ID: 1, UserID: 100, Name: "Work"},
		},
	}
	aclRepo := &fakeACLRepo{
		entries: []store.ACLEntry{
			{ResourcePath: "/dav/calendars/1", PrincipalHref: "/dav/principals/200/", IsGrant: true, Privilege: "read"},
			{ResourcePath: "/dav/calendars/1/private-event", PrincipalHref: "/dav/principals/200/", IsGrant: false, Privilege: "write-content"},
			{ResourcePath: "/dav/calendars/1", PrincipalHref: "/dav/principals/300/", IsGrant: true, Privilege: "read"},
		},
		setACLErr: errors.New("boom"),
	}
	handler := NewHandler(&config.Config{}, &store.Store{
		Calendars:  calRepo,
		ACLEntries: aclRepo,
	}, nil)

	req := httptest.NewRequest(http.MethodPost, "/calendars/1/shares/200/delete", nil)
	rctx := chi.NewRouteContext()
	rctx.URLParams.Add("id", "1")
	rctx.URLParams.Add("userId", "200")
	req = req.WithContext(context.WithValue(req.Context(), chi.RouteCtxKey, rctx))
	req = req.WithContext(auth.WithUser(req.Context(), &store.User{ID: 100, PrimaryEmail: "owner@example.com"}))

	w := httptest.NewRecorder()
	handler.UnshareCalendar(w, req)

	if w.Code != http.StatusFound {
		t.Fatalf("UnshareCalendar() status = %d, want %d", w.Code, http.StatusFound)
	}

	entries, err := aclRepo.ListByResource(context.Background(), "/dav/calendars/1")
	if err != nil {
		t.Fatalf("ListByResource() error = %v", err)
	}
	if len(entries) != 2 {
		t.Fatalf("expected collection ACLs to remain unchanged after failed removal, got %#v", entries)
	}

	descendantEntries, err := aclRepo.ListByResource(context.Background(), "/dav/calendars/1/private-event")
	if err != nil {
		t.Fatalf("ListByResource() error = %v", err)
	}
	if len(descendantEntries) != 1 || descendantEntries[0].PrincipalHref != "/dav/principals/200/" {
		t.Fatalf("expected descendant ACLs to remain unchanged after failed removal, got %#v", descendantEntries)
	}
}

func TestCalendarShareViewsIncludePrincipalsWithCustomAccess(t *testing.T) {
	userMap := map[int64]store.User{
		200: {ID: 200, PrimaryEmail: "delegate@example.com"},
	}
	h := NewHandler(&config.Config{}, &store.Store{
		ACLEntries: &fakeACLRepo{entries: []store.ACLEntry{
			{ResourcePath: "/dav/calendars/1", PrincipalHref: "/dav/principals/200/", IsGrant: true, Privilege: "bind"},
		}},
	}, nil)

	shares, err := h.calendarShareViews(context.Background(), 1, userMap)
	if err != nil {
		t.Fatalf("calendarShareViews() error = %v", err)
	}
	if len(shares) != 1 {
		t.Fatalf("expected principal with custom access grant to appear in share list, got %#v", shares)
	}
	if !shares[0].Editor {
		t.Fatalf("expected bind grant to be treated as editable access, got %#v", shares)
	}
}

func TestCalendarShareViewsHonorsOrderedDeny(t *testing.T) {
	userMap := map[int64]store.User{
		200: {ID: 200, PrimaryEmail: "delegate@example.com"},
	}
	h := NewHandler(&config.Config{}, &store.Store{
		ACLEntries: &fakeACLRepo{entries: []store.ACLEntry{
			{ID: 2, ResourcePath: "/dav/calendars/1", PrincipalHref: "/dav/principals/200/", IsGrant: true, Privilege: "bind", Position: 2},
			{ID: 1, ResourcePath: "/dav/calendars/1", PrincipalHref: "DAV:authenticated", IsGrant: false, Privilege: "all", Position: 1},
		}},
	}, nil)

	shares, err := h.calendarShareViews(context.Background(), 1, userMap)
	if err != nil {
		t.Fatalf("calendarShareViews() error = %v", err)
	}
	if len(shares) != 0 {
		t.Fatalf("expected effective deny to hide principal from share list, got %#v", shares)
	}
}

func TestDecideCalendarPrivilegeHonorsACEOrderAcrossResourceAliases(t *testing.T) {
	user := &store.User{ID: 200}
	cal := &store.CalendarAccess{Calendar: store.Calendar{ID: 1, UserID: 100}}

	tests := []struct {
		name    string
		entries map[string][]store.ACLEntry
		want    bool
	}{
		{
			name: "earlier alias deny wins",
			entries: map[string][]store.ACLEntry{
				"/dav/calendars/1/event.ics": {{ID: 2, PrincipalHref: "DAV:authenticated", IsGrant: true, Privilege: "read", Position: 2}},
				"/dav/calendars/1/event":     {{ID: 1, PrincipalHref: "/dav/principals/200/", IsGrant: false, Privilege: "read", Position: 1}},
			},
			want: false,
		},
		{
			name: "earlier alias grant wins",
			entries: map[string][]store.ACLEntry{
				"/dav/calendars/1/event.ics": {{ID: 2, PrincipalHref: "DAV:authenticated", IsGrant: false, Privilege: "read", Position: 2}},
				"/dav/calendars/1/event":     {{ID: 1, PrincipalHref: "/dav/principals/200/", IsGrant: true, Privilege: "read", Position: 1}},
			},
			want: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := decideCalendarPrivilege(user, cal, "/dav/calendars/1/event.ics", "read", func(candidate string) ([]store.ACLEntry, error) {
				return tt.entries[candidate], nil
			})
			if err != nil {
				t.Fatalf("decideCalendarPrivilege() error = %v", err)
			}
			if got != tt.want {
				t.Fatalf("decideCalendarPrivilege() = %t, want %t", got, tt.want)
			}
		})
	}
}

func TestRemoveCalendarShareRejectsIneffectiveManagedGrant(t *testing.T) {
	aclRepo := &fakeACLRepo{entries: []store.ACLEntry{
		{ID: 1, ResourcePath: "/dav/calendars/1", PrincipalHref: "DAV:authenticated", IsGrant: false, Privilege: "read", Position: 1},
		{ID: 2, ResourcePath: "/dav/calendars/1", PrincipalHref: "/dav/principals/200/", IsGrant: true, Privilege: "read", Position: 2},
	}}
	h := NewHandler(&config.Config{}, &store.Store{ACLEntries: aclRepo}, nil)

	err := h.removeCalendarShare(context.Background(), 1, 200, true)
	if !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("removeCalendarShare() error = %v, want %v", err, store.ErrNotFound)
	}
	if len(aclRepo.entries) != 2 {
		t.Fatalf("expected denied share ACLs to remain unchanged, got %#v", aclRepo.entries)
	}
}

func buildHiddenRecentEventACLs(hiddenCount int) *fakeACLRepo {
	entries := []store.ACLEntry{
		{ResourcePath: "/dav/calendars/1", PrincipalHref: "/dav/principals/100/", IsGrant: true, Privilege: "read"},
	}
	for i := 1; i <= hiddenCount; i++ {
		entries = append(entries, store.ACLEntry{
			ResourcePath:  fmt.Sprintf("/dav/calendars/1/hidden-%d", i),
			PrincipalHref: "/dav/principals/100/",
			IsGrant:       false,
			Privilege:     "read",
		})
	}
	return &fakeACLRepo{entries: entries}
}

func withRouteID(req *http.Request, id string) *http.Request {
	rctx := chi.NewRouteContext()
	rctx.URLParams.Add("id", id)
	return req.WithContext(context.WithValue(req.Context(), chi.RouteCtxKey, rctx))
}

func newICSImportRequest(t *testing.T, target, filename, content string) *http.Request {
	t.Helper()
	var body bytes.Buffer
	writer := multipart.NewWriter(&body)
	part, err := writer.CreateFormFile("ics_file", filename)
	if err != nil {
		t.Fatalf("CreateFormFile() error = %v", err)
	}
	if _, err := part.Write([]byte(content)); err != nil {
		t.Fatalf("Write() error = %v", err)
	}
	if err := writer.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	req := httptest.NewRequest(http.MethodPost, target, &body)
	req.Header.Set("Content-Type", writer.FormDataContentType())
	return req
}

// Fake repositories for testing

type fakeCalendarRepo struct {
	calendars      map[int64]*store.Calendar
	accessible     map[string]*store.CalendarAccess
	listAccessible []store.CalendarAccess
}

func (f *fakeCalendarRepo) GetByID(ctx context.Context, id int64) (*store.Calendar, error) {
	if cal, ok := f.calendars[id]; ok {
		copy := *cal
		return &copy, nil
	}
	return nil, nil
}

func (f *fakeCalendarRepo) ListByUser(ctx context.Context, userID int64) ([]store.Calendar, error) {
	var result []store.Calendar
	for _, cal := range f.calendars {
		if cal.UserID == userID {
			result = append(result, *cal)
		}
	}
	return result, nil
}

func (f *fakeCalendarRepo) ListAccessible(ctx context.Context, userID int64) ([]store.CalendarAccess, error) {
	if f.listAccessible != nil {
		result := make([]store.CalendarAccess, len(f.listAccessible))
		copy(result, f.listAccessible)
		return result, nil
	}
	cals, _ := f.ListByUser(ctx, userID)
	var result []store.CalendarAccess
	for _, cal := range cals {
		result = append(result, store.CalendarAccess{
			Calendar:   cal,
			OwnerEmail: "owner@example.com",
			Shared:     false,
			Editor:     true,
		})
	}
	return result, nil
}

func (f *fakeCalendarRepo) GetAccessible(ctx context.Context, calendarID, userID int64) (*store.CalendarAccess, error) {
	if f.accessible != nil {
		key := fmt.Sprintf("%d:%d", calendarID, userID)
		if cal, ok := f.accessible[key]; ok {
			copy := *cal
			return &copy, nil
		}
		return nil, nil
	}
	cal, _ := f.GetByID(ctx, calendarID)
	if cal == nil || cal.UserID != userID {
		return nil, nil
	}
	return &store.CalendarAccess{
		Calendar:   *cal,
		OwnerEmail: "owner@example.com",
		Shared:     false,
		Editor:     true,
	}, nil
}

func (f *fakeCalendarRepo) Create(ctx context.Context, cal store.Calendar) (*store.Calendar, error) {
	if f.calendars == nil {
		f.calendars = map[int64]*store.Calendar{}
	}
	if cal.ID == 0 {
		cal.ID = int64(len(f.calendars) + 1)
	}
	copy := cal
	f.calendars[copy.ID] = &copy
	return &copy, nil
}

func (f *fakeCalendarRepo) Update(ctx context.Context, userID, id int64, name string, description, timezone, color *string) error {
	cal, ok := f.calendars[id]
	if !ok || cal.UserID != userID {
		return store.ErrNotFound
	}
	cal.Name = name
	cal.Description = description
	cal.Timezone = timezone
	cal.Color = color
	return nil
}

func (f *fakeCalendarRepo) UpdateProperties(ctx context.Context, id int64, props store.CalendarProperties) error {
	cal, ok := f.calendars[id]
	if !ok {
		return store.ErrNotFound
	}
	cal.Name = props.Name
	cal.Description = props.Description
	cal.DescriptionLang = props.DescriptionLang
	cal.Timezone = props.Timezone
	cal.Color = props.Color
	return nil
}

func (f *fakeCalendarRepo) Rename(ctx context.Context, userID, id int64, name string) error {
	return nil
}

func (f *fakeCalendarRepo) Delete(ctx context.Context, userID, id int64) error {
	return nil
}

type fakeEventRepo struct {
	events map[string]*store.Event
	recent []store.Event
	moves  []string
}

func (f *fakeEventRepo) key(calendarID int64, uid string) string {
	return fmt.Sprintf("%d:%s", calendarID, uid)
}

func (f *fakeEventRepo) Upsert(ctx context.Context, event store.Event) (*store.Event, error) {
	return nil, nil
}

func (f *fakeEventRepo) DeleteByUID(ctx context.Context, calendarID int64, uid string) error {
	return nil
}

func (f *fakeEventRepo) GetByUID(ctx context.Context, calendarID int64, uid string) (*store.Event, error) {
	if ev, ok := f.events[f.key(calendarID, uid)]; ok {
		copy := *ev
		return &copy, nil
	}
	return nil, nil
}

func (f *fakeEventRepo) GetByResourceName(ctx context.Context, calendarID int64, resourceName string) (*store.Event, error) {
	for _, ev := range f.events {
		if ev.CalendarID != calendarID {
			continue
		}
		name := ev.ResourceName
		if name == "" {
			name = ev.UID
		}
		if name == resourceName {
			copy := *ev
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
	var result []store.Event
	for _, ev := range f.events {
		if ev.CalendarID == calendarID {
			result = append(result, *ev)
		}
	}
	return result, nil
}

func (f *fakeEventRepo) ListForCalendarFiltered(ctx context.Context, calendarID int64, _ store.EventFilter) ([]store.Event, error) {
	return f.ListForCalendar(ctx, calendarID)
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
	return truncateToLimit(result, limit), nil
}

// truncateToLimit mirrors what every paged repository in internal/store does
// with limit: a non-positive one reads nothing, which is the guard each
// PostgreSQL implementation opens with. A fake reading it as "no limit" instead
// would answer in full where storage answers with nothing.
func truncateToLimit[T any](rows []T, limit int) []T {
	if limit <= 0 {
		return nil
	}
	if len(rows) > limit {
		return rows[:limit]
	}
	return rows
}

func (f *fakeEventRepo) ListForCalendarPaginated(ctx context.Context, calendarID int64, limit, offset int) (*store.PaginatedResult[store.Event], error) {
	events, _ := f.ListForCalendar(ctx, calendarID)
	return &store.PaginatedResult[store.Event]{
		Items:      events,
		TotalCount: len(events),
		Limit:      limit,
		Offset:     offset,
	}, nil
}

func (f *fakeEventRepo) ListByUIDs(ctx context.Context, calendarID int64, uids []string) ([]store.Event, error) {
	return nil, nil
}

func (f *fakeEventRepo) ListModifiedSincePageAfter(ctx context.Context, calendarID, afterID int64, since time.Time, limit int) ([]store.Event, error) {
	return nil, nil
}

func (f *fakeEventRepo) ListRecentByUser(ctx context.Context, userID int64, limit int) ([]store.Event, error) {
	if f.recent != nil {
		if limit <= 0 || limit > len(f.recent) {
			limit = len(f.recent)
		}
		result := make([]store.Event, limit)
		copy(result, f.recent[:limit])
		return result, nil
	}
	return nil, nil
}

func (f *fakeEventRepo) MaxLastModified(ctx context.Context, calendarID int64) (time.Time, error) {
	return time.Time{}, nil
}
func (f *fakeEventRepo) MoveToCalendar(ctx context.Context, fromCalendarID, toCalendarID int64, uid, destResourceName string) error {
	key := f.key(fromCalendarID, uid)
	event := f.events[key]
	if event == nil {
		return store.ErrNotFound
	}
	delete(f.events, key)
	copy := *event
	copy.CalendarID = toCalendarID
	copy.ResourceName = destResourceName
	f.events[f.key(toCalendarID, uid)] = &copy
	f.moves = append(f.moves, fmt.Sprintf("%d:%d:%s", fromCalendarID, toCalendarID, uid))
	return nil
}
func (f *fakeEventRepo) CopyToCalendar(ctx context.Context, fromCalendarID, toCalendarID int64, uid, destResourceName, newETag string) (*store.Event, error) {
	return nil, nil
}

type fakeAddressBookRepo struct {
	books map[int64]*store.AddressBook
}

func (f *fakeAddressBookRepo) GetByID(ctx context.Context, id int64) (*store.AddressBook, error) {
	if book, ok := f.books[id]; ok {
		copy := *book
		return &copy, nil
	}
	return nil, nil
}

func (f *fakeAddressBookRepo) ListByUser(ctx context.Context, userID int64) ([]store.AddressBook, error) {
	var result []store.AddressBook
	for _, book := range f.books {
		if book.UserID == userID {
			result = append(result, *book)
		}
	}
	return result, nil
}

func (f *fakeAddressBookRepo) Create(ctx context.Context, book store.AddressBook) (*store.AddressBook, error) {
	return nil, nil
}

func (f *fakeAddressBookRepo) Update(ctx context.Context, userID, id int64, name string, description *string) error {
	return nil
}

func (f *fakeAddressBookRepo) UpdateProperties(ctx context.Context, id int64, name string, description *string) error {
	return nil
}

func (f *fakeAddressBookRepo) Rename(ctx context.Context, userID, id int64, name string) error {
	return nil
}

func (f *fakeAddressBookRepo) Delete(ctx context.Context, userID, id int64) error {
	return nil
}

type fakeContactRepo struct {
	contacts map[string]*store.Contact
}

type fakeDeadPropertyRepo struct {
	properties map[string][]store.DeadProperty
}

func (f *fakeDeadPropertyRepo) ListByResources(_ context.Context, resourcePaths []string) ([]store.DeadProperty, error) {
	var result []store.DeadProperty
	for _, resourcePath := range resourcePaths {
		result = append(result, f.properties[resourcePath]...)
	}
	return result, nil
}

func (f *fakeDeadPropertyRepo) Apply(_ context.Context, resourcePath string, mutations []store.DeadPropertyMutation) error {
	properties := f.properties[resourcePath]
	for _, mutation := range mutations {
		if !mutation.Remove {
			continue
		}
		filtered := properties[:0]
		for _, property := range properties {
			if property.NamespaceURI != mutation.NamespaceURI || property.LocalName != mutation.LocalName {
				filtered = append(filtered, property)
			}
		}
		properties = filtered
	}
	f.properties[resourcePath] = properties
	return nil
}

func (f *fakeContactRepo) key(bookID int64, uid string) string {
	return fmt.Sprintf("%d:%s", bookID, uid)
}

func (f *fakeContactRepo) Upsert(ctx context.Context, contact store.Contact) (*store.Contact, error) {
	return nil, nil
}

func (f *fakeContactRepo) DeleteByUID(ctx context.Context, addressBookID int64, uid string) error {
	return nil
}

func (f *fakeContactRepo) GetByUID(ctx context.Context, addressBookID int64, uid string) (*store.Contact, error) {
	if c, ok := f.contacts[f.key(addressBookID, uid)]; ok {
		copy := *c
		return &copy, nil
	}
	return nil, nil
}

func (f *fakeContactRepo) ListForBook(ctx context.Context, addressBookID int64) ([]store.Contact, error) {
	var result []store.Contact
	for _, c := range f.contacts {
		if c.AddressBookID == addressBookID {
			result = append(result, *c)
		}
	}
	return result, nil
}

func (f *fakeContactRepo) ListForBookFiltered(ctx context.Context, addressBookID int64, _ store.ContactFilter) ([]store.Contact, error) {
	return f.ListForBook(ctx, addressBookID)
}

func (f *fakeContactRepo) ListForBookPaginated(ctx context.Context, addressBookID int64, limit, offset int) (*store.PaginatedResult[store.Contact], error) {
	contacts, _ := f.ListForBook(ctx, addressBookID)
	return &store.PaginatedResult[store.Contact]{
		Items:      contacts,
		TotalCount: len(contacts),
		Limit:      limit,
		Offset:     offset,
	}, nil
}

func (f *fakeContactRepo) ListByUIDs(ctx context.Context, addressBookID int64, uids []string) ([]store.Contact, error) {
	return nil, nil
}

func (f *fakeContactRepo) ListModifiedSincePageAfter(ctx context.Context, addressBookID, afterID int64, since time.Time, limit int) ([]store.Contact, error) {
	return nil, nil
}

func (f *fakeContactRepo) ListRecentByUser(ctx context.Context, userID int64, limit int) ([]store.Contact, error) {
	return nil, nil
}

func (f *fakeContactRepo) MaxLastModified(ctx context.Context, addressBookID int64) (time.Time, error) {
	return time.Time{}, nil
}

func (f *fakeContactRepo) ListWithBirthdaysByUser(ctx context.Context, userID int64) ([]store.Contact, error) {
	return nil, nil
}

func (f *fakeContactRepo) ListWithBirthdaysByUserLimit(ctx context.Context, userID int64, limit int) ([]store.Contact, error) {
	return nil, nil
}

func (f *fakeContactRepo) MoveToAddressBook(ctx context.Context, fromAddressBookID, toAddressBookID int64, uid, destResourceName string) error {
	return nil
}
func (f *fakeContactRepo) GetByResourceName(ctx context.Context, addressBookID int64, resourceName string) (*store.Contact, error) {
	for _, c := range f.contacts {
		if c.AddressBookID != addressBookID {
			continue
		}
		name := c.ResourceName
		if name == "" {
			name = c.UID
		}
		if name == resourceName {
			copy := *c
			return &copy, nil
		}
	}
	return nil, nil
}
func (f *fakeContactRepo) ListByResourceNames(ctx context.Context, addressBookID int64, resourceNames []string) ([]store.Contact, error) {
	var out []store.Contact
	for _, name := range resourceNames {
		c, err := f.GetByResourceName(ctx, addressBookID, name)
		if err != nil {
			return nil, err
		}
		if c != nil {
			out = append(out, *c)
		}
	}
	return out, nil
}
func (f *fakeContactRepo) CopyToAddressBook(ctx context.Context, fromAddressBookID, toAddressBookID int64, uid, destResourceName, newETag string) (*store.Contact, error) {
	return nil, nil
}

type fakeUserRepo struct {
	users map[int64]*store.User
}

type fakeAppPasswordRepo struct{}

func (f *fakeAppPasswordRepo) Create(ctx context.Context, token store.AppPassword) (*store.AppPassword, error) {
	return nil, nil
}

func (f *fakeAppPasswordRepo) FindValidByUser(ctx context.Context, userID int64) ([]store.AppPassword, error) {
	return nil, nil
}

func (f *fakeAppPasswordRepo) ListByUser(ctx context.Context, userID int64) ([]store.AppPassword, error) {
	return nil, nil
}

func (f *fakeAppPasswordRepo) GetByID(ctx context.Context, id int64) (*store.AppPassword, error) {
	return nil, nil
}

func (f *fakeAppPasswordRepo) Revoke(ctx context.Context, id int64) error {
	return nil
}

func (f *fakeAppPasswordRepo) DeleteRevoked(ctx context.Context, id int64) error {
	return nil
}

func (f *fakeAppPasswordRepo) TouchLastUsed(ctx context.Context, id int64) error {
	return nil
}

func (f *fakeUserRepo) UpsertOAuthUser(ctx context.Context, subject, email, fullName, firstName string) (*store.User, error) {
	return nil, nil
}

func (f *fakeUserRepo) GetByID(ctx context.Context, id int64) (*store.User, error) {
	if user, ok := f.users[id]; ok {
		copy := *user
		return &copy, nil
	}
	return nil, nil
}

func (f *fakeUserRepo) GetByEmail(ctx context.Context, email string) (*store.User, error) {
	for _, user := range f.users {
		if user.PrimaryEmail == email {
			copy := *user
			return &copy, nil
		}
	}
	return nil, nil
}

func (f *fakeUserRepo) ListActive(ctx context.Context) ([]store.User, error) {
	result := make([]store.User, 0, len(f.users))
	for _, user := range f.users {
		result = append(result, *user)
	}
	return result, nil
}

func (f *fakeUserRepo) MarkOnboardingComplete(ctx context.Context, userID int64) error {
	if user, ok := f.users[userID]; ok {
		now := time.Now()
		user.OnboardingCompletedAt = &now
	}
	return nil
}

type fakeACLRepo struct {
	entries                                   []store.ACLEntry
	deletePrincipalEntriesByResourcePrefixErr error
	setACLErr                                 error
	listByResourceCalls                       int
	listByPrincipalCalls                      int
	listByResourcesAndPrincipalsCalls         int
	batchedResourcePaths                      []string
}

func (f *fakeACLRepo) SetACL(ctx context.Context, resourcePath string, entries []store.ACLEntry) error {
	if f.setACLErr != nil {
		return f.setACLErr
	}
	filtered := f.entries[:0]
	for _, entry := range f.entries {
		if entry.ResourcePath == resourcePath {
			continue
		}
		filtered = append(filtered, entry)
	}
	f.entries = append(filtered, entries...)
	return nil
}

func (f *fakeACLRepo) ListByResource(ctx context.Context, resourcePath string) ([]store.ACLEntry, error) {
	f.listByResourceCalls++
	var result []store.ACLEntry
	for _, entry := range f.entries {
		if entry.ResourcePath != resourcePath {
			continue
		}
		result = append(result, entry)
	}
	return result, nil
}

func (f *fakeACLRepo) ListByPrincipal(ctx context.Context, principalHref string) ([]store.ACLEntry, error) {
	f.listByPrincipalCalls++
	var result []store.ACLEntry
	for _, entry := range f.entries {
		if entry.PrincipalHref != principalHref {
			continue
		}
		result = append(result, entry)
	}
	return result, nil
}

func (f *fakeACLRepo) HasPrivilege(ctx context.Context, resourcePath, principalHref, privilege string) (bool, error) {
	for _, entry := range f.entries {
		if entry.ResourcePath == resourcePath && entry.PrincipalHref == principalHref && entry.Privilege == privilege && !entry.IsGrant {
			return false, nil
		}
	}
	for _, entry := range f.entries {
		if entry.ResourcePath == resourcePath && entry.PrincipalHref == principalHref && entry.Privilege == privilege && entry.IsGrant {
			return true, nil
		}
	}
	return false, nil
}

func (f *fakeACLRepo) MoveResourcePath(ctx context.Context, fromPath, toPath string) error {
	for i := range f.entries {
		if f.entries[i].ResourcePath == fromPath {
			f.entries[i].ResourcePath = toPath
		}
	}
	return nil
}

func (f *fakeACLRepo) Delete(ctx context.Context, resourcePath string) error {
	filtered := f.entries[:0]
	for _, entry := range f.entries {
		if entry.ResourcePath == resourcePath {
			continue
		}
		filtered = append(filtered, entry)
	}
	f.entries = filtered
	return nil
}

func (f *fakeACLRepo) DeletePrincipalEntriesByResourcePrefix(ctx context.Context, principalHref, resourcePathPrefix string) error {
	if f.deletePrincipalEntriesByResourcePrefixErr != nil {
		return f.deletePrincipalEntriesByResourcePrefixErr
	}
	filtered := f.entries[:0]
	for _, entry := range f.entries {
		if entry.PrincipalHref == principalHref && (entry.ResourcePath == resourcePathPrefix || strings.HasPrefix(entry.ResourcePath, resourcePathPrefix+"/")) {
			continue
		}
		filtered = append(filtered, entry)
	}
	f.entries = filtered
	return nil
}

// Extended fake repositories for CRUD tests

type fakeEventRepoWithUpsert struct {
	fakeEventRepo
	failOn map[string]error
}

func (f *fakeEventRepoWithUpsert) Upsert(ctx context.Context, event store.Event) (*store.Event, error) {
	if err := f.failOn[event.UID]; err != nil {
		return nil, err
	}
	key := fmt.Sprintf("%d:%s", event.CalendarID, event.UID)
	f.events[key] = &event
	return &event, nil
}

type fakeEventRepoWithDelete struct {
	fakeEventRepo
	deleted []string
}

func (f *fakeEventRepoWithDelete) DeleteByUID(ctx context.Context, calendarID int64, uid string) error {
	f.deleted = append(f.deleted, fmt.Sprintf("%d:%s", calendarID, uid))
	return nil
}

type fakeContactRepoWithUpsert struct {
	fakeContactRepo
}

func (f *fakeContactRepoWithUpsert) Upsert(ctx context.Context, contact store.Contact) (*store.Contact, error) {
	key := fmt.Sprintf("%d:%s", contact.AddressBookID, contact.UID)
	f.contacts[key] = &contact
	return &contact, nil
}

func (f *fakeContactRepoWithUpsert) MoveToAddressBook(ctx context.Context, fromAddressBookID, toAddressBookID int64, uid, destResourceName string) error {
	return f.fakeContactRepo.MoveToAddressBook(ctx, fromAddressBookID, toAddressBookID, uid, destResourceName)
}

type fakeContactRepoWithBirthdays struct {
	fakeContactRepo
	birthdays []store.Contact
}

func (f *fakeContactRepoWithBirthdays) ListWithBirthdaysByUser(ctx context.Context, userID int64) ([]store.Contact, error) {
	return f.birthdays, nil
}

func (f *fakeContactRepoWithBirthdays) ListWithBirthdaysByUserLimit(ctx context.Context, userID int64, limit int) ([]store.Contact, error) {
	return truncateToLimit(f.birthdays, limit), nil
}

func (f *fakeContactRepoWithBirthdays) MoveToAddressBook(ctx context.Context, fromAddressBookID, toAddressBookID int64, uid, destResourceName string) error {
	return f.fakeContactRepo.MoveToAddressBook(ctx, fromAddressBookID, toAddressBookID, uid, destResourceName)
}

func TestMoveContactHandler(t *testing.T) {
	bookRepo := &fakeAddressBookRepo{
		books: map[int64]*store.AddressBook{
			1: {ID: 1, UserID: 100, Name: "Work Contacts"},
			2: {ID: 2, UserID: 100, Name: "Personal Contacts"},
			3: {ID: 3, UserID: 200, Name: "Other User Contacts"},
		},
	}
	contactRepo := &fakeContactRepoWithMove{
		fakeContactRepo: fakeContactRepo{
			contacts: map[string]*store.Contact{
				"1:contact-1": {ID: 1, AddressBookID: 1, UID: "contact-1", RawVCard: "BEGIN:VCARD\r\nVERSION:3.0\r\nFN:John Doe\r\nEND:VCARD\r\n"},
			},
		},
	}

	s := &store.Store{
		AddressBooks: bookRepo,
		Contacts:     contactRepo,
	}

	handler := NewHandler(&config.Config{}, s, nil)

	testCases := []struct {
		name           string
		bookID         string
		uid            string
		userID         int64
		targetBookID   string
		wantStatusCode int
		wantMoved      bool
	}{
		{
			name:           "move contact success",
			bookID:         "1",
			uid:            "contact-1",
			userID:         100,
			targetBookID:   "2",
			wantStatusCode: http.StatusFound,
			wantMoved:      true,
		},
		{
			name:           "missing target address book",
			bookID:         "1",
			uid:            "contact-1",
			userID:         100,
			targetBookID:   "",
			wantStatusCode: http.StatusFound, // Redirects with error
			wantMoved:      false,
		},
		{
			name:           "invalid target address book id",
			bookID:         "1",
			uid:            "contact-1",
			userID:         100,
			targetBookID:   "invalid",
			wantStatusCode: http.StatusFound, // Redirects with error
			wantMoved:      false,
		},
		{
			name:           "source address book not found",
			bookID:         "999",
			uid:            "contact-1",
			userID:         100,
			targetBookID:   "2",
			wantStatusCode: http.StatusNotFound,
			wantMoved:      false,
		},
		{
			name:           "target address book belongs to different user",
			bookID:         "1",
			uid:            "contact-1",
			userID:         100,
			targetBookID:   "3",
			wantStatusCode: http.StatusFound, // Redirects with error
			wantMoved:      false,
		},
		{
			name:           "contact not found",
			bookID:         "1",
			uid:            "nonexistent-contact",
			userID:         100,
			targetBookID:   "2",
			wantStatusCode: http.StatusNotFound,
			wantMoved:      false,
		},
		{
			name:           "source address book belongs to different user",
			bookID:         "3",
			uid:            "contact-1",
			userID:         100,
			targetBookID:   "2",
			wantStatusCode: http.StatusNotFound,
			wantMoved:      false,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// Reset moved state
			contactRepo.moved = false
			contactRepo.tombstoneCreated = false

			form := make(url.Values)
			if tc.targetBookID != "" {
				form.Set("target_address_book_id", tc.targetBookID)
			}

			req := httptest.NewRequest(http.MethodPost, "/addressbooks/"+tc.bookID+"/contacts/"+tc.uid+"/move", strings.NewReader(form.Encode()))
			req.Header.Set("Content-Type", "application/x-www-form-urlencoded")

			rctx := chi.NewRouteContext()
			rctx.URLParams.Add("id", tc.bookID)
			rctx.URLParams.Add("uid", tc.uid)
			req = req.WithContext(context.WithValue(req.Context(), chi.RouteCtxKey, rctx))

			user := &store.User{ID: tc.userID, PrimaryEmail: "test@example.com"}
			req = req.WithContext(auth.WithUser(req.Context(), user))

			w := httptest.NewRecorder()
			handler.MoveContact(w, req)

			if w.Code != tc.wantStatusCode {
				t.Errorf("MoveContact() status = %d, want %d", w.Code, tc.wantStatusCode)
			}

			if contactRepo.moved != tc.wantMoved {
				t.Errorf("MoveContact() moved = %v, want %v", contactRepo.moved, tc.wantMoved)
			}

			// Check redirect location on success
			if tc.wantStatusCode == http.StatusFound && tc.wantMoved {
				location := w.Header().Get("Location")
				expectedLocation := "/addressbooks/" + tc.targetBookID
				if !strings.Contains(location, expectedLocation) {
					t.Errorf("MoveContact() redirect location = %s, expected to contain %s", location, expectedLocation)
				}
			}

			// Verify tombstone was created on successful move
			if tc.wantMoved {
				if !contactRepo.tombstoneCreated {
					t.Error("MoveContact() expected tombstone to be created")
				}
				if contactRepo.tombstoneBookID != 1 {
					t.Errorf("MoveContact() tombstone book ID = %d, want 1", contactRepo.tombstoneBookID)
				}
				if contactRepo.tombstoneUID != tc.uid {
					t.Errorf("MoveContact() tombstone UID = %s, want %s", contactRepo.tombstoneUID, tc.uid)
				}
			}
		})
	}
}

func TestMoveContactRejectsDestinationConflicts(t *testing.T) {
	bookRepo := &fakeAddressBookRepo{
		books: map[int64]*store.AddressBook{
			1: {ID: 1, UserID: 100, Name: "Work Contacts"},
			2: {ID: 2, UserID: 100, Name: "Personal Contacts"},
		},
	}
	contactRepo := &fakeContactRepoWithMove{
		fakeContactRepo: fakeContactRepo{
			contacts: map[string]*store.Contact{
				"1:contact-1": {ID: 1, AddressBookID: 1, UID: "contact-1", ResourceName: "contact-1", RawVCard: "BEGIN:VCARD\r\nVERSION:3.0\r\nFN:John Doe\r\nEND:VCARD\r\n"},
				"2:contact-1": {ID: 2, AddressBookID: 2, UID: "contact-1", ResourceName: "existing", RawVCard: "BEGIN:VCARD\r\nVERSION:3.0\r\nFN:Existing John\r\nEND:VCARD\r\n"},
			},
		},
	}

	handler := NewHandler(&config.Config{}, &store.Store{
		AddressBooks: bookRepo,
		Contacts:     contactRepo,
	}, nil)

	form := make(url.Values)
	form.Set("target_address_book_id", "2")

	req := httptest.NewRequest(http.MethodPost, "/addressbooks/1/contacts/contact-1/move", strings.NewReader(form.Encode()))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")

	rctx := chi.NewRouteContext()
	rctx.URLParams.Add("id", "1")
	rctx.URLParams.Add("uid", "contact-1")
	req = req.WithContext(context.WithValue(req.Context(), chi.RouteCtxKey, rctx))
	req = req.WithContext(auth.WithUser(req.Context(), &store.User{ID: 100, PrimaryEmail: "test@example.com"}))

	rr := httptest.NewRecorder()
	handler.MoveContact(rr, req)

	if rr.Code != http.StatusFound {
		t.Fatalf("MoveContact() status = %d, want %d", rr.Code, http.StatusFound)
	}
	if contactRepo.moved {
		t.Fatal("expected MoveContact() to reject the move before mutating contacts")
	}
	source, _ := contactRepo.GetByUID(req.Context(), 1, "contact-1")
	if source == nil {
		t.Fatal("expected source contact to remain in the source address book")
	}
	dest, _ := contactRepo.GetByUID(req.Context(), 2, "contact-1")
	if dest == nil || dest.ResourceName != "existing" {
		t.Fatalf("expected destination contact to remain unchanged, got %#v", dest)
	}
	location := rr.Header().Get("Location")
	if !strings.Contains(location, "/addressbooks/1") {
		t.Fatalf("expected redirect back to source book, got %q", location)
	}
}

type fakeContactRepoWithMove struct {
	fakeContactRepo
	moved            bool
	tombstoneCreated bool
	tombstoneBookID  int64
	tombstoneUID     string
}

func (f *fakeContactRepoWithMove) MoveToAddressBook(ctx context.Context, fromAddressBookID, toAddressBookID int64, uid, destResourceName string) error {
	// Check if contact exists
	contact, err := f.GetByUID(ctx, fromAddressBookID, uid)
	if err != nil || contact == nil {
		return store.ErrNotFound
	}

	// Move the contact
	oldKey := f.key(fromAddressBookID, uid)
	newKey := f.key(toAddressBookID, uid)
	if c, ok := f.contacts[oldKey]; ok {
		movedContact := *c
		movedContact.AddressBookID = toAddressBookID
		if destResourceName != "" {
			movedContact.ResourceName = destResourceName
		}
		f.contacts[newKey] = &movedContact
		delete(f.contacts, oldKey)
		f.moved = true

		// Simulate tombstone creation (in real implementation, this is done in the database)
		f.tombstoneCreated = true
		f.tombstoneBookID = fromAddressBookID
		f.tombstoneUID = uid
	}
	return nil
}

func TestUpdateEventPreservesResourceName(t *testing.T) {
	testCases := []struct {
		name                 string
		existingResourceName string
		expectedResourceName string
	}{
		{
			name:                 "preserves CalDAV-created resource name",
			existingResourceName: "custom-resource-name-123.ics",
			expectedResourceName: "custom-resource-name-123.ics",
		},
		{
			name:                 "preserves UID-based resource name",
			existingResourceName: "test-event-uid",
			expectedResourceName: "test-event-uid",
		},
		{
			name:                 "handles empty resource name by using UID",
			existingResourceName: "",
			expectedResourceName: "test-event-uid",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			calRepo := &fakeCalendarRepo{
				calendars: map[int64]*store.Calendar{
					1: {ID: 1, UserID: 100, Name: "Test Calendar"},
				},
			}

			eventRepo := &fakeEventRepoWithUpsert{
				fakeEventRepo: fakeEventRepo{
					events: map[string]*store.Event{
						"1:test-event-uid": {
							ID:           1,
							CalendarID:   1,
							UID:          "test-event-uid",
							ResourceName: tc.existingResourceName,
							RawICAL:      "BEGIN:VCALENDAR\r\nVERSION:2.0\r\nBEGIN:VEVENT\r\nUID:test-event-uid\r\nSUMMARY:Original Summary\r\nDTSTART:20240101T100000Z\r\nDTEND:20240101T110000Z\r\nEND:VEVENT\r\nEND:VCALENDAR\r\n",
							ETag:         "original-etag",
						},
					},
				},
			}

			s := &store.Store{
				Calendars: calRepo,
				Events:    eventRepo,
			}

			handler := NewHandler(&config.Config{}, s, nil)

			formData := url.Values{
				"summary":    {"Updated Summary"},
				"dtstart":    {"2024-01-02T10:00"},
				"dtend":      {"2024-01-02T11:00"},
				"edit_scope": {"series"},
			}

			req := httptest.NewRequest(http.MethodPost, "/calendars/1/events/test-event-uid", strings.NewReader(formData.Encode()))
			req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
			req = req.WithContext(auth.WithUser(context.Background(), &store.User{ID: 100}))

			rctx := chi.NewRouteContext()
			rctx.URLParams.Add("id", "1")
			rctx.URLParams.Add("uid", "test-event-uid")
			req = req.WithContext(context.WithValue(req.Context(), chi.RouteCtxKey, rctx))

			rec := httptest.NewRecorder()
			handler.UpdateEvent(rec, req)

			// Check that the event was updated
			updatedEvent := eventRepo.events["1:test-event-uid"]
			if updatedEvent == nil {
				t.Fatal("event should exist after update")
			}

			if updatedEvent.ResourceName != tc.expectedResourceName {
				t.Errorf("ResourceName = %q, want %q", updatedEvent.ResourceName, tc.expectedResourceName)
			}

			// Verify the event content was actually updated
			if !strings.Contains(updatedEvent.RawICAL, "Updated Summary") {
				t.Error("event summary should be updated")
			}
		})
	}
}

func TestUpdateEventMovesSeriesToSelectedCalendar(t *testing.T) {
	raw := "BEGIN:VCALENDAR\r\nBEGIN:VEVENT\r\nUID:event-1\r\nSUMMARY:Old\r\nDTSTART:20260724T100000Z\r\nDTEND:20260724T103000Z\r\nRRULE:FREQ=WEEKLY\r\nEND:VEVENT\r\nEND:VCALENDAR\r\n"
	eventRepo := &fakeEventRepoWithUpsert{
		fakeEventRepo: fakeEventRepo{events: map[string]*store.Event{
			"1:event-1": {
				CalendarID:   1,
				UID:          "event-1",
				ResourceName: "custom-name.ics",
				RawICAL:      raw,
				ETag:         "old",
			},
		}},
	}
	handler := NewHandler(&config.Config{}, &store.Store{
		Calendars: &fakeCalendarRepo{
			calendars: map[int64]*store.Calendar{
				1: {ID: 1, UserID: 100, Name: "Source"},
				2: {ID: 2, UserID: 100, Name: "Destination"},
			},
		},
		Events: eventRepo,
	}, nil)

	form := url.Values{
		"summary":                 {"Updated"},
		"dtstart":                 {"2026-07-24T10:00"},
		"dtend":                   {"2026-07-24T10:30"},
		"recurrence":              {"WEEKLY"},
		"recurrence_interval":     {"1"},
		"recurrence_end_type":     {"never"},
		"destination_calendar_id": {"2"},
		"return_to":               {"/calendars/all?view=week&date=2026-07-24"},
	}
	req := httptest.NewRequest(http.MethodPut, "/calendars/1/events/event-1", strings.NewReader(form.Encode()))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	req = withRouteID(req, "1")
	rctx := chi.RouteContext(req.Context())
	rctx.URLParams.Add("uid", "event-1")
	req = req.WithContext(auth.WithUser(req.Context(), &store.User{ID: 100, PrimaryEmail: "owner@example.com"}))
	w := httptest.NewRecorder()

	handler.UpdateEvent(w, req)

	if w.Code != http.StatusFound {
		t.Fatalf("UpdateEvent() status = %d, want %d: %s", w.Code, http.StatusFound, w.Body.String())
	}
	if eventRepo.events["1:event-1"] != nil {
		t.Fatalf("source calendar still contains moved event: %#v", eventRepo.events["1:event-1"])
	}
	moved := eventRepo.events["2:event-1"]
	if moved == nil {
		t.Fatalf("destination calendar missing moved event: %#v", eventRepo.events)
	}
	if moved.ResourceName != "custom-name.ics" {
		t.Fatalf("moved ResourceName = %q, want custom-name.ics", moved.ResourceName)
	}
	if !strings.Contains(moved.RawICAL, "SUMMARY:Updated") || !strings.Contains(moved.RawICAL, "RRULE:FREQ=WEEKLY") {
		t.Fatalf("moved event did not preserve updated recurring series: %s", moved.RawICAL)
	}
	location := w.Header().Get("Location")
	if !strings.Contains(location, "/calendars/all?") || !strings.Contains(location, "status=event_updated") {
		t.Fatalf("UpdateEvent() redirect = %q, want all-calendars success", location)
	}
}

func TestUpdateEventMoveRequiresDestinationBind(t *testing.T) {
	raw := "BEGIN:VCALENDAR\r\nBEGIN:VEVENT\r\nUID:event-1\r\nSUMMARY:Old\r\nDTSTART:20260724T100000Z\r\nDTEND:20260724T103000Z\r\nEND:VEVENT\r\nEND:VCALENDAR\r\n"
	eventRepo := &fakeEventRepoWithUpsert{
		fakeEventRepo: fakeEventRepo{events: map[string]*store.Event{
			"1:event-1": {CalendarID: 1, UID: "event-1", ResourceName: "event-1", RawICAL: raw},
		}},
	}
	handler := NewHandler(&config.Config{}, &store.Store{
		Calendars: &fakeCalendarRepo{accessible: map[string]*store.CalendarAccess{
			"1:100": {
				Calendar:           store.Calendar{ID: 1, UserID: 100, Name: "Source"},
				Privileges:         store.FullCalendarPrivileges(),
				PrivilegesResolved: true,
			},
			"2:100": {
				Calendar:           store.Calendar{ID: 2, UserID: 200, Name: "Read only"},
				Shared:             true,
				Privileges:         store.CalendarPrivileges{Read: true},
				PrivilegesResolved: true,
			},
		}},
		Events: eventRepo,
	}, nil)

	form := url.Values{
		"summary":                 {"Updated"},
		"dtstart":                 {"2026-07-24T10:00"},
		"dtend":                   {"2026-07-24T10:30"},
		"destination_calendar_id": {"2"},
	}
	req := httptest.NewRequest(http.MethodPut, "/calendars/1/events/event-1", strings.NewReader(form.Encode()))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	req = withRouteID(req, "1")
	chi.RouteContext(req.Context()).URLParams.Add("uid", "event-1")
	req = req.WithContext(auth.WithUser(req.Context(), &store.User{ID: 100, PrimaryEmail: "owner@example.com"}))
	w := httptest.NewRecorder()

	handler.UpdateEvent(w, req)

	if w.Code != http.StatusForbidden {
		t.Fatalf("UpdateEvent() status = %d, want %d", w.Code, http.StatusForbidden)
	}
	if eventRepo.events["1:event-1"] == nil || eventRepo.events["2:event-1"] != nil {
		t.Fatalf("denied move mutated events: %#v", eventRepo.events)
	}
}

func TestUpdateEventMoveRejectsDestinationConflict(t *testing.T) {
	raw := "BEGIN:VCALENDAR\r\nBEGIN:VEVENT\r\nUID:event-1\r\nSUMMARY:Old\r\nDTSTART:20260724T100000Z\r\nDTEND:20260724T103000Z\r\nEND:VEVENT\r\nEND:VCALENDAR\r\n"
	eventRepo := &fakeEventRepoWithUpsert{
		fakeEventRepo: fakeEventRepo{events: map[string]*store.Event{
			"1:event-1": {CalendarID: 1, UID: "event-1", ResourceName: "event-1", RawICAL: raw},
			"2:event-1": {CalendarID: 2, UID: "event-1", ResourceName: "event-1", RawICAL: raw},
		}},
	}
	handler := NewHandler(&config.Config{}, &store.Store{
		Calendars: &fakeCalendarRepo{calendars: map[int64]*store.Calendar{
			1: {ID: 1, UserID: 100, Name: "Source"},
			2: {ID: 2, UserID: 100, Name: "Destination"},
		}},
		Events: eventRepo,
	}, nil)

	form := url.Values{
		"summary":                 {"Updated"},
		"dtstart":                 {"2026-07-24T10:00"},
		"dtend":                   {"2026-07-24T10:30"},
		"destination_calendar_id": {"2"},
		"return_to":               {"/calendars/all?view=month&date=2026-07-24"},
	}
	req := httptest.NewRequest(http.MethodPut, "/calendars/1/events/event-1", strings.NewReader(form.Encode()))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	req = withRouteID(req, "1")
	chi.RouteContext(req.Context()).URLParams.Add("uid", "event-1")
	req = req.WithContext(auth.WithUser(req.Context(), &store.User{ID: 100, PrimaryEmail: "owner@example.com"}))
	w := httptest.NewRecorder()

	handler.UpdateEvent(w, req)

	if w.Code != http.StatusFound {
		t.Fatalf("UpdateEvent() status = %d, want %d", w.Code, http.StatusFound)
	}
	if !strings.Contains(w.Header().Get("Location"), "error=destination+calendar+already+contains+this+event") {
		t.Fatalf("UpdateEvent() redirect = %q, want destination conflict", w.Header().Get("Location"))
	}
	if len(eventRepo.moves) != 0 {
		t.Fatalf("conflicting move executed: %#v", eventRepo.moves)
	}
}

func TestUpdateAllDayEventAllowsSameStartAndEndDate(t *testing.T) {
	calRepo := &fakeCalendarRepo{
		calendars: map[int64]*store.Calendar{
			1: {ID: 1, UserID: 100, Name: "Test Calendar"},
		},
	}
	eventRepo := &fakeEventRepoWithUpsert{
		fakeEventRepo: fakeEventRepo{
			events: map[string]*store.Event{
				"1:test-event-uid": {
					ID:           1,
					CalendarID:   1,
					UID:          "test-event-uid",
					ResourceName: "test-event-uid.ics",
					RawICAL:      "BEGIN:VCALENDAR\r\nVERSION:2.0\r\nBEGIN:VEVENT\r\nUID:test-event-uid\r\nSUMMARY:Original Summary\r\nDTSTART;VALUE=DATE:20260724\r\nDTEND;VALUE=DATE:20260725\r\nEND:VEVENT\r\nEND:VCALENDAR\r\n",
					ETag:         "original-etag",
				},
			},
		},
	}
	handler := NewHandler(&config.Config{}, &store.Store{
		Calendars: calRepo,
		Events:    eventRepo,
	}, nil)

	formData := url.Values{
		"summary":    {"Updated Summary"},
		"dtstart":    {"2026-07-25"},
		"dtend":      {"2026-07-25"},
		"all_day":    {"on"},
		"edit_scope": {"series"},
	}
	req := httptest.NewRequest(http.MethodPost, "/calendars/1/events/test-event-uid", strings.NewReader(formData.Encode()))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	req = req.WithContext(auth.WithUser(context.Background(), &store.User{ID: 100}))
	rctx := chi.NewRouteContext()
	rctx.URLParams.Add("id", "1")
	rctx.URLParams.Add("uid", "test-event-uid")
	req = req.WithContext(context.WithValue(req.Context(), chi.RouteCtxKey, rctx))

	rec := httptest.NewRecorder()
	handler.UpdateEvent(rec, req)

	if location := rec.Header().Get("Location"); !strings.Contains(location, "status=event_updated") {
		t.Fatalf("UpdateEvent() redirect = %q, want successful update", location)
	}
	updated := eventRepo.events["1:test-event-uid"]
	if !strings.Contains(updated.RawICAL, "DTSTART;VALUE=DATE:20260725\r\n") {
		t.Errorf("updated event missing expected start date:\n%s", updated.RawICAL)
	}
	if !strings.Contains(updated.RawICAL, "DTEND;VALUE=DATE:20260726\r\n") {
		t.Errorf("updated event missing exclusive end date:\n%s", updated.RawICAL)
	}
}

func TestUpdateEventRequiresDates(t *testing.T) {
	calRepo := &fakeCalendarRepo{
		calendars: map[int64]*store.Calendar{
			1: {ID: 1, UserID: 100, Name: "Test Calendar"},
		},
	}

	eventRepo := &fakeEventRepoWithUpsert{
		fakeEventRepo: fakeEventRepo{
			events: map[string]*store.Event{
				"1:test-event-uid": {
					ID:           1,
					CalendarID:   1,
					UID:          "test-event-uid",
					ResourceName: "test-event-uid",
					RawICAL:      "BEGIN:VCALENDAR\r\nVERSION:2.0\r\nBEGIN:VEVENT\r\nUID:test-event-uid\r\nSUMMARY:Original Summary\r\nDTSTART:20240101T100000Z\r\nDTEND:20240101T110000Z\r\nEND:VEVENT\r\nEND:VCALENDAR\r\n",
					ETag:         "original-etag",
				},
			},
		},
	}

	s := &store.Store{
		Calendars: calRepo,
		Events:    eventRepo,
	}

	handler := NewHandler(&config.Config{}, s, nil)

	formData := url.Values{
		"summary":    {"Updated Summary"},
		"dtstart":    {"2024-01-02T10:00"},
		"edit_scope": {"series"},
	}

	req := httptest.NewRequest(http.MethodPost, "/calendars/1/events/test-event-uid", strings.NewReader(formData.Encode()))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	req = req.WithContext(auth.WithUser(context.Background(), &store.User{ID: 100}))

	rctx := chi.NewRouteContext()
	rctx.URLParams.Add("id", "1")
	rctx.URLParams.Add("uid", "test-event-uid")
	req = req.WithContext(context.WithValue(req.Context(), chi.RouteCtxKey, rctx))

	rec := httptest.NewRecorder()
	handler.UpdateEvent(rec, req)

	if rec.Code != http.StatusFound {
		t.Fatalf("UpdateEvent() status = %d, want %d", rec.Code, http.StatusFound)
	}

	updatedEvent := eventRepo.events["1:test-event-uid"]
	if updatedEvent == nil {
		t.Fatal("event should exist after update attempt")
	}
	if !strings.Contains(updatedEvent.RawICAL, "Original Summary") {
		t.Error("event summary should remain unchanged when dates are missing")
	}
}
