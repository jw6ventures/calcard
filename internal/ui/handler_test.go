package ui

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"

	"gitea.jw6.us/james/calcard/internal/auth"
	"gitea.jw6.us/james/calcard/internal/config"
	"gitea.jw6.us/james/calcard/internal/store"
	"github.com/go-chi/chi/v5"
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

	s := &store.Store{
		Calendars: calRepo,
		Events:    eventRepo,
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

// Fake repositories for testing

type fakeCalendarRepo struct {
	calendars map[int64]*store.Calendar
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
	return nil, nil
}

func (f *fakeCalendarRepo) Update(ctx context.Context, userID, id int64, name string, description, timezone *string) error {
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

func (f *fakeEventRepo) ListForCalendar(ctx context.Context, calendarID int64) ([]store.Event, error) {
	var result []store.Event
	for _, ev := range f.events {
		if ev.CalendarID == calendarID {
			result = append(result, *ev)
		}
	}
	return result, nil
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

func (f *fakeEventRepo) ListModifiedSince(ctx context.Context, calendarID int64, since time.Time) ([]store.Event, error) {
	return nil, nil
}

func (f *fakeEventRepo) ListRecentByUser(ctx context.Context, userID int64, limit int) ([]store.Event, error) {
	return nil, nil
}

func (f *fakeEventRepo) MaxLastModified(ctx context.Context, calendarID int64) (time.Time, error) {
	return time.Time{}, nil
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

func (f *fakeAddressBookRepo) Rename(ctx context.Context, userID, id int64, name string) error {
	return nil
}

func (f *fakeAddressBookRepo) Delete(ctx context.Context, userID, id int64) error {
	return nil
}

type fakeContactRepo struct {
	contacts map[string]*store.Contact
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

func (f *fakeContactRepo) ListModifiedSince(ctx context.Context, addressBookID int64, since time.Time) ([]store.Contact, error) {
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

func (f *fakeContactRepo) MoveToAddressBook(ctx context.Context, fromAddressBookID, toAddressBookID int64, uid string) error {
	return nil
}

// Extended fake repositories for CRUD tests

type fakeEventRepoWithUpsert struct {
	fakeEventRepo
}

func (f *fakeEventRepoWithUpsert) Upsert(ctx context.Context, event store.Event) (*store.Event, error) {
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

func (f *fakeContactRepoWithUpsert) MoveToAddressBook(ctx context.Context, fromAddressBookID, toAddressBookID int64, uid string) error {
	return f.fakeContactRepo.MoveToAddressBook(ctx, fromAddressBookID, toAddressBookID, uid)
}

type fakeContactRepoWithBirthdays struct {
	fakeContactRepo
	birthdays []store.Contact
}

func (f *fakeContactRepoWithBirthdays) ListWithBirthdaysByUser(ctx context.Context, userID int64) ([]store.Contact, error) {
	return f.birthdays, nil
}

func (f *fakeContactRepoWithBirthdays) MoveToAddressBook(ctx context.Context, fromAddressBookID, toAddressBookID int64, uid string) error {
	return f.fakeContactRepo.MoveToAddressBook(ctx, fromAddressBookID, toAddressBookID, uid)
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

type fakeContactRepoWithMove struct {
	fakeContactRepo
	moved             bool
	tombstoneCreated  bool
	tombstoneBookID   int64
	tombstoneUID      string
}

func (f *fakeContactRepoWithMove) MoveToAddressBook(ctx context.Context, fromAddressBookID, toAddressBookID int64, uid string) error {
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
