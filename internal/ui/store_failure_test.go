package ui

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"mime/multipart"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"

	"github.com/go-chi/chi/v5"
	"github.com/jw6ventures/calcard/internal/auth"
	"github.com/jw6ventures/calcard/internal/config"
	"github.com/jw6ventures/calcard/internal/dav"
	"github.com/jw6ventures/calcard/internal/store"
	"github.com/lib/pq"
)

// unreachableDatabase is a connection pool whose every attempt to connect
// fails with err, as lib/pq's does while PostgreSQL is stopped or out of
// connection slots.
type unreachableDatabase struct{ err error }

func (c unreachableDatabase) Connect(context.Context) (driver.Conn, error) { return nil, c.err }
func (c unreachableDatabase) Driver() driver.Driver                        { return c }
func (c unreachableDatabase) Open(string) (driver.Conn, error)             { return nil, c.err }

// Every page and form answers a database it cannot reach with 503 and
// Retry-After. A flash redirect would only lead to another page the database
// cannot serve, and a "not found" would tell the user their data is gone.
func TestEveryUIPageAndFormAnswersAnUnavailableStoreWith503(t *testing.T) {
	failures := []struct {
		name string
		err  error
	}{
		{name: "connection refused", err: &net.OpError{Op: "dial", Net: "tcp", Err: errors.New("connect: connection refused")}},
		{name: "too many clients", err: &pq.Error{Severity: "FATAL", Code: "53300", Message: "sorry, too many clients already"}},
	}
	eventForm := url.Values{"summary": {"Planning"}, "dtstart": {"2026-03-20T10:00"}, "dtend": {"2026-03-20T11:00"}, "etag": {"etag"}}.Encode()
	contactForm := url.Values{"display_name": {"Card"}, "email": {"not an address"}}.Encode()
	calendarImport, calendarImportType := multipartFile(t, "ics_file", "BEGIN:VCALENDAR\r\nVERSION:2.0\r\nPRODID:-//test//EN\r\nBEGIN:VEVENT\r\nUID:imported\r\nDTSTAMP:20260101T000000Z\r\nDTSTART:20260102T100000Z\r\nSUMMARY:Imported\r\nEND:VEVENT\r\nEND:VCALENDAR\r\n")
	cardImport, cardImportType := multipartFile(t, "file", "BEGIN:VCARD\r\nVERSION:3.0\r\nUID:imported\r\nFN:Imported\r\nEND:VCARD\r\n")
	// collectionsOnly marks a request the collection repositories serve on their
	// own, which has nothing to fail once they are readable.
	requests := []struct {
		method, path, form, contentType string
		collectionsOnly                 bool
	}{
		{method: http.MethodGet, path: "/"},
		{method: http.MethodGet, path: "/calendars"},
		{method: http.MethodGet, path: "/calendars/all", collectionsOnly: true},
		{method: http.MethodGet, path: "/calendars/all/events.json?start=2026-03-01&end=2026-04-01", collectionsOnly: true},
		{method: http.MethodGet, path: "/calendars/1", collectionsOnly: true},
		{method: http.MethodGet, path: "/calendars/1/export"},
		{method: http.MethodGet, path: "/calendars/1/events.json?start=2026-03-01&end=2026-04-01"},
		{method: http.MethodGet, path: "/addressbooks"},
		{method: http.MethodGet, path: "/addressbooks/5"},
		{method: http.MethodGet, path: "/app-passwords"},
		{method: http.MethodGet, path: "/sessions"},
		{method: http.MethodGet, path: "/birthdays"},
		{method: http.MethodPost, path: "/calendars", form: "name=Work", collectionsOnly: true},
		{method: http.MethodPut, path: "/calendars/1", form: "name=Renamed"},
		{method: http.MethodDelete, path: "/calendars/1"},
		{method: http.MethodPost, path: "/calendars/1/shares", form: "user_id=2"},
		{method: http.MethodDelete, path: "/calendars/1/shares/2"},
		{method: http.MethodPost, path: "/calendars/1/import", form: calendarImport, contentType: calendarImportType},
		{method: http.MethodPost, path: "/calendars/1/events", form: eventForm},
		{method: http.MethodPut, path: "/calendars/1/events/event", form: eventForm},
		{method: http.MethodDelete, path: "/calendars/1/events/event", form: "etag=etag"},
		{method: http.MethodPost, path: "/addressbooks", form: "name=Friends", collectionsOnly: true},
		{method: http.MethodPut, path: "/addressbooks/5", form: "name=Renamed"},
		{method: http.MethodDelete, path: "/addressbooks/5"},
		{method: http.MethodPost, path: "/addressbooks/5/shares", form: "user_id=2"},
		{method: http.MethodDelete, path: "/addressbooks/5/shares/2"},
		{method: http.MethodPost, path: "/addressbooks/5/import", form: cardImport, contentType: cardImportType},
		{method: http.MethodPost, path: "/addressbooks/5/contacts", form: "display_name=Card"},
		{method: http.MethodPut, path: "/addressbooks/5/contacts/card", form: contactForm},
		{method: http.MethodDelete, path: "/addressbooks/5/contacts/card"},
		{method: http.MethodPost, path: "/addressbooks/5/contacts/card/move", form: "target_address_book_id=6"},
		{method: http.MethodDelete, path: "/app-passwords/1"},
		{method: http.MethodPost, path: "/app-passwords/1/delete"},
		{method: http.MethodPost, path: "/sessions/other/revoke"},
		{method: http.MethodPost, path: "/sessions/revoke-all"},
		{method: http.MethodPost, path: "/onboarding/complete"},
	}
	// The collections are either unreadable too, so every request fails at its
	// first read, or readable, so it fails reading events, contacts or ACLs.
	stores := []struct {
		name  string
		build func(*store.Store)
	}{
		{name: "database down", build: func(*store.Store) {}},
		{name: "collections readable", build: func(st *store.Store) {
			st.Calendars = &fakeCalendarRepo{calendars: map[int64]*store.Calendar{1: {ID: 1, UserID: 1, Name: "Work", CTag: 1}}}
			st.AddressBooks = &fakeAddressBookRepo{books: map[int64]*store.AddressBook{
				5: {ID: 5, UserID: 1, Name: "Contacts", CTag: 1},
				6: {ID: 6, UserID: 1, Name: "Other", CTag: 1},
			}}
		}},
	}
	for _, failure := range failures {
		for _, variant := range stores {
			for _, request := range requests {
				if request.collectionsOnly && variant.name == "collections readable" {
					continue
				}
				t.Run(variant.name+" "+failure.name+" "+request.method+" "+request.path, func(t *testing.T) {
					pool := sql.OpenDB(unreachableDatabase{err: failure.err})
					t.Cleanup(func() { _ = pool.Close() })
					st := store.New(pool)
					variant.build(st)
					router := uiRouter(NewHandler(&config.Config{}, st, nil, dav.ValidateCalendarObject, dav.EventInstances))
					req := httptest.NewRequest(request.method, request.path, strings.NewReader(request.form))
					switch {
					case request.contentType != "":
						req.Header.Set("Content-Type", request.contentType)
					case request.form != "":
						req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
					}
					ctx := auth.WithUser(req.Context(), &store.User{ID: 1, PrimaryEmail: "user@example.com"})
					req = req.WithContext(auth.WithSessionID(ctx, "current"))
					rec := httptest.NewRecorder()

					router.ServeHTTP(rec, req)

					if rec.Code != http.StatusServiceUnavailable {
						t.Fatalf("status = %d, want 503 (Location %q): %q", rec.Code, rec.Header().Get("Location"), rec.Body.String())
					}
					if got := rec.Header().Get("Retry-After"); got != storeRetryAfterSeconds {
						t.Fatalf("Retry-After = %q, want %q", got, storeRetryAfterSeconds)
					}
					if got := strings.TrimSpace(rec.Body.String()); got != http.StatusText(http.StatusServiceUnavailable) {
						t.Fatalf("body = %q, want the status text alone", got)
					}
				})
			}
		}
	}
}

// multipartFile is a multipart form carrying content as the file field, and
// its content type.
func multipartFile(t *testing.T, field, content string) (string, string) {
	t.Helper()
	var body strings.Builder
	form := multipart.NewWriter(&body)
	part, err := form.CreateFormFile(field, "upload")
	if err != nil {
		t.Fatalf("CreateFormFile() error = %v", err)
	}
	if _, err := part.Write([]byte(content)); err != nil {
		t.Fatalf("Write() error = %v", err)
	}
	if err := form.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}
	return body.String(), form.FormDataContentType()
}

// uiRouter mounts h's pages and forms as internal/http mounts them behind the
// session middleware.
func uiRouter(h *Handler) http.Handler {
	r := chi.NewRouter()
	r.Get("/", h.Dashboard)
	r.Get("/calendars", h.Calendars)
	r.Get("/calendars/all", h.ViewAllCalendars)
	r.Get("/calendars/all/events.json", h.GetAllCalendarEventsJSON)
	r.Get("/calendars/{id}", h.ViewCalendar)
	r.Get("/calendars/{id}/export", h.ExportCalendar)
	r.Get("/calendars/{id}/events.json", h.GetCalendarEventsJSON)
	r.Get("/addressbooks", h.AddressBooks)
	r.Get("/addressbooks/{id}", h.ViewAddressBook)
	r.Get("/app-passwords", h.AppPasswords)
	r.Get("/sessions", h.Sessions)
	r.Get("/birthdays", h.ViewBirthdays)
	r.Post("/calendars", h.CreateCalendar)
	r.Put("/calendars/{id}", h.RenameCalendar)
	r.Delete("/calendars/{id}", h.DeleteCalendar)
	r.Post("/calendars/{id}/shares", h.ShareCalendar)
	r.Delete("/calendars/{id}/shares/{userId}", h.UnshareCalendar)
	r.Post("/calendars/{id}/import", h.ImportCalendar)
	r.Post("/calendars/{id}/events", h.CreateEvent)
	r.Put("/calendars/{id}/events/{uid}", h.UpdateEvent)
	r.Delete("/calendars/{id}/events/{uid}", h.DeleteEvent)
	r.Post("/addressbooks", h.CreateAddressBook)
	r.Put("/addressbooks/{id}", h.RenameAddressBook)
	r.Delete("/addressbooks/{id}", h.DeleteAddressBook)
	r.Post("/addressbooks/{id}/shares", h.ShareAddressBook)
	r.Delete("/addressbooks/{id}/shares/{userId}", h.UnshareAddressBook)
	r.Post("/addressbooks/{id}/import", h.ImportAddressBook)
	r.Post("/addressbooks/{id}/contacts", h.CreateContact)
	r.Put("/addressbooks/{id}/contacts/{uid}", h.UpdateContact)
	r.Delete("/addressbooks/{id}/contacts/{uid}", h.DeleteContact)
	r.Post("/addressbooks/{id}/contacts/{uid}/move", h.MoveContact)
	r.Delete("/app-passwords/{id}", h.RevokeAppPassword)
	r.Post("/app-passwords/{id}/delete", h.DeleteAppPassword)
	r.Post("/sessions/{id}/revoke", h.RevokeSession)
	r.Post("/sessions/revoke-all", h.RevokeAllSessions)
	r.Post("/onboarding/complete", h.CompleteOnboarding)
	return r
}
