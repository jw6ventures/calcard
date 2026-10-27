package api

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"net"
	"net/http"
	"net/http/httptest"
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

// Every REST endpoint answers a database it cannot reach with 503 and
// Retry-After, so a client retries rather than reading the outage as a missing
// resource or a fault of its request, and a statement the database failed with
// a 500; neither carries the driver's error.
func TestEveryAPIEndpointAnswersAStoreFailureAsTheServers(t *testing.T) {
	const eventBody = `{"inputMode":"structured","structured":{"summary":"Planning","dtstart":"2026-03-20T10:00","dtend":"2026-03-20T11:00"}}`
	const contactBody = `{"inputMode":"structured","structured":{"displayName":"Card"}}`
	failures := []struct {
		name       string
		err        error
		wantStatus int
	}{
		{name: "connection refused", err: &net.OpError{Op: "dial", Net: "tcp", Err: errors.New("connect: connection refused")}, wantStatus: http.StatusServiceUnavailable},
		{name: "too many clients", err: &pq.Error{Severity: "FATAL", Code: "53300", Message: "sorry, too many clients already"}, wantStatus: http.StatusServiceUnavailable},
		{name: "statement failure", err: errors.New(`pq: relation "calendars" does not exist`), wantStatus: http.StatusInternalServerError},
	}
	requests := []struct {
		method, path, body string
	}{
		{method: http.MethodGet, path: "/api/calendars"},
		{method: http.MethodGet, path: "/api/calendars/1"},
		{method: http.MethodGet, path: "/api/calendars/1/events"},
		{method: http.MethodGet, path: "/api/calendars/1/events/event"},
		{method: http.MethodPost, path: "/api/calendars/1/events", body: eventBody},
		{method: http.MethodPut, path: "/api/calendars/1/events/event", body: eventBody},
		{method: http.MethodDelete, path: "/api/calendars/1/events/event"},
		{method: http.MethodGet, path: "/api/addressbooks"},
		{method: http.MethodGet, path: "/api/addressbooks/5"},
		{method: http.MethodGet, path: "/api/addressbooks/5/shares"},
		{method: http.MethodPost, path: "/api/addressbooks/5/shares", body: `{"userId":2,"role":"editor"}`},
		{method: http.MethodDelete, path: "/api/addressbooks/5/shares/2"},
		{method: http.MethodGet, path: "/api/addressbooks/5/contacts"},
		{method: http.MethodGet, path: "/api/addressbooks/5/contacts/card"},
		{method: http.MethodPost, path: "/api/addressbooks/5/contacts", body: contactBody},
		{method: http.MethodPut, path: "/api/addressbooks/5/contacts/card", body: contactBody},
		{method: http.MethodDelete, path: "/api/addressbooks/5/contacts/card"},
	}
	for _, failure := range failures {
		for _, request := range requests {
			t.Run(failure.name+" "+request.method+" "+request.path, func(t *testing.T) {
				pool := sql.OpenDB(unreachableDatabase{err: failure.err})
				t.Cleanup(func() { _ = pool.Close() })
				router := apiRouter(NewHandler(&config.Config{}, store.New(pool), dav.ValidateCalendarObject))
				req := httptest.NewRequest(request.method, request.path, strings.NewReader(request.body))
				req.Header.Set("Content-Type", "application/json")
				req = req.WithContext(auth.WithUser(req.Context(), &store.User{ID: 1, PrimaryEmail: "user@example.com"}))
				rec := httptest.NewRecorder()

				router.ServeHTTP(rec, req)

				if rec.Code != failure.wantStatus {
					t.Fatalf("status = %d, want %d: %q", rec.Code, failure.wantStatus, rec.Body.String())
				}
				body := rec.Body.String()
				if strings.Contains(body, failure.err.Error()) || strings.Contains(body, "pq:") || strings.Contains(body, "connect:") {
					t.Fatalf("body %q exposes the store error", body)
				}
				retryAfter := rec.Header().Get("Retry-After")
				if failure.wantStatus != http.StatusServiceUnavailable {
					if retryAfter != "" {
						t.Fatalf("Retry-After = %q on a %d", retryAfter, rec.Code)
					}
					return
				}
				if retryAfter != storeRetryAfterSeconds {
					t.Fatalf("Retry-After = %q, want %q", retryAfter, storeRetryAfterSeconds)
				}
				if got := strings.TrimSpace(body); got != http.StatusText(http.StatusServiceUnavailable) {
					t.Fatalf("body = %q, want the status text alone", got)
				}
			})
		}
	}
}

// apiRouter mounts h as internal/http mounts it under /api.
func apiRouter(h *Handler) http.Handler {
	r := chi.NewRouter()
	r.Get("/api/calendars", h.ListCalendars)
	r.Get("/api/calendars/{id}", h.GetCalendar)
	r.Get("/api/calendars/{id}/events", h.ListEvents)
	r.Get("/api/calendars/{id}/events/{uid}", h.GetEvent)
	r.Post("/api/calendars/{id}/events", h.CreateEvent)
	r.Put("/api/calendars/{id}/events/{uid}", h.UpdateEvent)
	r.Delete("/api/calendars/{id}/events/{uid}", h.DeleteEvent)
	r.Get("/api/addressbooks", h.ListAddressBooks)
	r.Get("/api/addressbooks/{id}", h.GetAddressBook)
	r.Get("/api/addressbooks/{id}/shares", h.ListAddressBookShares)
	r.Post("/api/addressbooks/{id}/shares", h.ShareAddressBook)
	r.Delete("/api/addressbooks/{id}/shares/{userId}", h.UnshareAddressBook)
	r.Get("/api/addressbooks/{id}/contacts", h.ListContacts)
	r.Get("/api/addressbooks/{id}/contacts/{uid}", h.GetContact)
	r.Post("/api/addressbooks/{id}/contacts", h.CreateContact)
	r.Put("/api/addressbooks/{id}/contacts/{uid}", h.UpdateContact)
	r.Delete("/api/addressbooks/{id}/contacts/{uid}", h.DeleteContact)
	return r
}
