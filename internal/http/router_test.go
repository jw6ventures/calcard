package httpserver

import (
	"bytes"
	"log"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/go-chi/chi/v5/middleware"
	"github.com/jw6ventures/calcard/internal/auth"
	"github.com/jw6ventures/calcard/internal/config"
	"github.com/jw6ventures/calcard/internal/dav"
	"github.com/jw6ventures/calcard/internal/store"
)

func TestOverrideMethodOnlyPromotesPutAndDeleteOnPost(t *testing.T) {
	tests := []struct {
		name       string
		method     string
		target     string
		formBody   string
		wantMethod string
	}{
		{
			name:       "form override to delete",
			method:     http.MethodPost,
			target:     "/",
			formBody:   "_method=delete",
			wantMethod: http.MethodDelete,
		},
		{
			name:       "query override to put",
			method:     http.MethodPost,
			target:     "/?_method=put",
			wantMethod: http.MethodPut,
		},
		{
			name:       "get ignored",
			method:     http.MethodGet,
			target:     "/?_method=delete",
			wantMethod: http.MethodGet,
		},
		{
			name:       "patch ignored",
			method:     http.MethodPost,
			target:     "/?_method=patch",
			wantMethod: http.MethodPost,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var gotMethod string
			handler := overrideMethod(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				gotMethod = r.Method
				w.WriteHeader(http.StatusNoContent)
			}))

			req := httptest.NewRequest(tt.method, tt.target, strings.NewReader(tt.formBody))
			req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
			rec := httptest.NewRecorder()
			handler.ServeHTTP(rec, req)

			if gotMethod != tt.wantMethod {
				t.Fatalf("method = %q, want %q", gotMethod, tt.wantMethod)
			}
		})
	}
}

func TestNewRouterPublicEndpoints(t *testing.T) {
	db, mock, err := sqlmock.New(sqlmock.MonitorPingsOption(true))
	if err != nil {
		t.Fatalf("sqlmock.New() error = %v", err)
	}
	defer db.Close()

	cfg := &config.Config{BaseURL: "http://localhost:8080", PrometheusEnabled: true}
	r := NewRouter(cfg, store.New(db), nil)

	req := httptest.NewRequest(http.MethodGet, "/healthz", nil)
	rec := httptest.NewRecorder()
	r.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK || rec.Body.String() != "ok" {
		t.Fatalf("/healthz = %d %q", rec.Code, rec.Body.String())
	}

	mock.ExpectPing()
	req = httptest.NewRequest(http.MethodGet, "/readyz", nil)
	rec = httptest.NewRecorder()
	r.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK || rec.Body.String() != "ok" {
		t.Fatalf("/readyz = %d %q", rec.Code, rec.Body.String())
	}

	req = httptest.NewRequest(http.MethodGet, "/.well-known/caldav", nil)
	rec = httptest.NewRecorder()
	r.ServeHTTP(rec, req)
	if rec.Code != http.StatusMovedPermanently || rec.Header().Get("Location") != "/dav/" {
		t.Fatalf("/.well-known/caldav = %d %q", rec.Code, rec.Header().Get("Location"))
	}

	req = httptest.NewRequest("PROPFIND", "/principals/user", nil)
	rec = httptest.NewRecorder()
	r.ServeHTTP(rec, req)
	if rec.Code != http.StatusMovedPermanently || rec.Header().Get("Location") != "/dav/principals/" {
		t.Fatalf("/principals = %d %q", rec.Code, rec.Header().Get("Location"))
	}

	req = httptest.NewRequest(http.MethodOptions, "/dav/calendars", nil)
	rec = httptest.NewRecorder()
	r.ServeHTTP(rec, req)
	if rec.Code != http.StatusNoContent {
		t.Fatalf("OPTIONS /dav = %d", rec.Code)
	}

	req = httptest.NewRequest(http.MethodGet, "/metrics", nil)
	rec = httptest.NewRecorder()
	r.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK || !strings.Contains(rec.Body.String(), "calcard_http_requests_total") {
		t.Fatalf("/metrics = %d", rec.Code)
	}

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("sql expectations: %v", err)
	}
}

func TestNewRouterMetricsCanBeDisabled(t *testing.T) {
	db, _, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New() error = %v", err)
	}
	defer db.Close()

	r := NewRouter(&config.Config{BaseURL: "http://localhost:8080"}, store.New(db), nil)
	req := httptest.NewRequest(http.MethodGet, "/metrics", nil)
	rec := httptest.NewRecorder()
	r.ServeHTTP(rec, req)

	if rec.Code != http.StatusNotFound {
		t.Fatalf("/metrics status = %d", rec.Code)
	}
}

func TestNewRouterWithOptionsWiresDAVExtensions(t *testing.T) {
	db, _, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New() error = %v", err)
	}
	defer db.Close()

	r := NewRouterWithOptions(&config.Config{BaseURL: "http://localhost:8080"}, store.New(db), nil, RouterOptions{
		DAVExtensions: []dav.Extension{davExtensionFunc(func(reg *dav.Registry) {
			reg.RegisterCollection("/dav/pro")
			reg.RegisterMethod("SEARCH", "/dav/pro", dav.MethodOptions{Auth: dav.MethodAuthRequired}, func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(http.StatusAccepted)
			})
		})},
		DAVAuthMiddleware: func(next http.Handler) http.Handler {
			return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				ctx := auth.WithUser(r.Context(), &store.User{ID: 1})
				next.ServeHTTP(w, r.WithContext(ctx))
			})
		},
	})
	req := httptest.NewRequest("SEARCH", "/dav/pro/query", nil)
	rec := httptest.NewRecorder()

	r.ServeHTTP(rec, req)

	if rec.Code != http.StatusAccepted {
		t.Fatalf("extension SEARCH status = %d, want %d: %s", rec.Code, http.StatusAccepted, rec.Body.String())
	}

	req = httptest.NewRequest(http.MethodOptions, "/dav/pro/query", nil)
	rec = httptest.NewRecorder()

	r.ServeHTTP(rec, req)

	if rec.Code != http.StatusNoContent {
		t.Fatalf("extension OPTIONS status = %d, want %d: %s", rec.Code, http.StatusNoContent, rec.Body.String())
	}
	if allow := rec.Header().Get("Allow"); !strings.Contains(allow, "SEARCH") {
		t.Fatalf("extension OPTIONS Allow = %q, want SEARCH", allow)
	}
}

func TestNewRouterWithOptionsHonorsMethodAuthNone(t *testing.T) {
	db, _, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New() error = %v", err)
	}
	defer db.Close()

	r := NewRouterWithOptions(&config.Config{BaseURL: "http://localhost:8080"}, store.New(db), nil, RouterOptions{
		DAVExtensions: []dav.Extension{davExtensionFunc(func(reg *dav.Registry) {
			reg.RegisterCollection("/dav/pro")
			reg.RegisterMethod("SEARCH", "/dav/pro", dav.MethodOptions{Auth: dav.MethodAuthNone}, func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(http.StatusAccepted)
			})
		})},
		// Auth middleware that always rejects: a MethodAuthNone route must still
		// be reachable without passing through it.
		DAVAuthMiddleware: func(next http.Handler) http.Handler {
			return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				http.Error(w, "denied", http.StatusUnauthorized)
			})
		},
	})

	req := httptest.NewRequest("SEARCH", "/dav/pro/query", nil)
	rec := httptest.NewRecorder()

	r.ServeHTTP(rec, req)

	if rec.Code != http.StatusAccepted {
		t.Fatalf("MethodAuthNone SEARCH status = %d, want %d: %s", rec.Code, http.StatusAccepted, rec.Body.String())
	}
}

func TestNewRouterWithOptionsWiresAdditiveDAVMethodOnDefaultPath(t *testing.T) {
	db, _, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New() error = %v", err)
	}
	defer db.Close()

	r := NewRouterWithOptions(&config.Config{BaseURL: "http://localhost:8080"}, store.New(db), nil, RouterOptions{
		DAVExtensions: []dav.Extension{davExtensionFunc(func(reg *dav.Registry) {
			reg.RegisterMethod(http.MethodPost, "/dav/calendars", dav.MethodOptions{Auth: dav.MethodAuthRequired}, func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(http.StatusAccepted)
			})
		})},
		DAVAuthMiddleware: func(next http.Handler) http.Handler {
			return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				ctx := auth.WithUser(r.Context(), &store.User{ID: 1})
				next.ServeHTTP(w, r.WithContext(ctx))
			})
		},
	})

	req := httptest.NewRequest(http.MethodPost, "/dav/calendars/1/outbox", nil)
	rec := httptest.NewRecorder()

	r.ServeHTTP(rec, req)

	if rec.Code != http.StatusAccepted {
		t.Fatalf("extension POST status = %d, want %d: %s", rec.Code, http.StatusAccepted, rec.Body.String())
	}
}

func TestNewRouterDoesNotWriteRequestLogs(t *testing.T) {
	db, _, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New() error = %v", err)
	}
	defer db.Close()

	var logs bytes.Buffer
	originalLogger := middleware.DefaultLogger
	middleware.DefaultLogger = middleware.RequestLogger(&middleware.DefaultLogFormatter{
		Logger:  log.New(&logs, "", 0),
		NoColor: true,
	})
	t.Cleanup(func() {
		middleware.DefaultLogger = originalLogger
	})

	r := NewRouter(&config.Config{BaseURL: "http://localhost:8080"}, store.New(db), nil)
	req := httptest.NewRequest(http.MethodGet, "/healthz", nil)
	rec := httptest.NewRecorder()
	r.ServeHTTP(rec, req)

	if logs.Len() != 0 {
		t.Fatalf("request logs = %q, want none", logs.String())
	}
}

type davExtensionFunc func(*dav.Registry)

func (f davExtensionFunc) RegisterDAV(r *dav.Registry) {
	f(r)
}
