package csrf

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/jw6ventures/calcard/internal/config"
)

func TestMiddlewareIssuesTokenAndStoresItInContext(t *testing.T) {
	cfg := &config.Config{BaseURL: "https://calcard.example"}
	var tokenFromHandler string

	handler := Middleware(cfg)(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		tokenFromHandler = TokenFromContext(r.Context())
		w.WriteHeader(http.StatusNoContent)
	}))

	req := httptest.NewRequest(http.MethodGet, "/", nil)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusNoContent {
		t.Fatalf("status = %d", rec.Code)
	}
	cookies := rec.Result().Cookies()
	if len(cookies) != 1 {
		t.Fatalf("cookies = %#v", cookies)
	}
	if cookies[0].Name != csrfCookieName || cookies[0].Value == "" {
		t.Fatalf("cookie = %#v", cookies[0])
	}
	if !cookies[0].Secure {
		t.Fatal("expected secure cookie for https base URL")
	}
	if tokenFromHandler != cookies[0].Value {
		t.Fatalf("context token = %q, want %q", tokenFromHandler, cookies[0].Value)
	}
}

func TestMiddlewareRejectsStateChangingRequestsWithoutMatchingToken(t *testing.T) {
	cfg := &config.Config{BaseURL: "http://localhost:8080"}
	handler := Middleware(cfg)(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNoContent)
	}))

	req := httptest.NewRequest(http.MethodPost, "/", nil)
	req.AddCookie(&http.Cookie{Name: csrfCookieName, Value: "expected"})
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusForbidden {
		t.Fatalf("status = %d", rec.Code)
	}
}

func TestMiddlewareAcceptsHeaderAndFormTokens(t *testing.T) {
	cfg := &config.Config{BaseURL: "http://localhost:8080"}
	tests := []struct {
		name   string
		method string
		setup  func(*http.Request)
	}{
		{
			name:   "header",
			method: http.MethodDelete,
			setup: func(r *http.Request) {
				r.Header.Set("X-CSRF-Token", "token")
			},
		},
		{
			name:   "form",
			method: http.MethodPost,
			setup: func(r *http.Request) {
				r.Header.Set("Content-Type", "application/x-www-form-urlencoded")
				r.PostForm = map[string][]string{"_csrf": {"token"}}
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			handler := Middleware(cfg)(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(http.StatusAccepted)
			}))
			req := httptest.NewRequest(tt.method, "/", nil)
			req.AddCookie(&http.Cookie{Name: csrfCookieName, Value: "token"})
			tt.setup(req)
			rec := httptest.NewRecorder()
			handler.ServeHTTP(rec, req)
			if rec.Code != http.StatusAccepted {
				t.Fatalf("status = %d", rec.Code)
			}
		})
	}
}

func TestIsStateChanging(t *testing.T) {
	for _, method := range []string{http.MethodPost, http.MethodPut, http.MethodPatch, http.MethodDelete} {
		if !isStateChanging(method) {
			t.Fatalf("%s should be state changing", method)
		}
	}
	if isStateChanging(http.MethodGet) {
		t.Fatal("GET should not be state changing")
	}
}
