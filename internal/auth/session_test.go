package auth

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/jw6ventures/calcard/internal/config"
	"github.com/jw6ventures/calcard/internal/store"
)

type sessionRepoMock struct {
	createFn        func(context.Context, store.Session) (*store.Session, error)
	getByIDFn       func(context.Context, string) (*store.Session, error)
	touchLastSeenFn func(context.Context, string) error
	deleteFn        func(context.Context, string) error
}

func (m *sessionRepoMock) Create(ctx context.Context, s store.Session) (*store.Session, error) {
	return m.createFn(ctx, s)
}
func (m *sessionRepoMock) GetByID(ctx context.Context, id string) (*store.Session, error) {
	return m.getByIDFn(ctx, id)
}
func (m *sessionRepoMock) ListByUser(context.Context, int64) ([]store.Session, error) {
	return nil, nil
}
func (m *sessionRepoMock) TouchLastSeen(ctx context.Context, id string) error {
	if m.touchLastSeenFn != nil {
		return m.touchLastSeenFn(ctx, id)
	}
	return nil
}
func (m *sessionRepoMock) Delete(ctx context.Context, id string) error {
	if m.deleteFn != nil {
		return m.deleteFn(ctx, id)
	}
	return nil
}
func (m *sessionRepoMock) DeleteByUser(context.Context, int64) error    { return nil }
func (m *sessionRepoMock) DeleteExpired(context.Context) (int64, error) { return 0, nil }

func TestSessionManagerIssueStoresMetadataAndSetsCookie(t *testing.T) {
	var created store.Session
	manager := NewSessionManager(&config.Config{
		BaseURL:        "https://calcard.example",
		TrustedProxies: []string{"10.0.0.0/8"},
	}, &store.Store{
		Sessions: &sessionRepoMock{
			createFn: func(_ context.Context, s store.Session) (*store.Session, error) {
				created = s
				return &s, nil
			},
		},
	})

	req := httptest.NewRequest(http.MethodGet, "/", nil)
	req.Header.Set("User-Agent", "CalCard Test")
	req.Header.Set("X-Forwarded-For", "198.51.100.8, 10.0.0.1")
	req.RemoteAddr = "10.1.1.1:1234"
	rec := httptest.NewRecorder()

	if err := manager.Issue(context.Background(), rec, req, 42); err != nil {
		t.Fatalf("Issue() error = %v", err)
	}

	if created.UserID != 42 || created.ID == "" {
		t.Fatalf("created session = %#v", created)
	}
	if created.UserAgent == nil || *created.UserAgent != "CalCard Test" {
		t.Fatalf("UserAgent = %#v", created.UserAgent)
	}
	if created.IPAddress == nil || *created.IPAddress != "198.51.100.8" {
		t.Fatalf("IPAddress = %#v", created.IPAddress)
	}
	cookie := rec.Result().Cookies()[0]
	if cookie.Name != sessionCookieName || !cookie.Secure || cookie.Value == "" {
		t.Fatalf("cookie = %#v", cookie)
	}
}

func TestSessionManagerClearDeletesStoredSessionAndExpiresCookie(t *testing.T) {
	deleted := ""
	manager := NewSessionManager(&config.Config{BaseURL: "http://localhost:8080"}, &store.Store{
		Sessions: &sessionRepoMock{
			deleteFn: func(_ context.Context, id string) error {
				deleted = id
				return nil
			},
		},
	})

	req := httptest.NewRequest(http.MethodGet, "/", nil)
	req.AddCookie(&http.Cookie{Name: sessionCookieName, Value: "session-123"})
	rec := httptest.NewRecorder()
	manager.Clear(context.Background(), rec, req)

	if deleted != "session-123" {
		t.Fatalf("deleted session id = %q", deleted)
	}
	cookie := rec.Result().Cookies()[0]
	if cookie.Value != "" || cookie.Secure {
		t.Fatalf("cookie = %#v", cookie)
	}
}

func TestSessionManagerCurrentUserIDReadsSessionAndTouchesLastSeen(t *testing.T) {
	touched := make(chan string, 1)
	manager := NewSessionManager(&config.Config{BaseURL: "https://calcard.example"}, &store.Store{
		Sessions: &sessionRepoMock{
			getByIDFn: func(_ context.Context, id string) (*store.Session, error) {
				return &store.Session{ID: id, UserID: 55}, nil
			},
			touchLastSeenFn: func(_ context.Context, id string) error {
				touched <- id
				return nil
			},
		},
	})

	req := httptest.NewRequest(http.MethodGet, "/", nil)
	req.AddCookie(&http.Cookie{Name: sessionCookieName, Value: "session-55"})

	userID, sessionID, ok := manager.CurrentUserID(context.Background(), req)
	if !ok || userID != 55 || sessionID != "session-55" {
		t.Fatalf("CurrentUserID() = (%d, %q, %v)", userID, sessionID, ok)
	}

	select {
	case got := <-touched:
		if got != "session-55" {
			t.Fatalf("TouchLastSeen() id = %q", got)
		}
	case <-time.After(time.Second):
		t.Fatal("TouchLastSeen() was not called")
	}
}

func TestGetClientIP(t *testing.T) {
	tests := []struct {
		name    string
		manager *SessionManager
		req     *http.Request
		want    string
	}{
		{
			name: "xff returns first untrusted hop from right",
			manager: &SessionManager{
				trustedProxies: parseTrustedProxies([]string{"10.0.0.0/8"}),
			},
			req: func() *http.Request {
				r := httptest.NewRequest(http.MethodGet, "/", nil)
				r.Header.Set("X-Forwarded-For", " 198.51.100.1 , 203.0.113.9 , 10.0.0.1 ")
				r.RemoteAddr = "10.1.1.1:1234"
				return r
			}(),
			want: "203.0.113.9",
		},
		{
			name: "xff skips trusted hops",
			manager: &SessionManager{
				trustedProxies: parseTrustedProxies([]string{"10.0.0.0/8"}),
			},
			req: func() *http.Request {
				r := httptest.NewRequest(http.MethodGet, "/", nil)
				r.Header.Set("X-Forwarded-For", "198.51.100.1, 10.0.0.2, 10.0.0.3")
				r.RemoteAddr = "10.1.1.1:1234"
				return r
			}(),
			want: "198.51.100.1",
		},
		{
			name: "x-real-ip",
			manager: &SessionManager{
				trustedProxies: parseTrustedProxies([]string{"10.0.0.0/8"}),
			},
			req: func() *http.Request {
				r := httptest.NewRequest(http.MethodGet, "/", nil)
				r.Header.Set("X-Real-IP", " 198.51.100.2 ")
				r.RemoteAddr = "10.1.1.1:1234"
				return r
			}(),
			want: "198.51.100.2",
		},
		{
			name:    "empty trusted proxies trusts forwarded headers",
			manager: &SessionManager{},
			req: func() *http.Request {
				r := httptest.NewRequest(http.MethodGet, "/", nil)
				r.Header.Set("X-Forwarded-For", "198.51.100.9")
				r.RemoteAddr = "203.0.113.10:1234"
				return r
			}(),
			want: "198.51.100.9",
		},
		{
			name: "untrusted remote ignores forwarded headers",
			manager: &SessionManager{
				trustedProxies: parseTrustedProxies([]string{"10.0.0.0/8"}),
			},
			req: func() *http.Request {
				r := httptest.NewRequest(http.MethodGet, "/", nil)
				r.Header.Set("X-Forwarded-For", "198.51.100.3")
				r.Header.Set("X-Real-IP", "198.51.100.4")
				r.RemoteAddr = "203.0.113.11:1234"
				return r
			}(),
			want: "203.0.113.11",
		},
		{
			name: "remote addr fallback",
			manager: &SessionManager{
				trustedProxies: parseTrustedProxies([]string{"10.0.0.0/8"}),
			},
			req: func() *http.Request {
				r := httptest.NewRequest(http.MethodGet, "/", nil)
				r.RemoteAddr = "198.51.100.3:1234"
				return r
			}(),
			want: "198.51.100.3",
		},
		{
			name:    "invalid forwarded header falls back to remote addr host",
			manager: &SessionManager{},
			req: func() *http.Request {
				r := httptest.NewRequest(http.MethodGet, "/", nil)
				r.Header.Set("X-Forwarded-For", "not-an-ip")
				r.RemoteAddr = "198.51.100.4:4321"
				return r
			}(),
			want: "198.51.100.4",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.manager.getClientIP(tt.req); got != tt.want {
				t.Fatalf("getClientIP() = %q, want %q", got, tt.want)
			}
		})
	}
}

func TestParseTrustedProxies(t *testing.T) {
	trusted := parseTrustedProxies([]string{"10.0.0.0/8", "127.0.0.1", "2001:db8::1", "bad-value"})
	if len(trusted) != 3 {
		t.Fatalf("trusted proxy count = %d", len(trusted))
	}
}

func TestGenerateSessionID(t *testing.T) {
	id, err := generateSessionID()
	if err != nil {
		t.Fatalf("generateSessionID() error = %v", err)
	}
	if id == "" || strings.Contains(id, "=") {
		t.Fatalf("generateSessionID() = %q", id)
	}
}
