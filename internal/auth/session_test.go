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

func TestGenerateSessionID(t *testing.T) {
	id, err := generateSessionID()
	if err != nil {
		t.Fatalf("generateSessionID() error = %v", err)
	}
	if id == "" || strings.Contains(id, "=") {
		t.Fatalf("generateSessionID() = %q", id)
	}
}
