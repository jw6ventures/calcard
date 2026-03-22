package auth

import (
	"context"
	"crypto/tls"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/jw6ventures/calcard/internal/config"
	"github.com/jw6ventures/calcard/internal/store"
	"golang.org/x/crypto/bcrypt"
	"golang.org/x/oauth2"
)

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(r *http.Request) (*http.Response, error) {
	return f(r)
}

type userRepoMock struct {
	getByIDFn    func(context.Context, int64) (*store.User, error)
	getByEmailFn func(context.Context, string) (*store.User, error)
}

func (m *userRepoMock) UpsertOAuthUser(context.Context, string, string) (*store.User, error) {
	return nil, nil
}
func (m *userRepoMock) GetByID(ctx context.Context, id int64) (*store.User, error) {
	return m.getByIDFn(ctx, id)
}
func (m *userRepoMock) GetByEmail(ctx context.Context, email string) (*store.User, error) {
	return m.getByEmailFn(ctx, email)
}
func (m *userRepoMock) ListActive(context.Context) ([]store.User, error) { return nil, nil }

type appPasswordRepoMock struct {
	createFn          func(context.Context, store.AppPassword) (*store.AppPassword, error)
	findValidByUserFn func(context.Context, int64) ([]store.AppPassword, error)
	touchLastUsedFn   func(context.Context, int64) error
}

func (m *appPasswordRepoMock) Create(ctx context.Context, token store.AppPassword) (*store.AppPassword, error) {
	return m.createFn(ctx, token)
}
func (m *appPasswordRepoMock) FindValidByUser(ctx context.Context, userID int64) ([]store.AppPassword, error) {
	return m.findValidByUserFn(ctx, userID)
}
func (m *appPasswordRepoMock) ListByUser(context.Context, int64) ([]store.AppPassword, error) {
	return nil, nil
}
func (m *appPasswordRepoMock) GetByID(context.Context, int64) (*store.AppPassword, error) {
	return nil, nil
}
func (m *appPasswordRepoMock) Revoke(context.Context, int64) error        { return nil }
func (m *appPasswordRepoMock) DeleteRevoked(context.Context, int64) error { return nil }
func (m *appPasswordRepoMock) TouchLastUsed(ctx context.Context, id int64) error {
	if m.touchLastUsedFn != nil {
		return m.touchLastUsedFn(ctx, id)
	}
	return nil
}

func TestBeginOAuthSetsStateCookieAndRedirects(t *testing.T) {
	service := &Service{
		cfg: &config.Config{BaseURL: "https://calcard.example"},
		oauthCfg: &oauth2.Config{
			ClientID:    "client-id",
			RedirectURL: "https://calcard.example/auth/callback",
			Endpoint: oauth2.Endpoint{
				AuthURL: "https://issuer.example/auth",
			},
		},
	}

	req := httptest.NewRequest(http.MethodGet, "/auth/login", nil)
	rec := httptest.NewRecorder()
	service.BeginOAuth(rec, req)

	if rec.Code != http.StatusFound {
		t.Fatalf("status = %d", rec.Code)
	}
	cookie := rec.Result().Cookies()[0]
	if cookie.Name != "calcard_oauth_state" || cookie.Value == "" || !cookie.Secure {
		t.Fatalf("cookie = %#v", cookie)
	}
	location, err := url.Parse(rec.Header().Get("Location"))
	if err != nil {
		t.Fatalf("redirect parse error = %v", err)
	}
	if location.Query().Get("state") != cookie.Value {
		t.Fatalf("state query = %q, cookie = %q", location.Query().Get("state"), cookie.Value)
	}
}

func TestHandleOAuthCallbackRejectsInvalidStateAndMissingCode(t *testing.T) {
	service := &Service{}

	req := httptest.NewRequest(http.MethodGet, "/auth/callback?state=expected", nil)
	rec := httptest.NewRecorder()
	service.HandleOAuthCallback(rec, req)
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("invalid state status = %d", rec.Code)
	}

	req = httptest.NewRequest(http.MethodGet, "/auth/callback?state=expected", nil)
	req.AddCookie(&http.Cookie{Name: "calcard_oauth_state", Value: "expected"})
	rec = httptest.NewRecorder()
	service.HandleOAuthCallback(rec, req)
	if rec.Code != http.StatusBadRequest || !strings.Contains(rec.Body.String(), "missing oauth code") {
		t.Fatalf("missing code response = %d %q", rec.Code, rec.Body.String())
	}
}

func TestHandleOAuthCallbackReturnsBadRequestOnExchangeFailure(t *testing.T) {
	service := &Service{
		oauthCfg: &oauth2.Config{
			ClientID:     "client-id",
			ClientSecret: "secret",
			Endpoint: oauth2.Endpoint{
				TokenURL: "https://issuer.example/token",
			},
		},
	}

	req := httptest.NewRequest(http.MethodGet, "/auth/callback?state=expected&code=abc", nil)
	req.AddCookie(&http.Cookie{Name: "calcard_oauth_state", Value: "expected"})
	req = req.WithContext(context.WithValue(req.Context(), oauth2.HTTPClient, &http.Client{
		Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
			return &http.Response{
				StatusCode: http.StatusBadGateway,
				Status:     "502 Bad Gateway",
				Body:       io.NopCloser(strings.NewReader("upstream failure")),
				Header:     make(http.Header),
			}, nil
		}),
	}))
	rec := httptest.NewRecorder()
	service.HandleOAuthCallback(rec, req)

	if rec.Code != http.StatusBadRequest || !strings.Contains(rec.Body.String(), "failed to exchange oauth code") {
		t.Fatalf("response = %d %q", rec.Code, rec.Body.String())
	}
}

func TestCreateAndValidateAppPassword(t *testing.T) {
	var stored store.AppPassword
	var touched int64
	user := &store.User{ID: 9, PrimaryEmail: "user@example.com"}

	service := &Service{
		store: &store.Store{
			Users: &userRepoMock{
				getByEmailFn: func(_ context.Context, email string) (*store.User, error) {
					if email != user.PrimaryEmail {
						t.Fatalf("GetByEmail email = %q", email)
					}
					return user, nil
				},
				getByIDFn: func(_ context.Context, id int64) (*store.User, error) {
					return &store.User{ID: id}, nil
				},
			},
			AppPasswords: &appPasswordRepoMock{
				createFn: func(_ context.Context, token store.AppPassword) (*store.AppPassword, error) {
					token.ID = 77
					stored = token
					return &token, nil
				},
				findValidByUserFn: func(_ context.Context, userID int64) ([]store.AppPassword, error) {
					if userID != user.ID {
						t.Fatalf("FindValidByUser userID = %d", userID)
					}
					return []store.AppPassword{
						{ID: 1, TokenHash: "$2a$10$invalidinvalidinvalidinvalidinvalidinvalidinvalidinv"},
						{ID: 77, TokenHash: stored.TokenHash},
					}, nil
				},
				touchLastUsedFn: func(_ context.Context, id int64) error {
					touched = id
					return nil
				},
			},
		},
	}

	plaintext, created, err := service.CreateAppPassword(context.Background(), user.ID, "laptop", nil)
	if err != nil {
		t.Fatalf("CreateAppPassword() error = %v", err)
	}
	if plaintext == "" || created == nil || created.ID != 77 {
		t.Fatalf("CreateAppPassword() = %q %#v", plaintext, created)
	}
	if stored.Label != "laptop" || stored.UserID != user.ID {
		t.Fatalf("stored token = %#v", stored)
	}
	if err := bcrypt.CompareHashAndPassword([]byte(stored.TokenHash), []byte(plaintext)); err != nil {
		t.Fatalf("stored hash does not match plaintext: %v", err)
	}

	validatedUser, err := service.ValidateAppPassword(context.Background(), user.PrimaryEmail, plaintext)
	if err != nil {
		t.Fatalf("ValidateAppPassword() error = %v", err)
	}
	if validatedUser.ID != user.ID || touched != 77 {
		t.Fatalf("validatedUser = %#v, touched = %d", validatedUser, touched)
	}
}

func TestValidateAppPasswordRejectsUnknownExpiredAndRevokedPasswords(t *testing.T) {
	now := time.Now()
	hash, err := bcrypt.GenerateFromPassword([]byte("secret"), bcrypt.DefaultCost)
	if err != nil {
		t.Fatalf("GenerateFromPassword() error = %v", err)
	}
	service := &Service{
		store: &store.Store{
			Users: &userRepoMock{
				getByEmailFn: func(_ context.Context, email string) (*store.User, error) {
					if email == "missing@example.com" {
						return nil, nil
					}
					return &store.User{ID: 1, PrimaryEmail: email}, nil
				},
			},
			AppPasswords: &appPasswordRepoMock{
				findValidByUserFn: func(_ context.Context, userID int64) ([]store.AppPassword, error) {
					return []store.AppPassword{
						{ID: 1, TokenHash: string(hash), RevokedAt: &now},
						{ID: 2, TokenHash: string(hash), ExpiresAt: ptrTime(now.Add(-time.Hour))},
					}, nil
				},
			},
		},
	}

	if _, err := service.ValidateAppPassword(context.Background(), "missing@example.com", "secret"); err == nil || !strings.Contains(err.Error(), "unknown user") {
		t.Fatalf("unknown user error = %v", err)
	}
	if _, err := service.ValidateAppPassword(context.Background(), "user@example.com", "secret"); err == nil || !strings.Contains(err.Error(), "invalid app password") {
		t.Fatalf("invalid password error = %v", err)
	}
}

func TestRequireSessionRedirectsOrInjectsUser(t *testing.T) {
	service := &Service{
		store: &store.Store{
			Users: &userRepoMock{
				getByIDFn: func(_ context.Context, id int64) (*store.User, error) {
					return &store.User{ID: id, PrimaryEmail: "user@example.com"}, nil
				},
			},
		},
		sessions: &SessionManager{
			store: &store.Store{
				Sessions: &sessionRepoMock{
					getByIDFn: func(_ context.Context, id string) (*store.Session, error) {
						if id == "good-session" {
							return &store.Session{ID: id, UserID: 12}, nil
						}
						return nil, errors.New("missing")
					},
				},
			},
			secure: true,
		},
	}

	protected := service.RequireSession(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		user, ok := UserFromContext(r.Context())
		if !ok || user.ID != 12 {
			t.Fatalf("UserFromContext() = %#v, %v", user, ok)
		}
		if got := SessionIDFromContext(r.Context()); got != "good-session" {
			t.Fatalf("SessionIDFromContext() = %q", got)
		}
		w.WriteHeader(http.StatusNoContent)
	}))

	req := httptest.NewRequest(http.MethodGet, "/", nil)
	rec := httptest.NewRecorder()
	protected.ServeHTTP(rec, req)
	if rec.Code != http.StatusFound || rec.Header().Get("Location") != "/auth/login" {
		t.Fatalf("unauthenticated response = %d %q", rec.Code, rec.Header().Get("Location"))
	}

	req = httptest.NewRequest(http.MethodGet, "/", nil)
	req.AddCookie(&http.Cookie{Name: sessionCookieName, Value: "good-session"})
	rec = httptest.NewRecorder()
	protected.ServeHTTP(rec, req)
	if rec.Code != http.StatusNoContent {
		t.Fatalf("authenticated status = %d", rec.Code)
	}
}

func TestRequireDAVAuthChallengesAndAcceptsValidCredentials(t *testing.T) {
	hash, err := bcrypt.GenerateFromPassword([]byte("app-secret"), bcrypt.DefaultCost)
	if err != nil {
		t.Fatalf("GenerateFromPassword() error = %v", err)
	}
	service := &Service{
		store: &store.Store{
			Users: &userRepoMock{
				getByEmailFn: func(_ context.Context, email string) (*store.User, error) {
					return &store.User{ID: 7, PrimaryEmail: email}, nil
				},
			},
			AppPasswords: &appPasswordRepoMock{
				findValidByUserFn: func(_ context.Context, userID int64) ([]store.AppPassword, error) {
					return []store.AppPassword{{ID: 3, TokenHash: string(hash)}}, nil
				},
			},
		},
	}

	handler := service.RequireDAVAuth(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		user, ok := UserFromContext(r.Context())
		if !ok || user.ID != 7 {
			t.Fatalf("UserFromContext() = %#v, %v", user, ok)
		}
		w.WriteHeader(http.StatusNoContent)
	}))

	req := httptest.NewRequest(http.MethodGet, "/dav", nil)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	if rec.Code != http.StatusUnauthorized || rec.Header().Get("WWW-Authenticate") == "" {
		t.Fatalf("challenge response = %d %#v", rec.Code, rec.Header())
	}

	req = httptest.NewRequest(http.MethodGet, "/dav", nil)
	req.SetBasicAuth("user@example.com", "app-secret")
	rec = httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	if rec.Code != http.StatusNoContent {
		t.Fatalf("authenticated status = %d", rec.Code)
	}
}

func TestCookieSecureAndHelpers(t *testing.T) {
	service := &Service{cfg: &config.Config{BaseURL: "https://calcard.example"}}
	req := httptest.NewRequest(http.MethodGet, "/", nil)
	if !service.cookieSecure(req) {
		t.Fatal("expected https base URL to require secure cookies")
	}
	req = req.Clone(req.Context())
	req.TLS = &tls.ConnectionState{}
	if !service.cookieSecure(req) {
		t.Fatal("expected TLS request to require secure cookies")
	}
	if issuerFromDiscovery("https://issuer.example/.well-known/openid-configuration") != "https://issuer.example" {
		t.Fatal("issuerFromDiscovery() did not trim well-known path")
	}
	state, err := randomState()
	if err != nil || state == "" || strings.Contains(state, "=") {
		t.Fatalf("randomState() = %q, %v", state, err)
	}
}

func ptrTime(t time.Time) *time.Time { return &t }
