package auth

import (
	"context"
	"crypto/md5"
	"crypto/sha256"
	"crypto/tls"
	"encoding/hex"
	"errors"
	"fmt"
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

func (m *userRepoMock) UpsertOAuthUser(context.Context, string, string, string, string) (*store.User, error) {
	return nil, nil
}
func (m *userRepoMock) GetByID(ctx context.Context, id int64) (*store.User, error) {
	return m.getByIDFn(ctx, id)
}
func (m *userRepoMock) GetByEmail(ctx context.Context, email string) (*store.User, error) {
	return m.getByEmailFn(ctx, email)
}
func (m *userRepoMock) ListActive(context.Context) ([]store.User, error)    { return nil, nil }
func (m *userRepoMock) MarkOnboardingComplete(context.Context, int64) error { return nil }

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
		cfg: testAuthConfig("https://calcard.example"),
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

// testAuthConfig builds the configuration a Service needs in tests: a base URL
// plus the session secret the digest nonce and HA1 keys are derived from.
func testAuthConfig(baseURL string) *config.Config {
	cfg := &config.Config{BaseURL: baseURL}
	cfg.Session.Secret = "test-session-secret-at-least-32-bytes-long"
	return cfg
}

// davRequest builds a DAV request that arrived over TLS, which is what
// RequireDAVAuth conditions Basic authentication on.
func davRequest(method, target string) *http.Request {
	req := httptest.NewRequest(method, target, nil)
	req.TLS = &tls.ConnectionState{}
	return req
}

func TestCreateAndValidateAppPassword(t *testing.T) {
	var stored store.AppPassword
	touched := make(chan int64, 1)
	user := &store.User{ID: 9, PrimaryEmail: "user@example.com"}

	service := &Service{
		cfg: testAuthConfig("https://calcard.example"),
		store: &store.Store{
			Users: &userRepoMock{
				getByEmailFn: func(_ context.Context, email string) (*store.User, error) {
					if email != user.PrimaryEmail {
						t.Fatalf("GetByEmail email = %q", email)
					}
					return user, nil
				},
				getByIDFn: func(_ context.Context, id int64) (*store.User, error) {
					return &store.User{ID: id, PrimaryEmail: user.PrimaryEmail}, nil
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
					touched <- id
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
	// An HA1 authenticates without the password, so what reaches the row must
	// not be the bare hash Digest computes over the credentials.
	for _, tc := range []struct {
		algorithm string
		stored    *string
	}{
		{algorithm: "MD5", stored: stored.DigestMD5HA1},
		{algorithm: "SHA-256", stored: stored.DigestSHA256HA1},
	} {
		bare := testDigestHash(tc.algorithm, user.PrimaryEmail+":"+davDigestRealm+":"+plaintext)
		if tc.stored == nil || *tc.stored == "" {
			t.Fatalf("%s HA1 was not stored", tc.algorithm)
		}
		if *tc.stored == bare || strings.Contains(*tc.stored, bare) {
			t.Fatalf("%s HA1 stored in the clear: %q", tc.algorithm, *tc.stored)
		}
		opened, err := service.decryptDigestHA1(tc.algorithm, *tc.stored)
		if err != nil {
			t.Fatalf("decryptDigestHA1(%s) error = %v", tc.algorithm, err)
		}
		if opened != bare {
			t.Fatalf("decryptDigestHA1(%s) = %q, want %q", tc.algorithm, opened, bare)
		}
		// The algorithm is authenticated data, so a value cannot be moved
		// between the two columns and still open.
		other := "SHA-256"
		if tc.algorithm == "SHA-256" {
			other = "MD5"
		}
		if _, err := service.decryptDigestHA1(other, *tc.stored); err == nil {
			t.Fatalf("%s HA1 opened as %s", tc.algorithm, other)
		}
	}

	validatedUser, err := service.ValidateAppPassword(context.Background(), user.PrimaryEmail, plaintext)
	if err != nil {
		t.Fatalf("ValidateAppPassword() error = %v", err)
	}
	// touch_last_used now runs asynchronously off the request path.
	select {
	case id := <-touched:
		if id != 77 {
			t.Fatalf("touched = %d", id)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("TouchLastUsed was not called")
	}
	if validatedUser.ID != user.ID {
		t.Fatalf("validatedUser = %#v", validatedUser)
	}
}

func TestValidateAppPasswordRejectsUnknownExpiredAndRevokedPasswords(t *testing.T) {
	now := time.Now()
	hash, err := bcrypt.GenerateFromPassword([]byte("secret"), bcrypt.DefaultCost)
	if err != nil {
		t.Fatalf("GenerateFromPassword() error = %v", err)
	}
	service := &Service{
		cfg: testAuthConfig("https://calcard.example"),
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
		cfg: testAuthConfig("https://calcard.example"),
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

	req := davRequest(http.MethodGet, "/dav")
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	if rec.Code != http.StatusUnauthorized || rec.Header().Get("WWW-Authenticate") == "" {
		t.Fatalf("challenge response = %d %#v", rec.Code, rec.Header())
	}

	req = davRequest(http.MethodGet, "/dav")
	req.SetBasicAuth("user@example.com", "app-secret")
	rec = httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	if rec.Code != http.StatusNoContent {
		t.Fatalf("authenticated status = %d", rec.Code)
	}
}

// sealedTestHA1s returns the stored form of a token's digest credentials, so a
// test presents what CreateAppPassword actually writes rather than a bare HA1
// the verification path would refuse.
func sealedTestHA1s(t *testing.T, service *Service, username, password string) (*string, *string) {
	t.Helper()
	md5HA1, sha256HA1, err := service.sealDigestCredentials(username, password)
	if err != nil {
		t.Fatalf("sealDigestCredentials() error = %v", err)
	}
	if md5HA1 == nil || sha256HA1 == nil {
		t.Fatal("sealDigestCredentials() returned no credentials")
	}
	return md5HA1, sha256HA1
}

func TestRequireDAVAuthAcceptsSHA256AndMD5DigestAndRejectsReplay(t *testing.T) {
	const (
		username = "user@example.com"
		password = "app-secret"
	)
	service := &Service{cfg: testAuthConfig("https://calcard.example")}
	md5HA1, sha256HA1 := sealedTestHA1s(t, service, username, password)
	service.store = &store.Store{
		Users: &userRepoMock{getByEmailFn: func(_ context.Context, email string) (*store.User, error) {
			return &store.User{ID: 7, PrimaryEmail: email}, nil
		}},
		AppPasswords: &appPasswordRepoMock{findValidByUserFn: func(_ context.Context, userID int64) ([]store.AppPassword, error) {
			return []store.AppPassword{{ID: 3, DigestMD5HA1: md5HA1, DigestSHA256HA1: sha256HA1}}, nil
		}},
	}
	handler := service.RequireDAVAuth(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		user, ok := UserFromContext(r.Context())
		if !ok || user.ID != 7 {
			t.Fatalf("UserFromContext() = %#v, %v", user, ok)
		}
		w.WriteHeader(http.StatusNoContent)
	}))

	challengeRequest := davRequest(http.MethodGet, "/dav/calendars/?view=all")
	challengeResponse := httptest.NewRecorder()
	handler.ServeHTTP(challengeResponse, challengeRequest)
	if challengeResponse.Code != http.StatusUnauthorized {
		t.Fatalf("challenge status = %d", challengeResponse.Code)
	}
	challenges := challengeResponse.Header().Values("WWW-Authenticate")
	if len(challenges) != 3 || !strings.HasPrefix(challenges[0], "Digest ") || !strings.Contains(challenges[0], "algorithm=SHA-256") || strings.Contains(challenges[0], "charset=") ||
		!strings.Contains(challenges[1], "algorithm=MD5") || challenges[2] != `Basic realm="CalCard DAV"` {
		t.Fatalf("WWW-Authenticate = %#v", challenges)
	}

	for _, algorithm := range []string{"SHA-256", "MD5"} {
		t.Run(algorithm, func(t *testing.T) {
			algorithmChallengeResponse := httptest.NewRecorder()
			handler.ServeHTTP(algorithmChallengeResponse, davRequest(http.MethodGet, "/dav/calendars/?view=all"))
			challenge := digestChallengeForAlgorithm(t, algorithmChallengeResponse.Header().Values("WWW-Authenticate"), algorithm)
			authorization := testDigestAuthorization(t, challenge, algorithm, username, password, http.MethodGet, "/dav/calendars/?view=all", "00000001", "client-nonce-"+algorithm)
			req := davRequest(http.MethodGet, "/dav/calendars/?view=all")
			req.Header.Set("Authorization", authorization)
			rec := httptest.NewRecorder()
			handler.ServeHTTP(rec, req)
			if rec.Code != http.StatusNoContent {
				t.Fatalf("authenticated status = %d: %s", rec.Code, rec.Body.String())
			}

			replay := davRequest(http.MethodGet, "/dav/calendars/?view=all")
			replay.Header.Set("Authorization", authorization)
			replayRec := httptest.NewRecorder()
			handler.ServeHTTP(replayRec, replay)
			if replayRec.Code != http.StatusUnauthorized {
				t.Fatalf("replayed digest status = %d, want 401", replayRec.Code)
			}
		})
	}
}

func TestRequireDAVAuthDoesNotOfferOrAcceptBasicOnCleartextTransport(t *testing.T) {
	service := &Service{cfg: testAuthConfig("http://calcard.example")}
	handler := service.RequireDAVAuth(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusNoContent)
	}))

	challenge := httptest.NewRecorder()
	handler.ServeHTTP(challenge, httptest.NewRequest(http.MethodGet, "http://calcard.example/dav/", nil))
	if got := challenge.Header().Values("WWW-Authenticate"); len(got) != 2 {
		t.Fatalf("cleartext challenges = %#v, want only SHA-256 and MD5 Digest", got)
	}
	for _, value := range challenge.Header().Values("WWW-Authenticate") {
		if strings.HasPrefix(value, "Basic ") {
			t.Fatalf("cleartext response advertised Basic: %#v", challenge.Header().Values("WWW-Authenticate"))
		}
	}

	basic := httptest.NewRequest(http.MethodGet, "http://calcard.example/dav/", nil)
	basic.SetBasicAuth("user@example.com", "secret")
	basicResponse := httptest.NewRecorder()
	handler.ServeHTTP(basicResponse, basic)
	if basicResponse.Code != http.StatusUnauthorized {
		t.Fatalf("cleartext Basic status = %d, want 401", basicResponse.Code)
	}

	tlsRequest := httptest.NewRequest(http.MethodGet, "https://calcard.example/dav/", nil)
	tlsRequest.TLS = &tls.ConnectionState{}
	tlsResponse := httptest.NewRecorder()
	handler.ServeHTTP(tlsResponse, tlsRequest)
	if got := tlsResponse.Header().Values("WWW-Authenticate"); len(got) != 3 || !strings.HasPrefix(got[2], "Basic ") {
		t.Fatalf("TLS challenges = %#v, want Digest plus Basic", got)
	}
}

// A deployment naming its trusted proxies believes X-Forwarded-Proto only from
// those peers. The check reads r.RemoteAddr, so it holds only while the
// forwarded-address middleware leaves that field alone for an untrusted peer --
// a client that could rewrite it would be answering the trust question itself
// and could unlock Basic over cleartext.
func TestRequestIsSecureRejectsSpoofedForwardedHeadersFromUntrustedPeers(t *testing.T) {
	trusted := []string{"10.0.0.0/8"}

	tests := []struct {
		name       string
		remoteAddr string
		want       bool
	}{
		{name: "a trusted proxy is believed", remoteAddr: "10.1.2.3:4567", want: true},
		{name: "an untrusted peer is not", remoteAddr: "203.0.113.9:4567", want: false},
		{name: "an unparseable peer is not", remoteAddr: "not-an-address", want: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req := httptest.NewRequest(http.MethodGet, "http://calcard.example/dav/", nil)
			req.RemoteAddr = tt.remoteAddr
			req.Header.Set("X-Forwarded-Proto", "https")
			if got := RequestIsSecure(req, trusted); got != tt.want {
				t.Fatalf("RequestIsSecure(%q) = %v, want %v", tt.remoteAddr, got, tt.want)
			}
		})
	}
}

func TestTrustedProxiesAllowsPeer(t *testing.T) {
	tests := []struct {
		name       string
		remoteAddr string
		trusted    []string
		want       bool
	}{
		// An unconfigured deployment keeps the documented posture: every peer's
		// forwarded headers are honoured, and the config loader warns about it.
		{name: "no configured proxies trusts every peer", remoteAddr: "203.0.113.9:4567", want: true},
		{name: "a peer inside the CIDR", remoteAddr: "10.1.2.3:4567", trusted: []string{"10.0.0.0/8"}, want: true},
		{name: "a peer outside the CIDR", remoteAddr: "203.0.113.9:4567", trusted: []string{"10.0.0.0/8"}},
		{name: "a bare IP entry", remoteAddr: "192.0.2.7:1234", trusted: []string{"192.0.2.7"}, want: true},
		{name: "an address with no port", remoteAddr: "10.1.2.3", trusted: []string{"10.0.0.0/8"}, want: true},
		{name: "an unparseable address", remoteAddr: "not-an-address", trusted: []string{"10.0.0.0/8"}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := NewTrustedProxies(tt.trusted).AllowsPeer(tt.remoteAddr); got != tt.want {
				t.Fatalf("NewTrustedProxies(%v).AllowsPeer(%q) = %v, want %v", tt.trusted, tt.remoteAddr, got, tt.want)
			}
		})
	}
}

func TestDigestNonceCountAcceptsUniqueOutOfOrderValues(t *testing.T) {
	service := &Service{digestNow: func() time.Time { return time.Unix(1_700_000_000, 0) }}

	if !service.acceptDigestNonceCount(7, "nonce", 2) {
		t.Fatal("first nonce count was rejected")
	}
	if !service.acceptDigestNonceCount(7, "nonce", 1) {
		t.Fatal("unique out-of-order nonce count was rejected")
	}
	if service.acceptDigestNonceCount(7, "nonce", 2) {
		t.Fatal("duplicate nonce count was accepted")
	}
	if service.acceptDigestNonceCount(7, "nonce", 1) {
		t.Fatal("duplicate out-of-order nonce count was accepted")
	}
}

func TestAuthCacheClearUserRemovesOnlyThatUsersCredentials(t *testing.T) {
	service := &Service{}
	firstKey := authCacheKey("first@example.com", "first-secret")
	secondKey := authCacheKey("second@example.com", "second-secret")
	service.authCachePut(firstKey, &store.User{ID: 1}, 10)
	service.authCachePut(secondKey, &store.User{ID: 2}, 20)

	service.authCacheClearUser(1)

	if _, ok := service.authCacheGet(firstKey); ok {
		t.Fatal("cleared user's cached credential remains valid")
	}
	if _, ok := service.authCacheGet(secondKey); !ok {
		t.Fatal("clearing one user removed another user's cached credential")
	}
}

func TestRequireDAVAuthDigestRejectsBadRevokedExpiredAndMismatchedCredentials(t *testing.T) {
	const (
		username = "user@example.com"
		password = "app-secret"
	)
	now := time.Now()
	md5HA1 := testDigestHash("MD5", username+":"+davDigestRealm+":"+password)
	sha256HA1 := testDigestHash("SHA-256", username+":"+davDigestRealm+":"+password)
	service := &Service{
		cfg: testAuthConfig("https://calcard.example"),
		store: &store.Store{
			Users: &userRepoMock{getByEmailFn: func(_ context.Context, email string) (*store.User, error) {
				return &store.User{ID: 7, PrimaryEmail: email}, nil
			}},
			AppPasswords: &appPasswordRepoMock{findValidByUserFn: func(_ context.Context, userID int64) ([]store.AppPassword, error) {
				return []store.AppPassword{
					{ID: 1, DigestSHA256HA1: &sha256HA1, RevokedAt: &now},
					{ID: 2, DigestSHA256HA1: &sha256HA1, ExpiresAt: ptrTime(now.Add(-time.Minute))},
					{ID: 3, DigestMD5HA1: &md5HA1, DigestSHA256HA1: &sha256HA1},
				}, nil
			}},
		},
	}
	handler := service.RequireDAVAuth(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(http.StatusNoContent) }))
	challengeRec := httptest.NewRecorder()
	handler.ServeHTTP(challengeRec, httptest.NewRequest(http.MethodGet, "/dav/", nil))
	challenge := digestChallengeForAlgorithm(t, challengeRec.Header().Values("WWW-Authenticate"), "SHA-256")

	valid := testDigestAuthorization(t, challenge, "SHA-256", username, password, http.MethodGet, "/dav/", "00000001", "cnonce")
	for name, authorization := range map[string]string{
		"bad response":        strings.Replace(valid, `response="`, `response="00`, 1),
		"different URI":       strings.Replace(valid, `uri="/dav/"`, `uri="/dav/other"`, 1),
		"malformed duplicate": valid + `, username="attacker@example.com"`,
	} {
		t.Run(name, func(t *testing.T) {
			req := httptest.NewRequest(http.MethodGet, "/dav/", nil)
			req.Header.Set("Authorization", authorization)
			rec := httptest.NewRecorder()
			handler.ServeHTTP(rec, req)
			if rec.Code != http.StatusUnauthorized {
				t.Fatalf("status = %d, want 401", rec.Code)
			}
		})
	}
}

func TestRequireDAVAuthDigestRejectsRevokedAndExpiredTokensInIsolation(t *testing.T) {
	clock := time.Date(2026, 8, 3, 12, 0, 0, 0, time.UTC)
	const (
		username = "user@example.com"
		password = "app-secret"
	)
	ha1 := testDigestHash("SHA-256", username+":"+davDigestRealm+":"+password)
	tests := []struct {
		name  string
		token store.AppPassword
	}{
		{name: "revoked", token: store.AppPassword{ID: 1, DigestSHA256HA1: &ha1, RevokedAt: &clock}},
		{name: "expired", token: store.AppPassword{ID: 2, DigestSHA256HA1: &ha1, ExpiresAt: &clock}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			service := &Service{
				cfg:       &config.Config{BaseURL: "https://calcard.example"},
				digestNow: func() time.Time { return clock },
				store: &store.Store{
					Users: &userRepoMock{getByEmailFn: func(_ context.Context, email string) (*store.User, error) {
						return &store.User{ID: 7, PrimaryEmail: email}, nil
					}},
					AppPasswords: &appPasswordRepoMock{findValidByUserFn: func(context.Context, int64) ([]store.AppPassword, error) {
						return []store.AppPassword{tt.token}, nil
					}},
				},
			}
			handler := service.RequireDAVAuth(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				w.WriteHeader(http.StatusNoContent)
			}))
			challengeRec := httptest.NewRecorder()
			handler.ServeHTTP(challengeRec, httptest.NewRequest(http.MethodGet, "/dav/", nil))
			challenge := digestChallengeForAlgorithm(t, challengeRec.Header().Values("WWW-Authenticate"), "SHA-256")
			authorization := testDigestAuthorization(t, challenge, "SHA-256", username, password, http.MethodGet, "/dav/", "00000001", "cnonce")
			req := httptest.NewRequest(http.MethodGet, "/dav/", nil)
			req.Header.Set("Authorization", authorization)
			rec := httptest.NewRecorder()

			handler.ServeHTTP(rec, req)

			if rec.Code != http.StatusUnauthorized {
				t.Fatalf("status = %d, want 401", rec.Code)
			}
		})
	}
}

func TestLegacyAppPasswordWithoutDigestHA1RemainsBasicOnly(t *testing.T) {
	const (
		username = "user@example.com"
		password = "legacy-app-secret"
	)
	hash, err := bcrypt.GenerateFromPassword([]byte(password), bcrypt.MinCost)
	if err != nil {
		t.Fatalf("GenerateFromPassword() error = %v", err)
	}
	service := &Service{cfg: testAuthConfig("https://calcard.example"), store: &store.Store{
		Users: &userRepoMock{getByEmailFn: func(_ context.Context, email string) (*store.User, error) {
			return &store.User{ID: 7, PrimaryEmail: email}, nil
		}},
		AppPasswords: &appPasswordRepoMock{findValidByUserFn: func(context.Context, int64) ([]store.AppPassword, error) {
			return []store.AppPassword{{ID: 3, TokenHash: string(hash)}}, nil
		}},
	}}
	handler := service.RequireDAVAuth(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusNoContent)
	}))

	basicReq := davRequest(http.MethodGet, "/dav/")
	basicReq.SetBasicAuth(username, password)
	basicRec := httptest.NewRecorder()
	handler.ServeHTTP(basicRec, basicReq)
	if basicRec.Code != http.StatusNoContent {
		t.Fatalf("legacy Basic status = %d, want 204", basicRec.Code)
	}

	challengeRec := httptest.NewRecorder()
	handler.ServeHTTP(challengeRec, httptest.NewRequest(http.MethodGet, "/dav/", nil))
	challenge := digestChallengeForAlgorithm(t, challengeRec.Header().Values("WWW-Authenticate"), "SHA-256")
	digestReq := httptest.NewRequest(http.MethodGet, "/dav/", nil)
	digestReq.Header.Set("Authorization", testDigestAuthorization(t, challenge, "SHA-256", username, password, http.MethodGet, "/dav/", "00000001", "cnonce"))
	digestRec := httptest.NewRecorder()
	handler.ServeHTTP(digestRec, digestReq)
	if digestRec.Code != http.StatusUnauthorized {
		t.Fatalf("legacy Digest status = %d, want 401", digestRec.Code)
	}
}

func TestRequireDAVAuthDigestMarksExpiredSignedNonceStale(t *testing.T) {
	clock := time.Date(2026, 8, 2, 12, 0, 0, 0, time.UTC)
	const (
		username = "user@example.com"
		password = "app-secret"
	)
	sha256HA1 := testDigestHash("SHA-256", username+":"+davDigestRealm+":"+password)
	service := &Service{
		cfg:       &config.Config{BaseURL: "https://calcard.example"},
		digestNow: func() time.Time { return clock },
		store: &store.Store{
			Users: &userRepoMock{getByEmailFn: func(_ context.Context, email string) (*store.User, error) {
				return &store.User{ID: 7, PrimaryEmail: email}, nil
			}},
			AppPasswords: &appPasswordRepoMock{findValidByUserFn: func(_ context.Context, userID int64) ([]store.AppPassword, error) {
				return []store.AppPassword{{ID: 3, DigestSHA256HA1: &sha256HA1}}, nil
			}},
		},
	}
	handler := service.RequireDAVAuth(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(http.StatusNoContent) }))
	challengeRec := httptest.NewRecorder()
	handler.ServeHTTP(challengeRec, httptest.NewRequest(http.MethodGet, "/dav/", nil))
	challenge := digestChallengeForAlgorithm(t, challengeRec.Header().Values("WWW-Authenticate"), "SHA-256")
	authorization := testDigestAuthorization(t, challenge, "SHA-256", username, password, http.MethodGet, "/dav/", "00000001", "cnonce")

	clock = clock.Add(davDigestNonceTTL + time.Second)
	req := httptest.NewRequest(http.MethodGet, "/dav/", nil)
	req.Header.Set("Authorization", authorization)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("expired nonce status = %d, want 401", rec.Code)
	}
	for _, value := range rec.Header().Values("WWW-Authenticate")[:2] {
		if !strings.Contains(value, "stale=true") {
			t.Fatalf("stale challenge = %q", value)
		}
	}
}

func digestChallengeForAlgorithm(t *testing.T, challenges []string, algorithm string) string {
	t.Helper()
	for _, challenge := range challenges {
		if strings.HasPrefix(challenge, "Digest ") && strings.Contains(challenge, "algorithm="+algorithm) {
			return challenge
		}
	}
	t.Fatalf("no %s Digest challenge in %#v", algorithm, challenges)
	return ""
}

func testDigestAuthorization(t *testing.T, challenge, algorithm, username, password, method, requestURI, nc, cnonce string) string {
	t.Helper()
	params, err := parseDigestParameters(strings.TrimPrefix(challenge, "Digest "))
	if err != nil {
		t.Fatalf("parse challenge: %v", err)
	}
	ha1 := testDigestHash(algorithm, username+":"+params["realm"]+":"+password)
	ha2 := testDigestHash(algorithm, method+":"+requestURI)
	response := testDigestHash(algorithm, ha1+":"+params["nonce"]+":"+nc+":"+cnonce+":auth:"+ha2)
	return fmt.Sprintf(`Digest username=%q, realm=%q, nonce=%q, uri=%q, response=%q, algorithm=%s, qop=auth, nc=%s, cnonce=%q, opaque=%q`,
		username, params["realm"], params["nonce"], requestURI, response, algorithm, nc, cnonce, params["opaque"])
}

func testDigestHash(algorithm, value string) string {
	if algorithm == "MD5" {
		sum := md5.Sum([]byte(value))
		return hex.EncodeToString(sum[:])
	}
	sum := sha256.Sum256([]byte(value))
	return hex.EncodeToString(sum[:])
}

func TestCookieSecureAndHelpers(t *testing.T) {
	service := &Service{cfg: testAuthConfig("https://calcard.example")}
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

func TestIdentityFromClaimsIncludesProfileNames(t *testing.T) {
	identity, err := identityFromClaims(userInfo{
		Subject:   "oauth-subject",
		Email:     "dana@example.com",
		FullName:  " Dana Lee ",
		FirstName: " Dana ",
	})
	if err != nil {
		t.Fatalf("identityFromClaims() error = %v", err)
	}
	if identity.Subject != "oauth-subject" || identity.Email != "dana@example.com" || identity.FullName != "Dana Lee" || identity.FirstName != "Dana" {
		t.Fatalf("identityFromClaims() = %#v", identity)
	}
}

func TestIdentityFromClaimsAllowsMissingOptionalNames(t *testing.T) {
	identity, err := identityFromClaims(userInfo{Subject: "oauth-subject", Email: "dana@example.com"})
	if err != nil {
		t.Fatalf("identityFromClaims() error = %v", err)
	}
	if identity.FullName != "" || identity.FirstName != "" {
		t.Fatalf("identityFromClaims() = %#v", identity)
	}
}

// A nonce signed before a restart has to keep verifying afterwards, and every
// replica has to accept every other replica's nonces. A signature that no longer
// verifies cannot be reported as stale, so the client is told to re-prompt for
// credentials rather than to retry -- which is what a process-local key would
// cause on every restart.
func TestDigestNoncesSurviveRestartAndSpanReplicas(t *testing.T) {
	cfg := testAuthConfig("https://calcard.example")
	issuer := &Service{cfg: cfg}
	replica := &Service{cfg: cfg}

	nonce, opaque, err := issuer.newDigestNonce()
	if err != nil {
		t.Fatalf("newDigestNonce() error = %v", err)
	}
	if valid, stale := replica.validateDigestNonce(nonce, opaque); !valid || stale {
		t.Fatalf("replica validateDigestNonce() = (%t, %t), want (true, false)", valid, stale)
	}

	// A different secret is a different deployment, and its nonces must not
	// verify here.
	foreign := &Service{cfg: testAuthConfig("https://calcard.example")}
	foreign.cfg.Session.Secret = "a-completely-different-session-secret-value"
	foreignNonce, foreignOpaque, err := foreign.newDigestNonce()
	if err != nil {
		t.Fatalf("newDigestNonce() error = %v", err)
	}
	if valid, _ := replica.validateDigestNonce(foreignNonce, foreignOpaque); valid {
		t.Fatal("a nonce signed under another session secret verified")
	}
}

// Without a configured secret there is no key to seal an HA1 under, so a new app
// password is created Basic-only rather than storing the credential in the clear.
func TestAppPasswordWithoutSessionSecretStoresNoDigestCredential(t *testing.T) {
	service := &Service{cfg: &config.Config{BaseURL: "https://calcard.example"}}

	md5HA1, sha256HA1, err := service.sealDigestCredentials("user@example.com", "app-secret")
	if err != nil {
		t.Fatalf("sealDigestCredentials() error = %v", err)
	}
	if md5HA1 != nil || sha256HA1 != nil {
		t.Fatalf("sealDigestCredentials() = %v, %v, want both nil", md5HA1, sha256HA1)
	}
}

// RFC 4791 section 11 conditions Basic on the transport the request arrived on.
// A base URL describes how the deployment is addressed, not how this request
// travelled, and once proxies are named, an X-Forwarded-Proto from any other
// peer is just a header the client chose to send. Naming none keeps the
// deployment-wide fail-open the configuration loader warns about.
func TestSecureRequestTrustsTLSAndForwardedProto(t *testing.T) {
	withProxies := func(proxies ...string) *config.Config {
		cfg := testAuthConfig("https://calcard.example")
		cfg.TrustedProxies = proxies
		return cfg
	}

	tests := []struct {
		name       string
		cfg        *config.Config
		tls        bool
		remoteAddr string
		forwarded  string
		want       bool
	}{
		{name: "direct TLS", cfg: testAuthConfig("http://calcard.example"), tls: true, want: true},
		{name: "https base URL alone is not evidence", cfg: testAuthConfig("https://calcard.example"), want: false},
		{
			name: "trusted proxy asserting https", cfg: withProxies("10.0.0.0/8"),
			remoteAddr: "10.1.2.3:5000", forwarded: "https", want: true,
		},
		{
			name: "trusted proxy asserting http", cfg: withProxies("10.0.0.0/8"),
			remoteAddr: "10.1.2.3:5000", forwarded: "http", want: false,
		},
		{
			name: "untrusted peer claiming https", cfg: withProxies("10.0.0.0/8"),
			remoteAddr: "203.0.113.9:5000", forwarded: "https", want: false,
		},
		{
			name: "no trusted proxies configured trusts any peer", cfg: testAuthConfig("https://calcard.example"),
			remoteAddr: "203.0.113.9:5000", forwarded: "https", want: true,
		},
		{
			name: "no trusted proxies configured still requires the assertion", cfg: testAuthConfig("https://calcard.example"),
			remoteAddr: "203.0.113.9:5000", forwarded: "http", want: false,
		},
		{
			name: "first hop of a forwarded chain decides", cfg: withProxies("10.0.0.0/8"),
			remoteAddr: "10.1.2.3:5000", forwarded: "https, http", want: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			req := httptest.NewRequest(http.MethodGet, "/dav/", nil)
			if tc.tls {
				req.TLS = &tls.ConnectionState{}
			}
			if tc.remoteAddr != "" {
				req.RemoteAddr = tc.remoteAddr
			}
			if tc.forwarded != "" {
				req.Header.Set("X-Forwarded-Proto", tc.forwarded)
			}
			if got := (&Service{cfg: tc.cfg}).secureRequest(req); got != tc.want {
				t.Fatalf("secureRequest() = %t, want %t", got, tc.want)
			}
			// The DAV href resolver reaches the same rule through the exported
			// entry point, so the two cannot be allowed to drift apart.
			if got := RequestIsSecure(req, tc.cfg.TrustedProxies); got != tc.want {
				t.Fatalf("RequestIsSecure() = %t, want %t", got, tc.want)
			}
		})
	}
}
