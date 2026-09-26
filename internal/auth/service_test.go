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
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jw6ventures/calcard/internal/config"
	"github.com/jw6ventures/calcard/internal/http/clientip"
	"github.com/jw6ventures/calcard/internal/store"
	jw6_utils "github.com/jw6ventures/jw6-go-utils"
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
	createFn                   func(context.Context, store.AppPassword) (*store.AppPassword, error)
	findValidByUserFn          func(context.Context, int64) ([]store.AppPassword, error)
	touchLastUsedFn            func(context.Context, int64) error
	replaceDigestCredentialsFn func(ctx context.Context, id int64, newMD5, newSHA256, oldMD5, oldSHA256 *string) (bool, error)
}

func (m *appPasswordRepoMock) ReplaceDigestCredentials(ctx context.Context, id int64, newMD5, newSHA256, oldMD5, oldSHA256 *string) (bool, error) {
	if m.replaceDigestCredentialsFn != nil {
		return m.replaceDigestCredentialsFn(ctx, id, newMD5, newSHA256, oldMD5, oldSHA256)
	}
	return false, nil
}

func (m *appPasswordRepoMock) PurgeDigestCredentials(context.Context) (int64, error) { return 0, nil }

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

// digestNonceRepoMock is the shared record of spent nonce counts. Tests that
// stand up two Services point both at one instance, which is what the database
// is to two server processes.
type digestNonceRepoMock struct {
	mu        sync.Mutex
	consumed  map[string]struct{}
	consumeFn func(context.Context, int64, string, uint32, time.Time) (bool, error)
}

func (m *digestNonceRepoMock) Consume(ctx context.Context, tokenID int64, nonce string, nonceCount uint32, expiresAt time.Time) (bool, error) {
	if m.consumeFn != nil {
		return m.consumeFn(ctx, tokenID, nonce, nonceCount, expiresAt)
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.consumed == nil {
		m.consumed = make(map[string]struct{})
	}
	key := strconv.FormatInt(tokenID, 10) + "\x00" + nonce + "\x00" + strconv.FormatUint(uint64(nonceCount), 10)
	if _, duplicate := m.consumed[key]; duplicate {
		return false, nil
	}
	m.consumed[key] = struct{}{}
	return true, nil
}

func (m *digestNonceRepoMock) DeleteExpired(context.Context) (int64, error) { return 0, nil }

// digestTestStore is the store shape every Digest test needs: the user, the
// tokens, and the shared nonce-count record the replay guard now consults.
func digestTestStore(nonces store.DigestNonceRepository, tokens func(context.Context, int64) ([]store.AppPassword, error)) *store.Store {
	return &store.Store{
		Users: &userRepoMock{getByEmailFn: func(_ context.Context, email string) (*store.User, error) {
			return &store.User{ID: 7, PrimaryEmail: email}, nil
		}},
		AppPasswords: &appPasswordRepoMock{findValidByUserFn: tokens},
		DigestNonces: nonces,
	}
}

// digestCredentialRow is one app_passwords row, answering reads and applying
// the compare-and-swap ReplaceDigestCredentials performs in PostgreSQL.
type digestCredentialRow struct {
	t     *testing.T
	token store.AppPassword
	swaps int
}

func (r *digestCredentialRow) find(context.Context, int64) ([]store.AppPassword, error) {
	return []store.AppPassword{r.token}, nil
}

func (r *digestCredentialRow) replace(_ context.Context, id int64, newMD5, newSHA256, oldMD5, oldSHA256 *string) (bool, error) {
	if id != r.token.ID {
		r.t.Errorf("ReplaceDigestCredentials id = %d, want %d", id, r.token.ID)
	}
	if !sameStoredHA1(r.token.DigestMD5HA1, oldMD5) || !sameStoredHA1(r.token.DigestSHA256HA1, oldSHA256) {
		return false, nil
	}
	r.token.DigestMD5HA1, r.token.DigestSHA256HA1 = newMD5, newSHA256
	r.swaps++
	return true, nil
}

func (r *digestCredentialRow) backingStore() *store.Store {
	return &store.Store{
		Users: &userRepoMock{getByEmailFn: func(_ context.Context, email string) (*store.User, error) {
			return &store.User{ID: 7, PrimaryEmail: email}, nil
		}},
		AppPasswords: &appPasswordRepoMock{findValidByUserFn: r.find, replaceDigestCredentialsFn: r.replace},
		DigestNonces: &digestNonceRepoMock{},
	}
}

func sameStoredHA1(a, b *string) bool {
	return a == nil && b == nil || a != nil && b != nil && *a == *b
}

// recordingLogSink captures what a Service logs, so a test can assert on it
// without swapping the process-wide standard logger.
type recordingLogSink struct {
	mu    sync.Mutex
	lines []string
}

func (s *recordingLogSink) Log(_, _ string, _ jw6_utils.LogLevel, message string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.lines = append(s.lines, message)
}

func (s *recordingLogSink) count(substring string) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	matches := 0
	for _, line := range s.lines {
		if strings.Contains(line, substring) {
			matches++
		}
	}
	return matches
}

func (s *recordingLogSink) String() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return strings.Join(s.lines, "\n")
}

func loggedService(cfg *config.Config) (*Service, *recordingLogSink) {
	sink := &recordingLogSink{}
	service := &Service{cfg: cfg}
	service.SetLogger(sink)
	return service, sink
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

// testDigestAuthConfig is testAuthConfig with the DAV Digest scheme opted into,
// which is what a deployment does once its app passwords carry HA1s. Digest
// tests go through this rather than the plain helper so the default the
// configuration loader actually applies stays visible in the tests that rely on
// it.
func testDigestAuthConfig(baseURL string) *config.Config {
	cfg := testAuthConfig(baseURL)
	cfg.DAV.DigestEnabled = true
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
		cfg: testDigestAuthConfig("https://calcard.example"),
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
		opened, err := decryptDigestHA1(service.cfg, tc.algorithm, *tc.stored)
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
		if _, err := decryptDigestHA1(service.cfg, other, *tc.stored); err == nil {
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
	service := &Service{cfg: testDigestAuthConfig("https://calcard.example")}
	md5HA1, sha256HA1 := sealedTestHA1s(t, service, username, password)
	service.store = digestTestStore(&digestNonceRepoMock{}, func(context.Context, int64) ([]store.AppPassword, error) {
		return []store.AppPassword{{ID: 3, DigestMD5HA1: md5HA1, DigestSHA256HA1: sha256HA1}}, nil
	})
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

// A Digest nonce is signed with a key derived from the configured session
// secret, so it verifies at every instance holding that secret and after a
// restart. The record of which nonce counts have been spent has to be shared the
// same way: with it held per process, a captured Authorization header replayed
// at a second instance -- or at the same one after a restart -- authenticated
// again for the whole nonce lifetime.
func TestDigestReplayIsRejectedAtASecondInstance(t *testing.T) {
	const (
		username = "user@example.com"
		password = "app-secret"
	)
	nonces := &digestNonceRepoMock{}
	newInstance := func() http.Handler {
		service := &Service{cfg: testDigestAuthConfig("https://calcard.example")}
		_, sha256HA1 := sealedTestHA1s(t, service, username, password)
		service.store = digestTestStore(nonces, func(context.Context, int64) ([]store.AppPassword, error) {
			return []store.AppPassword{{ID: 3, DigestSHA256HA1: sha256HA1}}, nil
		})
		return service.RequireDAVAuth(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			w.WriteHeader(http.StatusNoContent)
		}))
	}
	first, second := newInstance(), newInstance()

	challengeRec := httptest.NewRecorder()
	first.ServeHTTP(challengeRec, davRequest(http.MethodPut, "/dav/calendars/1/event.ics"))
	challenge := digestChallengeForAlgorithm(t, challengeRec.Header().Values("WWW-Authenticate"), "SHA-256")
	authorization := testDigestAuthorization(t, challenge, "SHA-256", username, password, http.MethodPut, "/dav/calendars/1/event.ics", "00000001", "cnonce")

	accepted := httptest.NewRecorder()
	acceptedReq := davRequest(http.MethodPut, "/dav/calendars/1/event.ics")
	acceptedReq.Header.Set("Authorization", authorization)
	first.ServeHTTP(accepted, acceptedReq)
	if accepted.Code != http.StatusNoContent {
		t.Fatalf("first instance status = %d, want 204: %s", accepted.Code, accepted.Body.String())
	}

	// The same bytes, at an instance that never saw the original request. The
	// nonce still verifies there, which is exactly why the count must not.
	replayed := httptest.NewRecorder()
	replayedReq := davRequest(http.MethodPut, "/dav/calendars/1/event.ics")
	replayedReq.Header.Set("Authorization", authorization)
	second.ServeHTTP(replayed, replayedReq)
	if replayed.Code != http.StatusUnauthorized {
		t.Fatalf("second instance accepted the replayed credentials: status = %d, want 401", replayed.Code)
	}
}

// The request shape a TLS-terminating proxy produces once the forwarded-address
// middleware has run: RemoteAddr carries the client, the recorded peer is the
// proxy, and the proxy describes its own leg as HTTPS. Basic has to be offered
// and accepted here -- an app password issued before Digest credentials existed
// has nothing else to authenticate with.
//
// internal/http asserts that the middleware produces exactly this shape; this
// asserts what the authenticator then does with it.
func TestRequireDAVAuthAcceptsBasicForwardedByATrustedProxy(t *testing.T) {
	const (
		username = "user@example.com"
		password = "app-secret"
	)
	hash, err := bcrypt.GenerateFromPassword([]byte(password), bcrypt.MinCost)
	if err != nil {
		t.Fatalf("GenerateFromPassword() error = %v", err)
	}
	cfg := testAuthConfig("https://calcard.example")
	cfg.TrustedProxies = []string{"10.0.0.0/8"}
	service := &Service{
		cfg: cfg,
		store: digestTestStore(&digestNonceRepoMock{}, func(context.Context, int64) ([]store.AppPassword, error) {
			return []store.AppPassword{{ID: 3, TokenHash: string(hash)}}, nil
		}),
	}
	handler := service.RequireDAVAuth(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusNoContent)
	}))

	proxied := func() *http.Request {
		req := httptest.NewRequest(http.MethodGet, "http://calcard.example/dav/", nil)
		req.RemoteAddr = "198.51.100.7:4567"
		req.Header.Set("X-Forwarded-Proto", "https")
		return req.WithContext(clientip.WithPeerAddr(req.Context(), "10.1.2.3:4567"))
	}

	challenge := httptest.NewRecorder()
	handler.ServeHTTP(challenge, proxied())
	offersBasic := false
	for _, value := range challenge.Header().Values("WWW-Authenticate") {
		if strings.HasPrefix(value, "Basic ") {
			offersBasic = true
		}
	}
	if !offersBasic {
		t.Fatalf("a trusted proxy's HTTPS request was not offered Basic: %#v", challenge.Header().Values("WWW-Authenticate"))
	}

	req := proxied()
	req.SetBasicAuth(username, password)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	if rec.Code != http.StatusNoContent {
		t.Fatalf("status = %d, want 204: %s", rec.Code, rec.Body.String())
	}
}

func TestRequireDAVAuthDoesNotOfferOrAcceptBasicOnCleartextTransport(t *testing.T) {
	service := &Service{cfg: testDigestAuthConfig("http://calcard.example")}
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

	// Cleartext refuses Basic and Digest is off by default, so this combination
	// leaves no scheme to name. The 401 stands -- it is the status the request
	// deserves -- and it carries no challenge rather than advertising something
	// the server would then refuse.
	noScheme := &Service{cfg: testAuthConfig("http://calcard.example")}
	noSchemeRec := httptest.NewRecorder()
	noScheme.RequireDAVAuth(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusNoContent)
	})).ServeHTTP(noSchemeRec, httptest.NewRequest(http.MethodGet, "http://calcard.example/dav/", nil))
	if noSchemeRec.Code != http.StatusUnauthorized {
		t.Fatalf("cleartext status with Digest disabled = %d, want 401", noSchemeRec.Code)
	}
	if got := noSchemeRec.Header().Values("WWW-Authenticate"); len(got) != 0 {
		t.Fatalf("cleartext challenges with Digest disabled = %#v, want none", got)
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

func TestDigestNonceCountAcceptsUniqueOutOfOrderValues(t *testing.T) {
	ctx := context.Background()
	service := &Service{
		digestNow: func() time.Time { return time.Unix(1_700_000_000, 0) },
		store:     &store.Store{DigestNonces: &digestNonceRepoMock{}},
	}

	if claimed, err := service.acceptDigestNonceCount(ctx, 7, "nonce", 2); !claimed || err != nil {
		t.Fatalf("first nonce count = (%v, %v), want (true, nil)", claimed, err)
	}
	if claimed, err := service.acceptDigestNonceCount(ctx, 7, "nonce", 1); !claimed || err != nil {
		t.Fatalf("unique out-of-order nonce count = (%v, %v), want (true, nil)", claimed, err)
	}
	if claimed, err := service.acceptDigestNonceCount(ctx, 7, "nonce", 2); claimed || err != nil {
		t.Fatalf("duplicate nonce count = (%v, %v), want (false, nil)", claimed, err)
	}
	if claimed, err := service.acceptDigestNonceCount(ctx, 7, "nonce", 1); claimed || err != nil {
		t.Fatalf("duplicate out-of-order nonce count = (%v, %v), want (false, nil)", claimed, err)
	}
}

// A store failure and a genuine replay must both refuse the claim -- this
// server cannot tell an unrecorded count apart from a replayed one when the
// store cannot answer -- but they have to be reported differently so a
// transient outage is not indistinguishable from an actual replay: only the
// store failure carries an error.
func TestDigestNonceCountRejectsWhenTheStoreFails(t *testing.T) {
	ctx := context.Background()
	now := func() time.Time { return time.Unix(1_700_000_000, 0) }

	failing := &Service{
		digestNow: now,
		store: &store.Store{DigestNonces: &digestNonceRepoMock{
			consumeFn: func(context.Context, int64, string, uint32, time.Time) (bool, error) {
				return false, errors.New("database unreachable")
			},
		}},
	}
	if claimed, err := failing.acceptDigestNonceCount(ctx, 7, "nonce", 1); claimed {
		t.Fatal("a failed replay-state write admitted the request")
	} else if err == nil {
		t.Fatal("a failed replay-state write was not reported as an error")
	}

	replaying := &Service{digestNow: now, store: &store.Store{DigestNonces: &digestNonceRepoMock{}}}
	if claimed, err := replaying.acceptDigestNonceCount(ctx, 7, "nonce", 1); !claimed || err != nil {
		t.Fatalf("first claim = (%v, %v), want (true, nil)", claimed, err)
	}
	if claimed, err := replaying.acceptDigestNonceCount(ctx, 7, "nonce", 1); claimed || err != nil {
		t.Fatalf("a genuine replay = (%v, %v), want (false, nil): a replay must not be reported as a store error", claimed, err)
	}
}

// A replay-guard store failure is not a credential problem, so the challenge
// must carry stale=true -- unlike a genuine replay -- so a well-behaved client
// retries with a fresh nonce instead of re-prompting the user for credentials
// that were never at fault.
func TestRequireDAVAuthDigestMarksStoreFailureStaleUnlikeAGenuineReplay(t *testing.T) {
	const (
		username = "user@example.com"
		password = "app-secret"
	)
	service := &Service{cfg: testDigestAuthConfig("https://calcard.example")}
	_, sha256HA1 := sealedTestHA1s(t, service, username, password)
	service.store = digestTestStore(&digestNonceRepoMock{
		consumeFn: func(context.Context, int64, string, uint32, time.Time) (bool, error) {
			return false, errors.New("database unreachable")
		},
	}, func(context.Context, int64) ([]store.AppPassword, error) {
		return []store.AppPassword{{ID: 3, DigestSHA256HA1: sha256HA1}}, nil
	})
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
	for _, value := range rec.Header().Values("WWW-Authenticate")[:2] {
		if !strings.Contains(value, "stale=true") {
			t.Fatalf("store-failure challenge = %q, want stale=true", value)
		}
	}
}

func TestAuthCacheClearUserRemovesOnlyThatUsersCredentials(t *testing.T) {
	service := &Service{}
	firstKey := authCacheKey("first@example.com", "first-secret")
	secondKey := authCacheKey("second@example.com", "second-secret")
	service.authCachePut(firstKey, &store.User{ID: 1}, 10, nil)
	service.authCachePut(secondKey, &store.User{ID: 2}, 20, nil)

	service.authCacheClearUser(1)

	if _, ok := service.authCacheGet(firstKey); ok {
		t.Fatal("cleared user's cached credential remains valid")
	}
	if _, ok := service.authCacheGet(secondKey); !ok {
		t.Fatal("clearing one user removed another user's cached credential")
	}
}

// A cache hit skips the database entirely, so its lifetime is the window in
// which a credential keeps working after it should have stopped. An entry may
// therefore never outlive the password it stands for.
func TestAuthCacheEntryNeverOutlivesThePasswordExpiry(t *testing.T) {
	const (
		username = "user@example.com"
		password = "app-secret"
	)
	hash, err := bcrypt.GenerateFromPassword([]byte(password), bcrypt.MinCost)
	if err != nil {
		t.Fatalf("GenerateFromPassword() error = %v", err)
	}
	// Inside the cache TTL, so a fixed 60-second entry would outlast it.
	expiresAt := time.Now().Add(5 * time.Second)
	service := &Service{
		cfg: testAuthConfig("https://calcard.example"),
		store: &store.Store{
			Users: &userRepoMock{getByEmailFn: func(_ context.Context, email string) (*store.User, error) {
				return &store.User{ID: 7, PrimaryEmail: email}, nil
			}},
			AppPasswords: &appPasswordRepoMock{
				findValidByUserFn: func(context.Context, int64) ([]store.AppPassword, error) {
					return []store.AppPassword{{ID: 3, TokenHash: string(hash), ExpiresAt: &expiresAt}}, nil
				},
			},
		},
	}

	if _, err := service.ValidateAppPassword(context.Background(), username, password); err != nil {
		t.Fatalf("ValidateAppPassword() before expiry error = %v", err)
	}
	entry, ok := service.authCache[authCacheKey(username, password)]
	if !ok {
		t.Fatal("a successful authentication cached nothing")
	}
	if entry.expiresAt.After(expiresAt) {
		t.Fatalf("cache entry expires at %s, after the password's own %s", entry.expiresAt, expiresAt)
	}
}

// The same thing seen from the request path: a credential authenticated just
// before it expires must not still be accepted after it has.
func TestExpiredAppPasswordIsRejectedEvenAfterACacheHit(t *testing.T) {
	const (
		username = "user@example.com"
		password = "app-secret"
	)
	hash, err := bcrypt.GenerateFromPassword([]byte(password), bcrypt.MinCost)
	if err != nil {
		t.Fatalf("GenerateFromPassword() error = %v", err)
	}
	expiresAt := time.Now().Add(20 * time.Millisecond)
	service := &Service{
		cfg: testAuthConfig("https://calcard.example"),
		store: &store.Store{
			Users: &userRepoMock{getByEmailFn: func(_ context.Context, email string) (*store.User, error) {
				return &store.User{ID: 7, PrimaryEmail: email}, nil
			}},
			AppPasswords: &appPasswordRepoMock{
				findValidByUserFn: func(context.Context, int64) ([]store.AppPassword, error) {
					return []store.AppPassword{{ID: 3, TokenHash: string(hash), ExpiresAt: &expiresAt}}, nil
				},
			},
		},
	}

	if _, err := service.ValidateAppPassword(context.Background(), username, password); err != nil {
		t.Fatalf("ValidateAppPassword() before expiry error = %v", err)
	}
	time.Sleep(50 * time.Millisecond)
	if _, err := service.ValidateAppPassword(context.Background(), username, password); err == nil {
		t.Fatal("an expired app password was still accepted from the cache")
	}
}

// Revocation is immediate on the instance that performed it. Leaving the cached
// entry behind would keep the credential working there for the rest of the TTL,
// which is exactly the window an administrator revoking a password is trying to
// close.
func TestRevokingAnAppPasswordClearsItsCachedCredential(t *testing.T) {
	service := &Service{}
	revokedKey := authCacheKey("user@example.com", "revoked-secret")
	keptKey := authCacheKey("user@example.com", "kept-secret")
	user := &store.User{ID: 1}
	service.authCachePut(revokedKey, user, 10, nil)
	service.authCachePut(keptKey, user, 11, nil)

	service.InvalidateAppPassword(10)

	if _, ok := service.authCacheGet(revokedKey); ok {
		t.Fatal("the revoked credential is still cached")
	}
	if _, ok := service.authCacheGet(keptKey); !ok {
		t.Fatal("revoking one app password dropped another one's cached credential")
	}
}

func TestRequireDAVAuthDigestRejectsBadRevokedExpiredAndMismatchedCredentials(t *testing.T) {
	const (
		username = "user@example.com"
		password = "app-secret"
	)
	now := time.Now()
	service := &Service{cfg: testDigestAuthConfig("https://calcard.example")}
	md5HA1, sha256HA1 := sealedTestHA1s(t, service, username, password)
	service.store = digestTestStore(&digestNonceRepoMock{}, func(context.Context, int64) ([]store.AppPassword, error) {
		return []store.AppPassword{
			{ID: 1, DigestSHA256HA1: sha256HA1, RevokedAt: &now},
			{ID: 2, DigestSHA256HA1: sha256HA1, ExpiresAt: ptrTime(now.Add(-time.Minute))},
			{ID: 3, DigestMD5HA1: md5HA1, DigestSHA256HA1: sha256HA1},
		}, nil
	})
	handler := service.RequireDAVAuth(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(http.StatusNoContent) }))
	challengeRec := httptest.NewRecorder()
	handler.ServeHTTP(challengeRec, httptest.NewRequest(http.MethodGet, "/dav/", nil))
	challenge := digestChallengeForAlgorithm(t, challengeRec.Header().Values("WWW-Authenticate"), "SHA-256")

	valid := testDigestAuthorization(t, challenge, "SHA-256", username, password, http.MethodGet, "/dav/", "00000001", "cnonce")

	// Each rejection below has to be attributable to the tampering it names, so
	// establish first that the untampered header authenticates. Without this the
	// subtests pass whenever nothing at all can authenticate.
	acceptedRec := httptest.NewRecorder()
	acceptedReq := httptest.NewRequest(http.MethodGet, "/dav/", nil)
	acceptedReq.Header.Set("Authorization", valid)
	handler.ServeHTTP(acceptedRec, acceptedReq)
	if acceptedRec.Code != http.StatusNoContent {
		t.Fatalf("untampered status = %d, want 204", acceptedRec.Code)
	}
	tamper := map[string]func(string) string{
		"bad response":        func(header string) string { return strings.Replace(header, `response="`, `response="00`, 1) },
		"different URI":       func(header string) string { return strings.Replace(header, `uri="/dav/"`, `uri="/dav/other"`, 1) },
		"malformed duplicate": func(header string) string { return header + `, username="attacker@example.com"` },
	}
	nonceCount := 1
	for name, corrupt := range tamper {
		nonceCount++
		t.Run(name, func(t *testing.T) {
			// A fresh nonce count per case: the replay guard would otherwise
			// reject the second request whatever its contents.
			fresh := testDigestAuthorization(t, challenge, "SHA-256", username, password, http.MethodGet, "/dav/",
				fmt.Sprintf("%08x", nonceCount), "cnonce")
			req := httptest.NewRequest(http.MethodGet, "/dav/", nil)
			req.Header.Set("Authorization", corrupt(fresh))
			rec := httptest.NewRecorder()
			handler.ServeHTTP(rec, req)
			if rec.Code != http.StatusUnauthorized {
				t.Fatalf("status = %d, want 401", rec.Code)
			}
		})
	}
}

// Revocation and expiry each have to be the reason the credential is refused.
// Every leg therefore authenticates the same sealed credential first and only
// then applies the flag under test: a Digest credential this server cannot read
// at all is refused for its own reasons, and a test built on one proves nothing
// about either flag.
func TestRequireDAVAuthDigestRejectsRevokedAndExpiredTokensInIsolation(t *testing.T) {
	clock := time.Date(2026, 8, 3, 12, 0, 0, 0, time.UTC)
	const (
		username = "user@example.com"
		password = "app-secret"
	)
	tests := []struct {
		name     string
		suppress func(*store.AppPassword)
	}{
		{name: "revoked", suppress: func(token *store.AppPassword) { token.RevokedAt = &clock }},
		{name: "expired", suppress: func(token *store.AppPassword) { token.ExpiresAt = &clock }},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			service := &Service{
				cfg:       testDigestAuthConfig("https://calcard.example"),
				digestNow: func() time.Time { return clock },
			}
			_, sha256HA1 := sealedTestHA1s(t, service, username, password)
			token := store.AppPassword{ID: 1, DigestSHA256HA1: sha256HA1}
			service.store = digestTestStore(&digestNonceRepoMock{}, func(context.Context, int64) ([]store.AppPassword, error) {
				return []store.AppPassword{token}, nil
			})
			handler := service.RequireDAVAuth(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				w.WriteHeader(http.StatusNoContent)
			}))
			authenticate := func(nonceCount string) int {
				challengeRec := httptest.NewRecorder()
				handler.ServeHTTP(challengeRec, httptest.NewRequest(http.MethodGet, "/dav/", nil))
				challenge := digestChallengeForAlgorithm(t, challengeRec.Header().Values("WWW-Authenticate"), "SHA-256")
				req := httptest.NewRequest(http.MethodGet, "/dav/", nil)
				req.Header.Set("Authorization", testDigestAuthorization(t, challenge, "SHA-256", username, password, http.MethodGet, "/dav/", nonceCount, "cnonce"))
				rec := httptest.NewRecorder()
				handler.ServeHTTP(rec, req)
				return rec.Code
			}

			if code := authenticate("00000001"); code != http.StatusNoContent {
				t.Fatalf("status before %s = %d, want 204", tt.name, code)
			}

			tt.suppress(&token)

			if code := authenticate("00000002"); code != http.StatusUnauthorized {
				t.Fatalf("status after %s = %d, want 401", tt.name, code)
			}
		})
	}
}

// Digest verifies against a stored HA1, and every app password issued before
// Digest was enabled has none -- a database read cannot produce one, because the
// password is only ever stored as a bcrypt hash. A successful Basic
// authentication is the one moment the server holds the plaintext again, so it
// is where the credential catches up.
func TestLegacyAppPasswordGainsDigestCredentialsOnBasicAuth(t *testing.T) {
	const (
		username = "user@example.com"
		password = "legacy-app-secret"
	)
	hash, err := bcrypt.GenerateFromPassword([]byte(password), bcrypt.MinCost)
	if err != nil {
		t.Fatalf("GenerateFromPassword() error = %v", err)
	}

	row := &digestCredentialRow{t: t, token: store.AppPassword{ID: 3, TokenHash: string(hash)}}
	service, logged := loggedService(testDigestAuthConfig("https://calcard.example"))
	service.store = row.backingStore()
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

	if row.token.DigestMD5HA1 == nil || row.token.DigestSHA256HA1 == nil {
		t.Fatal("Basic authentication did not backfill the Digest credentials")
	}
	// Sealed, never the bare HA1: the stored value authenticates its holder
	// without the password, which is the whole reason it is encrypted at rest.
	for algorithm, sealed := range map[string]*string{"MD5": row.token.DigestMD5HA1, "SHA-256": row.token.DigestSHA256HA1} {
		opened, err := decryptDigestHA1(service.cfg, algorithm, *sealed)
		if err != nil {
			t.Fatalf("decryptDigestHA1(%s) error = %v", algorithm, err)
		}
		// Hashed over the account's own address rather than whatever spelling
		// the Basic request carried, so it matches what issuing the password
		// would have produced and what the Digest lookup resolves.
		if want := digestHA1(algorithm, username, password); opened != want {
			t.Fatalf("stored %s HA1 = %q, want %q", algorithm, opened, want)
		}
	}

	challengeRec := httptest.NewRecorder()
	handler.ServeHTTP(challengeRec, davRequest(http.MethodGet, "/dav/"))
	challenge := digestChallengeForAlgorithm(t, challengeRec.Header().Values("WWW-Authenticate"), "SHA-256")
	digestReq := davRequest(http.MethodGet, "/dav/")
	digestReq.Header.Set("Authorization", testDigestAuthorization(t, challenge, "SHA-256", username, password, http.MethodGet, "/dav/", "00000001", "cnonce"))
	digestRec := httptest.NewRecorder()
	handler.ServeHTTP(digestRec, digestReq)
	if digestRec.Code != http.StatusNoContent {
		t.Fatalf("Digest status after backfill = %d, want 204", digestRec.Code)
	}

	if row.swaps != 1 {
		t.Fatalf("ReplaceDigestCredentials swaps = %d, want exactly 1", row.swaps)
	}
	// Filling an empty row is routine and not a repair.
	if logged.count("re-sealed") != 0 {
		t.Fatalf("a first backfill was reported as a re-seal:\n%s", logged)
	}
}

// A credential that already carries HA1s must not be rewritten: the write would
// be pointless on every request, and re-sealing is not free.
func TestAppPasswordWithDigestCredentialsIsNotRewrittenOnBasicAuth(t *testing.T) {
	const (
		username = "user@example.com"
		password = "app-secret"
	)
	hash, err := bcrypt.GenerateFromPassword([]byte(password), bcrypt.MinCost)
	if err != nil {
		t.Fatalf("GenerateFromPassword() error = %v", err)
	}
	service := &Service{cfg: testDigestAuthConfig("https://calcard.example")}
	md5HA1, sha256HA1 := sealedTestHA1s(t, service, username, password)
	row := &digestCredentialRow{t: t, token: store.AppPassword{ID: 3, TokenHash: string(hash), DigestMD5HA1: md5HA1, DigestSHA256HA1: sha256HA1}}
	service.store = row.backingStore()

	if _, err := service.ValidateAppPassword(context.Background(), username, password); err != nil {
		t.Fatalf("ValidateAppPassword() error = %v", err)
	}
	if row.swaps != 0 {
		t.Fatalf("ReplaceDigestCredentials swaps = %d for a credential that already has HA1s, want 0", row.swaps)
	}
}

// Without a session secret there is no key to seal an HA1 under. That is the
// state tooling and tests run in, and it must leave Basic working rather than
// fail the request or write something unreadable. Digest is on here so the
// setting is not what stops the write.
func TestBasicAuthWithoutSessionSecretStoresNoDigestCredential(t *testing.T) {
	const password = "app-secret"
	hash, err := bcrypt.GenerateFromPassword([]byte(password), bcrypt.MinCost)
	if err != nil {
		t.Fatalf("GenerateFromPassword() error = %v", err)
	}
	secretless := &config.Config{BaseURL: "https://calcard.example"}
	secretless.DAV.DigestEnabled = true
	row := &digestCredentialRow{t: t, token: store.AppPassword{ID: 3, TokenHash: string(hash)}}
	service := &Service{cfg: secretless, store: row.backingStore()}

	if _, err := service.ValidateAppPassword(context.Background(), "user@example.com", password); err != nil {
		t.Fatalf("ValidateAppPassword() error = %v", err)
	}
	if row.swaps != 0 {
		t.Fatalf("ReplaceDigestCredentials swaps = %d with no key to seal under, want 0", row.swaps)
	}
}

// A deployment that has not opted into Digest must never have an HA1 written:
// neither when an app password is issued nor when an existing one
// authenticates over Basic. Turning the setting on has to restore both writes,
// or the scheme is unusable.
func TestDigestCredentialsAreWrittenOnlyWhileDigestIsEnabled(t *testing.T) {
	const password = "app-secret"
	hash, err := bcrypt.GenerateFromPassword([]byte(password), bcrypt.MinCost)
	if err != nil {
		t.Fatalf("GenerateFromPassword() error = %v", err)
	}
	user := &store.User{ID: 9, PrimaryEmail: "user@example.com"}

	for _, tc := range []struct {
		name      string
		cfg       *config.Config
		wantWrite bool
	}{
		{name: "disabled", cfg: testAuthConfig("https://calcard.example")},
		{name: "enabled", cfg: testDigestAuthConfig("https://calcard.example"), wantWrite: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var issued store.AppPassword
			row := &digestCredentialRow{t: t, token: store.AppPassword{ID: 3, TokenHash: string(hash)}}
			service := &Service{
				cfg: tc.cfg,
				store: &store.Store{
					Users: &userRepoMock{
						getByIDFn:    func(_ context.Context, id int64) (*store.User, error) { return user, nil },
						getByEmailFn: func(context.Context, string) (*store.User, error) { return user, nil },
					},
					AppPasswords: &appPasswordRepoMock{
						createFn: func(_ context.Context, token store.AppPassword) (*store.AppPassword, error) {
							token.ID = 77
							issued = token
							return &token, nil
						},
						findValidByUserFn:          row.find,
						replaceDigestCredentialsFn: row.replace,
					},
				},
			}

			if _, _, err := service.CreateAppPassword(context.Background(), user.ID, "laptop", nil); err != nil {
				t.Fatalf("CreateAppPassword() error = %v", err)
			}
			if got := issued.DigestMD5HA1 != nil || issued.DigestSHA256HA1 != nil; got != tc.wantWrite {
				t.Fatalf("issued app password carries HA1s = %v, want %v (md5=%v sha256=%v)", got, tc.wantWrite, issued.DigestMD5HA1, issued.DigestSHA256HA1)
			}

			if _, err := service.ValidateAppPassword(context.Background(), user.PrimaryEmail, password); err != nil {
				t.Fatalf("ValidateAppPassword() error = %v", err)
			}
			if got := row.token.DigestMD5HA1 != nil || row.token.DigestSHA256HA1 != nil; got != tc.wantWrite {
				t.Fatalf("Basic authentication backfilled HA1s = %v, want %v", got, tc.wantWrite)
			}
		})
	}
}

// With Digest off, an HA1 an earlier run stored is cleared the next time its
// app password authenticates over Basic, so an active credential does not wait
// on the periodic purge -- and Basic keeps working throughout.
func TestBasicAuthWithDigestDisabledClearsStoredDigestCredentials(t *testing.T) {
	const (
		username = "user@example.com"
		password = "app-secret"
	)
	hash, err := bcrypt.GenerateFromPassword([]byte(password), bcrypt.MinCost)
	if err != nil {
		t.Fatalf("GenerateFromPassword() error = %v", err)
	}
	md5HA1, sha256HA1 := sealedTestHA1s(t, &Service{cfg: testDigestAuthConfig("https://calcard.example")}, username, password)
	row := &digestCredentialRow{t: t, token: store.AppPassword{ID: 3, TokenHash: string(hash), DigestMD5HA1: md5HA1, DigestSHA256HA1: sha256HA1}}
	service, logged := loggedService(testAuthConfig("https://calcard.example"))
	service.store = row.backingStore()

	if _, err := service.ValidateAppPassword(context.Background(), username, password); err != nil {
		t.Fatalf("ValidateAppPassword() error = %v", err)
	}
	if row.token.DigestMD5HA1 != nil || row.token.DigestSHA256HA1 != nil {
		t.Fatalf("stored HA1s survived Basic authentication with Digest off: md5=%v sha256=%v", row.token.DigestMD5HA1, row.token.DigestSHA256HA1)
	}
	if row.swaps != 1 {
		t.Fatalf("ReplaceDigestCredentials swaps = %d, want 1", row.swaps)
	}
	if logged.count("cleared") != 1 {
		t.Fatalf("clearing was not reported exactly once:\n%s", logged)
	}
}

// A stored HA1 that no longer opens -- the state every app password is left in
// when APP_SESSION_SECRET is rotated -- cannot verify anything, so the row is
// re-sealed from the password a Basic authentication supplies.
func TestBasicAuthReplacesDigestCredentialsSealedUnderARotatedSecret(t *testing.T) {
	const (
		username = "user@example.com"
		password = "app-secret"
	)
	hash, err := bcrypt.GenerateFromPassword([]byte(password), bcrypt.MinCost)
	if err != nil {
		t.Fatalf("GenerateFromPassword() error = %v", err)
	}
	previous := &Service{cfg: testDigestAuthConfig("https://calcard.example")}
	previous.cfg.Session.Secret = "the-previous-session-secret-at-least-32-bytes"
	staleMD5, staleSHA256 := sealedTestHA1s(t, previous, username, password)

	row := &digestCredentialRow{t: t, token: store.AppPassword{ID: 3, TokenHash: string(hash), DigestMD5HA1: staleMD5, DigestSHA256HA1: staleSHA256}}
	service, logged := loggedService(testDigestAuthConfig("https://calcard.example"))
	service.store = row.backingStore()

	if _, err := service.ValidateAppPassword(context.Background(), username, password); err != nil {
		t.Fatalf("ValidateAppPassword() error = %v", err)
	}
	if row.swaps != 1 {
		t.Fatalf("ReplaceDigestCredentials swaps = %d, want exactly 1", row.swaps)
	}
	for algorithm, sealed := range map[string]*string{"MD5": row.token.DigestMD5HA1, "SHA-256": row.token.DigestSHA256HA1} {
		opened, err := decryptDigestHA1(service.cfg, algorithm, *sealed)
		if err != nil {
			t.Fatalf("decryptDigestHA1(%s) error = %v", algorithm, err)
		}
		if want := digestHA1(algorithm, username, password); opened != want {
			t.Fatalf("re-sealed %s HA1 = %q, want %q", algorithm, opened, want)
		}
	}
	if logged.count("re-sealed") != 1 {
		t.Fatalf("the re-seal was not reported exactly once:\n%s", logged)
	}

	// The repaired credential answers a Digest challenge, which is what the
	// rotation had silently taken away.
	handler := service.RequireDAVAuth(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusNoContent)
	}))
	challengeRec := httptest.NewRecorder()
	handler.ServeHTTP(challengeRec, davRequest(http.MethodGet, "/dav/"))
	digestReq := davRequest(http.MethodGet, "/dav/")
	digestReq.Header.Set("Authorization", testDigestAuthorization(t,
		digestChallengeForAlgorithm(t, challengeRec.Header().Values("WWW-Authenticate"), "SHA-256"),
		"SHA-256", username, password, http.MethodGet, "/dav/", "00000001", "cnonce"))
	digestRec := httptest.NewRecorder()
	handler.ServeHTTP(digestRec, digestReq)
	if digestRec.Code != http.StatusNoContent {
		t.Fatalf("Digest status after repair = %d, want 204", digestRec.Code)
	}
}

// A row holding only one of the two HA1s cannot answer every Digest challenge,
// so it is re-sealed as a whole, and the swap compares against the partial
// state it actually read.
func TestBasicAuthReSealsAPartialDigestCredential(t *testing.T) {
	const (
		username = "user@example.com"
		password = "app-secret"
	)
	hash, err := bcrypt.GenerateFromPassword([]byte(password), bcrypt.MinCost)
	if err != nil {
		t.Fatalf("GenerateFromPassword() error = %v", err)
	}
	service, logged := loggedService(testDigestAuthConfig("https://calcard.example"))
	_, sha256HA1 := sealedTestHA1s(t, service, username, password)
	row := &digestCredentialRow{t: t, token: store.AppPassword{ID: 3, TokenHash: string(hash), DigestSHA256HA1: sha256HA1}}
	service.store = row.backingStore()

	if _, err := service.ValidateAppPassword(context.Background(), username, password); err != nil {
		t.Fatalf("ValidateAppPassword() error = %v", err)
	}
	if row.swaps != 1 {
		t.Fatalf("ReplaceDigestCredentials swaps = %d, want 1", row.swaps)
	}
	if !DigestReady(service.cfg, row.token) {
		t.Fatalf("partial row was not re-sealed into a Digest-ready one: md5=%v sha256=%v", row.token.DigestMD5HA1, row.token.DigestSHA256HA1)
	}
	if logged.count("re-sealed") != 1 {
		t.Fatalf("the re-seal was not reported exactly once:\n%s", logged)
	}
}

// Two requests can read the same unreadable row. When another has already
// replaced it by the time this one writes, the compare-and-swap loses: the
// concurrent value stands, and nothing claims this request re-sealed it.
func TestReSealThatLosesTheCompareAndSwapKeepsTheConcurrentValue(t *testing.T) {
	const (
		username = "user@example.com"
		password = "app-secret"
	)
	hash, err := bcrypt.GenerateFromPassword([]byte(password), bcrypt.MinCost)
	if err != nil {
		t.Fatalf("GenerateFromPassword() error = %v", err)
	}
	previous := &Service{cfg: testDigestAuthConfig("https://calcard.example")}
	previous.cfg.Session.Secret = "the-previous-session-secret-at-least-32-bytes"
	staleMD5, staleSHA256 := sealedTestHA1s(t, previous, username, password)

	service, logged := loggedService(testDigestAuthConfig("https://calcard.example"))
	concurrentMD5, concurrentSHA256 := sealedTestHA1s(t, service, username, password)
	row := &digestCredentialRow{t: t, token: store.AppPassword{ID: 3, TokenHash: string(hash), DigestMD5HA1: staleMD5, DigestSHA256HA1: staleSHA256}}
	service.store = row.backingStore()
	service.store.AppPasswords.(*appPasswordRepoMock).replaceDigestCredentialsFn = func(ctx context.Context, id int64, newMD5, newSHA256, oldMD5, oldSHA256 *string) (bool, error) {
		row.token.DigestMD5HA1, row.token.DigestSHA256HA1 = concurrentMD5, concurrentSHA256
		return row.replace(ctx, id, newMD5, newSHA256, oldMD5, oldSHA256)
	}

	if _, err := service.ValidateAppPassword(context.Background(), username, password); err != nil {
		t.Fatalf("ValidateAppPassword() error = %v", err)
	}
	if row.swaps != 0 {
		t.Fatalf("ReplaceDigestCredentials swaps = %d against a row that changed underneath it, want 0", row.swaps)
	}
	if row.token.DigestMD5HA1 != concurrentMD5 || row.token.DigestSHA256HA1 != concurrentSHA256 {
		t.Fatal("the losing re-seal overwrote the concurrently stored credentials")
	}
	if logged.count("re-sealed") != 0 {
		t.Fatalf("a re-seal that lost the compare-and-swap was reported as done:\n%s", logged)
	}
}

// DigestReady backs the readiness badge on /app-passwords. It answers what the
// credential can actually do right now, so it stays false while the scheme is
// off and false for a value this server can no longer open.
func TestDigestReadyRequiresTheSettingAndAReadableCredential(t *testing.T) {
	enabled := &Service{cfg: testDigestAuthConfig("https://calcard.example")}
	md5HA1, sha256HA1 := sealedTestHA1s(t, enabled, "user@example.com", "app-secret")
	unreadable := "v1:" + strings.Repeat("A", 64)

	for _, tc := range []struct {
		name  string
		cfg   *config.Config
		token store.AppPassword
		want  bool
	}{
		{name: "sealed and enabled", cfg: enabled.cfg, token: store.AppPassword{DigestMD5HA1: md5HA1, DigestSHA256HA1: sha256HA1}, want: true},
		{name: "sealed but disabled", cfg: testAuthConfig("https://calcard.example"), token: store.AppPassword{DigestMD5HA1: md5HA1, DigestSHA256HA1: sha256HA1}},
		{name: "never backfilled", cfg: enabled.cfg, token: store.AppPassword{}},
		{name: "one column only", cfg: enabled.cfg, token: store.AppPassword{DigestSHA256HA1: sha256HA1}},
		{name: "unreadable after a secret rotation", cfg: enabled.cfg, token: store.AppPassword{DigestMD5HA1: &unreadable, DigestSHA256HA1: &unreadable}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := DigestReady(tc.cfg, tc.token); got != tc.want {
				t.Fatalf("DigestReady() = %v, want %v", got, tc.want)
			}
		})
	}
}

// The scheme is opt-in, so until a deployment turns it on the challenge must
// not name it and an Authorization header offering it must not be honoured --
// otherwise an upgrade hands a Digest challenge to a client holding a
// credential that cannot answer one.
func TestDAVDigestIsNeitherOfferedNorAcceptedUntilEnabled(t *testing.T) {
	const (
		username = "user@example.com"
		password = "app-secret"
	)
	hash, err := bcrypt.GenerateFromPassword([]byte(password), bcrypt.MinCost)
	if err != nil {
		t.Fatalf("GenerateFromPassword() error = %v", err)
	}
	enabled := &Service{
		cfg: testDigestAuthConfig("https://calcard.example"),
		store: digestTestStore(&digestNonceRepoMock{}, func(context.Context, int64) ([]store.AppPassword, error) {
			return nil, nil
		}),
	}
	enabledChallenge := httptest.NewRecorder()
	enabled.RequireDAVAuth(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {})).
		ServeHTTP(enabledChallenge, davRequest(http.MethodGet, "/dav/"))
	// Borrowed from the enabled server so the disabled one below is refusing a
	// challenge it would otherwise have signed itself.
	challenge := digestChallengeForAlgorithm(t, enabledChallenge.Header().Values("WWW-Authenticate"), "SHA-256")

	md5HA1, sha256HA1, err := enabled.sealDigestCredentials(username, password)
	if err != nil {
		t.Fatalf("sealDigestCredentials() error = %v", err)
	}
	disabled := &Service{
		cfg: testAuthConfig("https://calcard.example"),
		store: digestTestStore(&digestNonceRepoMock{}, func(context.Context, int64) ([]store.AppPassword, error) {
			return []store.AppPassword{{ID: 3, TokenHash: string(hash), DigestMD5HA1: md5HA1, DigestSHA256HA1: sha256HA1}}, nil
		}),
	}
	handler := disabled.RequireDAVAuth(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusNoContent)
	}))

	challengeRec := httptest.NewRecorder()
	handler.ServeHTTP(challengeRec, davRequest(http.MethodGet, "/dav/"))
	offered := challengeRec.Header().Values("WWW-Authenticate")
	if len(offered) != 1 || !strings.HasPrefix(offered[0], "Basic ") {
		t.Fatalf("challenge while disabled = %#v, want Basic alone", offered)
	}

	// A credential that would verify, refused because the scheme is off.
	digestReq := davRequest(http.MethodGet, "/dav/")
	digestReq.Header.Set("Authorization", testDigestAuthorization(t, challenge, "SHA-256", username, password, http.MethodGet, "/dav/", "00000001", "cnonce"))
	digestRec := httptest.NewRecorder()
	handler.ServeHTTP(digestRec, digestReq)
	if digestRec.Code != http.StatusUnauthorized {
		t.Fatalf("Digest status while disabled = %d, want 401", digestRec.Code)
	}

	// The credential the upgrade must keep working.
	basicReq := davRequest(http.MethodGet, "/dav/")
	basicReq.SetBasicAuth(username, password)
	basicRec := httptest.NewRecorder()
	handler.ServeHTTP(basicRec, basicReq)
	if basicRec.Code != http.StatusNoContent {
		t.Fatalf("Basic status while Digest is disabled = %d, want 204", basicRec.Code)
	}
}

func TestRequireDAVAuthDigestMarksExpiredSignedNonceStale(t *testing.T) {
	clock := time.Date(2026, 8, 2, 12, 0, 0, 0, time.UTC)
	const (
		username = "user@example.com"
		password = "app-secret"
	)
	service := &Service{
		cfg:       testDigestAuthConfig("https://calcard.example"),
		digestNow: func() time.Time { return clock },
	}
	_, sha256HA1 := sealedTestHA1s(t, service, username, password)
	service.store = digestTestStore(&digestNonceRepoMock{}, func(context.Context, int64) ([]store.AppPassword, error) {
		return []store.AppPassword{{ID: 3, DigestSHA256HA1: sha256HA1}}, nil
	})
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
	cfg := testDigestAuthConfig("https://calcard.example")
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
	foreign := &Service{cfg: testDigestAuthConfig("https://calcard.example")}
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

// secureRequest sits on the DAV auth hot path, so it must not re-parse
// APP_TRUSTED_PROXIES on every call the way the exported, per-call
// RequestIsSecure necessarily does. This compares the two paths' allocation
// rates rather than asserting zero, because parsing the request's own remote
// address is unavoidable per-call work; only the trusted-proxy CIDR parsing
// is supposed to be shared across calls.
func TestSecureRequestCachesTrustedProxiesInsteadOfReparsingPerRequest(t *testing.T) {
	trustedProxies := []string{"10.0.0.0/8", "192.168.0.0/16", "2001:db8::/32"}
	cfg := testAuthConfig("https://calcard.example")
	cfg.TrustedProxies = trustedProxies
	service := &Service{cfg: cfg}

	req := httptest.NewRequest(http.MethodGet, "/dav/", nil)
	req.RemoteAddr = "10.1.2.3:4567"
	req.Header.Set("X-Forwarded-Proto", "https")

	if !service.secureRequest(req) {
		t.Fatal("expected a trusted proxy's forwarded HTTPS request to be secure")
	}

	cachedAllocs := testing.AllocsPerRun(200, func() { service.secureRequest(req) })
	reparsedAllocs := testing.AllocsPerRun(200, func() { RequestIsSecure(req, trustedProxies) })

	if cachedAllocs >= reparsedAllocs {
		t.Fatalf("secureRequest() allocated %.1f/call, RequestIsSecure() allocated %.1f/call: "+
			"the Service path should reuse a cached trusted-proxy set instead of re-parsing the CIDRs on every request",
			cachedAllocs, reparsedAllocs)
	}
}

// The re-seal is reported only once the replacement is stored. A line claiming
// success ahead of a write that then fails would tell an operator the rotation
// was repaired while the credential still cannot answer Digest.
func TestFailedReSealIsNotReportedAsDone(t *testing.T) {
	const (
		username = "user@example.com"
		password = "app-secret"
	)
	hash, err := bcrypt.GenerateFromPassword([]byte(password), bcrypt.MinCost)
	if err != nil {
		t.Fatalf("GenerateFromPassword() error = %v", err)
	}
	previous := &Service{cfg: testDigestAuthConfig("https://calcard.example")}
	previous.cfg.Session.Secret = "the-previous-session-secret-at-least-32-bytes"
	staleMD5, staleSHA256 := sealedTestHA1s(t, previous, username, password)

	attempts := 0
	service, logged := loggedService(testDigestAuthConfig("https://calcard.example"))
	service.store = digestTestStore(&digestNonceRepoMock{}, func(context.Context, int64) ([]store.AppPassword, error) {
		return []store.AppPassword{{ID: 3, TokenHash: string(hash), DigestMD5HA1: staleMD5, DigestSHA256HA1: staleSHA256}}, nil
	})
	service.store.AppPasswords.(*appPasswordRepoMock).replaceDigestCredentialsFn = func(context.Context, int64, *string, *string, *string, *string) (bool, error) {
		attempts++
		return false, errors.New("write failed")
	}

	if _, err := service.ValidateAppPassword(context.Background(), username, password); err != nil {
		t.Fatalf("ValidateAppPassword() error = %v", err)
	}
	if attempts != 1 {
		t.Fatalf("re-seal attempts = %d, want 1", attempts)
	}
	if logged.count("could not store") != 1 {
		t.Fatalf("the failed write was not reported:\n%s", logged)
	}
	if logged.count("re-sealed") != 0 {
		t.Fatalf("a failed re-seal was reported as done:\n%s", logged)
	}
}

// An unreadable stored HA1 is reached before the response is checked, so any
// client that can fetch a challenge can trigger the report. It must be written
// once per process however many requests hit it.
func TestUnreadableDigestCredentialIsReportedOnce(t *testing.T) {
	const (
		username = "user@example.com"
		password = "app-secret"
	)
	previous := &Service{cfg: testDigestAuthConfig("https://calcard.example")}
	previous.cfg.Session.Secret = "the-previous-session-secret-at-least-32-bytes"
	staleMD5, staleSHA256 := sealedTestHA1s(t, previous, username, password)

	service, logged := loggedService(testDigestAuthConfig("https://calcard.example"))
	service.store = digestTestStore(&digestNonceRepoMock{}, func(context.Context, int64) ([]store.AppPassword, error) {
		return []store.AppPassword{{ID: 3, DigestMD5HA1: staleMD5, DigestSHA256HA1: staleSHA256}}, nil
	})
	handler := service.RequireDAVAuth(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusNoContent)
	}))

	for attempt := 1; attempt <= 3; attempt++ {
		challengeRec := httptest.NewRecorder()
		handler.ServeHTTP(challengeRec, davRequest(http.MethodGet, "/dav/"))
		req := davRequest(http.MethodGet, "/dav/")
		req.Header.Set("Authorization", testDigestAuthorization(t,
			digestChallengeForAlgorithm(t, challengeRec.Header().Values("WWW-Authenticate"), "SHA-256"),
			"SHA-256", username, password, http.MethodGet, "/dav/", fmt.Sprintf("%08x", attempt), "cnonce"))
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, req)
		if rec.Code != http.StatusUnauthorized {
			t.Fatalf("Digest status with an unreadable credential = %d, want 401", rec.Code)
		}
	}
	if got := logged.count("could not be opened"); got != 1 {
		t.Fatalf("unreadable credential reported %d times, want 1:\n%s", got, logged)
	}
}

// The report tells an operator what to do. A missing secret and a rotated one
// need different answers, and the rotation remedy has to hold for deployments
// that serve DAV without TLS, where Basic -- and so the re-seal -- is refused.
func TestUnreadableDigestCredentialReportNamesTheCause(t *testing.T) {
	for _, tc := range []struct {
		name     string
		err      error
		want     []string
		unwanted []string
	}{
		{
			name:     "no secret configured",
			err:      fmt.Errorf("open: %w", errDigestHA1KeyUnavailable),
			want:     []string{"APP_SESSION_SECRET is not configured"},
			unwanted: []string{"rotated"},
		},
		{
			name: "secret rotated",
			err:  errors.New("cipher: message authentication failed"),
			want: []string{"rotated", "HTTPS", "reissued"},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			service, logged := loggedService(testDigestAuthConfig("https://calcard.example"))
			service.reportUnreadableDigestCredential("SHA-256", 3, tc.err)
			for _, want := range tc.want {
				if logged.count(want) != 1 {
					t.Errorf("report does not mention %q:\n%s", want, logged)
				}
			}
			for _, unwanted := range tc.unwanted {
				if logged.count(unwanted) != 0 {
					t.Errorf("report mentions %q:\n%s", unwanted, logged)
				}
			}
		})
	}
}

// Sealing, verification and the per-row readiness badge all need the HA1
// cipher; deriving it anew on each call repeats HKDF and the AES key schedule.
// It is reused for the same secret and never for a different one.
func TestDigestHA1AEADIsDerivedOncePerSecret(t *testing.T) {
	cfg := testDigestAuthConfig("https://calcard.example")
	first, err := digestHA1AEAD(cfg)
	if err != nil {
		t.Fatalf("digestHA1AEAD() error = %v", err)
	}
	again, err := digestHA1AEAD(cfg)
	if err != nil {
		t.Fatalf("digestHA1AEAD() error = %v", err)
	}
	if first != again {
		t.Fatal("the HA1 cipher was derived again for an unchanged secret")
	}

	rotated := testDigestAuthConfig("https://calcard.example")
	rotated.Session.Secret = "the-previous-session-secret-at-least-32-bytes"
	other, err := digestHA1AEAD(rotated)
	if err != nil {
		t.Fatalf("digestHA1AEAD() error = %v", err)
	}
	if other == first {
		t.Fatal("a different secret reused the cached HA1 cipher")
	}
	sealed, err := encryptDigestHA1(cfg, "MD5", "ha1")
	if err != nil {
		t.Fatalf("encryptDigestHA1() error = %v", err)
	}
	if _, err := decryptDigestHA1(rotated, "MD5", sealed); err == nil {
		t.Fatal("a value sealed under one secret opened under another")
	}
}
