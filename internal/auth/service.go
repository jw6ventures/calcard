package auth

import (
	"context"
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/coreos/go-oidc/v3/oidc"
	"github.com/jw6ventures/calcard/internal/config"
	"github.com/jw6ventures/calcard/internal/http/clientip"
	"github.com/jw6ventures/calcard/internal/store"
	"golang.org/x/crypto/bcrypt"
	"golang.org/x/oauth2"
)

// Service encapsulates authentication flows for OAuth and app passwords.
type Service struct {
	cfg      *config.Config
	store    *store.Store
	sessions *SessionManager
	oauthCfg *oauth2.Config
	userinfo string
	provider *oidc.Provider
	verifier *oidc.IDTokenVerifier

	authMu    sync.Mutex
	authCache map[string]authCacheEntry

	digestMu  sync.Mutex
	digestKey []byte
	digestNow func() time.Time

	trustedProxiesOnce sync.Once
	trustedProxies     clientip.TrustedProxies
}

func NewService(cfg *config.Config, st *store.Store, sessions *SessionManager) (*Service, error) {
	redirectURL := strings.TrimRight(cfg.BaseURL, "/") + cfg.OAuth.RedirectPath
	discoveryURL := cfg.OAuth.DiscoveryURL
	if discoveryURL == "" {
		discoveryURL = cfg.OAuth.IssuerURL
	}

	oidcConfig, err := discoverOIDC(discoveryURL)
	if err != nil {
		return nil, err
	}

	providerURL := cfg.OAuth.IssuerURL
	if providerURL == "" {
		providerURL = issuerFromDiscovery(discoveryURL)
	}

	provider, err := oidc.NewProvider(context.Background(), providerURL)
	if err != nil {
		return nil, err
	}

	verifier := provider.Verifier(&oidc.Config{ClientID: cfg.OAuth.ClientID})

	return &Service{cfg: cfg, store: st, sessions: sessions, userinfo: oidcConfig.UserinfoEndpoint, provider: provider, verifier: verifier, oauthCfg: &oauth2.Config{
		ClientID:     cfg.OAuth.ClientID,
		ClientSecret: cfg.OAuth.ClientSecret,
		RedirectURL:  redirectURL,
		Endpoint:     provider.Endpoint(),
		Scopes:       []string{"openid", "email", "profile"},
	}}, nil
}

func (s *Service) BeginOAuth(w http.ResponseWriter, r *http.Request) {
	state, err := randomState()
	if err != nil {
		http.Error(w, "failed to start login", http.StatusInternalServerError)
		return
	}
	secure := s.cookieSecure(r)
	http.SetCookie(w, &http.Cookie{
		Name:     "calcard_oauth_state",
		Value:    state,
		Path:     "/",
		HttpOnly: true,
		Secure:   secure,
		SameSite: http.SameSiteLaxMode,
		Expires:  time.Now().Add(10 * time.Minute),
	})

	url := s.oauthCfg.AuthCodeURL(state, oauth2.AccessTypeOnline)
	http.Redirect(w, r, url, http.StatusFound)
}

func (s *Service) HandleOAuthCallback(w http.ResponseWriter, r *http.Request) {
	stateCookie, err := r.Cookie("calcard_oauth_state")
	if err != nil || stateCookie.Value == "" || stateCookie.Value != r.URL.Query().Get("state") {
		http.Error(w, "invalid oauth state", http.StatusBadRequest)
		return
	}

	code := r.URL.Query().Get("code")
	if code == "" {
		http.Error(w, "missing oauth code", http.StatusBadRequest)
		return
	}

	ctx := r.Context()
	token, err := s.oauthCfg.Exchange(ctx, code)
	if err != nil {
		http.Error(w, "failed to exchange oauth code", http.StatusBadRequest)
		return
	}

	identity, err := s.userIdentity(ctx, token)
	if err != nil {
		http.Error(w, "failed to fetch user identity", http.StatusBadRequest)
		return
	}

	user, err := s.store.Users.UpsertOAuthUser(ctx, identity.Subject, identity.Email, identity.FullName, identity.FirstName)
	if err != nil {
		log.Printf("failed to persist user for subject %q: %v", identity.Subject, err)
		http.Error(w, "failed to persist user", http.StatusInternalServerError)
		return
	}
	s.authCacheClearUser(user.ID)

	if err := s.store.EnsureDefaultCollections(ctx, user.ID); err != nil {
		http.Error(w, "failed to bootstrap user", http.StatusInternalServerError)
		return
	}

	if err := s.sessions.Issue(ctx, w, r, user.ID); err != nil {
		http.Error(w, "failed to set session", http.StatusInternalServerError)
		return
	}

	http.Redirect(w, r, "/", http.StatusFound)
}

// CreateAppPassword generates a random token, hashes it, stores it, and returns the plaintext.
func (s *Service) CreateAppPassword(ctx context.Context, userID int64, label string, expiresAt *time.Time) (string, *store.AppPassword, error) {
	if label == "" {
		return "", nil, errors.New("label required")
	}

	buf := make([]byte, 32)
	if _, err := rand.Read(buf); err != nil {
		return "", nil, err
	}
	plaintext := base64.RawURLEncoding.EncodeToString(buf)
	user, err := s.store.Users.GetByID(ctx, userID)
	if err != nil {
		return "", nil, err
	}
	if user == nil || user.PrimaryEmail == "" {
		return "", nil, errors.New("user email required")
	}

	hash, err := bcrypt.GenerateFromPassword([]byte(plaintext), bcrypt.DefaultCost)
	if err != nil {
		return "", nil, err
	}

	// An HA1 authenticates its holder without the password, so it is sealed
	// under a key derived from the session secret rather than stored as the
	// bare hash Digest computes. Without a configured secret there is no key,
	// and the app password is created usable over Basic alone.
	md5HA1, sha256HA1, err := s.sealDigestCredentials(user.PrimaryEmail, plaintext)
	if err != nil {
		return "", nil, err
	}
	created, err := s.store.AppPasswords.Create(ctx, store.AppPassword{
		UserID:          userID,
		Label:           label,
		TokenHash:       string(hash),
		DigestMD5HA1:    md5HA1,
		DigestSHA256HA1: sha256HA1,
		ExpiresAt:       expiresAt,
	})
	if err != nil {
		return "", nil, err
	}

	return plaintext, created, nil
}

func (s *Service) ValidateAppPassword(ctx context.Context, username, password string) (*store.User, error) {
	// Fast path: DAV clients re-send Basic credentials on every request, so a
	// short-lived cache lets us skip the GetByEmail/FindValidByUser reads and,
	// crucially, the per-request bcrypt comparison (the dominant CPU cost).
	cacheKey := authCacheKey(username, password)
	if user, ok := s.authCacheGet(cacheKey); ok {
		return user, nil
	}

	user, err := s.store.Users.GetByEmail(ctx, username)
	if err != nil {
		return nil, err
	}
	if user == nil {
		return nil, errors.New("unknown user")
	}

	tokens, err := s.store.AppPasswords.FindValidByUser(ctx, user.ID)
	if err != nil {
		return nil, err
	}

	for _, t := range tokens {
		if t.RevokedAt != nil {
			continue
		}
		if t.ExpiresAt != nil && t.ExpiresAt.Before(time.Now()) {
			continue
		}
		if bcrypt.CompareHashAndPassword([]byte(t.TokenHash), []byte(password)) == nil {
			s.backfillDigestCredentials(ctx, user, t, password)
			s.touchLastUsedThrottled(t)
			s.authCachePut(cacheKey, user, t.ID, t.ExpiresAt)
			return user, nil
		}
	}

	return nil, errors.New("invalid app password")
}

// backfillDigestCredentials attaches Digest HA1s to an app password issued
// without them. An HA1 cannot be derived from the stored bcrypt hash, so a
// successful Basic authentication is the only moment the server can produce
// one for an existing credential; every app password predating Digest depends
// on this to become able to answer a Digest challenge.
//
// Every failure here is logged and swallowed. The password has already been
// verified, and refusing the request because a convenience write did not land
// would turn a working credential into a broken one -- the exact outcome this
// exists to prevent.
func (s *Service) backfillDigestCredentials(ctx context.Context, user *store.User, token store.AppPassword, password string) {
	if token.DigestMD5HA1 != nil || token.DigestSHA256HA1 != nil {
		return
	}
	if user == nil || user.PrimaryEmail == "" || s.store == nil || s.store.AppPasswords == nil {
		return
	}
	// Hashed over the account's own address rather than the spelling the
	// request carried, so the value matches what issuing the password would
	// have produced and what the Digest lookup resolves the username to.
	md5HA1, sha256HA1, err := s.sealDigestCredentials(user.PrimaryEmail, password)
	if err != nil {
		log.Printf("dav digest: could not seal credentials for app password %d: %v", token.ID, err)
		return
	}
	if md5HA1 == nil || sha256HA1 == nil {
		// No session secret, so there is no key and no HA1 to store. The app
		// password stays Basic-only, which is the state it was already in.
		return
	}
	if err := s.store.AppPasswords.SetDigestCredentials(ctx, token.ID, *md5HA1, *sha256HA1); err != nil {
		log.Printf("dav digest: could not store credentials for app password %d: %v", token.ID, err)
	}
}

func (s *Service) RequireSession(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()
		uid, sessionID, ok := s.sessions.CurrentUserID(ctx, r)
		if !ok {
			http.Redirect(w, r, "/auth/login", http.StatusFound)
			return
		}
		user, err := s.store.Users.GetByID(ctx, uid)
		if err != nil || user == nil {
			s.sessions.Clear(ctx, w, r)
			http.Redirect(w, r, "/auth/login", http.StatusFound)
			return
		}
		ctx = WithUser(ctx, user)
		ctx = WithSessionID(ctx, sessionID)
		next.ServeHTTP(w, r.WithContext(ctx))
	})
}

func (s *Service) RequireDAVAuth(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		authorization := strings.TrimSpace(r.Header.Get("Authorization"))
		scheme, _, _ := strings.Cut(authorization, " ")
		var (
			user  *store.User
			err   error
			stale bool
		)
		switch {
		case strings.EqualFold(scheme, "Basic"):
			if !s.secureRequest(r) {
				err = errors.New("basic authentication requires a secure transport")
				break
			}
			username, password, ok := r.BasicAuth()
			if !ok || username == "" || password == "" {
				err = errors.New("invalid credentials")
				break
			}
			user, err = s.ValidateAppPassword(r.Context(), username, password)
		case strings.EqualFold(scheme, "Digest"):
			if !s.digestEnabled() {
				err = errors.New("digest authentication is not enabled")
				break
			}
			user, stale, err = s.validateDAVDigest(r)
		default:
			err = errors.New("authentication required")
		}
		if err != nil || user == nil {
			if challengeErr := s.writeDAVAuthChallenge(w, r, stale); challengeErr != nil {
				http.Error(w, "failed to issue authentication challenge", http.StatusInternalServerError)
				return
			}
			http.Error(w, "invalid credentials", http.StatusUnauthorized)
			return
		}

		ctx := WithUser(r.Context(), user)
		next.ServeHTTP(w, r.WithContext(ctx))
	})
}

func (s *Service) ClearSession(w http.ResponseWriter, r *http.Request) {
	s.sessions.Clear(r.Context(), w, r)
}

// cookieSecure reports whether to mark cookies Secure. It is deliberately more
// willing than secureRequest: a site addressed over https should set the flag
// even on a request whose transport cannot be confirmed, because the cost of
// setting it needlessly is a cookie the browser withholds over cleartext, while
// the cost of omitting it is a cookie the browser sends there.
func (s *Service) cookieSecure(r *http.Request) bool {
	if s.secureRequest(r) {
		return true
	}
	if s == nil || s.cfg == nil {
		return false
	}
	base, err := url.Parse(s.cfg.BaseURL)
	return err == nil && base.Scheme == "https"
}

func (s *Service) secureRequest(r *http.Request) bool {
	if s == nil || s.cfg == nil {
		return r != nil && r.TLS != nil
	}
	return requestIsSecure(r, s.trustedProxySet())
}

// trustedProxySet returns the Service's parsed trusted-proxy set, built once
// and cached from then on -- this sits on the DAV auth hot path via
// secureRequest, which runs for every Basic-authenticated DAV request, and
// re-parsing the configured CIDRs on every call would allocate for no reason.
//
// Building it lazily here, rather than requiring callers to go through
// NewService, keeps a Service assembled as a bare struct literal -- as most of
// this package's tests do -- correct: the first call parses cfg.TrustedProxies
// on demand instead of silently behaving as if none were configured.
func (s *Service) trustedProxySet() clientip.TrustedProxies {
	s.trustedProxiesOnce.Do(func() {
		var configured []string
		if s.cfg != nil {
			configured = s.cfg.TrustedProxies
		}
		s.trustedProxies = clientip.NewTrustedProxies(configured)
	})
	return s.trustedProxies
}

// RequestIsSecure reports whether this request reached the server over TLS.
//
// A request forwarded by a proxy listed in trustedProxies (APP_TRUSTED_PROXIES)
// is trusted to describe its own leg through X-Forwarded-Proto; a request from
// anywhere else is not, so the header cannot be spoofed into unlocking Basic
// over cleartext. Configuring no proxies at all trusts every peer's
// X-Forwarded-Proto, matching how the forwarded client IP is resolved and the
// warning the configuration loader prints at startup.
//
// The peer is read through PeerAddr, not r.RemoteAddr: the forwarded-address
// middleware resolves the client into RemoteAddr, and the client is not the
// proxy this is asking about.
//
// It is exported because the DAV href resolver needs the same answer: a
// DAV:href naming an absolute URI is compared against the Request-URI scheme,
// and behind a TLS-terminating proxy only this rule can supply it.
func RequestIsSecure(r *http.Request, trustedProxies []string) bool {
	return requestIsSecure(r, clientip.NewTrustedProxies(trustedProxies))
}

func requestIsSecure(r *http.Request, trusted clientip.TrustedProxies) bool {
	if r == nil {
		return false
	}
	if r.TLS != nil {
		return true
	}
	if !trusted.AllowsPeer(clientip.PeerAddr(r)) {
		return false
	}
	proto, _, _ := strings.Cut(r.Header.Get("X-Forwarded-Proto"), ",")
	return strings.EqualFold(strings.TrimSpace(proto), "https")
}

func randomState() (string, error) {
	buf := make([]byte, 32)
	if _, err := rand.Read(buf); err != nil {
		return "", err
	}
	return base64.RawURLEncoding.EncodeToString(buf), nil
}

type oidcConfiguration struct {
	AuthorizationEndpoint string `json:"authorization_endpoint"`
	TokenEndpoint         string `json:"token_endpoint"`
	UserinfoEndpoint      string `json:"userinfo_endpoint"`
}

func issuerFromDiscovery(raw string) string {
	trimmed := strings.TrimRight(raw, "/")
	const suffix = "/.well-known/openid-configuration"
	if strings.HasSuffix(trimmed, suffix) {
		return strings.TrimSuffix(trimmed, suffix)
	}
	return trimmed
}

func discoverOIDC(issuerOrDiscovery string) (*oidcConfiguration, error) {
	trimmed := strings.TrimRight(issuerOrDiscovery, "/")
	wellKnown := "/.well-known/openid-configuration"
	configURL := trimmed
	if !strings.HasSuffix(trimmed, wellKnown) {
		configURL = trimmed + wellKnown
	}
	parsed, err := url.Parse(configURL)
	if err != nil {
		return nil, fmt.Errorf("failed to parse oidc discovery url: %w", err)
	}
	if parsed.Scheme != "https" && parsed.Scheme != "http" {
		return nil, errors.New("unsupported discovery url scheme")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, configURL, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to build oidc discovery request: %w", err)
	}

	client := &http.Client{Timeout: 5 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("failed to fetch oidc discovery: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("oidc discovery responded with %s", resp.Status)
	}

	var doc oidcConfiguration
	if err := json.NewDecoder(resp.Body).Decode(&doc); err != nil {
		return nil, fmt.Errorf("failed to decode oidc discovery: %w", err)
	}

	if doc.AuthorizationEndpoint == "" || doc.TokenEndpoint == "" {
		return nil, errors.New("oidc discovery missing required endpoints")
	}

	return &doc, nil
}

type userInfo struct {
	Subject   string `json:"sub"`
	Email     string `json:"email"`
	FullName  string `json:"name"`
	FirstName string `json:"given_name"`
}

type oauthIdentity struct {
	Subject   string
	Email     string
	FullName  string
	FirstName string
}

func identityFromClaims(claims userInfo) (oauthIdentity, error) {
	if claims.Subject == "" || claims.Email == "" {
		return oauthIdentity{}, errors.New("identity missing subject or email")
	}
	return oauthIdentity{
		Subject:   claims.Subject,
		Email:     claims.Email,
		FullName:  strings.TrimSpace(claims.FullName),
		FirstName: strings.TrimSpace(claims.FirstName),
	}, nil
}

func (s *Service) userIdentity(ctx context.Context, token *oauth2.Token) (oauthIdentity, error) {
	if s.provider != nil {
		info, err := s.provider.UserInfo(ctx, oauth2.StaticTokenSource(token))
		if err == nil {
			var claims userInfo
			if err := info.Claims(&claims); err == nil {
				if identity, err := identityFromClaims(claims); err == nil {
					return identity, nil
				}
			}
		}
	}

	rawIDToken, _ := token.Extra("id_token").(string)
	if rawIDToken == "" {
		return oauthIdentity{}, errors.New("no userinfo or id_token available")
	}

	if s.verifier == nil {
		return oauthIdentity{}, errors.New("id_token verification unavailable")
	}

	idToken, err := s.verifier.Verify(ctx, rawIDToken)
	if err != nil {
		return oauthIdentity{}, err
	}

	var claims userInfo
	if err := idToken.Claims(&claims); err != nil {
		return oauthIdentity{}, err
	}

	return identityFromClaims(claims)
}
