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
	"time"

	"gitea.jw6.us/james/calcard/internal/config"
	"gitea.jw6.us/james/calcard/internal/store"
	"github.com/coreos/go-oidc/v3/oidc"
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

	oauthSubject, email, err := s.userIdentity(ctx, token)
	if err != nil {
		http.Error(w, "failed to fetch user identity", http.StatusBadRequest)
		return
	}

	user, err := s.store.Users.UpsertOAuthUser(ctx, oauthSubject, email)
	if err != nil {
		log.Printf("failed to persist user for subject %q: %v", oauthSubject, err)
		http.Error(w, "failed to persist user", http.StatusInternalServerError)
		return
	}

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

	hash, err := bcrypt.GenerateFromPassword([]byte(plaintext), bcrypt.DefaultCost)
	if err != nil {
		return "", nil, err
	}

	created, err := s.store.AppPasswords.Create(ctx, store.AppPassword{
		UserID:    userID,
		Label:     label,
		TokenHash: string(hash),
		ExpiresAt: expiresAt,
	})
	if err != nil {
		return "", nil, err
	}

	return plaintext, created, nil
}

func (s *Service) ValidateAppPassword(ctx context.Context, username, password string) (*store.User, error) {
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
			_ = s.store.AppPasswords.TouchLastUsed(ctx, t.ID)
			return user, nil
		}
	}

	return nil, errors.New("invalid app password")
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
		username, password, ok := r.BasicAuth()
		if !ok {
			w.Header().Set("WWW-Authenticate", "Basic realm=\"CalCard DAV\"")
			http.Error(w, "authentication required", http.StatusUnauthorized)
			return
		}
		if username == "" || password == "" {
			http.Error(w, "invalid credentials", http.StatusUnauthorized)
			return
		}

		ctx := r.Context()
		user, err := s.ValidateAppPassword(ctx, username, password)
		if err != nil {
			http.Error(w, "invalid credentials", http.StatusUnauthorized)
			return
		}

		ctx = WithUser(ctx, user)
		next.ServeHTTP(w, r.WithContext(ctx))
	})
}

func (s *Service) ClearSession(w http.ResponseWriter, r *http.Request) {
	s.sessions.Clear(r.Context(), w, r)
}

func (s *Service) cookieSecure(r *http.Request) bool {
	if r.TLS != nil {
		return true
	}
	if base, err := url.Parse(s.cfg.BaseURL); err == nil && base.Scheme == "https" {
		return true
	}
	return false
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
	Subject string `json:"sub"`
	Email   string `json:"email"`
}

func (s *Service) userIdentity(ctx context.Context, token *oauth2.Token) (string, string, error) {
	if s.provider != nil {
		info, err := s.provider.UserInfo(ctx, oauth2.StaticTokenSource(token))
		if err == nil {
			var claims userInfo
			if err := info.Claims(&claims); err == nil {
				if claims.Subject != "" && claims.Email != "" {
					return claims.Subject, claims.Email, nil
				}
			}
		}
	}

	rawIDToken, _ := token.Extra("id_token").(string)
	if rawIDToken == "" {
		return "", "", errors.New("no userinfo or id_token available")
	}

	if s.verifier == nil {
		return "", "", errors.New("id_token verification unavailable")
	}

	idToken, err := s.verifier.Verify(ctx, rawIDToken)
	if err != nil {
		return "", "", err
	}

	var claims userInfo
	if err := idToken.Claims(&claims); err != nil {
		return "", "", err
	}

	if claims.Subject == "" || claims.Email == "" {
		return "", "", errors.New("id_token missing subject or email")
	}

	return claims.Subject, claims.Email, nil
}
