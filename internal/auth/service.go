package auth

import (
	"context"
	"errors"
	"net/http"
	"time"

	"github.com/example/calcard/internal/config"
	"github.com/example/calcard/internal/store"
)

// Service encapsulates authentication flows for OAuth and app passwords.
type Service struct {
	cfg      *config.Config
	store    *store.Store
	sessions *SessionManager
}

func NewService(cfg *config.Config, store *store.Store, sessions *SessionManager) *Service {
	return &Service{cfg: cfg, store: store, sessions: sessions}
}

// BeginOAuth starts the OAuth/OIDC authorization flow.
func (s *Service) BeginOAuth(w http.ResponseWriter, r *http.Request) {
	// TODO: build OAuth authorization URL with state nonce and redirect user.
	http.Error(w, "oauth flow not implemented", http.StatusNotImplemented)
}

// HandleOAuthCallback completes the OAuth flow and creates a session.
func (s *Service) HandleOAuthCallback(w http.ResponseWriter, r *http.Request) {
	// TODO: validate state, exchange code for token, extract subject/email.
	// TODO: persist user, ensure default calendar/address book, start session cookie.
	http.Error(w, "oauth callback not implemented", http.StatusNotImplemented)
}

// ValidateAppPassword verifies Basic Auth credentials for DAV clients.
func (s *Service) ValidateAppPassword(ctx context.Context, username, password string) (*store.User, error) {
	// TODO: look up user by username/email, check hashed token, update last_used_at.
	return nil, errors.New("app password validation not implemented")
}

// RequireSession retrieves the current user from a web session or redirects.
func (s *Service) RequireSession(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// TODO: load session cookie, fetch user, handle redirects to login.
		next.ServeHTTP(w, r)
	})
}

// RequireDAVAuth enforces Basic Auth for DAV endpoints.
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

		// TODO: add user to context for handlers to consume.
		next.ServeHTTP(w, r)
	})
}

// TODO: add CSRF protection middleware and helpers.
