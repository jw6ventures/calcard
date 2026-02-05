package csrf

import (
	"context"
	"crypto/rand"
	"encoding/base64"
	"net/http"
	"net/url"

	"github.com/jw6ventures/calcard/internal/config"
)

type contextKey struct{}

const csrfCookieName = "calcard_csrf"

// Middleware issues a CSRF token cookie and validates it on mutating requests.
func Middleware(cfg *config.Config) func(http.Handler) http.Handler {
	secure := true
	if base, err := url.Parse(cfg.BaseURL); err == nil && base.Scheme != "https" {
		secure = false
	}

	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			token := ""
			if c, err := r.Cookie(csrfCookieName); err == nil {
				token = c.Value
			}
			if token == "" {
				var err error
				token, err = generateToken()
				if err != nil {
					http.Error(w, "failed to issue csrf token", http.StatusInternalServerError)
					return
				}
				http.SetCookie(w, &http.Cookie{
					Name:     csrfCookieName,
					Value:    token,
					Path:     "/",
					HttpOnly: true,
					Secure:   secure,
					SameSite: http.SameSiteLaxMode,
				})
			}

			if isStateChanging(r.Method) {
				provided := r.Header.Get("X-CSRF-Token")
				if provided == "" {
					provided = r.FormValue("_csrf")
				}
				if provided == "" || provided != token {
					http.Error(w, "invalid csrf token", http.StatusForbidden)
					return
				}
			}

			ctx := context.WithValue(r.Context(), contextKey{}, token)
			next.ServeHTTP(w, r.WithContext(ctx))
		})
	}
}

// TokenFromContext returns the CSRF token associated with the request.
func TokenFromContext(ctx context.Context) string {
	if v, ok := ctx.Value(contextKey{}).(string); ok {
		return v
	}
	return ""
}

func generateToken() (string, error) {
	buf := make([]byte, 32)
	if _, err := rand.Read(buf); err != nil {
		return "", err
	}
	return base64.RawURLEncoding.EncodeToString(buf), nil
}

func isStateChanging(method string) bool {
	switch method {
	case http.MethodPost, http.MethodPut, http.MethodPatch, http.MethodDelete:
		return true
	default:
		return false
	}
}
