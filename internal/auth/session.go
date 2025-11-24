package auth

import (
	"net/http"
	"time"

	"github.com/example/calcard/internal/config"
)

// SessionManager manages web UI sessions.
type SessionManager struct {
	cfg *config.Config
	// TODO: back session store with signed cookies; consider rotating secrets.
}

func NewSessionManager(cfg *config.Config) *SessionManager {
	return &SessionManager{cfg: cfg}
}

// Issue sets a placeholder session cookie for a user.
func (m *SessionManager) Issue(w http.ResponseWriter, userID int64) error {
	// TODO: sign/encrypt cookie value, set HttpOnly/SameSite flags.
	http.SetCookie(w, &http.Cookie{
		Name:     "calcard_session",
		Value:    "todo-session-token",
		Path:     "/",
		Expires:  time.Now().Add(24 * time.Hour),
		HttpOnly: true,
		Secure:   true,
	})
	return nil
}

// Clear removes the session cookie.
func (m *SessionManager) Clear(w http.ResponseWriter) {
	http.SetCookie(w, &http.Cookie{
		Name:    "calcard_session",
		Value:   "",
		Path:    "/",
		Expires: time.Unix(0, 0),
	})
}

// CurrentUserID extracts the user ID from the request session if present.
func (m *SessionManager) CurrentUserID(r *http.Request) (int64, bool) {
	// TODO: verify signature, deserialize, handle expiry.
	return 0, false
}
