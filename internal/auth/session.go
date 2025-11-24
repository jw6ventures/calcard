package auth

import (
	"net/http"
	"time"

	"github.com/example/calcard/internal/config"
	"github.com/gorilla/securecookie"
)

// SessionManager manages web UI sessions.
type SessionManager struct {
	cfg        *config.Config
	cookieName string
	codec      *securecookie.SecureCookie
}

func NewSessionManager(cfg *config.Config) *SessionManager {
	hashKey := []byte(cfg.Session.Secret)
	blockKey := hashKey
	sc := securecookie.New(hashKey, blockKey)
	sc.MaxAge(86400 * 7)
	sc.SetSerializer(securecookie.JSONEncoder{})

	return &SessionManager{
		cfg:        cfg,
		cookieName: "calcard_session",
		codec:      sc,
	}
}

// Issue sets a placeholder session cookie for a user.
func (m *SessionManager) Issue(w http.ResponseWriter, userID int64) error {
	value := map[string]any{
		"user_id": userID,
		"exp":     time.Now().Add(24 * time.Hour).Unix(),
	}

	encoded, err := m.codec.Encode(m.cookieName, value)
	if err != nil {
		return err
	}

	http.SetCookie(w, &http.Cookie{
		Name:     m.cookieName,
		Value:    encoded,
		Path:     "/",
		Expires:  time.Now().Add(24 * time.Hour),
		HttpOnly: true,
		Secure:   true,
		SameSite: http.SameSiteLaxMode,
	})
	return nil
}

// Clear removes the session cookie.
func (m *SessionManager) Clear(w http.ResponseWriter) {
	http.SetCookie(w, &http.Cookie{
		Name:    m.cookieName,
		Value:   "",
		Path:    "/",
		Expires: time.Unix(0, 0),
	})
}

// CurrentUserID extracts the user ID from the request session if present.
func (m *SessionManager) CurrentUserID(r *http.Request) (int64, bool) {
	c, err := r.Cookie(m.cookieName)
	if err != nil {
		return 0, false
	}

	var value map[string]any
	if err := m.codec.Decode(m.cookieName, c.Value, &value); err != nil {
		return 0, false
	}

	exp, ok := value["exp"].(float64)
	if !ok || time.Unix(int64(exp), 0).Before(time.Now()) {
		return 0, false
	}

	uid, ok := value["user_id"].(float64)
	if !ok {
		return 0, false
	}

	return int64(uid), true
}
