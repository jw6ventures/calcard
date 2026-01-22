package auth

import (
	"context"
	"crypto/rand"
	"encoding/base64"
	"net/http"
	"net/url"
	"time"

	"gitea.jw6.us/james/calcard/internal/config"
	"gitea.jw6.us/james/calcard/internal/store"
	"gitea.jw6.us/james/calcard/internal/util"
)

const (
	sessionCookieName = "calcard_session"
	sessionDuration   = 7 * 24 * time.Hour
)

type SessionManager struct {
	cfg    *config.Config
	store  *store.Store
	secure bool
}

func NewSessionManager(cfg *config.Config, st *store.Store) *SessionManager {
	secure := true
	if base, err := url.Parse(cfg.BaseURL); err == nil && base.Scheme != "https" {
		secure = false
	}

	return &SessionManager{
		cfg:    cfg,
		store:  st,
		secure: secure,
	}
}

func (m *SessionManager) Issue(ctx context.Context, w http.ResponseWriter, r *http.Request, userID int64) error {
	sessionID, err := generateSessionID()
	if err != nil {
		return err
	}

	var userAgent, ipAddress *string
	if ua := r.UserAgent(); ua != "" {
		userAgent = util.StrPtr(ua)
	}
	if ip := getClientIP(r); ip != "" {
		ipAddress = util.StrPtr(ip)
	}

	expiresAt := time.Now().Add(sessionDuration)

	_, err = m.store.Sessions.Create(ctx, store.Session{
		ID:        sessionID,
		UserID:    userID,
		UserAgent: userAgent,
		IPAddress: ipAddress,
		ExpiresAt: expiresAt,
	})
	if err != nil {
		return err
	}

	http.SetCookie(w, &http.Cookie{
		Name:     sessionCookieName,
		Value:    sessionID,
		Path:     "/",
		Expires:  expiresAt,
		HttpOnly: true,
		Secure:   m.secure,
		SameSite: http.SameSiteLaxMode,
	})
	return nil
}

func (m *SessionManager) Clear(ctx context.Context, w http.ResponseWriter, r *http.Request) {
	if c, err := r.Cookie(sessionCookieName); err == nil && c.Value != "" {
		_ = m.store.Sessions.Delete(ctx, c.Value)
	}

	http.SetCookie(w, &http.Cookie{
		Name:     sessionCookieName,
		Value:    "",
		Path:     "/",
		Expires:  time.Unix(0, 0),
		HttpOnly: true,
		Secure:   m.secure,
		SameSite: http.SameSiteLaxMode,
	})
}

func (m *SessionManager) GetSession(ctx context.Context, r *http.Request) (*store.Session, error) {
	c, err := r.Cookie(sessionCookieName)
	if err != nil {
		return nil, nil
	}
	if c.Value == "" {
		return nil, nil
	}

	session, err := m.store.Sessions.GetByID(ctx, c.Value)
	if err != nil {
		return nil, err
	}
	if session == nil {
		return nil, nil
	}

	go func() {
		_ = m.store.Sessions.TouchLastSeen(context.Background(), session.ID)
	}()

	return session, nil
}

func (m *SessionManager) CurrentUserID(ctx context.Context, r *http.Request) (int64, string, bool) {
	session, err := m.GetSession(ctx, r)
	if err != nil || session == nil {
		return 0, "", false
	}
	return session.UserID, session.ID, true
}

func generateSessionID() (string, error) {
	b := make([]byte, 32)
	if _, err := rand.Read(b); err != nil {
		return "", err
	}
	return base64.RawURLEncoding.EncodeToString(b), nil
}

func getClientIP(r *http.Request) string {
	if xff := r.Header.Get("X-Forwarded-For"); xff != "" {
		if idx := len(xff); idx > 0 {
			for i := 0; i < len(xff); i++ {
				if xff[i] == ',' {
					return xff[:i]
				}
			}
			return xff
		}
	}

	if xri := r.Header.Get("X-Real-IP"); xri != "" {
		return xri
	}

	return r.RemoteAddr
}
