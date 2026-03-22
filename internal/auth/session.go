package auth

import (
	"context"
	"crypto/rand"
	"encoding/base64"
	"net"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/jw6ventures/calcard/internal/config"
	"github.com/jw6ventures/calcard/internal/store"
	"github.com/jw6ventures/calcard/internal/util"
)

const (
	sessionCookieName = "calcard_session"
	sessionDuration   = 7 * 24 * time.Hour
)

type SessionManager struct {
	cfg            *config.Config
	store          *store.Store
	secure         bool
	trustedProxies []*net.IPNet
}

func NewSessionManager(cfg *config.Config, st *store.Store) *SessionManager {
	secure := true
	if base, err := url.Parse(cfg.BaseURL); err == nil && base.Scheme != "https" {
		secure = false
	}

	return &SessionManager{
		cfg:            cfg,
		store:          st,
		secure:         secure,
		trustedProxies: parseTrustedProxies(cfg.TrustedProxies),
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
	if ip := m.getClientIP(r); ip != "" {
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

func (m *SessionManager) getClientIP(r *http.Request) string {
	remoteIP, remoteHost := parseRemoteAddr(r.RemoteAddr)

	if len(m.trustedProxies) > 0 && !isTrustedProxy(remoteIP, m.trustedProxies) {
		if remoteIP != nil {
			return remoteIP.String()
		}
		return remoteHost
	}

	if xff := r.Header.Get("X-Forwarded-For"); xff != "" {
		if clientIP := forwardedClientIP(xff, m.trustedProxies); clientIP != nil {
			return clientIP.String()
		}
	}

	if xri := r.Header.Get("X-Real-IP"); xri != "" {
		if parsed := net.ParseIP(strings.TrimSpace(xri)); parsed != nil {
			return parsed.String()
		}
	}

	if remoteIP != nil {
		return remoteIP.String()
	}
	return remoteHost
}

func forwardedClientIP(xff string, trusted []*net.IPNet) net.IP {
	parts := strings.Split(xff, ",")

	if len(trusted) == 0 {
		for _, part := range parts {
			if parsed := net.ParseIP(strings.TrimSpace(part)); parsed != nil {
				return parsed
			}
		}
		return nil
	}

	for i := len(parts) - 1; i >= 0; i-- {
		candidate := strings.TrimSpace(parts[i])
		if candidate == "" {
			continue
		}
		parsed := net.ParseIP(candidate)
		if parsed == nil {
			continue
		}
		if !isTrustedProxy(parsed, trusted) {
			return parsed
		}
	}

	for _, part := range parts {
		if parsed := net.ParseIP(strings.TrimSpace(part)); parsed != nil {
			return parsed
		}
	}

	return nil
}

func parseTrustedProxies(values []string) []*net.IPNet {
	var trusted []*net.IPNet
	for _, raw := range values {
		value := strings.TrimSpace(raw)
		if value == "" {
			continue
		}
		_, ipnet, err := net.ParseCIDR(value)
		if err == nil {
			trusted = append(trusted, ipnet)
			continue
		}
		ip := net.ParseIP(value)
		if ip == nil {
			continue
		}
		suffix := "/128"
		if ip.To4() != nil {
			suffix = "/32"
		}
		_, ipnet, err = net.ParseCIDR(value + suffix)
		if err == nil {
			trusted = append(trusted, ipnet)
		}
	}
	return trusted
}

func isTrustedProxy(ip net.IP, trusted []*net.IPNet) bool {
	if ip == nil {
		return false
	}
	for _, ipnet := range trusted {
		if ipnet.Contains(ip) {
			return true
		}
	}
	return false
}

func parseRemoteAddr(remoteAddr string) (net.IP, string) {
	host, _, err := net.SplitHostPort(remoteAddr)
	if err == nil {
		if parsed := net.ParseIP(host); parsed != nil {
			return parsed, parsed.String()
		}
		return nil, host
	}

	trimmed := strings.TrimSpace(remoteAddr)
	if parsed := net.ParseIP(trimmed); parsed != nil {
		return parsed, parsed.String()
	}
	return nil, trimmed
}
