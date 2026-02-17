package ui

import (
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/jw6ventures/calcard/internal/auth"
	"github.com/jw6ventures/calcard/internal/store"
	"github.com/go-chi/chi/v5"
)

// Sessions displays the user's active sessions.
func (h *Handler) Sessions(w http.ResponseWriter, r *http.Request) {
	user, _ := auth.UserFromContext(r.Context())

	sessions, err := h.store.Sessions.ListByUser(r.Context(), user.ID)
	if err != nil {
		http.Error(w, "failed to load sessions", http.StatusInternalServerError)
		return
	}

	// Get current session ID from context to highlight it
	currentSessionID := auth.SessionIDFromContext(r.Context())

	var sessionData []map[string]any
	for _, s := range sessions {
		userAgent := ""
		if s.UserAgent != nil {
			userAgent = *s.UserAgent
		}
		ipAddress := ""
		if s.IPAddress != nil {
			ipAddress = *s.IPAddress
		}
		sessionData = append(sessionData, map[string]any{
			"ID":         s.ID,
			"UserAgent":  userAgent,
			"IPAddress":  ipAddress,
			"CreatedAt":  s.CreatedAt,
			"ExpiresAt":  s.ExpiresAt,
			"LastSeenAt": s.LastSeenAt,
			"IsCurrent":  s.ID == currentSessionID,
		})
	}

	data := h.withFlash(r, map[string]any{
		"Title":    "Active Sessions",
		"User":     user,
		"Sessions": sessionData,
	})
	h.render(w, r, "sessions.html", data)
}

// RevokeSession revokes a single session.
func (h *Handler) RevokeSession(w http.ResponseWriter, r *http.Request) {
	user, _ := auth.UserFromContext(r.Context())
	sessionID := chi.URLParam(r, "id")

	// Verify session belongs to user
	session, err := h.store.Sessions.GetByID(r.Context(), sessionID)
	if err != nil {
		http.Error(w, "failed to load session", http.StatusInternalServerError)
		return
	}
	if session == nil || session.UserID != user.ID {
		http.Error(w, "not found", http.StatusNotFound)
		return
	}

	if err := h.store.Sessions.Delete(r.Context(), sessionID); err != nil {
		http.Error(w, "failed to revoke session", http.StatusInternalServerError)
		return
	}

	h.redirect(w, r, "/sessions", map[string]string{"status": "revoked"})
}

// RevokeAllSessions revokes all sessions except the current one.
func (h *Handler) RevokeAllSessions(w http.ResponseWriter, r *http.Request) {
	user, _ := auth.UserFromContext(r.Context())
	currentSessionID := auth.SessionIDFromContext(r.Context())

	// Get all sessions for user
	sessions, err := h.store.Sessions.ListByUser(r.Context(), user.ID)
	if err != nil {
		http.Error(w, "failed to load sessions", http.StatusInternalServerError)
		return
	}

	// Delete all except current
	for _, s := range sessions {
		if s.ID != currentSessionID {
			_ = h.store.Sessions.Delete(r.Context(), s.ID)
		}
	}

	h.redirect(w, r, "/sessions", map[string]string{"status": "all_revoked"})
}

// AppPasswords displays the app passwords page (GET only).
func (h *Handler) AppPasswords(w http.ResponseWriter, r *http.Request) {
	user, _ := auth.UserFromContext(r.Context())
	h.renderAppPasswords(w, r, user, "")
}

// CreateAppPassword creates a new app password (POST only).
func (h *Handler) CreateAppPassword(w http.ResponseWriter, r *http.Request) {
	if err := r.ParseForm(); err != nil {
		http.Error(w, "invalid form", http.StatusBadRequest)
		return
	}
	label := strings.TrimSpace(r.FormValue("label"))
	if label == "" {
		http.Error(w, "label is required", http.StatusBadRequest)
		return
	}
	var expiresAt *time.Time
	if exp := strings.TrimSpace(r.FormValue("expires_at")); exp != "" {
		// Try datetime-local format first (from HTML5 input), then RFC3339
		var parsed time.Time
		var err error
		parsed, err = time.ParseInLocation("2006-01-02T15:04", exp, time.Local)
		if err != nil {
			parsed, err = time.Parse(time.RFC3339, exp)
		}
		if err != nil {
			http.Error(w, "invalid expiry format", http.StatusBadRequest)
			return
		}
		expiresAt = &parsed
	}

	user, _ := auth.UserFromContext(r.Context())
	token, _, err := h.authService.CreateAppPassword(r.Context(), user.ID, label, expiresAt)
	if err != nil {
		http.Error(w, "create failed", http.StatusInternalServerError)
		return
	}

	h.renderAppPasswords(w, r, user, token)
}

// RevokeAppPassword revokes an app password.
func (h *Handler) RevokeAppPassword(w http.ResponseWriter, r *http.Request) {
	user, _ := auth.UserFromContext(r.Context())

	idStr := chi.URLParam(r, "id")
	id, err := strconv.ParseInt(idStr, 10, 64)
	if err != nil {
		http.Error(w, "invalid token id", http.StatusBadRequest)
		return
	}

	token, err := h.store.AppPasswords.GetByID(r.Context(), id)
	if err != nil {
		http.Error(w, "failed to load app password", http.StatusInternalServerError)
		return
	}
	if token == nil || token.UserID != user.ID {
		http.Error(w, "not found", http.StatusNotFound)
		return
	}
	if token.RevokedAt == nil {
		if err := h.store.AppPasswords.Revoke(r.Context(), id); err != nil {
			http.Error(w, "failed to revoke app password", http.StatusInternalServerError)
			return
		}
	}

	http.Redirect(w, r, "/app-passwords", http.StatusFound)
}

// DeleteAppPassword deletes a revoked app password.
func (h *Handler) DeleteAppPassword(w http.ResponseWriter, r *http.Request) {
	user, _ := auth.UserFromContext(r.Context())

	idStr := chi.URLParam(r, "id")
	id, err := strconv.ParseInt(idStr, 10, 64)
	if err != nil {
		http.Error(w, "invalid token id", http.StatusBadRequest)
		return
	}

	token, err := h.store.AppPasswords.GetByID(r.Context(), id)
	if err != nil {
		http.Error(w, "failed to load app password", http.StatusInternalServerError)
		return
	}
	if token == nil || token.UserID != user.ID {
		http.Error(w, "not found", http.StatusNotFound)
		return
	}
	if token.RevokedAt == nil {
		http.Error(w, "token must be revoked before deletion", http.StatusBadRequest)
		return
	}

	if err := h.store.AppPasswords.DeleteRevoked(r.Context(), id); err != nil {
		http.Error(w, "failed to delete app password", http.StatusInternalServerError)
		return
	}

	http.Redirect(w, r, "/app-passwords", http.StatusFound)
}

// renderAppPasswords renders the app passwords page with optional plaintext token.
func (h *Handler) renderAppPasswords(w http.ResponseWriter, r *http.Request, user *store.User, plaintext string) {
	passwords, err := h.store.AppPasswords.ListByUser(r.Context(), user.ID)
	if err != nil {
		http.Error(w, "failed to load app passwords", http.StatusInternalServerError)
		return
	}

	var view []map[string]any
	now := time.Now()
	for _, p := range passwords {
		status := "active"
		revoked := p.RevokedAt != nil
		expired := p.ExpiresAt != nil && p.ExpiresAt.Before(now)
		if revoked {
			status = "revoked"
		} else if expired {
			status = "expired"
		}
		view = append(view, map[string]any{
			"id":         p.ID,
			"label":      p.Label,
			"created_at": p.CreatedAt,
			"expires_at": p.ExpiresAt,
			"last_used":  p.LastUsedAt,
			"status":     status,
			"revoked":    revoked,
			"expired":    expired,
		})
	}
	data := h.withFlash(r, map[string]any{
		"Title":        "App Passwords",
		"User":         user,
		"AppPasswords": view,
	})
	if plaintext != "" {
		data["PlainToken"] = plaintext
		data["FlashMessage"] = "created"
	}
	h.render(w, r, "app_passwords.html", data)
}

// Logout logs the user out.
func (h *Handler) Logout(w http.ResponseWriter, r *http.Request) {
	h.authService.RequireSession(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		h.authService.ClearSession(w, r)
		http.Redirect(w, r, "/", http.StatusFound)
	})).ServeHTTP(w, r)
}
