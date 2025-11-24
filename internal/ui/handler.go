package ui

import (
	"fmt"
	"html/template"
	"net/http"
	"time"

	"github.com/example/calcard/internal/auth"
	"github.com/example/calcard/internal/config"
	"github.com/example/calcard/internal/store"
)

// Handler serves server-rendered HTML pages.
type Handler struct {
	cfg         *config.Config
	store       *store.Store
	authService *auth.Service
	templates   *template.Template
}

func NewHandler(cfg *config.Config, store *store.Store, authService *auth.Service) *Handler {
	return &Handler{cfg: cfg, store: store, authService: authService, templates: templates}
}

func (h *Handler) Dashboard(w http.ResponseWriter, r *http.Request) {
	user, _ := auth.UserFromContext(r.Context())
	calendars, _ := h.store.Calendars.ListByUser(r.Context(), user.ID)
	books, _ := h.store.AddressBooks.ListByUser(r.Context(), user.ID)
	passwords, _ := h.store.AppPasswords.ListByUser(r.Context(), user.ID)

	data := map[string]any{
		"Title":         "Dashboard",
		"User":          user,
		"CalendarCount": len(calendars),
		"BookCount":     len(books),
		"AppPwdCount":   len(passwords),
	}

	h.render(w, "dashboard.html", data)
}

func (h *Handler) Calendars(w http.ResponseWriter, r *http.Request) {
	user, _ := auth.UserFromContext(r.Context())
	calendars, err := h.store.Calendars.ListByUser(r.Context(), user.ID)
	if err != nil {
		http.Error(w, "failed to load calendars", http.StatusInternalServerError)
		return
	}
	data := map[string]any{
		"Title":     "Calendars",
		"User":      user,
		"Calendars": calendars,
	}
	h.render(w, "calendars.html", data)
}

func (h *Handler) AddressBooks(w http.ResponseWriter, r *http.Request) {
	user, _ := auth.UserFromContext(r.Context())
	books, err := h.store.AddressBooks.ListByUser(r.Context(), user.ID)
	if err != nil {
		http.Error(w, "failed to load address books", http.StatusInternalServerError)
		return
	}
	data := map[string]any{
		"Title": "Address Books",
		"User":  user,
		"Books": books,
	}
	h.render(w, "addressbooks.html", data)
}

func (h *Handler) AppPasswords(w http.ResponseWriter, r *http.Request) {
	user, _ := auth.UserFromContext(r.Context())
	passwords, err := h.store.AppPasswords.ListByUser(r.Context(), user.ID)
	if err != nil {
		http.Error(w, "failed to load app passwords", http.StatusInternalServerError)
		return
	}
	var view []map[string]any
	for _, p := range passwords {
		status := "active"
		if p.RevokedAt != nil {
			status = "revoked"
		} else if p.ExpiresAt != nil && p.ExpiresAt.Before(time.Now()) {
			status = "expired"
		}
		view = append(view, map[string]any{
			"label":      p.Label,
			"created_at": p.CreatedAt,
			"expires_at": p.ExpiresAt,
			"last_used":  p.LastUsedAt,
			"status":     status,
		})
	}
	data := map[string]any{
		"Title":        "App Passwords",
		"User":         user,
		"AppPasswords": view,
	}
	h.render(w, "app_passwords.html", data)
}

func (h *Handler) Logout(w http.ResponseWriter, r *http.Request) {
	h.authService.RequireSession(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		h.authService.ClearSession(w)
		http.Redirect(w, r, "/auth/login", http.StatusFound)
	})).ServeHTTP(w, r)
}

func (h *Handler) render(w http.ResponseWriter, name string, data any) {
	if err := h.templates.ExecuteTemplate(w, name, data); err != nil {
		http.Error(w, fmt.Sprintf("template error: %v", err), http.StatusInternalServerError)
	}
}
