package ui

import (
	"fmt"
	"html/template"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/example/calcard/internal/auth"
	"github.com/example/calcard/internal/config"
	"github.com/example/calcard/internal/http/csrf"
	"github.com/example/calcard/internal/store"
	"github.com/go-chi/chi/v5"
)

// Handler serves server-rendered HTML pages.
type Handler struct {
	cfg         *config.Config
	store       *store.Store
	authService *auth.Service
	templates   map[string]*template.Template
}

func NewHandler(cfg *config.Config, store *store.Store, authService *auth.Service) *Handler {
	return &Handler{cfg: cfg, store: store, authService: authService, templates: templates}
}

func (h *Handler) Dashboard(w http.ResponseWriter, r *http.Request) {
	user, _ := auth.UserFromContext(r.Context())
	calendars, err := h.store.Calendars.ListByUser(r.Context(), user.ID)
	if err != nil {
		http.Error(w, "failed to load calendars", http.StatusInternalServerError)
		return
	}
	books, err := h.store.AddressBooks.ListByUser(r.Context(), user.ID)
	if err != nil {
		http.Error(w, "failed to load address books", http.StatusInternalServerError)
		return
	}
	passwords, err := h.store.AppPasswords.ListByUser(r.Context(), user.ID)
	if err != nil {
		http.Error(w, "failed to load app passwords", http.StatusInternalServerError)
		return
	}

	data := h.withFlash(r, map[string]any{
		"Title":         "Dashboard",
		"User":          user,
		"CalendarCount": len(calendars),
		"BookCount":     len(books),
		"AppPwdCount":   len(passwords),
	})

	h.render(w, "dashboard.html", data)
}

func (h *Handler) Calendars(w http.ResponseWriter, r *http.Request) {
	user, _ := auth.UserFromContext(r.Context())
	calendars, err := h.store.Calendars.ListByUser(r.Context(), user.ID)
	if err != nil {
		http.Error(w, "failed to load calendars", http.StatusInternalServerError)
		return
	}
	data := h.withFlash(r, map[string]any{
		"Title":     "Calendars",
		"User":      user,
		"Calendars": calendars,
	})
	h.render(w, "calendars.html", data)
}

func (h *Handler) AddressBooks(w http.ResponseWriter, r *http.Request) {
	user, _ := auth.UserFromContext(r.Context())
	books, err := h.store.AddressBooks.ListByUser(r.Context(), user.ID)
	if err != nil {
		http.Error(w, "failed to load address books", http.StatusInternalServerError)
		return
	}
	data := h.withFlash(r, map[string]any{
		"Title": "Address Books",
		"User":  user,
		"Books": books,
	})
	h.render(w, "addressbooks.html", data)
}

func (h *Handler) AppPasswords(w http.ResponseWriter, r *http.Request) {
	user, _ := auth.UserFromContext(r.Context())

	switch r.Method {
	case http.MethodPost:
		h.createAppPassword(w, r, user)
	default:
		h.renderAppPasswords(w, r, user, "")
	}
}

func (h *Handler) createAppPassword(w http.ResponseWriter, r *http.Request, user *store.User) {
	if err := r.ParseForm(); err != nil {
		http.Error(w, "invalid form", http.StatusBadRequest)
		return
	}
	label := r.FormValue("label")
	if label == "" {
		http.Error(w, "label is required", http.StatusBadRequest)
		return
	}
	expiresAtStr := r.FormValue("expires_at")

	var expiresAt *time.Time
	if expiresAtStr != "" {
		t, err := time.Parse(time.RFC3339, expiresAtStr)
		if err != nil {
			http.Error(w, "invalid expires_at format", http.StatusBadRequest)
			return
		}
		expiresAt = &t
	}

	plaintext, _, err := h.authService.CreateAppPassword(r.Context(), user.ID, label, expiresAt)
	if err != nil {
		http.Error(w, "failed to create app password", http.StatusInternalServerError)
		return
	}

	h.renderAppPasswords(w, r, user, plaintext)
}

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
	h.render(w, "app_passwords.html", data)
}

func (h *Handler) CreateCalendar(w http.ResponseWriter, r *http.Request) {
	if err := r.ParseForm(); err != nil {
		h.redirect(w, r, "/calendars", map[string]string{"error": "invalid form"})
		return
	}
	name := strings.TrimSpace(r.FormValue("name"))
	if name == "" {
		h.redirect(w, r, "/calendars", map[string]string{"error": "name is required"})
		return
	}

	user, _ := auth.UserFromContext(r.Context())
	_, err := h.store.Calendars.Create(r.Context(), store.Calendar{UserID: user.ID, Name: name})
	if err != nil {
		h.redirect(w, r, "/calendars", map[string]string{"error": "failed to create"})
		return
	}
	h.redirect(w, r, "/calendars", map[string]string{"status": "created"})
}

func (h *Handler) RenameCalendar(w http.ResponseWriter, r *http.Request) {
	if err := r.ParseForm(); err != nil {
		h.redirect(w, r, "/calendars", map[string]string{"error": "invalid form"})
		return
	}
	name := strings.TrimSpace(r.FormValue("name"))
	if name == "" {
		h.redirect(w, r, "/calendars", map[string]string{"error": "name is required"})
		return
	}
	user, _ := auth.UserFromContext(r.Context())
	id, err := strconv.ParseInt(chi.URLParam(r, "id"), 10, 64)
	if err != nil {
		h.redirect(w, r, "/calendars", map[string]string{"error": "invalid id"})
		return
	}
	cal, err := h.store.Calendars.GetByID(r.Context(), id)
	if err != nil {
		h.redirect(w, r, "/calendars", map[string]string{"error": "rename failed"})
		return
	}
	if cal == nil || cal.UserID != user.ID {
		http.Error(w, "not found", http.StatusNotFound)
		return
	}
	if err := h.store.Calendars.Rename(r.Context(), user.ID, id, name); err != nil {
		h.redirect(w, r, "/calendars", map[string]string{"error": "rename failed"})
		return
	}
	h.redirect(w, r, "/calendars", map[string]string{"status": "renamed"})
}

func (h *Handler) DeleteCalendar(w http.ResponseWriter, r *http.Request) {
	id, err := strconv.ParseInt(chi.URLParam(r, "id"), 10, 64)
	if err != nil {
		h.redirect(w, r, "/calendars", map[string]string{"error": "invalid id"})
		return
	}
	user, _ := auth.UserFromContext(r.Context())
	if err := h.store.Calendars.Delete(r.Context(), user.ID, id); err != nil {
		h.redirect(w, r, "/calendars", map[string]string{"error": "delete failed"})
		return
	}
	h.redirect(w, r, "/calendars", map[string]string{"status": "deleted"})
}

func (h *Handler) CreateAddressBook(w http.ResponseWriter, r *http.Request) {
	if err := r.ParseForm(); err != nil {
		h.redirect(w, r, "/addressbooks", map[string]string{"error": "invalid form"})
		return
	}
	name := strings.TrimSpace(r.FormValue("name"))
	if name == "" {
		h.redirect(w, r, "/addressbooks", map[string]string{"error": "name is required"})
		return
	}
	user, _ := auth.UserFromContext(r.Context())
	_, err := h.store.AddressBooks.Create(r.Context(), store.AddressBook{UserID: user.ID, Name: name})
	if err != nil {
		h.redirect(w, r, "/addressbooks", map[string]string{"error": "failed to create"})
		return
	}
	h.redirect(w, r, "/addressbooks", map[string]string{"status": "created"})
}

func (h *Handler) RenameAddressBook(w http.ResponseWriter, r *http.Request) {
	if err := r.ParseForm(); err != nil {
		h.redirect(w, r, "/addressbooks", map[string]string{"error": "invalid form"})
		return
	}
	name := strings.TrimSpace(r.FormValue("name"))
	if name == "" {
		h.redirect(w, r, "/addressbooks", map[string]string{"error": "name is required"})
		return
	}
	id, err := strconv.ParseInt(chi.URLParam(r, "id"), 10, 64)
	if err != nil {
		h.redirect(w, r, "/addressbooks", map[string]string{"error": "invalid id"})
		return
	}
	user, _ := auth.UserFromContext(r.Context())
	book, err := h.store.AddressBooks.GetByID(r.Context(), id)
	if err != nil {
		h.redirect(w, r, "/addressbooks", map[string]string{"error": "rename failed"})
		return
	}
	if book == nil || book.UserID != user.ID {
		http.Error(w, "not found", http.StatusNotFound)
		return
	}
	if err := h.store.AddressBooks.Rename(r.Context(), user.ID, id, name); err != nil {
		h.redirect(w, r, "/addressbooks", map[string]string{"error": "rename failed"})
		return
	}
	h.redirect(w, r, "/addressbooks", map[string]string{"status": "renamed"})
}

func (h *Handler) DeleteAddressBook(w http.ResponseWriter, r *http.Request) {
	id, err := strconv.ParseInt(chi.URLParam(r, "id"), 10, 64)
	if err != nil {
		h.redirect(w, r, "/addressbooks", map[string]string{"error": "invalid id"})
		return
	}
	user, _ := auth.UserFromContext(r.Context())
	if err := h.store.AddressBooks.Delete(r.Context(), user.ID, id); err != nil {
		h.redirect(w, r, "/addressbooks", map[string]string{"error": "delete failed"})
		return
	}
	h.redirect(w, r, "/addressbooks", map[string]string{"status": "deleted"})
}

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
		parsed, err := time.Parse(time.RFC3339, exp)
		if err != nil {
			http.Error(w, "invalid expiry", http.StatusBadRequest)
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

func (h *Handler) Logout(w http.ResponseWriter, r *http.Request) {
	h.authService.RequireSession(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		h.authService.ClearSession(w)
		http.Redirect(w, r, "/auth/login", http.StatusFound)
	})).ServeHTTP(w, r)
}

func (h *Handler) withFlash(r *http.Request, data map[string]any) map[string]any {
	q := r.URL.Query()
	if status := q.Get("status"); status != "" {
		data["FlashMessage"] = status
	}
	if err := q.Get("error"); err != "" {
		data["FlashError"] = err
	}
	if token := q.Get("token"); token != "" {
		data["PlainToken"] = token
	}
	if csrfToken := csrf.TokenFromContext(r.Context()); csrfToken != "" {
		data["CSRFToken"] = csrfToken
	}
	return data
}

func (h *Handler) redirect(w http.ResponseWriter, r *http.Request, path string, params map[string]string) {
	q := url.Values{}
	for k, v := range params {
		if v != "" {
			q.Set(k, v)
		}
	}
	location := path
	if encoded := q.Encode(); encoded != "" {
		location += "?" + encoded
	}
	http.Redirect(w, r, location, http.StatusFound)
}

func (h *Handler) render(w http.ResponseWriter, name string, data any) {
	tmpl, ok := h.templates[name]
	if !ok {
		http.Error(w, fmt.Sprintf("template %q not found", name), http.StatusInternalServerError)
		return
	}

	if err := tmpl.ExecuteTemplate(w, name, data); err != nil {
		http.Error(w, fmt.Sprintf("template error: %v", err), http.StatusInternalServerError)
	}
}
