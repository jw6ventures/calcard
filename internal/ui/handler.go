package ui

import (
	"crypto/rand"
	"encoding/base64"
	"fmt"
	"html/template"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/example/calcard/internal/auth"
	"github.com/example/calcard/internal/config"
	"github.com/example/calcard/internal/store"
	"github.com/go-chi/chi/v5"
	"golang.org/x/crypto/bcrypt"
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
			"id":         p.ID,
			"created_at": p.CreatedAt,
			"expires_at": p.ExpiresAt,
			"last_used":  p.LastUsedAt,
			"status":     status,
		})
	}
	data := h.withFlash(r, map[string]any{
		"Title":        "App Passwords",
		"User":         user,
		"AppPasswords": view,
	})
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
	id, err := strconv.ParseInt(chi.URLParam(r, "id"), 10, 64)
	if err != nil {
		h.redirect(w, r, "/calendars", map[string]string{"error": "invalid id"})
		return
	}
	if err := h.store.Calendars.Rename(r.Context(), id, name); err != nil {
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
	if err := h.store.Calendars.Delete(r.Context(), id); err != nil {
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
	if err := h.store.AddressBooks.Rename(r.Context(), id, name); err != nil {
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
	if err := h.store.AddressBooks.Delete(r.Context(), id); err != nil {
		h.redirect(w, r, "/addressbooks", map[string]string{"error": "delete failed"})
		return
	}
	h.redirect(w, r, "/addressbooks", map[string]string{"status": "deleted"})
}

func (h *Handler) CreateAppPassword(w http.ResponseWriter, r *http.Request) {
	if err := r.ParseForm(); err != nil {
		h.redirect(w, r, "/app-passwords", map[string]string{"error": "invalid form"})
		return
	}
	label := strings.TrimSpace(r.FormValue("label"))
	if label == "" {
		h.redirect(w, r, "/app-passwords", map[string]string{"error": "label is required"})
		return
	}
	var expiresAt *time.Time
	if exp := strings.TrimSpace(r.FormValue("expires_at")); exp != "" {
		parsed, err := time.Parse(time.RFC3339, exp)
		if err != nil {
			h.redirect(w, r, "/app-passwords", map[string]string{"error": "invalid expiry"})
			return
		}
		expiresAt = &parsed
	}

	token, err := generateToken()
	if err != nil {
		h.redirect(w, r, "/app-passwords", map[string]string{"error": "token generation failed"})
		return
	}
	hash, err := bcrypt.GenerateFromPassword([]byte(token), bcrypt.DefaultCost)
	if err != nil {
		h.redirect(w, r, "/app-passwords", map[string]string{"error": "hashing failed"})
		return
	}

	user, _ := auth.UserFromContext(r.Context())
	_, err = h.store.AppPasswords.Create(r.Context(), store.AppPassword{
		UserID:    user.ID,
		Label:     label,
		TokenHash: string(hash),
		ExpiresAt: expiresAt,
	})
	if err != nil {
		h.redirect(w, r, "/app-passwords", map[string]string{"error": "create failed"})
		return
	}

	h.redirect(w, r, "/app-passwords", map[string]string{"status": "created", "token": token})
}

func (h *Handler) RevokeAppPassword(w http.ResponseWriter, r *http.Request) {
	id, err := strconv.ParseInt(chi.URLParam(r, "id"), 10, 64)
	if err != nil {
		h.redirect(w, r, "/app-passwords", map[string]string{"error": "invalid id"})
		return
	}
	if err := h.store.AppPasswords.Revoke(r.Context(), id); err != nil {
		h.redirect(w, r, "/app-passwords", map[string]string{"error": "revoke failed"})
		return
	}
	h.redirect(w, r, "/app-passwords", map[string]string{"status": "revoked"})
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

func generateToken() (string, error) {
	buf := make([]byte, 18)
	if _, err := rand.Read(buf); err != nil {
		return "", err
	}
	return base64.RawURLEncoding.EncodeToString(buf), nil
}

func (h *Handler) render(w http.ResponseWriter, name string, data any) {
	if err := h.templates.ExecuteTemplate(w, name, data); err != nil {
		http.Error(w, fmt.Sprintf("template error: %v", err), http.StatusInternalServerError)
	}
}
