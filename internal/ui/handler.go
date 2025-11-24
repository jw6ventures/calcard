package ui

import (
	"html/template"
	"net/http"

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
	// TODO: parse templates from embedded filesystem.
	tmpl := template.New("base")
	return &Handler{cfg: cfg, store: store, authService: authService, templates: tmpl}
}

func (h *Handler) Dashboard(w http.ResponseWriter, r *http.Request) {
	// TODO: load counts for calendars, address books, and app passwords.
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte("Dashboard placeholder"))
}

func (h *Handler) Calendars(w http.ResponseWriter, r *http.Request) {
	// TODO: render list of calendars and forms for CRUD.
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte("Calendars placeholder"))
}

func (h *Handler) AddressBooks(w http.ResponseWriter, r *http.Request) {
	// TODO: render list of address books and forms for CRUD.
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte("Address books placeholder"))
}

func (h *Handler) AppPasswords(w http.ResponseWriter, r *http.Request) {
	// TODO: render list of app passwords and creation form.
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte("App passwords placeholder"))
}

func (h *Handler) Logout(w http.ResponseWriter, r *http.Request) {
	// TODO: revoke session cookie and redirect to home/login.
	h.authService.RequireSession(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte("Logged out placeholder"))
	})).ServeHTTP(w, r)
}
