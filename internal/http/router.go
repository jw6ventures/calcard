package httpserver

import (
	"net/http"
	"strings"

	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"

	"github.com/example/calcard/internal/auth"
	"github.com/example/calcard/internal/config"
	"github.com/example/calcard/internal/dav"
	"github.com/example/calcard/internal/metrics"
	"github.com/example/calcard/internal/store"
	"github.com/example/calcard/internal/ui"
)

// NewRouter wires all HTTP routes for UI and DAV endpoints.
func NewRouter(cfg *config.Config, store *store.Store, authService *auth.Service, sessions *auth.SessionManager) http.Handler {
	r := chi.NewRouter()

	r.Use(middleware.RequestID)
	r.Use(middleware.RealIP)
	r.Use(middleware.Logger)
	r.Use(middleware.Recoverer)
	r.Use(overrideMethod)
	r.Use(metrics.Middleware())
	// TODO: add CSRF middleware for UI.

	r.Get("/healthz", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	})

	r.Get("/metrics", func(w http.ResponseWriter, r *http.Request) {
		metrics.Handler().ServeHTTP(w, r)
	})

	uiHandler := ui.NewHandler(cfg, store, authService)
	r.Get("/", uiHandler.Dashboard)
	r.Route("/auth", func(r chi.Router) {
		r.Get("/login", authService.BeginOAuth)
		r.Get("/callback", authService.HandleOAuthCallback)
		r.Get("/logout", uiHandler.Logout)
	})

	r.Group(func(r chi.Router) {
		r.Use(authService.RequireSession)
		r.Get("/calendars", uiHandler.Calendars)
		r.Get("/addressbooks", uiHandler.AddressBooks)
		r.Get("/app-passwords", uiHandler.AppPasswords)
		r.Post("/calendars", uiHandler.CreateCalendar)
		r.Put("/calendars/{id}", uiHandler.RenameCalendar)
		r.Delete("/calendars/{id}", uiHandler.DeleteCalendar)

		r.Post("/addressbooks", uiHandler.CreateAddressBook)
		r.Put("/addressbooks/{id}", uiHandler.RenameAddressBook)
		r.Delete("/addressbooks/{id}", uiHandler.DeleteAddressBook)

		r.Post("/app-passwords", uiHandler.CreateAppPassword)
		r.Delete("/app-passwords/{id}", uiHandler.RevokeAppPassword)
		r.Post("/app-passwords/{id}/revoke", uiHandler.RevokeAppPassword)
	})

	r.Route("/dav", func(r chi.Router) {
		r.Use(authService.RequireDAVAuth)
		davHandler := dav.NewHandler(cfg, store)
		r.MethodFunc("OPTIONS", "/*", davHandler.Options)
		r.MethodFunc("PROPFIND", "/*", davHandler.Propfind)
		r.MethodFunc("PROPPATCH", "/*", davHandler.Proppatch)
		r.MethodFunc("MKCOL", "/*", davHandler.Mkcol)
		r.MethodFunc("MKCALENDAR", "/*", davHandler.Mkcalendar)
		r.MethodFunc("PUT", "/*", davHandler.Put)
		r.MethodFunc("DELETE", "/*", davHandler.Delete)
		r.MethodFunc("REPORT", "/*", davHandler.Report)
	})

	return r
}

func overrideMethod(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		method := r.Method
		if r.Method == http.MethodPost {
			if m := strings.TrimSpace(r.PostFormValue("_method")); m != "" {
				method = m
			} else if m := strings.TrimSpace(r.URL.Query().Get("_method")); m != "" {
				method = m
			}
		}
		switch strings.ToUpper(method) {
		case http.MethodPut, http.MethodDelete:
			r.Method = strings.ToUpper(method)
		}
		next.ServeHTTP(w, r)
	})
}
