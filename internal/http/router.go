package httpserver

import (
	"context"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"
	"golang.org/x/time/rate"

	"github.com/jw6ventures/calcard/internal/api"
	"github.com/jw6ventures/calcard/internal/auth"
	"github.com/jw6ventures/calcard/internal/config"
	"github.com/jw6ventures/calcard/internal/dav"
	"github.com/jw6ventures/calcard/internal/http/csrf"
	"github.com/jw6ventures/calcard/internal/http/ratelimit"
	"github.com/jw6ventures/calcard/internal/metrics"
	"github.com/jw6ventures/calcard/internal/store"
	"github.com/jw6ventures/calcard/internal/ui"
)

var registeredDAVMethods = struct {
	sync.Mutex
	methods map[string]struct{}
}{methods: map[string]struct{}{
	http.MethodHead:    {},
	http.MethodGet:     {},
	http.MethodOptions: {},
	http.MethodPut:     {},
	http.MethodDelete:  {},
	"PROPFIND":         {},
	"PROPPATCH":        {},
	"MKCOL":            {},
	"MKCALENDAR":       {},
	"REPORT":           {},
	"COPY":             {},
	"MOVE":             {},
	"LOCK":             {},
	"UNLOCK":           {},
	"ACL":              {},
}}

func init() {
	for _, method := range []string{
		"PROPFIND",
		"PROPPATCH",
		"MKCOL",
		"MKCALENDAR",
		"REPORT",
		"COPY",
		"MOVE",
		"LOCK",
		"UNLOCK",
		"ACL",
	} {
		chi.RegisterMethod(method)
	}
}

// RouterOptions configures optional router integrations.
type RouterOptions struct {
	DAVExtensions     []dav.Extension
	DAVAuthMiddleware func(http.Handler) http.Handler
}

// NewRouter wires all HTTP routes for UI and DAV endpoints.
func NewRouter(cfg *config.Config, store *store.Store, authService *auth.Service) http.Handler {
	return NewRouterWithOptions(cfg, store, authService, RouterOptions{})
}

// NewRouterWithOptions wires all HTTP routes and optional integrations.
func NewRouterWithOptions(cfg *config.Config, store *store.Store, authService *auth.Service, opts RouterOptions) http.Handler {
	r := chi.NewRouter()

	// Auth endpoints: 5 requests per second, burst of 10
	authRateLimiter := ratelimit.NewIPRateLimiter(rate.Limit(5), 10, 5*time.Minute, cfg.TrustedProxies)
	// DAV endpoints: 20 requests per second, burst of 50 (more permissive for sync clients)
	davRateLimiter := ratelimit.NewIPRateLimiter(rate.Limit(20), 50, 5*time.Minute, cfg.TrustedProxies)

	r.Use(middleware.RequestID)
	r.Use(middleware.RealIP)
	r.Use(middleware.Recoverer)
	r.Use(overrideMethod)
	r.Use(metrics.Middleware())

	r.Get("/healthz", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	})

	r.Get("/readyz", func(w http.ResponseWriter, r *http.Request) {
		ctx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
		defer cancel()

		if err := store.HealthCheck(ctx); err != nil {
			http.Error(w, "unready", http.StatusServiceUnavailable)
			return
		}

		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	})

	if cfg.PrometheusEnabled {
		r.Get("/metrics", func(w http.ResponseWriter, r *http.Request) {
			metrics.Handler().ServeHTTP(w, r)
		})
	}

	// Handle both GET and PROPFIND for CalDAV/CardDAV discovery
	wellKnownHandler := func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, "/dav/", http.StatusMovedPermanently)
	}
	r.Get("/.well-known/caldav", wellKnownHandler)
	r.MethodFunc("PROPFIND", "/.well-known/caldav", wellKnownHandler)

	r.Get("/.well-known/carddav", wellKnownHandler)
	r.MethodFunc("PROPFIND", "/.well-known/carddav", wellKnownHandler)

	// Redirect root PROPFIND to /dav/ for discovery
	r.MethodFunc("PROPFIND", "/", wellKnownHandler)

	// Redirect /principals/ to /dav/principals/ for Apple Calendar compatibility
	principalsRedirectHandler := func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, "/dav/principals/", http.StatusMovedPermanently)
	}
	r.MethodFunc("PROPFIND", "/principals/*", principalsRedirectHandler)

	// Redirect Apple-specific legacy path to /dav/
	r.MethodFunc("PROPFIND", "/calendar/*", wellKnownHandler)

	uiHandler := ui.NewHandler(cfg, store, authService)
	apiHandler := api.NewHandler(cfg, store)
	r.Route("/auth", func(r chi.Router) {
		r.Use(authRateLimiter.Middleware())
		r.Get("/login", authService.BeginOAuth)
		r.Get("/callback", authService.HandleOAuthCallback)
	})

	r.With(authService.RequireSession, csrf.Middleware(cfg)).Post("/auth/logout", uiHandler.Logout)

	r.Group(func(r chi.Router) {
		r.Use(authService.RequireSession)
		r.Use(csrf.Middleware(cfg))
		r.Get("/", uiHandler.Dashboard)
		r.Get("/calendars", uiHandler.Calendars)
		r.Get("/calendars/all", uiHandler.ViewAllCalendars)
		r.Get("/calendars/all/events.json", uiHandler.GetAllCalendarEventsJSON)
		r.Get("/calendars/{id}", uiHandler.ViewCalendar)
		r.Get("/calendars/{id}/export", uiHandler.ExportCalendar)
		r.Get("/calendars/{id}/events.json", uiHandler.GetCalendarEventsJSON)
		r.Get("/addressbooks", uiHandler.AddressBooks)
		r.Get("/addressbooks/{id}", uiHandler.ViewAddressBook)
		r.Get("/app-passwords", uiHandler.AppPasswords)
		r.Get("/sessions", uiHandler.Sessions)
		r.Get("/birthdays", uiHandler.ViewBirthdays)
		r.Get("/help", uiHandler.Help)

		r.Post("/calendars", uiHandler.CreateCalendar)
		r.Put("/calendars/{id}", uiHandler.RenameCalendar)
		r.Delete("/calendars/{id}", uiHandler.DeleteCalendar)
		r.Post("/calendars/{id}/shares", uiHandler.ShareCalendar)
		r.Delete("/calendars/{id}/shares/{userId}", uiHandler.UnshareCalendar)
		r.Post("/calendars/{id}/shares/{userId}/delete", uiHandler.UnshareCalendar) // HTML form fallback

		// Calendar import
		r.Post("/calendars/{id}/import", uiHandler.ImportCalendar)

		// Event CRUD
		r.Post("/calendars/{id}/events", uiHandler.CreateEvent)
		r.Put("/calendars/{id}/events/{uid}", uiHandler.UpdateEvent)
		r.Delete("/calendars/{id}/events/{uid}", uiHandler.DeleteEvent)
		r.Post("/calendars/{id}/events/{uid}/delete", uiHandler.DeleteEvent) // HTML form fallback

		r.Post("/addressbooks", uiHandler.CreateAddressBook)
		r.Put("/addressbooks/{id}", uiHandler.RenameAddressBook)
		r.Delete("/addressbooks/{id}", uiHandler.DeleteAddressBook)

		// Address book import
		r.Post("/addressbooks/{id}/import", uiHandler.ImportAddressBook)

		// Contact CRUD
		r.Post("/addressbooks/{id}/contacts", uiHandler.CreateContact)
		r.Put("/addressbooks/{id}/contacts/{uid}", uiHandler.UpdateContact)
		r.Delete("/addressbooks/{id}/contacts/{uid}", uiHandler.DeleteContact)
		r.Post("/addressbooks/{id}/contacts/{uid}/delete", uiHandler.DeleteContact) // HTML form fallback
		r.Post("/addressbooks/{id}/contacts/{uid}/move", uiHandler.MoveContact)     // Move contact to another address book

		r.Post("/app-passwords", uiHandler.CreateAppPassword)
		r.Delete("/app-passwords/{id}", uiHandler.RevokeAppPassword)
		r.Post("/app-passwords/{id}/revoke", uiHandler.RevokeAppPassword)
		r.Post("/app-passwords/{id}/delete", uiHandler.DeleteAppPassword)

		r.Post("/sessions/{id}/revoke", uiHandler.RevokeSession)
		r.Post("/sessions/revoke-all", uiHandler.RevokeAllSessions)

		r.Post("/onboarding/complete", uiHandler.CompleteOnboarding)
	})

	r.Route("/api", func(r chi.Router) {
		r.Use(davRateLimiter.Middleware())
		r.Use(authService.RequireDAVAuth)
		r.Get("/calendars", apiHandler.ListCalendars)
		r.Get("/calendars/{id}", apiHandler.GetCalendar)
		r.Get("/calendars/{id}/events", apiHandler.ListEvents)
		r.Get("/calendars/{id}/events/{uid}", apiHandler.GetEvent)
		r.Post("/calendars/{id}/events", apiHandler.CreateEvent)
		r.Put("/calendars/{id}/events/{uid}", apiHandler.UpdateEvent)
		r.Delete("/calendars/{id}/events/{uid}", apiHandler.DeleteEvent)

		r.Get("/addressbooks", apiHandler.ListAddressBooks)
		r.Get("/addressbooks/{id}", apiHandler.GetAddressBook)
		r.Get("/addressbooks/{id}/contacts", apiHandler.ListContacts)
		r.Get("/addressbooks/{id}/contacts/{uid}", apiHandler.GetContact)
		r.Post("/addressbooks/{id}/contacts", apiHandler.CreateContact)
		r.Put("/addressbooks/{id}/contacts/{uid}", apiHandler.UpdateContact)
		r.Delete("/addressbooks/{id}/contacts/{uid}", apiHandler.DeleteContact)
	})

	davHandler := dav.NewServer(dav.Options{Config: cfg, Store: store, Extensions: opts.DAVExtensions})
	registerDAVMethods(davHandler.RegisteredMethods())
	davAuth := opts.DAVAuthMiddleware
	if davAuth == nil && authService != nil {
		davAuth = authService.RequireDAVAuth
	}
	if davAuth == nil {
		davAuth = func(next http.Handler) http.Handler {
			return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				http.Error(w, "authentication required", http.StatusUnauthorized)
			})
		}
	}

	r.Route("/dav", func(r chi.Router) {
		r.Use(davRateLimiter.Middleware())

		// OPTIONS and root PROPFIND must be accessible without authentication for CalDAV client discovery
		r.MethodFunc("OPTIONS", "/*", davHandler.Options)

		// All other methods require authentication
		r.Group(func(r chi.Router) {
			r.Use(davAuth)
			r.MethodFunc("HEAD", "/*", davHandler.Head)
			r.MethodFunc("GET", "/*", davHandler.Get)
			r.MethodFunc("PROPFIND", "/*", davHandler.Propfind)
			r.MethodFunc("PROPPATCH", "/*", davHandler.Proppatch)
			r.MethodFunc("MKCOL", "/*", davHandler.Mkcol)
			r.MethodFunc("MKCALENDAR", "/*", davHandler.Mkcalendar)
			r.MethodFunc("PUT", "/*", davHandler.Put)
			r.MethodFunc("DELETE", "/*", davHandler.Delete)
			r.MethodFunc("REPORT", "/*", davHandler.Report)
			r.MethodFunc("COPY", "/*", davHandler.Copy)
			r.MethodFunc("MOVE", "/*", davHandler.Move)
			r.MethodFunc("LOCK", "/*", davHandler.Lock)
			r.MethodFunc("UNLOCK", "/*", davHandler.Unlock)
			r.MethodFunc("ACL", "/*", davHandler.Acl)
		})

		// Extension-registered methods carry their own per-route auth policy.
		// They are mounted outside the auth group so that MethodAuthNone routes
		// can be served without authentication; routes that require auth are
		// wrapped with davAuth at request time.
		extensionHandler := extensionMethodHandler(davHandler, davAuth)
		for _, method := range davHandler.RegisteredMethods() {
			if isBuiltInDAVMethod(method) {
				continue
			}
			r.Method(method, "/*", extensionHandler)
		}
	})

	return r
}

func registerDAVMethods(methods []string) {
	registeredDAVMethods.Lock()
	defer registeredDAVMethods.Unlock()
	for _, method := range methods {
		method = strings.ToUpper(strings.TrimSpace(method))
		if method == "" {
			continue
		}
		if _, ok := registeredDAVMethods.methods[method]; ok {
			continue
		}
		chi.RegisterMethod(method)
		registeredDAVMethods.methods[method] = struct{}{}
	}
}

// extensionMethodHandler serves an extension-registered DAV method, applying
// davAuth only when the matched route declares that it requires authentication.
func extensionMethodHandler(davHandler *dav.Server, davAuth func(http.Handler) http.Handler) http.Handler {
	authed := davAuth(http.HandlerFunc(davHandler.ServeHTTP))
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if davHandler.RouteRequiresAuth(r.Method, r.URL.Path) {
			authed.ServeHTTP(w, r)
			return
		}
		davHandler.ServeHTTP(w, r)
	})
}

func isBuiltInDAVMethod(method string) bool {
	method = strings.ToUpper(strings.TrimSpace(method))
	switch method {
	case http.MethodHead, http.MethodGet, http.MethodOptions, http.MethodPut, http.MethodDelete,
		"PROPFIND", "PROPPATCH", "MKCOL", "MKCALENDAR", "REPORT", "COPY", "MOVE", "LOCK", "UNLOCK", "ACL":
		return true
	default:
		return false
	}
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
