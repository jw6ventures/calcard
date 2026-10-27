package httpserver

import (
	"context"
	"io"
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
	"github.com/jw6ventures/calcard/internal/http/clientip"
	"github.com/jw6ventures/calcard/internal/http/csrf"
	"github.com/jw6ventures/calcard/internal/http/ratelimit"
	"github.com/jw6ventures/calcard/internal/http/trafficcapture"
	"github.com/jw6ventures/calcard/internal/logging"
	"github.com/jw6ventures/calcard/internal/metrics"
	"github.com/jw6ventures/calcard/internal/store"
	"github.com/jw6ventures/calcard/internal/ui"
)

// trustedRealIP resolves r.RemoteAddr to the client clientip.ClientIP
// attributes the request to, so traffic capture records the same client the
// rate limiter and the session record do; a request whose client is the peer
// keeps the peer's address, port included. It does so only when the immediate
// peer is a proxy APP_TRUSTED_PROXIES names: rewriting on any other peer's say-so
// lets a client claim to be a trusted proxy, and a spoofed X-Forwarded-Proto:
// https would then unlock HTTP Basic over cleartext -- the transport requirement
// RFC 4791 §11 makes of it.
//
// The peer address is recorded first, through clientip.WithPeerAddr. Trust is
// decided on the connection, and once RemoteAddr carries the client, testing it
// against the proxy CIDRs asks whether the client is a proxy. After this,
// RemoteAddr answers "who is the client" and clientip.PeerAddr answers "who
// connected".
//
// A deployment configuring no trusted proxies trusts every peer's forwarded
// headers, which the configuration loader already warns about at startup.
func trustedRealIP(trustedProxies []string) func(http.Handler) http.Handler {
	trusted := clientip.NewTrustedProxies(trustedProxies)
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if trusted.AllowsPeer(r.RemoteAddr) {
				r = r.WithContext(clientip.WithPeerAddr(r.Context(), r.RemoteAddr))
				r.RemoteAddr = trusted.RemoteAddr(r)
			}
			next.ServeHTTP(w, r)
		})
	}
}

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
	// Logger is the leveled log sink handed to the DAV server. A nil sink
	// disables DAV logging.
	Logger logging.Sink
	// TrafficCaptureWriter receives replay-compatible JSONL request records.
	// A nil writer disables traffic capture.
	TrafficCaptureWriter io.Writer
}

// NewRouter wires all HTTP routes for UI and DAV endpoints.
func NewRouter(cfg *config.Config, store *store.Store, authService *auth.Service) http.Handler {
	return NewRouterWithOptions(cfg, store, authService, RouterOptions{})
}

// oauthCallbackPath returns the path the OAuth callback is served at.
// config.Load has already validated a configured value.
func oauthCallbackPath(cfg *config.Config) string {
	if cfg.OAuth.RedirectPath == "" {
		return config.DefaultOAuthRedirectPath
	}
	return cfg.OAuth.RedirectPath
}

// rateLimitMiddleware limits each client IP to rps requests per second with a
// bucket of burst. A rate of 0 is a limiter turned off: the middleware passes
// every request through.
func rateLimitMiddleware(rps float64, burst int, trustedProxies []string) func(http.Handler) http.Handler {
	if rps <= 0 {
		return func(next http.Handler) http.Handler { return next }
	}
	return ratelimit.NewIPRateLimiter(rate.Limit(rps), burst, 5*time.Minute, trustedProxies).Middleware()
}

// NewRouterWithOptions wires all HTTP routes and optional integrations.
func NewRouterWithOptions(cfg *config.Config, store *store.Store, authService *auth.Service, opts RouterOptions) http.Handler {
	r := chi.NewRouter()

	authRateLimit := rateLimitMiddleware(cfg.RateLimit.AuthRPS, cfg.RateLimit.AuthBurst, cfg.TrustedProxies)
	davRateLimit := rateLimitMiddleware(cfg.RateLimit.DAVRPS, cfg.RateLimit.DAVBurst, cfg.TrustedProxies)

	r.Use(middleware.RequestID)
	r.Use(trustedRealIP(cfg.TrustedProxies))
	if opts.TrafficCaptureWriter != nil {
		captureLogger := logging.New(opts.Logger, "HTTP")
		r.Use(trafficcapture.Middleware(trafficcapture.Options{
			Writer: opts.TrafficCaptureWriter,
			OnError: func(err error) {
				captureLogger.Error("TrafficCapture", "%v", err)
			},
		}))
	}
	r.Use(middleware.Recoverer)
	r.Use(requestDeadline(cfg.HTTP.WriteTimeout))
	r.Use(overrideMethod)
	r.Use(metrics.Middleware())

	r.Get("/healthz", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	})

	readiness := &readinessProbe{check: store.HealthCheck, timeout: readinessTimeout}
	r.Get("/readyz", func(w http.ResponseWriter, r *http.Request) {
		if err := readiness.ready(r.Context()); err != nil {
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
	r.MethodFunc("OPTIONS", "/.well-known/caldav", wellKnownHandler)
	r.MethodFunc("PROPFIND", "/.well-known/caldav", wellKnownHandler)

	r.Get("/.well-known/carddav", wellKnownHandler)
	r.MethodFunc("OPTIONS", "/.well-known/carddav", wellKnownHandler)
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

	uiHandler := ui.NewHandler(cfg, store, authService, dav.ValidateCalendarObject, dav.EventInstances)
	uiHandler.ShareTimezoneOnsets(dav.SharedTimezoneOnsets())
	apiHandler := api.NewHandler(cfg, store, dav.ValidateCalendarObject)
	r.Route("/auth", func(r chi.Router) {
		r.Use(authRateLimit)
		r.Get("/login", authService.BeginOAuth)
	})
	// Registered on the root rather than under /auth because the auth service
	// sends the identity provider to BaseURL+RedirectPath, wherever that is.
	r.With(authRateLimit).Get(oauthCallbackPath(cfg), authService.HandleOAuthCallback)

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
		r.Post("/addressbooks/{id}/shares", uiHandler.ShareAddressBook)
		r.Delete("/addressbooks/{id}/shares/{userId}", uiHandler.UnshareAddressBook)
		r.Post("/addressbooks/{id}/shares/{userId}/delete", uiHandler.UnshareAddressBook) // HTML form fallback

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
		r.Use(davRateLimit)
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
		r.Get("/addressbooks/{id}/shares", apiHandler.ListAddressBookShares)
		r.Post("/addressbooks/{id}/shares", apiHandler.ShareAddressBook)
		r.Delete("/addressbooks/{id}/shares/{userId}", apiHandler.UnshareAddressBook)
		r.Get("/addressbooks/{id}/contacts", apiHandler.ListContacts)
		r.Get("/addressbooks/{id}/contacts/{uid}", apiHandler.GetContact)
		r.Post("/addressbooks/{id}/contacts", apiHandler.CreateContact)
		r.Put("/addressbooks/{id}/contacts/{uid}", apiHandler.UpdateContact)
		r.Delete("/addressbooks/{id}/contacts/{uid}", apiHandler.DeleteContact)
	})

	davHandler := dav.NewDavServer(dav.Options{Config: cfg, Store: store, Extensions: opts.DAVExtensions, Logger: opts.Logger})
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
		r.Use(davRateLimit)

		// OPTIONS and root PROPFIND must be accessible without authentication for CalDAV client discovery
		r.Method("OPTIONS", "/*", davHandler)

		// All other methods require authentication
		r.Group(func(r chi.Router) {
			r.Use(davAuth)
			r.Method("HEAD", "/*", davHandler)
			r.Method("GET", "/*", davHandler)
			r.Method("PROPFIND", "/*", davHandler)
			r.Method("PROPPATCH", "/*", davHandler)
			r.Method("MKCOL", "/*", davHandler)
			r.Method("MKCALENDAR", "/*", davHandler)
			r.Method("PUT", "/*", davHandler)
			r.Method("DELETE", "/*", davHandler)
			r.Method("REPORT", "/*", davHandler)
			r.Method("COPY", "/*", davHandler)
			r.Method("MOVE", "/*", davHandler)
			r.Method("LOCK", "/*", davHandler)
			r.Method("UNLOCK", "/*", davHandler)
			r.Method("ACL", "/*", davHandler)
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
func extensionMethodHandler(davHandler *dav.DavServer, davAuth func(http.Handler) http.Handler) http.Handler {
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

// requestDeadline ends each request's context once the server's write
// timeout has passed. A response is undeliverable after that, so this cancels
// only work that can no longer succeed, and it is what releases a handler
// blocked on a database that has stopped answering.
func requestDeadline(writeTimeout time.Duration) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		if writeTimeout <= 0 {
			return next
		}
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			ctx, cancel := context.WithTimeout(r.Context(), writeTimeout)
			defer cancel()
			next.ServeHTTP(w, r.WithContext(ctx))
		})
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
