package dav

import (
	"net/http"
	"path"

	"github.com/jw6ventures/calcard/internal/auth"
	"github.com/jw6ventures/calcard/internal/config"
	"github.com/jw6ventures/calcard/internal/store"
)

// Options configures a DAV server instance.
type Options struct {
	Config     *config.Config
	Store      *store.Store
	Extensions []Extension
}

// Server contains the DAV server state shared by default modules and
// registered extensions.
type Server struct {
	cfg      *config.Config
	store    *store.Store
	registry *Registry
}

// Handler is kept as a package compatibility alias while the DAV entrypoints
// move to Server.
type Handler = Server

// NewServer creates a DAV server with the Community DAV modules and any
// caller-provided extensions registered in order.
func NewServer(opts Options) *Server {
	registry := NewRegistry()
	registerDefaultDAVModules(registry)
	for _, ext := range opts.Extensions {
		if ext != nil {
			ext.RegisterDAV(registry)
		}
	}
	return &Server{cfg: opts.Config, store: opts.Store, registry: registry}
}

func (h *Handler) davRegistry() *Registry {
	if h.registry == nil {
		h.registry = NewRegistry()
		registerDefaultDAVModules(h.registry)
	}
	return h.registry
}

func (h *Handler) RegisteredMethods() []string {
	return h.davRegistry().RegisteredMethods()
}

// RouteRequiresAuth reports whether the registered extension route matching the
// given method and path requires authentication. Routes with no match default
// to requiring auth so callers fail closed.
func (h *Handler) RouteRequiresAuth(method, requestPath string) bool {
	route, ok := h.davRegistry().methodRoute(method, path.Clean(requestPath))
	if !ok {
		return true
	}
	return route.options.Auth != MethodAuthNone
}

func (h *Handler) handleRegisteredMethod(w http.ResponseWriter, r *http.Request) bool {
	if r == nil {
		return false
	}
	route, ok := h.davRegistry().methodRoute(r.Method, path.Clean(r.URL.Path))
	if !ok && r.Method == http.MethodHead {
		route, ok = h.davRegistry().methodRoute(http.MethodGet, path.Clean(r.URL.Path))
	}
	if !ok {
		return false
	}
	if route.options.Auth != MethodAuthNone {
		if _, ok := auth.UserFromContext(r.Context()); !ok {
			http.Error(w, "missing user", http.StatusUnauthorized)
			return true
		}
	}
	route.handler(w, r)
	return true
}

func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodOptions:
		h.Options(w, r)
	case http.MethodHead:
		h.Head(w, r)
	case http.MethodGet:
		h.Get(w, r)
	case "PROPFIND":
		h.Propfind(w, r)
	case "PROPPATCH":
		h.Proppatch(w, r)
	case "MKCOL":
		h.Mkcol(w, r)
	case "MKCALENDAR":
		h.Mkcalendar(w, r)
	case http.MethodPut:
		h.Put(w, r)
	case http.MethodDelete:
		h.Delete(w, r)
	case "REPORT":
		h.Report(w, r)
	case "COPY":
		h.Copy(w, r)
	case "MOVE":
		h.Move(w, r)
	case "LOCK":
		h.Lock(w, r)
	case "UNLOCK":
		h.Unlock(w, r)
	case "ACL":
		h.Acl(w, r)
	default:
		if h.handleRegisteredMethod(w, r) {
			return
		}
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
	}
}

func registerDefaultDAVModules(r *Registry) {
	for _, prefix := range []string{
		"/dav",
		"/dav/principals",
		"/dav/calendars",
		"/dav/addressbooks",
	} {
		r.RegisterCollection(prefix)
	}
}
