package dav

import (
	"net/http"
	"path"

	"github.com/jw6ventures/calcard/internal/auth"
	"github.com/jw6ventures/calcard/internal/config"
	"github.com/jw6ventures/calcard/internal/logging"
	"github.com/jw6ventures/calcard/internal/store"
)

// logClass is the component tag applied to every DAV log line.
const logClass = "DAV"

// Options configures a DAV server instance.
type Options struct {
	Config     *config.Config
	Store      *store.Store
	Extensions []Extension
	Logger     logging.Sink
}

// DavServer contains the DAV server state shared by default modules and
// registered extensions. It serves HTTP via ServeHTTP and dispatches each
// WebDAV/CalDAV/CardDAV method to the matching handler.
type DavServer struct {
	cfg      *config.Config
	store    *store.Store
	registry *Registry
	log      *logging.Logger
}

// NewDavServer creates a DAV server with the Community DAV modules and any
// caller-provided extensions registered in order.
func NewDavServer(opts Options) *DavServer {
	registry := NewRegistry()
	registerDefaultDAVModules(registry)
	for _, ext := range opts.Extensions {
		if ext != nil {
			ext.RegisterDAV(registry)
		}
	}
	return &DavServer{cfg: opts.Config, store: opts.Store, registry: registry, log: logging.New(opts.Logger, logClass)}
}

// logger returns a usable logger, lazily creating a no-op one so handlers never
// need to nil-check before logging.
func (h *DavServer) logger() *logging.Logger {
	if h.log == nil {
		h.log = logging.New(nil, logClass)
	}
	return h.log
}

func (h *DavServer) davRegistry() *Registry {
	if h.registry == nil {
		h.registry = NewRegistry()
		registerDefaultDAVModules(h.registry)
	}
	return h.registry
}

func (h *DavServer) RegisteredMethods() []string {
	return h.davRegistry().RegisteredMethods()
}

// RouteRequiresAuth reports whether the registered extension route matching the
// given method and path requires authentication. Routes with no match default
// to requiring auth so callers fail closed.
func (h *DavServer) RouteRequiresAuth(method, requestPath string) bool {
	route, ok := h.davRegistry().methodRoute(method, path.Clean(requestPath))
	if !ok {
		return true
	}
	return route.options.Auth != MethodAuthNone
}

func (h *DavServer) handleRegisteredMethod(w http.ResponseWriter, r *http.Request) bool {
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
			h.logger().Error("handleRegisteredMethod", "unauthenticated request for %s %s", r.Method, r.URL.Path)
			http.Error(w, "missing user", http.StatusUnauthorized)
			return true
		}
	}
	h.logger().Trace("handleRegisteredMethod", "dispatching %s %s to registered handler", r.Method, r.URL.Path)
	route.handler(w, r)
	return true
}

// ensureRequestCaches installs the shared request state used by ACL, lock and
// collection-resolution lookups unless the request already carries it.
func ensureRequestCaches(r *http.Request) *http.Request {
	if davRequestStateFromContext(r.Context()) != nil {
		return r
	}
	ctx := withDAVRequestState(r.Context())
	davRequestStateFromContext(ctx).setPrimaryDAVTarget(parseDAVTarget(r.URL.Path))
	return r.WithContext(ctx)
}

func (h *DavServer) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	h.logger().Debug("ServeHTTP", "%s %s", r.Method, r.URL.Path)
	h.serveRequest(w, r)
}

func (h *DavServer) serveRequest(w http.ResponseWriter, r *http.Request) {
	r = ensureRequestCaches(r)
	if _, authenticated := auth.UserFromContext(r.Context()); authenticated {
		if rejectBirthdayCalendarMutation(w, r) {
			return
		}
	}
	if h.handleRegisteredMethod(w, r) {
		return
	}
	switch r.Method {
	case http.MethodOptions:
		h.options(w, r)
	case http.MethodHead:
		h.head(w, r)
	case http.MethodGet:
		h.get(w, r)
	case "PROPFIND":
		h.propfind(w, r)
	case "PROPPATCH":
		h.proppatch(w, r)
	case "MKCOL":
		h.mkcol(w, r)
	case "MKCALENDAR":
		h.mkcalendar(w, r)
	case http.MethodPut:
		h.put(w, r)
	case http.MethodDelete:
		h.delete(w, r)
	case "REPORT":
		h.report(w, r)
	case "COPY":
		h.copy(w, r)
	case "MOVE":
		h.move(w, r)
	case "LOCK":
		h.lock(w, r)
	case "UNLOCK":
		h.unlock(w, r)
	case "ACL":
		h.acl(w, r)
	default:
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
	}
}

// The named method entry points are retained for callers that mount or test a
// single DAV method. All of them use the same dispatcher as ServeHTTP.
func (h *DavServer) Options(w http.ResponseWriter, r *http.Request)    { h.serveRequest(w, r) }
func (h *DavServer) Head(w http.ResponseWriter, r *http.Request)       { h.serveRequest(w, r) }
func (h *DavServer) Get(w http.ResponseWriter, r *http.Request)        { h.serveRequest(w, r) }
func (h *DavServer) Propfind(w http.ResponseWriter, r *http.Request)   { h.serveRequest(w, r) }
func (h *DavServer) Proppatch(w http.ResponseWriter, r *http.Request)  { h.serveRequest(w, r) }
func (h *DavServer) Mkcol(w http.ResponseWriter, r *http.Request)      { h.serveRequest(w, r) }
func (h *DavServer) Mkcalendar(w http.ResponseWriter, r *http.Request) { h.serveRequest(w, r) }
func (h *DavServer) Put(w http.ResponseWriter, r *http.Request)        { h.serveRequest(w, r) }
func (h *DavServer) Delete(w http.ResponseWriter, r *http.Request)     { h.serveRequest(w, r) }
func (h *DavServer) Report(w http.ResponseWriter, r *http.Request)     { h.serveRequest(w, r) }
func (h *DavServer) Copy(w http.ResponseWriter, r *http.Request)       { h.serveRequest(w, r) }
func (h *DavServer) Move(w http.ResponseWriter, r *http.Request)       { h.serveRequest(w, r) }
func (h *DavServer) Lock(w http.ResponseWriter, r *http.Request)       { h.serveRequest(w, r) }
func (h *DavServer) Unlock(w http.ResponseWriter, r *http.Request)     { h.serveRequest(w, r) }
func (h *DavServer) Acl(w http.ResponseWriter, r *http.Request)        { h.serveRequest(w, r) }

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
