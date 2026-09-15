package httpserver

import (
	"bytes"
	"fmt"
	"log"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/go-chi/chi/v5/middleware"
	"github.com/jw6ventures/calcard/internal/auth"
	"github.com/jw6ventures/calcard/internal/config"
	"github.com/jw6ventures/calcard/internal/dav"
	"github.com/jw6ventures/calcard/internal/http/ratelimit"
	"github.com/jw6ventures/calcard/internal/store"
	"golang.org/x/time/rate"
)

func TestOverrideMethodOnlyPromotesPutAndDeleteOnPost(t *testing.T) {
	tests := []struct {
		name       string
		method     string
		target     string
		formBody   string
		wantMethod string
	}{
		{
			name:       "form override to delete",
			method:     http.MethodPost,
			target:     "/",
			formBody:   "_method=delete",
			wantMethod: http.MethodDelete,
		},
		{
			name:       "query override to put",
			method:     http.MethodPost,
			target:     "/?_method=put",
			wantMethod: http.MethodPut,
		},
		{
			name:       "get ignored",
			method:     http.MethodGet,
			target:     "/?_method=delete",
			wantMethod: http.MethodGet,
		},
		{
			name:       "patch ignored",
			method:     http.MethodPost,
			target:     "/?_method=patch",
			wantMethod: http.MethodPost,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var gotMethod string
			handler := overrideMethod(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				gotMethod = r.Method
				w.WriteHeader(http.StatusNoContent)
			}))

			req := httptest.NewRequest(tt.method, tt.target, strings.NewReader(tt.formBody))
			req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
			rec := httptest.NewRecorder()
			handler.ServeHTTP(rec, req)

			if gotMethod != tt.wantMethod {
				t.Fatalf("method = %q, want %q", gotMethod, tt.wantMethod)
			}
		})
	}
}

func TestNewRouterPublicEndpoints(t *testing.T) {
	db, mock, err := sqlmock.New(sqlmock.MonitorPingsOption(true))
	if err != nil {
		t.Fatalf("sqlmock.New() error = %v", err)
	}
	defer db.Close()

	cfg := &config.Config{BaseURL: "http://localhost:8080", PrometheusEnabled: true}
	r := NewRouter(cfg, store.New(db), nil)

	req := httptest.NewRequest(http.MethodGet, "/healthz", nil)
	rec := httptest.NewRecorder()
	r.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK || rec.Body.String() != "ok" {
		t.Fatalf("/healthz = %d %q", rec.Code, rec.Body.String())
	}

	mock.ExpectPing()
	req = httptest.NewRequest(http.MethodGet, "/readyz", nil)
	rec = httptest.NewRecorder()
	r.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK || rec.Body.String() != "ok" {
		t.Fatalf("/readyz = %d %q", rec.Code, rec.Body.String())
	}

	req = httptest.NewRequest(http.MethodGet, "/.well-known/caldav", nil)
	rec = httptest.NewRecorder()
	r.ServeHTTP(rec, req)
	if rec.Code != http.StatusMovedPermanently || rec.Header().Get("Location") != "/dav/" {
		t.Fatalf("/.well-known/caldav = %d %q", rec.Code, rec.Header().Get("Location"))
	}

	for _, target := range []string{"/.well-known/caldav", "/.well-known/carddav"} {
		req = httptest.NewRequest(http.MethodOptions, target, nil)
		rec = httptest.NewRecorder()
		r.ServeHTTP(rec, req)
		if rec.Code != http.StatusMovedPermanently || rec.Header().Get("Location") != "/dav/" {
			t.Fatalf("OPTIONS %s = %d %q", target, rec.Code, rec.Header().Get("Location"))
		}
	}

	req = httptest.NewRequest("PROPFIND", "/principals/user", nil)
	rec = httptest.NewRecorder()
	r.ServeHTTP(rec, req)
	if rec.Code != http.StatusMovedPermanently || rec.Header().Get("Location") != "/dav/principals/" {
		t.Fatalf("/principals = %d %q", rec.Code, rec.Header().Get("Location"))
	}

	req = httptest.NewRequest(http.MethodOptions, "/dav/calendars", nil)
	rec = httptest.NewRecorder()
	r.ServeHTTP(rec, req)
	if rec.Code != http.StatusNoContent {
		t.Fatalf("OPTIONS /dav = %d", rec.Code)
	}

	req = httptest.NewRequest(http.MethodGet, "/metrics", nil)
	rec = httptest.NewRecorder()
	r.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK || !strings.Contains(rec.Body.String(), "calcard_http_requests_total") {
		t.Fatalf("/metrics = %d", rec.Code)
	}

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("sql expectations: %v", err)
	}
}

func TestNewRouterMetricsCanBeDisabled(t *testing.T) {
	db, _, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New() error = %v", err)
	}
	defer db.Close()

	r := NewRouter(&config.Config{BaseURL: "http://localhost:8080"}, store.New(db), nil)
	req := httptest.NewRequest(http.MethodGet, "/metrics", nil)
	rec := httptest.NewRecorder()
	r.ServeHTTP(rec, req)

	if rec.Code != http.StatusNotFound {
		t.Fatalf("/metrics status = %d", rec.Code)
	}
}

func TestNewRouterWithOptionsWiresDAVExtensions(t *testing.T) {
	db, _, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New() error = %v", err)
	}
	defer db.Close()

	r := NewRouterWithOptions(&config.Config{BaseURL: "http://localhost:8080"}, store.New(db), nil, RouterOptions{
		DAVExtensions: []dav.Extension{davExtensionFunc(func(reg *dav.Registry) {
			reg.RegisterCollection("/dav/pro")
			reg.RegisterMethod("SEARCH", "/dav/pro", dav.MethodOptions{Auth: dav.MethodAuthRequired}, func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(http.StatusAccepted)
			})
		})},
		DAVAuthMiddleware: func(next http.Handler) http.Handler {
			return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				ctx := auth.WithUser(r.Context(), &store.User{ID: 1})
				next.ServeHTTP(w, r.WithContext(ctx))
			})
		},
	})
	req := httptest.NewRequest("SEARCH", "/dav/pro/query", nil)
	rec := httptest.NewRecorder()

	r.ServeHTTP(rec, req)

	if rec.Code != http.StatusAccepted {
		t.Fatalf("extension SEARCH status = %d, want %d: %s", rec.Code, http.StatusAccepted, rec.Body.String())
	}

	req = httptest.NewRequest(http.MethodOptions, "/dav/pro/query", nil)
	rec = httptest.NewRecorder()

	r.ServeHTTP(rec, req)

	if rec.Code != http.StatusNoContent {
		t.Fatalf("extension OPTIONS status = %d, want %d: %s", rec.Code, http.StatusNoContent, rec.Body.String())
	}
	if allow := rec.Header().Get("Allow"); !strings.Contains(allow, "SEARCH") {
		t.Fatalf("extension OPTIONS Allow = %q, want SEARCH", allow)
	}
}

func TestNewRouterWithOptionsHonorsMethodAuthNone(t *testing.T) {
	db, _, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New() error = %v", err)
	}
	defer db.Close()

	r := NewRouterWithOptions(&config.Config{BaseURL: "http://localhost:8080"}, store.New(db), nil, RouterOptions{
		DAVExtensions: []dav.Extension{davExtensionFunc(func(reg *dav.Registry) {
			reg.RegisterCollection("/dav/pro")
			reg.RegisterMethod("SEARCH", "/dav/pro", dav.MethodOptions{Auth: dav.MethodAuthNone}, func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(http.StatusAccepted)
			})
		})},
		// Auth middleware that always rejects: a MethodAuthNone route must still
		// be reachable without passing through it.
		DAVAuthMiddleware: func(next http.Handler) http.Handler {
			return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				http.Error(w, "denied", http.StatusUnauthorized)
			})
		},
	})

	req := httptest.NewRequest("SEARCH", "/dav/pro/query", nil)
	rec := httptest.NewRecorder()

	r.ServeHTTP(rec, req)

	if rec.Code != http.StatusAccepted {
		t.Fatalf("MethodAuthNone SEARCH status = %d, want %d: %s", rec.Code, http.StatusAccepted, rec.Body.String())
	}
}

func TestNewRouterWithOptionsWiresAdditiveDAVMethodOnDefaultPath(t *testing.T) {
	db, _, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New() error = %v", err)
	}
	defer db.Close()

	r := NewRouterWithOptions(&config.Config{BaseURL: "http://localhost:8080"}, store.New(db), nil, RouterOptions{
		DAVExtensions: []dav.Extension{davExtensionFunc(func(reg *dav.Registry) {
			reg.RegisterMethod(http.MethodPost, "/dav/calendars", dav.MethodOptions{Auth: dav.MethodAuthRequired}, func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(http.StatusAccepted)
			})
		})},
		DAVAuthMiddleware: func(next http.Handler) http.Handler {
			return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				ctx := auth.WithUser(r.Context(), &store.User{ID: 1})
				next.ServeHTTP(w, r.WithContext(ctx))
			})
		},
	})

	req := httptest.NewRequest(http.MethodPost, "/dav/calendars/1/outbox", nil)
	rec := httptest.NewRecorder()

	r.ServeHTTP(rec, req)

	if rec.Code != http.StatusAccepted {
		t.Fatalf("extension POST status = %d, want %d: %s", rec.Code, http.StatusAccepted, rec.Body.String())
	}
}

func TestNewRouterDoesNotWriteRequestLogs(t *testing.T) {
	db, _, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New() error = %v", err)
	}
	defer db.Close()

	var logs bytes.Buffer
	originalLogger := middleware.DefaultLogger
	middleware.DefaultLogger = middleware.RequestLogger(&middleware.DefaultLogFormatter{
		Logger:  log.New(&logs, "", 0),
		NoColor: true,
	})
	t.Cleanup(func() {
		middleware.DefaultLogger = originalLogger
	})

	r := NewRouter(&config.Config{BaseURL: "http://localhost:8080"}, store.New(db), nil)
	req := httptest.NewRequest(http.MethodGet, "/healthz", nil)
	rec := httptest.NewRecorder()
	r.ServeHTTP(rec, req)

	if logs.Len() != 0 {
		t.Fatalf("request logs = %q, want none", logs.String())
	}
}

func TestNewRouterWithOptionsCapturesPublicDiscoveryTraffic(t *testing.T) {
	db, _, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New() error = %v", err)
	}
	defer db.Close()

	var capture bytes.Buffer
	r := NewRouterWithOptions(&config.Config{BaseURL: "http://localhost:8080"}, store.New(db), nil, RouterOptions{
		TrafficCaptureWriter: &capture,
	})
	req := httptest.NewRequest("PROPFIND", "/.well-known/caldav", strings.NewReader("<propfind/>"))
	req.Header.Set("Authorization", "Basic dXNlcjpwYXNz")
	rec := httptest.NewRecorder()

	r.ServeHTTP(rec, req)

	if rec.Code != http.StatusMovedPermanently {
		t.Fatalf("discovery status = %d, want %d", rec.Code, http.StatusMovedPermanently)
	}
	if got := capture.String(); !strings.Contains(got, `"path":"/.well-known/caldav"`) ||
		!strings.Contains(got, `"body":"<propfind/>"`) ||
		!strings.Contains(got, `"Authorization":"Basic ${CALCARD_DAV_BASIC_AUTH}"`) {
		t.Fatalf("traffic capture = %q", got)
	}
}

type davExtensionFunc func(*dav.Registry)

func (f davExtensionFunc) RegisterDAV(r *dav.Registry) {
	f(r)
}

// chi's middleware.RealIP rewrites r.RemoteAddr from client-supplied headers
// with no trust check, and it runs ahead of everything that later reads that
// field to decide whether the request is trustworthy. Once a deployment names
// its trusted proxies, the rewrite must happen only for those peers.
func TestTrustedRealIPOnlyRewritesForConfiguredProxies(t *testing.T) {
	tests := []struct {
		name           string
		trustedProxies []string
		remoteAddr     string
		want           string
	}{
		{
			name:           "a trusted proxy's forwarded address is adopted",
			trustedProxies: []string{"10.0.0.0/8"},
			remoteAddr:     "10.1.2.3:4567",
			want:           "198.51.100.7",
		},
		{
			name:           "an untrusted peer cannot name its own address",
			trustedProxies: []string{"10.0.0.0/8"},
			remoteAddr:     "203.0.113.9:4567",
			want:           "203.0.113.9:4567",
		},
		{
			// Unconfigured deployments trust every peer's forwarded headers;
			// the config loader warns about it at startup.
			name:       "no configured proxies keeps the forwarded address",
			remoteAddr: "203.0.113.9:4567",
			want:       "198.51.100.7",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var seen string
			handler := trustedRealIP(tt.trustedProxies)(http.HandlerFunc(func(_ http.ResponseWriter, r *http.Request) {
				seen = r.RemoteAddr
			}))

			req := httptest.NewRequest(http.MethodGet, "http://calcard.example/dav/", nil)
			req.RemoteAddr = tt.remoteAddr
			req.Header.Set("X-Real-IP", "198.51.100.7")
			handler.ServeHTTP(httptest.NewRecorder(), req)

			if seen != tt.want {
				t.Fatalf("RemoteAddr = %q, want %q", seen, tt.want)
			}
		})
	}
}

// The end-to-end shape of the same defect: a client on an untrusted address
// claiming to be a trusted proxy, over a cleartext connection it also claims is
// TLS, must not be offered or granted HTTP Basic.
func TestSpoofedForwardedHeadersDoNotUnlockBasicOverCleartext(t *testing.T) {
	trustedProxies := []string{"10.0.0.0/8"}
	var secure bool
	handler := trustedRealIP(trustedProxies)(http.HandlerFunc(func(_ http.ResponseWriter, r *http.Request) {
		secure = auth.RequestIsSecure(r, trustedProxies)
	}))

	req := httptest.NewRequest(http.MethodGet, "http://calcard.example/dav/", nil)
	req.RemoteAddr = "203.0.113.9:4567"
	req.Header.Set("X-Real-IP", "10.1.2.3")
	req.Header.Set("X-Forwarded-For", "10.1.2.3")
	req.Header.Set("True-Client-IP", "10.1.2.3")
	req.Header.Set("X-Forwarded-Proto", "https")
	handler.ServeHTTP(httptest.NewRecorder(), req)

	if secure {
		t.Fatal("a spoofed X-Real-IP made a cleartext request look secure, which would unlock HTTP Basic")
	}
}

// The defect's positive shape. A proxy the deployment trusts terminates TLS and
// forwards for a client that is, as clients are, outside the proxy CIDRs. The
// request is secure, and it has to stay secure once the forwarded address has
// been resolved into RemoteAddr -- otherwise every Basic app password behind a
// TLS-terminating proxy stops working, with no Digest to fall back to.
func TestTrustedProxyRequestStaysSecureAfterForwardedAddressResolution(t *testing.T) {
	trustedProxies := []string{"10.0.0.0/8"}

	req := httptest.NewRequest(http.MethodGet, "http://calcard.example/dav/", nil)
	req.RemoteAddr = "10.1.2.3:4567"
	req.Header.Set("X-Real-IP", "198.51.100.7")
	req.Header.Set("X-Forwarded-For", "198.51.100.7")
	req.Header.Set("X-Forwarded-Proto", "https")

	if !auth.RequestIsSecure(req, trustedProxies) {
		t.Fatal("a trusted proxy's forwarded request was not secure before the middleware")
	}

	var (
		secure     bool
		remoteAddr string
	)
	handler := trustedRealIP(trustedProxies)(http.HandlerFunc(func(_ http.ResponseWriter, r *http.Request) {
		secure = auth.RequestIsSecure(r, trustedProxies)
		remoteAddr = r.RemoteAddr
	}))
	handler.ServeHTTP(httptest.NewRecorder(), req)

	if remoteAddr != "198.51.100.7" {
		t.Fatalf("RemoteAddr = %q, want the forwarded client address", remoteAddr)
	}
	if !secure {
		t.Fatal("resolving the forwarded client address made a trusted proxy's HTTPS request look insecure")
	}
}

// A rate-limit bucket has to name the client the trusted proxy is forwarding
// for, and nothing the client can choose. The proxy appends its own hop to
// X-Forwarded-For, so the entries to its left are the client's to write: if the
// bucket follows one of them, a single client rotates the value and is never
// limited at all.
//
// The composition is what matters here -- trustedRealIP resolves the forwarded
// address into RemoteAddr before the limiter runs, so the limiter's own peer
// test cannot read that field.
func TestRateLimiterBucketsByTheClientNotAForwardedHeader(t *testing.T) {
	trustedProxies := []string{"10.0.0.0/8"}
	limiter := ratelimit.NewIPRateLimiter(rate.Limit(1), 2, time.Minute, trustedProxies)
	handler := trustedRealIP(trustedProxies)(limiter.Middleware()(http.HandlerFunc(
		func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusNoContent) })))

	// One client behind the proxy, writing whatever it likes ahead of the hop
	// the proxy appends for it.
	send := func(clientWritten string) int {
		req := httptest.NewRequest(http.MethodGet, "http://calcard.example/dav/", nil)
		req.RemoteAddr = "10.1.2.3:4567"
		req.Header.Set("X-Forwarded-For", clientWritten+", 198.51.100.7")
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, req)
		return rec.Code
	}

	for attempt := 0; attempt < 50; attempt++ {
		if send(fmt.Sprintf("203.0.113.%d", attempt)) == http.StatusTooManyRequests {
			return
		}
	}
	t.Fatal("rotating the leftmost X-Forwarded-For entry kept the client out of its own bucket for 50 requests")
}
