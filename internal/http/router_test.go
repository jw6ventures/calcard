package httpserver

import (
	"bytes"
	"fmt"
	"log"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"
	"github.com/jw6ventures/calcard/internal/auth"
	"github.com/jw6ventures/calcard/internal/config"
	"github.com/jw6ventures/calcard/internal/dav"
	"github.com/jw6ventures/calcard/internal/http/clientip"
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

	mock.ExpectExec("SELECT 1").WillReturnResult(sqlmock.NewResult(0, 0))
	req = httptest.NewRequest(http.MethodGet, "/readyz", nil)
	rec = httptest.NewRecorder()
	r.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK || rec.Body.String() != "ok" {
		t.Fatalf("/readyz = %d %q", rec.Code, rec.Body.String())
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("/readyz did not check the database: %v", err)
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

// The forwarded-address middleware runs ahead of everything that later reads
// r.RemoteAddr to decide whether the request is trustworthy. Once a deployment
// names its trusted proxies, the rewrite must happen only for those peers.
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

// RemoteAddr is what traffic capture records, so it has to name the client the
// rate limiter and the session record attribute the request to: True-Client-IP,
// X-Real-IP and the leftmost X-Forwarded-For entry are all the client's to
// write, and none of them may outrank the hop the trusted proxy appended. When
// the client is the peer itself, RemoteAddr keeps the port the connection came
// from.
func TestTrustedRealIPResolvesRemoteAddrAsClientIPDoes(t *testing.T) {
	trusted := []string{"10.0.0.0/8"}
	tests := []struct {
		name           string
		trustedProxies []string
		peer           string
		headers        map[string]string
		want           string
	}{
		{
			name:           "True-Client-IP is not read",
			trustedProxies: trusted,
			peer:           "10.1.2.3:4567",
			headers:        map[string]string{"True-Client-IP": "192.0.2.66", "X-Forwarded-For": "198.51.100.7"},
			want:           "198.51.100.7",
		},
		{
			name:           "X-Real-IP does not outrank X-Forwarded-For",
			trustedProxies: trusted,
			peer:           "10.1.2.3:4567",
			headers:        map[string]string{"X-Real-IP": "192.0.2.66", "X-Forwarded-For": "198.51.100.7"},
			want:           "198.51.100.7",
		},
		{
			name:           "the leftmost X-Forwarded-For entry is not the client",
			trustedProxies: trusted,
			peer:           "10.1.2.3:4567",
			headers:        map[string]string{"X-Forwarded-For": "192.0.2.66, 198.51.100.7"},
			want:           "198.51.100.7",
		},
		{
			name:           "an unreadable proxy hop leaves the peer",
			trustedProxies: trusted,
			peer:           "10.1.2.3:4567",
			headers:        map[string]string{"X-Real-IP": "192.0.2.66", "X-Forwarded-For": "192.0.2.66, unix:"},
			want:           "10.1.2.3:4567",
		},
		{
			name:           "no forwarding headers leaves the peer",
			trustedProxies: trusted,
			peer:           "10.1.2.3:4567",
			want:           "10.1.2.3:4567",
		},
		// Unconfigured deployments trust every peer's forwarded headers; the
		// config loader warns about it at startup.
		{
			name: "unconfigured: no forwarding headers leaves the peer",
			peer: "203.0.113.9:4567",
			want: "203.0.113.9:4567",
		},
		{
			name:    "unconfigured: True-Client-IP is not read",
			peer:    "203.0.113.9:4567",
			headers: map[string]string{"True-Client-IP": "192.0.2.66"},
			want:    "203.0.113.9:4567",
		},
		{
			name:    "unconfigured: X-Real-IP without X-Forwarded-For",
			peer:    "203.0.113.9:4567",
			headers: map[string]string{"X-Real-IP": "198.51.100.8"},
			want:    "198.51.100.8",
		},
		{
			name:    "unconfigured: the leftmost X-Forwarded-For entry outranks X-Real-IP",
			peer:    "203.0.113.9:4567",
			headers: map[string]string{"X-Real-IP": "198.51.100.8", "X-Forwarded-For": "192.0.2.66, 198.51.100.7"},
			want:    "192.0.2.66",
		},
		{
			name:    "unconfigured: an unreadable X-Forwarded-For falls back to X-Real-IP",
			peer:    "203.0.113.9:4567",
			headers: map[string]string{"X-Real-IP": "198.51.100.8", "X-Forwarded-For": "not-an-ip"},
			want:    "198.51.100.8",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var remoteAddr, peerAddr, clientIP string
			handler := trustedRealIP(tt.trustedProxies)(http.HandlerFunc(func(_ http.ResponseWriter, r *http.Request) {
				remoteAddr = r.RemoteAddr
				peerAddr = clientip.PeerAddr(r)
				clientIP = clientip.NewTrustedProxies(tt.trustedProxies).ClientIP(r)
			}))

			req := httptest.NewRequest(http.MethodGet, "http://calcard.example/dav/", nil)
			req.RemoteAddr = tt.peer
			for name, value := range tt.headers {
				req.Header.Set(name, value)
			}
			handler.ServeHTTP(httptest.NewRecorder(), req)

			if remoteAddr != tt.want {
				t.Fatalf("RemoteAddr = %q, want %q", remoteAddr, tt.want)
			}
			host := remoteAddr
			if h, _, err := net.SplitHostPort(remoteAddr); err == nil {
				host = h
			}
			if host != clientIP {
				t.Fatalf("RemoteAddr = %q, but ClientIP = %q", remoteAddr, clientIP)
			}
			if peerAddr != tt.peer {
				t.Fatalf("PeerAddr = %q, want the connecting peer %q", peerAddr, tt.peer)
			}
		})
	}
}

func TestTrafficCaptureRecordsTheClientTheTrustedProxyForwardedFor(t *testing.T) {
	db, _, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New() error = %v", err)
	}
	defer db.Close()

	var capture bytes.Buffer
	cfg := &config.Config{BaseURL: "http://localhost:8080", TrustedProxies: []string{"10.0.0.0/8"}}
	r := NewRouterWithOptions(cfg, store.New(db), nil, RouterOptions{TrafficCaptureWriter: &capture})
	req := httptest.NewRequest("PROPFIND", "/.well-known/caldav", strings.NewReader("<propfind/>"))
	req.RemoteAddr = "10.1.2.3:4567"
	req.Header.Set("True-Client-IP", "192.0.2.66")
	req.Header.Set("X-Forwarded-For", "192.0.2.66, 198.51.100.7")
	r.ServeHTTP(httptest.NewRecorder(), req)

	if got := capture.String(); !strings.Contains(got, `"ip":"198.51.100.7"`) {
		t.Fatalf("traffic capture = %q, want the client the proxy forwarded for", got)
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

// The auth and DAV limiters take their rate and burst from the configuration,
// each with a bucket of its own. The refill rate is far too slow to matter
// within the test, so each route admits exactly its burst.
// A rate of 0 turns a limiter off: a fleet that limits at its load balancer
// keeps CalCard's own buckets out of the way.
func TestNewRouterTurnsOffALimiterWhoseRateIsZero(t *testing.T) {
	db, _, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New() error = %v", err)
	}
	defer db.Close()

	cfg := &config.Config{BaseURL: "http://localhost:8080"}
	cfg.RateLimit.AuthRPS = 0
	cfg.RateLimit.AuthBurst = 1
	cfg.RateLimit.DAVRPS = 0
	cfg.RateLimit.DAVBurst = 1
	r := NewRouter(cfg, store.New(db), &auth.Service{})

	for _, target := range []string{"/auth/callback?state=x&code=y", "/dav/"} {
		for n := 0; n < 200; n++ {
			rec := httptest.NewRecorder()
			r.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, target, nil))
			if rec.Code == http.StatusTooManyRequests {
				t.Fatalf("%s answered 429 on request %d although its rate is 0", target, n+1)
			}
		}
	}
}

func TestNewRouterAppliesTheConfiguredRateLimits(t *testing.T) {
	db, _, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New() error = %v", err)
	}
	defer db.Close()

	cfg := &config.Config{BaseURL: "http://localhost:8080"}
	cfg.RateLimit.AuthRPS = 0.001
	cfg.RateLimit.AuthBurst = 2
	cfg.RateLimit.DAVRPS = 0.001
	cfg.RateLimit.DAVBurst = 3
	r := NewRouter(cfg, store.New(db), &auth.Service{})

	admitted := func(method, target string) int {
		for n := 0; n < 10; n++ {
			rec := httptest.NewRecorder()
			r.ServeHTTP(rec, httptest.NewRequest(method, target, nil))
			if rec.Code == http.StatusTooManyRequests {
				return n
			}
		}
		return -1
	}
	if got := admitted(http.MethodGet, "/auth/callback?state=x&code=y"); got != 2 {
		t.Errorf("auth endpoint admitted %d requests before 429, want the configured burst of 2", got)
	}
	if got := admitted(http.MethodOptions, "/dav/"); got != 3 {
		t.Errorf("DAV endpoint admitted %d requests before 429, want the configured burst of 3", got)
	}
}

// The auth service sends the identity provider to BaseURL+RedirectPath, so the
// router has to serve the callback at that same path. A handler reached without
// the state cookie answers 400 "invalid oauth state", which is what proves the
// request got to the callback rather than a 404 or the session redirect.
func TestNewRouterServesTheOAuthCallbackAtTheConfiguredPath(t *testing.T) {
	db, _, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New() error = %v", err)
	}
	defer db.Close()

	for _, tc := range []struct {
		name         string
		redirectPath string
		served       string
		notServed    string
	}{
		{name: "default", redirectPath: "", served: "/auth/callback"},
		{name: "explicit default", redirectPath: "/auth/callback", served: "/auth/callback"},
		{name: "custom outside /auth", redirectPath: "/oidc/return", served: "/oidc/return", notServed: "/auth/callback"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cfg := &config.Config{BaseURL: "http://localhost:8080"}
			cfg.OAuth.RedirectPath = tc.redirectPath
			r := NewRouter(cfg, store.New(db), &auth.Service{})

			rec := httptest.NewRecorder()
			r.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, tc.served+"?state=x&code=y", nil))
			if rec.Code != http.StatusBadRequest || !strings.Contains(rec.Body.String(), "invalid oauth state") {
				t.Fatalf("GET %s = %d %q, want the OAuth callback's 400", tc.served, rec.Code, rec.Body.String())
			}

			if tc.notServed != "" {
				rec = httptest.NewRecorder()
				r.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, tc.notServed+"?state=x&code=y", nil))
				if strings.Contains(rec.Body.String(), "invalid oauth state") {
					t.Fatalf("GET %s reached the OAuth callback although RedirectPath is %q", tc.notServed, tc.redirectPath)
				}
			}
		})
	}
}

// config.ValidateOAuthRedirectPath refuses a redirect path that collides with
// a route the application serves, from a list of owned prefixes kept beside it.
// Walking the real router is what keeps that list from falling behind: every
// registered route other than the callback itself must be refused.
func TestOAuthRedirectPathValidationCoversEveryRoute(t *testing.T) {
	db, _, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New() error = %v", err)
	}
	defer db.Close()

	cfg := &config.Config{BaseURL: "http://localhost:8080", PrometheusEnabled: true}
	routes, ok := NewRouter(cfg, store.New(db), &auth.Service{}).(chi.Routes)
	if !ok {
		t.Fatal("NewRouter no longer returns chi.Routes; walk its routes another way")
	}
	walked := 0
	err = chi.Walk(routes, func(method, route string, _ http.Handler, _ ...func(http.Handler) http.Handler) error {
		walked++
		if route == config.DefaultOAuthRedirectPath {
			return nil
		}
		// A pattern such as /dav/* or /calendars/{id} is checked through a
		// concrete path beneath it.
		concrete := strings.NewReplacer("/*", "/x", "{", "", "}", "").Replace(route)
		if config.ValidateOAuthRedirectPath(concrete) == nil {
			t.Errorf("%s %s is served by the router, but APP_OAUTH_REDIRECT_PATH=%s is accepted", method, route, concrete)
		}
		return nil
	})
	if err != nil {
		t.Fatalf("chi.Walk: %v", err)
	}
	if walked == 0 {
		t.Fatal("walked no routes")
	}
}
