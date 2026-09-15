package ratelimit

import (
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/jw6ventures/calcard/internal/http/clientip"
	"golang.org/x/time/rate"
)

func TestGetClientIPTrustedProxyUsesForwardedHeaders(t *testing.T) {
	limiter := NewIPRateLimiter(rate.Limit(1), 1, time.Hour, []string{"10.0.0.0/8", "2001:db8::1"})

	req := httptest.NewRequest(http.MethodGet, "/", nil)
	req.RemoteAddr = "10.1.2.3:1234"
	req.Header.Set("X-Forwarded-For", "198.51.100.7, 10.1.2.3")
	if got := limiter.getClientIP(req); got != "198.51.100.7" {
		t.Fatalf("getClientIP() = %q", got)
	}

	req = httptest.NewRequest(http.MethodGet, "/", nil)
	req.RemoteAddr = "[2001:db8::1]:1234"
	req.Header.Set("X-Real-IP", "2001:db8::99")
	if got := limiter.getClientIP(req); got != "2001:db8::99" {
		t.Fatalf("getClientIP() = %q", got)
	}
}

func TestGetClientIPUntrustedProxyIgnoresForwardedHeaders(t *testing.T) {
	limiter := NewIPRateLimiter(rate.Limit(1), 1, time.Hour, []string{"10.0.0.0/8"})
	req := httptest.NewRequest(http.MethodGet, "/", nil)
	req.RemoteAddr = "203.0.113.9:9000"
	req.Header.Set("X-Forwarded-For", "198.51.100.7")
	req.Header.Set("X-Real-IP", "198.51.100.8")

	if got := limiter.getClientIP(req); got != "203.0.113.9" {
		t.Fatalf("getClientIP() = %q", got)
	}
}

func TestMiddlewareReturnsTooManyRequestsAfterBurstIsExhausted(t *testing.T) {
	limiter := NewIPRateLimiter(rate.Limit(1000), 1, time.Hour, nil)
	handler := limiter.Middleware()(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNoContent)
	}))

	req := httptest.NewRequest(http.MethodGet, "/", nil)
	req.RemoteAddr = "198.51.100.10:1234"

	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	if rec.Code != http.StatusNoContent {
		t.Fatalf("first status = %d", rec.Code)
	}

	rec = httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	if rec.Code != http.StatusTooManyRequests {
		t.Fatalf("second status = %d", rec.Code)
	}
}

func TestGetLimiterEvictsOldestEntryWhenAtCapacity(t *testing.T) {
	limiter := &IPRateLimiter{
		limiters: map[string]*limiterEntry{
			"oldest": {limiter: rate.NewLimiter(rate.Limit(1), 1), lastAccess: time.Now().Add(-2 * time.Hour)},
			"newer":  {limiter: rate.NewLimiter(rate.Limit(1), 1), lastAccess: time.Now().Add(-1 * time.Hour)},
		},
		rate:       rate.Limit(1),
		burst:      1,
		maxEntries: 2,
	}

	_ = limiter.getLimiter("fresh")

	if _, ok := limiter.limiters["oldest"]; ok {
		t.Fatal("expected oldest entry to be evicted")
	}
	if _, ok := limiter.limiters["fresh"]; !ok {
		t.Fatal("expected fresh entry to be present")
	}
}

// The forwarded-address middleware resolves the client into RemoteAddr before
// this runs, so the trusted-proxy test has to read the peer instead: asking
// whether the client is a proxy answers no for every real client, which skips
// the walk that picks the client out of the chain.
//
// The chain is walked from the right because the proxy appends its own hop: the
// entries to its left are whatever the client sent, and the rightmost hop no
// trusted proxy wrote is the furthest one this server has any reason to believe.
func TestGetClientIPReadsThePeerAndTheRightmostUntrustedHop(t *testing.T) {
	limiter := NewIPRateLimiter(rate.Limit(1), 1, time.Hour, []string{"10.0.0.0/8"})

	req := httptest.NewRequest(http.MethodGet, "/", nil)
	// What middleware.RealIP leaves behind: the leftmost X-Forwarded-For entry,
	// which here is the one the client wrote for itself.
	req.RemoteAddr = "203.0.113.9"
	req.Header.Set("X-Forwarded-For", "203.0.113.9, 198.51.100.7, 10.1.2.3")
	req = req.WithContext(clientip.WithPeerAddr(req.Context(), "10.1.2.3:4567"))

	if got := limiter.getClientIP(req); got != "198.51.100.7" {
		t.Fatalf("getClientIP() = %q, want 198.51.100.7", got)
	}
}
