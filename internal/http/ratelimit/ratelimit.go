package ratelimit

import (
	"net/http"
	"sync"
	"time"

	"github.com/jw6ventures/calcard/internal/http/clientip"
	"golang.org/x/time/rate"
)

// IPRateLimiter manages rate limiters per IP address
type IPRateLimiter struct {
	limiters       map[string]*limiterEntry
	mu             sync.RWMutex
	rate           rate.Limit
	burst          int
	cleanup        time.Duration
	maxEntries     int
	trustedProxies clientip.TrustedProxies
}

type limiterEntry struct {
	limiter    *rate.Limiter
	lastAccess time.Time
}

// NewIPRateLimiter creates a new IP-based rate limiter
// rate: requests per second (e.g., 5 = 5 requests per second)
// burst: maximum burst size (e.g., 10 = allow 10 requests at once)
// cleanup: how often to clean up stale entries
// trustedProxies: CIDR ranges or IPs of trusted reverse proxies (empty = trust all proxies for backwards compatibility)
func NewIPRateLimiter(r rate.Limit, b int, cleanup time.Duration, trustedProxies []string) *IPRateLimiter {
	limiter := &IPRateLimiter{
		limiters:       make(map[string]*limiterEntry),
		rate:           r,
		burst:          b,
		cleanup:        cleanup,
		maxEntries:     10000, // Prevent unbounded growth
		trustedProxies: clientip.NewTrustedProxies(trustedProxies),
	}

	// Start cleanup goroutine to prevent memory leaks
	go limiter.cleanupStale()

	return limiter
}

func (l *IPRateLimiter) getLimiter(ip string) *rate.Limiter {
	l.mu.Lock()
	defer l.mu.Unlock()

	entry, exists := l.limiters[ip]
	if !exists {
		// Check if we've hit the max entries limit
		if len(l.limiters) >= l.maxEntries {
			// Evict oldest entry
			l.evictOldest()
		}

		entry = &limiterEntry{
			limiter:    rate.NewLimiter(l.rate, l.burst),
			lastAccess: time.Now(),
		}
		l.limiters[ip] = entry
	} else {
		entry.lastAccess = time.Now()
	}

	return entry.limiter
}

func (l *IPRateLimiter) evictOldest() {
	var oldestIP string
	var oldestTime time.Time

	for ip, entry := range l.limiters {
		if oldestIP == "" || entry.lastAccess.Before(oldestTime) {
			oldestIP = ip
			oldestTime = entry.lastAccess
		}
	}

	if oldestIP != "" {
		delete(l.limiters, oldestIP)
	}
}

func (l *IPRateLimiter) cleanupStale() {
	ticker := time.NewTicker(l.cleanup)
	defer ticker.Stop()

	for range ticker.C {
		l.mu.Lock()
		cutoff := time.Now().Add(-l.cleanup * 2) // Remove entries idle for 2x cleanup interval
		for ip, entry := range l.limiters {
			if entry.lastAccess.Before(cutoff) {
				delete(l.limiters, ip)
			}
		}
		l.mu.Unlock()
	}
}

// Middleware creates HTTP middleware for rate limiting
func (l *IPRateLimiter) Middleware() func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			ip := l.getClientIP(r)
			limiter := l.getLimiter(ip)

			if !limiter.Allow() {
				http.Error(w, "rate limit exceeded", http.StatusTooManyRequests)
				return
			}

			next.ServeHTTP(w, r)
		})
	}
}

// getClientIP names the bucket a request counts against. Resolving it is a
// trust decision about the connection, shared with the session record and
// auth's transport-security check through the clientip package so all three
// attribute a request to the same client.
func (l *IPRateLimiter) getClientIP(r *http.Request) string {
	return l.trustedProxies.ClientIP(r)
}
