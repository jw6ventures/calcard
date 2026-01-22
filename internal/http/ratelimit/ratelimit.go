package ratelimit

import (
	"net"
	"net/http"
	"strings"
	"sync"
	"time"

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
	trustedProxies []*net.IPNet
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
		limiters:   make(map[string]*limiterEntry),
		rate:       r,
		burst:      b,
		cleanup:    cleanup,
		maxEntries: 10000, // Prevent unbounded growth
	}

	// Parse trusted proxy CIDRs
	if len(trustedProxies) > 0 {
		for _, cidr := range trustedProxies {
			// Try parsing as CIDR first
			_, ipnet, err := net.ParseCIDR(cidr)
			if err != nil {
				// If not a CIDR, try as a single IP
				ip := net.ParseIP(cidr)
				if ip != nil {
					// Convert single IP to CIDR
					if ip.To4() != nil {
						_, ipnet, _ = net.ParseCIDR(cidr + "/32")
					} else {
						_, ipnet, _ = net.ParseCIDR(cidr + "/128")
					}
				}
			}
			if ipnet != nil {
				limiter.trustedProxies = append(limiter.trustedProxies, ipnet)
			}
		}
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

func (l *IPRateLimiter) getClientIP(r *http.Request) string {
	remoteIP := parseIP(r.RemoteAddr)

	// If we have trusted proxies configured, check if request is from one
	if len(l.trustedProxies) > 0 {
		trusted := false
		for _, ipnet := range l.trustedProxies {
			if ipnet.Contains(remoteIP) {
				trusted = true
				break
			}
		}

		// If not from a trusted proxy, use RemoteAddr directly
		if !trusted {
			return remoteIP.String()
		}
	}

	// Parse X-Forwarded-For header
	// Format: client, proxy1, proxy2
	// We want the leftmost (original client) IP
	xff := r.Header.Get("X-Forwarded-For")
	if xff != "" {
		ips := strings.Split(xff, ",")
		if len(ips) > 0 {
			clientIP := strings.TrimSpace(ips[0])
			if parsed := net.ParseIP(clientIP); parsed != nil {
				return parsed.String()
			}
		}
	}

	// Fall back to X-Real-IP
	if xri := r.Header.Get("X-Real-IP"); xri != "" {
		if parsed := net.ParseIP(xri); parsed != nil {
			return parsed.String()
		}
	}

	// Fall back to RemoteAddr
	return remoteIP.String()
}

func parseIP(addr string) net.IP {
	// Try parsing as IP:port first
	host, _, err := net.SplitHostPort(addr)
	if err == nil {
		return net.ParseIP(host)
	}
	return net.ParseIP(addr)
}
