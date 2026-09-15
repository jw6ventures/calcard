// Package clientip resolves the address an HTTP request is attributed to,
// honoring a forwarded header only when the immediate peer is a configured,
// trusted proxy.
//
// A rate-limit bucket, a recorded session address, and the transport-security
// decision behind HTTP Basic auth all have to agree on who the client is, so
// this logic lives in one leaf package with no dependencies beyond the
// standard library rather than being duplicated, or requiring every caller to
// depend on whichever higher-level package happened to implement it first.
package clientip

import (
	"context"
	"net"
	"net/http"
	"strings"
)

type contextKey string

const contextKeyPeerAddr contextKey = "peer_addr"

// WithPeerAddr records the address of the immediate peer, which is the only
// address a trust decision may be made about.
//
// The forwarded-address middleware resolves the client address into
// r.RemoteAddr, so from that point on RemoteAddr answers "who is the client"
// and no longer "who connected". Deciding whether to believe a forwarded header
// is a question about the connection, so it is answered from here instead: the
// client would otherwise be answering it for itself.
func WithPeerAddr(ctx context.Context, addr string) context.Context {
	return context.WithValue(ctx, contextKeyPeerAddr, addr)
}

// PeerAddr returns the address of the immediate peer, falling back to
// r.RemoteAddr when nothing recorded one -- a direct connection, or any path
// that does not run the forwarded-address middleware.
func PeerAddr(r *http.Request) string {
	if r == nil {
		return ""
	}
	if addr, ok := r.Context().Value(contextKeyPeerAddr).(string); ok && addr != "" {
		return addr
	}
	return r.RemoteAddr
}

// TrustedProxies is a parsed APP_TRUSTED_PROXIES set. It is built once and read
// per request, since the forwarded-address middleware asks about every request
// the server handles.
type TrustedProxies struct {
	nets []*net.IPNet
}

// NewTrustedProxies parses the configured proxy addresses and CIDR blocks,
// discarding entries that are neither.
func NewTrustedProxies(values []string) TrustedProxies {
	return TrustedProxies{nets: parseTrustedProxies(values)}
}

// AllowsPeer reports whether remoteAddr -- the address of the immediate peer,
// before any forwarded-header rewriting -- belongs to a configured proxy.
// Configuring none allows every peer, matching how RequestIsSecure and the rate
// limiter already treat an unconfigured deployment.
//
// The forwarded-address middleware asks this before it rewrites RemoteAddr:
// every later trust decision, RequestIsSecure included, reads that field, so
// rewriting it on an untrusted peer's say-so lets a client answer the question
// for itself.
func (t TrustedProxies) AllowsPeer(remoteAddr string) bool {
	if len(t.nets) == 0 {
		return true
	}
	ip, _ := parseRemoteAddr(remoteAddr)
	return isTrustedProxy(ip, t.nets)
}

// ClientIP resolves the address a request is attributed to: the peer, or, when
// a trusted proxy is forwarding, the furthest hop of the chain that no trusted
// proxy wrote.
//
// Everything keyed per client reads this -- a rate-limit bucket, a recorded
// session address -- and a bucket the client can choose is not a bucket. The
// chain is walked from the right because a proxy appends its own hop: the
// entries to its left are whatever the client sent, so the rightmost untrusted
// hop is the furthest one this server has any reason to believe.
//
// The peer is read through PeerAddr, not r.RemoteAddr. The forwarded-address
// middleware resolves the client into RemoteAddr, so testing that field asks
// whether the client is a proxy and, finding it is not, skips the walk.
func (t TrustedProxies) ClientIP(r *http.Request) string {
	remoteIP, remoteHost := parseRemoteAddr(PeerAddr(r))

	if len(t.nets) > 0 && !isTrustedProxy(remoteIP, t.nets) {
		if remoteIP != nil {
			return remoteIP.String()
		}
		return remoteHost
	}

	if xff := r.Header.Get("X-Forwarded-For"); xff != "" {
		if clientIP := forwardedClientIP(xff, t.nets); clientIP != nil {
			return clientIP.String()
		}
	}

	if xri := r.Header.Get("X-Real-IP"); xri != "" {
		if parsed := net.ParseIP(strings.TrimSpace(xri)); parsed != nil {
			return parsed.String()
		}
	}

	if remoteIP != nil {
		return remoteIP.String()
	}
	return remoteHost
}

func forwardedClientIP(xff string, trusted []*net.IPNet) net.IP {
	parts := strings.Split(xff, ",")

	if len(trusted) == 0 {
		for _, part := range parts {
			if parsed := net.ParseIP(strings.TrimSpace(part)); parsed != nil {
				return parsed
			}
		}
		return nil
	}

	for i := len(parts) - 1; i >= 0; i-- {
		candidate := strings.TrimSpace(parts[i])
		if candidate == "" {
			continue
		}
		parsed := net.ParseIP(candidate)
		if parsed == nil {
			continue
		}
		if !isTrustedProxy(parsed, trusted) {
			return parsed
		}
	}

	for _, part := range parts {
		if parsed := net.ParseIP(strings.TrimSpace(part)); parsed != nil {
			return parsed
		}
	}

	return nil
}

func parseTrustedProxies(values []string) []*net.IPNet {
	var trusted []*net.IPNet
	for _, raw := range values {
		value := strings.TrimSpace(raw)
		if value == "" {
			continue
		}
		_, ipnet, err := net.ParseCIDR(value)
		if err == nil {
			trusted = append(trusted, ipnet)
			continue
		}
		ip := net.ParseIP(value)
		if ip == nil {
			continue
		}
		suffix := "/128"
		if ip.To4() != nil {
			suffix = "/32"
		}
		_, ipnet, err = net.ParseCIDR(value + suffix)
		if err == nil {
			trusted = append(trusted, ipnet)
		}
	}
	return trusted
}

func isTrustedProxy(ip net.IP, trusted []*net.IPNet) bool {
	if ip == nil {
		return false
	}
	for _, ipnet := range trusted {
		if ipnet.Contains(ip) {
			return true
		}
	}
	return false
}

func parseRemoteAddr(remoteAddr string) (net.IP, string) {
	host, _, err := net.SplitHostPort(remoteAddr)
	if err == nil {
		if parsed := net.ParseIP(host); parsed != nil {
			return parsed, parsed.String()
		}
		return nil, host
	}

	trimmed := strings.TrimSpace(remoteAddr)
	if parsed := net.ParseIP(trimmed); parsed != nil {
		return parsed, parsed.String()
	}
	return nil, trimmed
}
