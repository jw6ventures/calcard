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

	if hops := forwardedHops(r.Header.Values("X-Forwarded-For")); len(hops) > 0 {
		if clientIP := forwardedClientIP(hops, remoteIP, t.nets); clientIP != nil {
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

// forwardedHops lists the X-Forwarded-For entries in order, across every field
// line. A proxy may append its hop as a line of its own; RFC 9110 §5.3 reads
// the lines, in order, as one list, and Header.Get sees only the first, which
// may be one the client sent. Empty elements carry nothing (RFC 9110 §5.6.1).
func forwardedHops(lines []string) []string {
	var hops []string
	for _, line := range lines {
		for _, hop := range strings.Split(line, ",") {
			if hop = strings.TrimSpace(hop); hop != "" {
				hops = append(hops, hop)
			}
		}
	}
	return hops
}

// RemoteAddr is the value r.RemoteAddr takes once the client is resolved: the
// address ClientIP attributes the request to, or the peer's own address, port
// included, when that client is the peer.
func (t TrustedProxies) RemoteAddr(r *http.Request) string {
	peer := PeerAddr(r)
	_, peerHost := parseRemoteAddr(peer)
	if client := t.ClientIP(r); client != peerHost {
		return client
	}
	return peer
}

func forwardedClientIP(hops []string, peer net.IP, trusted []*net.IPNet) net.IP {
	if len(trusted) == 0 {
		for _, hop := range hops {
			if parsed := net.ParseIP(hop); parsed != nil {
				return parsed
			}
		}
		return nil
	}

	// The trusted peer wrote the rightmost hop. If that hop is unreadable the
	// peer is the nearest address a trusted proxy vouches for, and X-Real-IP is
	// not consulted: a proxy that writes X-Forwarded-For need not overwrite it,
	// so it may be whatever the client sent.
	nearestTrusted := peer
	for i := len(hops) - 1; i >= 0; i-- {
		ip := parseForwardedHop(hops[i])
		// A hop that cannot be read is not a trusted proxy, so nothing to its
		// left is known to have been written by one: stepping past it would
		// let the client pick the address by sending one ahead of it.
		if ip == nil {
			return nearestTrusted
		}
		if !isTrustedProxy(ip, trusted) {
			return ip
		}
		nearestTrusted = ip
	}
	return nearestTrusted
}

// parseForwardedHop reads one X-Forwarded-For entry in the forms proxies
// write: an address, optionally with a port, and an IPv6 address optionally in
// brackets. A host name, a zoned address, or a quoted or obfuscated RFC 7239
// node reads as nil.
func parseForwardedHop(hop string) net.IP {
	if ip := net.ParseIP(hop); ip != nil {
		return ip
	}
	if host, _, err := net.SplitHostPort(hop); err == nil {
		return net.ParseIP(host)
	}
	if inner, ok := strings.CutPrefix(hop, "["); ok {
		if inner, ok = strings.CutSuffix(inner, "]"); ok {
			return net.ParseIP(inner)
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
