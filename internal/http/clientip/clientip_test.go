package clientip

import (
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestWithPeerAddrAndPeerAddr(t *testing.T) {
	req := httptest.NewRequest(http.MethodGet, "/", nil)
	req.RemoteAddr = "203.0.113.9:1234"

	if got := PeerAddr(req); got != "203.0.113.9:1234" {
		t.Fatalf("PeerAddr() without a recorded peer = %q, want RemoteAddr", got)
	}

	req = req.WithContext(WithPeerAddr(req.Context(), "10.1.2.3:4567"))
	if got := PeerAddr(req); got != "10.1.2.3:4567" {
		t.Fatalf("PeerAddr() = %q, want the recorded peer", got)
	}

	if got := PeerAddr(nil); got != "" {
		t.Fatalf("PeerAddr(nil) = %q, want empty", got)
	}
}

func TestTrustedProxiesClientIP(t *testing.T) {
	tests := []struct {
		name    string
		trusted TrustedProxies
		req     *http.Request
		want    string
	}{
		{
			name:    "xff returns first untrusted hop from right",
			trusted: NewTrustedProxies([]string{"10.0.0.0/8"}),
			req: func() *http.Request {
				r := httptest.NewRequest(http.MethodGet, "/", nil)
				r.Header.Set("X-Forwarded-For", " 198.51.100.1 , 203.0.113.9 , 10.0.0.1 ")
				r.RemoteAddr = "10.1.1.1:1234"
				return r
			}(),
			want: "203.0.113.9",
		},
		{
			name:    "xff skips trusted hops",
			trusted: NewTrustedProxies([]string{"10.0.0.0/8"}),
			req: func() *http.Request {
				r := httptest.NewRequest(http.MethodGet, "/", nil)
				r.Header.Set("X-Forwarded-For", "198.51.100.1, 10.0.0.2, 10.0.0.3")
				r.RemoteAddr = "10.1.1.1:1234"
				return r
			}(),
			want: "198.51.100.1",
		},
		{
			name:    "x-real-ip",
			trusted: NewTrustedProxies([]string{"10.0.0.0/8"}),
			req: func() *http.Request {
				r := httptest.NewRequest(http.MethodGet, "/", nil)
				r.Header.Set("X-Real-IP", " 198.51.100.2 ")
				r.RemoteAddr = "10.1.1.1:1234"
				return r
			}(),
			want: "198.51.100.2",
		},
		{
			name:    "empty trusted proxies trusts forwarded headers",
			trusted: TrustedProxies{},
			req: func() *http.Request {
				r := httptest.NewRequest(http.MethodGet, "/", nil)
				r.Header.Set("X-Forwarded-For", "198.51.100.9")
				r.RemoteAddr = "203.0.113.10:1234"
				return r
			}(),
			want: "198.51.100.9",
		},
		{
			name:    "untrusted remote ignores forwarded headers",
			trusted: NewTrustedProxies([]string{"10.0.0.0/8"}),
			req: func() *http.Request {
				r := httptest.NewRequest(http.MethodGet, "/", nil)
				r.Header.Set("X-Forwarded-For", "198.51.100.3")
				r.Header.Set("X-Real-IP", "198.51.100.4")
				r.RemoteAddr = "203.0.113.11:1234"
				return r
			}(),
			want: "203.0.113.11",
		},
		{
			name:    "remote addr fallback",
			trusted: NewTrustedProxies([]string{"10.0.0.0/8"}),
			req: func() *http.Request {
				r := httptest.NewRequest(http.MethodGet, "/", nil)
				r.RemoteAddr = "198.51.100.3:1234"
				return r
			}(),
			want: "198.51.100.3",
		},
		{
			name:    "invalid forwarded header falls back to remote addr host",
			trusted: TrustedProxies{},
			req: func() *http.Request {
				r := httptest.NewRequest(http.MethodGet, "/", nil)
				r.Header.Set("X-Forwarded-For", "not-an-ip")
				r.RemoteAddr = "198.51.100.4:4321"
				return r
			}(),
			want: "198.51.100.4",
		},
		{
			name:    "peer recorded through WithPeerAddr, not RemoteAddr",
			trusted: NewTrustedProxies([]string{"10.0.0.0/8"}),
			req: func() *http.Request {
				r := httptest.NewRequest(http.MethodGet, "/", nil)
				// RemoteAddr already names a client, as it does once the
				// forwarded-address middleware has run.
				r.RemoteAddr = "203.0.113.9"
				r.Header.Set("X-Forwarded-For", "203.0.113.9, 198.51.100.7, 10.1.2.3")
				return r.WithContext(WithPeerAddr(r.Context(), "10.1.2.3:4567"))
			}(),
			want: "198.51.100.7",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.trusted.ClientIP(tt.req); got != tt.want {
				t.Fatalf("ClientIP() = %q, want %q", got, tt.want)
			}
		})
	}
}

// Everything left of the hop the first trusted proxy appended is whatever the
// client sent, so the walk must never step past a hop it cannot read: doing so
// hands the attribution to an address the client chose.
func TestClientIPNeverWalksPastAForwardedHopItCannotRead(t *testing.T) {
	trusted := NewTrustedProxies([]string{"10.0.0.0/8"})

	tests := []struct {
		name      string
		forwarded []string
		realIP    string
		want      string
	}{
		{
			name:      "an unreadable hop ends the walk at the trusted hop to its right",
			forwarded: []string{"192.0.2.66, not-an-ip, 10.0.0.1"},
			want:      "10.0.0.1",
		},
		{
			name:      "an unreadable rightmost hop leaves the peer",
			forwarded: []string{"192.0.2.66, not-an-ip"},
			want:      "10.1.1.1",
		},
		{
			name:      "an unreadable rightmost hop leaves the peer over X-Real-IP",
			forwarded: []string{"192.0.2.66, not-an-ip"},
			realIP:    "192.0.2.67",
			want:      "10.1.1.1",
		},
		{
			name:      "a wholly unreadable header leaves the peer over X-Real-IP",
			forwarded: []string{"not-an-ip"},
			realIP:    "192.0.2.67",
			want:      "10.1.1.1",
		},
		{
			// What nginx writes for a client on a unix socket.
			name:      "a unix socket hop leaves the peer over X-Real-IP",
			forwarded: []string{"192.0.2.66, unix:"},
			realIP:    "192.0.2.67",
			want:      "10.1.1.1",
		},
		{
			// What Squid writes when it withholds the client address.
			name:      "an unknown hop leaves the peer over X-Real-IP",
			forwarded: []string{"unknown"},
			realIP:    "192.0.2.67",
			want:      "10.1.1.1",
		},
		{
			name:      "a header naming no hops leaves X-Real-IP",
			forwarded: []string{" , "},
			realIP:    "198.51.100.2",
			want:      "198.51.100.2",
		},
		{
			name:      "an all-trusted chain past an unreadable hop stops at that hop",
			forwarded: []string{"10.9.9.9, not-an-ip, 10.0.0.2"},
			want:      "10.0.0.2",
		},
		{
			name:      "an all-trusted chain resolves to its leftmost hop",
			forwarded: []string{"10.0.0.5, 10.0.0.2, 10.0.0.3"},
			want:      "10.0.0.5",
		},
		{
			name:      "an IPv4 hop with a port",
			forwarded: []string{"192.0.2.66, 203.0.113.9:51234, 10.0.0.1"},
			want:      "203.0.113.9",
		},
		{
			name:      "a trusted hop with a port",
			forwarded: []string{"192.0.2.66, 203.0.113.9, 10.0.0.1:443"},
			want:      "203.0.113.9",
		},
		{
			name:      "a bracketed IPv6 hop with a port",
			forwarded: []string{"192.0.2.66, [2001:db8::9]:51234"},
			want:      "2001:db8::9",
		},
		{
			name:      "a bracketed IPv6 hop without a port",
			forwarded: []string{"192.0.2.66, [2001:db8::9]"},
			want:      "2001:db8::9",
		},
		{
			name:      "a bare IPv6 hop",
			forwarded: []string{"192.0.2.66, 2001:db8::9"},
			want:      "2001:db8::9",
		},
		{
			name:      "an IPv6 hop with a zone is unreadable",
			forwarded: []string{"192.0.2.66, fe80::1%eth0, 10.0.0.1"},
			want:      "10.0.0.1",
		},
		{
			name:      "a quoted hop is unreadable",
			forwarded: []string{`192.0.2.66, "203.0.113.9", 10.0.0.1`},
			want:      "10.0.0.1",
		},
		{
			name:      "a host name hop is unreadable",
			forwarded: []string{"192.0.2.66, proxy.example:80, 10.0.0.1"},
			want:      "10.0.0.1",
		},
		{
			name:      "an obfuscated hop is unreadable",
			forwarded: []string{"192.0.2.66, unknown, 10.0.0.1"},
			want:      "10.0.0.1",
		},
		{
			// RFC 9110 §5.6.1: empty list elements carry nothing and are ignored.
			name:      "empty elements are not hops",
			forwarded: []string{" , 192.0.2.66, 203.0.113.9,, 10.0.0.1 , "},
			want:      "203.0.113.9",
		},
		{
			// RFC 9110 §5.3: field lines of one name combine, in order, into
			// a single comma-separated list.
			name:      "a hop appended on its own field line",
			forwarded: []string{"192.0.2.66", "203.0.113.9"},
			want:      "203.0.113.9",
		},
		{
			name:      "a trusted hop appended on its own field line",
			forwarded: []string{"192.0.2.66, 203.0.113.9", "10.0.0.1"},
			want:      "203.0.113.9",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := httptest.NewRequest(http.MethodGet, "/", nil)
			r.RemoteAddr = "10.1.1.1:1234"
			for _, line := range tt.forwarded {
				r.Header.Add("X-Forwarded-For", line)
			}
			if tt.realIP != "" {
				r.Header.Set("X-Real-IP", tt.realIP)
			}
			if got := trusted.ClientIP(r); got != tt.want {
				t.Fatalf("ClientIP() with X-Forwarded-For %q = %q, want %q", tt.forwarded, got, tt.want)
			}
		})
	}
}

func TestParseTrustedProxies(t *testing.T) {
	trusted := parseTrustedProxies([]string{"10.0.0.0/8", "127.0.0.1", "2001:db8::1", "bad-value"})
	if len(trusted) != 3 {
		t.Fatalf("trusted proxy count = %d", len(trusted))
	}
}

func TestTrustedProxiesAllowsPeer(t *testing.T) {
	tests := []struct {
		name       string
		remoteAddr string
		trusted    []string
		want       bool
	}{
		// An unconfigured deployment keeps the documented posture: every peer's
		// forwarded headers are honoured, and the config loader warns about it.
		{name: "no configured proxies trusts every peer", remoteAddr: "203.0.113.9:4567", want: true},
		{name: "a peer inside the CIDR", remoteAddr: "10.1.2.3:4567", trusted: []string{"10.0.0.0/8"}, want: true},
		{name: "a peer outside the CIDR", remoteAddr: "203.0.113.9:4567", trusted: []string{"10.0.0.0/8"}},
		{name: "a bare IP entry", remoteAddr: "192.0.2.7:1234", trusted: []string{"192.0.2.7"}, want: true},
		{name: "an address with no port", remoteAddr: "10.1.2.3", trusted: []string{"10.0.0.0/8"}, want: true},
		{name: "an unparseable address", remoteAddr: "not-an-address", trusted: []string{"10.0.0.0/8"}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := NewTrustedProxies(tt.trusted).AllowsPeer(tt.remoteAddr); got != tt.want {
				t.Fatalf("NewTrustedProxies(%v).AllowsPeer(%q) = %v, want %v", tt.trusted, tt.remoteAddr, got, tt.want)
			}
		})
	}
}
