package dav

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/jw6ventures/calcard/internal/auth"
	"github.com/jw6ventures/calcard/internal/config"
	"github.com/jw6ventures/calcard/internal/store"
)

// behindProxy is a request as a TLS-terminating reverse proxy delivers it: the
// proxy rewrote Host to the name it reaches CalCard by, and says through
// X-Forwarded-Proto that the client used https.
func behindProxy(method, target, body string) *http.Request {
	var req *http.Request
	if body == "" {
		req = httptest.NewRequest(method, target, nil)
	} else {
		req = httptest.NewRequest(method, target, strings.NewReader(body))
	}
	req.Host = "calcard.internal:8080"
	req.RemoteAddr = "192.0.2.7:41234"
	req.Header.Set("X-Forwarded-Proto", "https")
	return req
}

func proxiedConfig() *config.Config {
	return &config.Config{BaseURL: "https://dav.example.com", TrustedProxies: []string{"192.0.2.0/24"}}
}

// onTheLAN is a request from a LAN client that reaches CalCard directly over
// plain HTTP, on a server whose base URL publishes its https name: a split
// horizon, where both names are this server's.
func onTheLAN(method, target, body string) (*http.Request, *config.Config) {
	var req *http.Request
	if body == "" {
		req = httptest.NewRequest(method, target, nil)
	} else {
		req = httptest.NewRequest(method, target, strings.NewReader(body))
	}
	req.Host = "192.168.1.5:8080"
	req.RemoteAddr = "192.168.1.20:5555"
	return req, &config.Config{BaseURL: "https://cal.example.com", TrustedProxies: []string{"10.0.0.0/8"}}
}

// An absolute URI names this server when its host and port match the Host the
// request reached, or the authority APP_BASE_URL publishes. RFC 3986 §6.2.2.1
// and §6.2.3 make the host case-insensitive and an absent port the scheme's
// default, and an IPv6 literal has more than one spelling. The scheme itself is
// not compared: without TLS or a trusted X-Forwarded-Proto this server cannot
// tell which one the client used, so a Host without a port stands for either
// default port.
func TestIsLocalOriginComparesAuthorities(t *testing.T) {
	tests := []struct {
		name      string
		baseURL   string
		target    string
		host      string
		scheme    string
		authority string
		want      bool
	}{
		{name: "request authority", target: "http://example.com/dav/", scheme: "http", authority: "example.com", want: true},
		{name: "default http port spelled out", target: "http://example.com/dav/", scheme: "http", authority: "example.com:80", want: true},
		{name: "empty port", target: "http://example.com/dav/", scheme: "http", authority: "example.com:", want: true},
		{name: "port with leading zeros", target: "http://example.com/dav/", scheme: "http", authority: "example.com:00080", want: true},
		{name: "upper-case scheme and host", target: "http://example.com/dav/", scheme: "HTTP", authority: "EXAMPLE.com", want: true},
		{name: "fully qualified host", target: "http://example.com/dav/", scheme: "http", authority: "example.com.", want: true},
		{name: "host with two trailing dots", target: "http://example.com/dav/", scheme: "http", authority: "example.com..", want: false},
		{name: "https on a Host without a port", target: "http://example.com/dav/", scheme: "https", authority: "example.com", want: true},
		{name: "port 443 on a Host without a port", target: "http://example.com/dav/", scheme: "http", authority: "example.com:443", want: true},
		{name: "another port", target: "http://example.com/dav/", scheme: "http", authority: "example.com:8080", want: false},
		{name: "default port on a Host with another", target: "http://example.com:8080/dav/", scheme: "http", authority: "example.com", want: false},
		{name: "default port on a Host naming it", target: "https://example.com:443/dav/", scheme: "https", authority: "example.com", want: true},
		{name: "another host", target: "http://example.com/dav/", scheme: "http", authority: "other.example", want: false},
		{name: "userinfo", target: "http://example.com/dav/", scheme: "http", authority: "user@example.com", want: false},
		{name: "non-numeric port", target: "http://example.com/dav/", scheme: "http", authority: "example.com:http", want: false},
		{name: "non-http scheme", target: "http://example.com/dav/", scheme: "ftp", authority: "example.com", want: false},
		{name: "empty authority", target: "http://example.com/dav/", scheme: "http", authority: "", want: false},
		{name: "network-path reference", target: "http://example.com/dav/", scheme: "", authority: "example.com", want: true},
		{name: "network-path reference on another port", target: "http://example.com/dav/", scheme: "", authority: "example.com:8443", want: false},
		{name: "IPv6 literal", target: "http://[::1]:8080/dav/", scheme: "http", authority: "[::1]:8080", want: true},
		{name: "IPv6 literal spelled long", target: "http://[::1]:8080/dav/", scheme: "http", authority: "[0:0:0:0:0:0:0:1]:8080", want: true},
		{name: "IPv6 literal upper-case", target: "http://[fe80::a]:8080/dav/", scheme: "http", authority: "[FE80::A]:8080", want: true},
		{name: "IPv6 zone as url.Parse decodes it", target: "http://example.com/dav/", host: "[fe80::1%25eth0]:8080", scheme: "http", authority: "[fe80::1%eth0]:8080", want: true},
		{name: "another IPv6 literal", target: "http://[::1]:8080/dav/", scheme: "http", authority: "[::2]:8080", want: false},
		{name: "base URL authority", baseURL: "https://dav.example.com", target: "http://calcard.internal:8080/dav/", scheme: "https", authority: "dav.example.com", want: true},
		{name: "base URL default port spelled out", baseURL: "https://dav.example.com", target: "http://calcard.internal:8080/dav/", scheme: "https", authority: "dav.example.com:443", want: true},
		{name: "base URL host on port 80", baseURL: "https://dav.example.com", target: "http://calcard.internal:8080/dav/", scheme: "http", authority: "dav.example.com", want: false},
		{name: "base URL with explicit port", baseURL: "https://dav.example.com:8443/", target: "http://calcard.internal:8080/dav/", scheme: "https", authority: "dav.example.com:8443", want: true},
		{name: "base URL host without its port", baseURL: "https://dav.example.com:8443/", target: "http://calcard.internal:8080/dav/", scheme: "https", authority: "dav.example.com", want: false},
		{name: "base URL without a Host header", baseURL: "https://dav.example.com", target: "/dav/", host: "-", scheme: "https", authority: "dav.example.com", want: true},
		{name: "unparsable base URL", baseURL: "://", target: "http://example.com/dav/", scheme: "http", authority: "example.com", want: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			h := &DavServer{}
			if tt.baseURL != "" {
				h.cfg = &config.Config{BaseURL: tt.baseURL}
			}
			req := httptest.NewRequest(http.MethodGet, tt.target, nil)
			switch tt.host {
			case "":
			case "-":
				req.Host = ""
			default:
				req.Host = tt.host
			}
			if got := h.isLocalOrigin(req, tt.scheme, tt.authority); got != tt.want {
				t.Fatalf("isLocalOrigin(%q, %q) = %v, want %v", tt.scheme, tt.authority, got, tt.want)
			}
		})
	}
}

// RFC 4918 §9.8.5 and §9.9.4 answer a Destination on another server with 502.
// One that names this server through either of its authorities, with or
// without the default port, and over whichever scheme, is local. One whose
// authority is empty names no server at all and is malformed.
func TestDestinationNamingThisServerIsLocal(t *testing.T) {
	plain := func() (*http.Request, *config.Config) {
		return httptest.NewRequest("MOVE", "/dav/calendars/1/", nil), nil
	}
	proxied := func() (*http.Request, *config.Config) {
		return behindProxy("MOVE", "/dav/calendars/1/", ""), proxiedConfig()
	}
	lan := func() (*http.Request, *config.Config) {
		return onTheLAN("MOVE", "/dav/calendars/1/", "")
	}
	const (
		local   = "local"
		foreign = "foreign"
		invalid = "invalid"
	)
	tests := []struct {
		name        string
		setup       func() (*http.Request, *config.Config)
		destination string
		want        string
	}{
		{name: "client writes https, server sees http", setup: plain, destination: "https://example.com/dav/calendars/moved", want: local},
		{name: "port 443 on plain http", setup: plain, destination: "http://example.com:443/dav/calendars/moved", want: local},
		{name: "empty authority", setup: plain, destination: "http:///dav/calendars/moved", want: invalid},
		{name: "empty network-path authority", setup: plain, destination: "///dav/calendars/moved", want: invalid},
		{name: "non-http scheme", setup: plain, destination: "ftp://example.com/dav/calendars/moved", want: foreign},
		{name: "public origin", setup: proxied, destination: "https://dav.example.com/dav/calendars/moved", want: local},
		{name: "public origin with its port", setup: proxied, destination: "https://DAV.example.com:443/dav/calendars/moved", want: local},
		{name: "rewritten Host", setup: proxied, destination: "https://calcard.internal:8080/dav/calendars/moved", want: local},
		{name: "rewritten Host over http", setup: proxied, destination: "http://calcard.internal:8080/dav/calendars/moved", want: local},
		{name: "public host by network path", setup: proxied, destination: "//dav.example.com/dav/calendars/moved", want: local},
		{name: "absolute path", setup: proxied, destination: "/dav/calendars/moved", want: local},
		{name: "public host on port 80", setup: proxied, destination: "http://dav.example.com/dav/calendars/moved", want: foreign},
		{name: "public host on another port", setup: proxied, destination: "https://dav.example.com:8443/dav/calendars/moved", want: foreign},
		{name: "another host", setup: proxied, destination: "https://other.example/dav/calendars/moved", want: foreign},
		{name: "another host by network path", setup: proxied, destination: "//other.example/dav/calendars/moved", want: foreign},
		{name: "userinfo by network path", setup: proxied, destination: "//user@dav.example.com/dav/calendars/moved", want: foreign},
		{name: "split horizon, the origin the request came from", setup: lan, destination: "http://192.168.1.5:8080/dav/calendars/moved", want: local},
		{name: "split horizon, the published origin", setup: lan, destination: "https://cal.example.com/dav/calendars/moved", want: local},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req, cfg := tt.setup()
			h := &DavServer{cfg: cfg}
			req.Header.Set("Destination", tt.destination)

			got, _, err := h.parseDestinationHeader(req)
			switch tt.want {
			case local:
				if err != nil || got != "/dav/calendars/moved" {
					t.Fatalf("parseDestinationHeader() = %q, %v; want /dav/calendars/moved", got, err)
				}
			case foreign:
				if !errors.Is(err, errCrossAuthorityDestination) {
					t.Fatalf("error = %v, want %v", err, errCrossAuthorityDestination)
				}
			case invalid:
				if err == nil || errors.Is(err, errCrossAuthorityDestination) {
					t.Fatalf("error = %v, want a malformed Destination", err)
				}
			}
		})
	}
}

// RFC 4918 §10.6 spells Overwrite as "T" or "F", and RFC 5234 §2.3 makes a
// quoted literal in the grammar case-insensitive.
func TestOverwriteHeaderIsCaseInsensitive(t *testing.T) {
	tests := []struct {
		value     string
		overwrite bool
		valid     bool
	}{
		{value: "", overwrite: true, valid: true},
		{value: "T", overwrite: true, valid: true},
		{value: "t", overwrite: true, valid: true},
		{value: " t ", overwrite: true, valid: true},
		{value: "F", overwrite: false, valid: true},
		{value: "f", overwrite: false, valid: true},
		{value: "true", valid: false},
		{value: "yes", valid: false},
		{value: "FT", valid: false},
	}
	for _, tt := range tests {
		t.Run(tt.value, func(t *testing.T) {
			req := httptest.NewRequest("COPY", "/dav/calendars/1/a.ics", nil)
			req.Header.Set("Destination", "/dav/calendars/2/a.ics")
			req.Header.Set("Overwrite", tt.value)

			_, overwrite, err := (&DavServer{}).parseDestinationHeader(req)
			if !tt.valid {
				if err == nil {
					t.Fatalf("Overwrite %q accepted", tt.value)
				}
				return
			}
			if err != nil || overwrite != tt.overwrite {
				t.Fatalf("Overwrite %q = %v, %v; want %v", tt.value, overwrite, err, tt.overwrite)
			}
		})
	}
}

// A calendar-multiget DAV:href written with either of this server's
// authorities, with or without the default port, names the same resource as
// the path alone.
func TestCalendarMultigetResolvesAbsoluteHrefsNamingThisServer(t *testing.T) {
	bodyFor := func(href string) string {
		return `<?xml version="1.0" encoding="utf-8"?>
<C:calendar-multiget xmlns:D="DAV:" xmlns:C="urn:ietf:params:xml:ns:caldav">
  <D:prop><D:getetag/></D:prop>
  <D:href>` + href + `</D:href>
</C:calendar-multiget>`
	}
	tests := map[string]struct {
		cfg    *config.Config
		proxy  bool
		lan    bool
		target string
		href   string
		status int
	}{
		"public origin behind a proxy":         {cfg: proxiedConfig(), proxy: true, target: "/dav/calendars/1/", href: "https://dav.example.com/dav/calendars/1/standup.ics", status: http.StatusOK},
		"public origin with its port":          {cfg: proxiedConfig(), proxy: true, target: "/dav/calendars/1/", href: "https://dav.example.com:443/dav/calendars/1/standup.ics", status: http.StatusOK},
		"rewritten Host":                       {cfg: proxiedConfig(), proxy: true, target: "/dav/calendars/1/", href: "https://calcard.internal:8080/dav/calendars/1/standup.ics", status: http.StatusOK},
		"rewritten Host over http":             {cfg: proxiedConfig(), proxy: true, target: "/dav/calendars/1/", href: "http://calcard.internal:8080/dav/calendars/1/standup.ics", status: http.StatusOK},
		"public host on port 80":               {cfg: proxiedConfig(), proxy: true, target: "/dav/calendars/1/", href: "http://dav.example.com/dav/calendars/1/standup.ics", status: http.StatusNotFound},
		"another host behind a proxy":          {cfg: proxiedConfig(), proxy: true, target: "/dav/calendars/1/", href: "https://other.example/dav/calendars/1/standup.ics", status: http.StatusNotFound},
		"split horizon, the request's origin":  {lan: true, target: "/dav/calendars/1/", href: "http://192.168.1.5:8080/dav/calendars/1/standup.ics", status: http.StatusOK},
		"split horizon, the published origin":  {lan: true, target: "/dav/calendars/1/", href: "https://cal.example.com/dav/calendars/1/standup.ics", status: http.StatusOK},
		"request origin with its port":         {target: "http://example.com/dav/calendars/1/", href: "http://example.com:80/dav/calendars/1/standup.ics", status: http.StatusOK},
		"https href on a plain http request":   {target: "http://example.com/dav/calendars/1/", href: "https://example.com/dav/calendars/1/standup.ics", status: http.StatusOK},
		"request origin on another port":       {target: "http://example.com/dav/calendars/1/", href: "http://example.com:8080/dav/calendars/1/standup.ics", status: http.StatusNotFound},
		"request origin in upper-case":         {target: "http://example.com/dav/calendars/1/", href: "HTTP://EXAMPLE.COM/dav/calendars/1/standup.ics", status: http.StatusOK},
		"relative href without a Host":         {target: "/dav/calendars/1/", href: "standup.ics", status: http.StatusOK},
		"absolute path without a Host":         {target: "/dav/calendars/1/", href: "/dav/calendars/1/standup.ics", status: http.StatusOK},
		"absolute URI with an empty authority": {target: "/dav/calendars/1/", href: "http:///dav/calendars/1/standup.ics", status: http.StatusNotFound},
		"base URL origin without a proxy":      {cfg: &config.Config{BaseURL: "http://dav.example.com"}, target: "http://calcard.internal/dav/calendars/1/", href: "http://dav.example.com/dav/calendars/1/standup.ics", status: http.StatusOK},
		"base URL host on another port":        {cfg: &config.Config{BaseURL: "http://dav.example.com"}, target: "http://calcard.internal/dav/calendars/1/", href: "http://dav.example.com:8080/dav/calendars/1/standup.ics", status: http.StatusNotFound},
		"base URL path does not narrow host":   {cfg: &config.Config{BaseURL: "http://dav.example.com/calcard"}, target: "http://calcard.internal/dav/calendars/1/", href: "http://dav.example.com/dav/calendars/1/standup.ics", status: http.StatusOK},
	}
	for name, tt := range tests {
		t.Run(name, func(t *testing.T) {
			server := grammarTestServer()
			server.cfg = tt.cfg
			var req *http.Request
			switch {
			case tt.proxy:
				req = behindProxy("REPORT", tt.target, bodyFor(tt.href))
			case tt.lan:
				req, server.cfg = onTheLAN("REPORT", tt.target, bodyFor(tt.href))
			default:
				req = httptest.NewRequest("REPORT", tt.target, strings.NewReader(bodyFor(tt.href)))
				if !strings.HasPrefix(tt.target, "http") {
					req.Host = ""
				}
			}
			req = req.WithContext(auth.WithUser(req.Context(), &store.User{ID: 1}))
			rr := httptest.NewRecorder()
			server.Report(rr, req)

			ms := decodeMultistatus(t, rr)
			if len(ms.Responses) != 1 {
				t.Fatalf("responses = %d, want 1: %s", len(ms.Responses), rr.Body.String())
			}
			if tt.status == http.StatusOK {
				ms.Responses[0].assertPropstatNames(t, http.StatusOK, davQN("getetag"))
				return
			}
			ms.Responses[0].assertResponseStatus(t, tt.status)
		})
	}
}

// RFC 3744 §8.1.1 refuses an ACE whose principal the server does not
// recognise. A principal URL naming this server through either authority is
// one it does, and RFC 4918 §8.3 resolves a relative one against the
// Request-URI.
func TestACLPrincipalHrefNamingThisServerIsRecognized(t *testing.T) {
	tests := []struct {
		href       string
		wantStatus int
	}{
		{href: "/dav/principals/2/", wantStatus: http.StatusOK},
		{href: "https://dav.example.com/dav/principals/2/", wantStatus: http.StatusOK},
		{href: "https://dav.example.com:443/dav/principals/2/", wantStatus: http.StatusOK},
		{href: "https://calcard.internal:8080/dav/principals/2/", wantStatus: http.StatusOK},
		{href: "http://calcard.internal:8080/dav/principals/2/", wantStatus: http.StatusOK},
		{href: "//dav.example.com/dav/principals/2/", wantStatus: http.StatusOK},
		{href: "../../principals/2/", wantStatus: http.StatusOK},
		{href: "http://dav.example.com/dav/principals/2/", wantStatus: http.StatusForbidden},
		{href: "https://other.example/dav/principals/2/", wantStatus: http.StatusForbidden},
		{href: "//other.example/dav/principals/2/", wantStatus: http.StatusForbidden},
		{href: "/dav/principals/2/?x=1", wantStatus: http.StatusForbidden},
	}
	for _, tt := range tests {
		t.Run(tt.href, func(t *testing.T) {
			calendar := store.Calendar{ID: 5, UserID: 1, Name: "Work"}
			aclRepo := &fakeACLRepo{}
			h := NewDavServer(Options{Config: proxiedConfig(), Store: &store.Store{
				Calendars: &fakeCalendarRepo{
					accessible: []store.CalendarAccess{{Calendar: calendar, Editor: true}},
					calendars:  map[int64]*store.Calendar{5: &calendar},
				},
				ACLEntries: aclRepo,
			}})
			body := `<d:acl xmlns:d="DAV:"><d:ace><d:principal><d:href>` + tt.href +
				`</d:href></d:principal><d:grant><d:privilege><d:read/></d:privilege></d:grant></d:ace></d:acl>`
			req := behindProxy("ACL", "/dav/calendars/5/", body)
			req = req.WithContext(auth.WithUser(req.Context(), &store.User{ID: 1}))
			rr := httptest.NewRecorder()

			h.ServeHTTP(rr, req)

			if rr.Code != tt.wantStatus {
				t.Fatalf("ACL status = %d, want %d: %s", rr.Code, tt.wantStatus, rr.Body.String())
			}
			if tt.wantStatus == http.StatusForbidden {
				assertErrorConditions(t, rr, http.StatusForbidden, davQN("recognized-principal"))
				return
			}
			entries, err := aclRepo.ListByResource(context.Background(), "/dav/calendars/5")
			if err != nil {
				t.Fatal(err)
			}
			if len(entries) != 1 || entries[0].PrincipalHref != "/dav/principals/2/" {
				t.Fatalf("stored ACL entries = %#v", entries)
			}
		})
	}
}

// RFC 4918 §10.4.2: a tagged list applies to the resource its tag names. A tag
// naming this server through either authority submits its lock token for that
// resource; one naming another server submits nothing here, and its list is not
// read as an untagged one either.
func TestIfHeaderResourceTagsNameResourcesByAuthority(t *testing.T) {
	const destToken = "opaquelocktoken:dest-calendar-lock"
	tests := []struct {
		tag        string
		wantStatus int
	}{
		{tag: "/dav/calendars/6/renamed.ics", wantStatus: http.StatusNoContent},
		{tag: "https://dav.example.com/dav/calendars/6/renamed.ics", wantStatus: http.StatusNoContent},
		{tag: "https://dav.example.com:443/dav/calendars/6/renamed.ics", wantStatus: http.StatusNoContent},
		{tag: "https://calcard.internal:8080/dav/calendars/6/renamed.ics", wantStatus: http.StatusNoContent},
		{tag: "http://calcard.internal:8080/dav/calendars/6/renamed.ics", wantStatus: http.StatusNoContent},
		{tag: "https://other.example/dav/calendars/6/renamed.ics", wantStatus: http.StatusLocked},
		{tag: "http://dav.example.com/dav/calendars/6/renamed.ics", wantStatus: http.StatusLocked},
	}
	for _, tt := range tests {
		t.Run(tt.tag, func(t *testing.T) {
			owner := &store.User{ID: 1, PrimaryEmail: "owner@example.com"}
			now := store.Now()
			events := &fakeEventRepo{events: map[string]*store.Event{
				"5:alice": {CalendarID: 5, UID: "alice", ResourceName: "alice", RawICAL: buildCalendarObject(buildVEvent("alice", "SUMMARY:Alice")), ETag: "etag-a", LastModified: now},
				"6:bob":   {CalendarID: 6, UID: "bob", ResourceName: "renamed", RawICAL: buildCalendarObject(buildVEvent("bob", "SUMMARY:Bob")), ETag: "etag-b", LastModified: now},
			}}
			h := NewDavServer(Options{Config: proxiedConfig(), Store: &store.Store{
				Calendars: &fakeCalendarRepo{
					accessible: []store.CalendarAccess{
						{Calendar: store.Calendar{ID: 5, UserID: owner.ID, Name: "Source", UpdatedAt: now}, Editor: true},
						{Calendar: store.Calendar{ID: 6, UserID: owner.ID, Name: "Destination", UpdatedAt: now}, Editor: true},
					},
					calendars: map[int64]*store.Calendar{
						5: {ID: 5, UserID: owner.ID, Name: "Source", UpdatedAt: now},
						6: {ID: 6, UserID: owner.ID, Name: "Destination", UpdatedAt: now},
					},
				},
				Events:            events,
				CalendarTransfers: events,
				Locks: &fakeLockRepo{locks: map[string]*store.Lock{
					destToken: {
						Token: destToken, ResourcePath: "/dav/calendars/6/renamed", UserID: owner.ID,
						LockScope: "exclusive", LockType: "write", Depth: "0", ExpiresAt: time.Now().Add(time.Hour),
					},
				}},
				ACLEntries: &fakeACLRepo{},
			}})
			req := behindProxy("COPY", "/dav/calendars/5/alice.ics", "")
			req.Header.Set("Destination", "/dav/calendars/6/renamed.ics")
			req.Header.Set("Overwrite", "T")
			req.Header.Set("If", "<"+tt.tag+"> (<"+destToken+">)")
			req = req.WithContext(auth.WithUser(req.Context(), owner))
			rr := httptest.NewRecorder()

			h.ServeHTTP(rr, req)

			if rr.Code != tt.wantStatus {
				t.Fatalf("COPY status = %d, want %d: %s", rr.Code, tt.wantStatus, rr.Body.String())
			}
		})
	}
}

// A resource tag names a resource by its path. The Request-URI's query plays
// no part in it, unlike the DAV:href of a multiget, and a relative tag resolves
// against the Request-URI.
func TestIfHeaderResourceTagsIgnoreTheRequestQuery(t *testing.T) {
	const token = "opaquelocktoken:t"
	tests := []struct {
		name, target, tag, path string
	}{
		{name: "request with a query", target: "/dav/calendars/6/renamed.ics?x=1", tag: "/dav/calendars/6/renamed.ics", path: "/dav/calendars/6/renamed.ics"},
		{name: "relative tag", target: "/dav/calendars/6/renamed.ics", tag: "renamed.ics", path: "/dav/calendars/6/renamed.ics"},
		{name: "relative tag under a collection without its slash", target: "/dav/calendars/6", tag: "x.ics", path: "/dav/calendars/6/x.ics"},
		{name: "percent-encoded tag", target: "/dav/calendars/6/a%20b.ics", tag: "/dav/calendars/6/a%20b.ics", path: "/dav/calendars/6/a b.ics"},
		{name: "https tag on a plain http request", target: "/dav/calendars/6/x.ics", tag: "https://example.com/dav/calendars/6/x.ics", path: "/dav/calendars/6/x.ics"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req := httptest.NewRequest(http.MethodPut, tt.target, nil)
			req.Header.Set("If", "<"+tt.tag+"> (<"+token+">)")
			got := (&DavServer{}).parseIfHeader(req).lockTokensForPaths(tt.path)
			if len(got) != 1 || got[0] != token {
				t.Fatalf("tokens for %s = %v, want [%s]", tt.path, got, token)
			}
		})
	}
}

func BenchmarkResolveDAVHrefReference(b *testing.B) {
	h := &DavServer{cfg: &config.Config{BaseURL: "https://cal.example.com", TrustedProxies: []string{"10.0.0.0/8", "172.16.0.0/12", "192.168.0.0/16", "fd00::/8"}}}
	req := httptest.NewRequest("REPORT", "/dav/calendars/1/", nil)
	req.Host = "cal.example.com"
	req.RemoteAddr = "10.0.0.2:1"
	req.Header.Set("X-Forwarded-Proto", "https")
	for name, href := range map[string]string{
		"absolute": "https://cal.example.com/dav/calendars/1/x.ics",
		"relative": "/dav/calendars/1/x.ics",
	} {
		b.Run(name, func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				h.resolveDAVHrefReference(href, "/dav/calendars/1/", req)
			}
		})
	}
}
