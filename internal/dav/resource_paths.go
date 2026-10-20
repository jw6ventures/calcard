package dav

import (
	"context"
	"errors"
	"net/http"
	"net/netip"
	"net/url"
	"path"
	"strconv"
	"strings"

	"github.com/jw6ventures/calcard/internal/store"
)

type davPathDomain uint8

const (
	davPathUnknown davPathDomain = iota
	davPathCalendar
	davPathAddressBook
)

type davTarget struct {
	CleanPath         string
	Domain            davPathDomain
	CollectionSegment string
	ResourceName      string
	Resource          bool
	Valid             bool
}

// parseDAVTarget and parsedDAVTarget take a decoded path, such as
// r.URL.Path; a DAV:href from a body goes through normalizeDAVHref first.
func parseDAVTarget(rawPath string) davTarget {
	cleanPath := cleanDAVPath(rawPath)
	target := davTarget{CleanPath: cleanPath}
	for _, candidate := range []struct {
		prefix string
		domain davPathDomain
	}{
		{prefix: calendarPrefix, domain: davPathCalendar},
		{prefix: addressBookPrefix, domain: davPathAddressBook},
	} {
		if cleanPath == candidate.prefix {
			target.Domain = candidate.domain
			target.Valid = true
			return target
		}
		if !strings.HasPrefix(cleanPath, candidate.prefix+"/") {
			continue
		}
		target.Domain = candidate.domain
		parts := strings.Split(strings.TrimPrefix(cleanPath, candidate.prefix+"/"), "/")
		if len(parts) == 0 || parts[0] == "" {
			return target
		}
		target.CollectionSegment = parts[0]
		if len(parts) == 1 {
			target.Valid = true
			return target
		}
		if len(parts) != 2 || parts[1] == "" {
			return target
		}
		target.ResourceName = strings.TrimSuffix(parts[1], path.Ext(parts[1]))
		if target.ResourceName == "" {
			return target
		}
		target.Resource = true
		target.Valid = true
		return target
	}
	return target
}

func parsedDAVTarget(ctx context.Context, rawPath string) davTarget {
	cleanPath := cleanDAVPath(rawPath)
	if state := davRequestStateFromContext(ctx); state != nil {
		if target, ok := state.primaryDAVTarget(cleanPath); ok {
			return target
		}
	}
	return parseDAVTarget(cleanPath)
}

func (h *DavServer) resolveAddressBookID(ctx context.Context, user *store.User, segment string) (int64, bool, error) {
	if segment == "" {
		return 0, false, nil
	}
	if id, err := strconv.ParseInt(segment, 10, 64); err == nil {
		return id, true, nil
	}
	if user == nil {
		return 0, false, store.ErrNotFound
	}
	key := collectionResolutionKey{userID: user.ID, prefix: addressBookPrefix, segment: segment}
	if state := davRequestStateFromContext(ctx); state != nil {
		if result, ok := state.collectionResolution(key); ok {
			return result.id, result.ok, result.err
		}
	}
	book, err := h.loadAddressBookByName(ctx, user, segment)
	result := collectionResolutionResult{}
	if err != nil {
		if errors.Is(err, errAmbiguousAddressBook) {
			result.err = errAmbiguousAddressBook
		} else if errors.Is(err, store.ErrNotFound) {
			result.err = store.ErrNotFound
		} else {
			return 0, false, err
		}
	} else {
		result.id = book.ID
		result.ok = true
	}
	if state := davRequestStateFromContext(ctx); state != nil {
		state.putCollectionResolution(key, result)
	}
	return result.id, result.ok, result.err
}

func (h *DavServer) resolveCalendarID(ctx context.Context, user *store.User, segment string) (int64, bool, error) {
	if segment == "" {
		return 0, false, nil
	}
	if id, err := strconv.ParseInt(segment, 10, 64); err == nil {
		return id, true, nil
	}
	if h.store == nil || h.store.Calendars == nil {
		return 0, false, nil
	}
	userID := int64(0)
	if user != nil {
		userID = user.ID
	}
	key := collectionResolutionKey{userID: userID, prefix: calendarPrefix, segment: segment}
	if state := davRequestStateFromContext(ctx); state != nil {
		if result, ok := state.collectionResolution(key); ok {
			return result.id, result.ok, result.err
		}
	}
	cal, err := h.loadCalendarByName(ctx, user, segment)
	result := collectionResolutionResult{}
	if err != nil {
		if errors.Is(err, errAmbiguousCalendar) {
			result.err = errAmbiguousCalendar
		} else if errors.Is(err, store.ErrNotFound) {
			result.err = store.ErrNotFound
		} else {
			return 0, false, err
		}
	} else {
		result.id = cal.ID
		result.ok = true
	}
	if state := davRequestStateFromContext(ctx); state != nil {
		state.putCollectionResolution(key, result)
	}
	return result.id, result.ok, result.err
}

func (h *DavServer) parseCalendarResourcePath(ctx context.Context, user *store.User, rawPath string) (int64, string, bool, error) {
	target := parsedDAVTarget(ctx, rawPath)
	if !target.Valid || target.Domain != davPathCalendar || !target.Resource {
		return 0, "", false, nil
	}
	id, ok, err := h.resolveCalendarID(ctx, user, target.CollectionSegment)
	if err != nil {
		if errors.Is(err, errAmbiguousCalendar) {
			return 0, target.ResourceName, true, errAmbiguousCalendar
		}
		if errors.Is(err, store.ErrNotFound) {
			return 0, target.ResourceName, true, err
		}
		return 0, "", false, err
	}
	if !ok {
		return 0, target.ResourceName, true, store.ErrNotFound
	}
	return id, target.ResourceName, true, nil
}

func parseAddressBookResourceSegments(rawPath string) (string, string, bool) {
	target := parseDAVTarget(rawPath)
	if !target.Valid || target.Domain != davPathAddressBook || !target.Resource {
		return "", "", false
	}
	return target.CollectionSegment, target.ResourceName, true
}

func (h *DavServer) parseAddressBookResourcePath(ctx context.Context, user *store.User, rawPath string) (int64, string, bool, error) {
	target := parsedDAVTarget(ctx, rawPath)
	if !target.Valid || target.Domain != davPathAddressBook || !target.Resource {
		return 0, "", false, nil
	}
	id, ok, err := h.resolveAddressBookID(ctx, user, target.CollectionSegment)
	if err != nil {
		if errors.Is(err, errAmbiguousAddressBook) {
			return 0, target.ResourceName, true, errAmbiguousAddressBook
		}
		if errors.Is(err, store.ErrNotFound) {
			return 0, target.ResourceName, true, err
		}
		return 0, "", false, err
	}
	if !ok {
		return 0, target.ResourceName, true, store.ErrNotFound
	}
	return id, target.ResourceName, true, nil
}

// parseResourcePath extracts the numeric collection ID and resource name from a
// percent-encoded DAV:href, absolute or not. The boolean reports whether the
// href matched the expected prefix and named both parts.
func parseResourcePath(href, prefix string) (int64, string, bool) {
	target := parseDAVTarget(normalizeDAVHref(href))
	wantedDomain := davPathUnknown
	switch prefix {
	case calendarPrefix:
		wantedDomain = davPathCalendar
	case addressBookPrefix:
		wantedDomain = davPathAddressBook
	}
	if !target.Valid || !target.Resource || target.Domain != wantedDomain {
		return 0, "", false
	}
	id, err := strconv.ParseInt(target.CollectionSegment, 10, 64)
	if err != nil {
		return 0, "", false
	}
	return id, target.ResourceName, true
}

// normalizeDAVHref turns a percent-encoded href into the decoded, cleaned path
// it names. It must not be given a path that is already decoded: a resource
// name containing "%" would be decoded a second time.
func normalizeDAVHref(raw string) string {
	trimmed := strings.TrimSpace(raw)
	if trimmed == "" {
		return ""
	}
	if u, err := url.Parse(trimmed); err == nil {
		if u.Path != "" {
			trimmed = u.Path
		}
	}
	cleaned := path.Clean(trimmed)
	if cleaned == "." {
		cleaned = "/"
	}
	if !strings.HasPrefix(cleaned, "/") {
		cleaned = "/" + strings.TrimPrefix(cleaned, "/")
	}
	return cleaned
}

// cleanDAVPath cleans an already-decoded path without decoding it again.
func cleanDAVPath(decoded string) string {
	if strings.TrimSpace(decoded) == "" {
		return ""
	}
	cleaned := path.Clean(decoded)
	if cleaned == "." {
		cleaned = "/"
	}
	if !strings.HasPrefix(cleaned, "/") {
		cleaned = "/" + cleaned
	}
	return cleaned
}

// escapeDAVPath percent-encodes each segment of a decoded path, as hrefs for
// object resources are written.
func escapeDAVPath(decoded string) string {
	segments := strings.Split(decoded, "/")
	for i := range segments {
		segments[i] = url.PathEscape(segments[i])
	}
	return strings.Join(segments, "/")
}

// isLocalOrigin reports whether an absolute URI or network-path reference with
// this scheme and authority names this server. Every URI reference a client
// sends to name a resource here -- a Destination, a DAV:href in a request body,
// an ACE principal -- is judged by it.
//
// Two authorities are local: the Host the request reached, and the one the
// configured base URL publishes. A reverse proxy commonly rewrites Host to the
// name it reaches CalCard by, while the client goes on writing the public name
// into the URIs it sends; the base URL is how this server knows that name.
//
// The scheme is not compared. Without TLS or a trusted X-Forwarded-Proto the
// server cannot tell which scheme the client used, and no property of this
// server depends on it, so the scheme serves only to supply a missing port. For
// the same reason a Host that names no port stands for either default port.
func (h *DavServer) isLocalOrigin(r *http.Request, scheme, rawAuthority string) bool {
	port := ""
	switch strings.ToLower(scheme) {
	case "":
	case "http":
		port = "80"
	case "https":
		port = "443"
	default:
		return false
	}
	candidate, ok := parseAuthority(rawAuthority)
	if !ok {
		return false
	}
	if candidate.port == "" {
		candidate.port = port
	}
	if requested, ok := parseAuthority(r.Host); ok && candidate.matches(requested) {
		return true
	}
	published, ok := h.publishedAuthority()
	return ok && candidate.matches(published)
}

// authority is a host and port in the normal form RFC 3986 §6.2.2 and §6.2.3
// give them, so that equal authorities compare equal however they were
// spelled. An empty port is one the URI left to its scheme's default.
type authority struct {
	host string
	port string
}

// matches compares two authorities, reading an empty port on either side as
// whichever of the two default ports the other names.
func (a authority) matches(b authority) bool {
	if a.host != b.host {
		return false
	}
	switch {
	case a.port == b.port:
		return true
	case a.port == "":
		return b.port == "80" || b.port == "443"
	case b.port == "":
		return a.port == "80" || a.port == "443"
	}
	return false
}

// parseAuthority normalizes a URI authority or Host header: the host is
// lower-cased and loses one trailing dot, an IP literal takes its canonical
// spelling without a zone, and the port loses its leading zeros. An authority
// carrying userinfo, naming no host, or with a port that is not a number names
// no server.
func parseAuthority(raw string) (authority, bool) {
	if raw == "" || strings.Contains(raw, "@") {
		return authority{}, false
	}
	host, port := raw, ""
	if colon := strings.LastIndexByte(raw, ':'); colon >= 0 && !strings.Contains(raw[colon:], "]") {
		host, port = raw[:colon], raw[colon+1:]
	}
	if strings.HasPrefix(host, "[") && strings.HasSuffix(host, "]") {
		addr, err := netip.ParseAddr(host[1 : len(host)-1])
		if err != nil || !addr.Is6() {
			return authority{}, false
		}
		host = addr.WithZone("").String()
	} else {
		if strings.ContainsAny(host, ":[]") {
			return authority{}, false
		}
		host = strings.ToLower(strings.TrimSuffix(host, "."))
	}
	if host == "" {
		return authority{}, false
	}
	if port != "" {
		number, err := strconv.ParseUint(port, 10, 16)
		if err != nil {
			return authority{}, false
		}
		port = strconv.FormatUint(number, 10)
	}
	return authority{host: host, port: port}, true
}

// publishedAuthority is the authority of the configured base URL, with its
// scheme's default port filled in. It is parsed once per server.
func (h *DavServer) publishedAuthority() (authority, bool) {
	if h == nil || h.cfg == nil {
		return authority{}, false
	}
	h.publishedOnce.Do(func() {
		base, err := url.Parse(h.cfg.BaseURL)
		if err != nil || base.User != nil {
			return
		}
		published, ok := parseAuthority(base.Host)
		if !ok {
			return
		}
		if published.port == "" {
			switch strings.ToLower(base.Scheme) {
			case "http":
				published.port = "80"
			case "https":
				published.port = "443"
			default:
				return
			}
		}
		h.published, h.publishedOK = published, true
	})
	return h.published, h.publishedOK
}

// xmlWhitespace is what XML 1.0 §2.3 defines whitespace to be. It is the exact
// set a serializer can introduce around element content, and therefore the
// exact set that can be stripped back off a DAV:href.
const xmlWhitespace = " \t\r\n"

// trimHrefFraming removes the indentation an XML serializer can put around
// element content, so a client that pretty-prints its request body still names
// the URI it wrote.
//
// Deliberately narrower than strings.TrimSpace, which strips every Unicode
// space -- U+00A0, U+2028, U+3000 and the rest. A URI is US-ASCII (RFC 3986
// §2), so none of those can be framing this server added; they are part of what
// the client sent, and discarding them would silently resolve an href to a
// resource the client did not name. Left in place, they simply fail to name any
// resource, which is the honest answer to a malformed href.
func trimHrefFraming(raw string) string {
	return strings.Trim(raw, xmlWhitespace)
}

// resolveDAVHrefReference resolves a DAV:href from a request body against
// basePath, or against the Request-URI when basePath is empty. An href naming
// another server, or a query other than the Request-URI's, names nothing here.
func (h *DavServer) resolveDAVHrefReference(rawHref, basePath string, r *http.Request) (*url.URL, bool) {
	resolved, ok := h.resolveLocalReference(rawHref, basePath, r)
	if !ok || resolved.RawQuery != r.URL.RawQuery || resolved.ForceQuery != r.URL.ForceQuery {
		return nil, false
	}
	return resolved, true
}

// resolveLocalReference resolves a URI reference a client sent to name a
// resource here (RFC 4918 §8.3), against basePath or, when that is empty, the
// Request-URI. It refuses userinfo, a fragment, and an authority naming another
// server, and leaves the query to the caller. Only the path and query of the
// result are meaningful: a relative reference keeps no scheme or authority.
func (h *DavServer) resolveLocalReference(rawRef, basePath string, r *http.Request) (*url.URL, bool) {
	rawRef = trimHrefFraming(rawRef)
	if r == nil || r.URL == nil || rawRef == "" {
		return nil, false
	}
	reference, err := url.Parse(rawRef)
	if err != nil || reference.User != nil || reference.Fragment != "" {
		return nil, false
	}
	if (reference.Scheme != "" || reference.Host != "") && !h.isLocalOrigin(r, reference.Scheme, reference.Host) {
		return nil, false
	}
	baseURI := url.URL{Path: r.URL.Path, RawPath: r.URL.RawPath, RawQuery: r.URL.RawQuery, ForceQuery: r.URL.ForceQuery}
	if basePath != "" {
		base, parseErr := url.Parse(basePath)
		if parseErr != nil || base.Scheme != "" || base.Host != "" || base.User != nil || base.Fragment != "" {
			return nil, false
		}
		baseURI.Path = base.Path
		baseURI.RawPath = base.RawPath
	}
	return baseURI.ResolveReference(reference), true
}

// davHrefReferenceBase is the base a DAV:href in a request body resolves
// against. RFC 3986 §5.2.2 discards the last segment of a base that does not end
// in "/", so a collection Request-URI written without its trailing slash needs
// the slash restored or a relative href would resolve above the collection
// holding the resource it names. A Request-URI naming an object resource wants
// that same discard and is left alone. requestPath is decoded; the base is
// returned percent-encoded, as url.Parse expects it.
func davHrefReferenceBase(requestPath string) string {
	cleanPath := cleanDAVPath(requestPath)
	base := escapeDAVPath(cleanPath)
	if target := parseDAVTarget(cleanPath); target.Valid && target.Resource {
		return base
	}
	return ensureCollectionHref(base)
}

// resolvedObjectHref is one DAV:href from a request body resolved against the
// Request-URI. Path is the percent-encoded path it names and is set whenever
// the href resolved at all, including when it names no object resource, so a
// response can still carry the best href the request gave for it.
type resolvedObjectHref struct {
	Path         string
	Segment      string
	ResourceName string
}

// resolveCalendarHrefForRequest and resolveAddressBookHrefForRequest resolve one
// DAV:href from a multiget body against the Request-URI and split the result
// into its collection segment and resource name. The bool reports whether the
// href names an object resource in that domain, not whether it resolved.
func (h *DavServer) resolveCalendarHrefForRequest(rawHref string, r *http.Request) (resolvedObjectHref, bool) {
	return h.resolveObjectHrefForRequest(rawHref, calendarPrefix, r)
}

func (h *DavServer) resolveAddressBookHrefForRequest(rawHref string, r *http.Request) (resolvedObjectHref, bool) {
	return h.resolveObjectHrefForRequest(rawHref, addressBookPrefix, r)
}

func (h *DavServer) resolveObjectHrefForRequest(rawHref, prefix string, r *http.Request) (resolvedObjectHref, bool) {
	if r == nil || r.URL == nil {
		return resolvedObjectHref{}, false
	}
	resolved, ok := h.resolveDAVHrefReference(rawHref, davHrefReferenceBase(r.URL.Path), r)
	if !ok {
		return resolvedObjectHref{}, false
	}
	cleanPath := path.Clean(resolved.EscapedPath())
	segment, resourceName, ok := parseEscapedResourcePath(cleanPath, prefix)
	return resolvedObjectHref{Path: cleanPath, Segment: segment, ResourceName: resourceName}, ok
}

// parseEscapedResourcePath splits an already-cleaned object-resource path that
// is still percent-encoded, which is the form a resolved DAV:href arrives in.
func parseEscapedResourcePath(cleanPath, prefix string) (string, string, bool) {
	collectionPrefix := prefix + "/"
	if !strings.HasPrefix(cleanPath, collectionPrefix) {
		return "", "", false
	}
	parts := strings.Split(strings.TrimPrefix(cleanPath, collectionPrefix), "/")
	if len(parts) != 2 || parts[0] == "" || parts[1] == "" {
		return "", "", false
	}
	segment, err := url.PathUnescape(parts[0])
	if err != nil {
		return "", "", false
	}
	resourceSegment, err := url.PathUnescape(parts[1])
	if err != nil {
		return "", "", false
	}
	resourceName := strings.TrimSuffix(resourceSegment, path.Ext(resourceSegment))
	if segment == "" || resourceName == "" {
		return "", "", false
	}
	return segment, resourceName, true
}

// isValidCalendarSlug validates calendar slugs for path safety.
// Slugs must: start/end with alphanumeric, contain only [a-z0-9-], be 1-64 chars.
func isValidCalendarSlug(slug string) bool {
	if len(slug) == 0 || len(slug) > 64 {
		return false
	}
	if slug[0] == '-' || slug[len(slug)-1] == '-' {
		return false
	}
	for _, ch := range slug {
		if !((ch >= 'a' && ch <= 'z') || (ch >= '0' && ch <= '9') || ch == '-') {
			return false
		}
	}
	return true
}
