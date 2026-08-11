package dav

import (
	"context"
	"errors"
	"net/http"
	"net/url"
	"path"
	"strconv"
	"strings"

	"github.com/jw6ventures/calcard/internal/auth"
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

func parseDAVTarget(rawPath string) davTarget {
	cleanPath := normalizeDAVHref(rawPath)
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
	cleanPath := normalizeDAVHref(rawPath)
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

// parseResourcePath extracts the numeric collection ID and resource name from a DAV resource path.
// The returned boolean indicates whether the path matched the expected prefix and contained both parts.
func parseResourcePath(rawPath, prefix string) (int64, string, bool) {
	target := parseDAVTarget(rawPath)
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

// requestScheme is the scheme the client used to reach this server, which an
// absolute DAV:href has to match to name the same resource (RFC 3986 §6.2.1).
// http.Server leaves URL.Scheme empty and a TLS-terminating proxy leaves r.TLS
// nil, so behind one the only truthful answer comes from X-Forwarded-Proto,
// trusted on the terms internal/auth already sets for it. The configured base
// URL is the last resort, for a proxy that forwards no such header.
func (h *DavServer) requestScheme(r *http.Request) string {
	if r.URL.Scheme != "" {
		return strings.ToLower(r.URL.Scheme)
	}
	var trustedProxies []string
	if h != nil && h.cfg != nil {
		trustedProxies = h.cfg.TrustedProxies
	}
	if auth.RequestIsSecure(r, trustedProxies) {
		return "https"
	}
	if h != nil && h.cfg != nil {
		if baseURL, err := url.Parse(h.cfg.BaseURL); err == nil &&
			(strings.EqualFold(baseURL.Scheme, "http") || strings.EqualFold(baseURL.Scheme, "https")) {
			return strings.ToLower(baseURL.Scheme)
		}
	}
	return "http"
}

func (h *DavServer) resolveDAVHrefReference(rawHref, basePath string, r *http.Request) (*url.URL, bool) {
	if r == nil || r.URL == nil || rawHref == "" || strings.TrimSpace(rawHref) != rawHref {
		return nil, false
	}
	reference, err := url.Parse(rawHref)
	if err != nil || reference.User != nil || reference.Fragment != "" {
		return nil, false
	}

	requestURI := &url.URL{
		Scheme:     h.requestScheme(r),
		Host:       r.Host,
		Path:       r.URL.Path,
		RawPath:    r.URL.RawPath,
		RawQuery:   r.URL.RawQuery,
		ForceQuery: r.URL.ForceQuery,
	}
	baseURI := *requestURI
	if basePath != "" {
		base, parseErr := url.Parse(basePath)
		if parseErr != nil || base.Scheme != "" || base.Host != "" || base.User != nil || base.Fragment != "" {
			return nil, false
		}
		baseURI.Path = base.Path
		baseURI.RawPath = base.RawPath
	}
	resolved := baseURI.ResolveReference(reference)
	if resolved.User != nil || !strings.EqualFold(resolved.Scheme, requestURI.Scheme) ||
		!strings.EqualFold(resolved.Host, requestURI.Host) || resolved.RawQuery != requestURI.RawQuery ||
		resolved.ForceQuery != requestURI.ForceQuery || resolved.Fragment != "" {
		return nil, false
	}
	return resolved, true
}

// davHrefReferenceBase is the base a DAV:href in a request body resolves
// against. RFC 3986 §5.2.2 discards the last segment of a base that does not end
// in "/", so a collection Request-URI written without its trailing slash needs
// the slash restored or a relative href would resolve above the collection
// holding the resource it names. A Request-URI naming an object resource wants
// that same discard and is left alone.
func davHrefReferenceBase(requestPath string) string {
	cleanPath := normalizeDAVHref(requestPath)
	if target := parseDAVTarget(cleanPath); target.Valid && target.Resource {
		return cleanPath
	}
	return ensureCollectionHref(cleanPath)
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
	// Must start and end with alphanumeric (not hyphen)
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
