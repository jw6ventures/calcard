package dav

import (
	"context"
	"errors"
	"net/url"
	"path"
	"strconv"
	"strings"

	"github.com/jw6ventures/calcard/internal/store"
)

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
	book, err := h.loadAddressBookByName(ctx, user, segment)
	if err != nil {
		if errors.Is(err, errAmbiguousAddressBook) {
			return 0, false, errAmbiguousAddressBook
		}
		if err == store.ErrNotFound {
			return 0, false, store.ErrNotFound
		}
		return 0, false, err
	}
	return book.ID, true, nil
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
	cal, err := h.loadCalendarByName(ctx, user, segment)
	if err != nil {
		if errors.Is(err, errAmbiguousCalendar) {
			return 0, false, errAmbiguousCalendar
		}
		if err == store.ErrNotFound {
			return 0, false, store.ErrNotFound
		}
		return 0, false, err
	}
	return cal.ID, true, nil
}

func (h *DavServer) parseCalendarResourcePath(ctx context.Context, user *store.User, rawPath string) (int64, string, bool, error) {
	segment, resource, ok := parseCalendarResourceSegments(rawPath)
	if !ok {
		return 0, "", false, nil
	}
	id, ok, err := h.resolveCalendarID(ctx, user, segment)
	if err != nil {
		if errors.Is(err, errAmbiguousCalendar) {
			return 0, resource, true, errAmbiguousCalendar
		}
		if err == store.ErrNotFound {
			return 0, resource, true, err
		}
		return 0, "", false, err
	}
	if !ok {
		return 0, resource, true, store.ErrNotFound
	}
	return id, resource, true, nil
}

func parseAddressBookResourceSegments(rawPath string) (string, string, bool) {
	cleanPath := normalizeDAVHref(rawPath)
	if cleanPath == "" || !strings.HasPrefix(cleanPath, "/dav/addressbooks/") {
		return "", "", false
	}
	trimmed := strings.TrimPrefix(cleanPath, "/dav/addressbooks/")
	parts := strings.Split(trimmed, "/")
	if len(parts) != 2 || parts[0] == "" || parts[1] == "" {
		return "", "", false
	}
	uid := strings.TrimSuffix(parts[1], path.Ext(parts[1]))
	if uid == "" {
		return "", "", false
	}
	return parts[0], uid, true
}

func (h *DavServer) parseAddressBookResourcePath(ctx context.Context, user *store.User, rawPath string) (int64, string, bool, error) {
	segment, resource, ok := parseAddressBookResourceSegments(rawPath)
	if !ok {
		return 0, "", false, nil
	}
	id, ok, err := h.resolveAddressBookID(ctx, user, segment)
	if err != nil {
		if errors.Is(err, errAmbiguousAddressBook) {
			return 0, resource, true, errAmbiguousAddressBook
		}
		if err == store.ErrNotFound {
			return 0, resource, true, err
		}
		return 0, "", false, err
	}
	if !ok {
		return 0, resource, true, store.ErrNotFound
	}
	return id, resource, true, nil
}

// parseResourcePath extracts the numeric collection ID and resource name from a DAV resource path.
// The returned boolean indicates whether the path matched the expected prefix and contained both parts.
func parseResourcePath(rawPath, prefix string) (int64, string, bool) {
	cleanPath := normalizeDAVHref(rawPath)
	if cleanPath == "" || !strings.HasPrefix(cleanPath, prefix) {
		return 0, "", false
	}
	trimmed := strings.TrimPrefix(cleanPath, prefix)
	trimmed = strings.TrimPrefix(trimmed, "/")
	parts := strings.Split(trimmed, "/")
	if len(parts) < 2 || parts[0] == "" || parts[1] == "" {
		return 0, "", false
	}
	id, err := strconv.ParseInt(parts[0], 10, 64)
	if err != nil {
		return 0, "", false
	}
	uid := strings.TrimSuffix(parts[1], path.Ext(parts[1]))
	if uid == "" {
		return 0, "", false
	}
	return id, uid, true
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

func resolveDAVHref(basePath, rawHref string) string {
	trimmed := strings.TrimSpace(rawHref)
	if trimmed == "" {
		return ""
	}
	if strings.HasPrefix(trimmed, "http://") || strings.HasPrefix(trimmed, "https://") {
		if u, err := url.Parse(trimmed); err == nil {
			return normalizeDAVHref(u.Path)
		}
		return ""
	}
	if strings.HasPrefix(trimmed, "/") {
		return normalizeDAVHref(trimmed)
	}
	if u, err := url.Parse(trimmed); err == nil && u.Path != "" {
		if strings.HasPrefix(u.Path, "/") {
			return normalizeDAVHref(u.Path)
		}
		trimmed = u.Path
	}
	base := normalizeDAVHref(basePath)
	if base == "" {
		base = "/"
	}
	if !strings.HasSuffix(base, "/") {
		if _, _, ok := parseCalendarResourceSegments(base); ok {
			base = path.Dir(base) + "/"
		} else if _, _, ok := parseAddressBookResourceSegments(base); ok {
			base = path.Dir(base) + "/"
		} else {
			base += "/"
		}
	}
	return normalizeDAVHref(path.Join(base, trimmed))
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
