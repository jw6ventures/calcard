package dav

import (
	"path"
	"strings"
)

// parseCalendarResourceSegments extracts the calendar collection segment and resource name.
// It accepts collection segments as either numeric IDs or slugs.
func parseCalendarResourceSegments(rawPath string) (string, string, bool) {
	cleanPath := normalizeDAVHref(rawPath)
	if cleanPath == "" || !strings.HasPrefix(cleanPath, "/dav/calendars") {
		return "", "", false
	}
	trimmed := strings.TrimPrefix(cleanPath, "/dav/calendars")
	trimmed = strings.TrimPrefix(trimmed, "/")
	parts := strings.Split(trimmed, "/")
	if len(parts) < 2 || parts[0] == "" || parts[1] == "" {
		return "", "", false
	}
	resource := strings.TrimSuffix(parts[1], path.Ext(parts[1]))
	if resource == "" {
		return "", "", false
	}
	return parts[0], resource, true
}
