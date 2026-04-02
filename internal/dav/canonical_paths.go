package dav

import (
	"context"
	"fmt"
	"path"
	"strings"

	"github.com/jw6ventures/calcard/internal/store"
)

const pendingCollectionSegment = ".pending"

func (h *Handler) canonicalDAVPath(ctx context.Context, user *store.User, rawPath string) (string, error) {
	cleanPath := normalizeDAVHref(rawPath)
	switch {
	case cleanPath == "", cleanPath == "/":
		return cleanPath, nil
	case cleanPath == "/dav/addressbooks", cleanPath == "/dav/calendars", cleanPath == "/dav/principals", cleanPath == "/dav":
		return cleanPath, nil
	case strings.HasPrefix(cleanPath, "/dav/addressbooks/"):
		return h.canonicalCollectionPath(ctx, user, cleanPath, "/dav/addressbooks", h.resolveAddressBookID)
	case strings.HasPrefix(cleanPath, "/dav/calendars/"):
		return h.canonicalCollectionPath(ctx, user, cleanPath, "/dav/calendars", h.resolveCalendarID)
	default:
		return cleanPath, nil
	}
}

func (h *Handler) canonicalCollectionPath(ctx context.Context, user *store.User, cleanPath, prefix string, resolve func(context.Context, *store.User, string) (int64, bool, error)) (string, error) {
	trimmed := strings.Trim(strings.TrimPrefix(cleanPath, prefix), "/")
	if trimmed == "" {
		return prefix, nil
	}
	parts := strings.Split(trimmed, "/")
	if len(parts) > 2 {
		return cleanPath, nil
	}
	segment := strings.TrimSpace(parts[0])
	if segment == "" {
		return prefix, nil
	}

	id, ok, err := resolve(ctx, user, segment)
	if err != nil {
		if err == store.ErrNotFound && len(parts) == 1 {
			if user != nil {
				return pendingCollectionPath(prefix, user.ID, segment), nil
			}
			return cleanPath, nil
		}
		return "", err
	}
	if !ok {
		if len(parts) == 1 && user != nil {
			return pendingCollectionPath(prefix, user.ID, segment), nil
		}
		return cleanPath, nil
	}

	canonical := path.Join(prefix, fmt.Sprint(id))
	if len(parts) == 1 {
		return canonical, nil
	}

	resourcePart := parts[1]
	resourceName := strings.TrimSuffix(resourcePart, path.Ext(resourcePart))
	if resourceName == "" {
		return cleanPath, nil
	}
	return canonical + "/" + resourceName, nil
}

func pendingCollectionPath(prefix string, userID int64, segment string) string {
	return path.Join(prefix, pendingCollectionSegment, fmt.Sprint(userID), segment)
}

func publicPendingCollectionPath(canonicalPath string) (string, bool) {
	cleanPath := normalizeDAVHref(canonicalPath)
	for _, prefix := range []string{"/dav/addressbooks", "/dav/calendars"} {
		if !strings.HasPrefix(cleanPath, prefix+"/"+pendingCollectionSegment+"/") {
			continue
		}
		trimmed := strings.Trim(strings.TrimPrefix(cleanPath, prefix), "/")
		parts := strings.Split(trimmed, "/")
		if len(parts) != 3 || parts[0] != pendingCollectionSegment || parts[2] == "" {
			continue
		}
		return path.Join(prefix, parts[2]), true
	}
	return "", false
}

func normalizeDAVResourceIdentity(rawPath string) string {
	cleanPath := normalizeDAVHref(rawPath)
	if segment, resource, ok := parseCalendarResourceSegments(cleanPath); ok {
		resourcePart := path.Base(cleanPath)
		if strings.EqualFold(path.Ext(resourcePart), ".ics") {
			return path.Join("/dav/calendars", segment, resource)
		}
		return cleanPath
	}
	if segment, resource, ok := parseAddressBookResourceSegments(cleanPath); ok {
		resourcePart := path.Base(cleanPath)
		if strings.EqualFold(path.Ext(resourcePart), ".vcf") {
			return path.Join("/dav/addressbooks", segment, resource)
		}
		return cleanPath
	}
	return cleanPath
}

func legacyDAVResourcePaths(canonicalPath string) []string {
	switch {
	case strings.HasPrefix(canonicalPath, "/dav/calendars/"):
		if _, _, ok := parseCalendarResourceSegments(canonicalPath); ok {
			return []string{canonicalPath + ".ics"}
		}
	case strings.HasPrefix(canonicalPath, "/dav/addressbooks/"):
		if _, _, ok := parseAddressBookResourceSegments(canonicalPath); ok {
			return []string{canonicalPath + ".vcf"}
		}
	}
	return nil
}

func davStatePaths(resourcePath string) []string {
	resourcePath = normalizeDAVHref(resourcePath)
	if resourcePath == "" {
		return nil
	}

	seen := map[string]struct{}{}
	var paths []string
	addPath := func(p string) {
		p = normalizeDAVHref(p)
		if p == "" {
			return
		}
		if _, ok := seen[p]; ok {
			return
		}
		seen[p] = struct{}{}
		paths = append(paths, p)
	}

	addPath(resourcePath)
	normalized := normalizeDAVResourceIdentity(resourcePath)
	addPath(normalized)
	for _, legacyPath := range legacyDAVResourcePaths(normalized) {
		addPath(legacyPath)
	}
	return paths
}
