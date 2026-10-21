package dav

import (
	"context"
	"errors"
	"fmt"
	"path"
	"strings"

	"github.com/jw6ventures/calcard/internal/store"
)

const pendingCollectionSegment = ".pending"

// canonicalDAVPath resolves an already-decoded path; canonicalDAVHref resolves
// a percent-encoded href.
func (h *DavServer) canonicalDAVPath(ctx context.Context, user *store.User, decodedPath string) (string, error) {
	return h.resolveCanonicalDAVPath(ctx, user, cleanDAVPath(decodedPath))
}

func (h *DavServer) canonicalDAVHref(ctx context.Context, user *store.User, href string) (string, error) {
	return h.resolveCanonicalDAVPath(ctx, user, normalizeDAVHref(href))
}

func (h *DavServer) resolveCanonicalDAVPath(ctx context.Context, user *store.User, cleanPath string) (string, error) {
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

func (h *DavServer) canonicalCollectionPath(ctx context.Context, user *store.User, cleanPath, prefix string, resolve func(context.Context, *store.User, string) (int64, bool, error)) (string, error) {
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
		if errors.Is(err, store.ErrNotFound) && len(parts) == 1 {
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
	cleanPath := cleanDAVPath(canonicalPath)
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
