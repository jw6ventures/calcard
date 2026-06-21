package dav

import (
	"fmt"
	"path"
	"strings"

	"github.com/jw6ventures/calcard/internal/store"
)

// Collection URL prefixes for the default DAV modules. Used by the shared
// path/ACL helpers below so calendar and address-book code stays in lockstep.
const (
	calendarPrefix    = "/dav/calendars"
	addressBookPrefix = "/dav/addressbooks"
)

// collectionPathForPrefix reduces a resource path to its owning collection path
// (e.g. "/dav/calendars/12/foo.ics" -> "/dav/calendars/12"). Paths outside the
// prefix are returned normalized but otherwise unchanged.
func collectionPathForPrefix(cleanPath, prefix string) string {
	cleanPath = normalizeDAVHref(cleanPath)
	if !strings.HasPrefix(cleanPath, prefix+"/") {
		return cleanPath
	}
	trimmed := strings.TrimPrefix(cleanPath, prefix+"/")
	parts := strings.Split(trimmed, "/")
	if len(parts) == 0 || strings.TrimSpace(parts[0]) == "" {
		return prefix
	}
	return path.Join(prefix, parts[0])
}

// collectionResourcePath builds the canonical path of a collection by ID.
func collectionResourcePath(prefix string, id int64) string {
	return path.Join(prefix, fmt.Sprint(id))
}

// objectResourcePath builds the canonical path of an object within a collection.
func objectResourcePath(prefix string, id int64, resourceName string) string {
	return path.Join(prefix, fmt.Sprint(id), resourceName)
}

// objectACLPaths returns the ACL lookup paths for an object, covering both the
// stored resource name and its extension-normalized variant (ext includes the
// dot, e.g. ".ics" or ".vcf").
func objectACLPaths(prefix string, id int64, resourceName, ext string) []string {
	resourceName = strings.TrimSpace(resourceName)
	if resourceName == "" {
		return nil
	}
	base := objectResourcePath(prefix, id, resourceName)
	paths := []string{base}
	if strings.EqualFold(path.Ext(resourceName), ext) {
		paths = append(paths, strings.TrimSuffix(base, path.Ext(resourceName)))
	} else {
		paths = append(paths, base+ext)
	}
	return paths
}

// requirePrivilegeDecision turns the (allowed, denied) result of a privilege
// decision into the standard error contract shared by the require*Privilege
// helpers: nil when allowed, errForbidden when explicitly denied, and
// store.ErrNotFound otherwise (so callers fail closed without leaking
// existence).
func requirePrivilegeDecision(allowed, denied bool, err error) error {
	if err != nil {
		return err
	}
	if allowed {
		return nil
	}
	if denied {
		return errForbidden
	}
	return store.ErrNotFound
}
