package dav

import (
	"net/http"
	"path"
	"sort"
	"strings"
)

var davAllowMethods = []string{"OPTIONS", "HEAD", "GET", "PROPFIND", "PROPPATCH", "MKCOL", "MKCALENDAR", "PUT", "DELETE", "REPORT", "LOCK", "UNLOCK", "ACL"}
var davAllowMethodsWithCopyMove = []string{"OPTIONS", "HEAD", "GET", "PROPFIND", "PROPPATCH", "MKCOL", "MKCALENDAR", "PUT", "DELETE", "REPORT", "COPY", "MOVE", "LOCK", "UNLOCK", "ACL"}

func (h *DavServer) davHeaderForPath(cleanPath string) string {
	if cleanPath == "/dav" || cleanPath == "/dav/" {
		return "1, 2, 3, access-control, calendar-access, addressbook"
	}
	if h != nil && h.davRegistry().isExtensionPath(cleanPath) {
		return "1, 2, 3, access-control"
	}
	return "1, 2, 3, access-control, calendar-access, addressbook, extended-mkcol"
}

func (h *DavServer) Options(w http.ResponseWriter, r *http.Request) {
	if h.handleRegisteredMethod(w, r) {
		return
	}
	cleanPath := ""
	if r != nil {
		cleanPath = path.Clean(r.URL.Path)
	}
	h.logger().Trace("Options", "OPTIONS %s", cleanPath)
	w.Header().Set("Allow", h.allowHeaderForPath(cleanPath))
	davHeader := h.davHeaderForPath("")
	if r != nil {
		davHeader = h.davHeaderForPath(path.Clean(r.URL.Path))
	}
	w.Header().Set("DAV", davHeader)
	w.Header().Set("Accept-Patch", "application/xml")
	w.WriteHeader(http.StatusNoContent)
}

func (h *DavServer) Head(w http.ResponseWriter, r *http.Request) {
	if h.handleRegisteredMethod(w, r) {
		return
	}
	h.Get(w, r)
}

func (h *DavServer) allowHeaderForPath(cleanPath string) string {
	methods := davAllowMethods
	if supportsCopyMove(cleanPath) {
		methods = davAllowMethodsWithCopyMove
	}
	seen := make(map[string]struct{}, len(methods))
	allow := make([]string, 0, len(methods))
	for _, method := range methods {
		seen[method] = struct{}{}
		allow = append(allow, method)
	}
	var extensionMethods []string
	if h != nil {
		extensionMethods = h.davRegistry().registeredMethodsForPath(cleanPath)
	}
	sort.Strings(extensionMethods)
	for _, method := range extensionMethods {
		if _, ok := seen[method]; ok {
			continue
		}
		seen[method] = struct{}{}
		allow = append(allow, method)
	}
	return strings.Join(allow, ", ")
}

func supportsCopyMove(cleanPath string) bool {
	switch {
	case cleanPath == "" || cleanPath == "/" || cleanPath == ".":
		return false
	case strings.HasPrefix(cleanPath, "/dav/calendars/"):
		_, _, ok := parseCalendarResourceSegments(cleanPath)
		return ok
	case strings.HasPrefix(cleanPath, "/dav/addressbooks/"):
		_, _, ok := parseAddressBookResourceSegments(cleanPath)
		return ok
	default:
		return false
	}
}
