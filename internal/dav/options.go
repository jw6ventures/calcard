package dav

import (
	"net/http"
	"path"
	"strings"
)

const davAllowMethods = "OPTIONS, HEAD, GET, PROPFIND, PROPPATCH, MKCOL, MKCALENDAR, PUT, DELETE, REPORT, LOCK, UNLOCK, ACL"
const davAllowMethodsWithCopyMove = "OPTIONS, HEAD, GET, PROPFIND, PROPPATCH, MKCOL, MKCALENDAR, PUT, DELETE, REPORT, COPY, MOVE, LOCK, UNLOCK, ACL"

func davHeaderForPath(cleanPath string) string {
	if cleanPath == "/dav" || cleanPath == "/dav/" {
		return "1, 2, 3, access-control, calendar-access, addressbook"
	}
	return "1, 2, 3, access-control, calendar-access, addressbook, extended-mkcol"
}

func (h *Handler) Options(w http.ResponseWriter, r *http.Request) {
	allow := davAllowMethods
	if r != nil && supportsCopyMove(path.Clean(r.URL.Path)) {
		allow = davAllowMethodsWithCopyMove
	}
	w.Header().Set("Allow", allow)
	davHeader := davHeaderForPath("")
	if r != nil {
		davHeader = davHeaderForPath(path.Clean(r.URL.Path))
	}
	w.Header().Set("DAV", davHeader)
	w.Header().Set("Accept-Patch", "application/xml")
	w.WriteHeader(http.StatusNoContent)
}

func (h *Handler) Head(w http.ResponseWriter, r *http.Request) {
	h.Get(w, r)
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
