package dav

import "net/http"

// OPTIONS/HEAD handlers kept small to keep handler.go lean.

func (h *Handler) Options(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Allow", "OPTIONS, HEAD, GET, PROPFIND, PROPPATCH, MKCOL, MKCALENDAR, PUT, DELETE, REPORT")
	w.Header().Set("DAV", "1, 2, calendar-access, addressbook")
	w.Header().Set("Accept-Patch", "application/xml")
	w.WriteHeader(http.StatusNoContent)
}

func (h *Handler) Head(w http.ResponseWriter, r *http.Request) {
	h.Get(w, r)
}
