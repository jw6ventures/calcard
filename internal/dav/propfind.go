package dav

import (
	"encoding/xml"
	"errors"
	"io"
	"net/http"
	"strings"

	"gitea.jw6.us/james/calcard/internal/auth"
	"gitea.jw6.us/james/calcard/internal/store"
)

func (h *Handler) Propfind(w http.ResponseWriter, r *http.Request) {
	depth := strings.TrimSpace(r.Header.Get("Depth"))
	if depth == "" {
		depth = "1"
	}

	user, ok := auth.UserFromContext(r.Context())
	if !ok {
		http.Error(w, "missing user", http.StatusUnauthorized)
		return
	}

	var propfindReq propfindRequest
	if r.ContentLength > 0 {
		body, err := io.ReadAll(io.LimitReader(r.Body, maxDAVBodyBytes))
		if err != nil {
			http.Error(w, "failed to read body", http.StatusBadRequest)
			return
		}
		if err := safeUnmarshalXML(body, &propfindReq); err != nil {
			propfindReq.AllProp = &struct{}{}
		}
	} else {
		propfindReq.AllProp = &struct{}{}
	}

	responses, err := h.buildPropfindResponses(r.Context(), r.URL.Path, depth, user, &propfindReq)
	if err != nil {
		status := http.StatusBadRequest
		if errors.Is(err, errAmbiguousCalendar) {
			status = http.StatusConflict
		}
		if errors.Is(err, store.ErrNotFound) || errors.Is(err, http.ErrNotSupported) {
			status = http.StatusNotFound
		}
		http.Error(w, err.Error(), status)
		return
	}

	payload := multistatus{
		XMLName:  xml.Name{Space: "DAV:", Local: "multistatus"},
		XmlnsD:   "DAV:",
		XmlnsC:   "urn:ietf:params:xml:ns:caldav",
		XmlnsA:   "urn:ietf:params:xml:ns:carddav",
		XmlnsCS:  "http://calendarserver.org/ns/",
		Response: responses,
	}

	w.Header().Set("Content-Type", "application/xml; charset=utf-8")
	w.WriteHeader(http.StatusMultiStatus)
	_ = xml.NewEncoder(w).Encode(payload)
}
