package dav

import (
	"encoding/xml"
	"errors"
	"net/http"
	"strings"

	"github.com/jw6ventures/calcard/internal/auth"
	"github.com/jw6ventures/calcard/internal/store"
)

// propfindInfinityEnabled reports whether Depth: infinity PROPFIND is served.
// Enabled by default (including when no config is wired, as in tests); the
// APP_DAV_PROPFIND_INFINITY_ENABLED env var can turn it off.
func (h *DavServer) propfindInfinityEnabled() bool {
	return h == nil || h.cfg == nil || h.cfg.DAV.PropfindInfinityEnabled
}

func (h *DavServer) Propfind(w http.ResponseWriter, r *http.Request) {
	r = ensureRequestCaches(r)
	if h.handleRegisteredMethod(w, r) {
		return
	}
	depth := strings.TrimSpace(r.Header.Get("Depth"))
	if depth == "" {
		// RFC 4918 §9.1: a missing Depth header means Depth: infinity.
		depth = "infinity"
	}
	h.logger().Trace("Propfind", "PROPFIND %s depth=%s", r.URL.Path, depth)

	user, ok := auth.UserFromContext(r.Context())
	if !ok {
		http.Error(w, "missing user", http.StatusUnauthorized)
		return
	}

	switch depth {
	case "0", "1":
	case "infinity":
		// RFC 4918 §9.1.1: servers may refuse infinite-depth PROPFIND, but
		// must say so instead of silently degrading the depth.
		if !h.propfindInfinityEnabled() {
			writeDAVError(w, http.StatusForbidden, "propfind-finite-depth")
			return
		}
	default:
		http.Error(w, "invalid Depth header", http.StatusBadRequest)
		return
	}

	var propfindReq propfindRequest
	if r.Body != http.NoBody {
		body, err := readDAVBody(w, r, maxDAVBodyBytes)
		if err != nil {
			if errors.Is(err, errRequestTooLarge) {
				http.Error(w, "request too large", http.StatusRequestEntityTooLarge)
			} else {
				http.Error(w, "failed to read body", http.StatusBadRequest)
			}
			return
		}
		if err := safeUnmarshalXML(body, &propfindReq); err != nil {
			// RFC 4918 §9.1: a request body that is not valid XML is a 400,
			// not an implicit allprop.
			http.Error(w, "invalid PROPFIND body", http.StatusBadRequest)
			return
		}
	} else {
		propfindReq.AllProp = &struct{}{}
	}

	responses, err := h.buildPropfindResponses(r.Context(), r, r.URL.Path, depth, user, &propfindReq)
	if err != nil {
		status := http.StatusBadRequest
		if errors.Is(err, errAmbiguousCalendar) || errors.Is(err, errAmbiguousAddressBook) {
			status = http.StatusConflict
		}
		if errors.Is(err, errForbidden) {
			status = http.StatusForbidden
		}
		if errors.Is(err, store.ErrNotFound) || errors.Is(err, http.ErrNotSupported) {
			status = http.StatusNotFound
		}
		h.logger().Error("Propfind", "failed to build responses for %s (status %d): %v", r.URL.Path, status, err)
		http.Error(w, err.Error(), status)
		return
	}
	h.logger().Debug("Propfind", "%s returned %d responses", r.URL.Path, len(responses))

	payload := multistatus{
		XMLName:   xml.Name{Space: "DAV:", Local: "multistatus"},
		XmlnsD:    "DAV:",
		XmlnsC:    "urn:ietf:params:xml:ns:caldav",
		XmlnsA:    "urn:ietf:params:xml:ns:carddav",
		XmlnsCS:   "http://calendarserver.org/ns/",
		XmlnsICAL: "http://apple.com/ns/ical/",
		Response:  responses,
	}
	writeMultiStatus(w, payload)
}
