package dav

import (
	"bytes"
	"encoding/xml"
	"errors"
	"math"
	"net/http"
	"strings"
)

const (
	defaultMaxMultistatusResponses = 10000
	defaultMaxMultistatusBytes     = 67108864
	multistatusPageSize            = 256
)

var errMultistatusTooLarge = errors.New("multistatus response exceeds configured limit")

// newMultistatus builds a multistatus payload with the full set of namespace
// declarations every DAV response in this package uses. An empty syncToken
// omits the sync-token element.
func newMultistatus(responses []response, syncToken string) multistatus {
	return multistatus{
		XMLName:   xml.Name{Space: "DAV:", Local: "multistatus"},
		XmlnsD:    "DAV:",
		XmlnsC:    "urn:ietf:params:xml:ns:caldav",
		XmlnsA:    "urn:ietf:params:xml:ns:carddav",
		XmlnsCS:   "http://calendarserver.org/ns/",
		XmlnsICAL: "http://apple.com/ns/ical/",
		SyncToken: syncToken,
		Response:  responses,
	}
}

func writeMultiStatus(w http.ResponseWriter, payload multistatus) {
	w.Header().Set("Content-Type", "application/xml; charset=utf-8")
	w.WriteHeader(http.StatusMultiStatus)
	_ = xml.NewEncoder(w).Encode(payload)
}

type boundedXMLBuffer struct {
	bytes.Buffer
	limit int
}

func (b *boundedXMLBuffer) Write(p []byte) (int, error) {
	if len(p) > b.limit-b.Len() {
		return 0, errMultistatusTooLarge
	}
	return b.Buffer.Write(p)
}

func (h *DavServer) multistatusLimits() (int, int) {
	if h != nil && h.cfg != nil {
		return h.cfg.DAV.MaxMultistatusResponses, h.cfg.DAV.MaxMultistatusBytes
	}
	return defaultMaxMultistatusResponses, defaultMaxMultistatusBytes
}

func (h *DavServer) multistatusBuildLimit() int {
	maxResponses, _ := h.multistatusLimits()
	if maxResponses <= 0 {
		maxResponses = defaultMaxMultistatusResponses
	}
	if maxResponses == math.MaxInt {
		return maxResponses
	}
	return maxResponses + 1
}

func (h *DavServer) capMultistatusResponses(responses []response) ([]response, bool) {
	buildLimit := h.multistatusBuildLimit()
	if len(responses) > buildLimit {
		return responses[:buildLimit], true
	}
	maxResponses, _ := h.multistatusLimits()
	if maxResponses <= 0 {
		maxResponses = defaultMaxMultistatusResponses
	}
	return responses, len(responses) > maxResponses
}

func (h *DavServer) appendMultistatusResponses(responses, additions []response) []response {
	buildLimit := h.multistatusBuildLimit()
	if len(responses) >= buildLimit {
		return responses[:buildLimit]
	}
	remaining := buildLimit - len(responses)
	if len(additions) > remaining {
		additions = additions[:remaining]
	}
	return append(responses, additions...)
}

func (h *DavServer) multistatusBuildComplete(responses []response) bool {
	return len(responses) >= h.multistatusBuildLimit()
}

func (h *DavServer) writeBoundedMultiStatus(w http.ResponseWriter, payload multistatus) {
	maxResponses, maxBytes := h.multistatusLimits()
	if maxResponses <= 0 {
		maxResponses = defaultMaxMultistatusResponses
	}
	if maxBytes <= 0 {
		maxBytes = defaultMaxMultistatusBytes
	}
	if len(payload.Response) > maxResponses {
		http.Error(w, http.StatusText(http.StatusInsufficientStorage), http.StatusInsufficientStorage)
		return
	}

	buf := &boundedXMLBuffer{limit: maxBytes}
	if err := xml.NewEncoder(buf).Encode(payload); err != nil {
		status := http.StatusInternalServerError
		if errors.Is(err, errMultistatusTooLarge) {
			status = http.StatusInsufficientStorage
		}
		http.Error(w, http.StatusText(status), status)
		return
	}
	w.Header().Set("Content-Type", "application/xml; charset=utf-8")
	w.WriteHeader(http.StatusMultiStatus)
	_, _ = w.Write(buf.Bytes())
}

// ensureCollectionHref returns p with the trailing slash collection hrefs use.
func ensureCollectionHref(p string) string {
	if !strings.HasSuffix(p, "/") {
		return p + "/"
	}
	return p
}
