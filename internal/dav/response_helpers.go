package dav

import (
	"bytes"
	"encoding/xml"
	"errors"
	"math"
	"net/http"
	"strings"

	"github.com/jw6ventures/calcard/internal/store"
)

const (
	defaultMaxMultistatusResponses = 10000
	defaultMaxMultistatusBytes     = 67108864
	defaultMaxReportCandidateRows  = 50000
	defaultMaxMultigetHrefs        = 5000
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

// maxReportResponses is the number of DAV:response elements one report may
// return, the count DAV:number-of-matches-within-limits measures against.
func (h *DavServer) maxReportResponses() int {
	maxResponses, _ := h.multistatusLimits()
	if maxResponses <= 0 {
		return defaultMaxMultistatusResponses
	}
	return maxResponses
}

// reportCandidateRowLimit is how many stored resources one report will read and
// parse while looking for matches. A filter matching nothing costs the same
// work per row as one matching everything, so the row budget is what bounds a
// report whose response count never grows.
func (h *DavServer) reportCandidateRowLimit() int {
	if h != nil && h.cfg != nil && h.cfg.DAV.MaxReportCandidateRows > 0 {
		return h.cfg.DAV.MaxReportCandidateRows
	}
	return defaultMaxReportCandidateRows
}

// multigetHrefLimit is how many DAV:href elements one multiget may carry. It is
// capped by the response limit as well, since every href owes a DAV:response
// and a report cannot answer more responses than it may return.
func (h *DavServer) multigetHrefLimit() int {
	limit := defaultMaxMultigetHrefs
	if h != nil && h.cfg != nil && h.cfg.DAV.MaxMultigetHrefs > 0 {
		limit = h.cfg.DAV.MaxMultigetHrefs
	}
	if maxResponses := h.maxReportResponses(); limit > maxResponses {
		return maxResponses
	}
	return limit
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

// writeInsufficientStorage answers a request the server will not complete
// because it exceeds a configured bound. RFC 4918 names no condition for the
// status, so the body is the status text: the sentinels that reach here carry
// storage and query detail a client has no business reading.
func writeInsufficientStorage(w http.ResponseWriter) {
	http.Error(w, http.StatusText(http.StatusInsufficientStorage), http.StatusInsufficientStorage)
}

// davRetryAfterSeconds is the Retry-After sent with a 503 while the store is
// unavailable.
const davRetryAfterSeconds = "5"

// serverFailureStatus is the status for a request that failed for a reason of
// the server's own: 503 while the database cannot be reached, which clients
// retry later, and 500 otherwise.
func serverFailureStatus(err error) int {
	if store.IsUnavailable(err) {
		return http.StatusServiceUnavailable
	}
	return http.StatusInternalServerError
}

// writeStatusText answers with status and its text alone, adding Retry-After
// to a 503. Callers whose error carries storage or connection detail log it
// rather than send it.
func writeStatusText(w http.ResponseWriter, status int) {
	if status == http.StatusServiceUnavailable {
		w.Header().Set("Retry-After", davRetryAfterSeconds)
	}
	http.Error(w, http.StatusText(status), status)
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
		writeInsufficientStorage(w)
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

// writeServerFailure answers a request that failed with err for a reason of
// the server's own. While the store is unavailable it answers 503 with the
// status text alone so a client retries rather than giving up; otherwise it
// answers 500 with message.
func writeServerFailure(w http.ResponseWriter, err error, message string) {
	if store.IsUnavailable(err) {
		writeStatusText(w, http.StatusServiceUnavailable)
		return
	}
	http.Error(w, message, http.StatusInternalServerError)
}

// writeStoreUnavailable answers 503 and reports true when err shows the store
// is unavailable. Callers that map a failed lookup to a client status, such as
// 404 or 403, check it first so an outage never reads as a missing resource.
func writeStoreUnavailable(w http.ResponseWriter, err error) bool {
	if !store.IsUnavailable(err) {
		return false
	}
	writeStatusText(w, http.StatusServiceUnavailable)
	return true
}
