package dav

import (
	"encoding/xml"
	"net/http"
	"strings"
)

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

// ensureCollectionHref returns p with the trailing slash collection hrefs use.
func ensureCollectionHref(p string) string {
	if !strings.HasSuffix(p, "/") {
		return p + "/"
	}
	return p
}
