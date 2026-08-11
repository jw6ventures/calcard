package dav

import (
	"encoding/xml"
	"fmt"
	"net/http"
	"strings"
)

func isValidCalDAVCondition(s string) bool {
	if len(s) == 0 {
		return false
	}
	for i, ch := range s {
		if i == 0 {
			// First character must be lowercase letter
			if ch < 'a' || ch > 'z' {
				return false
			}
		} else {
			// Subsequent characters: lowercase letter, digit, or hyphen
			if !((ch >= 'a' && ch <= 'z') || (ch >= '0' && ch <= '9') || ch == '-') {
				return false
			}
		}
	}
	return true
}

func writeCalDAVError(w http.ResponseWriter, status int, condition string) {
	if !isValidCalDAVCondition(condition) {
		condition = "invalid-condition"
	}
	w.Header().Set("Content-Type", "application/xml; charset=utf-8")
	w.WriteHeader(status)
	_, _ = fmt.Fprint(w, buildCalDAVErrorXML([]string{condition}))
}

func writeCalDAVErrorMulti(w http.ResponseWriter, status int, conditions ...string) {
	if len(conditions) == 0 {
		w.WriteHeader(status)
		return
	}
	if len(conditions) == 1 {
		writeCalDAVError(w, status, conditions[0])
		return
	}
	w.Header().Set("Content-Type", "application/xml; charset=utf-8")
	w.WriteHeader(status)
	_, _ = fmt.Fprint(w, buildCalDAVErrorXML(conditions))
}

// writeCalDAVUIDConflict answers the CALDAV:no-uid-conflict precondition of
// RFC 4791 §5.3.2.1, whose DAV:href child names the resource already using the
// submitted UID. §1.3 makes the status 409: the user can remove or rename that
// resource and resubmit. Callers resolve the conflicting resource before using
// this response because DAV:href is part of the precondition's content model.
func writeCalDAVUIDConflict(w http.ResponseWriter, conflictHref string) {
	w.Header().Set("Content-Type", "application/xml; charset=utf-8")
	w.WriteHeader(http.StatusConflict)
	var body strings.Builder
	body.WriteString(`<?xml version="1.0" encoding="utf-8"?><D:error xmlns:D="DAV:" xmlns:C="urn:ietf:params:xml:ns:caldav"><C:no-uid-conflict>`)
	var escaped strings.Builder
	if err := xml.EscapeText(&escaped, []byte(conflictHref)); err == nil {
		body.WriteString("<D:href>")
		body.WriteString(escaped.String())
		body.WriteString("</D:href>")
	}
	body.WriteString(`</C:no-uid-conflict></D:error>`)
	_, _ = fmt.Fprint(w, body.String())
}

// writeCalDAVSupportedFilter answers the CALDAV:supported-filter precondition
// of RFC 4791 §7.8.8, whose content model is (comp-filter*, prop-filter*,
// param-filter*): §7.8 asks the server to name the filter elements it could not
// honour, so the offending ones are echoed back with their name attributes.
func writeCalDAVSupportedFilter(w http.ResponseWriter, offending []filterElementRef) {
	w.Header().Set("Content-Type", "application/xml; charset=utf-8")
	w.WriteHeader(http.StatusForbidden)
	var body strings.Builder
	body.WriteString(`<?xml version="1.0" encoding="utf-8"?><D:error xmlns:D="DAV:" xmlns:C="urn:ietf:params:xml:ns:caldav"><C:supported-filter>`)
	for _, ref := range offending {
		if !isValidCalDAVCondition(ref.Element) {
			continue
		}
		var escaped strings.Builder
		if err := xml.EscapeText(&escaped, []byte(ref.Name)); err != nil {
			continue
		}
		fmt.Fprintf(&body, `<C:%s name="%s"/>`, ref.Element, escaped.String())
	}
	body.WriteString(`</C:supported-filter></D:error>`)
	_, _ = fmt.Fprint(w, body.String())
}

// writeDAVError writes a DAV:-namespace precondition error body (RFC 4918 §16),
// e.g. DAV:supported-report or DAV:propfind-finite-depth.
func writeDAVError(w http.ResponseWriter, status int, condition string) {
	if !isValidCalDAVCondition(condition) {
		condition = "invalid-condition"
	}
	w.Header().Set("Content-Type", "application/xml; charset=utf-8")
	w.WriteHeader(status)
	_, _ = fmt.Fprintf(w, `<?xml version="1.0" encoding="utf-8"?><D:error xmlns:D="DAV:"><D:%s/></D:error>`, condition)
}

func buildCalDAVErrorXML(conditions []string) string {
	var builder strings.Builder
	builder.WriteString(`<?xml version="1.0" encoding="utf-8"?><D:error xmlns:D="DAV:" xmlns:C="urn:ietf:params:xml:ns:caldav">`)
	for _, condition := range conditions {
		if strings.TrimSpace(condition) == "" {
			continue
		}
		if !isValidCalDAVCondition(condition) {
			continue
		}
		builder.WriteString("<C:")
		builder.WriteString(condition)
		builder.WriteString("/>")
	}

	builder.WriteString("</D:error>")
	return builder.String()
}
