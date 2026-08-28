package dav

import (
	"encoding/xml"
	"fmt"
	"net/http"
	"strings"
)

// isValidConditionName reports whether s spells a precondition or
// postcondition element name. The DAV:, CalDAV and CardDAV namespaces are each
// reserved for the elements their specifications define -- RFC 4918 §16,
// RFC 4791 §1.2, RFC 6352 §10 -- so a name outside this shape names no
// condition and cannot be written into any of them.
func isValidConditionName(s string) bool {
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

// writeCalDAVError answers a CalDAV precondition or postcondition failure with
// the condition element under a top-level DAV:error, as RFC 4791 §1.3 requires.
func writeCalDAVError(w http.ResponseWriter, status int, condition string) {
	writeConditionError(w, status, namespaceCalDAV, condition)
}

func writeCalDAVErrorMulti(w http.ResponseWriter, status int, conditions ...string) {
	if len(conditions) == 0 {
		w.WriteHeader(status)
		return
	}
	writeConditionError(w, status, namespaceCalDAV, conditions...)
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
		if !isValidConditionName(ref.Element) {
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

// writeNumberOfMatchesWithinLimits answers the DAV:number-of-matches-within-limits
// postcondition RFC 4791 §7.8 and §7.10 place on calendar-query and
// free-busy-query. §1.3 fixes the status at 403: the limits are the server's own
// and no resubmission of the same report can bring the match set inside them.
// The client narrows its filter or its time-range instead.
func writeNumberOfMatchesWithinLimits(w http.ResponseWriter) {
	writeDAVError(w, http.StatusForbidden, "number-of-matches-within-limits")
}

// writeDAVError writes a DAV:-namespace precondition error body (RFC 4918 §16),
// e.g. DAV:supported-report or DAV:propfind-finite-depth.
func writeDAVError(w http.ResponseWriter, status int, condition string) {
	writeConditionError(w, status, namespaceDAV, condition)
}

// writeConditionError answers a precondition or postcondition failure with the
// named elements of one namespace under a top-level DAV:error, the placement
// RFC 4918 §16 and RFC 4791 §1.3 both require outside a multistatus.
func writeConditionError(w http.ResponseWriter, status int, namespace string, conditions ...string) {
	w.Header().Set("Content-Type", "application/xml; charset=utf-8")
	w.WriteHeader(status)
	_, _ = fmt.Fprint(w, buildConditionErrorXML(namespace, conditions))
}

// buildConditionErrorXML renders the DAV:error body. A name that is no
// condition name is dropped rather than replaced with a placeholder: each
// namespace is reserved for the elements its specification defines, and an
// invented one would violate that while telling the client nothing the status
// does not.
func buildConditionErrorXML(namespace string, conditions []string) string {
	// One namespace per body, so one prefix covers every condition in it. The
	// DAV: declaration belongs to the error element itself, and a DAV:
	// condition reuses it rather than binding a second prefix to the same URI.
	prefix := "C"
	var builder strings.Builder
	builder.WriteString(`<?xml version="1.0" encoding="utf-8"?><D:error xmlns:D="DAV:"`)
	if namespace == namespaceDAV {
		prefix = "D"
	} else {
		fmt.Fprintf(&builder, ` xmlns:%s=%q`, prefix, namespace)
	}
	builder.WriteString(">")
	for _, condition := range conditions {
		if !isValidConditionName(condition) {
			continue
		}
		fmt.Fprintf(&builder, "<%s:%s/>", prefix, condition)
	}
	builder.WriteString("</D:error>")
	return builder.String()
}
