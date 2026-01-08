package dav

import (
	"fmt"
	"net/http"
	"strings"
)

// isValidCalDAVCondition validates that a condition string is safe for XML output.
// CalDAV condition names must match: ^[a-z][a-z0-9-]*$
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
	// Validate condition to prevent XML injection
	if !isValidCalDAVCondition(condition) {
		// Fallback to generic error if condition is invalid
		condition = "invalid-condition"
	}
	w.Header().Set("Content-Type", "application/xml; charset=utf-8")
	w.WriteHeader(status)
	_, _ = fmt.Fprintf(w, `<?xml version="1.0" encoding="utf-8"?><D:error xmlns:D="DAV:" xmlns:C="urn:ietf:params:xml:ns:caldav"><C:%s/></D:error>`, condition)
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
	var builder strings.Builder
	builder.WriteString(`<?xml version="1.0" encoding="utf-8"?><D:error xmlns:D="DAV:" xmlns:C="urn:ietf:params:xml:ns:caldav">`)
	for _, condition := range conditions {
		if strings.TrimSpace(condition) == "" {
			continue
		}
		// Validate condition to prevent XML injection
		if !isValidCalDAVCondition(condition) {
			continue // Skip invalid conditions
		}
		builder.WriteString("<C:")
		builder.WriteString(condition)
		builder.WriteString("/>")
	}
	builder.WriteString("</D:error>")
	_, _ = fmt.Fprint(w, builder.String())
}
