package dav

import (
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
