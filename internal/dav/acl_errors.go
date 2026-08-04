package dav

import (
	"encoding/xml"
	"errors"
	"fmt"
	"net/http"
	"strings"

	"github.com/jw6ventures/calcard/internal/store"
)

type privilegeRequirementError struct {
	cause           error
	href            string
	privilege       string
	concealNotFound bool
}

func (e *privilegeRequirementError) Error() string {
	return fmt.Sprintf("%s privilege required on %s: %v", e.privilege, e.href, e.cause)
}

func (e *privilegeRequirementError) Unwrap() error { return e.cause }

func requirePrivilegeAt(err error, href, privilege string) error {
	if err == nil {
		return nil
	}
	return &privilegeRequirementError{cause: err, href: normalizeDAVHref(href), privilege: privilege}
}

func requirePrivatePrivilegeAt(err error, href, privilege string) error {
	if err == nil {
		return nil
	}
	return &privilegeRequirementError{cause: err, href: normalizeDAVHref(href), privilege: privilege, concealNotFound: true}
}

func writePrivilegeRequirementError(w http.ResponseWriter, err error) bool {
	var requirement *privilegeRequirementError
	if !errors.As(err, &requirement) {
		return false
	}
	switch {
	case errors.Is(requirement.cause, store.ErrNotFound) && !isPrivilegeNotGranted(requirement.cause):
		http.Error(w, "not found", http.StatusNotFound)
	case (errors.Is(requirement.cause, errForbidden) || isPrivilegeNotGranted(requirement.cause)) && !requirement.concealNotFound:
		writeNeedPrivileges(w, requirement.href, requirement.privilege)
	case errors.Is(requirement.cause, errForbidden) || isPrivilegeNotGranted(requirement.cause):
		http.Error(w, "not found", http.StatusNotFound)
	default:
		http.Error(w, "failed to evaluate privileges", http.StatusInternalServerError)
	}
	return true
}

func writeNeedPrivileges(w http.ResponseWriter, href, privilege string) {
	var escapedHref strings.Builder
	_ = xml.EscapeText(&escapedHref, []byte(normalizeDAVHref(href)))
	privilegeXML := davPrivilegeErrorElement(privilege)
	w.Header().Set("Content-Type", "application/xml; charset=utf-8")
	w.WriteHeader(http.StatusForbidden)
	_, _ = fmt.Fprintf(w, `<?xml version="1.0" encoding="utf-8"?><D:error xmlns:D="DAV:" xmlns:C="urn:ietf:params:xml:ns:caldav"><D:need-privileges><D:resource><D:href>%s</D:href><D:privilege>%s</D:privilege></D:resource></D:need-privileges></D:error>`, escapedHref.String(), privilegeXML)
}

func davPrivilegeErrorElement(privilege string) string {
	switch privilege {
	case "read", "write", "write-content", "write-properties", "read-acl", "read-current-user-privilege-set", "write-acl", "bind", "unbind", "unlock", "all":
		return "<D:" + privilege + "/>"
	case "read-free-busy":
		return "<C:read-free-busy/>"
	default:
		return "<D:all/>"
	}
}
