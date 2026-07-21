package dav

import (
	"crypto/sha256"
	"errors"
	"fmt"
	"io"
	"net/http"
	"path"
	"strings"

	"github.com/jw6ventures/calcard/internal/auth"
	"github.com/jw6ventures/calcard/internal/store"
)

// checkConditional validates If-Match and If-None-Match headers per RFC 7232:
// If-Match requires strong comparison, If-None-Match uses weak comparison.
func checkConditional(r *http.Request, etag string, exists bool) bool {
	if ifMatch := strings.TrimSpace(r.Header.Get("If-Match")); ifMatch != "" {
		if !exists || !etagListMatches(ifMatch, etag, false) {
			return false
		}
	}
	if ifNoneMatch := strings.TrimSpace(r.Header.Get("If-None-Match")); ifNoneMatch != "" {
		if exists && etagListMatches(ifNoneMatch, etag, true) {
			return false
		}
	}
	return true
}

// etagListMatches reports whether any entity-tag in a comma-separated
// If-Match/If-None-Match value matches etag. "*" matches any existing
// resource. Stored ETags are strong, so a weak candidate (W/"...") can only
// weak-compare equal (RFC 7232 §2.3.2); it never matches under the strong
// comparison If-Match requires.
func etagListMatches(headerValue, etag string, allowWeak bool) bool {
	if headerValue == "*" {
		return true
	}
	for _, candidate := range strings.Split(headerValue, ",") {
		candidate = strings.TrimSpace(candidate)
		if strings.HasPrefix(candidate, "W/") {
			if !allowWeak {
				continue
			}
			candidate = strings.TrimPrefix(candidate, "W/")
		}
		candidate = strings.Trim(candidate, "\"")
		if candidate != "" && candidate == etag {
			return true
		}
	}
	return false
}

// checkConditionalHeaders validates If-Match and If-None-Match headers for events
func (h *DavServer) checkConditionalHeaders(r *http.Request, existing *store.Event) bool {
	etag := ""
	if existing != nil {
		etag = existing.ETag
	}
	return checkConditional(r, etag, existing != nil)
}

// checkConditionalHeadersContact validates If-Match and If-None-Match headers for contacts
func (h *DavServer) checkConditionalHeadersContact(r *http.Request, existing *store.Contact) bool {
	etag := ""
	if existing != nil {
		etag = existing.ETag
	}
	return checkConditional(r, etag, existing != nil)
}

var allowedCalendarComponents = map[string]struct{}{
	"VCALENDAR": {},
	"VEVENT":    {},
	"VTODO":     {},
	"VJOURNAL":  {},
	"VFREEBUSY": {},
	"VTIMEZONE": {},
	"STANDARD":  {},
	"DAYLIGHT":  {},
	"VALARM":    {},
}

func (h *DavServer) put(w http.ResponseWriter, r *http.Request) {
	h.logger().Trace("Put", "PUT %s", r.URL.Path)
	user, ok := auth.UserFromContext(r.Context())
	if !ok {
		http.Error(w, "missing user", http.StatusUnauthorized)
		return
	}

	cleanPath := path.Clean(r.URL.Path)
	target := parsedDAVTarget(r.Context(), cleanPath)
	if !h.requireLock(w, r, cleanPath, "resource is locked") {
		return
	}
	if target.Domain == davPathCalendar && !target.Resource {
		http.Error(w, "forbidden", http.StatusForbidden)
		return
	}
	isCalendar := target.Valid && target.Domain == davPathCalendar && target.Resource
	isAddressBook := target.Valid && target.Domain == davPathAddressBook && target.Resource
	if r.ContentLength > maxDAVBodyBytes {
		if isCalendar {
			writeCalDAVError(w, http.StatusRequestEntityTooLarge, "max-resource-size")
		} else if isAddressBook {
			writeCardDAVPrecondition(w, http.StatusRequestEntityTooLarge, "max-resource-size")
		} else {
			http.Error(w, "request too large", http.StatusRequestEntityTooLarge)
		}
		return
	}
	limitedBody := http.MaxBytesReader(w, r.Body, maxDAVBodyBytes)
	body, err := io.ReadAll(limitedBody)
	if err != nil {
		var maxErr *http.MaxBytesError
		if errors.As(err, &maxErr) {
			if isCalendar {
				writeCalDAVError(w, http.StatusRequestEntityTooLarge, "max-resource-size")
			} else if isAddressBook {
				writeCardDAVPrecondition(w, http.StatusRequestEntityTooLarge, "max-resource-size")
			} else {
				http.Error(w, "request too large", http.StatusRequestEntityTooLarge)
			}
		} else {
			http.Error(w, "failed to read body", http.StatusBadRequest)
		}
		return
	}
	etag := fmt.Sprintf("%x", sha256.Sum256(body))
	bodyText := string(body)

	if calendarID, resourceUID, matched, err := h.parseCalendarResourcePath(r.Context(), user, cleanPath); err != nil {
		if err == store.ErrNotFound {
			http.Error(w, "calendar not found", http.StatusNotFound)
			return
		}
		if errors.Is(err, errAmbiguousCalendar) {
			http.Error(w, "ambiguous calendar path", http.StatusConflict)
			return
		}
		http.Error(w, "failed to load calendar", http.StatusInternalServerError)
		return
	} else if matched {
		h.putCalendarObject(w, r, user, calendarID, resourceUID, cleanPath, body, bodyText, etag)
		return
	}

	if addressBookID, _, matched, err := h.parseAddressBookResourcePath(r.Context(), user, cleanPath); err != nil {
		if err == store.ErrNotFound {
			http.Error(w, "address book not found", http.StatusNotFound)
			return
		}
		if errors.Is(err, errAmbiguousAddressBook) {
			http.Error(w, "ambiguous address book path", http.StatusConflict)
			return
		}
		http.Error(w, "failed to load address book", http.StatusInternalServerError)
		return
	} else if matched {
		h.putContact(w, r, user, addressBookID, cleanPath, body, bodyText, etag)
		return
	}

	http.Error(w, "unsupported path", http.StatusBadRequest)
}

func (h *DavServer) putCalendarObject(w http.ResponseWriter, r *http.Request, user *store.User, calendarID int64, resourceUID, cleanPath string, body []byte, bodyText, etag string) {
	existingByResource, err := h.store.Events.GetByResourceName(r.Context(), calendarID, resourceUID)
	if err != nil {
		http.Error(w, "failed to load event", http.StatusInternalServerError)
		return
	}
	requiredPrivilege := "bind"
	if existingByResource != nil {
		requiredPrivilege = "write-content"
	}
	_, err = h.loadCalendarWithPrivilege(r.Context(), user, calendarID, cleanPath, requiredPrivilege)
	if err != nil {
		status := http.StatusInternalServerError
		if err == store.ErrNotFound {
			status = http.StatusNotFound
		}
		if errors.Is(err, errForbidden) {
			status = http.StatusForbidden
		}
		http.Error(w, http.StatusText(status), status)
		return
	}

	contentType := strings.ToLower(strings.TrimSpace(r.Header.Get("Content-Type")))
	missingContentType := contentType == ""
	if contentType != "" &&
		!strings.HasPrefix(contentType, "text/calendar") &&
		!strings.HasPrefix(contentType, "application/ical") &&
		!strings.HasPrefix(contentType, "application/ics") {
		writeCalDAVError(w, http.StatusUnsupportedMediaType, "supported-calendar-data")
		return
	}

	analysis, err := analyzeICalendar(bodyText)
	if err != nil {
		writeCalDAVError(w, http.StatusBadRequest, "valid-calendar-data")
		return
	}

	componentTypes := analysis.ComponentTypes
	for comp := range componentTypes {
		if _, ok := allowedCalendarComponents[comp]; !ok {
			writeCalDAVError(w, http.StatusForbidden, "supported-calendar-component")
			return
		}
	}
	_, hasEvent := componentTypes["VEVENT"]
	_, hasTodo := componentTypes["VTODO"]
	_, hasJournal := componentTypes["VJOURNAL"]
	_, hasFreeBusy := componentTypes["VFREEBUSY"]
	if !hasEvent && !hasTodo && !hasJournal && !hasFreeBusy {
		writeCalDAVError(w, http.StatusForbidden, "valid-calendar-component")
		return
	}

	if analysis.HasMethod {
		writeCalDAVError(w, http.StatusConflict, "valid-calendar-object-resource")
		return
	}

	if conditions := analysis.objectValidationConditions(); len(conditions) > 0 {
		if len(conditions) == 2 {
			writeCalDAVError(w, http.StatusConflict, "valid-calendar-object-resource")
			return
		}
		writeCalDAVErrorMulti(w, http.StatusBadRequest, conditions...)
		return
	}

	minDate, maxDate := caldavDateLimits()
	for _, t := range analysis.DateTimes {
		if t.Before(minDate) {
			writeCalDAVError(w, http.StatusForbidden, "min-date-time")
			return
		}
		if t.After(maxDate) {
			writeCalDAVError(w, http.StatusForbidden, "max-date-time")
			return
		}
	}

	if analysis.MaxAttendees > caldavMaxAttendees {
		writeCalDAVError(w, http.StatusForbidden, "max-attendees-per-instance")
		return
	}
	if analysis.HasRRULECount && analysis.MaxRRULECount > caldavMaxInstances {
		writeCalDAVError(w, http.StatusForbidden, "max-instances")
		return
	}

	if missingContentType {
		writeCalDAVError(w, http.StatusUnsupportedMediaType, "supported-calendar-data")
		return
	}

	uid, err := analysis.uid()
	if err != nil {
		writeCalDAVError(w, http.StatusBadRequest, "valid-calendar-object-resource")
		return
	}
	resourceName := resourceUID
	if existingByResource == nil && !h.requireLock(w, r, path.Dir(cleanPath), "resource is locked") {
		return
	}
	if existingByResource != nil && existingByResource.UID != uid {
		// Reject: client trying to change UID of existing resource
		writeCalDAVError(w, http.StatusConflict, "no-uid-conflict")
		return
	}

	existing, err := h.store.Events.GetByUID(r.Context(), calendarID, uid)
	if err != nil {
		http.Error(w, "failed to load event", http.StatusInternalServerError)
		return
	}
	if existing != nil && existing.ResourceName != "" && existing.ResourceName != resourceName {
		// Reject: client trying to use same UID at different path
		writeCalDAVError(w, http.StatusConflict, "no-uid-conflict")
		return
	}

	if !h.checkConditionalHeaders(r, existing) {
		http.Error(w, "precondition failed", http.StatusPreconditionFailed)
		return
	}

	if err := h.davRegistry().validatePut(PutValidation{
		Context:      r.Context(),
		User:         user,
		Request:      r,
		Path:         cleanPath,
		ResourceType: ResourceTypeCalendarObject,
		CollectionID: calendarID,
		ResourceName: resourceName,
		ContentType:  contentType,
		Body:         body,
		ETag:         etag,
	}); writeResponseError(w, err) {
		return
	}

	if _, err := h.store.Events.Upsert(r.Context(), store.Event{CalendarID: calendarID, UID: uid, ResourceName: resourceName, RawICAL: bodyText, ETag: etag, WriteMetadata: &analysis.Metadata}); err != nil {
		if errors.Is(err, store.ErrConflict) {
			writeCalDAVError(w, http.StatusConflict, "no-uid-conflict")
			return
		}
		h.logger().Error("Put", "failed to save event %q in calendar %d: %v", uid, calendarID, err)
		http.Error(w, "failed to save event", http.StatusInternalServerError)
		return
	}
	w.Header().Set("ETag", fmt.Sprintf("\"%s\"", etag))
	if existing == nil {
		h.logger().Info("Put", "created event %q in calendar %d", uid, calendarID)
		w.WriteHeader(http.StatusCreated)
	} else {
		h.logger().Info("Put", "updated event %q in calendar %d", uid, calendarID)
		w.WriteHeader(http.StatusNoContent)
	}
}

func (h *DavServer) putContact(w http.ResponseWriter, r *http.Request, user *store.User, addressBookID int64, cleanPath string, body []byte, bodyText, etag string) {
	book, err := h.getAddressBook(r.Context(), addressBookID)
	if err != nil {
		status := http.StatusInternalServerError
		if err == store.ErrNotFound {
			status = http.StatusNotFound
		}
		http.Error(w, "address book not found", status)
		return
	}

	contentType := strings.ToLower(strings.TrimSpace(r.Header.Get("Content-Type")))
	if contentType != "" && !strings.HasPrefix(contentType, "text/vcard") {
		writeCardDAVPrecondition(w, http.StatusUnsupportedMediaType, "supported-address-data")
		return
	}

	if err := h.validateVCard(bodyText); err != nil {
		writeCardDAVPrecondition(w, http.StatusBadRequest, "valid-address-data")
		return
	}

	uid, err := extractUIDFromVCard(bodyText)
	if err != nil {
		writeCardDAVPrecondition(w, http.StatusBadRequest, "valid-address-data")
		return
	}

	// UID conflict detection (RFC 6352 §5.1, §6.3.2.1)
	resourceName := parsedDAVTarget(r.Context(), cleanPath).ResourceName

	// Check if an existing resource at this path has a different UID
	existingByName, err := h.store.Contacts.GetByResourceName(r.Context(), addressBookID, resourceName)
	if err != nil {
		http.Error(w, "failed to load contact", http.StatusInternalServerError)
		return
	}
	if existingByName == nil && !h.requireLock(w, r, path.Dir(cleanPath), "resource is locked") {
		return
	}
	requiredPrivilege := "bind"
	if existingByName != nil {
		requiredPrivilege = "write-content"
	}
	if err := h.requireAddressBookPrivilege(r.Context(), user, book, cleanPath, requiredPrivilege); err != nil {
		status := http.StatusForbidden
		if err == store.ErrNotFound {
			status = http.StatusNotFound
		}
		http.Error(w, http.StatusText(status), status)
		return
	}
	if existingByName != nil && existingByName.UID != uid {
		conflictHref := fmt.Sprintf("/dav/addressbooks/%d/%s.vcf", addressBookID, contactResourceName(*existingByName))
		writeCardDAVUIDConflict(w, conflictHref)
		return
	}

	// Check if another resource already uses this UID
	existingByUID, err := h.store.Contacts.GetByUID(r.Context(), addressBookID, uid)
	if err != nil {
		http.Error(w, "failed to load contact", http.StatusInternalServerError)
		return
	}
	if existingByUID != nil && contactResourceName(*existingByUID) != resourceName {
		conflictHref := fmt.Sprintf("/dav/addressbooks/%d/%s.vcf", addressBookID, contactResourceName(*existingByUID))
		writeCardDAVUIDConflict(w, conflictHref)
		return
	}

	existing := existingByUID

	if !h.checkConditionalHeadersContact(r, existing) {
		http.Error(w, "precondition failed", http.StatusPreconditionFailed)
		return
	}

	if err := h.davRegistry().validatePut(PutValidation{
		Context:      r.Context(),
		User:         user,
		Request:      r,
		Path:         cleanPath,
		ResourceType: ResourceTypeAddressObject,
		CollectionID: addressBookID,
		ResourceName: resourceName,
		ContentType:  contentType,
		Body:         body,
		ETag:         etag,
	}); writeResponseError(w, err) {
		return
	}

	if existingByName == nil {
		if err := h.deleteDAVACLState(r.Context(), user, cleanPath); err != nil {
			http.Error(w, "failed to reset resource ACL state", http.StatusInternalServerError)
			return
		}
	}

	if _, err := h.store.Contacts.Upsert(r.Context(), store.Contact{AddressBookID: addressBookID, UID: uid, ResourceName: resourceName, RawVCard: bodyText, ETag: etag}); err != nil {
		if errors.Is(err, store.ErrConflict) {
			writeCardDAVUIDConflict(w, cleanPath)
			return
		}
		h.logger().Error("Put", "failed to save contact %q in address book %d: %v", uid, addressBookID, err)
		http.Error(w, "failed to save contact", http.StatusInternalServerError)
		return
	}
	w.Header().Set("ETag", fmt.Sprintf("\"%s\"", etag))
	if existing == nil {
		h.logger().Info("Put", "created contact %q in address book %d", uid, addressBookID)
		w.WriteHeader(http.StatusCreated)
	} else {
		h.logger().Info("Put", "updated contact %q in address book %d", uid, addressBookID)
		w.WriteHeader(http.StatusNoContent)
	}
}
