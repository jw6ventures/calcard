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

// checkConditionalHeaders validates If-Match and If-None-Match headers for events
func (h *DavServer) checkConditionalHeaders(r *http.Request, existing *store.Event) bool {
	ifMatch := r.Header.Get("If-Match")
	ifNoneMatch := r.Header.Get("If-None-Match")

	// If-None-Match: * means "only create if doesn't exist"
	if ifNoneMatch == "*" {
		return existing == nil
	}

	// If-Match requires the resource to exist and match the given ETag
	if ifMatch != "" {
		if existing == nil {
			return false
		}
		requestETag := strings.Trim(ifMatch, "\"")
		return requestETag == existing.ETag
	}

	// If-None-Match with specific ETag means "only update if ETag doesn't match"
	if ifNoneMatch != "" {
		if existing == nil {
			return true
		}
		requestETag := strings.Trim(ifNoneMatch, "\"")
		return requestETag != existing.ETag
	}

	// No conditional headers, allow the request
	return true
}

// checkConditionalHeadersContact validates If-Match and If-None-Match headers for contacts
func (h *DavServer) checkConditionalHeadersContact(r *http.Request, existing *store.Contact) bool {
	ifMatch := r.Header.Get("If-Match")
	ifNoneMatch := r.Header.Get("If-None-Match")

	if ifNoneMatch == "*" {
		return existing == nil
	}

	if ifMatch != "" {
		if existing == nil {
			return false
		}
		requestETag := strings.Trim(ifMatch, "\"")
		return requestETag == existing.ETag
	}

	if ifNoneMatch != "" {
		if existing == nil {
			return true
		}
		requestETag := strings.Trim(ifNoneMatch, "\"")
		return requestETag != existing.ETag
	}

	return true
}

func (h *DavServer) Put(w http.ResponseWriter, r *http.Request) {
	if h.handleRegisteredMethod(w, r) {
		return
	}
	h.logger().Trace("Put", "PUT %s", r.URL.Path)
	user, ok := auth.UserFromContext(r.Context())
	if !ok {
		http.Error(w, "missing user", http.StatusUnauthorized)
		return
	}

	cleanPath := path.Clean(r.URL.Path)
	if !h.requireLock(w, r, cleanPath, "resource is locked") {
		return
	}
	if cleanPath == "/dav/calendars" || cleanPath == "/dav/calendars/" {
		http.Error(w, "forbidden", http.StatusForbidden)
		return
	}
	if strings.HasPrefix(cleanPath, "/dav/calendars/") {
		if _, _, ok := parseCalendarResourceSegments(cleanPath); !ok {
			http.Error(w, "forbidden", http.StatusForbidden)
			return
		}
	}
	_, _, isCalendar := parseCalendarResourceSegments(cleanPath)
	_, _, isAddressBook := parseAddressBookResourceSegments(cleanPath)
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
		if calendarID == birthdayCalendarID {
			http.Error(w, "birthday calendar is read-only", http.StatusForbidden)
			return
		}

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

		if err := h.validateICalendar(string(body)); err != nil {
			writeCalDAVError(w, http.StatusBadRequest, "valid-calendar-data")
			return
		}

		componentTypes := extractICalComponentTypes(string(body))
		allowedComponents := map[string]struct{}{
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
		for comp := range componentTypes {
			if _, ok := allowedComponents[comp]; !ok {
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

		if containsICalMethodProperty(string(body)) {
			writeCalDAVError(w, http.StatusConflict, "valid-calendar-object-resource")
			return
		}

		if conditions := validateCalendarObjectResource(string(body)); len(conditions) > 0 {
			if hasMultipleDifferentUIDs(string(body)) {
				writeCalDAVError(w, http.StatusConflict, "valid-calendar-object-resource")
				return
			}
			writeCalDAVErrorMulti(w, http.StatusBadRequest, conditions...)
			return
		}

		minDate, maxDate := caldavDateLimits()
		for _, t := range extractICalDateTimes(string(body)) {
			if t.Before(minDate) {
				writeCalDAVError(w, http.StatusForbidden, "min-date-time")
				return
			}
			if t.After(maxDate) {
				writeCalDAVError(w, http.StatusForbidden, "max-date-time")
				return
			}
		}

		if attendeeCount := countICalAttendees(string(body)); attendeeCount > caldavMaxAttendees {
			writeCalDAVError(w, http.StatusForbidden, "max-attendees-per-instance")
			return
		}
		if count, ok := extractICalRRULECount(string(body)); ok && count > caldavMaxInstances {
			writeCalDAVError(w, http.StatusForbidden, "max-instances")
			return
		}

		if missingContentType {
			writeCalDAVError(w, http.StatusUnsupportedMediaType, "supported-calendar-data")
			return
		}

		uid, err := extractUIDFromICalendar(string(body))
		if err != nil {
			writeCalDAVError(w, http.StatusBadRequest, "valid-calendar-object-resource")
			return
		}
		resourceName := resourceUID
		if resourceName == "" {
			resourceName = uid
		}

		existingByResource, err = h.store.Events.GetByResourceName(r.Context(), calendarID, resourceName)
		if err != nil {
			http.Error(w, "failed to load event", http.StatusInternalServerError)
			return
		}
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

		if _, err := h.store.Events.Upsert(r.Context(), store.Event{CalendarID: calendarID, UID: uid, ResourceName: resourceName, RawICAL: string(body), ETag: etag}); err != nil {
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

		if err := h.validateVCard(string(body)); err != nil {
			writeCardDAVPrecondition(w, http.StatusBadRequest, "valid-address-data")
			return
		}

		uid, err := extractUIDFromVCard(string(body))
		if err != nil {
			writeCardDAVPrecondition(w, http.StatusBadRequest, "valid-address-data")
			return
		}

		// UID conflict detection (RFC 6352 §5.1, §6.3.2.1)
		_, resourceName, _ := parseAddressBookResourceSegments(cleanPath)

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

		if _, err := h.store.Contacts.Upsert(r.Context(), store.Contact{AddressBookID: addressBookID, UID: uid, ResourceName: resourceName, RawVCard: string(body), ETag: etag}); err != nil {
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
		return
	}

	http.Error(w, "unsupported path", http.StatusBadRequest)
}
