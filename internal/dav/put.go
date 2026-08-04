package dav

import (
	"crypto/sha256"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"path"
	"slices"
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
	return headerValue == "*" || slices.Contains(entityTags(headerValue, allowWeak), etag)
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

// calendarAcceptsComponents reports whether every top-level component type in
// the submitted object appears in the target collection's
// CALDAV:supported-calendar-component-set. A collection carrying no set of its
// own falls back to the server default, which RFC 4791 §5.2.3 makes the meaning
// of an absent property.
func calendarAcceptsComponents(cal *store.CalendarAccess, components []calendarTopLevelComponent) bool {
	var stored []string
	if cal != nil {
		stored = cal.SupportedComponents
	}
	allowed := calendarSupportedComponents(stored)
	for _, component := range components {
		if !slices.Contains(allowed, component.Type) {
			return false
		}
	}
	return true
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
			// §1.3: a body over the limit fails however often it is resubmitted.
			writeCalDAVError(w, http.StatusForbidden, "max-resource-size")
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
				writeCalDAVError(w, http.StatusForbidden, "max-resource-size")
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
		if errors.Is(err, store.ErrNotFound) {
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
		if errors.Is(err, store.ErrNotFound) {
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
	cal, existingByResource, ok := h.authorizeCalendarObjectTarget(w, r, user, calendarID, resourceUID, cleanPath)
	if !ok {
		return
	}

	contentType := strings.TrimSpace(r.Header.Get("Content-Type"))
	missingContentType := contentType == ""
	if !missingContentType && !mediaTypeAdvertised(contentType, "text/calendar", calendarDataVersions) {
		writeCalDAVError(w, http.StatusForbidden, "supported-calendar-data")
		return
	}

	validated, fault := validateCalendarObjectForStorage(bodyText, cal)
	if fault != nil {
		writeCalendarObjectFault(w, fault)
		return
	}

	if missingContentType {
		writeCalDAVError(w, http.StatusForbidden, "supported-calendar-data")
		return
	}

	uid := validated.UID
	resourceName := resourceUID
	if existingByResource == nil && !h.requireLock(w, r, path.Dir(cleanPath), "resource is locked") {
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

	write := store.CalendarObjectWrite{
		CalendarID:           calendarID,
		UID:                  uid,
		ResourceName:         resourceName,
		RawICAL:              bodyText,
		ETag:                 etag,
		Metadata:             &validated.Analysis.Metadata,
		Precondition:         calendarObjectPrecondition(r),
		ExpectedState:        &store.CalendarObjectResourceState{Exists: existingByResource != nil},
		ExpectedCalendarCTag: &cal.CTag,
		LockPreconditions:    h.lockPreconditions(r, cleanPath, path.Dir(cleanPath)),
	}
	result, err := h.store.PutCalendarObject(r.Context(), write)
	if errors.Is(err, store.ErrResourceStateChanged) {
		currentCal, current, authorized := h.authorizeCalendarObjectTarget(w, r, user, calendarID, resourceUID, cleanPath)
		if !authorized {
			return
		}
		if !calendarAcceptsComponents(currentCal, validated.Analysis.Components) {
			writeCalDAVError(w, http.StatusForbidden, "supported-calendar-component")
			return
		}
		if current == nil && !h.requireLock(w, r, path.Dir(cleanPath), "resource is locked") {
			return
		}
		write.ExpectedState = &store.CalendarObjectResourceState{Exists: current != nil}
		write.ExpectedCalendarCTag = &currentCal.CTag
		result, err = h.store.PutCalendarObject(r.Context(), write)
	}
	switch {
	case errors.Is(err, store.ErrUIDConflict):
		var conflict *store.Event
		if result != nil {
			conflict = result.Conflict
		}
		if conflict == nil {
			h.logger().Error("Put", "calendar object UID conflict did not identify a resource for %q in calendar %d", uid, calendarID)
			http.Error(w, "failed to save event", http.StatusInternalServerError)
			return
		}
		writeCalDAVUIDConflict(w, calendarObjectHref(calendarID, conflict))
		return
	case errors.Is(err, store.ErrPreconditionFailed):
		http.Error(w, "precondition failed", http.StatusPreconditionFailed)
		return
	case errors.Is(err, store.ErrConflict):
		h.logger().Error("Put", "calendar object store conflict did not identify a resource for %q in calendar %d", uid, calendarID)
		http.Error(w, "failed to save event", http.StatusInternalServerError)
		return
	case errors.Is(err, store.ErrResourceStateChanged):
		http.Error(w, "resource changed while writing; retry the request", http.StatusConflict)
		return
	case err != nil:
		h.logger().Error("Put", "failed to save event %q in calendar %d: %v", uid, calendarID, err)
		http.Error(w, "failed to save event", http.StatusInternalServerError)
		return
	case result == nil:
		h.logger().Error("Put", "calendar object write returned no result for %q in calendar %d", uid, calendarID)
		http.Error(w, "failed to save event", http.StatusInternalServerError)
		return
	}

	// §5.3.4: the strong ETag says the stored octets are the submitted ones, so
	// it is only returned once the store confirms it kept them unchanged.
	if result.Event != nil && result.Event.RawICAL == bodyText {
		w.Header().Set("ETag", fmt.Sprintf("\"%s\"", result.Event.ETag))
	}
	if result.Created {
		h.logger().Info("Put", "created event %q in calendar %d", uid, calendarID)
		w.WriteHeader(http.StatusCreated)
	} else {
		h.logger().Info("Put", "updated event %q in calendar %d", uid, calendarID)
		w.WriteHeader(http.StatusNoContent)
	}
}

// calendarObjectPrecondition translates the request's conditional headers into
// the form the store re-evaluates inside its write transaction. RFC 7232 §3.1
// gives If-Match strong comparison, so a weak candidate never matches; §3.2
// gives If-None-Match weak comparison, so one may.
func calendarObjectPrecondition(r *http.Request) store.CalendarObjectPrecondition {
	precondition := store.CalendarObjectPrecondition{}
	if ifMatch := strings.TrimSpace(r.Header.Get("If-Match")); ifMatch != "" {
		precondition.IfMatch = &store.ETagCondition{Any: ifMatch == "*", ETags: entityTags(ifMatch, false)}
	}
	if ifNoneMatch := strings.TrimSpace(r.Header.Get("If-None-Match")); ifNoneMatch != "" {
		precondition.IfNoneMatch = &store.ETagCondition{Any: ifNoneMatch == "*", ETags: entityTags(ifNoneMatch, true)}
	}
	return precondition
}

// entityTags parses a comma-separated If-Match or If-None-Match value into the
// bare entity tags it names, dropping weak ones where the comparison is strong.
func entityTags(headerValue string, allowWeak bool) []string {
	var tags []string
	for _, candidate := range strings.Split(headerValue, ",") {
		candidate = strings.TrimSpace(candidate)
		if strings.HasPrefix(candidate, "W/") {
			if !allowWeak {
				continue
			}
			candidate = strings.TrimPrefix(candidate, "W/")
		}
		if candidate = strings.Trim(candidate, "\""); candidate != "" {
			tags = append(tags, candidate)
		}
	}
	return tags
}

// calendarObjectHref is the URL of one calendar object resource, as the
// CALDAV:no-uid-conflict error body reports it.
func calendarObjectHref(calendarID int64, event *store.Event) string {
	if event == nil {
		return ""
	}
	return fmt.Sprintf("/dav/calendars/%d/%s.ics", calendarID, url.PathEscape(eventResourceName(*event)))
}

func (h *DavServer) authorizeCalendarObjectTarget(w http.ResponseWriter, r *http.Request, user *store.User, calendarID int64, resourceName, cleanPath string) (*store.CalendarAccess, *store.Event, bool) {
	existing, err := h.store.Events.GetByResourceName(r.Context(), calendarID, resourceName)
	if err != nil {
		http.Error(w, "failed to load event", http.StatusInternalServerError)
		return nil, nil, false
	}
	requiredPrivilege := "bind"
	privilegePath := path.Dir(cleanPath)
	if existing != nil {
		requiredPrivilege = "write-content"
		privilegePath = cleanPath
	}
	cal, err := h.loadCalendarWithPrivilege(r.Context(), user, calendarID, privilegePath, requiredPrivilege)
	if err != nil {
		_ = writePrivilegeRequirementError(w, requirePrivilegeAt(err, privilegePath, requiredPrivilege))
		return nil, nil, false
	}
	return cal, existing, true
}

func (h *DavServer) putContact(w http.ResponseWriter, r *http.Request, user *store.User, addressBookID int64, cleanPath string, body []byte, bodyText, etag string) {
	book, err := h.getAddressBook(r.Context(), addressBookID)
	if err != nil {
		status := http.StatusInternalServerError
		if errors.Is(err, store.ErrNotFound) {
			status = http.StatusNotFound
		}
		http.Error(w, "address book not found", status)
		return
	}

	contentType := strings.TrimSpace(r.Header.Get("Content-Type"))
	if contentType != "" && !mediaTypeAdvertised(contentType, "text/vcard", addressDataVersions) {
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
	privilegePath := path.Dir(cleanPath)
	if existingByName != nil {
		requiredPrivilege = "write-content"
		privilegePath = cleanPath
	}
	if err := h.requireAddressBookPrivilege(r.Context(), user, book, privilegePath, requiredPrivilege); err != nil {
		_ = writePrivilegeRequirementError(w, requirePrivilegeAt(err, privilegePath, requiredPrivilege))
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

	canonicalPath, err := h.canonicalDAVPath(r.Context(), user, cleanPath)
	if err != nil {
		http.Error(w, "failed to resolve resource state", http.StatusInternalServerError)
		return
	}
	result, err := h.store.PutContactObject(r.Context(), store.ContactObjectWrite{
		AddressBookID:           addressBookID,
		UID:                     uid,
		ResourceName:            resourceName,
		RawVCard:                bodyText,
		ETag:                    etag,
		Precondition:            calendarObjectPrecondition(r),
		ExpectedState:           store.ContactDAVResourceState(existingByName),
		ExpectedAddressBookCTag: &book.CTag,
		StatePath:               canonicalPath,
		LockPreconditions:       h.lockPreconditions(r, cleanPath, path.Dir(cleanPath)),
	})
	switch {
	case errors.Is(err, store.ErrUIDConflict):
		if result == nil || result.Conflict == nil {
			http.Error(w, "failed to save contact", http.StatusInternalServerError)
			return
		}
		writeCardDAVUIDConflict(w, fmt.Sprintf("/dav/addressbooks/%d/%s.vcf", addressBookID, contactResourceName(*result.Conflict)))
		return
	case errors.Is(err, store.ErrConflict):
		writeCardDAVUIDConflict(w, cleanPath)
		return
	case errors.Is(err, store.ErrPreconditionFailed), errors.Is(err, store.ErrResourceStateChanged):
		http.Error(w, "precondition failed", http.StatusPreconditionFailed)
		return
	case errors.Is(err, store.ErrLockConflict):
		http.Error(w, "resource is locked", http.StatusLocked)
		return
	case err != nil:
		h.logger().Error("Put", "failed to save contact %q in address book %d: %v", uid, addressBookID, err)
		http.Error(w, "failed to save contact", http.StatusInternalServerError)
		return
	}
	w.Header().Set("ETag", fmt.Sprintf("\"%s\"", etag))
	if result != nil && result.Created {
		h.logger().Info("Put", "created contact %q in address book %d", uid, addressBookID)
		w.WriteHeader(http.StatusCreated)
	} else {
		h.logger().Info("Put", "updated contact %q in address book %d", uid, addressBookID)
		w.WriteHeader(http.StatusNoContent)
	}
}
