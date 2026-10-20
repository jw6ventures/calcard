package dav

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"io"
	"net/http"
	"path"
	"slices"
	"strings"

	"github.com/jw6ventures/calcard/internal/auth"
	"github.com/jw6ventures/calcard/internal/store"
)

// maxResourceStateRetries bounds how often a write re-reads the state it was
// authorized against and tries again after store.ErrResourceStateChanged.
//
// Nothing about such a request was wrong: it lost a race for the resource's
// create-or-update shape, and RFC 4918 §9.7 owes it the 201 or 204 the write
// itself earns, or the 412 its own conditional header earns. A bare 409 tells
// the client nothing it can act on, so the loser retries instead.
//
// Every loss means some other writer committed, so the bound is on how many
// writers contend for one resource rather than on elapsed time. Past it the
// write still answers 409, which keeps an adversarial client from holding a
// request open indefinitely.
const maxResourceStateRetries = 16

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

func (h *DavServer) checkConditionalHeaders(r *http.Request, existing *store.Event) bool {
	etag := ""
	if existing != nil {
		etag = existing.ETag
	}
	return checkConditional(r, etag, existing != nil)
}

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

	// Each write is authorized before the locks are consulted or the body is
	// read, so a principal refused the write gets the same answer whatever
	// locks are held and whatever it sent.
	cleanPath := path.Clean(r.URL.Path)
	target := parsedDAVTarget(r.Context(), cleanPath)
	if target.Domain == davPathCalendar && !target.Resource {
		http.Error(w, "forbidden", http.StatusForbidden)
		return
	}

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
		h.putCalendarObject(w, r, user, calendarID, resourceUID, cleanPath)
		return
	}

	if addressBookID, resourceName, matched, err := h.parseAddressBookResourcePath(r.Context(), user, cleanPath); err != nil {
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
		h.putContact(w, r, user, addressBookID, resourceName, cleanPath)
		return
	}

	if _, ok := readPutBody(w, r, davPathUnknown); ok {
		http.Error(w, "unsupported path", http.StatusBadRequest)
	}
}

// readPutBody reads a PUT body up to the resource size limit, answering the
// request itself when it cannot. domain picks how an oversized body is refused.
func readPutBody(w http.ResponseWriter, r *http.Request, domain davPathDomain) ([]byte, bool) {
	writeTooLarge := func() {
		switch domain {
		case davPathCalendar:
			// §1.3: a body over the limit fails however often it is resubmitted.
			writeCalDAVError(w, http.StatusForbidden, "max-resource-size")
		case davPathAddressBook:
			writeCardDAVPrecondition(w, http.StatusRequestEntityTooLarge, "max-resource-size")
		default:
			http.Error(w, "request too large", http.StatusRequestEntityTooLarge)
		}
	}
	if r.ContentLength > maxDAVBodyBytes {
		writeTooLarge()
		return nil, false
	}
	body, err := io.ReadAll(http.MaxBytesReader(w, r.Body, maxDAVBodyBytes))
	if err != nil {
		var maxErr *http.MaxBytesError
		if errors.As(err, &maxErr) {
			writeTooLarge()
		} else {
			http.Error(w, "failed to read body", http.StatusBadRequest)
		}
		return nil, false
	}
	return body, true
}

func (h *DavServer) putCalendarObject(w http.ResponseWriter, r *http.Request, user *store.User, calendarID int64, resourceUID, cleanPath string) {
	aclGuard, err := h.aclGuard(r.Context(), user, cleanPath)
	if err != nil {
		http.Error(w, "failed to evaluate ACL", http.StatusInternalServerError)
		return
	}
	cal, existingByResource, ok := h.authorizeCalendarObjectTarget(w, r, user, calendarID, resourceUID, cleanPath)
	if !ok {
		return
	}
	if !h.requireLock(w, r, cleanPath, "resource is locked") {
		return
	}
	body, ok := readPutBody(w, r, davPathCalendar)
	if !ok {
		return
	}
	etag := fmt.Sprintf("%x", sha256.Sum256(body))
	bodyText := string(body)

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

	// The store keeps the body the validator returns: the submitted octets
	// less a leading byte order mark, completed as completeCalendarObject
	// describes. A DATE DTEND on its DTSTART's date is written as the next
	// day, and a TZID parameter on a date property is trimmed of white space.
	// While the object stays within the resource size limit, a TZID naming
	// UTC is rewritten as the UTC value, VERSION, PRODID and DTSTAMP are added
	// where missing, and the host's definition of each zone the object names
	// without defining is added, replacing any definition of that zone the
	// server wrote before. The ETag names the stored octets, and the response
	// withholds it when they differ from the submitted ones.
	storedText := validated.Body
	if storedText != bodyText {
		body = []byte(storedText)
		etag = fmt.Sprintf("%x", sha256.Sum256(body))
	}
	uid := validated.UID
	resourceName := resourceUID
	if !storableMemberName(resourceName) {
		writeCalDAVError(w, http.StatusForbidden, "valid-calendar-object-resource")
		return
	}
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

	lockPreconditions, ok := h.writeLockPreconditions(w, r, putLockPaths(cleanPath, existingByResource == nil)...)
	if !ok {
		return
	}
	write := store.CalendarObjectWrite{
		CalendarID:        calendarID,
		UID:               uid,
		ResourceName:      resourceName,
		RawICAL:           storedText,
		ETag:              etag,
		Metadata:          &validated.Analysis.Metadata,
		Precondition:      calendarObjectPrecondition(r),
		ExpectedState:     &store.CalendarObjectResourceState{Exists: existingByResource != nil},
		ExpectedACL:       aclGuard,
		LockPreconditions: lockPreconditions,
	}
	result, err := h.store.PutCalendarObject(r.Context(), write)
	for attempt := 0; attempt < maxResourceStateRetries && errors.Is(err, store.ErrResourceStateChanged); attempt++ {
		// The per-request caches still hold the state the lost attempt was
		// authorized against, so they have to be dropped first: re-reading through
		// them would resend the same stale snapshot and lose again for the same
		// reason.
		invalidateDAVRequestState(r.Context())
		currentACL, aclErr := h.aclGuard(r.Context(), user, cleanPath)
		if aclErr != nil {
			http.Error(w, "failed to evaluate ACL", http.StatusInternalServerError)
			return
		}
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
		// The conditional headers are re-evaluated inside the write transaction
		// against the state read here, so a writer whose own precondition is what
		// failed still answers 412 rather than being retried into a wrong result.
		write.ExpectedState = &store.CalendarObjectResourceState{Exists: current != nil}
		write.ExpectedACL = currentACL
		if write.LockPreconditions, ok = h.writeLockPreconditions(w, r, putLockPaths(cleanPath, current == nil)...); !ok {
			return
		}
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
		writeCalDAVUIDConflict(w, calendarObjectConflictHref(calendarID, conflict))
		return
	case errors.Is(err, store.ErrPreconditionFailed):
		http.Error(w, "precondition failed", http.StatusPreconditionFailed)
		return
	case errors.Is(err, store.ErrLockConflict):
		http.Error(w, "resource is locked", http.StatusLocked)
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

// calendarObjectConflictHref is the URL of the calendar object resource already
// holding a UID, as the CALDAV:no-uid-conflict error body reports it. RFC 4791
// §5.3.2.1 asks for that URL and the element's DTD requires it, so it is named
// even to a requester that may not read the resource. That is deliberate: the
// requester already knows the UID and may add or rewrite a member of the
// collection, so it learns one href in a collection it can write to.
func calendarObjectConflictHref(calendarID int64, event *store.Event) string {
	if event == nil {
		return ""
	}
	return calendarObjectHref(fmt.Sprintf("/dav/calendars/%d", calendarID), eventResourceName(*event))
}

// addressObjectConflictHref is its CardDAV:no-uid-conflict counterpart, which
// RFC 6352 §6.3.2.1 asks for in the same way.
func addressObjectConflictHref(addressBookID int64, resourceName string) string {
	return addressObjectHref(fmt.Sprintf("/dav/addressbooks/%d", addressBookID), resourceName)
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
		refusal := requirePrivilegeAt(err, privilegePath, requiredPrivilege)
		if existing != nil {
			refusal = h.existingObjectWriteRefusal(r.Context(), user, cleanPath, refusal, func() error {
				return h.calendarMemberBindRefusal(r.Context(), user, calendarID, cleanPath)
			})
		}
		_ = writePrivilegeRequirementError(w, refusal)
		return nil, nil, false
	}
	return cal, existing, true
}

// calendarMemberBindRefusal is the refusal a write creating the member at
// cleanPath meets: DAV:bind on its calendar, or nil when user holds it.
func (h *DavServer) calendarMemberBindRefusal(ctx context.Context, user *store.User, calendarID int64, cleanPath string) error {
	parentPath := path.Dir(cleanPath)
	if _, err := h.loadCalendarWithPrivilege(ctx, user, calendarID, parentPath, "bind"); err != nil {
		return requirePrivilegeAt(err, parentPath, "bind")
	}
	return nil
}

// putLockPaths names the resources whose locks a PUT to cleanPath must hold
// the tokens for: the target, and its collection when the PUT creates a
// member. RFC 4918 §7.5 has a depth 0 lock on a collection cover its
// membership, not the content of its members.
func putLockPaths(cleanPath string, createsMember bool) []string {
	if createsMember {
		return []string{cleanPath, path.Dir(cleanPath)}
	}
	return []string{cleanPath}
}

// contactPutRequest is a contact PUT's target and body, read and checked once
// however often the write is retried.
type contactPutRequest struct {
	addressBookID int64
	resourceName  string
	cleanPath     string
	contentType   string
	body          []byte
	bodyText      string
	etag          string
	uid           string
}

func (h *DavServer) putContact(w http.ResponseWriter, r *http.Request, user *store.User, addressBookID int64, resourceName, cleanPath string) {
	aclGuard, existingByName, ok := h.authorizeContactObjectTarget(w, r, user, addressBookID, resourceName, cleanPath)
	if !ok {
		return
	}
	if !h.requireLock(w, r, cleanPath, "resource is locked") {
		return
	}
	body, ok := readPutBody(w, r, davPathAddressBook)
	if !ok {
		return
	}
	req := contactPutRequest{
		addressBookID: addressBookID,
		resourceName:  resourceName,
		cleanPath:     cleanPath,
		contentType:   strings.TrimSpace(r.Header.Get("Content-Type")),
		body:          body,
		bodyText:      string(body),
		etag:          fmt.Sprintf("%x", sha256.Sum256(body)),
	}
	if req.contentType != "" && !mediaTypeAdvertised(req.contentType, "text/vcard", addressDataVersions) {
		writeCardDAVPrecondition(w, http.StatusUnsupportedMediaType, "supported-address-data")
		return
	}

	if err := h.validateVCard(req.bodyText); err != nil {
		writeCardDAVPrecondition(w, http.StatusBadRequest, "valid-address-data")
		return
	}

	uid, err := extractUIDFromVCard(req.bodyText)
	if err != nil {
		writeCardDAVPrecondition(w, http.StatusBadRequest, "valid-address-data")
		return
	}
	req.uid = uid

	if !storableMemberName(resourceName) {
		writeCardDAVPrecondition(w, http.StatusBadRequest, "valid-address-data")
		return
	}
	h.putContactWithRetry(w, r, user, req, aclGuard, existingByName, maxResourceStateRetries)
}

// authorizeContactObjectTarget decides whether user may write the contact at
// cleanPath, answering the request itself when it may not. It returns the ACL
// state the decision read, for the write to re-check, and the contact the
// write would replace.
func (h *DavServer) authorizeContactObjectTarget(w http.ResponseWriter, r *http.Request, user *store.User, addressBookID int64, resourceName, cleanPath string) (*store.ACLGuard, *store.Contact, bool) {
	aclGuard, err := h.aclGuard(r.Context(), user, cleanPath)
	if err != nil {
		http.Error(w, "failed to evaluate ACL", http.StatusInternalServerError)
		return nil, nil, false
	}
	book, err := h.getAddressBook(r.Context(), addressBookID)
	if err != nil {
		status := http.StatusInternalServerError
		if errors.Is(err, store.ErrNotFound) {
			status = http.StatusNotFound
		}
		http.Error(w, "address book not found", status)
		return nil, nil, false
	}
	existing, err := h.store.Contacts.GetByResourceName(r.Context(), addressBookID, resourceName)
	if err != nil {
		http.Error(w, "failed to load contact", http.StatusInternalServerError)
		return nil, nil, false
	}
	requiredPrivilege := "bind"
	privilegePath := path.Dir(cleanPath)
	if existing != nil {
		requiredPrivilege = "write-content"
		privilegePath = cleanPath
	}
	if err := h.requireAddressBookPrivilege(r.Context(), user, book, privilegePath, requiredPrivilege); err != nil {
		refusal := requirePrivilegeAt(err, privilegePath, requiredPrivilege)
		if existing != nil {
			refusal = h.existingObjectWriteRefusal(r.Context(), user, cleanPath, refusal, func() error {
				parentPath := path.Dir(cleanPath)
				if err := h.requireAddressBookPrivilege(r.Context(), user, book, parentPath, "bind"); err != nil {
					return requirePrivilegeAt(err, parentPath, "bind")
				}
				return nil
			})
		}
		_ = writePrivilegeRequirementError(w, refusal)
		return nil, nil, false
	}
	return aclGuard, existing, true
}

func (h *DavServer) putContactWithRetry(w http.ResponseWriter, r *http.Request, user *store.User, req contactPutRequest, aclGuard *store.ACLGuard, existingByName *store.Contact, retries int) {
	if existingByName == nil && !h.requireLock(w, r, path.Dir(req.cleanPath), "resource is locked") {
		return
	}
	// RFC 6352 §5.1 and §6.3.2.1: a UID names one resource per address book.
	if existingByName != nil && existingByName.UID != req.uid {
		conflictHref := addressObjectConflictHref(req.addressBookID, contactResourceName(*existingByName))
		writeCardDAVUIDConflict(w, conflictHref)
		return
	}

	existingByUID, err := h.store.Contacts.GetByUID(r.Context(), req.addressBookID, req.uid)
	if err != nil {
		http.Error(w, "failed to load contact", http.StatusInternalServerError)
		return
	}
	if existingByUID != nil && contactResourceName(*existingByUID) != req.resourceName {
		conflictHref := addressObjectConflictHref(req.addressBookID, contactResourceName(*existingByUID))
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
		Path:         req.cleanPath,
		ResourceType: ResourceTypeAddressObject,
		CollectionID: req.addressBookID,
		ResourceName: req.resourceName,
		ContentType:  req.contentType,
		Body:         req.body,
		ETag:         req.etag,
	}); writeResponseError(w, err) {
		return
	}

	canonicalPath, err := h.canonicalDAVPath(r.Context(), user, req.cleanPath)
	if err != nil {
		http.Error(w, "failed to resolve resource state", http.StatusInternalServerError)
		return
	}
	expectedState := store.ContactDAVResourceState(existingByName)
	expectedState.ACL = aclGuard
	lockPreconditions, ok := h.writeLockPreconditions(w, r, putLockPaths(req.cleanPath, existingByName == nil)...)
	if !ok {
		return
	}
	result, err := h.store.PutContactObject(r.Context(), store.ContactObjectWrite{
		AddressBookID:     req.addressBookID,
		UID:               req.uid,
		ResourceName:      req.resourceName,
		RawVCard:          req.bodyText,
		ETag:              req.etag,
		Precondition:      calendarObjectPrecondition(r),
		ExpectedState:     expectedState,
		StatePath:         canonicalPath,
		LockPreconditions: lockPreconditions,
	})
	switch {
	case errors.Is(err, store.ErrUIDConflict):
		if result == nil || result.Conflict == nil {
			http.Error(w, "failed to save contact", http.StatusInternalServerError)
			return
		}
		writeCardDAVUIDConflict(w, addressObjectConflictHref(req.addressBookID, contactResourceName(*result.Conflict)))
		return
	case errors.Is(err, store.ErrConflict):
		writeCardDAVUIDConflict(w, req.cleanPath)
		return
	case errors.Is(err, store.ErrResourceStateChanged):
		if retries > 0 {
			invalidateDAVRequestState(r.Context())
			currentACL, current, authorized := h.authorizeContactObjectTarget(w, r, user, req.addressBookID, req.resourceName, req.cleanPath)
			if !authorized {
				return
			}
			h.putContactWithRetry(w, r, user, req, currentACL, current, retries-1)
			return
		}
		http.Error(w, "resource changed while writing; retry the request", http.StatusConflict)
		return
	case errors.Is(err, store.ErrPreconditionFailed):
		http.Error(w, "precondition failed", http.StatusPreconditionFailed)
		return
	case errors.Is(err, store.ErrLockConflict):
		http.Error(w, "resource is locked", http.StatusLocked)
		return
	case err != nil:
		h.logger().Error("Put", "failed to save contact %q in address book %d: %v", req.uid, req.addressBookID, err)
		http.Error(w, "failed to save contact", http.StatusInternalServerError)
		return
	}
	w.Header().Set("ETag", fmt.Sprintf("\"%s\"", req.etag))
	if result != nil && result.Created {
		h.logger().Info("Put", "created contact %q in address book %d", req.uid, req.addressBookID)
		w.WriteHeader(http.StatusCreated)
	} else {
		h.logger().Info("Put", "updated contact %q in address book %d", req.uid, req.addressBookID)
		w.WriteHeader(http.StatusNoContent)
	}
}

// storableMemberName reports whether a write may store an object as name:
// one store.ValidMemberName accepts, which as a unique index key PostgreSQL
// bounds in size.
func storableMemberName(name string) bool {
	return store.ValidMemberName(name) && len(name) <= store.MaxIdentifierOctets
}

// refusedMemberName refuses a request that would write the state of a member
// target names by a name store.ValidMemberName refuses, as a PUT of that name
// is refused, and reports whether it did. Such a member's state path names its
// collection, or that collection's parent, once cleaned, so a lock, ACL entry
// or dead property kept there would be read as theirs or not at all. A member
// stored under such a name before can still be read, copied or moved to a
// name the server accepts, or deleted.
func refusedMemberName(w http.ResponseWriter, target davTarget) bool {
	if !target.Resource || store.ValidMemberName(target.ResourceName) {
		return false
	}
	if target.Domain == davPathAddressBook {
		writeCardDAVPrecondition(w, http.StatusBadRequest, "valid-address-data")
	} else {
		writeCalDAVError(w, http.StatusForbidden, "valid-calendar-object-resource")
	}
	return true
}
