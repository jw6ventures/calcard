package dav

import (
	"context"
	"crypto/sha256"
	"encoding/xml"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"path"
	"strconv"
	"strings"
	"time"

	"github.com/jw6ventures/calcard/internal/auth"
	"github.com/jw6ventures/calcard/internal/config"
	"github.com/jw6ventures/calcard/internal/store"
	"github.com/lib/pq"
)

// Handler serves WebDAV/CalDAV/CardDAV requests.
type Handler struct {
	cfg   *config.Config
	store *store.Store
}

var errInvalidSyncToken = errors.New("invalid sync token")
var errInvalidPath = errors.New("invalid path")
var errAmbiguousCalendar = errors.New("ambiguous calendar path")
var errAmbiguousAddressBook = errors.New("ambiguous address book path")
var errForbidden = errors.New("forbidden")

const maxDAVBodyBytes int64 = 10 * 1024 * 1024

// birthdayCalendarID is a special virtual calendar ID for birthdays from contacts.
const birthdayCalendarID int64 = -1

func NewHandler(cfg *config.Config, store *store.Store) *Handler {
	return &Handler{cfg: cfg, store: store}
}

func (h *Handler) Proppatch(w http.ResponseWriter, r *http.Request) {
	user, ok := auth.UserFromContext(r.Context())
	if !ok {
		http.Error(w, "missing user", http.StatusUnauthorized)
		return
	}

	cleanPath := path.Clean(r.URL.Path)
	if !h.requireLock(w, r, cleanPath, "resource is locked") {
		return
	}

	// Parse PROPPATCH request body
	body, err := readDAVBody(w, r, maxDAVBodyBytes)
	if err != nil {
		if errors.Is(err, errRequestTooLarge) {
			http.Error(w, "request too large", http.StatusRequestEntityTooLarge)
		} else {
			http.Error(w, "failed to read body", http.StatusBadRequest)
		}
		return
	}

	var proppatchReq proppatchRequest
	if err := safeUnmarshalXML(body, &proppatchReq); err != nil {
		http.Error(w, "invalid PROPPATCH body", http.StatusBadRequest)
		return
	}

	// Process the property updates
	var responses []response

	if strings.HasPrefix(cleanPath, "/dav/calendars/") {
		resp, err := h.proppatchCalendar(r.Context(), user, cleanPath, &proppatchReq)
		if err != nil {
			if errors.Is(err, errInvalidPath) {
				http.Error(w, err.Error(), http.StatusBadRequest)
			} else {
				http.Error(w, err.Error(), http.StatusInternalServerError)
			}
			return
		}
		responses = append(responses, resp...)
	} else if strings.HasPrefix(cleanPath, "/dav/addressbooks/") {
		resp, err := h.proppatchAddressBook(r.Context(), user, cleanPath, &proppatchReq)
		if err != nil {
			if errors.Is(err, errInvalidPath) {
				http.Error(w, err.Error(), http.StatusBadRequest)
			} else {
				http.Error(w, err.Error(), http.StatusInternalServerError)
			}
			return
		}
		responses = append(responses, resp...)
	} else {
		http.Error(w, "unsupported path for PROPPATCH", http.StatusBadRequest)
		return
	}

	payload := multistatus{
		XMLName:  xml.Name{Space: "DAV:", Local: "multistatus"},
		XmlnsD:   "DAV:",
		XmlnsC:   "urn:ietf:params:xml:ns:caldav",
		XmlnsA:   "urn:ietf:params:xml:ns:carddav",
		Response: responses,
	}
	writeMultiStatus(w, payload)
}

func (h *Handler) proppatchCalendar(ctx context.Context, user *store.User, cleanPath string, req *proppatchRequest) ([]response, error) {
	parts := strings.Split(strings.TrimPrefix(cleanPath, "/dav/calendars"), "/")
	if len(parts) < 2 || strings.TrimSpace(parts[1]) == "" {
		return nil, fmt.Errorf("%w: invalid calendar path", errInvalidPath)
	}

	calID, err := strconv.ParseInt(parts[1], 10, 64)
	if err != nil {
		return nil, fmt.Errorf("%w: invalid calendar id", errInvalidPath)
	}

	// Block property changes on birthday calendar
	if calID == birthdayCalendarID {
		return []response{{
			Href: cleanPath,
			Propstat: []propstat{{
				Prop:   prop{},
				Status: httpStatusForbidden,
			}},
		}}, nil
	}

	calAccess, err := h.loadCalendar(ctx, user, calID)
	if err != nil {
		return nil, err
	}
	if err := h.requireCalendarPrivilege(ctx, user, &calAccess.Calendar, cleanPath, "write-properties"); err != nil {
		return []response{{
			Href: cleanPath,
			Propstat: []propstat{{
				Prop:   prop{},
				Status: httpStatusForbidden,
			}},
		}}, nil
	}

	var name *string
	var description *string
	var timezone *string

	if req.Set != nil {
		name = req.Set.Prop.DisplayName
		description = req.Set.Prop.CalendarDescription
		timezone = req.Set.Prop.CalendarTimezone
	}

	if name != nil || description != nil || timezone != nil {
		// Use existing name if not being updated
		updateName := calAccess.Name
		if name != nil {
			updateName = *name
		}

		err := h.store.Calendars.UpdateProperties(ctx, calID, updateName, description, timezone)
		if err != nil {
			log.Printf("failed to update calendar properties for calendar %d: %v", calID, err)
			return []response{{
				Href: cleanPath,
				Propstat: []propstat{{
					Prop:   prop{},
					Status: httpStatusInternalServerError,
				}},
			}}, nil
		}
	}

	// Return success response
	successProp := prop{}
	if name != nil {
		successProp.DisplayName = *name
	}
	if description != nil {
		successProp.CalendarDescription = *description
	}
	if timezone != nil {
		successProp.CalendarTimezone = timezone
	}

	return []response{{
		Href: cleanPath,
		Propstat: []propstat{{
			Prop:   successProp,
			Status: httpStatusOK,
		}},
	}}, nil
}

func (h *Handler) proppatchAddressBook(ctx context.Context, user *store.User, cleanPath string, req *proppatchRequest) ([]response, error) {
	parts := strings.Split(strings.TrimPrefix(cleanPath, "/dav/addressbooks"), "/")
	if len(parts) < 2 || strings.TrimSpace(parts[1]) == "" {
		return nil, fmt.Errorf("%w: invalid address book path", errInvalidPath)
	}

	bookID, ok, err := h.resolveAddressBookID(ctx, user, strings.TrimSpace(parts[1]))
	if err != nil {
		if errors.Is(err, errAmbiguousAddressBook) {
			return nil, errAmbiguousAddressBook
		}
		return nil, fmt.Errorf("%w: invalid address book id", errInvalidPath)
	}
	if !ok {
		return nil, fmt.Errorf("%w: invalid address book id", errInvalidPath)
	}

	book, err := h.getAddressBook(ctx, bookID)
	if err != nil {
		return nil, err
	}
	if err := h.requireAddressBookPrivilege(ctx, user, book, cleanPath, "write-properties"); err != nil {
		if errors.Is(err, errForbidden) || err == store.ErrNotFound {
			return []response{{
				Href: cleanPath,
				Propstat: []propstat{{
					Prop:   prop{},
					Status: httpStatusForbidden,
				}},
			}}, nil
		}
		return nil, err
	}

	// Extract properties to update
	var name *string
	var description *string
	var protectedProp prop
	var hasProtected bool

	if req.Set != nil {
		name = req.Set.Prop.DisplayName
		description = req.Set.Prop.AddressBookDesc
		if req.Set.Prop.SupportedAddressData != nil {
			protectedProp.SupportedAddressData = supportedAddressDataProp()
			hasProtected = true
		}
		if req.Set.Prop.AddressBookMaxResourceSize != nil {
			protectedProp.AddressBookMaxResourceSize = fmt.Sprintf("%d", maxDAVBodyBytes)
			hasProtected = true
		}
		if req.Set.Prop.SupportedCollationSet != nil {
			protectedProp.SupportedCollationSet = supportedCollationSetProp()
			hasProtected = true
		}
	}

	successProp := prop{}
	if name != nil {
		successProp.DisplayName = *name
	}
	if description != nil {
		successProp.AddressBookDesc = *description
	}

	if hasProtected {
		failedProp := protectedProp
		if name != nil {
			failedProp.DisplayName = *name
		}
		if description != nil {
			failedProp.AddressBookDesc = *description
		}
		return []response{{
			Href: cleanPath,
			Propstat: []propstat{{
				Prop:   failedProp,
				Status: httpStatusForbidden,
			}},
		}}, nil
	}

	// Update the address book
	if name != nil || description != nil {
		updateName := book.Name
		if name != nil {
			updateName = *name
		}

		err := h.store.AddressBooks.UpdateProperties(ctx, bookID, updateName, description)
		if err != nil {
			status := httpStatusInternalServerError
			if errors.Is(err, store.ErrConflict) {
				status = httpStatusConflict
			}
			log.Printf("failed to update address book properties for book %d: %v", bookID, err)
			return []response{{
				Href: cleanPath,
				Propstat: []propstat{{
					Prop:   successProp,
					Status: status,
				}},
			}}, nil
		}
	}

	return []response{{
		Href: cleanPath,
		Propstat: []propstat{{
			Prop:   successProp,
			Status: httpStatusOK,
		}},
	}}, nil
}

func (h *Handler) Mkcol(w http.ResponseWriter, r *http.Request) {
	user, ok := auth.UserFromContext(r.Context())
	if !ok {
		http.Error(w, "missing user", http.StatusUnauthorized)
		return
	}

	cleanPath := path.Clean(r.URL.Path)
	if !h.requireLocks(w, r, "resource is locked", cleanPath, path.Dir(cleanPath)) {
		return
	}
	pendingLockPath, err := h.canonicalDAVPath(r.Context(), user, cleanPath)
	if err != nil {
		http.Error(w, "failed to resolve collection path", http.StatusInternalServerError)
		return
	}
	if !strings.HasPrefix(cleanPath, "/dav/addressbooks/") {
		http.Error(w, "unsupported path", http.StatusBadRequest)
		return
	}
	parts := strings.Split(strings.TrimPrefix(cleanPath, "/dav/addressbooks"), "/")
	if len(parts) > 2 || (len(parts) == 2 && parts[0] != "" && parts[1] != "") {
		http.Error(w, "nested address book collections not allowed", http.StatusForbidden)
		return
	}
	name := strings.TrimSpace(parts[len(parts)-1])
	if name == "" {
		http.Error(w, "collection name required", http.StatusBadRequest)
		return
	}
	if _, err := strconv.ParseInt(name, 10, 64); err == nil {
		http.Error(w, "collection name must be non-numeric", http.StatusBadRequest)
		return
	}
	description := (*string)(nil)
	if r.Body != nil && r.Body != http.NoBody {
		body, err := readDAVBody(w, r, maxDAVBodyBytes)
		if err != nil {
			if errors.Is(err, errRequestTooLarge) {
				http.Error(w, "request too large", http.StatusRequestEntityTooLarge)
			} else {
				http.Error(w, "failed to read body", http.StatusBadRequest)
			}
			return
		}
		var mkReq mkcalendarRequest
		if len(body) > 0 {
			if err := safeUnmarshalXML(body, &mkReq); err != nil {
				http.Error(w, "invalid MKCOL body", http.StatusBadRequest)
				return
			}
			if mkReq.Set != nil {
				if mkReq.Set.Prop.DisplayName != nil && strings.TrimSpace(*mkReq.Set.Prop.DisplayName) != "" {
					name = strings.TrimSpace(*mkReq.Set.Prop.DisplayName)
				}
				description = mkReq.Set.Prop.AddressBookDesc
			}
		}
	}
	if _, err := strconv.ParseInt(name, 10, 64); err == nil {
		http.Error(w, "collection name must be non-numeric", http.StatusBadRequest)
		return
	}
	created, err := h.store.AddressBooks.Create(r.Context(), store.AddressBook{UserID: user.ID, Name: name, Description: description})
	if err != nil {
		if errors.Is(err, store.ErrConflict) {
			http.Error(w, "address book already exists", http.StatusConflict)
			return
		}
		http.Error(w, "failed to create", http.StatusInternalServerError)
		return
	}
	if created != nil {
		location := path.Join("/dav/addressbooks", fmt.Sprint(created.ID)) + "/"
		if err := h.rebindCollectionLocks(r.Context(), pendingLockPath, strings.TrimSuffix(location, "/")); err != nil {
			if deleteErr := h.store.AddressBooks.Delete(r.Context(), user.ID, created.ID); deleteErr != nil && !errors.Is(deleteErr, store.ErrNotFound) {
				log.Printf("failed to roll back address book %d after lock rebind failure: %v", created.ID, deleteErr)
			}
			http.Error(w, "failed to rebind collection locks", http.StatusInternalServerError)
			return
		}
		w.Header().Set("Location", location)
	}
	w.WriteHeader(http.StatusCreated)
}

func (h *Handler) Mkcalendar(w http.ResponseWriter, r *http.Request) {
	user, ok := auth.UserFromContext(r.Context())
	if !ok {
		http.Error(w, "missing user", http.StatusUnauthorized)
		return
	}

	cleanPath := path.Clean(r.URL.Path)
	if !h.requireLocks(w, r, "resource is locked", cleanPath, path.Dir(cleanPath)) {
		return
	}
	pendingLockPath, err := h.canonicalDAVPath(r.Context(), user, cleanPath)
	if err != nil {
		http.Error(w, "failed to resolve collection path", http.StatusInternalServerError)
		return
	}
	if !strings.HasPrefix(cleanPath, "/dav/calendars/") {
		http.Error(w, "unsupported path", http.StatusBadRequest)
		return
	}
	parts := strings.Split(strings.TrimPrefix(cleanPath, "/dav/calendars"), "/")

	if len(parts) > 2 || (len(parts) == 2 && parts[0] != "" && parts[1] != "") {
		http.Error(w, "nested calendar collections not allowed", http.StatusForbidden)
		return
	}

	pathName := strings.TrimSpace(parts[len(parts)-1])
	if pathName == "" {
		http.Error(w, "calendar name required", http.StatusBadRequest)
		return
	}
	if _, err := strconv.ParseInt(pathName, 10, 64); err == nil {
		http.Error(w, "calendar name must be non-numeric", http.StatusBadRequest)
		return
	}

	var mkReq mkcalendarRequest
	if r.Body != http.NoBody {
		body, err := readDAVBody(w, r, maxDAVBodyBytes)
		if err != nil {
			if errors.Is(err, errRequestTooLarge) {
				http.Error(w, "request too large", http.StatusRequestEntityTooLarge)
			} else {
				http.Error(w, "failed to read body", http.StatusBadRequest)
			}
			return
		}
		if err := safeUnmarshalXML(body, &mkReq); err != nil {
			http.Error(w, "invalid MKCALENDAR body", http.StatusBadRequest)
			return
		}
	}

	name := pathName
	var description *string
	var timezone *string
	if mkReq.Set != nil {
		if mkReq.Set.Prop.DisplayName != nil {
			trimmed := strings.TrimSpace(*mkReq.Set.Prop.DisplayName)
			if trimmed != "" {
				name = trimmed
			}
		}
		description = mkReq.Set.Prop.CalendarDescription
		timezone = mkReq.Set.Prop.CalendarTimezone
	}

	cals, err := h.store.Calendars.ListAccessible(r.Context(), user.ID)
	if err != nil {
		http.Error(w, "failed to check calendars", http.StatusInternalServerError)
		return
	}
	// Normalize slug for consistent case-insensitive comparison
	normalizedPathName := strings.ToLower(pathName)
	for _, cal := range cals {
		if cal.Slug != nil && *cal.Slug == normalizedPathName {
			http.Error(w, "calendar already exists", http.StatusConflict)
			return
		}
		if strings.EqualFold(cal.Name, pathName) {
			http.Error(w, "calendar already exists", http.StatusConflict)
			return
		}
	}
	// Use pre-normalized slug to match database constraint (LOWER(slug))
	slug := normalizedPathName
	// Validate slug for path safety (prevent path traversal, injection)
	if !isValidCalendarSlug(slug) {
		http.Error(w, "invalid calendar name: must contain only lowercase letters, numbers, and hyphens", http.StatusBadRequest)
		return
	}
	created, err := h.store.Calendars.Create(r.Context(), store.Calendar{
		UserID:      user.ID,
		Name:        name,
		Slug:        &slug,
		Description: description,
		Timezone:    timezone,
	})
	if err != nil {
		var pqErr *pq.Error
		if errors.As(err, &pqErr) && pqErr.Code == "23505" {
			http.Error(w, "calendar already exists", http.StatusConflict)
			return
		}
		http.Error(w, "failed to create", http.StatusInternalServerError)
		return
	}
	location := path.Join("/dav/calendars", fmt.Sprint(created.ID)) + "/"
	if err := h.rebindCollectionLocks(r.Context(), pendingLockPath, strings.TrimSuffix(location, "/")); err != nil {
		if deleteErr := h.store.Calendars.Delete(r.Context(), user.ID, created.ID); deleteErr != nil && !errors.Is(deleteErr, store.ErrNotFound) {
			log.Printf("failed to roll back calendar %d after lock rebind failure: %v", created.ID, deleteErr)
		}
		http.Error(w, "failed to rebind collection locks", http.StatusInternalServerError)
		return
	}
	w.Header().Set("Location", location)
	w.WriteHeader(http.StatusCreated)
}

func (h *Handler) rebindCollectionLocks(ctx context.Context, fromPath, toPath string) error {
	if fromPath == "" || toPath == "" || fromPath == toPath {
		return nil
	}
	if h == nil || h.store == nil || h.store.Locks == nil {
		return nil
	}
	return h.store.Locks.MoveResourcePath(ctx, fromPath, toPath)
}

func (h *Handler) moveDAVResourceState(ctx context.Context, user *store.User, fromPath, toPath string) error {
	fromCanonical, err := h.canonicalDAVPath(ctx, user, fromPath)
	if err != nil {
		return err
	}
	toCanonical, err := h.canonicalDAVPath(ctx, user, toPath)
	if err != nil {
		return err
	}
	if fromCanonical == "" || toCanonical == "" || fromCanonical == toCanonical {
		return nil
	}
	if h == nil || h.store == nil {
		return nil
	}
	movedACL := false
	if h.store.ACLEntries != nil {
		if err := h.store.ACLEntries.MoveResourcePath(ctx, fromCanonical, toCanonical); err != nil {
			return err
		}
		movedACL = true
	}
	if h.store.Locks != nil {
		if err := h.store.Locks.MoveResourcePath(ctx, fromCanonical, toCanonical); err != nil {
			if movedACL && h.store.ACLEntries != nil {
				if rollbackErr := h.store.ACLEntries.MoveResourcePath(ctx, toCanonical, fromCanonical); rollbackErr != nil {
					return fmt.Errorf("move locks: %w (acl rollback failed: %v)", err, rollbackErr)
				}
			}
			return err
		}
	}
	return nil
}

func (h *Handler) rebindMovedDAVResourceState(ctx context.Context, user *store.User, fromPath, toPath string, overwrite bool) error {
	return h.moveDAVResourceState(ctx, user, fromPath, toPath)
}

func (h *Handler) deleteDAVACLState(ctx context.Context, user *store.User, resourcePath string) error {
	canonicalPath, err := h.canonicalDAVPath(ctx, user, resourcePath)
	if err != nil {
		return err
	}
	if canonicalPath == "" || h == nil || h.store == nil || h.store.ACLEntries == nil {
		return nil
	}
	for _, statePath := range davStatePaths(canonicalPath) {
		if err := h.store.ACLEntries.Delete(ctx, statePath); err != nil {
			return err
		}
	}
	return nil
}

func (h *Handler) deleteDAVResourceState(ctx context.Context, user *store.User, resourcePath string) error {
	canonicalPath, err := h.canonicalDAVPath(ctx, user, resourcePath)
	if err != nil {
		return err
	}
	if canonicalPath == "" {
		return nil
	}
	if h == nil || h.store == nil {
		return nil
	}
	for _, statePath := range davStatePaths(canonicalPath) {
		if h.store.Locks != nil {
			if err := h.store.Locks.DeleteByResourcePath(ctx, statePath); err != nil {
				return err
			}
		}
		if h.store.ACLEntries != nil {
			if err := h.store.ACLEntries.Delete(ctx, statePath); err != nil {
				return err
			}
		}
	}
	return nil
}

// validateICalendar performs basic validation of iCalendar data (RFC 5545)
func (h *Handler) validateICalendar(data string) error {
	trimmed := strings.TrimSpace(data)

	// Must start with BEGIN:VCALENDAR
	if !strings.HasPrefix(strings.ToUpper(trimmed), "BEGIN:VCALENDAR") {
		return fmt.Errorf("missing BEGIN:VCALENDAR")
	}

	// Must end with END:VCALENDAR
	if !strings.HasSuffix(strings.ToUpper(trimmed), "END:VCALENDAR") {
		return fmt.Errorf("missing END:VCALENDAR")
	}

	// Must contain at least one component (VEVENT, VTODO, VJOURNAL, or VFREEBUSY)
	componentTypes := extractICalComponentTypes(data)
	_, hasEvent := componentTypes["VEVENT"]
	_, hasTodo := componentTypes["VTODO"]
	_, hasJournal := componentTypes["VJOURNAL"]
	_, hasFreeBusy := componentTypes["VFREEBUSY"]
	hasComponent := hasEvent || hasTodo || hasJournal || hasFreeBusy

	if !hasComponent {
		return fmt.Errorf("no calendar component found (VEVENT, VTODO, VJOURNAL, or VFREEBUSY required)")
	}

	// Check balanced BEGIN/END tags for all component types
	// This validates that every BEGIN has a matching END
	upper := strings.ToUpper(trimmed)
	if err := validateBalancedTags(upper); err != nil {
		return err
	}

	return nil
}

// validateBalancedTags checks that all BEGIN tags have matching END tags
func validateBalancedTags(data string) error {
	lines := strings.Split(data, "\n")
	var stack []string

	for _, line := range lines {
		line = strings.TrimSpace(line)
		if strings.HasPrefix(line, "BEGIN:") {
			componentType := strings.TrimPrefix(line, "BEGIN:")
			stack = append(stack, componentType)
		} else if strings.HasPrefix(line, "END:") {
			componentType := strings.TrimPrefix(line, "END:")
			if len(stack) == 0 {
				return fmt.Errorf("END:%s without matching BEGIN", componentType)
			}
			if stack[len(stack)-1] != componentType {
				return fmt.Errorf("mismatched tags: BEGIN:%s ... END:%s", stack[len(stack)-1], componentType)
			}
			stack = stack[:len(stack)-1]
		}
	}

	if len(stack) > 0 {
		return fmt.Errorf("unbalanced tags: BEGIN:%s without matching END", stack[len(stack)-1])
	}

	return nil
}

func extractICalComponentTypes(icalData string) map[string]struct{} {
	types := make(map[string]struct{})
	lines := unfoldICalLines(icalData)
	for _, line := range lines {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}
		upper := strings.ToUpper(line)
		if strings.HasPrefix(upper, "BEGIN:") {
			componentType := strings.TrimSpace(strings.TrimPrefix(upper, "BEGIN:"))
			if componentType != "" {
				types[componentType] = struct{}{}
			}
		}
	}
	return types
}

func extractICalRRULECount(icalData string) (int, bool) {
	lines := unfoldICalLines(icalData)
	for _, line := range lines {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}
		upper := strings.ToUpper(line)
		if strings.HasPrefix(upper, "RRULE") {
			colonIdx := strings.Index(line, ":")
			if colonIdx == -1 {
				continue
			}
			rule := line[colonIdx+1:]
			parts := strings.Split(rule, ";")
			for _, part := range parts {
				part = strings.TrimSpace(part)
				if part == "" {
					continue
				}
				if idx := strings.Index(part, "="); idx != -1 {
					if strings.EqualFold(part[:idx], "COUNT") {
						value := strings.TrimSpace(part[idx+1:])
						if value == "" {
							continue
						}
						if count, err := strconv.Atoi(value); err == nil {
							return count, true
						}
					}
				}
			}
		}
	}
	return 0, false
}

func countICalAttendees(icalData string) int {
	lines := unfoldICalLines(icalData)
	targets := map[string]struct{}{
		"VEVENT":   {},
		"VTODO":    {},
		"VJOURNAL": {},
	}
	inTarget := false
	currentCount := 0
	maxCount := 0

	for _, line := range lines {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}
		upper := strings.ToUpper(line)
		if strings.HasPrefix(upper, "BEGIN:") {
			name := strings.TrimSpace(strings.TrimPrefix(upper, "BEGIN:"))
			if !inTarget {
				if _, ok := targets[name]; ok {
					inTarget = true
					currentCount = 0
				}
			}
			continue
		}
		if strings.HasPrefix(upper, "END:") {
			name := strings.TrimSpace(strings.TrimPrefix(upper, "END:"))
			if inTarget {
				if _, ok := targets[name]; ok {
					if currentCount > maxCount {
						maxCount = currentCount
					}
					inTarget = false
				}
			}
			continue
		}
		if !inTarget {
			continue
		}
		if strings.HasPrefix(upper, "ATTENDEE") {
			if len(upper) == len("ATTENDEE") || (len(upper) > len("ATTENDEE") && (upper[len("ATTENDEE")] == ';' || upper[len("ATTENDEE")] == ':')) {
				currentCount++
			}
		}
	}
	if currentCount > maxCount {
		maxCount = currentCount
	}
	return maxCount
}

// extractUIDFromICalendar extracts the UID property from iCalendar data.
// For multi-component calendars, returns the UID from the first top-level component.
// The validateCalendarObjectResource function handles validation of multi-component UIDs.
func extractUIDFromICalendar(icalData string) (string, error) {
	components := parseCalendarTopLevelComponents(icalData)
	if len(components) == 0 {
		return "", fmt.Errorf("no calendar components found")
	}
	// Return UID from first top-level component
	firstComponent := components[0]
	if firstComponent.UIDEmpty || firstComponent.UIDCount == 0 {
		return "", fmt.Errorf("no UID property found in calendar data")
	}
	if firstComponent.UID == "" {
		return "", fmt.Errorf("empty UID property")
	}
	return firstComponent.UID, nil
}

// extractUIDFromVCard extracts the UID property from vCard data
func extractUIDFromVCard(vcardData string) (string, error) {
	// Unfold lines per RFC 6350 (same as RFC 5545)
	lines := unfoldICalLines(vcardData)
	for _, line := range lines {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}
		upperLine := strings.ToUpper(line)
		if strings.HasPrefix(upperLine, "UID") {
			// Check for proper delimiter (: or ;)
			if len(upperLine) == len("UID") || (len(upperLine) > len("UID") && (upperLine[len("UID")] == ':' || upperLine[len("UID")] == ';')) {
				colonIdx := strings.Index(line, ":")
				if colonIdx == -1 {
					continue
				}
				uid := strings.TrimSpace(line[colonIdx+1:])
				if uid == "" {
					return "", fmt.Errorf("empty UID property")
				}
				return uid, nil
			}
		}
	}
	return "", fmt.Errorf("no UID property found in vCard data")
}

type calendarTopLevelComponent struct {
	Type            string
	UID             string
	UIDCount        int
	UIDEmpty        bool
	HasRecurrenceID bool
}

func validateCalendarObjectResource(icalData string) []string {
	components := parseCalendarTopLevelComponents(icalData)
	for _, component := range components {
		if component.UIDEmpty || component.UIDCount == 0 {
			return []string{"valid-calendar-object-resource"}
		}
		if component.UIDCount > 1 {
			return []string{"valid-calendar-data"}
		}
	}
	if len(components) <= 1 {
		return nil
	}

	uid := components[0].UID
	sameUID := true
	for _, component := range components[1:] {
		if component.UID != uid {
			sameUID = false
			break
		}
	}
	if !sameUID {
		return []string{"valid-calendar-object-resource", "valid-calendar-data"}
	}

	withoutRecurrence := 0
	withRecurrence := 0
	for _, component := range components {
		if component.HasRecurrenceID {
			withRecurrence++
		} else {
			withoutRecurrence++
		}
	}
	if withRecurrence > 0 && withoutRecurrence == 1 {
		return nil
	}

	return []string{"valid-calendar-object-resource"}
}

func parseCalendarTopLevelComponents(icalData string) []calendarTopLevelComponent {
	lines := unfoldICalLines(icalData)
	var stack []string
	var current *calendarTopLevelComponent
	var components []calendarTopLevelComponent

	for _, line := range lines {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}
		upper := strings.ToUpper(line)
		if strings.HasPrefix(upper, "BEGIN:") {
			componentType := strings.TrimSpace(strings.TrimPrefix(upper, "BEGIN:"))
			stack = append(stack, componentType)
			if len(stack) == 2 && stack[0] == "VCALENDAR" && isTopLevelComponentType(componentType) {
				current = &calendarTopLevelComponent{Type: componentType}
			}
			continue
		}
		if strings.HasPrefix(upper, "END:") {
			componentType := strings.TrimSpace(strings.TrimPrefix(upper, "END:"))
			if current != nil && len(stack) == 2 && stack[0] == "VCALENDAR" && stack[1] == current.Type && componentType == current.Type {
				components = append(components, *current)
				current = nil
			}
			if len(stack) > 0 {
				stack = stack[:len(stack)-1]
			}
			continue
		}
		if current == nil {
			continue
		}
		if strings.HasPrefix(upper, "UID") {
			if len(upper) == len("UID") || (len(upper) > len("UID") && (upper[len("UID")] == ':' || upper[len("UID")] == ';')) {
				colonIdx := strings.Index(line, ":")
				if colonIdx == -1 {
					continue
				}
				uid := strings.TrimSpace(line[colonIdx+1:])
				current.UIDCount++
				if uid == "" {
					current.UIDEmpty = true
					continue
				}
				if current.UID == "" {
					current.UID = uid
				}
			}
			continue
		}
		if strings.HasPrefix(upper, "RECURRENCE-ID") {
			current.HasRecurrenceID = true
		}
	}

	if current != nil {
		components = append(components, *current)
	}

	return components
}

func isTopLevelComponentType(componentType string) bool {
	switch componentType {
	case "VEVENT", "VTODO", "VJOURNAL", "VFREEBUSY":
		return true
	default:
		return false
	}
}

func containsICalMethodProperty(icalData string) bool {
	lines := unfoldICalLines(icalData)
	for _, line := range lines {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}
		upper := strings.ToUpper(line)
		if strings.HasPrefix(upper, "METHOD") {
			if len(upper) == len("METHOD") || (len(upper) > len("METHOD") && (upper[len("METHOD")] == ':' || upper[len("METHOD")] == ';')) {
				return true
			}
		}
	}
	return false
}

func hasMultipleDifferentUIDs(icalData string) bool {
	components := parseCalendarTopLevelComponents(icalData)
	if len(components) <= 1 {
		return false
	}
	first := components[0].UID
	for _, component := range components[1:] {
		if component.UID != first {
			return true
		}
	}
	return false
}

func (h *Handler) validateVCard(data string) error {
	trimmed := strings.TrimSpace(data)

	if !strings.HasPrefix(strings.ToUpper(trimmed), "BEGIN:VCARD") {
		return fmt.Errorf("missing BEGIN:VCARD")
	}

	if !strings.HasSuffix(strings.ToUpper(trimmed), "END:VCARD") {
		return fmt.Errorf("missing END:VCARD")
	}

	upper := strings.ToUpper(trimmed)
	beginCount := strings.Count(upper, "BEGIN:VCARD")
	endCount := strings.Count(upper, "END:VCARD")
	if beginCount != endCount {
		return fmt.Errorf("unbalanced VCARD tags")
	}
	if beginCount != 1 {
		return fmt.Errorf("address object resources must contain exactly one VCARD")
	}

	lines := unfoldICalLines(trimmed)
	versionCount := 0
	hasFN := false
	uidCount := 0
	for _, line := range lines {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}
		colonIdx := strings.IndexByte(line, ':')
		if colonIdx == -1 {
			continue
		}
		head := strings.ToUpper(strings.TrimSpace(line[:colonIdx]))
		if semiIdx := strings.IndexByte(head, ';'); semiIdx >= 0 {
			head = head[:semiIdx]
		}
		name := vcardPropertyBaseName(head)
		value := strings.TrimSpace(line[colonIdx+1:])
		switch name {
		case "VERSION":
			versionCount++
			if value != "3.0" && value != "4.0" {
				return fmt.Errorf("unsupported VCARD version")
			}
		case "FN":
			if value != "" {
				hasFN = true
			}
		case "UID":
			uidCount++
			if value == "" {
				return fmt.Errorf("VCARD UID must not be empty")
			}
		}
	}
	if versionCount != 1 {
		return fmt.Errorf("VCARD must contain exactly one VERSION")
	}
	if uidCount != 1 {
		return fmt.Errorf("VCARD must contain exactly one UID")
	}
	if !hasFN {
		return fmt.Errorf("VCARD must contain FN")
	}

	return nil
}

// checkConditionalHeaders validates If-Match and If-None-Match headers for events
func (h *Handler) checkConditionalHeaders(r *http.Request, existing *store.Event) bool {
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
func (h *Handler) checkConditionalHeadersContact(r *http.Request, existing *store.Contact) bool {
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

func (h *Handler) Put(w http.ResponseWriter, r *http.Request) {
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

		if _, err := h.store.Events.Upsert(r.Context(), store.Event{CalendarID: calendarID, UID: uid, ResourceName: resourceName, RawICAL: string(body), ETag: etag}); err != nil {
			http.Error(w, "failed to save event", http.StatusInternalServerError)
			return
		}
		w.Header().Set("ETag", fmt.Sprintf("\"%s\"", etag))
		if existing == nil {
			w.WriteHeader(http.StatusCreated)
		} else {
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
			http.Error(w, "failed to save contact", http.StatusInternalServerError)
			return
		}
		w.Header().Set("ETag", fmt.Sprintf("\"%s\"", etag))
		if existing == nil {
			w.WriteHeader(http.StatusCreated)
		} else {
			w.WriteHeader(http.StatusNoContent)
		}
		return
	}

	http.Error(w, "unsupported path", http.StatusBadRequest)
}

func (h *Handler) Delete(w http.ResponseWriter, r *http.Request) {
	user, ok := auth.UserFromContext(r.Context())
	if !ok {
		http.Error(w, "missing user", http.StatusUnauthorized)
		return
	}

	cleanPath := path.Clean(r.URL.Path)
	if !h.requireLock(w, r, cleanPath, "resource is locked") {
		return
	}
	if calendarID, uid, matched, err := h.parseCalendarResourcePath(r.Context(), user, cleanPath); err != nil {
		if err == store.ErrNotFound {
			http.Error(w, "not found", http.StatusNotFound)
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

		_, err = h.loadCalendarWithPrivilege(r.Context(), user, calendarID, cleanPath, "unbind")
		if err != nil {
			status := http.StatusInternalServerError
			if err == store.ErrNotFound {
				status = http.StatusNotFound
			}
			if errors.Is(err, errForbidden) {
				status = http.StatusForbidden
			}
			http.Error(w, "not found", status)
			return
		}
		if !h.requireLock(w, r, path.Dir(cleanPath), "resource is locked") {
			return
		}
		existing, err := h.store.Events.GetByResourceName(r.Context(), calendarID, uid)
		if err != nil {
			http.Error(w, "failed to load event", http.StatusInternalServerError)
			return
		}
		if !h.checkConditionalHeaders(r, existing) {
			http.Error(w, "precondition failed", http.StatusPreconditionFailed)
			return
		}
		if existing == nil {
			http.Error(w, "not found", http.StatusNotFound)
			return
		}
		canonicalPath, err := h.canonicalDAVPath(r.Context(), user, cleanPath)
		if err != nil {
			http.Error(w, "failed to resolve resource state", http.StatusInternalServerError)
			return
		}
		if err := h.store.DeleteEventAndState(r.Context(), calendarID, existing.UID, canonicalPath); err != nil {
			http.Error(w, "failed to delete", http.StatusInternalServerError)
			return
		}
		w.WriteHeader(http.StatusNoContent)
		return
	}
	if addressBookID, resourceName, matched, err := h.parseAddressBookResourcePath(r.Context(), user, cleanPath); err != nil {
		if err == store.ErrNotFound {
			http.Error(w, "not found", http.StatusNotFound)
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
			http.Error(w, "not found", status)
			return
		}
		existing, err := h.store.Contacts.GetByResourceName(r.Context(), addressBookID, resourceName)
		if err != nil {
			http.Error(w, "failed to load contact", http.StatusInternalServerError)
			return
		}
		if err := h.requireAddressBookPrivilege(r.Context(), user, book, cleanPath, "unbind"); err != nil {
			status := http.StatusForbidden
			if err == store.ErrNotFound {
				status = http.StatusNotFound
			}
			http.Error(w, http.StatusText(status), status)
			return
		}
		if !h.requireLock(w, r, path.Dir(cleanPath), "resource is locked") {
			return
		}
		if !h.checkConditionalHeadersContact(r, existing) {
			http.Error(w, "precondition failed", http.StatusPreconditionFailed)
			return
		}
		if existing == nil {
			http.Error(w, "not found", http.StatusNotFound)
			return
		}
		canonicalPath, err := h.canonicalDAVPath(r.Context(), user, cleanPath)
		if err != nil {
			http.Error(w, "failed to resolve resource state", http.StatusInternalServerError)
			return
		}
		if err := h.store.DeleteContactAndState(r.Context(), addressBookID, existing.UID, canonicalPath); err != nil {
			http.Error(w, "failed to delete", http.StatusInternalServerError)
			return
		}
		w.WriteHeader(http.StatusNoContent)
		return
	}
	http.Error(w, "unsupported path", http.StatusBadRequest)
}

func (h *Handler) buildPropfindResponses(ctx context.Context, reqPath, depth string, user *store.User, propfindReq *propfindRequest) ([]response, error) {
	cleanPath := path.Clean(reqPath)
	if !strings.HasPrefix(cleanPath, "/dav") {
		return nil, http.ErrNotSupported
	}

	// Ensure trailing slash on collections for predictable href values.
	ensureCollectionHref := func(p string) string {
		if !strings.HasSuffix(p, "/") {
			return p + "/"
		}
		return p
	}

	switch {
	case cleanPath == "/dav" || cleanPath == "/dav/":
		href := ensureCollectionHref(cleanPath)
		principalHref := h.principalURL(user)
		res := []response{rootCollectionResponse(href, user, principalHref)}
		if depth == "1" {
			res = append(res,
				collectionResponse(ensureCollectionHref("/dav/calendars"), "Calendars"),
				collectionResponse(ensureCollectionHref("/dav/addressbooks"), "Address Books"),
				principalResponse(ensureCollectionHref(principalHref), user),
			)
		}
		if err := h.decoratePropfindResponses(ctx, user, res); err != nil {
			return nil, err
		}
		if propfindReq != nil && propfindReq.Prop != nil {
			for i := range res {
				res[i] = filterNonPrincipalPropfindResponse(res[i], propfindReq)
			}
		}
		return res, nil
	case strings.HasPrefix(cleanPath, "/dav/principals"):
		responses, err := h.principalResponses(cleanPath, depth, user, ensureCollectionHref)
		if err != nil {
			return nil, err
		}
		if err := h.decoratePropfindResponses(ctx, user, responses); err != nil {
			return nil, err
		}
		if propfindReq != nil && propfindReq.AllProp != nil {
			stripPrincipalAllprop(responses)
		}
		if propfindReq != nil && propfindReq.Prop != nil {
			for i := range responses {
				responses[i] = filterPrincipalPropfindResponse(responses[i], propfindReq)
			}
		}
		return responses, nil
	case strings.HasPrefix(cleanPath, "/dav/calendars"):
		responses, err := h.calendarResponses(ctx, cleanPath, depth, user, ensureCollectionHref)
		if err != nil {
			return nil, err
		}
		if err := h.decoratePropfindResponses(ctx, user, responses); err != nil {
			return nil, err
		}
		if propfindReq != nil && propfindReq.AllProp != nil {
			stripCalendarAllprop(responses)
		}
		if propfindReq != nil && propfindReq.Prop != nil {
			for i := range responses {
				responses[i] = filterNonPrincipalPropfindResponse(responses[i], propfindReq)
			}
		}
		return responses, nil
	case strings.HasPrefix(cleanPath, "/dav/addressbooks"):
		responses, err := h.addressBookResponses(ctx, cleanPath, depth, user, ensureCollectionHref, propfindReq)
		if err != nil {
			return nil, err
		}
		if err := h.decoratePropfindResponses(ctx, user, responses); err != nil {
			return nil, err
		}
		if propfindReq != nil && propfindReq.AllProp != nil {
			stripAddressBookAllprop(responses)
		}
		if propfindReq != nil && propfindReq.Prop != nil {
			for i := range responses {
				responses[i] = filterNonPrincipalPropfindResponse(responses[i], propfindReq)
			}
		}
		return responses, nil
	default:
		return nil, http.ErrNotSupported
	}
}

func stripCalendarAllprop(responses []response) {
	for i := range responses {
		for j := range responses[i].Propstat {
			prop := &responses[i].Propstat[j].Prop
			if prop.ResourceType.Calendar == nil {
				continue
			}
			prop.CalendarTimezone = nil
			prop.SupportedCalendarData = nil
		}
	}
}

func (h *Handler) calendarResponses(ctx context.Context, cleanPath, depth string, user *store.User, ensureCollectionHref func(string) string) ([]response, error) {
	relPath := strings.Trim(strings.TrimPrefix(cleanPath, "/dav/calendars"), "/")
	if relPath == "" {
		base := ensureCollectionHref("/dav/calendars")
		res := []response{collectionResponse(base, "Calendars")}
		if depth == "1" {
			cals, err := h.accessibleCalendars(ctx, user)
			if err != nil {
				return nil, err
			}
			principalHref := h.principalURL(user)

			// Add the virtual birthday calendar first
			birthdayHref := ensureCollectionHref(path.Join("/dav/calendars", fmt.Sprint(birthdayCalendarID)))
			birthdayName := "Birthdays"
			birthdayDesc := "Contact birthdays from your address books"
			// Use stable sync-token (epoch) for birthday calendar to ensure consistency
			birthdayToken := buildSyncToken("cal", birthdayCalendarID, time.Unix(0, 0))
			res = append(res, calendarCollectionResponse(birthdayHref, birthdayName, &birthdayDesc, nil, principalHref, birthdayToken, "0", true))

			// Add regular calendars
			for _, c := range cals {
				href := ensureCollectionHref(path.Join("/dav/calendars", fmt.Sprint(c.ID)))
				ctag := fmt.Sprintf("%d", c.CTag)
				syncToken := buildSyncToken("cal", c.ID, c.UpdatedAt)
				res = append(res, calendarCollectionResponseWithPrivileges(href, c.Name, c.Description, c.Timezone, principalHref, syncToken, ctag, c.EffectivePrivileges()))
			}
		}
		return res, nil
	}

	segments := strings.Split(relPath, "/")
	if len(segments) > 2 {
		return nil, http.ErrNotSupported
	}
	calID, err := strconv.ParseInt(segments[0], 10, 64)
	if calID == birthdayCalendarID {
		href := ensureCollectionHref(path.Join("/dav/calendars", fmt.Sprint(birthdayCalendarID)))
		birthdayName := "Birthdays"
		birthdayDesc := "Contact birthdays from your address books"
		// Use stable sync-token (epoch) for birthday calendar to ensure consistency
		syncToken := buildSyncToken("cal", birthdayCalendarID, time.Unix(0, 0))
		principalHref := h.principalURL(user)
		res := []response{calendarCollectionResponse(href, birthdayName, &birthdayDesc, nil, principalHref, syncToken, "0", true)}

		if depth == "1" {
			events, err := h.generateBirthdayEvents(ctx, user.ID)
			if err != nil {
				return nil, err
			}
			base := ensureCollectionHref(href)
			res = append(res, calendarResourceResponses(base, events)...)
		}
		return res, nil
	}

	var cal *store.CalendarAccess
	if err != nil {
		cal, err = h.loadCalendarByName(ctx, user, segments[0])
		if err != nil {
			if errors.Is(err, errAmbiguousCalendar) {
				return nil, errAmbiguousCalendar
			}
			return nil, http.ErrNotSupported
		}
	} else {
		cal, err = h.loadDiscoverableCalendar(ctx, user, calID)
		if err != nil {
			return nil, err
		}
	}

	if len(segments) == 2 {
		resourceName := strings.TrimSuffix(segments[1], path.Ext(segments[1]))
		if resourceName == "" {
			return nil, http.ErrNotSupported
		}
		href := ensureCollectionHref(path.Join("/dav/calendars", fmt.Sprint(cal.ID)))
		resourceHref := strings.TrimSuffix(href, "/") + "/" + resourceName + ".ics"
		allowed, err := h.canReadCalendarObject(ctx, user, cal, resourceName)
		if err != nil {
			return nil, err
		}
		if !allowed {
			return []response{{Href: resourceHref, Status: httpStatusNotFound}}, nil
		}
		event, err := h.store.Events.GetByResourceName(ctx, cal.ID, resourceName)
		if err != nil {
			return nil, err
		}
		if event == nil {
			return []response{{Href: resourceHref, Status: httpStatusNotFound}}, nil
		}
		return []response{resourceResponse(resourceHref, calendarResourcePropstat(event.ETag, event.RawICAL, true))}, nil
	}

	href := ensureCollectionHref(path.Join("/dav/calendars", fmt.Sprint(cal.ID)))
	ctag := fmt.Sprintf("%d", cal.CTag)
	syncToken := buildSyncToken("cal", cal.ID, cal.UpdatedAt)
	principalHref := h.principalURL(user)
	res := []response{calendarCollectionResponseWithPrivileges(href, cal.Name, cal.Description, cal.Timezone, principalHref, syncToken, ctag, cal.EffectivePrivileges())}
	if depth == "1" {
		events, err := h.store.Events.ListForCalendar(ctx, cal.ID)
		if err != nil {
			return nil, err
		}
		events, err = h.filterReadableCalendarEvents(ctx, user, cal, events)
		if err != nil {
			return nil, err
		}
		base := ensureCollectionHref(href)
		res = append(res, calendarResourceResponses(base, events)...)
	}
	return res, nil
}

func (h *Handler) loadDiscoverableCalendar(ctx context.Context, user *store.User, calendarID int64) (*store.CalendarAccess, error) {
	cal, err := h.loadCalendar(ctx, user, calendarID)
	if err == nil {
		return cal, nil
	}
	if err != store.ErrNotFound && !errors.Is(err, errForbidden) {
		return nil, err
	}

	calendars, err := h.accessibleCalendars(ctx, user)
	if err != nil {
		return nil, err
	}
	for _, candidate := range calendars {
		if candidate.ID == calendarID {
			copy := candidate
			return &copy, nil
		}
	}
	return nil, store.ErrNotFound
}

func (h *Handler) addressBookResponses(ctx context.Context, cleanPath, depth string, user *store.User, ensureCollectionHref func(string) string, propfindReq *propfindRequest) ([]response, error) {
	relPath := strings.Trim(strings.TrimPrefix(cleanPath, "/dav/addressbooks"), "/")
	if relPath == "" {
		base := ensureCollectionHref("/dav/addressbooks")
		res := []response{collectionResponse(base, "Address Books")}
		if depth == "1" {
			books, err := h.accessibleAddressBooks(ctx, user)
			if err != nil {
				return nil, err
			}
			principalHref := h.principalURL(user)
			for _, b := range books {
				href := ensureCollectionHref(path.Join("/dav/addressbooks", fmt.Sprint(b.ID)))
				ctag := fmt.Sprintf("%d", b.CTag)
				syncToken := buildSyncToken("card", b.ID, b.UpdatedAt)
				res = append(res, addressBookCollectionResponse(href, b.Name, b.Description, principalHref, syncToken, ctag))
			}
		}
		return res, nil
	}

	segments := strings.Split(relPath, "/")
	if len(segments) > 2 {
		return nil, http.ErrNotSupported
	}
	bookID, ok, err := h.resolveAddressBookID(ctx, user, strings.TrimSpace(segments[0]))
	if err != nil {
		if errors.Is(err, errAmbiguousAddressBook) {
			return nil, errAmbiguousAddressBook
		}
		return nil, http.ErrNotSupported
	}
	if !ok {
		return nil, http.ErrNotSupported
	}
	book, err := h.loadAddressBookWithPrivilege(ctx, user, bookID, cleanPath, "read")
	if err != nil {
		return nil, err
	}
	collectionHref := ensureCollectionHref(cleanPath)
	if len(segments) == 2 {
		collectionHref = ensureCollectionHref(strings.TrimSuffix(cleanPath, "/"+segments[1]))
	}
	if len(segments) == 2 {
		resourceName := strings.TrimSuffix(segments[1], path.Ext(segments[1]))
		if resourceName == "" {
			return nil, http.ErrNotSupported
		}
		contact, err := h.store.Contacts.GetByResourceName(ctx, book.ID, resourceName)
		if err != nil {
			return nil, err
		}
		href := strings.TrimSuffix(collectionHref, "/") + "/" + resourceName + ".vcf"
		if contact == nil {
			return []response{{Href: href, Status: httpStatusNotFound}}, nil
		}
		return []response{resourceResponse(href, addressBookResourcePropstat(contact.ETag, contact.RawVCard, true))}, nil
	}
	href := collectionHref
	ctag := fmt.Sprintf("%d", book.CTag)
	syncToken := buildSyncToken("card", book.ID, book.UpdatedAt)
	principalHref := h.principalURL(user)
	res := []response{addressBookCollectionResponse(href, book.Name, book.Description, principalHref, syncToken, ctag)}
	if depth == "1" {
		contacts, err := h.store.Contacts.ListForBook(ctx, book.ID)
		if err != nil {
			return nil, err
		}
		contacts, err = h.filterReadableAddressBookContacts(ctx, user, book, contacts)
		if err != nil {
			return nil, err
		}
		base := ensureCollectionHref(href)
		resourceResponses := addressBookResourceResponses(base, contacts)
		res = append(res, resourceResponses...)
	}
	return res, nil
}

func (h *Handler) loadCalendar(ctx context.Context, user *store.User, id int64) (*store.CalendarAccess, error) {
	return h.loadCalendarWithAnyPrivilege(ctx, user, id, path.Join("/dav/calendars", fmt.Sprint(id)))
}

func (h *Handler) loadCalendarByName(ctx context.Context, user *store.User, name string) (*store.CalendarAccess, error) {
	accessible, err := h.accessibleCalendars(ctx, user)
	if err != nil {
		return nil, err
	}
	var match *store.CalendarAccess
	for _, c := range accessible {
		if (c.Slug != nil && *c.Slug == strings.ToLower(name)) || c.Name == name {
			if match != nil {
				return nil, errAmbiguousCalendar
			}
			copy := c
			match = &copy
		}
	}
	if match == nil {
		return nil, store.ErrNotFound
	}
	return match, nil
}

func (h *Handler) loadAddressBook(ctx context.Context, user *store.User, id int64) (*store.AddressBook, error) {
	book, err := h.getAddressBook(ctx, id)
	if err != nil {
		return nil, err
	}
	if book.UserID != user.ID {
		return nil, store.ErrNotFound
	}
	return book, nil
}

func (h *Handler) getAddressBook(ctx context.Context, id int64) (*store.AddressBook, error) {
	if h == nil || h.store == nil || h.store.AddressBooks == nil {
		return nil, store.ErrNotFound
	}
	book, err := h.store.AddressBooks.GetByID(ctx, id)
	if err != nil {
		return nil, err
	}
	if book == nil {
		return nil, store.ErrNotFound
	}
	return book, nil
}

func addressBookCollectionPath(cleanPath string) string {
	cleanPath = normalizeDAVHref(cleanPath)
	if !strings.HasPrefix(cleanPath, "/dav/addressbooks/") {
		return cleanPath
	}
	trimmed := strings.TrimPrefix(cleanPath, "/dav/addressbooks/")
	parts := strings.Split(trimmed, "/")
	if len(parts) == 0 || strings.TrimSpace(parts[0]) == "" {
		return "/dav/addressbooks"
	}
	return path.Join("/dav/addressbooks", parts[0])
}

func (h *Handler) addressBookPrivilegeDecision(ctx context.Context, user *store.User, book *store.AddressBook, cleanPath, privilege string) (bool, bool, error) {
	if book == nil {
		return false, false, nil
	}
	if canonicalPath, err := h.canonicalDAVPath(ctx, user, cleanPath); err == nil && canonicalPath != "" {
		cleanPath = canonicalPath
	} else if err != nil {
		return false, false, err
	}
	if user != nil && book.UserID == user.ID {
		return true, false, nil
	}
	if granted, decided, err := h.aclDecisionMatchingPrivilege(ctx, user, cleanPath, privilege); err != nil {
		return false, false, err
	} else if decided {
		return granted, !granted, nil
	}
	collectionPath := addressBookCollectionPath(cleanPath)
	if collectionPath != cleanPath {
		if granted, decided, err := h.aclDecisionMatchingPrivilege(ctx, user, collectionPath, privilege); err != nil {
			return false, false, err
		} else if decided {
			return granted, !granted, nil
		}
	}
	return false, false, nil
}

func (h *Handler) requireAddressBookPrivilege(ctx context.Context, user *store.User, book *store.AddressBook, cleanPath, privilege string) error {
	allowed, denied, err := h.addressBookPrivilegeDecision(ctx, user, book, cleanPath, privilege)
	if err != nil {
		return err
	}
	if allowed {
		return nil
	}
	if denied {
		return errForbidden
	}
	return store.ErrNotFound
}

func (h *Handler) loadAddressBookWithPrivilege(ctx context.Context, user *store.User, id int64, cleanPath, privilege string) (*store.AddressBook, error) {
	book, err := h.getAddressBook(ctx, id)
	if err != nil {
		return nil, err
	}
	if err := h.requireAddressBookPrivilege(ctx, user, book, cleanPath, privilege); err != nil {
		return nil, err
	}
	return book, nil
}

func (h *Handler) loadAddressBookByName(ctx context.Context, user *store.User, name string) (*store.AddressBook, error) {
	if h.store == nil || h.store.AddressBooks == nil {
		return nil, store.ErrNotFound
	}
	books, err := h.store.AddressBooks.ListByUser(ctx, user.ID)
	if err != nil {
		return nil, err
	}
	var match *store.AddressBook
	for _, book := range books {
		if book.Name != name {
			continue
		}
		if match != nil {
			return nil, errAmbiguousAddressBook
		}
		copy := book
		match = &copy
	}
	if match == nil {
		return nil, store.ErrNotFound
	}
	return match, nil
}

func (h *Handler) resolveAddressBookID(ctx context.Context, user *store.User, segment string) (int64, bool, error) {
	if segment == "" {
		return 0, false, nil
	}
	if id, err := strconv.ParseInt(segment, 10, 64); err == nil {
		return id, true, nil
	}
	if user == nil {
		return 0, false, store.ErrNotFound
	}
	book, err := h.loadAddressBookByName(ctx, user, segment)
	if err != nil {
		if errors.Is(err, errAmbiguousAddressBook) {
			return 0, false, errAmbiguousAddressBook
		}
		if err == store.ErrNotFound {
			return 0, false, store.ErrNotFound
		}
		return 0, false, err
	}
	return book.ID, true, nil
}

func (h *Handler) resolveCalendarID(ctx context.Context, user *store.User, segment string) (int64, bool, error) {
	if segment == "" {
		return 0, false, nil
	}
	if id, err := strconv.ParseInt(segment, 10, 64); err == nil {
		return id, true, nil
	}
	if h.store == nil || h.store.Calendars == nil {
		return 0, false, nil
	}
	cal, err := h.loadCalendarByName(ctx, user, segment)
	if err != nil {
		if errors.Is(err, errAmbiguousCalendar) {
			return 0, false, errAmbiguousCalendar
		}
		if err == store.ErrNotFound {
			return 0, false, store.ErrNotFound
		}
		return 0, false, err
	}
	return cal.ID, true, nil
}

func (h *Handler) parseCalendarResourcePath(ctx context.Context, user *store.User, rawPath string) (int64, string, bool, error) {
	segment, resource, ok := parseCalendarResourceSegments(rawPath)
	if !ok {
		return 0, "", false, nil
	}
	id, ok, err := h.resolveCalendarID(ctx, user, segment)
	if err != nil {
		if errors.Is(err, errAmbiguousCalendar) {
			return 0, resource, true, errAmbiguousCalendar
		}
		if err == store.ErrNotFound {
			return 0, resource, true, err
		}
		return 0, "", false, err
	}
	if !ok {
		return 0, resource, true, store.ErrNotFound
	}
	return id, resource, true, nil
}

func parseAddressBookResourceSegments(rawPath string) (string, string, bool) {
	cleanPath := normalizeDAVHref(rawPath)
	if cleanPath == "" || !strings.HasPrefix(cleanPath, "/dav/addressbooks/") {
		return "", "", false
	}
	trimmed := strings.TrimPrefix(cleanPath, "/dav/addressbooks/")
	parts := strings.Split(trimmed, "/")
	if len(parts) != 2 || parts[0] == "" || parts[1] == "" {
		return "", "", false
	}
	uid := strings.TrimSuffix(parts[1], path.Ext(parts[1]))
	if uid == "" {
		return "", "", false
	}
	return parts[0], uid, true
}

func (h *Handler) parseAddressBookResourcePath(ctx context.Context, user *store.User, rawPath string) (int64, string, bool, error) {
	segment, resource, ok := parseAddressBookResourceSegments(rawPath)
	if !ok {
		return 0, "", false, nil
	}
	id, ok, err := h.resolveAddressBookID(ctx, user, segment)
	if err != nil {
		if errors.Is(err, errAmbiguousAddressBook) {
			return 0, resource, true, errAmbiguousAddressBook
		}
		if err == store.ErrNotFound {
			return 0, resource, true, err
		}
		return 0, "", false, err
	}
	if !ok {
		return 0, resource, true, store.ErrNotFound
	}
	return id, resource, true, nil
}

// parseResourcePath extracts the numeric collection ID and resource name from a DAV resource path.
// The returned boolean indicates whether the path matched the expected prefix and contained both parts.
func parseResourcePath(rawPath, prefix string) (int64, string, bool) {
	cleanPath := normalizeDAVHref(rawPath)
	if cleanPath == "" || !strings.HasPrefix(cleanPath, prefix) {
		return 0, "", false
	}
	trimmed := strings.TrimPrefix(cleanPath, prefix)
	trimmed = strings.TrimPrefix(trimmed, "/")
	parts := strings.Split(trimmed, "/")
	if len(parts) < 2 || parts[0] == "" || parts[1] == "" {
		return 0, "", false
	}
	id, err := strconv.ParseInt(parts[0], 10, 64)
	if err != nil {
		return 0, "", false
	}
	uid := strings.TrimSuffix(parts[1], path.Ext(parts[1]))
	if uid == "" {
		return 0, "", false
	}
	return id, uid, true
}

func normalizeDAVHref(raw string) string {
	trimmed := strings.TrimSpace(raw)
	if trimmed == "" {
		return ""
	}
	if u, err := url.Parse(trimmed); err == nil {
		if u.Path != "" {
			trimmed = u.Path
		}
	}
	cleaned := path.Clean(trimmed)
	if cleaned == "." {
		cleaned = "/"
	}
	if !strings.HasPrefix(cleaned, "/") {
		cleaned = "/" + strings.TrimPrefix(cleaned, "/")
	}
	return cleaned
}

func resolveDAVHref(basePath, rawHref string) string {
	trimmed := strings.TrimSpace(rawHref)
	if trimmed == "" {
		return ""
	}
	if strings.HasPrefix(trimmed, "http://") || strings.HasPrefix(trimmed, "https://") {
		if u, err := url.Parse(trimmed); err == nil {
			return normalizeDAVHref(u.Path)
		}
		return ""
	}
	if strings.HasPrefix(trimmed, "/") {
		return normalizeDAVHref(trimmed)
	}
	if u, err := url.Parse(trimmed); err == nil && u.Path != "" {
		if strings.HasPrefix(u.Path, "/") {
			return normalizeDAVHref(u.Path)
		}
		trimmed = u.Path
	}
	base := normalizeDAVHref(basePath)
	if base == "" {
		base = "/"
	}
	if !strings.HasSuffix(base, "/") {
		if _, _, ok := parseCalendarResourceSegments(base); ok {
			base = path.Dir(base) + "/"
		} else if _, _, ok := parseAddressBookResourceSegments(base); ok {
			base = path.Dir(base) + "/"
		} else {
			base += "/"
		}
	}
	return normalizeDAVHref(path.Join(base, trimmed))
}

// isValidCalendarSlug validates calendar slugs for path safety.
// Slugs must: start/end with alphanumeric, contain only [a-z0-9-], be 1-64 chars.
func isValidCalendarSlug(slug string) bool {
	if len(slug) == 0 || len(slug) > 64 {
		return false
	}
	// Must start and end with alphanumeric (not hyphen)
	if slug[0] == '-' || slug[len(slug)-1] == '-' {
		return false
	}
	for _, ch := range slug {
		if !((ch >= 'a' && ch <= 'z') || (ch >= '0' && ch <= '9') || ch == '-') {
			return false
		}
	}
	return true
}

const syncTokenPrefix = "urn:calcard-sync"

type syncTokenInfo struct {
	Kind      string
	ID        int64
	Timestamp time.Time
}

func buildSyncToken(kind string, id int64, ts time.Time) string {
	nanos := int64(0)
	if !ts.IsZero() {
		nanos = ts.UTC().UnixNano()
	}
	return fmt.Sprintf("%s:%s:%d:%d", syncTokenPrefix, kind, id, nanos)
}

func parseSyncToken(token string) (syncTokenInfo, error) {
	if token == "" || !strings.HasPrefix(token, syncTokenPrefix+":") {
		return syncTokenInfo{}, errInvalidSyncToken
	}
	parts := strings.Split(token[len(syncTokenPrefix)+1:], ":")
	if len(parts) != 3 {
		return syncTokenInfo{}, errInvalidSyncToken
	}
	id, err := strconv.ParseInt(parts[1], 10, 64)
	if err != nil {
		return syncTokenInfo{}, errInvalidSyncToken
	}
	nanos, err := strconv.ParseInt(parts[2], 10, 64)
	if err != nil {
		return syncTokenInfo{}, errInvalidSyncToken
	}
	info := syncTokenInfo{Kind: parts[0], ID: id}
	if nanos > 0 {
		info.Timestamp = time.Unix(0, nanos).UTC()
	}
	return info, nil
}

func (h *Handler) calendarSyncTokenValue(ctx context.Context, cal *store.CalendarAccess) (string, time.Time) {
	return buildSyncToken("cal", cal.ID, cal.UpdatedAt), cal.UpdatedAt
}

func (h *Handler) addressBookSyncTokenValue(ctx context.Context, book *store.AddressBook) (string, time.Time) {
	return buildSyncToken("card", book.ID, book.UpdatedAt), book.UpdatedAt
}

func (h *Handler) principalURL(user *store.User) string {
	return fmt.Sprintf("/dav/principals/%d/", user.ID)
}

func (h *Handler) principalResponses(cleanPath, depth string, user *store.User, ensureCollectionHref func(string) string) ([]response, error) {
	relPath := strings.Trim(strings.TrimPrefix(cleanPath, "/dav/principals"), "/")
	principalHref := ensureCollectionHref(h.principalURL(user))

	// Only the authenticated user's principal is exposed.
	if relPath == "" {
		res := []response{collectionResponse(ensureCollectionHref("/dav/principals"), "Principals")}
		if depth == "1" {
			res = append(res, principalResponse(principalHref, user))
		}
		return res, nil
	}

	if relPath != fmt.Sprint(user.ID) && relPath != fmt.Sprint(user.ID)+"/" {
		return nil, store.ErrNotFound
	}

	return []response{principalResponse(principalHref, user)}, nil
}

func principalResponse(href string, user *store.User) response {
	p := prop{
		DisplayName:             user.PrimaryEmail,
		ResourceType:            resourceType{Principal: &struct{}{}},
		PrincipalURL:            &expandableHrefProp{Href: href},
		CurrentUserPrincipal:    &expandableHrefProp{Href: href},
		CurrentUserPrincipalURL: &hrefProp{Href: href},
		CalendarHomeSet:         &hrefListProp{Href: []string{"/dav/calendars/"}},
		AddressbookHomeSet:      &hrefListProp{Href: []string{"/dav/addressbooks/"}},
		SupportedReportSet:      combinedSupportedReports(),
	}
	return response{Href: href, Propstat: []propstat{{Prop: p, Status: httpStatusOK}}}
}

func rootCollectionResponse(href string, user *store.User, principalHref string) response {
	p := prop{
		DisplayName:             "CalCard DAV",
		ResourceType:            resourceType{Collection: &struct{}{}},
		CurrentUserPrincipal:    &expandableHrefProp{Href: principalHref},
		CurrentUserPrincipalURL: &hrefProp{Href: principalHref},
		SupportedReportSet:      combinedSupportedReports(),
	}
	return response{Href: href, Propstat: []propstat{{Prop: p, Status: httpStatusOK}}}
}

func (h *Handler) expandedPrincipalProp(user *store.User, selections expandPropertySelection) prop {
	principalHref := h.principalURL(user)
	principalResp := principalResponse(principalHref, user)
	result := prop{}
	if selections.CurrentUserPrincipal != nil {
		filtered := principalResp
		if selections.CurrentUserPrincipal.Prop != nil {
			filtered = filterPrincipalPropfindResponse(principalResp, selections.CurrentUserPrincipal)
		}
		result.CurrentUserPrincipal = &expandableHrefProp{Response: []response{filtered}}
	}
	if selections.PrincipalURL != nil {
		filtered := principalResp
		if selections.PrincipalURL.Prop != nil {
			filtered = filterPrincipalPropfindResponse(principalResp, selections.PrincipalURL)
		}
		result.PrincipalURL = &expandableHrefProp{Response: []response{filtered}}
	}
	return result
}

func (h *Handler) calendarReportResponses(ctx context.Context, user *store.User, cal *store.CalendarAccess, principalHref, resolvePath, responsePath string, report reportRequest) ([]response, string, error) {
	calData := reportCalendarData(report)
	switch report.XMLName.Local {
	case "calendar-multiget":
		res, err := h.calendarMultiGet(ctx, user, cal, report.Hrefs, resolvePath, responsePath, calData)
		return res, "", err
	case "calendar-query":
		res, err := h.calendarQuery(ctx, user, cal, responsePath, report.Filter, calData)
		return res, "", err
	case "free-busy-query":
		res, err := h.freeBusyQuery(ctx, user, cal, responsePath, report.Filter)
		return res, "", err
	case "sync-collection":
		return h.calendarSyncCollection(ctx, user, cal, principalHref, responsePath, report, calData)
	default:
		// Fallback: return all events to keep clients moving even if they send unsupported report types.
		res, err := h.calendarQuery(ctx, user, cal, responsePath, nil, calData)
		return res, "", err
	}
}

func (h *Handler) addressBookReportResponses(ctx context.Context, user *store.User, book *store.AddressBook, principalHref, cleanPath string, report reportRequest, expandReq *expandPropertyRequest) ([]response, string, error) {
	targetResourceName := ""
	addressDataReq := reportAddressData(report)
	if _, resourceName, matched := parseAddressBookResourceSegments(cleanPath); matched {
		targetResourceName = resourceName
	}
	switch report.XMLName.Local {
	case "addressbook-multiget":
		res, err := h.addressBookMultiGetReport(ctx, user, book, report.Hrefs, cleanPath, report.Prop, addressDataReq)
		return res, "", err
	case "addressbook-query":
		res, err := h.addressBookQuery(ctx, user, book, cleanPath, report.CardFilter, report.Prop, addressDataReq, report.Limit)
		return res, "", err
	case "expand-property":
		collectionHref := strings.TrimSuffix(cleanPath, "/")
		if targetResourceName == "" {
			collectionHref += "/"
		}
		if targetResourceName != "" {
			contact, err := h.store.Contacts.GetByResourceName(ctx, book.ID, targetResourceName)
			if err != nil {
				return nil, "", fmt.Errorf("failed to fetch contact")
			}
			if contact == nil {
				return []response{{Href: collectionHref, Status: httpStatusNotFound}}, "", nil
			}
			return []response{buildAddressObjectExpandPropertyResponse(collectionHref, *contact, expandReq)}, "", nil
		}
		resp := addressBookCollectionResponse(collectionHref, book.Name, book.Description, principalHref, buildSyncToken("card", book.ID, book.UpdatedAt), fmt.Sprintf("%d", book.CTag))
		selections := expandPropertySelections(expandReq)
		if len(resp.Propstat) > 0 {
			expanded := h.expandedPrincipalProp(user, selections)
			if expanded.CurrentUserPrincipal != nil {
				resp.Propstat[0].Prop.CurrentUserPrincipal = expanded.CurrentUserPrincipal
			}
			if expanded.PrincipalURL != nil {
				resp.Propstat[0].Prop.PrincipalURL = expanded.PrincipalURL
			}
		}
		return []response{resp}, "", nil
	case "sync-collection":
		return h.addressBookSyncCollection(ctx, user, book, principalHref, cleanPath, report)
	default:
		res, err := h.addressBookQuery(ctx, user, book, cleanPath, report.CardFilter, report.Prop, addressDataReq, report.Limit)
		return res, "", err
	}
}

func (h *Handler) applyCalendarFilter(events []store.Event, filter *calFilter) []store.Event {
	if filter == nil {
		return events
	}

	var filtered []store.Event
	for _, event := range events {
		if h.eventMatchesFilter(event, filter) {
			filtered = append(filtered, event)
		}
	}
	return filtered
}

func (h *Handler) eventMatchesFilter(event store.Event, filter *calFilter) bool {
	return h.matchesCompFilter(event, &filter.CompFilter)
}

func (h *Handler) matchesCompFilter(event store.Event, compFilter *compFilter) bool {
	compType := compFilter.Name
	if compType != "" && !h.hasComponent(event.RawICAL, compType) {
		return false
	}

	if compFilter.TimeRange != nil {
		if !h.eventInTimeRange(event, compFilter.TimeRange) {
			return false
		}
	}

	for _, nestedFilter := range compFilter.CompFilter {
		if !h.matchesCompFilter(event, &nestedFilter) {
			return false
		}
	}

	for _, propFilter := range compFilter.PropFilter {
		if !h.matchesPropFilter(event, &propFilter) {
			return false
		}
	}

	if compFilter.TextMatch != nil {
		if !h.matchesTextMatch(event.RawICAL, compFilter.TextMatch) {
			return false
		}
	}

	return true
}

func (h *Handler) matchesPropFilter(event store.Event, propFilter *propFilter) bool {
	propName := strings.ToUpper(propFilter.Name)
	hasProp := strings.Contains(strings.ToUpper(event.RawICAL), propName+":")

	if propFilter.IsNotDefined != nil {
		return !hasProp
	}

	if !hasProp {
		return false
	}

	if propFilter.TextMatch != nil {
		return h.matchesTextMatch(event.RawICAL, propFilter.TextMatch)
	}

	return true
}

func (h *Handler) matchesTextMatch(icalData string, textMatch *textMatch) bool {
	text := strings.TrimSpace(textMatch.Text)
	if text == "" {
		return true
	}

	// Case-insensitive contains check (simplified - RFC 4790 has more complex rules)
	matches := strings.Contains(strings.ToUpper(icalData), strings.ToUpper(text))

	if textMatch.NegateCondition == "yes" {
		return !matches
	}

	return matches
}

func (h *Handler) hasComponent(icalData, componentType string) bool {
	componentType = strings.ToUpper(componentType)
	beginMarker := "BEGIN:" + componentType
	return strings.Contains(strings.ToUpper(icalData), beginMarker)
}

func (h *Handler) eventInTimeRange(event store.Event, tr *timeRange) bool {
	start, err := parseICalDateTime(tr.Start)
	if err != nil {
		return true // If we can't parse filter, include the event
	}

	var end time.Time
	if tr.End != "" {
		end, err = parseICalDateTime(tr.End)
		if err != nil {
			return true
		}
	} else {
		// No end means unbounded
		end = time.Date(9999, 12, 31, 23, 59, 59, 0, time.UTC)
	}

	if strings.Contains(strings.ToUpper(event.RawICAL), "RRULE:") {
		return h.recurringEventInTimeRange(event, start, end)
	}

	if event.DTStart != nil {
		eventEnd := event.DTEnd
		if eventEnd == nil {
			// If no end time, use start time
			eventEnd = event.DTStart
		}

		return event.DTStart.Before(end) && eventEnd.After(start)
	}

	return true
}

func (h *Handler) recurringEventInTimeRange(event store.Event, rangeStart, rangeEnd time.Time) bool {
	if event.DTStart == nil {
		return true
	}

	rrule := extractRRule(event.RawICAL)
	if rrule == "" {
		return true // Malformed, be permissive
	}

	// Parse RRULE parameters
	freq := extractRRuleParam(rrule, "FREQ")
	countStr := extractRRuleParam(rrule, "COUNT")
	untilStr := extractRRuleParam(rrule, "UNTIL")
	intervalStr := extractRRuleParam(rrule, "INTERVAL")

	interval := 1
	if intervalStr != "" {
		if i, err := strconv.Atoi(intervalStr); err == nil && i > 0 {
			interval = i
		}
	}

	maxOccurrences := 500 // Default limit to prevent infinite loops
	if countStr != "" {
		if c, err := strconv.Atoi(countStr); err == nil && c > 0 {
			maxOccurrences = c
		}
	}

	recurrenceEnd := rangeEnd.AddDate(0, 0, 1) // Default to just past query range
	if untilStr != "" {
		if until, err := parseICalDateTime(untilStr); err == nil {
			recurrenceEnd = until
		}
	}

	eventDuration := time.Hour // Default 1 hour
	if event.DTEnd != nil {
		eventDuration = event.DTEnd.Sub(*event.DTStart)
	}

	current := *event.DTStart
	for i := 0; i < maxOccurrences; i++ {
		if current.After(recurrenceEnd) {
			break
		}

		if current.After(rangeEnd.AddDate(0, 0, 7)) {
			break
		}

		instanceEnd := current.Add(eventDuration)
		if current.Before(rangeEnd) && instanceEnd.After(rangeStart) {
			return true
		}

		switch strings.ToUpper(freq) {
		case "DAILY":
			current = current.AddDate(0, 0, interval)
		case "WEEKLY":
			current = current.AddDate(0, 0, 7*interval)
		case "MONTHLY":
			current = current.AddDate(0, interval, 0)
		case "YEARLY":
			current = current.AddDate(interval, 0, 0)
		default:
			// Unknown frequency, be permissive
			return true
		}

		// Safety check
		if current.After(event.DTStart.AddDate(3, 0, 0)) && i > 100 {
			break
		}
	}

	return false
}

func (h *Handler) freeBusyQuery(ctx context.Context, user *store.User, cal *store.CalendarAccess, cleanPath string, filter *calFilter) ([]response, error) {
	events, err := h.store.Events.ListForCalendar(ctx, cal.ID)
	if err != nil {
		return nil, fmt.Errorf("failed to list events")
	}

	if filter != nil {
		events = h.applyCalendarFilter(events, filter)
	}
	events, err = h.filterCalendarEventsByPrivilege(ctx, user, cal, events, "read-free-busy")
	if err != nil {
		return nil, err
	}

	freeBusyData := h.generateFreeBusy(events, filter)

	href := strings.TrimSuffix(cleanPath, "/") + "/freebusy.ics"
	etag := fmt.Sprintf("%x", sha256.Sum256([]byte(freeBusyData)))

	return []response{
		resourceResponse(href, etagProp(etag, freeBusyData, true)),
	}, nil
}

func (h *Handler) generateFreeBusy(events []store.Event, filter *calFilter) string {
	var sb strings.Builder
	sb.WriteString("BEGIN:VCALENDAR\r\n")
	sb.WriteString("VERSION:2.0\r\n")
	sb.WriteString("PRODID:-//CalCard//CalDAV Server//EN\r\n")
	sb.WriteString("BEGIN:VFREEBUSY\r\n")
	sb.WriteString(fmt.Sprintf("DTSTAMP:%s\r\n", time.Now().UTC().Format("20060102T150405Z")))

	if filter != nil && filter.CompFilter.TimeRange != nil {
		if filter.CompFilter.TimeRange.Start != "" {
			sb.WriteString(fmt.Sprintf("DTSTART:%s\r\n", filter.CompFilter.TimeRange.Start))
		}
		if filter.CompFilter.TimeRange.End != "" {
			sb.WriteString(fmt.Sprintf("DTEND:%s\r\n", filter.CompFilter.TimeRange.End))
		}
	}

	for _, event := range events {
		if event.DTStart != nil {
			endTime := event.DTEnd
			if endTime == nil {
				endTime = event.DTStart
			}

			startStr := event.DTStart.UTC().Format("20060102T150405Z")
			endStr := endTime.UTC().Format("20060102T150405Z")
			sb.WriteString(fmt.Sprintf("FREEBUSY:%s/%s\r\n", startStr, endStr))
		}
	}

	sb.WriteString("END:VFREEBUSY\r\n")
	sb.WriteString("END:VCALENDAR\r\n")

	return sb.String()
}

func (h *Handler) calendarQuery(ctx context.Context, user *store.User, cal *store.CalendarAccess, cleanPath string, filter *calFilter, calData *calendarDataEl) ([]response, error) {
	events, err := h.store.Events.ListForCalendar(ctx, cal.ID)
	if err != nil {
		return nil, fmt.Errorf("failed to list events")
	}

	if filter != nil {
		events = h.applyCalendarFilter(events, filter)
	}
	events, err = h.filterReadableCalendarEvents(ctx, user, cal, events)
	if err != nil {
		return nil, err
	}

	return calendarResourceResponsesFiltered(cleanPath, events, calData), nil
}

func (h *Handler) calendarMultiGet(ctx context.Context, user *store.User, cal *store.CalendarAccess, hrefs []string, resolvePath, responsePath string, calData *calendarDataEl) ([]response, error) {
	if len(hrefs) == 0 {
		return h.calendarQuery(ctx, user, cal, responsePath, nil, calData)
	}
	responseBase := strings.TrimSuffix(responsePath, "/") + "/"
	var responses []response
	for _, href := range hrefs {
		cleanHref := resolveDAVHref(resolvePath, href)
		if cleanHref == "" {
			continue
		}
		segment, uid, ok := parseCalendarResourceSegments(cleanHref)
		if !ok || !calendarSegmentMatches(cal, segment) {
			continue
		}
		responseHref := responseBase + uid + ".ics"
		ev, err := h.store.Events.GetByResourceName(ctx, cal.ID, uid)
		if err != nil {
			return nil, fmt.Errorf("failed to fetch event")
		}
		if ev == nil {
			responses = append(responses, response{Href: responseHref, Status: httpStatusNotFound})
			continue
		}
		allowed, err := h.canReadCalendarObject(ctx, user, cal, uid)
		if err != nil {
			return nil, err
		}
		if !allowed {
			responses = append(responses, response{Href: responseHref, Status: httpStatusNotFound})
			continue
		}
		rawData := filterICalendarData(ev.RawICAL, calData)
		responses = append(responses, resourceResponse(responseHref, etagProp(ev.ETag, rawData, true)))
	}
	return responses, nil
}

func calendarSegmentMatches(cal *store.CalendarAccess, segment string) bool {
	if segment == "" {
		return false
	}
	if segment == strconv.FormatInt(cal.ID, 10) {
		return true
	}
	normalizedSegment := strings.ToLower(segment)
	if cal.Slug != nil && *cal.Slug == normalizedSegment {
		return true
	}
	return cal.Name == segment
}

func (h *Handler) addressBookQuery(ctx context.Context, user *store.User, book *store.AddressBook, cleanPath string, filter *cardFilter, reqProp *reportProp, addressDataReq *addressDataQuery, limit *addressbookLimit) ([]response, error) {
	contacts, err := h.store.Contacts.ListForBook(ctx, book.ID)
	if err != nil {
		return nil, fmt.Errorf("failed to list contacts")
	}
	targetResourceName := ""
	if _, resourceName, matched := parseAddressBookResourceSegments(cleanPath); matched {
		targetResourceName = resourceName
	}
	baseHref := strings.TrimSuffix(cleanPath, "/") + "/"
	if targetResourceName != "" {
		baseHref = strings.TrimSuffix(strings.TrimSuffix(cleanPath, "/"), "/"+targetResourceName+".vcf") + "/"
	}
	var responses []response
	for _, contact := range contacts {
		resourceName := contactResourceName(contact)
		if targetResourceName != "" && resourceName != targetResourceName {
			continue
		}
		allowed, err := h.canReadAddressBookContact(ctx, user, book, resourceName)
		if err != nil {
			return nil, err
		}
		if !allowed {
			continue
		}
		if !contactMatchesCardFilter(contact, filter) {
			continue
		}
		href := baseHref + resourceName + ".vcf"
		responses = append(responses, buildAddressObjectReportResponse(href, contact, reqProp, addressDataReq))
	}
	if limit != nil && limit.NResults > 0 && len(responses) > limit.NResults {
		responses = responses[:limit.NResults]
		responses = append(responses, response{
			Href:   cleanPath,
			Status: "HTTP/1.1 507 Insufficient Storage",
			Error:  &responseError{NumberOfMatchesWithinLimits: &struct{}{}},
		})
	}
	return responses, nil
}

func (h *Handler) addressBookMultiGet(ctx context.Context, user *store.User, bookID int64, hrefs []string, cleanPath string) ([]response, error) {
	book, err := h.getAddressBook(ctx, bookID)
	if err != nil {
		return nil, err
	}
	return h.addressBookMultiGetReport(ctx, user, book, hrefs, cleanPath, nil, nil)
}

func (h *Handler) addressBookMultiGetReport(ctx context.Context, user *store.User, book *store.AddressBook, hrefs []string, cleanPath string, reqProp *reportProp, addressDataReq *addressDataQuery) ([]response, error) {
	if len(hrefs) == 0 {
		return nil, fmt.Errorf("href required")
	}
	bookID := book.ID
	targetResourceName := ""
	if _, resourceName, matched := parseAddressBookResourceSegments(cleanPath); matched {
		targetResourceName = resourceName
	}
	var responses []response
	for _, href := range hrefs {
		cleanHref := resolveDAVHref(cleanPath, href)
		responseHref := cleanHref
		if responseHref == "" {
			responseHref = strings.TrimSpace(href)
		}
		if responseHref == "" {
			responseHref = cleanPath
		}
		if cleanHref == "" {
			responses = append(responses, response{Href: responseHref, Status: httpStatusNotFound})
			continue
		}
		segment, resourceName, ok := parseAddressBookResourceSegments(cleanHref)
		if !ok {
			responses = append(responses, response{Href: responseHref, Status: httpStatusNotFound})
			continue
		}
		id, ok, err := h.resolveAddressBookID(ctx, user, segment)
		if err != nil || !ok {
			responses = append(responses, response{Href: responseHref, Status: httpStatusNotFound})
			continue
		}
		if id != bookID {
			responses = append(responses, response{Href: responseHref, Status: httpStatusNotFound})
			continue
		}
		c, err := h.store.Contacts.GetByResourceName(ctx, bookID, resourceName)
		if err != nil {
			return nil, fmt.Errorf("failed to fetch contact")
		}
		if c == nil {
			responses = append(responses, response{Href: responseHref, Status: httpStatusNotFound})
			continue
		}
		if targetResourceName != "" && resourceName != targetResourceName {
			responses = append(responses, response{Href: responseHref, Status: httpStatusNotFound})
			continue
		}
		allowed, err := h.canReadAddressBookContact(ctx, user, book, resourceName)
		if err != nil {
			return nil, err
		}
		if !allowed {
			responses = append(responses, response{Href: responseHref, Status: httpStatusNotFound})
			continue
		}
		responses = append(responses, buildAddressObjectReportResponse(responseHref, *c, reqProp, addressDataReq))
	}
	return responses, nil
}

func calendarResourceResponses(base string, events []store.Event) []response {
	return calendarResourceResponsesWithData(base, events, true)
}

func eventResourceName(ev store.Event) string {
	if ev.ResourceName != "" {
		return ev.ResourceName
	}
	return ev.UID
}

func contactResourceName(contact store.Contact) string {
	if contact.ResourceName != "" {
		return contact.ResourceName
	}
	return contact.UID
}

func addressBookContactPath(bookID int64, resourceName string) string {
	return path.Join("/dav/addressbooks", fmt.Sprint(bookID), resourceName)
}

func (h *Handler) canReadAddressBookContact(ctx context.Context, user *store.User, book *store.AddressBook, resourceName string) (bool, error) {
	if strings.TrimSpace(resourceName) == "" {
		return false, nil
	}
	err := h.requireAddressBookPrivilege(ctx, user, book, addressBookContactPath(book.ID, resourceName), "read")
	switch {
	case err == nil:
		return true, nil
	case err == store.ErrNotFound || errors.Is(err, errForbidden):
		return false, nil
	default:
		return false, err
	}
}

func (h *Handler) filterReadableAddressBookContacts(ctx context.Context, user *store.User, book *store.AddressBook, contacts []store.Contact) ([]store.Contact, error) {
	visible := make([]store.Contact, 0, len(contacts))
	for _, contact := range contacts {
		allowed, err := h.canReadAddressBookContact(ctx, user, book, contactResourceName(contact))
		if err != nil {
			return nil, err
		}
		if allowed {
			visible = append(visible, contact)
		}
	}
	return visible, nil
}

func calendarResourceResponsesFiltered(base string, events []store.Event, calData *calendarDataEl) []response {
	baseHref := strings.TrimSuffix(base, "/") + "/"
	var responses []response
	for _, ev := range events {
		href := baseHref + eventResourceName(ev) + ".ics"
		rawData := filterICalendarData(ev.RawICAL, calData)
		responses = append(responses, resourceResponse(href, etagProp(ev.ETag, rawData, true)))
	}
	return responses
}

func calendarResourceResponsesWithData(base string, events []store.Event, includeData bool) []response {
	baseHref := strings.TrimSuffix(base, "/") + "/"
	var responses []response
	for _, ev := range events {
		href := baseHref + eventResourceName(ev) + ".ics"
		responses = append(responses, resourceResponse(href, etagPropWithData(ev.ETag, ev.RawICAL, true, includeData)))
	}
	return responses
}

func addressBookResourceResponses(base string, contacts []store.Contact) []response {
	baseHref := strings.TrimSuffix(base, "/") + "/"
	var responses []response
	for _, c := range contacts {
		href := baseHref + contactResourceName(c) + ".vcf"
		responses = append(responses, resourceResponse(href, etagProp(c.ETag, c.RawVCard, false)))
	}
	return responses
}

func (h *Handler) calendarSyncCollection(ctx context.Context, user *store.User, cal *store.CalendarAccess, principalHref, cleanPath string, report reportRequest, calData *calendarDataEl) ([]response, string, error) {
	syncToken, _ := h.calendarSyncTokenValue(ctx, cal)
	collectionHref := strings.TrimSuffix(cleanPath, "/") + "/"

	var since time.Time
	if report.SyncToken != "" {
		info, err := parseSyncToken(report.SyncToken)
		if err != nil || info.Kind != "cal" || info.ID != cal.ID {
			return nil, "", errInvalidSyncToken
		}
		since = info.Timestamp
	}

	var events []store.Event
	var err error
	if since.IsZero() {
		events, err = h.store.Events.ListForCalendar(ctx, cal.ID)
	} else {
		events, err = h.store.Events.ListModifiedSince(ctx, cal.ID, since)
	}
	if err != nil {
		return nil, "", fmt.Errorf("failed to list events")
	}
	allEvents := events
	events, err = h.filterReadableCalendarEvents(ctx, user, cal, events)
	if err != nil {
		return nil, "", err
	}

	responses := []response{
		calendarCollectionResponseWithPrivileges(collectionHref, cal.Name, cal.Description, cal.Timezone, principalHref, syncToken, fmt.Sprintf("%d", cal.CTag), cal.EffectivePrivileges()),
	}
	responses = append(responses, calendarResourceResponsesFiltered(collectionHref, events, calData)...)

	// Include deleted resources if this is an incremental sync
	if !since.IsZero() {
		deletedHrefs := make(map[string]struct{})
		visible := make(map[string]struct{}, len(events))
		for _, event := range events {
			visible[eventResourceName(event)] = struct{}{}
		}
		for _, event := range allEvents {
			if !event.LastModified.After(since) {
				continue
			}
			resourceName := eventResourceName(event)
			if _, ok := visible[resourceName]; ok {
				continue
			}
			href := collectionHref + resourceName + ".ics"
			responses = append(responses, deletedResponse(href))
			deletedHrefs[href] = struct{}{}
		}
		deleted, err := h.store.DeletedResources.ListDeletedSince(ctx, "event", cal.ID, since)
		if err != nil {
			return nil, "", fmt.Errorf("failed to list deleted events")
		}
		for _, d := range deleted {
			resourceName := d.ResourceName
			if resourceName == "" {
				resourceName = d.UID
			}
			href := collectionHref + resourceName + ".ics"
			if _, ok := deletedHrefs[href]; ok {
				continue
			}
			responses = append(responses, deletedResponse(href))
			deletedHrefs[href] = struct{}{}
		}
	}

	return responses, syncToken, nil
}

func (h *Handler) addressBookSyncCollection(ctx context.Context, user *store.User, book *store.AddressBook, principalHref, cleanPath string, report reportRequest) ([]response, string, error) {
	syncToken, _ := h.addressBookSyncTokenValue(ctx, book)
	collectionHref := strings.TrimSuffix(cleanPath, "/") + "/"

	var since time.Time
	if report.SyncToken != "" {
		info, err := parseSyncToken(report.SyncToken)
		if err != nil || info.Kind != "card" || info.ID != book.ID {
			return nil, "", errInvalidSyncToken
		}
		since = info.Timestamp
	}

	var contacts []store.Contact
	var err error
	if since.IsZero() {
		contacts, err = h.store.Contacts.ListForBook(ctx, book.ID)
	} else {
		contacts, err = h.store.Contacts.ListModifiedSince(ctx, book.ID, since)
	}
	if err != nil {
		return nil, "", fmt.Errorf("failed to list contacts")
	}
	contacts, err = h.filterReadableAddressBookContacts(ctx, user, book, contacts)
	if err != nil {
		return nil, "", err
	}

	responses := []response{
		addressBookCollectionResponse(collectionHref, book.Name, book.Description, principalHref, syncToken, fmt.Sprintf("%d", book.CTag)),
	}
	responses = append(responses, addressBookResourceResponses(collectionHref, contacts)...)

	// Include deleted resources if this is an incremental sync
	if !since.IsZero() {
		deleted, err := h.store.DeletedResources.ListDeletedSince(ctx, "contact", book.ID, since)
		if err != nil {
			return nil, "", fmt.Errorf("failed to list deleted contacts")
		}
		for _, d := range deleted {
			resourceName := d.ResourceName
			if resourceName == "" {
				resourceName = d.UID
			}
			allowed, err := h.canReadAddressBookContact(ctx, user, book, resourceName)
			if err != nil {
				return nil, "", err
			}
			if !allowed {
				continue
			}
			href := collectionHref + resourceName + ".vcf"
			responses = append(responses, deletedResponse(href))
		}
	}

	return responses, syncToken, nil
}

func (h *Handler) generateBirthdayEvents(ctx context.Context, userID int64) ([]store.Event, error) {
	contacts, err := h.store.Contacts.ListWithBirthdaysByUser(ctx, userID)
	if err != nil {
		return nil, err
	}

	now := time.Now()
	currentYear := now.Year()
	var events []store.Event

	for _, c := range contacts {
		if c.Birthday == nil {
			continue
		}

		displayName := "Unknown"
		if c.DisplayName != nil {
			displayName = *c.DisplayName
		}

		// Generate UID for this birthday event (based on contact UID to be stable)
		uid := fmt.Sprintf("birthday-%s@calcard", c.UID)

		var summaryAge string
		if c.Birthday.Year() > 1900 {
			birthdayThisYear := time.Date(currentYear, c.Birthday.Month(), c.Birthday.Day(), 23, 59, 59, 0, time.UTC)
			var ageAtNextBirthday int
			if birthdayThisYear.After(now) {
				ageAtNextBirthday = currentYear - c.Birthday.Year()
			} else {
				ageAtNextBirthday = (currentYear + 1) - c.Birthday.Year()
			}
			summaryAge = fmt.Sprintf(" (turning %d)", ageAtNextBirthday)
		}
		summary := fmt.Sprintf("🎂 %s's Birthday%s", displayName, summaryAge)

		startYear := currentYear
		birthdayThisYear := time.Date(currentYear, c.Birthday.Month(), c.Birthday.Day(), 23, 59, 59, 0, time.UTC)
		if birthdayThisYear.Before(now) {
			startYear = currentYear + 1
		}

		dtstart := time.Date(startYear, c.Birthday.Month(), c.Birthday.Day(), 0, 0, 0, 0, time.UTC)
		dtstartStr := dtstart.Format("20060102")

		// Build the iCal event with yearly recurrence
		var sb strings.Builder
		sb.WriteString("BEGIN:VCALENDAR\r\n")
		sb.WriteString("VERSION:2.0\r\n")
		sb.WriteString("PRODID:-//CalCard//Birthdays//EN\r\n")
		sb.WriteString("BEGIN:VEVENT\r\n")
		sb.WriteString(fmt.Sprintf("UID:%s\r\n", uid))
		sb.WriteString(fmt.Sprintf("DTSTAMP:%s\r\n", time.Now().UTC().Format("20060102T150405Z")))
		sb.WriteString(fmt.Sprintf("DTSTART;VALUE=DATE:%s\r\n", dtstartStr))
		sb.WriteString(fmt.Sprintf("SUMMARY:%s\r\n", escapeICalText(summary)))
		sb.WriteString("RRULE:FREQ=YEARLY\r\n")  // Recurring yearly
		sb.WriteString("TRANSP:TRANSPARENT\r\n") // Free/busy: free time
		sb.WriteString("CLASS:PUBLIC\r\n")

		// Add X-property to mark this as a birthday event
		sb.WriteString("X-CALCARD-TYPE:BIRTHDAY\r\n")
		sb.WriteString(fmt.Sprintf("X-CONTACT-UID:%s\r\n", c.UID))

		sb.WriteString("END:VEVENT\r\n")
		sb.WriteString("END:VCALENDAR\r\n")

		rawICAL := sb.String()
		etag := fmt.Sprintf("%x", sha256.Sum256([]byte(rawICAL)))

		events = append(events, store.Event{
			ID:           0, // Virtual event, no DB ID
			CalendarID:   birthdayCalendarID,
			UID:          uid,
			RawICAL:      rawICAL,
			ETag:         etag,
			Summary:      &summary,
			DTStart:      &dtstart,
			DTEnd:        nil,
			AllDay:       true,
			LastModified: c.LastModified,
		})
	}

	return events, nil
}

func escapeICalText(s string) string {
	s = strings.ReplaceAll(s, "\\", "\\\\")
	s = strings.ReplaceAll(s, ";", "\\;")
	s = strings.ReplaceAll(s, ",", "\\,")
	s = strings.ReplaceAll(s, "\n", "\\n")
	return s
}

func (h *Handler) birthdayCalendarReportResponses(ctx context.Context, user *store.User, principalHref, cleanPath string, report reportRequest) ([]response, string, error) {
	events, err := h.generateBirthdayEvents(ctx, user.ID)
	if err != nil {
		return nil, "", fmt.Errorf("failed to generate birthday events")
	}

	switch report.XMLName.Local {
	case "calendar-multiget":
		res, err := h.birthdayCalendarMultiGet(ctx, events, report.Hrefs, cleanPath)
		return res, "", err
	case "calendar-query":
		if report.Filter != nil {
			events = h.applyCalendarFilter(events, report.Filter)
		}
		return calendarResourceResponses(cleanPath, events), "", nil
	case "free-busy-query":
		if report.Filter != nil {
			events = h.applyCalendarFilter(events, report.Filter)
		}
		freeBusyData := h.generateFreeBusy(events, report.Filter)
		href := strings.TrimSuffix(cleanPath, "/") + "/freebusy.ics"
		etag := fmt.Sprintf("%x", sha256.Sum256([]byte(freeBusyData)))
		return []response{resourceResponse(href, etagProp(etag, freeBusyData, true))}, "", nil
	case "sync-collection":
		if report.SyncToken != "" {
			info, err := parseSyncToken(report.SyncToken)
			if err != nil || info.Kind != "cal" || info.ID != birthdayCalendarID {
				return nil, "", errInvalidSyncToken
			}
		}
		collectionHref := strings.TrimSuffix(cleanPath, "/") + "/"
		// Use a stable sync-token (epoch time) since we always return all events
		syncToken := buildSyncToken("cal", birthdayCalendarID, time.Unix(0, 0))
		birthdayName := "Birthdays"
		birthdayDesc := "Contact birthdays from your address books"
		calData := reportCalendarData(report)
		responses := []response{
			calendarCollectionResponse(collectionHref, birthdayName, &birthdayDesc, nil, principalHref, syncToken, "0", true),
		}
		responses = append(responses, calendarResourceResponsesFiltered(collectionHref, events, calData)...)
		return responses, syncToken, nil
	default:
		// Fallback: return all events
		return calendarResourceResponses(cleanPath, events), "", nil
	}
}

func (h *Handler) birthdayCalendarMultiGet(ctx context.Context, events []store.Event, hrefs []string, cleanPath string) ([]response, error) {
	if len(hrefs) == 0 {
		return calendarResourceResponses(cleanPath, events), nil
	}

	eventsByUID := make(map[string]store.Event)
	for _, ev := range events {
		eventsByUID[ev.UID] = ev
	}

	var responses []response
	for _, href := range hrefs {
		cleanHref := resolveDAVHref(cleanPath, href)
		if cleanHref == "" {
			continue
		}
		// Birthday calendar uses numeric-only parsing (special virtual calendar with constant ID -1)
		id, uid, ok := parseResourcePath(cleanHref, "/dav/calendars")
		if !ok || id != birthdayCalendarID {
			continue
		}
		ev, found := eventsByUID[uid]
		if !found {
			responses = append(responses, response{Href: cleanHref, Status: httpStatusNotFound})
			continue
		}
		responses = append(responses, resourceResponse(cleanHref, etagProp(ev.ETag, ev.RawICAL, true)))
	}
	return responses, nil
}
