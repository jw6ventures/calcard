package dav

import (
	"context"
	"crypto/sha256"
	"encoding/xml"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"path"
	"strconv"
	"strings"
	"time"

	"gitea.jw6.us/james/calcard/internal/auth"
	"gitea.jw6.us/james/calcard/internal/config"
	"gitea.jw6.us/james/calcard/internal/store"
)

// Handler serves WebDAV/CalDAV/CardDAV requests.
type Handler struct {
	cfg   *config.Config
	store *store.Store
}

var errInvalidSyncToken = errors.New("invalid sync token")

// maxDAVBodyBytes is the maximum body size for DAV requests. Preventing DOS attacks.
const maxDAVBodyBytes int64 = 10 * 1024 * 1024

func NewHandler(cfg *config.Config, store *store.Store) *Handler {
	return &Handler{cfg: cfg, store: store}
}

func (h *Handler) Options(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Allow", "OPTIONS, HEAD, GET, PROPFIND, PROPPATCH, MKCOL, MKCALENDAR, PUT, DELETE, REPORT")
	w.Header().Set("DAV", "1, 2, calendar-access, addressbook")
	w.Header().Set("Accept-Patch", "application/xml")
	w.WriteHeader(http.StatusNoContent)
}

func (h *Handler) Head(w http.ResponseWriter, r *http.Request) {
	h.Get(w, r)
}

func (h *Handler) Get(w http.ResponseWriter, r *http.Request) {
	cleanPath := path.Clean(r.URL.Path)
	if !strings.HasPrefix(cleanPath, "/dav") {
		http.Error(w, "not found", http.StatusNotFound)
		return
	}

	user, ok := auth.UserFromContext(r.Context())
	if !ok {
		http.Error(w, "missing user", http.StatusUnauthorized)
		return
	}

	if calendarID, uid, matched := parseResourcePath(cleanPath, "/dav/calendars"); matched {
		if _, err := h.loadCalendar(r.Context(), user, calendarID); err != nil {
			if err == store.ErrNotFound {
				http.Error(w, "not found", http.StatusNotFound)
				return
			}
			http.Error(w, "failed to load calendar", http.StatusInternalServerError)
			return
		}
		event, err := h.store.Events.GetByUID(r.Context(), calendarID, uid)
		if err != nil {
			http.Error(w, "failed to load event", http.StatusInternalServerError)
			return
		}
		if event == nil {
			http.Error(w, "not found", http.StatusNotFound)
			return
		}
		w.Header().Set("Content-Type", "text/calendar")
		w.Header().Set("ETag", fmt.Sprintf("\"%s\"", event.ETag))
		// RFC 4791 Section 5.3.4: Include Last-Modified header
		if !event.LastModified.IsZero() {
			w.Header().Set("Last-Modified", event.LastModified.UTC().Format(http.TimeFormat))
		}
		_, _ = w.Write([]byte(event.RawICAL))
		return
	}

	if addressBookID, uid, matched := parseResourcePath(cleanPath, "/dav/addressbooks"); matched {
		if _, err := h.loadAddressBook(r.Context(), user, addressBookID); err != nil {
			if err == store.ErrNotFound {
				http.Error(w, "not found", http.StatusNotFound)
				return
			}
			http.Error(w, "failed to load address book", http.StatusInternalServerError)
			return
		}
		contact, err := h.store.Contacts.GetByUID(r.Context(), addressBookID, uid)
		if err != nil {
			http.Error(w, "failed to load contact", http.StatusInternalServerError)
			return
		}
		if contact == nil {
			http.Error(w, "not found", http.StatusNotFound)
			return
		}
		w.Header().Set("Content-Type", "text/vcard")
		w.Header().Set("ETag", fmt.Sprintf("\"%s\"", contact.ETag))
		// Include Last-Modified header if available
		if !contact.LastModified.IsZero() {
			w.Header().Set("Last-Modified", contact.LastModified.UTC().Format(http.TimeFormat))
		}
		_, _ = w.Write([]byte(contact.RawVCard))
		return
	}

	w.Header().Set("DAV", "1, 2, calendar-access, addressbook")
	w.WriteHeader(http.StatusOK)
}

func (h *Handler) Propfind(w http.ResponseWriter, r *http.Request) {
	depth := strings.TrimSpace(r.Header.Get("Depth"))
	if depth == "" {
		depth = "1"
	}

	user, ok := auth.UserFromContext(r.Context())
	if !ok {
		http.Error(w, "missing user", http.StatusUnauthorized)
		return
	}

	// Parse PROPFIND request body (RFC 4918 Section 9.1)
	var propfindReq propfindRequest
	if r.ContentLength > 0 {
		body, err := io.ReadAll(io.LimitReader(r.Body, maxDAVBodyBytes))
		if err != nil {
			http.Error(w, "failed to read body", http.StatusBadRequest)
			return
		}
		if err := xml.Unmarshal(body, &propfindReq); err != nil {
			// If parsing fails, default to allprop behavior
			propfindReq.AllProp = &struct{}{}
		}
	} else {
		// Empty body means allprop by default
		propfindReq.AllProp = &struct{}{}
	}

	responses, err := h.buildPropfindResponses(r.Context(), r.URL.Path, depth, user, &propfindReq)
	if err != nil {
		status := http.StatusBadRequest
		if errors.Is(err, store.ErrNotFound) || errors.Is(err, http.ErrNotSupported) {
			status = http.StatusNotFound
		}
		http.Error(w, err.Error(), status)
		return
	}

	payload := multistatus{
		XMLName:  xml.Name{Space: "DAV:", Local: "multistatus"},
		XmlnsD:   "DAV:",
		XmlnsC:   "urn:ietf:params:xml:ns:caldav",
		XmlnsA:   "urn:ietf:params:xml:ns:carddav",
		XmlnsCS:  "http://calendarserver.org/ns/",
		Response: responses,
	}

	w.Header().Set("Content-Type", "application/xml; charset=utf-8")
	w.WriteHeader(http.StatusMultiStatus)
	_ = xml.NewEncoder(w).Encode(payload)
}

// proppatchRequest represents a PROPPATCH request body (RFC 4918 Section 9.2)
type proppatchRequest struct {
	XMLName xml.Name
	Set     *proppatchSet    `xml:"DAV: set"`
	Remove  *proppatchRemove `xml:"DAV: remove"`
}

type proppatchSet struct {
	Prop proppatchProp `xml:"DAV: prop"`
}

type proppatchRemove struct {
	Prop proppatchProp `xml:"DAV: prop"`
}

type proppatchProp struct {
	DisplayName         *string `xml:"DAV: displayname"`
	CalendarDescription *string `xml:"urn:ietf:params:xml:ns:caldav calendar-description"`
	CalendarTimezone    *string `xml:"urn:ietf:params:xml:ns:caldav calendar-timezone"`
	AddressBookDesc     *string `xml:"urn:ietf:params:xml:ns:carddav addressbook-description"`
}

func (h *Handler) Proppatch(w http.ResponseWriter, r *http.Request) {
	user, ok := auth.UserFromContext(r.Context())
	if !ok {
		http.Error(w, "missing user", http.StatusUnauthorized)
		return
	}

	cleanPath := path.Clean(r.URL.Path)

	// Parse PROPPATCH request body
	if r.ContentLength > maxDAVBodyBytes {
		http.Error(w, "request too large", http.StatusRequestEntityTooLarge)
		return
	}
	body, err := io.ReadAll(io.LimitReader(r.Body, maxDAVBodyBytes))
	if err != nil {
		http.Error(w, "failed to read body", http.StatusBadRequest)
		return
	}

	var proppatchReq proppatchRequest
	if err := xml.Unmarshal(body, &proppatchReq); err != nil {
		http.Error(w, "invalid PROPPATCH body", http.StatusBadRequest)
		return
	}

	// Process the property updates
	var responses []response
	success := true

	if strings.HasPrefix(cleanPath, "/dav/calendars/") {
		resp, err := h.proppatchCalendar(r.Context(), user, cleanPath, &proppatchReq)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		responses = append(responses, resp...)
		// Check if any property failed
		for _, r := range resp {
			for _, ps := range r.Propstat {
				if ps.Status != "HTTP/1.1 200 OK" {
					success = false
				}
			}
		}
	} else if strings.HasPrefix(cleanPath, "/dav/addressbooks/") {
		resp, err := h.proppatchAddressBook(r.Context(), user, cleanPath, &proppatchReq)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		responses = append(responses, resp...)
		for _, r := range resp {
			for _, ps := range r.Propstat {
				if ps.Status != "HTTP/1.1 200 OK" {
					success = false
				}
			}
		}
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

	w.Header().Set("Content-Type", "application/xml; charset=utf-8")
	if success {
		w.WriteHeader(http.StatusMultiStatus)
	} else {
		w.WriteHeader(http.StatusMultiStatus)
	}
	_ = xml.NewEncoder(w).Encode(payload)
}

func (h *Handler) proppatchCalendar(ctx context.Context, user *store.User, cleanPath string, req *proppatchRequest) ([]response, error) {
	parts := strings.Split(strings.TrimPrefix(cleanPath, "/dav/calendars"), "/")
	if len(parts) < 2 || strings.TrimSpace(parts[1]) == "" {
		return nil, fmt.Errorf("invalid calendar path")
	}

	calID, err := strconv.ParseInt(parts[1], 10, 64)
	if err != nil {
		return nil, fmt.Errorf("invalid calendar id")
	}

	cal, err := h.loadCalendar(ctx, user, calID)
	if err != nil {
		return nil, err
	}

	// Only owners or editors can modify properties
	if !cal.Editor {
		return []response{{
			Href: cleanPath,
			Propstat: []propstat{{
				Prop:   prop{},
				Status: "HTTP/1.1 403 Forbidden",
			}},
		}}, nil
	}

	// Extract properties to update
	var name *string
	var description *string
	var timezone *string

	if req.Set != nil {
		name = req.Set.Prop.DisplayName
		description = req.Set.Prop.CalendarDescription
		timezone = req.Set.Prop.CalendarTimezone
	}

	// Update the calendar
	if name != nil || description != nil || timezone != nil {
		// Use existing name if not being updated
		updateName := cal.Name
		if name != nil {
			updateName = *name
		}

		err := h.store.Calendars.Update(ctx, user.ID, calID, updateName, description, timezone)
		if err != nil {
			return []response{{
				Href: cleanPath,
				Propstat: []propstat{{
					Prop:   prop{},
					Status: "HTTP/1.1 500 Internal Server Error",
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
			Status: "HTTP/1.1 200 OK",
		}},
	}}, nil
}

func (h *Handler) proppatchAddressBook(ctx context.Context, user *store.User, cleanPath string, req *proppatchRequest) ([]response, error) {
	parts := strings.Split(strings.TrimPrefix(cleanPath, "/dav/addressbooks"), "/")
	if len(parts) < 2 || strings.TrimSpace(parts[1]) == "" {
		return nil, fmt.Errorf("invalid address book path")
	}

	bookID, err := strconv.ParseInt(parts[1], 10, 64)
	if err != nil {
		return nil, fmt.Errorf("invalid address book id")
	}

	book, err := h.loadAddressBook(ctx, user, bookID)
	if err != nil {
		return nil, err
	}

	// Only the owner can modify properties
	if book.UserID != user.ID {
		return []response{{
			Href: cleanPath,
			Propstat: []propstat{{
				Prop:   prop{},
				Status: "HTTP/1.1 403 Forbidden",
			}},
		}}, nil
	}

	// Extract properties to update
	var name *string
	var description *string

	if req.Set != nil {
		name = req.Set.Prop.DisplayName
		description = req.Set.Prop.AddressBookDesc
	}

	// Update the address book
	if name != nil || description != nil {
		updateName := book.Name
		if name != nil {
			updateName = *name
		}

		err := h.store.AddressBooks.Update(ctx, user.ID, bookID, updateName, description)
		if err != nil {
			return []response{{
				Href: cleanPath,
				Propstat: []propstat{{
					Prop:   prop{},
					Status: "HTTP/1.1 500 Internal Server Error",
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
		successProp.AddressBookDesc = *description
	}

	return []response{{
		Href: cleanPath,
		Propstat: []propstat{{
			Prop:   successProp,
			Status: "HTTP/1.1 200 OK",
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
	if !strings.HasPrefix(cleanPath, "/dav/addressbooks/") {
		http.Error(w, "unsupported path", http.StatusBadRequest)
		return
	}
	parts := strings.Split(strings.TrimPrefix(cleanPath, "/dav/addressbooks"), "/")
	name := strings.TrimSpace(parts[len(parts)-1])
	if name == "" {
		http.Error(w, "collection name required", http.StatusBadRequest)
		return
	}
	if _, err := h.store.AddressBooks.Create(r.Context(), store.AddressBook{UserID: user.ID, Name: name}); err != nil {
		http.Error(w, "failed to create", http.StatusInternalServerError)
		return
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
	if !strings.HasPrefix(cleanPath, "/dav/calendars/") {
		http.Error(w, "unsupported path", http.StatusBadRequest)
		return
	}
	parts := strings.Split(strings.TrimPrefix(cleanPath, "/dav/calendars"), "/")

	// RFC 4791 Section 4.2: Calendar collections MUST NOT contain other calendar collections
	// Only allow paths like /dav/calendars/name/, not /dav/calendars/id/name/
	if len(parts) > 2 || (len(parts) == 2 && parts[0] != "" && parts[1] != "") {
		http.Error(w, "nested calendar collections not allowed", http.StatusForbidden)
		return
	}

	name := strings.TrimSpace(parts[len(parts)-1])
	if name == "" {
		http.Error(w, "calendar name required", http.StatusBadRequest)
		return
	}
	if _, err := h.store.Calendars.Create(r.Context(), store.Calendar{UserID: user.ID, Name: name}); err != nil {
		http.Error(w, "failed to create", http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusCreated)
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
	upper := strings.ToUpper(trimmed)
	hasComponent := strings.Contains(upper, "BEGIN:VEVENT") ||
		strings.Contains(upper, "BEGIN:VTODO") ||
		strings.Contains(upper, "BEGIN:VJOURNAL") ||
		strings.Contains(upper, "BEGIN:VFREEBUSY")

	if !hasComponent {
		return fmt.Errorf("no calendar component found (VEVENT, VTODO, VJOURNAL, or VFREEBUSY required)")
	}

	// Check balanced BEGIN/END tags for all component types
	// This validates that every BEGIN has a matching END
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

// extractUIDFromICalendar extracts the UID property from iCalendar data
func extractUIDFromICalendar(icalData string) (string, error) {
	lines := strings.Split(icalData, "\n")
	for _, line := range lines {
		line = strings.TrimSpace(line)
		// Handle both \r\n and \n line endings
		line = strings.TrimSuffix(line, "\r")
		upperLine := strings.ToUpper(line)
		if strings.HasPrefix(upperLine, "UID:") {
			uid := strings.TrimSpace(strings.TrimPrefix(line, "UID:"))
			uid = strings.TrimSpace(strings.TrimPrefix(uid, "uid:"))
			if uid == "" {
				return "", fmt.Errorf("empty UID property")
			}
			return uid, nil
		}
	}
	return "", fmt.Errorf("no UID property found in calendar data")
}

// extractUIDFromVCard extracts the UID property from vCard data
func extractUIDFromVCard(vcardData string) (string, error) {
	lines := strings.Split(vcardData, "\n")
	for _, line := range lines {
		line = strings.TrimSpace(line)
		line = strings.TrimSuffix(line, "\r")
		upperLine := strings.ToUpper(line)
		if strings.HasPrefix(upperLine, "UID:") {
			uid := strings.TrimSpace(strings.TrimPrefix(line, "UID:"))
			uid = strings.TrimSpace(strings.TrimPrefix(uid, "uid:"))
			if uid == "" {
				return "", fmt.Errorf("empty UID property")
			}
			return uid, nil
		}
	}
	return "", fmt.Errorf("no UID property found in vCard data")
}

// validateVCard performs basic validation of vCard data (RFC 6350)
func (h *Handler) validateVCard(data string) error {
	trimmed := strings.TrimSpace(data)

	// Must start with BEGIN:VCARD
	if !strings.HasPrefix(strings.ToUpper(trimmed), "BEGIN:VCARD") {
		return fmt.Errorf("missing BEGIN:VCARD")
	}

	// Must end with END:VCARD
	if !strings.HasSuffix(strings.ToUpper(trimmed), "END:VCARD") {
		return fmt.Errorf("missing END:VCARD")
	}

	// Check balanced BEGIN/END tags
	upper := strings.ToUpper(trimmed)
	beginCount := strings.Count(upper, "BEGIN:VCARD")
	endCount := strings.Count(upper, "END:VCARD")
	if beginCount != endCount {
		return fmt.Errorf("unbalanced VCARD tags")
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
		// Strip quotes from header value
		requestETag := strings.Trim(ifMatch, "\"")
		return requestETag == existing.ETag
	}

	// If-None-Match with specific ETag means "only update if ETag doesn't match"
	if ifNoneMatch != "" {
		if existing == nil {
			return true // Resource doesn't exist, so create it
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
	if r.ContentLength > maxDAVBodyBytes {
		http.Error(w, "request too large", http.StatusRequestEntityTooLarge)
		return
	}
	limitedBody := http.MaxBytesReader(w, r.Body, maxDAVBodyBytes)
	body, err := io.ReadAll(limitedBody)
	if err != nil {
		var maxErr *http.MaxBytesError
		if errors.As(err, &maxErr) {
			http.Error(w, "request too large", http.StatusRequestEntityTooLarge)
		} else {
			http.Error(w, "failed to read body", http.StatusBadRequest)
		}
		return
	}
	etag := fmt.Sprintf("%x", sha256.Sum256(body))

	if calendarID, _, matched := parseResourcePath(cleanPath, "/dav/calendars"); matched {
		cal, err := h.loadCalendar(r.Context(), user, calendarID)
		if err != nil {
			status := http.StatusInternalServerError
			if err == store.ErrNotFound {
				status = http.StatusNotFound
			}
			http.Error(w, "calendar not found", status)
			return
		}
		if !cal.Editor {
			http.Error(w, "forbidden", http.StatusForbidden)
			return
		}

		// Validate iCalendar data
		if err := h.validateICalendar(string(body)); err != nil {
			http.Error(w, fmt.Sprintf("invalid iCalendar data: %v", err), http.StatusBadRequest)
			return
		}

		// Extract UID from calendar data (RFC 4791 Section 4.1)
		uid, err := extractUIDFromICalendar(string(body))
		if err != nil {
			http.Error(w, fmt.Sprintf("invalid iCalendar data: %v", err), http.StatusBadRequest)
			return
		}

		existing, _ := h.store.Events.GetByUID(r.Context(), calendarID, uid)

		// Check conditional request headers (RFC 4791 Section 5.3.1-5.3.2)
		if !h.checkConditionalHeaders(r, existing) {
			http.Error(w, "precondition failed", http.StatusPreconditionFailed)
			return
		}

		if _, err := h.store.Events.Upsert(r.Context(), store.Event{CalendarID: calendarID, UID: uid, RawICAL: string(body), ETag: etag}); err != nil {
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

	if addressBookID, _, matched := parseResourcePath(cleanPath, "/dav/addressbooks"); matched {
		if _, err := h.loadAddressBook(r.Context(), user, addressBookID); err != nil {
			status := http.StatusInternalServerError
			if err == store.ErrNotFound {
				status = http.StatusNotFound
			}
			http.Error(w, "address book not found", status)
			return
		}

		// Validate vCard data
		if err := h.validateVCard(string(body)); err != nil {
			http.Error(w, fmt.Sprintf("invalid vCard data: %v", err), http.StatusBadRequest)
			return
		}

		// Extract UID from vCard data, or use resource name if not present
		uid, err := extractUIDFromVCard(string(body))
		if err != nil {
			// UID is not strictly required in vCard 3.0, fall back to resource name
			_, resourceUID, matched := parseResourcePath(cleanPath, "/dav/addressbooks")
			if !matched || resourceUID == "" {
				http.Error(w, fmt.Sprintf("invalid vCard data: %v", err), http.StatusBadRequest)
				return
			}
			uid = resourceUID
		}

		existing, _ := h.store.Contacts.GetByUID(r.Context(), addressBookID, uid)

		// Check conditional request headers
		if !h.checkConditionalHeadersContact(r, existing) {
			http.Error(w, "precondition failed", http.StatusPreconditionFailed)
			return
		}

		if _, err := h.store.Contacts.Upsert(r.Context(), store.Contact{AddressBookID: addressBookID, UID: uid, RawVCard: string(body), ETag: etag}); err != nil {
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
	if calendarID, uid, matched := parseResourcePath(cleanPath, "/dav/calendars"); matched {
		cal, err := h.loadCalendar(r.Context(), user, calendarID)
		if err != nil {
			status := http.StatusInternalServerError
			if err == store.ErrNotFound {
				status = http.StatusNotFound
			}
			http.Error(w, "not found", status)
			return
		}
		if !cal.Editor {
			http.Error(w, "forbidden", http.StatusForbidden)
			return
		}
		// Check conditional headers before deletion
		existing, _ := h.store.Events.GetByUID(r.Context(), calendarID, uid)
		if !h.checkConditionalHeaders(r, existing) {
			http.Error(w, "precondition failed", http.StatusPreconditionFailed)
			return
		}
		if err := h.store.Events.DeleteByUID(r.Context(), calendarID, uid); err != nil {
			http.Error(w, "failed to delete", http.StatusInternalServerError)
			return
		}
		w.WriteHeader(http.StatusNoContent)
		return
	}
	if addressBookID, uid, matched := parseResourcePath(cleanPath, "/dav/addressbooks"); matched {
		if _, err := h.loadAddressBook(r.Context(), user, addressBookID); err != nil {
			status := http.StatusInternalServerError
			if err == store.ErrNotFound {
				status = http.StatusNotFound
			}
			http.Error(w, "not found", status)
			return
		}
		// Check conditional headers before deletion
		existing, _ := h.store.Contacts.GetByUID(r.Context(), addressBookID, uid)
		if !h.checkConditionalHeadersContact(r, existing) {
			http.Error(w, "precondition failed", http.StatusPreconditionFailed)
			return
		}
		if err := h.store.Contacts.DeleteByUID(r.Context(), addressBookID, uid); err != nil {
			http.Error(w, "failed to delete", http.StatusInternalServerError)
			return
		}
		w.WriteHeader(http.StatusNoContent)
		return
	}
	http.Error(w, "unsupported path", http.StatusBadRequest)
}

func (h *Handler) Report(w http.ResponseWriter, r *http.Request) {
	user, ok := auth.UserFromContext(r.Context())
	if !ok {
		http.Error(w, "missing user", http.StatusUnauthorized)
		return
	}

	cleanPath := path.Clean(r.URL.Path)
	w.Header().Set("Content-Type", "application/xml; charset=utf-8")
	if r.ContentLength > maxDAVBodyBytes {
		http.Error(w, "request too large", http.StatusRequestEntityTooLarge)
		return
	}
	limitedBody := http.MaxBytesReader(w, r.Body, maxDAVBodyBytes)
	body, err := io.ReadAll(limitedBody)
	if err != nil {
		var maxErr *http.MaxBytesError
		if errors.As(err, &maxErr) {
			http.Error(w, "request too large", http.StatusRequestEntityTooLarge)
		} else {
			http.Error(w, "failed to read body", http.StatusBadRequest)
		}
		return
	}
	var report reportRequest
	if err := xml.Unmarshal(body, &report); err != nil {
		http.Error(w, "invalid REPORT body", http.StatusBadRequest)
		return
	}

	if strings.HasPrefix(cleanPath, "/dav/calendars/") {
		parts := strings.Split(strings.TrimPrefix(cleanPath, "/dav/calendars"), "/")
		if len(parts) < 2 || strings.TrimSpace(parts[1]) == "" {
			http.Error(w, "invalid calendar path", http.StatusBadRequest)
			return
		}
		calID, err := strconv.ParseInt(parts[1], 10, 64)
		if err != nil {
			http.Error(w, "invalid calendar id", http.StatusBadRequest)
			return
		}
		cal, err := h.loadCalendar(r.Context(), user, calID)
		if err != nil {
			status := http.StatusInternalServerError
			if err == store.ErrNotFound {
				status = http.StatusNotFound
			}
			http.Error(w, "calendar not found", status)
			return
		}
		responses, syncToken, err := h.calendarReportResponses(r.Context(), cal, h.principalURL(user), cleanPath, report)
		if err != nil {
			if errors.Is(err, errInvalidSyncToken) {
				http.Error(w, "invalid sync token", http.StatusForbidden)
			} else {
				http.Error(w, err.Error(), http.StatusInternalServerError)
			}
			return
		}

		payload := multistatus{
			XMLName:   xml.Name{Space: "DAV:", Local: "multistatus"},
			XmlnsD:    "DAV:",
			XmlnsC:    "urn:ietf:params:xml:ns:caldav",
			XmlnsA:    "urn:ietf:params:xml:ns:carddav",
			XmlnsCS:   "http://calendarserver.org/ns/",
			SyncToken: syncToken,
			Response:  responses,
		}
		w.WriteHeader(http.StatusMultiStatus)
		_ = xml.NewEncoder(w).Encode(payload)
		return
	}

	if strings.HasPrefix(cleanPath, "/dav/addressbooks/") {
		parts := strings.Split(strings.TrimPrefix(cleanPath, "/dav/addressbooks"), "/")
		if len(parts) < 2 || parts[1] == "" {
			http.Error(w, "invalid address book path", http.StatusBadRequest)
			return
		}
		bookID, err := strconv.ParseInt(parts[1], 10, 64)
		if err != nil {
			http.Error(w, "invalid address book id", http.StatusBadRequest)
			return
		}
		book, err := h.loadAddressBook(r.Context(), user, bookID)
		if err != nil {
			status := http.StatusInternalServerError
			if err == store.ErrNotFound {
				status = http.StatusNotFound
			}
			http.Error(w, "address book not found", status)
			return
		}
		responses, syncToken, err := h.addressBookReportResponses(r.Context(), book, h.principalURL(user), cleanPath, report)
		if err != nil {
			if errors.Is(err, errInvalidSyncToken) {
				http.Error(w, "invalid sync token", http.StatusForbidden)
			} else {
				http.Error(w, err.Error(), http.StatusInternalServerError)
			}
			return
		}

		payload := multistatus{
			XMLName:   xml.Name{Space: "DAV:", Local: "multistatus"},
			XmlnsD:    "DAV:",
			XmlnsC:    "urn:ietf:params:xml:ns:caldav",
			XmlnsA:    "urn:ietf:params:xml:ns:carddav",
			XmlnsCS:   "http://calendarserver.org/ns/",
			SyncToken: syncToken,
			Response:  responses,
		}
		w.WriteHeader(http.StatusMultiStatus)
		_ = xml.NewEncoder(w).Encode(payload)
		return
	}

	http.Error(w, "REPORT not supported for path", http.StatusBadRequest)
}

// buildPropfindResponses constructs DAV multistatus responses for a path.
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
		return res, nil
	case strings.HasPrefix(cleanPath, "/dav/principals"):
		return h.principalResponses(cleanPath, depth, user, ensureCollectionHref)
	case strings.HasPrefix(cleanPath, "/dav/calendars"):
		return h.calendarResponses(ctx, cleanPath, depth, user, ensureCollectionHref)
	case strings.HasPrefix(cleanPath, "/dav/addressbooks"):
		return h.addressBookResponses(ctx, cleanPath, depth, user, ensureCollectionHref)
	default:
		return nil, http.ErrNotSupported
	}
}

func (h *Handler) calendarResponses(ctx context.Context, cleanPath, depth string, user *store.User, ensureCollectionHref func(string) string) ([]response, error) {
	relPath := strings.Trim(strings.TrimPrefix(cleanPath, "/dav/calendars"), "/")
	if relPath == "" {
		base := ensureCollectionHref("/dav/calendars")
		res := []response{collectionResponse(base, "Calendars")}
		if depth == "1" {
			cals, err := h.store.Calendars.ListAccessible(ctx, user.ID)
			if err != nil {
				return nil, err
			}
			principalHref := h.principalURL(user)
			for _, c := range cals {
				href := ensureCollectionHref(path.Join("/dav/calendars", fmt.Sprint(c.ID)))
				ctag := fmt.Sprintf("%d", c.CTag)
				syncToken := buildSyncToken("cal", c.ID, c.UpdatedAt)
				res = append(res, calendarCollectionResponse(href, c.Name, c.Description, c.Timezone, principalHref, syncToken, ctag))
			}
		}
		return res, nil
	}

	segments := strings.Split(relPath, "/")
	calID, err := strconv.ParseInt(segments[0], 10, 64)
	if err != nil {
		return nil, http.ErrNotSupported
	}
	cal, err := h.loadCalendar(ctx, user, calID)
	if err != nil {
		return nil, err
	}
	href := ensureCollectionHref(path.Join("/dav/calendars", fmt.Sprint(cal.ID)))
	ctag := fmt.Sprintf("%d", cal.CTag)
	syncToken := buildSyncToken("cal", cal.ID, cal.UpdatedAt)
	principalHref := h.principalURL(user)
	res := []response{calendarCollectionResponse(href, cal.Name, cal.Description, cal.Timezone, principalHref, syncToken, ctag)}
	if depth == "1" {
		events, err := h.store.Events.ListForCalendar(ctx, cal.ID)
		if err != nil {
			return nil, err
		}
		base := ensureCollectionHref(href)
		res = append(res, calendarResourceResponses(base, events)...)
	}
	return res, nil
}

func (h *Handler) addressBookResponses(ctx context.Context, cleanPath, depth string, user *store.User, ensureCollectionHref func(string) string) ([]response, error) {
	relPath := strings.Trim(strings.TrimPrefix(cleanPath, "/dav/addressbooks"), "/")
	if relPath == "" {
		base := ensureCollectionHref("/dav/addressbooks")
		res := []response{collectionResponse(base, "Address Books")}
		if depth == "1" {
			books, err := h.store.AddressBooks.ListByUser(ctx, user.ID)
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
	bookID, err := strconv.ParseInt(segments[0], 10, 64)
	if err != nil {
		return nil, http.ErrNotSupported
	}
	book, err := h.loadAddressBook(ctx, user, bookID)
	if err != nil {
		return nil, err
	}
	href := ensureCollectionHref(path.Join("/dav/addressbooks", fmt.Sprint(book.ID)))
	ctag := fmt.Sprintf("%d", book.CTag)
	syncToken := buildSyncToken("card", book.ID, book.UpdatedAt)
	principalHref := h.principalURL(user)
	res := []response{addressBookCollectionResponse(href, book.Name, book.Description, principalHref, syncToken, ctag)}
	if depth == "1" {
		contacts, err := h.store.Contacts.ListForBook(ctx, book.ID)
		if err != nil {
			return nil, err
		}
		base := ensureCollectionHref(href)
		res = append(res, addressBookResourceResponses(base, contacts)...)
	}
	return res, nil
}

func (h *Handler) loadCalendar(ctx context.Context, user *store.User, id int64) (*store.CalendarAccess, error) {
	cal, err := h.store.Calendars.GetAccessible(ctx, id, user.ID)
	if err != nil {
		return nil, err
	}
	if cal == nil {
		return nil, store.ErrNotFound
	}
	return cal, nil
}

func (h *Handler) loadAddressBook(ctx context.Context, user *store.User, id int64) (*store.AddressBook, error) {
	book, err := h.store.AddressBooks.GetByID(ctx, id)
	if err != nil {
		return nil, err
	}
	if book == nil || book.UserID != user.ID {
		return nil, store.ErrNotFound
	}
	return book, nil
}

// parseResourcePath extracts the numeric collection ID and resource UID from a DAV resource path.
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
		base += "/"
	}
	return normalizeDAVHref(path.Join(base, trimmed))
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

type multistatus struct {
	XMLName   xml.Name   `xml:"d:multistatus"`
	XmlnsD    string     `xml:"xmlns:d,attr"`
	XmlnsC    string     `xml:"xmlns:cal,attr"`
	XmlnsA    string     `xml:"xmlns:card,attr"`
	XmlnsCS   string     `xml:"xmlns:cs,attr,omitempty"`
	SyncToken string     `xml:"d:sync-token,omitempty"`
	Response  []response `xml:"d:response"`
}

type response struct {
	Href     string     `xml:"d:href"`
	Propstat []propstat `xml:"d:propstat,omitempty"`
	Status   string     `xml:"d:status,omitempty"`
}

type propstat struct {
	Prop   prop   `xml:"d:prop"`
	Status string `xml:"d:status"`
}

type prop struct {
	DisplayName                   string                         `xml:"d:displayname,omitempty"`
	ResourceType                  resourceType                   `xml:"d:resourcetype"`
	GetETag                       string                         `xml:"d:getetag,omitempty"`
	GetContentType                string                         `xml:"d:getcontenttype,omitempty"`
	CalendarData                  cdataString                    `xml:"cal:calendar-data,omitempty"`
	AddressData                   cdataString                    `xml:"card:address-data,omitempty"`
	CalendarDescription           string                         `xml:"cal:calendar-description,omitempty"`
	CalendarTimezone              *string                        `xml:"cal:calendar-timezone,omitempty"`
	AddressBookDesc               string                         `xml:"card:addressbook-description,omitempty"`
	SyncToken                     string                         `xml:"d:sync-token,omitempty"`
	CTag                          string                         `xml:"cs:getctag,omitempty"`
	CurrentUserPrincipal          *hrefProp                      `xml:"d:current-user-principal,omitempty"`
	PrincipalURL                  *hrefProp                      `xml:"d:principal-URL,omitempty"`
	CalendarHomeSet               *hrefListProp                  `xml:"cal:calendar-home-set,omitempty"`
	AddressbookHomeSet            *hrefListProp                  `xml:"card:addressbook-home-set,omitempty"`
	SupportedReportSet            *supportedReportSet            `xml:"d:supported-report-set,omitempty"`
	SupportedCalendarComponentSet *supportedCalendarComponentSet `xml:"cal:supported-calendar-component-set,omitempty"`
	MaxResourceSize               string                         `xml:"cal:max-resource-size,omitempty"`
	MinDateTime                   string                         `xml:"cal:min-date-time,omitempty"`
	MaxDateTime                   string                         `xml:"cal:max-date-time,omitempty"`
	MaxInstances                  string                         `xml:"cal:max-instances,omitempty"`
	MaxAttendeesPerInstance       string                         `xml:"cal:max-attendees-per-instance,omitempty"`
}

// cdataString wraps string content in CDATA for raw XML output.
// This preserves special characters like CRLF in iCalendar/vCard data.
type cdataString string

func (c cdataString) MarshalXML(e *xml.Encoder, start xml.StartElement) error {
	if c == "" {
		return nil
	}
	return e.EncodeElement(struct {
		S string `xml:",cdata"`
	}{S: string(c)}, start)
}

type resourceType struct {
	Collection  *struct{} `xml:"d:collection,omitempty"`
	Calendar    *struct{} `xml:"cal:calendar,omitempty"`
	AddressBook *struct{} `xml:"card:addressbook,omitempty"`
	Principal   *struct{} `xml:"d:principal,omitempty"`
}

type reportRequest struct {
	XMLName      xml.Name
	Hrefs        []string        `xml:"DAV: href"`
	SyncToken    string          `xml:"DAV: sync-token"`
	Filter       *calFilter      `xml:"urn:ietf:params:xml:ns:caldav filter"`
	CalendarData *calendarDataEl `xml:"urn:ietf:params:xml:ns:caldav calendar-data"`
	Prop         *reportProp     `xml:"DAV: prop"`
}

// reportProp captures the prop element in reports for partial retrieval
type reportProp struct {
	CalendarData *calendarDataEl `xml:"urn:ietf:params:xml:ns:caldav calendar-data"`
}

// calendarDataEl specifies what calendar data to return (RFC 4791 Section 9.6)
type calendarDataEl struct {
	Expand *expandEl `xml:"urn:ietf:params:xml:ns:caldav expand"`
}

// expandEl specifies recurrence expansion parameters
type expandEl struct {
	Start string `xml:"start,attr"`
	End   string `xml:"end,attr"`
}

// propfindRequest represents a PROPFIND request body (RFC 4918 Section 9.1)
type propfindRequest struct {
	XMLName  xml.Name
	AllProp  *struct{}          `xml:"DAV: allprop"`
	PropName *struct{}          `xml:"DAV: propname"`
	Prop     *propfindPropQuery `xml:"DAV: prop"`
}

// propfindPropQuery lists specific properties requested
type propfindPropQuery struct {
	DisplayName                   *struct{} `xml:"DAV: displayname"`
	ResourceType                  *struct{} `xml:"DAV: resourcetype"`
	GetETag                       *struct{} `xml:"DAV: getetag"`
	GetContentType                *struct{} `xml:"DAV: getcontenttype"`
	CalendarData                  *struct{} `xml:"urn:ietf:params:xml:ns:caldav calendar-data"`
	AddressData                   *struct{} `xml:"urn:ietf:params:xml:ns:carddav address-data"`
	CalendarDescription           *struct{} `xml:"urn:ietf:params:xml:ns:caldav calendar-description"`
	CalendarTimezone              *struct{} `xml:"urn:ietf:params:xml:ns:caldav calendar-timezone"`
	AddressBookDesc               *struct{} `xml:"urn:ietf:params:xml:ns:carddav addressbook-description"`
	SyncToken                     *struct{} `xml:"DAV: sync-token"`
	CTag                          *struct{} `xml:"http://calendarserver.org/ns/ getctag"`
	CurrentUserPrincipal          *struct{} `xml:"DAV: current-user-principal"`
	PrincipalURL                  *struct{} `xml:"DAV: principal-URL"`
	CalendarHomeSet               *struct{} `xml:"urn:ietf:params:xml:ns:caldav calendar-home-set"`
	AddressbookHomeSet            *struct{} `xml:"urn:ietf:params:xml:ns:carddav addressbook-home-set"`
	SupportedReportSet            *struct{} `xml:"DAV: supported-report-set"`
	SupportedCalendarComponentSet *struct{} `xml:"urn:ietf:params:xml:ns:caldav supported-calendar-component-set"`
}

// calFilter represents a CalDAV calendar-query filter (RFC 4791 Section 9.7)
type calFilter struct {
	CompFilter compFilter `xml:"urn:ietf:params:xml:ns:caldav comp-filter"`
}

// compFilter filters by component type and optionally by time-range
type compFilter struct {
	Name       string       `xml:"name,attr"`
	TimeRange  *timeRange   `xml:"urn:ietf:params:xml:ns:caldav time-range"`
	CompFilter []compFilter `xml:"urn:ietf:params:xml:ns:caldav comp-filter"`
	PropFilter []propFilter `xml:"urn:ietf:params:xml:ns:caldav prop-filter"`
	TextMatch  *textMatch   `xml:"urn:ietf:params:xml:ns:caldav text-match"`
}

// propFilter filters by property presence and optionally by text-match
type propFilter struct {
	Name         string     `xml:"name,attr"`
	IsNotDefined *struct{}  `xml:"urn:ietf:params:xml:ns:caldav is-not-defined"`
	TextMatch    *textMatch `xml:"urn:ietf:params:xml:ns:caldav text-match"`
}

// textMatch filters by text content
type textMatch struct {
	Text            string `xml:",chardata"`
	Collation       string `xml:"collation,attr,omitempty"`
	NegateCondition string `xml:"negate-condition,attr,omitempty"`
}

// timeRange filters events within a time window (RFC 4791 Section 9.9)
type timeRange struct {
	Start string `xml:"start,attr"`
	End   string `xml:"end,attr"`
}

func collectionResponse(href, name string) response {
	return response{
		Href:     href,
		Propstat: []propstat{statusOKProp(name, resourceType{Collection: &struct{}{}})},
	}
}

func calendarCollectionResponse(href, name string, description, timezone *string, principalHref, syncToken, ctag string) response {
	resp := response{
		Href:     href,
		Propstat: []propstat{statusOKPropWithExtras(name, resourceType{Collection: &struct{}{}, Calendar: &struct{}{}}, principalHref, true, false)},
	}
	if syncToken != "" {
		resp.Propstat[0].Prop.SyncToken = syncToken
	}
	if ctag != "" {
		resp.Propstat[0].Prop.CTag = ctag
	}
	if description != nil && *description != "" {
		resp.Propstat[0].Prop.CalendarDescription = *description
	}
	// Always include calendar-timezone property (RFC 4791 Section 5.2.2)
	// Even if empty, clients expect this property to be present
	if timezone != nil {
		resp.Propstat[0].Prop.CalendarTimezone = timezone
	} else {
		emptyTZ := ""
		resp.Propstat[0].Prop.CalendarTimezone = &emptyTZ
	}
	// Add supported calendar component set (RFC 4791 Section 5.2.3)
	resp.Propstat[0].Prop.SupportedCalendarComponentSet = supportedCalendarComponents()

	// Add calendar limits (RFC 4791 Section 5.2.5-5.2.9)
	resp.Propstat[0].Prop.MaxResourceSize = "10485760"     // 10MB limit
	resp.Propstat[0].Prop.MinDateTime = "19000101T000000Z" // Year 1900
	resp.Propstat[0].Prop.MaxDateTime = "21001231T235959Z" // Year 2100
	resp.Propstat[0].Prop.MaxInstances = "1000"            // Max recurring instances to expand
	resp.Propstat[0].Prop.MaxAttendeesPerInstance = "100"  // Max attendees per event

	return resp
}

func addressBookCollectionResponse(href, name string, description *string, principalHref, syncToken, ctag string) response {
	resp := response{
		Href:     href,
		Propstat: []propstat{statusOKPropWithExtras(name, resourceType{Collection: &struct{}{}, AddressBook: &struct{}{}}, principalHref, false, true)},
	}
	if syncToken != "" {
		resp.Propstat[0].Prop.SyncToken = syncToken
	}
	if ctag != "" {
		resp.Propstat[0].Prop.CTag = ctag
	}
	if description != nil && *description != "" {
		resp.Propstat[0].Prop.AddressBookDesc = *description
	}
	return resp
}

func statusOKProp(name string, rtype resourceType) propstat {
	return propstat{
		Prop: prop{
			DisplayName:  name,
			ResourceType: rtype,
		},
		Status: "HTTP/1.1 200 OK",
	}
}

func statusOKPropWithExtras(name string, rtype resourceType, principalHref string, includeCalendarHome, includeAddressHome bool) propstat {
	p := prop{
		DisplayName:          name,
		ResourceType:         rtype,
		CurrentUserPrincipal: &hrefProp{Href: principalHref},
	}
	if includeCalendarHome {
		p.CalendarHomeSet = &hrefListProp{Href: []string{"/dav/calendars/"}}
		p.SupportedReportSet = calendarSupportedReports()
	}
	if includeAddressHome {
		p.AddressbookHomeSet = &hrefListProp{Href: []string{"/dav/addressbooks/"}}
		p.SupportedReportSet = addressbookSupportedReports()
	}
	if !includeCalendarHome && !includeAddressHome {
		p.SupportedReportSet = combinedSupportedReports()
	}
	return propstat{Prop: p, Status: "HTTP/1.1 200 OK"}
}

func etagProp(etag, data string, calendar bool) propstat {
	return etagPropWithData(etag, data, calendar, true)
}

// etagPropWithData allows control over whether to include the full data
func etagPropWithData(etag, data string, calendar bool, includeData bool) propstat {
	propVal := prop{GetETag: fmt.Sprintf("\"%s\"", etag)}
	if includeData {
		if calendar {
			propVal.CalendarData = cdataString(data)
			propVal.GetContentType = "text/calendar; charset=utf-8"
		} else {
			propVal.AddressData = cdataString(data)
			propVal.GetContentType = "text/vcard; charset=utf-8"
		}
	}
	return propstat{Prop: propVal, Status: "HTTP/1.1 200 OK"}
}

func resourceResponse(href string, ps propstat) response {
	return response{Href: href, Propstat: []propstat{ps}}
}

// deletedResponse returns a response indicating the resource was deleted (for sync-collection).
func deletedResponse(href string) response {
	return response{Href: href, Status: "HTTP/1.1 404 Not Found"}
}

type hrefProp struct {
	Href string `xml:"d:href"`
}

type hrefListProp struct {
	Href []string `xml:"d:href"`
}

type supportedReportSet struct {
	Reports []supportedReport `xml:"d:supported-report"`
}

type supportedReport struct {
	Report reportType `xml:"d:report"`
}

type reportType struct {
	CalendarMultiGet    *struct{} `xml:"cal:calendar-multiget,omitempty"`
	CalendarQuery       *struct{} `xml:"cal:calendar-query,omitempty"`
	FreeBusyQuery       *struct{} `xml:"cal:free-busy-query,omitempty"`
	AddressbookMultiGet *struct{} `xml:"card:addressbook-multiget,omitempty"`
	AddressbookQuery    *struct{} `xml:"card:addressbook-query,omitempty"`
	SyncCollection      *struct{} `xml:"d:sync-collection,omitempty"`
}

type supportedCalendarComponentSet struct {
	Comps []comp `xml:"cal:comp"`
}

type comp struct {
	Name string `xml:"name,attr"`
}

func calendarSupportedReports() *supportedReportSet {
	return &supportedReportSet{
		Reports: []supportedReport{
			{Report: reportType{CalendarMultiGet: &struct{}{}}},
			{Report: reportType{CalendarQuery: &struct{}{}}},
			{Report: reportType{FreeBusyQuery: &struct{}{}}},
			{Report: reportType{SyncCollection: &struct{}{}}},
		},
	}
}

func addressbookSupportedReports() *supportedReportSet {
	return &supportedReportSet{
		Reports: []supportedReport{
			{Report: reportType{AddressbookMultiGet: &struct{}{}}},
			{Report: reportType{AddressbookQuery: &struct{}{}}},
			{Report: reportType{SyncCollection: &struct{}{}}},
		},
	}
}

func combinedSupportedReports() *supportedReportSet {
	return &supportedReportSet{
		Reports: []supportedReport{
			{Report: reportType{CalendarMultiGet: &struct{}{}}},
			{Report: reportType{CalendarQuery: &struct{}{}}},
			{Report: reportType{AddressbookMultiGet: &struct{}{}}},
			{Report: reportType{AddressbookQuery: &struct{}{}}},
			{Report: reportType{SyncCollection: &struct{}{}}},
		},
	}
}

func supportedCalendarComponents() *supportedCalendarComponentSet {
	return &supportedCalendarComponentSet{
		Comps: []comp{
			{Name: "VEVENT"},
			{Name: "VTODO"},
			{Name: "VJOURNAL"},
		},
	}
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
		DisplayName:          user.PrimaryEmail,
		ResourceType:         resourceType{Principal: &struct{}{}},
		PrincipalURL:         &hrefProp{Href: href},
		CurrentUserPrincipal: &hrefProp{Href: href},
		CalendarHomeSet:      &hrefListProp{Href: []string{"/dav/calendars/"}},
		AddressbookHomeSet:   &hrefListProp{Href: []string{"/dav/addressbooks/"}},
		SupportedReportSet:   combinedSupportedReports(),
	}
	return response{Href: href, Propstat: []propstat{{Prop: p, Status: "HTTP/1.1 200 OK"}}}
}

func rootCollectionResponse(href string, user *store.User, principalHref string) response {
	p := prop{
		DisplayName:          "CalCard DAV",
		ResourceType:         resourceType{Collection: &struct{}{}},
		CurrentUserPrincipal: &hrefProp{Href: principalHref},
		SupportedReportSet:   combinedSupportedReports(),
	}
	return response{Href: href, Propstat: []propstat{{Prop: p, Status: "HTTP/1.1 200 OK"}}}
}

func (h *Handler) calendarReportResponses(ctx context.Context, cal *store.CalendarAccess, principalHref, cleanPath string, report reportRequest) ([]response, string, error) {
	switch report.XMLName.Local {
	case "calendar-multiget":
		res, err := h.calendarMultiGet(ctx, cal.ID, report.Hrefs, cleanPath)
		return res, "", err
	case "calendar-query":
		res, err := h.calendarQuery(ctx, cal.ID, cleanPath, report.Filter)
		return res, "", err
	case "free-busy-query":
		res, err := h.freeBusyQuery(ctx, cal.ID, cleanPath, report.Filter)
		return res, "", err
	case "sync-collection":
		return h.calendarSyncCollection(ctx, cal, principalHref, cleanPath, report)
	default:
		// Fallback: return all events to keep clients moving even if they send unsupported report types.
		res, err := h.calendarQuery(ctx, cal.ID, cleanPath, nil)
		return res, "", err
	}
}

func (h *Handler) addressBookReportResponses(ctx context.Context, book *store.AddressBook, principalHref, cleanPath string, report reportRequest) ([]response, string, error) {
	switch report.XMLName.Local {
	case "addressbook-multiget":
		res, err := h.addressBookMultiGet(ctx, book.ID, report.Hrefs, cleanPath)
		return res, "", err
	case "addressbook-query":
		res, err := h.addressBookQuery(ctx, book.ID, cleanPath)
		return res, "", err
	case "sync-collection":
		return h.addressBookSyncCollection(ctx, book, principalHref, cleanPath, report)
	default:
		res, err := h.addressBookQuery(ctx, book.ID, cleanPath)
		return res, "", err
	}
}

// applyCalendarFilter filters events based on comp-filter and time-range
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

// eventMatchesFilter checks if an event matches the calendar filter criteria
func (h *Handler) eventMatchesFilter(event store.Event, filter *calFilter) bool {
	return h.matchesCompFilter(event, &filter.CompFilter)
}

// matchesCompFilter recursively checks if an event matches a component filter
func (h *Handler) matchesCompFilter(event store.Event, compFilter *compFilter) bool {
	// Check component type filter
	compType := compFilter.Name
	if compType != "" && !h.hasComponent(event.RawICAL, compType) {
		return false
	}

	// Check time-range filter if present at this level
	if compFilter.TimeRange != nil {
		if !h.eventInTimeRange(event, compFilter.TimeRange) {
			return false
		}
	}

	// Check nested component filters
	for _, nestedFilter := range compFilter.CompFilter {
		if !h.matchesCompFilter(event, &nestedFilter) {
			return false
		}
	}

	// Check property filters
	for _, propFilter := range compFilter.PropFilter {
		if !h.matchesPropFilter(event, &propFilter) {
			return false
		}
	}

	// Check text match at this level
	if compFilter.TextMatch != nil {
		if !h.matchesTextMatch(event.RawICAL, compFilter.TextMatch) {
			return false
		}
	}

	return true
}

// matchesPropFilter checks if an event matches a property filter
func (h *Handler) matchesPropFilter(event store.Event, propFilter *propFilter) bool {
	propName := strings.ToUpper(propFilter.Name)
	hasProp := strings.Contains(strings.ToUpper(event.RawICAL), propName+":")

	// If is-not-defined is set, event should NOT have the property
	if propFilter.IsNotDefined != nil {
		return !hasProp
	}

	// Otherwise, event should have the property
	if !hasProp {
		return false
	}

	// If there's a text-match, check it
	if propFilter.TextMatch != nil {
		return h.matchesTextMatch(event.RawICAL, propFilter.TextMatch)
	}

	return true
}

// matchesTextMatch checks if text content matches the filter
func (h *Handler) matchesTextMatch(icalData string, textMatch *textMatch) bool {
	text := strings.TrimSpace(textMatch.Text)
	if text == "" {
		return true
	}

	// Case-insensitive contains check (simplified - RFC 4790 has more complex rules)
	matches := strings.Contains(strings.ToUpper(icalData), strings.ToUpper(text))

	// Check negate-condition attribute
	if textMatch.NegateCondition == "yes" {
		return !matches
	}

	return matches
}

// hasComponent checks if the iCalendar data contains a component of the specified type
func (h *Handler) hasComponent(icalData, componentType string) bool {
	componentType = strings.ToUpper(componentType)
	beginMarker := "BEGIN:" + componentType
	return strings.Contains(strings.ToUpper(icalData), beginMarker)
}

// eventInTimeRange checks if an event overlaps with the specified time range
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

	// Check if event has recurrence rule
	if strings.Contains(strings.ToUpper(event.RawICAL), "RRULE:") {
		return h.recurringEventInTimeRange(event, start, end)
	}

	// Non-recurring event: check if it overlaps with query range
	if event.DTStart != nil {
		eventEnd := event.DTEnd
		if eventEnd == nil {
			// If no end time, use start time
			eventEnd = event.DTStart
		}

		// Event overlaps if: event.start < range.end AND event.end > range.start
		return event.DTStart.Before(end) && eventEnd.After(start)
	}

	// If no parsed dates, include the event (can't filter reliably)
	return true
}

// recurringEventInTimeRange checks if a recurring event has any instances in the time range
func (h *Handler) recurringEventInTimeRange(event store.Event, rangeStart, rangeEnd time.Time) bool {
	if event.DTStart == nil {
		return true // Can't determine without start time
	}

	// Extract RRULE from iCalendar data
	rrule := extractRRule(event.RawICAL)
	if rrule == "" {
		return true // Malformed, be permissive
	}

	// Parse RRULE parameters
	freq := extractRRuleParam(rrule, "FREQ")
	countStr := extractRRuleParam(rrule, "COUNT")
	untilStr := extractRRuleParam(rrule, "UNTIL")
	intervalStr := extractRRuleParam(rrule, "INTERVAL")

	// Determine recurrence interval
	interval := 1
	if intervalStr != "" {
		if i, err := strconv.Atoi(intervalStr); err == nil && i > 0 {
			interval = i
		}
	}

	// Determine maximum occurrences to check
	maxOccurrences := 500 // Default limit to prevent infinite loops
	if countStr != "" {
		if c, err := strconv.Atoi(countStr); err == nil && c > 0 {
			maxOccurrences = c
		}
	}

	// Determine recurrence end date
	recurrenceEnd := rangeEnd.AddDate(0, 0, 1) // Default to just past query range
	if untilStr != "" {
		if until, err := parseICalDateTime(untilStr); err == nil {
			recurrenceEnd = until
		}
	}

	// Calculate event duration
	eventDuration := time.Hour // Default 1 hour
	if event.DTEnd != nil {
		eventDuration = event.DTEnd.Sub(*event.DTStart)
	}

	// Generate instances and check if any fall in range
	current := *event.DTStart
	for i := 0; i < maxOccurrences; i++ {
		// Stop if we've passed the recurrence end
		if current.After(recurrenceEnd) {
			break
		}

		// Stop if we've gone well past the query range (optimization)
		if current.After(rangeEnd.AddDate(0, 0, 7)) {
			break
		}

		// Check if this instance overlaps with query range
		instanceEnd := current.Add(eventDuration)
		if current.Before(rangeEnd) && instanceEnd.After(rangeStart) {
			return true
		}

		// Calculate next occurrence based on frequency
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

		// Safety check: if we've checked 1000+ days into the future, stop
		if current.After(event.DTStart.AddDate(3, 0, 0)) && i > 100 {
			break
		}
	}

	return false
}

// extractRRule extracts the RRULE value from iCalendar data
func extractRRule(icalData string) string {
	lines := strings.Split(icalData, "\n")
	for _, line := range lines {
		line = strings.TrimSpace(line)
		line = strings.TrimSuffix(line, "\r")
		upperLine := strings.ToUpper(line)
		if strings.HasPrefix(upperLine, "RRULE:") {
			return strings.TrimPrefix(line, "RRULE:")
		}
	}
	return ""
}

// extractRRuleParam extracts a parameter value from an RRULE string
// Example: "FREQ=WEEKLY;BYDAY=MO,WE,FR" -> extractRRuleParam(rrule, "FREQ") returns "WEEKLY"
func extractRRuleParam(rrule, param string) string {
	parts := strings.Split(rrule, ";")
	paramUpper := strings.ToUpper(param) + "="
	for _, part := range parts {
		if strings.HasPrefix(strings.ToUpper(part), paramUpper) {
			return strings.TrimPrefix(part, param+"=")
		}
	}
	return ""
}

// parseICalDateTime parses iCalendar datetime format (RFC 5545)
func parseICalDateTime(s string) (time.Time, error) {
	if s == "" {
		return time.Time{}, fmt.Errorf("empty datetime")
	}

	// Remove timezone suffix if present (Z for UTC)
	s = strings.TrimSuffix(s, "Z")

	// Try various iCalendar formats
	formats := []string{
		"20060102T150405",      // Basic format
		"20060102T150405Z",     // UTC format
		"2006-01-02T15:04:05",  // Extended format
		"2006-01-02T15:04:05Z", // Extended UTC
	}

	for _, format := range formats {
		if t, err := time.Parse(format, s); err == nil {
			return t.UTC(), nil
		}
	}

	return time.Time{}, fmt.Errorf("invalid datetime format: %s", s)
}

// freeBusyQuery generates a VFREEBUSY response (RFC 4791 Section 7.10)
func (h *Handler) freeBusyQuery(ctx context.Context, calID int64, cleanPath string, filter *calFilter) ([]response, error) {
	events, err := h.store.Events.ListForCalendar(ctx, calID)
	if err != nil {
		return nil, fmt.Errorf("failed to list events")
	}

	// Apply time-range filter if present
	if filter != nil {
		events = h.applyCalendarFilter(events, filter)
	}

	// Generate VFREEBUSY component
	freeBusyData := h.generateFreeBusy(events, filter)

	// Return as a single resource response
	href := strings.TrimSuffix(cleanPath, "/") + "/freebusy.ics"
	etag := fmt.Sprintf("%x", sha256.Sum256([]byte(freeBusyData)))

	return []response{
		resourceResponse(href, etagProp(etag, freeBusyData, true)),
	}, nil
}

// generateFreeBusy creates a VFREEBUSY component from events
func (h *Handler) generateFreeBusy(events []store.Event, filter *calFilter) string {
	var sb strings.Builder
	sb.WriteString("BEGIN:VCALENDAR\r\n")
	sb.WriteString("VERSION:2.0\r\n")
	sb.WriteString("PRODID:-//CalCard//CalDAV Server//EN\r\n")
	sb.WriteString("BEGIN:VFREEBUSY\r\n")
	sb.WriteString(fmt.Sprintf("DTSTAMP:%s\r\n", time.Now().UTC().Format("20060102T150405Z")))

	// Add time range if specified in filter
	if filter != nil && filter.CompFilter.TimeRange != nil {
		if filter.CompFilter.TimeRange.Start != "" {
			sb.WriteString(fmt.Sprintf("DTSTART:%s\r\n", filter.CompFilter.TimeRange.Start))
		}
		if filter.CompFilter.TimeRange.End != "" {
			sb.WriteString(fmt.Sprintf("DTEND:%s\r\n", filter.CompFilter.TimeRange.End))
		}
	}

	// Add FREEBUSY periods for each event
	for _, event := range events {
		if event.DTStart != nil {
			endTime := event.DTEnd
			if endTime == nil {
				endTime = event.DTStart
			}

			// Format as FREEBUSY property (start/end)
			startStr := event.DTStart.UTC().Format("20060102T150405Z")
			endStr := endTime.UTC().Format("20060102T150405Z")
			sb.WriteString(fmt.Sprintf("FREEBUSY:%s/%s\r\n", startStr, endStr))
		}
	}

	sb.WriteString("END:VFREEBUSY\r\n")
	sb.WriteString("END:VCALENDAR\r\n")

	return sb.String()
}

func (h *Handler) calendarQuery(ctx context.Context, calID int64, cleanPath string, filter *calFilter) ([]response, error) {
	events, err := h.store.Events.ListForCalendar(ctx, calID)
	if err != nil {
		return nil, fmt.Errorf("failed to list events")
	}

	// Apply filters if provided
	if filter != nil {
		events = h.applyCalendarFilter(events, filter)
	}

	return calendarResourceResponses(cleanPath, events), nil
}

func (h *Handler) calendarMultiGet(ctx context.Context, calID int64, hrefs []string, cleanPath string) ([]response, error) {
	if len(hrefs) == 0 {
		return h.calendarQuery(ctx, calID, cleanPath, nil)
	}
	var responses []response
	for _, href := range hrefs {
		cleanHref := resolveDAVHref(cleanPath, href)
		if cleanHref == "" {
			continue
		}
		id, uid, ok := parseResourcePath(cleanHref, "/dav/calendars")
		if !ok || id != calID {
			continue
		}
		ev, err := h.store.Events.GetByUID(ctx, calID, uid)
		if err != nil {
			return nil, fmt.Errorf("failed to fetch event")
		}
		if ev == nil {
			continue
		}
		responses = append(responses, resourceResponse(cleanHref, etagProp(ev.ETag, ev.RawICAL, true)))
	}
	return responses, nil
}

func (h *Handler) addressBookQuery(ctx context.Context, bookID int64, cleanPath string) ([]response, error) {
	contacts, err := h.store.Contacts.ListForBook(ctx, bookID)
	if err != nil {
		return nil, fmt.Errorf("failed to list contacts")
	}
	return addressBookResourceResponses(cleanPath, contacts), nil
}

func (h *Handler) addressBookMultiGet(ctx context.Context, bookID int64, hrefs []string, cleanPath string) ([]response, error) {
	if len(hrefs) == 0 {
		return h.addressBookQuery(ctx, bookID, cleanPath)
	}
	var responses []response
	for _, href := range hrefs {
		cleanHref := resolveDAVHref(cleanPath, href)
		if cleanHref == "" {
			continue
		}
		id, uid, ok := parseResourcePath(cleanHref, "/dav/addressbooks")
		if !ok || id != bookID {
			continue
		}
		c, err := h.store.Contacts.GetByUID(ctx, bookID, uid)
		if err != nil {
			return nil, fmt.Errorf("failed to fetch contact")
		}
		if c == nil {
			continue
		}
		responses = append(responses, resourceResponse(cleanHref, etagProp(c.ETag, c.RawVCard, false)))
	}
	return responses, nil
}

func calendarResourceResponses(base string, events []store.Event) []response {
	return calendarResourceResponsesWithData(base, events, true)
}

// calendarResourceResponsesWithData allows control over including calendar data
func calendarResourceResponsesWithData(base string, events []store.Event, includeData bool) []response {
	baseHref := strings.TrimSuffix(base, "/") + "/"
	var responses []response
	for _, ev := range events {
		href := baseHref + ev.UID + ".ics"
		responses = append(responses, resourceResponse(href, etagPropWithData(ev.ETag, ev.RawICAL, true, includeData)))
	}
	return responses
}

func addressBookResourceResponses(base string, contacts []store.Contact) []response {
	baseHref := strings.TrimSuffix(base, "/") + "/"
	var responses []response
	for _, c := range contacts {
		href := baseHref + c.UID + ".vcf"
		responses = append(responses, resourceResponse(href, etagProp(c.ETag, c.RawVCard, false)))
	}
	return responses
}

func (h *Handler) calendarSyncCollection(ctx context.Context, cal *store.CalendarAccess, principalHref, cleanPath string, report reportRequest) ([]response, string, error) {
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

	responses := []response{
		calendarCollectionResponse(collectionHref, cal.Name, cal.Description, cal.Timezone, principalHref, syncToken, fmt.Sprintf("%d", cal.CTag)),
	}
	responses = append(responses, calendarResourceResponses(collectionHref, events)...)

	// Include deleted resources if this is an incremental sync
	if !since.IsZero() {
		deleted, err := h.store.DeletedResources.ListDeletedSince(ctx, "event", cal.ID, since)
		if err != nil {
			return nil, "", fmt.Errorf("failed to list deleted events")
		}
		for _, d := range deleted {
			href := collectionHref + d.UID + ".ics"
			responses = append(responses, deletedResponse(href))
		}
	}

	return responses, syncToken, nil
}

func (h *Handler) addressBookSyncCollection(ctx context.Context, book *store.AddressBook, principalHref, cleanPath string, report reportRequest) ([]response, string, error) {
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
			href := collectionHref + d.UID + ".vcf"
			responses = append(responses, deletedResponse(href))
		}
	}

	return responses, syncToken, nil
}
