package dav

import (
	"bytes"
	"context"
	"encoding/xml"
	"errors"
	"fmt"
	"io"
	"net/http"
	"path"
	"slices"
	"strconv"
	"strings"

	"github.com/jw6ventures/calcard/internal/auth"
	"github.com/jw6ventures/calcard/internal/store"
	"github.com/lib/pq"
)

func (h *DavServer) mkcol(w http.ResponseWriter, r *http.Request) {
	h.logger().Trace("Mkcol", "MKCOL %s", r.URL.Path)
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
		var mkReq mkcolRequest
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
	invalidateDAVRequestState(r.Context())
	if created != nil {
		location := path.Join("/dav/addressbooks", fmt.Sprint(created.ID)) + "/"
		if err := h.rebindCollectionLocks(r.Context(), pendingLockPath, strings.TrimSuffix(location, "/")); err != nil {
			if deleteErr := h.store.AddressBooks.Delete(r.Context(), user.ID, created.ID); deleteErr != nil && !errors.Is(deleteErr, store.ErrNotFound) {
				h.logger().Error("Mkcol", "failed to roll back address book %d after lock rebind failure: %v", created.ID, deleteErr)
			}
			http.Error(w, "failed to rebind collection locks", http.StatusInternalServerError)
			return
		}
		w.Header().Set("Location", location)
	}
	w.WriteHeader(http.StatusCreated)
}

// MKCALENDAR precondition elements from RFC 4791 §5.3.1.1. Each is reported as
// a child of a top-level DAV:error, and §1.3 fixes the status that carries it:
// 403 where the request can never succeed as written, 409 where the user can
// resolve the conflict and resubmit.
const (
	conditionResourceMustBeNull = "resource-must-be-null"
	conditionCalendarLocationOK = "calendar-collection-location-ok"
	conditionValidCalendarData  = "valid-calendar-data"
)

func (h *DavServer) mkcalendar(w http.ResponseWriter, r *http.Request) {
	h.logger().Trace("Mkcalendar", "MKCALENDAR %s", r.URL.Path)
	user, ok := auth.UserFromContext(r.Context())
	if !ok {
		http.Error(w, "missing user", http.StatusUnauthorized)
		return
	}

	cleanPath := path.Clean(r.URL.Path)
	if !h.requireLocks(w, r, "resource is locked", cleanPath, path.Dir(cleanPath)) {
		return
	}
	// The location precondition is checked before the Request-URI is resolved
	// for lock rebinding, because a URI that can never hold a calendar
	// collection has no canonical form to resolve.
	pathName, locationStatus, err := h.mkcalendarLocation(r.Context(), user, cleanPath)
	if err != nil {
		h.logger().Error("Mkcalendar", "failed to check the calendar location %s: %v", cleanPath, err)
		http.Error(w, "failed to check calendar location", http.StatusInternalServerError)
		return
	}
	if locationStatus != 0 {
		writeCalDAVError(w, locationStatus, conditionCalendarLocationOK)
		return
	}

	pendingLockPath, err := h.canonicalDAVPath(r.Context(), user, cleanPath)
	if err != nil {
		http.Error(w, "failed to resolve collection path", http.StatusInternalServerError)
		return
	}

	request, ok := h.readMkcalendarRequest(w, r)
	if !ok {
		return
	}

	state := mkcalendarState{name: pathName}
	preflight, condition := preflightMkcalendar(request, &state)
	if condition != "" {
		// The same request will always fail, which §1.3 makes a 403.
		writeCalDAVError(w, http.StatusForbidden, condition)
		return
	}
	if len(preflight.failures) != 0 {
		// RFC 4791 §5.3.1 (via RFC 2518 §12.13.2): report each instruction's own
		// status and 424 for the rest. Nothing has been written yet, so the
		// all-or-none rule holds without a rollback.
		writeMultiStatus(w, newMultistatus(failedProppatchResponse(cleanPath, preflight), ""))
		return
	}

	slug := strings.ToLower(pathName)
	exists, err := h.calendarCollectionExists(r.Context(), user, pathName, slug)
	if err != nil {
		http.Error(w, "failed to check calendars", http.StatusInternalServerError)
		return
	}
	if exists {
		// The user can remove what is there and resubmit, so §1.3 gives 409.
		writeDAVError(w, http.StatusConflict, conditionResourceMustBeNull)
		return
	}

	// The collection, the dead properties its body set, and the lock rebinding
	// are written as one unit, so a failure in any of them leaves no partial
	// collection behind (RFC 4791 §5.3.1).
	created, err := h.store.CreateCalendarAndState(r.Context(), store.Calendar{
		UserID:              user.ID,
		Name:                state.name,
		Slug:                &slug,
		Description:         state.description,
		DescriptionLang:     state.descriptionLang,
		Timezone:            state.timezone,
		Color:               state.color,
		SupportedComponents: state.components,
	}, preflight.dead, h.lockPreconditions(r, cleanPath, path.Dir(cleanPath)), pendingLockPath, calendarStatePath)
	invalidateDAVRequestState(r.Context())
	if err != nil {
		if errors.Is(err, store.ErrLockConflict) {
			http.Error(w, "resource is locked", http.StatusLocked)
			return
		}
		var pqErr *pq.Error
		if errors.As(err, &pqErr) && pqErr.Code == "23505" {
			writeDAVError(w, http.StatusConflict, conditionResourceMustBeNull)
			return
		}
		h.logger().Error("Mkcalendar", "failed to create calendar collection at %s: %v", cleanPath, err)
		http.Error(w, "failed to create", http.StatusInternalServerError)
		return
	}
	location := calendarStatePath(created.ID) + "/"
	w.Header().Set("Location", location)
	// RFC 4791 §5.3.1: a successful MKCALENDAR response must not be cached.
	w.Header().Set("Cache-Control", "no-cache")
	writeMkcalendarResponse(w, preflight.all)
}

// calendarStatePath is the canonical DAV path a calendar collection's lock,
// ACL and dead-property state is keyed by.
func calendarStatePath(calendarID int64) string {
	return collectionResourcePath(calendarPrefix, calendarID)
}

// mkcalendarLocation checks the Request-URI against the RFC 4791 §5.3.1.1
// CALDAV:calendar-collection-location-ok precondition. It returns the calendar
// name the last path segment carries, or the status to answer with when no
// calendar collection can be created there. A non-nil error means the location
// could not be judged at all, which is the server's failure rather than the
// client's.
func (h *DavServer) mkcalendarLocation(ctx context.Context, user *store.User, cleanPath string) (string, int, error) {
	if !strings.HasPrefix(cleanPath, "/dav/calendars/") {
		// CalCard hosts calendar collections only under /dav/calendars/, and
		// that namespace is fixed, so no user action makes such a URI work.
		return "", http.StatusForbidden, nil
	}
	segments := strings.Split(strings.Trim(strings.TrimPrefix(cleanPath, "/dav/calendars"), "/"), "/")
	if len(segments) > 1 {
		// A deeper URI is either nested inside an existing calendar collection,
		// which RFC 4791 §4.2 forbids outright, or hangs off a parent that does
		// not exist -- resolvable, so §1.3 makes it a 409. An unresolvable parent
		// name is the same kind of resolvable conflict; anything else means the
		// repository could not answer, and must not be reported as either.
		parentSegment := segments[0]
		if parentID, parseErr := strconv.ParseInt(parentSegment, 10, 64); parseErr == nil {
			if parentID == birthdayCalendarID {
				return "", http.StatusForbidden, nil
			}
			if h.store == nil || h.store.Calendars == nil {
				return "", 0, store.ErrNotFound
			}
			parent, err := h.store.Calendars.GetAccessible(ctx, parentID, user.ID)
			if err != nil {
				return "", 0, err
			}
			if parent != nil {
				return "", http.StatusForbidden, nil
			}
			return "", http.StatusConflict, nil
		}
		_, ok, err := h.resolveCalendarID(ctx, user, parentSegment)
		if err != nil && !errors.Is(err, store.ErrNotFound) && !errors.Is(err, errAmbiguousCalendar) {
			return "", 0, err
		}
		if ok {
			return "", http.StatusForbidden, nil
		}
		return "", http.StatusConflict, nil
	}
	pathName := strings.TrimSpace(segments[0])
	if pathName == "" {
		// The Request-URI is the calendar home collection itself.
		return "", http.StatusForbidden, nil
	}
	if _, err := strconv.ParseInt(pathName, 10, 64); err == nil {
		// A numeric last segment belongs to the by-ID namespace this handler
		// cannot bind a new collection into.
		return "", http.StatusForbidden, nil
	}
	if !isValidCalendarSlug(strings.ToLower(pathName)) {
		return "", http.StatusForbidden, nil
	}
	return pathName, 0, nil
}

func (h *DavServer) calendarCollectionExists(ctx context.Context, user *store.User, pathName, slug string) (bool, error) {
	cals, err := h.store.Calendars.ListAccessible(ctx, user.ID)
	if err != nil {
		return false, err
	}
	for _, cal := range cals {
		if cal.Slug != nil && *cal.Slug == slug {
			return true, nil
		}
		if strings.EqualFold(cal.Name, pathName) {
			return true, nil
		}
	}
	return false, nil
}

// readMkcalendarRequest reads and decodes the request body. RFC 4791 §5.3.1
// admits only a CALDAV:mkcalendar element, which mkcalendarRequest enforces.
func (h *DavServer) readMkcalendarRequest(w http.ResponseWriter, r *http.Request) (*mkcalendarRequest, bool) {
	request := &mkcalendarRequest{}
	if r.Body == http.NoBody {
		return request, true
	}
	body, err := readDAVBody(w, r, maxDAVBodyBytes)
	if err != nil {
		if errors.Is(err, errRequestTooLarge) {
			http.Error(w, "request too large", http.StatusRequestEntityTooLarge)
		} else {
			http.Error(w, "failed to read body", http.StatusBadRequest)
		}
		return nil, false
	}
	if len(bytes.TrimSpace(body)) == 0 {
		return request, true
	}
	if err := safeUnmarshalXML(body, request); err != nil {
		http.Error(w, "invalid MKCALENDAR body", http.StatusBadRequest)
		return nil, false
	}
	return request, true
}

// mkcalendarState is the collection a MKCALENDAR body describes, assembled
// before anything is written.
type mkcalendarState struct {
	name            string
	description     *string
	descriptionLang *string
	timezone        *string
	color           *string
	components      []string
}

// preflightMkcalendar applies the body's property instructions to state in
// document order (RFC 4791 §5.3.1), recording a per-property status for each
// instruction that cannot be honoured. It writes nothing, so a failure needs no
// rollback. A non-empty condition names a §5.3.1.1 precondition the body broke,
// which is reported as a DAV:error rather than a per-property status.
func preflightMkcalendar(request *mkcalendarRequest, state *mkcalendarState) (proppatchPreflight, string) {
	var result proppatchPreflight
	if request == nil {
		return result, ""
	}
	for _, instruction := range request.Instructions {
		for _, property := range instruction.Properties {
			status, condition := applyMkcalendarProperty(state, property, &result)
			if condition != "" {
				return proppatchPreflight{}, condition
			}
			result.record(property, status)
		}
	}
	return result, ""
}

func applyMkcalendarProperty(state *mkcalendarState, property proppatchProperty, result *proppatchPreflight) (int, string) {
	switch property.Name {
	case xml.Name{Space: "DAV:", Local: "displayname"}:
		if property.HasElement || property.Text == "" {
			return http.StatusConflict, ""
		}
		state.name = property.Text
	case xml.Name{Space: "urn:ietf:params:xml:ns:caldav", Local: "calendar-description"}:
		if property.HasElement {
			return http.StatusConflict, ""
		}
		state.description = stringPtr(property.Text)
		state.descriptionLang = nil
		if property.Lang != "" {
			state.descriptionLang = stringPtr(property.Lang)
		}
	case xml.Name{Space: "urn:ietf:params:xml:ns:caldav", Local: "calendar-timezone"}:
		if property.HasElement {
			return http.StatusConflict, ""
		}
		// §5.3.1.1 (CALDAV:valid-calendar-data): an unusable timezone is a
		// precondition failure, not a per-property status.
		if !validCalendarTimezone(property.Text) {
			return 0, conditionValidCalendarData
		}
		state.timezone = stringPtr(property.Text)
	case xml.Name{Space: "urn:ietf:params:xml:ns:caldav", Local: "supported-calendar-component-set"}:
		components, ok := parseSupportedComponentSet(property.InnerXML)
		if !ok {
			return http.StatusConflict, ""
		}
		state.components = components
	case xml.Name{Space: "http://apple.com/ns/ical/", Local: "calendar-color"}:
		if property.HasElement {
			return http.StatusConflict, ""
		}
		color, err := store.NormalizeCalendarColor(property.Text)
		if err != nil {
			return http.StatusConflict, ""
		}
		state.color = color
	default:
		if _, protected := protectedLiveProperties[property.Name]; protected {
			return http.StatusForbidden, ""
		}
		result.dead = append(result.dead, deadPropertyMutation(property, false))
	}
	return 0, ""
}

// parseSupportedComponentSet reads the CALDAV:comp children of a
// supported-calendar-component-set value. RFC 4791 §5.2.3 defines the content
// model as one or more empty CALDAV:comp elements carrying a name attribute, so
// anything else is rejected.
func parseSupportedComponentSet(innerXML string) ([]string, bool) {
	decoder := xml.NewDecoder(strings.NewReader("<set>" + innerXML + "</set>"))
	var names []string
	depth := 0
	for {
		token, err := decoder.Token()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, false
		}
		switch token := token.(type) {
		case xml.StartElement:
			depth++
			if depth == 1 {
				continue
			}
			if depth > 2 || token.Name.Space != "urn:ietf:params:xml:ns:caldav" || token.Name.Local != "comp" {
				return nil, false
			}
			name := ""
			for _, attr := range token.Attr {
				if attr.Name.Local == "name" && attr.Name.Space == "" {
					name = strings.ToUpper(strings.TrimSpace(attr.Value))
				}
			}
			if name == "" || !isSupportableCalendarComponent(name) {
				return nil, false
			}
			names = append(names, name)
		case xml.EndElement:
			depth--
		case xml.CharData:
			if depth >= 1 && strings.TrimSpace(string(token)) != "" {
				return nil, false
			}
		}
	}
	if len(names) == 0 {
		return nil, false
	}
	return names, true
}

// isSupportableCalendarComponent reports whether CalCard can store calendar
// object resources of the named component type. A collection cannot restrict
// itself to a type the server does not implement.
func isSupportableCalendarComponent(name string) bool {
	return slices.Contains(defaultSupportedCalendarComponents, name) || nonStandardICalendarName(name)
}

// writeMkcalendarResponse answers a successful MKCALENDAR. RFC 4791 §5.3.1
// requires a response body, when one is sent, to be a CALDAV:mkcalendar-response
// element; the properties applied are reported inside it so a client can see
// which instructions took effect.
func writeMkcalendarResponse(w http.ResponseWriter, applied []xml.Name) {
	payload := mkcalendarResponse{
		XmlnsD:    "DAV:",
		XmlnsCal:  "urn:ietf:params:xml:ns:caldav",
		XmlnsCard: "urn:ietf:params:xml:ns:carddav",
		XmlnsCS:   "http://calendarserver.org/ns/",
		XmlnsICAL: "http://apple.com/ns/ical/",
	}
	if names := uniqueXMLNames(applied); len(names) != 0 {
		payload.Propstat = []propstat{{PropNames: names, Status: httpStatusOK}}
	}
	w.Header().Set("Content-Type", "application/xml; charset=utf-8")
	w.WriteHeader(http.StatusCreated)
	_ = xml.NewEncoder(w).Encode(payload)
}
