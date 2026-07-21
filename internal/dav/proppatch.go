package dav

import (
	"context"
	"encoding/xml"
	"errors"
	"fmt"
	"net/http"
	"path"
	"strings"

	"github.com/jw6ventures/calcard/internal/auth"
	"github.com/jw6ventures/calcard/internal/ical"
	"github.com/jw6ventures/calcard/internal/store"
)

var settableLiveProperties = map[xml.Name]string{
	{Space: "DAV:", Local: "displayname"}:                                       "displayname",
	{Space: "urn:ietf:params:xml:ns:caldav", Local: "calendar-description"}:     "calendar-description",
	{Space: "urn:ietf:params:xml:ns:caldav", Local: "calendar-timezone"}:        "calendar-timezone",
	{Space: "http://apple.com/ns/ical/", Local: "calendar-color"}:               "calendar-color",
	{Space: "urn:ietf:params:xml:ns:carddav", Local: "addressbook-description"}: "addressbook-description",
}

var protectedLiveProperties = func() map[xml.Name]struct{} {
	properties := map[xml.Name]struct{}{
		{Space: "DAV:", Local: "creationdate"}:       {},
		{Space: "DAV:", Local: "getcontentlanguage"}: {},
		{Space: "DAV:", Local: "getcontentlength"}:   {},
		{Space: "DAV:", Local: "getlastmodified"}:    {},
	}
	for _, spec := range propfindPropertyTable {
		name := expandedPropertyName(spec.emptyName.Local)
		if name.Local != "" {
			properties[name] = struct{}{}
		}
	}
	return properties
}()

func expandedPropertyName(prefixed string) xml.Name {
	prefix, local, ok := strings.Cut(prefixed, ":")
	if !ok {
		return xml.Name{Local: prefixed}
	}
	spaces := map[string]string{
		"d":    "DAV:",
		"cal":  "urn:ietf:params:xml:ns:caldav",
		"card": "urn:ietf:params:xml:ns:carddav",
		"cs":   "http://calendarserver.org/ns/",
		"ical": "http://apple.com/ns/ical/",
	}
	return xml.Name{Space: spaces[prefix], Local: local}
}

func (h *DavServer) proppatch(w http.ResponseWriter, r *http.Request) {
	h.logger().Trace("Proppatch", "PROPPATCH %s", r.URL.Path)
	user, ok := auth.UserFromContext(r.Context())
	if !ok {
		http.Error(w, "missing user", http.StatusUnauthorized)
		return
	}

	cleanPath := path.Clean(r.URL.Path)
	target := parsedDAVTarget(r.Context(), cleanPath)
	if !target.Valid || target.CollectionSegment == "" || (target.Domain != davPathCalendar && target.Domain != davPathAddressBook) {
		http.Error(w, "unsupported path for PROPPATCH", http.StatusBadRequest)
		return
	}
	if !h.requireLock(w, r, cleanPath, "resource is locked") {
		return
	}

	body, err := readDAVBody(w, r, maxDAVBodyBytes)
	if err != nil {
		if errors.Is(err, errRequestTooLarge) {
			http.Error(w, "request too large", http.StatusRequestEntityTooLarge)
		} else {
			http.Error(w, "failed to read body", http.StatusBadRequest)
		}
		return
	}
	var request proppatchRequest
	if err := safeUnmarshalXML(body, &request); err != nil || len(request.Instructions) == 0 {
		http.Error(w, "invalid PROPPATCH body", http.StatusBadRequest)
		return
	}

	var responses []response
	switch target.Domain {
	case davPathCalendar:
		responses, err = h.proppatchCalendar(r.Context(), user, cleanPath, target, &request)
	case davPathAddressBook:
		responses, err = h.proppatchAddressBook(r.Context(), user, cleanPath, target, &request)
	}
	if err != nil {
		status := http.StatusInternalServerError
		switch {
		case errors.Is(err, errInvalidPath):
			status = http.StatusBadRequest
		case errors.Is(err, store.ErrNotFound):
			status = http.StatusNotFound
		case errors.Is(err, errAmbiguousCalendar), errors.Is(err, errAmbiguousAddressBook):
			status = http.StatusConflict
		}
		http.Error(w, err.Error(), status)
		return
	}
	writeMultiStatus(w, newMultistatus(responses, ""))
}

type calendarPatchState struct {
	name        string
	description *string
	timezone    *string
	color       *string
}

type addressBookPatchState struct {
	name        string
	description *string
}

type proppatchPreflight struct {
	dead     []store.DeadPropertyMutation
	failures map[int][]xml.Name
	all      []xml.Name
}

func (p *proppatchPreflight) record(property proppatchProperty, status int) {
	p.all = append(p.all, property.Name)
	if status != 0 {
		if p.failures == nil {
			p.failures = make(map[int][]xml.Name)
		}
		p.failures[status] = append(p.failures[status], property.Name)
	}
}

func (h *DavServer) proppatchCalendar(ctx context.Context, user *store.User, href string, target davTarget, request *proppatchRequest) ([]response, error) {
	calendarID, ok, err := h.resolveCalendarID(ctx, user, target.CollectionSegment)
	if err != nil {
		return nil, err
	}
	if !ok || calendarID == birthdayCalendarID {
		if calendarID == birthdayCalendarID {
			return forbiddenProppatchResponse(href, requestedProppatchPropertyNames(request)), nil
		}
		return nil, store.ErrNotFound
	}
	canonicalPath, err := h.canonicalDAVPath(ctx, user, href)
	if err != nil {
		return nil, err
	}
	cal, err := h.loadCalendarWithPrivilege(ctx, user, calendarID, canonicalPath, "write-properties")
	if err != nil {
		if errors.Is(err, errForbidden) {
			return forbiddenProppatchResponse(href, requestedProppatchPropertyNames(request)), nil
		}
		return nil, err
	}
	if target.Resource {
		event, err := h.store.Events.GetByResourceName(ctx, calendarID, target.ResourceName)
		if err != nil {
			return nil, err
		}
		if event == nil {
			return nil, store.ErrNotFound
		}
	}

	state := calendarPatchState{name: cal.Name, description: cal.Description, timezone: cal.Timezone, color: cal.Color}
	preflight := preflightCalendarPatch(request, target.Resource, &state)
	if len(preflight.failures) != 0 {
		return failedProppatchResponse(href, preflight), nil
	}
	if target.Resource {
		err = h.store.PatchDeadProperties(ctx, canonicalPath, preflight.dead)
	} else {
		err = h.store.PatchCalendarProperties(ctx, calendarID, state.name, state.description, state.timezone, state.color, canonicalPath, preflight.dead)
	}
	if err != nil {
		if errors.Is(err, store.ErrConflict) {
			failed := proppatchPreflight{all: preflight.all, failures: map[int][]xml.Name{http.StatusConflict: preflight.all}}
			return failedProppatchResponse(href, failed), nil
		}
		return nil, err
	}
	invalidateDAVRequestState(ctx)
	return successfulProppatchResponse(href, preflight.all), nil
}

func preflightCalendarPatch(request *proppatchRequest, object bool, state *calendarPatchState) proppatchPreflight {
	var result proppatchPreflight
	for _, instruction := range request.Instructions {
		for _, property := range instruction.Properties {
			status := 0
			kind, settable := settableLiveProperties[property.Name]
			if object && settable {
				status = http.StatusForbidden
			} else if settable {
				status = applyCalendarLivePatch(state, kind, property, instruction.Remove)
			} else if _, protected := protectedLiveProperties[property.Name]; protected {
				status = http.StatusForbidden
			} else {
				result.dead = append(result.dead, deadPropertyMutation(property, instruction.Remove))
			}
			result.record(property, status)
		}
	}
	return result
}

func applyCalendarLivePatch(state *calendarPatchState, kind string, property proppatchProperty, remove bool) int {
	if property.HasElement {
		return http.StatusConflict
	}
	switch kind {
	case "displayname":
		if remove || property.Text == "" {
			return http.StatusConflict
		}
		state.name = property.Text
	case "calendar-description":
		state.description = optionalPatchedText(property.Text, remove)
	case "calendar-timezone":
		if !remove && !validCalendarTimezone(property.Text) {
			return http.StatusConflict
		}
		state.timezone = optionalPatchedText(property.Text, remove)
	case "calendar-color":
		if remove {
			state.color = nil
			return 0
		}
		color, err := store.NormalizeCalendarColor(property.Text)
		if err != nil {
			return http.StatusConflict
		}
		state.color = color
	default:
		return http.StatusForbidden
	}
	return 0
}

func validCalendarTimezone(value string) bool {
	lines := ical.UnfoldLines(value)
	var stack []string
	rootSeen := false
	rootType := ""
	timezoneDepth := 0
	timezoneCount := 0
	hasTZID := false
	for _, rawLine := range lines {
		line := strings.TrimSpace(rawLine)
		if line == "" {
			continue
		}
		upper := strings.ToUpper(line)
		switch {
		case strings.HasPrefix(upper, "BEGIN:"):
			component := strings.TrimSpace(strings.TrimPrefix(upper, "BEGIN:"))
			if len(stack) == 0 {
				if rootSeen || (component != "VTIMEZONE" && component != "VCALENDAR") {
					return false
				}
				rootSeen = true
				rootType = component
				if component == "VTIMEZONE" {
					timezoneCount = 1
					timezoneDepth = 1
				}
			} else if len(stack) == 1 && rootType == "VCALENDAR" {
				if component != "VTIMEZONE" || timezoneCount != 0 {
					return false
				}
				timezoneCount = 1
				timezoneDepth = 2
			} else if component == "VTIMEZONE" {
				return false
			}
			stack = append(stack, component)
		case strings.HasPrefix(upper, "END:"):
			component := strings.TrimSpace(strings.TrimPrefix(upper, "END:"))
			if len(stack) == 0 || stack[len(stack)-1] != component {
				return false
			}
			stack = stack[:len(stack)-1]
		default:
			if len(stack) == 0 {
				return false
			}
			name, _, propertyValue, ok := splitICalendarProperty(line)
			if ok && len(stack) == timezoneDepth && stack[len(stack)-1] == "VTIMEZONE" && name == "TZID" && strings.TrimSpace(propertyValue) != "" {
				hasTZID = true
			}
		}
	}
	return rootSeen && len(stack) == 0 && timezoneCount == 1 && hasTZID
}

func (h *DavServer) proppatchAddressBook(ctx context.Context, user *store.User, href string, target davTarget, request *proppatchRequest) ([]response, error) {
	bookID, ok, err := h.resolveAddressBookID(ctx, user, target.CollectionSegment)
	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, store.ErrNotFound
	}
	canonicalPath, err := h.canonicalDAVPath(ctx, user, href)
	if err != nil {
		return nil, err
	}
	book, err := h.getAddressBook(ctx, bookID)
	if err != nil {
		return nil, err
	}
	if err := h.requireAddressBookPrivilege(ctx, user, book, canonicalPath, "write-properties"); err != nil {
		if errors.Is(err, errForbidden) || errors.Is(err, store.ErrNotFound) {
			return forbiddenProppatchResponse(href, requestedProppatchPropertyNames(request)), nil
		}
		return nil, err
	}
	if target.Resource {
		contact, err := h.store.Contacts.GetByResourceName(ctx, bookID, target.ResourceName)
		if err != nil {
			return nil, err
		}
		if contact == nil {
			return nil, store.ErrNotFound
		}
	}

	state := addressBookPatchState{name: book.Name, description: book.Description}
	preflight := preflightAddressBookPatch(request, target.Resource, &state)
	if len(preflight.failures) != 0 {
		return failedProppatchResponse(href, preflight), nil
	}
	if target.Resource {
		err = h.store.PatchDeadProperties(ctx, canonicalPath, preflight.dead)
	} else {
		err = h.store.PatchAddressBookProperties(ctx, bookID, state.name, state.description, canonicalPath, preflight.dead)
	}
	if err != nil {
		if errors.Is(err, store.ErrConflict) {
			failed := proppatchPreflight{all: preflight.all, failures: map[int][]xml.Name{http.StatusConflict: preflight.all}}
			return failedProppatchResponse(href, failed), nil
		}
		return nil, err
	}
	invalidateDAVRequestState(ctx)
	return successfulProppatchResponse(href, preflight.all), nil
}

func preflightAddressBookPatch(request *proppatchRequest, object bool, state *addressBookPatchState) proppatchPreflight {
	var result proppatchPreflight
	for _, instruction := range request.Instructions {
		for _, property := range instruction.Properties {
			status := 0
			kind, settable := settableLiveProperties[property.Name]
			switch {
			case object && settable:
				status = http.StatusForbidden
			case settable && kind == "displayname":
				if instruction.Remove || property.HasElement || property.Text == "" {
					status = http.StatusConflict
				} else {
					state.name = property.Text
				}
			case settable && kind == "addressbook-description":
				if property.HasElement {
					status = http.StatusConflict
				} else {
					state.description = optionalPatchedText(property.Text, instruction.Remove)
				}
			case settable:
				status = http.StatusForbidden
			default:
				if _, protected := protectedLiveProperties[property.Name]; protected {
					status = http.StatusForbidden
				} else {
					result.dead = append(result.dead, deadPropertyMutation(property, instruction.Remove))
				}
			}
			result.record(property, status)
		}
	}
	return result
}

func optionalPatchedText(value string, remove bool) *string {
	if remove {
		return nil
	}
	return stringPtr(value)
}

func deadPropertyMutation(property proppatchProperty, remove bool) store.DeadPropertyMutation {
	return store.DeadPropertyMutation{
		NamespaceURI: property.Name.Space,
		LocalName:    property.Name.Local,
		InnerXML:     property.InnerXML,
		Remove:       remove,
	}
}

func requestedProppatchPropertyNames(request *proppatchRequest) []xml.Name {
	if request == nil {
		return nil
	}
	var names []xml.Name
	for _, instruction := range request.Instructions {
		for _, property := range instruction.Properties {
			names = append(names, property.Name)
		}
	}
	return names
}

func forbiddenProppatchResponse(href string, names []xml.Name) []response {
	return []response{{Href: href, Propstat: []propstat{{PropNames: names, Status: httpStatusForbidden}}}}
}

func failedProppatchResponse(href string, preflight proppatchPreflight) []response {
	failed := make(map[xml.Name]struct{})
	var propstats []propstat
	for _, status := range []int{http.StatusForbidden, http.StatusConflict} {
		names := preflight.failures[status]
		if len(names) == 0 {
			continue
		}
		for _, name := range names {
			failed[name] = struct{}{}
		}
		propstats = append(propstats, propstat{PropNames: uniqueXMLNames(names), Status: fmt.Sprintf("HTTP/1.1 %d %s", status, http.StatusText(status))})
	}
	var dependencies []xml.Name
	for _, name := range preflight.all {
		if _, ok := failed[name]; !ok {
			dependencies = append(dependencies, name)
		}
	}
	if len(dependencies) != 0 {
		propstats = append(propstats, propstat{PropNames: uniqueXMLNames(dependencies), Status: "HTTP/1.1 424 Failed Dependency"})
	}
	return []response{{Href: href, Propstat: propstats}}
}

func successfulProppatchResponse(href string, names []xml.Name) []response {
	return []response{{Href: href, Propstat: []propstat{{PropNames: uniqueXMLNames(names), Status: httpStatusOK}}}}
}

func uniqueXMLNames(names []xml.Name) []xml.Name {
	seen := make(map[xml.Name]struct{}, len(names))
	result := make([]xml.Name, 0, len(names))
	for _, name := range names {
		if name.Local == "" {
			continue
		}
		if _, ok := seen[name]; ok {
			continue
		}
		seen[name] = struct{}{}
		result = append(result, name)
	}
	return result
}
