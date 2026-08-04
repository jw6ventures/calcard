package dav

import (
	"context"
	"encoding/xml"
	"errors"
	"fmt"
	"net/http"
	"path"
	"strconv"
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
	lockPreconditions := h.lockPreconditions(r, cleanPath)
	switch target.Domain {
	case davPathCalendar:
		responses, err = h.proppatchCalendar(r.Context(), user, cleanPath, target, &request, lockPreconditions)
	case davPathAddressBook:
		responses, err = h.proppatchAddressBook(r.Context(), user, cleanPath, target, &request, lockPreconditions)
	}
	if err != nil {
		if errors.Is(err, store.ErrLockConflict) {
			http.Error(w, "resource is locked", http.StatusLocked)
			return
		}
		if errors.Is(err, errForbidden) || isPrivilegeNotGranted(err) {
			writeNeedPrivileges(w, cleanPath, "write-properties")
			return
		}
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
	name            string
	description     *string
	descriptionLang *string
	timezone        *string
	color           *string
}

func (s calendarPatchState) properties() store.CalendarProperties {
	return store.CalendarProperties{
		Name:            s.name,
		Description:     s.description,
		DescriptionLang: s.descriptionLang,
		Timezone:        s.timezone,
		Color:           s.color,
	}
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

func (h *DavServer) proppatchCalendar(ctx context.Context, user *store.User, href string, target davTarget, request *proppatchRequest, lockPreconditions []store.LockPrecondition) ([]response, error) {
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
		if errors.Is(err, errForbidden) || isPrivilegeNotGranted(err) {
			return nil, errForbidden
		}
		return nil, err
	}
	var event *store.Event
	if target.Resource {
		event, err = h.store.Events.GetByResourceName(ctx, calendarID, target.ResourceName)
		if err != nil {
			return nil, err
		}
		if event == nil {
			return nil, store.ErrNotFound
		}
	}

	state := calendarPatchState{name: cal.Name, description: cal.Description, descriptionLang: cal.DescriptionLang, timezone: cal.Timezone, color: cal.Color}
	preflight := preflightCalendarPatch(request, target.Resource, &state)
	if len(preflight.failures) != 0 {
		return failedProppatchResponse(href, preflight), nil
	}
	if target.Resource {
		expected := store.EventDAVResourceState(event)
		expected.CollectionCTag = &cal.CTag
		err = h.store.PatchObjectDeadProperties(ctx, "calendar", calendarID, expected, canonicalPath, preflight.dead, lockPreconditions)
	} else {
		err = h.store.PatchCalendarProperties(ctx, calendarID, state.properties(), canonicalPath, preflight.dead, lockPreconditions)
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
		// RFC 4918 §4.3: the xml:lang in force for the submitted value travels
		// with it, so PROPFIND returns the property in the language it was set.
		state.descriptionLang = nil
		if !remove && property.Lang != "" {
			state.descriptionLang = stringPtr(property.Lang)
		}
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

// icalComponent is one open component while a CALDAV:calendar-timezone value is
// being read: the properties it has declared so far and the sub-components it
// has opened, both of which the RFC 5545 content model constrains.
type icalComponent struct {
	name          string
	properties    map[string]int
	propertyLines map[string][]icalProperty
	values        map[string]string
	children      map[string]int
}

func newICalComponent(name string) *icalComponent {
	return &icalComponent{
		name:          name,
		properties:    map[string]int{},
		propertyLines: map[string][]icalProperty{},
		values:        map[string]string{},
		children:      map[string]int{},
	}
}

// validCalendarTimezone reports whether value is what RFC 4791 §5.2.2 and
// §5.3.1.1 (CALDAV:valid-calendar-data) require of a CALDAV:calendar-timezone:
// a valid iCalendar object -- so a VCALENDAR envelope carrying the properties
// RFC 5545 §3.6 makes mandatory, not a bare component -- containing exactly one
// VTIMEZONE built the way §3.6.5 requires. The value is stored and served back
// verbatim, so anything accepted here is what clients later have to parse.
func validCalendarTimezone(value string) bool {
	var stack []*icalComponent
	rootSeen := false
	for _, rawLine := range ical.UnfoldLines(value) {
		line := strings.TrimSpace(rawLine)
		if line == "" {
			continue
		}
		upper := strings.ToUpper(line)
		switch {
		case strings.HasPrefix(upper, "BEGIN:"):
			name := strings.TrimSpace(strings.TrimPrefix(upper, "BEGIN:"))
			if !calendarTimezoneAdmitsComponent(stack, name) {
				return false
			}
			if len(stack) == 0 {
				if rootSeen {
					return false
				}
				rootSeen = true
			} else {
				stack[len(stack)-1].children[name]++
			}
			stack = append(stack, newICalComponent(name))
		case strings.HasPrefix(upper, "END:"):
			name := strings.TrimSpace(strings.TrimPrefix(upper, "END:"))
			if len(stack) == 0 || stack[len(stack)-1].name != name {
				return false
			}
			if !completeCalendarTimezoneComponent(stack[len(stack)-1]) {
				return false
			}
			stack = stack[:len(stack)-1]
		default:
			if len(stack) == 0 {
				return false
			}
			property, ok := parseICalProperty(line)
			if !ok {
				return false
			}
			name := property.name
			current := stack[len(stack)-1]
			if current.name == "VCALENDAR" && len(current.children) != 0 {
				return false
			}
			current.properties[name]++
			current.propertyLines[name] = append(current.propertyLines[name], property)
			if _, seen := current.values[name]; !seen {
				current.values[name] = property.value
			}
		}
	}
	return rootSeen && len(stack) == 0
}

// calendarTimezoneAdmitsComponent applies the RFC 5545 content model for the
// only nesting a calendar-timezone value may have: one VCALENDAR holding one
// VTIMEZONE, whose sub-components are STANDARD and DAYLIGHT.
func calendarTimezoneAdmitsComponent(stack []*icalComponent, name string) bool {
	switch len(stack) {
	case 0:
		return name == "VCALENDAR"
	case 1:
		// §5.2.2 admits exactly one VTIMEZONE and nothing else beside it.
		return name == "VTIMEZONE" && stack[0].children["VTIMEZONE"] == 0
	case 2:
		return name == "STANDARD" || name == "DAYLIGHT"
	default:
		return false
	}
}

// completeCalendarTimezoneComponent reports whether a component carries the
// properties and sub-components RFC 5545 §3.6 and §3.6.5 make mandatory for it.
func completeCalendarTimezoneComponent(component *icalComponent) bool {
	if !validCalendarTimezoneComponentProperties(component) {
		return false
	}
	switch component.name {
	case "VCALENDAR":
		// §3.6: VERSION and PRODID are required and occur once each, and §3.7.4
		// fixes the only version this grammar describes.
		if component.properties["VERSION"] != 1 || component.values["VERSION"] != "2.0" {
			return false
		}
		if component.properties["PRODID"] != 1 || component.values["PRODID"] == "" {
			return false
		}
		return component.children["VTIMEZONE"] == 1
	case "VTIMEZONE":
		// §3.6.5: TZID is required and occurs once, and at least one STANDARD or
		// DAYLIGHT sub-component must be present.
		if component.properties["TZID"] != 1 || component.values["TZID"] == "" {
			return false
		}
		return component.children["STANDARD"]+component.children["DAYLIGHT"] > 0
	case "STANDARD", "DAYLIGHT":
		// §3.6.5: each observance declares when it starts and the offsets it
		// moves between, once each.
		for _, required := range []string{"DTSTART", "TZOFFSETFROM", "TZOFFSETTO"} {
			if component.properties[required] != 1 || component.values[required] == "" {
				return false
			}
		}
		return true
	default:
		return false
	}
}

func validUTCOffset(value string) bool {
	if len(value) != 5 && len(value) != 7 {
		return false
	}
	if value[0] != '+' && value[0] != '-' {
		return false
	}
	digits := value[1:]
	for _, digit := range digits {
		if digit < '0' || digit > '9' {
			return false
		}
	}
	hour, _ := strconv.Atoi(digits[:2])
	minute, _ := strconv.Atoi(digits[2:4])
	second := 0
	if len(digits) == 6 {
		second, _ = strconv.Atoi(digits[4:6])
	}
	if hour > 23 || minute > 59 || second > 59 {
		return false
	}
	return value[0] != '-' || hour != 0 || minute != 0 || second != 0
}

func (h *DavServer) proppatchAddressBook(ctx context.Context, user *store.User, href string, target davTarget, request *proppatchRequest, lockPreconditions []store.LockPrecondition) ([]response, error) {
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
			return nil, errForbidden
		}
		return nil, err
	}
	var contact *store.Contact
	if target.Resource {
		contact, err = h.store.Contacts.GetByResourceName(ctx, bookID, target.ResourceName)
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
		expected := store.ContactDAVResourceState(contact)
		expected.CollectionCTag = &book.CTag
		err = h.store.PatchObjectDeadProperties(ctx, "addressbook", bookID, expected, canonicalPath, preflight.dead, lockPreconditions)
	} else {
		err = h.store.PatchAddressBookProperties(ctx, bookID, state.name, state.description, canonicalPath, preflight.dead, lockPreconditions)
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
