package dav

import (
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/jw6ventures/calcard/internal/ical"
)

// icalNode is one component of a parsed calendar object resource: the content
// lines it declares and the components nested inside it, both in document order.
type icalNode struct {
	name       string
	properties []icalProperty
	children   []*icalNode
}

func (n *icalNode) count(name string) int {
	total := 0
	for _, property := range n.properties {
		if property.name == name {
			total++
		}
	}
	return total
}

func (n *icalNode) value(name string) string {
	for _, property := range n.properties {
		if property.name == name {
			return property.value
		}
	}
	return ""
}

func (n *icalNode) childCount(name string) int {
	total := 0
	for _, child := range n.children {
		if child.name == name {
			total++
		}
	}
	return total
}

// parseICalendarObject reads raw into the component tree beneath its VCALENDAR
// root. It is deliberately unforgiving: RFC 4791 §5.3.2.1 makes a PUT whose body
// is not valid for its media type a precondition failure, so a content line that
// does not parse is an error here rather than a line to skip past.
func parseICalendarObject(raw string) (*icalNode, error) {
	var stack []*icalNode
	var root *icalNode
	for _, rawLine := range ical.UnfoldLines(raw) {
		if strings.TrimSpace(rawLine) == "" {
			continue
		}
		line := rawLine
		upper := strings.ToUpper(line)
		switch {
		case strings.HasPrefix(upper, "BEGIN:"):
			name := strings.TrimSpace(strings.TrimPrefix(upper, "BEGIN:"))
			if !validICalendarToken(name) {
				return nil, fmt.Errorf("invalid component name %q", name)
			}
			if len(stack) == 0 {
				if root != nil {
					return nil, fmt.Errorf("more than one VCALENDAR root")
				}
				if name != "VCALENDAR" {
					return nil, fmt.Errorf("expected one VCALENDAR root")
				}
			} else if name == "VCALENDAR" {
				return nil, fmt.Errorf("nested VCALENDAR component")
			}
			node := &icalNode{name: name}
			if len(stack) == 0 {
				root = node
			} else {
				parent := stack[len(stack)-1]
				parent.children = append(parent.children, node)
			}
			stack = append(stack, node)
		case strings.HasPrefix(upper, "END:"):
			name := strings.TrimSpace(strings.TrimPrefix(upper, "END:"))
			if len(stack) == 0 {
				return nil, fmt.Errorf("END:%s without matching BEGIN", name)
			}
			if stack[len(stack)-1].name != name {
				return nil, fmt.Errorf("mismatched tags: BEGIN:%s ... END:%s", stack[len(stack)-1].name, name)
			}
			stack = stack[:len(stack)-1]
		default:
			if len(stack) == 0 {
				return nil, fmt.Errorf("content outside VCALENDAR root")
			}
			property, ok := parseICalProperty(line)
			if !ok {
				return nil, fmt.Errorf("malformed content line")
			}
			current := stack[len(stack)-1]
			// RFC 5545 §3.4 and §3.6: properties precede sub-components in every
			// standard component. Extension components remain opaque and are
			// preserved without imposing a private content model on them.
			if !nonStandardICalendarName(current.name) && len(current.children) != 0 {
				return nil, fmt.Errorf("%s property after a component", current.name)
			}
			current.properties = append(current.properties, property)
		}
	}
	if len(stack) != 0 {
		return nil, fmt.Errorf("unbalanced tags: BEGIN:%s without matching END", stack[len(stack)-1].name)
	}
	if root == nil {
		return nil, fmt.Errorf("missing BEGIN:VCALENDAR")
	}
	return root, nil
}

// calendarObjectFault is a failed RFC 4791 §5.3.2.1 precondition together with
// the status §1.3 assigns it: 403 when repeating the request can only fail
// again, 409 when the user can resolve the conflict and resubmit.
type calendarObjectFault struct {
	status     int
	conditions []string
}

func invalidCalendarData() *calendarObjectFault {
	return &calendarObjectFault{status: http.StatusForbidden, conditions: []string{"valid-calendar-data"}}
}

func invalidCalendarObjectResource() *calendarObjectFault {
	return &calendarObjectFault{status: http.StatusForbidden, conditions: []string{"valid-calendar-object-resource"}}
}

// scheduledComponentTypes are the component types a CalCard calendar object
// resource may carry as its single §4.1 component type. VTIMEZONE is excluded
// deliberately: §5.2.3 admits a VTIMEZONE-only resource only from a server that
// stores one, and CalCard advertises no such support.
var scheduledComponentTypes = map[string]struct{}{
	"VEVENT": {}, "VTODO": {}, "VJOURNAL": {}, "VFREEBUSY": {},
}

// icalComponentRules is the RFC 5545 §3.6 content model of one component: the
// properties it requires, the standard properties it admits, those that may not
// repeat, and the components it may contain. A property name outside `allowed`
// is refused only when it is a standard one, so the non-standard properties
// §5.3.3 requires CalCard to store and return pass through untouched.
type icalComponentRules struct {
	required []string
	once     []string
	allowed  icalNameSet
	children icalNameSet
}

// icalNameSet is a set of uppercase iCalendar component or property names.
type icalNameSet map[string]struct{}

func (s icalNameSet) contains(name string) bool {
	_, ok := s[name]
	return ok
}

func nameSet(names ...string) icalNameSet {
	set := make(icalNameSet, len(names))
	for _, name := range names {
		set[name] = struct{}{}
	}
	return set
}

var icalObjectComponentRules = map[string]icalComponentRules{
	"VCALENDAR": {
		required: []string{"PRODID", "VERSION"},
		once:     []string{"PRODID", "VERSION", "CALSCALE", "METHOD"},
		allowed:  nameSet("PRODID", "VERSION", "CALSCALE", "METHOD"),
		children: nameSet("VEVENT", "VTODO", "VJOURNAL", "VFREEBUSY", "VTIMEZONE"),
	},
	// §3.6.1: DTSTART is required because a calendar object resource may not
	// specify METHOD (RFC 4791 §4.1), which is the only thing that relaxes it.
	"VEVENT": {
		required: []string{"UID", "DTSTAMP", "DTSTART"},
		once: []string{"UID", "DTSTAMP", "DTSTART", "CLASS", "CREATED", "DESCRIPTION", "GEO",
			"LAST-MODIFIED", "LOCATION", "ORGANIZER", "PRIORITY", "SEQUENCE", "STATUS",
			"SUMMARY", "TRANSP", "URL", "RECURRENCE-ID", "RRULE", "DTEND", "DURATION"},
		allowed: nameSet("UID", "DTSTAMP", "DTSTART", "CLASS", "CREATED", "DESCRIPTION", "GEO",
			"LAST-MODIFIED", "LOCATION", "ORGANIZER", "PRIORITY", "SEQUENCE", "STATUS",
			"SUMMARY", "TRANSP", "URL", "RECURRENCE-ID", "RRULE", "DTEND", "DURATION",
			"ATTACH", "ATTENDEE", "CATEGORIES", "COMMENT", "CONTACT", "EXDATE",
			"REQUEST-STATUS", "RELATED-TO", "RESOURCES", "RDATE"),
		children: nameSet("VALARM"),
	},
	"VTODO": {
		required: []string{"UID", "DTSTAMP"},
		once: []string{"UID", "DTSTAMP", "CLASS", "COMPLETED", "CREATED", "DESCRIPTION",
			"DTSTART", "GEO", "LAST-MODIFIED", "LOCATION", "ORGANIZER", "PERCENT-COMPLETE",
			"PRIORITY", "RECURRENCE-ID", "SEQUENCE", "STATUS", "SUMMARY", "URL", "RRULE", "DUE", "DURATION"},
		allowed: nameSet("UID", "DTSTAMP", "CLASS", "COMPLETED", "CREATED", "DESCRIPTION",
			"DTSTART", "GEO", "LAST-MODIFIED", "LOCATION", "ORGANIZER", "PERCENT-COMPLETE",
			"PRIORITY", "RECURRENCE-ID", "SEQUENCE", "STATUS", "SUMMARY", "URL", "RRULE",
			"DUE", "DURATION", "ATTACH", "ATTENDEE", "CATEGORIES", "COMMENT", "CONTACT",
			"EXDATE", "REQUEST-STATUS", "RELATED-TO", "RESOURCES", "RDATE"),
		children: nameSet("VALARM"),
	},
	"VJOURNAL": {
		required: []string{"UID", "DTSTAMP"},
		once: []string{"UID", "DTSTAMP", "CLASS", "CREATED", "DTSTART", "LAST-MODIFIED",
			"ORGANIZER", "RECURRENCE-ID", "SEQUENCE", "STATUS", "SUMMARY", "URL", "RRULE"},
		allowed: nameSet("UID", "DTSTAMP", "CLASS", "CREATED", "DESCRIPTION", "DTSTART",
			"LAST-MODIFIED", "ORGANIZER", "RECURRENCE-ID", "SEQUENCE", "STATUS", "SUMMARY",
			"URL", "RRULE", "ATTACH", "ATTENDEE", "CATEGORIES", "COMMENT", "CONTACT",
			"EXDATE", "RELATED-TO", "RDATE", "REQUEST-STATUS"),
	},
	"VFREEBUSY": {
		required: []string{"UID", "DTSTAMP"},
		once:     []string{"UID", "DTSTAMP", "CONTACT", "DTSTART", "DTEND", "ORGANIZER", "URL"},
		allowed: nameSet("UID", "DTSTAMP", "CONTACT", "DTSTART", "DTEND", "ORGANIZER", "URL",
			"ATTENDEE", "COMMENT", "FREEBUSY", "REQUEST-STATUS"),
	},
	"VTIMEZONE": {
		required: []string{"TZID"},
		once:     []string{"TZID", "LAST-MODIFIED", "TZURL"},
		allowed:  nameSet("TZID", "LAST-MODIFIED", "TZURL"),
		children: nameSet("STANDARD", "DAYLIGHT"),
	},
	"STANDARD": {
		required: []string{"DTSTART", "TZOFFSETFROM", "TZOFFSETTO"},
		once:     []string{"DTSTART", "TZOFFSETFROM", "TZOFFSETTO", "RRULE"},
		allowed:  nameSet("DTSTART", "TZOFFSETFROM", "TZOFFSETTO", "RRULE", "COMMENT", "RDATE", "TZNAME"),
	},
	"DAYLIGHT": {
		required: []string{"DTSTART", "TZOFFSETFROM", "TZOFFSETTO"},
		once:     []string{"DTSTART", "TZOFFSETFROM", "TZOFFSETTO", "RRULE"},
		allowed:  nameSet("DTSTART", "TZOFFSETFROM", "TZOFFSETTO", "RRULE", "COMMENT", "RDATE", "TZNAME"),
	},
	"VALARM": {
		required: []string{"ACTION", "TRIGGER"},
		once:     []string{"ACTION", "TRIGGER", "DURATION", "REPEAT", "DESCRIPTION", "SUMMARY"},
		allowed:  nameSet("ACTION", "TRIGGER", "DURATION", "REPEAT", "ATTACH", "DESCRIPTION", "SUMMARY", "ATTENDEE"),
	},
}

// validateCalendarObject applies the RFC 4791 §5.3.2.1 preconditions a submitted
// calendar object resource must satisfy before it is stored, in the order the
// §4.1 restrictions and the RFC 5545 content model depend on one another. A nil
// result means the object may be stored as far as its own content is concerned;
// the collection-scoped preconditions stay with the caller, which is the only
// place the target collection is known.
func validateCalendarObject(root *icalNode) *calendarObjectFault {
	// §4.1: the METHOD property makes the object a scheduling message rather
	// than a calendar object resource, whatever else it carries.
	if root.count("METHOD") > 0 {
		return invalidCalendarObjectResource()
	}
	if root.value("VERSION") != "2.0" {
		return invalidCalendarData()
	}
	if calscale := root.value("CALSCALE"); calscale != "" && !strings.EqualFold(calscale, "GREGORIAN") {
		return invalidCalendarData()
	}

	var scheduled []*icalNode
	componentTypes := make(map[string]struct{})
	for _, child := range root.children {
		if _, ok := allowedCalendarComponents[child.name]; !ok && !nonStandardICalendarName(child.name) {
			return &calendarObjectFault{status: http.StatusForbidden, conditions: []string{"supported-calendar-component"}}
		}
		if _, ok := scheduledComponentTypes[child.name]; ok || nonStandardICalendarName(child.name) {
			scheduled = append(scheduled, child)
			componentTypes[child.name] = struct{}{}
		}
	}
	if len(scheduled) == 0 {
		// A body carrying only VTIMEZONE components is well-formed iCalendar but
		// is the VTIMEZONE-only resource §5.2.3 lets CalCard decline; a body
		// carrying no component at all is not iCalendar (RFC 5545 §3.4).
		if root.childCount("VTIMEZONE") > 0 {
			return &calendarObjectFault{status: http.StatusForbidden, conditions: []string{"supported-calendar-component"}}
		}
		return invalidCalendarData()
	}
	// §4.1: one type of calendar component per resource, VTIMEZONE excepted.
	if len(componentTypes) > 1 {
		return invalidCalendarObjectResource()
	}
	if fault := validateRecurrenceSet(scheduled); fault != nil {
		return fault
	}
	if fault := validateTimezoneReferences(root); fault != nil {
		return fault
	}
	if fault := validateComponentContentModel(root); fault != nil {
		return fault
	}
	if fault := validateComponentDateRelationships(root); fault != nil {
		return fault
	}
	if fault := validateCalendarObjectDateValues(root); fault != nil {
		return fault
	}
	return validateRecurrenceIdentities(root, scheduled)
}

// nonStandardICalendarName reports whether name is one RFC 5545 §3.8.8.2 leaves
// to implementations, which the server stores and returns rather than judges.
func nonStandardICalendarName(name string) bool {
	return strings.HasPrefix(name, "X-") && validICalendarToken(name)
}

// validateRecurrenceSet applies the §4.1 rules binding the components of one
// calendar object resource together: every component carries exactly one UID,
// they all carry the same one, and they form a single recurrence set — at most
// one component without a RECURRENCE-ID, and no two overrides for the same
// instance. An object carrying overrides but no master is accepted: §4.1 makes
// that shape valid, and CALDAV:valid-calendar-object-resource admits rejection
// only for the restrictions §4.1 states.
func validateRecurrenceSet(scheduled []*icalNode) *calendarObjectFault {
	uid := ""
	masters := 0
	overrides := make(map[string]struct{}, len(scheduled))
	for _, component := range scheduled {
		switch component.count("UID") {
		case 1:
		case 0:
			return invalidCalendarObjectResource()
		default:
			return invalidCalendarData()
		}
		value := strings.TrimSpace(component.value("UID"))
		if value == "" {
			return invalidCalendarObjectResource()
		}
		if uid == "" {
			uid = value
		} else if value != uid {
			return invalidCalendarObjectResource()
		}

		switch component.count("RECURRENCE-ID") {
		case 0:
			masters++
		case 1:
			recurrenceID := strings.TrimSpace(component.value("RECURRENCE-ID"))
			if _, duplicate := overrides[recurrenceID]; duplicate {
				return invalidCalendarObjectResource()
			}
			overrides[recurrenceID] = struct{}{}
		default:
			return invalidCalendarData()
		}
	}
	if masters > 1 {
		return invalidCalendarObjectResource()
	}
	return nil
}

type recurrenceIdentitySignature struct {
	form icalDateForm
	tzid string
}

// validateRecurrenceIdentities applies the RECURRENCE-ID constraints that need
// parsed DATE values and submitted VTIMEZONE definitions. An override must use
// the same date form and TZID as the recurrence set's DTSTART, and two different
// spellings must not resolve to the same original recurrence instance.
func validateRecurrenceIdentities(root *icalNode, scheduled []*icalNode) *calendarObjectFault {
	if len(scheduled) != 0 && nonStandardICalendarName(scheduled[0].name) {
		return nil
	}
	var master *icalNode
	var overrides []icalProperty
	for _, component := range scheduled {
		recurrenceID, isOverride := firstICalProperty(component, "RECURRENCE-ID")
		if !isOverride {
			master = component
			continue
		}
		overrides = append(overrides, recurrenceID)
	}
	if len(overrides) == 0 {
		return nil
	}

	var expected recurrenceIdentitySignature
	if master != nil {
		dtstart, ok := firstICalProperty(master, "DTSTART")
		if !ok {
			return invalidCalendarData()
		}
		var valid bool
		expected, valid = recurrenceSignature(dtstart)
		if !valid {
			return invalidCalendarData()
		}
	} else {
		var valid bool
		expected, valid = recurrenceSignature(overrides[0])
		if !valid {
			return invalidCalendarData()
		}
	}

	instances := make(map[string]struct{}, len(overrides))
	for _, recurrenceID := range overrides {
		signature, ok := recurrenceSignature(recurrenceID)
		if !ok || signature != expected {
			return invalidCalendarData()
		}
		instants, ok := icalInstants(root, recurrenceID, recurrenceID.value)
		if !ok || len(instants) != 1 {
			return invalidCalendarData()
		}
		identity := instants[0].UTC().Format(time.RFC3339Nano)
		if _, duplicate := instances[identity]; duplicate {
			return invalidCalendarObjectResource()
		}
		instances[identity] = struct{}{}
	}
	return nil
}

func recurrenceSignature(property icalProperty) (recurrenceIdentitySignature, bool) {
	form, ok := parseICalDateForm(property.value)
	if !ok || !validICalDateValue(property, property.value) {
		return recurrenceIdentitySignature{}, false
	}
	return recurrenceIdentitySignature{
		form: form,
		tzid: strings.TrimSpace(property.parameters["TZID"]),
	}, true
}

// validateTimezoneReferences applies the §4.1 rule that a VTIMEZONE component is
// present for each unique TZID parameter value the object uses. A VTIMEZONE the
// object does not reference is left alone: §4.1 constrains the references, not
// the library of definitions a client chooses to ship with them.
func validateTimezoneReferences(root *icalNode) *calendarObjectFault {
	defined := make(map[string]struct{}, len(root.children))
	for _, child := range root.children {
		if child.name != "VTIMEZONE" {
			continue
		}
		tzid := strings.TrimSpace(child.value("TZID"))
		if tzid == "" {
			return invalidCalendarData()
		}
		if _, duplicate := defined[tzid]; duplicate {
			return invalidCalendarObjectResource()
		}
		defined[tzid] = struct{}{}
	}

	missing := false
	walkICalNodes(root, func(node *icalNode) {
		if node.name == "VTIMEZONE" || node.name == "STANDARD" || node.name == "DAYLIGHT" {
			return
		}
		for _, property := range node.properties {
			tzid, ok := property.parameters["TZID"]
			if !ok {
				continue
			}
			if _, defined := defined[strings.TrimSpace(tzid)]; !defined {
				missing = true
			}
		}
	})
	if missing {
		return invalidCalendarObjectResource()
	}
	return nil
}

func walkICalNodes(node *icalNode, visit func(*icalNode)) {
	visit(node)
	for _, child := range node.children {
		walkICalNodes(child, visit)
	}
}

// validateComponentContentModel checks every component against the RFC 5545 §3.6
// model for its type: the sub-components it may contain, the standard properties
// it admits, those it requires, and those that may not repeat.
func validateComponentContentModel(node *icalNode) *calendarObjectFault {
	if nonStandardICalendarName(node.name) {
		return nil
	}
	rules, known := icalObjectComponentRules[node.name]
	if !known {
		return invalidCalendarData()
	}
	for _, name := range rules.required {
		if node.count(name) == 0 {
			return invalidCalendarData()
		}
	}
	for _, name := range rules.once {
		if node.count(name) > 1 {
			return invalidCalendarData()
		}
	}
	for _, property := range node.properties {
		if (node.name == "VTIMEZONE" || node.name == "STANDARD" || node.name == "DAYLIGHT") &&
			!validCalendarTimezoneProperty(node.name, property) {
			return invalidCalendarData()
		}
		if _, ok := rules.allowed[property.name]; ok {
			continue
		}
		if _, standard := knownICalendarProperties[property.name]; standard {
			return invalidCalendarData()
		}
	}
	if fault := validateComponentPairs(node); fault != nil {
		return fault
	}
	for _, child := range node.children {
		if _, ok := rules.children[child.name]; !ok && !nonStandardICalendarName(child.name) {
			return invalidCalendarData()
		}
		if fault := validateComponentContentModel(child); fault != nil {
			return fault
		}
	}
	if node.name == "VTIMEZONE" && node.childCount("STANDARD")+node.childCount("DAYLIGHT") == 0 {
		return invalidCalendarData()
	}
	return nil
}

// validateComponentPairs applies the RFC 5545 rules that bind two properties of
// one component together rather than constraining either on its own.
func validateComponentPairs(node *icalNode) *calendarObjectFault {
	for _, property := range node.properties {
		switch property.name {
		case "DURATION":
			if duration, ok := ical.ParseDuration(property.value); !ok || duration <= 0 {
				return invalidCalendarData()
			}
		case "RRULE":
			valid := ical.ValidRecurrenceRule(property.value)
			if node.name == "STANDARD" || node.name == "DAYLIGHT" {
				valid = validTimezoneRecurrenceRule(property.value)
			}
			if !valid {
				return invalidCalendarData()
			}
		case "REPEAT":
			if repeat, err := strconv.Atoi(strings.TrimSpace(property.value)); err != nil || repeat < 0 {
				return invalidCalendarData()
			}
		}
	}
	switch node.name {
	case "VEVENT":
		// §3.6.1: DTEND and DURATION are alternative ways to say the same thing.
		if node.count("DTEND") > 0 && node.count("DURATION") > 0 {
			return invalidCalendarData()
		}
	case "VTODO":
		// §3.6.2: DUE and DURATION are alternatives, and DURATION only means
		// something relative to a DTSTART.
		if node.count("DUE") > 0 && node.count("DURATION") > 0 {
			return invalidCalendarData()
		}
		if node.count("DURATION") > 0 && node.count("DTSTART") == 0 {
			return invalidCalendarData()
		}
	case "VALARM":
		// §3.6.6: DURATION and REPEAT are specified together or not at all, and
		// each ACTION requires the properties that carry out that action.
		if (node.count("DURATION") > 0) != (node.count("REPEAT") > 0) {
			return invalidCalendarData()
		}
		action := strings.ToUpper(strings.TrimSpace(node.value("ACTION")))
		switch action {
		case "DISPLAY":
			if node.count("DESCRIPTION") != 1 {
				return invalidCalendarData()
			}
		case "EMAIL":
			if node.count("DESCRIPTION") != 1 || node.count("SUMMARY") != 1 || node.count("ATTENDEE") == 0 {
				return invalidCalendarData()
			}
		case "AUDIO":
			if node.count("ATTACH") > 1 {
				return invalidCalendarData()
			}
		}
		if !alarmPropertiesMatchAction(node, action) {
			return invalidCalendarData()
		}
	}
	return nil
}

// validateComponentDateRelationships applies the component-specific constraints
// that bind DTSTART to an optional end property. These cannot be decided by the
// generic DATE/DATE-TIME parser because the same property has different rules in
// VEVENT, VTODO, and VFREEBUSY.
func validateComponentDateRelationships(root *icalNode) *calendarObjectFault {
	var fault *calendarObjectFault
	walkICalNodes(root, func(node *icalNode) {
		if fault != nil {
			return
		}
		switch node.name {
		case "VEVENT":
			if !validOrderedComponentDates(root, node, "DTSTART", "DTEND", false) {
				fault = invalidCalendarData()
			}
		case "VTODO":
			if !validOrderedComponentDates(root, node, "DTSTART", "DUE", false) {
				fault = invalidCalendarData()
			}
		case "VFREEBUSY":
			if !validOrderedComponentDates(root, node, "DTSTART", "DTEND", true) {
				fault = invalidCalendarData()
			}
		}
	})
	return fault
}

func validOrderedComponentDates(root, node *icalNode, startName, endName string, utcOnly bool) bool {
	start, hasStart := firstICalProperty(node, startName)
	end, hasEnd := firstICalProperty(node, endName)

	startForm, startInstant, ok := componentDate(root, start, hasStart, utcOnly)
	if !ok {
		return false
	}
	endForm, endInstant, ok := componentDate(root, end, hasEnd, utcOnly)
	if !ok {
		return false
	}
	if !hasStart || !hasEnd {
		return true
	}
	if (startForm == icalDateOnly) != (endForm == icalDateOnly) {
		return false
	}
	return endInstant.After(startInstant)
}

func componentDate(root *icalNode, property icalProperty, present, utcOnly bool) (icalDateForm, time.Time, bool) {
	if !present {
		return icalDateOnly, time.Time{}, true
	}
	form, ok := parseICalDateForm(property.value)
	if !ok || !validICalDateValue(property, property.value) || (utcOnly && form != icalUTCDateTime) {
		return icalDateOnly, time.Time{}, false
	}
	instants, ok := icalInstants(root, property, property.value)
	if !ok || len(instants) != 1 {
		return icalDateOnly, time.Time{}, false
	}
	return form, instants[0], true
}

func alarmPropertiesMatchAction(node *icalNode, action string) bool {
	var allowed icalNameSet
	switch action {
	case "AUDIO":
		allowed = nameSet("ACTION", "TRIGGER", "DURATION", "REPEAT", "ATTACH")
	case "DISPLAY":
		allowed = nameSet("ACTION", "TRIGGER", "DURATION", "REPEAT", "DESCRIPTION")
	case "EMAIL":
		allowed = nameSet("ACTION", "TRIGGER", "DURATION", "REPEAT", "DESCRIPTION", "SUMMARY", "ATTENDEE", "ATTACH")
	default:
		return true
	}
	for _, property := range node.properties {
		if allowed.contains(property.name) {
			continue
		}
		if _, standard := knownICalendarProperties[property.name]; standard {
			return false
		}
	}
	return true
}

// icalDateForm is which of the three RFC 5545 §3.3.4 and §3.3.5 spellings a
// value uses. There is no fourth: a numeric UTC offset is not a legal iCalendar
// DATE-TIME however readily a tolerant parser accepts one.
type icalDateForm int

const (
	icalDateOnly icalDateForm = iota
	icalFloatingDateTime
	icalUTCDateTime
)

func parseICalDateForm(value string) (icalDateForm, bool) {
	switch {
	case len(value) == len("20060102"):
		_, err := ical.ParseDateTime(value)
		return icalDateOnly, err == nil
	case len(value) == len("20060102T150405"):
		_, err := ical.ParseDateTime(value)
		return icalFloatingDateTime, err == nil
	case len(value) == len("20060102T150405Z") && strings.HasSuffix(value, "Z"):
		_, err := ical.ParseDateTime(value)
		return icalUTCDateTime, err == nil
	}
	return icalDateOnly, false
}

// utcOnlyDateProperties are the properties RFC 5545 defines as "date with UTC
// time" outright, so neither a DATE nor a floating DATE-TIME satisfies them.
var utcOnlyDateProperties = nameSet("DTSTAMP", "CREATED", "LAST-MODIFIED", "COMPLETED")

// localDateProperties are the properties whose value is a DATE or a DATE-TIME in
// any of the three forms, the TZID parameter deciding which zone a floating one
// names.
var localDateProperties = nameSet("DTSTART", "DTEND", "DUE", "RECURRENCE-ID")

// validateCalendarObjectDateValues checks the form of every DATE and DATE-TIME
// the object carries and bounds each one by the collection's CALDAV:min-date-time
// and CALDAV:max-date-time. §5.2.6 and §5.2.7 make both limits inclusive, so a
// value exactly equal to either is stored.
//
// VTIMEZONE observances are excluded. The limits bound the data a user schedules;
// an observance start is reference data a client ships with it, and the values in
// circulation reach back to 1601, which no useful minimum would admit.
func validateCalendarObjectDateValues(root *icalNode) *calendarObjectFault {
	minDate, maxDate := ical.DateLimits()
	var fault *calendarObjectFault
	for _, child := range root.children {
		if child.name == "VTIMEZONE" {
			continue
		}
		walkICalNodes(child, func(node *icalNode) {
			if fault != nil {
				return
			}
			for _, property := range node.properties {
				values, ok := icalPropertyDateValues(root, node, property)
				if !ok {
					if fault == nil {
						fault = invalidCalendarData()
					}
					return
				}
				for _, value := range values {
					if value.Before(minDate) && fault == nil {
						fault = &calendarObjectFault{status: http.StatusForbidden, conditions: []string{"min-date-time"}}
					}
					if value.After(maxDate) && fault == nil {
						fault = &calendarObjectFault{status: http.StatusForbidden, conditions: []string{"max-date-time"}}
					}
				}
			}
		})
		if fault != nil {
			return fault
		}
	}
	return nil
}

// icalPropertyDateValues returns the instants a property carries, or false when
// its value is not a legal spelling of the type RFC 5545 gives that property. A
// property that carries no DATE or DATE-TIME at all yields no values and no
// error.
func icalPropertyDateValues(root, node *icalNode, property icalProperty) ([]time.Time, bool) {
	switch {
	case utcOnlyDateProperties.contains(property.name):
		form, ok := parseICalDateForm(property.value)
		if !ok || form != icalUTCDateTime || !validICalDateValue(property, property.value) {
			return nil, false
		}
		return icalInstants(root, property, property.value)
	case localDateProperties.contains(property.name):
		if !validICalDateValue(property, property.value) {
			return nil, false
		}
		return icalInstants(root, property, property.value)
	case property.name == "RRULE":
		return icalRecurrenceUntil(root, node, property)
	case property.name == "EXDATE":
		return icalDateList(root, property)
	case property.name == "RDATE":
		if strings.EqualFold(property.parameters["VALUE"], "PERIOD") {
			// §3.8.5.2: an RDATE period starts where the property's own TZID
			// says, so its start follows the same rules as a DTSTART.
			return icalPeriodList(root, property, false)
		}
		return icalDateList(root, property)
	case property.name == "FREEBUSY" && node.name == "VFREEBUSY":
		// §3.8.2.6: a FREEBUSY period is stated in UTC.
		return icalPeriodList(root, property, true)
	case property.name == "TRIGGER" && node.name == "VALARM":
		return icalTriggerValue(root, property)
	}
	if _, standard := knownICalendarProperties[property.name]; !standard {
		switch strings.ToUpper(strings.TrimSpace(property.parameters["VALUE"])) {
		case "DATE", "DATE-TIME":
			return icalDateList(root, property)
		}
	}
	return nil, true
}

// icalRecurrenceUntil returns the explicit DATE or DATE-TIME carried by an
// RRULE's UNTIL part. Generated recurrence instances are deliberately absent:
// RFC 4791 bounds submitted values while allowing unoverridden instances to
// fall outside the advertised date range.
func icalRecurrenceUntil(root, node *icalNode, property icalProperty) ([]time.Time, bool) {
	until := ""
	for _, part := range strings.Split(property.value, ";") {
		name, value, found := strings.Cut(part, "=")
		if found && strings.EqualFold(strings.TrimSpace(name), "UNTIL") {
			until = strings.TrimSpace(value)
			break
		}
	}
	if until == "" {
		return nil, true
	}

	dtstart, ok := firstICalProperty(node, "DTSTART")
	if !ok {
		return nil, false
	}
	dtstartForm, ok := parseICalDateForm(dtstart.value)
	if !ok {
		return nil, false
	}
	untilForm, ok := parseICalDateForm(until)
	if !ok {
		return nil, false
	}

	// RFC 5545 §3.3.10 ties UNTIL's representation to DTSTART: DATE uses
	// DATE, floating time uses floating time, and UTC or TZID-qualified time
	// uses UTC. VTIMEZONE observances are excluded before this helper is called.
	switch {
	case dtstartForm == icalDateOnly:
		if untilForm != icalDateOnly {
			return nil, false
		}
	case dtstartForm == icalUTCDateTime || strings.TrimSpace(dtstart.parameters["TZID"]) != "":
		if untilForm != icalUTCDateTime {
			return nil, false
		}
	case untilForm != icalFloatingDateTime:
		return nil, false
	}

	parsed, ok := icalInstants(root, icalProperty{keyPart: "UNTIL"}, until)
	if !ok || len(parsed) != 1 {
		return nil, false
	}
	return parsed, true
}

// validICalDateValue applies the RFC 5545 §3.2.19 rules on how a TZID parameter
// may combine with the value it qualifies: never with a DATE, and never with a
// value already stated in UTC.
func validICalDateValue(property icalProperty, value string) bool {
	form, ok := parseICalDateForm(value)
	if !ok {
		return false
	}
	if declared, present := property.parameters["VALUE"]; present {
		switch strings.ToUpper(strings.TrimSpace(declared)) {
		case "DATE":
			if form != icalDateOnly {
				return false
			}
		case "DATE-TIME":
			if form == icalDateOnly {
				return false
			}
		default:
			return false
		}
	} else if form == icalDateOnly {
		// §3.3.5: a DATE value has to say so, since DATE-TIME is the default.
		return false
	}
	if _, zoned := property.parameters["TZID"]; zoned && form != icalFloatingDateTime {
		return false
	}
	return true
}

// icalInstants resolves one already-validated value to the instant the limits
// are compared against. A TZID uses the VTIMEZONE shipped in this object; other
// values use the shared DATE/DATE-TIME parser.
func icalInstants(root *icalNode, property icalProperty, value string) ([]time.Time, bool) {
	if tzid := strings.TrimSpace(property.parameters["TZID"]); tzid != "" {
		wall, err := ical.ParseDateTime(value)
		if err != nil {
			return nil, false
		}
		offset, ok := submittedTimezoneOffset(root, tzid, wall)
		if !ok {
			return nil, false
		}
		return []time.Time{wall.Add(-offset).UTC()}, true
	}
	parsed, ok := ical.ParsePropertyDateTimeLocal(property.keyPart, value)
	if !ok {
		return nil, false
	}
	return []time.Time{parsed}, true
}

func submittedTimezoneOffset(root *icalNode, tzid string, wall time.Time) (time.Duration, bool) {
	var timezone *icalNode
	for _, child := range root.children {
		if child.name == "VTIMEZONE" && strings.TrimSpace(child.value("TZID")) == tzid {
			timezone = child
			break
		}
	}
	if timezone == nil {
		return 0, false
	}

	var latest time.Time
	var latestOffset time.Duration
	var earliest time.Time
	var earliestOffsetFrom time.Duration
	for _, observance := range timezone.children {
		if observance.name != "STANDARD" && observance.name != "DAYLIGHT" {
			continue
		}
		dtstartProperty, ok := firstICalProperty(observance, "DTSTART")
		if !ok {
			return 0, false
		}
		dtstart, err := ical.ParseDateTime(dtstartProperty.value)
		if err != nil {
			return 0, false
		}
		offsetFrom, ok := parseUTCOffsetDuration(observance.value("TZOFFSETFROM"))
		if !ok {
			return 0, false
		}
		offsetTo, ok := parseUTCOffsetDuration(observance.value("TZOFFSETTO"))
		if !ok {
			return 0, false
		}
		if earliest.IsZero() || dtstart.Before(earliest) {
			earliest = dtstart
			earliestOffsetFrom = offsetFrom
		}

		consider := func(candidate time.Time) {
			if candidate.After(wall) {
				return
			}
			if latest.IsZero() || candidate.After(latest) {
				latest = candidate
				latestOffset = offsetTo
			}
		}
		consider(dtstart)
		for _, property := range observance.properties {
			switch property.name {
			case "RDATE":
				for _, value := range strings.Split(property.value, ",") {
					candidate, err := ical.ParseDateTime(strings.TrimSpace(value))
					if err != nil {
						return 0, false
					}
					consider(candidate)
				}
			case "RRULE":
				candidate, found := ical.LatestRecurrenceOnOrBefore(dtstart, wall, property.value)
				if found {
					consider(candidate)
				}
			}
		}
	}
	if !latest.IsZero() {
		return latestOffset, true
	}
	if !earliest.IsZero() {
		return earliestOffsetFrom, true
	}
	return 0, false
}

func firstICalProperty(node *icalNode, name string) (icalProperty, bool) {
	for _, property := range node.properties {
		if property.name == name {
			return property, true
		}
	}
	return icalProperty{}, false
}

func parseUTCOffsetDuration(value string) (time.Duration, bool) {
	value = strings.TrimSpace(value)
	if !validUTCOffset(value) {
		return 0, false
	}
	digits := value[1:]
	hours, _ := strconv.Atoi(digits[:2])
	minutes, _ := strconv.Atoi(digits[2:4])
	seconds := 0
	if len(digits) == 6 {
		seconds, _ = strconv.Atoi(digits[4:6])
	}
	offset := time.Duration(hours)*time.Hour + time.Duration(minutes)*time.Minute + time.Duration(seconds)*time.Second
	if value[0] == '-' {
		offset = -offset
	}
	return offset, true
}

func icalDateList(root *icalNode, property icalProperty) ([]time.Time, bool) {
	parts := strings.Split(property.value, ",")
	instants := make([]time.Time, 0, len(parts))
	for _, part := range parts {
		part = strings.TrimSpace(part)
		if !validICalDateValue(property, part) {
			return nil, false
		}
		parsed, ok := icalInstants(root, property, part)
		if !ok {
			return nil, false
		}
		instants = append(instants, parsed...)
	}
	return instants, true
}

// icalPeriodList reads an RFC 5545 §3.3.9 PERIOD list: each period is an explicit
// pair of date-times or a start and a duration. utcOnly is set for the properties
// whose definition fixes the period in UTC rather than letting a TZID place it.
func icalPeriodList(root *icalNode, property icalProperty, utcOnly bool) ([]time.Time, bool) {
	validStart := func(value string) bool {
		form, ok := parseICalDateForm(value)
		if !ok || form == icalDateOnly {
			return false
		}
		if utcOnly {
			_, zoned := property.parameters["TZID"]
			return form == icalUTCDateTime && !zoned
		}
		_, zoned := property.parameters["TZID"]
		return !zoned || form == icalFloatingDateTime
	}

	parts := strings.Split(property.value, ",")
	instants := make([]time.Time, 0, len(parts))
	for _, part := range parts {
		start, end, found := strings.Cut(strings.TrimSpace(part), "/")
		if !found || !validStart(start) {
			return nil, false
		}
		parsed, ok := icalInstants(root, property, start)
		if !ok || len(parsed) != 1 {
			return nil, false
		}
		instants = append(instants, parsed...)
		if _, isDate := parseICalDateForm(end); isDate {
			if !validStart(end) {
				return nil, false
			}
			parsedEnd, ok := icalInstants(root, property, end)
			if !ok || len(parsedEnd) != 1 || !parsedEnd[0].After(parsed[0]) {
				return nil, false
			}
			instants = append(instants, parsedEnd...)
			continue
		}
		if duration, ok := ical.ParseDuration(end); !ok || duration <= 0 {
			return nil, false
		}
	}
	return instants, true
}

// icalTriggerValue reads the two RFC 5545 §3.8.6.3 spellings of a TRIGGER: a
// duration relative to the enclosing component, which carries no date, or the
// absolute form, which must be a date with UTC time.
func icalTriggerValue(root *icalNode, property icalProperty) ([]time.Time, bool) {
	if strings.EqualFold(property.parameters["VALUE"], "DATE-TIME") {
		form, ok := parseICalDateForm(property.value)
		if !ok || form != icalUTCDateTime {
			return nil, false
		}
		return icalInstants(root, property, property.value)
	}
	if _, present := property.parameters["RELATED"]; present {
		if _, ok := ical.ParseDuration(property.value); !ok {
			return nil, false
		}
		return nil, true
	}
	if _, ok := ical.ParseDuration(property.value); ok {
		return nil, true
	}
	return nil, false
}
