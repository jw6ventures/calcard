package dav

import (
	"fmt"
	"maps"
	"net/http"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/jw6ventures/calcard/internal/calobject"
	"github.com/jw6ventures/calcard/internal/ical"
)

// icalNode is one component of a parsed calendar object resource: the content
// lines it declares and the components nested inside it, both in document order.
type icalNode struct {
	name       string
	properties []icalProperty
	children   []*icalNode
	// zones holds, on the root of a parsed object, the zones its VTIMEZONEs
	// define, read once on first use by submittedZones. Nothing else in a tree
	// changes once it is parsed, so a tree may be read concurrently.
	zonesOnce sync.Once
	zones     *submittedZones
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
// root. Structure is not forgiven: RFC 4791 §5.3.2.1 makes a PUT whose body is
// not valid for its media type a precondition failure, so a line that is not a
// content line, or a BEGIN and END that do not pair up, is an error here rather
// than something to skip past. What calendar applications write inside sound
// structure is read: a property after a sub-component belongs to the enclosing
// component, as the recurrence expansion's own line reader also takes it, and
// parameters are read as parseICalProperty describes.
func parseICalendarObject(raw string) (*icalNode, error) {
	return parseICalendarTree(ical.UnfoldLines(raw), readSubmitted)
}

// parseStoredICalendarObject reads a calendar object resource the store already
// holds, for a report to match, project or derive free-busy time from. The
// store may hold objects whose only guarantee is balanced BEGIN and END lines,
// and those are resources GET and sync-collection list like any other. A
// report that could not read one would answer as though the resource held
// nothing -- calendar-query would leave it out of the result a sync client
// reconciles its local copies against, and free-busy would publish its time as
// free -- so on top of what parseICalendarObject reads, this reads past every
// defect it can recover from:
//   - a control character is read as written, a parameter without a valid
//     name or an "=" is left out, and a repeated parameter keeps its first
//     value whatever the repeats say;
//   - a line that is not a content line at all is skipped;
//   - an END closes the innermost open component of that name and everything
//     opened inside it, an END naming no open component is skipped, and
//     components still open when the object ends are closed there;
//   - a further VCALENDAR, whether after the first or nested inside it,
//     continues the first, and content outside any VCALENDAR is skipped.
//
// It fails only when there is no VCALENDAR to read. On a well-formed object it
// builds the tree parseICalendarObject does.
func parseStoredICalendarObject(raw string) (*icalNode, error) {
	return parseICalendarTree(ical.UnfoldLines(raw), readStored)
}

// parseICalendarTree reads lines as parseICalendarObject does, or, for
// readStored, as parseStoredICalendarObject does. Each content line is read as
// readICalProperty reads it.
func parseICalendarTree(lines []string, reading icalReading) (*icalNode, error) {
	stored := reading == readStored
	return ical.ReadComponents(lines, !stored,
		func(parent *icalNode, name string) (*icalNode, error) {
			if !stored && !validICalendarToken(name) {
				return nil, fmt.Errorf("invalid component name %q", name)
			}
			node := &icalNode{name: name}
			if parent != nil {
				parent.children = append(parent.children, node)
			}
			return node, nil
		},
		func(node *icalNode, line string) error {
			property, ok := readICalProperty(line, reading)
			if !ok {
				if stored {
					return nil
				}
				return fmt.Errorf("malformed content line")
			}
			node.properties = append(node.properties, property)
			return nil
		})
}

// calendarObjectFault is a failed RFC 4791 §5.3.2.1 precondition together with
// the status §1.3 assigns it: 403 when repeating the request can only fail
// again, 409 when the user can resolve the conflict and resubmit. cause, when
// set, is the specific reason a caller outside DAV can test for; the DAV
// response is decided by status and conditions alone.
type calendarObjectFault struct {
	status     int
	conditions []string
	cause      error
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
//
// The model is RFC 5545's where departing from it would change an answer, and
// what calendar applications write where it would not. DTSTAMP is not required,
// since nothing the server answers reads it; withRequiredCalendarProperties
// adds it to an accepted object that lacks it. A scheduling component holds a
// property to one exactly when the server reads that property to answer: to
// identify the component (UID, RECURRENCE-ID), to place it in time (DTSTART,
// DTEND, DUE, DURATION, RRULE, and the COMPLETED and CREATED RFC 4791 §9.9
// places a VTODO by), or to decide whether that time is busy or there at all
// (STATUS, which free-busy and the recurrence expansion read, and an event's
// TRANSP). A second one would leave the matcher, the free-busy report and the
// recurrence expansion free to read different ones, while a second DESCRIPTION
// or SUMMARY changes nothing they answer. VCALENDAR, VTIMEZONE and its
// observances, and VALARM keep the limits RFC 5545 and RFC 9074 set.
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
	// Nothing the server answers reads PRODID, so like DTSTAMP it is not
	// required, and validateCalendarObject decides VERSION itself;
	// withRequiredCalendarProperties adds either to an accepted object that
	// lacks it. RFC 7986 §4 adds the properties of the calendar as a whole.
	// Those RFC 5545 defines are admitted here: UID, which names the calendar
	// an object came from rather than the resource -- the RFC 4791 §4.1 UID of
	// a resource is its components' -- URL and LAST-MODIFIED once each, and
	// DESCRIPTION and CATEGORIES. NAME, COLOR, SOURCE, REFRESH-INTERVAL and
	// IMAGE are names RFC 5545 leaves to extensions, which pass as written.
	"VCALENDAR": {
		once: []string{"PRODID", "VERSION", "CALSCALE", "METHOD", "UID", "LAST-MODIFIED", "URL"},
		allowed: nameSet("PRODID", "VERSION", "CALSCALE", "METHOD", "UID", "LAST-MODIFIED", "URL",
			"DESCRIPTION", "CATEGORIES"),
		children: nameSet("VEVENT", "VTODO", "VJOURNAL", "VFREEBUSY", "VTIMEZONE"),
	},
	// §3.6.1: DTSTART is required because a calendar object resource may not
	// specify METHOD (RFC 4791 §4.1), which is the only thing that relaxes it.
	"VEVENT": {
		required: []string{"UID", "DTSTART"},
		once:     []string{"UID", "DTSTART", "DTEND", "DURATION", "RECURRENCE-ID", "RRULE", "STATUS", "TRANSP"},
		allowed: nameSet("UID", "DTSTAMP", "DTSTART", "CLASS", "CREATED", "DESCRIPTION", "GEO",
			"LAST-MODIFIED", "LOCATION", "ORGANIZER", "PRIORITY", "SEQUENCE", "STATUS",
			"SUMMARY", "TRANSP", "URL", "RECURRENCE-ID", "RRULE", "DTEND", "DURATION",
			"ATTACH", "ATTENDEE", "CATEGORIES", "COMMENT", "CONTACT", "EXDATE",
			"REQUEST-STATUS", "RELATED-TO", "RESOURCES", "RDATE"),
		children: nameSet("VALARM"),
	},
	// Some task applications write DTEND on a VTODO. §9.9 places a VTODO by
	// DTSTART, DUE and DURATION alone, so it is stored and read by nothing.
	"VTODO": {
		required: []string{"UID"},
		once: []string{"UID", "DTSTART", "DUE", "DURATION", "RECURRENCE-ID", "RRULE", "STATUS",
			"COMPLETED", "CREATED"},
		allowed: nameSet("UID", "DTSTAMP", "CLASS", "COMPLETED", "CREATED", "DESCRIPTION",
			"DTSTART", "GEO", "LAST-MODIFIED", "LOCATION", "ORGANIZER", "PERCENT-COMPLETE",
			"PRIORITY", "RECURRENCE-ID", "SEQUENCE", "STATUS", "SUMMARY", "URL", "RRULE",
			"DUE", "DURATION", "ATTACH", "ATTENDEE", "CATEGORIES", "COMMENT", "CONTACT",
			"EXDATE", "REQUEST-STATUS", "RELATED-TO", "RESOURCES", "RDATE", "DTEND"),
		children: nameSet("VALARM"),
	},
	"VJOURNAL": {
		required: []string{"UID"},
		once:     []string{"UID", "DTSTART", "RECURRENCE-ID", "RRULE", "STATUS"},
		allowed: nameSet("UID", "DTSTAMP", "CLASS", "CREATED", "DESCRIPTION", "DTSTART",
			"LAST-MODIFIED", "ORGANIZER", "RECURRENCE-ID", "SEQUENCE", "STATUS", "SUMMARY",
			"URL", "RRULE", "ATTACH", "ATTENDEE", "CATEGORIES", "COMMENT", "CONTACT",
			"EXDATE", "RELATED-TO", "RDATE", "REQUEST-STATUS"),
	},
	"VFREEBUSY": {
		required: []string{"UID"},
		once:     []string{"UID", "DTSTART", "DTEND"},
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
		once: []string{"ACTION", "TRIGGER", "DURATION", "REPEAT", "DESCRIPTION", "SUMMARY",
			"UID", "ACKNOWLEDGED", "PROXIMITY"},
		allowed: nameSet("ACTION", "TRIGGER", "DURATION", "REPEAT", "ATTACH", "DESCRIPTION", "SUMMARY", "ATTENDEE",
			"UID", "RELATED-TO", "ACKNOWLEDGED", "PROXIMITY"),
	},
}

// alarmExtensionProperties are the properties RFC 9074 adds to every VALARM
// whatever its ACTION. Apple Calendar writes UID on each alarm it saves.
var alarmExtensionProperties = nameSet("UID", "RELATED-TO", "ACKNOWLEDGED", "PROXIMITY")

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
		fault := invalidCalendarObjectResource()
		fault.cause = calobject.ErrSchedulingMessage
		return fault
	}
	// VERSION is how vCalendar 1.0, a different format, is told from
	// iCalendar, so a version other than 2.0 is refused. An object that leaves
	// VERSION out is read as iCalendar: scripts write such objects, and no
	// other format starts with BEGIN:VCALENDAR without saying which it is.
	if version, present := firstICalProperty(root, "VERSION"); present && strings.TrimSpace(version.value) != "2.0" {
		return invalidCalendarData()
	}
	if calscale := strings.TrimSpace(root.value("CALSCALE")); calscale != "" && !strings.EqualFold(calscale, "GREGORIAN") {
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
			fault := invalidCalendarObjectResource()
			fault.cause = calobject.ErrMultipleUIDs
			return fault
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
//
// A TZID the host's time zone database defines is admitted without one, and
// withHostTimezones stores the host's definition with the object unless the
// name is one of UTC itself (utcTZID). Plenty of clients and scripts name an
// IANA zone and ship no definition, so refusing it would lose the event without
// making any answer more exact. A name neither the object nor the host defines
// places nothing and is refused.
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

	// Each name is looked up once, and the first the host lacks decides.
	missing := false
	walkICalNodes(root, func(node *icalNode) {
		if missing || node.name == "VTIMEZONE" || node.name == "STANDARD" || node.name == "DAYLIGHT" {
			return
		}
		for _, property := range node.properties {
			tzid, ok := property.parameters["TZID"]
			if !ok {
				continue
			}
			tzid = strings.TrimSpace(tzid)
			if _, checked := defined[tzid]; checked {
				continue
			}
			if _, known := ical.HostLocation(tzid); !known {
				missing = true
				return
			}
			defined[tzid] = struct{}{}
		}
	})
	if missing {
		fault := invalidCalendarObjectResource()
		fault.cause = calobject.ErrUndefinedTimezone
		return fault
	}
	return nil
}

// bodyEdit replaces raw[start:end] with text: an insertion where start and end
// are equal, a removal where text is empty.
type bodyEdit struct {
	start, end int
	text       string
}

func editsDelta(edits []bodyEdit) int64 {
	var delta int64
	for _, edit := range edits {
		delta += int64(len(edit.text) - (edit.end - edit.start))
	}
	return delta
}

// applyBodyEdits splices edits, which do not overlap, into raw. Edits starting
// at one offset are applied in the order given.
func applyBodyEdits(raw string, edits []bodyEdit) string {
	if len(edits) == 0 {
		return raw
	}
	slices.SortStableFunc(edits, func(a, b bodyEdit) int { return a.start - b.start })
	var out strings.Builder
	out.Grow(len(raw) + int(max(editsDelta(edits), 0)))
	cursor := 0
	for _, edit := range edits {
		if edit.start > cursor {
			out.WriteString(raw[cursor:edit.start])
		}
		out.WriteString(edit.text)
		cursor = max(cursor, edit.end)
	}
	out.WriteString(raw[cursor:])
	return out.String()
}

// completeCalendarObject is the body a submitted object that the validator
// accepted is stored as: raw, whose unfolded content lines are lines and whose
// components are root, with the edits oneDayDateEndEdits,
// requiredPropertyEdits and hostTimezoneEdits describe, the latter two each
// made only while the object stays within limit octets, so a client can always
// write back what it read. Everything else in raw is kept octet for octet.
// generate has ical.VTimezone's contract, and countedEnd is as
// hostTimezoneSpans reads it.
func completeCalendarObject(raw string, lines []string, root *icalNode, countedEnd, now time.Time, generate func(tzid string, first, last time.Time) (string, bool), limit int64) string {
	offsets, ok := logicalLineOffsets(raw, lines)
	if !ok {
		return raw
	}
	edits := oneDayDateEndEdits(raw, lines, offsets)
	size := int64(len(raw)) + editsDelta(edits)
	if required := requiredPropertyEdits(raw, lines, offsets, now); size+editsDelta(required) <= limit {
		edits = append(edits, required...)
		size += editsDelta(required)
	}
	if zones := hostTimezoneEdits(raw, lines, offsets, root, countedEnd, generate); size+editsDelta(zones) <= limit {
		edits = append(edits, zones...)
	}
	return applyBodyEdits(raw, edits)
}

// calDAVServerProdID is the PRODID of the iCalendar objects the server writes.
const calDAVServerProdID = "-//CalCard//CalDAV Server//EN"

// requiredPropertyEdits adds the properties RFC 5545 requires that the
// validator does not: VERSION and PRODID of the VCALENDAR (§3.6), in that
// order after its BEGIN line, and DTSTAMP of each VEVENT, VTODO, VJOURNAL and
// VFREEBUSY, after the component's UID. Outside scheduling, §3.8.7.2 makes
// DTSTAMP equivalent to LAST-MODIFIED, so the stamp is the component's
// LAST-MODIFIED, else its CREATED, else now: an object that says when it was
// changed is stored the same way however often it is written. A property the
// object carries, whatever its value, is left as it is. Each inserted line ends
// as the line before it does.
func requiredPropertyEdits(raw string, lines []string, offsets []int, now time.Time) []bodyEdit {
	type insertion struct {
		after       int
		name, value string
	}
	var insertions []insertion
	stamp := now.UTC().Format("20060102T150405Z")
	depth, calendarBegin := 0, -1
	hasVersion, hasProdID := false, false
	type schedulingComponent struct {
		begin, uid            int
		stamped               bool
		lastModified, created string
	}
	var component *schedulingComponent
	for i, line := range lines {
		if strings.TrimSpace(line) == "" {
			continue
		}
		switch {
		case ical.HasPrefixFold(line, "BEGIN:"):
			depth++
			_, scheduled := scheduledComponentTypes[strings.ToUpper(strings.TrimSpace(line[len("BEGIN:"):]))]
			switch {
			case depth == 1:
				calendarBegin = i
			case depth == 2 && scheduled:
				component = &schedulingComponent{begin: i, uid: -1}
			}
		case ical.HasPrefixFold(line, "END:"):
			if depth == 2 && component != nil {
				if !component.stamped {
					after := component.uid
					if after < 0 {
						after = component.begin
					}
					value := stamp
					switch {
					case component.lastModified != "":
						value = component.lastModified
					case component.created != "":
						value = component.created
					}
					insertions = append(insertions, insertion{after: after, name: "DTSTAMP", value: value})
				}
				component = nil
			}
			depth--
		default:
			keyPart, value, ok := ical.SplitContentLine(line)
			if !ok {
				continue
			}
			name := strings.ToUpper(strings.TrimSpace(ical.PropertyName(keyPart)))
			switch {
			case depth == 2 && component != nil && name == "LAST-MODIFIED" && component.lastModified == "":
				component.lastModified = strings.TrimSpace(value)
			case depth == 2 && component != nil && name == "CREATED" && component.created == "":
				component.created = strings.TrimSpace(value)
			case depth == 1 && name == "VERSION":
				hasVersion = true
			case depth == 1 && name == "PRODID":
				hasProdID = true
			case depth == 2 && component != nil && name == "UID" && component.uid < 0:
				component.uid = i
			case depth == 2 && component != nil && name == "DTSTAMP":
				component.stamped = true
			}
		}
	}
	if calendarBegin >= 0 {
		var calendar []insertion
		if !hasVersion {
			calendar = append(calendar, insertion{after: calendarBegin, name: "VERSION", value: "2.0"})
		}
		if !hasProdID {
			calendar = append(calendar, insertion{after: calendarBegin, name: "PRODID", value: calDAVServerProdID})
		}
		insertions = append(calendar, insertions...)
	}
	edits := make([]bodyEdit, 0, len(insertions))
	for _, inserted := range insertions {
		at := offsets[inserted.after+1]
		newline := lineEndingBefore(raw, at)
		if newline == "" {
			return nil
		}
		edits = append(edits, bodyEdit{start: at, end: at, text: inserted.name + ":" + inserted.value + newline})
	}
	return edits
}

// oneDayDateEndEdits rewrites each VEVENT's DATE DTEND equal to its DATE
// DTSTART to the day after. Calendar applications export a one-day event that
// way, which RFC 5545 §3.8.2.2 does not admit -- DTEND is later than DTSTART,
// and a DATE DTEND is the day after the event's last (§3.6.1) -- and every
// reader here already reads it as occupying its day. The line keeps its name,
// parameters and line ending; a DTEND before its DTSTART, and a DATE-TIME one,
// are left as they are.
func oneDayDateEndEdits(raw string, lines []string, offsets []int) []bodyEdit {
	type dateLine struct {
		index          int
		keyPart, value string
	}
	var edits []bodyEdit
	depth := 0
	inEvent := false
	var start, end *dateLine
	for i, line := range lines {
		if strings.TrimSpace(line) == "" {
			continue
		}
		switch {
		case ical.HasPrefixFold(line, "BEGIN:"):
			depth++
			if depth == 2 {
				inEvent = strings.EqualFold(strings.TrimSpace(line[len("BEGIN:"):]), "VEVENT")
				start, end = nil, nil
			}
		case ical.HasPrefixFold(line, "END:"):
			if depth == 2 && inEvent && start != nil && end != nil {
				startForm, startOK := parseICalDateForm(start.value)
				endForm, endOK := parseICalDateForm(end.value)
				if day, err := time.Parse("20060102", end.value); err == nil && startOK && endOK &&
					startForm == icalDateOnly && endForm == icalDateOnly && start.value == end.value {
					begin, finish := offsets[end.index], offsets[end.index+1]
					edits = append(edits, bodyEdit{start: begin, end: finish,
						text: end.keyPart + ":" + day.AddDate(0, 0, 1).Format(ical.DateLayout) + lineEndingBefore(raw, finish)})
				}
			}
			if depth == 2 {
				inEvent = false
			}
			depth--
		case depth == 2 && inEvent:
			keyPart, value, ok := ical.SplitContentLine(line)
			if !ok {
				continue
			}
			property := dateLine{index: i, keyPart: keyPart, value: strings.TrimSpace(value)}
			switch name := strings.ToUpper(strings.TrimSpace(ical.PropertyName(keyPart))); {
			case name == "DTSTART" && start == nil:
				start = &property
			case name == "DTEND" && end == nil:
				end = &property
			}
		}
	}
	return edits
}

// lineEndingBefore is the line ending raw holds just before offset, or "" when
// none ends there.
func lineEndingBefore(raw string, offset int) string {
	switch {
	case strings.HasSuffix(raw[:offset], "\r\n"):
		return "\r\n"
	case strings.HasSuffix(raw[:offset], "\r"):
		return "\r"
	case strings.HasSuffix(raw[:offset], "\n"):
		return "\n"
	}
	return ""
}

// hostTimezoneEdits inserts, ahead of the object's first component, the host's
// definition of each zone the object names without defining (RFC 4791 §4.1),
// so every client reads its times as the server does. A definition carrying
// the server wrote for such a zone, as ical.TimezoneComponents reports it, is
// replaced: one written back for another year need not cover the new times. A
// zone generate cannot define keeps whatever definition the object carries.
func hostTimezoneEdits(raw string, lines []string, offsets []int, root *icalNode, countedEnd time.Time, generate func(tzid string, first, last time.Time) (string, bool)) []bodyEdit {
	spans := hostTimezoneSpans(root, countedEnd)
	if len(spans) == 0 {
		return nil
	}
	tzids := slices.Sorted(maps.Keys(spans))
	replaced := make(map[string]bool, len(spans))
	var definitions strings.Builder
	for _, tzid := range tzids {
		definition, ok := generate(tzid, spans[tzid].first, spans[tzid].last)
		if !ok {
			continue
		}
		replaced[tzid] = true
		definitions.WriteString(definition)
	}
	if len(replaced) == 0 {
		return nil
	}
	var edits []bodyEdit
	removedEnd := make(map[int]int) // a removed range's end line -> its start line
	removed := make(map[int]int)    // a removed range's start line -> its end line
	for _, timezone := range ical.TimezoneComponents(lines) {
		if timezone.HostDefinition && replaced[timezone.TZID] {
			edits = append(edits, bodyEdit{start: offsets[timezone.Start], end: offsets[timezone.End]})
			removed[timezone.Start], removedEnd[timezone.End] = timezone.End, timezone.Start
		}
	}
	insertAt, ok := firstKeptComponentLine(lines, removed)
	if !ok {
		return nil
	}
	// The line before the insertion is the last one kept ahead of it.
	before := insertAt
	for {
		start, wasRemoved := removedEnd[before]
		if !wasRemoved {
			break
		}
		before = start
	}
	newline := lineEndingBefore(raw, offsets[before])
	if newline == "" {
		newline = "\n"
	}
	inserted := definitions.String()
	if newline != "\r\n" {
		inserted = strings.ReplaceAll(inserted, "\r\n", newline)
	}
	return append(edits, bodyEdit{start: offsets[insertAt], end: offsets[insertAt], text: inserted})
}

// firstKeptComponentLine is the index of the first line opening a component
// inside the VCALENDAR root that is not in a removed range, keyed by its start
// line and holding its end.
func firstKeptComponentLine(lines []string, removed map[int]int) (int, bool) {
	depth := 0
	for i := 0; i < len(lines); i++ {
		if end, skip := removed[i]; skip {
			i = end - 1
			continue
		}
		switch line := lines[i]; {
		case ical.HasPrefixFold(line, "BEGIN:"):
			if depth == 1 && i > 0 {
				return i, true
			}
			depth++
		case ical.HasPrefixFold(line, "END:"):
			depth--
		}
	}
	return 0, false
}

// logicalLineOffsets returns where in raw each of lines, its unfolded content
// lines as ical.UnfoldLines splits them, starts, followed by len(raw). ok is
// false when raw does not hold as many content lines.
func logicalLineOffsets(raw string, lines []string) (offsets []int, ok bool) {
	offsets = make([]int, 0, len(lines)+1)
	for start := 0; start < len(raw); {
		offsets = append(offsets, start)
		_, start = logicalLineEnd(raw, start)
	}
	// ical.UnfoldLines reads a final line ending as ending one more, empty,
	// line, which starts where raw ends.
	for len(offsets) < len(lines) {
		offsets = append(offsets, len(raw))
	}
	if len(offsets) != len(lines) {
		return nil, false
	}
	return append(offsets, len(raw)), true
}

// wallClockSpan is the earliest and latest wall clock an object places in one
// zone, held as UTC times with the same fields.
type wallClockSpan struct {
	first, last time.Time
}

func (s *wallClockSpan) include(wall time.Time) {
	if s.first.IsZero() || wall.Before(s.first) {
		s.first = wall
	}
	if wall.After(s.last) {
		s.last = wall
	}
}

// hostTimezoneSpans finds the TZIDs the object names without defining whose
// definition the host holds -- a definition the server generated for one is
// the host's, not the object's -- other than the names of UTC itself, which
// need none (utcTZID), and for each the wall clocks its definition
// has to place: every value written in the zone, and, for a recurrence set
// starting in it, every instance through UNTIL, through countedEnd for a set
// that ends by COUNT, or through CALDAV:max-date-time for one that never ends.
// countedEnd is the latest instant the object's counted series reach, read as
// a wall clock since a definition's span is padded by more than any offset, or
// CALDAV:max-date-time when they cannot be bounded.
func hostTimezoneSpans(root *icalNode, countedEnd time.Time) map[string]*wallClockSpan {
	var defined map[string]struct{}
	for _, child := range root.children {
		if child.name == "VTIMEZONE" && !hostGeneratedTimezone(child) {
			if defined == nil {
				defined = make(map[string]struct{})
			}
			defined[strings.TrimSpace(child.value("TZID"))] = struct{}{}
		}
	}
	var spans map[string]*wallClockSpan
	// unplaced holds the names already found to need no span, so each name is
	// looked up once.
	unplaced := make(map[string]struct{})
	spanOf := func(tzid string) *wallClockSpan {
		tzid = strings.TrimSpace(tzid)
		if _, ok := defined[tzid]; ok {
			return nil
		}
		if span, ok := spans[tzid]; ok {
			return span
		}
		if _, ok := unplaced[tzid]; ok {
			return nil
		}
		if _, ok := ical.HostLocation(tzid); !ok || utcTZID(tzid) {
			unplaced[tzid] = struct{}{}
			return nil
		}
		if spans == nil {
			spans = make(map[string]*wallClockSpan)
		}
		span := &wallClockSpan{}
		spans[tzid] = span
		return span
	}
	_, maxDate := ical.DateLimits()
	for _, child := range root.children {
		if child.name == "VTIMEZONE" {
			continue
		}
		walkICalNodes(child, func(node *icalNode) {
			for _, property := range node.properties {
				if tzid, ok := property.parameters["TZID"]; ok {
					if span := spanOf(tzid); span != nil {
						includeWallClocks(span, property.value)
					}
				}
				if property.name != "RRULE" {
					continue
				}
				dtstart, ok := firstICalProperty(node, "DTSTART")
				if !ok {
					continue
				}
				if span := spanOf(dtstart.parameters["TZID"]); span != nil {
					span.include(recurrenceSetEnd(property.value, countedEnd, maxDate))
				}
			}
		})
	}
	for _, span := range spans {
		if span.first.IsZero() {
			// The zone qualifies no date-time, so it places nothing, and
			// the rules it keeps from CALDAV:max-date-time on define it.
			span.include(maxDate)
		}
	}
	return spans
}

// utcTZID reports whether tzid is a name of UTC itself, which Exchange and
// scripts write as a TZID. Calendar clients read those names as UTC without a
// definition, as the server does, so one generated for them would only make the
// stored object differ from the submitted one, and cost the client its ETag.
func utcTZID(tzid string) bool {
	return tzid == "UTC" || tzid == "Etc/UTC"
}

// includeWallClocks adds the date-times of a property value to span: a single
// value, a list, or a list of periods, whose durations carry no wall clock.
func includeWallClocks(span *wallClockSpan, value string) {
	for item := range strings.SplitSeq(value, ",") {
		for part := range strings.SplitSeq(item, "/") {
			if wall, err := ical.ParseDateTime(strings.TrimSpace(part)); err == nil {
				span.include(wall)
			}
		}
	}
}

// recurrenceSetEnd is the latest wall clock an RRULE can reach: its UNTIL,
// read as a wall clock since the definition's span is padded by more than any
// offset; countedEnd for a rule that ends by COUNT; and otherwise
// CALDAV:max-date-time, past which instances of a set with no end only extend
// what the definition already states.
func recurrenceSetEnd(rrule string, countedEnd, maxDate time.Time) time.Time {
	if value, ok := ical.RRuleParam(rrule, "UNTIL"); ok {
		if until, err := ical.ParseDateTime(value); err == nil {
			return until
		}
	}
	if _, counted := ical.RRuleParam(rrule, "COUNT"); counted && countedEnd.Before(maxDate) {
		return countedEnd
	}
	return maxDate
}

// logicalLineEnd returns where the content line starting at start ends, after
// any lines folded into it, and where the next one starts.
func logicalLineEnd(raw string, start int) (end, next int) {
	for {
		end, next = physicalLineEnd(raw, start)
		if next >= len(raw) || (raw[next] != ' ' && raw[next] != '\t') {
			return end, next
		}
		start = next
	}
}

func physicalLineEnd(raw string, start int) (end, next int) {
	index := strings.IndexAny(raw[start:], "\r\n")
	if index < 0 {
		return len(raw), len(raw)
	}
	end = start + index
	if raw[end] == '\r' && end+1 < len(raw) && raw[end+1] == '\n' {
		return end, end + 2
	}
	return end, end + 1
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
			// A zero DURATION is legal and makes an event or task an instant,
			// but an alarm repeating at no interval is not something to deliver.
			duration, ok := ical.ParseDuration(property.value)
			if !ok || duration < 0 || (duration == 0 && node.name == "VALARM") {
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
			// §3.6.6 requires the text to display, but a client that leaves it
			// out still means "remind me", and nothing here reads the text.
			if node.count("DESCRIPTION") > 1 {
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
//
// RFC 5545 wants an event's DTEND and a task's DUE later than DTSTART, but
// calendar applications write zero-length events and tasks due the day they
// start, and §9.9 places such an instant like any other, so an end equal to
// the start is admitted there. A published VFREEBUSY gets nothing from a
// zero-length span and is held to the RFC.
func validateComponentDateRelationships(root *icalNode) *calendarObjectFault {
	var fault *calendarObjectFault
	walkICalNodes(root, func(node *icalNode) {
		if fault != nil {
			return
		}
		switch node.name {
		case "VEVENT":
			if !validOrderedComponentDates(root, node, "DTSTART", "DTEND", false, true) {
				fault = invalidCalendarData()
			}
		case "VTODO":
			if !validOrderedComponentDates(root, node, "DTSTART", "DUE", false, true) {
				fault = invalidCalendarData()
			}
		case "VFREEBUSY":
			if !validOrderedComponentDates(root, node, "DTSTART", "DTEND", true, false) {
				fault = invalidCalendarData()
			}
		}
	})
	return fault
}

func validOrderedComponentDates(root, node *icalNode, startName, endName string, utcOnly, allowEqual bool) bool {
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
	return endInstant.After(startInstant) || (allowEqual && endInstant.Equal(startInstant))
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
		if allowed.contains(property.name) || alarmExtensionProperties.contains(property.name) {
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
	case len(value) == len(ical.DateLayout):
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
				if fault == nil {
					fault = dateLimitFault(values, minDate, maxDate)
				}
			}
		})
		if fault != nil {
			return fault
		}
	}
	return nil
}

// dateLimitFault is the precondition the first of values outside
// [minDate, maxDate] fails, or nil when none is.
func dateLimitFault(values []time.Time, minDate, maxDate time.Time) *calendarObjectFault {
	for _, value := range values {
		if value.Before(minDate) {
			return &calendarObjectFault{status: http.StatusForbidden, conditions: []string{"min-date-time"}}
		}
		if value.After(maxDate) {
			return &calendarObjectFault{status: http.StatusForbidden, conditions: []string{"max-date-time"}}
		}
	}
	return nil
}

// validateSubmittedCalendarProperties holds a submitted VCALENDAR's UID (RFC
// 7986 §5.3) to naming something, as a component's is, and its LAST-MODIFIED
// (§5.4) to the UTC DATE-TIME within the date limits a component's is held
// to. COPY/MOVE of stored objects is not held to either.
func validateSubmittedCalendarProperties(root *icalNode) *calendarObjectFault {
	for _, property := range root.properties {
		if property.name == "UID" && strings.TrimSpace(property.value) == "" {
			return invalidCalendarData()
		}
		if property.name != "LAST-MODIFIED" {
			continue
		}
		values, ok := icalPropertyDateValues(root, root, property)
		if !ok {
			return invalidCalendarData()
		}
		minDate, maxDate := ical.DateLimits()
		if fault := dateLimitFault(values, minDate, maxDate); fault != nil {
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

// icalRecurrenceUntil returns the instant the date limits judge an RRULE's
// UNTIL part by. Generated recurrence instances are deliberately absent: RFC
// 4791 bounds submitted values while allowing unoverridden instances to fall
// outside the advertised date range.
func icalRecurrenceUntil(root, node *icalNode, property icalProperty) ([]time.Time, bool) {
	until, _ := ical.RRuleParam(property.value, "UNTIL")
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
	// uses UTC. A timed series ending on a DATE is admitted too: RFC 2445
	// allowed it, and clients write it, CalCard's own UI among them. UNTIL is
	// inclusive, so the series runs through that whole day in DTSTART's zone,
	// as the expansion reads it. The limits admit the day when any instant of
	// it lies within them, as they admit a DATE value on its date: its last
	// second is judged against CALDAV:min-date-time, and its first against
	// CALDAV:max-date-time, so a zone's offset from UTC does not refuse a date
	// inside the limits. VTIMEZONE observances are excluded before this helper
	// is called.
	switch {
	case dtstartForm == icalDateOnly:
		if untilForm != icalDateOnly {
			return nil, false
		}
	case untilForm == icalDateOnly:
		zone := ""
		if dtstartForm == icalUTCDateTime {
			zone = "Z"
		}
		first, firstOK := icalInstants(root, dtstart, until+"T000000"+zone)
		last, lastOK := icalInstants(root, dtstart, until+"T235959"+zone)
		if !firstOK || !lastOK || len(first) != 1 || len(last) != 1 {
			return nil, false
		}
		minDate, maxDate := ical.DateLimits()
		switch {
		case last[0].Before(minDate):
			return last, true
		case first[0].After(maxDate):
			return first, true
		}
		// Some instant of the day is within the limits: the latest of its
		// first instant and CALDAV:min-date-time is one.
		if first[0].Before(minDate) {
			return []time.Time{minDate}, true
		}
		return first, true
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
	}
	// §3.3.5 makes DATE-TIME the default, so a DATE should say VALUE=DATE. An
	// eight-digit value cannot be read as anything but a DATE, though, and every
	// reader here takes it as one, so a value that leaves VALUE out is admitted.
	if _, zoned := property.parameters["TZID"]; zoned && form != icalFloatingDateTime {
		return false
	}
	return true
}

// icalInstants resolves one already-validated value to the instant the limits
// are compared against. A TZID is read as zonedWallInstant reads it, as the
// report matcher and the stored columns read it; a value in a zone neither the
// object's definition nor the host can place is refused. Other values use the
// shared DATE/DATE-TIME parser.
func icalInstants(root *icalNode, property icalProperty, value string) ([]time.Time, bool) {
	if tzid := strings.TrimSpace(property.parameters["TZID"]); tzid != "" {
		wall, err := ical.ParseDateTime(value)
		if err != nil {
			return nil, false
		}
		observances, _ := submittedTimezoneObservances(root, tzid)
		instant, ok := zonedWallInstant(observances, tzid, wall)
		if !ok {
			return nil, false
		}
		return []time.Time{instant}, true
	}
	parsed, ok := ical.ParsePropertyDateTimeLocal(property.keyPart, value)
	if !ok {
		return nil, false
	}
	return []time.Time{parsed}, true
}

// zonedWallInstant resolves a wall clock in the zone tzid names, whose
// definition in the object is observances, or nil when it ships none that can
// be read: through that definition, which RFC 4791 §4.1 makes authoritative,
// where it can place the wall clock, else through the host's zone of that
// name, as ical.ObjectTimes resolves it for the stored columns. ok is false
// when neither can.
func zonedWallInstant(observances *ical.IndexedTimezoneObservances, tzid string, wall time.Time) (time.Time, bool) {
	if offset, ok := observanceOffsetAtWall(observances, wall); ok {
		return wall.Add(-offset).UTC(), true
	}
	if loc, ok := ical.HostLocation(tzid); ok {
		return ical.WallClockIn(loc, wall).UTC(), true
	}
	return time.Time{}, false
}

// observanceOffsetAtWall resolves the offset a wall-clock reading in a
// submitted zone is read with, per ical.TimezoneObservances.OffsetAtWall.
func observanceOffsetAtWall(observances *ical.IndexedTimezoneObservances, wall time.Time) (time.Duration, bool) {
	return observances.OffsetAtWall(wall, sharedTimezoneOnsets)
}

// observanceOffsetAtInstant resolves the offset in force at an absolute
// instant, per ical.TimezoneObservances.OffsetAtInstant.
func observanceOffsetAtInstant(observances *ical.IndexedTimezoneObservances, instant time.Time) (time.Duration, bool) {
	return observances.OffsetAtInstant(instant, sharedTimezoneOnsets)
}

// submittedZones is what the VTIMEZONEs of one parsed object define, so a
// value naming a TZID finds its definition without walking the object, and
// the definition is read once however many values name it.
type submittedZones struct {
	// timezones is the VTIMEZONE each TZID names, the first whose TZID it
	// is. It is nil for a definition the server wrote for a zone the host
	// defines, as hostGeneratedTimezone recognises it: that speaks for the
	// host's database, which answers for the TZID instead, so no reading of
	// the object depends on the years the definition was generated for or on
	// the tzdata release it was generated from.
	timezones map[string]*icalNode
	// work is what every definition's rules may cost to walk where the
	// shared listings do not answer them, for as long as the parse is read.
	work *ical.ObservanceWork
	// observances is the reading of each TZID asked for so far, guarded by mu.
	mu          sync.Mutex
	observances map[string]*ical.IndexedTimezoneObservances
}

// submittedZones returns root's zones, reading its VTIMEZONEs the first time.
func (root *icalNode) submittedZones() *submittedZones {
	root.zonesOnce.Do(func() { root.zones = readSubmittedZones(root) })
	return root.zones
}

func readSubmittedZones(root *icalNode) *submittedZones {
	zones := &submittedZones{
		timezones:   make(map[string]*icalNode),
		work:        ical.NewObservanceWork(),
		observances: make(map[string]*ical.IndexedTimezoneObservances),
	}
	for _, child := range root.children {
		if child.name != "VTIMEZONE" {
			continue
		}
		tzid := strings.TrimSpace(child.value("TZID"))
		if _, earlier := zones.timezones[tzid]; earlier {
			continue
		}
		if hostGeneratedTimezone(child) {
			zones.timezones[tzid] = nil
			continue
		}
		zones.timezones[tzid] = child
	}
	return zones
}

// submittedTimezoneObservances reads the observances of the VTIMEZONE the
// object ships for tzid. ok is false when it ships none, or one this cannot
// read, per ical.ReadTimezoneObservances.
func submittedTimezoneObservances(root *icalNode, tzid string) (*ical.IndexedTimezoneObservances, bool) {
	if root == nil {
		return nil, false
	}
	zones := root.submittedZones()
	zones.mu.Lock()
	defer zones.mu.Unlock()
	if observances, read := zones.observances[tzid]; read {
		return observances, observances != nil
	}
	observances, ok := readSubmittedTimezone(zones.timezones[tzid])
	var indexed *ical.IndexedTimezoneObservances
	if ok {
		indexed = ical.IndexTimezoneObservances(observances, zones.work)
	}
	zones.observances[tzid] = indexed
	return indexed, ok
}

// readSubmittedTimezone reads the observances of timezone, or none when
// timezone is nil.
func readSubmittedTimezone(timezone *icalNode) (ical.TimezoneObservances, bool) {
	if timezone == nil {
		return nil, false
	}
	// One backing array holds every observance's lines.
	count, lines := 0, 0
	for _, node := range timezone.children {
		if node.name == "STANDARD" || node.name == "DAYLIGHT" {
			count++
			lines += len(node.properties)
		}
	}
	components := make([][]ical.PropertyValue, 0, count)
	properties := make([]ical.PropertyValue, 0, lines)
	for _, node := range timezone.children {
		if node.name != "STANDARD" && node.name != "DAYLIGHT" {
			continue
		}
		first := len(properties)
		for _, property := range node.properties {
			properties = append(properties, ical.PropertyValue{KeyPart: property.keyPart, Value: property.value})
		}
		components = append(components, properties[first:len(properties):len(properties)])
	}
	return ical.ReadTimezoneObservances(components)
}

// hostGeneratedTimezone reports whether the server wrote a VTIMEZONE from the
// host's database for a zone the host defines: whether it carries
// ical.HostDefinitionProperty or has the shape ical.V11GeneratedDefinition
// recognises, as ical.TimezoneComponents decides for the same component.
func hostGeneratedTimezone(timezone *icalNode) bool {
	if _, known := ical.HostLocation(strings.TrimSpace(timezone.value("TZID"))); !known {
		return false
	}
	return timezone.count(ical.HostDefinitionProperty) > 0 || v11GeneratedTimezone(timezone)
}

// v11GeneratedTimezone writes a VTIMEZONE back as content lines, its
// properties ahead of its components, for ical.V11GeneratedDefinition. Only a
// definition with one property of its own and observances holding no
// components can have that shape.
func v11GeneratedTimezone(timezone *icalNode) bool {
	if len(timezone.properties) != 1 {
		return false
	}
	lines := make([]string, 0, 3+6*len(timezone.children))
	lines = append(lines, "BEGIN:VTIMEZONE", timezone.properties[0].keyPart+":"+timezone.properties[0].value)
	for _, observance := range timezone.children {
		if len(observance.children) > 0 {
			return false
		}
		lines = append(lines, "BEGIN:"+observance.name)
		for _, property := range observance.properties {
			lines = append(lines, property.keyPart+":"+property.value)
		}
		lines = append(lines, "END:"+observance.name)
	}
	return ical.V11GeneratedDefinition(append(lines, "END:VTIMEZONE"))
}

func firstICalProperty(node *icalNode, name string) (icalProperty, bool) {
	for _, property := range node.properties {
		if property.name == name {
			return property, true
		}
	}
	return icalProperty{}, false
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
