package dav

import (
	"net/url"
	"strconv"
	"strings"

	"github.com/jw6ventures/calcard/internal/ical"
)

// icalProperty is one parsed RFC 5545 content line. keyPart keeps the name and
// parameters exactly as they were written, because the shared internal/ical
// helpers resolve a TZID from that spelling rather than from a parameter map.
type icalProperty struct {
	name       string
	keyPart    string
	value      string
	parameters map[string]string
}

var knownICalendarProperties = map[string]struct{}{
	"ACTION": {}, "ATTACH": {}, "ATTENDEE": {}, "CALSCALE": {}, "CATEGORIES": {},
	"CLASS": {}, "COMMENT": {}, "COMPLETED": {}, "CONTACT": {}, "CREATED": {},
	"DESCRIPTION": {}, "DTEND": {}, "DTSTAMP": {}, "DTSTART": {}, "DUE": {}, "BEGIN": {}, "END": {},
	"DURATION": {}, "EXDATE": {}, "FREEBUSY": {}, "GEO": {}, "LAST-MODIFIED": {},
	"LOCATION": {}, "METHOD": {}, "ORGANIZER": {}, "PERCENT-COMPLETE": {},
	"PRIORITY": {}, "PRODID": {}, "RDATE": {}, "RECURRENCE-ID": {}, "RELATED-TO": {},
	"REPEAT": {}, "REQUEST-STATUS": {}, "RESOURCES": {}, "RRULE": {}, "SEQUENCE": {},
	"STATUS": {}, "SUMMARY": {}, "TRANSP": {}, "TRIGGER": {}, "TZID": {},
	"TZNAME": {}, "TZOFFSETFROM": {}, "TZOFFSETTO": {}, "TZURL": {}, "UID": {},
	"URL": {}, "VERSION": {},
}

var knownICalendarParameters = map[string]struct{}{
	"ALTREP": {}, "CN": {}, "CUTYPE": {}, "DELEGATED-FROM": {}, "DELEGATED-TO": {},
	"DIR": {}, "ENCODING": {}, "FMTTYPE": {}, "FBTYPE": {}, "LANGUAGE": {},
	"MEMBER": {}, "PARTSTAT": {}, "RANGE": {}, "RELATED": {}, "RELTYPE": {},
	"ROLE": {}, "RSVP": {}, "SENT-BY": {}, "TZID": {}, "VALUE": {},
}

// icalReading is how a content line is read, which depends on where the object
// holding it comes from.
type icalReading int

const (
	// readSubmitted reads an object a client submits, as parseICalProperty
	// describes.
	readSubmitted icalReading = iota
	// readTransferred reads an object the store holds that COPY or MOVE writes
	// again: as submitted, except that a parameter without a valid name or an
	// "=" is left out, as readStored leaves it out.
	readTransferred
	// readStored reads an object the store holds to answer a request, which
	// may only balance its BEGIN and END lines: every control character is
	// read as written, a parameter without a valid name or an "=" is left out,
	// and a repeated parameter keeps its first value whatever the others say.
	readStored
)

// parseICalProperty reads one submitted content line. Parameters calendar
// applications write that RFC 5545 §3.1 does not admit but that change nothing
// the line says -- an empty CN=, a repeated parameter, an attendee's bare RSVP
// (bareAttendeeRSVP) -- are read: a value outside the grammar as written, a
// repeat as its first value. Refused are a NUL, CR or LF; any other parameter
// without a valid name or an "=" (§3.2), since leaving it out can change the
// line's meaning -- "DTSTART;TZID:20250310T090000" would read as floating; and
// a repeat of one of singleValuedParameters with a different value.
//
// The other control characters §3.1 excludes are read as written: Outlook and
// Exchange put them in text, and stored objects may carry them. XML responses
// write calendar-data through sanitizeXMLCharData.
func parseICalProperty(line string) (icalProperty, bool) {
	return readICalProperty(line, readSubmitted)
}

// singleValuedParameters are the parameters the server reads to resolve, type
// or classify a value: TZID and VALUE place and type a date, RANGE widens an
// override to the instances after it, RELATED anchors an alarm, and FBTYPE
// classifies a busy period. Repeated with different values, they would leave
// the readers free to disagree on which one counts.
var singleValuedParameters = nameSet("TZID", "VALUE", "RANGE", "RELATED", "FBTYPE")

func readICalProperty(line string, reading icalReading) (icalProperty, bool) {
	keyPart, value, ok := ical.SplitContentLine(line)
	if !ok || keyPart == "" {
		return icalProperty{}, false
	}
	// PostgreSQL TEXT cannot hold a NUL, and a CR or LF inside a content line
	// would end it when the line is written out.
	if reading != readStored && (strings.ContainsAny(keyPart, "\x00\r\n") || strings.ContainsAny(value, "\x00\r\n")) {
		return icalProperty{}, false
	}
	parts, ok := splitOutsideQuotes(keyPart, ';')
	if !ok || len(parts) == 0 {
		return icalProperty{}, false
	}
	name := strings.ToUpper(strings.TrimSpace(parts[0]))
	if !validICalendarToken(name) {
		return icalProperty{}, false
	}
	property := icalProperty{
		name:       name,
		keyPart:    keyPart,
		value:      value,
		parameters: make(map[string]string, len(parts)-1),
	}
	for _, rawParameter := range parts[1:] {
		parameterName, parameterValue, found := strings.Cut(rawParameter, "=")
		parameterName = strings.ToUpper(strings.TrimSpace(parameterName))
		parameterValue = unquoteICalendarParameter(strings.TrimSpace(parameterValue))
		if !found || !validICalendarToken(parameterName) {
			if reading == readSubmitted && !(!found && bareAttendeeRSVP(name, parameterName)) {
				return icalProperty{}, false
			}
			continue
		}
		if first, duplicate := property.parameters[parameterName]; duplicate {
			if reading != readStored && conflictingICalendarParameter(parameterName, first, parameterValue) {
				return icalProperty{}, false
			}
			continue
		}
		property.parameters[parameterName] = parameterValue
	}
	return property, true
}

// bareAttendeeRSVP reports whether parameterName, written without "=" or a
// value on a property named propertyName, is an attendee's RSVP. Calendar
// applications write "ATTENDEE;RSVP:mailto:..." for an attendee asked to reply.
// The line is accepted and stored as written, and the parameter is read as
// though absent, which RFC 5545 §3.2.17 makes FALSE: nothing the server reads
// depends on it, and reading a stored object leaves it out the same way.
func bareAttendeeRSVP(propertyName, parameterName string) bool {
	return propertyName == "ATTENDEE" && parameterName == "RSVP"
}

// conflictingICalendarParameter reports whether a repeat of the parameter name
// with value says something other than its first value did.
func conflictingICalendarParameter(name, first, value string) bool {
	switch {
	case !singleValuedParameters.contains(name):
		return false
	case name == "TZID":
		// Every TZID lookup matches the value as written.
		return first != value
	default:
		// The others are enumerations, which §3.2 makes case-insensitive.
		return !strings.EqualFold(first, value)
	}
}

func splitOutsideQuotes(value string, delimiter byte) ([]string, bool) {
	var parts []string
	start := 0
	quoted := false
	for i := 0; i < len(value); i++ {
		switch value[i] {
		case '"':
			quoted = !quoted
		case delimiter:
			if !quoted {
				parts = append(parts, value[start:i])
				start = i + 1
			}
		}
	}
	if quoted {
		return nil, false
	}
	parts = append(parts, value[start:])
	return parts, true
}

func validICalendarToken(value string) bool {
	if value == "" {
		return false
	}
	for _, char := range value {
		if (char >= 'A' && char <= 'Z') || (char >= 'a' && char <= 'z') ||
			(char >= '0' && char <= '9') || char == '-' {
			continue
		}
		return false
	}
	return true
}

func unquoteICalendarParameter(value string) string {
	if len(value) >= 2 && value[0] == '"' && value[len(value)-1] == '"' {
		return value[1 : len(value)-1]
	}
	return value
}

func validCalendarTimezoneComponentProperties(component *icalComponent) bool {
	allowed := map[string]struct{}{}
	switch component.name {
	case "VCALENDAR":
		allowed = map[string]struct{}{"VERSION": {}, "PRODID": {}, "CALSCALE": {}, "METHOD": {}}
	case "VTIMEZONE":
		allowed = map[string]struct{}{"TZID": {}, "LAST-MODIFIED": {}, "TZURL": {}}
	case "STANDARD", "DAYLIGHT":
		allowed = map[string]struct{}{
			"DTSTART": {}, "TZOFFSETFROM": {}, "TZOFFSETTO": {}, "RRULE": {},
			"COMMENT": {}, "RDATE": {}, "TZNAME": {},
		}
	default:
		return false
	}

	for name, properties := range component.propertyLines {
		if _, ok := allowed[name]; !ok {
			if _, standard := knownICalendarProperties[name]; standard {
				return false
			}
		}
		for _, property := range properties {
			if !validCalendarTimezoneProperty(component.name, property) {
				return false
			}
		}
	}

	switch component.name {
	case "VCALENDAR":
		return atMostOne(component, "CALSCALE", "METHOD")
	case "VTIMEZONE":
		return atMostOne(component, "LAST-MODIFIED", "TZURL")
	case "STANDARD", "DAYLIGHT":
		return atMostOne(component, "RRULE")
	default:
		return false
	}
}

func atMostOne(component *icalComponent, names ...string) bool {
	for _, name := range names {
		if component.properties[name] > 1 {
			return false
		}
	}
	return true
}

func validCalendarTimezoneProperty(componentName string, property icalProperty) bool {
	allowedParameters := map[string]struct{}{}
	switch property.name {
	case "DTSTART", "RDATE":
		allowedParameters["VALUE"] = struct{}{}
	case "COMMENT":
		allowedParameters["ALTREP"] = struct{}{}
		allowedParameters["LANGUAGE"] = struct{}{}
	case "TZNAME":
		allowedParameters["LANGUAGE"] = struct{}{}
	}
	for name := range property.parameters {
		if _, allowed := allowedParameters[name]; allowed {
			continue
		}
		if _, standard := knownICalendarParameters[name]; standard {
			return false
		}
	}

	switch property.name {
	case "VERSION":
		return componentName == "VCALENDAR" && property.value == "2.0"
	case "PRODID", "TZID":
		return property.value != "" && validICalendarText(property.value)
	case "CALSCALE":
		return strings.EqualFold(property.value, "GREGORIAN")
	case "METHOD":
		return validICalendarToken(property.value)
	case "LAST-MODIFIED":
		return validUTCDateTime(property.value)
	case "TZURL":
		parsed, err := url.ParseRequestURI(property.value)
		return err == nil && parsed.Scheme != ""
	case "DTSTART":
		return validTimezoneDateTimeProperty(property)
	case "TZOFFSETFROM", "TZOFFSETTO":
		return validUTCOffset(property.value)
	case "RRULE":
		return validTimezoneRecurrenceRule(property.value)
	case "RDATE":
		return validTimezoneRDate(property)
	case "COMMENT", "TZNAME":
		return validICalendarText(property.value)
	default:
		return validICalendarToken(property.name)
	}
}

func validUTCDateTime(value string) bool {
	form, ok := parseICalDateForm(strings.ToUpper(value))
	return ok && form == icalUTCDateTime
}

func validTimezoneDateTimeProperty(property icalProperty) bool {
	if valueType, ok := property.parameters["VALUE"]; ok && !strings.EqualFold(valueType, "DATE-TIME") {
		return false
	}
	form, ok := parseICalDateForm(property.value)
	return ok && form == icalFloatingDateTime
}

func validTimezoneRDate(property icalProperty) bool {
	if valueType, ok := property.parameters["VALUE"]; ok && !strings.EqualFold(valueType, "DATE-TIME") {
		return false
	}
	values := strings.Split(property.value, ",")
	if len(values) == 0 {
		return false
	}
	for _, value := range values {
		value = strings.TrimSpace(value)
		form, ok := parseICalDateForm(value)
		if value == "" || !ok || form != icalFloatingDateTime {
			return false
		}
	}
	return true
}

func validTimezoneRecurrenceRule(value string) bool {
	parts := strings.Split(value, ";")
	seen := make(map[string]struct{}, len(parts))
	values := make(map[string]string, len(parts))
	for _, part := range parts {
		name, ruleValue, found := strings.Cut(strings.TrimSpace(part), "=")
		name = strings.ToUpper(strings.TrimSpace(name))
		ruleValue = strings.TrimSpace(ruleValue)
		if !found || !validICalendarToken(name) || ruleValue == "" {
			return false
		}
		if _, duplicate := seen[name]; duplicate {
			return false
		}
		seen[name] = struct{}{}
		values[name] = ruleValue
	}

	frequency := strings.ToUpper(values["FREQ"])
	switch frequency {
	case "SECONDLY", "MINUTELY", "HOURLY", "DAILY", "WEEKLY", "MONTHLY", "YEARLY":
	default:
		return false
	}
	// RFC 5545 §3.6.5 requires a time-zone observance with a finite end to
	// express that end with an UTC UNTIL value rather than COUNT.
	if values["COUNT"] != "" {
		return false
	}
	if value := values["INTERVAL"]; value != "" && !validPositiveInteger(value) {
		return false
	}
	if value := values["UNTIL"]; value != "" && !validUTCDateTime(value) {
		return false
	}
	if value := values["WKST"]; value != "" && !validWeekday(value) {
		return false
	}

	numericLists := []struct {
		name      string
		min       int
		max       int
		allowZero bool
	}{
		{"BYSECOND", 0, 60, true},
		{"BYMINUTE", 0, 59, true},
		{"BYHOUR", 0, 23, true},
		{"BYMONTHDAY", -31, 31, false},
		{"BYYEARDAY", -366, 366, false},
		{"BYWEEKNO", -53, 53, false},
		{"BYMONTH", 1, 12, false},
		{"BYSETPOS", -366, 366, false},
	}
	for _, list := range numericLists {
		if value := values[list.name]; value != "" && !validIntegerList(value, list.min, list.max, list.allowZero) {
			return false
		}
	}
	ordinalWeekday, ok := validWeekdayList(values["BYDAY"])
	if !ok {
		return false
	}

	known := map[string]struct{}{
		"FREQ": {}, "UNTIL": {}, "COUNT": {}, "INTERVAL": {}, "BYSECOND": {},
		"BYMINUTE": {}, "BYHOUR": {}, "BYDAY": {}, "BYMONTHDAY": {},
		"BYYEARDAY": {}, "BYWEEKNO": {}, "BYMONTH": {}, "BYSETPOS": {}, "WKST": {},
	}
	for name := range values {
		if _, ok := known[name]; !ok {
			return false
		}
	}

	if ordinalWeekday && frequency != "MONTHLY" && frequency != "YEARLY" {
		return false
	}
	if ordinalWeekday && frequency == "YEARLY" && values["BYWEEKNO"] != "" {
		return false
	}
	if frequency == "WEEKLY" && values["BYMONTHDAY"] != "" {
		return false
	}
	if (frequency == "DAILY" || frequency == "WEEKLY" || frequency == "MONTHLY") && values["BYYEARDAY"] != "" {
		return false
	}
	if values["BYWEEKNO"] != "" && frequency != "YEARLY" {
		return false
	}
	if values["BYSETPOS"] != "" && !hasOtherByRule(values) {
		return false
	}
	return true
}

func validPositiveInteger(value string) bool {
	positive := false
	for _, char := range value {
		if char < '0' || char > '9' {
			return false
		}
		if char != '0' {
			positive = true
		}
	}
	return value != "" && positive
}

func validIntegerList(value string, minValue, maxValue int, allowZero bool) bool {
	for _, part := range strings.Split(value, ",") {
		parsed, err := strconv.Atoi(strings.TrimSpace(part))
		if err != nil || parsed < minValue || parsed > maxValue || (!allowZero && parsed == 0) {
			return false
		}
	}
	return true
}

func validWeekdayList(value string) (bool, bool) {
	if value == "" {
		return false, true
	}
	hasOrdinal := false
	for _, part := range strings.Split(value, ",") {
		part = strings.ToUpper(strings.TrimSpace(part))
		if len(part) < 2 || !validWeekday(part[len(part)-2:]) {
			return false, false
		}
		ordinal := part[:len(part)-2]
		if ordinal == "" {
			continue
		}
		value, err := strconv.Atoi(ordinal)
		if err != nil || value == 0 || value < -53 || value > 53 {
			return false, false
		}
		hasOrdinal = true
	}
	return hasOrdinal, true
}

func validWeekday(value string) bool {
	switch strings.ToUpper(value) {
	case "MO", "TU", "WE", "TH", "FR", "SA", "SU":
		return true
	default:
		return false
	}
}

func hasOtherByRule(values map[string]string) bool {
	for _, name := range []string{
		"BYSECOND", "BYMINUTE", "BYHOUR", "BYDAY", "BYMONTHDAY",
		"BYYEARDAY", "BYWEEKNO", "BYMONTH",
	} {
		if values[name] != "" {
			return true
		}
	}
	return false
}

func validICalendarText(value string) bool {
	for i := 0; i < len(value); i++ {
		switch value[i] {
		case ',', ';':
			return false
		case '\\':
			i++
			if i >= len(value) || !strings.ContainsRune(`nN,;\`, rune(value[i])) {
				return false
			}
		default:
			if value[i] < 0x20 && value[i] != '\t' {
				return false
			}
		}
	}
	return true
}
