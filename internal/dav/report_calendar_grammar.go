package dav

import (
	"encoding/xml"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/jw6ventures/calcard/internal/ical"
)

// The three CalDAV REPORT bodies are read against their RFC 4791 content models
// by token walks rather than through the permissive struct tags that frame them.
// Other REPORT bodies continue through reportRequest and their own validators.

// propertySelector is the `(DAV:allprop | DAV:propname | DAV:prop)?` head of
// both calendaring REPORT bodies. The three are alternatives, so at most one
// field is ever set.
type propertySelector struct {
	AllProp  bool
	PropName bool
	Prop     *reportProp
}

// calendarQueryRequest is the CALDAV:calendar-query body of RFC 4791 §9.5:
// ((DAV:allprop | DAV:propname | DAV:prop)?, CALDAV:filter, CALDAV:timezone?).
type calendarQueryRequest struct {
	Selector propertySelector
	Filter   *calFilter
	Timezone string
}

// calendarMultigetRequest is the CALDAV:calendar-multiget body of RFC 4791
// §9.10: ((DAV:allprop | DAV:propname | DAV:prop)?, DAV:href+).
type calendarMultigetRequest struct {
	Selector propertySelector
	Hrefs    []string
}

func invalidFilter(format string, args ...any) *reportGrammarFault {
	return &reportGrammarFault{condition: "valid-filter", reason: fmt.Sprintf(format, args...)}
}

// invalidReportCalendarData names the CALDAV:valid-calendar-data precondition,
// which RFC 4791 §7.8 raises when the timezone a REPORT carries is not a valid
// iCalendar object holding a single valid VTIMEZONE.
func invalidReportCalendarData(format string, args ...any) *reportGrammarFault {
	return &reportGrammarFault{condition: "valid-calendar-data", reason: fmt.Sprintf(format, args...)}
}

func unsupportedFilterElement(element, name string) *reportGrammarFault {
	return &reportGrammarFault{
		condition: "supported-filter",
		offending: []filterElementRef{{Element: element, Name: name}},
		reason:    fmt.Sprintf("%s names %q, which is not an iCalendar name", element, name),
	}
}

func unsupportedCollation(collation string) *reportGrammarFault {
	return &reportGrammarFault{
		condition: "supported-collation",
		reason:    fmt.Sprintf("unsupported collation %q", collation),
	}
}

// applyCalendarReportGrammar re-reads each CalDAV REPORT body against its RFC
// 4791 content model and replaces the permissively decoded fields with what the
// model actually admits. Other report types keep the permissive decode, save for
// the CALDAV:calendar-data inside them, which carries a content model of its own
// wherever it appears.
func applyCalendarReportGrammar(report *reportRequest, body []byte) *reportGrammarFault {
	switch report.XMLName.Local {
	case "calendar-query":
		parsed, fault := parseCalendarQueryRequest(body)
		if fault != nil {
			return fault
		}
		report.selector = parsed.Selector
		report.Prop = parsed.Selector.Prop
		report.Filter = parsed.Filter
		report.Timezone = parsed.Timezone
		report.Hrefs = nil
	case "calendar-multiget":
		parsed, fault := parseCalendarMultigetRequest(body)
		if fault != nil {
			return fault
		}
		report.selector = parsed.Selector
		report.Prop = parsed.Selector.Prop
		report.Hrefs = parsed.Hrefs
		report.Filter = nil
	case "free-busy-query":
		parsed, fault := parseFreeBusyQueryRequest(body)
		if fault != nil {
			return fault
		}
		report.TimeRange = parsed.TimeRange
		// §9.11 admits the time-range and nothing else, so anything the
		// permissive decode picked up names no part of this request.
		report.Filter = nil
		report.Prop = nil
		report.Hrefs = nil
		report.selector = propertySelector{}
	}
	// §9.6 binds a CALDAV:calendar-data selector wherever one appears, including
	// in a report whose own body this pass does not walk -- sync-collection is the
	// one CalCard answers. The three walks above raise the fault through
	// decodePropertySelector and leave none behind, so this reaches the rest.
	if calData := reportCalendarData(*report); calData != nil && calData.fault != nil {
		return calData.fault
	}
	return nil
}

// reportDepth reads the Depth a calendaring REPORT is scoped by. RFC 4791 §7.8
// and §7.10 both process a request carrying no Depth header as Depth: 0, which
// is the opposite of the RFC 4918 §9.1 PROPFIND default and reaches the
// Request-URI alone. RFC 4918 §10.2 fixes the three legal values, so anything
// else is a malformed request rather than a value to guess at.
func reportDepth(r *http.Request) (string, bool) {
	depth := strings.TrimSpace(r.Header.Get("Depth"))
	switch depth {
	case "":
		return "0", true
	case "0", "1", "infinity":
		return depth, true
	default:
		return "", false
	}
}

// calendarQueryExcludedByDepth reports whether the Depth header puts every
// candidate resource out of the query's reach. A calendar collection is not
// itself a calendar object resource, so Depth: 0 on one -- the default when the
// header is absent -- considers the collection alone and matches nothing. An
// object-resource Request-URI has no members, so Depth never narrows it.
func calendarQueryExcludedByDepth(r *http.Request, report reportRequest, targetResource string) bool {
	if report.XMLName.Local != "calendar-query" || targetResource != "" {
		return false
	}
	depth, ok := reportDepth(r)
	return ok && depth == "0"
}

// writeReportGrammarFault answers a rejected body. A fault naming a
// precondition answers 403 with the condition under a top-level DAV:error
// (RFC 4791 §1.3); a bare content-model violation names no precondition and is
// an ordinary malformed request.
func writeReportGrammarFault(w http.ResponseWriter, fault *reportGrammarFault) {
	switch fault.condition {
	case "":
		http.Error(w, "invalid REPORT body", http.StatusBadRequest)
	case "supported-filter":
		writeCalDAVSupportedFilter(w, fault.offending)
	default:
		writeCalDAVError(w, http.StatusForbidden, fault.condition)
	}
}

// calendarQueryStage is how far through the RFC 4791 §9.5 sequence a
// calendar-query body has been read. A child belonging to an earlier stage is
// out of document order.
type calendarQueryStage int

const (
	queryStageSelector calendarQueryStage = iota
	queryStageFilter
	queryStageTimezone
	queryStageEnd
)

func parseCalendarQueryRequest(body []byte) (*calendarQueryRequest, *reportGrammarFault) {
	decoder, start, fault := openReportBody(body, calDAVQName("calendar-query"))
	if fault != nil {
		return nil, fault
	}
	if fault := checkAttributes(start, "CALDAV:calendar-query"); fault != nil {
		return nil, fault
	}
	request := &calendarQueryRequest{}
	stage := queryStageSelector
	err := walkElementChildren(decoder, start, "CALDAV:calendar-query", malformedReport, func(dec *xml.Decoder, child xml.StartElement) *reportGrammarFault {
		switch {
		case stage == queryStageSelector && isPropertySelector(child.Name):
			selector, fault := decodePropertySelector(dec, child)
			if fault != nil {
				return fault
			}
			request.Selector = selector
			stage = queryStageFilter
			return nil
		case stage <= queryStageFilter && child.Name == calDAVQName("filter"):
			filter, fault := decodeCalendarFilter(dec, child)
			if fault != nil {
				return fault
			}
			request.Filter = filter
			stage = queryStageTimezone
			return nil
		case stage == queryStageTimezone && child.Name == calDAVQName("timezone"):
			if fault := checkAttributes(child, "CALDAV:timezone"); fault != nil {
				return fault
			}
			text, fault := decodeTextElement(dec, child, "CALDAV:timezone")
			if fault != nil {
				return fault
			}
			if !validCalendarTimezone(text) {
				return invalidReportCalendarData("CALDAV:timezone is not an iCalendar object carrying one valid VTIMEZONE")
			}
			request.Timezone = text
			stage = queryStageEnd
			return nil
		}
		return malformedReport("CALDAV:calendar-query carries %s out of the order §9.5 defines", xmlNameString(child.Name))
	})
	if err != nil {
		return nil, err
	}
	if request.Filter == nil {
		return nil, malformedReport("CALDAV:calendar-query carries no CALDAV:filter")
	}
	if fault := validateCalendarFilterSemantics(request.Filter); fault != nil {
		return nil, fault
	}
	return request, nil
}

func parseCalendarMultigetRequest(body []byte) (*calendarMultigetRequest, *reportGrammarFault) {
	decoder, start, fault := openReportBody(body, calDAVQName("calendar-multiget"))
	if fault != nil {
		return nil, fault
	}
	if fault := checkAttributes(start, "CALDAV:calendar-multiget"); fault != nil {
		return nil, fault
	}
	request := &calendarMultigetRequest{}
	// §9.10 puts the property selector ahead of the hrefs, so anything already
	// read closes the selector position whether it was a selector or an href.
	selectorClosed := false
	err := walkElementChildren(decoder, start, "CALDAV:calendar-multiget", malformedReport, func(dec *xml.Decoder, child xml.StartElement) *reportGrammarFault {
		switch {
		case isPropertySelector(child.Name):
			if selectorClosed {
				return malformedReport("CALDAV:calendar-multiget carries %s out of the order §9.10 defines", xmlNameString(child.Name))
			}
			selector, fault := decodePropertySelector(dec, child)
			if fault != nil {
				return fault
			}
			request.Selector = selector
			selectorClosed = true
			return nil
		case child.Name == davQName("href"):
			if fault := checkAttributes(child, "DAV:href"); fault != nil {
				return fault
			}
			href, fault := decodeTextElement(dec, child, "DAV:href")
			if fault != nil {
				return fault
			}
			request.Hrefs = append(request.Hrefs, href)
			selectorClosed = true
			return nil
		}
		return malformedReport("CALDAV:calendar-multiget carries unexpected %s", xmlNameString(child.Name))
	})
	if err != nil {
		return nil, err
	}
	if len(request.Hrefs) == 0 {
		return nil, malformedReport("CALDAV:calendar-multiget carries no DAV:href")
	}
	return request, nil
}

func isPropertySelector(name xml.Name) bool {
	return name == davQName("allprop") || name == davQName("propname") || name == davQName("prop")
}

// decodePropertySelector reads one of the three alternatives. DAV:prop keeps
// its struct-tag decoding, because its content is an open property list rather
// than a content model with an order to enforce.
func decodePropertySelector(dec *xml.Decoder, start xml.StartElement) (propertySelector, *reportGrammarFault) {
	switch start.Name {
	case davQName("allprop"):
		if fault := expectEmptyElement(dec, start, "DAV:allprop", malformedReport); fault != nil {
			return propertySelector{}, fault
		}
		return propertySelector{AllProp: true}, nil
	case davQName("propname"):
		if fault := expectEmptyElement(dec, start, "DAV:propname", malformedReport); fault != nil {
			return propertySelector{}, fault
		}
		return propertySelector{PropName: true}, nil
	default:
		if fault := checkAttributes(start, "DAV:prop"); fault != nil {
			return propertySelector{}, fault
		}
		var requested reportProp
		if err := dec.DecodeElement(&requested, &start); err != nil {
			return propertySelector{}, malformedReport("DAV:prop: %v", err)
		}
		// CALDAV:calendar-data is the one child of DAV:prop that has a content
		// model. It reads itself against §9.6 and records what it found there,
		// because a decoder hook cannot fail the permissive decode every other
		// report body still goes through.
		if requested.CalendarData != nil && requested.CalendarData.fault != nil {
			return propertySelector{}, requested.CalendarData.fault
		}
		return propertySelector{Prop: &requested}, nil
	}
}

// decodeCalendarFilter reads CALDAV:filter, whose §9.7 content model is exactly
// one CALDAV:comp-filter.
func decodeCalendarFilter(dec *xml.Decoder, start xml.StartElement) (*calFilter, *reportGrammarFault) {
	if fault := checkAttributes(start, "CALDAV:filter"); fault != nil {
		return nil, invalidFilter("%s", fault.reason)
	}
	var filter calFilter
	count := 0
	err := walkElementChildren(dec, start, "CALDAV:filter", invalidFilter, func(dec *xml.Decoder, child xml.StartElement) *reportGrammarFault {
		if child.Name != calDAVQName("comp-filter") {
			return invalidFilter("CALDAV:filter carries unexpected %s", xmlNameString(child.Name))
		}
		count++
		if count > 1 {
			return invalidFilter("CALDAV:filter carries more than one CALDAV:comp-filter")
		}
		decoded, fault := decodeCompFilter(dec, child, 1)
		if fault != nil {
			return fault
		}
		filter.CompFilter = *decoded
		return nil
	})
	if err != nil {
		return nil, err
	}
	if count != 1 {
		return nil, invalidFilter("CALDAV:filter carries %d CALDAV:comp-filter elements, want exactly 1", count)
	}
	return &filter, nil
}

// compFilterStage is how far through the RFC 4791 §9.7.1 sequence
// (time-range?, prop-filter*, comp-filter*) a comp-filter has been read.
type compFilterStage int

const (
	compStageTimeRange compFilterStage = iota
	compStagePropFilter
	compStageCompFilter
)

// decodeCompFilter reads CALDAV:comp-filter, whose §9.7.1 content model is
// is-not-defined alone or (time-range?, prop-filter*, comp-filter*). §9.7.1
// gives it one attribute, name; RFC 4791 defines no test attribute, so the
// CardDAV spelling of an any-of filter is malformed here.
func decodeCompFilter(dec *xml.Decoder, start xml.StartElement, depth int) (*compFilter, *reportGrammarFault) {
	if depth > maxRecursiveGrammarDepth {
		return nil, invalidFilter("CALDAV:comp-filter nests deeper than this server evaluates")
	}
	name, fault := filterElementName(start, "comp-filter", "CALDAV:comp-filter")
	if fault != nil {
		return nil, fault
	}
	filter := &compFilter{Name: name}
	stage := compStageTimeRange
	err := walkElementChildren(dec, start, "CALDAV:comp-filter", invalidFilter, func(dec *xml.Decoder, child xml.StartElement) *reportGrammarFault {
		// Every other child advances the stage, so the opening stage is also
		// what "no sibling yet" means for the is-not-defined rule.
		alone := stage == compStageTimeRange
		if handled, present, fault := decodeIsNotDefined(dec, child, "CALDAV:comp-filter", alone, filter.IsNotDefined != nil); handled {
			if present {
				filter.IsNotDefined = &struct{}{}
			}
			return fault
		}
		switch child.Name {
		case calDAVQName("time-range"):
			if stage != compStageTimeRange || filter.TimeRange != nil {
				return invalidFilter("CALDAV:comp-filter carries CALDAV:time-range out of the order §9.7.1 defines")
			}
			decoded, fault := decodeTimeRange(dec, child)
			if fault != nil {
				return fault
			}
			filter.TimeRange = decoded
			stage = compStagePropFilter
			return nil
		case calDAVQName("prop-filter"):
			if stage > compStagePropFilter {
				return invalidFilter("CALDAV:comp-filter carries CALDAV:prop-filter after a CALDAV:comp-filter")
			}
			decoded, fault := decodePropFilter(dec, child)
			if fault != nil {
				return fault
			}
			filter.PropFilter = append(filter.PropFilter, *decoded)
			stage = compStagePropFilter
			return nil
		case calDAVQName("comp-filter"):
			decoded, fault := decodeCompFilter(dec, child, depth+1)
			if fault != nil {
				return fault
			}
			filter.CompFilter = append(filter.CompFilter, *decoded)
			stage = compStageCompFilter
			return nil
		}
		return invalidFilter("CALDAV:comp-filter carries unexpected %s", xmlNameString(child.Name))
	})
	if err != nil {
		return nil, err
	}
	return filter, nil
}

// propFilterStage is how far through the RFC 4791 §9.7.2 sequence
// ((time-range | text-match)?, param-filter*) a prop-filter has been read.
type propFilterStage int

const (
	propStageMatch propFilterStage = iota
	propStageParamFilter
)

// decodePropFilter reads CALDAV:prop-filter, whose §9.7.2 content model is
// is-not-defined alone or ((time-range | text-match)?, param-filter*).
func decodePropFilter(dec *xml.Decoder, start xml.StartElement) (*propFilter, *reportGrammarFault) {
	name, fault := filterElementName(start, "prop-filter", "CALDAV:prop-filter")
	if fault != nil {
		return nil, fault
	}
	filter := &propFilter{Name: name}
	stage := propStageMatch
	err := walkElementChildren(dec, start, "CALDAV:prop-filter", invalidFilter, func(dec *xml.Decoder, child xml.StartElement) *reportGrammarFault {
		alone := stage == propStageMatch
		if handled, present, fault := decodeIsNotDefined(dec, child, "CALDAV:prop-filter", alone, filter.IsNotDefined != nil); handled {
			if present {
				filter.IsNotDefined = &struct{}{}
			}
			return fault
		}
		switch child.Name {
		case calDAVQName("time-range"), calDAVQName("text-match"):
			if stage != propStageMatch || filter.TimeRange != nil || filter.TextMatch != nil {
				return invalidFilter("CALDAV:prop-filter carries %s out of the order §9.7.2 defines", xmlNameString(child.Name))
			}
			if child.Name == calDAVQName("time-range") {
				decoded, fault := decodeTimeRange(dec, child)
				if fault != nil {
					return fault
				}
				filter.TimeRange = decoded
			} else {
				decoded, fault := decodeTextMatch(dec, child)
				if fault != nil {
					return fault
				}
				filter.TextMatch = decoded
			}
			stage = propStageParamFilter
			return nil
		case calDAVQName("param-filter"):
			decoded, fault := decodeParamFilter(dec, child)
			if fault != nil {
				return fault
			}
			filter.ParamFilter = append(filter.ParamFilter, *decoded)
			stage = propStageParamFilter
			return nil
		}
		return invalidFilter("CALDAV:prop-filter carries unexpected %s", xmlNameString(child.Name))
	})
	if err != nil {
		return nil, err
	}
	return filter, nil
}

// decodeParamFilter reads CALDAV:param-filter, whose §9.7.3 content model is
// is-not-defined alone or an optional text-match. Both alternatives are a
// single child, so there is no sequence to track.
func decodeParamFilter(dec *xml.Decoder, start xml.StartElement) (*paramFilter, *reportGrammarFault) {
	name, fault := filterElementName(start, "param-filter", "CALDAV:param-filter")
	if fault != nil {
		return nil, fault
	}
	filter := &paramFilter{Name: name}
	err := walkElementChildren(dec, start, "CALDAV:param-filter", invalidFilter, func(dec *xml.Decoder, child xml.StartElement) *reportGrammarFault {
		alone := filter.TextMatch == nil
		if handled, present, fault := decodeIsNotDefined(dec, child, "CALDAV:param-filter", alone, filter.IsNotDefined != nil); handled {
			if present {
				filter.IsNotDefined = &struct{}{}
			}
			return fault
		}
		if child.Name != calDAVQName("text-match") {
			return invalidFilter("CALDAV:param-filter carries unexpected %s", xmlNameString(child.Name))
		}
		if !alone {
			return invalidFilter("CALDAV:param-filter carries more than one child")
		}
		decoded, fault := decodeTextMatch(dec, child)
		if fault != nil {
			return fault
		}
		filter.TextMatch = decoded
		return nil
	})
	if err != nil {
		return nil, err
	}
	return filter, nil
}

// filterElementName reads the name attribute §9.7.1 through §9.7.3 require of
// each filter element. A name that is no iCalendar name at all cannot be
// matched against any well-formed calendar object, which is what §7.8.8 makes
// CALDAV:supported-filter report.
func filterElementName(start xml.StartElement, element, label string) (string, *reportGrammarFault) {
	if fault := checkAttributes(start, label, "name"); fault != nil {
		return "", invalidFilter("%s", fault.reason)
	}
	name, present := attributeValue(start, "name")
	if !present {
		return "", invalidFilter("%s carries no name attribute", label)
	}
	if !validICalendarToken(name) {
		return "", unsupportedFilterElement(element, name)
	}
	return name, nil
}

// decodeIsNotDefined reads CALDAV:is-not-defined, whose §9.7.4 definition makes
// it the only child its parent may carry. alone says no sibling has been read
// yet and seen says this parent already carries one; either rule broken is a
// content-model violation. handled is false for any other element, so the
// caller can match it against the rest of its own model, and present says the
// caller should record the element it just consumed.
func decodeIsNotDefined(dec *xml.Decoder, child xml.StartElement, parent string, alone, seen bool) (handled, present bool, fault *reportGrammarFault) {
	if seen {
		return true, false, invalidFilter("%s carries CALDAV:is-not-defined beside %s", parent, xmlNameString(child.Name))
	}
	if child.Name != calDAVQName("is-not-defined") {
		return false, false, nil
	}
	if !alone {
		return true, false, invalidFilter("%s carries CALDAV:is-not-defined beside another child", parent)
	}
	if fault := expectEmptyElement(dec, child, "CALDAV:is-not-defined", invalidFilter); fault != nil {
		return true, false, fault
	}
	return true, true, nil
}

// decodeTextMatch reads CALDAV:text-match. §9.7.5 gives it two attributes,
// collation and negate-condition; match-type belongs to CardDAV's text-match
// and has no RFC 4791 counterpart.
func decodeTextMatch(dec *xml.Decoder, start xml.StartElement) (*textMatch, *reportGrammarFault) {
	if fault := checkAttributes(start, "CALDAV:text-match", "collation", "negate-condition"); fault != nil {
		return nil, invalidFilter("%s", fault.reason)
	}
	collation, _ := attributeValue(start, "collation")
	negate, _ := attributeValue(start, "negate-condition")
	match := &textMatch{Collation: collation, NegateCondition: negate}
	if negate != "" && negate != "yes" && negate != "no" {
		return nil, invalidFilter("CALDAV:text-match negate-condition is %q, want yes or no", negate)
	}
	if !calendarCollationSupported(collation) {
		return nil, unsupportedCollation(collation)
	}
	text, fault := decodeTextElement(dec, start, "CALDAV:text-match")
	if fault != nil {
		return nil, invalidFilter("%s", fault.reason)
	}
	match.Text = text
	return match, nil
}

// decodeTimeRange reads CALDAV:time-range. The grammar fixes its attribute set
// and empty content model; semantic validation separately checks its bounds and
// whether the enclosing component or property admits a range.
func decodeTimeRange(dec *xml.Decoder, start xml.StartElement) (*timeRange, *reportGrammarFault) {
	if fault := expectEmptyElement(dec, start, "CALDAV:time-range", invalidFilter, "start", "end"); fault != nil {
		return nil, fault
	}
	from, _ := attributeValue(start, "start")
	until, _ := attributeValue(start, "end")
	return &timeRange{Start: from, End: until}, nil
}

// calendarTimeRangeComponents and calendarTimeRangeProperties are the
// components and properties RFC 4791 §9.9 defines an overlap test for. A
// CALDAV:time-range naming anything else has no defined result.
var calendarTimeRangeComponents = nameSet("VEVENT", "VTODO", "VJOURNAL", "VFREEBUSY", "VALARM")

var calendarTimeRangeProperties = nameSet(
	"COMPLETED", "CREATED", "DTEND", "DTSTAMP", "DTSTART", "DUE", "LAST-MODIFIED",
)

func validateCalendarFilterSemantics(filter *calFilter) *reportGrammarFault {
	if filter == nil {
		return nil
	}
	if !strings.EqualFold(filter.CompFilter.Name, "VCALENDAR") {
		return invalidFilter("the root CALDAV:comp-filter must name VCALENDAR")
	}
	return validateCompFilterSemantics(&filter.CompFilter)
}

// validateCompFilterSemantics checks the rules §9.9 adds to the §9.7 content
// model. A nesting no calendar object can satisfy is left alone: §9.7.1 does
// not forbid it, so it matches nothing rather than failing the request.
func validateCompFilterSemantics(filter *compFilter) *reportGrammarFault {
	name := strings.ToUpper(filter.Name)
	if filter.TimeRange != nil {
		if !calendarTimeRangeComponents.contains(name) {
			return invalidFilter("CALDAV:time-range cannot be applied to component %s", name)
		}
		if !validTimeRange(filter.TimeRange) {
			return invalidFilter("CALDAV:comp-filter %s carries an invalid CALDAV:time-range", name)
		}
	}
	for i := range filter.PropFilter {
		property := &filter.PropFilter[i]
		if property.TimeRange == nil {
			continue
		}
		propertyName := strings.ToUpper(property.Name)
		if !calendarTimeRangeProperties.contains(propertyName) {
			return invalidFilter("CALDAV:time-range cannot be applied to property %s", propertyName)
		}
		if !validTimeRange(property.TimeRange) {
			return invalidFilter("CALDAV:prop-filter %s carries an invalid CALDAV:time-range", propertyName)
		}
	}
	for i := range filter.CompFilter {
		if fault := validateCompFilterSemantics(&filter.CompFilter[i]); fault != nil {
			return fault
		}
	}
	return nil
}

// Request-side CALDAV:time-range checks. These judge what the request
// spelled -- the value form RFC 4791 §9.9 requires and the §7.8/§7.9 bounds --
// rather than whether stored data intersects it, which is time_range.go's job.

func validTimeRange(tr *timeRange) bool {
	if tr == nil {
		return true
	}
	_, _, ok := calendarTimeRangeBounds(tr)
	return ok
}

// reportTimeRangeDateLimitFault checks every CALDAV:time-range the request
// carries against the collection's CALDAV:min-date-time and
// CALDAV:max-date-time. RFC 4791 §7.8 and §7.9 bound the *request's* range,
// inclusively at both ends, which is a different rule from the §5.3.2.1
// preconditions bounding stored data. It returns the precondition to answer
// with, or an empty string when every range is within the limits.
//
// Only an attribute the request actually spells is tested: an omitted one means
// an infinity that no finite limit could contain.
func reportTimeRangeDateLimitFault(filter *calFilter, tr *timeRange, calData *calendarDataEl) string {
	minTime, maxTime := ical.DateLimits()
	exceeds := func(instant time.Time) string {
		switch {
		case instant.Before(minTime):
			return "min-date-time"
		case instant.After(maxTime):
			return "max-date-time"
		default:
			return ""
		}
	}
	for _, value := range reportTimeRangeValues(filter, tr) {
		instant, ok := parseUTCDateTime(value)
		if !ok {
			// The form is the grammar's to refuse; an unparseable value carries
			// no instant to compare against a limit.
			continue
		}
		if condition := exceeds(instant); condition != "" {
			return condition
		}
	}
	if calData == nil {
		return ""
	}
	// The §9.6.5–§9.6.7 ranges bound the request the same way a CALDAV:time-range
	// does, so the same limits reach them. Both endpoints are required there, so
	// there is no infinity to exempt.
	for _, r := range []*calendarRange{calData.Expand, calData.LimitRecurrenceSet, calData.LimitFreeBusySet} {
		if r == nil {
			continue
		}
		if condition := exceeds(r.Start); condition != "" {
			return condition
		}
		if condition := exceeds(r.End); condition != "" {
			return condition
		}
	}
	return ""
}

func reportTimeRangeValues(filter *calFilter, tr *timeRange) []string {
	var values []string
	appendRange := func(r *timeRange) {
		if r == nil {
			return
		}
		if strings.TrimSpace(r.Start) != "" {
			values = append(values, r.Start)
		}
		if strings.TrimSpace(r.End) != "" {
			values = append(values, r.End)
		}
	}
	appendRange(tr)
	if filter != nil {
		var walk func(*compFilter)
		walk = func(component *compFilter) {
			appendRange(component.TimeRange)
			for i := range component.PropFilter {
				appendRange(component.PropFilter[i].TimeRange)
			}
			for i := range component.CompFilter {
				walk(&component.CompFilter[i])
			}
		}
		walk(&filter.CompFilter)
	}
	return values
}

// calendarTimeRangeBounds resolves a CALDAV:time-range to the instants it
// spans. RFC 4791 §9.9 requires both attributes to be a "date with UTC time",
// so a DATE, a floating DATE-TIME, or a numeric offset is refused rather than
// interpreted; an omitted attribute means -infinity or +infinity, and at least
// one has to be present.
func calendarTimeRangeBounds(tr *timeRange) (time.Time, time.Time, bool) {
	if tr == nil {
		return time.Time{}, time.Time{}, false
	}

	hasStart := strings.TrimSpace(tr.Start) != ""
	hasEnd := strings.TrimSpace(tr.End) != ""
	if !hasStart && !hasEnd {
		return time.Time{}, time.Time{}, false
	}

	var start time.Time
	var ok bool
	if hasStart {
		start, ok = parseUTCDateTime(tr.Start)
		if !ok {
			return time.Time{}, time.Time{}, false
		}
	}

	end := ical.RecurrenceUntilSentinel
	if hasEnd {
		end, ok = parseUTCDateTime(tr.End)
		if !ok {
			return time.Time{}, time.Time{}, false
		}
	}
	if hasStart && !end.After(start) {
		return time.Time{}, time.Time{}, false
	}
	return start, end, true
}

// parseUTCDateTime accepts only the iCalendar "date with UTC time" spelling.
func parseUTCDateTime(value string) (time.Time, bool) {
	value = strings.TrimSpace(value)
	if form, ok := parseICalDateForm(value); !ok || form != icalUTCDateTime {
		return time.Time{}, false
	}
	parsed, err := ical.ParseDateTime(value)
	return parsed, err == nil
}
