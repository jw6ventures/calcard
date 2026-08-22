package dav

import (
	"encoding/xml"
	"strings"
	"time"
)

// The RFC 4791 §9.6 content model of CALDAV:calendar-data, read by a token walk
// rather than by struct tags. DAV:prop keeps its tag decoding, because its
// content is an open property list; calendar-data sits inside it and is the one
// child that does have a content model, so it hooks the decoder through
// UnmarshalXML instead of the walk in report_calendar_grammar.go.

// calendarDataStage is how far through the §9.6 sequence
// (comp?, (expand | limit-recurrence-set)?, limit-freebusy-set?) a
// CALDAV:calendar-data element has been read. A child belonging to an earlier
// stage is out of document order.
type calendarDataStage int

const (
	dataStageComp calendarDataStage = iota
	dataStageRecurrence
	dataStageFreeBusy
	dataStageEnd
)

// UnmarshalXML reads CALDAV:calendar-data against its §9.6 content model. A
// violation is recorded on the element rather than returned: the permissive
// decode in report() runs over every REPORT body, and only the §9 grammar pass
// is entitled to turn a content-model violation into a rejection. A recorded
// fault also leaves the selection at its zero value, so a request the grammar
// pass does not reach projects the whole resource rather than half of one.
func (c *calendarDataEl) UnmarshalXML(dec *xml.Decoder, start xml.StartElement) error {
	parsed, fault := decodeCalendarDataElement(dec, start)
	if fault != nil {
		c.fault = fault
		drainElement(dec)
		return nil
	}
	*c = *parsed
	return nil
}

// empty reports whether the selection asks for the resource in its entirety.
// The CALDAV:comp selection and the three range elements are the only children
// §9.6 defines, so a body carrying none of them narrows nothing.
func (c *calendarDataEl) empty() bool {
	return c.Comp == nil && c.Expand == nil && c.LimitRecurrenceSet == nil && c.LimitFreeBusySet == nil
}

// drainElement consumes what is left of the element an Unmarshaler was handed.
// encoding/xml fences the decoder at that element's end tag and rejects an
// Unmarshaler returning before it, so a walk that stopped early has to read the
// remainder or the fault it recorded is replaced by a decoder error and the
// whole body is refused.
func drainElement(dec *xml.Decoder) {
	for {
		if _, err := dec.Token(); err != nil {
			return
		}
	}
}

func decodeCalendarDataElement(dec *xml.Decoder, start xml.StartElement) (*calendarDataEl, *reportGrammarFault) {
	if fault := checkAttributes(start, "CALDAV:calendar-data", "content-type", "version"); fault != nil {
		return nil, fault
	}
	element := &calendarDataEl{}
	if value, ok := attributeValue(start, "content-type"); ok {
		element.ContentType = &value
	}
	if value, ok := attributeValue(start, "version"); ok {
		element.Version = &value
	}

	stage := dataStageComp
	fault := walkElementChildren(dec, start, "CALDAV:calendar-data", malformedReport, func(dec *xml.Decoder, child xml.StartElement) *reportGrammarFault {
		switch {
		case stage == dataStageComp && child.Name == calDAVQName("comp"):
			comp, fault := decodeCalendarDataComp(dec, child)
			if fault != nil {
				return fault
			}
			element.Comp = comp
			stage = dataStageRecurrence
			return nil
		case stage <= dataStageRecurrence && child.Name == calDAVQName("expand"):
			r, fault := decodeCalendarDataRange(dec, child, "CALDAV:expand")
			if fault != nil {
				return fault
			}
			element.Expand = r
			stage = dataStageFreeBusy
			return nil
		case stage <= dataStageRecurrence && child.Name == calDAVQName("limit-recurrence-set"):
			r, fault := decodeCalendarDataRange(dec, child, "CALDAV:limit-recurrence-set")
			if fault != nil {
				return fault
			}
			element.LimitRecurrenceSet = r
			stage = dataStageFreeBusy
			return nil
		case stage <= dataStageFreeBusy && child.Name == calDAVQName("limit-freebusy-set"):
			r, fault := decodeCalendarDataRange(dec, child, "CALDAV:limit-freebusy-set")
			if fault != nil {
				return fault
			}
			element.LimitFreeBusySet = r
			stage = dataStageEnd
			return nil
		}
		return malformedReport("CALDAV:calendar-data carries %s out of the order §9.6 defines", xmlNameString(child.Name))
	})
	if fault != nil {
		return nil, fault
	}
	return element, nil
}

// decodeCalendarDataComp reads CALDAV:comp, whose §9.6.1 content model is
// ((allprop | prop*), (allcomp | comp*)) with a required name. The alternations
// are exclusive, so allprop beside prop, or allcomp beside comp, is malformed
// rather than a union.
func decodeCalendarDataComp(dec *xml.Decoder, start xml.StartElement) (*calendarComp, *reportGrammarFault) {
	if fault := checkAttributes(start, "CALDAV:comp", "name"); fault != nil {
		return nil, fault
	}
	name, ok := attributeValue(start, "name")
	if !ok {
		return nil, malformedReport("CALDAV:comp carries no name attribute")
	}
	// RFC 4791 §7.7 requires non-standard component names to be selectable, so
	// the name is checked for the iCalendar token grammar rather than against a
	// list of the names the server happens to know.
	if !validICalendarToken(name) {
		return nil, malformedReport("CALDAV:comp names %q, which is not an iCalendar name", name)
	}

	comp := &calendarComp{Name: name}
	propertiesClosed := false
	fault := walkElementChildren(dec, start, "CALDAV:comp", malformedReport, func(dec *xml.Decoder, child xml.StartElement) *reportGrammarFault {
		switch child.Name {
		case calDAVQName("allprop"):
			if propertiesClosed || comp.AllProp || len(comp.Prop) != 0 {
				return malformedReport("CALDAV:comp carries CALDAV:allprop out of the order §9.6.1 defines")
			}
			if fault := expectEmptyElement(dec, child, "CALDAV:allprop", malformedReport); fault != nil {
				return fault
			}
			comp.AllProp = true
			return nil
		case calDAVQName("prop"):
			if propertiesClosed || comp.AllProp {
				return malformedReport("CALDAV:comp carries CALDAV:prop out of the order §9.6.1 defines")
			}
			property, fault := decodeCalendarDataProp(dec, child)
			if fault != nil {
				return fault
			}
			comp.Prop = append(comp.Prop, *property)
			return nil
		case calDAVQName("allcomp"):
			if comp.AllComp || len(comp.Comp) != 0 {
				return malformedReport("CALDAV:comp carries CALDAV:allcomp out of the order §9.6.1 defines")
			}
			if fault := expectEmptyElement(dec, child, "CALDAV:allcomp", malformedReport); fault != nil {
				return fault
			}
			comp.AllComp = true
			propertiesClosed = true
			return nil
		case calDAVQName("comp"):
			if comp.AllComp {
				return malformedReport("CALDAV:comp carries CALDAV:comp out of the order §9.6.1 defines")
			}
			child, fault := decodeCalendarDataComp(dec, child)
			if fault != nil {
				return fault
			}
			comp.Comp = append(comp.Comp, *child)
			propertiesClosed = true
			return nil
		}
		return malformedReport("CALDAV:comp carries unexpected %s", xmlNameString(child.Name))
	})
	if fault != nil {
		return nil, fault
	}
	return comp, nil
}

// decodeCalendarDataProp reads CALDAV:prop, which §9.6.4 declares EMPTY with a
// required name and a novalue defaulting to "no".
func decodeCalendarDataProp(dec *xml.Decoder, start xml.StartElement) (*calendarProp, *reportGrammarFault) {
	if fault := expectEmptyElement(dec, start, "CALDAV:prop", malformedReport, "name", "novalue"); fault != nil {
		return nil, fault
	}
	name, ok := attributeValue(start, "name")
	if !ok {
		return nil, malformedReport("CALDAV:prop carries no name attribute")
	}
	if !validICalendarToken(name) {
		return nil, malformedReport("CALDAV:prop names %q, which is not an iCalendar name", name)
	}
	property := &calendarProp{Name: name}
	switch novalue, ok := attributeValue(start, "novalue"); {
	case !ok, novalue == "no":
	case novalue == "yes":
		property.NoValue = true
	default:
		return nil, malformedReport("CALDAV:prop carries novalue=%q, which §9.6.4 does not define", novalue)
	}
	return property, nil
}

// decodeCalendarDataRange reads CALDAV:expand, CALDAV:limit-recurrence-set or
// CALDAV:limit-freebusy-set. All three are EMPTY and declare both endpoints
// #REQUIRED, so the open range CALDAV:time-range allows is malformed here.
func decodeCalendarDataRange(dec *xml.Decoder, start xml.StartElement, label string) (*calendarRange, *reportGrammarFault) {
	if fault := expectEmptyElement(dec, start, label, malformedReport, "start", "end"); fault != nil {
		return nil, fault
	}
	from, _ := attributeValue(start, "start")
	until, _ := attributeValue(start, "end")
	begin, end, ok := closedCalendarRangeBounds(from, until)
	if !ok {
		return nil, malformedReport("%s requires a start and an end, each a date with UTC time, with end greater than start", label)
	}
	return &calendarRange{Start: begin, End: end}, nil
}

// closedCalendarRangeBounds is calendarTimeRangeBounds with both endpoints
// required. RFC 4791 §9.6.5 through §9.6.7 declare start and end #REQUIRED, so
// the open range a CALDAV:time-range permits does not reach them; the value
// form and the ordering rule are the same.
func closedCalendarRangeBounds(from, until string) (time.Time, time.Time, bool) {
	if strings.TrimSpace(from) == "" || strings.TrimSpace(until) == "" {
		return time.Time{}, time.Time{}, false
	}
	start, ok := parseUTCDateTime(from)
	if !ok {
		return time.Time{}, time.Time{}, false
	}
	end, ok := parseUTCDateTime(until)
	if !ok {
		return time.Time{}, time.Time{}, false
	}
	if !end.After(start) {
		return time.Time{}, time.Time{}, false
	}
	return start, end, true
}
