package dav

import (
	"encoding/xml"
	"net/http"
)

// The CALDAV:free-busy-query body of RFC 4791 §9.11 and the Depth value that
// scopes it. The calendaring REPORT bodies that carry a property selector are
// read in report_calendar_grammar.go; this one carries none, so its whole
// content model fits here.

// freeBusyQueryRequest is the CALDAV:free-busy-query body of RFC 4791 §9.11:
// <!ELEMENT free-busy-query (time-range)>. The element admits exactly one
// child, so a CALDAV:filter is malformed rather than a second place to look for
// the range.
type freeBusyQueryRequest struct {
	TimeRange *timeRange
}

func parseFreeBusyQueryRequest(body []byte) (*freeBusyQueryRequest, *reportGrammarFault) {
	decoder, start, fault := openReportBody(body, calDAVQName("free-busy-query"))
	if fault != nil {
		return nil, fault
	}
	if fault := checkAttributes(start, "CALDAV:free-busy-query"); fault != nil {
		return nil, fault
	}
	request := &freeBusyQueryRequest{}
	fault = walkElementChildren(decoder, start, "CALDAV:free-busy-query", malformedReport, func(dec *xml.Decoder, child xml.StartElement) *reportGrammarFault {
		if child.Name != calDAVQName("time-range") {
			return malformedReport("CALDAV:free-busy-query carries unexpected %s", xmlNameString(child.Name))
		}
		if request.TimeRange != nil {
			return malformedReport("CALDAV:free-busy-query carries more than one CALDAV:time-range")
		}
		tr, fault := decodeTimeRange(dec, child)
		if fault != nil {
			return fault
		}
		request.TimeRange = tr
		return nil
	})
	if fault != nil {
		return nil, fault
	}
	if request.TimeRange == nil {
		return nil, malformedReport("CALDAV:free-busy-query carries no CALDAV:time-range")
	}
	return request, nil
}

// freeBusyExcludedByDepth reports whether the Depth header puts every calendar
// object resource out of the free-busy report's reach. RFC 4791 §7.10 considers
// only the resources the Depth value allows, on the same reading
// calendarQueryExcludedByDepth already applies: a collection is not itself a
// calendar object resource, so Depth: 0 -- the default when the header is
// absent -- reaches none of its members and the report answers with the
// FREEBUSY-less VFREEBUSY §7.10 requires when nothing matches. The report is
// refused on an object-resource Request-URI before this is reached, so a
// collection is the only target here.
func freeBusyExcludedByDepth(r *http.Request, report reportRequest) bool {
	if report.XMLName.Local != "free-busy-query" {
		return false
	}
	depth, ok := reportDepth(r)
	return ok && depth == "0"
}
