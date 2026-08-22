package dav

import (
	"net/http"
	"strings"
	"testing"
)

// calendarDataQueryBody wraps a CALDAV:calendar-data selector in the smallest
// calendar-query that reaches the §9.6 walker: the element is a child of
// DAV:prop, which is where §9.6 says it is spelled.
func calendarDataQueryBody(calendarData string) string {
	return calendarQueryBody(`<D:prop><D:getetag/>` + calendarData + `</D:prop>` +
		`<C:filter>` + grammarVEventFilter + `</C:filter>`)
}

// assertCalendarDataBodies runs each body and requires the status. A §9.6
// content-model violation names no precondition, so RFC 4791 §1.3 does not
// reach it and the answer is an ordinary 400 rather than a 403.
func assertCalendarDataBodies(t *testing.T, want int, bodies map[string]string) {
	t.Helper()
	for name, calendarData := range bodies {
		t.Run(name, func(t *testing.T) {
			rr := runCalendarReport(t, calendarDataQueryBody(calendarData))
			if rr.Code != want {
				t.Fatalf("status = %d, want %d; body: %s", rr.Code, want, rr.Body.String())
			}
		})
	}
}

// RFC 4791 §9.6 gives CALDAV:calendar-data the request-side content model
// (comp?, (expand | limit-recurrence-set)?, limit-freebusy-set?). The three
// positions are a sequence and the middle one is an alternation, so neither a
// repeat nor a reordering is admitted.
func TestRFC4791_CalendarDataContentModel(t *testing.T) {
	const expand = `<C:expand start="20240601T000000Z" end="20240602T000000Z"/>`
	const limitRecurrence = `<C:limit-recurrence-set start="20240601T000000Z" end="20240602T000000Z"/>`
	const limitFreeBusy = `<C:limit-freebusy-set start="20240601T000000Z" end="20240602T000000Z"/>`
	const comp = `<C:comp name="VCALENDAR"/>`

	assertCalendarDataBodies(t, http.StatusBadRequest, map[string]string{
		"comp after expand":                `<C:calendar-data>` + expand + comp + `</C:calendar-data>`,
		"expand and limit-recurrence-set":  `<C:calendar-data>` + expand + limitRecurrence + `</C:calendar-data>`,
		"limit-recurrence-set and expand":  `<C:calendar-data>` + limitRecurrence + expand + `</C:calendar-data>`,
		"two comps":                        `<C:calendar-data>` + comp + comp + `</C:calendar-data>`,
		"two expands":                      `<C:calendar-data>` + expand + expand + `</C:calendar-data>`,
		"two limit-freebusy-sets":          `<C:calendar-data>` + limitFreeBusy + limitFreeBusy + `</C:calendar-data>`,
		"limit-freebusy-set before expand": `<C:calendar-data>` + limitFreeBusy + expand + `</C:calendar-data>`,
		"a prop as a direct child":         `<C:calendar-data><C:prop name="UID"/></C:calendar-data>`,
		"an allcomp as a direct child":     `<C:calendar-data><C:allcomp/></C:calendar-data>`,
		"character data beside a selector": `<C:calendar-data>nonsense` + comp + `</C:calendar-data>`,
		"an undefined attribute":           `<C:calendar-data novalue="yes">` + comp + `</C:calendar-data>`,
		"an unexpected CalDAV child":       `<C:calendar-data><C:timezone>BEGIN:VCALENDAR</C:timezone></C:calendar-data>`,
		"expand carrying a child":          `<C:calendar-data><C:expand start="20240601T000000Z" end="20240602T000000Z">` + comp + `</C:expand></C:calendar-data>`,
	})

	// The legal shapes, in each of the positions the sequence admits.
	assertCalendarDataBodies(t, http.StatusMultiStatus, map[string]string{
		"nothing at all":                 `<C:calendar-data/>`,
		"comp alone":                     `<C:calendar-data>` + comp + `</C:calendar-data>`,
		"comp then expand":               `<C:calendar-data>` + comp + expand + `</C:calendar-data>`,
		"comp then limit-recurrence-set": `<C:calendar-data>` + comp + limitRecurrence + `</C:calendar-data>`,
		"the whole sequence":             `<C:calendar-data>` + comp + expand + limitFreeBusy + `</C:calendar-data>`,
		"limit-freebusy-set alone":       `<C:calendar-data>` + limitFreeBusy + `</C:calendar-data>`,
	})
}

// RFC 4791 §9.6.1 gives CALDAV:comp the content model
// ((allprop | prop*), (allcomp | comp*)) with a required name. Both
// alternations are exclusive.
func TestRFC4791_CalendarDataCompContentModel(t *testing.T) {
	data := func(inner string) string {
		return `<C:calendar-data>` + inner + `</C:calendar-data>`
	}

	assertCalendarDataBodies(t, http.StatusBadRequest, map[string]string{
		"comp with no name":       data(`<C:comp/>`),
		"comp with an empty name": data(`<C:comp name=""/>`),
		"comp naming a non-token": data(`<C:comp name="not a name"/>`),
		"leading whitespace":      data(`<C:comp name=" VEVENT"/>`),
		"trailing whitespace":     data(`<C:comp name="VEVENT "/>`),
		"allprop beside prop":     data(`<C:comp name="VEVENT"><C:allprop/><C:prop name="UID"/></C:comp>`),
		"prop beside allprop":     data(`<C:comp name="VEVENT"><C:prop name="UID"/><C:allprop/></C:comp>`),
		"two allprops":            data(`<C:comp name="VEVENT"><C:allprop/><C:allprop/></C:comp>`),
		"allcomp beside comp":     data(`<C:comp name="VEVENT"><C:allcomp/><C:comp name="VALARM"/></C:comp>`),
		"comp beside allcomp":     data(`<C:comp name="VEVENT"><C:comp name="VALARM"/><C:allcomp/></C:comp>`),
		"prop after comp":         data(`<C:comp name="VEVENT"><C:comp name="VALARM"/><C:prop name="UID"/></C:comp>`),
		"allprop after allcomp":   data(`<C:comp name="VEVENT"><C:allcomp/><C:allprop/></C:comp>`),
		"comp carrying char data": data(`<C:comp name="VEVENT">nonsense</C:comp>`),
		"an undefined attribute":  data(`<C:comp name="VEVENT" novalue="yes"/>`),
		"an unexpected child":     data(`<C:comp name="VEVENT"><C:expand start="20240601T000000Z" end="20240602T000000Z"/></C:comp>`),
	})

	assertCalendarDataBodies(t, http.StatusMultiStatus, map[string]string{
		"a bare comp":          data(`<C:comp name="VCALENDAR"/>`),
		"allprop then allcomp": data(`<C:comp name="VCALENDAR"><C:allprop/><C:allcomp/></C:comp>`),
		"props then comps":     data(`<C:comp name="VCALENDAR"><C:prop name="VERSION"/><C:comp name="VEVENT"><C:prop name="UID"/></C:comp></C:comp>`),
		"allprop then comps":   data(`<C:comp name="VCALENDAR"><C:allprop/><C:comp name="VEVENT"/></C:comp>`),
		"props then allcomp":   data(`<C:comp name="VCALENDAR"><C:prop name="VERSION"/><C:allcomp/></C:comp>`),
		"nested three deep":    data(`<C:comp name="VCALENDAR"><C:comp name="VEVENT"><C:comp name="VALARM"><C:allprop/></C:comp></C:comp></C:comp>`),
	})
}

// RFC 4791 §9.6.4 declares CALDAV:prop EMPTY with a required name and a novalue
// that takes only "yes" or "no", defaulting to "no".
func TestRFC4791_CalendarDataPropAttributes(t *testing.T) {
	prop := func(attributes string) string {
		return `<C:calendar-data><C:comp name="VEVENT"><C:prop ` + attributes + `/></C:comp></C:calendar-data>`
	}

	assertCalendarDataBodies(t, http.StatusBadRequest, map[string]string{
		"no name":                               prop(``),
		"an empty name":                         prop(`name=""`),
		"a name that is not an iCalendar token": prop(`name="not a name"`),
		"leading whitespace in the name":        prop(`name=" UID"`),
		"trailing whitespace in the name":       prop(`name="UID "`),
		"novalue outside its enumeration":       prop(`name="UID" novalue="maybe"`),
		"novalue in the wrong case":             prop(`name="UID" novalue="YES"`),
		"an undefined attribute":                prop(`name="UID" expand="yes"`),
		"a child element":                       `<C:calendar-data><C:comp name="VEVENT"><C:prop name="UID"><C:allprop/></C:prop></C:comp></C:calendar-data>`,
	})

	assertCalendarDataBodies(t, http.StatusMultiStatus, map[string]string{
		"novalue absent":      prop(`name="UID"`),
		"novalue no":          prop(`name="UID" novalue="no"`),
		"novalue yes":         prop(`name="UID" novalue="yes"`),
		"a non-standard name": prop(`name="X-ALT-DESC"`),
	})
}

// RFC 4791 §9.6.5 through §9.6.7 declare start and end #REQUIRED on all three
// range elements, each a "date with UTC time", with end greater than start. The
// open range CALDAV:time-range permits does not reach them.
func TestRFC4791_CalendarDataRangeAttributes(t *testing.T) {
	elements := []string{"expand", "limit-recurrence-set", "limit-freebusy-set"}

	refused := map[string]string{
		"no start at all":          `end="20240602T000000Z"`,
		"no end at all":            `start="20240601T000000Z"`,
		"neither attribute":        ``,
		"an empty start":           `start="" end="20240602T000000Z"`,
		"a DATE start":             `start="20240601" end="20240602T000000Z"`,
		"a floating start":         `start="20240601T000000" end="20240602T000000Z"`,
		"a numeric offset":         `start="20240601T000000+0100" end="20240602T000000Z"`,
		"an extended-format start": `start="2024-06-01T00:00:00Z" end="20240602T000000Z"`,
		"end equal to start":       `start="20240601T000000Z" end="20240601T000000Z"`,
		"end before start":         `start="20240602T000000Z" end="20240601T000000Z"`,
		"an undefined attribute":   `start="20240601T000000Z" end="20240602T000000Z" name="VEVENT"`,
	}
	accepted := map[string]string{
		"both endpoints in order": `start="20240601T000000Z" end="20240602T000000Z"`,
		"a one-second range":      `start="20240601T000000Z" end="20240601T000001Z"`,
	}

	for _, element := range elements {
		t.Run(element, func(t *testing.T) {
			for name, attributes := range refused {
				assertCalendarDataBodies(t, http.StatusBadRequest, map[string]string{
					name: `<C:calendar-data><C:` + element + ` ` + attributes + `/></C:calendar-data>`,
				})
			}
			for name, attributes := range accepted {
				assertCalendarDataBodies(t, http.StatusMultiStatus, map[string]string{
					name: `<C:calendar-data><C:` + element + ` ` + attributes + `/></C:calendar-data>`,
				})
			}
		})
	}
}

// The §7.8 CALDAV:min-date-time and CALDAV:max-date-time bounds measure every
// range the request spells, which includes the three §9.6 ones. §1.3 puts a
// precondition failure at 403.
func TestRFC4791_CalendarDataRangesHonourCollectionDateLimits(t *testing.T) {
	tests := map[string]struct {
		attributes string
		condition  string
	}{
		"before min-date-time": {`start="18991231T235959Z" end="20240602T000000Z"`, "min-date-time"},
		"after max-date-time":  {`start="20240601T000000Z" end="21010101T000000Z"`, "max-date-time"},
	}

	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			body := calendarDataQueryBody(`<C:calendar-data><C:expand ` + test.attributes + `/></C:calendar-data>`)
			assertErrorConditions(t, runCalendarReport(t, body), http.StatusForbidden, calQN(test.condition))
		})
	}
}

// RFC 4791 §7.7 requires non-standard component, property and parameter names
// to be supported in CALDAV:calendar-data, so the selector checks the iCalendar
// token grammar rather than a list of the names the server happens to know.
func TestRFC4791_CalendarDataAcceptsNonStandardNames(t *testing.T) {
	assertCalendarDataBodies(t, http.StatusMultiStatus, map[string]string{
		"a non-standard component": `<C:calendar-data><C:comp name="X-WOMBAT"/></C:calendar-data>`,
		"a non-standard property":  `<C:calendar-data><C:comp name="VEVENT"><C:prop name="X-ALT-DESC"/></C:comp></C:calendar-data>`,
		"both at once":             `<C:calendar-data><C:comp name="VCALENDAR"><C:comp name="X-WOMBAT"><C:prop name="X-DEPTH"/></C:comp></C:comp></C:calendar-data>`,
	})
}

// The §9.6 walker hooks the decoder inside DAV:prop, which stays an open
// property list. The undefined-property 404 propstat is the guard on that: a
// body naming an unknown property beside a full calendar-data selector must
// still report the unknown one absent rather than lose it.
func TestRFC4791_CalendarDataSelectorLeavesTheOpenPropertyListIntact(t *testing.T) {
	body := calendarQueryBody(
		`<D:prop><D:getetag/><X:mystery xmlns:X="urn:example:unknown"/>` +
			`<C:calendar-data><C:comp name="VCALENDAR"><C:comp name="VEVENT"><C:prop name="UID"/></C:comp></C:comp></C:calendar-data>` +
			`</D:prop><C:filter>` + grammarVEventFilter + `</C:filter>`)

	ms := decodeMultistatus(t, runCalendarReport(t, body))
	resp := ms.responseForHref(t, "/dav/calendars/1/standup.ics")
	resp.assertPropStatus(t, davQN("getetag"), http.StatusOK)
	resp.assertPropStatus(t, calQN("calendar-data"), http.StatusOK)
	resp.assertPropStatus(t, qn("urn:example:unknown", "mystery"), http.StatusNotFound)
}

// A §9.6 violation is carried out of the decoder hook rather than raised there,
// so the permissive decode every REPORT body goes through still succeeds and
// only the grammar pass turns the violation into a rejection. Without that, the
// hook fails the decode for the whole body and the diagnostic below is replaced
// by whatever encoding/xml says about an element left half-read.
func TestRFC4791_CalendarDataFaultReachesTheGrammarPass(t *testing.T) {
	violations := map[string]struct {
		calendarData string
		reason       string
	}{
		"an undefined attribute": {
			calendarData: `<C:calendar-data novalue="yes"/>`,
			reason:       `CALDAV:calendar-data carries undefined attribute "novalue"`,
		},
		"an unexpected child": {
			calendarData: `<C:calendar-data><C:allcomp/></C:calendar-data>`,
			reason:       "CALDAV:calendar-data carries urn:ietf:params:xml:ns:caldav allcomp out of the order §9.6 defines",
		},
		"a child out of document order": {
			calendarData: `<C:calendar-data>` +
				`<C:limit-freebusy-set start="20240601T000000Z" end="20240602T000000Z"/>` +
				`<C:comp name="VCALENDAR"/></C:calendar-data>`,
			reason: "CALDAV:calendar-data carries urn:ietf:params:xml:ns:caldav comp out of the order §9.6 defines",
		},
		"a range missing an endpoint": {
			calendarData: `<C:calendar-data><C:expand start="20240601T000000Z"/></C:calendar-data>`,
			reason:       "CALDAV:expand requires a start and an end",
		},
		"a comp naming a non-token": {
			calendarData: `<C:calendar-data><C:comp name="not a name"/></C:calendar-data>`,
			reason:       `CALDAV:comp names "not a name"`,
		},
	}

	for name, violation := range violations {
		t.Run(name, func(t *testing.T) {
			body := []byte(calendarDataQueryBody(violation.calendarData))

			var report reportRequest
			if err := safeUnmarshalXML(body, &report); err != nil {
				t.Fatalf("the permissive decode rejected the body: %v", err)
			}

			fault := applyCalendarReportGrammar(&report, body)
			if fault == nil {
				t.Fatalf("the grammar pass accepted a body violating the §9.6 content model")
			}
			if !strings.Contains(fault.reason, violation.reason) {
				t.Errorf("fault reason = %q, want it to name the violation: %q", fault.reason, violation.reason)
			}
		})
	}
}
