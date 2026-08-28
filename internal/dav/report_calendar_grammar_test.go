package dav

import (
	"encoding/xml"
	"fmt"
	"math"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/jw6ventures/calcard/internal/auth"
	"github.com/jw6ventures/calcard/internal/config"
	"github.com/jw6ventures/calcard/internal/ical"
	"github.com/jw6ventures/calcard/internal/store"
)

// grammarTestServer answers REPORTs against one calendar holding one event, so
// a body that survives the grammar pass produces a 207 and every rejection
// below is attributable to the body rather than to the target.
func grammarTestServer() *DavServer {
	return &DavServer{store: &store.Store{
		Calendars: &fakeCalendarRepo{accessible: []store.CalendarAccess{
			{Calendar: store.Calendar{ID: 1, UserID: 1, Name: "Test"}, Editor: true},
		}},
		Events: &fakeEventRepo{events: map[string]*store.Event{
			"1:standup": {CalendarID: 1, UID: "standup", ResourceName: "standup", ETag: "etag",
				RawICAL: "BEGIN:VCALENDAR\r\nVERSION:2.0\r\nBEGIN:VEVENT\r\nUID:standup\r\nSUMMARY:Standup\r\nEND:VEVENT\r\nEND:VCALENDAR\r\n"},
		}},
	}}
}

func runCalendarReport(t *testing.T, body string) *httptest.ResponseRecorder {
	t.Helper()
	req := httptest.NewRequest("REPORT", "/dav/calendars/1/", strings.NewReader(body))
	req.Header.Set("Depth", "1")
	req = req.WithContext(auth.WithUser(req.Context(), &store.User{ID: 1}))
	rr := httptest.NewRecorder()
	grammarTestServer().Report(rr, req)
	return rr
}

func calendarQueryBody(inner string) string {
	return `<?xml version="1.0" encoding="utf-8"?>
<C:calendar-query xmlns:D="DAV:" xmlns:C="urn:ietf:params:xml:ns:caldav">` + inner + `</C:calendar-query>`
}

func calendarFilterBody(inner string) string {
	return calendarQueryBody(`<D:prop><D:getetag/></D:prop><C:filter>` + inner + `</C:filter>`)
}

const grammarVEventFilter = `<C:comp-filter name="VCALENDAR"><C:comp-filter name="VEVENT"/></C:comp-filter>`

// grammarVTimezone is a valid CALDAV:timezone value, XML-escaped for a request
// body. RFC 4791 §7.8 requires the element to hold one valid VTIMEZONE, so a
// test about placement or cardinality has to carry a real definition.
var grammarVTimezone = grammarVTimezoneFor("Etc/UTC")

// grammarVTimezoneFor builds that definition for any TZID. The observance is a
// zero-offset placeholder, which is all a test about the element itself needs;
// a test about how values resolve names an IANA zone and lets the host's tzdata
// supply the real offsets.
func grammarVTimezoneFor(tzid string) string {
	return `BEGIN:VCALENDAR&#13;&#10;VERSION:2.0&#13;&#10;PRODID:-//test//EN&#13;&#10;` +
		`BEGIN:VTIMEZONE&#13;&#10;TZID:` + tzid + `&#13;&#10;BEGIN:STANDARD&#13;&#10;DTSTART:19700101T000000&#13;&#10;` +
		`TZOFFSETFROM:+0000&#13;&#10;TZOFFSETTO:+0000&#13;&#10;END:STANDARD&#13;&#10;END:VTIMEZONE&#13;&#10;END:VCALENDAR&#13;&#10;`
}

// RFC 4791 §9.5 gives CALDAV:calendar-query the content model
// ((DAV:allprop | DAV:propname | DAV:prop)?, CALDAV:filter, CALDAV:timezone?).
// A body outside it is malformed, which names no precondition, so RFC 4791 §1.3
// does not apply and the answer is an ordinary 400.
func TestRFC4791_CalendarQueryContentModel(t *testing.T) {
	tests := map[string]string{
		"two property selectors": calendarQueryBody(
			`<D:allprop/><D:prop><D:getetag/></D:prop><C:filter>` + grammarVEventFilter + `</C:filter>`),
		"prop and propname together": calendarQueryBody(
			`<D:prop><D:getetag/></D:prop><D:propname/><C:filter>` + grammarVEventFilter + `</C:filter>`),
		"property selector after the filter": calendarQueryBody(
			`<C:filter>` + grammarVEventFilter + `</C:filter><D:prop><D:getetag/></D:prop>`),
		"no filter":      calendarQueryBody(`<D:prop><D:getetag/></D:prop>`),
		"an empty body":  calendarQueryBody(``),
		"two filters":    calendarQueryBody(`<C:filter>` + grammarVEventFilter + `</C:filter><C:filter>` + grammarVEventFilter + `</C:filter>`),
		"timezone first": calendarQueryBody(`<C:timezone>BEGIN:VCALENDAR</C:timezone><C:filter>` + grammarVEventFilter + `</C:filter>`),
		"two timezones":  calendarQueryBody(`<C:filter>` + grammarVEventFilter + `</C:filter><C:timezone>` + grammarVTimezone + `</C:timezone><C:timezone>` + grammarVTimezone + `</C:timezone>`),
		"character data": calendarQueryBody(`nonsense<C:filter>` + grammarVEventFilter + `</C:filter>`),
		"unexpected child": calendarQueryBody(
			`<D:prop><D:getetag/></D:prop><C:filter>` + grammarVEventFilter + `</C:filter><D:href>/dav/calendars/1/standup.ics</D:href>`),
		// §9.6 makes CALDAV:calendar-data a child of DAV:prop, not of the report.
		"calendar-data outside the property selector": calendarQueryBody(
			`<C:calendar-data/><C:filter>` + grammarVEventFilter + `</C:filter>`),
		"a foreign root element":         `<C:calendar-query xmlns:C="urn:example:not-caldav"/>`,
		"an undefined root attribute":    strings.Replace(calendarQueryBody(`<C:filter>`+grammarVEventFilter+`</C:filter>`), `<C:calendar-query `, `<C:calendar-query limit="1" `, 1),
		"allprop carrying a DAV child":   calendarQueryBody(`<D:allprop><D:href>/dav/</D:href></D:allprop><C:filter>` + grammarVEventFilter + `</C:filter>`),
		"propname carrying an attribute": calendarQueryBody(`<D:propname limit="1"/><C:filter>` + grammarVEventFilter + `</C:filter>`),
		"timezone carrying an attribute": calendarQueryBody(`<C:filter>` + grammarVEventFilter + `</C:filter><C:timezone format="ical">BEGIN:VCALENDAR</C:timezone>`),
	}

	for name, body := range tests {
		t.Run(name, func(t *testing.T) {
			if rr := runCalendarReport(t, body); rr.Code != http.StatusBadRequest {
				t.Fatalf("status = %d, want 400; body: %s", rr.Code, rr.Body.String())
			}
		})
	}
}

// RFC 4791 §9.10 gives CALDAV:calendar-multiget the content model
// ((DAV:allprop | DAV:propname | DAV:prop)?, DAV:href+).
func TestRFC4791_CalendarMultigetContentModel(t *testing.T) {
	multiget := func(inner string) string {
		return `<?xml version="1.0" encoding="utf-8"?>
<C:calendar-multiget xmlns:D="DAV:" xmlns:C="urn:ietf:params:xml:ns:caldav">` + inner + `</C:calendar-multiget>`
	}
	href := `<D:href>/dav/calendars/1/standup.ics</D:href>`

	tests := map[string]string{
		"no href at all":                    multiget(`<D:prop><D:getetag/></D:prop>`),
		"an empty body":                     multiget(``),
		"a property selector after an href": multiget(href + `<D:prop><D:getetag/></D:prop>`),
		"two property selectors":            multiget(`<D:allprop/><D:prop><D:getetag/></D:prop>` + href),
		"an unexpected child":               multiget(`<D:prop><D:getetag/></D:prop>` + href + `<C:filter>` + grammarVEventFilter + `</C:filter>`),
		"character data":                    multiget(`nonsense` + href),
		"an undefined root attribute":       strings.Replace(multiget(href), `<C:calendar-multiget `, `<C:calendar-multiget limit="1" `, 1),
		"href carrying an attribute":        multiget(`<D:href format="uri">/dav/calendars/1/standup.ics</D:href>`),
		"allprop carrying a DAV child":      multiget(`<D:allprop><D:href>/dav/</D:href></D:allprop>` + href),
	}

	for name, body := range tests {
		t.Run(name, func(t *testing.T) {
			if rr := runCalendarReport(t, body); rr.Code != http.StatusBadRequest {
				t.Fatalf("status = %d, want 400; body: %s", rr.Code, rr.Body.String())
			}
		})
	}

	t.Run("one href is enough", func(t *testing.T) {
		rr := runCalendarReport(t, multiget(`<D:prop><D:getetag/></D:prop>`+href))
		ms := decodeMultistatus(t, rr)
		ms.assertHrefs(t, "/dav/calendars/1/standup.ics")
	})
}

// RFC 4791 §9.7 and §9.7.1–§9.7.5 give the filter elements content models of
// their own. A body that breaks one carries an invalid CALDAV:filter, which
// §7.8.6 answers with CALDAV:valid-filter; §1.3 fixes the status at 403,
// because resubmitting the same filter can never succeed.
func TestRFC4791_CalendarFilterContentModel(t *testing.T) {
	tests := map[string]string{
		"filter with no comp-filter":      calendarFilterBody(``),
		"filter with two comp-filters":    calendarFilterBody(grammarVEventFilter + grammarVEventFilter),
		"filter with a foreign DAV child": calendarFilterBody(`<D:href>/dav/</D:href>`),
		"filter with character data":      calendarFilterBody(`text` + grammarVEventFilter),
		"comp-filter with no name": calendarFilterBody(
			`<C:comp-filter><C:comp-filter name="VEVENT"/></C:comp-filter>`),
		"comp-filter with character data": calendarFilterBody(
			`<C:comp-filter name="VCALENDAR">text</C:comp-filter>`),
		// RFC 4791 defines no test attribute; filter@test is CardDAV's.
		"comp-filter with a test attribute": calendarFilterBody(
			`<C:comp-filter name="VCALENDAR" test="anyof"><C:comp-filter name="VEVENT"/></C:comp-filter>`),
		"comp-filter with is-not-defined beside a time-range": calendarFilterBody(
			`<C:comp-filter name="VCALENDAR"><C:comp-filter name="VEVENT">` +
				`<C:is-not-defined/><C:time-range start="20240101T000000Z" end="20240201T000000Z"/>` +
				`</C:comp-filter></C:comp-filter>`),
		"comp-filter with a time-range after a prop-filter": calendarFilterBody(
			`<C:comp-filter name="VCALENDAR"><C:comp-filter name="VEVENT">` +
				`<C:prop-filter name="SUMMARY"/><C:time-range start="20240101T000000Z" end="20240201T000000Z"/>` +
				`</C:comp-filter></C:comp-filter>`),
		"comp-filter with a prop-filter after a nested comp-filter": calendarFilterBody(
			`<C:comp-filter name="VCALENDAR"><C:comp-filter name="VEVENT">` +
				`<C:comp-filter name="VALARM"/><C:prop-filter name="SUMMARY"/>` +
				`</C:comp-filter></C:comp-filter>`),
		"prop-filter with no name": calendarFilterBody(
			`<C:comp-filter name="VCALENDAR"><C:comp-filter name="VEVENT">` +
				`<C:prop-filter><C:text-match>standup</C:text-match></C:prop-filter>` +
				`</C:comp-filter></C:comp-filter>`),
		"prop-filter with character data": calendarFilterBody(
			`<C:comp-filter name="VCALENDAR"><C:comp-filter name="VEVENT">` +
				`<C:prop-filter name="SUMMARY">text</C:prop-filter>` +
				`</C:comp-filter></C:comp-filter>`),
		"prop-filter with both a time-range and a text-match": calendarFilterBody(
			`<C:comp-filter name="VCALENDAR"><C:comp-filter name="VEVENT">` +
				`<C:prop-filter name="DTSTART">` +
				`<C:time-range start="20240101T000000Z" end="20240201T000000Z"/>` +
				`<C:text-match>2024</C:text-match></C:prop-filter>` +
				`</C:comp-filter></C:comp-filter>`),
		"prop-filter with a text-match after a param-filter": calendarFilterBody(
			`<C:comp-filter name="VCALENDAR"><C:comp-filter name="VEVENT">` +
				`<C:prop-filter name="ATTENDEE">` +
				`<C:param-filter name="PARTSTAT"/><C:text-match>mailto:</C:text-match>` +
				`</C:prop-filter></C:comp-filter></C:comp-filter>`),
		"prop-filter with is-not-defined beside a param-filter": calendarFilterBody(
			`<C:comp-filter name="VCALENDAR"><C:comp-filter name="VEVENT">` +
				`<C:prop-filter name="ATTENDEE"><C:is-not-defined/><C:param-filter name="PARTSTAT"/></C:prop-filter>` +
				`</C:comp-filter></C:comp-filter>`),
		"param-filter with no name": calendarFilterBody(
			`<C:comp-filter name="VCALENDAR"><C:comp-filter name="VEVENT">` +
				`<C:prop-filter name="ATTENDEE"><C:param-filter/></C:prop-filter>` +
				`</C:comp-filter></C:comp-filter>`),
		"param-filter with character data": calendarFilterBody(
			`<C:comp-filter name="VCALENDAR"><C:comp-filter name="VEVENT">` +
				`<C:prop-filter name="ATTENDEE"><C:param-filter name="PARTSTAT">text</C:param-filter></C:prop-filter>` +
				`</C:comp-filter></C:comp-filter>`),
		"param-filter with is-not-defined beside a text-match": calendarFilterBody(
			`<C:comp-filter name="VCALENDAR"><C:comp-filter name="VEVENT">` +
				`<C:prop-filter name="ATTENDEE"><C:param-filter name="PARTSTAT">` +
				`<C:is-not-defined/><C:text-match>ACCEPTED</C:text-match>` +
				`</C:param-filter></C:prop-filter></C:comp-filter></C:comp-filter>`),
		"param-filter with two text-matches": calendarFilterBody(
			`<C:comp-filter name="VCALENDAR"><C:comp-filter name="VEVENT">` +
				`<C:prop-filter name="ATTENDEE"><C:param-filter name="PARTSTAT">` +
				`<C:text-match>ACCEPTED</C:text-match><C:text-match>DECLINED</C:text-match>` +
				`</C:param-filter></C:prop-filter></C:comp-filter></C:comp-filter>`),
		// §9.7.5 gives text-match collation and negate-condition and nothing
		// else; match-type is CardDAV's (RFC 6352 §10.5.4).
		"text-match with a match-type attribute": calendarFilterBody(
			`<C:comp-filter name="VCALENDAR"><C:comp-filter name="VEVENT">` +
				`<C:prop-filter name="SUMMARY"><C:text-match match-type="equals">Standup</C:text-match></C:prop-filter>` +
				`</C:comp-filter></C:comp-filter>`),
		"text-match with a negate-condition outside its enumeration": calendarFilterBody(
			`<C:comp-filter name="VCALENDAR"><C:comp-filter name="VEVENT">` +
				`<C:prop-filter name="SUMMARY"><C:text-match negate-condition="maybe">Standup</C:text-match></C:prop-filter>` +
				`</C:comp-filter></C:comp-filter>`),
		"text-match with a padded negate-condition": calendarFilterBody(
			`<C:comp-filter name="VCALENDAR"><C:comp-filter name="VEVENT">` +
				`<C:prop-filter name="SUMMARY"><C:text-match negate-condition=" yes ">Standup</C:text-match></C:prop-filter>` +
				`</C:comp-filter></C:comp-filter>`),
		"is-not-defined carrying content": calendarFilterBody(
			`<C:comp-filter name="VCALENDAR"><C:comp-filter name="VEVENT">` +
				`<C:prop-filter name="SUMMARY"><C:is-not-defined><C:text-match>x</C:text-match></C:is-not-defined></C:prop-filter>` +
				`</C:comp-filter></C:comp-filter>`),
		"is-not-defined carrying an attribute": calendarFilterBody(
			`<C:comp-filter name="VCALENDAR"><C:comp-filter name="VEVENT">` +
				`<C:prop-filter name="SUMMARY"><C:is-not-defined test="yes"/></C:prop-filter>` +
				`</C:comp-filter></C:comp-filter>`),
		"is-not-defined carrying character data": calendarFilterBody(
			`<C:comp-filter name="VCALENDAR"><C:comp-filter name="VEVENT">` +
				`<C:prop-filter name="SUMMARY"><C:is-not-defined>text</C:is-not-defined></C:prop-filter>` +
				`</C:comp-filter></C:comp-filter>`),
		"text-match carrying a child": calendarFilterBody(
			`<C:comp-filter name="VCALENDAR"><C:comp-filter name="VEVENT">` +
				`<C:prop-filter name="SUMMARY"><C:text-match>Standup<C:is-not-defined/></C:text-match></C:prop-filter>` +
				`</C:comp-filter></C:comp-filter>`),
		"time-range carrying content": calendarFilterBody(
			`<C:comp-filter name="VCALENDAR"><C:comp-filter name="VEVENT">` +
				`<C:time-range start="20240101T000000Z" end="20240201T000000Z"><C:text-match>x</C:text-match></C:time-range>` +
				`</C:comp-filter></C:comp-filter>`),
		"time-range carrying character data": calendarFilterBody(
			`<C:comp-filter name="VCALENDAR"><C:comp-filter name="VEVENT">` +
				`<C:time-range start="20240101T000000Z" end="20240201T000000Z">text</C:time-range>` +
				`</C:comp-filter></C:comp-filter>`),
		"time-range with an undefined attribute": calendarFilterBody(
			`<C:comp-filter name="VCALENDAR"><C:comp-filter name="VEVENT">` +
				`<C:time-range start="20240101T000000Z" end="20240201T000000Z" limit="10"/>` +
				`</C:comp-filter></C:comp-filter>`),
	}

	for name, body := range tests {
		t.Run(name, func(t *testing.T) {
			assertErrorConditions(t, runCalendarReport(t, body), http.StatusForbidden, calQN("valid-filter"))
		})
	}
}

func TestRFC4791_CalendarQueryRejectsSemanticallyInvalidFilters(t *testing.T) {
	tests := map[string]string{
		"the root component is not VCALENDAR": calendarFilterBody(
			`<C:comp-filter name="VEVENT"/>`),
		"time-range is applied to SUMMARY": calendarFilterBody(
			`<C:comp-filter name="VCALENDAR"><C:comp-filter name="VEVENT">` +
				`<C:prop-filter name="SUMMARY"><C:time-range start="20240101T000000Z" end="20240201T000000Z"/>` +
				`</C:prop-filter></C:comp-filter></C:comp-filter>`),
		"a property time-range has invalid bounds": calendarFilterBody(
			`<C:comp-filter name="VCALENDAR"><C:comp-filter name="VEVENT">` +
				`<C:prop-filter name="DTSTART"><C:time-range start="not-a-date" end="20240201T000000Z"/>` +
				`</C:prop-filter></C:comp-filter></C:comp-filter>`),
	}

	for name, body := range tests {
		t.Run(name, func(t *testing.T) {
			assertErrorConditions(t, runCalendarReport(t, body), http.StatusForbidden, calQN("valid-filter"))
		})
	}
}

// RFC 4791 §9.7.1 constrains a comp-filter's content model, not which component
// names may nest inside which. A nesting no calendar object can satisfy is
// therefore a filter that matches nothing, not a filter the server refuses.
func TestRFC4791_CalendarQueryAcceptsUnsatisfiableComponentNesting(t *testing.T) {
	bodies := map[string]string{
		"VEVENT inside VTODO": calendarFilterBody(
			`<C:comp-filter name="VCALENDAR"><C:comp-filter name="VTODO">` +
				`<C:comp-filter name="VEVENT"/></C:comp-filter></C:comp-filter>`),
		"VEVENT inside VEVENT": calendarFilterBody(
			`<C:comp-filter name="VCALENDAR"><C:comp-filter name="VEVENT">` +
				`<C:comp-filter name="VEVENT"/></C:comp-filter></C:comp-filter>`),
		"VALARM directly inside VCALENDAR": calendarFilterBody(
			`<C:comp-filter name="VCALENDAR"><C:comp-filter name="VALARM"/></C:comp-filter>`),
	}

	for name, body := range bodies {
		t.Run(name, func(t *testing.T) {
			rr := runCalendarReport(t, body)
			if rr.Code != http.StatusMultiStatus {
				t.Fatalf("status = %d, want 207; body: %s", rr.Code, rr.Body.String())
			}
			decodeMultistatus(t, rr).assertHrefs(t)
		})
	}
}

// RFC 4791 §7.8.9 answers a filter naming something the server cannot filter on
// with CALDAV:supported-filter, and §7.8 asks that error to name the offending
// element. CalCard matches over the parsed component tree, so every well-formed
// iCalendar name is filterable and only a name that is no iCalendar name at all
// is unsupported.
func TestRFC4791_UnsupportedFilterNamesOffendingElement(t *testing.T) {
	tests := map[string]struct {
		body    string
		element xml.Name
		name    string
	}{
		"comp-filter": {
			body: calendarFilterBody(`<C:comp-filter name="VCALENDAR">` +
				`<C:comp-filter name="V EVENT"/></C:comp-filter>`),
			element: calQN("comp-filter"),
			name:    "V EVENT",
		},
		"prop-filter": {
			body: calendarFilterBody(`<C:comp-filter name="VCALENDAR"><C:comp-filter name="VEVENT">` +
				`<C:prop-filter name="SUM MARY"/></C:comp-filter></C:comp-filter>`),
			element: calQN("prop-filter"),
			name:    "SUM MARY",
		},
		"param-filter": {
			body: calendarFilterBody(`<C:comp-filter name="VCALENDAR"><C:comp-filter name="VEVENT">` +
				`<C:prop-filter name="ATTENDEE"><C:param-filter name="PART STAT"/></C:prop-filter>` +
				`</C:comp-filter></C:comp-filter>`),
			element: calQN("param-filter"),
			name:    "PART STAT",
		},
		"padded comp-filter": {
			body:    calendarFilterBody(`<C:comp-filter name=" VCALENDAR "/>`),
			element: calQN("comp-filter"),
			name:    " VCALENDAR ",
		},
		"padded prop-filter": {
			body: calendarFilterBody(`<C:comp-filter name="VCALENDAR"><C:comp-filter name="VEVENT">` +
				`<C:prop-filter name=" SUMMARY "/></C:comp-filter></C:comp-filter>`),
			element: calQN("prop-filter"),
			name:    " SUMMARY ",
		},
		"padded param-filter": {
			body: calendarFilterBody(`<C:comp-filter name="VCALENDAR"><C:comp-filter name="VEVENT">` +
				`<C:prop-filter name="ATTENDEE"><C:param-filter name=" PARTSTAT "/></C:prop-filter>` +
				`</C:comp-filter></C:comp-filter>`),
			element: calQN("param-filter"),
			name:    " PARTSTAT ",
		},
	}

	for name, tt := range tests {
		t.Run(name, func(t *testing.T) {
			rr := runCalendarReport(t, tt.body)
			if rr.Code != http.StatusForbidden {
				t.Fatalf("status = %d, want 403; body: %s", rr.Code, rr.Body.String())
			}
			root, err := parseRootElement(rr.Body.Bytes(), davQN("error"))
			if err != nil {
				t.Fatalf("decode DAV:error: %v; body: %s", err, rr.Body.String())
			}
			condition := assertSoleChild(t, root, calQN("supported-filter"))
			offending := assertSoleChild(t, condition, tt.element)
			got := ""
			for _, attr := range offending.Attr {
				if attr.Name.Local == "name" && attr.Name.Space == "" {
					got = attr.Value
				}
			}
			if got != tt.name {
				t.Errorf("offending %s name = %q, want %q", qnString(tt.element), got, tt.name)
			}
		})
	}
}

// RFC 4791 §9 content models are normative, while RFC 2518 Appendix 3 asks a
// server to tolerate what it does not understand. CalCard reconciles them by
// scope: an element in a namespace neither RFC 4791 nor RFC 4918 owns is an
// extension and is skipped, so it neither changes the result nor fails the
// request.
func TestRFC4791_CalendarQueryIgnoresForeignNamespaceExtensions(t *testing.T) {
	body := `<?xml version="1.0" encoding="utf-8"?>
<C:calendar-query xmlns:D="DAV:" xmlns:C="urn:ietf:params:xml:ns:caldav" xmlns:X="urn:example:vendor">
  <X:hint>ignore me</X:hint>
  <D:prop><D:getetag/></D:prop>
  <X:hint><X:nested/></X:hint>
  <C:filter>
    <X:hint/>
    <C:comp-filter name="VCALENDAR">
      <X:hint/>
      <C:comp-filter name="VEVENT"/>
    </C:comp-filter>
  </C:filter>
</C:calendar-query>`

	ms := decodeMultistatus(t, runCalendarReport(t, body))
	ms.assertHrefs(t, "/dav/calendars/1/standup.ics")
}

// RFC 4791 §9.5 and §9.10 admit DAV:allprop and DAV:propname as alternatives to
// DAV:prop, so a body carrying one is answered rather than treated as carrying
// no selector at all.
func TestRFC4791_CalendarQueryAcceptsEveryPropertySelector(t *testing.T) {
	for _, selector := range []string{`<D:allprop/>`, `<D:propname/>`, `<D:prop><D:getetag/></D:prop>`, ``} {
		t.Run(strings.TrimSpace(selector), func(t *testing.T) {
			body := calendarQueryBody(selector + `<C:filter>` + grammarVEventFilter + `</C:filter>`)
			ms := decodeMultistatus(t, runCalendarReport(t, body))
			ms.assertHrefs(t, "/dav/calendars/1/standup.ics")
		})
	}
}

func TestRFC4791_CalendarReportsWithoutPropertySelectorReturnOnlyResourceStatus(t *testing.T) {
	tests := map[string]string{
		"calendar-query": calendarQueryBody(`<C:filter>` + grammarVEventFilter + `</C:filter>`),
		"calendar-multiget": `<?xml version="1.0" encoding="utf-8"?>
<C:calendar-multiget xmlns:D="DAV:" xmlns:C="urn:ietf:params:xml:ns:caldav">
  <D:href>/dav/calendars/1/standup.ics</D:href>
</C:calendar-multiget>`,
	}

	for name, body := range tests {
		t.Run(name, func(t *testing.T) {
			response := decodeMultistatus(t, runCalendarReport(t, body)).
				responseForHref(t, "/dav/calendars/1/standup.ics")
			response.assertResponseStatus(t, http.StatusOK)
			if names := response.propNames(); len(names) != 0 {
				t.Fatalf("report without a property selector returned %s", qnList(names))
			}
		})
	}
}

// §9.5 sequences CALDAV:timezone last, after the filter. This test covers that
// placement; time-range evaluation tests cover how the value is used.
func TestRFC4791_CalendarQueryAcceptsTrailingTimezone(t *testing.T) {
	body := calendarQueryBody(`<D:prop><D:getetag/></D:prop><C:filter>` + grammarVEventFilter + `</C:filter>` +
		`<C:timezone>` + grammarVTimezone + `</C:timezone>`)
	ms := decodeMultistatus(t, runCalendarReport(t, body))
	ms.assertHrefs(t, "/dav/calendars/1/standup.ics")
}

// RFC 4791 §9.9 requires both time-range attributes to be an iCalendar "date
// with UTC time". A DATE, a floating DATE-TIME, or a numeric UTC offset names
// no unambiguous instant, so the filter is invalid rather than interpreted.
func TestRFC4791_TimeRangeRequiresDateWithUTCTime(t *testing.T) {
	refused := map[string][2]string{
		"DATE start":              {"20240601", "20240630T000000Z"},
		"DATE end":                {"20240601T000000Z", "20240630"},
		"floating start":          {"20240601T000000", "20240630T000000Z"},
		"floating end":            {"20240601T000000Z", "20240630T000000"},
		"numeric offset start":    {"20240601T000000-0500", "20240630T000000Z"},
		"extended-format start":   {"2024-06-01T00:00:00Z", "20240630T000000Z"},
		"lowercase zulu on start": {"20240601T000000z", "20240630T000000Z"},
	}
	for name, bounds := range refused {
		t.Run(name, func(t *testing.T) {
			body := calendarQueryBody(`<C:filter><C:comp-filter name="VCALENDAR"><C:comp-filter name="VEVENT">` +
				`<C:time-range start="` + bounds[0] + `" end="` + bounds[1] + `"/>` +
				`</C:comp-filter></C:comp-filter></C:filter>`)
			assertErrorConditions(t, runCalendarReport(t, body), http.StatusForbidden, calQN("valid-filter"))
		})
	}

	t.Run("a UTC pair is accepted", func(t *testing.T) {
		body := calendarQueryBody(`<D:prop><D:getetag/></D:prop><C:filter><C:comp-filter name="VCALENDAR">` +
			`<C:comp-filter name="VEVENT"><C:time-range start="20240601T000000Z" end="20240630T000000Z"/>` +
			`</C:comp-filter></C:comp-filter></C:filter>`)
		if rr := runCalendarReport(t, body); rr.Code != http.StatusMultiStatus {
			t.Fatalf("status = %d, want 207; body: %s", rr.Code, rr.Body.String())
		}
	})

	t.Run("an open range stays legal", func(t *testing.T) {
		body := calendarQueryBody(`<D:prop><D:getetag/></D:prop><C:filter><C:comp-filter name="VCALENDAR">` +
			`<C:comp-filter name="VEVENT"><C:time-range start="20240601T000000Z"/>` +
			`</C:comp-filter></C:comp-filter></C:filter>`)
		if rr := runCalendarReport(t, body); rr.Code != http.StatusMultiStatus {
			t.Fatalf("status = %d, want 207; body: %s", rr.Code, rr.Body.String())
		}
	})
}

// The zero time.Time is a legal instant a request can spell -- year 1 in UTC --
// so it cannot double as the marker for an omitted start. Reading it as omitted
// would skip the ordering rule and silently answer about an open range.
func TestTimeRangeBoundsDistinguishTheZeroInstantFromAnOmittedStart(t *testing.T) {
	if _, _, ok := calendarTimeRangeBounds(&timeRange{Start: "00010101T000000Z", End: "00010101T000000Z"}); ok {
		t.Error("an equal start and end at the zero instant was accepted")
	}
	if _, _, ok := calendarTimeRangeBounds(&timeRange{Start: "00010102T000000Z", End: "00010101T000000Z"}); ok {
		t.Error("an inverted pair at the zero instant was accepted")
	}
	start, end, ok := calendarTimeRangeBounds(&timeRange{Start: "00010101T000000Z", End: "00010102T000000Z"})
	if !ok {
		t.Fatal("a well-ordered pair at the zero instant was refused")
	}
	if !start.Equal(time.Time{}) || !end.After(start) {
		t.Errorf("bounds = %v..%v, want the zero instant and the day after it", start, end)
	}
}

// §9.9 requires at least one attribute, and requires end to be greater than
// start when both are present.
func TestRFC4791_TimeRangeBoundsMustBeOrdered(t *testing.T) {
	refused := map[string]string{
		"neither attribute":     ``,
		"end equal to start":    ` start="20240601T000000Z" end="20240601T000000Z"`,
		"end earlier than star": ` start="20240630T000000Z" end="20240601T000000Z"`,
	}
	for name, attrs := range refused {
		t.Run(name, func(t *testing.T) {
			body := calendarQueryBody(`<C:filter><C:comp-filter name="VCALENDAR"><C:comp-filter name="VEVENT">` +
				`<C:time-range` + attrs + `/></C:comp-filter></C:comp-filter></C:filter>`)
			assertErrorConditions(t, runCalendarReport(t, body), http.StatusForbidden, calQN("valid-filter"))
		})
	}
}

// RFC 4791 §7.8 and §7.9 bound the range a report asks about by the
// CALDAV:min-date-time and CALDAV:max-date-time of the targeted collection.
// The bound is inclusive at both ends, unlike the §5.3.2.1 preconditions that
// bound stored data.
func TestRFC4791_ReportTimeRangeHonoursCollectionDateLimits(t *testing.T) {
	tests := []struct {
		name      string
		start     string
		end       string
		condition string
	}{
		{name: "start before min-date-time", start: "18991231T235959Z", end: "20240630T000000Z", condition: "min-date-time"},
		{name: "end after max-date-time", start: "20240601T000000Z", end: "21010101T000000Z", condition: "max-date-time"},
		{name: "start exactly on min-date-time", start: ical.MinDateTime, end: "20240630T000000Z"},
		{name: "end exactly on max-date-time", start: "20240601T000000Z", end: ical.MaxDateTime},
		// An omitted attribute is an infinity the limits cannot contain, so it
		// must not be measured against them.
		{name: "an open-ended range is not measured", start: "20240601T000000Z"},
		{name: "an open-started range is not measured", end: "20240630T000000Z"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			attrs := ""
			if tt.start != "" {
				attrs += ` start="` + tt.start + `"`
			}
			if tt.end != "" {
				attrs += ` end="` + tt.end + `"`
			}
			body := calendarQueryBody(`<D:prop><D:getetag/></D:prop><C:filter><C:comp-filter name="VCALENDAR">` +
				`<C:comp-filter name="VEVENT"><C:time-range` + attrs + `/>` +
				`</C:comp-filter></C:comp-filter></C:filter>`)
			rr := runCalendarReport(t, body)
			if tt.condition == "" {
				if rr.Code != http.StatusMultiStatus {
					t.Fatalf("status = %d, want 207; body: %s", rr.Code, rr.Body.String())
				}
				return
			}
			assertErrorConditions(t, rr, http.StatusForbidden, calQN(tt.condition))
		})
	}
}

// RFC 4791 §7.8 makes CALDAV:valid-calendar-data a precondition on the timezone
// a REPORT carries: it has to be an iCalendar object holding a single valid
// VTIMEZONE, because §7.3 resolves every floating value in the request against
// it. §1.3 puts the failure at 403, since no resubmission of the same body can
// make an invalid definition work.
func TestRFC4791_CalendarQueryRejectsInvalidTimezone(t *testing.T) {
	tests := map[string]string{
		"not an iCalendar object": `nonsense`,
		"no VTIMEZONE":            `BEGIN:VCALENDAR&#13;&#10;VERSION:2.0&#13;&#10;END:VCALENDAR&#13;&#10;`,
		"two VTIMEZONEs":          grammarVTimezone + grammarVTimezone,
		"a VTIMEZONE with no observance": `BEGIN:VCALENDAR&#13;&#10;VERSION:2.0&#13;&#10;PRODID:-//test//EN&#13;&#10;` +
			`BEGIN:VTIMEZONE&#13;&#10;TZID:Etc/UTC&#13;&#10;END:VTIMEZONE&#13;&#10;END:VCALENDAR&#13;&#10;`,
	}

	for name, timezone := range tests {
		t.Run(name, func(t *testing.T) {
			body := calendarQueryBody(`<C:filter>` + grammarVEventFilter + `</C:filter><C:timezone>` + timezone + `</C:timezone>`)
			assertErrorConditions(t, runCalendarReport(t, body), http.StatusForbidden, calQN("valid-calendar-data"))
		})
	}
}

// RFC 4791 §7.8: a calendar-query MAY carry a Depth header and a request
// carrying none is processed as Depth: 0. A calendar collection is not itself a
// calendar object resource, so Depth: 0 on one reaches no member.
func TestRFC4791_CalendarQueryDepthScopesTheTargetSet(t *testing.T) {
	body := calendarQueryBody(`<D:prop><D:getetag/></D:prop><C:filter>` + grammarVEventFilter + `</C:filter>`)

	tests := []struct {
		depth string
		set   bool
		hrefs []string
	}{
		{depth: "", set: false, hrefs: nil},
		{depth: "0", set: true, hrefs: nil},
		{depth: "1", set: true, hrefs: []string{"/dav/calendars/1/standup.ics"}},
		{depth: "infinity", set: true, hrefs: []string{"/dav/calendars/1/standup.ics"}},
	}

	for _, tt := range tests {
		name := tt.depth
		if !tt.set {
			name = "absent"
		}
		t.Run(name, func(t *testing.T) {
			req := httptest.NewRequest("REPORT", "/dav/calendars/1/", strings.NewReader(body))
			if tt.set {
				req.Header.Set("Depth", tt.depth)
			}
			req = req.WithContext(auth.WithUser(req.Context(), &store.User{ID: 1}))
			rr := httptest.NewRecorder()
			grammarTestServer().Report(rr, req)

			decodeMultistatus(t, rr).assertHrefs(t, tt.hrefs...)
		})
	}

	t.Run("a Depth outside RFC 4918 §10.2 is malformed", func(t *testing.T) {
		req := httptest.NewRequest("REPORT", "/dav/calendars/1/", strings.NewReader(body))
		req.Header.Set("Depth", "2")
		req = req.WithContext(auth.WithUser(req.Context(), &store.User{ID: 1}))
		rr := httptest.NewRecorder()
		grammarTestServer().Report(rr, req)
		if rr.Code != http.StatusBadRequest {
			t.Fatalf("status = %d, want 400; body: %s", rr.Code, rr.Body.String())
		}
	})

	// §7 puts the report on calendar object resources too, where the resource
	// is the whole target set and Depth has no members to reach past.
	t.Run("an object-resource target is unaffected by Depth", func(t *testing.T) {
		for _, depth := range []string{"", "0", "1", "infinity"} {
			req := httptest.NewRequest("REPORT", "/dav/calendars/1/standup.ics", strings.NewReader(body))
			if depth != "" {
				req.Header.Set("Depth", depth)
			}
			req = req.WithContext(auth.WithUser(req.Context(), &store.User{ID: 1}))
			rr := httptest.NewRecorder()
			grammarTestServer().Report(rr, req)
			decodeMultistatus(t, rr).assertHrefs(t, "/dav/calendars/1/standup.ics")
		}
	})
}

// RFC 4791 §7.9: the Depth header is ignored for calendar-multiget, whose
// target set is exactly the hrefs the body names.
func TestRFC4791_CalendarMultigetIgnoresDepth(t *testing.T) {
	body := `<?xml version="1.0" encoding="utf-8"?>
<C:calendar-multiget xmlns:D="DAV:" xmlns:C="urn:ietf:params:xml:ns:caldav">
  <D:prop><D:getetag/></D:prop>
  <D:href>/dav/calendars/1/standup.ics</D:href>
</C:calendar-multiget>`

	for _, depth := range []string{"", "0", "1", "infinity", "2", "nonsense"} {
		name := depth
		if name == "" {
			name = "absent"
		}
		t.Run(name, func(t *testing.T) {
			req := httptest.NewRequest("REPORT", "/dav/calendars/1/", strings.NewReader(body))
			if depth != "" {
				req.Header.Set("Depth", depth)
			}
			req = req.WithContext(auth.WithUser(req.Context(), &store.User{ID: 1}))
			rr := httptest.NewRecorder()
			grammarTestServer().Report(rr, req)

			decodeMultistatus(t, rr).assertHrefs(t, "/dav/calendars/1/standup.ics")
		})
	}
}

// RFC 4791 §7.9: on a collection target the hrefs refer to calendar object
// resources within that collection. CalCard's calendar collections hold their
// objects in one flat level, so "at any depth" is that one level; an href
// naming another collection's member is out of scope and reports 404 in its own
// DAV:response rather than being served or dropped.
func TestRFC4791_CalendarMultigetHrefsAreScopedToTheCollection(t *testing.T) {
	body := `<?xml version="1.0" encoding="utf-8"?>
<C:calendar-multiget xmlns:D="DAV:" xmlns:C="urn:ietf:params:xml:ns:caldav">
  <D:prop><D:getetag/></D:prop>
  <D:href>/dav/calendars/1/standup.ics</D:href>
  <D:href>standup.ics</D:href>
  <D:href>/dav/calendars/2/standup.ics</D:href>
  <D:href>/dav/addressbooks/1/standup.vcf</D:href>
  <D:href>/dav/calendars/1/missing.ics</D:href>
</C:calendar-multiget>`

	rr := runCalendarReport(t, body)
	ms := decodeMultistatus(t, rr)
	ms.assertHrefs(t,
		"/dav/calendars/1/standup.ics",
		"/dav/calendars/1/standup.ics",
		"/dav/calendars/2/standup.ics",
		"/dav/addressbooks/1/standup.vcf",
		"/dav/calendars/1/missing.ics",
	)
	ms.responseForHref(t, "/dav/calendars/2/standup.ics").assertResponseStatus(t, http.StatusNotFound)
	ms.responseForHref(t, "/dav/addressbooks/1/standup.vcf").assertResponseStatus(t, http.StatusNotFound)
	ms.responseForHref(t, "/dav/calendars/1/missing.ics").assertResponseStatus(t, http.StatusNotFound)
}

// RFC 3986 §5.2.2 discards the last segment of a base URI that does not end in
// "/", so a relative DAV:href resolves against the collection whatever spelling
// of the collection's URI the Request-URI carries.
func TestRFC4791_CalendarMultigetResolvesRelativeHrefsAgainstTheCollection(t *testing.T) {
	body := `<?xml version="1.0" encoding="utf-8"?>
<C:calendar-multiget xmlns:D="DAV:" xmlns:C="urn:ietf:params:xml:ns:caldav">
  <D:prop><D:getetag/></D:prop>
  <D:href>standup.ics</D:href>
</C:calendar-multiget>`

	for _, requestURI := range []string{
		"/dav/calendars/1/",
		"/dav/calendars/1",
		"/dav/calendars/1/standup.ics",
	} {
		t.Run(requestURI, func(t *testing.T) {
			req := httptest.NewRequest("REPORT", requestURI, strings.NewReader(body))
			req = req.WithContext(auth.WithUser(req.Context(), &store.User{ID: 1}))
			rr := httptest.NewRecorder()
			grammarTestServer().Report(rr, req)

			decodeMultistatus(t, rr).
				responseForHref(t, "/dav/calendars/1/standup.ics").
				assertPropStatus(t, davQN("getetag"), http.StatusOK)
		})
	}
}

// RFC 4918 §8.3 makes DAV:href a URI, so a resource name holding a character no
// path segment may carry literally is percent-encoded. A client matches the
// hrefs one report returns against another's, so every report has to spell the
// same resource the same way.
func TestRFC4791_ReportsAgreeOnThePercentEncodedHref(t *testing.T) {
	const resourceName = "team standup"
	const wantHref = "/dav/calendars/1/team%20standup.ics"

	server := func() *DavServer {
		return &DavServer{store: &store.Store{
			Calendars: &fakeCalendarRepo{accessible: []store.CalendarAccess{
				{Calendar: store.Calendar{ID: 1, UserID: 1, Name: "Test"}, Editor: true},
			}},
			Events: &fakeEventRepo{events: map[string]*store.Event{
				"1:" + resourceName: {CalendarID: 1, UID: resourceName, ResourceName: resourceName, ETag: "etag",
					RawICAL: "BEGIN:VCALENDAR\r\nVERSION:2.0\r\nBEGIN:VEVENT\r\nUID:standup\r\nSUMMARY:Standup\r\nEND:VEVENT\r\nEND:VCALENDAR\r\n"},
			}},
		}}
	}

	bodies := map[string]string{
		"calendar-query": calendarQueryBody(
			`<D:prop><D:getetag/></D:prop><C:filter>` + grammarVEventFilter + `</C:filter>`),
		"calendar-multiget": `<?xml version="1.0" encoding="utf-8"?>
<C:calendar-multiget xmlns:D="DAV:" xmlns:C="urn:ietf:params:xml:ns:caldav">
  <D:prop><D:getetag/></D:prop>
  <D:href>` + wantHref + `</D:href>
</C:calendar-multiget>`,
	}

	for name, body := range bodies {
		t.Run(name, func(t *testing.T) {
			req := httptest.NewRequest("REPORT", "/dav/calendars/1/", strings.NewReader(body))
			req.Header.Set("Depth", "1")
			req = req.WithContext(auth.WithUser(req.Context(), &store.User{ID: 1}))
			rr := httptest.NewRecorder()
			server().Report(rr, req)

			decodeMultistatus(t, rr).assertHrefs(t, wantHref)
		})
	}
}

func TestRFC4791_CalendarMultigetUsesRequestURIEquivalenceForCollectionHrefs(t *testing.T) {
	bodyForHref := func(href string) string {
		return `<?xml version="1.0" encoding="utf-8"?>
<C:calendar-multiget xmlns:D="DAV:" xmlns:C="urn:ietf:params:xml:ns:caldav">
  <D:prop><D:getetag/></D:prop>
  <D:href>` + href + `</D:href>
</C:calendar-multiget>`
	}

	tests := map[string]struct {
		href   string
		status int
	}{
		"same absolute URI": {href: "http://example.com/dav/calendars/1/standup.ics", status: http.StatusOK},
		"foreign authority": {href: "https://other.example/dav/calendars/1/standup.ics", status: http.StatusNotFound},
		"different scheme":  {href: "https://example.com/dav/calendars/1/standup.ics", status: http.StatusNotFound},
		"query component":   {href: "http://example.com/dav/calendars/1/standup.ics?view=full", status: http.StatusNotFound},
		"fragment":          {href: "/dav/calendars/1/standup.ics#vevent", status: http.StatusNotFound},
		// A URI carries no surrounding whitespace, so padding is not part of the
		// reference and the href still names the resource.
		"padding":              {href: " /dav/calendars/1/standup.ics ", status: http.StatusOK},
		"tab and CRLF padding": {href: "\t\r\n/dav/calendars/1/standup.ics\r\n\t", status: http.StatusOK},
		"whitespace only":      {href: "   ", status: http.StatusNotFound},
		// Only the whitespace XML 1.0 §2.3 defines is framing. A URI is
		// US-ASCII, so a Unicode space is part of what the client sent, and
		// trimming it would answer with a resource the href does not name.
		//
		// Each of these pads the front, because a resource name is the last path
		// segment minus its extension and path.Ext takes everything after the
		// last dot -- so trailing padding is swallowed along with the ".ics",
		// exactly as "standup.icsZZZ" is, and cannot tell the two trim rules
		// apart.
		"leading no-break space":    {href: " /dav/calendars/1/standup.ics", status: http.StatusNotFound},
		"leading ideographic space": {href: "　/dav/calendars/1/standup.ics", status: http.StatusNotFound},
		"leading line separator":    {href: " /dav/calendars/1/standup.ics", status: http.StatusNotFound},
	}

	for name, tt := range tests {
		t.Run(name, func(t *testing.T) {
			req := httptest.NewRequest("REPORT", "http://example.com/dav/calendars/1/", strings.NewReader(bodyForHref(tt.href)))
			req = req.WithContext(auth.WithUser(req.Context(), &store.User{ID: 1}))
			rr := httptest.NewRecorder()
			grammarTestServer().Report(rr, req)

			ms := decodeMultistatus(t, rr)
			if len(ms.Responses) != 1 {
				t.Fatalf("responses = %d, want 1", len(ms.Responses))
			}
			if tt.status == http.StatusOK {
				ms.Responses[0].assertPropstatNames(t, http.StatusOK, davQN("getetag"))
			} else {
				ms.Responses[0].assertResponseStatus(t, tt.status)
			}
		})
	}
}

// An absolute DAV:href has to match the Request-URI scheme to name the same
// resource, and behind a TLS-terminating proxy the request itself carries no
// trace of the https leg: net/http leaves URL.Scheme empty and r.TLS nil. The
// scheme therefore comes from X-Forwarded-Proto on the same trust terms
// internal/auth applies to it, or from the configured base URL when the proxy
// forwards no such header.
func TestRFC4791_CalendarMultigetResolvesAbsoluteHrefsBehindATLSProxy(t *testing.T) {
	const body = `<?xml version="1.0" encoding="utf-8"?>
<C:calendar-multiget xmlns:D="DAV:" xmlns:C="urn:ietf:params:xml:ns:caldav">
  <D:prop><D:getetag/></D:prop>
  <D:href>https://cal.example.com/dav/calendars/1/standup.ics</D:href>
</C:calendar-multiget>`

	tests := map[string]struct {
		baseURL        string
		trustedProxies []string
		remoteAddr     string
		forwardedProto string
		status         int
	}{
		"trusted proxy forwards https": {
			baseURL: "http://localhost:8080", trustedProxies: []string{"192.0.2.0/24"},
			remoteAddr: "192.0.2.7:41234", forwardedProto: "https", status: http.StatusOK,
		},
		"untrusted peer cannot assert https": {
			baseURL: "http://localhost:8080", trustedProxies: []string{"192.0.2.0/24"},
			remoteAddr: "198.51.100.9:41234", forwardedProto: "https", status: http.StatusNotFound,
		},
		"no header, https base URL": {
			baseURL: "https://cal.example.com", status: http.StatusOK,
		},
		"no header, http base URL": {
			baseURL: "http://cal.example.com", status: http.StatusNotFound,
		},
	}

	for name, tt := range tests {
		t.Run(name, func(t *testing.T) {
			server := grammarTestServer()
			server.cfg = &config.Config{BaseURL: tt.baseURL, TrustedProxies: tt.trustedProxies}

			// A path-only target leaves URL.Scheme empty, as net/http does for
			// every origin-form request a real client sends.
			req := httptest.NewRequest("REPORT", "/dav/calendars/1/", strings.NewReader(body))
			req.Host = "cal.example.com"
			if tt.remoteAddr != "" {
				req.RemoteAddr = tt.remoteAddr
			}
			if tt.forwardedProto != "" {
				req.Header.Set("X-Forwarded-Proto", tt.forwardedProto)
			}
			req = req.WithContext(auth.WithUser(req.Context(), &store.User{ID: 1}))
			rr := httptest.NewRecorder()
			server.Report(rr, req)

			ms := decodeMultistatus(t, rr)
			if len(ms.Responses) != 1 {
				t.Fatalf("responses = %d, want 1", len(ms.Responses))
			}
			if tt.status == http.StatusOK {
				ms.Responses[0].assertPropstatNames(t, http.StatusOK, davQN("getetag"))
				return
			}
			ms.Responses[0].assertResponseStatus(t, tt.status)
		})
	}
}

// RFC 4791 §7.8 and §7.9 make a CALDAV:calendar-data naming an unsupported
// content-type or version a CALDAV:supported-calendar-data failure, answered at
// 403 per §1.3. §9.6 defaults both attributes, so an absent pair names
// text/calendar 2.0 and is served.
func TestRFC4791_ReportCalendarDataMediaType(t *testing.T) {
	query := func(attrs string) string {
		return calendarQueryBody(`<D:prop><D:getetag/><C:calendar-data ` + attrs + `/></D:prop>` +
			`<C:filter>` + grammarVEventFilter + `</C:filter>`)
	}
	multiget := func(attrs string) string {
		return `<?xml version="1.0" encoding="utf-8"?>
<C:calendar-multiget xmlns:D="DAV:" xmlns:C="urn:ietf:params:xml:ns:caldav">
  <D:prop><D:getetag/><C:calendar-data ` + attrs + `/></D:prop>
  <D:href>/dav/calendars/1/standup.ics</D:href>
</C:calendar-multiget>`
	}

	refused := []string{
		`content-type=""`,
		`version=""`,
		`content-type=" text/calendar"`,
		`content-type="text/calendar "`,
		`version=" 2.0"`,
		`version="2.0 "`,
		`content-type="text/vcard"`,
		`content-type="application/calendar+json"`,
		`version="1.0"`,
		`content-type="text/calendar" version="3.0"`,
	}
	for _, attrs := range refused {
		for report, build := range map[string]func(string) string{"calendar-query": query, "calendar-multiget": multiget} {
			t.Run(report+" "+attrs, func(t *testing.T) {
				assertErrorConditions(t, runCalendarReport(t, build(attrs)),
					http.StatusForbidden, calQN("supported-calendar-data"))
			})
		}
	}

	served := []string{``, `content-type="text/calendar"`, `version="2.0"`, `content-type="text/calendar" version="2.0"`}
	for _, attrs := range served {
		for report, build := range map[string]func(string) string{"calendar-query": query, "calendar-multiget": multiget} {
			t.Run("served "+report+" "+attrs, func(t *testing.T) {
				ms := decodeMultistatus(t, runCalendarReport(t, build(attrs)))
				ms.assertHrefs(t, "/dav/calendars/1/standup.ics")
			})
		}
	}
}

// RFC 4791 §7.8: a property the request names that the resource does not define
// is reported with a 404 status inside the response for that resource, rather
// than omitted from it.
func TestRFC4791_ReportsUndefinedPropertiesWith404(t *testing.T) {
	bodies := map[string]string{
		"calendar-query": calendarQueryBody(
			`<D:prop><D:getetag/><D:displayname/><X:mystery xmlns:X="urn:example:unknown"/></D:prop>` +
				`<C:filter>` + grammarVEventFilter + `</C:filter>`),
		"calendar-multiget": `<?xml version="1.0" encoding="utf-8"?>
<C:calendar-multiget xmlns:D="DAV:" xmlns:C="urn:ietf:params:xml:ns:caldav" xmlns:X="urn:example:unknown">
  <D:prop><D:getetag/><D:displayname/><X:mystery/></D:prop>
  <D:href>/dav/calendars/1/standup.ics</D:href>
</C:calendar-multiget>`,
	}

	for name, body := range bodies {
		t.Run(name, func(t *testing.T) {
			ms := decodeMultistatus(t, runCalendarReport(t, body))
			resp := ms.responseForHref(t, "/dav/calendars/1/standup.ics")
			resp.assertPropStatus(t, davQN("getetag"), http.StatusOK)
			resp.assertPropStatus(t, davQN("displayname"), http.StatusNotFound)
			resp.assertPropStatus(t, qn("urn:example:unknown", "mystery"), http.StatusNotFound)
		})
	}
}

// RFC 4791 §9.5 and §9.10 admit DAV:allprop and DAV:propname as alternatives to
// DAV:prop, so each selects a different response body rather than being parsed
// away and answered as though the body carried no selector at all.
func TestRFC4791_ReportPropertySelectorsSelectDifferentBodies(t *testing.T) {
	query := func(selector string) string {
		return calendarQueryBody(selector + `<C:filter>` + grammarVEventFilter + `</C:filter>`)
	}
	href := "/dav/calendars/1/standup.ics"

	t.Run("DAV:prop reports exactly what it names", func(t *testing.T) {
		ms := decodeMultistatus(t, runCalendarReport(t, query(`<D:prop><D:getetag/></D:prop>`)))
		ms.responseForHref(t, href).assertPropstatNames(t, http.StatusOK, davQN("getetag"))
	})

	t.Run("DAV:propname reports names without values", func(t *testing.T) {
		ms := decodeMultistatus(t, runCalendarReport(t, query(`<D:propname/>`)))
		resp := ms.responseForHref(t, href)
		names := resp.propNames()
		if len(names) < 2 {
			t.Fatalf("DAV:propname reported %s, want the resource's property names", qnList(names))
		}
		for _, name := range names {
			if el := resp.assertPropStatus(t, name, http.StatusOK); len(el.Children) != 0 || strings.TrimSpace(el.Text) != "" {
				t.Errorf("DAV:propname returned a value for %s", qnString(name))
			}
		}
		// CALDAV:calendar-data is a REPORT selector rather than a property, so
		// propname does not name it (RFC 4791 §9.6).
		resp.assertPropAbsent(t, calQN("calendar-data"))
	})

	t.Run("DAV:allprop reports values and excludes the CalDAV properties", func(t *testing.T) {
		ms := decodeMultistatus(t, runCalendarReport(t, query(`<D:allprop/>`)))
		resp := ms.responseForHref(t, href)
		resp.assertPropValue(t, davQN("getetag"), http.StatusOK, `"etag"`)
		// §5.2.x and §9.6: allprop carries neither the CalDAV limit properties
		// nor the calendar-data selector.
		resp.assertPropAbsent(t, calQN("calendar-data"))
		resp.assertPropAbsent(t, calQN("supported-collation-set"))
	})
}

// RFC 4791 §11 asks a server to take adequate precautions against a report that
// would consume excessive CPU or memory. Both recursive REPORT grammars --
// CALDAV:comp-filter and the CALDAV:comp of CALDAV:calendar-data -- descend
// once per nested element, so a body nested hundreds of thousands deep buys
// seconds of CPU and a stack to match for the price of one request under the
// body-size cap.
func TestReportBodyNestedPastTheDepthLimitIsRefused(t *testing.T) {
	nest := func(open, close string, depth int) string {
		var b strings.Builder
		for i := 0; i < depth; i++ {
			b.WriteString(open)
		}
		for i := 0; i < depth; i++ {
			b.WriteString(close)
		}
		return b.String()
	}

	t.Run("CALDAV:calendar-data comp", func(t *testing.T) {
		body := calendarQueryBody(`<D:prop><C:calendar-data>` +
			nest(`<C:comp name="VEVENT">`, `</C:comp>`, 5000) +
			`</C:calendar-data></D:prop><C:filter>` + grammarVEventFilter + `</C:filter>`)

		rr := runCalendarReport(t, body)

		if rr.Code != http.StatusBadRequest {
			t.Fatalf("status = %d, want 400; body: %s", rr.Code, rr.Body.String())
		}
	})

	t.Run("CALDAV:comp-filter", func(t *testing.T) {
		body := calendarFilterBody(`<C:comp-filter name="VCALENDAR">` +
			nest(`<C:comp-filter name="VEVENT">`, `</C:comp-filter>`, 5000) +
			`</C:comp-filter>`)

		rr := runCalendarReport(t, body)

		// Inside CALDAV:filter the refusal is the filter's own precondition
		// rather than a bare malformed-body status, on the same terms as any
		// other filter that is not valid.
		assertErrorConditions(t, rr, http.StatusForbidden, calQN("valid-filter"))
	})
}

// The nesting RFC 4791 actually defines is shallow -- VCALENDAR, VEVENT, VALARM
// is the deepest component path a filter can name -- so the limit refuses only
// bodies far past any filter with a meaning.
func TestFilterWithinTheDepthLimitIsEvaluated(t *testing.T) {
	body := calendarFilterBody(`<C:comp-filter name="VCALENDAR">` +
		`<C:comp-filter name="VEVENT"><C:comp-filter name="VALARM"/></C:comp-filter>` +
		`</C:comp-filter>`)

	rr := runCalendarReport(t, body)

	if rr.Code != http.StatusMultiStatus {
		t.Fatalf("status = %d, want 207; body: %s", rr.Code, rr.Body.String())
	}
}

// Every filter element is evaluated against every candidate resource, so the
// element count is a multiplier on the whole collection scan and is bounded in
// its own right rather than only through the body-size cap.
func TestFilterOverTheElementLimitFailsValidFilter(t *testing.T) {
	var props strings.Builder
	for i := 0; i < 500; i++ {
		fmt.Fprintf(&props, `<C:prop-filter name="SUMMARY-%d"/>`, i)
	}
	body := calendarFilterBody(`<C:comp-filter name="VCALENDAR"><C:comp-filter name="VEVENT">` +
		props.String() + `</C:comp-filter></C:comp-filter>`)

	rr := runCalendarReport(t, body)

	assertErrorConditions(t, rr, http.StatusForbidden, calQN("valid-filter"))
}

func TestFilterAtTheElementLimitIsEvaluated(t *testing.T) {
	// The filter, its two comp-filters and the prop-filters below it are all
	// counted, so the body stops just short of the default limit.
	var props strings.Builder
	for i := 0; i < defaultMaxFilterElements-3; i++ {
		fmt.Fprintf(&props, `<C:prop-filter name="SUMMARY-%d"/>`, i)
	}
	body := calendarFilterBody(`<C:comp-filter name="VCALENDAR"><C:comp-filter name="VEVENT">` +
		props.String() + `</C:comp-filter></C:comp-filter>`)

	rr := runCalendarReport(t, body)

	if rr.Code != http.StatusMultiStatus {
		t.Fatalf("status = %d, want 207; body: %s", rr.Code, rr.Body.String())
	}
}

// A limit set to 0 turns that limit off, which is an operator's decision to
// make. It must not become a way to kill the process: unbounded recursion grows
// the goroutine stack until the runtime gives up, so the decoders hold
// themselves to a ceiling of their own whatever the configuration says.
func TestReportBodyNestedPastTheDecoderCeilingIsRefusedWithLimitsDisabled(t *testing.T) {
	cfg := &config.Config{}
	cfg.DAV.MaxReportElementDepth = math.MaxInt
	cfg.DAV.MaxFilterElements = math.MaxInt
	h := NewDavServer(Options{Config: cfg, Store: grammarTestServer().store})

	nest := func(open, close string, depth int) string {
		var b strings.Builder
		for i := 0; i < depth; i++ {
			b.WriteString(open)
		}
		for i := 0; i < depth; i++ {
			b.WriteString(close)
		}
		return b.String()
	}
	run := func(t *testing.T, body string) *httptest.ResponseRecorder {
		t.Helper()
		req := httptest.NewRequest("REPORT", "/dav/calendars/1/", strings.NewReader(body))
		req.Header.Set("Depth", "1")
		req = req.WithContext(auth.WithUser(req.Context(), &store.User{ID: 1}))
		rr := httptest.NewRecorder()
		h.Report(rr, req)
		return rr
	}

	t.Run("CALDAV:comp-filter", func(t *testing.T) {
		rr := run(t, calendarFilterBody(`<C:comp-filter name="VCALENDAR">`+
			nest(`<C:comp-filter name="VEVENT">`, `</C:comp-filter>`, maxRecursiveGrammarDepth+50)+
			`</C:comp-filter>`))
		assertErrorConditions(t, rr, http.StatusForbidden, calQN("valid-filter"))
	})

	t.Run("CALDAV:calendar-data comp", func(t *testing.T) {
		rr := run(t, calendarQueryBody(`<D:prop><C:calendar-data>`+
			nest(`<C:comp name="VEVENT">`, `</C:comp>`, maxRecursiveGrammarDepth+50)+
			`</C:calendar-data></D:prop><C:filter>`+grammarVEventFilter+`</C:filter>`))
		if rr.Code != http.StatusBadRequest {
			t.Fatalf("status = %d, want 400; body: %s", rr.Code, rr.Body.String())
		}
	})

	// The ceiling is far above anything RFC 4791 defines, so a real filter is
	// still evaluated with the configured budget switched off.
	t.Run("a filter within the ceiling", func(t *testing.T) {
		rr := run(t, calendarFilterBody(`<C:comp-filter name="VCALENDAR">`+
			`<C:comp-filter name="VEVENT"><C:comp-filter name="VALARM"/></C:comp-filter>`+
			`</C:comp-filter>`))
		if rr.Code != http.StatusMultiStatus {
			t.Fatalf("status = %d, want 207; body: %s", rr.Code, rr.Body.String())
		}
	})
}
