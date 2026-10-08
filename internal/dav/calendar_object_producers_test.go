package dav

import (
	"errors"
	"net/http"
	"net/http/httptest"
	"reflect"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/jw6ventures/calcard/internal/auth"
	"github.com/jw6ventures/calcard/internal/calobject"
	"github.com/jw6ventures/calcard/internal/store"
)

const (
	berlinEvent = "DTSTART;TZID=Europe/Berlin:20260105T090000\r\nDTEND;TZID=Europe/Berlin:20260105T100000\r\n"
	utf8BOM     = "\xef\xbb\xbf"
)

// googleExport is a single event in the shape Google Calendar's export writes,
// with the METHOD line an export file carries left out.
var googleExport = "BEGIN:VCALENDAR\r\nPRODID:-//Google Inc//Google Calendar 70.9054//EN\r\nVERSION:2.0\r\nCALSCALE:GREGORIAN\r\n" +
	"X-WR-CALNAME:Work\r\nX-WR-TIMEZONE:America/New_York\r\n" +
	"BEGIN:VTIMEZONE\r\nTZID:America/New_York\r\nX-LIC-LOCATION:America/New_York\r\n" +
	"BEGIN:DAYLIGHT\r\nTZOFFSETFROM:-0500\r\nTZOFFSETTO:-0400\r\nTZNAME:EDT\r\nDTSTART:19700308T020000\r\nRRULE:FREQ=YEARLY;BYMONTH=3;BYDAY=2SU\r\nEND:DAYLIGHT\r\n" +
	"BEGIN:STANDARD\r\nTZOFFSETFROM:-0400\r\nTZOFFSETTO:-0500\r\nTZNAME:EST\r\nDTSTART:19701101T020000\r\nRRULE:FREQ=YEARLY;BYMONTH=11;BYDAY=1SU\r\nEND:STANDARD\r\n" +
	"END:VTIMEZONE\r\n" +
	"BEGIN:VEVENT\r\nDTSTART;TZID=America/New_York:20260105T090000\r\nDTEND;TZID=America/New_York:20260105T100000\r\n" +
	"RRULE:FREQ=WEEKLY;WKST=SU;UNTIL=20260302T135959Z;BYDAY=MO\r\nDTSTAMP:20261002T120000Z\r\nUID:7kukuqrfedlm2f9t0vr0p5b1sp@google.com\r\n" +
	"ATTENDEE;CUTYPE=INDIVIDUAL;ROLE=REQ-PARTICIPANT;PARTSTAT=ACCEPTED;CN=Jane Doe;X-NUM-GUESTS=0:mailto:jane@example.test\r\n" +
	"CREATED:20251201T150000Z\r\nDESCRIPTION:\r\nLAST-MODIFIED:20251201T150000Z\r\nLOCATION:\r\nSEQUENCE:0\r\nSTATUS:CONFIRMED\r\nSUMMARY:Standup\r\nTRANSP:OPAQUE\r\n" +
	"BEGIN:VALARM\r\nACTION:DISPLAY\r\nDESCRIPTION:This is an event reminder\r\nTRIGGER:-P0DT0H10M0S\r\nEND:VALARM\r\n" +
	"BEGIN:VALARM\r\nACTION:EMAIL\r\nDESCRIPTION:This is an event reminder\r\nSUMMARY:Alarm notification\r\nATTENDEE:mailto:jane@example.test\r\nTRIGGER:-P0DT0H30M0S\r\nEND:VALARM\r\n" +
	"END:VEVENT\r\nEND:VCALENDAR\r\n"

// outlookExport is a single event in the shape Exchange and Outlook write, with
// a Windows zone name and the METHOD line an export file carries left out.
var outlookExport = "BEGIN:VCALENDAR\r\nPRODID:Microsoft Exchange Server 2010\r\nVERSION:2.0\r\nX-WR-CALNAME:Calendar\r\n" +
	"BEGIN:VTIMEZONE\r\nTZID:Eastern Standard Time\r\n" +
	"BEGIN:STANDARD\r\nDTSTART:16010101T020000\r\nTZOFFSETFROM:-0400\r\nTZOFFSETTO:-0500\r\nRRULE:FREQ=YEARLY;INTERVAL=1;BYDAY=1SU;BYMONTH=11\r\nEND:STANDARD\r\n" +
	"BEGIN:DAYLIGHT\r\nDTSTART:16010101T020000\r\nTZOFFSETFROM:-0500\r\nTZOFFSETTO:-0400\r\nRRULE:FREQ=YEARLY;INTERVAL=1;BYDAY=2SU;BYMONTH=3\r\nEND:DAYLIGHT\r\n" +
	"END:VTIMEZONE\r\n" +
	"BEGIN:VEVENT\r\nDESCRIPTION:\\n\r\nRRULE:FREQ=WEEKLY;UNTIL=20260330T130000Z;INTERVAL=1;BYDAY=MO;WKST=SU\r\n" +
	"UID:040000008200E00074C5B7101A82E00800000000D0F2A3C1E5A5DA01000000000000000010000000\r\nSUMMARY:Standup\r\n" +
	"DTSTART;TZID=Eastern Standard Time:20260105T090000\r\nDTEND;TZID=Eastern Standard Time:20260105T093000\r\n" +
	"CLASS:PUBLIC\r\nPRIORITY:5\r\nDTSTAMP:20261002T120000Z\r\nTRANSP:OPAQUE\r\nSTATUS:CONFIRMED\r\nSEQUENCE:0\r\nLOCATION:Microsoft Teams Meeting\r\n" +
	"X-MICROSOFT-CDO-BUSYSTATUS:BUSY\r\nX-MICROSOFT-CDO-ALLDAYEVENT:FALSE\r\n" +
	"X-ALT-DESC;FMTTYPE=text/html:<html><body><p>Join: https://teams.example.test/x</p></body></html>\r\n" +
	"BEGIN:VALARM\r\nDESCRIPTION:REMINDER\r\nTRIGGER;RELATED=START:-PT15M\r\nACTION:DISPLAY\r\nEND:VALARM\r\n" +
	"END:VEVENT\r\nEND:VCALENDAR\r\n"

// producerObject wraps one component's lines in a minimal VCALENDAR.
func producerObject(component string, lines ...string) string {
	return "BEGIN:VCALENDAR\r\nVERSION:2.0\r\nPRODID:-//Producer//EN\r\nBEGIN:" + component + "\r\n" +
		strings.Join(lines, "\r\n") + "\r\nEND:" + component + "\r\nEND:VCALENDAR\r\n"
}

// What calendar applications and export files write is stored whenever the
// server can still answer every query about it truthfully, even where the
// object departs from RFC 5545: refusing it loses the user's event, while
// storing it costs nothing a report would get wrong.
func TestCalendarObjectValidationAcceptsWhatProducersWrite(t *testing.T) {
	tests := map[string]string{
		"Google Calendar export event": googleExport,
		"Outlook export event":         outlookExport,
		"Outlook control characters":   outlookControlCharacters,
		"zero-length event":            producerObject("VEVENT", "UID:z", "DTSTAMP:20261002T120000Z", "DTSTART:20260105T090000Z", "DTEND:20260105T090000Z"),
		"zero DURATION":                producerObject("VEVENT", "UID:z", "DTSTAMP:20261002T120000Z", "DTSTART:20260105T090000Z", "DURATION:PT0S"),
		"zero-length zoned event":      producerObject("VEVENT", "UID:z", "DTSTART;TZID=Europe/Berlin:20260105T090000", "DTEND;TZID=Europe/Berlin:20260105T090000"),
		"task starting on its due day": producerObject("VTODO", "UID:t", "DTSTAMP:20261002T120000Z", "DTSTART;VALUE=DATE:20260105", "DUE;VALUE=DATE:20260105"),
		"task carrying DTEND":          producerObject("VTODO", "UID:t", "DTSTAMP:20261002T120000Z", "DTSTART:20260105T090000Z", "DTEND:20260105T100000Z"),
		"event without DTSTAMP":        producerObject("VEVENT", "UID:e", "DTSTART:20260105T090000Z", "DTEND:20260105T100000Z"),
		"task without DTSTAMP":         producerObject("VTODO", "UID:t", "SUMMARY:Task"),
		"dates without VALUE=DATE":     producerObject("VEVENT", "UID:e", "DTSTART:20260105", "DTEND:20260106", "RRULE:FREQ=DAILY;COUNT=3", "EXDATE:20260106"),
		"DISPLAY alarm without DESCRIPTION": producerObject("VEVENT", "UID:e", "DTSTART:20260105T090000Z", "DTEND:20260105T100000Z",
			"BEGIN:VALARM", "ACTION:DISPLAY", "TRIGGER:-PT10M", "END:VALARM"),
		"repeated descriptive properties": producerObject("VEVENT", "UID:e", "DTSTART:20260105T090000Z", "DTEND:20260105T100000Z",
			"DESCRIPTION:a", "DESCRIPTION:b", "SUMMARY:a", "SUMMARY:b", "LOCATION:a", "LOCATION:b", "DTSTAMP:20261002T120000Z", "DTSTAMP:20261002T120000Z"),
		"DATE UNTIL on a UTC series":      producerObject("VEVENT", "UID:e", "DTSTART:20260105T090000Z", "DTEND:20260105T100000Z", "RRULE:FREQ=WEEKLY;UNTIL=20260301"),
		"DATE UNTIL on a zoned series":    producerObject("VEVENT", "UID:e", berlinEvent+"RRULE:FREQ=WEEKLY;UNTIL=20260301"),
		"DATE UNTIL on a floating series": producerObject("VEVENT", "UID:e", "DTSTART:20260105T090000", "DTEND:20260105T100000", "RRULE:FREQ=WEEKLY;UNTIL=20260301"),
		"IANA TZID without a VTIMEZONE": producerObject("VEVENT", "UID:e", berlinEvent+"RRULE:FREQ=WEEKLY;COUNT=4",
			"EXDATE;TZID=Europe/Berlin:20260112T090000"),
		"IANA TZID override without a VTIMEZONE": buildCalendarObject(
			buildComponent("VEVENT", "UID:e", berlinEvent+"RRULE:FREQ=WEEKLY;COUNT=4"),
			buildComponent("VEVENT", "UID:e", "RECURRENCE-ID;TZID=Europe/Berlin:20260112T090000",
				"DTSTART;TZID=Europe/Berlin:20260112T100000", "DTEND;TZID=Europe/Berlin:20260112T110000")),
		"calendar without VERSION":    "BEGIN:VCALENDAR\r\nPRODID:-//Script//EN\r\nBEGIN:VEVENT\r\nUID:e\r\nDTSTART:20260105T090000Z\r\nEND:VEVENT\r\nEND:VCALENDAR\r\n",
		"calendar without PRODID":     "BEGIN:VCALENDAR\r\nVERSION:2.0\r\nBEGIN:VEVENT\r\nUID:e\r\nDTSTART:20260105T090000Z\r\nEND:VEVENT\r\nEND:VCALENDAR\r\n",
		"VERSION with trailing space": strings.Replace(producerObject("VEVENT", "UID:e", "DTSTART:20260105T090000Z"), "VERSION:2.0", "VERSION:2.0 ", 1),
		"property after END:VALARM": producerObject("VEVENT", "UID:e", "DTSTART:20260105T090000Z",
			"BEGIN:VALARM", "ACTION:DISPLAY", "DESCRIPTION:x", "TRIGGER:-PT10M", "END:VALARM", "DTEND:20260105T100000Z"),
		"empty CN parameter":            producerObject("VEVENT", "UID:e", "DTSTART:20260105T090000Z", "ATTENDEE;CN=:mailto:a@example.test"),
		"duplicated standard parameter": producerObject("VEVENT", "UID:e", "DTSTART;VALUE=DATE-TIME;VALUE=DATE-TIME:20260105T090000Z"),
		"duplicated parameter differing only in case": producerObject("VEVENT", "UID:e", "DTSTART:20260105T090000Z",
			"BEGIN:VALARM", "ACTION:DISPLAY", "TRIGGER;RELATED=START;RELATED=start:-PT5M", "END:VALARM"),
		"duplicated TZID, once quoted": buildCalendarObject(buildVTimezone("Custom/Zone"),
			buildComponent("VEVENT", "UID:e", "DTSTART;TZID=\"Custom/Zone\";TZID=Custom/Zone:20260105T090000")),
		"HTAB in a parameter value":       producerObject("VEVENT", "UID:e", "DTSTART:20260105T090000Z", "ATTENDEE;CN=Jane\tDoe:mailto:a@example.test"),
		"repeated descriptive task dates": producerObject("VTODO", "UID:t", "DTSTAMP:20261002T120000Z", "DTSTAMP:20261002T120000Z", "LAST-MODIFIED:20261002T120000Z", "LAST-MODIFIED:20261002T120000Z"),
		"parameter without a value":       producerObject("VEVENT", "UID:e", "DTSTART:20260105T090000Z", "ATTENDEE;RSVP:mailto:a@example.test"),
	}
	for name, raw := range tests {
		t.Run(name, func(t *testing.T) {
			skipWithoutHostZone(t, raw, "Europe/Berlin")
			validated, fault := validateCalendarObjectForStorage(raw, nil)
			if fault != nil {
				t.Fatalf("refused: %+v\n%s", fault, raw)
			}
			if strings.Contains(raw, "TZID=Europe/Berlin") {
				// RFC 4791 §4.1: the zone the object names is stored with
				// the host's definition of it, and nothing else changes.
				const definition = "BEGIN:VTIMEZONE\r\nTZID:Europe/Berlin\r\n"
				begin := strings.Index(validated.Body, definition)
				end := strings.Index(validated.Body, "END:VTIMEZONE\r\n") + len("END:VTIMEZONE\r\n")
				if begin < 0 || end < begin {
					t.Fatalf("body to store lacks the Berlin definition:\n%s", validated.Body)
				}
				requireSentWithRequiredProperties(t, raw, validated.Body[:begin]+validated.Body[end:])
				return
			}
			requireSentWithRequiredProperties(t, raw, validated.Body)
		})
	}
}

// requireSentWithRequiredProperties checks that body is raw with nothing but
// the properties the validator adds where raw lacks them: VERSION:2.0, the
// server's PRODID and a UTC DTSTAMP.
func requireSentWithRequiredProperties(t *testing.T, raw, body string) {
	t.Helper()
	sent := strings.Split(raw, "\r\n")
	next := 0
	for _, line := range strings.Split(body, "\r\n") {
		switch {
		case next < len(sent) && line == sent[next]:
			next++
		case line == "VERSION:2.0", line == "PRODID:"+calDAVServerProdID, dtstampLine.MatchString(line):
		default:
			t.Fatalf("stored line %q is neither sent nor a property the validator adds:\n%s", line, body)
		}
	}
	if next != len(sent) {
		t.Fatalf("stored object lacks the sent line %q:\n%s", sent[next], body)
	}
}

// skipWithoutHostZone skips a case whose object names tzid without defining it
// when the host carries no time zone database to define it either.
func skipWithoutHostZone(t *testing.T, raw, tzid string) {
	t.Helper()
	if !strings.Contains(raw, "TZID="+tzid) {
		return
	}
	if _, err := time.LoadLocation(tzid); err != nil {
		t.Skipf("tzdata unavailable: %v", err)
	}
}

// A byte order mark is not iCalendar content, so it is not stored: every reader
// of the stored object, the server's own included, looks for BEGIN:VCALENDAR at
// its first octet.
func TestCalendarObjectValidationDropsAByteOrderMark(t *testing.T) {
	validated, fault := validateCalendarObjectForStorage(utf8BOM+outlookExport, nil)
	if fault != nil {
		t.Fatalf("refused: %+v", fault)
	}
	if validated.Body != outlookExport {
		t.Fatalf("body to store starts %q, want the object without its byte order mark", validated.Body[:16])
	}

	h, eventRepo := writableCalendarServer()
	rr := putCalendarObject(t, h, "outlook.ics", utf8BOM+outlookExport)
	if rr.Code != http.StatusCreated {
		t.Fatalf("PUT = %d, want 201; body: %s", rr.Code, rr.Body.String())
	}
	// RFC 4791 §5.3.4: the stored octets are not the submitted ones, so no
	// strong ETag may be returned for them.
	if etag := rr.Header().Get("ETag"); etag != "" {
		t.Errorf("PUT returned ETag %s for octets it did not store as sent", etag)
	}
	for _, stored := range eventRepo.events {
		if stored.RawICAL != outlookExport {
			t.Errorf("stored octets start %q, want the object without its byte order mark", stored.RawICAL[:16])
		}
	}
}

// What stays refused is what the server could not answer queries about
// truthfully, or what RFC 4791 §4.1 rules out of a calendar object resource.
func TestCalendarObjectValidationStillRefuses(t *testing.T) {
	const (
		data     = "valid-calendar-data"
		resource = "valid-calendar-object-resource"
	)
	event := func(lines ...string) string {
		return producerObject("VEVENT", append([]string{"UID:e"}, lines...)...)
	}
	tests := map[string]struct {
		raw, condition string
	}{
		"METHOD":                           {strings.Replace(event("DTSTART:20260105T090000Z"), "VERSION:2.0", "METHOD:PUBLISH\r\nVERSION:2.0", 1), resource},
		"no UID":                           {producerObject("VEVENT", "DTSTART:20260105T090000Z"), resource},
		"two UIDs":                         {buildCalendarObject(buildComponent("VEVENT", "UID:a", "DTSTART:20260105T090000Z"), buildComponent("VEVENT", "UID:b", "DTSTART:20260106T090000Z")), resource},
		"two masters":                      {buildCalendarObject(buildComponent("VEVENT", "UID:a", "DTSTART:20260105T090000Z"), buildComponent("VEVENT", "UID:a", "DTSTART:20260106T090000Z")), resource},
		"unknown TZID without a VTIMEZONE": {event("DTSTART;TZID=Eastern Standard Time:20260105T090000"), resource},
		"host-local TZID":                  {event("DTSTART;TZID=Local:20260105T090000"), resource},
		"unclosed component":               {strings.Replace(event("DTSTART:20260105T090000Z"), "END:VEVENT\r\n", "", 1), data},
		"line that is not a content line":  {event("DTSTART:20260105T090000Z", "not a content line"), data},
		"NUL in a value":                   {event("DTSTART:20260105T090000Z", "SUMMARY:a\x00b"), data},
		"NUL in a parameter value":         {event("DTSTART:20260105T090000Z", "ATTENDEE;CN=a\x00b:mailto:a@example.test"), data},
		"two STATUS values":                {event("DTSTART:20260105T090000Z", "STATUS:CONFIRMED", "STATUS:CANCELLED"), data},
		"two TRANSP values":                {event("DTSTART:20260105T090000Z", "TRANSP:OPAQUE", "TRANSP:TRANSPARENT"), data},
		"two STATUS values on a task":      {producerObject("VTODO", "UID:t", "STATUS:NEEDS-ACTION", "STATUS:CANCELLED"), data},
		"two COMPLETED values":             {producerObject("VTODO", "UID:t", "COMPLETED:20260105T090000Z", "COMPLETED:20260106T090000Z"), data},
		"two CREATED values":               {producerObject("VTODO", "UID:t", "CREATED:20260105T090000Z", "CREATED:20260106T090000Z"), data},
		"two STATUS values on a journal":   {producerObject("VJOURNAL", "UID:j", "STATUS:FINAL", "STATUS:CANCELLED"), data},
		"conflicting TZID parameters": {buildCalendarObject(buildVTimezone("Custom/A"), buildVTimezone("Custom/B"),
			buildComponent("VEVENT", "UID:e", "DTSTART;TZID=Custom/A;TZID=Custom/B:20260105T090000")), data},
		"conflicting VALUE parameters": {event("DTSTART;VALUE=DATE;VALUE=DATE-TIME:20260105"), data},
		"conflicting RANGE parameters": {buildCalendarObject(
			buildComponent("VEVENT", "UID:e", "DTSTART:20260105T090000Z", "RRULE:FREQ=DAILY;COUNT=5"),
			buildComponent("VEVENT", "UID:e", "RECURRENCE-ID;RANGE=THISANDFUTURE;RANGE=X-OTHER:20260106T090000Z", "DTSTART:20260106T100000Z")), data},
		"conflicting RELATED parameters": {event("DTSTART:20260105T090000Z", "DTEND:20260105T100000Z",
			"BEGIN:VALARM", "ACTION:DISPLAY", "TRIGGER;RELATED=START;RELATED=END:-PT5M", "END:VALARM"), data},
		"conflicting FBTYPE parameters": {producerObject("VFREEBUSY", "UID:f", "DTSTART:20260105T000000Z", "DTEND:20260106T000000Z",
			"FREEBUSY;FBTYPE=BUSY;FBTYPE=FREE:20260105T090000Z/PT1H"), data},
		"end before start":                 {event("DTSTART:20260105T090000Z", "DTEND:20260105T080000Z"), data},
		"negative DURATION":                {event("DTSTART:20260105T090000Z", "DURATION:-PT1H"), data},
		"date start with timed end":        {event("DTSTART;VALUE=DATE:20260105", "DTEND:20260105T100000Z"), data},
		"unparseable DTSTART":              {event("DTSTART:2026-01-05"), data},
		"invalid RRULE":                    {event("DTSTART:20260105T090000Z", "RRULE:FREQ=WEEKLY;COUNT=0"), data},
		"two DTSTARTs":                     {event("DTSTART:20260105T090000Z", "DTSTART:20260106T090000Z"), data},
		"two RRULEs":                       {event("DTSTART:20260105T090000Z", "RRULE:FREQ=DAILY;COUNT=2", "RRULE:FREQ=WEEKLY;COUNT=2"), data},
		"repeating alarm with no interval": {event("DTSTART:20260105T090000Z", "BEGIN:VALARM", "ACTION:DISPLAY", "TRIGGER:-PT10M", "DURATION:PT0S", "REPEAT:2", "END:VALARM"), data},
		"zero-length published free-busy":  {producerObject("VFREEBUSY", "UID:f", "DTSTART:20260105T090000Z", "DTEND:20260105T090000Z"), data},
		"DATE-TIME UNTIL on a DATE series": {event("DTSTART;VALUE=DATE:20260105", "RRULE:FREQ=DAILY;UNTIL=20260110T000000Z"), data},
		"vCalendar 1.0":                    {strings.Replace(event("DTSTART:20260105T090000Z"), "VERSION:2.0", "VERSION:1.0", 1), data},
		"DATE in a UTC-only property":      {event("DTSTART:20260105T090000Z", "DTSTAMP:20261002"), data},
		// RFC 5545 §3.1: a parameter is a name, "=" and a value. Left out,
		// this TZID would place the start as a floating time.
		"TZID parameter without \"=\"":       {event("DTSTART;TZID:20250310T090000"), data},
		"parameter without \"=\"":            {event("DTSTART:20260105T090000Z", "ATTENDEE;PARTSTAT:mailto:a@example.test"), data},
		"RSVP without \"=\" off an ATTENDEE": {event("DTSTART:20260105T090000Z", "ORGANIZER;RSVP:mailto:a@example.test"), data},
		"parameter name outside the grammar": {event("DTSTART:20260105T090000Z", "ATTENDEE;X BAD=1:mailto:a@example.test"), data},
		"empty parameter name":               {event("DTSTART;=Europe/Berlin:20260105T090000"), data},
	}
	for name, tt := range tests {
		t.Run(name, func(t *testing.T) {
			_, fault := validateCalendarObjectForStorage(tt.raw, nil)
			if fault == nil {
				t.Fatalf("accepted:\n%s", tt.raw)
			}
			if !reflect.DeepEqual(fault.conditions, []string{tt.condition}) {
				t.Errorf("conditions = %v, want [%s]", fault.conditions, tt.condition)
			}
		})
	}
}

// The REST API and the UI reach the validator through ValidateCalendarObject,
// which hands back what PUT stores and indexes, and refuses with the status and
// precondition PUT would answer.
func TestValidateCalendarObjectIsWhatPUTApplies(t *testing.T) {
	object, err := ValidateCalendarObject(utf8BOM+outlookExport, nil)
	if err != nil {
		t.Fatalf("refused: %v", err)
	}
	if object.Body != outlookExport || object.UID != "040000008200E00074C5B7101A82E00800000000D0F2A3C1E5A5DA01000000000000000010000000" {
		t.Errorf("object = %+v", object)
	}
	if object.Metadata.Summary == nil || *object.Metadata.Summary != "Standup" || object.Metadata.DTStart == nil {
		t.Errorf("metadata = %+v", object.Metadata)
	}

	todoOnly := &store.CalendarAccess{Calendar: store.Calendar{SupportedComponents: []string{"VTODO"}}}
	for raw, want := range map[string]calobject.Error{
		producerObject("VEVENT", "DTSTART:20260105T090000Z"): {Status: http.StatusForbidden, Conditions: []string{"valid-calendar-object-resource"}},
		googleExport: {Status: http.StatusForbidden, Conditions: []string{"supported-calendar-component"}},
	} {
		_, err := ValidateCalendarObject(raw, todoOnly)
		var refusal *calobject.Error
		if !errors.As(err, &refusal) || !reflect.DeepEqual(*refusal, want) {
			t.Errorf("error = %#v, want %#v", err, want)
		}
		if errors.Is(err, calobject.ErrUndefinedTimezone) {
			t.Errorf("%v reads as an undefined time zone", err)
		}
	}
}

// A TZID neither the object nor the host defines is refused with the status
// and precondition PUT answers, and the refusal says which reason it was, so
// the REST API can tell a user what to fix.
func TestValidateCalendarObjectNamesAnUndefinedTimezone(t *testing.T) {
	want := calobject.Error{Status: http.StatusForbidden, Conditions: []string{"valid-calendar-object-resource"}}
	for name, raw := range map[string]string{
		"zone neither defined nor known": producerObject("VEVENT", "UID:e", "DTSTART;TZID=Example/Nowhere:20260105T090000"),
		"host-local zone":                producerObject("VEVENT", "UID:e", "DTSTART;TZID=Local:20260105T090000"),
		"zone named by an alarm": producerObject("VEVENT", "UID:e", "DTSTART:20260105T090000Z",
			"BEGIN:VALARM", "ACTION:DISPLAY", "TRIGGER;VALUE=DATE-TIME:20260105T085000Z", "X-SNOOZE;TZID=Example/Nowhere:20260105T085500", "END:VALARM"),
	} {
		t.Run(name, func(t *testing.T) {
			_, err := ValidateCalendarObject(raw, nil)
			if !errors.Is(err, calobject.ErrUndefinedTimezone) {
				t.Errorf("error = %v, want it to wrap ErrUndefinedTimezone", err)
			}
			var refusal *calobject.Error
			if !errors.As(err, &refusal) || refusal.Status != want.Status || !reflect.DeepEqual(refusal.Conditions, want.Conditions) {
				t.Errorf("error = %#v, want %d %v", err, want.Status, want.Conditions)
			}

			h, _ := writableCalendarServer()
			rr := putCalendarObject(t, h, "undefined-zone.ics", raw)
			assertErrorConditions(t, rr, http.StatusForbidden, calQN("valid-calendar-object-resource"))
		})
	}

	duplicate := buildCalendarObject(buildVTimezone("Custom/Zone"), buildVTimezone("Custom/Zone"),
		buildComponent("VEVENT", "UID:e", "DTSTART;TZID=Custom/Zone:20260105T090000"))
	if _, err := ValidateCalendarObject(duplicate, nil); err == nil || errors.Is(err, calobject.ErrUndefinedTimezone) {
		t.Errorf("two definitions of one zone: error = %v, want a refusal that is not an undefined zone", err)
	}
}

// A zone the object names without defining is the host's definition of that
// name, both where PUT bounds the object's dates and where reports place it.
func TestReportsPlaceAnUndefinedIANAZoneByTheHostDefinition(t *testing.T) {
	if _, err := time.LoadLocation("Europe/Berlin"); err != nil {
		t.Skipf("tzdata unavailable: %v", err)
	}
	raw := producerObject("VEVENT", "UID:e", berlinEvent+"RRULE:FREQ=WEEKLY;COUNT=4", "EXDATE;TZID=Europe/Berlin:20260112T090000")
	if _, fault := validateCalendarObjectForStorage(raw, nil); fault != nil {
		t.Fatalf("refused: %+v", fault)
	}
	event := store.Event{UID: "e", RawICAL: raw}
	// Berlin is an hour ahead of UTC in January.
	if !filterMatches(event, calQueryWithTimeRange("20260119T080000Z", "20260119T081500Z"), floatingZone{}) {
		t.Error("calendar-query missed the 2026-01-19 instance at 08:00Z")
	}
	if filterMatches(event, calQueryWithTimeRange("20260112T000000Z", "20260113T000000Z"), floatingZone{}) {
		t.Error("calendar-query matched the excluded 2026-01-12 instance")
	}
	body := mustReportText((&DavServer{}).generateFreeBusy(t.Context(), freeBusyCandidates([]store.Event{event}, floatingZone{}),
		&timeRange{Start: "20260119T000000Z", End: "20260120T000000Z"}))
	assertPublishedFreeBusy(t, parsedFreeBusyLines(t, body), []string{"FREEBUSY:20260119T080000Z/20260119T090000Z"})
}

// A zero-length event is an instant, which RFC 4791 §9.9 selects when the range
// contains it.
func TestCalendarQuerySelectsAZeroLengthEventAtItsInstant(t *testing.T) {
	for name, raw := range map[string]string{
		"DTEND equal to DTSTART": producerObject("VEVENT", "UID:z", "DTSTART:20260105T090000Z", "DTEND:20260105T090000Z"),
		"zero DURATION":          producerObject("VEVENT", "UID:z", "DTSTART:20260105T090000Z", "DURATION:PT0S"),
	} {
		event := store.Event{UID: "z", RawICAL: raw}
		if !filterMatches(event, calQueryWithTimeRange("20260105T083000Z", "20260105T093000Z"), floatingZone{}) {
			t.Errorf("%s: calendar-query missed the event", name)
		}
		if filterMatches(event, calQueryWithTimeRange("20260105T093000Z", "20260105T103000Z"), floatingZone{}) {
			t.Errorf("%s: calendar-query matched a range after the event", name)
		}
	}
}

// dtstampLine matches the DTSTAMP the validator adds: a UTC date-time, as RFC
// 5545 §3.8.7.2 requires.
var dtstampLine = regexp.MustCompile(`^DTSTAMP:\d{8}T\d{6}Z$`)

// requireStamp checks that line is a DTSTAMP the validator added at a time
// within [before, after], and returns the time it names.
func requireStamp(t *testing.T, line string, before, after time.Time) {
	t.Helper()
	if !dtstampLine.MatchString(line) {
		t.Fatalf("%q is not a UTC DTSTAMP line", line)
	}
	stamp, err := time.Parse("20060102T150405Z", strings.TrimPrefix(line, "DTSTAMP:"))
	if err != nil {
		t.Fatal(err)
	}
	if stamp.Before(before.Truncate(time.Second)) || stamp.After(after) {
		t.Errorf("DTSTAMP %s is not the time of the write, between %s and %s", stamp, before, after)
	}
}

// RFC 5545 requires VERSION and PRODID of a VCALENDAR (§3.6) and DTSTAMP of
// each VEVENT, VTODO, VJOURNAL and VFREEBUSY (§3.6.1-§3.6.4). Scripts and
// minimal clients leave them out, and nothing the server answers reads them,
// so an object without them is accepted and stored with them added: VERSION
// and PRODID after BEGIN:VCALENDAR, and a DTSTAMP of the time of the write
// after each component's UID. The stored object then differs from the one
// sent, so PUT withholds its ETag, and every client reads it back complete.
func TestValidatorAddsThePropertiesRFC5545Requires(t *testing.T) {
	const minimal = "BEGIN:VCALENDAR\r\nBEGIN:VEVENT\r\nUID:minimal\r\nDTSTART:20260105T090000Z\r\nEND:VEVENT\r\nEND:VCALENDAR\r\n"
	h, eventRepo := writableCalendarServer()
	before := time.Now().UTC()
	rr := putCalendarObject(t, h, "minimal.ics", minimal)
	after := time.Now().UTC()
	if rr.Code != http.StatusCreated {
		t.Fatalf("PUT = %d: %s", rr.Code, rr.Body.String())
	}
	if etag := rr.Header().Get("ETag"); etag != "" {
		t.Errorf("PUT returned ETag %s for an object it stored completed", etag)
	}
	stored := storedEventByUID(eventRepo, "minimal").RawICAL
	lines := strings.Split(strings.TrimSuffix(stored, "\r\n"), "\r\n")
	want := []string{"BEGIN:VCALENDAR", "VERSION:2.0", "PRODID:" + calDAVServerProdID, "BEGIN:VEVENT", "UID:minimal", "",
		"DTSTART:20260105T090000Z", "END:VEVENT", "END:VCALENDAR"}
	if len(lines) != len(want) {
		t.Fatalf("stored\n%s", stored)
	}
	for i := range want {
		if i == 5 {
			requireStamp(t, lines[i], before, after)
			continue
		}
		if lines[i] != want[i] {
			t.Errorf("stored line %d = %q, want %q:\n%s", i, lines[i], want[i], stored)
		}
	}

	multiget := `<C:calendar-multiget xmlns:D="DAV:" xmlns:C="urn:ietf:params:xml:ns:caldav"><D:prop><D:getetag/><C:calendar-data/></D:prop>` +
		`<D:href>/dav/calendars/1/minimal.ics</D:href></C:calendar-multiget>`
	code, body := reportOn(t, h, multiget)
	if code != http.StatusMultiStatus || !strings.Contains(body, "VERSION:2.0") || !strings.Contains(body, "PRODID:"+calDAVServerProdID) ||
		!strings.Contains(body, lines[5]) {
		t.Errorf("calendar-multiget = %d without the added properties:\n%s", code, body)
	}

	// Written back as read, the object is stored as it is, with its ETag.
	rr = putCalendarObject(t, h, "minimal.ics", stored)
	if rr.Code != http.StatusNoContent || rr.Header().Get("ETag") == "" {
		t.Errorf("PUT of the stored object = %d, ETag %q; want 204 with the ETag", rr.Code, rr.Header().Get("ETag"))
	}
	if got := storedEventByUID(eventRepo, "minimal").RawICAL; got != stored {
		t.Errorf("the stored object written back was stored as\n%s", got)
	}
}

// A DTSTAMP is added to each scheduling component that lacks one, an override
// included, after its own UID rather than an alarm's, and to nothing else: a
// VTIMEZONE, its observances and a VALARM take none. A component that has one,
// and a VCALENDAR with VERSION and PRODID, are left as they are.
func TestValidatorAddsDTSTAMPOnlyToSchedulingComponents(t *testing.T) {
	alarm := []string{"BEGIN:VALARM", "UID:alarm", "ACTION:DISPLAY", "DESCRIPTION:x", "TRIGGER:-PT5M", "END:VALARM"}
	tests := map[string]struct {
		raw     string
		stamped []string // the line each DTSTAMP is added after
	}{
		"series with an override": {
			raw: buildCalendarObject(fixedOffsetVTimezone("Custom/Zone", "+0300"),
				buildComponent("VEVENT", append([]string{"DTSTART;TZID=Custom/Zone:20260105T090000", "RRULE:FREQ=DAILY;COUNT=3", "UID:series"}, alarm...)...),
				buildComponent("VEVENT", "UID:series", "RECURRENCE-ID;TZID=Custom/Zone:20260106T090000", "DTSTART;TZID=Custom/Zone:20260106T100000")),
			stamped: []string{"UID:series", "UID:series"},
		},
		"override already stamped": {
			raw: buildCalendarObject(
				buildComponent("VEVENT", "UID:series", "DTSTART:20260105T090000Z", "RRULE:FREQ=DAILY;COUNT=3"),
				buildComponent("VEVENT", "UID:series", "DTSTAMP:20250101T000000Z", "RECURRENCE-ID:20260106T090000Z", "DTSTART:20260106T100000Z")),
			stamped: []string{"UID:series"},
		},
		"task":          {raw: producerObject("VTODO", "SUMMARY:Task", "UID:task"), stamped: []string{"UID:task"}},
		"journal":       {raw: producerObject("VJOURNAL", "UID:note", "DTSTART;VALUE=DATE:20260105"), stamped: []string{"UID:note"}},
		"free-busy":     {raw: producerObject("VFREEBUSY", "UID:busy", "DTSTART:20260105T000000Z", "DTEND:20260106T000000Z"), stamped: []string{"UID:busy"}},
		"folded UID":    {raw: producerObject("VEVENT", "UID:fol\r\n ded", "DTSTART:20260105T090000Z"), stamped: []string{"UID:fol\r\n ded"}},
		"already whole": {raw: producerObject("VEVENT", "UID:whole", "DTSTAMP:20250101T000000Z", "DTSTART:20260105T090000Z")},
	}
	for name, tt := range tests {
		t.Run(name, func(t *testing.T) {
			before := time.Now().UTC()
			validated, fault := validateCalendarObjectForStorage(tt.raw, nil)
			after := time.Now().UTC()
			if fault != nil {
				t.Fatalf("refused: %+v\n%s", fault, tt.raw)
			}
			if len(tt.stamped) == 0 {
				if validated.Body != tt.raw {
					t.Errorf("stored\n%s\nwant the object as sent", validated.Body)
				}
				return
			}
			body := validated.Body
			stamps := regexp.MustCompile(`DTSTAMP:\d{8}T\d{6}Z\r\n`).FindAllStringIndex(body, -1)
			added := 0
			rest := body
			for _, uid := range tt.stamped {
				at := strings.Index(rest, uid+"\r\n")
				if at < 0 {
					t.Fatalf("no %q in\n%s", uid, body)
				}
				rest = rest[at+len(uid)+2:]
				line, _, _ := strings.Cut(rest, "\r\n")
				requireStamp(t, line, before, after)
				rest = rest[len(line)+2:]
				added++
			}
			if want := strings.Count(tt.raw, "DTSTAMP:") + added; len(stamps) != want {
				t.Errorf("%d DTSTAMPs stored, want %d:\n%s", len(stamps), want, body)
			}
			if without := regexp.MustCompile(`(\r\n)DTSTAMP:\d{8}T\d{6}Z\r\n`).ReplaceAllStringFunc(body, func(match string) string {
				if strings.Contains(tt.raw, match) {
					return match
				}
				return "\r\n"
			}); without != tt.raw {
				t.Errorf("stored\n%s\nwant the object as sent with DTSTAMPs added", body)
			}
		})
	}
}

// The properties are inserted with the line ending the object uses, and an
// object missing only some of them gets only those.
func TestValidatorAddsOnlyTheMissingCalendarProperties(t *testing.T) {
	tests := map[string]struct{ raw, want string }{
		"no PRODID, LF line endings": {
			raw:  "BEGIN:VCALENDAR\nVERSION:2.0\nBEGIN:VEVENT\nUID:e\nDTSTAMP:20250101T000000Z\nDTSTART:20260105T090000Z\nEND:VEVENT\nEND:VCALENDAR\n",
			want: "BEGIN:VCALENDAR\nPRODID:" + calDAVServerProdID + "\nVERSION:2.0\nBEGIN:VEVENT\nUID:e\nDTSTAMP:20250101T000000Z\nDTSTART:20260105T090000Z\nEND:VEVENT\nEND:VCALENDAR\n",
		},
		"no VERSION": {
			raw:  "BEGIN:VCALENDAR\r\nPRODID:-//Script//EN\r\nBEGIN:VEVENT\r\nUID:e\r\nDTSTAMP:20250101T000000Z\r\nDTSTART:20260105T090000Z\r\nEND:VEVENT\r\nEND:VCALENDAR\r\n",
			want: "BEGIN:VCALENDAR\r\nVERSION:2.0\r\nPRODID:-//Script//EN\r\nBEGIN:VEVENT\r\nUID:e\r\nDTSTAMP:20250101T000000Z\r\nDTSTART:20260105T090000Z\r\nEND:VEVENT\r\nEND:VCALENDAR\r\n",
		},
	}
	for name, tt := range tests {
		t.Run(name, func(t *testing.T) {
			validated, fault := validateCalendarObjectForStorage(tt.raw, nil)
			if fault != nil {
				t.Fatalf("refused: %+v", fault)
			}
			if validated.Body != tt.want {
				t.Errorf("stored\n%q\nwant\n%q", validated.Body, tt.want)
			}
		})
	}
	if _, fault := validateCalendarObjectForStorage(strings.Replace(producerObject("VEVENT", "UID:e", "DTSTART:20260105T090000Z"), "VERSION:2.0", "VERSION:1.0", 1), nil); fault == nil {
		t.Error("a vCalendar 1.0 object was accepted")
	}
}

// Properties that would take the object past CALDAV:max-resource-size are not
// added, so a client can always write back what it read.
func TestValidatorAddsNoPropertyPastTheMaximumResourceSize(t *testing.T) {
	object := producerObject("VEVENT", "UID:e", "DTSTART:20260105T090000Z", "DESCRIPTION:")
	raw := strings.Replace(object, "DESCRIPTION:", "DESCRIPTION:"+strings.Repeat("x", int(maxDAVBodyBytes)-len(object)-10), 1)
	validated, fault := validateCalendarObjectForStorage(raw, nil)
	if fault != nil {
		t.Fatalf("refused: %+v", fault)
	}
	if validated.Body != raw {
		t.Errorf("body to store is %d octets, want the %d submitted", len(validated.Body), len(raw))
	}
}

// COPY and MOVE write a stored object again through the validator, but
// complete nothing in it: the object was accepted as it is stored, and adding
// a DTSTAMP or a zone definition would change the octets, and so the ETag, of
// every member a collection MOVE carries, sending every client back for all of
// them. A PUT of the same object is completed.
func TestTransfersStoreAnIncompleteObjectAsItIs(t *testing.T) {
	requireHostZones(t, "Europe/Berlin")
	legacy := map[string]string{
		"no-dtstamp": "BEGIN:VCALENDAR\r\nBEGIN:VEVENT\r\nUID:no-dtstamp\r\nDTSTART:20260105T090000Z\r\nEND:VEVENT\r\nEND:VCALENDAR\r\n",
		"no-vtimezone": "BEGIN:VCALENDAR\r\nVERSION:2.0\r\nPRODID:-//CalCard//EN\r\nBEGIN:VEVENT\r\nUID:no-vtimezone\r\nDTSTAMP:20250101T000000Z\r\n" +
			"DTSTART;TZID=Europe/Berlin:20260105T090000\r\nEND:VEVENT\r\nEND:VCALENDAR\r\n",
	}
	serve := func(h *DavServer, method, path, destination string) *httptest.ResponseRecorder {
		req := httptest.NewRequest(method, path, nil)
		req.Header.Set("Destination", destination)
		rr := httptest.NewRecorder()
		h.ServeHTTP(rr, req.WithContext(auth.WithUser(req.Context(), &store.User{ID: 1})))
		return rr
	}
	for uid, raw := range legacy {
		t.Run(uid+"/PUT", func(t *testing.T) {
			h, eventRepo := writableCalendarServer()
			if rr := putCalendarObject(t, h, uid+".ics", raw); rr.Code != http.StatusCreated || rr.Header().Get("ETag") != "" {
				t.Fatalf("PUT = %d, ETag %q; want 201 without an ETag", rr.Code, rr.Header().Get("ETag"))
			}
			if stored := storedEventByUID(eventRepo, uid).RawICAL; stored == raw {
				t.Errorf("PUT stored the object as sent:\n%s", stored)
			}
		})
		for _, method := range []string{"COPY", "MOVE"} {
			t.Run(uid+"/"+method, func(t *testing.T) {
				calendars := &fakeCalendarRepo{accessible: []store.CalendarAccess{
					{Calendar: store.Calendar{ID: 1, UserID: 1, Name: "Source"}, Editor: true},
					{Calendar: store.Calendar{ID: 2, UserID: 1, Name: "Destination"}, Editor: true},
				}}
				events := &fakeEventRepo{events: map[string]*store.Event{
					"1:" + uid: {CalendarID: 1, UID: uid, ResourceName: uid, RawICAL: raw, ETag: "source-etag"},
				}}
				h := NewDavServer(Options{Store: &store.Store{Calendars: calendars, Events: events, CalendarTransfers: events}})
				if rr := serve(h, method, "/dav/calendars/1/"+uid+".ics", "/dav/calendars/2/"+uid+".ics"); rr.Code != http.StatusCreated {
					t.Fatalf("%s = %d, want 201: %s", method, rr.Code, rr.Body.String())
				}
				moved := events.events[events.key(2, uid)]
				if moved == nil || moved.RawICAL != raw {
					t.Fatalf("%s destination = %#v, want the source body", method, moved)
				}
				if method == "MOVE" && moved.ETag != "source-etag" {
					t.Errorf("MOVE changed the ETag to %q", moved.ETag)
				}
			})
		}
	}
	t.Run("collection MOVE", func(t *testing.T) {
		sourceSlug := "source"
		source := store.Calendar{ID: 1, UserID: 1, Name: "Source", Slug: &sourceSlug}
		calendars := &fakeCalendarRepo{
			accessible: []store.CalendarAccess{{Calendar: source, Editor: true}},
			calendars:  map[int64]*store.Calendar{1: &source},
		}
		events := &fakeEventRepo{events: map[string]*store.Event{}}
		id := int64(10)
		for uid, raw := range legacy {
			id++
			events.events["1:"+uid] = &store.Event{ID: id, CalendarID: 1, UID: uid, ResourceName: uid, RawICAL: raw, ETag: uid + "-etag"}
		}
		backend := &recordingCalendarCollectionBackend{result: &store.CalendarCollectionTransferResult{
			Calendar: &store.Calendar{ID: 9, UserID: 1, Name: "Source"}, Created: true,
		}}
		h := NewDavServer(Options{Store: &store.Store{Calendars: calendars, Events: events, CalendarCollections: backend}})
		if rr := serve(h, "MOVE", "/dav/calendars/source/", "/dav/calendars/renamed"); rr.Code != http.StatusCreated {
			t.Fatalf("MOVE = %d, want 201: %s", rr.Code, rr.Body.String())
		}
		if len(backend.transfers) != 1 || len(backend.transfers[0].Members) != len(legacy) {
			t.Fatalf("transferred %#v", backend.transfers)
		}
		for _, member := range backend.transfers[0].Members {
			if member.RawICAL != legacy[member.UID] || member.ETag != member.UID+"-etag" || member.NewETag != "" {
				t.Errorf("member %s moved as\n%s\nwith ETag %q/%q; want it as stored, with its ETag", member.UID, member.RawICAL, member.ETag, member.NewETag)
			}
		}
	})
}

// Calendar applications export a one-day event with its date as both ends,
// which RFC 5545 §3.8.2.2 does not allow: DTEND is later than DTSTART, and a
// DATE DTEND is the day after the last one the event occupies (§3.6.1). The
// server reads such an event as occupying its day, and a write stores it as
// saying so, with its DTEND moved to the next day and nothing else changed.
func TestValidatorStoresAOneDayEventsDateEndAsTheNextDay(t *testing.T) {
	const head = "BEGIN:VCALENDAR\r\nVERSION:2.0\r\nPRODID:-//Producer//EN\r\n"
	h, eventRepo := writableCalendarServer()
	raw := head + "BEGIN:VEVENT\r\nUID:one-day\r\nDTSTAMP:20250101T000000Z\r\nDTSTART;VALUE=DATE:20260301\r\nDTEND;VALUE=DATE:20260301\r\nEND:VEVENT\r\nEND:VCALENDAR\r\n"
	rr := putCalendarObject(t, h, "one-day.ics", raw)
	if rr.Code != http.StatusCreated || rr.Header().Get("ETag") != "" {
		t.Fatalf("PUT = %d, ETag %q; want 201 without an ETag", rr.Code, rr.Header().Get("ETag"))
	}
	stored := storedEventByUID(eventRepo, "one-day")
	if want := strings.Replace(raw, "DTEND;VALUE=DATE:20260301", "DTEND;VALUE=DATE:20260302", 1); stored.RawICAL != want {
		t.Errorf("stored\n%s\nwant\n%s", stored.RawICAL, want)
	}
	if end := stored.WriteMetadata.DTEnd; end == nil || !end.Equal(time.Date(2026, 3, 2, 0, 0, 0, 0, time.UTC)) {
		t.Errorf("stored dtend = %v, want 2 March", end)
	}
	if code, body := reportOn(t, h, timeRangeQuery("20260301T120000Z", "20260301T130000Z")); code != http.StatusMultiStatus || !strings.Contains(body, "one-day.ics") {
		t.Errorf("calendar-query over 1 March = %d without the event:\n%s", code, body)
	}

	tests := map[string]struct{ raw, want string }{
		"override, unmarked dates, LF line endings": {
			raw: "BEGIN:VCALENDAR\nVERSION:2.0\nPRODID:-//Producer//EN\n" +
				"BEGIN:VEVENT\nUID:series\nDTSTAMP:20250101T000000Z\nDTSTART;VALUE=DATE:20260301\nDTEND;VALUE=DATE:20260302\nRRULE:FREQ=WEEKLY;COUNT=3\nEND:VEVENT\n" +
				"BEGIN:VEVENT\nUID:series\nDTSTAMP:20250101T000000Z\nRECURRENCE-ID;VALUE=DATE:20260308\nDTSTART:20260309\nDTEND;X-NOTE=\"kept\":20260309\nEND:VEVENT\nEND:VCALENDAR\n",
			want: "BEGIN:VCALENDAR\nVERSION:2.0\nPRODID:-//Producer//EN\n" +
				"BEGIN:VEVENT\nUID:series\nDTSTAMP:20250101T000000Z\nDTSTART;VALUE=DATE:20260301\nDTEND;VALUE=DATE:20260302\nRRULE:FREQ=WEEKLY;COUNT=3\nEND:VEVENT\n" +
				"BEGIN:VEVENT\nUID:series\nDTSTAMP:20250101T000000Z\nRECURRENCE-ID;VALUE=DATE:20260308\nDTSTART:20260309\nDTEND;X-NOTE=\"kept\":20260310\nEND:VEVENT\nEND:VCALENDAR\n",
		},
		"end of a month and year": {
			raw:  head + "BEGIN:VEVENT\r\nUID:e\r\nDTSTAMP:20250101T000000Z\r\nDTSTART;VALUE=DATE:20261231\r\nDTEND;VALUE=DATE:20261231\r\nEND:VEVENT\r\nEND:VCALENDAR\r\n",
			want: head + "BEGIN:VEVENT\r\nUID:e\r\nDTSTAMP:20250101T000000Z\r\nDTSTART;VALUE=DATE:20261231\r\nDTEND;VALUE=DATE:20270101\r\nEND:VEVENT\r\nEND:VCALENDAR\r\n",
		},
		"folded DTEND": {
			raw:  head + "BEGIN:VEVENT\r\nUID:e\r\nDTSTAMP:20250101T000000Z\r\nDTSTART;VALUE=DATE:20260301\r\nDTE\r\n ND;VALUE=DATE:2026\r\n 0301\r\nEND:VEVENT\r\nEND:VCALENDAR\r\n",
			want: head + "BEGIN:VEVENT\r\nUID:e\r\nDTSTAMP:20250101T000000Z\r\nDTSTART;VALUE=DATE:20260301\r\nDTEND;VALUE=DATE:20260302\r\nEND:VEVENT\r\nEND:VCALENDAR\r\n",
		},
		"already the next day": {
			raw: head + "BEGIN:VEVENT\r\nUID:e\r\nDTSTAMP:20250101T000000Z\r\nDTSTART;VALUE=DATE:20260301\r\nDTEND;VALUE=DATE:20260302\r\nEND:VEVENT\r\nEND:VCALENDAR\r\n",
		},
		"timed event ending at its start": {
			raw: head + "BEGIN:VEVENT\r\nUID:e\r\nDTSTAMP:20250101T000000Z\r\nDTSTART:20260301T090000Z\r\nDTEND:20260301T090000Z\r\nEND:VEVENT\r\nEND:VCALENDAR\r\n",
		},
		"task with a DATE DTEND on its start": {
			raw: head + "BEGIN:VTODO\r\nUID:t\r\nDTSTAMP:20250101T000000Z\r\nDTSTART;VALUE=DATE:20260301\r\nDTEND;VALUE=DATE:20260301\r\nEND:VTODO\r\nEND:VCALENDAR\r\n",
		},
	}
	for name, tt := range tests {
		t.Run(name, func(t *testing.T) {
			want := tt.want
			if want == "" {
				want = tt.raw
			}
			validated, fault := validateCalendarObjectForStorage(tt.raw, nil)
			if fault != nil {
				t.Fatalf("refused: %+v", fault)
			}
			if validated.Body != want {
				t.Errorf("stored\n%q\nwant\n%q", validated.Body, want)
			}
		})
	}

	t.Run("already the next day, through PUT", func(t *testing.T) {
		h, eventRepo := writableCalendarServer()
		raw := tests["already the next day"].raw
		rr := putCalendarObject(t, h, "e.ics", raw)
		if rr.Code != http.StatusCreated || rr.Header().Get("ETag") == "" {
			t.Fatalf("PUT = %d, ETag %q; want 201 with the ETag", rr.Code, rr.Header().Get("ETag"))
		}
		if stored := storedEventByUID(eventRepo, "e").RawICAL; stored != raw {
			t.Errorf("stored\n%s", stored)
		}
	})
	t.Run("transfer", func(t *testing.T) {
		validated, fault := validateTransferredCalendarObject(raw, nil)
		if fault != nil || validated.Body != raw {
			t.Errorf("transfer stored %v, %+v; want the object as stored", validated, fault)
		}
	})
}

// RFC 5545 §3.8.7.2 makes DTSTAMP, outside scheduling, the time the component
// was last changed, which LAST-MODIFIED says, or else CREATED. A component
// without DTSTAMP that says either is stamped with it, so the same object
// written twice is stored the same way, under the same ETag; one saying
// neither is stamped with the time of the write.
func TestValidatorStampsAComponentWithItsLastModification(t *testing.T) {
	tests := map[string]struct{ properties []string }{
		"LAST-MODIFIED":            {[]string{"CREATED:20240101T000000Z", "LAST-MODIFIED:20250304T050607Z"}},
		"CREATED":                  {[]string{"CREATED:20250304T050607Z"}},
		"LAST-MODIFIED, lowercase": {[]string{"last-modified:20250304T050607Z"}},
	}
	for name, tt := range tests {
		t.Run(name, func(t *testing.T) {
			raw := producerObject("VEVENT", append([]string{"UID:e", "DTSTART:20260105T090000Z"}, tt.properties...)...)
			h, eventRepo := writableCalendarServer()
			if rr := putCalendarObject(t, h, "e.ics", raw); rr.Code != http.StatusCreated {
				t.Fatalf("PUT = %d: %s", rr.Code, rr.Body.String())
			}
			first := *storedEventByUID(eventRepo, "e")
			if !strings.Contains(first.RawICAL, "UID:e\r\nDTSTAMP:20250304T050607Z\r\n") {
				t.Errorf("stored\n%s\nwant DTSTAMP:20250304T050607Z after the UID", first.RawICAL)
			}
			if rr := putCalendarObject(t, h, "e.ics", raw); rr.Code != http.StatusNoContent {
				t.Fatalf("second PUT = %d: %s", rr.Code, rr.Body.String())
			}
			if second := storedEventByUID(eventRepo, "e"); second.RawICAL != first.RawICAL || second.ETag != first.ETag {
				t.Errorf("the same object written again is stored as\n%s\nunder %s; want\n%s\nunder %s", second.RawICAL, second.ETag, first.RawICAL, first.ETag)
			}
		})
	}
}
