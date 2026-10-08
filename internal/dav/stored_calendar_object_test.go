package dav

import (
	"net/http"
	"net/http/httptest"
	"reflect"
	"regexp"
	"strings"
	"testing"

	"github.com/jw6ventures/calcard/internal/auth"
	"github.com/jw6ventures/calcard/internal/store"
)

// storedObject wraps a VEVENT's content lines in a minimal VCALENDAR.
func storedObject(veventLines ...string) string {
	return "BEGIN:VCALENDAR\r\nVERSION:2.0\r\nPRODID:-//Legacy//EN\r\nBEGIN:VEVENT\r\n" +
		strings.Join(veventLines, "\r\n") + "\r\nEND:VEVENT\r\nEND:VCALENDAR\r\n"
}

// legacyStoredObjects are objects the store may hold whose only guarantee is
// balanced BEGIN/END lines, and which PUT refuses. Each is a weekly series of
// 09:00-10:00 UTC meetings starting Monday 2026-01-05.
var legacyStoredObjects = map[string]string{
	"line that is not a content line": storedObject(
		"UID:legacy", "DTSTAMP:20251201T000000Z", "DTSTART:20260105T090000Z", "DTEND:20260105T100000Z",
		"RRULE:FREQ=WEEKLY;COUNT=3", "Notes pasted without a property name"),
	// The second VCALENDAR holds the event, so reading it proves the two are
	// read as one calendar.
	"two VCALENDAR roots": "BEGIN:VCALENDAR\r\nVERSION:2.0\r\nPRODID:-//Legacy//EN\r\nEND:VCALENDAR\r\n" +
		storedObject("UID:legacy", "DTSTAMP:20251201T000000Z", "DTSTART:20260105T090000Z", "DTEND:20260105T100000Z",
			"RRULE:FREQ=WEEKLY;COUNT=3"),
}

// An event in a VCALENDAR nested inside the root is read as the root's own, so
// calendar-query reports the resource and free-busy publishes its time.
func TestReportsReadAnEventInANestedVCALENDAR(t *testing.T) {
	raw := "BEGIN:VCALENDAR\r\nVERSION:2.0\r\nPRODID:-//Legacy//EN\r\n" +
		storedObject("UID:legacy", "DTSTAMP:20251201T000000Z", "DTSTART:20260112T090000Z", "DTEND:20260112T100000Z") +
		"END:VCALENDAR\r\n"
	if _, fault := validateCalendarObjectForStorage(raw, nil); fault == nil {
		t.Fatal("PUT accepts the fixture, so it does not exercise stored-object reading")
	}
	event := store.Event{UID: "legacy", RawICAL: raw}
	if !filterMatches(event, calQueryWithTimeRange("20260112T000000Z", "20260113T000000Z"), floatingZone{}) {
		t.Error("calendar-query missed the nested event")
	}
	if filterMatches(event, calQueryWithTimeRange("20260113T000000Z", "20260114T000000Z"), floatingZone{}) {
		t.Error("calendar-query matched a day without the nested event")
	}
	body := mustReportText((&DavServer{}).generateFreeBusy(t.Context(), freeBusyCandidates([]store.Event{event}, floatingZone{}),
		&timeRange{Start: "20260112T000000Z", End: "20260113T000000Z"}))
	assertPublishedFreeBusy(t, parsedFreeBusyLines(t, body), []string{"FREEBUSY:20260112T090000Z/20260112T100000Z"})
}

// A stored object PUT would refuse is still the resource GET and
// sync-collection list. If calendar-query left it out, a sync client that
// reconciles by that query would delete its local copy, and if free-busy left it
// out, its time would be published as free. Every report has to read it.
func TestReportsReadStoredObjectsPUTRefuses(t *testing.T) {
	const day = `start="20260112T000000Z" end="20260113T000000Z"`
	reports := []struct {
		name, body string
		want       *regexp.Regexp
		status     int
	}{
		{
			name:   "calendar-query",
			body:   `<C:calendar-query xmlns:D="DAV:" xmlns:C="urn:ietf:params:xml:ns:caldav"><D:prop><D:getetag/></D:prop><C:filter><C:comp-filter name="VCALENDAR"><C:comp-filter name="VEVENT"><C:time-range ` + day + `/></C:comp-filter></C:comp-filter></C:filter></C:calendar-query>`,
			want:   regexp.MustCompile(`legacy\.ics`),
			status: 207,
		},
		{
			name: "calendar-query expanding calendar-data",
			body: `<C:calendar-query xmlns:D="DAV:" xmlns:C="urn:ietf:params:xml:ns:caldav"><D:prop><C:calendar-data><C:expand ` + day + `/></C:calendar-data></D:prop><C:filter><C:comp-filter name="VCALENDAR"><C:comp-filter name="VEVENT"><C:time-range ` + day + `/></C:comp-filter></C:comp-filter></C:filter></C:calendar-query>`,
			// The instance's RECURRENCE-ID carries the stored DTSTART's parameters.
			want:   regexp.MustCompile(`RECURRENCE-ID[^:\r\n]*:20260112T090000Z`),
			status: 207,
		},
		{
			name:   "free-busy",
			body:   `<C:free-busy-query xmlns:C="urn:ietf:params:xml:ns:caldav"><C:time-range ` + day + `/></C:free-busy-query>`,
			want:   regexp.MustCompile(`FREEBUSY:20260112T090000Z/20260112T100000Z`),
			status: 200,
		},
	}
	for shape, raw := range legacyStoredObjects {
		if _, fault := validateCalendarObjectForStorage(raw, nil); fault == nil {
			t.Fatalf("%s: PUT accepts the fixture, so it does not exercise stored-object reading", shape)
		}
		for _, report := range reports {
			t.Run(report.name+"/"+shape, func(t *testing.T) {
				h := &DavServer{store: &store.Store{
					Calendars: &fakeCalendarRepo{accessible: []store.CalendarAccess{
						{Calendar: store.Calendar{ID: 1, UserID: 1, Name: "Work"}, Editor: true},
					}},
					Events: &fakeEventRepo{events: map[string]*store.Event{
						"1:legacy": {ID: 1, CalendarID: 1, UID: "legacy", ResourceName: "legacy", ETag: "e", RawICAL: raw},
					}},
				}}
				req := httptest.NewRequest("REPORT", "/dav/calendars/1/", strings.NewReader(report.body))
				req.Header.Set("Depth", "1")
				rr := httptest.NewRecorder()
				h.Report(rr, req.WithContext(auth.WithUser(req.Context(), &store.User{ID: 1})))
				if rr.Code != report.status || !report.want.MatchString(rr.Body.String()) {
					t.Fatalf("want %d matching %q, got %d:\n%s", report.status, report.want, rr.Code, rr.Body.String())
				}
			})
		}
	}
}

// Stored-object reading recovers from defects; it does not reinterpret what is
// well formed, or the reports would read a valid object differently from PUT.
func TestStoredObjectParseAgreesWithPUTOnWellFormedObjects(t *testing.T) {
	for _, raw := range []string{
		storedObject("UID:a", "DTSTAMP:20251201T000000Z", "DTSTART;TZID=\"Europe/Berlin\":20260105T090000",
			"ATTENDEE;CN=\"Doe, Jane\";X-DUP=1;X-DUP=2:mailto:jane@example.test",
			"BEGIN:VALARM", "ACTION:DISPLAY", "DESCRIPTION:x", "TRIGGER:-PT5M", "END:VALARM"),
		"BEGIN:VCALENDAR\r\nVERSION:2.0\r\nPRODID:-//x//EN\r\nBEGIN:X-OPAQUE\r\nBEGIN:X-INNER\r\nEND:X-INNER\r\nX-AFTER:1\r\nEND:X-OPAQUE\r\nEND:VCALENDAR\r\n",
		storedObject("UID:a", "DTSTART:20260105T090000Z", "BEGIN:VALARM", "ACTION:DISPLAY", "TRIGGER:-PT5M", "END:VALARM",
			"SUMMARY:after the alarm", "ATTENDEE;CN=;RSVP;PARTSTAT=ACCEPTED;PARTSTAT=DECLINED:mailto:jane@example.test"),
	} {
		strict, err := parseICalendarObject(raw)
		if err != nil {
			t.Fatalf("fixture does not parse strictly: %v", err)
		}
		stored, err := parseStoredICalendarObject(raw)
		if err != nil {
			t.Fatalf("stored-object parse failed: %v", err)
		}
		if !reflect.DeepEqual(strict, stored) {
			t.Errorf("trees differ for\n%s", raw)
		}
	}
}

func TestStoredObjectParseRecoversTheComponentTree(t *testing.T) {
	const root = "BEGIN:VCALENDAR\r\nVERSION:2.0\r\n"
	tests := []struct {
		name, raw, want string
	}{
		{
			name: "an END naming an open component closes everything opened inside it",
			raw:  root + "BEGIN:VEVENT\r\nUID:a\r\nBEGIN:VALARM\r\nACTION:DISPLAY\r\nEND:VEVENT\r\nBEGIN:VTODO\r\nUID:b\r\nEND:VTODO\r\nEND:VCALENDAR\r\n",
			want: root + "BEGIN:VEVENT\r\nUID:a\r\nBEGIN:VALARM\r\nACTION:DISPLAY\r\nEND:VALARM\r\nEND:VEVENT\r\nBEGIN:VTODO\r\nUID:b\r\nEND:VTODO\r\nEND:VCALENDAR\r\n",
		},
		{
			name: "an END naming no open component is skipped",
			raw:  root + "BEGIN:VEVENT\r\nUID:a\r\nEND:VTODO\r\nSUMMARY:kept\r\nEND:VEVENT\r\nEND:VCALENDAR\r\n",
			want: root + "BEGIN:VEVENT\r\nUID:a\r\nSUMMARY:kept\r\nEND:VEVENT\r\nEND:VCALENDAR\r\n",
		},
		{
			name: "components left open close at the end of the object",
			raw:  root + "BEGIN:VEVENT\r\nUID:a\r\n",
			want: root + "BEGIN:VEVENT\r\nUID:a\r\nEND:VEVENT\r\nEND:VCALENDAR\r\n",
		},
		{
			name: "a further VCALENDAR continues the first",
			raw:  root + "BEGIN:VEVENT\r\nUID:a\r\nEND:VEVENT\r\nEND:VCALENDAR\r\nBEGIN:VCALENDAR\r\nBEGIN:VEVENT\r\nUID:a\r\nRECURRENCE-ID:20260112T090000Z\r\nEND:VEVENT\r\nEND:VCALENDAR\r\n",
			want: root + "BEGIN:VEVENT\r\nUID:a\r\nEND:VEVENT\r\nBEGIN:VEVENT\r\nUID:a\r\nRECURRENCE-ID:20260112T090000Z\r\nEND:VEVENT\r\nEND:VCALENDAR\r\n",
		},
		{
			name: "a VCALENDAR nested inside the root continues it",
			raw:  root + "BEGIN:VEVENT\r\nUID:a\r\nEND:VEVENT\r\nBEGIN:VCALENDAR\r\nVERSION:2.0\r\nBEGIN:VEVENT\r\nUID:a\r\nRECURRENCE-ID:20260112T090000Z\r\nEND:VEVENT\r\nEND:VCALENDAR\r\nX-AFTER:1\r\nEND:VCALENDAR\r\n",
			want: root + "VERSION:2.0\r\nX-AFTER:1\r\nBEGIN:VEVENT\r\nUID:a\r\nEND:VEVENT\r\nBEGIN:VEVENT\r\nUID:a\r\nRECURRENCE-ID:20260112T090000Z\r\nEND:VEVENT\r\nEND:VCALENDAR\r\n",
		},
		{
			name: "a VCALENDAR nested inside a component continues the root",
			raw:  root + "BEGIN:VEVENT\r\nUID:a\r\nBEGIN:VCALENDAR\r\nBEGIN:VTODO\r\nUID:b\r\nEND:VTODO\r\nEND:VCALENDAR\r\nSUMMARY:kept\r\nEND:VEVENT\r\nEND:VCALENDAR\r\n",
			want: root + "BEGIN:VEVENT\r\nUID:a\r\nSUMMARY:kept\r\nEND:VEVENT\r\nBEGIN:VTODO\r\nUID:b\r\nEND:VTODO\r\nEND:VCALENDAR\r\n",
		},
		{
			name: "content outside any VCALENDAR is skipped",
			raw:  "X-BEFORE:1\r\nBEGIN:VEVENT\r\nUID:stray\r\nEND:VEVENT\r\n" + root + "BEGIN:VEVENT\r\nUID:a\r\nEND:VEVENT\r\nEND:VCALENDAR\r\nX-AFTER:1\r\n",
			want: root + "BEGIN:VEVENT\r\nUID:a\r\nEND:VEVENT\r\nEND:VCALENDAR\r\n",
		},
		{
			name: "a line that is not a content line is skipped",
			raw:  root + "BEGIN:VEVENT\r\nUID:a\r\nnot a content line\r\nX BAD NAME:1\r\nSUMMARY:kept\r\nEND:VEVENT\r\nEND:VCALENDAR\r\n",
			want: root + "BEGIN:VEVENT\r\nUID:a\r\nSUMMARY:kept\r\nEND:VEVENT\r\nEND:VCALENDAR\r\n",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if _, err := parseICalendarObject(tt.raw); err == nil {
				t.Fatal("fixture parses strictly, so it does not exercise stored-object reading")
			}
			tree, err := parseStoredICalendarObject(tt.raw)
			if err != nil {
				t.Fatalf("parseStoredICalendarObject: %v", err)
			}
			if got := writeICalendarObject(tree); got != tt.want {
				t.Errorf("got\n%s\nwant\n%s", got, tt.want)
			}
		})
	}
}

// Parameters are read for matching and resolution, while the key part is kept
// as it was written, so a projection hands the client back the line it had.
// An empty value, a repeated parameter, and an attendee's RSVP written without
// a value are read from a client and from the store alike. The bare RSVP is
// read as though absent, which RFC 5545 §3.2.17 makes FALSE.
func TestPropertyReadingToleratesTheParametersProducersWrite(t *testing.T) {
	tests := []struct {
		line       string
		parameters map[string]string
	}{
		{"ATTENDEE;CN=:mailto:a@example.test", map[string]string{"CN": ""}},
		{"ATTENDEE;PARTSTAT=ACCEPTED;PARTSTAT=DECLINED:mailto:a@example.test", map[string]string{"PARTSTAT": "ACCEPTED"}},
		{"ATTENDEE;RSVP;ROLE=CHAIR:mailto:a@example.test", map[string]string{"ROLE": "CHAIR"}},
		{"attendee;rsvp :mailto:a@example.test", map[string]string{}},
	}
	for _, tt := range tests {
		for _, reading := range []icalReading{readSubmitted, readTransferred, readStored} {
			property, ok := readICalProperty(tt.line, reading)
			if !ok {
				t.Fatalf("%v %q: not read", reading, tt.line)
			}
			if !reflect.DeepEqual(property.parameters, tt.parameters) {
				t.Errorf("%v %q: parameters %v, want %v", reading, tt.line, property.parameters, tt.parameters)
			}
			if keyPart, _, _ := strings.Cut(tt.line, ":mailto"); property.keyPart != keyPart {
				t.Errorf("%v %q: key part %q, want it as written", reading, tt.line, property.keyPart)
			}
		}
	}
}

// RFC 5545 §3.1 and §3.2 make a parameter a name, "=" and a value. A client's
// line holding anything else is refused: left out, a TZID would place the
// value as a floating time, hours from where the client meant it. Objects
// stored without that check may hold such lines, and those are read, and
// written again by COPY and MOVE, with the parameter left out.
func TestMalformedParametersAreRefusedOnlyFromClients(t *testing.T) {
	tests := []struct {
		line       string
		parameters map[string]string
	}{
		{"DTSTART;TZID:20250310T090000", map[string]string{}},
		{"ATTENDEE;PARTSTAT;ROLE=CHAIR:mailto:a@example.test", map[string]string{"ROLE": "CHAIR"}},
		{"ORGANIZER;RSVP:mailto:a@example.test", map[string]string{}},
		{"ATTENDEE;X BAD=1;CN=\"Doe, Jane\":mailto:a@example.test", map[string]string{"CN": "Doe, Jane"}},
		{"DTSTART;=Europe/Berlin:20260105T090000", map[string]string{}},
	}
	for _, tt := range tests {
		if _, ok := readICalProperty(tt.line, readSubmitted); ok {
			t.Errorf("submitted %q was read", tt.line)
		}
		for _, reading := range []icalReading{readTransferred, readStored} {
			property, ok := readICalProperty(tt.line, reading)
			if !ok || !reflect.DeepEqual(property.parameters, tt.parameters) {
				t.Errorf("%v %q = %v, %v; want %v", reading, tt.line, property.parameters, ok, tt.parameters)
			}
		}
	}
}

// legacyMalformedParameter is an event stored without parameter checks, its
// TZID parameters missing their "=", which every reader takes as floating
// times.
var legacyMalformedParameter = producerObject("VEVENT", "UID:legacy-tzid", "DTSTAMP:20250101T000000Z",
	"DTSTART;TZID:20250310T090000", "DTEND;TZID:20250310T100000")

func TestPutRefusesAParameterWithoutItsValue(t *testing.T) {
	h, eventRepo := writableCalendarServer()
	rr := putCalendarObject(t, h, "legacy-tzid.ics", legacyMalformedParameter)
	if rr.Code != http.StatusForbidden || !strings.Contains(rr.Body.String(), "valid-calendar-data") {
		t.Fatalf("PUT = %d, want 403 valid-calendar-data: %s", rr.Code, rr.Body.String())
	}
	if stored := storedEventByUID(eventRepo, "legacy-tzid"); stored != nil {
		t.Errorf("stored %q", stored.RawICAL)
	}
}

// The store keeps such an object readable, and COPY and MOVE, which validate
// the object they write, carry it to another collection as it is stored.
func TestStoredObjectWithAMalformedParameterIsServedAndTransferred(t *testing.T) {
	newServer := func() (*DavServer, *fakeEventRepo) {
		calendars := &fakeCalendarRepo{accessible: []store.CalendarAccess{
			{Calendar: store.Calendar{ID: 1, UserID: 1, Name: "Source"}, Editor: true},
			{Calendar: store.Calendar{ID: 2, UserID: 1, Name: "Destination"}, Editor: true},
		}}
		events := &fakeEventRepo{events: map[string]*store.Event{
			"1:legacy-tzid": {ID: 11, CalendarID: 1, UID: "legacy-tzid", ResourceName: "legacy-tzid",
				RawICAL: legacyMalformedParameter, ETag: "source-etag"},
		}}
		return NewDavServer(Options{Store: &store.Store{Calendars: calendars, Events: events, CalendarTransfers: events}}), events
	}
	serve := func(h *DavServer, req *http.Request) *httptest.ResponseRecorder {
		rr := httptest.NewRecorder()
		h.ServeHTTP(rr, req.WithContext(auth.WithUser(req.Context(), &store.User{ID: 1})))
		return rr
	}

	h, _ := newServer()
	if rr := serve(h, httptest.NewRequest(http.MethodGet, "/dav/calendars/1/legacy-tzid.ics", nil)); rr.Code != http.StatusOK || rr.Body.String() != legacyMalformedParameter {
		t.Errorf("GET = %d:\n%s", rr.Code, rr.Body.String())
	}
	if code, body := reportOn(t, h, timeRangeQuery("20250310T093000Z", "20250310T094500Z")); code != http.StatusMultiStatus || !strings.Contains(body, "legacy-tzid.ics") {
		t.Errorf("calendar-query = %d without the event:\n%s", code, body)
	}

	for _, method := range []string{"COPY", "MOVE"} {
		t.Run(method, func(t *testing.T) {
			h, events := newServer()
			req := httptest.NewRequest(method, "/dav/calendars/1/legacy-tzid.ics", nil)
			req.Header.Set("Destination", "/dav/calendars/2/legacy-tzid.ics")
			if rr := serve(h, req); rr.Code != http.StatusCreated {
				t.Fatalf("%s = %d, want 201: %s", method, rr.Code, rr.Body.String())
			}
			if moved := events.events[events.key(2, "legacy-tzid")]; moved == nil || moved.RawICAL != legacyMalformedParameter {
				t.Fatalf("%s destination = %#v, want the source body", method, moved)
			}
		})
	}
	for _, method := range []string{"COPY", "MOVE"} {
		t.Run("collection "+method, func(t *testing.T) {
			sourceSlug := "source"
			source := store.Calendar{ID: 1, UserID: 1, Name: "Source", Slug: &sourceSlug}
			calendars := &fakeCalendarRepo{
				accessible: []store.CalendarAccess{{Calendar: source, Editor: true}},
				calendars:  map[int64]*store.Calendar{1: &source},
			}
			events := &fakeEventRepo{events: map[string]*store.Event{
				"1:legacy-tzid": {ID: 11, CalendarID: 1, UID: "legacy-tzid", ResourceName: "legacy-tzid",
					RawICAL: legacyMalformedParameter, ETag: "etag"},
			}}
			backend := &recordingCalendarCollectionBackend{result: &store.CalendarCollectionTransferResult{
				Calendar: &store.Calendar{ID: 9, UserID: 1, Name: "Source"}, Created: true,
			}}
			h := NewDavServer(Options{Store: &store.Store{Calendars: calendars, Events: events, CalendarCollections: backend}})
			req := httptest.NewRequest(method, "/dav/calendars/source/", nil)
			req.Header.Set("Destination", "/dav/calendars/copied")
			if rr := serve(h, req); rr.Code != http.StatusCreated {
				t.Fatalf("%s = %d, want 201: %s", method, rr.Code, rr.Body.String())
			}
			if len(backend.transfers) != 1 || len(backend.transfers[0].Members) != 1 ||
				backend.transfers[0].Members[0].RawICAL != legacyMalformedParameter {
				t.Fatalf("%s transferred %#v, want the member as stored", method, backend.transfers)
			}
		})
	}
}

// RFC 5545 §3.1 admits no control character but HTAB in a content line, yet
// Outlook and Exchange write vertical tabs and form feeds into text, and the
// store may hold any. Those are read as written, from a client and from the
// store alike. NUL is refused from a client, since
// PostgreSQL TEXT cannot hold it, and so are a CR and a LF, which can only end
// a content line.
func TestControlCharactersAreReadAsWrittenButNULAndLineBreaks(t *testing.T) {
	tests := []struct {
		line       string
		value      string
		parameters map[string]string
	}{
		{line: "SUMMARY:Stand\x01up", value: "Stand\x01up", parameters: map[string]string{}},
		{line: "DESCRIPTION:Agenda\vItem\fNext\x7f", value: "Agenda\vItem\fNext\x7f", parameters: map[string]string{}},
		{line: "ATTENDEE;CN=Jane\x01Doe:mailto:a@example.test", value: "mailto:a@example.test",
			parameters: map[string]string{"CN": "Jane\x01Doe"}},
		{line: "ATTENDEE;CN=\"Jane\x1fDoe\":mailto:a@example.test", value: "mailto:a@example.test",
			parameters: map[string]string{"CN": "Jane\x1fDoe"}},
		{line: "ATTENDEE;CN=Jane\x7fDoe:mailto:a@example.test", value: "mailto:a@example.test",
			parameters: map[string]string{"CN": "Jane\x7fDoe"}},
		{line: "ATTENDEE;CN=Jane\tDoe:mailto:a@example.test", value: "mailto:a@example.test",
			parameters: map[string]string{"CN": "Jane\tDoe"}},
	}
	for _, tt := range tests {
		for _, reading := range []icalReading{readSubmitted, readTransferred, readStored} {
			property, ok := readICalProperty(tt.line, reading)
			if !ok || property.value != tt.value || !reflect.DeepEqual(property.parameters, tt.parameters) {
				t.Errorf("%q (%v) = %q %v, %v; want it as written", tt.line, reading, property.value, property.parameters, ok)
			}
		}
	}
	for _, line := range []string{
		"SUMMARY:Stand\x00up", "ATTENDEE;CN=Jane\x00Doe:mailto:a@example.test",
		"SUMMARY:Stand\rup", "SUMMARY:Stand\nup", "ATTENDEE;CN=\"Jane\nDoe\":mailto:a@example.test",
	} {
		for _, reading := range []icalReading{readSubmitted, readTransferred} {
			if _, ok := readICalProperty(line, reading); ok {
				t.Errorf("%v %q was read", reading, line)
			}
		}
	}
}

// A parameter the server reads to place, type or classify a value is refused
// from a client when it is repeated with a different value, since the readers
// would be free to disagree on which one counts. A stored line is read with the
// first value, as every reader of it takes it.
func TestConflictingRepeatedParametersAreRefusedOnlyFromClients(t *testing.T) {
	tests := []struct {
		line     string
		conflict bool
		first    map[string]string
	}{
		{"DTSTART;TZID=Europe/Berlin;TZID=America/New_York:20260105T090000", true, map[string]string{"TZID": "Europe/Berlin"}},
		{"DTSTART;VALUE=DATE;VALUE=DATE-TIME:20260105", true, map[string]string{"VALUE": "DATE"}},
		{"RECURRENCE-ID;RANGE=THISANDFUTURE;RANGE=THISANDPRIOR:20260105T090000Z", true, map[string]string{"RANGE": "THISANDFUTURE"}},
		{"TRIGGER;RELATED=START;RELATED=END:-PT5M", true, map[string]string{"RELATED": "START"}},
		{"FREEBUSY;FBTYPE=BUSY;FBTYPE=FREE:20260105T090000Z/PT1H", true, map[string]string{"FBTYPE": "BUSY"}},
		{"DTSTART;TZID=\"Europe/Berlin\";TZID=Europe/Berlin:20260105T090000", false, map[string]string{"TZID": "Europe/Berlin"}},
		{"DTSTART;VALUE=DATE-TIME;VALUE=date-time:20260105T090000Z", false, map[string]string{"VALUE": "DATE-TIME"}},
		{"ATTENDEE;PARTSTAT=ACCEPTED;PARTSTAT=DECLINED:mailto:a@example.test", false, map[string]string{"PARTSTAT": "ACCEPTED"}},
	}
	for _, tt := range tests {
		for _, reading := range []icalReading{readSubmitted, readTransferred} {
			if _, ok := readICalProperty(tt.line, reading); ok == tt.conflict {
				t.Errorf("%v %q read = %v, want %v", reading, tt.line, ok, !tt.conflict)
			}
		}
		property, ok := readICalProperty(tt.line, readStored)
		if !ok || !reflect.DeepEqual(property.parameters, tt.first) {
			t.Errorf("stored %q = %v, %v; want %v", tt.line, property.parameters, ok, tt.first)
		}
	}
}

// With no VCALENDAR to read at all there is nothing to match against. A
// calendar-query keeps such a resource rather than tell a sync client it is
// gone: a resource reported that the filter would not select costs the client a
// fetch, while one left out costs it the event.
func TestCalendarQueryKeepsAStoredObjectWithNoCalendarToRead(t *testing.T) {
	filter := calQueryWithTimeRange("20260112T000000Z", "20260113T000000Z")
	for _, raw := range []string{"", "ICAL", "BEGIN:VEVENT\r\nUID:bare\r\nEND:VEVENT\r\n"} {
		if _, err := parseStoredICalendarObject(raw); err == nil {
			t.Fatalf("%q: read as a calendar", raw)
		}
		event := store.Event{UID: "unreadable", RawICAL: raw}
		if !filterMatches(event, filter, floatingZone{}) {
			t.Errorf("%q: left out by matchEventFilter", raw)
		}
		kept, err := applyCalendarFilter(t.Context(), []store.Event{event}, filter, floatingZone{})
		if err != nil {
			t.Fatalf("%q: %v", raw, err)
		}
		if len(kept) != 1 {
			t.Errorf("%q: left out by applyCalendarFilter", raw)
		}
	}
}
