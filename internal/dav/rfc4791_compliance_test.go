package dav

import (
	"context"
	"encoding/xml"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"slices"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/jw6ventures/calcard/internal/auth"
	"github.com/jw6ventures/calcard/internal/config"
	"github.com/jw6ventures/calcard/internal/ical"
	"github.com/jw6ventures/calcard/internal/store"
	"github.com/jw6ventures/calcard/internal/util"
)

// Shared calendar object fixtures.
//
// RFC 4791 §5.3.2.1 requires data submitted by PUT to be valid for its media
// type, so a PUT this suite expects to succeed cannot send an object RFC 5545
// rejects: the strict validator answers such a fixture before it reaches the
// behavior the test names, and the test stops measuring what it says it
// measures. A
// precondition test stays valid in every respect but the one condition it
// exercises, which is what makes that condition the reason for the failure.
const (
	// testProdID is the PRODID that RFC 5545 §3.6 requires of every iCalendar
	// object, alongside VERSION.
	testProdID = "-//CalCard//Strict Suite//EN"
	// testDTStamp and testDTStart are the DATE-TIME values a fixture carries
	// when the test does not care what they are. Both sit inside the
	// min-date-time and max-date-time window caldav_limits.go advertises.
	testDTStamp = "20240601T000000Z"
	testDTStart = "20240601T100000Z"
)

// buildCalendarObject wraps body — the components of the object, preceded by
// any object-level property lines the caller adds — in the VCALENDAR frame of
// RFC 5545 §3.6: VERSION and PRODID exactly once each, around at least one
// component.
func buildCalendarObject(body ...string) string {
	return "BEGIN:VCALENDAR\r\nVERSION:2.0\r\nPRODID:" + testProdID + "\r\n" +
		strings.Join(body, "") + "END:VCALENDAR\r\n"
}

// buildComponent builds one calendar component from its property lines, for the
// cases that need a component missing a property RFC 5545 makes required.
func buildComponent(name string, lines ...string) string {
	var b strings.Builder
	b.WriteString("BEGIN:" + name + "\r\n")
	for _, line := range lines {
		b.WriteString(line)
		b.WriteString("\r\n")
	}
	b.WriteString("END:" + name + "\r\n")
	return b.String()
}

// buildVEvent returns a VEVENT carrying the properties RFC 5545 §3.6.1 makes
// required — UID and DTSTAMP always, and DTSTART for an object specifying no
// METHOD — followed by the caller's own lines.
func buildVEvent(uid string, lines ...string) string {
	return buildComponent("VEVENT", withRequiredProperties(lines,
		"UID:"+uid, "DTSTAMP:"+testDTStamp, "DTSTART:"+testDTStart)...)
}

// buildVTodo returns a VTODO carrying the UID and DTSTAMP that RFC 5545 §3.6.2
// makes required. A VTODO requires no DTSTART.
func buildVTodo(uid string, lines ...string) string {
	return buildComponent("VTODO", withRequiredProperties(lines,
		"UID:"+uid, "DTSTAMP:"+testDTStamp)...)
}

// buildVJournal returns a VJOURNAL carrying the UID and DTSTAMP that RFC 5545
// §3.6.3 makes required.
func buildVJournal(uid string, lines ...string) string {
	return buildComponent("VJOURNAL", withRequiredProperties(lines,
		"UID:"+uid, "DTSTAMP:"+testDTStamp)...)
}

// buildVFreeBusy returns a VFREEBUSY carrying the UID, DTSTAMP, DTSTART and
// DTEND that RFC 5545 §3.6.4 admits; UID and DTSTAMP are the required pair.
func buildVFreeBusy(uid string, lines ...string) string {
	return buildComponent("VFREEBUSY", withRequiredProperties(lines,
		"UID:"+uid, "DTSTAMP:"+testDTStamp, "DTSTART:"+testDTStart, "DTEND:20240601T120000Z")...)
}

// withRequiredProperties prefixes lines with each required property the caller
// did not supply itself. A caller that controls one of them — a date-limit test
// choosing its own DTSTART — replaces the default rather than joining it, since
// RFC 5545 admits only one DTSTAMP, UID or DTSTART per component.
func withRequiredProperties(lines []string, required ...string) []string {
	out := make([]string, 0, len(required)+len(lines))
	for _, property := range required {
		name, _, _ := strings.Cut(property, ":")
		if !carriesProperty(lines, name) {
			out = append(out, property)
		}
	}
	return append(out, lines...)
}

// carriesProperty reports whether lines already spell the named property, in
// either the bare "NAME:" form or the parameterized "NAME;" one.
func carriesProperty(lines []string, name string) bool {
	for _, line := range lines {
		if strings.HasPrefix(line, name+":") || strings.HasPrefix(line, name+";") {
			return true
		}
	}
	return false
}

func TestRFC4791_OptionsAdvertisesCalendarAccess(t *testing.T) {
	// §5.1 scopes the requirement to "any resource" that supports a calendar
	// property, report, method or privilege, so the calendar home, a calendar
	// collection and a calendar object resource all qualify -- the object
	// resource because DAV:supported-report-set advertises the calendaring
	// reports on it.
	paths := []string{"/dav/calendars/", "/dav/calendars/1/", "/dav/calendars/1/event.ics"}
	for _, path := range paths {
		t.Run(path, func(t *testing.T) {
			h := NewDavServer(Options{Config: &config.Config{}, Store: &store.Store{}})
			req := httptest.NewRequest(http.MethodOptions, path, nil)
			rr := httptest.NewRecorder()

			h.Options(rr, req)

			// §5.1: the DAV header MUST carry "calendar-access" as a compliance class.
			assertHeaderToken(t, rr, "DAV", "calendar-access",
				"RFC 4791 §5.1 requires it on any resource supporting a calendar property, report, method or privilege")
			// §5.1: REPORT is required, and §5.3.1 makes MKCALENDAR a SHOULD.
			assertHeaderToken(t, rr, "Allow", "REPORT", "RFC 4791 §5.1 requires the calendaring reports")
			assertHeaderToken(t, rr, "Allow", "MKCALENDAR", "RFC 4791 §5.3.1 says a server SHOULD support MKCALENDAR")
		})
	}
}

// Section 5.2: Calendar Collection Properties
func TestRFC4791_CalendarCollectionMustHaveResourceType(t *testing.T) {
	now := store.Now()
	calRepo := &fakeCalendarRepo{
		accessible: []store.CalendarAccess{
			{Calendar: store.Calendar{ID: 1, UserID: 1, Name: "Test", UpdatedAt: now}, Editor: true},
		},
	}
	h := &DavServer{store: &store.Store{Calendars: calRepo, Events: &fakeEventRepo{}}}
	user := &store.User{ID: 1, PrimaryEmail: "test@example.com"}

	req := httptest.NewRequest("PROPFIND", "/dav/calendars/1/", nil)
	req.Header.Set("Depth", "0")
	req = req.WithContext(auth.WithUser(req.Context(), user))
	rr := httptest.NewRecorder()

	h.Propfind(rr, req)

	resp := decodeMultistatus(t, rr).responseForHref(t, "/dav/calendars/1/")
	resp.assertPropChildNames(t, davQN("resourcetype"), davQN("collection"), calQN("calendar"))
	calendar := resp.assertPropStatus(t, davQN("resourcetype"), http.StatusOK).child(t, calQN("calendar"))
	text, err := scalarText(calendar)
	if err != nil {
		t.Fatal(err)
	}
	if strings.TrimSpace(text) != "" {
		t.Errorf("CALDAV:calendar value = %q, want an empty element", text)
	}
}

// Section 5.2.3: CalCard defines supported-calendar-component-set, so its
// advertised value must accurately describe the component types it accepts.
// Defining the property is a MAY; this test does not make it mandatory.
func TestRFC4791_SupportedCalendarComponentSetAdvertisedValue(t *testing.T) {
	now := store.Now()
	calRepo := &fakeCalendarRepo{
		accessible: []store.CalendarAccess{
			{Calendar: store.Calendar{ID: 1, UserID: 1, Name: "Test", UpdatedAt: now}, Editor: true},
		},
	}
	h := &DavServer{store: &store.Store{Calendars: calRepo, Events: &fakeEventRepo{}}}
	user := &store.User{ID: 1}

	// §5.2.3 keeps the property out of DAV:allprop, so the value is observed
	// through a named request.
	body := `<?xml version="1.0" encoding="utf-8"?>
<d:propfind xmlns:d="DAV:" xmlns:cal="urn:ietf:params:xml:ns:caldav">
  <d:prop><cal:supported-calendar-component-set/></d:prop>
</d:propfind>`
	req := httptest.NewRequest("PROPFIND", "/dav/calendars/1/", strings.NewReader(body))
	req.Header.Set("Depth", "0")
	req = req.WithContext(auth.WithUser(req.Context(), user))
	rr := httptest.NewRecorder()

	h.Propfind(rr, req)

	decodeMultistatus(t, rr).
		responseForHref(t, "/dav/calendars/1/").
		assertSupportedComponents(t, "VEVENT", "VTODO", "VJOURNAL", "VFREEBUSY")
}

// Section 5.2.4: supported-calendar-data Property
func TestRFC4791_CalendarDataContentTypeTextCalendar(t *testing.T) {
	calRepo := &fakeCalendarRepo{
		accessible: []store.CalendarAccess{
			{Calendar: store.Calendar{ID: 1, UserID: 1, Name: "Test"}, Editor: true},
		},
	}
	eventRepo := &fakeEventRepo{
		events: map[string]*store.Event{
			"1:event": {CalendarID: 1, UID: "event", RawICAL: "BEGIN:VCALENDAR\r\nEND:VCALENDAR\r\n", ETag: "e"},
		},
	}
	h := &DavServer{store: &store.Store{Calendars: calRepo, Events: eventRepo}}
	user := &store.User{ID: 1}

	req := httptest.NewRequest(http.MethodGet, "/dav/calendars/1/event.ics", nil)
	req = req.WithContext(auth.WithUser(req.Context(), user))
	rr := httptest.NewRecorder()

	h.Get(rr, req)

	// §2 and §5.2.4: iCalendar is the calendar object resource media type. The
	// parameters are the server's business — RFC 5545 §3.1 admits charset and
	// component on text/calendar — so only the media type itself is asserted.
	assertMediaType(t, rr, "text/calendar")
}

// Section 5.3.1: MKCALENDAR creates a new calendar collection resource, which
// §2 makes a SHOULD-level requirement.
//
// The requirement is the postcondition, not the status. §5.3.1.1 gives
// "examples of response codes... by no means exhaustive", so nothing in
// RFC 4791 makes 201 mandatory; assertMKCalendarCreated pins the 201 CalCard
// returns as policy and asserts the §5.3.1 rule that a success body, when
// present, is a CALDAV:mkcalendar-response.
func TestRFC4791_MkcalendarCreatesCalendarCollection(t *testing.T) {
	calRepo := &fakeCalendarRepo{calendars: make(map[int64]*store.Calendar)}
	h := &DavServer{store: &store.Store{Calendars: calRepo}}
	user := &store.User{ID: 1}

	req := httptest.NewRequest("MKCALENDAR", "/dav/calendars/newcal", nil)
	req = req.WithContext(auth.WithUser(req.Context(), user))
	rr := httptest.NewRecorder()

	h.Mkcalendar(rr, req)

	assertMKCalendarCreated(t, rr)

	if len(calRepo.calendars) != 1 {
		t.Fatalf("MKCALENDAR reported success but the store holds %d calendars, want 1", len(calRepo.calendars))
	}
	for _, created := range calRepo.calendars {
		if created.Name != "newcal" {
			t.Errorf("created calendar name = %q, want the last path segment %q", created.Name, "newcal")
		}
	}
}

func TestRFC4791_MKCALENDAR_ResourcetypeIsCollectionAndCalendar(t *testing.T) {
	calRepo := &fakeCalendarRepo{calendars: make(map[int64]*store.Calendar)}
	h := &DavServer{store: &store.Store{Calendars: calRepo}}
	user := &store.User{ID: 1}

	req := httptest.NewRequest("MKCALENDAR", "/dav/calendars/newcal", nil)
	req = req.WithContext(auth.WithUser(req.Context(), user))
	rr := httptest.NewRecorder()

	h.Mkcalendar(rr, req)

	assertMKCalendarCreated(t, rr)

	propBody := `<?xml version="1.0" encoding="utf-8"?>
<d:propfind xmlns:d="DAV:" xmlns:cal="urn:ietf:params:xml:ns:caldav">
  <d:prop>
    <d:resourcetype/>
  </d:prop>
</d:propfind>`
	req = httptest.NewRequest("PROPFIND", "/dav/calendars/newcal/", strings.NewReader(propBody))
	req.Header.Set("Depth", "0")
	req = req.WithContext(auth.WithUser(req.Context(), user))
	rr = httptest.NewRecorder()

	h.Propfind(rr, req)

	// PROPFIND answers on the canonical by-ID href rather than echoing the
	// by-name Request-URI, which is legal: both URIs map to the one collection.
	ms := decodeMultistatus(t, rr)
	ms.assertHrefs(t, "/dav/calendars/1/")
	resp := ms.responseForHref(t, "/dav/calendars/1/")
	resp.assertPropChildNames(t, davQN("resourcetype"), davQN("collection"), calQN("calendar"))
}

// Section 5.3.1: properties carried by a MKCALENDAR body are applied to the new
// collection. The failure half of §5.3.1 — a 207 with a per-property propstat
// and 424 dependencies — is not implemented, so this test covers only the
// success path it exercises.
func TestRFC4791_MKCALENDAR_BodySetsProperties(t *testing.T) {
	calRepo := &fakeCalendarRepo{calendars: make(map[int64]*store.Calendar)}
	h := &DavServer{store: &store.Store{Calendars: calRepo}}
	user := &store.User{ID: 1}

	// §5.3.1.1 (CALDAV:valid-calendar-data): the timezone carried by a
	// MKCALENDAR body must be a valid iCalendar object wrapping exactly one
	// VTIMEZONE, so the fixture is VCALENDAR-wrapped rather than a bare
	// component. The STANDARD DTSTART is local time with no TZID and no "Z":
	// RFC 5545 §3.8.2.4 gives that as the only form admitted inside a
	// VTIMEZONE sub-component, so the UTC spelling would make the fixture
	// invalid and a strict server right to reject the request this test
	// requires to succeed.
	timezone := "BEGIN:VCALENDAR\nVERSION:2.0\nPRODID:-//CalCard//Strict Suite//EN\nBEGIN:VTIMEZONE\nTZID:UTC\nBEGIN:STANDARD\nDTSTART:19700101T000000\nTZOFFSETFROM:+0000\nTZOFFSETTO:+0000\nTZNAME:UTC\nEND:STANDARD\nEND:VTIMEZONE\nEND:VCALENDAR"
	body := `<?xml version="1.0" encoding="utf-8"?>
<cal:mkcalendar xmlns:d="DAV:" xmlns:cal="urn:ietf:params:xml:ns:caldav">
  <d:set>
    <d:prop>
      <d:displayname>Strict Suite Calendar</d:displayname>
      <cal:calendar-description>Strict suite test</cal:calendar-description>
      <cal:calendar-timezone>` + timezone + `</cal:calendar-timezone>
    </d:prop>
  </d:set>
</cal:mkcalendar>`

	req := httptest.NewRequest("MKCALENDAR", "/dav/calendars/propscal", strings.NewReader(body))
	req = req.WithContext(auth.WithUser(req.Context(), user))
	rr := httptest.NewRecorder()

	h.Mkcalendar(rr, req)

	assertMKCalendarCreated(t, rr)

	propBody := `<?xml version="1.0" encoding="utf-8"?>
<d:propfind xmlns:d="DAV:" xmlns:cal="urn:ietf:params:xml:ns:caldav">
  <d:prop>
    <d:displayname/>
    <cal:calendar-description/>
    <cal:calendar-timezone/>
  </d:prop>
</d:propfind>`
	req = httptest.NewRequest("PROPFIND", "/dav/calendars/propscal/", strings.NewReader(propBody))
	req.Header.Set("Depth", "0")
	req = req.WithContext(auth.WithUser(req.Context(), user))
	rr = httptest.NewRecorder()

	h.Propfind(rr, req)

	ms := decodeMultistatus(t, rr)
	resp := ms.responseForHref(t, "/dav/calendars/1/")
	resp.assertPropValue(t, davQN("displayname"), http.StatusOK, "Strict Suite Calendar")
	resp.assertPropValue(t, calQN("calendar-description"), http.StatusOK, "Strict suite test")
	resp.assertPropValue(t, calQN("calendar-timezone"), http.StatusOK, timezone)
}

// Section 5.3.2: a PUT to an unmapped URI inside a calendar collection creates
// a new calendar object resource. Conditional-request handling is RFC 9110
// §13.1 and lives in http_semantics_test.go; this is the unconditional case
// §5.3.2 itself describes.
func TestRFC4791_PutCreatesCalendarObjectAtUnmappedURI(t *testing.T) {
	calRepo := &fakeCalendarRepo{
		accessible: []store.CalendarAccess{
			{Calendar: store.Calendar{ID: 1, UserID: 1, Name: "Test"}, Editor: true},
		},
	}
	eventRepo := &fakeEventRepo{events: make(map[string]*store.Event)}
	h := &DavServer{store: &store.Store{Calendars: calRepo, Events: eventRepo}}
	user := &store.User{ID: 1}

	icalData := buildCalendarObject(buildVEvent("new-event"))
	req := newCalendarPutRequest("/dav/calendars/1/new-event.ics", strings.NewReader(icalData))
	req = req.WithContext(auth.WithUser(req.Context(), user))
	rr := httptest.NewRecorder()

	h.Put(rr, req)

	if rr.Code != http.StatusCreated {
		t.Fatalf("PUT to an unmapped URI = %d, want 201 Created (RFC 4791 §5.3.2); body: %s", rr.Code, rr.Body.String())
	}
	stored, ok := eventRepo.events["1:new-event"]
	if !ok {
		t.Fatalf("PUT reported 201 but stored no resource; the repository holds %d events", len(eventRepo.events))
	}
	if stored.RawICAL != icalData {
		t.Errorf("stored octets = %q, want the submitted %q", stored.RawICAL, icalData)
	}
}

// Section 5.3.4: a server SHOULD return a strong ETag from a PUT whose
// submitted octets it stores unchanged.
//
// The requirement is conditional, so the condition is asserted rather than
// assumed: the PUT succeeded, and what the repository now holds is octet for
// octet what was sent. Without those, a response that failed and happened to
// carry a quoted ETag satisfies the header checks. Strength is asserted too —
// §5.3.4 asks for a strong entity tag, and RFC 9110 §8.8.3 makes "W/" the
// spelling of a weak one.
func TestRFC4791_PutReturnsETagHeader(t *testing.T) {
	calRepo := &fakeCalendarRepo{
		accessible: []store.CalendarAccess{
			{Calendar: store.Calendar{ID: 1, UserID: 1, Name: "Test"}, Editor: true},
		},
	}
	eventRepo := &fakeEventRepo{events: make(map[string]*store.Event)}
	h := &DavServer{store: &store.Store{Calendars: calRepo, Events: eventRepo}}
	user := &store.User{ID: 1}

	icalData := buildCalendarObject(buildVEvent("test"))
	req := newCalendarPutRequest("/dav/calendars/1/test.ics", strings.NewReader(icalData))
	req = req.WithContext(auth.WithUser(req.Context(), user))
	rr := httptest.NewRecorder()

	h.Put(rr, req)

	if rr.Code != http.StatusCreated {
		t.Fatalf("PUT to an unmapped URI = %d, want 201 Created; body: %s", rr.Code, rr.Body.String())
	}
	stored, ok := eventRepo.events["1:test"]
	if !ok {
		t.Fatalf("PUT reported 201 but stored no resource; the repository holds %d events", len(eventRepo.events))
	}
	if stored.RawICAL != icalData {
		t.Fatalf("stored octets = %q, want the submitted %q; §5.3.4 conditions its ETag on the two being identical",
			stored.RawICAL, icalData)
	}

	assertStrongETag(t, rr.Header().Get("ETag"),
		"RFC 4791 §5.3.4 says a server SHOULD return a strong ETag from a PUT that stores the submitted octets unchanged")
}

// Section 6.2.1: calendar-home-set Property
func TestRFC4791_PrincipalHasCalendarHomeSet(t *testing.T) {
	h := &DavServer{}
	user := &store.User{ID: 1, PrimaryEmail: "user@example.com"}

	body := `<?xml version="1.0" encoding="utf-8"?>
<d:propfind xmlns:d="DAV:" xmlns:cal="urn:ietf:params:xml:ns:caldav">
  <d:prop>
    <cal:calendar-home-set/>
  </d:prop>
</d:propfind>`

	req := httptest.NewRequest("PROPFIND", "/dav/principals/1/", strings.NewReader(body))
	req.Header.Set("Depth", "0")
	req = req.WithContext(auth.WithUser(req.Context(), user))
	rr := httptest.NewRecorder()

	h.Propfind(rr, req)

	homeSet := decodeMultistatus(t, rr).
		responseForHref(t, "/dav/principals/1/").
		assertPropStatus(t, calQN("calendar-home-set"), http.StatusOK)

	assertSoleHref(t, homeSet, "/dav/calendars/")
}

// RFC 4791 Section 6.2.1: calendar-home-set is protected and SHOULD NOT appear in allprop
func TestRFC4791_PrincipalCalendarHomeSetNotInAllprop(t *testing.T) {
	h := &DavServer{}
	user := &store.User{ID: 1, PrimaryEmail: "user@example.com"}

	req := httptest.NewRequest("PROPFIND", "/dav/principals/1/", nil)
	req.Header.Set("Depth", "0")
	req = req.WithContext(auth.WithUser(req.Context(), user))
	rr := httptest.NewRecorder()

	h.Propfind(rr, req)

	decodeMultistatus(t, rr).
		responseForHref(t, "/dav/principals/1/").
		assertPropAbsent(t, calQN("calendar-home-set"))
}

// Section 6.2.1 applies to every principal response, including one reached as
// a member of the DAV root. Depth must not change DAV:allprop's property set.
func TestRFC4791_RootAllpropOmitsPrincipalCalendarHomeSet(t *testing.T) {
	h := &DavServer{}
	user := &store.User{ID: 1, PrimaryEmail: "user@example.com"}

	req := httptest.NewRequest("PROPFIND", "/dav/", nil)
	req.Header.Set("Depth", "1")
	req = req.WithContext(auth.WithUser(req.Context(), user))
	rr := httptest.NewRecorder()

	h.Propfind(rr, req)

	decodeMultistatus(t, rr).
		responseForHref(t, "/dav/principals/1/").
		assertPropAbsent(t, calQN("calendar-home-set"))
}

// CRITICAL: Calendar Discovery - RFC 4791 Section 6.2.1
// This is the primary way CalDAV clients discover available calendars
func TestRFC4791_CalendarHomeListsCalendars(t *testing.T) {
	now := store.Now()
	calRepo := &fakeCalendarRepo{
		accessible: []store.CalendarAccess{
			{Calendar: store.Calendar{ID: 1, UserID: 1, Name: "Work", UpdatedAt: now, CTag: 10}, Editor: true},
			{Calendar: store.Calendar{ID: 2, UserID: 1, Name: "Personal", UpdatedAt: now, CTag: 20}, Editor: true},
		},
	}
	h := &DavServer{store: &store.Store{Calendars: calRepo, Events: &fakeEventRepo{}}}
	user := &store.User{ID: 1, PrimaryEmail: "user@example.com"}

	// RFC 4791 Section 6.2.1: PROPFIND on calendar-home-set with Depth: 1 lists calendars
	req := httptest.NewRequest("PROPFIND", "/dav/calendars/", nil)
	req.Header.Set("Depth", "1")
	req = req.WithContext(auth.WithUser(req.Context(), user))
	rr := httptest.NewRecorder()

	h.Propfind(rr, req)

	// The birthday calendar is a always-present synthetic collection, so the
	// href set is the home, both real calendars, and it.
	ms := decodeMultistatus(t, rr)
	ms.assertHrefs(t, "/dav/calendars/", "/dav/calendars/-1/", "/dav/calendars/1/", "/dav/calendars/2/")

	for _, href := range []string{"/dav/calendars/1/", "/dav/calendars/2/"} {
		resp := ms.responseForHref(t, href)
		resp.assertPropChildNames(t, davQN("resourcetype"), davQN("collection"), calQN("calendar"))
	}
	ms.responseForHref(t, "/dav/calendars/1/").assertPropValue(t, davQN("displayname"), http.StatusOK, "Work")
	ms.responseForHref(t, "/dav/calendars/2/").assertPropValue(t, davQN("displayname"), http.StatusOK, "Personal")

	// The home itself is an ordinary collection, not a calendar collection.
	ms.responseForHref(t, "/dav/calendars/").assertPropChildNames(t, davQN("resourcetype"), davQN("collection"))
}

// Test that empty calendar-home-set returns no calendars (not an error)
func TestRFC4791_EmptyCalendarHomeReturnsNoCalendars(t *testing.T) {
	calRepo := &fakeCalendarRepo{
		accessible: []store.CalendarAccess{}, // No calendars
	}
	h := &DavServer{store: &store.Store{Calendars: calRepo, Events: &fakeEventRepo{}}}
	user := &store.User{ID: 1, PrimaryEmail: "user@example.com"}

	req := httptest.NewRequest("PROPFIND", "/dav/calendars/", nil)
	req.Header.Set("Depth", "1")
	req = req.WithContext(auth.WithUser(req.Context(), user))
	rr := httptest.NewRecorder()

	h.Propfind(rr, req)

	// A user with no calendars still sees the home plus the synthetic birthday
	// calendar, and nothing else.
	decodeMultistatus(t, rr).assertHrefs(t, "/dav/calendars/", "/dav/calendars/-1/")
}

// Test Depth: 0 on calendar home (should only return the home collection, not the calendars)
func TestRFC4791_CalendarHomeDepthZero(t *testing.T) {
	now := store.Now()
	calRepo := &fakeCalendarRepo{
		accessible: []store.CalendarAccess{
			{Calendar: store.Calendar{ID: 1, UserID: 1, Name: "Work", UpdatedAt: now}, Editor: true},
			{Calendar: store.Calendar{ID: 2, UserID: 1, Name: "Personal", UpdatedAt: now}, Editor: true},
		},
	}
	h := &DavServer{store: &store.Store{Calendars: calRepo, Events: &fakeEventRepo{}}}
	user := &store.User{ID: 1}

	req := httptest.NewRequest("PROPFIND", "/dav/calendars/", nil)
	req.Header.Set("Depth", "0")
	req = req.WithContext(auth.WithUser(req.Context(), user))
	rr := httptest.NewRecorder()

	h.Propfind(rr, req)

	decodeMultistatus(t, rr).assertHrefs(t, "/dav/calendars/")
}

// RFC 4791 Section 6.2.1: calendar-home-set discovery. The preceding
// DAV:current-user-principal step is RFC 5397 and lives in rfc5397_principal_test.go.
func TestRFC4791_CalendarDiscoverySequence(t *testing.T) {
	now := store.Now()
	calRepo := &fakeCalendarRepo{
		accessible: []store.CalendarAccess{
			{Calendar: store.Calendar{ID: 1, UserID: 1, Name: "Work", UpdatedAt: now, CTag: 10}, Editor: true},
			{Calendar: store.Calendar{ID: 2, UserID: 1, Name: "Home", UpdatedAt: now, CTag: 20}, Editor: true},
		},
	}
	h := &DavServer{store: &store.Store{Calendars: calRepo, Events: &fakeEventRepo{}}}
	user := &store.User{ID: 1, PrimaryEmail: "user@example.com"}

	// Step 2: PROPFIND on principal to get calendar-home-set (RFC 4791 Section 6.2.1)
	t.Run("Step2_DiscoverCalendarHome", func(t *testing.T) {
		body := `<?xml version="1.0" encoding="utf-8"?>
<d:propfind xmlns:d="DAV:" xmlns:cal="urn:ietf:params:xml:ns:caldav">
  <d:prop>
    <cal:calendar-home-set/>
  </d:prop>
</d:propfind>`

		req := httptest.NewRequest("PROPFIND", "/dav/principals/1/", strings.NewReader(body))
		req.Header.Set("Depth", "0")
		req = req.WithContext(auth.WithUser(req.Context(), user))
		rr := httptest.NewRecorder()

		h.Propfind(rr, req)

		homeSet := decodeMultistatus(t, rr).
			responseForHref(t, "/dav/principals/1/").
			assertPropStatus(t, calQN("calendar-home-set"), http.StatusOK)
		assertSoleHref(t, homeSet, "/dav/calendars/")
	})

	// Step 3: PROPFIND on calendar home with Depth: 1 to list calendars (RFC 4918)
	t.Run("Step3_ListCalendars", func(t *testing.T) {
		body := `<?xml version="1.0" encoding="utf-8"?>
<d:propfind xmlns:d="DAV:" xmlns:cal="urn:ietf:params:xml:ns:caldav" xmlns:cs="http://calendarserver.org/ns/">
  <d:prop>
    <d:displayname/>
    <d:resourcetype/>
    <cs:getctag/>
    <cal:supported-calendar-component-set/>
  </d:prop>
</d:propfind>`

		req := httptest.NewRequest("PROPFIND", "/dav/calendars/", strings.NewReader(body))
		req.Header.Set("Depth", "1")
		req = req.WithContext(auth.WithUser(req.Context(), user))
		rr := httptest.NewRecorder()

		h.Propfind(rr, req)

		ms := decodeMultistatus(t, rr)
		ms.assertHrefs(t, "/dav/calendars/", "/dav/calendars/-1/", "/dav/calendars/1/", "/dav/calendars/2/")

		for href, name := range map[string]string{"/dav/calendars/1/": "Work", "/dav/calendars/2/": "Home"} {
			resp := ms.responseForHref(t, href)
			resp.assertPropValue(t, davQN("displayname"), http.StatusOK, name)
			resp.assertPropChildNames(t, davQN("resourcetype"), davQN("collection"), calQN("calendar"))
		}
	})

	// Step 4: Verify individual calendar collection can be accessed
	t.Run("Step4_AccessIndividualCalendar", func(t *testing.T) {
		req := httptest.NewRequest("PROPFIND", "/dav/calendars/1/", nil)
		req.Header.Set("Depth", "0")
		req = req.WithContext(auth.WithUser(req.Context(), user))
		rr := httptest.NewRecorder()

		h.Propfind(rr, req)

		decodeMultistatus(t, rr).assertHrefs(t, "/dav/calendars/1/")
	})

}

// Section 7.8: calendar-query REPORT
func TestRFC4791_CalendarQueryReportBasic(t *testing.T) {
	calRepo := &fakeCalendarRepo{
		accessible: []store.CalendarAccess{
			{Calendar: store.Calendar{ID: 1, UserID: 1, Name: "Test"}, Editor: true},
		},
	}
	eventRepo := &fakeEventRepo{
		events: map[string]*store.Event{
			"1:event": {CalendarID: 1, UID: "event", RawICAL: "BEGIN:VCALENDAR\r\nBEGIN:VEVENT\r\nUID:event\r\nEND:VEVENT\r\nEND:VCALENDAR\r\n", ETag: "e"},
		},
	}
	h := &DavServer{store: &store.Store{Calendars: calRepo, Events: eventRepo}}
	user := &store.User{ID: 1}

	// RFC 4791 Section 7.8: calendar-query with filter
	body := `<?xml version="1.0" encoding="utf-8" ?>
<C:calendar-query xmlns:D="DAV:" xmlns:C="urn:ietf:params:xml:ns:caldav">
  <D:prop>
    <D:getetag/>
    <C:calendar-data/>
  </D:prop>
  <C:filter>
    <C:comp-filter name="VCALENDAR">
      <C:comp-filter name="VEVENT"/>
    </C:comp-filter>
  </C:filter>
</C:calendar-query>`

	req := httptest.NewRequest("REPORT", "/dav/calendars/1/", strings.NewReader(body))
	req.Header.Set("Depth", "1")
	req = req.WithContext(auth.WithUser(req.Context(), user))
	rr := httptest.NewRecorder()

	h.Report(rr, req)

	ms := decodeMultistatus(t, rr)
	ms.assertHrefs(t, "/dav/calendars/1/event.ics")
	resp := ms.responseForHref(t, "/dav/calendars/1/event.ics")
	resp.assertPropValue(t, davQN("getetag"), http.StatusOK, `"e"`)
	// encoding/xml normalizes CRLF to LF in character data, so the comparison is
	// against the LF form of the stored octets.
	resp.assertPropValue(t, calQN("calendar-data"), http.StatusOK,
		"BEGIN:VCALENDAR\nBEGIN:VEVENT\nUID:event\nEND:VEVENT\nEND:VCALENDAR")
}

// Sections 7.4 and 9.9: time-range filtering.
func TestRFC4791_TimeRangeFilteringAccuracy(t *testing.T) {
	// Test with specific time boundaries
	tests := []struct {
		name        string
		eventStart  time.Time
		eventEnd    time.Time
		rangeStart  string
		rangeEnd    string
		shouldMatch bool
	}{
		{
			name:        "event within range",
			eventStart:  time.Date(2024, 6, 15, 10, 0, 0, 0, time.UTC),
			eventEnd:    time.Date(2024, 6, 15, 11, 0, 0, 0, time.UTC),
			rangeStart:  "20240601T000000Z",
			rangeEnd:    "20240630T235959Z",
			shouldMatch: true,
		},
		{
			name:        "event starts before range ends in range",
			eventStart:  time.Date(2024, 5, 31, 23, 0, 0, 0, time.UTC),
			eventEnd:    time.Date(2024, 6, 1, 1, 0, 0, 0, time.UTC),
			rangeStart:  "20240601T000000Z",
			rangeEnd:    "20240630T235959Z",
			shouldMatch: true,
		},
		{
			name:        "event completely before range",
			eventStart:  time.Date(2024, 5, 1, 10, 0, 0, 0, time.UTC),
			eventEnd:    time.Date(2024, 5, 1, 11, 0, 0, 0, time.UTC),
			rangeStart:  "20240601T000000Z",
			rangeEnd:    "20240630T235959Z",
			shouldMatch: false,
		},
		{
			name:        "event completely after range",
			eventStart:  time.Date(2024, 7, 1, 10, 0, 0, 0, time.UTC),
			eventEnd:    time.Date(2024, 7, 1, 11, 0, 0, 0, time.UTC),
			rangeStart:  "20240601T000000Z",
			rangeEnd:    "20240630T235959Z",
			shouldMatch: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			calRepo := &fakeCalendarRepo{
				accessible: []store.CalendarAccess{
					{Calendar: store.Calendar{ID: 1, UserID: 1, Name: "Test"}, Editor: true},
				},
			}
			// The time-range test reads the component the filter names, so the
			// stored octets carry the schedule rather than the denormalized
			// dtstart/dtend columns the query plan narrows on.
			raw := fmt.Sprintf("BEGIN:VCALENDAR\r\nBEGIN:VEVENT\r\nUID:test\r\nDTSTART:%s\r\nDTEND:%s\r\nEND:VEVENT\r\nEND:VCALENDAR\r\n",
				tt.eventStart.Format("20060102T150405Z"), tt.eventEnd.Format("20060102T150405Z"))
			eventRepo := &fakeEventRepo{
				events: map[string]*store.Event{
					"1:test": {
						CalendarID: 1,
						UID:        "test",
						RawICAL:    raw,
						ETag:       "e",
						DTStart:    &tt.eventStart,
						DTEnd:      &tt.eventEnd,
					},
				},
			}
			h := &DavServer{store: &store.Store{Calendars: calRepo, Events: eventRepo}}
			user := &store.User{ID: 1}

			body := fmt.Sprintf(`<C:calendar-query xmlns:C="urn:ietf:params:xml:ns:caldav">
  <C:filter>
    <C:comp-filter name="VCALENDAR">
      <C:comp-filter name="VEVENT">
        <C:time-range start="%s" end="%s"/>
      </C:comp-filter>
    </C:comp-filter>
  </C:filter>
</C:calendar-query>`, tt.rangeStart, tt.rangeEnd)

			req := httptest.NewRequest("REPORT", "/dav/calendars/1/", strings.NewReader(body))
			req.Header.Set("Depth", "1")
			req = req.WithContext(auth.WithUser(req.Context(), user))
			rr := httptest.NewRecorder()

			h.Report(rr, req)

			ms := decodeMultistatus(t, rr)
			if tt.shouldMatch {
				ms.assertHrefs(t, "/dav/calendars/1/test.ics")
			} else {
				ms.assertHrefs(t)
			}
		})
	}
}

// Section 7.9: calendar-multiget REPORT
func TestRFC4791_CalendarMultigetReport(t *testing.T) {
	calRepo := &fakeCalendarRepo{
		accessible: []store.CalendarAccess{
			{Calendar: store.Calendar{ID: 1, UserID: 1, Name: "Test"}, Editor: true},
		},
	}
	eventRepo := &fakeEventRepo{
		events: map[string]*store.Event{
			"1:event1": {CalendarID: 1, UID: "event1", RawICAL: "BEGIN:VCALENDAR\r\nBEGIN:VEVENT\r\nUID:event1\r\nEND:VEVENT\r\nEND:VCALENDAR\r\n", ETag: "e1"},
			"1:event2": {CalendarID: 1, UID: "event2", RawICAL: "BEGIN:VCALENDAR\r\nBEGIN:VEVENT\r\nUID:event2\r\nEND:VEVENT\r\nEND:VCALENDAR\r\n", ETag: "e2"},
		},
	}
	h := &DavServer{store: &store.Store{Calendars: calRepo, Events: eventRepo}}
	user := &store.User{ID: 1}

	// RFC 4791 Section 7.9: calendar-multiget with specific hrefs
	body := `<?xml version="1.0" encoding="utf-8" ?>
<C:calendar-multiget xmlns:D="DAV:" xmlns:C="urn:ietf:params:xml:ns:caldav">
  <D:prop>
    <D:getetag/>
    <C:calendar-data/>
  </D:prop>
  <D:href>/dav/calendars/1/event1.ics</D:href>
  <D:href>/dav/calendars/1/event2.ics</D:href>
  <D:href>/dav/calendars/1/missing.ics</D:href>
</C:calendar-multiget>`

	req := httptest.NewRequest("REPORT", "/dav/calendars/1/", strings.NewReader(body))
	req = req.WithContext(auth.WithUser(req.Context(), user))
	rr := httptest.NewRecorder()

	h.Report(rr, req)

	// §7.9: one DAV:response per requested href, including hrefs that resolve to
	// nothing, which carry a bare 404 status rather than a propstat.
	ms := decodeMultistatus(t, rr)
	ms.assertHrefs(t,
		"/dav/calendars/1/event1.ics",
		"/dav/calendars/1/event2.ics",
		"/dav/calendars/1/missing.ics",
	)
	ms.responseForHref(t, "/dav/calendars/1/event1.ics").assertPropValue(t, davQN("getetag"), http.StatusOK, `"e1"`)
	ms.responseForHref(t, "/dav/calendars/1/event2.ics").assertPropValue(t, davQN("getetag"), http.StatusOK, `"e2"`)

	missing := ms.responseForHref(t, "/dav/calendars/1/missing.ics")
	if len(missing.Propstats) != 0 {
		t.Errorf("missing href carries %d propstats, want a bare DAV:status", len(missing.Propstats))
	}
	if got := statusCodeFromLine(t, missing.Status); got != http.StatusNotFound {
		t.Errorf("missing href status = %d, want 404 Not Found", got)
	}
}

// Section 7.2: support for the calendaring reports on ordinary collections is a
// MAY. CalCard declines it, which is compliant, so this pins the decline rather
// than asserting a requirement. RFC 3253 §3.6 fixes the shape of that decline:
// 403 naming DAV:supported-report under a top-level DAV:error. Calendar object
// resources are covered by the separate mandatory-target tests below.
func TestRFC4791_CalendarReports_OnOrdinaryCollection_AreDeclined(t *testing.T) {
	calRepo := &fakeCalendarRepo{
		accessible: []store.CalendarAccess{
			{Calendar: store.Calendar{ID: 1, UserID: 1, Name: "Test"}, Editor: true},
		},
	}
	eventRepo := &fakeEventRepo{
		events: map[string]*store.Event{
			"1:event": {CalendarID: 1, UID: "event", RawICAL: "BEGIN:VCALENDAR\r\nBEGIN:VEVENT\r\nUID:event\r\nEND:VEVENT\r\nEND:VCALENDAR\r\n", ETag: "e"},
		},
	}
	h := &DavServer{store: &store.Store{Calendars: calRepo, Events: eventRepo}}
	user := &store.User{ID: 1}

	tests := []struct {
		name string
		body string
	}{
		{
			name: "calendar-query",
			body: `<?xml version="1.0" encoding="utf-8" ?>
<C:calendar-query xmlns:D="DAV:" xmlns:C="urn:ietf:params:xml:ns:caldav">
  <D:prop>
    <C:calendar-data/>
  </D:prop>
  <C:filter>
    <C:comp-filter name="VCALENDAR">
      <C:comp-filter name="VEVENT"/>
    </C:comp-filter>
  </C:filter>
</C:calendar-query>`,
		},
		{
			name: "calendar-multiget",
			body: `<?xml version="1.0" encoding="utf-8" ?>
<C:calendar-multiget xmlns:D="DAV:" xmlns:C="urn:ietf:params:xml:ns:caldav">
  <D:prop>
    <C:calendar-data/>
  </D:prop>
  <D:href>/dav/calendars/1/event.ics</D:href>
</C:calendar-multiget>`,
		},
	}

	// The DAV root and the two home collections are all ordinary collections.
	collections := []string{"/dav/", "/dav/calendars/", "/dav/addressbooks/"}

	for _, tt := range tests {
		for _, collection := range collections {
			t.Run(tt.name+" on "+collection, func(t *testing.T) {
				req := httptest.NewRequest("REPORT", collection, strings.NewReader(tt.body))
				req = req.WithContext(auth.WithUser(req.Context(), user))
				rr := httptest.NewRecorder()

				h.Report(rr, req)

				assertErrorConditions(t, rr, http.StatusForbidden, davQN("supported-report"))
			})
		}
	}
}

// Section 7.10: free-busy-query REPORT
func TestRFC4791_FreeBusyQueryReport(t *testing.T) {
	start := time.Date(2024, 6, 1, 10, 0, 0, 0, time.UTC)
	end := time.Date(2024, 6, 1, 12, 0, 0, 0, time.UTC)

	calRepo := &fakeCalendarRepo{
		accessible: []store.CalendarAccess{
			{Calendar: store.Calendar{ID: 1, UserID: 1, Name: "Test"}, Editor: true},
		},
	}
	eventRepo := &fakeEventRepo{
		events: map[string]*store.Event{
			"1:event": {
				CalendarID: 1,
				UID:        "event",
				RawICAL:    "BEGIN:VCALENDAR\r\nBEGIN:VEVENT\r\nUID:event\r\nDTSTART:20240601T100000Z\r\nDTEND:20240601T120000Z\r\nEND:VEVENT\r\nEND:VCALENDAR\r\n",
				ETag:       "e",
				DTStart:    &start,
				DTEnd:      &end,
			},
		},
	}
	h := &DavServer{store: &store.Store{Calendars: calRepo, Events: eventRepo}}
	user := &store.User{ID: 1}

	// RFC 4791 Section 7.10: free-busy-query REPORT
	body := `<?xml version="1.0" encoding="utf-8" ?>
<C:free-busy-query xmlns:C="urn:ietf:params:xml:ns:caldav">
  <C:time-range start="20240601T000000Z" end="20240630T235959Z"/>
</C:free-busy-query>`

	req := httptest.NewRequest("REPORT", "/dav/calendars/1/", strings.NewReader(body))
	req.Header.Set("Depth", "1")
	req = req.WithContext(auth.WithUser(req.Context(), user))
	rr := httptest.NewRecorder()

	h.Report(rr, req)

	if rr.Code != http.StatusOK {
		t.Errorf("RFC 4791 Section 7.10: free-busy-query must return 200 OK, got %d", rr.Code)
	}
	assertMediaType(t, rr, "text/calendar")

	// §7.10 makes the body an iCalendar object with exactly one VFREEBUSY, so
	// it is parsed and its component asserted rather than searched for text.
	root, err := parseICalendarObject(rr.Body.String())
	if err != nil {
		t.Fatalf("RFC 4791 Section 7.10: response is not a valid iCalendar object: %v (%s)", err, rr.Body.String())
	}
	if got := root.childCount("VFREEBUSY"); got != 1 {
		t.Fatalf("RFC 4791 Section 7.10: response carries %d VFREEBUSY components, want exactly 1", got)
	}
	freeBusy, _ := namedComponent(root, "VFREEBUSY")

	// RFC 5545 §3.6.4 makes UID and DTSTAMP both REQUIRED in a VFREEBUSY.
	for _, required := range []string{"UID", "DTSTAMP"} {
		if freeBusy.count(required) != 1 {
			t.Errorf("RFC 5545 Section 3.6.4: VFREEBUSY must carry exactly one %s, got %d", required, freeBusy.count(required))
		}
	}
	if freeBusy.count("FREEBUSY") == 0 {
		t.Error("RFC 4791 Section 7.10: Response must include FREEBUSY properties")
	}
	if got := freeBusy.value("DTSTART"); got != "20240601T000000Z" {
		t.Errorf("VFREEBUSY DTSTART = %q, want the requested range start", got)
	}
	if got := freeBusy.value("DTEND"); got != "20240630T235959Z" {
		t.Errorf("VFREEBUSY DTEND = %q, want the requested range end", got)
	}
	assertPublishedFreeBusy(t, parsedFreeBusyLines(t, rr.Body.String()), []string{
		"FREEBUSY:20240601T100000Z/20240601T120000Z",
	})
}

// Two free-busy reports answered inside the same second still name two
// different VFREEBUSY components, so a client caching by UID cannot collapse
// them.
func TestRFC4791_FreeBusyResponseUIDIsUnique(t *testing.T) {
	h := &DavServer{}
	tr := &timeRange{Start: "20240601T000000Z", End: "20240630T235959Z"}

	uidOf := func(body string) string {
		t.Helper()
		root, err := parseICalendarObject(body)
		if err != nil {
			t.Fatalf("free-busy response is not a valid iCalendar object: %v", err)
		}
		freeBusy, _ := namedComponent(root, "VFREEBUSY")
		if freeBusy == nil {
			t.Fatal("free-busy response carries no VFREEBUSY")
		}
		return freeBusy.value("UID")
	}

	first := uidOf(h.generateFreeBusy(nil, tr))
	second := uidOf(h.generateFreeBusy(nil, tr))
	if first == "" {
		t.Fatal("VFREEBUSY carries no UID")
	}
	if first == second {
		t.Fatalf("two free-busy responses share the UID %q", first)
	}
}

func TestFreeBusyResponseUIDFallbackIsUnique(t *testing.T) {
	fixed := time.Date(2024, 6, 1, 10, 0, 0, 123, time.UTC)
	first := freeBusyUIDSuffixFrom(strings.NewReader(""), fixed)
	second := freeBusyUIDSuffixFrom(strings.NewReader(""), fixed)
	if first == second {
		t.Fatalf("two entropy failures at the same instant produced %q", first)
	}
}

// Section 7.10: free-busy-query with no matching events returns empty VFREEBUSY
func TestRFC4791_FreeBusyQueryNoMatches(t *testing.T) {
	calRepo := &fakeCalendarRepo{
		accessible: []store.CalendarAccess{
			{Calendar: store.Calendar{ID: 1, UserID: 1, Name: "Test"}, Editor: true},
		},
	}
	eventRepo := &fakeEventRepo{events: make(map[string]*store.Event)}
	h := &DavServer{store: &store.Store{Calendars: calRepo, Events: eventRepo}}
	user := &store.User{ID: 1}

	body := `<?xml version="1.0" encoding="utf-8" ?>
<C:free-busy-query xmlns:C="urn:ietf:params:xml:ns:caldav">
  <C:time-range start="20240601T000000Z" end="20240630T235959Z"/>
</C:free-busy-query>`

	req := httptest.NewRequest("REPORT", "/dav/calendars/1/", strings.NewReader(body))
	req.Header.Set("Depth", "1")
	req = req.WithContext(auth.WithUser(req.Context(), user))
	rr := httptest.NewRecorder()

	h.Report(rr, req)

	if rr.Code != http.StatusOK {
		t.Fatalf("RFC 4791 Section 7.10: free-busy-query must return 200 OK, got %d", rr.Code)
	}
	if periods := parsedFreeBusyLines(t, rr.Body.String()); len(periods) != 0 {
		t.Errorf("RFC 4791 Section 7.10: empty result published periods %v", periods)
	}
}

// Section 7.8.7 (CALDAV:supported-collation): a calendar-query naming a
// collation the server does not implement is refused, rather than answered by
// matching under a different one. §1.3 makes it a 403: resubmitting the same
// request cannot make an unimplemented collation work.
func TestRFC4791_CalendarQueryRejectsUnsupportedCollation(t *testing.T) {
	body := func(collation string) string {
		return `<?xml version="1.0" encoding="utf-8" ?>
<C:calendar-query xmlns:D="DAV:" xmlns:C="urn:ietf:params:xml:ns:caldav">
  <D:prop><D:getetag/></D:prop>
  <C:filter>
    <C:comp-filter name="VCALENDAR">
      <C:comp-filter name="VEVENT">
        <C:prop-filter name="SUMMARY">
          <C:text-match collation="` + collation + `">standup</C:text-match>
        </C:prop-filter>
      </C:comp-filter>
    </C:comp-filter>
  </C:filter>
</C:calendar-query>`
	}

	newServer := func() *DavServer {
		return &DavServer{store: &store.Store{
			Calendars: &fakeCalendarRepo{accessible: []store.CalendarAccess{
				{Calendar: store.Calendar{ID: 1, UserID: 1, Name: "Test"}, Editor: true},
			}},
			Events: &fakeEventRepo{events: map[string]*store.Event{
				"1:standup": {CalendarID: 1, UID: "standup", ResourceName: "standup", ETag: "etag",
					RawICAL: "BEGIN:VCALENDAR\r\nBEGIN:VEVENT\r\nUID:standup\r\nSUMMARY:Standup\r\nEND:VEVENT\r\nEND:VCALENDAR\r\n"},
			}},
		}}
	}
	user := &store.User{ID: 1}

	// A wildcard is refused rather than expanded, which §7.5 states as its own
	// MUST NOT alongside the unsupported-identifier rule.
	for _, collation := range []string{"i;unicode-casemap", "i;made-up", "i;ascii-*", "*", " i;octet "} {
		t.Run(collation, func(t *testing.T) {
			req := httptest.NewRequest("REPORT", "/dav/calendars/1/", strings.NewReader(body(collation)))
			req.Header.Set("Depth", "1")
			req = req.WithContext(auth.WithUser(req.Context(), user))
			rr := httptest.NewRecorder()

			newServer().Report(rr, req)

			assertErrorConditions(t, rr, http.StatusForbidden, calQN("supported-collation"))
		})
	}

	// The advertised collation is still served, so the refusal is scoped to what
	// CALDAV:supported-collation-set does not list.
	for _, collation := range []string{"i;ascii-casemap", "default"} {
		t.Run("supported/"+collation, func(t *testing.T) {
			req := httptest.NewRequest("REPORT", "/dav/calendars/1/", strings.NewReader(body(collation)))
			req.Header.Set("Depth", "1")
			req = req.WithContext(auth.WithUser(req.Context(), user))
			rr := httptest.NewRecorder()

			newServer().Report(rr, req)

			if rr.Code != http.StatusMultiStatus {
				t.Fatalf("calendar-query with collation %q = %d, want 207; body: %s", collation, rr.Code, rr.Body.String())
			}
		})
	}
}

// Section 7: calendar-query and calendar-multiget run against the resource the
// Request-URI names. A URI naming no resource has nothing to report on, so it
// is a request-level 404 -- an empty multistatus would tell the client the
// resource exists and matched nothing.
func TestRFC4791_ReportOnMissingCalendarObjectIsNotFound(t *testing.T) {
	bodies := map[string]string{
		"calendar-query": `<?xml version="1.0" encoding="utf-8" ?>
<C:calendar-query xmlns:D="DAV:" xmlns:C="urn:ietf:params:xml:ns:caldav">
  <D:prop><D:getetag/></D:prop>
  <C:filter><C:comp-filter name="VCALENDAR"><C:comp-filter name="VEVENT"/></C:comp-filter></C:filter>
</C:calendar-query>`,
		"calendar-multiget": `<?xml version="1.0" encoding="utf-8" ?>
<C:calendar-multiget xmlns:D="DAV:" xmlns:C="urn:ietf:params:xml:ns:caldav">
  <D:prop><D:getetag/></D:prop>
  <D:href>/dav/calendars/1/missing.ics</D:href>
</C:calendar-multiget>`,
	}

	t.Run("ordinary calendar", func(t *testing.T) {
		for name, body := range bodies {
			t.Run(name, func(t *testing.T) {
				calRepo := &fakeCalendarRepo{accessible: []store.CalendarAccess{
					{Calendar: store.Calendar{ID: 1, UserID: 1, Name: "Test"}, Editor: true},
				}}
				// The collection holds a sibling, so only the named resource is
				// missing -- the calendar itself resolves.
				eventRepo := &fakeEventRepo{events: map[string]*store.Event{
					"1:present": {CalendarID: 1, UID: "present", ResourceName: "present", ETag: "etag",
						RawICAL: "BEGIN:VCALENDAR\r\nBEGIN:VEVENT\r\nUID:present\r\nEND:VEVENT\r\nEND:VCALENDAR\r\n"},
				}}
				h := &DavServer{store: &store.Store{Calendars: calRepo, Events: eventRepo}}

				req := httptest.NewRequest("REPORT", "/dav/calendars/1/missing.ics", strings.NewReader(body))
				req = req.WithContext(auth.WithUser(req.Context(), &store.User{ID: 1}))
				rr := httptest.NewRecorder()

				h.Report(rr, req)

				if rr.Code != http.StatusNotFound {
					t.Errorf("%s on a missing object = %d, want 404; body: %s", name, rr.Code, rr.Body.String())
				}
			})
		}
	})

	t.Run("virtual birthday calendar", func(t *testing.T) {
		birthday := time.Date(1990, 5, 15, 0, 0, 0, 0, time.UTC)
		displayName := "Alice Example"
		for name, body := range bodies {
			t.Run(name, func(t *testing.T) {
				h := &DavServer{store: &store.Store{
					Calendars: &fakeCalendarRepo{},
					Events:    &fakeEventRepo{},
					Contacts: &fakeContactRepo{contacts: map[string]*store.Contact{
						"5:alice": {AddressBookID: 5, UID: "alice", DisplayName: &displayName, Birthday: &birthday},
					}},
				}}

				target := fmt.Sprintf("/dav/calendars/%d/missing.ics", birthdayCalendarID)
				if name == "calendar-multiget" {
					body = strings.Replace(body, "/dav/calendars/1/missing.ics", target, 1)
				}
				req := httptest.NewRequest("REPORT", target, strings.NewReader(body))
				req = req.WithContext(auth.WithUser(req.Context(), &store.User{ID: 1}))
				rr := httptest.NewRecorder()

				h.Report(rr, req)

				if rr.Code != http.StatusNotFound {
					t.Errorf("%s on a missing birthday object = %d, want 404; body: %s", name, rr.Code, rr.Body.String())
				}
			})
		}
	})

	// An existing object still answers with a multistatus, so the 404 is scoped
	// to a Request-URI that names nothing.
	t.Run("existing object still reports", func(t *testing.T) {
		calRepo := &fakeCalendarRepo{accessible: []store.CalendarAccess{
			{Calendar: store.Calendar{ID: 1, UserID: 1, Name: "Test"}, Editor: true},
		}}
		eventRepo := &fakeEventRepo{events: map[string]*store.Event{
			"1:present": {CalendarID: 1, UID: "present", ResourceName: "present", ETag: "etag",
				RawICAL: "BEGIN:VCALENDAR\r\nBEGIN:VEVENT\r\nUID:present\r\nEND:VEVENT\r\nEND:VCALENDAR\r\n"},
		}}
		h := &DavServer{store: &store.Store{Calendars: calRepo, Events: eventRepo}}

		req := httptest.NewRequest("REPORT", "/dav/calendars/1/present.ics", strings.NewReader(bodies["calendar-query"]))
		req = req.WithContext(auth.WithUser(req.Context(), &store.User{ID: 1}))
		rr := httptest.NewRecorder()

		h.Report(rr, req)

		decodeMultistatus(t, rr).assertHrefs(t, "/dav/calendars/1/present.ics")
	})
}

// Section 7.10: free-busy-query on calendar object resource must be forbidden
func TestRFC4791_FreeBusyQueryOnCalendarObjectForbidden(t *testing.T) {
	calRepo := &fakeCalendarRepo{
		accessible: []store.CalendarAccess{
			{Calendar: store.Calendar{ID: 1, UserID: 1, Name: "Test"}, Editor: true},
		},
	}
	eventRepo := &fakeEventRepo{events: make(map[string]*store.Event)}
	h := &DavServer{store: &store.Store{Calendars: calRepo, Events: eventRepo}}
	user := &store.User{ID: 1}

	body := `<?xml version="1.0" encoding="utf-8" ?>
<C:free-busy-query xmlns:C="urn:ietf:params:xml:ns:caldav">
  <C:time-range start="20240601T000000Z" end="20240630T235959Z"/>
</C:free-busy-query>`

	req := httptest.NewRequest("REPORT", "/dav/calendars/1/event.ics", strings.NewReader(body))
	req = req.WithContext(auth.WithUser(req.Context(), user))
	rr := httptest.NewRecorder()

	h.Report(rr, req)

	if rr.Code != http.StatusForbidden {
		t.Errorf("RFC 4791 Section 7.10: free-busy-query on calendar object must return 403, got %d", rr.Code)
	}
}

// Section 7.10: free-busy-query without privileges must return 404
func TestRFC4791_FreeBusyQueryUnauthorizedReturnsNotFound(t *testing.T) {
	calRepo := &fakeCalendarRepo{accessible: []store.CalendarAccess{}}
	h := &DavServer{store: &store.Store{Calendars: calRepo, Events: &fakeEventRepo{}}}
	user := &store.User{ID: 1}

	body := `<?xml version="1.0" encoding="utf-8" ?>
<C:free-busy-query xmlns:C="urn:ietf:params:xml:ns:caldav">
  <C:time-range start="20240601T000000Z" end="20240630T235959Z"/>
</C:free-busy-query>`

	req := httptest.NewRequest("REPORT", "/dav/calendars/1/", strings.NewReader(body))
	req = req.WithContext(auth.WithUser(req.Context(), user))
	rr := httptest.NewRecorder()

	h.Report(rr, req)

	if rr.Code != http.StatusNotFound {
		t.Errorf("RFC 4791 Section 7.10: free-busy-query without access must return 404, got %d", rr.Code)
	}
}

// Section 9: XML Namespace Compliance
func TestRFC4791_XMLNamespacesCorrect(t *testing.T) {
	calRepo := &fakeCalendarRepo{
		accessible: []store.CalendarAccess{
			{Calendar: store.Calendar{ID: 1, UserID: 1, Name: "Test", UpdatedAt: store.Now()}, Editor: true},
		},
	}
	h := &DavServer{store: &store.Store{Calendars: calRepo, Events: &fakeEventRepo{}}}
	user := &store.User{ID: 1}

	body := `<?xml version="1.0" encoding="utf-8"?>
<d:propfind xmlns:d="DAV:" xmlns:cal="urn:ietf:params:xml:ns:caldav">
  <d:prop><cal:supported-calendar-component-set/><d:resourcetype/></d:prop>
</d:propfind>`
	req := httptest.NewRequest("PROPFIND", "/dav/calendars/1/", strings.NewReader(body))
	req.Header.Set("Depth", "0")
	req = req.WithContext(auth.WithUser(req.Context(), user))
	rr := httptest.NewRecorder()

	h.Propfind(rr, req)

	// decodeMultistatus resolves every prefix against its declared namespace URI
	// and fails the test on an undeclared one, so properties answering to their
	// DAV: and CalDAV QNames prove both namespaces were declared correctly.
	resp := decodeMultistatus(t, rr).responseForHref(t, "/dav/calendars/1/")
	resp.assertPropStatus(t, calQN("supported-calendar-component-set"), http.StatusOK)
	resp.assertPropStatus(t, davQN("resourcetype"), http.StatusOK)
}

func TestRFC4791_ReportCalendarData_ReturnsValidICalendar(t *testing.T) {
	calRepo := &fakeCalendarRepo{
		accessible: []store.CalendarAccess{
			{Calendar: store.Calendar{ID: 1, UserID: 1, Name: "Test"}, Editor: true},
		},
	}
	eventRepo := &fakeEventRepo{
		events: map[string]*store.Event{
			"1:event": {
				CalendarID: 1,
				UID:        "event",
				RawICAL:    "BEGIN:VCALENDAR\r\nVERSION:2.0\r\nBEGIN:VEVENT\r\nUID:event\r\nSUMMARY:Test\r\nEND:VEVENT\r\nEND:VCALENDAR\r\n",
				ETag:       "e",
			},
		},
	}
	h := &DavServer{store: &store.Store{Calendars: calRepo, Events: eventRepo}}
	user := &store.User{ID: 1}

	body := `<?xml version="1.0" encoding="utf-8" ?>
<C:calendar-multiget xmlns:D="DAV:" xmlns:C="urn:ietf:params:xml:ns:caldav">
  <D:prop>
    <C:calendar-data/>
  </D:prop>
  <D:href>/dav/calendars/1/event.ics</D:href>
</C:calendar-multiget>`

	req := httptest.NewRequest("REPORT", "/dav/calendars/1/", strings.NewReader(body))
	req = req.WithContext(auth.WithUser(req.Context(), user))
	rr := httptest.NewRecorder()

	h.Report(rr, req)

	// The payload must be the value of CALDAV:calendar-data in the requested
	// resource's own 200 propstat. Searching the whole multistatus for the
	// iCalendar text would also pass on data carried by another resource, by a
	// different property, or by a DAV:responsedescription.
	data := decodeMultistatus(t, rr).
		responseForHref(t, "/dav/calendars/1/event.ics").
		assertPropStatus(t, calQN("calendar-data"), http.StatusOK).
		Value()

	if !strings.HasPrefix(data, "BEGIN:VCALENDAR") || !strings.HasSuffix(strings.TrimSpace(data), "END:VCALENDAR") {
		t.Errorf("calendar-data must be a complete VCALENDAR object, got %q", data)
	}
	if !strings.Contains(data, "UID:event") {
		t.Errorf("calendar-data must include the stored component data, got %q", data)
	}
}

func TestRFC4791_CalendarQuery_CalendarDataComponentFiltering_Works(t *testing.T) {
	start := time.Date(2024, 6, 1, 9, 0, 0, 0, time.UTC)
	end := time.Date(2024, 6, 1, 10, 0, 0, 0, time.UTC)
	calRepo := &fakeCalendarRepo{
		accessible: []store.CalendarAccess{
			{Calendar: store.Calendar{ID: 1, UserID: 1, Name: "Test"}, Editor: true},
		},
	}
	eventRepo := &fakeEventRepo{
		events: map[string]*store.Event{
			"1:event": {
				CalendarID: 1,
				UID:        "event",
				RawICAL:    "BEGIN:VCALENDAR\r\nVERSION:2.0\r\nBEGIN:VEVENT\r\nUID:event\r\nDTSTART:20240601T090000Z\r\nDTEND:20240601T100000Z\r\nSUMMARY:Filtered\r\nDESCRIPTION:Should be removed\r\nEND:VEVENT\r\nEND:VCALENDAR\r\n",
				ETag:       "e",
				DTStart:    &start,
				DTEnd:      &end,
			},
		},
	}
	h := &DavServer{store: &store.Store{Calendars: calRepo, Events: eventRepo}}
	user := &store.User{ID: 1}

	body := `<?xml version="1.0" encoding="utf-8"?>
<C:calendar-query xmlns:C="urn:ietf:params:xml:ns:caldav" xmlns:D="DAV:">
  <D:prop>
    <C:calendar-data>
      <C:comp name="VCALENDAR">
        <C:comp name="VEVENT">
          <C:prop name="DTSTART"/>
          <C:prop name="DTEND"/>
          <C:prop name="UID"/>
        </C:comp>
      </C:comp>
    </C:calendar-data>
  </D:prop>
  <C:filter>
    <C:comp-filter name="VCALENDAR">
      <C:comp-filter name="VEVENT"/>
    </C:comp-filter>
  </C:filter>
</C:calendar-query>`

	req := httptest.NewRequest("REPORT", "/dav/calendars/1/", strings.NewReader(body))
	req.Header.Set("Depth", "1")
	req = req.WithContext(auth.WithUser(req.Context(), user))
	rr := httptest.NewRecorder()

	h.Report(rr, req)

	// Both halves are asserted against the value of CALDAV:calendar-data in the
	// matching resource's 200 propstat. The negative half is the reason this
	// matters: SUMMARY and DESCRIPTION absent from the whole body proves nothing
	// about whether they were projected out of this property in particular.
	data := decodeMultistatus(t, rr).
		responseForHref(t, "/dav/calendars/1/event.ics").
		assertPropStatus(t, calQN("calendar-data"), http.StatusOK).
		Value()

	for _, want := range []string{"DTSTART", "DTEND", "UID:event"} {
		if !strings.Contains(data, want) {
			t.Errorf("calendar-data must include the requested property %s, got %q", want, data)
		}
	}
	for _, unwanted := range []string{"SUMMARY:", "DESCRIPTION:"} {
		if strings.Contains(data, unwanted) {
			t.Errorf("calendar-data must omit the unrequested property %s, got %q", unwanted, data)
		}
	}
}

// Section 5.3.2.1: submitted data must be valid for its media type. Each case
// carries exactly one structural defect and is otherwise a well-formed
// iCalendar object, so the rejection can only be the defect it names.
func TestRFC4791_RejectMalformedICalendar(t *testing.T) {
	calRepo := &fakeCalendarRepo{
		accessible: []store.CalendarAccess{
			{Calendar: store.Calendar{ID: 1, UserID: 1, Name: "Test"}, Editor: true},
		},
	}
	h := &DavServer{store: &store.Store{Calendars: calRepo, Events: &fakeEventRepo{}}}
	user := &store.User{ID: 1}

	tests := []struct {
		name        string
		data        string
		description string
	}{
		{
			name:        "missing VCALENDAR wrapper",
			data:        buildVEvent("test"),
			description: "calendar data must be wrapped in VCALENDAR",
		},
		{
			name:        "unbalanced BEGIN/END",
			data:        "BEGIN:VCALENDAR\r\nVERSION:2.0\r\nPRODID:" + testProdID + "\r\nBEGIN:VEVENT\r\nUID:test\r\nEND:VCALENDAR\r\n",
			description: "BEGIN must match END tags",
		},
		{
			name:        "no calendar components",
			data:        buildCalendarObject(),
			description: "calendar must contain at least one component",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req := newCalendarPutRequest("/dav/calendars/1/test.ics", strings.NewReader(tt.data))
			req = req.WithContext(auth.WithUser(req.Context(), user))
			rr := httptest.NewRecorder()

			h.Put(rr, req)

			// §1.3: resubmitting the same malformed body can only fail again,
			// which is the 403 half of the precondition status rule.
			assertErrorConditions(t, rr, http.StatusForbidden, calQN("valid-calendar-data"))
		})
	}
}

// Section 7: a calendar collection advertises every report it supports in
// DAV:supported-report-set (RFC 3253 §3.1.5).
//
// RFC 4791 requires only the three calendaring reports. DAV:sync-collection
// (RFC 6578 §3.2) and DAV:expand-property (RFC 3253 §3.8) appear in the
// expectation because the assertion is over the exact set: the property names
// every report the server supports, so an advertisement CalCard makes cannot be
// left out of the expectation without weakening it into a containment check.
// Those two reports' own behavior belongs to their own suites.
func TestRFC4791_SupportedReportSetProperty(t *testing.T) {
	now := store.Now()
	calRepo := &fakeCalendarRepo{
		accessible: []store.CalendarAccess{
			{Calendar: store.Calendar{ID: 1, UserID: 1, Name: "Test", UpdatedAt: now}, Editor: true},
		},
	}
	h := &DavServer{store: &store.Store{Calendars: calRepo, Events: &fakeEventRepo{}}}
	user := &store.User{ID: 1}

	req := httptest.NewRequest("PROPFIND", "/dav/calendars/1/", nil)
	req.Header.Set("Depth", "0")
	req = req.WithContext(auth.WithUser(req.Context(), user))
	rr := httptest.NewRecorder()

	h.Propfind(rr, req)

	ms := decodeMultistatus(t, rr)
	resp := ms.responseForHref(t, "/dav/calendars/1/")
	resp.assertSupportedReports(t,
		calQN("calendar-query"),
		calQN("calendar-multiget"),
		calQN("free-busy-query"),
		davQN("sync-collection"),
		davQN("expand-property"),
		davQN("acl-principal-prop-set"),
		davQN("principal-match"),
		davQN("principal-property-search"),
	)
}

// Sections 4.2 and 5.3.1.1: a calendar collection may not contain another one,
// so the Request-URI of a MKCALENDAR inside one identifies no location where a
// calendar collection can be created.
//
// The parent is a real, accessible calendar collection, which is what makes the
// request the nesting attempt the test names: against an empty repository the
// same 403 is equally consistent with a server that rejects an unknown parent
// and happily nests under a known one. The postcondition is asserted too — a
// rejected MKCALENDAR creates nothing — since a status alone does not say the
// collection was left uncreated.
func TestRFC4791_NoNestedCalendarCollections(t *testing.T) {
	for _, requestPath := range []string{"/dav/calendars/1/nested/", "/dav/calendars/1/ordinary/nested/"} {
		t.Run(requestPath, func(t *testing.T) {
			parent := &store.Calendar{ID: 1, UserID: 1, Name: "Test"}
			calRepo := &fakeCalendarRepo{
				calendars:  map[int64]*store.Calendar{1: parent},
				accessible: []store.CalendarAccess{{Calendar: *parent, Editor: true}},
			}
			h := &DavServer{store: &store.Store{Calendars: calRepo, Events: &fakeEventRepo{}}}
			user := &store.User{ID: 1}

			req := httptest.NewRequest("MKCALENDAR", requestPath, nil)
			req = req.WithContext(auth.WithUser(req.Context(), user))
			rr := httptest.NewRecorder()

			h.Mkcalendar(rr, req)

			if rr.Code != http.StatusForbidden {
				t.Errorf("MKCALENDAR inside a calendar collection = %d, want 403 Forbidden; body: %s", rr.Code, rr.Body.String())
			}
			if len(calRepo.calendars) != 1 || calRepo.calendars[1] != parent {
				t.Errorf("a rejected MKCALENDAR changed the collection set: %v", calRepo.calendars)
			}
			if len(calRepo.accessible) != 1 {
				t.Errorf("a rejected MKCALENDAR granted access to %d collections, want the parent alone", len(calRepo.accessible))
			}
		})
	}
}

// Test Content-Type validation for PUT
func TestRFC4791_PutContentTypeValidation(t *testing.T) {
	calRepo := &fakeCalendarRepo{
		accessible: []store.CalendarAccess{
			{Calendar: store.Calendar{ID: 1, UserID: 1, Name: "Test"}, Editor: true},
		},
	}
	h := &DavServer{store: &store.Store{Calendars: calRepo, Events: &fakeEventRepo{}}}
	user := &store.User{ID: 1}

	validIcal := buildCalendarObject(buildVEvent("test"))

	// §5.3.2.1 requires a supported calendar media type. Parameters do not
	// change the text/calendar media type.
	req := newCalendarPutRequest("/dav/calendars/1/test.ics", strings.NewReader(validIcal))
	req.Header.Set("Content-Type", "text/calendar; charset=utf-8")
	req = req.WithContext(auth.WithUser(req.Context(), user))
	rr := httptest.NewRecorder()

	h.Put(rr, req)

	if rr.Code != http.StatusCreated {
		t.Errorf("Valid calendar data with proper Content-Type should succeed, got %d", rr.Code)
	}
}

// CalCard advertises VTODO in supported-calendar-component-set, so §5.2.3
// requires it to be usable rather than rejected as an unsupported component.
func TestRFC4791_VTODOComponentSupport(t *testing.T) {
	calRepo := &fakeCalendarRepo{
		accessible: []store.CalendarAccess{
			{Calendar: store.Calendar{ID: 1, UserID: 1, Name: "Test"}, Editor: true},
		},
	}
	eventRepo := &fakeEventRepo{events: make(map[string]*store.Event)}
	h := &DavServer{store: &store.Store{Calendars: calRepo, Events: eventRepo}}
	user := &store.User{ID: 1}

	todoData := buildCalendarObject(buildVTodo("task1", "SUMMARY:Buy milk"))
	req := newCalendarPutRequest("/dav/calendars/1/task1.ics", strings.NewReader(todoData))
	req = req.WithContext(auth.WithUser(req.Context(), user))
	rr := httptest.NewRecorder()

	h.Put(rr, req)

	if rr.Code != http.StatusCreated {
		t.Errorf("PUT of the advertised VTODO component = %d, want 201 Created: %s", rr.Code, rr.Body.String())
	}
}

// CalCard advertises VJOURNAL in supported-calendar-component-set, so §5.2.3
// requires it to be usable rather than rejected as an unsupported component.
func TestRFC4791_VJOURNALComponentSupport(t *testing.T) {
	calRepo := &fakeCalendarRepo{
		accessible: []store.CalendarAccess{
			{Calendar: store.Calendar{ID: 1, UserID: 1, Name: "Test"}, Editor: true},
		},
	}
	eventRepo := &fakeEventRepo{events: make(map[string]*store.Event)}
	h := &DavServer{store: &store.Store{Calendars: calRepo, Events: eventRepo}}
	user := &store.User{ID: 1}

	journalData := buildCalendarObject(buildVJournal("journal1", "SUMMARY:Today's notes"))
	req := newCalendarPutRequest("/dav/calendars/1/journal1.ics", strings.NewReader(journalData))
	req = req.WithContext(auth.WithUser(req.Context(), user))
	rr := httptest.NewRecorder()

	h.Put(rr, req)

	if rr.Code != http.StatusCreated {
		t.Errorf("PUT of the advertised VJOURNAL component = %d, want 201 Created: %s", rr.Code, rr.Body.String())
	}
}

// Section 5.2.1: calendar-description Property
func TestRFC4791_CalendarDescriptionProperty(t *testing.T) {
	now := store.Now()
	calRepo := &fakeCalendarRepo{
		accessible: []store.CalendarAccess{
			{Calendar: store.Calendar{ID: 1, UserID: 1, Name: "My Calendar", Description: util.StrPtr("Personal events"), UpdatedAt: now}, Editor: true},
		},
	}
	h := &DavServer{store: &store.Store{Calendars: calRepo, Events: &fakeEventRepo{}}}
	user := &store.User{ID: 1}

	body := `<?xml version="1.0" encoding="utf-8"?>
<d:propfind xmlns:d="DAV:" xmlns:c="urn:ietf:params:xml:ns:caldav">
  <d:prop>
    <c:calendar-description/>
  </d:prop>
</d:propfind>`

	req := httptest.NewRequest("PROPFIND", "/dav/calendars/1/", strings.NewReader(body))
	req.Header.Set("Depth", "0")
	req = req.WithContext(auth.WithUser(req.Context(), user))
	rr := httptest.NewRecorder()

	h.Propfind(rr, req)

	decodeMultistatus(t, rr).
		responseForHref(t, "/dav/calendars/1/").
		assertPropValue(t, calQN("calendar-description"), http.StatusOK, "Personal events")
}

// §5.2.2 and §5.3.1.1 both require CALDAV:calendar-timezone to hold a valid
// iCalendar object containing one VTIMEZONE, so the served value carries a
// VCALENDAR envelope rather than a bare component. §5.2.2 also makes the property
// a SHOULD on every calendar collection, which the default value satisfies for a
// collection that has never had one set.
func TestRFC4791_CalendarCollection_CalendarTimezoneIsWrappedICalendarObject(t *testing.T) {
	now := store.Now()
	// What CalCard wrote before §5.2.2's envelope was required: a bare component,
	// complete in itself, that only lacks the VCALENDAR around it.
	stored := "BEGIN:VTIMEZONE\nTZID:America/Chicago\n" +
		"BEGIN:STANDARD\nDTSTART:19701101T020000\nTZOFFSETFROM:-0500\nTZOFFSETTO:-0600\nEND:STANDARD\n" +
		"END:VTIMEZONE"
	// The old validator also accepted incomplete components. Those values cannot
	// be made valid merely by adding a VCALENDAR envelope.
	invalidStored := "BEGIN:VTIMEZONE\nTZID:America/Chicago\nEND:VTIMEZONE"
	calRepo := &fakeCalendarRepo{
		accessible: []store.CalendarAccess{
			{Calendar: store.Calendar{ID: 1, UserID: 1, Name: "Test", UpdatedAt: now}, Editor: true},
			{Calendar: store.Calendar{ID: 2, UserID: 1, Name: "Legacy", Timezone: &stored, UpdatedAt: now}, Editor: true},
			{Calendar: store.Calendar{ID: 3, UserID: 1, Name: "Invalid Legacy", Timezone: &invalidStored, UpdatedAt: now}, Editor: true},
		},
	}
	h := &DavServer{store: &store.Store{Calendars: calRepo, Events: &fakeEventRepo{}}}
	user := &store.User{ID: 1}

	body := `<?xml version="1.0" encoding="utf-8"?>
<d:propfind xmlns:d="DAV:" xmlns:cal="urn:ietf:params:xml:ns:caldav">
  <d:prop>
    <cal:calendar-timezone/>
  </d:prop>
</d:propfind>`

	// The default value and a stored bare fragment must both come back wrapped:
	// the property's value is an iCalendar object either way.
	for _, href := range []string{"/dav/calendars/1/", "/dav/calendars/2/", "/dav/calendars/3/"} {
		req := httptest.NewRequest("PROPFIND", href, strings.NewReader(body))
		req.Header.Set("Depth", "0")
		req = req.WithContext(auth.WithUser(req.Context(), user))
		rr := httptest.NewRecorder()

		h.Propfind(rr, req)

		timezone := decodeMultistatus(t, rr).
			responseForHref(t, href).
			assertPropStatus(t, calQN("calendar-timezone"), http.StatusOK).
			Value()

		// The value is iCalendar, not XML, so it stays a string comparison.
		if !strings.HasPrefix(timezone, "BEGIN:VCALENDAR") || !strings.HasSuffix(timezone, "END:VCALENDAR") {
			t.Errorf("%s calendar-timezone = %q, want a VCALENDAR-wrapped iCalendar object", href, timezone)
		}
		if !validCalendarTimezone(timezone) {
			t.Errorf("%s calendar-timezone = %q, want a value carrying exactly one VTIMEZONE with a TZID", href, timezone)
		}
		if href == "/dav/calendars/3/" && timezone != defaultCalendarTimezone {
			t.Errorf("%s calendar-timezone = %q, want the valid server default for invalid legacy data", href, timezone)
		}
	}
}

func TestRFC4791_CalendarCollection_CalendarTimezone_NotInAllprop(t *testing.T) {
	now := store.Now()
	calRepo := &fakeCalendarRepo{
		accessible: []store.CalendarAccess{
			{Calendar: store.Calendar{ID: 1, UserID: 1, Name: "Test", UpdatedAt: now}, Editor: true},
		},
	}
	h := &DavServer{store: &store.Store{Calendars: calRepo, Events: &fakeEventRepo{}}}
	user := &store.User{ID: 1}

	req := httptest.NewRequest("PROPFIND", "/dav/calendars/1/", nil)
	req.Header.Set("Depth", "0")
	req = req.WithContext(auth.WithUser(req.Context(), user))
	rr := httptest.NewRecorder()

	h.Propfind(rr, req)

	decodeMultistatus(t, rr).
		responseForHref(t, "/dav/calendars/1/").
		assertPropAbsent(t, calQN("calendar-timezone"))
}

func TestRFC4791_CalendarCollection_Propfind_SupportedCalendarData_Advertised(t *testing.T) {
	now := store.Now()
	calRepo := &fakeCalendarRepo{
		accessible: []store.CalendarAccess{
			{Calendar: store.Calendar{ID: 1, UserID: 1, Name: "Test", UpdatedAt: now}, Editor: true},
		},
	}
	h := &DavServer{store: &store.Store{Calendars: calRepo, Events: &fakeEventRepo{}}}
	user := &store.User{ID: 1}

	body := `<?xml version="1.0" encoding="utf-8"?>
<d:propfind xmlns:d="DAV:" xmlns:cal="urn:ietf:params:xml:ns:caldav">
  <d:prop>
    <cal:supported-calendar-data/>
  </d:prop>
</d:propfind>`

	req := httptest.NewRequest("PROPFIND", "/dav/calendars/1/", strings.NewReader(body))
	req.Header.Set("Depth", "0")
	req = req.WithContext(auth.WithUser(req.Context(), user))
	rr := httptest.NewRecorder()

	h.Propfind(rr, req)

	supported := decodeMultistatus(t, rr).
		responseForHref(t, "/dav/calendars/1/").
		assertPropStatus(t, calQN("supported-calendar-data"), http.StatusOK)

	if got := qnList(supported.childNames()); got != qnList([]xml.Name{calQN("calendar-data")}) {
		t.Fatalf("supported-calendar-data children = %s, want %s", got, qnString(calQN("calendar-data")))
	}
	data := supported.child(t, calQN("calendar-data"))
	text, err := scalarText(data)
	if err != nil {
		t.Fatal(err)
	}
	if strings.TrimSpace(text) != "" {
		t.Errorf("supported-calendar-data calendar-data value = %q, want an empty element", text)
	}
	if got := data.attr("content-type"); got != "text/calendar" {
		t.Errorf("supported-calendar-data content-type = %q, want \"text/calendar\"", got)
	}
	if got := data.attr("version"); got != "2.0" {
		t.Errorf("supported-calendar-data version = %q, want \"2.0\"", got)
	}
}

func TestRFC4791_CalendarCollection_SupportedCalendarData_NotInAllprop(t *testing.T) {
	now := store.Now()
	calRepo := &fakeCalendarRepo{
		accessible: []store.CalendarAccess{
			{Calendar: store.Calendar{ID: 1, UserID: 1, Name: "Test", UpdatedAt: now}, Editor: true},
		},
	}
	h := &DavServer{store: &store.Store{Calendars: calRepo, Events: &fakeEventRepo{}}}
	user := &store.User{ID: 1}

	req := httptest.NewRequest("PROPFIND", "/dav/calendars/1/", nil)
	req.Header.Set("Depth", "0")
	req = req.WithContext(auth.WithUser(req.Context(), user))
	rr := httptest.NewRecorder()

	h.Propfind(rr, req)

	decodeMultistatus(t, rr).
		responseForHref(t, "/dav/calendars/1/").
		assertPropAbsent(t, calQN("supported-calendar-data"))
}

func TestRFC4791_CalendarCollection_Propfind_MaxResourceSizeAdvertised(t *testing.T) {
	now := store.Now()
	calRepo := &fakeCalendarRepo{
		accessible: []store.CalendarAccess{
			{Calendar: store.Calendar{ID: 1, UserID: 1, Name: "Test", UpdatedAt: now}, Editor: true},
		},
	}
	h := &DavServer{store: &store.Store{Calendars: calRepo, Events: &fakeEventRepo{}}}
	user := &store.User{ID: 1}

	body := `<?xml version="1.0" encoding="utf-8"?>
<d:propfind xmlns:d="DAV:" xmlns:cal="urn:ietf:params:xml:ns:caldav">
  <d:prop>
    <cal:max-resource-size/>
  </d:prop>
</d:propfind>`

	req := httptest.NewRequest("PROPFIND", "/dav/calendars/1/", strings.NewReader(body))
	req.Header.Set("Depth", "0")
	req = req.WithContext(auth.WithUser(req.Context(), user))
	rr := httptest.NewRecorder()

	h.Propfind(rr, req)

	// helpers.go advertises the limit unconditionally, so the RFC's "or 404"
	// alternative is not a branch this server can take.
	size := decodeMultistatus(t, rr).
		responseForHref(t, "/dav/calendars/1/").
		assertPropInt(t, calQN("max-resource-size"))

	if size != maxDAVBodyBytes {
		t.Errorf("max-resource-size = %d, want %d", size, maxDAVBodyBytes)
	}
}

func TestRFC4791_CalendarCollection_Propfind_MinMaxDateTimeAdvertised(t *testing.T) {
	now := store.Now()
	calRepo := &fakeCalendarRepo{
		accessible: []store.CalendarAccess{
			{Calendar: store.Calendar{ID: 1, UserID: 1, Name: "Test", UpdatedAt: now}, Editor: true},
		},
	}
	h := &DavServer{store: &store.Store{Calendars: calRepo, Events: &fakeEventRepo{}}}
	user := &store.User{ID: 1}

	body := `<?xml version="1.0" encoding="utf-8"?>
<d:propfind xmlns:d="DAV:" xmlns:cal="urn:ietf:params:xml:ns:caldav">
  <d:prop>
    <cal:min-date-time/>
    <cal:max-date-time/>
  </d:prop>
</d:propfind>`

	req := httptest.NewRequest("PROPFIND", "/dav/calendars/1/", strings.NewReader(body))
	req.Header.Set("Depth", "0")
	req = req.WithContext(auth.WithUser(req.Context(), user))
	rr := httptest.NewRecorder()

	h.Propfind(rr, req)

	resp := decodeMultistatus(t, rr).responseForHref(t, "/dav/calendars/1/")
	minTime := resp.assertPropDateTime(t, calQN("min-date-time"))
	maxTime := resp.assertPropDateTime(t, calQN("max-date-time"))

	if !minTime.Before(maxTime) {
		t.Errorf("min-date-time %s is not before max-date-time %s", minTime, maxTime)
	}
}

func TestRFC4791_Precondition_SupportedCalendarData_RejectUnsupportedMediaType(t *testing.T) {
	now := store.Now()
	calRepo := &fakeCalendarRepo{
		accessible: []store.CalendarAccess{
			{Calendar: store.Calendar{ID: 1, UserID: 1, Name: "Test", UpdatedAt: now}, Editor: true},
		},
	}
	h := &DavServer{store: &store.Store{Calendars: calRepo, Events: &fakeEventRepo{}}}
	user := &store.User{ID: 1}

	propBody := `<?xml version="1.0" encoding="utf-8"?>
<d:propfind xmlns:d="DAV:" xmlns:cal="urn:ietf:params:xml:ns:caldav">
  <d:prop>
    <cal:supported-calendar-data/>
  </d:prop>
</d:propfind>`
	req := httptest.NewRequest("PROPFIND", "/dav/calendars/1/", strings.NewReader(propBody))
	req.Header.Set("Depth", "0")
	req = req.WithContext(auth.WithUser(req.Context(), user))
	rr := httptest.NewRecorder()

	h.Propfind(rr, req)

	decodeMultistatus(t, rr).
		responseForHref(t, "/dav/calendars/1/").
		assertPropStatus(t, calQN("supported-calendar-data"), http.StatusOK)

	icalData := buildCalendarObject(buildVEvent("unsupported"))
	req = newCalendarPutRequest("/dav/calendars/1/unsupported.ics", strings.NewReader(icalData))
	req.Header.Set("Content-Type", "application/octet-stream")
	req = req.WithContext(auth.WithUser(req.Context(), user))
	rr = httptest.NewRecorder()

	h.Put(rr, req)

	assertErrorConditions(t, rr, http.StatusForbidden, calQN("supported-calendar-data"))
}

func TestRFC4791_Precondition_MinDateTime_RejectEarlierDates(t *testing.T) {
	now := store.Now()
	calRepo := &fakeCalendarRepo{
		accessible: []store.CalendarAccess{
			{Calendar: store.Calendar{ID: 1, UserID: 1, Name: "Test", UpdatedAt: now}, Editor: true},
		},
	}
	h := &DavServer{store: &store.Store{Calendars: calRepo, Events: &fakeEventRepo{}}}
	user := &store.User{ID: 1}

	propBody := `<?xml version="1.0" encoding="utf-8"?>
<d:propfind xmlns:d="DAV:" xmlns:cal="urn:ietf:params:xml:ns:caldav">
  <d:prop>
    <cal:min-date-time/>
  </d:prop>
</d:propfind>`
	req := httptest.NewRequest("PROPFIND", "/dav/calendars/1/", strings.NewReader(propBody))
	req.Header.Set("Depth", "0")
	req = req.WithContext(auth.WithUser(req.Context(), user))
	rr := httptest.NewRecorder()

	h.Propfind(rr, req)

	minTime := decodeMultistatus(t, rr).
		responseForHref(t, "/dav/calendars/1/").
		assertPropDateTime(t, calQN("min-date-time"))

	testStart := minTime.AddDate(0, 0, -1)
	testEnd := testStart.Add(time.Hour)
	icalData := buildCalendarObject(buildVEvent("too-early",
		"DTSTART:"+testStart.Format("20060102T150405Z"),
		"DTEND:"+testEnd.Format("20060102T150405Z")))

	req = newCalendarPutRequest("/dav/calendars/1/too-early.ics", strings.NewReader(icalData))
	req = req.WithContext(auth.WithUser(req.Context(), user))
	rr = httptest.NewRecorder()

	h.Put(rr, req)

	assertErrorConditions(t, rr, http.StatusForbidden, calQN("min-date-time"))
}

// newYorkVTimezone backs the TZID fixtures below. RFC 4791 §4.1 requires a
// calendar object resource to carry one VTIMEZONE for every unique TZID it
// names, so a fixture referencing America/New_York without this is not a valid
// calendar object resource and cannot stand as an RFC 4791 test.
const newYorkVTimezone = "BEGIN:VTIMEZONE\r\n" +
	"TZID:America/New_York\r\n" +
	"BEGIN:DAYLIGHT\r\n" +
	"DTSTART:19700308T020000\r\n" +
	"RRULE:FREQ=YEARLY;BYMONTH=3;BYDAY=2SU\r\n" +
	"TZOFFSETFROM:-0500\r\n" +
	"TZOFFSETTO:-0400\r\n" +
	"TZNAME:EDT\r\n" +
	"END:DAYLIGHT\r\n" +
	"BEGIN:STANDARD\r\n" +
	"DTSTART:19701101T020000\r\n" +
	"RRULE:FREQ=YEARLY;BYMONTH=11;BYDAY=1SU\r\n" +
	"TZOFFSETFROM:-0400\r\n" +
	"TZOFFSETTO:-0500\r\n" +
	"TZNAME:EST\r\n" +
	"END:STANDARD\r\n" +
	"END:VTIMEZONE\r\n"

// tzidEventFixture builds a one-VEVENT calendar object whose DTSTART and DTEND
// are RFC 5545 §3.3.5 form 3 values — date with local time and a time zone
// reference. That and UTC are the only forms a DATE-TIME may take; the numeric
// UTC-offset spelling iCalendar has no form for is what the date-limit
// fixtures used to send.
func tzidEventFixture(uid string, start, end time.Time) string {
	return buildCalendarObject(newYorkVTimezone, buildVEvent(uid,
		"DTSTART;TZID=America/New_York:"+start.Format("20060102T150405"),
		"DTEND;TZID=America/New_York:"+end.Format("20060102T150405")))
}

func TestRFC4791_Precondition_MinDateTime_RejectEarlierDatesWithTZID(t *testing.T) {
	now := store.Now()
	calRepo := &fakeCalendarRepo{
		accessible: []store.CalendarAccess{
			{Calendar: store.Calendar{ID: 1, UserID: 1, Name: "Test", UpdatedAt: now}, Editor: true},
		},
	}
	h := &DavServer{store: &store.Store{Calendars: calRepo, Events: &fakeEventRepo{}}}
	user := &store.User{ID: 1}

	loc, err := time.LoadLocation("America/New_York")
	if err != nil {
		t.Skipf("timezone data not available: %v", err)
	}

	propBody := `<?xml version="1.0" encoding="utf-8"?>
<d:propfind xmlns:d="DAV:" xmlns:cal="urn:ietf:params:xml:ns:caldav">
  <d:prop>
    <cal:min-date-time/>
  </d:prop>
</d:propfind>`
	req := httptest.NewRequest("PROPFIND", "/dav/calendars/1/", strings.NewReader(propBody))
	req.Header.Set("Depth", "0")
	req = req.WithContext(auth.WithUser(req.Context(), user))
	rr := httptest.NewRecorder()

	h.Propfind(rr, req)

	minTime := decodeMultistatus(t, rr).
		responseForHref(t, "/dav/calendars/1/").
		assertPropDateTime(t, calQN("min-date-time"))

	testStart := minTime.AddDate(0, 0, -1).In(loc)
	testEnd := testStart.Add(time.Hour)
	icalData := tzidEventFixture("too-early-tzid", testStart, testEnd)

	req = newCalendarPutRequest("/dav/calendars/1/too-early-tzid.ics", strings.NewReader(icalData))
	req = req.WithContext(auth.WithUser(req.Context(), user))
	rr = httptest.NewRecorder()

	h.Put(rr, req)

	assertErrorConditions(t, rr, http.StatusForbidden, calQN("min-date-time"))
}

func TestRFC4791_Precondition_MaxDateTime_RejectLaterDates(t *testing.T) {
	now := store.Now()
	calRepo := &fakeCalendarRepo{
		accessible: []store.CalendarAccess{
			{Calendar: store.Calendar{ID: 1, UserID: 1, Name: "Test", UpdatedAt: now}, Editor: true},
		},
	}
	h := &DavServer{store: &store.Store{Calendars: calRepo, Events: &fakeEventRepo{}}}
	user := &store.User{ID: 1}

	propBody := `<?xml version="1.0" encoding="utf-8"?>
<d:propfind xmlns:d="DAV:" xmlns:cal="urn:ietf:params:xml:ns:caldav">
  <d:prop>
    <cal:max-date-time/>
  </d:prop>
</d:propfind>`
	req := httptest.NewRequest("PROPFIND", "/dav/calendars/1/", strings.NewReader(propBody))
	req.Header.Set("Depth", "0")
	req = req.WithContext(auth.WithUser(req.Context(), user))
	rr := httptest.NewRecorder()

	h.Propfind(rr, req)

	maxTime := decodeMultistatus(t, rr).
		responseForHref(t, "/dav/calendars/1/").
		assertPropDateTime(t, calQN("max-date-time"))

	testStart := maxTime.AddDate(0, 0, 1)
	testEnd := testStart.Add(time.Hour)
	icalData := buildCalendarObject(buildVEvent("too-late",
		"DTSTART:"+testStart.Format("20060102T150405Z"),
		"DTEND:"+testEnd.Format("20060102T150405Z")))

	req = newCalendarPutRequest("/dav/calendars/1/too-late.ics", strings.NewReader(icalData))
	req = req.WithContext(auth.WithUser(req.Context(), user))
	rr = httptest.NewRecorder()

	h.Put(rr, req)

	assertErrorConditions(t, rr, http.StatusForbidden, calQN("max-date-time"))
}

// The max-date-time counterpart of the min-date-time TZID case above: the
// limit is compared against the instant a form-3 DATE-TIME denotes, not against
// its local wall-clock digits, which read as earlier than the limit here.
func TestRFC4791_Precondition_MaxDateTime_RejectLaterDatesWithTZID(t *testing.T) {
	now := store.Now()
	calRepo := &fakeCalendarRepo{
		accessible: []store.CalendarAccess{
			{Calendar: store.Calendar{ID: 1, UserID: 1, Name: "Test", UpdatedAt: now}, Editor: true},
		},
	}
	h := &DavServer{store: &store.Store{Calendars: calRepo, Events: &fakeEventRepo{}}}
	user := &store.User{ID: 1}

	loc, err := time.LoadLocation("America/New_York")
	if err != nil {
		t.Skipf("timezone data not available: %v", err)
	}

	propBody := `<?xml version="1.0" encoding="utf-8"?>
<d:propfind xmlns:d="DAV:" xmlns:cal="urn:ietf:params:xml:ns:caldav">
  <d:prop>
    <cal:max-date-time/>
  </d:prop>
</d:propfind>`
	req := httptest.NewRequest("PROPFIND", "/dav/calendars/1/", strings.NewReader(propBody))
	req.Header.Set("Depth", "0")
	req = req.WithContext(auth.WithUser(req.Context(), user))
	rr := httptest.NewRecorder()

	h.Propfind(rr, req)

	maxTime := decodeMultistatus(t, rr).
		responseForHref(t, "/dav/calendars/1/").
		assertPropDateTime(t, calQN("max-date-time"))

	testStart := maxTime.AddDate(0, 0, 1).In(loc)
	testEnd := testStart.Add(time.Hour)
	icalData := tzidEventFixture("too-late-tzid", testStart, testEnd)

	req = newCalendarPutRequest("/dav/calendars/1/too-late-tzid.ics", strings.NewReader(icalData))
	req = req.WithContext(auth.WithUser(req.Context(), user))
	rr = httptest.NewRecorder()

	h.Put(rr, req)

	assertErrorConditions(t, rr, http.StatusForbidden, calQN("max-date-time"))
}

// Sections 5.2.6, 5.2.7 and 5.3.2.1: the limit boundaries themselves.
//
// RFC 4791 states the max-date-time boundary twice and does not agree with
// itself. §5.2.7 calls the value "the inclusive latest date" and requires an
// error only for a value "later than this value"; the §5.3.2.1 precondition
// requires every stored DATE or DATE-TIME to be "less than" it, which rejects
// equality. CalCard resolves this in favour of §5.2.7, the property definition,
// which also makes the two limits symmetric: §5.2.6 ("earlier than") and
// §5.3.2.1 ("greater than or equal to") agree that min-date-time is inclusive.
//
// Both boundaries are pinned here so that choice cannot drift unnoticed. The
// one-unit-outside cases are what the neighbouring RejectLaterDates and
// PutBeforeMinDateTimeRejected tests cover; these are the equality cases they
// do not reach.
func TestRFC4791_Precondition_DateLimitsAreInclusive(t *testing.T) {
	now := store.Now()
	newServer := func() (*DavServer, *store.User) {
		calRepo := &fakeCalendarRepo{
			accessible: []store.CalendarAccess{
				{Calendar: store.Calendar{ID: 1, UserID: 1, Name: "Test", UpdatedAt: now}, Editor: true},
			},
		}
		return &DavServer{store: &store.Store{
			Calendars: calRepo,
			Events:    &fakeEventRepo{events: make(map[string]*store.Event)},
		}}, &store.User{ID: 1}
	}

	h, user := newServer()
	propBody := `<?xml version="1.0" encoding="utf-8"?>
<d:propfind xmlns:d="DAV:" xmlns:cal="urn:ietf:params:xml:ns:caldav">
  <d:prop>
    <cal:min-date-time/>
    <cal:max-date-time/>
  </d:prop>
</d:propfind>`
	req := httptest.NewRequest("PROPFIND", "/dav/calendars/1/", strings.NewReader(propBody))
	req.Header.Set("Depth", "0")
	req = req.WithContext(auth.WithUser(req.Context(), user))
	rr := httptest.NewRecorder()

	h.Propfind(rr, req)

	limits := decodeMultistatus(t, rr).responseForHref(t, "/dav/calendars/1/")
	minTime := limits.assertPropDateTime(t, calQN("min-date-time"))
	maxTime := limits.assertPropDateTime(t, calQN("max-date-time"))

	const icalUTC = "20060102T150405Z"
	tests := []struct {
		name      string
		uid       string
		dtStart   time.Time
		dtEnd     time.Time
		condition string // empty means the PUT must be accepted
	}{
		{
			name:    "DTEND exactly at max-date-time",
			uid:     "at-max",
			dtStart: maxTime.Add(-time.Hour),
			dtEnd:   maxTime,
		},
		{
			name:      "DTEND one second past max-date-time",
			uid:       "past-max",
			dtStart:   maxTime.Add(-time.Hour),
			dtEnd:     maxTime.Add(time.Second),
			condition: "max-date-time",
		},
		{
			name:    "DTSTART exactly at min-date-time",
			uid:     "at-min",
			dtStart: minTime,
			dtEnd:   minTime.Add(time.Hour),
		},
		{
			name:      "DTSTART one second before min-date-time",
			uid:       "before-min",
			dtStart:   minTime.Add(-time.Second),
			dtEnd:     minTime.Add(time.Hour),
			condition: "min-date-time",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			h, user := newServer()
			icalData := buildCalendarObject(buildVEvent(tt.uid,
				"DTSTART:"+tt.dtStart.UTC().Format(icalUTC),
				"DTEND:"+tt.dtEnd.UTC().Format(icalUTC)))

			req := newCalendarPutRequest("/dav/calendars/1/"+tt.uid+".ics", strings.NewReader(icalData))
			req = req.WithContext(auth.WithUser(req.Context(), user))
			rr := httptest.NewRecorder()

			h.Put(rr, req)

			if tt.condition != "" {
				assertErrorConditions(t, rr, http.StatusForbidden, calQN(tt.condition))
				return
			}
			if rr.Code != http.StatusCreated {
				t.Fatalf("PUT with a value exactly on the limit = %d, want 201 Created (RFC 4791 §5.2.6/§5.2.7 make both limits inclusive); body: %s",
					rr.Code, rr.Body.String())
			}
		})
	}
}

// Section 5.2.6: min-date-time Precondition
func TestRFC4791_PutBeforeMinDateTimeRejected(t *testing.T) {
	calRepo := &fakeCalendarRepo{
		accessible: []store.CalendarAccess{
			{Calendar: store.Calendar{ID: 1, UserID: 1, Name: "Test"}, Editor: true},
		},
	}
	h := &DavServer{store: &store.Store{Calendars: calRepo, Events: &fakeEventRepo{}}}
	user := &store.User{ID: 1}

	icalData := buildCalendarObject(buildVEvent("too-old",
		"DTSTART:18000101T000000Z", "DTEND:18000101T010000Z"))
	req := newCalendarPutRequest("/dav/calendars/1/too-old.ics", strings.NewReader(icalData))
	req = req.WithContext(auth.WithUser(req.Context(), user))
	rr := httptest.NewRecorder()

	h.Put(rr, req)

	assertErrorConditions(t, rr, http.StatusForbidden, calQN("min-date-time"))
}

// Section 5.2.7: max-date-time Precondition
func TestRFC4791_PutAfterMaxDateTimeRejected(t *testing.T) {
	calRepo := &fakeCalendarRepo{
		accessible: []store.CalendarAccess{
			{Calendar: store.Calendar{ID: 1, UserID: 1, Name: "Test"}, Editor: true},
		},
	}
	h := &DavServer{store: &store.Store{Calendars: calRepo, Events: &fakeEventRepo{}}}
	user := &store.User{ID: 1}

	icalData := buildCalendarObject(buildVEvent("too-far",
		"DTSTART:22000101T000000Z", "DTEND:22000101T010000Z"))
	req := newCalendarPutRequest("/dav/calendars/1/too-far.ics", strings.NewReader(icalData))
	req = req.WithContext(auth.WithUser(req.Context(), user))
	rr := httptest.NewRecorder()

	h.Put(rr, req)

	assertErrorConditions(t, rr, http.StatusForbidden, calQN("max-date-time"))
}

// Sections 5.2.6, 5.2.7 and 5.3.2.1 bound "any DATE or DATE-TIME value" a
// calendar object resource stores, not merely its DTSTART and DTEND. Every
// date-valued property below is read and bounded, in both the single-value and
// the comma-separated list forms, and in the PERIOD form VFREEBUSY uses.
func TestRFC4791_Precondition_DateLimits_CoverEveryDateProperty(t *testing.T) {
	tests := []struct {
		name      string
		condition string
		ical      string
	}{
		{
			name:      "VTODO DUE beyond max-date-time",
			condition: "max-date-time",
			ical: buildCalendarObject(buildVTodo("late-due",
				"DTSTART:20240601T000000Z", "DUE:99991231T235959Z")),
		},
		{
			name:      "RECURRENCE-ID before min-date-time",
			condition: "min-date-time",
			ical: buildCalendarObject(buildVEvent("early-recurrence-id",
				"RECURRENCE-ID:15000101T000000Z", "DTSTART:20240601T100000Z", "DTEND:20240601T110000Z")),
		},
		{
			name:      "RDATE beyond max-date-time",
			condition: "max-date-time",
			ical: buildCalendarObject(buildVEvent("late-rdate",
				"DTSTART:20240601T100000Z", "DTEND:20240601T110000Z", "RDATE:99991231T235959Z")),
		},
		{
			name:      "RDATE list with one out-of-range member",
			condition: "max-date-time",
			ical: buildCalendarObject(buildVEvent("late-rdate-list",
				"DTSTART:20240601T100000Z", "DTEND:20240601T110000Z",
				"RDATE:20240602T100000Z,99991231T235959Z")),
		},
		{
			name:      "RDATE PERIOD start before min-date-time",
			condition: "min-date-time",
			ical: buildCalendarObject(buildVEvent("early-rdate-period",
				"RDATE;VALUE=PERIOD:18991231T235959Z/PT1H")),
		},
		{
			name:      "RDATE PERIOD end beyond max-date-time",
			condition: "max-date-time",
			ical: buildCalendarObject(buildVEvent("late-rdate-period",
				"RDATE;VALUE=PERIOD:21001231T230000Z/21010101T010000Z")),
		},
		{
			name:      "RRULE UNTIL before min-date-time",
			condition: "min-date-time",
			ical: buildCalendarObject(buildVEvent("early-until",
				"RRULE:FREQ=YEARLY;UNTIL=18990101T000000Z")),
		},
		{
			name:      "RRULE UNTIL beyond max-date-time",
			condition: "max-date-time",
			ical: buildCalendarObject(buildVEvent("late-until",
				"RRULE:FREQ=YEARLY;UNTIL=22000101T000000Z")),
		},
		{
			name:      "EXDATE before min-date-time",
			condition: "min-date-time",
			ical: buildCalendarObject(buildVEvent("early-exdate",
				"DTSTART:20240601T100000Z", "DTEND:20240601T110000Z",
				"RRULE:FREQ=DAILY;COUNT=3", "EXDATE:15000101T000000Z")),
		},
		{
			name:      "VJOURNAL DTSTAMP beyond max-date-time",
			condition: "max-date-time",
			ical: buildCalendarObject(buildVJournal("late-dtstamp",
				"DTSTAMP:99991231T235959Z", "DTSTART;VALUE=DATE:20240601")),
		},
		{
			name:      "VTODO COMPLETED before min-date-time",
			condition: "min-date-time",
			ical: buildCalendarObject(buildVTodo("early-completed",
				"DTSTART:20240601T000000Z", "COMPLETED:15000101T000000Z")),
		},
		{
			name:      "CREATED before min-date-time",
			condition: "min-date-time",
			ical: buildCalendarObject(buildVEvent("early-created",
				"DTSTART:20240601T100000Z", "CREATED:15000101T000000Z")),
		},
		{
			name:      "LAST-MODIFIED beyond max-date-time",
			condition: "max-date-time",
			ical: buildCalendarObject(buildVEvent("late-last-modified",
				"DTSTART:20240601T100000Z", "LAST-MODIFIED:99991231T235959Z")),
		},
		{
			name:      "absolute VALARM TRIGGER beyond max-date-time",
			condition: "max-date-time",
			ical: buildCalendarObject(buildVEvent("late-trigger",
				"DTSTART:20240601T100000Z",
				buildComponent("VALARM",
					"ACTION:DISPLAY", "DESCRIPTION:Alarm",
					"TRIGGER;VALUE=DATE-TIME:99991231T235959Z"))),
		},
		{
			name:      "VFREEBUSY period beyond max-date-time",
			condition: "max-date-time",
			ical: buildCalendarObject(buildVFreeBusy("late-freebusy",
				"FREEBUSY:20240601T100000Z/99991231T235959Z")),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			calRepo := &fakeCalendarRepo{
				accessible: []store.CalendarAccess{
					{Calendar: store.Calendar{ID: 1, UserID: 1, Name: "Test"}, Editor: true},
				},
			}
			eventRepo := &fakeEventRepo{events: make(map[string]*store.Event)}
			h := &DavServer{store: &store.Store{Calendars: calRepo, Events: eventRepo}}
			user := &store.User{ID: 1}

			req := newCalendarPutRequest("/dav/calendars/1/limits.ics", strings.NewReader(tt.ical))
			req = req.WithContext(auth.WithUser(req.Context(), user))
			rr := httptest.NewRecorder()

			h.Put(rr, req)

			assertErrorConditions(t, rr, http.StatusForbidden, calQN(tt.condition))
			if len(eventRepo.events) != 0 {
				t.Fatalf("rejected object was stored: %#v", eventRepo.events)
			}
		})
	}
}

func TestRFC4791_Precondition_RecurrenceUntilDateLimitsAreInclusive(t *testing.T) {
	tests := map[string]string{
		"minimum": ical.MinDateTime,
		"maximum": ical.MaxDateTime,
	}

	for name, boundary := range tests {
		t.Run(name, func(t *testing.T) {
			h, eventRepo := writableCalendarServer()
			body := buildCalendarObject(buildVEvent("until-"+name,
				"DTSTART:"+boundary,
				"RRULE:FREQ=YEARLY;UNTIL="+boundary))

			rr := putCalendarObject(t, h, "until-"+name+".ics", body)

			if rr.Code != http.StatusCreated {
				t.Fatalf("PUT with RRULE UNTIL on the %s date limit = %d, want 201: %s", name, rr.Code, rr.Body.String())
			}
			if len(eventRepo.events) != 1 {
				t.Fatalf("stored events = %d, want 1", len(eventRepo.events))
			}
		})
	}
}

// Sections 5.2.6 and 5.2.7 bound the DATE and DATE-TIME values a resource
// carries, not the instances a recurrence rule generates from them. This rule
// has only two instances, so it stays within max-instances while its generated
// second instance falls beyond max-date-time.
func TestRFC4791_Precondition_DateLimitsIgnoreGeneratedRecurrenceInstances(t *testing.T) {
	calRepo := &fakeCalendarRepo{
		accessible: []store.CalendarAccess{
			{Calendar: store.Calendar{ID: 1, UserID: 1, Name: "Test"}, Editor: true},
		},
	}
	eventRepo := &fakeEventRepo{events: make(map[string]*store.Event)}
	h := &DavServer{store: &store.Store{Calendars: calRepo, Events: eventRepo}}
	user := &store.User{ID: 1}

	icalData := buildCalendarObject(buildVEvent("past-limit",
		"DTSTART:21001231T230000Z", "DTEND:21001231T235959Z", "RRULE:FREQ=YEARLY;COUNT=2"))
	req := newCalendarPutRequest("/dav/calendars/1/past-limit.ics", strings.NewReader(icalData))
	req = req.WithContext(auth.WithUser(req.Context(), user))
	rr := httptest.NewRecorder()

	h.Put(rr, req)

	if rr.Code != http.StatusCreated {
		t.Fatalf("PUT with only a generated instance beyond max-date-time = %d, want 201: %s", rr.Code, rr.Body.String())
	}
}

// A DATE-form DTSTART is checked, because analyzeICalendar parses the DATE and
// DATE-TIME spellings through the same helper. §5.3.2.1 names both forms, and
// this is the half of that rule CalCard does enforce.
func TestRFC4791_Precondition_MinDateTime_RejectsDateFormValue(t *testing.T) {
	calRepo := &fakeCalendarRepo{
		accessible: []store.CalendarAccess{
			{Calendar: store.Calendar{ID: 1, UserID: 1, Name: "Test"}, Editor: true},
		},
	}
	h := &DavServer{store: &store.Store{Calendars: calRepo, Events: &fakeEventRepo{events: make(map[string]*store.Event)}}}
	user := &store.User{ID: 1}

	icalData := buildCalendarObject(buildVEvent("early-date",
		"DTSTART;VALUE=DATE:18000101", "DTEND;VALUE=DATE:18000102"))
	req := newCalendarPutRequest("/dav/calendars/1/early-date.ics", strings.NewReader(icalData))
	req = req.WithContext(auth.WithUser(req.Context(), user))
	rr := httptest.NewRecorder()

	h.Put(rr, req)

	assertErrorConditions(t, rr, http.StatusForbidden, calQN("min-date-time"))
}

// Section 9.7.5: text-match filtering in calendar-query.
func TestRFC4791_TextMatchFilterInQuery(t *testing.T) {
	calRepo := &fakeCalendarRepo{
		accessible: []store.CalendarAccess{
			{Calendar: store.Calendar{ID: 1, UserID: 1, Name: "Test"}, Editor: true},
		},
	}
	eventRepo := &fakeEventRepo{
		events: map[string]*store.Event{
			"1:meeting": {
				CalendarID: 1,
				UID:        "meeting",
				RawICAL:    "BEGIN:VCALENDAR\r\nBEGIN:VEVENT\r\nUID:meeting\r\nSUMMARY:Team Meeting\r\nEND:VEVENT\r\nEND:VCALENDAR\r\n",
				ETag:       "e1",
			},
			"1:lunch": {
				CalendarID: 1,
				UID:        "lunch",
				RawICAL:    "BEGIN:VCALENDAR\r\nBEGIN:VEVENT\r\nUID:lunch\r\nSUMMARY:Lunch Break\r\nEND:VEVENT\r\nEND:VCALENDAR\r\n",
				ETag:       "e2",
			},
		},
	}
	h := &DavServer{store: &store.Store{Calendars: calRepo, Events: eventRepo}}
	user := &store.User{ID: 1}

	// RFC 4791 Section 9.7.5: Text match filter
	body := `<?xml version="1.0" encoding="utf-8"?>
<C:calendar-query xmlns:C="urn:ietf:params:xml:ns:caldav" xmlns:D="DAV:">
  <D:prop>
    <D:getetag/>
  </D:prop>
  <C:filter>
    <C:comp-filter name="VCALENDAR">
      <C:comp-filter name="VEVENT">
        <C:prop-filter name="SUMMARY">
          <C:text-match collation="i;ascii-casemap">meeting</C:text-match>
        </C:prop-filter>
      </C:comp-filter>
    </C:comp-filter>
  </C:filter>
</C:calendar-query>`

	req := httptest.NewRequest("REPORT", "/dav/calendars/1/", strings.NewReader(body))
	req.Header.Set("Depth", "1")
	req = req.WithContext(auth.WithUser(req.Context(), user))
	rr := httptest.NewRecorder()

	h.Report(rr, req)

	decodeMultistatus(t, rr).assertHrefs(t, "/dav/calendars/1/meeting.ics")
}

// Section 9.7.4: Prop Filter - is-not-defined
func TestRFC4791_PropFilterIsNotDefined(t *testing.T) {
	calRepo := &fakeCalendarRepo{
		accessible: []store.CalendarAccess{
			{Calendar: store.Calendar{ID: 1, UserID: 1, Name: "Test"}, Editor: true},
		},
	}
	eventRepo := &fakeEventRepo{
		events: map[string]*store.Event{
			"1:with-location": {
				CalendarID: 1,
				UID:        "with-location",
				RawICAL:    "BEGIN:VCALENDAR\r\nBEGIN:VEVENT\r\nUID:with-location\r\nLOCATION:Office\r\nEND:VEVENT\r\nEND:VCALENDAR\r\n",
				ETag:       "e1",
			},
			"1:without-location": {
				CalendarID: 1,
				UID:        "without-location",
				RawICAL:    "BEGIN:VCALENDAR\r\nBEGIN:VEVENT\r\nUID:without-location\r\nEND:VEVENT\r\nEND:VCALENDAR\r\n",
				ETag:       "e2",
			},
		},
	}
	h := &DavServer{store: &store.Store{Calendars: calRepo, Events: eventRepo}}
	user := &store.User{ID: 1}

	// RFC 4791 Section 9.7.4: is-not-defined test
	body := `<?xml version="1.0" encoding="utf-8"?>
<C:calendar-query xmlns:C="urn:ietf:params:xml:ns:caldav" xmlns:D="DAV:">
  <D:prop><D:getetag/></D:prop>
  <C:filter>
    <C:comp-filter name="VCALENDAR">
      <C:comp-filter name="VEVENT">
        <C:prop-filter name="LOCATION">
          <C:is-not-defined/>
        </C:prop-filter>
      </C:comp-filter>
    </C:comp-filter>
  </C:filter>
</C:calendar-query>`

	req := httptest.NewRequest("REPORT", "/dav/calendars/1/", strings.NewReader(body))
	req.Header.Set("Depth", "1")
	req = req.WithContext(auth.WithUser(req.Context(), user))
	rr := httptest.NewRecorder()

	h.Report(rr, req)

	decodeMultistatus(t, rr).assertHrefs(t, "/dav/calendars/1/without-location.ics")
}

// Sections 7.6 and 9.6: a calendar-data projection returns only the components
// and properties the request named, inside the VCALENDAR wrapper.
//
// What the projected VEVENT carries is asserted as an exact set. The stored
// resource is a valid calendar object resource — RFC 5545 §3.6.1 makes DTSTAMP
// and DTSTART required of a VEVENT — and the projection drops both, because the
// request named neither. §9.6 permits precisely that: returned calendar data
// "MAY be invalid per their media type specification if the
// CALDAV:calendar-data XML element part of the calendaring REPORT request did
// not specify required properties (e.g., UID, DTSTAMP, etc.)". The absent pair
// is therefore conformant, and a server adding back properties the client did
// not ask for would be the deviation.
func TestRFC4791_PartialCalendarDataRetrieval(t *testing.T) {
	calRepo := &fakeCalendarRepo{
		accessible: []store.CalendarAccess{
			{Calendar: store.Calendar{ID: 1, UserID: 1, Name: "Test"}, Editor: true},
		},
	}
	eventRepo := &fakeEventRepo{
		events: map[string]*store.Event{
			"1:event": {
				CalendarID: 1,
				UID:        "event",
				RawICAL: buildCalendarObject(buildVEvent("event",
					"SUMMARY:Test Event", "DESCRIPTION:Long description here")),
				ETag: "e",
			},
		},
	}
	h := &DavServer{store: &store.Store{Calendars: calRepo, Events: eventRepo}}
	user := &store.User{ID: 1}

	// §9.6.1: the projection is spelled as nested CALDAV:comp elements. The
	// second body names the inner component alone, which the returned object
	// must still wrap in its VCALENDAR.
	bodies := map[string]string{
		"comp nested under VCALENDAR": `<?xml version="1.0" encoding="utf-8"?>
<C:calendar-query xmlns:C="urn:ietf:params:xml:ns:caldav" xmlns:D="DAV:">
  <D:prop>
    <D:getetag/>
    <C:calendar-data>
      <C:comp name="VCALENDAR">
        <C:comp name="VEVENT">
          <C:prop name="SUMMARY"/>
          <C:prop name="UID"/>
        </C:comp>
      </C:comp>
    </C:calendar-data>
  </D:prop>
  <C:filter>
    <C:comp-filter name="VCALENDAR">
      <C:comp-filter name="VEVENT"/>
    </C:comp-filter>
  </C:filter>
</C:calendar-query>`,
		"comp naming VEVENT alone": `<?xml version="1.0" encoding="utf-8"?>
<C:calendar-query xmlns:C="urn:ietf:params:xml:ns:caldav" xmlns:D="DAV:">
  <D:prop>
    <D:getetag/>
    <C:calendar-data>
      <C:comp name="VEVENT">
        <C:prop name="SUMMARY"/>
        <C:prop name="UID"/>
      </C:comp>
    </C:calendar-data>
  </D:prop>
  <C:filter>
    <C:comp-filter name="VCALENDAR">
      <C:comp-filter name="VEVENT"/>
    </C:comp-filter>
  </C:filter>
</C:calendar-query>`,
	}

	for name, body := range bodies {
		t.Run(name, func(t *testing.T) {
			req := httptest.NewRequest("REPORT", "/dav/calendars/1/", strings.NewReader(body))
			req.Header.Set("Depth", "1")
			req = req.WithContext(auth.WithUser(req.Context(), user))
			rr := httptest.NewRecorder()

			h.Report(rr, req)

			data := decodeMultistatus(t, rr).
				responseForHref(t, "/dav/calendars/1/event.ics").
				assertPropStatus(t, calQN("calendar-data"), http.StatusOK).
				Value()

			root, err := parseICalendarObject(data)
			if err != nil {
				t.Fatalf("projected calendar-data does not parse: %v; value:\n%s", err, data)
			}
			if got := componentNames(t, data); !slices.Equal(got, []string{"VEVENT"}) {
				t.Fatalf("top-level components = %v, want [VEVENT]; value:\n%s", got, data)
			}
			if len(root.properties) != 0 {
				t.Errorf("VCALENDAR carries unrequested properties %v; value:\n%s", root.properties, data)
			}
			assertICalendarComponentProperties(t, data, "VEVENT", map[string]string{
				"UID":     "event",
				"SUMMARY": "Test Event",
			})
		})
	}
}

// assertICalendarObject parses value and asserts the iCalendar object frame:
// one VCALENDAR carrying exactly one VERSION and one PRODID of its own, per
// RFC 5545 §3.6, wrapping exactly the given top-level components in order.
func assertICalendarObject(t *testing.T, value string, wantComponents ...string) {
	t.Helper()
	analysis, err := analyzeICalendar(value)
	if err != nil {
		t.Fatalf("calendar-data is not a parseable iCalendar object: %v; value:\n%s", err, value)
	}
	got := make([]string, 0, len(analysis.Components))
	for _, component := range analysis.Components {
		got = append(got, component.Type)
	}
	if strings.Join(got, ",") != strings.Join(wantComponents, ",") {
		t.Errorf("calendar-data top-level components = %v, want %v; value:\n%s", got, wantComponents, value)
	}
	calendarProperties := icalendarPropertiesIn(t, value, "VCALENDAR")
	for _, required := range []string{"VERSION", "PRODID"} {
		if n := len(calendarProperties[required]); n != 1 {
			t.Errorf("VCALENDAR carries %d %s properties of its own, want exactly 1 (RFC 5545 §3.6); value:\n%s",
				n, required, value)
		}
	}
}

// assertICalendarComponentProperties asserts the named top-level component
// carries exactly these properties and values. The set is exact because that is
// what makes a projection assertable: a property the request did not name is as
// much a defect as a missing one, and a containment check sees neither.
func assertICalendarComponentProperties(t *testing.T, value, component string, want map[string]string) {
	t.Helper()
	got := make([]string, 0, len(want))
	for name, values := range icalendarPropertiesIn(t, value, "VCALENDAR", component) {
		for _, propertyValue := range values {
			got = append(got, name+":"+propertyValue)
		}
	}
	expected := make([]string, 0, len(want))
	for name, propertyValue := range want {
		expected = append(expected, name+":"+propertyValue)
	}
	sort.Strings(got)
	sort.Strings(expected)
	if strings.Join(got, ", ") != strings.Join(expected, ", ") {
		t.Errorf("%s carries [%s], want [%s]; value:\n%s",
			component, strings.Join(got, ", "), strings.Join(expected, ", "), value)
	}
}

// icalendarPropertiesIn indexes the parsed property values declared directly
// inside every component matching path. A malformed value fails the test rather
// than being walked line by line: this helper reads server output, and a
// partial walk would let an invalid projection satisfy semantic assertions.
func icalendarPropertiesIn(t *testing.T, value string, path ...string) map[string][]string {
	t.Helper()
	properties := make(map[string][]string)
	root, err := parseICalendarObject(value)
	if err != nil {
		t.Fatalf("server returned malformed iCalendar data: %v; value:\n%s", err, value)
	}
	if len(path) == 0 || !strings.EqualFold(path[0], root.name) {
		return properties
	}
	nodes := []*icalNode{root}
	for _, name := range path[1:] {
		var children []*icalNode
		for _, node := range nodes {
			for _, child := range node.children {
				if strings.EqualFold(child.name, name) {
					children = append(children, child)
				}
			}
		}
		nodes = children
	}
	for _, node := range nodes {
		for _, property := range node.properties {
			properties[property.name] = append(properties[property.name], property.value)
		}
	}
	return properties
}

// Section 9.6.6: limit-recurrence-set
func TestRFC4791_LimitRecurrenceSetInCalendarData(t *testing.T) {
	start := time.Date(2024, 1, 1, 10, 0, 0, 0, time.UTC)
	calRepo := &fakeCalendarRepo{
		accessible: []store.CalendarAccess{
			{Calendar: store.Calendar{ID: 1, UserID: 1, Name: "Test"}, Editor: true},
		},
	}
	// Recurring event
	eventRepo := &fakeEventRepo{
		events: map[string]*store.Event{
			"1:recurring": {
				CalendarID: 1,
				UID:        "recurring",
				RawICAL:    "BEGIN:VCALENDAR\r\nBEGIN:VEVENT\r\nUID:recurring\r\nDTSTART:20240101T100000Z\r\nRRULE:FREQ=DAILY;COUNT=30\r\nEND:VEVENT\r\nEND:VCALENDAR\r\n",
				ETag:       "e",
				DTStart:    &start,
			},
		},
	}
	h := &DavServer{store: &store.Store{Calendars: calRepo, Events: eventRepo}}
	user := &store.User{ID: 1}

	// RFC 4791 Section 9.6.6: limit-recurrence-set restricts recurring events to time range
	body := `<?xml version="1.0" encoding="utf-8"?>
<C:calendar-query xmlns:C="urn:ietf:params:xml:ns:caldav" xmlns:D="DAV:">
  <D:prop>
    <C:calendar-data>
      <C:limit-recurrence-set start="20240101T000000Z" end="20240110T235959Z"/>
    </C:calendar-data>
  </D:prop>
  <C:filter>
    <C:comp-filter name="VCALENDAR">
      <C:comp-filter name="VEVENT"/>
    </C:comp-filter>
  </C:filter>
</C:calendar-query>`

	req := httptest.NewRequest("REPORT", "/dav/calendars/1/", strings.NewReader(body))
	req.Header.Set("Depth", "1")
	req = req.WithContext(auth.WithUser(req.Context(), user))
	rr := httptest.NewRecorder()

	h.Report(rr, req)

	el := decodeMultistatus(t, rr).
		responseForHref(t, "/dav/calendars/1/recurring.ics").
		assertPropStatus(t, calQN("calendar-data"), http.StatusOK)
	value, err := scalarText(el)
	if err != nil {
		t.Fatal(err)
	}

	// §9.6.6 returns the master component and the overrides impacting the range.
	// This resource carries no override, so the master comes back as it stands:
	// the element narrows a recurrence set, it does not expand one, and the
	// RRULE is what still describes it.
	properties := icalendarPropertiesIn(t, value, "VCALENDAR", "VEVENT")
	if got := properties["RRULE"]; len(got) != 1 || got[0] != "FREQ=DAILY;COUNT=30" {
		t.Errorf("master RRULE = %v, want the stored rule; value:\n%s", got, value)
	}
	if got := properties["DTSTART"]; len(got) != 1 || got[0] != "20240101T100000Z" {
		t.Errorf("master DTSTART = %v, want the unexpanded one; value:\n%s", got, value)
	}
	if got := properties["RECURRENCE-ID"]; len(got) != 0 {
		t.Errorf("limit-recurrence-set synthesized RECURRENCE-ID %v, which is expand's job; value:\n%s", got, value)
	}
}

// Section 9.6.5: expand
func TestRFC4791_ExpandRecurringEventsInCalendarData(t *testing.T) {
	start := time.Date(2024, 1, 1, 10, 0, 0, 0, time.UTC)
	calRepo := &fakeCalendarRepo{
		accessible: []store.CalendarAccess{
			{Calendar: store.Calendar{ID: 1, UserID: 1, Name: "Test"}, Editor: true},
		},
	}
	eventRepo := &fakeEventRepo{
		events: map[string]*store.Event{
			"1:recurring": {
				CalendarID: 1,
				UID:        "recurring",
				RawICAL:    "BEGIN:VCALENDAR\r\nBEGIN:VEVENT\r\nUID:recurring\r\nDTSTART:20240101T100000Z\r\nRRULE:FREQ=DAILY;COUNT=5\r\nEND:VEVENT\r\nEND:VCALENDAR\r\n",
				ETag:       "e",
				DTStart:    &start,
			},
		},
	}
	h := &DavServer{store: &store.Store{Calendars: calRepo, Events: eventRepo}}
	user := &store.User{ID: 1}

	// RFC 4791 Section 9.6.5: expand converts recurring events to individual instances
	body := `<?xml version="1.0" encoding="utf-8"?>
<C:calendar-query xmlns:C="urn:ietf:params:xml:ns:caldav" xmlns:D="DAV:">
  <D:prop>
    <C:calendar-data>
      <C:expand start="20240101T000000Z" end="20240110T235959Z"/>
    </C:calendar-data>
  </D:prop>
  <C:filter>
    <C:comp-filter name="VCALENDAR">
      <C:comp-filter name="VEVENT"/>
    </C:comp-filter>
  </C:filter>
</C:calendar-query>`

	req := httptest.NewRequest("REPORT", "/dav/calendars/1/", strings.NewReader(body))
	req.Header.Set("Depth", "1")
	req = req.WithContext(auth.WithUser(req.Context(), user))
	rr := httptest.NewRecorder()

	h.Report(rr, req)

	el := decodeMultistatus(t, rr).
		responseForHref(t, "/dav/calendars/1/recurring.ics").
		assertPropStatus(t, calQN("calendar-data"), http.StatusOK)
	value, err := scalarText(el)
	if err != nil {
		t.Fatal(err)
	}

	// §9.6.5: one component per instance, each carrying the RECURRENCE-ID that
	// names it, and no recurrence property left to describe a set.
	properties := icalendarPropertiesIn(t, value, "VCALENDAR", "VEVENT")
	wantStarts := []string{
		"20240101T100000Z", "20240102T100000Z", "20240103T100000Z",
		"20240104T100000Z", "20240105T100000Z",
	}
	if !slices.Equal(properties["DTSTART"], wantStarts) {
		t.Errorf("expanded DTSTARTs = %v, want %v; value:\n%s", properties["DTSTART"], wantStarts, value)
	}
	if !slices.Equal(properties["RECURRENCE-ID"], wantStarts) {
		t.Errorf("expanded RECURRENCE-IDs = %v, want one per instance; value:\n%s",
			properties["RECURRENCE-ID"], value)
	}
	if got := properties["RRULE"]; len(got) != 0 {
		t.Errorf("expanded output still carries RRULE %v, which §9.6.5 forbids; value:\n%s", got, value)
	}
}

// Section 9.6.7: limit-freebusy-set
//
// The element trims the FREEBUSY period values of a returned VFREEBUSY to those
// intersecting the range, and drops a property left holding none. Driven through
// the handler because the transform reads a stored resource, and a REPORT is the
// only way a client reaches it.
func TestRFC4791_LimitFreeBusySetInCalendarData(t *testing.T) {
	start := time.Date(2024, 6, 1, 0, 0, 0, 0, time.UTC)
	h := &DavServer{store: &store.Store{
		Calendars: &fakeCalendarRepo{accessible: []store.CalendarAccess{{
			Calendar: store.Calendar{ID: 1, UserID: 1, Name: "Test"},
			Editor:   true,
		}}},
		Events: &fakeEventRepo{events: map[string]*store.Event{
			"1:busy": {
				CalendarID: 1,
				UID:        "busy",
				RawICAL: "BEGIN:VCALENDAR\r\nBEGIN:VFREEBUSY\r\nUID:busy\r\n" +
					"DTSTART:20240601T000000Z\r\nDTEND:20240605T000000Z\r\n" +
					"FREEBUSY:20240601T090000Z/20240601T100000Z\r\n" +
					"FREEBUSY:20240604T090000Z/20240604T100000Z\r\n" +
					"END:VFREEBUSY\r\nEND:VCALENDAR\r\n",
				ETag:    "e",
				DTStart: &start,
			},
		}},
	}}

	body := `<?xml version="1.0" encoding="utf-8"?>
<C:calendar-query xmlns:C="urn:ietf:params:xml:ns:caldav" xmlns:D="DAV:">
  <D:prop>
    <C:calendar-data>
      <C:limit-freebusy-set start="20240601T000000Z" end="20240602T000000Z"/>
    </C:calendar-data>
  </D:prop>
  <C:filter>
    <C:comp-filter name="VCALENDAR">
      <C:comp-filter name="VFREEBUSY"/>
    </C:comp-filter>
  </C:filter>
</C:calendar-query>`

	req := httptest.NewRequest("REPORT", "/dav/calendars/1/", strings.NewReader(body))
	req.Header.Set("Depth", "1")
	req = req.WithContext(auth.WithUser(req.Context(), &store.User{ID: 1}))
	rr := httptest.NewRecorder()

	h.Report(rr, req)

	el := decodeMultistatus(t, rr).
		responseForHref(t, "/dav/calendars/1/busy.ics").
		assertPropStatus(t, calQN("calendar-data"), http.StatusOK)
	value, err := scalarText(el)
	if err != nil {
		t.Fatal(err)
	}

	properties := icalendarPropertiesIn(t, value, "VCALENDAR", "VFREEBUSY")
	want := []string{"20240601T090000Z/20240601T100000Z"}
	if !slices.Equal(properties["FREEBUSY"], want) {
		t.Errorf("FREEBUSY periods = %v, want %v; value:\n%s", properties["FREEBUSY"], want, value)
	}
	// §9.6.7 narrows the periods; it does not touch the rest of the component.
	if got := properties["DTEND"]; len(got) != 1 || got[0] != "20240605T000000Z" {
		t.Errorf("DTEND = %v, want the stored bound; value:\n%s", got, value)
	}
}

// RFC 4791 §7.3 orders the zone a report resolves a floating value against: the
// CALDAV:timezone the request carries, else the CALDAV:calendar-timezone of the
// targeted collection, else UTC. §9.6.5 expansion is recurrence arithmetic over
// exactly those values, so the ordering has to reach the projection and not
// only the filter.
func TestRFC4791_ExpandResolvesFloatingValuesThroughTheReportTimezone(t *testing.T) {
	if _, err := time.LoadLocation("America/Chicago"); err != nil {
		t.Skip("tzdata unavailable")
	}

	// A floating DTSTART, so the zone decides which instant each instance names.
	const floatingRecurrence = "BEGIN:VCALENDAR\r\nBEGIN:VEVENT\r\nUID:floating\r\n" +
		"DTSTART:20240101T090000\r\nDTEND:20240101T100000\r\n" +
		"RRULE:FREQ=DAILY;COUNT=2\r\nEND:VEVENT\r\nEND:VCALENDAR\r\n"

	newServer := func(collectionTimezone *string) *DavServer {
		start := time.Date(2024, 1, 1, 9, 0, 0, 0, time.UTC)
		return &DavServer{store: &store.Store{
			Calendars: &fakeCalendarRepo{accessible: []store.CalendarAccess{{
				Calendar: store.Calendar{ID: 1, UserID: 1, Name: "Test", Timezone: collectionTimezone},
				Editor:   true,
			}}},
			Events: &fakeEventRepo{events: map[string]*store.Event{
				"1:floating": {CalendarID: 1, UID: "floating", RawICAL: floatingRecurrence, ETag: "e", DTStart: &start},
			}},
		}}
	}

	expandedStarts := func(t *testing.T, h *DavServer, body string) []string {
		t.Helper()
		req := httptest.NewRequest("REPORT", "/dav/calendars/1/", strings.NewReader(body))
		req.Header.Set("Depth", "1")
		req = req.WithContext(auth.WithUser(req.Context(), &store.User{ID: 1}))
		rr := httptest.NewRecorder()
		h.Report(rr, req)

		el := decodeMultistatus(t, rr).
			responseForHref(t, "/dav/calendars/1/floating.ics").
			assertPropStatus(t, calQN("calendar-data"), http.StatusOK)
		value, err := scalarText(el)
		if err != nil {
			t.Fatal(err)
		}
		return icalendarPropertiesIn(t, value, "VCALENDAR", "VEVENT")["DTSTART"]
	}

	queryBody := func(timezone string) string {
		return `<?xml version="1.0" encoding="utf-8"?>
<C:calendar-query xmlns:C="urn:ietf:params:xml:ns:caldav" xmlns:D="DAV:">
  <D:prop><C:calendar-data><C:expand start="20240101T000000Z" end="20240101T120000Z"/></C:calendar-data></D:prop>
  <C:filter><C:comp-filter name="VCALENDAR"><C:comp-filter name="VEVENT"/></C:comp-filter></C:filter>` +
			timezone + `
</C:calendar-query>`
	}

	// The range ends at noon UTC. The first instance's 09:00 wall clock is
	// 09:00Z read as UTC, which is inside it, and 15:00Z read in Chicago, which
	// is not. So the zone in force decides whether anything comes back at all.
	t.Run("UTC when neither source names a zone", func(t *testing.T) {
		got := expandedStarts(t, newServer(nil), queryBody(""))
		if want := []string{"20240101T090000"}; !slices.Equal(got, want) {
			t.Errorf("expanded DTSTARTs = %v, want %v", got, want)
		}
	})

	t.Run("the request timezone is honoured", func(t *testing.T) {
		body := queryBody(`<C:timezone>` + grammarVTimezoneFor("America/Chicago") + `</C:timezone>`)
		if got := expandedStarts(t, newServer(nil), body); len(got) != 0 {
			t.Errorf("expanded DTSTARTs = %v, want none: 09:00 Chicago is outside the requested day", got)
		}
	})

	t.Run("the collection timezone answers when the request names none", func(t *testing.T) {
		chicago := vTimezoneObject("America/Chicago")
		if got := expandedStarts(t, newServer(&chicago), queryBody("")); len(got) != 0 {
			t.Errorf("expanded DTSTARTs = %v, want none: the collection zone was not consulted", got)
		}
	})

	t.Run("the request timezone outranks the collection one", func(t *testing.T) {
		utc := vTimezoneObject("UTC")
		body := queryBody(`<C:timezone>` + grammarVTimezoneFor("America/Chicago") + `</C:timezone>`)
		if got := expandedStarts(t, newServer(&utc), body); len(got) != 0 {
			t.Errorf("expanded DTSTARTs = %v, want none: the collection zone outranked the request", got)
		}
	})
}

// Sections 7.4 and 9.9: time-range filtering with recurring events.
func TestRFC4791_TimeRangeFilteringWithRecurringEvents(t *testing.T) {
	start := time.Date(2024, 6, 1, 10, 0, 0, 0, time.UTC)
	calRepo := &fakeCalendarRepo{
		accessible: []store.CalendarAccess{
			{Calendar: store.Calendar{ID: 1, UserID: 1, Name: "Test"}, Editor: true},
		},
	}
	eventRepo := &fakeEventRepo{
		events: map[string]*store.Event{
			"1:recurring": {
				CalendarID: 1,
				UID:        "recurring",
				RawICAL:    "BEGIN:VCALENDAR\r\nBEGIN:VEVENT\r\nUID:recurring\r\nDTSTART:20240601T100000Z\r\nRRULE:FREQ=WEEKLY\r\nEND:VEVENT\r\nEND:VCALENDAR\r\n",
				ETag:       "e",
				DTStart:    &start,
			},
		},
	}
	h := &DavServer{store: &store.Store{Calendars: calRepo, Events: eventRepo}}
	user := &store.User{ID: 1}

	body := `<C:calendar-query xmlns:C="urn:ietf:params:xml:ns:caldav">
  <C:filter>
    <C:comp-filter name="VCALENDAR">
      <C:comp-filter name="VEVENT">
        <C:time-range start="20240615T000000Z" end="20240622T235959Z"/>
      </C:comp-filter>
    </C:comp-filter>
  </C:filter>
</C:calendar-query>`

	req := httptest.NewRequest("REPORT", "/dav/calendars/1/", strings.NewReader(body))
	req.Header.Set("Depth", "1")
	req = req.WithContext(auth.WithUser(req.Context(), user))
	rr := httptest.NewRecorder()

	h.Report(rr, req)

	decodeMultistatus(t, rr).assertHrefs(t, "/dav/calendars/1/recurring.ics")
}

// Sections 6.1.1 and 7.10: CALDAV:read-free-busy authorizes free-busy-query.
func TestRFC4791_ReadFreeBusyPrivilege(t *testing.T) {
	calRepo := &fakeCalendarRepo{
		accessible: []store.CalendarAccess{
			{Calendar: store.Calendar{ID: 1, UserID: 1, Name: "Test"}, Editor: true},
		},
	}
	h := &DavServer{store: &store.Store{Calendars: calRepo, Events: &fakeEventRepo{}}}
	user := &store.User{ID: 1}

	body := `<?xml version="1.0" encoding="utf-8" ?>
<C:free-busy-query xmlns:C="urn:ietf:params:xml:ns:caldav">
  <C:time-range start="20240601T000000Z" end="20240630T235959Z"/>
</C:free-busy-query>`

	req := httptest.NewRequest("REPORT", "/dav/calendars/1/", strings.NewReader(body))
	req = req.WithContext(auth.WithUser(req.Context(), user))
	rr := httptest.NewRecorder()

	h.Report(rr, req)

	if rr.Code != http.StatusOK {
		t.Fatalf("free-busy-query with read-free-busy = %d, want 200 OK (RFC 4791 §§6.1.1, 7.10)", rr.Code)
	}
	assertMediaType(t, rr, "text/calendar")
}

// RFC 3744 §5.4 current-user-privilege-set reports the CalDAV privilege
// defined by RFC 4791 §6.1.1.
func TestRFC4791_CurrentUserPrivilegeSetIncludesReadFreeBusy(t *testing.T) {
	now := store.Now()
	calRepo := &fakeCalendarRepo{
		accessible: []store.CalendarAccess{
			{Calendar: store.Calendar{ID: 1, UserID: 1, Name: "Test", UpdatedAt: now}, Editor: false},
		},
	}
	h := &DavServer{store: &store.Store{Calendars: calRepo, Events: &fakeEventRepo{}}}
	user := &store.User{ID: 1, PrimaryEmail: "user@example.com"}

	propBody := `<?xml version="1.0" encoding="utf-8"?>
<d:propfind xmlns:d="DAV:" xmlns:cal="urn:ietf:params:xml:ns:caldav">
  <d:prop>
    <d:current-user-privilege-set/>
  </d:prop>
</d:propfind>`

	req := httptest.NewRequest("PROPFIND", "/dav/calendars/1/", strings.NewReader(propBody))
	req.Header.Set("Depth", "0")
	req = req.WithContext(auth.WithUser(req.Context(), user))
	rr := httptest.NewRecorder()

	h.Propfind(rr, req)

	// §6.1: read-free-busy is granted in its own right, not only as part of the
	// DAV:read aggregate, so it must appear as a privilege of the collection.
	// This is membership only; the aggregation requirement is a separate
	// property, asserted by the test below.
	resp := decodeMultistatus(t, rr).responseForHref(t, "/dav/calendars/1/")
	resp.assertHasPrivilege(t, calQN("read-free-busy"))
	resp.assertHasPrivilege(t, davQN("read"))
}

// Section 6.1.1: "The CALDAV:read-free-busy privilege MUST be aggregated in the
// DAV:read privilege".
func TestRFC4791_SupportedPrivilegeSet_ReadFreeBusyAggregatedUnderRead(t *testing.T) {
	now := store.Now()
	calRepo := &fakeCalendarRepo{
		accessible: []store.CalendarAccess{
			{Calendar: store.Calendar{ID: 1, UserID: 1, Name: "Test", UpdatedAt: now}, Editor: false},
		},
	}
	h := &DavServer{store: &store.Store{Calendars: calRepo, Events: &fakeEventRepo{}}}
	user := &store.User{ID: 1, PrimaryEmail: "user@example.com"}

	propBody := `<?xml version="1.0" encoding="utf-8"?>
<d:propfind xmlns:d="DAV:">
  <d:prop>
    <d:supported-privilege-set/>
  </d:prop>
</d:propfind>`

	req := httptest.NewRequest("PROPFIND", "/dav/calendars/1/", strings.NewReader(propBody))
	req.Header.Set("Depth", "0")
	req = req.WithContext(auth.WithUser(req.Context(), user))
	rr := httptest.NewRecorder()

	h.Propfind(rr, req)

	resp := decodeMultistatus(t, rr).responseForHref(t, "/dav/calendars/1/")
	resp.assertSupportedPrivileges(t,
		"{DAV:}all",
		"{DAV:}all/{DAV:}read",
		"{DAV:}all/{DAV:}read-acl",
		"{DAV:}all/{DAV:}read-current-user-privilege-set",
		"{DAV:}all/{DAV:}read/{urn:ietf:params:xml:ns:caldav}read-free-busy",
		"{DAV:}all/{DAV:}write",
		"{DAV:}all/{DAV:}write-acl",
		"{DAV:}all/{DAV:}unlock",
		"{DAV:}all/{DAV:}write/{DAV:}bind",
		"{DAV:}all/{DAV:}write/{DAV:}unbind",
		"{DAV:}all/{DAV:}write/{DAV:}write-content",
		"{DAV:}all/{DAV:}write/{DAV:}write-properties",
	)
}

func TestRFC4791_ReadFreeBusyPrivilegeIsSupportedOnOrdinaryAndObjectResources(t *testing.T) {
	now := store.Now()
	calRepo := &fakeCalendarRepo{
		accessible: []store.CalendarAccess{{Calendar: store.Calendar{ID: 1, UserID: 1, Name: "Test", UpdatedAt: now}, Editor: true}},
	}
	eventRepo := &fakeEventRepo{events: map[string]*store.Event{
		"1:event": {CalendarID: 1, UID: "event", ResourceName: "event", RawICAL: buildCalendarObject(buildVEvent("event")), ETag: "etag"},
	}}
	h := &DavServer{store: &store.Store{Calendars: calRepo, Events: eventRepo}}
	user := &store.User{ID: 1, PrimaryEmail: "user@example.com"}
	body := `<?xml version="1.0" encoding="utf-8"?><D:propfind xmlns:D="DAV:"><D:prop><D:supported-privilege-set/></D:prop></D:propfind>`

	for _, href := range []string{"/dav/", "/dav/calendars/1/event.ics"} {
		t.Run(href, func(t *testing.T) {
			req := httptest.NewRequest("PROPFIND", href, strings.NewReader(body))
			req.Header.Set("Depth", "0")
			req = req.WithContext(auth.WithUser(req.Context(), user))
			rr := httptest.NewRecorder()

			h.Propfind(rr, req)

			privileges := decodeMultistatus(t, rr).responseForHref(t, href).supportedPrivileges(t)
			want := "{DAV:}all/{DAV:}read/{urn:ietf:params:xml:ns:caldav}read-free-busy"
			if !slices.Contains(privileges, want) {
				t.Fatalf("supported privileges on %s missing %s: %#v", href, want, privileges)
			}
		})
	}
}

// Sections 6.1.1 and 7.10: free-busy-query authorization must align with the
// advertised privilege.
func TestRFC4791_ReadFreeBusyPrivilegeEnforcedForReports(t *testing.T) {
	start := time.Date(2024, 6, 1, 10, 0, 0, 0, time.UTC)
	end := time.Date(2024, 6, 1, 12, 0, 0, 0, time.UTC)
	calRepo := &fakeCalendarRepo{
		accessible: []store.CalendarAccess{
			// Shared calendar with limited privileges (read-free-busy but not full read).
			{
				Calendar:   store.Calendar{ID: 1, UserID: 9, Name: "Test", UpdatedAt: store.Now()},
				Shared:     true,
				Editor:     false,
				Privileges: store.CalendarPrivileges{ReadFreeBusy: true},
			},
		},
	}
	eventRepo := &fakeEventRepo{
		events: map[string]*store.Event{
			"1:event": {
				CalendarID: 1,
				UID:        "event",
				RawICAL:    "BEGIN:VCALENDAR\r\nBEGIN:VEVENT\r\nUID:event\r\nEND:VEVENT\r\nEND:VCALENDAR\r\n",
				ETag:       "e",
				DTStart:    &start,
				DTEnd:      &end,
			},
		},
	}
	h := &DavServer{store: &store.Store{Calendars: calRepo, Events: eventRepo}}
	user := &store.User{ID: 1}

	freeBusyBody := `<?xml version="1.0" encoding="utf-8" ?>
<C:free-busy-query xmlns:C="urn:ietf:params:xml:ns:caldav">
  <C:time-range start="20240601T000000Z" end="20240630T235959Z"/>
</C:free-busy-query>`
	req := httptest.NewRequest("REPORT", "/dav/calendars/1/", strings.NewReader(freeBusyBody))
	req = req.WithContext(auth.WithUser(req.Context(), user))
	rr := httptest.NewRecorder()
	h.Report(rr, req)
	if rr.Code != http.StatusOK {
		t.Fatalf("free-busy-query must succeed with read-free-busy, got %d", rr.Code)
	}

	queryBody := `<?xml version="1.0" encoding="utf-8" ?>
<C:calendar-query xmlns:D="DAV:" xmlns:C="urn:ietf:params:xml:ns:caldav">
  <D:prop><D:getetag/></D:prop>
  <C:filter>
    <C:comp-filter name="VCALENDAR">
      <C:comp-filter name="VEVENT"/>
    </C:comp-filter>
  </C:filter>
</C:calendar-query>`
	req = httptest.NewRequest("REPORT", "/dav/calendars/1/", strings.NewReader(queryBody))
	req.Header.Set("Depth", "1")
	req = req.WithContext(auth.WithUser(req.Context(), user))
	rr = httptest.NewRecorder()
	h.Report(rr, req)
	if rr.Code != http.StatusForbidden {
		t.Fatalf("calendar-query with only read-free-busy = %d, want 403 Forbidden", rr.Code)
	}

	multigetBody := `<?xml version="1.0" encoding="utf-8" ?>
<C:calendar-multiget xmlns:D="DAV:" xmlns:C="urn:ietf:params:xml:ns:caldav">
  <D:prop><D:getetag/></D:prop>
  <D:href>/dav/calendars/1/event.ics</D:href>
</C:calendar-multiget>`
	req = httptest.NewRequest("REPORT", "/dav/calendars/1/", strings.NewReader(multigetBody))
	req = req.WithContext(auth.WithUser(req.Context(), user))
	rr = httptest.NewRecorder()
	h.Report(rr, req)
	if rr.Code != http.StatusForbidden {
		t.Fatalf("calendar-multiget with only read-free-busy = %d, want 403 Forbidden", rr.Code)
	}
}

// Section 7.8: calendar-query with Multiple Filters
func TestRFC4791_CalendarQueryWithMultipleFilters(t *testing.T) {
	start := time.Date(2024, 6, 15, 10, 0, 0, 0, time.UTC)
	end := time.Date(2024, 6, 15, 11, 0, 0, 0, time.UTC)
	nomatchStart := time.Date(2024, 7, 15, 10, 0, 0, 0, time.UTC)

	calRepo := &fakeCalendarRepo{
		accessible: []store.CalendarAccess{
			{Calendar: store.Calendar{ID: 1, UserID: 1, Name: "Test"}, Editor: true},
		},
	}
	eventRepo := &fakeEventRepo{
		events: map[string]*store.Event{
			"1:match": {
				CalendarID: 1,
				UID:        "match",
				RawICAL:    "BEGIN:VCALENDAR\r\nBEGIN:VEVENT\r\nUID:match\r\nSUMMARY:Important Meeting\r\nDTSTART:20240615T100000Z\r\nEND:VEVENT\r\nEND:VCALENDAR\r\n",
				ETag:       "e1",
				DTStart:    &start,
				DTEnd:      &end,
			},
			"1:nomatch": {
				CalendarID: 1,
				UID:        "nomatch",
				RawICAL:    "BEGIN:VCALENDAR\r\nBEGIN:VEVENT\r\nUID:nomatch\r\nSUMMARY:Other Event\r\nDTSTART:20240715T100000Z\r\nEND:VEVENT\r\nEND:VCALENDAR\r\n",
				ETag:       "e2",
				DTStart:    &nomatchStart,
			},
		},
	}
	h := &DavServer{store: &store.Store{Calendars: calRepo, Events: eventRepo}}
	user := &store.User{ID: 1}

	// Combining time-range and text-match filters
	body := `<?xml version="1.0" encoding="utf-8"?>
<C:calendar-query xmlns:C="urn:ietf:params:xml:ns:caldav" xmlns:D="DAV:">
  <D:prop><D:getetag/></D:prop>
  <C:filter>
    <C:comp-filter name="VCALENDAR">
      <C:comp-filter name="VEVENT">
        <C:time-range start="20240601T000000Z" end="20240630T235959Z"/>
        <C:prop-filter name="SUMMARY">
          <C:text-match>Important</C:text-match>
        </C:prop-filter>
      </C:comp-filter>
    </C:comp-filter>
  </C:filter>
</C:calendar-query>`

	req := httptest.NewRequest("REPORT", "/dav/calendars/1/", strings.NewReader(body))
	req.Header.Set("Depth", "1")
	req = req.WithContext(auth.WithUser(req.Context(), user))
	rr := httptest.NewRecorder()

	h.Report(rr, req)

	decodeMultistatus(t, rr).assertHrefs(t, "/dav/calendars/1/match.ics")
}

// Section 9.8: the CALDAV:timezone XML element. §9.5 admits it only as the last
// child of CALDAV:calendar-query -- §9.11's free-busy-query content model is
// `(time-range)` and takes no timezone -- so this is a calendar-query test.
//
// The half this asserts is that a request carrying a timezone is accepted and
// answered normally. The half it does not is §7.3 resolution: CalCard's
// reportRequest carries the value, but the time-range evaluator does not yet
// validate it or resolve floating values against it.
func TestRFC4791_TimezoneXMLElement(t *testing.T) {
	start := time.Date(2024, 6, 15, 10, 0, 0, 0, time.UTC)
	end := time.Date(2024, 6, 15, 11, 0, 0, 0, time.UTC)
	calRepo := &fakeCalendarRepo{
		accessible: []store.CalendarAccess{
			{Calendar: store.Calendar{ID: 1, UserID: 1, Name: "Test"}, Editor: true},
		},
	}
	eventRepo := &fakeEventRepo{
		events: map[string]*store.Event{
			"1:event": {
				CalendarID:   1,
				UID:          "event",
				ResourceName: "event",
				RawICAL:      "BEGIN:VCALENDAR\r\nBEGIN:VEVENT\r\nUID:event\r\nDTSTART:20240615T100000Z\r\nDTEND:20240615T110000Z\r\nEND:VEVENT\r\nEND:VCALENDAR\r\n",
				ETag:         "e1",
				DTStart:      &start,
				DTEnd:        &end,
			},
		},
	}
	h := &DavServer{store: &store.Store{Calendars: calRepo, Events: eventRepo}}
	user := &store.User{ID: 1}

	// §9.8 requires a valid iCalendar object with a single VTIMEZONE, which is
	// the VCALENDAR-wrapped form.
	body := `<?xml version="1.0" encoding="utf-8"?>
<C:calendar-query xmlns:C="urn:ietf:params:xml:ns:caldav" xmlns:D="DAV:">
  <D:prop><D:getetag/></D:prop>
  <C:filter>
    <C:comp-filter name="VCALENDAR">
      <C:comp-filter name="VEVENT">
        <C:time-range start="20240601T000000Z" end="20240630T235959Z"/>
      </C:comp-filter>
    </C:comp-filter>
  </C:filter>
  <C:timezone>BEGIN:VCALENDAR
VERSION:2.0
PRODID:-//CalCard//Strict Suite//EN
BEGIN:VTIMEZONE
TZID:America/New_York
BEGIN:STANDARD
DTSTART:20071104T020000
TZOFFSETFROM:-0400
TZOFFSETTO:-0500
TZNAME:EST
END:STANDARD
END:VTIMEZONE
END:VCALENDAR</C:timezone>
</C:calendar-query>`

	req := httptest.NewRequest("REPORT", "/dav/calendars/1/", strings.NewReader(body))
	req.Header.Set("Depth", "1")
	req = req.WithContext(auth.WithUser(req.Context(), user))
	rr := httptest.NewRecorder()

	h.Report(rr, req)

	ms := decodeMultistatus(t, rr)
	ms.assertHrefs(t, "/dav/calendars/1/event.ics")
	ms.responseForHref(t, "/dav/calendars/1/event.ics").
		assertPropStatus(t, davQN("getetag"), http.StatusOK)
}

// Section 9.6.1: calendar-multiget accepts a CALDAV:comp projection inside
// CALDAV:calendar-data.
func TestRFC4791_CalendarMultigetAcceptsComponentProjection(t *testing.T) {
	calRepo := &fakeCalendarRepo{
		accessible: []store.CalendarAccess{
			{Calendar: store.Calendar{ID: 1, UserID: 1, Name: "Test"}, Editor: true},
		},
	}
	eventRepo := &fakeEventRepo{
		events: map[string]*store.Event{
			"1:event": {
				CalendarID: 1,
				UID:        "event",
				RawICAL:    "BEGIN:VCALENDAR\r\nVERSION:2.0\r\nBEGIN:VEVENT\r\nUID:event\r\nSUMMARY:Test\r\nEND:VEVENT\r\nEND:VCALENDAR\r\n",
				ETag:       "e",
			},
		},
	}
	h := &DavServer{store: &store.Store{Calendars: calRepo, Events: eventRepo}}
	user := &store.User{ID: 1}

	// Request to limit returned components
	body := `<?xml version="1.0" encoding="utf-8"?>
<C:calendar-multiget xmlns:C="urn:ietf:params:xml:ns:caldav" xmlns:D="DAV:">
  <D:prop>
    <C:calendar-data>
      <C:comp name="VCALENDAR">
        <C:comp name="VEVENT"/>
      </C:comp>
    </C:calendar-data>
  </D:prop>
  <D:href>/dav/calendars/1/event.ics</D:href>
</C:calendar-multiget>`

	req := httptest.NewRequest("REPORT", "/dav/calendars/1/", strings.NewReader(body))
	req = req.WithContext(auth.WithUser(req.Context(), user))
	rr := httptest.NewRecorder()

	h.Report(rr, req)

	ms := decodeMultistatus(t, rr)
	ms.assertHrefs(t, "/dav/calendars/1/event.ics")
	ms.responseForHref(t, "/dav/calendars/1/event.ics").
		assertPropStatus(t, calQN("calendar-data"), http.StatusOK)
}

// RFC 4791 constrains the contents of a calendar object resource, never its
// URI. A ".ics" suffix is a client convention, so a resource stored without one
// must round-trip like any other.
func TestRFC4791_CalendarObjectResourceNameNeedsNoExtension(t *testing.T) {
	calRepo := &fakeCalendarRepo{
		accessible: []store.CalendarAccess{
			{Calendar: store.Calendar{ID: 1, UserID: 1, Name: "Test"}, Editor: true},
		},
	}
	eventRepo := &fakeEventRepo{events: make(map[string]*store.Event)}
	h := &DavServer{store: &store.Store{Calendars: calRepo, Events: eventRepo}}
	user := &store.User{ID: 1}

	icalData := buildCalendarObject(buildVEvent("test"))

	req := newCalendarPutRequest("/dav/calendars/1/event-no-extension", strings.NewReader(icalData))
	req = req.WithContext(auth.WithUser(req.Context(), user))
	rr := httptest.NewRecorder()

	h.Put(rr, req)

	if rr.Code != http.StatusCreated {
		t.Fatalf("PUT to an extensionless resource name = %d, want 201 Created; body: %s", rr.Code, rr.Body.String())
	}

	req = httptest.NewRequest(http.MethodGet, "/dav/calendars/1/event-no-extension", nil)
	req = req.WithContext(auth.WithUser(req.Context(), user))
	rr = httptest.NewRecorder()

	h.Get(rr, req)

	if rr.Code != http.StatusOK {
		t.Fatalf("GET of an extensionless resource name = %d, want 200 OK", rr.Code)
	}
	if rr.Body.String() != icalData {
		t.Errorf("GET returned %q, want the stored octets %q", rr.Body.String(), icalData)
	}
}

// Section 9.7.2: a prop-filter with no child tests property presence. The
// element spelling is deliberate: RFC 4791 grammar admits only is-not-defined,
// so presence is expressed by an empty prop-filter, not by an is-defined
// element carried over from the pre-RFC drafts.
func TestRFC4791_PropFilterWithNoChildMatchesDefinedProperty(t *testing.T) {
	calRepo := &fakeCalendarRepo{
		accessible: []store.CalendarAccess{
			{Calendar: store.Calendar{ID: 1, UserID: 1, Name: "Test"}, Editor: true},
		},
	}
	eventRepo := &fakeEventRepo{
		events: map[string]*store.Event{
			"1:with-desc": {
				CalendarID: 1,
				UID:        "with-desc",
				RawICAL:    "BEGIN:VCALENDAR\r\nBEGIN:VEVENT\r\nUID:with-desc\r\nDESCRIPTION:Has description\r\nEND:VEVENT\r\nEND:VCALENDAR\r\n",
				ETag:       "e1",
			},
			"1:without-desc": {
				CalendarID: 1,
				UID:        "without-desc",
				RawICAL:    "BEGIN:VCALENDAR\r\nBEGIN:VEVENT\r\nUID:without-desc\r\nEND:VEVENT\r\nEND:VCALENDAR\r\n",
				ETag:       "e2",
			},
		},
	}
	h := &DavServer{store: &store.Store{Calendars: calRepo, Events: eventRepo}}
	user := &store.User{ID: 1}

	body := `<?xml version="1.0" encoding="utf-8"?>
<C:calendar-query xmlns:C="urn:ietf:params:xml:ns:caldav" xmlns:D="DAV:">
  <D:prop><D:getetag/></D:prop>
  <C:filter>
    <C:comp-filter name="VCALENDAR">
      <C:comp-filter name="VEVENT">
        <C:prop-filter name="DESCRIPTION"/>
      </C:comp-filter>
    </C:comp-filter>
  </C:filter>
</C:calendar-query>`

	req := httptest.NewRequest("REPORT", "/dav/calendars/1/", strings.NewReader(body))
	req.Header.Set("Depth", "1")
	req = req.WithContext(auth.WithUser(req.Context(), user))
	rr := httptest.NewRecorder()

	h.Report(rr, req)

	ms := decodeMultistatus(t, rr)
	ms.assertHrefs(t, "/dav/calendars/1/with-desc.ics")
	resp := ms.responseForHref(t, "/dav/calendars/1/with-desc.ics")
	resp.assertPropValue(t, davQN("getetag"), http.StatusOK, `"e1"`)
}

// Section 9.7.5: the text-match negate-condition attribute
func TestRFC4791_NegateConditionInFilter(t *testing.T) {
	calRepo := &fakeCalendarRepo{
		accessible: []store.CalendarAccess{
			{Calendar: store.Calendar{ID: 1, UserID: 1, Name: "Test"}, Editor: true},
		},
	}
	eventRepo := &fakeEventRepo{
		events: map[string]*store.Event{
			"1:event1": {
				CalendarID: 1,
				UID:        "event1",
				RawICAL:    "BEGIN:VCALENDAR\r\nBEGIN:VEVENT\r\nUID:event1\r\nSUMMARY:Meeting\r\nEND:VEVENT\r\nEND:VCALENDAR\r\n",
				ETag:       "e1",
			},
			"1:event2": {
				CalendarID: 1,
				UID:        "event2",
				RawICAL:    "BEGIN:VCALENDAR\r\nBEGIN:VEVENT\r\nUID:event2\r\nSUMMARY:Lunch\r\nEND:VEVENT\r\nEND:VCALENDAR\r\n",
				ETag:       "e2",
			},
		},
	}
	h := &DavServer{store: &store.Store{Calendars: calRepo, Events: eventRepo}}
	user := &store.User{ID: 1}

	body := `<?xml version="1.0" encoding="utf-8"?>
<C:calendar-query xmlns:C="urn:ietf:params:xml:ns:caldav" xmlns:D="DAV:">
  <D:prop><D:getetag/></D:prop>
  <C:filter>
    <C:comp-filter name="VCALENDAR">
      <C:comp-filter name="VEVENT">
        <C:prop-filter name="SUMMARY">
          <C:text-match negate-condition="yes">Meeting</C:text-match>
        </C:prop-filter>
      </C:comp-filter>
    </C:comp-filter>
  </C:filter>
</C:calendar-query>`

	req := httptest.NewRequest("REPORT", "/dav/calendars/1/", strings.NewReader(body))
	req.Header.Set("Depth", "1")
	req = req.WithContext(auth.WithUser(req.Context(), user))
	rr := httptest.NewRecorder()

	h.Report(rr, req)

	ms := decodeMultistatus(t, rr)
	ms.assertHrefs(t, "/dav/calendars/1/event2.ics")
	resp := ms.responseForHref(t, "/dav/calendars/1/event2.ics")
	resp.assertPropValue(t, davQN("getetag"), http.StatusOK, `"e2"`)
}

// Section 5.2.9: max-attendees-per-instance Property
func TestRFC4791_MaxAttendeesPerInstanceProperty(t *testing.T) {
	now := store.Now()
	calRepo := &fakeCalendarRepo{
		accessible: []store.CalendarAccess{
			{Calendar: store.Calendar{ID: 1, UserID: 1, Name: "Test", UpdatedAt: now}, Editor: true},
		},
	}
	h := &DavServer{store: &store.Store{Calendars: calRepo, Events: &fakeEventRepo{}}}
	user := &store.User{ID: 1}

	body := `<?xml version="1.0" encoding="utf-8"?>
<d:propfind xmlns:d="DAV:" xmlns:c="urn:ietf:params:xml:ns:caldav">
  <d:prop>
    <c:max-attendees-per-instance/>
  </d:prop>
</d:propfind>`

	req := httptest.NewRequest("PROPFIND", "/dav/calendars/1/", strings.NewReader(body))
	req.Header.Set("Depth", "0")
	req = req.WithContext(auth.WithUser(req.Context(), user))
	rr := httptest.NewRecorder()

	h.Propfind(rr, req)

	// §5.2.9 makes the property a MAY, but CalCard enforces the limit, so the
	// value it advertises must match the one it enforces.
	got := decodeMultistatus(t, rr).
		responseForHref(t, "/dav/calendars/1/").
		assertPropInt(t, calQN("max-attendees-per-instance"))
	if got != ical.MaxAttendeesPerInstance {
		t.Errorf("max-attendees-per-instance = %d, want %d", got, ical.MaxAttendeesPerInstance)
	}
}

// Section 5.2.8: max-instances Property
func TestRFC4791_MaxInstancesProperty(t *testing.T) {
	now := store.Now()
	calRepo := &fakeCalendarRepo{
		accessible: []store.CalendarAccess{
			{Calendar: store.Calendar{ID: 1, UserID: 1, Name: "Test", UpdatedAt: now}, Editor: true},
		},
	}
	h := &DavServer{store: &store.Store{Calendars: calRepo, Events: &fakeEventRepo{}}}
	user := &store.User{ID: 1}

	body := `<?xml version="1.0" encoding="utf-8"?>
<d:propfind xmlns:d="DAV:" xmlns:c="urn:ietf:params:xml:ns:caldav">
  <d:prop>
    <c:max-instances/>
  </d:prop>
</d:propfind>`

	req := httptest.NewRequest("PROPFIND", "/dav/calendars/1/", strings.NewReader(body))
	req.Header.Set("Depth", "0")
	req = req.WithContext(auth.WithUser(req.Context(), user))
	rr := httptest.NewRecorder()

	h.Propfind(rr, req)

	// §5.2.8 makes the property a MAY, but CalCard enforces the limit, so the
	// value it advertises must match the one it enforces.
	got := decodeMultistatus(t, rr).
		responseForHref(t, "/dav/calendars/1/").
		assertPropInt(t, calQN("max-instances"))
	if got != ical.MaxRecurrenceInstances {
		t.Errorf("max-instances = %d, want %d", got, ical.MaxRecurrenceInstances)
	}
}

// Section 5.2.9: max-attendees-per-instance Precondition
func TestRFC4791_PutExceedsMaxAttendeesPerInstance(t *testing.T) {
	calRepo := &fakeCalendarRepo{
		accessible: []store.CalendarAccess{
			{Calendar: store.Calendar{ID: 1, UserID: 1, Name: "Test"}, Editor: true},
		},
	}
	h := &DavServer{store: &store.Store{Calendars: calRepo, Events: &fakeEventRepo{}}}
	user := &store.User{ID: 1}

	attendees := make([]string, 0, ical.MaxAttendeesPerInstance+2)
	attendees = append(attendees, "DTSTART:20240101T000000Z")
	alarmAttendees := []string{"ACTION:EMAIL", "TRIGGER:-PT15M", "DESCRIPTION:Alarm", "SUMMARY:Alarm"}
	for i := 0; i < ical.MaxAttendeesPerInstance+1; i++ {
		attendees = append(attendees, fmt.Sprintf("ATTENDEE:mailto:user%d@example.com", i))
		alarmAttendees = append(alarmAttendees, fmt.Sprintf("ATTENDEE:mailto:alarm%d@example.com", i))
	}
	tests := map[string]string{
		"VEVENT":              buildCalendarObject(buildVEvent("attendees", attendees...)),
		"VEVENT EMAIL VALARM": buildCalendarObject(buildVEvent("alarm-attendees", buildComponent("VALARM", alarmAttendees...))),
		"VFREEBUSY":           buildCalendarObject(buildVFreeBusy("freebusy-attendees", attendees...)),
	}
	for name, icalData := range tests {
		t.Run(name, func(t *testing.T) {
			req := newCalendarPutRequest("/dav/calendars/1/attendees.ics", strings.NewReader(icalData))
			req = req.WithContext(auth.WithUser(req.Context(), user))
			rr := httptest.NewRecorder()

			h.Put(rr, req)

			assertErrorConditions(t, rr, http.StatusForbidden, calQN("max-attendees-per-instance"))
		})
	}
}

// Section 5.2.8: max-instances Precondition
func TestRFC4791_PutExceedsMaxInstances(t *testing.T) {
	calRepo := &fakeCalendarRepo{
		accessible: []store.CalendarAccess{
			{Calendar: store.Calendar{ID: 1, UserID: 1, Name: "Test"}, Editor: true},
		},
	}
	h := &DavServer{store: &store.Store{Calendars: calRepo, Events: &fakeEventRepo{}}}
	user := &store.User{ID: 1}

	icalData := buildCalendarObject(buildVEvent("too-many",
		"DTSTART:20240101T000000Z", "RRULE:FREQ=DAILY;COUNT=2001"))
	req := newCalendarPutRequest("/dav/calendars/1/too-many.ics", strings.NewReader(icalData))
	req = req.WithContext(auth.WithUser(req.Context(), user))
	rr := httptest.NewRecorder()

	h.Put(rr, req)

	assertErrorConditions(t, rr, http.StatusForbidden, calQN("max-instances"))
}

func TestRFC4791_PutExceedsMaxInstancesLowercaseParams(t *testing.T) {
	calRepo := &fakeCalendarRepo{
		accessible: []store.CalendarAccess{
			{Calendar: store.Calendar{ID: 1, UserID: 1, Name: "Test"}, Editor: true},
		},
	}
	h := &DavServer{store: &store.Store{Calendars: calRepo, Events: &fakeEventRepo{}}}
	user := &store.User{ID: 1}

	icalData := buildCalendarObject(buildVEvent("too-many-lower",
		"DTSTART:20240101T000000Z", "RRULE:freq=daily;count=2001"))
	req := newCalendarPutRequest("/dav/calendars/1/too-many-lower.ics", strings.NewReader(icalData))
	req = req.WithContext(auth.WithUser(req.Context(), user))
	rr := httptest.NewRecorder()

	h.Put(rr, req)

	assertErrorConditions(t, rr, http.StatusForbidden, calQN("max-instances"))
}

func TestRFC4791_PutExceedsMaxInstancesWithoutCount(t *testing.T) {
	calRepo := &fakeCalendarRepo{
		accessible: []store.CalendarAccess{
			{Calendar: store.Calendar{ID: 1, UserID: 1, Name: "Test"}, Editor: true},
		},
	}
	user := &store.User{ID: 1}

	tests := map[string]string{
		"unbounded rule": buildCalendarObject(buildVEvent("unbounded",
			"DTSTART:20240101T000000Z", "RRULE:FREQ=DAILY")),
		"UNTIL beyond limit": buildCalendarObject(buildVEvent("until",
			"DTSTART:20240101T000000Z", "RRULE:FREQ=DAILY;UNTIL=20270101T000000Z")),
	}
	for name, body := range tests {
		t.Run(name, func(t *testing.T) {
			h := &DavServer{store: &store.Store{Calendars: calRepo, Events: &fakeEventRepo{}}}
			req := newCalendarPutRequest("/dav/calendars/1/instances.ics", strings.NewReader(body))
			req = req.WithContext(auth.WithUser(req.Context(), user))
			rr := httptest.NewRecorder()

			h.Put(rr, req)

			assertErrorConditions(t, rr, http.StatusForbidden, calQN("max-instances"))
		})
	}
}

func TestRFC4791_PutExceedsMaxInstancesWithRDates(t *testing.T) {
	calRepo := &fakeCalendarRepo{accessible: []store.CalendarAccess{{
		Calendar: store.Calendar{ID: 1, UserID: 1, Name: "Test"}, Editor: true,
	}}}
	h := &DavServer{store: &store.Store{Calendars: calRepo, Events: &fakeEventRepo{}}}
	user := &store.User{ID: 1}
	start := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	rdates := make([]string, 0, ical.MaxRecurrenceInstances)
	for i := 1; i <= ical.MaxRecurrenceInstances; i++ {
		rdates = append(rdates, start.AddDate(0, 0, i).Format("20060102T150405Z"))
	}
	body := buildCalendarObject(buildVEvent("rdates",
		"DTSTART:"+start.Format("20060102T150405Z"), "RDATE:"+strings.Join(rdates, ",")))
	req := newCalendarPutRequest("/dav/calendars/1/rdates.ics", strings.NewReader(body))
	req = req.WithContext(auth.WithUser(req.Context(), user))
	rr := httptest.NewRecorder()

	h.Put(rr, req)

	assertErrorConditions(t, rr, http.StatusForbidden, calQN("max-instances"))
}

// Section 5.2.10 & 5.3.2.1: PROPPATCH Preconditions
func TestRFC4791_ProppatchOnReadOnlyProperties(t *testing.T) {
	now := store.Now()
	calRepo := &fakeCalendarRepo{
		accessible: []store.CalendarAccess{
			{Calendar: store.Calendar{ID: 1, UserID: 1, Name: "Test", UpdatedAt: now}, Editor: true},
		},
	}
	h := &DavServer{store: &store.Store{Calendars: calRepo, Events: &fakeEventRepo{}}}
	user := &store.User{ID: 1}

	// The settable displayname rides along so the RFC 4918 §9.2 rule that no
	// instruction takes effect when one fails is asserted alongside the
	// protected-property failure itself.
	body := `<?xml version="1.0" encoding="utf-8"?>
<d:propertyupdate xmlns:d="DAV:" xmlns:c="urn:ietf:params:xml:ns:caldav">
  <d:set>
    <d:prop>
      <c:supported-calendar-component-set>
        <c:comp name="VEVENT"/>
      </c:supported-calendar-component-set>
      <d:displayname>Renamed</d:displayname>
    </d:prop>
  </d:set>
</d:propertyupdate>`

	req := httptest.NewRequest("PROPPATCH", "/dav/calendars/1/", strings.NewReader(body))
	req = req.WithContext(auth.WithUser(req.Context(), user))
	rr := httptest.NewRecorder()

	h.Proppatch(rr, req)

	ms := decodeMultistatus(t, rr)
	resp := ms.responseForHref(t, "/dav/calendars/1")
	resp.assertPropstatNames(t, http.StatusForbidden, calQN("supported-calendar-component-set"))
	resp.assertPropstatNames(t, http.StatusFailedDependency, davQN("displayname"))
}

// Section 5.3.2.1: CALDAV:supported-calendar-data Precondition
func TestRFC4791_PutWithUnsupportedMediaType(t *testing.T) {
	calRepo := &fakeCalendarRepo{
		accessible: []store.CalendarAccess{
			{Calendar: store.Calendar{ID: 1, UserID: 1, Name: "Test"}, Editor: true},
		},
	}
	h := &DavServer{store: &store.Store{Calendars: calRepo, Events: &fakeEventRepo{}}}
	user := &store.User{ID: 1}

	// Try to PUT with unsupported content type (e.g., JSON instead of iCalendar)
	jsonData := `{"summary": "Test Event"}`
	req := newCalendarPutRequest("/dav/calendars/1/test.ics", strings.NewReader(jsonData))
	req.Header.Set("Content-Type", "application/json")
	req = req.WithContext(auth.WithUser(req.Context(), user))
	rr := httptest.NewRecorder()

	h.Put(rr, req)

	assertErrorConditions(t, rr, http.StatusForbidden, calQN("supported-calendar-data"))
}

// Section 5.3.2.1: Content-Type is required for calendar object resources
func TestRFC4791_PutWithoutContentTypeRejected(t *testing.T) {
	calRepo := &fakeCalendarRepo{
		accessible: []store.CalendarAccess{
			{Calendar: store.Calendar{ID: 1, UserID: 1, Name: "Test"}, Editor: true},
		},
	}
	h := &DavServer{store: &store.Store{Calendars: calRepo, Events: &fakeEventRepo{}}}
	user := &store.User{ID: 1}

	icalData := buildCalendarObject(buildVEvent("no-ctype", "SUMMARY:No Content-Type"))
	req := httptest.NewRequest(http.MethodPut, "/dav/calendars/1/no-ctype.ics", strings.NewReader(icalData))
	req = req.WithContext(auth.WithUser(req.Context(), user))
	rr := httptest.NewRecorder()

	h.Put(rr, req)

	assertErrorConditions(t, rr, http.StatusForbidden, calQN("supported-calendar-data"))
}

// Section 5.3.2.1: text/plain is not supported calendar data
func TestRFC4791_PutWithTextPlainRejected(t *testing.T) {
	calRepo := &fakeCalendarRepo{
		accessible: []store.CalendarAccess{
			{Calendar: store.Calendar{ID: 1, UserID: 1, Name: "Test"}, Editor: true},
		},
	}
	h := &DavServer{store: &store.Store{Calendars: calRepo, Events: &fakeEventRepo{}}}
	user := &store.User{ID: 1}

	icalData := buildCalendarObject(buildVEvent("plain-text", "SUMMARY:Plain Text"))
	req := newCalendarPutRequest("/dav/calendars/1/plain-text.ics", strings.NewReader(icalData))
	req.Header.Set("Content-Type", "text/plain")
	req = req.WithContext(auth.WithUser(req.Context(), user))
	rr := httptest.NewRecorder()

	h.Put(rr, req)

	assertErrorConditions(t, rr, http.StatusForbidden, calQN("supported-calendar-data"))
}

// Section 5.3.2.1 (CALDAV:supported-calendar-data) and §5.2.4: PUT admits
// exactly the media type CALDAV:supported-calendar-data advertises. Matching the
// header by prefix instead of parsing it lets through types the collection never
// claimed to hold, including a version of iCalendar the server does not parse.
func TestRFC4791_PutMediaTypeMatchesTheAdvertisedCalendarData(t *testing.T) {
	accepted := map[string]string{
		"the advertised type":         "text/calendar",
		"with a charset parameter":    "text/calendar; charset=utf-8",
		"with the advertised version": "text/calendar; version=2.0",
		"case-insensitive type":       "TEXT/Calendar",
		"parameters in either order":  "text/calendar; version=2.0; charset=utf-8",
	}
	refused := map[string]string{
		"an unadvertised version":          "text/calendar; version=1.0",
		"a longer type sharing the prefix": "text/calendarjunk",
		"an unregistered iCalendar type":   "application/ical",
		"the .ics extension as a type":     "application/ics",
		"vCard data":                       "text/vcard",
		"JSON":                             "application/json",
		"an unparseable header":            "text/calendar;;",
	}

	newServer := func() *DavServer {
		return &DavServer{store: &store.Store{
			Calendars: &fakeCalendarRepo{accessible: []store.CalendarAccess{
				{Calendar: store.Calendar{ID: 1, UserID: 1, Name: "Test"}, Editor: true},
			}},
			Events: &fakeEventRepo{events: map[string]*store.Event{}},
		}}
	}
	user := &store.User{ID: 1}
	icalData := buildCalendarObject(buildVEvent("media-type", "SUMMARY:Media Type"))

	for name, contentType := range accepted {
		t.Run("accepted/"+name, func(t *testing.T) {
			req := httptest.NewRequest(http.MethodPut, "/dav/calendars/1/media-type.ics", strings.NewReader(icalData))
			req.Header.Set("Content-Type", contentType)
			req = req.WithContext(auth.WithUser(req.Context(), user))
			rr := httptest.NewRecorder()

			newServer().Put(rr, req)

			if rr.Code != http.StatusCreated {
				t.Errorf("PUT with Content-Type %q = %d, want 201; body: %s", contentType, rr.Code, rr.Body.String())
			}
		})
	}

	for name, contentType := range refused {
		t.Run("refused/"+name, func(t *testing.T) {
			req := httptest.NewRequest(http.MethodPut, "/dav/calendars/1/media-type.ics", strings.NewReader(icalData))
			req.Header.Set("Content-Type", contentType)
			req = req.WithContext(auth.WithUser(req.Context(), user))
			rr := httptest.NewRecorder()

			newServer().Put(rr, req)

			assertErrorConditions(t, rr, http.StatusForbidden, calQN("supported-calendar-data"))
		})
	}
}

// Section 5.3.2.1: CALDAV:supported-calendar-component Precondition
func TestRFC4791_PutWithUnsupportedComponent(t *testing.T) {
	calRepo := &fakeCalendarRepo{
		accessible: []store.CalendarAccess{
			{Calendar: store.Calendar{ID: 1, UserID: 1, Name: "Test"}, Editor: true},
		},
	}
	h := &DavServer{store: &store.Store{Calendars: calRepo, Events: &fakeEventRepo{}}}
	user := &store.User{ID: 1}

	// Try to PUT a mix of supported and unsupported components.
	unsupportedData := buildCalendarObject(
		buildVEvent("event1", "SUMMARY:Test"),
		buildComponent("VAVAILABILITY", "UID:avail1", "DTSTAMP:"+testDTStamp,
			"DTSTART:20240101T000000Z", "DTEND:20240101T235959Z"))
	req := newCalendarPutRequest("/dav/calendars/1/unsupported.ics", strings.NewReader(unsupportedData))
	req = req.WithContext(auth.WithUser(req.Context(), user))
	rr := httptest.NewRecorder()

	h.Put(rr, req)

	assertErrorConditions(t, rr, http.StatusForbidden, calQN("supported-calendar-component"))
}

// Section 5.3.2.1: CALDAV:max-resource-size Precondition
func TestRFC4791_PutExceedsMaxResourceSize(t *testing.T) {
	calRepo := &fakeCalendarRepo{
		accessible: []store.CalendarAccess{
			{Calendar: store.Calendar{ID: 1, UserID: 1, Name: "Test"}, Editor: true},
		},
	}
	h := &DavServer{store: &store.Store{Calendars: calRepo, Events: &fakeEventRepo{}}}
	user := &store.User{ID: 1}

	// Create a very large iCalendar object
	largeDescription := strings.Repeat("A", 1024*1024*15) // 15MB
	largeIcal := buildCalendarObject(buildVEvent("large", "DESCRIPTION:"+largeDescription))

	req := newCalendarPutRequest("/dav/calendars/1/large.ics", strings.NewReader(largeIcal))
	req = req.WithContext(auth.WithUser(req.Context(), user))
	rr := httptest.NewRecorder()

	h.Put(rr, req)

	assertErrorConditions(t, rr, http.StatusForbidden, calQN("max-resource-size"))
}

// Sections 4.1 and 5.3.2.1: METHOD is forbidden in calendar object resources.
func TestRFC4791_ValidCalendarObject_RejectMethodProperty(t *testing.T) {
	calRepo := &fakeCalendarRepo{
		accessible: []store.CalendarAccess{
			{Calendar: store.Calendar{ID: 1, UserID: 1, Name: "Test"}, Editor: true},
		},
	}
	h := &DavServer{store: &store.Store{Calendars: calRepo, Events: &fakeEventRepo{}}}
	user := &store.User{ID: 1}

	icalData := buildCalendarObject("METHOD:REQUEST\r\n",
		buildVEvent("method-reject", "SUMMARY:Method"))
	req := newCalendarPutRequest("/dav/calendars/1/method.ics", strings.NewReader(icalData))
	req = req.WithContext(auth.WithUser(req.Context(), user))
	rr := httptest.NewRecorder()

	h.Put(rr, req)

	assertErrorConditions(t, rr, http.StatusForbidden, calQN("valid-calendar-object-resource"))
}

// Section 4.1: a calendar object resource MUST NOT contain more than one type of
// calendar component, VTIMEZONE excepted. The differing-UID rule from the same
// section is covered by TestRFC4791_PutWithDifferentUIDsRejected.
func TestRFC4791_ValidCalendarObject_RejectMixedComponentTypes(t *testing.T) {
	calRepo := &fakeCalendarRepo{
		accessible: []store.CalendarAccess{
			{Calendar: store.Calendar{ID: 1, UserID: 1, Name: "Test"}, Editor: true},
		},
	}
	h := &DavServer{store: &store.Store{Calendars: calRepo, Events: &fakeEventRepo{}}}
	user := &store.User{ID: 1}

	icalData := buildCalendarObject(buildVEvent("same"), buildVTodo("same"))
	req := newCalendarPutRequest("/dav/calendars/1/mixed.ics", strings.NewReader(icalData))
	req = req.WithContext(auth.WithUser(req.Context(), user))
	rr := httptest.NewRecorder()

	h.Put(rr, req)

	assertErrorConditions(t, rr, http.StatusForbidden, calQN("valid-calendar-object-resource"))
}

func TestRFC4791_ValidCalendarObject_RejectMissingUID(t *testing.T) {
	calRepo := &fakeCalendarRepo{
		accessible: []store.CalendarAccess{
			{Calendar: store.Calendar{ID: 1, UserID: 1, Name: "Test"}, Editor: true},
		},
	}
	h := &DavServer{store: &store.Store{Calendars: calRepo, Events: &fakeEventRepo{}}}
	user := &store.User{ID: 1}

	// Valid but for the missing UID, so the rejection can only be that.
	icalData := buildCalendarObject(buildComponent("VEVENT",
		"DTSTAMP:"+testDTStamp, "DTSTART:"+testDTStart, "SUMMARY:Missing UID"))
	req := newCalendarPutRequest("/dav/calendars/1/missing-uid.ics", strings.NewReader(icalData))
	req = req.WithContext(auth.WithUser(req.Context(), user))
	rr := httptest.NewRecorder()

	h.Put(rr, req)

	assertErrorConditions(t, rr, http.StatusForbidden, calQN("valid-calendar-object-resource"))
}

// Section 4.1: a UID is unique within a calendar collection, so a second
// resource name carrying one already in use fails CALDAV:no-uid-conflict, whose
// DAV:href reports the resource that already holds it (§5.3.2.1).
//
// This is the sequential case. The concurrent one is settled inside
// Store.PutCalendarObject, whose transaction resolves the UID owner and writes
// under the same advisory locks; TestPostgres_ConcurrentPutsOneUIDTwoResources
// exercises it against a live database.
func TestRFC4791_UIDUniqueness_SameUIDMustBeSameResource(t *testing.T) {
	calRepo := &fakeCalendarRepo{
		accessible: []store.CalendarAccess{
			{Calendar: store.Calendar{ID: 1, UserID: 1, Name: "Test"}, Editor: true},
		},
	}
	eventRepo := &fakeEventRepo{events: make(map[string]*store.Event)}
	h := &DavServer{store: &store.Store{Calendars: calRepo, Events: eventRepo}}
	user := &store.User{ID: 1}

	icalData := buildCalendarObject(buildVEvent("dup-uid", "SUMMARY:First"))
	req := newCalendarPutRequest("/dav/calendars/1/first.ics", strings.NewReader(icalData))
	req = req.WithContext(auth.WithUser(req.Context(), user))
	rr := httptest.NewRecorder()
	h.Put(rr, req)

	if rr.Code != http.StatusCreated {
		t.Fatalf("initial PUT to an unmapped resource name = %d, want 201 Created", rr.Code)
	}

	icalData = buildCalendarObject(buildVEvent("dup-uid", "SUMMARY:Second"))
	req = newCalendarPutRequest("/dav/calendars/1/second.ics", strings.NewReader(icalData))
	req = req.WithContext(auth.WithUser(req.Context(), user))
	rr = httptest.NewRecorder()
	h.Put(rr, req)

	assertUIDConflict(t, rr, "/dav/calendars/1/first.ics")
}

// Section 5.3.2.1: an overwrite may not replace a resource with one carrying a
// different UID. The DAV:href names the resource whose UID the request would
// have changed, which is the request target itself.
func TestRFC4791_UpdateDoesNotAllowChangingUID(t *testing.T) {
	calRepo := &fakeCalendarRepo{
		accessible: []store.CalendarAccess{
			{Calendar: store.Calendar{ID: 1, UserID: 1, Name: "Test"}, Editor: true},
		},
	}
	eventRepo := &fakeEventRepo{events: make(map[string]*store.Event)}
	h := &DavServer{store: &store.Store{Calendars: calRepo, Events: eventRepo}}
	user := &store.User{ID: 1}

	icalData := buildCalendarObject(buildVEvent("uid-one", "SUMMARY:Original"))
	req := newCalendarPutRequest("/dav/calendars/1/same.ics", strings.NewReader(icalData))
	req = req.WithContext(auth.WithUser(req.Context(), user))
	rr := httptest.NewRecorder()
	h.Put(rr, req)

	if rr.Code != http.StatusCreated {
		t.Fatalf("initial PUT to an unmapped resource name = %d, want 201 Created", rr.Code)
	}

	icalData = buildCalendarObject(buildVEvent("uid-two", "SUMMARY:Changed"))
	req = newCalendarPutRequest("/dav/calendars/1/same.ics", strings.NewReader(icalData))
	req = req.WithContext(auth.WithUser(req.Context(), user))
	rr = httptest.NewRecorder()
	h.Put(rr, req)

	assertUIDConflict(t, rr, "/dav/calendars/1/same.ics")
}

// Section 5.3.2.1: Multiple different UIDs in a single resource must be rejected
func TestRFC4791_PutWithDifferentUIDsRejected(t *testing.T) {
	calRepo := &fakeCalendarRepo{
		accessible: []store.CalendarAccess{
			{Calendar: store.Calendar{ID: 1, UserID: 1, Name: "Test"}, Editor: true},
		},
	}
	h := &DavServer{store: &store.Store{Calendars: calRepo, Events: &fakeEventRepo{}}}
	user := &store.User{ID: 1}

	// RFC 4791 Section 4.1: Calendar object resource MUST NOT mix different UIDs in one resource
	multiEventData := buildCalendarObject(buildVEvent("event1"), buildVEvent("event2"))
	req := newCalendarPutRequest("/dav/calendars/1/multi.ics", strings.NewReader(multiEventData))
	req = req.WithContext(auth.WithUser(req.Context(), user))
	rr := httptest.NewRecorder()

	h.Put(rr, req)

	assertErrorConditions(t, rr, http.StatusForbidden, calQN("valid-calendar-object-resource"))
}

// Section 4.1: Recurrence set with same UID in a single resource is allowed
func TestRFC4791_PutWithRecurrenceSetSameUIDAccepted(t *testing.T) {
	calRepo := &fakeCalendarRepo{
		accessible: []store.CalendarAccess{
			{Calendar: store.Calendar{ID: 1, UserID: 1, Name: "Test"}, Editor: true},
		},
	}
	h := &DavServer{store: &store.Store{Calendars: calRepo, Events: &fakeEventRepo{}}}
	user := &store.User{ID: 1}

	icalData := buildCalendarObject(
		buildVEvent("recurring", "DTSTART:20240101T100000Z", "RRULE:FREQ=DAILY;COUNT=2"),
		buildVEvent("recurring", "DTSTART:20240102T100000Z",
			"RECURRENCE-ID:20240102T100000Z", "SUMMARY:Override"))
	req := newCalendarPutRequest("/dav/calendars/1/recurring.ics", strings.NewReader(icalData))
	req = req.WithContext(auth.WithUser(req.Context(), user))
	rr := httptest.NewRecorder()

	h.Put(rr, req)

	if rr.Code != http.StatusCreated {
		t.Errorf("RFC 4791 §4.1: PUT of a recurrence set to an unmapped resource name = %d, want 201 Created; body: %s", rr.Code, rr.Body.String())
	}
}

// Section 5.3.2.1: Calendar object resources MUST NOT be created directly in calendar-home
func TestRFC4791_PutCalendarObjectAtCalendarHomeRejected(t *testing.T) {
	calRepo := &fakeCalendarRepo{
		accessible: []store.CalendarAccess{
			{Calendar: store.Calendar{ID: 1, UserID: 1, Name: "Test"}, Editor: true},
		},
	}
	h := &DavServer{store: &store.Store{Calendars: calRepo, Events: &fakeEventRepo{}}}
	user := &store.User{ID: 1}

	icalData := buildCalendarObject(buildVEvent("home-root", "SUMMARY:Home Root"))
	req := newCalendarPutRequest("/dav/calendars/event.ics", strings.NewReader(icalData))
	req.Header.Set("Content-Type", "text/calendar")
	req = req.WithContext(auth.WithUser(req.Context(), user))
	rr := httptest.NewRecorder()

	h.Put(rr, req)

	if rr.Code != http.StatusForbidden {
		t.Errorf("PUT of a calendar object directly into the calendar home = %d, want 403 Forbidden", rr.Code)
	}
}

// Section 7.5: Text Match with Different Collations
func TestRFC4791_TextMatchWithCollation(t *testing.T) {
	calRepo := &fakeCalendarRepo{
		accessible: []store.CalendarAccess{
			{Calendar: store.Calendar{ID: 1, UserID: 1, Name: "Test"}, Editor: true},
		},
	}
	eventRepo := &fakeEventRepo{
		events: map[string]*store.Event{
			"1:event": {
				CalendarID: 1,
				UID:        "event",
				RawICAL:    "BEGIN:VCALENDAR\r\nBEGIN:VEVENT\r\nUID:event\r\nSUMMARY:Cafe Meeting\r\nEND:VEVENT\r\nEND:VCALENDAR\r\n",
				ETag:       "e",
			},
		},
	}
	h := &DavServer{store: &store.Store{Calendars: calRepo, Events: eventRepo}}
	user := &store.User{ID: 1}

	// RFC 4791 Section 7.5: Test with explicit collation
	body := `<?xml version="1.0" encoding="utf-8"?>
<C:calendar-query xmlns:C="urn:ietf:params:xml:ns:caldav" xmlns:D="DAV:">
  <D:prop><D:getetag/></D:prop>
  <C:filter>
    <C:comp-filter name="VCALENDAR">
      <C:comp-filter name="VEVENT">
        <C:prop-filter name="SUMMARY">
          <C:text-match collation="i;ascii-casemap">cafe</C:text-match>
        </C:prop-filter>
      </C:comp-filter>
    </C:comp-filter>
  </C:filter>
</C:calendar-query>`

	req := httptest.NewRequest("REPORT", "/dav/calendars/1/", strings.NewReader(body))
	req.Header.Set("Depth", "1")
	req = req.WithContext(auth.WithUser(req.Context(), user))
	rr := httptest.NewRecorder()

	h.Report(rr, req)

	decodeMultistatus(t, rr).assertHrefs(t, "/dav/calendars/1/event.ics")
}

// Section 5.3.2: a PUT/GET round trip preserves the submitted calendar
// component type and UID.
func TestRFC4791_PutPreservesComponentType(t *testing.T) {
	calRepo := &fakeCalendarRepo{
		accessible: []store.CalendarAccess{
			{Calendar: store.Calendar{ID: 1, UserID: 1, Name: "Test"}, Editor: true},
		},
	}
	eventRepo := &fakeEventRepo{events: make(map[string]*store.Event)}
	h := &DavServer{store: &store.Store{Calendars: calRepo, Events: eventRepo}}
	user := &store.User{ID: 1}

	// Store VTODO
	todoData := buildCalendarObject(buildVTodo("task1", "SUMMARY:Task"))
	req := newCalendarPutRequest("/dav/calendars/1/task1.ics", strings.NewReader(todoData))
	req = req.WithContext(auth.WithUser(req.Context(), user))
	rr := httptest.NewRecorder()

	h.Put(rr, req)

	if rr.Code != http.StatusCreated {
		t.Fatalf("PUT failed: %d", rr.Code)
	}

	// Retrieve and verify it's still VTODO
	req = httptest.NewRequest(http.MethodGet, "/dav/calendars/1/task1.ics", nil)
	req = req.WithContext(auth.WithUser(req.Context(), user))
	rr = httptest.NewRecorder()

	h.Get(rr, req)

	respBody := rr.Body.String()
	if !strings.Contains(respBody, "VTODO") {
		t.Error("PUT/GET round trip did not preserve the VTODO component type")
	}
	if !strings.Contains(respBody, "UID:task1") {
		t.Error("PUT/GET round trip did not preserve the UID")
	}
}

// Section 5.3.1.1 (CALDAV:calendar-collection-location-ok) and §1.3: a
// Request-URI where no calendar collection can be created fails that
// precondition, reported as a child of a top-level DAV:error. §1.3 fixes the
// status at 403 when the request can never succeed and 409 when the user can
// resolve the conflict, which is what separates the cases below.
func TestRFC4791_MkcalendarInvalidPath(t *testing.T) {
	tests := []struct {
		name       string
		path       string
		wantStatus int
	}{
		{
			// The Request-URI is the calendar home collection itself.
			name:       "no collection name",
			path:       "/dav/calendars/",
			wantStatus: http.StatusForbidden,
		},
		{
			// Outside the one namespace that holds calendar collections.
			name:       "outside the calendar home",
			path:       "/dav/addressbooks/work",
			wantStatus: http.StatusForbidden,
		},
		{
			// A numeric last segment belongs to the by-ID namespace.
			name:       "numeric collection name",
			path:       "/dav/calendars/123",
			wantStatus: http.StatusForbidden,
		},
		{
			// The parent collection does not exist, and creating it first would
			// let the request succeed.
			name:       "missing parent collection",
			path:       "/dav/calendars/missing/work",
			wantStatus: http.StatusConflict,
		},
		{
			// Numeric path parsing alone does not prove the parent exists.
			name:       "missing numeric parent collection",
			path:       "/dav/calendars/999/work",
			wantStatus: http.StatusConflict,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			calRepo := &fakeCalendarRepo{calendars: make(map[int64]*store.Calendar)}
			h := &DavServer{store: &store.Store{Calendars: calRepo}}
			user := &store.User{ID: 1}

			req := httptest.NewRequest("MKCALENDAR", tc.path, nil)
			req = req.WithContext(auth.WithUser(req.Context(), user))
			rr := httptest.NewRecorder()

			h.Mkcalendar(rr, req)

			assertErrorConditions(t, rr, tc.wantStatus, calQN("calendar-collection-location-ok"))
			if len(calRepo.calendars) != 0 {
				t.Errorf("MKCALENDAR at %s created %d calendars, want none", tc.path, len(calRepo.calendars))
			}
		})
	}
}

// --- Discovery, calendar collection properties, and MKCALENDAR ------------

// newCalendarPropfind builds a PROPFIND request naming exactly the properties
// given as prefixed wire names, so the CalDAV properties RFC 4791 keeps out of
// DAV:allprop can still be observed.
func newCalendarPropfind(t *testing.T, user *store.User, href string, properties ...string) *http.Request {
	t.Helper()
	var b strings.Builder
	b.WriteString(`<?xml version="1.0" encoding="utf-8"?>` + "\n")
	b.WriteString(`<d:propfind xmlns:d="DAV:" xmlns:cal="urn:ietf:params:xml:ns:caldav" xmlns:card="urn:ietf:params:xml:ns:carddav"><d:prop>`)
	for _, property := range properties {
		b.WriteString("<" + property + "/>")
	}
	b.WriteString(`</d:prop></d:propfind>`)
	req := httptest.NewRequest("PROPFIND", href, strings.NewReader(b.String()))
	req.Header.Set("Depth", "0")
	return req.WithContext(auth.WithUser(req.Context(), user))
}

// calendarPropertyServer returns a server holding one writable calendar at
// /dav/calendars/1/ built from cal, with cal's identity fields filled in.
func calendarPropertyServer(cal store.Calendar) (*DavServer, *fakeCalendarRepo, *store.User) {
	if cal.ID == 0 {
		cal.ID = 1
	}
	cal.UserID = 1
	if cal.Name == "" {
		cal.Name = "Test"
	}
	if cal.UpdatedAt.IsZero() {
		cal.UpdatedAt = store.Now()
	}
	calRepo := &fakeCalendarRepo{
		accessible: []store.CalendarAccess{{Calendar: cal, Editor: true}},
		calendars:  map[int64]*store.Calendar{cal.ID: &cal},
	}
	return &DavServer{store: &store.Store{Calendars: calRepo, Events: &fakeEventRepo{}}}, calRepo, &store.User{ID: 1}
}

// allpropCalendarResponse runs a DAV:allprop PROPFIND against the calendar
// collection and returns its response.
func allpropCalendarResponse(t *testing.T, h *DavServer, user *store.User) davResponse {
	t.Helper()
	req := httptest.NewRequest("PROPFIND", "/dav/calendars/1/", nil)
	req.Header.Set("Depth", "0")
	req = req.WithContext(auth.WithUser(req.Context(), user))
	rr := httptest.NewRecorder()
	h.Propfind(rr, req)
	return decodeMultistatus(t, rr).responseForHref(t, "/dav/calendars/1/")
}

// Sections 5.2.1 through 5.2.9, 6.2.1 and 7.5.1: each of these properties
// carries a SHOULD NOT against being returned by DAV:allprop. The rule is
// absence, not a 404 -- allprop reports the properties the server chooses to
// expose that way, so an excluded one is simply not in the response at all.
func TestRFC4791_CalendarCollection_AllpropExcludesCalDAVProperties(t *testing.T) {
	description := "Personal events"
	h, _, user := calendarPropertyServer(store.Calendar{Description: &description})
	resp := allpropCalendarResponse(t, h, user)

	excluded := []xml.Name{
		calQN("calendar-description"),
		calQN("calendar-timezone"),
		calQN("supported-calendar-component-set"),
		calQN("supported-calendar-data"),
		calQN("supported-collation-set"),
		calQN("calendar-home-set"),
		calQN("max-resource-size"),
		calQN("min-date-time"),
		calQN("max-date-time"),
		calQN("max-instances"),
		calQN("max-attendees-per-instance"),
	}
	for _, name := range excluded {
		resp.assertPropAbsent(t, name)
	}

	// The response is still a real allprop response rather than an empty one, so
	// the assertions above measure exclusion and not a broken request.
	resp.assertPropStatus(t, davQN("resourcetype"), http.StatusOK)
	resp.assertPropValue(t, davQN("displayname"), http.StatusOK, "Test")
}

// Section 5.2.1: a defined CALDAV:calendar-description carries the xml:lang it
// was set with. RFC 4918 §4.3 ties the language to the value, so a PROPPATCH
// that declares one is answered by a PROPFIND that returns it.
func TestRFC4791_CalendarDescriptionRoundTripsXMLLang(t *testing.T) {
	h, _, user := calendarPropertyServer(store.Calendar{})

	patch := `<?xml version="1.0" encoding="utf-8"?>
<d:propertyupdate xmlns:d="DAV:" xmlns:cal="urn:ietf:params:xml:ns:caldav">
  <d:set>
    <d:prop>
      <cal:calendar-description xml:lang="fr-CA">Réunions</cal:calendar-description>
    </d:prop>
  </d:set>
</d:propertyupdate>`
	req := httptest.NewRequest("PROPPATCH", "/dav/calendars/1/", strings.NewReader(patch))
	req = req.WithContext(auth.WithUser(req.Context(), user))
	rr := httptest.NewRecorder()
	h.Proppatch(rr, req)

	decodeMultistatus(t, rr).
		responseForHref(t, "/dav/calendars/1").
		assertPropstatNames(t, http.StatusOK, calQN("calendar-description"))

	rr = httptest.NewRecorder()
	h.Propfind(rr, newCalendarPropfind(t, user, "/dav/calendars/1/", "cal:calendar-description"))

	description := decodeMultistatus(t, rr).
		responseForHref(t, "/dav/calendars/1/").
		assertPropStatus(t, calQN("calendar-description"), http.StatusOK)
	if got := description.Value(); got != "Réunions" {
		t.Errorf("calendar-description = %q, want %q", got, "Réunions")
	}
	if got := description.attr("lang"); got != "" {
		t.Errorf(`unqualified attr("lang") = %q, want the attribute to be namespaced`, got)
	}
	lang := ""
	for _, attr := range description.Attr {
		if attr.Name.Local == "lang" && attr.Name.Space == xmlNamespaceURI {
			lang = attr.Value
		}
	}
	if lang != "fr-CA" {
		t.Errorf("calendar-description xml:lang = %q, want %q", lang, "fr-CA")
	}
}

// Section 5.2.1: a description set with no xml:lang comes back with none, so the
// attribute is not invented.
func TestRFC4791_CalendarDescriptionWithoutXMLLangCarriesNoAttribute(t *testing.T) {
	description := "Personal events"
	h, _, user := calendarPropertyServer(store.Calendar{Description: &description})

	rr := httptest.NewRecorder()
	h.Propfind(rr, newCalendarPropfind(t, user, "/dav/calendars/1/", "cal:calendar-description"))

	element := decodeMultistatus(t, rr).
		responseForHref(t, "/dav/calendars/1/").
		assertPropStatus(t, calQN("calendar-description"), http.StatusOK)
	for _, attr := range element.Attr {
		if attr.Name.Local == "lang" {
			t.Errorf("calendar-description carries xml:lang %q, want none", attr.Value)
		}
	}
}

// Sections 5.2.3, 5.2.4, 5.2.5 through 5.2.9 and 7.5.1: every one of these
// properties is protected, so a PROPPATCH naming it fails. RFC 4918 §9.2 also
// makes every other instruction in the request fail with 424, which the
// settable displayname riding along proves.
func TestRFC4791_ProtectedCalendarPropertiesRejectProppatch(t *testing.T) {
	protected := []struct {
		name    xml.Name
		element string
	}{
		{calQN("supported-calendar-component-set"), `<cal:supported-calendar-component-set><cal:comp name="VEVENT"/></cal:supported-calendar-component-set>`},
		{calQN("supported-calendar-data"), `<cal:supported-calendar-data><cal:calendar-data content-type="text/calendar" version="2.0"/></cal:supported-calendar-data>`},
		{calQN("supported-collation-set"), `<cal:supported-collation-set><cal:supported-collation>i;octet</cal:supported-collation></cal:supported-collation-set>`},
		{calQN("max-resource-size"), `<cal:max-resource-size>1024</cal:max-resource-size>`},
		{calQN("min-date-time"), `<cal:min-date-time>19700101T000000Z</cal:min-date-time>`},
		{calQN("max-date-time"), `<cal:max-date-time>20991231T235959Z</cal:max-date-time>`},
		{calQN("max-instances"), `<cal:max-instances>10</cal:max-instances>`},
		{calQN("max-attendees-per-instance"), `<cal:max-attendees-per-instance>5</cal:max-attendees-per-instance>`},
		// §9.6 makes calendar-data a REPORT selector rather than a WebDAV
		// property, so PROPPATCH must not accept it either.
		{calQN("calendar-data"), `<cal:calendar-data>BEGIN:VCALENDAR
END:VCALENDAR</cal:calendar-data>`},
	}

	for _, tc := range protected {
		t.Run(tc.name.Local, func(t *testing.T) {
			h, calRepo, user := calendarPropertyServer(store.Calendar{})

			body := `<?xml version="1.0" encoding="utf-8"?>
<d:propertyupdate xmlns:d="DAV:" xmlns:cal="urn:ietf:params:xml:ns:caldav">
  <d:set>
    <d:prop>` + tc.element + `<d:displayname>Renamed</d:displayname></d:prop>
  </d:set>
</d:propertyupdate>`
			req := httptest.NewRequest("PROPPATCH", "/dav/calendars/1/", strings.NewReader(body))
			req = req.WithContext(auth.WithUser(req.Context(), user))
			rr := httptest.NewRecorder()

			h.Proppatch(rr, req)

			resp := decodeMultistatus(t, rr).responseForHref(t, "/dav/calendars/1")
			resp.assertPropstatNames(t, http.StatusForbidden, tc.name)
			resp.assertPropstatNames(t, http.StatusFailedDependency, davQN("displayname"))
			if got := calRepo.calendars[1].Name; got != "Test" {
				t.Errorf("displayname was applied (%q) despite the failed instruction", got)
			}
		})
	}
}

// Section 7.5.1 and §9.4: CALDAV:supported-collation-set is defined wherever a
// text-matching report is supported -- for CalCard the calendar collection,
// which serves calendar-query -- and carries CALDAV:supported-collation text
// children each naming one collation identifier.
func TestRFC4791_SupportedCollationSetAdvertisedOnCalendarCollection(t *testing.T) {
	h, _, user := calendarPropertyServer(store.Calendar{})

	rr := httptest.NewRecorder()
	h.Propfind(rr, newCalendarPropfind(t, user, "/dav/calendars/1/", "cal:supported-collation-set"))

	set := decodeMultistatus(t, rr).
		responseForHref(t, "/dav/calendars/1/").
		assertPropStatus(t, calQN("supported-collation-set"), http.StatusOK)

	var got []string
	for _, child := range set.Children {
		if child.Name != calQN("supported-collation") {
			t.Fatalf("supported-collation-set child = %s, want %s", qnString(child.Name), qnString(calQN("supported-collation")))
		}
		if len(child.Children) != 0 {
			t.Errorf("supported-collation carries child elements %s, want character data only", qnList(child.childNames()))
		}
		got = append(got, child.Value())
	}
	if len(got) == 0 {
		t.Fatal("supported-collation-set carries no CALDAV:supported-collation children")
	}
	// Every advertised collation must be one the text matcher actually applies,
	// so the property never promises matching semantics the server lacks.
	for _, collation := range got {
		if !slices.Contains(supportedCalendarCollations, collation) {
			t.Errorf("advertised collation %q is not one the matcher implements (%v)", collation, supportedCalendarCollations)
		}
	}
	if len(got) != len(supportedCalendarCollations) {
		t.Errorf("supported-collation-set = %v, want %v", got, supportedCalendarCollations)
	}
}

// Section 7.5.1: CALDAV:supported-collation-set is defined on every resource
// that supports a report doing text matching. §7 makes calendar-query available
// on calendar object resources, so a client that reaches one must be able to
// discover the collations it will match under -- on ordinary objects and on the
// virtual birthday collection alike.
func TestRFC4791_SupportedCollationSetAdvertisedOnCalendarObjectResources(t *testing.T) {
	assertAdvertised := func(t *testing.T, rr *httptest.ResponseRecorder, href string) {
		t.Helper()
		set := decodeMultistatus(t, rr).
			responseForHref(t, href).
			assertPropStatus(t, calQN("supported-collation-set"), http.StatusOK)

		var got []string
		for _, child := range set.Children {
			if child.Name != calQN("supported-collation") {
				t.Fatalf("supported-collation-set child = %s, want %s", qnString(child.Name), qnString(calQN("supported-collation")))
			}
			got = append(got, child.Value())
		}
		if !slices.Equal(got, supportedCalendarCollations) {
			t.Errorf("supported-collation-set on %s = %v, want %v", href, got, supportedCalendarCollations)
		}
	}

	t.Run("ordinary calendar object", func(t *testing.T) {
		h, _, user := calendarPropertyServer(store.Calendar{})
		h.store.Events = &fakeEventRepo{events: map[string]*store.Event{
			"1:standup": {CalendarID: 1, UID: "standup", ResourceName: "standup", ETag: "etag",
				RawICAL: "BEGIN:VCALENDAR\r\nBEGIN:VEVENT\r\nUID:standup\r\nSUMMARY:Standup\r\nEND:VEVENT\r\nEND:VCALENDAR\r\n"},
		}}

		rr := httptest.NewRecorder()
		h.Propfind(rr, newCalendarPropfind(t, user, "/dav/calendars/1/standup.ics", "cal:supported-collation-set"))

		assertAdvertised(t, rr, "/dav/calendars/1/standup.ics")
	})

	// The birthday collection's objects are generated per request, so a client
	// reaches them by listing the collection rather than by naming one.
	t.Run("virtual birthday object", func(t *testing.T) {
		birthday := time.Date(1990, 5, 15, 0, 0, 0, 0, time.UTC)
		displayName := "Alice Example"
		h := &DavServer{store: &store.Store{
			Calendars: &fakeCalendarRepo{},
			Events:    &fakeEventRepo{},
			Contacts: &fakeContactRepo{contacts: map[string]*store.Contact{
				"5:alice": {AddressBookID: 5, UID: "alice", DisplayName: &displayName, Birthday: &birthday},
			}},
		}}
		user := &store.User{ID: 1}

		collection := fmt.Sprintf("/dav/calendars/%d/", birthdayCalendarID)
		req := newCalendarPropfind(t, user, collection, "cal:supported-collation-set")
		req.Header.Set("Depth", "1")
		rr := httptest.NewRecorder()
		h.Propfind(rr, req)

		assertAdvertised(t, rr, fmt.Sprintf("/dav/calendars/%d/birthday-alice@calcard.ics", birthdayCalendarID))
	})
}

// Section 7.5.1: the property carries a SHOULD NOT for DAV:allprop, and that
// applies wherever it is defined -- including the calendar object resources it
// is now served on.
func TestRFC4791_AllpropOmitsSupportedCollationSetOnCalendarObjects(t *testing.T) {
	h, _, user := calendarPropertyServer(store.Calendar{})
	h.store.Events = &fakeEventRepo{events: map[string]*store.Event{
		"1:standup": {CalendarID: 1, UID: "standup", ResourceName: "standup", ETag: "etag",
			RawICAL: "BEGIN:VCALENDAR\r\nBEGIN:VEVENT\r\nUID:standup\r\nSUMMARY:Standup\r\nEND:VEVENT\r\nEND:VCALENDAR\r\n"},
	}}

	req := httptest.NewRequest("PROPFIND", "/dav/calendars/1/standup.ics", nil)
	req.Header.Set("Depth", "0")
	req = req.WithContext(auth.WithUser(req.Context(), user))
	rr := httptest.NewRecorder()
	h.Propfind(rr, req)

	decodeMultistatus(t, rr).
		responseForHref(t, "/dav/calendars/1/standup.ics").
		assertPropAbsent(t, calQN("supported-collation-set"))
}

// Section 5.2.3: CALDAV:comp name="VTIMEZONE" is advertised only by a server
// that stores VTIMEZONE-only calendar object resources. CalCard does not, so it
// must not appear in the advertised set.
func TestRFC4791_SupportedCalendarComponentSetOmitsVTimezone(t *testing.T) {
	h, _, user := calendarPropertyServer(store.Calendar{})

	rr := httptest.NewRecorder()
	h.Propfind(rr, newCalendarPropfind(t, user, "/dav/calendars/1/", "cal:supported-calendar-component-set"))

	set := decodeMultistatus(t, rr).
		responseForHref(t, "/dav/calendars/1/").
		assertPropStatus(t, calQN("supported-calendar-component-set"), http.StatusOK)
	for _, child := range set.Children {
		if child.attr("name") == "VTIMEZONE" {
			t.Error("supported-calendar-component-set advertises VTIMEZONE, which CalCard cannot store as a calendar object resource")
		}
	}

	// A VTIMEZONE-only object is refused, which is what makes the omission true.
	// Only the refusal is asserted: which precondition names it, and the §1.3
	// status that precondition carries, are the PUT validation rows.
	timezoneOnly := "BEGIN:VCALENDAR\r\nVERSION:2.0\r\nPRODID:" + testProdID + "\r\n" +
		"BEGIN:VTIMEZONE\r\nTZID:UTC\r\nEND:VTIMEZONE\r\nEND:VCALENDAR\r\n"
	req := newCalendarPutRequest("/dav/calendars/1/tz.ics", strings.NewReader(timezoneOnly))
	req = req.WithContext(auth.WithUser(req.Context(), user))
	rr = httptest.NewRecorder()
	h.Put(rr, req)
	if rr.Code < 400 {
		t.Errorf("PUT of a VTIMEZONE-only resource = %d, want a refusal; body: %s", rr.Code, rr.Body.String())
	}

	rr = httptest.NewRecorder()
	getReq := httptest.NewRequest(http.MethodGet, "/dav/calendars/1/tz.ics", nil)
	getReq = getReq.WithContext(auth.WithUser(getReq.Context(), user))
	h.Get(rr, getReq)
	if rr.Code != http.StatusNotFound {
		t.Errorf("GET after the refused PUT = %d, want 404: the resource must not have been stored", rr.Code)
	}
}

// Section 5.2.3: with no CALDAV:supported-calendar-component-set of its own, a
// collection accepts every component type the server implements.
func TestRFC4791_CollectionWithoutComponentSetAcceptsEveryComponentType(t *testing.T) {
	if got := calendarSupportedComponents(nil); !slices.Equal(got, defaultSupportedCalendarComponents) {
		t.Fatalf("calendarSupportedComponents(nil) = %v, want the server default %v", got, defaultSupportedCalendarComponents)
	}

	objects := map[string]string{
		"VEVENT":    buildCalendarObject(buildVEvent("absent-set-event")),
		"VTODO":     buildCalendarObject(buildVTodo("absent-set-todo")),
		"VJOURNAL":  buildCalendarObject(buildVJournal("absent-set-journal")),
		"VFREEBUSY": buildCalendarObject(buildVFreeBusy("absent-set-freebusy")),
	}
	for component, data := range objects {
		t.Run(component, func(t *testing.T) {
			// SupportedComponents is nil, so the collection defines no set.
			h, _, user := calendarPropertyServer(store.Calendar{})
			req := newCalendarPutRequest("/dav/calendars/1/"+strings.ToLower(component)+".ics", strings.NewReader(data))
			req = req.WithContext(auth.WithUser(req.Context(), user))
			rr := httptest.NewRecorder()

			h.Put(rr, req)

			if rr.Code != http.StatusCreated {
				t.Errorf("PUT of a %s object into a collection defining no component set = %d, want 201; body: %s", component, rr.Code, rr.Body.String())
			}
		})
	}
}

// Sections 5.2.3 and 5.3.2.1: a collection that restricts itself to one
// component type advertises that set and refuses every other type with
// CALDAV:supported-calendar-component, for the per-collection set MKCALENDAR
// establishes.
func TestRFC4791_CollectionComponentSetIsEnforcedOnPut(t *testing.T) {
	h, _, user := calendarPropertyServer(store.Calendar{SupportedComponents: []string{"VTODO"}})

	rr := httptest.NewRecorder()
	h.Propfind(rr, newCalendarPropfind(t, user, "/dav/calendars/1/", "cal:supported-calendar-component-set"))
	decodeMultistatus(t, rr).
		responseForHref(t, "/dav/calendars/1/").
		assertSupportedComponents(t, "VTODO")

	req := newCalendarPutRequest("/dav/calendars/1/task.ics", strings.NewReader(buildCalendarObject(buildVTodo("restricted-todo"))))
	req = req.WithContext(auth.WithUser(req.Context(), user))
	rr = httptest.NewRecorder()
	h.Put(rr, req)
	if rr.Code != http.StatusCreated {
		t.Errorf("PUT of an advertised component type = %d, want 201; body: %s", rr.Code, rr.Body.String())
	}

	req = newCalendarPutRequest("/dav/calendars/1/meeting.ics", strings.NewReader(buildCalendarObject(buildVEvent("restricted-event"))))
	req = req.WithContext(auth.WithUser(req.Context(), user))
	rr = httptest.NewRecorder()
	h.Put(rr, req)
	assertErrorConditions(t, rr, http.StatusForbidden, calQN("supported-calendar-component"))
}

// Section 5.2.4: with no CALDAV:supported-calendar-data of its own a collection
// accepts only text/calendar version 2.0, which is exactly the single pair
// CalCard advertises everywhere.
func TestRFC4791_SupportedCalendarDataIsTextCalendarVersionTwo(t *testing.T) {
	h, _, user := calendarPropertyServer(store.Calendar{})

	rr := httptest.NewRecorder()
	h.Propfind(rr, newCalendarPropfind(t, user, "/dav/calendars/1/", "cal:supported-calendar-data"))

	advertised := decodeMultistatus(t, rr).
		responseForHref(t, "/dav/calendars/1/").
		assertPropStatus(t, calQN("supported-calendar-data"), http.StatusOK)
	child := assertSoleChild(t, advertised, calQN("calendar-data"))
	if got, want := child.attr("content-type"), "text/calendar"; got != want {
		t.Errorf("supported-calendar-data content-type = %q, want %q", got, want)
	}
	if got, want := child.attr("version"), "2.0"; got != want {
		t.Errorf("supported-calendar-data version = %q, want %q", got, want)
	}

	// The advertised pair is the one accepted, and nothing else is.
	rejected := map[string]string{
		"text/vcard":       "text/vcard; charset=utf-8",
		"application/json": "application/json",
	}
	for name, contentType := range rejected {
		t.Run(name, func(t *testing.T) {
			req := httptest.NewRequest(http.MethodPut, "/dav/calendars/1/x.ics", strings.NewReader(buildCalendarObject(buildVEvent("media-type"))))
			req.Header.Set("Content-Type", contentType)
			req = req.WithContext(auth.WithUser(req.Context(), user))
			rr := httptest.NewRecorder()
			h.Put(rr, req)
			assertErrorConditions(t, rr, http.StatusForbidden, calQN("supported-calendar-data"))
		})
	}
}

// Section 5.2.10: a PROPPATCH of CALDAV:calendar-timezone requires a valid
// iCalendar object containing exactly one VTIMEZONE.
func TestRFC4791_ProppatchCalendarTimezoneRequiresOneVTimezone(t *testing.T) {
	valid := "BEGIN:VCALENDAR\nVERSION:2.0\nPRODID:" + testProdID +
		"\nBEGIN:VTIMEZONE\nTZID:America/Chicago\nBEGIN:STANDARD\nDTSTART:19701101T020000\nTZOFFSETFROM:-0500\nTZOFFSETTO:-0600\nEND:STANDARD\nEND:VTIMEZONE\nEND:VCALENDAR"

	tests := []struct {
		name       string
		value      string
		wantStatus int
	}{
		{name: "valid object", value: valid, wantStatus: http.StatusOK},
		{
			name:       "bare component is not an iCalendar object",
			value:      "BEGIN:VTIMEZONE\nTZID:America/Chicago\nEND:VTIMEZONE",
			wantStatus: http.StatusConflict,
		},
		{
			name:       "two VTIMEZONE components",
			value:      "BEGIN:VCALENDAR\nVERSION:2.0\nBEGIN:VTIMEZONE\nTZID:A\nEND:VTIMEZONE\nBEGIN:VTIMEZONE\nTZID:B\nEND:VTIMEZONE\nEND:VCALENDAR",
			wantStatus: http.StatusConflict,
		},
		{
			name:       "no VTIMEZONE at all",
			value:      "BEGIN:VCALENDAR\nVERSION:2.0\nEND:VCALENDAR",
			wantStatus: http.StatusConflict,
		},
		// The value is stored verbatim and served back, so an envelope missing
		// what RFC 5545 §3.6 requires, or a VTIMEZONE missing what §3.6.5
		// requires, would hand clients an object they cannot parse.
		{
			name:       "envelope without PRODID",
			value:      strings.Replace(valid, "PRODID:"+testProdID+"\n", "", 1),
			wantStatus: http.StatusConflict,
		},
		{
			name:       "envelope naming another iCalendar version",
			value:      strings.Replace(valid, "VERSION:2.0", "VERSION:1.0", 1),
			wantStatus: http.StatusConflict,
		},
		{
			name: "VTIMEZONE with no observance",
			value: "BEGIN:VCALENDAR\nVERSION:2.0\nPRODID:" + testProdID +
				"\nBEGIN:VTIMEZONE\nTZID:America/Chicago\nEND:VTIMEZONE\nEND:VCALENDAR",
			wantStatus: http.StatusConflict,
		},
		{
			name:       "observance missing its offsets",
			value:      strings.Replace(valid, "TZOFFSETFROM:-0500\n", "", 1),
			wantStatus: http.StatusConflict,
		},
		{
			name:       "an unknown sub-component inside the VTIMEZONE",
			value:      strings.Replace(valid, "END:VTIMEZONE", "BEGIN:VALARM\nACTION:DISPLAY\nEND:VALARM\nEND:VTIMEZONE", 1),
			wantStatus: http.StatusConflict,
		},
		{
			name:       "malformed content",
			value:      "this is not a calendar",
			wantStatus: http.StatusConflict,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			h, _, user := calendarPropertyServer(store.Calendar{})
			body := `<?xml version="1.0" encoding="utf-8"?>
<d:propertyupdate xmlns:d="DAV:" xmlns:cal="urn:ietf:params:xml:ns:caldav">
  <d:set><d:prop><cal:calendar-timezone>` + tc.value + `</cal:calendar-timezone></d:prop></d:set>
</d:propertyupdate>`
			req := httptest.NewRequest("PROPPATCH", "/dav/calendars/1/", strings.NewReader(body))
			req = req.WithContext(auth.WithUser(req.Context(), user))
			rr := httptest.NewRecorder()

			h.Proppatch(rr, req)

			decodeMultistatus(t, rr).
				responseForHref(t, "/dav/calendars/1").
				assertPropstatNames(t, tc.wantStatus, calQN("calendar-timezone"))
		})
	}
}

// Section 9.6: CALDAV:calendar-data is a REPORT selector, not a WebDAV
// property, so PROPFIND never returns its value. The PROPPATCH half is covered
// by TestRFC4791_ProtectedCalendarPropertiesRejectProppatch.
func TestRFC4791_CalendarDataIsNotReturnedByPropfind(t *testing.T) {
	calRepo := &fakeCalendarRepo{
		accessible: []store.CalendarAccess{
			{Calendar: store.Calendar{ID: 1, UserID: 1, Name: "Test", UpdatedAt: store.Now()}, Editor: true},
		},
	}
	eventRepo := &fakeEventRepo{events: map[string]*store.Event{
		"1:event": {CalendarID: 1, UID: "event", ResourceName: "event", RawICAL: buildCalendarObject(buildVEvent("event")), ETag: "e"},
	}}
	h := &DavServer{store: &store.Store{Calendars: calRepo, Events: eventRepo}}
	user := &store.User{ID: 1}

	rr := httptest.NewRecorder()
	h.Propfind(rr, newCalendarPropfind(t, user, "/dav/calendars/1/event.ics", "cal:calendar-data", "d:getetag"))

	resp := decodeMultistatus(t, rr).responseForHref(t, "/dav/calendars/1/event.ics")
	resp.assertPropstatNames(t, http.StatusNotFound, calQN("calendar-data"))
	resp.assertPropStatus(t, davQN("getetag"), http.StatusOK)

	// An allprop request must not smuggle it back in either.
	req := httptest.NewRequest("PROPFIND", "/dav/calendars/1/event.ics", nil)
	req.Header.Set("Depth", "0")
	req = req.WithContext(auth.WithUser(req.Context(), user))
	rr = httptest.NewRecorder()
	h.Propfind(rr, req)
	decodeMultistatus(t, rr).
		responseForHref(t, "/dav/calendars/1/event.ics").
		assertPropAbsent(t, calQN("calendar-data"))
}

// Section 7: the calendaring reports are advertised in DAV:supported-report-set
// on calendar object resources, not only on calendar collections, and every
// report advertised there works. free-busy-query is absent because §7.10 makes
// it a 403 on an object resource, and sync-collection is a collection
// report.
func TestRFC4791_CalendarObjectResourceSupportedReportSet(t *testing.T) {
	start := time.Date(2024, 6, 1, 10, 0, 0, 0, time.UTC)
	end := time.Date(2024, 6, 1, 12, 0, 0, 0, time.UTC)
	newServer := func() (*DavServer, *store.User) {
		calRepo := &fakeCalendarRepo{
			accessible: []store.CalendarAccess{
				{Calendar: store.Calendar{ID: 1, UserID: 1, Name: "Test", UpdatedAt: store.Now()}, Editor: true},
			},
		}
		eventRepo := &fakeEventRepo{events: map[string]*store.Event{
			"1:event": {
				CalendarID:   1,
				UID:          "event",
				ResourceName: "event",
				RawICAL:      buildCalendarObject(buildVEvent("event", "SUMMARY:Standup")),
				ETag:         "e",
				DTStart:      &start,
				DTEnd:        &end,
			},
		}}
		user := &store.User{ID: 1, PrimaryEmail: "owner@example.com"}
		return &DavServer{store: &store.Store{
			Calendars: calRepo,
			Events:    eventRepo,
			Users:     &aclReportUserRepo{users: map[int64]store.User{user.ID: *user}},
		}}, user
	}

	// Each advertised report is paired with a body targeting the object resource
	// and the status a working implementation returns, so a report that gains an
	// advertisement without gaining an entry fails the exactness check below.
	cases := map[xml.Name]string{
		calQN("calendar-query"): `<C:calendar-query xmlns:D="DAV:" xmlns:C="urn:ietf:params:xml:ns:caldav">` +
			`<D:prop><D:getetag/></D:prop><C:filter><C:comp-filter name="VCALENDAR">` +
			`<C:comp-filter name="VEVENT"/></C:comp-filter></C:filter></C:calendar-query>`,
		calQN("calendar-multiget"): `<C:calendar-multiget xmlns:D="DAV:" xmlns:C="urn:ietf:params:xml:ns:caldav">` +
			`<D:prop><D:getetag/></D:prop><D:href>/dav/calendars/1/event.ics</D:href></C:calendar-multiget>`,
		davQN("expand-property"): `<D:expand-property xmlns:D="DAV:">` +
			`<D:property name="current-user-principal" namespace="DAV:">` +
			`<D:property name="displayname" namespace="DAV:"/>` +
			`</D:property></D:expand-property>`,
		davQN("acl-principal-prop-set"): `<D:acl-principal-prop-set xmlns:D="DAV:">` +
			`<D:prop><D:displayname/></D:prop></D:acl-principal-prop-set>`,
		davQN("principal-property-search"): `<D:principal-property-search xmlns:D="DAV:"><D:property-search>` +
			`<D:prop><D:displayname/></D:prop><D:match>owner</D:match></D:property-search>` +
			`<D:apply-to-principal-collection-set/></D:principal-property-search>`,
	}

	h, user := newServer()
	rr := httptest.NewRecorder()
	h.Propfind(rr, newCalendarPropfind(t, user, "/dav/calendars/1/event.ics", "d:supported-report-set"))
	resp := decodeMultistatus(t, rr).responseForHref(t, "/dav/calendars/1/event.ics")
	advertised := resp.supportedReports(t)

	known := make([]xml.Name, 0, len(cases))
	for name := range cases {
		known = append(known, name)
	}
	if qnList(advertised) != qnList(known) {
		t.Fatalf("calendar object supported-report-set advertises %s, but this test can exercise %s", qnList(advertised), qnList(known))
	}

	for _, name := range advertised {
		t.Run(name.Local, func(t *testing.T) {
			h, user := newServer()
			req := httptest.NewRequest("REPORT", "/dav/calendars/1/event.ics", strings.NewReader(cases[name]))
			req = req.WithContext(auth.WithUser(req.Context(), user))
			rr := httptest.NewRecorder()

			h.Report(rr, req)

			if rr.Code != http.StatusMultiStatus {
				t.Fatalf("%s on a calendar object resource = %d, want 207; body: %s", qnString(name), rr.Code, rr.Body.String())
			}
			ms := decodeMultistatus(t, rr)
			if name == davQN("acl-principal-prop-set") {
				ms.assertHrefs(t, "/dav/principals/1/")
				ms.responseForHref(t, "/dav/principals/1/").
					assertPropStatus(t, davQN("displayname"), http.StatusOK)
				return
			}
			if name == davQN("principal-property-search") {
				ms.assertHrefs(t, "/dav/principals/1/")
				return
			}
			if name == davQN("expand-property") {
				ms.assertHrefs(t, "/dav/calendars/1/event.ics")
				ms.responseForHref(t, "/dav/calendars/1/event.ics").
					assertPropstatNames(t, http.StatusNotFound, davQN("current-user-principal"))
				return
			}
			ms.assertHrefs(t, "/dav/calendars/1/event.ics")
			ms.responseForHref(t, "/dav/calendars/1/event.ics").
				assertPropStatus(t, davQN("getetag"), http.StatusOK)
		})
	}
}

// Section 7.9: a calendar-multiget run against a calendar object resource has
// exactly one DAV:href, and that href identifies the Request-URI.
func TestRFC4791_CalendarMultigetOnObjectResourceIsScopedToIt(t *testing.T) {
	calRepo := &fakeCalendarRepo{
		accessible: []store.CalendarAccess{
			{Calendar: store.Calendar{ID: 1, UserID: 1, Name: "Test", UpdatedAt: store.Now()}, Editor: true},
		},
	}
	eventRepo := &fakeEventRepo{events: map[string]*store.Event{
		"1:one": {CalendarID: 1, UID: "one", ResourceName: "one", RawICAL: buildCalendarObject(buildVEvent("one")), ETag: "e1"},
		"1:two": {CalendarID: 1, UID: "two", ResourceName: "two", RawICAL: buildCalendarObject(buildVEvent("two")), ETag: "e2"},
	}}
	h := &DavServer{store: &store.Store{Calendars: calRepo, Events: eventRepo}}
	user := &store.User{ID: 1}

	bodyForHrefs := func(hrefs ...string) string {
		var b strings.Builder
		b.WriteString(`<C:calendar-multiget xmlns:D="DAV:" xmlns:C="urn:ietf:params:xml:ns:caldav"><D:prop><D:getetag/></D:prop>`)
		for _, href := range hrefs {
			b.WriteString(`<D:href>` + href + `</D:href>`)
		}
		b.WriteString(`</C:calendar-multiget>`)
		return b.String()
	}

	for name, body := range map[string]string{
		"no href":           bodyForHrefs(),
		"multiple hrefs":    bodyForHrefs("/dav/calendars/1/one.ics", "/dav/calendars/1/two.ics"),
		"different object":  bodyForHrefs("/dav/calendars/1/two.ics"),
		"foreign authority": bodyForHrefs("http://other.example/dav/calendars/1/one.ics"),
		"different scheme":  bodyForHrefs("https://example.com/dav/calendars/1/one.ics"),
		"query component":   bodyForHrefs("http://example.com/dav/calendars/1/one.ics?view=full"),
	} {
		t.Run(name, func(t *testing.T) {
			req := httptest.NewRequest("REPORT", "/dav/calendars/1/one.ics", strings.NewReader(body))
			req = req.WithContext(auth.WithUser(req.Context(), user))
			rr := httptest.NewRecorder()

			h.Report(rr, req)

			if rr.Code != http.StatusBadRequest {
				t.Fatalf("calendar-multiget = %d, want 400; body: %s", rr.Code, rr.Body.String())
			}
		})
	}

	for name, target := range map[string]string{
		"absolute HTTP URI":        "http://example.com/dav/calendars/1/one.ics",
		"absolute HTTPS URI":       "https://example.com/dav/calendars/1/one.ics",
		"matching query component": "http://example.com/dav/calendars/1/one.ics?view=full",
	} {
		t.Run(name, func(t *testing.T) {
			req := httptest.NewRequest("REPORT", target, strings.NewReader(bodyForHrefs(target)))
			req = req.WithContext(auth.WithUser(req.Context(), user))
			rr := httptest.NewRecorder()
			h.Report(rr, req)

			if rr.Code != http.StatusMultiStatus {
				t.Fatalf("valid calendar-multiget = %d, want 207; body: %s", rr.Code, rr.Body.String())
			}
			decodeMultistatus(t, rr).
				responseForHref(t, "/dav/calendars/1/one.ics").
				assertPropStatus(t, davQN("getetag"), http.StatusOK)
		})
	}

	// XML keeps whitespace in element content, but a DAV:href is a URI and a URI
	// carries none, so a client that indents the href it wrote still names the
	// Request-URI.
	t.Run("surrounding padding", func(t *testing.T) {
		req := httptest.NewRequest("REPORT", "/dav/calendars/1/one.ics",
			strings.NewReader(bodyForHrefs("\n    /dav/calendars/1/one.ics\n  ")))
		req = req.WithContext(auth.WithUser(req.Context(), user))
		rr := httptest.NewRecorder()

		h.Report(rr, req)

		if rr.Code != http.StatusMultiStatus {
			t.Fatalf("padded calendar-multiget = %d, want 207; body: %s", rr.Code, rr.Body.String())
		}
		decodeMultistatus(t, rr).
			responseForHref(t, "/dav/calendars/1/one.ics").
			assertPropStatus(t, davQN("getetag"), http.StatusOK)
	})

	t.Run("equivalent collection alias", func(t *testing.T) {
		slug := "work"
		calRepo.accessible[0].Slug = &slug
		req := httptest.NewRequest("REPORT", "/dav/calendars/work/one.ics",
			strings.NewReader(bodyForHrefs("/dav/calendars/1/one.ics")))
		req = req.WithContext(auth.WithUser(req.Context(), user))
		rr := httptest.NewRecorder()

		h.Report(rr, req)

		if rr.Code != http.StatusMultiStatus {
			t.Fatalf("calendar-multiget through an equivalent alias = %d, want 207; body: %s", rr.Code, rr.Body.String())
		}
		decodeMultistatus(t, rr).
			responseForHref(t, "/dav/calendars/1/one.ics").
			assertPropStatus(t, davQN("getetag"), http.StatusOK)
	})
}

// mkcalendarBody wraps property elements in the CALDAV:mkcalendar grammar
// RFC 4791 §9.2 defines.
func mkcalendarBody(properties ...string) string {
	return `<?xml version="1.0" encoding="utf-8"?>
<cal:mkcalendar xmlns:d="DAV:" xmlns:cal="urn:ietf:params:xml:ns:caldav" xmlns:ical="http://apple.com/ns/ical/">
  <d:set><d:prop>` + strings.Join(properties, "") + `</d:prop></d:set>
</cal:mkcalendar>`
}

// wrappedVTimezone is a CALDAV:calendar-timezone value in the form RFC 4791
// §5.2.2 requires: an iCalendar object holding exactly one VTIMEZONE. The
// STANDARD DTSTART is local time with no TZID and no "Z", which RFC 5545
// §3.8.2.4 gives as the only form admitted inside a VTIMEZONE sub-component.
const wrappedVTimezone = "BEGIN:VCALENDAR\nVERSION:2.0\nPRODID:" + testProdID +
	"\nBEGIN:VTIMEZONE\nTZID:America/Chicago\nBEGIN:STANDARD\nDTSTART:19701101T020000\nTZOFFSETFROM:-0500\nTZOFFSETTO:-0600\nEND:STANDARD\nEND:VTIMEZONE\nEND:VCALENDAR"

func newMkcalendarRequest(t *testing.T, user *store.User, path, body string) *http.Request {
	t.Helper()
	var req *http.Request
	if body == "" {
		req = httptest.NewRequest("MKCALENDAR", path, nil)
	} else {
		req = httptest.NewRequest("MKCALENDAR", path, strings.NewReader(body))
	}
	return req.WithContext(auth.WithUser(req.Context(), user))
}

// Section 5.3.1: a MKCALENDAR response body is a CALDAV:mkcalendar-response
// element, and a successful response carries Cache-Control: no-cache.
func TestRFC4791_MkcalendarResponseElementAndCacheControl(t *testing.T) {
	calRepo := &fakeCalendarRepo{calendars: make(map[int64]*store.Calendar)}
	h := &DavServer{store: &store.Store{Calendars: calRepo}}
	user := &store.User{ID: 1}

	body := mkcalendarBody(`<d:displayname>Work</d:displayname>`,
		`<cal:calendar-description>Team calendar</cal:calendar-description>`)
	rr := httptest.NewRecorder()
	h.Mkcalendar(rr, newMkcalendarRequest(t, user, "/dav/calendars/work", body))

	assertMKCalendarCreated(t, rr)
	if got := rr.Header().Get("Cache-Control"); got != "no-cache" {
		t.Errorf("Cache-Control = %q, want %q", got, "no-cache")
	}

	// §9.3 declares the element ANY; CalCard reports the properties it applied,
	// so the body is a mkcalendar-response carrying one 200 propstat over them.
	root, err := parseRootElement(rr.Body.Bytes(), calQN("mkcalendar-response"))
	if err != nil {
		t.Fatalf("decode CALDAV:mkcalendar-response: %v; body: %s", err, rr.Body.String())
	}
	propstat, err := parsePropstat(assertSoleChild(t, root, davQN("propstat")))
	if err != nil {
		t.Fatalf("decode DAV:propstat: %v", err)
	}
	if got := statusCodeFromLine(t, propstat.Status); got != http.StatusOK {
		t.Errorf("mkcalendar-response propstat status = %d, want 200", got)
	}
	want := []xml.Name{davQN("displayname"), calQN("calendar-description")}
	if got := qnList(propBagNames(propstat.Prop)); got != qnList(want) {
		t.Errorf("mkcalendar-response reports %s, want %s", got, qnList(want))
	}
}

// Section 5.3.1: a MKCALENDAR request body, when present, is a
// CALDAV:mkcalendar element and nothing else.
func TestRFC4791_MkcalendarRejectsForeignRequestBody(t *testing.T) {
	bodies := map[string]string{
		"DAV:mkcalendar":     `<d:mkcalendar xmlns:d="DAV:"><d:set><d:prop><d:displayname>Work</d:displayname></d:prop></d:set></d:mkcalendar>`,
		"DAV:mkcol":          `<d:mkcol xmlns:d="DAV:"><d:set><d:prop><d:displayname>Work</d:displayname></d:prop></d:set></d:mkcol>`,
		"DAV:propertyupdate": `<d:propertyupdate xmlns:d="DAV:"><d:set><d:prop><d:displayname>Work</d:displayname></d:prop></d:set></d:propertyupdate>`,
		"CALDAV:set only":    `<cal:set xmlns:cal="urn:ietf:params:xml:ns:caldav"/>`,
		"DAV:remove inside mkcalendar": `<cal:mkcalendar xmlns:d="DAV:" xmlns:cal="urn:ietf:params:xml:ns:caldav">` +
			`<d:remove><d:prop><d:displayname/></d:prop></d:remove></cal:mkcalendar>`,
	}

	for name, body := range bodies {
		t.Run(name, func(t *testing.T) {
			calRepo := &fakeCalendarRepo{calendars: make(map[int64]*store.Calendar)}
			h := &DavServer{store: &store.Store{Calendars: calRepo}}
			rr := httptest.NewRecorder()

			h.Mkcalendar(rr, newMkcalendarRequest(t, &store.User{ID: 1}, "/dav/calendars/work", body))

			if rr.Code != http.StatusBadRequest {
				t.Errorf("MKCALENDAR with a %s body = %d, want 400; body: %s", name, rr.Code, rr.Body.String())
			}
			if len(calRepo.calendars) != 0 {
				t.Errorf("MKCALENDAR with a %s body created %d calendars, want none", name, len(calRepo.calendars))
			}
		})
	}
}

// Section 5.3.1: property instructions are processed in document order, so the
// last of two naming one property wins. Both live in the one
// DAV:set the §9.2 content model admits.
func TestRFC4791_MkcalendarProcessesInstructionsInDocumentOrder(t *testing.T) {
	calRepo := &fakeCalendarRepo{calendars: make(map[int64]*store.Calendar)}
	h := &DavServer{store: &store.Store{Calendars: calRepo}}
	user := &store.User{ID: 1}

	body := mkcalendarBody(
		`<d:displayname>First</d:displayname>`,
		`<d:displayname>Second</d:displayname>`)
	rr := httptest.NewRecorder()
	h.Mkcalendar(rr, newMkcalendarRequest(t, user, "/dav/calendars/ordered", body))

	assertMKCalendarCreated(t, rr)
	if len(calRepo.calendars) != 1 {
		t.Fatalf("MKCALENDAR created %d calendars, want 1", len(calRepo.calendars))
	}
	for _, created := range calRepo.calendars {
		if created.Name != "Second" {
			t.Errorf("displayname = %q, want %q: the later instruction in document order wins", created.Name, "Second")
		}
	}
}

// Section 9.2 fixes the content model as <!ELEMENT mkcalendar (DAV:set)>: one
// DAV:set, no character data beside it, and one document. A body outside that
// grammar is refused rather than partly applied.
func TestRFC4791_MkcalendarEnforcesRequestGrammar(t *testing.T) {
	bodies := map[string]string{
		"two DAV:set elements": `<cal:mkcalendar xmlns:d="DAV:" xmlns:cal="urn:ietf:params:xml:ns:caldav">` +
			`<d:set><d:prop><d:displayname>First</d:displayname></d:prop></d:set>` +
			`<d:set><d:prop><d:displayname>Second</d:displayname></d:prop></d:set>` +
			`</cal:mkcalendar>`,
		"no DAV:set at all": `<cal:mkcalendar xmlns:d="DAV:" xmlns:cal="urn:ietf:params:xml:ns:caldav"></cal:mkcalendar>`,
		"character data beside the set": `<cal:mkcalendar xmlns:d="DAV:" xmlns:cal="urn:ietf:params:xml:ns:caldav">stray text` +
			`<d:set><d:prop><d:displayname>Work</d:displayname></d:prop></d:set></cal:mkcalendar>`,
		"a second document after the root": `<cal:mkcalendar xmlns:d="DAV:" xmlns:cal="urn:ietf:params:xml:ns:caldav">` +
			`<d:set><d:prop><d:displayname>Work</d:displayname></d:prop></d:set></cal:mkcalendar>` +
			`<cal:mkcalendar xmlns:d="DAV:" xmlns:cal="urn:ietf:params:xml:ns:caldav">` +
			`<d:set><d:prop><d:displayname>Smuggled</d:displayname></d:prop></d:set></cal:mkcalendar>`,
		"trailing character data after the root": `<cal:mkcalendar xmlns:d="DAV:" xmlns:cal="urn:ietf:params:xml:ns:caldav">` +
			`<d:set><d:prop><d:displayname>Work</d:displayname></d:prop></d:set></cal:mkcalendar>trailing`,
		"trailing directive after the root": `<cal:mkcalendar xmlns:d="DAV:" xmlns:cal="urn:ietf:params:xml:ns:caldav">` +
			`<d:set><d:prop><d:displayname>Work</d:displayname></d:prop></d:set></cal:mkcalendar><!trailing>`,
	}

	for name, body := range bodies {
		t.Run(name, func(t *testing.T) {
			calRepo := &fakeCalendarRepo{calendars: make(map[int64]*store.Calendar)}
			h := &DavServer{store: &store.Store{Calendars: calRepo}}
			rr := httptest.NewRecorder()

			h.Mkcalendar(rr, newMkcalendarRequest(t, &store.User{ID: 1}, "/dav/calendars/work", body))

			if rr.Code != http.StatusBadRequest {
				t.Errorf("MKCALENDAR with %s = %d, want 400; body: %s", name, rr.Code, rr.Body.String())
			}
			if len(calRepo.calendars) != 0 {
				t.Errorf("MKCALENDAR with %s created %d calendars, want none", name, len(calRepo.calendars))
			}
		})
	}

	// §5.3.1 leaves the body optional, so a request that sends none still
	// creates the collection: the grammar constrains a body that is present.
	t.Run("no body at all", func(t *testing.T) {
		calRepo := &fakeCalendarRepo{calendars: make(map[int64]*store.Calendar)}
		h := &DavServer{store: &store.Store{Calendars: calRepo}}
		rr := httptest.NewRecorder()

		h.Mkcalendar(rr, newMkcalendarRequest(t, &store.User{ID: 1}, "/dav/calendars/work", ""))

		assertMKCalendarCreated(t, rr)
	})
}

// Section 5.3.1: when a property instruction fails, all of them fail. The
// response is the RFC 2518 §12.13.2 result -- a 207 carrying the failing
// property's own status and 424 for the rest -- and no collection is created.
func TestRFC4791_MkcalendarFailedInstructionCreatesNothing(t *testing.T) {
	calRepo := &fakeCalendarRepo{calendars: make(map[int64]*store.Calendar)}
	h := &DavServer{store: &store.Store{Calendars: calRepo}}
	user := &store.User{ID: 1}

	// DAV:getetag is a protected live property, so setting it can never succeed.
	body := mkcalendarBody(
		`<d:displayname>Work</d:displayname>`,
		`<d:getetag>"forged"</d:getetag>`,
		`<cal:calendar-description>Team calendar</cal:calendar-description>`)
	rr := httptest.NewRecorder()
	h.Mkcalendar(rr, newMkcalendarRequest(t, user, "/dav/calendars/work", body))

	if rr.Code != http.StatusMultiStatus {
		t.Fatalf("MKCALENDAR with a failing instruction = %d, want 207; body: %s", rr.Code, rr.Body.String())
	}
	resp := decodeMultistatus(t, rr).responseForHref(t, "/dav/calendars/work")
	resp.assertPropstatNames(t, http.StatusForbidden, davQN("getetag"))
	resp.assertPropstatNames(t, http.StatusFailedDependency, davQN("displayname"), calQN("calendar-description"))

	if len(calRepo.calendars) != 0 {
		t.Errorf("failed MKCALENDAR created %d calendars, want none", len(calRepo.calendars))
	}

	// The prior server state is restored exactly, so the same request succeeds
	// once the offending instruction is dropped.
	rr = httptest.NewRecorder()
	h.Mkcalendar(rr, newMkcalendarRequest(t, user, "/dav/calendars/work",
		mkcalendarBody(`<d:displayname>Work</d:displayname>`)))
	assertMKCalendarCreated(t, rr)
}

// Section 5.3.1: MKCALENDAR is all-or-none, so a failure at any point after the
// collection row is written must leave no trace of it -- not the collection,
// and not the dead properties already persisted against its path. Dead
// properties are keyed by path alone, so a rollback that only removed the
// collection would strand them where the next collection to take that path
// would inherit them.
func TestRFC4791_MkcalendarFailureAfterCreateLeavesNoState(t *testing.T) {
	const statePath = "/dav/calendars/1"
	body := mkcalendarBody(`<d:displayname>Work</d:displayname>`,
		`<x:note xmlns:x="urn:example:custom">keep</x:note>`)

	tests := map[string]struct {
		store func(*fakeCalendarRepo, *fakeDeadPropertyRepo, *fakeLockRepo)
	}{
		"dead property write fails": {
			store: func(_ *fakeCalendarRepo, dead *fakeDeadPropertyRepo, _ *fakeLockRepo) {
				dead.applyErr = errors.New("dead property write failed")
			},
		},
		"lock rebind fails after dead properties are written": {
			store: func(_ *fakeCalendarRepo, _ *fakeDeadPropertyRepo, locks *fakeLockRepo) {
				locks.moveResourcePathErr = errors.New("lock rebind failed")
			},
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			calRepo := &fakeCalendarRepo{calendars: make(map[int64]*store.Calendar)}
			dead := &fakeDeadPropertyRepo{}
			// A lock on the unresolved Request-URI is what MKCALENDAR rebinds onto
			// the created collection, so the rebind has something to move.
			locks := &fakeLockRepo{locks: map[string]*store.Lock{
				"pending": {Token: "pending", ResourcePath: "/dav/calendars/work", ExpiresAt: time.Now().Add(time.Hour)},
			}}
			tc.store(calRepo, dead, locks)
			h := &DavServer{store: &store.Store{
				Calendars:      calRepo,
				CalendarState:  &fakeCalendarStateCreator{calendars: calRepo, dead: dead, locks: locks},
				DeadProperties: dead,
				Locks:          locks,
			}}

			rr := httptest.NewRecorder()
			h.Mkcalendar(rr, newMkcalendarRequest(t, &store.User{ID: 1}, "/dav/calendars/work", body))

			if rr.Code != http.StatusInternalServerError {
				t.Fatalf("MKCALENDAR = %d, want 500; body: %s", rr.Code, rr.Body.String())
			}
			if len(calRepo.calendars) != 0 {
				t.Errorf("failed MKCALENDAR left %d calendars, want none", len(calRepo.calendars))
			}
			if len(dead.properties[statePath]) != 0 {
				t.Errorf("failed MKCALENDAR left dead properties at %s: %#v", statePath, dead.properties[statePath])
			}
			if got := locks.locks["pending"]; got == nil || got.ResourcePath != "/dav/calendars/work" {
				t.Errorf("failed MKCALENDAR moved the pending lock: %#v", locks.locks)
			}
		})
	}
}

// Section 5.3.1: a backend without a transaction or another atomic creation
// primitive is refused before the collection write. A failing Delete proves
// this path does not rely on best-effort compensation to simulate atomicity.
func TestRFC4791_MkcalendarRefusesNonAtomicStateCreationBeforeWrite(t *testing.T) {
	calRepo := &fakeCalendarRepo{
		calendars: make(map[int64]*store.Calendar),
		deleteErr: errors.New("rollback delete failed"),
	}
	dead := &fakeDeadPropertyRepo{applyErr: errors.New("dead property write failed")}
	h := &DavServer{store: &store.Store{Calendars: calRepo, DeadProperties: dead}}

	rr := httptest.NewRecorder()
	h.Mkcalendar(rr, newMkcalendarRequest(t, &store.User{ID: 1}, "/dav/calendars/work",
		mkcalendarBody(`<x:note xmlns:x="urn:example:custom">keep</x:note>`)))

	if rr.Code != http.StatusInternalServerError {
		t.Fatalf("MKCALENDAR = %d, want 500; body: %s", rr.Code, rr.Body.String())
	}
	if len(calRepo.calendars) != 0 {
		t.Fatalf("failed MKCALENDAR left %d calendars, want none", len(calRepo.calendars))
	}
}

// Section 5.3.1.1 (CALDAV:calendar-collection-location-ok) and §1.3: a 409 says
// the client can resolve the location and resubmit. A repository that cannot
// answer says nothing about the location, so it is reported as the server-side
// failure it is.
func TestRFC4791_MkcalendarLocationCheckDoesNotMaskRepositoryFailures(t *testing.T) {
	calRepo := &failingCalendarListRepo{err: errors.New("database unavailable")}
	h := &DavServer{store: &store.Store{Calendars: calRepo}}

	rr := httptest.NewRecorder()
	h.Mkcalendar(rr, newMkcalendarRequest(t, &store.User{ID: 1}, "/dav/calendars/parent/child", ""))

	if rr.Code != http.StatusInternalServerError {
		t.Fatalf("MKCALENDAR under an unreadable parent = %d, want 500; body: %s", rr.Code, rr.Body.String())
	}
}

// failingCalendarListRepo fails every lookup that reaches the database, which is
// how an outage reaches a handler that resolves a collection by name.
type failingCalendarListRepo struct {
	fakeCalendarRepo
	err error
}

func (f *failingCalendarListRepo) ListAccessible(context.Context, int64) ([]store.CalendarAccess, error) {
	return nil, f.err
}

func (f *failingCalendarListRepo) ListByUser(context.Context, int64) ([]store.Calendar, error) {
	return nil, f.err
}

// Section 5.3.1.1 (DAV:resource-must-be-null) and §1.3: no resource may exist at
// the Request-URI, and the failure names the condition under a top-level
// DAV:error. The user can remove the existing collection and resubmit, so §1.3
// makes it a 409.
func TestRFC4791_MkcalendarOnExistingResourceReportsResourceMustBeNull(t *testing.T) {
	calRepo := &fakeCalendarRepo{calendars: make(map[int64]*store.Calendar)}
	h := &DavServer{store: &store.Store{Calendars: calRepo}}
	user := &store.User{ID: 1}

	rr := httptest.NewRecorder()
	h.Mkcalendar(rr, newMkcalendarRequest(t, user, "/dav/calendars/testcal", ""))
	assertMKCalendarCreated(t, rr)

	rr = httptest.NewRecorder()
	h.Mkcalendar(rr, newMkcalendarRequest(t, user, "/dav/calendars/testcal", ""))

	assertErrorConditions(t, rr, http.StatusConflict, davQN("resource-must-be-null"))
	if len(calRepo.calendars) != 1 {
		t.Errorf("the refused MKCALENDAR left %d calendars, want the 1 that already existed", len(calRepo.calendars))
	}
}

// Section 5.3.1.1 (CALDAV:valid-calendar-data): a CALDAV:calendar-timezone in a
// MKCALENDAR body is a valid iCalendar object containing exactly one VTIMEZONE.
// The valid case also proves the value round-trips.
func TestRFC4791_MkcalendarValidatesCalendarTimezone(t *testing.T) {
	invalid := map[string]string{
		"bare component": "BEGIN:VTIMEZONE\nTZID:America/Chicago\nEND:VTIMEZONE",
		"two components": "BEGIN:VCALENDAR\nVERSION:2.0\nBEGIN:VTIMEZONE\nTZID:A\nEND:VTIMEZONE\nBEGIN:VTIMEZONE\nTZID:B\nEND:VTIMEZONE\nEND:VCALENDAR",
		"no VTIMEZONE":   "BEGIN:VCALENDAR\nVERSION:2.0\nEND:VCALENDAR",
		"not iCalendar":  "this is not a calendar",
		"missing TZID":   "BEGIN:VCALENDAR\nVERSION:2.0\nBEGIN:VTIMEZONE\nEND:VTIMEZONE\nEND:VCALENDAR",
	}
	for name, value := range invalid {
		t.Run(name, func(t *testing.T) {
			calRepo := &fakeCalendarRepo{calendars: make(map[int64]*store.Calendar)}
			h := &DavServer{store: &store.Store{Calendars: calRepo}}
			body := mkcalendarBody(`<cal:calendar-timezone>` + value + `</cal:calendar-timezone>`)
			rr := httptest.NewRecorder()

			h.Mkcalendar(rr, newMkcalendarRequest(t, &store.User{ID: 1}, "/dav/calendars/work", body))

			assertErrorConditions(t, rr, http.StatusForbidden, calQN("valid-calendar-data"))
			if len(calRepo.calendars) != 0 {
				t.Errorf("MKCALENDAR with an invalid timezone created %d calendars, want none", len(calRepo.calendars))
			}
		})
	}

	t.Run("valid object round-trips", func(t *testing.T) {
		calRepo := &fakeCalendarRepo{calendars: make(map[int64]*store.Calendar)}
		h := &DavServer{store: &store.Store{Calendars: calRepo}}
		user := &store.User{ID: 1}
		body := mkcalendarBody(`<cal:calendar-timezone>` + wrappedVTimezone + `</cal:calendar-timezone>`)
		rr := httptest.NewRecorder()

		h.Mkcalendar(rr, newMkcalendarRequest(t, user, "/dav/calendars/work", body))
		assertMKCalendarCreated(t, rr)

		rr = httptest.NewRecorder()
		h.Propfind(rr, newCalendarPropfind(t, user, "/dav/calendars/1/", "cal:calendar-timezone"))
		decodeMultistatus(t, rr).
			responseForHref(t, "/dav/calendars/1/").
			assertPropValue(t, calQN("calendar-timezone"), http.StatusOK, wrappedVTimezone)
	})
}

// Section 5.2.3: a MKCALENDAR body may set the initial
// CALDAV:supported-calendar-component-set, which the created collection then
// advertises and enforces. §5.2.3's content model is one or more empty
// CALDAV:comp elements carrying a name attribute, so a body breaking it fails.
func TestRFC4791_MkcalendarSetsSupportedCalendarComponentSet(t *testing.T) {
	t.Run("restricts the created collection", func(t *testing.T) {
		calRepo := &fakeCalendarRepo{calendars: make(map[int64]*store.Calendar)}
		h := &DavServer{store: &store.Store{Calendars: calRepo, Events: &fakeEventRepo{}}}
		user := &store.User{ID: 1}

		body := mkcalendarBody(`<cal:supported-calendar-component-set><cal:comp name="VTODO"/></cal:supported-calendar-component-set>`)
		rr := httptest.NewRecorder()
		h.Mkcalendar(rr, newMkcalendarRequest(t, user, "/dav/calendars/tasks", body))
		assertMKCalendarCreated(t, rr)

		rr = httptest.NewRecorder()
		h.Propfind(rr, newCalendarPropfind(t, user, "/dav/calendars/1/", "cal:supported-calendar-component-set"))
		decodeMultistatus(t, rr).
			responseForHref(t, "/dav/calendars/1/").
			assertSupportedComponents(t, "VTODO")

		req := newCalendarPutRequest("/dav/calendars/1/meeting.ics", strings.NewReader(buildCalendarObject(buildVEvent("rejected-event"))))
		req = req.WithContext(auth.WithUser(req.Context(), user))
		rr = httptest.NewRecorder()
		h.Put(rr, req)
		assertErrorConditions(t, rr, http.StatusForbidden, calQN("supported-calendar-component"))
	})

	malformed := map[string]string{
		"no comp children":    `<cal:supported-calendar-component-set/>`,
		"comp without a name": `<cal:supported-calendar-component-set><cal:comp/></cal:supported-calendar-component-set>`,
		"foreign child":       `<cal:supported-calendar-component-set><d:comp name="VEVENT"/></cal:supported-calendar-component-set>`,
		"unsupported type":    `<cal:supported-calendar-component-set><cal:comp name="VTIMEZONE"/></cal:supported-calendar-component-set>`,
		"comp with a child":   `<cal:supported-calendar-component-set><cal:comp name="VEVENT"><cal:comp name="VALARM"/></cal:comp></cal:supported-calendar-component-set>`,
		"text beside comp":    `<cal:supported-calendar-component-set>junk<cal:comp name="VEVENT"/></cal:supported-calendar-component-set>`,
	}
	for name, element := range malformed {
		t.Run(name, func(t *testing.T) {
			calRepo := &fakeCalendarRepo{calendars: make(map[int64]*store.Calendar)}
			h := &DavServer{store: &store.Store{Calendars: calRepo}}
			rr := httptest.NewRecorder()

			h.Mkcalendar(rr, newMkcalendarRequest(t, &store.User{ID: 1}, "/dav/calendars/tasks",
				mkcalendarBody(element, `<d:displayname>Tasks</d:displayname>`)))

			if rr.Code != http.StatusMultiStatus {
				t.Fatalf("MKCALENDAR with a malformed component set = %d, want 207; body: %s", rr.Code, rr.Body.String())
			}
			resp := decodeMultistatus(t, rr).responseForHref(t, "/dav/calendars/tasks")
			resp.assertPropstatNames(t, http.StatusConflict, calQN("supported-calendar-component-set"))
			resp.assertPropstatNames(t, http.StatusFailedDependency, davQN("displayname"))
			if len(calRepo.calendars) != 0 {
				t.Errorf("MKCALENDAR with a malformed component set created %d calendars, want none", len(calRepo.calendars))
			}
		})
	}
}

// Section 5.3.1: a MKCALENDAR body may carry properties the server does not
// recognize, which become dead properties on the created collection rather than
// failing the request (RFC 4918 §15).
func TestRFC4791_MkcalendarStoresUnrecognizedPropertiesAsDeadProperties(t *testing.T) {
	calRepo := &fakeCalendarRepo{calendars: make(map[int64]*store.Calendar)}
	deadRepo := &fakeDeadPropertyRepo{properties: map[string]map[string]store.DeadProperty{}}
	h := &DavServer{store: &store.Store{
		Calendars:      calRepo,
		CalendarState:  &fakeCalendarStateCreator{calendars: calRepo, dead: deadRepo},
		Events:         &fakeEventRepo{},
		DeadProperties: deadRepo,
	}}
	user := &store.User{ID: 1}

	body := `<?xml version="1.0" encoding="utf-8"?>
<cal:mkcalendar xmlns:d="DAV:" xmlns:cal="urn:ietf:params:xml:ns:caldav" xmlns:x="urn:example:vendor">
  <d:set><d:prop><x:note>keep me</x:note></d:prop></d:set>
</cal:mkcalendar>`
	rr := httptest.NewRecorder()
	h.Mkcalendar(rr, newMkcalendarRequest(t, user, "/dav/calendars/work", body))
	assertMKCalendarCreated(t, rr)

	rr = httptest.NewRecorder()
	propfind := `<?xml version="1.0" encoding="utf-8"?>
<d:propfind xmlns:d="DAV:" xmlns:x="urn:example:vendor"><d:prop><x:note/></d:prop></d:propfind>`
	req := httptest.NewRequest("PROPFIND", "/dav/calendars/1/", strings.NewReader(propfind))
	req.Header.Set("Depth", "0")
	req = req.WithContext(auth.WithUser(req.Context(), user))
	h.Propfind(rr, req)

	decodeMultistatus(t, rr).
		responseForHref(t, "/dav/calendars/1/").
		assertPropValue(t, qn("urn:example:vendor", "note"), http.StatusOK, "keep me")
}
