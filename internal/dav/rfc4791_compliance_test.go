package dav

import (
	"encoding/xml"
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
// rejects: once Phase 3 lands the strict validation the matrix schedules, such
// a fixture is answered by the validator before it reaches the behavior the
// test names, and the test stops measuring what it says it measures. A
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
	h := NewDavServer(Options{Config: &config.Config{}, Store: &store.Store{}})
	req := httptest.NewRequest(http.MethodOptions, "/dav/calendars/1/", nil)
	rr := httptest.NewRecorder()

	h.Options(rr, req)

	// §5.1: the DAV header MUST carry "calendar-access" as a compliance class.
	assertHeaderToken(t, rr, "DAV", "calendar-access",
		"RFC 4791 §5.1 requires it on any resource supporting a calendar property, report, method or privilege")
	// §5.1: REPORT is required, and §5.3.1 makes MKCALENDAR a SHOULD.
	assertHeaderToken(t, rr, "Allow", "REPORT", "RFC 4791 §5.1 requires the calendaring reports")
	assertHeaderToken(t, rr, "Allow", "MKCALENDAR", "RFC 4791 §5.3.1 says a server SHOULD support MKCALENDAR")
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

	req := httptest.NewRequest("PROPFIND", "/dav/calendars/1/", nil)
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

// Section 5.3.1: MKCALENDAR on existing resource
func TestRFC4791_MkcalendarOnExistingResourceFails(t *testing.T) {
	calRepo := &fakeCalendarRepo{calendars: make(map[int64]*store.Calendar)}
	h := &DavServer{store: &store.Store{Calendars: calRepo}}
	user := &store.User{ID: 1}

	// Create first time - should succeed
	req := httptest.NewRequest("MKCALENDAR", "/dav/calendars/testcal", nil)
	req = req.WithContext(auth.WithUser(req.Context(), user))
	rr := httptest.NewRecorder()
	h.Mkcalendar(rr, req)

	if rr.Code != http.StatusCreated {
		t.Errorf("First MKCALENDAR should succeed with 201, got %d", rr.Code)
	}

	// Second attempt on the same resource should fail
	req = httptest.NewRequest("MKCALENDAR", "/dav/calendars/testcal", nil)
	req = req.WithContext(auth.WithUser(req.Context(), user))
	rr = httptest.NewRecorder()

	h.Mkcalendar(rr, req)

	if rr.Code != http.StatusConflict {
		t.Errorf("MKCALENDAR onto an existing collection = %d, want 409 Conflict; body: %s", rr.Code, rr.Body.String())
	}
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
			eventRepo := &fakeEventRepo{
				events: map[string]*store.Event{
					"1:test": {
						CalendarID: 1,
						UID:        "test",
						RawICAL:    "BEGIN:VCALENDAR\r\nBEGIN:VEVENT\r\nUID:test\r\nEND:VEVENT\r\nEND:VCALENDAR\r\n",
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
// than asserting a requirement. The calendar-object-resource target is a
// different matter — §7 makes it mandatory — and CalCard does not meet it.
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

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req := httptest.NewRequest("REPORT", "/dav/", strings.NewReader(tt.body))
			req = req.WithContext(auth.WithUser(req.Context(), user))
			rr := httptest.NewRecorder()

			h.Report(rr, req)

			if rr.Code != http.StatusForbidden {
				t.Errorf("%s on an ordinary collection = %d, want 403 Forbidden; body: %s", tt.name, rr.Code, rr.Body.String())
			}
		})
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
				RawICAL:    "BEGIN:VCALENDAR\r\nBEGIN:VEVENT\r\nUID:event\r\nEND:VEVENT\r\nEND:VCALENDAR\r\n",
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
	req = req.WithContext(auth.WithUser(req.Context(), user))
	rr := httptest.NewRecorder()

	h.Report(rr, req)

	if rr.Code != http.StatusOK {
		t.Errorf("RFC 4791 Section 7.10: free-busy-query must return 200 OK, got %d", rr.Code)
	}
	assertMediaType(t, rr, "text/calendar")

	respBody := rr.Body.String()
	// RFC 4791 Section 7.10: Response must contain VFREEBUSY component
	if !strings.Contains(respBody, "BEGIN:VFREEBUSY") {
		t.Error("RFC 4791 Section 7.10: Response must contain VFREEBUSY component")
	}
	if !strings.Contains(respBody, "END:VFREEBUSY") {
		t.Error("RFC 4791 Section 7.10: Response must have complete VFREEBUSY component")
	}
	if strings.Count(respBody, "BEGIN:VFREEBUSY") != 1 || strings.Count(respBody, "END:VFREEBUSY") != 1 {
		t.Error("RFC 4791 Section 7.10: Response must contain exactly one VFREEBUSY component")
	}
	// Must include FREEBUSY periods
	if !strings.Contains(respBody, "FREEBUSY:") {
		t.Error("RFC 4791 Section 7.10: Response must include FREEBUSY properties")
	}
	if !strings.Contains(respBody, "DTSTART:20240601T000000Z") || !strings.Contains(respBody, "DTEND:20240630T235959Z") {
		t.Error("RFC 4791 Section 7.10: Response must include requested time range in VFREEBUSY")
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
	req = req.WithContext(auth.WithUser(req.Context(), user))
	rr := httptest.NewRecorder()

	h.Report(rr, req)

	if rr.Code != http.StatusOK {
		t.Fatalf("RFC 4791 Section 7.10: free-busy-query must return 200 OK, got %d", rr.Code)
	}
	respBody := rr.Body.String()
	if !strings.Contains(respBody, "BEGIN:VFREEBUSY") || !strings.Contains(respBody, "END:VFREEBUSY") {
		t.Fatal("RFC 4791 Section 7.10: Response must include VFREEBUSY component")
	}
	if strings.Count(respBody, "BEGIN:VFREEBUSY") != 1 || strings.Count(respBody, "END:VFREEBUSY") != 1 {
		t.Error("RFC 4791 Section 7.10: Empty result must contain exactly one VFREEBUSY component")
	}
	if strings.Contains(respBody, "FREEBUSY:") {
		t.Error("RFC 4791 Section 7.10: Empty result must not include FREEBUSY properties")
	}
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

	req := httptest.NewRequest("PROPFIND", "/dav/calendars/1/", nil)
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

			// RFC 4791: Invalid calendar data should be rejected
			if rr.Code != http.StatusBadRequest {
				t.Errorf("RFC 4791: Malformed iCalendar (%s) should return 400, got %d", tt.description, rr.Code)
			}
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
	parent := &store.Calendar{ID: 1, UserID: 1, Name: "Test"}
	calRepo := &fakeCalendarRepo{
		calendars:  map[int64]*store.Calendar{1: parent},
		accessible: []store.CalendarAccess{{Calendar: *parent, Editor: true}},
	}
	h := &DavServer{store: &store.Store{Calendars: calRepo, Events: &fakeEventRepo{}}}
	user := &store.User{ID: 1}

	req := httptest.NewRequest("MKCALENDAR", "/dav/calendars/1/nested/", nil)
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

// §5.2.10 and §5.3.1.1 both require CALDAV:calendar-timezone to hold a
// valid iCalendar object containing one VTIMEZONE, so a conforming server serves
// VCALENDAR-wrapped data. CalCard defaults the property to a bare VTIMEZONE
// component instead. This test pins that known defect so the fix is a visible
// change here rather than a silent one; it is deliberately not named for the
// requirement, because it asserts the opposite of it.
func TestRFC4791_CalendarCollection_CalendarTimezone_KnownBareFragmentDefect(t *testing.T) {
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
    <cal:calendar-timezone/>
  </d:prop>
</d:propfind>`

	req := httptest.NewRequest("PROPFIND", "/dav/calendars/1/", strings.NewReader(body))
	req.Header.Set("Depth", "0")
	req = req.WithContext(auth.WithUser(req.Context(), user))
	rr := httptest.NewRecorder()

	h.Propfind(rr, req)

	timezone := decodeMultistatus(t, rr).
		responseForHref(t, "/dav/calendars/1/").
		assertPropStatus(t, calQN("calendar-timezone"), http.StatusOK).
		Value()

	// The value is iCalendar, not XML, so it stays a string comparison. When the
	// wrapped form lands this assertion inverts to require it.
	if !strings.HasPrefix(timezone, "BEGIN:VTIMEZONE") || !strings.HasSuffix(timezone, "END:VTIMEZONE") {
		t.Errorf("calendar-timezone = %q, want the current bare VTIMEZONE fragment; if this now carries a VCALENDAR wrapper the defect is fixed and this test should be replaced", timezone)
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

	assertErrorConditions(t, rr, http.StatusUnsupportedMediaType, calQN("supported-calendar-data"))
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
// calendar object resource stores, not merely its DTSTART and DTEND. CalCard
// collects only those two in analyzeICalendar, so every other date-valued
// property passes the limits unread. This test pins that gap rather than the
// requirement: each case below is stored today and must be rejected once the
// limits cover every date-valued property, at which point the expectations
// below invert.
func TestRFC4791_Precondition_DateLimits_UncheckedPropertiesKnownDefect(t *testing.T) {
	tests := []struct {
		name     string
		property string
		ical     string
	}{
		{
			name:     "VTODO DUE beyond max-date-time",
			property: "DUE",
			ical: buildCalendarObject(buildVTodo("late-due",
				"DTSTART:20240601T000000Z", "DUE:99991231T235959Z")),
		},
		{
			name:     "RECURRENCE-ID before min-date-time",
			property: "RECURRENCE-ID",
			ical: buildCalendarObject(buildVEvent("early-recurrence-id",
				"RECURRENCE-ID:15000101T000000Z", "DTSTART:20240601T100000Z", "DTEND:20240601T110000Z")),
		},
		{
			name:     "RDATE beyond max-date-time",
			property: "RDATE",
			ical: buildCalendarObject(buildVEvent("late-rdate",
				"DTSTART:20240601T100000Z", "DTEND:20240601T110000Z", "RDATE:99991231T235959Z")),
		},
		{
			name:     "EXDATE before min-date-time",
			property: "EXDATE",
			ical: buildCalendarObject(buildVEvent("early-exdate",
				"DTSTART:20240601T100000Z", "DTEND:20240601T110000Z",
				"RRULE:FREQ=DAILY;COUNT=3", "EXDATE:15000101T000000Z")),
		},
		{
			name:     "VJOURNAL DATE-form DTSTAMP beyond max-date-time",
			property: "DTSTAMP",
			ical: buildCalendarObject(buildVJournal("late-dtstamp",
				"DTSTAMP:99991231T235959Z", "DTSTART;VALUE=DATE:20240601")),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			calRepo := &fakeCalendarRepo{
				accessible: []store.CalendarAccess{
					{Calendar: store.Calendar{ID: 1, UserID: 1, Name: "Test"}, Editor: true},
				},
			}
			h := &DavServer{store: &store.Store{Calendars: calRepo, Events: &fakeEventRepo{events: make(map[string]*store.Event)}}}
			user := &store.User{ID: 1}

			req := newCalendarPutRequest("/dav/calendars/1/limits.ics", strings.NewReader(tt.ical))
			req = req.WithContext(auth.WithUser(req.Context(), user))
			rr := httptest.NewRecorder()

			h.Put(rr, req)

			if rr.Code != http.StatusCreated {
				t.Fatalf("PUT of an out-of-range %s = %d, want the 201 CalCard returns today. "+
					"If the date limits now cover %s, invert this case into an assertErrorConditions check; body: %s",
					tt.property, rr.Code, tt.property, rr.Body.String())
			}
		})
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
	req = req.WithContext(auth.WithUser(req.Context(), user))
	rr := httptest.NewRecorder()

	h.Report(rr, req)

	decodeMultistatus(t, rr).assertHrefs(t, "/dav/calendars/1/without-location.ics")
}

// Sections 7.6 and 9.6: a calendar-data projection returns only the components
// and properties the request named, inside the VCALENDAR wrapper.
//
// The object frame is asserted by parsing the returned value rather than by
// looking for the BEGIN:VCALENDAR/END:VCALENDAR pair, which an empty wrapper
// carries too. Serving that frame — one VERSION and one PRODID on the VCALENDAR
// per RFC 5545 §3.6, wrapping the projected component — is CalCard policy
// rather than an RFC 4791 requirement, and pinning it is what separates a
// projection that honours the request from one that returns an empty shell.
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
			req = req.WithContext(auth.WithUser(req.Context(), user))
			rr := httptest.NewRecorder()

			h.Report(rr, req)

			data := decodeMultistatus(t, rr).
				responseForHref(t, "/dav/calendars/1/event.ics").
				assertPropStatus(t, calQN("calendar-data"), http.StatusOK).
				Value()

			assertICalendarObject(t, data, "VEVENT")
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
	calendarProperties := icalendarPropertiesIn(value, "VCALENDAR")
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
	for name, values := range icalendarPropertiesIn(value, "VCALENDAR", component) {
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

// icalendarPropertiesIn indexes the property values declared directly inside the
// component named by path — {"VCALENDAR"} for the calendar-level properties,
// {"VCALENDAR", "VEVENT"} for one component's own — unfolding continuation
// lines per RFC 5545 §3.1 first.
//
// The scope is the point. RFC 5545 §3.6 puts VERSION and PRODID on the
// VCALENDAR itself, so a count taken over the whole object is satisfied by a
// VERSION nested inside a VEVENT, which is not a valid iCalendar object at all.
func icalendarPropertiesIn(value string, path ...string) map[string][]string {
	properties := make(map[string][]string)
	var stack []string
	for _, raw := range ical.UnfoldLines(value) {
		line := strings.TrimSpace(raw)
		upper := strings.ToUpper(line)
		switch {
		case strings.HasPrefix(upper, "BEGIN:"):
			stack = append(stack, strings.TrimSpace(strings.TrimPrefix(upper, "BEGIN:")))
			continue
		case strings.HasPrefix(upper, "END:"):
			if len(stack) > 0 {
				stack = stack[:len(stack)-1]
			}
			continue
		}
		if !slices.Equal(stack, path) {
			continue
		}
		if name, _, propertyValue, ok := splitICalendarProperty(line); ok {
			properties[name] = append(properties[name], propertyValue)
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
	req = req.WithContext(auth.WithUser(req.Context(), user))
	rr := httptest.NewRecorder()

	h.Report(rr, req)

	// Phase 7 owns asserting that the recurrence set is actually limited.
	decodeMultistatus(t, rr).
		responseForHref(t, "/dav/calendars/1/recurring.ics").
		assertPropStatus(t, calQN("calendar-data"), http.StatusOK)
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
	req = req.WithContext(auth.WithUser(req.Context(), user))
	rr := httptest.NewRecorder()

	h.Report(rr, req)

	// Phase 7 owns asserting that the instances are actually expanded.
	decodeMultistatus(t, rr).
		responseForHref(t, "/dav/calendars/1/recurring.ics").
		assertPropStatus(t, calQN("calendar-data"), http.StatusOK)
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
//
// CalCard does not meet it. defaultSupportedPrivilegeSet in acl.go omits
// CALDAV:read-free-busy from DAV:supported-privilege-set entirely, so nothing
// places it below DAV:read, and the containment assertions above pass on two
// unrelated privileges. This test pins the hierarchy actually served, so the
// gap stays visible and any change to it is deliberate; the failure message
// says what to do with the test when the aggregation lands.
func TestRFC4791_SupportedPrivilegeSet_ReadFreeBusyNotAggregatedUnderRead_KnownDefect(t *testing.T) {
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
		"{DAV:}all/{DAV:}write",
		"{DAV:}all/{DAV:}write-acl",
		"{DAV:}all/{DAV:}write/{DAV:}bind",
		"{DAV:}all/{DAV:}write/{DAV:}unbind",
		"{DAV:}all/{DAV:}write/{DAV:}write-content",
		"{DAV:}all/{DAV:}write/{DAV:}write-properties",
	)

	aggregated := qnString(davQN("read")) + "/" + qnString(calQN("read-free-busy"))
	for _, path := range resp.supportedPrivileges(t) {
		if strings.HasSuffix(path, aggregated) {
			t.Errorf("%s now aggregates %s. Replace this test with a positive "+
				"assertion of the aggregation and update CALDAV_FIXES.md.",
				qnString(davQN("read")), qnString(calQN("read-free-busy")))
		}
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
	req = req.WithContext(auth.WithUser(req.Context(), user))
	rr = httptest.NewRecorder()
	h.Report(rr, req)
	// §7.10 requires free-busy access not to reveal resource URLs, so a caller
	// holding only read-free-busy sees the collection as absent rather than
	// forbidden.
	if rr.Code != http.StatusNotFound {
		t.Fatalf("calendar-query with only read-free-busy = %d, want 404 Not Found", rr.Code)
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
	if rr.Code != http.StatusNotFound {
		t.Fatalf("calendar-multiget with only read-free-busy = %d, want 404 Not Found", rr.Code)
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
// reportRequest has no timezone field, so the element is parsed away and
// floating values are not resolved against it.
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
	if got != caldavMaxAttendees {
		t.Errorf("max-attendees-per-instance = %d, want %d", got, caldavMaxAttendees)
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
	if got != caldavMaxInstances {
		t.Errorf("max-instances = %d, want %d", got, caldavMaxInstances)
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

	attendees := make([]string, 0, caldavMaxAttendees+2)
	attendees = append(attendees, "DTSTART:20240101T000000Z")
	for i := 0; i < caldavMaxAttendees+1; i++ {
		attendees = append(attendees, fmt.Sprintf("ATTENDEE:mailto:user%d@example.com", i))
	}
	icalData := buildCalendarObject(buildVEvent("attendees", attendees...))

	req := newCalendarPutRequest("/dav/calendars/1/attendees.ics", strings.NewReader(icalData))
	req = req.WithContext(auth.WithUser(req.Context(), user))
	rr := httptest.NewRecorder()

	h.Put(rr, req)

	assertErrorConditions(t, rr, http.StatusForbidden, calQN("max-attendees-per-instance"))
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

	assertErrorConditions(t, rr, http.StatusUnsupportedMediaType, calQN("supported-calendar-data"))
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

	if rr.Code != http.StatusUnsupportedMediaType {
		t.Errorf("PUT without Content-Type = %d, want 415 Unsupported Media Type", rr.Code)
	}
	assertErrorConditions(t, rr, http.StatusUnsupportedMediaType, calQN("supported-calendar-data"))
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

	assertErrorConditions(t, rr, http.StatusUnsupportedMediaType, calQN("supported-calendar-data"))
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

	assertErrorConditions(t, rr, http.StatusRequestEntityTooLarge, calQN("max-resource-size"))
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

	assertErrorConditions(t, rr, http.StatusConflict, calQN("valid-calendar-object-resource"))
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

	assertErrorConditions(t, rr, http.StatusBadRequest, calQN("valid-calendar-object-resource"))
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

	assertErrorConditions(t, rr, http.StatusBadRequest, calQN("valid-calendar-object-resource"))
}

// Section 4.1: a UID is unique within a calendar collection, so a second
// resource name carrying one already in use fails CALDAV:no-uid-conflict.
//
// This covers the sequential case only. putCalendarObject reads the UID
// through Events.GetByUID and writes
// through Events.Upsert as two statements, so two concurrent PUTs naming one
// UID at different resource names can both find no conflict; the second write
// then takes the ON CONFLICT (calendar_id, uid) DO UPDATE branch in
// internal/store/postgres.go, silently rewriting resource_name, and both
// requests answer 201. Closing the row needs the atomic repository operation
// and the PostgreSQL concurrency tests Phase 3 owns; no handler-level test can
// establish it.
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

	assertErrorConditions(t, rr, http.StatusConflict, calQN("no-uid-conflict"))
}

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

	assertErrorConditions(t, rr, http.StatusConflict, calQN("no-uid-conflict"))
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

	assertErrorConditions(t, rr, http.StatusConflict, calQN("valid-calendar-object-resource"))
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

// Section 5.3.1: MKCALENDAR on Invalid Path
func TestRFC4791_MkcalendarInvalidPath(t *testing.T) {
	calRepo := &fakeCalendarRepo{calendars: make(map[int64]*store.Calendar)}
	h := &DavServer{store: &store.Store{Calendars: calRepo}}
	user := &store.User{ID: 1}

	// Try to create calendar with invalid path (e.g., missing name)
	req := httptest.NewRequest("MKCALENDAR", "/dav/calendars/", nil)
	req = req.WithContext(auth.WithUser(req.Context(), user))
	rr := httptest.NewRecorder()

	h.Mkcalendar(rr, req)

	if rr.Code != http.StatusBadRequest {
		t.Errorf("MKCALENDAR without a collection name = %d, want 400 Bad Request; body: %s", rr.Code, rr.Body.String())
	}
}
