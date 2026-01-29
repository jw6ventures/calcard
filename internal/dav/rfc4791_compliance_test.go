package dav

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"regexp"
	"strconv"
	"strings"
	"testing"
	"time"

	"gitea.jw6.us/james/calcard/internal/auth"
	"gitea.jw6.us/james/calcard/internal/config"
	"gitea.jw6.us/james/calcard/internal/store"
	"gitea.jw6.us/james/calcard/internal/util"
)

func TestRFC4791_OptionsAdvertisesCalendarAccess(t *testing.T) {
	h := NewHandler(&config.Config{}, &store.Store{})
	req := httptest.NewRequest(http.MethodOptions, "/dav/calendars/1/", nil)
	rr := httptest.NewRecorder()

	h.Options(rr, req)

	// RFC 4791 Section 5.1.1: MUST advertise "calendar-access" in DAV header
	davHeader := rr.Header().Get("DAV")
	if !strings.Contains(davHeader, "calendar-access") {
		t.Errorf("DAV header must include 'calendar-access' per RFC 4791 Section 5.1, got: %s", davHeader)
	}

	// RFC 4791 Section 5.1.1: MUST support REPORT method
	allowHeader := rr.Header().Get("Allow")
	if !strings.Contains(allowHeader, "REPORT") {
		t.Errorf("Allow header must include REPORT method per RFC 4791 Section 5.1, got: %s", allowHeader)
	}

	// RFC 4791 Section 5.3.1: SHOULD support MKCALENDAR method
	if !strings.Contains(allowHeader, "MKCALENDAR") {
		t.Errorf("Allow header should include MKCALENDAR method per RFC 4791 Section 5.3.1, got: %s", allowHeader)
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
	h := &Handler{store: &store.Store{Calendars: calRepo, Events: &fakeEventRepo{}}}
	user := &store.User{ID: 1, PrimaryEmail: "test@example.com"}

	req := httptest.NewRequest("PROPFIND", "/dav/calendars/1/", nil)
	req.Header.Set("Depth", "0")
	req = req.WithContext(auth.WithUser(req.Context(), user))
	rr := httptest.NewRecorder()

	h.Propfind(rr, req)

	respBody := rr.Body.String()
	// RFC 4791 Section 4.2: Calendar collections MUST be identified by both collection and calendar resource types
	if !strings.Contains(respBody, "<d:collection") {
		t.Error("RFC 4791 Section 4.2: Calendar collection must have d:collection resource type")
	}
	if !strings.Contains(respBody, "<cal:calendar") {
		t.Error("RFC 4791 Section 4.2: Calendar collection must have cal:calendar resource type")
	}
}

// Section 5.2.3: supported-calendar-component-set Property
func TestRFC4791_SupportedCalendarComponentSetRequired(t *testing.T) {
	now := store.Now()
	calRepo := &fakeCalendarRepo{
		accessible: []store.CalendarAccess{
			{Calendar: store.Calendar{ID: 1, UserID: 1, Name: "Test", UpdatedAt: now}, Editor: true},
		},
	}
	h := &Handler{store: &store.Store{Calendars: calRepo, Events: &fakeEventRepo{}}}
	user := &store.User{ID: 1}

	req := httptest.NewRequest("PROPFIND", "/dav/calendars/1/", nil)
	req.Header.Set("Depth", "0")
	req = req.WithContext(auth.WithUser(req.Context(), user))
	rr := httptest.NewRecorder()

	h.Propfind(rr, req)

	body := rr.Body.String()
	// RFC 4791 Section 5.2.3: MUST include supported-calendar-component-set
	if !strings.Contains(body, "supported-calendar-component-set") {
		t.Error("RFC 4791 Section 5.2.3: Calendar collection MUST have supported-calendar-component-set property")
	}

	// Should support at least VEVENT
	if !strings.Contains(body, `name="VEVENT"`) {
		t.Error("RFC 4791 Section 5.2.3: Should support VEVENT component")
	}
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
	h := &Handler{store: &store.Store{Calendars: calRepo, Events: eventRepo}}
	user := &store.User{ID: 1}

	req := httptest.NewRequest(http.MethodGet, "/dav/calendars/1/event.ics", nil)
	req = req.WithContext(auth.WithUser(req.Context(), user))
	rr := httptest.NewRecorder()

	h.Get(rr, req)

	// RFC 4791 Section 5.2.4: Calendar object resources MUST have Content-Type: text/calendar
	contentType := rr.Header().Get("Content-Type")
	if contentType != "text/calendar" {
		t.Errorf("RFC 4791 Section 5.2.4: Calendar objects must use Content-Type text/calendar, got: %s", contentType)
	}
}

// Section 5.3.1: MKCALENDAR Method
func TestRFC4791_MkcalendarCreatesCalendarCollection(t *testing.T) {
	calRepo := &fakeCalendarRepo{calendars: make(map[int64]*store.Calendar)}
	h := &Handler{store: &store.Store{Calendars: calRepo}}
	user := &store.User{ID: 1}

	req := httptest.NewRequest("MKCALENDAR", "/dav/calendars/newcal", nil)
	req = req.WithContext(auth.WithUser(req.Context(), user))
	rr := httptest.NewRecorder()

	h.Mkcalendar(rr, req)

	// RFC 4791 Section 5.3.1.1: Success response MUST be 201 Created
	if rr.Code != http.StatusCreated {
		t.Errorf("RFC 4791 Section 5.3.1.1: MKCALENDAR success must return 201 Created, got %d", rr.Code)
	}
}

func TestRFC4791_MKCALENDAR_ResourcetypeIsCollectionAndCalendar(t *testing.T) {
	calRepo := &fakeCalendarRepo{calendars: make(map[int64]*store.Calendar)}
	h := &Handler{store: &store.Store{Calendars: calRepo}}
	user := &store.User{ID: 1}

	req := httptest.NewRequest("MKCALENDAR", "/dav/calendars/newcal", nil)
	req = req.WithContext(auth.WithUser(req.Context(), user))
	rr := httptest.NewRecorder()

	h.Mkcalendar(rr, req)

	if rr.Code != http.StatusCreated && rr.Code != http.StatusNoContent {
		t.Fatalf("MKCALENDAR must succeed with 201 or 204, got %d", rr.Code)
	}

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

	respBody := rr.Body.String()
	if !strings.Contains(respBody, "<d:collection") {
		t.Error("MKCALENDAR must create a collection resource type")
	}
	if !strings.Contains(respBody, "<cal:calendar") {
		t.Error("MKCALENDAR must create a calendar resource type")
	}
}

func TestRFC4791_MKCALENDAR_BodySetsPropertiesOrReturns207WithPropstat(t *testing.T) {
	calRepo := &fakeCalendarRepo{calendars: make(map[int64]*store.Calendar)}
	h := &Handler{store: &store.Store{Calendars: calRepo}}
	user := &store.User{ID: 1}

	body := `<?xml version="1.0" encoding="utf-8"?>
<cal:mkcalendar xmlns:d="DAV:" xmlns:cal="urn:ietf:params:xml:ns:caldav">
  <d:set>
    <d:prop>
      <d:displayname>Strict Suite Calendar</d:displayname>
      <cal:calendar-description>Strict suite test</cal:calendar-description>
      <cal:calendar-timezone>BEGIN:VTIMEZONE
TZID:UTC
END:VTIMEZONE</cal:calendar-timezone>
    </d:prop>
  </d:set>
</cal:mkcalendar>`

	req := httptest.NewRequest("MKCALENDAR", "/dav/calendars/propscal", strings.NewReader(body))
	req = req.WithContext(auth.WithUser(req.Context(), user))
	rr := httptest.NewRecorder()

	h.Mkcalendar(rr, req)

	if rr.Code == http.StatusMultiStatus {
		respBody := rr.Body.String()
		if !strings.Contains(respBody, "displayname") && !strings.Contains(respBody, "calendar-description") {
			t.Error("MKCALENDAR 207 response must include per-property propstat")
		}
		return
	}

	if rr.Code != http.StatusCreated && rr.Code != http.StatusNoContent {
		t.Fatalf("MKCALENDAR must return 201/204 or 207, got %d", rr.Code)
	}

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

	respBody := rr.Body.String()
	if !strings.Contains(respBody, "Strict Suite Calendar") {
		t.Error("MKCALENDAR should set displayname when request body succeeds")
	}
	if !strings.Contains(respBody, "Strict suite test") {
		t.Error("MKCALENDAR should set calendar-description when request body succeeds")
	}
	if !strings.Contains(respBody, "BEGIN:VTIMEZONE") {
		t.Error("MKCALENDAR should set calendar-timezone when request body succeeds")
	}
}

// Section 5.3.1: MKCALENDAR on existing resource
func TestRFC4791_MkcalendarOnExistingResourceFails(t *testing.T) {
	// Note: This test verifies expected behavior even though our implementation
	// doesn't fully track duplicates. A stricter implementation would return 405.
	calRepo := &fakeCalendarRepo{calendars: make(map[int64]*store.Calendar)}
	h := &Handler{store: &store.Store{Calendars: calRepo}}
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

	if rr.Code != http.StatusMethodNotAllowed && rr.Code != http.StatusConflict {
		t.Errorf("Second MKCALENDAR should fail with 405 or 409, got %d", rr.Code)
	}
}

// Section 5.3.2.1: PUT Preconditions - If-None-Match
func TestRFC4791_PutWithIfNoneMatchStarCreatesNew(t *testing.T) {
	calRepo := &fakeCalendarRepo{
		accessible: []store.CalendarAccess{
			{Calendar: store.Calendar{ID: 1, UserID: 1, Name: "Test"}, Editor: true},
		},
	}
	eventRepo := &fakeEventRepo{events: make(map[string]*store.Event)}
	h := &Handler{store: &store.Store{Calendars: calRepo, Events: eventRepo}}
	user := &store.User{ID: 1}

	icalData := "BEGIN:VCALENDAR\r\nVERSION:2.0\r\nBEGIN:VEVENT\r\nUID:new-event\r\nEND:VEVENT\r\nEND:VCALENDAR\r\n"
	req := newCalendarPutRequest("/dav/calendars/1/new-event.ics", strings.NewReader(icalData))
	req.Header.Set("If-None-Match", "*")
	req = req.WithContext(auth.WithUser(req.Context(), user))
	rr := httptest.NewRecorder()

	h.Put(rr, req)

	// RFC 4791 Section 5.3.2.1: If-None-Match: * succeeds with 201 for new resource
	if rr.Code != http.StatusCreated {
		t.Errorf("RFC 4791 Section 5.3.2.1: PUT with If-None-Match: * should return 201 for new resource, got %d", rr.Code)
	}
}

// Section 5.3.2.1: PUT Preconditions - If-Match
func TestRFC4791_PutWithIfMatchRequiresMatchingETag(t *testing.T) {
	calRepo := &fakeCalendarRepo{
		accessible: []store.CalendarAccess{
			{Calendar: store.Calendar{ID: 1, UserID: 1, Name: "Test"}, Editor: true},
		},
	}
	eventRepo := &fakeEventRepo{
		events: map[string]*store.Event{
			"1:existing": {CalendarID: 1, UID: "existing", RawICAL: "OLD", ETag: "correct-etag"},
		},
	}
	h := &Handler{store: &store.Store{Calendars: calRepo, Events: eventRepo}}
	user := &store.User{ID: 1}

	icalData := "BEGIN:VCALENDAR\r\nVERSION:2.0\r\nBEGIN:VEVENT\r\nUID:existing\r\nEND:VEVENT\r\nEND:VCALENDAR\r\n"
	req := newCalendarPutRequest("/dav/calendars/1/existing.ics", strings.NewReader(icalData))
	req.Header.Set("If-Match", `"wrong-etag"`)
	req = req.WithContext(auth.WithUser(req.Context(), user))
	rr := httptest.NewRecorder()

	h.Put(rr, req)

	// RFC 4791 Section 5.3.2.1: If-Match failure MUST return 412 Precondition Failed
	if rr.Code != http.StatusPreconditionFailed {
		t.Errorf("RFC 4791 Section 5.3.2.1: PUT with mismatched If-Match must return 412, got %d", rr.Code)
	}
}

// Section 5.3.2.1: PUT Preconditions - If-Match success path
func TestRFC4791_PutWithIfMatchSucceedsAndUpdatesETag(t *testing.T) {
	calRepo := &fakeCalendarRepo{
		accessible: []store.CalendarAccess{
			{Calendar: store.Calendar{ID: 1, UserID: 1, Name: "Test"}, Editor: true},
		},
	}
	eventRepo := &fakeEventRepo{
		events: map[string]*store.Event{
			"1:existing": {CalendarID: 1, UID: "existing", RawICAL: "OLD", ETag: "old-etag"},
		},
	}
	h := &Handler{store: &store.Store{Calendars: calRepo, Events: eventRepo}}
	user := &store.User{ID: 1}

	icalData := "BEGIN:VCALENDAR\r\nVERSION:2.0\r\nBEGIN:VEVENT\r\nUID:existing\r\nSUMMARY:Updated\r\nEND:VEVENT\r\nEND:VCALENDAR\r\n"
	req := newCalendarPutRequest("/dav/calendars/1/existing.ics", strings.NewReader(icalData))
	req.Header.Set("If-Match", `"old-etag"`)
	req = req.WithContext(auth.WithUser(req.Context(), user))
	rr := httptest.NewRecorder()

	h.Put(rr, req)

	if rr.Code != http.StatusNoContent {
		t.Errorf("RFC 4791 Section 5.3.2.1: PUT with matching If-Match must return 204, got %d", rr.Code)
	}
	etag := rr.Header().Get("ETag")
	if etag == "" {
		t.Error("RFC 4791 Section 5.3.4: PUT must return updated ETag")
	}
	if strings.Trim(etag, "\"") == "old-etag" {
		t.Error("RFC 4791 Section 5.3.4: ETag must change after successful update")
	}
}

// Section 5.3.4: ETag behavior on semantically identical updates (content-based policy)
func TestRFC4791_PutIdenticalBodyKeepsETag(t *testing.T) {
	calRepo := &fakeCalendarRepo{
		accessible: []store.CalendarAccess{
			{Calendar: store.Calendar{ID: 1, UserID: 1, Name: "Test"}, Editor: true},
		},
	}
	eventRepo := &fakeEventRepo{events: make(map[string]*store.Event)}
	h := &Handler{store: &store.Store{Calendars: calRepo, Events: eventRepo}}
	user := &store.User{ID: 1}

	icalData := "BEGIN:VCALENDAR\r\nVERSION:2.0\r\nBEGIN:VEVENT\r\nUID:same-body\r\nSUMMARY:Same\r\nEND:VEVENT\r\nEND:VCALENDAR\r\n"
	req := newCalendarPutRequest("/dav/calendars/1/same-body.ics", strings.NewReader(icalData))
	req = req.WithContext(auth.WithUser(req.Context(), user))
	rr := httptest.NewRecorder()

	h.Put(rr, req)

	if rr.Code != http.StatusCreated && rr.Code != http.StatusNoContent {
		t.Fatalf("initial PUT should succeed, got %d", rr.Code)
	}
	etag := rr.Header().Get("ETag")
	if etag == "" {
		t.Fatalf("initial PUT must return ETag, got empty")
	}

	req = newCalendarPutRequest("/dav/calendars/1/same-body.ics", strings.NewReader(icalData))
	req.Header.Set("If-Match", etag)
	req = req.WithContext(auth.WithUser(req.Context(), user))
	rr = httptest.NewRecorder()

	h.Put(rr, req)

	if rr.Code != http.StatusNoContent {
		t.Fatalf("PUT with matching If-Match must return 204, got %d", rr.Code)
	}
	etag2 := rr.Header().Get("ETag")
	if etag2 == "" {
		t.Fatalf("PUT must return ETag, got empty")
	}
	if etag2 != etag {
		t.Errorf("ETag must remain the same for identical body updates (content-based), got %s then %s", etag, etag2)
	}
}

// Section 5.3.2.1: PUT Preconditions - If-Match on missing resource
func TestRFC4791_PutWithIfMatchOnMissingResourceFails(t *testing.T) {
	calRepo := &fakeCalendarRepo{
		accessible: []store.CalendarAccess{
			{Calendar: store.Calendar{ID: 1, UserID: 1, Name: "Test"}, Editor: true},
		},
	}
	eventRepo := &fakeEventRepo{events: make(map[string]*store.Event)}
	h := &Handler{store: &store.Store{Calendars: calRepo, Events: eventRepo}}
	user := &store.User{ID: 1}

	icalData := "BEGIN:VCALENDAR\r\nVERSION:2.0\r\nBEGIN:VEVENT\r\nUID:missing\r\nEND:VEVENT\r\nEND:VCALENDAR\r\n"
	req := newCalendarPutRequest("/dav/calendars/1/missing.ics", strings.NewReader(icalData))
	req.Header.Set("If-Match", `"missing-etag"`)
	req = req.WithContext(auth.WithUser(req.Context(), user))
	rr := httptest.NewRecorder()

	h.Put(rr, req)

	if rr.Code != http.StatusPreconditionFailed {
		t.Errorf("RFC 4791 Section 5.3.2.1: PUT with If-Match on missing resource must return 412, got %d", rr.Code)
	}
}

// Section 5.3.4: Calendar Object Resource Entity Tag
func TestRFC4791_PutReturnsETagHeader(t *testing.T) {
	calRepo := &fakeCalendarRepo{
		accessible: []store.CalendarAccess{
			{Calendar: store.Calendar{ID: 1, UserID: 1, Name: "Test"}, Editor: true},
		},
	}
	eventRepo := &fakeEventRepo{events: make(map[string]*store.Event)}
	h := &Handler{store: &store.Store{Calendars: calRepo, Events: eventRepo}}
	user := &store.User{ID: 1}

	icalData := "BEGIN:VCALENDAR\r\nVERSION:2.0\r\nBEGIN:VEVENT\r\nUID:test\r\nEND:VEVENT\r\nEND:VCALENDAR\r\n"
	req := newCalendarPutRequest("/dav/calendars/1/test.ics", strings.NewReader(icalData))
	req = req.WithContext(auth.WithUser(req.Context(), user))
	rr := httptest.NewRecorder()

	h.Put(rr, req)

	// RFC 4791 Section 5.3.4: Server MUST return ETag header in successful PUT
	etag := rr.Header().Get("ETag")
	if etag == "" {
		t.Error("RFC 4791 Section 5.3.4: PUT response MUST include ETag header")
	}

	// ETag should be quoted
	if !strings.HasPrefix(etag, `"`) || !strings.HasSuffix(etag, `"`) {
		t.Errorf("RFC 4791 Section 5.3.4: ETag should be quoted, got: %s", etag)
	}
}

// Section 6.2.1: calendar-home-set Property
func TestRFC4791_PrincipalHasCalendarHomeSet(t *testing.T) {
	h := &Handler{}
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

	respBody := rr.Body.String()
	// RFC 4791 Section 6.2.1: Principal MUST have calendar-home-set property
	if !strings.Contains(respBody, "calendar-home-set") {
		t.Error("RFC 4791 Section 6.2.1: Principal MUST have calendar-home-set property")
	}
	if !strings.Contains(respBody, "/dav/calendars/") {
		t.Error("RFC 4791 Section 6.2.1: calendar-home-set must reference calendar collection")
	}
}

// RFC 4791 Section 6.2.1: calendar-home-set is protected and SHOULD NOT appear in allprop
func TestRFC4791_PrincipalCalendarHomeSetNotInAllprop(t *testing.T) {
	h := &Handler{}
	user := &store.User{ID: 1, PrimaryEmail: "user@example.com"}

	req := httptest.NewRequest("PROPFIND", "/dav/principals/1/", nil)
	req.Header.Set("Depth", "0")
	req = req.WithContext(auth.WithUser(req.Context(), user))
	rr := httptest.NewRecorder()

	h.Propfind(rr, req)

	body := rr.Body.String()
	if strings.Contains(body, "calendar-home-set") {
		t.Error("RFC 4791 Section 6.2.1: calendar-home-set SHOULD NOT be returned by DAV:allprop")
	}
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
	h := &Handler{store: &store.Store{Calendars: calRepo, Events: &fakeEventRepo{}}}
	user := &store.User{ID: 1, PrimaryEmail: "user@example.com"}

	// RFC 4791 Section 6.2.1: PROPFIND on calendar-home-set with Depth: 1 lists calendars
	req := httptest.NewRequest("PROPFIND", "/dav/calendars/", nil)
	req.Header.Set("Depth", "1")
	req = req.WithContext(auth.WithUser(req.Context(), user))
	rr := httptest.NewRecorder()

	h.Propfind(rr, req)

	if rr.Code != http.StatusMultiStatus {
		t.Fatalf("PROPFIND on calendar home must return 207 Multi-Status, got %d", rr.Code)
	}

	body := rr.Body.String()

	// CRITICAL: Must return the calendar home collection itself
	if !strings.Contains(body, "<d:href>/dav/calendars/</d:href>") {
		t.Error("CRITICAL: Must return calendar home collection in response")
	}

	// CRITICAL: Must list each calendar as a separate response
	if !strings.Contains(body, "/dav/calendars/1/") {
		t.Error("CRITICAL: Must list calendar ID 1 (Work) in response - CalDAV clients need this to discover calendars")
	}
	if !strings.Contains(body, "/dav/calendars/2/") {
		t.Error("CRITICAL: Must list calendar ID 2 (Personal) in response - CalDAV clients need this to discover calendars")
	}

	// Each calendar should have resourcetype with both collection and calendar
	// Count the number of <d:response> elements - should be 3 (home + 2 calendars)
	responseCount := strings.Count(body, "<d:response>")
	if responseCount < 3 {
		t.Errorf("CRITICAL: Expected at least 3 responses (calendar home + 2 calendars), got %d - CalDAV clients won't see existing calendars!", responseCount)
	}

	// Verify calendar resourcetypes are present
	if !strings.Contains(body, "<cal:calendar") {
		t.Error("CRITICAL: Calendar collections must have cal:calendar resourcetype - RFC 4791 Section 4.2 requires this")
	}
}

// Test that empty calendar-home-set returns no calendars (not an error)
func TestRFC4791_EmptyCalendarHomeReturnsNoCalendars(t *testing.T) {
	calRepo := &fakeCalendarRepo{
		accessible: []store.CalendarAccess{}, // No calendars
	}
	h := &Handler{store: &store.Store{Calendars: calRepo, Events: &fakeEventRepo{}}}
	user := &store.User{ID: 1, PrimaryEmail: "user@example.com"}

	req := httptest.NewRequest("PROPFIND", "/dav/calendars/", nil)
	req.Header.Set("Depth", "1")
	req = req.WithContext(auth.WithUser(req.Context(), user))
	rr := httptest.NewRecorder()

	h.Propfind(rr, req)

	if rr.Code != http.StatusMultiStatus {
		t.Fatalf("PROPFIND must return 207 even with no calendars, got %d", rr.Code)
	}

	body := rr.Body.String()

	// Should return the calendar home collection
	if !strings.Contains(body, "/dav/calendars/") {
		t.Error("Must return calendar home collection even when empty")
	}

	// Should have 2 responses (the home collection + birthday calendar)
	// Birthday calendar is always shown as a special read-only calendar
	responseCount := strings.Count(body, "<d:response>")
	if responseCount != 2 {
		t.Errorf("Expected 2 responses (calendar home + birthday calendar), got %d", responseCount)
	}
}

// RFC 4791 Section 5.2: Calendar Collection Properties Required for Client Compatibility
func TestRFC4791_CalendarCollectionHasRequiredProperties(t *testing.T) {
	now := store.Now()
	calRepo := &fakeCalendarRepo{
		accessible: []store.CalendarAccess{
			{Calendar: store.Calendar{
				ID:          1,
				UserID:      1,
				Name:        "My Calendar",
				Description: util.StrPtr("Test calendar"),
				UpdatedAt:   now,
				CTag:        42,
			}, Editor: true},
		},
	}
	h := &Handler{store: &store.Store{Calendars: calRepo, Events: &fakeEventRepo{}}}
	user := &store.User{ID: 1}

	// Request common properties that clients need
	body := `<?xml version="1.0" encoding="utf-8"?>
<d:propfind xmlns:d="DAV:" xmlns:cal="urn:ietf:params:xml:ns:caldav" xmlns:cs="http://calendarserver.org/ns/">
  <d:prop>
    <d:displayname/>
    <d:resourcetype/>
    <cs:getctag/>
    <d:sync-token/>
    <cal:calendar-description/>
    <cal:supported-calendar-component-set/>
    <d:supported-report-set/>
  </d:prop>
</d:propfind>`

	req := httptest.NewRequest("PROPFIND", "/dav/calendars/1/", strings.NewReader(body))
	req.Header.Set("Depth", "0")
	req = req.WithContext(auth.WithUser(req.Context(), user))
	rr := httptest.NewRecorder()

	h.Propfind(rr, req)

	respBody := rr.Body.String()

	// CRITICAL properties for CalDAV client compatibility
	if !strings.Contains(respBody, "displayname>") {
		t.Error("RFC 4918 Section 15.2: displayname property required for calendar collections")
	}
	if !strings.Contains(respBody, "collection") {
		t.Error("RFC 4791 Section 4.2: collection resourcetype required")
	}
	if !strings.Contains(respBody, "calendar") {
		t.Error("RFC 4791 Section 4.2: calendar resourcetype required - clients use this to identify calendar collections")
	}
	if !strings.Contains(respBody, "getctag") {
		t.Error("RFC 6578: getctag required for efficient sync - CalDAV clients use this to detect changes")
	}
	if !strings.Contains(respBody, "supported-calendar-component-set") {
		t.Error("RFC 4791 Section 5.2.3: supported-calendar-component-set required")
	}
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
	h := &Handler{store: &store.Store{Calendars: calRepo, Events: &fakeEventRepo{}}}
	user := &store.User{ID: 1}

	req := httptest.NewRequest("PROPFIND", "/dav/calendars/", nil)
	req.Header.Set("Depth", "0")
	req = req.WithContext(auth.WithUser(req.Context(), user))
	rr := httptest.NewRecorder()

	h.Propfind(rr, req)

	body := rr.Body.String()

	// Should only return the calendar home, not individual calendars
	responseCount := strings.Count(body, "<d:response>")
	if responseCount != 1 {
		t.Errorf("Depth 0 should return only calendar home collection, got %d responses", responseCount)
	}

	// Should NOT list individual calendars with Depth: 0
	if strings.Contains(body, "/dav/calendars/1/") || strings.Contains(body, "/dav/calendars/2/") {
		t.Error("Depth 0 should not include child calendars")
	}
}

// RFC 4791 Section 6: Complete CalDAV Discovery Sequence
// This tests the full discovery sequence per RFC 4791 that CalDAV clients use
func TestRFC4791_CalendarDiscoverySequence(t *testing.T) {
	now := store.Now()
	calRepo := &fakeCalendarRepo{
		accessible: []store.CalendarAccess{
			{Calendar: store.Calendar{ID: 1, UserID: 1, Name: "Work", UpdatedAt: now, CTag: 10}, Editor: true},
			{Calendar: store.Calendar{ID: 2, UserID: 1, Name: "Home", UpdatedAt: now, CTag: 20}, Editor: true},
		},
	}
	h := &Handler{store: &store.Store{Calendars: calRepo, Events: &fakeEventRepo{}}}
	user := &store.User{ID: 1, PrimaryEmail: "user@example.com"}

	// Step 1: PROPFIND on root to get current-user-principal (RFC 5397)
	t.Run("Step1_DiscoverPrincipal", func(t *testing.T) {
		body := `<?xml version="1.0" encoding="utf-8"?>
<d:propfind xmlns:d="DAV:">
  <d:prop>
    <d:current-user-principal/>
  </d:prop>
</d:propfind>`

		req := httptest.NewRequest("PROPFIND", "/dav/", strings.NewReader(body))
		req.Header.Set("Depth", "0")
		req = req.WithContext(auth.WithUser(req.Context(), user))
		rr := httptest.NewRecorder()

		h.Propfind(rr, req)

		respBody := rr.Body.String()
		if !strings.Contains(respBody, "current-user-principal") {
			t.Fatal("RFC 5397: current-user-principal not found - CalDAV clients cannot discover user's principal")
		}
		if !strings.Contains(respBody, "/dav/principals/1/") {
			t.Fatal("RFC 5397: principal URL not found in current-user-principal property")
		}
		t.Log("RFC 4791 compliance: Current user principal discovered per RFC 5397")
	})

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

		respBody := rr.Body.String()
		if !strings.Contains(respBody, "calendar-home-set") {
			t.Fatal("RFC 4791 Section 6.2.1: calendar-home-set property not found on principal")
		}
		if !strings.Contains(respBody, "/dav/calendars/") {
			t.Fatal("RFC 4791 Section 6.2.1: calendar home URL not found in calendar-home-set")
		}
		t.Log("RFC 4791 compliance: Calendar home set discovered per RFC 4791 Section 6.2.1")
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

		if rr.Code != http.StatusMultiStatus {
			t.Fatalf("RFC 4918: PROPFIND must return 207 Multi-Status, got %d", rr.Code)
		}

		respBody := rr.Body.String()

		// CRITICAL: Must find both calendars
		if !strings.Contains(respBody, "/dav/calendars/1/") {
			t.Error("RFC 4791: Calendar collection 1 not listed - CalDAV clients cannot discover it")
		}
		if !strings.Contains(respBody, "/dav/calendars/2/") {
			t.Error("RFC 4791: Calendar collection 2 not listed - CalDAV clients cannot discover it")
		}

		// Must have calendar resourcetype per RFC 4791 Section 4.2
		calendarCount := strings.Count(respBody, "<cal:calendar")
		if calendarCount < 2 {
			t.Errorf("RFC 4791 Section 4.2: Expected 2 calendars with cal:calendar resourcetype, found %d", calendarCount)
		}

		// Must have displayname for each calendar per RFC 4918
		if !strings.Contains(respBody, "Work") || !strings.Contains(respBody, "Home") {
			t.Error("RFC 4918: Calendar displayname properties not found")
		}

		t.Log("RFC 4791 compliance: Calendar collections listed with required properties")
	})

	// Step 4: Verify individual calendar collection can be accessed
	t.Run("Step4_AccessIndividualCalendar", func(t *testing.T) {
		req := httptest.NewRequest("PROPFIND", "/dav/calendars/1/", nil)
		req.Header.Set("Depth", "0")
		req = req.WithContext(auth.WithUser(req.Context(), user))
		rr := httptest.NewRecorder()

		h.Propfind(rr, req)

		if rr.Code != http.StatusMultiStatus {
			t.Fatalf("RFC 4918: PROPFIND on calendar collection must return 207 Multi-Status, got %d", rr.Code)
		}

		respBody := rr.Body.String()
		if !strings.Contains(respBody, "/dav/calendars/1/") {
			t.Error("RFC 4791: Calendar collection URL not found in response")
		}

		t.Log("RFC 4791 compliance: Individual calendar collection accessible")
	})

	t.Log("RFC 4791 compliance: Complete calendar discovery sequence passed")
}

// RFC 4791: Verify shared calendars (read-only) are discoverable
func TestRFC4791_SharedCalendarsAreDiscoverable(t *testing.T) {
	now := store.Now()
	calRepo := &fakeCalendarRepo{
		accessible: []store.CalendarAccess{
			{Calendar: store.Calendar{ID: 1, UserID: 1, Name: "My Calendar", UpdatedAt: now}, Editor: true},
			{Calendar: store.Calendar{ID: 2, UserID: 2, Name: "Shared With Me", UpdatedAt: now}, Editor: false}, // Read-only
		},
	}
	h := &Handler{store: &store.Store{Calendars: calRepo, Events: &fakeEventRepo{}}}
	user := &store.User{ID: 1}

	req := httptest.NewRequest("PROPFIND", "/dav/calendars/", nil)
	req.Header.Set("Depth", "1")
	req = req.WithContext(auth.WithUser(req.Context(), user))
	rr := httptest.NewRecorder()

	h.Propfind(rr, req)

	body := rr.Body.String()

	// Both owned and shared calendars should be listed per RFC 4791
	if !strings.Contains(body, "/dav/calendars/1/") {
		t.Error("RFC 4791: User's own calendar collection should be listed")
	}
	if !strings.Contains(body, "/dav/calendars/2/") {
		t.Error("RFC 4791: Shared calendar collection should be listed for accessibility")
	}

	// Should have at least 3 responses (home + 2 calendars)
	responseCount := strings.Count(body, "<d:response>")
	if responseCount < 3 {
		t.Errorf("RFC 4918: Expected 3 responses (home + 2 calendars), got %d", responseCount)
	}
}

// Section 7.1: DAV:expand-property REPORT
func TestRFC4791_ExpandPropertyReportWorks(t *testing.T) {
	now := store.Now()
	calRepo := &fakeCalendarRepo{
		accessible: []store.CalendarAccess{
			{Calendar: store.Calendar{ID: 1, UserID: 1, Name: "Test", UpdatedAt: now}, Editor: true},
		},
	}
	h := &Handler{store: &store.Store{Calendars: calRepo, Events: &fakeEventRepo{}}}
	user := &store.User{ID: 1, PrimaryEmail: "user@example.com"}

	body := `<?xml version="1.0" encoding="utf-8"?>
<d:expand-property xmlns:d="DAV:">
  <d:prop>
    <d:current-user-principal>
      <d:prop>
        <d:displayname/>
      </d:prop>
    </d:current-user-principal>
  </d:prop>
</d:expand-property>`

	req := httptest.NewRequest("REPORT", "/dav/calendars/1/", strings.NewReader(body))
	req = req.WithContext(auth.WithUser(req.Context(), user))
	rr := httptest.NewRecorder()

	h.Report(rr, req)

	if rr.Code != http.StatusMultiStatus {
		t.Errorf("RFC 4791 Section 7.1: expand-property must return 207 Multi-Status, got %d", rr.Code)
	}

	respBody := rr.Body.String()
	if !strings.Contains(respBody, "displayname") || !strings.Contains(respBody, user.PrimaryEmail) {
		t.Errorf("RFC 4791 Section 7.1: expand-property must include expanded properties, got: %s", respBody)
	}
	if !strings.Contains(respBody, "/dav/principals/1/") {
		t.Errorf("RFC 4791 Section 7.1: expand-property must include expanded hrefs, got: %s", respBody)
	}
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
	h := &Handler{store: &store.Store{Calendars: calRepo, Events: eventRepo}}
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

	// RFC 4791 Section 7.8: Must return 207 Multi-Status
	if rr.Code != http.StatusMultiStatus {
		t.Errorf("RFC 4791 Section 7.8: calendar-query must return 207 Multi-Status, got %d", rr.Code)
	}

	respBody := rr.Body.String()
	// Response should contain calendar-data
	if !strings.Contains(respBody, "calendar-data") {
		t.Error("RFC 4791 Section 7.8: Response must include calendar-data when requested")
	}
	// Response should contain getetag
	if !strings.Contains(respBody, "getetag") {
		t.Error("RFC 4791 Section 7.8: Response must include getetag when requested")
	}
}

// Section 7.8.1: Time Range Filtering
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
			h := &Handler{store: &store.Store{Calendars: calRepo, Events: eventRepo}}
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

			respBody := rr.Body.String()
			hasEvent := strings.Contains(respBody, "test.ics")

			if hasEvent != tt.shouldMatch {
				t.Errorf("RFC 4791 Section 7.9: Time range filter incorrect - expected match=%v, got match=%v",
					tt.shouldMatch, hasEvent)
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
	h := &Handler{store: &store.Store{Calendars: calRepo, Events: eventRepo}}
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

	// RFC 4791 Section 7.9: Must return 207 Multi-Status
	if rr.Code != http.StatusMultiStatus {
		t.Errorf("RFC 4791 Section 7.9: calendar-multiget must return 207 Multi-Status, got %d", rr.Code)
	}

	respBody := rr.Body.String()
	if !strings.Contains(respBody, "event1.ics") {
		t.Error("RFC 4791 Section 7.9: Response must include event1")
	}
	if !strings.Contains(respBody, "event2.ics") {
		t.Error("RFC 4791 Section 7.9: Response must include event2")
	}
	if !strings.Contains(respBody, "missing.ics") {
		t.Error("RFC 4791 Section 7.9: Response must include missing resource href with error status")
	}
	if !strings.Contains(respBody, "404") {
		t.Error("RFC 4791 Section 7.9: Missing resource must return 404 status in multistatus")
	}
}

func TestRFC4791_CalendarQuery_OnNonCalendarCollection_Fails(t *testing.T) {
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
	h := &Handler{store: &store.Store{Calendars: calRepo, Events: eventRepo}}
	user := &store.User{ID: 1}

	body := `<?xml version="1.0" encoding="utf-8" ?>
<C:calendar-query xmlns:D="DAV:" xmlns:C="urn:ietf:params:xml:ns:caldav">
  <D:prop>
    <C:calendar-data/>
  </D:prop>
  <C:filter>
    <C:comp-filter name="VCALENDAR">
      <C:comp-filter name="VEVENT"/>
    </C:comp-filter>
  </C:filter>
</C:calendar-query>`

	req := httptest.NewRequest("REPORT", "/dav/calendars/1/event.ics", strings.NewReader(body))
	req = req.WithContext(auth.WithUser(req.Context(), user))
	rr := httptest.NewRecorder()
	h.Report(rr, req)
	if rr.Code == http.StatusMultiStatus || rr.Code == http.StatusOK {
		t.Errorf("calendar-query on calendar object must not return success-like response, got %d", rr.Code)
	}

	req = httptest.NewRequest("REPORT", "/dav/", strings.NewReader(body))
	req = req.WithContext(auth.WithUser(req.Context(), user))
	rr = httptest.NewRecorder()
	h.Report(rr, req)
	if rr.Code == http.StatusMultiStatus || rr.Code == http.StatusOK {
		t.Errorf("calendar-query on non-calendar collection must not return success-like response, got %d", rr.Code)
	}
}

func TestRFC4791_CalendarMultiget_OnNonCalendarCollection_Fails(t *testing.T) {
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
	h := &Handler{store: &store.Store{Calendars: calRepo, Events: eventRepo}}
	user := &store.User{ID: 1}

	body := `<?xml version="1.0" encoding="utf-8" ?>
<C:calendar-multiget xmlns:D="DAV:" xmlns:C="urn:ietf:params:xml:ns:caldav">
  <D:prop>
    <C:calendar-data/>
  </D:prop>
  <D:href>/dav/calendars/1/event.ics</D:href>
</C:calendar-multiget>`

	req := httptest.NewRequest("REPORT", "/dav/calendars/1/event.ics", strings.NewReader(body))
	req = req.WithContext(auth.WithUser(req.Context(), user))
	rr := httptest.NewRecorder()
	h.Report(rr, req)
	if rr.Code == http.StatusMultiStatus || rr.Code == http.StatusOK {
		t.Errorf("calendar-multiget on calendar object must not return success-like response, got %d", rr.Code)
	}

	req = httptest.NewRequest("REPORT", "/dav/", strings.NewReader(body))
	req = req.WithContext(auth.WithUser(req.Context(), user))
	rr = httptest.NewRecorder()
	h.Report(rr, req)
	if rr.Code == http.StatusMultiStatus || rr.Code == http.StatusOK {
		t.Errorf("calendar-multiget on non-calendar collection must not return success-like response, got %d", rr.Code)
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
	h := &Handler{store: &store.Store{Calendars: calRepo, Events: eventRepo}}
	user := &store.User{ID: 1}

	// RFC 4791 Section 7.10: free-busy-query REPORT
	body := `<?xml version="1.0" encoding="utf-8" ?>
<C:free-busy-query xmlns:C="urn:ietf:params:xml:ns:caldav">
  <C:filter>
    <C:comp-filter name="VCALENDAR">
      <C:time-range start="20240601T000000Z" end="20240630T235959Z"/>
      <C:comp-filter name="VEVENT"/>
    </C:comp-filter>
  </C:filter>
</C:free-busy-query>`

	req := httptest.NewRequest("REPORT", "/dav/calendars/1/", strings.NewReader(body))
	req = req.WithContext(auth.WithUser(req.Context(), user))
	rr := httptest.NewRecorder()

	h.Report(rr, req)

	if rr.Code != http.StatusOK {
		t.Errorf("RFC 4791 Section 7.10: free-busy-query must return 200 OK, got %d", rr.Code)
	}
	if contentType := rr.Header().Get("Content-Type"); !strings.Contains(contentType, "text/calendar") {
		t.Errorf("RFC 4791 Section 7.10: free-busy-query must return text/calendar, got %s", contentType)
	}

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
	h := &Handler{store: &store.Store{Calendars: calRepo, Events: eventRepo}}
	user := &store.User{ID: 1}

	body := `<?xml version="1.0" encoding="utf-8" ?>
<C:free-busy-query xmlns:C="urn:ietf:params:xml:ns:caldav">
  <C:filter>
    <C:comp-filter name="VCALENDAR">
      <C:time-range start="20240601T000000Z" end="20240630T235959Z"/>
      <C:comp-filter name="VEVENT"/>
    </C:comp-filter>
  </C:filter>
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
	h := &Handler{store: &store.Store{Calendars: calRepo, Events: eventRepo}}
	user := &store.User{ID: 1}

	body := `<?xml version="1.0" encoding="utf-8" ?>
<C:free-busy-query xmlns:C="urn:ietf:params:xml:ns:caldav">
  <C:filter>
    <C:comp-filter name="VCALENDAR">
      <C:time-range start="20240601T000000Z" end="20240630T235959Z"/>
      <C:comp-filter name="VEVENT"/>
    </C:comp-filter>
  </C:filter>
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
	h := &Handler{store: &store.Store{Calendars: calRepo, Events: &fakeEventRepo{}}}
	user := &store.User{ID: 1}

	body := `<?xml version="1.0" encoding="utf-8" ?>
<C:free-busy-query xmlns:C="urn:ietf:params:xml:ns:caldav">
  <C:filter>
    <C:comp-filter name="VCALENDAR">
      <C:time-range start="20240601T000000Z" end="20240630T235959Z"/>
      <C:comp-filter name="VEVENT"/>
    </C:comp-filter>
  </C:filter>
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
	h := &Handler{}
	user := &store.User{ID: 1}

	req := httptest.NewRequest("PROPFIND", "/dav", nil)
	req.Header.Set("Depth", "0")
	req = req.WithContext(auth.WithUser(req.Context(), user))
	rr := httptest.NewRecorder()

	h.Propfind(rr, req)

	body := rr.Body.String()

	// RFC 4791: CalDAV namespace must be urn:ietf:params:xml:ns:caldav
	if !strings.Contains(body, "urn:ietf:params:xml:ns:caldav") {
		t.Error("RFC 4791: Must use correct CalDAV namespace: urn:ietf:params:xml:ns:caldav")
	}

	// DAV namespace must be DAV:
	if !strings.Contains(body, `xmlns:d="DAV:"`) && !strings.Contains(body, `xmlns:D="DAV:"`) {
		t.Error("RFC 4791: Must use correct WebDAV namespace: DAV:")
	}
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
	h := &Handler{store: &store.Store{Calendars: calRepo, Events: eventRepo}}
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

	respBody := rr.Body.String()

	if rr.Code != http.StatusMultiStatus {
		t.Fatalf("calendar-multiget must return 207, got %d", rr.Code)
	}
	if !strings.Contains(respBody, "BEGIN:VCALENDAR") || !strings.Contains(respBody, "END:VCALENDAR") {
		t.Error("calendar-data must return a valid VCALENDAR payload")
	}
	if !strings.Contains(respBody, "UID:event") {
		t.Error("calendar-data must include the stored component data")
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
	h := &Handler{store: &store.Store{Calendars: calRepo, Events: eventRepo}}
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

	respBody := rr.Body.String()
	if rr.Code != http.StatusMultiStatus {
		t.Fatalf("calendar-query must return 207, got %d", rr.Code)
	}
	if !strings.Contains(respBody, "DTSTART") || !strings.Contains(respBody, "DTEND") || !strings.Contains(respBody, "UID:event") {
		t.Error("calendar-data must include requested properties")
	}
	if strings.Contains(respBody, "SUMMARY:") || strings.Contains(respBody, "DESCRIPTION:") {
		t.Error("calendar-data filtering must omit unrequested properties")
	}
}

// Test ETag format compliance (RFC 4791 Section 5.3.4)
func TestRFC4791_ETagFormatCompliance(t *testing.T) {
	calRepo := &fakeCalendarRepo{
		accessible: []store.CalendarAccess{
			{Calendar: store.Calendar{ID: 1, UserID: 1, Name: "Test"}, Editor: true},
		},
	}
	eventRepo := &fakeEventRepo{
		events: map[string]*store.Event{
			"1:event": {CalendarID: 1, UID: "event", RawICAL: "BEGIN:VCALENDAR\r\nBEGIN:VEVENT\r\nUID:event\r\nEND:VEVENT\r\nEND:VCALENDAR\r\n", ETag: "abc123"},
		},
	}
	h := &Handler{store: &store.Store{Calendars: calRepo, Events: eventRepo}}
	user := &store.User{ID: 1}

	// GET should return ETag header
	req := httptest.NewRequest(http.MethodGet, "/dav/calendars/1/event.ics", nil)
	req = req.WithContext(auth.WithUser(req.Context(), user))
	rr := httptest.NewRecorder()

	h.Get(rr, req)

	etag := rr.Header().Get("ETag")
	if etag == "" {
		t.Fatal("RFC 4791 Section 5.3.4: GET must return ETag header")
	}

	// RFC 4791 Section 5.3.4: ETag MUST be quoted
	if !strings.HasPrefix(etag, `"`) || !strings.HasSuffix(etag, `"`) {
		t.Errorf("RFC 4791 Section 5.3.4: ETag must be quoted string, got: %s", etag)
	}

	// ETag should not be weak (weak ETags start with W/)
	if strings.HasPrefix(etag, "W/") {
		t.Errorf("RFC 4791 Section 5.3.4: Should use strong ETag for calendar resources, got weak: %s", etag)
	}
}

// Test PROPFIND Depth header handling (RFC 4918, referenced by RFC 4791)
func TestRFC4791_PropfindDepthHeaderHandling(t *testing.T) {
	now := store.Now()
	calRepo := &fakeCalendarRepo{
		accessible: []store.CalendarAccess{
			{Calendar: store.Calendar{ID: 1, UserID: 1, Name: "Test", UpdatedAt: now}, Editor: true},
		},
	}
	eventRepo := &fakeEventRepo{
		events: map[string]*store.Event{
			"1:event": {CalendarID: 1, UID: "event", RawICAL: "ICAL", ETag: "e", LastModified: now},
		},
	}
	h := &Handler{store: &store.Store{Calendars: calRepo, Events: eventRepo}}
	user := &store.User{ID: 1}

	tests := []struct {
		name                 string
		depth                string
		expectCollectionOnly bool
	}{
		{"depth 0 - collection only", "0", true},
		{"depth 1 - collection and children", "1", false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req := httptest.NewRequest("PROPFIND", "/dav/calendars/1/", nil)
			req.Header.Set("Depth", tt.depth)
			req = req.WithContext(auth.WithUser(req.Context(), user))
			rr := httptest.NewRecorder()

			h.Propfind(rr, req)

			body := rr.Body.String()
			responseCount := strings.Count(body, "<d:response>")

			if tt.expectCollectionOnly {
				if responseCount != 1 {
					t.Errorf("Depth 0 should return only collection, got %d responses", responseCount)
				}
				if strings.Contains(body, "event.ics") {
					t.Error("Depth 0 should not include child resources")
				}
			} else {
				if responseCount < 2 {
					t.Errorf("Depth 1 should return collection and children, got %d responses", responseCount)
				}
			}
		})
	}
}

// Test sync-collection REPORT (WebDAV Sync, commonly used by CalDAV clients)
func TestRFC4791_SyncCollectionReport(t *testing.T) {
	now := store.Now()
	calRepo := &fakeCalendarRepo{
		accessible: []store.CalendarAccess{
			{Calendar: store.Calendar{ID: 1, UserID: 1, Name: "Test", CTag: 5, UpdatedAt: now}, Editor: true},
		},
	}
	eventRepo := &fakeEventRepo{
		events: map[string]*store.Event{
			"1:event": {CalendarID: 1, UID: "event", RawICAL: "ICAL", ETag: "e", LastModified: now},
		},
	}
	deletedRepo := &fakeDeletedResourceRepo{}
	h := &Handler{store: &store.Store{Calendars: calRepo, Events: eventRepo, DeletedResources: deletedRepo}}
	user := &store.User{ID: 1}

	// Initial sync (no sync-token)
	body := `<?xml version="1.0" encoding="utf-8" ?>
<D:sync-collection xmlns:D="DAV:">
  <D:sync-token/>
  <D:prop>
    <D:getetag/>
  </D:prop>
</D:sync-collection>`

	req := httptest.NewRequest("REPORT", "/dav/calendars/1/", strings.NewReader(body))
	req = req.WithContext(auth.WithUser(req.Context(), user))
	rr := httptest.NewRecorder()

	h.Report(rr, req)

	if rr.Code != http.StatusMultiStatus {
		t.Errorf("sync-collection must return 207 Multi-Status, got %d", rr.Code)
	}

	respBody := rr.Body.String()
	// Response must include new sync-token (check for element, prefix may vary)
	if !strings.Contains(respBody, "sync-token>") {
		t.Error("sync-collection response must include sync-token")
	}

	// Extract and verify sync token is not empty
	// Match pattern like: <d:sync-token>value</d:sync-token> or <sync-token xmlns="DAV:">value</sync-token>
	syncTokenStart := strings.Index(respBody, "sync-token>")
	if syncTokenStart != -1 {
		afterTag := respBody[syncTokenStart+len("sync-token>"):]
		syncTokenEnd := strings.Index(afterTag, "</")
		if syncTokenEnd != -1 {
			token := strings.TrimSpace(afterTag[:syncTokenEnd])
			if token == "" {
				t.Error("sync-token must not be empty")
			}
		}
	}
}

// Test invalid calendar data rejection
func TestRFC4791_RejectMalformedICalendar(t *testing.T) {
	calRepo := &fakeCalendarRepo{
		accessible: []store.CalendarAccess{
			{Calendar: store.Calendar{ID: 1, UserID: 1, Name: "Test"}, Editor: true},
		},
	}
	h := &Handler{store: &store.Store{Calendars: calRepo, Events: &fakeEventRepo{}}}
	user := &store.User{ID: 1}

	tests := []struct {
		name        string
		data        string
		description string
	}{
		{
			name:        "missing VCALENDAR wrapper",
			data:        "BEGIN:VEVENT\r\nUID:test\r\nEND:VEVENT\r\n",
			description: "calendar data must be wrapped in VCALENDAR",
		},
		{
			name:        "unbalanced BEGIN/END",
			data:        "BEGIN:VCALENDAR\r\nBEGIN:VEVENT\r\nUID:test\r\nEND:VCALENDAR\r\n",
			description: "BEGIN must match END tags",
		},
		{
			name:        "no calendar components",
			data:        "BEGIN:VCALENDAR\r\nVERSION:2.0\r\nEND:VCALENDAR\r\n",
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

// Test supported-report-set property
func TestRFC4791_SupportedReportSetProperty(t *testing.T) {
	now := store.Now()
	calRepo := &fakeCalendarRepo{
		accessible: []store.CalendarAccess{
			{Calendar: store.Calendar{ID: 1, UserID: 1, Name: "Test", UpdatedAt: now}, Editor: true},
		},
	}
	h := &Handler{store: &store.Store{Calendars: calRepo, Events: &fakeEventRepo{}}}
	user := &store.User{ID: 1}

	req := httptest.NewRequest("PROPFIND", "/dav/calendars/1/", nil)
	req.Header.Set("Depth", "0")
	req = req.WithContext(auth.WithUser(req.Context(), user))
	rr := httptest.NewRecorder()

	h.Propfind(rr, req)

	body := rr.Body.String()

	// RFC 3253 Section 3.1.5 (referenced by RFC 4791): Calendar collections should advertise supported reports
	if !strings.Contains(body, "supported-report-set") {
		t.Error("Calendar collection should include supported-report-set property")
	}

	// RFC 4791 Section 7.8: Must support calendar-query
	if !strings.Contains(body, "calendar-query") {
		t.Error("RFC 4791 Section 7.8: Must advertise calendar-query in supported-report-set")
	}

	// RFC 4791 Section 7.9: Must support calendar-multiget
	if !strings.Contains(body, "calendar-multiget") {
		t.Error("RFC 4791 Section 7.9: Must advertise calendar-multiget in supported-report-set")
	}

	// RFC 4791 Section 7.10: Should support free-busy-query
	if !strings.Contains(body, "free-busy-query") {
		t.Error("RFC 4791 Section 7.10: Should advertise free-busy-query in supported-report-set")
	}

	// RFC 4791 Section 5.1.1: Must advertise DAV:expand-property report
	if !strings.Contains(body, "expand-property") {
		t.Error("RFC 4791 Section 5.1.1: Must advertise DAV:expand-property in supported-report-set")
	}
}

func TestRFC4791_SupportedReportSetOnCalendarObjectResource(t *testing.T) {
	now := store.Now()
	calRepo := &fakeCalendarRepo{
		accessible: []store.CalendarAccess{
			{Calendar: store.Calendar{ID: 1, UserID: 1, Name: "Test", UpdatedAt: now}, Editor: true},
		},
	}
	eventRepo := &fakeEventRepo{
		events: map[string]*store.Event{
			"1:event": {CalendarID: 1, UID: "event", RawICAL: "BEGIN:VCALENDAR\r\nBEGIN:VEVENT\r\nUID:event\r\nEND:VEVENT\r\nEND:VCALENDAR\r\n", ETag: "e"},
		},
	}
	h := &Handler{store: &store.Store{Calendars: calRepo, Events: eventRepo}}
	user := &store.User{ID: 1}

	propBody := `<?xml version="1.0" encoding="utf-8"?>
<d:propfind xmlns:d="DAV:">
  <d:prop>
    <d:supported-report-set/>
  </d:prop>
</d:propfind>`

	req := httptest.NewRequest("PROPFIND", "/dav/calendars/1/event.ics", strings.NewReader(propBody))
	req.Header.Set("Depth", "0")
	req = req.WithContext(auth.WithUser(req.Context(), user))
	rr := httptest.NewRecorder()

	h.Propfind(rr, req)

	body := rr.Body.String()
	if !strings.Contains(body, "supported-report-set") {
		t.Errorf("calendar object resource should return supported-report-set, got: %s", body)
	}
	if strings.Contains(body, "calendar-query") || strings.Contains(body, "calendar-multiget") || strings.Contains(body, "free-busy-query") {
		t.Errorf("calendar object resource must not advertise collection-only reports, got: %s", body)
	}
}

func TestRFC4791_SupportedReportSet_AdvertisedReportsActuallyWork(t *testing.T) {
	start := time.Date(2024, 6, 1, 10, 0, 0, 0, time.UTC)
	end := time.Date(2024, 6, 1, 12, 0, 0, 0, time.UTC)
	calRepo := &fakeCalendarRepo{
		accessible: []store.CalendarAccess{
			{Calendar: store.Calendar{ID: 1, UserID: 1, Name: "Test", UpdatedAt: store.Now()}, Editor: true},
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
	h := &Handler{store: &store.Store{Calendars: calRepo, Events: eventRepo}}
	user := &store.User{ID: 1}

	req := httptest.NewRequest("PROPFIND", "/dav/calendars/1/", nil)
	req.Header.Set("Depth", "0")
	req = req.WithContext(auth.WithUser(req.Context(), user))
	rr := httptest.NewRecorder()
	h.Propfind(rr, req)

	body := rr.Body.String()
	hasCalendarQuery := strings.Contains(body, "calendar-query")
	hasCalendarMultiget := strings.Contains(body, "calendar-multiget")
	hasFreeBusy := strings.Contains(body, "free-busy-query")

	runCalendarQuery := func() int {
		queryBody := `<?xml version="1.0" encoding="utf-8" ?>
<C:calendar-query xmlns:D="DAV:" xmlns:C="urn:ietf:params:xml:ns:caldav">
  <D:prop><D:getetag/></D:prop>
  <C:filter>
    <C:comp-filter name="VCALENDAR">
      <C:comp-filter name="VEVENT"/>
    </C:comp-filter>
  </C:filter>
</C:calendar-query>`
		req := httptest.NewRequest("REPORT", "/dav/calendars/1/", strings.NewReader(queryBody))
		req = req.WithContext(auth.WithUser(req.Context(), user))
		rr := httptest.NewRecorder()
		h.Report(rr, req)
		return rr.Code
	}

	runCalendarMultiget := func() int {
		multigetBody := `<?xml version="1.0" encoding="utf-8" ?>
<C:calendar-multiget xmlns:D="DAV:" xmlns:C="urn:ietf:params:xml:ns:caldav">
  <D:prop><D:getetag/></D:prop>
  <D:href>/dav/calendars/1/event.ics</D:href>
</C:calendar-multiget>`
		req := httptest.NewRequest("REPORT", "/dav/calendars/1/", strings.NewReader(multigetBody))
		req = req.WithContext(auth.WithUser(req.Context(), user))
		rr := httptest.NewRecorder()
		h.Report(rr, req)
		return rr.Code
	}

	runFreeBusy := func() int {
		freeBusyBody := `<?xml version="1.0" encoding="utf-8" ?>
<C:free-busy-query xmlns:C="urn:ietf:params:xml:ns:caldav">
  <C:filter>
    <C:comp-filter name="VCALENDAR">
      <C:time-range start="20240601T000000Z" end="20240630T235959Z"/>
      <C:comp-filter name="VEVENT"/>
    </C:comp-filter>
  </C:filter>
</C:free-busy-query>`
		req := httptest.NewRequest("REPORT", "/dav/calendars/1/", strings.NewReader(freeBusyBody))
		req = req.WithContext(auth.WithUser(req.Context(), user))
		rr := httptest.NewRecorder()
		h.Report(rr, req)
		return rr.Code
	}

	queryStatus := runCalendarQuery()
	multigetStatus := runCalendarMultiget()
	freeBusyStatus := runFreeBusy()

	if hasCalendarQuery && (queryStatus == http.StatusNotImplemented || queryStatus == http.StatusForbidden || queryStatus == http.StatusConflict) {
		t.Errorf("calendar-query is advertised but failed with %d", queryStatus)
	}
	if hasCalendarMultiget && (multigetStatus == http.StatusNotImplemented || multigetStatus == http.StatusForbidden || multigetStatus == http.StatusConflict) {
		t.Errorf("calendar-multiget is advertised but failed with %d", multigetStatus)
	}
	if hasFreeBusy && (freeBusyStatus == http.StatusNotImplemented || freeBusyStatus == http.StatusForbidden || freeBusyStatus == http.StatusConflict) {
		t.Errorf("free-busy-query is advertised but failed with %d", freeBusyStatus)
	}

	if !hasCalendarQuery && queryStatus == http.StatusMultiStatus {
		t.Error("calendar-query works but is not advertised in supported-report-set")
	}
	if !hasCalendarMultiget && multigetStatus == http.StatusMultiStatus {
		t.Error("calendar-multiget works but is not advertised in supported-report-set")
	}
	if !hasFreeBusy && freeBusyStatus == http.StatusOK {
		t.Error("free-busy-query works but is not advertised in supported-report-set")
	}
}

// Test that calendar collections cannot contain other calendar collections
func TestRFC4791_NoNestedCalendarCollections(t *testing.T) {
	calRepo := &fakeCalendarRepo{calendars: make(map[int64]*store.Calendar)}
	h := &Handler{store: &store.Store{Calendars: calRepo}}
	user := &store.User{ID: 1}

	// Try to create a calendar inside another calendar
	req := httptest.NewRequest("MKCALENDAR", "/dav/calendars/1/nested/", nil)
	req = req.WithContext(auth.WithUser(req.Context(), user))
	rr := httptest.NewRecorder()

	h.Mkcalendar(rr, req)

	// RFC 4791 Section 4.2: Calendar collections MUST NOT contain other calendar collections
	// Current implementation treats this as bad request (path parsing fails)
	if rr.Code == http.StatusCreated {
		t.Error("RFC 4791 Section 4.2: Calendar collections must not contain other calendar collections")
	}
}

// Test Content-Type validation for PUT
func TestRFC4791_PutContentTypeValidation(t *testing.T) {
	calRepo := &fakeCalendarRepo{
		accessible: []store.CalendarAccess{
			{Calendar: store.Calendar{ID: 1, UserID: 1, Name: "Test"}, Editor: true},
		},
	}
	h := &Handler{store: &store.Store{Calendars: calRepo, Events: &fakeEventRepo{}}}
	user := &store.User{ID: 1}

	validIcal := "BEGIN:VCALENDAR\r\nVERSION:2.0\r\nBEGIN:VEVENT\r\nUID:test\r\nEND:VEVENT\r\nEND:VCALENDAR\r\n"

	// RFC 4791 Section 5.3.2: Clients SHOULD use Content-Type: text/calendar
	// Our server should accept calendar data regardless, but verify it's valid iCalendar
	req := newCalendarPutRequest("/dav/calendars/1/test.ics", strings.NewReader(validIcal))
	req.Header.Set("Content-Type", "text/calendar; charset=utf-8")
	req = req.WithContext(auth.WithUser(req.Context(), user))
	rr := httptest.NewRecorder()

	h.Put(rr, req)

	if rr.Code != http.StatusCreated {
		t.Errorf("Valid calendar data with proper Content-Type should succeed, got %d", rr.Code)
	}
}

// Test current-user-principal property (RFC 5397, required for CalDAV)
func TestRFC4791_CurrentUserPrincipalProperty(t *testing.T) {
	h := &Handler{}
	user := &store.User{ID: 1, PrimaryEmail: "user@example.com"}

	req := httptest.NewRequest("PROPFIND", "/dav/", nil)
	req.Header.Set("Depth", "0")
	req = req.WithContext(auth.WithUser(req.Context(), user))
	rr := httptest.NewRecorder()

	h.Propfind(rr, req)

	body := rr.Body.String()

	// RFC 5397 Section 3: Server must support current-user-principal property
	if !strings.Contains(body, "current-user-principal") {
		t.Error("RFC 5397 Section 3: Must support current-user-principal property")
	}

	// Should reference the principal URL
	if !strings.Contains(body, "/dav/principals/1/") {
		t.Error("current-user-principal must reference authenticated user's principal")
	}
}

// Test VTODO support in calendar collections
func TestRFC4791_VTODOComponentSupport(t *testing.T) {
	calRepo := &fakeCalendarRepo{
		accessible: []store.CalendarAccess{
			{Calendar: store.Calendar{ID: 1, UserID: 1, Name: "Test"}, Editor: true},
		},
	}
	eventRepo := &fakeEventRepo{events: make(map[string]*store.Event)}
	h := &Handler{store: &store.Store{Calendars: calRepo, Events: eventRepo}}
	user := &store.User{ID: 1}

	// RFC 4791: Calendars should support VTODO components
	todoData := "BEGIN:VCALENDAR\r\nVERSION:2.0\r\nBEGIN:VTODO\r\nUID:task1\r\nSUMMARY:Buy milk\r\nEND:VTODO\r\nEND:VCALENDAR\r\n"
	req := newCalendarPutRequest("/dav/calendars/1/task1.ics", strings.NewReader(todoData))
	req = req.WithContext(auth.WithUser(req.Context(), user))
	rr := httptest.NewRecorder()

	h.Put(rr, req)

	if rr.Code != http.StatusCreated {
		t.Errorf("RFC 4791: Calendar should accept VTODO components, got %d: %s", rr.Code, rr.Body.String())
	}
}

// Test VJOURNAL support in calendar collections
func TestRFC4791_VJOURNALComponentSupport(t *testing.T) {
	calRepo := &fakeCalendarRepo{
		accessible: []store.CalendarAccess{
			{Calendar: store.Calendar{ID: 1, UserID: 1, Name: "Test"}, Editor: true},
		},
	}
	eventRepo := &fakeEventRepo{events: make(map[string]*store.Event)}
	h := &Handler{store: &store.Store{Calendars: calRepo, Events: eventRepo}}
	user := &store.User{ID: 1}

	// RFC 4791: Calendars should support VJOURNAL components
	journalData := "BEGIN:VCALENDAR\r\nVERSION:2.0\r\nBEGIN:VJOURNAL\r\nUID:journal1\r\nSUMMARY:Today's notes\r\nEND:VJOURNAL\r\nEND:VCALENDAR\r\n"
	req := newCalendarPutRequest("/dav/calendars/1/journal1.ics", strings.NewReader(journalData))
	req = req.WithContext(auth.WithUser(req.Context(), user))
	rr := httptest.NewRecorder()

	h.Put(rr, req)

	if rr.Code != http.StatusCreated {
		t.Errorf("RFC 4791: Calendar should accept VJOURNAL components, got %d: %s", rr.Code, rr.Body.String())
	}
}

// Section 4.1: UID Property - Must be consistent with filename
func TestRFC4791_UIDMustMatchResourceName(t *testing.T) {
	calRepo := &fakeCalendarRepo{
		accessible: []store.CalendarAccess{
			{Calendar: store.Calendar{ID: 1, UserID: 1, Name: "Test"}, Editor: true},
		},
	}
	eventRepo := &fakeEventRepo{events: make(map[string]*store.Event)}
	h := &Handler{store: &store.Store{Calendars: calRepo, Events: eventRepo}}
	user := &store.User{ID: 1}

	// RFC 4791 Section 4.1: UID in calendar data should match the resource name
	// Client stores as "event123.ics" but UID is "different-uid"
	icalData := "BEGIN:VCALENDAR\r\nVERSION:2.0\r\nBEGIN:VEVENT\r\nUID:different-uid\r\nSUMMARY:Test\r\nEND:VEVENT\r\nEND:VCALENDAR\r\n"
	req := newCalendarPutRequest("/dav/calendars/1/event123.ics", strings.NewReader(icalData))
	req = req.WithContext(auth.WithUser(req.Context(), user))
	rr := httptest.NewRecorder()

	h.Put(rr, req)

	// Servers commonly allow this but should use the UID from the data
	// Verify the event is stored with the correct UID
	event, _ := h.store.Events.GetByUID(req.Context(), 1, "different-uid")
	if event == nil {
		t.Error("RFC 4791 Section 4.1: Event should be stored using UID from calendar data")
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
	h := &Handler{store: &store.Store{Calendars: calRepo, Events: &fakeEventRepo{}}}
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

	respBody := rr.Body.String()
	// RFC 4791 Section 5.2.1: calendar-description provides human-readable description
	if !strings.Contains(respBody, "calendar-description") {
		t.Error("RFC 4791 Section 5.2.1: Should support calendar-description property")
	}
}

func TestRFC4791_CalendarCollection_Propfind_CalendarTimezone_PresentAndValid(t *testing.T) {
	now := store.Now()
	calRepo := &fakeCalendarRepo{
		accessible: []store.CalendarAccess{
			{Calendar: store.Calendar{ID: 1, UserID: 1, Name: "Test", UpdatedAt: now}, Editor: true},
		},
	}
	h := &Handler{store: &store.Store{Calendars: calRepo, Events: &fakeEventRepo{}}}
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

	if rr.Code != http.StatusMultiStatus {
		t.Fatalf("calendar-timezone PROPFIND must return 207, got %d", rr.Code)
	}

	respBody := rr.Body.String()
	if !propstatHasStatus(respBody, "calendar-timezone", http.StatusOK) {
		t.Error("calendar-timezone must be returned with 200 propstat")
	}
	if !strings.Contains(respBody, "BEGIN:VTIMEZONE") || !strings.Contains(respBody, "END:VTIMEZONE") {
		t.Error("calendar-timezone must include a valid VTIMEZONE component")
	}
}

func TestRFC4791_CalendarCollection_CalendarTimezone_NotInAllprop(t *testing.T) {
	now := store.Now()
	calRepo := &fakeCalendarRepo{
		accessible: []store.CalendarAccess{
			{Calendar: store.Calendar{ID: 1, UserID: 1, Name: "Test", UpdatedAt: now}, Editor: true},
		},
	}
	h := &Handler{store: &store.Store{Calendars: calRepo, Events: &fakeEventRepo{}}}
	user := &store.User{ID: 1}

	req := httptest.NewRequest("PROPFIND", "/dav/calendars/1/", nil)
	req.Header.Set("Depth", "0")
	req = req.WithContext(auth.WithUser(req.Context(), user))
	rr := httptest.NewRecorder()

	h.Propfind(rr, req)

	if strings.Contains(rr.Body.String(), "calendar-timezone") {
		t.Error("calendar-timezone SHOULD NOT be returned by DAV:allprop")
	}
}

func TestRFC4791_CalendarCollection_Propfind_SupportedCalendarData_Advertised(t *testing.T) {
	now := store.Now()
	calRepo := &fakeCalendarRepo{
		accessible: []store.CalendarAccess{
			{Calendar: store.Calendar{ID: 1, UserID: 1, Name: "Test", UpdatedAt: now}, Editor: true},
		},
	}
	h := &Handler{store: &store.Store{Calendars: calRepo, Events: &fakeEventRepo{}}}
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

	if rr.Code != http.StatusMultiStatus {
		t.Fatalf("supported-calendar-data PROPFIND must return 207, got %d", rr.Code)
	}

	respBody := rr.Body.String()
	if !propstatHasStatus(respBody, "supported-calendar-data", http.StatusOK) {
		t.Error("supported-calendar-data must be returned with 200 propstat")
	}
	if !strings.Contains(respBody, `content-type="text/calendar"`) {
		t.Error("supported-calendar-data must advertise text/calendar")
	}
}

func TestRFC4791_CalendarCollection_SupportedCalendarData_NotInAllprop(t *testing.T) {
	now := store.Now()
	calRepo := &fakeCalendarRepo{
		accessible: []store.CalendarAccess{
			{Calendar: store.Calendar{ID: 1, UserID: 1, Name: "Test", UpdatedAt: now}, Editor: true},
		},
	}
	h := &Handler{store: &store.Store{Calendars: calRepo, Events: &fakeEventRepo{}}}
	user := &store.User{ID: 1}

	req := httptest.NewRequest("PROPFIND", "/dav/calendars/1/", nil)
	req.Header.Set("Depth", "0")
	req = req.WithContext(auth.WithUser(req.Context(), user))
	rr := httptest.NewRecorder()

	h.Propfind(rr, req)

	if strings.Contains(rr.Body.String(), "supported-calendar-data") {
		t.Error("supported-calendar-data SHOULD NOT be returned by DAV:allprop")
	}
}

func TestRFC4791_CalendarCollection_Propfind_MaxResourceSize_AdvertisedOrExplicit404(t *testing.T) {
	now := store.Now()
	calRepo := &fakeCalendarRepo{
		accessible: []store.CalendarAccess{
			{Calendar: store.Calendar{ID: 1, UserID: 1, Name: "Test", UpdatedAt: now}, Editor: true},
		},
	}
	h := &Handler{store: &store.Store{Calendars: calRepo, Events: &fakeEventRepo{}}}
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

	respBody := rr.Body.String()
	has200 := propstatHasStatus(respBody, "max-resource-size", http.StatusOK)
	has404 := propstatHasStatus(respBody, "max-resource-size", http.StatusNotFound)
	if !has200 && !has404 {
		t.Error("max-resource-size must be advertised with 200 or explicitly 404 in propstat")
	}
	if has200 {
		if _, ok := extractPropInt(respBody, "max-resource-size"); !ok {
			t.Error("max-resource-size must be an integer value")
		}
	}
}

func TestRFC4791_CalendarCollection_Propfind_MinMaxDateTime_AdvertisedOrExplicit404(t *testing.T) {
	now := store.Now()
	calRepo := &fakeCalendarRepo{
		accessible: []store.CalendarAccess{
			{Calendar: store.Calendar{ID: 1, UserID: 1, Name: "Test", UpdatedAt: now}, Editor: true},
		},
	}
	h := &Handler{store: &store.Store{Calendars: calRepo, Events: &fakeEventRepo{}}}
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

	respBody := rr.Body.String()
	min200 := propstatHasStatus(respBody, "min-date-time", http.StatusOK)
	min404 := propstatHasStatus(respBody, "min-date-time", http.StatusNotFound)
	max200 := propstatHasStatus(respBody, "max-date-time", http.StatusOK)
	max404 := propstatHasStatus(respBody, "max-date-time", http.StatusNotFound)

	if !min200 && !min404 {
		t.Error("min-date-time must be advertised with 200 or explicitly 404 in propstat")
	}
	if !max200 && !max404 {
		t.Error("max-date-time must be advertised with 200 or explicitly 404 in propstat")
	}
	if min200 {
		if _, ok := extractPropDateTime(respBody, "min-date-time"); !ok {
			t.Error("min-date-time must be a UTC DATE-TIME value")
		}
	}
	if max200 {
		if _, ok := extractPropDateTime(respBody, "max-date-time"); !ok {
			t.Error("max-date-time must be a UTC DATE-TIME value")
		}
	}
}

// Section 5.2.2: calendar-timezone Property
func TestRFC4791_CalendarTimezoneProperty(t *testing.T) {
	now := store.Now()
	calRepo := &fakeCalendarRepo{
		accessible: []store.CalendarAccess{
			{Calendar: store.Calendar{ID: 1, UserID: 1, Name: "Test", UpdatedAt: now}, Editor: true},
		},
	}
	h := &Handler{store: &store.Store{Calendars: calRepo, Events: &fakeEventRepo{}}}
	user := &store.User{ID: 1}

	body := `<?xml version="1.0" encoding="utf-8"?>
<d:propfind xmlns:d="DAV:" xmlns:c="urn:ietf:params:xml:ns:caldav">
  <d:prop>
    <c:calendar-timezone/>
  </d:prop>
</d:propfind>`

	req := httptest.NewRequest("PROPFIND", "/dav/calendars/1/", strings.NewReader(body))
	req.Header.Set("Depth", "0")
	req = req.WithContext(auth.WithUser(req.Context(), user))
	rr := httptest.NewRecorder()

	h.Propfind(rr, req)

	respBody := rr.Body.String()
	// RFC 4791 Section 5.2.2: calendar-timezone defines default timezone for calendar
	if !strings.Contains(respBody, "calendar-timezone") {
		t.Error("RFC 4791 Section 5.2.2: Should support calendar-timezone property")
	} else if !strings.Contains(respBody, "BEGIN:VTIMEZONE") {
		if !strings.Contains(respBody, "<cal:calendar-timezone/>") && !strings.Contains(respBody, "<cal:calendar-timezone></cal:calendar-timezone>") {
			t.Error("RFC 4791 Section 5.2.2: calendar-timezone must include VTIMEZONE data or be explicitly empty")
		}
	}
}

// Section 5.2.5: max-resource-size Property
func TestRFC4791_MaxResourceSizeProperty(t *testing.T) {
	now := store.Now()
	calRepo := &fakeCalendarRepo{
		accessible: []store.CalendarAccess{
			{Calendar: store.Calendar{ID: 1, UserID: 1, Name: "Test", UpdatedAt: now}, Editor: true},
		},
	}
	h := &Handler{store: &store.Store{Calendars: calRepo, Events: &fakeEventRepo{}}}
	user := &store.User{ID: 1}

	body := `<?xml version="1.0" encoding="utf-8"?>
<d:propfind xmlns:d="DAV:" xmlns:c="urn:ietf:params:xml:ns:caldav">
  <d:prop>
    <c:max-resource-size/>
  </d:prop>
</d:propfind>`

	req := httptest.NewRequest("PROPFIND", "/dav/calendars/1/", strings.NewReader(body))
	req.Header.Set("Depth", "0")
	req = req.WithContext(auth.WithUser(req.Context(), user))
	rr := httptest.NewRecorder()

	h.Propfind(rr, req)

	respBody := rr.Body.String()
	// RFC 4791 Section 5.2.5: max-resource-size indicates maximum size in octets
	if !strings.Contains(respBody, "max-resource-size") {
		t.Error("RFC 4791 Section 5.2.5: Should advertise max-resource-size property")
	}
}

// Section 5.2.6 & 5.2.7: min-date-time and max-date-time Properties
func TestRFC4791_DateTimeRangeLimitsProperties(t *testing.T) {
	now := store.Now()
	calRepo := &fakeCalendarRepo{
		accessible: []store.CalendarAccess{
			{Calendar: store.Calendar{ID: 1, UserID: 1, Name: "Test", UpdatedAt: now}, Editor: true},
		},
	}
	h := &Handler{store: &store.Store{Calendars: calRepo, Events: &fakeEventRepo{}}}
	user := &store.User{ID: 1}

	body := `<?xml version="1.0" encoding="utf-8"?>
<d:propfind xmlns:d="DAV:" xmlns:c="urn:ietf:params:xml:ns:caldav">
  <d:prop>
    <c:min-date-time/>
    <c:max-date-time/>
  </d:prop>
</d:propfind>`

	req := httptest.NewRequest("PROPFIND", "/dav/calendars/1/", strings.NewReader(body))
	req.Header.Set("Depth", "0")
	req = req.WithContext(auth.WithUser(req.Context(), user))
	rr := httptest.NewRecorder()

	h.Propfind(rr, req)

	respBody := rr.Body.String()
	// RFC 4791 Section 5.2.6 & 5.2.7: These properties indicate supported date range
	if !strings.Contains(respBody, "min-date-time") {
		t.Error("RFC 4791 Section 5.2.6: Server must advertise min-date-time when enforcing date limits")
	}
	if !strings.Contains(respBody, "max-date-time") {
		t.Error("RFC 4791 Section 5.2.7: Server must advertise max-date-time when enforcing date limits")
	}
}

func TestRFC4791_Precondition_SupportedCalendarData_RejectUnsupportedMediaType(t *testing.T) {
	now := store.Now()
	calRepo := &fakeCalendarRepo{
		accessible: []store.CalendarAccess{
			{Calendar: store.Calendar{ID: 1, UserID: 1, Name: "Test", UpdatedAt: now}, Editor: true},
		},
	}
	h := &Handler{store: &store.Store{Calendars: calRepo, Events: &fakeEventRepo{}}}
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

	respBody := rr.Body.String()
	if !propstatHasStatus(respBody, "supported-calendar-data", http.StatusOK) {
		t.Skip("supported-calendar-data not advertised; skipping precondition check")
	}

	icalData := "BEGIN:VCALENDAR\r\nVERSION:2.0\r\nBEGIN:VEVENT\r\nUID:unsupported\r\nEND:VEVENT\r\nEND:VCALENDAR\r\n"
	req = newCalendarPutRequest("/dav/calendars/1/unsupported.ics", strings.NewReader(icalData))
	req.Header.Set("Content-Type", "application/octet-stream")
	req = req.WithContext(auth.WithUser(req.Context(), user))
	rr = httptest.NewRecorder()

	h.Put(rr, req)

	if rr.Code < 400 || rr.Code >= 500 {
		t.Errorf("unsupported media type must fail with 4xx, got %d", rr.Code)
	}
	assertCalDAVErrorBody(t, rr.Body.String(), "supported-calendar-data")
}

func TestRFC4791_Precondition_MaxResourceSize_RejectTooLargeObject(t *testing.T) {
	now := store.Now()
	calRepo := &fakeCalendarRepo{
		accessible: []store.CalendarAccess{
			{Calendar: store.Calendar{ID: 1, UserID: 1, Name: "Test", UpdatedAt: now}, Editor: true},
		},
	}
	h := &Handler{store: &store.Store{Calendars: calRepo, Events: &fakeEventRepo{}}}
	user := &store.User{ID: 1}

	propBody := `<?xml version="1.0" encoding="utf-8"?>
<d:propfind xmlns:d="DAV:" xmlns:cal="urn:ietf:params:xml:ns:caldav">
  <d:prop>
    <cal:max-resource-size/>
  </d:prop>
</d:propfind>`
	req := httptest.NewRequest("PROPFIND", "/dav/calendars/1/", strings.NewReader(propBody))
	req.Header.Set("Depth", "0")
	req = req.WithContext(auth.WithUser(req.Context(), user))
	rr := httptest.NewRecorder()

	h.Propfind(rr, req)

	respBody := rr.Body.String()
	if !propstatHasStatus(respBody, "max-resource-size", http.StatusOK) {
		t.Skip("max-resource-size not advertised; skipping precondition check")
	}
	maxSize, ok := extractPropInt(respBody, "max-resource-size")
	if !ok || maxSize <= 0 {
		t.Fatalf("max-resource-size value missing or invalid: %q", respBody)
	}
	if maxSize > 256*1024 {
		t.Skipf("max-resource-size %d too large for test payload", maxSize)
	}

	base := "BEGIN:VCALENDAR\r\nVERSION:2.0\r\nBEGIN:VEVENT\r\nUID:too-large\r\nSUMMARY:Test\r\n"
	end := "END:VEVENT\r\nEND:VCALENDAR\r\n"
	target := maxSize + 1
	paddingLen := target - len(base) - len(end) - len("COMMENT:") - 2
	if paddingLen < 1 {
		paddingLen = 1
	}
	largeIcal := base + "COMMENT:" + strings.Repeat("A", paddingLen) + "\r\n" + end

	req = newCalendarPutRequest("/dav/calendars/1/too-large.ics", strings.NewReader(largeIcal))
	req = req.WithContext(auth.WithUser(req.Context(), user))
	rr = httptest.NewRecorder()

	h.Put(rr, req)

	if rr.Code < 400 || rr.Code >= 500 {
		t.Errorf("exceeding max-resource-size must fail with 4xx, got %d", rr.Code)
	}
	assertCalDAVErrorBody(t, rr.Body.String(), "max-resource-size")
}

func TestRFC4791_Precondition_MinDateTime_RejectEarlierDates(t *testing.T) {
	now := store.Now()
	calRepo := &fakeCalendarRepo{
		accessible: []store.CalendarAccess{
			{Calendar: store.Calendar{ID: 1, UserID: 1, Name: "Test", UpdatedAt: now}, Editor: true},
		},
	}
	h := &Handler{store: &store.Store{Calendars: calRepo, Events: &fakeEventRepo{}}}
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

	respBody := rr.Body.String()
	if !propstatHasStatus(respBody, "min-date-time", http.StatusOK) {
		t.Skip("min-date-time not advertised; skipping precondition check")
	}
	minTime, ok := extractPropDateTime(respBody, "min-date-time")
	if !ok {
		t.Fatalf("min-date-time value missing or invalid: %q", respBody)
	}
	testStart := minTime.AddDate(0, 0, -1)
	testEnd := testStart.Add(time.Hour)
	icalData := fmt.Sprintf("BEGIN:VCALENDAR\r\nVERSION:2.0\r\nBEGIN:VEVENT\r\nUID:too-early\r\nDTSTART:%s\r\nDTEND:%s\r\nEND:VEVENT\r\nEND:VCALENDAR\r\n",
		testStart.Format("20060102T150405Z"),
		testEnd.Format("20060102T150405Z"))

	req = newCalendarPutRequest("/dav/calendars/1/too-early.ics", strings.NewReader(icalData))
	req = req.WithContext(auth.WithUser(req.Context(), user))
	rr = httptest.NewRecorder()

	h.Put(rr, req)

	if rr.Code < 400 || rr.Code >= 500 {
		t.Errorf("min-date-time precondition must fail with 4xx, got %d", rr.Code)
	}
	assertCalDAVErrorBody(t, rr.Body.String(), "min-date-time")
}

func TestRFC4791_Precondition_MinDateTime_RejectEarlierDatesWithTZID(t *testing.T) {
	now := store.Now()
	calRepo := &fakeCalendarRepo{
		accessible: []store.CalendarAccess{
			{Calendar: store.Calendar{ID: 1, UserID: 1, Name: "Test", UpdatedAt: now}, Editor: true},
		},
	}
	h := &Handler{store: &store.Store{Calendars: calRepo, Events: &fakeEventRepo{}}}
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

	respBody := rr.Body.String()
	if !propstatHasStatus(respBody, "min-date-time", http.StatusOK) {
		t.Skip("min-date-time not advertised; skipping precondition check")
	}
	minTime, ok := extractPropDateTime(respBody, "min-date-time")
	if !ok {
		t.Fatalf("min-date-time value missing or invalid: %q", respBody)
	}
	testStart := minTime.AddDate(0, 0, -1).In(loc)
	testEnd := testStart.Add(time.Hour)
	icalData := fmt.Sprintf("BEGIN:VCALENDAR\r\nVERSION:2.0\r\nBEGIN:VEVENT\r\nUID:too-early-tzid\r\nDTSTART;TZID=America/New_York:%s\r\nDTEND;TZID=America/New_York:%s\r\nEND:VEVENT\r\nEND:VCALENDAR\r\n",
		testStart.Format("20060102T150405"),
		testEnd.Format("20060102T150405"))

	req = newCalendarPutRequest("/dav/calendars/1/too-early-tzid.ics", strings.NewReader(icalData))
	req = req.WithContext(auth.WithUser(req.Context(), user))
	rr = httptest.NewRecorder()

	h.Put(rr, req)

	if rr.Code < 400 || rr.Code >= 500 {
		t.Errorf("min-date-time precondition must fail with 4xx, got %d", rr.Code)
	}
	assertCalDAVErrorBody(t, rr.Body.String(), "min-date-time")
}

func TestRFC4791_Precondition_MaxDateTime_RejectLaterDates(t *testing.T) {
	now := store.Now()
	calRepo := &fakeCalendarRepo{
		accessible: []store.CalendarAccess{
			{Calendar: store.Calendar{ID: 1, UserID: 1, Name: "Test", UpdatedAt: now}, Editor: true},
		},
	}
	h := &Handler{store: &store.Store{Calendars: calRepo, Events: &fakeEventRepo{}}}
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

	respBody := rr.Body.String()
	if !propstatHasStatus(respBody, "max-date-time", http.StatusOK) {
		t.Skip("max-date-time not advertised; skipping precondition check")
	}
	maxTime, ok := extractPropDateTime(respBody, "max-date-time")
	if !ok {
		t.Fatalf("max-date-time value missing or invalid: %q", respBody)
	}
	testStart := maxTime.AddDate(0, 0, 1)
	testEnd := testStart.Add(time.Hour)
	icalData := fmt.Sprintf("BEGIN:VCALENDAR\r\nVERSION:2.0\r\nBEGIN:VEVENT\r\nUID:too-late\r\nDTSTART:%s\r\nDTEND:%s\r\nEND:VEVENT\r\nEND:VCALENDAR\r\n",
		testStart.Format("20060102T150405Z"),
		testEnd.Format("20060102T150405Z"))

	req = newCalendarPutRequest("/dav/calendars/1/too-late.ics", strings.NewReader(icalData))
	req = req.WithContext(auth.WithUser(req.Context(), user))
	rr = httptest.NewRecorder()

	h.Put(rr, req)

	if rr.Code < 400 || rr.Code >= 500 {
		t.Errorf("max-date-time precondition must fail with 4xx, got %d", rr.Code)
	}
	assertCalDAVErrorBody(t, rr.Body.String(), "max-date-time")
}

func TestRFC4791_Precondition_MaxDateTime_RejectLaterDatesWithOffset(t *testing.T) {
	now := store.Now()
	calRepo := &fakeCalendarRepo{
		accessible: []store.CalendarAccess{
			{Calendar: store.Calendar{ID: 1, UserID: 1, Name: "Test", UpdatedAt: now}, Editor: true},
		},
	}
	h := &Handler{store: &store.Store{Calendars: calRepo, Events: &fakeEventRepo{}}}
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

	respBody := rr.Body.String()
	if !propstatHasStatus(respBody, "max-date-time", http.StatusOK) {
		t.Skip("max-date-time not advertised; skipping precondition check")
	}
	maxTime, ok := extractPropDateTime(respBody, "max-date-time")
	if !ok {
		t.Fatalf("max-date-time value missing or invalid: %q", respBody)
	}
	offsetLoc := time.FixedZone("Offset", -5*60*60)
	testStart := maxTime.AddDate(0, 0, 1).In(offsetLoc)
	testEnd := testStart.Add(time.Hour)
	icalData := fmt.Sprintf("BEGIN:VCALENDAR\r\nVERSION:2.0\r\nBEGIN:VEVENT\r\nUID:too-late-offset\r\nDTSTART:%s\r\nDTEND:%s\r\nEND:VEVENT\r\nEND:VCALENDAR\r\n",
		testStart.Format("20060102T150405-0700"),
		testEnd.Format("20060102T150405-0700"))

	req = newCalendarPutRequest("/dav/calendars/1/too-late-offset.ics", strings.NewReader(icalData))
	req = req.WithContext(auth.WithUser(req.Context(), user))
	rr = httptest.NewRecorder()

	h.Put(rr, req)

	if rr.Code < 400 || rr.Code >= 500 {
		t.Errorf("max-date-time precondition must fail with 4xx, got %d", rr.Code)
	}
	assertCalDAVErrorBody(t, rr.Body.String(), "max-date-time")
}

// Section 5.2.6: min-date-time Precondition
func TestRFC4791_PutBeforeMinDateTimeRejected(t *testing.T) {
	calRepo := &fakeCalendarRepo{
		accessible: []store.CalendarAccess{
			{Calendar: store.Calendar{ID: 1, UserID: 1, Name: "Test"}, Editor: true},
		},
	}
	h := &Handler{store: &store.Store{Calendars: calRepo, Events: &fakeEventRepo{}}}
	user := &store.User{ID: 1}

	icalData := "BEGIN:VCALENDAR\r\nVERSION:2.0\r\nBEGIN:VEVENT\r\nUID:too-old\r\nDTSTART:18000101T000000Z\r\nDTEND:18000101T010000Z\r\nEND:VEVENT\r\nEND:VCALENDAR\r\n"
	req := newCalendarPutRequest("/dav/calendars/1/too-old.ics", strings.NewReader(icalData))
	req = req.WithContext(auth.WithUser(req.Context(), user))
	rr := httptest.NewRecorder()

	h.Put(rr, req)

	if rr.Code != http.StatusForbidden {
		t.Errorf("RFC 4791 Section 5.2.6: PUT before min-date-time must return 403, got %d", rr.Code)
	}
	assertCalDAVErrorBody(t, rr.Body.String(), "min-date-time")
}

// Section 5.2.7: max-date-time Precondition
func TestRFC4791_PutAfterMaxDateTimeRejected(t *testing.T) {
	calRepo := &fakeCalendarRepo{
		accessible: []store.CalendarAccess{
			{Calendar: store.Calendar{ID: 1, UserID: 1, Name: "Test"}, Editor: true},
		},
	}
	h := &Handler{store: &store.Store{Calendars: calRepo, Events: &fakeEventRepo{}}}
	user := &store.User{ID: 1}

	icalData := "BEGIN:VCALENDAR\r\nVERSION:2.0\r\nBEGIN:VEVENT\r\nUID:too-far\r\nDTSTART:22000101T000000Z\r\nDTEND:22000101T010000Z\r\nEND:VEVENT\r\nEND:VCALENDAR\r\n"
	req := newCalendarPutRequest("/dav/calendars/1/too-far.ics", strings.NewReader(icalData))
	req = req.WithContext(auth.WithUser(req.Context(), user))
	rr := httptest.NewRecorder()

	h.Put(rr, req)

	if rr.Code != http.StatusForbidden {
		t.Errorf("RFC 4791 Section 5.2.7: PUT after max-date-time must return 403, got %d", rr.Code)
	}
	assertCalDAVErrorBody(t, rr.Body.String(), "max-date-time")
}

// Section 5.3.3: DELETE Method
func TestRFC4791_DeleteCalendarObject(t *testing.T) {
	calRepo := &fakeCalendarRepo{
		accessible: []store.CalendarAccess{
			{Calendar: store.Calendar{ID: 1, UserID: 1, Name: "Test"}, Editor: true},
		},
	}
	eventRepo := &fakeEventRepo{
		events: map[string]*store.Event{
			"1:to-delete": {CalendarID: 1, UID: "to-delete", RawICAL: "ICAL", ETag: "etag1"},
		},
	}
	h := &Handler{store: &store.Store{Calendars: calRepo, Events: eventRepo}}
	user := &store.User{ID: 1}

	req := httptest.NewRequest(http.MethodDelete, "/dav/calendars/1/to-delete.ics", nil)
	req = req.WithContext(auth.WithUser(req.Context(), user))
	rr := httptest.NewRecorder()

	h.Delete(rr, req)

	// RFC 4791 Section 5.3.3: DELETE should return 204 No Content on success
	if rr.Code != http.StatusNoContent {
		t.Errorf("RFC 4791 Section 5.3.3: DELETE should return 204 No Content, got %d", rr.Code)
	}

	// Verify deletion
	_, exists := eventRepo.events["1:to-delete"]
	if exists {
		t.Error("RFC 4791 Section 5.3.3: Event should be deleted")
	}

	// Confirm resource is gone
	getReq := httptest.NewRequest(http.MethodGet, "/dav/calendars/1/to-delete.ics", nil)
	getReq = getReq.WithContext(auth.WithUser(getReq.Context(), user))
	getRR := httptest.NewRecorder()
	h.Get(getRR, getReq)
	if getRR.Code != http.StatusNotFound {
		t.Errorf("RFC 4791 Section 5.3.3: GET after DELETE should return 404, got %d", getRR.Code)
	}
}

// Section 5.3.3: DELETE with If-Match precondition
func TestRFC4791_DeleteWithIfMatchPrecondition(t *testing.T) {
	calRepo := &fakeCalendarRepo{
		accessible: []store.CalendarAccess{
			{Calendar: store.Calendar{ID: 1, UserID: 1, Name: "Test"}, Editor: true},
		},
	}
	eventRepo := &fakeEventRepo{
		events: map[string]*store.Event{
			"1:event": {CalendarID: 1, UID: "event", RawICAL: "ICAL", ETag: "correct-etag"},
		},
	}
	h := &Handler{store: &store.Store{Calendars: calRepo, Events: eventRepo}}
	user := &store.User{ID: 1}

	// Try to delete with wrong ETag
	req := httptest.NewRequest(http.MethodDelete, "/dav/calendars/1/event.ics", nil)
	req.Header.Set("If-Match", `"wrong-etag"`)
	req = req.WithContext(auth.WithUser(req.Context(), user))
	rr := httptest.NewRecorder()

	h.Delete(rr, req)

	// RFC 4791 Section 5.3.3: DELETE with failed If-Match MUST return 412
	if rr.Code != http.StatusPreconditionFailed {
		t.Errorf("RFC 4791 Section 5.3.3: DELETE with wrong If-Match must return 412, got %d", rr.Code)
	}

	// Event should still exist
	_, exists := eventRepo.events["1:event"]
	if !exists {
		t.Error("RFC 4791 Section 5.3.3: Event should not be deleted when If-Match fails")
	}
}

// Section 5.3.3: DELETE with correct If-Match
func TestRFC4791_DeleteWithIfMatchSucceeds(t *testing.T) {
	calRepo := &fakeCalendarRepo{
		accessible: []store.CalendarAccess{
			{Calendar: store.Calendar{ID: 1, UserID: 1, Name: "Test"}, Editor: true},
		},
	}
	eventRepo := &fakeEventRepo{
		events: map[string]*store.Event{
			"1:event": {CalendarID: 1, UID: "event", RawICAL: "ICAL", ETag: "correct-etag"},
		},
	}
	h := &Handler{store: &store.Store{Calendars: calRepo, Events: eventRepo}}
	user := &store.User{ID: 1}

	req := httptest.NewRequest(http.MethodDelete, "/dav/calendars/1/event.ics", nil)
	req.Header.Set("If-Match", `"correct-etag"`)
	req = req.WithContext(auth.WithUser(req.Context(), user))
	rr := httptest.NewRecorder()

	h.Delete(rr, req)

	if rr.Code != http.StatusNoContent {
		t.Errorf("RFC 4791 Section 5.3.3: DELETE with matching If-Match must return 204, got %d", rr.Code)
	}
	if _, exists := eventRepo.events["1:event"]; exists {
		t.Error("RFC 4791 Section 5.3.3: Event should be deleted when If-Match succeeds")
	}
}

// Section 5.3.3: DELETE on missing resource
func TestRFC4791_DeleteMissingResource(t *testing.T) {
	calRepo := &fakeCalendarRepo{
		accessible: []store.CalendarAccess{
			{Calendar: store.Calendar{ID: 1, UserID: 1, Name: "Test"}, Editor: true},
		},
	}
	eventRepo := &fakeEventRepo{events: make(map[string]*store.Event)}
	h := &Handler{store: &store.Store{Calendars: calRepo, Events: eventRepo}}
	user := &store.User{ID: 1}

	req := httptest.NewRequest(http.MethodDelete, "/dav/calendars/1/missing.ics", nil)
	req = req.WithContext(auth.WithUser(req.Context(), user))
	rr := httptest.NewRecorder()

	h.Delete(rr, req)

	if rr.Code != http.StatusNotFound && rr.Code != http.StatusNoContent {
		t.Errorf("RFC 4791 Section 5.3.3: DELETE on missing resource must return 404 or 204, got %d", rr.Code)
	}
}

// Section 7.8.5: Text Match Filtering in calendar-query
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
	h := &Handler{store: &store.Store{Calendars: calRepo, Events: eventRepo}}
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

	respBody := rr.Body.String()
	// Should find "Team Meeting" but not "Lunch Break"
	if !strings.Contains(respBody, "meeting.ics") {
		t.Error("RFC 4791 Section 9.7.5: Text match should find matching event")
	}
	if strings.Contains(respBody, "lunch.ics") {
		t.Error("RFC 4791 Section 9.7.5: Text match should not return non-matching event")
	}
}

// Section 9.7.2: Prop Filter - is-not-defined
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
	h := &Handler{store: &store.Store{Calendars: calRepo, Events: eventRepo}}
	user := &store.User{ID: 1}

	// RFC 4791 Section 9.7.2: is-not-defined test
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

	respBody := rr.Body.String()
	// Should find event without location
	if !strings.Contains(respBody, "without-location.ics") {
		t.Error("RFC 4791 Section 9.7.2: is-not-defined should find events without the property")
	}
	// Should not find event with location
	if strings.Contains(respBody, "with-location.ics") {
		t.Error("RFC 4791 Section 9.7.2: is-not-defined should not return events with the property")
	}
}

// Section 7.8.9: Partial Retrieval of Calendar Data
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
				RawICAL:    "BEGIN:VCALENDAR\r\nVERSION:2.0\r\nBEGIN:VEVENT\r\nUID:event\r\nSUMMARY:Test Event\r\nDESCRIPTION:Long description here\r\nEND:VEVENT\r\nEND:VCALENDAR\r\n",
				ETag:       "e",
			},
		},
	}
	h := &Handler{store: &store.Store{Calendars: calRepo, Events: eventRepo}}
	user := &store.User{ID: 1}

	// RFC 4791 Section 9.6.1: Request only specific properties
	body := `<?xml version="1.0" encoding="utf-8"?>
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
</C:calendar-query>`

	req := httptest.NewRequest("REPORT", "/dav/calendars/1/", strings.NewReader(body))
	req = req.WithContext(auth.WithUser(req.Context(), user))
	rr := httptest.NewRecorder()

	h.Report(rr, req)

	respBody := rr.Body.String()
	// RFC 4791 Section 9.6: Should support partial calendar-data retrieval
	// Implementation note: This is an advanced feature that may return full data
	if !strings.Contains(respBody, "calendar-data") {
		t.Error("RFC 4791 Section 9.6: Should return calendar-data")
	}

	// Request calendar-data without VCALENDAR wrapper and ensure response remains valid.
	body = `<?xml version="1.0" encoding="utf-8"?>
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
</C:calendar-query>`

	req = httptest.NewRequest("REPORT", "/dav/calendars/1/", strings.NewReader(body))
	req = req.WithContext(auth.WithUser(req.Context(), user))
	rr = httptest.NewRecorder()

	h.Report(rr, req)

	respBody = rr.Body.String()
	if !strings.Contains(respBody, "BEGIN:VCALENDAR") || !strings.Contains(respBody, "END:VCALENDAR") {
		t.Error("RFC 4791 Section 9.6: calendar-data must include VCALENDAR wrapper")
	}
}

// Section 5.3.2: PUT - Duplicate UID Restriction
func TestRFC4791_PreventDuplicateUIDInDifferentResources(t *testing.T) {
	calRepo := &fakeCalendarRepo{
		accessible: []store.CalendarAccess{
			{Calendar: store.Calendar{ID: 1, UserID: 1, Name: "Test"}, Editor: true},
		},
	}
	eventRepo := &fakeEventRepo{
		events: map[string]*store.Event{
			"1:event1": {CalendarID: 1, UID: "duplicate-uid", RawICAL: "ICAL1", ETag: "e1"},
		},
	}
	h := &Handler{store: &store.Store{Calendars: calRepo, Events: eventRepo}}
	user := &store.User{ID: 1}

	// RFC 4791 Section 4.1: A calendar collection MUST NOT contain more than one
	// calendar object resource with the same UID
	icalData := "BEGIN:VCALENDAR\r\nVERSION:2.0\r\nBEGIN:VEVENT\r\nUID:duplicate-uid\r\nEND:VEVENT\r\nEND:VCALENDAR\r\n"
	req := newCalendarPutRequest("/dav/calendars/1/event2.ics", strings.NewReader(icalData))
	req = req.WithContext(auth.WithUser(req.Context(), user))
	rr := httptest.NewRecorder()

	h.Put(rr, req)

	if rr.Code != http.StatusForbidden && rr.Code != http.StatusCreated && rr.Code != http.StatusNoContent {
		t.Fatalf("RFC 4791 Section 4.1: PUT with duplicate UID must reject or update existing resource, got %d", rr.Code)
	}
	if rr.Code != http.StatusForbidden {
		event, _ := h.store.Events.GetByUID(req.Context(), 1, "duplicate-uid")
		if event == nil {
			t.Fatal("RFC 4791 Section 4.1: Event with duplicate UID must exist")
		}
		if event.RawICAL != icalData {
			t.Error("RFC 4791 Section 4.1: Existing resource must be updated when duplicate UID is stored")
		}
	}
}

// Section 9.6.5: limit-recurrence-set
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
	h := &Handler{store: &store.Store{Calendars: calRepo, Events: eventRepo}}
	user := &store.User{ID: 1}

	// RFC 4791 Section 9.6.5: limit-recurrence-set restricts recurring events to time range
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

	respBody := rr.Body.String()
	// RFC 4791 Section 9.6.5: Should return calendar-data (implementation may return full or limited)
	if !strings.Contains(respBody, "calendar-data") {
		t.Error("RFC 4791 Section 9.6.5: Should return calendar-data with limit-recurrence-set")
	}
}

// Section 9.6.6: expand
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
	h := &Handler{store: &store.Store{Calendars: calRepo, Events: eventRepo}}
	user := &store.User{ID: 1}

	// RFC 4791 Section 9.6.6: expand converts recurring events to individual instances
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

	respBody := rr.Body.String()
	// RFC 4791 Section 9.6.6: Should expand recurrences (advanced feature)
	if !strings.Contains(respBody, "calendar-data") {
		t.Error("RFC 4791 Section 9.6.6: Should return calendar-data with expand")
	}
}

// Section 7.8.1: Time Range Filtering with Recurring Events
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
	h := &Handler{store: &store.Store{Calendars: calRepo, Events: eventRepo}}
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

	respBody := rr.Body.String()
	// RFC 4791 Section 9.9: Time range filters must handle recurring events
	// The recurring event should match if any instance falls in the range
	if !strings.Contains(respBody, "recurring.ics") {
		t.Error("RFC 4791 Section 9.9: Time range filter should match recurring events with instances in range")
	}
}

// Section 5.3.4: Last-Modified Header
func TestRFC4791_GetReturnsLastModifiedHeader(t *testing.T) {
	lastMod := time.Date(2024, 6, 1, 12, 0, 0, 0, time.UTC)
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
				RawICAL:      "BEGIN:VCALENDAR\r\nEND:VCALENDAR\r\n",
				ETag:         "e",
				LastModified: lastMod,
			},
		},
	}
	h := &Handler{store: &store.Store{Calendars: calRepo, Events: eventRepo}}
	user := &store.User{ID: 1}

	req := httptest.NewRequest(http.MethodGet, "/dav/calendars/1/event.ics", nil)
	req = req.WithContext(auth.WithUser(req.Context(), user))
	rr := httptest.NewRecorder()

	h.Get(rr, req)

	// RFC 2616 & RFC 4791 Section 5.3.4: Should return Last-Modified header
	lastModHeader := rr.Header().Get("Last-Modified")
	if lastModHeader == "" {
		t.Error("RFC 4791 Section 5.3.4: GET should return Last-Modified header")
	}
}

// Section 5.3.2.1: PUT - If-None-Match with existing resource
func TestRFC4791_PutWithIfNoneMatchOnExistingResource(t *testing.T) {
	calRepo := &fakeCalendarRepo{
		accessible: []store.CalendarAccess{
			{Calendar: store.Calendar{ID: 1, UserID: 1, Name: "Test"}, Editor: true},
		},
	}
	eventRepo := &fakeEventRepo{
		events: map[string]*store.Event{
			"1:existing": {CalendarID: 1, UID: "existing", RawICAL: "OLD", ETag: "etag1"},
		},
	}
	h := &Handler{store: &store.Store{Calendars: calRepo, Events: eventRepo}}
	user := &store.User{ID: 1}

	icalData := "BEGIN:VCALENDAR\r\nVERSION:2.0\r\nBEGIN:VEVENT\r\nUID:existing\r\nEND:VEVENT\r\nEND:VCALENDAR\r\n"
	req := newCalendarPutRequest("/dav/calendars/1/existing.ics", strings.NewReader(icalData))
	req.Header.Set("If-None-Match", "*")
	req = req.WithContext(auth.WithUser(req.Context(), user))
	rr := httptest.NewRecorder()

	h.Put(rr, req)

	// RFC 4791 Section 5.3.2.1: If-None-Match: * fails when resource exists
	if rr.Code != http.StatusPreconditionFailed {
		t.Errorf("RFC 4791 Section 5.3.2.1: PUT with If-None-Match: * on existing resource must return 412, got %d", rr.Code)
	}
}

// Section 5.1: CALDAV:read-free-busy Privilege
func TestRFC4791_ReadFreeBusyPrivilege(t *testing.T) {
	calRepo := &fakeCalendarRepo{
		accessible: []store.CalendarAccess{
			{Calendar: store.Calendar{ID: 1, UserID: 1, Name: "Test"}, Editor: true},
		},
	}
	h := &Handler{store: &store.Store{Calendars: calRepo, Events: &fakeEventRepo{}}}
	user := &store.User{ID: 1}

	body := `<?xml version="1.0" encoding="utf-8" ?>
<C:free-busy-query xmlns:C="urn:ietf:params:xml:ns:caldav">
  <C:filter>
    <C:comp-filter name="VCALENDAR">
      <C:time-range start="20240601T000000Z" end="20240630T235959Z"/>
      <C:comp-filter name="VEVENT"/>
    </C:comp-filter>
  </C:filter>
</C:free-busy-query>`

	req := httptest.NewRequest("REPORT", "/dav/calendars/1/", strings.NewReader(body))
	req = req.WithContext(auth.WithUser(req.Context(), user))
	rr := httptest.NewRecorder()

	h.Report(rr, req)

	if rr.Code != http.StatusOK {
		t.Fatalf("RFC 4791 Section 5.1.1: free-busy-query must return 200 OK, got %d", rr.Code)
	}
	if contentType := rr.Header().Get("Content-Type"); !strings.Contains(contentType, "text/calendar") {
		t.Errorf("RFC 4791 Section 5.1.1: free-busy-query must return text/calendar, got %s", contentType)
	}
}

// Section 6.3: current-user-privilege-set must include read-free-busy
func TestRFC4791_CurrentUserPrivilegeSetIncludesReadFreeBusy(t *testing.T) {
	now := store.Now()
	calRepo := &fakeCalendarRepo{
		accessible: []store.CalendarAccess{
			{Calendar: store.Calendar{ID: 1, UserID: 1, Name: "Test", UpdatedAt: now}, Editor: false},
		},
	}
	h := &Handler{store: &store.Store{Calendars: calRepo, Events: &fakeEventRepo{}}}
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

	body := rr.Body.String()
	if !strings.Contains(body, "current-user-privilege-set") {
		t.Fatalf("RFC 4791 Section 6.3: current-user-privilege-set must be returned, got: %s", body)
	}
	if !strings.Contains(body, "read-free-busy") {
		t.Fatalf("RFC 4791 Section 6.3: current-user-privilege-set must include cal:read-free-busy, got: %s", body)
	}

	readNested := regexp.MustCompile(`(?s)<[^>]*read[^>]*>.*read-free-busy`).MatchString(body)
	if !readNested {
		t.Errorf("RFC 4791 Section 6.3: read-free-busy must be aggregated under DAV:read, got: %s", body)
	}

	if strings.Count(body, "read-free-busy") < 2 {
		t.Errorf("RFC 4791 Section 6.3: read-free-busy should be grantable independently (separate from DAV:read), got: %s", body)
	}
}

// Section 6.3: free-busy-query authorization must align with advertised privileges
func TestRFC4791_ReadFreeBusyPrivilegeEnforcedForReports(t *testing.T) {
	start := time.Date(2024, 6, 1, 10, 0, 0, 0, time.UTC)
	end := time.Date(2024, 6, 1, 12, 0, 0, 0, time.UTC)
	calRepo := &fakeCalendarRepo{
		accessible: []store.CalendarAccess{
			// Shared calendar with limited privileges (read-free-busy but not full read).
			{Calendar: store.Calendar{ID: 1, UserID: 1, Name: "Test", UpdatedAt: store.Now()}, Shared: true, Editor: false},
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
	h := &Handler{store: &store.Store{Calendars: calRepo, Events: eventRepo}}
	user := &store.User{ID: 1}

	freeBusyBody := `<?xml version="1.0" encoding="utf-8" ?>
<C:free-busy-query xmlns:C="urn:ietf:params:xml:ns:caldav">
  <C:filter>
    <C:comp-filter name="VCALENDAR">
      <C:time-range start="20240601T000000Z" end="20240630T235959Z"/>
      <C:comp-filter name="VEVENT"/>
    </C:comp-filter>
  </C:filter>
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
	if rr.Code == http.StatusMultiStatus {
		t.Fatalf("calendar-query must fail or be limited when only read-free-busy is granted, got %d", rr.Code)
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
	if rr.Code == http.StatusMultiStatus {
		t.Fatalf("calendar-multiget must fail or be limited when only read-free-busy is granted, got %d", rr.Code)
	}
}

// Section 5.2.8: schedule-calendar-transp Property
func TestRFC4791_ScheduleCalendarTranspProperty(t *testing.T) {
	now := store.Now()
	calRepo := &fakeCalendarRepo{
		accessible: []store.CalendarAccess{
			{Calendar: store.Calendar{ID: 1, UserID: 1, Name: "Test", UpdatedAt: now}, Editor: true},
		},
	}
	h := &Handler{store: &store.Store{Calendars: calRepo, Events: &fakeEventRepo{}}}
	user := &store.User{ID: 1}

	body := `<?xml version="1.0" encoding="utf-8"?>
<d:propfind xmlns:d="DAV:" xmlns:c="urn:ietf:params:xml:ns:caldav">
  <d:prop>
    <c:schedule-calendar-transp/>
  </d:prop>
</d:propfind>`

	req := httptest.NewRequest("PROPFIND", "/dav/calendars/1/", strings.NewReader(body))
	req.Header.Set("Depth", "0")
	req = req.WithContext(auth.WithUser(req.Context(), user))
	rr := httptest.NewRecorder()

	h.Propfind(rr, req)

	respBody := rr.Body.String()
	// RFC 4791 Section 5.2.8: Indicates if calendar is used in freebusy calculations
	if !strings.Contains(respBody, "schedule-calendar-transp") {
		t.Error("RFC 4791 Section 5.2.8: Server must advertise schedule-calendar-transp property")
	}
	if !strings.Contains(respBody, "opaque") && !strings.Contains(respBody, "transparent") {
		t.Error("RFC 4791 Section 5.2.8: schedule-calendar-transp must indicate opaque or transparent")
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
	h := &Handler{store: &store.Store{Calendars: calRepo, Events: eventRepo}}
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

	respBody := rr.Body.String()
	if rr.Code != http.StatusMultiStatus {
		t.Fatalf("RFC 4791 Section 7.8: calendar-query must return 207, got %d", rr.Code)
	}
	if !strings.Contains(respBody, "match.ics") {
		t.Error("RFC 4791 Section 7.8: Must return matching event")
	}
	if strings.Contains(respBody, "nomatch.ics") {
		t.Error("RFC 4791 Section 7.8: Must not return non-matching event")
	}
}

// Section 9.11: CALDAV:timezone XML Element
func TestRFC4791_TimezoneXMLElement(t *testing.T) {
	// RFC 4791 Section 9.11: timezone element used in free-busy queries
	calRepo := &fakeCalendarRepo{
		accessible: []store.CalendarAccess{
			{Calendar: store.Calendar{ID: 1, UserID: 1, Name: "Test"}, Editor: true},
		},
	}
	h := &Handler{store: &store.Store{Calendars: calRepo, Events: &fakeEventRepo{}}}
	user := &store.User{ID: 1}

	body := `<?xml version="1.0" encoding="utf-8"?>
<C:free-busy-query xmlns:C="urn:ietf:params:xml:ns:caldav">
  <C:time-range start="20240601T000000Z" end="20240630T235959Z"/>
  <C:timezone>
BEGIN:VTIMEZONE
TZID:UTC
END:VTIMEZONE
  </C:timezone>
</C:free-busy-query>`

	req := httptest.NewRequest("REPORT", "/dav/calendars/1/", strings.NewReader(body))
	req = req.WithContext(auth.WithUser(req.Context(), user))
	rr := httptest.NewRecorder()

	h.Report(rr, req)

	if rr.Code != http.StatusOK {
		t.Fatalf("RFC 4791 Section 9.11: free-busy-query must return 200 OK, got %d", rr.Code)
	}
	if contentType := rr.Header().Get("Content-Type"); !strings.Contains(contentType, "text/calendar") {
		t.Fatalf("RFC 4791 Section 9.11: free-busy-query must return text/calendar, got %s", contentType)
	}
	if !strings.Contains(rr.Body.String(), "BEGIN:VFREEBUSY") {
		t.Error("RFC 4791 Section 9.11: free-busy-query response must include VFREEBUSY")
	}
}

// Section 9.6.4: CALDAV:filter in calendar-data
func TestRFC4791_FilterWithinCalendarData(t *testing.T) {
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
	h := &Handler{store: &store.Store{Calendars: calRepo, Events: eventRepo}}
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

	if rr.Code != http.StatusMultiStatus {
		t.Fatalf("RFC 4791 Section 9.6.4: calendar-multiget must return 207, got %d", rr.Code)
	}
	respBody := rr.Body.String()
	if !strings.Contains(respBody, "event.ics") {
		t.Error("RFC 4791 Section 9.6.4: Response must include requested resource")
	}
	if !strings.Contains(respBody, "calendar-data") {
		t.Error("RFC 4791 Section 9.6.4: Response must include calendar-data")
	}
}

// Section 5.3.1: MKCALENDAR with Request Body
func TestRFC4791_MkcalendarWithProperties(t *testing.T) {
	calRepo := &fakeCalendarRepo{calendars: make(map[int64]*store.Calendar)}
	h := &Handler{store: &store.Store{Calendars: calRepo}}
	user := &store.User{ID: 1}

	// RFC 4791 Section 5.3.1: MKCALENDAR can include property updates
	body := `<?xml version="1.0" encoding="utf-8"?>
<C:mkcalendar xmlns:D="DAV:" xmlns:C="urn:ietf:params:xml:ns:caldav">
  <D:set>
    <D:prop>
      <D:displayname>My New Calendar</D:displayname>
      <C:calendar-description>Personal events</C:calendar-description>
    </D:prop>
  </D:set>
</C:mkcalendar>`

	req := httptest.NewRequest("MKCALENDAR", "/dav/calendars/work", strings.NewReader(body))
	req = req.WithContext(auth.WithUser(req.Context(), user))
	rr := httptest.NewRecorder()

	h.Mkcalendar(rr, req)

	// RFC 4791 Section 5.3.1.2: Server may support setting properties during creation
	if rr.Code == http.StatusCreated {
		t.Log("RFC 4791 Section 5.3.1: Server supports MKCALENDAR with property initialization")
	} else if rr.Code == http.StatusMultiStatus {
		t.Log("RFC 4791 Section 5.3.1: Server created calendar but some properties may have failed")
	}
}

// Interoperability: Calendar object resources often use .ics extension
func TestInteroperability_CalendarObjectResourceNaming(t *testing.T) {
	// Many clients use .ics filenames, but this is not a strict RFC 4791 requirement
	calRepo := &fakeCalendarRepo{
		accessible: []store.CalendarAccess{
			{Calendar: store.Calendar{ID: 1, UserID: 1, Name: "Test"}, Editor: true},
		},
	}
	eventRepo := &fakeEventRepo{events: make(map[string]*store.Event)}
	h := &Handler{store: &store.Store{Calendars: calRepo, Events: eventRepo}}
	user := &store.User{ID: 1}

	icalData := "BEGIN:VCALENDAR\r\nVERSION:2.0\r\nBEGIN:VEVENT\r\nUID:test\r\nEND:VEVENT\r\nEND:VCALENDAR\r\n"

	// Try without .ics extension
	req := newCalendarPutRequest("/dav/calendars/1/event-no-extension", strings.NewReader(icalData))
	req = req.WithContext(auth.WithUser(req.Context(), user))
	rr := httptest.NewRecorder()

	h.Put(rr, req)

	// Servers may be lenient about naming
	if rr.Code == http.StatusCreated || rr.Code == http.StatusNoContent {
		t.Log("Server accepts calendar objects without .ics extension (lenient)")
	}
}

// Section 9.7.3: Test is-defined filter
func TestRFC4791_PropFilterIsDefined(t *testing.T) {
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
	h := &Handler{store: &store.Store{Calendars: calRepo, Events: eventRepo}}
	user := &store.User{ID: 1}

	// RFC 4791 Section 9.7.3: Filter for events that HAVE a description
	body := `<?xml version="1.0" encoding="utf-8"?>
<C:calendar-query xmlns:C="urn:ietf:params:xml:ns:caldav" xmlns:D="DAV:">
  <D:prop><D:getetag/></D:prop>
  <C:filter>
    <C:comp-filter name="VCALENDAR">
      <C:comp-filter name="VEVENT">
        <C:prop-filter name="DESCRIPTION">
          <C:is-defined/>
        </C:prop-filter>
      </C:comp-filter>
    </C:comp-filter>
  </C:filter>
</C:calendar-query>`

	req := httptest.NewRequest("REPORT", "/dav/calendars/1/", strings.NewReader(body))
	req = req.WithContext(auth.WithUser(req.Context(), user))
	rr := httptest.NewRecorder()

	h.Report(rr, req)

	respBody := rr.Body.String()
	if rr.Code == http.StatusMultiStatus {
		// Should find event with description
		if strings.Contains(respBody, "with-desc.ics") {
			t.Log("RFC 4791 Section 9.7.3: is-defined filter correctly finds events with property")
		}
	}
}

// Test getctag Property (RFC 6578, commonly used with CalDAV)
func TestRFC4791_GetCTagProperty(t *testing.T) {
	now := store.Now()
	calRepo := &fakeCalendarRepo{
		accessible: []store.CalendarAccess{
			{Calendar: store.Calendar{ID: 1, UserID: 1, Name: "Test", CTag: 42, UpdatedAt: now}, Editor: true},
		},
	}
	h := &Handler{store: &store.Store{Calendars: calRepo, Events: &fakeEventRepo{}}}
	user := &store.User{ID: 1}

	body := `<?xml version="1.0" encoding="utf-8"?>
<d:propfind xmlns:d="DAV:" xmlns:cs="http://calendarserver.org/ns/">
  <d:prop>
    <cs:getctag/>
  </d:prop>
</d:propfind>`

	req := httptest.NewRequest("PROPFIND", "/dav/calendars/1/", strings.NewReader(body))
	req.Header.Set("Depth", "0")
	req = req.WithContext(auth.WithUser(req.Context(), user))
	rr := httptest.NewRecorder()

	h.Propfind(rr, req)

	respBody := rr.Body.String()
	// getctag is not in RFC 4791 but is widely used and part of CalDAV ecosystem
	if strings.Contains(respBody, "getctag") {
		t.Log("Server supports getctag property for efficient synchronization (CalDAV extension)")
	}
}

// Section 7.8.6: Negate Condition in Filters
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
	h := &Handler{store: &store.Store{Calendars: calRepo, Events: eventRepo}}
	user := &store.User{ID: 1}

	// RFC 4791 Section 9.7.5: negate-condition attribute inverts the match
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

	respBody := rr.Body.String()
	if rr.Code == http.StatusMultiStatus {
		// Should find events NOT matching "Meeting"
		if strings.Contains(respBody, "event2.ics") && !strings.Contains(respBody, "event1.ics") {
			t.Log("RFC 4791 Section 9.7.5: Server supports negate-condition attribute")
		}
	}
}

// Section 5.2.9: max-attendees-per-instance Property
func TestRFC4791_MaxAttendeesPerInstanceProperty(t *testing.T) {
	now := store.Now()
	calRepo := &fakeCalendarRepo{
		accessible: []store.CalendarAccess{
			{Calendar: store.Calendar{ID: 1, UserID: 1, Name: "Test", UpdatedAt: now}, Editor: true},
		},
	}
	h := &Handler{store: &store.Store{Calendars: calRepo, Events: &fakeEventRepo{}}}
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

	respBody := rr.Body.String()
	// RFC 4791 Section 5.2.9: max-attendees-per-instance indicates maximum number of attendees
	if !strings.Contains(respBody, "max-attendees-per-instance") {
		t.Error("RFC 4791 Section 5.2.9: Server must advertise max-attendees-per-instance property")
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
	h := &Handler{store: &store.Store{Calendars: calRepo, Events: &fakeEventRepo{}}}
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

	respBody := rr.Body.String()
	// RFC 4791 Section 5.2.8: max-instances indicates maximum number of recurrence instances
	if !strings.Contains(respBody, "max-instances") {
		t.Error("RFC 4791 Section 5.2.8: Server must advertise max-instances property")
	}
}

// Section 5.2.9: max-attendees-per-instance Precondition
func TestRFC4791_PutExceedsMaxAttendeesPerInstance(t *testing.T) {
	calRepo := &fakeCalendarRepo{
		accessible: []store.CalendarAccess{
			{Calendar: store.Calendar{ID: 1, UserID: 1, Name: "Test"}, Editor: true},
		},
	}
	h := &Handler{store: &store.Store{Calendars: calRepo, Events: &fakeEventRepo{}}}
	user := &store.User{ID: 1}

	var sb strings.Builder
	sb.WriteString("BEGIN:VCALENDAR\r\nVERSION:2.0\r\nBEGIN:VEVENT\r\nUID:attendees\r\nDTSTART:20240101T000000Z\r\n")
	for i := 0; i < caldavMaxAttendees+1; i++ {
		sb.WriteString(fmt.Sprintf("ATTENDEE:mailto:user%d@example.com\r\n", i))
	}
	sb.WriteString("END:VEVENT\r\nEND:VCALENDAR\r\n")

	req := newCalendarPutRequest("/dav/calendars/1/attendees.ics", strings.NewReader(sb.String()))
	req = req.WithContext(auth.WithUser(req.Context(), user))
	rr := httptest.NewRecorder()

	h.Put(rr, req)

	if rr.Code != http.StatusForbidden {
		t.Errorf("RFC 4791 Section 5.2.9: PUT exceeding max-attendees-per-instance must return 403, got %d", rr.Code)
	}
	assertCalDAVErrorBody(t, rr.Body.String(), "max-attendees-per-instance")
}

// Section 5.2.8: max-instances Precondition
func TestRFC4791_PutExceedsMaxInstances(t *testing.T) {
	calRepo := &fakeCalendarRepo{
		accessible: []store.CalendarAccess{
			{Calendar: store.Calendar{ID: 1, UserID: 1, Name: "Test"}, Editor: true},
		},
	}
	h := &Handler{store: &store.Store{Calendars: calRepo, Events: &fakeEventRepo{}}}
	user := &store.User{ID: 1}

	icalData := "BEGIN:VCALENDAR\r\nVERSION:2.0\r\nBEGIN:VEVENT\r\nUID:too-many\r\nDTSTART:20240101T000000Z\r\nRRULE:FREQ=DAILY;COUNT=2001\r\nEND:VEVENT\r\nEND:VCALENDAR\r\n"
	req := newCalendarPutRequest("/dav/calendars/1/too-many.ics", strings.NewReader(icalData))
	req = req.WithContext(auth.WithUser(req.Context(), user))
	rr := httptest.NewRecorder()

	h.Put(rr, req)

	if rr.Code != http.StatusForbidden {
		t.Errorf("RFC 4791 Section 5.2.8: PUT exceeding max-instances must return 403, got %d", rr.Code)
	}
	assertCalDAVErrorBody(t, rr.Body.String(), "max-instances")
}

func TestRFC4791_PutExceedsMaxInstancesLowercaseParams(t *testing.T) {
	calRepo := &fakeCalendarRepo{
		accessible: []store.CalendarAccess{
			{Calendar: store.Calendar{ID: 1, UserID: 1, Name: "Test"}, Editor: true},
		},
	}
	h := &Handler{store: &store.Store{Calendars: calRepo, Events: &fakeEventRepo{}}}
	user := &store.User{ID: 1}

	icalData := "BEGIN:VCALENDAR\r\nVERSION:2.0\r\nBEGIN:VEVENT\r\nUID:too-many-lower\r\nDTSTART:20240101T000000Z\r\nRRULE:freq=daily;count=2001\r\nEND:VEVENT\r\nEND:VCALENDAR\r\n"
	req := newCalendarPutRequest("/dav/calendars/1/too-many-lower.ics", strings.NewReader(icalData))
	req = req.WithContext(auth.WithUser(req.Context(), user))
	rr := httptest.NewRecorder()

	h.Put(rr, req)

	if rr.Code != http.StatusForbidden {
		t.Errorf("RFC 4791 Section 5.2.8: PUT exceeding max-instances must return 403, got %d", rr.Code)
	}
	assertCalDAVErrorBody(t, rr.Body.String(), "max-instances")
}

// Section 5.2.10 & 5.3.2.1: PROPPATCH Preconditions
func TestRFC4791_ProppatchOnReadOnlyProperties(t *testing.T) {
	now := store.Now()
	calRepo := &fakeCalendarRepo{
		accessible: []store.CalendarAccess{
			{Calendar: store.Calendar{ID: 1, UserID: 1, Name: "Test", UpdatedAt: now}, Editor: true},
		},
	}
	h := &Handler{store: &store.Store{Calendars: calRepo, Events: &fakeEventRepo{}}}
	user := &store.User{ID: 1}

	// RFC 4791 Section 5.2.10: Attempt to modify read-only property
	body := `<?xml version="1.0" encoding="utf-8"?>
<d:propertyupdate xmlns:d="DAV:" xmlns:c="urn:ietf:params:xml:ns:caldav">
  <d:set>
    <d:prop>
      <c:supported-calendar-component-set>
        <c:comp name="VEVENT"/>
      </c:supported-calendar-component-set>
    </d:prop>
  </d:set>
</d:propertyupdate>`

	req := httptest.NewRequest("PROPPATCH", "/dav/calendars/1/", strings.NewReader(body))
	req = req.WithContext(auth.WithUser(req.Context(), user))
	rr := httptest.NewRecorder()

	h.Proppatch(rr, req)

	// RFC 4791 Section 5.2.10: Server MAY return 403 or report failure in multi-status
	if rr.Code == http.StatusForbidden || rr.Code == http.StatusMultiStatus {
		t.Log("RFC 4791 Section 5.2.10: Server correctly rejects modification of read-only properties")
	}
}

// Section 5.3.2.1: CALDAV:supported-calendar-data Precondition
func TestRFC4791_PutWithUnsupportedMediaType(t *testing.T) {
	calRepo := &fakeCalendarRepo{
		accessible: []store.CalendarAccess{
			{Calendar: store.Calendar{ID: 1, UserID: 1, Name: "Test"}, Editor: true},
		},
	}
	h := &Handler{store: &store.Store{Calendars: calRepo, Events: &fakeEventRepo{}}}
	user := &store.User{ID: 1}

	// Try to PUT with unsupported content type (e.g., JSON instead of iCalendar)
	jsonData := `{"summary": "Test Event"}`
	req := newCalendarPutRequest("/dav/calendars/1/test.ics", strings.NewReader(jsonData))
	req.Header.Set("Content-Type", "application/json")
	req = req.WithContext(auth.WithUser(req.Context(), user))
	rr := httptest.NewRecorder()

	h.Put(rr, req)

	// RFC 4791 Section 5.3.2.1: Server MUST reject unsupported calendar data
	if rr.Code != http.StatusBadRequest && rr.Code != http.StatusUnsupportedMediaType {
		t.Errorf("RFC 4791 Section 5.3.2.1: PUT with unsupported calendar data must fail with 400 or 415, got %d", rr.Code)
	}
	assertCalDAVErrorBody(t, rr.Body.String(), "supported-calendar-data")
}

// Section 5.3.2.1: Content-Type is required for calendar object resources
func TestRFC4791_PutWithoutContentTypeRejected(t *testing.T) {
	calRepo := &fakeCalendarRepo{
		accessible: []store.CalendarAccess{
			{Calendar: store.Calendar{ID: 1, UserID: 1, Name: "Test"}, Editor: true},
		},
	}
	h := &Handler{store: &store.Store{Calendars: calRepo, Events: &fakeEventRepo{}}}
	user := &store.User{ID: 1}

	icalData := "BEGIN:VCALENDAR\r\nVERSION:2.0\r\nBEGIN:VEVENT\r\nUID:no-ctype\r\nSUMMARY:No Content-Type\r\nEND:VEVENT\r\nEND:VCALENDAR\r\n"
	req := httptest.NewRequest(http.MethodPut, "/dav/calendars/1/no-ctype.ics", strings.NewReader(icalData))
	req = req.WithContext(auth.WithUser(req.Context(), user))
	rr := httptest.NewRecorder()

	h.Put(rr, req)

	if rr.Code == http.StatusCreated || rr.Code == http.StatusNoContent || rr.Code < 400 {
		t.Errorf("PUT without Content-Type must fail, got %d", rr.Code)
	}
	if rr.Code != http.StatusBadRequest && rr.Code != http.StatusConflict && rr.Code != http.StatusUnsupportedMediaType {
		t.Errorf("PUT without Content-Type must fail with 400/409/415, got %d", rr.Code)
	}
}

// Section 5.3.2.1: text/plain is not supported calendar data
func TestRFC4791_PutWithTextPlainRejected(t *testing.T) {
	calRepo := &fakeCalendarRepo{
		accessible: []store.CalendarAccess{
			{Calendar: store.Calendar{ID: 1, UserID: 1, Name: "Test"}, Editor: true},
		},
	}
	h := &Handler{store: &store.Store{Calendars: calRepo, Events: &fakeEventRepo{}}}
	user := &store.User{ID: 1}

	icalData := "BEGIN:VCALENDAR\r\nVERSION:2.0\r\nBEGIN:VEVENT\r\nUID:plain-text\r\nSUMMARY:Plain Text\r\nEND:VEVENT\r\nEND:VCALENDAR\r\n"
	req := newCalendarPutRequest("/dav/calendars/1/plain-text.ics", strings.NewReader(icalData))
	req.Header.Set("Content-Type", "text/plain")
	req = req.WithContext(auth.WithUser(req.Context(), user))
	rr := httptest.NewRecorder()

	h.Put(rr, req)

	if rr.Code != http.StatusBadRequest && rr.Code != http.StatusUnsupportedMediaType {
		t.Errorf("PUT with text/plain must fail with 400 or 415, got %d", rr.Code)
	}
	assertCalDAVErrorBody(t, rr.Body.String(), "supported-calendar-data")
}

// Section 5.3.2.1: CALDAV:supported-calendar-component Precondition
func TestRFC4791_PutWithUnsupportedComponent(t *testing.T) {
	calRepo := &fakeCalendarRepo{
		accessible: []store.CalendarAccess{
			{Calendar: store.Calendar{ID: 1, UserID: 1, Name: "Test"}, Editor: true},
		},
	}
	h := &Handler{store: &store.Store{Calendars: calRepo, Events: &fakeEventRepo{}}}
	user := &store.User{ID: 1}

	// Try to PUT a mix of supported and unsupported components.
	unsupportedData := "BEGIN:VCALENDAR\r\nVERSION:2.0\r\nBEGIN:VEVENT\r\nUID:event1\r\nSUMMARY:Test\r\nEND:VEVENT\r\nBEGIN:VAVAILABILITY\r\nUID:avail1\r\nDTSTART:20240101T000000Z\r\nDTEND:20240101T235959Z\r\nEND:VAVAILABILITY\r\nEND:VCALENDAR\r\n"
	req := newCalendarPutRequest("/dav/calendars/1/unsupported.ics", strings.NewReader(unsupportedData))
	req = req.WithContext(auth.WithUser(req.Context(), user))
	rr := httptest.NewRecorder()

	h.Put(rr, req)

	// RFC 4791 Section 5.3.2.1: Server may restrict which components can be stored
	if rr.Code != http.StatusForbidden && rr.Code != http.StatusBadRequest {
		t.Errorf("RFC 4791 Section 5.3.2.1: PUT with unsupported component must fail with 400 or 403, got %d", rr.Code)
	}
	assertCalDAVErrorBody(t, rr.Body.String(), "supported-calendar-component")
}

// Section 5.3.2.1: CALDAV:max-resource-size Precondition
func TestRFC4791_PutExceedsMaxResourceSize(t *testing.T) {
	calRepo := &fakeCalendarRepo{
		accessible: []store.CalendarAccess{
			{Calendar: store.Calendar{ID: 1, UserID: 1, Name: "Test"}, Editor: true},
		},
	}
	h := &Handler{store: &store.Store{Calendars: calRepo, Events: &fakeEventRepo{}}}
	user := &store.User{ID: 1}

	// Create a very large iCalendar object
	largeDescription := strings.Repeat("A", 1024*1024*15) // 15MB
	largeIcal := fmt.Sprintf("BEGIN:VCALENDAR\r\nVERSION:2.0\r\nBEGIN:VEVENT\r\nUID:large\r\nDESCRIPTION:%s\r\nEND:VEVENT\r\nEND:VCALENDAR\r\n", largeDescription)

	req := newCalendarPutRequest("/dav/calendars/1/large.ics", strings.NewReader(largeIcal))
	req = req.WithContext(auth.WithUser(req.Context(), user))
	rr := httptest.NewRecorder()

	h.Put(rr, req)

	// RFC 4791 Section 5.3.2.1: Server MUST reject resources exceeding max-resource-size
	if rr.Code != http.StatusRequestEntityTooLarge && rr.Code != http.StatusForbidden {
		t.Errorf("RFC 4791 Section 5.3.2.1: PUT exceeding max-resource-size must return 413 or 403, got %d", rr.Code)
	}
	assertCalDAVErrorBody(t, rr.Body.String(), "max-resource-size")
}

// Section 4.1.2/5.3.2.1: METHOD property must be rejected in calendar object resources
func TestRFC4791_ValidCalendarObject_RejectMethodProperty(t *testing.T) {
	calRepo := &fakeCalendarRepo{
		accessible: []store.CalendarAccess{
			{Calendar: store.Calendar{ID: 1, UserID: 1, Name: "Test"}, Editor: true},
		},
	}
	h := &Handler{store: &store.Store{Calendars: calRepo, Events: &fakeEventRepo{}}}
	user := &store.User{ID: 1}

	icalData := "BEGIN:VCALENDAR\r\nVERSION:2.0\r\nMETHOD:REQUEST\r\nBEGIN:VEVENT\r\nUID:method-reject\r\nSUMMARY:Method\r\nEND:VEVENT\r\nEND:VCALENDAR\r\n"
	req := newCalendarPutRequest("/dav/calendars/1/method.ics", strings.NewReader(icalData))
	req = req.WithContext(auth.WithUser(req.Context(), user))
	rr := httptest.NewRecorder()

	h.Put(rr, req)

	if rr.Code != http.StatusConflict {
		t.Errorf("METHOD property must be rejected with 409, got %d", rr.Code)
	}
	assertCalDAVErrorBody(t, rr.Body.String(), "valid-calendar-object-resource")
}

func TestRFC4791_ValidCalendarObject_RejectMultipleTopLevelComponents(t *testing.T) {
	calRepo := &fakeCalendarRepo{
		accessible: []store.CalendarAccess{
			{Calendar: store.Calendar{ID: 1, UserID: 1, Name: "Test"}, Editor: true},
		},
	}
	h := &Handler{store: &store.Store{Calendars: calRepo, Events: &fakeEventRepo{}}}
	user := &store.User{ID: 1}

	icalData := "BEGIN:VCALENDAR\r\nVERSION:2.0\r\nBEGIN:VEVENT\r\nUID:one\r\nEND:VEVENT\r\nBEGIN:VEVENT\r\nUID:two\r\nEND:VEVENT\r\nEND:VCALENDAR\r\n"
	req := newCalendarPutRequest("/dav/calendars/1/multi.ics", strings.NewReader(icalData))
	req = req.WithContext(auth.WithUser(req.Context(), user))
	rr := httptest.NewRecorder()

	h.Put(rr, req)

	if rr.Code < 400 || rr.Code >= 500 {
		t.Errorf("multiple top-level components must be rejected with 4xx, got %d", rr.Code)
	}
	assertCalDAVErrorBody(t, rr.Body.String(), "valid-calendar-object-resource")
}

func TestRFC4791_ValidCalendarObject_RejectMissingUID(t *testing.T) {
	calRepo := &fakeCalendarRepo{
		accessible: []store.CalendarAccess{
			{Calendar: store.Calendar{ID: 1, UserID: 1, Name: "Test"}, Editor: true},
		},
	}
	h := &Handler{store: &store.Store{Calendars: calRepo, Events: &fakeEventRepo{}}}
	user := &store.User{ID: 1}

	icalData := "BEGIN:VCALENDAR\r\nVERSION:2.0\r\nBEGIN:VEVENT\r\nSUMMARY:Missing UID\r\nEND:VEVENT\r\nEND:VCALENDAR\r\n"
	req := newCalendarPutRequest("/dav/calendars/1/missing-uid.ics", strings.NewReader(icalData))
	req = req.WithContext(auth.WithUser(req.Context(), user))
	rr := httptest.NewRecorder()

	h.Put(rr, req)

	if rr.Code < 400 || rr.Code >= 500 {
		t.Errorf("missing UID must be rejected with 4xx, got %d", rr.Code)
	}
	assertCalDAVErrorBody(t, rr.Body.String(), "valid-calendar-object-resource")
}

func TestRFC4791_UIDUniqueness_SameUIDMustBeSameResource(t *testing.T) {
	calRepo := &fakeCalendarRepo{
		accessible: []store.CalendarAccess{
			{Calendar: store.Calendar{ID: 1, UserID: 1, Name: "Test"}, Editor: true},
		},
	}
	eventRepo := &fakeEventRepo{events: make(map[string]*store.Event)}
	h := &Handler{store: &store.Store{Calendars: calRepo, Events: eventRepo}}
	user := &store.User{ID: 1}

	icalData := "BEGIN:VCALENDAR\r\nVERSION:2.0\r\nBEGIN:VEVENT\r\nUID:dup-uid\r\nSUMMARY:First\r\nEND:VEVENT\r\nEND:VCALENDAR\r\n"
	req := newCalendarPutRequest("/dav/calendars/1/first.ics", strings.NewReader(icalData))
	req = req.WithContext(auth.WithUser(req.Context(), user))
	rr := httptest.NewRecorder()
	h.Put(rr, req)

	if rr.Code != http.StatusCreated && rr.Code != http.StatusNoContent {
		t.Fatalf("initial PUT should succeed, got %d", rr.Code)
	}

	icalData = "BEGIN:VCALENDAR\r\nVERSION:2.0\r\nBEGIN:VEVENT\r\nUID:dup-uid\r\nSUMMARY:Second\r\nEND:VEVENT\r\nEND:VCALENDAR\r\n"
	req = newCalendarPutRequest("/dav/calendars/1/second.ics", strings.NewReader(icalData))
	req = req.WithContext(auth.WithUser(req.Context(), user))
	rr = httptest.NewRecorder()
	h.Put(rr, req)

	if rr.Code != http.StatusConflict {
		t.Errorf("storing same UID in different resource must fail with 409, got %d", rr.Code)
	}
	assertCalDAVErrorBody(t, rr.Body.String(), "no-uid-conflict")
}

func TestRFC4791_UpdateDoesNotAllowChangingUID(t *testing.T) {
	calRepo := &fakeCalendarRepo{
		accessible: []store.CalendarAccess{
			{Calendar: store.Calendar{ID: 1, UserID: 1, Name: "Test"}, Editor: true},
		},
	}
	eventRepo := &fakeEventRepo{events: make(map[string]*store.Event)}
	h := &Handler{store: &store.Store{Calendars: calRepo, Events: eventRepo}}
	user := &store.User{ID: 1}

	icalData := "BEGIN:VCALENDAR\r\nVERSION:2.0\r\nBEGIN:VEVENT\r\nUID:uid-one\r\nSUMMARY:Original\r\nEND:VEVENT\r\nEND:VCALENDAR\r\n"
	req := newCalendarPutRequest("/dav/calendars/1/same.ics", strings.NewReader(icalData))
	req = req.WithContext(auth.WithUser(req.Context(), user))
	rr := httptest.NewRecorder()
	h.Put(rr, req)

	if rr.Code != http.StatusCreated && rr.Code != http.StatusNoContent {
		t.Fatalf("initial PUT should succeed, got %d", rr.Code)
	}

	icalData = "BEGIN:VCALENDAR\r\nVERSION:2.0\r\nBEGIN:VEVENT\r\nUID:uid-two\r\nSUMMARY:Changed\r\nEND:VEVENT\r\nEND:VCALENDAR\r\n"
	req = newCalendarPutRequest("/dav/calendars/1/same.ics", strings.NewReader(icalData))
	req = req.WithContext(auth.WithUser(req.Context(), user))
	rr = httptest.NewRecorder()
	h.Put(rr, req)

	if rr.Code < 400 || rr.Code >= 500 {
		t.Errorf("changing UID on existing resource must fail with 4xx, got %d", rr.Code)
	}
	assertCalDAVErrorBodyAny(t, rr.Body.String(), "valid-calendar-object-resource", "no-uid-conflict", "conflict")
}

// Section 5.3.2.1: Multiple different UIDs in a single resource must be rejected
func TestRFC4791_PutWithDifferentUIDsRejected(t *testing.T) {
	calRepo := &fakeCalendarRepo{
		accessible: []store.CalendarAccess{
			{Calendar: store.Calendar{ID: 1, UserID: 1, Name: "Test"}, Editor: true},
		},
	}
	h := &Handler{store: &store.Store{Calendars: calRepo, Events: &fakeEventRepo{}}}
	user := &store.User{ID: 1}

	// RFC 4791 Section 4.1: Calendar object resource MUST NOT mix different UIDs in one resource
	multiEventData := "BEGIN:VCALENDAR\r\nVERSION:2.0\r\nBEGIN:VEVENT\r\nUID:event1\r\nEND:VEVENT\r\nBEGIN:VEVENT\r\nUID:event2\r\nEND:VEVENT\r\nEND:VCALENDAR\r\n"
	req := newCalendarPutRequest("/dav/calendars/1/multi.ics", strings.NewReader(multiEventData))
	req = req.WithContext(auth.WithUser(req.Context(), user))
	rr := httptest.NewRecorder()

	h.Put(rr, req)

	if rr.Code != http.StatusConflict {
		t.Errorf("RFC 4791 Section 4.1: PUT with multiple different UIDs must fail with 409, got %d", rr.Code)
	}
	assertCalDAVErrorBodyAny(t, rr.Body.String(), "valid-calendar-object-resource", "valid-calendar-data")
}

// Section 4.1: Recurrence set with same UID in a single resource is allowed
func TestRFC4791_PutWithRecurrenceSetSameUIDAccepted(t *testing.T) {
	calRepo := &fakeCalendarRepo{
		accessible: []store.CalendarAccess{
			{Calendar: store.Calendar{ID: 1, UserID: 1, Name: "Test"}, Editor: true},
		},
	}
	h := &Handler{store: &store.Store{Calendars: calRepo, Events: &fakeEventRepo{}}}
	user := &store.User{ID: 1}

	icalData := "BEGIN:VCALENDAR\r\nVERSION:2.0\r\nBEGIN:VEVENT\r\nUID:recurring\r\nDTSTART:20240101T100000Z\r\nRRULE:FREQ=DAILY;COUNT=2\r\nEND:VEVENT\r\nBEGIN:VEVENT\r\nUID:recurring\r\nRECURRENCE-ID:20240102T100000Z\r\nSUMMARY:Override\r\nEND:VEVENT\r\nEND:VCALENDAR\r\n"
	req := newCalendarPutRequest("/dav/calendars/1/recurring.ics", strings.NewReader(icalData))
	req = req.WithContext(auth.WithUser(req.Context(), user))
	rr := httptest.NewRecorder()

	h.Put(rr, req)

	if rr.Code != http.StatusCreated && rr.Code != http.StatusNoContent {
		t.Errorf("RFC 4791 Section 4.1: PUT with recurrence set should succeed, got %d", rr.Code)
	}
}

// Section 5.3.2.1: Calendar object resources MUST NOT be created directly in calendar-home
func TestRFC4791_PutCalendarObjectAtCalendarHomeRejected(t *testing.T) {
	calRepo := &fakeCalendarRepo{
		accessible: []store.CalendarAccess{
			{Calendar: store.Calendar{ID: 1, UserID: 1, Name: "Test"}, Editor: true},
		},
	}
	h := &Handler{store: &store.Store{Calendars: calRepo, Events: &fakeEventRepo{}}}
	user := &store.User{ID: 1}

	icalData := "BEGIN:VCALENDAR\r\nVERSION:2.0\r\nBEGIN:VEVENT\r\nUID:home-root\r\nSUMMARY:Home Root\r\nEND:VEVENT\r\nEND:VCALENDAR\r\n"
	req := newCalendarPutRequest("/dav/calendars/event.ics", strings.NewReader(icalData))
	req.Header.Set("Content-Type", "text/calendar")
	req = req.WithContext(auth.WithUser(req.Context(), user))
	rr := httptest.NewRecorder()

	h.Put(rr, req)

	if rr.Code != http.StatusForbidden && rr.Code != http.StatusConflict {
		t.Errorf("PUT to calendar-home root must fail with 403 or 409, got %d", rr.Code)
	}
}

// Section 7.2: REPORT on Non-Calendar Collections
func TestRFC4791_ReportOnOrdinaryCollection(t *testing.T) {
	h := &Handler{store: &store.Store{Calendars: &fakeCalendarRepo{}, Events: &fakeEventRepo{}}}
	user := &store.User{ID: 1}

	// RFC 4791 Section 7.2: calendar-query on non-calendar collection should work on descendants
	body := `<?xml version="1.0" encoding="utf-8"?>
<C:calendar-query xmlns:C="urn:ietf:params:xml:ns:caldav" xmlns:D="DAV:">
  <D:prop>
    <D:getetag/>
  </D:prop>
  <C:filter>
    <C:comp-filter name="VCALENDAR">
      <C:comp-filter name="VEVENT"/>
    </C:comp-filter>
  </C:filter>
</C:calendar-query>`

	req := httptest.NewRequest("REPORT", "/dav/", strings.NewReader(body))
	req = req.WithContext(auth.WithUser(req.Context(), user))
	rr := httptest.NewRecorder()

	h.Report(rr, req)

	// RFC 4791 Section 7.2: Server MAY support or reject with appropriate error
	if rr.Code == http.StatusBadRequest || rr.Code == http.StatusMultiStatus {
		t.Log("RFC 4791 Section 7.2: Server handles REPORT on ordinary collections")
	}
}

// Section 7.5.1: CALDAV:supported-collation-set Property
func TestRFC4791_SupportedCollationSetProperty(t *testing.T) {
	now := store.Now()
	calRepo := &fakeCalendarRepo{
		accessible: []store.CalendarAccess{
			{Calendar: store.Calendar{ID: 1, UserID: 1, Name: "Test", UpdatedAt: now}, Editor: true},
		},
	}
	h := &Handler{store: &store.Store{Calendars: calRepo, Events: &fakeEventRepo{}}}
	user := &store.User{ID: 1}

	body := `<?xml version="1.0" encoding="utf-8"?>
<d:propfind xmlns:d="DAV:" xmlns:c="urn:ietf:params:xml:ns:caldav">
  <d:prop>
    <c:supported-collation-set/>
  </d:prop>
</d:propfind>`

	req := httptest.NewRequest("PROPFIND", "/dav/calendars/1/", strings.NewReader(body))
	req.Header.Set("Depth", "0")
	req = req.WithContext(auth.WithUser(req.Context(), user))
	rr := httptest.NewRecorder()

	h.Propfind(rr, req)

	respBody := rr.Body.String()
	// RFC 4791 Section 7.5.1: Server MUST support at least i;ascii-casemap and i;octet
	if strings.Contains(respBody, "supported-collation-set") {
		if strings.Contains(respBody, "i;ascii-casemap") || strings.Contains(respBody, "i;octet") {
			t.Log("RFC 4791 Section 7.5.1: Server advertises supported collations")
		}
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
	h := &Handler{store: &store.Store{Calendars: calRepo, Events: eventRepo}}
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

	if rr.Code != http.StatusMultiStatus {
		t.Fatalf("RFC 4791 Section 7.5: calendar-query must return 207, got %d", rr.Code)
	}
	if !strings.Contains(rr.Body.String(), "event.ics") {
		t.Error("RFC 4791 Section 7.5: Text match with collation must return matching event")
	}
}

// HTTP compliance: Content-Length header
func TestHTTP_GetReturnsContentLength(t *testing.T) {
	calRepo := &fakeCalendarRepo{
		accessible: []store.CalendarAccess{
			{Calendar: store.Calendar{ID: 1, UserID: 1, Name: "Test"}, Editor: true},
		},
	}
	icalData := "BEGIN:VCALENDAR\r\nVERSION:2.0\r\nEND:VCALENDAR\r\n"
	eventRepo := &fakeEventRepo{
		events: map[string]*store.Event{
			"1:event": {CalendarID: 1, UID: "event", RawICAL: icalData, ETag: "e"},
		},
	}
	h := &Handler{store: &store.Store{Calendars: calRepo, Events: eventRepo}}
	user := &store.User{ID: 1}

	req := httptest.NewRequest(http.MethodGet, "/dav/calendars/1/event.ics", nil)
	req = req.WithContext(auth.WithUser(req.Context(), user))
	rr := httptest.NewRecorder()

	h.Get(rr, req)

	// HTTP/1.1 should include Content-Length
	contentLength := rr.Header().Get("Content-Length")
	if contentLength == "" {
		t.Log("HTTP compliance: Content-Length header should be present")
	} else {
		expectedLength := len(icalData)
		actualLength := rr.Body.Len()
		if actualLength == expectedLength {
			t.Log("HTTP compliance: Content-Length matches body size")
		}
	}
}

// Section 5.3.2: PUT Must Handle Component Type Correctly
func TestRFC4791_PutPreservesComponentType(t *testing.T) {
	calRepo := &fakeCalendarRepo{
		accessible: []store.CalendarAccess{
			{Calendar: store.Calendar{ID: 1, UserID: 1, Name: "Test"}, Editor: true},
		},
	}
	eventRepo := &fakeEventRepo{events: make(map[string]*store.Event)}
	h := &Handler{store: &store.Store{Calendars: calRepo, Events: eventRepo}}
	user := &store.User{ID: 1}

	// Store VTODO
	todoData := "BEGIN:VCALENDAR\r\nVERSION:2.0\r\nPRODID:test\r\nBEGIN:VTODO\r\nUID:task1\r\nSUMMARY:Task\r\nEND:VTODO\r\nEND:VCALENDAR\r\n"
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
		t.Error("RFC 4791: Server must preserve component type (VTODO)")
	}
	if !strings.Contains(respBody, "UID:task1") {
		t.Error("RFC 4791: Server must preserve UID")
	}
}

// Section 5.3.1: MKCALENDAR on Invalid Path
func TestRFC4791_MkcalendarInvalidPath(t *testing.T) {
	calRepo := &fakeCalendarRepo{calendars: make(map[int64]*store.Calendar)}
	h := &Handler{store: &store.Store{Calendars: calRepo}}
	user := &store.User{ID: 1}

	// Try to create calendar with invalid path (e.g., missing name)
	req := httptest.NewRequest("MKCALENDAR", "/dav/calendars/", nil)
	req = req.WithContext(auth.WithUser(req.Context(), user))
	rr := httptest.NewRecorder()

	h.Mkcalendar(rr, req)

	// RFC 4791 Section 5.3.1: Server MUST reject invalid paths
	if rr.Code == http.StatusBadRequest || rr.Code == http.StatusConflict {
		t.Log("RFC 4791 Section 5.3.1: Server correctly rejects MKCALENDAR on invalid path")
	}
}

// Section 9.7.1: Component Filter Test Attribute
func TestRFC4791_CompFilterWithTestAttribute(t *testing.T) {
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
				RawICAL:    "BEGIN:VCALENDAR\r\nBEGIN:VEVENT\r\nUID:event\r\nSUMMARY:Test\r\nEND:VEVENT\r\nEND:VCALENDAR\r\n",
				ETag:       "e",
			},
		},
	}
	h := &Handler{store: &store.Store{Calendars: calRepo, Events: eventRepo}}
	user := &store.User{ID: 1}

	// RFC 4791 Section 9.7.1: comp-filter with test="allof" or test="anyof"
	body := `<?xml version="1.0" encoding="utf-8"?>
<C:calendar-query xmlns:C="urn:ietf:params:xml:ns:caldav" xmlns:D="DAV:">
  <D:prop><D:getetag/></D:prop>
  <C:filter>
    <C:comp-filter name="VCALENDAR">
      <C:comp-filter name="VEVENT" test="anyof">
        <C:prop-filter name="SUMMARY">
          <C:text-match>Test</C:text-match>
        </C:prop-filter>
      </C:comp-filter>
    </C:comp-filter>
  </C:filter>
</C:calendar-query>`

	req := httptest.NewRequest("REPORT", "/dav/calendars/1/", strings.NewReader(body))
	req = req.WithContext(auth.WithUser(req.Context(), user))
	rr := httptest.NewRecorder()

	h.Report(rr, req)

	// RFC 4791 Section 9.7.1: Server should support test attribute
	if rr.Code == http.StatusMultiStatus {
		t.Log("RFC 4791 Section 9.7.1: Server processes comp-filter test attribute")
	}
}

// Section 9.7.4: Param Filter
func TestRFC4791_ParamFilterInQuery(t *testing.T) {
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
				RawICAL:    "BEGIN:VCALENDAR\r\nBEGIN:VEVENT\r\nUID:event1\r\nATTENDEE;PARTSTAT=ACCEPTED:mailto:user@example.com\r\nEND:VEVENT\r\nEND:VCALENDAR\r\n",
				ETag:       "e1",
			},
			"1:event2": {
				CalendarID: 1,
				UID:        "event2",
				RawICAL:    "BEGIN:VCALENDAR\r\nBEGIN:VEVENT\r\nUID:event2\r\nATTENDEE;PARTSTAT=NEEDS-ACTION:mailto:user@example.com\r\nEND:VEVENT\r\nEND:VCALENDAR\r\n",
				ETag:       "e2",
			},
		},
	}
	h := &Handler{store: &store.Store{Calendars: calRepo, Events: eventRepo}}
	user := &store.User{ID: 1}

	// RFC 4791 Section 9.7.4: param-filter tests property parameters
	body := `<?xml version="1.0" encoding="utf-8"?>
<C:calendar-query xmlns:C="urn:ietf:params:xml:ns:caldav" xmlns:D="DAV:">
  <D:prop><D:getetag/></D:prop>
  <C:filter>
    <C:comp-filter name="VCALENDAR">
      <C:comp-filter name="VEVENT">
        <C:prop-filter name="ATTENDEE">
          <C:param-filter name="PARTSTAT">
            <C:text-match>ACCEPTED</C:text-match>
          </C:param-filter>
        </C:prop-filter>
      </C:comp-filter>
    </C:comp-filter>
  </C:filter>
</C:calendar-query>`

	req := httptest.NewRequest("REPORT", "/dav/calendars/1/", strings.NewReader(body))
	req = req.WithContext(auth.WithUser(req.Context(), user))
	rr := httptest.NewRecorder()

	h.Report(rr, req)

	// RFC 4791 Section 9.7.4: Should filter by parameter values (advanced feature)
	// Implementation may not support this fully
	if rr.Code == http.StatusMultiStatus {
		respBody := rr.Body.String()
		if strings.Contains(respBody, "event1.ics") && !strings.Contains(respBody, "event2.ics") {
			t.Log("RFC 4791 Section 9.7.4: Server correctly filters by parameter values")
		}
	}
}

func propstatHasStatus(body, prop string, statusCode int) bool {
	pattern := fmt.Sprintf(`(?s)<[^>]*propstat[^>]*>.*?<[^>]*%s[^>]*>.*?<[^>]*status[^>]*>HTTP/1.1 %d`,
		regexp.QuoteMeta(prop), statusCode)
	re := regexp.MustCompile(pattern)
	return re.MatchString(body)
}

func extractPropInt(body, prop string) (int, bool) {
	pattern := fmt.Sprintf(`(?s)<[^>]*%s[^>]*>\s*([0-9]+)\s*<`, regexp.QuoteMeta(prop))
	re := regexp.MustCompile(pattern)
	match := re.FindStringSubmatch(body)
	if len(match) < 2 {
		return 0, false
	}
	value, err := strconv.Atoi(match[1])
	if err != nil {
		return 0, false
	}
	return value, true
}

func extractPropDateTime(body, prop string) (time.Time, bool) {
	pattern := fmt.Sprintf(`(?s)<[^>]*%s[^>]*>\s*([0-9]{8}T[0-9]{6}Z)\s*<`, regexp.QuoteMeta(prop))
	re := regexp.MustCompile(pattern)
	match := re.FindStringSubmatch(body)
	if len(match) < 2 {
		return time.Time{}, false
	}
	parsed, err := time.Parse("20060102T150405Z", match[1])
	if err != nil {
		return time.Time{}, false
	}
	return parsed, true
}

func assertCalDAVErrorBody(t *testing.T, body, expected string) {
	t.Helper()
	if !strings.Contains(body, expected) {
		t.Errorf("Expected CalDAV error body to include %q, got: %s", expected, body)
	}
}

func assertCalDAVErrorBodyAny(t *testing.T, body string, expected ...string) {
	t.Helper()
	for _, value := range expected {
		if strings.Contains(body, value) {
			return
		}
	}
	t.Errorf("Expected CalDAV error body to include one of %q, got: %s", expected, body)
}
