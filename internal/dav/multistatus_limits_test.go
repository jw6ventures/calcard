package dav

import (
	"bytes"
	"context"
	"encoding/xml"
	"errors"
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

// limitsTestServer builds a calendar collection of size events behind the given
// limits, so a test can drive one REPORT against the whole handler path rather
// than against a query helper in isolation.
func limitsTestServer(t *testing.T, cfg *config.Config, events int) *DavServer {
	t.Helper()
	calRepo := &fakeCalendarRepo{
		accessible: []store.CalendarAccess{
			{Calendar: store.Calendar{ID: 1, UserID: 1, Name: "Test"}, Editor: true},
		},
	}
	eventRepo := &fakeEventRepo{events: make(map[string]*store.Event, events)}
	for id := int64(1); id <= int64(events); id++ {
		uid := fmt.Sprintf("event-%d", id)
		eventRepo.events["1:"+uid] = &store.Event{
			ID:           id,
			CalendarID:   1,
			UID:          uid,
			ResourceName: uid,
			ETag:         "e",
			RawICAL: "BEGIN:VCALENDAR\r\nBEGIN:VEVENT\r\nUID:" + uid +
				"\r\nDTSTART:20240101T000000Z\r\nDTEND:20240101T010000Z\r\nEND:VEVENT\r\nEND:VCALENDAR\r\n",
		}
	}
	return NewDavServer(Options{Config: cfg, Store: &store.Store{Calendars: calRepo, Events: eventRepo}})
}

func limitsReportRequest(t *testing.T, h *DavServer, body string) *httptest.ResponseRecorder {
	t.Helper()
	req := httptest.NewRequest("REPORT", "/dav/calendars/1/", strings.NewReader(body))
	req.Header.Set("Depth", "1")
	req = req.WithContext(auth.WithUser(req.Context(), &store.User{ID: 1}))
	rr := httptest.NewRecorder()
	h.Report(rr, req)
	return rr
}

const limitsCalendarQueryBody = `<?xml version="1.0" encoding="utf-8"?>
<C:calendar-query xmlns:D="DAV:" xmlns:C="urn:ietf:params:xml:ns:caldav">
  <D:prop><D:getetag/></D:prop>
  <C:filter><C:comp-filter name="VCALENDAR"><C:comp-filter name="VEVENT"/></C:comp-filter></C:filter>
</C:calendar-query>`

// RFC 4791 §7.8 postcondition DAV:number-of-matches-within-limits, returned per
// §1.3 as the child of a top-level DAV:error. A truncated multistatus would
// claim to be the complete answer to the query, which is the failure mode the
// postcondition exists to prevent.
func TestCalendarQueryOverTheMatchLimitFailsNumberOfMatchesWithinLimits(t *testing.T) {
	cfg := &config.Config{}
	cfg.DAV.MaxMultistatusResponses = 2
	cfg.DAV.MaxMultistatusBytes = defaultMaxMultistatusBytes
	h := limitsTestServer(t, cfg, 3)

	rr := limitsReportRequest(t, h, limitsCalendarQueryBody)

	assertErrorConditions(t, rr, http.StatusForbidden, davQN("number-of-matches-within-limits"))
	if strings.Contains(rr.Body.String(), "multistatus") {
		t.Fatalf("the postcondition returned a multistatus: %s", rr.Body.String())
	}
}

// The limit is inclusive: a query matching exactly as many resources as the
// server allows is answered, and only the one past it fails.
func TestCalendarQueryAtTheMatchLimitReturnsEveryResponse(t *testing.T) {
	cfg := &config.Config{}
	cfg.DAV.MaxMultistatusResponses = 3
	cfg.DAV.MaxMultistatusBytes = defaultMaxMultistatusBytes
	h := limitsTestServer(t, cfg, 3)

	rr := limitsReportRequest(t, h, limitsCalendarQueryBody)

	if rr.Code != http.StatusMultiStatus {
		t.Fatalf("status = %d, want 207; body: %s", rr.Code, rr.Body.String())
	}
	ms := decodeMultistatus(t, rr)
	if len(ms.Responses) != 3 {
		t.Fatalf("responses = %d, want 3", len(ms.Responses))
	}
}

// RFC 4791 §11 asks a server to bound the work one report can provoke. A filter
// matching nothing still reads and parses every candidate row, so the row
// budget is what bounds a query whose match count never grows.
func TestCalendarQueryOverTheCandidateRowLimitFailsNumberOfMatchesWithinLimits(t *testing.T) {
	cfg := &config.Config{}
	cfg.DAV.MaxMultistatusResponses = defaultMaxMultistatusResponses
	cfg.DAV.MaxMultistatusBytes = defaultMaxMultistatusBytes
	cfg.DAV.MaxReportCandidateRows = 100
	h := limitsTestServer(t, cfg, 400)

	body := `<?xml version="1.0" encoding="utf-8"?>
<C:calendar-query xmlns:D="DAV:" xmlns:C="urn:ietf:params:xml:ns:caldav">
  <D:prop><D:getetag/></D:prop>
  <C:filter><C:comp-filter name="VCALENDAR"><C:comp-filter name="VEVENT">
    <C:prop-filter name="SUMMARY"><C:text-match>matches-nothing</C:text-match></C:prop-filter>
  </C:comp-filter></C:comp-filter></C:filter>
</C:calendar-query>`

	rr := limitsReportRequest(t, h, body)

	assertErrorConditions(t, rr, http.StatusForbidden, davQN("number-of-matches-within-limits"))
}

func TestCalendarQueryAtTheCandidateRowLimitIsAnswered(t *testing.T) {
	cfg := &config.Config{}
	cfg.DAV.MaxMultistatusResponses = defaultMaxMultistatusResponses
	cfg.DAV.MaxMultistatusBytes = defaultMaxMultistatusBytes
	cfg.DAV.MaxReportCandidateRows = 256
	h := limitsTestServer(t, cfg, 256)

	rr := limitsReportRequest(t, h, limitsCalendarQueryBody)

	if rr.Code != http.StatusMultiStatus {
		t.Fatalf("status = %d, want 207; body: %s", rr.Code, rr.Body.String())
	}
}

// RFC 4791 §7.10 gives free-busy-query the same postcondition §7.8 gives
// calendar-query. The report answers one iCalendar object rather than a
// multistatus, so its own row budget is the only thing bounding the collection
// it reads and parses.
func TestFreeBusyQueryOverTheCandidateRowLimitFailsNumberOfMatchesWithinLimits(t *testing.T) {
	cfg := &config.Config{}
	cfg.DAV.MaxReportCandidateRows = 100
	h := limitsTestServer(t, cfg, 400)

	body := `<?xml version="1.0" encoding="utf-8"?>
<C:free-busy-query xmlns:C="urn:ietf:params:xml:ns:caldav">
  <C:time-range start="20230101T000000Z" end="20250101T000000Z"/>
</C:free-busy-query>`

	rr := limitsReportRequest(t, h, body)

	assertErrorConditions(t, rr, http.StatusForbidden, davQN("number-of-matches-within-limits"))
	if strings.Contains(rr.Body.String(), "VFREEBUSY") {
		t.Fatalf("the postcondition returned free-busy data: %s", rr.Body.String())
	}
}

func TestFreeBusyQueryAtTheCandidateRowLimitIsAnswered(t *testing.T) {
	cfg := &config.Config{}
	cfg.DAV.MaxReportCandidateRows = 256
	h := limitsTestServer(t, cfg, 256)

	body := `<?xml version="1.0" encoding="utf-8"?>
<C:free-busy-query xmlns:C="urn:ietf:params:xml:ns:caldav">
  <C:time-range start="20230101T000000Z" end="20250101T000000Z"/>
</C:free-busy-query>`

	rr := limitsReportRequest(t, h, body)

	if rr.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200; body: %s", rr.Code, rr.Body.String())
	}
	if !strings.Contains(rr.Body.String(), "VFREEBUSY") {
		t.Fatalf("free-busy response carried no VFREEBUSY: %s", rr.Body.String())
	}
}

// RFC 4791 §7.9 owes one DAV:response per requested DAV:href. A multiget over
// the server's href limit is therefore refused outright: answering the leading
// hrefs and dropping the rest would present a partial result as the complete
// one. §7.9 states no DAV:number-of-matches-within-limits postcondition of its
// own -- §7.8 and §7.10 are the sections that give one -- so the refusal is the
// RFC 4918 capacity status rather than a CalDAV condition invented for it.
func TestCalendarMultigetOverTheHrefLimitIsRefusedRatherThanTruncated(t *testing.T) {
	cfg := &config.Config{}
	cfg.DAV.MaxMultigetHrefs = 2
	h := limitsTestServer(t, cfg, 3)

	var hrefs strings.Builder
	for id := 1; id <= 3; id++ {
		fmt.Fprintf(&hrefs, "<D:href>/dav/calendars/1/event-%d.ics</D:href>", id)
	}
	body := `<?xml version="1.0" encoding="utf-8"?>
<C:calendar-multiget xmlns:D="DAV:" xmlns:C="urn:ietf:params:xml:ns:caldav">
  <D:prop><D:getetag/></D:prop>` + hrefs.String() + `
</C:calendar-multiget>`

	rr := limitsReportRequest(t, h, body)

	if rr.Code != http.StatusInsufficientStorage {
		t.Fatalf("status = %d, want 507; body: %s", rr.Code, rr.Body.String())
	}
	if strings.Contains(rr.Body.String(), "multistatus") {
		t.Fatalf("an over-limit multiget returned a partial multistatus: %s", rr.Body.String())
	}
}

func TestCalendarMultigetAtTheHrefLimitAnswersEveryHref(t *testing.T) {
	cfg := &config.Config{}
	cfg.DAV.MaxMultigetHrefs = 3
	h := limitsTestServer(t, cfg, 3)

	var hrefs strings.Builder
	for id := 1; id <= 3; id++ {
		fmt.Fprintf(&hrefs, "<D:href>/dav/calendars/1/event-%d.ics</D:href>", id)
	}
	body := `<?xml version="1.0" encoding="utf-8"?>
<C:calendar-multiget xmlns:D="DAV:" xmlns:C="urn:ietf:params:xml:ns:caldav">
  <D:prop><D:getetag/></D:prop>` + hrefs.String() + `
</C:calendar-multiget>`

	rr := limitsReportRequest(t, h, body)

	if rr.Code != http.StatusMultiStatus {
		t.Fatalf("status = %d, want 207; body: %s", rr.Code, rr.Body.String())
	}
	ms := decodeMultistatus(t, rr)
	if len(ms.Responses) != 3 {
		t.Fatalf("responses = %d, want one per href", len(ms.Responses))
	}
}

// hostileRecurrenceServer stores a resource whose recurrence rule terminates at
// neither COUNT nor UNTIL and fires once a second. PUT refuses that shape
// today, so the only way it reaches a report is as data already in the
// collection -- a row stored before a limit existed, or written around the DAV
// layer. The reports still have to answer it in bounded time.
func hostileRecurrenceServer() *DavServer {
	calRepo := &fakeCalendarRepo{
		accessible: []store.CalendarAccess{
			{Calendar: store.Calendar{ID: 1, UserID: 1, Name: "Test"}, Editor: true},
		},
	}
	eventRepo := &fakeEventRepo{events: map[string]*store.Event{
		"1:forever": {
			ID: 1, CalendarID: 1, UID: "forever", ResourceName: "forever", ETag: "e",
			RawICAL: "BEGIN:VCALENDAR\r\nVERSION:2.0\r\nBEGIN:VEVENT\r\nUID:forever\r\n" +
				"DTSTART:20240101T000000Z\r\nDTEND:20240101T000001Z\r\nRRULE:FREQ=SECONDLY\r\n" +
				"END:VEVENT\r\nEND:VCALENDAR\r\n",
		},
	}}
	return &DavServer{store: &store.Store{Calendars: calRepo, Events: eventRepo}}
}

func TestExpandOfAnUnboundedSubSecondRecurrenceStaysBounded(t *testing.T) {
	h := hostileRecurrenceServer()
	body := `<?xml version="1.0" encoding="utf-8"?>
<C:calendar-query xmlns:D="DAV:" xmlns:C="urn:ietf:params:xml:ns:caldav">
  <D:prop><C:calendar-data><C:expand start="20240101T000000Z" end="20250101T000000Z"/></C:calendar-data></D:prop>
  <C:filter><C:comp-filter name="VCALENDAR"><C:comp-filter name="VEVENT"/></C:comp-filter></C:filter>
</C:calendar-query>`

	req := httptest.NewRequest("REPORT", "/dav/calendars/1/", strings.NewReader(body))
	req.Header.Set("Depth", "1")
	req = req.WithContext(auth.WithUser(req.Context(), &store.User{ID: 1}))
	rr := httptest.NewRecorder()
	h.Report(rr, req)

	if rr.Code != http.StatusMultiStatus {
		t.Fatalf("status = %d, want 207; body: %s", rr.Code, rr.Body.String())
	}
	// A year of one-second instances is 31,536,000 components. The expansion
	// budget is what keeps the response to the instance limit instead.
	if instances := strings.Count(rr.Body.String(), "BEGIN:VEVENT"); instances > ical.MaxRecurrenceInstances {
		t.Fatalf("expanded instances = %d, want at most %d", instances, ical.MaxRecurrenceInstances)
	}
}

func TestFreeBusyOfAnUnboundedSubSecondRecurrenceStaysBounded(t *testing.T) {
	h := hostileRecurrenceServer()
	body := `<?xml version="1.0" encoding="utf-8"?>
<C:free-busy-query xmlns:C="urn:ietf:params:xml:ns:caldav">
  <C:time-range start="20240101T000000Z" end="20250101T000000Z"/>
</C:free-busy-query>`

	req := httptest.NewRequest("REPORT", "/dav/calendars/1/", strings.NewReader(body))
	req.Header.Set("Depth", "1")
	req = req.WithContext(auth.WithUser(req.Context(), &store.User{ID: 1}))
	rr := httptest.NewRecorder()
	h.Report(rr, req)

	if rr.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200; body: %s", rr.Code, rr.Body.String())
	}
	if periods := strings.Count(rr.Body.String(), "FREEBUSY"); periods > ical.MaxRecurrenceInstances {
		t.Fatalf("free-busy periods = %d, want at most %d", periods, ical.MaxRecurrenceInstances)
	}
}

// A body at the size cap is refused on its Content-Length rather than read,
// parsed and then measured.
func TestOversizedReportBodyIsRefusedBeforeParsing(t *testing.T) {
	h := hostileRecurrenceServer()
	filler := strings.Repeat("x", int(maxDAVBodyBytes)+1)
	body := calendarFilterBody(`<C:comp-filter name="VCALENDAR"><C:comp-filter name="VEVENT">` +
		`<C:prop-filter name="SUMMARY"><C:text-match>` + filler + `</C:text-match></C:prop-filter>` +
		`</C:comp-filter></C:comp-filter>`)

	req := httptest.NewRequest("REPORT", "/dav/calendars/1/", strings.NewReader(body))
	req.Header.Set("Depth", "1")
	req = req.WithContext(auth.WithUser(req.Context(), &store.User{ID: 1}))
	rr := httptest.NewRecorder()
	h.Report(rr, req)

	if rr.Code != http.StatusRequestEntityTooLarge {
		t.Fatalf("status = %d, want 413; body: %s", rr.Code, rr.Body.String())
	}
}

// The generated birthday collection is a calendar collection answering
// calendar-query and calendar-multiget, so the two limits reach it on the same
// terms as a stored one: no truncated match set, and no truncated href list.
func TestBirthdayCalendarReportsHonourTheSameLimits(t *testing.T) {
	contacts := map[string]*store.Contact{}
	for i := 1; i <= 5; i++ {
		uid := fmt.Sprintf("contact-%d", i)
		name := fmt.Sprintf("Person %d", i)
		birthday := time.Date(1990, 6, 15, 0, 0, 0, 0, time.UTC)
		contacts["1:"+uid] = &store.Contact{
			ID: int64(i), AddressBookID: 1, UID: uid, DisplayName: &name, Birthday: &birthday,
		}
	}
	newServer := func(configure func(*config.Config)) *DavServer {
		cfg := &config.Config{}
		configure(cfg)
		return NewDavServer(Options{Config: cfg, Store: &store.Store{Contacts: &fakeContactRepo{contacts: contacts}}})
	}
	run := func(t *testing.T, h *DavServer, body string) *httptest.ResponseRecorder {
		t.Helper()
		req := httptest.NewRequest("REPORT", birthdayCalendarHref(), strings.NewReader(body))
		req.Header.Set("Depth", "1")
		req = req.WithContext(auth.WithUser(req.Context(), &store.User{ID: 1}))
		rr := httptest.NewRecorder()
		h.Report(rr, req)
		return rr
	}

	t.Run("calendar-query past the match limit", func(t *testing.T) {
		h := newServer(func(cfg *config.Config) { cfg.DAV.MaxMultistatusResponses = 2 })
		rr := run(t, h, limitsCalendarQueryBody)
		assertErrorConditions(t, rr, http.StatusForbidden, davQN("number-of-matches-within-limits"))
	})

	t.Run("calendar-query at the match limit", func(t *testing.T) {
		h := newServer(func(cfg *config.Config) { cfg.DAV.MaxMultistatusResponses = 5 })
		rr := run(t, h, limitsCalendarQueryBody)
		if rr.Code != http.StatusMultiStatus {
			t.Fatalf("status = %d, want 207; body: %s", rr.Code, rr.Body.String())
		}
	})

	t.Run("calendar-multiget past the href limit", func(t *testing.T) {
		h := newServer(func(cfg *config.Config) { cfg.DAV.MaxMultigetHrefs = 2 })
		var hrefs strings.Builder
		for i := 1; i <= 5; i++ {
			fmt.Fprintf(&hrefs, "<D:href>%scontact-%d.ics</D:href>", birthdayCalendarHref(), i)
		}
		body := `<?xml version="1.0" encoding="utf-8"?>
<C:calendar-multiget xmlns:D="DAV:" xmlns:C="urn:ietf:params:xml:ns:caldav">
  <D:prop><D:getetag/></D:prop>` + hrefs.String() + `</C:calendar-multiget>`

		rr := run(t, h, body)

		if rr.Code != http.StatusInsufficientStorage {
			t.Fatalf("status = %d, want 507; body: %s", rr.Code, rr.Body.String())
		}
	})
}

// PROPFIND carries no CalDAV postcondition, so the RFC 4918 capacity answer
// stays what it was: the CalDAV reading does not leak onto it.
func TestPropfindOverTheResponseLimitStillReturns507(t *testing.T) {
	cfg := &config.Config{}
	cfg.DAV.MaxMultistatusResponses = 1
	cfg.DAV.MaxMultistatusBytes = defaultMaxMultistatusBytes
	h := limitsTestServer(t, cfg, 5)

	req := httptest.NewRequest("PROPFIND", "/dav/calendars/1/", strings.NewReader(
		`<?xml version="1.0" encoding="utf-8"?><D:propfind xmlns:D="DAV:"><D:prop><D:getetag/></D:prop></D:propfind>`))
	req.Header.Set("Depth", "1")
	req = req.WithContext(auth.WithUser(req.Context(), &store.User{ID: 1}))
	rr := httptest.NewRecorder()
	h.Propfind(rr, req)

	if rr.Code != http.StatusInsufficientStorage {
		t.Fatalf("status = %d, want 507; body: %s", rr.Code, rr.Body.String())
	}
}

// Every limit accepts 0 as "unlimited", which config.Load carries as
// math.MaxInt so the comparisons downstream need no separate "is it set" test.
// A report may therefore not size an allocation from a limit: the reports below
// answer over a collection the server will read whole, and a preallocation the
// size of the budget is neither possible nor wanted.
func TestReportsAnswerWithEveryLimitDisabled(t *testing.T) {
	unlimited := func() *config.Config {
		cfg := &config.Config{}
		cfg.DAV.MaxMultistatusResponses = math.MaxInt
		cfg.DAV.MaxMultistatusBytes = math.MaxInt
		cfg.DAV.MaxFilterElements = math.MaxInt
		cfg.DAV.MaxReportElementDepth = math.MaxInt
		cfg.DAV.MaxMultigetHrefs = math.MaxInt
		cfg.DAV.MaxReportCandidateRows = math.MaxInt
		return cfg
	}

	t.Run("calendar-query", func(t *testing.T) {
		h := limitsTestServer(t, unlimited(), 3)

		rr := limitsReportRequest(t, h, limitsCalendarQueryBody)

		if rr.Code != http.StatusMultiStatus {
			t.Fatalf("status = %d, want 207; body: %s", rr.Code, rr.Body.String())
		}
		if ms := decodeMultistatus(t, rr); len(ms.Responses) != 3 {
			t.Fatalf("responses = %d, want 3", len(ms.Responses))
		}
	})

	t.Run("calendar-multiget", func(t *testing.T) {
		h := limitsTestServer(t, unlimited(), 3)
		var hrefs strings.Builder
		for id := 1; id <= 3; id++ {
			fmt.Fprintf(&hrefs, "<D:href>/dav/calendars/1/event-%d.ics</D:href>", id)
		}
		body := `<?xml version="1.0" encoding="utf-8"?>
<C:calendar-multiget xmlns:D="DAV:" xmlns:C="urn:ietf:params:xml:ns:caldav">
  <D:prop><D:getetag/></D:prop>` + hrefs.String() + `
</C:calendar-multiget>`

		rr := limitsReportRequest(t, h, body)

		if rr.Code != http.StatusMultiStatus {
			t.Fatalf("status = %d, want 207; body: %s", rr.Code, rr.Body.String())
		}
		if ms := decodeMultistatus(t, rr); len(ms.Responses) != 3 {
			t.Fatalf("responses = %d, want one per href", len(ms.Responses))
		}
	})

	t.Run("free-busy-query", func(t *testing.T) {
		h := limitsTestServer(t, unlimited(), 3)

		rr := limitsReportRequest(t, h, `<?xml version="1.0" encoding="utf-8"?>
<C:free-busy-query xmlns:C="urn:ietf:params:xml:ns:caldav">
  <C:time-range start="20230101T000000Z" end="20250101T000000Z"/>
</C:free-busy-query>`)

		if rr.Code != http.StatusOK {
			t.Fatalf("status = %d, want 200; body: %s", rr.Code, rr.Body.String())
		}
	})

	t.Run("sync-collection", func(t *testing.T) {
		h := limitsTestServer(t, unlimited(), 3)

		rr := limitsReportRequest(t, h, `<?xml version="1.0" encoding="utf-8"?>
<D:sync-collection xmlns:D="DAV:"><D:sync-token/><D:prop><D:getetag/></D:prop></D:sync-collection>`)

		if rr.Code != http.StatusMultiStatus {
			t.Fatalf("status = %d, want 207; body: %s", rr.Code, rr.Body.String())
		}
	})

	t.Run("PROPFIND", func(t *testing.T) {
		h := limitsTestServer(t, unlimited(), 3)

		req := httptest.NewRequest("PROPFIND", "/dav/calendars/1/", strings.NewReader(
			`<?xml version="1.0" encoding="utf-8"?><D:propfind xmlns:D="DAV:"><D:prop><D:getetag/></D:prop></D:propfind>`))
		req.Header.Set("Depth", "1")
		req = req.WithContext(auth.WithUser(req.Context(), &store.User{ID: 1}))
		rr := httptest.NewRecorder()
		h.Propfind(rr, req)

		if rr.Code != http.StatusMultiStatus {
			t.Fatalf("status = %d, want 207; body: %s", rr.Code, rr.Body.String())
		}
	})
}

func TestWriteBoundedMultiStatusRejectsResponseCountBeforeHeaders(t *testing.T) {
	cfg := &config.Config{}
	cfg.DAV.MaxMultistatusResponses = 1
	cfg.DAV.MaxMultistatusBytes = 1024
	h := NewDavServer(Options{Config: cfg})
	rr := httptest.NewRecorder()

	h.writeBoundedMultiStatus(rr, newMultistatus([]response{{Href: "/one"}, {Href: "/two"}}, ""))

	if rr.Code != http.StatusInsufficientStorage {
		t.Fatalf("status = %d, want 507", rr.Code)
	}
	if strings.Contains(rr.Body.String(), "multistatus") {
		t.Fatalf("hard response limit returned a partial multistatus: %s", rr.Body.String())
	}
}

func TestWriteBoundedMultiStatusRejectsEncodedBytesBeforeHeaders(t *testing.T) {
	cfg := &config.Config{}
	cfg.DAV.MaxMultistatusResponses = 10
	cfg.DAV.MaxMultistatusBytes = 64
	h := NewDavServer(Options{Config: cfg})
	rr := httptest.NewRecorder()

	h.writeBoundedMultiStatus(rr, newMultistatus([]response{{Href: "/a-very-long-resource-name"}}, ""))

	if rr.Code != http.StatusInsufficientStorage {
		t.Fatalf("status = %d, want 507", rr.Code)
	}
	if strings.Contains(rr.Body.String(), "multistatus") {
		t.Fatalf("hard byte limit returned a partial multistatus: %s", rr.Body.String())
	}
}

func TestWriteBoundedMultiStatusEncodedByteBoundary(t *testing.T) {
	payload := newMultistatus([]response{{Href: "/exact-boundary"}}, "")
	var encoded bytes.Buffer
	if err := xml.NewEncoder(&encoded).Encode(payload); err != nil {
		t.Fatalf("Encode() error = %v", err)
	}

	for _, tt := range []struct {
		name       string
		limit      int
		wantStatus int
	}{
		{name: "exact", limit: encoded.Len(), wantStatus: http.StatusMultiStatus},
		{name: "one byte short", limit: encoded.Len() - 1, wantStatus: http.StatusInsufficientStorage},
	} {
		t.Run(tt.name, func(t *testing.T) {
			cfg := &config.Config{}
			cfg.DAV.MaxMultistatusResponses = 1
			cfg.DAV.MaxMultistatusBytes = tt.limit
			h := NewDavServer(Options{Config: cfg})
			rr := httptest.NewRecorder()

			h.writeBoundedMultiStatus(rr, payload)

			if rr.Code != tt.wantStatus {
				t.Fatalf("status = %d, want %d", rr.Code, tt.wantStatus)
			}
		})
	}
}

func TestWriteBoundedMultiStatusDefaultResponseBoundary(t *testing.T) {
	h := NewDavServer(Options{})
	responses := make([]response, defaultMaxMultistatusResponses)
	for i := range responses {
		responses[i].Href = "/r"
	}

	rr := httptest.NewRecorder()
	h.writeBoundedMultiStatus(rr, newMultistatus(responses, ""))
	if rr.Code != http.StatusMultiStatus {
		t.Fatalf("10,000 response status = %d, want 207", rr.Code)
	}

	rr = httptest.NewRecorder()
	responses = append(responses, response{Href: "/overflow"})
	h.writeBoundedMultiStatus(rr, newMultistatus(responses, ""))
	if rr.Code != http.StatusInsufficientStorage {
		t.Fatalf("10,001 response status = %d, want 507", rr.Code)
	}
	if strings.Contains(rr.Body.String(), "multistatus") {
		t.Fatalf("hard response limit returned a partial multistatus: %s", rr.Body.String())
	}
}

// The build stops one response past the limit -- far enough to know the set
// overflowed, no further -- and the decoration queries a returnable set would
// need are never issued for one that will not be returned.
func TestCalendarReportStopsBuildingAtOverflow(t *testing.T) {
	cfg := &config.Config{}
	cfg.DAV.MaxMultistatusResponses = 1
	cfg.DAV.MaxMultistatusBytes = defaultMaxMultistatusBytes
	dead := &fakeDeadPropertyRepo{}
	h := NewDavServer(Options{Config: cfg, Store: &store.Store{DeadProperties: dead}})
	events := []store.Event{
		{UID: "one", ResourceName: "one"},
		{UID: "two", ResourceName: "two"},
		{UID: "three", ResourceName: "three"},
	}

	_, err := h.calendarResourceReportResponses(context.Background(), &store.User{ID: 1}, "/dav/calendars/1/", events, propertySelector{Prop: &reportProp{}}, calendarDataProjection{})
	if !errors.Is(err, errNumberOfMatchesExceeded) {
		t.Fatalf("calendarResourceReportResponses() error = %v, want errNumberOfMatchesExceeded", err)
	}
	if dead.listCalls != 0 {
		t.Fatalf("overflow response performed dead-property decoration queries: %d", dead.listCalls)
	}
}

// The overflow is detected inside one keyset page: the query neither reads the
// whole collection to discover it nor falls back to an unbounded list.
func TestCalendarQueryUsesKeysetPageAndStopsAtOverflow(t *testing.T) {
	cfg := &config.Config{}
	cfg.DAV.MaxMultistatusResponses = 1
	cfg.DAV.MaxMultistatusBytes = defaultMaxMultistatusBytes
	eventRepo := &fakeEventRepo{events: make(map[string]*store.Event)}
	for id := int64(1); id <= 600; id++ {
		uid := fmt.Sprintf("event-%d", id)
		eventRepo.events[uid] = &store.Event{
			ID:           id,
			CalendarID:   1,
			UID:          uid,
			ResourceName: uid,
			RawICAL:      "BEGIN:VCALENDAR\r\nBEGIN:VEVENT\r\nUID:" + uid + "\r\nEND:VEVENT\r\nEND:VCALENDAR\r\n",
		}
	}
	h := NewDavServer(Options{Config: cfg, Store: &store.Store{Events: eventRepo}})
	cal := &store.CalendarAccess{Calendar: store.Calendar{ID: 1, UserID: 1}}

	responses, err := h.calendarQuery(context.Background(), &store.User{ID: 1}, cal, "/dav/calendars/1/", "", nil, calendarDataProjection{}, propertySelector{})
	if !errors.Is(err, errNumberOfMatchesExceeded) {
		t.Fatalf("calendarQuery() error = %v, want errNumberOfMatchesExceeded", err)
	}
	if responses != nil {
		t.Fatalf("calendarQuery() returned %d responses beside the postcondition", len(responses))
	}
	if eventRepo.pageLookupCount != 1 {
		t.Fatalf("keyset page queries = %d, want 1", eventRepo.pageLookupCount)
	}
	if eventRepo.listForCalendarCalls != 0 {
		t.Fatalf("unbounded calendar queries = %d, want 0", eventRepo.listForCalendarCalls)
	}
}

func TestCalendarQueryContinuesPagingPastNonmatchingRows(t *testing.T) {
	eventRepo := &fakeEventRepo{events: make(map[string]*store.Event)}
	for id := int64(1); id <= 300; id++ {
		uid := fmt.Sprintf("event-%d", id)
		summary := "ordinary"
		if id == 300 {
			summary = "needle"
		}
		eventRepo.events[uid] = &store.Event{
			ID:           id,
			CalendarID:   1,
			UID:          uid,
			ResourceName: uid,
			RawICAL:      "BEGIN:VCALENDAR\r\nBEGIN:VEVENT\r\nUID:" + uid + "\r\nSUMMARY:" + summary + "\r\nEND:VEVENT\r\nEND:VCALENDAR\r\n",
		}
	}
	h := NewDavServer(Options{Store: &store.Store{Events: eventRepo}})
	cal := &store.CalendarAccess{Calendar: store.Calendar{ID: 1, UserID: 1}}
	filter := &calFilter{CompFilter: compFilter{
		Name: "VCALENDAR",
		CompFilter: []compFilter{{
			Name: "VEVENT",
			PropFilter: []propFilter{{
				Name:      "SUMMARY",
				TextMatch: &textMatch{Text: "needle"},
			}},
		}},
	}}

	responses, err := h.calendarQuery(context.Background(), &store.User{ID: 1}, cal, "/dav/calendars/1/", "", filter, calendarDataProjection{}, propertySelector{})
	if err != nil {
		t.Fatalf("calendarQuery() error = %v", err)
	}
	if len(responses) != 1 || responses[0].Href != "/dav/calendars/1/event-300.ics" {
		t.Fatalf("calendarQuery() responses = %#v, want event-300", responses)
	}
	if eventRepo.pageLookupCount != 2 {
		t.Fatalf("keyset page queries = %d, want 2", eventRepo.pageLookupCount)
	}
	if eventRepo.listForCalendarCalls != 0 {
		t.Fatalf("unbounded calendar queries = %d, want 0", eventRepo.listForCalendarCalls)
	}
}

func TestCalendarPropfindUsesKeysetPageAndStopsAtOverflowSentinel(t *testing.T) {
	cfg := &config.Config{}
	cfg.DAV.MaxMultistatusResponses = 1
	cfg.DAV.MaxMultistatusBytes = defaultMaxMultistatusBytes
	eventRepo := &fakeEventRepo{events: make(map[string]*store.Event)}
	for id := int64(1); id <= 600; id++ {
		uid := fmt.Sprintf("event-%d", id)
		eventRepo.events[uid] = &store.Event{ID: id, CalendarID: 1, UID: uid, ResourceName: uid}
	}
	h := NewDavServer(Options{Config: cfg, Store: &store.Store{Events: eventRepo}})
	cal := &store.CalendarAccess{Calendar: store.Calendar{ID: 1, UserID: 1}}

	responses, err := h.appendCalendarPropfindPages(
		context.Background(),
		&store.User{ID: 1},
		cal,
		"/dav/calendars/1/",
		[]response{{Href: "/dav/calendars/1/"}},
	)
	if err != nil {
		t.Fatalf("appendCalendarPropfindPages() error = %v", err)
	}
	if len(responses) != 2 {
		t.Fatalf("appendCalendarPropfindPages() responses = %d, want max+1 sentinel", len(responses))
	}
	if eventRepo.pageLookupCount != 1 {
		t.Fatalf("keyset page queries = %d, want 1", eventRepo.pageLookupCount)
	}
	if eventRepo.listForCalendarCalls != 0 {
		t.Fatalf("unbounded calendar queries = %d, want 0", eventRepo.listForCalendarCalls)
	}
}

// The candidate-row budget reaches sync-collection too, which lists a whole
// collection on its initial sync. The answer differs from the one
// calendar-query gives: RFC 6578 states no DAV:number-of-matches-within-limits
// postcondition, so the refusal is the RFC 4918 capacity status rather than a
// CalDAV condition borrowed from §7.8.
func TestCalendarSyncCollectionOverTheCandidateRowLimitReturns507(t *testing.T) {
	cfg := &config.Config{}
	cfg.DAV.MaxReportCandidateRows = 100
	h := limitsTestServer(t, cfg, 400)

	rr := limitsReportRequest(t, h, `<?xml version="1.0" encoding="utf-8"?>
<D:sync-collection xmlns:D="DAV:"><D:sync-token/><D:prop><D:getetag/></D:prop></D:sync-collection>`)

	if rr.Code != http.StatusInsufficientStorage {
		t.Fatalf("status = %d, want 507; body: %s", rr.Code, rr.Body.String())
	}
	if strings.Contains(rr.Body.String(), "number-of-matches-within-limits") {
		t.Fatalf("sync-collection answered a postcondition RFC 6578 does not give it: %s", rr.Body.String())
	}
}

func TestCalendarSyncCollectionAtTheCandidateRowLimitIsAnswered(t *testing.T) {
	cfg := &config.Config{}
	cfg.DAV.MaxReportCandidateRows = 256
	h := limitsTestServer(t, cfg, 256)

	rr := limitsReportRequest(t, h, `<?xml version="1.0" encoding="utf-8"?>
<D:sync-collection xmlns:D="DAV:"><D:sync-token/><D:prop><D:getetag/></D:prop></D:sync-collection>`)

	if rr.Code != http.StatusMultiStatus {
		t.Fatalf("status = %d, want 207; body: %s", rr.Code, rr.Body.String())
	}
}

func TestAddressBookSyncCollectionOverTheCandidateRowLimitReturns507(t *testing.T) {
	contacts := map[string]*store.Contact{}
	for i := 1; i <= 400; i++ {
		uid := fmt.Sprintf("contact-%d", i)
		contacts["5:"+uid] = &store.Contact{
			ID: int64(i), AddressBookID: 5, UID: uid, ResourceName: uid, ETag: "e",
			RawVCard: buildVCard("3.0", "UID:"+uid, "FN:"+uid),
		}
	}
	cfg := &config.Config{}
	cfg.DAV.MaxReportCandidateRows = 100
	h := NewDavServer(Options{Config: cfg, Store: &store.Store{
		AddressBooks: &fakeAddressBookRepo{books: map[int64]*store.AddressBook{5: {ID: 5, UserID: 1, Name: "Contacts"}}},
		Contacts:     &fakeContactRepo{contacts: contacts},
	}})

	req := httptest.NewRequest("REPORT", "/dav/addressbooks/5/", strings.NewReader(`<?xml version="1.0" encoding="utf-8"?>
<D:sync-collection xmlns:D="DAV:"><D:sync-token/><D:prop><D:getetag/></D:prop></D:sync-collection>`))
	req.Header.Set("Depth", "1")
	req = req.WithContext(auth.WithUser(req.Context(), &store.User{ID: 1}))
	rr := httptest.NewRecorder()
	h.Report(rr, req)

	if rr.Code != http.StatusInsufficientStorage {
		t.Fatalf("status = %d, want 507; body: %s", rr.Code, rr.Body.String())
	}
}
