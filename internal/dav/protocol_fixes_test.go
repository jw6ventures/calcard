package dav

// Regression tests for the July 2026 dav-package review (FINDINGS.md wave 1):
// conditional-header handling, unknown-report rejection, PROPFIND body/Depth
// conformance, birthday ETag stability, PUT upsert conflicts, COPY/MOVE error
// mapping, 403-vs-404 disclosure policy, and PUT lookup efficiency.

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/jw6ventures/calcard/internal/auth"
	"github.com/jw6ventures/calcard/internal/store"
)

func TestCheckConditionalETagHandling(t *testing.T) {
	tests := []struct {
		name        string
		ifMatch     string
		ifNoneMatch string
		etag        string
		exists      bool
		want        bool
	}{
		{name: "if-match star with existing resource", ifMatch: "*", etag: "abc", exists: true, want: true},
		{name: "if-match star without resource", ifMatch: "*", exists: false, want: false},
		{name: "if-match list with match", ifMatch: `"other", "abc"`, etag: "abc", exists: true, want: true},
		{name: "if-match list without match", ifMatch: `"other", "third"`, etag: "abc", exists: true, want: false},
		{name: "if-match weak validator", ifMatch: `W/"abc"`, etag: "abc", exists: true, want: true},
		{name: "if-none-match star with existing resource", ifNoneMatch: "*", etag: "abc", exists: true, want: false},
		{name: "if-none-match star without resource", ifNoneMatch: "*", exists: false, want: true},
		{name: "if-none-match list with match", ifNoneMatch: `"x", "abc"`, etag: "abc", exists: true, want: false},
		{name: "if-none-match weak validator with match", ifNoneMatch: `W/"abc"`, etag: "abc", exists: true, want: false},
		{name: "if-none-match list without match", ifNoneMatch: `"x"`, etag: "abc", exists: true, want: true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			req := httptest.NewRequest(http.MethodPut, "/dav/calendars/1/x.ics", nil)
			if tc.ifMatch != "" {
				req.Header.Set("If-Match", tc.ifMatch)
			}
			if tc.ifNoneMatch != "" {
				req.Header.Set("If-None-Match", tc.ifNoneMatch)
			}
			if got := checkConditional(req, tc.etag, tc.exists); got != tc.want {
				t.Fatalf("checkConditional(If-Match=%q, If-None-Match=%q, etag=%q, exists=%t) = %t, want %t",
					tc.ifMatch, tc.ifNoneMatch, tc.etag, tc.exists, got, tc.want)
			}
		})
	}
}

func TestPutWithIfMatchStarUpdatesExistingEvent(t *testing.T) {
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
	h := &DavServer{store: &store.Store{Calendars: calRepo, Events: eventRepo}}
	user := &store.User{ID: 1}

	icalData := "BEGIN:VCALENDAR\r\nVERSION:2.0\r\nBEGIN:VEVENT\r\nUID:existing\r\nEND:VEVENT\r\nEND:VCALENDAR\r\n"
	req := newCalendarPutRequest("/dav/calendars/1/existing.ics", strings.NewReader(icalData))
	req.Header.Set("If-Match", "*")
	req = req.WithContext(auth.WithUser(req.Context(), user))
	rr := httptest.NewRecorder()

	h.Put(rr, req)

	if rr.Code != http.StatusNoContent {
		t.Fatalf("PUT with If-Match: * against existing resource = %d, want 204: %s", rr.Code, rr.Body.String())
	}
}

func TestReportUnknownCalendarReportReturns403(t *testing.T) {
	calRepo := &fakeCalendarRepo{
		accessible: []store.CalendarAccess{
			{Calendar: store.Calendar{ID: 1, UserID: 1, Name: "Test"}, Editor: true},
		},
	}
	eventRepo := &fakeEventRepo{
		events: map[string]*store.Event{
			"1:secret": {CalendarID: 1, UID: "secret", RawICAL: "BEGIN:VCALENDAR\r\nBEGIN:VEVENT\r\nUID:secret\r\nEND:VEVENT\r\nEND:VCALENDAR\r\n", ETag: "e"},
		},
	}
	h := &DavServer{store: &store.Store{Calendars: calRepo, Events: eventRepo}}
	user := &store.User{ID: 1}

	body := `<?xml version="1.0"?><x:bogus-report xmlns:x="urn:example:bogus"/>`
	req := httptest.NewRequest("REPORT", "/dav/calendars/1/", strings.NewReader(body))
	req.Header.Set("Depth", "1")
	req = req.WithContext(auth.WithUser(req.Context(), user))
	rr := httptest.NewRecorder()

	h.Report(rr, req)

	if rr.Code != http.StatusForbidden {
		t.Fatalf("unknown calendar REPORT = %d, want 403: %s", rr.Code, rr.Body.String())
	}
	if !strings.Contains(rr.Body.String(), "supported-report") {
		t.Fatalf("unknown calendar REPORT body missing supported-report precondition: %s", rr.Body.String())
	}
	if strings.Contains(rr.Body.String(), "secret") {
		t.Fatalf("unknown calendar REPORT leaked event data: %s", rr.Body.String())
	}
}

func TestReportUnknownAddressBookReportReturns403(t *testing.T) {
	bookRepo := &fakeAddressBookRepo{books: map[int64]*store.AddressBook{
		2: {ID: 2, UserID: 1, Name: "Book"},
	}}
	contactRepo := &fakeContactRepo{
		contacts: map[string]*store.Contact{
			"2:secret": {AddressBookID: 2, UID: "secret", RawVCard: "BEGIN:VCARD\r\nVERSION:3.0\r\nUID:secret\r\nFN:Secret\r\nEND:VCARD\r\n", ETag: "e"},
		},
	}
	h := &DavServer{store: &store.Store{AddressBooks: bookRepo, Contacts: contactRepo}}
	user := &store.User{ID: 1}

	body := `<?xml version="1.0"?><x:bogus-report xmlns:x="urn:example:bogus"/>`
	req := httptest.NewRequest("REPORT", "/dav/addressbooks/2/", strings.NewReader(body))
	req.Header.Set("Depth", "1")
	req = req.WithContext(auth.WithUser(req.Context(), user))
	rr := httptest.NewRecorder()

	h.Report(rr, req)

	if rr.Code != http.StatusForbidden {
		t.Fatalf("unknown address-book REPORT = %d, want 403: %s", rr.Code, rr.Body.String())
	}
	if !strings.Contains(rr.Body.String(), "supported-report") {
		t.Fatalf("unknown address-book REPORT body missing supported-report precondition: %s", rr.Body.String())
	}
	if strings.Contains(rr.Body.String(), "secret") {
		t.Fatalf("unknown address-book REPORT leaked contact data: %s", rr.Body.String())
	}
}

func TestReportUnknownBirthdayReportReturns403(t *testing.T) {
	contactRepo := &fakeContactRepo{contacts: map[string]*store.Contact{}}
	h := &DavServer{store: &store.Store{Contacts: contactRepo}}
	user := &store.User{ID: 1}

	body := `<?xml version="1.0"?><x:bogus-report xmlns:x="urn:example:bogus"/>`
	req := httptest.NewRequest("REPORT", "/dav/calendars/-1/", strings.NewReader(body))
	req.Header.Set("Depth", "1")
	req = req.WithContext(auth.WithUser(req.Context(), user))
	rr := httptest.NewRecorder()

	h.Report(rr, req)

	if rr.Code != http.StatusForbidden {
		t.Fatalf("unknown birthday REPORT = %d, want 403: %s", rr.Code, rr.Body.String())
	}
	if !strings.Contains(rr.Body.String(), "supported-report") {
		t.Fatalf("unknown birthday REPORT body missing supported-report precondition: %s", rr.Body.String())
	}
}

func TestPropfindMalformedXMLReturns400(t *testing.T) {
	h := &DavServer{}
	req := httptest.NewRequest("PROPFIND", "/dav/", strings.NewReader("<not-xml"))
	req.Header.Set("Depth", "0")
	req = req.WithContext(auth.WithUser(req.Context(), &store.User{ID: 1}))
	rr := httptest.NewRecorder()

	h.Propfind(rr, req)

	if rr.Code != http.StatusBadRequest {
		t.Fatalf("PROPFIND with malformed XML = %d, want 400: %s", rr.Code, rr.Body.String())
	}
}

func TestPropfindDepthHandling(t *testing.T) {
	tests := []struct {
		name     string
		depth    string
		setDepth bool
		want     int
	}{
		{name: "explicit infinity refused", depth: "infinity", setDepth: true, want: http.StatusForbidden},
		{name: "missing depth means infinity", setDepth: false, want: http.StatusForbidden},
		{name: "invalid depth", depth: "2", setDepth: true, want: http.StatusBadRequest},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			h := &DavServer{}
			req := httptest.NewRequest("PROPFIND", "/dav/", nil)
			if tc.setDepth {
				req.Header.Set("Depth", tc.depth)
			}
			req = req.WithContext(auth.WithUser(req.Context(), &store.User{ID: 1}))
			rr := httptest.NewRecorder()

			h.Propfind(rr, req)

			if rr.Code != tc.want {
				t.Fatalf("PROPFIND depth=%q = %d, want %d: %s", tc.depth, rr.Code, tc.want, rr.Body.String())
			}
			if tc.want == http.StatusForbidden && !strings.Contains(rr.Body.String(), "propfind-finite-depth") {
				t.Fatalf("PROPFIND infinity refusal missing propfind-finite-depth precondition: %s", rr.Body.String())
			}
		})
	}
}

func TestBirthdayEventsUseStableDTStampAndSummary(t *testing.T) {
	lastMod := time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)
	name := "John Doe"
	birthday := time.Date(1990, 5, 15, 0, 0, 0, 0, time.UTC)
	contactRepo := &fakeContactRepo{
		contacts: map[string]*store.Contact{
			"1:c1": {AddressBookID: 1, UID: "c1", DisplayName: &name, Birthday: &birthday, LastModified: lastMod},
		},
	}
	h := &DavServer{store: &store.Store{Contacts: contactRepo}}

	events, err := h.generateBirthdayEvents(context.Background(), 1)
	if err != nil {
		t.Fatalf("generateBirthdayEvents returned error: %v", err)
	}
	if len(events) != 1 {
		t.Fatalf("expected 1 event, got %d", len(events))
	}
	if !strings.Contains(events[0].RawICAL, "DTSTAMP:20260102T030405Z") {
		t.Errorf("DTSTAMP must derive from the contact's LastModified, got: %s", events[0].RawICAL)
	}
	if strings.Contains(events[0].RawICAL, "turning") {
		t.Errorf("summary must not bake an age into a yearly recurring event, got: %s", events[0].RawICAL)
	}

	again, err := h.generateBirthdayEvents(context.Background(), 1)
	if err != nil {
		t.Fatalf("generateBirthdayEvents returned error: %v", err)
	}
	if again[0].ETag != events[0].ETag {
		t.Errorf("birthday event ETag must be stable across requests: %q != %q", again[0].ETag, events[0].ETag)
	}
}

func TestPutEventUpsertConflictReturnsNoUIDConflict(t *testing.T) {
	calRepo := &fakeCalendarRepo{
		accessible: []store.CalendarAccess{
			{Calendar: store.Calendar{ID: 1, UserID: 1, Name: "Test"}, Editor: true},
		},
	}
	eventRepo := &fakeEventRepo{upsertErr: store.ErrConflict}
	h := &DavServer{store: &store.Store{Calendars: calRepo, Events: eventRepo}}
	user := &store.User{ID: 1}

	icalData := "BEGIN:VCALENDAR\r\nVERSION:2.0\r\nBEGIN:VEVENT\r\nUID:new-event\r\nEND:VEVENT\r\nEND:VCALENDAR\r\n"
	req := newCalendarPutRequest("/dav/calendars/1/new-event.ics", strings.NewReader(icalData))
	req = req.WithContext(auth.WithUser(req.Context(), user))
	rr := httptest.NewRecorder()

	h.Put(rr, req)

	if rr.Code != http.StatusConflict {
		t.Fatalf("PUT with store conflict = %d, want 409: %s", rr.Code, rr.Body.String())
	}
	if !strings.Contains(rr.Body.String(), "no-uid-conflict") {
		t.Fatalf("PUT conflict body missing no-uid-conflict precondition: %s", rr.Body.String())
	}
}

func TestCopyMoveAmbiguousSourceCalendarReturns409(t *testing.T) {
	calRepo := &fakeCalendarRepo{
		accessible: []store.CalendarAccess{
			{Calendar: store.Calendar{ID: 1, UserID: 1, Name: "Dup"}, Editor: true},
			{Calendar: store.Calendar{ID: 2, UserID: 1, Name: "Dup"}, Editor: true},
		},
	}
	h := &DavServer{store: &store.Store{Calendars: calRepo, Events: &fakeEventRepo{}}}
	user := &store.User{ID: 1}

	for _, method := range []string{"COPY", "MOVE"} {
		t.Run(method, func(t *testing.T) {
			req := httptest.NewRequest(method, "/dav/calendars/Dup/event.ics", nil)
			req.Header.Set("Destination", "/dav/calendars/1/copy.ics")
			req = req.WithContext(auth.WithUser(req.Context(), user))
			rr := httptest.NewRecorder()

			if method == "COPY" {
				h.Copy(rr, req)
			} else {
				h.Move(rr, req)
			}

			if rr.Code != http.StatusConflict {
				t.Fatalf("%s with ambiguous source calendar = %d, want 409: %s", method, rr.Code, rr.Body.String())
			}
		})
	}
}

func TestDeleteEventWithoutUnbindReturns403WithConsistentBody(t *testing.T) {
	calRepo := &fakeCalendarRepo{
		accessible: []store.CalendarAccess{
			{Calendar: store.Calendar{ID: 1, UserID: 2, Name: "Shared"}, Shared: true, Editor: false},
		},
	}
	eventRepo := &fakeEventRepo{
		events: map[string]*store.Event{
			"1:existing": {CalendarID: 1, UID: "existing", RawICAL: "DATA", ETag: "e"},
		},
	}
	aclRepo := &fakeACLRepo{
		entries: []store.ACLEntry{
			{ResourcePath: "/dav/calendars/1/existing", PrincipalHref: "/dav/principals/1/", IsGrant: false, Privilege: "unbind"},
		},
	}
	h := &DavServer{store: &store.Store{Calendars: calRepo, Events: eventRepo, ACLEntries: aclRepo}}
	user := &store.User{ID: 1}

	req := httptest.NewRequest(http.MethodDelete, "/dav/calendars/1/existing.ics", nil)
	req = req.WithContext(auth.WithUser(req.Context(), user))
	rr := httptest.NewRecorder()

	h.Delete(rr, req)

	if rr.Code != http.StatusForbidden {
		t.Fatalf("DELETE without unbind = %d, want 403: %s", rr.Code, rr.Body.String())
	}
	if strings.Contains(strings.ToLower(rr.Body.String()), "not found") {
		t.Fatalf("403 response must not carry a not-found body: %s", rr.Body.String())
	}
}

func TestGetUnknownDavPathReturns404(t *testing.T) {
	h := &DavServer{store: &store.Store{}}
	user := &store.User{ID: 1}

	req := httptest.NewRequest(http.MethodGet, "/dav/unknown-collection", nil)
	req = req.WithContext(auth.WithUser(req.Context(), user))
	rr := httptest.NewRecorder()

	h.Get(rr, req)

	if rr.Code != http.StatusNotFound {
		t.Fatalf("GET on unknown /dav path = %d, want 404: %s", rr.Code, rr.Body.String())
	}
}

func TestPutEventFetchesResourceOnce(t *testing.T) {
	calRepo := &fakeCalendarRepo{
		accessible: []store.CalendarAccess{
			{Calendar: store.Calendar{ID: 1, UserID: 1, Name: "Test"}, Editor: true},
		},
	}
	eventRepo := &fakeEventRepo{events: map[string]*store.Event{}}
	h := &DavServer{store: &store.Store{Calendars: calRepo, Events: eventRepo}}
	user := &store.User{ID: 1}

	icalData := "BEGIN:VCALENDAR\r\nVERSION:2.0\r\nBEGIN:VEVENT\r\nUID:new-event\r\nEND:VEVENT\r\nEND:VCALENDAR\r\n"
	req := newCalendarPutRequest("/dav/calendars/1/new-event.ics", strings.NewReader(icalData))
	req = req.WithContext(auth.WithUser(req.Context(), user))
	rr := httptest.NewRecorder()

	h.Put(rr, req)

	if rr.Code != http.StatusCreated {
		t.Fatalf("PUT = %d, want 201: %s", rr.Code, rr.Body.String())
	}
	if eventRepo.resourceLookupCount != 1 {
		t.Fatalf("PUT performed %d GetByResourceName lookups, want 1", eventRepo.resourceLookupCount)
	}
}
