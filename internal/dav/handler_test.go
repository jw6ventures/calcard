package dav

import (
	"context"
	"encoding/xml"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"gitea.jw6.us/james/calcard/internal/auth"
	"gitea.jw6.us/james/calcard/internal/config"
	"gitea.jw6.us/james/calcard/internal/store"
)

func newCalendarPutRequest(path string, body io.Reader) *http.Request {
	req := httptest.NewRequest(http.MethodPut, path, body)
	req.Header.Set("Content-Type", "text/calendar")
	return req
}

func TestBirthdayCalendarGeneration(t *testing.T) {
	now := time.Now()
	birthday1 := time.Date(1990, 5, 15, 0, 0, 0, 0, time.UTC)
	birthday2 := time.Date(2000, 12, 25, 0, 0, 0, 0, time.UTC)
	birthdayNoYear := time.Date(1, 3, 10, 0, 0, 0, 0, time.UTC)

	name1 := "John Doe"
	name2 := "Jane Smith"
	name3 := "Bob Johnson"

	contactRepo := &fakeContactRepo{
		contacts: map[string]*store.Contact{
			"1:contact1": {
				AddressBookID: 1,
				UID:           "contact1",
				DisplayName:   &name1,
				Birthday:      &birthday1,
				LastModified:  now,
			},
			"1:contact2": {
				AddressBookID: 1,
				UID:           "contact2",
				DisplayName:   &name2,
				Birthday:      &birthday2,
				LastModified:  now,
			},
			"1:contact3": {
				AddressBookID: 1,
				UID:           "contact3",
				DisplayName:   &name3,
				Birthday:      &birthdayNoYear,
				LastModified:  now,
			},
			"1:contact4": {
				AddressBookID: 1,
				UID:           "contact4",
				DisplayName:   &name1,
				Birthday:      nil,
			},
		},
	}
	h := &Handler{
		store: &store.Store{Contacts: contactRepo},
	}

	events, err := h.generateBirthdayEvents(context.Background(), 1)
	if err != nil {
		t.Fatalf("generateBirthdayEvents returned error: %v", err)
	}

	if len(events) != 3 {
		t.Fatalf("expected 3 events, got %d", len(events))
	}

	// Check that UIDs are properly formatted
	for _, ev := range events {
		if !strings.HasPrefix(ev.UID, "birthday-") {
			t.Errorf("expected birthday event UID to start with 'birthday-', got %q", ev.UID)
		}
		if !strings.Contains(ev.RawICAL, "RRULE:FREQ=YEARLY") {
			t.Errorf("expected birthday event to have yearly recurrence")
		}
		if !strings.Contains(ev.RawICAL, "TRANSP:TRANSPARENT") {
			t.Errorf("expected birthday event to be transparent (free time)")
		}
	}
}

func TestBirthdayCalendarReadOnly(t *testing.T) {
	cfg := &config.Config{}
	h := NewHandler(cfg, &store.Store{})

	user := &store.User{ID: 1, PrimaryEmail: "test@example.com"}

	// Test PUT is blocked
	putBody := "BEGIN:VCALENDAR\r\nVERSION:2.0\r\nBEGIN:VEVENT\r\nUID:test\r\nSUMMARY:Test\r\nEND:VEVENT\r\nEND:VCALENDAR\r\n"
	putReq := newCalendarPutRequest("/dav/calendars/-1/test.ics", strings.NewReader(putBody))
	putReq = putReq.WithContext(auth.WithUser(context.Background(), user))
	putRec := httptest.NewRecorder()
	h.Put(putRec, putReq)

	if putRec.Code != http.StatusForbidden {
		t.Errorf("expected PUT to birthday calendar to return 403, got %d", putRec.Code)
	}
	if !strings.Contains(putRec.Body.String(), "read-only") {
		t.Errorf("expected error message to mention read-only, got %q", putRec.Body.String())
	}

	// Test DELETE is blocked
	delReq := httptest.NewRequest("DELETE", "/dav/calendars/-1/test.ics", nil)
	delReq = delReq.WithContext(auth.WithUser(context.Background(), user))
	delRec := httptest.NewRecorder()
	h.Delete(delRec, delReq)

	if delRec.Code != http.StatusForbidden {
		t.Errorf("expected DELETE from birthday calendar to return 403, got %d", delRec.Code)
	}
	if !strings.Contains(delRec.Body.String(), "read-only") {
		t.Errorf("expected error message to mention read-only, got %q", delRec.Body.String())
	}
}

func TestParseResourcePathAcceptsAbsoluteHref(t *testing.T) {
	id, uid, ok := parseResourcePath("https://cal.example.com/dav/calendars/42/test-event.ics", "/dav/calendars")
	if !ok {
		t.Fatalf("expected absolute href to be parsed")
	}
	if id != 42 {
		t.Fatalf("expected id 42, got %d", id)
	}
	if uid != "test-event" {
		t.Fatalf("expected uid test-event, got %q", uid)
	}
}

func TestCalendarMultiGetHandlesAbsoluteHref(t *testing.T) {
	repo := &fakeEventRepo{
		events: map[string]*store.Event{
			"2:test-event": {
				CalendarID: 2,
				UID:        "test-event",
				RawICAL:    "BEGIN:VCALENDAR\r\nBEGIN:VEVENT\r\nUID:test-event\r\nEND:VEVENT\r\nEND:VCALENDAR\r\n",
				ETag:       "abc123",
			},
		},
	}
	h := &Handler{store: &store.Store{Events: repo, DeletedResources: &fakeDeletedResourceRepo{}}}

	hrefs := []string{"https://cal.example.com/dav/calendars/2/test-event.ics"}
	cal := &store.CalendarAccess{Calendar: store.Calendar{ID: 2}}
	responses, err := h.calendarMultiGet(context.Background(), cal, hrefs, "/dav/calendars/2/", "/dav/calendars/2/", nil)
	if err != nil {
		t.Fatalf("calendarMultiGet returned error: %v", err)
	}
	if len(responses) != 1 {
		t.Fatalf("expected 1 response, got %d", len(responses))
	}
	if responses[0].Href != "/dav/calendars/2/test-event.ics" {
		t.Fatalf("unexpected href %q", responses[0].Href)
	}
}

func TestCalendarMultiGetHandlesRelativeHref(t *testing.T) {
	repo := &fakeEventRepo{
		events: map[string]*store.Event{
			"2:test-event": {
				CalendarID: 2,
				UID:        "test-event",
				RawICAL:    "BEGIN:VCALENDAR\r\nBEGIN:VEVENT\r\nUID:test-event\r\nEND:VEVENT\r\nEND:VCALENDAR\r\n",
				ETag:       "abc123",
			},
		},
	}
	h := &Handler{store: &store.Store{Events: repo, DeletedResources: &fakeDeletedResourceRepo{}}}

	hrefs := []string{"test-event.ics"}
	cal := &store.CalendarAccess{Calendar: store.Calendar{ID: 2}}
	responses, err := h.calendarMultiGet(context.Background(), cal, hrefs, "/dav/calendars/2/", "/dav/calendars/2/", nil)
	if err != nil {
		t.Fatalf("calendarMultiGet returned error: %v", err)
	}
	if len(responses) != 1 {
		t.Fatalf("expected 1 response, got %d", len(responses))
	}
	if responses[0].Href != "/dav/calendars/2/test-event.ics" {
		t.Fatalf("unexpected href %q", responses[0].Href)
	}
}

func TestParseResourcePathRejectsInvalid(t *testing.T) {
	tests := []string{
		"",
		"/dav/calendars/not-a-number/test-event.ics",
		"/dav/calendars/2/",
		"/dav/calendars//missing.ics",
		"/dav/addressbooks/2/",
	}
	for _, raw := range tests {
		if _, _, ok := parseResourcePath(raw, "/dav/calendars"); ok {
			t.Fatalf("expected %q to be rejected", raw)
		}
	}
}

func TestCalendarReportSyncCollectionReturnsToken(t *testing.T) {
	now := store.Now()
	repo := &fakeEventRepo{
		events: map[string]*store.Event{
			"2:test-event": {
				CalendarID:   2,
				UID:          "test-event",
				RawICAL:      "BEGIN:VCALENDAR\r\nBEGIN:VEVENT\r\nUID:test-event\r\nEND:VEVENT\r\nEND:VCALENDAR\r\n",
				ETag:         "abc123",
				LastModified: now,
			},
		},
	}
	h := &Handler{store: &store.Store{Events: repo, DeletedResources: &fakeDeletedResourceRepo{}}}

	report := reportRequest{XMLName: xml.Name{Local: "sync-collection"}}
	cal := &store.CalendarAccess{Calendar: store.Calendar{ID: 2, Name: "Test", CTag: 1, UpdatedAt: now}, Editor: true}
	responses, token, err := h.calendarReportResponses(context.Background(), cal, "/dav/principals/1/", "/dav/calendars/2/", "/dav/calendars/2/", report)
	if err != nil {
		t.Fatalf("calendarReportResponses returned error: %v", err)
	}
	if token == "" {
		t.Fatalf("expected sync token, got empty string")
	}
	if len(responses) != 2 {
		t.Fatalf("expected 2 responses, got %d", len(responses))
	}
	if responses[0].Href != "/dav/calendars/2/" {
		t.Fatalf("expected collection response first, got %s", responses[0].Href)
	}
}

func TestCalendarSyncCollectionIncludesDeletedResources(t *testing.T) {
	now := store.Now()
	repo := &fakeEventRepo{
		events: map[string]*store.Event{},
	}
	deletedRepo := &fakeDeletedResourceRepo{
		deleted: []store.DeletedResource{
			{ID: 1, ResourceType: "event", CollectionID: 2, UID: "deleted-uid", ResourceName: "deleted-resource", DeletedAt: now},
		},
	}
	h := &Handler{store: &store.Store{Events: repo, DeletedResources: deletedRepo}}

	report := reportRequest{
		XMLName:   xml.Name{Local: "sync-collection"},
		SyncToken: buildSyncToken("cal", 2, now.Add(-time.Hour)),
	}
	cal := &store.CalendarAccess{Calendar: store.Calendar{ID: 2, Name: "Test", CTag: 2, UpdatedAt: now}, Editor: true}
	responses, _, err := h.calendarReportResponses(context.Background(), cal, "/dav/principals/1/", "/dav/calendars/2/", "/dav/calendars/2/", report)
	if err != nil {
		t.Fatalf("calendarReportResponses returned error: %v", err)
	}
	// Should have collection response + deleted event
	if len(responses) != 2 {
		t.Fatalf("expected 2 responses, got %d", len(responses))
	}
	// Check that deleted event has 404 status
	found := false
	for _, r := range responses {
		if strings.Contains(r.Href, "deleted-resource") {
			if r.Status != "HTTP/1.1 404 Not Found" {
				t.Errorf("expected 404 status for deleted event, got %q", r.Status)
			}
			found = true
		}
	}
	if !found {
		t.Error("deleted event response not found")
	}
}

func TestCalendarSyncCollectionRejectsInvalidToken(t *testing.T) {
	now := store.Now()
	repo := &fakeEventRepo{events: map[string]*store.Event{}}
	h := &Handler{store: &store.Store{Events: repo, DeletedResources: &fakeDeletedResourceRepo{}}}
	report := reportRequest{
		XMLName:   xml.Name{Local: "sync-collection"},
		SyncToken: buildSyncToken("card", 2, now), // wrong kind for calendar
	}
	cal := &store.CalendarAccess{Calendar: store.Calendar{ID: 2, Name: "Test", CTag: 2, UpdatedAt: now}, Editor: true}
	_, _, err := h.calendarReportResponses(context.Background(), cal, "/dav/principals/1/", "/dav/calendars/2/", "/dav/calendars/2/", report)
	if !errors.Is(err, errInvalidSyncToken) {
		t.Fatalf("expected errInvalidSyncToken, got %v", err)
	}
}

func TestResolveDAVHrefHandlesRelativeAbsoluteAndURL(t *testing.T) {
	base := "/dav/calendars/2/"
	cases := map[string]string{
		"event.ics":                                    "/dav/calendars/2/event.ics",
		"/dav/calendars/2/absolute.ics":                "/dav/calendars/2/absolute.ics",
		"https://example.com/dav/calendars/2/full.ics": "/dav/calendars/2/full.ics",
		"http://example.com/dav/calendars/2/full.ics":  "/dav/calendars/2/full.ics",
	}
	for raw, want := range cases {
		if got := resolveDAVHref(base, raw); got != want {
			t.Fatalf("resolveDAVHref(%q) = %q, want %q", raw, got, want)
		}
	}
}

func TestParseSyncTokenRoundTrip(t *testing.T) {
	ts := time.Date(2024, 6, 1, 12, 0, 0, 123, time.UTC)
	token := buildSyncToken("cal", 7, ts)
	info, err := parseSyncToken(token)
	if err != nil {
		t.Fatalf("parseSyncToken returned error: %v", err)
	}
	if info.Kind != "cal" || info.ID != 7 {
		t.Fatalf("unexpected info: %+v", info)
	}
	if !info.Timestamp.Equal(ts) {
		t.Fatalf("timestamp mismatch, got %v want %v", info.Timestamp, ts)
	}
}

func TestParseSyncTokenRejectsGarbage(t *testing.T) {
	if _, err := parseSyncToken("not-a-token"); !errors.Is(err, errInvalidSyncToken) {
		t.Fatalf("expected errInvalidSyncToken, got %v", err)
	}
}

func TestOptionsAdvertisesDAVCapabilities(t *testing.T) {
	h := &Handler{}
	rr := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodOptions, "/dav", nil)

	h.Options(rr, req)

	res := rr.Result()
	if res.StatusCode != http.StatusNoContent {
		t.Fatalf("unexpected status %d", res.StatusCode)
	}
	if allow := res.Header.Get("Allow"); !strings.Contains(allow, "PROPFIND") || !strings.Contains(allow, "REPORT") {
		t.Fatalf("Allow header missing DAV verbs: %q", allow)
	}
	if dav := res.Header.Get("DAV"); dav != "1, 2, calendar-access, addressbook" {
		t.Fatalf("DAV header = %q", dav)
	}
	if patch := res.Header.Get("Accept-Patch"); patch != "application/xml" {
		t.Fatalf("Accept-Patch header = %q", patch)
	}
}

func TestPropfindRootIncludesPrincipalAndHomes(t *testing.T) {
	h := &Handler{}
	u := &store.User{ID: 1, PrimaryEmail: "user@example.com"}

	req := httptest.NewRequest("PROPFIND", "/dav", nil)
	req = req.WithContext(auth.WithUser(req.Context(), u))
	req.Header.Set("Depth", "1")
	rr := httptest.NewRecorder()

	h.Propfind(rr, req)

	res := rr.Result()
	if res.StatusCode != http.StatusMultiStatus {
		t.Fatalf("expected 207, got %d", res.StatusCode)
	}
	body := rr.Body.Bytes()
	if responses := strings.Count(string(body), "<d:response>"); responses != 4 {
		t.Fatalf("expected 4 responses (root, calendars, addressbooks, principal), got %d", responses)
	}
	if !strings.Contains(string(body), "<d:href>/dav/principals/1/</d:href>") {
		t.Fatal("principal response not found")
	}
	if !strings.Contains(string(body), "<cal:calendar-home-set>") || !strings.Contains(string(body), "<card:addressbook-home-set>") {
		t.Fatal("principal response missing home sets")
	}
}

func TestPropfindCalendarCollectionIncludesReportsAndSync(t *testing.T) {
	now := store.Now()
	calRepo := &fakeCalendarRepo{
		accessible: []store.CalendarAccess{
			{Calendar: store.Calendar{ID: 2, UserID: 1, Name: "Work", CTag: 5, UpdatedAt: now}, Editor: true},
		},
	}
	eventRepo := &fakeEventRepo{
		events: map[string]*store.Event{
			"2:event": {CalendarID: 2, UID: "event", RawICAL: "ICAL", ETag: "etag", LastModified: now},
		},
	}
	h := &Handler{store: &store.Store{Calendars: calRepo, Events: eventRepo}}
	u := &store.User{ID: 1}

	req := httptest.NewRequest("PROPFIND", "/dav/calendars/2/", nil)
	req.Header.Set("Depth", "1")
	req = req.WithContext(auth.WithUser(req.Context(), u))
	rr := httptest.NewRecorder()

	h.Propfind(rr, req)

	res := rr.Result()
	if res.StatusCode != http.StatusMultiStatus {
		t.Fatalf("expected 207, got %d", res.StatusCode)
	}
	body := rr.Body.Bytes()
	if strings.Count(string(body), "<d:response>") != 2 {
		t.Fatalf("expected collection + 1 event, got body: %s", string(body))
	}
	if !strings.Contains(string(body), "<d:sync-token>") || !strings.Contains(string(body), "<cs:getctag>5</cs:getctag>") {
		t.Fatalf("missing sync metadata: %s", string(body))
	}
	if !strings.Contains(string(body), "<d:supported-report-set>") {
		t.Fatalf("supported-report-set missing: %s", string(body))
	}
	if !strings.Contains(string(body), "<d:getcontenttype>text/calendar; charset=utf-8</d:getcontenttype>") {
		t.Fatalf("unexpected content type: %s", string(body))
	}
}

func TestPropfindAddressBookCollectionIncludesReportsAndSync(t *testing.T) {
	now := store.Now()
	bookRepo := &fakeAddressBookRepo{
		books: map[int64]*store.AddressBook{
			3: {ID: 3, UserID: 1, Name: "Contacts", CTag: 9, UpdatedAt: now},
		},
	}
	contactRepo := &fakeContactRepo{
		contacts: map[string]*store.Contact{
			"3:alice": {AddressBookID: 3, UID: "alice", RawVCard: "VCARD", ETag: "e1", LastModified: now},
		},
	}
	h := &Handler{store: &store.Store{AddressBooks: bookRepo, Contacts: contactRepo}}
	u := &store.User{ID: 1}

	req := httptest.NewRequest("PROPFIND", "/dav/addressbooks/3/", nil)
	req.Header.Set("Depth", "1")
	req = req.WithContext(auth.WithUser(req.Context(), u))
	rr := httptest.NewRecorder()

	h.Propfind(rr, req)

	res := rr.Result()
	if res.StatusCode != http.StatusMultiStatus {
		t.Fatalf("expected 207, got %d", res.StatusCode)
	}
	body := rr.Body.Bytes()
	if strings.Count(string(body), "<d:response>") != 2 {
		t.Fatalf("expected collection + 1 contact, got body: %s", string(body))
	}
	if !strings.Contains(string(body), "<d:sync-token>") || !strings.Contains(string(body), "<cs:getctag>9</cs:getctag>") {
		t.Fatalf("missing sync metadata: %s", string(body))
	}
	if !strings.Contains(string(body), "<d:supported-report-set>") {
		t.Fatal("supported-report-set missing")
	}
	if !strings.Contains(string(body), "<d:getcontenttype>text/vcard; charset=utf-8</d:getcontenttype>") {
		t.Fatalf("unexpected content type: %s", string(body))
	}
}

func TestPropfindCalendarDepth0DoesNotListEvents(t *testing.T) {
	now := store.Now()
	calRepo := &fakeCalendarRepo{
		accessible: []store.CalendarAccess{
			{Calendar: store.Calendar{ID: 2, UserID: 1, Name: "Work", CTag: 1, UpdatedAt: now}, Editor: true},
		},
	}
	eventRepo := &fakeEventRepo{
		events: map[string]*store.Event{
			"2:event": {CalendarID: 2, UID: "event", RawICAL: "ICAL", ETag: "etag", LastModified: now},
		},
	}
	h := &Handler{store: &store.Store{Calendars: calRepo, Events: eventRepo}}
	u := &store.User{ID: 1}

	req := httptest.NewRequest("PROPFIND", "/dav/calendars/2/", nil)
	req.Header.Set("Depth", "0")
	req = req.WithContext(auth.WithUser(req.Context(), u))
	rr := httptest.NewRecorder()

	h.Propfind(rr, req)

	if rr.Code != http.StatusMultiStatus {
		t.Fatalf("expected 207, got %d", rr.Code)
	}
	body := rr.Body.String()
	if strings.Count(body, "<d:response>") != 1 {
		t.Fatalf("expected only collection response at depth 0, got %s", body)
	}
	if strings.Contains(body, ".ics") {
		t.Fatalf("expected no event resources at depth 0, got %s", body)
	}
}

func TestPropfindCalendarResourceReturnsProps(t *testing.T) {
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
				RawICAL:      "BEGIN:VCALENDAR\r\nBEGIN:VEVENT\r\nUID:event\r\nEND:VEVENT\r\nEND:VCALENDAR\r\n",
				ETag:         "etag1",
			},
		},
	}
	h := &Handler{store: &store.Store{Calendars: calRepo, Events: eventRepo}}
	user := &store.User{ID: 1}

	req := httptest.NewRequest("PROPFIND", "/dav/calendars/1/event.ics", nil)
	req.Header.Set("Depth", "0")
	req = req.WithContext(auth.WithUser(req.Context(), user))
	rr := httptest.NewRecorder()

	h.Propfind(rr, req)

	if rr.Code != http.StatusMultiStatus {
		t.Fatalf("expected 207, got %d: %s", rr.Code, rr.Body.String())
	}
	resp := rr.Body.String()
	if !strings.Contains(resp, "getetag") {
		t.Fatalf("expected getetag in response, got %s", resp)
	}
	if !strings.Contains(resp, "BEGIN:VEVENT") {
		t.Fatalf("expected calendar data in response, got %s", resp)
	}
}

func TestPropfindPrincipalsDepth0OmitsUserPrincipal(t *testing.T) {
	h := &Handler{}
	u := &store.User{ID: 1, PrimaryEmail: "user@example.com"}

	req := httptest.NewRequest("PROPFIND", "/dav/principals", nil)
	req.Header.Set("Depth", "0")
	req = req.WithContext(auth.WithUser(req.Context(), u))
	rr := httptest.NewRecorder()

	h.Propfind(rr, req)

	if rr.Code != http.StatusMultiStatus {
		t.Fatalf("expected 207, got %d", rr.Code)
	}
	body := rr.Body.String()
	if strings.Count(body, "<d:response>") != 1 {
		t.Fatalf("expected only collection response, got %s", body)
	}
	if strings.Contains(body, "/dav/principals/1/") {
		t.Fatalf("depth 0 should not include principal resource: %s", body)
	}
}

func TestPropfindCalendarsRootListsCollections(t *testing.T) {
	calRepo := &fakeCalendarRepo{
		accessible: []store.CalendarAccess{
			{Calendar: store.Calendar{ID: 1, UserID: 1, Name: "One"}},
			{Calendar: store.Calendar{ID: 2, UserID: 1, Name: "Two"}},
		},
	}
	h := &Handler{store: &store.Store{Calendars: calRepo}}
	u := &store.User{ID: 1}
	req := httptest.NewRequest("PROPFIND", "/dav/calendars", nil)
	req.Header.Set("Depth", "1")
	req = req.WithContext(auth.WithUser(req.Context(), u))
	rr := httptest.NewRecorder()

	h.Propfind(rr, req)

	if rr.Code != http.StatusMultiStatus {
		t.Fatalf("expected 207, got %d", rr.Code)
	}
	body := rr.Body.String()
	if strings.Count(body, "<d:response>") != 4 { // collection + birthday calendar + two calendars
		t.Fatalf("expected calendar collection listings, got %s", body)
	}
}

func TestPropfindAddressBooksRootListsCollections(t *testing.T) {
	bookRepo := &fakeAddressBookRepo{
		books: map[int64]*store.AddressBook{
			1: {ID: 1, UserID: 1, Name: "Personal"},
			2: {ID: 2, UserID: 1, Name: "Shared"},
		},
	}
	h := &Handler{store: &store.Store{AddressBooks: bookRepo}}
	u := &store.User{ID: 1}
	req := httptest.NewRequest("PROPFIND", "/dav/addressbooks", nil)
	req.Header.Set("Depth", "1")
	req = req.WithContext(auth.WithUser(req.Context(), u))
	rr := httptest.NewRecorder()

	h.Propfind(rr, req)

	if rr.Code != http.StatusMultiStatus {
		t.Fatalf("expected 207, got %d", rr.Code)
	}
	body := rr.Body.String()
	if strings.Count(body, "<d:response>") != 3 { // collection + two books
		t.Fatalf("expected address book listings, got %s", body)
	}
}

func TestCalendarReportFallsBackToQueryForUnknownType(t *testing.T) {
	eventRepo := &fakeEventRepo{
		events: map[string]*store.Event{
			"1:event": {CalendarID: 1, UID: "event", RawICAL: "ICAL", ETag: "e"},
		},
	}
	h := &Handler{store: &store.Store{Events: eventRepo}}
	report := reportRequest{XMLName: xml.Name{Local: "unknown"}}
	cal := &store.CalendarAccess{Calendar: store.Calendar{ID: 1, Name: "Test"}}

	responses, _, err := h.calendarReportResponses(context.Background(), cal, "/dav/principals/1/", "/dav/calendars/1/", "/dav/calendars/1/", report)
	if err != nil {
		t.Fatalf("calendarReportResponses returned error: %v", err)
	}
	if len(responses) != 1 {
		t.Fatalf("expected 1 response, got %d", len(responses))
	}
	if responses[0].Propstat[0].Prop.GetContentType != "text/calendar; charset=utf-8" {
		t.Fatalf("unexpected content type %q", responses[0].Propstat[0].Prop.GetContentType)
	}
}

func TestAddressBookReportFallsBackToQueryForUnknownType(t *testing.T) {
	contactRepo := &fakeContactRepo{
		contacts: map[string]*store.Contact{
			"4:alice": {AddressBookID: 4, UID: "alice", RawVCard: "VCARD", ETag: "e"},
		},
	}
	h := &Handler{store: &store.Store{Contacts: contactRepo}}
	report := reportRequest{XMLName: xml.Name{Local: "unknown"}}
	book := &store.AddressBook{ID: 4, Name: "Contacts"}

	responses, _, err := h.addressBookReportResponses(context.Background(), book, "/dav/principals/1/", "/dav/addressbooks/4/", report)
	if err != nil {
		t.Fatalf("addressBookReportResponses returned error: %v", err)
	}
	if len(responses) != 1 {
		t.Fatalf("expected 1 response, got %d", len(responses))
	}
	if responses[0].Propstat[0].Prop.GetContentType != "text/vcard; charset=utf-8" {
		t.Fatalf("unexpected content type %q", responses[0].Propstat[0].Prop.GetContentType)
	}
}

func TestGetServesCalendarEvent(t *testing.T) {
	calRepo := &fakeCalendarRepo{
		accessible: []store.CalendarAccess{
			{Calendar: store.Calendar{ID: 2, UserID: 1, Name: "Work"}, Editor: true},
		},
	}
	eventRepo := &fakeEventRepo{
		events: map[string]*store.Event{
			"2:event": {CalendarID: 2, UID: "event", RawICAL: "ICALDATA", ETag: "etag1"},
		},
	}
	h := &Handler{store: &store.Store{Calendars: calRepo, Events: eventRepo}}
	u := &store.User{ID: 1}

	req := httptest.NewRequest(http.MethodGet, "/dav/calendars/2/event.ics", nil)
	req = req.WithContext(auth.WithUser(req.Context(), u))
	rr := httptest.NewRecorder()

	h.Get(rr, req)

	if rr.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rr.Code)
	}
	if rr.Header().Get("Content-Type") != "text/calendar" {
		t.Fatalf("unexpected content type %q", rr.Header().Get("Content-Type"))
	}
	if !strings.Contains(rr.Header().Get("ETag"), "etag1") {
		t.Fatalf("missing ETag header, got %q", rr.Header().Get("ETag"))
	}
	if rr.Body.String() != "ICALDATA" {
		t.Fatalf("unexpected body %q", rr.Body.String())
	}
}

func TestGetServesContact(t *testing.T) {
	bookRepo := &fakeAddressBookRepo{
		books: map[int64]*store.AddressBook{
			5: {ID: 5, UserID: 1, Name: "Contacts"},
		},
	}
	contactRepo := &fakeContactRepo{
		contacts: map[string]*store.Contact{
			"5:alice": {AddressBookID: 5, UID: "alice", RawVCard: "VCARD", ETag: "etag2"},
		},
	}
	h := &Handler{store: &store.Store{AddressBooks: bookRepo, Contacts: contactRepo}}
	u := &store.User{ID: 1}

	req := httptest.NewRequest(http.MethodGet, "/dav/addressbooks/5/alice.vcf", nil)
	req = req.WithContext(auth.WithUser(req.Context(), u))
	rr := httptest.NewRecorder()

	h.Get(rr, req)

	if rr.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rr.Code)
	}
	if rr.Header().Get("Content-Type") != "text/vcard" {
		t.Fatalf("unexpected content type %q", rr.Header().Get("Content-Type"))
	}
	if !strings.Contains(rr.Header().Get("ETag"), "etag2") {
		t.Fatalf("missing ETag header, got %q", rr.Header().Get("ETag"))
	}
	if rr.Body.String() != "VCARD" {
		t.Fatalf("unexpected body %q", rr.Body.String())
	}
}

func TestGetRequiresUser(t *testing.T) {
	h := &Handler{}
	req := httptest.NewRequest(http.MethodGet, "/dav/calendars/1/e.ics", nil)
	rr := httptest.NewRecorder()

	h.Get(rr, req)

	if rr.Code != http.StatusUnauthorized {
		t.Fatalf("expected 401, got %d", rr.Code)
	}
}

func TestDeleteRemovesContactFromAddressBook(t *testing.T) {
	bookRepo := &fakeAddressBookRepo{
		books: map[int64]*store.AddressBook{
			5: {ID: 5, UserID: 1, Name: "Contacts"},
		},
	}
	contactRepo := &fakeContactRepo{
		contacts: map[string]*store.Contact{
			"5:alice": {AddressBookID: 5, UID: "alice"},
		},
	}
	h := &Handler{store: &store.Store{AddressBooks: bookRepo, Contacts: contactRepo}}
	u := &store.User{ID: 1}
	req := httptest.NewRequest(http.MethodDelete, "/dav/addressbooks/5/alice.vcf", nil)
	req = req.WithContext(auth.WithUser(req.Context(), u))
	rr := httptest.NewRecorder()

	h.Delete(rr, req)

	if rr.Code != http.StatusNoContent {
		t.Fatalf("expected 204, got %d", rr.Code)
	}
	if _, ok := contactRepo.contacts[contactRepo.key(5, "alice")]; ok {
		t.Fatal("contact should be deleted")
	}
}

func TestReportRequiresAuthentication(t *testing.T) {
	h := &Handler{}
	req := httptest.NewRequest("REPORT", "/dav/calendars/1/", strings.NewReader(`<cal:calendar-query xmlns:cal="urn:ietf:params:xml:ns:caldav"/>`))
	rr := httptest.NewRecorder()

	h.Report(rr, req)

	if rr.Code != http.StatusUnauthorized {
		t.Fatalf("expected 401, got %d", rr.Code)
	}
}

func TestReportRejectsTooLargeBody(t *testing.T) {
	calRepo := &fakeCalendarRepo{
		accessible: []store.CalendarAccess{
			{Calendar: store.Calendar{ID: 1, UserID: 1, Name: "Test"}, Editor: true},
		},
	}
	h := &Handler{store: &store.Store{Calendars: calRepo, Events: &fakeEventRepo{}}}
	req := httptest.NewRequest("REPORT", "/dav/calendars/1/", nil)
	req.ContentLength = maxDAVBodyBytes + 1
	req = req.WithContext(auth.WithUser(req.Context(), &store.User{ID: 1}))
	rr := httptest.NewRecorder()

	h.Report(rr, req)

	if rr.Code != http.StatusRequestEntityTooLarge {
		t.Fatalf("expected 413, got %d", rr.Code)
	}
}

func TestReportCalendarQueryReturnsEvents(t *testing.T) {
	calRepo := &fakeCalendarRepo{
		accessible: []store.CalendarAccess{
			{Calendar: store.Calendar{ID: 1, UserID: 1, Name: "Test"}, Editor: true},
		},
	}
	eventRepo := &fakeEventRepo{
		events: map[string]*store.Event{
			"1:event": {CalendarID: 1, UID: "event", RawICAL: "ICAL", ETag: "etag"},
		},
	}
	h := &Handler{store: &store.Store{Calendars: calRepo, Events: eventRepo}}
	body := `<cal:calendar-query xmlns:cal="urn:ietf:params:xml:ns:caldav"/>`
	req := httptest.NewRequest("REPORT", "/dav/calendars/1/", strings.NewReader(body))
	req = req.WithContext(auth.WithUser(req.Context(), &store.User{ID: 1}))
	rr := httptest.NewRecorder()

	h.Report(rr, req)

	if rr.Code != http.StatusMultiStatus {
		t.Fatalf("expected 207, got %d", rr.Code)
	}
	if !strings.Contains(rr.Body.String(), "event.ics") {
		t.Fatalf("expected event href in body, got %s", rr.Body.String())
	}
}

func TestReportAddressBookQueryReturnsContacts(t *testing.T) {
	bookRepo := &fakeAddressBookRepo{
		books: map[int64]*store.AddressBook{
			3: {ID: 3, UserID: 1, Name: "Contacts"},
		},
	}
	contactRepo := &fakeContactRepo{
		contacts: map[string]*store.Contact{
			"3:alice": {AddressBookID: 3, UID: "alice", RawVCard: "VCARD", ETag: "etag"},
		},
	}
	h := &Handler{store: &store.Store{AddressBooks: bookRepo, Contacts: contactRepo}}
	body := `<card:addressbook-query xmlns:card="urn:ietf:params:xml:ns:carddav"/>`
	req := httptest.NewRequest("REPORT", "/dav/addressbooks/3/", strings.NewReader(body))
	req = req.WithContext(auth.WithUser(req.Context(), &store.User{ID: 1}))
	rr := httptest.NewRecorder()

	h.Report(rr, req)

	if rr.Code != http.StatusMultiStatus {
		t.Fatalf("expected 207, got %d", rr.Code)
	}
	if !strings.Contains(rr.Body.String(), "alice.vcf") {
		t.Fatalf("expected contact href in body, got %s", rr.Body.String())
	}
}

func TestReportInvalidCalendarIDReturnsBadRequest(t *testing.T) {
	h := &Handler{}
	req := httptest.NewRequest("REPORT", "/dav/calendars/notanint/", strings.NewReader(`<D:sync-collection xmlns:D="DAV:"></D:sync-collection>`))
	req = req.WithContext(auth.WithUser(req.Context(), &store.User{ID: 1}))
	rr := httptest.NewRecorder()

	h.Report(rr, req)

	if rr.Code != http.StatusBadRequest {
		t.Fatalf("expected 400 for invalid calendar id, got %d", rr.Code)
	}
}

func TestReportAddressBookRejectsInvalidSyncToken(t *testing.T) {
	now := store.Now()
	bookRepo := &fakeAddressBookRepo{
		books: map[int64]*store.AddressBook{
			3: {ID: 3, UserID: 1, Name: "Contacts", CTag: 1, UpdatedAt: now},
		},
	}
	h := &Handler{store: &store.Store{AddressBooks: bookRepo, Contacts: &fakeContactRepo{}, DeletedResources: &fakeDeletedResourceRepo{}}}
	req := httptest.NewRequest("REPORT", "/dav/addressbooks/3/", strings.NewReader(`<D:sync-collection xmlns:D="DAV:"><D:sync-token>urn:calcard-sync:cal:3:0</D:sync-token></D:sync-collection>`))
	req.Header.Set("Content-Type", "application/xml")
	req = req.WithContext(auth.WithUser(req.Context(), &store.User{ID: 1}))
	rr := httptest.NewRecorder()

	h.Report(rr, req)

	if rr.Code != http.StatusForbidden {
		t.Fatalf("expected 403 for invalid sync token kind, got %d", rr.Code)
	}
}

func TestPutCreatesCalendarEventWhenEditor(t *testing.T) {
	now := store.Now()
	calRepo := &fakeCalendarRepo{
		accessible: []store.CalendarAccess{
			{Calendar: store.Calendar{ID: 2, Name: "Work", UpdatedAt: now}, Editor: true},
		},
	}
	eventRepo := &fakeEventRepo{events: map[string]*store.Event{}}
	h := &Handler{store: &store.Store{Calendars: calRepo, Events: eventRepo}}
	u := &store.User{ID: 1}

	validIcal := "BEGIN:VCALENDAR\r\nVERSION:2.0\r\nBEGIN:VEVENT\r\nUID:new\r\nEND:VEVENT\r\nEND:VCALENDAR\r\n"
	req := newCalendarPutRequest("/dav/calendars/2/new.ics", strings.NewReader(validIcal))
	req = req.WithContext(auth.WithUser(req.Context(), u))
	rr := httptest.NewRecorder()

	h.Put(rr, req)

	if rr.Code != http.StatusCreated {
		t.Fatalf("expected 201, got %d: %s", rr.Code, rr.Body.String())
	}
	if rr.Header().Get("ETag") == "" {
		t.Fatal("expected ETag header")
	}
	if _, ok := eventRepo.events[eventRepo.key(2, "new")]; !ok {
		t.Fatal("event not stored via Upsert")
	}
}

func TestPutRejectsCalendarWriteWithoutEditor(t *testing.T) {
	calRepo := &fakeCalendarRepo{
		accessible: []store.CalendarAccess{
			{Calendar: store.Calendar{ID: 2, Name: "Work"}, Editor: false},
		},
	}
	h := &Handler{store: &store.Store{Calendars: calRepo, Events: &fakeEventRepo{}}}
	u := &store.User{ID: 1}

	req := newCalendarPutRequest("/dav/calendars/2/new.ics", strings.NewReader("ICALDATA"))
	req = req.WithContext(auth.WithUser(req.Context(), u))
	rr := httptest.NewRecorder()

	h.Put(rr, req)

	if rr.Code != http.StatusForbidden {
		t.Fatalf("expected 403, got %d", rr.Code)
	}
}

func TestPutCreatesContact(t *testing.T) {
	now := store.Now()
	bookRepo := &fakeAddressBookRepo{
		books: map[int64]*store.AddressBook{
			5: {ID: 5, UserID: 1, Name: "Contacts", UpdatedAt: now},
		},
	}
	contactRepo := &fakeContactRepo{contacts: map[string]*store.Contact{}}
	h := &Handler{store: &store.Store{AddressBooks: bookRepo, Contacts: contactRepo}}
	u := &store.User{ID: 1}

	validVCard := "BEGIN:VCARD\r\nVERSION:3.0\r\nFN:Alice\r\nEND:VCARD\r\n"
	req := httptest.NewRequest(http.MethodPut, "/dav/addressbooks/5/alice.vcf", strings.NewReader(validVCard))
	req = req.WithContext(auth.WithUser(req.Context(), u))
	rr := httptest.NewRecorder()

	h.Put(rr, req)

	if rr.Code != http.StatusCreated {
		t.Fatalf("expected 201, got %d: %s", rr.Code, rr.Body.String())
	}
	if rr.Header().Get("ETag") == "" {
		t.Fatal("expected ETag header")
	}
	if _, ok := contactRepo.contacts[contactRepo.key(5, "alice")]; !ok {
		t.Fatal("contact not stored via Upsert")
	}
}

func TestDeleteCalendarEventHonorsEditor(t *testing.T) {
	calRepo := &fakeCalendarRepo{
		accessible: []store.CalendarAccess{
			{Calendar: store.Calendar{ID: 2, Name: "Work"}, Editor: false},
		},
	}
	eventRepo := &fakeEventRepo{
		events: map[string]*store.Event{
			"2:old": {CalendarID: 2, UID: "old"},
		},
	}
	h := &Handler{store: &store.Store{Calendars: calRepo, Events: eventRepo}}
	u := &store.User{ID: 1}

	req := httptest.NewRequest(http.MethodDelete, "/dav/calendars/2/old.ics", nil)
	req = req.WithContext(auth.WithUser(req.Context(), u))
	rr := httptest.NewRecorder()

	h.Delete(rr, req)

	if rr.Code != http.StatusForbidden {
		t.Fatalf("expected 403 without editor, got %d", rr.Code)
	}

	// Grant editor and delete
	calRepo.accessible[0].Editor = true
	rr = httptest.NewRecorder()
	h.Delete(rr, req)
	if rr.Code != http.StatusNoContent {
		t.Fatalf("expected 204, got %d", rr.Code)
	}
	if _, ok := eventRepo.events[eventRepo.key(2, "old")]; ok {
		t.Fatal("event should be deleted")
	}
}

func TestMkcolCreatesAddressBook(t *testing.T) {
	bookRepo := &fakeAddressBookRepo{}
	h := &Handler{store: &store.Store{AddressBooks: bookRepo}}
	u := &store.User{ID: 1}
	req := httptest.NewRequest("MKCOL", "/dav/addressbooks/NewBook", nil)
	req = req.WithContext(auth.WithUser(req.Context(), u))
	rr := httptest.NewRecorder()

	h.Mkcol(rr, req)

	if rr.Code != http.StatusCreated {
		t.Fatalf("expected 201, got %d", rr.Code)
	}
	if len(bookRepo.books) != 1 {
		t.Fatalf("expected book to be created, got %d", len(bookRepo.books))
	}
}

func TestMkcalendarCreatesCalendar(t *testing.T) {
	calRepo := &fakeCalendarRepo{}
	h := &Handler{store: &store.Store{Calendars: calRepo}}
	u := &store.User{ID: 1}
	req := httptest.NewRequest("MKCALENDAR", "/dav/calendars/NewCal", nil)
	req = req.WithContext(auth.WithUser(req.Context(), u))
	rr := httptest.NewRecorder()

	h.Mkcalendar(rr, req)

	if rr.Code != http.StatusCreated {
		t.Fatalf("expected 201, got %d", rr.Code)
	}
	if len(calRepo.calendars) != 1 {
		t.Fatalf("expected calendar to be created, got %d", len(calRepo.calendars))
	}
}

func TestMkcalendarParsesChunkedRequestBody(t *testing.T) {
	calRepo := &fakeCalendarRepo{}
	h := &Handler{store: &store.Store{Calendars: calRepo}}
	u := &store.User{ID: 1}
	body := `<?xml version="1.0" encoding="utf-8" ?>
<C:mkcalendar xmlns:D="DAV:" xmlns:C="urn:ietf:params:xml:ns:caldav">
  <D:set>
    <D:prop>
      <D:displayname>Chunked Cal</D:displayname>
    </D:prop>
  </D:set>
</C:mkcalendar>`

	req := httptest.NewRequest("MKCALENDAR", "/dav/calendars/Chunked", strings.NewReader(body))
	req.ContentLength = -1
	req = req.WithContext(auth.WithUser(req.Context(), u))
	rr := httptest.NewRecorder()

	h.Mkcalendar(rr, req)

	if rr.Code != http.StatusCreated {
		t.Fatalf("expected 201, got %d", rr.Code)
	}
	if len(calRepo.calendars) != 1 {
		t.Fatalf("expected calendar to be created, got %d", len(calRepo.calendars))
	}
}

func TestMkcalendarRejectsSlugNameCollisions(t *testing.T) {
	slug := "team"
	calRepo := &fakeCalendarRepo{
		accessible: []store.CalendarAccess{
			{Calendar: store.Calendar{ID: 1, UserID: 1, Name: "Work", Slug: &slug}, Editor: true},
		},
	}
	h := &Handler{store: &store.Store{Calendars: calRepo}}
	u := &store.User{ID: 1}

	req := httptest.NewRequest("MKCALENDAR", "/dav/calendars/work", nil)
	req = req.WithContext(auth.WithUser(req.Context(), u))
	rr := httptest.NewRecorder()
	h.Mkcalendar(rr, req)
	if rr.Code != http.StatusConflict {
		t.Fatalf("expected 409 for name collision, got %d", rr.Code)
	}

	req = httptest.NewRequest("MKCALENDAR", "/dav/calendars/TEAM", nil)
	req = req.WithContext(auth.WithUser(req.Context(), u))
	rr = httptest.NewRecorder()
	h.Mkcalendar(rr, req)
	if rr.Code != http.StatusConflict {
		t.Fatalf("expected 409 for slug collision, got %d", rr.Code)
	}
}

func TestAddressBookMultiGetFiltersByBook(t *testing.T) {
	repo := &fakeContactRepo{
		contacts: map[string]*store.Contact{
			"2:keep": {AddressBookID: 2, UID: "keep", RawVCard: "VCARD", ETag: "etag-1"},
			"3:skip": {AddressBookID: 3, UID: "skip", RawVCard: "VCARD", ETag: "etag-2"},
		},
	}
	h := &Handler{store: &store.Store{Contacts: repo, DeletedResources: &fakeDeletedResourceRepo{}}}
	hrefs := []string{"/dav/addressbooks/2/keep.vcf", "/dav/addressbooks/3/skip.vcf"}
	responses, err := h.addressBookMultiGet(context.Background(), 2, hrefs, "/dav/addressbooks/2/")
	if err != nil {
		t.Fatalf("addressBookMultiGet returned error: %v", err)
	}
	if len(responses) != 1 {
		t.Fatalf("expected 1 response, got %d", len(responses))
	}
	if responses[0].Href != "/dav/addressbooks/2/keep.vcf" {
		t.Fatalf("unexpected href %q", responses[0].Href)
	}
}

func TestAddressBookMultiGetMissingReturns404(t *testing.T) {
	repo := &fakeContactRepo{
		contacts: map[string]*store.Contact{
			"2:present": {AddressBookID: 2, UID: "present", RawVCard: "VCARD", ETag: "etag-1"},
		},
	}
	h := &Handler{store: &store.Store{Contacts: repo, DeletedResources: &fakeDeletedResourceRepo{}}}
	hrefs := []string{"/dav/addressbooks/2/present.vcf", "/dav/addressbooks/2/missing.vcf"}
	responses, err := h.addressBookMultiGet(context.Background(), 2, hrefs, "/dav/addressbooks/2/")
	if err != nil {
		t.Fatalf("addressBookMultiGet returned error: %v", err)
	}
	if len(responses) != 2 {
		t.Fatalf("expected 2 responses, got %d", len(responses))
	}
	if responses[1].Status != "HTTP/1.1 404 Not Found" {
		t.Fatalf("expected 404 for missing contact, got %q", responses[1].Status)
	}
}

func TestAddressBookSyncCollectionIncludesDeleted(t *testing.T) {
	now := store.Now()
	contacts := &fakeContactRepo{
		contacts: map[string]*store.Contact{
			"5:alive": {AddressBookID: 5, UID: "alive", RawVCard: "VCARD", ETag: "e", LastModified: now},
		},
	}
	deleted := &fakeDeletedResourceRepo{
		deleted: []store.DeletedResource{
			{ResourceType: "contact", CollectionID: 5, UID: "gone", DeletedAt: now},
		},
	}
	h := &Handler{store: &store.Store{Contacts: contacts, DeletedResources: deleted}}
	report := reportRequest{
		XMLName:   xml.Name{Local: "sync-collection"},
		SyncToken: buildSyncToken("card", 5, now.Add(-time.Hour)),
	}
	book := &store.AddressBook{ID: 5, Name: "Book", CTag: 1, UpdatedAt: now}
	responses, token, err := h.addressBookReportResponses(context.Background(), book, "/dav/principals/1/", "/dav/addressbooks/5/", report)
	if err != nil {
		t.Fatalf("addressBookReportResponses returned error: %v", err)
	}
	if token == "" {
		t.Fatal("expected non-empty sync token")
	}
	if len(responses) != 3 {
		t.Fatalf("expected 3 responses (collection, contact, deleted), got %d", len(responses))
	}
	var sawDeleted bool
	for _, r := range responses {
		if strings.Contains(r.Href, "gone") {
			if r.Status != "HTTP/1.1 404 Not Found" {
				t.Fatalf("expected deleted resource to have 404 status, got %q", r.Status)
			}
			sawDeleted = true
		}
	}
	if !sawDeleted {
		t.Fatal("expected deleted resource response")
	}
}

func TestCalendarSyncCollectionFiltersByModifiedSince(t *testing.T) {
	then := time.Now().Add(-time.Hour)
	now := time.Now()
	repo := &fakeEventRepo{
		events: map[string]*store.Event{
			"2:old": {CalendarID: 2, UID: "old", RawICAL: "OLD", ETag: "1", LastModified: then},
			"2:new": {CalendarID: 2, UID: "new", RawICAL: "NEW", ETag: "2", LastModified: now},
		},
	}
	h := &Handler{store: &store.Store{Events: repo, DeletedResources: &fakeDeletedResourceRepo{}}}
	report := reportRequest{
		XMLName:   xml.Name{Local: "sync-collection"},
		SyncToken: buildSyncToken("cal", 2, then),
	}
	cal := &store.CalendarAccess{Calendar: store.Calendar{ID: 2, Name: "Test", CTag: 2, UpdatedAt: now}, Editor: true}
	responses, _, err := h.calendarReportResponses(context.Background(), cal, "/dav/principals/1/", "/dav/calendars/2/", "/dav/calendars/2/", report)
	if err != nil {
		t.Fatalf("calendarReportResponses returned error: %v", err)
	}
	body := fmt.Sprint(responses)
	if strings.Contains(body, "old") {
		t.Fatalf("expected old event to be excluded from incremental sync, got %v", body)
	}
	if !strings.Contains(body, "new") {
		t.Fatalf("expected new event included, got %v", body)
	}
}

func TestNewHandlerInitializesFields(t *testing.T) {
	cfg := &config.Config{}
	s := &store.Store{}
	h := NewHandler(cfg, s)
	if h.cfg != cfg || h.store != s {
		t.Fatal("handler fields not initialized")
	}
}

func TestHeadDelegatesToGet(t *testing.T) {
	calRepo := &fakeCalendarRepo{
		accessible: []store.CalendarAccess{
			{Calendar: store.Calendar{ID: 2, UserID: 1, Name: "Work"}, Editor: true},
		},
	}
	eventRepo := &fakeEventRepo{
		events: map[string]*store.Event{
			"2:event": {CalendarID: 2, UID: "event", RawICAL: "ICALDATA", ETag: "etag1"},
		},
	}
	h := &Handler{store: &store.Store{Calendars: calRepo, Events: eventRepo}}
	req := httptest.NewRequest(http.MethodHead, "/dav/calendars/2/event.ics", nil)
	req = req.WithContext(auth.WithUser(req.Context(), &store.User{ID: 1}))
	rr := httptest.NewRecorder()

	h.Head(rr, req)

	if rr.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rr.Code)
	}
	if rr.Header().Get("Content-Type") != "text/calendar" {
		t.Fatalf("expected calendar content type, got %q", rr.Header().Get("Content-Type"))
	}
}

func TestProppatchRequiresAuth(t *testing.T) {
	h := &Handler{}
	req := httptest.NewRequest("PROPPATCH", "/dav/calendars/1", nil)
	rr := httptest.NewRecorder()
	h.Proppatch(rr, req)
	if rr.Code != http.StatusUnauthorized {
		t.Fatalf("expected 401, got %d", rr.Code)
	}
}

func TestProppatchCalendarUpdatesProperties(t *testing.T) {
	calRepo := &fakeCalendarRepo{
		accessible: []store.CalendarAccess{
			{Calendar: store.Calendar{ID: 2, UserID: 1, Name: "Old Name"}, Editor: true},
		},
	}
	h := &Handler{store: &store.Store{Calendars: calRepo}}
	u := &store.User{ID: 1}

	body := `<?xml version="1.0" encoding="utf-8" ?>
<D:propertyupdate xmlns:D="DAV:" xmlns:C="urn:ietf:params:xml:ns:caldav">
  <D:set>
    <D:prop>
      <D:displayname>New Name</D:displayname>
      <C:calendar-description>Test description</C:calendar-description>
    </D:prop>
  </D:set>
</D:propertyupdate>`

	req := httptest.NewRequest("PROPPATCH", "/dav/calendars/2", strings.NewReader(body))
	req = req.WithContext(auth.WithUser(req.Context(), u))
	rr := httptest.NewRecorder()

	h.Proppatch(rr, req)

	if rr.Code != http.StatusMultiStatus {
		t.Fatalf("expected 207, got %d: %s", rr.Code, rr.Body.String())
	}
	respBody := rr.Body.String()
	if !strings.Contains(respBody, "200 OK") {
		t.Errorf("expected success status in response, got %s", respBody)
	}
}

func TestProppatchCalendarRejectsSlugPath(t *testing.T) {
	h := &Handler{store: &store.Store{Calendars: &fakeCalendarRepo{}}}
	u := &store.User{ID: 1}

	req := httptest.NewRequest("PROPPATCH", "/dav/calendars/work", nil)
	req = req.WithContext(auth.WithUser(req.Context(), u))
	rr := httptest.NewRecorder()

	h.Proppatch(rr, req)

	if rr.Code != http.StatusBadRequest {
		t.Fatalf("expected 400 for slug calendar path, got %d", rr.Code)
	}
}

func TestPropfindRejectsAmbiguousCalendarSlug(t *testing.T) {
	slug := "work"
	calRepo := &fakeCalendarRepo{
		accessible: []store.CalendarAccess{
			{Calendar: store.Calendar{ID: 1, UserID: 2, Name: "Work", Slug: &slug}, Editor: false},
			{Calendar: store.Calendar{ID: 2, UserID: 3, Name: "Work", Slug: &slug}, Editor: false},
		},
	}
	h := &Handler{store: &store.Store{Calendars: calRepo}}
	u := &store.User{ID: 1}

	req := httptest.NewRequest("PROPFIND", "/dav/calendars/work/", nil)
	req = req.WithContext(auth.WithUser(req.Context(), u))
	rr := httptest.NewRecorder()

	h.Propfind(rr, req)

	if rr.Code != http.StatusConflict {
		t.Fatalf("expected 409 for ambiguous calendar slug, got %d: %s", rr.Code, rr.Body.String())
	}
}

func TestGetNotFoundReturns404(t *testing.T) {
	calRepo := &fakeCalendarRepo{
		accessible: []store.CalendarAccess{
			{Calendar: store.Calendar{ID: 2, UserID: 1, Name: "Work"}, Editor: true},
		},
	}
	h := &Handler{store: &store.Store{Calendars: calRepo, Events: &fakeEventRepo{}}}
	req := httptest.NewRequest(http.MethodGet, "/dav/calendars/2/missing.ics", nil)
	req = req.WithContext(auth.WithUser(req.Context(), &store.User{ID: 1}))
	rr := httptest.NewRecorder()

	h.Get(rr, req)

	if rr.Code != http.StatusNotFound {
		t.Fatalf("expected 404 for missing event, got %d", rr.Code)
	}
}

func TestGetRejectsUnsupportedPath(t *testing.T) {
	h := &Handler{}
	req := httptest.NewRequest(http.MethodGet, "/unknown", nil)
	req = req.WithContext(auth.WithUser(req.Context(), &store.User{ID: 1}))
	rr := httptest.NewRecorder()

	h.Get(rr, req)

	if rr.Code != http.StatusNotFound {
		t.Fatalf("expected 404 for unsupported path, got %d", rr.Code)
	}
}

func TestDeleteUnsupportedPath(t *testing.T) {
	h := &Handler{}
	req := httptest.NewRequest(http.MethodDelete, "/dav/unknown", nil)
	req = req.WithContext(auth.WithUser(req.Context(), &store.User{ID: 1}))
	rr := httptest.NewRecorder()

	h.Delete(rr, req)

	if rr.Code != http.StatusBadRequest {
		t.Fatalf("expected 400 for unsupported delete path, got %d", rr.Code)
	}
}

func TestDeleteRequiresUser(t *testing.T) {
	h := &Handler{}
	req := httptest.NewRequest(http.MethodDelete, "/dav/calendars/1/e.ics", nil)
	rr := httptest.NewRecorder()

	h.Delete(rr, req)

	if rr.Code != http.StatusUnauthorized {
		t.Fatalf("expected 401, got %d", rr.Code)
	}
}

func TestReportInvalidXMLReturnsBadRequest(t *testing.T) {
	h := &Handler{}
	req := httptest.NewRequest("REPORT", "/dav/calendars/1/", strings.NewReader("<bad"))
	req = req.WithContext(auth.WithUser(req.Context(), &store.User{ID: 1}))
	rr := httptest.NewRecorder()

	h.Report(rr, req)

	if rr.Code != http.StatusBadRequest {
		t.Fatalf("expected 400 for invalid XML, got %d", rr.Code)
	}
}

func TestReportUnsupportedPath(t *testing.T) {
	h := &Handler{}
	req := httptest.NewRequest("REPORT", "/dav/unknown", strings.NewReader(`<D:sync-collection xmlns:D="DAV:"></D:sync-collection>`))
	req = req.WithContext(auth.WithUser(req.Context(), &store.User{ID: 1}))
	rr := httptest.NewRecorder()

	h.Report(rr, req)

	if rr.Code != http.StatusBadRequest {
		t.Fatalf("expected 400 for unsupported path, got %d", rr.Code)
	}
}

func TestReportCalendarInvalidSyncToken(t *testing.T) {
	calRepo := &fakeCalendarRepo{
		accessible: []store.CalendarAccess{
			{Calendar: store.Calendar{ID: 1, UserID: 1, Name: "Test"}, Editor: true},
		},
	}
	h := &Handler{store: &store.Store{Calendars: calRepo, Events: &fakeEventRepo{}, DeletedResources: &fakeDeletedResourceRepo{}}}
	req := httptest.NewRequest("REPORT", "/dav/calendars/1/", strings.NewReader(`<D:sync-collection xmlns:D="DAV:"><D:sync-token>urn:calcard-sync:card:1:0</D:sync-token></D:sync-collection>`))
	req = req.WithContext(auth.WithUser(req.Context(), &store.User{ID: 1}))
	rr := httptest.NewRecorder()

	h.Report(rr, req)

	if rr.Code != http.StatusForbidden {
		t.Fatalf("expected 403 for invalid calendar sync token, got %d", rr.Code)
	}
}

func TestPutUpdatesExistingEventReturnsNoContent(t *testing.T) {
	calRepo := &fakeCalendarRepo{
		accessible: []store.CalendarAccess{
			{Calendar: store.Calendar{ID: 2, UserID: 1, Name: "Work"}, Editor: true},
		},
	}
	eventRepo := &fakeEventRepo{
		events: map[string]*store.Event{
			"2:event": {CalendarID: 2, UID: "event", RawICAL: "BEGIN:VCALENDAR\r\nBEGIN:VEVENT\r\nUID:event\r\nEND:VEVENT\r\nEND:VCALENDAR\r\n", ETag: "old"},
		},
	}
	h := &Handler{store: &store.Store{Calendars: calRepo, Events: eventRepo}}
	newIcal := "BEGIN:VCALENDAR\r\nVERSION:2.0\r\nBEGIN:VEVENT\r\nUID:event\r\nSUMMARY:Updated\r\nEND:VEVENT\r\nEND:VCALENDAR\r\n"
	req := newCalendarPutRequest("/dav/calendars/2/event.ics", strings.NewReader(newIcal))
	req = req.WithContext(auth.WithUser(req.Context(), &store.User{ID: 1}))
	rr := httptest.NewRecorder()

	h.Put(rr, req)

	if rr.Code != http.StatusNoContent {
		t.Fatalf("expected 204, got %d: %s", rr.Code, rr.Body.String())
	}
}

func TestPutUnsupportedPath(t *testing.T) {
	h := &Handler{}
	req := httptest.NewRequest(http.MethodPut, "/dav/unknown", strings.NewReader("data"))
	req = req.WithContext(auth.WithUser(req.Context(), &store.User{ID: 1}))
	rr := httptest.NewRecorder()

	h.Put(rr, req)

	if rr.Code != http.StatusBadRequest {
		t.Fatalf("expected 400 for unsupported put path, got %d", rr.Code)
	}
}

func TestPutTooLarge(t *testing.T) {
	h := &Handler{}
	req := newCalendarPutRequest("/dav/calendars/1/e.ics", nil)
	req.ContentLength = maxDAVBodyBytes + 1
	req = req.WithContext(auth.WithUser(req.Context(), &store.User{ID: 1}))
	rr := httptest.NewRecorder()

	h.Put(rr, req)

	if rr.Code != http.StatusRequestEntityTooLarge {
		t.Fatalf("expected 413, got %d", rr.Code)
	}
}

func TestMkcolValidatesPathAndName(t *testing.T) {
	h := &Handler{}
	u := &store.User{ID: 1}
	// unsupported path
	req := httptest.NewRequest("MKCOL", "/dav/calendars/bad", nil)
	req = req.WithContext(auth.WithUser(req.Context(), u))
	rr := httptest.NewRecorder()
	h.Mkcol(rr, req)
	if rr.Code != http.StatusBadRequest {
		t.Fatalf("expected 400 for unsupported mkcol path, got %d", rr.Code)
	}

	// missing name
	req = httptest.NewRequest("MKCOL", "/dav/addressbooks/", nil)
	req = req.WithContext(auth.WithUser(req.Context(), u))
	rr = httptest.NewRecorder()
	h.Mkcol(rr, req)
	if rr.Code != http.StatusBadRequest {
		t.Fatalf("expected 400 for missing collection name, got %d", rr.Code)
	}
}

func TestMkcalendarValidatesPathAndName(t *testing.T) {
	h := &Handler{}
	u := &store.User{ID: 1}
	// unsupported path
	req := httptest.NewRequest("MKCALENDAR", "/dav/addressbooks/bad", nil)
	req = req.WithContext(auth.WithUser(req.Context(), u))
	rr := httptest.NewRecorder()
	h.Mkcalendar(rr, req)
	if rr.Code != http.StatusBadRequest {
		t.Fatalf("expected 400 for unsupported mkcalendar path, got %d", rr.Code)
	}

	// missing name
	req = httptest.NewRequest("MKCALENDAR", "/dav/calendars/", nil)
	req = req.WithContext(auth.WithUser(req.Context(), u))
	rr = httptest.NewRecorder()
	h.Mkcalendar(rr, req)
	if rr.Code != http.StatusBadRequest {
		t.Fatalf("expected 400 for missing calendar name, got %d", rr.Code)
	}

	// numeric name
	req = httptest.NewRequest("MKCALENDAR", "/dav/calendars/123", nil)
	req = req.WithContext(auth.WithUser(req.Context(), u))
	rr = httptest.NewRecorder()
	h.Mkcalendar(rr, req)
	if rr.Code != http.StatusBadRequest {
		t.Fatalf("expected 400 for numeric calendar name, got %d", rr.Code)
	}
}

func TestLoadCalendarNotFound(t *testing.T) {
	h := &Handler{store: &store.Store{Calendars: &fakeCalendarRepo{accessible: []store.CalendarAccess{}}}}
	if _, err := h.loadCalendar(context.Background(), &store.User{ID: 1}, 10); !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("expected ErrNotFound, got %v", err)
	}
}

func TestLoadAddressBookWrongUser(t *testing.T) {
	bookRepo := &fakeAddressBookRepo{
		books: map[int64]*store.AddressBook{
			1: {ID: 1, UserID: 2, Name: "Other"},
		},
	}
	h := &Handler{store: &store.Store{AddressBooks: bookRepo}}
	if _, err := h.loadAddressBook(context.Background(), &store.User{ID: 1}, 1); !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("expected ErrNotFound, got %v", err)
	}
}

func TestPrincipalResponsesRejectsOtherPrincipal(t *testing.T) {
	h := &Handler{}
	_, err := h.principalResponses("/dav/principals/999", "0", &store.User{ID: 1}, func(s string) string { return s })
	if !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("expected ErrNotFound, got %v", err)
	}
}

func TestCdataStringEmptySkipsEncoding(t *testing.T) {
	var buf strings.Builder
	enc := xml.NewEncoder(&buf)
	if err := cdataString("").MarshalXML(enc, xml.StartElement{Name: xml.Name{Local: "card:address-data"}}); err != nil {
		t.Fatalf("MarshalXML returned error: %v", err)
	}
	enc.Flush()
	if buf.Len() != 0 {
		t.Fatalf("expected no output for empty cdataString, got %q", buf.String())
	}
}

func TestPropfindRequiresUser(t *testing.T) {
	h := &Handler{}
	req := httptest.NewRequest("PROPFIND", "/dav", nil)
	rr := httptest.NewRecorder()
	h.Propfind(rr, req)
	if rr.Code != http.StatusUnauthorized {
		t.Fatalf("expected 401, got %d", rr.Code)
	}
}

func TestPropfindUnsupportedPathReturnsNotFound(t *testing.T) {
	h := &Handler{}
	req := httptest.NewRequest("PROPFIND", "/invalid", nil)
	req = req.WithContext(auth.WithUser(req.Context(), &store.User{ID: 1}))
	rr := httptest.NewRecorder()
	h.Propfind(rr, req)
	if rr.Code != http.StatusNotFound {
		t.Fatalf("expected 404, got %d", rr.Code)
	}
}

func TestGetAddressBookNotFoundForWrongUser(t *testing.T) {
	bookRepo := &fakeAddressBookRepo{
		books: map[int64]*store.AddressBook{
			5: {ID: 5, UserID: 2, Name: "Other"},
		},
	}
	h := &Handler{store: &store.Store{AddressBooks: bookRepo, Contacts: &fakeContactRepo{}}}
	req := httptest.NewRequest(http.MethodGet, "/dav/addressbooks/5/alice.vcf", nil)
	req = req.WithContext(auth.WithUser(req.Context(), &store.User{ID: 1}))
	rr := httptest.NewRecorder()
	h.Get(rr, req)
	if rr.Code != http.StatusNotFound {
		t.Fatalf("expected 404 for foreign address book, got %d", rr.Code)
	}
}

func TestPutRequiresUser(t *testing.T) {
	h := &Handler{}
	req := newCalendarPutRequest("/dav/calendars/1/e.ics", strings.NewReader("data"))
	rr := httptest.NewRecorder()
	h.Put(rr, req)
	if rr.Code != http.StatusUnauthorized {
		t.Fatalf("expected 401, got %d", rr.Code)
	}
}

func TestPutAddressBookNotFound(t *testing.T) {
	bookRepo := &fakeAddressBookRepo{books: map[int64]*store.AddressBook{}}
	h := &Handler{store: &store.Store{AddressBooks: bookRepo, Contacts: &fakeContactRepo{}}}
	req := httptest.NewRequest(http.MethodPut, "/dav/addressbooks/9/alice.vcf", strings.NewReader("VCARD"))
	req = req.WithContext(auth.WithUser(req.Context(), &store.User{ID: 1}))
	rr := httptest.NewRecorder()
	h.Put(rr, req)
	if rr.Code != http.StatusNotFound {
		t.Fatalf("expected 404 for missing address book, got %d", rr.Code)
	}
}

func TestDeleteCalendarNotFound(t *testing.T) {
	h := &Handler{store: &store.Store{Calendars: &fakeCalendarRepo{accessible: []store.CalendarAccess{}}, Events: &fakeEventRepo{}}}
	req := httptest.NewRequest(http.MethodDelete, "/dav/calendars/1/e.ics", nil)
	req = req.WithContext(auth.WithUser(req.Context(), &store.User{ID: 1}))
	rr := httptest.NewRecorder()
	h.Delete(rr, req)
	if rr.Code != http.StatusNotFound {
		t.Fatalf("expected 404 when calendar missing, got %d", rr.Code)
	}
}

func TestReportCalendarSyncCollectionViaHandler(t *testing.T) {
	now := store.Now()
	calRepo := &fakeCalendarRepo{
		accessible: []store.CalendarAccess{
			{Calendar: store.Calendar{ID: 2, UserID: 1, Name: "Work", CTag: 1, UpdatedAt: now}, Editor: true},
		},
	}
	eventRepo := &fakeEventRepo{
		events: map[string]*store.Event{
			"2:event": {CalendarID: 2, UID: "event", RawICAL: "ICAL", ETag: "e", LastModified: now},
		},
	}
	deletedRepo := &fakeDeletedResourceRepo{
		deleted: []store.DeletedResource{
			{ResourceType: "event", CollectionID: 2, UID: "gone", DeletedAt: now},
		},
	}
	h := &Handler{store: &store.Store{Calendars: calRepo, Events: eventRepo, DeletedResources: deletedRepo}}
	body := `<D:sync-collection xmlns:D="DAV:"><D:sync-token>` + buildSyncToken("cal", 2, now.Add(-time.Hour)) + `</D:sync-token></D:sync-collection>`
	req := httptest.NewRequest("REPORT", "/dav/calendars/2/", strings.NewReader(body))
	req = req.WithContext(auth.WithUser(req.Context(), &store.User{ID: 1}))
	rr := httptest.NewRecorder()
	h.Report(rr, req)
	if rr.Code != http.StatusMultiStatus {
		t.Fatalf("expected 207, got %d", rr.Code)
	}
	respBody := rr.Body.String()
	if !strings.Contains(respBody, "event.ics") || !strings.Contains(respBody, "gone.ics") {
		t.Fatalf("expected sync responses for event and deleted resource, got %s", respBody)
	}
}

func TestReportAddressBookSyncCollectionViaHandler(t *testing.T) {
	now := store.Now()
	bookRepo := &fakeAddressBookRepo{
		books: map[int64]*store.AddressBook{
			3: {ID: 3, UserID: 1, Name: "Contacts", CTag: 1, UpdatedAt: now},
		},
	}
	contactRepo := &fakeContactRepo{
		contacts: map[string]*store.Contact{
			"3:alice": {AddressBookID: 3, UID: "alice", RawVCard: "VCARD", ETag: "e", LastModified: now},
		},
	}
	deletedRepo := &fakeDeletedResourceRepo{
		deleted: []store.DeletedResource{
			{ResourceType: "contact", CollectionID: 3, UID: "bob", DeletedAt: now},
		},
	}
	h := &Handler{store: &store.Store{AddressBooks: bookRepo, Contacts: contactRepo, DeletedResources: deletedRepo}}
	body := `<D:sync-collection xmlns:D="DAV:"><D:sync-token>` + buildSyncToken("card", 3, now.Add(-time.Hour)) + `</D:sync-token></D:sync-collection>`
	req := httptest.NewRequest("REPORT", "/dav/addressbooks/3/", strings.NewReader(body))
	req = req.WithContext(auth.WithUser(req.Context(), &store.User{ID: 1}))
	rr := httptest.NewRecorder()
	h.Report(rr, req)
	if rr.Code != http.StatusMultiStatus {
		t.Fatalf("expected 207, got %d", rr.Code)
	}
	respBody := rr.Body.String()
	if !strings.Contains(respBody, "alice.vcf") || !strings.Contains(respBody, "bob.vcf") {
		t.Fatalf("expected sync responses for contact and deleted resource, got %s", respBody)
	}
}

func TestMkcolRequiresUser(t *testing.T) {
	h := &Handler{}
	req := httptest.NewRequest("MKCOL", "/dav/addressbooks/Book", nil)
	rr := httptest.NewRecorder()
	h.Mkcol(rr, req)
	if rr.Code != http.StatusUnauthorized {
		t.Fatalf("expected 401, got %d", rr.Code)
	}
}

func TestMkcalendarRequiresUser(t *testing.T) {
	h := &Handler{}
	req := httptest.NewRequest("MKCALENDAR", "/dav/calendars/Cal", nil)
	rr := httptest.NewRecorder()
	h.Mkcalendar(rr, req)
	if rr.Code != http.StatusUnauthorized {
		t.Fatalf("expected 401, got %d", rr.Code)
	}
}

func TestPutCalendarNotFound(t *testing.T) {
	calRepo := &fakeCalendarRepo{accessible: []store.CalendarAccess{}}
	h := &Handler{store: &store.Store{Calendars: calRepo, Events: &fakeEventRepo{}}}
	req := newCalendarPutRequest("/dav/calendars/9/e.ics", strings.NewReader("ICAL"))
	req = req.WithContext(auth.WithUser(req.Context(), &store.User{ID: 1}))
	rr := httptest.NewRecorder()
	h.Put(rr, req)
	if rr.Code != http.StatusNotFound {
		t.Fatalf("expected 404, got %d", rr.Code)
	}
}

type errReader struct{}

func (errReader) Read(p []byte) (int, error) { return 0, errors.New("boom") }

func TestPutReadErrorReturnsBadRequest(t *testing.T) {
	calRepo := &fakeCalendarRepo{
		accessible: []store.CalendarAccess{
			{Calendar: store.Calendar{ID: 1, UserID: 1, Name: "Work"}, Editor: true},
		},
	}
	h := &Handler{store: &store.Store{Calendars: calRepo, Events: &fakeEventRepo{}}}
	req := newCalendarPutRequest("/dav/calendars/1/e.ics", io.NopCloser(errReader{}))
	req = req.WithContext(auth.WithUser(req.Context(), &store.User{ID: 1}))
	rr := httptest.NewRecorder()
	h.Put(rr, req)
	if rr.Code != http.StatusBadRequest {
		t.Fatalf("expected 400 on read error, got %d", rr.Code)
	}
}

func TestPutUpdatesExistingContactReturnsNoContent(t *testing.T) {
	bookRepo := &fakeAddressBookRepo{
		books: map[int64]*store.AddressBook{
			5: {ID: 5, UserID: 1, Name: "Contacts"},
		},
	}
	contactRepo := &fakeContactRepo{
		contacts: map[string]*store.Contact{
			"5:alice": {AddressBookID: 5, UID: "alice", RawVCard: "BEGIN:VCARD\r\nVERSION:3.0\r\nFN:Alice\r\nEND:VCARD\r\n", ETag: "e"},
		},
	}
	h := &Handler{store: &store.Store{AddressBooks: bookRepo, Contacts: contactRepo}}
	newVCard := "BEGIN:VCARD\r\nVERSION:3.0\r\nFN:Alice Updated\r\nEMAIL:alice@example.com\r\nEND:VCARD\r\n"
	req := httptest.NewRequest(http.MethodPut, "/dav/addressbooks/5/alice.vcf", strings.NewReader(newVCard))
	req = req.WithContext(auth.WithUser(req.Context(), &store.User{ID: 1}))
	rr := httptest.NewRecorder()
	h.Put(rr, req)
	if rr.Code != http.StatusNoContent {
		t.Fatalf("expected 204, got %d: %s", rr.Code, rr.Body.String())
	}
}

func TestDeleteAddressBookNotFound(t *testing.T) {
	h := &Handler{store: &store.Store{AddressBooks: &fakeAddressBookRepo{}, Contacts: &fakeContactRepo{}}}
	req := httptest.NewRequest(http.MethodDelete, "/dav/addressbooks/9/alice.vcf", nil)
	req = req.WithContext(auth.WithUser(req.Context(), &store.User{ID: 1}))
	rr := httptest.NewRecorder()
	h.Delete(rr, req)
	if rr.Code != http.StatusNotFound {
		t.Fatalf("expected 404, got %d", rr.Code)
	}
}

func TestReportCalendarNotFound(t *testing.T) {
	calRepo := &fakeCalendarRepo{accessible: []store.CalendarAccess{}}
	h := &Handler{store: &store.Store{Calendars: calRepo, Events: &fakeEventRepo{}}}
	body := `<cal:calendar-query xmlns:cal="urn:ietf:params:xml:ns:caldav"/>`
	req := httptest.NewRequest("REPORT", "/dav/calendars/9/", strings.NewReader(body))
	req = req.WithContext(auth.WithUser(req.Context(), &store.User{ID: 1}))
	rr := httptest.NewRecorder()
	h.Report(rr, req)
	if rr.Code != http.StatusNotFound {
		t.Fatalf("expected 404, got %d", rr.Code)
	}
}

func TestReportAddressBookNotFound(t *testing.T) {
	bookRepo := &fakeAddressBookRepo{books: map[int64]*store.AddressBook{}}
	h := &Handler{store: &store.Store{AddressBooks: bookRepo, Contacts: &fakeContactRepo{}}}
	body := `<card:addressbook-query xmlns:card="urn:ietf:params:xml:ns:carddav"/>`
	req := httptest.NewRequest("REPORT", "/dav/addressbooks/9/", strings.NewReader(body))
	req = req.WithContext(auth.WithUser(req.Context(), &store.User{ID: 1}))
	rr := httptest.NewRecorder()
	h.Report(rr, req)
	if rr.Code != http.StatusNotFound {
		t.Fatalf("expected 404, got %d", rr.Code)
	}
}

func TestReportCalendarMultiGetPath(t *testing.T) {
	calRepo := &fakeCalendarRepo{
		accessible: []store.CalendarAccess{
			{Calendar: store.Calendar{ID: 2, UserID: 1, Name: "Work"}, Editor: true},
		},
	}
	eventRepo := &fakeEventRepo{
		events: map[string]*store.Event{
			"2:event": {CalendarID: 2, UID: "event", RawICAL: "ICAL", ETag: "e"},
		},
	}
	h := &Handler{store: &store.Store{Calendars: calRepo, Events: eventRepo}}
	body := `<cal:calendar-multiget xmlns:cal="urn:ietf:params:xml:ns:caldav"><D:href xmlns:D="DAV:">/dav/calendars/2/event.ics</D:href></cal:calendar-multiget>`
	req := httptest.NewRequest("REPORT", "/dav/calendars/2/", strings.NewReader(body))
	req = req.WithContext(auth.WithUser(req.Context(), &store.User{ID: 1}))
	rr := httptest.NewRecorder()
	h.Report(rr, req)
	if rr.Code != http.StatusMultiStatus {
		t.Fatalf("expected 207, got %d", rr.Code)
	}
	if !strings.Contains(rr.Body.String(), "event.ics") {
		t.Fatalf("expected event response, got %s", rr.Body.String())
	}
}

func TestReportAddressBookMultiGetPath(t *testing.T) {
	bookRepo := &fakeAddressBookRepo{
		books: map[int64]*store.AddressBook{
			3: {ID: 3, UserID: 1, Name: "Contacts"},
		},
	}
	contactRepo := &fakeContactRepo{
		contacts: map[string]*store.Contact{
			"3:alice": {AddressBookID: 3, UID: "alice", RawVCard: "VCARD", ETag: "e"},
		},
	}
	h := &Handler{store: &store.Store{AddressBooks: bookRepo, Contacts: contactRepo}}
	body := `<card:addressbook-multiget xmlns:card="urn:ietf:params:xml:ns:carddav"><D:href xmlns:D="DAV:">/dav/addressbooks/3/alice.vcf</D:href></card:addressbook-multiget>`
	req := httptest.NewRequest("REPORT", "/dav/addressbooks/3/", strings.NewReader(body))
	req = req.WithContext(auth.WithUser(req.Context(), &store.User{ID: 1}))
	rr := httptest.NewRecorder()
	h.Report(rr, req)
	if rr.Code != http.StatusMultiStatus {
		t.Fatalf("expected 207, got %d", rr.Code)
	}
	if !strings.Contains(rr.Body.String(), "alice.vcf") {
		t.Fatalf("expected contact response, got %s", rr.Body.String())
	}
}

func TestCalendarMultiGetReturnsErrorWhenRepoFails(t *testing.T) {
	brokenRepo := &errorEventRepo{}
	h := &Handler{store: &store.Store{Events: brokenRepo, DeletedResources: &fakeDeletedResourceRepo{}}}
	cal := &store.CalendarAccess{Calendar: store.Calendar{ID: 1}}
	_, err := h.calendarMultiGet(context.Background(), cal, []string{"/dav/calendars/1/e.ics"}, "/dav/calendars/1/", "/dav/calendars/1/", nil)
	if err == nil {
		t.Fatal("expected error from repo")
	}
}

type fakeEventRepo struct {
	events map[string]*store.Event
}

func (f *fakeEventRepo) key(calendarID int64, uid string) string {
	return fmt.Sprintf("%d:%s", calendarID, uid)
}

func (f *fakeEventRepo) Upsert(ctx context.Context, event store.Event) (*store.Event, error) {
	if f.events == nil {
		f.events = map[string]*store.Event{}
	}
	if event.ResourceName == "" {
		event.ResourceName = event.UID
	}
	copy := event
	f.events[f.key(event.CalendarID, event.UID)] = &copy
	return &copy, nil
}

func (f *fakeEventRepo) DeleteByUID(ctx context.Context, calendarID int64, uid string) error {
	delete(f.events, f.key(calendarID, uid))
	return nil
}

func (f *fakeEventRepo) GetByUID(ctx context.Context, calendarID int64, uid string) (*store.Event, error) {
	if ev, ok := f.events[f.key(calendarID, uid)]; ok {
		copy := *ev
		return &copy, nil
	}
	return nil, nil
}

func (f *fakeEventRepo) GetByResourceName(ctx context.Context, calendarID int64, resourceName string) (*store.Event, error) {
	for _, ev := range f.events {
		if ev.CalendarID != calendarID {
			continue
		}
		name := ev.ResourceName
		if name == "" {
			name = ev.UID
		}
		if name == resourceName {
			copy := *ev
			return &copy, nil
		}
	}
	return nil, nil
}

func (f *fakeEventRepo) ListForCalendar(ctx context.Context, calendarID int64) ([]store.Event, error) {
	var result []store.Event
	for _, ev := range f.events {
		if ev.CalendarID != calendarID {
			continue
		}
		copy := *ev
		result = append(result, copy)
	}
	return result, nil
}

func (f *fakeEventRepo) ListForCalendarPaginated(ctx context.Context, calendarID int64, limit, offset int) (*store.PaginatedResult[store.Event], error) {
	events, _ := f.ListForCalendar(ctx, calendarID)
	return &store.PaginatedResult[store.Event]{
		Items:      events,
		TotalCount: len(events),
		Limit:      limit,
		Offset:     offset,
	}, nil
}

func (f *fakeEventRepo) ListByUIDs(ctx context.Context, calendarID int64, uids []string) ([]store.Event, error) {
	return nil, nil
}

func (f *fakeEventRepo) ListModifiedSince(ctx context.Context, calendarID int64, since time.Time) ([]store.Event, error) {
	var result []store.Event
	for _, ev := range f.events {
		if ev.CalendarID != calendarID {
			continue
		}
		if ev.LastModified.After(since) {
			copy := *ev
			result = append(result, copy)
		}
	}
	return result, nil
}

func (f *fakeEventRepo) ListRecentByUser(ctx context.Context, userID int64, limit int) ([]store.Event, error) {
	return nil, nil
}

func (f *fakeEventRepo) MaxLastModified(ctx context.Context, calendarID int64) (time.Time, error) {
	var max time.Time
	for _, ev := range f.events {
		if ev.CalendarID != calendarID {
			continue
		}
		if ev.LastModified.After(max) {
			max = ev.LastModified
		}
	}
	return max, nil
}

type errorEventRepo struct{}

func (e *errorEventRepo) Upsert(ctx context.Context, event store.Event) (*store.Event, error) {
	return nil, errors.New("fail")
}

func (e *errorEventRepo) DeleteByUID(ctx context.Context, calendarID int64, uid string) error {
	return errors.New("fail")
}

func (e *errorEventRepo) GetByUID(ctx context.Context, calendarID int64, uid string) (*store.Event, error) {
	return nil, errors.New("fail")
}

func (e *errorEventRepo) GetByResourceName(ctx context.Context, calendarID int64, resourceName string) (*store.Event, error) {
	return nil, errors.New("fail")
}

func (e *errorEventRepo) ListForCalendar(ctx context.Context, calendarID int64) ([]store.Event, error) {
	return nil, errors.New("fail")
}

func (e *errorEventRepo) ListForCalendarPaginated(ctx context.Context, calendarID int64, limit, offset int) (*store.PaginatedResult[store.Event], error) {
	return nil, errors.New("fail")
}

func (e *errorEventRepo) ListByUIDs(ctx context.Context, calendarID int64, uids []string) ([]store.Event, error) {
	return nil, errors.New("fail")
}

func (e *errorEventRepo) ListModifiedSince(ctx context.Context, calendarID int64, since time.Time) ([]store.Event, error) {
	return nil, errors.New("fail")
}

func (e *errorEventRepo) ListRecentByUser(ctx context.Context, userID int64, limit int) ([]store.Event, error) {
	return nil, errors.New("fail")
}

func (e *errorEventRepo) MaxLastModified(ctx context.Context, calendarID int64) (time.Time, error) {
	return time.Time{}, errors.New("fail")
}

type fakeContactRepo struct {
	contacts map[string]*store.Contact
}

func (f *fakeContactRepo) key(bookID int64, uid string) string {
	return fmt.Sprintf("%d:%s", bookID, uid)
}

func (f *fakeContactRepo) Upsert(ctx context.Context, contact store.Contact) (*store.Contact, error) {
	if f.contacts == nil {
		f.contacts = map[string]*store.Contact{}
	}
	copy := contact
	f.contacts[f.key(contact.AddressBookID, contact.UID)] = &copy
	return &copy, nil
}

func (f *fakeContactRepo) DeleteByUID(ctx context.Context, addressBookID int64, uid string) error {
	delete(f.contacts, f.key(addressBookID, uid))
	return nil
}

func (f *fakeContactRepo) GetByUID(ctx context.Context, addressBookID int64, uid string) (*store.Contact, error) {
	if c, ok := f.contacts[f.key(addressBookID, uid)]; ok {
		copy := *c
		return &copy, nil
	}
	return nil, nil
}

func (f *fakeContactRepo) ListForBook(ctx context.Context, addressBookID int64) ([]store.Contact, error) {
	var result []store.Contact
	for _, c := range f.contacts {
		if c.AddressBookID != addressBookID {
			continue
		}
		copy := *c
		result = append(result, copy)
	}
	return result, nil
}

func (f *fakeContactRepo) ListForBookPaginated(ctx context.Context, addressBookID int64, limit, offset int) (*store.PaginatedResult[store.Contact], error) {
	contacts, _ := f.ListForBook(ctx, addressBookID)
	return &store.PaginatedResult[store.Contact]{
		Items:      contacts,
		TotalCount: len(contacts),
		Limit:      limit,
		Offset:     offset,
	}, nil
}

func (f *fakeContactRepo) ListByUIDs(ctx context.Context, addressBookID int64, uids []string) ([]store.Contact, error) {
	return nil, nil
}

func (f *fakeContactRepo) ListModifiedSince(ctx context.Context, addressBookID int64, since time.Time) ([]store.Contact, error) {
	var result []store.Contact
	for _, c := range f.contacts {
		if c.AddressBookID != addressBookID {
			continue
		}
		if c.LastModified.After(since) {
			copy := *c
			result = append(result, copy)
		}
	}
	return result, nil
}

func (f *fakeContactRepo) ListRecentByUser(ctx context.Context, userID int64, limit int) ([]store.Contact, error) {
	return nil, nil
}

func (f *fakeContactRepo) MaxLastModified(ctx context.Context, addressBookID int64) (time.Time, error) {
	var max time.Time
	for _, c := range f.contacts {
		if c.AddressBookID != addressBookID {
			continue
		}
		if c.LastModified.After(max) {
			max = c.LastModified
		}
	}
	return max, nil
}

func (f *fakeContactRepo) ListWithBirthdaysByUser(ctx context.Context, userID int64) ([]store.Contact, error) {
	if f.contacts == nil {
		return nil, nil
	}
	var result []store.Contact
	for _, c := range f.contacts {
		if c.Birthday != nil {
			result = append(result, *c)
		}
	}
	return result, nil
}

func (f *fakeContactRepo) MoveToAddressBook(ctx context.Context, fromAddressBookID, toAddressBookID int64, uid string) error {
	oldKey := f.key(fromAddressBookID, uid)
	contact, ok := f.contacts[oldKey]
	if !ok {
		return nil // Contact not found
	}
	// Remove from old location
	delete(f.contacts, oldKey)
	// Update address book ID
	contact.AddressBookID = toAddressBookID
	// Add to new location
	newKey := f.key(toAddressBookID, uid)
	f.contacts[newKey] = contact
	return nil
}

type fakeCalendarRepo struct {
	accessible []store.CalendarAccess
	calendars  map[int64]*store.Calendar
}

func (f *fakeCalendarRepo) GetByID(ctx context.Context, id int64) (*store.Calendar, error) {
	if f.calendars == nil {
		return nil, nil
	}
	return f.calendars[id], nil
}

func (f *fakeCalendarRepo) ListByUser(ctx context.Context, userID int64) ([]store.Calendar, error) {
	if f.calendars == nil {
		return nil, nil
	}
	var result []store.Calendar
	for _, cal := range f.calendars {
		if cal.UserID == userID {
			result = append(result, *cal)
		}
	}
	return result, nil
}

func (f *fakeCalendarRepo) ListAccessible(ctx context.Context, userID int64) ([]store.CalendarAccess, error) {
	return f.accessible, nil
}

func (f *fakeCalendarRepo) GetAccessible(ctx context.Context, calendarID, userID int64) (*store.CalendarAccess, error) {
	for _, c := range f.accessible {
		if c.ID == calendarID {
			copy := c
			return &copy, nil
		}
	}
	return nil, nil
}

func (f *fakeCalendarRepo) Create(ctx context.Context, cal store.Calendar) (*store.Calendar, error) {
	if f.calendars == nil {
		f.calendars = map[int64]*store.Calendar{}
	}
	cal.ID = int64(len(f.calendars) + 1)
	copy := cal
	f.calendars[copy.ID] = &copy
	if cal.UserID != 0 {
		f.accessible = append(f.accessible, store.CalendarAccess{Calendar: copy, Editor: true})
	}
	return &copy, nil
}

func (f *fakeCalendarRepo) Update(ctx context.Context, userID, id int64, name string, description, timezone *string) error {
	return nil
}

func (f *fakeCalendarRepo) Rename(ctx context.Context, userID, id int64, name string) error {
	return nil
}

func (f *fakeCalendarRepo) Delete(ctx context.Context, userID, id int64) error {
	return nil
}

type fakeAddressBookRepo struct {
	books map[int64]*store.AddressBook
}

func (f *fakeAddressBookRepo) GetByID(ctx context.Context, id int64) (*store.AddressBook, error) {
	if f.books == nil {
		return nil, nil
	}
	return f.books[id], nil
}

func (f *fakeAddressBookRepo) ListByUser(ctx context.Context, userID int64) ([]store.AddressBook, error) {
	var res []store.AddressBook
	for _, b := range f.books {
		if b.UserID == userID {
			copy := *b
			res = append(res, copy)
		}
	}
	return res, nil
}

func (f *fakeAddressBookRepo) Create(ctx context.Context, book store.AddressBook) (*store.AddressBook, error) {
	if f.books == nil {
		f.books = map[int64]*store.AddressBook{}
	}
	book.ID = int64(len(f.books) + 1)
	copy := book
	f.books[copy.ID] = &copy
	return &copy, nil
}

func (f *fakeAddressBookRepo) Update(ctx context.Context, userID, id int64, name string, description *string) error {
	return nil
}

func (f *fakeAddressBookRepo) Rename(ctx context.Context, userID, id int64, name string) error {
	return nil
}

func (f *fakeAddressBookRepo) Delete(ctx context.Context, userID, id int64) error {
	return nil
}

type fakeDeletedResourceRepo struct {
	deleted []store.DeletedResource
}

func (f *fakeDeletedResourceRepo) ListDeletedSince(ctx context.Context, resourceType string, collectionID int64, since time.Time) ([]store.DeletedResource, error) {
	var result []store.DeletedResource
	for _, d := range f.deleted {
		if d.ResourceType == resourceType && d.CollectionID == collectionID && d.DeletedAt.After(since) {
			result = append(result, d)
		}
	}
	return result, nil
}

func (f *fakeDeletedResourceRepo) Cleanup(ctx context.Context, olderThan time.Duration) (int64, error) {
	return 0, nil
}

func TestCalendarDataUseCDATA(t *testing.T) {
	ps := etagProp("abc123", "BEGIN:VCALENDAR\r\nEND:VCALENDAR\r\n", true)
	resp := response{Href: "/test.ics", Propstat: []propstat{ps}}
	payload := multistatus{
		XmlnsD:   "DAV:",
		XmlnsC:   "urn:ietf:params:xml:ns:caldav",
		XmlnsA:   "urn:ietf:params:xml:ns:carddav",
		Response: []response{resp},
	}
	out, err := xml.Marshal(payload)
	if err != nil {
		t.Fatalf("failed to marshal: %v", err)
	}
	xmlStr := string(out)
	// Check that calendar data is wrapped in CDATA and contains raw CRLF
	if !strings.Contains(xmlStr, "<![CDATA[BEGIN:VCALENDAR") {
		t.Errorf("expected CDATA wrapper, got: %s", xmlStr)
	}
	// Check that CRLF is not escaped
	if strings.Contains(xmlStr, "&#xD;") || strings.Contains(xmlStr, "&#xA;") {
		t.Errorf("CRLF should not be escaped, got: %s", xmlStr)
	}
}

func TestReportRequestParsesDifferentNamespacePrefixes(t *testing.T) {
	tests := []struct {
		name     string
		body     string
		wantType string
		wantHref string
	}{
		{
			name: "lowercase d prefix",
			body: `<?xml version="1.0" encoding="UTF-8"?>
<c:calendar-multiget xmlns:d="DAV:" xmlns:c="urn:ietf:params:xml:ns:caldav">
  <d:href>/dav/calendars/1/event1.ics</d:href>
</c:calendar-multiget>`,
			wantType: "calendar-multiget",
			wantHref: "/dav/calendars/1/event1.ics",
		},
		{
			name: "uppercase D prefix (Thunderbird style)",
			body: `<?xml version="1.0" encoding="UTF-8"?>
<C:calendar-multiget xmlns:D="DAV:" xmlns:C="urn:ietf:params:xml:ns:caldav">
  <D:href>/dav/calendars/1/event2.ics</D:href>
</C:calendar-multiget>`,
			wantType: "calendar-multiget",
			wantHref: "/dav/calendars/1/event2.ics",
		},
		{
			name: "sync-collection with sync-token",
			body: `<?xml version="1.0" encoding="UTF-8"?>
<D:sync-collection xmlns:D="DAV:">
  <D:sync-token>urn:calcard-sync:cal:1:0</D:sync-token>
</D:sync-collection>`,
			wantType: "sync-collection",
			wantHref: "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var report reportRequest
			if err := xml.Unmarshal([]byte(tt.body), &report); err != nil {
				t.Fatalf("failed to unmarshal: %v", err)
			}
			if report.XMLName.Local != tt.wantType {
				t.Errorf("XMLName.Local = %q, want %q", report.XMLName.Local, tt.wantType)
			}
			if tt.wantHref != "" {
				if len(report.Hrefs) == 0 {
					t.Fatalf("expected hrefs, got none")
				}
				if report.Hrefs[0] != tt.wantHref {
					t.Errorf("Hrefs[0] = %q, want %q", report.Hrefs[0], tt.wantHref)
				}
			}
		})
	}
}

func TestCalendarQueryWithCompFilter(t *testing.T) {
	calRepo := &fakeCalendarRepo{
		accessible: []store.CalendarAccess{
			{Calendar: store.Calendar{ID: 1, UserID: 1, Name: "Test"}, Editor: true},
		},
	}
	eventRepo := &fakeEventRepo{
		events: map[string]*store.Event{
			"1:event1": {CalendarID: 1, UID: "event1", RawICAL: "BEGIN:VCALENDAR\r\nBEGIN:VEVENT\r\nUID:event1\r\nEND:VEVENT\r\nEND:VCALENDAR\r\n", ETag: "e1"},
			"1:todo1":  {CalendarID: 1, UID: "todo1", RawICAL: "BEGIN:VCALENDAR\r\nBEGIN:VTODO\r\nUID:todo1\r\nEND:VTODO\r\nEND:VCALENDAR\r\n", ETag: "t1"},
		},
	}
	h := &Handler{store: &store.Store{Calendars: calRepo, Events: eventRepo}}

	// Filter for VEVENT only
	body := `<cal:calendar-query xmlns:cal="urn:ietf:params:xml:ns:caldav">
		<cal:filter>
			<cal:comp-filter name="VEVENT"/>
		</cal:filter>
	</cal:calendar-query>`

	req := httptest.NewRequest("REPORT", "/dav/calendars/1/", strings.NewReader(body))
	req = req.WithContext(auth.WithUser(req.Context(), &store.User{ID: 1}))
	rr := httptest.NewRecorder()

	h.Report(rr, req)

	if rr.Code != http.StatusMultiStatus {
		t.Fatalf("expected 207, got %d", rr.Code)
	}
	respBody := rr.Body.String()
	if !strings.Contains(respBody, "event1.ics") {
		t.Errorf("expected event1 in response, got %s", respBody)
	}
	if strings.Contains(respBody, "todo1.ics") {
		t.Errorf("should not include todo1 (filtered out), got %s", respBody)
	}
}

func TestCalendarQueryWithTimeRangeFilter(t *testing.T) {
	start := time.Date(2024, 6, 1, 10, 0, 0, 0, time.UTC)
	end := time.Date(2024, 6, 1, 12, 0, 0, 0, time.UTC)

	calRepo := &fakeCalendarRepo{
		accessible: []store.CalendarAccess{
			{Calendar: store.Calendar{ID: 1, UserID: 1, Name: "Test"}, Editor: true},
		},
	}
	eventRepo := &fakeEventRepo{
		events: map[string]*store.Event{
			"1:in-range":  {CalendarID: 1, UID: "in-range", RawICAL: "BEGIN:VCALENDAR\r\nBEGIN:VEVENT\r\nUID:in-range\r\nEND:VEVENT\r\nEND:VCALENDAR\r\n", ETag: "e1", DTStart: &start, DTEnd: &end},
			"1:out-range": {CalendarID: 1, UID: "out-range", RawICAL: "BEGIN:VCALENDAR\r\nBEGIN:VEVENT\r\nUID:out-range\r\nEND:VEVENT\r\nEND:VCALENDAR\r\n", ETag: "e2", DTStart: ptrTime(time.Date(2024, 7, 1, 10, 0, 0, 0, time.UTC)), DTEnd: ptrTime(time.Date(2024, 7, 1, 12, 0, 0, 0, time.UTC))},
		},
	}
	h := &Handler{store: &store.Store{Calendars: calRepo, Events: eventRepo}}

	// Query for events in June 2024 - filter should apply to VEVENT component
	body := `<cal:calendar-query xmlns:cal="urn:ietf:params:xml:ns:caldav">
		<cal:filter>
			<cal:comp-filter name="VEVENT">
				<cal:time-range start="20240601T000000Z" end="20240630T235959Z"/>
			</cal:comp-filter>
		</cal:filter>
	</cal:calendar-query>`

	req := httptest.NewRequest("REPORT", "/dav/calendars/1/", strings.NewReader(body))
	req = req.WithContext(auth.WithUser(req.Context(), &store.User{ID: 1}))
	rr := httptest.NewRecorder()

	h.Report(rr, req)

	if rr.Code != http.StatusMultiStatus {
		t.Fatalf("expected 207, got %d", rr.Code)
	}
	respBody := rr.Body.String()
	if !strings.Contains(respBody, "in-range.ics") {
		t.Errorf("expected in-range event, got %s", respBody)
	}
	if strings.Contains(respBody, "out-range.ics") {
		t.Errorf("should not include out-range event, got %s", respBody)
	}
}

func TestPropfindIncludesSupportedCalendarComponentSet(t *testing.T) {
	now := store.Now()
	calRepo := &fakeCalendarRepo{
		accessible: []store.CalendarAccess{
			{Calendar: store.Calendar{ID: 2, UserID: 1, Name: "Work", CTag: 5, UpdatedAt: now}, Editor: true},
		},
	}
	h := &Handler{store: &store.Store{Calendars: calRepo, Events: &fakeEventRepo{}}}
	u := &store.User{ID: 1}

	req := httptest.NewRequest("PROPFIND", "/dav/calendars/2/", nil)
	req.Header.Set("Depth", "0")
	req = req.WithContext(auth.WithUser(req.Context(), u))
	rr := httptest.NewRecorder()

	h.Propfind(rr, req)

	if rr.Code != http.StatusMultiStatus {
		t.Fatalf("expected 207, got %d", rr.Code)
	}
	body := rr.Body.String()
	if !strings.Contains(body, "<cal:supported-calendar-component-set>") {
		t.Errorf("missing supported-calendar-component-set: %s", body)
	}
	if !strings.Contains(body, `<cal:comp name="VEVENT"`) {
		t.Errorf("missing VEVENT component: %s", body)
	}
	if !strings.Contains(body, `<cal:comp name="VTODO"`) {
		t.Errorf("missing VTODO component: %s", body)
	}
	if !strings.Contains(body, `<cal:comp name="VFREEBUSY"`) {
		t.Errorf("missing VFREEBUSY component: %s", body)
	}
}

func TestPutWithIfMatchSuccess(t *testing.T) {
	calRepo := &fakeCalendarRepo{
		accessible: []store.CalendarAccess{
			{Calendar: store.Calendar{ID: 2, UserID: 1, Name: "Work"}, Editor: true},
		},
	}
	eventRepo := &fakeEventRepo{
		events: map[string]*store.Event{
			"2:event": {CalendarID: 2, UID: "event", RawICAL: "OLD", ETag: "old-etag"},
		},
	}
	h := &Handler{store: &store.Store{Calendars: calRepo, Events: eventRepo}}

	icalData := "BEGIN:VCALENDAR\r\nVERSION:2.0\r\nBEGIN:VEVENT\r\nUID:event\r\nEND:VEVENT\r\nEND:VCALENDAR\r\n"
	req := newCalendarPutRequest("/dav/calendars/2/event.ics", strings.NewReader(icalData))
	req.Header.Set("If-Match", `"old-etag"`)
	req = req.WithContext(auth.WithUser(req.Context(), &store.User{ID: 1}))
	rr := httptest.NewRecorder()

	h.Put(rr, req)

	if rr.Code != http.StatusNoContent {
		t.Errorf("expected 204, got %d", rr.Code)
	}
}

func TestPutWithIfMatchFailure(t *testing.T) {
	calRepo := &fakeCalendarRepo{
		accessible: []store.CalendarAccess{
			{Calendar: store.Calendar{ID: 2, UserID: 1, Name: "Work"}, Editor: true},
		},
	}
	eventRepo := &fakeEventRepo{
		events: map[string]*store.Event{
			"2:event": {CalendarID: 2, UID: "event", RawICAL: "OLD", ETag: "old-etag"},
		},
	}
	h := &Handler{store: &store.Store{Calendars: calRepo, Events: eventRepo}}

	icalData := "BEGIN:VCALENDAR\r\nVERSION:2.0\r\nBEGIN:VEVENT\r\nUID:event\r\nEND:VEVENT\r\nEND:VCALENDAR\r\n"
	req := newCalendarPutRequest("/dav/calendars/2/event.ics", strings.NewReader(icalData))
	req.Header.Set("If-Match", `"wrong-etag"`)
	req = req.WithContext(auth.WithUser(req.Context(), &store.User{ID: 1}))
	rr := httptest.NewRecorder()

	h.Put(rr, req)

	if rr.Code != http.StatusPreconditionFailed {
		t.Errorf("expected 412, got %d", rr.Code)
	}
}

func TestPutWithIfNoneMatchStar(t *testing.T) {
	calRepo := &fakeCalendarRepo{
		accessible: []store.CalendarAccess{
			{Calendar: store.Calendar{ID: 2, UserID: 1, Name: "Work"}, Editor: true},
		},
	}
	eventRepo := &fakeEventRepo{events: map[string]*store.Event{}}
	h := &Handler{store: &store.Store{Calendars: calRepo, Events: eventRepo}}

	icalData := "BEGIN:VCALENDAR\r\nVERSION:2.0\r\nBEGIN:VEVENT\r\nUID:new\r\nEND:VEVENT\r\nEND:VCALENDAR\r\n"
	req := newCalendarPutRequest("/dav/calendars/2/new.ics", strings.NewReader(icalData))
	req.Header.Set("If-None-Match", "*")
	req = req.WithContext(auth.WithUser(req.Context(), &store.User{ID: 1}))
	rr := httptest.NewRecorder()

	h.Put(rr, req)

	if rr.Code != http.StatusCreated {
		t.Errorf("expected 201, got %d", rr.Code)
	}
}

func TestPutWithIfNoneMatchStarFailsIfExists(t *testing.T) {
	calRepo := &fakeCalendarRepo{
		accessible: []store.CalendarAccess{
			{Calendar: store.Calendar{ID: 2, UserID: 1, Name: "Work"}, Editor: true},
		},
	}
	eventRepo := &fakeEventRepo{
		events: map[string]*store.Event{
			"2:event": {CalendarID: 2, UID: "event", RawICAL: "OLD", ETag: "old"},
		},
	}
	h := &Handler{store: &store.Store{Calendars: calRepo, Events: eventRepo}}

	icalData := "BEGIN:VCALENDAR\r\nVERSION:2.0\r\nBEGIN:VEVENT\r\nUID:event\r\nEND:VEVENT\r\nEND:VCALENDAR\r\n"
	req := newCalendarPutRequest("/dav/calendars/2/event.ics", strings.NewReader(icalData))
	req.Header.Set("If-None-Match", "*")
	req = req.WithContext(auth.WithUser(req.Context(), &store.User{ID: 1}))
	rr := httptest.NewRecorder()

	h.Put(rr, req)

	if rr.Code != http.StatusPreconditionFailed {
		t.Errorf("expected 412, got %d", rr.Code)
	}
}

func TestDeleteWithIfMatchSuccess(t *testing.T) {
	calRepo := &fakeCalendarRepo{
		accessible: []store.CalendarAccess{
			{Calendar: store.Calendar{ID: 2, UserID: 1, Name: "Work"}, Editor: true},
		},
	}
	eventRepo := &fakeEventRepo{
		events: map[string]*store.Event{
			"2:event": {CalendarID: 2, UID: "event", RawICAL: "ICAL", ETag: "current"},
		},
	}
	h := &Handler{store: &store.Store{Calendars: calRepo, Events: eventRepo}}

	req := httptest.NewRequest(http.MethodDelete, "/dav/calendars/2/event.ics", nil)
	req.Header.Set("If-Match", `"current"`)
	req = req.WithContext(auth.WithUser(req.Context(), &store.User{ID: 1}))
	rr := httptest.NewRecorder()

	h.Delete(rr, req)

	if rr.Code != http.StatusNoContent {
		t.Errorf("expected 204, got %d", rr.Code)
	}
}

func TestDeleteWithIfMatchFailure(t *testing.T) {
	calRepo := &fakeCalendarRepo{
		accessible: []store.CalendarAccess{
			{Calendar: store.Calendar{ID: 2, UserID: 1, Name: "Work"}, Editor: true},
		},
	}
	eventRepo := &fakeEventRepo{
		events: map[string]*store.Event{
			"2:event": {CalendarID: 2, UID: "event", RawICAL: "ICAL", ETag: "current"},
		},
	}
	h := &Handler{store: &store.Store{Calendars: calRepo, Events: eventRepo}}

	req := httptest.NewRequest(http.MethodDelete, "/dav/calendars/2/event.ics", nil)
	req.Header.Set("If-Match", `"wrong"`)
	req = req.WithContext(auth.WithUser(req.Context(), &store.User{ID: 1}))
	rr := httptest.NewRecorder()

	h.Delete(rr, req)

	if rr.Code != http.StatusPreconditionFailed {
		t.Errorf("expected 412, got %d", rr.Code)
	}
}

func TestPutRejectsInvalidICalendar(t *testing.T) {
	calRepo := &fakeCalendarRepo{
		accessible: []store.CalendarAccess{
			{Calendar: store.Calendar{ID: 2, UserID: 1, Name: "Work"}, Editor: true},
		},
	}
	h := &Handler{store: &store.Store{Calendars: calRepo, Events: &fakeEventRepo{}}}

	tests := []struct {
		name string
		data string
	}{
		{"missing BEGIN:VCALENDAR", "BEGIN:VEVENT\r\nEND:VEVENT\r\n"},
		{"missing END:VCALENDAR", "BEGIN:VCALENDAR\r\nBEGIN:VEVENT\r\nEND:VEVENT\r\n"},
		{"no component", "BEGIN:VCALENDAR\r\nEND:VCALENDAR\r\n"},
		{"unbalanced tags", "BEGIN:VCALENDAR\r\nBEGIN:VCALENDAR\r\nBEGIN:VEVENT\r\nEND:VEVENT\r\nEND:VCALENDAR\r\n"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req := newCalendarPutRequest("/dav/calendars/2/event.ics", strings.NewReader(tt.data))
			req = req.WithContext(auth.WithUser(req.Context(), &store.User{ID: 1}))
			rr := httptest.NewRecorder()

			h.Put(rr, req)

			if rr.Code != http.StatusBadRequest {
				t.Errorf("expected 400 for invalid iCalendar, got %d", rr.Code)
			}
			if !strings.Contains(rr.Body.String(), "valid-calendar-data") {
				t.Errorf("expected CalDAV error body for invalid calendar data, got %s", rr.Body.String())
			}
		})
	}
}

func TestPutAcceptsValidICalendar(t *testing.T) {
	calRepo := &fakeCalendarRepo{
		accessible: []store.CalendarAccess{
			{Calendar: store.Calendar{ID: 2, UserID: 1, Name: "Work"}, Editor: true},
		},
	}
	h := &Handler{store: &store.Store{Calendars: calRepo, Events: &fakeEventRepo{}}}

	validData := "BEGIN:VCALENDAR\r\nVERSION:2.0\r\nBEGIN:VEVENT\r\nUID:test\r\nDTSTART:20240601T100000Z\r\nEND:VEVENT\r\nEND:VCALENDAR\r\n"
	req := newCalendarPutRequest("/dav/calendars/2/test.ics", strings.NewReader(validData))
	req = req.WithContext(auth.WithUser(req.Context(), &store.User{ID: 1}))
	rr := httptest.NewRecorder()

	h.Put(rr, req)

	if rr.Code != http.StatusCreated {
		t.Errorf("expected 201 for valid iCalendar, got %d: %s", rr.Code, rr.Body.String())
	}
}

func TestValidateICalendarDetectsVEVENT(t *testing.T) {
	h := &Handler{}
	data := "BEGIN:VCALENDAR\r\nBEGIN:VEVENT\r\nUID:test\r\nEND:VEVENT\r\nEND:VCALENDAR\r\n"
	if err := h.validateICalendar(data); err != nil {
		t.Fatalf("expected valid iCalendar, got error: %v", err)
	}
}

func TestPutRejectsInvalidVCard(t *testing.T) {
	bookRepo := &fakeAddressBookRepo{
		books: map[int64]*store.AddressBook{
			5: {ID: 5, UserID: 1, Name: "Contacts"},
		},
	}
	h := &Handler{store: &store.Store{AddressBooks: bookRepo, Contacts: &fakeContactRepo{}}}

	invalidData := "NOT A VCARD"
	req := httptest.NewRequest(http.MethodPut, "/dav/addressbooks/5/alice.vcf", strings.NewReader(invalidData))
	req = req.WithContext(auth.WithUser(req.Context(), &store.User{ID: 1}))
	rr := httptest.NewRecorder()

	h.Put(rr, req)

	if rr.Code != http.StatusBadRequest {
		t.Errorf("expected 400 for invalid vCard, got %d", rr.Code)
	}
	if !strings.Contains(rr.Body.String(), "invalid vCard") {
		t.Errorf("expected error message about invalid vCard, got %s", rr.Body.String())
	}
}

func TestPutAcceptsValidVCard(t *testing.T) {
	bookRepo := &fakeAddressBookRepo{
		books: map[int64]*store.AddressBook{
			5: {ID: 5, UserID: 1, Name: "Contacts"},
		},
	}
	h := &Handler{store: &store.Store{AddressBooks: bookRepo, Contacts: &fakeContactRepo{}}}

	validData := "BEGIN:VCARD\r\nVERSION:3.0\r\nFN:Alice\r\nEND:VCARD\r\n"
	req := httptest.NewRequest(http.MethodPut, "/dav/addressbooks/5/alice.vcf", strings.NewReader(validData))
	req = req.WithContext(auth.WithUser(req.Context(), &store.User{ID: 1}))
	rr := httptest.NewRecorder()

	h.Put(rr, req)

	if rr.Code != http.StatusCreated {
		t.Errorf("expected 201 for valid vCard, got %d: %s", rr.Code, rr.Body.String())
	}
}

func TestParseICalDateTime(t *testing.T) {
	tests := []struct {
		input    string
		wantErr  bool
		wantYear int
	}{
		{"20240601T100000Z", false, 2024},
		{"20240601T100000", false, 2024},
		{"2024-06-01T10:00:00Z", false, 2024},
		{"2024-06-01T10:00:00", false, 2024},
		{"invalid", true, 0},
		{"", true, 0},
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			result, err := parseICalDateTime(tt.input)
			if tt.wantErr {
				if err == nil {
					t.Errorf("expected error for %q", tt.input)
				}
			} else {
				if err != nil {
					t.Errorf("unexpected error for %q: %v", tt.input, err)
				}
				if result.Year() != tt.wantYear {
					t.Errorf("expected year %d, got %d", tt.wantYear, result.Year())
				}
			}
		})
	}
}

func ptrTime(t time.Time) *time.Time {
	return &t
}

// Tests for new medium priority features

func TestFreeBusyQueryReport(t *testing.T) {
	start := time.Date(2024, 6, 1, 10, 0, 0, 0, time.UTC)
	end := time.Date(2024, 6, 1, 12, 0, 0, 0, time.UTC)

	calRepo := &fakeCalendarRepo{
		accessible: []store.CalendarAccess{
			{Calendar: store.Calendar{ID: 1, UserID: 1, Name: "Test"}, Editor: true},
		},
	}
	eventRepo := &fakeEventRepo{
		events: map[string]*store.Event{
			"1:event1": {CalendarID: 1, UID: "event1", RawICAL: "BEGIN:VCALENDAR\r\nBEGIN:VEVENT\r\nUID:event1\r\nEND:VEVENT\r\nEND:VCALENDAR\r\n", ETag: "e1", DTStart: &start, DTEnd: &end},
		},
	}
	h := &Handler{store: &store.Store{Calendars: calRepo, Events: eventRepo}}

	body := `<cal:free-busy-query xmlns:cal="urn:ietf:params:xml:ns:caldav">
		<cal:filter>
			<cal:comp-filter name="VEVENT">
				<cal:time-range start="20240601T000000Z" end="20240630T235959Z"/>
			</cal:comp-filter>
		</cal:filter>
	</cal:free-busy-query>`

	req := httptest.NewRequest("REPORT", "/dav/calendars/1/", strings.NewReader(body))
	req = req.WithContext(auth.WithUser(req.Context(), &store.User{ID: 1}))
	rr := httptest.NewRecorder()

	h.Report(rr, req)

	if rr.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rr.Code)
	}
	if rr.Header().Get("Content-Type") != "text/calendar" {
		t.Fatalf("expected text/calendar, got %s", rr.Header().Get("Content-Type"))
	}
	respBody := rr.Body.String()
	if !strings.Contains(respBody, "VFREEBUSY") {
		t.Errorf("expected VFREEBUSY in response, got %s", respBody)
	}
	if !strings.Contains(respBody, "FREEBUSY:") {
		t.Errorf("expected FREEBUSY property in response, got %s", respBody)
	}
}

func TestPropfindParsesRequestBody(t *testing.T) {
	calRepo := &fakeCalendarRepo{
		accessible: []store.CalendarAccess{
			{Calendar: store.Calendar{ID: 2, UserID: 1, Name: "Work"}, Editor: true},
		},
	}
	h := &Handler{store: &store.Store{Calendars: calRepo, Events: &fakeEventRepo{}}}
	u := &store.User{ID: 1}

	body := `<?xml version="1.0" encoding="utf-8" ?>
<D:propfind xmlns:D="DAV:" xmlns:C="urn:ietf:params:xml:ns:caldav">
  <D:prop>
    <D:displayname/>
    <D:resourcetype/>
  </D:prop>
</D:propfind>`

	req := httptest.NewRequest("PROPFIND", "/dav/calendars/2/", strings.NewReader(body))
	req.Header.Set("Depth", "0")
	req = req.WithContext(auth.WithUser(req.Context(), u))
	rr := httptest.NewRecorder()

	h.Propfind(rr, req)

	if rr.Code != http.StatusMultiStatus {
		t.Fatalf("expected 207, got %d: %s", rr.Code, rr.Body.String())
	}
	// Response should contain displayname and resourcetype
	respBody := rr.Body.String()
	if !strings.Contains(respBody, "displayname") {
		t.Errorf("expected displayname in response, got %s", respBody)
	}
}

func TestPropfindParsesChunkedRequestBody(t *testing.T) {
	calRepo := &fakeCalendarRepo{
		accessible: []store.CalendarAccess{
			{Calendar: store.Calendar{ID: 2, UserID: 1, Name: "Work"}, Editor: true},
		},
	}
	h := &Handler{store: &store.Store{Calendars: calRepo, Events: &fakeEventRepo{}}}
	u := &store.User{ID: 1}

	body := `<?xml version="1.0" encoding="utf-8" ?>
<D:propfind xmlns:D="DAV:" xmlns:C="urn:ietf:params:xml:ns:caldav">
  <D:prop>
    <D:displayname/>
    <D:resourcetype/>
  </D:prop>
</D:propfind>`

	req := httptest.NewRequest("PROPFIND", "/dav/calendars/2/", strings.NewReader(body))
	req.Header.Set("Depth", "0")
	req.ContentLength = -1
	req = req.WithContext(auth.WithUser(req.Context(), u))
	rr := httptest.NewRecorder()

	h.Propfind(rr, req)

	if rr.Code != http.StatusMultiStatus {
		t.Fatalf("expected 207, got %d: %s", rr.Code, rr.Body.String())
	}
	respBody := rr.Body.String()
	if !strings.Contains(respBody, "displayname") {
		t.Errorf("expected displayname in response, got %s", respBody)
	}
}

func TestProppatchAddressBookUpdatesProperties(t *testing.T) {
	bookRepo := &fakeAddressBookRepo{
		books: map[int64]*store.AddressBook{
			5: {ID: 5, UserID: 1, Name: "Old Name"},
		},
	}
	h := &Handler{store: &store.Store{AddressBooks: bookRepo}}
	u := &store.User{ID: 1}

	body := `<?xml version="1.0" encoding="utf-8" ?>
<D:propertyupdate xmlns:D="DAV:" xmlns:C="urn:ietf:params:xml:ns:carddav">
  <D:set>
    <D:prop>
      <D:displayname>Updated Name</D:displayname>
    </D:prop>
  </D:set>
</D:propertyupdate>`

	req := httptest.NewRequest("PROPPATCH", "/dav/addressbooks/5", strings.NewReader(body))
	req = req.WithContext(auth.WithUser(req.Context(), u))
	rr := httptest.NewRecorder()

	h.Proppatch(rr, req)

	if rr.Code != http.StatusMultiStatus {
		t.Fatalf("expected 207, got %d: %s", rr.Code, rr.Body.String())
	}
	respBody := rr.Body.String()
	if !strings.Contains(respBody, "200 OK") {
		t.Errorf("expected success status in response, got %s", respBody)
	}
}

func TestCalendarPropertiesIncludeLimits(t *testing.T) {
	now := store.Now()
	calRepo := &fakeCalendarRepo{
		accessible: []store.CalendarAccess{
			{Calendar: store.Calendar{ID: 2, UserID: 1, Name: "Work", CTag: 5, UpdatedAt: now}, Editor: true},
		},
	}
	h := &Handler{store: &store.Store{Calendars: calRepo, Events: &fakeEventRepo{}}}
	u := &store.User{ID: 1}

	req := httptest.NewRequest("PROPFIND", "/dav/calendars/2/", nil)
	req.Header.Set("Depth", "0")
	req = req.WithContext(auth.WithUser(req.Context(), u))
	rr := httptest.NewRecorder()

	h.Propfind(rr, req)

	if rr.Code != http.StatusMultiStatus {
		t.Fatalf("expected 207, got %d", rr.Code)
	}
	body := rr.Body.String()

	// Check for required calendar properties
	expectedProps := []string{
		"<cal:max-resource-size>",
		"<cal:min-date-time>",
		"<cal:max-date-time>",
		"<cal:max-instances>",
		"<cal:max-attendees-per-instance>",
	}

	for _, prop := range expectedProps {
		if !strings.Contains(body, prop) {
			t.Errorf("missing required property %s in response: %s", prop, body)
		}
	}
}

func TestProppatchRejectsForbidden(t *testing.T) {
	calRepo := &fakeCalendarRepo{
		accessible: []store.CalendarAccess{
			{Calendar: store.Calendar{ID: 2, UserID: 2, Name: "Not Mine"}, Editor: false},
		},
	}
	h := &Handler{store: &store.Store{Calendars: calRepo}}
	u := &store.User{ID: 1}

	body := `<?xml version="1.0" encoding="utf-8" ?>
<D:propertyupdate xmlns:D="DAV:">
  <D:set>
    <D:prop>
      <D:displayname>Hacked</D:displayname>
    </D:prop>
  </D:set>
</D:propertyupdate>`

	req := httptest.NewRequest("PROPPATCH", "/dav/calendars/2", strings.NewReader(body))
	req = req.WithContext(auth.WithUser(req.Context(), u))
	rr := httptest.NewRecorder()

	h.Proppatch(rr, req)

	if rr.Code != http.StatusMultiStatus {
		t.Fatalf("expected 207, got %d", rr.Code)
	}
	respBody := rr.Body.String()
	if !strings.Contains(respBody, "403 Forbidden") {
		t.Errorf("expected 403 Forbidden for non-editor, got %s", respBody)
	}
}

func TestFreeBusyIncludesDateRange(t *testing.T) {
	start := time.Date(2024, 6, 1, 10, 0, 0, 0, time.UTC)
	end := time.Date(2024, 6, 1, 12, 0, 0, 0, time.UTC)

	calRepo := &fakeCalendarRepo{
		accessible: []store.CalendarAccess{
			{Calendar: store.Calendar{ID: 1, UserID: 1, Name: "Test"}, Editor: true},
		},
	}
	eventRepo := &fakeEventRepo{
		events: map[string]*store.Event{
			"1:event1": {CalendarID: 1, UID: "event1", RawICAL: "ICAL", ETag: "e1", DTStart: &start, DTEnd: &end},
		},
	}
	h := &Handler{store: &store.Store{Calendars: calRepo, Events: eventRepo}}

	body := `<cal:free-busy-query xmlns:cal="urn:ietf:params:xml:ns:caldav">
		<cal:filter>
			<cal:comp-filter name="VEVENT">
				<cal:time-range start="20240601T000000Z" end="20240630T235959Z"/>
			</cal:comp-filter>
		</cal:filter>
	</cal:free-busy-query>`

	req := httptest.NewRequest("REPORT", "/dav/calendars/1/", strings.NewReader(body))
	req = req.WithContext(auth.WithUser(req.Context(), &store.User{ID: 1}))
	rr := httptest.NewRecorder()

	h.Report(rr, req)

	respBody := rr.Body.String()
	if !strings.Contains(respBody, "DTSTART:20240601T000000Z") {
		t.Errorf("expected DTSTART in freebusy, got %s", respBody)
	}
	if !strings.Contains(respBody, "DTEND:20240630T235959Z") {
		t.Errorf("expected DTEND in freebusy, got %s", respBody)
	}
}

func TestReportRejectsCalendarResourcePath(t *testing.T) {
	calRepo := &fakeCalendarRepo{
		accessible: []store.CalendarAccess{
			{Calendar: store.Calendar{ID: 1, UserID: 1, Name: "Test"}, Editor: true},
		},
	}
	h := &Handler{store: &store.Store{Calendars: calRepo, Events: &fakeEventRepo{}}}

	testCases := []struct {
		name         string
		path         string
		reportType   string
		errorMessage string
	}{
		{
			name:         "calendar-query on resource path",
			path:         "/dav/calendars/1/event.ics",
			reportType:   "calendar-query",
			errorMessage: "calendar reports not allowed on calendar object resources",
		},
		{
			name:         "sync-collection on resource path",
			path:         "/dav/calendars/1/event.ics",
			reportType:   "sync-collection",
			errorMessage: "REPORT not allowed on calendar object resources",
		},
		{
			name:         "free-busy-query on resource path",
			path:         "/dav/calendars/1/event.ics",
			reportType:   "free-busy-query",
			errorMessage: "free-busy-query not allowed on calendar object resources",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			body := fmt.Sprintf(`<cal:%s xmlns:cal="urn:ietf:params:xml:ns:caldav" xmlns:D="DAV:"/>`, tc.reportType)
			req := httptest.NewRequest("REPORT", tc.path, strings.NewReader(body))
			req = req.WithContext(auth.WithUser(req.Context(), &store.User{ID: 1}))
			rr := httptest.NewRecorder()

			h.Report(rr, req)

			if rr.Code != http.StatusForbidden {
				t.Errorf("expected 403 Forbidden, got %d: %s", rr.Code, rr.Body.String())
			}
			if !strings.Contains(rr.Body.String(), tc.errorMessage) {
				t.Errorf("expected error message %q, got %s", tc.errorMessage, rr.Body.String())
			}
		})
	}
}

func TestReportRejectsAddressBookResourcePath(t *testing.T) {
	bookRepo := &fakeAddressBookRepo{
		books: map[int64]*store.AddressBook{
			3: {ID: 3, UserID: 1, Name: "Contacts"},
		},
	}
	h := &Handler{store: &store.Store{AddressBooks: bookRepo, Contacts: &fakeContactRepo{}}}

	testCases := []struct {
		name       string
		path       string
		reportType string
	}{
		{
			name:       "addressbook-query on resource path",
			path:       "/dav/addressbooks/3/contact.vcf",
			reportType: "addressbook-query",
		},
		{
			name:       "sync-collection on resource path",
			path:       "/dav/addressbooks/3/contact.vcf",
			reportType: "sync-collection",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			body := fmt.Sprintf(`<card:%s xmlns:card="urn:ietf:params:xml:ns:carddav" xmlns:D="DAV:"/>`, tc.reportType)
			req := httptest.NewRequest("REPORT", tc.path, strings.NewReader(body))
			req = req.WithContext(auth.WithUser(req.Context(), &store.User{ID: 1}))
			rr := httptest.NewRecorder()

			h.Report(rr, req)

			if rr.Code != http.StatusForbidden {
				t.Errorf("expected 403 Forbidden, got %d: %s", rr.Code, rr.Body.String())
			}
			if !strings.Contains(rr.Body.String(), "REPORT not allowed on address book object resources") {
				t.Errorf("expected error message about resource path, got %s", rr.Body.String())
			}
		})
	}
}
