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

	"gitea.jw6.us/james/calcard/internal/config"
	"gitea.jw6.us/james/calcard/internal/store"
	"gitea.jw6.us/james/calcard/internal/auth"
)

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
	responses, err := h.calendarMultiGet(context.Background(), 2, hrefs, "/dav/calendars/2/")
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
	responses, err := h.calendarMultiGet(context.Background(), 2, hrefs, "/dav/calendars/2/")
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
	responses, token, err := h.calendarReportResponses(context.Background(), cal, "/dav/principals/1/", "/dav/calendars/2/", report)
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
			{ID: 1, ResourceType: "event", CollectionID: 2, UID: "deleted-event", DeletedAt: now},
		},
	}
	h := &Handler{store: &store.Store{Events: repo, DeletedResources: deletedRepo}}

	report := reportRequest{
		XMLName:   xml.Name{Local: "sync-collection"},
		SyncToken: buildSyncToken("cal", 2, now.Add(-time.Hour)),
	}
	cal := &store.CalendarAccess{Calendar: store.Calendar{ID: 2, Name: "Test", CTag: 2, UpdatedAt: now}, Editor: true}
	responses, _, err := h.calendarReportResponses(context.Background(), cal, "/dav/principals/1/", "/dav/calendars/2/", report)
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
		if strings.Contains(r.Href, "deleted-event") {
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
	_, _, err := h.calendarReportResponses(context.Background(), cal, "/dav/principals/1/", "/dav/calendars/2/", report)
	if !errors.Is(err, errInvalidSyncToken) {
		t.Fatalf("expected errInvalidSyncToken, got %v", err)
	}
}

func TestResolveDAVHrefHandlesRelativeAbsoluteAndURL(t *testing.T) {
	base := "/dav/calendars/2/"
	cases := map[string]string{
		"event.ics":                                     "/dav/calendars/2/event.ics",
		"/dav/calendars/2/absolute.ics":                 "/dav/calendars/2/absolute.ics",
		"https://example.com/dav/calendars/2/full.ics":  "/dav/calendars/2/full.ics",
		"http://example.com/dav/calendars/2/full.ics":   "/dav/calendars/2/full.ics",
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
	if strings.Count(body, "<d:response>") != 3 { // collection + two calendars
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

	responses, _, err := h.calendarReportResponses(context.Background(), cal, "/dav/principals/1/", "/dav/calendars/1/", report)
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

	req := httptest.NewRequest(http.MethodPut, "/dav/calendars/2/new.ics", strings.NewReader("ICALDATA"))
	req = req.WithContext(auth.WithUser(req.Context(), u))
	rr := httptest.NewRecorder()

	h.Put(rr, req)

	if rr.Code != http.StatusCreated {
		t.Fatalf("expected 201, got %d", rr.Code)
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

	req := httptest.NewRequest(http.MethodPut, "/dav/calendars/2/new.ics", strings.NewReader("ICALDATA"))
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

	req := httptest.NewRequest(http.MethodPut, "/dav/addressbooks/5/alice.vcf", strings.NewReader("VCARD"))
	req = req.WithContext(auth.WithUser(req.Context(), u))
	rr := httptest.NewRecorder()

	h.Put(rr, req)

	if rr.Code != http.StatusCreated {
		t.Fatalf("expected 201, got %d", rr.Code)
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
	responses, _, err := h.calendarReportResponses(context.Background(), cal, "/dav/principals/1/", "/dav/calendars/2/", report)
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

func TestProppatchNoContent(t *testing.T) {
	h := &Handler{}
	req := httptest.NewRequest("PROPPATCH", "/dav/calendars/1", nil)
	rr := httptest.NewRecorder()
	h.Proppatch(rr, req)
	if rr.Code != http.StatusNoContent {
		t.Fatalf("expected 204, got %d", rr.Code)
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
			"2:event": {CalendarID: 2, UID: "event", RawICAL: "ICAL", ETag: "old"},
		},
	}
	h := &Handler{store: &store.Store{Calendars: calRepo, Events: eventRepo}}
	req := httptest.NewRequest(http.MethodPut, "/dav/calendars/2/event.ics", strings.NewReader("NEW"))
	req = req.WithContext(auth.WithUser(req.Context(), &store.User{ID: 1}))
	rr := httptest.NewRecorder()

	h.Put(rr, req)

	if rr.Code != http.StatusNoContent {
		t.Fatalf("expected 204, got %d", rr.Code)
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
	req := httptest.NewRequest(http.MethodPut, "/dav/calendars/1/e.ics", nil)
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
	req := httptest.NewRequest(http.MethodPut, "/dav/calendars/1/e.ics", strings.NewReader("data"))
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
	req := httptest.NewRequest(http.MethodPut, "/dav/calendars/9/e.ics", strings.NewReader("ICAL"))
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
	req := httptest.NewRequest(http.MethodPut, "/dav/calendars/1/e.ics", io.NopCloser(errReader{}))
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
			"5:alice": {AddressBookID: 5, UID: "alice", RawVCard: "OLD", ETag: "e"},
		},
	}
	h := &Handler{store: &store.Store{AddressBooks: bookRepo, Contacts: contactRepo}}
	req := httptest.NewRequest(http.MethodPut, "/dav/addressbooks/5/alice.vcf", strings.NewReader("NEW"))
	req = req.WithContext(auth.WithUser(req.Context(), &store.User{ID: 1}))
	rr := httptest.NewRecorder()
	h.Put(rr, req)
	if rr.Code != http.StatusNoContent {
		t.Fatalf("expected 204, got %d", rr.Code)
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
	_, err := h.calendarMultiGet(context.Background(), 1, []string{"/dav/calendars/1/e.ics"}, "/dav/calendars/1/")
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
	return nil, nil
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
	return nil, nil
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
