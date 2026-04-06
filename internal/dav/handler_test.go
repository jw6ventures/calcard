package dav

import (
	"context"
	"encoding/xml"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/jw6ventures/calcard/internal/auth"
	"github.com/jw6ventures/calcard/internal/config"
	"github.com/jw6ventures/calcard/internal/store"
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
	cal := &store.CalendarAccess{Calendar: store.Calendar{ID: 2, UserID: 1}}
	responses, err := h.calendarMultiGet(context.Background(), &store.User{ID: 1}, cal, hrefs, "/dav/calendars/2/", "/dav/calendars/2/", nil)
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
	cal := &store.CalendarAccess{Calendar: store.Calendar{ID: 2, UserID: 1}}
	responses, err := h.calendarMultiGet(context.Background(), &store.User{ID: 1}, cal, hrefs, "/dav/calendars/2/", "/dav/calendars/2/", nil)
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
	cal := &store.CalendarAccess{Calendar: store.Calendar{ID: 2, UserID: 1, Name: "Test", CTag: 1, UpdatedAt: now}, Editor: true}
	responses, token, err := h.calendarReportResponses(context.Background(), &store.User{ID: 1}, cal, "/dav/principals/1/", "/dav/calendars/2/", "/dav/calendars/2/", report)
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
	cal := &store.CalendarAccess{Calendar: store.Calendar{ID: 2, UserID: 1, Name: "Test", CTag: 2, UpdatedAt: now}, Editor: true}
	responses, _, err := h.calendarReportResponses(context.Background(), &store.User{ID: 1}, cal, "/dav/principals/1/", "/dav/calendars/2/", "/dav/calendars/2/", report)
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
	cal := &store.CalendarAccess{Calendar: store.Calendar{ID: 2, UserID: 1, Name: "Test", CTag: 2, UpdatedAt: now}, Editor: true}
	_, _, err := h.calendarReportResponses(context.Background(), &store.User{ID: 1}, cal, "/dav/principals/1/", "/dav/calendars/2/", "/dav/calendars/2/", report)
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

func TestAddressBookMultiGetReportHandlesRelativeHrefAgainstResourceBase(t *testing.T) {
	contactRepo := &fakeContactRepo{
		contacts: map[string]*store.Contact{
			"5:alice": {
				AddressBookID: 5,
				UID:           "alice",
				ResourceName:  "alice",
				RawVCard:      buildVCard("3.0", "UID:alice", "FN:Alice Example"),
				ETag:          "etag-alice",
			},
		},
	}
	h := &Handler{store: &store.Store{Contacts: contactRepo}}
	book := &store.AddressBook{ID: 5, UserID: 1, Name: "Contacts"}

	responses, err := h.addressBookMultiGetReport(context.Background(), &store.User{ID: 1}, book, []string{"alice.vcf"}, "/dav/addressbooks/5/alice.vcf", nil, nil)
	if err != nil {
		t.Fatalf("addressBookMultiGetReport returned error: %v", err)
	}
	if len(responses) != 1 {
		t.Fatalf("expected 1 response, got %d", len(responses))
	}
	if responses[0].Href != "/dav/addressbooks/5/alice.vcf" {
		t.Fatalf("unexpected href %q", responses[0].Href)
	}
	if len(responses[0].Propstat) == 0 || responses[0].Propstat[0].Prop.GetETag != "\"etag-alice\"" {
		t.Fatalf("expected response to include the fetched contact, got %#v", responses[0])
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
	if dav := res.Header.Get("DAV"); dav != "1, 2, 3, access-control, calendar-access, addressbook" {
		t.Fatalf("DAV header = %q", dav)
	}
	if patch := res.Header.Get("Accept-Patch"); patch != "application/xml" {
		t.Fatalf("Accept-Patch header = %q", patch)
	}
}

func TestOptionsAdvertisesCopyMoveOnlyForObjectResources(t *testing.T) {
	h := &Handler{}

	tests := []struct {
		path          string
		wantCopyMove  bool
		wantDAVHeader string
	}{
		{path: "/dav/addressbooks/5/", wantCopyMove: false, wantDAVHeader: "1, 2, 3, access-control, calendar-access, addressbook, extended-mkcol"},
		{path: "/dav/addressbooks/5/alice.vcf", wantCopyMove: true, wantDAVHeader: "1, 2, 3, access-control, calendar-access, addressbook, extended-mkcol"},
		{path: "/dav/calendars/2/", wantCopyMove: false, wantDAVHeader: "1, 2, 3, access-control, calendar-access, addressbook, extended-mkcol"},
		{path: "/dav/calendars/2/event.ics", wantCopyMove: true, wantDAVHeader: "1, 2, 3, access-control, calendar-access, addressbook, extended-mkcol"},
	}

	for _, tc := range tests {
		t.Run(tc.path, func(t *testing.T) {
			rr := httptest.NewRecorder()
			req := httptest.NewRequest(http.MethodOptions, tc.path, nil)

			h.Options(rr, req)

			allow := rr.Result().Header.Get("Allow")
			hasCopy := strings.Contains(allow, "COPY")
			hasMove := strings.Contains(allow, "MOVE")
			if hasCopy != tc.wantCopyMove || hasMove != tc.wantCopyMove {
				t.Fatalf("Allow header for %s = %q, want COPY/MOVE present=%t", tc.path, allow, tc.wantCopyMove)
			}
			if got := rr.Result().Header.Get("DAV"); got != tc.wantDAVHeader {
				t.Fatalf("DAV header for %s = %q, want %q", tc.path, got, tc.wantDAVHeader)
			}
		})
	}
}

func TestGetAdvertisesCurrentDAVCapabilities(t *testing.T) {
	h := &Handler{}

	t.Run("positive root request advertises current capabilities", func(t *testing.T) {
		rr := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodGet, "/dav", nil)
		req = req.WithContext(auth.WithUser(req.Context(), &store.User{ID: 1}))

		h.Get(rr, req)

		if rr.Code != http.StatusOK {
			t.Fatalf("expected 200, got %d", rr.Code)
		}
		if got := rr.Header().Get("DAV"); got != "1, 2, 3, access-control, calendar-access, addressbook" {
			t.Fatalf("GET DAV header = %q", got)
		}
	})

	t.Run("negative root request does not advertise collection-only extensions", func(t *testing.T) {
		rr := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodGet, "/dav", nil)
		req = req.WithContext(auth.WithUser(req.Context(), &store.User{ID: 1}))

		h.Get(rr, req)

		if got := rr.Header().Get("DAV"); strings.Contains(got, "extended-mkcol") {
			t.Fatalf("GET DAV header should not include extended-mkcol, got %q", got)
		}
	})
}

func TestSupportsCopyMove(t *testing.T) {
	tests := []struct {
		path string
		want bool
	}{
		{path: "", want: false},
		{path: "/", want: false},
		{path: "/dav", want: false},
		{path: "/dav/addressbooks/5/", want: false},
		{path: "/dav/addressbooks/5/alice.vcf", want: true},
		{path: "/dav/calendars/2/", want: false},
		{path: "/dav/calendars/2/event.ics", want: true},
	}

	for _, tc := range tests {
		if got := supportsCopyMove(tc.path); got != tc.want {
			t.Fatalf("supportsCopyMove(%q) = %t, want %t", tc.path, got, tc.want)
		}
	}
}

func TestCanLockCalendarPath(t *testing.T) {
	user := &store.User{ID: 1, PrimaryEmail: "owner@example.com"}
	sharedUser := &store.User{ID: 2, PrimaryEmail: "editor@example.com"}
	aclRepo := &fakeACLRepo{entries: []store.ACLEntry{
		{ResourcePath: "/dav/calendars/7", PrincipalHref: "/dav/principals/2/", IsGrant: true, Privilege: "read"},
		{ResourcePath: "/dav/calendars/7", PrincipalHref: "/dav/principals/2/", IsGrant: true, Privilege: "write"},
		{ResourcePath: "/dav/calendars/8", PrincipalHref: "/dav/principals/2/", IsGrant: true, Privilege: "read"},
		{ResourcePath: "/dav/calendars/8", PrincipalHref: "/dav/principals/2/", IsGrant: true, Privilege: "bind"},
		{ResourcePath: "/dav/calendars/9", PrincipalHref: "/dav/principals/2/", IsGrant: true, Privilege: "read"},
		{ResourcePath: "/dav/calendars/9", PrincipalHref: "/dav/principals/2/", IsGrant: true, Privilege: "write-content"},
		{ResourcePath: "/dav/calendars/10", PrincipalHref: "/dav/principals/2/", IsGrant: true, Privilege: "read"},
		{ResourcePath: "/dav/calendars/10", PrincipalHref: "/dav/principals/2/", IsGrant: true, Privilege: "unbind"},
		{ResourcePath: "/dav/calendars/11/event", PrincipalHref: "/dav/principals/2/", IsGrant: true, Privilege: "write-content"},
	}}
	calRepo := &fakeCalendarRepo{
		accessible: []store.CalendarAccess{
			{Calendar: store.Calendar{ID: 5, UserID: 1, Name: "Home"}, Editor: true},
			{Calendar: store.Calendar{ID: 6, UserID: 1, Name: "Readonly Shared"}, Shared: true, Editor: false},
			{Calendar: store.Calendar{ID: 7, UserID: 1, Name: "Writable Shared"}, Shared: true, Editor: true},
			{Calendar: store.Calendar{ID: 8, UserID: 1, Name: "Bind Shared"}, Shared: true, Privileges: store.CalendarPrivileges{Read: true, Bind: true}},
			{Calendar: store.Calendar{ID: 9, UserID: 1, Name: "Write Content Shared"}, Shared: true, Privileges: store.CalendarPrivileges{Read: true, WriteContent: true}},
			{Calendar: store.Calendar{ID: 10, UserID: 1, Name: "Unbind Shared"}, Shared: true, Privileges: store.CalendarPrivileges{Read: true, Unbind: true}},
		},
		calendars: map[int64]*store.Calendar{
			5:  {ID: 5, UserID: 1, Name: "Home"},
			6:  {ID: 6, UserID: 1, Name: "Readonly Shared"},
			7:  {ID: 7, UserID: 1, Name: "Writable Shared"},
			8:  {ID: 8, UserID: 1, Name: "Bind Shared"},
			9:  {ID: 9, UserID: 1, Name: "Write Content Shared"},
			10: {ID: 10, UserID: 1, Name: "Unbind Shared"},
			11: {ID: 11, UserID: 1, Name: "Direct Object Shared"},
		},
	}
	eventRepo := &fakeEventRepo{
		events: map[string]*store.Event{
			"7:event":  {CalendarID: 7, UID: "event", ResourceName: "event"},
			"11:event": {CalendarID: 11, UID: "event", ResourceName: "event"},
		},
	}
	h := &Handler{store: &store.Store{Calendars: calRepo, Events: eventRepo, ACLEntries: aclRepo}}

	tests := []struct {
		name string
		user *store.User
		path string
		want bool
	}{
		{name: "owner collection", user: user, path: "/dav/calendars/5/", want: true},
		{name: "shared editor resource", user: sharedUser, path: "/dav/calendars/7/event.ics", want: true},
		{name: "shared readonly collection", user: sharedUser, path: "/dav/calendars/6/", want: false},
		{name: "shared bind-only collection", user: sharedUser, path: "/dav/calendars/8/", want: true},
		{name: "shared write-content-only collection", user: sharedUser, path: "/dav/calendars/9/", want: true},
		{name: "shared unbind-only collection", user: sharedUser, path: "/dav/calendars/10/", want: true},
		{name: "direct object write-content grant without collection access", user: sharedUser, path: "/dav/calendars/11/event.ics", want: true},
		{name: "birthday calendar", user: user, path: fmt.Sprintf("/dav/calendars/%d/", birthdayCalendarID), want: false},
		{name: "unsupported path", user: user, path: "/dav/unknown", want: false},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			var (
				got bool
				err error
			)
			if strings.HasPrefix(tc.path, "/dav/calendars/") {
				got, err = h.canLockCalendarPath(context.Background(), tc.user, tc.path)
			} else {
				got, err = h.canLockPath(context.Background(), tc.user, tc.path)
			}
			if err != nil {
				t.Fatalf("canLock path error = %v", err)
			}
			if got != tc.want {
				t.Fatalf("canLock path = %v, want %v", got, tc.want)
			}
		})
	}
}

func TestDefaultSupportedLock(t *testing.T) {
	prop := defaultSupportedLock()
	if prop == nil {
		t.Fatal("defaultSupportedLock() = nil")
	}
	if len(prop.LockEntries) != 2 {
		t.Fatalf("defaultSupportedLock() entries = %d, want 2", len(prop.LockEntries))
	}
	if prop.LockEntries[0].LockScope.Exclusive == nil || prop.LockEntries[0].LockType.Write == nil {
		t.Fatal("first supported lock entry should advertise exclusive write")
	}
	if prop.LockEntries[1].LockScope.Shared == nil || prop.LockEntries[1].LockType.Write == nil {
		t.Fatal("second supported lock entry should advertise shared write")
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
	if strings.Contains(body, "<d:href>/dav/principals/1/</d:href></d:response>") {
		t.Fatalf("depth 0 should not include principal resource response: %s", body)
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
	cal := &store.CalendarAccess{Calendar: store.Calendar{ID: 1, UserID: 1, Name: "Test"}}

	responses, _, err := h.calendarReportResponses(context.Background(), &store.User{ID: 1}, cal, "/dav/principals/1/", "/dav/calendars/1/", "/dav/calendars/1/", report)
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
	book := &store.AddressBook{ID: 4, UserID: 1, Name: "Contacts"}
	user := &store.User{ID: 1}

	responses, _, err := h.addressBookReportResponses(context.Background(), user, book, "/dav/principals/1/", "/dav/addressbooks/4/", report, nil)
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

func TestGetRejectsWildcardAcceptRangeWithZeroQuality(t *testing.T) {
	bookRepo := &fakeAddressBookRepo{
		books: map[int64]*store.AddressBook{
			5: {ID: 5, UserID: 1, Name: "Contacts"},
		},
	}
	contactRepo := &fakeContactRepo{
		contacts: map[string]*store.Contact{
			"5:alice": {
				AddressBookID: 5,
				UID:           "alice",
				ResourceName:  "alice",
				RawVCard:      buildVCard("3.0", "UID:alice", "FN:Alice Example"),
				ETag:          "etag2",
			},
		},
	}
	h := &Handler{store: &store.Store{AddressBooks: bookRepo, Contacts: contactRepo}}
	u := &store.User{ID: 1}

	req := httptest.NewRequest(http.MethodGet, "/dav/addressbooks/5/alice.vcf", nil)
	req.Header.Set("Accept", `text/vcard; version="4.0", */*;q=0`)
	req = req.WithContext(auth.WithUser(req.Context(), u))
	rr := httptest.NewRecorder()

	h.Get(rr, req)

	if rr.Code != http.StatusNotAcceptable {
		t.Fatalf("expected 406, got %d: %s", rr.Code, rr.Body.String())
	}
	if !strings.Contains(rr.Body.String(), "supported-address-data-conversion") {
		t.Fatalf("expected supported-address-data-conversion error, got %s", rr.Body.String())
	}
}

func TestGetAddressBookResourceReturnsInternalServerErrorWhenACLLookupFails(t *testing.T) {
	bookRepo := &fakeAddressBookRepo{
		books: map[int64]*store.AddressBook{
			5: {ID: 5, UserID: 1, Name: "Contacts"},
		},
	}
	contactRepo := &fakeContactRepo{
		contacts: map[string]*store.Contact{
			"5:alice": {
				AddressBookID: 5,
				UID:           "alice",
				ResourceName:  "alice",
				RawVCard:      buildVCard("3.0", "UID:alice", "FN:Alice Example"),
				ETag:          "etag-alice",
			},
		},
	}
	aclRepo := &fakeACLRepo{listByResourceErr: errors.New("acl lookup failed")}
	h := &Handler{store: &store.Store{AddressBooks: bookRepo, Contacts: contactRepo, ACLEntries: aclRepo}}

	req := httptest.NewRequest(http.MethodGet, "/dav/addressbooks/5/alice.vcf", nil)
	req = req.WithContext(auth.WithUser(req.Context(), &store.User{ID: 2, PrimaryEmail: "reader@example.com"}))
	rr := httptest.NewRecorder()

	h.Get(rr, req)

	if rr.Code != http.StatusInternalServerError {
		t.Fatalf("expected ACL lookup failure to return 500, got %d: %s", rr.Code, rr.Body.String())
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

func TestGetRequiresUserEvenForDAVAllAddressBookRead(t *testing.T) {
	bookRepo := &fakeAddressBookRepo{
		books: map[int64]*store.AddressBook{
			5: {ID: 5, UserID: 1, Name: "Contacts"},
		},
	}
	contactRepo := &fakeContactRepo{
		contacts: map[string]*store.Contact{
			"5:alice": {AddressBookID: 5, UID: "alice", ResourceName: "alice", RawVCard: buildVCard("3.0", "UID:alice", "FN:Alice Example"), ETag: "etag-alice"},
		},
	}
	aclRepo := &fakeACLRepo{
		entries: []store.ACLEntry{
			{ResourcePath: "/dav/addressbooks/5", PrincipalHref: "DAV:all", IsGrant: true, Privilege: "read"},
		},
	}
	h := &Handler{store: &store.Store{AddressBooks: bookRepo, Contacts: contactRepo, ACLEntries: aclRepo}}
	req := httptest.NewRequest(http.MethodGet, "/dav/addressbooks/5/alice.vcf", nil)
	rr := httptest.NewRecorder()

	h.Get(rr, req)

	if rr.Code != http.StatusUnauthorized {
		t.Fatalf("expected 401, got %d", rr.Code)
	}
	if !strings.Contains(rr.Body.String(), "missing user") {
		t.Fatalf("expected missing user error, got %q", rr.Body.String())
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

func TestDeleteAddressBookContactPropagatesLookupErrors(t *testing.T) {
	user := &store.User{ID: 1}
	bookRepo := &fakeAddressBookRepo{
		books: map[int64]*store.AddressBook{
			5: {ID: 5, UserID: 1, Name: "Contacts"},
		},
	}

	t.Run("positive delete succeeds", func(t *testing.T) {
		contactRepo := &fakeContactRepo{
			contacts: map[string]*store.Contact{
				"5:alice": {AddressBookID: 5, UID: "alice", ResourceName: "alice", RawVCard: buildVCard("3.0", "UID:alice", "FN:Alice Example"), ETag: "etag-alice"},
			},
		}
		h := &Handler{store: &store.Store{AddressBooks: bookRepo, Contacts: contactRepo}}
		req := httptest.NewRequest(http.MethodDelete, "/dav/addressbooks/5/alice.vcf", nil)
		req = req.WithContext(auth.WithUser(req.Context(), user))
		rr := httptest.NewRecorder()

		h.Delete(rr, req)

		if rr.Code != http.StatusNoContent {
			t.Fatalf("expected successful delete to return 204, got %d: %s", rr.Code, rr.Body.String())
		}
		if remaining, _ := contactRepo.GetByResourceName(req.Context(), 5, "alice"); remaining != nil {
			t.Fatalf("expected contact to be deleted, got %#v", remaining)
		}
	})

	t.Run("negative lookup errors return 500 and preserve state", func(t *testing.T) {
		contactRepo := &fakeContactRepo{
			contacts: map[string]*store.Contact{
				"5:alice": {AddressBookID: 5, UID: "alice", ResourceName: "alice", RawVCard: buildVCard("3.0", "UID:alice", "FN:Alice Example"), ETag: "etag-alice"},
			},
			getByResourceNameErr:    errors.New("lookup failed"),
			getByResourceNameErrKey: "5:alice",
		}
		h := &Handler{store: &store.Store{AddressBooks: bookRepo, Contacts: contactRepo}}
		req := httptest.NewRequest(http.MethodDelete, "/dav/addressbooks/5/alice.vcf", nil)
		req = req.WithContext(auth.WithUser(req.Context(), user))
		rr := httptest.NewRecorder()

		h.Delete(rr, req)

		if rr.Code != http.StatusInternalServerError {
			t.Fatalf("expected lookup failure to return 500, got %d: %s", rr.Code, rr.Body.String())
		}
		contactRepo.getByResourceNameErr = nil
		if remaining, _ := contactRepo.GetByResourceName(req.Context(), 5, "alice"); remaining == nil {
			t.Fatal("expected contact lookup failure to leave the contact untouched")
		}
		if len(contactRepo.deleted) != 0 {
			t.Fatalf("expected delete to be skipped on lookup failure, got %#v", contactRepo.deleted)
		}
	})
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
	body := `<card:addressbook-query xmlns:card="urn:ietf:params:xml:ns:carddav"><card:filter/></card:addressbook-query>`
	req := httptest.NewRequest("REPORT", "/dav/addressbooks/3/", strings.NewReader(body))
	req.Header.Set("Depth", "1")
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
			{Calendar: store.Calendar{ID: 2, UserID: 1, Name: "Work", UpdatedAt: now}, Editor: true},
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
			{Calendar: store.Calendar{ID: 2, UserID: 2, Name: "Work"}, Shared: true, Editor: false},
		},
	}
	aclRepo := &fakeACLRepo{entries: []store.ACLEntry{
		{ResourcePath: "/dav/calendars/2", PrincipalHref: "/dav/principals/1/", IsGrant: true, Privilege: "read"},
	}}
	h := &Handler{store: &store.Store{Calendars: calRepo, Events: &fakeEventRepo{}, ACLEntries: aclRepo}}
	u := &store.User{ID: 1}

	req := newCalendarPutRequest("/dav/calendars/2/new.ics", strings.NewReader("BEGIN:VCALENDAR\r\nVERSION:2.0\r\nBEGIN:VEVENT\r\nUID:new\r\nEND:VEVENT\r\nEND:VCALENDAR\r\n"))
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

	validVCard := "BEGIN:VCARD\r\nVERSION:3.0\r\nUID:alice\r\nFN:Alice\r\nEND:VCARD\r\n"
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
			{Calendar: store.Calendar{ID: 2, UserID: 2, Name: "Work"}, Shared: true, Editor: false},
		},
	}
	aclRepo := &fakeACLRepo{entries: []store.ACLEntry{
		{ResourcePath: "/dav/calendars/2", PrincipalHref: "/dav/principals/1/", IsGrant: true, Privilege: "read"},
	}}
	eventRepo := &fakeEventRepo{
		events: map[string]*store.Event{
			"2:old": {CalendarID: 2, UID: "old"},
		},
	}
	h := &Handler{store: &store.Store{Calendars: calRepo, Events: eventRepo, ACLEntries: aclRepo}}
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
	aclRepo.entries = append(aclRepo.entries, store.ACLEntry{
		ResourcePath:  "/dav/calendars/2",
		PrincipalHref: "/dav/principals/1/",
		IsGrant:       true,
		Privilege:     "unbind",
	})
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

func TestMkcalendarRequiresParentCollectionLockToken(t *testing.T) {
	calRepo := &fakeCalendarRepo{}
	lockRepo := &fakeLockRepo{
		locks: map[string]*store.Lock{
			"opaquelocktoken:root": {
				Token:        "opaquelocktoken:root",
				ResourcePath: "/dav/calendars",
				UserID:       1,
				LockScope:    "exclusive",
				LockType:     "write",
				Depth:        "0",
				ExpiresAt:    time.Now().Add(time.Hour),
			},
		},
	}
	h := &Handler{store: &store.Store{Calendars: calRepo, Locks: lockRepo}}
	u := &store.User{ID: 1}

	req := httptest.NewRequest("MKCALENDAR", "/dav/calendars/NewCal", nil)
	req = req.WithContext(auth.WithUser(req.Context(), u))
	rr := httptest.NewRecorder()

	h.Mkcalendar(rr, req)

	if rr.Code != http.StatusLocked {
		t.Fatalf("expected 423 without parent collection lock token, got %d: %s", rr.Code, rr.Body.String())
	}

	req = httptest.NewRequest("MKCALENDAR", "/dav/calendars/NewCal", nil)
	req.Header.Set("If", "(<opaquelocktoken:root>)")
	req = req.WithContext(auth.WithUser(req.Context(), u))
	rr = httptest.NewRecorder()

	h.Mkcalendar(rr, req)

	if rr.Code != http.StatusCreated {
		t.Fatalf("expected 201 with parent collection lock token, got %d: %s", rr.Code, rr.Body.String())
	}
	if len(calRepo.calendars) != 1 {
		t.Fatalf("expected calendar to be created once lock token was supplied, got %d", len(calRepo.calendars))
	}
}

func TestAddressBookMultiGetFiltersByBook(t *testing.T) {
	repo := &fakeContactRepo{
		contacts: map[string]*store.Contact{
			"2:keep": {AddressBookID: 2, UID: "keep", RawVCard: "VCARD", ETag: "etag-1"},
			"3:skip": {AddressBookID: 3, UID: "skip", RawVCard: "VCARD", ETag: "etag-2"},
		},
	}
	bookRepo := &fakeAddressBookRepo{books: map[int64]*store.AddressBook{
		2: {ID: 2, UserID: 1, Name: "Book"},
	}}
	h := &Handler{store: &store.Store{AddressBooks: bookRepo, Contacts: repo, DeletedResources: &fakeDeletedResourceRepo{}}}
	hrefs := []string{"/dav/addressbooks/2/keep.vcf", "/dav/addressbooks/3/skip.vcf"}
	responses, err := h.addressBookMultiGet(context.Background(), &store.User{ID: 1}, 2, hrefs, "/dav/addressbooks/2/")
	if err != nil {
		t.Fatalf("addressBookMultiGet returned error: %v", err)
	}
	if len(responses) != 2 {
		t.Fatalf("expected 2 responses, got %d", len(responses))
	}
	if responses[0].Href != "/dav/addressbooks/2/keep.vcf" {
		t.Fatalf("unexpected href %q", responses[0].Href)
	}
	if responses[1].Href != "/dav/addressbooks/3/skip.vcf" {
		t.Fatalf("unexpected filtered href %q", responses[1].Href)
	}
	if responses[1].Status != httpStatusNotFound {
		t.Fatalf("expected out-of-book href to return 404, got %q", responses[1].Status)
	}
}

func TestAddressBookMultiGetMissingReturns404(t *testing.T) {
	repo := &fakeContactRepo{
		contacts: map[string]*store.Contact{
			"2:present": {AddressBookID: 2, UID: "present", RawVCard: "VCARD", ETag: "etag-1"},
		},
	}
	bookRepo := &fakeAddressBookRepo{books: map[int64]*store.AddressBook{
		2: {ID: 2, UserID: 1, Name: "Book"},
	}}
	h := &Handler{store: &store.Store{AddressBooks: bookRepo, Contacts: repo, DeletedResources: &fakeDeletedResourceRepo{}}}
	hrefs := []string{"/dav/addressbooks/2/present.vcf", "/dav/addressbooks/2/missing.vcf"}
	responses, err := h.addressBookMultiGet(context.Background(), &store.User{ID: 1}, 2, hrefs, "/dav/addressbooks/2/")
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
	book := &store.AddressBook{ID: 5, UserID: 1, Name: "Book", CTag: 1, UpdatedAt: now}
	user := &store.User{ID: 1}
	responses, token, err := h.addressBookReportResponses(context.Background(), user, book, "/dav/principals/1/", "/dav/addressbooks/5/", report, nil)
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
	cal := &store.CalendarAccess{Calendar: store.Calendar{ID: 2, UserID: 1, Name: "Test", CTag: 2, UpdatedAt: now}, Editor: true}
	responses, _, err := h.calendarReportResponses(context.Background(), &store.User{ID: 1}, cal, "/dav/principals/1/", "/dav/calendars/2/", "/dav/calendars/2/", report)
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
		calendars: map[int64]*store.Calendar{
			2: {ID: 2, UserID: 1, Name: "Old Name"},
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
	aclRepo := &fakeACLRepo{entries: []store.ACLEntry{
		{ResourcePath: "/dav/calendars/1", PrincipalHref: "/dav/principals/1/", IsGrant: true, Privilege: "read"},
		{ResourcePath: "/dav/calendars/2", PrincipalHref: "/dav/principals/1/", IsGrant: true, Privilege: "read"},
	}}
	h := &Handler{store: &store.Store{Calendars: calRepo, ACLEntries: aclRepo}}
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

func TestReportCalendarSyncCollectionReturnsTombstoneForACLHiddenEvent(t *testing.T) {
	now := store.Now()
	calRepo := &fakeCalendarRepo{
		calendars: map[int64]*store.Calendar{
			2: {ID: 2, UserID: 9, Name: "Shared", CTag: 4, UpdatedAt: now},
		},
	}
	eventRepo := &fakeEventRepo{
		events: map[string]*store.Event{
			"2:hidden": {
				CalendarID:   2,
				UID:          "hidden",
				ResourceName: "hidden",
				RawICAL:      "BEGIN:VCALENDAR\r\nBEGIN:VEVENT\r\nUID:hidden\r\nEND:VEVENT\r\nEND:VCALENDAR\r\n",
				ETag:         "etag-hidden",
				LastModified: now,
			},
		},
	}
	h := &Handler{store: &store.Store{
		Calendars:        calRepo,
		Events:           eventRepo,
		DeletedResources: &fakeDeletedResourceRepo{},
		ACLEntries: &fakeACLRepo{entries: []store.ACLEntry{
			{ResourcePath: "/dav/calendars/2", PrincipalHref: "/dav/principals/1/", IsGrant: true, Privilege: "read"},
			{ResourcePath: "/dav/calendars/2/hidden", PrincipalHref: "/dav/principals/1/", IsGrant: false, Privilege: "read"},
		}},
	}}

	body := `<D:sync-collection xmlns:D="DAV:"><D:sync-token>` + buildSyncToken("cal", 2, now.Add(-time.Hour)) + `</D:sync-token></D:sync-collection>`
	req := httptest.NewRequest("REPORT", "/dav/calendars/2/", strings.NewReader(body))
	req = req.WithContext(auth.WithUser(req.Context(), &store.User{ID: 1}))
	rr := httptest.NewRecorder()

	h.Report(rr, req)

	if rr.Code != http.StatusMultiStatus {
		t.Fatalf("expected 207, got %d: %s", rr.Code, rr.Body.String())
	}
	respBody := rr.Body.String()
	if !strings.Contains(respBody, "hidden.ics") {
		t.Fatalf("expected sync response for ACL-hidden event, got %s", respBody)
	}
	if !strings.Contains(respBody, "404 Not Found") {
		t.Fatalf("expected ACL-hidden event to be emitted as deleted response, got %s", respBody)
	}
}

func TestReportCalendarSyncCollectionDoesNotRepeatACLHiddenTombstoneAfterTokenAdvances(t *testing.T) {
	now := store.Now()
	calRepo := &fakeCalendarRepo{
		calendars: map[int64]*store.Calendar{
			2: {ID: 2, UserID: 9, Name: "Shared", CTag: 4, UpdatedAt: now},
		},
	}
	eventRepo := &fakeEventRepo{
		events: map[string]*store.Event{
			"2:hidden": {
				CalendarID:   2,
				UID:          "hidden",
				ResourceName: "hidden",
				RawICAL:      "BEGIN:VCALENDAR\r\nBEGIN:VEVENT\r\nUID:hidden\r\nEND:VEVENT\r\nEND:VCALENDAR\r\n",
				ETag:         "etag-hidden",
				LastModified: now,
			},
		},
	}
	h := &Handler{store: &store.Store{
		Calendars:        calRepo,
		Events:           eventRepo,
		DeletedResources: &fakeDeletedResourceRepo{},
		ACLEntries: &fakeACLRepo{entries: []store.ACLEntry{
			{ResourcePath: "/dav/calendars/2", PrincipalHref: "/dav/principals/1/", IsGrant: true, Privilege: "read"},
			{ResourcePath: "/dav/calendars/2/hidden", PrincipalHref: "/dav/principals/1/", IsGrant: false, Privilege: "read"},
		}},
	}}

	firstBody := `<D:sync-collection xmlns:D="DAV:"><D:sync-token>` + buildSyncToken("cal", 2, now.Add(-time.Hour)) + `</D:sync-token></D:sync-collection>`
	firstReq := httptest.NewRequest("REPORT", "/dav/calendars/2/", strings.NewReader(firstBody))
	firstReq = firstReq.WithContext(auth.WithUser(firstReq.Context(), &store.User{ID: 1}))
	firstRR := httptest.NewRecorder()
	h.Report(firstRR, firstReq)
	if firstRR.Code != http.StatusMultiStatus {
		t.Fatalf("expected first sync 207, got %d: %s", firstRR.Code, firstRR.Body.String())
	}
	if !strings.Contains(firstRR.Body.String(), "hidden.ics") {
		t.Fatalf("expected first incremental sync to include hidden tombstone, got %s", firstRR.Body.String())
	}

	secondBody := `<D:sync-collection xmlns:D="DAV:"><D:sync-token>` + buildSyncToken("cal", 2, now) + `</D:sync-token></D:sync-collection>`
	secondReq := httptest.NewRequest("REPORT", "/dav/calendars/2/", strings.NewReader(secondBody))
	secondReq = secondReq.WithContext(auth.WithUser(secondReq.Context(), &store.User{ID: 1}))
	secondRR := httptest.NewRecorder()
	h.Report(secondRR, secondReq)
	if secondRR.Code != http.StatusMultiStatus {
		t.Fatalf("expected second sync 207, got %d: %s", secondRR.Code, secondRR.Body.String())
	}
	if strings.Contains(secondRR.Body.String(), "hidden.ics") {
		t.Fatalf("expected advanced sync token to suppress repeated hidden tombstone, got %s", secondRR.Body.String())
	}
}

func TestCalendarQueryBatchesACLLookupsForEventFiltering(t *testing.T) {
	aclRepo := &fakeACLRepo{
		entries: []store.ACLEntry{
			{ResourcePath: "/dav/calendars/2", PrincipalHref: "/dav/principals/1/", IsGrant: true, Privilege: "read"},
			{ResourcePath: "/dav/calendars/2/hidden", PrincipalHref: "/dav/principals/1/", IsGrant: false, Privilege: "read"},
		},
	}
	h := &Handler{store: &store.Store{
		Events: &fakeEventRepo{events: map[string]*store.Event{
			"2:visible": {CalendarID: 2, UID: "visible", ResourceName: "visible", RawICAL: "BEGIN:VCALENDAR\r\nBEGIN:VEVENT\r\nUID:visible\r\nEND:VEVENT\r\nEND:VCALENDAR\r\n", ETag: "etag-visible"},
			"2:hidden":  {CalendarID: 2, UID: "hidden", ResourceName: "hidden", RawICAL: "BEGIN:VCALENDAR\r\nBEGIN:VEVENT\r\nUID:hidden\r\nEND:VEVENT\r\nEND:VCALENDAR\r\n", ETag: "etag-hidden"},
			"2:other":   {CalendarID: 2, UID: "other", ResourceName: "other", RawICAL: "BEGIN:VCALENDAR\r\nBEGIN:VEVENT\r\nUID:other\r\nEND:VEVENT\r\nEND:VCALENDAR\r\n", ETag: "etag-other"},
		}},
		ACLEntries: aclRepo,
	}}
	cal := &store.CalendarAccess{
		Calendar:           store.Calendar{ID: 2, UserID: 9, Name: "Shared"},
		Shared:             true,
		PrivilegesResolved: true,
		Privileges:         store.CalendarPrivileges{Read: true},
	}

	responses, err := h.calendarQuery(context.Background(), &store.User{ID: 1}, cal, "/dav/calendars/2/", nil, nil)
	if err != nil {
		t.Fatalf("calendarQuery() error = %v", err)
	}
	if len(responses) != 2 {
		t.Fatalf("calendarQuery() responses = %#v, want only visible resources", responses)
	}
	if aclRepo.listByResourceCalls != 0 {
		t.Fatalf("expected batched ACL lookup without per-resource calls, got %d ListByResource calls", aclRepo.listByResourceCalls)
	}
	if aclRepo.listByPrincipalCalls == 0 || aclRepo.listByPrincipalCalls > 3 {
		t.Fatalf("expected batched ListByPrincipal calls, got %d", aclRepo.listByPrincipalCalls)
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

func TestReportAddressBookSyncCollectionUsesStoredResourceNames(t *testing.T) {
	now := store.Now()
	bookRepo := &fakeAddressBookRepo{
		books: map[int64]*store.AddressBook{
			3: {ID: 3, UserID: 1, Name: "Contacts", CTag: 1, UpdatedAt: now},
		},
	}
	contactRepo := &fakeContactRepo{
		contacts: map[string]*store.Contact{
			"3:shared-uid": {AddressBookID: 3, UID: "shared-uid", ResourceName: "first", RawVCard: "VCARD", ETag: "e", LastModified: now},
		},
	}
	deletedRepo := &fakeDeletedResourceRepo{
		deleted: []store.DeletedResource{
			{ResourceType: "contact", CollectionID: 3, UID: "gone-uid", ResourceName: "former", DeletedAt: now},
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
	if !strings.Contains(respBody, "first.vcf") || !strings.Contains(respBody, "former.vcf") {
		t.Fatalf("expected sync responses to use stored resource names, got %s", respBody)
	}
	if strings.Contains(respBody, "shared-uid.vcf") || strings.Contains(respBody, "gone-uid.vcf") {
		t.Fatalf("sync responses should not fall back to UID-derived hrefs when resource names exist, got %s", respBody)
	}
}

func TestReportAddressBookSyncCollectionPreservesDeletedObjectACLVisibility(t *testing.T) {
	now := store.Now()
	bookRepo := &fakeAddressBookRepo{
		books: map[int64]*store.AddressBook{
			3: {ID: 3, UserID: 1, Name: "Contacts", CTag: 1, UpdatedAt: now},
		},
	}
	contactRepo := &fakeContactRepo{
		contacts: map[string]*store.Contact{
			"3:secret": {AddressBookID: 3, UID: "secret", ResourceName: "secret", RawVCard: buildVCard("3.0", "UID:secret", "FN:Secret Person"), ETag: "secret-etag", LastModified: now},
		},
	}
	deletedRepo := &fakeDeletedResourceRepo{
		deleted: []store.DeletedResource{
			{ResourceType: "contact", CollectionID: 3, UID: "public", ResourceName: "public", DeletedAt: now},
		},
	}
	aclRepo := &fakeACLRepo{
		entries: []store.ACLEntry{
			{ResourcePath: "/dav/addressbooks/3", PrincipalHref: "/dav/principals/2/", IsGrant: true, Privilege: "read"},
			{ResourcePath: "/dav/addressbooks/3/secret", PrincipalHref: "/dav/principals/2/", IsGrant: false, Privilege: "read"},
		},
	}
	st := &store.Store{
		AddressBooks:     bookRepo,
		Contacts:         contactRepo,
		DeletedResources: deletedRepo,
		ACLEntries:       aclRepo,
		Locks:            &fakeLockRepo{},
	}
	if err := st.DeleteContactAndState(context.Background(), 3, "secret", "/dav/addressbooks/3/secret"); err != nil {
		t.Fatalf("DeleteContactAndState() error = %v", err)
	}
	deletedRepo.deleted = append(deletedRepo.deleted, store.DeletedResource{
		ResourceType: "contact",
		CollectionID: 3,
		UID:          "secret",
		ResourceName: "secret",
		DeletedAt:    now,
	})

	h := &Handler{store: st}
	body := `<D:sync-collection xmlns:D="DAV:"><D:sync-token>` + buildSyncToken("card", 3, now.Add(-time.Hour)) + `</D:sync-token></D:sync-collection>`
	req := httptest.NewRequest("REPORT", "/dav/addressbooks/3/", strings.NewReader(body))
	req = req.WithContext(auth.WithUser(req.Context(), &store.User{ID: 2, PrimaryEmail: "delegate@example.com"}))
	rr := httptest.NewRecorder()
	h.Report(rr, req)
	if rr.Code != http.StatusMultiStatus {
		t.Fatalf("expected 207, got %d: %s", rr.Code, rr.Body.String())
	}
	respBody := rr.Body.String()
	if !strings.Contains(respBody, "public.vcf") {
		t.Fatalf("expected collection-level read grant to expose unrelated deleted tombstones, got %s", respBody)
	}
	if strings.Contains(respBody, "secret.vcf") {
		t.Fatalf("expected object-level deny ACL to keep deleted tombstone hidden after delete, got %s", respBody)
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
			"5:alice": {AddressBookID: 5, UID: "alice", RawVCard: "BEGIN:VCARD\r\nVERSION:3.0\r\nUID:alice\r\nFN:Alice\r\nEND:VCARD\r\n", ETag: "e"},
		},
	}
	h := &Handler{store: &store.Store{AddressBooks: bookRepo, Contacts: contactRepo}}
	newVCard := "BEGIN:VCARD\r\nVERSION:3.0\r\nUID:alice\r\nFN:Alice Updated\r\nEMAIL:alice@example.com\r\nEND:VCARD\r\n"
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
	body := `<card:addressbook-query xmlns:card="urn:ietf:params:xml:ns:carddav"><card:filter/></card:addressbook-query>`
	req := httptest.NewRequest("REPORT", "/dav/addressbooks/9/", strings.NewReader(body))
	req.Header.Set("Depth", "1")
	req = req.WithContext(auth.WithUser(req.Context(), &store.User{ID: 1}))
	rr := httptest.NewRecorder()
	h.Report(rr, req)
	if rr.Code != http.StatusNotFound {
		t.Fatalf("expected 404, got %d", rr.Code)
	}
}

func TestReportAddressBookAliasResolutionDistinguishesMissingAndPresentAliases(t *testing.T) {
	bookRepo := &fakeAddressBookRepo{
		books: map[int64]*store.AddressBook{
			3: {ID: 3, UserID: 1, Name: "Contacts"},
		},
	}
	contactRepo := &fakeContactRepo{
		contacts: map[string]*store.Contact{
			"3:alice": {AddressBookID: 3, UID: "alice", ResourceName: "alice", RawVCard: buildVCard("3.0", "UID:alice", "FN:Alice Example"), ETag: "etag-alice"},
		},
	}
	h := &Handler{store: &store.Store{AddressBooks: bookRepo, Contacts: contactRepo}}

	t.Run("positive alias resolves to address book", func(t *testing.T) {
		body := `<card:addressbook-query xmlns:card="urn:ietf:params:xml:ns:carddav"><card:filter/></card:addressbook-query>`
		req := httptest.NewRequest("REPORT", "/dav/addressbooks/Contacts/", strings.NewReader(body))
		req.Header.Set("Depth", "1")
		req = req.WithContext(auth.WithUser(req.Context(), &store.User{ID: 1}))
		rr := httptest.NewRecorder()

		h.Report(rr, req)

		if rr.Code != http.StatusMultiStatus {
			t.Fatalf("expected 207 for alias-based address book REPORT, got %d: %s", rr.Code, rr.Body.String())
		}
		if !strings.Contains(rr.Body.String(), "alice.vcf") {
			t.Fatalf("expected alias-based REPORT to include contact, got %s", rr.Body.String())
		}
	})

	t.Run("negative missing alias returns not found", func(t *testing.T) {
		body := `<card:addressbook-query xmlns:card="urn:ietf:params:xml:ns:carddav"><card:filter/></card:addressbook-query>`
		req := httptest.NewRequest("REPORT", "/dav/addressbooks/Missing/", strings.NewReader(body))
		req.Header.Set("Depth", "1")
		req = req.WithContext(auth.WithUser(req.Context(), &store.User{ID: 1}))
		rr := httptest.NewRecorder()

		h.Report(rr, req)

		if rr.Code != http.StatusNotFound {
			t.Fatalf("expected missing alias REPORT to return 404, got %d: %s", rr.Code, rr.Body.String())
		}
	})
}

func TestReportAddressBookQueryDepthZeroRequiresAccess(t *testing.T) {
	bookRepo := &fakeAddressBookRepo{
		books: map[int64]*store.AddressBook{
			9: {ID: 9, UserID: 1, Name: "Private"},
		},
	}
	h := &Handler{store: &store.Store{AddressBooks: bookRepo, Contacts: &fakeContactRepo{}}}
	body := `<card:addressbook-query xmlns:card="urn:ietf:params:xml:ns:carddav"><card:filter/></card:addressbook-query>`
	req := httptest.NewRequest("REPORT", "/dav/addressbooks/9/", strings.NewReader(body))
	req.Header.Set("Depth", "0")
	req = req.WithContext(auth.WithUser(req.Context(), &store.User{ID: 2}))
	rr := httptest.NewRecorder()

	h.Report(rr, req)

	if rr.Code != http.StatusNotFound {
		t.Fatalf("expected 404 for inaccessible depth-0 addressbook-query, got %d: %s", rr.Code, rr.Body.String())
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
	req.Header.Set("Depth", "0")
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

func TestReportAddressBookMultiGetResolvesAliasHrefWithinNumericRequest(t *testing.T) {
	bookRepo := &fakeAddressBookRepo{
		books: map[int64]*store.AddressBook{
			3: {ID: 3, UserID: 1, Name: "Contacts"},
		},
	}
	contactRepo := &fakeContactRepo{
		contacts: map[string]*store.Contact{
			"3:alice": {AddressBookID: 3, UID: "alice", ResourceName: "alice", RawVCard: "VCARD", ETag: "e"},
		},
	}
	h := &Handler{store: &store.Store{AddressBooks: bookRepo, Contacts: contactRepo}}
	body := `<card:addressbook-multiget xmlns:card="urn:ietf:params:xml:ns:carddav"><D:href xmlns:D="DAV:">/dav/addressbooks/Contacts/alice.vcf</D:href></card:addressbook-multiget>`
	req := httptest.NewRequest("REPORT", "/dav/addressbooks/3/", strings.NewReader(body))
	req.Header.Set("Depth", "0")
	req = req.WithContext(auth.WithUser(req.Context(), &store.User{ID: 1}))
	rr := httptest.NewRecorder()
	h.Report(rr, req)
	if rr.Code != http.StatusMultiStatus {
		t.Fatalf("expected 207, got %d: %s", rr.Code, rr.Body.String())
	}
	respBody := rr.Body.String()
	if !strings.Contains(respBody, "/dav/addressbooks/Contacts/alice.vcf") {
		t.Fatalf("expected multiget to preserve the requested alias href, got %s", respBody)
	}
	if strings.Contains(respBody, "404 Not Found") {
		t.Fatalf("expected alias href to resolve to the requested address book, got %s", respBody)
	}
}

func TestCalendarMultiGetReturnsErrorWhenRepoFails(t *testing.T) {
	brokenRepo := &errorEventRepo{}
	h := &Handler{store: &store.Store{Events: brokenRepo, DeletedResources: &fakeDeletedResourceRepo{}}}
	cal := &store.CalendarAccess{Calendar: store.Calendar{ID: 1, UserID: 1}}
	_, err := h.calendarMultiGet(context.Background(), &store.User{ID: 1}, cal, []string{"/dav/calendars/1/e.ics"}, "/dav/calendars/1/", "/dav/calendars/1/", nil)
	if err == nil {
		t.Fatal("expected error from repo")
	}
}

func TestCalendarCopyAndMoveToSameDestinationAreNoOps(t *testing.T) {
	user := &store.User{ID: 1}
	calRepo := &fakeCalendarRepo{
		accessible: []store.CalendarAccess{
			{Calendar: store.Calendar{ID: 2, UserID: 1, Name: "Work"}, Editor: true},
		},
	}

	for _, method := range []string{"COPY", "MOVE"} {
		t.Run(method, func(t *testing.T) {
			eventRepo := &fakeEventRepo{
				events: map[string]*store.Event{
					"2:event": {CalendarID: 2, UID: "event", ResourceName: "event", RawICAL: "BEGIN:VCALENDAR\r\nBEGIN:VEVENT\r\nUID:event\r\nEND:VEVENT\r\nEND:VCALENDAR\r\n", ETag: "etag-event"},
				},
			}
			h := &Handler{store: &store.Store{Calendars: calRepo, Events: eventRepo}}

			req := httptest.NewRequest(method, "/dav/calendars/2/event.ics", nil)
			req.Header.Set("Destination", "https://example.com/dav/calendars/2/event.ics")
			req = req.WithContext(auth.WithUser(req.Context(), user))
			rr := httptest.NewRecorder()

			if method == "COPY" {
				h.Copy(rr, req)
			} else {
				h.Move(rr, req)
			}

			if rr.Code != http.StatusNoContent {
				t.Fatalf("expected same-resource %s to return 204, got %d", method, rr.Code)
			}
			event, _ := eventRepo.GetByResourceName(req.Context(), 2, "event")
			if event == nil {
				t.Fatalf("expected same-resource %s to preserve the source event", method)
			}
		})
	}
}

func TestContactCopyAndMoveToSameDestinationAreNoOps(t *testing.T) {
	user := &store.User{ID: 1}
	bookRepo := &fakeAddressBookRepo{
		books: map[int64]*store.AddressBook{
			5: {ID: 5, UserID: 1, Name: "Contacts"},
		},
	}

	for _, method := range []string{"COPY", "MOVE"} {
		t.Run(method, func(t *testing.T) {
			contactRepo := &fakeContactRepo{
				contacts: map[string]*store.Contact{
					"5:alice": {AddressBookID: 5, UID: "alice", ResourceName: "alice", RawVCard: buildVCard("3.0", "UID:alice", "FN:Alice Example"), ETag: "etag-alice"},
				},
			}
			h := &Handler{store: &store.Store{AddressBooks: bookRepo, Contacts: contactRepo}}

			req := httptest.NewRequest(method, "/dav/addressbooks/5/alice.vcf", nil)
			req.Header.Set("Destination", "https://example.com/dav/addressbooks/5/alice.vcf")
			req = req.WithContext(auth.WithUser(req.Context(), user))
			rr := httptest.NewRecorder()

			if method == "COPY" {
				h.Copy(rr, req)
			} else {
				h.Move(rr, req)
			}

			if rr.Code != http.StatusNoContent {
				t.Fatalf("expected same-resource %s to return 204, got %d", method, rr.Code)
			}
			contact, _ := contactRepo.GetByResourceName(req.Context(), 5, "alice")
			if contact == nil {
				t.Fatalf("expected same-resource %s to preserve the source contact", method)
			}
		})
	}
}

func TestCopyAndMoveOverwriteFailurePreservesExistingDestination(t *testing.T) {
	user := &store.User{ID: 1}
	calRepo := &fakeCalendarRepo{
		accessible: []store.CalendarAccess{
			{Calendar: store.Calendar{ID: 2, UserID: 1, Name: "Work"}, Editor: true},
			{Calendar: store.Calendar{ID: 3, UserID: 1, Name: "Archive"}, Editor: true},
		},
	}
	bookRepo := &fakeAddressBookRepo{
		books: map[int64]*store.AddressBook{
			5: {ID: 5, UserID: 1, Name: "Contacts"},
			6: {ID: 6, UserID: 1, Name: "Archive"},
		},
	}

	t.Run("calendar copy", func(t *testing.T) {
		eventRepo := &fakeEventRepo{
			events: map[string]*store.Event{
				"2:event": {CalendarID: 2, UID: "event", ResourceName: "event", RawICAL: "BEGIN:VCALENDAR\r\nBEGIN:VEVENT\r\nUID:event\r\nEND:VEVENT\r\nEND:VCALENDAR\r\n", ETag: "etag-source"},
				"3:event": {CalendarID: 3, UID: "event", ResourceName: "copied", RawICAL: "BEGIN:VCALENDAR\r\nBEGIN:VEVENT\r\nUID:event\r\nSUMMARY:Old\r\nEND:VEVENT\r\nEND:VCALENDAR\r\n", ETag: "etag-dest"},
			},
			copyErr: errors.New("copy failed"),
		}
		h := &Handler{store: &store.Store{Calendars: calRepo, Events: eventRepo}}

		req := httptest.NewRequest("COPY", "/dav/calendars/2/event.ics", nil)
		req.Header.Set("Destination", "https://example.com/dav/calendars/3/copied.ics")
		req = req.WithContext(auth.WithUser(req.Context(), user))
		rr := httptest.NewRecorder()
		h.Copy(rr, req)

		if rr.Code != http.StatusInternalServerError {
			t.Fatalf("expected 500, got %d", rr.Code)
		}
		if got, _ := eventRepo.GetByResourceName(req.Context(), 3, "copied"); got == nil || got.ETag != "etag-dest" {
			t.Fatalf("expected existing destination event to be preserved, got %#v", got)
		}
		if len(eventRepo.deleted) != 0 {
			t.Fatalf("expected no eager destination delete, got %#v", eventRepo.deleted)
		}
	})

	t.Run("calendar move", func(t *testing.T) {
		eventRepo := &fakeEventRepo{
			events: map[string]*store.Event{
				"2:event": {CalendarID: 2, UID: "event", ResourceName: "event", RawICAL: "BEGIN:VCALENDAR\r\nBEGIN:VEVENT\r\nUID:event\r\nEND:VEVENT\r\nEND:VCALENDAR\r\n", ETag: "etag-source"},
				"3:event": {CalendarID: 3, UID: "event", ResourceName: "moved", RawICAL: "BEGIN:VCALENDAR\r\nBEGIN:VEVENT\r\nUID:event\r\nSUMMARY:Old\r\nEND:VEVENT\r\nEND:VCALENDAR\r\n", ETag: "etag-dest"},
			},
			moveErr: errors.New("move failed"),
		}
		h := &Handler{store: &store.Store{Calendars: calRepo, Events: eventRepo}}

		req := httptest.NewRequest("MOVE", "/dav/calendars/2/event.ics", nil)
		req.Header.Set("Destination", "https://example.com/dav/calendars/3/moved.ics")
		req = req.WithContext(auth.WithUser(req.Context(), user))
		rr := httptest.NewRecorder()
		h.Move(rr, req)

		if rr.Code != http.StatusInternalServerError {
			t.Fatalf("expected 500, got %d", rr.Code)
		}
		if got, _ := eventRepo.GetByResourceName(req.Context(), 3, "moved"); got == nil || got.ETag != "etag-dest" {
			t.Fatalf("expected existing destination event to be preserved, got %#v", got)
		}
		if len(eventRepo.deleted) != 0 {
			t.Fatalf("expected no eager destination delete, got %#v", eventRepo.deleted)
		}
	})

	t.Run("contact copy", func(t *testing.T) {
		contactRepo := &fakeContactRepo{
			contacts: map[string]*store.Contact{
				"5:alice": {AddressBookID: 5, UID: "alice", ResourceName: "alice", RawVCard: buildVCard("3.0", "UID:alice", "FN:Alice Example"), ETag: "etag-source"},
				"6:alice": {AddressBookID: 6, UID: "alice", ResourceName: "copied", RawVCard: buildVCard("3.0", "UID:alice", "FN:Old Alice"), ETag: "etag-dest"},
			},
			copyErr: errors.New("copy failed"),
		}
		h := &Handler{store: &store.Store{AddressBooks: bookRepo, Contacts: contactRepo}}

		req := httptest.NewRequest("COPY", "/dav/addressbooks/5/alice.vcf", nil)
		req.Header.Set("Destination", "https://example.com/dav/addressbooks/6/copied.vcf")
		req = req.WithContext(auth.WithUser(req.Context(), user))
		rr := httptest.NewRecorder()
		h.Copy(rr, req)

		if rr.Code != http.StatusInternalServerError {
			t.Fatalf("expected 500, got %d", rr.Code)
		}
		if got, _ := contactRepo.GetByResourceName(req.Context(), 6, "copied"); got == nil || got.ETag != "etag-dest" {
			t.Fatalf("expected existing destination contact to be preserved, got %#v", got)
		}
		if len(contactRepo.deleted) != 0 {
			t.Fatalf("expected no eager destination delete, got %#v", contactRepo.deleted)
		}
	})

	t.Run("contact move", func(t *testing.T) {
		contactRepo := &fakeContactRepo{
			contacts: map[string]*store.Contact{
				"5:alice": {AddressBookID: 5, UID: "alice", ResourceName: "alice", RawVCard: buildVCard("3.0", "UID:alice", "FN:Alice Example"), ETag: "etag-source"},
				"6:alice": {AddressBookID: 6, UID: "alice", ResourceName: "moved", RawVCard: buildVCard("3.0", "UID:alice", "FN:Old Alice"), ETag: "etag-dest"},
			},
			moveErr: errors.New("move failed"),
		}
		h := &Handler{store: &store.Store{AddressBooks: bookRepo, Contacts: contactRepo}}

		req := httptest.NewRequest("MOVE", "/dav/addressbooks/5/alice.vcf", nil)
		req.Header.Set("Destination", "https://example.com/dav/addressbooks/6/moved.vcf")
		req = req.WithContext(auth.WithUser(req.Context(), user))
		rr := httptest.NewRecorder()
		h.Move(rr, req)

		if rr.Code != http.StatusInternalServerError {
			t.Fatalf("expected 500, got %d", rr.Code)
		}
		if got, _ := contactRepo.GetByResourceName(req.Context(), 6, "moved"); got == nil || got.ETag != "etag-dest" {
			t.Fatalf("expected existing destination contact to be preserved, got %#v", got)
		}
		if len(contactRepo.deleted) != 0 {
			t.Fatalf("expected no eager destination delete, got %#v", contactRepo.deleted)
		}
	})
}

func TestMoveRebindsACLStateAndRollsBackOnACLRebindFailure(t *testing.T) {
	user := &store.User{ID: 1}

	t.Run("calendar move rebinding succeeds", func(t *testing.T) {
		calRepo := &fakeCalendarRepo{
			accessible: []store.CalendarAccess{
				{Calendar: store.Calendar{ID: 2, UserID: 1, Name: "Work"}, Editor: true},
				{Calendar: store.Calendar{ID: 3, UserID: 1, Name: "Archive"}, Editor: true},
			},
		}
		eventRepo := &fakeEventRepo{
			events: map[string]*store.Event{
				"2:event": {CalendarID: 2, UID: "event", ResourceName: "event", RawICAL: "BEGIN:VCALENDAR\r\nBEGIN:VEVENT\r\nUID:event\r\nEND:VEVENT\r\nEND:VCALENDAR\r\n", ETag: "etag-source"},
			},
		}
		aclRepo := &fakeACLRepo{
			entries: []store.ACLEntry{
				{ResourcePath: "/dav/calendars/2/event", PrincipalHref: "/dav/principals/1/", IsGrant: true, Privilege: "read"},
			},
		}
		h := &Handler{store: &store.Store{Calendars: calRepo, Events: eventRepo, ACLEntries: aclRepo}}

		req := httptest.NewRequest("MOVE", "/dav/calendars/2/event.ics", nil)
		req.Header.Set("Destination", "https://example.com/dav/calendars/3/moved.ics")
		req = req.WithContext(auth.WithUser(req.Context(), user))
		rr := httptest.NewRecorder()
		h.Move(rr, req)

		if rr.Code != http.StatusCreated {
			t.Fatalf("expected 201, got %d: %s", rr.Code, rr.Body.String())
		}
		if moved, _ := eventRepo.GetByResourceName(req.Context(), 3, "moved"); moved == nil {
			t.Fatal("expected destination event after successful move")
		}
		if src, _ := eventRepo.GetByResourceName(req.Context(), 2, "event"); src != nil {
			t.Fatalf("expected source event to be removed after move, got %#v", src)
		}
		if entries, _ := aclRepo.ListByResource(req.Context(), "/dav/calendars/3/moved"); len(entries) != 1 {
			t.Fatalf("expected ACL entry to move to destination, got %#v", entries)
		}
		if entries, _ := aclRepo.ListByResource(req.Context(), "/dav/calendars/2/event"); len(entries) != 0 {
			t.Fatalf("expected source ACL entry to be cleared, got %#v", entries)
		}
	})

	t.Run("calendar move rolls back when ACL rebind fails", func(t *testing.T) {
		calRepo := &fakeCalendarRepo{
			accessible: []store.CalendarAccess{
				{Calendar: store.Calendar{ID: 2, UserID: 1, Name: "Work"}, Editor: true},
				{Calendar: store.Calendar{ID: 3, UserID: 1, Name: "Archive"}, Editor: true},
			},
		}
		eventRepo := &fakeEventRepo{
			events: map[string]*store.Event{
				"2:event": {CalendarID: 2, UID: "event", ResourceName: "event", RawICAL: "BEGIN:VCALENDAR\r\nBEGIN:VEVENT\r\nUID:event\r\nSUMMARY:Source\r\nEND:VEVENT\r\nEND:VCALENDAR\r\n", ETag: "etag-source"},
			},
		}
		deletedRepo := &fakeDeletedResourceRepo{}
		aclRepo := &fakeACLRepo{
			entries: []store.ACLEntry{
				{ResourcePath: "/dav/calendars/2/event", PrincipalHref: "/dav/principals/1/", IsGrant: true, Privilege: "read"},
			},
			moveResourcePathHook: func(fromPath, toPath string) {
				deletedRepo.deleted = []store.DeletedResource{
					{ResourceType: "event", CollectionID: 2, UID: "event", ResourceName: "event", DeletedAt: time.Now()},
					{ResourceType: "event", CollectionID: 3, UID: "event", ResourceName: "moved", DeletedAt: time.Now()},
				}
			},
			moveResourcePathErr: errors.New("acl move failed"),
		}
		h := &Handler{store: &store.Store{Calendars: calRepo, Events: eventRepo, DeletedResources: deletedRepo, ACLEntries: aclRepo}}

		req := httptest.NewRequest("MOVE", "/dav/calendars/2/event.ics", nil)
		req.Header.Set("Destination", "https://example.com/dav/calendars/3/moved.ics")
		req = req.WithContext(auth.WithUser(req.Context(), user))
		rr := httptest.NewRecorder()
		h.Move(rr, req)

		if rr.Code != http.StatusInternalServerError {
			t.Fatalf("expected 500, got %d: %s", rr.Code, rr.Body.String())
		}
		if src, _ := eventRepo.GetByResourceName(req.Context(), 2, "event"); src == nil || src.ETag != "etag-source" {
			t.Fatalf("expected source event to be restored, got %#v", src)
		}
		if dest, _ := eventRepo.GetByResourceName(req.Context(), 3, "moved"); dest != nil {
			t.Fatalf("expected destination event creation to be rolled back, got %#v", dest)
		}
		if entries, _ := aclRepo.ListByResource(req.Context(), "/dav/calendars/2/event"); len(entries) != 1 {
			t.Fatalf("expected source ACL entry to remain, got %#v", entries)
		}
		if entries, _ := aclRepo.ListByResource(req.Context(), "/dav/calendars/3/moved"); len(entries) != 0 {
			t.Fatalf("expected destination ACL entry creation to be rolled back, got %#v", entries)
		}
		if tombstones, _ := deletedRepo.ListDeletedSince(req.Context(), "event", 2, time.Time{}); len(tombstones) != 0 {
			t.Fatalf("expected source event tombstones to be removed during rollback, got %#v", tombstones)
		}
		if tombstones, _ := deletedRepo.ListDeletedSince(req.Context(), "event", 3, time.Time{}); len(tombstones) != 0 {
			t.Fatalf("expected destination event tombstones to be removed during rollback, got %#v", tombstones)
		}
	})

	t.Run("contact move rebinding succeeds", func(t *testing.T) {
		bookRepo := &fakeAddressBookRepo{
			books: map[int64]*store.AddressBook{
				5: {ID: 5, UserID: 1, Name: "Contacts"},
				6: {ID: 6, UserID: 1, Name: "Archive"},
			},
		}
		contactRepo := &fakeContactRepo{
			contacts: map[string]*store.Contact{
				"5:alice": {AddressBookID: 5, UID: "alice", ResourceName: "alice", RawVCard: buildVCard("3.0", "UID:alice", "FN:Alice Example"), ETag: "etag-source"},
			},
		}
		aclRepo := &fakeACLRepo{
			entries: []store.ACLEntry{
				{ResourcePath: "/dav/addressbooks/5/alice", PrincipalHref: "/dav/principals/1/", IsGrant: true, Privilege: "read"},
			},
		}
		h := &Handler{store: &store.Store{AddressBooks: bookRepo, Contacts: contactRepo, ACLEntries: aclRepo}}

		req := httptest.NewRequest("MOVE", "/dav/addressbooks/5/alice.vcf", nil)
		req.Header.Set("Destination", "https://example.com/dav/addressbooks/6/moved.vcf")
		req = req.WithContext(auth.WithUser(req.Context(), user))
		rr := httptest.NewRecorder()
		h.Move(rr, req)

		if rr.Code != http.StatusCreated {
			t.Fatalf("expected 201, got %d: %s", rr.Code, rr.Body.String())
		}
		if moved, _ := contactRepo.GetByResourceName(req.Context(), 6, "moved"); moved == nil {
			t.Fatal("expected destination contact after successful move")
		}
		if src, _ := contactRepo.GetByResourceName(req.Context(), 5, "alice"); src != nil {
			t.Fatalf("expected source contact to be removed after move, got %#v", src)
		}
		if entries, _ := aclRepo.ListByResource(req.Context(), "/dav/addressbooks/6/moved"); len(entries) != 1 {
			t.Fatalf("expected ACL entry to move to destination, got %#v", entries)
		}
		if entries, _ := aclRepo.ListByResource(req.Context(), "/dav/addressbooks/5/alice"); len(entries) != 0 {
			t.Fatalf("expected source ACL entry to be cleared, got %#v", entries)
		}
	})

	t.Run("contact move rolls back when ACL rebind fails", func(t *testing.T) {
		bookRepo := &fakeAddressBookRepo{
			books: map[int64]*store.AddressBook{
				5: {ID: 5, UserID: 1, Name: "Contacts"},
				6: {ID: 6, UserID: 1, Name: "Archive"},
			},
		}
		contactRepo := &fakeContactRepo{
			contacts: map[string]*store.Contact{
				"5:alice": {AddressBookID: 5, UID: "alice", ResourceName: "alice", RawVCard: buildVCard("3.0", "UID:alice", "FN:Alice Example"), ETag: "etag-source"},
			},
		}
		deletedRepo := &fakeDeletedResourceRepo{}
		aclRepo := &fakeACLRepo{
			entries: []store.ACLEntry{
				{ResourcePath: "/dav/addressbooks/5/alice", PrincipalHref: "/dav/principals/1/", IsGrant: true, Privilege: "read"},
			},
			moveResourcePathHook: func(fromPath, toPath string) {
				deletedRepo.deleted = []store.DeletedResource{
					{ResourceType: "contact", CollectionID: 5, UID: "alice", ResourceName: "alice", DeletedAt: time.Now()},
					{ResourceType: "contact", CollectionID: 6, UID: "alice", ResourceName: "moved", DeletedAt: time.Now()},
				}
			},
			moveResourcePathErr: errors.New("acl move failed"),
		}
		h := &Handler{store: &store.Store{AddressBooks: bookRepo, Contacts: contactRepo, DeletedResources: deletedRepo, ACLEntries: aclRepo}}

		req := httptest.NewRequest("MOVE", "/dav/addressbooks/5/alice.vcf", nil)
		req.Header.Set("Destination", "https://example.com/dav/addressbooks/6/moved.vcf")
		req = req.WithContext(auth.WithUser(req.Context(), user))
		rr := httptest.NewRecorder()
		h.Move(rr, req)

		if rr.Code != http.StatusInternalServerError {
			t.Fatalf("expected 500, got %d: %s", rr.Code, rr.Body.String())
		}
		if src, _ := contactRepo.GetByResourceName(req.Context(), 5, "alice"); src == nil || src.ETag != "etag-source" {
			t.Fatalf("expected source contact to be restored, got %#v", src)
		}
		if dest, _ := contactRepo.GetByResourceName(req.Context(), 6, "moved"); dest != nil {
			t.Fatalf("expected destination contact creation to be rolled back, got %#v", dest)
		}
		if entries, _ := aclRepo.ListByResource(req.Context(), "/dav/addressbooks/5/alice"); len(entries) != 1 {
			t.Fatalf("expected source ACL entry to remain, got %#v", entries)
		}
		if entries, _ := aclRepo.ListByResource(req.Context(), "/dav/addressbooks/6/moved"); len(entries) != 0 {
			t.Fatalf("expected destination ACL entry creation to be rolled back, got %#v", entries)
		}
		if tombstones, _ := deletedRepo.ListDeletedSince(req.Context(), "contact", 5, time.Time{}); len(tombstones) != 0 {
			t.Fatalf("expected source contact tombstones to be removed during rollback, got %#v", tombstones)
		}
		if tombstones, _ := deletedRepo.ListDeletedSince(req.Context(), "contact", 6, time.Time{}); len(tombstones) != 0 {
			t.Fatalf("expected destination contact tombstones to be removed during rollback, got %#v", tombstones)
		}
	})
}

func TestMoveCalendarEventRequiresSourceReadPrivilegeBeforeLookup(t *testing.T) {
	owner := &store.User{ID: 1}
	delegate := &store.User{ID: 2}
	calRepo := &fakeCalendarRepo{
		calendars: map[int64]*store.Calendar{
			2: {ID: 2, UserID: delegate.ID, Name: "Destination"},
			9: {ID: 9, UserID: owner.ID, Name: "Shared"},
		},
	}
	eventRepo := &fakeEventRepo{
		events: map[string]*store.Event{
			"9:secret": {
				CalendarID:   9,
				UID:          "secret",
				ResourceName: "secret",
				RawICAL:      "BEGIN:VCALENDAR\r\nBEGIN:VEVENT\r\nUID:secret\r\nEND:VEVENT\r\nEND:VCALENDAR\r\n",
				ETag:         "etag-secret",
			},
		},
	}
	aclRepo := &fakeACLRepo{entries: []store.ACLEntry{
		{ResourcePath: "/dav/calendars/9", PrincipalHref: "/dav/principals/2/", IsGrant: true, Privilege: "read"},
		{ResourcePath: "/dav/calendars/9", PrincipalHref: "/dav/principals/2/", IsGrant: true, Privilege: "unbind"},
		{ResourcePath: "/dav/calendars/9/secret", PrincipalHref: "/dav/principals/2/", IsGrant: false, Privilege: "read"},
		{ResourcePath: "/dav/calendars/2", PrincipalHref: "/dav/principals/2/", IsGrant: true, Privilege: "read"},
		{ResourcePath: "/dav/calendars/2", PrincipalHref: "/dav/principals/2/", IsGrant: true, Privilege: "bind"},
	}}
	h := &Handler{store: &store.Store{Calendars: calRepo, Events: eventRepo, ACLEntries: aclRepo}}

	req := httptest.NewRequest("MOVE", "/dav/calendars/9/secret.ics", nil)
	req.Header.Set("Destination", "https://example.com/dav/calendars/2/copied.ics")
	req = req.WithContext(auth.WithUser(req.Context(), delegate))
	rr := httptest.NewRecorder()

	h.Move(rr, req)

	if rr.Code != http.StatusForbidden {
		t.Fatalf("expected source read denial to return 403, got %d: %s", rr.Code, rr.Body.String())
	}
	if eventRepo.resourceLookupCount != 0 {
		t.Fatalf("expected MOVE to reject denied source before loading the event, got %d lookups", eventRepo.resourceLookupCount)
	}
}

func TestPropfindCalendarCollectionDoesNotOverAdvertisePartialWritePrivileges(t *testing.T) {
	owner := &store.User{ID: 1}
	delegate := &store.User{ID: 2}
	now := store.Now()
	calRepo := &fakeCalendarRepo{
		accessibleByUser: map[int64][]store.CalendarAccess{
			delegate.ID: {
				{
					Calendar:   store.Calendar{ID: 5, UserID: owner.ID, Name: "Shared", UpdatedAt: now},
					Shared:     true,
					Editor:     false,
					Privileges: store.CalendarPrivileges{Read: true, ReadFreeBusy: true, Bind: true},
				},
			},
		},
		calendars: map[int64]*store.Calendar{
			5: {ID: 5, UserID: owner.ID, Name: "Shared", UpdatedAt: now},
		},
	}
	h := &Handler{store: &store.Store{Calendars: calRepo}}

	req := httptest.NewRequest("PROPFIND", "/dav/calendars/5/", nil)
	req.Header.Set("Depth", "0")
	req = req.WithContext(auth.WithUser(req.Context(), delegate))
	rr := httptest.NewRecorder()

	h.Propfind(rr, req)

	if rr.Code != http.StatusMultiStatus {
		t.Fatalf("expected 207, got %d: %s", rr.Code, rr.Body.String())
	}
	body := rr.Body.String()
	privilegeSet := regexp.MustCompile(`(?s)<d:current-user-privilege-set>.*?</d:current-user-privilege-set>`).FindString(body)
	if privilegeSet == "" {
		t.Fatalf("expected current-user-privilege-set in response, got %s", body)
	}
	if !strings.Contains(privilegeSet, "bind") {
		t.Fatalf("expected bind privilege in response, got %s", privilegeSet)
	}
	if strings.Contains(privilegeSet, "write-content") {
		t.Fatalf("did not expect write-content privilege for bind-only access, got %s", privilegeSet)
	}
	if strings.Contains(privilegeSet, "write-properties") {
		t.Fatalf("did not expect write-properties privilege for bind-only access, got %s", privilegeSet)
	}
	if strings.Contains(privilegeSet, "unbind") {
		t.Fatalf("did not expect unbind privilege for bind-only access, got %s", privilegeSet)
	}
}

func TestCurrentUserPrivilegeSetForCalendarOmitsDeniedReadFreeBusy(t *testing.T) {
	owner := &store.User{ID: 1}
	delegate := &store.User{ID: 2}
	calRepo := &fakeCalendarRepo{
		calendars: map[int64]*store.Calendar{
			5: {ID: 5, UserID: owner.ID, Name: "Shared"},
		},
	}
	aclRepo := &fakeACLRepo{entries: []store.ACLEntry{
		{ResourcePath: "/dav/calendars/5", PrincipalHref: "/dav/principals/2/", IsGrant: true, Privilege: "read"},
		{ResourcePath: "/dav/calendars/5", PrincipalHref: "/dav/principals/2/", IsGrant: false, Privilege: "read-free-busy"},
	}}
	h := &Handler{store: &store.Store{Calendars: calRepo, ACLEntries: aclRepo}}

	privilegeSet := h.currentUserPrivilegeSetForPath(context.Background(), delegate, "/dav/calendars/5/")
	if privilegeSet == nil {
		t.Fatal("expected privilege set for readable calendar")
	}
	if len(privilegeSet.Privileges) != 1 {
		t.Fatalf("expected only DAV:read privilege, got %#v", privilegeSet.Privileges)
	}
	if privilegeSet.Privileges[0].Read == nil {
		t.Fatalf("expected DAV:read privilege, got %#v", privilegeSet.Privileges[0])
	}
	if privilegeSet.Privileges[0].Read.ReadFreeBusy != nil {
		t.Fatalf("did not expect read-free-busy nested under DAV:read when explicitly denied, got %#v", privilegeSet.Privileges[0])
	}
	if privilegeSet.Privileges[0].ReadFreeBusy != nil {
		t.Fatalf("did not expect standalone read-free-busy privilege when explicitly denied, got %#v", privilegeSet.Privileges[0])
	}
}

func TestPropfindListsAndLoadsBindOnlyCalendarCollections(t *testing.T) {
	owner := &store.User{ID: 1}
	delegate := &store.User{ID: 2}
	now := store.Now()
	calRepo := &fakeCalendarRepo{
		accessibleByUser: map[int64][]store.CalendarAccess{
			delegate.ID: {
				{
					Calendar:   store.Calendar{ID: 8, UserID: owner.ID, Name: "Inbox", UpdatedAt: now},
					Shared:     true,
					Privileges: store.CalendarPrivileges{Bind: true},
				},
			},
		},
		calendars: map[int64]*store.Calendar{
			8: {ID: 8, UserID: owner.ID, Name: "Inbox", UpdatedAt: now},
		},
	}
	aclRepo := &fakeACLRepo{entries: []store.ACLEntry{
		{ResourcePath: "/dav/calendars/8", PrincipalHref: "/dav/principals/2/", IsGrant: true, Privilege: "bind"},
	}}
	h := &Handler{store: &store.Store{Calendars: calRepo, Events: &fakeEventRepo{}, ACLEntries: aclRepo}}

	rootReq := httptest.NewRequest("PROPFIND", "/dav/calendars/", nil)
	rootReq.Header.Set("Depth", "1")
	rootReq = rootReq.WithContext(auth.WithUser(rootReq.Context(), delegate))
	rootRR := httptest.NewRecorder()

	h.Propfind(rootRR, rootReq)

	if rootRR.Code != http.StatusMultiStatus {
		t.Fatalf("expected calendar home PROPFIND to succeed, got %d: %s", rootRR.Code, rootRR.Body.String())
	}
	if !strings.Contains(rootRR.Body.String(), "/dav/calendars/8/") {
		t.Fatalf("expected bind-only calendar to be discoverable, got %s", rootRR.Body.String())
	}

	calReq := httptest.NewRequest("PROPFIND", "/dav/calendars/8/", nil)
	calReq.Header.Set("Depth", "0")
	calReq = calReq.WithContext(auth.WithUser(calReq.Context(), delegate))
	calRR := httptest.NewRecorder()

	h.Propfind(calRR, calReq)

	if calRR.Code != http.StatusMultiStatus {
		t.Fatalf("expected bind-only calendar PROPFIND to succeed, got %d: %s", calRR.Code, calRR.Body.String())
	}
	body := calRR.Body.String()
	if !strings.Contains(body, "/dav/calendars/8/") {
		t.Fatalf("expected bind-only calendar href in response, got %s", body)
	}
	if !strings.Contains(body, "bind") {
		t.Fatalf("expected bind privilege in response, got %s", body)
	}
}

func TestPropfindListsAndLoadsPartialAccessCalendarCollections(t *testing.T) {
	owner := &store.User{ID: 1}
	delegate := &store.User{ID: 2}
	now := store.Now()
	reviewSlug := "review"

	calRepo := &fakeCalendarRepo{
		accessibleByUser: map[int64][]store.CalendarAccess{
			delegate.ID: {
				{
					Calendar:   store.Calendar{ID: 9, UserID: owner.ID, Name: "Drafts", UpdatedAt: now},
					Shared:     true,
					Privileges: store.CalendarPrivileges{WriteContent: true},
				},
				{
					Calendar:   store.Calendar{ID: 10, UserID: owner.ID, Name: "Archive", UpdatedAt: now},
					Shared:     true,
					Privileges: store.CalendarPrivileges{Unbind: true},
				},
				{
					Calendar:   store.Calendar{ID: 11, UserID: owner.ID, Name: "Review", Slug: &reviewSlug, UpdatedAt: now},
					Shared:     true,
					Privileges: store.CalendarPrivileges{WriteContent: true},
				},
			},
		},
		calendars: map[int64]*store.Calendar{
			9:  {ID: 9, UserID: owner.ID, Name: "Drafts", UpdatedAt: now},
			10: {ID: 10, UserID: owner.ID, Name: "Archive", UpdatedAt: now},
			11: {ID: 11, UserID: owner.ID, Name: "Review", Slug: &reviewSlug, UpdatedAt: now},
		},
	}
	aclRepo := &fakeACLRepo{entries: []store.ACLEntry{
		{ResourcePath: "/dav/calendars/9", PrincipalHref: "/dav/principals/2/", IsGrant: true, Privilege: "write-content"},
		{ResourcePath: "/dav/calendars/10", PrincipalHref: "/dav/principals/2/", IsGrant: true, Privilege: "unbind"},
		{ResourcePath: "/dav/calendars/11", PrincipalHref: "/dav/principals/2/", IsGrant: true, Privilege: "write-content"},
	}}
	h := &Handler{store: &store.Store{Calendars: calRepo, Events: &fakeEventRepo{}, ACLEntries: aclRepo}}

	rootReq := httptest.NewRequest("PROPFIND", "/dav/calendars/", nil)
	rootReq.Header.Set("Depth", "1")
	rootReq = rootReq.WithContext(auth.WithUser(rootReq.Context(), delegate))
	rootRR := httptest.NewRecorder()

	h.Propfind(rootRR, rootReq)

	if rootRR.Code != http.StatusMultiStatus {
		t.Fatalf("expected calendar home PROPFIND to succeed, got %d: %s", rootRR.Code, rootRR.Body.String())
	}
	rootBody := rootRR.Body.String()
	for _, href := range []string{"/dav/calendars/9/", "/dav/calendars/10/", "/dav/calendars/11/"} {
		if !strings.Contains(rootBody, href) {
			t.Fatalf("expected partial-access calendar %s to be discoverable, got %s", href, rootBody)
		}
	}

	tests := []struct {
		name      string
		path      string
		wantHref  string
		privilege string
	}{
		{name: "write-content by id", path: "/dav/calendars/9/", wantHref: "/dav/calendars/9/", privilege: "write-content"},
		{name: "unbind by id", path: "/dav/calendars/10/", wantHref: "/dav/calendars/10/", privilege: "unbind"},
		{name: "write-content by slug", path: "/dav/calendars/review/", wantHref: "/dav/calendars/11/", privilege: "write-content"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			req := httptest.NewRequest("PROPFIND", tc.path, nil)
			req.Header.Set("Depth", "0")
			req = req.WithContext(auth.WithUser(req.Context(), delegate))
			rr := httptest.NewRecorder()

			h.Propfind(rr, req)

			if rr.Code != http.StatusMultiStatus {
				t.Fatalf("expected partial-access calendar PROPFIND to succeed, got %d: %s", rr.Code, rr.Body.String())
			}
			body := rr.Body.String()
			if !strings.Contains(body, tc.wantHref) {
				t.Fatalf("expected calendar href %s in response, got %s", tc.wantHref, body)
			}
			if !strings.Contains(body, tc.privilege) {
				t.Fatalf("expected privilege %q in response, got %s", tc.privilege, body)
			}
		})
	}
}

func TestPropfindDiscoveryIncludesObjectGrantedCalendars(t *testing.T) {
	owner := &store.User{ID: 1}
	delegate := &store.User{ID: 2}
	now := store.Now()
	calRepo := &fakeCalendarRepo{
		accessibleByUser: map[int64][]store.CalendarAccess{
			owner.ID: {
				{Calendar: store.Calendar{ID: 12, UserID: owner.ID, Name: "Object Shared", UpdatedAt: now}, Editor: true},
			},
		},
		calendars: map[int64]*store.Calendar{
			12: {ID: 12, UserID: owner.ID, Name: "Object Shared", UpdatedAt: now},
		},
	}
	eventRepo := &fakeEventRepo{
		events: map[string]*store.Event{
			"12:special": {
				CalendarID:   12,
				UID:          "special",
				ResourceName: "special",
				RawICAL:      "BEGIN:VCALENDAR\r\nBEGIN:VEVENT\r\nUID:special\r\nEND:VEVENT\r\nEND:VCALENDAR\r\n",
				ETag:         "etag-special",
			},
		},
	}
	aclRepo := &fakeACLRepo{entries: []store.ACLEntry{
		{ResourcePath: "/dav/calendars/12/special", PrincipalHref: "/dav/principals/2/", IsGrant: true, Privilege: "read"},
	}}
	h := &Handler{store: &store.Store{Calendars: calRepo, Events: eventRepo, ACLEntries: aclRepo}}

	rootReq := httptest.NewRequest("PROPFIND", "/dav/calendars/", nil)
	rootReq.Header.Set("Depth", "1")
	rootReq = rootReq.WithContext(auth.WithUser(rootReq.Context(), delegate))
	rootRR := httptest.NewRecorder()

	h.Propfind(rootRR, rootReq)

	if rootRR.Code != http.StatusMultiStatus {
		t.Fatalf("expected calendar home PROPFIND to succeed, got %d: %s", rootRR.Code, rootRR.Body.String())
	}
	if !strings.Contains(rootRR.Body.String(), "/dav/calendars/12/") {
		t.Fatalf("expected object-granted calendar to be discoverable, got %s", rootRR.Body.String())
	}

	calReq := httptest.NewRequest("PROPFIND", "/dav/calendars/12/", nil)
	calReq.Header.Set("Depth", "0")
	calReq = calReq.WithContext(auth.WithUser(calReq.Context(), delegate))
	calRR := httptest.NewRecorder()

	h.Propfind(calRR, calReq)

	if calRR.Code != http.StatusMultiStatus {
		t.Fatalf("expected direct calendar PROPFIND to succeed for object grant, got %d: %s", calRR.Code, calRR.Body.String())
	}
	body := calRR.Body.String()
	if !strings.Contains(body, "/dav/calendars/12/") {
		t.Fatalf("expected object-granted calendar href in response, got %s", body)
	}
	privilegeSet := regexp.MustCompile(`(?s)<d:current-user-privilege-set>.*?</d:current-user-privilege-set>`).FindString(body)
	if privilegeSet == "" {
		t.Fatalf("expected current-user-privilege-set in response, got %s", body)
	}
	for _, privilege := range []string{"<d:read", "read-free-busy", "write-content", "write-properties", "<d:bind", "<d:unbind", "<d:write"} {
		if strings.Contains(privilegeSet, privilege) {
			t.Fatalf("did not expect collection privilege %q for object-only calendar discovery, got %s", privilege, privilegeSet)
		}
	}
}

func TestPropfindDiscoveryExcludesCalendarsWithOnlyObjectLevelDenyACE(t *testing.T) {
	owner := &store.User{ID: 1}
	delegate := &store.User{ID: 2}
	now := store.Now()
	calRepo := &fakeCalendarRepo{
		accessibleByUser: map[int64][]store.CalendarAccess{
			owner.ID: {
				{Calendar: store.Calendar{ID: 13, UserID: owner.ID, Name: "Hidden By Deny", UpdatedAt: now}, Editor: true},
			},
		},
		calendars: map[int64]*store.Calendar{
			13: {ID: 13, UserID: owner.ID, Name: "Hidden By Deny", UpdatedAt: now},
		},
	}
	eventRepo := &fakeEventRepo{
		events: map[string]*store.Event{
			"13:secret": {
				CalendarID:   13,
				UID:          "secret",
				ResourceName: "secret",
				RawICAL:      "BEGIN:VCALENDAR\r\nBEGIN:VEVENT\r\nUID:secret\r\nEND:VEVENT\r\nEND:VCALENDAR\r\n",
				ETag:         "etag-secret",
			},
		},
	}
	aclRepo := &fakeACLRepo{entries: []store.ACLEntry{
		{ResourcePath: "/dav/calendars/13/secret", PrincipalHref: "/dav/principals/2/", IsGrant: false, Privilege: "read"},
	}}
	h := &Handler{store: &store.Store{Calendars: calRepo, Events: eventRepo, ACLEntries: aclRepo}}

	rootReq := httptest.NewRequest("PROPFIND", "/dav/calendars/", nil)
	rootReq.Header.Set("Depth", "1")
	rootReq = rootReq.WithContext(auth.WithUser(rootReq.Context(), delegate))
	rootRR := httptest.NewRecorder()

	h.Propfind(rootRR, rootReq)

	if rootRR.Code != http.StatusMultiStatus {
		t.Fatalf("expected calendar home PROPFIND to succeed, got %d: %s", rootRR.Code, rootRR.Body.String())
	}
	if strings.Contains(rootRR.Body.String(), "/dav/calendars/13/") {
		t.Fatalf("did not expect deny-only object ACE to surface calendar discovery, got %s", rootRR.Body.String())
	}
}

func TestCopyGeneratesFreshETagsOnRepeatedOverwrite(t *testing.T) {
	user := &store.User{ID: 1}
	calRepo := &fakeCalendarRepo{
		accessible: []store.CalendarAccess{
			{Calendar: store.Calendar{ID: 2, UserID: 1, Name: "Work"}, Editor: true},
			{Calendar: store.Calendar{ID: 3, UserID: 1, Name: "Archive"}, Editor: true},
		},
	}
	bookRepo := &fakeAddressBookRepo{
		books: map[int64]*store.AddressBook{
			5: {ID: 5, UserID: 1, Name: "Contacts"},
			6: {ID: 6, UserID: 1, Name: "Archive"},
		},
	}

	t.Run("calendar", func(t *testing.T) {
		eventRepo := &fakeEventRepo{
			events: map[string]*store.Event{
				"2:event": {CalendarID: 2, UID: "event", ResourceName: "event", RawICAL: "BEGIN:VCALENDAR\r\nBEGIN:VEVENT\r\nUID:event\r\nEND:VEVENT\r\nEND:VCALENDAR\r\n", ETag: "etag-source"},
				"3:event": {CalendarID: 3, UID: "event", ResourceName: "copied", RawICAL: "BEGIN:VCALENDAR\r\nBEGIN:VEVENT\r\nUID:event\r\nSUMMARY:Old\r\nEND:VEVENT\r\nEND:VCALENDAR\r\n", ETag: "etag-dest"},
			},
		}
		h := &Handler{store: &store.Store{Calendars: calRepo, Events: eventRepo}}

		req1 := httptest.NewRequest("COPY", "/dav/calendars/2/event.ics", nil)
		req1.Header.Set("Destination", "https://example.com/dav/calendars/3/copied.ics")
		req1 = req1.WithContext(auth.WithUser(req1.Context(), user))
		rr1 := httptest.NewRecorder()
		h.Copy(rr1, req1)

		req2 := httptest.NewRequest("COPY", "/dav/calendars/2/event.ics", nil)
		req2.Header.Set("Destination", "https://example.com/dav/calendars/3/copied.ics")
		req2 = req2.WithContext(auth.WithUser(req2.Context(), user))
		rr2 := httptest.NewRecorder()
		h.Copy(rr2, req2)

		if rr1.Code != http.StatusNoContent || rr2.Code != http.StatusNoContent {
			t.Fatalf("expected repeated calendar COPY overwrite to return 204, got %d and %d", rr1.Code, rr2.Code)
		}
		etag1 := strings.Trim(rr1.Header().Get("ETag"), "\"")
		etag2 := strings.Trim(rr2.Header().Get("ETag"), "\"")
		if etag1 == "" || etag2 == "" {
			t.Fatalf("expected COPY responses to include ETags, got %q and %q", etag1, etag2)
		}
		if etag1 == etag2 {
			t.Fatalf("expected repeated calendar COPY overwrite to generate a fresh ETag, got %q twice", etag1)
		}
	})

	t.Run("contact", func(t *testing.T) {
		contactRepo := &fakeContactRepo{
			contacts: map[string]*store.Contact{
				"5:alice": {AddressBookID: 5, UID: "alice", ResourceName: "alice", RawVCard: buildVCard("3.0", "UID:alice", "FN:Alice"), ETag: "etag-source"},
				"6:alice": {AddressBookID: 6, UID: "alice", ResourceName: "copied", RawVCard: buildVCard("3.0", "UID:alice", "FN:Old Alice"), ETag: "etag-dest"},
			},
		}
		h := &Handler{store: &store.Store{AddressBooks: bookRepo, Contacts: contactRepo}}

		req1 := httptest.NewRequest("COPY", "/dav/addressbooks/5/alice.vcf", nil)
		req1.Header.Set("Destination", "https://example.com/dav/addressbooks/6/copied.vcf")
		req1 = req1.WithContext(auth.WithUser(req1.Context(), user))
		rr1 := httptest.NewRecorder()
		h.Copy(rr1, req1)

		req2 := httptest.NewRequest("COPY", "/dav/addressbooks/5/alice.vcf", nil)
		req2.Header.Set("Destination", "https://example.com/dav/addressbooks/6/copied.vcf")
		req2 = req2.WithContext(auth.WithUser(req2.Context(), user))
		rr2 := httptest.NewRecorder()
		h.Copy(rr2, req2)

		if rr1.Code != http.StatusNoContent || rr2.Code != http.StatusNoContent {
			t.Fatalf("expected repeated contact COPY overwrite to return 204, got %d and %d", rr1.Code, rr2.Code)
		}
		etag1 := strings.Trim(rr1.Header().Get("ETag"), "\"")
		etag2 := strings.Trim(rr2.Header().Get("ETag"), "\"")
		if etag1 == "" || etag2 == "" {
			t.Fatalf("expected COPY responses to include ETags, got %q and %q", etag1, etag2)
		}
		if etag1 == etag2 {
			t.Fatalf("expected repeated contact COPY overwrite to generate a fresh ETag, got %q twice", etag1)
		}
	})
}

func TestCalendarCopyAndMoveFailClosedOnDestinationLookupErrors(t *testing.T) {
	user := &store.User{ID: 1}
	calRepo := &fakeCalendarRepo{
		accessible: []store.CalendarAccess{
			{Calendar: store.Calendar{ID: 2, UserID: 1, Name: "Work"}, Editor: true},
			{Calendar: store.Calendar{ID: 3, UserID: 1, Name: "Archive"}, Editor: true},
		},
	}

	t.Run("positive copy succeeds when destination lookups succeed", func(t *testing.T) {
		eventRepo := &fakeEventRepo{
			events: map[string]*store.Event{
				"2:event": {CalendarID: 2, UID: "event", ResourceName: "event", RawICAL: "BEGIN:VCALENDAR\r\nBEGIN:VEVENT\r\nUID:event\r\nEND:VEVENT\r\nEND:VCALENDAR\r\n", ETag: "etag-source"},
			},
		}
		h := &Handler{store: &store.Store{Calendars: calRepo, Events: eventRepo}}

		req := httptest.NewRequest("COPY", "/dav/calendars/2/event.ics", nil)
		req.Header.Set("Destination", "https://example.com/dav/calendars/3/copied.ics")
		req = req.WithContext(auth.WithUser(req.Context(), user))
		rr := httptest.NewRecorder()
		h.Copy(rr, req)

		if rr.Code != http.StatusCreated {
			t.Fatalf("expected successful copy to return 201, got %d: %s", rr.Code, rr.Body.String())
		}
		if copied, _ := eventRepo.GetByResourceName(req.Context(), 3, "copied"); copied == nil {
			t.Fatal("expected destination event to be created")
		}
	})

	t.Run("negative copy returns 500 on destination resource lookup error", func(t *testing.T) {
		eventRepo := &fakeEventRepo{
			events: map[string]*store.Event{
				"2:event": {CalendarID: 2, UID: "event", ResourceName: "event", RawICAL: "BEGIN:VCALENDAR\r\nBEGIN:VEVENT\r\nUID:event\r\nEND:VEVENT\r\nEND:VCALENDAR\r\n", ETag: "etag-source"},
				"3:event": {CalendarID: 3, UID: "event", ResourceName: "copied", RawICAL: "BEGIN:VCALENDAR\r\nBEGIN:VEVENT\r\nUID:event\r\nSUMMARY:Old\r\nEND:VEVENT\r\nEND:VCALENDAR\r\n", ETag: "etag-dest"},
			},
			getByResourceNameErr: errors.New("resource lookup failed"),
			getByResourceNameKey: "3:copied",
		}
		h := &Handler{store: &store.Store{Calendars: calRepo, Events: eventRepo}}

		req := httptest.NewRequest("COPY", "/dav/calendars/2/event.ics", nil)
		req.Header.Set("Destination", "https://example.com/dav/calendars/3/copied.ics")
		req = req.WithContext(auth.WithUser(req.Context(), user))
		rr := httptest.NewRecorder()
		h.Copy(rr, req)

		if rr.Code != http.StatusInternalServerError {
			t.Fatalf("expected destination lookup failure to return 500, got %d: %s", rr.Code, rr.Body.String())
		}
		eventRepo.getByResourceNameErr = nil
		if dest, _ := eventRepo.GetByResourceName(req.Context(), 3, "copied"); dest == nil || dest.ETag != "etag-dest" {
			t.Fatalf("expected destination event to remain unchanged, got %#v", dest)
		}
		if src, _ := eventRepo.GetByResourceName(req.Context(), 2, "event"); src == nil {
			t.Fatal("expected source event to remain after failed copy")
		}
	})

	t.Run("negative move returns 500 on destination UID lookup error", func(t *testing.T) {
		eventRepo := &fakeEventRepo{
			events: map[string]*store.Event{
				"2:event": {CalendarID: 2, UID: "event", ResourceName: "event", RawICAL: "BEGIN:VCALENDAR\r\nBEGIN:VEVENT\r\nUID:event\r\nEND:VEVENT\r\nEND:VCALENDAR\r\n", ETag: "etag-source"},
				"3:event": {CalendarID: 3, UID: "event", ResourceName: "moved", RawICAL: "BEGIN:VCALENDAR\r\nBEGIN:VEVENT\r\nUID:event\r\nSUMMARY:Old\r\nEND:VEVENT\r\nEND:VCALENDAR\r\n", ETag: "etag-dest"},
			},
			getByUIDErr:    errors.New("uid lookup failed"),
			getByUIDErrKey: "3:event",
		}
		h := &Handler{store: &store.Store{Calendars: calRepo, Events: eventRepo}}

		req := httptest.NewRequest("MOVE", "/dav/calendars/2/event.ics", nil)
		req.Header.Set("Destination", "https://example.com/dav/calendars/3/moved.ics")
		req = req.WithContext(auth.WithUser(req.Context(), user))
		rr := httptest.NewRecorder()
		h.Move(rr, req)

		if rr.Code != http.StatusInternalServerError {
			t.Fatalf("expected destination UID lookup failure to return 500, got %d: %s", rr.Code, rr.Body.String())
		}
		eventRepo.getByUIDErr = nil
		if dest, _ := eventRepo.GetByResourceName(req.Context(), 3, "moved"); dest == nil || dest.ETag != "etag-dest" {
			t.Fatalf("expected destination event to remain unchanged, got %#v", dest)
		}
		if src, _ := eventRepo.GetByResourceName(req.Context(), 2, "event"); src == nil {
			t.Fatal("expected source event to remain after failed move")
		}
	})
}

func TestCopyWithinSameCollectionRejectsRebindingSameUID(t *testing.T) {
	user := &store.User{ID: 1}

	t.Run("calendar", func(t *testing.T) {
		calRepo := &fakeCalendarRepo{
			accessible: []store.CalendarAccess{
				{Calendar: store.Calendar{ID: 2, UserID: 1, Name: "Work"}, Editor: true},
			},
		}
		eventRepo := &fakeEventRepo{
			events: map[string]*store.Event{
				"2:event": {CalendarID: 2, UID: "event", ResourceName: "original", RawICAL: "BEGIN:VCALENDAR\r\nBEGIN:VEVENT\r\nUID:event\r\nEND:VEVENT\r\nEND:VCALENDAR\r\n", ETag: "etag-source"},
			},
		}
		h := &Handler{store: &store.Store{Calendars: calRepo, Events: eventRepo}}

		req := httptest.NewRequest("COPY", "/dav/calendars/2/original.ics", nil)
		req.Header.Set("Destination", "https://example.com/dav/calendars/2/renamed.ics")
		req = req.WithContext(auth.WithUser(req.Context(), user))
		rr := httptest.NewRecorder()
		h.Copy(rr, req)

		if rr.Code != http.StatusConflict {
			t.Fatalf("expected same-calendar COPY rebinding to fail with 409, got %d: %s", rr.Code, rr.Body.String())
		}
		source, _ := eventRepo.GetByResourceName(req.Context(), 2, "original")
		if source == nil || source.UID != "event" {
			t.Fatalf("expected source event to remain bound to original href, got %#v", source)
		}
		dest, _ := eventRepo.GetByResourceName(req.Context(), 2, "renamed")
		if dest != nil {
			t.Fatalf("expected destination href to remain absent, got %#v", dest)
		}
		events, _ := eventRepo.ListForCalendar(req.Context(), 2)
		if len(events) != 1 {
			t.Fatalf("expected same-calendar COPY rejection to leave one event, got %#v", events)
		}
	})

	t.Run("contact", func(t *testing.T) {
		bookRepo := &fakeAddressBookRepo{
			books: map[int64]*store.AddressBook{
				5: {ID: 5, UserID: 1, Name: "Contacts"},
			},
		}
		contactRepo := &fakeContactRepo{
			contacts: map[string]*store.Contact{
				"5:alice": {AddressBookID: 5, UID: "alice", ResourceName: "original", RawVCard: buildVCard("3.0", "UID:alice", "FN:Alice Example"), ETag: "etag-source"},
			},
		}
		h := &Handler{store: &store.Store{AddressBooks: bookRepo, Contacts: contactRepo}}

		req := httptest.NewRequest("COPY", "/dav/addressbooks/5/original.vcf", nil)
		req.Header.Set("Destination", "https://example.com/dav/addressbooks/5/renamed.vcf")
		req = req.WithContext(auth.WithUser(req.Context(), user))
		rr := httptest.NewRecorder()
		h.Copy(rr, req)

		if rr.Code != http.StatusConflict {
			t.Fatalf("expected same-address-book COPY rebinding to fail with 409, got %d: %s", rr.Code, rr.Body.String())
		}
		if !strings.Contains(rr.Body.String(), "no-uid-conflict") {
			t.Fatalf("expected CardDAV no-uid-conflict response, got %s", rr.Body.String())
		}
		source, _ := contactRepo.GetByResourceName(req.Context(), 5, "original")
		if source == nil || source.UID != "alice" {
			t.Fatalf("expected source contact to remain bound to original href, got %#v", source)
		}
		dest, _ := contactRepo.GetByResourceName(req.Context(), 5, "renamed")
		if dest != nil {
			t.Fatalf("expected destination href to remain absent, got %#v", dest)
		}
		contacts, _ := contactRepo.ListForBook(req.Context(), 5)
		if len(contacts) != 1 {
			t.Fatalf("expected same-address-book COPY rejection to leave one contact, got %#v", contacts)
		}
	})
}

func TestCalendarCopyAndMoveRejectDestinationUIDConflict(t *testing.T) {
	user := &store.User{ID: 1}

	for _, method := range []string{"COPY", "MOVE"} {
		t.Run(method, func(t *testing.T) {
			calRepo := &fakeCalendarRepo{
				accessible: []store.CalendarAccess{
					{Calendar: store.Calendar{ID: 2, UserID: 1, Name: "Source"}, Editor: true},
					{Calendar: store.Calendar{ID: 3, UserID: 1, Name: "Destination"}, Editor: true},
				},
			}
			eventRepo := &fakeEventRepo{
				events: map[string]*store.Event{
					"2:event": {CalendarID: 2, UID: "event", ResourceName: "original", RawICAL: "BEGIN:VCALENDAR\r\nBEGIN:VEVENT\r\nUID:event\r\nEND:VEVENT\r\nEND:VCALENDAR\r\n", ETag: "etag-source"},
					"3:event": {CalendarID: 3, UID: "event", ResourceName: "existing", RawICAL: "BEGIN:VCALENDAR\r\nBEGIN:VEVENT\r\nUID:event\r\nEND:VEVENT\r\nEND:VCALENDAR\r\n", ETag: "etag-dest"},
				},
			}
			h := &Handler{store: &store.Store{Calendars: calRepo, Events: eventRepo}}

			req := httptest.NewRequest(method, "/dav/calendars/2/original.ics", nil)
			req.Header.Set("Destination", "https://example.com/dav/calendars/3/renamed.ics")
			req.Header.Set("Overwrite", "F")
			req = req.WithContext(auth.WithUser(req.Context(), user))
			rr := httptest.NewRecorder()

			if method == "COPY" {
				h.Copy(rr, req)
			} else {
				h.Move(rr, req)
			}

			if rr.Code != http.StatusConflict {
				t.Fatalf("expected %s with destination UID conflict to return 409, got %d: %s", method, rr.Code, rr.Body.String())
			}
			if !strings.Contains(rr.Body.String(), "no-uid-conflict") {
				t.Fatalf("expected %s conflict body to contain no-uid-conflict, got %s", method, rr.Body.String())
			}
			dest, _ := eventRepo.GetByResourceName(req.Context(), 3, "existing")
			if dest == nil || dest.UID != "event" {
				t.Fatalf("expected %s to preserve the existing destination event, got %#v", method, dest)
			}
			renamed, _ := eventRepo.GetByResourceName(req.Context(), 3, "renamed")
			if renamed != nil {
				t.Fatalf("expected %s to leave the requested destination href absent, got %#v", method, renamed)
			}
			source, _ := eventRepo.GetByResourceName(req.Context(), 2, "original")
			if source == nil || source.UID != "event" {
				t.Fatalf("expected %s to preserve the source event, got %#v", method, source)
			}
		})
	}
}

func TestMoveCalendarEventOverwriteWithinSameCalendarReplacesDestination(t *testing.T) {
	user := &store.User{ID: 1}
	calRepo := &fakeCalendarRepo{
		accessible: []store.CalendarAccess{
			{Calendar: store.Calendar{ID: 2, UserID: 1, Name: "Work"}, Editor: true},
		},
	}
	eventRepo := &fakeEventRepo{
		events: map[string]*store.Event{
			"2:source":      {CalendarID: 2, UID: "source", ResourceName: "original", RawICAL: "BEGIN:VCALENDAR\r\nBEGIN:VEVENT\r\nUID:source\r\nSUMMARY:Source\r\nEND:VEVENT\r\nEND:VCALENDAR\r\n", ETag: "etag-source"},
			"2:destination": {CalendarID: 2, UID: "destination", ResourceName: "renamed", RawICAL: "BEGIN:VCALENDAR\r\nBEGIN:VEVENT\r\nUID:destination\r\nSUMMARY:Destination\r\nEND:VEVENT\r\nEND:VCALENDAR\r\n", ETag: "etag-dest"},
		},
	}
	h := &Handler{store: &store.Store{Calendars: calRepo, Events: eventRepo}}

	req := httptest.NewRequest("MOVE", "/dav/calendars/2/original.ics", nil)
	req.Header.Set("Destination", "https://example.com/dav/calendars/2/renamed.ics")
	req = req.WithContext(auth.WithUser(req.Context(), user))
	rr := httptest.NewRecorder()
	h.Move(rr, req)

	if rr.Code != http.StatusNoContent {
		t.Fatalf("expected overwrite MOVE within same calendar to return 204, got %d: %s", rr.Code, rr.Body.String())
	}
	source, _ := eventRepo.GetByResourceName(req.Context(), 2, "original")
	if source != nil {
		t.Fatalf("expected old source href to be removed, got %#v", source)
	}
	moved, _ := eventRepo.GetByResourceName(req.Context(), 2, "renamed")
	if moved == nil || moved.UID != "source" {
		t.Fatalf("expected destination href to point at moved source event, got %#v", moved)
	}
	replaced, _ := eventRepo.GetByUID(req.Context(), 2, "destination")
	if replaced != nil {
		t.Fatalf("expected overwritten destination event to be removed, got %#v", replaced)
	}
	events, _ := eventRepo.ListForCalendar(req.Context(), 2)
	if len(events) != 1 {
		t.Fatalf("expected overwrite MOVE to leave one event in the calendar, got %#v", events)
	}
}

func TestMoveCalendarEventOverwriteClearsDestinationTombstone(t *testing.T) {
	user := &store.User{ID: 1}
	deletedRepo := &fakeDeletedResourceRepo{}
	calRepo := &fakeCalendarRepo{
		accessible: []store.CalendarAccess{
			{Calendar: store.Calendar{ID: 2, UserID: 1, Name: "Work"}, Editor: true},
		},
	}
	eventRepo := &fakeEventRepo{
		events: map[string]*store.Event{
			"2:source":      {CalendarID: 2, UID: "source", ResourceName: "original", RawICAL: "BEGIN:VCALENDAR\r\nBEGIN:VEVENT\r\nUID:source\r\nSUMMARY:Source\r\nEND:VEVENT\r\nEND:VCALENDAR\r\n", ETag: "etag-source"},
			"2:destination": {CalendarID: 2, UID: "destination", ResourceName: "renamed", RawICAL: "BEGIN:VCALENDAR\r\nBEGIN:VEVENT\r\nUID:destination\r\nSUMMARY:Destination\r\nEND:VEVENT\r\nEND:VCALENDAR\r\n", ETag: "etag-dest"},
		},
		overwriteMoveDeletedRepo: deletedRepo,
	}
	h := &Handler{store: &store.Store{Calendars: calRepo, Events: eventRepo, DeletedResources: deletedRepo}}

	req := httptest.NewRequest("MOVE", "/dav/calendars/2/original.ics", nil)
	req.Header.Set("Destination", "https://example.com/dav/calendars/2/renamed.ics")
	req = req.WithContext(auth.WithUser(req.Context(), user))
	rr := httptest.NewRecorder()
	h.Move(rr, req)

	if rr.Code != http.StatusNoContent {
		t.Fatalf("expected overwrite MOVE within same calendar to return 204, got %d: %s", rr.Code, rr.Body.String())
	}
	tombstones, err := deletedRepo.ListDeletedSince(req.Context(), "event", 2, time.Time{})
	if err != nil {
		t.Fatalf("ListDeletedSince() error = %v", err)
	}
	for _, tombstone := range tombstones {
		if tombstone.ResourceName == "renamed" {
			t.Fatalf("expected overwrite MOVE to clear destination tombstone, got %#v", tombstones)
		}
	}
}

func TestCanonicalDAVPathUsesExtensionlessResourceIdentity(t *testing.T) {
	slug := "work"
	h := &Handler{store: &store.Store{
		Calendars: &fakeCalendarRepo{
			accessible: []store.CalendarAccess{
				{Calendar: store.Calendar{ID: 2, UserID: 1, Name: "Work", Slug: &slug}, Editor: true},
			},
			calendars: map[int64]*store.Calendar{
				2: {ID: 2, UserID: 1, Name: "Work", Slug: &slug},
			},
		},
		AddressBooks: &fakeAddressBookRepo{
			books: map[int64]*store.AddressBook{
				5: {ID: 5, UserID: 1, Name: "Contacts"},
			},
		},
	}}
	user := &store.User{ID: 1}

	tests := []struct {
		path string
		want string
	}{
		{path: "/dav/calendars/2/event.ics", want: "/dav/calendars/2/event"},
		{path: "/dav/calendars/work/event.txt", want: "/dav/calendars/2/event"},
		{path: "/dav/addressbooks/5/alice.vcf", want: "/dav/addressbooks/5/alice"},
		{path: "/dav/addressbooks/Contacts/alice.txt", want: "/dav/addressbooks/5/alice"},
	}

	for _, tc := range tests {
		got, err := h.canonicalDAVPath(context.Background(), user, tc.path)
		if err != nil {
			t.Fatalf("canonicalDAVPath(%q) error = %v", tc.path, err)
		}
		if got != tc.want {
			t.Fatalf("canonicalDAVPath(%q) = %q, want %q", tc.path, got, tc.want)
		}
	}
}

func TestNormalizeDAVResourceIdentityPreservesDottedCanonicalNames(t *testing.T) {
	tests := []struct {
		path string
		want string
	}{
		{path: "/dav/calendars/2/event.v2", want: "/dav/calendars/2/event.v2"},
		{path: "/dav/addressbooks/5/alice.v1", want: "/dav/addressbooks/5/alice.v1"},
		{path: "/dav/calendars/2/event.ics", want: "/dav/calendars/2/event"},
		{path: "/dav/addressbooks/5/alice.vcf", want: "/dav/addressbooks/5/alice"},
	}

	for _, tc := range tests {
		if got := normalizeDAVResourceIdentity(tc.path); got != tc.want {
			t.Fatalf("normalizeDAVResourceIdentity(%q) = %q, want %q", tc.path, got, tc.want)
		}
	}
}

func TestDeleteRejectsCanonicalResourceLockAcrossExtensions(t *testing.T) {
	bookRepo := &fakeAddressBookRepo{
		books: map[int64]*store.AddressBook{
			5: {ID: 5, UserID: 1, Name: "Contacts"},
		},
	}
	contactRepo := &fakeContactRepo{
		contacts: map[string]*store.Contact{
			"5:alice": {AddressBookID: 5, UID: "alice", ResourceName: "alice", RawVCard: buildVCard("3.0", "UID:alice", "FN:Alice"), ETag: "etag-alice"},
		},
	}
	lockRepo := &fakeLockRepo{
		locks: map[string]*store.Lock{
			"tok-1": {Token: "tok-1", ResourcePath: "/dav/addressbooks/5/alice", UserID: 2, LockScope: "exclusive", Depth: "0", ExpiresAt: time.Now().Add(time.Hour)},
		},
	}
	h := &Handler{store: &store.Store{AddressBooks: bookRepo, Contacts: contactRepo, Locks: lockRepo}}
	user := &store.User{ID: 1}

	req := httptest.NewRequest(http.MethodDelete, "/dav/addressbooks/5/alice.txt", nil)
	req = req.WithContext(auth.WithUser(req.Context(), user))
	rr := httptest.NewRecorder()

	h.Delete(rr, req)

	if rr.Code != http.StatusLocked {
		t.Fatalf("expected canonical lock check to block DELETE with 423, got %d: %s", rr.Code, rr.Body.String())
	}
}

func TestGetRejectsCanonicalResourceACLAcrossExtensions(t *testing.T) {
	bookRepo := &fakeAddressBookRepo{
		books: map[int64]*store.AddressBook{
			5: {ID: 5, UserID: 1, Name: "Contacts"},
		},
	}
	contactRepo := &fakeContactRepo{
		contacts: map[string]*store.Contact{
			"5:alice": {AddressBookID: 5, UID: "alice", ResourceName: "alice", RawVCard: buildVCard("3.0", "UID:alice", "FN:Alice"), ETag: "etag-alice"},
		},
	}
	aclRepo := &fakeACLRepo{
		entries: []store.ACLEntry{
			{ResourcePath: "/dav/addressbooks/5/alice", PrincipalHref: "/dav/principals/2/", IsGrant: false, Privilege: "read"},
		},
	}
	h := &Handler{store: &store.Store{AddressBooks: bookRepo, Contacts: contactRepo, ACLEntries: aclRepo}}
	user := &store.User{ID: 2, PrimaryEmail: "delegate@example.com"}

	req := httptest.NewRequest(http.MethodGet, "/dav/addressbooks/5/alice.txt", nil)
	req = req.WithContext(auth.WithUser(req.Context(), user))
	rr := httptest.NewRecorder()

	h.Get(rr, req)

	if rr.Code != http.StatusForbidden {
		t.Fatalf("expected canonical ACL check to reject GET with 403, got %d: %s", rr.Code, rr.Body.String())
	}
}

func TestLockAndACLRejectOversizedBodies(t *testing.T) {
	user := &store.User{ID: 1}
	bookRepo := &fakeAddressBookRepo{
		books: map[int64]*store.AddressBook{
			5: {ID: 5, UserID: 1, Name: "Contacts"},
		},
	}

	t.Run("lock", func(t *testing.T) {
		h := &Handler{store: &store.Store{AddressBooks: bookRepo, Locks: &fakeLockRepo{locks: map[string]*store.Lock{}}}}
		body := strings.Repeat("A", int(maxDAVBodyBytes)+1)
		req := httptest.NewRequest("LOCK", "/dav/addressbooks/5/alice.vcf", strings.NewReader(body))
		req = req.WithContext(auth.WithUser(req.Context(), user))
		rr := httptest.NewRecorder()

		h.Lock(rr, req)

		if rr.Code != http.StatusRequestEntityTooLarge {
			t.Fatalf("expected oversized LOCK body to return 413, got %d: %s", rr.Code, rr.Body.String())
		}
	})

	t.Run("acl", func(t *testing.T) {
		h := &Handler{store: &store.Store{AddressBooks: bookRepo, ACLEntries: &fakeACLRepo{}}}
		body := strings.Repeat("A", int(maxDAVBodyBytes)+1)
		req := httptest.NewRequest("ACL", "/dav/addressbooks/5/", strings.NewReader(body))
		req = req.WithContext(auth.WithUser(req.Context(), user))
		rr := httptest.NewRecorder()

		h.Acl(rr, req)

		if rr.Code != http.StatusRequestEntityTooLarge {
			t.Fatalf("expected oversized ACL body to return 413, got %d: %s", rr.Code, rr.Body.String())
		}
	})
}

func TestCalendarCopyRequiresReadAccessToSource(t *testing.T) {
	owner := &store.User{ID: 1}
	attacker := &store.User{ID: 2}
	calRepo := &fakeCalendarRepo{
		accessibleByUser: map[int64][]store.CalendarAccess{
			owner.ID: {
				{Calendar: store.Calendar{ID: 9, UserID: owner.ID, Name: "Private"}, Editor: true},
			},
			attacker.ID: {
				{Calendar: store.Calendar{ID: 2, UserID: attacker.ID, Name: "Mine"}, Editor: true},
			},
		},
		calendars: map[int64]*store.Calendar{
			2: {ID: 2, UserID: attacker.ID, Name: "Mine"},
			9: {ID: 9, UserID: owner.ID, Name: "Private"},
		},
	}
	eventRepo := &fakeEventRepo{
		events: map[string]*store.Event{
			"9:secret": {
				CalendarID:   9,
				UID:          "secret",
				ResourceName: "secret",
				RawICAL:      "BEGIN:VCALENDAR\r\nBEGIN:VEVENT\r\nUID:secret\r\nSUMMARY:Top Secret\r\nEND:VEVENT\r\nEND:VCALENDAR\r\n",
				ETag:         "etag-secret",
			},
		},
	}
	h := &Handler{store: &store.Store{Calendars: calRepo, Events: eventRepo}}

	req := httptest.NewRequest("COPY", "/dav/calendars/9/secret.ics", nil)
	req.Header.Set("Destination", "https://example.com/dav/calendars/2/copied.ics")
	req = req.WithContext(auth.WithUser(req.Context(), attacker))
	rr := httptest.NewRecorder()
	h.Copy(rr, req)

	if rr.Code != http.StatusNotFound {
		t.Fatalf("expected unauthorized source COPY to return 404, got %d: %s", rr.Code, rr.Body.String())
	}
	if copied, _ := eventRepo.GetByResourceName(req.Context(), 2, "copied"); copied != nil {
		t.Fatalf("expected COPY to leave destination untouched, got %#v", copied)
	}
}

func TestCalendarMoveRejectsUnauthorizedSourceBeforeEventLookup(t *testing.T) {
	owner := &store.User{ID: 1}
	attacker := &store.User{ID: 2}
	calRepo := &fakeCalendarRepo{
		accessibleByUser: map[int64][]store.CalendarAccess{
			owner.ID: {
				{Calendar: store.Calendar{ID: 9, UserID: owner.ID, Name: "Private"}, Editor: true},
			},
			attacker.ID: {
				{Calendar: store.Calendar{ID: 2, UserID: attacker.ID, Name: "Mine"}, Editor: true},
			},
		},
		calendars: map[int64]*store.Calendar{
			2: {ID: 2, UserID: attacker.ID, Name: "Mine"},
			9: {ID: 9, UserID: owner.ID, Name: "Private"},
		},
	}
	eventRepo := &fakeEventRepo{
		events: map[string]*store.Event{
			"9:secret": {
				CalendarID:   9,
				UID:          "secret",
				ResourceName: "secret",
				RawICAL:      "BEGIN:VCALENDAR\r\nBEGIN:VEVENT\r\nUID:secret\r\nSUMMARY:Top Secret\r\nEND:VEVENT\r\nEND:VCALENDAR\r\n",
				ETag:         "etag-secret",
			},
		},
	}
	h := &Handler{store: &store.Store{Calendars: calRepo, Events: eventRepo}}

	req := httptest.NewRequest("MOVE", "/dav/calendars/9/secret.ics", nil)
	req.Header.Set("Destination", "https://example.com/dav/calendars/2/copied.ics")
	req = req.WithContext(auth.WithUser(req.Context(), attacker))
	rr := httptest.NewRecorder()
	h.Move(rr, req)

	if rr.Code != http.StatusNotFound {
		t.Fatalf("expected unauthorized source MOVE to return 404, got %d: %s", rr.Code, rr.Body.String())
	}
	if eventRepo.resourceLookupCount != 0 {
		t.Fatalf("expected MOVE to reject unauthorized source before loading the event, got %d lookups", eventRepo.resourceLookupCount)
	}
}

func TestContactCopyAndMoveRejectUnauthorizedSourceBeforeContactLookup(t *testing.T) {
	owner := &store.User{ID: 1}
	attacker := &store.User{ID: 2}
	bookRepo := &fakeAddressBookRepo{
		books: map[int64]*store.AddressBook{
			2: {ID: 2, UserID: attacker.ID, Name: "Mine"},
			9: {ID: 9, UserID: owner.ID, Name: "Private"},
		},
	}

	for _, method := range []string{"COPY", "MOVE"} {
		t.Run(method, func(t *testing.T) {
			contactRepo := &fakeContactRepo{
				contacts: map[string]*store.Contact{
					"9:secret": {
						AddressBookID: 9,
						UID:           "secret",
						ResourceName:  "secret",
						RawVCard:      buildVCard("3.0", "UID:secret", "FN:Top Secret"),
						ETag:          "etag-secret",
					},
				},
			}
			h := &Handler{store: &store.Store{AddressBooks: bookRepo, Contacts: contactRepo}}

			req := httptest.NewRequest(method, "/dav/addressbooks/9/secret.vcf", nil)
			req.Header.Set("Destination", "https://example.com/dav/addressbooks/2/copied.vcf")
			req = req.WithContext(auth.WithUser(req.Context(), attacker))
			rr := httptest.NewRecorder()

			if method == "COPY" {
				h.Copy(rr, req)
			} else {
				h.Move(rr, req)
			}

			if rr.Code != http.StatusNotFound {
				t.Fatalf("expected unauthorized source %s to return 404, got %d: %s", method, rr.Code, rr.Body.String())
			}
			if contactRepo.resourceLookupCount != 0 {
				t.Fatalf("expected %s to reject unauthorized source before loading the contact, got %d lookups", method, contactRepo.resourceLookupCount)
			}
		})
	}
}

func TestUnlockRejectsMismatchedRequestURI(t *testing.T) {
	user := &store.User{ID: 1, PrimaryEmail: "owner@example.com"}
	lockRepo := &fakeLockRepo{locks: map[string]*store.Lock{}}
	bookRepo := &fakeAddressBookRepo{
		books: map[int64]*store.AddressBook{
			5: {ID: 5, UserID: user.ID, Name: "Contacts"},
		},
	}
	h := &Handler{store: &store.Store{AddressBooks: bookRepo, Locks: lockRepo}}

	lockBody := `<?xml version="1.0" encoding="utf-8"?><D:lockinfo xmlns:D="DAV:"><D:lockscope><D:exclusive/></D:lockscope><D:locktype><D:write/></D:locktype></D:lockinfo>`
	lockReq := httptest.NewRequest("LOCK", "/dav/addressbooks/5/alice.vcf", strings.NewReader(lockBody))
	lockReq = lockReq.WithContext(auth.WithUser(lockReq.Context(), user))
	lockRR := httptest.NewRecorder()
	h.Lock(lockRR, lockReq)

	if lockRR.Code != http.StatusOK {
		t.Fatalf("expected LOCK to succeed, got %d: %s", lockRR.Code, lockRR.Body.String())
	}

	token := lockRR.Header().Get("Lock-Token")
	unlockReq := httptest.NewRequest("UNLOCK", "/dav/addressbooks/5/bob.vcf", nil)
	unlockReq.Header.Set("Lock-Token", token)
	unlockReq = unlockReq.WithContext(auth.WithUser(unlockReq.Context(), user))
	unlockRR := httptest.NewRecorder()
	h.Unlock(unlockRR, unlockReq)

	if unlockRR.Code != http.StatusPreconditionFailed {
		t.Fatalf("expected mismatched UNLOCK to return 412, got %d: %s", unlockRR.Code, unlockRR.Body.String())
	}
	if lock, _ := lockRepo.GetByToken(context.Background(), strings.Trim(token, "<>")); lock == nil {
		t.Fatal("expected mismatched UNLOCK to preserve the original lock")
	}
}

func TestCollectionLockRefreshAndUnlockRequireLockRoot(t *testing.T) {
	user := &store.User{ID: 1, PrimaryEmail: "owner@example.com"}
	bookRepo := &fakeAddressBookRepo{
		books: map[int64]*store.AddressBook{
			5: {ID: 5, UserID: user.ID, Name: "Contacts"},
		},
	}
	lockRepo := &fakeLockRepo{locks: map[string]*store.Lock{}}
	h := &Handler{store: &store.Store{AddressBooks: bookRepo, Locks: lockRepo}}

	lockBody := `<?xml version="1.0" encoding="utf-8"?><D:lockinfo xmlns:D="DAV:"><D:lockscope><D:exclusive/></D:lockscope><D:locktype><D:write/></D:locktype></D:lockinfo>`
	lockReq := httptest.NewRequest("LOCK", "/dav/addressbooks/5/", strings.NewReader(lockBody))
	lockReq.Header.Set("Depth", "infinity")
	lockReq = lockReq.WithContext(auth.WithUser(lockReq.Context(), user))
	lockRR := httptest.NewRecorder()
	h.Lock(lockRR, lockReq)

	if lockRR.Code != http.StatusOK {
		t.Fatalf("expected collection LOCK to succeed, got %d: %s", lockRR.Code, lockRR.Body.String())
	}
	token := lockRR.Header().Get("Lock-Token")
	token = strings.Trim(token, "<>")
	original, _ := lockRepo.GetByToken(context.Background(), token)
	if original == nil {
		t.Fatal("expected collection lock to be stored")
	}

	t.Run("refresh uses lock root", func(t *testing.T) {
		refreshReq := httptest.NewRequest("LOCK", "/dav/addressbooks/5/", nil)
		refreshReq.Header.Set("If", "(<"+token+">)")
		refreshReq.Header.Set("Timeout", "Second-7200")
		refreshReq = refreshReq.WithContext(auth.WithUser(refreshReq.Context(), user))
		refreshRR := httptest.NewRecorder()
		h.Lock(refreshRR, refreshReq)

		if refreshRR.Code != http.StatusOK {
			t.Fatalf("expected lock-root refresh to succeed, got %d: %s", refreshRR.Code, refreshRR.Body.String())
		}

		descendantReq := httptest.NewRequest("LOCK", "/dav/addressbooks/5/alice.vcf", nil)
		descendantReq.Header.Set("If", "(<"+token+">)")
		descendantReq.Header.Set("Timeout", "Second-3600")
		descendantReq = descendantReq.WithContext(auth.WithUser(descendantReq.Context(), user))
		descendantRR := httptest.NewRecorder()
		h.Lock(descendantRR, descendantReq)

		if descendantRR.Code != http.StatusPreconditionFailed {
			t.Fatalf("expected descendant refresh to return 412, got %d: %s", descendantRR.Code, descendantRR.Body.String())
		}
	})

	t.Run("unlock uses lock root", func(t *testing.T) {
		descendantReq := httptest.NewRequest("UNLOCK", "/dav/addressbooks/5/alice.vcf", nil)
		descendantReq.Header.Set("Lock-Token", "<"+token+">")
		descendantReq = descendantReq.WithContext(auth.WithUser(descendantReq.Context(), user))
		descendantRR := httptest.NewRecorder()
		h.Unlock(descendantRR, descendantReq)

		if descendantRR.Code != http.StatusPreconditionFailed {
			t.Fatalf("expected descendant UNLOCK to return 412, got %d: %s", descendantRR.Code, descendantRR.Body.String())
		}
		if lock, _ := lockRepo.GetByToken(context.Background(), token); lock == nil {
			t.Fatal("expected descendant UNLOCK to preserve the collection lock")
		}

		unlockReq := httptest.NewRequest("UNLOCK", "/dav/addressbooks/5/", nil)
		unlockReq.Header.Set("Lock-Token", "<"+token+">")
		unlockReq = unlockReq.WithContext(auth.WithUser(unlockReq.Context(), user))
		unlockRR := httptest.NewRecorder()
		h.Unlock(unlockRR, unlockReq)

		if unlockRR.Code != http.StatusNoContent {
			t.Fatalf("expected lock-root UNLOCK to succeed, got %d: %s", unlockRR.Code, unlockRR.Body.String())
		}
	})
}

func TestLockCoversPath(t *testing.T) {
	tests := []struct {
		name        string
		lockPath    string
		depth       string
		requestPath string
		want        bool
	}{
		{name: "exact match", lockPath: "/dav/addressbooks/5/alice", depth: "0", requestPath: "/dav/addressbooks/5/alice.vcf", want: true},
		{name: "descendant covered by infinity", lockPath: "/dav/addressbooks/5", depth: "infinity", requestPath: "/dav/addressbooks/5/alice.vcf", want: true},
		{name: "descendant not covered by depth zero", lockPath: "/dav/addressbooks/5", depth: "0", requestPath: "/dav/addressbooks/5/alice.vcf", want: false},
		{name: "sibling not covered", lockPath: "/dav/addressbooks/5/alice", depth: "infinity", requestPath: "/dav/addressbooks/5/bob.vcf", want: false},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := lockCoversPath(tc.lockPath, tc.depth, tc.requestPath); got != tc.want {
				t.Fatalf("lockCoversPath(%q, %q, %q) = %t, want %t", tc.lockPath, tc.depth, tc.requestPath, got, tc.want)
			}
		})
	}
}

func TestACLNormalizesPrincipalHrefWithoutTrailingSlash(t *testing.T) {
	owner := &store.User{ID: 1, PrimaryEmail: "owner@example.com"}
	user2 := &store.User{ID: 2, PrimaryEmail: "user2@example.com"}
	bookRepo := &fakeAddressBookRepo{
		books: map[int64]*store.AddressBook{
			5: {ID: 5, UserID: owner.ID, Name: "Shared Contacts"},
		},
	}
	aclRepo := &fakeACLRepo{}
	h := &Handler{store: &store.Store{AddressBooks: bookRepo, ACLEntries: aclRepo}}

	body := `<?xml version="1.0" encoding="utf-8"?>
<D:acl xmlns:D="DAV:">
  <D:ace>
    <D:principal><D:href>/dav/principals/2</D:href></D:principal>
    <D:grant>
      <D:privilege><D:read/></D:privilege>
    </D:grant>
  </D:ace>
</D:acl>`

	req := httptest.NewRequest("ACL", "/dav/addressbooks/5/", strings.NewReader(body))
	req = req.WithContext(auth.WithUser(req.Context(), owner))
	rr := httptest.NewRecorder()
	h.Acl(rr, req)

	if rr.Code != http.StatusOK {
		t.Fatalf("expected ACL to succeed, got %d: %s", rr.Code, rr.Body.String())
	}
	granted, err := h.checkACLPrivilege(context.Background(), user2, "/dav/addressbooks/5", "read")
	if err != nil {
		t.Fatalf("checkACLPrivilege() error = %v", err)
	}
	if !granted {
		t.Fatal("expected normalized principal href to grant read access")
	}
}

func TestACLRejectsInvalidPrivileges(t *testing.T) {
	owner := &store.User{ID: 1, PrimaryEmail: "owner@example.com"}
	bookRepo := &fakeAddressBookRepo{
		books: map[int64]*store.AddressBook{
			5: {ID: 5, UserID: owner.ID, Name: "Shared Contacts"},
		},
	}

	tests := []struct {
		name string
		body string
	}{
		{
			name: "unsupported privilege element",
			body: `<?xml version="1.0" encoding="utf-8"?>
<D:acl xmlns:D="DAV:">
  <D:ace>
    <D:principal><D:href>/dav/principals/2/</D:href></D:principal>
    <D:grant>
      <D:privilege><D:unlock/></D:privilege>
    </D:grant>
  </D:ace>
</D:acl>`,
		},
		{
			name: "mixed valid and unsupported privileges",
			body: `<?xml version="1.0" encoding="utf-8"?>
<D:acl xmlns:D="DAV:">
  <D:ace>
    <D:principal><D:href>/dav/principals/2/</D:href></D:principal>
    <D:grant>
      <D:privilege><D:read/></D:privilege>
      <D:privilege><D:unlock/></D:privilege>
    </D:grant>
  </D:ace>
</D:acl>`,
		},
		{
			name: "known privilege contains nested element",
			body: `<?xml version="1.0" encoding="utf-8"?>
<D:acl xmlns:D="DAV:">
  <D:ace>
    <D:principal><D:href>/dav/principals/2/</D:href></D:principal>
    <D:grant>
      <D:privilege>
        <D:read>
          <D:write/>
        </D:read>
      </D:privilege>
    </D:grant>
  </D:ace>
</D:acl>`,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			aclRepo := &fakeACLRepo{
				entries: []store.ACLEntry{{
					ResourcePath:  "/dav/addressbooks/5",
					PrincipalHref: "/dav/principals/9/",
					IsGrant:       true,
					Privilege:     "read",
				}},
			}
			h := &Handler{store: &store.Store{AddressBooks: bookRepo, ACLEntries: aclRepo}}

			req := httptest.NewRequest("ACL", "/dav/addressbooks/5/", strings.NewReader(tc.body))
			req = req.WithContext(auth.WithUser(req.Context(), owner))
			rr := httptest.NewRecorder()
			h.Acl(rr, req)

			if rr.Code != http.StatusBadRequest {
				t.Fatalf("expected ACL to reject invalid privilege with 400, got %d: %s", rr.Code, rr.Body.String())
			}

			entries, err := aclRepo.ListByResource(context.Background(), "/dav/addressbooks/5")
			if err != nil {
				t.Fatalf("ListByResource() error = %v", err)
			}
			if len(entries) != 1 || entries[0].PrincipalHref != "/dav/principals/9/" || entries[0].Privilege != "read" {
				t.Fatalf("expected invalid ACL request to leave stored entries unchanged, got %#v", entries)
			}
		})
	}
}

func TestLockAndACLCanonicalizeAddressBookAliases(t *testing.T) {
	t.Run("lock alias blocks canonical path writes", func(t *testing.T) {
		user := &store.User{ID: 1, PrimaryEmail: "owner@example.com"}
		bookRepo := &fakeAddressBookRepo{
			books: map[int64]*store.AddressBook{
				5: {ID: 5, UserID: user.ID, Name: "ProvisionedBook"},
			},
		}
		contactRepo := &fakeContactRepo{
			contacts: map[string]*store.Contact{
				"5:alice": {AddressBookID: 5, UID: "alice", ResourceName: "alice", RawVCard: buildVCard("3.0", "UID:alice", "FN:Alice"), ETag: "etag-a"},
			},
		}
		lockRepo := &fakeLockRepo{locks: map[string]*store.Lock{}}
		h := &Handler{store: &store.Store{AddressBooks: bookRepo, Contacts: contactRepo, Locks: lockRepo}}

		lockBody := `<?xml version="1.0" encoding="utf-8"?>
<D:lockinfo xmlns:D="DAV:">
  <D:lockscope><D:exclusive/></D:lockscope>
  <D:locktype><D:write/></D:locktype>
</D:lockinfo>`

		lockReq := httptest.NewRequest("LOCK", "/dav/addressbooks/ProvisionedBook/alice.vcf", strings.NewReader(lockBody))
		lockReq = lockReq.WithContext(auth.WithUser(lockReq.Context(), user))
		lockRR := httptest.NewRecorder()
		h.Lock(lockRR, lockReq)

		if lockRR.Code != http.StatusOK {
			t.Fatalf("expected LOCK to succeed, got %d: %s", lockRR.Code, lockRR.Body.String())
		}
		var created *store.Lock
		for _, lock := range lockRepo.locks {
			created = lock
			break
		}
		if created == nil {
			t.Fatalf("expected lock to be stored, got %#v", lockRepo.locks)
		}
		if created.ResourcePath != "/dav/addressbooks/5/alice" {
			t.Fatalf("expected canonical lock path, got %q", created.ResourcePath)
		}

		putReq := newAddressBookPutRequest("/dav/addressbooks/5/alice.vcf", strings.NewReader(buildVCard("3.0", "UID:alice", "FN:Alice Updated")))
		putReq = putReq.WithContext(auth.WithUser(putReq.Context(), user))
		putRR := httptest.NewRecorder()
		h.Put(putRR, putReq)

		if putRR.Code != http.StatusLocked {
			t.Fatalf("expected canonical path PUT to be blocked by alias lock, got %d: %s", putRR.Code, putRR.Body.String())
		}
	})

	t.Run("acl alias grants apply to canonical path", func(t *testing.T) {
		owner := &store.User{ID: 1, PrimaryEmail: "owner@example.com"}
		reader := &store.User{ID: 2, PrimaryEmail: "reader@example.com"}
		bookRepo := &fakeAddressBookRepo{
			books: map[int64]*store.AddressBook{
				5: {ID: 5, UserID: owner.ID, Name: "Contacts"},
			},
		}
		contactRepo := &fakeContactRepo{
			contacts: map[string]*store.Contact{
				"5:alice": {AddressBookID: 5, UID: "alice", ResourceName: "alice", RawVCard: buildVCard("3.0", "UID:alice", "FN:Alice"), ETag: "etag-a"},
			},
		}
		aclRepo := &fakeACLRepo{}
		h := &Handler{store: &store.Store{AddressBooks: bookRepo, Contacts: contactRepo, ACLEntries: aclRepo}}

		body := `<?xml version="1.0" encoding="utf-8"?>
<D:acl xmlns:D="DAV:">
  <D:ace>
    <D:principal><D:href>/dav/principals/2/</D:href></D:principal>
    <D:grant><D:privilege><D:read/></D:privilege></D:grant>
  </D:ace>
</D:acl>`

		aclReq := httptest.NewRequest("ACL", "/dav/addressbooks/Contacts/", strings.NewReader(body))
		aclReq = aclReq.WithContext(auth.WithUser(aclReq.Context(), owner))
		aclRR := httptest.NewRecorder()
		h.Acl(aclRR, aclReq)

		if aclRR.Code != http.StatusOK {
			t.Fatalf("expected ACL to succeed, got %d: %s", aclRR.Code, aclRR.Body.String())
		}
		entries, err := aclRepo.ListByResource(context.Background(), "/dav/addressbooks/5")
		if err != nil {
			t.Fatalf("ListByResource() error = %v", err)
		}
		if len(entries) != 1 {
			t.Fatalf("expected ACL to be stored on canonical path, got %#v", entries)
		}

		getReq := httptest.NewRequest(http.MethodGet, "/dav/addressbooks/5/alice.vcf", nil)
		getReq = getReq.WithContext(auth.WithUser(getReq.Context(), reader))
		getRR := httptest.NewRecorder()
		h.Get(getRR, getReq)

		if getRR.Code != http.StatusOK {
			t.Fatalf("expected canonical GET to honor alias ACL grant, got %d: %s", getRR.Code, getRR.Body.String())
		}
	})
}

func TestLockResponseUsesServedResourceHref(t *testing.T) {
	user := &store.User{ID: 1, PrimaryEmail: "owner@example.com"}
	bookRepo := &fakeAddressBookRepo{
		books: map[int64]*store.AddressBook{
			5: {ID: 5, UserID: user.ID, Name: "Contacts"},
		},
	}
	lockRepo := &fakeLockRepo{locks: map[string]*store.Lock{}}
	h := &Handler{store: &store.Store{AddressBooks: bookRepo, Locks: lockRepo}}

	lockBody := `<?xml version="1.0" encoding="utf-8"?>
<D:lockinfo xmlns:D="DAV:">
  <D:lockscope><D:exclusive/></D:lockscope>
  <D:locktype><D:write/></D:locktype>
</D:lockinfo>`

	req := httptest.NewRequest("LOCK", "/dav/addressbooks/5/alice.vcf", strings.NewReader(lockBody))
	req = req.WithContext(auth.WithUser(req.Context(), user))
	rr := httptest.NewRecorder()

	h.Lock(rr, req)

	if rr.Code != http.StatusOK {
		t.Fatalf("expected LOCK to succeed, got %d: %s", rr.Code, rr.Body.String())
	}
	if !strings.Contains(rr.Body.String(), "/dav/addressbooks/5/alice.vcf") {
		t.Fatalf("expected LOCK response to advertise a served resource href, got %s", rr.Body.String())
	}
}

func TestLockCreateRequiresRequestBody(t *testing.T) {
	user := &store.User{ID: 1, PrimaryEmail: "owner@example.com"}
	bookRepo := &fakeAddressBookRepo{
		books: map[int64]*store.AddressBook{
			5: {ID: 5, UserID: user.ID, Name: "Contacts"},
		},
	}
	contactRepo := &fakeContactRepo{
		contacts: map[string]*store.Contact{
			"5:alice": {AddressBookID: 5, UID: "alice", ResourceName: "alice", RawVCard: buildVCard("3.0", "UID:alice", "FN:Alice"), ETag: "etag-a"},
		},
	}
	lockRepo := &fakeLockRepo{locks: map[string]*store.Lock{}}
	h := &Handler{store: &store.Store{AddressBooks: bookRepo, Contacts: contactRepo, Locks: lockRepo}}

	req := httptest.NewRequest("LOCK", "/dav/addressbooks/5/alice.vcf", nil)
	req = req.WithContext(auth.WithUser(req.Context(), user))
	rr := httptest.NewRecorder()

	h.Lock(rr, req)

	if rr.Code != http.StatusBadRequest {
		t.Fatalf("expected bodyless LOCK create to return 400, got %d: %s", rr.Code, rr.Body.String())
	}
	if len(lockRepo.locks) != 0 {
		t.Fatalf("expected bodyless LOCK create to persist no locks, got %#v", lockRepo.locks)
	}
}

func TestLockRejectsInvalidRequestBodies(t *testing.T) {
	user := &store.User{ID: 1, PrimaryEmail: "owner@example.com"}
	bookRepo := &fakeAddressBookRepo{
		books: map[int64]*store.AddressBook{
			5: {ID: 5, UserID: user.ID, Name: "Contacts"},
		},
	}
	lockRepo := &fakeLockRepo{locks: map[string]*store.Lock{}}
	h := &Handler{store: &store.Store{AddressBooks: bookRepo, Contacts: &fakeContactRepo{}, Locks: lockRepo}}

	tests := []struct {
		name string
		body string
	}{
		{
			name: "malformed XML",
			body: `<D:lockinfo xmlns:D="DAV:"><D:lockscope><D:exclusive/></D:lockscope>`,
		},
		{
			name: "missing lockscope",
			body: `<?xml version="1.0" encoding="utf-8"?>
<D:lockinfo xmlns:D="DAV:">
  <D:locktype><D:write/></D:locktype>
</D:lockinfo>`,
		},
		{
			name: "missing locktype",
			body: `<?xml version="1.0" encoding="utf-8"?>
<D:lockinfo xmlns:D="DAV:">
  <D:lockscope><D:exclusive/></D:lockscope>
</D:lockinfo>`,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			req := httptest.NewRequest("LOCK", "/dav/addressbooks/5/alice.vcf", strings.NewReader(tc.body))
			req = req.WithContext(auth.WithUser(req.Context(), user))
			rr := httptest.NewRecorder()

			h.Lock(rr, req)

			if rr.Code != http.StatusBadRequest {
				t.Fatalf("expected invalid LOCK request to return 400, got %d: %s", rr.Code, rr.Body.String())
			}
			if len(lockRepo.locks) != 0 {
				t.Fatalf("expected invalid LOCK request to persist no locks, got %#v", lockRepo.locks)
			}
		})
	}
}

func TestLockOnUnmappedCollectionReturnsCreated(t *testing.T) {
	user := &store.User{ID: 1, PrimaryEmail: "owner@example.com"}
	bookRepo := &fakeAddressBookRepo{}
	lockRepo := &fakeLockRepo{locks: map[string]*store.Lock{}}
	h := &Handler{store: &store.Store{AddressBooks: bookRepo, Locks: lockRepo}}

	lockBody := `<?xml version="1.0" encoding="utf-8"?>
<D:lockinfo xmlns:D="DAV:">
  <D:lockscope><D:exclusive/></D:lockscope>
  <D:locktype><D:write/></D:locktype>
</D:lockinfo>`

	req := httptest.NewRequest("LOCK", "/dav/addressbooks/NewBook", strings.NewReader(lockBody))
	req = req.WithContext(auth.WithUser(req.Context(), user))
	rr := httptest.NewRecorder()

	h.Lock(rr, req)

	if rr.Code != http.StatusCreated {
		t.Fatalf("expected LOCK on an unmapped collection to return 201, got %d: %s", rr.Code, rr.Body.String())
	}
	if rr.Header().Get("Lock-Token") == "" {
		t.Fatal("expected LOCK on an unmapped collection to return a lock token")
	}
}

func TestLockDoesNotPersistWhenTargetResolutionFails(t *testing.T) {
	user := &store.User{ID: 1, PrimaryEmail: "owner@example.com"}
	bookRepo := &fakeAddressBookRepo{
		books: map[int64]*store.AddressBook{
			5: {ID: 5, UserID: user.ID, Name: "Contacts"},
		},
	}
	contactRepo := &fakeContactRepo{
		getByResourceNameErr:    errors.New("lookup failed"),
		getByResourceNameErrKey: "5:alice",
	}
	lockRepo := &fakeLockRepo{locks: map[string]*store.Lock{}}
	h := &Handler{store: &store.Store{AddressBooks: bookRepo, Contacts: contactRepo, Locks: lockRepo}}

	lockBody := `<?xml version="1.0" encoding="utf-8"?>
<D:lockinfo xmlns:D="DAV:">
  <D:lockscope><D:exclusive/></D:lockscope>
  <D:locktype><D:write/></D:locktype>
</D:lockinfo>`

	req := httptest.NewRequest("LOCK", "/dav/addressbooks/5/alice.vcf", strings.NewReader(lockBody))
	req = req.WithContext(auth.WithUser(req.Context(), user))
	rr := httptest.NewRecorder()

	h.Lock(rr, req)

	if rr.Code != http.StatusInternalServerError {
		t.Fatalf("expected LOCK target resolution failure to return 500, got %d: %s", rr.Code, rr.Body.String())
	}
	if len(lockRepo.locks) != 0 {
		t.Fatalf("expected failed LOCK to leave no persisted lock, got %#v", lockRepo.locks)
	}
}

func TestMkcolRebindsPendingCollectionLocks(t *testing.T) {
	user := &store.User{ID: 1, PrimaryEmail: "owner@example.com"}
	bookRepo := &fakeAddressBookRepo{}
	lockRepo := &fakeLockRepo{locks: map[string]*store.Lock{}}
	h := &Handler{store: &store.Store{AddressBooks: bookRepo, Locks: lockRepo}}

	lockBody := `<?xml version="1.0" encoding="utf-8"?>
<D:lockinfo xmlns:D="DAV:">
  <D:lockscope><D:exclusive/></D:lockscope>
  <D:locktype><D:write/></D:locktype>
</D:lockinfo>`

	lockReq := httptest.NewRequest("LOCK", "/dav/addressbooks/NewBook", strings.NewReader(lockBody))
	lockReq = lockReq.WithContext(auth.WithUser(lockReq.Context(), user))
	lockRR := httptest.NewRecorder()
	h.Lock(lockRR, lockReq)

	if lockRR.Code != http.StatusCreated {
		t.Fatalf("expected LOCK to succeed with 201, got %d: %s", lockRR.Code, lockRR.Body.String())
	}
	token := strings.Trim(lockRR.Header().Get("Lock-Token"), "<>")
	if token == "" {
		t.Fatal("expected lock token to be returned")
	}

	createReq := httptest.NewRequest("MKCOL", "/dav/addressbooks/NewBook", nil)
	createReq.Header.Set("If", "(<"+token+">)")
	createReq = createReq.WithContext(auth.WithUser(createReq.Context(), user))
	createRR := httptest.NewRecorder()
	h.Mkcol(createRR, createReq)

	if createRR.Code != http.StatusCreated {
		t.Fatalf("expected MKCOL to succeed, got %d: %s", createRR.Code, createRR.Body.String())
	}
	if got := createRR.Header().Get("Location"); got != "/dav/addressbooks/1/" {
		t.Fatalf("expected MKCOL location to point at the created collection, got %q", got)
	}
	createdLock, err := lockRepo.GetByToken(context.Background(), token)
	if err != nil {
		t.Fatalf("GetByToken() error = %v", err)
	}
	if createdLock == nil || createdLock.ResourcePath != "/dav/addressbooks/1" {
		t.Fatalf("expected pending lock to be rebound to created collection, got %#v", createdLock)
	}

	body := `<?xml version="1.0" encoding="utf-8"?>
<d:propertyupdate xmlns:d="DAV:" xmlns:card="urn:ietf:params:xml:ns:carddav">
  <d:set>
    <d:prop>
      <d:displayname>Renamed</d:displayname>
    </d:prop>
  </d:set>
</d:propertyupdate>`

	withoutTokenReq := httptest.NewRequest("PROPPATCH", "/dav/addressbooks/1/", strings.NewReader(body))
	withoutTokenReq = withoutTokenReq.WithContext(auth.WithUser(withoutTokenReq.Context(), user))
	withoutTokenRR := httptest.NewRecorder()
	h.Proppatch(withoutTokenRR, withoutTokenReq)

	if withoutTokenRR.Code != http.StatusLocked {
		t.Fatalf("expected PROPPATCH without a lock token to be blocked after MKCOL, got %d: %s", withoutTokenRR.Code, withoutTokenRR.Body.String())
	}

	withTokenReq := httptest.NewRequest("PROPPATCH", "/dav/addressbooks/1/", strings.NewReader(body))
	withTokenReq.Header.Set("If", "(<"+token+">)")
	withTokenReq = withTokenReq.WithContext(auth.WithUser(withTokenReq.Context(), user))
	withTokenRR := httptest.NewRecorder()
	h.Proppatch(withTokenRR, withTokenReq)

	if withTokenRR.Code != http.StatusMultiStatus {
		t.Fatalf("expected PROPPATCH with the lock token to succeed after MKCOL, got %d: %s", withTokenRR.Code, withTokenRR.Body.String())
	}
}

func TestMkcolAppliesExtendedBodyProperties(t *testing.T) {
	bookRepo := &fakeAddressBookRepo{}
	h := &Handler{store: &store.Store{AddressBooks: bookRepo}}

	body := `<?xml version="1.0" encoding="utf-8"?>
<d:mkcol xmlns:d="DAV:" xmlns:card="urn:ietf:params:xml:ns:carddav">
  <d:set>
    <d:prop>
      <d:displayname>Renamed From Body</d:displayname>
      <card:addressbook-description>Personal contacts</card:addressbook-description>
    </d:prop>
  </d:set>
</d:mkcol>`
	req := httptest.NewRequest("MKCOL", "/dav/addressbooks/tmp", strings.NewReader(body))
	req = req.WithContext(auth.WithUser(req.Context(), &store.User{ID: 1}))
	rr := httptest.NewRecorder()

	h.Mkcol(rr, req)

	if rr.Code != http.StatusCreated {
		t.Fatalf("expected MKCOL extended body to succeed, got %d: %s", rr.Code, rr.Body.String())
	}
	if got := rr.Header().Get("Location"); got != "/dav/addressbooks/1/" {
		t.Fatalf("expected Location for created address book, got %q", got)
	}
	book := bookRepo.books[1]
	if book == nil {
		t.Fatal("expected address book to be created")
	}
	if book.Name != "Renamed From Body" {
		t.Fatalf("expected displayname from body to be used, got %q", book.Name)
	}
	if book.Description == nil || *book.Description != "Personal contacts" {
		t.Fatalf("expected address book description from body, got %#v", book.Description)
	}
}

func TestMkcolRejectsOversizedRequestBody(t *testing.T) {
	bookRepo := &fakeAddressBookRepo{}
	h := &Handler{store: &store.Store{AddressBooks: bookRepo}}

	req := httptest.NewRequest("MKCOL", "/dav/addressbooks/tmp", strings.NewReader(strings.Repeat("x", int(maxDAVBodyBytes)+1)))
	req = req.WithContext(auth.WithUser(req.Context(), &store.User{ID: 1}))
	rr := httptest.NewRecorder()

	h.Mkcol(rr, req)

	if rr.Code != http.StatusRequestEntityTooLarge {
		t.Fatalf("expected oversized MKCOL body to return 413, got %d: %s", rr.Code, rr.Body.String())
	}
	if len(bookRepo.books) != 0 {
		t.Fatalf("expected oversized MKCOL to create no address book, got %#v", bookRepo.books)
	}
}

func TestMkcolReturnsInternalServerErrorWhenLockRebindFails(t *testing.T) {
	user := &store.User{ID: 1, PrimaryEmail: "owner@example.com"}
	bookRepo := &fakeAddressBookRepo{}
	lockRepo := &fakeLockRepo{
		locks:               map[string]*store.Lock{},
		moveResourcePathErr: errors.New("rebind failed"),
	}
	h := &Handler{store: &store.Store{AddressBooks: bookRepo, Locks: lockRepo}}

	lockBody := `<?xml version="1.0" encoding="utf-8"?>
<D:lockinfo xmlns:D="DAV:">
  <D:lockscope><D:exclusive/></D:lockscope>
  <D:locktype><D:write/></D:locktype>
</D:lockinfo>`

	lockReq := httptest.NewRequest("LOCK", "/dav/addressbooks/NewBook", strings.NewReader(lockBody))
	lockReq = lockReq.WithContext(auth.WithUser(lockReq.Context(), user))
	lockRR := httptest.NewRecorder()
	h.Lock(lockRR, lockReq)
	token := strings.Trim(lockRR.Header().Get("Lock-Token"), "<>")

	createReq := httptest.NewRequest("MKCOL", "/dav/addressbooks/NewBook", nil)
	createReq.Header.Set("If", "(<"+token+">)")
	createReq = createReq.WithContext(auth.WithUser(createReq.Context(), user))
	createRR := httptest.NewRecorder()
	h.Mkcol(createRR, createReq)

	if createRR.Code != http.StatusInternalServerError {
		t.Fatalf("expected MKCOL lock rebind failure to return 500, got %d: %s", createRR.Code, createRR.Body.String())
	}
	if got := createRR.Header().Get("Location"); got != "" {
		t.Fatalf("expected MKCOL rebind failure to avoid Location header, got %q", got)
	}
	if len(bookRepo.books) != 0 {
		t.Fatalf("expected MKCOL rebind failure to roll back created address book, got %#v", bookRepo.books)
	}
}

func TestMkcalendarRebindsPendingCollectionLocks(t *testing.T) {
	user := &store.User{ID: 1, PrimaryEmail: "owner@example.com"}
	calRepo := &fakeCalendarRepo{}
	lockRepo := &fakeLockRepo{locks: map[string]*store.Lock{}}
	h := &Handler{store: &store.Store{Calendars: calRepo, Locks: lockRepo}}

	lockBody := `<?xml version="1.0" encoding="utf-8"?>
<D:lockinfo xmlns:D="DAV:">
  <D:lockscope><D:exclusive/></D:lockscope>
  <D:locktype><D:write/></D:locktype>
</D:lockinfo>`

	lockReq := httptest.NewRequest("LOCK", "/dav/calendars/work", strings.NewReader(lockBody))
	lockReq = lockReq.WithContext(auth.WithUser(lockReq.Context(), user))
	lockRR := httptest.NewRecorder()
	h.Lock(lockRR, lockReq)

	if lockRR.Code != http.StatusCreated {
		t.Fatalf("expected LOCK to succeed with 201, got %d: %s", lockRR.Code, lockRR.Body.String())
	}
	token := strings.Trim(lockRR.Header().Get("Lock-Token"), "<>")
	if token == "" {
		t.Fatal("expected lock token to be returned")
	}

	createReq := httptest.NewRequest("MKCALENDAR", "/dav/calendars/work", nil)
	createReq.Header.Set("If", "(<"+token+">)")
	createReq = createReq.WithContext(auth.WithUser(createReq.Context(), user))
	createRR := httptest.NewRecorder()
	h.Mkcalendar(createRR, createReq)

	if createRR.Code != http.StatusCreated {
		t.Fatalf("expected MKCALENDAR to succeed, got %d: %s", createRR.Code, createRR.Body.String())
	}
	if got := createRR.Header().Get("Location"); got != "/dav/calendars/1/" {
		t.Fatalf("expected MKCALENDAR location to point at the created collection, got %q", got)
	}
	createdLock, err := lockRepo.GetByToken(context.Background(), token)
	if err != nil {
		t.Fatalf("GetByToken() error = %v", err)
	}
	if createdLock == nil || createdLock.ResourcePath != "/dav/calendars/1" {
		t.Fatalf("expected pending lock to be rebound to created collection, got %#v", createdLock)
	}

	body := `<?xml version="1.0" encoding="utf-8"?>
<d:propertyupdate xmlns:d="DAV:" xmlns:c="urn:ietf:params:xml:ns:caldav">
  <d:set>
    <d:prop>
      <d:displayname>Renamed</d:displayname>
    </d:prop>
  </d:set>
</d:propertyupdate>`

	withoutTokenReq := httptest.NewRequest("PROPPATCH", "/dav/calendars/1/", strings.NewReader(body))
	withoutTokenReq = withoutTokenReq.WithContext(auth.WithUser(withoutTokenReq.Context(), user))
	withoutTokenRR := httptest.NewRecorder()
	h.Proppatch(withoutTokenRR, withoutTokenReq)

	if withoutTokenRR.Code != http.StatusLocked {
		t.Fatalf("expected PROPPATCH without a lock token to be blocked after MKCALENDAR, got %d: %s", withoutTokenRR.Code, withoutTokenRR.Body.String())
	}

	withTokenReq := httptest.NewRequest("PROPPATCH", "/dav/calendars/1/", strings.NewReader(body))
	withTokenReq.Header.Set("If", "(<"+token+">)")
	withTokenReq = withTokenReq.WithContext(auth.WithUser(withTokenReq.Context(), user))
	withTokenRR := httptest.NewRecorder()
	h.Proppatch(withTokenRR, withTokenReq)

	if withTokenRR.Code != http.StatusMultiStatus {
		t.Fatalf("expected PROPPATCH with the lock token to succeed after MKCALENDAR, got %d: %s", withTokenRR.Code, withTokenRR.Body.String())
	}
}

func TestMkcalendarRejectsInvalidBodyAndDoesNotCreate(t *testing.T) {
	calRepo := &fakeCalendarRepo{}
	h := &Handler{store: &store.Store{Calendars: calRepo}}

	req := httptest.NewRequest("MKCALENDAR", "/dav/calendars/work", strings.NewReader(`<d:mkcalendar xmlns:d="DAV:"><d:set>`))
	req = req.WithContext(auth.WithUser(req.Context(), &store.User{ID: 1}))
	rr := httptest.NewRecorder()

	h.Mkcalendar(rr, req)

	if rr.Code != http.StatusBadRequest {
		t.Fatalf("expected MKCALENDAR to reject invalid body, got %d: %s", rr.Code, rr.Body.String())
	}
	if len(calRepo.calendars) != 0 {
		t.Fatalf("expected invalid MKCALENDAR body to create no calendar, got %#v", calRepo.calendars)
	}
}

func TestMkcalendarRejectsOversizedRequestBody(t *testing.T) {
	calRepo := &fakeCalendarRepo{}
	h := &Handler{store: &store.Store{Calendars: calRepo}}

	req := httptest.NewRequest("MKCALENDAR", "/dav/calendars/work", strings.NewReader(strings.Repeat("x", int(maxDAVBodyBytes)+1)))
	req = req.WithContext(auth.WithUser(req.Context(), &store.User{ID: 1}))
	rr := httptest.NewRecorder()

	h.Mkcalendar(rr, req)

	if rr.Code != http.StatusRequestEntityTooLarge {
		t.Fatalf("expected oversized MKCALENDAR body to return 413, got %d: %s", rr.Code, rr.Body.String())
	}
	if len(calRepo.calendars) != 0 {
		t.Fatalf("expected oversized MKCALENDAR to create no calendar, got %#v", calRepo.calendars)
	}
}

func TestMkcalendarReturnsInternalServerErrorWhenLockRebindFails(t *testing.T) {
	user := &store.User{ID: 1, PrimaryEmail: "owner@example.com"}
	calRepo := &fakeCalendarRepo{}
	lockRepo := &fakeLockRepo{
		locks:               map[string]*store.Lock{},
		moveResourcePathErr: errors.New("rebind failed"),
	}
	h := &Handler{store: &store.Store{Calendars: calRepo, Locks: lockRepo}}

	lockBody := `<?xml version="1.0" encoding="utf-8"?>
<D:lockinfo xmlns:D="DAV:">
  <D:lockscope><D:exclusive/></D:lockscope>
  <D:locktype><D:write/></D:locktype>
</D:lockinfo>`

	lockReq := httptest.NewRequest("LOCK", "/dav/calendars/work", strings.NewReader(lockBody))
	lockReq = lockReq.WithContext(auth.WithUser(lockReq.Context(), user))
	lockRR := httptest.NewRecorder()
	h.Lock(lockRR, lockReq)
	token := strings.Trim(lockRR.Header().Get("Lock-Token"), "<>")

	createReq := httptest.NewRequest("MKCALENDAR", "/dav/calendars/work", nil)
	createReq.Header.Set("If", "(<"+token+">)")
	createReq = createReq.WithContext(auth.WithUser(createReq.Context(), user))
	createRR := httptest.NewRecorder()
	h.Mkcalendar(createRR, createReq)

	if createRR.Code != http.StatusInternalServerError {
		t.Fatalf("expected MKCALENDAR lock rebind failure to return 500, got %d: %s", createRR.Code, createRR.Body.String())
	}
	if got := createRR.Header().Get("Location"); got != "" {
		t.Fatalf("expected MKCALENDAR rebind failure to avoid Location header, got %q", got)
	}
	if len(calRepo.calendars) != 0 {
		t.Fatalf("expected MKCALENDAR rebind failure to roll back created calendar, got %#v", calRepo.calendars)
	}
}

func TestPutAddressBookMapsUpsertConflictsToCardDAVConflict(t *testing.T) {
	user := &store.User{ID: 1, PrimaryEmail: "owner@example.com"}
	bookRepo := &fakeAddressBookRepo{
		books: map[int64]*store.AddressBook{
			5: {ID: 5, UserID: user.ID, Name: "Contacts"},
		},
	}
	contactRepo := &fakeContactRepo{upsertErr: store.ErrConflict}
	h := &Handler{store: &store.Store{AddressBooks: bookRepo, Contacts: contactRepo, Locks: &fakeLockRepo{}}}

	req := newAddressBookPutRequest("/dav/addressbooks/5/alice.vcf", strings.NewReader(buildVCard("3.0", "UID:alice", "FN:Alice Example")))
	req = req.WithContext(auth.WithUser(req.Context(), user))
	rr := httptest.NewRecorder()

	h.Put(rr, req)

	if rr.Code != http.StatusConflict {
		t.Fatalf("expected CardDAV PUT conflict to return 409, got %d: %s", rr.Code, rr.Body.String())
	}
	if !strings.Contains(rr.Body.String(), "no-uid-conflict") {
		t.Fatalf("expected CardDAV PUT conflict body to contain no-uid-conflict, got %s", rr.Body.String())
	}
}

func TestPutAddressBookPreservesInternalErrorsFromUpsert(t *testing.T) {
	user := &store.User{ID: 1, PrimaryEmail: "owner@example.com"}
	bookRepo := &fakeAddressBookRepo{
		books: map[int64]*store.AddressBook{
			5: {ID: 5, UserID: user.ID, Name: "Contacts"},
		},
	}
	contactRepo := &fakeContactRepo{upsertErr: errors.New("db unavailable")}
	h := &Handler{store: &store.Store{AddressBooks: bookRepo, Contacts: contactRepo, Locks: &fakeLockRepo{}}}

	req := newAddressBookPutRequest("/dav/addressbooks/5/alice.vcf", strings.NewReader(buildVCard("3.0", "UID:alice", "FN:Alice Example")))
	req = req.WithContext(auth.WithUser(req.Context(), user))
	rr := httptest.NewRecorder()

	h.Put(rr, req)

	if rr.Code != http.StatusInternalServerError {
		t.Fatalf("expected unexpected upsert error to return 500, got %d: %s", rr.Code, rr.Body.String())
	}
	if strings.Contains(rr.Body.String(), "no-uid-conflict") {
		t.Fatalf("did not expect generic upsert failures to be reported as CardDAV conflicts, got %s", rr.Body.String())
	}
}

func TestCopyUsesTaggedIfTokensForLockedSourceAndDestination(t *testing.T) {
	user := &store.User{ID: 1, PrimaryEmail: "owner@example.com"}
	bookRepo := &fakeAddressBookRepo{
		books: map[int64]*store.AddressBook{
			5: {ID: 5, UserID: user.ID, Name: "Source"},
			6: {ID: 6, UserID: user.ID, Name: "Destination"},
		},
	}
	contactRepo := &fakeContactRepo{
		contacts: map[string]*store.Contact{
			"5:alice": {AddressBookID: 5, UID: "alice", ResourceName: "alice", RawVCard: buildVCard("3.0", "UID:alice", "FN:Alice Example"), ETag: "etag-alice"},
		},
	}
	lockRepo := &fakeLockRepo{
		locks: map[string]*store.Lock{
			"opaquelocktoken:src": {
				Token:        "opaquelocktoken:src",
				ResourcePath: "/dav/addressbooks/5/alice.vcf",
				UserID:       user.ID,
				LockScope:    "exclusive",
				LockType:     "write",
				Depth:        "0",
				ExpiresAt:    time.Now().Add(time.Hour),
			},
			"opaquelocktoken:dest": {
				Token:        "opaquelocktoken:dest",
				ResourcePath: "/dav/addressbooks/6/copied.vcf",
				UserID:       user.ID,
				LockScope:    "exclusive",
				LockType:     "write",
				Depth:        "0",
				ExpiresAt:    time.Now().Add(time.Hour),
			},
		},
	}
	h := &Handler{store: &store.Store{AddressBooks: bookRepo, Contacts: contactRepo, Locks: lockRepo}}

	req := httptest.NewRequest("COPY", "/dav/addressbooks/5/alice.vcf", nil)
	req.Header.Set("Destination", "https://example.com/dav/addressbooks/6/copied.vcf")
	req.Header.Set("If", `</dav/addressbooks/5/alice.vcf> (<opaquelocktoken:src>) </dav/addressbooks/6/copied.vcf> (<opaquelocktoken:dest>)`)
	req = req.WithContext(auth.WithUser(req.Context(), user))
	rr := httptest.NewRecorder()

	h.Copy(rr, req)

	if rr.Code != http.StatusCreated {
		t.Fatalf("expected COPY with tagged lock tokens to succeed, got %d: %s", rr.Code, rr.Body.String())
	}
	if copied, _ := contactRepo.GetByResourceName(req.Context(), 6, "copied"); copied == nil {
		t.Fatal("expected destination contact to be created")
	}
}

func TestPropfindAddressBookCollectionIncludesLockAndACLProperties(t *testing.T) {
	user := &store.User{ID: 1, PrimaryEmail: "owner@example.com"}
	bookRepo := &fakeAddressBookRepo{
		books: map[int64]*store.AddressBook{
			5: {ID: 5, UserID: user.ID, Name: "Shared Contacts"},
		},
	}
	lockRepo := &fakeLockRepo{
		locks: map[string]*store.Lock{
			"opaquelocktoken:book": {
				Token:          "opaquelocktoken:book",
				ResourcePath:   "/dav/addressbooks/5",
				UserID:         user.ID,
				LockScope:      "exclusive",
				LockType:       "write",
				Depth:          "0",
				TimeoutSeconds: 3600,
				ExpiresAt:      time.Now().Add(time.Hour),
			},
		},
	}
	aclRepo := &fakeACLRepo{
		entries: []store.ACLEntry{
			{ResourcePath: "/dav/addressbooks/5", PrincipalHref: "/dav/principals/2/", IsGrant: true, Privilege: "read"},
		},
	}
	h := &Handler{store: &store.Store{AddressBooks: bookRepo, Locks: lockRepo, ACLEntries: aclRepo}}

	body := `<?xml version="1.0" encoding="utf-8"?>
<d:propfind xmlns:d="DAV:">
  <d:prop>
    <d:supportedlock/>
    <d:lockdiscovery/>
    <d:acl/>
    <d:supported-privilege-set/>
    <d:principal-collection-set/>
  </d:prop>
</d:propfind>`
	req := httptest.NewRequest("PROPFIND", "/dav/addressbooks/5/", strings.NewReader(body))
	req.Header.Set("Depth", "0")
	req = req.WithContext(auth.WithUser(req.Context(), user))
	rr := httptest.NewRecorder()

	h.Propfind(rr, req)

	if rr.Code != http.StatusMultiStatus {
		t.Fatalf("expected PROPFIND to succeed, got %d: %s", rr.Code, rr.Body.String())
	}
	respBody := rr.Body.String()
	for _, needle := range []string{"supportedlock", "lockdiscovery", "opaquelocktoken:book", "supported-privilege-set", "principal-collection-set", "/dav/principals/", "<d:acl>"} {
		if !strings.Contains(respBody, needle) {
			t.Fatalf("expected PROPFIND response to include %q, got %s", needle, respBody)
		}
	}
}

func TestACLRejectsInvalidACEs(t *testing.T) {
	owner := &store.User{ID: 1, PrimaryEmail: "owner@example.com"}
	bookRepo := &fakeAddressBookRepo{
		books: map[int64]*store.AddressBook{
			5: {ID: 5, UserID: owner.ID, Name: "Shared Contacts"},
		},
	}

	tests := []struct {
		name string
		body string
	}{
		{
			name: "ace missing grant and deny",
			body: `<?xml version="1.0" encoding="utf-8"?>
<D:acl xmlns:D="DAV:">
  <D:ace>
    <D:principal><D:href>/dav/principals/2/</D:href></D:principal>
  </D:ace>
</D:acl>`,
		},
		{
			name: "ace has both grant and deny",
			body: `<?xml version="1.0" encoding="utf-8"?>
<D:acl xmlns:D="DAV:">
  <D:ace>
    <D:principal><D:href>/dav/principals/2/</D:href></D:principal>
    <D:grant>
      <D:privilege><D:read/></D:privilege>
    </D:grant>
    <D:deny>
      <D:privilege><D:write/></D:privilege>
    </D:deny>
  </D:ace>
</D:acl>`,
		},
		{
			name: "ace grant has no privileges",
			body: `<?xml version="1.0" encoding="utf-8"?>
<D:acl xmlns:D="DAV:">
  <D:ace>
    <D:principal><D:href>/dav/principals/2/</D:href></D:principal>
    <D:grant/>
  </D:ace>
</D:acl>`,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			aclRepo := &fakeACLRepo{
				entries: []store.ACLEntry{{
					ResourcePath:  "/dav/addressbooks/5",
					PrincipalHref: "/dav/principals/9/",
					IsGrant:       true,
					Privilege:     "read",
				}},
			}
			h := &Handler{store: &store.Store{AddressBooks: bookRepo, ACLEntries: aclRepo}}

			req := httptest.NewRequest("ACL", "/dav/addressbooks/5/", strings.NewReader(tc.body))
			req = req.WithContext(auth.WithUser(req.Context(), owner))
			rr := httptest.NewRecorder()
			h.Acl(rr, req)

			if rr.Code != http.StatusBadRequest {
				t.Fatalf("expected ACL to reject invalid ACE with 400, got %d: %s", rr.Code, rr.Body.String())
			}

			entries, err := aclRepo.ListByResource(context.Background(), "/dav/addressbooks/5")
			if err != nil {
				t.Fatalf("ListByResource() error = %v", err)
			}
			if len(entries) != 1 || entries[0].PrincipalHref != "/dav/principals/9/" || entries[0].Privilege != "read" {
				t.Fatalf("expected invalid ACL request to leave stored entries unchanged, got %#v", entries)
			}
		})
	}
}

func TestPropfindAddressBookCurrentUserPrivilegeSetForDelegate(t *testing.T) {
	owner := &store.User{ID: 1, PrimaryEmail: "owner@example.com"}
	delegate := &store.User{ID: 2, PrimaryEmail: "delegate@example.com"}
	bookRepo := &fakeAddressBookRepo{
		books: map[int64]*store.AddressBook{
			5: {ID: 5, UserID: owner.ID, Name: "Contacts"},
		},
	}
	aclRepo := &fakeACLRepo{
		entries: []store.ACLEntry{
			{ResourcePath: "/dav/addressbooks/5", PrincipalHref: "/dav/principals/2/", IsGrant: true, Privilege: "read"},
			{ResourcePath: "/dav/addressbooks/5", PrincipalHref: "/dav/principals/2/", IsGrant: true, Privilege: "bind"},
			{ResourcePath: "/dav/addressbooks/5", PrincipalHref: "/dav/principals/2/", IsGrant: true, Privilege: "write-content"},
		},
	}
	h := &Handler{store: &store.Store{AddressBooks: bookRepo, ACLEntries: aclRepo}}
	if privs := h.currentUserPrivilegeSetForPath(context.Background(), delegate, "/dav/addressbooks/5/"); privs == nil || len(privs.Privileges) == 0 {
		t.Fatalf("expected computed privilege set for delegate, got %#v", privs)
	}

	req := httptest.NewRequest("PROPFIND", "/dav/addressbooks/5/", nil)
	req.Header.Set("Depth", "0")
	req = req.WithContext(auth.WithUser(req.Context(), delegate))
	rr := httptest.NewRecorder()

	h.Propfind(rr, req)

	if rr.Code != http.StatusMultiStatus {
		t.Fatalf("expected PROPFIND to succeed, got %d: %s", rr.Code, rr.Body.String())
	}
	respBody := rr.Body.String()
	for _, needle := range []string{"current-user-privilege-set", "<d:read", "<d:bind", "<d:write-content"} {
		if !strings.Contains(respBody, needle) {
			t.Fatalf("expected PROPFIND response to include %q, got %s", needle, respBody)
		}
	}
}

func TestPropfindCalendarCurrentUserPrivilegeSetForDelegate(t *testing.T) {
	owner := &store.User{ID: 1, PrimaryEmail: "owner@example.com"}
	delegate := &store.User{ID: 2, PrimaryEmail: "delegate@example.com"}
	calRepo := &fakeCalendarRepo{
		accessibleByUser: map[int64][]store.CalendarAccess{
			owner.ID: {
				{Calendar: store.Calendar{ID: 5, UserID: owner.ID, Name: "Work"}, Editor: true},
			},
		},
		calendars: map[int64]*store.Calendar{
			5: {ID: 5, UserID: owner.ID, Name: "Work"},
		},
	}
	aclRepo := &fakeACLRepo{
		entries: []store.ACLEntry{
			{ResourcePath: "/dav/calendars/5", PrincipalHref: "/dav/principals/2/", IsGrant: true, Privilege: "read"},
			{ResourcePath: "/dav/calendars/5", PrincipalHref: "/dav/principals/2/", IsGrant: true, Privilege: "bind"},
			{ResourcePath: "/dav/calendars/5", PrincipalHref: "/dav/principals/2/", IsGrant: true, Privilege: "write-content"},
		},
	}
	h := &Handler{store: &store.Store{Calendars: calRepo, ACLEntries: aclRepo}}
	if privs := h.currentUserPrivilegeSetForPath(context.Background(), delegate, "/dav/calendars/5/"); privs == nil || len(privs.Privileges) == 0 {
		t.Fatalf("expected computed privilege set for delegate, got %#v", privs)
	}

	req := httptest.NewRequest("PROPFIND", "/dav/calendars/5/", nil)
	req.Header.Set("Depth", "0")
	req = req.WithContext(auth.WithUser(req.Context(), delegate))
	rr := httptest.NewRecorder()

	h.Propfind(rr, req)

	if rr.Code != http.StatusMultiStatus {
		t.Fatalf("expected PROPFIND to succeed, got %d: %s", rr.Code, rr.Body.String())
	}
	respBody := rr.Body.String()
	for _, needle := range []string{"current-user-privilege-set", "<d:read", "<d:bind", "<d:write-content"} {
		if !strings.Contains(respBody, needle) {
			t.Fatalf("expected PROPFIND response to include %q, got %s", needle, respBody)
		}
	}
}

func TestCalendarCurrentUserPrivilegeSetForReadFreeBusyDelegate(t *testing.T) {
	delegate := &store.User{ID: 2, PrimaryEmail: "delegate@example.com"}
	calRepo := &fakeCalendarRepo{
		calendars: map[int64]*store.Calendar{
			5: {ID: 5, UserID: 1, Name: "Work"},
		},
	}
	aclRepo := &fakeACLRepo{
		entries: []store.ACLEntry{
			{ResourcePath: "/dav/calendars/5", PrincipalHref: "/dav/principals/2/", IsGrant: true, Privilege: "read-free-busy"},
		},
	}
	h := &Handler{store: &store.Store{Calendars: calRepo, ACLEntries: aclRepo}}

	privs := h.currentUserPrivilegeSetForPath(context.Background(), delegate, "/dav/calendars/5/")
	if privs == nil {
		t.Fatal("expected computed privilege set for read-free-busy delegate")
	}
	if len(privs.Privileges) != 1 || privs.Privileges[0].ReadFreeBusy == nil {
		t.Fatalf("expected only read-free-busy privilege, got %#v", privs)
	}
	if privs.Privileges[0].Read != nil {
		t.Fatalf("did not expect DAV:read privilege, got %#v", privs)
	}
}

func TestCalendarCurrentUserPrivilegeSetOmitsAggregateWriteWhenSubPrivilegeDenied(t *testing.T) {
	delegate := &store.User{ID: 2, PrimaryEmail: "delegate@example.com"}
	calRepo := &fakeCalendarRepo{
		calendars: map[int64]*store.Calendar{
			5: {ID: 5, UserID: 1, Name: "Work"},
		},
	}
	aclRepo := &fakeACLRepo{
		entries: []store.ACLEntry{
			{ResourcePath: "/dav/calendars/5", PrincipalHref: "/dav/principals/2/", IsGrant: true, Privilege: "write"},
			{ResourcePath: "/dav/calendars/5", PrincipalHref: "/dav/principals/2/", IsGrant: false, Privilege: "write-content"},
		},
	}
	h := &Handler{store: &store.Store{Calendars: calRepo, ACLEntries: aclRepo}}

	privs := h.currentUserPrivilegeSetForPath(context.Background(), delegate, "/dav/calendars/5/")
	if privs == nil {
		t.Fatal("expected computed privilege set")
	}
	for _, privilege := range privs.Privileges {
		if privilege.Write != nil {
			t.Fatalf("did not expect aggregate write privilege when write-content is denied, got %#v", privs)
		}
	}
}

func TestPropfindCalendarDiscoveryIncludesReadFreeBusyOnlyCalendars(t *testing.T) {
	owner := &store.User{ID: 1, PrimaryEmail: "owner@example.com"}
	delegate := &store.User{ID: 2, PrimaryEmail: "delegate@example.com"}
	calRepo := &fakeCalendarRepo{
		accessibleByUser: map[int64][]store.CalendarAccess{
			owner.ID: {
				{Calendar: store.Calendar{ID: 5, UserID: owner.ID, Name: "Work"}, Editor: true},
			},
		},
		calendars: map[int64]*store.Calendar{
			5: {ID: 5, UserID: owner.ID, Name: "Work"},
		},
	}
	aclRepo := &fakeACLRepo{
		entries: []store.ACLEntry{
			{ResourcePath: "/dav/calendars/5", PrincipalHref: "/dav/principals/2/", IsGrant: true, Privilege: "read-free-busy"},
		},
	}
	h := &Handler{store: &store.Store{Calendars: calRepo, ACLEntries: aclRepo}}

	t.Run("calendar home discovery", func(t *testing.T) {
		req := httptest.NewRequest("PROPFIND", "/dav/calendars/", nil)
		req.Header.Set("Depth", "1")
		req = req.WithContext(auth.WithUser(req.Context(), delegate))
		rr := httptest.NewRecorder()

		h.Propfind(rr, req)

		if rr.Code != http.StatusMultiStatus {
			t.Fatalf("expected PROPFIND to succeed, got %d: %s", rr.Code, rr.Body.String())
		}
		if !strings.Contains(rr.Body.String(), "<d:href>/dav/calendars/5/</d:href>") {
			t.Fatalf("expected read-free-busy calendar in discovery response, got %s", rr.Body.String())
		}
	})

	t.Run("direct collection propfind", func(t *testing.T) {
		req := httptest.NewRequest("PROPFIND", "/dav/calendars/5/", nil)
		req.Header.Set("Depth", "0")
		req = req.WithContext(auth.WithUser(req.Context(), delegate))
		rr := httptest.NewRecorder()

		h.Propfind(rr, req)

		if rr.Code != http.StatusMultiStatus {
			t.Fatalf("expected direct PROPFIND to succeed with read-free-busy, got %d: %s", rr.Code, rr.Body.String())
		}
		if !strings.Contains(rr.Body.String(), "read-free-busy") {
			t.Fatalf("expected direct PROPFIND to advertise read-free-busy, got %s", rr.Body.String())
		}
	})
}

func TestPropfindCalendarDiscoveryIncludesACLGrantedCalendars(t *testing.T) {
	owner := &store.User{ID: 1, PrimaryEmail: "owner@example.com"}
	delegate := &store.User{ID: 2, PrimaryEmail: "delegate@example.com"}
	calRepo := &fakeCalendarRepo{
		accessibleByUser: map[int64][]store.CalendarAccess{
			owner.ID: {
				{Calendar: store.Calendar{ID: 5, UserID: owner.ID, Name: "Work"}, Editor: true},
			},
		},
		calendars: map[int64]*store.Calendar{
			5: {ID: 5, UserID: owner.ID, Name: "Work"},
		},
	}
	h := &Handler{store: &store.Store{
		Calendars: calRepo,
		ACLEntries: &fakeACLRepo{entries: []store.ACLEntry{
			{ResourcePath: "/dav/calendars/5", PrincipalHref: "/dav/principals/2/", IsGrant: true, Privilege: "read"},
		}},
	}}

	req := httptest.NewRequest("PROPFIND", "/dav/calendars/", nil)
	req.Header.Set("Depth", "1")
	req = req.WithContext(auth.WithUser(req.Context(), delegate))
	rr := httptest.NewRecorder()

	h.Propfind(rr, req)

	if rr.Code != http.StatusMultiStatus {
		t.Fatalf("expected PROPFIND to succeed, got %d: %s", rr.Code, rr.Body.String())
	}
	respBody := rr.Body.String()
	if !strings.Contains(respBody, "<d:href>/dav/calendars/5/</d:href>") {
		t.Fatalf("expected ACL-granted calendar in discovery response, got %s", respBody)
	}
}

func TestPropfindAddressBookObjectACLUsesCanonicalStoredPath(t *testing.T) {
	user := &store.User{ID: 1, PrimaryEmail: "owner@example.com"}
	bookRepo := &fakeAddressBookRepo{
		books: map[int64]*store.AddressBook{
			5: {ID: 5, UserID: user.ID, Name: "Contacts"},
		},
	}
	contactRepo := &fakeContactRepo{
		contacts: map[string]*store.Contact{
			"5:alice": {
				AddressBookID: 5,
				UID:           "alice",
				ResourceName:  "alice",
				RawVCard:      buildVCard("3.0", "UID:alice", "FN:Alice Example"),
				ETag:          "etag-alice",
			},
		},
	}
	aclRepo := &fakeACLRepo{
		entries: []store.ACLEntry{
			{ResourcePath: "/dav/addressbooks/5/alice", PrincipalHref: "/dav/principals/2/", IsGrant: true, Privilege: "read"},
		},
	}
	h := &Handler{store: &store.Store{AddressBooks: bookRepo, Contacts: contactRepo, ACLEntries: aclRepo}}

	body := `<?xml version="1.0" encoding="utf-8"?>
<d:propfind xmlns:d="DAV:">
  <d:prop>
    <d:acl/>
  </d:prop>
</d:propfind>`
	req := httptest.NewRequest("PROPFIND", "/dav/addressbooks/5/alice.vcf", strings.NewReader(body))
	req.Header.Set("Depth", "0")
	req = req.WithContext(auth.WithUser(req.Context(), user))
	rr := httptest.NewRecorder()

	h.Propfind(rr, req)

	if rr.Code != http.StatusMultiStatus {
		t.Fatalf("expected PROPFIND to succeed, got %d: %s", rr.Code, rr.Body.String())
	}
	respBody := rr.Body.String()
	if !strings.Contains(respBody, "<d:acl>") || !strings.Contains(respBody, "/dav/principals/2/") || !strings.Contains(respBody, "<d:read") {
		t.Fatalf("expected PROPFIND to include the stored ACE for the canonical resource path, got %s", respBody)
	}
}

func TestPropfindGenericCollectionReportsUnsupportedRequestedPropertiesAs404(t *testing.T) {
	h := &Handler{store: &store.Store{}}

	body := `<?xml version="1.0" encoding="utf-8"?>
<d:propfind xmlns:d="DAV:" xmlns:c="urn:ietf:params:xml:ns:caldav">
  <d:prop>
    <c:calendar-home-set/>
  </d:prop>
</d:propfind>`
	req := httptest.NewRequest("PROPFIND", "/dav/addressbooks/", strings.NewReader(body))
	req.Header.Set("Depth", "0")
	req = req.WithContext(auth.WithUser(req.Context(), &store.User{ID: 1, PrimaryEmail: "owner@example.com"}))
	rr := httptest.NewRecorder()

	h.Propfind(rr, req)

	if rr.Code != http.StatusMultiStatus {
		t.Fatalf("expected PROPFIND to succeed, got %d: %s", rr.Code, rr.Body.String())
	}
	respBody := rr.Body.String()
	if !propstatHasStatus(respBody, "calendar-home-set", http.StatusNotFound) {
		t.Fatalf("expected unsupported calendar-home-set to be reported with 404, got %s", respBody)
	}
	if propstatHasStatus(respBody, "displayname", http.StatusOK) {
		t.Fatalf("expected PROPFIND prop response to avoid leaking unrelated default properties, got %s", respBody)
	}
}

func TestPropfindCalendarPropRequestReturnsOnlyRequestedProperties(t *testing.T) {
	user := &store.User{ID: 1, PrimaryEmail: "owner@example.com"}
	calRepo := &fakeCalendarRepo{
		accessible: []store.CalendarAccess{
			{Calendar: store.Calendar{ID: 5, UserID: user.ID, Name: "Work"}, Editor: true},
		},
		calendars: map[int64]*store.Calendar{
			5: {ID: 5, UserID: user.ID, Name: "Work"},
		},
	}
	eventRepo := &fakeEventRepo{
		events: map[string]*store.Event{
			"5:event": {
				CalendarID:   5,
				UID:          "event",
				ResourceName: "event",
				RawICAL:      "BEGIN:VCALENDAR\r\nBEGIN:VEVENT\r\nUID:event\r\nEND:VEVENT\r\nEND:VCALENDAR\r\n",
				ETag:         "etag-event",
			},
		},
	}
	lockRepo := &fakeLockRepo{
		locks: map[string]*store.Lock{
			"opaquelocktoken:event": {
				Token:          "opaquelocktoken:event",
				ResourcePath:   "/dav/calendars/5/event.ics",
				UserID:         user.ID,
				LockScope:      "exclusive",
				LockType:       "write",
				Depth:          "0",
				TimeoutSeconds: 3600,
				ExpiresAt:      time.Now().Add(time.Hour),
			},
		},
	}
	aclRepo := &fakeACLRepo{
		entries: []store.ACLEntry{
			{ResourcePath: "/dav/calendars/5/event.ics", PrincipalHref: "/dav/principals/2/", IsGrant: true, Privilege: "read"},
		},
	}
	h := &Handler{store: &store.Store{Calendars: calRepo, Events: eventRepo, Locks: lockRepo, ACLEntries: aclRepo}}

	body := `<?xml version="1.0" encoding="utf-8"?>
<d:propfind xmlns:d="DAV:">
  <d:prop>
    <d:getetag/>
  </d:prop>
</d:propfind>`
	req := httptest.NewRequest("PROPFIND", "/dav/calendars/5/event.ics", strings.NewReader(body))
	req.Header.Set("Depth", "0")
	req = req.WithContext(auth.WithUser(req.Context(), user))
	rr := httptest.NewRecorder()

	h.Propfind(rr, req)

	if rr.Code != http.StatusMultiStatus {
		t.Fatalf("expected PROPFIND to succeed, got %d: %s", rr.Code, rr.Body.String())
	}
	respBody := rr.Body.String()
	if !strings.Contains(respBody, "<d:getetag>") {
		t.Fatalf("expected getetag in response, got %s", respBody)
	}
	for _, forbidden := range []string{"supportedlock", "lockdiscovery", "<d:acl>", "supported-privilege-set", "principal-collection-set", "calendar-data", "getcontenttype"} {
		if strings.Contains(respBody, forbidden) {
			t.Fatalf("did not expect %q in prop-only response, got %s", forbidden, respBody)
		}
	}
}

func TestPropfindCalendarObjectHidesDeniedEvent(t *testing.T) {
	user := &store.User{ID: 2, PrimaryEmail: "reader@example.com"}
	calRepo := &fakeCalendarRepo{
		calendars: map[int64]*store.Calendar{
			5: {ID: 5, UserID: 1, Name: "Work"},
		},
	}
	eventRepo := &fakeEventRepo{
		events: map[string]*store.Event{
			"5:event": {
				CalendarID:   5,
				UID:          "event",
				ResourceName: "event",
				RawICAL:      "BEGIN:VCALENDAR\r\nBEGIN:VEVENT\r\nUID:event\r\nEND:VEVENT\r\nEND:VCALENDAR\r\n",
				ETag:         "etag-event",
			},
		},
	}
	aclRepo := &fakeACLRepo{
		entries: []store.ACLEntry{
			{ResourcePath: "/dav/calendars/5", PrincipalHref: "/dav/principals/2/", IsGrant: true, Privilege: "read"},
			{ResourcePath: "/dav/calendars/5/event", PrincipalHref: "/dav/principals/2/", IsGrant: false, Privilege: "read"},
		},
	}
	h := &Handler{store: &store.Store{Calendars: calRepo, Events: eventRepo, ACLEntries: aclRepo}}

	body := `<?xml version="1.0" encoding="utf-8"?>
<d:propfind xmlns:d="DAV:">
  <d:prop>
    <d:getetag/>
  </d:prop>
</d:propfind>`
	req := httptest.NewRequest("PROPFIND", "/dav/calendars/5/event.ics", strings.NewReader(body))
	req.Header.Set("Depth", "0")
	req = req.WithContext(auth.WithUser(req.Context(), user))
	rr := httptest.NewRecorder()

	h.Propfind(rr, req)

	if rr.Code != http.StatusMultiStatus {
		t.Fatalf("expected PROPFIND to succeed, got %d: %s", rr.Code, rr.Body.String())
	}
	respBody := rr.Body.String()
	if strings.Contains(respBody, "<d:getetag>") {
		t.Fatalf("expected denied object PROPFIND to hide getetag, got %s", respBody)
	}
	if !strings.Contains(respBody, "404 Not Found") {
		t.Fatalf("expected denied object PROPFIND to look like not found, got %s", respBody)
	}
}

func TestPropfindAddressBookPropRequestReturnsOnlyRequestedProperties(t *testing.T) {
	user := &store.User{ID: 1, PrimaryEmail: "owner@example.com"}
	bookRepo := &fakeAddressBookRepo{
		books: map[int64]*store.AddressBook{
			5: {ID: 5, UserID: user.ID, Name: "Contacts"},
		},
	}
	contactRepo := &fakeContactRepo{
		contacts: map[string]*store.Contact{
			"5:alice": {
				AddressBookID: 5,
				UID:           "alice",
				ResourceName:  "alice",
				RawVCard:      buildVCard("3.0", "UID:alice", "FN:Alice Example"),
				ETag:          "etag-alice",
			},
		},
	}
	lockRepo := &fakeLockRepo{
		locks: map[string]*store.Lock{
			"opaquelocktoken:alice": {
				Token:          "opaquelocktoken:alice",
				ResourcePath:   "/dav/addressbooks/5/alice.vcf",
				UserID:         user.ID,
				LockScope:      "exclusive",
				LockType:       "write",
				Depth:          "0",
				TimeoutSeconds: 3600,
				ExpiresAt:      time.Now().Add(time.Hour),
			},
		},
	}
	aclRepo := &fakeACLRepo{
		entries: []store.ACLEntry{
			{ResourcePath: "/dav/addressbooks/5/alice.vcf", PrincipalHref: "DAV:authenticated", IsGrant: true, Privilege: "read"},
		},
	}
	h := &Handler{store: &store.Store{AddressBooks: bookRepo, Contacts: contactRepo, Locks: lockRepo, ACLEntries: aclRepo}}

	body := `<?xml version="1.0" encoding="utf-8"?>
<d:propfind xmlns:d="DAV:">
  <d:prop>
    <d:getetag/>
  </d:prop>
</d:propfind>`
	req := httptest.NewRequest("PROPFIND", "/dav/addressbooks/5/alice.vcf", strings.NewReader(body))
	req.Header.Set("Depth", "0")
	req = req.WithContext(auth.WithUser(req.Context(), user))
	rr := httptest.NewRecorder()

	h.Propfind(rr, req)

	if rr.Code != http.StatusMultiStatus {
		t.Fatalf("expected PROPFIND to succeed, got %d: %s", rr.Code, rr.Body.String())
	}
	respBody := rr.Body.String()
	if !strings.Contains(respBody, "<d:getetag>") {
		t.Fatalf("expected getetag in response, got %s", respBody)
	}
	for _, forbidden := range []string{"supportedlock", "lockdiscovery", "<d:acl>", "supported-privilege-set", "principal-collection-set", "address-data", "getcontenttype"} {
		if strings.Contains(respBody, forbidden) {
			t.Fatalf("did not expect %q in prop-only response, got %s", forbidden, respBody)
		}
	}
}

func TestPropfindAddressBookDepthOneFiltersDeniedContacts(t *testing.T) {
	now := store.Now()
	user := &store.User{ID: 2, PrimaryEmail: "reader@example.com"}

	newHandler := func(denySecret bool) *Handler {
		bookRepo := &fakeAddressBookRepo{
			books: map[int64]*store.AddressBook{
				5: {ID: 5, UserID: 1, Name: "Shared Contacts", UpdatedAt: now},
			},
		}
		contactRepo := &fakeContactRepo{
			contacts: map[string]*store.Contact{
				"5:public": {AddressBookID: 5, UID: "public", ResourceName: "public", RawVCard: buildVCard("3.0", "UID:public", "FN:Public"), ETag: "etag-public", LastModified: now},
				"5:secret": {AddressBookID: 5, UID: "secret", ResourceName: "secret", RawVCard: buildVCard("3.0", "UID:secret", "FN:Secret"), ETag: "etag-secret", LastModified: now},
			},
		}
		entries := []store.ACLEntry{
			{ResourcePath: "/dav/addressbooks/5", PrincipalHref: "/dav/principals/2/", IsGrant: true, Privilege: "read"},
		}
		if denySecret {
			entries = append(entries, store.ACLEntry{
				ResourcePath:  "/dav/addressbooks/5/secret",
				PrincipalHref: "/dav/principals/2/",
				IsGrant:       false,
				Privilege:     "read",
			})
		}
		return &Handler{store: &store.Store{AddressBooks: bookRepo, Contacts: contactRepo, ACLEntries: &fakeACLRepo{entries: entries}}}
	}

	t.Run("positive_includes_visible_members", func(t *testing.T) {
		req := httptest.NewRequest("PROPFIND", "/dav/addressbooks/5/", nil)
		req.Header.Set("Depth", "1")
		req = req.WithContext(auth.WithUser(req.Context(), user))
		rr := httptest.NewRecorder()

		newHandler(false).Propfind(rr, req)

		if rr.Code != http.StatusMultiStatus {
			t.Fatalf("expected PROPFIND to succeed, got %d: %s", rr.Code, rr.Body.String())
		}
		respBody := rr.Body.String()
		for _, want := range []string{"public.vcf", "secret.vcf"} {
			if !strings.Contains(respBody, want) {
				t.Fatalf("expected PROPFIND response to include %q, got %s", want, respBody)
			}
		}
	})

	t.Run("negative_omits_denied_members", func(t *testing.T) {
		req := httptest.NewRequest("PROPFIND", "/dav/addressbooks/5/", nil)
		req.Header.Set("Depth", "1")
		req = req.WithContext(auth.WithUser(req.Context(), user))
		rr := httptest.NewRecorder()

		newHandler(true).Propfind(rr, req)

		if rr.Code != http.StatusMultiStatus {
			t.Fatalf("expected PROPFIND to succeed, got %d: %s", rr.Code, rr.Body.String())
		}
		respBody := rr.Body.String()
		if !strings.Contains(respBody, "public.vcf") {
			t.Fatalf("expected PROPFIND response to include visible contact, got %s", respBody)
		}
		if strings.Contains(respBody, "secret.vcf") {
			t.Fatalf("expected PROPFIND response to omit denied contact, got %s", respBody)
		}
	})
}

func TestAddressBookReportsFilterDeniedContacts(t *testing.T) {
	now := store.Now()
	user := &store.User{ID: 2, PrimaryEmail: "reader@example.com"}

	newHandler := func(denySecret bool) *Handler {
		bookRepo := &fakeAddressBookRepo{
			books: map[int64]*store.AddressBook{
				5: {ID: 5, UserID: 1, Name: "Shared Contacts", UpdatedAt: now, CTag: 2},
			},
		}
		contactRepo := &fakeContactRepo{
			contacts: map[string]*store.Contact{
				"5:public": {AddressBookID: 5, UID: "public", ResourceName: "public", RawVCard: buildVCard("3.0", "UID:public", "FN:Public"), ETag: "etag-public", LastModified: now},
				"5:secret": {AddressBookID: 5, UID: "secret", ResourceName: "secret", RawVCard: buildVCard("3.0", "UID:secret", "FN:Secret"), ETag: "etag-secret", LastModified: now},
			},
		}
		entries := []store.ACLEntry{
			{ResourcePath: "/dav/addressbooks/5", PrincipalHref: "/dav/principals/2/", IsGrant: true, Privilege: "read"},
		}
		if denySecret {
			entries = append(entries, store.ACLEntry{
				ResourcePath:  "/dav/addressbooks/5/secret",
				PrincipalHref: "/dav/principals/2/",
				IsGrant:       false,
				Privilege:     "read",
			})
		}
		return &Handler{store: &store.Store{AddressBooks: bookRepo, Contacts: contactRepo, ACLEntries: &fakeACLRepo{entries: entries}}}
	}

	tests := []struct {
		name    string
		body    string
		depth   string
		wantAll bool
	}{
		{
			name: "addressbook_query",
			body: `<?xml version="1.0" encoding="utf-8"?>
<card:addressbook-query xmlns:card="urn:ietf:params:xml:ns:carddav" xmlns:D="DAV:">
  <D:prop><card:address-data/></D:prop>
  <card:filter/>
</card:addressbook-query>`,
			depth:   "1",
			wantAll: true,
		},
		{
			name: "addressbook_multiget",
			body: `<?xml version="1.0" encoding="utf-8"?>
<card:addressbook-multiget xmlns:card="urn:ietf:params:xml:ns:carddav" xmlns:D="DAV:">
  <D:prop><card:address-data/></D:prop>
  <D:href>/dav/addressbooks/5/public.vcf</D:href>
  <D:href>/dav/addressbooks/5/secret.vcf</D:href>
</card:addressbook-multiget>`,
			depth:   "0",
			wantAll: false,
		},
		{
			name: "sync_collection",
			body: `<?xml version="1.0" encoding="utf-8"?>
<D:sync-collection xmlns:D="DAV:">
</D:sync-collection>`,
			depth:   "",
			wantAll: false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name+"/positive_includes_visible_resources", func(t *testing.T) {
			req := httptest.NewRequest("REPORT", "/dav/addressbooks/5/", strings.NewReader(tc.body))
			if tc.depth != "" {
				req.Header.Set("Depth", tc.depth)
			}
			req = req.WithContext(auth.WithUser(req.Context(), user))
			rr := httptest.NewRecorder()

			newHandler(false).Report(rr, req)

			if rr.Code != http.StatusMultiStatus {
				t.Fatalf("expected REPORT to succeed, got %d: %s", rr.Code, rr.Body.String())
			}
			respBody := rr.Body.String()
			if !strings.Contains(respBody, "UID:public") {
				t.Fatalf("expected REPORT response to include visible contact data, got %s", respBody)
			}
			if tc.wantAll && !strings.Contains(respBody, "UID:secret") {
				t.Fatalf("expected REPORT response to include unrestricted contact data, got %s", respBody)
			}
		})

		t.Run(tc.name+"/negative_omits_denied_resources", func(t *testing.T) {
			req := httptest.NewRequest("REPORT", "/dav/addressbooks/5/", strings.NewReader(tc.body))
			if tc.depth != "" {
				req.Header.Set("Depth", tc.depth)
			}
			req = req.WithContext(auth.WithUser(req.Context(), user))
			rr := httptest.NewRecorder()

			newHandler(true).Report(rr, req)

			if rr.Code != http.StatusMultiStatus {
				t.Fatalf("expected REPORT to succeed, got %d: %s", rr.Code, rr.Body.String())
			}
			respBody := rr.Body.String()
			if !strings.Contains(respBody, "UID:public") {
				t.Fatalf("expected REPORT response to include visible contact data, got %s", respBody)
			}
			if strings.Contains(respBody, "UID:secret") {
				t.Fatalf("expected REPORT response to omit denied contact data, got %s", respBody)
			}
		})
	}
}

func TestPropfindAddressDataRequestsRespectSelection(t *testing.T) {
	user := &store.User{ID: 1, PrimaryEmail: "owner@example.com"}
	bookRepo := &fakeAddressBookRepo{
		books: map[int64]*store.AddressBook{
			5: {ID: 5, UserID: user.ID, Name: "Contacts"},
		},
	}
	contactRepo := &fakeContactRepo{
		contacts: map[string]*store.Contact{
			"5:alice": {
				AddressBookID: 5,
				UID:           "alice",
				ResourceName:  "alice",
				RawVCard:      buildVCard("3.0", "UID:alice", "FN:Alice Example", "EMAIL:alice@example.com"),
				ETag:          "etag-alice",
			},
		},
	}
	h := &Handler{store: &store.Store{AddressBooks: bookRepo, Contacts: contactRepo}}

	t.Run("positive_empty_address_data_returns_full_card", func(t *testing.T) {
		body := `<?xml version="1.0" encoding="utf-8"?>
<d:propfind xmlns:d="DAV:" xmlns:card="urn:ietf:params:xml:ns:carddav">
  <d:prop>
    <card:address-data/>
  </d:prop>
</d:propfind>`
		req := httptest.NewRequest("PROPFIND", "/dav/addressbooks/5/alice.vcf", strings.NewReader(body))
		req.Header.Set("Depth", "0")
		req = req.WithContext(auth.WithUser(req.Context(), user))
		rr := httptest.NewRecorder()

		h.Propfind(rr, req)

		if rr.Code != http.StatusMultiStatus {
			t.Fatalf("expected PROPFIND to succeed, got %d: %s", rr.Code, rr.Body.String())
		}
		respBody := rr.Body.String()
		for _, want := range []string{"FN:Alice Example", "EMAIL:alice@example.com"} {
			if !strings.Contains(respBody, want) {
				t.Fatalf("expected full address-data response to include %q, got %s", want, respBody)
			}
		}
	})

	t.Run("negative_selected_properties_exclude_unrequested_fields", func(t *testing.T) {
		body := `<?xml version="1.0" encoding="utf-8"?>
<d:propfind xmlns:d="DAV:" xmlns:card="urn:ietf:params:xml:ns:carddav">
  <d:prop>
    <card:address-data>
      <card:prop name="FN"/>
    </card:address-data>
  </d:prop>
</d:propfind>`
		req := httptest.NewRequest("PROPFIND", "/dav/addressbooks/5/alice.vcf", strings.NewReader(body))
		req.Header.Set("Depth", "0")
		req = req.WithContext(auth.WithUser(req.Context(), user))
		rr := httptest.NewRecorder()

		h.Propfind(rr, req)

		if rr.Code != http.StatusMultiStatus {
			t.Fatalf("expected PROPFIND to succeed, got %d: %s", rr.Code, rr.Body.String())
		}
		respBody := rr.Body.String()
		if !strings.Contains(respBody, "FN:Alice Example") {
			t.Fatalf("expected filtered address-data to include FN, got %s", respBody)
		}
		if strings.Contains(respBody, "EMAIL:alice@example.com") {
			t.Fatalf("expected filtered address-data to omit EMAIL, got %s", respBody)
		}
	})
}

func TestPropfindCollectionPropRequestsReportUnsupportedProperties(t *testing.T) {
	user := &store.User{ID: 1, PrimaryEmail: "owner@example.com"}
	bookRepo := &fakeAddressBookRepo{
		books: map[int64]*store.AddressBook{
			5: {ID: 5, UserID: user.ID, Name: "Contacts"},
		},
	}
	h := &Handler{store: &store.Store{AddressBooks: bookRepo}}

	t.Run("positive_supported_addressbook_property_only", func(t *testing.T) {
		body := `<?xml version="1.0" encoding="utf-8"?>
<d:propfind xmlns:d="DAV:">
  <d:prop>
    <d:displayname/>
  </d:prop>
</d:propfind>`
		req := httptest.NewRequest("PROPFIND", "/dav/addressbooks/5/", strings.NewReader(body))
		req.Header.Set("Depth", "0")
		req = req.WithContext(auth.WithUser(req.Context(), user))
		rr := httptest.NewRecorder()

		h.Propfind(rr, req)

		if rr.Code != http.StatusMultiStatus {
			t.Fatalf("expected PROPFIND to succeed, got %d: %s", rr.Code, rr.Body.String())
		}
		respBody := rr.Body.String()
		if !strings.Contains(respBody, "<d:displayname>Contacts</d:displayname>") {
			t.Fatalf("expected displayname in response, got %s", respBody)
		}
		for _, forbidden := range []string{"addressbook-description", "supported-address-data", "supportedlock"} {
			if strings.Contains(respBody, forbidden) {
				t.Fatalf("did not expect %q in supported prop-only response, got %s", forbidden, respBody)
			}
		}
	})

	t.Run("negative_unsupported_addressbook_property_returns_404", func(t *testing.T) {
		body := `<?xml version="1.0" encoding="utf-8"?>
<d:propfind xmlns:d="DAV:">
  <d:prop>
    <d:principal-URL/>
  </d:prop>
</d:propfind>`
		req := httptest.NewRequest("PROPFIND", "/dav/addressbooks/5/", strings.NewReader(body))
		req.Header.Set("Depth", "0")
		req = req.WithContext(auth.WithUser(req.Context(), user))
		rr := httptest.NewRecorder()

		h.Propfind(rr, req)

		if rr.Code != http.StatusMultiStatus {
			t.Fatalf("expected PROPFIND to succeed, got %d: %s", rr.Code, rr.Body.String())
		}
		respBody := rr.Body.String()
		if !propstatHasStatus(respBody, "principal-URL", http.StatusNotFound) {
			t.Fatalf("expected unsupported principal-URL to be reported with 404, got %s", respBody)
		}
		if strings.Contains(respBody, "<d:displayname>Contacts</d:displayname>") {
			t.Fatalf("expected unsupported prop request to avoid leaking default collection properties, got %s", respBody)
		}
	})
}

func TestPropfindPrincipalUnsupportedPropertyReturns404(t *testing.T) {
	user := &store.User{ID: 1, PrimaryEmail: "owner@example.com"}
	h := &Handler{store: &store.Store{}}

	t.Run("positive_supported_principal_property_only", func(t *testing.T) {
		body := `<?xml version="1.0" encoding="utf-8"?>
<d:propfind xmlns:d="DAV:">
  <d:prop>
    <d:principal-URL/>
  </d:prop>
</d:propfind>`
		req := httptest.NewRequest("PROPFIND", "/dav/principals/1/", strings.NewReader(body))
		req.Header.Set("Depth", "0")
		req = req.WithContext(auth.WithUser(req.Context(), user))
		rr := httptest.NewRecorder()

		h.Propfind(rr, req)

		if rr.Code != http.StatusMultiStatus {
			t.Fatalf("expected PROPFIND to succeed, got %d: %s", rr.Code, rr.Body.String())
		}
		respBody := rr.Body.String()
		if !strings.Contains(respBody, "<d:principal-URL>") {
			t.Fatalf("expected principal-URL in response, got %s", respBody)
		}
		if strings.Contains(respBody, "calendar-home-set") {
			t.Fatalf("did not expect unrelated principal properties in prop-only response, got %s", respBody)
		}
	})

	t.Run("negative_unsupported_principal_property_returns_404", func(t *testing.T) {
		body := `<?xml version="1.0" encoding="utf-8"?>
<d:propfind xmlns:d="DAV:" xmlns:c="urn:ietf:params:xml:ns:caldav">
  <d:prop>
    <c:calendar-description/>
  </d:prop>
</d:propfind>`
		req := httptest.NewRequest("PROPFIND", "/dav/principals/1/", strings.NewReader(body))
		req.Header.Set("Depth", "0")
		req = req.WithContext(auth.WithUser(req.Context(), user))
		rr := httptest.NewRecorder()

		h.Propfind(rr, req)

		if rr.Code != http.StatusMultiStatus {
			t.Fatalf("expected PROPFIND to succeed, got %d: %s", rr.Code, rr.Body.String())
		}
		respBody := rr.Body.String()
		if !propstatHasStatus(respBody, "calendar-description", http.StatusNotFound) {
			t.Fatalf("expected unsupported principal property to be reported with 404, got %s", respBody)
		}
		if strings.Contains(respBody, "<d:principal-URL>") {
			t.Fatalf("expected unsupported prop request to avoid leaking principal defaults, got %s", respBody)
		}
	})
}

func TestIsResourceOwnerParsesPrincipalIDPositionally(t *testing.T) {
	user := &store.User{ID: 1, PrimaryEmail: "owner@example.com"}
	h := &Handler{store: &store.Store{}}

	if !h.isResourceOwner(context.Background(), user, "/dav/principals/1/") {
		t.Fatal("expected authenticated principal path to be owned by the matching user")
	}
	if h.isResourceOwner(context.Background(), user, "/dav/principals/settings/1/") {
		t.Fatal("expected nested principal path with user ID in a later segment to not be treated as owner path")
	}
}

func TestPutAddressBookUIDConflictEscapesHref(t *testing.T) {
	user := &store.User{ID: 1, PrimaryEmail: "owner@example.com"}
	bookRepo := &fakeAddressBookRepo{
		books: map[int64]*store.AddressBook{
			5: {ID: 5, UserID: user.ID, Name: "Contacts"},
		},
	}

	type uidConflictBody struct {
		Conflict struct {
			Href string `xml:"href"`
		} `xml:"no-uid-conflict"`
	}

	run := func(t *testing.T, resourceName string) string {
		t.Helper()
		contactRepo := &fakeContactRepo{
			contacts: map[string]*store.Contact{
				fmt.Sprintf("5:%s", "existing"): {
					AddressBookID: 5,
					UID:           "existing",
					ResourceName:  resourceName,
					RawVCard:      buildVCard("3.0", "UID:existing", "FN:Existing"),
					ETag:          "etag-existing",
				},
			},
		}
		h := &Handler{store: &store.Store{AddressBooks: bookRepo, Contacts: contactRepo}}
		req := newAddressBookPutRequest("/dav/addressbooks/5/"+resourceName+".vcf", strings.NewReader(buildVCard("3.0", "UID:new", "FN:New")))
		req = req.WithContext(auth.WithUser(req.Context(), user))
		rr := httptest.NewRecorder()

		h.Put(rr, req)

		if rr.Code != http.StatusConflict {
			t.Fatalf("expected UID conflict PUT to return 409, got %d: %s", rr.Code, rr.Body.String())
		}
		var payload uidConflictBody
		if err := xml.Unmarshal(rr.Body.Bytes(), &payload); err != nil {
			t.Fatalf("expected UID conflict body to be valid XML, got parse error %v: %s", err, rr.Body.String())
		}
		return payload.Conflict.Href
	}

	t.Run("positive_plain_href_remains_valid_xml", func(t *testing.T) {
		if href := run(t, "conflict"); href != "/dav/addressbooks/5/conflict.vcf" {
			t.Fatalf("unexpected href %q", href)
		}
	})

	t.Run("negative_special_characters_are_escaped", func(t *testing.T) {
		if href := run(t, "conflict&name"); href != "/dav/addressbooks/5/conflict&name.vcf" {
			t.Fatalf("unexpected escaped href %q", href)
		}
	})
}

func TestMkcolRejectsNumericDisplayNameFromBody(t *testing.T) {
	bookRepo := &fakeAddressBookRepo{}
	h := &Handler{store: &store.Store{AddressBooks: bookRepo}}

	body := `<?xml version="1.0" encoding="utf-8"?>
<d:mkcol xmlns:d="DAV:" xmlns:card="urn:ietf:params:xml:ns:carddav">
  <d:set>
    <d:prop>
      <d:displayname>123</d:displayname>
    </d:prop>
  </d:set>
</d:mkcol>`
	req := httptest.NewRequest("MKCOL", "/dav/addressbooks/tmp", strings.NewReader(body))
	req = req.WithContext(auth.WithUser(req.Context(), &store.User{ID: 1}))
	rr := httptest.NewRecorder()

	h.Mkcol(rr, req)

	if rr.Code != http.StatusBadRequest {
		t.Fatalf("expected MKCOL to reject numeric displayname, got %d: %s", rr.Code, rr.Body.String())
	}
	if len(bookRepo.books) != 0 {
		t.Fatalf("expected no address book to be created, got %#v", bookRepo.books)
	}
}

func TestMkcolRejectsInvalidExtendedBodyAndDoesNotCreate(t *testing.T) {
	bookRepo := &fakeAddressBookRepo{}
	h := &Handler{store: &store.Store{AddressBooks: bookRepo}}

	req := httptest.NewRequest("MKCOL", "/dav/addressbooks/tmp", strings.NewReader(`<d:mkcol xmlns:d="DAV:"><d:set>`))
	req = req.WithContext(auth.WithUser(req.Context(), &store.User{ID: 1}))
	rr := httptest.NewRecorder()

	h.Mkcol(rr, req)

	if rr.Code != http.StatusBadRequest {
		t.Fatalf("expected MKCOL to reject invalid extended body, got %d: %s", rr.Code, rr.Body.String())
	}
	if len(bookRepo.books) != 0 {
		t.Fatalf("expected no address book to be created, got %#v", bookRepo.books)
	}
}

func TestMkcolRejectsDuplicateAddressBookName(t *testing.T) {
	bookRepo := &fakeAddressBookRepo{
		books: map[int64]*store.AddressBook{
			5: {ID: 5, UserID: 1, Name: "Contacts"},
		},
	}
	h := &Handler{store: &store.Store{AddressBooks: bookRepo}}

	req := httptest.NewRequest("MKCOL", "/dav/addressbooks/contacts", nil)
	req = req.WithContext(auth.WithUser(req.Context(), &store.User{ID: 1}))
	rr := httptest.NewRecorder()

	h.Mkcol(rr, req)

	if rr.Code != http.StatusConflict {
		t.Fatalf("expected duplicate MKCOL to return 409, got %d: %s", rr.Code, rr.Body.String())
	}
}

func TestAddressBookHandlersRejectExtraResourcePathSegments(t *testing.T) {
	user := &store.User{ID: 1}
	bookRepo := &fakeAddressBookRepo{
		books: map[int64]*store.AddressBook{
			5: {ID: 5, UserID: user.ID, Name: "Contacts"},
		},
	}
	contactRepo := &fakeContactRepo{
		contacts: map[string]*store.Contact{
			"5:alice": {AddressBookID: 5, UID: "alice", ResourceName: "alice", RawVCard: buildVCard("3.0", "UID:alice", "FN:Alice"), ETag: "etag-a"},
		},
	}
	h := &Handler{store: &store.Store{AddressBooks: bookRepo, Contacts: contactRepo}}

	t.Run("get", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/dav/addressbooks/5/alice.vcf/extra", nil)
		req = req.WithContext(auth.WithUser(req.Context(), user))
		rr := httptest.NewRecorder()

		h.Get(rr, req)

		if rr.Code != http.StatusNotFound {
			t.Fatalf("expected GET on nested address object path to return 404, got %d: %s", rr.Code, rr.Body.String())
		}
	})

	t.Run("propfind", func(t *testing.T) {
		req := httptest.NewRequest("PROPFIND", "/dav/addressbooks/5/alice.vcf/extra", nil)
		req.Header.Set("Depth", "0")
		req = req.WithContext(auth.WithUser(req.Context(), user))
		rr := httptest.NewRecorder()

		h.Propfind(rr, req)

		if rr.Code != http.StatusNotFound {
			t.Fatalf("expected PROPFIND on nested address object path to return 404, got %d: %s", rr.Code, rr.Body.String())
		}
	})
}

func TestAddressBookCopyAndMoveOverwriteReplaceDifferentUIDDestination(t *testing.T) {
	user := &store.User{ID: 1, PrimaryEmail: "owner@example.com"}
	now := store.Now()

	for _, method := range []string{"COPY", "MOVE"} {
		t.Run(method, func(t *testing.T) {
			bookRepo := &fakeAddressBookRepo{
				books: map[int64]*store.AddressBook{
					5: {ID: 5, UserID: user.ID, Name: "Source", UpdatedAt: now},
					6: {ID: 6, UserID: user.ID, Name: "Destination", UpdatedAt: now},
				},
			}
			contactRepo := &fakeContactRepo{
				contacts: map[string]*store.Contact{
					"5:alice": {AddressBookID: 5, UID: "alice", ResourceName: "alice", RawVCard: buildVCard("3.0", "UID:alice", "FN:Alice"), ETag: "etag-a", LastModified: now},
					"6:bob":   {AddressBookID: 6, UID: "bob", ResourceName: "renamed", RawVCard: buildVCard("3.0", "UID:bob", "FN:Bob"), ETag: "etag-b", LastModified: now},
				},
			}
			lockRepo := &fakeLockRepo{}
			h := &Handler{store: &store.Store{AddressBooks: bookRepo, Contacts: contactRepo, Locks: lockRepo}}

			req := httptest.NewRequest(method, "/dav/addressbooks/5/alice.vcf", nil)
			req.Header.Set("Destination", "https://example.com/dav/addressbooks/6/renamed.vcf")
			req = req.WithContext(auth.WithUser(req.Context(), user))
			rr := httptest.NewRecorder()

			if method == "COPY" {
				h.Copy(rr, req)
			} else {
				h.Move(rr, req)
			}

			if rr.Code != http.StatusNoContent {
				t.Fatalf("expected overwrite %s to return 204, got %d: %s", method, rr.Code, rr.Body.String())
			}
			dest, _ := contactRepo.GetByResourceName(req.Context(), 6, "renamed")
			if dest == nil || dest.UID != "alice" {
				t.Fatalf("expected overwrite %s to replace the destination binding, got %#v", method, dest)
			}
			if stale, _ := contactRepo.GetByUID(req.Context(), 6, "bob"); stale != nil {
				t.Fatalf("expected overwrite %s to remove the previous destination resource, got %#v", method, stale)
			}
			src, _ := contactRepo.GetByUID(req.Context(), 5, "alice")
			if method == "COPY" && src == nil {
				t.Fatal("expected COPY to preserve the source resource")
			}
			if method == "MOVE" && src != nil {
				t.Fatalf("expected MOVE to remove the source resource, got %#v", src)
			}
		})
	}
}

func TestMoveContactOverwriteClearsDestinationTombstone(t *testing.T) {
	user := &store.User{ID: 1, PrimaryEmail: "owner@example.com"}
	now := store.Now()
	deletedRepo := &fakeDeletedResourceRepo{}
	bookRepo := &fakeAddressBookRepo{
		books: map[int64]*store.AddressBook{
			5: {ID: 5, UserID: user.ID, Name: "Contacts", UpdatedAt: now},
			6: {ID: 6, UserID: user.ID, Name: "Archive", UpdatedAt: now},
		},
	}
	contactRepo := &fakeContactRepo{
		contacts: map[string]*store.Contact{
			"5:alice": {AddressBookID: 5, UID: "alice", ResourceName: "alice", RawVCard: buildVCard("3.0", "UID:alice", "FN:Alice"), ETag: "etag-a", LastModified: now},
			"6:bob":   {AddressBookID: 6, UID: "bob", ResourceName: "renamed", RawVCard: buildVCard("3.0", "UID:bob", "FN:Bob"), ETag: "etag-b", LastModified: now},
		},
		overwriteMoveDeletedRepo: deletedRepo,
	}
	h := &Handler{store: &store.Store{AddressBooks: bookRepo, Contacts: contactRepo, DeletedResources: deletedRepo}}

	req := httptest.NewRequest("MOVE", "/dav/addressbooks/5/alice.vcf", nil)
	req.Header.Set("Destination", "https://example.com/dav/addressbooks/6/renamed.vcf")
	req = req.WithContext(auth.WithUser(req.Context(), user))
	rr := httptest.NewRecorder()
	h.Move(rr, req)

	if rr.Code != http.StatusNoContent {
		t.Fatalf("expected overwrite MOVE to return 204, got %d: %s", rr.Code, rr.Body.String())
	}
	tombstones, err := deletedRepo.ListDeletedSince(req.Context(), "contact", 6, time.Time{})
	if err != nil {
		t.Fatalf("ListDeletedSince() error = %v", err)
	}
	for _, tombstone := range tombstones {
		if tombstone.ResourceName == "renamed" {
			t.Fatalf("expected overwrite MOVE to clear destination tombstone, got %#v", tombstones)
		}
	}
}

func TestAddressBookCopyAndMoveOverwriteDifferentUIDDestinationRequiresRebindingPrivileges(t *testing.T) {
	now := store.Now()

	for _, method := range []string{"COPY", "MOVE"} {
		t.Run(method, func(t *testing.T) {
			bookRepo := &fakeAddressBookRepo{
				books: map[int64]*store.AddressBook{
					5: {ID: 5, UserID: 1, Name: "Source", UpdatedAt: now},
					6: {ID: 6, UserID: 1, Name: "Destination", UpdatedAt: now},
				},
			}
			contactRepo := &fakeContactRepo{
				contacts: map[string]*store.Contact{
					"5:alice": {AddressBookID: 5, UID: "alice", ResourceName: "alice", RawVCard: buildVCard("3.0", "UID:alice", "FN:Alice"), ETag: "etag-a", LastModified: now},
					"6:bob":   {AddressBookID: 6, UID: "bob", ResourceName: "renamed", RawVCard: buildVCard("3.0", "UID:bob", "FN:Bob"), ETag: "etag-b", LastModified: now},
				},
			}
			aclEntries := []store.ACLEntry{
				{ResourcePath: "/dav/addressbooks/5", PrincipalHref: "/dav/principals/2/", IsGrant: true, Privilege: "read"},
				{ResourcePath: "/dav/addressbooks/6", PrincipalHref: "/dav/principals/2/", IsGrant: true, Privilege: "write-content"},
			}
			if method == "MOVE" {
				aclEntries = append(aclEntries, store.ACLEntry{
					ResourcePath:  "/dav/addressbooks/5",
					PrincipalHref: "/dav/principals/2/",
					IsGrant:       true,
					Privilege:     "unbind",
				})
			}
			h := &Handler{store: &store.Store{
				AddressBooks: bookRepo,
				Contacts:     contactRepo,
				Locks:        &fakeLockRepo{},
				ACLEntries:   &fakeACLRepo{entries: aclEntries},
			}}
			user := &store.User{ID: 2, PrimaryEmail: "delegate@example.com"}

			req := httptest.NewRequest(method, "/dav/addressbooks/5/alice.vcf", nil)
			req.Header.Set("Destination", "https://example.com/dav/addressbooks/6/renamed.vcf")
			req = req.WithContext(auth.WithUser(req.Context(), user))
			rr := httptest.NewRecorder()

			if method == "COPY" {
				h.Copy(rr, req)
			} else {
				h.Move(rr, req)
			}

			if rr.Code != http.StatusNotFound {
				t.Fatalf("expected overwrite %s without rebinding privileges to be rejected, got %d: %s", method, rr.Code, rr.Body.String())
			}
			dest, _ := contactRepo.GetByResourceName(req.Context(), 6, "renamed")
			if dest == nil || dest.UID != "bob" {
				t.Fatalf("expected %s to preserve the existing destination resource, got %#v", method, dest)
			}
			src, _ := contactRepo.GetByUID(req.Context(), 5, "alice")
			if src == nil {
				t.Fatalf("expected %s to preserve the source resource on forbidden overwrite", method)
			}
		})
	}
}

func TestCalendarCopyAndMoveOverwriteDifferentUIDDestinationRequiresRebindingPrivileges(t *testing.T) {
	now := store.Now()

	for _, method := range []string{"COPY", "MOVE"} {
		t.Run(method, func(t *testing.T) {
			calRepo := &fakeCalendarRepo{
				calendars: map[int64]*store.Calendar{
					5: {ID: 5, UserID: 1, Name: "Source", UpdatedAt: now},
					6: {ID: 6, UserID: 1, Name: "Destination", UpdatedAt: now},
				},
			}
			eventRepo := &fakeEventRepo{
				events: map[string]*store.Event{
					"5:alice": {
						CalendarID:   5,
						UID:          "alice",
						ResourceName: "alice",
						RawICAL:      "BEGIN:VCALENDAR\r\nBEGIN:VEVENT\r\nUID:alice\r\nSUMMARY:Alice\r\nEND:VEVENT\r\nEND:VCALENDAR\r\n",
						ETag:         "etag-a",
						LastModified: now,
					},
					"6:bob": {
						CalendarID:   6,
						UID:          "bob",
						ResourceName: "renamed",
						RawICAL:      "BEGIN:VCALENDAR\r\nBEGIN:VEVENT\r\nUID:bob\r\nSUMMARY:Bob\r\nEND:VEVENT\r\nEND:VCALENDAR\r\n",
						ETag:         "etag-b",
						LastModified: now,
					},
				},
			}
			aclEntries := []store.ACLEntry{
				{ResourcePath: "/dav/calendars/5", PrincipalHref: "/dav/principals/2/", IsGrant: true, Privilege: "read"},
				{ResourcePath: "/dav/calendars/6", PrincipalHref: "/dav/principals/2/", IsGrant: true, Privilege: "write-content"},
			}
			if method == "MOVE" {
				aclEntries = append(aclEntries, store.ACLEntry{
					ResourcePath:  "/dav/calendars/5",
					PrincipalHref: "/dav/principals/2/",
					IsGrant:       true,
					Privilege:     "unbind",
				})
			}
			h := &Handler{store: &store.Store{
				Calendars:  calRepo,
				Events:     eventRepo,
				Locks:      &fakeLockRepo{},
				ACLEntries: &fakeACLRepo{entries: aclEntries},
			}}
			user := &store.User{ID: 2, PrimaryEmail: "delegate@example.com"}

			req := httptest.NewRequest(method, "/dav/calendars/5/alice.ics", nil)
			req.Header.Set("Destination", "https://example.com/dav/calendars/6/renamed.ics")
			req = req.WithContext(auth.WithUser(req.Context(), user))
			rr := httptest.NewRecorder()

			if method == "COPY" {
				h.Copy(rr, req)
			} else {
				h.Move(rr, req)
			}

			if rr.Code != http.StatusForbidden {
				t.Fatalf("expected overwrite %s without rebinding privileges to be rejected, got %d: %s", method, rr.Code, rr.Body.String())
			}
			dest, _ := eventRepo.GetByResourceName(req.Context(), 6, "renamed")
			if dest == nil || dest.UID != "bob" {
				t.Fatalf("expected %s to preserve the existing destination event, got %#v", method, dest)
			}
			src, _ := eventRepo.GetByUID(req.Context(), 5, "alice")
			if src == nil {
				t.Fatalf("expected %s to preserve the source event on forbidden overwrite", method)
			}
		})
	}
}

func TestMoveContactRebindsDirectLockToDestination(t *testing.T) {
	user := &store.User{ID: 1, PrimaryEmail: "owner@example.com"}
	now := store.Now()
	bookRepo := &fakeAddressBookRepo{
		books: map[int64]*store.AddressBook{
			5: {ID: 5, UserID: user.ID, Name: "Source", UpdatedAt: now},
			6: {ID: 6, UserID: user.ID, Name: "Destination", UpdatedAt: now},
		},
	}
	contactRepo := &fakeContactRepo{
		contacts: map[string]*store.Contact{
			"5:alice": {AddressBookID: 5, UID: "alice", ResourceName: "alice", RawVCard: buildVCard("3.0", "UID:alice", "FN:Alice"), ETag: "etag-a", LastModified: now},
		},
	}
	lockToken := "opaquelocktoken:alice-lock"
	lockRepo := &fakeLockRepo{
		locks: map[string]*store.Lock{
			lockToken: {
				Token:        lockToken,
				ResourcePath: "/dav/addressbooks/5/alice",
				UserID:       user.ID,
				LockScope:    "exclusive",
				LockType:     "write",
				Depth:        "0",
				ExpiresAt:    time.Now().Add(time.Hour),
			},
		},
	}
	h := &Handler{store: &store.Store{AddressBooks: bookRepo, Contacts: contactRepo, Locks: lockRepo}}

	moveReq := httptest.NewRequest("MOVE", "/dav/addressbooks/5/alice.vcf", nil)
	moveReq.Header.Set("Destination", "https://example.com/dav/addressbooks/6/copied.vcf")
	moveReq.Header.Set("If", "(<"+lockToken+">)")
	moveReq = moveReq.WithContext(auth.WithUser(moveReq.Context(), user))
	moveRR := httptest.NewRecorder()
	h.Move(moveRR, moveReq)

	if moveRR.Code != http.StatusCreated {
		t.Fatalf("expected MOVE to succeed, got %d: %s", moveRR.Code, moveRR.Body.String())
	}

	putDestReq := newAddressBookPutRequest("/dav/addressbooks/6/copied.vcf", strings.NewReader(buildVCard("3.0", "UID:alice", "FN:Alice Updated")))
	putDestReq = putDestReq.WithContext(auth.WithUser(putDestReq.Context(), user))
	putDestRR := httptest.NewRecorder()
	h.Put(putDestRR, putDestReq)

	if putDestRR.Code != http.StatusLocked {
		t.Fatalf("expected moved lock to protect destination resource, got %d: %s", putDestRR.Code, putDestRR.Body.String())
	}

	putDestReq = newAddressBookPutRequest("/dav/addressbooks/6/copied.vcf", strings.NewReader(buildVCard("3.0", "UID:alice", "FN:Alice Updated")))
	putDestReq.Header.Set("If", "(<"+lockToken+">)")
	putDestReq = putDestReq.WithContext(auth.WithUser(putDestReq.Context(), user))
	putDestRR = httptest.NewRecorder()
	h.Put(putDestRR, putDestReq)

	if putDestRR.Code != http.StatusNoContent {
		t.Fatalf("expected destination write with moved lock token to succeed, got %d: %s", putDestRR.Code, putDestRR.Body.String())
	}

	putOldReq := newAddressBookPutRequest("/dav/addressbooks/5/alice.vcf", strings.NewReader(buildVCard("3.0", "UID:new-alice", "FN:Replacement Alice")))
	putOldReq = putOldReq.WithContext(auth.WithUser(putOldReq.Context(), user))
	putOldRR := httptest.NewRecorder()
	h.Put(putOldRR, putOldReq)

	if putOldRR.Code != http.StatusCreated {
		t.Fatalf("expected old path to be unlocked after move, got %d: %s", putOldRR.Code, putOldRR.Body.String())
	}
}

func TestMoveContactOverwritePreservesDestinationDAVState(t *testing.T) {
	user := &store.User{ID: 1, PrimaryEmail: "owner@example.com"}
	delegate := &store.User{ID: 2, PrimaryEmail: "delegate@example.com"}
	now := store.Now()
	bookRepo := &fakeAddressBookRepo{
		books: map[int64]*store.AddressBook{
			5: {ID: 5, UserID: user.ID, Name: "Source", UpdatedAt: now},
			6: {ID: 6, UserID: user.ID, Name: "Destination", UpdatedAt: now},
		},
	}
	contactRepo := &fakeContactRepo{
		contacts: map[string]*store.Contact{
			"5:alice": {AddressBookID: 5, UID: "alice", ResourceName: "alice", RawVCard: buildVCard("3.0", "UID:alice", "FN:Alice"), ETag: "etag-a", LastModified: now},
			"6:bob":   {AddressBookID: 6, UID: "bob", ResourceName: "renamed", RawVCard: buildVCard("3.0", "UID:bob", "FN:Bob"), ETag: "etag-b", LastModified: now},
		},
	}
	srcToken := "opaquelocktoken:source-lock"
	destToken := "opaquelocktoken:dest-lock"
	lockRepo := &fakeLockRepo{
		locks: map[string]*store.Lock{
			srcToken: {
				Token:        srcToken,
				ResourcePath: "/dav/addressbooks/5/alice",
				UserID:       user.ID,
				LockScope:    "exclusive",
				LockType:     "write",
				Depth:        "0",
				ExpiresAt:    time.Now().Add(time.Hour),
			},
			destToken: {
				Token:        destToken,
				ResourcePath: "/dav/addressbooks/6/renamed",
				UserID:       user.ID,
				LockScope:    "exclusive",
				LockType:     "write",
				Depth:        "0",
				ExpiresAt:    time.Now().Add(time.Hour),
			},
		},
	}
	aclRepo := &fakeACLRepo{
		entries: []store.ACLEntry{
			{ResourcePath: "/dav/addressbooks/6/renamed", PrincipalHref: "/dav/principals/2/", IsGrant: true, Privilege: "read"},
		},
	}
	h := &Handler{store: &store.Store{AddressBooks: bookRepo, Contacts: contactRepo, Locks: lockRepo, ACLEntries: aclRepo}}

	moveReq := httptest.NewRequest("MOVE", "/dav/addressbooks/5/alice.vcf", nil)
	moveReq.Header.Set("Destination", "https://example.com/dav/addressbooks/6/renamed.vcf")
	moveReq.Header.Set("Overwrite", "T")
	moveReq.Header.Set("If", "</dav/addressbooks/5/alice.vcf> (<"+srcToken+">) </dav/addressbooks/6/renamed.vcf> (<"+destToken+">)")
	moveReq = moveReq.WithContext(auth.WithUser(moveReq.Context(), user))
	moveRR := httptest.NewRecorder()
	h.Move(moveRR, moveReq)

	if moveRR.Code != http.StatusNoContent {
		t.Fatalf("expected overwrite MOVE to succeed, got %d: %s", moveRR.Code, moveRR.Body.String())
	}

	putReq := newAddressBookPutRequest("/dav/addressbooks/6/renamed.vcf", strings.NewReader(buildVCard("3.0", "UID:alice", "FN:Alice Updated")))
	putReq.Header.Set("If", "(<"+srcToken+">)")
	putReq = putReq.WithContext(auth.WithUser(putReq.Context(), user))
	putRR := httptest.NewRecorder()
	h.Put(putRR, putReq)

	if putRR.Code != http.StatusNoContent {
		t.Fatalf("expected source lock token to be rebound onto overwritten destination, got %d: %s", putRR.Code, putRR.Body.String())
	}

	putReq = newAddressBookPutRequest("/dav/addressbooks/6/renamed.vcf", strings.NewReader(buildVCard("3.0", "UID:alice", "FN:Alice Updated Again")))
	putReq.Header.Set("If", "(<"+destToken+">)")
	putReq = putReq.WithContext(auth.WithUser(putReq.Context(), user))
	putRR = httptest.NewRecorder()
	h.Put(putRR, putReq)

	if putRR.Code != http.StatusLocked {
		t.Fatalf("expected overwritten destination lock token to be cleared, got %d: %s", putRR.Code, putRR.Body.String())
	}

	getReq := httptest.NewRequest(http.MethodGet, "/dav/addressbooks/6/renamed.vcf", nil)
	getReq = getReq.WithContext(auth.WithUser(getReq.Context(), delegate))
	getRR := httptest.NewRecorder()
	h.Get(getRR, getReq)

	if getRR.Code != http.StatusNotFound {
		t.Fatalf("expected overwrite MOVE to clear destination ACL state, got %d: %s", getRR.Code, getRR.Body.String())
	}
}

func TestDeleteContactRemovesDirectLockState(t *testing.T) {
	user := &store.User{ID: 1, PrimaryEmail: "owner@example.com"}
	bookRepo := &fakeAddressBookRepo{
		books: map[int64]*store.AddressBook{
			5: {ID: 5, UserID: user.ID, Name: "Contacts"},
		},
	}
	contactRepo := &fakeContactRepo{
		contacts: map[string]*store.Contact{
			"5:alice": {AddressBookID: 5, UID: "alice", ResourceName: "alice", RawVCard: buildVCard("3.0", "UID:alice", "FN:Alice"), ETag: "etag-a"},
		},
	}
	lockToken := "opaquelocktoken:alice-delete"
	lockRepo := &fakeLockRepo{
		locks: map[string]*store.Lock{
			lockToken: {
				Token:        lockToken,
				ResourcePath: "/dav/addressbooks/5/alice",
				UserID:       user.ID,
				LockScope:    "exclusive",
				LockType:     "write",
				Depth:        "0",
				ExpiresAt:    time.Now().Add(time.Hour),
			},
		},
	}
	h := &Handler{store: &store.Store{AddressBooks: bookRepo, Contacts: contactRepo, Locks: lockRepo}}

	deleteReq := httptest.NewRequest(http.MethodDelete, "/dav/addressbooks/5/alice.vcf", nil)
	deleteReq.Header.Set("If", "(<"+lockToken+">)")
	deleteReq = deleteReq.WithContext(auth.WithUser(deleteReq.Context(), user))
	deleteRR := httptest.NewRecorder()
	h.Delete(deleteRR, deleteReq)

	if deleteRR.Code != http.StatusNoContent {
		t.Fatalf("expected DELETE to succeed, got %d: %s", deleteRR.Code, deleteRR.Body.String())
	}

	putReq := newAddressBookPutRequest("/dav/addressbooks/5/alice.vcf", strings.NewReader(buildVCard("3.0", "UID:replacement", "FN:Replacement Alice")))
	putReq = putReq.WithContext(auth.WithUser(putReq.Context(), user))
	putRR := httptest.NewRecorder()
	h.Put(putRR, putReq)

	if putRR.Code != http.StatusCreated {
		t.Fatalf("expected deleted path to be reusable without stale lock token, got %d: %s", putRR.Code, putRR.Body.String())
	}
}

func TestMoveContactRebindsACLToDestination(t *testing.T) {
	owner := &store.User{ID: 1, PrimaryEmail: "owner@example.com"}
	delegate := &store.User{ID: 2, PrimaryEmail: "delegate@example.com"}
	now := store.Now()
	bookRepo := &fakeAddressBookRepo{
		books: map[int64]*store.AddressBook{
			5: {ID: 5, UserID: owner.ID, Name: "Source", UpdatedAt: now},
			6: {ID: 6, UserID: owner.ID, Name: "Destination", UpdatedAt: now},
		},
	}
	contactRepo := &fakeContactRepo{
		contacts: map[string]*store.Contact{
			"5:alice": {AddressBookID: 5, UID: "alice", ResourceName: "alice", RawVCard: buildVCard("3.0", "UID:alice", "FN:Alice"), ETag: "etag-a", LastModified: now},
		},
	}
	aclRepo := &fakeACLRepo{
		entries: []store.ACLEntry{
			{ResourcePath: "/dav/addressbooks/5/alice", PrincipalHref: "/dav/principals/2/", IsGrant: true, Privilege: "read"},
		},
	}
	h := &Handler{store: &store.Store{AddressBooks: bookRepo, Contacts: contactRepo, Locks: &fakeLockRepo{}, ACLEntries: aclRepo}}

	moveReq := httptest.NewRequest("MOVE", "/dav/addressbooks/5/alice.vcf", nil)
	moveReq.Header.Set("Destination", "https://example.com/dav/addressbooks/6/copied.vcf")
	moveReq = moveReq.WithContext(auth.WithUser(moveReq.Context(), owner))
	moveRR := httptest.NewRecorder()
	h.Move(moveRR, moveReq)

	if moveRR.Code != http.StatusCreated {
		t.Fatalf("expected MOVE to succeed, got %d: %s", moveRR.Code, moveRR.Body.String())
	}

	getDestReq := httptest.NewRequest(http.MethodGet, "/dav/addressbooks/6/copied.vcf", nil)
	getDestReq = getDestReq.WithContext(auth.WithUser(getDestReq.Context(), delegate))
	getDestRR := httptest.NewRecorder()
	h.Get(getDestRR, getDestReq)

	if getDestRR.Code != http.StatusOK {
		t.Fatalf("expected moved ACL to grant delegate read access at destination, got %d: %s", getDestRR.Code, getDestRR.Body.String())
	}

	recreateReq := newAddressBookPutRequest("/dav/addressbooks/5/alice.vcf", strings.NewReader(buildVCard("3.0", "UID:replacement", "FN:Replacement Alice")))
	recreateReq = recreateReq.WithContext(auth.WithUser(recreateReq.Context(), owner))
	recreateRR := httptest.NewRecorder()
	h.Put(recreateRR, recreateReq)

	if recreateRR.Code != http.StatusCreated {
		t.Fatalf("expected owner to recreate source path after move, got %d: %s", recreateRR.Code, recreateRR.Body.String())
	}

	getOldReq := httptest.NewRequest(http.MethodGet, "/dav/addressbooks/5/alice.vcf", nil)
	getOldReq = getOldReq.WithContext(auth.WithUser(getOldReq.Context(), delegate))
	getOldRR := httptest.NewRecorder()
	h.Get(getOldRR, getOldReq)

	if getOldRR.Code != http.StatusNotFound {
		t.Fatalf("expected source ACL to be removed after move, got %d: %s", getOldRR.Code, getOldRR.Body.String())
	}
}

func TestDeleteContactRemovesResourceACLState(t *testing.T) {
	owner := &store.User{ID: 1, PrimaryEmail: "owner@example.com"}
	delegate := &store.User{ID: 2, PrimaryEmail: "delegate@example.com"}
	bookRepo := &fakeAddressBookRepo{
		books: map[int64]*store.AddressBook{
			5: {ID: 5, UserID: owner.ID, Name: "Contacts"},
		},
	}
	contactRepo := &fakeContactRepo{
		contacts: map[string]*store.Contact{
			"5:alice": {AddressBookID: 5, UID: "alice", ResourceName: "alice", RawVCard: buildVCard("3.0", "UID:alice", "FN:Alice"), ETag: "etag-a"},
		},
	}
	aclRepo := &fakeACLRepo{
		entries: []store.ACLEntry{
			{ResourcePath: "/dav/addressbooks/5/alice", PrincipalHref: "/dav/principals/2/", IsGrant: true, Privilege: "read"},
		},
	}
	h := &Handler{store: &store.Store{AddressBooks: bookRepo, Contacts: contactRepo, ACLEntries: aclRepo}}

	deleteReq := httptest.NewRequest(http.MethodDelete, "/dav/addressbooks/5/alice.vcf", nil)
	deleteReq = deleteReq.WithContext(auth.WithUser(deleteReq.Context(), owner))
	deleteRR := httptest.NewRecorder()
	h.Delete(deleteRR, deleteReq)

	if deleteRR.Code != http.StatusNoContent {
		t.Fatalf("expected DELETE to succeed, got %d: %s", deleteRR.Code, deleteRR.Body.String())
	}

	recreateReq := newAddressBookPutRequest("/dav/addressbooks/5/alice.vcf", strings.NewReader(buildVCard("3.0", "UID:replacement", "FN:Replacement Alice")))
	recreateReq = recreateReq.WithContext(auth.WithUser(recreateReq.Context(), owner))
	recreateRR := httptest.NewRecorder()
	h.Put(recreateRR, recreateReq)

	if recreateRR.Code != http.StatusCreated {
		t.Fatalf("expected owner to recreate deleted path, got %d: %s", recreateRR.Code, recreateRR.Body.String())
	}

	getReq := httptest.NewRequest(http.MethodGet, "/dav/addressbooks/5/alice.vcf", nil)
	getReq = getReq.WithContext(auth.WithUser(getReq.Context(), delegate))
	getRR := httptest.NewRecorder()
	h.Get(getRR, getReq)

	if getRR.Code != http.StatusNotFound {
		t.Fatalf("expected deleted ACL state to be cleared before recreation, got %d: %s", getRR.Code, getRR.Body.String())
	}
}

func TestCopyContactOverwriteClearsDestinationDAVState(t *testing.T) {
	owner := &store.User{ID: 1, PrimaryEmail: "owner@example.com"}
	delegate := &store.User{ID: 2, PrimaryEmail: "delegate@example.com"}
	now := store.Now()
	bookRepo := &fakeAddressBookRepo{
		books: map[int64]*store.AddressBook{
			5: {ID: 5, UserID: owner.ID, Name: "Source", UpdatedAt: now},
			6: {ID: 6, UserID: owner.ID, Name: "Destination", UpdatedAt: now},
		},
	}
	contactRepo := &fakeContactRepo{
		contacts: map[string]*store.Contact{
			"5:alice": {AddressBookID: 5, UID: "alice", ResourceName: "alice", RawVCard: buildVCard("3.0", "UID:alice", "FN:Alice"), ETag: "etag-a", LastModified: now},
			"6:bob":   {AddressBookID: 6, UID: "bob", ResourceName: "renamed", RawVCard: buildVCard("3.0", "UID:bob", "FN:Bob"), ETag: "etag-b", LastModified: now},
		},
	}
	destToken := "opaquelocktoken:dest-lock"
	lockRepo := &fakeLockRepo{
		locks: map[string]*store.Lock{
			destToken: {
				Token:        destToken,
				ResourcePath: "/dav/addressbooks/6/renamed",
				UserID:       owner.ID,
				LockScope:    "exclusive",
				LockType:     "write",
				Depth:        "0",
				ExpiresAt:    time.Now().Add(time.Hour),
			},
		},
	}
	aclRepo := &fakeACLRepo{
		entries: []store.ACLEntry{
			{ResourcePath: "/dav/addressbooks/6/renamed", PrincipalHref: "/dav/principals/2/", IsGrant: true, Privilege: "read"},
		},
	}
	h := &Handler{store: &store.Store{
		AddressBooks: bookRepo,
		Contacts:     contactRepo,
		Locks:        lockRepo,
		ACLEntries:   aclRepo,
	}}

	copyReq := httptest.NewRequest("COPY", "/dav/addressbooks/5/alice.vcf", nil)
	copyReq.Header.Set("Destination", "https://example.com/dav/addressbooks/6/renamed.vcf")
	copyReq.Header.Set("Overwrite", "T")
	copyReq.Header.Set("If", "</dav/addressbooks/6/renamed.vcf> (<"+destToken+">)")
	copyReq = copyReq.WithContext(auth.WithUser(copyReq.Context(), owner))
	copyRR := httptest.NewRecorder()
	h.Copy(copyRR, copyReq)

	if copyRR.Code != http.StatusNoContent {
		t.Fatalf("expected overwrite COPY to succeed, got %d: %s", copyRR.Code, copyRR.Body.String())
	}
	if _, ok := lockRepo.locks[destToken]; ok {
		t.Fatalf("expected overwrite COPY to clear destination lock state, got %#v", lockRepo.locks)
	}
	if entries, _ := aclRepo.ListByResource(context.Background(), "/dav/addressbooks/6/renamed"); len(entries) != 0 {
		t.Fatalf("expected overwrite COPY to clear destination ACL state, got %#v", entries)
	}

	getReq := httptest.NewRequest(http.MethodGet, "/dav/addressbooks/6/renamed.vcf", nil)
	getReq = getReq.WithContext(auth.WithUser(getReq.Context(), delegate))
	getRR := httptest.NewRecorder()
	h.Get(getRR, getReq)

	if getRR.Code != http.StatusNotFound {
		t.Fatalf("expected overwrite COPY to clear destination ACL access, got %d: %s", getRR.Code, getRR.Body.String())
	}
}

func TestCopyCalendarOverwriteClearsDestinationDAVState(t *testing.T) {
	owner := &store.User{ID: 1, PrimaryEmail: "owner@example.com"}
	delegate := &store.User{ID: 2, PrimaryEmail: "delegate@example.com"}
	now := store.Now()
	calRepo := &fakeCalendarRepo{
		accessible: []store.CalendarAccess{
			{Calendar: store.Calendar{ID: 5, UserID: owner.ID, Name: "Source", UpdatedAt: now}, Editor: true},
			{Calendar: store.Calendar{ID: 6, UserID: owner.ID, Name: "Destination", UpdatedAt: now}, Editor: true},
		},
		calendars: map[int64]*store.Calendar{
			5: {ID: 5, UserID: owner.ID, Name: "Source", UpdatedAt: now},
			6: {ID: 6, UserID: owner.ID, Name: "Destination", UpdatedAt: now},
		},
	}
	eventRepo := &fakeEventRepo{
		events: map[string]*store.Event{
			"5:alice": {CalendarID: 5, UID: "alice", ResourceName: "alice", RawICAL: "BEGIN:VCALENDAR\r\nBEGIN:VEVENT\r\nUID:alice\r\nSUMMARY:Alice\r\nEND:VEVENT\r\nEND:VCALENDAR\r\n", ETag: "etag-a", LastModified: now},
			"6:bob":   {CalendarID: 6, UID: "bob", ResourceName: "renamed", RawICAL: "BEGIN:VCALENDAR\r\nBEGIN:VEVENT\r\nUID:bob\r\nSUMMARY:Bob\r\nEND:VEVENT\r\nEND:VCALENDAR\r\n", ETag: "etag-b", LastModified: now},
		},
	}
	destToken := "opaquelocktoken:dest-calendar-lock"
	lockRepo := &fakeLockRepo{
		locks: map[string]*store.Lock{
			destToken: {
				Token:        destToken,
				ResourcePath: "/dav/calendars/6/renamed",
				UserID:       owner.ID,
				LockScope:    "exclusive",
				LockType:     "write",
				Depth:        "0",
				ExpiresAt:    time.Now().Add(time.Hour),
			},
		},
	}
	aclRepo := &fakeACLRepo{
		entries: []store.ACLEntry{
			{ResourcePath: "/dav/calendars/6/renamed", PrincipalHref: "/dav/principals/2/", IsGrant: true, Privilege: "read"},
		},
	}
	h := &Handler{store: &store.Store{
		Calendars:  calRepo,
		Events:     eventRepo,
		Locks:      lockRepo,
		ACLEntries: aclRepo,
	}}

	copyReq := httptest.NewRequest("COPY", "/dav/calendars/5/alice.ics", nil)
	copyReq.Header.Set("Destination", "https://example.com/dav/calendars/6/renamed.ics")
	copyReq.Header.Set("Overwrite", "T")
	copyReq.Header.Set("If", "</dav/calendars/6/renamed.ics> (<"+destToken+">)")
	copyReq = copyReq.WithContext(auth.WithUser(copyReq.Context(), owner))
	copyRR := httptest.NewRecorder()
	h.Copy(copyRR, copyReq)

	if copyRR.Code != http.StatusNoContent {
		t.Fatalf("expected overwrite COPY to succeed, got %d: %s", copyRR.Code, copyRR.Body.String())
	}
	if _, ok := lockRepo.locks[destToken]; ok {
		t.Fatalf("expected overwrite COPY to clear destination lock state, got %#v", lockRepo.locks)
	}
	if entries, _ := aclRepo.ListByResource(context.Background(), "/dav/calendars/6/renamed"); len(entries) != 0 {
		t.Fatalf("expected overwrite COPY to clear destination ACL state, got %#v", entries)
	}

	getReq := httptest.NewRequest(http.MethodGet, "/dav/calendars/6/renamed.ics", nil)
	getReq = getReq.WithContext(auth.WithUser(getReq.Context(), delegate))
	getRR := httptest.NewRecorder()
	h.Get(getRR, getReq)

	if getRR.Code != http.StatusNotFound {
		t.Fatalf("expected overwrite COPY to clear destination ACL access, got %d: %s", getRR.Code, getRR.Body.String())
	}
}

func TestPropfindACLUsesSpecialPrincipalElements(t *testing.T) {
	user := &store.User{ID: 1, PrimaryEmail: "owner@example.com"}
	bookRepo := &fakeAddressBookRepo{
		books: map[int64]*store.AddressBook{
			5: {ID: 5, UserID: user.ID, Name: "Contacts"},
		},
	}
	aclRepo := &fakeACLRepo{
		entries: []store.ACLEntry{
			{ResourcePath: "/dav/addressbooks/5", PrincipalHref: "DAV:authenticated", IsGrant: true, Privilege: "read"},
			{ResourcePath: "/dav/addressbooks/5", PrincipalHref: "DAV:all", IsGrant: false, Privilege: "write"},
		},
	}
	h := &Handler{store: &store.Store{AddressBooks: bookRepo, ACLEntries: aclRepo}}

	body := `<?xml version="1.0" encoding="utf-8"?>
<d:propfind xmlns:d="DAV:">
  <d:prop>
    <d:acl/>
  </d:prop>
</d:propfind>`
	req := httptest.NewRequest("PROPFIND", "/dav/addressbooks/5/", strings.NewReader(body))
	req.Header.Set("Depth", "0")
	req = req.WithContext(auth.WithUser(req.Context(), user))
	rr := httptest.NewRecorder()

	h.Propfind(rr, req)

	if rr.Code != http.StatusMultiStatus {
		t.Fatalf("expected PROPFIND to succeed, got %d: %s", rr.Code, rr.Body.String())
	}
	respBody := rr.Body.String()
	if !strings.Contains(respBody, "<d:authenticated") {
		t.Fatalf("expected DAV:authenticated principal element, got %s", respBody)
	}
	if !strings.Contains(respBody, "<d:all") {
		t.Fatalf("expected DAV:all principal element, got %s", respBody)
	}
	for _, invalid := range []string{"<d:href>DAV:authenticated</d:href>", "<d:href>DAV:all</d:href>"} {
		if strings.Contains(respBody, invalid) {
			t.Fatalf("did not expect ACL principal to be serialized as href %q, got %s", invalid, respBody)
		}
	}
}

func TestBuildACLPropFromEntriesSeparatesGrantAndDenyACEs(t *testing.T) {
	acl := buildACLPropFromEntries([]store.ACLEntry{
		{PrincipalHref: "/dav/principals/2/", IsGrant: true, Privilege: "read"},
		{PrincipalHref: "/dav/principals/2/", IsGrant: false, Privilege: "write"},
	})

	if len(acl.ACE) != 2 {
		t.Fatalf("expected separate ACEs for grant and deny, got %#v", acl.ACE)
	}
	if acl.ACE[0].Grant == nil || acl.ACE[0].Deny != nil {
		t.Fatalf("expected first ACE to contain only a grant, got %#v", acl.ACE[0])
	}
	if acl.ACE[1].Deny == nil || acl.ACE[1].Grant != nil {
		t.Fatalf("expected second ACE to contain only a deny, got %#v", acl.ACE[1])
	}
}

func TestContactWritesFailClosedOnLookupErrors(t *testing.T) {
	user := &store.User{ID: 1, PrimaryEmail: "owner@example.com"}
	bookRepo := &fakeAddressBookRepo{
		books: map[int64]*store.AddressBook{
			5: {ID: 5, UserID: user.ID, Name: "Source"},
			6: {ID: 6, UserID: user.ID, Name: "Destination"},
		},
	}

	t.Run("put", func(t *testing.T) {
		contactRepo := &fakeContactRepo{
			contacts:             map[string]*store.Contact{},
			getByResourceNameErr: errors.New("resource lookup failed"),
		}
		h := &Handler{store: &store.Store{AddressBooks: bookRepo, Contacts: contactRepo}}
		req := newAddressBookPutRequest("/dav/addressbooks/5/alice.vcf", strings.NewReader(buildVCard("3.0", "UID:alice", "FN:Alice Example")))
		req = req.WithContext(auth.WithUser(req.Context(), user))
		rr := httptest.NewRecorder()

		h.Put(rr, req)

		if rr.Code != http.StatusInternalServerError {
			t.Fatalf("expected PUT lookup failure to return 500, got %d: %s", rr.Code, rr.Body.String())
		}
		if len(contactRepo.contacts) != 0 {
			t.Fatalf("expected PUT lookup failure to avoid writes, got %#v", contactRepo.contacts)
		}
	})

	t.Run("put uid lookup", func(t *testing.T) {
		contactRepo := &fakeContactRepo{
			contacts:       map[string]*store.Contact{},
			getByUIDErr:    errors.New("uid lookup failed"),
			getByUIDErrKey: "5:alice",
		}
		h := &Handler{store: &store.Store{AddressBooks: bookRepo, Contacts: contactRepo}}
		req := newAddressBookPutRequest("/dav/addressbooks/5/alice.vcf", strings.NewReader(buildVCard("3.0", "UID:alice", "FN:Alice Example")))
		req = req.WithContext(auth.WithUser(req.Context(), user))
		rr := httptest.NewRecorder()

		h.Put(rr, req)

		if rr.Code != http.StatusInternalServerError {
			t.Fatalf("expected PUT UID lookup failure to return 500, got %d: %s", rr.Code, rr.Body.String())
		}
		if len(contactRepo.contacts) != 0 {
			t.Fatalf("expected PUT UID lookup failure to avoid writes, got %#v", contactRepo.contacts)
		}
	})

	for _, method := range []string{"COPY", "MOVE"} {
		t.Run(strings.ToLower(method)+" destination name lookup", func(t *testing.T) {
			contactRepo := &fakeContactRepo{
				contacts: map[string]*store.Contact{
					"5:alice": {
						AddressBookID: 5,
						UID:           "alice",
						ResourceName:  "alice",
						RawVCard:      buildVCard("3.0", "UID:alice", "FN:Alice Example"),
						ETag:          "etag-alice",
					},
				},
				getByResourceNameErr:    errors.New("destination lookup failed"),
				getByResourceNameErrKey: "6:copied",
			}
			h := &Handler{store: &store.Store{AddressBooks: bookRepo, Contacts: contactRepo}}
			req := httptest.NewRequest(method, "/dav/addressbooks/5/alice.vcf", nil)
			req.Header.Set("Destination", "https://example.com/dav/addressbooks/6/copied.vcf")
			req = req.WithContext(auth.WithUser(req.Context(), user))
			rr := httptest.NewRecorder()

			if method == "COPY" {
				h.Copy(rr, req)
			} else {
				h.Move(rr, req)
			}

			if rr.Code != http.StatusInternalServerError {
				t.Fatalf("expected %s lookup failure to return 500, got %d: %s", method, rr.Code, rr.Body.String())
			}
			if _, ok := contactRepo.contacts["6:alice"]; ok {
				t.Fatalf("expected %s lookup failure to avoid destination writes, got %#v", method, contactRepo.contacts)
			}
			if _, ok := contactRepo.contacts["5:alice"]; !ok {
				t.Fatalf("expected %s lookup failure to preserve source, got %#v", method, contactRepo.contacts)
			}
		})

		t.Run(strings.ToLower(method)+" source lookup", func(t *testing.T) {
			contactRepo := &fakeContactRepo{
				contacts: map[string]*store.Contact{
					"5:alice": {
						AddressBookID: 5,
						UID:           "alice",
						ResourceName:  "alice",
						RawVCard:      buildVCard("3.0", "UID:alice", "FN:Alice Example"),
						ETag:          "etag-alice",
					},
				},
				getByResourceNameErr:    errors.New("source lookup failed"),
				getByResourceNameErrKey: "5:alice",
			}
			h := &Handler{store: &store.Store{AddressBooks: bookRepo, Contacts: contactRepo}}
			req := httptest.NewRequest(method, "/dav/addressbooks/5/alice.vcf", nil)
			req.Header.Set("Destination", "https://example.com/dav/addressbooks/6/copied.vcf")
			req = req.WithContext(auth.WithUser(req.Context(), user))
			rr := httptest.NewRecorder()

			if method == "COPY" {
				h.Copy(rr, req)
			} else {
				h.Move(rr, req)
			}

			if rr.Code != http.StatusInternalServerError {
				t.Fatalf("expected %s source lookup failure to return 500, got %d: %s", method, rr.Code, rr.Body.String())
			}
			if _, ok := contactRepo.contacts["6:alice"]; ok {
				t.Fatalf("expected %s source lookup failure to avoid destination writes, got %#v", method, contactRepo.contacts)
			}
		})

		t.Run(strings.ToLower(method)+" destination uid lookup", func(t *testing.T) {
			contactRepo := &fakeContactRepo{
				contacts: map[string]*store.Contact{
					"5:alice": {
						AddressBookID: 5,
						UID:           "alice",
						ResourceName:  "alice",
						RawVCard:      buildVCard("3.0", "UID:alice", "FN:Alice Example"),
						ETag:          "etag-alice",
					},
				},
				getByUIDErr:    errors.New("destination uid lookup failed"),
				getByUIDErrKey: "6:alice",
			}
			h := &Handler{store: &store.Store{AddressBooks: bookRepo, Contacts: contactRepo}}
			req := httptest.NewRequest(method, "/dav/addressbooks/5/alice.vcf", nil)
			req.Header.Set("Destination", "https://example.com/dav/addressbooks/6/copied.vcf")
			req = req.WithContext(auth.WithUser(req.Context(), user))
			rr := httptest.NewRecorder()

			if method == "COPY" {
				h.Copy(rr, req)
			} else {
				h.Move(rr, req)
			}

			if rr.Code != http.StatusInternalServerError {
				t.Fatalf("expected %s destination UID lookup failure to return 500, got %d: %s", method, rr.Code, rr.Body.String())
			}
			if _, ok := contactRepo.contacts["6:alice"]; ok {
				t.Fatalf("expected %s destination UID lookup failure to avoid destination writes, got %#v", method, contactRepo.contacts)
			}
			if _, ok := contactRepo.contacts["5:alice"]; !ok {
				t.Fatalf("expected %s destination UID lookup failure to preserve source, got %#v", method, contactRepo.contacts)
			}
		})
	}
}

func TestCanLockAddressBookCollectionRequiresMoreThanBind(t *testing.T) {
	user := &store.User{ID: 2, PrimaryEmail: "delegate@example.com"}
	bookRepo := &fakeAddressBookRepo{
		books: map[int64]*store.AddressBook{
			5: {ID: 5, UserID: 1, Name: "Contacts"},
		},
	}
	h := &Handler{store: &store.Store{
		AddressBooks: bookRepo,
		Contacts:     &fakeContactRepo{},
		ACLEntries: &fakeACLRepo{entries: []store.ACLEntry{
			{ResourcePath: "/dav/addressbooks/5", PrincipalHref: "/dav/principals/2/", IsGrant: true, Privilege: "bind"},
		}},
	}}

	got, err := h.canLockAddressBookPath(context.Background(), user, "/dav/addressbooks/5/")
	if err != nil {
		t.Fatalf("canLockAddressBookPath() error = %v", err)
	}
	if got {
		t.Fatal("expected bind-only principal to be unable to lock an existing address book collection")
	}
}

func TestLockScopesDirectChildCollectionCreationPathsPerUser(t *testing.T) {
	tests := []struct {
		name string
		path string
	}{
		{name: "address books", path: "/dav/addressbooks/Shared"},
		{name: "calendars", path: "/dav/calendars/Shared"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			lockRepo := &fakeLockRepo{}
			h := &Handler{store: &store.Store{Locks: lockRepo}}
			lockBody := `<?xml version="1.0" encoding="utf-8"?>
<D:lockinfo xmlns:D="DAV:">
  <D:lockscope><D:exclusive/></D:lockscope>
  <D:locktype><D:write/></D:locktype>
</D:lockinfo>`

			for _, user := range []*store.User{
				{ID: 1, PrimaryEmail: "owner1@example.com"},
				{ID: 2, PrimaryEmail: "owner2@example.com"},
			} {
				req := httptest.NewRequest("LOCK", tc.path, strings.NewReader(lockBody))
				req = req.WithContext(auth.WithUser(req.Context(), user))
				rr := httptest.NewRecorder()

				h.Lock(rr, req)

				if rr.Code != http.StatusCreated {
					t.Fatalf("expected LOCK on %s for user %d to succeed, got %d: %s", tc.path, user.ID, rr.Code, rr.Body.String())
				}
			}

			if len(lockRepo.locks) != 2 {
				t.Fatalf("expected both users to get independent locks, got %#v", lockRepo.locks)
			}

			paths := make(map[string]struct{}, len(lockRepo.locks))
			for _, lock := range lockRepo.locks {
				paths[lock.ResourcePath] = struct{}{}
			}
			if len(paths) != 2 {
				t.Fatalf("expected lock paths to be scoped per user, got %#v", lockRepo.locks)
			}
		})
	}
}

func TestMkcolIgnoresAnotherUsersPendingLockForSameCollectionName(t *testing.T) {
	lockRepo := &fakeLockRepo{}
	bookRepo := &fakeAddressBookRepo{}
	h := &Handler{store: &store.Store{Locks: lockRepo, AddressBooks: bookRepo}}
	lockBody := `<?xml version="1.0" encoding="utf-8"?>
<D:lockinfo xmlns:D="DAV:">
  <D:lockscope><D:exclusive/></D:lockscope>
  <D:locktype><D:write/></D:locktype>
</D:lockinfo>`

	lockReq := httptest.NewRequest("LOCK", "/dav/addressbooks/Shared", strings.NewReader(lockBody))
	lockReq = lockReq.WithContext(auth.WithUser(lockReq.Context(), &store.User{ID: 1, PrimaryEmail: "owner1@example.com"}))
	lockRR := httptest.NewRecorder()
	h.Lock(lockRR, lockReq)
	if lockRR.Code != http.StatusCreated {
		t.Fatalf("expected initial LOCK to succeed, got %d: %s", lockRR.Code, lockRR.Body.String())
	}

	mkcolReq := httptest.NewRequest("MKCOL", "/dav/addressbooks/Shared", nil)
	mkcolReq = mkcolReq.WithContext(auth.WithUser(mkcolReq.Context(), &store.User{ID: 2, PrimaryEmail: "owner2@example.com"}))
	mkcolRR := httptest.NewRecorder()
	h.Mkcol(mkcolRR, mkcolReq)

	if mkcolRR.Code != http.StatusCreated {
		t.Fatalf("expected MKCOL for another user to ignore the foreign pending lock, got %d: %s", mkcolRR.Code, mkcolRR.Body.String())
	}
	books, err := bookRepo.ListByUser(context.Background(), 2)
	if err != nil {
		t.Fatalf("ListByUser() error = %v", err)
	}
	if len(books) != 1 || books[0].Name != "Shared" {
		t.Fatalf("expected second user to get an independent Shared address book, got %#v", books)
	}
}

func TestMkcalendarIgnoresAnotherUsersPendingLockForSameCollectionName(t *testing.T) {
	lockRepo := &fakeLockRepo{}
	calRepo := &fakeCalendarRepo{}
	h := &Handler{store: &store.Store{Locks: lockRepo, Calendars: calRepo}}
	lockBody := `<?xml version="1.0" encoding="utf-8"?>
<D:lockinfo xmlns:D="DAV:">
  <D:lockscope><D:exclusive/></D:lockscope>
  <D:locktype><D:write/></D:locktype>
</D:lockinfo>`

	lockReq := httptest.NewRequest("LOCK", "/dav/calendars/Shared", strings.NewReader(lockBody))
	lockReq = lockReq.WithContext(auth.WithUser(lockReq.Context(), &store.User{ID: 1, PrimaryEmail: "owner1@example.com"}))
	lockRR := httptest.NewRecorder()
	h.Lock(lockRR, lockReq)
	if lockRR.Code != http.StatusCreated {
		t.Fatalf("expected initial LOCK to succeed, got %d: %s", lockRR.Code, lockRR.Body.String())
	}

	mkcalendarReq := httptest.NewRequest("MKCALENDAR", "/dav/calendars/Shared", nil)
	mkcalendarReq = mkcalendarReq.WithContext(auth.WithUser(mkcalendarReq.Context(), &store.User{ID: 2, PrimaryEmail: "owner2@example.com"}))
	mkcalendarRR := httptest.NewRecorder()
	h.Mkcalendar(mkcalendarRR, mkcalendarReq)

	if mkcalendarRR.Code != http.StatusCreated {
		t.Fatalf("expected MKCALENDAR for another user to ignore the foreign pending lock, got %d: %s", mkcalendarRR.Code, mkcalendarRR.Body.String())
	}
	calendars, err := calRepo.ListByUser(context.Background(), 2)
	if err != nil {
		t.Fatalf("ListByUser() error = %v", err)
	}
	if len(calendars) != 1 || calendars[0].Name != "Shared" {
		t.Fatalf("expected second user to get an independent Shared calendar, got %#v", calendars)
	}
}

func TestPutReturnsInternalServerErrorWhenLockLookupFails(t *testing.T) {
	bookRepo := &fakeAddressBookRepo{
		books: map[int64]*store.AddressBook{
			5: {ID: 5, UserID: 1, Name: "Contacts"},
		},
	}
	lockRepo := &fakeLockRepo{listByResourcesErr: errors.New("lock lookup failed")}
	h := &Handler{store: &store.Store{AddressBooks: bookRepo, Contacts: &fakeContactRepo{}, Locks: lockRepo}}

	req := httptest.NewRequest(http.MethodPut, "/dav/addressbooks/5/alice.vcf", strings.NewReader(buildVCard("3.0", "UID:alice", "FN:Alice Example")))
	req.Header.Set("Content-Type", "text/vcard")
	req = req.WithContext(auth.WithUser(req.Context(), &store.User{ID: 1}))
	rr := httptest.NewRecorder()

	h.Put(rr, req)

	if rr.Code != http.StatusInternalServerError {
		t.Fatalf("expected lock lookup failure to return 500, got %d: %s", rr.Code, rr.Body.String())
	}
}

func TestProppatchIgnoresParentCollectionLock(t *testing.T) {
	now := store.Now()
	bookRepo := &fakeAddressBookRepo{
		books: map[int64]*store.AddressBook{
			5: {ID: 5, UserID: 1, Name: "Contacts", UpdatedAt: now},
		},
	}
	lockRepo := &fakeLockRepo{
		locks: map[string]*store.Lock{
			"opaquelocktoken:root": {
				Token:        "opaquelocktoken:root",
				ResourcePath: "/dav/addressbooks",
				UserID:       1,
				LockScope:    "exclusive",
				LockType:     "write",
				Depth:        "0",
				ExpiresAt:    time.Now().Add(time.Hour),
			},
		},
	}
	h := &Handler{store: &store.Store{AddressBooks: bookRepo, Contacts: &fakeContactRepo{}, Locks: lockRepo}}

	body := `<?xml version="1.0" encoding="utf-8"?>
<D:propertyupdate xmlns:D="DAV:" xmlns:card="urn:ietf:params:xml:ns:carddav">
  <D:set>
    <D:prop>
      <D:displayname>Renamed Book</D:displayname>
    </D:prop>
  </D:set>
</D:propertyupdate>`

	req := httptest.NewRequest("PROPPATCH", "/dav/addressbooks/5/", strings.NewReader(body))
	req = req.WithContext(auth.WithUser(req.Context(), &store.User{ID: 1}))
	rr := httptest.NewRecorder()

	h.Proppatch(rr, req)

	if rr.Code != http.StatusMultiStatus {
		t.Fatalf("expected PROPPATCH to ignore parent collection lock and return 207, got %d: %s", rr.Code, rr.Body.String())
	}
	if got := bookRepo.books[5].Name; got != "Renamed Book" {
		t.Fatalf("expected PROPPATCH to update the address book name, got %q", got)
	}
}

func TestPropfindAddressBookHomeSetIncludesACLSharedBooks(t *testing.T) {
	user := &store.User{ID: 2, PrimaryEmail: "reader@example.com"}
	bookRepo := &fakeAddressBookRepo{
		books: map[int64]*store.AddressBook{
			2: {ID: 2, UserID: user.ID, Name: "Owned"},
			5: {ID: 5, UserID: 1, Name: "Shared"},
		},
	}
	aclRepo := &fakeACLRepo{
		entries: []store.ACLEntry{
			{ResourcePath: "/dav/addressbooks/5", PrincipalHref: "/dav/principals/2/", IsGrant: true, Privilege: "read"},
		},
	}
	h := &Handler{store: &store.Store{AddressBooks: bookRepo, Contacts: &fakeContactRepo{}, ACLEntries: aclRepo}}

	body := `<?xml version="1.0" encoding="utf-8"?>
<d:propfind xmlns:d="DAV:">
  <d:prop>
    <d:displayname/>
    <d:resourcetype/>
  </d:prop>
</d:propfind>`
	req := httptest.NewRequest("PROPFIND", "/dav/addressbooks/", strings.NewReader(body))
	req.Header.Set("Depth", "1")
	req = req.WithContext(auth.WithUser(req.Context(), user))
	rr := httptest.NewRecorder()

	h.Propfind(rr, req)

	if rr.Code != http.StatusMultiStatus {
		t.Fatalf("expected PROPFIND to succeed, got %d: %s", rr.Code, rr.Body.String())
	}
	respBody := rr.Body.String()
	for _, href := range []string{"/dav/addressbooks/2/", "/dav/addressbooks/5/"} {
		if !strings.Contains(respBody, href) {
			t.Fatalf("expected addressbook-home-set listing to include %s, got %s", href, respBody)
		}
	}
}

type fakeEventRepo struct {
	events                   map[string]*store.Event
	deleted                  []string
	copyErr                  error
	moveErr                  error
	getByUIDErr              error
	getByUIDErrKey           string
	getByResourceNameErr     error
	getByResourceNameKey     string
	resourceLookupCount      int
	overwriteMoveDeletedRepo *fakeDeletedResourceRepo
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
	f.deleted = append(f.deleted, f.key(calendarID, uid))
	delete(f.events, f.key(calendarID, uid))
	return nil
}

func (f *fakeEventRepo) GetByUID(ctx context.Context, calendarID int64, uid string) (*store.Event, error) {
	if f.getByUIDErr != nil && (f.getByUIDErrKey == "" || f.getByUIDErrKey == f.key(calendarID, uid)) {
		return nil, f.getByUIDErr
	}
	if ev, ok := f.events[f.key(calendarID, uid)]; ok {
		copy := *ev
		return &copy, nil
	}
	return nil, nil
}

func (f *fakeEventRepo) GetByResourceName(ctx context.Context, calendarID int64, resourceName string) (*store.Event, error) {
	f.resourceLookupCount++
	if f.getByResourceNameErr != nil && (f.getByResourceNameKey == "" || f.getByResourceNameKey == f.key(calendarID, resourceName)) {
		return nil, f.getByResourceNameErr
	}
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

func (f *fakeEventRepo) MoveToCalendar(ctx context.Context, fromCalendarID, toCalendarID int64, uid, destResourceName string) error {
	if f.moveErr != nil {
		return f.moveErr
	}
	oldKey := f.key(fromCalendarID, uid)
	ev, ok := f.events[oldKey]
	if !ok {
		return nil
	}
	for key, existing := range f.events {
		if existing.CalendarID != toCalendarID || existing.UID == uid {
			continue
		}
		name := existing.ResourceName
		if name == "" {
			name = existing.UID
		}
		if destResourceName != "" && name == destResourceName {
			if f.overwriteMoveDeletedRepo != nil {
				f.overwriteMoveDeletedRepo.deleted = append(f.overwriteMoveDeletedRepo.deleted, store.DeletedResource{
					ResourceType: "event",
					CollectionID: toCalendarID,
					UID:          existing.UID,
					ResourceName: name,
					DeletedAt:    time.Now(),
				})
			}
			delete(f.events, key)
		}
	}
	delete(f.events, oldKey)
	ev.CalendarID = toCalendarID
	if destResourceName != "" {
		ev.ResourceName = destResourceName
	}
	f.events[f.key(toCalendarID, uid)] = ev
	return nil
}

func (f *fakeEventRepo) CopyToCalendar(ctx context.Context, fromCalendarID, toCalendarID int64, uid, destResourceName, newETag string) (*store.Event, error) {
	if f.copyErr != nil {
		return nil, f.copyErr
	}
	src, ok := f.events[f.key(fromCalendarID, uid)]
	if !ok {
		return nil, nil
	}
	copy := *src
	copy.CalendarID = toCalendarID
	if destResourceName != "" {
		copy.ResourceName = destResourceName
	}
	copy.ETag = newETag
	f.events[f.key(toCalendarID, copy.UID)] = &copy
	return &copy, nil
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

func (e *errorEventRepo) MoveToCalendar(ctx context.Context, fromCalendarID, toCalendarID int64, uid, destResourceName string) error {
	return errors.New("fail")
}

func (e *errorEventRepo) CopyToCalendar(ctx context.Context, fromCalendarID, toCalendarID int64, uid, destResourceName, newETag string) (*store.Event, error) {
	return nil, errors.New("fail")
}

type fakeContactRepo struct {
	contacts                 map[string]*store.Contact
	deleted                  []string
	copyErr                  error
	moveErr                  error
	upsertErr                error
	getByUIDErr              error
	getByUIDErrKey           string
	getByResourceNameErr     error
	getByResourceNameErrKey  string
	resourceLookupCount      int
	overwriteMoveDeletedRepo *fakeDeletedResourceRepo
}

func (f *fakeContactRepo) key(bookID int64, uid string) string {
	return fmt.Sprintf("%d:%s", bookID, uid)
}

func (f *fakeContactRepo) Upsert(ctx context.Context, contact store.Contact) (*store.Contact, error) {
	if f.upsertErr != nil {
		return nil, f.upsertErr
	}
	if f.contacts == nil {
		f.contacts = map[string]*store.Contact{}
	}
	if contact.ResourceName == "" {
		contact.ResourceName = contact.UID
	}
	copy := contact
	f.contacts[f.key(contact.AddressBookID, contact.UID)] = &copy
	return &copy, nil
}

func (f *fakeContactRepo) DeleteByUID(ctx context.Context, addressBookID int64, uid string) error {
	f.deleted = append(f.deleted, f.key(addressBookID, uid))
	delete(f.contacts, f.key(addressBookID, uid))
	return nil
}

func (f *fakeContactRepo) GetByUID(ctx context.Context, addressBookID int64, uid string) (*store.Contact, error) {
	if f.getByUIDErr != nil && (f.getByUIDErrKey == "" || f.getByUIDErrKey == f.key(addressBookID, uid)) {
		return nil, f.getByUIDErr
	}
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
	uidSet := make(map[string]struct{}, len(uids))
	for _, uid := range uids {
		uidSet[uid] = struct{}{}
	}
	var result []store.Contact
	for _, c := range f.contacts {
		if c.AddressBookID != addressBookID {
			continue
		}
		if _, ok := uidSet[c.UID]; ok {
			result = append(result, *c)
		}
	}
	return result, nil
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

func (f *fakeContactRepo) GetByResourceName(ctx context.Context, addressBookID int64, resourceName string) (*store.Contact, error) {
	f.resourceLookupCount++
	if f.getByResourceNameErr != nil && (f.getByResourceNameErrKey == "" || f.getByResourceNameErrKey == f.key(addressBookID, resourceName)) {
		return nil, f.getByResourceNameErr
	}
	for _, c := range f.contacts {
		if c.AddressBookID != addressBookID {
			continue
		}
		name := c.ResourceName
		if name == "" {
			name = c.UID
		}
		if name == resourceName {
			copy := *c
			return &copy, nil
		}
	}
	return nil, nil
}

func (f *fakeContactRepo) CopyToAddressBook(ctx context.Context, fromAddressBookID, toAddressBookID int64, uid, destResourceName, newETag string) (*store.Contact, error) {
	if f.copyErr != nil {
		return nil, f.copyErr
	}
	src, ok := f.contacts[f.key(fromAddressBookID, uid)]
	if !ok {
		return nil, nil
	}
	copy := *src
	copy.AddressBookID = toAddressBookID
	if destResourceName != "" {
		copy.ResourceName = destResourceName
	}
	for key, existing := range f.contacts {
		if existing.AddressBookID != toAddressBookID || existing.UID == copy.UID {
			continue
		}
		name := existing.ResourceName
		if name == "" {
			name = existing.UID
		}
		if name == copy.ResourceName {
			delete(f.contacts, key)
		}
	}
	copy.ETag = newETag
	f.contacts[f.key(toAddressBookID, copy.UID)] = &copy
	return &copy, nil
}

func (f *fakeContactRepo) MoveToAddressBook(ctx context.Context, fromAddressBookID, toAddressBookID int64, uid, destResourceName string) error {
	if f.moveErr != nil {
		return f.moveErr
	}
	oldKey := f.key(fromAddressBookID, uid)
	contact, ok := f.contacts[oldKey]
	if !ok {
		return nil // Contact not found
	}
	// Remove from old location
	delete(f.contacts, oldKey)
	// Update address book ID
	contact.AddressBookID = toAddressBookID
	if destResourceName != "" {
		contact.ResourceName = destResourceName
	}
	for key, existing := range f.contacts {
		if existing.AddressBookID != toAddressBookID || existing.UID == contact.UID {
			continue
		}
		name := existing.ResourceName
		if name == "" {
			name = existing.UID
		}
		if name == contact.ResourceName {
			if f.overwriteMoveDeletedRepo != nil {
				f.overwriteMoveDeletedRepo.deleted = append(f.overwriteMoveDeletedRepo.deleted, store.DeletedResource{
					ResourceType: "contact",
					CollectionID: toAddressBookID,
					UID:          existing.UID,
					ResourceName: name,
					DeletedAt:    time.Now(),
				})
			}
			delete(f.contacts, key)
		}
	}
	// Add to new location
	newKey := f.key(toAddressBookID, uid)
	f.contacts[newKey] = contact
	return nil
}

type fakeCalendarRepo struct {
	accessible       []store.CalendarAccess
	accessibleByUser map[int64][]store.CalendarAccess
	calendars        map[int64]*store.Calendar
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
	if f.accessibleByUser != nil {
		return f.accessibleByUser[userID], nil
	}
	return f.accessible, nil
}

func (f *fakeCalendarRepo) GetAccessible(ctx context.Context, calendarID, userID int64) (*store.CalendarAccess, error) {
	if f.accessibleByUser != nil {
		for _, c := range f.accessibleByUser[userID] {
			if c.ID == calendarID {
				if c.UserID == 0 && !c.Shared {
					c.UserID = userID
				}
				copy := c
				return &copy, nil
			}
		}
		return nil, nil
	}
	for _, c := range f.accessible {
		if c.ID == calendarID {
			if c.UserID == 0 && !c.Shared {
				c.UserID = userID
			}
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
	cal, ok := f.calendars[id]
	if !ok || cal.UserID != userID {
		return store.ErrNotFound
	}
	cal.Name = name
	cal.Description = description
	cal.Timezone = timezone
	return nil
}

func (f *fakeCalendarRepo) UpdateProperties(ctx context.Context, id int64, name string, description, timezone *string) error {
	cal, ok := f.calendars[id]
	if !ok {
		return store.ErrNotFound
	}
	cal.Name = name
	cal.Description = description
	cal.Timezone = timezone
	return nil
}

func (f *fakeCalendarRepo) Rename(ctx context.Context, userID, id int64, name string) error {
	return nil
}

func (f *fakeCalendarRepo) Delete(ctx context.Context, userID, id int64) error {
	cal, ok := f.calendars[id]
	if !ok {
		return store.ErrNotFound
	}
	if cal.UserID != userID {
		return store.ErrNotFound
	}
	delete(f.calendars, id)
	return nil
}

type fakeAddressBookRepo struct {
	books map[int64]*store.AddressBook
}

func (f *fakeAddressBookRepo) hasDuplicateName(userID, excludeID int64, name string) bool {
	for id, book := range f.books {
		if id == excludeID || book.UserID != userID {
			continue
		}
		if strings.EqualFold(book.Name, name) {
			return true
		}
	}
	return false
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
	if f.hasDuplicateName(book.UserID, 0, book.Name) {
		return nil, store.ErrConflict
	}
	book.ID = int64(len(f.books) + 1)
	copy := book
	f.books[copy.ID] = &copy
	return &copy, nil
}

func (f *fakeAddressBookRepo) Update(ctx context.Context, userID, id int64, name string, description *string) error {
	book, ok := f.books[id]
	if !ok {
		return store.ErrNotFound
	}
	if book.UserID != userID {
		return store.ErrNotFound
	}
	if f.hasDuplicateName(userID, id, name) {
		return store.ErrConflict
	}
	book.Name = name
	if description != nil {
		book.Description = description
	}
	return nil
}

func (f *fakeAddressBookRepo) UpdateProperties(ctx context.Context, id int64, name string, description *string) error {
	book, ok := f.books[id]
	if !ok {
		return store.ErrNotFound
	}
	if f.hasDuplicateName(book.UserID, id, name) {
		return store.ErrConflict
	}
	book.Name = name
	if description != nil {
		book.Description = description
	}
	return nil
}

func (f *fakeAddressBookRepo) Rename(ctx context.Context, userID, id int64, name string) error {
	book, ok := f.books[id]
	if !ok {
		return store.ErrNotFound
	}
	if book.UserID != userID {
		return store.ErrNotFound
	}
	if f.hasDuplicateName(userID, id, name) {
		return store.ErrConflict
	}
	book.Name = name
	return nil
}

func (f *fakeAddressBookRepo) Delete(ctx context.Context, userID, id int64) error {
	book, ok := f.books[id]
	if !ok {
		return store.ErrNotFound
	}
	if book.UserID != userID {
		return store.ErrNotFound
	}
	delete(f.books, id)
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

func (f *fakeDeletedResourceRepo) DeleteByIdentity(ctx context.Context, resourceType string, collectionID int64, uid, resourceName string) error {
	filtered := f.deleted[:0]
	for _, d := range f.deleted {
		if d.ResourceType == resourceType && d.CollectionID == collectionID && d.UID == uid && d.ResourceName == resourceName {
			continue
		}
		filtered = append(filtered, d)
	}
	f.deleted = filtered
	return nil
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

func TestGetCalendarObjectUsesCollectionACLFallback(t *testing.T) {
	owner := &store.User{ID: 1, PrimaryEmail: "owner@example.com"}
	delegate := &store.User{ID: 2, PrimaryEmail: "delegate@example.com"}
	calRepo := &fakeCalendarRepo{
		accessibleByUser: map[int64][]store.CalendarAccess{
			owner.ID: {
				{Calendar: store.Calendar{ID: 2, UserID: owner.ID, Name: "Work"}, Editor: true},
			},
		},
		calendars: map[int64]*store.Calendar{
			2: {ID: 2, UserID: owner.ID, Name: "Work"},
		},
	}
	eventRepo := &fakeEventRepo{
		events: map[string]*store.Event{
			"2:event": {
				CalendarID:   2,
				UID:          "event",
				ResourceName: "event",
				RawICAL:      "BEGIN:VCALENDAR\r\nVERSION:2.0\r\nBEGIN:VEVENT\r\nUID:event\r\nEND:VEVENT\r\nEND:VCALENDAR\r\n",
				ETag:         "etag-event",
			},
		},
	}
	h := &Handler{store: &store.Store{
		Calendars: calRepo,
		Events:    eventRepo,
		ACLEntries: &fakeACLRepo{entries: []store.ACLEntry{
			{ResourcePath: "/dav/calendars/2", PrincipalHref: "/dav/principals/2/", IsGrant: true, Privilege: "read"},
		}},
	}}

	req := httptest.NewRequest(http.MethodGet, "/dav/calendars/2/event.ics", nil)
	req = req.WithContext(auth.WithUser(req.Context(), delegate))
	rr := httptest.NewRecorder()

	h.Get(rr, req)

	if rr.Code != http.StatusOK {
		t.Fatalf("expected GET to succeed via collection ACL, got %d: %s", rr.Code, rr.Body.String())
	}
	if !strings.Contains(rr.Body.String(), "UID:event") {
		t.Fatalf("expected event body in GET response, got %s", rr.Body.String())
	}
}

func TestGetCalendarObjectUsesCollectionACLFallbackDespiteUnrelatedObjectACL(t *testing.T) {
	owner := &store.User{ID: 1, PrimaryEmail: "owner@example.com"}
	delegate := &store.User{ID: 2, PrimaryEmail: "delegate@example.com"}
	calRepo := &fakeCalendarRepo{
		accessibleByUser: map[int64][]store.CalendarAccess{
			owner.ID: {
				{Calendar: store.Calendar{ID: 2, UserID: owner.ID, Name: "Work"}, Editor: true},
			},
		},
		calendars: map[int64]*store.Calendar{
			2: {ID: 2, UserID: owner.ID, Name: "Work"},
		},
	}
	eventRepo := &fakeEventRepo{
		events: map[string]*store.Event{
			"2:event": {
				CalendarID:   2,
				UID:          "event",
				ResourceName: "event",
				RawICAL:      "BEGIN:VCALENDAR\r\nVERSION:2.0\r\nBEGIN:VEVENT\r\nUID:event\r\nEND:VEVENT\r\nEND:VCALENDAR\r\n",
				ETag:         "etag-event",
			},
		},
	}
	h := &Handler{store: &store.Store{
		Calendars: calRepo,
		Events:    eventRepo,
		ACLEntries: &fakeACLRepo{entries: []store.ACLEntry{
			{ResourcePath: "/dav/calendars/2", PrincipalHref: "/dav/principals/2/", IsGrant: true, Privilege: "read"},
			{ResourcePath: "/dav/calendars/2/event", PrincipalHref: "/dav/principals/2/", IsGrant: true, Privilege: "read-acl"},
		}},
	}}

	req := httptest.NewRequest(http.MethodGet, "/dav/calendars/2/event.ics", nil)
	req = req.WithContext(auth.WithUser(req.Context(), delegate))
	rr := httptest.NewRecorder()

	h.Get(rr, req)

	if rr.Code != http.StatusOK {
		t.Fatalf("expected GET to succeed via collection ACL fallback, got %d: %s", rr.Code, rr.Body.String())
	}
	if !strings.Contains(rr.Body.String(), "UID:event") {
		t.Fatalf("expected event body in GET response, got %s", rr.Body.String())
	}
}

func TestGetCalendarObjectAllowsObjectReadGrantWithoutCollectionAccess(t *testing.T) {
	owner := &store.User{ID: 1, PrimaryEmail: "owner@example.com"}
	delegate := &store.User{ID: 2, PrimaryEmail: "delegate@example.com"}
	calRepo := &fakeCalendarRepo{
		accessibleByUser: map[int64][]store.CalendarAccess{
			owner.ID: {
				{Calendar: store.Calendar{ID: 2, UserID: owner.ID, Name: "Work"}, Editor: true},
			},
		},
		calendars: map[int64]*store.Calendar{
			2: {ID: 2, UserID: owner.ID, Name: "Work"},
		},
	}
	eventRepo := &fakeEventRepo{
		events: map[string]*store.Event{
			"2:event": {
				CalendarID:   2,
				UID:          "event",
				ResourceName: "event",
				RawICAL:      "BEGIN:VCALENDAR\r\nVERSION:2.0\r\nBEGIN:VEVENT\r\nUID:event\r\nEND:VEVENT\r\nEND:VCALENDAR\r\n",
				ETag:         "etag-event",
			},
		},
	}
	h := &Handler{store: &store.Store{
		Calendars: calRepo,
		Events:    eventRepo,
		ACLEntries: &fakeACLRepo{entries: []store.ACLEntry{
			{ResourcePath: "/dav/calendars/2/event", PrincipalHref: "/dav/principals/2/", IsGrant: true, Privilege: "read"},
		}},
	}}

	req := httptest.NewRequest(http.MethodGet, "/dav/calendars/2/event.ics", nil)
	req = req.WithContext(auth.WithUser(req.Context(), delegate))
	rr := httptest.NewRecorder()

	h.Get(rr, req)

	if rr.Code != http.StatusOK {
		t.Fatalf("expected GET to succeed via object ACL, got %d: %s", rr.Code, rr.Body.String())
	}
}

func TestGetCalendarObjectHonorsExplicitObjectReadDeny(t *testing.T) {
	owner := &store.User{ID: 1, PrimaryEmail: "owner@example.com"}
	delegate := &store.User{ID: 2, PrimaryEmail: "delegate@example.com"}
	calRepo := &fakeCalendarRepo{
		accessibleByUser: map[int64][]store.CalendarAccess{
			delegate.ID: {
				{
					Calendar:   store.Calendar{ID: 2, UserID: owner.ID, Name: "Work"},
					Shared:     true,
					Editor:     false,
					Privileges: store.CalendarPrivileges{Read: true, ReadFreeBusy: true},
				},
			},
		},
		calendars: map[int64]*store.Calendar{
			2: {ID: 2, UserID: owner.ID, Name: "Work"},
		},
	}
	eventRepo := &fakeEventRepo{
		events: map[string]*store.Event{
			"2:event": {
				CalendarID:   2,
				UID:          "event",
				ResourceName: "event",
				RawICAL:      "BEGIN:VCALENDAR\r\nVERSION:2.0\r\nBEGIN:VEVENT\r\nUID:event\r\nEND:VEVENT\r\nEND:VCALENDAR\r\n",
				ETag:         "etag-event",
			},
		},
	}
	h := &Handler{store: &store.Store{
		Calendars: calRepo,
		Events:    eventRepo,
		ACLEntries: &fakeACLRepo{entries: []store.ACLEntry{
			{ResourcePath: "/dav/calendars/2", PrincipalHref: "/dav/principals/2/", IsGrant: true, Privilege: "read"},
			{ResourcePath: "/dav/calendars/2/event", PrincipalHref: "/dav/principals/2/", IsGrant: false, Privilege: "read"},
		}},
	}}

	req := httptest.NewRequest(http.MethodGet, "/dav/calendars/2/event.ics", nil)
	req = req.WithContext(auth.WithUser(req.Context(), delegate))
	rr := httptest.NewRecorder()

	h.Get(rr, req)

	if rr.Code != http.StatusNotFound {
		t.Fatalf("expected GET to hide explicitly denied object, got %d: %s", rr.Code, rr.Body.String())
	}
	if strings.Contains(rr.Body.String(), "UID:event") {
		t.Fatalf("expected denied GET not to expose event data, got %s", rr.Body.String())
	}
}

func TestReportCalendarQueryUsesCollectionACLFallback(t *testing.T) {
	owner := &store.User{ID: 1, PrimaryEmail: "owner@example.com"}
	delegate := &store.User{ID: 2, PrimaryEmail: "delegate@example.com"}
	calRepo := &fakeCalendarRepo{
		accessibleByUser: map[int64][]store.CalendarAccess{
			owner.ID: {
				{Calendar: store.Calendar{ID: 1, UserID: owner.ID, Name: "Work"}, Editor: true},
			},
		},
		calendars: map[int64]*store.Calendar{
			1: {ID: 1, UserID: owner.ID, Name: "Work"},
		},
	}
	eventRepo := &fakeEventRepo{
		events: map[string]*store.Event{
			"1:event1": {CalendarID: 1, UID: "event1", ResourceName: "event1", RawICAL: "BEGIN:VCALENDAR\r\nBEGIN:VEVENT\r\nUID:event1\r\nEND:VEVENT\r\nEND:VCALENDAR\r\n", ETag: "e1"},
		},
	}
	h := &Handler{store: &store.Store{
		Calendars: calRepo,
		Events:    eventRepo,
		ACLEntries: &fakeACLRepo{entries: []store.ACLEntry{
			{ResourcePath: "/dav/calendars/1", PrincipalHref: "/dav/principals/2/", IsGrant: true, Privilege: "read"},
		}},
	}}

	body := `<cal:calendar-query xmlns:cal="urn:ietf:params:xml:ns:caldav"/>`
	req := httptest.NewRequest("REPORT", "/dav/calendars/1/", strings.NewReader(body))
	req = req.WithContext(auth.WithUser(req.Context(), delegate))
	rr := httptest.NewRecorder()

	h.Report(rr, req)

	if rr.Code != http.StatusMultiStatus {
		t.Fatalf("expected REPORT to succeed via collection ACL, got %d: %s", rr.Code, rr.Body.String())
	}
	if !strings.Contains(rr.Body.String(), "event1.ics") {
		t.Fatalf("expected REPORT to include event resource, got %s", rr.Body.String())
	}
}

func TestPutCalendarObjectUsesCollectionWriteFallbackDespiteUnrelatedObjectACL(t *testing.T) {
	owner := &store.User{ID: 1, PrimaryEmail: "owner@example.com"}
	delegate := &store.User{ID: 2, PrimaryEmail: "delegate@example.com"}
	calRepo := &fakeCalendarRepo{
		accessibleByUser: map[int64][]store.CalendarAccess{
			owner.ID: {
				{Calendar: store.Calendar{ID: 2, UserID: owner.ID, Name: "Work"}, Editor: true},
			},
		},
		calendars: map[int64]*store.Calendar{
			2: {ID: 2, UserID: owner.ID, Name: "Work"},
		},
	}
	eventRepo := &fakeEventRepo{
		events: map[string]*store.Event{
			"2:event": {
				CalendarID:   2,
				UID:          "event",
				ResourceName: "event",
				RawICAL:      "BEGIN:VCALENDAR\r\nVERSION:2.0\r\nBEGIN:VEVENT\r\nUID:event\r\nSUMMARY:Original\r\nDTSTART:20240601T100000Z\r\nEND:VEVENT\r\nEND:VCALENDAR\r\n",
				ETag:         "old-etag",
			},
		},
	}
	h := &Handler{store: &store.Store{
		Calendars: calRepo,
		Events:    eventRepo,
		ACLEntries: &fakeACLRepo{entries: []store.ACLEntry{
			{ResourcePath: "/dav/calendars/2", PrincipalHref: "/dav/principals/2/", IsGrant: true, Privilege: "read"},
			{ResourcePath: "/dav/calendars/2", PrincipalHref: "/dav/principals/2/", IsGrant: true, Privilege: "write-content"},
			{ResourcePath: "/dav/calendars/2/event", PrincipalHref: "/dav/principals/2/", IsGrant: true, Privilege: "read-acl"},
		}},
	}}

	body := "BEGIN:VCALENDAR\r\nVERSION:2.0\r\nBEGIN:VEVENT\r\nUID:event\r\nSUMMARY:Updated\r\nDTSTART:20240601T100000Z\r\nEND:VEVENT\r\nEND:VCALENDAR\r\n"
	req := newCalendarPutRequest("/dav/calendars/2/event.ics", strings.NewReader(body))
	req.Header.Set("If-Match", `"old-etag"`)
	req = req.WithContext(auth.WithUser(req.Context(), delegate))
	rr := httptest.NewRecorder()

	h.Put(rr, req)

	if rr.Code != http.StatusNoContent {
		t.Fatalf("expected PUT to succeed via collection ACL fallback, got %d: %s", rr.Code, rr.Body.String())
	}
	updated := eventRepo.events["2:event"]
	if updated == nil || !strings.Contains(updated.RawICAL, "SUMMARY:Updated") {
		t.Fatalf("expected event update to succeed, got %#v", updated)
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

func TestCalendarCollectionACLControlsWriteOperations(t *testing.T) {
	owner := &store.User{ID: 1, PrimaryEmail: "owner@example.com"}
	delegate := &store.User{ID: 2, PrimaryEmail: "delegate@example.com"}
	icalData := "BEGIN:VCALENDAR\r\nVERSION:2.0\r\nBEGIN:VEVENT\r\nUID:event\r\nDTSTART:20240601T100000Z\r\nEND:VEVENT\r\nEND:VCALENDAR\r\n"

	t.Run("read-only grant cannot create event", func(t *testing.T) {
		calRepo := &fakeCalendarRepo{
			accessibleByUser: map[int64][]store.CalendarAccess{
				owner.ID: {
					{Calendar: store.Calendar{ID: 2, UserID: owner.ID, Name: "Work"}, Editor: true},
				},
			},
			calendars: map[int64]*store.Calendar{
				2: {ID: 2, UserID: owner.ID, Name: "Work"},
			},
		}
		eventRepo := &fakeEventRepo{events: map[string]*store.Event{}}
		h := &Handler{store: &store.Store{
			Calendars: calRepo,
			Events:    eventRepo,
			ACLEntries: &fakeACLRepo{entries: []store.ACLEntry{
				{ResourcePath: "/dav/calendars/2", PrincipalHref: "/dav/principals/2/", IsGrant: true, Privilege: "read"},
			}},
		}}

		req := newCalendarPutRequest("/dav/calendars/2/event.ics", strings.NewReader(icalData))
		req = req.WithContext(auth.WithUser(req.Context(), delegate))
		rr := httptest.NewRecorder()

		h.Put(rr, req)

		if rr.Code != http.StatusForbidden {
			t.Fatalf("expected read-only delegate PUT to be forbidden, got %d: %s", rr.Code, rr.Body.String())
		}
	})

	t.Run("bind grant can create event", func(t *testing.T) {
		calRepo := &fakeCalendarRepo{
			accessibleByUser: map[int64][]store.CalendarAccess{
				owner.ID: {
					{Calendar: store.Calendar{ID: 2, UserID: owner.ID, Name: "Work"}, Editor: true},
				},
			},
			calendars: map[int64]*store.Calendar{
				2: {ID: 2, UserID: owner.ID, Name: "Work"},
			},
		}
		eventRepo := &fakeEventRepo{events: map[string]*store.Event{}}
		h := &Handler{store: &store.Store{
			Calendars: calRepo,
			Events:    eventRepo,
			ACLEntries: &fakeACLRepo{entries: []store.ACLEntry{
				{ResourcePath: "/dav/calendars/2", PrincipalHref: "/dav/principals/2/", IsGrant: true, Privilege: "read"},
				{ResourcePath: "/dav/calendars/2", PrincipalHref: "/dav/principals/2/", IsGrant: true, Privilege: "bind"},
			}},
		}}

		req := newCalendarPutRequest("/dav/calendars/2/event.ics", strings.NewReader(icalData))
		req = req.WithContext(auth.WithUser(req.Context(), delegate))
		rr := httptest.NewRecorder()

		h.Put(rr, req)

		if rr.Code != http.StatusCreated {
			t.Fatalf("expected bind grant to allow calendar PUT create, got %d: %s", rr.Code, rr.Body.String())
		}
	})

	t.Run("write-content grant can modify existing event", func(t *testing.T) {
		calRepo := &fakeCalendarRepo{
			accessibleByUser: map[int64][]store.CalendarAccess{
				owner.ID: {
					{Calendar: store.Calendar{ID: 2, UserID: owner.ID, Name: "Work"}, Editor: true},
				},
			},
			calendars: map[int64]*store.Calendar{
				2: {ID: 2, UserID: owner.ID, Name: "Work"},
			},
		}
		eventRepo := &fakeEventRepo{
			events: map[string]*store.Event{
				"2:event": {CalendarID: 2, UID: "event", ResourceName: "event", RawICAL: "OLD", ETag: "old-etag"},
			},
		}
		h := &Handler{store: &store.Store{
			Calendars: calRepo,
			Events:    eventRepo,
			ACLEntries: &fakeACLRepo{entries: []store.ACLEntry{
				{ResourcePath: "/dav/calendars/2", PrincipalHref: "/dav/principals/2/", IsGrant: true, Privilege: "read"},
				{ResourcePath: "/dav/calendars/2", PrincipalHref: "/dav/principals/2/", IsGrant: true, Privilege: "write-content"},
			}},
		}}

		req := newCalendarPutRequest("/dav/calendars/2/event.ics", strings.NewReader(icalData))
		req.Header.Set("If-Match", `"old-etag"`)
		req = req.WithContext(auth.WithUser(req.Context(), delegate))
		rr := httptest.NewRecorder()

		h.Put(rr, req)

		if rr.Code != http.StatusNoContent {
			t.Fatalf("expected write-content grant to allow calendar PUT update, got %d: %s", rr.Code, rr.Body.String())
		}
	})

	t.Run("object-level write-content grant can modify existing event without collection access", func(t *testing.T) {
		calRepo := &fakeCalendarRepo{
			accessibleByUser: map[int64][]store.CalendarAccess{
				owner.ID: {
					{Calendar: store.Calendar{ID: 2, UserID: owner.ID, Name: "Work"}, Editor: true},
				},
			},
			calendars: map[int64]*store.Calendar{
				2: {ID: 2, UserID: owner.ID, Name: "Work"},
			},
		}
		eventRepo := &fakeEventRepo{
			events: map[string]*store.Event{
				"2:event": {CalendarID: 2, UID: "event", ResourceName: "event", RawICAL: "OLD", ETag: "old-etag"},
			},
		}
		h := &Handler{store: &store.Store{
			Calendars: calRepo,
			Events:    eventRepo,
			ACLEntries: &fakeACLRepo{entries: []store.ACLEntry{
				{ResourcePath: "/dav/calendars/2/event", PrincipalHref: "/dav/principals/2/", IsGrant: true, Privilege: "write-content"},
			}},
		}}

		req := newCalendarPutRequest("/dav/calendars/2/event.ics", strings.NewReader(icalData))
		req.Header.Set("If-Match", `"old-etag"`)
		req = req.WithContext(auth.WithUser(req.Context(), delegate))
		rr := httptest.NewRecorder()

		h.Put(rr, req)

		if rr.Code != http.StatusNoContent {
			t.Fatalf("expected object-level write-content grant to allow calendar PUT update, got %d: %s", rr.Code, rr.Body.String())
		}
	})

	t.Run("object-level write-content deny blocks existing event update despite collection grant", func(t *testing.T) {
		calRepo := &fakeCalendarRepo{
			accessibleByUser: map[int64][]store.CalendarAccess{
				delegate.ID: {
					{
						Calendar:   store.Calendar{ID: 2, UserID: owner.ID, Name: "Work"},
						Shared:     true,
						Editor:     true,
						Privileges: store.CalendarPrivileges{Read: true, WriteContent: true},
					},
				},
			},
			calendars: map[int64]*store.Calendar{
				2: {ID: 2, UserID: owner.ID, Name: "Work"},
			},
		}
		eventRepo := &fakeEventRepo{
			events: map[string]*store.Event{
				"2:event": {CalendarID: 2, UID: "event", ResourceName: "event", RawICAL: "OLD", ETag: "old-etag"},
			},
		}
		h := &Handler{store: &store.Store{
			Calendars: calRepo,
			Events:    eventRepo,
			ACLEntries: &fakeACLRepo{entries: []store.ACLEntry{
				{ResourcePath: "/dav/calendars/2", PrincipalHref: "/dav/principals/2/", IsGrant: true, Privilege: "read"},
				{ResourcePath: "/dav/calendars/2", PrincipalHref: "/dav/principals/2/", IsGrant: true, Privilege: "write-content"},
				{ResourcePath: "/dav/calendars/2/event", PrincipalHref: "/dav/principals/2/", IsGrant: false, Privilege: "write-content"},
			}},
		}}

		req := newCalendarPutRequest("/dav/calendars/2/event.ics", strings.NewReader(icalData))
		req.Header.Set("If-Match", `"old-etag"`)
		req = req.WithContext(auth.WithUser(req.Context(), delegate))
		rr := httptest.NewRecorder()

		h.Put(rr, req)

		if rr.Code != http.StatusForbidden {
			t.Fatalf("expected object-level deny to block calendar PUT update, got %d: %s", rr.Code, rr.Body.String())
		}
	})

	t.Run("unbind grant can delete existing event", func(t *testing.T) {
		calRepo := &fakeCalendarRepo{
			accessibleByUser: map[int64][]store.CalendarAccess{
				owner.ID: {
					{Calendar: store.Calendar{ID: 2, UserID: owner.ID, Name: "Work"}, Editor: true},
				},
			},
			calendars: map[int64]*store.Calendar{
				2: {ID: 2, UserID: owner.ID, Name: "Work"},
			},
		}
		eventRepo := &fakeEventRepo{
			events: map[string]*store.Event{
				"2:event": {CalendarID: 2, UID: "event", ResourceName: "event", RawICAL: "ICAL", ETag: "current"},
			},
		}
		h := &Handler{store: &store.Store{
			Calendars: calRepo,
			Events:    eventRepo,
			ACLEntries: &fakeACLRepo{entries: []store.ACLEntry{
				{ResourcePath: "/dav/calendars/2", PrincipalHref: "/dav/principals/2/", IsGrant: true, Privilege: "read"},
				{ResourcePath: "/dav/calendars/2", PrincipalHref: "/dav/principals/2/", IsGrant: true, Privilege: "unbind"},
			}},
		}}

		req := httptest.NewRequest(http.MethodDelete, "/dav/calendars/2/event.ics", nil)
		req.Header.Set("If-Match", `"current"`)
		req = req.WithContext(auth.WithUser(req.Context(), delegate))
		rr := httptest.NewRecorder()

		h.Delete(rr, req)

		if rr.Code != http.StatusNoContent {
			t.Fatalf("expected unbind grant to allow calendar DELETE, got %d: %s", rr.Code, rr.Body.String())
		}
	})

	t.Run("object-level unbind grant can delete existing event without collection access", func(t *testing.T) {
		calRepo := &fakeCalendarRepo{
			accessibleByUser: map[int64][]store.CalendarAccess{
				owner.ID: {
					{Calendar: store.Calendar{ID: 2, UserID: owner.ID, Name: "Work"}, Editor: true},
				},
			},
			calendars: map[int64]*store.Calendar{
				2: {ID: 2, UserID: owner.ID, Name: "Work"},
			},
		}
		eventRepo := &fakeEventRepo{
			events: map[string]*store.Event{
				"2:event": {CalendarID: 2, UID: "event", ResourceName: "event", RawICAL: "ICAL", ETag: "current"},
			},
		}
		h := &Handler{store: &store.Store{
			Calendars: calRepo,
			Events:    eventRepo,
			ACLEntries: &fakeACLRepo{entries: []store.ACLEntry{
				{ResourcePath: "/dav/calendars/2/event", PrincipalHref: "/dav/principals/2/", IsGrant: true, Privilege: "unbind"},
			}},
		}}

		req := httptest.NewRequest(http.MethodDelete, "/dav/calendars/2/event.ics", nil)
		req.Header.Set("If-Match", `"current"`)
		req = req.WithContext(auth.WithUser(req.Context(), delegate))
		rr := httptest.NewRecorder()

		h.Delete(rr, req)

		if rr.Code != http.StatusNoContent {
			t.Fatalf("expected object-level unbind grant to allow calendar DELETE, got %d: %s", rr.Code, rr.Body.String())
		}
	})

	t.Run("object-level unbind deny blocks existing event delete despite collection grant", func(t *testing.T) {
		calRepo := &fakeCalendarRepo{
			accessibleByUser: map[int64][]store.CalendarAccess{
				delegate.ID: {
					{
						Calendar:   store.Calendar{ID: 2, UserID: owner.ID, Name: "Work"},
						Shared:     true,
						Editor:     true,
						Privileges: store.CalendarPrivileges{Read: true, Unbind: true},
					},
				},
			},
			calendars: map[int64]*store.Calendar{
				2: {ID: 2, UserID: owner.ID, Name: "Work"},
			},
		}
		eventRepo := &fakeEventRepo{
			events: map[string]*store.Event{
				"2:event": {CalendarID: 2, UID: "event", ResourceName: "event", RawICAL: "ICAL", ETag: "current"},
			},
		}
		h := &Handler{store: &store.Store{
			Calendars: calRepo,
			Events:    eventRepo,
			ACLEntries: &fakeACLRepo{entries: []store.ACLEntry{
				{ResourcePath: "/dav/calendars/2", PrincipalHref: "/dav/principals/2/", IsGrant: true, Privilege: "read"},
				{ResourcePath: "/dav/calendars/2", PrincipalHref: "/dav/principals/2/", IsGrant: true, Privilege: "unbind"},
				{ResourcePath: "/dav/calendars/2/event", PrincipalHref: "/dav/principals/2/", IsGrant: false, Privilege: "unbind"},
			}},
		}}

		req := httptest.NewRequest(http.MethodDelete, "/dav/calendars/2/event.ics", nil)
		req.Header.Set("If-Match", `"current"`)
		req = req.WithContext(auth.WithUser(req.Context(), delegate))
		rr := httptest.NewRecorder()

		h.Delete(rr, req)

		if rr.Code != http.StatusForbidden {
			t.Fatalf("expected object-level deny to block calendar DELETE, got %d: %s", rr.Code, rr.Body.String())
		}
	})
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
	if !strings.Contains(rr.Body.String(), "valid-address-data") {
		t.Errorf("expected CardDAV valid-address-data precondition error, got %s", rr.Body.String())
	}
}

func TestPutAcceptsValidVCard(t *testing.T) {
	bookRepo := &fakeAddressBookRepo{
		books: map[int64]*store.AddressBook{
			5: {ID: 5, UserID: 1, Name: "Contacts"},
		},
	}
	h := &Handler{store: &store.Store{AddressBooks: bookRepo, Contacts: &fakeContactRepo{}}}

	validData := "BEGIN:VCARD\r\nVERSION:3.0\r\nUID:alice\r\nFN:Alice\r\nEND:VCARD\r\n"
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

func TestProppatchAddressBookRejectsDuplicateDisplayName(t *testing.T) {
	bookRepo := &fakeAddressBookRepo{
		books: map[int64]*store.AddressBook{
			5: {ID: 5, UserID: 1, Name: "Contacts"},
			6: {ID: 6, UserID: 1, Name: "Work"},
		},
	}
	h := &Handler{store: &store.Store{AddressBooks: bookRepo}}
	u := &store.User{ID: 1}

	body := `<?xml version="1.0" encoding="utf-8" ?>
<D:propertyupdate xmlns:D="DAV:" xmlns:C="urn:ietf:params:xml:ns:carddav">
  <D:set>
    <D:prop>
      <D:displayname>contacts</D:displayname>
    </D:prop>
  </D:set>
</D:propertyupdate>`

	req := httptest.NewRequest("PROPPATCH", "/dav/addressbooks/6", strings.NewReader(body))
	req = req.WithContext(auth.WithUser(req.Context(), u))
	rr := httptest.NewRecorder()

	h.Proppatch(rr, req)

	if rr.Code != http.StatusMultiStatus {
		t.Fatalf("expected 207, got %d: %s", rr.Code, rr.Body.String())
	}
	if !strings.Contains(rr.Body.String(), "409 Conflict") {
		t.Fatalf("expected duplicate displayname PROPPATCH to return 409 in propstat, got %s", rr.Body.String())
	}
}

func TestProppatchAddressBookProtectedPropertyIsAtomic(t *testing.T) {
	bookRepo := &fakeAddressBookRepo{
		books: map[int64]*store.AddressBook{
			5: {ID: 5, UserID: 1, Name: "Contacts"},
		},
	}
	h := &Handler{store: &store.Store{AddressBooks: bookRepo}}
	u := &store.User{ID: 1}

	body := `<?xml version="1.0" encoding="utf-8" ?>
<D:propertyupdate xmlns:D="DAV:" xmlns:C="urn:ietf:params:xml:ns:carddav">
  <D:set>
    <D:prop>
      <D:displayname>Renamed Contacts</D:displayname>
      <C:supported-address-data>
        <C:address-data-type content-type="text/vcard" version="4.0"/>
      </C:supported-address-data>
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
	if !strings.Contains(rr.Body.String(), "403 Forbidden") {
		t.Fatalf("expected protected property failure in response, got %s", rr.Body.String())
	}
	if strings.Contains(rr.Body.String(), "200 OK") {
		t.Fatalf("expected atomic failure with no successful propstat, got %s", rr.Body.String())
	}
	if got := bookRepo.books[5].Name; got != "Contacts" {
		t.Fatalf("expected PROPPATCH to leave address book unchanged, got %q", got)
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
		calendars: map[int64]*store.Calendar{
			2: {ID: 2, UserID: 2, Name: "Not Mine"},
		},
	}
	aclRepo := &fakeACLRepo{entries: []store.ACLEntry{
		{ResourcePath: "/dav/calendars/2", PrincipalHref: "/dav/principals/1/", IsGrant: true, Privilege: "read"},
	}}
	h := &Handler{store: &store.Store{Calendars: calRepo, ACLEntries: aclRepo}}
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

func TestProppatchSharedCalendarWritePropertiesGrantPersistsChange(t *testing.T) {
	owner := &store.User{ID: 1}
	delegate := &store.User{ID: 2}
	description := "Before"
	calRepo := &fakeCalendarRepo{
		calendars: map[int64]*store.Calendar{
			5: {ID: 5, UserID: owner.ID, Name: "Shared", Description: &description},
		},
	}
	aclRepo := &fakeACLRepo{entries: []store.ACLEntry{
		{ResourcePath: "/dav/calendars/5", PrincipalHref: "/dav/principals/2/", IsGrant: true, Privilege: "write-properties"},
	}}
	h := &Handler{store: &store.Store{Calendars: calRepo, ACLEntries: aclRepo}}

	body := `<?xml version="1.0" encoding="utf-8" ?>
<D:propertyupdate xmlns:D="DAV:" xmlns:C="urn:ietf:params:xml:ns:caldav">
  <D:set>
    <D:prop>
      <D:displayname>Renamed Shared</D:displayname>
      <C:calendar-description>Updated by delegate</C:calendar-description>
    </D:prop>
  </D:set>
</D:propertyupdate>`

	req := httptest.NewRequest("PROPPATCH", "/dav/calendars/5", strings.NewReader(body))
	req = req.WithContext(auth.WithUser(req.Context(), delegate))
	rr := httptest.NewRecorder()

	h.Proppatch(rr, req)

	if rr.Code != http.StatusMultiStatus {
		t.Fatalf("expected 207, got %d: %s", rr.Code, rr.Body.String())
	}
	updated, err := calRepo.GetByID(context.Background(), 5)
	if err != nil {
		t.Fatalf("GetByID() error = %v", err)
	}
	if updated == nil {
		t.Fatal("expected updated calendar")
	}
	if updated.Name != "Renamed Shared" {
		t.Fatalf("expected shared PROPPATCH to persist display name, got %#v", updated)
	}
	if updated.Description == nil || *updated.Description != "Updated by delegate" {
		t.Fatalf("expected shared PROPPATCH to persist description, got %#v", updated)
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

func TestFreeBusyQuerySkipsDeniedCalendarObjects(t *testing.T) {
	visibleStart := time.Date(2024, 6, 1, 10, 0, 0, 0, time.UTC)
	visibleEnd := time.Date(2024, 6, 1, 11, 0, 0, 0, time.UTC)
	hiddenStart := time.Date(2024, 6, 2, 12, 0, 0, 0, time.UTC)
	hiddenEnd := time.Date(2024, 6, 2, 13, 0, 0, 0, time.UTC)

	calRepo := &fakeCalendarRepo{
		calendars: map[int64]*store.Calendar{
			1: {ID: 1, UserID: 1, Name: "Shared"},
		},
	}
	eventRepo := &fakeEventRepo{
		events: map[string]*store.Event{
			"1:visible": {CalendarID: 1, UID: "visible", ResourceName: "visible", RawICAL: "BEGIN:VCALENDAR\r\nBEGIN:VEVENT\r\nUID:visible\r\nDTSTART:20240601T100000Z\r\nDTEND:20240601T110000Z\r\nEND:VEVENT\r\nEND:VCALENDAR\r\n", ETag: "e1", DTStart: &visibleStart, DTEnd: &visibleEnd},
			"1:hidden":  {CalendarID: 1, UID: "hidden", ResourceName: "hidden", RawICAL: "BEGIN:VCALENDAR\r\nBEGIN:VEVENT\r\nUID:hidden\r\nDTSTART:20240602T120000Z\r\nDTEND:20240602T130000Z\r\nEND:VEVENT\r\nEND:VCALENDAR\r\n", ETag: "e2", DTStart: &hiddenStart, DTEnd: &hiddenEnd},
		},
	}
	aclRepo := &fakeACLRepo{entries: []store.ACLEntry{
		{ResourcePath: "/dav/calendars/1", PrincipalHref: "/dav/principals/2/", IsGrant: true, Privilege: "read-free-busy"},
		{ResourcePath: "/dav/calendars/1/hidden", PrincipalHref: "/dav/principals/2/", IsGrant: false, Privilege: "read-free-busy"},
	}}
	h := &Handler{store: &store.Store{Calendars: calRepo, Events: eventRepo, ACLEntries: aclRepo}}

	body := `<cal:free-busy-query xmlns:cal="urn:ietf:params:xml:ns:caldav">
		<cal:filter>
			<cal:comp-filter name="VEVENT">
				<cal:time-range start="20240601T000000Z" end="20240630T235959Z"/>
			</cal:comp-filter>
		</cal:filter>
	</cal:free-busy-query>`

	req := httptest.NewRequest("REPORT", "/dav/calendars/1/", strings.NewReader(body))
	req = req.WithContext(auth.WithUser(req.Context(), &store.User{ID: 2}))
	rr := httptest.NewRecorder()

	h.Report(rr, req)

	if rr.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", rr.Code, rr.Body.String())
	}
	respBody := rr.Body.String()
	if !strings.Contains(respBody, "FREEBUSY:20240601T100000Z/20240601T110000Z") {
		t.Fatalf("expected visible busy slot, got %s", respBody)
	}
	if strings.Contains(respBody, "FREEBUSY:20240602T120000Z/20240602T130000Z") {
		t.Fatalf("expected denied busy slot to be omitted, got %s", respBody)
	}
}

func TestFreeBusyQueryRejectsReadOnlyCalendarWhenReadFreeBusyIsExplicitlyDenied(t *testing.T) {
	delegate := &store.User{ID: 2, PrimaryEmail: "delegate@example.com"}
	start := time.Date(2024, 6, 1, 10, 0, 0, 0, time.UTC)
	end := time.Date(2024, 6, 1, 11, 0, 0, 0, time.UTC)

	calRepo := &fakeCalendarRepo{
		accessibleByUser: map[int64][]store.CalendarAccess{
			delegate.ID: {
				{
					Calendar:   store.Calendar{ID: 1, UserID: 1, Name: "Shared"},
					Shared:     true,
					Editor:     false,
					Privileges: store.CalendarPrivileges{Read: true},
				},
			},
		},
		calendars: map[int64]*store.Calendar{
			1: {ID: 1, UserID: 1, Name: "Shared"},
		},
	}
	eventRepo := &fakeEventRepo{
		events: map[string]*store.Event{
			"1:event": {
				CalendarID:   1,
				UID:          "event",
				ResourceName: "event",
				RawICAL:      "BEGIN:VCALENDAR\r\nBEGIN:VEVENT\r\nUID:event\r\nDTSTART:20240601T100000Z\r\nDTEND:20240601T110000Z\r\nEND:VEVENT\r\nEND:VCALENDAR\r\n",
				ETag:         "e1",
				DTStart:      &start,
				DTEnd:        &end,
			},
		},
	}
	aclRepo := &fakeACLRepo{entries: []store.ACLEntry{
		{ResourcePath: "/dav/calendars/1", PrincipalHref: "/dav/principals/2/", IsGrant: true, Privilege: "read"},
		{ResourcePath: "/dav/calendars/1", PrincipalHref: "/dav/principals/2/", IsGrant: false, Privilege: "read-free-busy"},
	}}
	h := &Handler{store: &store.Store{Calendars: calRepo, Events: eventRepo, ACLEntries: aclRepo}}

	body := `<cal:free-busy-query xmlns:cal="urn:ietf:params:xml:ns:caldav">
		<cal:filter>
			<cal:comp-filter name="VEVENT">
				<cal:time-range start="20240601T000000Z" end="20240630T235959Z"/>
			</cal:comp-filter>
		</cal:filter>
	</cal:free-busy-query>`

	req := httptest.NewRequest("REPORT", "/dav/calendars/1/", strings.NewReader(body))
	req = req.WithContext(auth.WithUser(req.Context(), delegate))
	rr := httptest.NewRecorder()

	h.Report(rr, req)

	if rr.Code != http.StatusForbidden {
		t.Fatalf("expected explicit read-free-busy deny to block REPORT, got %d: %s", rr.Code, rr.Body.String())
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
			if tc.reportType == "addressbook-query" {
				body = `<card:addressbook-query xmlns:card="urn:ietf:params:xml:ns:carddav" xmlns:D="DAV:"><card:filter/></card:addressbook-query>`
			}
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
