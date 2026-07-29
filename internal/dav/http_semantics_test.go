package dav

// Generic HTTP semantics for DAV resources: entity metadata, conditional
// requests, and DELETE. ETag syntax is RFC 9110 §8.8.3 (formerly RFC 2616/7232),
// Last-Modified is RFC 9110 §8.8.2, If-Match and If-None-Match are RFC 9110
// §13.1.1 and §13.1.2, and DELETE is RFC 9110 §9.3.5 as WebDAV §9.6 applies it.
//
// None of this is RFC 4791. §5.3.4 requires a strong ETag on calendar object
// resources and says nothing about Last-Modified; CalCard serves it because it
// tracks a modification time for every resource and RFC 9110 §8.8.2 recommends
// sending one when it is known. RFC 4791 has no DELETE section at all — §5.3.3
// is "Non-Standard Components, Properties, and Parameters" — and it defines no
// conditional-request behavior of its own, so the cases below are HTTP and
// CalCard policy tests rather than CalDAV conformance tests.

import (
	"mime"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/jw6ventures/calcard/internal/auth"
	"github.com/jw6ventures/calcard/internal/store"
)

// headerTokens splits a comma-separated header field value into its members,
// which is what RFC 9110 §5.6.1 makes a list-based field. Substring matching
// over the raw value is not the same test in either direction: "REPORT" is
// carried by "XREPORT" and "calendar-access" by "not-calendar-access", so a
// server advertising neither would satisfy a Contains check.
func headerTokens(value string) []string {
	var tokens []string
	for _, token := range strings.Split(value, ",") {
		if token = strings.TrimSpace(token); token != "" {
			tokens = append(tokens, token)
		}
	}
	return tokens
}

// assertHeaderToken asserts a list-based header carries want as a whole member.
// The comparison is exact: HTTP method names are case-sensitive (RFC 9110 §9)
// and the DAV compliance classes of RFC 4918 §10.1 are spelled by the
// specification that defines them.
func assertHeaderToken(t *testing.T, rr *httptest.ResponseRecorder, header, want, why string) {
	t.Helper()
	value := rr.Header().Get(header)
	for _, token := range headerTokens(value) {
		if token == want {
			return
		}
	}
	t.Errorf("%s header = %q, want it to carry the %q token: %s", header, value, want, why)
}

// assertMediaType asserts the response's Content-Type names wantType. RFC 9110
// §8.3 admits parameters on any media type, so "text/calendar; charset=utf-8"
// is the same media type as "text/calendar" and an exact string comparison
// rejects a legal spelling; matching the prefix as a substring is the opposite
// error, since "text/calendar-x" carries it too.
func assertMediaType(t *testing.T, rr *httptest.ResponseRecorder, wantType string) {
	t.Helper()
	value := rr.Header().Get("Content-Type")
	mediaType, _, err := mime.ParseMediaType(value)
	if err != nil {
		t.Fatalf("Content-Type %q is not an RFC 9110 §8.3 media type: %v", value, err)
	}
	if mediaType != wantType {
		t.Errorf("Content-Type media type = %q, want %q (full header %q)", mediaType, wantType, value)
	}
}

// assertStrongETag asserts value is an RFC 9110 §8.8.3 entity-tag in its strong
// form: a quoted-string carrying no "W/" prefix. why names the requirement the
// caller is holding the server to, since the ETag rules differ between the
// method and the resource being asserted about.
func assertStrongETag(t *testing.T, value, why string) {
	t.Helper()
	if value == "" {
		t.Fatalf("no ETag header: %s", why)
	}
	if strings.HasPrefix(value, "W/") {
		t.Errorf("ETag = %s, want a strong validator: %s", value, why)
	}
	if len(value) < 2 || !strings.HasPrefix(value, `"`) || !strings.HasSuffix(value, `"`) {
		t.Errorf("ETag = %s, want an RFC 9110 §8.8.3 quoted-string", value)
	}
}

func calendarObjectServer(t *testing.T, ev *store.Event) (*DavServer, *store.User) {
	t.Helper()
	calRepo := &fakeCalendarRepo{
		accessible: []store.CalendarAccess{
			{Calendar: store.Calendar{ID: 1, UserID: 1, Name: "Test"}, Editor: true},
		},
	}
	eventRepo := &fakeEventRepo{events: map[string]*store.Event{"1:event": ev}}
	return &DavServer{store: &store.Store{Calendars: calRepo, Events: eventRepo}}, &store.User{ID: 1}
}

// RFC 4791 §5.3.4 requires an ETag on calendar object resources; RFC 9110
// §8.8.3 fixes its syntax as a quoted string, optionally weak-prefixed. CalCard
// stores whole octets, so the validator must be strong.
func TestHTTP_CalendarObjectETagIsStrongAndQuoted(t *testing.T) {
	h, user := calendarObjectServer(t, &store.Event{
		CalendarID: 1,
		UID:        "event",
		RawICAL:    "BEGIN:VCALENDAR\r\nBEGIN:VEVENT\r\nUID:event\r\nEND:VEVENT\r\nEND:VCALENDAR\r\n",
		ETag:       "abc123",
	})

	req := httptest.NewRequest(http.MethodGet, "/dav/calendars/1/event.ics", nil)
	req = req.WithContext(auth.WithUser(req.Context(), user))
	rr := httptest.NewRecorder()

	h.Get(rr, req)

	assertStrongETag(t, rr.Header().Get("ETag"),
		"RFC 4791 §5.3.4 defines a strong ETag on every calendar object resource, and CalCard stores the submitted octets verbatim")
}

func TestHTTP_CalendarObjectReturnsLastModified(t *testing.T) {
	lastMod := time.Date(2024, 6, 1, 12, 0, 0, 0, time.UTC)
	h, user := calendarObjectServer(t, &store.Event{
		CalendarID:   1,
		UID:          "event",
		RawICAL:      "BEGIN:VCALENDAR\r\nEND:VCALENDAR\r\n",
		ETag:         "e",
		LastModified: lastMod,
	})

	req := httptest.NewRequest(http.MethodGet, "/dav/calendars/1/event.ics", nil)
	req = req.WithContext(auth.WithUser(req.Context(), user))
	rr := httptest.NewRecorder()

	h.Get(rr, req)

	header := rr.Header().Get("Last-Modified")
	if header == "" {
		t.Fatal("GET of a calendar object resource returns no Last-Modified; CalCard knows the modification time and RFC 9110 §8.8.2 says to send it")
	}
	got, err := http.ParseTime(header)
	if err != nil {
		t.Fatalf("Last-Modified %q is not an HTTP-date: %v", header, err)
	}
	if !got.Equal(lastMod) {
		t.Errorf("Last-Modified = %s, want %s", got.UTC(), lastMod)
	}
}

// conditionalCalendarServer builds a calendar collection holding the given
// events, keyed as the fake repository keys them.
func conditionalCalendarServer(events map[string]*store.Event) (*DavServer, *store.User) {
	calRepo := &fakeCalendarRepo{
		accessible: []store.CalendarAccess{
			{Calendar: store.Calendar{ID: 1, UserID: 1, Name: "Test"}, Editor: true},
		},
	}
	return &DavServer{store: &store.Store{Calendars: calRepo, Events: &fakeEventRepo{events: events}}}, &store.User{ID: 1}
}

// RFC 9110 §13.1.1: an If-Match that no current representation satisfies fails
// the request with 412, and the resource is left alone.
func TestHTTP_PutWithMismatchedIfMatchFails(t *testing.T) {
	events := map[string]*store.Event{
		"1:existing": {CalendarID: 1, UID: "existing", RawICAL: "OLD", ETag: "correct-etag"},
	}
	h, user := conditionalCalendarServer(events)

	icalData := buildCalendarObject(buildVEvent("existing"))
	req := newCalendarPutRequest("/dav/calendars/1/existing.ics", strings.NewReader(icalData))
	req.Header.Set("If-Match", `"wrong-etag"`)
	req = req.WithContext(auth.WithUser(req.Context(), user))
	rr := httptest.NewRecorder()

	h.Put(rr, req)

	if rr.Code != http.StatusPreconditionFailed {
		t.Errorf("PUT with a mismatched If-Match = %d, want 412 Precondition Failed (RFC 9110 §13.1.1)", rr.Code)
	}
	if stored := events["1:existing"]; stored == nil || stored.RawICAL != "OLD" {
		t.Errorf("a failed If-Match still rewrote the resource: %+v", stored)
	}
}

// RFC 9110 §13.1.1 with a matching validator, plus RFC 4791 §5.3.4: the stored
// octets changed, so the strong ETag must change with them.
func TestHTTP_PutWithMatchingIfMatchSucceeds(t *testing.T) {
	h, user := conditionalCalendarServer(map[string]*store.Event{
		"1:existing": {CalendarID: 1, UID: "existing", RawICAL: "OLD", ETag: "old-etag"},
	})

	icalData := buildCalendarObject(buildVEvent("existing", "SUMMARY:Updated"))
	req := newCalendarPutRequest("/dav/calendars/1/existing.ics", strings.NewReader(icalData))
	req.Header.Set("If-Match", `"old-etag"`)
	req = req.WithContext(auth.WithUser(req.Context(), user))
	rr := httptest.NewRecorder()

	h.Put(rr, req)

	if rr.Code != http.StatusNoContent {
		t.Fatalf("PUT with a matching If-Match = %d, want 204 No Content; body: %s", rr.Code, rr.Body.String())
	}
	etag := rr.Header().Get("ETag")
	if etag == "" {
		t.Fatal("PUT returned no ETag header (RFC 4791 §5.3.4)")
	}
	if strings.Trim(etag, `"`) == "old-etag" {
		t.Error("ETag is unchanged after a successful update; RFC 4791 §5.3.4 makes it a strong validator of the stored octets")
	}
}

// RFC 9110 §13.1.1: If-Match on a resource with no current representation
// cannot be satisfied, "*" included.
func TestHTTP_PutWithIfMatchOnMissingResourceFails(t *testing.T) {
	h, user := conditionalCalendarServer(make(map[string]*store.Event))

	icalData := buildCalendarObject(buildVEvent("missing"))
	req := newCalendarPutRequest("/dav/calendars/1/missing.ics", strings.NewReader(icalData))
	req.Header.Set("If-Match", `"missing-etag"`)
	req = req.WithContext(auth.WithUser(req.Context(), user))
	rr := httptest.NewRecorder()

	h.Put(rr, req)

	if rr.Code != http.StatusPreconditionFailed {
		t.Errorf("PUT with If-Match on a missing resource = %d, want 412 Precondition Failed (RFC 9110 §13.1.1)", rr.Code)
	}
}

// RFC 9110 §13.1.2: "If-None-Match: *" is the create-if-absent request, so it
// succeeds exactly when the target has no current representation.
func TestHTTP_PutWithIfNoneMatchStarCreatesNew(t *testing.T) {
	h, user := conditionalCalendarServer(make(map[string]*store.Event))

	icalData := buildCalendarObject(buildVEvent("new-event"))
	req := newCalendarPutRequest("/dav/calendars/1/new-event.ics", strings.NewReader(icalData))
	req.Header.Set("If-None-Match", "*")
	req = req.WithContext(auth.WithUser(req.Context(), user))
	rr := httptest.NewRecorder()

	h.Put(rr, req)

	if rr.Code != http.StatusCreated {
		t.Errorf("PUT with If-None-Match: * to an unmapped URI = %d, want 201 Created (RFC 9110 §13.1.2)", rr.Code)
	}
}

func TestHTTP_PutWithIfNoneMatchStarOnExistingResourceFails(t *testing.T) {
	events := map[string]*store.Event{
		"1:existing": {CalendarID: 1, UID: "existing", RawICAL: "OLD", ETag: "etag1"},
	}
	h, user := conditionalCalendarServer(events)

	icalData := buildCalendarObject(buildVEvent("existing"))
	req := newCalendarPutRequest("/dav/calendars/1/existing.ics", strings.NewReader(icalData))
	req.Header.Set("If-None-Match", "*")
	req = req.WithContext(auth.WithUser(req.Context(), user))
	rr := httptest.NewRecorder()

	h.Put(rr, req)

	if rr.Code != http.StatusPreconditionFailed {
		t.Errorf("PUT with If-None-Match: * on an existing resource = %d, want 412 Precondition Failed (RFC 9110 §13.1.2)", rr.Code)
	}
	if stored := events["1:existing"]; stored == nil || stored.RawICAL != "OLD" {
		t.Errorf("a failed If-None-Match still rewrote the resource: %+v", stored)
	}
}

// RFC 4918 §9.6 applies RFC 9110 §9.3.5 DELETE to a DAV resource: the mapping
// is removed, so a subsequent GET finds nothing.
func TestHTTP_DeleteRemovesCalendarObject(t *testing.T) {
	events := map[string]*store.Event{
		"1:to-delete": {CalendarID: 1, UID: "to-delete", RawICAL: "ICAL", ETag: "etag1"},
	}
	h, user := conditionalCalendarServer(events)

	req := httptest.NewRequest(http.MethodDelete, "/dav/calendars/1/to-delete.ics", nil)
	req = req.WithContext(auth.WithUser(req.Context(), user))
	rr := httptest.NewRecorder()

	h.Delete(rr, req)

	if rr.Code != http.StatusNoContent {
		t.Errorf("DELETE = %d, want the 204 No Content CalCard returns when no body accompanies the response", rr.Code)
	}
	if _, exists := events["1:to-delete"]; exists {
		t.Error("DELETE reported success but the resource is still stored")
	}

	getReq := httptest.NewRequest(http.MethodGet, "/dav/calendars/1/to-delete.ics", nil)
	getReq = getReq.WithContext(auth.WithUser(getReq.Context(), user))
	getRR := httptest.NewRecorder()
	h.Get(getRR, getReq)
	if getRR.Code != http.StatusNotFound {
		t.Errorf("GET after DELETE = %d, want 404 Not Found", getRR.Code)
	}
}

func TestHTTP_DeleteWithMismatchedIfMatchFails(t *testing.T) {
	events := map[string]*store.Event{
		"1:event": {CalendarID: 1, UID: "event", RawICAL: "ICAL", ETag: "correct-etag"},
	}
	h, user := conditionalCalendarServer(events)

	req := httptest.NewRequest(http.MethodDelete, "/dav/calendars/1/event.ics", nil)
	req.Header.Set("If-Match", `"wrong-etag"`)
	req = req.WithContext(auth.WithUser(req.Context(), user))
	rr := httptest.NewRecorder()

	h.Delete(rr, req)

	if rr.Code != http.StatusPreconditionFailed {
		t.Errorf("DELETE with a mismatched If-Match = %d, want 412 Precondition Failed (RFC 9110 §13.1.1)", rr.Code)
	}
	if _, exists := events["1:event"]; !exists {
		t.Error("a failed If-Match still deleted the resource")
	}
}

func TestHTTP_DeleteWithMatchingIfMatchSucceeds(t *testing.T) {
	events := map[string]*store.Event{
		"1:event": {CalendarID: 1, UID: "event", RawICAL: "ICAL", ETag: "correct-etag"},
	}
	h, user := conditionalCalendarServer(events)

	req := httptest.NewRequest(http.MethodDelete, "/dav/calendars/1/event.ics", nil)
	req.Header.Set("If-Match", `"correct-etag"`)
	req = req.WithContext(auth.WithUser(req.Context(), user))
	rr := httptest.NewRecorder()

	h.Delete(rr, req)

	if rr.Code != http.StatusNoContent {
		t.Errorf("DELETE with a matching If-Match = %d, want 204 No Content", rr.Code)
	}
	if _, exists := events["1:event"]; exists {
		t.Error("DELETE with a satisfied If-Match left the resource in place")
	}
}

func TestHTTP_DeleteMissingResourceReturnsNotFound(t *testing.T) {
	h, user := conditionalCalendarServer(make(map[string]*store.Event))

	req := httptest.NewRequest(http.MethodDelete, "/dav/calendars/1/missing.ics", nil)
	req = req.WithContext(auth.WithUser(req.Context(), user))
	rr := httptest.NewRecorder()

	h.Delete(rr, req)

	if rr.Code != http.StatusNotFound {
		t.Errorf("DELETE of a resource that does not exist = %d, want 404 Not Found", rr.Code)
	}
}
