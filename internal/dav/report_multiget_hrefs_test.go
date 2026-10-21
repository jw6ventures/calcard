package dav

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/jw6ventures/calcard/internal/store"
)

func multiGetCalendarServer() *DavServer {
	calRepo := &fakeCalendarRepo{
		accessible: []store.CalendarAccess{
			{Calendar: store.Calendar{ID: 1, UserID: 1, Name: "Work"}, Editor: true},
		},
	}
	eventRepo := &fakeEventRepo{
		events: map[string]*store.Event{
			"1:event": {
				CalendarID:   1,
				UID:          "event",
				ResourceName: "event",
				RawICAL:      "BEGIN:VCALENDAR\r\nBEGIN:VEVENT\r\nUID:event\r\nEND:VEVENT\r\nEND:VCALENDAR\r\n",
				ETag:         "e1",
			},
		},
	}
	return &DavServer{store: &store.Store{Calendars: calRepo, Events: eventRepo}}
}

func calendarMultiGetBody(hrefs ...string) string {
	var sb strings.Builder
	sb.WriteString(`<cal:calendar-multiget xmlns:cal="urn:ietf:params:xml:ns:caldav" xmlns:D="DAV:">`)
	for _, href := range hrefs {
		sb.WriteString("<D:href>" + href + "</D:href>")
	}
	sb.WriteString(`</cal:calendar-multiget>`)
	return sb.String()
}

// RFC 4791 §7.9 requires a DAV:response for every requested href; silently
// dropping unresolvable ones leaves the client without a status.
func TestCalendarMultiGetReturnsPerHrefStatus(t *testing.T) {
	tests := []struct {
		name     string
		href     string
		wantHref string
	}{
		{
			name:     "href naming another collection",
			href:     "/dav/calendars/99/event.ics",
			wantHref: "/dav/calendars/99/event.ics",
		},
		{
			name:     "href outside the calendar namespace",
			href:     "/dav/addressbooks/3/alice.vcf",
			wantHref: "/dav/addressbooks/3/alice.vcf",
		},
		{
			name:     "collection href with no resource segment",
			href:     "/dav/calendars/1/",
			wantHref: "/dav/calendars/1",
		},
		{
			name:     "href outside the DAV tree",
			href:     "/not/a/dav/path",
			wantHref: "/not/a/dav/path",
		},
		{
			name:     "missing resource in this calendar",
			href:     "/dav/calendars/1/absent.ics",
			wantHref: "/dav/calendars/1/absent.ics",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			h := multiGetCalendarServer()
			rr := httptest.NewRecorder()

			h.Report(rr, reportRequestFor("/dav/calendars/1/", calendarMultiGetBody(tt.href), &store.User{ID: 1}))

			if rr.Code != http.StatusMultiStatus {
				t.Fatalf("multiget = %d, want 207: %s", rr.Code, rr.Body.String())
			}
			resp := davResponseForHref(t, rr.Body.String(), tt.wantHref)
			if !strings.Contains(resp, "404 Not Found") {
				t.Fatalf("expected 404 status for href %q, got %s", tt.href, resp)
			}
		})
	}
}

func TestCalendarMultiGetAllInvalidHrefsReturnsNonEmptyMultistatus(t *testing.T) {
	h := multiGetCalendarServer()
	rr := httptest.NewRecorder()
	body := calendarMultiGetBody("/dav/calendars/99/one.ics", "/not/a/dav/path")

	h.Report(rr, reportRequestFor("/dav/calendars/1/", body, &store.User{ID: 1}))

	if rr.Code != http.StatusMultiStatus {
		t.Fatalf("multiget = %d, want 207: %s", rr.Code, rr.Body.String())
	}
	respBody := rr.Body.String()
	if got := strings.Count(respBody, "<d:response>"); got != 2 {
		t.Fatalf("expected one response per requested href, got %d: %s", got, respBody)
	}
	if got := strings.Count(respBody, "404 Not Found"); got != 2 {
		t.Fatalf("expected both hrefs to report 404, got %d: %s", got, respBody)
	}
}

func TestCalendarMultiGetMixedHrefsPreserveRequestOrder(t *testing.T) {
	h := multiGetCalendarServer()
	rr := httptest.NewRecorder()
	body := `<cal:calendar-multiget xmlns:cal="urn:ietf:params:xml:ns:caldav" xmlns:D="DAV:">` +
		`<D:prop><D:getetag/><cal:calendar-data/></D:prop>` +
		`<D:href>/dav/calendars/99/first.ics</D:href>` +
		`<D:href>/dav/calendars/1/event.ics</D:href>` +
		`<D:href>/not/a/dav/path</D:href>` +
		`</cal:calendar-multiget>`

	h.Report(rr, reportRequestFor("/dav/calendars/1/", body, &store.User{ID: 1}))

	if rr.Code != http.StatusMultiStatus {
		t.Fatalf("multiget = %d, want 207: %s", rr.Code, rr.Body.String())
	}
	respBody := rr.Body.String()
	if got := strings.Count(respBody, "<d:response>"); got != 3 {
		t.Fatalf("expected one response per requested href, got %d: %s", got, respBody)
	}
	order := []string{"/dav/calendars/99/first.ics", "/dav/calendars/1/event.ics", "/not/a/dav/path"}
	last := -1
	for _, href := range order {
		idx := strings.Index(respBody, ">"+href+"</d:href>")
		if idx < 0 {
			t.Fatalf("missing response for href %q in %s", href, respBody)
		}
		if idx <= last {
			t.Fatalf("responses are out of request order at href %q: %s", href, respBody)
		}
		last = idx
	}
	if resp := davResponseForHref(t, respBody, "/dav/calendars/1/event.ics"); !strings.Contains(resp, "<d:getetag>") {
		t.Fatalf("expected the valid href to carry the requested properties, got %s", resp)
	}
	// A dropped href reports status only; requested properties must not be
	// decorated onto a resource that does not exist.
	for _, href := range []string{"/dav/calendars/99/first.ics", "/not/a/dav/path"} {
		resp := davResponseForHref(t, respBody, href)
		if !strings.Contains(resp, "404 Not Found") {
			t.Fatalf("expected 404 status for href %q, got %s", href, resp)
		}
		if strings.Contains(resp, "<d:propstat>") {
			t.Fatalf("expected no propstat for unresolvable href %q, got %s", href, resp)
		}
	}
}

func TestBirthdayCalendarMultiGetReturnsPerHrefStatus(t *testing.T) {
	birthday := time.Date(1990, 6, 1, 0, 0, 0, 0, time.UTC)
	name := "Ada"
	contactRepo := &fakeContactRepo{contacts: map[string]*store.Contact{
		"1:ada": {AddressBookID: 1, UID: "ada", DisplayName: &name, Birthday: &birthday},
	}}
	h := &DavServer{store: &store.Store{Contacts: contactRepo}}
	rr := httptest.NewRecorder()
	body := calendarMultiGetBody("/dav/calendars/7/birthday-ada@calcard.ics", "/not/a/dav/path")

	h.Report(rr, reportRequestFor("/dav/calendars/-1/", body, &store.User{ID: 1}))

	if rr.Code != http.StatusMultiStatus {
		t.Fatalf("birthday multiget = %d, want 207: %s", rr.Code, rr.Body.String())
	}
	respBody := rr.Body.String()
	if got := strings.Count(respBody, "<d:response>"); got != 2 {
		t.Fatalf("expected one response per requested href, got %d: %s", got, respBody)
	}
	if got := strings.Count(respBody, "404 Not Found"); got != 2 {
		t.Fatalf("expected both out-of-scope hrefs to report 404, got %d: %s", got, respBody)
	}
}

// A multiget reads its hrefs' resources in one batch and then builds one
// response per href, which for hundreds of hrefs with calendar-data is the
// bulk of the report's work. A client that has already left stops that loop
// rather than leaving it to project every resource for nobody, over a stored
// collection and over the generated birthday collection alike. Each request is
// first answered for a client that stays, so the href is known to name a
// resource the loop would otherwise build a response for.
func TestMultiGetStopsForACancelledRequest(t *testing.T) {
	birthdayServer := func() *DavServer {
		birthday := time.Date(1990, 6, 1, 0, 0, 0, 0, time.UTC)
		name := "Ada"
		contactRepo := &fakeContactRepo{contacts: map[string]*store.Contact{
			"1:ada": {AddressBookID: 1, UID: "ada", DisplayName: &name, Birthday: &birthday},
		}}
		return &DavServer{store: &store.Store{Contacts: contactRepo}}
	}
	birthdays, err := birthdayServer().generateBirthdayEvents(t.Context(), 1)
	if err != nil || len(birthdays) != 1 {
		t.Fatalf("generateBirthdayEvents() = %d events, %v; want one", len(birthdays), err)
	}

	tests := []struct {
		name   string
		server func() *DavServer
		path   string
		href   string
	}{
		{
			name:   "calendar",
			server: multiGetCalendarServer,
			path:   "/dav/calendars/1/",
			href:   "/dav/calendars/1/event.ics",
		},
		{
			name:   "birthday calendar",
			server: birthdayServer,
			path:   "/dav/calendars/-1/",
			href:   calendarObjectHref("/dav/calendars/-1/", eventResourceName(birthdays[0])),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			body := calendarMultiGetBody(tt.href)

			answered := httptest.NewRecorder()
			tt.server().Report(answered, reportRequestFor(tt.path, body, &store.User{ID: 1}))
			if answered.Code != http.StatusMultiStatus {
				t.Fatalf("multiget = %d, want 207: %s", answered.Code, answered.Body.String())
			}
			if resp := davResponseForHref(t, answered.Body.String(), tt.href); !strings.Contains(resp, "200 OK") {
				t.Fatalf("expected %q to name a resource, got %s", tt.href, resp)
			}

			req := reportRequestFor(tt.path, body, &store.User{ID: 1})
			ctx, cancel := context.WithCancel(req.Context())
			cancel()
			abandoned := httptest.NewRecorder()
			tt.server().Report(abandoned, req.WithContext(ctx))

			if abandoned.Code != statusClientClosedRequest {
				t.Fatalf("multiget = %d, want %d: %s", abandoned.Code, statusClientClosedRequest, abandoned.Body.String())
			}
			if abandoned.Body.Len() != 0 {
				t.Fatalf("body = %q, want none for a client that left", abandoned.Body.String())
			}
		})
	}
}
