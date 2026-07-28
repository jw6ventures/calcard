package dav

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/jw6ventures/calcard/internal/auth"
	"github.com/jw6ventures/calcard/internal/store"
)

func freeBusyCalendarServer() (*DavServer, *fakeEventRepo) {
	start := time.Date(2024, 6, 1, 10, 0, 0, 0, time.UTC)
	end := time.Date(2024, 6, 1, 12, 0, 0, 0, time.UTC)
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
				RawICAL:      "BEGIN:VCALENDAR\r\nBEGIN:VEVENT\r\nUID:event\r\nDTSTART:20240601T100000Z\r\nDTEND:20240601T120000Z\r\nEND:VEVENT\r\nEND:VCALENDAR\r\n",
				ETag:         "e1",
				DTStart:      &start,
				DTEnd:        &end,
			},
		},
	}
	return &DavServer{store: &store.Store{Calendars: calRepo, Events: eventRepo}}, eventRepo
}

func reportRequestFor(path, body string, user *store.User) *http.Request {
	req := httptest.NewRequest("REPORT", path, strings.NewReader(body))
	return req.WithContext(auth.WithUser(req.Context(), user))
}

// RFC 4791 §7.10 requires exactly one CALDAV:time-range in a free-busy-query.
// Without one the report would read and serialize the whole collection.
func TestFreeBusyQueryWithoutTimeRangeIsRejected(t *testing.T) {
	tests := []struct {
		name string
		body string
	}{
		{
			name: "bare free-busy-query",
			body: `<C:free-busy-query xmlns:C="urn:ietf:params:xml:ns:caldav"/>`,
		},
		{
			name: "filter without any time-range",
			body: `<C:free-busy-query xmlns:C="urn:ietf:params:xml:ns:caldav">
  <C:filter>
    <C:comp-filter name="VCALENDAR">
      <C:comp-filter name="VEVENT"/>
    </C:comp-filter>
  </C:filter>
</C:free-busy-query>`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			h, eventRepo := freeBusyCalendarServer()
			rr := httptest.NewRecorder()

			h.Report(rr, reportRequestFor("/dav/calendars/1/", tt.body, &store.User{ID: 1}))

			if rr.Code != http.StatusBadRequest {
				t.Fatalf("free-busy-query without time-range = %d, want 400: %s", rr.Code, rr.Body.String())
			}
			if eventRepo.listForCalendarCalls != 0 || eventRepo.pageLookupCount != 0 {
				t.Fatalf("expected no event repository reads, got ListForCalendar=%d ListForCalendarPageAfter=%d",
					eventRepo.listForCalendarCalls, eventRepo.pageLookupCount)
			}
		})
	}
}

func TestFreeBusyQueryAcceptsTimeRangeFromEitherSource(t *testing.T) {
	tests := []struct {
		name string
		body string
	}{
		{
			name: "top-level time-range",
			body: `<C:free-busy-query xmlns:C="urn:ietf:params:xml:ns:caldav">
  <C:time-range start="20240601T000000Z" end="20240630T235959Z"/>
</C:free-busy-query>`,
		},
		{
			name: "filter-embedded time-range only",
			body: `<C:free-busy-query xmlns:C="urn:ietf:params:xml:ns:caldav">
  <C:filter>
    <C:comp-filter name="VCALENDAR">
      <C:comp-filter name="VEVENT">
        <C:time-range start="20240601T000000Z" end="20240630T235959Z"/>
      </C:comp-filter>
    </C:comp-filter>
  </C:filter>
</C:free-busy-query>`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			h, _ := freeBusyCalendarServer()
			rr := httptest.NewRecorder()

			h.Report(rr, reportRequestFor("/dav/calendars/1/", tt.body, &store.User{ID: 1}))

			if rr.Code != http.StatusOK {
				t.Fatalf("free-busy-query with time-range = %d, want 200: %s", rr.Code, rr.Body.String())
			}
			if !strings.Contains(rr.Body.String(), "FREEBUSY:20240601T100000Z/20240601T120000Z") {
				t.Fatalf("expected busy period in response, got %s", rr.Body.String())
			}
		})
	}
}

func TestBirthdayFreeBusyQueryWithoutTimeRangeIsRejected(t *testing.T) {
	birthday := time.Date(1990, 6, 1, 0, 0, 0, 0, time.UTC)
	name := "Ada"
	contactRepo := &fakeContactRepo{contacts: map[string]*store.Contact{
		"1:ada": {AddressBookID: 1, UID: "ada", DisplayName: &name, Birthday: &birthday},
	}}
	h := &DavServer{store: &store.Store{Contacts: contactRepo}}
	rr := httptest.NewRecorder()

	body := `<C:free-busy-query xmlns:C="urn:ietf:params:xml:ns:caldav"/>`
	h.Report(rr, reportRequestFor("/dav/calendars/-1/", body, &store.User{ID: 1}))

	if rr.Code != http.StatusBadRequest {
		t.Fatalf("birthday free-busy-query without time-range = %d, want 400: %s", rr.Code, rr.Body.String())
	}
}
