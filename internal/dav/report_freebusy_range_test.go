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

// freeBusyRequestFor is reportRequestFor with an explicit Depth. RFC 4791 §7.10
// defaults an absent header to Depth: 0, which reaches the Request-URI alone
// and therefore no calendar object resource, so a fixture about which resources
// the report publishes has to say Depth: 1.
func freeBusyRequestFor(path, body string, user *store.User) *http.Request {
	req := reportRequestFor(path, body, user)
	req.Header.Set("Depth", "1")
	return req
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

// The RFC 4791 §9.11 request shape: `<!ELEMENT free-busy-query (time-range)>`.
func TestFreeBusyQueryAcceptsDirectTimeRange(t *testing.T) {
	body := `<C:free-busy-query xmlns:C="urn:ietf:params:xml:ns:caldav">
  <C:time-range start="20240601T000000Z" end="20240630T235959Z"/>
</C:free-busy-query>`

	h, _ := freeBusyCalendarServer()
	rr := httptest.NewRecorder()

	h.Report(rr, freeBusyRequestFor("/dav/calendars/1/", body, &store.User{ID: 1}))

	if rr.Code != http.StatusOK {
		t.Fatalf("free-busy-query with a direct time-range = %d, want 200: %s", rr.Code, rr.Body.String())
	}
	assertPublishedFreeBusy(t, parsedFreeBusyLines(t, rr.Body.String()), []string{
		"FREEBUSY:20240601T100000Z/20240601T120000Z",
	})
}

// RFC 4791 §7.10 requires exactly one CALDAV:time-range as a direct child of
// CALDAV:free-busy-query, and §9.11's content model -- (time-range) -- admits
// nothing else at all. A range buried in a CALDAV:filter is therefore not a
// second place to look for the bound: the filter itself is a body the model
// does not describe.
func TestFreeBusyQueryRejectsAnythingButOneTimeRange(t *testing.T) {
	const timeRangeChild = `<C:time-range start="20240601T000000Z" end="20240630T235959Z"/>`

	bodies := map[string]string{
		"a filter carrying the range": `<C:free-busy-query xmlns:C="urn:ietf:params:xml:ns:caldav">
  <C:filter>
    <C:comp-filter name="VCALENDAR">
      <C:comp-filter name="VEVENT">` + timeRangeChild + `</C:comp-filter>
    </C:comp-filter>
  </C:filter>
</C:free-busy-query>`,
		"a filter beside the range": `<C:free-busy-query xmlns:C="urn:ietf:params:xml:ns:caldav">` +
			timeRangeChild + `<C:filter><C:comp-filter name="VCALENDAR"/></C:filter></C:free-busy-query>`,
		"two time-ranges": `<C:free-busy-query xmlns:C="urn:ietf:params:xml:ns:caldav">` +
			timeRangeChild + timeRangeChild + `</C:free-busy-query>`,
		"a property selector": `<C:free-busy-query xmlns:C="urn:ietf:params:xml:ns:caldav" xmlns:D="DAV:">` +
			`<D:prop><D:getetag/></D:prop>` + timeRangeChild + `</C:free-busy-query>`,
		"character data": `<C:free-busy-query xmlns:C="urn:ietf:params:xml:ns:caldav">nonsense` +
			timeRangeChild + `</C:free-busy-query>`,
		"an undefined attribute": `<C:free-busy-query xmlns:C="urn:ietf:params:xml:ns:caldav" depth="1">` +
			timeRangeChild + `</C:free-busy-query>`,
	}

	for name, body := range bodies {
		t.Run(name, func(t *testing.T) {
			h, eventRepo := freeBusyCalendarServer()
			rr := httptest.NewRecorder()

			h.Report(rr, freeBusyRequestFor("/dav/calendars/1/", body, &store.User{ID: 1}))

			if rr.Code != http.StatusBadRequest {
				t.Fatalf("status = %d, want 400: %s", rr.Code, rr.Body.String())
			}
			if eventRepo.listForCalendarCalls != 0 || eventRepo.pageLookupCount != 0 {
				t.Fatalf("a refused body still read the repository: ListForCalendar=%d ListForCalendarPageAfter=%d",
					eventRepo.listForCalendarCalls, eventRepo.pageLookupCount)
			}
		})
	}
}

// RFC 4791 §7.10 considers only the calendar object resources the Depth value
// allows and processes an absent header as Depth: 0. A calendar collection is
// not itself a calendar object resource, which is the same reading
// calendar-query already applies, so the default reaches none of its members
// and the report answers with the FREEBUSY-less VFREEBUSY §7.10 requires.
func TestRFC4791_FreeBusyQueryDepthScopesTheTargetSet(t *testing.T) {
	body := `<C:free-busy-query xmlns:C="urn:ietf:params:xml:ns:caldav">
  <C:time-range start="20240601T000000Z" end="20240630T235959Z"/>
</C:free-busy-query>`

	tests := map[string]struct {
		depth    string
		setDepth bool
		wantCode int
		wantBusy bool
	}{
		"absent":        {setDepth: false, wantCode: http.StatusOK, wantBusy: false},
		"0":             {depth: "0", setDepth: true, wantCode: http.StatusOK, wantBusy: false},
		"1":             {depth: "1", setDepth: true, wantCode: http.StatusOK, wantBusy: true},
		"infinity":      {depth: "infinity", setDepth: true, wantCode: http.StatusOK, wantBusy: true},
		"2":             {depth: "2", setDepth: true, wantCode: http.StatusBadRequest},
		"a bogus value": {depth: "1,noroot", setDepth: true, wantCode: http.StatusBadRequest},
	}

	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			h, _ := freeBusyCalendarServer()
			req := reportRequestFor("/dav/calendars/1/", body, &store.User{ID: 1})
			if test.setDepth {
				req.Header.Set("Depth", test.depth)
			}
			rr := httptest.NewRecorder()

			h.Report(rr, req)

			if rr.Code != test.wantCode {
				t.Fatalf("status = %d, want %d: %s", rr.Code, test.wantCode, rr.Body.String())
			}
			if test.wantCode != http.StatusOK {
				return
			}
			// §7.10: exactly one VFREEBUSY either way -- parsedFreeBusyLines
			// asserts that -- carrying no FREEBUSY property when nothing
			// satisfies the Depth value.
			var want []string
			if test.wantBusy {
				want = []string{"FREEBUSY:20240601T100000Z/20240601T120000Z"}
			}
			assertPublishedFreeBusy(t, parsedFreeBusyLines(t, rr.Body.String()), want)
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
