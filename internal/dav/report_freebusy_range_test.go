package dav

import (
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/jw6ventures/calcard/internal/auth"
	"github.com/jw6ventures/calcard/internal/ical"
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

// unboundedRecurrenceFreeBusyServer holds a "repeats daily, no end date" event
// beside an ordinary one-off. The daily rule is the default shape every major
// client writes for that request and is storable, so the report has to answer
// over it rather than fail.
func unboundedRecurrenceFreeBusyServer() *DavServer {
	calRepo := &fakeCalendarRepo{
		accessible: []store.CalendarAccess{
			{Calendar: store.Calendar{ID: 1, UserID: 1, Name: "Work"}, Editor: true},
		},
	}
	standUpStart := time.Date(2024, 1, 1, 9, 0, 0, 0, time.UTC)
	standUpEnd := time.Date(2024, 1, 1, 9, 30, 0, 0, time.UTC)
	oneOffStart := time.Date(2024, 3, 4, 14, 0, 0, 0, time.UTC)
	oneOffEnd := time.Date(2024, 3, 4, 15, 0, 0, 0, time.UTC)
	eventRepo := &fakeEventRepo{events: map[string]*store.Event{
		"1:standup": {
			CalendarID: 1, UID: "standup", ResourceName: "standup", ETag: "e1",
			RawICAL: "BEGIN:VCALENDAR\r\nVERSION:2.0\r\nBEGIN:VEVENT\r\nUID:standup\r\n" +
				"DTSTART:20240101T090000Z\r\nDTEND:20240101T093000Z\r\nRRULE:FREQ=DAILY\r\n" +
				"END:VEVENT\r\nEND:VCALENDAR\r\n",
			DTStart: &standUpStart, DTEnd: &standUpEnd,
		},
		"1:oneoff": {
			CalendarID: 1, UID: "oneoff", ResourceName: "oneoff", ETag: "e2",
			RawICAL: "BEGIN:VCALENDAR\r\nVERSION:2.0\r\nBEGIN:VEVENT\r\nUID:oneoff\r\n" +
				"DTSTART:20240304T140000Z\r\nDTEND:20240304T150000Z\r\n" +
				"END:VEVENT\r\nEND:VCALENDAR\r\n",
			DTStart: &oneOffStart, DTEnd: &oneOffEnd,
		},
	}}
	return &DavServer{store: &store.Store{Calendars: calRepo, Events: eventRepo}}
}

// CALDAV:max-instances bounds what one resource may store; it is not a bound on
// how many occurrences a requested range contains. A range holding more than a
// thousand instances of a single daily event is an ordinary request, and
// refusing it would withhold the busy time every other resource in the
// collection publishes too.
func TestFreeBusyPublishesEveryInstanceOfAnUnboundedRule(t *testing.T) {
	h := unboundedRecurrenceFreeBusyServer()
	body := `<C:free-busy-query xmlns:C="urn:ietf:params:xml:ns:caldav">
  <C:time-range start="20240101T000000Z" end="20270101T000000Z"/>
</C:free-busy-query>`

	rr := httptest.NewRecorder()
	h.Report(rr, freeBusyRequestFor("/dav/calendars/1/", body, &store.User{ID: 1}))

	if rr.Code != http.StatusOK {
		t.Fatalf("free-busy over three years of a daily rule = %d, want 200: %s", rr.Code, rr.Body.String())
	}
	periods := parsedFreeBusyPeriods(t, rr.Body.String())
	// 1096 days between 2024-01-01 and 2027-01-01, plus the unrelated one-off.
	if len(periods) != 1097 {
		t.Fatalf("published %d busy periods, want 1097", len(periods))
	}
	if !periods[0].start.Equal(time.Date(2024, 1, 1, 9, 0, 0, 0, time.UTC)) {
		t.Errorf("first period starts at %v, want the first stand-up", periods[0].start)
	}
	last := periods[len(periods)-1]
	if !last.start.Equal(time.Date(2026, 12, 31, 9, 0, 0, 0, time.UTC)) {
		t.Errorf("last period starts at %v, want the last stand-up inside the range", last.start)
	}
	// The sibling resource must survive the recurring one: a per-resource
	// problem is not a reason to publish nothing for the rest of the collection.
	oneOff := time.Date(2024, 3, 4, 14, 0, 0, 0, time.UTC)
	found := false
	for _, period := range periods {
		if period.start.Equal(oneOff) {
			found = true
			break
		}
	}
	if !found {
		t.Error("the one-off event's busy time was not published")
	}
}

// RFC 5545 §3.2 lets a TZID parameter be a quoted string and Exchange writes
// every one that way. The master's DTSTART and the instances generated from it
// have to read that parameter identically, or the report publishes the first
// occurrence at one instant and the rest a whole UTC offset away.
func TestFreeBusyResolvesAQuotedTZIDForEveryInstance(t *testing.T) {
	calRepo := &fakeCalendarRepo{
		accessible: []store.CalendarAccess{
			{Calendar: store.Calendar{ID: 1, UserID: 1, Name: "Work"}, Editor: true},
		},
	}
	object := func(tzid string) string {
		return "BEGIN:VCALENDAR\r\nVERSION:2.0\r\nBEGIN:VTIMEZONE\r\nTZID:America/New_York\r\n" +
			"BEGIN:STANDARD\r\nDTSTART:19701101T020000\r\nRRULE:FREQ=YEARLY;BYMONTH=11;BYDAY=1SU\r\n" +
			"TZOFFSETFROM:-0400\r\nTZOFFSETTO:-0500\r\nTZNAME:EST\r\nEND:STANDARD\r\n" +
			"BEGIN:DAYLIGHT\r\nDTSTART:19700308T020000\r\nRRULE:FREQ=YEARLY;BYMONTH=3;BYDAY=2SU\r\n" +
			"TZOFFSETFROM:-0500\r\nTZOFFSETTO:-0400\r\nTZNAME:EDT\r\nEND:DAYLIGHT\r\nEND:VTIMEZONE\r\n" +
			"BEGIN:VEVENT\r\nUID:standup\r\nDTSTART;TZID=" + tzid + ":20240108T090000\r\n" +
			"DTEND;TZID=" + tzid + ":20240108T093000\r\nRRULE:FREQ=DAILY;COUNT=3\r\n" +
			"END:VEVENT\r\nEND:VCALENDAR\r\n"
	}
	body := `<C:free-busy-query xmlns:C="urn:ietf:params:xml:ns:caldav">
  <C:time-range start="20240101T000000Z" end="20240201T000000Z"/>
</C:free-busy-query>`
	// EST is UTC-5 in January, so every occurrence is 14:00Z.
	want := []string{
		"FREEBUSY:20240108T140000Z/20240108T143000Z",
		"FREEBUSY:20240109T140000Z/20240109T143000Z",
		"FREEBUSY:20240110T140000Z/20240110T143000Z",
	}
	for name, tzid := range map[string]string{
		"unquoted": "America/New_York",
		"quoted":   `"America/New_York"`,
	} {
		t.Run(name, func(t *testing.T) {
			h := &DavServer{store: &store.Store{Calendars: calRepo, Events: &fakeEventRepo{
				events: map[string]*store.Event{"1:standup": {
					CalendarID: 1, UID: "standup", ResourceName: "standup", ETag: "e1",
					RawICAL: object(tzid),
				}},
			}}}
			rr := httptest.NewRecorder()
			h.Report(rr, freeBusyRequestFor("/dav/calendars/1/", body, &store.User{ID: 1}))
			if rr.Code != http.StatusOK {
				t.Fatalf("status = %d, want 200: %s", rr.Code, rr.Body.String())
			}
			assertPublishedFreeBusy(t, parsedFreeBusyLines(t, rr.Body.String()), want)
		})
	}
}

// The periods of every candidate are held at once so they can be merged, so the
// expansion budget belongs to the report rather than to each resource: a
// per-resource bound would multiply by the candidate row budget. Exhausting it
// refuses the report, which is the only answer that neither invents free time
// nor lets one collection decide how much memory a request costs.
func TestFreeBusyExpansionBudgetIsSpentAcrossTheWholeReport(t *testing.T) {
	calRepo := &fakeCalendarRepo{
		accessible: []store.CalendarAccess{
			{Calendar: store.Calendar{ID: 1, UserID: 1, Name: "Work"}, Editor: true},
		},
	}
	// Each daily rule contributes 1096 occurrences over the range, so no single
	// resource is over the budget and together they are past it.
	resources := maxFreeBusyExpandedPeriods/1096 + 1
	events := make(map[string]*store.Event, resources)
	for i := 0; i < resources; i++ {
		uid := fmt.Sprintf("daily-%02d", i)
		events["1:"+uid] = &store.Event{
			CalendarID: 1, UID: uid, ResourceName: uid, ETag: "e" + uid,
			RawICAL: "BEGIN:VCALENDAR\r\nVERSION:2.0\r\nBEGIN:VEVENT\r\nUID:" + uid + "\r\n" +
				"DTSTART:20240101T090000Z\r\nDTEND:20240101T093000Z\r\nRRULE:FREQ=DAILY\r\n" +
				"END:VEVENT\r\nEND:VCALENDAR\r\n",
		}
	}
	h := &DavServer{store: &store.Store{Calendars: calRepo, Events: &fakeEventRepo{events: events}}}
	body := `<C:free-busy-query xmlns:C="urn:ietf:params:xml:ns:caldav">
  <C:time-range start="20240101T000000Z" end="20270101T000000Z"/>
</C:free-busy-query>`

	rr := httptest.NewRecorder()
	h.Report(rr, freeBusyRequestFor("/dav/calendars/1/", body, &store.User{ID: 1}))

	if rr.Code != http.StatusForbidden {
		t.Fatalf("status = %d, want 403", rr.Code)
	}
	if !strings.Contains(rr.Body.String(), "number-of-matches-within-limits") {
		t.Fatalf("refusal carries no DAV:number-of-matches-within-limits postcondition: %s", rr.Body.String())
	}
	if strings.Contains(rr.Body.String(), "FREEBUSY") {
		t.Fatalf("a refused report published a truncated answer: %s", rr.Body.String())
	}
}

// The expansion budget bounds occurrences generated from recurrence rules. The
// periods a stored VFREEBUSY lists are bounded by the size of the resource that
// holds them, so they must not use it up and leave an ordinary daily rule
// beside them refused.
func TestFreeBusyExpansionBudgetIsNotSpentOnStoredPeriods(t *testing.T) {
	calRepo := &fakeCalendarRepo{
		accessible: []store.CalendarAccess{
			{Calendar: store.Calendar{ID: 1, UserID: 1, Name: "Work"}, Editor: true},
		},
	}
	var published strings.Builder
	first := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	for i := 0; i < maxFreeBusyExpandedPeriods; i++ {
		if i > 0 {
			published.WriteByte(',')
		}
		start := first.Add(time.Duration(i) * 2 * time.Minute)
		published.WriteString(start.Format("20060102T150405Z") + "/" + start.Add(time.Minute).Format("20060102T150405Z"))
	}
	eventRepo := &fakeEventRepo{events: map[string]*store.Event{
		"1:a-published": {
			CalendarID: 1, UID: "a-published", ResourceName: "a-published", ETag: "e1",
			RawICAL: "BEGIN:VCALENDAR\r\nVERSION:2.0\r\nBEGIN:VFREEBUSY\r\nUID:a-published\r\n" +
				"DTSTAMP:20240101T000000Z\r\nDTSTART:20240101T000000Z\r\nDTEND:20250101T000000Z\r\n" +
				"FREEBUSY:" + published.String() + "\r\nEND:VFREEBUSY\r\nEND:VCALENDAR\r\n",
		},
		"1:b-standup": {
			CalendarID: 1, UID: "b-standup", ResourceName: "b-standup", ETag: "e2",
			RawICAL: "BEGIN:VCALENDAR\r\nVERSION:2.0\r\nBEGIN:VEVENT\r\nUID:b-standup\r\n" +
				"DTSTART:20240101T090000Z\r\nDTEND:20240101T093000Z\r\nRRULE:FREQ=DAILY\r\n" +
				"END:VEVENT\r\nEND:VCALENDAR\r\n",
		},
	}}
	h := &DavServer{store: &store.Store{Calendars: calRepo, Events: eventRepo}}
	body := `<C:free-busy-query xmlns:C="urn:ietf:params:xml:ns:caldav">
  <C:time-range start="20240101T000000Z" end="20250101T000000Z"/>
</C:free-busy-query>`

	rr := httptest.NewRecorder()
	h.Report(rr, freeBusyRequestFor("/dav/calendars/1/", body, &store.User{ID: 1}))

	if rr.Code != http.StatusOK {
		t.Fatalf("free-busy with a large stored VFREEBUSY beside a daily rule = %d, want 200: %.500s", rr.Code, rr.Body.String())
	}
	if !strings.Contains(rr.Body.String(), "20241231T090000Z/20241231T093000Z") {
		t.Fatal("the daily rule's last occurrence in the range was not published")
	}
}

// The report's recurrence expansion budget is spent across the whole response,
// so every period a resource expands has to be charged to it -- not only the
// ones it goes on to publish. A TRANSP:TRANSPARENT master with one opaque
// override publishes a single period: charging by published interval would
// leave the rest of its set costing nothing, and each further resource would
// then receive the whole budget again, which is the per-resource multiplication
// the shared budget exists to prevent.
func TestFreeBusyExpansionBudgetChargesUnpublishedPeriods(t *testing.T) {
	rangeStart := time.Date(2024, 6, 1, 0, 0, 0, 0, time.UTC)
	rangeEnd := time.Date(2024, 6, 11, 0, 0, 0, 0, time.UTC)
	const dailyInstancesInRange = 10

	raw := "BEGIN:VCALENDAR\r\nBEGIN:VEVENT\r\nUID:transparent\r\n" +
		"DTSTART:20240601T090000Z\r\nDTEND:20240601T100000Z\r\n" +
		"RRULE:FREQ=DAILY\r\nTRANSP:TRANSPARENT\r\nEND:VEVENT\r\n" +
		"BEGIN:VEVENT\r\nUID:transparent\r\nRECURRENCE-ID:20240603T090000Z\r\n" +
		"DTSTART:20240603T090000Z\r\nDTEND:20240603T100000Z\r\nTRANSP:OPAQUE\r\nEND:VEVENT\r\n" +
		"END:VCALENDAR\r\n"
	event := store.Event{CalendarID: 1, UID: "transparent", ResourceName: "transparent", RawICAL: raw}
	matcher, parsed := newEventTimeRangeMatcher(event, floatingZone{})
	if !parsed {
		t.Fatal("fixture did not parse")
	}

	budget := newFreeBusyBudget()
	intervals, err := freeBusyIntervals(freeBusyCandidate{event: event, matcher: matcher}, rangeStart, rangeEnd, budget)
	if err != nil {
		t.Fatalf("freeBusyIntervals() err = %v", err)
	}

	if len(intervals) != 1 {
		t.Fatalf("published intervals = %d, want only the opaque override", len(intervals))
	}
	if charged := maxFreeBusyExpandedPeriods - budget.expandedPeriods; charged != dailyInstancesInRange {
		t.Fatalf("expansion charged = %d, want %d: the periods expanded, not the intervals published",
			charged, dailyInstancesInRange)
	}
}

// A recurrence set none of whose components publish busy time cannot
// contribute a period however it expands, so it is not expanded: it costs the
// report's budget nothing and publishes nothing.
func TestFreeBusyDoesNotExpandASetPublishingNoBusyTime(t *testing.T) {
	rangeStart := time.Date(1900, 1, 1, 0, 0, 0, 0, time.UTC)
	rangeEnd := time.Date(2100, 12, 31, 0, 0, 0, 0, time.UTC)
	tests := map[string]string{
		"transparent master": "BEGIN:VCALENDAR\r\nBEGIN:VEVENT\r\nUID:quiet\r\n" +
			"DTSTART;VALUE=DATE:19000101\r\nRRULE:FREQ=DAILY\r\nTRANSP:TRANSPARENT\r\n" +
			"END:VEVENT\r\nEND:VCALENDAR\r\n",
		"cancelled master and transparent override": "BEGIN:VCALENDAR\r\nBEGIN:VEVENT\r\nUID:quiet\r\n" +
			"DTSTART:19000101T090000Z\r\nDTEND:19000101T100000Z\r\nRRULE:FREQ=DAILY\r\nSTATUS:CANCELLED\r\nEND:VEVENT\r\n" +
			"BEGIN:VEVENT\r\nUID:quiet\r\nRECURRENCE-ID:19000102T090000Z\r\n" +
			"DTSTART:19000102T090000Z\r\nDTEND:19000102T100000Z\r\nTRANSP:TRANSPARENT\r\nEND:VEVENT\r\n" +
			"END:VCALENDAR\r\n",
	}
	for name, raw := range tests {
		t.Run(name, func(t *testing.T) {
			event := store.Event{CalendarID: 1, UID: "quiet", ResourceName: "quiet", RawICAL: raw}
			matcher, parsed := newEventTimeRangeMatcher(event, floatingZone{})
			if !parsed {
				t.Fatal("fixture did not parse")
			}
			budget := newFreeBusyBudget()
			intervals, err := freeBusyIntervals(freeBusyCandidate{event: event, matcher: matcher}, rangeStart, rangeEnd, budget)
			if err != nil {
				t.Fatalf("freeBusyIntervals() err = %v", err)
			}
			if len(intervals) != 0 {
				t.Fatalf("published intervals = %d, want 0", len(intervals))
			}
			if budget.expandedPeriods != maxFreeBusyExpandedPeriods {
				t.Fatalf("expansion charged = %d, want 0", maxFreeBusyExpandedPeriods-budget.expandedPeriods)
			}
		})
	}
}

// Every limit the report can reach is answered with the §7.8 postcondition
// §7.10 gives it, but the cause survives in the error so the log records which
// one: the output budgets are spent on what the requested range holds, while
// running out of scan work is a property of one stored rule.
func TestFreeBusyReportErrorKeepsTheLimitReached(t *testing.T) {
	tests := map[string]struct {
		cause error
		scan  bool
	}{
		"expansion output budget": {cause: ical.ErrRecurrenceExpansionLimit},
		"rule scan work":          {cause: ical.ErrRecurrenceScanLimit, scan: true},
		"stored period cap":       {cause: errFreeBusyStoredPeriodLimit},
	}
	for name, tt := range tests {
		t.Run(name, func(t *testing.T) {
			err := freeBusyReportError(tt.cause)
			if !errors.Is(err, errNumberOfMatchesExceeded) {
				t.Fatalf("freeBusyReportError(%v) = %v, want errNumberOfMatchesExceeded", tt.cause, err)
			}
			if !errors.Is(err, tt.cause) {
				t.Fatalf("freeBusyReportError(%v) = %v, want the cause kept", tt.cause, err)
			}
			if errors.Is(err, ical.ErrRecurrenceScanLimit) != tt.scan {
				t.Fatalf("freeBusyReportError(%v) reads as scan exhaustion = %t, want %t", tt.cause, !tt.scan, tt.scan)
			}
		})
	}

	other := errors.New("storage failure")
	if err := freeBusyReportError(other); err != other {
		t.Fatalf("freeBusyReportError(%v) = %v, want it unchanged", other, err)
	}
}

// A stored rule whose single period is too large to generate cannot be
// expanded over any range. The report refuses rather than publishing that
// resource's busy time as free, and says so with the §7.8 postcondition.
func TestFreeBusyRefusesARuleItCannotScan(t *testing.T) {
	calRepo := &fakeCalendarRepo{
		accessible: []store.CalendarAccess{
			{Calendar: store.Calendar{ID: 1, UserID: 1, Name: "Work"}, Editor: true},
		},
	}
	all := func(n int) string {
		values := make([]string, n)
		for i := range values {
			values[i] = strconv.Itoa(i)
		}
		return strings.Join(values, ",")
	}
	dense := "FREQ=YEARLY;BYDAY=MO,TU,WE,TH,FR,SA,SU;BYHOUR=" + all(24) +
		";BYMINUTE=" + all(60) + ";BYSECOND=" + all(60) + ";BYSETPOS=-1"
	h := &DavServer{store: &store.Store{Calendars: calRepo, Events: &fakeEventRepo{events: map[string]*store.Event{
		"1:dense": {
			CalendarID: 1, UID: "dense", ResourceName: "dense", ETag: "e1",
			RawICAL: "BEGIN:VCALENDAR\r\nVERSION:2.0\r\nBEGIN:VEVENT\r\nUID:dense\r\n" +
				"DTSTART:20240101T000000Z\r\nDTEND:20240101T000001Z\r\nRRULE:" + dense + "\r\n" +
				"END:VEVENT\r\nEND:VCALENDAR\r\n",
		},
	}}}}
	body := `<C:free-busy-query xmlns:C="urn:ietf:params:xml:ns:caldav">
  <C:time-range start="20240101T000000Z" end="20250101T000000Z"/>
</C:free-busy-query>`

	rr := httptest.NewRecorder()
	h.Report(rr, freeBusyRequestFor("/dav/calendars/1/", body, &store.User{ID: 1}))

	if rr.Code != http.StatusForbidden {
		t.Fatalf("status = %d, want 403: %.500s", rr.Code, rr.Body.String())
	}
	if !strings.Contains(rr.Body.String(), "number-of-matches-within-limits") {
		t.Fatalf("refusal carries no DAV:number-of-matches-within-limits postcondition: %.500s", rr.Body.String())
	}
}

// Every generated birthday is TRANSP:TRANSPARENT, so the birthday collection
// publishes no busy time over any range. Each one recurs yearly from this year
// or next, so a range reaching CALDAV:max-date-time holds some seventy
// occurrences per contact, and enough contacts put their sum past the report's
// expansion budget. The answer is still the empty VFREEBUSY: occurrences that
// can never publish busy time are not expanded into a budget at all.
func TestBirthdayFreeBusyOverTheStorableSpanPublishesNothing(t *testing.T) {
	count := maxFreeBusyExpandedPeriods / 50
	contacts := make(map[string]*store.Contact, count)
	for i := 0; i < count; i++ {
		birthday := time.Date(1900+i%200, time.Month(1+i%12), 1+i%28, 0, 0, 0, 0, time.UTC)
		name := fmt.Sprintf("Contact %04d", i)
		uid := fmt.Sprintf("contact-%04d", i)
		contacts["1:"+uid] = &store.Contact{ID: int64(i + 1), AddressBookID: 1, UID: uid, DisplayName: &name, Birthday: &birthday}
	}
	h := &DavServer{store: &store.Store{Contacts: &fakeContactRepo{contacts: contacts}}}
	body := `<C:free-busy-query xmlns:C="urn:ietf:params:xml:ns:caldav">
  <C:time-range start="19000101T000000Z" end="21001231T000000Z"/>
</C:free-busy-query>`

	rr := httptest.NewRecorder()
	h.Report(rr, freeBusyRequestFor("/dav/calendars/-1/", body, &store.User{ID: 1}))

	if rr.Code != http.StatusOK {
		t.Fatalf("birthday free-busy over the storable span = %d, want 200: %.500s", rr.Code, rr.Body.String())
	}
	if periods := parsedFreeBusyPeriods(t, rr.Body.String()); len(periods) != 0 {
		t.Fatalf("birthday collection published %d busy periods, want none", len(periods))
	}
}

// The expansion budget bounds occurrences generated from rules, but the
// periods stored VFREEBUSY components list are held for the merge just the
// same. Each resource is bounded by the body limit; the report is not, since
// the candidate row budget multiplies that bound, so the stored periods of a
// whole report are capped on their own.
func TestFreeBusyStoredPeriodsAreBoundedAcrossTheReport(t *testing.T) {
	calRepo := &fakeCalendarRepo{
		accessible: []store.CalendarAccess{
			{Calendar: store.Calendar{ID: 1, UserID: 1, Name: "Work"}, Editor: true},
		},
	}
	perResource := maxFreeBusyStoredPeriods/2 + 1
	var published strings.Builder
	first := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	for i := 0; i < perResource; i++ {
		if i > 0 {
			published.WriteByte(',')
		}
		start := first.Add(time.Duration(i) * 2 * time.Minute)
		published.WriteString(start.Format("20060102T150405Z") + "/" + start.Add(time.Minute).Format("20060102T150405Z"))
	}
	events := make(map[string]*store.Event, 2)
	for _, uid := range []string{"published-a", "published-b"} {
		events["1:"+uid] = &store.Event{
			CalendarID: 1, UID: uid, ResourceName: uid, ETag: "e" + uid,
			RawICAL: "BEGIN:VCALENDAR\r\nVERSION:2.0\r\nBEGIN:VFREEBUSY\r\nUID:" + uid + "\r\n" +
				"DTSTAMP:20240101T000000Z\r\nDTSTART:20240101T000000Z\r\nDTEND:20250101T000000Z\r\n" +
				"FREEBUSY:" + published.String() + "\r\nEND:VFREEBUSY\r\nEND:VCALENDAR\r\n",
		}
	}
	h := &DavServer{store: &store.Store{Calendars: calRepo, Events: &fakeEventRepo{events: events}}}
	body := `<C:free-busy-query xmlns:C="urn:ietf:params:xml:ns:caldav">
  <C:time-range start="20240101T000000Z" end="20250101T000000Z"/>
</C:free-busy-query>`

	rr := httptest.NewRecorder()
	h.Report(rr, freeBusyRequestFor("/dav/calendars/1/", body, &store.User{ID: 1}))

	if rr.Code != http.StatusForbidden {
		t.Fatalf("status = %d, want 403", rr.Code)
	}
	if !strings.Contains(rr.Body.String(), "number-of-matches-within-limits") {
		t.Fatalf("refusal carries no DAV:number-of-matches-within-limits postcondition: %.500s", rr.Body.String())
	}
}
