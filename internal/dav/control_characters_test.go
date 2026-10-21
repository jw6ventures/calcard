package dav

import (
	"encoding/xml"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/jw6ventures/calcard/internal/auth"
	"github.com/jw6ventures/calcard/internal/store"
)

// outlookControlCharacters is an event whose DESCRIPTION carries the vertical
// tab and form feed Outlook and Exchange write into pasted text, and whose
// attendee's name carries a vertical tab. RFC 5545 §3.1 admits neither in a
// content line, and the store may hold both.
var outlookControlCharacters = producerObject("VEVENT", "UID:outlook-controls", "DTSTAMP:20261002T120000Z",
	"DTSTART:20260105T090000Z", "DTEND:20260105T100000Z",
	"DESCRIPTION:Agenda\vItem one\fItem two",
	"ATTENDEE;CN=\"Jane\vDoe\":mailto:jane@example.test")

func TestPutStoresControlCharactersAsSent(t *testing.T) {
	h, eventRepo := writableCalendarServer()
	rr := putCalendarObject(t, h, "outlook-controls.ics", outlookControlCharacters)
	if rr.Code != http.StatusCreated {
		t.Fatalf("PUT = %d, want 201: %s", rr.Code, rr.Body.String())
	}
	if rr.Header().Get("ETag") == "" {
		t.Error("PUT withheld the ETag of an object it stored as sent")
	}
	if stored := storedEventByUID(eventRepo, "outlook-controls"); stored == nil || stored.RawICAL != outlookControlCharacters {
		t.Fatalf("stored %#v, want the object as sent", stored)
	}
}

// COPY and MOVE validate the stored object with the transferred reading, not
// PUT's, so an object stored with control characters is not stranded where it
// is.
func TestCopyMoveTransferAStoredObjectWithControlCharacters(t *testing.T) {
	for _, method := range []string{"COPY", "MOVE"} {
		t.Run(method, func(t *testing.T) {
			calendars := &fakeCalendarRepo{accessible: []store.CalendarAccess{
				{Calendar: store.Calendar{ID: 1, UserID: 1, Name: "Source"}, Editor: true},
				{Calendar: store.Calendar{ID: 2, UserID: 1, Name: "Destination"}, Editor: true},
			}}
			events := &fakeEventRepo{events: map[string]*store.Event{
				"1:outlook-controls": {CalendarID: 1, UID: "outlook-controls", ResourceName: "outlook-controls",
					RawICAL: outlookControlCharacters, ETag: "source-etag"},
			}}
			h := NewDavServer(Options{Store: &store.Store{Calendars: calendars, Events: events, CalendarTransfers: events}})
			req := httptest.NewRequest(method, "/dav/calendars/1/outlook-controls.ics", nil)
			req.Header.Set("Destination", "/dav/calendars/2/outlook-controls.ics")
			rr := httptest.NewRecorder()
			h.ServeHTTP(rr, req.WithContext(auth.WithUser(req.Context(), &store.User{ID: 1})))
			if rr.Code != http.StatusCreated {
				t.Fatalf("%s = %d, want 201: %s", method, rr.Code, rr.Body.String())
			}
			if moved := events.events[events.key(2, "outlook-controls")]; moved == nil || moved.RawICAL != outlookControlCharacters {
				t.Fatalf("%s destination = %#v, want the source body", method, moved)
			}
		})
	}
}

func TestCollectionCopyMoveTransferAMemberWithControlCharacters(t *testing.T) {
	for _, method := range []string{"COPY", "MOVE"} {
		t.Run(method, func(t *testing.T) {
			sourceSlug := "source"
			source := store.Calendar{ID: 1, UserID: 1, Name: "Source", Slug: &sourceSlug}
			calendars := &fakeCalendarRepo{
				accessible: []store.CalendarAccess{{Calendar: source, Editor: true}},
				calendars:  map[int64]*store.Calendar{1: &source},
			}
			events := &fakeEventRepo{events: map[string]*store.Event{
				"1:outlook-controls": {ID: 11, CalendarID: 1, UID: "outlook-controls", ResourceName: "outlook-controls",
					RawICAL: outlookControlCharacters, ETag: "etag"},
			}}
			backend := &recordingCalendarCollectionBackend{result: &store.CalendarCollectionTransferResult{
				Calendar: &store.Calendar{ID: 9, UserID: 1, Name: "Source"}, Created: true,
			}}
			h := NewDavServer(Options{Store: &store.Store{Calendars: calendars, Events: events, CalendarCollections: backend}})
			req := httptest.NewRequest(method, "/dav/calendars/source/", nil)
			req.Header.Set("Destination", "/dav/calendars/copied")
			rr := httptest.NewRecorder()
			h.ServeHTTP(rr, req.WithContext(auth.WithUser(req.Context(), &store.User{ID: 1})))
			if rr.Code != http.StatusCreated {
				t.Fatalf("%s = %d, want 201: %s", method, rr.Code, rr.Body.String())
			}
			if len(backend.transfers) != 1 || len(backend.transfers[0].Members) != 1 ||
				backend.transfers[0].Members[0].RawICAL != outlookControlCharacters {
				t.Fatalf("%s transferred %#v, want the member as stored", method, backend.transfers)
			}
		})
	}
}

// XML 1.0 can carry none of these octets, so the calendar-data a REPORT
// returns replaces each with U+FFFD and the multistatus stays well-formed.
func TestReportReturnsControlCharactersAsWellFormedXML(t *testing.T) {
	h := hostileRecurrenceServer()
	h.store.Events = &fakeEventRepo{events: map[string]*store.Event{
		"1:outlook-controls": {ID: 1, CalendarID: 1, UID: "outlook-controls", ResourceName: "outlook-controls", ETag: "e", RawICAL: outlookControlCharacters},
	}}
	reports := map[string]string{
		"calendar-query": calendarDataQuery(""),
		"calendar-multiget": `<C:calendar-multiget xmlns:D="DAV:" xmlns:C="urn:ietf:params:xml:ns:caldav"><D:prop><C:calendar-data/></D:prop>` +
			`<D:href>/dav/calendars/1/outlook-controls.ics</D:href></C:calendar-multiget>`,
	}
	for name, body := range reports {
		t.Run(name, func(t *testing.T) {
			req := httptest.NewRequest("REPORT", "/dav/calendars/1/", strings.NewReader(body))
			req.Header.Set("Depth", "1")
			rr := httptest.NewRecorder()
			h.Report(rr, req.WithContext(auth.WithUser(req.Context(), &store.User{ID: 1})))
			if rr.Code != http.StatusMultiStatus {
				t.Fatalf("status = %d: %s", rr.Code, rr.Body.String())
			}
			var calendarData strings.Builder
			inCalendarData := false
			decoder := xml.NewDecoder(strings.NewReader(rr.Body.String()))
			for {
				token, err := decoder.Token()
				if errors.Is(err, io.EOF) {
					break
				}
				if err != nil {
					t.Fatalf("multistatus is not well-formed XML: %v\n%q", err, rr.Body.String())
				}
				switch token := token.(type) {
				case xml.StartElement:
					inCalendarData = token.Name.Local == "calendar-data"
				case xml.EndElement:
					inCalendarData = false
				case xml.CharData:
					if inCalendarData {
						calendarData.Write(token)
					}
				}
			}
			for _, want := range []string{"DESCRIPTION:Agenda\uFFFDItem one\uFFFDItem two", "ATTENDEE;CN=\"Jane\uFFFDDoe\""} {
				if !strings.Contains(calendarData.String(), want) {
					t.Errorf("calendar-data lacks %q:\n%q", want, calendarData.String())
				}
			}
		})
	}
}
