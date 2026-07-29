package dav

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/jw6ventures/calcard/internal/auth"
	"github.com/jw6ventures/calcard/internal/store"
)

// RFC 6638 §9.1 defines CALDAV:schedule-calendar-transp on a calendar
// collection, and fixes its value as exactly one of CALDAV:opaque or
// CALDAV:transparent. RFC 4791 §5.2.8 is CALDAV:max-instances, not this.
func TestRFC6638_ScheduleCalendarTranspIsOpaqueOrTransparent(t *testing.T) {
	now := store.Now()
	calRepo := &fakeCalendarRepo{
		accessible: []store.CalendarAccess{
			{Calendar: store.Calendar{ID: 1, UserID: 1, Name: "Test", UpdatedAt: now}, Editor: true},
		},
	}
	h := &DavServer{store: &store.Store{Calendars: calRepo, Events: &fakeEventRepo{}}}
	user := &store.User{ID: 1}

	body := `<?xml version="1.0" encoding="utf-8"?>
<d:propfind xmlns:d="DAV:" xmlns:c="urn:ietf:params:xml:ns:caldav">
  <d:prop>
    <c:schedule-calendar-transp/>
  </d:prop>
</d:propfind>`

	req := httptest.NewRequest("PROPFIND", "/dav/calendars/1/", strings.NewReader(body))
	req.Header.Set("Depth", "0")
	req = req.WithContext(auth.WithUser(req.Context(), user))
	rr := httptest.NewRecorder()

	h.Propfind(rr, req)

	ms := decodeMultistatus(t, rr)
	resp := ms.responseForHref(t, "/dav/calendars/1/")
	prop := resp.assertPropStatus(t, calQN("schedule-calendar-transp"), http.StatusOK)

	names := prop.childNames()
	if len(names) != 1 {
		t.Fatalf("schedule-calendar-transp has %d children (%s), want exactly 1", len(names), qnList(names))
	}
	switch names[0] {
	case calQN("opaque"), calQN("transparent"):
	default:
		t.Errorf("schedule-calendar-transp value = %s, want %s or %s",
			qnString(names[0]), qnString(calQN("opaque")), qnString(calQN("transparent")))
	}
}

// Extension: Schedule-Tag Header (RFC 6638 - CalDAV Scheduling)
func TestRFC6638_ScheduleTagHeader(t *testing.T) {
	calRepo := &fakeCalendarRepo{
		accessible: []store.CalendarAccess{
			{Calendar: store.Calendar{ID: 1, UserID: 1, Name: "Test"}, Editor: true},
		},
	}
	eventRepo := &fakeEventRepo{
		events: map[string]*store.Event{
			"1:meeting": {
				CalendarID: 1,
				UID:        "meeting",
				RawICAL:    "BEGIN:VCALENDAR\r\nBEGIN:VEVENT\r\nUID:meeting\r\nATTENDEE:mailto:user@example.com\r\nEND:VEVENT\r\nEND:VCALENDAR\r\n",
				ETag:       "e",
			},
		},
	}
	h := &DavServer{store: &store.Store{Calendars: calRepo, Events: eventRepo}}
	user := &store.User{ID: 1}

	req := httptest.NewRequest(http.MethodGet, "/dav/calendars/1/meeting.ics", nil)
	req = req.WithContext(auth.WithUser(req.Context(), user))
	rr := httptest.NewRecorder()

	h.Get(rr, req)

	scheduleTag := rr.Header().Get("Schedule-Tag")
	if scheduleTag != "" {
		t.Log("Server supports Schedule-Tag header for CalDAV scheduling")
	}
}
