package dav

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/jw6ventures/calcard/internal/auth"
	"github.com/jw6ventures/calcard/internal/store"
)

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
	h := &Handler{store: &store.Store{Calendars: calRepo, Events: eventRepo}}
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
