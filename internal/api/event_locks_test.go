package api

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/jw6ventures/calcard/internal/config"
	"github.com/jw6ventures/calcard/internal/dav"
	"github.com/jw6ventures/calcard/internal/store"
)

// fixedLocks answers every lock lookup with the locks it holds.
type fixedLocks struct {
	store.LockRepository
	locks []store.Lock
}

func (f *fixedLocks) ListByResources(context.Context, []string) ([]store.Lock, error) {
	return f.locks, nil
}

func (f *fixedLocks) DeleteByResourcePath(context.Context, string) error { return nil }

// An event write the API makes submits no lock token, so it answers 423 Locked
// where a CalDAV write without one would, and writes nothing; a lock that has
// expired refuses nothing.
func TestEventWritesAnswer423WhereAWebDAVLockCoversThem(t *testing.T) {
	const etag = "current"
	stored := store.Event{CalendarID: 1, UID: "event-1", ResourceName: "event-1", ETag: etag, RawICAL: "BEGIN:VCALENDAR\r\nEND:VCALENDAR\r\n"}
	body := `{"inputMode":"structured","structured":{"summary":"Edited","dtstart":"2026-03-20T10:00","dtend":"2026-03-20T11:00"}}`
	requests := []struct {
		name   string
		method string
		uid    string
		body   string
		serve  func(*Handler, http.ResponseWriter, *http.Request)
		ok     int
	}{
		{"create", http.MethodPost, "", body, (*Handler).CreateEvent, http.StatusCreated},
		{"update", http.MethodPut, "event-1", body, (*Handler).UpdateEvent, http.StatusOK},
		{"delete", http.MethodDelete, "event-1", "", (*Handler).DeleteEvent, http.StatusNoContent},
	}
	for _, expired := range []bool{false, true} {
		for _, tt := range requests {
			name := tt.name + "/active lock"
			if expired {
				name = tt.name + "/expired lock"
			}
			t.Run(name, func(t *testing.T) {
				expiry := time.Now().Add(time.Hour)
				if expired {
					expiry = time.Now().Add(-time.Hour)
				}
				events := &fakeEventRepo{events: map[string]store.Event{"1:event-1": stored}}
				handler := NewHandler(&config.Config{}, &store.Store{
					Calendars: &fakeCalendarRepo{calendars: map[int64]*store.CalendarAccess{
						1: {Calendar: store.Calendar{ID: 1, UserID: 1, Name: "Work"}, Editor: true},
					}},
					Events: events,
					Locks: &fixedLocks{locks: []store.Lock{
						{Token: "opaquelocktoken:held", ResourcePath: "/dav/calendars/1", Depth: "infinity", ExpiresAt: expiry},
					}},
				}, dav.ValidateCalendarObject)

				req := httptest.NewRequest(tt.method, "/api/calendars/1/events/"+tt.uid, strings.NewReader(tt.body))
				req.Header.Set("Content-Type", "application/json")
				req = withUserAndRoute(req, "1", tt.uid)
				rec := httptest.NewRecorder()
				tt.serve(handler, rec, req)

				if expired {
					if rec.Code != tt.ok {
						t.Fatalf("%s answered %d, want %d: %s", tt.name, rec.Code, tt.ok, rec.Body.String())
					}
					return
				}
				if rec.Code != http.StatusLocked {
					t.Fatalf("%s answered %d, want 423: %s", tt.name, rec.Code, rec.Body.String())
				}
				if len(events.events) != 1 || events.events["1:event-1"].ETag != etag {
					t.Fatalf("a refused %s changed the stored events: %+v", tt.name, events.events)
				}
			})
		}
	}
}
