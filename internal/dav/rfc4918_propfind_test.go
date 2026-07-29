package dav

// RFC 4918 §9.1: PROPFIND Depth semantics. RFC 4791 relies on these but does
// not restate them, so they are asserted here rather than in the CalDAV suite.

import (
	"net/http/httptest"
	"testing"

	"github.com/jw6ventures/calcard/internal/auth"
	"github.com/jw6ventures/calcard/internal/store"
)

func TestRFC4918_PropfindDepthSelectsCollectionOrMembers(t *testing.T) {
	now := store.Now()
	calRepo := &fakeCalendarRepo{
		accessible: []store.CalendarAccess{
			{Calendar: store.Calendar{ID: 1, UserID: 1, Name: "Test", UpdatedAt: now}, Editor: true},
		},
	}
	eventRepo := &fakeEventRepo{
		events: map[string]*store.Event{
			"1:event": {CalendarID: 1, UID: "event", ResourceName: "event", RawICAL: "ICAL", ETag: "e", LastModified: now},
		},
	}
	h := &DavServer{store: &store.Store{Calendars: calRepo, Events: eventRepo}}
	user := &store.User{ID: 1}

	tests := []struct {
		name      string
		depth     string
		wantHrefs []string
	}{
		{
			name:      "depth 0 returns the collection alone",
			depth:     "0",
			wantHrefs: []string{"/dav/calendars/1/"},
		},
		{
			name:      "depth 1 returns the collection and its members",
			depth:     "1",
			wantHrefs: []string{"/dav/calendars/1/", "/dav/calendars/1/event.ics"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req := httptest.NewRequest("PROPFIND", "/dav/calendars/1/", nil)
			req.Header.Set("Depth", tt.depth)
			req = req.WithContext(auth.WithUser(req.Context(), user))
			rr := httptest.NewRecorder()

			h.Propfind(rr, req)

			decodeMultistatus(t, rr).assertHrefs(t, tt.wantHrefs...)
		})
	}
}
