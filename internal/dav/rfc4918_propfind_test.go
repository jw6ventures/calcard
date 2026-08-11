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

// RFC 4918 §8.3 makes DAV:href a URI, so a resource name holding a character no
// path segment may carry literally is percent-encoded. A client keys its cache
// on the href, so the member listing and the object's own response have to
// spell one resource one way.
func TestRFC4918_PropfindSpellsOneResourceOneWay(t *testing.T) {
	const calendarResource = "team standup"
	const addressResource = "dana lee"
	displayName := "Dana Lee"
	user := &store.User{ID: 1}
	h := &DavServer{store: &store.Store{
		Calendars: &fakeCalendarRepo{accessible: []store.CalendarAccess{
			{Calendar: store.Calendar{ID: 1, UserID: 1, Name: "Test"}, Editor: true},
		}},
		Events: &fakeEventRepo{events: map[string]*store.Event{
			"1:" + calendarResource: {CalendarID: 1, UID: calendarResource, ResourceName: calendarResource, ETag: "e1",
				RawICAL: "BEGIN:VCALENDAR\r\nBEGIN:VEVENT\r\nUID:event\r\nEND:VEVENT\r\nEND:VCALENDAR\r\n"},
		}},
		AddressBooks: &fakeAddressBookRepo{books: map[int64]*store.AddressBook{
			1: {ID: 1, UserID: 1, Name: "Contacts"},
		}},
		Contacts: &fakeContactRepo{contacts: map[string]*store.Contact{
			"1:" + addressResource: {ID: 1, AddressBookID: 1, UID: addressResource, ResourceName: addressResource,
				DisplayName: &displayName, ETag: "c1", RawVCard: "BEGIN:VCARD\r\nVERSION:3.0\r\nFN:Dana Lee\r\nEND:VCARD\r\n"},
		}},
	}}

	// The hrefs are compared verbatim rather than canonically: two spellings
	// that canonicalize alike are still two cache keys to a client, and a
	// literal space is not a legal URI character in the first place.
	propfind := func(t *testing.T, target, depth string) davMultistatus {
		t.Helper()
		req := httptest.NewRequest("PROPFIND", target, nil)
		req.Header.Set("Depth", depth)
		req = req.WithContext(auth.WithUser(req.Context(), user))
		rr := httptest.NewRecorder()
		h.Propfind(rr, req)
		return decodeMultistatus(t, rr)
	}

	tests := map[string]struct {
		collection string
		object     string
	}{
		"a calendar object": {collection: "/dav/calendars/1/", object: "/dav/calendars/1/team%20standup.ics"},
		"an address object": {collection: "/dav/addressbooks/1/", object: "/dav/addressbooks/1/dana%20lee.vcf"},
	}

	for name, tt := range tests {
		t.Run(name, func(t *testing.T) {
			listed := propfind(t, tt.collection, "1")
			if len(listed.Responses) != 2 {
				t.Fatalf("Depth 1 returned %d responses, want the collection and its member", len(listed.Responses))
			}
			if got := listed.Responses[1].Hrefs[0]; got != tt.object {
				t.Errorf("member href = %q, want %q", got, tt.object)
			}

			targeted := propfind(t, tt.object, "0")
			if len(targeted.Responses) != 1 {
				t.Fatalf("Depth 0 returned %d responses, want 1", len(targeted.Responses))
			}
			if got := targeted.Responses[0].Hrefs[0]; got != tt.object {
				t.Errorf("object href = %q, want %q", got, tt.object)
			}
		})
	}
}
