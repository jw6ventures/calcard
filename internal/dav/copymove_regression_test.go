package dav

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/jw6ventures/calcard/internal/auth"
	"github.com/jw6ventures/calcard/internal/store"
)

func TestCopyContactMapsTransactionalUIDConflictToCardDAVPrecondition(t *testing.T) {
	books := &fakeAddressBookRepo{books: map[int64]*store.AddressBook{
		1: {ID: 1, UserID: 1, Name: "Source"},
		2: {ID: 2, UserID: 1, Name: "Destination"},
	}}
	contacts := &fakeContactRepo{
		contacts: map[string]*store.Contact{
			"1:alice": {AddressBookID: 1, UID: "alice", ResourceName: "alice", RawVCard: buildVCard("3.0", "UID:alice", "FN:Alice"), ETag: "etag"},
		},
		copyErr: store.ErrConflict,
	}
	h := NewDavServer(Options{Store: &store.Store{AddressBooks: books, Contacts: contacts}})
	request := httptest.NewRequest("COPY", "/dav/addressbooks/1/alice.vcf", nil)
	request.Header.Set("Destination", "/dav/addressbooks/2/copied.vcf")
	request = request.WithContext(auth.WithUser(request.Context(), &store.User{ID: 1}))
	response := httptest.NewRecorder()

	h.ServeHTTP(response, request)

	if response.Code != http.StatusConflict {
		t.Fatalf("COPY status = %d, want 409: %s", response.Code, response.Body.String())
	}
	for _, want := range []string{"no-uid-conflict", "/dav/addressbooks/2/copied.vcf"} {
		if !strings.Contains(response.Body.String(), want) {
			t.Fatalf("COPY conflict response missing %q: %s", want, response.Body.String())
		}
	}
}

func TestMoveContactMapsTransactionalUIDConflictToCardDAVPrecondition(t *testing.T) {
	books := &fakeAddressBookRepo{books: map[int64]*store.AddressBook{
		1: {ID: 1, UserID: 1, Name: "Source"},
		2: {ID: 2, UserID: 1, Name: "Destination"},
	}}
	contacts := &fakeContactRepo{
		contacts: map[string]*store.Contact{
			"1:alice": {AddressBookID: 1, UID: "alice", ResourceName: "alice", RawVCard: buildVCard("3.0", "UID:alice", "FN:Alice"), ETag: "etag"},
		},
		moveErr: store.ErrConflict,
	}
	h := NewDavServer(Options{Store: &store.Store{AddressBooks: books, Contacts: contacts}})
	request := httptest.NewRequest("MOVE", "/dav/addressbooks/1/alice.vcf", nil)
	request.Header.Set("Destination", "/dav/addressbooks/2/moved.vcf")
	request = request.WithContext(auth.WithUser(request.Context(), &store.User{ID: 1}))
	response := httptest.NewRecorder()

	h.ServeHTTP(response, request)

	if response.Code != http.StatusConflict {
		t.Fatalf("MOVE status = %d, want 409: %s", response.Code, response.Body.String())
	}
	for _, want := range []string{"no-uid-conflict", "/dav/addressbooks/2/moved.vcf"} {
		if !strings.Contains(response.Body.String(), want) {
			t.Fatalf("MOVE conflict response missing %q: %s", want, response.Body.String())
		}
	}
}

func TestCalendarCopyMoveDoNotEmitNoUIDConflictWithoutHref(t *testing.T) {
	calendars := &fakeCalendarRepo{accessible: []store.CalendarAccess{
		{Calendar: store.Calendar{ID: 1, UserID: 1, Name: "Source"}, Editor: true},
		{Calendar: store.Calendar{ID: 2, UserID: 1, Name: "Destination"}, Editor: true},
	}}
	for _, method := range []string{"COPY", "MOVE"} {
		t.Run(method, func(t *testing.T) {
			events := &fakeEventRepo{events: map[string]*store.Event{
				"1:event": {CalendarID: 1, UID: "event", ResourceName: "event", RawICAL: buildCalendarObject(buildVEvent("event")), ETag: "etag"},
			}}
			if method == "COPY" {
				events.copyErr = store.ErrConflict
			} else {
				events.moveErr = store.ErrConflict
			}
			h := NewDavServer(Options{Store: &store.Store{Calendars: calendars, Events: events}})
			request := httptest.NewRequest(method, "/dav/calendars/1/event.ics", nil)
			request.Header.Set("Destination", "/dav/calendars/2/destination.ics")
			request = request.WithContext(auth.WithUser(request.Context(), &store.User{ID: 1}))
			response := httptest.NewRecorder()

			h.ServeHTTP(response, request)

			if response.Code != http.StatusInternalServerError {
				t.Fatalf("%s unidentified conflict = %d, want 500: %s", method, response.Code, response.Body.String())
			}
			if strings.Contains(response.Body.String(), "no-uid-conflict") {
				t.Fatalf("%s emitted no-uid-conflict without its required DAV:href: %s", method, response.Body.String())
			}
		})
	}
}
