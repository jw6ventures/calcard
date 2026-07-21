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
