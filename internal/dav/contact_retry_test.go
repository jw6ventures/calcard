package dav

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/jw6ventures/calcard/internal/auth"
	"github.com/jw6ventures/calcard/internal/store"
)

type changingContactWriter struct {
	calls  int
	change func()
}

func (b *changingContactWriter) PutContactObject(context.Context, store.ContactObjectWrite) (*store.ContactObjectWriteResult, error) {
	b.calls++
	if b.change != nil {
		b.change()
	}
	return nil, store.ErrResourceStateChanged
}

func TestContactPutRetryRechecksPreconditionsAndAuthorization(t *testing.T) {
	for _, test := range []struct {
		name          string
		status, calls int
	}{
		{"etag changed", http.StatusPreconditionFailed, 1},
		{"access revoked", http.StatusForbidden, 1},
		{"collection removed", http.StatusNotFound, 1},
		{"retry limit", http.StatusConflict, maxResourceStateRetries + 1},
	} {
		t.Run(test.name, func(t *testing.T) {
			books := &fakeAddressBookRepo{books: map[int64]*store.AddressBook{1: {ID: 1, UserID: 1, Name: "Contacts", CTag: 1}}}
			contacts := &fakeContactRepo{contacts: map[string]*store.Contact{"1:c": {ID: 1, AddressBookID: 1, UID: "c", ResourceName: "c", ETag: "original"}}}
			backend := &changingContactWriter{}
			backend.change = func() {
				switch test.name {
				case "etag changed":
					contacts.contacts["1:c"].ETag = "changed"
				case "access revoked":
					books.books[1] = &store.AddressBook{ID: 1, UserID: 2, Name: "Contacts", CTag: 2}
				case "collection removed":
					delete(books.books, 1)
				}
			}
			h := NewDavServer(Options{Store: &store.Store{AddressBooks: books, Contacts: contacts, ContactObjects: backend}})
			req := newAddressBookPutRequest("/dav/addressbooks/1/c.vcf", strings.NewReader(buildVCard("3.0", "UID:c", "FN:Contact")))
			req.Header.Set("If-Match", `"original"`)
			rr := httptest.NewRecorder()
			h.ServeHTTP(rr, req.WithContext(auth.WithUser(req.Context(), &store.User{ID: 1})))
			if rr.Code != test.status || backend.calls != test.calls {
				t.Fatalf("status=%d calls=%d, want %d/%d: %s", rr.Code, backend.calls, test.status, test.calls, rr.Body.String())
			}
		})
	}
}
