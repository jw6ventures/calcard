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

// activeLocks holds the locks a WebDAV client has taken.
type activeLocks struct {
	store.LockRepository
	locks []store.Lock
}

func (a *activeLocks) ListByResources(_ context.Context, paths []string) ([]store.Lock, error) {
	var found []store.Lock
	for _, lock := range a.locks {
		for _, resourcePath := range paths {
			if lock.ResourcePath == resourcePath {
				found = append(found, lock)
			}
		}
	}
	return found, nil
}

func (a *activeLocks) DeleteByResourcePath(context.Context, string) error { return nil }

// The REST API cannot submit a lock token, so it writes a contact only as a
// WebDAV write with none would go through: a contact a client has locked, or
// an address book locked to its members, is answered 423 Locked and left as it
// was.
func TestContactWritesAnswerLockedWhereDAVWould(t *testing.T) {
	const card = "BEGIN:VCARD\r\nVERSION:3.0\r\nUID:u1\r\nFN:Replaced\r\nEND:VCARD\r\n"
	lockAt := func(resourcePath, depth string, expires time.Time) []store.Lock {
		return []store.Lock{{Token: "opaquelocktoken:t", ResourcePath: resourcePath, UserID: 1, Depth: depth, ExpiresAt: expires}}
	}
	future, past := time.Now().Add(time.Hour), time.Now().Add(-time.Hour)
	tests := []struct {
		name   string
		locks  []store.Lock
		method string
		uid    string
		want   int
	}{
		{name: "update of a locked contact", locks: lockAt("/dav/addressbooks/2/u1", "0", future), method: http.MethodPut, uid: "u1", want: http.StatusLocked},
		{name: "update under a lock on the book to its members", locks: lockAt("/dav/addressbooks/2", "infinity", future), method: http.MethodPut, uid: "u1", want: http.StatusLocked},
		{name: "update under a lock on the book alone", locks: lockAt("/dav/addressbooks/2", "0", future), method: http.MethodPut, uid: "u1", want: http.StatusOK},
		{name: "update after the lock expired", locks: lockAt("/dav/addressbooks/2/u1", "0", past), method: http.MethodPut, uid: "u1", want: http.StatusOK},
		{name: "delete of a locked contact", locks: lockAt("/dav/addressbooks/2/u1", "0", future), method: http.MethodDelete, uid: "u1", want: http.StatusLocked},
		{name: "delete under a lock on the book alone", locks: lockAt("/dav/addressbooks/2", "0", future), method: http.MethodDelete, uid: "u1", want: http.StatusLocked},
		{name: "delete beside a locked contact", locks: lockAt("/dav/addressbooks/2/other", "0", future), method: http.MethodDelete, uid: "u1", want: http.StatusNoContent},
		{name: "create in a locked book", locks: lockAt("/dav/addressbooks/2", "0", future), method: http.MethodPost, want: http.StatusLocked},
		{name: "create at a name a lock-null resource holds", locks: lockAt("/dav/addressbooks/2/u3", "0", future), method: http.MethodPost, want: http.StatusLocked},
		{name: "create beside a lock-null resource", locks: lockAt("/dav/addressbooks/2/other", "0", future), method: http.MethodPost, want: http.StatusCreated},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			contacts := &fakeContactRepo{contacts: map[string]store.Contact{
				contactKey(2, "u1"): {ID: 1, AddressBookID: 2, UID: "u1", ResourceName: "u1", ETag: "e1", RawVCard: "BEGIN:VCARD\r\nVERSION:3.0\r\nUID:u1\r\nFN:Original\r\nEND:VCARD\r\n"},
			}}
			h := NewHandler(&config.Config{}, &store.Store{
				AddressBooks: &fakeAddressBookRepo{books: map[int64]*store.AddressBook{2: {ID: 2, UserID: 1, Name: "Mine"}}},
				Contacts:     contacts,
				Locks:        &activeLocks{locks: tc.locks},
			}, dav.ValidateCalendarObject)

			body := card
			if tc.method == http.MethodPost {
				body = strings.Replace(strings.Replace(card, "UID:u1", "UID:u3", 1), "Replaced", "Created", 1)
			}
			req := httptest.NewRequest(tc.method, "/api/addressbooks/2/contacts", strings.NewReader(body))
			req.Header.Set("Content-Type", "text/vcard")
			rec := httptest.NewRecorder()
			switch tc.method {
			case http.MethodPut:
				h.UpdateContact(rec, withUserAndRoute(req, "2", tc.uid))
			case http.MethodDelete:
				h.DeleteContact(rec, withUserAndRoute(req, "2", tc.uid))
			default:
				h.CreateContact(rec, withUserAndRoute(req, "2", ""))
			}
			if rec.Code != tc.want {
				t.Fatalf("status = %d (%s), want %d", rec.Code, rec.Body.String(), tc.want)
			}
			if tc.want == http.StatusLocked {
				if !strings.Contains(strings.ToLower(rec.Body.String()), "lock") {
					t.Errorf("body = %q, want it to say why", rec.Body.String())
				}
				if got := contacts.contacts[contactKey(2, "u1")]; !strings.Contains(got.RawVCard, "FN:Original") {
					t.Errorf("the refused write changed the contact: %q", got.RawVCard)
				}
				if _, created := contacts.contacts[contactKey(2, "u3")]; created {
					t.Error("the refused write created a contact")
				}
			}
		})
	}
}

// Sharing an address book changes its ACL, which the ACL method refuses on a
// locked book, so the REST API answers 423 for a lock that covers the book and
// leaves the ACL alone. Unsharing only removes grants and is refused by no lock.
func TestAddressBookSharingAnswersLockedWhereTheACLMethodWould(t *testing.T) {
	future, past := time.Now().Add(time.Hour), time.Now().Add(-time.Hour)
	tests := []struct {
		name  string
		lock  store.Lock
		share int
	}{
		{name: "lock on the book", lock: store.Lock{ResourcePath: "/dav/addressbooks/1", Depth: "0", ExpiresAt: future}, share: http.StatusLocked},
		{name: "lock on the book to its members", lock: store.Lock{ResourcePath: "/dav/addressbooks/1", Depth: "infinity", ExpiresAt: future}, share: http.StatusLocked},
		{name: "lock on a contact", lock: store.Lock{ResourcePath: "/dav/addressbooks/1/c", Depth: "0", ExpiresAt: future}, share: http.StatusNoContent},
		{name: "expired lock on the book", lock: store.Lock{ResourcePath: "/dav/addressbooks/1", Depth: "infinity", ExpiresAt: past}, share: http.StatusNoContent},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			aclRepo := &fakeACLRepo{entries: []store.ACLEntry{{ResourcePath: "/dav/addressbooks/1", PrincipalHref: "/dav/principals/2/", IsGrant: true, Privilege: "read"}}}
			h := NewHandler(&config.Config{}, &store.Store{
				AddressBooks: &fakeAddressBookRepo{books: map[int64]*store.AddressBook{1: {ID: 1, UserID: 1, Name: "Personal"}}},
				Contacts:     &fakeContactRepo{contacts: map[string]store.Contact{}},
				ACLEntries:   aclRepo,
				Locks:        &activeLocks{locks: []store.Lock{tc.lock}},
				Users: &fakeUserRepo{users: map[int64]*store.User{
					1: {ID: 1, PrimaryEmail: "owner@example.com"},
					2: {ID: 2, PrimaryEmail: "sharee@example.com"},
				}},
			}, dav.ValidateCalendarObject)

			share := httptest.NewRecorder()
			h.ShareAddressBook(share, routeReq(httptest.NewRequest(http.MethodPost, "/api/addressbooks/1/shares", strings.NewReader(`{"userId":2,"role":"viewer"}`)), map[string]string{"id": "1"}))
			unshare := httptest.NewRecorder()
			h.UnshareAddressBook(unshare, routeReq(httptest.NewRequest(http.MethodDelete, "/api/addressbooks/1/shares/2", nil), map[string]string{"id": "1", "userId": "2"}))
			if share.Code != tc.share || unshare.Code != http.StatusNoContent {
				t.Fatalf("share = %d (%s), unshare = %d (%s), want %d and %d", share.Code, share.Body.String(), unshare.Code, unshare.Body.String(), tc.share, http.StatusNoContent)
			}
			if len(aclRepo.entries) != 0 {
				t.Errorf("ACL = %+v after the unshare, want none", aclRepo.entries)
			}
		})
	}
}

// A create answers a user who cannot see the address book as for one that is
// not there, and one who can see it but not add to it as forbidden.
func TestCreateContactAnswersByWhatTheUserMaySee(t *testing.T) {
	user := "/dav/principals/1/"
	tests := []struct {
		name    string
		entries []store.ACLEntry
		bookID  string
		want    int
	}{
		{name: "no entry at all", bookID: "2", want: http.StatusNotFound},
		{name: "read only", bookID: "2", entries: []store.ACLEntry{{ResourcePath: "/dav/addressbooks/2", PrincipalHref: user, IsGrant: true, Privilege: "read"}}, want: http.StatusForbidden},
		{name: "everything denied", bookID: "2", entries: []store.ACLEntry{{ResourcePath: "/dav/addressbooks/2", PrincipalHref: user, IsGrant: false, Privilege: "all"}}, want: http.StatusForbidden},
		{name: "bind", bookID: "2", entries: []store.ACLEntry{{ResourcePath: "/dav/addressbooks/2", PrincipalHref: user, IsGrant: true, Privilege: "bind"}}, want: http.StatusCreated},
		{name: "a book that is not there", bookID: "9", want: http.StatusNotFound},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			h := NewHandler(&config.Config{}, &store.Store{
				AddressBooks: &fakeAddressBookRepo{books: map[int64]*store.AddressBook{2: {ID: 2, UserID: 99, Name: "Theirs"}}},
				Contacts:     &fakeContactRepo{contacts: map[string]store.Contact{}},
				ACLEntries:   &fakeACLRepo{entries: tc.entries},
			}, dav.ValidateCalendarObject)
			req := httptest.NewRequest(http.MethodPost, "/api/addressbooks/"+tc.bookID+"/contacts", strings.NewReader("BEGIN:VCARD\r\nVERSION:3.0\r\nUID:n1\r\nFN:New\r\nEND:VCARD\r\n"))
			req.Header.Set("Content-Type", "text/vcard")
			rec := httptest.NewRecorder()
			h.CreateContact(rec, withUserAndRoute(req, tc.bookID, ""))
			if rec.Code != tc.want {
				t.Fatalf("status = %d (%s), want %d", rec.Code, rec.Body.String(), tc.want)
			}
		})
	}
}
