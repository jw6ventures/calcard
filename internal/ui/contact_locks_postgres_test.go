package ui

import (
	"bytes"
	"context"
	"fmt"
	"mime/multipart"
	"net/http"
	"net/http/httptest"
	"net/url"
	"regexp"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/go-chi/chi/v5"

	"github.com/jw6ventures/calcard/internal/auth"
	"github.com/jw6ventures/calcard/internal/config"
	"github.com/jw6ventures/calcard/internal/dav"
	"github.com/jw6ventures/calcard/internal/store"
)

// contactLockFixture is an owner with two address books, the contact "alice"
// in the first put there by a CardDAV client, and a DAV server the same client
// locks resources through.
type contactLockFixture struct {
	t        *testing.T
	database *store.Store
	handler  *Handler
	dav      *dav.DavServer
	owner    *store.User
	fromBook int64
	toBook   int64
}

func newContactLockFixture(t *testing.T) *contactLockFixture {
	t.Helper()
	database := newUIPostgresStore(t)
	owner, err := database.Users.UpsertOAuthUser(t.Context(), "locker", "locker@example.test", "Locker", "Locker")
	if err != nil {
		t.Fatal(err)
	}
	f := &contactLockFixture{
		t:        t,
		database: database,
		handler:  NewHandler(&config.Config{}, database, nil, dav.ValidateCalendarObject, dav.EventInstances),
		dav:      dav.NewDavServer(dav.Options{Store: database}),
		owner:    owner,
	}
	ids := make([]int64, 2)
	for i, name := range []string{"From", "To"} {
		book, err := database.AddressBooks.Create(t.Context(), store.AddressBook{UserID: owner.ID, Name: name})
		if err != nil {
			t.Fatal(err)
		}
		ids[i] = book.ID
	}
	f.fromBook, f.toBook = ids[0], ids[1]
	if rr := f.davRequest(http.MethodPut, f.href(f.fromBook, "alice"), contactLockCard("alice", "Alice"), nil); rr.Code != http.StatusCreated {
		t.Fatalf("PUT alice = %d: %s", rr.Code, rr.Body.String())
	}
	return f
}

func contactLockCard(uid, name string) string {
	return "BEGIN:VCARD\r\nVERSION:3.0\r\nUID:" + uid + "\r\nFN:" + name + "\r\nEND:VCARD\r\n"
}

func (f *contactLockFixture) href(bookID int64, name string) string {
	return fmt.Sprintf("/dav/addressbooks/%d/%s.vcf", bookID, name)
}

func (f *contactLockFixture) davRequest(method, target, body string, headers map[string]string) *httptest.ResponseRecorder {
	f.t.Helper()
	return f.davRequestAs(f.owner, method, target, body, headers)
}

func (f *contactLockFixture) davRequestAs(user *store.User, method, target, body string, headers map[string]string) *httptest.ResponseRecorder {
	f.t.Helper()
	req := httptest.NewRequest(method, target, strings.NewReader(body))
	for name, value := range headers {
		req.Header.Set(name, value)
	}
	rr := httptest.NewRecorder()
	f.dav.ServeHTTP(rr, req.WithContext(auth.WithUser(req.Context(), user)))
	return rr
}

var lockTokenPattern = regexp.MustCompile(`opaquelocktoken:[0-9a-fA-F-]+`)

// lock takes a write lock through a CardDAV LOCK and returns its token.
func (f *contactLockFixture) lock(target, depth string) string {
	f.t.Helper()
	return f.lockAs(f.owner, target, depth)
}

// lockAs is lock for a lock held by user.
func (f *contactLockFixture) lockAs(user *store.User, target, depth string) string {
	f.t.Helper()
	const body = `<?xml version="1.0" encoding="utf-8"?><D:lockinfo xmlns:D="DAV:"><D:lockscope><D:exclusive/></D:lockscope><D:locktype><D:write/></D:locktype><D:owner>another client</D:owner></D:lockinfo>`
	rr := f.davRequestAs(user, "LOCK", target, body, map[string]string{"Depth": depth, "Content-Type": "application/xml", "Timeout": "Second-3600"})
	if rr.Code != http.StatusOK && rr.Code != http.StatusCreated {
		f.t.Fatalf("LOCK %s = %d: %s", target, rr.Code, rr.Body.String())
	}
	token := lockTokenPattern.FindString(rr.Header().Get("Lock-Token") + rr.Body.String())
	if token == "" {
		f.t.Fatalf("LOCK %s gave no token: %s", target, rr.Body.String())
	}
	return token
}

func (f *contactLockFixture) post(handle http.HandlerFunc, bookID int64, uid string, form url.Values) *httptest.ResponseRecorder {
	f.t.Helper()
	req := httptest.NewRequest(http.MethodPost, "/addressbooks/x/contacts/y", strings.NewReader(form.Encode()))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	routeContext := chi.NewRouteContext()
	routeContext.URLParams.Add("id", strconv.FormatInt(bookID, 10))
	routeContext.URLParams.Add("uid", uid)
	req = req.WithContext(context.WithValue(auth.WithUser(req.Context(), f.owner), chi.RouteCtxKey, routeContext))
	rr := httptest.NewRecorder()
	handle(rr, req)
	return rr
}

func (f *contactLockFixture) flash(rr *httptest.ResponseRecorder, key string) string {
	f.t.Helper()
	if rr.Code != http.StatusFound {
		f.t.Fatalf("status = %d (%s), want a redirect carrying the flash", rr.Code, rr.Body.String())
	}
	location, err := url.Parse(rr.Header().Get("Location"))
	if err != nil {
		f.t.Fatal(err)
	}
	return location.Query().Get(key)
}

func (f *contactLockFixture) alice(bookID int64) *store.Contact {
	f.t.Helper()
	contact, err := f.database.Contacts.GetByUID(f.t.Context(), bookID, "alice")
	if err != nil {
		f.t.Fatal(err)
	}
	return contact
}

func (f *contactLockFixture) lockHeld(token string) bool {
	f.t.Helper()
	held, err := f.database.Locks.GetByToken(f.t.Context(), token)
	if err != nil {
		f.t.Fatal(err)
	}
	return held != nil
}

func (f *contactLockFixture) updateForm() url.Values {
	return url.Values{"display_name": {"Changed"}, "etag": {f.alice(f.fromBook).ETag}}
}

// The UI writes a contact only as a CardDAV client holding no lock token could:
// a contact another client has locked is not updated, deleted or moved, and
// says why, where a move used to drop the lock on its way out.
func TestPostgres_UIContactWritesHonourACardDAVLock(t *testing.T) {
	const lockedMessage = "locked"
	tests := []struct {
		name string
		// lock takes the lock the write meets.
		lock func(f *contactLockFixture) string
		do   func(f *contactLockFixture) *httptest.ResponseRecorder
	}{
		{
			name: "update of a locked contact",
			lock: func(f *contactLockFixture) string { return f.lock(f.href(f.fromBook, "alice"), "0") },
			do: func(f *contactLockFixture) *httptest.ResponseRecorder {
				return f.post(f.handler.UpdateContact, f.fromBook, "alice", f.updateForm())
			},
		},
		{
			name: "delete of a locked contact",
			lock: func(f *contactLockFixture) string { return f.lock(f.href(f.fromBook, "alice"), "0") },
			do: func(f *contactLockFixture) *httptest.ResponseRecorder {
				return f.post(f.handler.DeleteContact, f.fromBook, "alice", nil)
			},
		},
		{
			name: "move of a locked contact",
			lock: func(f *contactLockFixture) string { return f.lock(f.href(f.fromBook, "alice"), "0") },
			do: func(f *contactLockFixture) *httptest.ResponseRecorder {
				return f.post(f.handler.MoveContact, f.fromBook, "alice", url.Values{"target_address_book_id": {strconv.FormatInt(f.toBook, 10)}})
			},
		},
		{
			name: "move onto a name locked in the destination",
			lock: func(f *contactLockFixture) string { return f.lock(f.href(f.toBook, "alice"), "0") },
			do: func(f *contactLockFixture) *httptest.ResponseRecorder {
				return f.post(f.handler.MoveContact, f.fromBook, "alice", url.Values{"target_address_book_id": {strconv.FormatInt(f.toBook, 10)}})
			},
		},
		{
			name: "create in an address book locked to its members",
			lock: func(f *contactLockFixture) string {
				return f.lock(fmt.Sprintf("/dav/addressbooks/%d/", f.fromBook), "infinity")
			},
			do: func(f *contactLockFixture) *httptest.ResponseRecorder {
				return f.post(f.handler.CreateContact, f.fromBook, "", url.Values{"display_name": {"Brand New"}})
			},
		},
		{
			name: "update under an address book locked to its members",
			lock: func(f *contactLockFixture) string {
				return f.lock(fmt.Sprintf("/dav/addressbooks/%d/", f.fromBook), "infinity")
			},
			do: func(f *contactLockFixture) *httptest.ResponseRecorder {
				return f.post(f.handler.UpdateContact, f.fromBook, "alice", f.updateForm())
			},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			f := newContactLockFixture(t)
			token := tc.lock(f)
			rr := tc.do(f)
			message := strings.ToLower(f.flash(rr, "error"))
			if !strings.Contains(message, lockedMessage) || strings.Contains(message, "failed") || strings.Contains(message, "wrongly") {
				t.Errorf("error flash = %q, want it to say the contact is locked", message)
			}
			if f.flash(rr, "status") != "" {
				t.Errorf("status flash = %q, want none", f.flash(rr, "status"))
			}
			alice := f.alice(f.fromBook)
			if alice == nil || !strings.Contains(alice.RawVCard, "FN:Alice") {
				t.Errorf("the refused write changed the contact: %+v", alice)
			}
			if f.alice(f.toBook) != nil {
				t.Error("the refused move put the contact in the destination")
			}
			if !f.lockHeld(token) {
				t.Error("the lock is gone: the refused write dropped another client's lock")
			}
		})
	}
}

// A lock that has expired refuses nothing, and a move takes the contact over
// it, as a DAV MOVE does.
func TestPostgres_UIContactWritesIgnoreAnExpiredCardDAVLock(t *testing.T) {
	f := newContactLockFixture(t)
	for _, target := range []string{f.href(f.fromBook, "alice"), f.href(f.toBook, "alice"), fmt.Sprintf("/dav/addressbooks/%d/", f.fromBook)} {
		token := f.lock(target, "0")
		if _, err := f.database.Locks.Refresh(t.Context(), token, 1, time.Now().Add(-time.Minute)); err != nil {
			t.Fatalf("expire the lock on %s: %v", target, err)
		}
	}
	if message := f.flash(f.post(f.handler.UpdateContact, f.fromBook, "alice", f.updateForm()), "error"); message != "" {
		t.Fatalf("update refused: %q", message)
	}
	if rr := f.post(f.handler.MoveContact, f.fromBook, "alice", url.Values{"target_address_book_id": {strconv.FormatInt(f.toBook, 10)}}); f.flash(rr, "status") != "contact_moved" {
		t.Fatalf("move refused: %q", f.flash(rr, "error"))
	}
	if f.alice(f.toBook) == nil || f.alice(f.fromBook) != nil {
		t.Error("the contact did not move")
	}
	if rr := f.post(f.handler.DeleteContact, f.toBook, "alice", nil); f.flash(rr, "status") != "contact_deleted" {
		t.Fatalf("delete refused: %q", f.flash(rr, "error"))
	}
}

// An import skips the cards whose contact, or whose address book, a CardDAV
// client has locked, and says so.
func TestPostgres_UIImportSkipsCardsLockedByACardDAVClient(t *testing.T) {
	f := newContactLockFixture(t)
	token := f.lock(f.href(f.fromBook, "alice"), "0")

	var body bytes.Buffer
	upload := multipart.NewWriter(&body)
	part, err := upload.CreateFormFile("file", "contacts.vcf")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := part.Write([]byte(contactLockCard("alice", "Replaced") + contactLockCard("carol", "Carol"))); err != nil {
		t.Fatal(err)
	}
	if err := upload.Close(); err != nil {
		t.Fatal(err)
	}
	req := httptest.NewRequest(http.MethodPost, "/addressbooks/x/import", &body)
	req.Header.Set("Content-Type", upload.FormDataContentType())
	req = withRouteID(req, strconv.FormatInt(f.fromBook, 10))
	req = req.WithContext(auth.WithUser(req.Context(), f.owner))
	rr := httptest.NewRecorder()
	f.handler.ImportAddressBook(rr, req)

	message := f.flash(rr, "error")
	if !strings.Contains(message, "1 card skipped") || !strings.Contains(message, "card 1") || !strings.Contains(strings.ToLower(message), "locked") {
		t.Errorf("error flash = %q, want card 1 skipped as locked", message)
	}
	if f.flash(rr, "status") != "Imported 1 contact." {
		t.Errorf("status flash = %q, want one contact imported", f.flash(rr, "status"))
	}
	if alice := f.alice(f.fromBook); alice == nil || !strings.Contains(alice.RawVCard, "FN:Alice") {
		t.Errorf("the locked contact was replaced: %+v", alice)
	}
	if !f.lockHeld(token) {
		t.Error("the import dropped the lock")
	}
}

func (f *contactLockFixture) bookPath(bookID int64) string {
	return fmt.Sprintf("/dav/addressbooks/%d/", bookID)
}

func (f *contactLockFixture) bookName(bookID int64) string {
	f.t.Helper()
	book, err := f.database.AddressBooks.GetByID(f.t.Context(), bookID)
	if err != nil || book == nil {
		f.t.Fatalf("load address book %d: %v", bookID, err)
	}
	return book.Name
}

func (f *contactLockFixture) bookACL(bookID int64) []store.ACLEntry {
	f.t.Helper()
	entries, err := f.database.ACLEntries.ListByResource(f.t.Context(), strings.TrimSuffix(f.bookPath(bookID), "/"))
	if err != nil {
		f.t.Fatal(err)
	}
	return entries
}

func (f *contactLockFixture) newUser(name string) *store.User {
	f.t.Helper()
	user, err := f.database.Users.UpsertOAuthUser(f.t.Context(), name, name+"@example.test", name, name)
	if err != nil {
		f.t.Fatal(err)
	}
	return user
}

// davRefuses reports whether the ACL method and a PROPPATCH of the display name
// on the book are answered 423 Locked.
func (f *contactLockFixture) davRefuses(bookID int64, other *store.User) (proppatch, aclMethod bool) {
	f.t.Helper()
	patch := f.davRequest("PROPPATCH", f.bookPath(bookID), `<?xml version="1.0" encoding="utf-8"?><D:propertyupdate xmlns:D="DAV:"><D:set><D:prop><D:displayname>Via DAV</D:displayname></D:prop></D:set></D:propertyupdate>`, map[string]string{"Content-Type": "application/xml"})
	grant := f.davRequest("ACL", f.bookPath(bookID), fmt.Sprintf(`<?xml version="1.0" encoding="utf-8"?><D:acl xmlns:D="DAV:"><D:ace><D:principal><D:href>/dav/principals/%d/</D:href></D:principal><D:grant><D:privilege><D:read/></D:privilege></D:grant></D:ace></D:acl>`, other.ID), map[string]string{"Content-Type": "application/xml"})
	return patch.Code == http.StatusLocked, grant.Code == http.StatusLocked
}

func (f *contactLockFixture) share(handle http.HandlerFunc, bookID int64, form url.Values, userID string) *httptest.ResponseRecorder {
	f.t.Helper()
	return f.shareAs(f.owner, handle, bookID, form, userID)
}

func (f *contactLockFixture) shareAs(user *store.User, handle http.HandlerFunc, bookID int64, form url.Values, userID string) *httptest.ResponseRecorder {
	f.t.Helper()
	req := httptest.NewRequest(http.MethodPost, "/addressbooks/x/shares", strings.NewReader(form.Encode()))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	routeContext := chi.NewRouteContext()
	routeContext.URLParams.Add("id", strconv.FormatInt(bookID, 10))
	if userID != "" {
		routeContext.URLParams.Add("userId", userID)
	}
	req = req.WithContext(context.WithValue(auth.WithUser(req.Context(), user), chi.RouteCtxKey, routeContext))
	rr := httptest.NewRecorder()
	handle(rr, req)
	return rr
}

// The UI renames and shares an address book only as the same PROPPATCH and ACL
// request would go through: a book a CardDAV client has locked, or a book
// beneath a lock to its members, is left as it was, and the DAV requests it is
// compared with answer 423 for the same lock. Unsharing is not refused.
func TestPostgres_UIAddressBookWritesHonourACardDAVLock(t *testing.T) {
	tests := []struct {
		name  string
		depth string
		// target is what the client locks; the book itself unless it is a contact.
		contact bool
		// refused is whether DAV answers 423 to both requests.
		refused bool
	}{
		{name: "a depth-0 lock on the book", depth: "0", refused: true},
		{name: "a depth-infinity lock on the book", depth: "infinity", refused: true},
		{name: "a lock on a contact of the book", depth: "0", contact: true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			f := newContactLockFixture(t)
			other := f.newUser("sharee")
			target := f.bookPath(f.fromBook)
			if tc.contact {
				target = f.href(f.fromBook, "alice")
			}
			token := f.lock(target, tc.depth)

			patchRefused, aclRefused := f.davRefuses(f.fromBook, other)
			if patchRefused != tc.refused || aclRefused != tc.refused {
				t.Fatalf("DAV answers 423 to PROPPATCH = %v and ACL = %v, want both %v", patchRefused, aclRefused, tc.refused)
			}

			rename := f.post(f.handler.RenameAddressBook, f.fromBook, "", url.Values{"name": {"Renamed"}})
			share := f.share(f.handler.ShareAddressBook, f.fromBook, url.Values{"user_id": {strconv.FormatInt(other.ID, 10)}, "role": {"editor"}}, "")
			unshare := f.share(f.handler.UnshareAddressBook, f.fromBook, nil, strconv.FormatInt(other.ID, 10))

			// Unsharing only removes grants, so no lock refuses it.
			if message := f.flash(unshare, "error"); message != "" {
				t.Errorf("unshare was refused: %q", message)
			}
			if tc.refused {
				for name, rr := range map[string]*httptest.ResponseRecorder{"rename": rename, "share": share} {
					if message := strings.ToLower(f.flash(rr, "error")); !strings.Contains(message, "locked") {
						t.Errorf("%s error flash = %q, want it to say the address book is locked", name, message)
					}
				}
				if name := f.bookName(f.fromBook); name != "From" {
					t.Errorf("address book name = %q after a refused rename", name)
				}
				if entries := f.bookACL(f.fromBook); len(entries) != 0 {
					t.Errorf("ACL = %+v after refused share requests, want none", entries)
				}
				if !f.lockHeld(token) {
					t.Error("the lock is gone")
				}
				return
			}
			if f.flash(rename, "status") != "addressbook_renamed" || f.bookName(f.fromBook) != "Renamed" {
				t.Errorf("rename beside a locked contact: flash %q error %q name %q", f.flash(rename, "status"), f.flash(rename, "error"), f.bookName(f.fromBook))
			}
			if f.flash(share, "status") != "addressbook_shared" {
				t.Errorf("share beside a locked contact: error %q", f.flash(share, "error"))
			}
		})
	}
}

// A lock that has expired refuses neither a rename nor a share.
func TestPostgres_UIAddressBookWritesIgnoreAnExpiredCardDAVLock(t *testing.T) {
	f := newContactLockFixture(t)
	other := f.newUser("sharee")
	token := f.lock(f.bookPath(f.fromBook), "infinity")
	if _, err := f.database.Locks.Refresh(t.Context(), token, 1, time.Now().Add(-time.Minute)); err != nil {
		t.Fatal(err)
	}
	if rr := f.post(f.handler.RenameAddressBook, f.fromBook, "", url.Values{"name": {"Renamed"}}); f.flash(rr, "status") != "addressbook_renamed" {
		t.Errorf("rename refused: %q", f.flash(rr, "error"))
	}
	if rr := f.share(f.handler.ShareAddressBook, f.fromBook, url.Values{"user_id": {strconv.FormatInt(other.ID, 10)}, "role": {"viewer"}}, ""); f.flash(rr, "status") != "addressbook_shared" {
		t.Errorf("share refused: %q", f.flash(rr, "error"))
	}
}

// Taking a principal's write grants off a contact, as unsharing and sharing
// read-only do, is part of changing the book's sharing, which a lock on the
// book refuses as it refuses the ACL method on the book. A lock on one of the
// contacts the grants are taken off does not refuse it, and the lock stays.
func TestPostgres_UIAddressBookUnshareIgnoresALockOnAContactItChanges(t *testing.T) {
	for name, change := range map[string]func(f *contactLockFixture, other *store.User) *httptest.ResponseRecorder{
		"unshare": func(f *contactLockFixture, other *store.User) *httptest.ResponseRecorder {
			return f.share(f.handler.UnshareAddressBook, f.fromBook, nil, strconv.FormatInt(other.ID, 10))
		},
		"share read-only": func(f *contactLockFixture, other *store.User) *httptest.ResponseRecorder {
			return f.share(f.handler.ShareAddressBook, f.fromBook, url.Values{"user_id": {strconv.FormatInt(other.ID, 10)}, "role": {"viewer"}}, "")
		},
	} {
		t.Run(name, func(t *testing.T) {
			f := newContactLockFixture(t)
			other := f.newUser("sharee")
			href := fmt.Sprintf("/dav/principals/%d/", other.ID)
			memberPath := fmt.Sprintf("/dav/addressbooks/%d/alice", f.fromBook)
			if err := f.database.ACLEntries.SetACL(t.Context(), memberPath, []store.ACLEntry{{PrincipalHref: href, IsGrant: true, Privilege: "write"}}); err != nil {
				t.Fatal(err)
			}
			if err := f.database.ACLEntries.SetACL(t.Context(), strings.TrimSuffix(f.bookPath(f.fromBook), "/"), []store.ACLEntry{{PrincipalHref: href, IsGrant: true, Privilege: "read"}}); err != nil {
				t.Fatal(err)
			}
			token := f.lock(f.href(f.fromBook, "alice"), "0")

			rr := change(f, other)

			if message := f.flash(rr, "error"); message != "" {
				t.Errorf("the change was refused: %q", message)
			}
			if member, err := f.database.ACLEntries.ListByResource(t.Context(), memberPath); err != nil || len(member) != 0 {
				t.Errorf("the contact's ACL = %+v, %v, want its write grant taken off", member, err)
			}
			if !f.lockHeld(token) {
				t.Error("the lock on the contact is gone")
			}
		})
	}
}

// A read-only share that a lock on the address book refuses changes nothing:
// no contact's grants are swept before the refusal.
func TestPostgres_UIAddressBookReadOnlyShareRefusedByALockChangesNothing(t *testing.T) {
	for _, depth := range []string{"0", "infinity"} {
		t.Run("depth "+depth, func(t *testing.T) {
			f := newContactLockFixture(t)
			other := f.newUser("sharee")
			href := fmt.Sprintf("/dav/principals/%d/", other.ID)
			memberPath := fmt.Sprintf("/dav/addressbooks/%d/alice", f.fromBook)
			if err := f.database.ACLEntries.SetACL(t.Context(), memberPath, []store.ACLEntry{{PrincipalHref: href, IsGrant: true, Privilege: "write"}}); err != nil {
				t.Fatal(err)
			}
			token := f.lock(f.bookPath(f.fromBook), depth)

			rr := f.share(f.handler.ShareAddressBook, f.fromBook, url.Values{"user_id": {strconv.FormatInt(other.ID, 10)}, "role": {"viewer"}}, "")

			if message := strings.ToLower(f.flash(rr, "error")); !strings.Contains(message, "locked") {
				t.Errorf("error flash = %q, want it to say the address book is locked", message)
			}
			if member, err := f.database.ACLEntries.ListByResource(t.Context(), memberPath); err != nil || len(member) != 1 || member[0].Privilege != "write" {
				t.Errorf("the contact's ACL = %+v, %v, want its write grant kept", member, err)
			}
			if entries := f.bookACL(f.fromBook); len(entries) != 0 {
				t.Errorf("the book's ACL = %+v, want it untouched", entries)
			}
			if !f.lockHeld(token) {
				t.Error("the lock is gone")
			}
		})
	}
}

// Removing a principal's access only removes grants, so no lock refuses it. An
// owner who unshares a sharee who has locked the book also drops that sharee's
// lock, which they have no access to keep; a sharee leaving a book somebody
// else locked leaves that lock where it is.
func TestPostgres_UIAddressBookRevocationIgnoresLocks(t *testing.T) {
	t.Run("the owner revokes a sharee who locked the book", func(t *testing.T) {
		f := newContactLockFixture(t)
		sharee := f.newUser("sharee")
		if rr := f.share(f.handler.ShareAddressBook, f.fromBook, url.Values{"user_id": {strconv.FormatInt(sharee.ID, 10)}, "role": {"editor"}}, ""); f.flash(rr, "status") != "addressbook_shared" {
			t.Fatalf("share: %q", f.flash(rr, "error"))
		}
		ownerLock := f.lock(f.href(f.fromBook, "alice"), "0")
		shareeLock := f.lockAs(sharee, f.bookPath(f.fromBook), "0")

		rr := f.share(f.handler.UnshareAddressBook, f.fromBook, nil, strconv.FormatInt(sharee.ID, 10))

		if f.flash(rr, "status") != "addressbook_updated" {
			t.Errorf("unshare: %q", f.flash(rr, "error"))
		}
		if f.lockHeld(shareeLock) {
			t.Error("the revoked sharee's lock was kept")
		}
		if !f.lockHeld(ownerLock) {
			t.Error("the owner's lock was dropped")
		}
	})

	t.Run("a sharee leaves a book the owner locked", func(t *testing.T) {
		f := newContactLockFixture(t)
		sharee := f.newUser("sharee")
		if rr := f.share(f.handler.ShareAddressBook, f.fromBook, url.Values{"user_id": {strconv.FormatInt(sharee.ID, 10)}, "role": {"editor"}}, ""); f.flash(rr, "status") != "addressbook_shared" {
			t.Fatalf("share: %q", f.flash(rr, "error"))
		}
		ownerLock := f.lock(f.bookPath(f.fromBook), "0")

		rr := f.shareAs(sharee, f.handler.UnshareAddressBook, f.fromBook, nil, strconv.FormatInt(sharee.ID, 10))

		if f.flash(rr, "status") != "addressbook_updated" {
			t.Errorf("leave: %q", f.flash(rr, "error"))
		}
		if entries := f.bookACL(f.fromBook); len(entries) != 0 {
			t.Errorf("the book's ACL after the sharee left = %+v", entries)
		}
		if !f.lockHeld(ownerLock) {
			t.Error("the owner's lock was dropped")
		}
	})
}

// DAV meets the lock ahead of a failed precondition, so a stale If-Match on a
// locked contact is 423 there, which the services answer alike.
func TestPostgres_CardDAVAnswersLockedAheadOfAFailedPrecondition(t *testing.T) {
	f := newContactLockFixture(t)
	f.lock(f.href(f.fromBook, "alice"), "0")
	stale := map[string]string{"If-Match": `"stale"`, "Content-Type": "text/vcard"}
	if rr := f.davRequest(http.MethodPut, f.href(f.fromBook, "alice"), contactLockCard("alice", "Changed"), stale); rr.Code != http.StatusLocked {
		t.Errorf("PUT with a stale If-Match = %d, want 423", rr.Code)
	}
	if rr := f.davRequest(http.MethodDelete, f.href(f.fromBook, "alice"), "", map[string]string{"If-Match": `"stale"`}); rr.Code != http.StatusLocked {
		t.Errorf("DELETE with a stale If-Match = %d, want 423", rr.Code)
	}
}

// The UI deletes an address book as a collection DELETE is refused when a
// client holds a lock: by a lock on the book at any depth, by a lock on any
// contact in it, or by a depth-infinity lock above it. The book, its contacts
// and the lock are left as they were. A lock that has expired refuses nothing.
func TestPostgres_UIAddressBookDeleteHonoursACardDAVLock(t *testing.T) {
	tests := []struct {
		name   string
		target func(f *contactLockFixture) string
		depth  string
		expire bool
		locked bool
	}{
		{name: "a depth-0 lock on the book", target: func(f *contactLockFixture) string { return f.bookPath(f.fromBook) }, depth: "0", locked: true},
		{name: "a depth-infinity lock on the book", target: func(f *contactLockFixture) string { return f.bookPath(f.fromBook) }, depth: "infinity", locked: true},
		{name: "a lock on a contact", target: func(f *contactLockFixture) string { return f.href(f.fromBook, "alice") }, depth: "0", locked: true},
		{name: "a lock on a name no contact holds", target: func(f *contactLockFixture) string { return f.href(f.fromBook, "ghost") }, depth: "0", locked: true},
		{name: "a lock on another book", target: func(f *contactLockFixture) string { return f.bookPath(f.toBook) }, depth: "infinity"},
		{name: "an expired lock on the book", target: func(f *contactLockFixture) string { return f.bookPath(f.fromBook) }, depth: "infinity", expire: true},
		{name: "an expired lock on a contact", target: func(f *contactLockFixture) string { return f.href(f.fromBook, "alice") }, depth: "0", expire: true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			f := newContactLockFixture(t)
			token := f.lock(tc.target(f), tc.depth)
			if tc.expire {
				if _, err := f.database.Locks.Refresh(t.Context(), token, 1, time.Now().Add(-time.Minute)); err != nil {
					t.Fatal(err)
				}
			}
			rr := f.post(f.handler.DeleteAddressBook, f.fromBook, "", nil)
			if !tc.locked {
				if f.flash(rr, "status") != "addressbook_deleted" {
					t.Fatalf("delete refused: %q", f.flash(rr, "error"))
				}
				if book, _ := f.database.AddressBooks.GetByID(t.Context(), f.fromBook); book != nil {
					t.Error("the address book is still stored")
				}
				return
			}
			if message := strings.ToLower(f.flash(rr, "error")); !strings.Contains(message, "locked") {
				t.Errorf("error flash = %q, want it to say the address book is locked", message)
			}
			if book, _ := f.database.AddressBooks.GetByID(t.Context(), f.fromBook); book == nil {
				t.Error("the refused delete removed the address book")
			}
			if f.alice(f.fromBook) == nil {
				t.Error("the refused delete removed a contact")
			}
			if !f.lockHeld(token) {
				t.Error("the lock is gone")
			}
		})
	}
}
