package api

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/jw6ventures/calcard/internal/acl"
	"github.com/jw6ventures/calcard/internal/auth"
	"github.com/jw6ventures/calcard/internal/config"
	"github.com/jw6ventures/calcard/internal/store"
)

type fakeACLRepo struct{ entries []store.ACLEntry }

func (f *fakeACLRepo) SetACL(_ context.Context, resourcePath string, entries []store.ACLEntry) error {
	kept := f.entries[:0:0]
	for _, e := range f.entries {
		if e.ResourcePath != resourcePath {
			kept = append(kept, e)
		}
	}
	for _, e := range entries {
		e.ResourcePath = resourcePath
		if e.CreatedAt.IsZero() {
			e.CreatedAt = time.Now().UTC()
		}
		kept = append(kept, e)
	}
	f.entries = kept
	return nil
}
func (f *fakeACLRepo) ListByResource(_ context.Context, resourcePath string) ([]store.ACLEntry, error) {
	var out []store.ACLEntry
	for _, e := range f.entries {
		if e.ResourcePath == resourcePath {
			out = append(out, e)
		}
	}
	return out, nil
}
func (f *fakeACLRepo) UpdateACL(ctx context.Context, resourcePath string, mutate func([]store.ACLEntry) ([]store.ACLEntry, error)) error {
	current, err := f.ListByResource(ctx, resourcePath)
	if err != nil {
		return err
	}
	next, err := mutate(current)
	if err != nil {
		return err
	}
	return f.SetACL(ctx, resourcePath, next)
}
func (f *fakeACLRepo) RevokePrincipalGrants(_ context.Context, collectionPath, principalHref string, collectionPrivileges []string) error {
	kept := f.entries[:0:0]
	for _, entry := range f.entries {
		if !entry.IsGrant || acl.NormalizePrincipalHref(entry.PrincipalHref) != acl.NormalizePrincipalHref(principalHref) {
			kept = append(kept, entry)
			continue
		}
		if entry.ResourcePath == collectionPath && slices.Contains(collectionPrivileges, entry.Privilege) {
			continue
		}
		if strings.HasPrefix(entry.ResourcePath, collectionPath+"/") {
			continue
		}
		kept = append(kept, entry)
	}
	f.entries = kept
	return nil
}
func (f *fakeACLRepo) ListByPrincipal(_ context.Context, principalHref string) ([]store.ACLEntry, error) {
	var out []store.ACLEntry
	for _, e := range f.entries {
		if e.PrincipalHref == principalHref {
			out = append(out, e)
		}
	}
	return out, nil
}
func (f *fakeACLRepo) HasPrivilege(context.Context, string, string, string) (bool, error) {
	return false, nil
}
func (f *fakeACLRepo) MoveResourcePath(context.Context, string, string) error { return nil }
func (f *fakeACLRepo) Delete(context.Context, string) error                   { return nil }

type fakeUserRepo struct{ users map[int64]*store.User }

func (f *fakeUserRepo) UpsertOAuthUser(context.Context, string, string, string, string) (*store.User, error) {
	return nil, nil
}
func (f *fakeUserRepo) GetByID(_ context.Context, id int64) (*store.User, error) {
	if u, ok := f.users[id]; ok {
		cp := *u
		return &cp, nil
	}
	return nil, nil
}
func (f *fakeUserRepo) GetByEmail(context.Context, string) (*store.User, error) { return nil, nil }
func (f *fakeUserRepo) ListActive(context.Context) ([]store.User, error)        { return nil, nil }
func (f *fakeUserRepo) MarkOnboardingComplete(context.Context, int64) error     { return nil }

func newSharingHandler() (*Handler, *fakeACLRepo) {
	acl := &fakeACLRepo{}
	h := NewHandler(&config.Config{}, &store.Store{
		AddressBooks: &fakeAddressBookRepo{books: map[int64]*store.AddressBook{
			1: {ID: 1, UserID: 1, Name: "Personal"},
		}},
		Contacts:   &fakeContactRepo{contacts: map[string]store.Contact{}},
		ACLEntries: acl,
		Users: &fakeUserRepo{users: map[int64]*store.User{
			1: {ID: 1, PrimaryEmail: "owner@example.com"},
			2: {ID: 2, PrimaryEmail: "sharee@example.com"},
		}},
	})
	return h, acl
}

func routeReq(req *http.Request, params map[string]string) *http.Request {
	req = req.WithContext(auth.WithUser(req.Context(), &store.User{ID: 1, PrimaryEmail: "owner@example.com"}))
	rc := chi.NewRouteContext()
	for k, v := range params {
		rc.URLParams.Add(k, v)
	}
	return req.WithContext(context.WithValue(req.Context(), chi.RouteCtxKey, rc))
}

func TestAPIShareLifecycle(t *testing.T) {
	h, _ := newSharingHandler()

	// Share as editor.
	body := strings.NewReader(`{"userId":2,"role":"editor"}`)
	req := routeReq(httptest.NewRequest(http.MethodPost, "/api/addressbooks/1/shares", body), map[string]string{"id": "1"})
	rec := httptest.NewRecorder()
	h.ShareAddressBook(rec, req)
	if rec.Code != http.StatusNoContent {
		t.Fatalf("share status=%d body=%s", rec.Code, rec.Body.String())
	}

	// List shares.
	req = routeReq(httptest.NewRequest(http.MethodGet, "/api/addressbooks/1/shares", nil), map[string]string{"id": "1"})
	rec = httptest.NewRecorder()
	h.ListAddressBookShares(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("list status=%d", rec.Code)
	}
	var shares []addressBookShareResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &shares); err != nil {
		t.Fatal(err)
	}
	if len(shares) != 1 || shares[0].UserID != 2 || shares[0].Role != "editor" || shares[0].Email != "sharee@example.com" {
		t.Fatalf("shares=%+v, want one editor share for sharee@example.com", shares)
	}

	// Unshare.
	req = routeReq(httptest.NewRequest(http.MethodDelete, "/api/addressbooks/1/shares/2", nil), map[string]string{"id": "1", "userId": "2"})
	rec = httptest.NewRecorder()
	h.UnshareAddressBook(rec, req)
	if rec.Code != http.StatusNoContent {
		t.Fatalf("unshare status=%d", rec.Code)
	}

	req = routeReq(httptest.NewRequest(http.MethodGet, "/api/addressbooks/1/shares", nil), map[string]string{"id": "1"})
	rec = httptest.NewRecorder()
	h.ListAddressBookShares(rec, req)
	json.Unmarshal(rec.Body.Bytes(), &shares)
	if len(shares) != 0 {
		t.Fatalf("after unshare shares=%+v, want none", shares)
	}
}

// A grant on one contact is evaluated ahead of the book's, so unsharing the
// book must revoke it too or that contact stays readable.
func TestAPIUnshareRevokesResourceLevelGrant(t *testing.T) {
	h, aclRepo := newSharingHandler()
	h.store.Contacts.(*fakeContactRepo).contacts[contactKey(1, "secret")] = store.Contact{
		AddressBookID: 1, UID: "secret", ResourceName: "secret", ETag: "e1",
		RawVCard: "BEGIN:VCARD\r\nVERSION:3.0\r\nUID:secret\r\nFN:Secret\r\nEND:VCARD\r\n",
	}

	body := strings.NewReader(`{"userId":2,"role":"viewer"}`)
	req := routeReq(httptest.NewRequest(http.MethodPost, "/api/addressbooks/1/shares", body), map[string]string{"id": "1"})
	rec := httptest.NewRecorder()
	h.ShareAddressBook(rec, req)
	if rec.Code != http.StatusNoContent {
		t.Fatalf("share status=%d body=%s", rec.Code, rec.Body.String())
	}
	aclRepo.entries = append(aclRepo.entries, store.ACLEntry{
		ResourcePath:  "/dav/addressbooks/1/secret",
		PrincipalHref: "/dav/principals/2/",
		IsGrant:       true,
		Privilege:     "read",
	})

	getSecret := func() *httptest.ResponseRecorder {
		req := httptest.NewRequest(http.MethodGet, "/api/addressbooks/1/contacts/secret", nil)
		req = req.WithContext(auth.WithUser(req.Context(), &store.User{ID: 2, PrimaryEmail: "sharee@example.com"}))
		rc := chi.NewRouteContext()
		rc.URLParams.Add("id", "1")
		rc.URLParams.Add("uid", "secret")
		req = req.WithContext(context.WithValue(req.Context(), chi.RouteCtxKey, rc))
		rec := httptest.NewRecorder()
		h.GetContact(rec, req)
		return rec
	}
	if rec := getSecret(); rec.Code != http.StatusOK {
		t.Fatalf("sharee GetContact before unshare status=%d, want 200", rec.Code)
	}

	req = routeReq(httptest.NewRequest(http.MethodDelete, "/api/addressbooks/1/shares/2", nil), map[string]string{"id": "1", "userId": "2"})
	rec = httptest.NewRecorder()
	h.UnshareAddressBook(rec, req)
	if rec.Code != http.StatusNoContent {
		t.Fatalf("unshare status=%d", rec.Code)
	}

	if rec := getSecret(); rec.Code != http.StatusNotFound && rec.Code != http.StatusForbidden {
		t.Fatalf("sharee GetContact after unshare status=%d, want 404 or 403", rec.Code)
	}
}

func TestAPIListAddressBooksMarksSharedReadOnly(t *testing.T) {
	h, acl := newSharingHandler()
	// User 1 owns book 1; pretend book 1 is also reachable by them as owner.
	// Add a read-only grant for user 1 on a book they do not own to exercise the
	// shared/readOnly flags via ListAccessible.
	h.store.AddressBooks.(*fakeAddressBookRepo).books[2] = &store.AddressBook{ID: 2, UserID: 99, Name: "Shared In"}
	acl.entries = append(acl.entries, store.ACLEntry{ResourcePath: "/dav/addressbooks/2", PrincipalHref: "/dav/principals/1/", IsGrant: true, Privilege: "read"})

	req := routeReq(httptest.NewRequest(http.MethodGet, "/api/addressbooks", nil), nil)
	rec := httptest.NewRecorder()
	h.ListAddressBooks(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("status=%d", rec.Code)
	}
	var books []addressBookResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &books); err != nil {
		t.Fatal(err)
	}
	var owned, shared *addressBookResponse
	for i := range books {
		switch books[i].ID {
		case 1:
			owned = &books[i]
		case 2:
			shared = &books[i]
		}
	}
	if owned == nil || owned.Shared || owned.ReadOnly {
		t.Fatalf("owned book = %+v, want shared=false readOnly=false", owned)
	}
	if shared == nil || !shared.Shared || !shared.ReadOnly {
		t.Fatalf("shared book = %+v, want shared=true readOnly=true", shared)
	}
}
