package dav

import (
	"context"
	"encoding/xml"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/jw6ventures/calcard/internal/auth"
	"github.com/jw6ventures/calcard/internal/config"
	"github.com/jw6ventures/calcard/internal/store"
)

func newAddressBookPutRequest(path string, body io.Reader) *http.Request {
	req := httptest.NewRequest(http.MethodPut, path, body)
	req.Header.Set("Content-Type", "text/vcard")
	return req
}

func buildVCard(version string, lines ...string) string {
	var b strings.Builder
	b.WriteString("BEGIN:VCARD\r\n")
	b.WriteString("VERSION:")
	b.WriteString(version)
	b.WriteString("\r\n")
	for _, line := range lines {
		b.WriteString(line)
		b.WriteString("\r\n")
	}
	b.WriteString("END:VCARD\r\n")
	return b.String()
}

type fakeLockRepo struct {
	locks               map[string]*store.Lock
	listByResourcesErr  error
	moveResourcePathErr error
}

func (f *fakeLockRepo) Create(ctx context.Context, lock store.Lock) (*store.Lock, error) {
	if f.locks == nil {
		f.locks = map[string]*store.Lock{}
	}
	// Simulate conflict checking (mirrors postgres lockRepo.Create behavior).
	now := time.Now()
	for _, existing := range f.locks {
		if existing.ResourcePath != lock.ResourcePath {
			if existing.Depth != "infinity" || !containsPath(lock.ResourcePath, existing.ResourcePath) {
				if lock.Depth != "infinity" || !containsPath(existing.ResourcePath, lock.ResourcePath) {
					continue
				}
			}
		}
		if existing.ExpiresAt.Before(now) {
			continue
		}
		if existing.LockScope == "exclusive" || lock.LockScope == "exclusive" {
			return nil, store.ErrLockConflict
		}
	}
	copy := lock
	f.locks[lock.Token] = &copy
	return &copy, nil
}

func (f *fakeLockRepo) GetByToken(ctx context.Context, token string) (*store.Lock, error) {
	if lock, ok := f.locks[token]; ok {
		copy := *lock
		return &copy, nil
	}
	return nil, nil
}

func (f *fakeLockRepo) ListByResource(ctx context.Context, resourcePath string) ([]store.Lock, error) {
	var result []store.Lock
	for _, lock := range f.locks {
		if lock.ResourcePath == resourcePath {
			result = append(result, *lock)
		}
	}
	return result, nil
}

func (f *fakeLockRepo) ListByResources(ctx context.Context, paths []string) ([]store.Lock, error) {
	if f.listByResourcesErr != nil {
		return nil, f.listByResourcesErr
	}
	pathSet := make(map[string]struct{}, len(paths))
	for _, p := range paths {
		pathSet[p] = struct{}{}
	}
	var result []store.Lock
	for _, lock := range f.locks {
		if _, ok := pathSet[lock.ResourcePath]; ok {
			result = append(result, *lock)
		}
	}
	return result, nil
}

func containsPath(path, parent string) bool {
	if path == parent {
		return true
	}
	parent = strings.TrimSuffix(parent, "/")
	if parent == "" {
		return false
	}
	return strings.HasPrefix(path, parent+"/")
}

func (f *fakeLockRepo) ListByResourcePrefix(ctx context.Context, prefix string) ([]store.Lock, error) {
	var result []store.Lock
	for _, lock := range f.locks {
		if strings.HasPrefix(lock.ResourcePath, prefix) {
			result = append(result, *lock)
		}
	}
	return result, nil
}

func (f *fakeLockRepo) MoveResourcePath(ctx context.Context, fromPath, toPath string) error {
	if f.moveResourcePathErr != nil {
		return f.moveResourcePathErr
	}
	for token, lock := range f.locks {
		if lock.ResourcePath == toPath {
			delete(f.locks, token)
		}
	}
	for _, lock := range f.locks {
		if lock.ResourcePath == fromPath {
			lock.ResourcePath = toPath
		}
	}
	return nil
}

func (f *fakeLockRepo) DeleteByResourcePath(ctx context.Context, resourcePath string) error {
	for token, lock := range f.locks {
		if lock.ResourcePath == resourcePath {
			delete(f.locks, token)
		}
	}
	return nil
}

func (f *fakeLockRepo) Delete(ctx context.Context, token string) error {
	delete(f.locks, token)
	return nil
}

func (f *fakeLockRepo) DeleteExpired(ctx context.Context) (int64, error) {
	return 0, nil
}

func (f *fakeLockRepo) Refresh(ctx context.Context, token string, newTimeout int, newExpiry time.Time) (*store.Lock, error) {
	lock, ok := f.locks[token]
	if !ok {
		return nil, store.ErrNotFound
	}
	lock.TimeoutSeconds = newTimeout
	lock.ExpiresAt = newExpiry
	copy := *lock
	return &copy, nil
}

type fakeACLRepo struct {
	entries              []store.ACLEntry
	listByResourceErr    error
	listByPrincipalErr   error
	moveResourcePathHook func(fromPath, toPath string)
	moveResourcePathErr  error
}

func (f *fakeACLRepo) SetACL(ctx context.Context, resourcePath string, entries []store.ACLEntry) error {
	filtered := make([]store.ACLEntry, 0, len(f.entries))
	for _, entry := range f.entries {
		if entry.ResourcePath != resourcePath {
			filtered = append(filtered, entry)
		}
	}
	for _, entry := range entries {
		entry.ResourcePath = resourcePath
		filtered = append(filtered, entry)
	}
	f.entries = filtered
	return nil
}

func (f *fakeACLRepo) ListByResource(ctx context.Context, resourcePath string) ([]store.ACLEntry, error) {
	if f.listByResourceErr != nil {
		return nil, f.listByResourceErr
	}
	var result []store.ACLEntry
	for _, entry := range f.entries {
		if entry.ResourcePath == resourcePath {
			result = append(result, entry)
		}
	}
	return result, nil
}

func (f *fakeACLRepo) ListByPrincipal(ctx context.Context, principalHref string) ([]store.ACLEntry, error) {
	if f.listByPrincipalErr != nil {
		return nil, f.listByPrincipalErr
	}
	var result []store.ACLEntry
	for _, entry := range f.entries {
		if entry.PrincipalHref == principalHref {
			result = append(result, entry)
		}
	}
	return result, nil
}

func (f *fakeACLRepo) HasPrivilege(ctx context.Context, resourcePath, principalHref, privilege string) (bool, error) {
	entries, err := f.ListByResource(ctx, resourcePath)
	if err != nil {
		return false, err
	}
	for _, entry := range entries {
		if entry.PrincipalHref == principalHref && entry.Privilege == privilege && !entry.IsGrant {
			return false, nil
		}
	}
	for _, entry := range entries {
		if entry.PrincipalHref == principalHref && entry.Privilege == privilege && entry.IsGrant {
			return true, nil
		}
	}
	return false, nil
}

func (f *fakeACLRepo) Delete(ctx context.Context, resourcePath string) error {
	filtered := f.entries[:0]
	for _, entry := range f.entries {
		if entry.ResourcePath != resourcePath {
			filtered = append(filtered, entry)
		}
	}
	f.entries = filtered
	return nil
}

func (f *fakeACLRepo) MoveResourcePath(ctx context.Context, fromPath, toPath string) error {
	if f.moveResourcePathHook != nil {
		f.moveResourcePathHook(fromPath, toPath)
	}
	if f.moveResourcePathErr != nil {
		return f.moveResourcePathErr
	}
	filtered := f.entries[:0]
	for _, entry := range f.entries {
		if entry.ResourcePath != toPath {
			filtered = append(filtered, entry)
		}
	}
	f.entries = filtered
	for i := range f.entries {
		if f.entries[i].ResourcePath == fromPath {
			f.entries[i].ResourcePath = toPath
		}
	}
	return nil
}

func TestRFC6352_RequirementsOverview(t *testing.T) {
	t.Run("Section3_OptionsAdvertisesCardDAV", func(t *testing.T) {
		h := NewHandler(&config.Config{}, &store.Store{})
		req := httptest.NewRequest(http.MethodOptions, "/dav/addressbooks/1/", nil)
		rr := httptest.NewRecorder()

		h.Options(rr, req)

		davHeader := rr.Header().Get("DAV")
		allowHeader := rr.Header().Get("Allow")

		if !strings.Contains(davHeader, "addressbook") {
			t.Errorf("RFC 6352 Sections 3 and 6.1: DAV header MUST include addressbook, got %q", davHeader)
		}
		if !strings.Contains(allowHeader, "REPORT") {
			t.Errorf("RFC 6352 Section 3: CardDAV servers MUST support REPORT, got Allow=%q", allowHeader)
		}
		if !strings.Contains(allowHeader, "PROPFIND") || !strings.Contains(allowHeader, "PROPPATCH") {
			t.Errorf("RFC 6352 Section 3: CardDAV servers MUST support WebDAV property discovery/update methods, got Allow=%q", allowHeader)
		}
		if !strings.Contains(allowHeader, "PUT") || !strings.Contains(allowHeader, "DELETE") {
			t.Errorf("RFC 6352 Sections 5.1 and 6.3.2: CardDAV servers MUST support address object retrieval/update lifecycle methods, got Allow=%q", allowHeader)
		}
		if !strings.Contains(allowHeader, "MKCOL") {
			t.Errorf("RFC 6352 Section 6.3.1: Server SHOULD support MKCOL for address book creation, got Allow=%q", allowHeader)
		}
	})

	t.Run("Section3_WebDAVClass3AndACLAreAdvertised", func(t *testing.T) {
		h := NewHandler(&config.Config{}, &store.Store{})
		req := httptest.NewRequest(http.MethodOptions, "/dav/addressbooks/1/", nil)
		rr := httptest.NewRecorder()

		h.Options(rr, req)

		davHeader := rr.Header().Get("DAV")
		if !strings.Contains(davHeader, "3") {
			t.Errorf("RFC 6352 Section 3: CardDAV servers MUST support WebDAV Class 3, got DAV=%q", davHeader)
		}
		if !strings.Contains(davHeader, "access-control") {
			t.Errorf("RFC 6352 Section 3: CardDAV servers MUST support WebDAV ACL and advertise access-control, got DAV=%q", davHeader)
		}
	})

	t.Run("Section3_WebDAVMethodSurfaceMatchesAdvertisedSupport", func(t *testing.T) {
		h := NewHandler(&config.Config{}, &store.Store{})
		t.Run("collection", func(t *testing.T) {
			req := httptest.NewRequest(http.MethodOptions, "/dav/addressbooks/1/", nil)
			rr := httptest.NewRecorder()

			h.Options(rr, req)

			allowHeader := rr.Header().Get("Allow")
			for _, method := range []string{"LOCK", "UNLOCK", "ACL"} {
				if !strings.Contains(allowHeader, method) {
					t.Errorf("RFC 6352 Section 3: collection Allow header should expose %s, got Allow=%q", method, allowHeader)
				}
			}
			for _, method := range []string{"COPY", "MOVE"} {
				if strings.Contains(allowHeader, method) {
					t.Errorf("RFC 6352 Section 3: collection Allow header should not advertise unsupported %s, got Allow=%q", method, allowHeader)
				}
			}
		})

		t.Run("object", func(t *testing.T) {
			req := httptest.NewRequest(http.MethodOptions, "/dav/addressbooks/1/alice.vcf", nil)
			rr := httptest.NewRecorder()

			h.Options(rr, req)

			allowHeader := rr.Header().Get("Allow")
			for _, method := range []string{"COPY", "MOVE", "LOCK", "UNLOCK", "ACL"} {
				if !strings.Contains(allowHeader, method) {
					t.Errorf("RFC 6352 Section 3: object Allow header should expose %s, got Allow=%q", method, allowHeader)
				}
			}
		})
	})

	t.Run("Section3_WebDAVClass3LocksBlockWritesOnDescendants", func(t *testing.T) {
		user := &store.User{ID: 1, PrimaryEmail: "owner@example.com"}
		bookRepo := &fakeAddressBookRepo{
			books: map[int64]*store.AddressBook{
				5: {ID: 5, UserID: 1, Name: "Contacts"},
			},
		}
		lockRepo := &fakeLockRepo{
			locks: map[string]*store.Lock{
				"opaquelocktoken:book-lock": {
					Token:        "opaquelocktoken:book-lock",
					ResourcePath: "/dav/addressbooks/5",
					UserID:       1,
					Depth:        "infinity",
					ExpiresAt:    time.Now().Add(time.Hour),
				},
			},
		}
		h := &Handler{store: &store.Store{AddressBooks: bookRepo, Contacts: &fakeContactRepo{}, Locks: lockRepo}}
		vcard := buildVCard("3.0", "UID:locked", "FN:Locked Contact")

		req := newAddressBookPutRequest("/dav/addressbooks/5/locked.vcf", strings.NewReader(vcard))
		req = req.WithContext(auth.WithUser(req.Context(), user))
		rr := httptest.NewRecorder()
		h.Put(rr, req)

		if rr.Code != http.StatusLocked {
			t.Fatalf("RFC 6352 Section 3: depth-infinity collection locks must block child writes without the lock token, got %d", rr.Code)
		}

		req = newAddressBookPutRequest("/dav/addressbooks/5/locked.vcf", strings.NewReader(vcard))
		req.Header.Set("If", "(<opaquelocktoken:book-lock>)")
		req = req.WithContext(auth.WithUser(req.Context(), user))
		rr = httptest.NewRecorder()
		h.Put(rr, req)

		if rr.Code != http.StatusCreated {
			t.Fatalf("RFC 6352 Section 3: writes with the matching lock token should succeed, got %d", rr.Code)
		}
	})

	t.Run("Section3_ACLDenyOverridesBroadGrant", func(t *testing.T) {
		user := &store.User{ID: 2, PrimaryEmail: "reader@example.com"}
		bookRepo := &fakeAddressBookRepo{
			books: map[int64]*store.AddressBook{
				5: {ID: 5, UserID: 1, Name: "Shared Contacts"},
			},
		}
		aclRepo := &fakeACLRepo{
			entries: []store.ACLEntry{
				{ResourcePath: "/dav/addressbooks/5/alice.vcf", PrincipalHref: "DAV:authenticated", IsGrant: true, Privilege: "read"},
				{ResourcePath: "/dav/addressbooks/5/alice.vcf", PrincipalHref: "/dav/principals/2/", IsGrant: false, Privilege: "read"},
			},
		}
		h := &Handler{store: &store.Store{AddressBooks: bookRepo, ACLEntries: aclRepo}}
		req := httptest.NewRequest(http.MethodGet, "/dav/addressbooks/5/alice.vcf", nil)
		req = req.WithContext(auth.WithUser(req.Context(), user))

		granted, err := h.checkACLPrivilege(req.Context(), user, "/dav/addressbooks/5/alice.vcf", "read")
		if err != nil {
			t.Fatalf("checkACLPrivilege() error = %v", err)
		}
		if granted {
			t.Fatal("RFC 6352 Section 3: deny ACEs must override broader grants for the same resource")
		}
	})

	t.Run("Section3_ACLReadGrantAllowsGETOnAddressObject", func(t *testing.T) {
		user := &store.User{ID: 2, PrimaryEmail: "reader@example.com"}
		bookRepo := &fakeAddressBookRepo{
			books: map[int64]*store.AddressBook{
				5: {ID: 5, UserID: 1, Name: "Shared Contacts"},
			},
		}
		contactRepo := &fakeContactRepo{
			contacts: map[string]*store.Contact{
				"5:alice": {AddressBookID: 5, UID: "alice", ResourceName: "alice", RawVCard: buildVCard("3.0", "UID:alice", "FN:Alice Example"), ETag: "etag-alice"},
			},
		}
		aclRepo := &fakeACLRepo{
			entries: []store.ACLEntry{
				{ResourcePath: "/dav/addressbooks/5/alice.vcf", PrincipalHref: "/dav/principals/2/", IsGrant: true, Privilege: "read"},
			},
		}
		h := &Handler{store: &store.Store{AddressBooks: bookRepo, Contacts: contactRepo, ACLEntries: aclRepo}}

		req := httptest.NewRequest(http.MethodGet, "/dav/addressbooks/5/alice.vcf", nil)
		req = req.WithContext(auth.WithUser(req.Context(), user))
		rr := httptest.NewRecorder()

		h.Get(rr, req)

		if rr.Code != http.StatusOK {
			t.Fatalf("RFC 6352 Sections 3 and 7: read ACLs must be enforced by GET handlers, got %d: %s", rr.Code, rr.Body.String())
		}
	})

	t.Run("Section3_ACLReadGrantAllowsPROPFINDOnSharedAddressBook", func(t *testing.T) {
		user := &store.User{ID: 2, PrimaryEmail: "reader@example.com"}
		bookRepo := &fakeAddressBookRepo{
			books: map[int64]*store.AddressBook{
				5: {ID: 5, UserID: 1, Name: "Shared Contacts"},
			},
		}
		aclRepo := &fakeACLRepo{
			entries: []store.ACLEntry{
				{ResourcePath: "/dav/addressbooks/5", PrincipalHref: "/dav/principals/2/", IsGrant: true, Privilege: "read"},
			},
		}
		h := &Handler{store: &store.Store{AddressBooks: bookRepo, Contacts: &fakeContactRepo{}, ACLEntries: aclRepo}}

		body := `<?xml version="1.0" encoding="utf-8"?>
<d:propfind xmlns:d="DAV:">
  <d:prop>
    <d:resourcetype/>
  </d:prop>
</d:propfind>`
		req := httptest.NewRequest("PROPFIND", "/dav/addressbooks/5/", strings.NewReader(body))
		req.Header.Set("Depth", "0")
		req = req.WithContext(auth.WithUser(req.Context(), user))
		rr := httptest.NewRecorder()

		h.Propfind(rr, req)

		if rr.Code != http.StatusMultiStatus {
			t.Fatalf("RFC 6352 Sections 3, 5.2 and 7: shared address books with read ACLs must be discoverable via PROPFIND, got %d: %s", rr.Code, rr.Body.String())
		}
	})

	t.Run("Section3_ACLDenyBlocksGETOnAddressObject", func(t *testing.T) {
		user := &store.User{ID: 2, PrimaryEmail: "reader@example.com"}
		bookRepo := &fakeAddressBookRepo{
			books: map[int64]*store.AddressBook{
				5: {ID: 5, UserID: 1, Name: "Shared Contacts"},
			},
		}
		contactRepo := &fakeContactRepo{
			contacts: map[string]*store.Contact{
				"5:alice": {AddressBookID: 5, UID: "alice", ResourceName: "alice", RawVCard: buildVCard("3.0", "UID:alice", "FN:Alice Example"), ETag: "etag-alice"},
			},
		}
		aclRepo := &fakeACLRepo{
			entries: []store.ACLEntry{
				{ResourcePath: "/dav/addressbooks/5", PrincipalHref: "DAV:authenticated", IsGrant: true, Privilege: "read"},
				{ResourcePath: "/dav/addressbooks/5/alice.vcf", PrincipalHref: "/dav/principals/2/", IsGrant: false, Privilege: "read"},
			},
		}
		h := &Handler{store: &store.Store{AddressBooks: bookRepo, Contacts: contactRepo, ACLEntries: aclRepo}}

		req := httptest.NewRequest(http.MethodGet, "/dav/addressbooks/5/alice.vcf", nil)
		req = req.WithContext(auth.WithUser(req.Context(), user))
		rr := httptest.NewRecorder()

		h.Get(rr, req)

		if rr.Code != http.StatusForbidden {
			t.Fatalf("RFC 6352 Sections 3 and 7: deny ACEs must be enforced by GET handlers, got %d: %s", rr.Code, rr.Body.String())
		}
	})

	t.Run("Section3_ACLWriteGrantAllowsPUTIntoSharedAddressBook", func(t *testing.T) {
		user := &store.User{ID: 2, PrimaryEmail: "writer@example.com"}
		bookRepo := &fakeAddressBookRepo{
			books: map[int64]*store.AddressBook{
				5: {ID: 5, UserID: 1, Name: "Shared Contacts"},
			},
		}
		contactRepo := &fakeContactRepo{contacts: map[string]*store.Contact{}}
		aclRepo := &fakeACLRepo{
			entries: []store.ACLEntry{
				{ResourcePath: "/dav/addressbooks/5", PrincipalHref: "/dav/principals/2/", IsGrant: true, Privilege: "bind"},
			},
		}
		h := &Handler{store: &store.Store{AddressBooks: bookRepo, Contacts: contactRepo, ACLEntries: aclRepo}}
		vcard := buildVCard("3.0", "UID:alice", "FN:Alice Example")

		req := newAddressBookPutRequest("/dav/addressbooks/5/alice.vcf", strings.NewReader(vcard))
		req = req.WithContext(auth.WithUser(req.Context(), user))
		rr := httptest.NewRecorder()

		h.Put(rr, req)

		if rr.Code != http.StatusCreated {
			t.Fatalf("RFC 6352 Sections 3 and 7: collection bind privileges must permit shared writes, got %d: %s", rr.Code, rr.Body.String())
		}
	})

	t.Run("Section3_ACLWritePropertiesGrantAllowsPROPPATCHOnSharedAddressBook", func(t *testing.T) {
		user := &store.User{ID: 2, PrimaryEmail: "editor@example.com"}
		description := "Shared Contacts"
		bookRepo := &fakeAddressBookRepo{
			books: map[int64]*store.AddressBook{
				5: {ID: 5, UserID: 1, Name: "Shared Contacts", Description: &description},
			},
		}
		aclRepo := &fakeACLRepo{
			entries: []store.ACLEntry{
				{ResourcePath: "/dav/addressbooks/5", PrincipalHref: "/dav/principals/2/", IsGrant: true, Privilege: "read"},
				{ResourcePath: "/dav/addressbooks/5", PrincipalHref: "/dav/principals/2/", IsGrant: true, Privilege: "write-properties"},
			},
		}
		h := &Handler{store: &store.Store{AddressBooks: bookRepo, Contacts: &fakeContactRepo{}, ACLEntries: aclRepo}}

		body := `<?xml version="1.0" encoding="utf-8"?>
<D:propertyupdate xmlns:D="DAV:" xmlns:C="urn:ietf:params:xml:ns:carddav">
  <D:set>
    <D:prop>
      <C:addressbook-description>Updated by delegate</C:addressbook-description>
    </D:prop>
  </D:set>
</D:propertyupdate>`

		req := httptest.NewRequest("PROPPATCH", "/dav/addressbooks/5", strings.NewReader(body))
		req = req.WithContext(auth.WithUser(req.Context(), user))
		rr := httptest.NewRecorder()

		h.Proppatch(rr, req)

		if rr.Code != http.StatusMultiStatus {
			t.Fatalf("RFC 6352 Sections 3, 6.2.1, and 7: write-properties ACLs must permit shared collection property updates, got %d: %s", rr.Code, rr.Body.String())
		}
		if !propstatHasStatus(rr.Body.String(), "addressbook-description", http.StatusOK) {
			t.Fatalf("RFC 6352 Sections 3, 6.2.1, and 7: mutable address book properties should be writable with DAV:write-properties, got %s", rr.Body.String())
		}
		updatedBook, err := bookRepo.GetByID(req.Context(), 5)
		if err != nil {
			t.Fatalf("GetByID() error = %v", err)
		}
		if updatedBook == nil || updatedBook.Description == nil || *updatedBook.Description != "Updated by delegate" {
			t.Fatalf("RFC 6352 Sections 3, 6.2.1, and 7: delegated PROPPATCH must persist the updated description, got %#v", updatedBook)
		}
	})

	t.Run("Section3_AddressBookCollectionOwnersCanManageACLs", func(t *testing.T) {
		user := &store.User{ID: 1, PrimaryEmail: "owner@example.com"}
		bookRepo := &fakeAddressBookRepo{
			books: map[int64]*store.AddressBook{
				5: {ID: 5, UserID: 1, Name: "Shared Contacts"},
			},
		}
		aclRepo := &fakeACLRepo{}
		h := &Handler{store: &store.Store{AddressBooks: bookRepo, ACLEntries: aclRepo}}

		body := `<?xml version="1.0" encoding="utf-8"?>
<D:acl xmlns:D="DAV:">
  <D:ace>
    <D:principal><D:authenticated/></D:principal>
    <D:grant>
      <D:privilege><D:read/></D:privilege>
    </D:grant>
  </D:ace>
</D:acl>`

		req := httptest.NewRequest("ACL", "/dav/addressbooks/5/", strings.NewReader(body))
		req = req.WithContext(auth.WithUser(req.Context(), user))
		rr := httptest.NewRecorder()

		h.Acl(rr, req)

		if rr.Code != http.StatusOK {
			t.Fatalf("RFC 6352 Sections 3 and 7: address book owners must be able to manage ACLs on address book collections, got %d: %s", rr.Code, rr.Body.String())
		}
		entries, err := aclRepo.ListByResource(req.Context(), "/dav/addressbooks/5")
		if err != nil {
			t.Fatalf("ListByResource() error = %v", err)
		}
		if len(entries) != 1 || entries[0].PrincipalHref != "DAV:authenticated" || entries[0].Privilege != "read" || !entries[0].IsGrant {
			t.Fatalf("RFC 6352 Sections 3 and 7: ACL request should persist the requested ACEs, got %#v", entries)
		}
	})

	t.Run("Section3_ExtendedMKCOLShouldBeAdvertisedWhenSupported", func(t *testing.T) {
		h := NewHandler(&config.Config{}, &store.Store{})
		req := httptest.NewRequest(http.MethodOptions, "/dav/addressbooks/", nil)
		rr := httptest.NewRecorder()

		h.Options(rr, req)

		if !strings.Contains(rr.Header().Get("DAV"), "extended-mkcol") {
			t.Errorf("RFC 6352 Section 3: CardDAV servers SHOULD support extended MKCOL and advertise extended-mkcol when they do")
		}
	})

	t.Run("Section3_CurrentUserPrincipalDiscovery", func(t *testing.T) {
		h := &Handler{}
		user := &store.User{ID: 1, PrimaryEmail: "user@example.com"}

		body := `<?xml version="1.0" encoding="utf-8"?>
<d:propfind xmlns:d="DAV:">
  <d:prop>
    <d:current-user-principal/>
  </d:prop>
</d:propfind>`

		req := httptest.NewRequest("PROPFIND", "/dav/", strings.NewReader(body))
		req.Header.Set("Depth", "0")
		req = req.WithContext(auth.WithUser(req.Context(), user))
		rr := httptest.NewRecorder()

		h.Propfind(rr, req)

		respBody := rr.Body.String()
		if !strings.Contains(respBody, "current-user-principal") {
			t.Errorf("RFC 6352 Section 3 and RFC 5397: server SHOULD expose current-user-principal for client discovery")
		}
		if !strings.Contains(respBody, "/dav/principals/1/") {
			t.Errorf("RFC 6352 Section 3 and RFC 5397: current-user-principal should identify the authenticated principal, got %s", respBody)
		}
	})

	t.Run("Section3_CurrentUserPrincipalURLDiscovery", func(t *testing.T) {
		h := &Handler{}
		user := &store.User{ID: 1, PrimaryEmail: "user@example.com"}

		body := `<?xml version="1.0" encoding="utf-8"?>
<d:propfind xmlns:d="DAV:">
  <d:prop>
    <d:current-user-principal-URL/>
  </d:prop>
</d:propfind>`

		req := httptest.NewRequest("PROPFIND", "/dav/", strings.NewReader(body))
		req.Header.Set("Depth", "0")
		req = req.WithContext(auth.WithUser(req.Context(), user))
		rr := httptest.NewRecorder()

		h.Propfind(rr, req)

		respBody := rr.Body.String()
		if !strings.Contains(respBody, "current-user-principal-URL") {
			t.Errorf("RFC 6352 Section 3 and RFC 5397: server SHOULD expose current-user-principal-URL for fast principal discovery, got %s", respBody)
		}
		if !strings.Contains(respBody, "/dav/principals/1/") {
			t.Errorf("RFC 6352 Section 3 and RFC 5397: current-user-principal-URL should identify the authenticated principal, got %s", respBody)
		}
	})

	t.Run("Section3_TLSRequirementIsDeploymentLevel", func(t *testing.T) {
		t.Skip("RFC 6352 Section 3 TLS support must be validated in integration or deployment tests, not this in-process handler suite")
	})
}

func TestRFC6352_DeploymentAndSecurityRequirements(t *testing.T) {
	t.Run("Section3_SecureTransportIsDeploymentLevel", func(t *testing.T) {
		t.Skip("RFC 6352 Section 3 secure transport and certificate validation must be verified in end-to-end TLS deployment tests")
	})

	t.Run("Section13_BasicAuthWithoutTLSIsDeploymentLevel", func(t *testing.T) {
		t.Skip("RFC 6352 Section 13 Basic authentication over TLS requirements are deployment- and auth-stack-level, not unit-testable in this handler suite")
	})

	t.Run("Section13_NewlyProvisionedAddressBooksDefaultToPrivate", func(t *testing.T) {
		owner := &store.User{ID: 1, PrimaryEmail: "owner@example.com"}
		reader := &store.User{ID: 2, PrimaryEmail: "reader@example.com"}
		bookRepo := &fakeAddressBookRepo{}
		contactRepo := &fakeContactRepo{contacts: map[string]*store.Contact{}}
		aclRepo := &fakeACLRepo{}
		h := &Handler{store: &store.Store{AddressBooks: bookRepo, Contacts: contactRepo, ACLEntries: aclRepo}}

		mkcolReq := httptest.NewRequest("MKCOL", "/dav/addressbooks/ProvisionedBook", nil)
		mkcolReq = mkcolReq.WithContext(auth.WithUser(mkcolReq.Context(), owner))
		mkcolRR := httptest.NewRecorder()
		h.Mkcol(mkcolRR, mkcolReq)
		if mkcolRR.Code != http.StatusCreated {
			t.Fatalf("RFC 6352 Section 13: provisioning a new address book should succeed, got %d: %s", mkcolRR.Code, mkcolRR.Body.String())
		}

		putReq := newAddressBookPutRequest("/dav/addressbooks/ProvisionedBook/alice.vcf", strings.NewReader(buildVCard("3.0", "UID:alice", "FN:Alice Example")))
		putReq = putReq.WithContext(auth.WithUser(putReq.Context(), owner))
		putRR := httptest.NewRecorder()
		h.Put(putRR, putReq)
		if putRR.Code != http.StatusCreated {
			t.Fatalf("RFC 6352 Section 13: owner should be able to populate a newly provisioned private address book, got %d: %s", putRR.Code, putRR.Body.String())
		}

		authenticatedReq := httptest.NewRequest(http.MethodGet, "/dav/addressbooks/1/alice.vcf", nil)
		authenticatedReq = authenticatedReq.WithContext(auth.WithUser(authenticatedReq.Context(), reader))
		authenticatedRR := httptest.NewRecorder()
		h.Get(authenticatedRR, authenticatedReq)
		if authenticatedRR.Code != http.StatusNotFound {
			t.Fatalf("RFC 6352 Section 13: newly provisioned private address books must not be readable by other authenticated users, got %d: %s", authenticatedRR.Code, authenticatedRR.Body.String())
		}

		unauthenticatedReq := httptest.NewRequest(http.MethodGet, "/dav/addressbooks/1/alice.vcf", nil)
		unauthenticatedRR := httptest.NewRecorder()
		h.Get(unauthenticatedRR, unauthenticatedReq)
		if unauthenticatedRR.Code != http.StatusUnauthorized {
			t.Fatalf("RFC 6352 Section 13: newly provisioned private address books must not be readable by unauthenticated users, got %d: %s", unauthenticatedRR.Code, unauthenticatedRR.Body.String())
		}

		entries, err := aclRepo.ListByResource(context.Background(), "/dav/addressbooks/1")
		if err != nil {
			t.Fatalf("ListByResource() error = %v", err)
		}
		if len(entries) != 0 {
			t.Fatalf("RFC 6352 Section 13: provisioning a private address book should not create broad ACL grants, got %#v", entries)
		}
	})

	t.Run("Section13_ClientWarningsAreClientSide", func(t *testing.T) {
		t.Skip("RFC 6352 Section 13 client warning requirements apply to CardDAV clients rather than this server package")
	})

}

func TestRFC6352_AddressBookSecuritySemantics(t *testing.T) {
	reader := &store.User{ID: 2, PrimaryEmail: "reader@example.com"}
	now := store.Now()
	bookRepo := &fakeAddressBookRepo{
		books: map[int64]*store.AddressBook{
			5: {ID: 5, UserID: 1, Name: "Contacts", UpdatedAt: now, CTag: 1},
		},
	}
	contactRepo := &fakeContactRepo{
		contacts: map[string]*store.Contact{
			"5:alice": {AddressBookID: 5, UID: "alice", ResourceName: "alice", RawVCard: buildVCard("3.0", "UID:alice", "FN:Alice Example"), ETag: "etag-a", LastModified: now},
		},
	}

	t.Run("Section13_PrivateAddressBooksAreNotReadableByOtherAuthenticatedUsers", func(t *testing.T) {
		h := &Handler{store: &store.Store{AddressBooks: bookRepo, Contacts: contactRepo}}

		req := httptest.NewRequest(http.MethodGet, "/dav/addressbooks/5/alice.vcf", nil)
		req = req.WithContext(auth.WithUser(req.Context(), reader))
		rr := httptest.NewRecorder()
		h.Get(rr, req)

		if rr.Code != http.StatusNotFound {
			t.Fatalf("RFC 6352 Section 13: private address books should only be readable by the owner unless shared, got %d: %s", rr.Code, rr.Body.String())
		}
	})

	t.Run("Section13_SharingAllowsSpecifiedAuthenticatedReaders", func(t *testing.T) {
		aclRepo := &fakeACLRepo{
			entries: []store.ACLEntry{
				{ResourcePath: "/dav/addressbooks/5", PrincipalHref: "/dav/principals/2/", IsGrant: true, Privilege: "read"},
			},
		}
		h := &Handler{store: &store.Store{AddressBooks: bookRepo, Contacts: contactRepo, ACLEntries: aclRepo}}

		req := httptest.NewRequest(http.MethodGet, "/dav/addressbooks/5/alice.vcf", nil)
		req = req.WithContext(auth.WithUser(req.Context(), reader))
		rr := httptest.NewRecorder()
		h.Get(rr, req)

		if rr.Code != http.StatusOK {
			t.Fatalf("RFC 6352 Section 13: shared address books should be readable by explicitly granted authenticated users, got %d: %s", rr.Code, rr.Body.String())
		}
	})

	t.Run("Section13_PrivateAndSharedAddressBooksRejectUnauthenticatedReaders", func(t *testing.T) {
		aclRepo := &fakeACLRepo{
			entries: []store.ACLEntry{
				{ResourcePath: "/dav/addressbooks/5", PrincipalHref: "/dav/principals/2/", IsGrant: true, Privilege: "read"},
			},
		}
		h := &Handler{store: &store.Store{AddressBooks: bookRepo, Contacts: contactRepo, ACLEntries: aclRepo}}

		req := httptest.NewRequest(http.MethodGet, "/dav/addressbooks/5/alice.vcf", nil)
		rr := httptest.NewRecorder()
		h.Get(rr, req)

		if rr.Code != http.StatusUnauthorized {
			t.Fatalf("RFC 6352 Section 13: private and shared address books MUST NOT be accessible by unauthenticated users, got %d: %s", rr.Code, rr.Body.String())
		}
	})

	t.Run("Section13_PublicAddressBooksAllowAuthenticatedReaders", func(t *testing.T) {
		aclRepo := &fakeACLRepo{
			entries: []store.ACLEntry{
				{ResourcePath: "/dav/addressbooks/5", PrincipalHref: "DAV:all", IsGrant: true, Privilege: "read"},
			},
		}
		h := &Handler{store: &store.Store{AddressBooks: bookRepo, Contacts: contactRepo, ACLEntries: aclRepo}}

		req := httptest.NewRequest(http.MethodGet, "/dav/addressbooks/5/alice.vcf", nil)
		req = req.WithContext(auth.WithUser(req.Context(), reader))
		rr := httptest.NewRecorder()
		h.Get(rr, req)

		if rr.Code != http.StatusOK {
			t.Fatalf("RFC 6352 Section 13: public address books should be readable by authenticated users, got %d: %s", rr.Code, rr.Body.String())
		}
	})

	t.Run("Section13_DAVSurfaceRejectsUnauthenticatedRequestsEvenForPublicACLs", func(t *testing.T) {
		aclRepo := &fakeACLRepo{
			entries: []store.ACLEntry{
				{ResourcePath: "/dav/addressbooks/5", PrincipalHref: "DAV:all", IsGrant: true, Privilege: "read"},
			},
		}
		h := &Handler{store: &store.Store{AddressBooks: bookRepo, Contacts: contactRepo, ACLEntries: aclRepo}}

		req := httptest.NewRequest(http.MethodGet, "/dav/addressbooks/5/alice.vcf", nil)
		rr := httptest.NewRecorder()
		h.Get(rr, req)

		if rr.Code != http.StatusUnauthorized {
			t.Fatalf("Server policy: the DAV surface remains authenticated even when DAV:all grants read, got %d: %s", rr.Code, rr.Body.String())
		}
	})
}

// RFC 6352 Section 7.1.1 and practical client bootstrapping:
// root -> principal -> addressbook-home-set -> address book listings.
func TestRFC6352_AddressBookDiscoverySequence(t *testing.T) {
	now := store.Now()
	desc1 := "Work contacts"
	desc2 := "Family contacts"
	bookRepo := &fakeAddressBookRepo{
		books: map[int64]*store.AddressBook{
			1: {ID: 1, UserID: 1, Name: "Work", Description: &desc1, UpdatedAt: now, CTag: 10},
			2: {ID: 2, UserID: 1, Name: "Family", Description: &desc2, UpdatedAt: now, CTag: 20},
		},
	}
	h := &Handler{store: &store.Store{AddressBooks: bookRepo, Contacts: &fakeContactRepo{}}}
	user := &store.User{ID: 1, PrimaryEmail: "user@example.com"}

	t.Run("Step1_DiscoverCurrentUserPrincipal", func(t *testing.T) {
		body := `<?xml version="1.0" encoding="utf-8"?>
<d:propfind xmlns:d="DAV:">
  <d:prop>
    <d:current-user-principal/>
  </d:prop>
</d:propfind>`

		req := httptest.NewRequest("PROPFIND", "/dav/", strings.NewReader(body))
		req.Header.Set("Depth", "0")
		req = req.WithContext(auth.WithUser(req.Context(), user))
		rr := httptest.NewRecorder()

		h.Propfind(rr, req)

		respBody := rr.Body.String()
		if !strings.Contains(respBody, "current-user-principal") {
			t.Fatal("RFC 6352 Section 3 and RFC 5397: current-user-principal not found on DAV root")
		}
		if !strings.Contains(respBody, "/dav/principals/1/") {
			t.Fatal("RFC 6352 Section 3 and RFC 5397: principal URL not found in current-user-principal property")
		}
	})

	t.Run("Step2_DiscoverAddressBookHomeSet", func(t *testing.T) {
		body := `<?xml version="1.0" encoding="utf-8"?>
<d:propfind xmlns:d="DAV:" xmlns:card="urn:ietf:params:xml:ns:carddav">
  <d:prop>
    <card:addressbook-home-set/>
  </d:prop>
</d:propfind>`

		req := httptest.NewRequest("PROPFIND", "/dav/principals/1/", strings.NewReader(body))
		req.Header.Set("Depth", "0")
		req = req.WithContext(auth.WithUser(req.Context(), user))
		rr := httptest.NewRecorder()

		h.Propfind(rr, req)

		respBody := rr.Body.String()
		if !strings.Contains(respBody, "addressbook-home-set") {
			t.Fatal("RFC 6352 Section 7.1.1: addressbook-home-set property not found on principal")
		}
		if !strings.Contains(respBody, "/dav/addressbooks/") {
			t.Fatal("RFC 6352 Section 7.1.1: addressbook-home-set must identify the user's address book home")
		}
	})

	t.Run("Step3_ListAddressBooks", func(t *testing.T) {
		body := `<?xml version="1.0" encoding="utf-8"?>
<d:propfind xmlns:d="DAV:" xmlns:card="urn:ietf:params:xml:ns:carddav">
  <d:prop>
    <d:displayname/>
    <d:resourcetype/>
    <card:addressbook-description/>
    <d:supported-report-set/>
  </d:prop>
</d:propfind>`

		req := httptest.NewRequest("PROPFIND", "/dav/addressbooks/", strings.NewReader(body))
		req.Header.Set("Depth", "1")
		req = req.WithContext(auth.WithUser(req.Context(), user))
		rr := httptest.NewRecorder()

		h.Propfind(rr, req)

		if rr.Code != http.StatusMultiStatus {
			t.Fatalf("RFC 6352 Section 7.1.1: PROPFIND on addressbook-home-set must return 207, got %d", rr.Code)
		}

		respBody := rr.Body.String()
		if !strings.Contains(respBody, "/dav/addressbooks/1/") {
			t.Error("RFC 6352 Section 7.1.1: first address book was not discoverable")
		}
		if !strings.Contains(respBody, "/dav/addressbooks/2/") {
			t.Error("RFC 6352 Section 7.1.1: second address book was not discoverable")
		}
		if strings.Count(respBody, "<card:addressbook") < 2 {
			t.Errorf("RFC 6352 Section 5.2: expected each listed collection to advertise card:addressbook resource type, got %s", respBody)
		}
		if !strings.Contains(respBody, "Work") || !strings.Contains(respBody, "Family") {
			t.Errorf("RFC 6352 Sections 6.2.1 and 7.1.1: listed address books should include human-readable names, got %s", respBody)
		}
	})

	t.Run("Step4_AccessIndividualAddressBook", func(t *testing.T) {
		req := httptest.NewRequest("PROPFIND", "/dav/addressbooks/1/", nil)
		req.Header.Set("Depth", "0")
		req = req.WithContext(auth.WithUser(req.Context(), user))
		rr := httptest.NewRecorder()

		h.Propfind(rr, req)

		if rr.Code != http.StatusMultiStatus {
			t.Fatalf("RFC 6352 Section 5.2: PROPFIND on an address book collection must return 207, got %d", rr.Code)
		}
		if !strings.Contains(rr.Body.String(), "/dav/addressbooks/1/") {
			t.Errorf("RFC 6352 Section 5.2: collection href missing from PROPFIND response, got %s", rr.Body.String())
		}
	})
}

func TestRFC6352_AddressBookCollectionProperties(t *testing.T) {
	now := store.Now()
	description := "Primary contacts"
	bookRepo := &fakeAddressBookRepo{
		books: map[int64]*store.AddressBook{
			5: {ID: 5, UserID: 1, Name: "Contacts", Description: &description, UpdatedAt: now, CTag: 9},
		},
	}
	h := &Handler{store: &store.Store{AddressBooks: bookRepo, Contacts: &fakeContactRepo{}}}
	user := &store.User{ID: 1, PrimaryEmail: "user@example.com"}

	t.Run("Section5_2_ResourceType", func(t *testing.T) {
		body := `<?xml version="1.0" encoding="utf-8"?>
<d:propfind xmlns:d="DAV:">
  <d:prop>
    <d:resourcetype/>
  </d:prop>
</d:propfind>`

		req := httptest.NewRequest("PROPFIND", "/dav/addressbooks/5/", strings.NewReader(body))
		req.Header.Set("Depth", "0")
		req = req.WithContext(auth.WithUser(req.Context(), user))
		rr := httptest.NewRecorder()

		h.Propfind(rr, req)

		respBody := rr.Body.String()
		if !strings.Contains(respBody, "<d:collection") {
			t.Errorf("RFC 6352 Section 5.2: address book collections MUST advertise d:collection, got %s", respBody)
		}
		if !strings.Contains(respBody, "<card:addressbook") {
			t.Errorf("RFC 6352 Section 5.2: address book collections MUST advertise card:addressbook, got %s", respBody)
		}
	})

	t.Run("Section6_2_1_AddressBookDescriptionExplicit", func(t *testing.T) {
		body := `<?xml version="1.0" encoding="utf-8"?>
<d:propfind xmlns:d="DAV:" xmlns:card="urn:ietf:params:xml:ns:carddav">
  <d:prop>
    <card:addressbook-description/>
  </d:prop>
</d:propfind>`

		req := httptest.NewRequest("PROPFIND", "/dav/addressbooks/5/", strings.NewReader(body))
		req.Header.Set("Depth", "0")
		req = req.WithContext(auth.WithUser(req.Context(), user))
		rr := httptest.NewRecorder()

		h.Propfind(rr, req)

		respBody := rr.Body.String()
		if !strings.Contains(respBody, "addressbook-description") {
			t.Errorf("RFC 6352 Section 6.2.1: addressbook-description property should be exposed when requested, got %s", respBody)
		}
		if !strings.Contains(respBody, description) {
			t.Errorf("RFC 6352 Section 6.2.1: addressbook-description value not returned, got %s", respBody)
		}
	})

	t.Run("Section6_2_1_AddressBookDescriptionShouldBeMutable", func(t *testing.T) {
		body := `<?xml version="1.0" encoding="utf-8"?>
<D:propertyupdate xmlns:D="DAV:" xmlns:C="urn:ietf:params:xml:ns:carddav">
  <D:set>
    <D:prop>
      <C:addressbook-description>Updated description</C:addressbook-description>
    </D:prop>
  </D:set>
</D:propertyupdate>`

		req := httptest.NewRequest("PROPPATCH", "/dav/addressbooks/5", strings.NewReader(body))
		req = req.WithContext(auth.WithUser(req.Context(), user))
		rr := httptest.NewRecorder()

		h.Proppatch(rr, req)

		if rr.Code != http.StatusMultiStatus {
			t.Fatalf("RFC 6352 Section 6.2.1: PROPPATCH should return 207 Multi-Status, got %d", rr.Code)
		}
		if !propstatHasStatus(rr.Body.String(), "addressbook-description", http.StatusOK) {
			t.Errorf("RFC 6352 Section 6.2.1: addressbook-description SHOULD NOT be protected against user updates, got %s", rr.Body.String())
		}

		// Verify the update actually persisted by doing a follow-up PROPFIND
		propfindBody := `<?xml version="1.0" encoding="utf-8"?>
<d:propfind xmlns:d="DAV:" xmlns:card="urn:ietf:params:xml:ns:carddav">
  <d:prop>
    <card:addressbook-description/>
  </d:prop>
</d:propfind>`
		req2 := httptest.NewRequest("PROPFIND", "/dav/addressbooks/5/", strings.NewReader(propfindBody))
		req2.Header.Set("Depth", "0")
		req2 = req2.WithContext(auth.WithUser(req2.Context(), user))
		rr2 := httptest.NewRecorder()
		h.Propfind(rr2, req2)

		if !strings.Contains(rr2.Body.String(), "Updated description") {
			t.Errorf("RFC 6352 Section 6.2.1: PROPPATCH on addressbook-description must persist the change; follow-up PROPFIND got %s", rr2.Body.String())
		}
	})

	t.Run("Section6_2_1_AddressBookDescriptionNotInAllprop", func(t *testing.T) {
		req := httptest.NewRequest("PROPFIND", "/dav/addressbooks/5/", nil)
		req.Header.Set("Depth", "0")
		req = req.WithContext(auth.WithUser(req.Context(), user))
		rr := httptest.NewRecorder()

		h.Propfind(rr, req)

		if strings.Contains(rr.Body.String(), "addressbook-description") {
			t.Errorf("RFC 6352 Section 6.2.1: addressbook-description SHOULD NOT be returned by DAV:allprop, got %s", rr.Body.String())
		}
	})

	t.Run("Section6_2_2_SupportedAddressDataProperty", func(t *testing.T) {
		body := `<?xml version="1.0" encoding="utf-8"?>
<d:propfind xmlns:d="DAV:" xmlns:card="urn:ietf:params:xml:ns:carddav">
  <d:prop>
    <card:supported-address-data/>
  </d:prop>
</d:propfind>`

		req := httptest.NewRequest("PROPFIND", "/dav/addressbooks/5/", strings.NewReader(body))
		req.Header.Set("Depth", "0")
		req = req.WithContext(auth.WithUser(req.Context(), user))
		rr := httptest.NewRecorder()

		h.Propfind(rr, req)

		respBody := rr.Body.String()
		if !strings.Contains(respBody, "supported-address-data") {
			t.Errorf("RFC 6352 Section 6.2.2: address book collections MUST advertise supported-address-data, got %s", respBody)
		}
		if !strings.Contains(respBody, `content-type="text/vcard"`) {
			t.Errorf("RFC 6352 Section 6.2.2: server MUST advertise text/vcard support or default to it explicitly, got %s", respBody)
		}
		if !strings.Contains(respBody, `version="3.0"`) {
			t.Errorf("RFC 6352 Section 6.2.2: server MUST advertise or default to vCard 3.0 support, got %s", respBody)
		}
	})

	t.Run("Section6_2_2_SupportedAddressDataNotInAllprop", func(t *testing.T) {
		req := httptest.NewRequest("PROPFIND", "/dav/addressbooks/5/", nil)
		req.Header.Set("Depth", "0")
		req = req.WithContext(auth.WithUser(req.Context(), user))
		rr := httptest.NewRecorder()

		h.Propfind(rr, req)

		if strings.Contains(rr.Body.String(), "supported-address-data") {
			t.Errorf("RFC 6352 Section 6.2.2: supported-address-data SHOULD NOT be returned by DAV:allprop, got %s", rr.Body.String())
		}
	})

	t.Run("Section6_2_3_MaxResourceSizeProperty", func(t *testing.T) {
		body := `<?xml version="1.0" encoding="utf-8"?>
<d:propfind xmlns:d="DAV:" xmlns:card="urn:ietf:params:xml:ns:carddav">
  <d:prop>
    <card:max-resource-size/>
  </d:prop>
</d:propfind>`

		req := httptest.NewRequest("PROPFIND", "/dav/addressbooks/5/", strings.NewReader(body))
		req.Header.Set("Depth", "0")
		req = req.WithContext(auth.WithUser(req.Context(), user))
		rr := httptest.NewRecorder()

		h.Propfind(rr, req)

		respBody := rr.Body.String()
		if !strings.Contains(respBody, "max-resource-size") {
			t.Errorf("RFC 6352 Section 6.2.3: address book collections SHOULD advertise max-resource-size, got %s", respBody)
		}
		if strings.Contains(respBody, "max-resource-size") {
			if _, ok := extractPropInt(respBody, "max-resource-size"); !ok {
				t.Errorf("RFC 6352 Section 6.2.3: max-resource-size value must be numeric, got %s", respBody)
			}
		}
	})

	t.Run("Section6_2_3_MaxResourceSizeNotInAllprop", func(t *testing.T) {
		req := httptest.NewRequest("PROPFIND", "/dav/addressbooks/5/", nil)
		req.Header.Set("Depth", "0")
		req = req.WithContext(auth.WithUser(req.Context(), user))
		rr := httptest.NewRecorder()

		h.Propfind(rr, req)

		if strings.Contains(rr.Body.String(), "max-resource-size") {
			t.Errorf("RFC 6352 Section 6.2.3: max-resource-size SHOULD NOT be returned by DAV:allprop, got %s", rr.Body.String())
		}
	})

	t.Run("Section8_SupportedReportSetOnCollection", func(t *testing.T) {
		body := `<?xml version="1.0" encoding="utf-8"?>
<d:propfind xmlns:d="DAV:">
  <d:prop>
    <d:supported-report-set/>
  </d:prop>
</d:propfind>`

		req := httptest.NewRequest("PROPFIND", "/dav/addressbooks/5/", strings.NewReader(body))
		req.Header.Set("Depth", "0")
		req = req.WithContext(auth.WithUser(req.Context(), user))
		rr := httptest.NewRecorder()

		h.Propfind(rr, req)

		respBody := rr.Body.String()
		for _, reportName := range []string{"addressbook-query", "addressbook-multiget", "expand-property"} {
			if !strings.Contains(respBody, reportName) {
				t.Errorf("RFC 6352 Section 8: address book collections MUST advertise %s in supported-report-set, got %s", reportName, respBody)
			}
		}
	})

	t.Run("Section8_3_1_SupportedCollationSetProperty", func(t *testing.T) {
		body := `<?xml version="1.0" encoding="utf-8"?>
<d:propfind xmlns:d="DAV:" xmlns:card="urn:ietf:params:xml:ns:carddav">
  <d:prop>
    <card:supported-collation-set/>
  </d:prop>
</d:propfind>`

		req := httptest.NewRequest("PROPFIND", "/dav/addressbooks/5/", strings.NewReader(body))
		req.Header.Set("Depth", "0")
		req = req.WithContext(auth.WithUser(req.Context(), user))
		rr := httptest.NewRecorder()

		h.Propfind(rr, req)

		respBody := rr.Body.String()
		if !strings.Contains(respBody, "supported-collation-set") {
			t.Errorf("RFC 6352 Sections 8.3 and 8.3.1: resources supporting text-match queries MUST advertise supported-collation-set, got %s", respBody)
		}
		if !strings.Contains(respBody, "i;ascii-casemap") {
			t.Errorf("RFC 6352 Section 8.3.1: supported-collation-set MUST include i;ascii-casemap, got %s", respBody)
		}
		if !strings.Contains(respBody, "i;unicode-casemap") {
			t.Errorf("RFC 6352 Section 8.3.1: supported-collation-set MUST include i;unicode-casemap, got %s", respBody)
		}
	})

	t.Run("Section8_3_1_SupportedCollationSetNotInAllprop", func(t *testing.T) {
		req := httptest.NewRequest("PROPFIND", "/dav/addressbooks/5/", nil)
		req.Header.Set("Depth", "0")
		req = req.WithContext(auth.WithUser(req.Context(), user))
		rr := httptest.NewRecorder()

		h.Propfind(rr, req)

		if strings.Contains(rr.Body.String(), "supported-collation-set") {
			t.Errorf("RFC 6352 Section 8.3.1: supported-collation-set SHOULD NOT be returned by DAV:allprop, got %s", rr.Body.String())
		}
	})
}

func TestRFC6352_PrincipalProperties(t *testing.T) {
	h := &Handler{}
	user := &store.User{ID: 1, PrimaryEmail: "user@example.com"}

	t.Run("Section7_1_1_AddressbookHomeSetExplicit", func(t *testing.T) {
		body := `<?xml version="1.0" encoding="utf-8"?>
<d:propfind xmlns:d="DAV:" xmlns:card="urn:ietf:params:xml:ns:carddav">
  <d:prop>
    <card:addressbook-home-set/>
  </d:prop>
</d:propfind>`

		req := httptest.NewRequest("PROPFIND", "/dav/principals/1/", strings.NewReader(body))
		req.Header.Set("Depth", "0")
		req = req.WithContext(auth.WithUser(req.Context(), user))
		rr := httptest.NewRecorder()

		h.Propfind(rr, req)

		respBody := rr.Body.String()
		if !strings.Contains(respBody, "addressbook-home-set") {
			t.Errorf("RFC 6352 Section 7.1.1: principals should expose addressbook-home-set when requested, got %s", respBody)
		}
		if !strings.Contains(respBody, "/dav/addressbooks/") {
			t.Errorf("RFC 6352 Section 7.1.1: addressbook-home-set should reference the address book home collection, got %s", respBody)
		}
	})

	t.Run("Section7_1_1_AddressbookHomeSetNotInAllprop", func(t *testing.T) {
		req := httptest.NewRequest("PROPFIND", "/dav/principals/1/", nil)
		req.Header.Set("Depth", "0")
		req = req.WithContext(auth.WithUser(req.Context(), user))
		rr := httptest.NewRecorder()

		h.Propfind(rr, req)

		if strings.Contains(rr.Body.String(), "addressbook-home-set") {
			t.Errorf("RFC 6352 Section 7.1.1: addressbook-home-set SHOULD NOT be returned by DAV:allprop, got %s", rr.Body.String())
		}
	})

	t.Run("Section7_1_2_PrincipalAddressExplicitOr404", func(t *testing.T) {
		body := `<?xml version="1.0" encoding="utf-8"?>
<d:propfind xmlns:d="DAV:" xmlns:card="urn:ietf:params:xml:ns:carddav">
  <d:prop>
    <card:principal-address/>
  </d:prop>
</d:propfind>`

		req := httptest.NewRequest("PROPFIND", "/dav/principals/1/", strings.NewReader(body))
		req.Header.Set("Depth", "0")
		req = req.WithContext(auth.WithUser(req.Context(), user))
		rr := httptest.NewRecorder()

		h.Propfind(rr, req)

		respBody := rr.Body.String()
		if !strings.Contains(respBody, "principal-address") && !propstatHasStatus(respBody, "principal-address", http.StatusNotFound) {
			t.Errorf("RFC 6352 Section 7.1.2: principal-address should either be returned or explicitly reported as 404, got %s", respBody)
		}
	})

	t.Run("Section7_1_2_PrincipalAddressNotInAllprop", func(t *testing.T) {
		req := httptest.NewRequest("PROPFIND", "/dav/principals/1/", nil)
		req.Header.Set("Depth", "0")
		req = req.WithContext(auth.WithUser(req.Context(), user))
		rr := httptest.NewRecorder()

		h.Propfind(rr, req)

		if strings.Contains(rr.Body.String(), "principal-address") {
			t.Errorf("RFC 6352 Section 7.1.2: principal-address SHOULD NOT be returned by DAV:allprop, got %s", rr.Body.String())
		}
	})
}

func TestRFC6352_ProtectedCardDAVProperties(t *testing.T) {
	bookRepo := &fakeAddressBookRepo{
		books: map[int64]*store.AddressBook{
			5: {ID: 5, UserID: 1, Name: "Contacts"},
		},
	}
	h := &Handler{store: &store.Store{AddressBooks: bookRepo}}
	user := &store.User{ID: 1}

	tests := []struct {
		name string
		prop string
		body string
	}{
		{
			name: "Section6_2_2_SupportedAddressDataIsProtected",
			prop: "supported-address-data",
			body: `<?xml version="1.0" encoding="utf-8"?>
<D:propertyupdate xmlns:D="DAV:" xmlns:C="urn:ietf:params:xml:ns:carddav">
  <D:set>
    <D:prop>
      <C:supported-address-data>
        <C:address-data-type content-type="text/vcard" version="4.0"/>
      </C:supported-address-data>
    </D:prop>
  </D:set>
</D:propertyupdate>`,
		},
		{
			name: "Section6_2_3_MaxResourceSizeIsProtected",
			prop: "max-resource-size",
			body: `<?xml version="1.0" encoding="utf-8"?>
<D:propertyupdate xmlns:D="DAV:" xmlns:C="urn:ietf:params:xml:ns:carddav">
  <D:set>
    <D:prop>
      <C:max-resource-size>1</C:max-resource-size>
    </D:prop>
  </D:set>
</D:propertyupdate>`,
		},
		{
			name: "Section8_3_1_SupportedCollationSetIsProtected",
			prop: "supported-collation-set",
			body: `<?xml version="1.0" encoding="utf-8"?>
<D:propertyupdate xmlns:D="DAV:" xmlns:C="urn:ietf:params:xml:ns:carddav">
  <D:set>
    <D:prop>
      <C:supported-collation-set>
        <C:supported-collation>i;octet</C:supported-collation>
      </C:supported-collation-set>
    </D:prop>
  </D:set>
</D:propertyupdate>`,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			req := httptest.NewRequest("PROPPATCH", "/dav/addressbooks/5", strings.NewReader(tc.body))
			req = req.WithContext(auth.WithUser(req.Context(), user))
			rr := httptest.NewRecorder()

			h.Proppatch(rr, req)

			if rr.Code != http.StatusMultiStatus {
				t.Fatalf("protected property PROPPATCH should return 207, got %d", rr.Code)
			}
			if !strings.Contains(rr.Body.String(), tc.prop) {
				t.Errorf("RFC 6352 protected property handling should mention %s in the propstat response, got %s", tc.prop, rr.Body.String())
			}
			if propstatHasStatus(rr.Body.String(), tc.prop, http.StatusOK) {
				t.Errorf("RFC 6352 protected properties MUST NOT be writable: %s returned 200 OK", tc.prop)
			}
		})
	}
}

func TestRFC6352_AddressObjectResources(t *testing.T) {
	now := store.Now()
	bookRepo := &fakeAddressBookRepo{
		books: map[int64]*store.AddressBook{
			5: {ID: 5, UserID: 1, Name: "Contacts", UpdatedAt: now},
		},
	}
	user := &store.User{ID: 1}

	t.Run("Section6_3_2_PutCreatesNewResourceOnUnmappedURI", func(t *testing.T) {
		contactRepo := &fakeContactRepo{contacts: map[string]*store.Contact{}}
		h := &Handler{store: &store.Store{AddressBooks: bookRepo, Contacts: contactRepo}}

		vcard := buildVCard("3.0",
			"UID:alice",
			"FN:Alice Example",
			"EMAIL:alice@example.com",
		)
		req := newAddressBookPutRequest("/dav/addressbooks/5/alice.vcf", strings.NewReader(vcard))
		req = req.WithContext(auth.WithUser(req.Context(), user))
		rr := httptest.NewRecorder()

		h.Put(rr, req)

		if rr.Code != http.StatusCreated {
			t.Errorf("RFC 6352 Section 6.3.2: PUT to an unmapped URI must create a new address object resource with 201, got %d", rr.Code)
		}
	})

	t.Run("Section6_3_2_IfNoneMatchStarCreatesNewResource", func(t *testing.T) {
		contactRepo := &fakeContactRepo{contacts: map[string]*store.Contact{}}
		h := &Handler{store: &store.Store{AddressBooks: bookRepo, Contacts: contactRepo}}

		vcard := buildVCard("3.0", "UID:new-uid", "FN:New Contact")
		req := newAddressBookPutRequest("/dav/addressbooks/5/new.vcf", strings.NewReader(vcard))
		req.Header.Set("If-None-Match", "*")
		req = req.WithContext(auth.WithUser(req.Context(), user))
		rr := httptest.NewRecorder()

		h.Put(rr, req)

		if rr.Code != http.StatusCreated {
			t.Errorf("RFC 6352 Section 6.3.2: PUT with If-None-Match:* should create a new resource with 201, got %d", rr.Code)
		}
	})

	t.Run("Section6_3_2_IfNoneMatchStarFailsOnExistingResource", func(t *testing.T) {
		contactRepo := &fakeContactRepo{
			contacts: map[string]*store.Contact{
				"5:alice": {AddressBookID: 5, UID: "alice", RawVCard: buildVCard("3.0", "UID:alice", "FN:Alice"), ETag: "old"},
			},
		}
		h := &Handler{store: &store.Store{AddressBooks: bookRepo, Contacts: contactRepo}}

		vcard := buildVCard("3.0", "UID:alice", "FN:Alice Updated")
		req := newAddressBookPutRequest("/dav/addressbooks/5/alice.vcf", strings.NewReader(vcard))
		req.Header.Set("If-None-Match", "*")
		req = req.WithContext(auth.WithUser(req.Context(), user))
		rr := httptest.NewRecorder()

		h.Put(rr, req)

		if rr.Code != http.StatusPreconditionFailed {
			t.Errorf("RFC 6352 Section 6.3.2: PUT with If-None-Match:* against an existing resource must return 412, got %d", rr.Code)
		}
	})

	t.Run("Section6_3_2_IfMatchRequiresMatchingETag", func(t *testing.T) {
		contactRepo := &fakeContactRepo{
			contacts: map[string]*store.Contact{
				"5:alice": {AddressBookID: 5, UID: "alice", RawVCard: buildVCard("3.0", "UID:alice", "FN:Alice"), ETag: "correct-etag"},
			},
		}
		h := &Handler{store: &store.Store{AddressBooks: bookRepo, Contacts: contactRepo}}

		vcard := buildVCard("3.0", "UID:alice", "FN:Alice Updated")
		req := newAddressBookPutRequest("/dav/addressbooks/5/alice.vcf", strings.NewReader(vcard))
		req.Header.Set("If-Match", `"wrong-etag"`)
		req = req.WithContext(auth.WithUser(req.Context(), user))
		rr := httptest.NewRecorder()

		h.Put(rr, req)

		if rr.Code != http.StatusPreconditionFailed {
			t.Errorf("RFC 6352 Section 6.3.2: PUT with a mismatched If-Match must return 412, got %d", rr.Code)
		}
	})

	t.Run("Section6_3_2_Section6_3_2_3_IfMatchSuccessReturnsStrongETag", func(t *testing.T) {
		contactRepo := &fakeContactRepo{
			contacts: map[string]*store.Contact{
				"5:alice": {AddressBookID: 5, UID: "alice", RawVCard: buildVCard("3.0", "UID:alice", "FN:Alice"), ETag: "old-etag"},
			},
		}
		h := &Handler{store: &store.Store{AddressBooks: bookRepo, Contacts: contactRepo}}

		vcard := buildVCard("3.0", "UID:alice", "FN:Alice Updated", "EMAIL:alice@example.com")
		req := newAddressBookPutRequest("/dav/addressbooks/5/alice.vcf", strings.NewReader(vcard))
		req.Header.Set("If-Match", `"old-etag"`)
		req = req.WithContext(auth.WithUser(req.Context(), user))
		rr := httptest.NewRecorder()

		h.Put(rr, req)

		if rr.Code != http.StatusNoContent {
			t.Fatalf("RFC 6352 Section 6.3.2: PUT with a matching If-Match should update the resource with 204, got %d", rr.Code)
		}
		etag := rr.Header().Get("ETag")
		if etag == "" {
			t.Error("RFC 6352 Section 6.3.2.3: successful PUT should return an ETag when octet-equivalent data is stored")
		}
		if !strings.HasPrefix(etag, `"`) || !strings.HasSuffix(etag, `"`) {
			t.Errorf("RFC 6352 Section 6.3.2.3: ETag must be strong/quoted, got %q", etag)
		}
	})

	t.Run("Section5_1_Section6_3_2_3_GetReturnsTextVCardAndStrongETag", func(t *testing.T) {
		vcard := buildVCard("3.0", "UID:alice", "FN:Alice Example", "EMAIL:alice@example.com")
		contactRepo := &fakeContactRepo{
			contacts: map[string]*store.Contact{
				"5:alice": {AddressBookID: 5, UID: "alice", RawVCard: vcard, ETag: "etag-1"},
			},
		}
		h := &Handler{store: &store.Store{AddressBooks: bookRepo, Contacts: contactRepo}}

		req := httptest.NewRequest(http.MethodGet, "/dav/addressbooks/5/alice.vcf", nil)
		req = req.WithContext(auth.WithUser(req.Context(), user))
		rr := httptest.NewRecorder()

		h.Get(rr, req)

		if rr.Code != http.StatusOK {
			t.Fatalf("GET on an address object resource must succeed with 200, got %d", rr.Code)
		}
		if rr.Header().Get("Content-Type") != "text/vcard" {
			t.Errorf("RFC 6352 Section 5.1: address object resources must be served as text/vcard, got %q", rr.Header().Get("Content-Type"))
		}
		etag := rr.Header().Get("ETag")
		if etag == "" {
			t.Error("RFC 6352 Section 6.3.2.3: GET must return an ETag header")
		}
		if !strings.HasPrefix(etag, `"`) || !strings.HasSuffix(etag, `"`) {
			t.Errorf("RFC 6352 Section 6.3.2.3: GET ETag must be strong/quoted, got %q", etag)
		}
		if rr.Body.String() != vcard {
			t.Errorf("GET should return the stored vCard body verbatim, got %q", rr.Body.String())
		}
	})

	t.Run("Section5_1_1_1_GetRejectsUnsupportedAddressDataConversion", func(t *testing.T) {
		vcard := buildVCard("3.0", "UID:alice", "FN:Alice Example")
		contactRepo := &fakeContactRepo{
			contacts: map[string]*store.Contact{
				"5:alice": {AddressBookID: 5, UID: "alice", RawVCard: vcard, ETag: "etag-1"},
			},
		}
		h := &Handler{store: &store.Store{AddressBooks: bookRepo, Contacts: contactRepo}}

		req := httptest.NewRequest(http.MethodGet, "/dav/addressbooks/5/alice.vcf", nil)
		req.Header.Set("Accept", "application/json")
		req = req.WithContext(auth.WithUser(req.Context(), user))
		rr := httptest.NewRecorder()

		h.Get(rr, req)

		assertCardDAVPreconditionStatus(t, rr, http.StatusNotAcceptable, "supported-address-data-conversion")
	})

	t.Run("Section5_1_1_1_GetRejectsUnsupportedVCardVersionConversion", func(t *testing.T) {
		vcard := buildVCard("4.0", "UID:alice-v4", "FN:Alice Example")
		contactRepo := &fakeContactRepo{
			contacts: map[string]*store.Contact{
				"5:alice-v4": {AddressBookID: 5, UID: "alice-v4", RawVCard: vcard, ETag: "etag-v4"},
			},
		}
		h := &Handler{store: &store.Store{AddressBooks: bookRepo, Contacts: contactRepo}}

		req := httptest.NewRequest(http.MethodGet, "/dav/addressbooks/5/alice-v4.vcf", nil)
		req.Header.Set("Accept", `text/vcard; version="3.0"`)
		req = req.WithContext(auth.WithUser(req.Context(), user))
		rr := httptest.NewRecorder()

		h.Get(rr, req)

		if rr.Code != http.StatusNotAcceptable {
			t.Fatalf("RFC 6352 Section 5.1.1.1: GET must fail when the requested vCard version would require unsupported conversion, got %d", rr.Code)
		}
		assertCardDAVErrorBody(t, rr.Body.String(), "supported-address-data-conversion")
	})

	t.Run("Section6_3_2_1_RejectsUnsupportedMediaType", func(t *testing.T) {
		contactRepo := &fakeContactRepo{contacts: map[string]*store.Contact{}}
		h := &Handler{store: &store.Store{AddressBooks: bookRepo, Contacts: contactRepo}}

		vcard := buildVCard("3.0", "UID:alice", "FN:Alice Example")
		req := httptest.NewRequest(http.MethodPut, "/dav/addressbooks/5/alice.vcf", strings.NewReader(vcard))
		req.Header.Set("Content-Type", "application/json")
		req = req.WithContext(auth.WithUser(req.Context(), user))
		rr := httptest.NewRecorder()

		h.Put(rr, req)

		if rr.Code == http.StatusCreated || rr.Code == http.StatusNoContent {
			t.Errorf("RFC 6352 Section 6.3.2.1: PUT with unsupported media type must fail with supported-address-data precondition, got %d", rr.Code)
		}
		assertCardDAVErrorBody(t, rr.Body.String(), "supported-address-data")
	})

	t.Run("Section6_3_2_1_RejectsInvalidVCardData", func(t *testing.T) {
		contactRepo := &fakeContactRepo{contacts: map[string]*store.Contact{}}
		h := &Handler{store: &store.Store{AddressBooks: bookRepo, Contacts: contactRepo}}

		req := newAddressBookPutRequest("/dav/addressbooks/5/alice.vcf", strings.NewReader("NOT A VCARD"))
		req = req.WithContext(auth.WithUser(req.Context(), user))
		rr := httptest.NewRecorder()

		h.Put(rr, req)

		if rr.Code != http.StatusBadRequest {
			t.Errorf("RFC 6352 Section 6.3.2.1: invalid vCard data must be rejected with 400, got %d", rr.Code)
		}
		assertCardDAVErrorBody(t, rr.Body.String(), "valid-address-data")
	})

	t.Run("Section5_1_RejectsVCardWithoutUID", func(t *testing.T) {
		contactRepo := &fakeContactRepo{contacts: map[string]*store.Contact{}}
		h := &Handler{store: &store.Store{AddressBooks: bookRepo, Contacts: contactRepo}}

		card := buildVCard("3.0", "FN:Alice Example")
		req := newAddressBookPutRequest("/dav/addressbooks/5/alice.vcf", strings.NewReader(card))
		req = req.WithContext(auth.WithUser(req.Context(), user))
		rr := httptest.NewRecorder()

		h.Put(rr, req)

		if rr.Code != http.StatusBadRequest {
			t.Fatalf("RFC 6352 Section 5.1: vCards without UID must be rejected, got %d", rr.Code)
		}
		assertCardDAVErrorBody(t, rr.Body.String(), "valid-address-data")
	})

	t.Run("Section6_3_2_1_RejectsVCardWithoutVersion", func(t *testing.T) {
		contactRepo := &fakeContactRepo{contacts: map[string]*store.Contact{}}
		h := &Handler{store: &store.Store{AddressBooks: bookRepo, Contacts: contactRepo}}

		card := "BEGIN:VCARD\r\nUID:alice\r\nFN:Alice Example\r\nEND:VCARD\r\n"
		req := newAddressBookPutRequest("/dav/addressbooks/5/alice.vcf", strings.NewReader(card))
		req = req.WithContext(auth.WithUser(req.Context(), user))
		rr := httptest.NewRecorder()

		h.Put(rr, req)

		if rr.Code != http.StatusBadRequest {
			t.Fatalf("RFC 6352 Section 6.3.2.1: vCards without VERSION must be rejected, got %d", rr.Code)
		}
		assertCardDAVErrorBody(t, rr.Body.String(), "valid-address-data")
	})

	t.Run("Section6_3_2_1_RejectsUnsupportedVCardVersion", func(t *testing.T) {
		contactRepo := &fakeContactRepo{contacts: map[string]*store.Contact{}}
		h := &Handler{store: &store.Store{AddressBooks: bookRepo, Contacts: contactRepo}}

		card := buildVCard("2.1", "UID:alice", "FN:Alice Example")
		req := newAddressBookPutRequest("/dav/addressbooks/5/alice.vcf", strings.NewReader(card))
		req = req.WithContext(auth.WithUser(req.Context(), user))
		rr := httptest.NewRecorder()

		h.Put(rr, req)

		if rr.Code != http.StatusBadRequest {
			t.Fatalf("RFC 6352 Section 6.3.2.1: unsupported vCard versions must be rejected, got %d", rr.Code)
		}
		assertCardDAVErrorBody(t, rr.Body.String(), "valid-address-data")
	})

	t.Run("Section6_3_2_1_RejectsVCardWithoutFN", func(t *testing.T) {
		contactRepo := &fakeContactRepo{contacts: map[string]*store.Contact{}}
		h := &Handler{store: &store.Store{AddressBooks: bookRepo, Contacts: contactRepo}}

		card := buildVCard("3.0", "UID:alice")
		req := newAddressBookPutRequest("/dav/addressbooks/5/alice.vcf", strings.NewReader(card))
		req = req.WithContext(auth.WithUser(req.Context(), user))
		rr := httptest.NewRecorder()

		h.Put(rr, req)

		if rr.Code != http.StatusBadRequest {
			t.Fatalf("RFC 6352 Section 6.3.2.1: vCards without FN must be rejected, got %d", rr.Code)
		}
		assertCardDAVErrorBody(t, rr.Body.String(), "valid-address-data")
	})

	t.Run("Section6_3_2_1_AcceptsValidFNMutations", func(t *testing.T) {
		tests := []struct {
			name string
			card string
			path string
		}{
			{
				name: "language parameter",
				card: buildVCard("3.0", "UID:alice-fn-lang", "FN;LANGUAGE=en:Alice Example"),
				path: "/dav/addressbooks/5/alice-fn-lang.vcf",
			},
			{
				name: "non-standard parameter",
				card: buildVCard("3.0", "UID:alice-fn-x", "FN;X-ABC-LABEL=preferred:Alice Example"),
				path: "/dav/addressbooks/5/alice-fn-x.vcf",
			},
			{
				name: "grouped property",
				card: buildVCard("3.0", "UID:alice-fn-group", "item1.FN:Alice Example"),
				path: "/dav/addressbooks/5/alice-fn-group.vcf",
			},
		}

		for _, tc := range tests {
			t.Run(tc.name, func(t *testing.T) {
				contactRepo := &fakeContactRepo{contacts: map[string]*store.Contact{}}
				h := &Handler{store: &store.Store{AddressBooks: bookRepo, Contacts: contactRepo}}

				req := newAddressBookPutRequest(tc.path, strings.NewReader(tc.card))
				req = req.WithContext(auth.WithUser(req.Context(), user))
				rr := httptest.NewRecorder()

				h.Put(rr, req)

				if rr.Code != http.StatusCreated && rr.Code != http.StatusNoContent {
					t.Fatalf("RFC 6350 contentline syntax allows this FN mutation, got %d: %s", rr.Code, rr.Body.String())
				}
			})
		}
	})

	t.Run("Section5_1_AddressObjectContainsSingleVCardComponentOnly", func(t *testing.T) {
		contactRepo := &fakeContactRepo{contacts: map[string]*store.Contact{}}
		h := &Handler{store: &store.Store{AddressBooks: bookRepo, Contacts: contactRepo}}

		doubleVCard := buildVCard("3.0", "UID:first", "FN:First") + buildVCard("3.0", "UID:second", "FN:Second")
		req := newAddressBookPutRequest("/dav/addressbooks/5/double.vcf", strings.NewReader(doubleVCard))
		req = req.WithContext(auth.WithUser(req.Context(), user))
		rr := httptest.NewRecorder()

		h.Put(rr, req)

		assertCardDAVPreconditionStatus(t, rr, http.StatusBadRequest, "valid-address-data")
	})

	t.Run("Section3_ServerShouldSupportVCard4", func(t *testing.T) {
		contactRepo := &fakeContactRepo{contacts: map[string]*store.Contact{}}
		h := &Handler{store: &store.Store{AddressBooks: bookRepo, Contacts: contactRepo}}

		vcard := buildVCard("4.0", "UID:alice-v4", "FN:Alice Example")
		req := newAddressBookPutRequest("/dav/addressbooks/5/alice-v4.vcf", strings.NewReader(vcard))
		req = req.WithContext(auth.WithUser(req.Context(), user))
		rr := httptest.NewRecorder()

		h.Put(rr, req)

		if rr.Code != http.StatusCreated && rr.Code != http.StatusNoContent {
			t.Errorf("RFC 6352 Section 3: CardDAV servers SHOULD support vCard 4.0 address object resources, got %d", rr.Code)
		}
	})

	t.Run("Section6_3_2_2_AcceptsNonStandardXProperties", func(t *testing.T) {
		contactRepo := &fakeContactRepo{contacts: map[string]*store.Contact{}}
		h := &Handler{store: &store.Store{AddressBooks: bookRepo, Contacts: contactRepo}}

		vcard := buildVCard("3.0",
			"UID:alice-x",
			"FN:Alice Example",
			"X-ABC-CUSTOM:custom-value",
		)
		req := newAddressBookPutRequest("/dav/addressbooks/5/alice-x.vcf", strings.NewReader(vcard))
		req = req.WithContext(auth.WithUser(req.Context(), user))
		rr := httptest.NewRecorder()

		h.Put(rr, req)

		if rr.Code != http.StatusCreated && rr.Code != http.StatusNoContent {
			t.Fatalf("RFC 6352 Section 6.3.2.2: PUT with X- properties should succeed, got %d", rr.Code)
		}

		getReq := httptest.NewRequest(http.MethodGet, "/dav/addressbooks/5/alice-x.vcf", nil)
		getReq = getReq.WithContext(auth.WithUser(getReq.Context(), user))
		getRR := httptest.NewRecorder()
		h.Get(getRR, getReq)

		if !strings.Contains(getRR.Body.String(), "X-ABC-CUSTOM:custom-value") {
			t.Errorf("RFC 6352 Section 6.3.2.2: non-standard X- properties must be preserved, got %s", getRR.Body.String())
		}
	})

	t.Run("Section6_3_2_2_AcceptsNonStandardParameters", func(t *testing.T) {
		contactRepo := &fakeContactRepo{contacts: map[string]*store.Contact{}}
		h := &Handler{store: &store.Store{AddressBooks: bookRepo, Contacts: contactRepo}}

		vcard := buildVCard("3.0",
			"UID:alice-param",
			"FN:Alice Example",
			"EMAIL;X-ABC-TYPE=custom:alice@example.com",
		)
		req := newAddressBookPutRequest("/dav/addressbooks/5/alice-param.vcf", strings.NewReader(vcard))
		req = req.WithContext(auth.WithUser(req.Context(), user))
		rr := httptest.NewRecorder()

		h.Put(rr, req)

		if rr.Code != http.StatusCreated && rr.Code != http.StatusNoContent {
			t.Fatalf("RFC 6352 Section 6.3.2.2: PUT with non-standard parameters should succeed, got %d", rr.Code)
		}

		getReq := httptest.NewRequest(http.MethodGet, "/dav/addressbooks/5/alice-param.vcf", nil)
		getReq = getReq.WithContext(auth.WithUser(getReq.Context(), user))
		getRR := httptest.NewRecorder()
		h.Get(getRR, getReq)

		if !strings.Contains(getRR.Body.String(), "EMAIL;X-ABC-TYPE=custom:alice@example.com") {
			t.Errorf("RFC 6352 Section 6.3.2.2: non-standard vCard parameters must be preserved, got %s", getRR.Body.String())
		}
	})

	t.Run("Section5_1_Section6_3_2_1_DuplicateUIDAcrossResourcesRejected", func(t *testing.T) {
		contactRepo := &fakeContactRepo{contacts: map[string]*store.Contact{}}
		h := &Handler{store: &store.Store{AddressBooks: bookRepo, Contacts: contactRepo}}

		first := buildVCard("3.0", "UID:shared-uid", "FN:First Contact")
		req := newAddressBookPutRequest("/dav/addressbooks/5/first.vcf", strings.NewReader(first))
		req.Header.Set("If-None-Match", "*")
		req = req.WithContext(auth.WithUser(req.Context(), user))
		rr := httptest.NewRecorder()
		h.Put(rr, req)
		if rr.Code != http.StatusCreated {
			t.Fatalf("initial PUT should succeed, got %d", rr.Code)
		}

		second := buildVCard("3.0", "UID:shared-uid", "FN:Second Contact")
		req = newAddressBookPutRequest("/dav/addressbooks/5/second.vcf", strings.NewReader(second))
		req.Header.Set("If-None-Match", "*")
		req = req.WithContext(auth.WithUser(req.Context(), user))
		rr = httptest.NewRecorder()
		h.Put(rr, req)

		assertCardDAVPreconditionStatus(t, rr, http.StatusConflict, "no-uid-conflict")
	})

	t.Run("Section6_3_2_1_ChangingUIDOfExistingResourceRejected", func(t *testing.T) {
		contactRepo := &fakeContactRepo{contacts: map[string]*store.Contact{}}
		h := &Handler{store: &store.Store{AddressBooks: bookRepo, Contacts: contactRepo}}

		initial := buildVCard("3.0", "UID:alice", "FN:Alice Example")
		req := newAddressBookPutRequest("/dav/addressbooks/5/alice.vcf", strings.NewReader(initial))
		req = req.WithContext(auth.WithUser(req.Context(), user))
		rr := httptest.NewRecorder()
		h.Put(rr, req)
		if rr.Code != http.StatusCreated {
			t.Fatalf("initial PUT should succeed, got %d", rr.Code)
		}
		etag := rr.Header().Get("ETag")
		if etag == "" {
			t.Fatalf("initial PUT should return ETag")
		}

		updated := buildVCard("3.0", "UID:different-uid", "FN:Alice Example")
		req = newAddressBookPutRequest("/dav/addressbooks/5/alice.vcf", strings.NewReader(updated))
		req.Header.Set("If-Match", etag)
		req = req.WithContext(auth.WithUser(req.Context(), user))
		rr = httptest.NewRecorder()
		h.Put(rr, req)

		assertCardDAVPreconditionStatus(t, rr, http.StatusConflict, "no-uid-conflict")
	})

	t.Run("Section5_1_Section6_3_2_1_ResourceHrefsPreserveDestinationNames", func(t *testing.T) {
		contactRepo := &fakeContactRepo{contacts: map[string]*store.Contact{}}
		h := &Handler{store: &store.Store{AddressBooks: bookRepo, Contacts: contactRepo}}

		first := buildVCard("3.0", "UID:shared-uid", "FN:First Contact")
		req := newAddressBookPutRequest("/dav/addressbooks/5/first.vcf", strings.NewReader(first))
		req = req.WithContext(auth.WithUser(req.Context(), user))
		rr := httptest.NewRecorder()
		h.Put(rr, req)
		if rr.Code != http.StatusCreated {
			t.Fatalf("initial PUT should succeed, got %d", rr.Code)
		}

		getReq := httptest.NewRequest(http.MethodGet, "/dav/addressbooks/5/first.vcf", nil)
		getReq = getReq.WithContext(auth.WithUser(getReq.Context(), user))
		getRR := httptest.NewRecorder()
		h.Get(getRR, getReq)
		if getRR.Code != http.StatusOK || !strings.Contains(getRR.Body.String(), "UID:shared-uid") {
			t.Fatalf("RFC 6352 Section 5.1: address objects must remain retrievable at their request URI, got %d with %q", getRR.Code, getRR.Body.String())
		}

		propfindBody := `<?xml version="1.0" encoding="utf-8"?>
<d:propfind xmlns:d="DAV:">
  <d:prop>
    <d:getetag/>
  </d:prop>
</d:propfind>`
		propfindReq := httptest.NewRequest("PROPFIND", "/dav/addressbooks/5/first.vcf", strings.NewReader(propfindBody))
		propfindReq.Header.Set("Depth", "0")
		propfindReq = propfindReq.WithContext(auth.WithUser(propfindReq.Context(), user))
		propfindRR := httptest.NewRecorder()
		h.Propfind(propfindRR, propfindReq)
		if !strings.Contains(propfindRR.Body.String(), "/dav/addressbooks/5/first.vcf") {
			t.Fatalf("RFC 6352 Section 6.3.2.3: PROPFIND responses must use the stored resource href, got %s", propfindRR.Body.String())
		}

		queryBody := `<?xml version="1.0" encoding="utf-8"?>
<card:addressbook-query xmlns:card="urn:ietf:params:xml:ns:carddav" xmlns:D="DAV:">
  <D:prop>
    <D:getetag/>
  </D:prop>
  <card:filter>
    <card:prop-filter name="UID">
      <card:text-match collation="i;unicode-casemap" match-type="equals">shared-uid</card:text-match>
    </card:prop-filter>
  </card:filter>
</card:addressbook-query>`
		queryReq := httptest.NewRequest("REPORT", "/dav/addressbooks/5/", strings.NewReader(queryBody))
		queryReq.Header.Set("Depth", "1")
		queryReq = queryReq.WithContext(auth.WithUser(queryReq.Context(), user))
		queryRR := httptest.NewRecorder()
		h.Report(queryRR, queryReq)
		if !strings.Contains(queryRR.Body.String(), "/dav/addressbooks/5/first.vcf") || strings.Contains(queryRR.Body.String(), "/dav/addressbooks/5/shared-uid.vcf") {
			t.Fatalf("RFC 6352 Section 8.6: query responses must preserve the destination resource name, got %s", queryRR.Body.String())
		}

		multigetBody := `<?xml version="1.0" encoding="utf-8"?>
<card:addressbook-multiget xmlns:card="urn:ietf:params:xml:ns:carddav" xmlns:D="DAV:">
  <D:href>/dav/addressbooks/5/first.vcf</D:href>
</card:addressbook-multiget>`
		multigetReq := httptest.NewRequest("REPORT", "/dav/addressbooks/5/", strings.NewReader(multigetBody))
		multigetReq.Header.Set("Depth", "0")
		multigetReq = multigetReq.WithContext(auth.WithUser(multigetReq.Context(), user))
		multigetRR := httptest.NewRecorder()
		h.Report(multigetRR, multigetReq)
		if !strings.Contains(multigetRR.Body.String(), "/dav/addressbooks/5/first.vcf") || strings.Contains(multigetRR.Body.String(), "404 Not Found") {
			t.Fatalf("RFC 6352 Section 8.7: multiget must resolve the requested href rather than UID-derived names, got %s", multigetRR.Body.String())
		}

		deleteReq := httptest.NewRequest(http.MethodDelete, "/dav/addressbooks/5/first.vcf", nil)
		deleteReq = deleteReq.WithContext(auth.WithUser(deleteReq.Context(), user))
		deleteRR := httptest.NewRecorder()
		h.Delete(deleteRR, deleteReq)
		if deleteRR.Code != http.StatusNoContent {
			t.Fatalf("RFC 6352 Section 6.3.2.1: DELETE must operate on the original resource href, got %d", deleteRR.Code)
		}
	})

	t.Run("Section6_3_2_1_COPYPreservesUIDAndUsesDestinationResourceName", func(t *testing.T) {
		bookRepo := &fakeAddressBookRepo{
			books: map[int64]*store.AddressBook{
				5: {ID: 5, UserID: 1, Name: "Source"},
				6: {ID: 6, UserID: 1, Name: "Destination"},
			},
		}
		contactRepo := &fakeContactRepo{
			contacts: map[string]*store.Contact{
				"5:shared-uid": {AddressBookID: 5, UID: "shared-uid", ResourceName: "first", RawVCard: buildVCard("3.0", "UID:shared-uid", "FN:First Contact"), ETag: "etag-1"},
			},
		}
		h := &Handler{store: &store.Store{AddressBooks: bookRepo, Contacts: contactRepo}}

		req := httptest.NewRequest("COPY", "/dav/addressbooks/5/first.vcf", nil)
		req.Header.Set("Destination", "https://example.com/dav/addressbooks/6/renamed.vcf")
		req.Header.Set("Overwrite", "F")
		req = req.WithContext(auth.WithUser(req.Context(), user))
		rr := httptest.NewRecorder()
		h.Copy(rr, req)

		if rr.Code != http.StatusCreated {
			t.Fatalf("RFC 6352 Section 6.3.2.1: COPY to a new destination href should succeed, got %d", rr.Code)
		}
		copied, _ := contactRepo.GetByResourceName(req.Context(), 6, "renamed")
		if copied == nil || copied.UID != "shared-uid" {
			t.Fatalf("RFC 6352 Section 6.3.2.1: COPY must preserve UID while binding the destination resource name, got %#v", copied)
		}
	})

	t.Run("Section6_3_2_1_COPYRejectsDestinationUIDConflicts", func(t *testing.T) {
		bookRepo := &fakeAddressBookRepo{
			books: map[int64]*store.AddressBook{
				5: {ID: 5, UserID: 1, Name: "Source"},
				6: {ID: 6, UserID: 1, Name: "Destination"},
			},
		}
		contactRepo := &fakeContactRepo{
			contacts: map[string]*store.Contact{
				"5:shared-uid": {AddressBookID: 5, UID: "shared-uid", ResourceName: "first", RawVCard: buildVCard("3.0", "UID:shared-uid", "FN:First Contact"), ETag: "etag-1"},
				"6:shared-uid": {AddressBookID: 6, UID: "shared-uid", ResourceName: "existing", RawVCard: buildVCard("3.0", "UID:shared-uid", "FN:Existing Contact"), ETag: "etag-2"},
			},
		}
		h := &Handler{store: &store.Store{AddressBooks: bookRepo, Contacts: contactRepo}}

		req := httptest.NewRequest("COPY", "/dav/addressbooks/5/first.vcf", nil)
		req.Header.Set("Destination", "https://example.com/dav/addressbooks/6/renamed.vcf")
		req.Header.Set("Overwrite", "F")
		req = req.WithContext(auth.WithUser(req.Context(), user))
		rr := httptest.NewRecorder()
		h.Copy(rr, req)

		assertCardDAVPreconditionStatus(t, rr, http.StatusConflict, "no-uid-conflict")
	})

	t.Run("Section6_3_2_1_MOVEPreservesUIDAndUsesDestinationResourceName", func(t *testing.T) {
		bookRepo := &fakeAddressBookRepo{
			books: map[int64]*store.AddressBook{
				5: {ID: 5, UserID: 1, Name: "Source"},
				6: {ID: 6, UserID: 1, Name: "Destination"},
			},
		}
		contactRepo := &fakeContactRepo{
			contacts: map[string]*store.Contact{
				"5:shared-uid": {AddressBookID: 5, UID: "shared-uid", ResourceName: "first", RawVCard: buildVCard("3.0", "UID:shared-uid", "FN:First Contact"), ETag: "etag-1"},
			},
		}
		h := &Handler{store: &store.Store{AddressBooks: bookRepo, Contacts: contactRepo}}

		req := httptest.NewRequest("MOVE", "/dav/addressbooks/5/first.vcf", nil)
		req.Header.Set("Destination", "https://example.com/dav/addressbooks/6/renamed.vcf")
		req.Header.Set("Overwrite", "F")
		req = req.WithContext(auth.WithUser(req.Context(), user))
		rr := httptest.NewRecorder()
		h.Move(rr, req)

		if rr.Code != http.StatusCreated {
			t.Fatalf("RFC 6352 Section 6.3.2.1: MOVE to a new destination href should succeed, got %d", rr.Code)
		}
		moved, _ := contactRepo.GetByResourceName(req.Context(), 6, "renamed")
		if moved == nil || moved.UID != "shared-uid" {
			t.Fatalf("RFC 6352 Section 6.3.2.1: MOVE must preserve UID while rebinding to the destination resource name, got %#v", moved)
		}
		source, _ := contactRepo.GetByResourceName(req.Context(), 5, "first")
		if source != nil {
			t.Fatalf("RFC 6352 Section 6.3.2.1: MOVE must remove the source href, got %#v", source)
		}
	})

	t.Run("Section6_3_2_1_RejectsResourcesExceedingMaxResourceSize", func(t *testing.T) {
		contactRepo := &fakeContactRepo{contacts: map[string]*store.Contact{}}
		h := &Handler{store: &store.Store{AddressBooks: bookRepo, Contacts: contactRepo}}

		oversized := buildVCard("3.0", "UID:alice", "FN:"+strings.Repeat("A", int(maxDAVBodyBytes)))
		req := newAddressBookPutRequest("/dav/addressbooks/5/alice.vcf", strings.NewReader(oversized))
		req = req.WithContext(auth.WithUser(req.Context(), user))
		rr := httptest.NewRecorder()

		h.Put(rr, req)

		if rr.Code != http.StatusRequestEntityTooLarge && rr.Code != http.StatusForbidden {
			t.Errorf("RFC 6352 Section 6.3.2.1: PUT exceeding max-resource-size must return 413 or 403, got %d", rr.Code)
		}
		assertCardDAVErrorBody(t, rr.Body.String(), "max-resource-size")
	})

	t.Run("Section6_3_2_1_COPYRejectsResourcesExceedingMaxResourceSize", func(t *testing.T) {
		bookRepo := &fakeAddressBookRepo{
			books: map[int64]*store.AddressBook{
				5: {ID: 5, UserID: 1, Name: "Source"},
				6: {ID: 6, UserID: 1, Name: "Destination"},
			},
		}
		oversized := buildVCard("3.0", "UID:shared-uid", "FN:"+strings.Repeat("A", int(maxDAVBodyBytes)))
		contactRepo := &fakeContactRepo{
			contacts: map[string]*store.Contact{
				"5:shared-uid": {AddressBookID: 5, UID: "shared-uid", ResourceName: "first", RawVCard: oversized, ETag: "etag-1"},
			},
		}
		h := &Handler{store: &store.Store{AddressBooks: bookRepo, Contacts: contactRepo}}

		req := httptest.NewRequest("COPY", "/dav/addressbooks/5/first.vcf", nil)
		req.Header.Set("Destination", "https://example.com/dav/addressbooks/6/renamed.vcf")
		req = req.WithContext(auth.WithUser(req.Context(), user))
		rr := httptest.NewRecorder()

		h.Copy(rr, req)

		if rr.Code != http.StatusRequestEntityTooLarge {
			t.Fatalf("RFC 6352 Section 6.3.2.1: COPY exceeding max-resource-size must return 413, got %d", rr.Code)
		}
		assertCardDAVErrorBody(t, rr.Body.String(), "max-resource-size")
	})

	t.Run("Section6_3_2_1_MOVERejectsResourcesExceedingMaxResourceSize", func(t *testing.T) {
		bookRepo := &fakeAddressBookRepo{
			books: map[int64]*store.AddressBook{
				5: {ID: 5, UserID: 1, Name: "Source"},
				6: {ID: 6, UserID: 1, Name: "Destination"},
			},
		}
		oversized := buildVCard("3.0", "UID:shared-uid", "FN:"+strings.Repeat("A", int(maxDAVBodyBytes)))
		contactRepo := &fakeContactRepo{
			contacts: map[string]*store.Contact{
				"5:shared-uid": {AddressBookID: 5, UID: "shared-uid", ResourceName: "first", RawVCard: oversized, ETag: "etag-1"},
			},
		}
		h := &Handler{store: &store.Store{AddressBooks: bookRepo, Contacts: contactRepo}}

		req := httptest.NewRequest("MOVE", "/dav/addressbooks/5/first.vcf", nil)
		req.Header.Set("Destination", "https://example.com/dav/addressbooks/6/renamed.vcf")
		req = req.WithContext(auth.WithUser(req.Context(), user))
		rr := httptest.NewRecorder()

		h.Move(rr, req)

		if rr.Code != http.StatusRequestEntityTooLarge {
			t.Fatalf("RFC 6352 Section 6.3.2.1: MOVE exceeding max-resource-size must return 413, got %d", rr.Code)
		}
		assertCardDAVErrorBody(t, rr.Body.String(), "max-resource-size")
	})

	t.Run("Section6_3_2_3_ObjectResourcePropfindExposesGetETagContentTypeAndReports", func(t *testing.T) {
		vcard := buildVCard("3.0", "UID:alice", "FN:Alice Example", "EMAIL:alice@example.com")
		contactRepo := &fakeContactRepo{
			contacts: map[string]*store.Contact{
				"5:alice": {AddressBookID: 5, UID: "alice", RawVCard: vcard, ETag: "etag-1"},
			},
		}
		h := &Handler{store: &store.Store{AddressBooks: bookRepo, Contacts: contactRepo}}

		body := `<?xml version="1.0" encoding="utf-8"?>
<d:propfind xmlns:d="DAV:">
  <d:prop>
    <d:getetag/>
    <d:getcontenttype/>
    <d:supported-report-set/>
  </d:prop>
</d:propfind>`

		req := httptest.NewRequest("PROPFIND", "/dav/addressbooks/5/alice.vcf", strings.NewReader(body))
		req.Header.Set("Depth", "0")
		req = req.WithContext(auth.WithUser(req.Context(), user))
		rr := httptest.NewRecorder()

		h.Propfind(rr, req)

		respBody := rr.Body.String()
		if !strings.Contains(respBody, "/dav/addressbooks/5/alice.vcf") {
			t.Errorf("RFC 6352 Sections 6.3.2.3 and 8: PROPFIND on an address object resource should respond for the resource itself, got %s", respBody)
		}
		if !strings.Contains(respBody, "getetag") {
			t.Errorf("RFC 6352 Section 6.3.2.3: DAV:getetag must be defined on address object resources, got %s", respBody)
		}
		if !strings.Contains(respBody, "text/vcard") {
			t.Errorf("RFC 6352 Section 5.1: address object resources should expose text/vcard content type, got %s", respBody)
		}
		for _, reportName := range []string{"addressbook-query", "addressbook-multiget", "expand-property"} {
			if !strings.Contains(respBody, reportName) {
				t.Errorf("RFC 6352 Section 8: address object resources must advertise %s support, got %s", reportName, respBody)
			}
		}
	})
}

func TestRFC6352_AddressBookCreation(t *testing.T) {
	user := &store.User{ID: 1}

	t.Run("Section6_3_1_MKCOLCreatesAddressBookCollection", func(t *testing.T) {
		bookRepo := &fakeAddressBookRepo{}
		h := &Handler{store: &store.Store{AddressBooks: bookRepo}}

		req := httptest.NewRequest("MKCOL", "/dav/addressbooks/NewBook", nil)
		req = req.WithContext(auth.WithUser(req.Context(), user))
		rr := httptest.NewRecorder()

		h.Mkcol(rr, req)

		if rr.Code != http.StatusCreated {
			t.Errorf("RFC 6352 Section 6.3.1: MKCOL-based address book creation should return 201 when supported, got %d", rr.Code)
		}
		if rr.Header().Get("Location") != "/dav/addressbooks/1/" {
			t.Errorf("MKCOL should return the canonical collection location, got %q", rr.Header().Get("Location"))
		}
		if len(bookRepo.books) != 1 {
			t.Fatalf("expected one address book to be created, got %d", len(bookRepo.books))
		}

		req = httptest.NewRequest("PROPFIND", "/dav/addressbooks/NewBook/", nil)
		req.Header.Set("Depth", "0")
		req = req.WithContext(auth.WithUser(req.Context(), user))
		rr = httptest.NewRecorder()

		h.Propfind(rr, req)

		if rr.Code != http.StatusMultiStatus {
			t.Fatalf("created address book should be accessible via the request URI, got %d", rr.Code)
		}
		if !strings.Contains(rr.Body.String(), "/dav/addressbooks/NewBook/") {
			t.Errorf("PROPFIND should preserve the request-URI collection href, got %s", rr.Body.String())
		}
	})

	t.Run("Section6_3_1_ExtendedMKCOLInitializesProperties", func(t *testing.T) {
		bookRepo := &fakeAddressBookRepo{}
		h := &Handler{store: &store.Store{AddressBooks: bookRepo}}

		body := `<?xml version="1.0" encoding="utf-8"?>
<D:mkcol xmlns:D="DAV:" xmlns:C="urn:ietf:params:xml:ns:carddav">
  <D:set>
    <D:prop>
      <D:resourcetype>
        <D:collection/>
        <C:addressbook/>
      </D:resourcetype>
      <D:displayname>Strict Suite Address Book</D:displayname>
      <C:addressbook-description>Created by RFC 6352 compliance test</C:addressbook-description>
    </D:prop>
  </D:set>
</D:mkcol>`

		req := httptest.NewRequest("MKCOL", "/dav/addressbooks/StrictSuite", strings.NewReader(body))
		req = req.WithContext(auth.WithUser(req.Context(), user))
		rr := httptest.NewRecorder()

		h.Mkcol(rr, req)

		if rr.Code != http.StatusCreated && rr.Code != http.StatusMultiStatus {
			t.Fatalf("RFC 6352 Section 6.3.1: extended MKCOL should return 201 or 207, got %d", rr.Code)
		}
		location := rr.Header().Get("Location")
		if location == "" {
			t.Fatal("extended MKCOL should return a canonical Location header")
		}

		propfindBody := `<?xml version="1.0" encoding="utf-8"?>
<d:propfind xmlns:d="DAV:" xmlns:card="urn:ietf:params:xml:ns:carddav">
  <d:prop>
    <d:resourcetype/>
    <d:displayname/>
    <card:addressbook-description/>
  </d:prop>
</d:propfind>`

		req = httptest.NewRequest("PROPFIND", location, strings.NewReader(propfindBody))
		req.Header.Set("Depth", "0")
		req = req.WithContext(auth.WithUser(req.Context(), user))
		rr = httptest.NewRecorder()
		h.Propfind(rr, req)

		respBody := rr.Body.String()
		if !strings.Contains(respBody, "<card:addressbook") {
			t.Errorf("RFC 6352 Section 6.3.1: MKCOL-created collection must advertise card:addressbook type, got %s", respBody)
		}
		if !strings.Contains(respBody, "Strict Suite Address Book") {
			t.Errorf("RFC 6352 Section 6.3.1: extended MKCOL should initialize DAV:displayname, got %s", respBody)
		}
		if !strings.Contains(respBody, "Created by RFC 6352 compliance test") {
			t.Errorf("RFC 6352 Section 6.3.1: extended MKCOL should initialize CARDDAV:addressbook-description, got %s", respBody)
		}
	})

	t.Run("Section5_2_NestedAddressBooksRejected", func(t *testing.T) {
		bookRepo := &fakeAddressBookRepo{
			books: map[int64]*store.AddressBook{
				5: {ID: 5, UserID: 1, Name: "Contacts"},
			},
		}
		h := &Handler{store: &store.Store{AddressBooks: bookRepo}}

		req := httptest.NewRequest("MKCOL", "/dav/addressbooks/5/ChildBook", nil)
		req = req.WithContext(auth.WithUser(req.Context(), user))
		rr := httptest.NewRecorder()

		h.Mkcol(rr, req)

		if rr.Code == http.StatusCreated {
			t.Errorf("RFC 6352 Section 5.2: nested address book collections MUST NOT be allowed, got %d", rr.Code)
		}
	})
}

func TestRFC6352_ReportMethodSupport(t *testing.T) {
	user := &store.User{ID: 1}
	now := store.Now()
	bookRepo := &fakeAddressBookRepo{
		books: map[int64]*store.AddressBook{
			5: {ID: 5, UserID: 1, Name: "Contacts", UpdatedAt: now, CTag: 1},
		},
	}
	contactRepo := &fakeContactRepo{
		contacts: map[string]*store.Contact{
			"5:alice": {AddressBookID: 5, UID: "alice", RawVCard: buildVCard("3.0", "UID:alice", "FN:Alice Adams"), ETag: "etag-a", LastModified: now},
		},
	}
	h := &Handler{store: &store.Store{AddressBooks: bookRepo, Contacts: contactRepo}}

	t.Run("Section8_1_ExpandPropertySupportedOnCollection", func(t *testing.T) {
		body := `<?xml version="1.0" encoding="utf-8"?>
<D:expand-property xmlns:D="DAV:">
  <D:prop>
    <D:current-user-principal>
      <D:prop>
        <D:displayname/>
      </D:prop>
    </D:current-user-principal>
  </D:prop>
</D:expand-property>`

		req := httptest.NewRequest("REPORT", "/dav/addressbooks/5/", strings.NewReader(body))
		req.Header.Set("Depth", "0")
		req = req.WithContext(auth.WithUser(req.Context(), user))
		rr := httptest.NewRecorder()

		h.Report(rr, req)

		if rr.Code != http.StatusMultiStatus {
			t.Errorf("RFC 6352 Section 8.1: servers MUST support the expand-property REPORT on address book collections, got %d", rr.Code)
		}
		respBody := rr.Body.String()
		if !strings.Contains(respBody, "/dav/principals/1/") {
			t.Errorf("expand-property on a collection should include the expanded principal href, got %s", respBody)
		}
		if !strings.Contains(respBody, user.PrimaryEmail) {
			t.Errorf("expand-property on a collection should include the expanded principal properties, got %s", respBody)
		}
	})

	t.Run("Section8_1_ExpandPropertySupportedOnAddressObjectResource", func(t *testing.T) {
		body := `<?xml version="1.0" encoding="utf-8"?>
<D:expand-property xmlns:D="DAV:">
  <D:prop>
    <D:current-user-principal>
      <D:prop>
        <D:displayname/>
      </D:prop>
    </D:current-user-principal>
  </D:prop>
</D:expand-property>`

		req := httptest.NewRequest("REPORT", "/dav/addressbooks/5/alice.vcf", strings.NewReader(body))
		req.Header.Set("Depth", "0")
		req = req.WithContext(auth.WithUser(req.Context(), user))
		rr := httptest.NewRecorder()

		h.Report(rr, req)

		if rr.Code != http.StatusMultiStatus {
			t.Errorf("RFC 6352 Sections 3 and 8.1: servers MUST support advertised reports on address object resources as well, got %d", rr.Code)
		}
		if !propstatHasStatus(rr.Body.String(), "current-user-principal", http.StatusNotFound) {
			t.Errorf("expand-property on an address object should report non-expandable href properties as 404, got %s", rr.Body.String())
		}
	})
}

func TestRFC6352_AddressbookQueryReport(t *testing.T) {
	user := &store.User{ID: 1}
	now := store.Now()
	bookRepo := &fakeAddressBookRepo{
		books: map[int64]*store.AddressBook{
			5: {ID: 5, UserID: 1, Name: "Contacts", UpdatedAt: now, CTag: 1},
		},
	}

	aliceVCard := buildVCard("3.0",
		"UID:alice",
		"FN:Alice Adams",
		"EMAIL:alice@example.com",
		"NICKNAME:ally",
		"X-ABC-CUSTOM:custom-one",
	)
	bobVCard := buildVCard("3.0",
		"UID:bob",
		"FN:Bob Brown",
		"EMAIL:bob@example.com",
	)
	carolVCard := buildVCard("3.0",
		"UID:carol",
		"FN:Carol Clark",
		"NICKNAME:cc",
	)

	baseContacts := map[string]*store.Contact{
		"5:alice": {AddressBookID: 5, UID: "alice", RawVCard: aliceVCard, ETag: "etag-a", LastModified: now},
		"5:bob":   {AddressBookID: 5, UID: "bob", RawVCard: bobVCard, ETag: "etag-b", LastModified: now},
		"5:carol": {AddressBookID: 5, UID: "carol", RawVCard: carolVCard, ETag: "etag-c", LastModified: now},
	}

	t.Run("Section8_1_8_6_QuerySupportedOnAddressBookCollections", func(t *testing.T) {
		h := &Handler{store: &store.Store{AddressBooks: bookRepo, Contacts: &fakeContactRepo{contacts: baseContacts}}}

		body := `<?xml version="1.0" encoding="utf-8"?>
<card:addressbook-query xmlns:card="urn:ietf:params:xml:ns:carddav" xmlns:D="DAV:">
  <D:prop>
    <D:getetag/>
    <card:address-data/>
  </D:prop>
  <card:filter>
    <card:prop-filter name="FN"/>
  </card:filter>
</card:addressbook-query>`

		req := httptest.NewRequest("REPORT", "/dav/addressbooks/5/", strings.NewReader(body))
		req.Header.Set("Depth", "1")
		req = req.WithContext(auth.WithUser(req.Context(), user))
		rr := httptest.NewRecorder()

		h.Report(rr, req)

		if rr.Code != http.StatusMultiStatus {
			t.Fatalf("RFC 6352 Section 8.6: addressbook-query on a collection must return 207, got %d", rr.Code)
		}
		respBody := rr.Body.String()
		if !strings.Contains(respBody, "alice.vcf") || !strings.Contains(respBody, "bob.vcf") {
			t.Errorf("RFC 6352 Section 8.6: matching address object resources should be returned, got %s", respBody)
		}
		if !strings.Contains(respBody, "address-data") {
			t.Errorf("RFC 6352 Section 8.6: addressbook-query responses must include address-data when requested, got %s", respBody)
		}
	})

	t.Run("Section8_6_TopLevelAddressDataSelectionIsHonored", func(t *testing.T) {
		h := &Handler{store: &store.Store{AddressBooks: bookRepo, Contacts: &fakeContactRepo{contacts: baseContacts}}}

		body := `<?xml version="1.0" encoding="utf-8"?>
<card:addressbook-query xmlns:card="urn:ietf:params:xml:ns:carddav" xmlns:D="DAV:">
  <card:address-data>
    <card:prop name="FN"/>
  </card:address-data>
  <card:filter>
    <card:prop-filter name="UID">
      <card:text-match collation="i;unicode-casemap" match-type="equals">alice</card:text-match>
    </card:prop-filter>
  </card:filter>
</card:addressbook-query>`

		req := httptest.NewRequest("REPORT", "/dav/addressbooks/5/", strings.NewReader(body))
		req.Header.Set("Depth", "1")
		req = req.WithContext(auth.WithUser(req.Context(), user))
		rr := httptest.NewRecorder()

		h.Report(rr, req)

		if rr.Code != http.StatusMultiStatus {
			t.Fatalf("RFC 6352 Section 8.6: top-level address-data requests must return 207, got %d: %s", rr.Code, rr.Body.String())
		}
		respBody := rr.Body.String()
		if !strings.Contains(respBody, "FN:Alice Adams") {
			t.Fatalf("RFC 6352 Section 10.4: requested vCard properties must be returned for top-level address-data, got %s", respBody)
		}
		for _, unexpected := range []string{"EMAIL:alice@example.com", "TEL:+1-555-0100"} {
			if strings.Contains(respBody, unexpected) {
				t.Fatalf("RFC 6352 Section 10.4: unrequested vCard properties must be omitted for top-level address-data, got %s", respBody)
			}
		}
	})

	t.Run("Section8_6_QueryRequiresDepthHeader", func(t *testing.T) {
		h := &Handler{store: &store.Store{AddressBooks: bookRepo, Contacts: &fakeContactRepo{contacts: baseContacts}}}

		body := `<?xml version="1.0" encoding="utf-8"?>
<card:addressbook-query xmlns:card="urn:ietf:params:xml:ns:carddav">
  <card:filter>
    <card:prop-filter name="FN"/>
  </card:filter>
</card:addressbook-query>`

		req := httptest.NewRequest("REPORT", "/dav/addressbooks/5/", strings.NewReader(body))
		req = req.WithContext(auth.WithUser(req.Context(), user))
		rr := httptest.NewRecorder()

		h.Report(rr, req)

		if rr.Code != http.StatusBadRequest {
			t.Errorf("RFC 6352 Section 8.6: addressbook-query requests MUST include a Depth header and should fail with 400 when omitted, got %d: %s", rr.Code, rr.Body.String())
		}
	})

	t.Run("Section8_6_QuerySupportsStandardVCard4PropertiesInFilters", func(t *testing.T) {
		prodidContacts := map[string]*store.Contact{
			"5:alice": {
				AddressBookID: 5,
				UID:           "alice",
				ResourceName:  "alice",
				RawVCard: buildVCard(
					"4.0",
					"UID:alice",
					"FN:Alice Adams",
					"PRODID:-//CalCard Tests//CardDAV Filter Regression//EN",
				),
				ETag:         "etag-a",
				LastModified: now,
			},
		}
		h := &Handler{store: &store.Store{AddressBooks: bookRepo, Contacts: &fakeContactRepo{contacts: prodidContacts}}}

		body := `<?xml version="1.0" encoding="utf-8"?>
<card:addressbook-query xmlns:card="urn:ietf:params:xml:ns:carddav" xmlns:D="DAV:">
  <D:prop>
    <D:getetag/>
    <card:address-data/>
  </D:prop>
  <card:filter>
    <card:prop-filter name="PRODID">
      <card:text-match collation="i;unicode-casemap" match-type="contains">CardDAV Filter Regression</card:text-match>
    </card:prop-filter>
  </card:filter>
</card:addressbook-query>`

		req := httptest.NewRequest("REPORT", "/dav/addressbooks/5/", strings.NewReader(body))
		req.Header.Set("Depth", "1")
		req = req.WithContext(auth.WithUser(req.Context(), user))
		rr := httptest.NewRecorder()

		h.Report(rr, req)

		if rr.Code != http.StatusMultiStatus {
			t.Fatalf("RFC 6352 Sections 8.6 and 10.5: standard vCard 4.0 filter properties like PRODID must be accepted, got %d: %s", rr.Code, rr.Body.String())
		}
		if !strings.Contains(rr.Body.String(), "alice.vcf") {
			t.Fatalf("expected matching contact in response, got %s", rr.Body.String())
		}
	})

	t.Run("Section8_QueryReportsSupportedOnAddressObjectResources", func(t *testing.T) {
		h := &Handler{store: &store.Store{AddressBooks: bookRepo, Contacts: &fakeContactRepo{contacts: baseContacts}}}

		body := `<?xml version="1.0" encoding="utf-8"?>
<card:addressbook-query xmlns:card="urn:ietf:params:xml:ns:carddav" xmlns:D="DAV:">
  <D:prop>
    <D:getetag/>
    <card:address-data/>
  </D:prop>
  <card:filter>
    <card:prop-filter name="UID">
      <card:text-match collation="i;unicode-casemap" match-type="equals">alice</card:text-match>
    </card:prop-filter>
  </card:filter>
</card:addressbook-query>`

		req := httptest.NewRequest("REPORT", "/dav/addressbooks/5/alice.vcf", strings.NewReader(body))
		req.Header.Set("Depth", "0")
		req = req.WithContext(auth.WithUser(req.Context(), user))
		rr := httptest.NewRecorder()

		h.Report(rr, req)

		if rr.Code != http.StatusMultiStatus {
			t.Errorf("RFC 6352 Section 8: addressbook-query should also be supported on address object resources, got %d", rr.Code)
		}
	})

	t.Run("Section8_6_EmptyMultistatusWhenNoMatches", func(t *testing.T) {
		h := &Handler{store: &store.Store{AddressBooks: bookRepo, Contacts: &fakeContactRepo{contacts: map[string]*store.Contact{}}}}

		body := `<?xml version="1.0" encoding="utf-8"?>
<card:addressbook-query xmlns:card="urn:ietf:params:xml:ns:carddav">
  <card:filter>
    <card:prop-filter name="FN"/>
  </card:filter>
</card:addressbook-query>`

		req := httptest.NewRequest("REPORT", "/dav/addressbooks/5/", strings.NewReader(body))
		req.Header.Set("Depth", "1")
		req = req.WithContext(auth.WithUser(req.Context(), user))
		rr := httptest.NewRecorder()

		h.Report(rr, req)

		if rr.Code != http.StatusMultiStatus {
			t.Fatalf("RFC 6352 Section 8.6: empty queries must still return 207 Multi-Status, got %d", rr.Code)
		}
		if strings.Contains(rr.Body.String(), ".vcf") {
			t.Errorf("RFC 6352 Section 8.6: empty result set should return an empty multistatus without address object responses, got %s", rr.Body.String())
		}
	})

	t.Run("Section8_6_MissingRequestedWebDAVPropertyReturns404Propstat", func(t *testing.T) {
		h := &Handler{store: &store.Store{AddressBooks: bookRepo, Contacts: &fakeContactRepo{contacts: baseContacts}}}

		body := `<?xml version="1.0" encoding="utf-8"?>
<card:addressbook-query xmlns:card="urn:ietf:params:xml:ns:carddav" xmlns:D="DAV:">
  <D:prop>
    <D:displayname/>
  </D:prop>
  <card:filter>
    <card:prop-filter name="UID">
      <card:text-match collation="i;unicode-casemap" match-type="equals">alice</card:text-match>
    </card:prop-filter>
  </card:filter>
</card:addressbook-query>`

		req := httptest.NewRequest("REPORT", "/dav/addressbooks/5/", strings.NewReader(body))
		req.Header.Set("Depth", "1")
		req = req.WithContext(auth.WithUser(req.Context(), user))
		rr := httptest.NewRecorder()

		h.Report(rr, req)

		if !propstatHasStatus(rr.Body.String(), "displayname", http.StatusNotFound) {
			t.Errorf("RFC 6352 Section 8.6: missing WebDAV properties in addressbook-query responses MUST be reported with 404 propstat, got %s", rr.Body.String())
		}
	})

	t.Run("Section8_3_FilteringByTextMatchModes", func(t *testing.T) {
		h := &Handler{store: &store.Store{AddressBooks: bookRepo, Contacts: &fakeContactRepo{contacts: baseContacts}}}

		tests := []struct {
			name      string
			matchType string
			value     string
			wantHref  string
		}{
			{name: "equals", matchType: "equals", value: "Alice Adams", wantHref: "alice.vcf"},
			{name: "contains", matchType: "contains", value: "Alice", wantHref: "alice.vcf"},
			{name: "starts-with", matchType: "starts-with", value: "Alice", wantHref: "alice.vcf"},
			{name: "ends-with", matchType: "ends-with", value: "Adams", wantHref: "alice.vcf"},
		}

		for _, tc := range tests {
			t.Run(tc.name, func(t *testing.T) {
				body := fmt.Sprintf(`<?xml version="1.0" encoding="utf-8"?>
<card:addressbook-query xmlns:card="urn:ietf:params:xml:ns:carddav" xmlns:D="DAV:">
  <D:prop>
    <D:getetag/>
  </D:prop>
  <card:filter>
    <card:prop-filter name="FN">
      <card:text-match collation="i;unicode-casemap" match-type="%s">%s</card:text-match>
    </card:prop-filter>
  </card:filter>
</card:addressbook-query>`, tc.matchType, tc.value)

				req := httptest.NewRequest("REPORT", "/dav/addressbooks/5/", strings.NewReader(body))
				req.Header.Set("Depth", "1")
				req = req.WithContext(auth.WithUser(req.Context(), user))
				rr := httptest.NewRecorder()

				h.Report(rr, req)

				respBody := rr.Body.String()
				if !strings.Contains(respBody, tc.wantHref) {
					t.Errorf("RFC 6352 Sections 8.3 and 10.5.4: %s text-match should find %s, got %s", tc.matchType, tc.wantHref, respBody)
				}
				if strings.Contains(respBody, "bob.vcf") || strings.Contains(respBody, "carol.vcf") {
					t.Errorf("RFC 6352 Sections 8.3 and 10.5.4: %s text-match should exclude non-matching contacts, got %s", tc.matchType, respBody)
				}
			})
		}
	})

	t.Run("Section8_3_DefaultCollationAliasUsesUnicodeCasemap", func(t *testing.T) {
		h := &Handler{store: &store.Store{AddressBooks: bookRepo, Contacts: &fakeContactRepo{contacts: baseContacts}}}

		body := `<?xml version="1.0" encoding="utf-8"?>
<card:addressbook-query xmlns:card="urn:ietf:params:xml:ns:carddav" xmlns:D="DAV:">
  <D:prop>
    <D:getetag/>
  </D:prop>
  <card:filter>
    <card:prop-filter name="FN">
      <card:text-match collation="default" match-type="contains">alice</card:text-match>
    </card:prop-filter>
  </card:filter>
</card:addressbook-query>`

		req := httptest.NewRequest("REPORT", "/dav/addressbooks/5/", strings.NewReader(body))
		req.Header.Set("Depth", "1")
		req = req.WithContext(auth.WithUser(req.Context(), user))
		rr := httptest.NewRecorder()

		h.Report(rr, req)

		if rr.Code != http.StatusMultiStatus {
			t.Fatalf("RFC 6352 Section 8.3: default collation alias should be accepted, got %d: %s", rr.Code, rr.Body.String())
		}
		if !strings.Contains(rr.Body.String(), "alice.vcf") {
			t.Errorf("RFC 6352 Section 8.3: default collation should behave like i;unicode-casemap, got %s", rr.Body.String())
		}
	})

	t.Run("Section10_5_4_MissingCollationDefaultsToUnicodeCasemap", func(t *testing.T) {
		h := &Handler{store: &store.Store{AddressBooks: bookRepo, Contacts: &fakeContactRepo{contacts: baseContacts}}}

		body := `<?xml version="1.0" encoding="utf-8"?>
<card:addressbook-query xmlns:card="urn:ietf:params:xml:ns:carddav" xmlns:D="DAV:">
  <D:prop>
    <D:getetag/>
  </D:prop>
  <card:filter>
    <card:prop-filter name="FN">
      <card:text-match match-type="contains">alice</card:text-match>
    </card:prop-filter>
  </card:filter>
</card:addressbook-query>`

		req := httptest.NewRequest("REPORT", "/dav/addressbooks/5/", strings.NewReader(body))
		req.Header.Set("Depth", "1")
		req = req.WithContext(auth.WithUser(req.Context(), user))
		rr := httptest.NewRecorder()

		h.Report(rr, req)

		if rr.Code != http.StatusMultiStatus {
			t.Fatalf("RFC 6352 Section 10.5.4: omitted collation should default to i;unicode-casemap, got %d: %s", rr.Code, rr.Body.String())
		}
		if !strings.Contains(rr.Body.String(), "alice.vcf") {
			t.Errorf("RFC 6352 Section 10.5.4: omitted collation should still match using i;unicode-casemap, got %s", rr.Body.String())
		}
	})

	t.Run("Section10_5_3_IsNotDefinedFilter", func(t *testing.T) {
		h := &Handler{store: &store.Store{AddressBooks: bookRepo, Contacts: &fakeContactRepo{contacts: baseContacts}}}

		body := `<?xml version="1.0" encoding="utf-8"?>
<card:addressbook-query xmlns:card="urn:ietf:params:xml:ns:carddav" xmlns:D="DAV:">
  <D:prop>
    <D:getetag/>
  </D:prop>
  <card:filter>
    <card:prop-filter name="NICKNAME">
      <card:is-not-defined/>
    </card:prop-filter>
  </card:filter>
</card:addressbook-query>`

		req := httptest.NewRequest("REPORT", "/dav/addressbooks/5/", strings.NewReader(body))
		req.Header.Set("Depth", "1")
		req = req.WithContext(auth.WithUser(req.Context(), user))
		rr := httptest.NewRecorder()

		h.Report(rr, req)

		respBody := rr.Body.String()
		if !strings.Contains(respBody, "bob.vcf") {
			t.Errorf("RFC 6352 Section 10.5.3: is-not-defined should match resources lacking NICKNAME, got %s", respBody)
		}
		if strings.Contains(respBody, "alice.vcf") || strings.Contains(respBody, "carol.vcf") {
			t.Errorf("RFC 6352 Section 10.5.3: is-not-defined should exclude resources defining NICKNAME, got %s", respBody)
		}
	})

	t.Run("Section10_5_FilterTestAttributeAnyOfAndAllOf", func(t *testing.T) {
		h := &Handler{store: &store.Store{AddressBooks: bookRepo, Contacts: &fakeContactRepo{contacts: baseContacts}}}

		defaultBody := `<?xml version="1.0" encoding="utf-8"?>
<card:addressbook-query xmlns:card="urn:ietf:params:xml:ns:carddav" xmlns:D="DAV:">
  <D:prop>
    <D:getetag/>
  </D:prop>
  <card:filter>
    <card:prop-filter name="FN">
      <card:text-match collation="i;unicode-casemap" match-type="contains">Alice</card:text-match>
    </card:prop-filter>
    <card:prop-filter name="EMAIL">
      <card:text-match collation="i;unicode-casemap" match-type="contains">bob@example.com</card:text-match>
    </card:prop-filter>
  </card:filter>
</card:addressbook-query>`

		req := httptest.NewRequest("REPORT", "/dav/addressbooks/5/", strings.NewReader(defaultBody))
		req.Header.Set("Depth", "1")
		req = req.WithContext(auth.WithUser(req.Context(), user))
		rr := httptest.NewRecorder()
		h.Report(rr, req)

		respBody := rr.Body.String()
		if !strings.Contains(respBody, "alice.vcf") || !strings.Contains(respBody, "bob.vcf") {
			t.Errorf("RFC 6352 Section 10.5: filter without test should default to anyof, got %s", respBody)
		}
		if strings.Contains(respBody, "carol.vcf") {
			t.Errorf("RFC 6352 Section 10.5: default filter semantics should not match unrelated resources, got %s", respBody)
		}

		anyOfBody := `<?xml version="1.0" encoding="utf-8"?>
<card:addressbook-query xmlns:card="urn:ietf:params:xml:ns:carddav" xmlns:D="DAV:">
  <D:prop>
    <D:getetag/>
  </D:prop>
  <card:filter test="anyof">
    <card:prop-filter name="FN">
      <card:text-match collation="i;unicode-casemap" match-type="contains">Alice</card:text-match>
    </card:prop-filter>
    <card:prop-filter name="EMAIL">
      <card:text-match collation="i;unicode-casemap" match-type="contains">bob@example.com</card:text-match>
    </card:prop-filter>
  </card:filter>
</card:addressbook-query>`

		req = httptest.NewRequest("REPORT", "/dav/addressbooks/5/", strings.NewReader(anyOfBody))
		req.Header.Set("Depth", "1")
		req = req.WithContext(auth.WithUser(req.Context(), user))
		rr = httptest.NewRecorder()
		h.Report(rr, req)

		respBody = rr.Body.String()
		if !strings.Contains(respBody, "alice.vcf") || !strings.Contains(respBody, "bob.vcf") {
			t.Errorf("RFC 6352 Section 10.5: filter test=anyof should match resources satisfying either prop-filter, got %s", respBody)
		}

		allOfBody := `<?xml version="1.0" encoding="utf-8"?>
<card:addressbook-query xmlns:card="urn:ietf:params:xml:ns:carddav" xmlns:D="DAV:">
  <D:prop>
    <D:getetag/>
  </D:prop>
  <card:filter test="allof">
    <card:prop-filter name="FN">
      <card:text-match collation="i;unicode-casemap" match-type="contains">Alice</card:text-match>
    </card:prop-filter>
    <card:prop-filter name="EMAIL">
      <card:text-match collation="i;unicode-casemap" match-type="contains">bob@example.com</card:text-match>
    </card:prop-filter>
  </card:filter>
</card:addressbook-query>`

		req = httptest.NewRequest("REPORT", "/dav/addressbooks/5/", strings.NewReader(allOfBody))
		req.Header.Set("Depth", "1")
		req = req.WithContext(auth.WithUser(req.Context(), user))
		rr = httptest.NewRecorder()
		h.Report(rr, req)

		if strings.Contains(rr.Body.String(), "alice.vcf") || strings.Contains(rr.Body.String(), "bob.vcf") || strings.Contains(rr.Body.String(), "carol.vcf") {
			t.Errorf("RFC 6352 Section 10.5: filter test=allof should require all prop-filters to match, got %s", rr.Body.String())
		}
	})

	t.Run("Section10_5_2_ParamFilter", func(t *testing.T) {
		homeVCard := buildVCard("3.0", "UID:home", "FN:Home Contact", "EMAIL;TYPE=HOME:home@example.com")
		workVCard := buildVCard("3.0", "UID:work", "FN:Work Contact", "EMAIL;TYPE=WORK:work@example.com")
		contacts := map[string]*store.Contact{
			"5:home": {AddressBookID: 5, UID: "home", RawVCard: homeVCard, ETag: "etag-home", LastModified: now},
			"5:work": {AddressBookID: 5, UID: "work", RawVCard: workVCard, ETag: "etag-work", LastModified: now},
		}
		h := &Handler{store: &store.Store{AddressBooks: bookRepo, Contacts: &fakeContactRepo{contacts: contacts}}}

		body := `<?xml version="1.0" encoding="utf-8"?>
<card:addressbook-query xmlns:card="urn:ietf:params:xml:ns:carddav" xmlns:D="DAV:">
  <D:prop>
    <D:getetag/>
  </D:prop>
  <card:filter>
    <card:prop-filter name="EMAIL">
      <card:param-filter name="TYPE">
        <card:text-match collation="i;unicode-casemap" match-type="equals">HOME</card:text-match>
      </card:param-filter>
    </card:prop-filter>
  </card:filter>
</card:addressbook-query>`

		req := httptest.NewRequest("REPORT", "/dav/addressbooks/5/", strings.NewReader(body))
		req.Header.Set("Depth", "1")
		req = req.WithContext(auth.WithUser(req.Context(), user))
		rr := httptest.NewRecorder()

		h.Report(rr, req)

		respBody := rr.Body.String()
		if !strings.Contains(respBody, "home.vcf") {
			t.Errorf("RFC 6352 Section 10.5.2: param-filter should match resources with matching parameter values, got %s", respBody)
		}
		if strings.Contains(respBody, "work.vcf") {
			t.Errorf("RFC 6352 Section 10.5.2: param-filter should exclude resources whose parameter values do not match, got %s", respBody)
		}
	})

	t.Run("Section10_5_2_ParamFilterIsNotDefined", func(t *testing.T) {
		typedVCard := buildVCard("3.0", "UID:typed", "FN:Typed Contact", "EMAIL;TYPE=HOME:typed@example.com")
		untypedVCard := buildVCard("3.0", "UID:untyped", "FN:Untyped Contact", "EMAIL:untyped@example.com")
		contacts := map[string]*store.Contact{
			"5:typed":   {AddressBookID: 5, UID: "typed", RawVCard: typedVCard, ETag: "etag-typed", LastModified: now},
			"5:untyped": {AddressBookID: 5, UID: "untyped", RawVCard: untypedVCard, ETag: "etag-untyped", LastModified: now},
		}
		h := &Handler{store: &store.Store{AddressBooks: bookRepo, Contacts: &fakeContactRepo{contacts: contacts}}}

		body := `<?xml version="1.0" encoding="utf-8"?>
<card:addressbook-query xmlns:card="urn:ietf:params:xml:ns:carddav" xmlns:D="DAV:">
  <D:prop>
    <D:getetag/>
  </D:prop>
  <card:filter>
    <card:prop-filter name="EMAIL">
      <card:param-filter name="TYPE">
        <card:is-not-defined/>
      </card:param-filter>
    </card:prop-filter>
  </card:filter>
</card:addressbook-query>`

		req := httptest.NewRequest("REPORT", "/dav/addressbooks/5/", strings.NewReader(body))
		req.Header.Set("Depth", "1")
		req = req.WithContext(auth.WithUser(req.Context(), user))
		rr := httptest.NewRecorder()

		h.Report(rr, req)

		respBody := rr.Body.String()
		if !strings.Contains(respBody, "untyped.vcf") {
			t.Errorf("RFC 6352 Sections 10.5.2 and 10.5.3: param-filter is-not-defined should match resources lacking TYPE, got %s", respBody)
		}
		if strings.Contains(respBody, "/dav/addressbooks/5/typed.vcf") {
			t.Errorf("RFC 6352 Sections 10.5.2 and 10.5.3: param-filter is-not-defined should exclude resources with TYPE, got %s", respBody)
		}
	})

	t.Run("Section10_5_4_NegateCondition", func(t *testing.T) {
		h := &Handler{store: &store.Store{AddressBooks: bookRepo, Contacts: &fakeContactRepo{contacts: baseContacts}}}

		body := `<?xml version="1.0" encoding="utf-8"?>
<card:addressbook-query xmlns:card="urn:ietf:params:xml:ns:carddav" xmlns:D="DAV:">
  <D:prop>
    <D:getetag/>
  </D:prop>
  <card:filter>
    <card:prop-filter name="FN">
      <card:text-match collation="i;unicode-casemap" match-type="contains" negate-condition="yes">Bob</card:text-match>
    </card:prop-filter>
  </card:filter>
</card:addressbook-query>`

		req := httptest.NewRequest("REPORT", "/dav/addressbooks/5/", strings.NewReader(body))
		req.Header.Set("Depth", "1")
		req = req.WithContext(auth.WithUser(req.Context(), user))
		rr := httptest.NewRecorder()

		h.Report(rr, req)

		respBody := rr.Body.String()
		if strings.Contains(respBody, "bob.vcf") {
			t.Errorf("RFC 6352 Section 10.5.4: negate-condition=yes should invert the text-match result, got %s", respBody)
		}
	})

	t.Run("Section8_3_RejectsUnsupportedCollation", func(t *testing.T) {
		h := &Handler{store: &store.Store{AddressBooks: bookRepo, Contacts: &fakeContactRepo{contacts: baseContacts}}}

		body := `<?xml version="1.0" encoding="utf-8"?>
<card:addressbook-query xmlns:card="urn:ietf:params:xml:ns:carddav" xmlns:D="DAV:">
  <D:prop>
    <D:getetag/>
  </D:prop>
  <card:filter>
    <card:prop-filter name="FN">
      <card:text-match collation="i;not-supported" match-type="contains">alice</card:text-match>
    </card:prop-filter>
  </card:filter>
</card:addressbook-query>`

		req := httptest.NewRequest("REPORT", "/dav/addressbooks/5/", strings.NewReader(body))
		req.Header.Set("Depth", "1")
		req = req.WithContext(auth.WithUser(req.Context(), user))
		rr := httptest.NewRecorder()

		h.Report(rr, req)

		assertCardDAVPreconditionStatus(t, rr, http.StatusNotImplemented, "supported-collation")
	})

	t.Run("Section8_3_RejectsWildcardCollationIdentifiers", func(t *testing.T) {
		h := &Handler{store: &store.Store{AddressBooks: bookRepo, Contacts: &fakeContactRepo{contacts: baseContacts}}}

		body := `<?xml version="1.0" encoding="utf-8"?>
<card:addressbook-query xmlns:card="urn:ietf:params:xml:ns:carddav" xmlns:D="DAV:">
  <D:prop>
    <D:getetag/>
  </D:prop>
  <card:filter>
    <card:prop-filter name="FN">
      <card:text-match collation="i;unicode-*" match-type="contains">alice</card:text-match>
    </card:prop-filter>
  </card:filter>
</card:addressbook-query>`

		req := httptest.NewRequest("REPORT", "/dav/addressbooks/5/", strings.NewReader(body))
		req.Header.Set("Depth", "1")
		req = req.WithContext(auth.WithUser(req.Context(), user))
		rr := httptest.NewRecorder()

		h.Report(rr, req)

		assertCardDAVPreconditionStatus(t, rr, http.StatusNotImplemented, "supported-collation")
	})

	t.Run("Section8_6_RejectsUnsupportedRequestedAddressDataType", func(t *testing.T) {
		h := &Handler{store: &store.Store{AddressBooks: bookRepo, Contacts: &fakeContactRepo{contacts: baseContacts}}}

		body := `<?xml version="1.0" encoding="utf-8"?>
<card:addressbook-query xmlns:card="urn:ietf:params:xml:ns:carddav" xmlns:D="DAV:">
  <D:prop>
    <card:address-data content-type="application/json" version="1.0"/>
  </D:prop>
  <card:filter>
    <card:prop-filter name="FN"/>
  </card:filter>
</card:addressbook-query>`

		req := httptest.NewRequest("REPORT", "/dav/addressbooks/5/", strings.NewReader(body))
		req.Header.Set("Depth", "1")
		req = req.WithContext(auth.WithUser(req.Context(), user))
		rr := httptest.NewRecorder()

		h.Report(rr, req)

		assertCardDAVPreconditionStatus(t, rr, http.StatusUnsupportedMediaType, "supported-address-data")
	})

	t.Run("Section8_4_PartialRetrievalAllprop", func(t *testing.T) {
		h := &Handler{store: &store.Store{AddressBooks: bookRepo, Contacts: &fakeContactRepo{contacts: baseContacts}}}

		body := `<?xml version="1.0" encoding="utf-8"?>
<card:addressbook-query xmlns:card="urn:ietf:params:xml:ns:carddav" xmlns:D="DAV:">
  <D:prop>
    <card:address-data>
      <card:allprop/>
    </card:address-data>
  </D:prop>
  <card:filter>
    <card:prop-filter name="UID">
      <card:text-match collation="i;unicode-casemap" match-type="equals">alice</card:text-match>
    </card:prop-filter>
  </card:filter>
</card:addressbook-query>`

		req := httptest.NewRequest("REPORT", "/dav/addressbooks/5/", strings.NewReader(body))
		req.Header.Set("Depth", "1")
		req = req.WithContext(auth.WithUser(req.Context(), user))
		rr := httptest.NewRecorder()

		h.Report(rr, req)

		respBody := rr.Body.String()
		if !strings.Contains(respBody, "EMAIL:alice@example.com") || !strings.Contains(respBody, "X-ABC-CUSTOM:custom-one") {
			t.Errorf("RFC 6352 Section 8.4 and 10.4.1: card:allprop should return all vCard properties, got %s", respBody)
		}
	})

	t.Run("Section8_4_SelectedPropertiesPartialRetrieval", func(t *testing.T) {
		h := &Handler{store: &store.Store{AddressBooks: bookRepo, Contacts: &fakeContactRepo{contacts: baseContacts}}}

		body := `<?xml version="1.0" encoding="utf-8"?>
<card:addressbook-query xmlns:card="urn:ietf:params:xml:ns:carddav" xmlns:D="DAV:">
  <D:prop>
    <card:address-data>
      <card:prop name="UID"/>
      <card:prop name="FN"/>
    </card:address-data>
  </D:prop>
  <card:filter>
    <card:prop-filter name="UID">
      <card:text-match collation="i;unicode-casemap" match-type="equals">alice</card:text-match>
    </card:prop-filter>
  </card:filter>
</card:addressbook-query>`

		req := httptest.NewRequest("REPORT", "/dav/addressbooks/5/", strings.NewReader(body))
		req.Header.Set("Depth", "1")
		req = req.WithContext(auth.WithUser(req.Context(), user))
		rr := httptest.NewRecorder()

		h.Report(rr, req)

		respBody := rr.Body.String()
		if !strings.Contains(respBody, "UID:alice") || !strings.Contains(respBody, "FN:Alice Adams") {
			t.Errorf("RFC 6352 Section 8.4 and 10.4.2: selected properties must be returned, got %s", respBody)
		}
		if strings.Contains(respBody, "EMAIL:alice@example.com") {
			t.Errorf("RFC 6352 Section 8.4 and 10.4.2: unrequested properties should be omitted during partial retrieval, got %s", respBody)
		}
	})

	t.Run("Section10_4_2_GroupPrefixedPropertySelection", func(t *testing.T) {
		groupedVCard := buildVCard(
			"3.0",
			"UID:grouped",
			"FN:Grouped Example",
			"TEL:222",
			"X-ABC.TEL:111",
			"X-DEF.TEL:333",
		)
		contacts := map[string]*store.Contact{
			"5:grouped": {AddressBookID: 5, UID: "grouped", RawVCard: groupedVCard, ETag: "etag-grouped", LastModified: now},
		}
		h := &Handler{store: &store.Store{AddressBooks: bookRepo, Contacts: &fakeContactRepo{contacts: contacts}}}

		ungroupedBody := `<?xml version="1.0" encoding="utf-8"?>
<card:addressbook-query xmlns:card="urn:ietf:params:xml:ns:carddav" xmlns:D="DAV:">
  <D:prop>
    <card:address-data>
      <card:prop name="TEL"/>
    </card:address-data>
  </D:prop>
  <card:filter>
    <card:prop-filter name="UID">
      <card:text-match collation="i;unicode-casemap" match-type="equals">grouped</card:text-match>
    </card:prop-filter>
  </card:filter>
</card:addressbook-query>`

		req := httptest.NewRequest("REPORT", "/dav/addressbooks/5/", strings.NewReader(ungroupedBody))
		req.Header.Set("Depth", "1")
		req = req.WithContext(auth.WithUser(req.Context(), user))
		rr := httptest.NewRecorder()

		h.Report(rr, req)

		respBody := rr.Body.String()
		if !strings.Contains(respBody, "TEL:222") || !strings.Contains(respBody, "X-ABC.TEL:111") || !strings.Contains(respBody, "X-DEF.TEL:333") {
			t.Errorf("RFC 6352 Section 10.4.2: an ungrouped card:prop name must match grouped and ungrouped vCard properties, got %s", respBody)
		}

		exactGroupBody := `<?xml version="1.0" encoding="utf-8"?>
<card:addressbook-query xmlns:card="urn:ietf:params:xml:ns:carddav" xmlns:D="DAV:">
  <D:prop>
    <card:address-data>
      <card:prop name="X-ABC.TEL"/>
    </card:address-data>
  </D:prop>
  <card:filter>
    <card:prop-filter name="UID">
      <card:text-match collation="i;unicode-casemap" match-type="equals">grouped</card:text-match>
    </card:prop-filter>
  </card:filter>
</card:addressbook-query>`

		req = httptest.NewRequest("REPORT", "/dav/addressbooks/5/", strings.NewReader(exactGroupBody))
		req.Header.Set("Depth", "1")
		req = req.WithContext(auth.WithUser(req.Context(), user))
		rr = httptest.NewRecorder()

		h.Report(rr, req)

		respBody = rr.Body.String()
		if !strings.Contains(respBody, "X-ABC.TEL:111") {
			t.Errorf("RFC 6352 Section 10.4.2: an exact grouped card:prop name must match the same group prefix, got %s", respBody)
		}
		if strings.Contains(respBody, "TEL:222") || strings.Contains(respBody, "X-DEF.TEL:333") {
			t.Errorf("RFC 6352 Section 10.4.2: an exact grouped card:prop name must not match other group prefixes, got %s", respBody)
		}
	})

	t.Run("Section10_4_2_NovalueSelection", func(t *testing.T) {
		h := &Handler{store: &store.Store{AddressBooks: bookRepo, Contacts: &fakeContactRepo{contacts: baseContacts}}}

		body := `<?xml version="1.0" encoding="utf-8"?>
<card:addressbook-query xmlns:card="urn:ietf:params:xml:ns:carddav" xmlns:D="DAV:">
  <D:prop>
    <card:address-data>
      <card:prop name="EMAIL" novalue="yes"/>
    </card:address-data>
  </D:prop>
  <card:filter>
    <card:prop-filter name="UID">
      <card:text-match collation="i;unicode-casemap" match-type="equals">alice</card:text-match>
    </card:prop-filter>
  </card:filter>
</card:addressbook-query>`

		req := httptest.NewRequest("REPORT", "/dav/addressbooks/5/", strings.NewReader(body))
		req.Header.Set("Depth", "1")
		req = req.WithContext(auth.WithUser(req.Context(), user))
		rr := httptest.NewRecorder()

		h.Report(rr, req)

		respBody := rr.Body.String()
		if !strings.Contains(respBody, "EMAIL:") {
			t.Errorf("RFC 6352 Section 10.4.2: novalue=yes should still return the property name, got %s", respBody)
		}
		if strings.Contains(respBody, "EMAIL:alice@example.com") {
			t.Errorf("RFC 6352 Section 10.4.2: novalue=yes should suppress the property value, got %s", respBody)
		}
	})

	t.Run("Section8_5_NonStandardPropertySelection", func(t *testing.T) {
		h := &Handler{store: &store.Store{AddressBooks: bookRepo, Contacts: &fakeContactRepo{contacts: baseContacts}}}

		body := `<?xml version="1.0" encoding="utf-8"?>
<card:addressbook-query xmlns:card="urn:ietf:params:xml:ns:carddav" xmlns:D="DAV:">
  <D:prop>
    <card:address-data>
      <card:prop name="X-ABC-CUSTOM"/>
    </card:address-data>
  </D:prop>
  <card:filter>
    <card:prop-filter name="UID">
      <card:text-match collation="i;unicode-casemap" match-type="equals">alice</card:text-match>
    </card:prop-filter>
  </card:filter>
</card:addressbook-query>`

		req := httptest.NewRequest("REPORT", "/dav/addressbooks/5/", strings.NewReader(body))
		req.Header.Set("Depth", "1")
		req = req.WithContext(auth.WithUser(req.Context(), user))
		rr := httptest.NewRecorder()

		h.Report(rr, req)

		if !strings.Contains(rr.Body.String(), "X-ABC-CUSTOM:custom-one") {
			t.Errorf("RFC 6352 Section 8.5: servers MUST support non-standard property names in card:address-data selection, got %s", rr.Body.String())
		}
	})

	t.Run("Section8_5_UnsupportedInvalidPropertyFilterFails", func(t *testing.T) {
		h := &Handler{store: &store.Store{AddressBooks: bookRepo, Contacts: &fakeContactRepo{contacts: baseContacts}}}

		// Use a property name that is neither a standard vCard property nor an X- extension
		body := `<?xml version="1.0" encoding="utf-8"?>
<card:addressbook-query xmlns:card="urn:ietf:params:xml:ns:carddav" xmlns:D="DAV:">
  <D:prop>
    <D:getetag/>
  </D:prop>
  <card:filter>
    <card:prop-filter name="INVALID-NOT-STANDARD">
      <card:text-match collation="i;unicode-casemap" match-type="contains">foo</card:text-match>
    </card:prop-filter>
  </card:filter>
</card:addressbook-query>`

		req := httptest.NewRequest("REPORT", "/dav/addressbooks/5/", strings.NewReader(body))
		req.Header.Set("Depth", "1")
		req = req.WithContext(auth.WithUser(req.Context(), user))
		rr := httptest.NewRecorder()

		h.Report(rr, req)

		assertCardDAVPreconditionStatus(t, rr, http.StatusBadRequest, "supported-filter")
	})

	t.Run("Section8_5_UnsupportedParamFilterFailsWithSupportedFilter", func(t *testing.T) {
		h := &Handler{store: &store.Store{AddressBooks: bookRepo, Contacts: &fakeContactRepo{contacts: baseContacts}}}

		body := `<?xml version="1.0" encoding="utf-8"?>
<card:addressbook-query xmlns:card="urn:ietf:params:xml:ns:carddav" xmlns:D="DAV:">
  <D:prop>
    <D:getetag/>
  </D:prop>
  <card:filter>
    <card:prop-filter name="EMAIL">
      <card:param-filter name="INVALID-PARAM">
        <card:text-match collation="i;unicode-casemap" match-type="contains">home</card:text-match>
      </card:param-filter>
    </card:prop-filter>
  </card:filter>
</card:addressbook-query>`

		req := httptest.NewRequest("REPORT", "/dav/addressbooks/5/", strings.NewReader(body))
		req.Header.Set("Depth", "1")
		req = req.WithContext(auth.WithUser(req.Context(), user))
		rr := httptest.NewRecorder()

		h.Report(rr, req)

		assertCardDAVPreconditionStatus(t, rr, http.StatusBadRequest, "supported-filter")
	})

	t.Run("Section8_5_XPropertyFilterWorks", func(t *testing.T) {
		xPropVCard := buildVCard("3.0", "UID:x-prop-user", "FN:X-Prop User", "X-CUSTOM-FIELD:special-value")
		noXPropVCard := buildVCard("3.0", "UID:no-x-prop", "FN:No X-Prop", "TEL:555-1234")
		contacts := map[string]*store.Contact{
			"5:x-prop-user": {AddressBookID: 5, UID: "x-prop-user", ResourceName: "x-prop-user", RawVCard: xPropVCard, ETag: "etag-xp", LastModified: now},
			"5:no-x-prop":   {AddressBookID: 5, UID: "no-x-prop", ResourceName: "no-x-prop", RawVCard: noXPropVCard, ETag: "etag-nxp", LastModified: now},
		}
		h := &Handler{store: &store.Store{AddressBooks: bookRepo, Contacts: &fakeContactRepo{contacts: contacts}}}

		body := `<?xml version="1.0" encoding="utf-8"?>
<card:addressbook-query xmlns:card="urn:ietf:params:xml:ns:carddav" xmlns:D="DAV:">
  <D:prop>
    <D:getetag/>
  </D:prop>
  <card:filter>
    <card:prop-filter name="X-CUSTOM-FIELD">
      <card:text-match collation="i;unicode-casemap" match-type="contains">special</card:text-match>
    </card:prop-filter>
  </card:filter>
</card:addressbook-query>`

		req := httptest.NewRequest("REPORT", "/dav/addressbooks/5/", strings.NewReader(body))
		req.Header.Set("Depth", "1")
		req = req.WithContext(auth.WithUser(req.Context(), user))
		rr := httptest.NewRecorder()

		h.Report(rr, req)

		if rr.Code != http.StatusMultiStatus {
			t.Fatalf("RFC 6352 Section 8.5: X-property filter should succeed, got %d: %s", rr.Code, rr.Body.String())
		}
		respBody := rr.Body.String()
		if !strings.Contains(respBody, "x-prop-user.vcf") {
			t.Errorf("RFC 6352 Section 8.5: X-property filter should match contact with X-CUSTOM-FIELD, got %s", respBody)
		}
		if strings.Contains(respBody, "no-x-prop.vcf") {
			t.Errorf("RFC 6352 Section 8.5: X-property filter should NOT match contact without X-CUSTOM-FIELD, got %s", respBody)
		}
	})

	t.Run("Section10_5_1_GroupPrefixedPropertyFilterMatching", func(t *testing.T) {
		groupedA := buildVCard("3.0", "UID:grouped-a", "FN:Grouped A", "X-ABC.TEL:111")
		groupedB := buildVCard("3.0", "UID:grouped-b", "FN:Grouped B", "X-DEF.TEL:111")
		plain := buildVCard("3.0", "UID:plain", "FN:Plain", "TEL:111")
		contacts := map[string]*store.Contact{
			"5:grouped-a": {AddressBookID: 5, UID: "grouped-a", RawVCard: groupedA, ETag: "etag-grouped-a", LastModified: now},
			"5:grouped-b": {AddressBookID: 5, UID: "grouped-b", RawVCard: groupedB, ETag: "etag-grouped-b", LastModified: now},
			"5:plain":     {AddressBookID: 5, UID: "plain", RawVCard: plain, ETag: "etag-plain", LastModified: now},
		}
		h := &Handler{store: &store.Store{AddressBooks: bookRepo, Contacts: &fakeContactRepo{contacts: contacts}}}

		ungroupedBody := `<?xml version="1.0" encoding="utf-8"?>
<card:addressbook-query xmlns:card="urn:ietf:params:xml:ns:carddav" xmlns:D="DAV:">
  <D:prop>
    <D:getetag/>
  </D:prop>
  <card:filter>
    <card:prop-filter name="TEL">
      <card:text-match collation="i;unicode-casemap" match-type="equals">111</card:text-match>
    </card:prop-filter>
  </card:filter>
</card:addressbook-query>`

		req := httptest.NewRequest("REPORT", "/dav/addressbooks/5/", strings.NewReader(ungroupedBody))
		req.Header.Set("Depth", "1")
		req = req.WithContext(auth.WithUser(req.Context(), user))
		rr := httptest.NewRecorder()

		h.Report(rr, req)

		respBody := rr.Body.String()
		for _, want := range []string{"grouped-a.vcf", "grouped-b.vcf", "plain.vcf"} {
			if !strings.Contains(respBody, want) {
				t.Errorf("RFC 6352 Section 10.5.1: an ungrouped prop-filter name must match grouped and ungrouped properties, missing %s in %s", want, respBody)
			}
		}

		exactGroupBody := `<?xml version="1.0" encoding="utf-8"?>
<card:addressbook-query xmlns:card="urn:ietf:params:xml:ns:carddav" xmlns:D="DAV:">
  <D:prop>
    <D:getetag/>
  </D:prop>
  <card:filter>
    <card:prop-filter name="X-ABC.TEL">
      <card:text-match collation="i;unicode-casemap" match-type="equals">111</card:text-match>
    </card:prop-filter>
  </card:filter>
</card:addressbook-query>`

		req = httptest.NewRequest("REPORT", "/dav/addressbooks/5/", strings.NewReader(exactGroupBody))
		req.Header.Set("Depth", "1")
		req = req.WithContext(auth.WithUser(req.Context(), user))
		rr = httptest.NewRecorder()

		h.Report(rr, req)

		respBody = rr.Body.String()
		if !strings.Contains(respBody, "grouped-a.vcf") {
			t.Errorf("RFC 6352 Section 10.5.1: an exact grouped prop-filter must match the same group prefix, got %s", respBody)
		}
		if strings.Contains(respBody, "grouped-b.vcf") || strings.Contains(respBody, "plain.vcf") {
			t.Errorf("RFC 6352 Section 10.5.1: an exact grouped prop-filter must not match other groups or ungrouped properties, got %s", respBody)
		}
	})

	t.Run("Section8_6_1_And_8_6_2_ClientLimitHandling", func(t *testing.T) {
		h := &Handler{store: &store.Store{AddressBooks: bookRepo, Contacts: &fakeContactRepo{contacts: baseContacts}}}

		body := `<?xml version="1.0" encoding="utf-8"?>
<card:addressbook-query xmlns:card="urn:ietf:params:xml:ns:carddav" xmlns:D="DAV:">
  <D:prop>
    <D:getetag/>
  </D:prop>
  <card:filter>
    <card:prop-filter name="FN"/>
  </card:filter>
  <card:limit>
    <card:nresults>1</card:nresults>
  </card:limit>
</card:addressbook-query>`

		req := httptest.NewRequest("REPORT", "/dav/addressbooks/5/", strings.NewReader(body))
		req.Header.Set("Depth", "1")
		req = req.WithContext(auth.WithUser(req.Context(), user))
		rr := httptest.NewRecorder()

		h.Report(rr, req)

		respBody := rr.Body.String()
		matchResponses := strings.Count(respBody, ".vcf")
		if matchResponses <= 1 {
			if !strings.Contains(respBody, "507 Insufficient Storage") {
				t.Errorf("RFC 6352 Section 8.6.2: truncated query results must include a 507 response for the Request-URI, got %s", respBody)
			}
			if !strings.Contains(respBody, "d:number-of-matches-within-limits") {
				t.Errorf("RFC 6352 Section 8.6.2: truncated responses should include the DAV:number-of-matches-within-limits element, got %s", respBody)
			}
			if strings.Contains(respBody, "card:number-of-matches-within-limits") {
				t.Errorf("RFC 6352 Section 8.6.2: number-of-matches-within-limits must be in the DAV namespace, got %s", respBody)
			}
			return
		}

		t.Log("RFC 6352 Section 8.6.1: server ignored the client limit, which the RFC explicitly permits")
	})
}

func TestRFC6352_AddressbookMultigetReport(t *testing.T) {
	user := &store.User{ID: 1}
	now := store.Now()
	bookRepo := &fakeAddressBookRepo{
		books: map[int64]*store.AddressBook{
			5: {ID: 5, UserID: 1, Name: "Contacts", UpdatedAt: now, CTag: 1},
		},
	}

	aliceVCard := buildVCard("3.0", "UID:alice", "FN:Alice Adams", "EMAIL:alice@example.com", "X-ABC-CUSTOM:custom-one")
	bobVCard := buildVCard("3.0", "UID:bob", "FN:Bob Brown", "EMAIL:bob@example.com")
	baseContacts := map[string]*store.Contact{
		"5:alice": {AddressBookID: 5, UID: "alice", RawVCard: aliceVCard, ETag: "etag-a", LastModified: now},
		"5:bob":   {AddressBookID: 5, UID: "bob", RawVCard: bobVCard, ETag: "etag-b", LastModified: now},
	}

	t.Run("Section8_7_RequestMustIncludeAtLeastOneHref", func(t *testing.T) {
		h := &Handler{store: &store.Store{AddressBooks: bookRepo, Contacts: &fakeContactRepo{contacts: baseContacts}}}

		body := `<?xml version="1.0" encoding="utf-8"?>
<card:addressbook-multiget xmlns:card="urn:ietf:params:xml:ns:carddav" xmlns:D="DAV:"/>`

		req := httptest.NewRequest("REPORT", "/dav/addressbooks/5/", strings.NewReader(body))
		req.Header.Set("Depth", "0")
		req = req.WithContext(auth.WithUser(req.Context(), user))
		rr := httptest.NewRecorder()

		h.Report(rr, req)

		if rr.Code == http.StatusMultiStatus {
			t.Errorf("RFC 6352 Section 8.7: addressbook-multiget request bodies MUST contain at least one DAV:href, got %d", rr.Code)
		}
	})

	t.Run("Section8_7_MultigetSupportedOnAddressBookCollections", func(t *testing.T) {
		h := &Handler{store: &store.Store{AddressBooks: bookRepo, Contacts: &fakeContactRepo{contacts: baseContacts}}}

		body := `<?xml version="1.0" encoding="utf-8"?>
<card:addressbook-multiget xmlns:card="urn:ietf:params:xml:ns:carddav" xmlns:D="DAV:">
  <D:href>/dav/addressbooks/5/alice.vcf</D:href>
  <D:href>/dav/addressbooks/5/bob.vcf</D:href>
</card:addressbook-multiget>`

		req := httptest.NewRequest("REPORT", "/dav/addressbooks/5/", strings.NewReader(body))
		req.Header.Set("Depth", "0")
		req = req.WithContext(auth.WithUser(req.Context(), user))
		rr := httptest.NewRecorder()

		h.Report(rr, req)

		if rr.Code != http.StatusMultiStatus {
			t.Fatalf("RFC 6352 Section 8.7: addressbook-multiget on a collection must return 207, got %d", rr.Code)
		}
		respBody := rr.Body.String()
		if !strings.Contains(respBody, "alice.vcf") || !strings.Contains(respBody, "bob.vcf") {
			t.Errorf("RFC 6352 Section 8.7: a successful multiget must return a response for each requested href, got %s", respBody)
		}
	})

	t.Run("Section8_7_MultigetSupportedOnAddressObjectResources", func(t *testing.T) {
		h := &Handler{store: &store.Store{AddressBooks: bookRepo, Contacts: &fakeContactRepo{contacts: baseContacts}}}

		body := `<?xml version="1.0" encoding="utf-8"?>
<card:addressbook-multiget xmlns:card="urn:ietf:params:xml:ns:carddav" xmlns:D="DAV:">
  <D:href>/dav/addressbooks/5/alice.vcf</D:href>
</card:addressbook-multiget>`

		req := httptest.NewRequest("REPORT", "/dav/addressbooks/5/alice.vcf", strings.NewReader(body))
		req.Header.Set("Depth", "0")
		req = req.WithContext(auth.WithUser(req.Context(), user))
		rr := httptest.NewRecorder()

		h.Report(rr, req)

		if rr.Code != http.StatusMultiStatus {
			t.Errorf("RFC 6352 Section 8: addressbook-multiget should also be supported on address object resources, got %d", rr.Code)
		}
	})

	t.Run("Section8_7_MultigetRequiresDepthZero", func(t *testing.T) {
		h := &Handler{store: &store.Store{AddressBooks: bookRepo, Contacts: &fakeContactRepo{contacts: baseContacts}}}

		body := `<?xml version="1.0" encoding="utf-8"?>
<card:addressbook-multiget xmlns:card="urn:ietf:params:xml:ns:carddav" xmlns:D="DAV:">
  <D:href>/dav/addressbooks/5/alice.vcf</D:href>
</card:addressbook-multiget>`

		req := httptest.NewRequest("REPORT", "/dav/addressbooks/5/", strings.NewReader(body))
		req.Header.Set("Depth", "1")
		req = req.WithContext(auth.WithUser(req.Context(), user))
		rr := httptest.NewRecorder()

		h.Report(rr, req)

		if rr.Code == http.StatusMultiStatus {
			t.Errorf("RFC 6352 Section 8.7: addressbook-multiget requests MUST include Depth: 0 and should fail otherwise")
		}
	})

	t.Run("Section8_7_MultigetRequiresDepthHeader", func(t *testing.T) {
		h := &Handler{store: &store.Store{AddressBooks: bookRepo, Contacts: &fakeContactRepo{contacts: baseContacts}}}

		body := `<?xml version="1.0" encoding="utf-8"?>
<card:addressbook-multiget xmlns:card="urn:ietf:params:xml:ns:carddav" xmlns:D="DAV:">
  <D:href>/dav/addressbooks/5/alice.vcf</D:href>
</card:addressbook-multiget>`

		req := httptest.NewRequest("REPORT", "/dav/addressbooks/5/", strings.NewReader(body))
		req = req.WithContext(auth.WithUser(req.Context(), user))
		rr := httptest.NewRecorder()

		h.Report(rr, req)

		if rr.Code == http.StatusMultiStatus {
			t.Errorf("RFC 6352 Section 8.7: addressbook-multiget requests MUST fail when the Depth header is omitted")
		}
	})

	t.Run("Section8_7_MissingHrefReturns404ForThatResponse", func(t *testing.T) {
		h := &Handler{store: &store.Store{AddressBooks: bookRepo, Contacts: &fakeContactRepo{contacts: baseContacts}}}

		body := `<?xml version="1.0" encoding="utf-8"?>
<card:addressbook-multiget xmlns:card="urn:ietf:params:xml:ns:carddav" xmlns:D="DAV:">
  <D:href>/dav/addressbooks/5/alice.vcf</D:href>
  <D:href>/dav/addressbooks/5/missing.vcf</D:href>
</card:addressbook-multiget>`

		req := httptest.NewRequest("REPORT", "/dav/addressbooks/5/", strings.NewReader(body))
		req.Header.Set("Depth", "0")
		req = req.WithContext(auth.WithUser(req.Context(), user))
		rr := httptest.NewRecorder()

		h.Report(rr, req)

		if !strings.Contains(rr.Body.String(), "HTTP/1.1 404 Not Found") {
			t.Errorf("RFC 6352 Section 8.7: missing hrefs must be reported with per-resource 404 statuses, got %s", rr.Body.String())
		}
	})

	t.Run("Section8_7_OutOfScopeHrefReturnsPerResourceError", func(t *testing.T) {
		h := &Handler{store: &store.Store{AddressBooks: bookRepo, Contacts: &fakeContactRepo{contacts: baseContacts}}}

		body := `<?xml version="1.0" encoding="utf-8"?>
<card:addressbook-multiget xmlns:card="urn:ietf:params:xml:ns:carddav" xmlns:D="DAV:">
  <D:href>/dav/addressbooks/9/other.vcf</D:href>
</card:addressbook-multiget>`

		req := httptest.NewRequest("REPORT", "/dav/addressbooks/5/", strings.NewReader(body))
		req.Header.Set("Depth", "0")
		req = req.WithContext(auth.WithUser(req.Context(), user))
		rr := httptest.NewRecorder()

		h.Report(rr, req)

		if rr.Code != http.StatusMultiStatus {
			t.Fatalf("RFC 6352 Section 8.7: out-of-scope hrefs must still produce a multistatus response, got %d: %s", rr.Code, rr.Body.String())
		}
		respBody := rr.Body.String()
		if !strings.Contains(respBody, "/dav/addressbooks/9/other.vcf") || !strings.Contains(respBody, "HTTP/1.1 404 Not Found") {
			t.Fatalf("RFC 6352 Section 8.7: each requested href must receive a corresponding error response when it is outside the request scope, got %s", respBody)
		}
	})

	t.Run("Section8_7_SelectedPropertiesPartialRetrieval", func(t *testing.T) {
		h := &Handler{store: &store.Store{AddressBooks: bookRepo, Contacts: &fakeContactRepo{contacts: baseContacts}}}

		body := `<?xml version="1.0" encoding="utf-8"?>
<card:addressbook-multiget xmlns:card="urn:ietf:params:xml:ns:carddav" xmlns:D="DAV:">
  <D:prop>
    <card:address-data>
      <card:prop name="UID"/>
    </card:address-data>
  </D:prop>
  <D:href>/dav/addressbooks/5/alice.vcf</D:href>
</card:addressbook-multiget>`

		req := httptest.NewRequest("REPORT", "/dav/addressbooks/5/", strings.NewReader(body))
		req.Header.Set("Depth", "0")
		req = req.WithContext(auth.WithUser(req.Context(), user))
		rr := httptest.NewRecorder()

		h.Report(rr, req)

		respBody := rr.Body.String()
		if !strings.Contains(respBody, "UID:alice") {
			t.Errorf("RFC 6352 Sections 8.4 and 8.7: selected vCard properties must be returned in multiget responses, got %s", respBody)
		}
		if strings.Contains(respBody, "EMAIL:alice@example.com") {
			t.Errorf("RFC 6352 Sections 8.4 and 8.7: unrequested properties should be omitted from partial multiget responses, got %s", respBody)
		}
	})

	t.Run("Section8_7_PerResourceVersionConversionFailureUsesSupportedAddressDataConversion", func(t *testing.T) {
		h := &Handler{store: &store.Store{
			AddressBooks: bookRepo,
			Contacts: &fakeContactRepo{contacts: map[string]*store.Contact{
				"5:alice-v4": {AddressBookID: 5, UID: "alice-v4", ResourceName: "alice-v4", RawVCard: buildVCard("4.0", "UID:alice-v4", "FN:Alice Example"), ETag: "etag-v4"},
			}},
		}}

		body := `<?xml version="1.0" encoding="utf-8"?>
<card:addressbook-multiget xmlns:card="urn:ietf:params:xml:ns:carddav" xmlns:D="DAV:">
  <D:prop>
    <D:getetag/>
    <card:address-data content-type="text/vcard" version="3.0"/>
  </D:prop>
  <D:href>/dav/addressbooks/5/alice-v4.vcf</D:href>
</card:addressbook-multiget>`

		req := httptest.NewRequest("REPORT", "/dav/addressbooks/5/", strings.NewReader(body))
		req.Header.Set("Depth", "0")
		req = req.WithContext(auth.WithUser(req.Context(), user))
		rr := httptest.NewRecorder()

		h.Report(rr, req)

		if rr.Code != http.StatusMultiStatus {
			t.Fatalf("RFC 6352 Section 5.1.1: multiget should report per-resource conversion errors in a multistatus body, got %d", rr.Code)
		}
		respBody := rr.Body.String()
		if !strings.Contains(respBody, "406 Not Acceptable") {
			t.Fatalf("RFC 6352 Section 5.1.1: the targeted resource should report a 406 conversion failure, got %s", respBody)
		}
		if !strings.Contains(respBody, "supported-address-data-conversion") {
			t.Fatalf("RFC 6352 Section 5.1.1: multiget conversion failures must identify the supported-address-data-conversion precondition, got %s", respBody)
		}
	})

	t.Run("Section8_7_TopLevelVersionConversionFailureUsesSupportedAddressDataConversion", func(t *testing.T) {
		h := &Handler{store: &store.Store{
			AddressBooks: bookRepo,
			Contacts: &fakeContactRepo{contacts: map[string]*store.Contact{
				"5:alice-v4": {AddressBookID: 5, UID: "alice-v4", ResourceName: "alice-v4", RawVCard: buildVCard("4.0", "UID:alice-v4", "FN:Alice Example"), ETag: "etag-v4"},
			}},
		}}

		body := `<?xml version="1.0" encoding="utf-8"?>
<card:addressbook-multiget xmlns:card="urn:ietf:params:xml:ns:carddav" xmlns:D="DAV:">
  <card:address-data content-type="text/vcard" version="3.0"/>
  <D:href>/dav/addressbooks/5/alice-v4.vcf</D:href>
</card:addressbook-multiget>`

		req := httptest.NewRequest("REPORT", "/dav/addressbooks/5/", strings.NewReader(body))
		req.Header.Set("Depth", "0")
		req = req.WithContext(auth.WithUser(req.Context(), user))
		rr := httptest.NewRecorder()

		h.Report(rr, req)

		if rr.Code != http.StatusMultiStatus {
			t.Fatalf("RFC 6352 Section 5.1.1: top-level address-data conversion failures must be reported in a multistatus body, got %d", rr.Code)
		}
		respBody := rr.Body.String()
		if !strings.Contains(respBody, "406 Not Acceptable") {
			t.Fatalf("RFC 6352 Section 5.1.1: top-level address-data conversion failures must return 406, got %s", respBody)
		}
		if !strings.Contains(respBody, "supported-address-data-conversion") {
			t.Fatalf("RFC 6352 Section 5.1.1: top-level address-data conversion failures must identify the supported-address-data-conversion precondition, got %s", respBody)
		}
	})

	t.Run("Section8_7_RejectsUnsupportedRequestedAddressDataType", func(t *testing.T) {
		h := &Handler{store: &store.Store{AddressBooks: bookRepo, Contacts: &fakeContactRepo{contacts: baseContacts}}}

		body := `<?xml version="1.0" encoding="utf-8"?>
<card:addressbook-multiget xmlns:card="urn:ietf:params:xml:ns:carddav" xmlns:D="DAV:">
  <D:prop>
    <card:address-data content-type="application/json" version="1.0"/>
  </D:prop>
  <D:href>/dav/addressbooks/5/alice.vcf</D:href>
</card:addressbook-multiget>`

		req := httptest.NewRequest("REPORT", "/dav/addressbooks/5/", strings.NewReader(body))
		req.Header.Set("Depth", "0")
		req = req.WithContext(auth.WithUser(req.Context(), user))
		rr := httptest.NewRecorder()

		h.Report(rr, req)

		assertCardDAVPreconditionStatus(t, rr, http.StatusUnsupportedMediaType, "supported-address-data")
	})
}

func TestRFC6352_UnicodeCollation(t *testing.T) {
	user := &store.User{ID: 1}
	now := store.Now()
	bookRepo := &fakeAddressBookRepo{
		books: map[int64]*store.AddressBook{
			5: {ID: 5, UserID: 1, Name: "Contacts", UpdatedAt: now, CTag: 1},
		},
	}

	joseVCard := buildVCard("3.0", "UID:jose", "FN:José García", "EMAIL:jose@example.com")
	strasseVCard := buildVCard("3.0", "UID:strasse", "FN:Straße User", "EMAIL:strasse@example.com")
	contacts := map[string]*store.Contact{
		"5:jose":    {AddressBookID: 5, UID: "jose", ResourceName: "jose", RawVCard: joseVCard, ETag: "etag-jose", LastModified: now},
		"5:strasse": {AddressBookID: 5, UID: "strasse", ResourceName: "strasse", RawVCard: strasseVCard, ETag: "etag-strasse", LastModified: now},
	}
	h := &Handler{store: &store.Store{AddressBooks: bookRepo, Contacts: &fakeContactRepo{contacts: contacts}}}

	t.Run("UnicodeCasemapMatchesAccentedCharacters", func(t *testing.T) {
		body := `<?xml version="1.0" encoding="utf-8"?>
<card:addressbook-query xmlns:card="urn:ietf:params:xml:ns:carddav" xmlns:D="DAV:">
  <D:prop><D:getetag/></D:prop>
  <card:filter>
    <card:prop-filter name="FN">
      <card:text-match collation="i;unicode-casemap" match-type="contains">josé</card:text-match>
    </card:prop-filter>
  </card:filter>
</card:addressbook-query>`

		req := httptest.NewRequest("REPORT", "/dav/addressbooks/5/", strings.NewReader(body))
		req.Header.Set("Depth", "1")
		req = req.WithContext(auth.WithUser(req.Context(), user))
		rr := httptest.NewRecorder()
		h.Report(rr, req)

		if rr.Code != http.StatusMultiStatus {
			t.Fatalf("expected 207, got %d: %s", rr.Code, rr.Body.String())
		}
		if !strings.Contains(rr.Body.String(), "jose.vcf") {
			t.Errorf("i;unicode-casemap should match 'José' when searching for 'josé', got %s", rr.Body.String())
		}
	})

	t.Run("UnicodeCasemapFoldsGermanEszett", func(t *testing.T) {
		// Unicode case folding: ß folds to ss
		body := `<?xml version="1.0" encoding="utf-8"?>
<card:addressbook-query xmlns:card="urn:ietf:params:xml:ns:carddav" xmlns:D="DAV:">
  <D:prop><D:getetag/></D:prop>
  <card:filter>
    <card:prop-filter name="FN">
      <card:text-match collation="i;unicode-casemap" match-type="contains">strasse</card:text-match>
    </card:prop-filter>
  </card:filter>
</card:addressbook-query>`

		req := httptest.NewRequest("REPORT", "/dav/addressbooks/5/", strings.NewReader(body))
		req.Header.Set("Depth", "1")
		req = req.WithContext(auth.WithUser(req.Context(), user))
		rr := httptest.NewRecorder()
		h.Report(rr, req)

		if rr.Code != http.StatusMultiStatus {
			t.Fatalf("expected 207, got %d: %s", rr.Code, rr.Body.String())
		}
		if !strings.Contains(rr.Body.String(), "strasse.vcf") {
			t.Errorf("i;unicode-casemap should fold ß to ss so 'Straße' matches 'strasse', got %s", rr.Body.String())
		}
	})

	t.Run("AsciiCasemapDoesNotFoldUnicode", func(t *testing.T) {
		// i;ascii-casemap only does ASCII uppercasing; 'é' != 'É' via ToUpper for non-ASCII
		// but more importantly, ß does NOT fold to SS under ASCII-only rules
		body := `<?xml version="1.0" encoding="utf-8"?>
<card:addressbook-query xmlns:card="urn:ietf:params:xml:ns:carddav" xmlns:D="DAV:">
  <D:prop><D:getetag/></D:prop>
  <card:filter>
    <card:prop-filter name="FN">
      <card:text-match collation="i;ascii-casemap" match-type="contains">strasse</card:text-match>
    </card:prop-filter>
  </card:filter>
</card:addressbook-query>`

		req := httptest.NewRequest("REPORT", "/dav/addressbooks/5/", strings.NewReader(body))
		req.Header.Set("Depth", "1")
		req = req.WithContext(auth.WithUser(req.Context(), user))
		rr := httptest.NewRecorder()
		h.Report(rr, req)

		if rr.Code != http.StatusMultiStatus {
			t.Fatalf("expected 207, got %d: %s", rr.Code, rr.Body.String())
		}
		// Under ascii-casemap, "Straße" uppercases to "STRAßE", and "strasse" uppercases to "STRASSE"
		// These don't match because ß is not SS under ASCII rules
		if strings.Contains(rr.Body.String(), "strasse.vcf") {
			t.Errorf("i;ascii-casemap must NOT fold ß to ss; 'Straße' should not match 'strasse' under ASCII collation, got %s", rr.Body.String())
		}
	})
}

func TestRFC6352_Depth0AddressbookQuery(t *testing.T) {
	user := &store.User{ID: 1}
	now := store.Now()
	bookRepo := &fakeAddressBookRepo{
		books: map[int64]*store.AddressBook{
			5: {ID: 5, UserID: 1, Name: "Contacts", UpdatedAt: now, CTag: 1},
		},
	}
	aliceVCard := buildVCard("3.0", "UID:alice", "FN:Alice", "EMAIL:alice@example.com")
	contacts := map[string]*store.Contact{
		"5:alice": {AddressBookID: 5, UID: "alice", ResourceName: "alice", RawVCard: aliceVCard, ETag: "etag-a", LastModified: now},
	}
	h := &Handler{store: &store.Store{AddressBooks: bookRepo, Contacts: &fakeContactRepo{contacts: contacts}}}

	t.Run("Section8_6_Depth0OnCollectionReturnsNoResources", func(t *testing.T) {
		body := `<?xml version="1.0" encoding="utf-8"?>
<card:addressbook-query xmlns:card="urn:ietf:params:xml:ns:carddav" xmlns:D="DAV:">
  <D:prop><D:getetag/></D:prop>
  <card:filter>
    <card:prop-filter name="FN"/>
  </card:filter>
</card:addressbook-query>`

		req := httptest.NewRequest("REPORT", "/dav/addressbooks/5/", strings.NewReader(body))
		req.Header.Set("Depth", "0")
		req = req.WithContext(auth.WithUser(req.Context(), user))
		rr := httptest.NewRecorder()
		h.Report(rr, req)

		// Depth:0 on a collection means only the collection itself, not its children
		// For addressbook-query, this should either return an empty multistatus or
		// only collection-level info, but not individual contact resources
		if rr.Code != http.StatusMultiStatus {
			t.Fatalf("expected 207, got %d: %s", rr.Code, rr.Body.String())
		}
		if strings.Contains(rr.Body.String(), "alice.vcf") {
			t.Errorf("RFC 6352 Section 8.6: Depth:0 on a collection should not return child address object resources, got %s", rr.Body.String())
		}
	})
}

func TestRFC6352_UIDConflictIncludesHref(t *testing.T) {
	user := &store.User{ID: 1}
	now := store.Now()
	bookRepo := &fakeAddressBookRepo{
		books: map[int64]*store.AddressBook{
			5: {ID: 5, UserID: 1, Name: "Contacts", UpdatedAt: now, CTag: 1},
		},
	}
	existingVCard := buildVCard("3.0", "UID:duplicate-uid", "FN:Existing Contact")
	contacts := map[string]*store.Contact{
		"5:duplicate-uid": {AddressBookID: 5, UID: "duplicate-uid", ResourceName: "existing", RawVCard: existingVCard, ETag: "etag-existing", LastModified: now},
	}
	h := &Handler{
		store: &store.Store{
			AddressBooks:     bookRepo,
			Contacts:         &fakeContactRepo{contacts: contacts},
			Locks:            &fakeLockRepo{},
			DeletedResources: &fakeDeletedResourceRepo{},
		},
	}

	t.Run("Section6_3_2_1_NoUIDConflictIncludesHref", func(t *testing.T) {
		// Try to PUT a new contact at a different path but with the same UID
		newVCard := buildVCard("3.0", "UID:duplicate-uid", "FN:New Contact Same UID")
		req := newAddressBookPutRequest("/dav/addressbooks/5/different-resource.vcf", strings.NewReader(newVCard))
		req = req.WithContext(auth.WithUser(req.Context(), user))
		rr := httptest.NewRecorder()

		h.Put(rr, req)

		if rr.Code != http.StatusConflict {
			t.Fatalf("RFC 6352 Section 6.3.2.1: duplicate UID should return 409, got %d: %s", rr.Code, rr.Body.String())
		}
		respBody := rr.Body.String()
		if !strings.Contains(respBody, "no-uid-conflict") {
			t.Errorf("RFC 6352 Section 6.3.2.1: error must contain no-uid-conflict element, got %s", respBody)
		}
		if !strings.Contains(respBody, "<D:href>") {
			t.Errorf("RFC 6352 Section 6.3.2.1: no-uid-conflict SHOULD include the href of the conflicting resource, got %s", respBody)
		}
		if !strings.Contains(respBody, "/dav/addressbooks/5/existing.vcf") {
			t.Errorf("RFC 6352 Section 6.3.2.1: href should point to the existing conflicting resource, got %s", respBody)
		}
	})
}

func TestRFC6352_ExpandPropertyAtDAVRoot(t *testing.T) {
	user := &store.User{ID: 1, PrimaryEmail: "root@example.com"}
	now := store.Now()
	bookRepo := &fakeAddressBookRepo{
		books: map[int64]*store.AddressBook{
			5: {ID: 5, UserID: 1, Name: "Contacts", UpdatedAt: now, CTag: 1},
		},
	}
	h := &Handler{store: &store.Store{AddressBooks: bookRepo, Contacts: &fakeContactRepo{}}}

	t.Run("Section8_1_ExpandPropertySupportedOnDAVRoot", func(t *testing.T) {
		body := `<?xml version="1.0" encoding="utf-8"?>
<D:expand-property xmlns:D="DAV:">
  <D:property name="current-user-principal" namespace="DAV:">
    <D:property name="displayname" namespace="DAV:"/>
  </D:property>
</D:expand-property>`

		req := httptest.NewRequest("REPORT", "/dav/", strings.NewReader(body))
		req.Header.Set("Depth", "0")
		req = req.WithContext(auth.WithUser(req.Context(), user))
		rr := httptest.NewRecorder()
		h.Report(rr, req)

		if rr.Code != http.StatusMultiStatus {
			t.Fatalf("RFC 6352 Section 8.1: expand-property should be supported on the DAV root, got %d: %s", rr.Code, rr.Body.String())
		}
		if strings.Contains(rr.Body.String(), "/dav/principals/1//") {
			t.Fatalf("RFC 6352 Section 8.1: expanded principal hrefs on the DAV root must be canonical, got %s", rr.Body.String())
		}
		if !strings.Contains(rr.Body.String(), "<d:href>/dav/</d:href>") {
			t.Fatalf("RFC 6352 Section 8.1: expand-property must respond on the request resource href, got %s", rr.Body.String())
		}
		if !strings.Contains(rr.Body.String(), "<d:current-user-principal><d:response><d:href>/dav/principals/1/</d:href>") {
			t.Fatalf("RFC 6352 Section 8.1: current-user-principal must be expanded inline on the request resource, got %s", rr.Body.String())
		}
		if !strings.Contains(rr.Body.String(), "/dav/principals/1/") {
			t.Fatalf("RFC 6352 Section 8.1: expanded principal href should reference the current user principal, got %s", rr.Body.String())
		}
		if !strings.Contains(rr.Body.String(), "<d:displayname>"+user.PrimaryEmail+"</d:displayname>") {
			t.Fatalf("RFC 6352 Section 8.1: expanded principal properties should be returned inside the inline response, got %s", rr.Body.String())
		}
	})
}

func assertCardDAVErrorBody(t *testing.T, body, expected string) {
	t.Helper()
	if !strings.Contains(body, expected) {
		t.Errorf("Expected CardDAV error body to include %q, got: %s", expected, body)
	}
}

func assertCardDAVPreconditionStatus(t *testing.T, rr *httptest.ResponseRecorder, expectedStatus int, expectedPrecondition string) {
	t.Helper()
	if rr.Code != expectedStatus {
		t.Fatalf("expected status %d with %s precondition, got %d: %s", expectedStatus, expectedPrecondition, rr.Code, rr.Body.String())
	}
	assertCardDAVErrorBody(t, rr.Body.String(), expectedPrecondition)
}

// ---------------------------------------------------------------------------
// Lock conflict, wrong-user UNLOCK, and lock refresh tests
// ---------------------------------------------------------------------------

func TestRFC6352_LockConflictBetweenUsers(t *testing.T) {
	user1 := &store.User{ID: 1, PrimaryEmail: "alice@example.com"}
	user2 := &store.User{ID: 2, PrimaryEmail: "bob@example.com"}
	lockRepo := &fakeLockRepo{locks: map[string]*store.Lock{}}
	bookRepo := &fakeAddressBookRepo{
		books: map[int64]*store.AddressBook{
			5: {ID: 5, UserID: user1.ID, Name: "Contacts"},
		},
	}
	aclRepo := &fakeACLRepo{
		entries: []store.ACLEntry{
			{ResourcePath: "/dav/addressbooks/5", PrincipalHref: "/dav/principals/2/", IsGrant: true, Privilege: "bind"},
		},
	}
	h := &Handler{store: &store.Store{AddressBooks: bookRepo, Locks: lockRepo, ACLEntries: aclRepo}}

	t.Run("ExclusiveLockByUser1BlocksUser2Lock", func(t *testing.T) {
		// User 1 takes an exclusive lock
		lockBody := `<?xml version="1.0" encoding="utf-8"?><D:lockinfo xmlns:D="DAV:"><D:lockscope><D:exclusive/></D:lockscope><D:locktype><D:write/></D:locktype></D:lockinfo>`
		req := httptest.NewRequest("LOCK", "/dav/addressbooks/5/contact.vcf", strings.NewReader(lockBody))
		req.Header.Set("Timeout", "Second-3600")
		req = req.WithContext(auth.WithUser(req.Context(), user1))
		rr := httptest.NewRecorder()
		h.Lock(rr, req)

		if rr.Code != http.StatusOK {
			t.Fatalf("expected user1 LOCK to succeed with 200, got %d: %s", rr.Code, rr.Body.String())
		}

		// User 2 attempts an exclusive lock on the same resource
		req2 := httptest.NewRequest("LOCK", "/dav/addressbooks/5/contact.vcf", strings.NewReader(lockBody))
		req2.Header.Set("Timeout", "Second-3600")
		req2 = req2.WithContext(auth.WithUser(req2.Context(), user2))
		rr2 := httptest.NewRecorder()
		h.Lock(rr2, req2)

		if rr2.Code != http.StatusLocked {
			t.Fatalf("RFC 4918: exclusive lock by user1 MUST block user2 from locking the same resource, got %d", rr2.Code)
		}
	})

	t.Run("SharedLockAllowsAnotherSharedLock", func(t *testing.T) {
		sharedLockRepo := &fakeLockRepo{locks: map[string]*store.Lock{}}
		sh := &Handler{store: &store.Store{AddressBooks: bookRepo, Locks: sharedLockRepo, ACLEntries: aclRepo}}

		sharedBody := `<?xml version="1.0" encoding="utf-8"?><D:lockinfo xmlns:D="DAV:"><D:lockscope><D:shared/></D:lockscope><D:locktype><D:write/></D:locktype></D:lockinfo>`
		req := httptest.NewRequest("LOCK", "/dav/addressbooks/5/shared.vcf", strings.NewReader(sharedBody))
		req.Header.Set("Timeout", "Second-3600")
		req = req.WithContext(auth.WithUser(req.Context(), user1))
		rr := httptest.NewRecorder()
		sh.Lock(rr, req)

		if rr.Code != http.StatusOK {
			t.Fatalf("expected shared lock to succeed, got %d: %s", rr.Code, rr.Body.String())
		}

		// User 2 takes another shared lock — should succeed
		req2 := httptest.NewRequest("LOCK", "/dav/addressbooks/5/shared.vcf", strings.NewReader(sharedBody))
		req2.Header.Set("Timeout", "Second-3600")
		req2 = req2.WithContext(auth.WithUser(req2.Context(), user2))
		rr2 := httptest.NewRecorder()
		sh.Lock(rr2, req2)

		if rr2.Code != http.StatusOK {
			t.Fatalf("RFC 4918: shared locks MUST allow additional shared locks, got %d: %s", rr2.Code, rr2.Body.String())
		}
	})

	t.Run("SharedLockBlocksExclusiveLock", func(t *testing.T) {
		sharedLockRepo := &fakeLockRepo{locks: map[string]*store.Lock{}}
		sh := &Handler{store: &store.Store{AddressBooks: bookRepo, Locks: sharedLockRepo, ACLEntries: aclRepo}}

		sharedBody := `<?xml version="1.0" encoding="utf-8"?><D:lockinfo xmlns:D="DAV:"><D:lockscope><D:shared/></D:lockscope><D:locktype><D:write/></D:locktype></D:lockinfo>`
		req := httptest.NewRequest("LOCK", "/dav/addressbooks/5/mixed.vcf", strings.NewReader(sharedBody))
		req.Header.Set("Timeout", "Second-3600")
		req = req.WithContext(auth.WithUser(req.Context(), user1))
		rr := httptest.NewRecorder()
		sh.Lock(rr, req)

		if rr.Code != http.StatusOK {
			t.Fatalf("expected shared lock to succeed, got %d", rr.Code)
		}

		// User 2 requests an exclusive lock — should be blocked
		exclusiveBody := `<?xml version="1.0" encoding="utf-8"?><D:lockinfo xmlns:D="DAV:"><D:lockscope><D:exclusive/></D:lockscope><D:locktype><D:write/></D:locktype></D:lockinfo>`
		req2 := httptest.NewRequest("LOCK", "/dav/addressbooks/5/mixed.vcf", strings.NewReader(exclusiveBody))
		req2.Header.Set("Timeout", "Second-3600")
		req2 = req2.WithContext(auth.WithUser(req2.Context(), user2))
		rr2 := httptest.NewRecorder()
		sh.Lock(rr2, req2)

		if rr2.Code != http.StatusLocked {
			t.Fatalf("RFC 4918: shared lock MUST block exclusive lock requests, got %d", rr2.Code)
		}
	})

	t.Run("CollectionDepthInfinityLockConflictsWithLockedDescendant", func(t *testing.T) {
		descendantLockRepo := &fakeLockRepo{locks: map[string]*store.Lock{
			"opaquelocktoken:child": {
				Token:        "opaquelocktoken:child",
				ResourcePath: "/dav/addressbooks/5/alice.vcf",
				UserID:       user1.ID,
				LockScope:    "exclusive",
				LockType:     "write",
				Depth:        "0",
				ExpiresAt:    time.Now().Add(time.Hour),
			},
		}}
		bookRepo := &fakeAddressBookRepo{
			books: map[int64]*store.AddressBook{
				5: {ID: 5, UserID: user1.ID, Name: "Contacts"},
			},
		}
		h := &Handler{store: &store.Store{AddressBooks: bookRepo, Locks: descendantLockRepo}}

		lockBody := `<?xml version="1.0" encoding="utf-8"?><D:lockinfo xmlns:D="DAV:"><D:lockscope><D:exclusive/></D:lockscope><D:locktype><D:write/></D:locktype></D:lockinfo>`
		req := httptest.NewRequest("LOCK", "/dav/addressbooks/5/", strings.NewReader(lockBody))
		req.Header.Set("Depth", "infinity")
		req = req.WithContext(auth.WithUser(req.Context(), user1))
		rr := httptest.NewRecorder()
		h.Lock(rr, req)

		if rr.Code != http.StatusLocked {
			t.Fatalf("RFC 4918: depth-infinity collection locks must conflict with existing descendant locks, got %d: %s", rr.Code, rr.Body.String())
		}
	})
}

func TestRFC4918_LockResponseUsesDAVXML(t *testing.T) {
	user := &store.User{ID: 1, PrimaryEmail: "alice@example.com"}
	lockRepo := &fakeLockRepo{locks: map[string]*store.Lock{}}
	bookRepo := &fakeAddressBookRepo{
		books: map[int64]*store.AddressBook{
			5: {ID: 5, UserID: user.ID, Name: "Contacts"},
		},
	}
	h := &Handler{store: &store.Store{AddressBooks: bookRepo, Locks: lockRepo}}

	lockBody := `<?xml version="1.0" encoding="utf-8"?><D:lockinfo xmlns:D="DAV:"><D:lockscope><D:exclusive/></D:lockscope><D:locktype><D:write/></D:locktype></D:lockinfo>`
	req := httptest.NewRequest("LOCK", "/dav/addressbooks/5/alice.vcf", strings.NewReader(lockBody))
	req = req.WithContext(auth.WithUser(req.Context(), user))
	rr := httptest.NewRecorder()
	h.Lock(rr, req)

	if rr.Code != http.StatusOK {
		t.Fatalf("expected LOCK to succeed, got %d: %s", rr.Code, rr.Body.String())
	}

	var resp struct {
		XMLName       xml.Name `xml:"DAV: prop"`
		LockDiscovery struct {
			ActiveLocks []struct {
				Depth string `xml:"DAV: depth"`
			} `xml:"DAV: activelock"`
		} `xml:"DAV: lockdiscovery"`
	}
	if err := xml.Unmarshal(rr.Body.Bytes(), &resp); err != nil {
		t.Fatalf("RFC 4918 LOCK response body must be valid DAV XML, got parse error: %v\nbody=%s", err, rr.Body.String())
	}
	if resp.XMLName.Space != "DAV:" || resp.XMLName.Local != "prop" {
		t.Fatalf("expected DAV:prop root element, got %+v", resp.XMLName)
	}
	if len(resp.LockDiscovery.ActiveLocks) != 1 {
		t.Fatalf("expected one active lock in DAV response, got %+v", resp.LockDiscovery.ActiveLocks)
	}
}

func TestRFC6352_UnlockByWrongUser(t *testing.T) {
	user1 := &store.User{ID: 1, PrimaryEmail: "alice@example.com"}
	user2 := &store.User{ID: 2, PrimaryEmail: "bob@example.com"}
	lockRepo := &fakeLockRepo{locks: map[string]*store.Lock{}}
	bookRepo := &fakeAddressBookRepo{
		books: map[int64]*store.AddressBook{
			5: {ID: 5, UserID: user1.ID, Name: "Contacts"},
		},
	}
	h := &Handler{store: &store.Store{AddressBooks: bookRepo, Locks: lockRepo}}

	// User 1 creates a lock
	lockBody := `<?xml version="1.0" encoding="utf-8"?><D:lockinfo xmlns:D="DAV:"><D:lockscope><D:exclusive/></D:lockscope><D:locktype><D:write/></D:locktype></D:lockinfo>`
	req := httptest.NewRequest("LOCK", "/dav/addressbooks/5/alice.vcf", strings.NewReader(lockBody))
	req.Header.Set("Timeout", "Second-3600")
	req = req.WithContext(auth.WithUser(req.Context(), user1))
	rr := httptest.NewRecorder()
	h.Lock(rr, req)

	if rr.Code != http.StatusOK {
		t.Fatalf("expected LOCK to succeed, got %d: %s", rr.Code, rr.Body.String())
	}

	lockToken := rr.Header().Get("Lock-Token")
	if lockToken == "" {
		t.Fatal("expected Lock-Token header in response")
	}

	// User 2 attempts to UNLOCK the resource — should be forbidden
	unlockReq := httptest.NewRequest("UNLOCK", "/dav/addressbooks/5/alice.vcf", nil)
	unlockReq.Header.Set("Lock-Token", lockToken)
	unlockReq = unlockReq.WithContext(auth.WithUser(unlockReq.Context(), user2))
	unlockRR := httptest.NewRecorder()
	h.Unlock(unlockRR, unlockReq)

	if unlockRR.Code != http.StatusForbidden {
		t.Fatalf("RFC 4918: UNLOCK by a different user MUST be forbidden, got %d: %s", unlockRR.Code, unlockRR.Body.String())
	}

	// User 1 can successfully UNLOCK
	unlockReq2 := httptest.NewRequest("UNLOCK", "/dav/addressbooks/5/alice.vcf", nil)
	unlockReq2.Header.Set("Lock-Token", lockToken)
	unlockReq2 = unlockReq2.WithContext(auth.WithUser(unlockReq2.Context(), user1))
	unlockRR2 := httptest.NewRecorder()
	h.Unlock(unlockRR2, unlockReq2)

	if unlockRR2.Code != http.StatusNoContent {
		t.Fatalf("expected owner UNLOCK to succeed with 204, got %d: %s", unlockRR2.Code, unlockRR2.Body.String())
	}
}

func TestRFC6352_LockRefresh(t *testing.T) {
	user := &store.User{ID: 1, PrimaryEmail: "alice@example.com"}
	lockRepo := &fakeLockRepo{locks: map[string]*store.Lock{}}
	bookRepo := &fakeAddressBookRepo{
		books: map[int64]*store.AddressBook{
			5: {ID: 5, UserID: user.ID, Name: "Contacts"},
		},
	}
	h := &Handler{store: &store.Store{AddressBooks: bookRepo, Locks: lockRepo}}

	// Create a lock with a short timeout
	lockBody := `<?xml version="1.0" encoding="utf-8"?><D:lockinfo xmlns:D="DAV:"><D:lockscope><D:exclusive/></D:lockscope><D:locktype><D:write/></D:locktype></D:lockinfo>`
	req := httptest.NewRequest("LOCK", "/dav/addressbooks/5/refresh.vcf", strings.NewReader(lockBody))
	req.Header.Set("Timeout", "Second-600")
	req = req.WithContext(auth.WithUser(req.Context(), user))
	rr := httptest.NewRecorder()
	h.Lock(rr, req)

	if rr.Code != http.StatusOK {
		t.Fatalf("expected initial LOCK to succeed, got %d: %s", rr.Code, rr.Body.String())
	}

	lockToken := rr.Header().Get("Lock-Token")
	// Strip angle brackets
	token := strings.TrimPrefix(strings.TrimSuffix(lockToken, ">"), "<")

	t.Run("RefreshExtensdsTimeout", func(t *testing.T) {
		// Send a LOCK request with If header to refresh
		refreshReq := httptest.NewRequest("LOCK", "/dav/addressbooks/5/refresh.vcf", nil)
		refreshReq.Header.Set("If", "(<"+token+">)")
		refreshReq.Header.Set("Timeout", "Second-3600")
		refreshReq = refreshReq.WithContext(auth.WithUser(refreshReq.Context(), user))
		refreshRR := httptest.NewRecorder()
		h.Lock(refreshRR, refreshReq)

		if refreshRR.Code != http.StatusOK {
			t.Fatalf("RFC 4918: lock refresh via If header MUST succeed, got %d: %s", refreshRR.Code, refreshRR.Body.String())
		}

		// Verify the lock still exists and timeout was updated
		refreshed, _ := lockRepo.GetByToken(context.Background(), token)
		if refreshed == nil {
			t.Fatal("expected lock to still exist after refresh")
		}
		if refreshed.TimeoutSeconds != 3600 {
			t.Errorf("expected refreshed timeout to be 3600, got %d", refreshed.TimeoutSeconds)
		}
	})

	t.Run("RefreshOnDifferentPathFails", func(t *testing.T) {
		refreshReq := httptest.NewRequest("LOCK", "/dav/addressbooks/5/other.vcf", nil)
		refreshReq.Header.Set("If", "(<"+token+">)")
		refreshReq.Header.Set("Timeout", "Second-7200")
		refreshReq = refreshReq.WithContext(auth.WithUser(refreshReq.Context(), user))
		refreshRR := httptest.NewRecorder()
		h.Lock(refreshRR, refreshReq)

		if refreshRR.Code != http.StatusPreconditionFailed {
			t.Fatalf("RFC 4918: lock refresh on a different resource path MUST fail, got %d: %s", refreshRR.Code, refreshRR.Body.String())
		}

		refreshed, _ := lockRepo.GetByToken(context.Background(), token)
		if refreshed == nil {
			t.Fatal("expected original lock to remain after failed refresh")
		}
		if refreshed.TimeoutSeconds != 3600 {
			t.Fatalf("failed refresh should not mutate the stored timeout, got %d", refreshed.TimeoutSeconds)
		}
	})

	t.Run("RefreshByWrongUserIsForbidden", func(t *testing.T) {
		user2 := &store.User{ID: 2, PrimaryEmail: "bob@example.com"}
		refreshReq := httptest.NewRequest("LOCK", "/dav/addressbooks/5/refresh.vcf", nil)
		refreshReq.Header.Set("If", "(<"+token+">)")
		refreshReq.Header.Set("Timeout", "Second-7200")
		refreshReq = refreshReq.WithContext(auth.WithUser(refreshReq.Context(), user2))
		refreshRR := httptest.NewRecorder()
		h.Lock(refreshRR, refreshReq)

		if refreshRR.Code != http.StatusForbidden {
			t.Fatalf("RFC 4918: lock refresh by non-owner MUST be forbidden, got %d: %s", refreshRR.Code, refreshRR.Body.String())
		}
	})

	t.Run("RefreshWithInvalidTokenFails", func(t *testing.T) {
		refreshReq := httptest.NewRequest("LOCK", "/dav/addressbooks/5/refresh.vcf", nil)
		refreshReq.Header.Set("If", "(<opaquelocktoken:nonexistent>)")
		refreshReq.Header.Set("Timeout", "Second-3600")
		refreshReq = refreshReq.WithContext(auth.WithUser(refreshReq.Context(), user))
		refreshRR := httptest.NewRecorder()
		h.Lock(refreshRR, refreshReq)

		if refreshRR.Code != http.StatusPreconditionFailed {
			t.Fatalf("RFC 4918: refresh with invalid token MUST return 412, got %d", refreshRR.Code)
		}
	})
}

func TestRFC6352_Depth0CollectionLockBlocksMemberMutation(t *testing.T) {
	user := &store.User{ID: 1, PrimaryEmail: "alice@example.com"}
	now := store.Now()
	bookRepo := &fakeAddressBookRepo{
		books: map[int64]*store.AddressBook{
			5: {ID: 5, UserID: user.ID, Name: "Contacts", UpdatedAt: now},
		},
	}
	contactRepo := &fakeContactRepo{
		contacts: map[string]*store.Contact{
			"5:alice": {AddressBookID: 5, UID: "alice", ResourceName: "alice", RawVCard: buildVCard("3.0", "UID:alice", "FN:Alice"), ETag: "etag-alice", LastModified: now},
		},
	}
	lockRepo := &fakeLockRepo{
		locks: map[string]*store.Lock{
			"tok-collection": {
				Token:        "tok-collection",
				ResourcePath: "/dav/addressbooks/5",
				UserID:       user.ID,
				LockScope:    "exclusive",
				LockType:     "write",
				Depth:        "0",
				ExpiresAt:    time.Now().Add(time.Hour),
			},
		},
	}
	h := &Handler{store: &store.Store{AddressBooks: bookRepo, Contacts: contactRepo, Locks: lockRepo}}

	t.Run("PutOfNewMemberReturns423", func(t *testing.T) {
		req := newAddressBookPutRequest("/dav/addressbooks/5/bob.vcf", strings.NewReader(buildVCard("3.0", "UID:bob", "FN:Bob")))
		req = req.WithContext(auth.WithUser(req.Context(), user))
		rr := httptest.NewRecorder()
		h.Put(rr, req)

		if rr.Code != http.StatusLocked {
			t.Fatalf("RFC 4918: creating a member inside a depth-0 locked collection MUST return 423 without the lock token, got %d: %s", rr.Code, rr.Body.String())
		}
	})

	t.Run("DeleteOfExistingMemberReturns423", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodDelete, "/dav/addressbooks/5/alice.vcf", nil)
		req = req.WithContext(auth.WithUser(req.Context(), user))
		rr := httptest.NewRecorder()
		h.Delete(rr, req)

		if rr.Code != http.StatusLocked {
			t.Fatalf("RFC 4918: deleting a member inside a depth-0 locked collection MUST return 423 without the lock token, got %d: %s", rr.Code, rr.Body.String())
		}
	})
}

func TestRFC6352_InvalidLockDepthRejected(t *testing.T) {
	user := &store.User{ID: 1, PrimaryEmail: "alice@example.com"}
	bookRepo := &fakeAddressBookRepo{
		books: map[int64]*store.AddressBook{
			5: {ID: 5, UserID: user.ID, Name: "Contacts"},
		},
	}
	lockRepo := &fakeLockRepo{locks: map[string]*store.Lock{}}
	h := &Handler{store: &store.Store{AddressBooks: bookRepo, Locks: lockRepo}}

	lockBody := `<?xml version="1.0" encoding="utf-8"?><D:lockinfo xmlns:D="DAV:"><D:lockscope><D:exclusive/></D:lockscope><D:locktype><D:write/></D:locktype></D:lockinfo>`
	req := httptest.NewRequest("LOCK", "/dav/addressbooks/5/alice.vcf", strings.NewReader(lockBody))
	req.Header.Set("Depth", "1")
	req = req.WithContext(auth.WithUser(req.Context(), user))
	rr := httptest.NewRecorder()
	h.Lock(rr, req)

	if rr.Code != http.StatusBadRequest {
		t.Fatalf("RFC 4918: LOCK must reject invalid Depth values, got %d: %s", rr.Code, rr.Body.String())
	}
	if len(lockRepo.locks) != 0 {
		t.Fatalf("invalid LOCK request must not persist a lock, got %#v", lockRepo.locks)
	}
}

func TestRFC6352_CollectionLockTokensAuthorizeMemberWrites(t *testing.T) {
	user := &store.User{ID: 1, PrimaryEmail: "alice@example.com"}
	now := store.Now()
	bookRepo := &fakeAddressBookRepo{
		books: map[int64]*store.AddressBook{
			5: {ID: 5, UserID: user.ID, Name: "Contacts", UpdatedAt: now},
		},
	}
	contactRepo := &fakeContactRepo{contacts: map[string]*store.Contact{}}
	lockRepo := &fakeLockRepo{
		locks: map[string]*store.Lock{
			"opaquelocktoken:collection": {
				Token:          "opaquelocktoken:collection",
				ResourcePath:   "/dav/addressbooks/5",
				UserID:         user.ID,
				LockScope:      "exclusive",
				LockType:       "write",
				Depth:          "infinity",
				TimeoutSeconds: 3600,
				ExpiresAt:      time.Now().Add(time.Hour),
			},
		},
	}
	h := &Handler{store: &store.Store{AddressBooks: bookRepo, Contacts: contactRepo, Locks: lockRepo}}

	req := newAddressBookPutRequest("/dav/addressbooks/5/alice.vcf", strings.NewReader(buildVCard("3.0", "UID:alice", "FN:Alice Example")))
	req.Header.Set("If", `</dav/addressbooks/5/> (<opaquelocktoken:collection>)`)
	req = req.WithContext(auth.WithUser(req.Context(), user))
	rr := httptest.NewRecorder()

	h.Put(rr, req)

	if rr.Code != http.StatusCreated {
		t.Fatalf("RFC 4918: a depth-infinity collection lock token tagged to the collection URI must authorize member writes, got %d: %s", rr.Code, rr.Body.String())
	}
}

func TestRFC4918_NegatedIfStateDoesNotAuthorizeLockedWrite(t *testing.T) {
	user := &store.User{ID: 1, PrimaryEmail: "alice@example.com"}
	now := store.Now()
	bookRepo := &fakeAddressBookRepo{
		books: map[int64]*store.AddressBook{
			5: {ID: 5, UserID: user.ID, Name: "Contacts", UpdatedAt: now},
		},
	}
	contactRepo := &fakeContactRepo{
		contacts: map[string]*store.Contact{
			"5:alice": {AddressBookID: 5, UID: "alice", ResourceName: "alice", RawVCard: buildVCard("3.0", "UID:alice", "FN:Alice Example"), ETag: "etag-alice", LastModified: now},
		},
	}
	lockRepo := &fakeLockRepo{
		locks: map[string]*store.Lock{
			"opaquelocktoken:member": {
				Token:          "opaquelocktoken:member",
				ResourcePath:   "/dav/addressbooks/5/alice.vcf",
				UserID:         user.ID,
				LockScope:      "exclusive",
				LockType:       "write",
				Depth:          "0",
				TimeoutSeconds: 3600,
				ExpiresAt:      time.Now().Add(time.Hour),
			},
		},
	}
	h := &Handler{store: &store.Store{AddressBooks: bookRepo, Contacts: contactRepo, Locks: lockRepo}}

	req := httptest.NewRequest(http.MethodDelete, "/dav/addressbooks/5/alice.vcf", nil)
	req.Header.Set("If", `(Not <opaquelocktoken:member>)`)
	req = req.WithContext(auth.WithUser(req.Context(), user))
	rr := httptest.NewRecorder()

	h.Delete(rr, req)

	if rr.Code != http.StatusLocked {
		t.Fatalf("RFC 4918: negated If state must not authorize writes against a locked resource, got %d: %s", rr.Code, rr.Body.String())
	}
}

func TestRFC6352_LockAuthorizationAndErrorHandling(t *testing.T) {
	t.Run("UnauthorizedUserCannotLockAnotherUsersAddressObject", func(t *testing.T) {
		user := &store.User{ID: 2, PrimaryEmail: "intruder@example.com"}
		bookRepo := &fakeAddressBookRepo{
			books: map[int64]*store.AddressBook{
				5: {ID: 5, UserID: 1, Name: "Private Contacts"},
			},
		}
		lockRepo := &fakeLockRepo{locks: map[string]*store.Lock{}}
		h := &Handler{store: &store.Store{AddressBooks: bookRepo, Locks: lockRepo}}

		lockBody := `<?xml version="1.0" encoding="utf-8"?><D:lockinfo xmlns:D="DAV:"><D:lockscope><D:exclusive/></D:lockscope><D:locktype><D:write/></D:locktype></D:lockinfo>`
		req := httptest.NewRequest("LOCK", "/dav/addressbooks/5/alice.vcf", strings.NewReader(lockBody))
		req = req.WithContext(auth.WithUser(req.Context(), user))
		rr := httptest.NewRecorder()
		h.Lock(rr, req)

		if rr.Code != http.StatusForbidden {
			t.Fatalf("RFC 4918 and RFC 3744: users must not be able to lock resources they cannot modify, got %d: %s", rr.Code, rr.Body.String())
		}
		if len(lockRepo.locks) != 0 {
			t.Fatalf("unauthorized LOCK should not persist a lock, got %#v", lockRepo.locks)
		}
	})

	t.Run("LockLookupErrorsBlockWrites", func(t *testing.T) {
		user := &store.User{ID: 1, PrimaryEmail: "owner@example.com"}
		bookRepo := &fakeAddressBookRepo{
			books: map[int64]*store.AddressBook{
				5: {ID: 5, UserID: 1, Name: "Contacts"},
			},
		}
		lockRepo := &fakeLockRepo{
			locks:              map[string]*store.Lock{},
			listByResourcesErr: errors.New("lock store unavailable"),
		}
		h := &Handler{store: &store.Store{AddressBooks: bookRepo, Contacts: &fakeContactRepo{}, Locks: lockRepo}}

		req := newAddressBookPutRequest("/dav/addressbooks/5/alice.vcf", strings.NewReader(buildVCard("3.0", "UID:alice", "FN:Alice")))
		req = req.WithContext(auth.WithUser(req.Context(), user))
		rr := httptest.NewRecorder()
		h.Put(rr, req)

		if rr.Code != http.StatusInternalServerError {
			t.Fatalf("RFC 4918: writes must fail closed when lock lookups fail, got %d: %s", rr.Code, rr.Body.String())
		}
	})
}

// ---------------------------------------------------------------------------
// Sync-collection REPORT on address book
// ---------------------------------------------------------------------------

func TestRFC6352_SyncCollectionOnAddressBook(t *testing.T) {
	user := &store.User{ID: 1, PrimaryEmail: "test@example.com"}
	now := store.Now()
	bookRepo := &fakeAddressBookRepo{
		books: map[int64]*store.AddressBook{
			5: {ID: 5, UserID: 1, Name: "Contacts", UpdatedAt: now, CTag: 3},
		},
	}
	contactRepo := &fakeContactRepo{
		contacts: map[string]*store.Contact{
			"5:alice": {AddressBookID: 5, UID: "alice", ResourceName: "alice", RawVCard: buildVCard("3.0", "UID:alice", "FN:Alice"), ETag: "etag-alice", LastModified: now},
		},
	}
	deletedRepo := &fakeDeletedResourceRepo{}
	h := &Handler{store: &store.Store{AddressBooks: bookRepo, Contacts: contactRepo, DeletedResources: deletedRepo}}

	t.Run("Section8_2_InitialSyncReturnsAllContacts", func(t *testing.T) {
		body := `<?xml version="1.0" encoding="utf-8"?>
<D:sync-collection xmlns:D="DAV:" xmlns:A="urn:ietf:params:xml:ns:carddav">
  <D:sync-token/>
  <D:prop>
    <D:getetag/>
    <A:address-data/>
  </D:prop>
</D:sync-collection>`
		req := httptest.NewRequest("REPORT", "/dav/addressbooks/5/", strings.NewReader(body))
		req.Header.Set("Depth", "1")
		req = req.WithContext(auth.WithUser(req.Context(), user))
		rr := httptest.NewRecorder()
		h.Report(rr, req)

		if rr.Code != http.StatusMultiStatus {
			t.Fatalf("RFC 6578: sync-collection on address book MUST return 207, got %d: %s", rr.Code, rr.Body.String())
		}
		responseBody := rr.Body.String()
		if !strings.Contains(responseBody, "alice") {
			t.Errorf("expected sync-collection to include alice contact, got: %s", responseBody)
		}
		if !strings.Contains(responseBody, "sync-token") {
			t.Errorf("expected sync-collection response to include sync-token, got: %s", responseBody)
		}
	})
}

// ---------------------------------------------------------------------------
// Timeout header parsing edge cases
// ---------------------------------------------------------------------------

func TestRFC6352_LockTimeoutParsing(t *testing.T) {
	t.Run("ValidSecondTimeout", func(t *testing.T) {
		if got := parseLockTimeout("Second-300"); got != 300 {
			t.Errorf("expected 300, got %d", got)
		}
	})

	t.Run("InfiniteTimeout", func(t *testing.T) {
		if got := parseLockTimeout("Infinite"); got != maxLockTimeout {
			t.Errorf("expected maxLockTimeout (%d), got %d", maxLockTimeout, got)
		}
	})

	t.Run("EmptyTimeout", func(t *testing.T) {
		if got := parseLockTimeout(""); got != defaultLockTimeout {
			t.Errorf("expected defaultLockTimeout (%d), got %d", defaultLockTimeout, got)
		}
	})

	t.Run("ExceedsMaxCapped", func(t *testing.T) {
		if got := parseLockTimeout("Second-999999999"); got != maxLockTimeout {
			t.Errorf("expected maxLockTimeout (%d) for oversized timeout, got %d", maxLockTimeout, got)
		}
	})

	t.Run("MalformedFallsToDefault", func(t *testing.T) {
		if got := parseLockTimeout("Second-abc"); got != defaultLockTimeout {
			t.Errorf("expected defaultLockTimeout (%d) for malformed, got %d", defaultLockTimeout, got)
		}
	})

	t.Run("NegativeValueFallsToDefault", func(t *testing.T) {
		if got := parseLockTimeout("Second--5"); got != defaultLockTimeout {
			t.Errorf("expected defaultLockTimeout (%d) for negative, got %d", defaultLockTimeout, got)
		}
	})

	t.Run("MultipleValuesFirstWins", func(t *testing.T) {
		if got := parseLockTimeout("Second-120, Second-600"); got != 120 {
			t.Errorf("expected 120 (first valid), got %d", got)
		}
	})

	t.Run("InfiniteBeforeSecond", func(t *testing.T) {
		if got := parseLockTimeout("Infinite, Second-120"); got != maxLockTimeout {
			t.Errorf("expected maxLockTimeout when Infinite appears first, got %d", got)
		}
	})
}

// ---------------------------------------------------------------------------
// 1. Incremental sync-collection (RFC 6578 Section 3)
// ---------------------------------------------------------------------------

func TestRFC6352_IncrementalSyncCollection(t *testing.T) {
	user := &store.User{ID: 1, PrimaryEmail: "test@example.com"}
	baseTime := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)
	laterTime := baseTime.Add(time.Hour)

	bookRepo := &fakeAddressBookRepo{
		books: map[int64]*store.AddressBook{
			5: {ID: 5, UserID: 1, Name: "Contacts", UpdatedAt: laterTime, CTag: 3},
		},
	}
	contactRepo := &fakeContactRepo{
		contacts: map[string]*store.Contact{
			"5:alice": {AddressBookID: 5, UID: "alice", ResourceName: "alice", RawVCard: buildVCard("3.0", "UID:alice", "FN:Alice"), ETag: "etag-alice", LastModified: baseTime},
			"5:bob":   {AddressBookID: 5, UID: "bob", ResourceName: "bob", RawVCard: buildVCard("3.0", "UID:bob", "FN:Bob"), ETag: "etag-bob", LastModified: laterTime},
		},
	}
	deletedRepo := &fakeDeletedResourceRepo{
		deleted: []store.DeletedResource{
			{ResourceType: "contact", CollectionID: 5, UID: "carol", ResourceName: "carol", DeletedAt: laterTime},
		},
	}
	h := &Handler{store: &store.Store{AddressBooks: bookRepo, Contacts: contactRepo, DeletedResources: deletedRepo}}

	t.Run("IncrementalSyncReturnsOnlyChangedAndDeleted", func(t *testing.T) {
		// Build a sync-token that represents a point in time before bob was modified and carol was deleted,
		// but after alice was created.
		syncToken := buildSyncToken("card", 5, baseTime)
		body := fmt.Sprintf(`<?xml version="1.0" encoding="utf-8"?>
<D:sync-collection xmlns:D="DAV:" xmlns:A="urn:ietf:params:xml:ns:carddav">
  <D:sync-token>%s</D:sync-token>
  <D:prop>
    <D:getetag/>
    <A:address-data/>
  </D:prop>
</D:sync-collection>`, syncToken)

		req := httptest.NewRequest("REPORT", "/dav/addressbooks/5/", strings.NewReader(body))
		req.Header.Set("Depth", "1")
		req = req.WithContext(auth.WithUser(req.Context(), user))
		rr := httptest.NewRecorder()
		h.Report(rr, req)

		if rr.Code != http.StatusMultiStatus {
			t.Fatalf("RFC 6578 Section 3: incremental sync-collection MUST return 207, got %d: %s", rr.Code, rr.Body.String())
		}
		respBody := rr.Body.String()

		// Bob was modified after baseTime, so should appear
		if !strings.Contains(respBody, "bob") {
			t.Errorf("RFC 6578 Section 3: incremental sync should include resources modified since the sync-token, missing bob: %s", respBody)
		}

		// Alice was NOT modified after baseTime, so should NOT appear as a resource response
		// (She might appear in the collection response, but not as a separate resource with an etag)
		if strings.Contains(respBody, "alice.vcf") {
			t.Errorf("RFC 6578 Section 3: incremental sync should NOT include unchanged resources, but found alice: %s", respBody)
		}

		// Carol was deleted after baseTime, so should appear as a 404 response
		if !strings.Contains(respBody, "carol") {
			t.Errorf("RFC 6578 Section 3: incremental sync should include deleted resources, missing carol: %s", respBody)
		}
		if !strings.Contains(respBody, "404") {
			t.Errorf("RFC 6578 Section 3: deleted resources must be reported with 404 status, got: %s", respBody)
		}

		// Must include a new sync-token for the next sync
		if !strings.Contains(respBody, "sync-token") {
			t.Errorf("RFC 6578 Section 3: sync-collection response must include a new sync-token, got: %s", respBody)
		}
	})
}

// ---------------------------------------------------------------------------
// 2. DELETE of address book collection (RFC 6352 Section 6.3 / RFC 4918 Section 9.6)
// ---------------------------------------------------------------------------

func TestRFC6352_DeleteAddressBookCollection(t *testing.T) {
	user := &store.User{ID: 1}
	now := store.Now()

	t.Run("DeleteContactResource204", func(t *testing.T) {
		bookRepo := &fakeAddressBookRepo{
			books: map[int64]*store.AddressBook{
				5: {ID: 5, UserID: 1, Name: "Contacts", UpdatedAt: now},
			},
		}
		contactRepo := &fakeContactRepo{
			contacts: map[string]*store.Contact{
				"5:alice": {AddressBookID: 5, UID: "alice", ResourceName: "alice", RawVCard: buildVCard("3.0", "UID:alice", "FN:Alice"), ETag: "etag-alice"},
			},
		}
		h := &Handler{store: &store.Store{AddressBooks: bookRepo, Contacts: contactRepo, Locks: &fakeLockRepo{}}}

		req := httptest.NewRequest(http.MethodDelete, "/dav/addressbooks/5/alice.vcf", nil)
		req = req.WithContext(auth.WithUser(req.Context(), user))
		rr := httptest.NewRecorder()
		h.Delete(rr, req)

		if rr.Code != http.StatusNoContent {
			t.Fatalf("RFC 4918 Section 9.6: DELETE on an address object resource MUST return 204, got %d: %s", rr.Code, rr.Body.String())
		}

		// Verify the contact was actually removed
		c, _ := contactRepo.GetByResourceName(context.Background(), 5, "alice")
		if c != nil {
			t.Error("RFC 4918 Section 9.6: DELETE must actually remove the resource from the store")
		}
	})

	t.Run("DeleteNonExistentReturns404", func(t *testing.T) {
		bookRepo := &fakeAddressBookRepo{
			books: map[int64]*store.AddressBook{
				5: {ID: 5, UserID: 1, Name: "Contacts", UpdatedAt: now},
			},
		}
		h := &Handler{store: &store.Store{AddressBooks: bookRepo, Contacts: &fakeContactRepo{}, Locks: &fakeLockRepo{}}}

		req := httptest.NewRequest(http.MethodDelete, "/dav/addressbooks/5/nonexistent.vcf", nil)
		req = req.WithContext(auth.WithUser(req.Context(), user))
		rr := httptest.NewRecorder()
		h.Delete(rr, req)

		if rr.Code != http.StatusNotFound {
			t.Fatalf("RFC 4918 Section 9.6: DELETE on a non-existent resource MUST return 404, got %d", rr.Code)
		}
	})

	t.Run("DeleteLockedResourceReturns423", func(t *testing.T) {
		bookRepo := &fakeAddressBookRepo{
			books: map[int64]*store.AddressBook{
				5: {ID: 5, UserID: 1, Name: "Contacts", UpdatedAt: now},
			},
		}
		contactRepo := &fakeContactRepo{
			contacts: map[string]*store.Contact{
				"5:alice": {AddressBookID: 5, UID: "alice", ResourceName: "alice", RawVCard: buildVCard("3.0", "UID:alice", "FN:Alice"), ETag: "etag-alice"},
			},
		}
		lockRepo := &fakeLockRepo{
			locks: map[string]*store.Lock{
				"tok-1": {Token: "tok-1", ResourcePath: "/dav/addressbooks/5/alice.vcf", UserID: 1, LockScope: "exclusive", Depth: "0", ExpiresAt: time.Now().Add(time.Hour)},
			},
		}
		h := &Handler{store: &store.Store{AddressBooks: bookRepo, Contacts: contactRepo, Locks: lockRepo}}

		req := httptest.NewRequest(http.MethodDelete, "/dav/addressbooks/5/alice.vcf", nil)
		req = req.WithContext(auth.WithUser(req.Context(), user))
		rr := httptest.NewRecorder()
		h.Delete(rr, req)

		if rr.Code != http.StatusLocked {
			t.Fatalf("RFC 4918 Section 9.6: DELETE on a locked resource without valid lock token MUST return 423, got %d", rr.Code)
		}
	})
}

// ---------------------------------------------------------------------------
// 3. addressbook-query with empty filter (RFC 6352 Section 8.6)
// ---------------------------------------------------------------------------

func TestRFC6352_AddressbookQueryEmptyFilter(t *testing.T) {
	user := &store.User{ID: 1}
	now := store.Now()
	bookRepo := &fakeAddressBookRepo{
		books: map[int64]*store.AddressBook{
			5: {ID: 5, UserID: 1, Name: "Contacts", UpdatedAt: now, CTag: 1},
		},
	}
	contacts := map[string]*store.Contact{
		"5:alice": {AddressBookID: 5, UID: "alice", ResourceName: "alice", RawVCard: buildVCard("3.0", "UID:alice", "FN:Alice"), ETag: "etag-a", LastModified: now},
		"5:bob":   {AddressBookID: 5, UID: "bob", ResourceName: "bob", RawVCard: buildVCard("3.0", "UID:bob", "FN:Bob"), ETag: "etag-b", LastModified: now},
	}
	h := &Handler{store: &store.Store{AddressBooks: bookRepo, Contacts: &fakeContactRepo{contacts: contacts}}}

	t.Run("EmptyFilterMatchesAllResources", func(t *testing.T) {
		body := `<?xml version="1.0" encoding="utf-8"?>
<card:addressbook-query xmlns:card="urn:ietf:params:xml:ns:carddav" xmlns:D="DAV:">
  <D:prop>
    <D:getetag/>
    <card:address-data/>
  </D:prop>
  <card:filter/>
</card:addressbook-query>`

		req := httptest.NewRequest("REPORT", "/dav/addressbooks/5/", strings.NewReader(body))
		req.Header.Set("Depth", "1")
		req = req.WithContext(auth.WithUser(req.Context(), user))
		rr := httptest.NewRecorder()
		h.Report(rr, req)

		if rr.Code != http.StatusMultiStatus {
			t.Fatalf("RFC 6352 Section 8.6: addressbook-query with empty filter MUST return 207, got %d: %s", rr.Code, rr.Body.String())
		}
		respBody := rr.Body.String()
		if !strings.Contains(respBody, "alice") {
			t.Errorf("RFC 6352 Section 8.6: empty filter should match all resources, missing alice: %s", respBody)
		}
		if !strings.Contains(respBody, "bob") {
			t.Errorf("RFC 6352 Section 8.6: empty filter should match all resources, missing bob: %s", respBody)
		}
	})

	t.Run("MissingFilterElementFails", func(t *testing.T) {
		body := `<?xml version="1.0" encoding="utf-8"?>
<card:addressbook-query xmlns:card="urn:ietf:params:xml:ns:carddav" xmlns:D="DAV:">
  <D:prop>
    <D:getetag/>
  </D:prop>
</card:addressbook-query>`

		req := httptest.NewRequest("REPORT", "/dav/addressbooks/5/", strings.NewReader(body))
		req.Header.Set("Depth", "1")
		req = req.WithContext(auth.WithUser(req.Context(), user))
		rr := httptest.NewRecorder()
		h.Report(rr, req)

		if rr.Code != http.StatusBadRequest {
			t.Fatalf("RFC 6352 Sections 8.6 and 10.3: addressbook-query without the required filter element MUST fail, got %d: %s", rr.Code, rr.Body.String())
		}
	})
}

// ---------------------------------------------------------------------------
// 4. PROPPATCH on address object resources (RFC 6352 Section 6.3.2.3)
// ---------------------------------------------------------------------------

func TestRFC6352_ProppatchOnAddressBook(t *testing.T) {
	user := &store.User{ID: 1}
	now := store.Now()

	t.Run("ProppatchDisplayNameUpdatesAddressBook", func(t *testing.T) {
		bookRepo := &fakeAddressBookRepo{
			books: map[int64]*store.AddressBook{
				5: {ID: 5, UserID: 1, Name: "Contacts", UpdatedAt: now},
			},
		}
		h := &Handler{store: &store.Store{AddressBooks: bookRepo, Contacts: &fakeContactRepo{}, Locks: &fakeLockRepo{}}}

		body := `<?xml version="1.0" encoding="utf-8"?>
<D:propertyupdate xmlns:D="DAV:" xmlns:card="urn:ietf:params:xml:ns:carddav">
  <D:set>
    <D:prop>
      <D:displayname>Renamed Book</D:displayname>
    </D:prop>
  </D:set>
</D:propertyupdate>`

		req := httptest.NewRequest("PROPPATCH", "/dav/addressbooks/5/", strings.NewReader(body))
		req = req.WithContext(auth.WithUser(req.Context(), user))
		rr := httptest.NewRecorder()
		h.Proppatch(rr, req)

		if rr.Code != http.StatusMultiStatus {
			t.Fatalf("RFC 4918 Section 9.2: PROPPATCH MUST return 207, got %d: %s", rr.Code, rr.Body.String())
		}
		if !strings.Contains(rr.Body.String(), "200") {
			t.Errorf("RFC 4918 Section 9.2: successful PROPPATCH should include 200 propstat, got: %s", rr.Body.String())
		}

		// Verify the update took effect
		book := bookRepo.books[5]
		if book.Name != "Renamed Book" {
			t.Errorf("PROPPATCH should update the display name, got %q", book.Name)
		}
	})

	t.Run("ProppatchProtectedPropertyReturnsForbidden", func(t *testing.T) {
		bookRepo := &fakeAddressBookRepo{
			books: map[int64]*store.AddressBook{
				5: {ID: 5, UserID: 1, Name: "Contacts", UpdatedAt: now},
			},
		}
		h := &Handler{store: &store.Store{AddressBooks: bookRepo, Contacts: &fakeContactRepo{}, Locks: &fakeLockRepo{}}}

		body := `<?xml version="1.0" encoding="utf-8"?>
<D:propertyupdate xmlns:D="DAV:" xmlns:card="urn:ietf:params:xml:ns:carddav">
  <D:set>
    <D:prop>
      <card:supported-address-data/>
    </D:prop>
  </D:set>
</D:propertyupdate>`

		req := httptest.NewRequest("PROPPATCH", "/dav/addressbooks/5/", strings.NewReader(body))
		req = req.WithContext(auth.WithUser(req.Context(), user))
		rr := httptest.NewRecorder()
		h.Proppatch(rr, req)

		if rr.Code != http.StatusMultiStatus {
			t.Fatalf("expected 207, got %d: %s", rr.Code, rr.Body.String())
		}
		respBody := rr.Body.String()
		if !strings.Contains(respBody, "403") {
			t.Errorf("RFC 6352 Section 6.3.2.3: PROPPATCH on protected property (supported-address-data) MUST return 403, got: %s", respBody)
		}
	})

	t.Run("ProppatchByNonOwnerReturnsForbidden", func(t *testing.T) {
		nonOwner := &store.User{ID: 2}
		bookRepo := &fakeAddressBookRepo{
			books: map[int64]*store.AddressBook{
				5: {ID: 5, UserID: 1, Name: "Contacts", UpdatedAt: now},
			},
		}
		aclRepo := &fakeACLRepo{
			entries: []store.ACLEntry{
				{ResourcePath: "/dav/addressbooks/5", PrincipalHref: "/dav/principals/2/", IsGrant: true, Privilege: "read"},
			},
		}
		h := &Handler{store: &store.Store{AddressBooks: bookRepo, Contacts: &fakeContactRepo{}, Locks: &fakeLockRepo{}, ACLEntries: aclRepo}}

		body := `<?xml version="1.0" encoding="utf-8"?>
<D:propertyupdate xmlns:D="DAV:">
  <D:set>
    <D:prop>
      <D:displayname>Hijacked</D:displayname>
    </D:prop>
  </D:set>
</D:propertyupdate>`

		req := httptest.NewRequest("PROPPATCH", "/dav/addressbooks/5/", strings.NewReader(body))
		req = req.WithContext(auth.WithUser(req.Context(), nonOwner))
		rr := httptest.NewRecorder()
		h.Proppatch(rr, req)

		if rr.Code != http.StatusMultiStatus {
			t.Fatalf("expected 207, got %d: %s", rr.Code, rr.Body.String())
		}
		if !strings.Contains(rr.Body.String(), "403") {
			t.Errorf("RFC 4918/RFC 3744: PROPPATCH by non-owner MUST return 403 propstat, got: %s", rr.Body.String())
		}
	})
}

// ---------------------------------------------------------------------------
// 5. Depth:infinity for addressbook-query (RFC 6352 Section 8.6.1)
// ---------------------------------------------------------------------------

func TestRFC6352_DepthInfinityAddressbookQuery(t *testing.T) {
	user := &store.User{ID: 1}
	now := store.Now()
	bookRepo := &fakeAddressBookRepo{
		books: map[int64]*store.AddressBook{
			5: {ID: 5, UserID: 1, Name: "Contacts", UpdatedAt: now, CTag: 1},
		},
	}
	contacts := map[string]*store.Contact{
		"5:alice": {AddressBookID: 5, UID: "alice", ResourceName: "alice", RawVCard: buildVCard("3.0", "UID:alice", "FN:Alice"), ETag: "etag-a", LastModified: now},
	}
	h := &Handler{store: &store.Store{AddressBooks: bookRepo, Contacts: &fakeContactRepo{contacts: contacts}}}

	t.Run("DepthInfinityReturnsContactResources", func(t *testing.T) {
		body := `<?xml version="1.0" encoding="utf-8"?>
<card:addressbook-query xmlns:card="urn:ietf:params:xml:ns:carddav" xmlns:D="DAV:">
  <D:prop><D:getetag/></D:prop>
  <card:filter>
    <card:prop-filter name="FN"/>
  </card:filter>
</card:addressbook-query>`

		req := httptest.NewRequest("REPORT", "/dav/addressbooks/5/", strings.NewReader(body))
		req.Header.Set("Depth", "infinity")
		req = req.WithContext(auth.WithUser(req.Context(), user))
		rr := httptest.NewRecorder()
		h.Report(rr, req)

		// Depth:infinity on addressbook-query should behave like Depth:1 — return
		// all matching address object resources in the collection.
		if rr.Code != http.StatusMultiStatus {
			t.Fatalf("RFC 6352 Section 8.6.1: addressbook-query with Depth:infinity MUST return 207, got %d: %s", rr.Code, rr.Body.String())
		}
		if !strings.Contains(rr.Body.String(), "alice") {
			t.Errorf("RFC 6352 Section 8.6.1: Depth:infinity query should include address object resources, got: %s", rr.Body.String())
		}
	})
}

// ---------------------------------------------------------------------------
// 6. COPY/MOVE cross-collection integration (RFC 4918 Sections 9.8/9.9)
// ---------------------------------------------------------------------------

func TestRFC6352_CopyMoveIntegration(t *testing.T) {
	user := &store.User{ID: 1}
	now := store.Now()
	aliceVCard := buildVCard("3.0", "UID:alice", "FN:Alice")

	t.Run("CopyContactToAnotherAddressBook", func(t *testing.T) {
		bookRepo := &fakeAddressBookRepo{
			books: map[int64]*store.AddressBook{
				5: {ID: 5, UserID: 1, Name: "Personal", UpdatedAt: now},
				6: {ID: 6, UserID: 1, Name: "Work", UpdatedAt: now},
			},
		}
		contactRepo := &fakeContactRepo{
			contacts: map[string]*store.Contact{
				"5:alice": {AddressBookID: 5, UID: "alice", ResourceName: "alice", RawVCard: aliceVCard, ETag: "etag-a", LastModified: now},
			},
		}
		lockRepo := &fakeLockRepo{}
		h := &Handler{store: &store.Store{AddressBooks: bookRepo, Contacts: contactRepo, Locks: lockRepo}}

		req := httptest.NewRequest("COPY", "/dav/addressbooks/5/alice.vcf", nil)
		req.Header.Set("Destination", "/dav/addressbooks/6/alice.vcf")
		req = req.WithContext(auth.WithUser(req.Context(), user))
		rr := httptest.NewRecorder()
		h.Copy(rr, req)

		if rr.Code != http.StatusCreated {
			t.Fatalf("RFC 4918 Section 9.8: COPY to a new resource MUST return 201, got %d: %s", rr.Code, rr.Body.String())
		}
		// Verify original still exists
		orig, _ := contactRepo.GetByResourceName(context.Background(), 5, "alice")
		if orig == nil {
			t.Error("RFC 4918 Section 9.8: COPY must not remove the source resource")
		}
		// Verify copy exists
		copied, _ := contactRepo.GetByResourceName(context.Background(), 6, "alice")
		if copied == nil {
			t.Error("RFC 4918 Section 9.8: COPY must create the destination resource")
		}
	})

	t.Run("MoveContactToAnotherAddressBook", func(t *testing.T) {
		bookRepo := &fakeAddressBookRepo{
			books: map[int64]*store.AddressBook{
				5: {ID: 5, UserID: 1, Name: "Personal", UpdatedAt: now},
				6: {ID: 6, UserID: 1, Name: "Work", UpdatedAt: now},
			},
		}
		contactRepo := &fakeContactRepo{
			contacts: map[string]*store.Contact{
				"5:bob": {AddressBookID: 5, UID: "bob", ResourceName: "bob", RawVCard: buildVCard("3.0", "UID:bob", "FN:Bob"), ETag: "etag-b", LastModified: now},
			},
		}
		lockRepo := &fakeLockRepo{}
		h := &Handler{store: &store.Store{AddressBooks: bookRepo, Contacts: contactRepo, Locks: lockRepo}}

		req := httptest.NewRequest("MOVE", "/dav/addressbooks/5/bob.vcf", nil)
		req.Header.Set("Destination", "/dav/addressbooks/6/bob.vcf")
		req = req.WithContext(auth.WithUser(req.Context(), user))
		rr := httptest.NewRecorder()
		h.Move(rr, req)

		if rr.Code != http.StatusCreated {
			t.Fatalf("RFC 4918 Section 9.9: MOVE to a new resource MUST return 201, got %d: %s", rr.Code, rr.Body.String())
		}
		// Verify source is removed
		orig, _ := contactRepo.GetByResourceName(context.Background(), 5, "bob")
		if orig != nil {
			t.Error("RFC 4918 Section 9.9: MOVE must remove the source resource")
		}
		// Verify destination exists
		moved, _ := contactRepo.GetByResourceName(context.Background(), 6, "bob")
		if moved == nil {
			t.Error("RFC 4918 Section 9.9: MOVE must create the destination resource")
		}
	})

	t.Run("CopyOverwriteFalseFailsIfDestExists", func(t *testing.T) {
		bookRepo := &fakeAddressBookRepo{
			books: map[int64]*store.AddressBook{
				5: {ID: 5, UserID: 1, Name: "Personal", UpdatedAt: now},
				6: {ID: 6, UserID: 1, Name: "Work", UpdatedAt: now},
			},
		}
		contactRepo := &fakeContactRepo{
			contacts: map[string]*store.Contact{
				"5:alice": {AddressBookID: 5, UID: "alice", ResourceName: "alice", RawVCard: aliceVCard, ETag: "etag-a", LastModified: now},
				"6:alice": {AddressBookID: 6, UID: "alice", ResourceName: "alice", RawVCard: aliceVCard, ETag: "etag-a2", LastModified: now},
			},
		}
		lockRepo := &fakeLockRepo{}
		h := &Handler{store: &store.Store{AddressBooks: bookRepo, Contacts: contactRepo, Locks: lockRepo}}

		req := httptest.NewRequest("COPY", "/dav/addressbooks/5/alice.vcf", nil)
		req.Header.Set("Destination", "/dav/addressbooks/6/alice.vcf")
		req.Header.Set("Overwrite", "F")
		req = req.WithContext(auth.WithUser(req.Context(), user))
		rr := httptest.NewRecorder()
		h.Copy(rr, req)

		if rr.Code != http.StatusPreconditionFailed {
			t.Fatalf("RFC 4918 Section 9.8: COPY with Overwrite:F to existing resource MUST return 412, got %d", rr.Code)
		}
	})

	t.Run("MoveLockedSourceReturns423", func(t *testing.T) {
		bookRepo := &fakeAddressBookRepo{
			books: map[int64]*store.AddressBook{
				5: {ID: 5, UserID: 1, Name: "Personal", UpdatedAt: now},
				6: {ID: 6, UserID: 1, Name: "Work", UpdatedAt: now},
			},
		}
		contactRepo := &fakeContactRepo{
			contacts: map[string]*store.Contact{
				"5:carol": {AddressBookID: 5, UID: "carol", ResourceName: "carol", RawVCard: buildVCard("3.0", "UID:carol", "FN:Carol"), ETag: "etag-c", LastModified: now},
			},
		}
		lockRepo := &fakeLockRepo{
			locks: map[string]*store.Lock{
				"tok-src": {Token: "tok-src", ResourcePath: "/dav/addressbooks/5/carol.vcf", UserID: 1, LockScope: "exclusive", Depth: "0", ExpiresAt: time.Now().Add(time.Hour)},
			},
		}
		h := &Handler{store: &store.Store{AddressBooks: bookRepo, Contacts: contactRepo, Locks: lockRepo}}

		req := httptest.NewRequest("MOVE", "/dav/addressbooks/5/carol.vcf", nil)
		req.Header.Set("Destination", "/dav/addressbooks/6/carol.vcf")
		req = req.WithContext(auth.WithUser(req.Context(), user))
		rr := httptest.NewRecorder()
		h.Move(rr, req)

		if rr.Code != http.StatusLocked {
			t.Fatalf("RFC 4918 Section 9.9: MOVE on a locked source without lock token MUST return 423, got %d", rr.Code)
		}
	})
}

// ---------------------------------------------------------------------------
// 7. ACL enforcement on writes (RFC 3744)
// ---------------------------------------------------------------------------

func TestRFC6352_ACLDenyBlocksPutDelete(t *testing.T) {
	user := &store.User{ID: 2, PrimaryEmail: "reader@example.com"}
	now := store.Now()

	t.Run("DenyBindBlocksPUT", func(t *testing.T) {
		bookRepo := &fakeAddressBookRepo{
			books: map[int64]*store.AddressBook{
				5: {ID: 5, UserID: 1, Name: "Shared Contacts", UpdatedAt: now},
			},
		}
		contactRepo := &fakeContactRepo{contacts: map[string]*store.Contact{}}
		aclRepo := &fakeACLRepo{
			entries: []store.ACLEntry{
				{ResourcePath: "/dav/addressbooks/5", PrincipalHref: "/dav/principals/2/", IsGrant: false, Privilege: "bind"},
			},
		}
		lockRepo := &fakeLockRepo{}
		h := &Handler{store: &store.Store{AddressBooks: bookRepo, Contacts: contactRepo, ACLEntries: aclRepo, Locks: lockRepo}}

		vcard := buildVCard("3.0", "UID:alice", "FN:Alice")
		req := newAddressBookPutRequest("/dav/addressbooks/5/alice.vcf", strings.NewReader(vcard))
		req = req.WithContext(auth.WithUser(req.Context(), user))
		rr := httptest.NewRecorder()
		h.Put(rr, req)

		if rr.Code != http.StatusForbidden {
			t.Fatalf("RFC 3744: deny bind ACE must block PUT (new resource creation), got %d: %s", rr.Code, rr.Body.String())
		}
	})

	t.Run("DenyUnbindBlocksDELETE", func(t *testing.T) {
		bookRepo := &fakeAddressBookRepo{
			books: map[int64]*store.AddressBook{
				5: {ID: 5, UserID: 1, Name: "Shared Contacts", UpdatedAt: now},
			},
		}
		contactRepo := &fakeContactRepo{
			contacts: map[string]*store.Contact{
				"5:alice": {AddressBookID: 5, UID: "alice", ResourceName: "alice", RawVCard: buildVCard("3.0", "UID:alice", "FN:Alice"), ETag: "etag-a"},
			},
		}
		aclRepo := &fakeACLRepo{
			entries: []store.ACLEntry{
				{ResourcePath: "/dav/addressbooks/5", PrincipalHref: "/dav/principals/2/", IsGrant: false, Privilege: "unbind"},
			},
		}
		lockRepo := &fakeLockRepo{}
		h := &Handler{store: &store.Store{AddressBooks: bookRepo, Contacts: contactRepo, ACLEntries: aclRepo, Locks: lockRepo}}

		req := httptest.NewRequest(http.MethodDelete, "/dav/addressbooks/5/alice.vcf", nil)
		req = req.WithContext(auth.WithUser(req.Context(), user))
		rr := httptest.NewRecorder()
		h.Delete(rr, req)

		if rr.Code != http.StatusForbidden {
			t.Fatalf("RFC 3744: deny unbind ACE must block DELETE, got %d: %s", rr.Code, rr.Body.String())
		}
	})

	t.Run("GrantWriteContentAllowsPUTUpdate", func(t *testing.T) {
		bookRepo := &fakeAddressBookRepo{
			books: map[int64]*store.AddressBook{
				5: {ID: 5, UserID: 1, Name: "Shared Contacts", UpdatedAt: now},
			},
		}
		contactRepo := &fakeContactRepo{
			contacts: map[string]*store.Contact{
				"5:alice": {AddressBookID: 5, UID: "alice", ResourceName: "alice", RawVCard: buildVCard("3.0", "UID:alice", "FN:Alice"), ETag: "etag-a"},
			},
		}
		aclRepo := &fakeACLRepo{
			entries: []store.ACLEntry{
				{ResourcePath: "/dav/addressbooks/5", PrincipalHref: "/dav/principals/2/", IsGrant: true, Privilege: "write-content"},
			},
		}
		lockRepo := &fakeLockRepo{}
		h := &Handler{store: &store.Store{AddressBooks: bookRepo, Contacts: contactRepo, ACLEntries: aclRepo, Locks: lockRepo, DeletedResources: &fakeDeletedResourceRepo{}}}

		vcard := buildVCard("3.0", "UID:alice", "FN:Alice Updated")
		req := newAddressBookPutRequest("/dav/addressbooks/5/alice.vcf", strings.NewReader(vcard))
		req = req.WithContext(auth.WithUser(req.Context(), user))
		rr := httptest.NewRecorder()
		h.Put(rr, req)

		if rr.Code != http.StatusNoContent {
			t.Fatalf("RFC 3744: grant write-content ACE must allow PUT updates, got %d: %s", rr.Code, rr.Body.String())
		}
	})
}

// ---------------------------------------------------------------------------
// 8. Multiple shared locks (RFC 4918 Section 6.2)
// ---------------------------------------------------------------------------

func TestRFC6352_SharedLocksAllowWrites(t *testing.T) {
	user1 := &store.User{ID: 1, PrimaryEmail: "alice@example.com"}
	user2 := &store.User{ID: 2, PrimaryEmail: "bob@example.com"}
	now := store.Now()

	bookRepo := &fakeAddressBookRepo{
		books: map[int64]*store.AddressBook{
			5: {ID: 5, UserID: 1, Name: "Contacts", UpdatedAt: now},
		},
	}
	contactRepo := &fakeContactRepo{contacts: map[string]*store.Contact{}}
	lockRepo := &fakeLockRepo{locks: map[string]*store.Lock{}}
	aclRepo := &fakeACLRepo{
		entries: []store.ACLEntry{
			{ResourcePath: "/dav/addressbooks/5", PrincipalHref: "/dav/principals/2/", IsGrant: true, Privilege: "bind"},
			{ResourcePath: "/dav/addressbooks/5", PrincipalHref: "/dav/principals/2/", IsGrant: true, Privilege: "write-content"},
		},
	}
	h := &Handler{store: &store.Store{AddressBooks: bookRepo, Contacts: contactRepo, Locks: lockRepo, ACLEntries: aclRepo}}

	// Both users take shared locks
	sharedBody := `<?xml version="1.0" encoding="utf-8"?><D:lockinfo xmlns:D="DAV:"><D:lockscope><D:shared/></D:lockscope><D:locktype><D:write/></D:locktype></D:lockinfo>`
	req1 := httptest.NewRequest("LOCK", "/dav/addressbooks/5/shared-contact.vcf", strings.NewReader(sharedBody))
	req1.Header.Set("Timeout", "Second-3600")
	req1 = req1.WithContext(auth.WithUser(req1.Context(), user1))
	rr1 := httptest.NewRecorder()
	h.Lock(rr1, req1)

	if rr1.Code != http.StatusCreated {
		t.Fatalf("expected shared lock to succeed, got %d: %s", rr1.Code, rr1.Body.String())
	}
	token1 := rr1.Header().Get("Lock-Token")

	req2 := httptest.NewRequest("LOCK", "/dav/addressbooks/5/shared-contact.vcf", strings.NewReader(sharedBody))
	req2.Header.Set("Timeout", "Second-3600")
	req2 = req2.WithContext(auth.WithUser(req2.Context(), user2))
	rr2 := httptest.NewRecorder()
	h.Lock(rr2, req2)

	if rr2.Code != http.StatusCreated {
		t.Fatalf("expected second shared lock to succeed, got %d: %s", rr2.Code, rr2.Body.String())
	}
	token2 := rr2.Header().Get("Lock-Token")

	t.Run("SharedLockHolderCanWriteWithToken", func(t *testing.T) {
		vcard := buildVCard("3.0", "UID:shared-contact", "FN:Shared Contact")
		req := newAddressBookPutRequest("/dav/addressbooks/5/shared-contact.vcf", strings.NewReader(vcard))
		// Strip angle brackets from lock token for If header
		cleanToken := strings.TrimPrefix(strings.TrimSuffix(token1, ">"), "<")
		req.Header.Set("If", "(<"+cleanToken+">)")
		req = req.WithContext(auth.WithUser(req.Context(), user1))
		rr := httptest.NewRecorder()
		h.Put(rr, req)

		if rr.Code != http.StatusCreated {
			t.Fatalf("RFC 4918 Section 6.2: shared lock holder should be able to write with their token, got %d: %s", rr.Code, rr.Body.String())
		}
	})

	t.Run("OtherSharedLockHolderCanWriteWithTheirToken", func(t *testing.T) {
		vcard := buildVCard("3.0", "UID:shared-contact", "FN:Shared Contact Updated by Bob")
		req := newAddressBookPutRequest("/dav/addressbooks/5/shared-contact.vcf", strings.NewReader(vcard))
		cleanToken := strings.TrimPrefix(strings.TrimSuffix(token2, ">"), "<")
		req.Header.Set("If", "(<"+cleanToken+">)")
		req = req.WithContext(auth.WithUser(req.Context(), user2))
		rr := httptest.NewRecorder()
		h.Put(rr, req)

		if rr.Code != http.StatusNoContent {
			t.Fatalf("RFC 4918 Section 6.2: second shared lock holder should be able to write with their token, got %d: %s", rr.Code, rr.Body.String())
		}
	})

	t.Run("WriteWithoutTokenBlockedBySharedLock", func(t *testing.T) {
		vcard := buildVCard("3.0", "UID:shared-contact", "FN:No Token")
		req := newAddressBookPutRequest("/dav/addressbooks/5/shared-contact.vcf", strings.NewReader(vcard))
		// No If header — no lock token provided
		req = req.WithContext(auth.WithUser(req.Context(), user1))
		rr := httptest.NewRecorder()
		h.Put(rr, req)

		if rr.Code != http.StatusLocked {
			t.Fatalf("RFC 4918 Section 6.2: writes to a shared-locked resource without a valid lock token MUST return 423, got %d", rr.Code)
		}
	})
}

// ---------------------------------------------------------------------------
// 9. Lock timeout capping integration test
// ---------------------------------------------------------------------------

func TestRFC6352_LockTimeoutCappingIntegration(t *testing.T) {
	user := &store.User{ID: 1, PrimaryEmail: "alice@example.com"}
	lockRepo := &fakeLockRepo{locks: map[string]*store.Lock{}}
	bookRepo := &fakeAddressBookRepo{
		books: map[int64]*store.AddressBook{
			5: {ID: 5, UserID: user.ID, Name: "Contacts"},
		},
	}
	h := &Handler{store: &store.Store{AddressBooks: bookRepo, Locks: lockRepo}}

	t.Run("OversizedTimeoutIsCapped", func(t *testing.T) {
		lockBody := `<?xml version="1.0" encoding="utf-8"?><D:lockinfo xmlns:D="DAV:"><D:lockscope><D:exclusive/></D:lockscope><D:locktype><D:write/></D:locktype></D:lockinfo>`
		req := httptest.NewRequest("LOCK", "/dav/addressbooks/5/capped.vcf", strings.NewReader(lockBody))
		req.Header.Set("Timeout", "Second-99999999")
		req = req.WithContext(auth.WithUser(req.Context(), user))
		rr := httptest.NewRecorder()
		h.Lock(rr, req)

		if rr.Code != http.StatusOK {
			t.Fatalf("expected LOCK to succeed, got %d: %s", rr.Code, rr.Body.String())
		}

		// Check the response contains the capped timeout
		respBody := rr.Body.String()
		expected := fmt.Sprintf("Second-%d", maxLockTimeout)
		if !strings.Contains(respBody, expected) {
			t.Errorf("RFC 4918: server SHOULD cap oversized timeouts; expected %q in response, got: %s", expected, respBody)
		}
	})

	t.Run("InfiniteTimeoutIsCapped", func(t *testing.T) {
		lockBody := `<?xml version="1.0" encoding="utf-8"?><D:lockinfo xmlns:D="DAV:"><D:lockscope><D:exclusive/></D:lockscope><D:locktype><D:write/></D:locktype></D:lockinfo>`
		req := httptest.NewRequest("LOCK", "/dav/addressbooks/5/infinite.vcf", strings.NewReader(lockBody))
		req.Header.Set("Timeout", "Infinite")
		req = req.WithContext(auth.WithUser(req.Context(), user))
		rr := httptest.NewRecorder()
		h.Lock(rr, req)

		if rr.Code != http.StatusOK {
			t.Fatalf("expected LOCK to succeed, got %d: %s", rr.Code, rr.Body.String())
		}

		respBody := rr.Body.String()
		expected := fmt.Sprintf("Second-%d", maxLockTimeout)
		if !strings.Contains(respBody, expected) {
			t.Errorf("RFC 4918: server SHOULD cap Infinite timeouts; expected %q in response, got: %s", expected, respBody)
		}
	})
}

// ---------------------------------------------------------------------------
// 10. content-type/version in address-data responses (RFC 6352 Section 10.3)
// ---------------------------------------------------------------------------

func TestRFC6352_AddressDataResponseFormat(t *testing.T) {
	user := &store.User{ID: 1}
	now := store.Now()
	aliceVCard := buildVCard("3.0", "UID:alice", "FN:Alice", "EMAIL:alice@example.com")
	bookRepo := &fakeAddressBookRepo{
		books: map[int64]*store.AddressBook{
			5: {ID: 5, UserID: 1, Name: "Contacts", UpdatedAt: now, CTag: 1},
		},
	}
	contacts := map[string]*store.Contact{
		"5:alice": {AddressBookID: 5, UID: "alice", ResourceName: "alice", RawVCard: aliceVCard, ETag: "etag-a", LastModified: now},
	}
	h := &Handler{store: &store.Store{AddressBooks: bookRepo, Contacts: &fakeContactRepo{contacts: contacts}}}

	t.Run("MultigetIncludesAddressDataInResponse", func(t *testing.T) {
		body := `<?xml version="1.0" encoding="utf-8"?>
<card:addressbook-multiget xmlns:card="urn:ietf:params:xml:ns:carddav" xmlns:D="DAV:">
  <D:prop>
    <D:getetag/>
    <card:address-data/>
  </D:prop>
  <D:href>/dav/addressbooks/5/alice.vcf</D:href>
		</card:addressbook-multiget>`

		req := httptest.NewRequest("REPORT", "/dav/addressbooks/5/", strings.NewReader(body))
		req.Header.Set("Depth", "0")
		req = req.WithContext(auth.WithUser(req.Context(), user))
		rr := httptest.NewRecorder()
		h.Report(rr, req)

		if rr.Code != http.StatusMultiStatus {
			t.Fatalf("expected 207, got %d: %s", rr.Code, rr.Body.String())
		}
		respBody := rr.Body.String()

		// RFC 6352 Section 10.3: address-data in responses must contain the actual vCard data
		if !strings.Contains(respBody, "BEGIN:VCARD") {
			t.Errorf("RFC 6352 Section 10.3: address-data MUST contain the vCard data, got: %s", respBody)
		}
		if !strings.Contains(respBody, "FN:Alice") {
			t.Errorf("RFC 6352 Section 10.3: address-data should contain the requested contact's properties, got: %s", respBody)
		}
		if !strings.Contains(respBody, "address-data") {
			t.Errorf("RFC 6352 Section 10.3: response MUST include the address-data element, got: %s", respBody)
		}
	})

	t.Run("QueryIncludesAddressDataWithVCardContent", func(t *testing.T) {
		body := `<?xml version="1.0" encoding="utf-8"?>
<card:addressbook-query xmlns:card="urn:ietf:params:xml:ns:carddav" xmlns:D="DAV:">
  <D:prop>
    <D:getetag/>
    <card:address-data/>
  </D:prop>
  <card:filter>
    <card:prop-filter name="FN">
      <card:text-match collation="i;unicode-casemap" match-type="contains">Alice</card:text-match>
    </card:prop-filter>
  </card:filter>
</card:addressbook-query>`

		req := httptest.NewRequest("REPORT", "/dav/addressbooks/5/", strings.NewReader(body))
		req.Header.Set("Depth", "1")
		req = req.WithContext(auth.WithUser(req.Context(), user))
		rr := httptest.NewRecorder()
		h.Report(rr, req)

		if rr.Code != http.StatusMultiStatus {
			t.Fatalf("expected 207, got %d: %s", rr.Code, rr.Body.String())
		}
		respBody := rr.Body.String()

		// Verify the response contains the vCard data
		if !strings.Contains(respBody, "BEGIN:VCARD") {
			t.Errorf("RFC 6352 Section 10.3: addressbook-query response MUST include vCard data when address-data is requested, got: %s", respBody)
		}
		// Verify the content type is indicated in the response
		if !strings.Contains(respBody, "text/vcard") {
			t.Logf("Note: address-data element may not include explicit content-type attribute — acceptable if vCard is returned correctly")
		}
	})

	t.Run("GetReturnsVCardContentType", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/dav/addressbooks/5/alice.vcf", nil)
		req = req.WithContext(auth.WithUser(req.Context(), user))
		rr := httptest.NewRecorder()
		h.Get(rr, req)

		if rr.Code != http.StatusOK {
			t.Fatalf("expected 200, got %d", rr.Code)
		}
		ct := rr.Header().Get("Content-Type")
		if ct != "text/vcard" {
			t.Errorf("RFC 6352 Section 10.3: GET on address object MUST return Content-Type: text/vcard, got %q", ct)
		}
		if !strings.Contains(rr.Body.String(), "VERSION:3.0") {
			t.Errorf("RFC 6352 Section 10.3: address data MUST include VERSION property, got: %s", rr.Body.String())
		}
	})
}
