package dav

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/jw6ventures/calcard/internal/auth"
	"github.com/jw6ventures/calcard/internal/store"
)

// lockedByAliceServer builds an address book Alice owns and Bob may write to,
// carrying one exclusive lock Alice holds over a member resource.
func lockedByAliceServer(t *testing.T) (*DavServer, *store.User, *store.User, string) {
	t.Helper()
	alice := &store.User{ID: 1, PrimaryEmail: "alice@example.com"}
	bob := &store.User{ID: 2, PrimaryEmail: "bob@example.com"}
	now := store.Now()
	const token = "opaquelocktoken:alice-secret"

	bookRepo := &fakeAddressBookRepo{
		books: map[int64]*store.AddressBook{
			5: {ID: 5, UserID: alice.ID, Name: "Contacts", UpdatedAt: now},
		},
	}
	contactRepo := &fakeContactRepo{
		contacts: map[string]*store.Contact{
			"5:alice": {AddressBookID: 5, UID: "alice", ResourceName: "alice", RawVCard: buildVCard("3.0", "UID:alice", "FN:Alice Example"), ETag: "etag-alice", LastModified: now},
		},
	}
	lockRepo := &fakeLockRepo{
		locks: map[string]*store.Lock{
			token: {
				Token:          token,
				ResourcePath:   "/dav/addressbooks/5/alice.vcf",
				UserID:         alice.ID,
				LockScope:      "exclusive",
				LockType:       "write",
				Depth:          "0",
				TimeoutSeconds: 3600,
				ExpiresAt:      time.Now().Add(time.Hour),
			},
		},
	}
	aclRepo := &fakeACLRepo{
		entries: []store.ACLEntry{
			{ResourcePath: "/dav/addressbooks/5", PrincipalHref: "/dav/principals/2/", IsGrant: true, Privilege: "write-content"},
			{ResourcePath: "/dav/addressbooks/5", PrincipalHref: "/dav/principals/2/", IsGrant: true, Privilege: "read"},
		},
	}
	h := &DavServer{store: &store.Store{AddressBooks: bookRepo, Contacts: contactRepo, Locks: lockRepo, ACLEntries: aclRepo}}
	return h, alice, bob, token
}

// RFC 4918 Section 6.4: a server MUST check that the authenticated principal
// matches the lock creator in addition to checking for a valid lock token.
// Without that check a write lock is not a boundary between principals at all:
// on a shared collection anyone holding the token can write through it.
func TestLockedWriteRejectsAPrincipalThatIsNotTheLockCreator(t *testing.T) {
	h, _, bob, token := lockedByAliceServer(t)

	req := newAddressBookPutRequest("/dav/addressbooks/5/alice.vcf", strings.NewReader(buildVCard("3.0", "UID:alice", "FN:Bob Was Here")))
	req.Header.Set("If", "(<"+token+">)")
	req = req.WithContext(auth.WithUser(req.Context(), bob))
	rr := httptest.NewRecorder()

	h.Put(rr, req)

	if rr.Code != http.StatusLocked {
		t.Fatalf("RFC 4918 Section 6.4: a principal that did not create the lock must not write through it even holding its token, got %d: %s", rr.Code, rr.Body.String())
	}
}

// The lock creator must still be able to write through its own lock, which is
// the whole point of submitting the token.
func TestLockedWriteAllowsTheLockCreator(t *testing.T) {
	h, alice, _, token := lockedByAliceServer(t)

	req := newAddressBookPutRequest("/dav/addressbooks/5/alice.vcf", strings.NewReader(buildVCard("3.0", "UID:alice", "FN:Alice Example Edited")))
	req.Header.Set("If", "(<"+token+">)")
	req = req.WithContext(auth.WithUser(req.Context(), alice))
	rr := httptest.NewRecorder()

	h.Put(rr, req)

	if rr.Code != http.StatusNoContent && rr.Code != http.StatusCreated {
		t.Fatalf("the lock creator submitting its own token must be allowed to write, got %d: %s", rr.Code, rr.Body.String())
	}
}

// DAV:lockdiscovery is readable by anyone with read access, so returning the
// token in it hands every reader the credential the write check enforces.
// RFC 4918 Section 15.8 allows the token to be withheld; the rest of the
// activelock element still reports that the resource is locked.
func TestLockDiscoveryWithholdsTheTokenFromOtherPrincipals(t *testing.T) {
	h, alice, bob, token := lockedByAliceServer(t)

	propfind := func(user *store.User) string {
		body := `<?xml version="1.0" encoding="utf-8"?><D:propfind xmlns:D="DAV:"><D:prop><D:lockdiscovery/></D:prop></D:propfind>`
		req := httptest.NewRequest("PROPFIND", "/dav/addressbooks/5/alice.vcf", strings.NewReader(body))
		req.Header.Set("Depth", "0")
		req.Header.Set("Content-Type", "application/xml")
		req = req.WithContext(auth.WithUser(req.Context(), user))
		rr := httptest.NewRecorder()
		h.Propfind(rr, req)
		if rr.Code != http.StatusMultiStatus {
			t.Fatalf("PROPFIND lockdiscovery: got %d: %s", rr.Code, rr.Body.String())
		}
		return rr.Body.String()
	}

	if got := propfind(bob); strings.Contains(got, token) {
		t.Errorf("lockdiscovery disclosed another principal's lock token to a reader:\n%s", got)
	}
	// The lock itself is still reported, so a client can tell the resource is
	// locked and that it does not hold the lock.
	if got := propfind(bob); !strings.Contains(got, "activelock") {
		t.Errorf("lockdiscovery hid the lock entirely from a reader:\n%s", got)
	}
	if got := propfind(alice); !strings.Contains(got, token) {
		t.Errorf("lockdiscovery withheld the token from the lock creator:\n%s", got)
	}
}
