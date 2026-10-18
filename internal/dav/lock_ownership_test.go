package dav

import (
	"errors"
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
				ResourcePath:   "/dav/addressbooks/5/alice",
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

// A lock binds a write to its resource however the request spells the path:
// the write's target is cleaned and canonicalized before its locks are looked
// up, so an empty or "." segment, or the collection's name, reaches the lock
// stored at the member's canonical path.
func TestLockedWriteFindsTheLockWhateverTheTargetsSpelling(t *testing.T) {
	const token = "opaquelocktoken:event"
	owner := &store.User{ID: 1}
	h := &DavServer{store: &store.Store{
		Calendars: &fakeCalendarRepo{
			calendars:  map[int64]*store.Calendar{5: {ID: 5, UserID: owner.ID, Name: "Owner"}},
			accessible: []store.CalendarAccess{{Calendar: store.Calendar{ID: 5, UserID: owner.ID, Name: "Owner"}}},
		},
		Locks: &fakeLockRepo{locks: map[string]*store.Lock{token: {
			Token: token, ResourcePath: "/dav/calendars/5/event", UserID: owner.ID, LockScope: "exclusive", LockType: "write",
			Depth: "0", ExpiresAt: time.Now().Add(time.Hour),
		}}},
	}}
	for _, target := range []string{"/dav/calendars/5/event.ics", "/dav/calendars/5//event.ics", "/dav/calendars/5/./event.ics", "/dav/calendars/5/event", "/dav/calendars/Owner/event.ics"} {
		t.Run(target, func(t *testing.T) {
			req := httptest.NewRequest(http.MethodPut, "/dav/calendars/5/event.ics", nil)
			req = req.WithContext(auth.WithUser(withDAVRequestState(req.Context()), owner))
			if allowed, err := h.checkLock(req, target); err != nil || allowed {
				t.Fatalf("checkLock(%s) without the token = %v, %v; want the lock to bind it", target, allowed, err)
			}
			req = httptest.NewRequest(http.MethodPut, "/dav/calendars/5/event.ics", nil)
			req.Header.Set("If", "(<"+token+">)")
			req = req.WithContext(auth.WithUser(withDAVRequestState(req.Context()), owner))
			if allowed, err := h.checkLock(req, target); err != nil || !allowed {
				t.Fatalf("checkLock(%s) with the lock's token = %v, %v; want it allowed", target, allowed, err)
			}
		})
	}
}

// RFC 4918 §7.5 and §10.4.1 have a lock token submitted by its appearing in
// the If header, whatever list it appears in and whether or not that list is
// evaluated, so a COPY or MOVE may submit its destination's token untagged,
// beside the source's, in one list with it, or beside the (Not <DAV:no-lock>)
// idiom of §10.4.8. A tagged list submits it too, whatever resource its tag
// names: the tag decides which resource the list's conditions are evaluated
// against, not which locks its tokens are submitted for. A token appearing
// only after Not is not submitted. Whatever the form, a token counts only in
// its lock creator's hands (§6.4).
func TestIfHeaderSubmitsEveryTokenItNamesForEveryResource(t *testing.T) {
	const (
		sourceToken      = "opaquelocktoken:source-lock"
		destinationToken = "opaquelocktoken:destination-lock"
	)
	owner, other := &store.User{ID: 1}, &store.User{ID: 3}
	const (
		eventSource, eventDestination     = "/dav/calendars/5/alice.ics", "/dav/calendars/6/renamed.ics"
		contactSource, contactDestination = "/dav/addressbooks/7/carol.vcf", "/dav/addressbooks/8/renamed.vcf"
	)
	tests := []struct {
		name, method, source, destination string
		ifHeader                          string
		destinationLockedBy               *store.User
		want                              int
	}{
		{name: "source and destination tokens in separate untagged lists", method: "MOVE", source: eventSource, destination: eventDestination,
			ifHeader: "(<" + sourceToken + ">) (<" + destinationToken + ">)", destinationLockedBy: owner, want: http.StatusNoContent},
		{name: "source and destination tokens in one untagged list", method: "MOVE", source: eventSource, destination: eventDestination,
			ifHeader: "(<" + sourceToken + "> <" + destinationToken + ">)", destinationLockedBy: owner, want: http.StatusNoContent},
		{name: "destination token beside the no-lock idiom", method: "COPY", source: eventSource, destination: eventDestination,
			ifHeader: "(<" + destinationToken + ">) (Not <DAV:no-lock>)", destinationLockedBy: owner, want: http.StatusNoContent},
		{name: "destination token untagged", method: "COPY", source: eventSource, destination: eventDestination,
			ifHeader: "(<" + destinationToken + ">)", destinationLockedBy: owner, want: http.StatusNoContent},
		{name: "destination token tagged", method: "COPY", source: eventSource, destination: eventDestination,
			ifHeader: "<" + eventDestination + "> (<" + destinationToken + ">)", destinationLockedBy: owner, want: http.StatusNoContent},
		{name: "contact source and destination tokens in separate untagged lists", method: "MOVE", source: contactSource, destination: contactDestination,
			ifHeader: "(<" + sourceToken + ">) (<" + destinationToken + ">)", destinationLockedBy: owner, want: http.StatusNoContent},
		{name: "contact destination token tagged", method: "MOVE", source: contactSource, destination: contactDestination,
			ifHeader: "<" + contactSource + "> (<" + sourceToken + ">) <" + contactDestination + "> (<" + destinationToken + ">)", destinationLockedBy: owner, want: http.StatusNoContent},
		{name: "another principal's destination token untagged", method: "COPY", source: eventSource, destination: eventDestination,
			ifHeader: "(<" + destinationToken + ">)", destinationLockedBy: other, want: http.StatusLocked},
		{name: "another principal's destination token beside the no-lock idiom", method: "COPY", source: eventSource, destination: eventDestination,
			ifHeader: "(<" + destinationToken + ">) (Not <DAV:no-lock>)", destinationLockedBy: other, want: http.StatusLocked},
		{name: "another principal's destination token tagged", method: "COPY", source: eventSource, destination: eventDestination,
			ifHeader: "<" + eventDestination + "> (<" + destinationToken + ">)", destinationLockedBy: other, want: http.StatusLocked},
		{name: "another principal's contact destination token untagged", method: "MOVE", source: contactSource, destination: contactDestination,
			ifHeader: "(<" + sourceToken + ">) (<" + destinationToken + ">)", destinationLockedBy: other, want: http.StatusLocked},
		{name: "destination token in a list tagged with the source", method: "COPY", source: eventSource, destination: eventDestination,
			ifHeader: "<" + eventSource + "> (<" + destinationToken + ">)", destinationLockedBy: owner, want: http.StatusNoContent},
		{name: "source and destination tokens each tagged with the other", method: "MOVE", source: eventSource, destination: eventDestination,
			ifHeader: "<" + eventSource + "> (<" + destinationToken + ">) <" + eventDestination + "> (<" + sourceToken + ">)", destinationLockedBy: owner, want: http.StatusNoContent},
		{name: "destination token in a list tagged with another server's resource", method: "COPY", source: eventSource, destination: eventDestination,
			ifHeader: "<http://elsewhere.example/dav/calendars/6/renamed.ics> (<" + destinationToken + ">)", destinationLockedBy: owner, want: http.StatusNoContent},
		{name: "contact destination token in a list tagged with the source", method: "COPY", source: contactSource, destination: contactDestination,
			ifHeader: "<" + contactSource + "> (<" + destinationToken + ">)", destinationLockedBy: owner, want: http.StatusNoContent},
		{name: "contact source and destination tokens each tagged with the other", method: "MOVE", source: contactSource, destination: contactDestination,
			ifHeader: "<" + contactSource + "> (<" + destinationToken + ">) <" + contactDestination + "> (<" + sourceToken + ">)", destinationLockedBy: owner, want: http.StatusNoContent},
		{name: "another principal's destination token in a list tagged with the source", method: "COPY", source: eventSource, destination: eventDestination,
			ifHeader: "<" + eventSource + "> (<" + destinationToken + ">)", destinationLockedBy: other, want: http.StatusLocked},
		{name: "another principal's contact destination token in a list tagged with the source", method: "COPY", source: contactSource, destination: contactDestination,
			ifHeader: "<" + contactSource + "> (<" + destinationToken + ">)", destinationLockedBy: other, want: http.StatusLocked},
		{name: "destination token only negated", method: "COPY", source: eventSource, destination: eventDestination,
			ifHeader: "(Not <" + destinationToken + ">)", destinationLockedBy: owner, want: http.StatusLocked},
		{name: "contact destination token only negated", method: "COPY", source: contactSource, destination: contactDestination,
			ifHeader: "<" + contactDestination + "> (Not <" + destinationToken + ">)", destinationLockedBy: owner, want: http.StatusLocked},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			lockPath := func(href string) string { return strings.TrimSuffix(strings.TrimSuffix(href, ".ics"), ".vcf") }
			events := &fakeEventRepo{events: map[string]*store.Event{
				"5:alice": {CalendarID: 5, UID: "alice", ResourceName: "alice", RawICAL: buildCalendarObject(buildVEvent("alice")), ETag: "etag-a"},
				"6:bob":   {CalendarID: 6, UID: "bob", ResourceName: "renamed", RawICAL: buildCalendarObject(buildVEvent("bob")), ETag: "etag-b"},
			}}
			h := NewDavServer(Options{Store: &store.Store{
				Calendars: &fakeCalendarRepo{calendars: map[int64]*store.Calendar{
					5: {ID: 5, UserID: owner.ID, Name: "Source"},
					6: {ID: 6, UserID: owner.ID, Name: "Destination"},
				}},
				Events:            events,
				CalendarTransfers: events,
				AddressBooks: &fakeAddressBookRepo{books: map[int64]*store.AddressBook{
					7: {ID: 7, UserID: owner.ID, Name: "Source"},
					8: {ID: 8, UserID: owner.ID, Name: "Destination"},
				}},
				Contacts: &fakeContactRepo{contacts: map[string]*store.Contact{
					"7:carol": {AddressBookID: 7, UID: "carol", ResourceName: "carol", RawVCard: buildVCard("3.0", "UID:carol", "FN:Carol"), ETag: "etag-c"},
					"8:dave":  {AddressBookID: 8, UID: "dave", ResourceName: "renamed", RawVCard: buildVCard("3.0", "UID:dave", "FN:Dave"), ETag: "etag-d"},
				}},
				Locks: &fakeLockRepo{locks: map[string]*store.Lock{
					sourceToken: {
						Token: sourceToken, ResourcePath: lockPath(tt.source), UserID: owner.ID, LockScope: "exclusive", LockType: "write",
						Depth: "0", ExpiresAt: time.Now().Add(time.Hour),
					},
					destinationToken: {
						Token: destinationToken, ResourcePath: lockPath(tt.destination), UserID: tt.destinationLockedBy.ID, LockScope: "exclusive", LockType: "write",
						Depth: "0", ExpiresAt: time.Now().Add(time.Hour),
					},
				}},
				ACLEntries: &fakeACLRepo{},
			}})
			req := httptest.NewRequest(tt.method, tt.source, nil)
			req.Header.Set("Destination", tt.destination)
			req.Header.Set("Overwrite", "T")
			req.Header.Set("If", tt.ifHeader)
			rr := httptest.NewRecorder()
			h.ServeHTTP(rr, req.WithContext(auth.WithUser(req.Context(), owner)))
			if rr.Code != tt.want {
				t.Fatalf("%s status = %d, want %d: %s", tt.method, rr.Code, tt.want, rr.Body.String())
			}
		})
	}
}

// A write whose target cannot be resolved to the resource its locks are
// stored at fails rather than looking its locks up under the spelling the
// request used, which would find none.
func TestLockCheckFailsWhenTheTargetCannotBeResolved(t *testing.T) {
	owner := &store.User{ID: 1}
	h := &DavServer{store: &store.Store{
		Calendars: &fakeCalendarRepo{accessErr: errors.New("calendar store unavailable")},
		Locks:     &fakeLockRepo{locks: map[string]*store.Lock{}},
	}}
	const target = "/dav/calendars/Owner/event.ics"
	newRequest := func() *http.Request {
		req := httptest.NewRequest(http.MethodPut, target, nil)
		return req.WithContext(auth.WithUser(withDAVRequestState(req.Context()), owner))
	}
	if allowed, err := h.checkLock(newRequest(), target); err == nil {
		t.Fatalf("checkLock = %v with no error, want the resolution failure", allowed)
	}
	if allowed, err := h.checkLocks(newRequest(), target, "/dav/calendars/Owner"); err == nil {
		t.Fatalf("checkLocks = %v with no error, want the resolution failure", allowed)
	}
	if preconditions, err := h.lockPreconditions(newRequest(), target); err == nil {
		t.Fatalf("lockPreconditions = %#v with no error, want the resolution failure", preconditions)
	}
	rr := httptest.NewRecorder()
	if h.requireLock(rr, newRequest(), target, "resource is locked") || rr.Code != http.StatusInternalServerError {
		t.Fatalf("requireLock answered %d, want 500: %s", rr.Code, rr.Body.String())
	}
}
