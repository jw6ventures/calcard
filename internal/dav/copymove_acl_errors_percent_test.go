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

// COPY and MOVE read the locks on their source and destination in one batch
// keyed by path, and their own lock checks answer from it. A LOCK on an object
// whose name holds "%3B" has to be found there under that object's path; keyed
// under the decoded spelling, the check reports the object unlocked and leaves
// the refusal to the write transaction.
func TestCopyMoveLockBatchKeysANameContainingPercentByItsPath(t *testing.T) {
	owner := &store.User{ID: 1, PrimaryEmail: "owner@example.com"}
	h, _ := percentNameServer(nil)
	h.store.Locks = &fakeLockRepo{locks: map[string]*store.Lock{
		"opaquelocktoken:held": {
			Token: "opaquelocktoken:held", ResourcePath: "/dav/calendars/2/a%3Bb", UserID: owner.ID,
			LockScope: "exclusive", LockType: "write", Depth: "0", ExpiresAt: time.Now().Add(time.Hour),
		},
	}}
	req := httptest.NewRequest("MOVE", calendarObjectHref("/dav/calendars/2/", "a%3Bb"), nil)
	req = req.WithContext(auth.WithUser(req.Context(), owner))
	req = h.prefetchCopyMoveLocks(req, "/dav/calendars/2/a%3Bb.ics", "/dav/calendars/2/moved.ics")
	if lockBatchIndexFromContext(req.Context()) == nil {
		t.Fatal("prefetchCopyMoveLocks installed no lock batch")
	}
	for _, tt := range []struct {
		path    string
		allowed bool
	}{
		{path: "/dav/calendars/2/a%3Bb.ics", allowed: false},
		{path: "/dav/calendars/2/a;b.ics", allowed: true},
	} {
		allowed, err := h.checkLock(req, tt.path)
		if err != nil {
			t.Fatalf("checkLock(%s): %v", tt.path, err)
		}
		if allowed != tt.allowed {
			t.Errorf("checkLock(%s) = %v, want %v", tt.path, allowed, tt.allowed)
		}
	}
}

// DAV:need-privileges names the resource the privilege is missing on by its
// href, so a name holding "%" or a space is written escaped, the way the
// server lists it.
func TestNeedPrivilegesHrefForANameContainingPercentAndSpace(t *testing.T) {
	delegate := &store.User{ID: 2, PrimaryEmail: "delegate@example.com"}
	h, _ := percentNameServer(nil)
	target := calendarObjectHref("/dav/calendars/2/", "a%3Bb c")

	rr := serveAsUser(t, h, delegate, "PROPPATCH", target, `<D:propertyupdate xmlns:D="DAV:" xmlns:X="urn:test"><D:set><D:prop><X:note>x</X:note></D:prop></D:set></D:propertyupdate>`, "")
	if rr.Code != http.StatusForbidden {
		t.Fatalf("PROPPATCH by a reader = %d, want 403: %s", rr.Code, rr.Body.String())
	}
	if want := "<D:href>" + target + "</D:href>"; !strings.Contains(rr.Body.String(), want) {
		t.Fatalf("need-privileges does not name %s: %s", target, rr.Body.String())
	}
}
