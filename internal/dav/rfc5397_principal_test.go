package dav

// RFC 5397: WebDAV Current Principal Extension.
//
// CalDAV client discovery starts here (RFC 4791 §6.1 assumes it), but the
// DAV:current-user-principal property itself is defined by RFC 5397, not by
// RFC 4791.

import (
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/jw6ventures/calcard/internal/auth"
	"github.com/jw6ventures/calcard/internal/store"
)

// RFC 5397 §3: a server supporting the property MUST report the principal URL
// of the currently authenticated principal.
func TestRFC5397_CurrentUserPrincipalIdentifiesAuthenticatedPrincipal(t *testing.T) {
	h := &DavServer{}
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

	// §3 gives the property one DAV:href, so the assertion is over its exact
	// children: a second href, or a sibling that is not an href, names an
	// ambiguous principal rather than the authenticated one.
	decodeMultistatus(t, rr).
		responseForHref(t, "/dav/").
		assertPropHrefs(t, davQN("current-user-principal"), "/dav/principals/1/")
}
