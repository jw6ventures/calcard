package dav

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/jw6ventures/calcard/internal/acl"
	"github.com/jw6ventures/calcard/internal/auth"
	"github.com/jw6ventures/calcard/internal/store"
)

// A member's ACL is read from several spellings of its path, and decided from
// what they hold rather than from the order rows were saved: the entries at
// the member's own path come first. The ACL method on a member replaces every
// spelling its decisions read, so the ACL it leaves is the ACL that decides.
func TestPostgres_MemberACLDecisionsDependOnContentNotSaveHistory(t *testing.T) {
	database := newDAVPostgresStore(t)
	ctx := t.Context()
	newUser := func(name string) *store.User {
		t.Helper()
		user, err := database.Users.UpsertOAuthUser(ctx, name, name+"@example.test", name, name)
		if err != nil {
			t.Fatal(err)
		}
		return user
	}
	owner, sharee, stranger := newUser("owner"), newUser("sharee"), newUser("stranger")
	shareeHref, strangerHref := acl.PrincipalHref(sharee.ID), acl.PrincipalHref(stranger.ID)
	cal, err := database.Calendars.Create(ctx, store.Calendar{UserID: owner.ID, Name: "Spellings"})
	if err != nil {
		t.Fatal(err)
	}
	collectionPath := calendarCollectionResourcePath(cal.ID)
	if err := database.ACLEntries.SetACL(ctx, collectionPath, []store.ACLEntry{{PrincipalHref: shareeHref, IsGrant: true, Privilege: "read"}}); err != nil {
		t.Fatal(err)
	}
	h := NewDavServer(Options{Store: database})
	do := func(user *store.User, method, target, body string) *httptest.ResponseRecorder {
		t.Helper()
		req := httptest.NewRequest(method, target, strings.NewReader(body))
		if method == http.MethodPut {
			req.Header.Set("Content-Type", "text/calendar")
		}
		if method == "PROPFIND" {
			req.Header.Set("Depth", "0")
		}
		req = req.WithContext(auth.WithUser(req.Context(), user))
		rr := httptest.NewRecorder()
		h.ServeHTTP(rr, req)
		return rr
	}
	href := func(name string) string { return collectionPath + "/" + name + ".ics" }
	for _, name := range []string{"X.ICS", "X.ics"} {
		if put := do(owner, http.MethodPut, href(name), buildCalendarObject(buildVEvent("uid-"+name, "SUMMARY:x"))); put.Code != http.StatusCreated {
			t.Fatalf("PUT %s = %d: %s", name, put.Code, put.Body.String())
		}
	}
	ace := func(principal string, grant bool, privileges ...string) string {
		element := "deny"
		if grant {
			element = "grant"
		}
		var b strings.Builder
		b.WriteString(`<d:ace><d:principal><d:href>` + principal + `</d:href></d:principal><d:` + element + `>`)
		for _, privilege := range privileges {
			b.WriteString(`<d:privilege><d:` + privilege + `/></d:privilege>`)
		}
		b.WriteString(`</d:` + element + `></d:ace>`)
		return b.String()
	}
	setACL := func(user *store.User, name string, aces ...string) int {
		t.Helper()
		return do(user, "ACL", href(name), `<?xml version="1.0" encoding="utf-8"?><d:acl xmlns:d="DAV:">`+strings.Join(aces, "")+`</d:acl>`).Code
	}
	strangerReads := func() bool {
		t.Helper()
		return do(stranger, http.MethodGet, href("X.ICS"), "").Code == http.StatusOK
	}

	// X.ICS denies the stranger in its second ACE; the sibling spelling X.ics,
	// which X.ICS's decisions also read, grants it in its first, saved later.
	if code := setACL(owner, "X.ICS", ace(shareeHref, true, "read-acl"), ace(strangerHref, false, "read")); code != http.StatusOK {
		t.Fatalf("ACL X.ICS = %d", code)
	}
	if err := database.ACLEntries.SetACL(ctx, collectionPath+"/X.ics", []store.ACLEntry{{PrincipalHref: strangerHref, IsGrant: true, Privilege: "read"}}); err != nil {
		t.Fatal(err)
	}
	if strangerReads() {
		t.Error("a grant at a sibling spelling overrides the deny at X.ICS's own path")
	}
	listing := do(owner, "PROPFIND", href("X.ICS"), `<?xml version="1.0" encoding="utf-8"?><d:propfind xmlns:d="DAV:"><d:prop><d:acl/></d:prop></d:propfind>`).Body.String()
	if deny, grant := strings.Index(listing, "<d:deny>"), strings.LastIndex(listing, strangerHref+"</d:href></d:principal><d:grant>"); deny < 0 || grant < 0 || deny > grant {
		t.Errorf("DAV:acl does not list X.ICS's own deny ahead of the sibling spelling's grant: %s", listing)
	}

	// Saving the same ACL again, on either member, decides nothing new.
	if code := setACL(owner, "X.ics", ace(strangerHref, true, "read")); code != http.StatusOK {
		t.Fatalf("ACL X.ics = %d", code)
	}
	if strangerReads() {
		t.Error("re-saving the sibling's ACL lets the stranger read X.ICS")
	}
	if code := setACL(owner, "X.ICS", ace(shareeHref, true, "read-acl"), ace(strangerHref, false, "read")); code != http.StatusOK {
		t.Fatalf("ACL X.ICS = %d", code)
	}
	if strangerReads() {
		t.Error("re-saving X.ICS's ACL lets the stranger read it")
	}

	// Clearing X.ICS's ACL leaves nothing its decisions read.
	if code := setACL(owner, "X.ICS"); code != http.StatusOK {
		t.Fatalf("ACL X.ICS = %d", code)
	}
	for _, spelling := range []string{"X.ICS", "X", "X.ics"} {
		if entries, err := database.ACLEntries.ListByResource(ctx, collectionPath+"/"+spelling); err != nil || len(entries) != 0 {
			t.Errorf("after X.ICS's ACL was cleared, %s holds %#v, %v", spelling, entries, err)
		}
	}
	if strangerReads() {
		t.Error("the stranger reads X.ICS after its ACL was cleared")
	}

	// A write-acl grant on the sibling does not survive a deny on X.ICS.
	if code := setACL(owner, "X.ics", ace(shareeHref, true, "read", "write-acl")); code != http.StatusOK {
		t.Fatalf("ACL X.ics = %d", code)
	}
	if code := setACL(owner, "X.ICS", ace(shareeHref, false, "write-acl")); code != http.StatusOK {
		t.Fatalf("ACL X.ICS = %d", code)
	}
	if code := setACL(sharee, "X.ICS", ace(shareeHref, true, "all")); code != http.StatusForbidden {
		t.Errorf("ACL on X.ICS by the sharee denied write-acl there = %d, want 403", code)
	}
}
