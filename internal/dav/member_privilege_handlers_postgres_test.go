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

// Each handler that decides a privilege on a member decides it on that member,
// whatever its name: a deny on uid@example.com refuses a request naming it
// even where the collection grants the privilege, and no request reads it as
// naming uid@example, which the deny does not cover.
func TestPostgres_MemberPrivilegeDeniesHoldInEveryHandler(t *testing.T) {
	const memberName = "uid@example.com"
	database := newDAVPostgresStore(t)
	ctx := t.Context()
	owner, err := database.Users.UpsertOAuthUser(ctx, "owner", "owner@example.test", "Owner", "Owner")
	if err != nil {
		t.Fatal(err)
	}
	sharee, err := database.Users.UpsertOAuthUser(ctx, "sharee", "sharee@example.test", "Sharee", "Sharee")
	if err != nil {
		t.Fatal(err)
	}
	shareeHref := acl.PrincipalHref(sharee.ID)
	privileges := []string{"read", "read-acl", "write-acl", "write-properties", "unlock"}
	collectionGrant := make([]store.ACLEntry, 0, len(privileges))
	for i, privilege := range privileges {
		collectionGrant = append(collectionGrant, store.ACLEntry{PrincipalHref: shareeHref, IsGrant: true, Privilege: privilege, Position: i})
	}
	cal, err := database.Calendars.Create(ctx, store.Calendar{UserID: owner.ID, Name: "Shared"})
	if err != nil {
		t.Fatal(err)
	}
	book, err := database.AddressBooks.Create(ctx, store.AddressBook{UserID: owner.ID, Name: "Shared"})
	if err != nil {
		t.Fatal(err)
	}
	calendarPath, bookPath := calendarCollectionResourcePath(cal.ID), addressBookCollectionResourcePath(book.ID)
	for _, name := range []string{memberName, "uid@example"} {
		if _, err := database.Events.Upsert(ctx, store.Event{CalendarID: cal.ID, UID: "uid-" + name, ResourceName: name, RawICAL: buildCalendarObject(buildVEvent("uid-"+name, "SUMMARY:x")), ETag: "e-" + name}); err != nil {
			t.Fatal(err)
		}
		if _, err := database.Contacts.Upsert(ctx, store.Contact{AddressBookID: book.ID, UID: "uid-" + name, ResourceName: name, RawVCard: buildVCard("3.0", "UID:uid-"+name, "FN:x"), ETag: "e-" + name}); err != nil {
			t.Fatal(err)
		}
	}
	h := NewDavServer(Options{Store: database})
	do := func(user *store.User, method, target, body string, header map[string]string) *httptest.ResponseRecorder {
		req := httptest.NewRequest(method, target, strings.NewReader(body))
		for key, value := range header {
			req.Header.Set(key, value)
		}
		req = req.WithContext(auth.WithUser(req.Context(), user))
		rr := httptest.NewRecorder()
		h.ServeHTTP(rr, req)
		return rr
	}
	calendarMember := calendarPath + "/" + memberName + ".ics"
	bookMember := bookPath + "/" + memberName + ".vcf"
	const proppatch = `<?xml version="1.0" encoding="utf-8"?><d:propertyupdate xmlns:d="DAV:" xmlns:x="urn:example"><d:set><d:prop><x:note>mine</x:note></d:prop></d:set></d:propertyupdate>`
	cases := []struct {
		name      string
		privilege string
		memberACL string
		request   func() *httptest.ResponseRecorder
	}{
		{"ACL", "write-acl", calendarPath + "/" + memberName, func() *httptest.ResponseRecorder {
			body := `<?xml version="1.0" encoding="utf-8"?><d:acl xmlns:d="DAV:"><d:ace><d:principal><d:href>` + shareeHref + `</d:href></d:principal><d:grant><d:privilege><d:all/></d:privilege></d:grant></d:ace></d:acl>`
			return do(sharee, "ACL", calendarMember, body, nil)
		}},
		{"acl-principal-prop-set", "read-acl", calendarPath + "/" + memberName, func() *httptest.ResponseRecorder {
			body := `<?xml version="1.0" encoding="utf-8"?><d:acl-principal-prop-set xmlns:d="DAV:"><d:prop><d:displayname/></d:prop></d:acl-principal-prop-set>`
			return do(sharee, "REPORT", calendarMember, body, map[string]string{"Depth": "0"})
		}},
		{"UNLOCK", "unlock", calendarPath + "/" + memberName, func() *httptest.ResponseRecorder {
			lock := do(owner, "LOCK", calendarMember, memberDecisionSharedLockBody, map[string]string{"Depth": "0"})
			if lock.Code != http.StatusOK {
				t.Fatalf("owner LOCK = %d: %s", lock.Code, lock.Body.String())
			}
			return do(sharee, "UNLOCK", calendarMember, "", map[string]string{"Lock-Token": lock.Header().Get("Lock-Token")})
		}},
		{"calendar PROPPATCH", "write-properties", calendarPath + "/" + memberName, func() *httptest.ResponseRecorder {
			return do(sharee, "PROPPATCH", calendarMember, proppatch, nil)
		}},
		{"address book PROPPATCH", "write-properties", bookPath + "/" + memberName, func() *httptest.ResponseRecorder {
			return do(sharee, "PROPPATCH", bookMember, proppatch, nil)
		}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			for _, collectionPath := range []string{calendarPath, bookPath} {
				if err := database.ACLEntries.SetACL(ctx, collectionPath, collectionGrant); err != nil {
					t.Fatal(err)
				}
			}
			memberDeny := []store.ACLEntry{
				{PrincipalHref: shareeHref, IsGrant: false, Privilege: tc.privilege},
				{PrincipalHref: shareeHref, IsGrant: true, Privilege: "read", Position: 1},
			}
			if err := database.ACLEntries.SetACL(ctx, tc.memberACL, memberDeny); err != nil {
				t.Fatal(err)
			}
			rr := tc.request()
			if rr.Code != http.StatusForbidden {
				t.Errorf("%s by a principal denied %s on %s = %d, want 403: %s", tc.name, tc.privilege, memberName, rr.Code, rr.Body.String())
			}
			stored, err := database.ACLEntries.ListByResource(ctx, tc.memberACL)
			if err != nil {
				t.Fatal(err)
			}
			if len(stored) != len(memberDeny) || stored[0].IsGrant {
				t.Errorf("%s changed the member's ACL to %#v", tc.name, stored)
			}
		})
	}
}
