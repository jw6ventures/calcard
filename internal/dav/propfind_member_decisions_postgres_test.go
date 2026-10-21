package dav

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"reflect"
	"slices"
	"strings"
	"testing"

	"github.com/jw6ventures/calcard/internal/acl"
	"github.com/jw6ventures/calcard/internal/auth"
	"github.com/jw6ventures/calcard/internal/store"
)

const memberDecisionPropfindBody = `<?xml version="1.0" encoding="utf-8"?>
<d:propfind xmlns:d="DAV:"><d:prop><d:getetag/><d:current-user-privilege-set/><d:acl/><d:owner/><d:lockdiscovery/></d:prop></d:propfind>`

const memberDecisionSharedLockBody = `<?xml version="1.0" encoding="utf-8"?>
<d:lockinfo xmlns:d="DAV:"><d:lockscope><d:shared/></d:lockscope><d:locktype><d:write/></d:locktype><d:owner>owner</d:owner></d:lockinfo>`

const memberDecisionSyncBody = `<?xml version="1.0" encoding="utf-8"?>
<d:sync-collection xmlns:d="DAV:"><d:sync-token/><d:sync-level>1</d:sync-level><d:prop><d:getetag/></d:prop></d:sync-collection>`

const memberDecisionCalendarQueryBody = `<?xml version="1.0" encoding="utf-8"?>
<c:calendar-query xmlns:d="DAV:" xmlns:c="urn:ietf:params:xml:ns:caldav"><d:prop><d:getetag/></d:prop><c:filter><c:comp-filter name="VCALENDAR"/></c:filter></c:calendar-query>`

// memberACL is an ACL stored at the state path of the member storedName,
// which need not be the member it is listed under.
type memberACL struct {
	storedName string
	entries    []store.ACLEntry
}

// A Depth: 1 PROPFIND and the listing REPORTs have to list exactly the members
// a request naming each one may read, the PROPFIND describing each listed
// member exactly as a Depth: 0 PROPFIND of that member does, whatever the
// member is named. Only the ACL stored at a member's own path decides for it:
// one stored under another spelling of its name is another resource's.
func TestPostgres_PropfindListsAndDescribesEachMemberAsItsOwnRequestDoes(t *testing.T) {
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
	grant := func(principal string, privileges ...string) []store.ACLEntry {
		entries := make([]store.ACLEntry, 0, len(privileges))
		for _, privilege := range privileges {
			entries = append(entries, store.ACLEntry{PrincipalHref: principal, IsGrant: true, Privilege: privilege})
		}
		return entries
	}
	deny := func(principal string, privileges ...string) []store.ACLEntry {
		entries := grant(principal, privileges...)
		for i := range entries {
			entries[i].IsGrant = false
		}
		return entries
	}
	positioned := func(aces ...[]store.ACLEntry) []store.ACLEntry {
		var entries []store.ACLEntry
		for position, ace := range aces {
			for _, entry := range ace {
				entry.Position = position
				entries = append(entries, entry)
			}
		}
		return entries
	}
	collectionACL := positioned(
		grant(shareeHref, "read", "read-acl", "read-current-user-privilege-set"),
		grant(acl.PrincipalAuthenticated, "read"),
	)

	// Each domain creates a collection and adds members to it by stored
	// resource name; ext is the extension a member's href carries.
	domains := []struct {
		name      string
		prefix    string
		ext       string
		create    func() int64
		addMember func(collectionID int64, resourceName string)
		reports   []string
	}{
		{
			name:   "calendar",
			prefix: calendarPrefix,
			ext:    ".ics",
			create: func() int64 {
				cal, err := database.Calendars.Create(ctx, store.Calendar{UserID: owner.ID, Name: "Decisions"})
				if err != nil {
					t.Fatal(err)
				}
				return cal.ID
			},
			addMember: func(collectionID int64, resourceName string) {
				uid := "uid-" + resourceName
				raw := buildCalendarObject(buildVEvent(uid, "SUMMARY:x"))
				if _, err := database.Events.Upsert(ctx, store.Event{CalendarID: collectionID, UID: uid, ResourceName: resourceName, RawICAL: raw, ETag: "etag-" + resourceName}); err != nil {
					t.Fatal(err)
				}
			},
			reports: []string{memberDecisionSyncBody, memberDecisionCalendarQueryBody},
		},
		{
			name:   "address book",
			prefix: addressBookPrefix,
			ext:    ".vcf",
			create: func() int64 {
				book, err := database.AddressBooks.Create(ctx, store.AddressBook{UserID: owner.ID, Name: "Decisions"})
				if err != nil {
					t.Fatal(err)
				}
				return book.ID
			},
			addMember: func(collectionID int64, resourceName string) {
				uid := "uid-" + resourceName
				if _, err := database.Contacts.Upsert(ctx, store.Contact{AddressBookID: collectionID, UID: uid, ResourceName: resourceName, RawVCard: buildVCard("3.0", "UID:"+uid, "FN:"+resourceName), ETag: "etag-" + resourceName}); err != nil {
					t.Fatal(err)
				}
			},
			reports: []string{memberDecisionSyncBody},
		},
	}

	for _, domain := range domains {
		t.Run(domain.name, func(t *testing.T) {
			upper := strings.ToUpper(domain.ext)
			// Each member carries the ACLs stored for it and whether the sharee,
			// who may read the collection, may read the member.
			members := map[string]struct {
				acls        []memberACL
				shareeReads bool
			}{
				"plain": {nil, true},
				"member-grant": {[]memberACL{{storedName: "member-grant", entries: positioned(
					grant(strangerHref, "read", "read-acl", "read-current-user-privilege-set"))}}, true},
				"member-deny":        {[]memberACL{{storedName: "member-deny", entries: positioned(deny(shareeHref, "read"))}}, false},
				"deny-all":           {[]memberACL{{storedName: "deny-all", entries: positioned(deny(shareeHref, "all"))}}, false},
				"authenticated-deny": {[]memberACL{{storedName: "authenticated-deny", entries: positioned(deny(acl.PrincipalAuthenticated, "read"))}}, false},
				"grant-then-deny": {[]memberACL{{storedName: "grant-then-deny", entries: positioned(
					grant(shareeHref, "read"), deny(shareeHref, "read"))}}, true},
				"deny-then-grant": {[]memberACL{{storedName: "deny-then-grant", entries: positioned(
					deny(acl.PrincipalAll, "read"), grant(shareeHref, "read"))}}, false},
				"stored-with-ext": {[]memberACL{{storedName: "stored-with-ext" + domain.ext, entries: positioned(deny(shareeHref, "read"))}}, true},
				// Members named with the extension a request strips, in either
				// case, with a denial stored at their own path, which the ACL
				// method writes, or at another spelling of their name.
				"named" + domain.ext:     {[]memberACL{{storedName: "named", entries: positioned(deny(shareeHref, "read"))}}, true},
				"legacy" + domain.ext:    {[]memberACL{{storedName: "legacy" + domain.ext, entries: positioned(deny(shareeHref, "read"))}}, false},
				"Shout" + upper:          {[]memberACL{{storedName: "Shout" + domain.ext, entries: positioned(deny(shareeHref, "read"))}}, true},
				"Quiet" + upper:          {[]memberACL{{storedName: "Quiet" + upper, entries: positioned(deny(shareeHref, "read"))}}, false},
				"uid@example.com":        {[]memberACL{{storedName: "uid@example.com", entries: positioned(grant(strangerHref, "read-acl"), deny(shareeHref, "read"))}}, false},
				"dotted":                 {[]memberACL{{storedName: "dotted", entries: positioned(deny(shareeHref, "read"), deny(acl.PrincipalAuthenticated, "read"))}}, false},
				"dotted.name":            {nil, true},
				"all-grant":              {[]memberACL{{storedName: "all-grant", entries: positioned(grant(acl.PrincipalAll, "read", "read-current-user-privilege-set"))}}, true},
				"write-only":             {[]memberACL{{storedName: "write-only", entries: positioned(deny(strangerHref, "read"), grant(strangerHref, "write-content"))}}, true},
				"two-spellings":          {[]memberACL{{storedName: "two-spellings", entries: positioned(grant(strangerHref, "read-acl"))}, {storedName: "two-spellings" + domain.ext, entries: positioned(deny(shareeHref, "read"))}}, true},
				"owner-property-deny":    {[]memberACL{{storedName: "owner-property-deny", entries: positioned(deny(acl.PrincipalPropertyOwner, "read-acl"), grant(shareeHref, "read-acl"))}}, true},
				"unauthenticated-denied": {[]memberACL{{storedName: "unauthenticated-denied", entries: positioned(deny(acl.PrincipalUnauthenticated, "read"))}}, true},
				// A name with no stem before its dot, and an ACL stored at the
				// name with the extension added, another resource's.
				".hidden": {[]memberACL{{storedName: ".hidden" + domain.ext, entries: positioned(deny(shareeHref, "read-acl"))}}, true},
			}

			collectionID := domain.create()
			collectionPath := collectionResourcePath(domain.prefix, collectionID)
			if err := database.ACLEntries.SetACL(ctx, collectionPath, collectionACL); err != nil {
				t.Fatal(err)
			}
			for name, member := range members {
				domain.addMember(collectionID, name)
				for _, stored := range member.acls {
					if err := database.ACLEntries.SetACL(ctx, collectionPath+"/"+stored.storedName, stored.entries); err != nil {
						t.Fatalf("store ACL of %q at %q: %v", name, stored.storedName, err)
					}
				}
			}

			h := NewDavServer(Options{Store: database})
			do := func(user *store.User, method, target, depth, body string) *httptest.ResponseRecorder {
				t.Helper()
				req := httptest.NewRequest(method, target, strings.NewReader(body))
				if depth != "" {
					req.Header.Set("Depth", depth)
				}
				req = req.WithContext(auth.WithUser(req.Context(), user))
				rr := httptest.NewRecorder()
				h.ServeHTTP(rr, req)
				return rr
			}
			collectionHref := collectionPath + "/"
			memberHref := func(name string) string { return collectionHref + url.PathEscape(name) + domain.ext }

			// Every member is under a lock taken on the collection, and some
			// also under one taken on the member itself.
			if lock := do(owner, "LOCK", collectionHref, "infinity", memberDecisionSharedLockBody); lock.Code != http.StatusOK {
				t.Fatalf("LOCK %s = %d: %s", collectionHref, lock.Code, lock.Body.String())
			}
			lockedDirectly := []string{"plain", "dotted.name", "all-grant"}
			for _, name := range lockedDirectly {
				if lock := do(owner, "LOCK", memberHref(name), "0", memberDecisionSharedLockBody); lock.Code != http.StatusOK {
					t.Fatalf("LOCK %s = %d: %s", memberHref(name), lock.Code, lock.Body.String())
				}
			}

			for _, user := range []*store.User{owner, sharee, stranger} {
				listing := decodeMultistatus(t, do(user, "PROPFIND", collectionHref, "1", memberDecisionPropfindBody))
				listed := make(map[string]davResponse)
				for _, resp := range listing.Responses {
					href := canonicalHref(resp.Hrefs[0])
					if href == canonicalHref(collectionHref) {
						continue
					}
					listed[href] = resp
				}
				reported := make([]map[string]bool, len(domain.reports))
				for i, body := range domain.reports {
					reported[i] = make(map[string]bool)
					for _, resp := range decodeMultistatus(t, do(user, "REPORT", collectionHref, "1", body)).Responses {
						if _, ok := grantedPropertyOf(resp, "getetag"); ok {
							reported[i][canonicalHref(resp.Hrefs[0])] = true
						}
					}
				}
				for name := range members {
					href := memberHref(name)
					get := do(user, http.MethodGet, href, "", "")
					readable := get.Code == http.StatusOK
					if get.Code != http.StatusOK && get.Code != http.StatusNotFound && get.Code != http.StatusForbidden {
						t.Fatalf("user %d GET %s = %d: %s", user.ID, href, get.Code, get.Body.String())
					}
					if user == sharee && readable != members[name].shareeReads {
						t.Errorf("sharee GET of member %q = %d, want readable %v", name, get.Code, members[name].shareeReads)
					}
					for i := range domain.reports {
						if reported[i][canonicalHref(href)] != readable {
							t.Errorf("user %d: member %q reported = %v by %.60q, but GET answers %d", user.ID, name, reported[i][canonicalHref(href)], domain.reports[i], get.Code)
						}
					}
					fromListing, isListed := listed[canonicalHref(href)]
					if isListed != readable {
						t.Errorf("user %d: member %q listed = %v, but GET answers %d", user.ID, name, isListed, get.Code)
						continue
					}
					single := do(user, "PROPFIND", href, "0", memberDecisionPropfindBody)
					if !readable {
						if single.Code == http.StatusMultiStatus {
							ms := decodeMultistatus(t, single)
							for _, resp := range ms.Responses {
								for _, ps := range resp.Propstats {
									if strings.Contains(ps.Status, " 200 ") {
										t.Errorf("user %d: Depth 0 PROPFIND of unreadable member %q answers 200 properties: %s", user.ID, name, single.Body.String())
									}
								}
							}
						}
						continue
					}
					ms := decodeMultistatus(t, single)
					if len(ms.Responses) != 1 {
						t.Fatalf("user %d: Depth 0 PROPFIND of %q gave %d responses", user.ID, name, len(ms.Responses))
					}
					if privileges, ok := grantedPropertyOf(fromListing, "current-user-privilege-set"); ok && !strings.Contains(describeElement(privileges), "privilege{read}") {
						t.Errorf("user %d: member %q is readable, but its listed privilege set is %s", user.ID, name, describeElement(privileges))
					}
					wantLocks := 1
					if slices.Contains(lockedDirectly, name) {
						wantLocks = 2
					}
					if locks, _ := grantedPropertyOf(fromListing, "lockdiscovery"); len(locks.Children) != wantLocks {
						t.Errorf("user %d: member %q is listed under %d locks, want %d", user.ID, name, len(locks.Children), wantLocks)
					}
					if !reflect.DeepEqual(ms.Responses[0].Propstats, fromListing.Propstats) {
						t.Errorf("user %d: member %q is described differently\nDepth 1: %s\nDepth 0: %s", user.ID, name,
							describePropstats(fromListing.Propstats), describePropstats(ms.Responses[0].Propstats))
					}
				}
				for href := range listed {
					found := false
					for name := range members {
						if canonicalHref(memberHref(name)) == href {
							found = true
						}
					}
					if !found {
						t.Errorf("user %d: listing names %q, which is no member", user.ID, href)
					}
				}
			}
		})
	}
}

// grantedPropertyOf returns the DAV property local answered with 200 OK.
func grantedPropertyOf(resp davResponse, local string) (davElement, bool) {
	for _, ps := range resp.Propstats {
		if !strings.Contains(ps.Status, " 200 ") {
			continue
		}
		for _, p := range ps.Prop.Props {
			if p.Name == davQN(local) {
				return p, true
			}
		}
	}
	return davElement{}, false
}

func describePropstats(propstats []davPropstat) string {
	var b strings.Builder
	for _, ps := range propstats {
		fmt.Fprintf(&b, "[%s:", ps.Status)
		for _, p := range ps.Prop.Props {
			fmt.Fprintf(&b, " %s", describeElement(p))
		}
		b.WriteString("]")
	}
	return b.String()
}

func describeElement(e davElement) string {
	if len(e.Children) == 0 {
		if text := strings.TrimSpace(e.Text); text != "" {
			return e.Name.Local + "=" + text
		}
		return e.Name.Local
	}
	parts := make([]string, 0, len(e.Children))
	for _, child := range e.Children {
		parts = append(parts, describeElement(child))
	}
	return e.Name.Local + "{" + strings.Join(parts, " ") + "}"
}

// A Depth: 1 PROPFIND reads storage a fixed number of times however many
// members it lists, for the owner and for a sharee, whatever properties it
// asks for and however its members are named.
func TestPostgres_PropfindDepthOneQueriesDoNotGrowWithMembers(t *testing.T) {
	database, queries := newDAVPostgresStoreCountingQueries(t)
	ctx := t.Context()
	owner, err := database.Users.UpsertOAuthUser(ctx, "owner", "owner@example.test", "Owner", "Owner")
	if err != nil {
		t.Fatal(err)
	}
	sharee, err := database.Users.UpsertOAuthUser(ctx, "sharee", "sharee@example.test", "Sharee", "Sharee")
	if err != nil {
		t.Fatal(err)
	}
	cal, err := database.Calendars.Create(ctx, store.Calendar{UserID: owner.ID, Name: "Many"})
	if err != nil {
		t.Fatal(err)
	}
	book, err := database.AddressBooks.Create(ctx, store.AddressBook{UserID: owner.ID, Name: "Many"})
	if err != nil {
		t.Fatal(err)
	}
	calendarPath := calendarCollectionResourcePath(cal.ID)
	bookPath := addressBookCollectionResourcePath(book.ID)
	shareeGrant := []store.ACLEntry{
		{PrincipalHref: acl.PrincipalHref(sharee.ID), IsGrant: true, Privilege: "read"},
		{PrincipalHref: acl.PrincipalHref(sharee.ID), IsGrant: true, Privilege: "read-acl"},
		{PrincipalHref: acl.PrincipalHref(sharee.ID), IsGrant: true, Privilege: "read-current-user-privilege-set"},
	}
	for _, collectionPath := range []string{calendarPath, bookPath} {
		if err := database.ACLEntries.SetACL(ctx, collectionPath, shareeGrant); err != nil {
			t.Fatal(err)
		}
	}

	members := 0
	addMembers := func(upTo int) {
		t.Helper()
		for ; members < upTo; members++ {
			// Members are named as DAV clients name them, as UI-created objects
			// are (with the extension), and with a dot of their own; every fifth
			// carries its own ACL.
			name := fmt.Sprintf("member-%03d", members)
			switch members % 3 {
			case 1:
				name += ".ics"
			case 2:
				name += "@example.com"
			}
			raw := buildCalendarObject(buildVEvent("uid-"+name, "SUMMARY:x"))
			if _, err := database.Events.Upsert(ctx, store.Event{CalendarID: cal.ID, UID: "uid-" + name, ResourceName: name, RawICAL: raw, ETag: "e-" + name}); err != nil {
				t.Fatal(err)
			}
			if _, err := database.Contacts.Upsert(ctx, store.Contact{AddressBookID: book.ID, UID: "uid-" + name, ResourceName: name, RawVCard: buildVCard("3.0", "UID:uid-"+name, "FN:"+name), ETag: "e-" + name}); err != nil {
				t.Fatal(err)
			}
			if members%5 == 0 {
				memberACL := []store.ACLEntry{{PrincipalHref: acl.PrincipalHref(sharee.ID), IsGrant: true, Privilege: "write-content"}}
				for _, collectionPath := range []string{calendarPath, bookPath} {
					if err := database.ACLEntries.SetACL(ctx, collectionPath+"/"+name, memberACL); err != nil {
						t.Fatal(err)
					}
				}
			}
		}
	}

	h := NewDavServer(Options{Store: database})
	const privilegesBody = `<?xml version="1.0"?><d:propfind xmlns:d="DAV:"><d:prop><d:getetag/><d:current-user-privilege-set/><d:acl/><d:owner/><d:lockdiscovery/></d:prop></d:propfind>`
	queriesFor := func(user *store.User, collectionPath, body string) int {
		t.Helper()
		req := httptest.NewRequest("PROPFIND", collectionPath+"/", strings.NewReader(body))
		if body == "" {
			req.Body = http.NoBody
			req.ContentLength = 0
		}
		req.Header.Set("Depth", "1")
		req = req.WithContext(auth.WithUser(req.Context(), user))
		rr := httptest.NewRecorder()
		queries.reset()
		h.ServeHTTP(rr, req)
		if rr.Code != http.StatusMultiStatus {
			t.Fatalf("PROPFIND %s = %d: %s", collectionPath, rr.Code, rr.Body.String())
		}
		if listed := strings.Count(rr.Body.String(), "<d:response>") - 1; listed != members {
			t.Fatalf("PROPFIND %s listed %d members, want %d", collectionPath, listed, members)
		}
		return queries.count()
	}

	type request struct {
		user           *store.User
		collectionPath string
		body           string
	}
	var requests []request
	for _, user := range []*store.User{owner, sharee} {
		for _, collectionPath := range []string{calendarPath, bookPath} {
			for _, body := range []string{"", privilegesBody} {
				requests = append(requests, request{user, collectionPath, body})
			}
		}
	}
	addMembers(4)
	few := make([]int, len(requests))
	for i, r := range requests {
		few[i] = queriesFor(r.user, r.collectionPath, r.body)
	}
	addMembers(60)
	for i, r := range requests {
		if many := queriesFor(r.user, r.collectionPath, r.body); many != few[i] {
			t.Errorf("user %d PROPFIND of %s (allprop %v): %d queries for 4 members, %d for 60:\n%s",
				r.user.ID, r.collectionPath, r.body == "", few[i], many, queries.summary())
		}
	}
}
