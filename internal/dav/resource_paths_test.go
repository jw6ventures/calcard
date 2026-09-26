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

func serveAs(t *testing.T, h *DavServer, method, target, body string, depth string) *httptest.ResponseRecorder {
	t.Helper()
	var reader *strings.Reader
	if body != "" {
		reader = strings.NewReader(body)
	}
	var req *http.Request
	if reader != nil {
		req = httptest.NewRequest(method, target, reader)
	} else {
		req = httptest.NewRequest(method, target, nil)
	}
	if depth != "" {
		req.Header.Set("Depth", depth)
	}
	req = req.WithContext(auth.WithUser(req.Context(), &store.User{ID: 1, PrimaryEmail: "owner@example.com"}))
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, req)
	return rr
}

// Go decodes the Request-URI once into URL.Path. A resource name that itself
// contains "%" is sent as "%25" and arrives as "%", so decoding the path again
// names another resource, or none. Every href the server lists has to fetch
// the resource it was listed for.
func TestResourceNamesContainingPercentRoundTrip(t *testing.T) {
	const ical = "BEGIN:VCALENDAR\r\nVERSION:2.0\r\nBEGIN:VEVENT\r\nUID:%s\r\nDTSTAMP:20240101T000000Z\r\nDTSTART:20240101T100000Z\r\nEND:VEVENT\r\nEND:VCALENDAR\r\n"
	names := []string{"100% done", "a%3Bb", "plain name", "50%"}
	events := map[string]*store.Event{}
	for i, name := range names {
		events["1:"+name] = &store.Event{
			ID: int64(i + 1), CalendarID: 1, UID: "uid-" + name, ResourceName: name,
			ETag: "e" + name, RawICAL: strings.Replace(ical, "%s", "uid-"+name, 1),
			LastModified: time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC),
		}
	}
	h := NewDavServer(Options{Store: &store.Store{
		Calendars: &fakeCalendarRepo{calendars: map[int64]*store.Calendar{1: {ID: 1, UserID: 1, Name: "Work"}}},
		Events:    &fakeEventRepo{events: events},
	}})

	for _, name := range names {
		href := calendarObjectHref("/dav/calendars/1/", name)
		t.Run(name, func(t *testing.T) {
			get := serveAs(t, h, http.MethodGet, href, "", "")
			if get.Code != http.StatusOK {
				t.Fatalf("GET %s status = %d: %s", href, get.Code, get.Body.String())
			}
			if !strings.Contains(get.Body.String(), "UID:uid-"+name) {
				t.Fatalf("GET %s returned another resource:\n%s", href, get.Body.String())
			}

			find := serveAs(t, h, "PROPFIND", href, `<D:propfind xmlns:D="DAV:"><D:prop><D:getetag/></D:prop></D:propfind>`, "0")
			if find.Code != http.StatusMultiStatus {
				t.Fatalf("PROPFIND %s status = %d: %s", href, find.Code, find.Body.String())
			}
			ms := decodeMultistatus(t, find)
			ms.assertHrefs(t, href)
			ms.responseForHref(t, href).assertPropValue(t, davQN("getetag"), http.StatusOK, `"e`+name+`"`)
		})
	}

	listing := serveAs(t, h, "PROPFIND", "/dav/calendars/1/", `<D:propfind xmlns:D="DAV:"><D:prop><D:getetag/></D:prop></D:propfind>`, "1")
	if listing.Code != http.StatusMultiStatus {
		t.Fatalf("PROPFIND collection status = %d: %s", listing.Code, listing.Body.String())
	}
	ms := decodeMultistatus(t, listing)
	for _, name := range names {
		ms.responseForHref(t, calendarObjectHref("/dav/calendars/1/", name))
	}
}

// Birthday resources are named from percent-encoded contact UIDs, so their
// hrefs carry "%25" and must fetch the resource they list.
func TestBirthdayResourceWithEncodedUIDIsFetchedByItsHref(t *testing.T) {
	for _, uid := range []string{"a;b,c", "50%off", "tab\tbed", `back\slash`} {
		t.Run(uid, func(t *testing.T) {
			created := time.Date(2026, 9, 1, 10, 0, 0, 0, time.UTC)
			contact := birthdayContact(uid, time.Date(1990, 5, 15, 0, 0, 0, 0, time.UTC), created)
			book := store.AddressBook{ID: 5, UserID: 1, Name: "Contacts", CTag: 4, UpdatedAt: created}
			h := birthdaySyncServer(book, map[string]*store.Contact{"5:" + uid: contact})

			href := calendarObjectHref(birthdayCalendarHref(), birthdayEventUID(5, uid))
			get := serveAs(t, h, http.MethodGet, href, "", "")
			if get.Code != http.StatusOK {
				t.Fatalf("GET %s status = %d: %s", href, get.Code, get.Body.String())
			}
			if !icalHasLine(get.Body.String(), "UID:"+birthdayEventUID(5, uid)) {
				t.Fatalf("GET %s returned another resource:\n%s", href, get.Body.String())
			}
		})
	}
}

func TestCleanDAVPathDoesNotDecode(t *testing.T) {
	for raw, want := range map[string]string{
		"/dav/calendars/1/a%3Bb.ics": "/dav/calendars/1/a%3Bb.ics",
		"/dav/calendars/1/100% x":    "/dav/calendars/1/100% x",
		"/dav/calendars/1/a?b.ics":   "/dav/calendars/1/a?b.ics",
		"/dav/calendars/1/./x/../y/": "/dav/calendars/1/y",
		"dav/calendars":              "/dav/calendars",
		"":                           "",
	} {
		if got := cleanDAVPath(raw); got != want {
			t.Errorf("cleanDAVPath(%q) = %q, want %q", raw, got, want)
		}
	}
	target := parseDAVTarget("/dav/calendars/1/a%3Bb.ics")
	if !target.Resource || target.ResourceName != "a%3Bb" {
		t.Fatalf("parseDAVTarget decoded the path again: %#v", target)
	}
}

// percentNameServer holds calendar 2, owned by user 1 and shared read-only with
// user 2, with two objects whose names differ only in whether ";" is written
// as the literal text "%3B". Decoding a path twice makes them one resource.
func percentNameServer(entries []store.ACLEntry) (*DavServer, *fakeDeadPropertyRepo) {
	const ical = "BEGIN:VCALENDAR\r\nVERSION:2.0\r\nBEGIN:VEVENT\r\nUID:%s\r\nDTSTAMP:20240101T000000Z\r\nDTSTART:20240101T100000Z\r\nEND:VEVENT\r\nEND:VCALENDAR\r\n"
	calendar := store.Calendar{ID: 2, UserID: 1, Name: "Work"}
	events := map[string]*store.Event{}
	for i, name := range []string{"a;b", "a%3Bb"} {
		events["2:"+name] = &store.Event{
			ID: int64(i + 1), CalendarID: 2, UID: "uid-" + name, ResourceName: name,
			ETag: "e" + name, RawICAL: strings.Replace(ical, "%s", "uid-"+name, 1),
		}
	}
	dead := &fakeDeadPropertyRepo{}
	return NewDavServer(Options{Store: &store.Store{
		Calendars: &fakeCalendarRepo{
			calendars: map[int64]*store.Calendar{2: &calendar},
			accessibleByUser: map[int64][]store.CalendarAccess{
				1: {{Calendar: calendar, Editor: true}},
				2: {{Calendar: calendar, Shared: true, Privileges: store.CalendarPrivileges{Read: true, ReadFreeBusy: true}}},
			},
		},
		Events:         &fakeEventRepo{events: events},
		ACLEntries:     &fakeACLRepo{entries: entries},
		DeadProperties: dead,
	}}), dead
}

func serveAsUser(t *testing.T, h *DavServer, user *store.User, method, target, body, depth string) *httptest.ResponseRecorder {
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

// An object-level deny names one resource. Listing a collection reads object
// ACEs in a batch keyed by path, so a key decoded twice lands the deny on the
// sibling whose name is the decoded spelling.
func TestObjectDenyOnANameContainingPercentStaysOnThatObject(t *testing.T) {
	delegate := &store.User{ID: 2, PrimaryEmail: "delegate@example.com"}
	h, _ := percentNameServer([]store.ACLEntry{
		{ResourcePath: "/dav/calendars/2", PrincipalHref: "/dav/principals/2/", IsGrant: true, Privilege: "read"},
		{ResourcePath: "/dav/calendars/2/a%3Bb", PrincipalHref: "/dav/principals/2/", IsGrant: false, Privilege: "read"},
	})
	denied := calendarObjectHref("/dav/calendars/2/", "a%3Bb")
	allowed := calendarObjectHref("/dav/calendars/2/", "a;b")

	listing := serveAsUser(t, h, delegate, "PROPFIND", "/dav/calendars/2/", `<D:propfind xmlns:D="DAV:"><D:prop><D:getetag/></D:prop></D:propfind>`, "1")
	if listing.Code != http.StatusMultiStatus {
		t.Fatalf("PROPFIND status = %d: %s", listing.Code, listing.Body.String())
	}
	ms := decodeMultistatus(t, listing)
	ms.responseForHref(t, allowed).assertPropValue(t, davQN("getetag"), http.StatusOK, `"ea;b"`)
	for _, href := range ms.hrefs() {
		if href == denied {
			t.Fatalf("PROPFIND listed the denied object %s: %s", denied, listing.Body.String())
		}
	}

	query := `<C:calendar-query xmlns:D="DAV:" xmlns:C="urn:ietf:params:xml:ns:caldav"><D:prop><D:getetag/></D:prop><C:filter><C:comp-filter name="VCALENDAR"/></C:filter></C:calendar-query>`
	report := serveAsUser(t, h, delegate, "REPORT", "/dav/calendars/2/", query, "1")
	if report.Code != http.StatusMultiStatus {
		t.Fatalf("REPORT status = %d: %s", report.Code, report.Body.String())
	}
	decodeMultistatus(t, report).assertHrefs(t, allowed)
}

// Dead properties are keyed by resource path, so a property set on one object
// must not appear on the sibling its name decodes to.
func TestDeadPropertyOnANameContainingPercentStaysOnThatObject(t *testing.T) {
	owner := &store.User{ID: 1, PrimaryEmail: "owner@example.com"}
	h, _ := percentNameServer(nil)
	target := calendarObjectHref("/dav/calendars/2/", "a%3Bb")
	sibling := calendarObjectHref("/dav/calendars/2/", "a;b")

	patch := serveAsUser(t, h, owner, "PROPPATCH", target, `<D:propertyupdate xmlns:D="DAV:" xmlns:X="urn:test"><D:set><D:prop><X:note>mine</X:note></D:prop></D:set></D:propertyupdate>`, "")
	if patch.Code != http.StatusMultiStatus {
		t.Fatalf("PROPPATCH status = %d: %s", patch.Code, patch.Body.String())
	}
	find := `<D:propfind xmlns:D="DAV:" xmlns:X="urn:test"><D:prop><X:note/></D:prop></D:propfind>`
	got := serveAsUser(t, h, owner, "PROPFIND", target, find, "0")
	decodeMultistatus(t, got).responseForHref(t, target).assertPropValue(t, qn("urn:test", "note"), http.StatusOK, "mine")
	other := serveAsUser(t, h, owner, "PROPFIND", sibling, find, "0")
	decodeMultistatus(t, other).responseForHref(t, sibling).assertPropStatus(t, qn("urn:test", "note"), http.StatusNotFound)
}

// expand-property, DAV:acl and acl-principal-prop-set resolve their
// Request-URI and the ACL they read through the same path handling.
func TestACLReportsOnANameContainingPercent(t *testing.T) {
	owner := &store.User{ID: 1, PrimaryEmail: "owner@example.com"}
	h, _ := percentNameServer([]store.ACLEntry{
		{ResourcePath: "/dav/calendars/2/a%3Bb", PrincipalHref: "/dav/principals/2/", IsGrant: true, Privilege: "read"},
		{ResourcePath: "/dav/calendars/2/a;b", PrincipalHref: "/dav/principals/3/", IsGrant: true, Privilege: "read"},
	})
	target := calendarObjectHref("/dav/calendars/2/", "a%3Bb")

	expand := serveAsUser(t, h, owner, "REPORT", target, `<D:expand-property xmlns:D="DAV:"><D:property name="getetag"/><D:property name="owner"><D:property name="displayname"/></D:property></D:expand-property>`, "0")
	if expand.Code != http.StatusMultiStatus {
		t.Fatalf("expand-property status = %d: %s", expand.Code, expand.Body.String())
	}
	ms := decodeMultistatus(t, expand)
	ms.assertHrefs(t, target)
	ms.responseForHref(t, target).assertPropValue(t, davQN("getetag"), http.StatusOK, `"ea%3Bb"`)

	aclProp := serveAsUser(t, h, owner, "PROPFIND", target, `<D:propfind xmlns:D="DAV:"><D:prop><D:acl/></D:prop></D:propfind>`, "0")
	if aclProp.Code != http.StatusMultiStatus {
		t.Fatalf("PROPFIND DAV:acl status = %d: %s", aclProp.Code, aclProp.Body.String())
	}
	body := aclProp.Body.String()
	if !strings.Contains(body, "/dav/principals/2/") || strings.Contains(body, "/dav/principals/3/") {
		t.Fatalf("DAV:acl of %s is not its own ACL: %s", target, body)
	}

	set := serveAsUser(t, h, owner, "REPORT", target, `<D:acl-principal-prop-set xmlns:D="DAV:"><D:prop><D:displayname/></D:prop></D:acl-principal-prop-set>`, "0")
	if set.Code != http.StatusMultiStatus {
		t.Fatalf("acl-principal-prop-set status = %d: %s", set.Code, set.Body.String())
	}
}

// A LOCK on one of two objects whose names differ only in "%3B" versus ";"
// holds that object and not the other: its lock-root names it, a write to it
// without the token is refused, and a write to the sibling is not.
func TestLockOnANameContainingPercentStaysOnThatObject(t *testing.T) {
	owner := &store.User{ID: 1, PrimaryEmail: "owner@example.com"}
	h, _ := percentNameServer(nil)
	h.store.Locks = &fakeLockRepo{}
	target := calendarObjectHref("/dav/calendars/2/", "a%3Bb")
	sibling := calendarObjectHref("/dav/calendars/2/", "a;b")

	lock := serveAsUser(t, h, owner, "LOCK", target, `<?xml version="1.0" encoding="utf-8"?><D:lockinfo xmlns:D="DAV:"><D:lockscope><D:exclusive/></D:lockscope><D:locktype><D:write/></D:locktype></D:lockinfo>`, "0")
	if lock.Code != http.StatusOK {
		t.Fatalf("LOCK status = %d: %s", lock.Code, lock.Body.String())
	}
	if !strings.Contains(lock.Body.String(), ">"+target+"<") {
		t.Fatalf("LOCK lock-root does not name %s: %s", target, lock.Body.String())
	}

	put := func(href, uid string) *httptest.ResponseRecorder {
		body := "BEGIN:VCALENDAR\r\nVERSION:2.0\r\nPRODID:-//Test//EN\r\nBEGIN:VEVENT\r\nUID:" + uid + "\r\nDTSTAMP:20240101T000000Z\r\nDTSTART:20240101T100000Z\r\nSUMMARY:changed\r\nEND:VEVENT\r\nEND:VCALENDAR\r\n"
		req := httptest.NewRequest(http.MethodPut, href, strings.NewReader(body))
		req.Header.Set("Content-Type", "text/calendar")
		req = req.WithContext(auth.WithUser(req.Context(), owner))
		rr := httptest.NewRecorder()
		h.ServeHTTP(rr, req)
		return rr
	}
	if rr := put(target, "uid-a%3Bb"); rr.Code != http.StatusLocked {
		t.Fatalf("PUT to the locked object without its token = %d, want 423: %s", rr.Code, rr.Body.String())
	}
	if rr := put(sibling, "uid-a;b"); rr.Code != http.StatusNoContent {
		t.Fatalf("PUT to the unlocked sibling = %d, want 204: %s", rr.Code, rr.Body.String())
	}
}

// An object named "%2e%2e" is an ordinary member. Decoding its path a second
// time turns it into "..", which cleans to the calendar home, a protected
// virtual root whose ACL cannot be written.
func TestACLOnANameThatDecodesToADotSegment(t *testing.T) {
	owner := &store.User{ID: 1, PrimaryEmail: "owner@example.com"}
	const ical = "BEGIN:VCALENDAR\r\nVERSION:2.0\r\nBEGIN:VEVENT\r\nUID:dots\r\nDTSTAMP:20240101T000000Z\r\nDTSTART:20240101T100000Z\r\nEND:VEVENT\r\nEND:VCALENDAR\r\n"
	calendar := store.Calendar{ID: 2, UserID: 1, Name: "Work"}
	acls := &fakeACLRepo{}
	h := NewDavServer(Options{Store: &store.Store{
		Calendars:  &fakeCalendarRepo{calendars: map[int64]*store.Calendar{2: &calendar}},
		Events:     &fakeEventRepo{events: map[string]*store.Event{"2:%2e%2e": {ID: 1, CalendarID: 2, UID: "dots", ResourceName: "%2e%2e", ETag: "e", RawICAL: ical}}},
		ACLEntries: acls,
	}})
	target := calendarObjectHref("/dav/calendars/2/", "%2e%2e")

	body := `<?xml version="1.0" encoding="utf-8"?><D:acl xmlns:D="DAV:"><D:ace><D:principal><D:href>/dav/principals/2/</D:href></D:principal><D:grant><D:privilege><D:read/></D:privilege></D:grant></D:ace></D:acl>`
	rr := serveAsUser(t, h, owner, "ACL", target, body, "")
	if rr.Code != http.StatusOK {
		t.Fatalf("ACL on %s = %d, want 200: %s", target, rr.Code, rr.Body.String())
	}
	entries, err := acls.ListByResource(t.Context(), "/dav/calendars/2/%2e%2e")
	if err != nil || len(entries) != 1 {
		t.Fatalf("stored ACL of the object = %v, %v; want the one grant", entries, err)
	}
}

// A MOVE Destination differing from the source only in a "%" its name holds
// names another collection. Decoding it a second time makes the two look like
// one collection and refuses the MOVE as onto itself; answered on the
// destination's own merits, a name holding "%" is no calendar slug.
func TestPostgresCalendarCollectionMoveToANameContainingPercent(t *testing.T) {
	db := newDAVPostgresStore(t)
	ctx := t.Context()
	owner, err := db.Users.UpsertOAuthUser(ctx, "percent-move", "percent-move@example.test", "", "")
	if err != nil {
		t.Fatal(err)
	}
	slug := "aA"
	if _, err := db.Calendars.Create(ctx, store.Calendar{UserID: owner.ID, Name: "Source", Slug: &slug}); err != nil {
		t.Fatal(err)
	}
	h := NewDavServer(Options{Store: db})
	req := httptest.NewRequest("MOVE", "/dav/calendars/aA/", nil)
	req.Header.Set("Destination", "/dav/calendars/a%2541/")
	req = req.WithContext(auth.WithUser(req.Context(), owner))
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, req)
	if rr.Code != http.StatusForbidden || !strings.Contains(rr.Body.String(), "calendar-collection-location-ok") {
		t.Fatalf("MOVE to /dav/calendars/a%%2541/ = %d, want 403 CALDAV:calendar-collection-location-ok: %s", rr.Code, rr.Body.String())
	}
	calendars, err := db.Calendars.ListByUser(ctx, owner.ID)
	if err != nil {
		t.Fatal(err)
	}
	if len(calendars) != 1 || calendars[0].Slug == nil || *calendars[0].Slug != "aA" {
		t.Fatalf("calendars after the refused MOVE = %+v, want the source untouched", calendars)
	}
}

// A lock-null resource is listed under the href that names it, so a name
// holding "%" is written escaped, as every other member's href is.
func TestLockNullResourceWithPercentIsListedByItsHref(t *testing.T) {
	owner := &store.User{ID: 1, PrimaryEmail: "owner@example.com"}
	h, _ := percentNameServer(nil)
	h.store.Locks = &fakeLockRepo{}
	target := calendarObjectHref("/dav/calendars/2/", "new%3Bx")

	lock := serveAsUser(t, h, owner, "LOCK", target, `<?xml version="1.0" encoding="utf-8"?><D:lockinfo xmlns:D="DAV:"><D:lockscope><D:exclusive/></D:lockscope><D:locktype><D:write/></D:locktype></D:lockinfo>`, "0")
	if lock.Code != http.StatusCreated {
		t.Fatalf("LOCK status = %d: %s", lock.Code, lock.Body.String())
	}
	find := `<D:propfind xmlns:D="DAV:"><D:prop><D:resourcetype/></D:prop></D:propfind>`
	listing := serveAsUser(t, h, owner, "PROPFIND", "/dav/calendars/2/", find, "1")
	if listing.Code != http.StatusMultiStatus {
		t.Fatalf("PROPFIND collection status = %d: %s", listing.Code, listing.Body.String())
	}
	decodeMultistatus(t, listing).responseForHref(t, target)

	single := serveAsUser(t, h, owner, "PROPFIND", target, find, "0")
	if single.Code != http.StatusMultiStatus {
		t.Fatalf("PROPFIND lock-null status = %d: %s", single.Code, single.Body.String())
	}
	decodeMultistatus(t, single).assertHrefs(t, target)
}
