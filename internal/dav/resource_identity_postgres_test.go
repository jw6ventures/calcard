package dav

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"path"
	"regexp"
	"slices"
	"strings"
	"testing"

	"github.com/jw6ventures/calcard/internal/acl"
	"github.com/jw6ventures/calcard/internal/auth"
	"github.com/jw6ventures/calcard/internal/store"
)

// identityFixture is one owner's calendar and address book behind the real DAV
// handler over PostgreSQL, with a sharee and a stranger to decide for.
type identityFixture struct {
	t                        *testing.T
	database                 *store.Store
	handler                  *DavServer
	owner, sharee, stranger  *store.User
	calendarPath, bookPath   string
	sourcePath               string
	calendarID, addressBook  int64
	shareeHref, strangerHref string
}

func newIdentityFixture(t *testing.T) *identityFixture {
	t.Helper()
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
	f := &identityFixture{t: t, database: database, handler: NewDavServer(Options{Store: database})}
	f.owner, f.sharee, f.stranger = newUser("owner"), newUser("sharee"), newUser("stranger")
	f.shareeHref, f.strangerHref = acl.PrincipalHref(f.sharee.ID), acl.PrincipalHref(f.stranger.ID)
	cal, err := database.Calendars.Create(ctx, store.Calendar{UserID: f.owner.ID, Name: "Identity"})
	if err != nil {
		t.Fatal(err)
	}
	source, err := database.Calendars.Create(ctx, store.Calendar{UserID: f.owner.ID, Name: "Source"})
	if err != nil {
		t.Fatal(err)
	}
	book, err := database.AddressBooks.Create(ctx, store.AddressBook{UserID: f.owner.ID, Name: "Identity"})
	if err != nil {
		t.Fatal(err)
	}
	f.calendarID, f.addressBook = cal.ID, book.ID
	f.calendarPath, f.bookPath = calendarCollectionResourcePath(cal.ID), collectionResourcePath(addressBookPrefix, book.ID)
	f.sourcePath = calendarCollectionResourcePath(source.ID)
	return f
}

func (f *identityFixture) do(user *store.User, method, target, body string, headers ...string) *httptest.ResponseRecorder {
	f.t.Helper()
	req := httptest.NewRequest(method, target, strings.NewReader(body))
	switch {
	case method == http.MethodPut && strings.HasPrefix(target, calendarPrefix):
		req.Header.Set("Content-Type", "text/calendar")
	case method == http.MethodPut:
		req.Header.Set("Content-Type", "text/vcard")
	}
	for i := 0; i+1 < len(headers); i += 2 {
		req.Header.Set(headers[i], headers[i+1])
	}
	req = req.WithContext(auth.WithUser(req.Context(), user))
	rr := httptest.NewRecorder()
	f.handler.ServeHTTP(rr, req)
	return rr
}

// putEvent creates the event a request names by href, which is the stored
// name plus the extension the server strips.
func (f *identityFixture) putEvent(href, uid string) {
	f.t.Helper()
	if rr := f.do(f.owner, http.MethodPut, href, buildCalendarObject(buildVEvent(uid, "SUMMARY:"+uid))); rr.Code != http.StatusCreated {
		f.t.Fatalf("PUT %s = %d: %s", href, rr.Code, rr.Body.String())
	}
}

func (f *identityFixture) putContact(href, uid string) {
	f.t.Helper()
	if rr := f.do(f.owner, http.MethodPut, href, buildVCard("3.0", "UID:"+uid, "FN:"+uid, "N:"+uid+";;;;")); rr.Code != http.StatusCreated {
		f.t.Fatalf("PUT %s = %d: %s", href, rr.Code, rr.Body.String())
	}
}

func identityACE(principal string, grant bool, privileges ...string) string {
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

func (f *identityFixture) setACL(href string, aces ...string) {
	f.t.Helper()
	body := `<?xml version="1.0" encoding="utf-8"?><d:acl xmlns:d="DAV:">` + strings.Join(aces, "") + `</d:acl>`
	if rr := f.do(f.owner, "ACL", href, body); rr.Code != http.StatusOK {
		f.t.Fatalf("ACL %s = %d: %s", href, rr.Code, rr.Body.String())
	}
}

func (f *identityFixture) setDeadProperty(href, value string) {
	f.t.Helper()
	body := `<?xml version="1.0" encoding="utf-8"?><d:propertyupdate xmlns:d="DAV:" xmlns:x="urn:x"><d:set><d:prop><x:tag>` + value + `</x:tag></d:prop></d:set></d:propertyupdate>`
	if rr := f.do(f.owner, "PROPPATCH", href, body); rr.Code != http.StatusMultiStatus || !strings.Contains(rr.Body.String(), "200 OK") {
		f.t.Fatalf("PROPPATCH %s = %d: %s", href, rr.Code, rr.Body.String())
	}
}

// state is what the store holds at exactly statePath: its ACL entries, as
// principal and privilege, and its dead property values.
func (f *identityFixture) state(statePath string) (aces []string, dead []string) {
	f.t.Helper()
	entries, err := f.database.ACLEntries.ListByResource(f.t.Context(), statePath)
	if err != nil {
		f.t.Fatal(err)
	}
	for _, entry := range entries {
		aces = append(aces, fmt.Sprintf("%s %t %s", entry.PrincipalHref, entry.IsGrant, entry.Privilege))
	}
	properties, err := f.database.DeadProperties.ListByResources(f.t.Context(), []string{statePath})
	if err != nil {
		f.t.Fatal(err)
	}
	for _, property := range properties {
		dead = append(dead, property.InnerXML)
	}
	return aces, dead
}

func (f *identityFixture) requireState(statePath string, wantACEs, wantDead []string) {
	f.t.Helper()
	aces, dead := f.state(statePath)
	if !slices.Equal(aces, wantACEs) || !slices.Equal(dead, wantDead) {
		f.t.Errorf("%s holds ACL %q and dead properties %q, want %q and %q", statePath, aces, dead, wantACEs, wantDead)
	}
}

var lockTokenPattern = regexp.MustCompile(`opaquelocktoken:[0-9a-f-]+`)

const identityLockBody = `<?xml version="1.0" encoding="utf-8"?><d:lockinfo xmlns:d="DAV:"><d:lockscope><d:exclusive/></d:lockscope><d:locktype><d:write/></d:locktype><d:owner>sharee</d:owner></d:lockinfo>`

// A principal holding only DAV:bind may lock an unmapped URL, which reserves
// the name that URL binds and no other. /N/event.ics.ics binds event.ics; the
// owner's event is stored as event, served at /N/event.ics, and the lock must
// leave it writable by its owner.
func TestPostgres_LockOnANewNameLeavesTheStrippedNameUnlocked(t *testing.T) {
	f := newIdentityFixture(t)
	f.putEvent(f.calendarPath+"/event.ics", "event-uid")
	if err := f.database.ACLEntries.SetACL(t.Context(), f.calendarPath, []store.ACLEntry{{PrincipalHref: f.shareeHref, IsGrant: true, Privilege: "bind"}}); err != nil {
		t.Fatal(err)
	}

	lock := f.do(f.sharee, "LOCK", f.calendarPath+"/event.ics.ics", identityLockBody, "Depth", "0")
	if lock.Code != http.StatusCreated {
		t.Fatalf("LOCK event.ics.ics by a bind-only sharee = %d: %s", lock.Code, lock.Body.String())
	}
	if root := f.calendarPath + "/event.ics.ics"; !strings.Contains(lock.Body.String(), "<D:href>"+root+"</D:href>") && !strings.Contains(lock.Body.String(), root+"</") {
		t.Errorf("the lock's lockroot is not %s: %s", root, lock.Body.String())
	}
	token := lockTokenPattern.FindString(lock.Header().Get("Lock-Token"))

	if put := f.do(f.owner, http.MethodPut, f.calendarPath+"/event.ics", buildCalendarObject(buildVEvent("event-uid", "SUMMARY:edited"))); put.Code != http.StatusNoContent {
		t.Errorf("owner PUT of event under a lock on event.ics = %d, want 204: %s", put.Code, put.Body.String())
	}
	if unlock := f.do(f.owner, "UNLOCK", f.calendarPath+"/event.ics", "", "Lock-Token", "<"+token+">"); unlock.Code != http.StatusConflict {
		t.Errorf("UNLOCK of event with the token of event.ics's lock = %d, want 409", unlock.Code)
	}
	if put := f.do(f.owner, http.MethodPut, f.calendarPath+"/event.ics.ics", buildCalendarObject(buildVEvent("other-uid"))); put.Code != http.StatusLocked {
		t.Errorf("owner PUT to the locked event.ics.ics without its token = %d, want 423", put.Code)
	}
	if del := f.do(f.owner, http.MethodDelete, f.calendarPath+"/event.ics", ""); del.Code != http.StatusNoContent {
		t.Errorf("owner DELETE of event under a lock on event.ics = %d, want 204: %s", del.Code, del.Body.String())
	}
	if locks, err := f.database.Locks.ListByResource(t.Context(), f.calendarPath+"/event.ics"); err != nil || len(locks) != 1 {
		t.Errorf("deleting event left %d locks on event.ics (%v), want its one lock kept", len(locks), err)
	}
}

// COPY, MOVE and a new member's create and delete each touch the state of the
// resources they name and nothing else. A UI event named U.ics is served at
// /N/U.ics.ics, and a DAV client's /N/U.ics names the different resource U; a
// trailing space names yet another.
func TestPostgres_TransfersAndDeletesKeepOtherResourcesState(t *testing.T) {
	type transfer struct {
		name string
		// victim is the stored name of the resource whose state must survive,
		// and act the requests that must leave it alone.
		victim string
		act    func(f *identityFixture)
	}
	for _, test := range []transfer{
		{name: "COPY onto the name with the extension", victim: "victim", act: func(f *identityFixture) {
			if rr := f.do(f.owner, "COPY", f.sourcePath+"/source.ics", "", "Destination", f.calendarPath+"/victim.ics.ics"); rr.Code != http.StatusCreated {
				f.t.Fatalf("COPY = %d: %s", rr.Code, rr.Body.String())
			}
		}},
		{name: "MOVE onto the name with the extension", victim: "victim", act: func(f *identityFixture) {
			if rr := f.do(f.owner, "MOVE", f.sourcePath+"/source.ics", "", "Destination", f.calendarPath+"/victim.ics.ics"); rr.Code != http.StatusCreated {
				f.t.Fatalf("MOVE = %d: %s", rr.Code, rr.Body.String())
			}
		}},
		{name: "COPY onto the name with a trailing space", victim: "victim", act: func(f *identityFixture) {
			if rr := f.do(f.owner, "COPY", f.sourcePath+"/source.ics", "", "Destination", f.calendarPath+"/victim.ics%20.ics"); rr.Code != http.StatusCreated {
				f.t.Fatalf("COPY = %d: %s", rr.Code, rr.Body.String())
			}
		}},
		{name: "MOVE away from the name with the extension", victim: "victim", act: func(f *identityFixture) {
			f.putEvent(f.calendarPath+"/victim.ics.ics", "sibling-uid")
			if rr := f.do(f.owner, "MOVE", f.calendarPath+"/victim.ics.ics", "", "Destination", f.calendarPath+"/elsewhere.ics"); rr.Code != http.StatusCreated {
				f.t.Fatalf("MOVE = %d: %s", rr.Code, rr.Body.String())
			}
		}},
		{name: "create and delete the stripped sibling of a UI event", victim: "victim.ics", act: func(f *identityFixture) {
			f.putEvent(f.calendarPath+"/victim.ics", "sibling-uid")
			if rr := f.do(f.owner, http.MethodDelete, f.calendarPath+"/victim.ics", ""); rr.Code != http.StatusNoContent {
				f.t.Fatalf("DELETE = %d: %s", rr.Code, rr.Body.String())
			}
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			f := newIdentityFixture(t)
			f.putEvent(f.sourcePath+"/source.ics", "source-uid")
			victimHref := f.calendarPath + "/" + test.victim + ".ics"
			f.putEvent(victimHref, "victim-uid")
			f.setACL(victimHref, identityACE(f.strangerHref, false, "read"), identityACE(f.shareeHref, true, "read"))
			f.setDeadProperty(victimHref, "kept")
			want := []string{f.strangerHref + " false read", f.shareeHref + " true read"}

			test.act(f)

			f.requireState(f.calendarPath+"/"+test.victim, want, []string{"kept"})
			if rr := f.do(f.stranger, http.MethodGet, victimHref, ""); rr.Code == http.StatusOK {
				t.Errorf("the stranger the victim's ACL denies reads it after the transfer")
			}
			if rr := f.do(f.sharee, http.MethodGet, victimHref, ""); rr.Code != http.StatusOK {
				t.Errorf("the sharee the victim's ACL grants gets %d after the transfer", rr.Code)
			}
		})
	}
}

// Writing a member's ACL replaces that member's entries only: X, X.ics and
// X.ICS are three resources, each with its own ACL.
func TestPostgres_ACLWriteReplacesOnlyItsOwnMembersEntries(t *testing.T) {
	f := newIdentityFixture(t)
	for _, name := range []string{"X", "X.ics", "X.ICS"} {
		f.putEvent(f.calendarPath+"/"+name+".ics", "uid-"+name)
	}
	f.setACL(f.calendarPath+"/X.ics", identityACE(f.strangerHref, false, "read"))
	f.setACL(f.calendarPath+"/X.ics.ics", identityACE(f.strangerHref, false, "write"))
	f.setACL(f.calendarPath+"/X.ICS.ics", identityACE(f.shareeHref, true, "read"))

	f.requireState(f.calendarPath+"/X", []string{f.strangerHref + " false read"}, nil)
	f.requireState(f.calendarPath+"/X.ics", []string{f.strangerHref + " false write"}, nil)
	f.requireState(f.calendarPath+"/X.ICS", []string{f.shareeHref + " true read"}, nil)
	if rr := f.do(f.sharee, http.MethodGet, f.calendarPath+"/X.ics", ""); rr.Code == http.StatusOK {
		t.Error("X.ICS's grant to the sharee lets it read X")
	}
	if rr := f.do(f.sharee, http.MethodGet, f.calendarPath+"/X.ICS.ics", ""); rr.Code != http.StatusOK {
		t.Errorf("the sharee X.ICS grants read gets %d", rr.Code)
	}
}

// A contact named dav.VCF is decided by its own ACL, in a listing exactly as
// in a request for it alone, and the ACLs of its siblings dav and dav.vcf
// apply to it in neither.
func TestPostgres_ContactListingAndRequestReadTheSameACL(t *testing.T) {
	f := newIdentityFixture(t)
	if err := f.database.ACLEntries.SetACL(t.Context(), f.bookPath, []store.ACLEntry{{PrincipalHref: f.shareeHref, IsGrant: true, Privilege: "read"}}); err != nil {
		t.Fatal(err)
	}
	f.putContact(f.bookPath+"/dav.VCF.vcf", "upper-uid")
	f.putContact(f.bookPath+"/dav.vcf", "plain-uid")
	f.putContact(f.bookPath+"/dav.vcf.vcf", "lower-uid")
	f.setACL(f.bookPath+"/dav.vcf", identityACE(f.shareeHref, false, "read"))
	f.setACL(f.bookPath+"/dav.vcf.vcf", identityACE(f.shareeHref, false, "read"))

	listing := f.do(f.sharee, "PROPFIND", f.bookPath+"/", `<?xml version="1.0" encoding="utf-8"?><d:propfind xmlns:d="DAV:"><d:prop><d:getetag/></d:prop></d:propfind>`, "Depth", "1").Body.String()
	for name, readable := range map[string]bool{"dav.VCF": true, "dav": false, "dav.vcf": false} {
		href := f.bookPath + "/" + name + ".vcf"
		listed := strings.Contains(listing, "<D:href>"+href+"</D:href>") || strings.Contains(listing, ">"+href+"<")
		got := f.do(f.sharee, http.MethodGet, href, "").Code == http.StatusOK
		if listed != readable || got != readable {
			t.Errorf("%s: listed %t, GET readable %t, want both %t: %s", name, listed, got, readable, listing)
		}
	}
}

// A lock on an existing member is listed with the member, at the member's own
// href, and adds no lock-null resource beside it: the member named X.ics,
// served at /X.ics.ics, under a lock, is one response, and /X.ics, which
// names the member X that is not there, is none.
func TestPostgres_LockedMembersListNoPhantomLockNullResource(t *testing.T) {
	f := newIdentityFixture(t)
	members := []string{f.calendarPath + "/X.ics.ics", f.calendarPath + "/uid@example.com.ics", f.calendarPath + "/plain.ics"}
	for i, href := range members {
		f.putEvent(href, fmt.Sprintf("uid-%d", i))
		if lock := f.do(f.owner, "LOCK", href, identityLockBody, "Depth", "0"); lock.Code != http.StatusOK {
			t.Fatalf("LOCK %s = %d: %s", href, lock.Code, lock.Body.String())
		}
	}
	listing := f.do(f.owner, "PROPFIND", f.calendarPath+"/", `<?xml version="1.0" encoding="utf-8"?><d:propfind xmlns:d="DAV:"><d:prop><d:getetag/><d:lockdiscovery/></d:prop></d:propfind>`, "Depth", "1").Body.String()
	var listed []string
	for _, response := range regexp.MustCompile(`<d:response><d:href>([^<]*)</d:href>`).FindAllStringSubmatch(listing, -1) {
		if href, err := url.PathUnescape(response[1]); err == nil && href != f.calendarPath+"/" {
			listed = append(listed, href)
		}
	}
	slices.Sort(listed)
	slices.Sort(members)
	if !slices.Equal(listed, members) {
		t.Errorf("Depth 1 PROPFIND lists %q, want exactly the locked members %q: %s", listed, members, listing)
	}
}

// A Depth 0 PROPFIND of a lock-null resource names it by the href a mapped
// member at the same spelling of its URL is named by, so a client sees one
// href for the resource before and after a PUT maps it: a calendar member by
// its calendar's ID, an address book member under the collection segment the
// request used.
func TestPostgres_LockNullResourceIsNamedAsAMappedMemberIs(t *testing.T) {
	f := newIdentityFixture(t)
	const body = `<?xml version="1.0" encoding="utf-8"?><d:propfind xmlns:d="DAV:"><d:prop><d:lockdiscovery/></d:prop></d:propfind>`
	hrefPattern := regexp.MustCompile(`<d:response><d:href>([^<]*)</d:href>`)
	hrefOf := func(target string) string {
		t.Helper()
		rr := f.do(f.owner, "PROPFIND", target, body, "Depth", "0")
		match := hrefPattern.FindStringSubmatch(rr.Body.String())
		if rr.Code != http.StatusMultiStatus || match == nil {
			t.Fatalf("PROPFIND %s = %d: %s", target, rr.Code, rr.Body.String())
		}
		return match[1]
	}
	f.putEvent(f.calendarPath+"/mapped.ics", "mapped-event")
	f.putContact(f.bookPath+"/mapped.vcf", "mapped-contact")
	for _, spelling := range []struct{ collection, extension string }{
		{f.calendarPath, ".ics"},
		{f.calendarPath, ""},
		{f.calendarPath, ".ICS"},
		{"/dav/calendars/Identity", ".ics"},
		{f.bookPath, ".vcf"},
		{f.bookPath, ""},
		{"/dav/addressbooks/Identity", ".vcf"},
	} {
		lockNull := spelling.collection + "/unmapped" + spelling.extension
		t.Run(lockNull, func(t *testing.T) {
			lock := f.do(f.owner, "LOCK", lockNull, identityLockBody, "Depth", "0")
			if lock.Code != http.StatusCreated {
				t.Fatalf("LOCK %s = %d: %s", lockNull, lock.Code, lock.Body.String())
			}
			defer func() {
				if unlock := f.do(f.owner, "UNLOCK", lockNull, "", "Lock-Token", lock.Header().Get("Lock-Token")); unlock.Code != http.StatusNoContent {
					t.Fatalf("UNLOCK %s = %d: %s", lockNull, unlock.Code, unlock.Body.String())
				}
			}()
			mapped := hrefOf(spelling.collection + "/mapped" + spelling.extension)
			if got, want := hrefOf(lockNull), strings.Replace(mapped, "mapped", "unmapped", 1); got != want {
				t.Errorf("lock-null resource %s is named %s, want %s as the mapped member at the same spelling", lockNull, got, want)
			}
		})
	}
}

// RFC 4918 §6.4 has a locked resource written only by its lock's creator
// submitting the lock's token. PROPPATCH is checked against the resource's
// locks in the store's transaction alone, which holds it to the same rule:
// another principal that knows the token may not write through the lock.
func TestPostgres_LockedPropertyWriteNeedsTheLockCreator(t *testing.T) {
	const body = `<?xml version="1.0" encoding="utf-8"?><d:propertyupdate xmlns:d="DAV:" xmlns:x="urn:x"><d:set><d:prop><x:tag>v</x:tag></d:prop></d:set></d:propertyupdate>`
	f := newIdentityFixture(t)
	f.putEvent(f.calendarPath+"/locked.ics", "locked-event")
	f.putContact(f.bookPath+"/locked.vcf", "locked-contact")
	for _, member := range []string{f.calendarPath + "/locked.ics", f.bookPath + "/locked.vcf"} {
		t.Run(member, func(t *testing.T) {
			f.setACL(path.Dir(member)+"/", identityACE(f.shareeHref, true, "read", "write-properties"))
			lock := f.do(f.owner, "LOCK", member, identityLockBody, "Depth", "0")
			if lock.Code != http.StatusOK {
				t.Fatalf("LOCK %s = %d: %s", member, lock.Code, lock.Body.String())
			}
			ifHeader := "(" + lock.Header().Get("Lock-Token") + ")"
			if rr := f.do(f.sharee, "PROPPATCH", member, body, "If", ifHeader); rr.Code != http.StatusLocked {
				t.Errorf("sharee PROPPATCH with the owner's token = %d, want 423: %s", rr.Code, rr.Body.String())
			}
			if rr := f.do(f.owner, "PROPPATCH", member, body, "If", ifHeader); rr.Code != http.StatusMultiStatus || !strings.Contains(rr.Body.String(), "200 OK") {
				t.Errorf("owner PROPPATCH with its own token = %d: %s", rr.Code, rr.Body.String())
			}
		})
	}
}

// A COPY onto an existing object replaces its properties along with its body,
// so a principal denied DAV:write-properties on it is refused, as its PROPPATCH
// is, and the object keeps its dead properties and ACL.
func TestPostgres_CopyCannotOverwritePropertiesThePrincipalMayNotWrite(t *testing.T) {
	f := newIdentityFixture(t)
	sourceBook, err := f.database.AddressBooks.Create(t.Context(), store.AddressBook{UserID: f.owner.ID, Name: "Source"})
	if err != nil {
		t.Fatal(err)
	}
	sourceBookPath := collectionResourcePath(addressBookPrefix, sourceBook.ID)
	for _, tt := range []struct{ source, destination, statePath string }{
		{f.sourcePath + "/s.ics", f.calendarPath + "/d.ics", f.calendarPath + "/d"},
		{sourceBookPath + "/s.vcf", f.bookPath + "/d.vcf", f.bookPath + "/d"},
	} {
		t.Run(tt.destination, func(t *testing.T) {
			if strings.HasSuffix(tt.source, ".ics") {
				f.putEvent(tt.source, "copy-source")
				f.putEvent(tt.destination, "copy-destination")
			} else {
				f.putContact(tt.source, "copy-source")
				f.putContact(tt.destination, "copy-destination")
			}
			f.setACL(path.Dir(tt.source)+"/", identityACE(f.shareeHref, true, "read"))
			f.setACL(path.Dir(tt.destination)+"/", identityACE(f.shareeHref, true, "read", "bind", "unbind", "write-content"))
			f.setDeadProperty(tt.destination, "owner-value")
			denial := identityACE(f.shareeHref, false, "write-properties")
			f.setACL(tt.destination, denial)
			wantACEs, wantDead := f.state(tt.statePath)
			if rr := f.do(f.sharee, "COPY", tt.source, "", "Destination", tt.destination, "Overwrite", "T"); rr.Code != http.StatusForbidden || !strings.Contains(rr.Body.String(), "write-properties") {
				t.Fatalf("COPY onto an object denied write-properties = %d: %s", rr.Code, rr.Body.String())
			}
			f.requireState(tt.statePath, wantACEs, wantDead)
		})
	}
}
