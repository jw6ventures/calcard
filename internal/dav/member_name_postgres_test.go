package dav

import (
	"encoding/xml"
	"net/http"
	"testing"

	"github.com/jw6ventures/calcard/internal/store"
)

// The hrefs ..ics and ...ics, and ..vcf and ...vcf, name the members "." and
// "..": the server strips the extension. Such a name is the collection or its
// parent once a path is cleaned, so its state path would be read as the
// collection's. Every write that would store one is refused as a PUT of an
// unacceptable name is, and nothing is stored.
func TestPostgres_DotMemberNamesAreRefusedOnEveryWrite(t *testing.T) {
	f := newIdentityFixture(t)
	f.putEvent(f.calendarPath+"/source.ics", "source-event")
	f.putContact(f.bookPath+"/source.vcf", "source-contact")
	const lockBody = `<?xml version="1.0" encoding="utf-8"?><d:lockinfo xmlns:d="DAV:"><d:lockscope><d:exclusive/></d:lockscope><d:locktype><d:write/></d:locktype><d:owner>x</d:owner></d:lockinfo>`
	domains := []struct {
		name, collection, extension, source string
		body                                func(uid string) string
		status                              int
		condition                           xml.Name
	}{
		{"calendar", f.calendarPath, ".ics", f.calendarPath + "/source.ics", func(uid string) string { return buildCalendarObject(buildVEvent(uid, "SUMMARY:dot")) }, http.StatusForbidden, calQN("valid-calendar-object-resource")},
		{"address book", f.bookPath, ".vcf", f.bookPath + "/source.vcf", func(uid string) string { return buildVCard("3.0", "UID:"+uid, "FN:Dot") }, http.StatusBadRequest, qn(nsCardDAV, "valid-address-data")},
	}
	for _, domain := range domains {
		for _, name := range []string{".", ".."} {
			href := domain.collection + "/" + name + domain.extension
			t.Run(domain.name+" "+name, func(t *testing.T) {
				assertErrorConditions(t, f.do(f.owner, http.MethodPut, href, domain.body("dot"+name)), domain.status, domain.condition)
				for _, method := range []string{"COPY", "MOVE"} {
					assertErrorConditions(t, f.do(f.owner, method, domain.source, "", "Destination", "http://example.com"+href), domain.status, domain.condition)
				}
				assertErrorConditions(t, f.do(f.owner, "LOCK", href, lockBody, "Depth", "0"), domain.status, domain.condition)
				locks, err := f.database.Locks.ListByResources(t.Context(), []string{store.MemberStatePath(domain.collection, name)})
				if err != nil {
					t.Fatal(err)
				}
				if len(locks) != 0 {
					t.Errorf("LOCK %s stored %d locks", href, len(locks))
				}
			})
		}
	}
	for _, name := range []string{".", ".."} {
		if event, err := f.database.Events.GetByResourceName(t.Context(), f.calendarID, name); err != nil || event != nil {
			t.Errorf("an event is stored as %q: %v, %v", name, event, err)
		}
		if contact, err := f.database.Contacts.GetByResourceName(t.Context(), f.addressBook, name); err != nil || contact != nil {
			t.Errorf("a contact is stored as %q: %v, %v", name, contact, err)
		}
	}
	if event, err := f.database.Events.GetByResourceName(t.Context(), f.calendarID, "source"); err != nil || event == nil {
		t.Errorf("a refused MOVE removed the source event: %v, %v", event, err)
	}
	if contact, err := f.database.Contacts.GetByResourceName(t.Context(), f.addressBook, "source"); err != nil || contact == nil {
		t.Errorf("a refused MOVE removed the source contact: %v, %v", contact, err)
	}
}

// A member a 1.1.x release stored as "." or ".." can be read, moved or copied
// to a name the server accepts, or deleted. Every other write is refused as a
// write naming it anew is, so no ACL entry or dead property is kept at a state
// path that names the collection once cleaned.
func TestPostgres_ADotMemberStoredBeforeIsOnlyReadMovedCopiedOrDeleted(t *testing.T) {
	type domain struct {
		name, collection, other, extension string
		store                              func(name, uid string)
		stored                             func(name string) bool
		status                             int
		condition                          xml.Name
	}
	for _, name := range []string{".", ".."} {
		f := newIdentityFixture(t)
		ctx := t.Context()
		otherBook, err := f.database.AddressBooks.Create(ctx, store.AddressBook{UserID: f.owner.ID, Name: "Other"})
		if err != nil {
			t.Fatal(err)
		}
		domains := []domain{
			{
				name: "calendar", collection: f.calendarPath, other: f.sourcePath, extension: ".ics",
				store: func(name, uid string) {
					if _, err := f.database.Events.Upsert(ctx, store.Event{CalendarID: f.calendarID, UID: uid, ResourceName: name, RawICAL: buildCalendarObject(buildVEvent(uid, "SUMMARY:legacy")), ETag: uid}); err != nil {
						t.Fatal(err)
					}
				},
				stored: func(name string) bool {
					event, err := f.database.Events.GetByResourceName(ctx, f.calendarID, name)
					if err != nil {
						t.Fatal(err)
					}
					return event != nil
				},
				status: http.StatusForbidden, condition: calQN("valid-calendar-object-resource"),
			},
			{
				name: "address book", collection: f.bookPath, other: collectionResourcePath(addressBookPrefix, otherBook.ID), extension: ".vcf",
				store: func(name, uid string) {
					if _, err := f.database.Contacts.Upsert(ctx, store.Contact{AddressBookID: f.addressBook, UID: uid, ResourceName: name, RawVCard: buildVCard("3.0", "UID:"+uid, "FN:Legacy"), ETag: uid}); err != nil {
						t.Fatal(err)
					}
				},
				stored: func(name string) bool {
					contact, err := f.database.Contacts.GetByResourceName(ctx, f.addressBook, name)
					if err != nil {
						t.Fatal(err)
					}
					return contact != nil
				},
				status: http.StatusBadRequest, condition: qn(nsCardDAV, "valid-address-data"),
			},
		}
		for _, d := range domains {
			t.Run(d.name+" "+name, func(t *testing.T) {
				d.store(name, "legacy")
				href := d.collection + "/" + name + d.extension
				aclBody := `<?xml version="1.0" encoding="utf-8"?><d:acl xmlns:d="DAV:">` + identityACE(f.shareeHref, false, "read") + `</d:acl>`
				assertErrorConditions(t, f.do(f.owner, "ACL", href, aclBody), d.status, d.condition)
				propertyBody := `<?xml version="1.0" encoding="utf-8"?><d:propertyupdate xmlns:d="DAV:" xmlns:x="urn:x"><d:set><d:prop><x:tag>member</x:tag></d:prop></d:set></d:propertyupdate>`
				assertErrorConditions(t, f.do(f.owner, "PROPPATCH", href, propertyBody), d.status, d.condition)
				for _, statePath := range []string{d.collection, store.MemberStatePath(d.collection, name)} {
					if aces, dead := f.state(statePath); len(aces) != 0 || len(dead) != 0 {
						t.Errorf("a refused write left ACL %v and dead properties %v at %s", aces, dead, statePath)
					}
				}
				if rr := f.do(f.owner, http.MethodGet, href, ""); rr.Code != http.StatusOK {
					t.Errorf("GET %s = %d, want 200", href, rr.Code)
				}
				if rr := f.do(f.owner, "COPY", href, "", "Destination", "http://example.com"+d.other+"/copied"+d.extension); rr.Code != http.StatusCreated {
					t.Errorf("COPY %s = %d, want 201: %s", href, rr.Code, rr.Body.String())
				}
				if rr := f.do(f.owner, "MOVE", href, "", "Destination", "http://example.com"+d.collection+"/renamed"+d.extension); rr.Code != http.StatusCreated {
					t.Errorf("MOVE %s = %d, want 201: %s", href, rr.Code, rr.Body.String())
				}
				d.store(name, "legacy-again")
				if rr := f.do(f.owner, http.MethodDelete, href, ""); rr.Code != http.StatusNoContent {
					t.Errorf("DELETE %s = %d, want 204: %s", href, rr.Code, rr.Body.String())
				}
				if d.stored(name) {
					t.Errorf("the member stored as %q is still there", name)
				}
			})
		}
	}
}
