package dav

import (
	"errors"
	"net/http"
	"testing"

	"github.com/jw6ventures/calcard/internal/contacts"
	"github.com/jw6ventures/calcard/internal/events"
)

// A removal needs some privilege on the member, and a principal holding none
// and refused none is answered as for a member that is not there. An ACE that
// names the principal but decides none of the privileges a removal reads,
// such as a grant or denial of DAV:write-acl alone, holds and refuses nothing,
// so a calendar and an address book with the same ACL answer a DELETE alike,
// over DAV and REST, whether the member exists or not.
func TestPostgres_AnACEDecidingNothingHoldsNothingInEitherDomain(t *testing.T) {
	for _, privilege := range []string{"write-acl", "read-acl", "read-current-user-privilege-set", "unlock"} {
		for _, grant := range []bool{true, false} {
			f := newIdentityFixture(t)
			f.putEvent(f.calendarPath+"/member.ics", "member-event")
			f.putContact(f.bookPath+"/member.vcf", "member-contact")
			f.setACL(f.calendarPath+"/", identityACE(f.shareeHref, grant, privilege))
			f.setACL(f.bookPath+"/", identityACE(f.shareeHref, grant, privilege))
			label := privilege + " granted"
			if !grant {
				label = privilege + " denied"
			}
			for _, href := range []string{f.calendarPath + "/member.ics", f.calendarPath + "/missing.ics", f.bookPath + "/member.vcf", f.bookPath + "/missing.vcf"} {
				if rr := f.do(f.sharee, http.MethodDelete, href, ""); rr.Code != http.StatusNotFound {
					t.Errorf("%s: DAV DELETE %s = %d, want 404: %s", label, href, rr.Code, rr.Body.String())
				}
			}
			eventService := events.NewService(f.database, ValidateCalendarObject)
			contactService := contacts.NewService(f.database)
			for _, uid := range []string{"member-event", "missing"} {
				if err := eventService.DeleteEvent(t.Context(), f.sharee, f.calendarID, uid, "", ""); !errors.Is(err, events.ErrNotFound) {
					t.Errorf("%s: REST delete of event %q = %v, want not found", label, uid, err)
				}
			}
			for _, uid := range []string{"member-contact", "missing"} {
				if err := contactService.DeleteContact(t.Context(), f.sharee, f.addressBook, uid, "", ""); !errors.Is(err, contacts.ErrNotFound) {
					t.Errorf("%s: REST delete of contact %q = %v, want not found", label, uid, err)
				}
			}
		}
	}
}
