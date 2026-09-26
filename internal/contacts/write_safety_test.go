package contacts

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/lib/pq"

	"github.com/jw6ventures/calcard/internal/store"
)

// Text is stored as UTF-8, so octets that are not UTF-8, and NUL, are refused
// with the card rather than failing in the database.
func TestRawVCardRefusesOctetsItCannotStore(t *testing.T) {
	for name, card := range map[string]string{
		"latin-1 bytes": "BEGIN:VCARD\r\nVERSION:3.0\r\nUID:l1\r\nFN:Ren\xe9\r\nEND:VCARD\r\n",
		"NUL":           "BEGIN:VCARD\r\nVERSION:3.0\r\nUID:n1\r\nFN:a\x00b\r\nEND:VCARD\r\n",
		"escape":        "BEGIN:VCARD\r\nVERSION:3.0\r\nUID:e1\r\nFN:a\x1bb\r\nEND:VCARD\r\n",
	} {
		t.Run(name, func(t *testing.T) {
			svc, _ := newTestService()
			if _, _, err := svc.CreateContact(context.Background(), owner, 1, UpsertInput{RawVCard: card}); !errors.Is(err, ErrBadRequest) {
				t.Fatalf("CreateContact() error = %v, want ErrBadRequest", err)
			}
		})
	}
}

func TestImportSkipsUnstorableOctetsAndKeepsGoing(t *testing.T) {
	svc, _ := newTestService()
	file := "BEGIN:VCARD\r\nVERSION:3.0\r\nUID:l1\r\nFN:Ren\xe9\r\nEND:VCARD\r\n" +
		"BEGIN:VCARD\r\nVERSION:2.1\r\nFN:Photo\r\nPHOTO;ENCODING=BASE64:\r\n AA\x00A\r\n\r\nEND:VCARD\r\n" +
		"BEGIN:VCARD\r\nVERSION:3.0\r\nUID:ok\r\nFN:Fine\r\nEND:VCARD\r\n"
	result, err := svc.ImportVCards(context.Background(), owner, 1, file)
	if err != nil {
		t.Fatalf("ImportVCards: %v", err)
	}
	if result.Imported != 1 || len(result.Skipped) != 2 {
		t.Fatalf("result = %+v, want 1 imported and 2 skipped", result)
	}
	for _, skip := range result.Skipped {
		if skip.Code != ImportSkipMalformed {
			t.Errorf("card %d code = %q, want malformed (%s)", skip.Card, skip.Code, skip.Reason)
		}
	}
}

// failingUpsertContacts fails the write of one UID the way PostgreSQL refuses
// a value it cannot store.
type failingUpsertContacts struct {
	*fakeContacts
	uid string
	err error
}

func (f *failingUpsertContacts) Upsert(ctx context.Context, c store.Contact) (*store.Contact, error) {
	if c.UID == f.uid {
		return nil, f.err
	}
	return f.fakeContacts.Upsert(ctx, c)
}

func TestImportTreatsAStoreDataErrorAsASkip(t *testing.T) {
	svc, _ := newTestService()
	base := svc.store.Contacts.(*fakeContacts)
	svc.store.Contacts = &failingUpsertContacts{fakeContacts: base, uid: "bad", err: &pq.Error{Code: "22021", Message: "invalid byte sequence"}}
	file := "BEGIN:VCARD\r\nVERSION:3.0\r\nUID:bad\r\nFN:Bad\r\nEND:VCARD\r\n" +
		"BEGIN:VCARD\r\nVERSION:3.0\r\nUID:good\r\nFN:Good\r\nEND:VCARD\r\n"
	result, err := svc.ImportVCards(context.Background(), owner, 1, file)
	if err != nil {
		t.Fatalf("ImportVCards: %v", err)
	}
	if result.Imported != 1 || len(result.Skipped) != 1 || result.Skipped[0].Code != ImportSkipMalformed {
		t.Fatalf("result = %+v", result)
	}
	if strings.Contains(result.Skipped[0].Reason, "invalid byte sequence") {
		t.Errorf("reason leaks the database message: %q", result.Skipped[0].Reason)
	}

	svc.store.Contacts = &failingUpsertContacts{fakeContacts: base, uid: "down", err: errors.New("connection refused")}
	if _, err := svc.ImportVCards(context.Background(), owner, 1, "BEGIN:VCARD\r\nVERSION:3.0\r\nUID:down\r\nFN:Down\r\nEND:VCARD\r\n"); err == nil {
		t.Fatal("an infrastructure failure was reported as a skipped card")
	}
}

// A 2.1 card's UID can be quoted-printable; the import identifies the card by
// the UID it stores, so re-importing replaces rather than duplicates.
func TestImportIdentifiesAVCard21CardByItsDecodedUID(t *testing.T) {
	svc, _ := newTestService()
	storeCard(svc, "a=1", "BEGIN:VCARD\r\nVERSION:3.0\r\nUID:a=1\r\nFN:Old\r\nN:;;;;\r\nEND:VCARD\r\n")
	result, err := svc.ImportVCards(context.Background(), owner, 1, "BEGIN:VCARD\r\nVERSION:2.1\r\nUID;ENCODING=QUOTED-PRINTABLE:a=3D1\r\nFN:New\r\nEND:VCARD\r\n")
	if err != nil || result.Imported != 1 {
		t.Fatalf("ImportVCards = %+v, %v", result, err)
	}
	if c := storedContact(t, svc, 1, "a=1"); !strings.Contains(c.RawVCard, "FN:New") {
		t.Fatalf("the stored contact was not replaced:\n%s", c.RawVCard)
	}
	if n := len(svc.store.Contacts.(*fakeContacts).items); n != 2 {
		t.Fatalf("the book holds %d contacts, want the original two", n)
	}
}

// A contact another client creates between CreateContact's check and its write
// is not overwritten.
func TestCreateContactDoesNotOverwriteAConcurrentCreate(t *testing.T) {
	svc, _ := newTestService()
	base := svc.store.Contacts.(*fakeContacts)
	svc.store.Contacts = &concurrentWriteContacts{fakeContacts: base, write: func() {
		base.items["1:race"] = store.Contact{AddressBookID: 1, UID: "race", ResourceName: "race", ETag: "theirs", RawVCard: "BEGIN:VCARD\r\nVERSION:3.0\r\nUID:race\r\nFN:Theirs\r\nEND:VCARD\r\n"}
	}}
	_, _, err := svc.CreateContact(context.Background(), owner, 1, UpsertInput{RawVCard: "BEGIN:VCARD\r\nVERSION:3.0\r\nUID:race\r\nFN:Mine\r\nEND:VCARD\r\n"})
	if !errors.Is(err, ErrConflict) {
		t.Fatalf("CreateContact() error = %v, want ErrConflict", err)
	}
	if got := base.items["1:race"].RawVCard; !strings.Contains(got, "FN:Theirs") {
		t.Fatalf("the concurrent contact was overwritten:\n%s", got)
	}
}

func TestImportUpdatesAContactCreatedConcurrently(t *testing.T) {
	svc, _ := newTestService()
	base := svc.store.Contacts.(*fakeContacts)
	svc.store.Contacts = &concurrentWriteContacts{fakeContacts: base, write: func() {
		base.items["1:race"] = store.Contact{AddressBookID: 1, UID: "race", ResourceName: "race", ETag: "theirs", RawVCard: "BEGIN:VCARD\r\nVERSION:3.0\r\nUID:race\r\nFN:Theirs\r\nEND:VCARD\r\n"}
	}}
	result, err := svc.ImportVCards(context.Background(), owner, 1, "BEGIN:VCARD\r\nVERSION:3.0\r\nUID:race\r\nFN:Imported\r\nEND:VCARD\r\n")
	if err != nil || result.Imported != 1 {
		t.Fatalf("ImportVCards = %+v, %v", result, err)
	}
	if got := base.items["1:race"].RawVCard; !strings.Contains(got, "FN:Imported") {
		t.Fatalf("import did not replace the concurrently created contact:\n%s", got)
	}
}

// revokingACL withdraws the sharee's write grant when the store checks the
// write's ACL guard: the second ListByResources, after the service pinned the
// entries with the first and decided the write on them.
type revokingACL struct {
	*fakeACL
	calls int
}

func (r *revokingACL) ListByResources(ctx context.Context, paths []string) ([]store.ACLEntry, error) {
	r.calls++
	if r.calls == 2 {
		kept := r.entries[:0:0]
		for _, e := range r.entries {
			if !(e.IsGrant && e.Privilege == "write") {
				kept = append(kept, e)
			}
		}
		r.entries = kept
	}
	return r.fakeACL.ListByResources(ctx, paths)
}

func TestUpdateContactRechecksAnACLChangedBeforeTheWrite(t *testing.T) {
	svc, aclRepo := newTestService()
	ctx := context.Background()
	if err := svc.ShareAddressBook(ctx, owner, 1, sharee.ID, true); err != nil {
		t.Fatal(err)
	}
	svc.store.ACLEntries = &revokingACL{fakeACL: aclRepo}
	_, _, err := svc.UpdateContact(ctx, sharee, 1, "c1", UpsertInput{RawVCard: "BEGIN:VCARD\r\nVERSION:3.0\r\nUID:c1\r\nFN:Sharee Edit\r\nEND:VCARD\r\n"})
	if !errors.Is(err, ErrForbidden) {
		t.Fatalf("UpdateContact() error = %v, want ErrForbidden after the grant was withdrawn", err)
	}
	if c := storedContact(t, svc, 1, "c1"); strings.Contains(c.RawVCard, "Sharee Edit") {
		t.Fatalf("the write went through a withdrawn grant:\n%s", c.RawVCard)
	}
}

// A contact-level write grant committed while the viewer role is being written
// is swept after it.
func TestDowngradeSweepsAWriteGrantAddedDuringTheRoleWrite(t *testing.T) {
	svc, aclRepo := newTestService()
	ctx := context.Background()
	if err := svc.ShareAddressBook(ctx, owner, 1, sharee.ID, true); err != nil {
		t.Fatal(err)
	}
	aclRepo.raceWrite = func() {
		aclRepo.raceWrite = nil
		aclRepo.entries = append(aclRepo.entries, store.ACLEntry{ResourcePath: "/dav/addressbooks/1/c1", PrincipalHref: sharePrincipalHref(sharee.ID), IsGrant: true, Privilege: "write"})
	}
	if err := svc.ShareAddressBook(ctx, owner, 1, sharee.ID, false); err != nil {
		t.Fatal(err)
	}
	if granted, _, _ := svc.privilegeDecision(ctx, sharee, 1, "c1", "write-content"); granted {
		t.Fatalf("viewer kept a write grant on c1: %#v", aclRepo.entries)
	}
}

// A grant withdrawn while a create is in flight is re-decided, so the sharee
// is told they may not write rather than that the UID already exists.
func TestCreateContactRedecidesAnACLChangedBeforeTheWrite(t *testing.T) {
	svc, aclRepo := newTestService()
	ctx := context.Background()
	if err := svc.ShareAddressBook(ctx, owner, 1, sharee.ID, true); err != nil {
		t.Fatal(err)
	}
	svc.store.ACLEntries = &revokingACL{fakeACL: aclRepo}
	_, _, err := svc.CreateContact(ctx, sharee, 1, UpsertInput{RawVCard: "BEGIN:VCARD\r\nVERSION:3.0\r\nUID:fresh\r\nFN:Fresh\r\nEND:VCARD\r\n"})
	if !errors.Is(err, ErrForbidden) {
		t.Fatalf("CreateContact() error = %v, want ErrForbidden", err)
	}
	if c, _ := svc.store.Contacts.GetByUID(ctx, 1, "fresh"); c != nil {
		t.Fatal("the create went through a withdrawn grant")
	}
}
