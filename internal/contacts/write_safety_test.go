package contacts

import (
	"context"
	"errors"
	"maps"
	"strings"
	"testing"

	"github.com/lib/pq"

	"github.com/jw6ventures/calcard/internal/acl"
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

// A file concatenated from Windows exports starts each card with a byte order
// mark; every card is imported, and none is stored with the mark.
func TestImportReadsCardsPastByteOrderMarks(t *testing.T) {
	svc, _ := newTestService()
	const bom = "\xef\xbb\xbf"
	file := bom + "BEGIN:VCARD\r\nVERSION:3.0\r\nUID:first\r\nFN:First\r\nEND:VCARD\r\n" +
		bom + "BEGIN:VCARD\r\nVERSION:2.1\r\nUID:second\r\nFN;ENCODING=QUOTED-PRINTABLE:Jos=C3=A9\r\nEND:VCARD\r\n"
	result, err := svc.ImportVCards(context.Background(), owner, 1, file)
	if err != nil {
		t.Fatalf("ImportVCards: %v", err)
	}
	if result.Imported != 2 || len(result.Skipped) != 0 {
		t.Fatalf("result = %+v, want both cards imported", result)
	}
	for _, uid := range []string{"first", "second"} {
		c, err := svc.store.Contacts.GetByUID(context.Background(), 1, uid)
		if err != nil || c == nil {
			t.Fatalf("contact %q not stored: %v", uid, err)
		}
		if strings.Contains(c.RawVCard, bom) {
			t.Errorf("contact %q was stored with a byte order mark: %q", uid, c.RawVCard)
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

// A grant withdrawn while a delete is in flight is re-decided, so the delete
// does not go through it.
func TestDeleteContactRechecksAnACLChangedBeforeTheWrite(t *testing.T) {
	svc, aclRepo := newTestService()
	ctx := context.Background()
	if err := svc.ShareAddressBook(ctx, owner, 1, sharee.ID, true); err != nil {
		t.Fatal(err)
	}
	svc.store.ACLEntries = &revokingACL{fakeACL: aclRepo}
	if err := svc.DeleteContact(ctx, sharee, 1, "c1", "", ""); !errors.Is(err, ErrForbidden) {
		t.Fatalf("DeleteContact() error = %v, want ErrForbidden after the grant was withdrawn", err)
	}
	if c, _ := svc.store.Contacts.GetByUID(ctx, 1, "c1"); c == nil {
		t.Fatal("the delete went through a withdrawn grant")
	}
}

// A grant withdrawn while a move is in flight is re-decided, so the move does
// not go through it.
func TestMoveContactRechecksAnACLChangedBeforeTheWrite(t *testing.T) {
	svc, aclRepo := newTestService()
	ctx := context.Background()
	svc.store.AddressBooks.(*fakeAB).books[2] = &store.AddressBook{ID: 2, UserID: sharee.ID, Name: "Sharee book"}
	if err := svc.ShareAddressBook(ctx, owner, 1, sharee.ID, true); err != nil {
		t.Fatal(err)
	}
	svc.store.ACLEntries = &revokingACL{fakeACL: aclRepo}
	if err := svc.MoveContact(ctx, sharee, 1, 2, "c1"); !errors.Is(err, ErrForbidden) {
		t.Fatalf("MoveContact() error = %v, want ErrForbidden after the grant was withdrawn", err)
	}
}

// A stored contact whose resource name is blank has no DAV path, so deleting
// or moving it reports it missing rather than failing.
func TestDeleteAndMoveReportAContactWithABlankResourceNameAsMissing(t *testing.T) {
	svc, _ := newTestService()
	ctx := context.Background()
	svc.store.AddressBooks.(*fakeAB).books[2] = &store.AddressBook{ID: 2, UserID: owner.ID, Name: "Second book"}
	svc.store.Contacts.(*fakeContacts).items["1:blank"] = store.Contact{AddressBookID: 1, UID: "blank", ResourceName: " ", ETag: "b"}
	if err := svc.MoveContact(ctx, owner, 1, 2, "blank"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("MoveContact() error = %v, want ErrNotFound", err)
	}
	if err := svc.DeleteContact(ctx, owner, 1, "blank", "", ""); !errors.Is(err, ErrNotFound) {
		t.Fatalf("DeleteContact() error = %v, want ErrNotFound", err)
	}
}

// Removing a contact from a book, by deleting or moving it, is decided as a
// CardDAV DELETE or MOVE of it is: DAV:unbind on the book (RFC 3744 section
// 3.10), and some privilege on the contact, which its own ACL decides ahead of
// the book's. A user denied every privilege on the contact is answered as for
// a contact that is not there.
func TestContactRemovalIsDecidedAsDAVDecidesIt(t *testing.T) {
	const (
		noShare = iota
		viewer
		editor
	)
	entry := func(resourcePath, principal string, grant bool, privilege string, position int) store.ACLEntry {
		return store.ACLEntry{ResourcePath: resourcePath, PrincipalHref: principal, IsGrant: grant, Privilege: privilege, Position: position}
	}
	cases := []struct {
		name    string
		user    *store.User
		share   int
		entries []store.ACLEntry
		want    error
	}{
		{
			name:    "a grant on the contact alone does not reach the book",
			user:    stranger,
			entries: []store.ACLEntry{entry("/dav/addressbooks/1/c1", sharePrincipalHref(stranger.ID), true, "write", 0)},
			want:    ErrForbidden,
		},
		{
			name:  "write on the book with the contact granting read and denying write",
			user:  sharee,
			share: editor,
			entries: []store.ACLEntry{
				entry("/dav/addressbooks/1/c1", sharePrincipalHref(sharee.ID), true, "read", 0),
				entry("/dav/addressbooks/1/c1", sharePrincipalHref(sharee.ID), false, "write", 1),
			},
		},
		{
			name:    "a group principal denied everything on the contact",
			user:    sharee,
			share:   editor,
			entries: []store.ACLEntry{entry("/dav/addressbooks/1/c1", acl.PrincipalAuthenticated, false, "all", 0)},
			want:    ErrNotFound,
		},
		{
			name:    "a deny stored on the contact's .vcf path",
			user:    sharee,
			share:   editor,
			entries: []store.ACLEntry{entry("/dav/addressbooks/1/c1.vcf", sharePrincipalHref(sharee.ID), false, "all", 0)},
			want:    ErrNotFound,
		},
		{
			name:  "read on the book alone",
			user:  sharee,
			share: viewer,
			want:  ErrForbidden,
		},
		{
			name: "no entry at all",
			user: stranger,
			want: ErrNotFound,
		},
	}
	for _, tc := range cases {
		for _, operation := range []string{"delete", "move"} {
			t.Run(tc.name+"/"+operation, func(t *testing.T) {
				svc, aclRepo := newTestService()
				ctx := context.Background()
				svc.store.AddressBooks.(*fakeAB).books[4] = &store.AddressBook{ID: 4, UserID: tc.user.ID, Name: "Own book"}
				if tc.share != noShare {
					if err := svc.ShareAddressBook(ctx, owner, 1, tc.user.ID, tc.share == editor); err != nil {
						t.Fatal(err)
					}
				}
				aclRepo.entries = append(aclRepo.entries, tc.entries...)
				var err error
				if operation == "delete" {
					err = svc.DeleteContact(ctx, tc.user, 1, "c1", "", "")
				} else {
					err = svc.MoveContact(ctx, tc.user, 1, 4, "c1")
				}
				if tc.want == nil && err != nil {
					t.Fatalf("%s error = %v, want it allowed", operation, err)
				}
				if tc.want != nil && !errors.Is(err, tc.want) {
					t.Fatalf("%s error = %v, want %v", operation, err, tc.want)
				}
				_, kept := svc.store.Contacts.(*fakeContacts).items["1:c1"]
				if operation == "delete" && kept != (tc.want != nil) {
					t.Fatalf("contact kept = %v after the delete returned %v", kept, err)
				}
			})
		}
	}
}

// churningACL commits a grant to an unrelated principal on the book right
// after each read of the book's entries, for the first changes reads, so the
// entries any write pinned have changed by the time the store re-checks them.
// The grant decides nothing the write depends on, but fails the guard.
type churningACL struct {
	*fakeACL
	changes int
}

func (c *churningACL) ListByResources(ctx context.Context, paths []string) ([]store.ACLEntry, error) {
	entries, err := c.fakeACL.ListByResources(ctx, paths)
	if c.changes > 0 {
		c.changes--
		principal := sharePrincipalHref(int64(1000 + len(c.entries)))
		c.entries = append(c.entries, store.ACLEntry{ResourcePath: "/dav/addressbooks/1", PrincipalHref: principal, IsGrant: true, Privilege: "read", Position: 100 + len(c.entries)})
	}
	return entries, err
}

// A write the caller made conditional on the contact's ETag is not refused
// because only the ACL changed under it: the precondition is checked again
// against the contact, which still matches.
func TestConditionalWritesSurviveAnUnrelatedACLChange(t *testing.T) {
	writes := map[string]func(*Service) error{
		"create": func(svc *Service) error {
			_, _, err := svc.CreateContact(context.Background(), sharee, 1, UpsertInput{IfNoneMatch: "*", RawVCard: "BEGIN:VCARD\r\nVERSION:3.0\r\nUID:fresh\r\nFN:Fresh\r\nEND:VCARD\r\n"})
			return err
		},
		"update": func(svc *Service) error {
			_, _, err := svc.UpdateContact(context.Background(), sharee, 1, "c1", UpsertInput{IfMatch: "e1", RawVCard: "BEGIN:VCARD\r\nVERSION:3.0\r\nUID:c1\r\nFN:Edited\r\nEND:VCARD\r\n"})
			return err
		},
		"delete": func(svc *Service) error {
			return svc.DeleteContact(context.Background(), sharee, 1, "c1", "e1", "")
		},
	}
	for name, write := range writes {
		t.Run(name, func(t *testing.T) {
			svc, aclRepo := newTestService()
			if err := svc.ShareAddressBook(context.Background(), owner, 1, sharee.ID, true); err != nil {
				t.Fatal(err)
			}
			svc.store.ACLEntries = &churningACL{fakeACL: aclRepo, changes: 1}
			if err := write(svc); err != nil {
				t.Fatalf("%s error = %v, want it to succeed on the next attempt", name, err)
			}
		})
	}
}

// A write whose ACL keeps changing under every attempt gives up as a conflict
// rather than as a failed precondition, and leaves the book as it was.
func TestWritesGiveUpWhenTheACLKeepsChanging(t *testing.T) {
	writes := map[string]func(*Service) error{
		"create": func(svc *Service) error {
			_, _, err := svc.CreateContact(context.Background(), sharee, 1, UpsertInput{IfNoneMatch: "*", RawVCard: "BEGIN:VCARD\r\nVERSION:3.0\r\nUID:fresh\r\nFN:Fresh\r\nEND:VCARD\r\n"})
			return err
		},
		"update": func(svc *Service) error {
			_, _, err := svc.UpdateContact(context.Background(), sharee, 1, "c1", UpsertInput{IfMatch: "e1", RawVCard: "BEGIN:VCARD\r\nVERSION:3.0\r\nUID:c1\r\nFN:Edited\r\nEND:VCARD\r\n"})
			return err
		},
		"delete": func(svc *Service) error {
			return svc.DeleteContact(context.Background(), sharee, 1, "c1", "e1", "")
		},
		"move": func(svc *Service) error {
			return svc.MoveContact(context.Background(), sharee, 1, 2, "c1")
		},
	}
	for name, write := range writes {
		t.Run(name, func(t *testing.T) {
			svc, aclRepo := newTestService()
			svc.store.AddressBooks.(*fakeAB).books[2] = &store.AddressBook{ID: 2, UserID: sharee.ID, Name: "Sharee book"}
			if err := svc.ShareAddressBook(context.Background(), owner, 1, sharee.ID, true); err != nil {
				t.Fatal(err)
			}
			before := maps.Clone(svc.store.Contacts.(*fakeContacts).items)
			svc.store.ACLEntries = &churningACL{fakeACL: aclRepo, changes: 2 * maxUpdateAttempts}
			err := write(svc)
			if !errors.Is(err, ErrChangedConcurrently) || !errors.Is(err, ErrConflict) {
				t.Fatalf("%s error = %v, want ErrChangedConcurrently, a conflict", name, err)
			}
			if after := svc.store.Contacts.(*fakeContacts).items; !maps.Equal(before, after) {
				t.Fatalf("the book changed: %v, was %v", after, before)
			}
		})
	}
}

// countingACL counts the reads of ACL entries.
type countingACL struct {
	*fakeACL
	reads int
}

func (c *countingACL) ListByResources(ctx context.Context, paths []string) ([]store.ACLEntry, error) {
	c.reads++
	return c.fakeACL.ListByResources(ctx, paths)
}

func (c *countingACL) ListByResource(ctx context.Context, resourcePath string) ([]store.ACLEntry, error) {
	c.reads++
	return c.fakeACL.ListByResource(ctx, resourcePath)
}

func (c *countingACL) ListByPrincipal(ctx context.Context, principalHref string) ([]store.ACLEntry, error) {
	c.reads++
	return c.fakeACL.ListByPrincipal(ctx, principalHref)
}

// A sharee's delete or move is decided from the ACL entries it pins for the
// store to re-check, so it reads the ACL once and the store reads it once
// more.
func TestRemovalReadsTheACLOnceForItsDecision(t *testing.T) {
	removals := map[string]func(*Service) error{
		"delete": func(svc *Service) error { return svc.DeleteContact(context.Background(), sharee, 1, "c1", "", "") },
		"move":   func(svc *Service) error { return svc.MoveContact(context.Background(), sharee, 1, 2, "c1") },
	}
	for name, remove := range removals {
		t.Run(name, func(t *testing.T) {
			svc, aclRepo := newTestService()
			svc.store.AddressBooks.(*fakeAB).books[2] = &store.AddressBook{ID: 2, UserID: owner.ID, Name: "Second book"}
			for _, bookID := range []int64{1, 2} {
				if err := svc.ShareAddressBook(context.Background(), owner, bookID, sharee.ID, true); err != nil {
					t.Fatal(err)
				}
			}
			counting := &countingACL{fakeACL: aclRepo}
			svc.store.ACLEntries = counting
			if err := remove(svc); err != nil {
				t.Fatalf("%s: %v", name, err)
			}
			if counting.reads != 2 {
				t.Fatalf("%s read the ACL %d times, want once for its decision and once for the store's re-check", name, counting.reads)
			}
		})
	}
}

// A move into a book that is not there is refused as a move into a book the
// user may not bind into, and only after the source has been decided: a user
// with no privilege on the contact, or moving a contact that is not there, is
// told that first.
func TestMoveIntoAMissingBookIsRefusedAfterTheSourceChecks(t *testing.T) {
	svc, _ := newTestService()
	ctx := context.Background()
	if err := svc.ShareAddressBook(ctx, owner, 1, sharee.ID, true); err != nil {
		t.Fatal(err)
	}
	if err := svc.MoveContact(ctx, sharee, 1, 999, "c1"); !errors.Is(err, ErrDestinationForbidden) {
		t.Fatalf("MoveContact() into a missing book error = %v, want ErrDestinationForbidden", err)
	}
	if err := svc.MoveContact(ctx, stranger, 1, 999, "c1"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("MoveContact() of a contact the user cannot see error = %v, want ErrNotFound", err)
	}
	if err := svc.MoveContact(ctx, sharee, 1, 999, "absent"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("MoveContact() of a missing contact error = %v, want ErrNotFound", err)
	}
}

// A contact's own ACL decides whether a sharee may move or delete it, ahead of
// the write the sharee holds on the book; one that denies them everything is
// answered as a contact that is not there.
func TestMoveAndDeleteContactHonourTheContactsOwnACL(t *testing.T) {
	svc, aclRepo := newTestService()
	ctx := context.Background()
	svc.store.AddressBooks.(*fakeAB).books[2] = &store.AddressBook{ID: 2, UserID: sharee.ID, Name: "Sharee book"}
	if err := svc.ShareAddressBook(ctx, owner, 1, sharee.ID, true); err != nil {
		t.Fatal(err)
	}
	aclRepo.entries = append(aclRepo.entries, store.ACLEntry{ResourcePath: "/dav/addressbooks/1/c1", PrincipalHref: sharePrincipalHref(sharee.ID), Privilege: "all"})
	if err := svc.MoveContact(ctx, sharee, 1, 2, "c1"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("MoveContact() error = %v, want ErrNotFound", err)
	}
	if err := svc.DeleteContact(ctx, sharee, 1, "c1", "", ""); !errors.Is(err, ErrNotFound) {
		t.Fatalf("DeleteContact() error = %v, want ErrNotFound", err)
	}
	if c, _ := svc.store.Contacts.GetByUID(ctx, 1, "c1"); c == nil {
		t.Fatal("the contact was removed from the book")
	}
}
