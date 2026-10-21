package contacts

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/jw6ventures/calcard/internal/store"
)

// defaultHTTPWriteTimeout is the default of APP_HTTP_WRITE_TIMEOUT in
// internal/config, which an import request has to finish inside to be answered.
const defaultHTTPWriteTimeout = 15 * time.Second

func importFixture(t *testing.T) (*store.Store, *store.User, *store.AddressBook) {
	t.Helper()
	database := newContactsPostgresStore(t)
	ctx := t.Context()
	user, err := database.Users.UpsertOAuthUser(ctx, "import-batch", "import-batch@example.test", "Import", "Batch")
	if err != nil {
		t.Fatalf("create user: %v", err)
	}
	book, err := database.AddressBooks.Create(ctx, store.AddressBook{UserID: user.ID, Name: "Import"})
	if err != nil {
		t.Fatalf("create address book: %v", err)
	}
	return database, user, book
}

func threeLineCards(n int) string {
	var file strings.Builder
	for i := range n {
		fmt.Fprintf(&file, "BEGIN:VCARD\r\nVERSION:3.0\r\nUID:card-%05d\r\nFN:Card %d\r\nEND:VCARD\r\n", i, i)
	}
	return file.String()
}

func TestPostgres_ImportOfTwoThousandCardsFitsTheWriteTimeout(t *testing.T) {
	database, user, book := importFixture(t)
	const cards = 2000
	start := time.Now()
	result, err := NewService(database).ImportVCards(t.Context(), user, book.ID, threeLineCards(cards))
	elapsed := time.Since(start)
	if err != nil {
		t.Fatalf("ImportVCards: %v", err)
	}
	t.Logf("imported %d cards in %s (%s per card)", result.Imported, elapsed, elapsed/cards)
	if result.Imported != cards || len(result.Skipped) != 0 {
		t.Fatalf("result = %d imported, %d skipped, want all %d imported", result.Imported, len(result.Skipped), cards)
	}
	// The race detector multiplies every run, so only the result is checked there.
	if !raceDetectorEnabled && elapsed > defaultHTTPWriteTimeout {
		t.Fatalf("import took %s, past the %s default write timeout", elapsed, defaultHTTPWriteTimeout)
	}
	listed, err := database.Contacts.ListForBookPaginated(t.Context(), book.ID, 1, 0)
	if err != nil || listed.TotalCount != cards {
		t.Fatalf("stored contacts = %+v, %v, want %d", listed, err, cards)
	}
}

// A card that cannot be stored costs that card, in the middle of a batch as at
// its edges: the cards on both sides of it are stored, and the skips are
// reported in file order whether the card was refused before it was written or
// by the store when the batch was.
func TestPostgres_ImportSkipsABadCardInTheMiddleOfABatchAndKeepsItsNeighbours(t *testing.T) {
	database, user, book := importFixture(t)
	if _, err := database.Contacts.Upsert(t.Context(), store.Contact{
		AddressBookID: book.ID, UID: "another-uid", ResourceName: "taken", RawVCard: lockTestCard("another-uid", "Holds the name"), ETag: "e",
	}); err != nil {
		t.Fatal(err)
	}
	const cards = 450
	var file strings.Builder
	for i := 1; i <= cards; i++ {
		switch i {
		case 100:
			file.WriteString("BEGIN:VCARD\r\nVERSION:3.0\r\nUID:nameless\r\nEND:VCARD\r\n")
		case 275:
			file.WriteString(lockTestCard("taken", "Wants a name another UID holds"))
		default:
			fmt.Fprintf(&file, "BEGIN:VCARD\r\nVERSION:3.0\r\nUID:card-%03d\r\nFN:Card %d\r\nEND:VCARD\r\n", i, i)
		}
	}
	result, err := NewService(database).ImportVCards(t.Context(), user, book.ID, file.String())
	if err != nil {
		t.Fatalf("ImportVCards: %v", err)
	}
	if result.Imported != cards-2 || len(result.Skipped) != 2 {
		t.Fatalf("result = %d imported, skipped %+v, want %d imported and 2 skipped", result.Imported, result.Skipped, cards-2)
	}
	if first, second := result.Skipped[0], result.Skipped[1]; first.Card != 100 || first.Code != ImportSkipMissingFN || second.Card != 275 || second.Code != ImportSkipDuplicateUID {
		t.Fatalf("skipped = %+v, want card 100 without a name, then card 275 for a name another UID holds", result.Skipped)
	}
	for _, uid := range []string{"card-099", "card-101", "card-274", "card-276", "card-450"} {
		if c, err := database.Contacts.GetByUID(t.Context(), book.ID, uid); err != nil || c == nil {
			t.Errorf("contact %q next to a skipped card was not stored: %v", uid, err)
		}
	}
}

// A card for a UID the book holds replaces that contact, and a UID the file
// repeats is stored as it would be one card at a time: the later card replaces
// the earlier.
func TestPostgres_ImportReplacesWhatTheBookHoldsAndWhatTheFileRepeats(t *testing.T) {
	database, user, book := importFixture(t)
	ctx := t.Context()
	service := NewService(database)
	if _, _, err := service.CreateContact(ctx, user, book.ID, UpsertInput{RawVCard: lockTestCard("held", "Old")}); err != nil {
		t.Fatal(err)
	}
	file := lockTestCard("held", "Replaced") + lockTestCard("again", "First") + lockTestCard("other", "Other") + lockTestCard("again", "Second")
	result, err := service.ImportVCards(ctx, user, book.ID, file)
	if err != nil {
		t.Fatalf("ImportVCards: %v", err)
	}
	if result.Imported != 4 || len(result.Skipped) != 0 {
		t.Fatalf("result = %+v, want all four cards imported", result)
	}
	for uid, want := range map[string]string{"held": "FN:Replaced", "again": "FN:Second", "other": "FN:Other"} {
		c, err := database.Contacts.GetByUID(ctx, book.ID, uid)
		if err != nil || c == nil || !strings.Contains(c.RawVCard, want) {
			t.Errorf("contact %q = %+v, %v, want %s", uid, c, err, want)
		}
	}
	if listed, err := database.Contacts.ListForBookPaginated(ctx, book.ID, 10, 0); err != nil || listed.TotalCount != 3 {
		t.Errorf("contacts in the book = %+v, %v, want 3", listed, err)
	}
}

// Every imported card gets what a single write of it gives: its own ETag, a
// change a sync-collection report lists, and a move of the book's CTag.
func TestPostgres_ImportedCardsEachGetAnETagASyncChangeAndACTagMove(t *testing.T) {
	database, user, book := importFixture(t)
	ctx := t.Context()
	before, err := database.AddressBooks.GetByID(ctx, book.ID)
	if err != nil {
		t.Fatal(err)
	}
	const cards = 30
	if _, err := NewService(database).ImportVCards(ctx, user, book.ID, threeLineCards(cards)); err != nil {
		t.Fatalf("ImportVCards: %v", err)
	}
	after, err := database.AddressBooks.GetByID(ctx, book.ID)
	if err != nil {
		t.Fatal(err)
	}
	if after.CTag != before.CTag+cards {
		t.Errorf("ctag = %d, want %d", after.CTag, before.CTag+cards)
	}
	page, err := database.Contacts.ListChangesAfter(ctx, book.ID, store.SyncCursorAt(before.UpdatedAt), false, 100)
	if err != nil || len(page.Members) != cards {
		t.Fatalf("sync changes = %+v, %v, want %d members", page, err, cards)
	}
	etags := map[string]bool{}
	for _, c := range page.Members {
		if c.ETag == "" || etags[c.ETag] {
			t.Errorf("contact %q has ETag %q, want one of its own", c.UID, c.ETag)
		}
		etags[c.ETag] = true
	}
}

// A sharee imports under the ACL a CardDAV PUT of each card meets: a card for
// a contact the sharee may not write is skipped, and the rest are stored.
func TestPostgres_ImportByASharee(t *testing.T) {
	database, owner, book := importFixture(t)
	ctx := t.Context()
	sharee, err := database.Users.UpsertOAuthUser(ctx, "import-sharee", "import-sharee@example.test", "Sharee", "Test")
	if err != nil {
		t.Fatal(err)
	}
	service := NewService(database)
	for _, uid := range []string{"mine", "protected"} {
		if _, _, err := service.CreateContact(ctx, owner, book.ID, UpsertInput{RawVCard: lockTestCard(uid, "Owner's "+uid)}); err != nil {
			t.Fatal(err)
		}
	}
	href := "/dav/principals/" + fmt.Sprint(sharee.ID) + "/"
	bookPath := addressBookACLCollectionPath(book.ID)
	if err := database.ACLEntries.SetACL(ctx, bookPath, []store.ACLEntry{
		{PrincipalHref: href, IsGrant: true, Privilege: "read"}, {PrincipalHref: href, IsGrant: true, Privilege: "write", Position: 1},
	}); err != nil {
		t.Fatal(err)
	}
	if err := database.ACLEntries.SetACL(ctx, addressBookACLResourcePaths(book.ID, "protected")[0], []store.ACLEntry{
		{PrincipalHref: href, IsGrant: false, Privilege: "write-content"},
	}); err != nil {
		t.Fatal(err)
	}

	file := lockTestCard("mine", "Edited") + lockTestCard("protected", "Edited") + lockTestCard("fresh", "Fresh")
	result, err := service.ImportVCards(ctx, sharee, book.ID, file)
	if err != nil {
		t.Fatalf("ImportVCards: %v", err)
	}
	if result.Imported != 2 || len(result.Skipped) != 1 || result.Skipped[0].Card != 2 || result.Skipped[0].Code != ImportSkipForbidden {
		t.Fatalf("result = %+v, want card 2 skipped as forbidden and the other two imported", result)
	}
	protected, err := database.Contacts.GetByUID(ctx, book.ID, "protected")
	if err != nil || protected == nil || strings.Contains(protected.RawVCard, "Edited") {
		t.Errorf("protected contact = %+v, %v, want it unchanged", protected, err)
	}
	if mine, _ := database.Contacts.GetByUID(ctx, book.ID, "mine"); mine == nil || !strings.Contains(mine.RawVCard, "FN:Edited") {
		t.Errorf("contact the sharee may write = %+v, want it replaced", mine)
	}
}

// uidLookups counts the lookups of one UID.
type uidLookups struct {
	store.ContactRepository
	uid   string
	count int
}

func (l *uidLookups) GetByUID(ctx context.Context, bookID int64, uid string) (*store.Contact, error) {
	if uid == l.uid {
		l.count++
	}
	return l.ContactRepository.GetByUID(ctx, bookID, uid)
}

// A card whose UID an earlier card of the file has not stored yet is decided
// after that card is stored, against the contact it made: as a replacement,
// from one lookup. Decided before, it would be a second creation that fails
// and is decided again.
func TestPostgres_ImportDecidesARepeatedUIDAfterStoringTheFirstCard(t *testing.T) {
	database, user, book := importFixture(t)
	lookups := &uidLookups{ContactRepository: database.Contacts, uid: "again"}
	counted := *database
	counted.Contacts = lookups
	file := lockTestCard("again", "First") + lockTestCard("again", "Second")
	result, err := NewService(&counted).ImportVCards(t.Context(), user, book.ID, file)
	if err != nil || result.Imported != 2 || len(result.Skipped) != 0 {
		t.Fatalf("ImportVCards = %+v, %v, want both cards imported", result, err)
	}
	// The first card is looked up for a replacement and then for a clash, the
	// second only for the contact the first made.
	if lookups.count != 3 {
		t.Errorf("the UID was looked up %d times, want 3", lookups.count)
	}
	stored, err := database.Contacts.GetByUID(t.Context(), book.ID, "again")
	if err != nil || stored == nil || !strings.Contains(stored.RawVCard, "FN:Second") {
		t.Errorf("stored contact = %+v, %v, want the second card", stored, err)
	}
}
