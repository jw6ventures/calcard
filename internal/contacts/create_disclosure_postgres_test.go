package contacts

import (
	"errors"
	"fmt"
	"testing"

	"github.com/jw6ventures/calcard/internal/store"
)

// A create, and an import, into an address book answers a principal who cannot
// see the book as for a book that is not there, one who can see it but not add
// to it as forbidden, and one granted DAV:bind as allowed, whichever surface
// the write arrives through.
func TestPostgres_CreateAnswersByWhatThePrincipalMaySee(t *testing.T) {
	database, owner, bookID, _ := newPostgresMoveFixture(t)
	ctx := t.Context()
	newUser := func(name string) *store.User {
		user, err := database.Users.UpsertOAuthUser(ctx, "disclosure-"+name, name+"@example.test", name, name)
		if err != nil {
			t.Fatal(err)
		}
		return user
	}
	stranger, reader, denied, binder := newUser("stranger"), newUser("reader"), newUser("denied"), newUser("binder")
	grant := func(user *store.User, isGrant bool, privilege string) store.ACLEntry {
		return store.ACLEntry{PrincipalHref: fmt.Sprintf("/dav/principals/%d/", user.ID), IsGrant: isGrant, Privilege: privilege}
	}
	bookPath := addressBookACLCollectionPath(bookID)
	if err := database.ACLEntries.SetACL(ctx, bookPath, []store.ACLEntry{
		grant(reader, true, "read"), grant(denied, false, "all"), grant(binder, true, "bind"),
	}); err != nil {
		t.Fatal(err)
	}

	tests := []struct {
		name   string
		user   *store.User
		bookID int64
		want   error
		// imported is how many cards an import of one card stores, and skipped
		// the reason it gives for the card it does not.
		imported int
		skipped  ImportSkipCode
	}{
		{name: "owner", user: owner, bookID: bookID, imported: 1},
		{name: "no entry at all", user: stranger, bookID: bookID, want: ErrNotFound},
		{name: "read only", user: reader, bookID: bookID, want: ErrForbidden},
		{name: "everything denied", user: denied, bookID: bookID, want: ErrForbidden},
		{name: "bind alone", user: binder, bookID: bookID, skipped: ImportSkipForbidden},
		{name: "a book that is not there", user: owner, bookID: bookID + 1000, want: ErrNotFound},
	}
	service := NewService(database)
	for i, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			card := lockTestCard(fmt.Sprintf("created-%d", i), "Created")
			_, _, createErr := service.CreateContact(ctx, tc.user, tc.bookID, UpsertInput{RawVCard: card})
			_, _, structuredErr := service.CreateContact(ctx, tc.user, tc.bookID, UpsertInput{Structured: &StructuredInput{DisplayName: "Structured"}})
			result, importErr := service.ImportVCards(ctx, tc.user, tc.bookID, lockTestCard(fmt.Sprintf("imported-%d", i), "Imported"))
			for name, err := range map[string]error{"raw create": createErr, "structured create": structuredErr} {
				if tc.want == nil && err != nil || tc.want != nil && !errors.Is(err, tc.want) {
					t.Errorf("%s error = %v, want %v", name, err, tc.want)
				}
			}
			// An import refuses a book the user cannot add to outright, but
			// skips a card that a user granted only DAV:bind may not write.
			if tc.want != nil && !errors.Is(importErr, tc.want) || tc.want == nil && importErr != nil {
				t.Errorf("import error = %v, want %v", importErr, tc.want)
			}
			if result.Imported != tc.imported {
				t.Errorf("import stored %d cards, want %d", result.Imported, tc.imported)
			}
			if tc.skipped != "" && (len(result.Skipped) != 1 || result.Skipped[0].Code != tc.skipped) {
				t.Errorf("import skipped %+v, want one card skipped as %s", result.Skipped, tc.skipped)
			}
		})
	}
}
