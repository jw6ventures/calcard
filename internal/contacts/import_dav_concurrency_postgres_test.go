package contacts_test

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jw6ventures/calcard/internal/auth"
	"github.com/jw6ventures/calcard/internal/contacts"
	"github.com/jw6ventures/calcard/internal/dav"
	"github.com/jw6ventures/calcard/internal/store"
)

// CardDAV PUTs of the same UIDs and names, some under another UID than the
// import's card for the name, run while a multi-batch import does. Neither
// side deadlocks or fails: every PUT is answered with a status DAV defines for
// it, and every card is stored or reported as skipped.
func TestPostgres_ImportRunsBesideCardDAVPuts(t *testing.T) {
	database := newParityStore(t)
	ctx, cancel := context.WithTimeout(t.Context(), time.Minute)
	defer cancel()
	user, err := database.Users.UpsertOAuthUser(ctx, "import-race", "import-race@example.test", "Import", "Race")
	if err != nil {
		t.Fatal(err)
	}
	book, err := database.AddressBooks.Create(ctx, store.AddressBook{UserID: user.ID, Name: "Race"})
	if err != nil {
		t.Fatal(err)
	}
	card := func(uid, name string) string {
		return "BEGIN:VCARD\r\nVERSION:3.0\r\nUID:" + uid + "\r\nFN:" + name + "\r\nEND:VCARD\r\n"
	}
	const cards = 600
	var file strings.Builder
	for i := range cards {
		file.WriteString(card(fmt.Sprintf("card-%03d", i), "Imported"))
	}

	server := dav.NewDavServer(dav.Options{Store: database})
	put := func(resourceName, uid string) int {
		req := httptest.NewRequest(http.MethodPut, fmt.Sprintf("/dav/addressbooks/%d/%s.vcf", book.ID, resourceName), strings.NewReader(card(uid, "Put")))
		req.Header.Set("Content-Type", "text/vcard")
		rr := httptest.NewRecorder()
		server.ServeHTTP(rr, req.WithContext(auth.WithUser(ctx, user)))
		return rr.Code
	}

	var wg sync.WaitGroup
	const putsPerWorker = cards / 4
	statuses := make(chan int, 4*putsPerWorker)
	for worker := range 4 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := range putsPerWorker {
				n := (i*7 + worker*131) % cards
				switch worker {
				case 0, 1:
					statuses <- put(fmt.Sprintf("card-%03d", n), fmt.Sprintf("card-%03d", n))
				case 3:
					// A contact of its own, which nothing else writes.
					name := fmt.Sprintf("put-%d-%03d", worker, i)
					if status := put(name, name); status != http.StatusCreated {
						t.Errorf("PUT of %s beside the import answered %d, want 201", name, status)
					}
				default:
					// A name the import's card for it does not own.
					statuses <- put(fmt.Sprintf("card-%03d", n), fmt.Sprintf("card-%03d", (n+1)%cards))
				}
			}
		}()
	}
	var result contacts.ImportResult
	var importErr error
	wg.Add(1)
	go func() {
		defer wg.Done()
		result, importErr = contacts.NewService(database).ImportVCards(ctx, user, book.ID, file.String())
	}()
	wg.Wait()
	close(statuses)

	if importErr != nil {
		t.Fatalf("ImportVCards: %v", importErr)
	}
	if result.Imported+len(result.Skipped) != cards {
		t.Errorf("imported %d and skipped %d, want %d cards accounted for", result.Imported, len(result.Skipped), cards)
	}
	skipped := map[string]bool{}
	for _, skip := range result.Skipped {
		skipped[skip.UID] = true
	}
	for i := range cards {
		uid := fmt.Sprintf("card-%03d", i)
		stored, err := database.Contacts.GetByUID(ctx, book.ID, uid)
		if err != nil {
			t.Fatal(err)
		}
		switch {
		case stored == nil && !skipped[uid]:
			t.Errorf("%s was imported but is not stored", uid)
		case stored != nil && !strings.Contains(stored.RawVCard, "FN:Imported") && !strings.Contains(stored.RawVCard, "FN:Put"):
			t.Errorf("%s is stored as %q, want a card the import or a PUT wrote", uid, stored.RawVCard)
		}
	}
	for i := range putsPerWorker {
		name := fmt.Sprintf("put-3-%03d", i)
		stored, err := database.Contacts.GetByUID(ctx, book.ID, name)
		if err != nil {
			t.Fatal(err)
		}
		if stored == nil || !strings.Contains(stored.RawVCard, "FN:Put") {
			t.Errorf("the PUT of %s is not stored: %+v", name, stored)
		}
	}
	for status := range statuses {
		switch status {
		case http.StatusCreated, http.StatusNoContent, http.StatusConflict:
		default:
			t.Errorf("a PUT beside the import answered %d", status)
		}
	}
	if err := ctx.Err(); err != nil {
		t.Fatalf("the import and the PUTs did not finish: %v", err)
	}
}
