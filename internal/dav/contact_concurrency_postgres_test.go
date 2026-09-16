package dav

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/jw6ventures/calcard/internal/auth"
	"github.com/jw6ventures/calcard/internal/store"
)

type contactBookBarrier struct {
	store.AddressBookRepository
	bookID  int64
	reads   atomic.Int32
	ready   sync.WaitGroup
	release chan struct{}
}

func (b *contactBookBarrier) GetByID(ctx context.Context, id int64) (*store.AddressBook, error) {
	book, err := b.AddressBookRepository.GetByID(ctx, id)
	if id == b.bookID && b.reads.Add(1) <= 2 {
		b.ready.Done()
		<-b.release
	}
	return book, err
}
func TestPostgresIndependentContactWrites(t *testing.T) {
	for _, method := range []string{"create", "update", "DELETE", "COPY", "MOVE"} {
		t.Run(method, func(t *testing.T) {
			db := newDAVPostgresStore(t)
			user, err := db.Users.UpsertOAuthUser(t.Context(), "concurrent", "concurrent@example.test", "Concurrent", "Concurrent")
			if err != nil {
				t.Fatal(err)
			}
			book, err := db.AddressBooks.Create(t.Context(), store.AddressBook{UserID: user.ID, Name: "Source"})
			if err != nil {
				t.Fatal(err)
			}
			dest, err := db.AddressBooks.Create(t.Context(), store.AddressBook{UserID: user.ID, Name: "Destination"})
			if err != nil {
				t.Fatal(err)
			}
			h := NewDavServer(Options{Store: db})
			bodies := make([]string, 2)
			etags := make([]string, 2)
			for i := range bodies {
				bodies[i] = fmt.Sprintf("BEGIN:VCARD\r\nVERSION:3.0\r\nUID:c%d\r\nFN:Contact %d\r\nN:Contact;%d;;;\r\nEND:VCARD\r\n", i, i, i)
				if method != "create" {
					req := httptest.NewRequest(http.MethodPut, fmt.Sprintf("/dav/addressbooks/%d/c%d.vcf", book.ID, i), strings.NewReader(bodies[i]))
					req.Header.Set("Content-Type", "text/vcard")
					rr := httptest.NewRecorder()
					h.ServeHTTP(rr, req.WithContext(auth.WithUser(req.Context(), user)))
					if rr.Code != http.StatusCreated {
						t.Fatalf("seed: %d %s", rr.Code, rr.Body.String())
					}
					etags[i] = rr.Header().Get("ETag")
				}
			}
			barrier := &contactBookBarrier{AddressBookRepository: db.AddressBooks, bookID: book.ID, release: make(chan struct{})}
			if method == "COPY" || method == "MOVE" {
				barrier.bookID = dest.ID
			}
			barrier.ready.Add(2)
			db.AddressBooks = barrier
			results := make(chan *httptest.ResponseRecorder, 2)
			for i := 0; i < 2; i++ {
				go func(i int) {
					verb := method
					if method == "create" || method == "update" {
						verb = http.MethodPut
					}
					req := httptest.NewRequest(verb, fmt.Sprintf("/dav/addressbooks/%d/c%d.vcf", book.ID, i), strings.NewReader(strings.ReplaceAll(bodies[i], "FN:Contact", "FN:Updated")))
					req.Header.Set("Content-Type", "text/vcard")
					if etags[i] != "" {
						req.Header.Set("If-Match", etags[i])
					}
					if method == "COPY" || method == "MOVE" {
						req.Header.Set("Destination", fmt.Sprintf("/dav/addressbooks/%d/c%d.vcf", dest.ID, i))
					}
					rr := httptest.NewRecorder()
					h.ServeHTTP(rr, req.WithContext(auth.WithUser(req.Context(), user)))
					results <- rr
				}(i)
			}
			barrier.ready.Wait()
			close(barrier.release)
			want := http.StatusCreated
			if method == "update" || method == "DELETE" {
				want = http.StatusNoContent
			}
			for i := 0; i < 2; i++ {
				rr := <-results
				if rr.Code != want {
					t.Errorf("independent %s = %d, want %d: %s", method, rr.Code, want, rr.Body.String())
				}
			}
		})
	}
}
