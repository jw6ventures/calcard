package ui

import (
	"bytes"
	"context"
	"fmt"
	"mime/multipart"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strconv"
	"strings"
	"testing"

	"github.com/go-chi/chi/v5"

	"github.com/jw6ventures/calcard/internal/auth"
	"github.com/jw6ventures/calcard/internal/store"
)

// A create or an import into an address book answers a user who cannot see the
// book as for a book that is not there, and one who can see it but not write to
// it as forbidden, without saying more about the book than that.
func TestPostgres_UICreateAndImportAnswerByWhatTheUserMaySee(t *testing.T) {
	f := newContactLockFixture(t)
	stranger, reader := f.newUser("stranger"), f.newUser("reader")
	if err := f.database.ACLEntries.SetACL(t.Context(), strings.TrimSuffix(f.bookPath(f.fromBook), "/"), []store.ACLEntry{
		{PrincipalHref: fmt.Sprintf("/dav/principals/%d/", reader.ID), IsGrant: true, Privilege: "read"},
	}); err != nil {
		t.Fatal(err)
	}
	tests := []struct {
		name string
		user *store.User
		book int64
		want int
	}{
		{name: "no entry at all", user: stranger, book: f.fromBook, want: http.StatusNotFound},
		{name: "read only", user: reader, book: f.fromBook, want: http.StatusForbidden},
		{name: "a book that is not there", user: f.owner, book: f.toBook + 1000, want: http.StatusNotFound},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			routed := func(req *http.Request) *http.Request {
				routeContext := chi.NewRouteContext()
				routeContext.URLParams.Add("id", strconv.FormatInt(tc.book, 10))
				return req.WithContext(context.WithValue(auth.WithUser(req.Context(), tc.user), chi.RouteCtxKey, routeContext))
			}
			create := httptest.NewRequest(http.MethodPost, "/addressbooks/x/contacts", strings.NewReader(url.Values{"display_name": {"New"}}.Encode()))
			create.Header.Set("Content-Type", "application/x-www-form-urlencoded")
			created := httptest.NewRecorder()
			f.handler.CreateContact(created, routed(create))

			var body bytes.Buffer
			upload := multipart.NewWriter(&body)
			part, err := upload.CreateFormFile("file", "contacts.vcf")
			if err != nil {
				t.Fatal(err)
			}
			part.Write([]byte(contactLockCard("imported", "Imported")))
			upload.Close()
			importReq := httptest.NewRequest(http.MethodPost, "/addressbooks/x/import", &body)
			importReq.Header.Set("Content-Type", upload.FormDataContentType())
			imported := httptest.NewRecorder()
			f.handler.ImportAddressBook(imported, routed(importReq))

			for name, rr := range map[string]*httptest.ResponseRecorder{"create": created, "import": imported} {
				if rr.Code != tc.want {
					t.Errorf("%s status = %d (%s), want %d", name, rr.Code, strings.TrimSpace(rr.Body.String()), tc.want)
				}
			}
		})
	}
}
