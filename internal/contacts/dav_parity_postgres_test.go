package contacts_test

import (
	"database/sql"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	_ "github.com/lib/pq"

	"github.com/jw6ventures/calcard/internal/acl"
	"github.com/jw6ventures/calcard/internal/auth"
	"github.com/jw6ventures/calcard/internal/contacts"
	"github.com/jw6ventures/calcard/internal/dav"
	"github.com/jw6ventures/calcard/internal/store"
)

func newParityStore(t *testing.T) *store.Store {
	t.Helper()
	dsn := os.Getenv("CALCARD_TEST_POSTGRES_DSN")
	if dsn == "" {
		t.Skip("CALCARD_TEST_POSTGRES_DSN is unset; set it to run the DAV parity test")
	}
	schema := fmt.Sprintf("calcard_contacts_parity_%d", time.Now().UnixNano())
	admin, err := sql.Open("postgres", dsn)
	if err != nil {
		t.Fatalf("open PostgreSQL: %v", err)
	}
	if _, err := admin.Exec(`CREATE SCHEMA ` + schema); err != nil {
		admin.Close()
		t.Fatalf("create schema %s: %v", schema, err)
	}
	parsed, err := url.Parse(dsn)
	if err != nil {
		admin.Close()
		t.Fatalf("parse CALCARD_TEST_POSTGRES_DSN: %v", err)
	}
	query := parsed.Query()
	query.Set("search_path", schema)
	parsed.RawQuery = query.Encode()
	pool, err := sql.Open("postgres", parsed.String())
	if err != nil {
		admin.Close()
		t.Fatalf("open schema %s: %v", schema, err)
	}
	t.Cleanup(func() {
		pool.Close()
		if _, err := admin.Exec(`DROP SCHEMA ` + schema + ` CASCADE`); err != nil {
			t.Errorf("drop schema %s: %v", schema, err)
		}
		admin.Close()
	})
	schemaSQL, err := os.ReadFile(filepath.Join("..", "..", "db.sql"))
	if err != nil {
		t.Fatalf("read db.sql: %v", err)
	}
	if _, err := pool.Exec(string(schemaSQL)); err != nil {
		t.Fatalf("apply db.sql: %v", err)
	}
	pool.SetMaxOpenConns(5)
	return store.New(pool)
}

// The service answers for a contact as CardDAV does for the same user under
// the same ACL. It deletes, moves and updates a contact when, and only when,
// the DAV DELETE, MOVE and PUT of it succeed. A delete, a move and a read
// answer with the status DAV answers. A user who may not read the contact is
// answered as for a contact that is not there, preconditions included. Each
// case runs for a plain resource name, a dotted one and one ending in an
// upper-case .VCF, each with its ACL at its own state path.
func TestPostgres_ContactRemovalMatchesDAV(t *testing.T) {
	database := newParityStore(t)
	ctx := t.Context()
	users := map[string]*store.User{}
	for _, name := range []string{"owner", "sharee", "stranger", "other"} {
		user, err := database.Users.UpsertOAuthUser(ctx, "parity-"+name, name+"@example.test", "Parity", name)
		if err != nil {
			t.Fatalf("create %s: %v", name, err)
		}
		users[name] = user
	}
	sharee := acl.PrincipalHref(users["sharee"].ID)
	stranger := acl.PrincipalHref(users["stranger"].ID)
	ace := func(principal string, grant bool, privilege string, position int) store.ACLEntry {
		return store.ACLEntry{PrincipalHref: principal, IsGrant: grant, Privilege: privilege, Position: position}
	}
	viewer := []store.ACLEntry{ace(sharee, true, "read", 0)}
	editor := []store.ACLEntry{ace(sharee, true, "read", 0), ace(sharee, true, "write", 0)}
	const (
		ownDestination       = ""
		invisibleDestination = "invisible"
		readOnlyDestination  = "read-only"
	)
	cases := []struct {
		name        string
		actor       string
		book        []store.ACLEntry
		contact     []store.ACLEntry
		onVCFPath   bool
		deleteCode  int
		moveAllowed bool
		destination string
		moveMissing bool
	}{
		{name: "owner", actor: "owner", deleteCode: http.StatusNoContent, moveAllowed: true},
		{name: "write on the book", actor: "sharee", book: editor, deleteCode: http.StatusNoContent, moveAllowed: true},
		{name: "read on the book", actor: "sharee", book: viewer, deleteCode: http.StatusForbidden},
		{name: "unbind alone on the book", actor: "sharee", book: []store.ACLEntry{ace(sharee, true, "unbind", 0)}, deleteCode: http.StatusNoContent, moveAllowed: true},
		{name: "unbind denied ahead of write on the book", actor: "sharee", book: []store.ACLEntry{ace(sharee, false, "unbind", 0), ace(sharee, true, "write", 1)}, deleteCode: http.StatusForbidden},
		{name: "book denies everything", actor: "sharee", book: []store.ACLEntry{ace(sharee, false, "all", 0)}, deleteCode: http.StatusForbidden},
		{name: "write on the contact alone", actor: "stranger", contact: []store.ACLEntry{ace(stranger, true, "write", 0)}, deleteCode: http.StatusForbidden},
		{name: "no entry at all", actor: "stranger", deleteCode: http.StatusNotFound},
		{name: "contact grants read and denies write", actor: "sharee", book: editor, contact: []store.ACLEntry{ace(sharee, true, "read", 0), ace(sharee, false, "write", 1)}, deleteCode: http.StatusNoContent, moveAllowed: true},
		{name: "contact denies write alone", actor: "sharee", book: editor, contact: []store.ACLEntry{ace(sharee, false, "write", 0)}, deleteCode: http.StatusNoContent, moveAllowed: true},
		{name: "contact denies everything", actor: "sharee", book: editor, contact: []store.ACLEntry{ace(sharee, false, "all", 0)}, deleteCode: http.StatusNotFound},
		{name: "contact denies everything to a group", actor: "sharee", book: editor, contact: []store.ACLEntry{ace(acl.PrincipalAuthenticated, false, "all", 0)}, deleteCode: http.StatusNotFound},
		{name: "contact denies a group ahead of granting everyone", actor: "sharee", book: editor, contact: []store.ACLEntry{ace(acl.PrincipalAuthenticated, false, "all", 0), ace(acl.PrincipalAll, true, "all", 1)}, deleteCode: http.StatusNotFound},
		{name: "another contact's ACL at its name with .vcf denies everything", actor: "sharee", book: editor, contact: []store.ACLEntry{ace(sharee, false, "all", 0)}, onVCFPath: true, deleteCode: http.StatusNoContent, moveAllowed: true},
		{name: "contact denies everything and the book unbind", actor: "sharee", book: viewer, contact: []store.ACLEntry{ace(sharee, false, "all", 0)}, deleteCode: http.StatusForbidden},
		{name: "contact denies everything and the book grants nothing", actor: "stranger", contact: []store.ACLEntry{ace(stranger, false, "all", 0)}, deleteCode: http.StatusNotFound},
		{name: "write on the book into an invisible book", actor: "sharee", book: editor, deleteCode: http.StatusNoContent, destination: invisibleDestination},
		{name: "write on the book into a read-only book", actor: "sharee", book: editor, deleteCode: http.StatusNoContent, destination: readOnlyDestination},
		{name: "concealed contact into an invisible book", actor: "sharee", book: editor, contact: []store.ACLEntry{ace(sharee, false, "all", 0)}, deleteCode: http.StatusNotFound, destination: invisibleDestination},
		{name: "viewer moves a missing contact", actor: "sharee", book: viewer, deleteCode: http.StatusForbidden, moveMissing: true},
		{name: "editor moves a missing contact", actor: "sharee", book: editor, deleteCode: http.StatusNoContent, moveMissing: true},
	}
	namings := map[string]string{"plain": "", "dotted": ".with.dots", "upper-case .VCF": ".VCF"}
	davServer := dav.NewDavServer(dav.Options{Store: database})
	svc := contacts.NewService(database)
	status := func(err error, success int) int {
		if err == nil {
			return success
		}
		return contacts.StatusCode(err)
	}

	books := 0
	for _, tc := range cases {
		for naming, suffix := range namings {
			t.Run(tc.name+"/"+naming, func(t *testing.T) {
				actor := users[tc.actor]
				newBook := func(owner *store.User, entries []store.ACLEntry) int64 {
					books++
					book, err := database.AddressBooks.Create(ctx, store.AddressBook{UserID: owner.ID, Name: fmt.Sprintf("Book %d", books)})
					if err != nil {
						t.Fatal(err)
					}
					if err := database.ACLEntries.SetACL(ctx, fmt.Sprintf("/dav/addressbooks/%d", book.ID), entries); err != nil {
						t.Fatal(err)
					}
					return book.ID
				}
				bookID := newBook(users["owner"], tc.book)
				var destinationID int64
				switch tc.destination {
				case invisibleDestination:
					destinationID = newBook(users["other"], nil)
				case readOnlyDestination:
					destinationID = newBook(users["other"], []store.ACLEntry{ace(acl.PrincipalHref(actor.ID), true, "read", 0)})
				default:
					destinationID = newBook(actor, nil)
				}
				card := func(uid, fn string) string {
					return "BEGIN:VCARD\r\nVERSION:3.0\r\nUID:" + uid + "\r\nFN:" + fn + "\r\nEND:VCARD\r\n"
				}
				// contact stores a contact named uid+suffix under the case's
				// ACL and returns the href DAV gives it.
				contact := func(uid string) string {
					resourceName := uid + suffix
					if _, err := database.Contacts.Upsert(ctx, store.Contact{AddressBookID: bookID, UID: uid, ResourceName: resourceName, RawVCard: card(uid, uid), ETag: uid}); err != nil {
						t.Fatal(err)
					}
					aclPath := fmt.Sprintf("/dav/addressbooks/%d/%s", bookID, resourceName)
					if tc.onVCFPath {
						aclPath += ".vcf"
					}
					if err := database.ACLEntries.SetACL(ctx, aclPath, tc.contact); err != nil {
						t.Fatal(err)
					}
					return fmt.Sprintf("/dav/addressbooks/%d/%s.vcf", bookID, resourceName)
				}
				serve := func(req *http.Request) int {
					rr := httptest.NewRecorder()
					davServer.ServeHTTP(rr, req.WithContext(auth.WithUser(req.Context(), actor)))
					return rr.Code
				}

				// Each operation runs against its own contact, through DAV and
				// through the service.
				deletes := []struct {
					name    string
					ifMatch func(uid string) string
					missing bool
				}{
					{name: "unconditional", ifMatch: func(string) string { return "" }},
					{name: "If-Match on its ETag", ifMatch: func(uid string) string { return `"` + uid + `"` }},
					{name: "If-Match on a stale ETag", ifMatch: func(string) string { return `"stale"` }},
					{name: "missing contact under If-Match", ifMatch: func(string) string { return `"stale"` }, missing: true},
				}
				for n, variant := range deletes {
					davUID, serviceUID := fmt.Sprintf("dav-delete-%d", n), fmt.Sprintf("service-delete-%d", n)
					davPath := fmt.Sprintf("/dav/addressbooks/%d/%s%s.vcf", bookID, davUID, suffix)
					if !variant.missing {
						davPath = contact(davUID)
						contact(serviceUID)
					}
					req := httptest.NewRequest(http.MethodDelete, davPath, nil)
					if ifMatch := variant.ifMatch(davUID); ifMatch != "" {
						req.Header.Set("If-Match", ifMatch)
					}
					davCode := serve(req)
					if n == 0 && davCode != tc.deleteCode {
						t.Fatalf("DAV DELETE = %d, want %d", davCode, tc.deleteCode)
					}
					err := svc.DeleteContact(ctx, actor, bookID, serviceUID, variant.ifMatch(serviceUID), "")
					if got := status(err, http.StatusNoContent); got != davCode {
						t.Errorf("%s: DeleteContact() = %d (%v), DAV DELETE answered %d", variant.name, got, err, davCode)
					}
				}

				davGet := serve(httptest.NewRequest(http.MethodGet, contact("dav-get"), nil))
				contact("service-get")
				_, err := svc.GetContact(ctx, actor, bookID, "service-get")
				if got := status(err, http.StatusOK); got != davGet {
					t.Errorf("GetContact() = %d (%v), DAV GET answered %d", got, err, davGet)
				}

				put := httptest.NewRequest(http.MethodPut, contact("dav-put"), strings.NewReader(card("dav-put", "Edited")))
				put.Header.Set("Content-Type", "text/vcard; charset=utf-8")
				davPut := serve(put)
				contact("service-put")
				_, _, err = svc.UpdateContact(ctx, actor, bookID, "service-put", contacts.UpsertInput{RawVCard: card("service-put", "Edited")})
				if davAllowed := davPut/100 == 2; davAllowed != (err == nil) {
					t.Errorf("UpdateContact() error = %v, DAV PUT answered %d", err, davPut)
				}
				if err != nil {
					// A user who may not read the contact learns no more than a
					// user updating a UID that is not there.
					_, _, missingErr := svc.UpdateContact(ctx, actor, bookID, "service-put-missing", contacts.UpsertInput{RawVCard: card("service-put-missing", "Edited")})
					want := http.StatusForbidden
					if davGet == http.StatusNotFound {
						want = status(missingErr, http.StatusOK)
					}
					if got := contacts.StatusCode(err); got != want {
						t.Errorf("UpdateContact() = %d (%v), want %d: DAV GET answered %d and a missing UID %v", got, err, want, davGet, missingErr)
					}
				}

				davSource := fmt.Sprintf("/dav/addressbooks/%d/dav-move%s.vcf", bookID, suffix)
				if !tc.moveMissing {
					davSource = contact("dav-move")
					contact("service-move")
				}
				move := httptest.NewRequest("MOVE", davSource, nil)
				move.Header.Set("Destination", fmt.Sprintf("http://example.com/dav/addressbooks/%d/dav-move%s.vcf", destinationID, suffix))
				davMove := serve(move)
				if davMoved := davMove == http.StatusCreated; davMoved != tc.moveAllowed {
					t.Fatalf("DAV MOVE = %d, want it allowed = %v", davMove, tc.moveAllowed)
				}
				err = svc.MoveContact(ctx, actor, bookID, destinationID, "service-move")
				if got := status(err, http.StatusCreated); got != davMove {
					t.Errorf("MoveContact() = %d (%v), DAV MOVE answered %d", got, err, davMove)
				}
			})
		}
	}
}
