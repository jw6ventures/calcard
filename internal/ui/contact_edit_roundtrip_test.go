package ui

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"slices"
	"strings"
	"testing"

	"github.com/go-chi/chi/v5"
	"github.com/jw6ventures/calcard/internal/auth"
	"github.com/jw6ventures/calcard/internal/config"
	"github.com/jw6ventures/calcard/internal/dav"
	"github.com/jw6ventures/calcard/internal/store"
	"github.com/jw6ventures/calcard/internal/vcard"
)

// storedContactShapes are cards the edit form has to read back exactly as the
// structured edit reads them: an unchanged field the form shows differently is
// written into the card as changed on the next save. Each case names the
// values the form must show.
var storedContactShapes = []struct {
	name string
	uid  string
	card string
	form map[string]string
}{
	{
		name: "vCard 2.1 quoted-printable UTF-8",
		uid:  "legacy-qp",
		card: "BEGIN:VCARD\r\nVERSION:2.1\r\nUID:legacy-qp\r\n" +
			"N;CHARSET=UTF-8;ENCODING=QUOTED-PRINTABLE:M=C3=BCller;Jos=C3=A9;;;\r\n" +
			"FN;CHARSET=UTF-8;ENCODING=QUOTED-PRINTABLE:Jos=C3=A9 M=C3=BCller\r\n" +
			"ORG;CHARSET=UTF-8;ENCODING=QUOTED-PRINTABLE:Caf=C3=A9 GmbH;Kasse\r\n" +
			"NOTE;ENCODING=QUOTED-PRINTABLE;CHARSET=UTF-8:First line=0D=0ASecond line, long enough to need a soft br=\r\n" +
			"eak\r\n" +
			"TEL;CELL:+49 30 1234\r\nEMAIL;INTERNET:jose@example.com\r\nEND:VCARD\r\n",
		form: map[string]string{
			"display_name": "José Müller",
			"first_name":   "José",
			"last_name":    "Müller",
			"company":      "Café GmbH",
			"notes":        "First line\nSecond line, long enough to need a soft break",
			"phone":        "+49 30 1234",
			"email":        "jose@example.com",
		},
	},
	{
		// The form shows the address the conversion decodes, which the
		// form's own address check refuses; the edit still accepts it back
		// because the card the page was served carries it.
		name: "vCard 2.1 quoted-printable EMAIL",
		uid:  "legacy-qp-email",
		card: "BEGIN:VCARD\r\nVERSION:2.1\r\nUID:legacy-qp-email\r\nN:Ruiz;Jose\r\nFN:Jose Ruiz\r\n" +
			"EMAIL;INTERNET;ENCODING=QUOTED-PRINTABLE:jose=20at=20example.com\r\nEND:VCARD\r\n",
		form: map[string]string{
			"display_name": "Jose Ruiz",
			"email":        "jose at example.com",
		},
	},
	{
		name: "vCard 2.1 with a byte order mark and bare LF",
		uid:  "legacy-bom",
		card: "\xef\xbb\xbfBEGIN:VCARD\nVERSION:2.1\nUID:legacy-bom\nN;CHARSET=UTF-8;ENCODING=QUOTED-PRINTABLE:Gr=C3=BCn;Lena\n" +
			"FN;CHARSET=UTF-8;ENCODING=QUOTED-PRINTABLE:Lena Gr=C3=BCn\nEND:VCARD\n",
		form: map[string]string{
			"display_name": "Lena Grün",
			"first_name":   "Lena",
			"last_name":    "Grün",
		},
	},
	{
		name: "vCard 2.1 quoted-printable Latin-1",
		uid:  "legacy-latin1",
		card: "BEGIN:VCARD\r\nVERSION:2.1\r\nUID:legacy-latin1\r\n" +
			"N;CHARSET=ISO-8859-1;ENCODING=QUOTED-PRINTABLE:Dupont;Ren=E9\r\n" +
			"FN;CHARSET=ISO-8859-1;ENCODING=QUOTED-PRINTABLE:Ren=E9 Dupont\r\nEND:VCARD\r\n",
		form: map[string]string{
			"display_name": "René Dupont",
			"first_name":   "René",
			"last_name":    "Dupont",
		},
	},
	{
		name: "vCard 3.0 folded with bare LF",
		uid:  "lf-folded",
		card: "BEGIN:VCARD\nVERSION:3.0\nUID:lf-folded\nFN:Bartholomew Montgomery-\n Fitzgerald\n" +
			"N:Montgomery-Fitzgerald;Bartholomew;;;\nORG:Montgomery\n  & Sons\n" +
			"NOTE:A note folded\n  over two lines\\, with an escape\nEND:VCARD\n",
		form: map[string]string{
			"display_name": "Bartholomew Montgomery-Fitzgerald",
			"first_name":   "Bartholomew",
			"last_name":    "Montgomery-Fitzgerald",
			"company":      "Montgomery & Sons",
			"notes":        "A note folded over two lines, with an escape",
		},
	},
	{
		// A fold keeps every indentation character past the first, so the
		// form shows an address with a space that its own check refuses.
		name: "vCard 3.0 EMAIL folded with deeper indentation",
		uid:  "folded-email",
		card: "BEGIN:VCARD\r\nVERSION:3.0\r\nUID:folded-email\r\nFN:John Doe\r\nN:Doe;John;;;\r\n" +
			"EMAIL;TYPE=INTERNET:john\r\n  doe@example.com\r\nEND:VCARD\r\n",
		form: map[string]string{
			"display_name": "John Doe",
			"email":        "john doe@example.com",
		},
	},
	{
		name: "vCard 3.0 folded with bare CR",
		uid:  "cr-folded",
		card: "BEGIN:VCARD\rVERSION:3.0\rUID:cr-folded\rFN:Ada\r  Lovelace\rN:Lovelace;Ada;;;\rEND:VCARD\r",
		form: map[string]string{
			"display_name": "Ada Lovelace",
			"first_name":   "Ada",
			"last_name":    "Lovelace",
		},
	},
}

func storedContactShapesRepo() *fakeContactRepoWithUpsert {
	repo := &fakeContactRepoWithUpsert{fakeContactRepo: fakeContactRepo{contacts: map[string]*store.Contact{}}}
	for i, shape := range storedContactShapes {
		repo.contacts["1:"+shape.uid] = &store.Contact{ID: int64(i + 1), AddressBookID: 1, UID: shape.uid,
			ResourceName: shape.uid, ETag: "etag-" + shape.uid, RawVCard: shape.card}
	}
	return repo
}

// servedContacts renders the address book page and returns its contact data
// as the page's script receives it.
func servedContacts(t *testing.T, handler *Handler) []map[string]any {
	t.Helper()
	req := httptest.NewRequest(http.MethodGet, "/addressbooks/1", nil)
	rctx := chi.NewRouteContext()
	rctx.URLParams.Add("id", "1")
	req = req.WithContext(context.WithValue(req.Context(), chi.RouteCtxKey, rctx))
	req = req.WithContext(auth.WithUser(req.Context(), &store.User{ID: 100, PrimaryEmail: "test@example.com"}))
	response := httptest.NewRecorder()
	handler.ViewAddressBook(response, req)
	if response.Code != http.StatusOK {
		t.Fatalf("ViewAddressBook status = %d", response.Code)
	}
	match := regexp.MustCompile(`var rawContacts = (.*) \|\| \[\];`).FindStringSubmatch(response.Body.String())
	if match == nil {
		t.Fatal("the page carries no rawContacts")
	}
	var served []map[string]any
	if err := json.Unmarshal([]byte(match[1]), &served); err != nil {
		t.Fatalf("decode rawContacts: %v", err)
	}
	return served
}

func newContactShapesHandler(repo *fakeContactRepoWithUpsert) *Handler {
	return NewHandler(&config.Config{}, &store.Store{
		AddressBooks: &fakeAddressBookRepo{books: map[int64]*store.AddressBook{1: {ID: 1, UserID: 100, Name: "Contacts"}}},
		Contacts:     repo,
	}, nil, dav.ValidateCalendarObject, dav.EventInstances)
}

// The page is served each card as the structured edit reads it, so the
// browser never has to decode vCard 2.1.
func TestAddressBookPageServesCardsAsTheEditReadsThem(t *testing.T) {
	served := servedContacts(t, newContactShapesHandler(storedContactShapesRepo()))
	if len(served) != len(storedContactShapes) {
		t.Fatalf("served %d contacts, want %d", len(served), len(storedContactShapes))
	}
	stored := map[string]string{}
	for _, shape := range storedContactShapes {
		stored[shape.uid] = shape.card
	}
	for _, contact := range served {
		card, _ := contact["VCard"].(string)
		original := stored[contact["UID"].(string)]
		if !strings.Contains(original, "VERSION:2.1") {
			// Only vCard 2.1 needs converting; any other card is served as
			// stored, line endings and all, for the page to read itself.
			if card != original {
				t.Errorf("contact %v was served %q, want it as stored: %q", contact["UID"], card, original)
			}
			continue
		}
		if strings.Contains(card, "QUOTED-PRINTABLE") || strings.Contains(card, "VERSION:2.1") {
			t.Errorf("contact %v was served as vCard 2.1: %q", contact["UID"], card)
		}
		if _, raw := contact["RawVCard"]; raw {
			t.Errorf("contact %v is still served its stored octets", contact["UID"])
		}
	}
}

// The edit form, opened on a stored card and saved without a change, stores
// the card the edit read: only REV moves. A field the form shows encoded or
// truncated would be written back as the user's edit.
func TestContactEditorSavesAnUnchangedCardUnchanged(t *testing.T) {
	node := requireNode(t)
	repo := storedContactShapesRepo()
	handler := newContactShapesHandler(repo)
	served := servedContacts(t, handler)

	contactsFile := filepath.Join(t.TempDir(), "contacts.json")
	data, err := json.Marshal(served)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(contactsFile, data, 0o600); err != nil {
		t.Fatal(err)
	}
	cmd := exec.Command(node, filepath.Join("testdata", "contact_edit_roundtrip.mjs"),
		filepath.Join("templates", "addressbook_view.html"), safeHTMLPartial, contactsFile)
	output, err := cmd.Output()
	if err != nil {
		if exit, ok := err.(*exec.ExitError); ok {
			t.Fatalf("contact edit round trip: %v\n%s", err, exit.Stderr)
		}
		t.Fatalf("contact edit round trip: %v", err)
	}
	var submitted []submittedContactForm
	if err := json.Unmarshal(output, &submitted); err != nil {
		t.Fatalf("decode the submitted forms: %v\n%s", err, output)
	}
	servedCards := map[string]string{}
	for _, contact := range served {
		servedCards[contact["UID"].(string)] = contact["VCard"].(string)
	}

	for _, shape := range storedContactShapes {
		t.Run(shape.name, func(t *testing.T) {
			i := slices.IndexFunc(submitted, func(s submittedContactForm) bool { return s.UID == shape.uid })
			if i < 0 {
				t.Fatalf("the editor was not opened on %s", shape.uid)
			}
			form := submitted[i].Form
			for field, want := range shape.form {
				if form[field] != want {
					t.Errorf("the form shows %s = %q, want %q", field, form[field], want)
				}
			}

			values := url.Values{}
			for field, value := range form {
				values.Set(field, value)
			}
			req := httptest.NewRequest(http.MethodPut, "/addressbooks/1/contacts/"+url.PathEscape(shape.uid), strings.NewReader(values.Encode()))
			req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
			response := httptest.NewRecorder()
			contactFormRouter(handler, 100).ServeHTTP(response, req)
			location, err := url.Parse(response.Header().Get("Location"))
			if err != nil {
				t.Fatalf("parse redirect: %v", err)
			}
			if got := location.Query().Get("status"); got != "contact_updated" {
				t.Fatalf("the unchanged save redirected to %s", location)
			}

			stored := repo.contacts["1:"+shape.uid].RawVCard
			if got, want := linesWithoutRevision(stored), linesWithoutRevision(servedCards[shape.uid]); !slices.Equal(got, want) {
				t.Errorf("an unchanged save rewrote the card:\n got  %q\n want %q", got, want)
			}
		})
	}
}

// submittedContactForm is one contact's edit form as contact_edit_roundtrip.mjs
// reports it: the fields the form would submit, keyed by field name.
type submittedContactForm struct {
	UID  string            `json:"uid"`
	Form map[string]string `json:"form"`
}

func linesWithoutRevision(card string) []string {
	var lines []string
	for _, line := range vcard.ContentLines(card) {
		if parsed, ok := vcard.ParseLine(line); ok && parsed.Is("REV") {
			continue
		}
		lines = append(lines, line)
	}
	return lines
}
