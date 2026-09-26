package contacts

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/jw6ventures/calcard/internal/store"
	"github.com/jw6ventures/calcard/internal/ui/utils"
	"github.com/jw6ventures/calcard/internal/vcard"
)

func storedContact(t *testing.T, svc *Service, bookID int64, uid string) store.Contact {
	t.Helper()
	c, err := svc.store.Contacts.GetByUID(context.Background(), bookID, uid)
	if err != nil || c == nil {
		t.Fatalf("GetByUID(%q) = %v, %v", uid, c, err)
	}
	return *c
}

// The raw-vCard API and VCF import store a body as the address object, so it is
// held to the same single-card structure a CardDAV PUT is.
func TestRawVCardPayloadMustBeOneValidCard(t *testing.T) {
	const card = "BEGIN:VCARD\r\nVERSION:3.0\r\nUID:%s\r\nFN:%s\r\nEND:VCARD\r\n"
	tests := []struct {
		name string
		body string
	}{
		{name: "two cards", body: "BEGIN:VCARD\r\nVERSION:3.0\r\nUID:a\r\nFN:A\r\nEND:VCARD\r\nBEGIN:VCARD\r\nVERSION:3.0\r\nUID:b\r\nFN:B\r\nEND:VCARD\r\n"},
		{name: "two UIDs", body: "BEGIN:VCARD\r\nVERSION:3.0\r\nUID:a\r\nUID:b\r\nFN:A\r\nEND:VCARD\r\n"},
		{name: "content after END", body: "BEGIN:VCARD\r\nVERSION:3.0\r\nEND:VCARD\r\nUID:a\r\nFN:x\r\n"},
		{name: "no FN", body: "BEGIN:VCARD\r\nVERSION:3.0\r\nUID:a\r\nEND:VCARD\r\n"},
		{name: "no VERSION", body: "BEGIN:VCARD\r\nUID:a\r\nFN:A\r\nEND:VCARD\r\n"},
		{name: "unsupported VERSION", body: "BEGIN:VCARD\r\nVERSION:5.0\r\nUID:a\r\nFN:A\r\nEND:VCARD\r\n"},
		{name: "empty UID", body: "BEGIN:VCARD\r\nVERSION:3.0\r\nUID:\r\nFN:A\r\nEND:VCARD\r\n"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			svc, _ := newTestService()
			_, _, err := svc.CreateContact(context.Background(), owner, 1, UpsertInput{RawVCard: tt.body})
			if !errors.Is(err, ErrBadRequest) {
				t.Fatalf("CreateContact() error = %v, want ErrBadRequest", err)
			}
		})
	}

	svc, _ := newTestService()
	body := strings.Replace(card, "%s", "ok", 1)
	body = strings.Replace(body, "%s", "Okay", 1)
	if _, _, err := svc.CreateContact(context.Background(), owner, 1, UpsertInput{RawVCard: body}); err != nil {
		t.Fatalf("CreateContact() of a valid card: %v", err)
	}
}

// A UID written with parameters is the card's UID; a reader that only knows
// "UID:" would inject a second one and store an invalid card.
func TestRawVCardWithParameterisedUIDKeepsOneUID(t *testing.T) {
	svc, _ := newTestService()
	body := "BEGIN:VCARD\r\nVERSION:4.0\r\nUID;VALUE=text:p1\r\nFN:Param\r\nEND:VCARD\r\n"
	c, _, err := svc.CreateContact(context.Background(), owner, 1, UpsertInput{RawVCard: body})
	if err != nil {
		t.Fatalf("CreateContact(): %v", err)
	}
	if c.UID != "p1" {
		t.Fatalf("stored UID = %q, want p1", c.UID)
	}
	if n := vcard.ReadStructure(c.RawVCard).UIDCount; n != 1 {
		t.Fatalf("stored card carries %d UIDs:\n%s", n, c.RawVCard)
	}
}

func TestRawVCardWithoutUIDGetsOne(t *testing.T) {
	svc, _ := newTestService()
	body := "BEGIN:VCARD\r\nVERSION:3.0\r\nFN:NoUID\r\nEND:VCARD\r\n"
	c, _, err := svc.CreateContact(context.Background(), owner, 1, UpsertInput{RawVCard: body})
	if err != nil {
		t.Fatalf("CreateContact(): %v", err)
	}
	if err := vcard.Validate(c.RawVCard, true); err != nil {
		t.Fatalf("stored card is invalid: %v\n%s", err, c.RawVCard)
	}
}

// A new contact's UID names its resource, so it is held to characters that are
// safe as one path segment. An existing contact keeps whatever UID it has.
func TestNewContactUIDMustBeASafePathSegment(t *testing.T) {
	for _, uid := range []string{"a/b", "a?b", "a#b", "a%2Fb", "a b", "../x", "a;b", "a,b", `a\b`} {
		if _, _, err := structuredPayload(&StructuredInput{UID: uid, DisplayName: "Bob"}, ""); !errors.Is(err, ErrBadRequest) {
			t.Errorf("new contact with UID %q: error = %v, want ErrBadRequest", uid, err)
		}
	}
	for _, uid := range []string{"abc-123", "3F2504E0-4F89-11D3-9A0C-0305E82C3301:ABPerson", "urn:uuid:1f0e", "x_y.z@calcard", "a+b=c~d"} {
		if _, _, err := structuredPayload(&StructuredInput{UID: uid, DisplayName: "Bob"}, ""); err != nil {
			t.Errorf("new contact with UID %q: %v", uid, err)
		}
	}
}

// A raw card keeps the UID its author wrote, but a UID that is not a safe path
// segment does not become the resource name.
func TestRawVCardWithUnsafeUIDGetsASafeResourceName(t *testing.T) {
	svc, _ := newTestService()
	body := "BEGIN:VCARD\r\nVERSION:3.0\r\nUID:a/b?c#d\r\nFN:Odd\r\nEND:VCARD\r\n"
	c, _, err := svc.CreateContact(context.Background(), owner, 1, UpsertInput{RawVCard: body})
	if err != nil {
		t.Fatalf("CreateContact(): %v", err)
	}
	if c.UID != "a/b?c#d" {
		t.Fatalf("UID = %q, want the card's own", c.UID)
	}
	if !safeResourceSegment(c.ResourceName) {
		t.Fatalf("resource name %q is not a safe path segment", c.ResourceName)
	}
}

const richCard40 = "BEGIN:VCARD\r\n" +
	"VERSION:4.0\r\n" +
	"UID:rich\r\n" +
	"FN:Ada Lovelace\r\n" +
	"N:Lovelace;Ada;Augusta;Countess;\r\n" +
	"ORG:Analytical Engines;Research\r\n" +
	"item1.EMAIL;TYPE=work:ada@work.example\r\n" +
	"item1.X-ABLabel:_$!<Work>!$_\r\n" +
	"EMAIL;TYPE=home:ada@home.example\r\n" +
	"TEL;TYPE=cell;PREF=1:+15551234,,99;ext=5\r\n" +
	"TEL;TYPE=work:+15550000\r\n" +
	"ADR;TYPE=home:;;12 St James's Sq;London;;SW1Y;UK\r\n" +
	"PHOTO:data:image/png;base64,iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAYAAAAfFcSJAAAADUlEQVR42mNkYPhfDwAChwGA60e6kgAAAABJRU5ErkJggg==\r\n" +
	"CATEGORIES:friends,math\r\n" +
	"URL:https://example.com/ada\r\n" +
	"BDAY:18151210\r\n" +
	"NOTE:first\\nsecond\r\n" +
	"REV:20200101T000000Z\r\n" +
	"END:VCARD\r\n"

// richCardForm is what the web form shows for richCard40: the first EMAIL and
// TEL raw, the rest decoded.
func richCardForm() StructuredInput {
	return StructuredInput{
		UID:         "rich",
		DisplayName: "Ada Lovelace",
		FirstName:   "Ada",
		LastName:    "Lovelace",
		Email:       "ada@work.example",
		Phone:       "+15551234,,99;ext=5",
		Birthday:    "1815-12-10",
		Notes:       "first\nsecond",
		Company:     "Analytical Engines",
	}
}

func newRichContactService(t *testing.T) *Service {
	t.Helper()
	svc, _ := newTestService()
	contacts := svc.store.Contacts.(*fakeContacts)
	contacts.items["1:rich"] = store.Contact{AddressBookID: 1, UID: "rich", ResourceName: "rich", ETag: "r1", RawVCard: richCard40}
	return svc
}

func contentLinesWithout(card string, drop ...string) []string {
	var lines []string
	for _, line := range vcard.ContentLines(card) {
		parsed, ok := vcard.ParseLine(line)
		skip := false
		for _, name := range drop {
			if ok && parsed.Is(name) {
				skip = true
			}
		}
		if !skip {
			lines = append(lines, line)
		}
	}
	return lines
}

// Saving the form unchanged must not lose anything the form does not show, nor
// rewrite anything it does.
func TestStructuredEditKeepsEverythingTheFormDoesNotShow(t *testing.T) {
	svc := newRichContactService(t)
	form := richCardForm()
	c, _, err := svc.UpdateContact(context.Background(), owner, 1, "rich", UpsertInput{Structured: &form})
	if err != nil {
		t.Fatalf("UpdateContact(): %v", err)
	}
	got := strings.Join(contentLinesWithout(c.RawVCard, "REV"), "\n")
	want := strings.Join(contentLinesWithout(richCard40, "REV"), "\n")
	if got != want {
		t.Fatalf("unchanged form rewrote the card:\n got:\n%s\nwant:\n%s", got, want)
	}
	if strings.Contains(c.RawVCard, "REV:20200101T000000Z") {
		t.Fatalf("REV was not bumped:\n%s", c.RawVCard)
	}
	if err := vcard.Validate(c.RawVCard, true); err != nil {
		t.Fatalf("merged card is invalid: %v", err)
	}
}

func TestStructuredEditReplacesOnlyTheFormFields(t *testing.T) {
	svc := newRichContactService(t)
	form := richCardForm()
	form.DisplayName = "Ada King"
	form.FirstName = "Augusta Ada"
	form.LastName = "King"
	form.Company = "Babbage & Co; Ltd"
	form.Email = "ada@new.example"
	form.Phone = "+1 555 0100,1"
	form.Birthday = "--12-10"
	form.Notes = "one, two"
	c, _, err := svc.UpdateContact(context.Background(), owner, 1, "rich", UpsertInput{Structured: &form})
	if err != nil {
		t.Fatalf("UpdateContact(): %v", err)
	}
	card := c.RawVCard
	for _, want := range []string{
		"VERSION:4.0",
		"FN:Ada King",
		`N:King;Augusta Ada;Augusta;Countess;`,
		`ORG:Babbage & Co\; Ltd;Research`,
		"item1.EMAIL;TYPE=work:ada@new.example",
		"item1.X-ABLabel:_$!<Work>!$_",
		"EMAIL;TYPE=home:ada@home.example",
		"TEL;TYPE=cell;PREF=1:+1 555 0100,1",
		"TEL;TYPE=work:+15550000",
		"ADR;TYPE=home:;;12 St James's Sq;London;;SW1Y;UK",
		"CATEGORIES:friends,math",
		"URL:https://example.com/ada",
		"BDAY:--1210",
		`NOTE:one\, two`,
	} {
		if !containsContentLine(card, want) {
			t.Errorf("merged card lacks %q:\n%s", want, card)
		}
	}
	if !strings.Contains(strings.Join(vcard.ContentLines(card), ""), "PHOTO:data:image/png;base64,iVBOR") {
		t.Errorf("merged card lost PHOTO:\n%s", card)
	}
	if err := vcard.Validate(card, true); err != nil {
		t.Fatalf("merged card is invalid: %v", err)
	}
}

func TestStructuredEditClearingAFieldRemovesOnlyThatProperty(t *testing.T) {
	svc := newRichContactService(t)
	form := richCardForm()
	form.Email = ""
	form.Phone = ""
	form.Company = ""
	form.Notes = ""
	form.Birthday = ""
	c, _, err := svc.UpdateContact(context.Background(), owner, 1, "rich", UpsertInput{Structured: &form})
	if err != nil {
		t.Fatalf("UpdateContact(): %v", err)
	}
	card := c.RawVCard
	for _, gone := range []string{"item1.EMAIL;TYPE=work:ada@work.example", "item1.X-ABLabel:_$!<Work>!$_", "TEL;TYPE=cell;PREF=1:+15551234,,99;ext=5", "ORG:Analytical Engines;Research", `NOTE:first\nsecond`, "BDAY:18151210"} {
		if containsContentLine(card, gone) {
			t.Errorf("cleared field left %q:\n%s", gone, card)
		}
	}
	for _, kept := range []string{"EMAIL;TYPE=home:ada@home.example", "TEL;TYPE=work:+15550000", "ADR;TYPE=home:;;12 St James's Sq;London;;SW1Y;UK"} {
		if !containsContentLine(card, kept) {
			t.Errorf("clearing the form's field removed %q:\n%s", kept, card)
		}
	}
}

// A vCard 3.0 card gains the properties the form adds in 3.0 form, and a card
// without N gains one, since 3.0 requires it.
func TestStructuredEditAddsMissingPropertiesInTheCardsVersion(t *testing.T) {
	svc, _ := newTestService()
	contacts := svc.store.Contacts.(*fakeContacts)
	contacts.items["1:v3"] = store.Contact{AddressBookID: 1, UID: "v3", ResourceName: "v3", RawVCard: "BEGIN:VCARD\r\nVERSION:3.0\r\nUID:v3\r\nFN:Old\r\nX-CUSTOM:keep\r\nEND:VCARD\r\n"}
	form := StructuredInput{DisplayName: "New", Email: "a,b@example.com", Phone: `+1\2`, Birthday: "1990-02-03"}
	c, _, err := svc.UpdateContact(context.Background(), owner, 1, "v3", UpsertInput{Structured: &form})
	if err != nil {
		t.Fatalf("UpdateContact(): %v", err)
	}
	for _, want := range []string{"VERSION:3.0", "FN:New", "N:;;;;", "EMAIL;TYPE=INTERNET:a,b@example.com", `TEL;TYPE=CELL:+1\2`, "BDAY:1990-02-03", "X-CUSTOM:keep"} {
		if !containsContentLine(c.RawVCard, want) {
			t.Errorf("card lacks %q:\n%s", want, c.RawVCard)
		}
	}
	if err := vcard.Validate(c.RawVCard, true); err != nil {
		t.Fatalf("merged card is invalid: %v\n%s", err, c.RawVCard)
	}
}

// The web form reads TEL and EMAIL raw and posts them back, so saving the form
// twice must leave the values as they were.
func TestStructuredEditPhoneAndEmailAreIdempotent(t *testing.T) {
	svc, _ := newTestService()
	form := StructuredInput{UID: "idem", DisplayName: "Idem", Phone: `+15551234,,99;ext=5\x`, Email: `a,b;c\d@example.com`}
	c, _, err := svc.CreateContact(context.Background(), owner, 1, UpsertInput{Structured: &form})
	if err != nil {
		t.Fatalf("CreateContact(): %v", err)
	}
	for i := 0; i < 2; i++ {
		read := StructuredInput{DisplayName: "Idem", Phone: rawValue(c.RawVCard, "TEL"), Email: rawValue(c.RawVCard, "EMAIL")}
		if read.Phone != form.Phone || read.Email != form.Email {
			t.Fatalf("save %d: TEL %q EMAIL %q, want %q %q", i, read.Phone, read.Email, form.Phone, form.Email)
		}
		c, _, err = svc.UpdateContact(context.Background(), owner, 1, "idem", UpsertInput{Structured: &read})
		if err != nil {
			t.Fatalf("UpdateContact(): %v", err)
		}
	}
}

func TestStructuredEditRefusesAnUnrepresentableBirthdayOnlyWhenChanged(t *testing.T) {
	svc, _ := newTestService()
	contacts := svc.store.Contacts.(*fakeContacts)
	contacts.items["1:t"] = store.Contact{AddressBookID: 1, UID: "t", ResourceName: "t", RawVCard: "BEGIN:VCARD\r\nVERSION:4.0\r\nUID:t\r\nFN:T\r\nBDAY;VALUE=text:circa 1800\r\nEND:VCARD\r\n"}
	form := StructuredInput{DisplayName: "T"}
	c, _, err := svc.UpdateContact(context.Background(), owner, 1, "t", UpsertInput{Structured: &form})
	if err != nil {
		t.Fatalf("UpdateContact(): %v", err)
	}
	if !containsContentLine(c.RawVCard, "BDAY;VALUE=text:circa 1800") {
		t.Fatalf("a birthday the form cannot show was dropped:\n%s", c.RawVCard)
	}
	form.Birthday = "--02-29"
	c, _, err = svc.UpdateContact(context.Background(), owner, 1, "t", UpsertInput{Structured: &form})
	if err != nil {
		t.Fatalf("UpdateContact(): %v", err)
	}
	if !containsContentLine(c.RawVCard, "BDAY:--0229") {
		t.Fatalf("new birthday not written in vCard 4.0 form:\n%s", c.RawVCard)
	}
}

func containsContentLine(card, want string) bool {
	for _, line := range vcard.ContentLines(card) {
		if line == want {
			return true
		}
	}
	return false
}

func rawValue(card, name string) string {
	for _, line := range vcard.ContentLines(card) {
		if parsed, ok := vcard.ParseLine(line); ok && parsed.Is(name) {
			return parsed.Value
		}
	}
	return ""
}

func TestStructuredRefusalsNameTheirField(t *testing.T) {
	tests := []struct {
		input StructuredInput
		field string
		text  string
	}{
		{input: StructuredInput{DisplayName: "B", Email: "a@b\r\nX:1"}, field: "email", text: "bad request: email must not contain control characters"},
		{input: StructuredInput{DisplayName: "B", Notes: "a\x00"}, field: "notes", text: "bad request: notes must not contain control characters"},
		{input: StructuredInput{DisplayName: "  "}, field: "displayName", text: "bad request: displayName is required"},
		{input: StructuredInput{DisplayName: "B", UID: "a/b"}, field: "uid", text: "bad request: uid may contain only letters, digits and - . _ ~ @ : + ="},
	}
	for _, tt := range tests {
		_, _, err := structuredPayload(&tt.input, "")
		var fieldErr *FieldError
		if !errors.As(err, &fieldErr) || fieldErr.Field != tt.field {
			t.Errorf("%+v: error = %#v, want a FieldError for %q", tt.input, err, tt.field)
			continue
		}
		if !errors.Is(err, ErrBadRequest) {
			t.Errorf("%+v: FieldError does not unwrap to ErrBadRequest", tt.input)
		}
		if err.Error() != tt.text {
			t.Errorf("%+v: text = %q, want %q", tt.input, err.Error(), tt.text)
		}
	}
}

func TestBirthdayRefusalIsTypedOnCreateAndEdit(t *testing.T) {
	if _, _, err := structuredPayload(&StructuredInput{DisplayName: "B", Birthday: "1990-02-31"}, ""); !errors.Is(err, utils.ErrInvalidBirthday) || !errors.Is(err, ErrBadRequest) {
		t.Errorf("create: error = %v, want ErrInvalidBirthday and ErrBadRequest", err)
	}
	svc := newRichContactService(t)
	form := richCardForm()
	form.Birthday = "--13-40"
	if _, _, err := svc.UpdateContact(context.Background(), owner, 1, "rich", UpsertInput{Structured: &form}); !errors.Is(err, utils.ErrInvalidBirthday) || !errors.Is(err, ErrBadRequest) {
		t.Errorf("edit: error = %v, want ErrInvalidBirthday and ErrBadRequest", err)
	}
}

func storeCard(svc *Service, uid, card string) {
	svc.store.Contacts.(*fakeContacts).items["1:"+uid] = store.Contact{AddressBookID: 1, UID: uid, ResourceName: uid, ETag: "stored", RawVCard: card}
}

// Apple clients write a year-less birthday as a stand-in year named in
// X-APPLE-OMIT-YEAR. The form shows it without a year, so saving it back
// unchanged leaves the line as it was.
func TestStructuredEditReadsAppleOmitYearBirthdays(t *testing.T) {
	for _, bday := range []string{"BDAY;X-APPLE-OMIT-YEAR=1604:1604-03-15", `BDAY;X-APPLE-OMIT-YEAR="1604":1604-03-15`, "BDAY;X-APPLE-OMIT-YEAR=1604;VALUE=date:1604-03-15"} {
		t.Run(bday, func(t *testing.T) {
			svc, _ := newTestService()
			storeCard(svc, "ap", "BEGIN:VCARD\r\nVERSION:3.0\r\nUID:ap\r\nFN:Ap\r\nN:;;;;\r\n"+bday+"\r\nEND:VCARD\r\n")
			form := StructuredInput{DisplayName: "Ap", Birthday: "--03-15"}
			c, _, err := svc.UpdateContact(context.Background(), owner, 1, "ap", UpsertInput{Structured: &form})
			if err != nil {
				t.Fatalf("UpdateContact(): %v", err)
			}
			if !containsContentLine(c.RawVCard, bday) {
				t.Fatalf("unchanged birthday rewritten:\n%s", c.RawVCard)
			}

			form.Birthday = "--04-01"
			c, _, err = svc.UpdateContact(context.Background(), owner, 1, "ap", UpsertInput{Structured: &form})
			if err != nil {
				t.Fatalf("UpdateContact(): %v", err)
			}
			if !containsContentLine(c.RawVCard, "BDAY;X-APPLE-OMIT-YEAR=1604:1604-04-01") && !containsContentLine(c.RawVCard, `BDAY;X-APPLE-OMIT-YEAR="1604":1604-04-01`) && !containsContentLine(c.RawVCard, "BDAY;X-APPLE-OMIT-YEAR=1604;VALUE=date:1604-04-01") {
				t.Fatalf("changed year-less birthday not kept in Apple form:\n%s", c.RawVCard)
			}

			form.Birthday = "1990-04-01"
			c, _, err = svc.UpdateContact(context.Background(), owner, 1, "ap", UpsertInput{Structured: &form})
			if err != nil {
				t.Fatalf("UpdateContact(): %v", err)
			}
			if !containsContentLine(c.RawVCard, "BDAY:1990-04-01") && !containsContentLine(c.RawVCard, "BDAY;VALUE=date:1990-04-01") {
				t.Fatalf("birthday with a year still carries the omit-year form:\n%s", c.RawVCard)
			}
		})
	}
}

// A date-time BDAY shows as its date in the form, so the date is what an edit
// compares, keeps and clears.
func TestStructuredEditTreatsADateTimeBirthdayAsItsDate(t *testing.T) {
	const line = "BDAY:1990-01-15T00:00:00Z"
	svc, _ := newTestService()
	storeCard(svc, "dt", "BEGIN:VCARD\r\nVERSION:3.0\r\nUID:dt\r\nFN:Dt\r\nN:;;;;\r\n"+line+"\r\nEND:VCARD\r\n")
	form := StructuredInput{DisplayName: "Dt", Birthday: "1990-01-15"}
	c, _, err := svc.UpdateContact(context.Background(), owner, 1, "dt", UpsertInput{Structured: &form})
	if err != nil {
		t.Fatalf("UpdateContact(): %v", err)
	}
	if !containsContentLine(c.RawVCard, line) {
		t.Fatalf("unchanged date-time birthday rewritten:\n%s", c.RawVCard)
	}
	form.Birthday = ""
	c, _, err = svc.UpdateContact(context.Background(), owner, 1, "dt", UpsertInput{Structured: &form})
	if err != nil {
		t.Fatalf("UpdateContact(): %v", err)
	}
	if strings.Contains(c.RawVCard, "BDAY") {
		t.Fatalf("cleared date-time birthday kept:\n%s", c.RawVCard)
	}
}

// Year 0000 is how some clients write a birthday without a year; the form
// shows it without one and posts it back as --MM-DD.
func TestStructuredEditReadsYearZeroAsNoYear(t *testing.T) {
	const line = "BDAY:0000-04-15"
	svc, _ := newTestService()
	storeCard(svc, "z", "BEGIN:VCARD\r\nVERSION:3.0\r\nUID:z\r\nFN:Z\r\nN:;;;;\r\n"+line+"\r\nEND:VCARD\r\n")
	form := StructuredInput{DisplayName: "Z", Birthday: "--04-15"}
	c, _, err := svc.UpdateContact(context.Background(), owner, 1, "z", UpsertInput{Structured: &form})
	if err != nil {
		t.Fatalf("UpdateContact(): %v", err)
	}
	if !containsContentLine(c.RawVCard, line) {
		t.Fatalf("unchanged year-zero birthday rewritten:\n%s", c.RawVCard)
	}
	form.Birthday = "--04-16"
	c, _, err = svc.UpdateContact(context.Background(), owner, 1, "z", UpsertInput{Structured: &form})
	if err != nil {
		t.Fatalf("UpdateContact(): %v", err)
	}
	if !containsContentLine(c.RawVCard, "BDAY:--04-16") {
		t.Fatalf("changed birthday not written year-less:\n%s", c.RawVCard)
	}
}

// structuredPayload builds a structured payload the way CreateContact does.
func structuredPayload(input *StructuredInput, expectedUID string) (string, string, error) {
	return normalizeVCardPayload(UpsertInput{Structured: input}, expectedUID, "")
}

const android21Export = "BEGIN:VCARD\r\n" +
	"VERSION:2.1\r\n" +
	"N;CHARSET=UTF-8;ENCODING=QUOTED-PRINTABLE:M=C3=BCller;J=C3=BCrgen;;;\r\n" +
	"FN;CHARSET=UTF-8;ENCODING=QUOTED-PRINTABLE:J=C3=BCrgen M=C3=BCller\r\n" +
	"TEL;CELL;PREF:+49 170 1234567\r\n" +
	"NOTE;ENCODING=QUOTED-PRINTABLE:Erste Zeile=0D=0A=\r\n" +
	"Zweite Zeile\r\n" +
	"END:VCARD\r\n"

// A VCF import reports every card it could not store and why, and converts
// vCard 2.1 cards rather than refusing them.
func TestImportVCardsConvertsVCard21AndReportsSkips(t *testing.T) {
	svc, _ := newTestService()
	file := android21Export +
		"BEGIN:VCARD\r\nVERSION:3.0\r\nUID:plain\r\nFN:Plain\r\nEND:VCARD\r\n" +
		"BEGIN:VCARD\r\nVERSION:3.0\r\nUID:nameless\r\nEND:VCARD\r\n" +
		"BEGIN:VCARD\r\nVERSION:2.1\r\nFN;CHARSET=SHIFT_JIS;ENCODING=QUOTED-PRINTABLE:=82=A0\r\nEND:VCARD\r\n"
	result, err := svc.ImportVCards(context.Background(), owner, 1, file)
	if err != nil {
		t.Fatalf("ImportVCards: %v", err)
	}
	if result.Imported != 2 || len(result.Skipped) != 2 {
		t.Fatalf("result = %+v, want 2 imported and 2 skipped", result)
	}
	if result.Skipped[0].Card != 3 || !strings.Contains(result.Skipped[0].Reason, "FN") {
		t.Errorf("first skip = %+v, want card 3 lacking FN", result.Skipped[0])
	}
	if result.Skipped[1].Card != 4 || !strings.Contains(result.Skipped[1].Reason, "SHIFT_JIS") {
		t.Errorf("second skip = %+v, want card 4 naming its charset", result.Skipped[1])
	}
	var converted *store.Contact
	for _, c := range svc.store.Contacts.(*fakeContacts).items {
		if strings.Contains(c.RawVCard, "Müller") {
			cc := c
			converted = &cc
		}
	}
	if converted == nil {
		t.Fatal("the vCard 2.1 card was not stored")
	}
	for _, want := range []string{"VERSION:3.0", "FN:Jürgen Müller", "TEL;TYPE=CELL,PREF:+49 170 1234567", `NOTE:Erste Zeile\nZweite Zeile`} {
		if !containsContentLine(converted.RawVCard, want) {
			t.Errorf("converted card lacks %q:\n%s", want, converted.RawVCard)
		}
	}
	if err := vcard.Validate(converted.RawVCard, true); err != nil {
		t.Errorf("stored card is invalid: %v", err)
	}
}

func TestImportVCardsReplacesAKnownUID(t *testing.T) {
	svc, _ := newTestService()
	result, err := svc.ImportVCards(context.Background(), owner, 1, "BEGIN:VCARD\r\nVERSION:3.0\r\nUID:c1\r\nFN:Alice Again\r\nEND:VCARD\r\n")
	if err != nil || result.Imported != 1 {
		t.Fatalf("ImportVCards = %+v, %v", result, err)
	}
	if c := storedContact(t, svc, 1, "c1"); !strings.Contains(c.RawVCard, "Alice Again") {
		t.Fatalf("known UID not replaced:\n%s", c.RawVCard)
	}
}

func TestImportVCardsRequiresWriteAccess(t *testing.T) {
	svc, _ := newTestService()
	if _, err := svc.ImportVCards(context.Background(), stranger, 1, android21Export); !errors.Is(err, ErrNotFound) {
		t.Fatalf("stranger import error = %v, want ErrNotFound", err)
	}
}

// A structured edit of a stored vCard 2.1 card converts it first, so nothing
// the form does not show is lost.
func TestStructuredEditOfAVCard21CardKeepsItsFields(t *testing.T) {
	svc, _ := newTestService()
	storeCard(svc, "old", "BEGIN:VCARD\r\nVERSION:2.1\r\nUID:old\r\nN:Doe;Jane;;;\r\nFN:Jane Doe\r\nTEL;CELL:+1 555\r\nADR;HOME:;;1 Main St;Town;;12345;US\r\nEND:VCARD\r\n")
	form := StructuredInput{DisplayName: "Jane Q Doe", FirstName: "Jane", LastName: "Doe", Phone: "+1 555"}
	c, _, err := svc.UpdateContact(context.Background(), owner, 1, "old", UpsertInput{Structured: &form})
	if err != nil {
		t.Fatalf("UpdateContact(): %v", err)
	}
	for _, want := range []string{"VERSION:3.0", "FN:Jane Q Doe", "TEL;TYPE=CELL:+1 555", "ADR;TYPE=HOME:;;1 Main St;Town;;12345;US"} {
		if !containsContentLine(c.RawVCard, want) {
			t.Errorf("edited card lacks %q:\n%s", want, c.RawVCard)
		}
	}
}

// A stored card the merge cannot read is refused rather than rebuilt from the
// form, which would drop whatever the form does not show.
func TestStructuredEditRefusesACardItCannotMerge(t *testing.T) {
	svc, _ := newTestService()
	storeCard(svc, "two", "BEGIN:VCARD\r\nVERSION:3.0\r\nVERSION:4.0\r\nUID:two\r\nFN:Two\r\nADR:;;x;;;;\r\nEND:VCARD\r\n")
	form := StructuredInput{DisplayName: "Two"}
	_, _, err := svc.UpdateContact(context.Background(), owner, 1, "two", UpsertInput{Structured: &form})
	if !errors.Is(err, ErrCardNotEditable) || !errors.Is(err, ErrBadRequest) {
		t.Fatalf("UpdateContact() error = %v, want ErrCardNotEditable", err)
	}
	if c := storedContact(t, svc, 1, "two"); !strings.Contains(c.RawVCard, "ADR") {
		t.Fatalf("card was rewritten:\n%s", c.RawVCard)
	}
}

// concurrentWriteContacts commits a CardDAV client's write the first time the
// service looks the contact up by resource name, which is after the edit has
// read the card it merges into.
type concurrentWriteContacts struct {
	*fakeContacts
	write func()
}

func (c *concurrentWriteContacts) GetByResourceName(ctx context.Context, bookID int64, name string) (*store.Contact, error) {
	if c.write != nil {
		write := c.write
		c.write = nil
		write()
	}
	return c.fakeContacts.GetByResourceName(ctx, bookID, name)
}

func TestStructuredEditKeepsAConcurrentWrite(t *testing.T) {
	svc, _ := newTestService()
	base := svc.store.Contacts.(*fakeContacts)
	storeCard(svc, "cc", "BEGIN:VCARD\r\nVERSION:3.0\r\nUID:cc\r\nFN:Before\r\nN:;;;;\r\nEND:VCARD\r\n")
	svc.store.Contacts = &concurrentWriteContacts{fakeContacts: base, write: func() {
		c := base.items["1:cc"]
		c.RawVCard = "BEGIN:VCARD\r\nVERSION:3.0\r\nUID:cc\r\nFN:Before\r\nN:;;;;\r\nX-CONCURRENT:kept\r\nEND:VCARD\r\n"
		c.ETag = "concurrent"
		base.items["1:cc"] = c
	}}
	form := StructuredInput{DisplayName: "After"}
	c, _, err := svc.UpdateContact(context.Background(), owner, 1, "cc", UpsertInput{Structured: &form})
	if err != nil {
		t.Fatalf("UpdateContact(): %v", err)
	}
	if !containsContentLine(c.RawVCard, "X-CONCURRENT:kept") || !containsContentLine(c.RawVCard, "FN:After") {
		t.Fatalf("edit lost the concurrent write or its own change:\n%s", c.RawVCard)
	}
}

// A caller's own If-Match names the version it read, so a concurrent write
// fails its precondition instead of being retried over.
func TestStructuredEditWithIfMatchFailsOnAConcurrentWrite(t *testing.T) {
	svc, _ := newTestService()
	base := svc.store.Contacts.(*fakeContacts)
	storeCard(svc, "cc", "BEGIN:VCARD\r\nVERSION:3.0\r\nUID:cc\r\nFN:Before\r\nN:;;;;\r\nEND:VCARD\r\n")
	svc.store.Contacts = &concurrentWriteContacts{fakeContacts: base, write: func() {
		c := base.items["1:cc"]
		c.RawVCard = strings.Replace(c.RawVCard, "FN:Before", "FN:Other", 1)
		c.ETag = "concurrent"
		base.items["1:cc"] = c
	}}
	form := StructuredInput{DisplayName: "After"}
	_, _, err := svc.UpdateContact(context.Background(), owner, 1, "cc", UpsertInput{Structured: &form, IfMatch: `"stored"`})
	if !errors.Is(err, ErrPreconditionFailed) {
		t.Fatalf("UpdateContact() error = %v, want ErrPreconditionFailed", err)
	}
}

func TestImportSkipsCarryACode(t *testing.T) {
	svc, _ := newTestService()
	svc.store.Contacts.(*fakeContacts).items["1:x"] = store.Contact{AddressBookID: 1, UID: "x", ResourceName: "taken"}
	file := "BEGIN:VCARD\r\nVERSION:3.0\r\nUID:a\r\nEND:VCARD\r\n" +
		"BEGIN:VCARD\r\nVERSION:2.1\r\nTEL:1\r\nEND:VCARD\r\n" +
		"BEGIN:VCARD\r\nVERSION:2.1\r\nFN;CHARSET=SHIFT_JIS;ENCODING=QUOTED-PRINTABLE:=82=A0\r\nEND:VCARD\r\n" +
		"BEGIN:VCARD\r\nVERSION:2.1\r\nFN;ENCODING=QUOTED-PRINTABLE:a=ZZ\r\nEND:VCARD\r\n" +
		"BEGIN:VCARD\r\nVERSION:5.0\r\nFN:v\r\nEND:VCARD\r\n" +
		"BEGIN:VCARD\r\nVERSION:3.0\r\nUID:a\r\nUID:b\r\nFN:u\r\nEND:VCARD\r\n" +
		"BEGIN:VCARD\r\nVERSION:3.0\r\nUID:\r\nFN:Empty UID\r\nEND:VCARD\r\n"
	result, err := svc.ImportVCards(context.Background(), owner, 1, file)
	if err != nil {
		t.Fatalf("ImportVCards: %v", err)
	}
	want := []ImportSkipCode{
		ImportSkipMissingFN, ImportSkipMissingFN, ImportSkipUnsupportedCharset,
		ImportSkipMalformed, ImportSkipMalformed, ImportSkipInvalid, ImportSkipInvalid,
	}
	if len(result.Skipped) != len(want) {
		t.Fatalf("skipped = %+v, want %d skips", result.Skipped, len(want))
	}
	for i, skip := range result.Skipped {
		if skip.Code != want[i] {
			t.Errorf("card %d: code = %q, want %q (reason %q)", skip.Card, skip.Code, want[i], skip.Reason)
		}
		if skip.Reason == "" {
			t.Errorf("card %d: no reason", skip.Card)
		}
	}

	conflict, err := svc.ImportVCards(context.Background(), owner, 1, "BEGIN:VCARD\r\nVERSION:3.0\r\nUID:taken\r\nFN:T\r\nEND:VCARD\r\n")
	if err != nil {
		t.Fatalf("ImportVCards: %v", err)
	}
	if len(conflict.Skipped) != 1 || conflict.Skipped[0].Code != ImportSkipDuplicateUID {
		t.Fatalf("conflict skips = %+v, want ImportSkipDuplicateUID", conflict.Skipped)
	}
}

func TestNewStructuredContactUIDIsBounded(t *testing.T) {
	_, _, err := structuredPayload(&StructuredInput{UID: strings.Repeat("u", vcard.MaxUIDOctets+1), DisplayName: "Bob"}, "")
	var fieldErr *FieldError
	if !errors.As(err, &fieldErr) || fieldErr.Field != "uid" {
		t.Fatalf("error = %v, want a FieldError for uid", err)
	}
}
