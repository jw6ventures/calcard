package utils

import (
	"errors"
	"strings"
	"testing"
	"unicode/utf8"

	"github.com/jw6ventures/calcard/internal/ical"
)

// vcardProperties returns the property name of every content line in a card,
// which is what a reader sees after unfolding. A value that closed its own
// content line shows up here as a property the builder never writes.
func vcardProperties(t *testing.T, card string) []string {
	t.Helper()
	var names []string
	for _, line := range ical.UnfoldLines(card) {
		if strings.TrimSpace(line) == "" {
			continue
		}
		name, _, ok := strings.Cut(line, ":")
		if !ok {
			t.Errorf("content line has no value delimiter: %q", line)
			continue
		}
		name, _, _ = strings.Cut(name, ";")
		names = append(names, name)
	}
	return names
}

// A CR or LF in any value BuildVCard interpolates ends the content line it is
// written into, so the remainder would be read back as properties -- or whole
// cards -- of the caller's choosing. Every value is therefore escaped or held to
// the grammar of its property, and a value that cannot be carried either way is
// refused.
func TestBuildVCardCannotBeMadeToWriteAnExtraContentLine(t *testing.T) {
	written := map[string]struct{}{
		"BEGIN": {}, "VERSION": {}, "UID": {}, "FN": {}, "N": {},
		"ORG": {}, "EMAIL": {}, "TEL": {}, "BDAY": {}, "NOTE": {}, "REV": {}, "END": {},
	}
	const planted = "\r\nEND:VCARD\r\nBEGIN:VCARD\r\nVERSION:3.0\r\nUID:planted\r\nFN:Planted"

	tests := []struct {
		name                                                                      string
		uid, displayName, firstName, lastName, email, phone, birthday, notes, org string
		wantErr                                                                   bool
	}{
		{name: "uid opens a property", uid: "a\r\nEMAIL;TYPE=INTERNET:pwn@evil.test", displayName: "Bob", wantErr: true},
		{name: "uid carries a bare CR", uid: "a\rX-INJECTED:1", displayName: "Bob", wantErr: true},
		{name: "uid carries NUL", uid: "a\x00b", displayName: "Bob", wantErr: true},
		{name: "uid is padded", uid: " a ", displayName: "Bob", wantErr: true},
		{name: "uid is empty", displayName: "Bob", wantErr: true},
		{name: "bday opens whole cards", uid: "u1", displayName: "Bob", birthday: "--01-01" + planted, wantErr: true},
		{name: "bday opens a property", uid: "u1", displayName: "Bob", birthday: "--01-01\r\nX-INJECTED:1", wantErr: true},
		{name: "display name carries a bare CR", uid: "u1", displayName: "Alice\rEMAIL;TYPE=INTERNET:x@evil.test"},
		{name: "display name carries CRLF", uid: "u1", displayName: "Alice" + planted},
		{name: "given name carries CRLF", uid: "u1", displayName: "Bob", firstName: "A\r\nX-INJECTED:1"},
		{name: "family name carries CRLF", uid: "u1", displayName: "Bob", lastName: "A\r\nX-INJECTED:1"},
		{name: "email opens a property", uid: "u1", displayName: "Bob", email: "x@y.z\r\nX-INJECTED:1", wantErr: true},
		{name: "phone opens a property", uid: "u1", displayName: "Bob", phone: "+1\r\nX-INJECTED:1", wantErr: true},
		{name: "phone carries a bare LF", uid: "u1", displayName: "Bob", phone: "+1\nX-INJECTED:1", wantErr: true},
		{name: "notes open a property", uid: "u1", displayName: "Bob", notes: "hi\r\nX-INJECTED:1"},
		{name: "org opens a property", uid: "u1", displayName: "Bob", org: "Acme\r\nX-INJECTED:1"},
		{name: "display name carries a control character", uid: "u1", displayName: "A\x01B", wantErr: true},
		{name: "notes carry DEL", uid: "u1", displayName: "Bob", notes: "C\x7fD", wantErr: true},
		{name: "org carries a vertical tab", uid: "u1", displayName: "Bob", org: "E\vF", wantErr: true},
		{name: "email carries a form feed", uid: "u1", displayName: "Bob", email: "g\fh@y.z", wantErr: true},
		{name: "phone carries an escape sequence", uid: "u1", displayName: "Bob", phone: "\x1b[0m", wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			card, err := BuildVCard(tt.uid, tt.displayName, tt.firstName, tt.lastName, tt.email, tt.phone, tt.birthday, tt.notes, tt.org)
			if tt.wantErr {
				if err == nil {
					t.Fatalf("BuildVCard accepted the value:\n%s", card)
				}
				if card != "" {
					t.Errorf("BuildVCard returned a card alongside an error:\n%s", card)
				}
				return
			}
			if err != nil {
				t.Fatalf("BuildVCard: %v", err)
			}
			var begins, ends int
			for _, name := range vcardProperties(t, card) {
				switch name {
				case "BEGIN":
					begins++
				case "END":
					ends++
				}
				if _, ok := written[name]; !ok {
					t.Errorf("card holds property %q, which BuildVCard never writes:\n%s", name, card)
				}
			}
			if begins != 1 || ends != 1 {
				t.Errorf("card holds %d BEGIN and %d END content lines, want 1 of each:\n%s", begins, ends, card)
			}
			for _, control := range []string{"\x00", "\x01", "\v", "\f", "\x1b", "\x7f"} {
				if strings.Contains(card, control) {
					t.Errorf("card holds control character %q:\n%q", control, card)
				}
			}
		})
	}
}

// A card is folded at 75 octets, so no value can make a content line a reader
// has to guess the end of. Folding a multi-octet character in half would make
// the unfolded value differ from the one that was written.
func TestBuildVCardFoldsLongContentLines(t *testing.T) {
	notes := strings.Repeat("é", 200)
	card, err := BuildVCard("u1", strings.Repeat("Ada ", 40), "", "", "", "", "", notes, "")
	if err != nil {
		t.Fatalf("BuildVCard: %v", err)
	}
	for _, line := range strings.Split(strings.TrimSuffix(card, "\r\n"), "\r\n") {
		if len(line) > 75 {
			t.Errorf("content line is %d octets, want at most 75: %q", len(line), line)
		}
		if !utf8.ValidString(line) {
			t.Errorf("fold split a multi-octet character: %q", line)
		}
	}
	var found bool
	for _, line := range ical.UnfoldLines(card) {
		if value, ok := strings.CutPrefix(line, "NOTE:"); ok {
			found = true
			if value != notes {
				t.Errorf("unfolded NOTE = %q, want %q", value, notes)
			}
		}
	}
	if !found {
		t.Error("card has no NOTE line")
	}
}

// ExtractVCardUID reads a card this package folds, so it has to unfold before
// it reads or it returns the first 75 octets of a long UID.
func TestExtractVCardUIDUnfolds(t *testing.T) {
	uid := strings.Repeat("a", 120) + "@calcard"
	card, err := BuildVCard(uid, "Bob", "", "", "", "", "", "", "")
	if err != nil {
		t.Fatalf("BuildVCard: %v", err)
	}
	if got := ExtractVCardUID(card); got != uid {
		t.Errorf("ExtractVCardUID() = %q, want %q", got, uid)
	}
}

func TestBuildVCard(t *testing.T) {
	tests := []struct {
		name        string
		uid         string
		displayName string
		firstName   string
		lastName    string
		email       string
		phone       string
		birthday    string
		notes       string
		company     string
		wantFields  []string
	}{
		{
			name:        "complete vCard",
			uid:         "test-uid@calcard",
			displayName: "John Doe",
			firstName:   "John",
			lastName:    "Doe",
			email:       "john@example.com",
			phone:       "+1234567890",
			birthday:    "1990-01-15",
			notes:       "Test contact",
			company:     "Acme Corp",
			wantFields: []string{
				"BEGIN:VCARD",
				"VERSION:3.0",
				"UID:test-uid@calcard",
				"FN:John Doe",
				"N:Doe;John;;;",
				"ORG:Acme Corp",
				"EMAIL;TYPE=INTERNET:john@example.com",
				"TEL;TYPE=CELL:+1234567890",
				"BDAY:1990-01-15",
				"NOTE:Test contact",
				"REV:",
				"END:VCARD",
			},
		},
		{
			name:        "minimal vCard",
			uid:         "minimal@calcard",
			displayName: "Jane Smith",
			firstName:   "Jane",
			lastName:    "Smith",
			wantFields: []string{
				"BEGIN:VCARD",
				"VERSION:3.0",
				"UID:minimal@calcard",
				"FN:Jane Smith",
				"N:Smith;Jane;;;",
				"END:VCARD",
			},
		},
		{
			name:        "vCard with birthday no year",
			uid:         "noyear@calcard",
			displayName: "No Year",
			firstName:   "No",
			lastName:    "Year",
			birthday:    "--12-25",
			wantFields: []string{
				"BDAY:--12-25",
			},
		},
		{
			name:        "vCard with special characters",
			uid:         "special@calcard",
			displayName: "Test;User,Name",
			firstName:   "Test;First",
			lastName:    "Test,Last",
			notes:       "Line1\nLine2",
			wantFields: []string{
				"FN:Test\\;User\\,Name",
				"N:Test\\,Last;Test\\;First;;;",
				"NOTE:Line1\\nLine2",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			vcard, err := BuildVCard(tt.uid, tt.displayName, tt.firstName, tt.lastName, tt.email, tt.phone, tt.birthday, tt.notes, tt.company)
			if err != nil {
				t.Fatalf("BuildVCard: %v", err)
			}

			for _, field := range tt.wantFields {
				if !strings.Contains(vcard, field) {
					t.Errorf("BuildVCard() missing expected field: %s\nGot:\n%s", field, vcard)
				}
			}

			// Verify it starts and ends correctly
			if !strings.HasPrefix(vcard, "BEGIN:VCARD\r\n") {
				t.Error("BuildVCard() should start with BEGIN:VCARD")
			}
			if !strings.HasSuffix(vcard, "END:VCARD\r\n") {
				t.Error("BuildVCard() should end with END:VCARD")
			}
		})
	}
}

func TestEscapeVCardValue(t *testing.T) {
	tests := []struct {
		name  string
		input string
		want  string
	}{
		{
			name:  "no special characters",
			input: "Simple text",
			want:  "Simple text",
		},
		{
			name:  "backslash",
			input: "Path\\to\\file",
			want:  "Path\\\\to\\\\file",
		},
		{
			name:  "semicolon",
			input: "Last;First",
			want:  "Last\\;First",
		},
		{
			name:  "comma",
			input: "City,State",
			want:  "City\\,State",
		},
		{
			name:  "newline",
			input: "Line1\nLine2",
			want:  "Line1\\nLine2",
		},
		{
			name:  "multiple special chars",
			input: "Complex;Value,With\\Chars\nAnd newlines",
			want:  "Complex\\;Value\\,With\\\\Chars\\nAnd newlines",
		},
		{
			name:  "empty string",
			input: "",
			want:  "",
		},
		{
			name:  "bare carriage return",
			input: "Line1\rLine2",
			want:  "Line1\\nLine2",
		},
		{
			name:  "carriage return line feed is one break",
			input: "Line1\r\nLine2",
			want:  "Line1\\nLine2",
		},

		{
			name:  "tab survives",
			input: "A\tB",
			want:  "A\tB",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := EscapeVCardValue(tt.input)
			if err != nil {
				t.Fatalf("EscapeVCardValue(%q): %v", tt.input, err)
			}
			if got != tt.want {
				t.Errorf("EscapeVCardValue() = %q, want %q", got, tt.want)
			}
		})
	}
}

func TestBuildVCard_EmailAndPhone(t *testing.T) {
	tests := []struct {
		name  string
		email string
		phone string
		want  string
		skip  string
	}{
		{
			name:  "with email",
			email: "test@example.com",
			want:  "EMAIL;TYPE=INTERNET:test@example.com",
		},
		{
			name: "without email",
			skip: "EMAIL",
		},
		{
			name:  "with phone",
			phone: "+1234567890",
			want:  "TEL;TYPE=CELL:+1234567890",
		},
		{
			name: "without phone",
			skip: "TEL",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			vcard, err := BuildVCard("test-uid", "Test User", "Test", "User", tt.email, tt.phone, "", "", "")
			if err != nil {
				t.Fatalf("BuildVCard: %v", err)
			}

			if tt.want != "" && !strings.Contains(vcard, tt.want) {
				t.Errorf("BuildVCard() should contain %q", tt.want)
			}
			if tt.skip != "" && strings.Contains(vcard, tt.skip+":") {
				t.Errorf("BuildVCard() should not contain %q when field is empty", tt.skip)
			}
		})
	}
}

// BDAY carries an RFC 2426 Section 3.1.5 date, either complete or in the
// --MM-DD form that omits the year. A birthday that is not one leaves no date to
// write, so it is refused rather than left out of the card: a contact stored
// without the birthday its caller named is a contact that was not stored as
// sent. A caller naming no birthday is naming a contact without one, which is
// not an error.
func TestBuildVCard_Birthday(t *testing.T) {
	tests := []struct {
		name     string
		birthday string
		want     string
		wantErr  bool
	}{
		{
			name:     "full date",
			birthday: "1990-01-15",
			want:     "BDAY:1990-01-15",
		},
		{
			name:     "no year format",
			birthday: "--12-25",
			want:     "BDAY:--12-25",
		},
		{
			name:     "no year format on a leap day",
			birthday: "--02-29",
			want:     "BDAY:--02-29",
		},
		{
			name:     "empty birthday",
			birthday: "",
		},
		{
			name:     "not a date",
			birthday: "invalid-date",
			wantErr:  true,
		},
		{
			name:     "month and day out of range",
			birthday: "--13-45",
			wantErr:  true,
		},
		{
			name:     "day the month does not have",
			birthday: "1990-02-31",
			wantErr:  true,
		},
		{
			name:     "no year format the month does not have",
			birthday: "--02-31",
			wantErr:  true,
		},
		{
			name:     "date followed by other text",
			birthday: "1990-01-15 or so",
			wantErr:  true,
		},
		{
			name:     "year alone",
			birthday: "1990",
			wantErr:  true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			vcard, err := BuildVCard("test-uid", "Test User", "Test", "User", "", "", tt.birthday, "", "")
			if tt.wantErr {
				if err == nil {
					t.Fatalf("BuildVCard accepted birthday %q:\n%s", tt.birthday, vcard)
				}
				if vcard != "" {
					t.Errorf("BuildVCard returned a card alongside an error:\n%s", vcard)
				}
				return
			}
			if err != nil {
				t.Fatalf("BuildVCard: %v", err)
			}
			if tt.want == "" {
				if strings.Contains(vcard, "BDAY") {
					t.Errorf("card holds a BDAY line for birthday %q:\n%s", tt.birthday, vcard)
				}
				return
			}
			if !strings.Contains(vcard, tt.want) {
				t.Errorf("BuildVCard() should contain %q\nGot:\n%s", tt.want, vcard)
			}
		})
	}
}

func TestBuildVCard_Notes(t *testing.T) {
	tests := []struct {
		name  string
		notes string
		want  string
	}{
		{
			name:  "simple notes",
			notes: "Important contact",
			want:  "NOTE:Important contact",
		},
		{
			name:  "notes with newline",
			notes: "Line 1\nLine 2",
			want:  "NOTE:Line 1\\nLine 2",
		},
		{
			name:  "notes with special chars",
			notes: "Text;with,special\\chars",
			want:  "NOTE:Text\\;with\\,special\\\\chars",
		},
		{
			name:  "empty notes",
			notes: "",
			want:  "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			vcard, err := BuildVCard("test-uid", "Test User", "Test", "User", "", "", "", tt.notes, "")
			if err != nil {
				t.Fatalf("BuildVCard: %v", err)
			}

			if tt.want != "" {
				if !strings.Contains(vcard, tt.want) {
					t.Errorf("BuildVCard() should contain %q\nGot:\n%s", tt.want, vcard)
				}
			} else {
				if strings.Contains(vcard, "NOTE:") {
					t.Error("BuildVCard() should not include NOTE when notes are empty")
				}
			}
		})
	}
}

func TestBuildVCard_Structure(t *testing.T) {
	vcard, err := BuildVCard("uid", "Full Name", "First", "Last", "email@test.com", "123", "2000-01-01", "Notes", "Company")
	if err != nil {
		t.Fatalf("BuildVCard: %v", err)
	}

	// Check line endings
	lines := strings.Split(vcard, "\r\n")
	if len(lines) < 5 {
		t.Errorf("BuildVCard() should have multiple lines, got %d", len(lines))
	}

	// Check required fields are present
	requiredFields := []string{
		"BEGIN:VCARD",
		"VERSION:3.0",
		"UID:",
		"FN:",
		"N:",
		"REV:",
		"END:VCARD",
	}

	for _, field := range requiredFields {
		found := false
		for _, line := range lines {
			if strings.HasPrefix(line, field) {
				found = true
				break
			}
		}
		if !found {
			t.Errorf("BuildVCard() missing required field: %s", field)
		}
	}
}

func TestBuildVCard_REVTimestamp(t *testing.T) {
	vcard1, err := BuildVCard("uid1", "Test", "T", "User", "", "", "", "", "")
	if err != nil {
		t.Fatalf("BuildVCard: %v", err)
	}
	vcard2, err := BuildVCard("uid2", "Test", "T", "User", "", "", "", "", "")
	if err != nil {
		t.Fatalf("BuildVCard: %v", err)
	}

	// Both should have REV field
	if !strings.Contains(vcard1, "REV:") {
		t.Error("BuildVCard() should include REV timestamp")
	}
	if !strings.Contains(vcard2, "REV:") {
		t.Error("BuildVCard() should include REV timestamp")
	}

	// Extract REV values
	extractREV := func(vcard string) string {
		for _, line := range strings.Split(vcard, "\r\n") {
			if strings.HasPrefix(line, "REV:") {
				return line
			}
		}
		return ""
	}

	rev1 := extractREV(vcard1)
	_ = extractREV(vcard2) // Both should have REV, just checking format of one

	// REV should be in format YYYYMMDDTHHMMSSZ
	if !strings.HasSuffix(rev1, "Z") {
		t.Error("REV timestamp should end with Z (UTC)")
	}
	if len(rev1) < len("REV:20250101T000000Z") {
		t.Errorf("REV timestamp seems too short: %s", rev1)
	}
}

func TestBuildVCard_Company(t *testing.T) {
	tests := []struct {
		name    string
		company string
		want    string
	}{
		{
			name:    "with company",
			company: "Acme Corp",
			want:    "ORG:Acme Corp",
		},
		{
			name:    "company with special chars",
			company: "Test;Company,Inc\\More",
			want:    "ORG:Test\\;Company\\,Inc\\\\More",
		},
		{
			name:    "empty company",
			company: "",
			want:    "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			vcard, err := BuildVCard("test-uid", "Test User", "Test", "User", "", "", "", "", tt.company)
			if err != nil {
				t.Fatalf("BuildVCard: %v", err)
			}

			if tt.want != "" {
				if !strings.Contains(vcard, tt.want) {
					t.Errorf("BuildVCard() should contain %q\nGot:\n%s", tt.want, vcard)
				}
			} else {
				if strings.Contains(vcard, "ORG:") {
					t.Error("BuildVCard() should not include ORG when company is empty")
				}
			}
		})
	}
}

// A UID another CardDAV client stored can carry TEXT delimiters, and it reaches
// BuildVCard verbatim when that contact is edited in the web UI. They cannot end
// a content line, so the card is written and the UID has to come back exactly as
// ExtractVCardUID reads it, or the edit would move the contact to a new identity.
func TestBuildVCardKeepsAStoredUIDCarryingTextDelimiters(t *testing.T) {
	for _, uid := range []string{"a;b", "a,b", `a\,b`, "urn:uuid:1;x"} {
		card, err := BuildVCard(uid, "Bob", "", "", "", "", "", "", "")
		if err != nil {
			t.Fatalf("BuildVCard(%q): %v", uid, err)
		}
		if got := ExtractVCardUID(card); got != uid {
			t.Fatalf("UID %q came back as %q:\n%s", uid, got, card)
		}
	}
}

// A control character cannot be carried by a TEXT value, so it is refused
// rather than dropped from a card that would then not say what was sent.
func TestEscapeVCardValueRefusesControlCharacters(t *testing.T) {
	for _, value := range []string{"A\x00B", "A\x01B", "C\vD", "D\fE", "E\x1bF", "F\x7fG"} {
		if got, err := EscapeVCardValue(value); err == nil {
			t.Errorf("EscapeVCardValue(%q) = %q, want an error", value, got)
		}
	}
}

// TEL and EMAIL are not TEXT values in vCard 3.0, and the web UI reads them back
// raw. Writing them TEXT-escaped would add a backslash before every , ; and \
// on each save, so a card built from what was read must carry the same value.
func TestBuildVCardPhoneAndEmailRoundTrip(t *testing.T) {
	tests := []struct{ phone, email string }{
		{phone: "+15551234,,99;ext=5", email: "a,b;c@example.com"},
		{phone: `+1 555 \ 0100`, email: `odd\name@example.com`},
		{phone: "+1 (555) 010-0199", email: "plain@example.com"},
	}
	for _, tt := range tests {
		card, err := BuildVCard("u1", "Bob", "", "", tt.email, tt.phone, "", "", "")
		if err != nil {
			t.Fatalf("BuildVCard: %v", err)
		}
		phone, email := rawPropertyValue(card, "TEL"), rawPropertyValue(card, "EMAIL")
		if phone != tt.phone || email != tt.email {
			t.Fatalf("card carries TEL %q and EMAIL %q, want %q and %q:\n%s", phone, email, tt.phone, tt.email, card)
		}
		again, err := BuildVCard("u1", "Bob", "", "", email, phone, "", "", "")
		if err != nil {
			t.Fatalf("BuildVCard from read-back values: %v", err)
		}
		if rawPropertyValue(again, "TEL") != phone || rawPropertyValue(again, "EMAIL") != email {
			t.Fatalf("second build changed the values:\n%s\n---\n%s", card, again)
		}
	}
}

// rawPropertyValue reads a property's value the way the web UI reads TEL and
// EMAIL: after unfolding, from the first colon, without unescaping.
func rawPropertyValue(card, name string) string {
	for _, line := range ical.UnfoldLines(card) {
		key, value, ok := strings.Cut(line, ":")
		if !ok {
			continue
		}
		key, _, _ = strings.Cut(key, ";")
		if strings.EqualFold(key, name) {
			return value
		}
	}
	return ""
}

// A UID written with parameters is still the card's UID. Reading only "UID:"
// lines would report none, and the caller would inject a second one.
func TestExtractVCardUIDAcceptsParameters(t *testing.T) {
	cards := map[string]string{
		"BEGIN:VCARD\r\nVERSION:4.0\r\nUID;VALUE=text:abc\r\nFN:x\r\nEND:VCARD\r\n":       "abc",
		"BEGIN:VCARD\r\nVERSION:4.0\r\nUID;VALUE=uri:urn:uuid:1\r\nFN:x\r\nEND:VCARD\r\n": "urn:uuid:1",
		"BEGIN:VCARD\r\nVERSION:3.0\r\nFN:x\r\nEND:VCARD\r\n":                             "",
	}
	for card, want := range cards {
		if got := ExtractVCardUID(card); got != want {
			t.Errorf("ExtractVCardUID(%q) = %q, want %q", card, got, want)
		}
	}
}

// Callers map refusals to form fields, so each is identifiable with errors.Is
// while its text stays as it was.
func TestBuildVCardRefusalsAreTyped(t *testing.T) {
	tests := []struct {
		name     string
		build    func() (string, error)
		sentinel error
		text     string
	}{
		{name: "birthday", build: func() (string, error) { return BuildVCard("u", "B", "", "", "", "", "1990-02-31", "", "") }, sentinel: ErrInvalidBirthday, text: "vCard BDAY must be a YYYY-MM-DD or --MM-DD date"},
		{name: "empty uid", build: func() (string, error) { return BuildVCard("", "B", "", "", "", "", "", "", "") }, sentinel: ErrInvalidUID, text: "vCard UID must not be empty"},
		{name: "padded uid", build: func() (string, error) { return BuildVCard(" u", "B", "", "", "", "", "", "", "") }, sentinel: ErrInvalidUID, text: "vCard UID must not be padded with whitespace"},
		{name: "uid control", build: func() (string, error) { return BuildVCard("u\x00", "B", "", "", "", "", "", "", "") }, sentinel: ErrControlCharacter, text: "vCard UID must not contain control characters"},
		{name: "phone control", build: func() (string, error) { return BuildVCard("u", "B", "", "", "", "+1\n", "", "", "") }, sentinel: ErrControlCharacter},
		{name: "name control", build: func() (string, error) { return BuildVCard("u", "B\x01", "", "", "", "", "", "", "") }, sentinel: ErrControlCharacter},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := tt.build()
			if !errors.Is(err, tt.sentinel) {
				t.Fatalf("error = %v, want errors.Is %v", err, tt.sentinel)
			}
			if tt.text != "" && err.Error() != tt.text {
				t.Fatalf("error text = %q, want %q", err.Error(), tt.text)
			}
		})
	}
	_, err := BuildVCard("u\x00", "B", "", "", "", "", "", "", "")
	if !errors.Is(err, ErrInvalidUID) {
		t.Fatalf("a UID carrying a control character is not ErrInvalidUID: %v", err)
	}
}
