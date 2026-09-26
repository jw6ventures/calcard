package vcard

import (
	"errors"
	"strings"
	"testing"
	"time"
)

func TestValidateEnvelope(t *testing.T) {
	tests := []struct {
		name    string
		card    string
		wantErr bool
	}{
		{name: "one card", card: "BEGIN:VCARD\r\nVERSION:3.0\r\nFN:x\r\nEND:VCARD\r\n"},
		{name: "value spells a delimiter", card: "BEGIN:VCARD\r\nFN:END:VCARD\r\nEND:VCARD"},
		{name: "folded delimiter spelling", card: "BEGIN:VCARD\r\nFN:BEGIN\r\n :VCARD\r\nEND:VCARD"},
		{name: "two cards", card: "BEGIN:VCARD\r\nFN:a\r\nEND:VCARD\r\nBEGIN:VCARD\r\nFN:b\r\nEND:VCARD\r\n", wantErr: true},
		{name: "content after END", card: "BEGIN:VCARD\r\nVERSION:3.0\r\nEND:VCARD\r\nUID:u1\r\nFN:x\r\nX-A:END:VCARD", wantErr: true},
		{name: "content before BEGIN", card: "UID:u1\r\nBEGIN:VCARD\r\nEND:VCARD", wantErr: true},
		{name: "nested card", card: "BEGIN:VCARD\r\nBEGIN:VCARD\r\nEND:VCARD\r\nEND:VCARD", wantErr: true},
		{name: "empty", card: "", wantErr: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := ValidateEnvelope(tt.card)
			if (err != nil) != tt.wantErr {
				t.Fatalf("ValidateEnvelope() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestValidate(t *testing.T) {
	const head = "BEGIN:VCARD\r\nVERSION:3.0\r\n"
	tests := []struct {
		name       string
		card       string
		requireUID bool
		wantErr    bool
	}{
		{name: "complete", card: head + "UID:u\r\nFN:x\r\nEND:VCARD", requireUID: true},
		{name: "uid optional", card: head + "FN:x\r\nEND:VCARD"},
		{name: "uid required", card: head + "FN:x\r\nEND:VCARD", requireUID: true, wantErr: true},
		{name: "two uids", card: head + "UID:a\r\nUID;VALUE=text:b\r\nFN:x\r\nEND:VCARD", wantErr: true},
		{name: "grouped second uid", card: head + "UID:a\r\ng.UID:b\r\nFN:x\r\nEND:VCARD", wantErr: true},
		{name: "empty uid", card: head + "UID:\r\nFN:x\r\nEND:VCARD", wantErr: true},
		{name: "no fn", card: head + "UID:u\r\nEND:VCARD", wantErr: true},
		{name: "no version", card: "BEGIN:VCARD\r\nUID:u\r\nFN:x\r\nEND:VCARD", wantErr: true},
		{name: "vcard 2.1", card: "BEGIN:VCARD\r\nVERSION:2.1\r\nUID:u\r\nFN:x\r\nEND:VCARD", wantErr: true},
		{name: "two cards", card: head + "UID:a\r\nFN:x\r\nEND:VCARD\r\n" + head + "UID:b\r\nFN:y\r\nEND:VCARD", wantErr: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := Validate(tt.card, tt.requireUID)
			if (err != nil) != tt.wantErr {
				t.Fatalf("Validate() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestUIDAcceptsParametersAndGroups(t *testing.T) {
	tests := map[string]string{
		"BEGIN:VCARD\r\nUID:plain\r\nEND:VCARD":                   "plain",
		"BEGIN:VCARD\r\nUID;VALUE=text:with-param\r\nEND:VCARD":   "with-param",
		"BEGIN:VCARD\r\nitem1.UID:grouped\r\nEND:VCARD":           "grouped",
		"BEGIN:VCARD\r\nuid:lower\r\nEND:VCARD":                   "lower",
		"BEGIN:VCARD\r\nUIDX:not-uid\r\nEND:VCARD":                "",
		"BEGIN:VCARD\r\nX-P;A=\"UID:no\":v\r\nUID:u\r\nEND:VCARD": "u",
		"BEGIN:VCARD\r\nUID:" + strings.Repeat("a", 90) + "\r\n":  strings.Repeat("a", 90),
	}
	for card, want := range tests {
		if got := UID(card); got != want {
			t.Errorf("UID(%q) = %q, want %q", card, got, want)
		}
	}
}

func TestParseLineHonoursQuotedParameters(t *testing.T) {
	line, ok := ParseLine(`item2.TEL;TYPE=home,"voice";X-L="a:b;c":+1 555`)
	if !ok {
		t.Fatal("ParseLine refused a valid line")
	}
	if line.Group != "item2" || line.Name != "TEL" || line.Value != "+1 555" {
		t.Fatalf("ParseLine = %#v", line)
	}
	if got := line.ParamValues("type"); len(got) != 2 || got[0] != "home" || got[1] != "voice" {
		t.Fatalf("ParamValues(type) = %q", got)
	}
	if got := line.String(); got != `item2.TEL;TYPE=home,"voice";X-L="a:b;c":+1 555` {
		t.Fatalf("String() = %q, want the line back byte for byte", got)
	}
	if got := line.WithoutParam("X-L").String(); got != `item2.TEL;TYPE=home,"voice":+1 555` {
		t.Fatalf("WithoutParam = %q", got)
	}
}

func TestEscapeTextRefusesControlCharacters(t *testing.T) {
	for _, value := range []string{"a\x00b", "a\x1bb", "a\x7fb", "a\vb"} {
		if _, err := EscapeText(value); !errors.Is(err, ErrControlCharacter) {
			t.Errorf("EscapeText(%q) error = %v, want ErrControlCharacter", value, err)
		}
	}
	got, err := EscapeText("a;b,c\\d\r\ne\rf\ng\th")
	if err != nil {
		t.Fatal(err)
	}
	if want := `a\;b\,c\\d\ne\nf\ng` + "\th"; got != want {
		t.Fatalf("EscapeText = %q, want %q", got, want)
	}
	if back := UnescapeText(got); back != "a;b,c\\d\ne\nf\ng\th" {
		t.Fatalf("UnescapeText = %q", back)
	}
}

func TestSplitComponentsKeepsEscapedSemicolons(t *testing.T) {
	got := SplitComponents(`Doe\;Jr;John;;Dr.;`)
	want := []string{`Doe\;Jr`, "John", "", "Dr.", ""}
	if strings.Join(got, "|") != strings.Join(want, "|") {
		t.Fatalf("SplitComponents = %q, want %q", got, want)
	}
}

func TestWriteLineFoldsOnCharacterBoundaries(t *testing.T) {
	var sb strings.Builder
	WriteLine(&sb, "NOTE:"+strings.Repeat("é", 100))
	for _, physical := range strings.Split(strings.TrimSuffix(sb.String(), "\r\n"), "\r\n") {
		if len(physical) > MaxLineOctets {
			t.Fatalf("physical line of %d octets", len(physical))
		}
	}
	if got := ContentLines(sb.String()); len(got) != 1 || got[0] != "NOTE:"+strings.Repeat("é", 100) {
		t.Fatalf("unfolded = %q", got)
	}
}

func TestParseDate(t *testing.T) {
	valid := map[string]Date{
		"1990-01-15": {Year: 1990, Month: time.January, Day: 15, HasYear: true},
		"19900115":   {Year: 1990, Month: time.January, Day: 15, HasYear: true},
		"--02-29":    {Month: time.February, Day: 29},
		"--0229":     {Month: time.February, Day: 29},
		"1992-02-29": {Year: 1992, Month: time.February, Day: 29, HasYear: true},
	}
	for value, want := range valid {
		got, ok := ParseDate(value)
		if !ok || got != want {
			t.Errorf("ParseDate(%q) = %#v, %v; want %#v", value, got, ok, want)
		}
	}
	for _, value := range []string{"", "1990", "1990-02-31", "1991-02-29", "--13-01", "--0230", "circa 1800", "1990-01-15T00:00:00Z", "+990-01-15", "1990-1-015"} {
		if got, ok := ParseDate(value); ok {
			t.Errorf("ParseDate(%q) = %#v, want refusal", value, got)
		}
	}
}

func TestDateFormatFollowsVersion(t *testing.T) {
	withYear := Date{Year: 1990, Month: time.March, Day: 4, HasYear: true}
	noYear := Date{Month: time.March, Day: 4}
	cases := map[string]string{
		withYear.Format("3.0"): "1990-03-04",
		withYear.Format("4.0"): "19900304",
		noYear.Format("3.0"):   "--03-04",
		noYear.Format("4.0"):   "--0304",
	}
	for got, want := range cases {
		if got != want {
			t.Errorf("Format = %q, want %q", got, want)
		}
	}
}

func TestParseDateProperty(t *testing.T) {
	valid := map[string]Date{
		"BDAY:1990-01-15T00:00:00Z":                {Year: 1990, Month: time.January, Day: 15, HasYear: true},
		"BDAY:19900115T120000":                     {Year: 1990, Month: time.January, Day: 15, HasYear: true},
		"BDAY;X-APPLE-OMIT-YEAR=1604:1604-03-15":   {Month: time.March, Day: 15},
		`BDAY;X-APPLE-OMIT-YEAR="1604":1604-03-15`: {Month: time.March, Day: 15},
		"BDAY;X-APPLE-OMIT-YEAR=1604:1990-03-15":   {Year: 1990, Month: time.March, Day: 15, HasYear: true},
		"BDAY;VALUE=date-and-or-time:--0315":       {Month: time.March, Day: 15},
	}
	for raw, want := range valid {
		line, _ := ParseLine(raw)
		if got, ok := ParseDateProperty(line); !ok || got != want {
			t.Errorf("%s = %#v, %v; want %#v", raw, got, ok, want)
		}
	}
	for _, raw := range []string{"BDAY;VALUE=text:1990-01-15", "BDAY:1990-01-15Tx", "BDAY:1990-01-15T", "BDAY:circa"} {
		line, _ := ParseLine(raw)
		if got, ok := ParseDateProperty(line); ok {
			t.Errorf("%s = %#v, want refusal", raw, got)
		}
	}
}

func TestValidateBoundsTheUID(t *testing.T) {
	card := func(uid string) string {
		return "BEGIN:VCARD\r\nVERSION:3.0\r\nUID:" + uid + "\r\nFN:x\r\nEND:VCARD\r\n"
	}
	if err := Validate(card(strings.Repeat("u", MaxUIDOctets)), true); err != nil {
		t.Fatalf("UID at the limit refused: %v", err)
	}
	if err := Validate(card(strings.Repeat("u", MaxUIDOctets+1)), true); err == nil {
		t.Fatal("UID past the limit accepted")
	}
}
