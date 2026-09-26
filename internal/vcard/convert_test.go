package vcard

import (
	"errors"
	"strings"
	"testing"
)

// An Android contacts export: UTF-8 quoted-printable values with soft line
// breaks, bare type parameters and a folded base64 photo.
const android21 = "BEGIN:VCARD\r\n" +
	"VERSION:2.1\r\n" +
	"N;CHARSET=UTF-8;ENCODING=QUOTED-PRINTABLE:M=C3=BCller;J=C3=BCrgen;;;\r\n" +
	"FN;CHARSET=UTF-8;ENCODING=QUOTED-PRINTABLE:J=C3=BCrgen M=C3=BCller\r\n" +
	"TEL;CELL;PREF:+49 170 1234567\r\n" +
	"TEL;WORK;VOICE:+49 30 123456\r\n" +
	"EMAIL;INTERNET:juergen@example.de\r\n" +
	"ADR;HOME;CHARSET=UTF-8;ENCODING=QUOTED-PRINTABLE:;;Hauptstra=C3=9Fe 1;Berlin;;10115;Deutschl=\r\n" +
	"and\r\n" +
	"NOTE;ENCODING=QUOTED-PRINTABLE:Erste Zeile=0D=0A=\r\n" +
	"Zweite Zeile, mit Komma\r\n" +
	"ORG:Acme GmbH;Vertrieb\r\n" +
	"BDAY:19800312\r\n" +
	"PHOTO;JPEG;ENCODING=BASE64:\r\n" +
	" /9j/4AAQSkZJRgABAQAAAQABAAD\r\n" +
	" /2wBDAAMCAgICAgMCAgIDAwMD\r\n" +
	"\r\n" +
	"END:VCARD\r\n"

// An Outlook export: Windows-1252 text.
const outlook21 = "BEGIN:VCARD\r\n" +
	"VERSION:2.1\r\n" +
	"N;LANGUAGE=fr;CHARSET=Windows-1252;ENCODING=QUOTED-PRINTABLE:Dupont;Ren=E9\r\n" +
	"FN;CHARSET=Windows-1252;ENCODING=QUOTED-PRINTABLE:Ren=E9 Dupont =80\r\n" +
	"TITLE:Directeur\r\n" +
	"X-MS-OL-DEFAULT-POSTAL-ADDRESS:0\r\n" +
	"REV:20240101T120000Z\r\n" +
	"END:VCARD\r\n"

func TestConvert21To30(t *testing.T) {
	tests := []struct {
		name string
		card string
		want []string
	}{
		{name: "android", card: android21, want: []string{
			"VERSION:3.0",
			"N:Müller;Jürgen;;;",
			"FN:Jürgen Müller",
			"TEL;TYPE=CELL,PREF:+49 170 1234567",
			"TEL;TYPE=WORK,VOICE:+49 30 123456",
			"EMAIL;TYPE=INTERNET:juergen@example.de",
			"ADR;TYPE=HOME:;;Hauptstraße 1;Berlin;;10115;Deutschland",
			`NOTE:Erste Zeile\nZweite Zeile\, mit Komma`,
			"ORG:Acme GmbH;Vertrieb",
			"BDAY:19800312",
			"PHOTO;TYPE=JPEG;ENCODING=b:/9j/4AAQSkZJRgABAQAAAQABAAD/2wBDAAMCAgICAgMCAgIDAwMD",
		}},
		{name: "outlook", card: outlook21, want: []string{
			"VERSION:3.0",
			"N;LANGUAGE=fr:Dupont;René",
			"FN:René Dupont €",
			"TITLE:Directeur",
			"X-MS-OL-DEFAULT-POSTAL-ADDRESS:0",
		}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := Convert21To30(tt.card)
			if err != nil {
				t.Fatalf("Convert21To30: %v", err)
			}
			lines := ContentLines(got)
			for _, want := range tt.want {
				found := false
				for _, line := range lines {
					found = found || line == want
				}
				if !found {
					t.Errorf("converted card lacks %q:\n%s", want, got)
				}
			}
			if err := Validate(got, false); err != nil {
				t.Fatalf("converted card is invalid: %v\n%s", err, got)
			}
			for _, physical := range strings.Split(strings.TrimSuffix(got, "\r\n"), "\r\n") {
				if len(physical) > MaxLineOctets {
					t.Errorf("unfolded physical line of %d octets", len(physical))
				}
			}
		})
	}
}

func TestConvert21To30DerivesAMissingFN(t *testing.T) {
	got, err := Convert21To30("BEGIN:VCARD\r\nVERSION:2.1\r\nN:Doe;Jane;;;\r\nEND:VCARD\r\n")
	if err != nil {
		t.Fatalf("Convert21To30: %v", err)
	}
	if !strings.Contains(got, "FN:Jane Doe\r\n") {
		t.Fatalf("no FN derived from N:\n%s", got)
	}
}

func TestConvert21To30Refusals(t *testing.T) {
	tests := map[string]string{
		"unsupported charset":  "BEGIN:VCARD\r\nVERSION:2.1\r\nFN;CHARSET=SHIFT_JIS;ENCODING=QUOTED-PRINTABLE:=82=A0\r\nEND:VCARD\r\n",
		"not 2.1":              "BEGIN:VCARD\r\nVERSION:3.0\r\nFN:x\r\nEND:VCARD\r\n",
		"no name at all":       "BEGIN:VCARD\r\nVERSION:2.1\r\nTEL:1\r\nEND:VCARD\r\n",
		"bad quoted-printable": "BEGIN:VCARD\r\nVERSION:2.1\r\nFN;ENCODING=QUOTED-PRINTABLE:a=ZZ\r\nEND:VCARD\r\n",
	}
	for name, card := range tests {
		if got, err := Convert21To30(card); err == nil {
			t.Errorf("%s: converted to\n%s", name, got)
		}
	}
	_, err := Convert21To30(tests["unsupported charset"])
	if !errors.Is(err, ErrUnsupportedCharset) {
		t.Errorf("charset refusal = %v, want ErrUnsupportedCharset", err)
	}
}

func TestSplitCardsKeepsQuotedPrintableSoftBreaks(t *testing.T) {
	cards := SplitCards(android21 + outlook21 + "garbage\r\n" + "begin:vcard\r\nVERSION:3.0\r\nFN:x\r\nend:vcard\r\n")
	if len(cards) != 3 {
		t.Fatalf("SplitCards found %d cards, want 3", len(cards))
	}
	if cards[0] != android21 {
		t.Fatalf("first card altered:\n%q", cards[0])
	}
}

func TestSplitCardsStartsANewCardAtATopLevelBEGIN(t *testing.T) {
	file := "BEGIN:VCARD\r\nVERSION:3.0\r\nFN:unterminated\r\n" +
		"BEGIN:VCARD\r\nVERSION:3.0\r\nFN:second\r\nEND:VCARD\r\n" +
		"BEGIN:VCARD\r\nVERSION:3.0\r\nFN:trailing"
	cards := SplitCards(file)
	if len(cards) != 3 {
		t.Fatalf("SplitCards found %d cards, want 3: %q", len(cards), cards)
	}
	if ValidateEnvelope(cards[0]) == nil || ValidateEnvelope(cards[2]) == nil {
		t.Fatalf("an unterminated card came back whole: %q", cards)
	}
	if !strings.Contains(cards[1], "FN:second") || ValidateEnvelope(cards[1]) != nil {
		t.Fatalf("the complete card was lost: %q", cards[1])
	}
}

func TestSplitCardsReadsCROnlyLineEndings(t *testing.T) {
	cards := SplitCards("BEGIN:VCARD\rVERSION:3.0\rFN:a\rEND:VCARD\rBEGIN:VCARD\rVERSION:3.0\rFN:b\rEND:VCARD\r")
	if len(cards) != 2 || ValidateEnvelope(cards[1]) != nil {
		t.Fatalf("SplitCards = %q", cards)
	}
}

// A vCard 2.1 AGENT property may carry a whole card as its value. That card
// belongs to the property, so it neither ends the outer card nor starts one.
const agent21 = "BEGIN:VCARD\r\nVERSION:2.1\r\nFN:Boss\r\n" +
	"AGENT:\r\nBEGIN:VCARD\r\nVERSION:2.1\r\nFN:Assistant\r\nTEL:+1 555\r\nEND:VCARD\r\n" +
	"TEL;WORK:+1 666\r\nEND:VCARD\r\n"

func TestSplitCardsNestsOnlyInsideAnAgentValue(t *testing.T) {
	cards := SplitCards(agent21 + "BEGIN:VCARD\r\nVERSION:3.0\r\nFN:next\r\nEND:VCARD\r\n")
	if len(cards) != 2 || cards[0] != agent21 {
		t.Fatalf("SplitCards = %q", cards)
	}
}

// The embedded AGENT card is dropped: vCard 3.0 would need it re-encoded as
// an escaped text value, RFC 6350 removed AGENT, and no client this server
// serves reads it back. The rest of the card is kept.
func TestConvert21To30DropsAnEmbeddedAgentCard(t *testing.T) {
	got, err := Convert21To30(agent21)
	if err != nil {
		t.Fatalf("Convert21To30: %v", err)
	}
	if strings.Contains(got, "Assistant") || strings.Contains(got, "AGENT") || !strings.Contains(got, "TEL;TYPE=WORK:+1 666") || !strings.Contains(got, "FN:Boss") {
		t.Fatalf("converted:\n%s", got)
	}
}

// RFC 822 folding, which vCard 2.1 uses, keeps the whitespace that starts the
// continuation.
func TestConvert21To30KeepsFoldWhitespace(t *testing.T) {
	got, err := Convert21To30("BEGIN:VCARD\r\nVERSION:2.1\r\nFN:x\r\nNOTE:Hello\r\n World\r\nPHOTO;ENCODING=BASE64:\r\n AAAA\r\n BBBB\r\n\r\nEND:VCARD\r\n")
	if err != nil {
		t.Fatalf("Convert21To30: %v", err)
	}
	for _, want := range []string{"NOTE:Hello World", "PHOTO;ENCODING=b:AAAABBBB"} {
		if !strings.Contains(strings.Join(ContentLines(got), "\n"), want) {
			t.Errorf("converted card lacks %q:\n%s", want, got)
		}
	}
}

func TestConvert21To30QuotedPrintableSoftBreakStopsAtEND(t *testing.T) {
	got, err := Convert21To30("BEGIN:VCARD\r\nVERSION:2.1\r\nFN:x\r\nNOTE;ENCODING=QUOTED-PRINTABLE:abc=\r\nEND:VCARD\r\n")
	if err != nil {
		t.Fatalf("Convert21To30: %v", err)
	}
	if !strings.Contains(got, "NOTE:abc\r\n") {
		t.Fatalf("converted:\n%s", got)
	}
}

func TestConvert21To30Edges(t *testing.T) {
	got, err := Convert21To30("BEGIN:VCARD\r\nVERSION:2.1\r\nN:Doe;Jane\r\nFN:\r\n" +
		"UID;ENCODING=QUOTED-PRINTABLE:abc=3D1\r\n" +
		"NOTE;CHARSET=\"UTF-8\";ENCODING=QUOTED-PRINTABLE:caf=C3=A9\r\n" +
		"X-ANDROID-CUSTOM:vnd.android.cursor.item/nickname;Bob;1;;;;;;;;;;;;;\r\n" +
		"X-NOTE-2;ENCODING=QUOTED-PRINTABLE:a=0D=0Ab, c\r\n" +
		"END:VCARD\r\n")
	if err != nil {
		t.Fatalf("Convert21To30: %v", err)
	}
	lines := ContentLines(got)
	fns := 0
	for _, line := range lines {
		if strings.HasPrefix(line, "FN:") {
			fns++
		}
	}
	if fns != 1 {
		t.Errorf("converted card carries %d FN lines:\n%s", fns, got)
	}
	for _, want := range []string{"FN:Jane Doe", "UID:abc=1", "NOTE:café", "X-ANDROID-CUSTOM:vnd.android.cursor.item/nickname;Bob;1;;;;;;;;;;;;;", `X-NOTE-2:a\nb, c`} {
		found := false
		for _, line := range lines {
			found = found || line == want
		}
		if !found {
			t.Errorf("converted card lacks %q:\n%s", want, got)
		}
	}
	if UID(got) != "abc=1" {
		t.Errorf("UID(converted) = %q", UID(got))
	}
}

// A folded continuation is part of the value before it, however it reads.
func TestFoldedDelimiterTextIsNotADelimiter(t *testing.T) {
	for _, fold := range []string{" BEGIN:VCARD", " END:VCARD", "\tEND:VCARD"} {
		t.Run(fold, func(t *testing.T) {
			first := "BEGIN:VCARD\r\nVERSION:2.1\r\nFN:A\r\nNOTE:x\r\n" + fold + "\r\nEND:VCARD\r\n"
			second := "BEGIN:VCARD\r\nVERSION:3.0\r\nFN:B\r\nEND:VCARD\r\n"
			cards := SplitCards(first + second)
			if len(cards) != 2 || cards[0] != first {
				t.Fatalf("SplitCards = %q", cards)
			}
			got, err := Convert21To30(cards[0])
			if err != nil {
				t.Fatalf("Convert21To30: %v", err)
			}
			if !strings.Contains(strings.Join(ContentLines(got), "\n"), "NOTE:x"+fold) {
				t.Fatalf("folded text lost:\n%s", got)
			}
		})
	}
	agent := "BEGIN:VCARD\r\nVERSION:2.1\r\nFN:A\r\nAGENT:\r\n BEGIN:VCARD\r\nEND:VCARD\r\n"
	if cards := SplitCards(agent); len(cards) != 1 || cards[0] != agent {
		t.Fatalf("SplitCards(agent with folded text) = %q", cards)
	}
	qp := "BEGIN:VCARD\r\nVERSION:2.1\r\nFN:A\r\nNOTE;ENCODING=QUOTED-PRINTABLE:a=\r\n END:VCARD\r\nEND:VCARD\r\n"
	got, err := Convert21To30(qp)
	if err != nil {
		t.Fatalf("Convert21To30(qp): %v", err)
	}
	if !strings.Contains(got, "NOTE:a END:VCARD") {
		t.Fatalf("quoted-printable continuation lost:\n%s", got)
	}
}

func TestConvert21To30EscapesBackslashesInExtensionValues(t *testing.T) {
	got, err := Convert21To30("BEGIN:VCARD\r\nVERSION:2.1\r\nFN:x\r\nX-PATH:C:\\new\\dir\r\nEND:VCARD\r\n")
	if err != nil {
		t.Fatalf("Convert21To30: %v", err)
	}
	if !strings.Contains(got, `X-PATH:C:\\new\\dir`+"\r\n") {
		t.Fatalf("converted:\n%s", got)
	}
}

// vCard 2.1 escapes a literal semicolon as \;, which vCard 3.0 spells the same
// way, so that escape passes through while other backslashes are doubled.
func TestConvert21To30KeepsTheSemicolonEscapeInExtensionValues(t *testing.T) {
	got, err := Convert21To30("BEGIN:VCARD\r\nVERSION:2.1\r\nFN:x\r\nX-FOO:a\\;b\r\nX-BAR:a\\,b\\\\c\r\nEND:VCARD\r\n")
	if err != nil {
		t.Fatalf("Convert21To30: %v", err)
	}
	for _, want := range []string{`X-FOO:a\;b`, `X-BAR:a\,b\\c`} {
		if !strings.Contains(got, want+"\r\n") {
			t.Errorf("converted card lacks %q:\n%s", want, got)
		}
	}
}
