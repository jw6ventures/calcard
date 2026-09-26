package store

import (
	"strings"
	"testing"
	"time"
	"unicode/utf8"

	"github.com/lib/pq"

	"github.com/jw6ventures/calcard/internal/vcard"
)

// The first FN is the display name, as the contact form and the structured
// edit read it.
func TestParseVCardFieldsTakesTheFirstFN(t *testing.T) {
	name, _, _ := parseVCardFields("BEGIN:VCARD\r\nVERSION:4.0\r\nFN:First\r\nFN;LANGUAGE=de:Zweite\r\nEND:VCARD\r\n")
	if name == nil || *name != "First" {
		t.Fatalf("display name = %v, want First", name)
	}
}

func TestParseVCardFieldsReadsBirthdayForms(t *testing.T) {
	tests := map[string]time.Time{
		"BDAY:1990-01-15T00:00:00Z":                time.Date(1990, 1, 15, 0, 0, 0, 0, time.UTC),
		"BDAY:19900115T120000":                     time.Date(1990, 1, 15, 0, 0, 0, 0, time.UTC),
		`BDAY;X-APPLE-OMIT-YEAR="1604":1604-03-15`: time.Date(NoYearBirthdayYear, 3, 15, 0, 0, 0, 0, time.UTC),
		"BDAY;X-APPLE-OMIT-YEAR=1604:1604-03-15":   time.Date(NoYearBirthdayYear, 3, 15, 0, 0, 0, 0, time.UTC),
	}
	for line, want := range tests {
		_, _, got := parseVCardFields("BEGIN:VCARD\r\nVERSION:3.0\r\nFN:x\r\n" + line + "\r\nEND:VCARD\r\n")
		if got == nil || !got.Equal(want) {
			t.Errorf("%s: birthday = %v, want %v", line, got, want)
		}
	}
	if _, _, got := parseVCardFields("BEGIN:VCARD\r\nVERSION:4.0\r\nFN:x\r\nBDAY;VALUE=text:circa 1800\r\nEND:VCARD\r\n"); got != nil {
		t.Errorf("text birthday parsed as %v", got)
	}
}

func TestIsDataErrorCoversValuesPastALimit(t *testing.T) {
	for code, want := range map[pq.ErrorCode]bool{"22021": true, "54000": true, "08006": false, "23505": false} {
		if got := IsDataError(&pq.Error{Code: code}); got != want {
			t.Errorf("IsDataError(%s) = %v, want %v", code, got, want)
		}
	}
}

func TestDisplayNameIsCutToTheIndexLimit(t *testing.T) {
	fn := strings.Repeat("é", maxDisplayNameOctets)
	name, _, _ := parseVCardFields("BEGIN:VCARD\r\nVERSION:3.0\r\nFN:" + fn + "\r\nEND:VCARD\r\n")
	if name == nil || len(*name) > maxDisplayNameOctets || !utf8.ValidString(*name) || !strings.HasPrefix(fn, *name) {
		t.Fatalf("display name of %d octets, valid UTF-8 %v", len(*name), utf8.ValidString(*name))
	}
}

func TestUIDLimitMatchesTheStoreLimit(t *testing.T) {
	if vcard.MaxUIDOctets != MaxIdentifierOctets {
		t.Fatalf("vcard.MaxUIDOctets = %d, store.MaxIdentifierOctets = %d", vcard.MaxUIDOctets, MaxIdentifierOctets)
	}
}
