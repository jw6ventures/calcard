package dav

import (
	"encoding/xml"
	"strings"
	"testing"
)

// RFC 4791 §9.6: the iCalendar data embedded in CALDAV:calendar-data must
// follow the standard XML character data encoding rules, and a CDATA section is
// one of the two ways §9.6 names. CDATA escapes nothing, so what has to hold is
// that the payload is character data at all: an octet outside the XML 1.0 §2.2
// Char production makes the whole multistatus unparseable, not just that one
// property.
func TestCalendarDataIsWellFormedCharacterDataForHostileOctets(t *testing.T) {
	tests := map[string]struct {
		stored string
		want   string
	}{
		"a CDATA terminator in the value": {
			stored: "SUMMARY:before ]]> after",
			want:   "SUMMARY:before ]]> after",
		},
		"a form feed": {
			stored: "SUMMARY:a\x0cb",
			want:   "SUMMARY:a�b",
		},
		"an escape character": {
			stored: "SUMMARY:a\x1bb",
			want:   "SUMMARY:a�b",
		},
		"a NUL": {
			stored: "SUMMARY:a\x00b",
			want:   "SUMMARY:a�b",
		},
		"an invalid UTF-8 byte": {
			stored: "SUMMARY:a\xffb",
			want:   "SUMMARY:a�b",
		},
		"a lone surrogate encoded as UTF-8": {
			stored: "SUMMARY:a\xed\xa0\x80b",
			want:   "SUMMARY:a���b",
		},
		// §9.6 anticipates this row: "Given that XML parsers normalize the
		// two-character sequence CRLF ... to a single LF character, the CR
		// character MAY be omitted". So the CR the encoder wrote is expected to
		// come back as an LF, and nothing else about the value may change.
		"the characters Char does admit": {
			stored: "SUMMARY:tab\there\r\nnewline é☃",
			want:   "SUMMARY:tab\there\nnewline é☃",
		},
	}

	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			encoded, err := xml.Marshal(struct {
				XMLName xml.Name `xml:"value"`
				Data    cdataString
			}{Data: cdataString(test.stored)})
			if err != nil {
				t.Fatalf("marshal: %v", err)
			}

			// The point of the row: the document still parses.
			var decoded struct {
				Data string `xml:"Data"`
			}
			if err := xml.Unmarshal(encoded, &decoded); err != nil {
				t.Fatalf("the encoded property is not parseable XML: %v; encoded: %q", err, encoded)
			}
			if decoded.Data != test.want {
				t.Errorf("round-tripped value = %q, want %q; encoded: %q", decoded.Data, test.want, encoded)
			}
			// CDATA is what §9.6 names and what clients here already expect, so
			// the sanitizing must not have quietly switched to entity escaping.
			if !strings.Contains(string(encoded), "<![CDATA[") {
				t.Errorf("value is no longer written as CDATA: %q", encoded)
			}
		})
	}
}

// A "]]>" in stored data has to survive as data rather than closing the
// section early, which is the hazard §9.6 calls out by name.
func TestCalendarDataCDATATerminatorDoesNotEscapeTheSection(t *testing.T) {
	encoded, err := xml.Marshal(struct {
		XMLName xml.Name `xml:"value"`
		Data    cdataString
	}{Data: cdataString("BEGIN:VCALENDAR\r\nX-EVIL:]]><injected/>\r\nEND:VCALENDAR\r\n")})
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	// The encoded form may well contain the characters "<injected/>" -- they sit
	// inside the second CDATA section Go opens after splitting the terminator.
	// What matters is that a parser sees them as character data, so decoding
	// yields no element of that name.
	decoder := xml.NewDecoder(strings.NewReader(string(encoded)))
	for {
		token, err := decoder.Token()
		if err != nil {
			break
		}
		if start, ok := token.(xml.StartElement); ok && start.Name.Local == "injected" {
			t.Fatalf("the payload escaped its CDATA section and parsed as markup: %q", encoded)
		}
	}

	var decoded struct {
		Data string `xml:"Data"`
	}
	if err := xml.Unmarshal(encoded, &decoded); err != nil {
		t.Fatalf("decode: %v; encoded: %q", err, encoded)
	}
	if !strings.Contains(decoded.Data, "X-EVIL:]]><injected/>") {
		t.Errorf("round-tripped value lost the literal terminator: %q", decoded.Data)
	}
}

// A projected resource reaches the encoder as freshly serialized octets rather
// than as the stored ones, so the two halves have to hold together: the parse,
// transform and serialize must carry a hostile octet through unchanged, and the
// encoder must still be the thing that makes it safe. A request that narrows
// nothing takes the raw fast path and never exercises that.
func TestProjectedCalendarDataIsWellFormedCharacterData(t *testing.T) {
	raw := "BEGIN:VCALENDAR\r\n" +
		"VERSION:2.0\r\n" +
		"PRODID:-//CalCard//EN\r\n" +
		"BEGIN:VEVENT\r\n" +
		"UID:hostile\r\n" +
		"DTSTAMP:20240601T080000Z\r\n" +
		"DTSTART:20240601T090000Z\r\n" +
		"SUMMARY:a\x0cb ]]> c\r\n" +
		"END:VEVENT\r\n" +
		"END:VCALENDAR\r\n"

	projected := projectFor(raw, &calendarDataEl{Comp: &calendarComp{
		Name: "VCALENDAR",
		Comp: []calendarComp{{Name: "VEVENT", Prop: []calendarProp{{Name: "UID"}, {Name: "SUMMARY"}}}},
	}})
	// The transform is not the sanitizer: it hands the octets on as they were
	// stored, and the encoder is the single place they are made safe.
	if !strings.Contains(projected, "a\x0cb ]]> c") {
		t.Fatalf("the projection altered the stored value before encoding:\n%s", projected)
	}

	encoded, err := xml.Marshal(struct {
		XMLName xml.Name `xml:"value"`
		Data    cdataString
	}{Data: cdataString(projected)})
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	var decoded struct {
		Data string `xml:"Data"`
	}
	if err := xml.Unmarshal(encoded, &decoded); err != nil {
		t.Fatalf("the projected value is not parseable XML: %v; encoded: %q", err, encoded)
	}
	if !strings.Contains(decoded.Data, "SUMMARY:a�b ]]> c") {
		t.Errorf("round-tripped projection = %q, want the form feed replaced and the terminator intact", decoded.Data)
	}
}

// sanitizeXMLCharData is applied to every value CalCard writes as CDATA, which
// is the CALDAV:calendar-data of a REPORT and the CARDDAV:address-data beside
// it. The function is the shared guarantee, so it is pinned directly too.
func TestSanitizeXMLCharDataLeavesWellFormedValuesUntouched(t *testing.T) {
	value := "BEGIN:VCALENDAR\r\nSUMMARY:naïve ☃\r\nEND:VCALENDAR\r\n"
	if got := sanitizeXMLCharData(value); got != value {
		t.Fatalf("well-formed value was rewritten:\n got %q\nwant %q", got, value)
	}
}
