package dav

import (
	"encoding/xml"
	"strings"
	"unicode/utf8"
)

// XML character-data encoding for the iCalendar and vCard payloads a DAV
// response embeds. Those octets reach here unchanged from a PUT, and what
// RFC 5545 and RFC 6350 admit is wider than what XML does.

// cdataString wraps string content in CDATA for raw XML output.
type cdataString string

// MarshalXML writes the value as a CDATA section. RFC 4791 §9.6 requires the
// iCalendar data embedded in CALDAV:calendar-data to follow the standard XML
// character data encoding rules, and names CDATA as one of the two ways to do
// it. CDATA escapes nothing, so the encoding rule that survives is the XML 1.0
// §2.2 Char production: an octet outside it would make the whole multistatus
// unparseable rather than merely odd. Go's ",cdata" encoder already splits an
// embedded "]]>" across two sections, which is the other §9.6 hazard.
func (c cdataString) MarshalXML(e *xml.Encoder, start xml.StartElement) error {
	if c == "" {
		return nil
	}
	return e.EncodeElement(struct {
		S string `xml:",cdata"`
	}{S: sanitizeXMLCharData(string(c))}, start)
}

// sanitizeXMLCharData replaces every rune the XML 1.0 §2.2 Char production
// excludes, and every byte that is not valid UTF-8, with U+FFFD.
func sanitizeXMLCharData(value string) string {
	if wellFormedXMLCharData(value) {
		return value
	}
	var b strings.Builder
	b.Grow(len(value))
	for i := 0; i < len(value); {
		r, width := utf8.DecodeRuneInString(value[i:])
		// A decode failure yields RuneError with a width of 1, which the
		// replacement below handles the same way as a disallowed rune.
		if (r == utf8.RuneError && width == 1) || !validXMLChar(r) {
			b.WriteRune(utf8.RuneError)
			i += width
			continue
		}
		b.WriteString(value[i : i+width])
		i += width
	}
	return b.String()
}

// wellFormedXMLCharData reports whether every rune is valid UTF-8 and admitted
// by the Char production. Almost every value is, so the common path allocates
// nothing.
func wellFormedXMLCharData(value string) bool {
	if !utf8.ValidString(value) {
		return false
	}
	for _, r := range value {
		if !validXMLChar(r) {
			return false
		}
	}
	return true
}

// validXMLChar reports whether r is admitted by the XML 1.0 §2.2 Char
// production: tab, newline, carriage return, and the printable ranges above
// them, minus the surrogate block.
func validXMLChar(r rune) bool {
	switch {
	case r == 0x09, r == 0x0A, r == 0x0D:
		return true
	case r >= 0x20 && r <= 0xD7FF:
		return true
	case r >= 0xE000 && r <= 0xFFFD:
		return true
	case r >= 0x10000 && r <= 0x10FFFF:
		return true
	default:
		return false
	}
}
