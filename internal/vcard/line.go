// Package vcard holds the vCard content-line handling shared by the CardDAV
// handlers and the contacts API: reading, validating, escaping and folding
// cards, independent of how either side stores or serves them.
package vcard

import (
	"errors"
	"strings"

	"github.com/jw6ventures/calcard/internal/ical"
)

// MaxLineOctets is the RFC 2426 Section 2.6 / RFC 6350 Section 3.2 line
// length, counted in octets and excluding the line break.
const MaxLineOctets = 75

// ErrControlCharacter reports a value carrying an octet no content line may
// hold.
var ErrControlCharacter = errors.New("vCard value must not contain control characters")

// Line is one unfolded content line. Params and Value are kept as written, so
// a line that is only re-serialized comes back byte for byte.
type Line struct {
	Group  string
	Name   string
	Params []string
	Value  string
}

// ContentLines unfolds a card and returns its non-blank content lines.
func ContentLines(data string) []string {
	unfolded := ical.UnfoldLines(data)
	lines := unfolded[:0]
	for _, line := range unfolded {
		if strings.TrimSpace(line) != "" {
			lines = append(lines, line)
		}
	}
	return lines
}

// ParseLine splits an unfolded content line. Quoted parameter values may hold
// ":" and ";", so the split honours quoting.
func ParseLine(raw string) (Line, bool) {
	colon := indexOutsideQuotes(raw, ':')
	if colon <= 0 {
		return Line{}, false
	}
	segments := splitOutsideQuotes(raw[:colon], ';')
	name := strings.TrimSpace(segments[0])
	var line Line
	if dot := strings.LastIndexByte(name, '.'); dot >= 0 {
		line.Group, name = name[:dot], name[dot+1:]
	}
	if name == "" {
		return Line{}, false
	}
	line.Name = name
	line.Params = segments[1:]
	line.Value = raw[colon+1:]
	return line, true
}

// Is reports whether the line is the named property, ignoring group and case.
func (l Line) Is(name string) bool {
	return strings.EqualFold(l.Name, name)
}

// ParamValues returns the unquoted values of the named parameter.
func (l Line) ParamValues(name string) []string {
	var values []string
	for _, param := range l.Params {
		key, value, ok := strings.Cut(param, "=")
		if !ok || !strings.EqualFold(strings.TrimSpace(key), name) {
			continue
		}
		for _, v := range splitOutsideQuotes(value, ',') {
			values = append(values, strings.Trim(strings.TrimSpace(v), `"`))
		}
	}
	return values
}

// WithoutParam returns the line with every instance of the named parameter
// removed.
func (l Line) WithoutParam(name string) Line {
	kept := make([]string, 0, len(l.Params))
	for _, param := range l.Params {
		key, _, _ := strings.Cut(param, "=")
		if strings.EqualFold(strings.TrimSpace(key), name) {
			continue
		}
		kept = append(kept, param)
	}
	l.Params = kept
	return l
}

func (l Line) String() string {
	var b strings.Builder
	if l.Group != "" {
		b.WriteString(l.Group)
		b.WriteByte('.')
	}
	b.WriteString(l.Name)
	for _, param := range l.Params {
		b.WriteByte(';')
		b.WriteString(param)
	}
	b.WriteByte(':')
	b.WriteString(l.Value)
	return b.String()
}

func indexOutsideQuotes(s string, delimiter byte) int {
	quoted := false
	for i := 0; i < len(s); i++ {
		switch s[i] {
		case '"':
			quoted = !quoted
		case delimiter:
			if !quoted {
				return i
			}
		}
	}
	return -1
}

func splitOutsideQuotes(s string, delimiter byte) []string {
	var parts []string
	for {
		i := indexOutsideQuotes(s, delimiter)
		if i < 0 {
			return append(parts, s)
		}
		parts = append(parts, s[:i])
		s = s[i+1:]
	}
}

// WriteLine writes one content line folded at MaxLineOctets, never inside a
// multi-octet UTF-8 sequence. The continuation's leading space counts toward
// the limit.
func WriteLine(sb *strings.Builder, line string) {
	limit := MaxLineOctets
	for len(line) > limit {
		cut := foldPoint(line, limit)
		sb.WriteString(line[:cut])
		sb.WriteString("\r\n ")
		line = line[cut:]
		limit = MaxLineOctets - 1
	}
	sb.WriteString(line)
	sb.WriteString("\r\n")
}

// foldPoint backs up over UTF-8 continuation octets (10xxxxxx). Input that is
// not valid UTF-8 can be continuation octets all the way back; it folds at the
// limit so the writer still makes progress.
func foldPoint(line string, limit int) int {
	cut := limit
	for cut > 0 && line[cut]&0xC0 == 0x80 {
		cut--
	}
	if cut == 0 {
		return limit
	}
	return cut
}

// IsControlOctet reports whether an octet may not appear in a content line:
// every C0 octet but tab, and DEL. No octet of a multi-octet UTF-8 sequence
// falls below 0x80, so the test is safe octet by octet.
func IsControlOctet(c byte) bool {
	return (c < 0x20 && c != '\t') || c == 0x7F
}

// CheckRawValue refuses a value written into a content line unescaped: any
// control octet, line breaks included, would corrupt or end the line.
func CheckRawValue(s string) error {
	for i := 0; i < len(s); i++ {
		if IsControlOctet(s[i]) {
			return ErrControlCharacter
		}
	}
	return nil
}

// EscapeText escapes a TEXT value (RFC 2426 Section 5, RFC 6350 Section 3.4).
// A line break becomes \n; any other control octet is refused.
func EscapeText(s string) (string, error) {
	var b strings.Builder
	b.Grow(len(s))
	for i := 0; i < len(s); i++ {
		switch c := s[i]; c {
		case '\\', ';', ',':
			b.WriteByte('\\')
			b.WriteByte(c)
		case '\r':
			b.WriteString(`\n`)
			if i+1 < len(s) && s[i+1] == '\n' {
				i++
			}
		case '\n':
			b.WriteString(`\n`)
		default:
			if IsControlOctet(c) {
				return "", ErrControlCharacter
			}
			b.WriteByte(c)
		}
	}
	return b.String(), nil
}

// UnescapeText reverses EscapeText for one TEXT value or component.
func UnescapeText(s string) string {
	if !strings.Contains(s, `\`) {
		return s
	}
	var b strings.Builder
	b.Grow(len(s))
	for i := 0; i < len(s); i++ {
		c := s[i]
		if c != '\\' || i+1 == len(s) {
			b.WriteByte(c)
			continue
		}
		i++
		switch next := s[i]; next {
		case 'n', 'N':
			b.WriteByte('\n')
		default:
			b.WriteByte(next)
		}
	}
	return b.String()
}

// SplitComponents splits a structured value (N, ORG, ADR) at unescaped
// semicolons, leaving each component escaped.
func SplitComponents(value string) []string {
	var parts []string
	start := 0
	for i := 0; i < len(value); i++ {
		switch value[i] {
		case '\\':
			i++
		case ';':
			parts = append(parts, value[start:i])
			start = i + 1
		}
	}
	return append(parts, value[start:])
}
