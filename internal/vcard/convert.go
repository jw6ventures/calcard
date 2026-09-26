package vcard

import (
	"errors"
	"fmt"
	"strings"
	"unicode/utf8"
)

// ErrUnsupportedCharset reports a vCard 2.1 CHARSET this converter cannot decode.
var ErrUnsupportedCharset = errors.New("unsupported character set")

// SplitCards returns each top-level BEGIN:VCARD ... END:VCARD block of a VCF
// file as written, with CRLF line endings. Physical lines are kept as they are,
// so vCard 2.1 quoted-printable soft line breaks survive for Convert21To30. A
// BEGIN:VCARD nests only as the value of a vCard 2.1 AGENT property; anywhere
// else it starts a new card, so a card missing its END:VCARD cannot swallow
// the cards after it. Such a card, and one cut off by the end of the file, is
// still returned so that its refusal can be reported.
func SplitCards(content string) []string {
	var cards []string
	var current []string
	depth := 0
	agentValue := false
	emit := func() {
		if len(current) > 0 {
			cards = append(cards, strings.Join(current, "\r\n")+"\r\n")
		}
		current, depth, agentValue = nil, 0, false
	}
	for _, line := range physicalLines(content) {
		trimmed := strings.TrimSpace(line)
		if isDelimiterLine(line, "BEGIN") {
			if depth > 0 && !agentValue {
				emit()
			}
			depth++
			agentValue = false
			current = append(current, line)
			continue
		}
		if depth == 0 {
			continue
		}
		current = append(current, line)
		if isDelimiterLine(line, "END") {
			depth--
			if depth == 0 {
				emit()
			}
			continue
		}
		if trimmed != "" && !isContinuationLine(line) {
			agentValue = isEmptyAgent(trimmed)
		}
	}
	emit()
	return cards
}

// physicalLines splits content at CRLF, LF or a bare CR.
func physicalLines(content string) []string {
	content = strings.ReplaceAll(content, "\r\n", "\n")
	content = strings.ReplaceAll(content, "\r", "\n")
	return strings.Split(strings.TrimSuffix(content, "\n"), "\n")
}

// isContinuationLine reports a folded continuation, which belongs to the value
// before it however it reads.
func isContinuationLine(line string) bool {
	return line != "" && (line[0] == ' ' || line[0] == '\t')
}

// isDelimiterLine reports a BEGIN:VCARD or END:VCARD physical line; a folded
// continuation spelling one out is value text.
func isDelimiterLine(line, keyword string) bool {
	return !isContinuationLine(line) && isDelimiter(line, keyword)
}

// isEmptyAgent reports a vCard 2.1 AGENT property whose value is the card on
// the lines that follow.
func isEmptyAgent(line string) bool {
	parsed, ok := ParseLine(line)
	return ok && parsed.Is("AGENT") && strings.TrimSpace(parsed.Value) == ""
}

// Version returns the VERSION a card declares, or "" when it declares none.
func Version(card string) string {
	if versions := ReadStructure(card).Versions; len(versions) > 0 {
		return versions[0]
	}
	return ""
}

// Convert21To30 rewrites one vCard 2.1 card as vCard 3.0: quoted-printable and
// CHARSET values are decoded to UTF-8, bare parameter values become TYPE
// values, base64 becomes ENCODING=b, text values are escaped as RFC 2426 TEXT,
// and a card without FN gets one derived from N or ORG.
//
// An AGENT property carrying an embedded card is dropped with that card:
// vCard 3.0 would need it re-encoded as an escaped text value, RFC 6350
// removed AGENT, and the rest of the card is worth more than refusing it.
func Convert21To30(card string) (string, error) {
	logical := unfold21(withoutAgentCards(physicalLines(card)))
	if len(logical) < 2 || !isDelimiter(logical[0], "BEGIN") || !isDelimiter(logical[len(logical)-1], "END") {
		return "", malformed("not a single vCard")
	}
	body := logical[1 : len(logical)-1]

	var out []string
	var family, given, org string
	hasFN, hasN := false, false
	versions := 0
	for _, raw := range body {
		if isDelimiter(raw, "BEGIN") || isDelimiter(raw, "END") {
			return "", malformed("not a single vCard")
		}
		line, ok := ParseLine(raw)
		if !ok {
			return "", malformed(fmt.Sprintf("malformed content line %q", truncate(raw)))
		}
		if line.Is("VERSION") {
			if strings.TrimSpace(line.Value) != "2.1" {
				return "", malformed("not a vCard 2.1 card")
			}
			versions++
			continue
		}
		converted, components, err := convert21Line(line)
		if err != nil {
			return "", err
		}
		switch strings.ToUpper(line.Name) {
		case "FN":
			if strings.TrimSpace(components[0]) == "" {
				continue
			}
			hasFN = true
		case "N":
			hasN = true
			if len(components) > 1 {
				family, given = components[0], components[1]
			} else {
				family = components[0]
			}
		case "ORG":
			org = components[0]
		}
		out = append(out, converted)
	}
	if versions != 1 {
		return "", malformed("not a vCard 2.1 card")
	}

	if !hasFN {
		name := strings.TrimSpace(strings.TrimSpace(given) + " " + strings.TrimSpace(family))
		if name == "" {
			name = strings.TrimSpace(org)
		}
		if name == "" {
			return "", &kindError{message: "card has no name", kind: ErrMissingFN}
		}
		escaped, err := EscapeText(name)
		if err != nil {
			return "", err
		}
		out = append([]string{"FN:" + escaped}, out...)
	}
	if !hasN {
		// RFC 2426 Section 3.1.2 makes N mandatory in vCard 3.0.
		out = append([]string{"N:;;;;"}, out...)
	}

	var sb strings.Builder
	WriteLine(&sb, "BEGIN:VCARD")
	WriteLine(&sb, "VERSION:3.0")
	for _, line := range out {
		WriteLine(&sb, line)
	}
	WriteLine(&sb, "END:VCARD")
	return sb.String(), nil
}

// withoutAgentCards drops each AGENT property whose value is an embedded card,
// together with that card.
func withoutAgentCards(physical []string) []string {
	kept := make([]string, 0, len(physical))
	for i := 0; i < len(physical); i++ {
		trimmed := strings.TrimSpace(physical[i])
		if isContinuationLine(physical[i]) || !isEmptyAgent(trimmed) {
			kept = append(kept, physical[i])
			continue
		}
		next := i + 1
		for next < len(physical) && strings.TrimSpace(physical[next]) == "" {
			next++
		}
		if next == len(physical) || !isDelimiterLine(physical[next], "BEGIN") {
			kept = append(kept, physical[i])
			continue
		}
		depth := 0
		for i = next; i < len(physical); i++ {
			switch {
			case isDelimiterLine(physical[i], "BEGIN"):
				depth++
			case isDelimiterLine(physical[i], "END"):
				depth--
			}
			if depth == 0 {
				break
			}
		}
	}
	return kept
}

// unfold21 joins folded lines and quoted-printable soft line breaks, each
// logical line joined once. vCard 2.1 folds as RFC 822 does, so the whitespace
// starting a continuation is part of the value. A quoted-printable value
// ending in "=" continues on the next physical line unless that line closes
// or opens a card.
func unfold21(physical []string) []string {
	var pieces [][]string
	for i := 0; i < len(physical); i++ {
		line := physical[i]
		if strings.TrimSpace(line) == "" {
			continue
		}
		if len(pieces) > 0 && (line[0] == ' ' || line[0] == '\t') {
			last := len(pieces) - 1
			pieces[last] = append(pieces[last], line)
			continue
		}
		parts := []string{line}
		if isQuotedPrintable(line) {
			for i+1 < len(physical) && strings.HasSuffix(parts[len(parts)-1], "=") {
				if isDelimiterLine(physical[i+1], "END") || isDelimiterLine(physical[i+1], "BEGIN") {
					break
				}
				tail := parts[len(parts)-1]
				parts[len(parts)-1] = tail[:len(tail)-1]
				i++
				parts = append(parts, physical[i])
			}
		}
		pieces = append(pieces, parts)
	}
	lines := make([]string, len(pieces))
	for i, parts := range pieces {
		lines[i] = strings.Join(parts, "")
	}
	return lines
}

func isQuotedPrintable(raw string) bool {
	line, ok := ParseLine(raw)
	if !ok {
		return false
	}
	for _, param := range line.Params {
		name, value, named := strings.Cut(param, "=")
		if !named && strings.EqualFold(strings.TrimSpace(name), "QUOTED-PRINTABLE") {
			return true
		}
		if named && strings.EqualFold(strings.TrimSpace(name), "ENCODING") && strings.EqualFold(strings.Trim(strings.TrimSpace(value), `"`), "QUOTED-PRINTABLE") {
			return true
		}
	}
	return false
}

// convert21Line rewrites one content line and returns its decoded text
// components (one for a non-structured value).
func convert21Line(line Line) (string, []string, error) {
	var types, kept []string
	encoding, charset := "", ""
	for _, param := range line.Params {
		name, value, named := strings.Cut(param, "=")
		name, value = strings.TrimSpace(name), strings.Trim(strings.TrimSpace(value), `"`)
		if !named {
			switch strings.ToUpper(name) {
			case "QUOTED-PRINTABLE", "BASE64", "8BIT", "7BIT":
				encoding = strings.ToUpper(name)
			case "":
			default:
				types = append(types, name)
			}
			continue
		}
		switch strings.ToUpper(name) {
		case "TYPE":
			types = append(types, strings.Split(value, ",")...)
		case "ENCODING":
			encoding = strings.ToUpper(value)
		case "CHARSET":
			charset = value
		case "VALUE":
			if strings.EqualFold(value, "URL") {
				value = "uri"
			}
			kept = append(kept, name+"="+value)
		default:
			kept = append(kept, param)
		}
	}

	value := line.Value
	switch encoding {
	case "QUOTED-PRINTABLE":
		decoded, err := decodeQuotedPrintable(value)
		if err != nil {
			return "", nil, fmt.Errorf("%s: %w", line.Name, err)
		}
		value = decoded
	case "BASE64", "B":
		value = strings.Join(strings.Fields(value), "")
	}
	if encoding != "BASE64" && encoding != "B" {
		text, err := decodeCharset(value, charset)
		if err != nil {
			return "", nil, fmt.Errorf("%s: %w", line.Name, err)
		}
		value = text
	}

	var components []string
	switch upper := strings.ToUpper(line.Name); {
	case encoding == "BASE64" || encoding == "B":
		components = []string{value}
		if err := CheckRawValue(value); err != nil {
			return "", nil, &kindError{message: line.Name + ": " + err.Error(), kind: ErrMalformed}
		}
	case upper == "N" || upper == "ADR" || upper == "ORG":
		escaped := make([]string, 0, 7)
		for _, component := range splitComponents21(value) {
			text := unescape21(component)
			components = append(components, text)
			e, err := EscapeText(text)
			if err != nil {
				return "", nil, fmt.Errorf("%s: %w", line.Name, err)
			}
			escaped = append(escaped, e)
		}
		value = strings.Join(escaped, ";")
	case strings.HasPrefix(upper, "X-"):
		// An extension property's value is passed through: its delimiters
		// may be structure, so only lone backslashes and the line breaks a
		// decoded value may carry get the escapes 3.0 requires.
		value = escapeLineBreaks(escapeExtension21(value))
		components = []string{value}
		if err := CheckRawValue(value); err != nil {
			return "", nil, fmt.Errorf("%s: %w", line.Name, err)
		}
	case isText21(upper):
		text := unescape21(value)
		components = []string{text}
		e, err := EscapeText(text)
		if err != nil {
			return "", nil, fmt.Errorf("%s: %w", line.Name, err)
		}
		value = e
	default:
		components = []string{value}
		if err := CheckRawValue(value); err != nil {
			return "", nil, fmt.Errorf("%s: %w", line.Name, err)
		}
	}

	out := Line{Group: line.Group, Name: line.Name, Value: value}
	if len(types) > 0 {
		out.Params = append(out.Params, "TYPE="+strings.Join(types, ","))
	}
	out.Params = append(out.Params, kept...)
	if encoding == "BASE64" || encoding == "B" {
		out.Params = append(out.Params, "ENCODING=b")
	}
	return out.String(), components, nil
}

func isText21(upperName string) bool {
	switch upperName {
	case "FN", "NOTE", "TITLE", "ROLE", "LABEL", "SORT-STRING":
		return true
	}
	return false
}

// escapeExtension21 doubles each backslash that is not already an escape both
// versions spell alike: \; and \, for the delimiter, \\ for a backslash.
func escapeExtension21(s string) string {
	if !strings.Contains(s, `\`) {
		return s
	}
	var b strings.Builder
	b.Grow(len(s) + 4)
	for i := 0; i < len(s); i++ {
		if s[i] != '\\' {
			b.WriteByte(s[i])
			continue
		}
		if i+1 < len(s) && (s[i+1] == ';' || s[i+1] == ',' || s[i+1] == '\\') {
			b.WriteString(s[i : i+2])
			i++
			continue
		}
		b.WriteString(`\\`)
	}
	return b.String()
}

func escapeLineBreaks(s string) string {
	s = strings.ReplaceAll(s, "\r\n", "\n")
	s = strings.ReplaceAll(s, "\r", "\n")
	return strings.ReplaceAll(s, "\n", `\n`)
}

// splitComponents21 splits a vCard 2.1 structured value at semicolons not
// escaped with a backslash.
func splitComponents21(value string) []string {
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

// unescape21 undoes the only escapes vCard 2.1 defines: \; and \\.
func unescape21(s string) string {
	if !strings.Contains(s, `\`) {
		return s
	}
	var b strings.Builder
	for i := 0; i < len(s); i++ {
		if s[i] == '\\' && i+1 < len(s) && (s[i+1] == ';' || s[i+1] == '\\') {
			i++
		}
		b.WriteByte(s[i])
	}
	return b.String()
}

func decodeQuotedPrintable(s string) (string, error) {
	var b strings.Builder
	b.Grow(len(s))
	for i := 0; i < len(s); i++ {
		if s[i] != '=' {
			b.WriteByte(s[i])
			continue
		}
		if i == len(s)-1 {
			// A soft line break with nothing after it ends the value.
			break
		}
		if i+2 >= len(s) {
			return "", malformed("invalid quoted-printable value")
		}
		hi, okHi := hexValue(s[i+1])
		lo, okLo := hexValue(s[i+2])
		if !okHi || !okLo {
			return "", malformed("invalid quoted-printable value")
		}
		b.WriteByte(hi<<4 | lo)
		i += 2
	}
	return b.String(), nil
}

func hexValue(c byte) (byte, bool) {
	switch {
	case '0' <= c && c <= '9':
		return c - '0', true
	case 'a' <= c && c <= 'f':
		return c - 'a' + 10, true
	case 'A' <= c && c <= 'F':
		return c - 'A' + 10, true
	}
	return 0, false
}

// decodeCharset turns value octets in charset into UTF-8. vCard 2.1 defaults
// to US-ASCII, but exporters that omit CHARSET commonly write Windows-1252, so
// octets that are not UTF-8 are read as that.
func decodeCharset(value, charset string) (string, error) {
	switch strings.ToUpper(strings.TrimSpace(charset)) {
	case "", "UTF-8", "UTF8", "US-ASCII", "ASCII":
		if utf8.ValidString(value) {
			return value, nil
		}
		if charset != "" && !strings.EqualFold(charset, "US-ASCII") && !strings.EqualFold(charset, "ASCII") {
			return "", malformed("value is not valid " + charset)
		}
		return decodeWindows1252(value), nil
	case "ISO-8859-1", "LATIN1", "ISO_8859-1":
		runes := make([]rune, len(value))
		for i := 0; i < len(value); i++ {
			runes[i] = rune(value[i])
		}
		return string(runes), nil
	case "WINDOWS-1252", "CP1252":
		return decodeWindows1252(value), nil
	default:
		return "", fmt.Errorf("%w %s", ErrUnsupportedCharset, charset)
	}
}

// windows1252High maps 0x80-0x9F, where Windows-1252 differs from ISO-8859-1.
var windows1252High = [32]rune{
	'€', 0x81, '‚', 'ƒ', '„', '…', '†', '‡', 'ˆ', '‰', 'Š', '‹', 'Œ', 0x8D, 'Ž', 0x8F,
	0x90, '‘', '’', '“', '”', '•', '–', '—', '˜', '™', 'š', '›', 'œ', 0x9D, 'ž', 'Ÿ',
}

func decodeWindows1252(value string) string {
	var b strings.Builder
	b.Grow(len(value))
	for i := 0; i < len(value); i++ {
		c := value[i]
		if c >= 0x80 && c <= 0x9F {
			b.WriteRune(windows1252High[c-0x80])
			continue
		}
		b.WriteRune(rune(c))
	}
	return b.String()
}

func truncate(s string) string {
	if len(s) > 40 {
		return s[:40] + "..."
	}
	return s
}
