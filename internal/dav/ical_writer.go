package dav

import "strings"

// maxContentLineOctets is the RFC 5545 §3.1 line length, counted in octets and
// excluding the line break.
const maxContentLineOctets = 75

// writeICalendarObject serializes a parsed calendar object back to RFC 5545
// content lines. It is the counterpart of parseICalendarObject, so everything
// that survives the parse survives the round trip: icalProperty.keyPart keeps
// the property name and its parameters exactly as they were written, and the
// value is emitted without re-escaping. The parse itself normalizes line
// endings, fold points and the case of component names, and those do not come
// back.
func writeICalendarObject(node *icalNode) string {
	if node == nil {
		return ""
	}
	var b strings.Builder
	writeICalendarNode(&b, node)
	return b.String()
}

func writeICalendarNode(b *strings.Builder, node *icalNode) {
	writeFoldedContentLine(b, "BEGIN:"+node.name)
	for _, property := range node.properties {
		writeFoldedContentLine(b, property.keyPart+":"+property.value)
	}
	for _, child := range node.children {
		writeICalendarNode(b, child)
	}
	writeFoldedContentLine(b, "END:"+node.name)
}

// writeFoldedContentLine writes one content line, folded per RFC 5545 §3.1: a
// CRLF followed by a single space, never inside a multi-octet UTF-8 sequence.
// The continuation's leading space counts toward the octet limit, so a
// continuation carries one octet less of its own content than the first line.
func writeFoldedContentLine(b *strings.Builder, line string) {
	limit := maxContentLineOctets
	for len(line) > limit {
		cut := foldPoint(line, limit)
		b.WriteString(line[:cut])
		b.WriteString("\r\n ")
		line = line[cut:]
		limit = maxContentLineOctets - 1
	}
	b.WriteString(line)
	b.WriteString("\r\n")
}

// foldPoint is the largest offset at or below limit that does not fall inside a
// UTF-8 sequence. A continuation byte is 10xxxxxx, so backing up over those
// reaches the start of the character the limit landed in. Octets that are not
// valid UTF-8 at all can carry continuation bytes the whole way back; those
// fold at the limit, because splitting a sequence that is already malformed is
// better than making no progress.
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
