package ical

import (
	"strings"
	"testing"
	"unicode/utf8"
)

// A content line folded into many continuations, such as an inline photo, is
// unfolded in time linear in its length.
func TestUnfoldLinesScalesLinearly(t *testing.T) {
	assertScalesLinearly(t, 3000, func(n int) func() {
		raw := "BEGIN:VCALENDAR\r\nATTACH:" + strings.Repeat("A", 74) + strings.Repeat("\r\n "+strings.Repeat("A", 74), n) + "\r\nEND:VCALENDAR\r\n"
		return func() {
			if lines := UnfoldLines(raw); len(lines) != 4 {
				t.Fatalf("unfolded into %d lines", len(lines))
			}
		}
	})
}

// A byte order mark is not iCalendar content: only one leading the object is
// dropped, and nothing else is.
func TestTrimByteOrderMark(t *testing.T) {
	for raw, want := range map[string]string{
		"\xef\xbb\xbfBEGIN:VCALENDAR\r\n":             "BEGIN:VCALENDAR\r\n",
		"BEGIN:VCALENDAR\r\n":                         "BEGIN:VCALENDAR\r\n",
		"\xef\xbb\xbf\xef\xbb\xbfBEGIN:VCALENDAR\r\n": "\xef\xbb\xbfBEGIN:VCALENDAR\r\n",
		"BEGIN:VCALENDAR\r\nSUMMARY:\xef\xbb\xbf\r\n": "BEGIN:VCALENDAR\r\nSUMMARY:\xef\xbb\xbf\r\n",
	} {
		if got := TrimByteOrderMark(raw); got != want {
			t.Errorf("TrimByteOrderMark(%q) = %q, want %q", raw, got, want)
		}
	}
	if r, size := utf8.DecodeRuneInString(ByteOrderMark); r != 0xFEFF || size != len(ByteOrderMark) {
		t.Errorf("ByteOrderMark = %q, want U+FEFF in UTF-8", ByteOrderMark)
	}
}
