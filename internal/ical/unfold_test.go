package ical

import (
	"strings"
	"testing"
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
