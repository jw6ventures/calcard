package dav

import (
	"strings"
	"testing"
	"unicode/utf8"

	"github.com/jw6ventures/calcard/internal/ical"
)

// TestWriteICalendarObjectRoundTripsParsedInput pins the contract the §9.6
// projection depends on: whatever parseICalendarObject accepted, the writer
// gives back unchanged apart from the normalizations the parser itself
// performs. A projection that selects everything must not rewrite the octets.
func TestWriteICalendarObjectRoundTripsParsedInput(t *testing.T) {
	cases := map[string]string{
		"a plain event": strings.Join([]string{
			"BEGIN:VCALENDAR",
			"VERSION:2.0",
			"PRODID:-//CalCard//EN",
			"BEGIN:VEVENT",
			"UID:event-1",
			"DTSTAMP:20240601T090000Z",
			"DTSTART:20240601T100000Z",
			"DTEND:20240601T110000Z",
			"SUMMARY:Test Event",
			"END:VEVENT",
			"END:VCALENDAR",
		}, "\r\n") + "\r\n",
		"a zoned value keeps its TZID spelling": strings.Join([]string{
			"BEGIN:VCALENDAR",
			"VERSION:2.0",
			"PRODID:-//CalCard//EN",
			"BEGIN:VTIMEZONE",
			"TZID:America/New_York",
			"BEGIN:STANDARD",
			"DTSTART:19701101T020000",
			"TZOFFSETFROM:-0400",
			"TZOFFSETTO:-0500",
			"END:STANDARD",
			"END:VTIMEZONE",
			"BEGIN:VEVENT",
			"UID:event-2",
			"DTSTAMP:20240601T090000Z",
			"DTSTART;TZID=America/New_York:20240601T100000",
			"END:VEVENT",
			"END:VCALENDAR",
		}, "\r\n") + "\r\n",
		"a quoted parameter carrying a colon and a semicolon": strings.Join([]string{
			"BEGIN:VCALENDAR",
			"VERSION:2.0",
			"PRODID:-//CalCard//EN",
			"BEGIN:VEVENT",
			"UID:event-3",
			"DTSTAMP:20240601T090000Z",
			"DTSTART:20240601T100000Z",
			`ATTENDEE;CN="Doe; John: chair";ROLE=CHAIR:mailto:john@example.com`,
			"END:VEVENT",
			"END:VCALENDAR",
		}, "\r\n") + "\r\n",
		"escaped text keeps its backslashes": strings.Join([]string{
			"BEGIN:VCALENDAR",
			"VERSION:2.0",
			"PRODID:-//CalCard//EN",
			"BEGIN:VEVENT",
			"UID:event-4",
			"DTSTAMP:20240601T090000Z",
			"DTSTART:20240601T100000Z",
			`SUMMARY:a\,b\nc\;d`,
			"END:VEVENT",
			"END:VCALENDAR",
		}, "\r\n") + "\r\n",
		"non-standard names in every position": strings.Join([]string{
			"BEGIN:VCALENDAR",
			"VERSION:2.0",
			"PRODID:-//CalCard//EN",
			"BEGIN:VEVENT",
			"UID:event-5",
			"DTSTAMP:20240601T090000Z",
			"DTSTART:20240601T100000Z",
			"X-ALT-DESC;X-VENDOR-FLAG=on:custom",
			"BEGIN:X-WOMBAT",
			"X-DEPTH:1",
			"END:X-WOMBAT",
			"END:VEVENT",
			"END:VCALENDAR",
		}, "\r\n") + "\r\n",
		"a nested alarm": strings.Join([]string{
			"BEGIN:VCALENDAR",
			"VERSION:2.0",
			"PRODID:-//CalCard//EN",
			"BEGIN:VEVENT",
			"UID:event-6",
			"DTSTAMP:20240601T090000Z",
			"DTSTART:20240601T100000Z",
			"BEGIN:VALARM",
			"ACTION:DISPLAY",
			"DESCRIPTION:Reminder",
			"TRIGGER:-PT15M",
			"END:VALARM",
			"END:VEVENT",
			"END:VCALENDAR",
		}, "\r\n") + "\r\n",
	}

	for name, raw := range cases {
		t.Run(name, func(t *testing.T) {
			root, err := parseICalendarObject(raw)
			if err != nil {
				t.Fatalf("parseICalendarObject: %v", err)
			}
			if got := writeICalendarObject(root); got != raw {
				t.Fatalf("round trip changed the octets:\n got %q\nwant %q", got, raw)
			}
		})
	}
}

// TestWriteICalendarObjectNormalizesWhatTheParserNormalizes covers the three
// things a round trip deliberately does not preserve, because the parser has
// already discarded them: the original line endings, the original fold points,
// and the case of a component name.
func TestWriteICalendarObjectNormalizesWhatTheParserNormalizes(t *testing.T) {
	raw := "begin:vcalendar\n" +
		"VERSION:2.0\n" +
		"PRODID:-//CalCard//EN\n" +
		"begin:vevent\n" +
		"UID:event-1\n" +
		"DTSTAMP:20240601T090000Z\n" +
		"DTSTART:20240601T100000Z\n" +
		"SUMMARY:folded\n  value\n" +
		"end:vevent\n" +
		"end:vcalendar\n"

	want := strings.Join([]string{
		"BEGIN:VCALENDAR",
		"VERSION:2.0",
		"PRODID:-//CalCard//EN",
		"BEGIN:VEVENT",
		"UID:event-1",
		"DTSTAMP:20240601T090000Z",
		"DTSTART:20240601T100000Z",
		"SUMMARY:folded value",
		"END:VEVENT",
		"END:VCALENDAR",
	}, "\r\n") + "\r\n"

	root, err := parseICalendarObject(raw)
	if err != nil {
		t.Fatalf("parseICalendarObject: %v", err)
	}
	if got := writeICalendarObject(root); got != want {
		t.Fatalf("normalization changed:\n got %q\nwant %q", got, want)
	}
}

// TestWriteICalendarObjectFoldsAtSeventyFiveOctets pins RFC 5545 §3.1: no
// content line exceeds 75 octets, a continuation begins with one space, and a
// multi-byte character is never split across the fold.
func TestWriteICalendarObjectFoldsAtSeventyFiveOctets(t *testing.T) {
	cases := map[string]string{
		"ascii":      strings.Repeat("a", 300),
		"multi-byte": strings.Repeat("é☃", 100),
	}

	for name, summary := range cases {
		t.Run(name, func(t *testing.T) {
			raw := strings.Join([]string{
				"BEGIN:VCALENDAR",
				"VERSION:2.0",
				"PRODID:-//CalCard//EN",
				"BEGIN:VEVENT",
				"UID:event-1",
				"DTSTAMP:20240601T090000Z",
				"DTSTART:20240601T100000Z",
				"SUMMARY:" + summary,
				"END:VEVENT",
				"END:VCALENDAR",
			}, "\r\n") + "\r\n"

			root, err := parseICalendarObject(raw)
			if err != nil {
				t.Fatalf("parseICalendarObject: %v", err)
			}
			written := writeICalendarObject(root)

			if !strings.HasSuffix(written, "\r\n") {
				t.Fatalf("output does not end with CRLF: %q", written)
			}
			for _, line := range strings.Split(strings.TrimSuffix(written, "\r\n"), "\r\n") {
				if len(line) > 75 {
					t.Fatalf("line of %d octets exceeds the 75-octet limit: %q", len(line), line)
				}
				if strings.HasPrefix(line, " ") && strings.HasPrefix(line[1:], " ") {
					t.Fatalf("continuation carries more than one leading space: %q", line)
				}
				if !utf8.ValidString(line) {
					t.Fatalf("fold split a UTF-8 sequence: %q", line)
				}
			}

			unfolded := ical.UnfoldLines(written)
			if !containsLine(unfolded, "SUMMARY:"+summary) {
				t.Fatalf("unfolding did not reproduce the original content line; got %v", unfolded)
			}
		})
	}
}

// TestWriteFoldedContentLineLeavesShortLinesAlone keeps a line that already
// fits from acquiring a spurious fold.
func TestWriteFoldedContentLineLeavesShortLinesAlone(t *testing.T) {
	line := "SUMMARY:" + strings.Repeat("a", 67)
	if len(line) != 75 {
		t.Fatalf("fixture is %d octets, want exactly 75", len(line))
	}
	var b strings.Builder
	writeFoldedContentLine(&b, line)
	if got := b.String(); got != line+"\r\n" {
		t.Fatalf("a 75-octet line was folded: %q", got)
	}
}

func containsLine(lines []string, want string) bool {
	for _, line := range lines {
		if line == want {
			return true
		}
	}
	return false
}
