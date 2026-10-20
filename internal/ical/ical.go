package ical

import (
	"fmt"
	"iter"
	"strings"
	"time"
)

// The CalDAV collection limits RFC 4791 §5.2.5 through §5.2.9 let a server
// advertise. They live here rather than in internal/dav because the same
// calendar data is reachable through the DAV handlers and the events service,
// and validating it against two different sets of limits would let a client
// store through one path what the other refuses -- while the CalDAV properties
// advertise only one of them.
const (
	// MinDateTime and MaxDateTime are CALDAV:min-date-time and
	// CALDAV:max-date-time, inclusive at both ends.
	MinDateTime = "19000101T000000Z"
	MaxDateTime = "21001231T235959Z"
	// MaxRecurrenceInstances is CALDAV:max-instances, the instance count one
	// recurrence set may generate.
	MaxRecurrenceInstances = 1000
	// MaxAttendeesPerInstance is CALDAV:max-attendees-per-instance, counted per
	// instance rather than per resource.
	MaxAttendeesPerInstance = 100
)

var (
	minDateTimeValue time.Time
	maxDateTimeValue time.Time
)

func init() {
	var err error
	if minDateTimeValue, err = ParseDateTime(MinDateTime); err != nil {
		panic(fmt.Sprintf("invalid MinDateTime constant: %v", err))
	}
	if maxDateTimeValue, err = ParseDateTime(MaxDateTime); err != nil {
		panic(fmt.Sprintf("invalid MaxDateTime constant: %v", err))
	}
}

// DateLimits returns the parsed CALDAV:min-date-time and CALDAV:max-date-time
// bounds. Both are inclusive: RFC 4791 §5.2.6 and §5.2.7 define them as the
// earliest and latest date a resource may carry.
func DateLimits() (time.Time, time.Time) {
	return minDateTimeValue, maxDateTimeValue
}

var icalDateTimeFormats = []string{
	"20060102",             // Date only
	"20060102T150405",      // Basic format
	"20060102T150405Z",     // UTC format
	"20060102T150405-0700", // Basic format with offset
	"20060102T150405-07:00",
	"2006-01-02T15:04:05",  // Extended format
	"2006-01-02T15:04:05Z", // Extended UTC
	"2006-01-02T15:04:05-0700",
	"2006-01-02T15:04:05-07:00",
}

var icalLocalFormats = []string{
	"20060102",
	"20060102T150405",
	"2006-01-02T15:04:05",
}

// ParseDateTime parses an iCalendar DATE or DATE-TIME value in UTC.
func ParseDateTime(s string) (time.Time, error) {
	if s == "" {
		return time.Time{}, fmt.Errorf("empty datetime")
	}
	s = normalizeLeapSecond(s)

	if t, ok := parseBasicDateTime(s); ok {
		return t, nil
	}
	for _, format := range icalDateTimeFormats {
		if t, err := time.Parse(format, s); err == nil {
			return t.UTC(), nil
		}
	}

	return time.Time{}, fmt.Errorf("invalid datetime format: %s", s)
}

// parseBasicDateTime reads the three forms RFC 5545 writes a date or a
// date-time in -- 20060102, 20060102T150405 and 20060102T150405Z -- as the first
// three of icalDateTimeFormats read them, without the cost of time.Parse, which
// is most of what a report spends on a calendar of recurring events. ok is
// false for anything else, which the caller reads in full, and for a value
// those layouts refuse: a month or day the calendar does not have, a time of
// day past 23:59:59. The value is read in UTC, and the form ending in Z is the
// same instant.
func parseBasicDateTime(s string) (time.Time, bool) {
	switch len(s) {
	case 8, 15, 16:
	default:
		return time.Time{}, false
	}
	digits := func(from, to int) (n int, ok bool) {
		for i := from; i < to; i++ {
			c := s[i]
			if c < '0' || c > '9' {
				return 0, false
			}
			n = n*10 + int(c-'0')
		}
		return n, true
	}
	year, ok1 := digits(0, 4)
	month, ok2 := digits(4, 6)
	day, ok3 := digits(6, 8)
	if !ok1 || !ok2 || !ok3 {
		return time.Time{}, false
	}
	var hour, minute, second int
	if len(s) > 8 {
		if s[8] != 'T' {
			return time.Time{}, false
		}
		var okH, okM, okS bool
		hour, okH = digits(9, 11)
		minute, okM = digits(11, 13)
		second, okS = digits(13, 15)
		if !okH || !okM || !okS || hour > 23 || minute > 59 || second > 59 {
			return time.Time{}, false
		}
		if len(s) == 16 && s[15] != 'Z' {
			return time.Time{}, false
		}
	}
	if month < 1 || month > 12 || day < 1 {
		return time.Time{}, false
	}
	t := time.Date(year, time.Month(month), day, hour, minute, second, 0, time.UTC)
	if t.Day() != day || int(t.Month()) != month {
		return time.Time{}, false
	}
	return t, true
}

// ParseDateTimeInLocation parses a floating DATE or DATE-TIME value in loc,
// reading a wall clock a transition skips or repeats as WallClockIn does;
// values carrying their own zone information ignore loc.
func ParseDateTimeInLocation(s string, loc *time.Location) (time.Time, error) {
	if loc == nil || hasZoneSuffix(s) {
		return ParseDateTime(s)
	}
	s = normalizeLeapSecond(s)

	if wall, ok := parseBasicDateTime(s); ok {
		if loc == time.UTC {
			return wall, nil
		}
		return WallClockIn(loc, wall).UTC(), nil
	}
	for _, format := range icalLocalFormats {
		if wall, err := time.Parse(format, s); err == nil {
			if loc == time.UTC {
				return wall, nil
			}
			return WallClockIn(loc, wall).UTC(), nil
		}
	}

	return time.Time{}, fmt.Errorf("invalid datetime format: %s", s)
}

// RFC 5545 permits second 60 for a positive leap second and recommends that
// implementations without leap-second support interpret it as second 59.
func normalizeLeapSecond(value string) string {
	timeSeparator := strings.IndexByte(value, 'T')
	if timeSeparator < 0 {
		return value
	}
	secondStart := timeSeparator + 5
	if len(value) > timeSeparator+3 && value[timeSeparator+3] == ':' {
		secondStart = timeSeparator + 7
	}
	if secondStart+2 > len(value) || value[secondStart:secondStart+2] != "60" {
		return value
	}
	return value[:secondStart] + "59" + value[secondStart+2:]
}

func hasZoneSuffix(s string) bool {
	if strings.HasSuffix(s, "Z") {
		return true
	}
	if len(s) >= 5 {
		tail := s[len(s)-5:]
		if (tail[0] == '+' || tail[0] == '-') && isDigits(tail[1:]) {
			return true
		}
	}
	if len(s) >= 6 {
		tail := s[len(s)-6:]
		if (tail[0] == '+' || tail[0] == '-') && tail[3] == ':' && isDigits(tail[1:3]) && isDigits(tail[4:]) {
			return true
		}
	}
	return false
}

func isDigits(s string) bool {
	for _, r := range s {
		if r < '0' || r > '9' {
			return false
		}
	}
	return s != ""
}

// RRuleParam returns the value of the named part of an RRULE value, read as
// the recurrence expansion reads the rule's parts: the name matched without
// regard to case, and white space around the name, the "=" and the value
// ignored. found is false when no part carries the name with an "=". The
// first part of a name answers; a rule repeating one is refused wherever it is
// written, and only stored data predating that check holds one.
func RRuleParam(rrule, name string) (value string, found bool) {
	for key, value := range rruleParts(rrule) {
		if strings.EqualFold(key, name) {
			return value, true
		}
	}
	return "", false
}

// RRuleParts returns every part of an RRULE value as RRuleParam reads it,
// keyed by its name in upper case: a part with no "=" names nothing, and the
// first part of a name answers.
func RRuleParts(rrule string) map[string]string {
	parts := make(map[string]string)
	for key, value := range rruleParts(rrule) {
		key = strings.ToUpper(key)
		if _, seen := parts[key]; !seen {
			parts[key] = value
		}
	}
	return parts
}

// rruleParts yields, in order, the name and value of each part of an RRULE
// value that has an "=", without the white space around either.
func rruleParts(rrule string) iter.Seq2[string, string] {
	return func(yield func(string, string) bool) {
		for part := range strings.SplitSeq(rrule, ";") {
			key, value, ok := strings.Cut(part, "=")
			if ok && !yield(strings.TrimSpace(key), strings.TrimSpace(value)) {
				return
			}
		}
	}
}

// ByteOrderMark is the UTF-8 encoding of U+FEFF, which Windows tools put at the
// start of a text file. It is not iCalendar content, and every reader of an
// object looks for BEGIN:VCALENDAR at its first octet.
const ByteOrderMark = "\xef\xbb\xbf"

// TrimByteOrderMark returns raw without the ByteOrderMark it starts with, if
// any.
func TrimByteOrderMark(raw string) string {
	return strings.TrimPrefix(raw, ByteOrderMark)
}

// UnfoldLines normalizes line endings and unfolds RFC 5545 folded content
// lines (continuations starting with a space or tab). A leading ByteOrderMark
// and whitespace before the first line are dropped, so the first line is the
// object's BEGIN rather than a continuation or an unreadable line.
func UnfoldLines(raw string) []string {
	raw = strings.TrimLeft(TrimByteOrderMark(raw), " \t\r\n")
	raw = strings.ReplaceAll(raw, "\r\n", "\n")
	raw = strings.ReplaceAll(raw, "\r", "\n")
	rawLines := strings.Split(raw, "\n")
	lines := make([]string, 0, len(rawLines))
	for i := 0; i < len(rawLines); {
		line := rawLines[i]
		end := i + 1
		for end < len(rawLines) && isContinuation(rawLines[end]) {
			end++
		}
		if end > i+1 {
			// Joined once, so a line folded many times unfolds in linear time.
			var b strings.Builder
			b.Grow(foldedLength(rawLines[i:end]))
			b.WriteString(line)
			for _, continuation := range rawLines[i+1 : end] {
				b.WriteString(continuation[1:])
			}
			line = b.String()
		}
		lines = append(lines, line)
		i = end
	}
	return lines
}

func isContinuation(line string) bool {
	return strings.HasPrefix(line, " ") || strings.HasPrefix(line, "\t")
}

func foldedLength(lines []string) int {
	n := 0
	for _, line := range lines {
		n += len(line)
	}
	return n
}
