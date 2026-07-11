package ical

import (
	"fmt"
	"strings"
	"time"
)

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

	for _, format := range icalDateTimeFormats {
		if t, err := time.Parse(format, s); err == nil {
			return t.UTC(), nil
		}
	}

	return time.Time{}, fmt.Errorf("invalid datetime format: %s", s)
}

// ParseDateTimeInLocation parses a floating DATE or DATE-TIME value in loc;
// values carrying their own zone information ignore loc.
func ParseDateTimeInLocation(s string, loc *time.Location) (time.Time, error) {
	if loc == nil || hasZoneSuffix(s) {
		return ParseDateTime(s)
	}

	for _, format := range icalLocalFormats {
		if t, err := time.ParseInLocation(format, s, loc); err == nil {
			return t.UTC(), nil
		}
	}

	return time.Time{}, fmt.Errorf("invalid datetime format: %s", s)
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

// ExtractRRuleParam extracts a parameter value from an RRULE string.
// Example: "FREQ=WEEKLY;BYDAY=MO,WE,FR" -> ExtractRRuleParam(rrule, "FREQ") returns "WEEKLY".
func ExtractRRuleParam(rrule, param string) string {
	parts := strings.Split(rrule, ";")
	for _, part := range parts {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		if idx := strings.Index(part, "="); idx != -1 {
			if strings.EqualFold(part[:idx], param) {
				return part[idx+1:]
			}
		}
	}
	return ""
}

// UnfoldLines normalizes line endings and unfolds RFC 5545 folded content
// lines (continuations starting with a space or tab).
func UnfoldLines(raw string) []string {
	raw = strings.ReplaceAll(raw, "\r\n", "\n")
	raw = strings.ReplaceAll(raw, "\r", "\n")
	rawLines := strings.Split(raw, "\n")
	var lines []string
	for _, line := range rawLines {
		if len(lines) > 0 && (strings.HasPrefix(line, " ") || strings.HasPrefix(line, "\t")) {
			lines[len(lines)-1] += strings.TrimLeft(line, " \t")
			continue
		}
		lines = append(lines, line)
	}
	return lines
}
