package dav

import (
	"fmt"
	"strings"
	"time"
)

// Shared iCalendar helpers used by DAV handlers.

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

func parseICalDateTime(s string) (time.Time, error) {
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

func parseICalDateTimeInLocation(s string, loc *time.Location) (time.Time, error) {
	if loc == nil || hasICalZoneSuffix(s) {
		return parseICalDateTime(s)
	}

	for _, format := range icalLocalFormats {
		if t, err := time.ParseInLocation(format, s, loc); err == nil {
			return t.UTC(), nil
		}
	}

	return time.Time{}, fmt.Errorf("invalid datetime format: %s", s)
}

func hasICalZoneSuffix(s string) bool {
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

func extractRRule(icalData string) string {
	lines := strings.Split(icalData, "\n")
	for _, line := range lines {
		line = strings.TrimSpace(line)
		line = strings.TrimSuffix(line, "\r")
		upperLine := strings.ToUpper(line)
		if strings.HasPrefix(upperLine, "RRULE:") {
			return strings.TrimPrefix(line, "RRULE:")
		}
	}
	return ""
}

// extractRRuleParam extracts a parameter value from an RRULE string
// Example: "FREQ=WEEKLY;BYDAY=MO,WE,FR" -> extractRRuleParam(rrule, "FREQ") returns "WEEKLY"
func extractRRuleParam(rrule, param string) string {
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
