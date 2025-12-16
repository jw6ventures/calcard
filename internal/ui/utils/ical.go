package utils

import (
	"crypto/sha256"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"
)

// GenerateUID creates a unique identifier for calendar/contact objects.
func GenerateUID() string {
	return fmt.Sprintf("%d-%s@calcard", time.Now().UnixNano(), RandomString(8))
}

// GenerateETag creates an ETag from content.
func GenerateETag(content string) string {
	h := sha256.Sum256([]byte(content))
	return fmt.Sprintf("%x", h)
}

// RecurrenceOptions holds recurrence rule parameters.
type RecurrenceOptions struct {
	Frequency string // DAILY, WEEKLY, MONTHLY, YEARLY
	Interval  int    // Every N days/weeks/months/years
	Count     int    // Number of occurrences (0 = no limit)
	Until     string // End date in YYYY-MM-DD format
}

// ParseRecurrenceOptions extracts recurrence options from form data.
func ParseRecurrenceOptions(r *http.Request) *RecurrenceOptions {
	freq := strings.TrimSpace(r.FormValue("recurrence"))
	if freq == "" {
		return nil
	}

	interval := 1
	if i, err := strconv.Atoi(r.FormValue("recurrence_interval")); err == nil && i > 0 {
		interval = i
	}

	var count int
	var until string
	endType := r.FormValue("recurrence_end_type")
	switch endType {
	case "after":
		if c, err := strconv.Atoi(r.FormValue("recurrence_count")); err == nil && c > 0 {
			count = c
		}
	case "on":
		until = strings.TrimSpace(r.FormValue("recurrence_until"))
	}

	return &RecurrenceOptions{
		Frequency: freq,
		Interval:  interval,
		Count:     count,
		Until:     until,
	}
}

// FormatICalDateTime converts form inputs into iCalendar date/time strings.
func FormatICalDateTime(value string, allDay bool, exclusiveEnd bool, prop string) (string, error) {
	if value == "" {
		return "", nil
	}

	if allDay {
		t, err := time.Parse("2006-01-02", value)
		if err != nil {
			return "", err
		}
		if exclusiveEnd {
			t = t.AddDate(0, 0, 1)
		}
		return fmt.Sprintf("%s;VALUE=DATE:%s", prop, t.Format("20060102")), nil
	}

	t, err := time.ParseInLocation("2006-01-02T15:04", value, time.Local)
	if err != nil {
		return "", err
	}
	return fmt.Sprintf("%s:%s", prop, t.UTC().Format("20060102T150405Z")), nil
}

// BuildEventComponent builds a single VEVENT component (without VCALENDAR wrapper).
func BuildEventComponent(uid, summary, dtstart, dtend string, allDay bool, location, description string, recurrence *RecurrenceOptions, recurrenceID string) []string {
	var lines []string
	lines = append(lines, fmt.Sprintf("UID:%s", uid))
	lines = append(lines, fmt.Sprintf("DTSTAMP:%s", time.Now().UTC().Format("20060102T150405Z")))

	if recurrenceID != "" {
		if recLine, err := FormatICalDateTime(recurrenceID, allDay, false, "RECURRENCE-ID"); err == nil && recLine != "" {
			lines = append(lines, recLine)
		}
	}

	if startLine, err := FormatICalDateTime(dtstart, allDay, false, "DTSTART"); err == nil && startLine != "" {
		lines = append(lines, startLine)
	}
	if endLine, err := FormatICalDateTime(dtend, allDay, true, "DTEND"); err == nil && endLine != "" {
		lines = append(lines, endLine)
	}

	lines = append(lines, fmt.Sprintf("SUMMARY:%s", EscapeICalValue(summary)))

	if location != "" {
		lines = append(lines, fmt.Sprintf("LOCATION:%s", EscapeICalValue(location)))
	}

	if description != "" {
		lines = append(lines, fmt.Sprintf("DESCRIPTION:%s", EscapeICalValue(description)))
	}

	if recurrence != nil && recurrence.Frequency != "" && recurrenceID == "" {
		rrule := fmt.Sprintf("RRULE:FREQ=%s", recurrence.Frequency)
		if recurrence.Interval > 1 {
			rrule += fmt.Sprintf(";INTERVAL=%d", recurrence.Interval)
		}
		if recurrence.Count > 0 {
			rrule += fmt.Sprintf(";COUNT=%d", recurrence.Count)
		} else if recurrence.Until != "" {
			if t, err := time.Parse("2006-01-02", recurrence.Until); err == nil {
				rrule += fmt.Sprintf(";UNTIL=%s", t.Format("20060102"))
			}
		}
		lines = append(lines, rrule)
	}

	return lines
}

// BuildEvent constructs a valid iCalendar event.
func BuildEvent(uid, summary, dtstart, dtend string, allDay bool, location, description string, recurrence *RecurrenceOptions) string {
	eventLines := BuildEventComponent(uid, summary, dtstart, dtend, allDay, location, description, recurrence, "")

	var sb strings.Builder
	sb.WriteString("BEGIN:VCALENDAR\r\n")
	sb.WriteString("VERSION:2.0\r\n")
	sb.WriteString("PRODID:-//CalCard//EN\r\n")
	sb.WriteString("BEGIN:VEVENT\r\n")
	for _, line := range eventLines {
		sb.WriteString(line + "\r\n")
	}
	sb.WriteString("END:VEVENT\r\n")
	sb.WriteString("END:VCALENDAR\r\n")

	return sb.String()
}

// UnfoldLines unfolds folded lines without pulling in a full parser.
func UnfoldLines(ical string) []string {
	ical = strings.ReplaceAll(ical, "\r\n", "\n")
	ical = strings.ReplaceAll(ical, "\r", "\n")
	rawLines := strings.Split(ical, "\n")
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

// SplitComponents separates VCALENDAR header/footer and VEVENT components.
func SplitComponents(ical string) (header []string, events [][]string, footer []string) {
	lines := UnfoldLines(ical)
	var current []string
	inEvent := false
	for _, line := range lines {
		switch line {
		case "BEGIN:VEVENT":
			inEvent = true
			current = nil
		case "END:VEVENT":
			if inEvent {
				events = append(events, current)
				current = nil
				inEvent = false
			}
		default:
			if inEvent {
				current = append(current, line)
			} else if len(events) == 0 {
				header = append(header, line)
			} else {
				footer = append(footer, line)
			}
		}
	}
	return
}

// BuildFromComponents reconstructs iCalendar from header, events, and footer.
func BuildFromComponents(header []string, events [][]string, footer []string) string {
	var sb strings.Builder
	write := func(line string) {
		if line == "" {
			return
		}
		if strings.HasSuffix(line, "\r\n") {
			sb.WriteString(line)
			return
		}
		sb.WriteString(line + "\r\n")
	}

	for _, line := range header {
		write(line)
	}
	for _, ev := range events {
		write("BEGIN:VEVENT")
		for _, line := range ev {
			write(line)
		}
		write("END:VEVENT")
	}
	for _, line := range footer {
		write(line)
	}
	return sb.String()
}

// RecurrenceIDValue extracts the RECURRENCE-ID value from event lines.
func RecurrenceIDValue(lines []string) string {
	for _, line := range lines {
		if strings.HasPrefix(line, "RECURRENCE-ID") {
			parts := strings.SplitN(line, ":", 2)
			if len(parts) == 2 {
				return parts[1]
			}
		}
	}
	return ""
}

// HasPropertyValue checks if a property has a specific value.
func HasPropertyValue(lines []string, prop, value string) bool {
	for _, line := range lines {
		if strings.HasPrefix(line, prop) {
			parts := strings.SplitN(line, ":", 2)
			if len(parts) == 2 && parts[1] == value {
				return true
			}
		}
	}
	return false
}

// EscapeICalValue escapes special characters for iCalendar format.
func EscapeICalValue(s string) string {
	s = strings.ReplaceAll(s, "\\", "\\\\")
	s = strings.ReplaceAll(s, ";", "\\;")
	s = strings.ReplaceAll(s, ",", "\\,")
	s = strings.ReplaceAll(s, "\n", "\\n")
	return s
}

// ParseICSFile parses an ICS file and returns individual VEVENT components wrapped in VCALENDAR.
func ParseICSFile(icsContent string) []string {
	lines := UnfoldLines(icsContent)
	var events []string
	var currentEvent []string
	var header []string
	var inEvent bool
	var inCalendar bool

	for _, line := range lines {
		trimmed := strings.TrimSpace(line)

		if trimmed == "BEGIN:VCALENDAR" {
			inCalendar = true
			continue
		}
		if trimmed == "END:VCALENDAR" {
			inCalendar = false
			continue
		}
		if trimmed == "BEGIN:VEVENT" {
			inEvent = true
			currentEvent = []string{}
			continue
		}
		if trimmed == "END:VEVENT" {
			if inEvent && len(currentEvent) > 0 {
				// Wrap the event in VCALENDAR structure
				var eventICAL strings.Builder
				eventICAL.WriteString("BEGIN:VCALENDAR\r\n")
				// Add header properties
				for _, h := range header {
					eventICAL.WriteString(h + "\r\n")
				}
				eventICAL.WriteString("BEGIN:VEVENT\r\n")
				for _, eventLine := range currentEvent {
					eventICAL.WriteString(eventLine + "\r\n")
				}
				eventICAL.WriteString("END:VEVENT\r\n")
				eventICAL.WriteString("END:VCALENDAR\r\n")
				events = append(events, eventICAL.String())
			}
			inEvent = false
			currentEvent = nil
			continue
		}

		if inEvent {
			currentEvent = append(currentEvent, trimmed)
		} else if inCalendar && !inEvent {
			// Store calendar header properties (VERSION, PRODID, etc.)
			if strings.HasPrefix(trimmed, "VERSION:") || strings.HasPrefix(trimmed, "PRODID:") || strings.HasPrefix(trimmed, "CALSCALE:") {
				header = append(header, trimmed)
			}
		}
	}

	return events
}

// ExtractUID extracts the UID from an iCalendar VEVENT.
func ExtractUID(ical string) string {
	lines := UnfoldLines(ical)
	for _, line := range lines {
		if strings.HasPrefix(line, "UID:") {
			return strings.TrimSpace(strings.TrimPrefix(line, "UID:"))
		}
	}
	return ""
}
