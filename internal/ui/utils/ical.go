package utils

import (
	"crypto/sha256"
	"fmt"
	"net/http"
	"net/mail"
	"net/url"
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
	Frequency  string // DAILY, WEEKLY, MONTHLY, YEARLY
	Interval   int    // Every N days/weeks/months/years
	Count      int    // Number of occurrences (0 = no limit)
	Until      string // End date in YYYY-MM-DD format
	ByDay      []string
	ByMonth    int
	ByMonthDay int
}

// EventOptions holds optional VEVENT properties.
type EventOptions struct {
	Timezone     string
	URL          string
	Status       string
	Categories   []string
	Class        string
	Transparency string
	Organizer    string
	Attendees    []string
	Attachments  []string
	Reminders    []int
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

	var byDay []string
	for _, d := range r.Form["recurrence_byday"] {
		d = strings.ToUpper(strings.TrimSpace(d))
		switch d {
		case "MO", "TU", "WE", "TH", "FR", "SA", "SU":
			byDay = append(byDay, d)
		}
	}

	var byMonthDay int
	if v := strings.TrimSpace(r.FormValue("recurrence_bymonthday")); v != "" {
		if md, err := strconv.Atoi(v); err == nil && md >= 1 && md <= 31 {
			byMonthDay = md
		}
	}

	var byMonth int
	if v := strings.TrimSpace(r.FormValue("recurrence_bymonth")); v != "" {
		if m, err := strconv.Atoi(v); err == nil && m >= 1 && m <= 12 {
			byMonth = m
		}
	}

	return &RecurrenceOptions{
		Frequency:  freq,
		Interval:   interval,
		Count:      count,
		Until:      until,
		ByDay:      byDay,
		ByMonth:    byMonth,
		ByMonthDay: byMonthDay,
	}
}

// FormatICalDateTime converts form inputs into iCalendar date/time strings.
func FormatICalDateTime(value string, allDay bool, exclusiveEnd bool, prop string, tzid string) (string, error) {
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

	if tzid != "" {
		if loc, err := time.LoadLocation(tzid); err == nil {
			t, err := parseDateTimeLocal(value, loc)
			if err != nil {
				return "", err
			}
			return fmt.Sprintf("%s;TZID=%s:%s", prop, tzid, t.Format("20060102T150405")), nil
		}
	}

	t, err := parseDateTimeLocal(value, time.Local)
	if err != nil {
		return "", err
	}
	return fmt.Sprintf("%s:%s", prop, t.UTC().Format("20060102T150405Z")), nil
}

// BuildEventComponent builds a single VEVENT component (without VCALENDAR wrapper).
func BuildEventComponent(uid, summary, dtstart, dtend string, allDay bool, location, description string, recurrence *RecurrenceOptions, recurrenceID string, opts *EventOptions) []string {
	var lines []string
	lines = append(lines, fmt.Sprintf("UID:%s", uid))
	lines = append(lines, fmt.Sprintf("DTSTAMP:%s", time.Now().UTC().Format("20060102T150405Z")))

	tzid := ""
	if opts != nil {
		tzid = strings.TrimSpace(opts.Timezone)
	}

	if recurrenceID != "" {
		if recLine, err := FormatICalDateTime(recurrenceID, allDay, false, "RECURRENCE-ID", tzid); err == nil && recLine != "" {
			lines = append(lines, recLine)
		}
	}

	if startLine, err := FormatICalDateTime(dtstart, allDay, false, "DTSTART", tzid); err == nil && startLine != "" {
		lines = append(lines, startLine)
	}
	if endLine, err := FormatICalDateTime(dtend, allDay, true, "DTEND", tzid); err == nil && endLine != "" {
		lines = append(lines, endLine)
	}

	lines = append(lines, fmt.Sprintf("SUMMARY:%s", EscapeICalValue(summary)))

	if location != "" {
		lines = append(lines, fmt.Sprintf("LOCATION:%s", EscapeICalValue(location)))
	}

	if description != "" {
		lines = append(lines, fmt.Sprintf("DESCRIPTION:%s", EscapeICalValue(description)))
	}

	if opts != nil {
		if urlVal := sanitizeICalURI(opts.URL); urlVal != "" {
			lines = append(lines, fmt.Sprintf("URL:%s", urlVal))
		}
		if opts.Status != "" {
			lines = append(lines, fmt.Sprintf("STATUS:%s", strings.ToUpper(opts.Status)))
		}
		if opts.Class != "" {
			lines = append(lines, fmt.Sprintf("CLASS:%s", strings.ToUpper(opts.Class)))
		}
		if opts.Transparency != "" {
			lines = append(lines, fmt.Sprintf("TRANSP:%s", strings.ToUpper(opts.Transparency)))
		}
		if len(opts.Categories) > 0 {
			var cats []string
			for _, c := range opts.Categories {
				if c = strings.TrimSpace(c); c != "" {
					cats = append(cats, EscapeICalValue(c))
				}
			}
			if len(cats) > 0 {
				lines = append(lines, fmt.Sprintf("CATEGORIES:%s", strings.Join(cats, ",")))
			}
		}
		if line := buildMailtoLine("ORGANIZER", opts.Organizer); line != "" {
			lines = append(lines, line)
		}
		for _, a := range opts.Attendees {
			if line := buildMailtoLine("ATTENDEE", a); line != "" {
				lines = append(lines, line)
			}
		}
		for _, att := range opts.Attachments {
			if attVal := sanitizeICalURI(att); attVal != "" {
				lines = append(lines, fmt.Sprintf("ATTACH:%s", attVal))
			}
		}
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
		if len(recurrence.ByDay) > 0 {
			rrule += fmt.Sprintf(";BYDAY=%s", strings.Join(recurrence.ByDay, ","))
		}
		if recurrence.ByMonthDay > 0 {
			rrule += fmt.Sprintf(";BYMONTHDAY=%d", recurrence.ByMonthDay)
		}
		if recurrence.ByMonth > 0 {
			rrule += fmt.Sprintf(";BYMONTH=%d", recurrence.ByMonth)
		}
		lines = append(lines, rrule)
	}

	if opts != nil && len(opts.Reminders) > 0 {
		for _, minutes := range opts.Reminders {
			if minutes < 0 {
				continue
			}
			lines = append(lines, "BEGIN:VALARM")
			lines = append(lines, "ACTION:DISPLAY")
			lines = append(lines, "DESCRIPTION:Reminder")
			lines = append(lines, fmt.Sprintf("TRIGGER:-PT%dM", minutes))
			lines = append(lines, "END:VALARM")
		}
	}

	return lines
}

// BuildEvent constructs a valid iCalendar event.
func BuildEvent(uid, summary, dtstart, dtend string, allDay bool, location, description string, recurrence *RecurrenceOptions, opts *EventOptions) string {
	eventLines := BuildEventComponent(uid, summary, dtstart, dtend, allDay, location, description, recurrence, "", opts)

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

func buildMailtoLine(prop, value string) string {
	name, email := parseNameEmail(value)
	if email == "" {
		return ""
	}
	emailLower := strings.ToLower(email)
	if strings.HasPrefix(emailLower, "mailto:") {
		email = strings.TrimSpace(email[len("mailto:"):])
	}
	if hasInvalidICalURI(email) {
		return ""
	}
	addr, err := mail.ParseAddress(email)
	if err != nil || addr.Address == "" {
		return ""
	}
	line := prop
	if safeName := sanitizeICalText(name); safeName != "" {
		line += ";CN=" + EscapeICalValue(safeName)
	}
	line += ":mailto:" + addr.Address
	return line
}

func parseNameEmail(value string) (name, email string) {
	value = strings.TrimSpace(value)
	if value == "" {
		return "", ""
	}
	if lt := strings.Index(value, "<"); lt != -1 {
		if gt := strings.Index(value, ">"); gt != -1 && gt > lt {
			name = strings.TrimSpace(value[:lt])
			email = strings.TrimSpace(value[lt+1 : gt])
			return name, email
		}
	}
	return "", value
}

func parseDateTimeLocal(value string, loc *time.Location) (time.Time, error) {
	layouts := []string{"2006-01-02T15:04", "2006-01-02T15:04:05"}
	var lastErr error
	for _, layout := range layouts {
		t, err := time.ParseInLocation(layout, value, loc)
		if err == nil {
			return t, nil
		}
		lastErr = err
	}
	return time.Time{}, lastErr
}

func sanitizeICalText(value string) string {
	value = strings.TrimSpace(value)
	value = strings.ReplaceAll(value, "\r\n", "\n")
	value = strings.ReplaceAll(value, "\r", "\n")
	if value == "" {
		return ""
	}
	if hasInvalidICalText(value) {
		return ""
	}
	return value
}

func sanitizeICalURI(value string) string {
	value = strings.TrimSpace(value)
	if value == "" {
		return ""
	}
	if hasInvalidICalURI(value) {
		return ""
	}
	if _, err := url.Parse(value); err != nil {
		return ""
	}
	return value
}

func hasInvalidICalURI(value string) bool {
	if strings.ContainsAny(value, "\r\n") {
		return true
	}
	return hasInvalidICalText(value)
}

func hasInvalidICalText(value string) bool {
	for _, r := range value {
		if r == '\n' || r == '\t' {
			continue
		}
		if r < 0x20 || r == 0x7f {
			return true
		}
	}
	return false
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
	s = strings.ReplaceAll(s, "\r\n", "\n")
	s = strings.ReplaceAll(s, "\r", "\n")
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
