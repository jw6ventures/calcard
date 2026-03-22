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

// GenerateVTimezone produces a VTIMEZONE component for the given IANA timezone ID.
// When years are provided, observances are generated for that inclusive range.
// Returns empty string for UTC or invalid timezone IDs.
func GenerateVTimezone(tzid string, years ...int) string {
	tzid = strings.TrimSpace(tzid)
	if tzid == "" || strings.EqualFold(tzid, "UTC") {
		return ""
	}
	loc, err := time.LoadLocation(tzid)
	if err != nil {
		return ""
	}

	type zoneInfo struct {
		name   string
		offset int // seconds east of UTC
		start  time.Time
		prev   int
	}

	startYear := time.Now().Year()
	endYear := startYear
	if len(years) > 0 {
		startYear = years[0]
		endYear = years[0]
		for _, year := range years[1:] {
			if year < startYear {
				startYear = year
			}
			if year > endYear {
				endYear = year
			}
		}
	}

	zoneType := func(offset, standardOffset int) string {
		if offset > standardOffset {
			return "DAYLIGHT"
		}
		return "STANDARD"
	}

	formatOffset := func(seconds int) string {
		sign := "+"
		if seconds < 0 {
			sign = "-"
			seconds = -seconds
		}
		h := seconds / 3600
		m := (seconds % 3600) / 60
		return fmt.Sprintf("%s%02d%02d", sign, h, m)
	}

	formatTransition := func(utc time.Time, prevOffset int) string {
		return utc.Add(time.Duration(prevOffset) * time.Second).UTC().Format("20060102T150405")
	}

	findTransitionUTC := func(lo, hi time.Time, prevName string, prevOffset int) time.Time {
		for hi.Sub(lo) > time.Second {
			mid := lo.Add(hi.Sub(lo) / 2)
			name, offset := mid.In(loc).Zone()
			if name == prevName && offset == prevOffset {
				lo = mid
			} else {
				hi = mid
			}
		}
		return hi.Truncate(time.Second)
	}

	var zones []zoneInfo
	for year := startYear; year <= endYear; year++ {
		yearStart := time.Date(year, 1, 1, 0, 0, 0, 0, loc)
		yearEnd := time.Date(year+1, 1, 1, 0, 0, 0, 0, loc)
		initialName, initialOffset := yearStart.Zone()
		zones = append(zones, zoneInfo{
			name:   initialName,
			offset: initialOffset,
			start:  yearStart,
			prev:   initialOffset,
		})

		scanStartUTC := yearStart.Add(-2 * time.Hour).UTC()
		scanEndUTC := yearEnd.Add(2 * time.Hour).UTC()
		prevUTC := scanStartUTC
		prevName, prevOffset := prevUTC.In(loc).Zone()

		for t := prevUTC.Add(time.Hour); !t.After(scanEndUTC); t = t.Add(time.Hour) {
			name, offset := t.In(loc).Zone()
			if name == prevName && offset == prevOffset {
				prevUTC = t
				continue
			}

			transitionUTC := findTransitionUTC(prevUTC, t, prevName, prevOffset)
			transitionLocal := transitionUTC.In(loc)
			if transitionLocal.Year() == year {
				zones = append(zones, zoneInfo{
					name:   name,
					offset: offset,
					start:  transitionUTC,
					prev:   prevOffset,
				})
			}

			prevUTC = t
			prevName = name
			prevOffset = offset
		}
	}

	if len(zones) == 0 {
		return ""
	}

	standardOffset := zones[0].offset
	for _, z := range zones[1:] {
		if z.offset < standardOffset {
			standardOffset = z.offset
		}
	}

	var sb strings.Builder
	sb.WriteString("BEGIN:VTIMEZONE\r\n")
	sb.WriteString(fmt.Sprintf("TZID:%s\r\n", tzid))

	if len(zones) == 1 {
		z := zones[0]
		sb.WriteString("BEGIN:STANDARD\r\n")
		sb.WriteString(fmt.Sprintf("DTSTART:%s\r\n", z.start.Format("20060102T150405")))
		sb.WriteString(fmt.Sprintf("TZOFFSETFROM:%s\r\n", formatOffset(z.offset)))
		sb.WriteString(fmt.Sprintf("TZOFFSETTO:%s\r\n", formatOffset(z.offset)))
		sb.WriteString(fmt.Sprintf("TZNAME:%s\r\n", z.name))
		sb.WriteString("END:STANDARD\r\n")
	} else {
		for _, z := range zones {
			compType := zoneType(z.offset, standardOffset)
			sb.WriteString(fmt.Sprintf("BEGIN:%s\r\n", compType))
			start := z.start.Format("20060102T150405")
			if z.prev != z.offset {
				start = formatTransition(z.start, z.prev)
			}
			sb.WriteString(fmt.Sprintf("DTSTART:%s\r\n", start))
			sb.WriteString(fmt.Sprintf("TZOFFSETFROM:%s\r\n", formatOffset(z.prev)))
			sb.WriteString(fmt.Sprintf("TZOFFSETTO:%s\r\n", formatOffset(z.offset)))
			sb.WriteString(fmt.Sprintf("TZNAME:%s\r\n", z.name))
			sb.WriteString(fmt.Sprintf("END:%s\r\n", compType))
		}
	}

	sb.WriteString("END:VTIMEZONE\r\n")
	return sb.String()
}

// BuildEvent constructs a valid iCalendar event.
func BuildEvent(uid, summary, dtstart, dtend string, allDay bool, location, description string, recurrence *RecurrenceOptions, opts *EventOptions) string {
	eventLines := BuildEventComponent(uid, summary, dtstart, dtend, allDay, location, description, recurrence, "", opts)

	var sb strings.Builder
	sb.WriteString("BEGIN:VCALENDAR\r\n")
	sb.WriteString("VERSION:2.0\r\n")
	sb.WriteString("PRODID:-//CalCard//EN\r\n")

	if opts != nil {
		if vtz := GenerateVTimezone(opts.Timezone, vTimezoneYears(dtstart, allDay, opts.Timezone, recurrence)...); vtz != "" {
			sb.WriteString(vtz)
		}
	}

	sb.WriteString("BEGIN:VEVENT\r\n")
	for _, line := range eventLines {
		sb.WriteString(line + "\r\n")
	}
	sb.WriteString("END:VEVENT\r\n")
	sb.WriteString("END:VCALENDAR\r\n")

	return sb.String()
}

func vTimezoneYears(dtstart string, allDay bool, tzid string, recurrence *RecurrenceOptions) []int {
	startYear, ok := parseEventYear(dtstart, allDay, tzid)
	if !ok {
		return nil
	}

	endYear := startYear
	if recurrence != nil && recurrence.Until != "" {
		if until, err := time.Parse("2006-01-02", recurrence.Until); err == nil {
			endYear = until.Year()
		}
	}

	years := make([]int, 0, endYear-startYear+1)
	for year := startYear; year <= endYear; year++ {
		years = append(years, year)
	}
	return years
}

func parseEventYear(value string, allDay bool, tzid string) (int, bool) {
	if value == "" {
		return 0, false
	}
	if allDay {
		t, err := time.Parse("2006-01-02", value)
		if err != nil {
			return 0, false
		}
		return t.Year(), true
	}
	if tzid != "" {
		if loc, err := time.LoadLocation(tzid); err == nil {
			if t, err := parseDateTimeLocal(value, loc); err == nil {
				return t.Year(), true
			}
		}
	}
	if t, err := parseDateTimeLocal(value, time.Local); err == nil {
		return t.Year(), true
	}
	return 0, false
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

func writeICalLine(sb *strings.Builder, line string) {
	if line == "" {
		return
	}
	if strings.HasSuffix(line, "\r\n") {
		sb.WriteString(line)
		return
	}
	sb.WriteString(line + "\r\n")
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

// ParseICSFile parses an ICS file and returns per-resource VCALENDAR payloads grouped by UID.
func ParseICSFile(icsContent string) ([]string, error) {
	if err := validateICSContent(icsContent); err != nil {
		return nil, err
	}

	lines := UnfoldLines(icsContent)
	var (
		headerLines          []string
		topLevelExtras       [][]string
		currentExtra         []string
		currentExtraDepth    int
		currentEvent         []string
		currentEventDepth    int
		inCalendar           bool
		groupedEvents        []string
		groupedEventUIDOrder []string
		eventGroups          = make(map[string][][]string)
	)

	for _, line := range lines {
		trimmed := strings.TrimSpace(line)
		upper := strings.ToUpper(trimmed)
		if trimmed == "" {
			continue
		}

		switch upper {
		case "BEGIN:VCALENDAR":
			inCalendar = true
			continue
		case "END:VCALENDAR":
			inCalendar = false
			continue
		}

		if !inCalendar {
			continue
		}

		if currentExtraDepth > 0 {
			currentExtra = append(currentExtra, trimmed)
			if strings.HasPrefix(upper, "BEGIN:") {
				currentExtraDepth++
			} else if strings.HasPrefix(upper, "END:") {
				currentExtraDepth--
				if currentExtraDepth == 0 {
					topLevelExtras = append(topLevelExtras, currentExtra)
					currentExtra = nil
				}
			}
			continue
		}

		if currentEventDepth > 0 {
			if upper == "END:VEVENT" {
				currentEventDepth--
				if currentEventDepth == 0 {
					uid := extractUIDFromLines(currentEvent)
					if uid == "" {
						uid = fmt.Sprintf("__missing_uid_%d", len(groupedEventUIDOrder))
					}
					if _, ok := eventGroups[uid]; !ok {
						groupedEventUIDOrder = append(groupedEventUIDOrder, uid)
					}
					eventGroups[uid] = append(eventGroups[uid], append([]string(nil), currentEvent...))
					currentEvent = nil
					continue
				}
				currentEvent = append(currentEvent, trimmed)
				continue
			}
			if strings.HasPrefix(upper, "BEGIN:") {
				currentEventDepth++
			}
			currentEvent = append(currentEvent, trimmed)
			continue
		}

		if strings.HasPrefix(upper, "BEGIN:") {
			name := strings.TrimSpace(trimmed[len("BEGIN:"):])
			if strings.EqualFold(name, "VEVENT") {
				currentEventDepth = 1
				currentEvent = nil
			} else {
				currentExtraDepth = 1
				currentExtra = []string{trimmed}
			}
			continue
		}

		headerLines = append(headerLines, trimmed)
	}

	if currentEventDepth != 0 || currentExtraDepth != 0 || inCalendar {
		return nil, fmt.Errorf("malformed ICS content")
	}

	if len(groupedEventUIDOrder) == 0 {
		return nil, fmt.Errorf("no VEVENT components found")
	}

	for _, uid := range groupedEventUIDOrder {
		events := eventGroups[uid]
		var eventICAL strings.Builder
		writeICalLine(&eventICAL, "BEGIN:VCALENDAR")
		for _, line := range headerLines {
			writeICalLine(&eventICAL, line)
		}
		for _, comp := range topLevelExtras {
			for _, line := range comp {
				writeICalLine(&eventICAL, line)
			}
		}
		for _, eventLines := range events {
			writeICalLine(&eventICAL, "BEGIN:VEVENT")
			for _, line := range eventLines {
				writeICalLine(&eventICAL, line)
			}
			writeICalLine(&eventICAL, "END:VEVENT")
		}
		writeICalLine(&eventICAL, "END:VCALENDAR")
		groupedEvents = append(groupedEvents, eventICAL.String())
	}

	return groupedEvents, nil
}

// ExtractUID extracts the UID from an iCalendar VEVENT.
func ExtractUID(ical string) string {
	lines := UnfoldLines(ical)
	return extractUIDFromLines(lines)
}

func extractUIDFromLines(lines []string) string {
	for _, line := range lines {
		if !strings.HasPrefix(strings.ToUpper(line), "UID:") {
			continue
		}
		return strings.TrimSpace(line[4:])
	}
	return ""
}

// EnsureUID injects a UID into the first VEVENT if missing.
func EnsureUID(ical, uid string) string {
	if uid == "" || ExtractUID(ical) != "" {
		return ical
	}

	lines := UnfoldLines(ical)
	var out []string
	inserted := false
	for _, line := range lines {
		trimmed := strings.TrimSpace(line)
		out = append(out, trimmed)
		if !inserted && strings.EqualFold(trimmed, "BEGIN:VEVENT") {
			out = append(out, "UID:"+uid)
			inserted = true
		}
	}
	return strings.Join(out, "\r\n") + "\r\n"
}

// ResourceNameForUID derives a stable .ics resource name from a UID.
func ResourceNameForUID(uid string) string {
	uid = strings.TrimSpace(uid)
	if uid == "" {
		return "event.ics"
	}
	var b strings.Builder
	for _, r := range uid {
		switch {
		case (r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') || (r >= '0' && r <= '9'):
			b.WriteRune(r)
		case r == '.' || r == '-' || r == '_':
			b.WriteRune(r)
		default:
			b.WriteByte('_')
		}
	}
	name := strings.Trim(b.String(), "._")
	if name == "" {
		name = "event"
	}
	if !strings.HasSuffix(strings.ToLower(name), ".ics") {
		name += ".ics"
	}
	return name
}

func validateICSContent(icsContent string) error {
	trimmed := strings.TrimSpace(icsContent)
	upper := strings.ToUpper(trimmed)
	if !strings.HasPrefix(upper, "BEGIN:VCALENDAR") {
		return fmt.Errorf("missing BEGIN:VCALENDAR")
	}
	if !strings.HasSuffix(upper, "END:VCALENDAR") {
		return fmt.Errorf("missing END:VCALENDAR")
	}

	lines := UnfoldLines(trimmed)
	var stack []string
	hasEvent := false
	for _, line := range lines {
		line = strings.TrimSpace(line)
		upperLine := strings.ToUpper(line)
		switch {
		case strings.HasPrefix(upperLine, "BEGIN:"):
			name := strings.TrimSpace(upperLine[len("BEGIN:"):])
			stack = append(stack, name)
			if name == "VEVENT" {
				hasEvent = true
			}
		case strings.HasPrefix(upperLine, "END:"):
			name := strings.TrimSpace(upperLine[len("END:"):])
			if len(stack) == 0 || stack[len(stack)-1] != name {
				return fmt.Errorf("unbalanced calendar tags")
			}
			stack = stack[:len(stack)-1]
		}
	}
	if len(stack) != 0 {
		return fmt.Errorf("unbalanced calendar tags")
	}
	if !hasEvent {
		return fmt.Errorf("no VEVENT components found")
	}
	return nil
}
