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

	"github.com/jw6ventures/calcard/internal/ical"
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
	// Timezone is the zone timed values are wall clocks in, per EventZoneIn.
	Timezone string
	// Object holds the unfolded content lines of the stored calendar object
	// an edit rewrites, whose VTIMEZONEs may define Timezone.
	Object       []string
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

// FormatICalDateTime converts form inputs into iCalendar date/time strings,
// reading a timed value in the host's zone tzid names, as FormatICalDateTimeIn
// does for EventZoneIn(tzid, nil).
func FormatICalDateTime(value string, allDay bool, exclusiveEnd bool, prop string, tzid string) (string, error) {
	return FormatICalDateTimeIn(value, allDay, exclusiveEnd, prop, EventZoneIn(tzid, nil))
}

// FormatICalDateTimeIn converts a form input into the iCalendar content line
// for prop. An all-day value is a DATE, the day after it when exclusiveEnd is
// set. A timed value is a wall clock in zone: a named zone gets it as written,
// with its TZID, so a wall clock the zone skips keeps the reading RFC 5545
// §3.3.5 gives it rather than one normalized away from it; otherwise it is
// written in UTC.
func FormatICalDateTimeIn(value string, allDay bool, exclusiveEnd bool, prop string, zone EventZone) (string, error) {
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

	wall, err := parseDateTimeLocal(value, time.UTC)
	if err != nil {
		return "", err
	}
	if zone.tzid != "" {
		return fmt.Sprintf("%s;TZID=%s:%s", prop, quoteParamValue(zone.tzid), wall.Format("20060102T150405")), nil
	}
	return fmt.Sprintf("%s:%s", prop, wall.Format("20060102T150405Z")), nil
}

// EventZone is the zone the timed values of an event's form are wall clocks
// in. The zero EventZone is UTC.
type EventZone struct {
	// tzid is the TZID values are written with, or "" for UTC.
	tzid string
	zone ical.ObjectTimezone
}

// EventZoneIn is the zone tzid names for an event: the zone object, the
// unfolded content lines of the stored calendar object an edit rewrites,
// defines by that name, read as the DAV layer reads it; else the host's zone
// of that name; else UTC. The server and the editor both read a value with no
// TZID, or with one neither the object nor the host defines, in UTC, so saving
// what the editor shows does not move the event whatever zone the server
// process runs in.
func EventZoneIn(tzid string, object []string) EventZone {
	tzid = strings.TrimSpace(tzid)
	if tzid == "" || tzid == "UTC" || !validParamValue(tzid) {
		return EventZone{}
	}
	zone := ical.ResolveObjectTimezone(object, tzid)
	if !zone.Defined() {
		return EventZone{}
	}
	return EventZone{tzid: tzid, zone: zone}
}

// Instant is the instant a wall clock in the zone names, held as the UTC time
// with the same fields, read per RFC 5545 §3.3.5 as the DAV layer reads it.
func (z EventZone) Instant(wall time.Time) time.Time {
	if z.tzid == "" {
		return wall
	}
	return z.zone.Instant(wall).UTC()
}

// WallClock is the wall clock the zone shows at instant, held as the UTC time
// with the same fields: the reading Instant takes back to instant, for every
// wall clock the zone neither skips nor repeats.
func (z EventZone) WallClock(instant time.Time) time.Time {
	if z.tzid == "" {
		return instant.UTC()
	}
	if offsets, ok := z.zone.Offsets(instant, instant); ok && len(offsets) > 0 {
		return instant.Add(offsets[0].Offset).UTC()
	}
	if loc, ok := ical.HostLocation(z.tzid); ok {
		local := instant.In(loc)
		return time.Date(local.Year(), local.Month(), local.Day(), local.Hour(), local.Minute(), local.Second(), local.Nanosecond(), time.UTC)
	}
	return instant.UTC()
}

// FormInstant is the instant a form value names in the zone: a date's
// midnight, or a timed value's wall clock read as Instant reads it.
func (z EventZone) FormInstant(value string) (time.Time, error) {
	if day, err := time.Parse("2006-01-02", value); err == nil {
		return day, nil
	}
	wall, err := parseDateTimeLocal(value, time.UTC)
	if err != nil {
		return time.Time{}, err
	}
	return z.Instant(wall), nil
}

// ValidateEventDates checks an event form's start and end: dates for an
// all-day event and local date-times for a timed one, the forms the builder
// writes, with the end not before the start as zone reads them.
func ValidateEventDates(dtstart, dtend string, allDay bool, zone EventZone) error {
	shaped := func(value string) bool {
		if allDay {
			_, err := time.Parse("2006-01-02", value)
			return err == nil
		}
		_, err := parseDateTimeLocal(value, time.UTC)
		return err == nil
	}
	if !shaped(dtstart) {
		return fmt.Errorf("invalid start date format")
	}
	if !shaped(dtend) {
		return fmt.Errorf("invalid end date format")
	}
	startTime, err := zone.FormInstant(dtstart)
	if err != nil {
		return fmt.Errorf("invalid start date format")
	}
	endTime, err := zone.FormInstant(dtend)
	if err != nil {
		return fmt.Errorf("invalid end date format")
	}
	if endTime.Before(startTime) {
		return fmt.Errorf("end date must not be before start date")
	}
	return nil
}

// validParamValue reports whether value can be written as an iCalendar
// parameter value, quoted where RFC 5545 §3.2 requires: it holds no DQUOTE
// and no control character.
func validParamValue(value string) bool {
	for _, r := range value {
		if r == '"' || r < 0x20 || r == 0x7f {
			return false
		}
	}
	return true
}

// quoteParamValue writes value as an RFC 5545 §3.2 parameter value, quoted
// when it holds a colon, semicolon or comma: Exchange names zones such as
// "(UTC-05:00) Eastern Time (US & Canada)".
func quoteParamValue(value string) string {
	if strings.ContainsAny(value, ":;,") {
		return `"` + value + `"`
	}
	return value
}

// recurrenceUntil is the RRULE UNTIL value that makes the picked date
// (YYYY-MM-DD) the last day of a series. RFC 5545 §3.3.10 gives UNTIL the
// value type of DTSTART: an all-day series takes the DATE, which is inclusive,
// and a timed one, whose DTSTART is zoned or UTC, takes a UTC DATE-TIME, here
// the last second of that date in the zone the event's times are read in.
func recurrenceUntil(date string, allDay bool, zone EventZone) (string, bool) {
	day, err := time.Parse("2006-01-02", date)
	if err != nil {
		return "", false
	}
	if allDay {
		return day.Format("20060102"), true
	}
	lastSecond := day.Add(24*time.Hour - time.Second)
	return zone.Instant(lastSecond).Format("20060102T150405Z"), true
}

// BuildEventComponent builds a single VEVENT component (without VCALENDAR
// wrapper). It fails when a start, end or RECURRENCE-ID is not of the shape
// allDay calls for.
func BuildEventComponent(uid, summary, dtstart, dtend string, allDay bool, location, description string, recurrence *RecurrenceOptions, recurrenceID string, opts *EventOptions) ([]string, error) {
	var lines []string
	lines = append(lines, fmt.Sprintf("UID:%s", uid))
	lines = append(lines, fmt.Sprintf("DTSTAMP:%s", time.Now().UTC().Format("20060102T150405Z")))

	var zone EventZone
	if opts != nil {
		zone = EventZoneIn(opts.Timezone, opts.Object)
	}

	for _, value := range []struct {
		value, name string
		end         bool
	}{{recurrenceID, "RECURRENCE-ID", false}, {dtstart, "DTSTART", false}, {dtend, "DTEND", true}} {
		line, err := FormatICalDateTimeIn(value.value, allDay, value.end, value.name, zone)
		if err != nil {
			return nil, fmt.Errorf("%s: %w", value.name, err)
		}
		if line != "" {
			lines = append(lines, line)
		}
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
		for _, token := range []struct {
			name, value string
			allowed     func(string) bool
		}{
			// A stored event may carry a value RFC 5545 does not list,
			// such as an x-name, which the editor submits back unchanged;
			// any token is written, and nothing else is.
			{"STATUS", opts.Status, ianaToken},
			{"CLASS", opts.Class, ianaToken},
			{"TRANSP", opts.Transparency, ianaToken},
		} {
			if token.value == "" {
				continue
			}
			value := strings.ToUpper(token.value)
			if !token.allowed(value) {
				return nil, fmt.Errorf("invalid %s", strings.ToLower(token.name))
			}
			lines = append(lines, token.name+":"+value)
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
		if line := AttendeeLine("ORGANIZER", opts.Organizer); line != "" {
			lines = append(lines, line)
		}
		for _, a := range opts.Attendees {
			if line := AttendeeLine("ATTENDEE", a); line != "" {
				lines = append(lines, line)
			}
		}
		for _, att := range opts.Attachments {
			if line := AttachmentLine(att); line != "" {
				lines = append(lines, line)
			}
		}
	}

	if recurrence != nil && recurrence.Frequency != "" && recurrenceID == "" {
		frequency := strings.ToUpper(recurrence.Frequency)
		if !recurrenceFrequency(frequency) {
			return nil, fmt.Errorf("invalid recurrence frequency")
		}
		rrule := "RRULE:FREQ=" + frequency
		if recurrence.Interval > 1 {
			rrule += fmt.Sprintf(";INTERVAL=%d", recurrence.Interval)
		}
		if recurrence.Count > 0 {
			rrule += fmt.Sprintf(";COUNT=%d", recurrence.Count)
		} else if until, ok := recurrenceUntil(recurrence.Until, allDay, zone); ok {
			rrule += ";UNTIL=" + until
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

	return lines, nil
}

// BuildEvent constructs an iCalendar object holding one event. It defines no
// VTIMEZONE: every caller stores it through the calendar object validator,
// which adds the host's definition of each zone the object names, spanning
// every time the object places in it.
func BuildEvent(uid, summary, dtstart, dtend string, allDay bool, location, description string, recurrence *RecurrenceOptions, opts *EventOptions) (string, error) {
	eventLines, err := BuildEventComponent(uid, summary, dtstart, dtend, allDay, location, description, recurrence, "", opts)
	if err != nil {
		return "", err
	}

	var sb strings.Builder
	sb.WriteString("BEGIN:VCALENDAR\r\n")
	sb.WriteString("VERSION:2.0\r\n")
	sb.WriteString("PRODID:-//CalCard//EN\r\n")
	sb.WriteString("BEGIN:VEVENT\r\n")
	for _, line := range eventLines {
		sb.WriteString(line)
		sb.WriteString("\r\n")
	}
	sb.WriteString("END:VEVENT\r\n")
	sb.WriteString("END:VCALENDAR\r\n")

	return sb.String(), nil
}

// recurrenceFrequency reports whether frequency, upper-cased, is an RRULE
// FREQ (RFC 5545 §3.3.10).
func recurrenceFrequency(frequency string) bool {
	switch frequency {
	case "SECONDLY", "MINUTELY", "HOURLY", "DAILY", "WEEKLY", "MONTHLY", "YEARLY":
		return true
	}
	return false
}

// ianaToken reports whether value is an RFC 5545 §3.1 iana-token, which an
// x-name also is: one or more letters, digits and dashes.
func ianaToken(value string) bool {
	if value == "" {
		return false
	}
	for _, r := range value {
		if (r < 'A' || r > 'Z') && (r < 'a' || r > 'z') && (r < '0' || r > '9') && r != '-' {
			return false
		}
	}
	return true
}

// AttendeeLine is the content line of prop, ORGANIZER or ATTENDEE, for value,
// an address as the event form takes one -- "Name <address>" or a bare
// address -- or "" when value holds no mail address.
func AttendeeLine(prop, value string) string {
	name, email := NameAndAddress(value)
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
	if name := DisplayNameParam(name); name != "" {
		line += ";CN=" + name
	}
	line += ":mailto:" + addr.Address
	return line
}

// DisplayNameParam writes name as the value of a CN parameter, quoted when RFC
// 5545 §3.2 requires. A parameter value cannot hold a DQUOTE or a control
// character, so a name the form quoted is unquoted, any DQUOTE left is
// dropped, and each control character becomes a space. It is "" for a name
// with nothing else to write.
func DisplayNameParam(name string) string {
	name = strings.TrimSpace(name)
	if len(name) >= 2 && strings.HasPrefix(name, `"`) && strings.HasSuffix(name, `"`) {
		name = name[1 : len(name)-1]
	}
	name = strings.Map(func(r rune) rune {
		switch {
		case r == '"':
			return -1
		case r < 0x20 || r == 0x7f:
			return ' '
		}
		return r
	}, name)
	name = strings.TrimSpace(name)
	if name == "" {
		return ""
	}
	return quoteParamValue(name)
}

// AttachmentLine is the ATTACH content line for a URI the event form takes, or
// "" when value is not one.
func AttachmentLine(value string) string {
	if uri := sanitizeICalURI(value); uri != "" {
		return "ATTACH:" + uri
	}
	return ""
}

// NameAndAddress splits an address as the event form takes one, "Name
// <address>" or a bare address, into its display name and address.
func NameAndAddress(value string) (name, address string) {
	value = strings.TrimSpace(value)
	if value == "" {
		return "", ""
	}
	// The address is the last bracketed part: a quoted name may hold
	// brackets of its own.
	if lt := strings.LastIndex(value, "<"); lt != -1 {
		if gt := strings.LastIndex(value, ">"); gt > lt {
			name = strings.TrimSpace(value[:lt])
			address = strings.TrimSpace(value[lt+1 : gt])
			return name, address
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
	lines := make([]string, 0, len(rawLines))
	for i := 0; i < len(rawLines); {
		end := i + 1
		for end < len(rawLines) && (strings.HasPrefix(rawLines[end], " ") || strings.HasPrefix(rawLines[end], "\t")) {
			end++
		}
		if end == i+1 {
			lines = append(lines, rawLines[i])
		} else {
			// Joined once, so a line folded many times unfolds in linear time.
			var b strings.Builder
			b.WriteString(rawLines[i])
			for _, continuation := range rawLines[i+1 : end] {
				b.WriteString(strings.TrimLeft(continuation, " \t"))
			}
			lines = append(lines, b.String())
		}
		i = end
	}
	return lines
}

// SplitComponents separates VCALENDAR header/footer and VEVENT components.
func SplitComponents(ical string) (header []string, events [][]string, footer []string) {
	return SplitComponentLines(UnfoldLines(ical))
}

// SplitComponentLines is SplitComponents for the unfolded content lines of an
// object.
func SplitComponentLines(lines []string) (header []string, events [][]string, footer []string) {
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
		sb.WriteString(line)
		sb.WriteString("\r\n")
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
	sb.WriteString(line)
	sb.WriteString("\r\n")
}

// propertyLine splits an unfolded content line when it is the named property.
// A quoted parameter value may hold a colon, so the split honours quoting.
func propertyLine(line, name string) (keyPart, value string, ok bool) {
	keyPart, value, ok = ical.SplitContentLine(line)
	if !ok || !strings.EqualFold(ical.PropertyName(keyPart), name) {
		return "", "", false
	}
	return keyPart, value, true
}

// RecurrenceIDValue extracts the RECURRENCE-ID value from event lines.
func RecurrenceIDValue(lines []string) string {
	for _, line := range lines {
		if _, value, ok := propertyLine(line, "RECURRENCE-ID"); ok {
			return value
		}
	}
	return ""
}

// SameRecurrenceID compares occurrence identities, including equivalent timezone spellings.
func SameRecurrenceID(left, right []string) bool {
	read := func(lines []string) (string, string) {
		for _, line := range lines {
			if key, value, ok := propertyLine(line, "RECURRENCE-ID"); ok {
				return key, value
			}
		}
		return "", ""
	}
	lk, lv := read(left)
	rk, rv := read(right)
	if lv == "" || rv == "" {
		return false
	}
	floating := func(key, value string) bool { return len(value) == 15 && !strings.Contains(key, "TZID=") }
	if (len(lv) == 8) != (len(rv) == 8) || floating(lk, lv) != floating(rk, rv) {
		return false
	}
	lt, lok := ical.ParsePropertyDateTimeLocal(lk, lv)
	rt, rok := ical.ParsePropertyDateTimeLocal(rk, rv)
	if lok && rok {
		return lt.Equal(rt)
	}
	return lk == rk && lv == rv
}

// HasPropertyValue checks if a property has a specific value.
func HasPropertyValue(lines []string, prop, value string) bool {
	for _, line := range lines {
		if _, got, ok := propertyLine(line, prop); ok && got == value {
			return true
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

// ParseICSFile parses an ICS file and returns per-resource VCALENDAR payloads
// grouped by UID, each shaped as a calendar object resource RFC 4791 §4.1
// admits, and the number of the file's top-level components other than VEVENT
// and VTIMEZONE, which no payload carries.
//
// An export file is not a resource. Its VCALENDAR describes the calendar it
// was exported from -- METHOD, which §4.1 rules out of a stored resource, and
// the name, description, colour, source and the like that RFC 7986 and
// producers' X-WR-* extensions give it -- none of which describes any one
// event, so each payload carries only the properties saying how its iCalendar
// is written: PRODID, VERSION and CALSCALE. That leaves out X-WR-TIMEZONE too,
// which some clients read floating times in while the server reads them in
// the collection's CALDAV:calendar-timezone. Of the file's other top-level
// components each payload carries only the VTIMEZONEs its events name, since a
// resource holds one type of calendar component, and a zone definition one
// event cannot use should not decide whether another event's payload is valid.
// A file may also be several exports concatenated, each with its own calendar
// properties and zone definitions. A resource may carry each property only
// once, so the first VCALENDAR's are the ones kept; an event's zones are
// defined as its own VCALENDAR defines them, since exports can define one TZID
// differently.
func ParseICSFile(icsContent string) (events []string, leftOut int, err error) {
	icsContent = ical.TrimByteOrderMark(icsContent)
	if err := validateICSContent(icsContent); err != nil {
		return nil, 0, err
	}

	lines := UnfoldLines(icsContent)
	var (
		headerLines          []string
		headerNames          = make(map[string]struct{})
		calendarZones        []calendarTimezones
		currentExtra         []string
		currentExtraDepth    int
		currentEvent         []string
		currentEventDepth    int
		inCalendar           bool
		groupedEventUIDOrder []string
		eventGroups          = make(map[string][]importedEvent)
	)

	for _, line := range lines {
		trimmed := strings.TrimSpace(line)
		upper := strings.ToUpper(trimmed)
		if trimmed == "" {
			continue
		}

		switch upper {
		case "BEGIN:VCALENDAR":
			if !inCalendar {
				calendarZones = append(calendarZones, make(calendarTimezones))
			}
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
					if strings.EqualFold(currentExtra[0], "BEGIN:VTIMEZONE") {
						calendarZones[len(calendarZones)-1].define(currentExtra)
					} else {
						leftOut++
					}
					currentExtra = nil
				}
			}
			continue
		}

		if currentEventDepth > 0 {
			if strings.HasPrefix(upper, "BEGIN:") {
				currentEventDepth++
			} else if strings.HasPrefix(upper, "END:") {
				currentEventDepth--
			}
			if currentEventDepth > 0 {
				currentEvent = append(currentEvent, trimmed)
				continue
			}
			uid := extractUIDFromLines(currentEvent)
			if uid == "" {
				uid = fmt.Sprintf("__missing_uid_%d", len(groupedEventUIDOrder))
			}
			if _, ok := eventGroups[uid]; !ok {
				groupedEventUIDOrder = append(groupedEventUIDOrder, uid)
			}
			eventGroups[uid] = append(eventGroups[uid], importedEvent{
				lines:    append([]string(nil), currentEvent...),
				calendar: len(calendarZones) - 1,
			})
			currentEvent = nil
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

		name := strings.ToUpper(strings.TrimSpace(ical.PropertyName(trimmed)))
		if _, carried := resourceCalendarProperties[name]; !carried {
			continue
		}
		if _, seen := headerNames[name]; seen {
			continue
		}
		headerNames[name] = struct{}{}
		headerLines = append(headerLines, trimmed)
	}

	if currentEventDepth != 0 || currentExtraDepth != 0 || inCalendar {
		return nil, 0, fmt.Errorf("malformed ICS content")
	}

	if len(groupedEventUIDOrder) == 0 {
		return nil, 0, fmt.Errorf("no VEVENT components found")
	}

	for _, uid := range groupedEventUIDOrder {
		group := eventGroups[uid]
		var eventICAL strings.Builder
		writeICalLine(&eventICAL, "BEGIN:VCALENDAR")
		for _, line := range headerLines {
			writeICalLine(&eventICAL, line)
		}
		for _, definition := range groupTimezones(group, calendarZones) {
			for _, line := range definition {
				writeICalLine(&eventICAL, line)
			}
		}
		for _, event := range group {
			writeICalLine(&eventICAL, "BEGIN:VEVENT")
			for _, line := range event.lines {
				writeICalLine(&eventICAL, line)
			}
			writeICalLine(&eventICAL, "END:VEVENT")
		}
		writeICalLine(&eventICAL, "END:VCALENDAR")
		events = append(events, eventICAL.String())
	}

	return events, leftOut, nil
}

// resourceCalendarProperties are the VCALENDAR properties an imported event's
// payload carries from the file.
var resourceCalendarProperties = map[string]struct{}{"PRODID": {}, "VERSION": {}, "CALSCALE": {}}

// importedEvent is one VEVENT of an imported file, as the content lines
// between its BEGIN and END, and the index of the VCALENDAR holding it.
type importedEvent struct {
	lines    []string
	calendar int
}

// calendarTimezones maps each TZID one VCALENDAR defines to the VTIMEZONE
// defining it.
type calendarTimezones map[string][]string

// define records vtimezone unless it names no zone, which RFC 5545 §3.6.5 does
// not allow and which therefore defines none, or its zone already has a
// definition: the DAV layer resolves a TZID through the first VTIMEZONE
// declaring it.
func (zones calendarTimezones) define(vtimezone []string) {
	wrapped := make([]string, 0, len(vtimezone)+2)
	wrapped = append(wrapped, "BEGIN:VCALENDAR")
	wrapped = append(wrapped, vtimezone...)
	read := ical.TimezoneComponents(append(wrapped, "END:VCALENDAR"))
	if len(read) != 1 || read[0].TZID == "" {
		return
	}
	tzid := read[0].TZID
	if _, defined := zones[tzid]; !defined {
		zones[tzid] = vtimezone
	}
}

// groupTimezones is the VTIMEZONEs defining the zones a group of events name,
// in the order they are first named. A resource defines each zone once, so a
// zone named by events from several VCALENDARs is defined as the VCALENDAR of
// the first event naming it defines it.
func groupTimezones(group []importedEvent, calendarZones []calendarTimezones) [][]string {
	var definitions [][]string
	seen := make(map[string]struct{})
	for _, event := range group {
		for _, tzid := range timezoneReferences(event.lines) {
			if _, ok := seen[tzid]; ok {
				continue
			}
			seen[tzid] = struct{}{}
			if definition, ok := calendarZones[event.calendar][tzid]; ok {
				definitions = append(definitions, definition)
			}
		}
	}
	return definitions
}

// timezoneReferences lists the non-empty TZID parameter values of lines in the
// order the lines carry them.
func timezoneReferences(lines []string) []string {
	var tzids []string
	for _, line := range lines {
		keyPart, _, ok := ical.SplitContentLine(line)
		if !ok {
			continue
		}
		if tzid, ok := ical.PropertyParam(keyPart, "TZID"); ok {
			if tzid = strings.TrimSpace(tzid); tzid != "" {
				tzids = append(tzids, tzid)
			}
		}
	}
	return tzids
}

// ExtractUID returns the UID set directly on the first VEVENT, or "" if that
// VEVENT has none.
func ExtractUID(ical string) string {
	depth := 0
	eventDepth := 0
	for _, line := range UnfoldLines(ical) {
		trimmed := strings.TrimSpace(line)
		upper := strings.ToUpper(trimmed)
		switch {
		case strings.HasPrefix(upper, "BEGIN:"):
			depth++
			if eventDepth == 0 && upper == "BEGIN:VEVENT" {
				eventDepth = depth
			}
		case strings.HasPrefix(upper, "END:"):
			if eventDepth > 0 && depth == eventDepth {
				return ""
			}
			depth--
		case eventDepth > 0 && depth == eventDepth:
			if uid, ok := uidPropertyValue(trimmed); ok {
				return uid
			}
		}
	}
	return ""
}

// extractUIDFromLines returns the UID among a VEVENT's own properties, given
// the lines between its BEGIN and END markers. UIDs inside nested components
// are skipped: RFC 9074 §4 lets a VALARM carry its own UID, and Apple Calendar
// writes one on every alarm.
func extractUIDFromLines(lines []string) string {
	depth := 0
	for _, line := range lines {
		trimmed := strings.TrimSpace(line)
		upper := strings.ToUpper(trimmed)
		switch {
		case strings.HasPrefix(upper, "BEGIN:"):
			depth++
		case strings.HasPrefix(upper, "END:"):
			depth--
		case depth == 0:
			if uid, ok := uidPropertyValue(trimmed); ok {
				return uid
			}
		}
	}
	return ""
}

// uidPropertyValue returns the value of a UID content line, which may carry
// parameters ("UID;X-SOURCE=import:value") whose quoted values hold colons.
func uidPropertyValue(line string) (string, bool) {
	keyPart, value, ok := ical.SplitContentLine(line)
	if !ok || !strings.EqualFold(strings.TrimSpace(ical.PropertyName(keyPart)), "UID") {
		return "", false
	}
	return strings.TrimSpace(value), true
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
