package utils

import (
	"maps"
	"net/http"
	"net/url"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/jw6ventures/calcard/internal/ical"
)

func TestGenerateUID(t *testing.T) {
	uid1 := GenerateUID()
	uid2 := GenerateUID()

	if uid1 == "" {
		t.Error("GenerateUID returned empty string")
	}
	if uid2 == "" {
		t.Error("GenerateUID returned empty string")
	}
	if uid1 == uid2 {
		t.Error("GenerateUID should generate unique IDs")
	}
	if !strings.HasSuffix(uid1, "@calcard") {
		t.Error("GenerateUID should end with @calcard")
	}
}

func TestGenerateETag(t *testing.T) {
	content := "test content"
	etag1 := GenerateETag(content)
	etag2 := GenerateETag(content)
	etag3 := GenerateETag("different content")

	if etag1 != etag2 {
		t.Error("Same content should generate same ETag")
	}
	if etag1 == etag3 {
		t.Error("Different content should generate different ETag")
	}
	if len(etag1) != 64 {
		t.Errorf("ETag should be 64 characters (SHA256 hex), got %d", len(etag1))
	}
}

func TestParseRecurrenceOptions(t *testing.T) {
	tests := []struct {
		name     string
		formData url.Values
		want     *RecurrenceOptions
	}{
		{
			name:     "no recurrence",
			formData: url.Values{},
			want:     nil,
		},
		{
			name: "daily recurrence",
			formData: url.Values{
				"recurrence": []string{"DAILY"},
			},
			want: &RecurrenceOptions{
				Frequency: "DAILY",
				Interval:  1,
				Count:     0,
				Until:     "",
			},
		},
		{
			name: "weekly with interval",
			formData: url.Values{
				"recurrence":          []string{"WEEKLY"},
				"recurrence_interval": []string{"2"},
			},
			want: &RecurrenceOptions{
				Frequency: "WEEKLY",
				Interval:  2,
				Count:     0,
				Until:     "",
			},
		},
		{
			name: "monthly with count",
			formData: url.Values{
				"recurrence":          []string{"MONTHLY"},
				"recurrence_end_type": []string{"after"},
				"recurrence_count":    []string{"10"},
			},
			want: &RecurrenceOptions{
				Frequency: "MONTHLY",
				Interval:  1,
				Count:     10,
				Until:     "",
			},
		},
		{
			name: "yearly with until date",
			formData: url.Values{
				"recurrence":          []string{"YEARLY"},
				"recurrence_end_type": []string{"on"},
				"recurrence_until":    []string{"2025-12-31"},
			},
			want: &RecurrenceOptions{
				Frequency: "YEARLY",
				Interval:  1,
				Count:     0,
				Until:     "2025-12-31",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req := &http.Request{
				Form: tt.formData,
			}
			got, err := ParseRecurrenceOptions(req)
			if err != nil {
				t.Fatalf("ParseRecurrenceOptions() error = %v", err)
			}

			if tt.want == nil && got != nil {
				t.Errorf("Expected nil, got %+v", got)
				return
			}
			if tt.want != nil && got == nil {
				t.Error("Expected RecurrenceOptions, got nil")
				return
			}
			if tt.want == nil && got == nil {
				return
			}

			if got.Frequency != tt.want.Frequency {
				t.Errorf("Frequency: got %q, want %q", got.Frequency, tt.want.Frequency)
			}
			if got.Interval != tt.want.Interval {
				t.Errorf("Interval: got %d, want %d", got.Interval, tt.want.Interval)
			}
			if got.Count != tt.want.Count {
				t.Errorf("Count: got %d, want %d", got.Count, tt.want.Count)
			}
			if got.Until != tt.want.Until {
				t.Errorf("Until: got %q, want %q", got.Until, tt.want.Until)
			}
			if !slices.Equal(got.ByMonthDay, tt.want.ByMonthDay) {
				t.Errorf("ByMonthDay: got %v, want %v", got.ByMonthDay, tt.want.ByMonthDay)
			}
		})
	}
}

// The days of the month field holds a list, and RFC 5545 §3.3.10 counts a
// negative day back from the month's end. A list the rule cannot hold is
// refused rather than stored short of what was typed.
func TestParseRecurrenceOptionsReadsDaysOfTheMonth(t *testing.T) {
	parse := func(days string) ([]int, error) {
		got, err := ParseRecurrenceOptions(&http.Request{Form: url.Values{"recurrence": {"MONTHLY"}, "recurrence_bymonthday": {days}}})
		if err != nil {
			return nil, err
		}
		return got.ByMonthDay, nil
	}
	for days, want := range map[string][]int{"": nil, "15": {15}, " 1, 15 ,-1": {1, 15, -1}, "-31,31": {-31, 31}} {
		if got, err := parse(days); err != nil || !slices.Equal(got, want) {
			t.Errorf("days %q read as %v, %v; want %v", days, got, err, want)
		}
	}
	for _, days := range []string{"0", "32", "-32", "1,,2", "1,x", "1.5"} {
		if got, err := parse(days); err == nil {
			t.Errorf("days %q read as %v, want an error", days, got)
		}
	}
	component, err := BuildEventComponent("uid", "", "2026-01-01T09:00", "2026-01-01T10:00", false, "", "",
		&RecurrenceOptions{Frequency: "MONTHLY", Interval: 1, ByMonthDay: []int{1, 15, -1}}, "", nil)
	if err != nil || !slices.Contains(component, "RRULE:FREQ=MONTHLY;BYMONTHDAY=1,15,-1") {
		t.Errorf("BuildEventComponent() = %q, %v; want BYMONTHDAY=1,15,-1", component, err)
	}
}

func TestFormatICalDateTime(t *testing.T) {
	tests := []struct {
		name         string
		value        string
		allDay       bool
		exclusiveEnd bool
		prop         string
		wantContains string
		wantErr      bool
	}{
		{
			name:         "empty value",
			value:        "",
			allDay:       false,
			exclusiveEnd: false,
			prop:         "DTSTART",
			wantContains: "",
			wantErr:      false,
		},
		{
			name:         "all-day event",
			value:        "2025-01-15",
			allDay:       true,
			exclusiveEnd: false,
			prop:         "DTSTART",
			wantContains: "DTSTART;VALUE=DATE:20250115",
			wantErr:      false,
		},
		{
			name:         "all-day event with exclusive end",
			value:        "2025-01-15",
			allDay:       true,
			exclusiveEnd: true,
			prop:         "DTEND",
			wantContains: "DTEND;VALUE=DATE:20250116",
			wantErr:      false,
		},
		{
			name:         "datetime event",
			value:        "2025-01-15T14:30",
			allDay:       false,
			exclusiveEnd: false,
			prop:         "DTSTART",
			wantContains: "DTSTART:",
			wantErr:      false,
		},
		{
			name:         "invalid date format",
			value:        "invalid",
			allDay:       true,
			exclusiveEnd: false,
			prop:         "DTSTART",
			wantContains: "",
			wantErr:      true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := FormatICalDateTime(tt.value, tt.allDay, tt.exclusiveEnd, tt.prop, "")

			if (err != nil) != tt.wantErr {
				t.Errorf("FormatICalDateTime() error = %v, wantErr %v", err, tt.wantErr)
				return
			}

			if tt.wantContains != "" && !strings.Contains(got, tt.wantContains) {
				t.Errorf("FormatICalDateTime() = %q, want to contain %q", got, tt.wantContains)
			}
		})
	}
}

func TestBuildEvent(t *testing.T) {
	uid := "test-uid@calcard"
	summary := "Test Event"
	dtstart := "2025-01-15"
	dtend := "2025-01-16"
	location := "Test Location"
	description := "Test Description"

	ical := mustBuildEvent(t, uid, summary, dtstart, dtend, true, location, description, nil, nil)

	requiredFields := []string{
		"BEGIN:VCALENDAR",
		"VERSION:2.0",
		"PRODID:-//CalCard//EN",
		"BEGIN:VEVENT",
		"UID:" + uid,
		"SUMMARY:" + summary,
		"LOCATION:" + location,
		"DESCRIPTION:" + description,
		"END:VEVENT",
		"END:VCALENDAR",
	}

	for _, field := range requiredFields {
		if !strings.Contains(ical, field) {
			t.Errorf("BuildEvent() missing required field: %s", field)
		}
	}
}

func TestBuildEventWithRecurrence(t *testing.T) {
	uid := "test-uid@calcard"
	recurrence := &RecurrenceOptions{
		Frequency: "WEEKLY",
		Interval:  2,
		Count:     10,
	}

	ical := mustBuildEvent(t, uid, "Weekly Meeting", "2025-01-15T14:00", "2025-01-15T15:00", false, "", "", recurrence, nil)

	if !strings.Contains(ical, "RRULE:FREQ=WEEKLY") {
		t.Error("BuildEvent() should include RRULE frequency")
	}
	if !strings.Contains(ical, "INTERVAL=2") {
		t.Error("BuildEvent() should include RRULE interval")
	}
	if !strings.Contains(ical, "COUNT=10") {
		t.Error("BuildEvent() should include RRULE count")
	}
}

// The picked end date is the last day of the series. RFC 5545 §3.3.10 gives a
// DATE UNTIL on an all-day series that meaning, but a zoned or UTC DTSTART
// needs a UTC DATE-TIME UNTIL, so a timed series ends at the last second of
// the picked date in the event's zone. A series with no zone, or one the host
// does not know, has its times read in UTC, whatever zone the server runs in.
func TestBuildEventComponentEndsTheSeriesOnThePickedDate(t *testing.T) {
	setLocalZone(t, "America/New_York")
	tests := []struct {
		name      string
		dtstart   string
		allDay    bool
		tzid      string
		wantUntil string
	}{
		{name: "all-day series", dtstart: "2026-01-05", allDay: true, tzid: "Europe/Berlin", wantUntil: "UNTIL=20260302;"},
		{name: "timed series in a zone", dtstart: "2026-01-05T09:00", tzid: "Europe/Berlin", wantUntil: "UNTIL=20260302T225959Z;"},
		{name: "timed series west of UTC", dtstart: "2026-01-05T09:00", tzid: "America/Los_Angeles", wantUntil: "UNTIL=20260303T075959Z;"},
		{name: "timed series in UTC", dtstart: "2026-01-05T09:00", tzid: "UTC", wantUntil: "UNTIL=20260302T235959Z;"},
		{name: "timed series without a zone", dtstart: "2026-01-05T09:00", wantUntil: "UNTIL=20260302T235959Z;"},
		{name: "timed series in an unknown zone", dtstart: "2026-01-05T09:00", tzid: "Not/AZone", wantUntil: "UNTIL=20260302T235959Z;"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			recurrence := &RecurrenceOptions{Frequency: "WEEKLY", Until: "2026-03-02", ByDay: []string{"MO"}}
			lines := mustBuildEventComponent(t, "uid", "Series", tt.dtstart, tt.dtstart, tt.allDay, "", "", recurrence, "", &EventOptions{Timezone: tt.tzid})
			var rrule string
			for _, line := range lines {
				if strings.HasPrefix(line, "RRULE:") {
					rrule = line
				}
			}
			if !strings.Contains(rrule, tt.wantUntil) {
				t.Fatalf("RRULE = %q, want it to contain %q", rrule, tt.wantUntil)
			}
		})
	}
}

// setLocalZone runs the rest of the test with the process's local zone set to
// name, as if the server ran with TZ=name.
func setLocalZone(t *testing.T, name string) {
	t.Helper()
	loc, err := time.LoadLocation(name)
	if err != nil {
		t.Fatalf("LoadLocation(%q) error = %v", name, err)
	}
	saved := time.Local
	time.Local = loc
	t.Cleanup(func() { time.Local = saved })
}

// The editor reads the times of an event with no TZID, or one the host does
// not know, in UTC, as the server does. Writing them back from what it reads
// must reproduce them whatever zone the server runs in, or each save moves the
// event: on a server west of UTC, a series ending on the last second of its end
// date in the server's zone reads back as ending a day later, and the next save
// moves it a day further.
func TestBuildEventComponentRoundTripsTimesWithoutAHostZone(t *testing.T) {
	setLocalZone(t, "America/New_York")
	for _, tzid := range []string{"", "Not/AZone"} {
		t.Run("tzid="+tzid, func(t *testing.T) {
			form := struct{ start, end, until string }{"2025-01-15T14:00", "2025-01-15T15:00", "2025-01-31"}
			build := func(start, end, until string) map[string]string {
				recurrence := &RecurrenceOptions{Frequency: "DAILY", Until: until}
				lines := mustBuildEventComponent(t, "uid", "Series", start, end, false, "", "", recurrence, "", &EventOptions{Timezone: tzid})
				written := make(map[string]string)
				for _, line := range lines {
					if keyPart, _, ok := ical.SplitContentLine(line); ok {
						switch name := ical.PropertyName(keyPart); name {
						case "DTSTART", "DTEND", "RRULE":
							written[name] = line
						}
					}
				}
				return written
			}
			readTime := func(line string) string {
				keyPart, value, _ := ical.SplitContentLine(line)
				instant, ok := ical.ParsePropertyDateTimeLocal(keyPart, value)
				if !ok {
					t.Fatalf("cannot read %q", line)
				}
				return instant.UTC().Format("2006-01-02T15:04")
			}
			readUntilDay := func(rrule string) string {
				for _, part := range strings.Split(strings.TrimPrefix(rrule, "RRULE:"), ";") {
					if value, ok := strings.CutPrefix(part, "UNTIL="); ok {
						instant, err := time.Parse("20060102T150405Z", value)
						if err != nil {
							t.Fatalf("UNTIL %q is not a UTC DATE-TIME", value)
						}
						return instant.UTC().Format("2006-01-02")
					}
				}
				t.Fatalf("RRULE %q has no UNTIL", rrule)
				return ""
			}

			first := build(form.start, form.end, form.until)
			read := struct{ start, end, until string }{readTime(first["DTSTART"]), readTime(first["DTEND"]), readUntilDay(first["RRULE"])}
			if read != form {
				t.Fatalf("wrote %q, which reads back as %+v, want %+v", first, read, form)
			}
			if second := build(read.start, read.end, read.until); !maps.Equal(second, first) {
				t.Fatalf("saving what was read wrote %q, want %q", second, first)
			}
		})
	}
}

func TestFormatICalDateTime_TimezoneHandling(t *testing.T) {
	t.Run("valid timezone uses TZID", func(t *testing.T) {
		got, err := FormatICalDateTime("2025-01-15T14:30", false, false, "DTSTART", "America/New_York")
		if err != nil {
			t.Fatalf("FormatICalDateTime() error = %v", err)
		}
		if !strings.Contains(got, "DTSTART;TZID=America/New_York:20250115T143000") {
			t.Fatalf("expected TZID datetime, got %q", got)
		}
	})

	t.Run("invalid timezone falls back to UTC format", func(t *testing.T) {
		got, err := FormatICalDateTime("2025-01-15T14:30", false, false, "DTSTART", "Not/A_Real_Zone")
		if err != nil {
			t.Fatalf("FormatICalDateTime() error = %v", err)
		}
		if strings.Contains(got, "TZID=") {
			t.Fatalf("unexpected TZID in fallback output: %q", got)
		}
		if !strings.HasPrefix(got, "DTSTART:") || !strings.HasSuffix(got, "Z") {
			t.Fatalf("expected UTC DTSTART output, got %q", got)
		}
	})
}

// A timed value in a named zone is written as the wall clock entered, so one
// the zone skips keeps the reading RFC 5545 §3.3.5 gives it, and everything
// derived from it reads it that way too. A zone only the stored object defines
// is named as that object names it, quoted where its name needs it.
func TestFormatICalDateTimeInWritesTheEnteredWallClock(t *testing.T) {
	if _, err := time.LoadLocation("America/New_York"); err != nil {
		t.Skipf("tzdata unavailable: %v", err)
	}
	const exchange = "(UTC-05:00) Eastern Time (US & Canada)"
	object := UnfoldLines("BEGIN:VCALENDAR\r\nVERSION:2.0\r\nBEGIN:VTIMEZONE\r\nTZID:" + exchange + "\r\n" +
		"BEGIN:STANDARD\r\nDTSTART:16010101T020000\r\nTZOFFSETFROM:-0400\r\nTZOFFSETTO:-0500\r\nRRULE:FREQ=YEARLY;INTERVAL=1;BYDAY=1SU;BYMONTH=11\r\nEND:STANDARD\r\n" +
		"BEGIN:DAYLIGHT\r\nDTSTART:16010101T020000\r\nTZOFFSETFROM:-0500\r\nTZOFFSETTO:-0400\r\nRRULE:FREQ=YEARLY;INTERVAL=1;BYDAY=2SU;BYMONTH=3\r\nEND:DAYLIGHT\r\n" +
		"END:VTIMEZONE\r\nEND:VCALENDAR\r\n")
	tests := []struct {
		name        string
		value       string
		zone        EventZone
		wantLine    string
		wantInstant string
	}{
		{name: "host zone, skipped wall clock", value: "2024-03-10T02:30", zone: EventZoneIn("America/New_York", nil),
			wantLine: "DTSTART;TZID=America/New_York:20240310T023000", wantInstant: "2024-03-10T07:30:00Z"},
		{name: "zone the object defines", value: "2024-03-11T09:00", zone: EventZoneIn(exchange, object),
			wantLine: `DTSTART;TZID="` + exchange + `":20240311T090000`, wantInstant: "2024-03-11T13:00:00Z"},
		{name: "zone the object defines, skipped wall clock", value: "2024-03-10T02:30:15", zone: EventZoneIn(exchange, object),
			wantLine: `DTSTART;TZID="` + exchange + `":20240310T023015`, wantInstant: "2024-03-10T07:30:15Z"},
		{name: "that zone without the object", value: "2024-03-11T09:00", zone: EventZoneIn(exchange, nil),
			wantLine: "DTSTART:20240311T090000Z", wantInstant: "2024-03-11T09:00:00Z"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			line, err := FormatICalDateTimeIn(tt.value, false, false, "DTSTART", tt.zone)
			if err != nil || line != tt.wantLine {
				t.Errorf("FormatICalDateTimeIn() = %q, %v, want %q", line, err, tt.wantLine)
			}
			instant, err := tt.zone.FormInstant(tt.value)
			if err != nil || instant.Format(time.RFC3339) != tt.wantInstant {
				t.Errorf("FormInstant() = %s, %v, want %s", instant.Format(time.RFC3339), err, tt.wantInstant)
			}
		})
	}
	if got, err := FormatICalDateTime("2024-03-10T02:30", false, false, "DTSTART", "America/New_York"); err != nil || got != tests[0].wantLine {
		t.Errorf("FormatICalDateTime() = %q, %v, want %q", got, err, tests[0].wantLine)
	}

	lines := mustBuildEventComponent(t, "uid", "Series", "2024-03-04T09:00", "2024-03-04T10:00", false, "", "",
		&RecurrenceOptions{Frequency: "WEEKLY", Until: "2024-03-25"}, "", &EventOptions{Timezone: exchange, Object: object})
	for _, want := range []string{
		`DTSTART;TZID="` + exchange + `":20240304T090000`,
		`DTEND;TZID="` + exchange + `":20240304T100000`,
		"RRULE:FREQ=WEEKLY;UNTIL=20240326T035959Z",
	} {
		if !slices.Contains(lines, want) {
			t.Errorf("BuildEventComponent() = %q, want it to hold %q", lines, want)
		}
	}
}

// ResolveEventZone says whether a name named a zone, and reads a wall clock in
// the zone it names, or in UTC for a name nothing defines.
func TestResolveEventZoneSaysWhetherTheNameIsKnown(t *testing.T) {
	if _, err := time.LoadLocation("America/New_York"); err != nil {
		t.Skipf("tzdata unavailable: %v", err)
	}
	const exchange = "(UTC-05:00) Eastern Time (US & Canada)"
	// The object's definition keeps standard time all year.
	object := UnfoldLines("BEGIN:VCALENDAR\r\nVERSION:2.0\r\nBEGIN:VTIMEZONE\r\nTZID:" + exchange + "\r\n" +
		"BEGIN:STANDARD\r\nDTSTART:19700101T000000\r\nTZOFFSETFROM:-0500\r\nTZOFFSETTO:-0500\r\nEND:STANDARD\r\n" +
		"END:VTIMEZONE\r\nEND:VCALENDAR\r\n")
	wall := time.Date(2024, time.July, 1, 10, 0, 0, 0, time.UTC)
	tests := []struct {
		name   string
		tzid   string
		object []string
		known  bool
		// wantTZID is the TZID values are written with, "" for UTC.
		wantTZID string
		// wantInstant is the instant the wall clock 2024-07-01T10:00 names.
		wantInstant string
	}{
		{"no name", "", nil, true, "", "2024-07-01T10:00:00Z"},
		{"blank name", "  ", nil, true, "", "2024-07-01T10:00:00Z"},
		{"UTC", "UTC", nil, true, "", "2024-07-01T10:00:00Z"},
		{"utc lowercase", "utc", nil, true, "", "2024-07-01T10:00:00Z"},
		{"Utc padded", " Utc ", nil, true, "", "2024-07-01T10:00:00Z"},
		{"host zone", "America/New_York", nil, true, "America/New_York", "2024-07-01T14:00:00Z"},
		{"host zone with surrounding space", " America/New_York ", nil, true, "America/New_York", "2024-07-01T14:00:00Z"},
		{"zone the object defines", exchange, object, true, exchange, "2024-07-01T15:00:00Z"},
		{"that zone without the object", exchange, nil, false, "", "2024-07-01T10:00:00Z"},
		{"name no one defines", "Nowhere/Zone", nil, false, "", "2024-07-01T10:00:00Z"},
		{"the server's own zone", "Local", nil, false, "", "2024-07-01T10:00:00Z"},
		{"name holding a quote", `America/"New_York`, nil, false, "", "2024-07-01T10:00:00Z"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			zone, known := ResolveEventZone(tt.tzid, tt.object)
			if known != tt.known {
				t.Errorf("ResolveEventZone(%q) known = %v, want %v", tt.tzid, known, tt.known)
			}
			if zone.tzid != tt.wantTZID {
				t.Errorf("ResolveEventZone(%q) names zone %q, want %q", tt.tzid, zone.tzid, tt.wantTZID)
			}
			if got := zone.Instant(wall).Format(time.RFC3339); got != tt.wantInstant {
				t.Errorf("ResolveEventZone(%q) reads 2024-07-01T10:00 as %s, want %s", tt.tzid, got, tt.wantInstant)
			}
		})
	}
}

// WallClock reads an instant on the zone's clock as Instant reads a wall clock
// back: through the object's definition, the host's zone, or UTC.
func TestEventZoneWallClockIsTheWallClockInstantReads(t *testing.T) {
	if _, err := time.LoadLocation("America/New_York"); err != nil {
		t.Skipf("tzdata unavailable: %v", err)
	}
	const exchange = "(UTC-05:00) Eastern Time (US & Canada)"
	// The object's definition keeps standard time all year, unlike the
	// host's zone of the same name.
	object := UnfoldLines("BEGIN:VCALENDAR\r\nVERSION:2.0\r\nBEGIN:VTIMEZONE\r\nTZID:America/New_York\r\n" +
		"BEGIN:STANDARD\r\nDTSTART:19700101T000000\r\nTZOFFSETFROM:-0500\r\nTZOFFSETTO:-0500\r\nEND:STANDARD\r\n" +
		"END:VTIMEZONE\r\nEND:VCALENDAR\r\n")
	tests := []struct {
		name    string
		zone    EventZone
		instant string
		want    string
	}{
		{"host zone in daylight time", EventZoneIn("America/New_York", nil), "2024-07-01T14:00:00Z", "2024-07-01T10:00"},
		{"host zone in standard time", EventZoneIn("America/New_York", nil), "2024-01-15T15:00:00Z", "2024-01-15T10:00"},
		{"the object's definition", EventZoneIn("America/New_York", object), "2024-07-01T15:00:00Z", "2024-07-01T10:00"},
		{"a zone neither defines", EventZoneIn(exchange, nil), "2024-07-01T14:00:00Z", "2024-07-01T14:00"},
		{"UTC", EventZoneIn("", nil), "2024-07-01T14:00:00Z", "2024-07-01T14:00"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			instant, err := time.Parse(time.RFC3339, tt.instant)
			if err != nil {
				t.Fatal(err)
			}
			wall := tt.zone.WallClock(instant)
			if got := wall.Format("2006-01-02T15:04"); got != tt.want || wall.Location() != time.UTC {
				t.Errorf("WallClock(%s) = %s in %s, want %s in UTC", tt.instant, got, wall.Location(), tt.want)
			}
			if back := tt.zone.Instant(wall); !back.Equal(instant) {
				t.Errorf("Instant(WallClock(%s)) = %s", tt.instant, back.Format(time.RFC3339))
			}
		})
	}
}

func TestBuildEvent_WithOptions(t *testing.T) {
	opts := &EventOptions{
		Timezone:     "America/New_York",
		URL:          "https://example.com/event",
		Status:       "confirmed",
		Categories:   []string{"Team", "Planning"},
		Class:        "private",
		Transparency: "opaque",
		Organizer:    "Alice Example <alice@example.com>",
		Attendees:    []string{"Bob Example <bob@example.com>"},
		Attachments:  []string{"https://example.com/agenda.pdf"},
		Reminders:    []int{10},
	}

	ical := mustBuildEvent(t, "uid-1@calcard", "Planning", "2025-01-15T14:00", "2025-01-15T15:00", false, "HQ", "Discuss roadmap", nil, opts)

	required := []string{
		"DTSTART;TZID=America/New_York:20250115T140000",
		"DTEND;TZID=America/New_York:20250115T150000",
		"URL:https://example.com/event",
		"STATUS:CONFIRMED",
		"CLASS:PRIVATE",
		"TRANSP:OPAQUE",
		"CATEGORIES:Team,Planning",
		"ORGANIZER;CN=Alice Example:mailto:alice@example.com",
		"ATTENDEE;CN=Bob Example:mailto:bob@example.com",
		"ATTACH:https://example.com/agenda.pdf",
		"BEGIN:VALARM",
		"TRIGGER:-PT10M",
	}
	for _, want := range required {
		if !strings.Contains(ical, want) {
			t.Errorf("BuildEvent() missing %q in:\n%s", want, ical)
		}
	}
	if strings.Contains(ical, "VTIMEZONE") {
		t.Errorf("BuildEvent() defines a zone, which the validator storing it defines instead:\n%s", ical)
	}
}

func TestBuildEvent_SkipsInjectedOptionValues(t *testing.T) {
	opts := &EventOptions{
		URL:         "https://example.com\r\nBEGIN:VALARM",
		Organizer:   "Alice <alice@example.com\r\nATTENDEE:mailto:mallory@example.com>",
		Attendees:   []string{"Bob <bob@example.com>", "Evil <evil@example.com\r\nSUMMARY:Hacked>"},
		Attachments: []string{"https://example.com/file.pdf\r\nSUMMARY:Hacked"},
	}

	ical := mustBuildEvent(t, "uid-2@calcard", "Secure Event", "2025-01-15T14:00", "2025-01-15T15:00", false, "", "", nil, opts)

	forbidden := []string{
		"URL:https://example.com",
		"ORGANIZER;CN=Alice",
		"ATTACH:https://example.com/file.pdf",
		"SUMMARY:Hacked",
	}
	for _, bad := range forbidden {
		if strings.Contains(ical, bad) {
			t.Errorf("BuildEvent() should not contain injected/invalid value %q in:\n%s", bad, ical)
		}
	}
	if !strings.Contains(ical, "ATTENDEE;CN=Bob:mailto:bob@example.com") {
		t.Errorf("BuildEvent() should keep valid attendees in:\n%s", ical)
	}
}

// STATUS, CLASS and TRANSP and the RRULE's FREQ are written as the form sends
// them, so a value that is not a token -- for FREQ, not one of its seven -- is
// refused rather than written: a line break in one would start a content line
// of the sender's choosing.
func TestBuildEventRefusesValuesThatAreNotTokens(t *testing.T) {
	injected := "CONFIRMED\r\nATTENDEE;CN=Evil:mailto:evil@example.com"
	tests := []struct {
		name       string
		opts       EventOptions
		recurrence *RecurrenceOptions
	}{
		{name: "status", opts: EventOptions{Status: injected}},
		{name: "class", opts: EventOptions{Class: "PUBLIC\r\nX-INJECTED:1"}},
		{name: "class with a space", opts: EventOptions{Class: "VERY PRIVATE"}},
		{name: "transparency", opts: EventOptions{Transparency: "OPAQUE\nX-INJECTED:1"}},
		{name: "frequency", recurrence: &RecurrenceOptions{Frequency: "WEEKLY\r\nX-INJECTED:1", Interval: 1}},
		{name: "frequency with a rule part", recurrence: &RecurrenceOptions{Frequency: "DAILY;BYHOUR=1,2,3", Interval: 1}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			lines, err := BuildEventComponent("uid", "Event", "2025-01-15T14:00", "2025-01-15T15:00", false, "", "", tt.recurrence, "", &tt.opts)
			if err == nil {
				t.Fatalf("BuildEventComponent() = %q, want an error", lines)
			}
		})
	}

	lines := mustBuildEventComponent(t, "uid", "Event", "2025-01-15T14:00", "2025-01-15T15:00", false, "", "",
		&RecurrenceOptions{Frequency: "weekly", Interval: 1},
		"", &EventOptions{Status: "tentative", Class: "x-team-only", Transparency: "Transparent"})
	for _, want := range []string{"STATUS:TENTATIVE", "CLASS:X-TEAM-ONLY", "TRANSP:TRANSPARENT", "RRULE:FREQ=WEEKLY"} {
		if !slices.Contains(lines, want) {
			t.Errorf("BuildEventComponent() = %q, want it to hold %q", lines, want)
		}
	}
	// A stored event may carry a value RFC 5545 does not list for VEVENT,
	// which an edit submits back as the editor showed it.
	lines = mustBuildEventComponent(t, "uid", "Event", "2025-01-15T14:00", "2025-01-15T15:00", false, "", "", nil,
		"", &EventOptions{Status: "NEEDS-ACTION", Transparency: "X-FOO"})
	for _, want := range []string{"STATUS:NEEDS-ACTION", "TRANSP:X-FOO"} {
		if !slices.Contains(lines, want) {
			t.Errorf("BuildEventComponent() = %q, want it to hold %q", lines, want)
		}
	}
}

// A display name is a CN parameter value, which RFC 5545 §3.2 quotes when it
// holds a colon, semicolon or comma and which cannot hold a DQUOTE or a
// control character at all.
func TestBuildEventQuotesDisplayNames(t *testing.T) {
	tests := []struct {
		attendee string
		want     string
	}{
		{"Doe, John <john@example.com>", `ATTENDEE;CN="Doe, John":mailto:john@example.com`},
		{"Doe; John <john@example.com>", `ATTENDEE;CN="Doe; John":mailto:john@example.com`},
		{"Dr: Who <who@example.com>", `ATTENDEE;CN="Dr: Who":mailto:who@example.com`},
		{`"Doe, Jane" <jane@example.com>`, `ATTENDEE;CN="Doe, Jane":mailto:jane@example.com`},
		{`Bob "the Builder" Smith <bob@example.com>`, `ATTENDEE;CN=Bob the Builder Smith:mailto:bob@example.com`},
		{"Ann\tLee <ann@example.com>", `ATTENDEE;CN=Ann Lee:mailto:ann@example.com`},
		{`Back\slash <b@example.com>`, `ATTENDEE;CN=Back\slash:mailto:b@example.com`},
		{"plain@example.com", `ATTENDEE:mailto:plain@example.com`},
		{`"Ops <team>" <ops@example.com>`, `ATTENDEE;CN=Ops <team>:mailto:ops@example.com`},
	}
	for _, tt := range tests {
		if got := AttendeeLine("ATTENDEE", tt.attendee); got != tt.want {
			t.Errorf("AttendeeLine(%q) = %q, want %q", tt.attendee, got, tt.want)
		}
	}
}

func TestUnfoldLines(t *testing.T) {
	tests := []struct {
		name  string
		input string
		want  []string
	}{
		{
			name:  "no folding",
			input: "LINE1\nLINE2\nLINE3",
			want:  []string{"LINE1", "LINE2", "LINE3"},
		},
		{
			name:  "folded line with space",
			input: "DESCRIPTION:This is a long\n description that spans multiple lines",
			want:  []string{"DESCRIPTION:This is a longdescription that spans multiple lines"},
		},
		{
			name:  "folded line with tab",
			input: "SUMMARY:Test\n\tEvent",
			want:  []string{"SUMMARY:TestEvent"},
		},
		{
			name:  "crlf line endings",
			input: "LINE1\r\nLINE2\r\nLINE3",
			want:  []string{"LINE1", "LINE2", "LINE3"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := UnfoldLines(tt.input)
			if len(got) != len(tt.want) {
				t.Errorf("UnfoldLines() got %d lines, want %d", len(got), len(tt.want))
				return
			}
			for i := range got {
				if got[i] != tt.want[i] {
					t.Errorf("UnfoldLines() line %d = %q, want %q", i, got[i], tt.want[i])
				}
			}
		})
	}
}

func TestSplitComponents(t *testing.T) {
	ical := `BEGIN:VCALENDAR
VERSION:2.0
PRODID:-//Test//EN
BEGIN:VEVENT
UID:event1
SUMMARY:Event 1
END:VEVENT
BEGIN:VEVENT
UID:event2
SUMMARY:Event 2
END:VEVENT
END:VCALENDAR`

	header, events, footer := SplitComponents(ical)

	if len(header) < 2 {
		t.Errorf("SplitComponents() header should have VERSION and PRODID, got %d items", len(header))
	}
	if len(events) != 2 {
		t.Errorf("SplitComponents() should find 2 events, got %d", len(events))
	}
	if len(footer) < 1 {
		t.Errorf("SplitComponents() should have footer with END:VCALENDAR, got %d items", len(footer))
	}
}

func TestBuildFromComponents(t *testing.T) {
	header := []string{"BEGIN:VCALENDAR", "VERSION:2.0"}
	events := [][]string{
		{"UID:event1", "SUMMARY:Event 1"},
		{"UID:event2", "SUMMARY:Event 2"},
	}
	footer := []string{"END:VCALENDAR"}

	ical := BuildFromComponents(header, events, footer)

	if !strings.Contains(ical, "BEGIN:VCALENDAR") {
		t.Error("BuildFromComponents() missing BEGIN:VCALENDAR")
	}
	if !strings.Contains(ical, "BEGIN:VEVENT") {
		t.Error("BuildFromComponents() missing BEGIN:VEVENT")
	}
	if !strings.Contains(ical, "UID:event1") {
		t.Error("BuildFromComponents() missing event1")
	}
	if !strings.Contains(ical, "UID:event2") {
		t.Error("BuildFromComponents() missing event2")
	}
	if !strings.Contains(ical, "END:VCALENDAR") {
		t.Error("BuildFromComponents() missing END:VCALENDAR")
	}
}

func TestRecurrenceIDValue(t *testing.T) {
	tests := []struct {
		name  string
		lines []string
		want  string
	}{
		{
			name:  "no recurrence-id",
			lines: []string{"UID:test", "SUMMARY:Test"},
			want:  "",
		},
		{
			name:  "has recurrence-id",
			lines: []string{"UID:test", "RECURRENCE-ID:20250115T140000Z", "SUMMARY:Test"},
			want:  "20250115T140000Z",
		},
		{
			name:  "recurrence-id with parameters",
			lines: []string{"UID:test", "RECURRENCE-ID;VALUE=DATE:20250115", "SUMMARY:Test"},
			want:  "20250115",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := RecurrenceIDValue(tt.lines)
			if got != tt.want {
				t.Errorf("RecurrenceIDValue() = %q, want %q", got, tt.want)
			}
		})
	}
}

func TestHasPropertyValue(t *testing.T) {
	lines := []string{
		"UID:test123",
		"SUMMARY:Test Event",
		"STATUS:CONFIRMED",
	}

	tests := []struct {
		name  string
		prop  string
		value string
		want  bool
	}{
		{
			name:  "property exists with matching value",
			prop:  "STATUS",
			value: "CONFIRMED",
			want:  true,
		},
		{
			name:  "property exists with non-matching value",
			prop:  "STATUS",
			value: "TENTATIVE",
			want:  false,
		},
		{
			name:  "property does not exist",
			prop:  "LOCATION",
			value: "Office",
			want:  false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := HasPropertyValue(lines, tt.prop, tt.value)
			if got != tt.want {
				t.Errorf("HasPropertyValue() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestEscapeICalValue(t *testing.T) {
	tests := []struct {
		name  string
		input string
		want  string
	}{
		{
			name:  "no special characters",
			input: "Simple text",
			want:  "Simple text",
		},
		{
			name:  "backslash",
			input: "Path\\to\\file",
			want:  "Path\\\\to\\\\file",
		},
		{
			name:  "semicolon",
			input: "Item1;Item2;Item3",
			want:  "Item1\\;Item2\\;Item3",
		},
		{
			name:  "comma",
			input: "One,Two,Three",
			want:  "One\\,Two\\,Three",
		},
		{
			name:  "newline",
			input: "Line1\nLine2",
			want:  "Line1\\nLine2",
		},
		{
			name:  "multiple special chars",
			input: "Text;with,special\\chars\nand newlines",
			want:  "Text\\;with\\,special\\\\chars\\nand newlines",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := EscapeICalValue(tt.input)
			if got != tt.want {
				t.Errorf("EscapeICalValue() = %q, want %q", got, tt.want)
			}
		})
	}
}

func TestParseICSFile(t *testing.T) {
	icsContent := `BEGIN:VCALENDAR
VERSION:2.0
PRODID:-//Test//EN
BEGIN:VEVENT
UID:event1@example.com
SUMMARY:Event 1
DTSTART:20250115T140000Z
DTEND:20250115T150000Z
END:VEVENT
BEGIN:VEVENT
UID:event2@example.com
SUMMARY:Event 2
DTSTART:20250116T140000Z
DTEND:20250116T150000Z
END:VEVENT
END:VCALENDAR`

	events, _, err := ParseICSFile(icsContent)
	if err != nil {
		t.Fatalf("ParseICSFile() error = %v", err)
	}

	if len(events) != 2 {
		t.Errorf("ParseICSFile() should return 2 events, got %d", len(events))
	}

	for i, event := range events {
		if !strings.Contains(event, "BEGIN:VCALENDAR") {
			t.Errorf("Event %d should be wrapped in VCALENDAR", i)
		}
		if !strings.Contains(event, "BEGIN:VEVENT") {
			t.Errorf("Event %d should contain VEVENT", i)
		}
		if !strings.Contains(event, "END:VCALENDAR") {
			t.Errorf("Event %d should have END:VCALENDAR", i)
		}
	}

	if !strings.Contains(events[0], "event1@example.com") {
		t.Error("First event should contain event1 UID")
	}
	if !strings.Contains(events[1], "event2@example.com") {
		t.Error("Second event should contain event2 UID")
	}
}

func TestParseICSFileGroupsSharedUIDAndPreservesTimezone(t *testing.T) {
	icsContent := `BEGIN:VCALENDAR
VERSION:2.0
PRODID:-//Test//EN
BEGIN:VTIMEZONE
TZID:America/Chicago
END:VTIMEZONE
BEGIN:VEVENT
UID:series@example.com
SUMMARY:Series
DTSTART;TZID=America/Chicago:20250115T140000
END:VEVENT
BEGIN:VEVENT
UID:series@example.com
RECURRENCE-ID;TZID=America/Chicago:20250122T140000
SUMMARY:Override
DTSTART;TZID=America/Chicago:20250122T150000
END:VEVENT
END:VCALENDAR`

	events, _, err := ParseICSFile(icsContent)
	if err != nil {
		t.Fatalf("ParseICSFile() error = %v", err)
	}
	if len(events) != 1 {
		t.Fatalf("ParseICSFile() should group matching UIDs into one resource, got %d", len(events))
	}
	if strings.Count(events[0], "BEGIN:VEVENT") != 2 {
		t.Fatalf("expected grouped resource to contain 2 VEVENTs, got: %s", events[0])
	}
	if !strings.Contains(events[0], "BEGIN:VTIMEZONE") {
		t.Fatalf("expected grouped resource to preserve VTIMEZONE, got: %s", events[0])
	}
}

func TestParseICSFileKeepsNestedAlarms(t *testing.T) {
	icsContent := "BEGIN:VCALENDAR\r\nVERSION:2.0\r\n" +
		"BEGIN:VEVENT\r\nUID:with-alarm@example.com\r\nDTSTART:20260320T100000Z\r\n" +
		"BEGIN:VALARM\r\nACTION:DISPLAY\r\nTRIGGER:-PT15M\r\nEND:VALARM\r\nEND:VEVENT\r\n" +
		"BEGIN:VEVENT\r\nUID:plain@example.com\r\nDTSTART:20260321T100000Z\r\nEND:VEVENT\r\n" +
		"END:VCALENDAR\r\n"

	events, _, err := ParseICSFile(icsContent)
	if err != nil {
		t.Fatalf("ParseICSFile() error = %v", err)
	}
	if len(events) != 2 {
		t.Fatalf("ParseICSFile() returned %d resources, want 2", len(events))
	}
	if !strings.Contains(events[0], "BEGIN:VALARM\r\nACTION:DISPLAY\r\nTRIGGER:-PT15M\r\nEND:VALARM\r\nEND:VEVENT\r\n") {
		t.Fatalf("expected the alarm to stay inside its event, got: %s", events[0])
	}
	if strings.Contains(events[1], "VALARM") {
		t.Fatalf("expected the alarm not to leak into the next event, got: %s", events[1])
	}
}

func TestParseICSFileGroupsByEventUIDNotAlarmUID(t *testing.T) {
	const eventUID = "6C4E3B5F-2A7D-4E3C-9F1B-8D2A6E7C4B10"
	icsContent := "BEGIN:VCALENDAR\r\nVERSION:2.0\r\nPRODID:-//Apple Inc.//macOS 15.0//EN\r\n" +
		"BEGIN:VEVENT\r\n" + appleAlarm("A1A1A1A1-0000-4000-8000-000000000001") +
		"UID:" + eventUID + "\r\nSUMMARY:Standup\r\nDTSTART:20260320T100000Z\r\nRRULE:FREQ=WEEKLY;COUNT=10\r\nEND:VEVENT\r\n" +
		"BEGIN:VEVENT\r\n" + appleAlarm("B2B2B2B2-0000-4000-8000-000000000002") +
		"UID:" + eventUID + "\r\nRECURRENCE-ID:20260327T100000Z\r\nSUMMARY:Standup (moved)\r\nDTSTART:20260327T110000Z\r\nEND:VEVENT\r\n" +
		"END:VCALENDAR\r\n"

	events, _, err := ParseICSFile(icsContent)
	if err != nil {
		t.Fatalf("ParseICSFile() error = %v", err)
	}
	if len(events) != 1 {
		t.Fatalf("ParseICSFile() should group the master and override into one resource, got %d", len(events))
	}
	if strings.Count(events[0], "BEGIN:VEVENT") != 2 || strings.Count(events[0], "BEGIN:VALARM") != 2 {
		t.Fatalf("expected both VEVENTs and their alarms in the resource, got: %s", events[0])
	}
	if got := ExtractUID(events[0]); got != eventUID {
		t.Fatalf("ExtractUID() = %q, want %q", got, eventUID)
	}
}

func TestParseICSFileGroupsPlainAndParameterizedUIDs(t *testing.T) {
	icsContent := "BEGIN:VCALENDAR\r\nVERSION:2.0\r\n" +
		"BEGIN:VEVENT\r\nUID;X-SOURCE=import:series@example.com\r\nDTSTART:20260320T100000Z\r\nRRULE:FREQ=WEEKLY;COUNT=10\r\nEND:VEVENT\r\n" +
		"BEGIN:VEVENT\r\nUID:series@example.com\r\nRECURRENCE-ID:20260327T100000Z\r\nDTSTART:20260327T110000Z\r\nEND:VEVENT\r\n" +
		"END:VCALENDAR\r\n"

	events, _, err := ParseICSFile(icsContent)
	if err != nil {
		t.Fatalf("ParseICSFile() error = %v", err)
	}
	if len(events) != 1 || strings.Count(events[0], "BEGIN:VEVENT") != 2 {
		t.Fatalf("ParseICSFile() should group the master and override into one resource, got %q", events)
	}
}

func TestParseICSFileRejectsMalformedInput(t *testing.T) {
	tests := []struct {
		name string
		ics  string
	}{
		{name: "missing calendar wrapper", ics: "BEGIN:VEVENT\r\nEND:VEVENT\r\n"},
		{name: "missing end", ics: "BEGIN:VCALENDAR\r\nBEGIN:VEVENT\r\nEND:VEVENT\r\n"},
		{name: "missing event", ics: "BEGIN:VCALENDAR\r\nEND:VCALENDAR\r\n"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			events, _, err := ParseICSFile(tt.ics)
			if err == nil {
				t.Fatalf("expected error, got events: %#v", events)
			}
		})
	}
}

func TestExtractUID(t *testing.T) {
	tests := []struct {
		name string
		ical string
		want string
	}{
		{
			name: "valid UID",
			ical: `BEGIN:VCALENDAR
VERSION:2.0
BEGIN:VEVENT
UID:test-uid@example.com
SUMMARY:Test
END:VEVENT
END:VCALENDAR`,
			want: "test-uid@example.com",
		},
		{
			name: "no UID",
			ical: `BEGIN:VCALENDAR
VERSION:2.0
BEGIN:VEVENT
SUMMARY:Test
END:VEVENT
END:VCALENDAR`,
			want: "",
		},
		{
			name: "UID with whitespace",
			ical: `BEGIN:VCALENDAR
BEGIN:VEVENT
UID:  spaced-uid@example.com
END:VEVENT
END:VCALENDAR`,
			want: "spaced-uid@example.com",
		},
		{
			name: "Apple alarm UID before the event UID",
			ical: "BEGIN:VCALENDAR\r\nBEGIN:VEVENT\r\n" + appleAlarm("D1B2C3E4-F5A6-4B7C-8D9E-0F1A2B3C4D5E") +
				"UID:6C4E3B5F-2A7D-4E3C-9F1B-8D2A6E7C4B10\r\nEND:VEVENT\r\nEND:VCALENDAR\r\n",
			want: "6C4E3B5F-2A7D-4E3C-9F1B-8D2A6E7C4B10",
		},
		{
			name: "UID with parameters",
			ical: "BEGIN:VCALENDAR\r\nBEGIN:VEVENT\r\nUID;X-SOURCE=import:param-uid@example.com\r\nEND:VEVENT\r\nEND:VCALENDAR\r\n",
			want: "param-uid@example.com",
		},
		{
			name: "UID with a quoted parameter holding a colon",
			ical: "BEGIN:VCALENDAR\r\nBEGIN:VEVENT\r\nUID;X-SOURCE=\"https://example.com:8443/feed\":param-uid@example.com\r\nEND:VEVENT\r\nEND:VCALENDAR\r\n",
			want: "param-uid@example.com",
		},
		{
			name: "property name merely starting with UID",
			ical: "BEGIN:VCALENDAR\r\nBEGIN:VEVENT\r\nUIDX:not-a-uid@example.com\r\nEND:VEVENT\r\nEND:VCALENDAR\r\n",
			want: "",
		},
		{
			name: "UID only on the alarm",
			ical: "BEGIN:VCALENDAR\r\nBEGIN:VEVENT\r\nSUMMARY:Test\r\n" + appleAlarm("D1B2C3E4-F5A6-4B7C-8D9E-0F1A2B3C4D5E") +
				"END:VEVENT\r\nEND:VCALENDAR\r\n",
			want: "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := ExtractUID(tt.ical)
			if got != tt.want {
				t.Errorf("ExtractUID() = %q, want %q", got, tt.want)
			}
		})
	}
}

func TestEnsureUID(t *testing.T) {
	ical := "BEGIN:VCALENDAR\r\nBEGIN:VEVENT\r\nSUMMARY:Test\r\nEND:VEVENT\r\nEND:VCALENDAR\r\n"
	got := EnsureUID(ical, "generated@example.com")
	if !strings.Contains(got, "UID:generated@example.com") {
		t.Fatalf("expected UID to be injected, got: %s", got)
	}
}

func TestEnsureUIDIgnoresAlarmUID(t *testing.T) {
	ical := "BEGIN:VCALENDAR\r\nBEGIN:VEVENT\r\nSUMMARY:Test\r\n" + appleAlarm("D1B2C3E4-F5A6-4B7C-8D9E-0F1A2B3C4D5E") + "END:VEVENT\r\nEND:VCALENDAR\r\n"
	got := EnsureUID(ical, "generated@example.com")
	if ExtractUID(got) != "generated@example.com" {
		t.Fatalf("expected the event UID to be injected, got: %s", got)
	}
}

// appleAlarm is a VALARM as Apple Calendar writes it: RFC 9074 §4 gives the
// alarm its own UID, mirrored in X-WR-ALARMUID.
func appleAlarm(uid string) string {
	return "BEGIN:VALARM\r\nACTION:DISPLAY\r\nDESCRIPTION:Reminder\r\nTRIGGER:-PT15M\r\nUID:" + uid + "\r\nX-WR-ALARMUID:" + uid + "\r\nEND:VALARM\r\n"
}

// A UI event's resource name is its UID made safe for a path segment, with no
// extension added: CalDAV serves it at that name plus .ics, so a name already
// ending in .ics would be served at a .ics.ics href. Every character it
// replaces becomes one octet, so the name is never longer than the UID, and a
// UID the server accepts gives a name it accepts.
func TestResourceNameForUID(t *testing.T) {
	for uid, want := range map[string]string{
		"series/one@example.com": "series_one_example.com",
		"plain":                  "plain",
		"imported.ics":           "imported.ics",
		" padded ":               "padded",
		"日本":                     "event",
		"":                       "event",
	} {
		if got := ResourceNameForUID(uid); got != want {
			t.Errorf("ResourceNameForUID(%q) = %q, want %q", uid, got, want)
		}
	}
	for _, uid := range []string{strings.Repeat("a", 1024), strings.Repeat("é", 512), "x" + strings.Repeat("é", 511) + "y"} {
		if got := ResourceNameForUID(uid); len(got) > len(uid) {
			t.Errorf("ResourceNameForUID of a %d-octet UID = %d octets", len(uid), len(got))
		}
	}
}

func TestFormatICalDateTimeUTCRecurrenceIdentity(t *testing.T) {
	got, err := FormatICalDateTime("2025-07-24T09:00", false, false, "RECURRENCE-ID", "UTC")
	if err != nil || got != "RECURRENCE-ID:20250724T090000Z" {
		t.Fatalf("got %q, %v", got, err)
	}
}

func TestSameRecurrenceID(t *testing.T) {
	for _, tc := range []struct {
		left, right string
		want        bool
	}{
		{"RECURRENCE-ID;TZID=UTC:20250724T090000", "RECURRENCE-ID:20250724T090000Z", true},
		{"RECURRENCE-ID;TZID=America/New_York:20250724T090000", "RECURRENCE-ID:20250724T130000Z", true},
		{"RECURRENCE-ID;TZID=America/New_York:20250724T090000", "RECURRENCE-ID;TZID=America/Los_Angeles:20250724T090000", false},
		{"RECURRENCE-ID;VALUE=DATE:20250724", "RECURRENCE-ID:20250724T000000Z", false},
		{"RECURRENCE-ID:20250724T090000", "RECURRENCE-ID:20250724T090000Z", false},
	} {
		if got := SameRecurrenceID([]string{tc.left}, []string{tc.right}); got != tc.want {
			t.Errorf("%s vs %s: %t", tc.left, tc.right, got)
		}
	}
}

// RFC 5545 Section 3.1 lets a quoted parameter value hold a colon; Exchange
// writes TZIDs such as "(UTC-05:00) Eastern". The value starts at the first
// colon outside quotes.
func TestContentLineReadersHonourQuotedColons(t *testing.T) {
	const quoted = `RECURRENCE-ID;TZID="(UTC-05:00) Eastern":20250724T090000`
	if got := RecurrenceIDValue([]string{"UID:x", quoted}); got != "20250724T090000" {
		t.Errorf("RecurrenceIDValue() = %q, want 20250724T090000", got)
	}
	exdate := `EXDATE;TZID="(UTC-05:00) Eastern":20250724T090000`
	if !HasPropertyValue([]string{exdate}, "EXDATE", "20250724T090000") {
		t.Errorf("HasPropertyValue missed %q", exdate)
	}
	left := `RECURRENCE-ID;X-NOTE="a:b";TZID=America/New_York:20250724T090000`
	if !SameRecurrenceID([]string{left}, []string{"RECURRENCE-ID:20250724T130000Z"}) {
		t.Errorf("SameRecurrenceID did not resolve %q", left)
	}
}

func TestContentLineReadersMatchWholePropertyNames(t *testing.T) {
	if got := RecurrenceIDValue([]string{"RECURRENCE-IDX:1", "X-FOO:2"}); got != "" {
		t.Errorf("RecurrenceIDValue() = %q from another property", got)
	}
	if HasPropertyValue([]string{"EXDATEX:1"}, "EXDATE", "1") {
		t.Error("HasPropertyValue matched another property")
	}
}

// An export file is not a calendar object resource. Each payload drops the
// file's METHOD, which RFC 4791 §4.1 rules out of a stored resource, and
// carries only the VTIMEZONEs its own events name, leaving behind the file's
// other components -- a VTODO in a payload of VEVENTs would make it two types of
// calendar component -- and zones only other events use.
func TestParseICSFileShapesEachPayloadAsAResource(t *testing.T) {
	zone := func(tzid string) string {
		return "BEGIN:VTIMEZONE\r\nTZID:" + tzid + "\r\nBEGIN:STANDARD\r\nDTSTART:19701101T020000\r\nTZOFFSETFROM:-0500\r\nTZOFFSETTO:-0600\r\nEND:STANDARD\r\nEND:VTIMEZONE\r\n"
	}
	ics := "\xef\xbb\xbfBEGIN:VCALENDAR\r\nVERSION:2.0\r\nPRODID:-//Google Inc//Google Calendar 70.9054//EN\r\nMETHOD:PUBLISH\r\n" +
		zone("America/Chicago") + zone("Europe/Berlin") +
		"BEGIN:VTODO\r\nUID:task\r\nSUMMARY:Task\r\nEND:VTODO\r\n" +
		"BEGIN:VEVENT\r\nUID:chicago\r\nDTSTART;TZID=America/Chicago:20260105T090000\r\nEND:VEVENT\r\n" +
		"BEGIN:VEVENT\r\nUID:utc\r\nDTSTART:20260105T090000Z\r\nEND:VEVENT\r\n" +
		"END:VCALENDAR\r\n"

	events, leftOut, err := ParseICSFile(ics)
	if err != nil {
		t.Fatalf("ParseICSFile() error = %v", err)
	}
	if len(events) != 2 {
		t.Fatalf("ParseICSFile() returned %d payloads, want 2", len(events))
	}
	if leftOut != 1 {
		t.Errorf("ParseICSFile() left out %d components, want the VTODO counted", leftOut)
	}
	for _, event := range events {
		for _, unwanted := range []string{"METHOD", "VTODO", "Europe/Berlin", "\xef\xbb\xbf"} {
			if strings.Contains(event, unwanted) {
				t.Errorf("payload carries %q:\n%s", unwanted, event)
			}
		}
	}
	if !strings.Contains(events[0], "TZID:America/Chicago") {
		t.Errorf("the Chicago event lost the zone it names:\n%s", events[0])
	}
	if strings.Contains(events[1], "VTIMEZONE") {
		t.Errorf("the UTC event carries a zone it does not name:\n%s", events[1])
	}
}

// A file of concatenated exports (cat a.ics b.ics) holds several VCALENDARs.
// Each payload is one calendar object resource, which RFC 5545 §3.6 lets carry
// a calendar property once, so the first calendar's header is the one kept;
// each event's zones are defined as its own calendar defines them.
func TestParseICSFileMergesConcatenatedCalendars(t *testing.T) {
	zone := func(tzname string) string {
		return "BEGIN:VTIMEZONE\r\nTZID:Europe/Berlin\r\nBEGIN:STANDARD\r\nDTSTART:19701025T030000\r\nTZOFFSETFROM:+0200\r\nTZOFFSETTO:+0100\r\nTZNAME:" + tzname + "\r\nEND:STANDARD\r\nEND:VTIMEZONE\r\n"
	}
	calendar := func(name, uid string) string {
		return "BEGIN:VCALENDAR\r\nVERSION:2.0\r\nPRODID:-//" + name + "//EN\r\nCALSCALE:GREGORIAN\r\nMETHOD:PUBLISH\r\nX-WR-CALNAME:" + name + "\r\n" +
			zone(name) +
			"BEGIN:VEVENT\r\nUID:" + uid + "\r\nDTSTAMP:20260101T000000Z\r\nSUMMARY:" + uid + "\r\nDTSTART;TZID=Europe/Berlin:20260105T090000\r\nDTEND;TZID=Europe/Berlin:20260105T100000\r\nEND:VEVENT\r\n" +
			"BEGIN:VJOURNAL\r\nUID:note-" + uid + "\r\nDTSTAMP:20260101T000000Z\r\nEND:VJOURNAL\r\n" +
			"END:VCALENDAR\r\n"
	}
	ics := calendar("First", "a") + calendar("Second", "b")

	events, leftOut, err := ParseICSFile(ics)
	if err != nil {
		t.Fatalf("ParseICSFile() error = %v", err)
	}
	if len(events) != 2 {
		t.Fatalf("ParseICSFile() returned %d payloads, want 2", len(events))
	}
	if leftOut != 2 {
		t.Errorf("ParseICSFile() left out %d components, want both VJOURNALs counted", leftOut)
	}
	for i, event := range events {
		zoneName := []string{"TZNAME:First", "TZNAME:Second"}[i]
		for _, line := range []string{"VERSION:2.0", "PRODID:-//First//EN", "CALSCALE:GREGORIAN", "BEGIN:VTIMEZONE", zoneName} {
			if n := strings.Count(event, line+"\r\n"); n != 1 {
				t.Errorf("payload carries %q %d times, want once:\n%s", line, n, event)
			}
		}
		for _, unwanted := range []string{"PRODID:-//Second", "X-WR-CALNAME", "METHOD", "VJOURNAL"} {
			if strings.Contains(event, unwanted) {
				t.Errorf("payload carries %q:\n%s", unwanted, event)
			}
		}
	}
	if !strings.Contains(events[0], "UID:a\r\n") || !strings.Contains(events[1], "UID:b\r\n") {
		t.Errorf("payloads = %q, want the first and second calendars' events in order", events)
	}
}

// Outlook names every zone it exports without a standard name "Customized Time
// Zone", so two exports concatenated into one file can define that TZID
// differently. Each event is placed by its own calendar's definition, wherever
// in that calendar the definition sits.
func TestParseICSFileDefinesEachEventsZonesAsItsOwnCalendarDoes(t *testing.T) {
	zone := func(offset string) string {
		return "BEGIN:VTIMEZONE\r\nTZID:Customized Time Zone\r\nBEGIN:STANDARD\r\nDTSTART:16010101T000000\r\n" +
			"TZOFFSETFROM:" + offset + "\r\nTZOFFSETTO:" + offset + "\r\nEND:STANDARD\r\nEND:VTIMEZONE\r\n"
	}
	event := func(uid string) string {
		return "BEGIN:VEVENT\r\nUID:" + uid + "\r\nDTSTAMP:20260101T000000Z\r\n" +
			"DTSTART;TZID=\"Customized Time Zone\":20260105T090000\r\nDTEND;TZID=\"Customized Time Zone\":20260105T100000\r\nEND:VEVENT\r\n"
	}
	ics := "BEGIN:VCALENDAR\r\nVERSION:2.0\r\nPRODID:-//Microsoft Corporation//Outlook 16.0 MIMEDIR//EN\r\nMETHOD:PUBLISH\r\n" +
		zone("-0500") + event("chicago") + "END:VCALENDAR\r\n" +
		"BEGIN:VCALENDAR\r\nVERSION:2.0\r\nPRODID:-//Microsoft Corporation//Outlook 16.0 MIMEDIR//EN\r\nMETHOD:PUBLISH\r\n" +
		event("berlin") + zone("+0100") + "END:VCALENDAR\r\n"

	events, _, err := ParseICSFile(ics)
	if err != nil {
		t.Fatalf("ParseICSFile() error = %v", err)
	}
	if len(events) != 2 {
		t.Fatalf("ParseICSFile() returned %d payloads, want 2", len(events))
	}
	for i, want := range []struct{ uid, offset, otherOffset string }{
		{"chicago", "-0500", "+0100"},
		{"berlin", "+0100", "-0500"},
	} {
		payload := events[i]
		if !strings.Contains(payload, "UID:"+want.uid+"\r\n") {
			t.Fatalf("payload %d is not %q's:\n%s", i, want.uid, payload)
		}
		if n := strings.Count(payload, "BEGIN:VTIMEZONE\r\n"); n != 1 {
			t.Errorf("%s payload defines %d zones, want 1:\n%s", want.uid, n, payload)
		}
		if !strings.Contains(payload, "TZOFFSETTO:"+want.offset+"\r\n") || strings.Contains(payload, want.otherOffset) {
			t.Errorf("%s payload does not carry its own calendar's definition (offset %s):\n%s", want.uid, want.offset, payload)
		}
	}
}

// Every component of one resource shares one definition of each zone, so a UID
// whose components come from several calendars takes each zone's definition
// from the calendar of its first component naming the zone.
func TestParseICSFileDefinesAZoneOnceForAUIDSpanningCalendars(t *testing.T) {
	zone := func(offset string) string {
		return "BEGIN:VTIMEZONE\r\nTZID:Customized Time Zone\r\nBEGIN:STANDARD\r\nDTSTART:16010101T000000\r\n" +
			"TZOFFSETFROM:" + offset + "\r\nTZOFFSETTO:" + offset + "\r\nEND:STANDARD\r\nEND:VTIMEZONE\r\n"
	}
	ics := "BEGIN:VCALENDAR\r\nVERSION:2.0\r\nPRODID:-//A//EN\r\n" + zone("-0500") +
		"BEGIN:VEVENT\r\nUID:series\r\nDTSTAMP:20260101T000000Z\r\nDTSTART;TZID=Customized Time Zone:20260105T090000\r\nRRULE:FREQ=DAILY;COUNT=3\r\nEND:VEVENT\r\n" +
		"END:VCALENDAR\r\n" +
		"BEGIN:VCALENDAR\r\nVERSION:2.0\r\nPRODID:-//B//EN\r\n" + zone("+0100") +
		"BEGIN:VEVENT\r\nUID:series\r\nDTSTAMP:20260101T000000Z\r\nRECURRENCE-ID;TZID=Customized Time Zone:20260106T090000\r\nDTSTART;TZID=Customized Time Zone:20260106T110000\r\nEND:VEVENT\r\n" +
		"END:VCALENDAR\r\n"

	events, _, err := ParseICSFile(ics)
	if err != nil {
		t.Fatalf("ParseICSFile() error = %v", err)
	}
	if len(events) != 1 {
		t.Fatalf("ParseICSFile() returned %d payloads, want 1", len(events))
	}
	if n := strings.Count(events[0], "BEGIN:VTIMEZONE\r\n"); n != 1 || !strings.Contains(events[0], "TZOFFSETTO:-0500\r\n") {
		t.Errorf("payload defines the zone %d times, want once as the master's calendar does:\n%s", n, events[0])
	}
}

// RFC 5545 §3.6.5 requires a VTIMEZONE's TZID. The DAV layer resolves a TZID
// through the first VTIMEZONE of the object declaring it, so a calendar
// defining a zone twice places its events by the first definition, and a
// definition that names no zone defines nothing, not even for an event whose
// TZID parameter is empty.
func TestParseICSFileKeepsAZonesFirstDefinitionAndNoNamelessOne(t *testing.T) {
	zone := func(tzidLine, offset string) string {
		return "BEGIN:VTIMEZONE\r\n" + tzidLine + "BEGIN:STANDARD\r\nDTSTART:16010101T000000\r\n" +
			"TZOFFSETFROM:" + offset + "\r\nTZOFFSETTO:" + offset + "\r\nEND:STANDARD\r\nEND:VTIMEZONE\r\n"
	}
	ics := "BEGIN:VCALENDAR\r\nVERSION:2.0\r\nPRODID:-//A//EN\r\n" +
		zone("", "+0300") + zone("TZID:Office\r\n", "-0500") + zone("TZID:Office\r\n", "+0100") + zone("", "+0400") +
		"BEGIN:VEVENT\r\nUID:office\r\nDTSTAMP:20260101T000000Z\r\nDTSTART;TZID=Office:20260105T090000\r\nEND:VEVENT\r\n" +
		"BEGIN:VEVENT\r\nUID:unnamed\r\nDTSTAMP:20260101T000000Z\r\nDTSTART;TZID=:20260105T090000\r\nEND:VEVENT\r\n" +
		"END:VCALENDAR\r\n"

	events, _, err := ParseICSFile(ics)
	if err != nil {
		t.Fatalf("ParseICSFile() error = %v", err)
	}
	if len(events) != 2 {
		t.Fatalf("ParseICSFile() returned %d payloads, want 2", len(events))
	}
	if n := strings.Count(events[0], "BEGIN:VTIMEZONE\r\n"); n != 1 || !strings.Contains(events[0], "TZOFFSETTO:-0500\r\n") {
		t.Errorf("office payload defines Office %d times, want once by its first definition:\n%s", n, events[0])
	}
	if strings.Contains(events[1], "VTIMEZONE") {
		t.Errorf("a definition naming no zone was attached:\n%s", events[1])
	}
}

// The importer reads the TZID a VTIMEZONE declares as the DAV layer reads it
// (ical.TimezoneComponents): its first TZID outside its observances, the
// property name in any case and with white space around it.
func TestImportReadsAZonesTZIDAsTheDAVLayerDoes(t *testing.T) {
	zones := make(calendarTimezones)
	zones.define([]string{"BEGIN:VTIMEZONE", "BEGIN:STANDARD", "TZID:Inner", "END:STANDARD", "tzid :Custom/Zone", "TZID:Later", "END:VTIMEZONE"})
	if _, ok := zones["Custom/Zone"]; !ok || len(zones) != 1 {
		t.Errorf("zones = %v, want the definition under Custom/Zone alone", zones)
	}
}

func mustBuildEvent(t *testing.T, uid, summary, dtstart, dtend string, allDay bool, location, description string, recurrence *RecurrenceOptions, opts *EventOptions) string {
	t.Helper()
	body, err := BuildEvent(uid, summary, dtstart, dtend, allDay, location, description, recurrence, opts)
	if err != nil {
		t.Fatalf("BuildEvent() error = %v", err)
	}
	return body
}

func mustBuildEventComponent(t *testing.T, uid, summary, dtstart, dtend string, allDay bool, location, description string, recurrence *RecurrenceOptions, recurrenceID string, opts *EventOptions) []string {
	t.Helper()
	lines, err := BuildEventComponent(uid, summary, dtstart, dtend, allDay, location, description, recurrence, recurrenceID, opts)
	if err != nil {
		t.Fatalf("BuildEventComponent() error = %v", err)
	}
	return lines
}

// A start, end or RECURRENCE-ID the builder cannot write in the shape allDay
// calls for fails the build rather than leaving the line out.
func TestBuildEventComponentRefusesValuesOfTheWrongShape(t *testing.T) {
	for _, tt := range []struct {
		dtstart, dtend, recurrenceID string
		allDay                       bool
	}{
		{"2026-03-20", "2026-03-25T10:00", "", true},
		{"2026-03-20T10:00", "2026-03-21", "", false},
		{"2026-03-20T10:00", "2026-03-20T11:00", "2026-03-20", false},
	} {
		if lines, err := BuildEventComponent("uid", "x", tt.dtstart, tt.dtend, tt.allDay, "", "", nil, tt.recurrenceID, nil); err == nil {
			t.Errorf("BuildEventComponent(%q, %q, %q, allDay=%v) = %q, want an error", tt.dtstart, tt.dtend, tt.recurrenceID, tt.allDay, lines)
		}
	}
}
