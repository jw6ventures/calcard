package utils

import (
	"net/http"
	"net/url"
	"strings"
	"testing"
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
			got := ParseRecurrenceOptions(req)

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
		})
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
			got, err := FormatICalDateTime(tt.value, tt.allDay, tt.exclusiveEnd, tt.prop)

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

	ical := BuildEvent(uid, summary, dtstart, dtend, true, location, description, nil)

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

	ical := BuildEvent(uid, "Weekly Meeting", "2025-01-15T14:00", "2025-01-15T15:00", false, "", "", recurrence)

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

	events := ParseICSFile(icsContent)

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

func TestExtractUID(t *testing.T) {
	tests := []struct {
		name  string
		ical  string
		want  string
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
