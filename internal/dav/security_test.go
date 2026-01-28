package dav

import (
	"strings"
	"testing"
)

// Tests for vCard UID extraction with line folding (RFC 6350)
func TestExtractUIDFromVCard_SimpleLine(t *testing.T) {
	vcard := "BEGIN:VCARD\nVERSION:3.0\nUID:12345\nFN:John Doe\nEND:VCARD"
	uid, err := extractUIDFromVCard(vcard)
	if err != nil {
		t.Fatalf("expected no error, got: %v", err)
	}
	if uid != "12345" {
		t.Errorf("expected UID '12345', got: %s", uid)
	}
}

func TestExtractUIDFromVCard_FoldedLine(t *testing.T) {
	vcard := "BEGIN:VCARD\nVERSION:3.0\nUID:very-long-unique-identifi\n er-that-spans-multiple-lines\nFN:John Doe\nEND:VCARD"
	uid, err := extractUIDFromVCard(vcard)
	if err != nil {
		t.Fatalf("expected no error for folded line, got: %v", err)
	}
	expected := "very-long-unique-identifier-that-spans-multiple-lines"
	if uid != expected {
		t.Errorf("expected UID '%s', got: '%s'", expected, uid)
	}
}

func TestExtractUIDFromVCard_WithParameters(t *testing.T) {
	// UID can have parameters before the colon
	vcard := "BEGIN:VCARD\nVERSION:3.0\nUID;VALUE=TEXT:test-uid-123\nFN:Jane Doe\nEND:VCARD"
	uid, err := extractUIDFromVCard(vcard)
	if err != nil {
		t.Fatalf("expected no error, got: %v", err)
	}
	if uid != "test-uid-123" {
		t.Errorf("expected UID 'test-uid-123', got: %s", uid)
	}
}

func TestExtractUIDFromVCard_Empty(t *testing.T) {
	vcard := "BEGIN:VCARD\nVERSION:3.0\nUID:\nFN:John Doe\nEND:VCARD"
	_, err := extractUIDFromVCard(vcard)
	if err == nil {
		t.Error("expected error for empty UID")
	}
	if !strings.Contains(err.Error(), "empty") {
		t.Errorf("expected 'empty' in error message, got: %v", err)
	}
}

func TestExtractUIDFromVCard_Missing(t *testing.T) {
	vcard := "BEGIN:VCARD\nVERSION:3.0\nFN:John Doe\nEND:VCARD"
	_, err := extractUIDFromVCard(vcard)
	if err == nil {
		t.Error("expected error for missing UID")
	}
	if !strings.Contains(err.Error(), "UID property") {
		t.Errorf("expected 'UID property' in error message, got: %v", err)
	}
}

// Tests for calendar slug validation
func TestIsValidCalendarSlug(t *testing.T) {
	tests := []struct {
		slug  string
		valid bool
	}{
		// Valid slugs
		{"test", true},
		{"my-calendar", true},
		{"cal123", true},
		{"a", true},
		{"test-123-calendar", true},
		{"abc123def456", true},
		{strings.Repeat("a", 64), true}, // Max length

		// Invalid slugs
		{"", false},                      // Empty
		{"-test", false},                 // Starts with hyphen
		{"test-", false},                 // Ends with hyphen
		{"Test", false},                  // Uppercase
		{"test_calendar", false},         // Underscore
		{"test calendar", false},         // Space
		{"test.calendar", false},         // Period
		{"test/calendar", false},         // Slash
		{"../../../etc", false},          // Path traversal
		{"test<script>", false},          // XML injection attempt
		{strings.Repeat("a", 65), false}, // Too long
		{"test@calendar", false},         // Special character
		{"CALENDAR", false},              // All uppercase
		{"123", true},                    // All numeric is technically valid
	}

	for _, tt := range tests {
		t.Run(tt.slug, func(t *testing.T) {
			result := isValidCalendarSlug(tt.slug)
			if result != tt.valid {
				t.Errorf("isValidCalendarSlug(%q) = %v, want %v", tt.slug, result, tt.valid)
			}
		})
	}
}

// Tests for multi-UID extraction
func TestExtractUIDFromICalendar_SingleComponent(t *testing.T) {
	ical := `BEGIN:VCALENDAR
VERSION:2.0
PRODID:-//Test//EN
BEGIN:VEVENT
UID:event-123
DTSTART:20240101T120000Z
SUMMARY:Test Event
END:VEVENT
END:VCALENDAR`

	uid, err := extractUIDFromICalendar(ical)
	if err != nil {
		t.Fatalf("expected no error, got: %v", err)
	}
	if uid != "event-123" {
		t.Errorf("expected UID 'event-123', got: %s", uid)
	}
}

func TestExtractUIDFromICalendar_MultipleComponentsSameUID(t *testing.T) {
	// Multiple components with same UID and RECURRENCE-ID (valid per RFC 5545)
	ical := `BEGIN:VCALENDAR
VERSION:2.0
BEGIN:VEVENT
UID:recurring-event
DTSTART:20240101T120000Z
SUMMARY:Master Event
END:VEVENT
BEGIN:VEVENT
UID:recurring-event
RECURRENCE-ID:20240108T120000Z
DTSTART:20240108T130000Z
SUMMARY:Exception
END:VEVENT
END:VCALENDAR`

	uid, err := extractUIDFromICalendar(ical)
	if err != nil {
		t.Fatalf("expected no error for same UID with recurrence, got: %v", err)
	}
	if uid != "recurring-event" {
		t.Errorf("expected UID 'recurring-event', got: %s", uid)
	}
}

func TestExtractUIDFromICalendar_MultipleComponentsDifferentUIDs(t *testing.T) {
	// Different component types can have different UIDs
	ical := `BEGIN:VCALENDAR
VERSION:2.0
BEGIN:VEVENT
UID:event-uid
DTSTART:20240101T120000Z
SUMMARY:Event
END:VEVENT
BEGIN:VTODO
UID:todo-uid
SUMMARY:Task
END:VTODO
END:VCALENDAR`

	// Should return UID from first component
	uid, err := extractUIDFromICalendar(ical)
	if err != nil {
		t.Fatalf("expected no error, got: %v", err)
	}
	if uid != "event-uid" {
		t.Errorf("expected UID from first component 'event-uid', got: %s", uid)
	}
}

func TestExtractUIDFromICalendar_NoUID(t *testing.T) {
	ical := `BEGIN:VCALENDAR
VERSION:2.0
BEGIN:VEVENT
DTSTART:20240101T120000Z
SUMMARY:Event without UID
END:VEVENT
END:VCALENDAR`

	_, err := extractUIDFromICalendar(ical)
	if err == nil {
		t.Error("expected error for missing UID")
	}
}

func TestExtractUIDFromICalendar_EmptyUID(t *testing.T) {
	ical := `BEGIN:VCALENDAR
VERSION:2.0
BEGIN:VEVENT
UID:
DTSTART:20240101T120000Z
SUMMARY:Event
END:VEVENT
END:VCALENDAR`

	_, err := extractUIDFromICalendar(ical)
	if err == nil {
		t.Error("expected error for empty UID")
	}
}

func TestExtractUIDFromICalendar_FoldedUID(t *testing.T) {
	// Test line folding in UID
	ical := `BEGIN:VCALENDAR
VERSION:2.0
BEGIN:VEVENT
UID:very-long-unique-identif
 ier-folded-line
DTSTART:20240101T120000Z
SUMMARY:Event
END:VEVENT
END:VCALENDAR`

	uid, err := extractUIDFromICalendar(ical)
	if err != nil {
		t.Fatalf("expected no error for folded UID, got: %v", err)
	}
	expected := "very-long-unique-identifier-folded-line"
	if uid != expected {
		t.Errorf("expected UID '%s', got: '%s'", expected, uid)
	}
}

func TestExtractUIDFromICalendar_UIDWithParameters(t *testing.T) {
	// UID can have parameters
	ical := `BEGIN:VCALENDAR
VERSION:2.0
BEGIN:VEVENT
UID;VALUE=TEXT:event-with-params
DTSTART:20240101T120000Z
SUMMARY:Event
END:VEVENT
END:VCALENDAR`

	uid, err := extractUIDFromICalendar(ical)
	if err != nil {
		t.Fatalf("expected no error, got: %v", err)
	}
	if uid != "event-with-params" {
		t.Errorf("expected UID 'event-with-params', got: %s", uid)
	}
}
