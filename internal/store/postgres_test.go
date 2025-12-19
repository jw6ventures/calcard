package store

import (
	"testing"
	"time"
)

func TestParseICalFields(t *testing.T) {
	ical := `BEGIN:VCALENDAR
BEGIN:VEVENT
SUMMARY:Test Event
DTSTART:20231225
DTEND:20231226
END:VEVENT
END:VCALENDAR`

	summary, dtstart, dtend, allDay := parseICalFields(ical)

	if summary == nil || *summary != "Test Event" {
		t.Errorf("expected summary 'Test Event', got %v", summary)
	}
	if dtstart == nil {
		t.Fatal("expected dtstart to be set")
	}
	if dtstart.Year() != 2023 || dtstart.Month() != 12 || dtstart.Day() != 25 {
		t.Errorf("unexpected dtstart: %v", dtstart)
	}
	if !allDay {
		t.Error("expected allDay to be true for YYYYMMDD format")
	}
	if dtend == nil || dtend.Day() != 26 {
		t.Errorf("unexpected dtend: %v", dtend)
	}
}

func TestParseICalFieldsWithTime(t *testing.T) {
	ical := `BEGIN:VCALENDAR
BEGIN:VEVENT
SUMMARY:Meeting
DTSTART:20231225T140000Z
DTEND:20231225T150000Z
END:VEVENT
END:VCALENDAR`

	summary, dtstart, dtend, allDay := parseICalFields(ical)

	if summary == nil || *summary != "Meeting" {
		t.Errorf("expected summary 'Meeting', got %v", summary)
	}
	if dtstart == nil {
		t.Fatal("expected dtstart to be set")
	}
	if dtstart.Hour() != 14 || dtstart.Minute() != 0 {
		t.Errorf("unexpected dtstart time: %v", dtstart)
	}
	if allDay {
		t.Error("expected allDay to be false for datetime format")
	}
	if dtend == nil || dtend.Hour() != 15 {
		t.Errorf("unexpected dtend: %v", dtend)
	}
}

func TestParseICalFieldsWithTZID(t *testing.T) {
	ical := `BEGIN:VCALENDAR
BEGIN:VEVENT
SUMMARY:Meeting East
DTSTART;TZID=America/New_York:20240201T120000
DTEND;TZID=America/New_York:20240201T130000
END:VEVENT
END:VCALENDAR`

	summary, dtstart, dtend, allDay := parseICalFields(ical)

	if summary == nil || *summary != "Meeting East" {
		t.Errorf("expected summary 'Meeting East', got %v", summary)
	}
	if dtstart == nil || dtend == nil {
		t.Fatalf("expected both dtstart and dtend to be set")
	}
	if allDay {
		t.Fatal("expected allDay to be false for TZID datetime")
	}

	if got := dtstart.In(time.UTC); got.Hour() != 17 || got.Minute() != 0 {
		t.Errorf("expected dtstart 17:00 UTC, got %v", got)
	}
	if got := dtend.In(time.UTC); got.Hour() != 18 || got.Minute() != 0 {
		t.Errorf("expected dtend 18:00 UTC, got %v", got)
	}
}

func TestParseICalFieldsWithOffset(t *testing.T) {
	ical := `BEGIN:VCALENDAR
BEGIN:VEVENT
SUMMARY:Offset Event
DTSTART:20240201T120000-0500
DTEND:20240201T123000-0500
END:VEVENT
END:VCALENDAR`

	summary, dtstart, dtend, allDay := parseICalFields(ical)

	if summary == nil || *summary != "Offset Event" {
		t.Errorf("expected summary 'Offset Event', got %v", summary)
	}
	if dtstart == nil || dtend == nil {
		t.Fatalf("expected both dtstart and dtend to be set")
	}
	if allDay {
		t.Fatal("expected allDay to be false for offset datetime")
	}

	if got := dtstart.UTC(); got.Hour() != 17 || got.Minute() != 0 {
		t.Errorf("expected dtstart 17:00 UTC, got %v", got)
	}
	if got := dtend.UTC(); got.Hour() != 17 || got.Minute() != 30 {
		t.Errorf("expected dtend 17:30 UTC, got %v", got)
	}
}

func TestParseICalFieldsWithFoldedLines(t *testing.T) {
	// Lines are folded by CRLF followed by space - the unfold regex handles this
	ical := "BEGIN:VCALENDAR\r\nBEGIN:VEVENT\r\nSUMMARY:Long summary\r\nDTSTART:20231225\r\nEND:VEVENT\r\nEND:VCALENDAR"

	summary, _, _, _ := parseICalFields(ical)

	expected := "Long summary"
	if summary == nil || *summary != expected {
		t.Errorf("expected summary %q, got %v", expected, summary)
	}
}

func TestParseVCardFields(t *testing.T) {
	vcard := `BEGIN:VCARD
VERSION:3.0
FN:John Doe
EMAIL:john@example.com
END:VCARD`

	displayName, primaryEmail, birthday := parseVCardFields(vcard)

	if displayName == nil || *displayName != "John Doe" {
		t.Errorf("expected displayName 'John Doe', got %v", displayName)
	}
	if primaryEmail == nil || *primaryEmail != "john@example.com" {
		t.Errorf("expected primaryEmail 'john@example.com', got %v", primaryEmail)
	}
	if birthday != nil {
		t.Errorf("expected birthday to be nil, got %v", birthday)
	}
}

func TestParseVCardFieldsMultipleEmails(t *testing.T) {
	vcard := `BEGIN:VCARD
VERSION:3.0
FN:Jane Smith
EMAIL;TYPE=work:jane.work@example.com
EMAIL;TYPE=home:jane.home@example.com
END:VCARD`

	displayName, primaryEmail, _ := parseVCardFields(vcard)

	if displayName == nil || *displayName != "Jane Smith" {
		t.Errorf("expected displayName 'Jane Smith', got %v", displayName)
	}
	// Should return the first email
	if primaryEmail == nil || *primaryEmail != "jane.work@example.com" {
		t.Errorf("expected primaryEmail 'jane.work@example.com', got %v", primaryEmail)
	}
}

func TestParseVCardFieldsEscapedCharacters(t *testing.T) {
	vcard := `BEGIN:VCARD
VERSION:3.0
FN:John\, Jr. Doe
END:VCARD`

	displayName, _, _ := parseVCardFields(vcard)

	if displayName == nil || *displayName != "John, Jr. Doe" {
		t.Errorf("expected displayName 'John, Jr. Doe', got %v", displayName)
	}
}

func TestParseVCardFieldsWithBirthday(t *testing.T) {
	testCases := []struct {
		name      string
		vcard     string
		wantYear  int
		wantMonth int
		wantDay   int
	}{
		{
			name: "YYYY-MM-DD format",
			vcard: `BEGIN:VCARD
VERSION:3.0
FN:John Doe
BDAY:1990-05-15
END:VCARD`,
			wantYear: 1990, wantMonth: 5, wantDay: 15,
		},
		{
			name: "YYYYMMDD format",
			vcard: `BEGIN:VCARD
VERSION:3.0
FN:Jane Doe
BDAY:19850720
END:VCARD`,
			wantYear: 1985, wantMonth: 7, wantDay: 20,
		},
		{
			name: "--MM-DD format (no year)",
			vcard: `BEGIN:VCARD
VERSION:3.0
FN:Bob Smith
BDAY:--03-10
END:VCARD`,
			wantYear: 1, wantMonth: 3, wantDay: 10,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			_, _, birthday := parseVCardFields(tc.vcard)

			if birthday == nil {
				t.Fatal("expected birthday to be set")
			}
			if birthday.Year() != tc.wantYear {
				t.Errorf("expected year %d, got %d", tc.wantYear, birthday.Year())
			}
			if int(birthday.Month()) != tc.wantMonth {
				t.Errorf("expected month %d, got %d", tc.wantMonth, birthday.Month())
			}
			if birthday.Day() != tc.wantDay {
				t.Errorf("expected day %d, got %d", tc.wantDay, birthday.Day())
			}
		})
	}
}
