package dav

import (
	"testing"
	"time"

	"github.com/jw6ventures/calcard/internal/ical"
)

func outlookEasternEvent(extra ...string) string {
	properties := append([]string{"UID:outlook", "DTSTAMP:20240101T000000Z",
		`DTSTART;TZID="Eastern Standard Time":20240304T090000`, `DTEND;TZID="Eastern Standard Time":20240304T100000`}, extra...)
	return zeroLengthCalendar(outlookEasternTimezone, componentLines("VEVENT", properties...))
}

func utcInstant(t *testing.T, value string) time.Time {
	t.Helper()
	parsed, err := ical.ParseDateTime(value)
	if err != nil {
		t.Fatal(err)
	}
	return parsed
}

// The columns stored for an event in a zone only the object defines hold the
// instants its own VTIMEZONE names, the ones calendar-query matches it at, not
// its wall clocks read as UTC.
func TestStoredColumnsReadATZIDThroughTheObjectsOwnDefinition(t *testing.T) {
	raw := outlookEasternEvent()
	validated, fault := validateCalendarObjectForStorage(raw, nil)
	if fault != nil {
		t.Fatalf("refused: %+v", fault)
	}
	metadata := validated.Analysis.Metadata
	if want := utcInstant(t, "20240304T140000Z"); metadata.DTStart == nil || !metadata.DTStart.Equal(want) {
		t.Errorf("stored dtstart = %v, want %v", metadata.DTStart, want)
	}
	if want := utcInstant(t, "20240304T150000Z"); metadata.DTEnd == nil || !metadata.DTEnd.Equal(want) {
		t.Errorf("stored dtend = %v, want %v", metadata.DTEnd, want)
	}
	if !assertComponentInRange(t, raw, "VEVENT", "20240304T143000Z", "20240304T144500Z", floatingZone{}) {
		t.Error("calendar-query does not place the event where its columns do")
	}
	if assertComponentInRange(t, raw, "VEVENT", "20240304T090000Z", "20240304T100000Z", floatingZone{}) {
		t.Error("calendar-query places the event at its wall clock read as UTC")
	}
}

// A recurring event in such a zone stores the recurrence bounds of the
// instants its own definition places, daylight-saving time included.
func TestStoredRecurrenceBoundsReadATZIDThroughTheObjectsOwnDefinition(t *testing.T) {
	raw := outlookEasternEvent(`RRULE:FREQ=WEEKLY;UNTIL=20240318T130000Z`)
	validated, fault := validateCalendarObjectForStorage(raw, nil)
	if fault != nil {
		t.Fatalf("refused: %+v", fault)
	}
	metadata := validated.Analysis.Metadata
	first := utcInstant(t, "20240304T140000Z")
	if metadata.RecurrenceStart == nil || !metadata.RecurrenceStart.Equal(first) {
		t.Errorf("recurrence_start = %v, want the first instance at %v", metadata.RecurrenceStart, first)
	}
	// The last instance, 18 March in daylight time, starts at 13:00Z and
	// lasts an hour.
	last := utcInstant(t, "20240318T140000Z")
	if metadata.RecurrenceUntil == nil || metadata.RecurrenceUntil.Before(last) {
		t.Errorf("recurrence_until = %v, want no earlier than the last instance's end %v", metadata.RecurrenceUntil, last)
	}
	if !assertComponentInRange(t, raw, "VEVENT", "20240318T130000Z", "20240318T133000Z", floatingZone{}) {
		t.Error("calendar-query does not reach the last instance")
	}
}

// The end stored for an event with no DTEND, or one whose DTEND cannot end it,
// is the end the reports read it with, and none where a DURATION decides that
// end instead. PUT refuses every DTEND here that cannot end its event, but
// the store may hold them, and the analysis is what a write of the same object
// stores.
func TestStoredEndOfAnEventWhoseDTENDCannotEndIt(t *testing.T) {
	tests := []struct {
		name       string
		properties []string
		want       string
	}{
		{"DATE-TIME DTEND before DTSTART", []string{"DTSTART:20250301T100000Z", "DTEND:20250301T080000Z"}, "20250301T100000Z"},
		{"DATE DTEND before DTSTART", []string{"DTSTART;VALUE=DATE:20250301", "DTEND;VALUE=DATE:20250226"}, "20250302T000000Z"},
		{"unusable DTEND beside a DURATION", []string{"DTSTART:20250301T100000Z", "DTEND:20250301T080000Z", "DURATION:PT3H"}, ""},
		{"DATE DTEND equal beside a DURATION", []string{"DTSTART;VALUE=DATE:20250301", "DTEND;VALUE=DATE:20250301", "DURATION:P5D"}, ""},
		{"no DTEND on a DATE-TIME start", []string{"DTSTART:20250301T100000Z"}, "20250301T100000Z"},
		{"no DTEND on a DATE start", []string{"DTSTART;VALUE=DATE:20250301"}, "20250302T000000Z"},
		{"no DTEND beside a DURATION", []string{"DTSTART;VALUE=DATE:20250301", "DURATION:P5D"}, ""},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			raw := producerObject("VEVENT", append([]string{"UID:legacy", "DTSTAMP:20250101T000000Z"}, tt.properties...)...)
			analysis, err := analyzeICalendar(raw)
			if err != nil {
				t.Fatal(err)
			}
			got := analysis.Metadata.DTEnd
			if tt.want == "" {
				if got != nil {
					t.Errorf("stored dtend = %v, want none", got)
				}
				return
			}
			if want := utcInstant(t, tt.want); got == nil || !got.Equal(want) {
				t.Errorf("stored dtend = %v, want %v", got, want)
			}
		})
	}
}

// Every reader of an RRULE part in the DAV layer reads it as ical.RRuleParam
// does: lowercase names and white space around the "=" and the ";" read the
// same, a part without "=" names nothing, and the first of a repeated part
// answers.
func TestDAVReadersOfRRuleParts(t *testing.T) {
	maxDate := time.Date(2100, 12, 31, 23, 59, 59, 0, time.UTC)
	for _, tt := range []struct {
		rrule   string
		counted bool
		count   int
		end     time.Time
	}{
		{"freq=daily ; count = 3 ; until = 20260301T000000Z", true, 3, time.Date(2026, 3, 1, 0, 0, 0, 0, time.UTC)},
		{"FREQ=DAILY;COUNT;UNTIL=20260301", false, 0, time.Date(2026, 3, 1, 0, 0, 0, 0, time.UTC)},
		{"FREQ=DAILY;UNTIL=20260301;UNTIL=20270301", false, 0, time.Date(2026, 3, 1, 0, 0, 0, 0, time.UTC)},
		{"FREQ=DAILY", false, 0, maxDate},
	} {
		if got := countedRule(tt.rrule); got != tt.counted {
			t.Errorf("countedRule(%q) = %v, want %v", tt.rrule, got, tt.counted)
		}
		if got, ok := rruleCount(tt.rrule); ok != tt.counted || got != tt.count {
			t.Errorf("rruleCount(%q) = %d, %v; want %d", tt.rrule, got, ok, tt.count)
		}
		if got := recurrenceSetEnd(tt.rrule, maxDate, maxDate); !got.Equal(tt.end) {
			t.Errorf("recurrenceSetEnd(%q) = %v, want %v", tt.rrule, got, tt.end)
		}
	}
}
