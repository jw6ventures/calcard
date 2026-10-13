package dav

import (
	"strings"
	"testing"

	"github.com/jw6ventures/calcard/internal/ical"
	"github.com/jw6ventures/calcard/internal/store"
)

// legacyNewYork2025 is the definition the v1.1 editor generated for a series
// created in America/New_York during 2025: the host's transitions in that year
// alone, after an observance restating on 1 January the offset kept then, and
// no ical.HostDefinitionProperty.
const legacyNewYork2025 = "BEGIN:VTIMEZONE\r\nTZID:America/New_York\r\n" +
	"BEGIN:STANDARD\r\nDTSTART:20250101T000000\r\nTZOFFSETFROM:-0500\r\nTZOFFSETTO:-0500\r\nTZNAME:EST\r\nEND:STANDARD\r\n" +
	"BEGIN:DAYLIGHT\r\nDTSTART:20250309T020000\r\nTZOFFSETFROM:-0500\r\nTZOFFSETTO:-0400\r\nTZNAME:EDT\r\nEND:DAYLIGHT\r\n" +
	"BEGIN:STANDARD\r\nDTSTART:20251102T020000\r\nTZOFFSETFROM:-0400\r\nTZOFFSETTO:-0500\r\nTZNAME:EST\r\nEND:STANDARD\r\n" +
	"END:VTIMEZONE\r\n"

// legacyNewYorkSeries is a weekly series created in the v1.1 editor during
// 2025, at 10:00 in America/New_York, stored with its definition of that year.
func legacyNewYorkSeries(start string) string {
	return "BEGIN:VCALENDAR\r\nVERSION:2.0\r\nPRODID:-//CalCard//EN\r\n" + legacyNewYork2025 +
		"BEGIN:VEVENT\r\nUID:legacy\r\nDTSTAMP:20250701T000000Z\r\nSUMMARY:Weekly\r\n" +
		"DTSTART;TZID=America/New_York:" + start + "T100000\r\nDTEND;TZID=America/New_York:" + start + "T110000\r\n" +
		"RRULE:FREQ=WEEKLY\r\nEND:VEVENT\r\nEND:VCALENDAR\r\n"
}

// The v1.1 editor's definitions carry no marker but speak for the host's
// database as a marked one does: calendar-query places the series' summer 2026
// instances in daylight time, as the host's zone does, not in the standard
// time the 2025 definition ends in.
func TestCalendarQueryReadsAV11DefinitionAsTheHostZone(t *testing.T) {
	requireHostZones(t, "America/New_York")
	event := store.Event{UID: "legacy", RawICAL: legacyNewYorkSeries("20250706")}
	if !filterMatches(t, event, calQueryWithTimeRange("20260705T140000Z", "20260705T143000Z"), floatingZone{}) {
		t.Error("calendar-query missed the 2026-07-05 10:00 EDT instance")
	}
	if filterMatches(t, event, calQueryWithTimeRange("20260705T150000Z", "20260705T153000Z"), floatingZone{}) {
		t.Error("calendar-query placed the 2026-07-05 instance at 10:00 EST")
	}
}

// The columns stored for such an object hold the instants the host's zone
// names, and the definition is replaced by the host's, marked as the server's,
// when the object is written again.
func TestWriteReadsAndReplacesAV11Definition(t *testing.T) {
	requireHostZones(t, "America/New_York")
	validated, fault := validateCalendarObjectForStorage(legacyNewYorkSeries("20260705"), nil)
	if fault != nil {
		t.Fatalf("refused: %+v", fault)
	}
	metadata := validated.Analysis.Metadata
	if want := utcInstant(t, "20260705T140000Z"); metadata.DTStart == nil || !metadata.DTStart.Equal(want) {
		t.Errorf("stored dtstart = %v, want %v", metadata.DTStart, want)
	}
	if want := utcInstant(t, "20260705T150000Z"); metadata.DTEnd == nil || !metadata.DTEnd.Equal(want) {
		t.Errorf("stored dtend = %v, want %v", metadata.DTEnd, want)
	}
	if want := utcInstant(t, "20260705T140000Z"); metadata.RecurrenceStart == nil || !metadata.RecurrenceStart.Equal(want) {
		t.Errorf("stored recurrence_start = %v, want %v", metadata.RecurrenceStart, want)
	}
	if strings.Contains(validated.Body, legacyNewYork2025) || strings.Count(validated.Body, ical.HostDefinitionProperty) != 1 {
		t.Errorf("the v1.1 definition was not replaced by the host's:\n%s", validated.Body)
	}
}
