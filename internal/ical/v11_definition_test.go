package ical

import (
	"strings"
	"testing"
)

// legacyNewYork2025 is the definition the v1.1 editor generated for a series
// created in America/New_York during 2025: the host's transitions in that year
// alone, after an observance restating on 1 January the offset kept then, and
// no HostDefinitionProperty.
const legacyNewYork2025 = "BEGIN:VTIMEZONE\r\nTZID:America/New_York\r\n" +
	"BEGIN:STANDARD\r\nDTSTART:20250101T000000\r\nTZOFFSETFROM:-0500\r\nTZOFFSETTO:-0500\r\nTZNAME:EST\r\nEND:STANDARD\r\n" +
	"BEGIN:DAYLIGHT\r\nDTSTART:20250309T020000\r\nTZOFFSETFROM:-0500\r\nTZOFFSETTO:-0400\r\nTZNAME:EDT\r\nEND:DAYLIGHT\r\n" +
	"BEGIN:STANDARD\r\nDTSTART:20251102T020000\r\nTZOFFSETFROM:-0400\r\nTZOFFSETTO:-0500\r\nTZNAME:EST\r\nEND:STANDARD\r\n" +
	"END:VTIMEZONE"

// The v1.1 editor wrote its definitions of the host's zones without
// HostDefinitionProperty, for the years an event was edited in alone. Such a
// definition speaks for the host's database as a marked one does, so a weekly
// series created during 2025 is read in summer 2026 in daylight time, as the
// host's zone reads it, not in the standard time the definition ends in.
func TestV11GeneratedDefinitionsReadAsTheHostZone(t *testing.T) {
	requireHostZone(t, "America/New_York")
	lines := objectWithTimezone(strings.Split(legacyNewYork2025, "\r\n")...)

	timezones := TimezoneComponents(lines)
	if len(timezones) != 1 || !timezones[0].HostDefinition {
		t.Fatalf("TimezoneComponents() = %+v, want the v1.1 definition read as the server's", timezones)
	}
	got, ok := NewObjectTimes(lines).ResolvePropertyTime("DTSTART;TZID=America/New_York", "20260705T100000")
	if want, _ := ParseDateTime("20260705T140000Z"); !ok || !got.Equal(want) {
		t.Errorf("2026-07-05 10:00 in America/New_York = %v (%v), want %v", got, ok, want)
	}

	// A zone the host does not define has no other definition than the
	// object's, whatever its shape.
	custom := objectWithTimezone(strings.Split(strings.ReplaceAll(legacyNewYork2025, "America/New_York", "Custom/Zone"), "\r\n")...)
	got, ok = NewObjectTimes(custom).ResolvePropertyTime("DTSTART;TZID=Custom/Zone", "20260705T100000")
	if want, _ := ParseDateTime("20260705T150000Z"); !ok || !got.Equal(want) {
		t.Errorf("2026-07-05 10:00 in Custom/Zone = %v (%v), want %v", got, ok, want)
	}
}
