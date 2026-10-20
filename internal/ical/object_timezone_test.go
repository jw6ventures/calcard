package ical

import (
	"testing"
	"time"
)

// outlookEasternObject defines Outlook's "Eastern Standard Time", a name the
// host's zone database lacks.
var outlookEasternObject = UnfoldLines("BEGIN:VCALENDAR\r\nVERSION:2.0\r\n" +
	"BEGIN:VTIMEZONE\r\nTZID:Eastern Standard Time\r\n" +
	"BEGIN:STANDARD\r\nDTSTART:16010101T020000\r\nTZOFFSETFROM:-0400\r\nTZOFFSETTO:-0500\r\nRRULE:FREQ=YEARLY;INTERVAL=1;BYDAY=1SU;BYMONTH=11\r\nEND:STANDARD\r\n" +
	"BEGIN:DAYLIGHT\r\nDTSTART:16010101T020000\r\nTZOFFSETFROM:-0500\r\nTZOFFSETTO:-0400\r\nRRULE:FREQ=YEARLY;INTERVAL=1;BYDAY=2SU;BYMONTH=3\r\nEND:DAYLIGHT\r\n" +
	"END:VTIMEZONE\r\n" +
	"BEGIN:VEVENT\r\nUID:x\r\nDTSTART;TZID=\"Eastern Standard Time\":20240304T090000\r\nEND:VEVENT\r\nEND:VCALENDAR\r\n")

// A TZID the host does not know resolves through the object's own VTIMEZONE,
// observing its daylight-saving rules, and one the object does not define
// through the host's zone of that name.
func TestResolveObjectTimezone(t *testing.T) {
	object := outlookEasternObject
	wall := func(value string) time.Time {
		parsed, err := ParseDateTime(value)
		if err != nil {
			t.Fatal(err)
		}
		return parsed
	}
	tests := []struct {
		name    string
		lines   []string
		tzid    string
		defined bool
		wall    string
		want    string
	}{
		{name: "object zone in standard time", lines: object, tzid: "Eastern Standard Time", defined: true, wall: "20240304T090000", want: "20240304T140000Z"},
		{name: "object zone in daylight time", lines: object, tzid: "Eastern Standard Time", defined: true, wall: "20240311T090000", want: "20240311T130000Z"},
		{name: "object zone in its gap", lines: object, tzid: "Eastern Standard Time", defined: true, wall: "20240310T023000", want: "20240310T073000Z"},
		{name: "host zone in its gap", tzid: "America/New_York", defined: true, wall: "20240310T023000", want: "20240310T073000Z"},
		{name: "zone defined by neither", lines: object, tzid: "Pacific Standard Time", defined: false, wall: "20240304T090000", want: "20240304T090000Z"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.lines == nil {
				if _, ok := HostLocation(tt.tzid); !ok {
					t.Skipf("%s is not available", tt.tzid)
				}
			}
			zone := ResolveObjectTimezone(tt.lines, tt.tzid)
			if zone.Defined() != tt.defined {
				t.Errorf("Defined() = %v, want %v", zone.Defined(), tt.defined)
			}
			if got := zone.Instant(wall(tt.wall)); !got.Equal(wall(tt.want)) {
				t.Errorf("Instant(%s) = %s, want %s", tt.wall, got.UTC().Format("20060102T150405Z"), tt.want)
			}
		})
	}
}

// The offsets the object's own definition keeps are listed with the instants
// its transitions happen at, as the definition reads them.
func TestObjectTimezoneOffsets(t *testing.T) {
	zone := ResolveObjectTimezone(outlookEasternObject, "Eastern Standard Time")
	from := time.Date(2024, time.January, 1, 0, 0, 0, 0, time.UTC)
	through := time.Date(2025, time.January, 1, 0, 0, 0, 0, time.UTC)
	got, ok := zone.Offsets(from, through)
	want := []ZoneOffset{
		{From: from, Offset: -5 * time.Hour},
		{From: time.Date(2024, time.March, 10, 7, 0, 0, 0, time.UTC), Offset: -4 * time.Hour},
		{From: time.Date(2024, time.November, 3, 6, 0, 0, 0, time.UTC), Offset: -5 * time.Hour},
	}
	if !ok || len(got) != len(want) {
		t.Fatalf("Offsets() = %v, %v, want %v", got, ok, want)
	}
	for i := range want {
		if !got[i].From.Equal(want[i].From) || got[i].Offset != want[i].Offset {
			t.Errorf("Offsets()[%d] = %v, want %v", i, got[i], want[i])
		}
	}
	for instant := from; instant.Before(through); instant = instant.Add(37 * time.Minute) {
		listed := got[0].Offset
		for _, offset := range got {
			if !offset.From.After(instant) {
				listed = offset.Offset
			}
		}
		if read, _ := zone.definition.observances.OffsetAtInstant(instant, nil); read != listed {
			t.Fatalf("at %s the listing gives %s, the definition %s", instant, listed, read)
		}
	}
	if _, ok := ResolveObjectTimezone(nil, "America/New_York").Offsets(from, through); ok {
		t.Error("Offsets() of a zone only the host defines should report it is not the object's")
	}
}

// TimezoneComponents lists every VTIMEZONE beneath the object's VCALENDAR, a
// later definition of a TZID included, with the TZID it declares, whether it
// carries HostDefinitionProperty, and the lines it spans, read with the
// structural recovery ReadComponents applies to a stored object.
func TestTimezoneComponents(t *testing.T) {
	lines := []string{
		"BEGIN:VCALENDAR", // 0
		"VERSION:2.0",
		"BEGIN:VTIMEZONE", // 2
		"TZID: Europe/Berlin ",
		HostDefinitionProperty + ":TRUE",
		"BEGIN:STANDARD", "DTSTART:19701025T030000", "TZOFFSETFROM:+0200", "TZOFFSETTO:+0100", "END:STANDARD",
		"END:VTIMEZONE",   // 10
		"begin:vtimezone", // 11
		"TZID :Europe/Berlin",
		"x-calcard-host-zone :TRUE",
		"END:VTIMEZONE", // 14
		"BEGIN:VEVENT",  // 15
		"UID:x",
		"BEGIN:VTIMEZONE", "TZID:Nested/Zone", "END:VTIMEZONE",
		"END:VEVENT",      // 20
		"BEGIN:VTIMEZONE", // 21: its observance is never closed
		"TZID:America/New_York",
		"BEGIN:DAYLIGHT", "DTSTART:19700308T020000", "TZOFFSETFROM:-0500", "TZOFFSETTO:-0400",
		"END:VTIMEZONE",   // 27
		"BEGIN:VTIMEZONE", // 28: no TZID
		"END:VTIMEZONE",
		"END:VCALENDAR",   // 30
		"BEGIN:VCALENDAR", // a second root continues the first
		"BEGIN:VTIMEZONE", // 32
		"TZID:Custom/Zone",
		"END:VTIMEZONE",
		"END:VCALENDAR",
	}
	want := []TimezoneComponent{
		{TZID: "Europe/Berlin", HostDefinition: true, Start: 2, End: 11},
		{TZID: "Europe/Berlin", HostDefinition: true, Start: 11, End: 15},
		{TZID: "America/New_York", Start: 21, End: 28},
		{Start: 28, End: 30},
		{TZID: "Custom/Zone", Start: 32, End: 35},
	}
	got := TimezoneComponents(lines)
	if len(got) != len(want) {
		t.Fatalf("TimezoneComponents = %+v, want %+v", got, want)
	}
	for i := range want {
		if got[i].TZID != want[i].TZID || got[i].HostDefinition != want[i].HostDefinition || got[i].Start != want[i].Start || got[i].End != want[i].End {
			t.Errorf("component %d = %+v, want %+v", i, got[i], want[i])
		}
	}

	// A TZID resolves through the first definition naming it.
	timezones := objectTimezones(lines)
	if berlin := timezones["Europe/Berlin"]; !berlin.hostGenerated || len(berlin.observances) != 1 {
		t.Errorf("Europe/Berlin resolves through %+v, want the first definition", berlin)
	}
	if _, ok := timezones["Nested/Zone"]; ok {
		t.Error("a VTIMEZONE inside a VEVENT defines a zone")
	}
	if _, ok := timezones[""]; ok {
		t.Error("a VTIMEZONE without a TZID defines a zone")
	}
}

// HostDefinitionProperty is read for its value, not merely its presence: only
// TRUE, trimmed of white space and matched without regard to case, marks a
// VTIMEZONE as the server's. FALSE, an empty value or any other spelling
// leaves it read as though it carried no such property.
func TestTimezoneComponentsOnlyTrueMarksTheServersDefinition(t *testing.T) {
	lines := []string{
		"BEGIN:VCALENDAR",
		"BEGIN:VTIMEZONE", "TZID:Asia/Tokyo", HostDefinitionProperty + ":FALSE",
		"BEGIN:STANDARD", "DTSTART:19700101T000000", "TZOFFSETFROM:+0900", "TZOFFSETTO:+0900", "END:STANDARD",
		"END:VTIMEZONE",
		"BEGIN:VTIMEZONE", "TZID:Asia/Seoul", HostDefinitionProperty + ":",
		"BEGIN:STANDARD", "DTSTART:19700101T000000", "TZOFFSETFROM:+0900", "TZOFFSETTO:+0900", "END:STANDARD",
		"END:VTIMEZONE",
		"BEGIN:VTIMEZONE", "TZID:Asia/Shanghai", HostDefinitionProperty + ": true ",
		"BEGIN:STANDARD", "DTSTART:19700101T000000", "TZOFFSETFROM:+0800", "TZOFFSETTO:+0800", "END:STANDARD",
		"END:VTIMEZONE",
		"END:VCALENDAR",
	}
	got := TimezoneComponents(lines)
	if len(got) != 3 {
		t.Fatalf("TimezoneComponents = %+v, want 3 components", got)
	}
	if got[0].HostDefinition {
		t.Errorf("FALSE marked %+v as the server's", got[0])
	}
	if got[1].HostDefinition {
		t.Errorf("an empty marker value marked %+v as the server's", got[1])
	}
	if !got[2].HostDefinition {
		t.Errorf("a trimmed, lower-cased TRUE did not mark %+v as the server's", got[2])
	}
}
