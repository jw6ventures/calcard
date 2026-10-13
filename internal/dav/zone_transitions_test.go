package dav

import (
	"testing"
	"time"

	"github.com/jw6ventures/calcard/internal/ical"
)

// clientVTimezones are definitions in the shape clients ship, stating each
// zone's present rules from 1970.
var clientVTimezones = map[string]string{
	"America/New_York": buildComponent("VTIMEZONE", "TZID:America/New_York",
		buildComponent("DAYLIGHT", "TZOFFSETFROM:-0500", "TZOFFSETTO:-0400", "DTSTART:19700308T020000", "RRULE:FREQ=YEARLY;BYMONTH=3;BYDAY=2SU"),
		buildComponent("STANDARD", "TZOFFSETFROM:-0400", "TZOFFSETTO:-0500", "DTSTART:19701101T020000", "RRULE:FREQ=YEARLY;BYMONTH=11;BYDAY=1SU")),
	"Europe/Berlin": buildComponent("VTIMEZONE", "TZID:Europe/Berlin",
		buildComponent("DAYLIGHT", "TZOFFSETFROM:+0100", "TZOFFSETTO:+0200", "DTSTART:19700329T020000", "RRULE:FREQ=YEARLY;BYMONTH=3;BYDAY=-1SU"),
		buildComponent("STANDARD", "TZOFFSETFROM:+0200", "TZOFFSETTO:+0100", "DTSTART:19701025T030000", "RRULE:FREQ=YEARLY;BYMONTH=10;BYDAY=-1SU")),
	"Europe/Dublin": buildComponent("VTIMEZONE", "TZID:Europe/Dublin",
		buildComponent("DAYLIGHT", "TZOFFSETFROM:+0100", "TZOFFSETTO:+0000", "DTSTART:19701025T020000", "RRULE:FREQ=YEARLY;BYMONTH=10;BYDAY=-1SU"),
		buildComponent("STANDARD", "TZOFFSETFROM:+0000", "TZOFFSETTO:+0100", "DTSTART:19700329T010000", "RRULE:FREQ=YEARLY;BYMONTH=3;BYDAY=-1SU")),
	"Australia/Lord_Howe": buildComponent("VTIMEZONE", "TZID:Australia/Lord_Howe",
		buildComponent("STANDARD", "TZOFFSETFROM:+1100", "TZOFFSETTO:+1030", "DTSTART:19700405T020000", "RRULE:FREQ=YEARLY;BYMONTH=4;BYDAY=1SU"),
		buildComponent("DAYLIGHT", "TZOFFSETFROM:+1030", "TZOFFSETTO:+1100", "DTSTART:19701004T020000", "RRULE:FREQ=YEARLY;BYMONTH=10;BYDAY=1SU")),
}

// RFC 5545 §3.3.5: a wall clock a transition skips is read with the offset in
// force before the gap, and one it repeats is its first occurrence, likewise
// the offset before the transition. Every way the server reads a TZID agrees:
// the host's zone for an object that defines none, the definition the
// validator stores for it, and the definition a client ships.
func TestEveryZoneReadingFollowsRFC5545AtTransitions(t *testing.T) {
	readings := []struct {
		tzid, wall, want string
	}{
		{"America/New_York", "20260308T023000", "20260308T073000Z"},    // skipped
		{"America/New_York", "20260308T030000", "20260308T070000Z"},    //
		{"America/New_York", "20261101T013000", "20261101T053000Z"},    // repeated
		{"America/New_York", "20261101T020000", "20261101T070000Z"},    //
		{"Europe/Berlin", "20260329T023000", "20260329T013000Z"},       // skipped
		{"Europe/Berlin", "20260329T030000", "20260329T010000Z"},       //
		{"Europe/Berlin", "20261025T023000", "20261025T003000Z"},       // repeated
		{"Europe/Berlin", "20261025T030000", "20261025T020000Z"},       //
		{"Europe/Dublin", "20260329T013000", "20260329T013000Z"},       // skipped
		{"Europe/Dublin", "20260329T020000", "20260329T010000Z"},       //
		{"Europe/Dublin", "20261025T013000", "20261025T003000Z"},       // repeated
		{"Europe/Dublin", "20261025T020000", "20261025T020000Z"},       //
		{"Australia/Lord_Howe", "20261004T021500", "20261003T154500Z"}, // skipped
		{"Australia/Lord_Howe", "20261004T023000", "20261003T153000Z"}, //
		{"Australia/Lord_Howe", "20260405T014500", "20260404T144500Z"}, // repeated
		{"Australia/Lord_Howe", "20260405T020000", "20260404T153000Z"}, //
	}
	for _, tt := range readings {
		t.Run(tt.tzid+" "+tt.wall, func(t *testing.T) {
			requireHostZones(t, tt.tzid)
			want, err := ical.ParseDateTime(tt.want)
			if err != nil {
				t.Fatal(err)
			}
			event := buildComponent("VEVENT", "UID:e", "DTSTART;TZID="+tt.tzid+":"+tt.wall)
			stored, fault := validateCalendarObjectForStorage(buildCalendarObject(event), nil)
			if fault != nil {
				t.Fatalf("refused: %+v", fault)
			}
			for name, raw := range map[string]string{
				"host zone":         buildCalendarObject(event),
				"stored definition": stored.Body,
				"client definition": buildCalendarObject(clientVTimezones[tt.tzid], event),
			} {
				root, err := parseICalendarObject(raw)
				if err != nil {
					t.Fatalf("%s: %v", name, err)
				}
				m := newCalendarTimeRangeMatcher(raw, root, floatingZone{})
				got, ok := m.resolveInstant(tt.tzid, tt.wall, icalFloatingDateTime)
				if !ok || !got.Equal(want) {
					t.Errorf("%s: %s read as %s, want %s", name, tt.wall, got, want)
				}
				if instants, ok := icalInstants(root, icalProperty{keyPart: "DTSTART;TZID=" + tt.tzid, parameters: map[string]string{"TZID": tt.tzid}}, tt.wall); !ok || len(instants) != 1 || !instants[0].Equal(want) {
					t.Errorf("%s: validator reads %s as %v, want %s", name, tt.wall, instants, want)
				}
			}
		})
	}
}

// A collection's CALDAV:calendar-timezone reads floating values the same way.
func TestFloatingValuesFollowRFC5545AtTransitions(t *testing.T) {
	requireHostZones(t, "Europe/Berlin")
	for name, zone := range map[string]floatingZone{
		"host zone":         newFloatingZone(buildCalendarObject(buildComponent("VTIMEZONE", "TZID:Europe/Berlin"))),
		"client definition": newFloatingZone(buildCalendarObject(clientVTimezones["Europe/Berlin"])),
	} {
		for wall, want := range map[string]time.Time{
			"20260329T023000": time.Date(2026, 3, 29, 1, 30, 0, 0, time.UTC),
			"20261025T023000": time.Date(2026, 10, 25, 0, 30, 0, 0, time.UTC),
		} {
			if got, ok := zone.resolve(wall); !ok || !got.Equal(want) {
				t.Errorf("%s: %s read as %s, want %s", name, wall, got, want)
			}
		}
	}
}

// The validator admits a TZOFFSETFROM or TZOFFSETTO exactly when the observance
// reader can parse it, so a VTIMEZONE that is stored is one the reader follows
// rather than one an object silently falls back to the host's zone over. The
// reader also reads past whitespace around a stored value; the validator holds
// a submitted one to the RFC 5545 §3.3.14 grammar, which has none.
func TestTimezoneOffsetValidatorAdmitsWhatTheReaderParses(t *testing.T) {
	for _, value := range []string{
		"+0000", "-0001", "+2359", "-235959", "+000001", "-000001", "+0530", "-013015",
		"-0000", "-000000", "+2400", "+0060", "+000060", "0100", "+01:00", "+01000", "+0100000",
		"+1", "", "++0100", "+01a0", "+０１００", "x0100",
	} {
		_, parsed := ical.ParseUTCOffset(value)
		if got := validUTCOffset(value); got != parsed {
			t.Errorf("validUTCOffset(%q) = %v, but the reader parses it: %v", value, got, parsed)
		}
	}
	for _, value := range []string{" +0100", "+0100 ", "+0100\t", " +0100"} {
		if _, parsed := ical.ParseUTCOffset(value); !parsed {
			t.Errorf("reader refuses stored %q", value)
		}
		if validUTCOffset(value) {
			t.Errorf("validUTCOffset(%q) admits whitespace the grammar does not", value)
		}
	}
}
