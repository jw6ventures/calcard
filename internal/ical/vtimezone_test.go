package ical

import (
	"os"
	"strings"
	"testing"
	"time"
)

func wallClock(year int, month time.Month, day int) time.Time {
	return time.Date(year, month, day, 9, 0, 0, 0, time.UTC)
}

// A zone on stable rules is stated as one yearly rule per observance, anchored
// at the transition in force when the first value falls, so the definition is
// a few lines long and holds however far a recurrence runs.
func TestVTimezoneStatesStableRulesAsYearlyRules(t *testing.T) {
	tests := map[string]string{
		"Europe/Berlin": "BEGIN:VTIMEZONE\r\nTZID:Europe/Berlin\r\nX-CALCARD-HOST-ZONE:TRUE\r\n" +
			"BEGIN:STANDARD\r\nDTSTART:20251026T030000\r\nTZOFFSETFROM:+0200\r\nTZOFFSETTO:+0100\r\nTZNAME:CET\r\n" +
			"RRULE:FREQ=YEARLY;BYMONTH=10;BYDAY=-1SU\r\nEND:STANDARD\r\n" +
			"BEGIN:DAYLIGHT\r\nDTSTART:20260329T020000\r\nTZOFFSETFROM:+0100\r\nTZOFFSETTO:+0200\r\nTZNAME:CEST\r\n" +
			"RRULE:FREQ=YEARLY;BYMONTH=3;BYDAY=-1SU\r\nEND:DAYLIGHT\r\n" +
			"END:VTIMEZONE\r\n",
		"America/New_York": "BEGIN:VTIMEZONE\r\nTZID:America/New_York\r\nX-CALCARD-HOST-ZONE:TRUE\r\n" +
			"BEGIN:STANDARD\r\nDTSTART:20251102T020000\r\nTZOFFSETFROM:-0400\r\nTZOFFSETTO:-0500\r\nTZNAME:EST\r\n" +
			"RRULE:FREQ=YEARLY;BYMONTH=11;BYDAY=1SU\r\nEND:STANDARD\r\n" +
			"BEGIN:DAYLIGHT\r\nDTSTART:20260308T020000\r\nTZOFFSETFROM:-0500\r\nTZOFFSETTO:-0400\r\nTZNAME:EDT\r\n" +
			"RRULE:FREQ=YEARLY;BYMONTH=3;BYDAY=2SU\r\nEND:DAYLIGHT\r\n" +
			"END:VTIMEZONE\r\n",
	}
	for tzid, want := range tests {
		t.Run(tzid, func(t *testing.T) {
			requireHostZone(t, tzid)
			got, ok := VTimezone(tzid, wallClock(2026, time.January, 5), wallClock(2026, time.January, 5))
			if !ok {
				t.Fatal("VTimezone() ok = false")
			}
			if got != want {
				t.Errorf("VTimezone() =\n%s\nwant\n%s", got, want)
			}
		})
	}
}

// Santiago changes clocks at 24:00 on the first Saturday, which is a Sunday
// between the 2nd and the 8th: no BYDAY ordinal names that day every year, so
// the rule bounds the Sunday by the days of the month it can fall on.
func TestVTimezoneBoundsAWeekdayByMonthDaysWhereNoOrdinalFits(t *testing.T) {
	requireHostZone(t, "America/Santiago")
	got, ok := VTimezone("America/Santiago", wallClock(2026, time.January, 5), wallClock(2026, time.January, 5))
	if !ok {
		t.Fatal("VTimezone() ok = false")
	}
	for _, want := range []string{
		"RRULE:FREQ=YEARLY;BYMONTH=4;BYMONTHDAY=2,3,4,5,6,7,8;BYDAY=SU\r\n",
		"RRULE:FREQ=YEARLY;BYMONTH=9;BYMONTHDAY=2,3,4,5,6,7,8;BYDAY=SU\r\n",
	} {
		if !strings.Contains(got, want) {
			t.Errorf("VTimezone() lacks %q:\n%s", want, got)
		}
	}
	if strings.Contains(got, "RDATE") {
		t.Errorf("VTimezone() lists transitions a rule states:\n%s", got)
	}
}

// A zone that no longer changes its clocks is the transition that set its
// present offset, and a zone that never has is that offset alone.
func TestVTimezoneStatesAFixedOffset(t *testing.T) {
	tests := map[string]string{
		"Asia/Kolkata": "BEGIN:VTIMEZONE\r\nTZID:Asia/Kolkata\r\nX-CALCARD-HOST-ZONE:TRUE\r\n" +
			"BEGIN:STANDARD\r\nDTSTART:19451015T000000\r\nTZOFFSETFROM:+0630\r\nTZOFFSETTO:+0530\r\nTZNAME:IST\r\nEND:STANDARD\r\n" +
			"END:VTIMEZONE\r\n",
		"Etc/GMT+5": "BEGIN:VTIMEZONE\r\nTZID:Etc/GMT+5\r\nX-CALCARD-HOST-ZONE:TRUE\r\n" +
			"BEGIN:STANDARD\r\nDTSTART:19700101T000000\r\nTZOFFSETFROM:-0500\r\nTZOFFSETTO:-0500\r\nTZNAME:-05\r\nEND:STANDARD\r\n" +
			"END:VTIMEZONE\r\n",
		"UTC": "BEGIN:VTIMEZONE\r\nTZID:UTC\r\nX-CALCARD-HOST-ZONE:TRUE\r\n" +
			"BEGIN:STANDARD\r\nDTSTART:19700101T000000\r\nTZOFFSETFROM:+0000\r\nTZOFFSETTO:+0000\r\nTZNAME:UTC\r\nEND:STANDARD\r\n" +
			"END:VTIMEZONE\r\n",
	}
	for tzid, want := range tests {
		t.Run(tzid, func(t *testing.T) {
			requireHostZone(t, tzid)
			got, ok := VTimezone(tzid, wallClock(2026, time.January, 5), wallClock(2100, time.December, 31))
			if !ok {
				t.Fatal("VTimezone() ok = false")
			}
			if got != want {
				t.Errorf("VTimezone() =\n%s\nwant\n%s", got, want)
			}
		})
	}
}

// Tehran kept daylight time from the March equinox until it abandoned it in
// 2022, on dates no yearly rule states, so its transitions are listed and the
// last of them is the one that left its present offset in force.
func TestVTimezoneListsTransitionsNoRuleStates(t *testing.T) {
	requireHostZone(t, "Asia/Tehran")
	got, ok := VTimezone("Asia/Tehran", wallClock(2010, time.January, 5), wallClock(2100, time.December, 31))
	if !ok {
		t.Fatal("VTimezone() ok = false")
	}
	if strings.Contains(got, "RRULE") || !strings.Contains(got, "RDATE:20220922T000000") || strings.Contains(got, ":2023") {
		t.Errorf("VTimezone() does not list the transitions through 2022 alone:\n%s", got)
	}
}

// Transitions no yearly rule describes, such as the dates Palestine is
// predicted to move its clocks on around Ramadan, are listed, and only through
// the year of the last value the definition has to place.
func TestVTimezoneListsIrregularTransitionsOnlyThroughTheSpan(t *testing.T) {
	requireHostZone(t, "Asia/Gaza")
	short, ok := VTimezone("Asia/Gaza", wallClock(2045, time.January, 5), wallClock(2045, time.December, 5))
	if !ok {
		t.Fatal("VTimezone() ok = false")
	}
	long, ok := VTimezone("Asia/Gaza", wallClock(2045, time.January, 5), wallClock(2100, time.December, 31))
	if !ok {
		t.Fatal("VTimezone() ok = false")
	}
	if !strings.Contains(short, "RDATE:2045") {
		t.Fatalf("VTimezone() lists no 2045 transitions:\n%s", short)
	}
	for _, line := range strings.Split(short, "\r\n") {
		if strings.HasPrefix(line, "RDATE:2047") || strings.HasPrefix(line, "DTSTART:2047") {
			t.Errorf("a definition needed through 2045 lists %s", line)
		}
	}
	if !strings.Contains(long, "RDATE:2080") {
		t.Errorf("a definition needed through 2100 lacks the 2080 transitions:\n%s", long)
	}
}

// Every onset of an offset the definition states, read the way a VTIMEZONE
// reader reads it, is a change of the host's offset, and every change of the
// host's offset over the span is one of its onsets.
func TestVTimezoneOnsetsAreTheHostTransitions(t *testing.T) {
	minDate, maxDate := DateLimits()
	spans := [][2]time.Time{
		{minDate, maxDate},
		{wallClock(2026, time.March, 1), maxDate},
		{wallClock(1987, time.June, 1), wallClock(2007, time.June, 1)},
	}
	for _, tzid := range []string{
		"Europe/Berlin", "America/New_York", "Australia/Sydney", "America/Santiago", "Asia/Kolkata",
		"Africa/Casablanca", "Pacific/Chatham", "Asia/Tehran", "Europe/Dublin", "Etc/GMT+5", "America/Nuuk",
		"Asia/Gaza", "Pacific/Apia", "Antarctica/Troll", "Australia/Lord_Howe",
	} {
		t.Run(tzid, func(t *testing.T) {
			loc := requireHostZone(t, tzid)
			for _, span := range spans {
				text, ok := VTimezone(tzid, span[0], span[1])
				if !ok {
					t.Fatalf("VTimezone(%s..%s) ok = false", span[0], span[1])
				}
				stated := statedOnsets(t, text, span[1].AddDate(1, 0, 0))
				from := time.Date(span[0].Year(), time.January, 1, 0, 0, 0, 0, time.UTC).AddDate(0, 0, -2)
				through := time.Date(span[1].Year()+1, time.January, 1, 0, 0, 0, 0, time.UTC)
				for _, transition := range hostTransitions(loc, from, through.AddDate(0, 0, 2)) {
					// A change of name or daylight saving status alone moves
					// no reading; the observances' names are checked apart.
					if transition.at.Before(from) || transition.at.After(through) || transition.from == transition.to {
						continue
					}
					got, found := stated[transition.onset()]
					if !found {
						t.Errorf("span from %d: host transition at %s is not stated", span[0].Year(), transition.at)
						continue
					}
					if got != [2]int{transition.from, transition.to} {
						t.Errorf("span from %d: onset %s states %v, host moves %d -> %d", span[0].Year(), transition.onset(), got, transition.from, transition.to)
					}
					delete(stated, transition.onset())
				}
				for onset := range stated {
					if onset.Add(-26*time.Hour).After(from) && onset.Before(through.Add(-26*time.Hour)) {
						t.Errorf("span from %d: stated onset %s is no host transition", span[0].Year(), onset)
					}
				}
			}
		})
	}
}

// statedOnsets lists the onsets each observance of a generated VTIMEZONE
// states through the given wall clock, with the offsets it moves between.
func statedOnsets(t *testing.T, text string, through time.Time) map[time.Time][2]int {
	t.Helper()
	onsets := make(map[time.Time][2]int)
	var dtstart time.Time
	var from, to int
	var rrule string
	var rdates []time.Time
	for _, line := range strings.Split(strings.TrimSuffix(text, "\r\n"), "\r\n") {
		name, value, _ := strings.Cut(line, ":")
		switch name {
		case "BEGIN":
			dtstart, rrule, rdates = time.Time{}, "", nil
		case "DTSTART":
			dtstart = mustParseDateTime(t, value)
		case "RDATE":
			rdates = append(rdates, mustParseDateTime(t, value))
		case "RRULE":
			rrule = value
		case "TZOFFSETFROM":
			from = parseOffsetSeconds(t, value)
		case "TZOFFSETTO":
			to = parseOffsetSeconds(t, value)
		case "END":
			if value == "VTIMEZONE" {
				continue
			}
			stated := append([]time.Time{dtstart}, rdates...)
			if rrule != "" {
				lister := NewRecurrenceStartLister(dtstart, rrule)
				if !lister.ListThrough(through, 1<<16) {
					t.Fatalf("rule %s from %s cannot be listed", rrule, dtstart)
				}
				stated = lister.starts
			}
			for _, onset := range stated {
				if from == to {
					// A zone that keeps one offset states no transition.
					continue
				}
				if _, duplicate := onsets[onset]; duplicate {
					t.Errorf("onset %s is stated twice", onset)
				}
				onsets[onset] = [2]int{from, to}
			}
		}
	}
	return onsets
}

func mustParseDateTime(t *testing.T, value string) time.Time {
	t.Helper()
	parsed, err := ParseDateTime(value)
	if err != nil {
		t.Fatalf("ParseDateTime(%q): %v", value, err)
	}
	return parsed
}

func parseOffsetSeconds(t *testing.T, value string) int {
	t.Helper()
	sign := 1
	if value[0] == '-' {
		sign = -1
	}
	digits := value[1:]
	for len(digits) < 6 {
		digits += "0"
	}
	parsed, err := time.Parse("150405", digits)
	if err != nil {
		t.Fatalf("offset %q: %v", value, err)
	}
	return sign * (parsed.Hour()*3600 + parsed.Minute()*60 + parsed.Second())
}

func TestVTimezoneRefusesZonesTheHostDoesNotDefine(t *testing.T) {
	for _, tzid := range []string{"", "Local", "Example/Nowhere", "Eastern Standard Time"} {
		if text, ok := VTimezone(tzid, wallClock(2026, time.January, 5), wallClock(2026, time.January, 5)); ok || text != "" {
			t.Errorf("VTimezone(%q) = %q, %v; want nothing", tzid, text, ok)
		}
	}
}

// Generation runs on every write of an object that names a zone it does not
// define, so the second request for a span is answered from the cache.
func TestVTimezoneIsCachedPerZoneAndSpan(t *testing.T) {
	requireHostZone(t, "Australia/Sydney")
	cache := newVTimezoneCache(2)
	build := func(year int) string {
		text, ok := cache.generate("Australia/Sydney", wallClock(year, time.May, 1), wallClock(year, time.May, 1))
		if !ok {
			t.Fatal("generate() ok = false")
		}
		return text
	}
	first := build(2026)
	if again := build(2026); again != first {
		t.Fatal("a cached definition differs from the generated one")
	}
	build(2027)
	build(2028)
	if held := cache.definitions.Len(); held != 2 {
		t.Errorf("cache holds %d entries, want its bound of 2", held)
	}
	if _, held := cache.definitions.Get(vtimezoneKey{tzid: "Australia/Sydney", fromYear: 2026, throughYear: 2026}); held {
		t.Error("the least recently used definition was not evicted")
	}
}

func BenchmarkVTimezone(b *testing.B) {
	requireHostZone(b, "Europe/Berlin")
	first, last := wallClock(2026, time.January, 5), wallClock(2100, time.December, 31)
	b.Run("uncached", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			cache := newVTimezoneCache(1)
			if _, ok := cache.generate("Europe/Berlin", first, last); !ok {
				b.Fatal("generate() ok = false")
			}
		}
	})
	b.Run("cached", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			if _, ok := VTimezone("Europe/Berlin", first, last); !ok {
				b.Fatal("VTimezone() ok = false")
			}
		}
	})
}

// An observance RRULE's UNTIL naming a moment is restated as the wall clock
// its onsets are stated on, read in TZOFFSETFROM; one already on that scale is
// left as written.
func TestObservanceRuleUntilIsRestatedOnTheOnsetsScale(t *testing.T) {
	const offsetFrom = time.Hour
	for _, tt := range []struct{ rrule, want string }{
		{"FREQ=YEARLY;BYMONTH=3;BYDAY=-1SU;UNTIL=20260329T010000Z", "FREQ=YEARLY;BYMONTH=3;BYDAY=-1SU;UNTIL=20260329T020000"},
		{"FREQ=YEARLY;UNTIL=20260329T010000z;BYMONTH=3", "FREQ=YEARLY;UNTIL=20260329T020000;BYMONTH=3"},
		{"FREQ=YEARLY;until=20260329T020000+0100", "FREQ=YEARLY;until=20260329T020000"},
		{"FREQ=YEARLY;UNTIL=20260329T020000", "FREQ=YEARLY;UNTIL=20260329T020000"},
		{"FREQ=YEARLY;UNTIL=20260329", "FREQ=YEARLY;UNTIL=20260329"},
		{"FREQ=YEARLY;COUNT=3", "FREQ=YEARLY;COUNT=3"},
	} {
		if got := observanceRuleOnWallClock(tt.rrule, offsetFrom); got != tt.want {
			t.Errorf("observanceRuleOnWallClock(%q) = %q, want %q", tt.rrule, got, tt.want)
		}
	}
}

func TestParseUTCOffsetRefusesNegativeZero(t *testing.T) {
	for value, want := range map[string]time.Duration{"+0000": 0, "+0530": 5*time.Hour + 30*time.Minute, "-0800": -8 * time.Hour, "-013015": -(time.Hour + 30*time.Minute + 15*time.Second)} {
		if got, ok := ParseUTCOffset(value); !ok || got != want {
			t.Errorf("ParseUTCOffset(%q) = %v, %v; want %v", value, got, ok, want)
		}
	}
	for _, value := range []string{"-0000", "-000000", "+2400", "+0060", "0100", "+01:00"} {
		if got, ok := ParseUTCOffset(value); ok {
			t.Errorf("ParseUTCOffset(%q) = %v, want refused", value, got)
		}
	}
}

// generatedObservance is one STANDARD or DAYLIGHT of a generated definition,
// as a client displays it: its kind, TZNAME and onsets.
type generatedObservance struct {
	daylight bool
	name     string
	onsets   []time.Time
	TimezoneObservance
}

// readGeneratedVTimezone reads a generated definition's observances as a
// VTIMEZONE reader does, listing each one's onsets through through.
func readGeneratedVTimezone(t *testing.T, text string, through time.Time) []generatedObservance {
	t.Helper()
	var observances []generatedObservance
	var current []PropertyValue
	var daylight bool
	inside := false
	for _, line := range strings.Split(strings.TrimSuffix(text, "\r\n"), "\r\n") {
		switch line {
		case "BEGIN:STANDARD", "BEGIN:DAYLIGHT":
			inside, current, daylight = true, nil, line == "BEGIN:DAYLIGHT"
		case "END:STANDARD", "END:DAYLIGHT":
			inside = false
			read, ok := ReadTimezoneObservances([][]PropertyValue{current})
			if !ok {
				t.Fatalf("an observance cannot be read:\n%s", text)
			}
			observance := generatedObservance{daylight: daylight, TimezoneObservance: read[0]}
			observance.onsets = append([]time.Time{read[0].Start}, read[0].RDates...)
			for _, property := range current {
				switch property.KeyPart {
				case "TZNAME":
					observance.name = property.Value
				case "RRULE":
					lister := NewRecurrenceStartLister(read[0].Start, property.Value)
					if !lister.ListThrough(through, 1<<16) {
						t.Fatalf("rule %s cannot be listed", property.Value)
					}
					observance.onsets = append(observance.onsets, lister.starts...)
				}
			}
			observances = append(observances, observance)
		default:
			if inside {
				keyPart, value, _ := strings.Cut(line, ":")
				current = append(current, PropertyValue{KeyPart: keyPart, Value: value})
			}
		}
	}
	if len(observances) == 0 {
		t.Fatalf("the definition has no observance:\n%s", text)
	}
	return observances
}

// requireDefinitionReadsTheHostZone checks that a reader of the definition
// generated for one day keeps the offset the host's zone keeps through it, and
// that the observance in force there names the time the host names, with the
// host's daylight saving status.
func requireDefinitionReadsTheHostZone(t *testing.T, tzid string, loc *time.Location, day time.Time) {
	t.Helper()
	text, ok := VTimezone(tzid, day, day)
	if !ok {
		t.Fatalf("VTimezone(%s, %s) ok = false", tzid, day.Format(time.DateOnly))
	}
	observances := readGeneratedVTimezone(t, text, day.AddDate(0, 0, 2))
	definition := make(TimezoneObservances, len(observances))
	for i, observance := range observances {
		definition[i] = observance.TimezoneObservance
	}
	for hour := 0; hour < 24; hour += 6 {
		instant := day.Add(time.Duration(hour) * time.Hour)
		hostName, hostOffset := instant.In(loc).Zone()
		got, ok := definition.OffsetAtInstant(instant, nil)
		if !ok || got != time.Duration(hostOffset)*time.Second {
			t.Fatalf("%s at %s: the definition reads %v (ok=%t), the host keeps %v\n%s", tzid, instant, got, ok, time.Duration(hostOffset)*time.Second, text)
		}
		var inForce *generatedObservance
		var latest time.Time
		for i := range observances {
			for _, onset := range observances[i].onsets {
				if at := onset.Add(-observances[i].OffsetFrom); !at.After(instant) && (inForce == nil || at.After(latest)) {
					inForce, latest = &observances[i], at
				}
			}
		}
		if inForce == nil {
			// Before every onset only the first one's TZOFFSETFROM speaks,
			// which states the offset of the zone's first period and no name.
			if start, _ := instant.In(loc).ZoneBounds(); !start.IsZero() {
				t.Fatalf("%s at %s: no observance is in force\n%s", tzid, instant, text)
			}
			continue
		}
		if inForce.name != hostName || inForce.daylight != instant.In(loc).IsDST() {
			t.Fatalf("%s at %s: the observance in force is %q, daylight %t; the host keeps %q, daylight %t\n%s",
				tzid, instant, inForce.name, inForce.daylight, hostName, instant.In(loc).IsDST(), text)
		}
	}
}

// A zone whose period in force over the span began with a change of name or
// of daylight saving status alone, and whose offset does not change within the
// span, is stated by that period: its start, its name and its daylight saving
// status, with the offset it keeps. Warsaw's mean time was renamed in 1880
// without changing its offset until 1915; Buenos Aires stopped calling its
// offset daylight time in 2000; Yukon kept its daylight offset as standard
// time from 2020; and Istanbul has kept +03 as standard time since 2016.
func TestVTimezoneStatesTheOffsetInForceAfterAChangeOfNameAlone(t *testing.T) {
	for _, tt := range []struct {
		tzid string
		day  time.Time
	}{
		{"Europe/Warsaw", time.Date(1905, time.June, 1, 0, 0, 0, 0, time.UTC)},
		{"America/Argentina/Buenos_Aires", time.Date(2000, time.June, 1, 0, 0, 0, 0, time.UTC)},
		{"America/Whitehorse", time.Date(2024, time.June, 1, 0, 0, 0, 0, time.UTC)},
		{"Europe/Istanbul", time.Date(2024, time.June, 1, 0, 0, 0, 0, time.UTC)},
	} {
		t.Run(tt.tzid, func(t *testing.T) {
			requireDefinitionReadsTheHostZone(t, tt.tzid, requireHostZone(t, tt.tzid), tt.day)
		})
	}
}

// Every zone the host defines yields a definition holding at least one
// observance for a span of a single day, in years when hundreds of zones were
// in a period that a change of name alone began, and the definition keeps the
// host's offset, name and daylight saving status through that day.
func TestVTimezoneForEveryHostZoneStatesAnObservance(t *testing.T) {
	data, err := os.ReadFile("/usr/share/zoneinfo/tzdata.zi")
	if err != nil {
		t.Skipf("host zone list unavailable: %v", err)
	}
	var zones []string
	for _, line := range strings.Split(string(data), "\n") {
		fields := strings.Fields(line)
		switch {
		case len(fields) >= 2 && fields[0] == "Z":
			zones = append(zones, fields[1])
		case len(fields) >= 3 && fields[0] == "L":
			zones = append(zones, fields[2])
		}
	}
	for _, tzid := range zones {
		loc, err := time.LoadLocation(tzid)
		if err != nil || !validTZIDText(tzid) {
			continue
		}
		for _, year := range []int{1900, 1905, 1920, 1950, 2000, 2024} {
			requireDefinitionReadsTheHostZone(t, tzid, loc, time.Date(year, time.June, 1, 0, 0, 0, 0, time.UTC))
		}
	}
}
