package dav

import (
	"strings"
	"testing"
	"time"

	"github.com/jw6ventures/calcard/internal/ical"
	"github.com/jw6ventures/calcard/internal/store"
)

// sydneyBefore2008 is Australia/Sydney as a client ships it across the 2008
// rule change: the 1996 rules end with a UTC UNTIL at their last onset, as RFC
// 5545 §3.3.10 requires of an observance whose DTSTART is a local time.
var sydneyBefore2008 = buildComponent("VTIMEZONE", "TZID:Australia/Sydney",
	buildComponent("STANDARD", "DTSTART:19960331T030000", "TZOFFSETFROM:+1100", "TZOFFSETTO:+1000",
		"RRULE:FREQ=YEARLY;BYMONTH=3;BYDAY=-1SU;UNTIL=20070324T160000Z"),
	buildComponent("DAYLIGHT", "DTSTART:20011028T020000", "TZOFFSETFROM:+1000", "TZOFFSETTO:+1100",
		"RRULE:FREQ=YEARLY;BYMONTH=10;BYDAY=-1SU;UNTIL=20071027T160000Z"),
	buildComponent("STANDARD", "DTSTART:20080406T030000", "TZOFFSETFROM:+1100", "TZOFFSETTO:+1000",
		"RRULE:FREQ=YEARLY;BYMONTH=4;BYDAY=1SU"),
	buildComponent("DAYLIGHT", "DTSTART:20081005T020000", "TZOFFSETFROM:+1000", "TZOFFSETTO:+1100",
		"RRULE:FREQ=YEARLY;BYMONTH=10;BYDAY=1SU"))

// newYorkBefore2007 is America/New_York across the 2007 rule change, the 1987
// and 1967 rules ending with a UTC UNTIL at their last onset.
var newYorkBefore2007 = buildComponent("VTIMEZONE", "TZID:America/New_York",
	buildComponent("DAYLIGHT", "DTSTART:19870405T020000", "TZOFFSETFROM:-0500", "TZOFFSETTO:-0400",
		"RRULE:FREQ=YEARLY;BYMONTH=4;BYDAY=1SU;UNTIL=20060402T070000Z"),
	buildComponent("STANDARD", "DTSTART:19671029T020000", "TZOFFSETFROM:-0400", "TZOFFSETTO:-0500",
		"RRULE:FREQ=YEARLY;BYMONTH=10;BYDAY=-1SU;UNTIL=20061029T060000Z"),
	buildComponent("DAYLIGHT", "DTSTART:20070311T020000", "TZOFFSETFROM:-0500", "TZOFFSETTO:-0400",
		"RRULE:FREQ=YEARLY;BYMONTH=3;BYDAY=2SU"),
	buildComponent("STANDARD", "DTSTART:20071104T020000", "TZOFFSETFROM:-0400", "TZOFFSETTO:-0500",
		"RRULE:FREQ=YEARLY;BYMONTH=11;BYDAY=1SU"))

func definitionObservances(t *testing.T, definition string) *ical.IndexedTimezoneObservances {
	t.Helper()
	root, err := parseICalendarObject(buildCalendarObject(definition))
	if err != nil {
		t.Fatal(err)
	}
	tzid := strings.TrimPrefix(strings.Split(definition, "\r\n")[1], "TZID:")
	observances, ok := submittedTimezoneObservances(root, tzid)
	if !ok {
		t.Fatalf("%s has no usable observance", tzid)
	}
	return observances
}

func hours(n float64) time.Duration { return time.Duration(n * float64(time.Hour)) }

// An observance's onsets are local wall clocks (RFC 5545 §3.6.5) while its
// UNTIL is UTC (§3.3.10), so the last onset a rule keeps is the one whose
// moment -- the wall clock less TZOFFSETFROM -- is at or before UNTIL. East of
// UTC a wall clock is later than the moment it names; west of it, earlier.
// Whether or not the cache answers, the reading is the same.
func TestObservanceUntilIsComparedAsTheMomentItNames(t *testing.T) {
	sydneyEndingAt := func(until string) string {
		return buildComponent("VTIMEZONE", "TZID:Example/Sydney",
			buildComponent("STANDARD", "DTSTART:19960331T030000", "TZOFFSETFROM:+1100", "TZOFFSETTO:+1000",
				"RRULE:FREQ=YEARLY;BYMONTH=3;BYDAY=-1SU;UNTIL="+until),
			buildComponent("DAYLIGHT", "DTSTART:19961027T020000", "TZOFFSETFROM:+1000", "TZOFFSETTO:+1100",
				"RRULE:FREQ=YEARLY;BYMONTH=10;BYDAY=-1SU"))
	}
	newYorkEndingAt := func(until string) string {
		return buildComponent("VTIMEZONE", "TZID:Example/New_York",
			buildComponent("DAYLIGHT", "DTSTART:19870405T020000", "TZOFFSETFROM:-0500", "TZOFFSETTO:-0400",
				"RRULE:FREQ=YEARLY;BYMONTH=4;BYDAY=1SU"),
			buildComponent("STANDARD", "DTSTART:19671029T020000", "TZOFFSETFROM:-0400", "TZOFFSETTO:-0500",
				"RRULE:FREQ=YEARLY;BYMONTH=10;BYDAY=-1SU;UNTIL="+until))
	}
	tests := []struct {
		name       string
		definition string
		wall       time.Time
		want       time.Duration
	}{
		{"Sydney after its last March onset", sydneyBefore2008, time.Date(2007, 3, 26, 9, 0, 0, 0, time.UTC), hours(10)},
		{"Sydney a week later", sydneyBefore2008, time.Date(2007, 4, 1, 9, 0, 0, 0, time.UTC), hours(10)},
		{"Sydney before its last March onset", sydneyBefore2008, time.Date(2007, 3, 25, 1, 0, 0, 0, time.UTC), hours(11)},
		{"Sydney after its last October onset", sydneyBefore2008, time.Date(2007, 11, 1, 9, 0, 0, 0, time.UTC), hours(11)},
		{"Sydney under the 2008 rules", sydneyBefore2008, time.Date(2008, 4, 10, 9, 0, 0, 0, time.UTC), hours(10)},
		{"UNTIL at the onset's moment keeps it", sydneyEndingAt("20070324T160000Z"), time.Date(2007, 3, 26, 9, 0, 0, 0, time.UTC), hours(10)},
		{"UNTIL a second before it drops it", sydneyEndingAt("20070324T155959Z"), time.Date(2007, 3, 26, 9, 0, 0, 0, time.UTC), hours(11)},
		{"New York after its last October onset", newYorkBefore2007, time.Date(2006, 11, 1, 9, 0, 0, 0, time.UTC), hours(-5)},
		{"New York after its last April onset", newYorkBefore2007, time.Date(2006, 4, 10, 9, 0, 0, 0, time.UTC), hours(-4)},
		{"New York before its last October onset", newYorkBefore2007, time.Date(2006, 10, 29, 1, 30, 0, 0, time.UTC), hours(-4)},
		{"New York under the 2007 rules", newYorkBefore2007, time.Date(2007, 3, 20, 9, 0, 0, 0, time.UTC), hours(-4)},
		{"west of UTC, UNTIL at the onset's moment keeps it", newYorkEndingAt("20061029T060000Z"), time.Date(2006, 11, 1, 9, 0, 0, 0, time.UTC), hours(-5)},
		{"west of UTC, UNTIL a second before it drops it", newYorkEndingAt("20061029T055959Z"), time.Date(2006, 11, 1, 9, 0, 0, 0, time.UTC), hours(-4)},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			observances := definitionObservances(t, tt.definition)
			for name, onsets := range map[string]*timezoneOnsets{"walking": nil, "cached": newTimezoneOnsets(maxHeldOnsets)} {
				if got, ok := observances.OffsetAtWall(tt.wall, onsets); !ok || got != tt.want {
					t.Errorf("%s: wall clock %v reads %v, %t, want %v", name, tt.wall, got, ok, tt.want)
				}
				instant := tt.wall.Add(-tt.want)
				if got, ok := observances.OffsetAtInstant(instant, onsets); !ok || got != tt.want {
					t.Errorf("%s: instant %v reads %v, %t, want %v", name, instant, got, ok, tt.want)
				}
			}
		})
	}
}

// The same RRULE text ends at a different onset under a different
// TZOFFSETFROM, so the cache cannot answer one zone from another's listing.
func TestObservanceRuleSharedByZonesOfDifferentOffsetsEndsWhereEachSays(t *testing.T) {
	const rule = "RRULE:FREQ=YEARLY;BYMONTH=3;BYDAY=-1SU;UNTIL=20070325T000000Z"
	zone := func(tzid, from, to string) string {
		return buildComponent("VTIMEZONE", "TZID:"+tzid,
			buildComponent("STANDARD", "DTSTART:19960331T030000", "TZOFFSETFROM:"+from, "TZOFFSETTO:"+to, rule),
			buildComponent("DAYLIGHT", "DTSTART:19961027T020000", "TZOFFSETFROM:"+to, "TZOFFSETTO:"+from,
				"RRULE:FREQ=YEARLY;BYMONTH=10;BYDAY=-1SU"))
	}
	onsets := useTimezoneOnsets(t, maxHeldOnsets)
	wall := time.Date(2007, 3, 26, 9, 0, 0, 0, time.UTC)
	for _, tt := range []struct {
		definition string
		want       time.Duration
	}{
		// 03:00 at +1100 is 16:00Z the day before, inside UNTIL.
		{zone("Example/East", "+1100", "+1000"), hours(10)},
		// 03:00 at -0500 is 08:00Z, past UNTIL.
		{zone("Example/West", "-0500", "-0600"), hours(-5)},
		{zone("Example/East", "+1100", "+1000"), hours(10)},
	} {
		observances := definitionObservances(t, tt.definition)
		if got, ok := observanceOffsetAtWall(observances, wall); !ok || got != tt.want {
			t.Errorf("wall clock %v reads %v, %t, want %v", wall, got, ok, tt.want)
		}
	}
	if walks := onsets.walks.Load(); walks != 0 {
		t.Fatalf("walked %d rules, want every lookup answered from a listing", walks)
	}
}

// A value whose TZID a client defines is placed by that definition, through to
// a calendar-query over it.
func TestClientDefinitionPlacesValuesAfterAnObservancesLastOnset(t *testing.T) {
	raw := buildCalendarObject(sydneyBefore2008, buildComponent("VEVENT", "UID:e", "DTSTAMP:20070101T000000Z",
		"DTSTART;TZID=Australia/Sydney:20070304T090000", "DTEND;TZID=Australia/Sydney:20070304T100000",
		"RRULE:FREQ=WEEKLY;COUNT=8"))
	root, err := parseICalendarObject(raw)
	if err != nil {
		t.Fatal(err)
	}
	m := newCalendarTimeRangeMatcher(raw, root, floatingZone{})
	for wall, want := range map[string]time.Time{
		"20070318T090000": time.Date(2007, 3, 17, 22, 0, 0, 0, time.UTC),
		"20070326T090000": time.Date(2007, 3, 25, 23, 0, 0, 0, time.UTC),
		"20070401T090000": time.Date(2007, 3, 31, 23, 0, 0, 0, time.UTC),
	} {
		if got, ok := m.resolveInstant("Australia/Sydney", wall, icalFloatingDateTime); !ok || !got.Equal(want) {
			t.Errorf("%s read as %v, %t, want %v", wall, got, ok, want)
		}
	}

	event := store.Event{UID: "e", RawICAL: raw}
	// The 1 April instance runs 23:00Z to 00:00Z at +1000.
	if !filterMatches(t, event, calQueryWithTimeRange("20070331T233000Z", "20070401T000000Z"), floatingZone{}) {
		t.Error("calendar-query missed the 1 April instance at its +1000 time")
	}
	if filterMatches(t, event, calQueryWithTimeRange("20070331T220000Z", "20070331T230000Z"), floatingZone{}) {
		t.Error("calendar-query matched the 1 April instance an hour early, at +1100")
	}
}
