package ical

import (
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"
)

func requireHostZone(t testing.TB, tzid string) *time.Location {
	t.Helper()
	loc, err := time.LoadLocation(tzid)
	if err != nil {
		t.Skipf("host has no %s zone data: %v", tzid, err)
	}
	return loc
}

func TestHostLocationResolvesZonesTheHostKnows(t *testing.T) {
	want := requireHostZone(t, "America/Chicago")
	got, ok := HostLocation("America/Chicago")
	if !ok {
		t.Fatal("HostLocation(America/Chicago) ok = false")
	}
	instant := time.Date(2026, 7, 1, 12, 0, 0, 0, time.UTC)
	_, gotOffset := instant.In(got).Zone()
	_, wantOffset := instant.In(want).Zone()
	if gotOffset != wantOffset {
		t.Fatalf("HostLocation(America/Chicago) offset = %d, want %d", gotOffset, wantOffset)
	}
	again, ok := HostLocation("America/Chicago")
	if !ok || again != got {
		t.Fatalf("a second lookup returned %p, want the cached %p", again, got)
	}
}

// "Local" is the time package's name for the server's own zone, and LoadLocation
// reads the empty name as UTC. No calendar object can mean either.
func TestHostLocationRefusesNamesNoCalendarObjectCanMean(t *testing.T) {
	for _, tzid := range []string{"", "Local"} {
		if loc, ok := HostLocation(tzid); ok || loc != nil {
			t.Errorf("HostLocation(%q) = %v, %v; want nil, false", tzid, loc, ok)
		}
	}
}

func TestHostLocationDoesNotCacheNamesThatFailToLoadAsLocations(t *testing.T) {
	const garbage = "Not/A_Zone_The_Host_Knows"
	if _, ok := HostLocation(garbage); ok {
		t.Fatalf("HostLocation(%q) ok = true", garbage)
	}
	if _, cached := hostLocations.found.Load(garbage); cached {
		t.Fatalf("a TZID that failed to load was cached as a location")
	}
	if _, ok := HostLocation("Local"); ok {
		t.Fatal(`HostLocation("Local") ok = true`)
	}
	if _, cached := hostLocations.found.Load("Local"); cached {
		t.Fatal(`"Local" was cached`)
	}
}

// countingLoader loads names through time.LoadLocation and counts the calls.
type countingLoader struct {
	mu    sync.Mutex
	calls map[string]int
}

func (l *countingLoader) load(name string) (*time.Location, error) {
	l.mu.Lock()
	if l.calls == nil {
		l.calls = make(map[string]int)
	}
	l.calls[name]++
	l.mu.Unlock()
	return time.LoadLocation(name)
}

func (l *countingLoader) count(name string) int {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.calls[name]
}

// Outlook names a zone "Eastern Standard Time" and ships its definition, and
// every write of such an object asks the host about the name. Each probe of the
// zone database for a name it lacks costs several failed file lookups, so a
// name the host lacks is asked about once.
func TestHostLocationProbesANameTheHostLacksOnce(t *testing.T) {
	loader := &countingLoader{}
	cache := newHostLocationCache(loader.load, 8, 8)
	for range 3 {
		if _, ok := cache.lookup("Eastern Standard Time"); ok {
			t.Fatal("lookup(Eastern Standard Time) ok = true")
		}
	}
	if got := loader.count("Eastern Standard Time"); got != 1 {
		t.Fatalf("zone database probed %d times, want 1", got)
	}
}

// TZIDs are client-controlled, so the names remembered as missing are bounded
// in number, a long-unused one gives way to a newer one, and a name longer than
// any zone database name is remembered by a digest of fixed size.
func TestHostLocationRemembersBoundedMisses(t *testing.T) {
	const limit = 4
	loader := &countingLoader{}
	cache := newHostLocationCache(loader.load, 8, limit)
	for i := range 3 * limit {
		cache.lookup(fmt.Sprintf("Missing/Zone_%d", i))
	}
	if got := cache.missCount(); got != limit {
		t.Fatalf("misses remembered = %d, want %d", got, limit)
	}
	cache.lookup("Missing/Zone_0")
	if got := loader.count("Missing/Zone_0"); got != 2 {
		t.Errorf("an evicted name was probed %d times, want 2", got)
	}
	recent := fmt.Sprintf("Missing/Zone_%d", 3*limit-1)
	cache.lookup(recent)
	if got := loader.count(recent); got != 1 {
		t.Errorf("a remembered name was probed %d times, want 1", got)
	}

	long := strings.Repeat("x", 200)
	for range 1000 {
		cache.lookup(long)
	}
	if got := loader.count(long); got != 1 {
		t.Errorf("an overlong name was probed %d times, want once", got)
	}
	if other := strings.Repeat("x", 199) + "y"; func() bool { cache.lookup(other); return loader.count(other) != 1 }() {
		t.Error("another overlong name was taken for the one remembered")
	}
}

// A name that loads is still found, and found once, whatever the misses.
func TestHostLocationMissesDoNotHideZonesTheHostKnows(t *testing.T) {
	requireHostZone(t, "Europe/Berlin")
	loader := &countingLoader{}
	cache := newHostLocationCache(loader.load, 8, 1)
	cache.lookup("Missing/Zone")
	for range 3 {
		if _, ok := cache.lookup("Europe/Berlin"); !ok {
			t.Fatal("lookup(Europe/Berlin) ok = false")
		}
	}
	if got := loader.count("Europe/Berlin"); got != 1 {
		t.Fatalf("Europe/Berlin loaded %d times, want 1", got)
	}
}

func TestHostLocationIsSafeForConcurrentUse(t *testing.T) {
	requireHostZone(t, "Europe/Berlin")
	requireHostZone(t, "Asia/Tokyo")
	zones := []string{"Europe/Berlin", "Asia/Tokyo", "Not/A_Zone", ""}
	results := make([][]*time.Location, 16)
	var wg sync.WaitGroup
	for worker := range results {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for range 50 {
				for _, tzid := range zones {
					loc, ok := HostLocation(tzid)
					if ok != (loc != nil) {
						t.Errorf("HostLocation(%q) = %v, %v", tzid, loc, ok)
					}
					if tzid == "Europe/Berlin" {
						results[worker] = append(results[worker], loc)
					}
				}
			}
		}()
	}
	wg.Wait()
	first := results[0][0]
	if first == nil {
		t.Fatal("HostLocation(Europe/Berlin) returned nil")
	}
	for _, locs := range results {
		for _, loc := range locs {
			if loc != first {
				t.Fatalf("concurrent lookups returned different locations %p and %p", loc, first)
			}
		}
	}
}

func TestParsePropertyDateTimeLocalDoesNotResolveTheServersOwnZone(t *testing.T) {
	got, ok := ParsePropertyDateTimeLocal("DTSTART;TZID=Local", "20260105T090000")
	if !ok {
		t.Fatal("ParsePropertyDateTimeLocal() ok = false")
	}
	if want := time.Date(2026, 1, 5, 9, 0, 0, 0, time.UTC); !got.Equal(want) {
		t.Fatalf("TZID=Local read as %v, want the unresolvable-TZID reading %v", got, want)
	}
}

func BenchmarkHostLocation(b *testing.B) {
	requireHostZone(b, "America/New_York")
	b.Run("cached", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			if _, ok := HostLocation("America/New_York"); !ok {
				b.Fatal("HostLocation() ok = false")
			}
		}
	})
	b.Run("LoadLocation", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			if _, err := time.LoadLocation("America/New_York"); err != nil {
				b.Fatal(err)
			}
		}
	})
}

// rfc5545Readings are wall clocks either side of and inside the transitions
// of zones west and east of UTC, with negative daylight saving, and with a
// half-hour shift, each with the instant RFC 5545 §3.3.5 names: a wall clock
// a transition skips is read with the offset in force before the gap, and one
// it repeats is its first occurrence, likewise the offset before the
// transition.
var rfc5545Readings = []struct {
	tzid, wall, want string
}{
	{"America/New_York", "20260308T015959", "20260308T065959Z"},
	{"America/New_York", "20260308T020000", "20260308T070000Z"}, // skipped
	{"America/New_York", "20260308T023000", "20260308T073000Z"}, // skipped
	{"America/New_York", "20260308T030000", "20260308T070000Z"},
	{"America/New_York", "20261101T005959", "20261101T045959Z"},
	{"America/New_York", "20261101T010000", "20261101T050000Z"}, // repeated
	{"America/New_York", "20261101T013000", "20261101T053000Z"}, // repeated
	{"America/New_York", "20261101T020000", "20261101T070000Z"},
	{"Europe/Berlin", "20260329T015959", "20260329T005959Z"},
	{"Europe/Berlin", "20260329T020000", "20260329T010000Z"}, // skipped
	{"Europe/Berlin", "20260329T023000", "20260329T013000Z"}, // skipped
	{"Europe/Berlin", "20260329T030000", "20260329T010000Z"},
	{"Europe/Berlin", "20261025T015959", "20261024T235959Z"},
	{"Europe/Berlin", "20261025T020000", "20261025T000000Z"}, // repeated
	{"Europe/Berlin", "20261025T023000", "20261025T003000Z"}, // repeated
	{"Europe/Berlin", "20261025T030000", "20261025T020000Z"},
	{"Europe/Dublin", "20260329T005959", "20260329T005959Z"},
	{"Europe/Dublin", "20260329T013000", "20260329T013000Z"}, // skipped
	{"Europe/Dublin", "20260329T020000", "20260329T010000Z"},
	{"Europe/Dublin", "20261025T005959", "20261024T235959Z"},
	{"Europe/Dublin", "20261025T013000", "20261025T003000Z"}, // repeated
	{"Europe/Dublin", "20261025T020000", "20261025T020000Z"},
	{"Australia/Lord_Howe", "20261004T015959", "20261003T152959Z"},
	{"Australia/Lord_Howe", "20261004T021500", "20261003T154500Z"}, // skipped
	{"Australia/Lord_Howe", "20261004T023000", "20261003T153000Z"},
	{"Australia/Lord_Howe", "20260405T012959", "20260404T142959Z"},
	{"Australia/Lord_Howe", "20260405T014500", "20260404T144500Z"}, // repeated
	{"Australia/Lord_Howe", "20260405T020000", "20260404T153000Z"},
}

func TestWallClockInReadsSkippedAndRepeatedWallClocksAsRFC5545Does(t *testing.T) {
	for _, tt := range rfc5545Readings {
		t.Run(tt.tzid+" "+tt.wall, func(t *testing.T) {
			loc := requireHostZone(t, tt.tzid)
			wall, err := ParseDateTime(tt.wall)
			if err != nil {
				t.Fatal(err)
			}
			want, err := ParseDateTime(tt.want)
			if err != nil {
				t.Fatal(err)
			}
			if got := WallClockIn(loc, wall); !got.Equal(want) {
				t.Errorf("WallClockIn() = %s, want %s", got.UTC(), want)
			}
			if got, err := ParseDateTimeInLocation(tt.wall, loc); err != nil || !got.Equal(want) {
				t.Errorf("ParseDateTimeInLocation() = %s, %v; want %s", got, err, want)
			}
			if got, ok := ParsePropertyDateTimeLocal("DTSTART;TZID="+tt.tzid, tt.wall); !ok || !got.Equal(want) {
				t.Errorf("ParsePropertyDateTimeLocal() = %s, %v; want %s", got, ok, want)
			}
		})
	}
}

// Samoa skipped all of 2011-12-30 moving from -10 to +14; a reading on the
// missing day is still read with the offset before the gap.
func TestWallClockInReadsADaySkippedWhole(t *testing.T) {
	loc := requireHostZone(t, "Pacific/Apia")
	got := WallClockIn(loc, time.Date(2011, 12, 30, 12, 0, 0, 0, time.UTC))
	if want := time.Date(2011, 12, 30, 22, 0, 0, 0, time.UTC); !got.Equal(want) {
		t.Errorf("WallClockIn() = %s, want %s", got.UTC(), want)
	}
}

// RFC 5545 §3.3.10 leaves out of a recurrence set an instance its rule
// generates on a local time a transition skips, so whether a reading exists is
// asked separately from the instant §3.3.5 reads it as. A reading the clocks
// repeat exists; only one inside a gap does not.
func TestWallClockExistsOnlyOutsideTheGapsAZoneSkips(t *testing.T) {
	tests := []struct {
		tzid, wall string
		want       bool
	}{
		{"America/New_York", "20260308T015959", true},
		{"America/New_York", "20260308T020000", false},
		{"America/New_York", "20260308T023000", false},
		{"America/New_York", "20260308T025959", false},
		{"America/New_York", "20260308T030000", true},
		{"America/New_York", "20261101T013000", true},
		{"Europe/Berlin", "20260329T015959", true},
		{"Europe/Berlin", "20260329T020000", false},
		{"Europe/Berlin", "20260329T023000", false},
		{"Europe/Berlin", "20260329T030000", true},
		{"Europe/Berlin", "20261025T023000", true},
		{"Europe/Berlin", "20260705T023000", true},
		{"Europe/Dublin", "20260329T013000", false},
		{"Europe/Dublin", "20261025T013000", true},
		{"Australia/Lord_Howe", "20261004T021500", false},
		{"Australia/Lord_Howe", "20261004T023000", true},
		{"Australia/Lord_Howe", "20260405T014500", true},
		{"Pacific/Apia", "20111230T120000", false},
		{"Pacific/Apia", "20111231T000000", true},
		{"Asia/Kolkata", "20260329T023000", true},
		{"UTC", "20260329T023000", true},
	}
	for _, tt := range tests {
		t.Run(tt.tzid+" "+tt.wall, func(t *testing.T) {
			loc := requireHostZone(t, tt.tzid)
			wall, err := ParseDateTime(tt.wall)
			if err != nil {
				t.Fatal(err)
			}
			if got := WallClockExists(loc, wall); got != tt.want {
				t.Errorf("WallClockExists() = %v, want %v", got, tt.want)
			}
			if got := PropertyTimeResolver(nil).LocalTimeExists(tt.tzid, wall); got != tt.want {
				t.Errorf("PropertyTimeResolver(nil).LocalTimeExists() = %v, want %v", got, tt.want)
			}
		})
	}
}

// ParsePropertyDateTimeLocal reads a TZID the host does not define as UTC,
// which skips nothing, so the default resolver finds every reading of it.
func TestDefaultResolverFindsEveryReadingOfAnUndefinedZone(t *testing.T) {
	if !PropertyTimeResolver(nil).LocalTimeExists("Not/A_Zone", time.Date(2026, 3, 29, 2, 30, 0, 0, time.UTC)) {
		t.Error("LocalTimeExists(Not/A_Zone) = false, want true")
	}
}

func BenchmarkWallClockIn(b *testing.B) {
	loc := requireHostZone(b, "Europe/Berlin")
	wall := time.Date(2026, 7, 1, 9, 0, 0, 0, time.UTC)
	for b.Loop() {
		WallClockIn(loc, wall)
	}
}

// BenchmarkHostLocationOfANameTheHostLacks measures resolving a TZID such as
// Outlook's "Eastern Standard Time", which an object defines itself and the
// host's time zone database cannot load.
func BenchmarkHostLocationOfANameTheHostLacks(b *testing.B) {
	b.ReportAllocs()
	for b.Loop() {
		if _, ok := HostLocation("Eastern Standard Time"); ok {
			b.Fatal("HostLocation(Eastern Standard Time) ok = true")
		}
	}
}

// A TZID the object's own VTIMEZONE answers for is read by that definition
// alone; the host's database is asked about it only where the definition
// cannot be followed.
func TestObjectDefinedZoneDoesNotAskTheHost(t *testing.T) {
	tzid := "Private Zone " + t.Name()
	lines := UnfoldLines("BEGIN:VCALENDAR\r\nBEGIN:VTIMEZONE\r\nTZID:" + tzid + "\r\n" +
		"BEGIN:STANDARD\r\nDTSTART:19700101T000000\r\nTZOFFSETFROM:-0500\r\nTZOFFSETTO:-0500\r\nEND:STANDARD\r\n" +
		"END:VTIMEZONE\r\nBEGIN:VEVENT\r\nUID:e\r\nDTSTART;TZID=" + tzid + ":20260105T090000\r\nEND:VEVENT\r\nEND:VCALENDAR\r\n")
	zone := resolveTZID(lines, tzid)
	if got, want := zone.instant(time.Date(2026, 1, 5, 9, 0, 0, 0, time.UTC)), time.Date(2026, 1, 5, 14, 0, 0, 0, time.UTC); !got.Equal(want) {
		t.Fatalf("instant = %s, want %s", got, want)
	}
	if gaps, ok := seriesLocalTimeGaps(NewObjectTimes(lines, nil, nil), "DTSTART;TZID="+tzid, "20260105T090000"); !ok || gaps == nil {
		t.Fatal("the object's definition lists no gaps")
	}
	if hostLocations.missed(tzid) {
		t.Fatal("the host's database was asked about a TZID the object defines")
	}
}
