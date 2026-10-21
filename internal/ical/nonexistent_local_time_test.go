package ical

import (
	"errors"
	"fmt"
	"math/rand"
	"slices"
	"strings"
	"testing"
	"time"
)

// privateZoneResolver reads one TZID the host does not define through a zone of
// the test's choosing, and says which local times that zone skips from the same
// zone -- the way the DAV layer answers from the VTIMEZONE an object ships. The
// expansion has nowhere else to learn it: the host knows no such name.
type privateZoneResolver struct {
	tzid string
	loc  *time.Location
}

func (r privateZoneResolver) wall(keyPart, value string) (time.Time, bool) {
	value = strings.TrimSpace(value)
	if tzid, ok := PropertyParam(keyPart, "TZID"); !ok || tzid != r.tzid || hasZoneSuffix(value) {
		return time.Time{}, false
	}
	wall, err := ParseDateTime(value)
	return wall, err == nil
}

func (r privateZoneResolver) ResolvePropertyTime(keyPart, value string) (time.Time, bool) {
	if wall, ok := r.wall(keyPart, value); ok {
		return WallClockIn(r.loc, wall).UTC(), true
	}
	return ParsePropertyDateTimeLocal(keyPart, value)
}

func (r privateZoneResolver) LocalTimeExists(tzid string, wall time.Time) bool {
	if tzid == r.tzid {
		return WallClockExists(r.loc, wall)
	}
	return PropertyTimeResolver(nil).LocalTimeExists(tzid, wall)
}

func recurringObject(lines ...string) string {
	return "BEGIN:VCALENDAR\r\nBEGIN:VEVENT\r\nUID:series\r\n" +
		strings.Join(lines, "\r\n") + "\r\nEND:VEVENT\r\nEND:VCALENDAR\r\n"
}

func utcInstants(values ...string) []time.Time {
	instants := make([]time.Time, 0, len(values))
	for _, value := range values {
		instant, err := time.Parse(time.RFC3339, value)
		if err != nil {
			panic(err)
		}
		instants = append(instants, instant)
	}
	return instants
}

func sameInstants(got, want []time.Time) bool {
	if len(got) != len(want) {
		return false
	}
	for i := range want {
		if !got[i].Equal(want[i]) {
			return false
		}
	}
	return true
}

func utcStrings(values []time.Time) []string {
	out := make([]string, 0, len(values))
	for _, value := range values {
		out = append(out, value.UTC().Format(time.RFC3339))
	}
	return out
}

// A recurrence instance on a local time a transition skips is read at the
// offset before the gap (RFC 5545 §3.3.5) and counted like any other
// instance, as the DTSTART -- the first instance whatever it names
// (§3.8.5.3) -- and an RDATE are. Where that reading repeats the instant of
// another candidate, the two are one instance, not two (§3.8.5.3 "duplicate
// instances are ignored").
func TestRecurrenceMovesInstancesForwardOnSkippedLocalTimes(t *testing.T) {
	requireHostZone(t, "Europe/Berlin")
	requireHostZone(t, "America/New_York")
	requireHostZone(t, "Australia/Lord_Howe")
	berlin := requireHostZone(t, "Europe/Berlin")
	private := privateZoneResolver{tzid: "Private/Berlin", loc: berlin}
	floatingInBerlin := PropertyTimeResolver(func(keyPart, value string) (time.Time, bool) {
		if _, ok := PropertyParam(keyPart, "TZID"); !ok && !hasZoneSuffix(strings.TrimSpace(value)) {
			wall, err := ParseDateTime(strings.TrimSpace(value))
			return WallClockIn(berlin, wall).UTC(), err == nil
		}
		return ParsePropertyDateTimeLocal(keyPart, value)
	})

	tests := []struct {
		name    string
		lines   []string
		resolve RecurrenceTimeResolver
		want    []time.Time
	}{
		{
			name:  "weekly Sunday 02:30 across the Berlin spring change, host zone",
			lines: []string{"DTSTART;TZID=Europe/Berlin:20260315T023000", "RRULE:FREQ=WEEKLY;COUNT=4"},
			want:  utcInstants("2026-03-15T01:30:00Z", "2026-03-22T01:30:00Z", "2026-03-29T01:30:00Z", "2026-04-05T00:30:00Z"),
		},
		{
			name:    "weekly Sunday 02:30 across the spring change, zone the object defines",
			lines:   []string{"DTSTART;TZID=Private/Berlin:20260315T023000", "RRULE:FREQ=WEEKLY;COUNT=4"},
			resolve: private,
			want:    utcInstants("2026-03-15T01:30:00Z", "2026-03-22T01:30:00Z", "2026-03-29T01:30:00Z", "2026-04-05T00:30:00Z"),
		},
		{
			name:  "weekly Sunday 02:30 with BYDAY across the spring change",
			lines: []string{"DTSTART;TZID=Europe/Berlin:20260315T023000", "RRULE:FREQ=WEEKLY;BYDAY=SU;COUNT=4"},
			want:  utcInstants("2026-03-15T01:30:00Z", "2026-03-22T01:30:00Z", "2026-03-29T01:30:00Z", "2026-04-05T00:30:00Z"),
		},
		{
			name:  "a repeated 02:30 stays, as its first occurrence",
			lines: []string{"DTSTART;TZID=Europe/Berlin:20261018T023000", "RRULE:FREQ=WEEKLY;COUNT=3"},
			want:  utcInstants("2026-10-18T00:30:00Z", "2026-10-25T00:30:00Z", "2026-11-01T01:30:00Z"),
		},
		{
			name:  "daily 02:30 across the New York spring change",
			lines: []string{"DTSTART;TZID=America/New_York:20260307T023000", "RRULE:FREQ=DAILY;COUNT=3"},
			want:  utcInstants("2026-03-07T07:30:00Z", "2026-03-08T07:30:00Z", "2026-03-09T06:30:00Z"),
		},
		{
			name:  "a DTSTART on the skipped time is still the first instance",
			lines: []string{"DTSTART;TZID=Europe/Berlin:20260329T023000", "RRULE:FREQ=WEEKLY;COUNT=3"},
			want:  utcInstants("2026-03-29T01:30:00Z", "2026-04-05T00:30:00Z", "2026-04-12T00:30:00Z"),
		},
		{
			name:  "a DTSTART on the skipped time the rule also generates",
			lines: []string{"DTSTART;TZID=Europe/Berlin:20260329T023000", "RRULE:FREQ=WEEKLY;BYDAY=SU;BYHOUR=2;COUNT=3"},
			want:  utcInstants("2026-03-29T01:30:00Z", "2026-04-05T00:30:00Z", "2026-04-12T00:30:00Z"),
		},
		{
			// The RRULE's own second instance names the same slot the RDATE
			// does, so the RDATE adds nothing past what the rule generates.
			name: "an RDATE on the already-generated skipped time adds nothing",
			lines: []string{
				"DTSTART;TZID=Europe/Berlin:20260322T023000", "RRULE:FREQ=WEEKLY;COUNT=2",
				"RDATE;TZID=Europe/Berlin:20260329T023000",
			},
			want: utcInstants("2026-03-22T01:30:00Z", "2026-03-29T01:30:00Z"),
		},
		{
			// The hour inside the gap and the hour right after it read the
			// same instant, so the series still counts four in five hours of
			// wall clock -- the same set a drop would have produced, since an
			// hourly step is exactly the gap's length.
			name:  "an hourly series merges the hour a collision produces",
			lines: []string{"DTSTART;TZID=Europe/Berlin:20260329T003000", "RRULE:FREQ=HOURLY;COUNT=4"},
			want:  utcInstants("2026-03-28T23:30:00Z", "2026-03-29T00:30:00Z", "2026-03-29T01:30:00Z", "2026-03-29T02:30:00Z"),
		},
		{
			// A floating series is defined in no zone, so none of its
			// instances can be one a zone skips; the zone RFC 4791 §7.3 reads
			// it in places each one per §3.3.5.
			name:    "a floating series keeps every instance",
			lines:   []string{"DTSTART:20260322T023000", "RRULE:FREQ=WEEKLY;COUNT=3"},
			resolve: floatingInBerlin,
			want:    utcInstants("2026-03-22T01:30:00Z", "2026-03-29T01:30:00Z", "2026-04-05T00:30:00Z"),
		},
		{
			name:  "a UTC series keeps every instance",
			lines: []string{"DTSTART:20260322T013000Z", "RRULE:FREQ=WEEKLY;COUNT=3"},
			want:  utcInstants("2026-03-22T01:30:00Z", "2026-03-29T01:30:00Z", "2026-04-05T01:30:00Z"),
		},
		{
			name:  "an invalid date is skipped and not counted",
			lines: []string{"DTSTART;TZID=Europe/Berlin:20260130T090000", "RRULE:FREQ=MONTHLY;COUNT=3"},
			want:  utcInstants("2026-01-30T08:00:00Z", "2026-03-30T07:00:00Z", "2026-04-30T07:00:00Z"),
		},
		{
			// 02:45 on the New York spring change is skipped and reads 03:45
			// EDT, the instant the rule's own 03:45 also names, so the two
			// are one instance -- and it follows the 03:15 DTSTART, so it is
			// in the set, read at that existing 03:45.
			name:  "a skipped candidate merged into a later existing time on the DTSTART's day",
			lines: []string{"DTSTART;TZID=America/New_York:20260308T031500", "RRULE:FREQ=DAILY;BYHOUR=2,3;BYMINUTE=15,45;COUNT=6"},
			want: utcInstants("2026-03-08T07:15:00Z", "2026-03-08T07:45:00Z",
				"2026-03-09T06:15:00Z", "2026-03-09T06:45:00Z", "2026-03-09T07:15:00Z", "2026-03-09T07:45:00Z"),
		},
		{
			// Lord Howe skips 02:00-02:30 on 4 October 2026, so the 02:15
			// DTSTART reads 15:45Z at +10:30, after the 02:30 candidate's
			// 15:30Z at +11:00, which therefore precedes the series. The
			// DTSTART is the rule's own 02:15 candidate and is counted once.
			name:  "a DTSTART on a skipped time the rule generates, after a later wall clock's instant",
			lines: []string{"DTSTART;TZID=Australia/Lord_Howe:20261004T021500", "RRULE:FREQ=DAILY;BYHOUR=2;BYMINUTE=15,30;COUNT=3"},
			want:  utcInstants("2026-10-03T15:45:00Z", "2026-10-04T15:15:00Z", "2026-10-04T15:30:00Z"),
		},
		{
			// 02:45 on the New York spring change reads 07:45Z, after the
			// 03:15 EDT DTSTART's 07:15Z though its wall clock is before it,
			// and nothing else in the rule names that instant.
			name:  "a skipped candidate before the DTSTART's wall clock but after its instant",
			lines: []string{"DTSTART;TZID=America/New_York:20260308T031500", "RRULE:FREQ=DAILY;BYHOUR=2;BYMINUTE=45;COUNT=3"},
			want:  utcInstants("2026-03-08T07:15:00Z", "2026-03-08T07:45:00Z", "2026-03-09T06:45:00Z"),
		},
		{
			// The rule's 02:45 reads the DTSTART's own 03:45 EDT instant, so
			// it is the DTSTART again and not a second instance.
			name:  "a skipped candidate reading the DTSTART's instant is the DTSTART",
			lines: []string{"DTSTART;TZID=America/New_York:20260308T034500", "RRULE:FREQ=DAILY;BYHOUR=2;BYMINUTE=45;COUNT=3"},
			want:  utcInstants("2026-03-08T07:45:00Z", "2026-03-09T06:45:00Z", "2026-03-10T06:45:00Z"),
		},
		{
			name:  "a skipped candidate merged into a later existing time, host Berlin",
			lines: []string{"DTSTART;TZID=Europe/Berlin:20260329T031500", "RRULE:FREQ=DAILY;BYHOUR=2,3;BYMINUTE=15,45;COUNT=6"},
			want: utcInstants("2026-03-29T01:15:00Z", "2026-03-29T01:45:00Z",
				"2026-03-30T00:15:00Z", "2026-03-30T00:45:00Z", "2026-03-30T01:15:00Z", "2026-03-30T01:45:00Z"),
		},
		{
			name:    "a skipped candidate merged into a later existing time, zone the object defines",
			lines:   []string{"DTSTART;TZID=Private/Berlin:20260329T031500", "RRULE:FREQ=DAILY;BYHOUR=2,3;BYMINUTE=15,45;COUNT=6"},
			resolve: private,
			want: utcInstants("2026-03-29T01:15:00Z", "2026-03-29T01:45:00Z",
				"2026-03-30T00:15:00Z", "2026-03-30T00:45:00Z", "2026-03-30T01:15:00Z", "2026-03-30T01:45:00Z"),
		},
		{
			// 02:00 on the New York spring change is skipped and reads 07:00Z,
			// before the 03:05 EDT DTSTART's 07:05Z although its wall clock
			// is not, so it precedes the series and is not an instance.
			name:  "a skipped candidate whose instant precedes the DTSTART's is not an instance",
			lines: []string{"DTSTART;TZID=America/New_York:20260308T030500", "RRULE:FREQ=DAILY;BYHOUR=2;BYMINUTE=0;COUNT=3"},
			want:  utcInstants("2026-03-08T07:05:00Z", "2026-03-09T06:00:00Z", "2026-03-10T06:00:00Z"),
		},
		{
			// 02:30 reads 07:30Z, after the DTSTART's 07:05Z, so it is the
			// second instance although its wall clock precedes the DTSTART's.
			name:  "a skipped candidate whose instant follows the DTSTART's is the second instance",
			lines: []string{"DTSTART;TZID=America/New_York:20260308T030500", "RRULE:FREQ=DAILY;BYHOUR=2;BYMINUTE=30;COUNT=3"},
			want:  utcInstants("2026-03-08T07:05:00Z", "2026-03-08T07:30:00Z", "2026-03-09T06:30:00Z"),
		},
		{
			// The skipped 02:30 reads 07:30Z, the UNTIL, and ends the series
			// with the existing 03:30 EDT it shares that instant with.
			name:  "UNTIL ending on a skipped candidate kept past the DTSTART's wall clock",
			lines: []string{"DTSTART;TZID=America/New_York:20260308T030500", "RRULE:FREQ=DAILY;BYHOUR=2,3;BYMINUTE=30;UNTIL=20260308T073000Z"},
			want:  utcInstants("2026-03-08T07:05:00Z", "2026-03-08T07:30:00Z"),
		},
		{
			name:  "a weekly series whose skipped candidate follows the DTSTART's instant",
			lines: []string{"DTSTART;TZID=America/New_York:20260308T031500", "RRULE:FREQ=WEEKLY;BYDAY=SU;BYHOUR=2,3;BYMINUTE=45;COUNT=3"},
			want:  utcInstants("2026-03-08T07:15:00Z", "2026-03-08T07:45:00Z", "2026-03-15T06:45:00Z"),
		},
		{
			// An hourly period holds the 02:15 DTSTART and the 02:30 after
			// it on the wall clock, which reads 15:30Z at +11:00 and so comes
			// before the DTSTART's 15:45Z at +10:30.
			name:  "an hourly DTSTART on a skipped time, after a later wall clock's instant",
			lines: []string{"DTSTART;TZID=Australia/Lord_Howe:20261004T021500", "RRULE:FREQ=HOURLY;BYMINUTE=15,30;COUNT=3"},
			want:  utcInstants("2026-10-03T15:45:00Z", "2026-10-03T16:15:00Z", "2026-10-03T16:30:00Z"),
		},
		{
			// BYSETPOS numbers every candidate of the period, so the last
			// Sunday at 02:30 in March 2026 Berlin is still the 29th, read at
			// the offset before the gap it falls in.
			name:  "BYSETPOS selects the period's actual last candidate",
			lines: []string{"DTSTART;TZID=Europe/Berlin:20260222T023000", "RRULE:FREQ=MONTHLY;BYDAY=SU;BYSETPOS=-1;COUNT=3"},
			want:  utcInstants("2026-02-22T01:30:00Z", "2026-03-29T01:30:00Z", "2026-04-26T00:30:00Z"),
		},
	}

	rangeStart := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	rangeEnd := time.Date(2027, 1, 1, 0, 0, 0, 0, time.UTC)
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			raw := recurringObject(tt.lines...)
			master := PrimaryVEventComponent(raw)
			dtstartProperty, _ := ComponentProperty(master, "DTSTART")
			resolver := tt.resolve
			if resolver == nil {
				resolver = PropertyTimeResolver(nil)
			}
			dtstart, ok := resolver.ResolvePropertyTime(dtstartProperty.KeyPart, dtstartProperty.Value)
			if !ok {
				t.Fatal("DTSTART did not resolve")
			}

			instances := instanceStarts(requireRecurrenceResult[RecurrenceInstance](t)(
				collectRecurrenceInstances(raw, "VEVENT", dtstart, time.Hour, rangeStart, rangeEnd, MaxRecurrenceInstances, tt.resolve)))
			sortTimes(instances)
			if !sameInstants(instances, tt.want) {
				t.Errorf("VisitRecurrenceInstances() = %v, want %v", utcStrings(instances), utcStrings(tt.want))
			}

			periods, err := RecurringBusyPeriods(raw, dtstart, time.Hour, rangeStart, rangeEnd, MaxRecurrenceInstances, tt.resolve, nil)
			if err != nil {
				t.Fatalf("RecurringBusyPeriods() err = %v", err)
			}
			busy := make([]time.Time, 0, len(periods))
			for _, period := range periods {
				busy = append(busy, period.Start)
			}
			sortTimes(busy)
			if !sameInstants(busy, tt.want) {
				t.Errorf("RecurringBusyPeriods() = %v, want %v", utcStrings(busy), utcStrings(tt.want))
			}

			slots, err := RecurrenceSlots(raw, "VEVENT", dtstart, time.Hour, rangeStart, rangeEnd, MaxRecurrenceInstances, tt.resolve, nil)
			if err != nil {
				t.Fatalf("RecurrenceSlots() err = %v", err)
			}
			slotStarts := make([]time.Time, 0, len(slots))
			for _, slot := range slots {
				slotStarts = append(slotStarts, slot.RecurrenceID)
			}
			sortTimes(slotStarts)
			if !sameInstants(slotStarts, tt.want) {
				t.Errorf("RecurrenceSlots() = %v, want %v", utcStrings(slotStarts), utcStrings(tt.want))
			}
		})
	}
}

// A counted sub-daily series is fast-forwarded to a far range by counting the
// periods it passes over, one instance each. A skipped hour among them is no
// instance, so the count would come up one short and the series end an hour
// early.
func TestCountedSubDailyFastForwardDoesNotCountASkippedHour(t *testing.T) {
	requireHostZone(t, "Europe/Berlin")
	raw := recurringObject("DTSTART;TZID=Europe/Berlin:20260329T003000", "RRULE:FREQ=HOURLY;COUNT=60")
	dtstart, _ := ParsePropertyDateTimeLocal("DTSTART;TZID=Europe/Berlin", "20260329T003000")
	got := instanceStarts(requireRecurrenceResult[RecurrenceInstance](t)(collectRecurrenceInstances(raw, "VEVENT", dtstart, time.Hour,
		time.Date(2026, 3, 31, 10, 31, 0, 0, time.UTC), time.Date(2026, 3, 31, 18, 0, 0, 0, time.UTC), MaxRecurrenceInstances, nil)))
	// The sixtieth instance is 12:30 CEST on the 31st: sixty hours on from
	// 00:30 on the 29th, one of them the 02:30 the change skips.
	want := utcInstants("2026-03-31T10:30:00Z")
	if !sameInstants(got, want) {
		t.Errorf("instances = %v, want %v", utcStrings(got), utcStrings(want))
	}
}

// periodWalkResolver reads dates as resolver does but cannot list a zone's
// gaps, so an expansion through it walks a counted sub-daily rule one period at
// a time.
type periodWalkResolver struct {
	RecurrenceTimeResolver
}

// A counted sub-daily rule in a zone with transitions is jumped over the
// periods before the range rather than generated from its DTSTART, and the
// jump leaves out exactly the instances the period-by-period walk does: the
// candidates on local times a spring change skips, and none of those a fall
// change repeats.
func TestCountedSubDailyFastForwardMatchesThePeriodWalkAcrossTransitions(t *testing.T) {
	type series struct {
		tzid    string
		dtstart string
		rrule   string
	}
	var cases []series
	add := func(tzid, dtstart string, rrules ...string) {
		requireHostZone(t, tzid)
		for _, rrule := range rrules {
			cases = append(cases, series{tzid: tzid, dtstart: dtstart, rrule: rrule})
		}
	}
	// counted gives each fixed-step frequency the COUNT that runs it for
	// wallHours of wall-clock periods, so its last instance falls in one of
	// the ranges tested below, past where the jump begins.
	counted := func(wallHours int) []string {
		var rrules []string
		for _, step := range []struct {
			freq     string
			interval int
			seconds  int
		}{{"SECONDLY", 37, 37}, {"MINUTELY", 1, 60}, {"MINUTELY", 7, 420}, {"MINUTELY", 45, 2700}, {"HOURLY", 1, 3600}, {"HOURLY", 5, 18000}} {
			rrules = append(rrules, fmt.Sprintf("FREQ=%s;INTERVAL=%d;COUNT=%d", step.freq, step.interval, wallHours*3600/step.seconds+1))
		}
		return rrules
	}
	until := func(until string) []string {
		return []string{
			"FREQ=SECONDLY;INTERVAL=37;UNTIL=" + until,
			"FREQ=MINUTELY;INTERVAL=7;UNTIL=" + until,
			"FREQ=HOURLY;UNTIL=" + until,
		}
	}
	// Spring gap and fall overlap of an hour, a half-hour transition each
	// way, a gap at midnight, and the day Samoa skipped.
	add("America/New_York", "20260308T004513", counted(31)...)
	add("America/New_York", "20260308T004513", until("20260309T120000Z")...)
	add("America/New_York", "20260308T015900", "FREQ=SECONDLY;COUNT=96000", "FREQ=SECONDLY;UNTIL=20260309T080000Z")
	add("America/New_York", "20261101T003000", counted(31)...)
	add("America/New_York", "20261101T003000", until("20261102T120000Z")...)
	add("America/New_York", "20261101T003000", "FREQ=SECONDLY;COUNT=93000", "FREQ=SECONDLY;UNTIL=20261102T060000Z")
	add("Australia/Lord_Howe", "20261004T013000", counted(31)...)
	add("Australia/Lord_Howe", "20260405T010000", counted(31)...)
	add("America/Sao_Paulo", "20171014T220000", counted(31)...)
	add("Pacific/Apia", "20111229T200000", counted(55)...)
	// A DTSTART inside the gap is an instance whatever it names.
	add("America/New_York", "20260308T023000", "FREQ=MINUTELY;INTERVAL=10;COUNT=186", "FREQ=HOURLY;COUNT=31")
	// A step that does not divide the gap, from a DTSTART inside it: the
	// candidates past the gap that read before the DTSTART are no instances.
	add("America/New_York", "20260308T025000", "FREQ=MINUTELY;INTERVAL=40;COUNT=55", "FREQ=MINUTELY;INTERVAL=7;COUNT=400")

	for _, tt := range cases {
		t.Run(tt.tzid+"/"+tt.dtstart+"/"+tt.rrule, func(t *testing.T) {
			raw := recurringObject("DTSTART;TZID="+tt.tzid+":"+tt.dtstart, "RRULE:"+tt.rrule)
			dtstart, ok := ParsePropertyDateTimeLocal("DTSTART;TZID="+tt.tzid, tt.dtstart)
			if !ok {
				t.Fatal("DTSTART did not resolve")
			}
			expand := func(resolve RecurrenceTimeResolver, rangeStart, rangeEnd time.Time) []time.Time {
				instances, err := collectRecurrenceInstances(raw, "VEVENT", dtstart, time.Minute, rangeStart, rangeEnd, MaxRecurrenceInstances*100, resolve)
				if err != nil {
					t.Fatalf("expansion over [%v, %v): %v", rangeStart, rangeEnd, err)
				}
				return instanceStarts(instances)
			}
			// The scan of a zoned series is widened by a day, so only a range
			// starting more than a day past the DTSTART is jumped toward.
			jumpedOver := false
			for _, offset := range []time.Duration{0, 3 * time.Hour, 25 * time.Hour, 30 * time.Hour, 50 * time.Hour} {
				rangeStart := dtstart.Add(offset)
				rangeEnd := rangeStart.Add(6 * time.Hour)
				jumped := expand(nil, rangeStart, rangeEnd)
				walked := expand(periodWalkResolver{PropertyTimeResolver(nil)}, rangeStart, rangeEnd)
				if !sameInstants(jumped, walked) {
					t.Fatalf("range from %v: jumped expansion has %d instances, period walk %d\njumped: %v\nwalked: %v",
						rangeStart.UTC(), len(jumped), len(walked), utcStrings(jumped), utcStrings(walked))
				}
				jumpedOver = jumpedOver || (offset > 24*time.Hour && len(walked) > 0)
			}
			if !jumpedOver {
				t.Error("no range the expansion jumps toward holds an instance; the case tests nothing")
			}
		})
	}
}

// Testing a rule's candidates against the zone's listed gaps, and asking only
// those inside one whether it exists, leaves out exactly the candidates asking
// about every one of them does -- before BYSETPOS numbers a period as much as
// for a rule without it.
func TestGapListedExistenceMatchesAskingEveryCandidate(t *testing.T) {
	rrules := []string{
		"FREQ=DAILY;COUNT=20",
		"FREQ=DAILY;BYHOUR=1,2,3;BYMINUTE=0,30;COUNT=60",
		"FREQ=WEEKLY;BYDAY=SU;BYHOUR=2;BYMINUTE=15;COUNT=40",
		"FREQ=MONTHLY;BYDAY=SU;BYHOUR=1,2;BYMINUTE=30;BYSETPOS=3,-1;COUNT=30",
		"FREQ=YEARLY;BYMONTH=3,4,10,11;BYDAY=1SU,2SU;BYHOUR=0,1,2,3;BYMINUTE=30;COUNT=40",
		"FREQ=HOURLY;BYMINUTE=0,30;COUNT=300",
		"FREQ=MINUTELY;INTERVAL=20;BYSECOND=0,30;COUNT=3000",
	}
	for _, series := range []struct{ tzid, dtstart string }{
		{"America/New_York", "20260301T023000"},
		{"America/New_York", "20261025T013000"},
		{"Australia/Lord_Howe", "20260927T020000"},
		{"Pacific/Apia", "20111225T000000"},
	} {
		requireHostZone(t, series.tzid)
		for _, rrule := range rrules {
			t.Run(series.tzid+"/"+series.dtstart+"/"+rrule, func(t *testing.T) {
				raw := recurringObject("DTSTART;TZID="+series.tzid+":"+series.dtstart, "RRULE:"+rrule)
				dtstart, ok := ParsePropertyDateTimeLocal("DTSTART;TZID="+series.tzid, series.dtstart)
				if !ok {
					t.Fatal("DTSTART did not resolve")
				}
				rangeStart, rangeEnd := dtstart.AddDate(0, 0, -1), dtstart.AddDate(3, 0, 0)
				expand := func(resolve RecurrenceTimeResolver) []time.Time {
					instances, err := collectRecurrenceInstances(raw, "VEVENT", dtstart, time.Minute, rangeStart, rangeEnd, MaxRecurrenceInstances*100, resolve)
					if err != nil {
						t.Fatal(err)
					}
					return instanceStarts(instances)
				}
				listed, asked := expand(nil), expand(periodWalkResolver{PropertyTimeResolver(nil)})
				if !sameInstants(listed, asked) {
					t.Fatalf("gap-listed expansion has %d instances, asking every candidate %d\nlisted: %v\nasked: %v",
						len(listed), len(asked), utcStrings(listed), utcStrings(asked))
				}
			})
		}
	}
}

// The jump over a counted minutely rule's periods in New York's spring change
// counts the sixty minutes the change skips out of the instances it passes.
func TestCountedSubDailyFastForwardJumpsAZonedRule(t *testing.T) {
	newYork := requireHostZone(t, "America/New_York")
	rule, ok := parseRecurrenceRule("FREQ=MINUTELY;COUNT=100000", time.UTC, nil, nil)
	if !ok {
		t.Fatal("rule did not parse")
	}
	rule.localTimeExists = func(wall time.Time) bool { return WallClockExists(newYork, wall) }
	dtstart := time.Date(2026, 3, 8, 1, 0, 0, 0, time.UTC)
	threshold := time.Date(2026, 3, 8, 4, 0, 0, 0, time.UTC)
	got, passed, ok := fastForwardCountedSubDailyRecurrence(dtstart, threshold, dtstart, rule, rule.Count, hostLocalTimeGaps(newYork))
	if !ok {
		t.Fatal("fast-forward refused a minutely rule in a host zone")
	}
	if !got.Equal(threshold) || passed != 120 {
		t.Errorf("fast-forward = %v, %d instances; want %v, 120", got, passed, threshold)
	}
}

// The validator counts CALDAV:max-instances on the calendar of the series'
// zone the same way the expansion counts the set: a daily 02:30 series
// through the Berlin spring change has six instances in six days, its 29
// March one read at the offset before the gap, whether the zone is the
// host's or one only the object defines.
func TestRecurrenceSetExceedsLimitCountsTheSkippedLocalTimeAsAnInstance(t *testing.T) {
	berlin := requireHostZone(t, "Europe/Berlin")
	tests := []struct {
		name    string
		raw     string
		resolve RecurrenceTimeResolver
	}{
		{
			name: "host zone",
			raw:  recurringObject("DTSTART;TZID=Europe/Berlin:20260326T023000", "RRULE:FREQ=DAILY;UNTIL=20260331T235959Z"),
		},
		{
			name: "zone only the object defines",
			raw: strings.Replace(recurringObject("DTSTART;TZID=Private/Berlin:20260326T023000", "RRULE:FREQ=DAILY;UNTIL=20260331T235959Z"),
				"BEGIN:VEVENT", strings.TrimSuffix(privateBerlinTimezone, "\r\n")+"\r\nBEGIN:VEVENT", 1),
			resolve: privateZoneResolver{tzid: "Private/Berlin", loc: berlin},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			master := PrimaryVEventComponent(tt.raw)
			dtstartProperty, _ := ComponentProperty(master, "DTSTART")
			resolver := tt.resolve
			if resolver == nil {
				resolver = PropertyTimeResolver(nil)
			}
			dtstart, _ := resolver.ResolvePropertyTime(dtstartProperty.KeyPart, dtstartProperty.Value)
			instances := requireRecurrenceResult[RecurrenceInstance](t)(collectRecurrenceInstances(tt.raw, "VEVENT", dtstart, time.Hour,
				time.Date(2026, 3, 1, 0, 0, 0, 0, time.UTC), time.Date(2026, 5, 1, 0, 0, 0, 0, time.UTC), MaxRecurrenceInstances, tt.resolve))
			if len(instances) != 6 {
				t.Fatalf("expansion = %v, want six instances", utcStrings(instanceStarts(instances)))
			}
			if exceeds, valid := RecurrenceSetExceedsLimit(tt.raw, 6); !valid || exceeds {
				t.Errorf("RecurrenceSetExceedsLimit(raw, 6) = %v, %v; want false, true", exceeds, valid)
			}
			if exceeds, valid := RecurrenceSetExceedsLimit(tt.raw, 5); !valid || !exceeds {
				t.Errorf("RecurrenceSetExceedsLimit(raw, 5) = %v, %v; want true, true", exceeds, valid)
			}
		})
	}
}

// The validator counts a skipped candidate that names the instant of an
// existing one once, and counts a DTSTART the rule generates once, as the
// expansion does: COUNT=6 and COUNT=3 below are exactly six and three
// instances, so a limit one below each is exceeded and the count itself is
// not.
func TestRecurrenceSetExceedsLimitCountsMergedGapCandidatesAsTheExpansionDoes(t *testing.T) {
	requireHostZone(t, "America/New_York")
	requireHostZone(t, "Australia/Lord_Howe")
	tests := []struct {
		name      string
		raw       string
		instances int
	}{
		{
			name:      "skipped candidate merged into a later existing time",
			raw:       recurringObject("DTSTART;TZID=America/New_York:20260308T031500", "RRULE:FREQ=DAILY;BYHOUR=2,3;BYMINUTE=15,45;COUNT=6"),
			instances: 6,
		},
		{
			name:      "DTSTART on a skipped time the rule generates",
			raw:       recurringObject("DTSTART;TZID=Australia/Lord_Howe:20261004T021500", "RRULE:FREQ=DAILY;BYHOUR=2;BYMINUTE=15,30;COUNT=3"),
			instances: 3,
		},
		{
			name:      "skipped candidate before the DTSTART's wall clock but after its instant",
			raw:       recurringObject("DTSTART;TZID=America/New_York:20260308T031500", "RRULE:FREQ=DAILY;BYHOUR=2;BYMINUTE=45;COUNT=3"),
			instances: 3,
		},
		{
			name:      "skipped candidate whose instant precedes the DTSTART's",
			raw:       recurringObject("DTSTART;TZID=America/New_York:20260308T030500", "RRULE:FREQ=DAILY;BYHOUR=2;BYMINUTE=0;COUNT=3"),
			instances: 3,
		},
		{
			name:      "skipped candidate whose instant follows the DTSTART's",
			raw:       recurringObject("DTSTART;TZID=America/New_York:20260308T030500", "RRULE:FREQ=DAILY;BYHOUR=2;BYMINUTE=30;COUNT=3"),
			instances: 3,
		},
		{
			name:      "weekly skipped candidate following the DTSTART's instant",
			raw:       recurringObject("DTSTART;TZID=America/New_York:20260308T031500", "RRULE:FREQ=WEEKLY;BYDAY=SU;BYHOUR=2,3;BYMINUTE=45;COUNT=3"),
			instances: 3,
		},
		{
			name:      "hourly DTSTART on a skipped time the rule generates",
			raw:       recurringObject("DTSTART;TZID=Australia/Lord_Howe:20261004T021500", "RRULE:FREQ=HOURLY;BYMINUTE=15,30;COUNT=3"),
			instances: 3,
		},
		{
			name:      "skipped candidate reading the DTSTART's instant",
			raw:       recurringObject("DTSTART;TZID=America/New_York:20260308T034500", "RRULE:FREQ=DAILY;BYHOUR=2;BYMINUTE=45;COUNT=3"),
			instances: 3,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if exceeds, valid := RecurrenceSetExceedsLimit(tt.raw, tt.instances); !valid || exceeds {
				t.Errorf("RecurrenceSetExceedsLimit(raw, %d) = %v, %v; want false, true", tt.instances, exceeds, valid)
			}
			if exceeds, valid := RecurrenceSetExceedsLimit(tt.raw, tt.instances-1); !valid || !exceeds {
				t.Errorf("RecurrenceSetExceedsLimit(raw, %d) = %v, %v; want true, true", tt.instances-1, exceeds, valid)
			}
		})
	}
}

// The validator counts the instances the expansion generates, on the same wall
// clocks and resolved to the same instants, in a zone the host defines as in
// one the object does: a series starting on a local time a spring change skips
// keeps that time of day on the days after it, and a time a fall change
// repeats names its first occurrence (RFC 5545 §3.3.5). An override naming
// each expanded instance by its UTC instant then replaces it rather than
// adding one, so the set holds exactly as many instances as the expansion.
func TestRecurrenceSetExceedsLimitCountsTheInstantsTheExpansionGenerates(t *testing.T) {
	tests := []struct {
		name    string
		dtstart string
		rrule   string
	}{
		{"New York, DTSTART on the skipped hour", "DTSTART;TZID=America/New_York:20240310T023000", "FREQ=DAILY;COUNT=5"},
		{"New York, daily through the skipped hour", "DTSTART;TZID=America/New_York:20240308T023000", "FREQ=DAILY;COUNT=5"},
		{"New York, hourly through the skipped hour", "DTSTART;TZID=America/New_York:20240310T003000", "FREQ=HOURLY;COUNT=5"},
		{"New York, DTSTART on the repeated hour", "DTSTART;TZID=America/New_York:20241103T013000", "FREQ=DAILY;COUNT=5"},
		{"New York, daily through the repeated hour", "DTSTART;TZID=America/New_York:20241101T013000", "FREQ=DAILY;UNTIL=20241105T235959Z"},
		{"New York, hourly through the repeated hour", "DTSTART;TZID=America/New_York:20241103T003000", "FREQ=HOURLY;COUNT=5"},
		// The candidates just before, inside and after the gap read 07:00Z,
		// 07:45Z and 07:30Z: the interval's step does not divide the gap's
		// length, so none of the three collide, but the one past the gap
		// reads earlier than the one inside it.
		{"New York, a 45-minute interval crossing the skipped hour out of wall order", "DTSTART;TZID=America/New_York:20260307T221500", "FREQ=MINUTELY;INTERVAL=45;COUNT=40"},
		{"Berlin, DTSTART on the skipped hour", "DTSTART;TZID=Europe/Berlin:20240331T023000", "FREQ=DAILY;COUNT=5"},
		{"Berlin, weekly through the skipped hour", "DTSTART;TZID=Europe/Berlin:20240317T023000", "FREQ=WEEKLY;COUNT=5"},
		{"Berlin, hourly through the skipped hour", "DTSTART;TZID=Europe/Berlin:20240331T003000", "FREQ=HOURLY;COUNT=5"},
		{"Berlin, DTSTART on the repeated hour", "DTSTART;TZID=Europe/Berlin:20241027T023000", "FREQ=DAILY;COUNT=5"},
		{"Berlin, daily through the repeated hour", "DTSTART;TZID=Europe/Berlin:20241025T023000", "FREQ=DAILY;COUNT=5"},
		{"Berlin, hourly through the repeated hour", "DTSTART;TZID=Europe/Berlin:20241027T003000", "FREQ=HOURLY;UNTIL=20241027T040000Z"},
		// Samoa skipped the whole of 30 December 2011.
		{"Apia, daily through the skipped day", "DTSTART;TZID=Pacific/Apia:20111227T100000", "FREQ=DAILY;COUNT=6"},
		{"Apia, hourly through the skipped day", "DTSTART;TZID=Pacific/Apia:20111229T200000", "FREQ=HOURLY;COUNT=56"},
		// Lord Howe Island's clocks move by half an hour: 02:00 to 02:30 in
		// October, and 02:00 back to 01:30 in April.
		{"Lord Howe, quarter hours of 02:00 through the skipped half hour", "DTSTART;TZID=Australia/Lord_Howe:20241006T020000", "FREQ=DAILY;BYHOUR=2;BYMINUTE=0,15,30,45;COUNT=12"},
		{"Lord Howe, every 15 minutes through the repeated half hour", "DTSTART;TZID=Australia/Lord_Howe:20240407T010000", "FREQ=MINUTELY;INTERVAL=15;COUNT=20"},
		// BYSETPOS must number the period's candidates by the instant each
		// reads, not by wall clock: 20 minutes past the gap reads earlier
		// than 0 and 40 minutes do.
		{"Lord Howe, BYSETPOS over a gap day's reordered candidates", "DTSTART;TZID=Australia/Lord_Howe:20261003T020000", "FREQ=DAILY;BYHOUR=2;BYMINUTE=0,20,40;BYSETPOS=2;COUNT=10"},
		// Havana's clocks skip from midnight to 01:00 on 10 March 2024.
		{"Havana, daily at midnight through the skipped hour", "DTSTART;TZID=America/Havana:20240308T000000", "FREQ=DAILY;COUNT=5"},
		// A DTSTART on a skipped time that the rule does not generate itself is
		// still the first instance, and the candidate after the gap that reads
		// its instant is the same instance, not a second one.
		{"New York, DTSTART on the skipped hour of a rule generating 03:30", "DTSTART;TZID=America/New_York:20260308T023000", "FREQ=DAILY;BYHOUR=3;BYMINUTE=30;COUNT=3"},
		{"Santiago, DTSTART on the skipped hour of a rule generating 01:00 to 04:30", "DTSTART;TZID=America/Santiago:20260906T003000", "FREQ=DAILY;BYHOUR=1,3,4;BYMINUTE=0,30;COUNT=8"},
		{"Lord Howe, DTSTART on the skipped half hour of an hourly rule", "DTSTART;TZID=Australia/Lord_Howe:20261004T021000", "FREQ=HOURLY;BYMINUTE=0,20,40;COUNT=9"},
		{"Apia, DTSTART on the skipped day of a weekly BYSETPOS rule", "DTSTART;TZID=Pacific/Apia:20111230T024000", "FREQ=WEEKLY;BYDAY=MO,TU,WE,TH,FR,SA,SU;BYSETPOS=-2;COUNT=6"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			raw := recurringObject(tt.dtstart, "RRULE:"+tt.rrule)
			keyPart, value, _ := SplitContentLine(tt.dtstart)
			tzid, _ := PropertyParam(keyPart, "TZID")
			requireHostZone(t, tzid)
			dtstart, ok := PropertyTimeResolver(nil).ResolvePropertyTime(keyPart, value)
			if !ok {
				t.Fatalf("cannot read %s", tt.dtstart)
			}
			expanded := instanceStarts(requireRecurrenceResult[RecurrenceInstance](t)(collectRecurrenceInstances(raw, "VEVENT", dtstart, time.Hour,
				dtstart.AddDate(0, 0, -1), dtstart.AddDate(1, 0, 0), MaxRecurrenceInstances, nil)))
			n := len(expanded)
			if exceeds, valid := RecurrenceSetExceedsLimit(raw, n); !valid || exceeds {
				t.Errorf("RecurrenceSetExceedsLimit(raw, %d) = %v, %v; want false, true for the expansion's %v", n, exceeds, valid, utcStrings(expanded))
			}
			if exceeds, valid := RecurrenceSetExceedsLimit(raw, n-1); !valid || !exceeds {
				t.Errorf("RecurrenceSetExceedsLimit(raw, %d) = %v, %v; want true, true for the expansion's %v", n-1, exceeds, valid, utcStrings(expanded))
			}
			overridden := strings.TrimSuffix(raw, "END:VCALENDAR\r\n")
			for _, start := range expanded {
				instant := start.UTC().Format("20060102T150405Z")
				overridden += "BEGIN:VEVENT\r\nUID:series\r\nRECURRENCE-ID:" + instant + "\r\nDTSTART:" + instant + "\r\nSUMMARY:moved\r\nEND:VEVENT\r\n"
			}
			overridden += "END:VCALENDAR\r\n"
			if exceeds, valid := RecurrenceSetExceedsLimit(overridden, n); !valid || exceeds {
				t.Errorf("with every instance of %v overridden, RecurrenceSetExceedsLimit(raw, %d) = %v, %v; want false, true", utcStrings(expanded), n, exceeds, valid)
			}
		})
	}
}

// An object shipping its own VTIMEZONE under a name the host also defines is
// read through its own definition, as the DAV layer's matcher reads it, so the
// validator counts the instances the expansion produces: the skipped local
// times are the object's, and an EXDATE naming one of its wall clocks removes
// that instance.
func TestRecurrenceSetExceedsLimitReadsTheObjectsOwnDefinitionOfAHostZone(t *testing.T) {
	requireHostZone(t, "America/New_York")
	withTimezone := func(timezone string, lines ...string) string {
		return strings.Replace(recurringObject(lines...), "BEGIN:VEVENT", timezone+"BEGIN:VEVENT", 1)
	}
	const fixedOffset = "BEGIN:VTIMEZONE\r\nTZID:America/New_York\r\n" +
		"BEGIN:STANDARD\r\nDTSTART:19700101T000000\r\nTZOFFSETFROM:-0500\r\nTZOFFSETTO:-0500\r\nEND:STANDARD\r\n" +
		"END:VTIMEZONE\r\n"
	const earlySpring = "BEGIN:VTIMEZONE\r\nTZID:America/New_York\r\n" +
		"BEGIN:STANDARD\r\nDTSTART:19700101T000000\r\nTZOFFSETFROM:-0500\r\nTZOFFSETTO:-0500\r\nEND:STANDARD\r\n" +
		"BEGIN:DAYLIGHT\r\nDTSTART:20260301T020000\r\nTZOFFSETFROM:-0500\r\nTZOFFSETTO:-0400\r\nEND:DAYLIGHT\r\n" +
		"END:VTIMEZONE\r\n"
	tests := []struct {
		name string
		raw  string
	}{
		{
			// The host skips 02:30 on 8 March; this definition does not, so
			// the EXDATE removes one of the five instances.
			name: "the object skips nothing the host skips",
			raw: withTimezone(fixedOffset, "DTSTART;TZID=America/New_York:20260306T023000",
				"RRULE:FREQ=DAILY;COUNT=5", "EXDATE;TZID=America/New_York:20260308T023000"),
		},
		{
			// This definition's onset falls on 1 March; the object's own
			// reading of it keeps that instance, read at the offset before
			// the gap, rather than skipping it, so COUNT's fifth instance is
			// 3 March, where the EXDATE removes it.
			name: "the object skips a time the host does not",
			raw: withTimezone(earlySpring, "DTSTART;TZID=America/New_York:20260227T023000",
				"RRULE:FREQ=DAILY;COUNT=5", "EXDATE;TZID=America/New_York:20260303T023000"),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if exceeds, valid := RecurrenceSetExceedsLimit(tt.raw, 4); !valid || exceeds {
				t.Errorf("RecurrenceSetExceedsLimit(raw, 4) = %v, %v; want false, true", exceeds, valid)
			}
			if exceeds, valid := RecurrenceSetExceedsLimit(tt.raw, 3); !valid || !exceeds {
				t.Errorf("RecurrenceSetExceedsLimit(raw, 3) = %v, %v; want true, true", exceeds, valid)
			}
		})
	}
}

// A series in a zone the object defines is counted as the instants its wall
// clocks name, so an EXDATE or UNTIL written in UTC, as clients write them,
// removes or ends the instance it names. The host's definition is shipped
// without HostDefinitionProperty, so it is read as the object's own.
func TestRecurrenceSetExceedsLimitCountsAShippedZonesInstantsAsUTCValuesNameThem(t *testing.T) {
	requireHostZone(t, "America/New_York")
	timezone, ok := VTimezone("America/New_York", time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC), time.Date(2026, 12, 31, 0, 0, 0, 0, time.UTC))
	if !ok {
		t.Fatal("no definition of America/New_York")
	}
	timezone = strings.Replace(timezone, HostDefinitionProperty+":TRUE\r\n", "", 1)
	for name, lines := range map[string][]string{
		// 14:00Z on 6 January is that day's 09:00 in New York.
		"an EXDATE in UTC": {"DTSTART;TZID=America/New_York:20260105T090000", "RRULE:FREQ=DAILY;COUNT=5", "EXDATE:20260106T140000Z"},
		// 13:30Z on 9 January is half an hour before that day's instance.
		"an UNTIL in UTC": {"DTSTART;TZID=America/New_York:20260105T090000", "RRULE:FREQ=DAILY;UNTIL=20260109T133000Z"},
	} {
		t.Run(name, func(t *testing.T) {
			raw := strings.Replace(recurringObject(lines...), "BEGIN:VEVENT", timezone+"BEGIN:VEVENT", 1)
			if exceeds, valid := RecurrenceSetExceedsLimit(raw, 4); !valid || exceeds {
				t.Errorf("RecurrenceSetExceedsLimit(raw, 4) = %v, %v; want false, true", exceeds, valid)
			}
			if exceeds, valid := RecurrenceSetExceedsLimit(raw, 3); !valid || !exceeds {
				t.Errorf("RecurrenceSetExceedsLimit(raw, 3) = %v, %v; want true, true", exceeds, valid)
			}
		})
	}
}

// An override naming the slot of an instance a transition skips names the
// slot the master generates there, read at the offset before the gap
// (RFC 5545 §3.3.5): it replaces that instance exactly as an override of an
// ordinary slot does, rather than standing alone.
func TestOverrideOfASkippedInstanceReplacesTheGeneratedSlot(t *testing.T) {
	requireHostZone(t, "Europe/Berlin")
	raw := "BEGIN:VCALENDAR\r\n" +
		"BEGIN:VEVENT\r\nUID:series\r\nDTSTART;TZID=Europe/Berlin:20260322T023000\r\nRRULE:FREQ=WEEKLY;COUNT=2\r\nEND:VEVENT\r\n" +
		"BEGIN:VEVENT\r\nUID:series\r\nRECURRENCE-ID;TZID=Europe/Berlin:20260329T023000\r\nDTSTART;TZID=Europe/Berlin:20260329T100000\r\nEND:VEVENT\r\n" +
		"END:VCALENDAR\r\n"
	dtstart, _ := ParsePropertyDateTimeLocal("DTSTART;TZID=Europe/Berlin", "20260322T023000")
	rangeStart, rangeEnd := time.Date(2026, 3, 1, 0, 0, 0, 0, time.UTC), time.Date(2026, 5, 1, 0, 0, 0, 0, time.UTC)

	periods, err := RecurringBusyPeriods(raw, dtstart, time.Hour, rangeStart, rangeEnd, MaxRecurrenceInstances, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	busy := make([]time.Time, 0, len(periods))
	for _, period := range periods {
		busy = append(busy, period.Start)
	}
	sortTimes(busy)
	// COUNT=2 is satisfied by 22 and 29 March; the override moves the second
	// of those rather than adding a third.
	if want := utcInstants("2026-03-22T01:30:00Z", "2026-03-29T08:00:00Z"); !sameInstants(busy, want) {
		t.Errorf("RecurringBusyPeriods() = %v, want %v", utcStrings(busy), utcStrings(want))
	}

	slots, err := RecurrenceSlots(raw, "VEVENT", dtstart, time.Hour, rangeStart, rangeEnd, MaxRecurrenceInstances, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	skipped := utcInstants("2026-03-29T01:30:00Z")[0]
	found := false
	for _, slot := range slots {
		if !slot.RecurrenceID.Equal(skipped) {
			continue
		}
		found = true
		if !slot.ExactOverride {
			t.Errorf("slot %v: the override of the generated slot should replace it", slot)
		}
	}
	if !found {
		t.Error("the master's generated slot for 29 March is missing")
	}
	if len(slots) != 2 {
		t.Errorf("RecurrenceSlots() = %d slots, want 2", len(slots))
	}
}

// A rule whose only candidate every period is a local time the zone skips --
// the last Sunday of March at 02:30 in Berlin, which is exactly the hour the
// clocks jump -- generates that instance every period, read at the offset
// before the gap: a COUNT of two reaches it as the second instance. Each entry
// point has to decide that within the scan budgets, the way it decides any
// other rule, rather than follow it for ever.
func TestRuleWhoseOnlyCandidateIsASkippedLocalTimeStillTerminates(t *testing.T) {
	requireHostZone(t, "Europe/Berlin")
	dtstart, _ := ParsePropertyDateTimeLocal("DTSTART;TZID=Europe/Berlin", "20260301T023000")
	rangeStart := RecurrenceStartSentinel
	rangeEnd := time.Date(9999, 12, 31, 23, 59, 59, 0, time.UTC)
	want := utcInstants("2026-03-01T01:30:00Z", "2026-03-29T01:30:00Z")
	for _, rrule := range []string{
		"FREQ=YEARLY;BYMONTH=3;BYDAY=-1SU;BYHOUR=2;BYMINUTE=30;COUNT=2",
		"FREQ=MONTHLY;BYMONTH=3;BYDAY=-1SU;BYHOUR=2;COUNT=2",
		"FREQ=DAILY;BYMONTH=3;BYDAY=SU;BYMONTHDAY=25,26,27,28,29,30,31;BYHOUR=2;COUNT=2",
	} {
		t.Run(rrule, func(t *testing.T) {
			raw := recurringObject("DTSTART;TZID=Europe/Berlin:20260301T023000", "RRULE:"+rrule)
			finishesWithin(t, 2*time.Second, func() string {
				instances, err := collectRecurrenceInstances(raw, "VEVENT", dtstart, time.Hour, rangeStart, rangeEnd, MaxRecurrenceInstances, nil)
				if err != nil {
					return "VisitRecurrenceInstances() err = " + err.Error()
				}
				if starts := instanceStarts(instances); !sameInstants(starts, want) {
					return "VisitRecurrenceInstances() = " + strings.Join(utcStrings(starts), ",") + ", want " + strings.Join(utcStrings(want), ",")
				}
				if exceeds, valid := RecurrenceSetExceedsLimit(raw, MaxRecurrenceInstances); !valid || exceeds {
					return "RecurrenceSetExceedsLimit() refused the set"
				}
				return ""
			})
		})
	}
}

// The same rule without a COUNT is an ordinary unbounded yearly series,
// generating one instance a year: a request spanning every year
// CALDAV:max-date-time admits terminates within the scan budgets, failing
// closed on the output limit rather than hanging. The validator
// measures an unbounded rule over one year from DTSTART regardless, which
// this reaches within -- it is not what fails closed here.
func TestUnboundedRuleWhoseOnlyCandidateIsASkippedLocalTimeTerminates(t *testing.T) {
	requireHostZone(t, "Europe/Berlin")
	dtstart, _ := ParsePropertyDateTimeLocal("DTSTART;TZID=Europe/Berlin", "20260301T023000")
	raw := recurringObject("DTSTART;TZID=Europe/Berlin:20260301T023000", "RRULE:FREQ=YEARLY;BYMONTH=3;BYDAY=-1SU;BYHOUR=2;BYMINUTE=30")
	finishesWithin(t, 2*time.Second, func() string {
		_, err := collectRecurrenceInstances(raw, "VEVENT", dtstart, time.Hour,
			RecurrenceStartSentinel, time.Date(9999, 12, 31, 23, 59, 59, 0, time.UTC), MaxRecurrenceInstances, nil)
		if !errors.Is(err, ErrRecurrenceExpansionLimit) {
			return fmt.Sprintf("VisitRecurrenceInstances() err = %v, want ErrRecurrenceExpansionLimit", err)
		}
		return ""
	})
}

const privateBerlinTimezone = "BEGIN:VTIMEZONE\r\n" +
	"TZID:Private/Berlin\r\n" +
	"BEGIN:DAYLIGHT\r\n" +
	"TZOFFSETFROM:+0100\r\n" +
	"TZOFFSETTO:+0200\r\n" +
	"DTSTART:19810329T020000\r\n" +
	"RRULE:FREQ=YEARLY;BYMONTH=3;BYDAY=-1SU\r\n" +
	"END:DAYLIGHT\r\n" +
	"BEGIN:STANDARD\r\n" +
	"TZOFFSETFROM:+0200\r\n" +
	"TZOFFSETTO:+0100\r\n" +
	"DTSTART:19961027T030000\r\n" +
	"RRULE:FREQ=YEARLY;BYMONTH=10;BYDAY=-1SU\r\n" +
	"END:STANDARD\r\n" +
	"END:VTIMEZONE\r\n"

// Skipping an instance carries a COUNT series' last one a step further, so the
// recurrence_until a stored row is prefiltered on has to reach it: a bound
// stopping at the COUNT-th generated start drops the row from every range
// holding only the final instance. The skips come from the definition the
// expansion reads the series through -- the VTIMEZONE the object ships, else
// the host's zone of that name.
func TestRecurrenceBoundsCoverTheInstanceAfterASkippedLocalTime(t *testing.T) {
	requireHostZone(t, "Europe/Berlin")
	requireHostZone(t, "America/New_York")
	berlin := requireHostZone(t, "Europe/Berlin")
	tests := []struct {
		name     string
		raw      string
		resolve  RecurrenceTimeResolver
		maxSlack time.Duration
	}{
		{
			name: "weekly, host zone",
			raw: recurringObject("DTSTART;TZID=Europe/Berlin:20260315T023000", "DTEND;TZID=Europe/Berlin:20260315T033000",
				"RRULE:FREQ=WEEKLY;COUNT=4"),
			maxSlack: 2 * 24 * time.Hour,
		},
		{
			name: "weekly, zone only the object defines",
			raw: strings.Replace(recurringObject("DTSTART;TZID=Private/Berlin:20260315T023000", "DTEND;TZID=Private/Berlin:20260315T033000",
				"RRULE:FREQ=WEEKLY;COUNT=4"), "BEGIN:VEVENT", strings.TrimSuffix(privateBerlinTimezone, "\r\n")+"\r\nBEGIN:VEVENT", 1),
			resolve:  privateZoneResolver{tzid: "Private/Berlin", loc: berlin},
			maxSlack: 2 * 24 * time.Hour,
		},
		{
			name: "weekly, host zone the object also defines",
			raw: strings.Replace(recurringObject("DTSTART;TZID=Europe/Berlin:20260315T023000", "DTEND;TZID=Europe/Berlin:20260315T033000",
				"RRULE:FREQ=WEEKLY;COUNT=4"), "BEGIN:VEVENT", strings.TrimSuffix(strings.Replace(privateBerlinTimezone, "Private/Berlin", "Europe/Berlin", 1), "\r\n")+"\r\nBEGIN:VEVENT", 1),
			maxSlack: 2 * 24 * time.Hour,
		},
		{
			// A VTIMEZONE with no observance defines nothing, so the host's
			// zone of that name answers, as it does for the DAV layer.
			name: "weekly, host zone the object names without observances",
			raw: strings.Replace(recurringObject("DTSTART;TZID=Europe/Berlin:20260315T023000", "DTEND;TZID=Europe/Berlin:20260315T033000",
				"RRULE:FREQ=WEEKLY;COUNT=4"), "BEGIN:VEVENT", "BEGIN:VTIMEZONE\r\nTZID:Europe/Berlin\r\nEND:VTIMEZONE\r\nBEGIN:VEVENT", 1),
			maxSlack: 2 * 24 * time.Hour,
		},
		{
			name:     "daily over three spring changes, host zone",
			raw:      recurringObject("DTSTART;TZID=America/New_York:20260307T023000", "DURATION:PT1H", "RRULE:FREQ=DAILY;COUNT=800"),
			maxSlack: 5 * 24 * time.Hour,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			master := PrimaryVEventComponent(tt.raw)
			dtstartProperty, _ := ComponentProperty(master, "DTSTART")
			resolver := tt.resolve
			if resolver == nil {
				resolver = PropertyTimeResolver(nil)
			}
			dtstart, ok := resolver.ResolvePropertyTime(dtstartProperty.KeyPart, dtstartProperty.Value)
			if !ok {
				t.Fatal("DTSTART did not resolve")
			}
			periods, err := RecurringBusyPeriods(tt.raw, dtstart, time.Hour, RecurrenceStartSentinel, RecurrenceUntilSentinel, MaxRecurrenceInstances, tt.resolve, nil)
			if err != nil {
				t.Fatal(err)
			}
			var lastEnd time.Time
			for _, period := range periods {
				if period.End.After(lastEnd) {
					lastEnd = period.End
				}
			}

			bounds := ConservativeRecurrenceBounds(tt.raw)
			if bounds.UntilUnknown || bounds.Until == nil {
				t.Fatalf("Until unknown; want a bound past %v", lastEnd)
			}
			if bounds.Until.Before(lastEnd) {
				t.Errorf("Until = %v, before the last instance's end %v", bounds.Until.UTC(), lastEnd.UTC())
			}
			if slack := bounds.Until.Sub(lastEnd); slack > tt.maxSlack {
				t.Errorf("Until = %v, %v past the last instance's end %v", bounds.Until.UTC(), slack, lastEnd.UTC())
			}
		})
	}
}

// privateBerlinObject is a weekly Sunday 02:30 series of four from 15 March
// 2026 in Private/Berlin, defined by a VTIMEZONE holding the given DAYLIGHT
// observances and the usual STANDARD one.
func privateBerlinObject(daylight ...string) string {
	timezone := "BEGIN:VTIMEZONE\r\nTZID:Private/Berlin\r\n" + strings.Join(daylight, "") +
		"BEGIN:STANDARD\r\nTZOFFSETFROM:+0200\r\nTZOFFSETTO:+0100\r\nDTSTART:19961027T030000\r\n" +
		"RRULE:FREQ=YEARLY;BYMONTH=10;BYDAY=-1SU\r\nEND:STANDARD\r\nEND:VTIMEZONE"
	return strings.Replace(recurringObject("DTSTART;TZID=Private/Berlin:20260315T023000", "DTEND;TZID=Private/Berlin:20260315T033000",
		"RRULE:FREQ=WEEKLY;COUNT=4"), "BEGIN:VEVENT", timezone+"\r\nBEGIN:VEVENT", 1)
}

func daylightObservance(lines ...string) string {
	return "BEGIN:DAYLIGHT\r\n" + strings.Join(lines, "\r\n") + "\r\nEND:DAYLIGHT\r\n"
}

// However the object's definition states the transition at 29 March -- a rule
// ended by a UTC UNTIL and its successor, a counted rule, an RDATE -- the
// bound reaches the series' last instance: COUNT=4 is satisfied by 15, 22, 29
// March and 5 April, the 29th read at the offset before the gap, so the series
// ends on 5 April. A definition the bound
// cannot read leaves it unknown rather than assuming the zone skips nothing.
func TestRecurrenceBoundsReadEveryFormOfAnObservance(t *testing.T) {
	lastEnd := utcInstants("2026-04-05T01:30:00Z")[0]
	tests := []struct {
		name    string
		raw     string
		unknown bool
	}{
		{
			name: "a rule ended by a UTC UNTIL, and its successor",
			raw: privateBerlinObject(
				daylightObservance("TZOFFSETFROM:+0100", "TZOFFSETTO:+0200", "DTSTART:19810329T020000",
					"RRULE:FREQ=YEARLY;BYMONTH=3;BYDAY=-1SU;UNTIL=19950326T010000Z"),
				daylightObservance("TZOFFSETFROM:+0100", "TZOFFSETTO:+0200", "DTSTART:19960331T020000",
					"RRULE:FREQ=YEARLY;BYMONTH=3;BYDAY=-1SU")),
		},
		{
			// UNTIL names the moment of the 2026 onset itself, 02:00 in the
			// +01:00 the transition leaves.
			name: "a rule whose UTC UNTIL is its last onset",
			raw: privateBerlinObject(daylightObservance("TZOFFSETFROM:+0100", "TZOFFSETTO:+0200", "DTSTART:19810329T020000",
				"RRULE:FREQ=YEARLY;BYMONTH=3;BYDAY=-1SU;UNTIL=20260329T010000Z")),
		},
		{
			name: "a counted rule",
			raw: privateBerlinObject(daylightObservance("TZOFFSETFROM:+0100", "TZOFFSETTO:+0200", "DTSTART:19810329T020000",
				"RRULE:FREQ=YEARLY;BYMONTH=3;BYDAY=-1SU;COUNT=100")),
		},
		{
			name: "an RDATE",
			raw: privateBerlinObject(daylightObservance("TZOFFSETFROM:+0100", "TZOFFSETTO:+0200", "DTSTART:20250330T020000",
				"RDATE:20260329T020000,20270328T020000")),
		},
		{
			// The DAV layer reads a rule it cannot parse as generating no
			// onset, so the bound does: the DTSTART is the 2026 onset.
			name: "an onset stated by its DTSTART beside a rule that cannot be read",
			raw: privateBerlinObject(daylightObservance("TZOFFSETFROM:+0100", "TZOFFSETTO:+0200", "DTSTART:20260329T020000",
				"RRULE:FREQ=YEARLY;BYMONTH=13")),
		},
		{
			// RFC 5545 §3.3.14 does not admit -0000, so no zone can be
			// followed through the definition.
			name:    "an observance whose offset is -0000",
			raw:     privateBerlinObject(daylightObservance("TZOFFSETFROM:-0000", "TZOFFSETTO:+0200", "DTSTART:19810329T020000", "RRULE:FREQ=YEARLY;BYMONTH=3;BYDAY=-1SU")),
			unknown: true,
		},
		{
			name:    "an observance missing its TZOFFSETTO",
			raw:     privateBerlinObject(daylightObservance("TZOFFSETFROM:+0100", "DTSTART:19810329T020000", "RRULE:FREQ=YEARLY;BYMONTH=3;BYDAY=-1SU")),
			unknown: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			bounds := ConservativeRecurrenceBounds(tt.raw)
			if bounds.UntilUnknown != tt.unknown {
				t.Fatalf("UntilUnknown = %v, want %v", bounds.UntilUnknown, tt.unknown)
			}
			if tt.unknown {
				return
			}
			if bounds.Until == nil || bounds.Until.Before(lastEnd) {
				t.Fatalf("Until = %v, before the last instance's end %v", bounds.Until, lastEnd)
			}
			if slack := bounds.Until.Sub(lastEnd); slack > 2*24*time.Hour {
				t.Errorf("Until = %v, %v past the last instance's end", bounds.Until.UTC(), slack)
			}
		})
	}
}

// A TZID padded with white space inside its quotes -- a space Microsoft
// clients write, among others -- resolves, and finds its gaps, exactly as the
// unpadded spelling does: the expansion, the recurrence-limit count and the
// stored bound all read the same series, crossing the same spring gap, the
// same way under either spelling.
func TestPaddedTZIDCrossesAGapLikeTheUnpaddedSpelling(t *testing.T) {
	requireHostZone(t, "Europe/Berlin")
	padded := recurringObject(`DTSTART;TZID=" Europe/Berlin":20260315T023000`, "RRULE:FREQ=WEEKLY;COUNT=4")
	unpadded := recurringObject("DTSTART;TZID=Europe/Berlin:20260315T023000", "RRULE:FREQ=WEEKLY;COUNT=4")

	master := PrimaryVEventComponent(padded)
	dtstartProperty, _ := ComponentProperty(master, "DTSTART")
	dtstart, ok := PropertyTimeResolver(nil).ResolvePropertyTime(dtstartProperty.KeyPart, dtstartProperty.Value)
	if !ok {
		t.Fatal("padded DTSTART did not resolve")
	}
	rangeStart, rangeEnd := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC), time.Date(2027, 1, 1, 0, 0, 0, 0, time.UTC)
	want := utcInstants("2026-03-15T01:30:00Z", "2026-03-22T01:30:00Z", "2026-03-29T01:30:00Z", "2026-04-05T00:30:00Z")

	instances := instanceStarts(requireRecurrenceResult[RecurrenceInstance](t)(
		collectRecurrenceInstances(padded, "VEVENT", dtstart, time.Hour, rangeStart, rangeEnd, MaxRecurrenceInstances, nil)))
	sortTimes(instances)
	if !sameInstants(instances, want) {
		t.Errorf("padded TZID: VisitRecurrenceInstances() = %v, want %v", utcStrings(instances), utcStrings(want))
	}

	if exceeds, valid := RecurrenceSetExceedsLimit(padded, 4); !valid || exceeds {
		t.Errorf("padded TZID: RecurrenceSetExceedsLimit(raw, 4) = %v, %v; want false, true", exceeds, valid)
	}
	if exceeds, valid := RecurrenceSetExceedsLimit(padded, 3); !valid || !exceeds {
		t.Errorf("padded TZID: RecurrenceSetExceedsLimit(raw, 3) = %v, %v; want true, true", exceeds, valid)
	}

	boundsPadded := ConservativeRecurrenceBounds(padded)
	boundsUnpadded := ConservativeRecurrenceBounds(unpadded)
	if boundsPadded.UntilUnknown || boundsPadded.Until == nil {
		t.Fatal("padded TZID: Until unknown")
	}
	if !boundsPadded.Until.Equal(*boundsUnpadded.Until) {
		t.Errorf("padded TZID: Until = %v, want %v (the unpadded spelling's)", boundsPadded.Until.UTC(), boundsUnpadded.Until.UTC())
	}
}

// gapCrossingBatchRule is a DAILY rule enumerating every hour, minute and the
// first seconds seconds of the minute, so one day's candidate batch holds
// 1440*seconds entries. DTSTART the day before New York's spring change puts
// the request's range on the change's own day, the one batch a gap touches.
func gapCrossingBatchRule(seconds int) (raw string, dtstart time.Time) {
	rrule := "FREQ=DAILY;BYHOUR=" + enumeratedInts(24) + ";BYMINUTE=" + enumeratedInts(60) + ";BYSECOND=" + enumeratedInts(seconds)
	raw = recurringObject("DTSTART;TZID=America/New_York:20260307T000000", "RRULE:"+rrule)
	dtstart, _ = ParsePropertyDateTimeLocal("DTSTART;TZID=America/New_York", "20260307T000000")
	return raw, dtstart
}

// orderedByInstant charges no work budget, so ordering and merging a batch
// that holds a gap must cost no more than generating the batch does: a
// client-chosen BYHOUR/BYMINUTE/BYSECOND enumeration landing one day on a
// spring change must not cost minutes of CPU answering one request.
func TestOrderedByInstantOfAGapBatchScalesLinearly(t *testing.T) {
	requireHostZone(t, "America/New_York")
	rangeStart := time.Date(2026, 3, 8, 0, 0, 0, 0, time.UTC)
	rangeEnd := time.Date(2026, 3, 9, 0, 0, 0, 0, time.UTC)
	assertScalesLinearly(t, 10080, func(n int) func() {
		raw, dtstart := gapCrossingBatchRule(n / 1440)
		return func() {
			if _, err := RecurringBusyPeriods(raw, dtstart, time.Second, rangeStart, rangeEnd, 2*n, nil, nil); err != nil {
				t.Fatal(err)
			}
		}
	})
}

// RecurrenceSetExceedsLimit walks the same batch to decide CALDAV:max-instances,
// so it must scale the same way. DTSTART is the gap day itself, so the batch
// the merge costs is the first and only one COUNT needs.
func TestRecurrenceSetExceedsLimitOfAGapBatchScalesLinearly(t *testing.T) {
	requireHostZone(t, "America/New_York")
	assertScalesLinearly(t, 10080, func(n int) func() {
		rrule := "FREQ=DAILY;COUNT=900;BYHOUR=" + enumeratedInts(24) + ";BYMINUTE=" + enumeratedInts(60) + ";BYSECOND=" + enumeratedInts(n/1440)
		raw := recurringObject("DTSTART;TZID=America/New_York:20260308T000000", "RRULE:"+rrule)
		return func() {
			RecurrenceSetExceedsLimit(raw, MaxRecurrenceInstances)
		}
	})
}

// RFC 5545 §3.3.10 numbers BYSETPOS over a period's instances, not its wall
// clocks, so a gap that reorders a period's candidates against their own
// wall order must reorder what BYSETPOS counts them by too. Lord Howe moves
// its clocks forward half an hour at 02:00 on 4 October 2026: the period's
// three candidates read 02:00, 02:20 and 02:40, whose instants are
// 15:30Z, 15:50Z and 15:40Z -- 02:40 falls chronologically between the other
// two, not after them. BYSETPOS=2 on 3 October, an ordinary day, picks
// 02:20; on 4 October it must pick the period's second instant, 02:40, not
// the second by wall clock, 02:20.
func TestBySetPosNumbersAGapDaysCandidatesByInstant(t *testing.T) {
	requireHostZone(t, "Australia/Lord_Howe")
	raw := recurringObject("DTSTART;TZID=Australia/Lord_Howe:20261003T020000",
		"RRULE:FREQ=DAILY;BYHOUR=2;BYMINUTE=0,20,40;BYSETPOS=2;COUNT=3")
	dtstart, ok := ParsePropertyDateTimeLocal("DTSTART;TZID=Australia/Lord_Howe", "20261003T020000")
	if !ok {
		t.Fatal("DTSTART did not resolve")
	}
	// The DTSTART is always the set's first instance (RFC 5545 §3.8.5.3),
	// whether or not BYSETPOS would itself select it; 3 October's own
	// BYSETPOS pick is the second; 4 October's, the gap day, is the third.
	want := utcInstants("2026-10-02T15:30:00Z", "2026-10-02T15:50:00Z", "2026-10-03T15:40:00Z")
	rangeStart := time.Date(2026, 10, 1, 0, 0, 0, 0, time.UTC)
	rangeEnd := time.Date(2026, 10, 10, 0, 0, 0, 0, time.UTC)

	instances := instanceStarts(requireRecurrenceResult[RecurrenceInstance](t)(
		collectRecurrenceInstances(raw, "VEVENT", dtstart, time.Hour, rangeStart, rangeEnd, MaxRecurrenceInstances, nil)))
	sortTimes(instances)
	if !sameInstants(instances, want) {
		t.Errorf("VisitRecurrenceInstances() = %v, want %v", utcStrings(instances), utcStrings(want))
	}

	periods, err := RecurringBusyPeriods(raw, dtstart, time.Hour, rangeStart, rangeEnd, MaxRecurrenceInstances, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	busy := make([]time.Time, 0, len(periods))
	for _, period := range periods {
		busy = append(busy, period.Start)
	}
	sortTimes(busy)
	if !sameInstants(busy, want) {
		t.Errorf("RecurringBusyPeriods() = %v, want %v", utcStrings(busy), utcStrings(want))
	}

	slots, err := RecurrenceSlots(raw, "VEVENT", dtstart, time.Hour, rangeStart, rangeEnd, MaxRecurrenceInstances, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	slotStarts := make([]time.Time, 0, len(slots))
	for _, slot := range slots {
		slotStarts = append(slotStarts, slot.RecurrenceID)
	}
	sortTimes(slotStarts)
	if !sameInstants(slotStarts, want) {
		t.Errorf("RecurrenceSlots() = %v, want %v", utcStrings(slotStarts), utcStrings(want))
	}

	if exceeds, valid := RecurrenceSetExceedsLimit(raw, 3); !valid || exceeds {
		t.Errorf("RecurrenceSetExceedsLimit(raw, 3) = %v, %v; want false, true", exceeds, valid)
	}
	if exceeds, valid := RecurrenceSetExceedsLimit(raw, 2); !valid || !exceeds {
		t.Errorf("RecurrenceSetExceedsLimit(raw, 2) = %v, %v; want true, true", exceeds, valid)
	}
}

// A DTSTART on a skipped time that the rule does not generate itself, with a
// COUNT past the instance limit, is refused: the expansion yields COUNT
// instances, so the count cannot treat the candidate that reads the DTSTART's
// instant as a second one.
func TestRecurrenceSetExceedsLimitRefusesACountPastTheLimitFromASkippedDTStart(t *testing.T) {
	requireHostZone(t, "America/New_York")
	raw := recurringObject("DTSTART;TZID=America/New_York:20260308T023000",
		fmt.Sprintf("RRULE:FREQ=DAILY;BYHOUR=3;BYMINUTE=30;COUNT=%d", MaxRecurrenceInstances+1))
	dtstart, _ := ParsePropertyDateTimeLocal("DTSTART;TZID=America/New_York", "20260308T023000")
	instances := requireRecurrenceResult[RecurrenceInstance](t)(collectRecurrenceInstances(raw, "VEVENT", dtstart, time.Hour,
		dtstart.AddDate(0, 0, -1), dtstart.AddDate(4, 0, 0), 10*MaxRecurrenceInstances, nil))
	if len(instances) != MaxRecurrenceInstances+1 {
		t.Fatalf("expansion has %d instances, want %d", len(instances), MaxRecurrenceInstances+1)
	}
	if exceeds, valid := RecurrenceSetExceedsLimit(raw, MaxRecurrenceInstances); !valid || !exceeds {
		t.Errorf("RecurrenceSetExceedsLimit(raw, %d) = %v, %v; want true, true", MaxRecurrenceInstances, exceeds, valid)
	}
}

// gapZone is a zone with a transition that skips local times, anchored at the
// wall clock where the skipped stretch begins.
type gapZone struct {
	tzid   string
	anchor string
	length time.Duration
}

var gapZones = []gapZone{
	{"America/New_York", "20260308T020000", time.Hour},
	{"Australia/Lord_Howe", "20261004T020000", 30 * time.Minute},
	{"Pacific/Apia", "20111230T000000", 24 * time.Hour},
	{"Asia/Tehran", "20210322T000000", time.Hour},
	{"America/Santiago", "20260906T000000", time.Hour},
	{"America/Havana", "20240310T000000", time.Hour},
	{"Europe/Berlin", "20260329T020000", time.Hour},
}

// randomGapSeries is a series in zone whose DTSTART falls somewhere from a few
// hours before the skipped stretch to its last minute, so a series starts on
// the stretch itself as often as it crosses it.
func randomGapSeries(t *testing.T, random *rand.Rand, zone gapZone) (tzid, dtstartText string, dtstartWall time.Time) {
	t.Helper()
	anchor, err := ParseDateTime(zone.anchor)
	if err != nil {
		t.Fatal(err)
	}
	offset := time.Duration(random.Intn(int((6*time.Hour+zone.length)/time.Minute))) * time.Minute
	dtstartWall = anchor.Add(-6 * time.Hour).Add(offset)
	return zone.tzid, dtstartWall.Format("20060102T150405"), dtstartWall
}

// firstDifference describes where two sorted lists of instants part.
func firstDifference(got, want []time.Time) string {
	for i := 0; i < len(got) || i < len(want); i++ {
		switch {
		case i >= len(got):
			return fmt.Sprintf("%d instances, want %d; first missing %s", len(got), len(want), want[i].UTC().Format(time.RFC3339))
		case i >= len(want):
			return fmt.Sprintf("%d instances, want %d; first extra %s", len(got), len(want), got[i].UTC().Format(time.RFC3339))
		case !got[i].Equal(want[i]):
			return fmt.Sprintf("%d instances, want %d; at index %d got %s, want %s", len(got), len(want), i, got[i].UTC().Format(time.RFC3339), want[i].UTC().Format(time.RFC3339))
		}
	}
	return "none"
}

// oracleSubDailyInstants lists the instants of a fixed-step sub-daily series by
// its definition rather than the walk's: every wall clock stepped from the
// DTSTART is read with WallClockIn (RFC 5545 §3.3.5), the DTSTART is the first
// instance and none precedes it, instances at the same instant are one
// (§3.8.5.3), and the instants UNTIL or COUNT leaves out are dropped.
func oracleSubDailyInstants(loc *time.Location, dtstartWall time.Time, step time.Duration, count int, until *time.Time) []time.Time {
	start := WallClockIn(loc, dtstartWall)
	seen := map[time.Time]bool{}
	var instants []time.Time
	for k := 0; ; k++ {
		wall := dtstartWall.Add(time.Duration(k) * step)
		instant := WallClockIn(loc, wall)
		if until != nil && wall.After(until.Add(3*24*time.Hour)) {
			break
		}
		if count > 0 && len(instants) == count {
			break
		}
		if k > 0 && (instant.Before(start) || (until != nil && instant.After(*until))) {
			continue
		}
		if seen[instant.UTC()] {
			continue
		}
		seen[instant.UTC()] = true
		instants = append(instants, instant.UTC())
	}
	sortTimes(instants)
	return instants
}

// A fixed-step sub-daily series over a gap is the instants the oracle names
// however the series ends: the expansion, the same expansion read through a
// window anywhere in it, and the max-instances count all agree with it. A
// candidate on the far side of a gap can read an instant before one inside it,
// so neither UNTIL nor a window may end the walk at the first one that passes.
func TestSubDailySeriesAcrossAGapMatchesTheOracleInEveryReader(t *testing.T) {
	random := rand.New(rand.NewSource(20260308))
	cases := 0
	for _, zone := range gapZones {
		loc := requireHostZone(t, zone.tzid)
		for i := 0; i < 40; i++ {
			tzid, dtstartText, dtstartWall := randomGapSeries(t, random, zone)
			var rrule string
			step := time.Duration(1+2*random.Intn(60)) * time.Minute
			interval := int(step / time.Minute)
			if random.Intn(3) == 0 {
				step = time.Duration(1+random.Intn(7)) * time.Hour
				rrule = fmt.Sprintf("FREQ=HOURLY;INTERVAL=%d", step/time.Hour)
			} else {
				rrule = fmt.Sprintf("FREQ=MINUTELY;INTERVAL=%d", interval)
			}
			var until *time.Time
			count := 0
			if random.Intn(2) == 0 {
				count = 1 + random.Intn(150)
				rrule += fmt.Sprintf(";COUNT=%d", count)
			} else {
				bound := WallClockIn(loc, dtstartWall).Add(time.Duration(random.Intn(40*60)) * time.Minute).UTC()
				until = &bound
				rrule += ";UNTIL=" + bound.Format("20060102T150405Z")
			}
			name := fmt.Sprintf("%s/%s/%s", tzid, dtstartText, rrule)
			raw := recurringObject("DTSTART;TZID="+tzid+":"+dtstartText, "RRULE:"+rrule)
			dtstart, ok := ParsePropertyDateTimeLocal("DTSTART;TZID="+tzid, dtstartText)
			if !ok {
				t.Fatalf("%s: DTSTART did not resolve", name)
			}
			want := oracleSubDailyInstants(loc, dtstartWall, step, count, until)
			rangeStart, rangeEnd := dtstart.Add(-24*time.Hour), dtstart.Add(60*24*time.Hour)
			expand := func(from, to time.Time) []time.Time {
				instances, err := collectRecurrenceInstances(raw, "VEVENT", dtstart, time.Minute, from, to, 100*MaxRecurrenceInstances, nil)
				if err != nil {
					t.Fatalf("%s: expansion over [%v, %v): %v", name, from, to, err)
				}
				starts := instanceStarts(instances)
				sortTimes(starts)
				return starts
			}
			if got := expand(rangeStart, rangeEnd); !sameInstants(got, want) {
				t.Fatalf("%s: expansion differs: %s", name, firstDifference(got, want))
			}
			if n := len(want); n <= MaxRecurrenceInstances {
				if exceeds, valid := RecurrenceSetExceedsLimit(raw, n); !valid || exceeds {
					t.Fatalf("%s: RecurrenceSetExceedsLimit(raw, %d) = %v, %v; want false, true", name, n, exceeds, valid)
				}
				if exceeds, valid := RecurrenceSetExceedsLimit(raw, n-1); !valid || !exceeds {
					t.Fatalf("%s: RecurrenceSetExceedsLimit(raw, %d) = %v, %v; want true, true", name, n-1, exceeds, valid)
				}
			}
			// A window around some of the instances finds the instances the
			// full expansion has there.
			for _, instant := range want {
				if random.Intn(4) != 0 {
					continue
				}
				from := instant.Add(-time.Duration(random.Intn(40)) * time.Minute)
				to := instant.Add(time.Minute + time.Duration(random.Intn(40))*time.Minute)
				var wantWindow []time.Time
				for _, candidate := range want {
					if !candidate.Add(time.Minute).Before(from) && !candidate.After(to) {
						wantWindow = append(wantWindow, candidate)
					}
				}
				if got := expand(from, to); !sameInstants(got, wantWindow) {
					t.Fatalf("%s: window [%v, %v) differs: %s", name, from, to, firstDifference(got, wantWindow))
				}
			}
			cases++
		}
	}
	if cases == 0 {
		t.Fatal("no case ran")
	}
}

// The max-instances count and the expansion agree on a rule whose candidates a
// gap reorders and collides, however the rule is written: random daily,
// weekly and sub-daily rules with BY parts, from a DTSTART before, on or after
// the skipped stretch.
func TestRecurrenceSetExceedsLimitAgreesWithTheExpansionAcrossGaps(t *testing.T) {
	random := rand.New(rand.NewSource(5545))
	pick := func(values ...string) string { return values[random.Intn(len(values))] }
	for _, zone := range gapZones {
		requireHostZone(t, zone.tzid)
		for i := 0; i < 150; i++ {
			tzid, dtstartText, _ := randomGapSeries(t, random, zone)
			rrule := pick(
				"FREQ=DAILY;BYHOUR="+pick("1,3", "2", "0,2,4", "3")+";BYMINUTE="+pick("0,30", "10,40", "20", "0,20,40"),
				"FREQ=DAILY;BYHOUR="+pick("1,2,3", "2")+";BYMINUTE="+pick("0,20,40", "15,45")+";BYSETPOS="+pick("1", "2", "-1", "-2", "1,-1"),
				"FREQ=HOURLY;BYMINUTE="+pick("0,20,40", "10,50", "30"),
				"FREQ=HOURLY;INTERVAL="+pick("2", "3", "5")+";BYMINUTE="+pick("0,30", "10"),
				"FREQ=WEEKLY;BYDAY=MO,TU,WE,TH,FR,SA,SU;BYHOUR="+pick("1,3", "2", "0,4")+";BYMINUTE="+pick("0", "30", "10,40"),
				"FREQ=WEEKLY;BYDAY=MO,TU,WE,TH,FR,SA,SU;BYSETPOS="+pick("-2", "2", "1,-1"),
				"FREQ=MINUTELY;INTERVAL="+pick("10", "20", "25", "40", "45", "50"),
			)
			rrule += fmt.Sprintf(";COUNT=%d", 1+random.Intn(80))
			name := fmt.Sprintf("%s/%s/%s", tzid, dtstartText, rrule)
			raw := recurringObject("DTSTART;TZID="+tzid+":"+dtstartText, "RRULE:"+rrule)
			dtstart, ok := ParsePropertyDateTimeLocal("DTSTART;TZID="+tzid, dtstartText)
			if !ok {
				t.Fatalf("%s: DTSTART did not resolve", name)
			}
			instances, err := collectRecurrenceInstances(raw, "VEVENT", dtstart, time.Minute, dtstart.Add(-24*time.Hour), dtstart.AddDate(2, 0, 0), 100*MaxRecurrenceInstances, nil)
			if err != nil {
				t.Fatalf("%s: %v", name, err)
			}
			n := len(instances)
			if exceeds, valid := RecurrenceSetExceedsLimit(raw, n); !valid || exceeds {
				t.Fatalf("%s: RecurrenceSetExceedsLimit(raw, %d) = %v, %v; want false, true for the expansion's %v", name, n, exceeds, valid, utcStrings(instanceStarts(instances)))
			}
			if exceeds, valid := RecurrenceSetExceedsLimit(raw, n-1); !valid || !exceeds {
				t.Fatalf("%s: RecurrenceSetExceedsLimit(raw, %d) = %v, %v; want true, true for the expansion's %v", name, n-1, exceeds, valid, utcStrings(instanceStarts(instances)))
			}
		}
	}
}

// The candidate right after a gap reads before the ones inside it, so UNTIL
// ends a series at the first candidate past it that the zone has, not at one
// inside a gap: at 02:20 EST the 40-minute series below reads 07:20Z, past
// UNTIL, and 03:00 EDT after it is 07:00Z, before it.
func TestUntilDoesNotEndASeriesAtACandidateInsideAGap(t *testing.T) {
	requireHostZone(t, "America/New_York")
	raw := recurringObject("DTSTART;TZID=America/New_York:20260308T010000", "RRULE:FREQ=MINUTELY;INTERVAL=40;UNTIL=20260308T071000Z")
	dtstart, _ := ParsePropertyDateTimeLocal("DTSTART;TZID=America/New_York", "20260308T010000")
	got := instanceStarts(requireRecurrenceResult[RecurrenceInstance](t)(collectRecurrenceInstances(raw, "VEVENT", dtstart, time.Minute,
		dtstart.Add(-time.Hour), dtstart.Add(48*time.Hour), MaxRecurrenceInstances, nil)))
	sortTimes(got)
	want := utcInstants("2026-03-08T06:00:00Z", "2026-03-08T06:40:00Z", "2026-03-08T07:00:00Z")
	if !sameInstants(got, want) {
		t.Errorf("expansion = %v, want %v", utcStrings(got), utcStrings(want))
	}
	if exceeds, valid := RecurrenceSetExceedsLimit(raw, 3); !valid || exceeds {
		t.Errorf("RecurrenceSetExceedsLimit(raw, 3) = %v, %v; want false, true", exceeds, valid)
	}
	if exceeds, valid := RecurrenceSetExceedsLimit(raw, 2); !valid || !exceeds {
		t.Errorf("RecurrenceSetExceedsLimit(raw, 2) = %v, %v; want true, true", exceeds, valid)
	}
}

// A window over the end of a counted sub-daily series whose DTSTART is on a
// skipped time finds the series' last instances, which the jump over the
// periods before the window must not count past: the candidates after the gap
// that read before the DTSTART's instant are no instances.
func TestWindowOverTheEndOfACountedSeriesStartingInAGapFindsItsLastInstance(t *testing.T) {
	requireHostZone(t, "America/New_York")
	raw := recurringObject("DTSTART;TZID=America/New_York:20260308T025000", "RRULE:FREQ=MINUTELY;INTERVAL=40;COUNT=55")
	dtstart, _ := ParsePropertyDateTimeLocal("DTSTART;TZID=America/New_York", "20260308T025000")
	got := instanceStarts(requireRecurrenceResult[RecurrenceInstance](t)(collectRecurrenceInstances(raw, "VEVENT", dtstart, time.Minute,
		time.Date(2026, 3, 9, 19, 20, 0, 0, time.UTC), time.Date(2026, 3, 9, 20, 0, 0, 0, time.UTC), MaxRecurrenceInstances, nil)))
	want := utcInstants("2026-03-09T19:30:00Z")
	if !sameInstants(got, want) {
		t.Errorf("window = %v, want %v", utcStrings(got), utcStrings(want))
	}
}

// orderedByInstant orders a day's candidates by the gaps' lengths alone, and
// that is the order and the merging resolving each one gives, on every day of
// a year in the zones with a gap -- including the days a fall change repeats
// an hour, which the gaps do not list: no host zone has a gap and an overlap
// within a week of each other, so a day's batch never spans both.
func TestOrderedByInstantMatchesResolvingEveryCandidate(t *testing.T) {
	random := rand.New(rand.NewSource(20261004))
	checked := 0
	for _, zone := range append(gapZones, gapZone{"America/Sao_Paulo", "20181104T000000", time.Hour}, gapZone{"Africa/Cairo", "20230428T000000", time.Hour}) {
		loc := requireHostZone(t, zone.tzid)
		anchor, _ := ParseDateTime(zone.anchor)
		rule := recurrenceRule{
			localTimeExists:  func(wall time.Time) bool { return WallClockExists(loc, wall) },
			resolveCandidate: func(wall time.Time) (time.Time, bool) { return WallClockIn(loc, wall).UTC(), true },
			gaps:             hostLocalTimeGaps(loc),
		}
		for day := anchor.AddDate(0, 0, -200); day.Before(anchor.AddDate(0, 0, 200)); day = day.AddDate(0, 0, 1) {
			var candidates []time.Time
			for i := 0; i < 30; i++ {
				candidates = append(candidates, day.Add(time.Duration(random.Intn(24*12))*5*time.Minute))
			}
			if day.Equal(anchor.Truncate(24 * time.Hour)) {
				// Every five-minute wall clock of the gap's own day.
				candidates = candidates[:0]
				for at := day; at.Before(day.AddDate(0, 0, 1)); at = at.Add(5 * time.Minute) {
					candidates = append(candidates, at)
				}
			}
			sortTimes(candidates)
			candidates = uniqueTimes(candidates)
			dtstart := candidates[0].AddDate(-1, 0, 0)

			wantOrder := slices.Clone(candidates)
			instants := map[time.Time]time.Time{}
			for _, wall := range wantOrder {
				instants[wall] = WallClockIn(loc, wall).UTC()
			}
			slices.SortStableFunc(wantOrder, func(a, b time.Time) int { return instants[a].Compare(instants[b]) })
			var want []time.Time
			for i, wall := range wantOrder {
				if i > 0 && instants[wantOrder[i-1]].Equal(instants[wall]) {
					continue
				}
				want = append(want, wall)
			}
			got := rule.orderedByInstant(slices.Clone(candidates), dtstart)
			if !sameInstants(got, want) {
				t.Fatalf("%s, day %s: ordered %v, want %v", zone.tzid, day.Format("2006-01-02"), got, want)
			}
			checked++
		}
	}
	if checked == 0 {
		t.Fatal("no day checked")
	}
}
