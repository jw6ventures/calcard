package ical

import (
	"errors"
	"fmt"
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

// RFC 5545 §3.3.10: "Recurrence rules may generate recurrence instances with
// an invalid date ... or nonexistent local time ... Such recurrence instances
// MUST be ignored and MUST NOT be counted as part of the recurrence set." A
// COUNT series in a zone whose clocks skip its time of day therefore ends one
// occurrence later, while the DTSTART -- the first instance whatever it names
// (§3.8.5.3) -- and an RDATE, which no rule generated, keep the §3.3.5 reading.
func TestRecurrenceLeavesOutInstancesOnSkippedLocalTimes(t *testing.T) {
	requireHostZone(t, "Europe/Berlin")
	requireHostZone(t, "America/New_York")
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
			want:  utcInstants("2026-03-15T01:30:00Z", "2026-03-22T01:30:00Z", "2026-04-05T00:30:00Z", "2026-04-12T00:30:00Z"),
		},
		{
			name:    "weekly Sunday 02:30 across the spring change, zone the object defines",
			lines:   []string{"DTSTART;TZID=Private/Berlin:20260315T023000", "RRULE:FREQ=WEEKLY;COUNT=4"},
			resolve: private,
			want:    utcInstants("2026-03-15T01:30:00Z", "2026-03-22T01:30:00Z", "2026-04-05T00:30:00Z", "2026-04-12T00:30:00Z"),
		},
		{
			name:  "weekly Sunday 02:30 with BYDAY across the spring change",
			lines: []string{"DTSTART;TZID=Europe/Berlin:20260315T023000", "RRULE:FREQ=WEEKLY;BYDAY=SU;COUNT=4"},
			want:  utcInstants("2026-03-15T01:30:00Z", "2026-03-22T01:30:00Z", "2026-04-05T00:30:00Z", "2026-04-12T00:30:00Z"),
		},
		{
			name:  "a repeated 02:30 stays, as its first occurrence",
			lines: []string{"DTSTART;TZID=Europe/Berlin:20261018T023000", "RRULE:FREQ=WEEKLY;COUNT=3"},
			want:  utcInstants("2026-10-18T00:30:00Z", "2026-10-25T00:30:00Z", "2026-11-01T01:30:00Z"),
		},
		{
			name:  "daily 02:30 across the New York spring change",
			lines: []string{"DTSTART;TZID=America/New_York:20260307T023000", "RRULE:FREQ=DAILY;COUNT=3"},
			want:  utcInstants("2026-03-07T07:30:00Z", "2026-03-09T06:30:00Z", "2026-03-10T06:30:00Z"),
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
			name: "an RDATE on the skipped time is kept",
			lines: []string{
				"DTSTART;TZID=Europe/Berlin:20260322T023000", "RRULE:FREQ=WEEKLY;COUNT=2",
				"RDATE;TZID=Europe/Berlin:20260329T023000",
			},
			want: utcInstants("2026-03-22T01:30:00Z", "2026-03-29T01:30:00Z", "2026-04-05T00:30:00Z"),
		},
		{
			name:  "an hourly series is counted past the skipped hour",
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
			// BYSETPOS numbers the instances that exist, as it numbers only
			// the valid dates of BYMONTHDAY=28,29,30,31;BYSETPOS=-1, so the
			// last Sunday at 02:30 in March 2026 Berlin is the 22nd.
			name:  "BYSETPOS selects among the instances that exist",
			lines: []string{"DTSTART;TZID=Europe/Berlin:20260222T023000", "RRULE:FREQ=MONTHLY;BYDAY=SU;BYSETPOS=-1;COUNT=3"},
			want:  utcInstants("2026-02-22T01:30:00Z", "2026-03-22T01:30:00Z", "2026-04-26T00:30:00Z"),
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

			periods, err := RecurringBusyPeriods(raw, dtstart, time.Hour, rangeStart, rangeEnd, MaxRecurrenceInstances, tt.resolve)
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

			slots, err := RecurrenceSlots(raw, "VEVENT", dtstart, time.Hour, rangeStart, rangeEnd, MaxRecurrenceInstances, tt.resolve)
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
// zone, which leaves the skipped instance out of the count just as the
// expansion leaves it out of the set: a daily 02:30 series through the Berlin
// spring change has five instances in six days, whether the zone is the host's
// or one only the object defines.
func TestRecurrenceSetExceedsLimitDoesNotCountSkippedLocalTimes(t *testing.T) {
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
			if len(instances) != 5 {
				t.Fatalf("expansion = %v, want five instances", utcStrings(instanceStarts(instances)))
			}
			if exceeds, valid := RecurrenceSetExceedsLimit(tt.raw, 5); !valid || exceeds {
				t.Errorf("RecurrenceSetExceedsLimit(raw, 5) = %v, %v; want false, true", exceeds, valid)
			}
			if exceeds, valid := RecurrenceSetExceedsLimit(tt.raw, 4); !valid || !exceeds {
				t.Errorf("RecurrenceSetExceedsLimit(raw, 4) = %v, %v; want true, true", exceeds, valid)
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
		{"Berlin, DTSTART on the skipped hour", "DTSTART;TZID=Europe/Berlin:20240331T023000", "FREQ=DAILY;COUNT=5"},
		{"Berlin, weekly through the skipped hour", "DTSTART;TZID=Europe/Berlin:20240317T023000", "FREQ=WEEKLY;COUNT=5"},
		{"Berlin, DTSTART on the repeated hour", "DTSTART;TZID=Europe/Berlin:20241027T023000", "FREQ=DAILY;COUNT=5"},
		{"Berlin, daily through the repeated hour", "DTSTART;TZID=Europe/Berlin:20241025T023000", "FREQ=DAILY;COUNT=5"},
		{"Berlin, hourly through the repeated hour", "DTSTART;TZID=Europe/Berlin:20241027T003000", "FREQ=HOURLY;UNTIL=20241027T040000Z"},
		// Samoa skipped the whole of 30 December 2011.
		{"Apia, daily through the skipped day", "DTSTART;TZID=Pacific/Apia:20111227T100000", "FREQ=DAILY;COUNT=6"},
		// Lord Howe Island's clocks move by half an hour: 02:00 to 02:30 in
		// October, and 02:00 back to 01:30 in April.
		{"Lord Howe, quarter hours of 02:00 through the skipped half hour", "DTSTART;TZID=Australia/Lord_Howe:20241006T020000", "FREQ=DAILY;BYHOUR=2;BYMINUTE=0,15,30,45;COUNT=12"},
		{"Lord Howe, every 15 minutes through the repeated half hour", "DTSTART;TZID=Australia/Lord_Howe:20240407T010000", "FREQ=MINUTELY;INTERVAL=15;COUNT=20"},
		// Havana's clocks skip from midnight to 01:00 on 10 March 2024.
		{"Havana, daily at midnight through the skipped hour", "DTSTART;TZID=America/Havana:20240308T000000", "FREQ=DAILY;COUNT=5"},
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
			// This definition skips 02:30 on 1 March, which carries the
			// fifth instance to 4 March, where the EXDATE removes it.
			name: "the object skips a time the host does not",
			raw: withTimezone(earlySpring, "DTSTART;TZID=America/New_York:20260227T023000",
				"RRULE:FREQ=DAILY;COUNT=5", "EXDATE;TZID=America/New_York:20260304T023000"),
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

// An override naming the slot of an instance the rule's zone skips names no
// instance of the set. It is matched the way any such override is: free-busy
// publishes it as the period it states, and the master's own pattern neither
// generates the slot nor carries it as an overridden one.
func TestOverrideOfASkippedInstanceNamesNoGeneratedSlot(t *testing.T) {
	requireHostZone(t, "Europe/Berlin")
	raw := "BEGIN:VCALENDAR\r\n" +
		"BEGIN:VEVENT\r\nUID:series\r\nDTSTART;TZID=Europe/Berlin:20260322T023000\r\nRRULE:FREQ=WEEKLY;COUNT=2\r\nEND:VEVENT\r\n" +
		"BEGIN:VEVENT\r\nUID:series\r\nRECURRENCE-ID;TZID=Europe/Berlin:20260329T023000\r\nDTSTART;TZID=Europe/Berlin:20260329T100000\r\nEND:VEVENT\r\n" +
		"END:VCALENDAR\r\n"
	dtstart, _ := ParsePropertyDateTimeLocal("DTSTART;TZID=Europe/Berlin", "20260322T023000")
	rangeStart, rangeEnd := time.Date(2026, 3, 1, 0, 0, 0, 0, time.UTC), time.Date(2026, 5, 1, 0, 0, 0, 0, time.UTC)

	periods, err := RecurringBusyPeriods(raw, dtstart, time.Hour, rangeStart, rangeEnd, MaxRecurrenceInstances, nil)
	if err != nil {
		t.Fatal(err)
	}
	busy := make([]time.Time, 0, len(periods))
	for _, period := range periods {
		busy = append(busy, period.Start)
	}
	sortTimes(busy)
	if want := utcInstants("2026-03-22T01:30:00Z", "2026-03-29T08:00:00Z", "2026-04-05T00:30:00Z"); !sameInstants(busy, want) {
		t.Errorf("RecurringBusyPeriods() = %v, want %v", utcStrings(busy), utcStrings(want))
	}

	slots, err := RecurrenceSlots(raw, "VEVENT", dtstart, time.Hour, rangeStart, rangeEnd, MaxRecurrenceInstances, nil)
	if err != nil {
		t.Fatal(err)
	}
	for _, slot := range slots {
		if slot.ExactOverride || slot.RecurrenceID.Equal(utcInstants("2026-03-29T01:30:00Z")[0]) {
			t.Errorf("slot %v: the skipped instance is no slot of the master", slot)
		}
	}
	if len(slots) != 2 {
		t.Errorf("RecurrenceSlots() = %d slots, want 2", len(slots))
	}
}

// A rule whose every candidate past the DTSTART is a local time the zone skips
// -- the last Sunday of March at 02:30 in Berlin, which is exactly the hour the
// clocks jump -- generates nothing past the DTSTART. Each entry point has to
// recognize that within the scan budgets, the way it recognizes a rule whose BY
// parts never hold together, rather than follow it for ever.
func TestRuleGeneratingOnlySkippedLocalTimesTerminates(t *testing.T) {
	requireHostZone(t, "Europe/Berlin")
	dtstart, _ := ParsePropertyDateTimeLocal("DTSTART;TZID=Europe/Berlin", "20260301T023000")
	rangeStart := RecurrenceStartSentinel
	rangeEnd := time.Date(9999, 12, 31, 23, 59, 59, 0, time.UTC)
	for _, rrule := range []string{
		"FREQ=YEARLY;BYMONTH=3;BYDAY=-1SU;BYHOUR=2;BYMINUTE=30;COUNT=2",
		"FREQ=YEARLY;BYMONTH=3;BYDAY=-1SU;BYHOUR=2;BYMINUTE=30",
		"FREQ=MONTHLY;BYMONTH=3;BYDAY=-1SU;BYHOUR=2;COUNT=2",
		"FREQ=DAILY;BYMONTH=3;BYDAY=SU;BYMONTHDAY=25,26,27,28,29,30,31;BYHOUR=2;COUNT=2",
	} {
		t.Run(rrule, func(t *testing.T) {
			raw := recurringObject("DTSTART;TZID=Europe/Berlin:20260301T023000", "RRULE:"+rrule)
			finishesWithin(t, 2*time.Second, func() string {
				instances, err := collectRecurrenceInstances(raw, "VEVENT", dtstart, time.Hour, rangeStart, rangeEnd, MaxRecurrenceInstances, nil)
				// A daily rule's calendar cycle is more periods than the scan
				// covers, so it may instead run its budget out, which fails
				// closed like any rule the budget cannot finish.
				if err != nil && !(strings.HasPrefix(rrule, "FREQ=DAILY") && errors.Is(err, ErrRecurrenceScanLimit)) {
					return "VisitRecurrenceInstances() err = " + err.Error()
				}
				if starts := instanceStarts(instances); !sameInstants(starts, []time.Time{dtstart}) {
					return "VisitRecurrenceInstances() = " + strings.Join(utcStrings(starts), ",") + ", want the DTSTART alone"
				}
				if exceeds, valid := RecurrenceSetExceedsLimit(raw, MaxRecurrenceInstances); !valid || exceeds {
					return "RecurrenceSetExceedsLimit() refused the set"
				}
				return ""
			})
		})
	}
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
			periods, err := RecurringBusyPeriods(tt.raw, dtstart, time.Hour, RecurrenceStartSentinel, RecurrenceUntilSentinel, MaxRecurrenceInstances, tt.resolve)
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

// However the object's definition states the transition that skips the 29
// March instance -- a rule ended by a UTC UNTIL and its successor, a counted
// rule, an RDATE -- the bound reaches the series' last instance, 12 April at
// 02:30 CEST. A definition the bound cannot read leaves it unknown rather than
// assuming the zone skips nothing.
func TestRecurrenceBoundsReadEveryFormOfAnObservance(t *testing.T) {
	lastEnd := utcInstants("2026-04-12T01:30:00Z")[0]
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
