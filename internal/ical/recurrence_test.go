package ical

import (
	"errors"
	"math"
	"runtime"
	"strconv"
	"strings"
	"testing"
	"time"
)

func TestParseDateTime(t *testing.T) {
	tests := []struct {
		input   string
		want    time.Time
		wantErr bool
	}{
		{"20250115T120000Z", time.Date(2025, 1, 15, 12, 0, 0, 0, time.UTC), false},
		{"19970630T235960Z", time.Date(1997, 6, 30, 23, 59, 59, 0, time.UTC), false},
		{"20250115", time.Date(2025, 1, 15, 0, 0, 0, 0, time.UTC), false},
		{"2025-01-15T12:00:00Z", time.Date(2025, 1, 15, 12, 0, 0, 0, time.UTC), false},
		{"", time.Time{}, true},
		{"not-a-date", time.Time{}, true},
	}
	for _, tt := range tests {
		got, err := ParseDateTime(tt.input)
		if (err != nil) != tt.wantErr {
			t.Fatalf("ParseDateTime(%q) error = %v, wantErr %v", tt.input, err, tt.wantErr)
		}
		if err == nil && !got.Equal(tt.want) {
			t.Fatalf("ParseDateTime(%q) = %v, want %v", tt.input, got, tt.want)
		}
	}
}

func TestParseDuration(t *testing.T) {
	tests := []struct {
		input string
		want  time.Duration
		ok    bool
	}{
		{"PT1H", time.Hour, true},
		{"P1D", 24 * time.Hour, true},
		{"P1W", 7 * 24 * time.Hour, true},
		{"PT1H30M", 90 * time.Minute, true},
		{"-PT15M", -15 * time.Minute, true},
		{"P", 0, false},
		{"1H", 0, false},
		{"PT1M2", 0, false},
	}
	for _, tt := range tests {
		got, ok := ParseDuration(tt.input)
		if ok != tt.ok || got != tt.want {
			t.Fatalf("ParseDuration(%q) = (%v, %v), want (%v, %v)", tt.input, got, ok, tt.want, tt.ok)
		}
	}
}

func TestRecurrenceSetExceedsLimitCountsSparseFiniteRule(t *testing.T) {
	tests := map[string]struct {
		rrule   string
		extra   string
		exceeds bool
	}{
		"COUNT": {
			rrule:   "FREQ=DAILY;COUNT=1001;BYMONTH=2;BYMONTHDAY=29",
			exceeds: true,
		},
		"COUNT with irrelevant EXDATE": {
			rrule:   "FREQ=DAILY;COUNT=1001;BYMONTH=2;BYMONTHDAY=29",
			extra:   "EXDATE:20240301T000000Z\r\n",
			exceeds: true,
		},
		"COUNT reduced to the limit by EXDATE": {
			rrule:   "FREQ=DAILY;COUNT=1001;BYMONTH=2;BYMONTHDAY=29",
			extra:   "EXDATE:20240229T000000Z\r\n",
			exceeds: false,
		},
		"UNTIL": {
			rrule:   "FREQ=HOURLY;UNTIL=99991231T235959Z;BYMONTH=2;BYMONTHDAY=29;BYHOUR=0",
			exceeds: true,
		},
	}

	for name, tt := range tests {
		t.Run(name, func(t *testing.T) {
			raw := "BEGIN:VCALENDAR\r\n" +
				"BEGIN:VEVENT\r\n" +
				"UID:sparse\r\n" +
				"DTSTART:20240229T000000Z\r\n" +
				"RRULE:" + tt.rrule + "\r\n" +
				tt.extra +
				"END:VEVENT\r\n" +
				"END:VCALENDAR\r\n"

			exceeds, valid := RecurrenceSetExceedsLimit(raw, 1000)
			if !valid {
				t.Fatal("RecurrenceSetExceedsLimit() valid = false")
			}
			if exceeds != tt.exceeds {
				t.Fatalf("RecurrenceSetExceedsLimit() exceeds = %t, want %t", exceeds, tt.exceeds)
			}
		})
	}
}

func TestRecurrenceSetExceedsLimitDoesNotAssumeFiniteRuleCanProduceItsCount(t *testing.T) {
	raw := "BEGIN:VCALENDAR\r\n" +
		"BEGIN:VEVENT\r\n" +
		"UID:impossible\r\n" +
		"DTSTART:20240101T000000Z\r\n" +
		"RRULE:FREQ=DAILY;COUNT=1001;BYMONTH=2;BYMONTHDAY=30\r\n" +
		"END:VEVENT\r\n" +
		"END:VCALENDAR\r\n"

	exceeds, valid := RecurrenceSetExceedsLimit(raw, 1000)
	if !valid {
		t.Fatal("RecurrenceSetExceedsLimit() valid = false")
	}
	if exceeds {
		t.Fatal("RecurrenceSetExceedsLimit() exceeds = true, want false")
	}
}

func TestRecurrenceSetExceedsLimitCountsRuleWhoseFirstCandidateIsBeyondScanGuard(t *testing.T) {
	raw := "BEGIN:VCALENDAR\r\n" +
		"BEGIN:VEVENT\r\n" +
		"UID:delayed-sparse\r\n" +
		"DTSTART:20240101T000000Z\r\n" +
		"RRULE:FREQ=SECONDLY;COUNT=1001;BYMONTH=2\r\n" +
		"END:VEVENT\r\n" +
		"END:VCALENDAR\r\n"

	exceeds, valid := RecurrenceSetExceedsLimit(raw, 1000)
	if !valid {
		t.Fatal("RecurrenceSetExceedsLimit() valid = false")
	}
	if !exceeds {
		t.Fatal("RecurrenceSetExceedsLimit() exceeds = false, want true")
	}
}

func TestRecurrenceSetExceedsLimitDoesNotAssumeIntervalCanReachFilteredTime(t *testing.T) {
	raw := "BEGIN:VCALENDAR\r\n" +
		"BEGIN:VEVENT\r\n" +
		"UID:unaligned\r\n" +
		"DTSTART:20240101T000000Z\r\n" +
		"RRULE:FREQ=SECONDLY;INTERVAL=86400;COUNT=1001;BYHOUR=1\r\n" +
		"END:VEVENT\r\n" +
		"END:VCALENDAR\r\n"

	exceeds, valid := RecurrenceSetExceedsLimit(raw, 1000)
	if !valid {
		t.Fatal("RecurrenceSetExceedsLimit() valid = false")
	}
	if exceeds {
		t.Fatal("RecurrenceSetExceedsLimit() exceeds = true, want false")
	}
}

func TestRecurrenceSetExceedsLimitFinishesImpossibleSparseUntilRule(t *testing.T) {
	raw := "BEGIN:VCALENDAR\r\n" +
		"BEGIN:VEVENT\r\n" +
		"UID:impossible-sparse-until\r\n" +
		"DTSTART:20240101T000000Z\r\n" +
		"RRULE:FREQ=SECONDLY;UNTIL=99991231T235959Z;BYSECOND=0;BYSETPOS=2\r\n" +
		"END:VEVENT\r\n" +
		"END:VCALENDAR\r\n"

	exceeds, valid := RecurrenceSetExceedsLimit(raw, 1000)
	if !valid {
		t.Fatal("RecurrenceSetExceedsLimit() valid = false")
	}
	if exceeds {
		t.Fatal("RecurrenceSetExceedsLimit() exceeds = true, want false")
	}
}

// 2562048 hours is a little over 292 years, the point where a step stops fitting
// in a time.Duration. The interval has to be carried in seconds rather than a
// Duration for the rule to be measured at all; done that way it is simply a very
// sparse rule -- one instance every three centuries -- and nothing about it is
// over the instance limit.
func TestRecurrenceSetExceedsLimitHandlesLargeSubDailyInterval(t *testing.T) {
	raw := "BEGIN:VCALENDAR\r\n" +
		"BEGIN:VEVENT\r\n" +
		"UID:large-interval\r\n" +
		"DTSTART:20240101T000000Z\r\n" +
		"RRULE:FREQ=HOURLY;INTERVAL=2562048\r\n" +
		"END:VEVENT\r\n" +
		"END:VCALENDAR\r\n"

	exceeds, valid := RecurrenceSetExceedsLimit(raw, 1000)
	if !valid {
		t.Fatal("RecurrenceSetExceedsLimit() valid = false")
	}
	if exceeds {
		t.Fatal("RecurrenceSetExceedsLimit() exceeds = true for one instance every 292 years")
	}
}

func TestUnfoldLines(t *testing.T) {
	raw := "DESCRIPTION:line one\r\n continues here\r\nLOCATION:first\r\n  second"
	lines := UnfoldLines(raw)
	if len(lines) != 2 {
		t.Fatalf("UnfoldLines() = %d lines, want 2: %#v", len(lines), lines)
	}
	if lines[0] != "DESCRIPTION:line onecontinues here" {
		t.Fatalf("UnfoldLines() first line = %q", lines[0])
	}
	if lines[1] != "LOCATION:first second" {
		t.Fatalf("UnfoldLines() second line = %q", lines[1])
	}
}

// instanceStarts is the starts-only view of an expansion, which is what a test
// about *which* occurrences were generated asserts on. The slot each one
// belongs to has its own cases below.
func instanceStarts(instances []RecurrenceInstance) []time.Time {
	starts := make([]time.Time, 0, len(instances))
	for _, instance := range instances {
		starts = append(starts, instance.Start)
	}
	return starts
}

const recurringEvent = "BEGIN:VCALENDAR\r\n" +
	"BEGIN:VEVENT\r\n" +
	"UID:weekly\r\n" +
	"DTSTART:20250106T100000Z\r\n" +
	"DTEND:20250106T110000Z\r\n" +
	"RRULE:FREQ=WEEKLY\r\n" +
	"EXDATE:20250120T100000Z\r\n" +
	"END:VEVENT\r\n" +
	"END:VCALENDAR\r\n"

func TestEventHasRecurrence(t *testing.T) {
	if !EventHasRecurrence(recurringEvent) {
		t.Fatal("EventHasRecurrence() = false for RRULE event")
	}
	plain := "BEGIN:VCALENDAR\r\nBEGIN:VEVENT\r\nUID:x\r\nDTSTART:20250106T100000Z\r\nEND:VEVENT\r\nEND:VCALENDAR\r\n"
	if EventHasRecurrence(plain) {
		t.Fatal("EventHasRecurrence() = true for non-recurring event")
	}
}

func TestRecurringBusyPeriodsExpandsWeeklyRuleWithExdate(t *testing.T) {
	dtstart := time.Date(2025, 1, 6, 10, 0, 0, 0, time.UTC)
	rangeStart := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)
	rangeEnd := time.Date(2025, 2, 1, 0, 0, 0, 0, time.UTC)

	periods := requireRecurrenceResult[BusyPeriod](t)(RecurringBusyPeriods(recurringEvent, dtstart, time.Hour, rangeStart, rangeEnd, 1000, nil))

	// Mondays Jan 6, 13, 27 (Jan 20 excluded by EXDATE).
	want := []time.Time{
		time.Date(2025, 1, 6, 10, 0, 0, 0, time.UTC),
		time.Date(2025, 1, 13, 10, 0, 0, 0, time.UTC),
		time.Date(2025, 1, 27, 10, 0, 0, 0, time.UTC),
	}
	if len(periods) != len(want) {
		t.Fatalf("RecurringBusyPeriods() = %d periods, want %d: %#v", len(periods), len(want), periods)
	}
	for i, p := range periods {
		if !p.Start.Equal(want[i]) {
			t.Fatalf("period %d start = %v, want %v", i, p.Start, want[i])
		}
		if !p.End.Equal(want[i].Add(time.Hour)) {
			t.Fatalf("period %d end = %v, want %v", i, p.End, want[i].Add(time.Hour))
		}
	}
}

func TestRecurringBusyPeriodsAppliesOverride(t *testing.T) {
	raw := "BEGIN:VCALENDAR\r\n" +
		"BEGIN:VEVENT\r\n" +
		"UID:weekly\r\n" +
		"DTSTART:20250106T100000Z\r\n" +
		"DTEND:20250106T110000Z\r\n" +
		"RRULE:FREQ=WEEKLY;COUNT=2\r\n" +
		"END:VEVENT\r\n" +
		"BEGIN:VEVENT\r\n" +
		"UID:weekly\r\n" +
		"RECURRENCE-ID:20250113T100000Z\r\n" +
		"DTSTART:20250113T150000Z\r\n" +
		"DTEND:20250113T160000Z\r\n" +
		"END:VEVENT\r\n" +
		"END:VCALENDAR\r\n"
	dtstart := time.Date(2025, 1, 6, 10, 0, 0, 0, time.UTC)
	rangeStart := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)
	rangeEnd := time.Date(2025, 2, 1, 0, 0, 0, 0, time.UTC)

	periods := requireRecurrenceResult[BusyPeriod](t)(RecurringBusyPeriods(raw, dtstart, time.Hour, rangeStart, rangeEnd, 1000, nil))

	overridden := time.Date(2025, 1, 13, 15, 0, 0, 0, time.UTC)
	foundOverride := false
	for _, p := range periods {
		if p.Start.Equal(time.Date(2025, 1, 13, 10, 0, 0, 0, time.UTC)) {
			t.Fatalf("suppressed occurrence still present: %#v", p)
		}
		if p.Start.Equal(overridden) {
			foundOverride = true
		}
	}
	if !foundOverride {
		t.Fatalf("override occurrence missing: %#v", periods)
	}
}

func TestRecurrenceInstancesExpandNonEventComponents(t *testing.T) {
	for _, component := range []string{"VTODO", "VJOURNAL"} {
		t.Run(component, func(t *testing.T) {
			raw := "BEGIN:VCALENDAR\r\n" +
				"BEGIN:" + component + "\r\n" +
				"UID:repeating\r\n" +
				"DTSTART:20250106T100000Z\r\n" +
				"RRULE:FREQ=WEEKLY;COUNT=3\r\n" +
				"END:" + component + "\r\n" +
				"END:VCALENDAR\r\n"
			dtstart := time.Date(2025, 1, 6, 10, 0, 0, 0, time.UTC)

			if !componentHasRecurrence(raw, component) {
				t.Fatalf("componentHasRecurrence(%s) = false", component)
			}
			got := instanceStarts(requireRecurrenceResult[RecurrenceInstance](t)(RecurrenceInstances(raw, component, dtstart, 0,
				time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC),
				time.Date(2025, 2, 1, 0, 0, 0, 0, time.UTC), 1000, nil)))

			want := []time.Time{
				time.Date(2025, 1, 6, 10, 0, 0, 0, time.UTC),
				time.Date(2025, 1, 13, 10, 0, 0, 0, time.UTC),
				time.Date(2025, 1, 20, 10, 0, 0, 0, time.UTC),
			}
			if len(got) != len(want) {
				t.Fatalf("RecurrenceInstances() = %v, want %v", got, want)
			}
			for i := range want {
				if !got[i].Equal(want[i]) {
					t.Fatalf("instance %d = %v, want %v", i, got[i], want[i])
				}
			}
		})
	}
}

// A zero-duration occurrence sitting exactly on the end of the scan window is
// still a candidate: RFC 4791 §9.9 conditions such as "start <= DTSTART" accept
// an endpoint that a half-open overlap would discard before the caller ever
// evaluates them.
func TestRecurrenceInstancesIncludeOccurrencesOnTheBounds(t *testing.T) {
	raw := "BEGIN:VCALENDAR\r\n" +
		"BEGIN:VEVENT\r\n" +
		"UID:daily\r\n" +
		"DTSTART:20250106T100000Z\r\n" +
		"RRULE:FREQ=DAILY;COUNT=3\r\n" +
		"END:VEVENT\r\n" +
		"END:VCALENDAR\r\n"
	dtstart := time.Date(2025, 1, 6, 10, 0, 0, 0, time.UTC)
	boundary := time.Date(2025, 1, 7, 10, 0, 0, 0, time.UTC)

	got := instanceStarts(requireRecurrenceResult[RecurrenceInstance](t)(RecurrenceInstances(raw, "VEVENT", dtstart, 0, boundary, boundary, 1000, nil)))
	if len(got) != 1 || !got[0].Equal(boundary) {
		t.Fatalf("RecurrenceInstances() = %v, want exactly %v", got, boundary)
	}
}

// An overridden instance is reported by the override component itself, so the
// generated set must not also carry the recurrence-ID it replaced.
func TestRecurrenceInstancesOmitOverriddenInstances(t *testing.T) {
	raw := "BEGIN:VCALENDAR\r\n" +
		"BEGIN:VEVENT\r\n" +
		"UID:weekly\r\n" +
		"DTSTART:20250106T100000Z\r\n" +
		"RRULE:FREQ=WEEKLY;COUNT=2\r\n" +
		"END:VEVENT\r\n" +
		"BEGIN:VEVENT\r\n" +
		"UID:weekly\r\n" +
		"RECURRENCE-ID:20250113T100000Z\r\n" +
		"DTSTART:20250113T150000Z\r\n" +
		"END:VEVENT\r\n" +
		"END:VCALENDAR\r\n"
	dtstart := time.Date(2025, 1, 6, 10, 0, 0, 0, time.UTC)

	got := instanceStarts(requireRecurrenceResult[RecurrenceInstance](t)(RecurrenceInstances(raw, "VEVENT", dtstart, time.Hour,
		time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC),
		time.Date(2025, 2, 1, 0, 0, 0, 0, time.UTC), 1000, nil)))

	for _, start := range got {
		if start.Equal(time.Date(2025, 1, 13, 10, 0, 0, 0, time.UTC)) {
			t.Fatalf("overridden recurrence-id still generated: %v", got)
		}
		if start.Equal(time.Date(2025, 1, 13, 15, 0, 0, 0, time.UTC)) {
			t.Fatalf("override occurrence returned as a generated instance: %v", got)
		}
	}
	if len(got) != 1 || !got[0].Equal(dtstart) {
		t.Fatalf("RecurrenceInstances() = %v, want exactly %v", got, dtstart)
	}
}

// A caller holding a timezone this package cannot see resolves the whole
// recurrence set through it, not only the DTSTART it passes in. Reading EXDATE
// here instead would compare an instant in one zone against instants in another
// and quietly stop excluding anything.
func TestRecurrenceInstancesUseTheSuppliedResolver(t *testing.T) {
	// Floating throughout, so every value depends on the resolver.
	raw := "BEGIN:VCALENDAR\r\n" +
		"BEGIN:VEVENT\r\n" +
		"UID:floating\r\n" +
		"DTSTART:20250106T100000\r\n" +
		"RRULE:FREQ=WEEKLY;COUNT=3\r\n" +
		"EXDATE:20250113T100000\r\n" +
		"END:VEVENT\r\n" +
		"END:VCALENDAR\r\n"
	scanStart := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)
	scanEnd := time.Date(2025, 2, 1, 0, 0, 0, 0, time.UTC)

	// A resolver three hours behind the plain UTC reading, standing in for any
	// zone the caller resolved a floating value against.
	behind := PropertyTimeResolver(func(keyPart, value string) (time.Time, bool) {
		parsed, ok := ParsePropertyDateTimeLocal(keyPart, value)
		if !ok {
			return time.Time{}, false
		}
		return parsed.Add(3 * time.Hour), true
	})
	dtstart, ok := behind("DTSTART", "20250106T100000")
	if !ok {
		t.Fatal("resolver rejected the DTSTART")
	}

	got := instanceStarts(requireRecurrenceResult[RecurrenceInstance](t)(RecurrenceInstances(raw, "VEVENT", dtstart, 0, scanStart, scanEnd, 1000, behind)))
	want := []time.Time{
		time.Date(2025, 1, 6, 13, 0, 0, 0, time.UTC),
		time.Date(2025, 1, 20, 13, 0, 0, 0, time.UTC),
	}
	if len(got) != len(want) {
		t.Fatalf("RecurrenceInstances() = %v, want %v", got, want)
	}
	for i := range want {
		if !got[i].Equal(want[i]) {
			t.Fatalf("instance %d = %v, want %v", i, got[i], want[i])
		}
	}

	// A nil resolver keeps the ParsePropertyDateTimeLocal reading, which is what
	// every caller holding no zone of its own relies on.
	plain, _ := ParsePropertyDateTimeLocal("DTSTART", "20250106T100000")
	bare := instanceStarts(requireRecurrenceResult[RecurrenceInstance](t)(RecurrenceInstances(raw, "VEVENT", plain, 0, scanStart, scanEnd, 1000, nil)))
	if len(bare) != 2 || !bare[0].Equal(plain) {
		t.Fatalf("a nil resolver did not read as ParsePropertyDateTimeLocal: %v", bare)
	}
}

func TestRecurrenceInstancesResolveEveryGeneratedCivilTime(t *testing.T) {
	tests := []struct {
		name       string
		dtstart    string
		offsetFor  func(time.Time) time.Duration
		wantStarts []time.Time
	}{
		{
			name:    "spring forward",
			dtstart: "20240309T090000",
			offsetFor: func(wall time.Time) time.Duration {
				if !wall.Before(time.Date(2024, 3, 10, 2, 0, 0, 0, time.UTC)) {
					return -5 * time.Hour
				}
				return -6 * time.Hour
			},
			wantStarts: []time.Time{
				time.Date(2024, 3, 9, 15, 0, 0, 0, time.UTC),
				time.Date(2024, 3, 10, 14, 0, 0, 0, time.UTC),
				time.Date(2024, 3, 11, 14, 0, 0, 0, time.UTC),
			},
		},
		{
			name:    "fall back",
			dtstart: "20241102T090000",
			offsetFor: func(wall time.Time) time.Duration {
				if !wall.Before(time.Date(2024, 11, 3, 2, 0, 0, 0, time.UTC)) {
					return -6 * time.Hour
				}
				return -5 * time.Hour
			},
			wantStarts: []time.Time{
				time.Date(2024, 11, 2, 14, 0, 0, 0, time.UTC),
				time.Date(2024, 11, 3, 15, 0, 0, 0, time.UTC),
				time.Date(2024, 11, 4, 15, 0, 0, 0, time.UTC),
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			resolve := PropertyTimeResolver(func(keyPart, value string) (time.Time, bool) {
				wall, ok := ParsePropertyDateTimeLocal(keyPart, value)
				if !ok {
					return time.Time{}, false
				}
				return wall.Add(-test.offsetFor(wall)), true
			})
			dtstart, ok := resolve("DTSTART;TZID=Review/Chicago", test.dtstart)
			if !ok {
				t.Fatal("resolver rejected DTSTART")
			}
			raw := "BEGIN:VCALENDAR\r\n" +
				"BEGIN:VEVENT\r\n" +
				"UID:daily\r\n" +
				"DTSTART;TZID=Review/Chicago:" + test.dtstart + "\r\n" +
				"RRULE:FREQ=DAILY;COUNT=3\r\n" +
				"END:VEVENT\r\n" +
				"END:VCALENDAR\r\n"

			got := instanceStarts(requireRecurrenceResult[RecurrenceInstance](t)(RecurrenceInstances(raw, "VEVENT", dtstart, time.Hour,
				test.wantStarts[0].Add(-time.Hour), test.wantStarts[len(test.wantStarts)-1].Add(2*time.Hour),
				1000, resolve)))
			if len(got) != len(test.wantStarts) {
				t.Fatalf("RecurrenceInstances() = %v, want %v", got, test.wantStarts)
			}
			for i := range test.wantStarts {
				if !got[i].Equal(test.wantStarts[i]) {
					t.Fatalf("instance %d = %v, want %v", i, got[i], test.wantStarts[i])
				}
			}
		})
	}
}

// The zone a value resolves in, in the order the parameters and the value
// itself decide it. A caller holding a request or collection timezone does not
// come through here at all.
func TestParsePropertyDateTimeLocalZoneSelection(t *testing.T) {
	tests := []struct {
		name    string
		keyPart string
		value   string
		want    time.Time
	}{
		{
			name:    "a floating value reads as UTC",
			keyPart: "DTSTART", value: "20250106T100000",
			want: time.Date(2025, 1, 6, 10, 0, 0, 0, time.UTC),
		},
		{
			name:    "a value carrying its own zone keeps it",
			keyPart: "DTSTART", value: "20250106T100000Z",
			want: time.Date(2025, 1, 6, 10, 0, 0, 0, time.UTC),
		},
		{
			name:    "a TZID the host knows wins",
			keyPart: "DTSTART;TZID=UTC", value: "20250106T100000",
			want: time.Date(2025, 1, 6, 10, 0, 0, 0, time.UTC),
		},
		{
			name:    "a TZID the host cannot resolve falls back to UTC",
			keyPart: "DTSTART;TZID=Custom/Unknown", value: "20250106T100000",
			want: time.Date(2025, 1, 6, 10, 0, 0, 0, time.UTC),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, ok := ParsePropertyDateTimeLocal(tt.keyPart, tt.value)
			if !ok {
				t.Fatalf("ParsePropertyDateTimeLocal(%q, %q) ok = false", tt.keyPart, tt.value)
			}
			if !got.Equal(tt.want) {
				t.Fatalf("ParsePropertyDateTimeLocal(%q, %q) = %v, want %v", tt.keyPart, tt.value, got, tt.want)
			}
		})
	}
}

func TestParsePropertyDateTimeLocalHonorsTZID(t *testing.T) {
	got, ok := ParsePropertyDateTimeLocal("DTSTART;TZID=America/Chicago", "20250106T100000")
	if !ok {
		t.Fatal("ParsePropertyDateTimeLocal() ok = false")
	}
	loc, err := time.LoadLocation("America/Chicago")
	if err != nil {
		t.Skipf("tzdata unavailable: %v", err)
	}
	want := time.Date(2025, 1, 6, 10, 0, 0, 0, loc)
	if !got.Equal(want) {
		t.Fatalf("ParsePropertyDateTimeLocal() = %v, want %v", got, want)
	}
}

// RecurrenceInstances keeps the slot each occurrence belongs to alongside where
// it actually falls. The two diverge under a RANGE=THISANDFUTURE override, and a
// caller writing a RECURRENCE-ID out needs the slot: RFC 5545 §3.8.4.4 makes the
// property identify the instance within the master's pattern, not the time the
// override moved it to.
func TestRecurrenceInstancesReportsTheSlotSeparatelyFromTheOccurrence(t *testing.T) {
	raw := "BEGIN:VCALENDAR\r\n" +
		"BEGIN:VEVENT\r\n" +
		"UID:daily\r\n" +
		"DTSTART:20250106T100000Z\r\n" +
		"DTEND:20250106T110000Z\r\n" +
		"RRULE:FREQ=DAILY;COUNT=4\r\n" +
		"END:VEVENT\r\n" +
		"BEGIN:VEVENT\r\n" +
		"UID:daily\r\n" +
		"RECURRENCE-ID;RANGE=THISANDFUTURE:20250108T100000Z\r\n" +
		"DTSTART:20250108T150000Z\r\n" +
		"DTEND:20250108T160000Z\r\n" +
		"END:VEVENT\r\n" +
		"END:VCALENDAR\r\n"
	dtstart := time.Date(2025, 1, 6, 10, 0, 0, 0, time.UTC)
	rangeStart := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)
	rangeEnd := time.Date(2025, 2, 1, 0, 0, 0, 0, time.UTC)

	instances := requireRecurrenceResult[RecurrenceInstance](t)(RecurrenceInstances(raw, "VEVENT", dtstart, time.Hour, rangeStart, rangeEnd, 1000, nil))
	if len(instances) != 4 {
		t.Fatalf("instances = %d, want 4: %#v", len(instances), instances)
	}

	type pair struct{ slot, start string }
	var got []pair
	for _, instance := range instances {
		got = append(got, pair{
			slot:  instance.RecurrenceID.UTC().Format("20060102T150405Z"),
			start: instance.Start.UTC().Format("20060102T150405Z"),
		})
	}
	want := []pair{
		{"20250106T100000Z", "20250106T100000Z"},
		{"20250107T100000Z", "20250107T100000Z"},
		// From the override's slot onward the occurrence moves but the slot the
		// pattern generated does not.
		{"20250108T100000Z", "20250108T150000Z"},
		{"20250109T100000Z", "20250109T150000Z"},
	}
	for i := range want {
		if got[i] != want[i] {
			t.Errorf("instance %d = %+v, want %+v", i, got[i], want[i])
		}
	}
}

func TestRecurrenceSlotsExposeOverridePrecedence(t *testing.T) {
	raw := "BEGIN:VCALENDAR\r\n" +
		"BEGIN:VEVENT\r\n" +
		"UID:daily\r\n" +
		"DTSTART:20250106T100000Z\r\n" +
		"DTEND:20250106T110000Z\r\n" +
		"RRULE:FREQ=DAILY;COUNT=4\r\n" +
		"END:VEVENT\r\n" +
		"BEGIN:VEVENT\r\n" +
		"UID:daily\r\n" +
		"RECURRENCE-ID;RANGE=THISANDFUTURE:20250107T100000Z\r\n" +
		"DTSTART:20250107T120000Z\r\n" +
		"DTEND:20250107T130000Z\r\n" +
		"END:VEVENT\r\n" +
		"BEGIN:VEVENT\r\n" +
		"UID:daily\r\n" +
		"RECURRENCE-ID:20250108T100000Z\r\n" +
		"DTSTART:20250120T100000Z\r\n" +
		"DTEND:20250120T110000Z\r\n" +
		"END:VEVENT\r\n" +
		"BEGIN:VEVENT\r\n" +
		"UID:daily\r\n" +
		"RECURRENCE-ID;RANGE=THISANDFUTURE:20250109T100000Z\r\n" +
		"STATUS:CANCELLED\r\n" +
		"END:VEVENT\r\n" +
		"END:VCALENDAR\r\n"
	dtstart := time.Date(2025, 1, 6, 10, 0, 0, 0, time.UTC)
	slots := requireRecurrenceResult[RecurrenceSlot](t)(RecurrenceSlots(raw, "VEVENT", dtstart, time.Hour,
		time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC),
		time.Date(2025, 2, 1, 0, 0, 0, 0, time.UTC), 1000, nil))
	if len(slots) != 4 {
		t.Fatalf("slots = %d, want 4: %#v", len(slots), slots)
	}
	firstRange := time.Date(2025, 1, 7, 10, 0, 0, 0, time.UTC)
	cancelRange := time.Date(2025, 1, 9, 10, 0, 0, 0, time.UTC)
	if !slots[1].GoverningRangeRecurrenceID.Equal(firstRange) || slots[1].Suppressed || slots[1].ExactOverride {
		t.Errorf("first shifted slot metadata = %#v", slots[1])
	}
	if want := time.Date(2025, 1, 7, 12, 0, 0, 0, time.UTC); !slots[1].Effective.Start.Equal(want) {
		t.Errorf("first shifted slot starts %v, want %v", slots[1].Effective.Start, want)
	}
	if !slots[2].ExactOverride || !slots[2].Suppressed || !slots[2].GoverningRangeRecurrenceID.Equal(firstRange) {
		t.Errorf("ordinary override slot metadata = %#v", slots[2])
	}
	if slots[3].ExactOverride || !slots[3].Suppressed || !slots[3].GoverningRangeRecurrenceID.Equal(cancelRange) {
		t.Errorf("cancelled range slot metadata = %#v", slots[3])
	}
}

// Without a RANGE=THISANDFUTURE override nothing has moved an occurrence off
// the slot that generated it, so the two readings coincide.
func TestRecurrenceInstancesSlotMatchesTheStartWithoutAnOverride(t *testing.T) {
	raw := "BEGIN:VCALENDAR\r\n" +
		"BEGIN:VEVENT\r\n" +
		"UID:daily\r\n" +
		"DTSTART:20250106T100000Z\r\n" +
		"DTEND:20250106T110000Z\r\n" +
		"RRULE:FREQ=DAILY;COUNT=3\r\n" +
		"END:VEVENT\r\n" +
		"END:VCALENDAR\r\n"
	dtstart := time.Date(2025, 1, 6, 10, 0, 0, 0, time.UTC)
	rangeStart := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)
	rangeEnd := time.Date(2025, 2, 1, 0, 0, 0, 0, time.UTC)

	instances := requireRecurrenceResult[RecurrenceInstance](t)(RecurrenceInstances(raw, "VEVENT", dtstart, time.Hour, rangeStart, rangeEnd, 1000, nil))
	if len(instances) != 3 {
		t.Fatalf("instances = %d, want 3", len(instances))
	}
	for i, instance := range instances {
		if !instance.RecurrenceID.Equal(instance.Start) {
			t.Errorf("instance %d slot %v differs from its start %v with no override present",
				i, instance.RecurrenceID, instance.Start)
		}
	}
}

func TestRecurrenceInstancesKeepDistinctSlotsAtTheSameScheduledTime(t *testing.T) {
	raw := "BEGIN:VCALENDAR\r\n" +
		"BEGIN:VEVENT\r\n" +
		"UID:daily\r\n" +
		"DTSTART:20250106T100000Z\r\n" +
		"DTEND:20250106T110000Z\r\n" +
		"RRULE:FREQ=DAILY;COUNT=2\r\n" +
		"END:VEVENT\r\n" +
		"BEGIN:VEVENT\r\n" +
		"UID:daily\r\n" +
		"RECURRENCE-ID;RANGE=THISANDFUTURE:20250107T100000Z\r\n" +
		"DTSTART:20250106T100000Z\r\n" +
		"DTEND:20250106T110000Z\r\n" +
		"END:VEVENT\r\n" +
		"END:VCALENDAR\r\n"
	dtstart := time.Date(2025, 1, 6, 10, 0, 0, 0, time.UTC)
	rangeStart := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)
	rangeEnd := time.Date(2025, 2, 1, 0, 0, 0, 0, time.UTC)

	instances := requireRecurrenceResult[RecurrenceInstance](t)(RecurrenceInstances(raw, "VEVENT", dtstart, time.Hour, rangeStart, rangeEnd, 1000, nil))
	if len(instances) != 2 {
		t.Fatalf("instances = %d, want 2 distinct recurrence slots: %#v", len(instances), instances)
	}
	if !instances[0].Start.Equal(instances[1].Start) {
		t.Fatalf("scheduled starts = %v and %v, want the same time", instances[0].Start, instances[1].Start)
	}
	if instances[0].RecurrenceID.Equal(instances[1].RecurrenceID) {
		t.Fatalf("recurrence IDs = %v and %v, want distinct slots", instances[0].RecurrenceID, instances[1].RecurrenceID)
	}
}

// enumeratedInts spells out a full BY* list, which is how a compact rule
// describes an enormous period.
func enumeratedInts(n int) string {
	parts := make([]string, n)
	for i := range parts {
		parts[i] = strconv.Itoa(i)
	}
	return strings.Join(parts, ",")
}

func recurringTestComponent(rrule string) string {
	return "BEGIN:VCALENDAR\r\n" +
		"BEGIN:VEVENT\r\n" +
		"UID:dense\r\n" +
		"DTSTART:20240101T000000Z\r\n" +
		"RRULE:" + rrule + "\r\n" +
		"END:VEVENT\r\n" +
		"END:VCALENDAR\r\n"
}

// A yearly rule naming every day, hour, minute and second describes about 31.5
// million candidate starts for one period. Validating a PUT against it must not
// generate them: the answer needs the first thousand and one, or the one
// BYSETPOS selects. The allocation bound is the assertion: the cost at stake is
// not time but a request making the server hold gigabytes.
func TestRecurrenceSetExceedsLimitDoesNotMaterializeADensePeriod(t *testing.T) {
	dense := "BYDAY=MO,TU,WE,TH,FR,SA,SU" +
		";BYHOUR=" + enumeratedInts(24) +
		";BYMINUTE=" + enumeratedInts(60) +
		";BYSECOND=" + enumeratedInts(60)

	tests := map[string]struct {
		rrule   string
		exceeds bool
	}{
		// One instance in total, selected by the first position of the period.
		"positive BYSETPOS with COUNT": {
			rrule:   "FREQ=YEARLY;" + dense + ";COUNT=1;BYSETPOS=1",
			exceeds: false,
		},
		// The period cannot be enumerated within the work bound, and a period
		// that large is past the advertised instance limit either way.
		"negative BYSETPOS": {
			rrule:   "FREQ=YEARLY;" + dense + ";BYSETPOS=-1",
			exceeds: true,
		},
		// No positional selection: the limit is reached in the first day.
		"no BYSETPOS": {
			rrule:   "FREQ=YEARLY;" + dense,
			exceeds: true,
		},
	}

	for name, tt := range tests {
		t.Run(name, func(t *testing.T) {
			raw := recurringTestComponent(tt.rrule)

			var before, after runtime.MemStats
			runtime.GC()
			runtime.ReadMemStats(&before)
			exceeds, valid := RecurrenceSetExceedsLimit(raw, MaxRecurrenceInstances)
			runtime.ReadMemStats(&after)

			if !valid {
				t.Fatal("RecurrenceSetExceedsLimit() valid = false")
			}
			if exceeds != tt.exceeds {
				t.Fatalf("RecurrenceSetExceedsLimit() exceeds = %t, want %t", exceeds, tt.exceeds)
			}
			// Generous next to the 8.7 GiB the materializing generator took, and
			// far below any figure that could be reached by holding a period.
			const allocationBudget = 64 << 20
			if allocated := after.TotalAlloc - before.TotalAlloc; allocated > allocationBudget {
				t.Fatalf("allocated %d bytes, want at most %d: the period is being materialized", allocated, allocationBudget)
			}
		})
	}
}

// The period is streamed rather than sorted whole, so the order and the
// selection have to be what a sort over the whole period would produce --
// including for a rule whose days are generated out of order. A DTSTART the
// rule does not select still leads the set (RFC 5545 §3.8.5.3).
func TestRecurrenceInstancesAppliesBySetPosOverTheWholePeriod(t *testing.T) {
	tests := map[string]struct {
		rrule string
		want  []string
	}{
		"first of the month": {
			rrule: "FREQ=MONTHLY;BYMONTHDAY=15,1;BYSETPOS=1;COUNT=3",
			want:  []string{"20240101T000000Z", "20240201T000000Z", "20240301T000000Z"},
		},
		"last of the month": {
			rrule: "FREQ=MONTHLY;BYMONTHDAY=15,1;BYSETPOS=-1;COUNT=4",
			want:  []string{"20240101T000000Z", "20240115T000000Z", "20240215T000000Z", "20240315T000000Z"},
		},
		"both ends": {
			rrule: "FREQ=MONTHLY;BYMONTHDAY=15,1;BYSETPOS=1,-1;COUNT=4",
			want:  []string{"20240101T000000Z", "20240115T000000Z", "20240201T000000Z", "20240215T000000Z"},
		},
		"position past the end of the period": {
			rrule: "FREQ=MONTHLY;BYMONTHDAY=15,1;BYSETPOS=3;COUNT=2",
			want:  []string{"20240101T000000Z"},
		},
		"last weekday of the month": {
			rrule: "FREQ=MONTHLY;BYDAY=MO,TU,WE,TH,FR;BYSETPOS=-1;COUNT=4",
			want:  []string{"20240101T000000Z", "20240131T000000Z", "20240229T000000Z", "20240329T000000Z"},
		},
		"day list out of order without BYSETPOS": {
			rrule: "FREQ=MONTHLY;BYMONTHDAY=15,1;COUNT=4",
			want:  []string{"20240101T000000Z", "20240115T000000Z", "20240201T000000Z", "20240215T000000Z"},
		},
	}

	for name, tt := range tests {
		t.Run(name, func(t *testing.T) {
			dtstart := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
			instances := requireRecurrenceResult[RecurrenceInstance](t)(RecurrenceInstances(recurringTestComponent(tt.rrule), "VEVENT", dtstart, time.Hour,
				dtstart, dtstart.AddDate(1, 0, 0), MaxRecurrenceInstances, nil))

			var got []string
			for _, instance := range instances {
				got = append(got, instance.Start.UTC().Format("20060102T150405Z"))
			}
			if len(got) != len(tt.want) {
				t.Fatalf("starts = %v, want %v", got, tt.want)
			}
			for i := range tt.want {
				if got[i] != tt.want[i] {
					t.Fatalf("starts = %v, want %v", got, tt.want)
				}
			}
		})
	}
}

// A VTIMEZONE observance is resolved through LatestRecurrenceOnOrBefore, so an
// answer it cannot complete has to say so: the offset picked from a half-generated
// period is the wrong one, and the caller cannot tell it apart from the right one.
func TestLatestRecurrenceOnOrBeforeReportsAnIncompletePeriod(t *testing.T) {
	dense := ";BYMONTH=1,2,3,4,5,6;BYMONTHDAY=" + rangeInts(1, 31) +
		";BYHOUR=" + enumeratedInts(24) + ";BYMINUTE=" + enumeratedInts(60) + ";BYSECOND=" + enumeratedInts(60)
	dtstart := time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC)
	before := time.Date(2026, 6, 1, 0, 0, 0, 0, time.UTC)

	tests := map[string]struct {
		rrule    string
		dtstart  time.Time
		before   time.Time
		complete bool
		found    bool
		want     string
	}{
		// The shape every real VTIMEZONE observance has.
		"ordinary observance rule": {
			rrule:    "FREQ=YEARLY;BYMONTH=3;BYDAY=2SU",
			dtstart:  time.Date(2007, 3, 11, 2, 0, 0, 0, time.UTC),
			before:   time.Date(2026, 6, 1, 0, 0, 0, 0, time.UTC),
			complete: true,
			found:    true,
			want:     "20260308T020000Z",
		},
		// Generating the period exhausts the work bound partway through, so the
		// maximum reached is not the period's maximum.
		"period too large to generate": {
			rrule: "FREQ=YEARLY" + dense, dtstart: dtstart, before: before,
			complete: false,
		},
		// BYSETPOS resolves only once the period ends, so an abandoned period
		// selects nothing at all -- not even a partial answer.
		"period too large to generate with BYSETPOS": {
			rrule: "FREQ=YEARLY;BYSETPOS=-1" + dense, dtstart: dtstart, before: before,
			complete: false,
		},
	}

	for name, tt := range tests {
		t.Run(name, func(t *testing.T) {
			latest, found, complete := LatestRecurrenceOnOrBefore(tt.dtstart, tt.before, tt.rrule)
			if complete != tt.complete {
				t.Fatalf("complete = %t, want %t", complete, tt.complete)
			}
			if !tt.complete {
				return
			}
			if found != tt.found {
				t.Fatalf("found = %t, want %t", found, tt.found)
			}
			if got := latest.UTC().Format("20060102T150405Z"); found && got != tt.want {
				t.Fatalf("latest = %s, want %s", got, tt.want)
			}
		})
	}
}

// rangeInts spells out an inclusive range, the compact way a rule names every day
// of a month.
func rangeInts(lo, hi int) string {
	parts := make([]string, 0, hi-lo+1)
	for i := lo; i <= hi; i++ {
		parts = append(parts, strconv.Itoa(i))
	}
	return strings.Join(parts, ",")
}

// Chile's 2024-09-08 00:00 DST gap normalizes weeklyDays' Sunday entry
// backwards onto 2024-09-07, the same calendar date as its Saturday entry. The
// day list has to collapse those into one date rather than keep both instants,
// or the date's whole candidate set is produced twice.
func TestVisitRecurrenceCandidatesDedupesDayListByCalendarDate(t *testing.T) {
	loc, err := time.LoadLocation("America/Santiago")
	if err != nil {
		t.Skipf("tzdata unavailable: %v", err)
	}
	rule, ok := parseRecurrenceRule("FREQ=WEEKLY;BYDAY=SA,SU;BYHOUR=0,23", loc, nil)
	if !ok {
		t.Fatal("parseRecurrenceRule() ok = false")
	}
	dtstart := time.Date(2024, 9, 2, 0, 30, 0, 0, loc)
	periodStart := recurrencePeriodStart(dtstart, rule)

	var got []time.Time
	visitRecurrenceCandidates(periodStart, dtstart, rule, func(current time.Time) bool {
		got = append(got, current)
		return false
	})

	want := []time.Time{
		time.Date(2024, 9, 7, 0, 30, 0, 0, loc),
		time.Date(2024, 9, 7, 23, 30, 0, 0, loc),
	}
	if len(got) != len(want) {
		t.Fatalf("candidates = %v, want %v", got, want)
	}
	for i := range want {
		if !got[i].Equal(want[i]) {
			t.Fatalf("candidates = %v, want %v", got, want)
		}
	}
}

// The same Chilean DST gap, this time with BYSETPOS applied. The period holds
// exactly two real candidates once the day list is deduplicated by date; a
// stream that still offers each of them twice breaks both the position count
// (BYSETPOS=3 should select nothing) and the ascending order BYSETPOS relies on.
func TestVisitRecurrenceCandidatesBySetPosOverDeduplicatedDayList(t *testing.T) {
	loc, err := time.LoadLocation("America/Santiago")
	if err != nil {
		t.Skipf("tzdata unavailable: %v", err)
	}
	dtstart := time.Date(2024, 9, 2, 0, 30, 0, 0, loc)
	first := time.Date(2024, 9, 7, 0, 30, 0, 0, loc)
	last := time.Date(2024, 9, 7, 23, 30, 0, 0, loc)

	tests := map[string]struct {
		bysetpos string
		want     []time.Time
	}{
		"first of the two real candidates": {bysetpos: "1", want: []time.Time{first}},
		"last of the two real candidates":  {bysetpos: "-1", want: []time.Time{last}},
		"position past the real count":     {bysetpos: "3", want: nil},
	}

	for name, tt := range tests {
		t.Run(name, func(t *testing.T) {
			rule, ok := parseRecurrenceRule("FREQ=WEEKLY;BYDAY=SA,SU;BYHOUR=0,23;BYSETPOS="+tt.bysetpos, loc, nil)
			if !ok {
				t.Fatal("parseRecurrenceRule() ok = false")
			}
			periodStart := recurrencePeriodStart(dtstart, rule)

			var got []time.Time
			visitRecurrenceCandidates(periodStart, dtstart, rule, func(current time.Time) bool {
				got = append(got, current)
				return false
			})

			if len(got) != len(tt.want) {
				t.Fatalf("candidates = %v, want %v", got, tt.want)
			}
			for i := range tt.want {
				if !got[i].Equal(tt.want[i]) {
					t.Fatalf("candidates = %v, want %v", got, tt.want)
				}
			}
		})
	}
}

// finishSparseRecurrence's DAILY branch discards visitRecurrenceCandidates'
// overBudget flag. A single real day can never trip recurrencePeriodWorkLimit
// (24 hours * 60 minutes * 61 BYSECOND values tops out at 87,840), but a rule
// built by hand rather than through parseRecurrenceRule can, and the caller
// must fail closed exactly like every other visitRecurrenceCandidates caller
// rather than silently treating the day as producing nothing.
func TestFinishSparseRecurrenceFailsClosedWhenADayIsOverBudget(t *testing.T) {
	dtstart := time.Date(2030, 1, 1, 0, 0, 0, 0, time.UTC)
	until := time.Date(2030, 12, 31, 23, 59, 59, 0, time.UTC)

	hours := make([]int, 200)
	for i := range hours {
		hours[i] = i % 24
	}
	minutes := make([]int, 600)
	for i := range minutes {
		minutes[i] = i % 60
	}
	rule := recurrenceRule{
		Freq:     "DAILY",
		Interval: 1,
		Until:    &until,
		ByHour:   hours,
		ByMinute: minutes,
	}

	occurrences := 0
	exceeds, handled := finishSparseRecurrence(dtstart, dtstart, rule, &occurrences, func(time.Time) bool {
		return false
	})
	if !handled || !exceeds {
		t.Fatalf("finishSparseRecurrence() = (exceeds=%t, handled=%t), want (true, true) when a day's clock combinations exceed recurrencePeriodWorkLimit", exceeds, handled)
	}
}

func requireRecurrenceResult[T any](t *testing.T) func([]T, error) []T {
	t.Helper()
	return func(values []T, err error) []T {
		t.Helper()
		if err != nil {
			t.Fatal(err)
		}
		return values
	}
}

func TestSparseRecurrenceReadsPreserveCountAndExceptions(t *testing.T) {
	start := time.Date(2026, 1, 1, 9, 0, 0, 0, time.UTC)
	for _, freq := range []string{"MINUTELY", "SECONDLY"} {
		for _, test := range []struct {
			name, extra, override string
			year, hour, count     int
		}{
			{name: "second occurrence", year: 2027, hour: 9, count: 1},
			{name: "count exhausted", year: 2028, hour: 9, count: 0},
			{name: "excluded first still counts", extra: "EXDATE:20260101T090000Z\r\n", year: 2028, hour: 9, count: 0},
			{name: "excluded second", extra: "EXDATE:20270101T090000Z\r\n", year: 2027, hour: 9, count: 0},
			{name: "override", override: "BEGIN:VEVENT\r\nUID:sparse\r\nRECURRENCE-ID:20270101T090000Z\r\nDTSTART:20270101T120000Z\r\nDTEND:20270101T130000Z\r\nEND:VEVENT\r\n", year: 2027, hour: 12, count: 1},
			{name: "range override", override: "BEGIN:VEVENT\r\nUID:sparse\r\nRECURRENCE-ID;RANGE=THISANDFUTURE:20260101T090000Z\r\nDTSTART:20260101T120000Z\r\nDTEND:20260101T130000Z\r\nEND:VEVENT\r\n", year: 2027, hour: 12, count: 1},
		} {
			t.Run(freq+"/"+test.name, func(t *testing.T) {
				raw := "BEGIN:VCALENDAR\r\nBEGIN:VEVENT\r\nUID:sparse\r\nDTSTART:20260101T090000Z\r\nDTEND:20260101T100000Z\r\nRRULE:FREQ=" + freq + ";BYMONTH=1;BYMONTHDAY=1;BYHOUR=9;BYMINUTE=0;BYSECOND=0;COUNT=2\r\n" + test.extra + "END:VEVENT\r\n" + test.override + "END:VCALENDAR\r\n"
				from := time.Date(test.year, 1, 1, test.hour, 0, 0, 0, time.UTC)
				periods, err := RecurringBusyPeriods(raw, start, time.Hour, from, from.Add(time.Hour), 1000, nil)
				if err != nil {
					t.Fatal(err)
				}
				if len(periods) != test.count {
					t.Fatalf("periods = %#v, want %d", periods, test.count)
				}
				if len(periods) > 0 && !periods[0].Start.Equal(from) {
					t.Fatalf("start = %v, want %v", periods[0].Start, from)
				}
			})
		}
	}
}

func TestRecurrenceInstancePeriodDurationAndRangeOverride(t *testing.T) {
	start := time.Date(2024, 6, 1, 9, 0, 0, 0, time.UTC)
	base := "BEGIN:VCALENDAR\r\nVERSION:2.0\r\nBEGIN:VEVENT\r\nUID:period\r\nDTSTART:20240601T090000Z\r\nDTEND:20240601T100000Z\r\nRRULE:FREQ=DAILY;COUNT=5\r\nRDATE;VALUE=PERIOD:20240605T090000Z/PT3H\r\nEND:VEVENT\r\n"
	for _, override := range []bool{false, true} {
		t.Run(strconv.FormatBool(override), func(t *testing.T) {
			raw := base
			if override {
				raw += "BEGIN:VEVENT\r\nUID:period\r\nRECURRENCE-ID;RANGE=THISANDFUTURE:20240603T090000Z\r\nDTSTART:20240603T100000Z\r\nDTEND:20240603T140000Z\r\nEND:VEVENT\r\n"
			}
			raw += "END:VCALENDAR\r\n"
			instances, err := RecurrenceInstances(raw, "VEVENT", start, time.Hour, start.AddDate(0, 0, 4), start.AddDate(0, 0, 5), 10, nil)
			if err != nil || len(instances) != 1 {
				t.Fatalf("%+v, %v", instances, err)
			}
			wantStart := start.AddDate(0, 0, 4)
			wantEnd := wantStart.Add(3 * time.Hour)
			if override {
				wantStart = wantStart.Add(time.Hour)
				wantEnd = wantStart.Add(4 * time.Hour)
			}
			got := instances[0]
			if !got.Start.Equal(wantStart) || !got.End.Equal(wantEnd) || !got.RecurrenceID.Equal(start.AddDate(0, 0, 4)) {
				t.Fatalf("got %+v, want %s/%s", got, wantStart, wantEnd)
			}
		})
	}
}

// A recurrence rule arrives from the network, so no rule may take unbounded
// time to evaluate. Both entry points below are reachable before storage: the
// first from a VTIMEZONE observance on PUT, the second from a REPORT whose
// time-range has no end. A rule whose BY parts can never be satisfied, or whose
// DTSTART predates the request by more than time.Duration's ~292-year range,
// must not leave the fast-forward stepping one period at a time.
func TestRecurrenceEvaluationAlwaysTerminates(t *testing.T) {
	// Exchange emits 16010101 as the DTSTART of a VTIMEZONE observance, so the
	// span from dtstart to the query is past what time.Duration can represent.
	dtstart := time.Date(1601, 1, 1, 0, 0, 0, 0, time.UTC)
	before := time.Date(2026, 9, 17, 0, 0, 0, 0, time.UTC)

	rules := []string{
		"FREQ=SECONDLY",
		"FREQ=SECONDLY;BYMONTH=2;BYMONTHDAY=30",
		"FREQ=SECONDLY;BYYEARDAY=366;BYMONTH=1",
		"FREQ=MINUTELY;BYMONTH=2;BYMONTHDAY=30",
		"FREQ=HOURLY;BYMONTH=2;BYMONTHDAY=30",
		"FREQ=SECONDLY;UNTIL=16010101T001000Z",
	}
	for _, rule := range rules {
		t.Run(rule, func(t *testing.T) {
			done := make(chan struct{})
			go func() {
				defer close(done)
				LatestRecurrenceOnOrBefore(dtstart, before, rule)
			}()
			select {
			case <-done:
			case <-time.After(10 * time.Second):
				t.Fatalf("LatestRecurrenceOnOrBefore did not return for %q", rule)
			}
		})
	}
}

// A stored sub-daily rule that ended long ago must not be walked forward one
// period at a time to reach a far-future range start: the scan can stop at
// UNTIL, because no instance exists past it.
func TestRecurringBusyPeriodsTerminatesForAFinishedSubDailyRule(t *testing.T) {
	dtstart := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	// The VCALENDAR wrapper is what makes the VEVENT reachable: the component
	// walk accepts a component nested one level inside the object, so a bare
	// BEGIN:VEVENT is found by nothing and exercises none of the scan below.
	raw := recurringTestComponent("FREQ=SECONDLY;UNTIL=20240101T000959Z")
	rangeStart := time.Date(2400, 1, 1, 0, 0, 0, 0, time.UTC)
	rangeEnd := rangeStart.AddDate(0, 0, 1)

	type result struct {
		periods []BusyPeriod
		err     error
	}
	done := make(chan result, 1)
	go func() {
		periods, err := RecurringBusyPeriods(raw, dtstart, time.Hour, rangeStart, rangeEnd, MaxRecurrenceInstances, nil)
		done <- result{periods: periods, err: err}
	}()
	select {
	case got := <-done:
		if got.err != nil {
			t.Fatalf("RecurringBusyPeriods() err = %v, want nil", got.err)
		}
		if len(got.periods) != 0 {
			t.Fatalf("periods = %d, want 0: the rule ended in 2024", len(got.periods))
		}
	case <-time.After(10 * time.Second):
		t.Fatal("RecurringBusyPeriods did not return for a rule that ended in 2024")
	}
}

// A rule with neither COUNT nor UNTIL generates instances forever, so
// CALDAV:max-instances cannot be read as a count of the whole set without
// refusing "repeats weekly, no end date" -- the shape iOS, Thunderbird,
// Evolution and DAVx5 all emit by default. The limit is applied instead to the
// instances the rule generates in one year from DTSTART, which still refuses
// the sub-daily rules whose expansion is genuinely expensive.
func TestRecurrenceSetExceedsLimitAdmitsOrdinaryUnboundedRules(t *testing.T) {
	object := func(rrule string) string {
		return "BEGIN:VCALENDAR\r\nVERSION:2.0\r\nBEGIN:VEVENT\r\nUID:unbounded\r\n" +
			"DTSTART:20240101T090000Z\r\nDTEND:20240101T100000Z\r\n" +
			rrule + "\r\nEND:VEVENT\r\nEND:VCALENDAR\r\n"
	}
	for _, tt := range []struct {
		rrule       string
		wantExceeds bool
	}{
		{rrule: "RRULE:FREQ=DAILY", wantExceeds: false},
		{rrule: "RRULE:FREQ=WEEKLY", wantExceeds: false},
		{rrule: "RRULE:FREQ=WEEKLY;BYDAY=MO,WE,FR", wantExceeds: false},
		{rrule: "RRULE:FREQ=MONTHLY", wantExceeds: false},
		{rrule: "RRULE:FREQ=YEARLY", wantExceeds: false},
		{rrule: "RRULE:FREQ=DAILY;INTERVAL=2", wantExceeds: false},
		// Sub-daily rules stay refused: an unbounded hourly rule is already
		// past the limit inside the first year.
		{rrule: "RRULE:FREQ=HOURLY", wantExceeds: true},
		{rrule: "RRULE:FREQ=MINUTELY", wantExceeds: true},
		{rrule: "RRULE:FREQ=SECONDLY", wantExceeds: true},
		// A rule that declares its own size is still measured against it.
		{rrule: "RRULE:FREQ=DAILY;COUNT=2001", wantExceeds: true},
		{rrule: "RRULE:FREQ=DAILY;COUNT=100", wantExceeds: false},
	} {
		t.Run(tt.rrule, func(t *testing.T) {
			exceeds, valid := RecurrenceSetExceedsLimit(object(tt.rrule), MaxRecurrenceInstances)
			if !valid {
				t.Fatalf("%s was not recognized as a valid recurrence set", tt.rrule)
			}
			if exceeds != tt.wantExceeds {
				t.Errorf("%s: exceeds=%v, want %v", tt.rrule, exceeds, tt.wantExceeds)
			}
		})
	}
}

// An accepted unbounded rule has to be stored with an open-ended upper bound,
// or the SQL candidate filter drops it from every time-range query past the
// bound it did record.
func TestConservativeRecurrenceBoundsLeaveUnboundedRulesOpenEnded(t *testing.T) {
	raw := "BEGIN:VCALENDAR\r\nVERSION:2.0\r\nBEGIN:VEVENT\r\nUID:unbounded\r\n" +
		"DTSTART:20240101T090000Z\r\nDTEND:20240101T100000Z\r\n" +
		"RRULE:FREQ=WEEKLY\r\nEND:VEVENT\r\nEND:VCALENDAR\r\n"
	bounds := ConservativeRecurrenceBounds(raw)
	if !bounds.Recurring {
		t.Fatal("an unbounded weekly rule was not recognized as recurring")
	}
	// UntilUnknown is the signal the store turns into RecurrenceUntilSentinel.
	// A computed upper bound here would cut the series off at it.
	if !bounds.UntilUnknown {
		t.Errorf("unbounded rule reported a known upper bound (%v), so it would stop being a time-range candidate past it", bounds.Until)
	}
}

// A BYSETPOS ordinal larger than any period of the rule can ever produce
// selects nothing, whatever period the scan reaches. Proving that by
// generating a hundred thousand periods costs seconds of CPU for a two-hundred
// byte object, on the PUT gate and on every expansion of the stored resource,
// so the answer has to come from the rule rather than from the scan.
func TestUnreachableBySetPosIsAnsweredWithoutScanning(t *testing.T) {
	rules := []string{
		// One candidate per period: a second is a second.
		"FREQ=SECONDLY;COUNT=999;BYSETPOS=-366;BYDAY=SA,SU",
		"FREQ=SECONDLY;COUNT=999;BYDAY=SA,SU;BYSETPOS=366",
		// A minute offers only its BYSECOND values, and there are none.
		"FREQ=MINUTELY;COUNT=1000;BYSETPOS=366;BYDAY=SA,SU",
		// A year holds at most 53 Saturdays and 53 Sundays.
		"FREQ=YEARLY;BYDAY=SA,SU;COUNT=999;BYSETPOS=366",
		// Twenty-eight named month days cannot reach a twenty-ninth position.
		"FREQ=MONTHLY;BYMONTHDAY=1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28;COUNT=1000;BYSETPOS=31",
		// Two weekdays share the fifty-two complete weeks of a year and the two
		// days left over, so the longest year holds 106 of them.
		"FREQ=YEARLY;BYDAY=SA,SU;COUNT=999;BYSETPOS=107",
		// Five weekdays share the four complete weeks of a month and the three
		// days left over, so the longest month holds 23 of them, not 25.
		"FREQ=MONTHLY;BYDAY=MO,TU,WE,TH,FR;COUNT=999;BYSETPOS=25",
	}
	// Generous for an answer read off the rule, and far below what scanning a
	// hundred thousand periods costs.
	const budget = 250 * time.Millisecond
	dtstart := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	rangeEnd := time.Date(2098, 1, 1, 0, 0, 0, 0, time.UTC)

	for _, rule := range rules {
		t.Run(rule, func(t *testing.T) {
			raw := recurringTestComponent(rule)

			started := time.Now()
			exceeds, valid := RecurrenceSetExceedsLimit(raw, MaxRecurrenceInstances)
			if elapsed := time.Since(started); elapsed > budget {
				t.Errorf("RecurrenceSetExceedsLimit took %v, want at most %v", elapsed, budget)
			}
			if !valid {
				t.Fatal("RecurrenceSetExceedsLimit() valid = false")
			}
			if exceeds {
				t.Error("a rule that selects nothing was reported as exceeding the instance limit")
			}

			started = time.Now()
			periods, err := RecurringBusyPeriods(raw, dtstart, time.Hour, dtstart, rangeEnd, MaxRecurrenceInstances, nil)
			if elapsed := time.Since(started); elapsed > budget {
				t.Errorf("RecurringBusyPeriods took %v, want at most %v", elapsed, budget)
			}
			if err != nil {
				t.Errorf("RecurringBusyPeriods() err = %v, want nil: the rule selects nothing, which is an answer", err)
			}
			// RFC 5545 §3.8.5.3 keeps the DTSTART as the one instance of the set.
			if len(periods) != 1 || !periods[0].Start.Equal(dtstart) {
				t.Errorf("RecurringBusyPeriods() = %v, want the DTSTART alone", periods)
			}
		})
	}
}

// The reasoning that refuses an unreachable BYSETPOS must not reach a position
// the rule does select, or an ordinary "last working day of the month" event
// stops generating instances. The DTSTART is not one the rules select, so it
// leads each set and takes the first of its COUNT (RFC 5545 §3.8.5.3).
func TestReachableBySetPosStillSelects(t *testing.T) {
	dtstart := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	rangeEnd := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)
	tests := map[string]struct {
		rrule string
		want  []string
	}{
		"last weekday of the month": {
			rrule: "FREQ=MONTHLY;BYDAY=MO,TU,WE,TH,FR;BYSETPOS=-1;COUNT=4",
			want:  []string{"2024-01-01", "2024-01-31", "2024-02-29", "2024-03-29"},
		},
		"second Saturday of the year": {
			rrule: "FREQ=YEARLY;BYDAY=SA;BYSETPOS=2;COUNT=2",
			want:  []string{"2024-01-01", "2024-01-13"},
		},
		// 104 weekend days fall in 2024, so the last of them is reachable.
		"final weekend day of the year": {
			rrule: "FREQ=YEARLY;BYDAY=SA,SU;BYSETPOS=104;COUNT=2",
			want:  []string{"2024-01-01", "2024-12-29"},
		},
		"twenty-eighth named month day": {
			rrule: "FREQ=MONTHLY;BYMONTHDAY=1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28;BYSETPOS=28;COUNT=3",
			want:  []string{"2024-01-01", "2024-01-28", "2024-02-28"},
		},
		// The weekday bound's boundary: 23 working days do fall in a 31-day
		// month whose first three days are working days.
		"twenty-third working day of the month": {
			rrule: "FREQ=MONTHLY;BYDAY=MO,TU,WE,TH,FR;BYSETPOS=23;COUNT=3",
			want:  []string{"2024-01-01", "2024-01-31", "2024-05-31"},
		},
	}
	for name, tt := range tests {
		t.Run(name, func(t *testing.T) {
			periods, err := RecurringBusyPeriods(recurringTestComponent(tt.rrule), dtstart, time.Hour,
				dtstart, rangeEnd, MaxRecurrenceInstances, nil)
			if err != nil {
				t.Fatalf("RecurringBusyPeriods() err = %v", err)
			}
			var got []string
			for _, period := range periods {
				got = append(got, period.Start.Format("2006-01-02"))
			}
			if strings.Join(got, ",") != strings.Join(tt.want, ",") {
				t.Errorf("starts = %v, want %v", got, tt.want)
			}
		})
	}
}

// CALDAV:max-instances bounds what one resource may store, measured over a year
// for a rule that never stops. Reading it as a bound on the instances a
// requested range contains would make an ordinary "repeats daily, no end date"
// event unanswerable past the first 1000 days.
func TestRecurringBusyPeriodsCoversTheWholeRequestedRange(t *testing.T) {
	dtstart := time.Date(2024, 1, 1, 9, 0, 0, 0, time.UTC)
	tests := map[string]struct {
		rrule string
		years int
		want  int
	}{
		"daily over three years":  {rrule: "FREQ=DAILY", years: 3, want: 1096},
		"daily over ten years":    {rrule: "FREQ=DAILY", years: 10, want: 3653},
		"weekly over forty years": {rrule: "FREQ=WEEKLY", years: 40, want: 2088},
	}
	for name, tt := range tests {
		t.Run(name, func(t *testing.T) {
			rangeEnd := dtstart.AddDate(tt.years, 0, 0)
			periods, err := RecurringBusyPeriods(recurringTestComponent(tt.rrule), dtstart, time.Hour,
				dtstart, rangeEnd, 50_000, nil)
			if err != nil {
				t.Fatalf("RecurringBusyPeriods() err = %v", err)
			}
			if len(periods) != tt.want {
				t.Errorf("periods = %d, want %d", len(periods), tt.want)
			}
		})
	}
}

// An expansion cut short by the instance budget still owes the caller the
// periods it did collect: every one of them reaches the requested range, and
// the caller decides whether a partial set answers its question.
func TestRecurringBusyPeriodsReturnsThePeriodsCollectedBeforeTheBudgetRanOut(t *testing.T) {
	dtstart := time.Date(2024, 1, 1, 9, 0, 0, 0, time.UTC)
	periods, err := RecurringBusyPeriods(recurringTestComponent("FREQ=DAILY"), dtstart, time.Hour,
		dtstart, dtstart.AddDate(3, 0, 0), 10, nil)
	if !errors.Is(err, ErrRecurrenceExpansionLimit) {
		t.Fatalf("err = %v, want ErrRecurrenceExpansionLimit", err)
	}
	if len(periods) != 10 {
		t.Fatalf("periods = %d, want the 10 generated before the budget ran out", len(periods))
	}
	if !periods[0].Start.Equal(dtstart) {
		t.Errorf("first period = %v, want %v", periods[0].Start, dtstart)
	}
}

// Running out of the caller's output budget says the range holds more
// instances than the caller will take; running out of scan work says the
// stored rule cannot be expanded over the range at all. Both are incomplete
// expansions, but only the second is ErrRecurrenceScanLimit.
func TestRecurrenceExpansionDistinguishesScanFromOutputExhaustion(t *testing.T) {
	dtstart := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)

	_, err := RecurringBusyPeriods(recurringTestComponent("FREQ=DAILY"), dtstart, time.Hour,
		dtstart, dtstart.AddDate(1, 0, 0), 10, nil)
	if !errors.Is(err, ErrRecurrenceExpansionLimit) || errors.Is(err, ErrRecurrenceScanLimit) {
		t.Fatalf("output budget exhaustion err = %v, want ErrRecurrenceExpansionLimit alone", err)
	}

	dense := "FREQ=YEARLY;BYDAY=MO,TU,WE,TH,FR,SA,SU;BYHOUR=" + enumeratedInts(24) +
		";BYMINUTE=" + enumeratedInts(60) + ";BYSECOND=" + enumeratedInts(60) + ";BYSETPOS=-1"
	_, err = RecurringBusyPeriods(recurringTestComponent(dense), dtstart, time.Second,
		dtstart, dtstart.AddDate(1, 0, 0), 10_000_000, nil)
	if !errors.Is(err, ErrRecurrenceScanLimit) {
		t.Fatalf("scan work exhaustion err = %v, want ErrRecurrenceScanLimit", err)
	}
	if !errors.Is(err, ErrRecurrenceExpansionLimit) {
		t.Fatalf("scan work exhaustion err = %v, want it to remain an ErrRecurrenceExpansionLimit", err)
	}
}

// RFC 5545 §3.2 makes a parameter value a quoted string whenever it contains a
// colon, a semicolon or a comma, and Exchange quotes every TZID unconditionally.
// A reader that keeps the quotes resolves no zone at all, which silently moves
// every recurrence instance by the whole UTC offset.
func TestPropertyParameterValuesAreUnquoted(t *testing.T) {
	tests := map[string]struct {
		keyPart string
		want    string
	}{
		"unquoted":              {keyPart: "DTSTART;TZID=America/New_York", want: "America/New_York"},
		"quoted":                {keyPart: `DTSTART;TZID="America/New_York"`, want: "America/New_York"},
		"quoted with delimiter": {keyPart: `DTSTART;TZID="(UTC-08:00) Pacific Standard Time";VALUE=DATE-TIME`, want: "(UTC-08:00) Pacific Standard Time"},
	}
	for name, tt := range tests {
		t.Run(name, func(t *testing.T) {
			got, ok := PropertyParam(tt.keyPart, "TZID")
			if !ok || got != tt.want {
				t.Errorf("PropertyParam() = %q, %v, want %q, true", got, ok, tt.want)
			}
		})
	}

	// A semicolon inside the quoted value must not be read as the start of
	// another parameter.
	if got, ok := PropertyParam(`DTSTART;TZID="a;b";VALUE=DATE`, "VALUE"); !ok || got != "DATE" {
		t.Errorf("PropertyParam(VALUE) = %q, %v, want \"DATE\", true", got, ok)
	}
	if !PropertyParamEquals(`RECURRENCE-ID;RANGE="THISANDFUTURE"`, "RANGE", "THISANDFUTURE") {
		t.Error("a quoted RANGE parameter was not recognized")
	}
}

// A quoted parameter value may itself contain a colon -- Exchange names its
// zones "(UTC-05:00) Eastern Time (US & Canada)" -- so the value of a content
// line begins at the first colon outside double quotes. Splitting at the first
// colon of any kind reads the rest of the zone name as the value, which neither
// the DTSTART nor an EXDATE carrying that TZID can then be parsed from.
func TestContentLinesSplitAtTheFirstColonOutsideQuotes(t *testing.T) {
	const tzid = `"(UTC-05:00) Eastern"`
	raw := "BEGIN:VCALENDAR\r\nBEGIN:VEVENT\r\nUID:quoted\r\n" +
		"DTSTART;TZID=" + tzid + ":20240108T090000\r\n" +
		"RRULE:FREQ=DAILY;COUNT=3\r\n" +
		"EXDATE;TZID=" + tzid + ";VALUE=DATE-TIME:20240109T090000\r\n" +
		"END:VEVENT\r\nEND:VCALENDAR\r\n"

	component := PrimaryVEventComponent(raw)
	dtstart, ok := ComponentProperty(component, "DTSTART")
	if !ok {
		t.Fatal("DTSTART was not found")
	}
	if dtstart.Value != "20240108T090000" {
		t.Fatalf("DTSTART value = %q, want %q", dtstart.Value, "20240108T090000")
	}
	if got, ok := PropertyParam(dtstart.KeyPart, "TZID"); !ok || got != "(UTC-05:00) Eastern" {
		t.Fatalf("DTSTART TZID = %q, %v, want %q", got, ok, "(UTC-05:00) Eastern")
	}
	exdate, ok := ComponentProperty(component, "EXDATE")
	if !ok || exdate.Value != "20240109T090000" {
		t.Fatalf("EXDATE = %#v, %v, want value %q", exdate, ok, "20240109T090000")
	}

	eastern := time.FixedZone("Eastern", -5*60*60)
	resolve := func(keyPart, value string) (time.Time, bool) {
		if zone, ok := PropertyParam(keyPart, "TZID"); ok && zone == "(UTC-05:00) Eastern" {
			parsed, err := ParseDateTimeInLocation(strings.TrimSpace(value), eastern)
			return parsed, err == nil
		}
		return ParsePropertyDateTimeLocal(keyPart, value)
	}
	start := time.Date(2024, 1, 8, 9, 0, 0, 0, eastern)
	periods, err := RecurringBusyPeriods(raw, start, time.Hour, start.AddDate(0, 0, -1), start.AddDate(0, 0, 7), MaxRecurrenceInstances, resolve)
	if err != nil {
		t.Fatalf("RecurringBusyPeriods() err = %v", err)
	}
	want := []time.Time{start, start.AddDate(0, 0, 2)}
	if len(periods) != len(want) {
		t.Fatalf("periods = %v, want starts %v: the EXDATE was not applied", periods, want)
	}
	for i, period := range periods {
		if !period.Start.Equal(want[i]) {
			t.Errorf("period %d starts at %v, want %v", i, period.Start, want[i])
		}
	}

	exceeds, valid := RecurrenceSetExceedsLimit(raw, MaxRecurrenceInstances)
	if !valid || exceeds {
		t.Fatalf("RecurrenceSetExceedsLimit() = %v, %v, want false, true", exceeds, valid)
	}
}

// The zone a quoted TZID names has to resolve the same way the unquoted
// spelling does, or a master's own DTSTART and the instances generated from it
// land on different instants.
func TestParsePropertyDateTimeLocalResolvesAQuotedTZID(t *testing.T) {
	want, ok := ParsePropertyDateTimeLocal("DTSTART;TZID=America/New_York", "20240101T090000")
	if !ok {
		t.Fatal("the unquoted spelling did not resolve")
	}
	got, ok := ParsePropertyDateTimeLocal(`DTSTART;TZID="America/New_York"`, "20240101T090000")
	if !ok {
		t.Fatal("the quoted spelling did not resolve")
	}
	if !got.Equal(want) {
		t.Errorf("quoted TZID resolved to %v, want %v", got, want)
	}
}

// exDatesObject is a daily rule of n instances from 2024 carrying n EXDATEs,
// one a minute from 1900, none of which names an instance.
func exDatesObject(n int) string {
	var excluded strings.Builder
	excluded.WriteString("EXDATE:")
	start := time.Date(1900, 1, 1, 0, 0, 0, 0, time.UTC)
	for i := 0; i < n; i++ {
		if i > 0 {
			excluded.WriteByte(',')
		}
		excluded.WriteString(start.Add(time.Duration(i) * time.Minute).Format("20060102T150405Z"))
	}
	return "BEGIN:VCALENDAR\r\nBEGIN:VEVENT\r\nUID:excluded\r\n" +
		"DTSTART:20240101T000000Z\r\nRRULE:FREQ=DAILY;COUNT=" + strconv.Itoa(n) + "\r\n" +
		excluded.String() + "\r\nEND:VEVENT\r\nEND:VCALENDAR\r\n"
}

// EXDATE exclusion is tested once per generated instance, and RFC 5545
// §3.8.5.1 bounds neither count, so the expansion's work has to grow with the
// instances plus the exclusions rather than with their product. A 9 MB object
// is accepted well inside the body limit and carries hundreds of thousands.
func TestRecurringBusyPeriodsScalesWithManyExDates(t *testing.T) {
	dtstart := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	expand := func(t *testing.T, raw string, n int) []BusyPeriod {
		t.Helper()
		periods, err := RecurringBusyPeriods(raw, dtstart, time.Hour, dtstart, dtstart.AddDate(0, 0, n), n, nil)
		if err != nil {
			t.Fatalf("RecurringBusyPeriods() err = %v", err)
		}
		return periods
	}
	const exdates = 1_000
	if periods := expand(t, exDatesObject(exdates), exdates); len(periods) != exdates {
		t.Fatalf("periods = %d, want %d", len(periods), exdates)
	}

	assertScalesLinearly(t, 8_000, func(n int) func() {
		raw := exDatesObject(n)
		return func() { expand(t, raw, n) }
	})
}

// manyOverridesObject is a daily rule from 1900 whose first overrides slots are
// each replaced by an override component. RFC 5545 §3.8.4.4 bounds neither how
// many a resource may carry nor what they may say, and the body limit admits
// tens of thousands of them.
func manyOverridesObject(overrides int, override func(recurrenceID string) string) string {
	var b strings.Builder
	b.WriteString("BEGIN:VCALENDAR\r\nBEGIN:VEVENT\r\nUID:overridden\r\n" +
		"DTSTART:19000101T090000Z\r\nDTEND:19000101T100000Z\r\nRRULE:FREQ=DAILY\r\nEND:VEVENT\r\n")
	first := time.Date(1900, 1, 1, 9, 0, 0, 0, time.UTC)
	for i := 0; i < overrides; i++ {
		b.WriteString("BEGIN:VEVENT\r\nUID:overridden\r\n")
		b.WriteString(override(first.AddDate(0, 0, i).Format("20060102T150405Z")))
		b.WriteString("END:VEVENT\r\n")
	}
	b.WriteString("END:VCALENDAR\r\n")
	return b.String()
}

// maxLinearScalingRatio separates work linear in its input, which grows about
// fourfold when the input does, from quadratic work, which grows sixteenfold,
// with room on both sides for timing noise.
const maxLinearScalingRatio = 12

// linearScalingFloor is the duration below which the larger run is too short
// to time against noise. Callers size their inputs so a quadratic path takes
// several times longer than this at 4n.
const linearScalingFloor = 200 * time.Millisecond

// assertScalesLinearly times the work prepare builds for n and for 4n and fails
// when the larger took more than maxLinearScalingRatio times the smaller. A
// ratio lets machine speed and steady load cancel out, and interleaving five
// runs of each size and keeping the fastest stops one stall from deciding the
// outcome. Fixtures are built by prepare outside the timing.
func assertScalesLinearly(t *testing.T, n int, prepare func(n int) func()) {
	t.Helper()
	if raceDetectorEnabled {
		t.Skip("scaling is measured without the race detector, which multiplies every run")
	}
	if testing.Short() {
		t.Skip("scaling measurement skipped in -short mode")
	}
	small, large := prepare(n), prepare(4*n)
	fastest := func(run func(), best time.Duration) time.Duration {
		started := time.Now()
		run()
		return min(best, time.Since(started))
	}
	smallBest, largeBest := time.Duration(math.MaxInt64), time.Duration(math.MaxInt64)
	for range 5 {
		smallBest = fastest(small, smallBest)
		largeBest = fastest(large, largeBest)
	}
	ratio := float64(largeBest) / float64(smallBest)
	t.Logf("n=%d took %v, 4n took %v: ratio %.1f", n, smallBest, largeBest, ratio)
	if largeBest < linearScalingFloor {
		return
	}
	if ratio > maxLinearScalingRatio {
		t.Errorf("work at 4n took %v against %v at n=%d, a ratio of %.1f; linear work stays under %d",
			largeBest, smallBest, n, ratio, maxLinearScalingRatio)
	}
}

// cancelledOverridesWithGoverningShift is manyOverridesObject with its n
// overrides cancelled, followed by a RANGE=THISANDFUTURE override at slot n
// that moves every later instance an hour on.
func cancelledOverridesWithGoverningShift(n int) string {
	raw := manyOverridesObject(n, func(id string) string {
		return "RECURRENCE-ID:" + id + "\r\nSTATUS:CANCELLED\r\n"
	})
	slot := time.Date(1900, 1, 1, 9, 0, 0, 0, time.UTC).AddDate(0, 0, n)
	return strings.Replace(raw, "END:VCALENDAR\r\n",
		"BEGIN:VEVENT\r\nUID:overridden\r\nRECURRENCE-ID;RANGE=THISANDFUTURE:"+slot.Format("20060102T150405Z")+"\r\n"+
			"DTSTART:"+slot.Add(time.Hour).Format("20060102T150405Z")+"\r\n"+
			"DTEND:"+slot.Add(2*time.Hour).Format("20060102T150405Z")+"\r\nEND:VEVENT\r\nEND:VCALENDAR\r\n", 1)
}

// busyPeriodsOverDays expands raw over the first days slots of its rule.
func busyPeriodsOverDays(t *testing.T, raw string, days int) []BusyPeriod {
	t.Helper()
	dtstart := time.Date(1900, 1, 1, 9, 0, 0, 0, time.UTC)
	periods, err := RecurringBusyPeriods(raw, dtstart, time.Hour, dtstart, dtstart.AddDate(0, 0, days), 4*days, nil)
	if err != nil {
		t.Fatalf("RecurringBusyPeriods() err = %v", err)
	}
	return periods
}

// Every generated instance is checked against the overrides for an exact
// RECURRENCE-ID and for a governing RANGE=THISANDFUTURE one, and a resource may
// carry tens of thousands of overrides, so the expansion's work has to grow
// with the instances plus the overrides rather than with their product.
func TestRecurringBusyPeriodsScalesWithManyOverrides(t *testing.T) {
	const overrides = 1_000
	periods := busyPeriodsOverDays(t, cancelledOverridesWithGoverningShift(overrides), 2*overrides)
	if len(periods) != overrides {
		t.Fatalf("periods = %d, want %d: every slot but the cancelled ones", len(periods), overrides)
	}
	for _, period := range periods {
		if period.Start.Sub(period.RecurrenceID) != time.Hour {
			t.Fatalf("period %v of slot %v was not moved by the governing THISANDFUTURE override", period.Start, period.RecurrenceID)
		}
	}

	assertScalesLinearly(t, 8_000, func(n int) func() {
		raw := cancelledOverridesWithGoverningShift(n)
		return func() { busyPeriodsOverDays(t, raw, 2*n) }
	})
}

// A STATUS:CANCELLED override replaces the instance it names rather than
// adding one: RFC 5545 removes an instance only through EXDATE, so the slot is
// still an instance of the set, counted once whichever component describes it.
// One naming no slot the rule generates describes nothing and adds nothing.
// The components are bounded on their own, since each is read by every
// expansion of the resource whether or not it adds an instance.
func TestRecurrenceSetExceedsLimitCountsCancelledOverridesByTheSlotsTheyName(t *testing.T) {
	countedRule := func(overrides string) string {
		return "BEGIN:VCALENDAR\r\nBEGIN:VEVENT\r\nUID:counted\r\nDTSTART:20240101T090000Z\r\n" +
			"RRULE:FREQ=DAILY;COUNT=" + strconv.Itoa(MaxRecurrenceInstances) + "\r\nEND:VEVENT\r\n" +
			overrides + "END:VCALENDAR\r\n"
	}
	cancelledAt := func(hour int) string {
		var b strings.Builder
		for day := 0; day < 10; day++ {
			slot := time.Date(2024, 1, 1+day, hour, 0, 0, 0, time.UTC)
			b.WriteString("BEGIN:VEVENT\r\nUID:counted\r\nRECURRENCE-ID:" + slot.Format("20060102T150405Z") +
				"\r\nSTATUS:CANCELLED\r\nEND:VEVENT\r\n")
		}
		return b.String()
	}
	cancelled := func(id string) string { return "RECURRENCE-ID:" + id + "\r\nSTATUS:CANCELLED\r\n" }
	tests := map[string]struct {
		raw     string
		exceeds bool
	}{
		"cancelled overrides of generated slots at the limit": {raw: countedRule(cancelledAt(9)), exceeds: false},
		"orphan cancelled overrides at the limit":             {raw: countedRule(cancelledAt(10)), exceeds: false},
		"override components at their own bound":              {raw: manyOverridesObject(maxRecurrenceOverrideComponents, cancelled), exceeds: false},
		"override components past their own bound":            {raw: manyOverridesObject(maxRecurrenceOverrideComponents+1, cancelled), exceeds: true},
	}
	for name, tt := range tests {
		t.Run(name, func(t *testing.T) {
			exceeds, valid := RecurrenceSetExceedsLimit(tt.raw, MaxRecurrenceInstances)
			if !valid {
				t.Fatal("RecurrenceSetExceedsLimit() valid = false")
			}
			if exceeds != tt.exceeds {
				t.Fatalf("RecurrenceSetExceedsLimit() exceeds = %t, want %t", exceeds, tt.exceeds)
			}
		})
	}
}

// RFC 5545 §3.8.5.3 makes the DTSTART the first instance of the set even when
// it falls after UNTIL, so expansion and the CALDAV:max-instances check both
// hold it -- whether or not the rule would generate it.
func TestRecurrenceSetKeepsADTStartAfterUntil(t *testing.T) {
	dtstart := time.Date(2024, 1, 1, 9, 0, 0, 0, time.UTC)
	for _, rrule := range []string{
		"FREQ=MONTHLY;BYMONTHDAY=15;UNTIL=20231231T000000Z",
		"FREQ=DAILY;UNTIL=20231231T000000Z",
	} {
		t.Run(rrule, func(t *testing.T) {
			raw := "BEGIN:VCALENDAR\r\nBEGIN:VEVENT\r\nUID:late\r\nDTSTART:20240101T090000Z\r\n" +
				"RRULE:" + rrule + "\r\nEND:VEVENT\r\nEND:VCALENDAR\r\n"
			periods, err := RecurringBusyPeriods(raw, dtstart, time.Hour, dtstart.AddDate(-1, 0, 0), dtstart.AddDate(1, 0, 0), MaxRecurrenceInstances, nil)
			if err != nil {
				t.Fatalf("RecurringBusyPeriods() err = %v", err)
			}
			if len(periods) != 1 || !periods[0].Start.Equal(dtstart) {
				t.Fatalf("periods = %v, want the DTSTART alone", periods)
			}
			if exceeds, valid := RecurrenceSetExceedsLimit(raw, 0); !valid || !exceeds {
				t.Fatalf("RecurrenceSetExceedsLimit(limit 0) = %t, %t, want the DTSTART counted", exceeds, valid)
			}
			if exceeds, valid := RecurrenceSetExceedsLimit(raw, 1); !valid || exceeds {
				t.Fatalf("RecurrenceSetExceedsLimit(limit 1) = %t, %t, want the DTSTART alone counted", exceeds, valid)
			}
		})
	}
}

// Whether the rule generates the DTSTART is decided from the period holding it.
// When that period is too large to generate, the DTSTART is still an instance
// of the set, so it is collected before the scan limit is reported.
func TestRecurrenceSetCollectsTheDTStartBeforeAScanLimit(t *testing.T) {
	dtstart := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	dense := "FREQ=YEARLY;BYDAY=MO,TU,WE,TH,FR,SA,SU;BYHOUR=" + enumeratedInts(24) +
		";BYMINUTE=" + enumeratedInts(60) + ";BYSECOND=" + enumeratedInts(60) + ";BYSETPOS=-1"
	periods, err := RecurringBusyPeriods(recurringTestComponent(dense), dtstart, time.Second,
		dtstart, dtstart.AddDate(1, 0, 0), MaxRecurrenceInstances, nil)
	if !errors.Is(err, ErrRecurrenceScanLimit) {
		t.Fatalf("err = %v, want ErrRecurrenceScanLimit", err)
	}
	if len(periods) != 1 || !periods[0].Start.Equal(dtstart) {
		t.Fatalf("periods = %v, want the DTSTART collected before the limit", periods)
	}
}

// A VTIMEZONE observance's DTSTART is its first onset (RFC 5545 §3.6.5), and
// it counts toward COUNT like any DTSTART (§3.8.5.3) even when the rule would
// not generate it. Exchange writes observances that way, starting on 1 January.
func TestLatestRecurrenceOnOrBeforeCountsAnUnsynchronizedObservanceStart(t *testing.T) {
	dtstart := time.Date(2007, 1, 1, 2, 0, 0, 0, time.UTC)
	const rule = "FREQ=YEARLY;BYMONTH=3;BYDAY=2SU;COUNT=2"
	tests := map[string]struct {
		before time.Time
		want   time.Time
	}{
		"before the first generated onset": {before: time.Date(2007, 2, 1, 0, 0, 0, 0, time.UTC), want: dtstart},
		"after the last counted onset":     {before: time.Date(2008, 6, 1, 0, 0, 0, 0, time.UTC), want: time.Date(2007, 3, 11, 2, 0, 0, 0, time.UTC)},
	}
	for name, tt := range tests {
		t.Run(name, func(t *testing.T) {
			latest, found, complete := LatestRecurrenceOnOrBefore(dtstart, tt.before, rule)
			if !found || !complete || !latest.Equal(tt.want) {
				t.Fatalf("LatestRecurrenceOnOrBefore() = %v, %t, %t, want %v, true, true", latest, found, complete, tt.want)
			}
		})
	}
}

// RFC 5545 §3.8.5.3: "The DTSTART property value always counts as the first
// occurrence", including toward COUNT, whether or not the rule would generate
// it. Expansion and the CALDAV:max-instances check have to agree on that set.
func TestRecurrenceSetIncludesAnUnsynchronizedDTStart(t *testing.T) {
	day := func(month time.Month, d int) time.Time { return time.Date(2024, month, d, 9, 0, 0, 0, time.UTC) }
	tests := map[string]struct {
		rrule string
		want  []time.Time
	}{
		"COUNT counts the DTSTART": {
			rrule: "FREQ=MONTHLY;BYMONTHDAY=15;COUNT=3",
			want:  []time.Time{day(1, 1), day(1, 15), day(2, 15)},
		},
		"unbounded rule": {
			rrule: "FREQ=MONTHLY;BYMONTHDAY=15",
			want:  []time.Time{day(1, 1), day(1, 15), day(2, 15), day(3, 15)},
		},
		"BYSETPOS selecting another day": {
			rrule: "FREQ=MONTHLY;BYDAY=MO,TU,WE,TH,FR;BYSETPOS=-1;COUNT=2",
			want:  []time.Time{day(1, 1), day(1, 31)},
		},
		"BYSETPOS selecting nothing": {
			rrule: "FREQ=MONTHLY;BYDAY=MO,TU,WE,TH,FR;BYSETPOS=25;COUNT=999",
			want:  []time.Time{day(1, 1)},
		},
		"synchronized DTSTART is not doubled": {
			rrule: "FREQ=MONTHLY;BYMONTHDAY=1;COUNT=2",
			want:  []time.Time{day(1, 1), day(2, 1)},
		},
	}
	for name, tt := range tests {
		t.Run(name, func(t *testing.T) {
			raw := "BEGIN:VCALENDAR\r\nBEGIN:VEVENT\r\nUID:unsynchronized\r\n" +
				"DTSTART:20240101T090000Z\r\nRRULE:" + tt.rrule + "\r\nEND:VEVENT\r\nEND:VCALENDAR\r\n"
			periods, err := RecurringBusyPeriods(raw, day(1, 1), time.Hour, day(1, 1), day(3, 20), MaxRecurrenceInstances, nil)
			if err != nil {
				t.Fatalf("RecurringBusyPeriods() err = %v", err)
			}
			got := make([]time.Time, 0, len(periods))
			for _, period := range periods {
				got = append(got, period.Start)
			}
			if len(got) != len(tt.want) {
				t.Fatalf("instances = %v, want %v", got, tt.want)
			}
			for i := range got {
				if !got[i].Equal(tt.want[i]) {
					t.Fatalf("instances = %v, want %v", got, tt.want)
				}
			}
		})
	}
}

// A COUNT rule whose DTSTART it would not generate holds COUNT instances, the
// DTSTART among them, so it is inside the limit at exactly the limit.
func TestRecurrenceSetExceedsLimitCountsAnUnsynchronizedDTStartOnce(t *testing.T) {
	raw := "BEGIN:VCALENDAR\r\nBEGIN:VEVENT\r\nUID:unsynchronized\r\n" +
		"DTSTART:20240101T090000Z\r\nRRULE:FREQ=DAILY;BYHOUR=10;COUNT=" + strconv.Itoa(MaxRecurrenceInstances) +
		"\r\nEND:VEVENT\r\nEND:VCALENDAR\r\n"
	exceeds, valid := RecurrenceSetExceedsLimit(raw, MaxRecurrenceInstances)
	if !valid || exceeds {
		t.Fatalf("RecurrenceSetExceedsLimit() = %t, %t, want false, true", exceeds, valid)
	}
	dtstart := time.Date(2024, 1, 1, 9, 0, 0, 0, time.UTC)
	periods, err := RecurringBusyPeriods(raw, dtstart, time.Hour, dtstart, dtstart.AddDate(5, 0, 0), MaxRecurrenceInstances, nil)
	if err != nil {
		t.Fatalf("RecurringBusyPeriods() err = %v", err)
	}
	if len(periods) != MaxRecurrenceInstances {
		t.Fatalf("periods = %d, want %d", len(periods), MaxRecurrenceInstances)
	}
}

// The exclusions themselves still have to be applied, and an EXDATE names the
// slot of the pattern rather than the instant an override moved it to.
func TestRecurringBusyPeriodsAppliesExDates(t *testing.T) {
	raw := "BEGIN:VCALENDAR\r\nBEGIN:VEVENT\r\nUID:excluded\r\n" +
		"DTSTART:20240101T090000Z\r\nRRULE:FREQ=DAILY;COUNT=5\r\n" +
		"EXDATE:20240102T090000Z,20240104T090000Z\r\nEND:VEVENT\r\nEND:VCALENDAR\r\n"
	dtstart := time.Date(2024, 1, 1, 9, 0, 0, 0, time.UTC)
	periods, err := RecurringBusyPeriods(raw, dtstart, time.Hour, dtstart, dtstart.AddDate(0, 1, 0), MaxRecurrenceInstances, nil)
	if err != nil {
		t.Fatalf("RecurringBusyPeriods() err = %v", err)
	}
	var got []string
	for _, period := range periods {
		got = append(got, period.Start.Format("2006-01-02"))
	}
	want := "2024-01-01,2024-01-03,2024-01-05"
	if strings.Join(got, ",") != want {
		t.Errorf("starts = %v, want %v", got, want)
	}
}

// The analytic bound the BYSETPOS guard rests on must not be mistaken for a
// filter in its own right: a rule whose days are rare still generates every one
// of them. The DTSTART, which neither rule selects, leads each set.
func TestSparseCalendarRulesStillGenerateTheirRareDays(t *testing.T) {
	dtstart := time.Date(2000, 1, 1, 9, 0, 0, 0, time.UTC)
	rangeEnd := time.Date(2030, 1, 1, 0, 0, 0, 0, time.UTC)
	tests := map[string]struct {
		rrule string
		want  []string
	}{
		// Only a leap year has a 366th day.
		"BYYEARDAY=366": {
			rrule: "FREQ=YEARLY;BYYEARDAY=366",
			want: []string{
				"2000-01-01", "2000-12-31", "2004-12-31", "2008-12-31", "2012-12-31",
				"2016-12-31", "2020-12-31", "2024-12-31", "2028-12-31",
			},
		},
		// Only an ISO long year has a 53rd week.
		"BYWEEKNO=53": {
			rrule: "FREQ=YEARLY;BYWEEKNO=53;BYDAY=MO",
			want:  []string{"2000-01-01", "2004-12-27", "2009-12-28", "2015-12-28", "2020-12-28", "2026-12-28"},
		},
	}
	for name, tt := range tests {
		t.Run(name, func(t *testing.T) {
			periods, err := RecurringBusyPeriods(recurringTestComponent(tt.rrule), dtstart, time.Hour,
				dtstart, rangeEnd, MaxRecurrenceInstances, nil)
			if err != nil {
				t.Fatalf("RecurringBusyPeriods() err = %v", err)
			}
			var got []string
			for _, period := range periods {
				got = append(got, period.Start.Format("2006-01-02"))
			}
			if strings.Join(got, ",") != strings.Join(tt.want, ",") {
				t.Errorf("starts = %v, want %v", got, tt.want)
			}
		})
	}
}

// recurrenceMaxCandidatesPerPeriod is only safe as an upper bound. Under-count
// one rule shape and the BYSETPOS guard starts refusing positions the rule does
// select, which silently empties a legitimate recurrence set -- so it is
// checked against what the generator actually offers, over every frequency and
// every BY part combination the grammar admits together.
func TestRecurrenceCandidateBoundIsNeverBelowWhatAPeriodOffers(t *testing.T) {
	rules := []string{
		"FREQ=SECONDLY", "FREQ=SECONDLY;BYSECOND=0,30", "FREQ=SECONDLY;BYHOUR=9;BYMINUTE=0",
		"FREQ=MINUTELY", "FREQ=MINUTELY;BYSECOND=0,15,30,45,60", "FREQ=MINUTELY;BYDAY=SA,SU",
		"FREQ=HOURLY", "FREQ=HOURLY;BYMINUTE=0,30;BYSECOND=0,1,2", "FREQ=HOURLY;BYMONTH=2",
		"FREQ=DAILY", "FREQ=DAILY;BYHOUR=9,17;BYMINUTE=0,30", "FREQ=DAILY;BYDAY=MO,TU,WE,TH,FR",
		"FREQ=DAILY;BYMONTHDAY=1,-1", "FREQ=DAILY;INTERVAL=3;BYSECOND=0,20,40",
		"FREQ=WEEKLY", "FREQ=WEEKLY;BYDAY=MO,WE,FR", "FREQ=WEEKLY;BYDAY=SU,MO,TU,WE,TH,FR,SA",
		"FREQ=WEEKLY;BYDAY=MO,MO,TU;WKST=SU", "FREQ=WEEKLY;BYDAY=TU;BYHOUR=8,12,16",
		"FREQ=MONTHLY", "FREQ=MONTHLY;BYMONTHDAY=1,15,-1", "FREQ=MONTHLY;BYDAY=MO,TU,WE,TH,FR",
		"FREQ=MONTHLY;BYDAY=1MO,-1FR", "FREQ=MONTHLY;BYDAY=SA,SU;BYHOUR=9,10;BYMINUTE=0,30",
		"FREQ=MONTHLY;BYMONTHDAY=1,2,3,4,5,6,7;BYDAY=MO", "FREQ=MONTHLY;BYMONTH=1,7;BYMONTHDAY=29,30,31",
		"FREQ=YEARLY", "FREQ=YEARLY;BYMONTH=3;BYMONTHDAY=15", "FREQ=YEARLY;BYDAY=SA,SU",
		"FREQ=YEARLY;BYDAY=MO,TU,WE,TH,FR,SA,SU", "FREQ=YEARLY;BYDAY=1MO,-1FR",
		"FREQ=YEARLY;BYWEEKNO=1,53", "FREQ=YEARLY;BYWEEKNO=1,20,53;BYDAY=MO,TH",
		"FREQ=YEARLY;BYYEARDAY=1,100,366,-1", "FREQ=YEARLY;BYMONTH=1,2,3;BYDAY=MO,TU",
		"FREQ=YEARLY;BYMONTHDAY=13;BYDAY=FR", "FREQ=YEARLY;BYMONTH=2;BYMONTHDAY=29",
		"FREQ=YEARLY;BYHOUR=0,6,12,18;BYMINUTE=0,30;BYSECOND=0,30",
	}
	// Leap and non-leap years, a 53-week ISO year, and a February whose length
	// changes what a month-day list resolves to.
	starts := []time.Time{
		time.Date(2024, 2, 29, 9, 30, 15, 0, time.UTC),
		time.Date(2023, 1, 1, 0, 0, 0, 0, time.UTC),
		time.Date(2020, 12, 31, 23, 59, 59, 0, time.UTC),
		time.Date(2026, 7, 15, 12, 0, 0, 0, time.UTC),
	}
	for _, spelling := range rules {
		for _, dtstart := range starts {
			rule, ok := parseRecurrenceRule(spelling, dtstart.Location(), nil)
			if !ok {
				t.Fatalf("%s is not a rule this package parses", spelling)
			}
			bound := recurrenceMaxCandidatesPerPeriod(dtstart, rule)
			periodStart := recurrencePeriodStart(dtstart, rule)
			// Several consecutive periods, so a month or year whose shape
			// differs from the first one is covered too.
			for period := 0; period < 40; period++ {
				offered := 0
				_, overBudget := visitRecurrenceCandidates(periodStart, dtstart, rule, func(time.Time) bool {
					offered++
					return false
				})
				if overBudget {
					t.Fatalf("%s from %v: period %v could not be generated", spelling, dtstart, periodStart)
				}
				if offered > bound {
					t.Fatalf("%s from %v: period %v offered %d candidates, bound says at most %d",
						spelling, dtstart, periodStart, offered, bound)
				}
				next := advanceRecurrencePeriod(periodStart, rule)
				if !next.After(periodStart) {
					break
				}
				periodStart = next
			}
		}
	}
}

// A RANGE=THISANDFUTURE override may move its instances further than a
// time.Duration can measure: RFC 5545 §3.3.4 admits any four-digit year, and
// nothing bounds how far an override moves its slot. The governed instances
// have to land the full distance away, and the scan has to reach back far
// enough to find the slots they came from.
func TestThisAndFutureOverrideMovesInstancesCenturiesAway(t *testing.T) {
	raw := "BEGIN:VCALENDAR\r\nBEGIN:VEVENT\r\nUID:moved\r\n" +
		"DTSTART:17000601T130000Z\r\nDTEND:17000601T140000Z\r\nRRULE:FREQ=YEARLY;COUNT=5\r\nEND:VEVENT\r\n" +
		"BEGIN:VEVENT\r\nUID:moved\r\nRECURRENCE-ID;RANGE=THISANDFUTURE:17020601T130000Z\r\n" +
		"DTSTART:20220601T130000Z\r\nDTEND:20220601T140000Z\r\nEND:VEVENT\r\nEND:VCALENDAR\r\n"
	dtstart := time.Date(1700, 6, 1, 13, 0, 0, 0, time.UTC)
	rangeStart := time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC)
	periods, err := RecurringBusyPeriods(raw, dtstart, time.Hour, rangeStart, rangeStart.AddDate(10, 0, 0), MaxRecurrenceInstances, nil)
	if err != nil {
		t.Fatalf("RecurringBusyPeriods() err = %v", err)
	}
	var got []string
	for _, period := range periods {
		got = append(got, period.Start.Format("2006-01-02")+"<-"+period.RecurrenceID.Format("2006"))
	}
	want := "2022-06-01<-1702,2023-06-01<-1703,2024-06-01<-1704"
	if strings.Join(got, ",") != want {
		t.Fatalf("moved instances = %v, want %s", got, want)
	}
}

func TestFastForwardCountedSubDailyRecurrenceSpansCenturies(t *testing.T) {
	rule, ok := parseRecurrenceRule("FREQ=HOURLY;COUNT=100000000", time.UTC, nil)
	if !ok {
		t.Fatal("rule did not parse")
	}
	periodStart := time.Date(1601, 1, 1, 0, 0, 0, 0, time.UTC)
	threshold := time.Date(2026, 9, 1, 12, 30, 0, 0, time.UTC)

	got, skipped, ok := fastForwardCountedSubDailyRecurrence(periodStart, threshold, rule)
	if !ok {
		t.Fatal("fast-forward refused a fixed-step hourly rule")
	}
	want := time.Date(2026, 9, 1, 12, 0, 0, 0, time.UTC)
	if !got.Equal(want) {
		t.Fatalf("fast-forward landed on %s, want %s", got, want)
	}
	if wantSkipped := int((want.Unix() - periodStart.Unix()) / 3600); skipped != wantSkipped {
		t.Fatalf("skipped %d instances, want %d", skipped, wantSkipped)
	}
}
