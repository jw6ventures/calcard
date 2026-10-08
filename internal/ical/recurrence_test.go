package ical

import (
	"errors"
	"fmt"
	"maps"
	"math"
	"runtime"
	"slices"
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
			got := instanceStarts(requireRecurrenceResult[RecurrenceInstance](t)(collectRecurrenceInstances(raw, component, dtstart, 0,
				time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC),
				time.Date(2025, 2, 1, 0, 0, 0, 0, time.UTC), 1000, nil)))

			want := []time.Time{
				time.Date(2025, 1, 6, 10, 0, 0, 0, time.UTC),
				time.Date(2025, 1, 13, 10, 0, 0, 0, time.UTC),
				time.Date(2025, 1, 20, 10, 0, 0, 0, time.UTC),
			}
			if len(got) != len(want) {
				t.Fatalf("collectRecurrenceInstances() = %v, want %v", got, want)
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

	got := instanceStarts(requireRecurrenceResult[RecurrenceInstance](t)(collectRecurrenceInstances(raw, "VEVENT", dtstart, 0, boundary, boundary, 1000, nil)))
	if len(got) != 1 || !got[0].Equal(boundary) {
		t.Fatalf("collectRecurrenceInstances() = %v, want exactly %v", got, boundary)
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

	got := instanceStarts(requireRecurrenceResult[RecurrenceInstance](t)(collectRecurrenceInstances(raw, "VEVENT", dtstart, time.Hour,
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
		t.Fatalf("collectRecurrenceInstances() = %v, want exactly %v", got, dtstart)
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

	got := instanceStarts(requireRecurrenceResult[RecurrenceInstance](t)(collectRecurrenceInstances(raw, "VEVENT", dtstart, 0, scanStart, scanEnd, 1000, behind)))
	want := []time.Time{
		time.Date(2025, 1, 6, 13, 0, 0, 0, time.UTC),
		time.Date(2025, 1, 20, 13, 0, 0, 0, time.UTC),
	}
	if len(got) != len(want) {
		t.Fatalf("collectRecurrenceInstances() = %v, want %v", got, want)
	}
	for i := range want {
		if !got[i].Equal(want[i]) {
			t.Fatalf("instance %d = %v, want %v", i, got[i], want[i])
		}
	}

	// A nil resolver keeps the ParsePropertyDateTimeLocal reading, which is what
	// every caller holding no zone of its own relies on.
	plain, _ := ParsePropertyDateTimeLocal("DTSTART", "20250106T100000")
	bare := instanceStarts(requireRecurrenceResult[RecurrenceInstance](t)(collectRecurrenceInstances(raw, "VEVENT", plain, 0, scanStart, scanEnd, 1000, nil)))
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

			got := instanceStarts(requireRecurrenceResult[RecurrenceInstance](t)(collectRecurrenceInstances(raw, "VEVENT", dtstart, time.Hour,
				test.wantStarts[0].Add(-time.Hour), test.wantStarts[len(test.wantStarts)-1].Add(2*time.Hour),
				1000, resolve)))
			if len(got) != len(test.wantStarts) {
				t.Fatalf("collectRecurrenceInstances() = %v, want %v", got, test.wantStarts)
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

// VisitRecurrenceInstances keeps the slot each occurrence belongs to alongside
// where it actually falls. The two diverge under a RANGE=THISANDFUTURE override,
// and a caller writing a RECURRENCE-ID out needs the slot: RFC 5545 §3.8.4.4
// makes the property identify the instance within the master's pattern, not the
// time the override moved it to.
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

	instances := requireRecurrenceResult[RecurrenceInstance](t)(collectRecurrenceInstances(raw, "VEVENT", dtstart, time.Hour, rangeStart, rangeEnd, 1000, nil))
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

	instances := requireRecurrenceResult[RecurrenceInstance](t)(collectRecurrenceInstances(raw, "VEVENT", dtstart, time.Hour, rangeStart, rangeEnd, 1000, nil))
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

	instances := requireRecurrenceResult[RecurrenceInstance](t)(collectRecurrenceInstances(raw, "VEVENT", dtstart, time.Hour, rangeStart, rangeEnd, 1000, nil))
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
			instances := requireRecurrenceResult[RecurrenceInstance](t)(collectRecurrenceInstances(recurringTestComponent(tt.rrule), "VEVENT", dtstart, time.Hour,
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
	rule, ok := parseRecurrenceRule("FREQ=WEEKLY;BYDAY=SA,SU;BYHOUR=0,23", loc, nil, nil)
	if !ok {
		t.Fatal("parseRecurrenceRule() ok = false")
	}
	dtstart := time.Date(2024, 9, 2, 0, 30, 0, 0, loc)
	periodStart := recurrencePeriodStart(dtstart, rule)

	var got []time.Time
	visitRecurrenceCandidates(periodStart, dtstart, rule, newRecurrenceWork(), func(current time.Time) bool {
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
			rule, ok := parseRecurrenceRule("FREQ=WEEKLY;BYDAY=SA,SU;BYHOUR=0,23;BYSETPOS="+tt.bysetpos, loc, nil, nil)
			if !ok {
				t.Fatal("parseRecurrenceRule() ok = false")
			}
			periodStart := recurrencePeriodStart(dtstart, rule)

			var got []time.Time
			visitRecurrenceCandidates(periodStart, dtstart, rule, newRecurrenceWork(), func(current time.Time) bool {
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
	exceeds, handled := finishSparseRecurrence(dtstart, dtstart, rule, newRecurrenceWork(), &occurrences, func(time.Time) bool {
		return false
	})
	if !handled || !exceeds {
		t.Fatalf("finishSparseRecurrence() = (exceeds=%t, handled=%t), want (true, true) when a day's clock combinations exceed recurrencePeriodWorkLimit", exceeds, handled)
	}
}

// collectRecurrenceInstances gathers every instance VisitRecurrenceInstances
// hands out, returning them with its error the way a caller owing the whole set
// reads them: a budget error keeps what was collected, any other error none.
func collectRecurrenceInstances(raw, componentName string, dtstart time.Time, duration time.Duration, rangeStart, rangeEnd time.Time, maxInstances int, resolve RecurrenceTimeResolver) ([]RecurrenceInstance, error) {
	var instances []RecurrenceInstance
	err := VisitRecurrenceInstances(raw, componentName, dtstart, duration, rangeStart, rangeEnd, maxInstances, resolve, func(instance RecurrenceInstance) bool {
		instances = append(instances, instance)
		return false
	})
	if err != nil && !errors.Is(err, ErrRecurrenceExpansionLimit) {
		return nil, err
	}
	return instances, err
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
			instances, err := collectRecurrenceInstances(raw, "VEVENT", start, time.Hour, start.AddDate(0, 0, 4), start.AddDate(0, 0, 5), 10, nil)
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

// finishesWithin runs evaluate, which returns what it found wrong or "" for
// nothing, and fails the test with that, or if evaluate takes longer than
// budget, or does not return at all within the hard limit. evaluate runs on its
// own goroutine, which may outlive the test, so it reports rather than calling
// t. The budget is not asserted under the race detector, whose per-access cost
// would decide it.
func finishesWithin(t *testing.T, budget time.Duration, evaluate func() string) {
	t.Helper()
	const hardLimit = 10 * time.Second
	type outcome struct {
		problem string
		elapsed time.Duration
	}
	done := make(chan outcome, 1)
	go func() {
		started := time.Now()
		problem := evaluate()
		done <- outcome{problem: problem, elapsed: time.Since(started)}
	}()
	select {
	case got := <-done:
		if got.problem != "" {
			t.Error(got.problem)
		}
		if got.elapsed > budget && !raceDetectorEnabled {
			t.Errorf("took %v, want at most %v", got.elapsed, budget)
		}
	case <-time.After(hardLimit):
		t.Fatalf("did not return within %v", hardLimit)
	}
}

// A rule whose BY parts can never be satisfied together generates nothing past
// its DTSTART. The twenty-sixth weekday of a year falls between the 24th of June
// and the 1st of July, so no BYMONTHDAY from 2 to 23 can ever hold one. Every
// period pattern of the Gregorian calendar repeats after 400 years, so that is
// known once one cycle of periods has been generated: scanning further is work
// no answer needs, and on this 316-byte rule it came to ten minutes of CPU per
// evaluation, with a duplicated BYDAY list multiplying it. Each entry point a
// stored rule reaches -- the PUT gate, free-busy, an open-ended calendar-query
// and a VTIMEZONE observance -- has to answer it exactly and promptly.
//
// A daily or sub-daily rule's cycle is more periods than the period scan
// covers, so the same answer has to come from the rule itself: a date its
// filters can never admit, or a clock its interval can never reach, is known
// without generating a single period.
func TestNeverMatchingRuleIsAnsweredWithinOneCalendarCycle(t *testing.T) {
	yearOrdinals := "26MO,26TU,26WE,26TH,26FR,26SA,26SU"
	yearly := ";BYMONTHDAY=" + rangeInts(2, 23) + ";BYDAY=" + yearOrdinals
	// The second weekday of a month falls between its 8th and 14th days.
	monthly := ";BYMONTHDAY=" + rangeInts(1, 7) + ";BYDAY=2MO,2TU,2WE,2TH,2FR,2SA,2SU"
	// February has no 30th.
	daily := ";BYMONTH=2;BYMONTHDAY=30"
	rules := map[string]string{
		"daily with COUNT":      "FREQ=DAILY;COUNT=2" + daily,
		"daily without an end":  "FREQ=DAILY" + daily,
		"hourly with COUNT":     "FREQ=HOURLY;COUNT=2" + daily,
		"hourly without an end": "FREQ=HOURLY" + daily,
		// Every date is admitted, but five-second steps from :00 never reach :22.
		"secondly off its interval": "FREQ=SECONDLY;INTERVAL=5;BYSECOND=22;COUNT=5",
		"yearly with COUNT":         "FREQ=YEARLY;COUNT=2" + yearly,
		"yearly with UNTIL":         "FREQ=YEARLY;UNTIL=99991231T000000Z" + yearly,
		"yearly without an end":     "FREQ=YEARLY" + yearly,
		"yearly with BYDAY repeats": "FREQ=YEARLY;COUNT=2;BYMONTHDAY=" + rangeInts(2, 23) + ";BYDAY=" + strings.Repeat(yearOrdinals+",", 99) + yearOrdinals,
		"yearly every third year":   "FREQ=YEARLY;INTERVAL=3;COUNT=2" + yearly,
		"monthly with COUNT":        "FREQ=MONTHLY;COUNT=2" + monthly,
		"monthly without an end":    "FREQ=MONTHLY" + monthly,
	}
	const budget = 250 * time.Millisecond
	dtstart := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)

	for name, rule := range rules {
		t.Run(name, func(t *testing.T) {
			raw := recurringTestComponent(rule)

			t.Run("PUT validation", func(t *testing.T) {
				finishesWithin(t, budget, func() string {
					exceeds, valid := RecurrenceSetExceedsLimit(raw, MaxRecurrenceInstances)
					if !valid || exceeds {
						return fmt.Sprintf("RecurrenceSetExceedsLimit() = (exceeds=%t, valid=%t), want (false, true): the set is the DTSTART alone", exceeds, valid)
					}
					return ""
				})
			})
			t.Run("free-busy", func(t *testing.T) {
				finishesWithin(t, budget, func() string {
					periods, err := RecurringBusyPeriods(raw, dtstart, time.Hour, dtstart, dtstart.AddDate(100, 0, 0), MaxRecurrenceInstances, nil)
					if err != nil || len(periods) != 1 || !periods[0].Start.Equal(dtstart) {
						return fmt.Sprintf("RecurringBusyPeriods() = (%v, %v), want the DTSTART alone", periods, err)
					}
					return ""
				})
			})
			t.Run("open-ended calendar-query", func(t *testing.T) {
				finishesWithin(t, budget, func() string {
					instances, err := collectRecurrenceInstances(raw, "VEVENT", dtstart, time.Hour, dtstart, RecurrenceUntilSentinel, MaxRecurrenceInstances+2, nil)
					if err != nil || len(instances) != 1 || !instances[0].Start.Equal(dtstart) {
						return fmt.Sprintf("collectRecurrenceInstances() = (%v, %v), want the DTSTART alone", instances, err)
					}
					return ""
				})
			})
			t.Run("VTIMEZONE observance", func(t *testing.T) {
				finishesWithin(t, budget, func() string {
					latest, found, complete := LatestRecurrenceOnOrBefore(dtstart, dtstart.AddDate(500, 0, 0), rule)
					if !found || !complete || !latest.Equal(dtstart) {
						return fmt.Sprintf("LatestRecurrenceOnOrBefore() = (%v, found=%t, complete=%t), want the DTSTART, found and complete", latest, found, complete)
					}
					return ""
				})
			})
		})
	}
}

// The sparse scan pays for the days a rule's date filters admit, not for the
// calendar days between them, so a rule whose days are centuries apart is
// still followed exactly: validated against its COUNT, and expanded to the end
// of an open-ended range.
func TestRareDailyRuleIsFollowedToItsEnd(t *testing.T) {
	const budget = 250 * time.Millisecond
	dtstart := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)

	for _, rule := range []string{
		// Every other leap day: 1,461 days separate two of them, so
		// INTERVAL=2 reaches one leap day in two. A thousand span 8,000 years.
		"FREQ=DAILY;INTERVAL=2;COUNT=1000;BYMONTH=2;BYMONTHDAY=29",
		// A leap day falling on a Monday or a Tuesday: 28 in 400 years.
		"FREQ=DAILY;COUNT=1000;BYMONTH=2;BYMONTHDAY=29;BYDAY=MO,TU",
		"FREQ=HOURLY;COUNT=1000;BYMONTH=2;BYMONTHDAY=29;BYDAY=MO,TU;BYHOUR=9",
	} {
		t.Run("PUT validation/"+rule, func(t *testing.T) {
			finishesWithin(t, budget, func() string {
				exceeds, valid := RecurrenceSetExceedsLimit(recurringTestComponent(rule), MaxRecurrenceInstances)
				if !valid || exceeds {
					return fmt.Sprintf("RecurrenceSetExceedsLimit() = (exceeds=%t, valid=%t), want (false, true): COUNT holds the set to the limit", exceeds, valid)
				}
				return ""
			})
		})
	}

	t.Run("open-ended calendar-query", func(t *testing.T) {
		var want []string
		for year := dtstart.Year(); year <= RecurrenceUntilSentinel.Year(); year++ {
			if day := time.Date(year, time.February, 29, 0, 0, 0, 0, time.UTC); day.Month() == time.February && day.Weekday() == time.Monday {
				want = append(want, day.Format(time.DateOnly))
			}
		}
		raw := recurringTestComponent("FREQ=DAILY;BYMONTH=2;BYMONTHDAY=29;BYDAY=MO")
		finishesWithin(t, budget, func() string {
			instances, err := collectRecurrenceInstances(raw, "VEVENT", dtstart, time.Hour, dtstart.AddDate(0, 0, 1), RecurrenceUntilSentinel, MaxRecurrenceInstances+2, nil)
			if err != nil {
				return fmt.Sprintf("collectRecurrenceInstances() err = %v, want every leap-day Monday to the end of the range", err)
			}
			got := make([]string, 0, len(instances))
			for _, instance := range instances {
				got = append(got, instance.Start.Format(time.DateOnly))
			}
			if !slices.Equal(got, want) {
				return fmt.Sprintf("collectRecurrenceInstances() = %d starts from %v, want %d from %v", len(got), got[:min(3, len(got))], len(want), want[:3])
			}
			return ""
		})
	})
}

// The generation a rule costs is bounded however it spreads across periods. The
// 29th of February is always the ninth of its weekday in the year, so this rule
// selects it when it falls on a Monday -- fourteen times in 400 years -- while
// every one of its yearly periods generates 732 days. Its thousand instances
// span about 28,000 years, which is within the period limit and far beyond the
// work limit, so each entry point fails closed rather than spend seconds on it:
// validation answers max-instances, expansion ErrRecurrenceScanLimit, and the
// observance search an incomplete answer.
func TestRecurrenceWorkIsBoundedAcrossPeriods(t *testing.T) {
	var yearDays []string
	for day := 1; day <= 366; day++ {
		yearDays = append(yearDays, strconv.Itoa(day), strconv.Itoa(-day))
	}
	rule := "FREQ=YEARLY;COUNT=1000;BYYEARDAY=" + strings.Join(yearDays, ",") + ";BYMONTHDAY=29;BYDAY=9MO"
	raw := recurringTestComponent(rule)
	dtstart := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	farAhead := dtstart.AddDate(20_000, 0, 0)
	const budget = time.Second

	t.Run("PUT validation", func(t *testing.T) {
		finishesWithin(t, budget, func() string {
			exceeds, valid := RecurrenceSetExceedsLimit(raw, MaxRecurrenceInstances)
			if !valid || !exceeds {
				return fmt.Sprintf("RecurrenceSetExceedsLimit() = (exceeds=%t, valid=%t), want (true, true)", exceeds, valid)
			}
			return ""
		})
	})
	t.Run("expansion", func(t *testing.T) {
		finishesWithin(t, budget, func() string {
			_, err := RecurringBusyPeriods(raw, dtstart, time.Hour, farAhead, farAhead.AddDate(1, 0, 0), MaxRecurrenceInstances, nil)
			if !errors.Is(err, ErrRecurrenceScanLimit) {
				return fmt.Sprintf("RecurringBusyPeriods() err = %v, want ErrRecurrenceScanLimit", err)
			}
			return ""
		})
	})
	t.Run("VTIMEZONE observance", func(t *testing.T) {
		finishesWithin(t, budget, func() string {
			if _, _, complete := LatestRecurrenceOnOrBefore(dtstart, farAhead, rule); complete {
				return "LatestRecurrenceOnOrBefore() complete = true, want false"
			}
			return ""
		})
	})
}

// A BYDAY list costs whatever its distinct specifiers cost, however often the
// text repeats them.
func TestParseRecurrenceRuleDeduplicatesByDay(t *testing.T) {
	rule, ok := parseRecurrenceRule("FREQ=MONTHLY;BYDAY="+strings.Repeat("MO,2TU,-1FR,+2TU,", 100)+"MO", time.UTC, nil, nil)
	if !ok {
		t.Fatal("parseRecurrenceRule() ok = false")
	}
	want := []weekdaySpecifier{{Day: time.Monday}, {Ordinal: 2, Day: time.Tuesday}, {Ordinal: -1, Day: time.Friday}}
	if !slices.Equal(rule.ByDay, want) {
		t.Fatalf("ByDay = %+v, want %+v", rule.ByDay, want)
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
	resolve := PropertyTimeResolver(func(keyPart, value string) (time.Time, bool) {
		if zone, ok := PropertyParam(keyPart, "TZID"); ok && zone == "(UTC-05:00) Eastern" {
			parsed, err := ParseDateTimeInLocation(strings.TrimSpace(value), eastern)
			return parsed, err == nil
		}
		return ParsePropertyDateTimeLocal(keyPart, value)
	})
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
			rule, ok := parseRecurrenceRule(spelling, dtstart.Location(), nil, nil)
			if !ok {
				t.Fatalf("%s is not a rule this package parses", spelling)
			}
			bound := recurrenceMaxCandidatesPerPeriod(dtstart, rule)
			periodStart := recurrencePeriodStart(dtstart, rule)
			// Several consecutive periods, so a month or year whose shape
			// differs from the first one is covered too.
			for period := 0; period < 40; period++ {
				offered := 0
				_, overBudget := visitRecurrenceCandidates(periodStart, dtstart, rule, newRecurrenceWork(), func(time.Time) bool {
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
	rule, ok := parseRecurrenceRule("FREQ=HOURLY;COUNT=100000000", time.UTC, nil, nil)
	if !ok {
		t.Fatal("rule did not parse")
	}
	periodStart := time.Date(1601, 1, 1, 0, 0, 0, 0, time.UTC)
	threshold := time.Date(2026, 9, 1, 12, 30, 0, 0, time.UTC)

	got, skipped, ok := fastForwardCountedSubDailyRecurrence(periodStart, threshold, periodStart, rule, rule.Count, nil)
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

// A RecurrenceStartLister lists the set LatestRecurrenceOnOrBefore searches, so
// the latest listed start at or before any bound has to be the start
// LatestRecurrenceOnOrBefore answers for that bound -- whether the listing was
// made in one extension or grown a few years at a time, as a caller resolving
// ever later instants grows it. The rules cover the shapes a VTIMEZONE
// observance takes -- Exchange's 1601 DTSTART, a rule retired by UNTIL, a
// counted rule, a DTSTART the rule does not generate -- and the selection paths
// that end a set early.
func TestRecurrenceStartListerAgreesWithLatestRecurrenceOnOrBefore(t *testing.T) {
	tests := []struct {
		name    string
		dtstart time.Time
		rrule   string
	}{
		{"Exchange standard", time.Date(1601, 1, 1, 2, 0, 0, 0, time.UTC), "FREQ=YEARLY;INTERVAL=1;BYDAY=1SU;BYMONTH=11"},
		{"Exchange daylight", time.Date(1601, 1, 1, 2, 0, 0, 0, time.UTC), "FREQ=YEARLY;INTERVAL=1;BYDAY=2SU;BYMONTH=3"},
		{"last Sunday", time.Date(1996, 10, 27, 3, 0, 0, 0, time.UTC), "FREQ=YEARLY;BYMONTH=10;BYDAY=-1SU"},
		{"retired by UNTIL", time.Date(1987, 4, 5, 2, 0, 0, 0, time.UTC), "FREQ=YEARLY;BYMONTH=4;BYDAY=1SU;UNTIL=20060402T070000Z"},
		{"counted", time.Date(2007, 1, 1, 2, 0, 0, 0, time.UTC), "FREQ=YEARLY;BYMONTH=3;BYDAY=2SU;COUNT=2"},
		{"counted past the range", time.Date(1990, 1, 1, 2, 0, 0, 0, time.UTC), "FREQ=YEARLY;BYMONTH=3,10;BYDAY=-1SU;COUNT=90"},
		{"unsynchronized start", time.Date(2010, 1, 15, 2, 0, 0, 0, time.UTC), "FREQ=YEARLY;BYMONTH=3,10;BYDAY=-1SU"},
		{"every other year", time.Date(2000, 3, 26, 1, 0, 0, 0, time.UTC), "FREQ=YEARLY;INTERVAL=2;BYMONTH=3;BYDAY=-1SU"},
		{"monthly set position", time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC), "FREQ=MONTHLY;BYDAY=MO,TU,WE,TH,FR;BYSETPOS=-1"},
		{"unreachable set position", time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC), "FREQ=YEARLY;BYMONTH=2;BYMONTHDAY=1;BYSETPOS=5"},
		{"never matching", time.Date(1970, 1, 2, 0, 0, 0, 0, time.UTC), "FREQ=YEARLY;BYMONTH=2;BYMONTHDAY=30"},
		{"week number", time.Date(2020, 1, 6, 10, 0, 0, 0, time.UTC), "FREQ=YEARLY;BYWEEKNO=1;BYDAY=MO,TU"},
		{"counted week number", time.Date(2020, 1, 6, 10, 0, 0, 0, time.UTC), "FREQ=YEARLY;BYWEEKNO=1,-1;BYDAY=MO,SU;COUNT=40"},
		{"unparsable", time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC), "FREQ=FORTNIGHTLY"},
	}
	through := time.Date(2041, 1, 1, 0, 0, 0, 0, time.UTC)
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			whole := NewRecurrenceStartLister(tt.dtstart, tt.rrule)
			if !whole.ListThrough(through, 4096) {
				t.Fatal("ListThrough() = false, want the whole range listed")
			}
			if !slices.IsSortedFunc(whole.starts, time.Time.Compare) {
				t.Fatalf("listed %v, want ascending", whole.starts)
			}
			grown := NewRecurrenceStartLister(tt.dtstart, tt.rrule)
			for bound := tt.dtstart; bound.Before(through); bound = bound.Add(3*365*24*time.Hour + 17*time.Hour) {
				if !grown.ListThrough(bound, 4096) {
					t.Fatalf("ListThrough(%v) = false while growing, want true", bound)
				}
			}
			if !grown.ListThrough(through, 4096) {
				t.Fatal("ListThrough() = false at the end of growing, want true")
			}

			var bounds []time.Time
			for bound := tt.dtstart.Add(-time.Hour); bound.Before(through); bound = bound.Add(97 * 24 * time.Hour) {
				bounds = append(bounds, bound)
			}
			if whole.Covers(through.AddDate(500, 0, 0)) {
				// A listing of a set that has ended answers every later bound
				// as well, which is what lets a caller stop extending it.
				bounds = append(bounds, through, through.AddDate(1, 0, 0), through.AddDate(500, 0, 0))
			}
			for _, bound := range bounds {
				for _, before := range []time.Time{bound, bound.Add(-time.Nanosecond)} {
					wantLatest, wantFound, wantComplete := LatestRecurrenceOnOrBefore(tt.dtstart, before, tt.rrule)
					if !wantComplete {
						t.Fatalf("LatestRecurrenceOnOrBefore(%v) complete = false", before)
					}
					for name, lister := range map[string]*RecurrenceStartLister{"whole": whole, "grown": grown} {
						got, found, ok := lister.LatestOnOrBefore(before)
						if !ok || found != wantFound || (found && !got.Equal(wantLatest)) {
							t.Fatalf("%s listing at or before %v = %v (found=%t, ok=%t), LatestRecurrenceOnOrBefore = %v (found=%t)",
								name, before, got, found, ok, wantLatest, wantFound)
						}
					}
				}
			}
		})
	}
}

// Growing a listing continues from where the last extension stopped. Listing
// four centuries of Exchange's rule a year at a time costs about what listing
// it at once does, where starting again from 1601 every year would cost some
// four hundred times as much.
func TestRecurrenceStartListerResumesWhereItStopped(t *testing.T) {
	dtstart := time.Date(1601, 1, 1, 2, 0, 0, 0, time.UTC)
	const rrule = "FREQ=YEARLY;INTERVAL=1;BYDAY=1SU;BYMONTH=11"
	through := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)

	spent := func(list func(*RecurrenceStartLister)) int {
		lister := NewRecurrenceStartLister(dtstart, rrule)
		list(lister)
		if !lister.Covers(through) {
			t.Fatal("the listing does not reach the bound")
		}
		return recurrenceWorkLimit - lister.work.remaining
	}
	once := spent(func(l *RecurrenceStartLister) {
		l.ListThrough(through, 4096)
	})
	yearly := spent(func(l *RecurrenceStartLister) {
		for bound := dtstart; bound.Before(through); bound = bound.AddDate(1, 0, 0) {
			l.ListThrough(bound, 4096)
		}
		l.ListThrough(through, 4096)
	})
	if yearly > 3*once {
		t.Fatalf("growing a year at a time spent %d units, listing at once %d", yearly, once)
	}
}

// A set is ended, and answers every later bound, only when it is known to have
// no start past what was listed: a counted or UNTIL-bounded rule that ran out,
// or one that can never produce a start.
func TestRecurrenceStartListerReportsWhetherTheSetEnded(t *testing.T) {
	soon := time.Date(2030, 1, 1, 0, 0, 0, 0, time.UTC)
	tests := []struct {
		name    string
		rrule   string
		through time.Time
		ended   bool
	}{
		{"open-ended", "FREQ=YEARLY;BYMONTH=3;BYDAY=2SU", soon, false},
		{"retired by UNTIL", "FREQ=YEARLY;BYMONTH=3;BYDAY=2SU;UNTIL=20100101T000000Z", soon, true},
		{"counted out", "FREQ=YEARLY;BYMONTH=3;BYDAY=2SU;COUNT=3", soon, true},
		{"counted past through", "FREQ=YEARLY;BYMONTH=3;BYDAY=2SU;COUNT=300", soon, false},
		// One Gregorian cycle of yearly periods offering nothing shows that
		// none ever will.
		{"never matching", "FREQ=YEARLY;BYMONTH=2;BYMONTHDAY=30", time.Date(2500, 1, 1, 0, 0, 0, 0, time.UTC), true},
	}
	dtstart := time.Date(2007, 3, 11, 2, 0, 0, 0, time.UTC)
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			lister := NewRecurrenceStartLister(dtstart, tt.rrule)
			if !lister.ListThrough(tt.through, 4096) {
				t.Fatal("ListThrough() = false, want true")
			}
			if ended := lister.Covers(time.Date(9999, 1, 1, 0, 0, 0, 0, time.UTC)); ended != tt.ended {
				t.Fatalf("listing covers every later bound = %t, want %t", ended, tt.ended)
			}
		})
	}
}

// A listing draws every extension from one work budget, the one a single
// LatestRecurrenceOnOrBefore spends. Growing it in steps would otherwise carry
// it to a bound that walking a counted rule refuses as too costly, and whether
// a bound was answered would depend on which bounds had been asked about
// before it. The
// rule here costs about 9,700 units a year, as every minute of the first week
// of January is generated for BYSETPOS to pick one, so three centuries outrun
// the budget while any 25-year step of them fits.
func TestRecurrenceStartListerAnswersOnlyWhatOneWalkAnswers(t *testing.T) {
	dtstart := time.Date(1601, 1, 1, 0, 0, 0, 0, time.UTC)
	hours := make([]string, 0, 23)
	for hour := 1; hour < 24; hour++ {
		hours = append(hours, strconv.Itoa(hour))
	}
	minutes := make([]string, 0, 60)
	for minute := 0; minute < 60; minute++ {
		minutes = append(minutes, strconv.Itoa(minute))
	}
	rrule := "FREQ=YEARLY;COUNT=100000;BYMONTH=1;BYMONTHDAY=1,2,3,4,5,6,7;BYHOUR=" + strings.Join(hours, ",") +
		";BYMINUTE=" + strings.Join(minutes, ",") + ";BYSETPOS=-1"
	near := time.Date(1650, 6, 1, 0, 0, 0, 0, time.UTC)
	far := time.Date(1900, 6, 1, 0, 0, 0, 0, time.UTC)

	if _, _, complete := LatestRecurrenceOnOrBefore(dtstart, far, rrule); complete {
		t.Fatalf("walking %q to %v fit the work budget; the test needs a bound it refuses", rrule, far)
	}
	want, wantFound, wantComplete := LatestRecurrenceOnOrBefore(dtstart, near, rrule)
	if !wantComplete || !wantFound {
		t.Fatalf("walking %q to %v = %v, %t, %t; want a complete answer", rrule, near, want, wantFound, wantComplete)
	}

	lister := NewRecurrenceStartLister(dtstart, rrule)
	for bound := dtstart; bound.Before(far); bound = bound.AddDate(25, 0, 0) {
		lister.ListThrough(bound, 4096)
	}
	lister.ListThrough(far, 4096)
	if got, found, ok := lister.LatestOnOrBefore(near); !ok || !found || !got.Equal(want) {
		t.Fatalf("listing answered %v with %v, %t, %t; want %v as walking it does", near, got, found, ok, want)
	}
	if got, found, ok := lister.LatestOnOrBefore(far); ok {
		t.Fatalf("listing grown in steps answered %v with %v, %t; walking the rule refuses it", far, got, found)
	}
}

// RFC 5545 numbers a year's weeks from the one holding its first Thursday, so a
// yearly BYWEEKNO=1 period generates days up to three days before its January
// 1. A bound in those days has to see them, whether it is answered by walking
// the rule or from a listing grown to it, and a listing grown past them in
// steps that landed among them still holds them.
func TestRecurrenceStartsIncludeWeeksBegunInThePreviousYear(t *testing.T) {
	dtstart := time.Date(2020, 1, 6, 10, 0, 0, 0, time.UTC)
	at := func(year int, month time.Month, day int) time.Time {
		return time.Date(year, month, day, 10, 0, 0, 0, time.UTC)
	}
	// Week 1 of 2025 begins on Monday 2024-12-30, and week 1 of 2026 on Monday
	// 2025-12-29.
	want := []time.Time{
		dtstart,
		at(2021, 1, 4), at(2021, 1, 5),
		at(2022, 1, 3), at(2022, 1, 4),
		at(2023, 1, 2), at(2023, 1, 3),
		at(2024, 1, 1), at(2024, 1, 2),
		at(2024, 12, 30), at(2024, 12, 31),
		at(2025, 12, 29), at(2025, 12, 30),
	}
	bounds := []struct {
		before time.Time
		want   time.Time
	}{
		{time.Date(2024, 12, 30, 23, 0, 0, 0, time.UTC), at(2024, 12, 30)},
		{time.Date(2024, 12, 31, 12, 0, 0, 0, time.UTC), at(2024, 12, 31)},
		{time.Date(2025, 12, 29, 9, 0, 0, 0, time.UTC), at(2024, 12, 31)},
		{time.Date(2025, 12, 29, 11, 0, 0, 0, time.UTC), at(2025, 12, 29)},
	}
	end := time.Date(2026, 6, 1, 0, 0, 0, 0, time.UTC)

	for _, rrule := range []string{"FREQ=YEARLY;BYWEEKNO=1;BYDAY=MO,TU", "FREQ=YEARLY;BYWEEKNO=1;BYDAY=MO,TU;COUNT=20"} {
		t.Run(rrule, func(t *testing.T) {
			grown := NewRecurrenceStartLister(dtstart, rrule)
			for _, bound := range bounds {
				if latest, found, complete := LatestRecurrenceOnOrBefore(dtstart, bound.before, rrule); !found || !complete || !latest.Equal(bound.want) {
					t.Errorf("LatestRecurrenceOnOrBefore(%v) = %v, %t, %t, want %v", bound.before, latest, found, complete, bound.want)
				}
				if !grown.ListThrough(bound.before, 4096) {
					t.Fatalf("ListThrough(%v) = false, want true", bound.before)
				}
				if latest, found, ok := grown.LatestOnOrBefore(bound.before); !ok || !found || !latest.Equal(bound.want) {
					t.Errorf("listing grown to %v answered %v, %t, %t, want %v", bound.before, latest, found, ok, bound.want)
				}
			}
			if !grown.ListThrough(end, 4096) {
				t.Fatalf("ListThrough(%v) = false, want true", end)
			}
			if !slices.EqualFunc(grown.starts, want, time.Time.Equal) {
				t.Fatalf("listed %v, want %v", grown.starts, want)
			}
		})
	}
}

// A yearly BYWEEKNO period's weeks overhang its calendar year at both ends: week
// 1 can begin up to three days before January 1, and the last week can end up
// to three days after December 31. Expansion has to reach into the periods on
// either side of a range for the instances they put inside it.
func TestRecurrenceInstancesIncludeWeeksOverhangingTheYear(t *testing.T) {
	event := func(dtstart, rrule string) string {
		return "BEGIN:VCALENDAR\r\nBEGIN:VEVENT\r\nUID:weeks\r\nDTSTART:" + dtstart + "\r\nRRULE:" + rrule + "\r\nEND:VEVENT\r\nEND:VCALENDAR\r\n"
	}
	at := func(year int, month time.Month, day int) time.Time {
		return time.Date(year, month, day, 10, 0, 0, 0, time.UTC)
	}
	tests := []struct {
		name                 string
		dtstart              time.Time
		rrule                string
		rangeStart, rangeEnd time.Time
		want                 []time.Time
	}{
		{
			// Week 1 of 2025 begins on Monday 2024-12-30, in the period after
			// the one holding the range.
			name:       "week 1 begun before the range's year ends",
			dtstart:    at(2020, 1, 6),
			rrule:      "FREQ=YEARLY;BYWEEKNO=1;BYDAY=MO,TU",
			rangeStart: time.Date(2024, 12, 1, 0, 0, 0, 0, time.UTC),
			rangeEnd:   time.Date(2024, 12, 31, 12, 0, 0, 0, time.UTC),
			want:       []time.Time{at(2024, 12, 30), at(2024, 12, 31)},
		},
		{
			// Week 53 of 2020 ends on Sunday 2021-01-03, in the period before
			// the one holding the range.
			name:       "last week ending after the range's year begins",
			dtstart:    at(2019, 12, 28),
			rrule:      "FREQ=YEARLY;BYWEEKNO=-1;BYDAY=SA,SU",
			rangeStart: time.Date(2021, 1, 2, 0, 0, 0, 0, time.UTC),
			rangeEnd:   time.Date(2021, 1, 5, 0, 0, 0, 0, time.UTC),
			want:       []time.Time{at(2021, 1, 2), at(2021, 1, 3)},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			raw := event(tt.dtstart.Format("20060102T150405Z"), tt.rrule)
			got := instanceStarts(requireRecurrenceResult[RecurrenceInstance](t)(collectRecurrenceInstances(raw, "VEVENT", tt.dtstart, time.Hour, tt.rangeStart, tt.rangeEnd, MaxRecurrenceInstances, nil)))
			if !slices.EqualFunc(got, tt.want, time.Time.Equal) {
				t.Fatalf("collectRecurrenceInstances() = %v, want %v", got, tt.want)
			}
		})
	}
}

// PUT validation counts a set to UNTIL, or over the horizon an unbounded rule is
// measured across, and a yearly BYWEEKNO period that begins past either can
// still put instances before it.
func TestRecurrenceSetExceedsLimitCountsWeeksBegunInThePreviousYear(t *testing.T) {
	event := func(dtstart, rrule string) string {
		return "BEGIN:VCALENDAR\r\nBEGIN:VEVENT\r\nUID:weeks\r\nDTSTART:" + dtstart + "\r\nRRULE:" + rrule + "\r\nEND:VEVENT\r\nEND:VCALENDAR\r\n"
	}
	tests := []struct {
		name  string
		raw   string
		count int
	}{
		{
			// The DTSTART and two days a year through 2024, the last two of
			// them in week 1 of 2025.
			name:  "bounded by UNTIL",
			raw:   event("20200106T100000Z", "FREQ=YEARLY;BYWEEKNO=1;BYDAY=MO,TU;UNTIL=20241231T120000Z"),
			count: 11,
		},
		{
			// The year from 2023-12-30 holds the DTSTART, 2024-01-01,
			// 2024-01-02 and 2024-12-30, which week 1 of 2025 begins on.
			name:  "unbounded, over its horizon",
			raw:   event("20231230T100000Z", "FREQ=YEARLY;BYWEEKNO=1;BYDAY=MO,TU"),
			count: 4,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if exceeds, ok := RecurrenceSetExceedsLimit(tt.raw, tt.count-1); !ok || !exceeds {
				t.Errorf("RecurrenceSetExceedsLimit(limit %d) = %t, %t, want the %d instances to exceed it", tt.count-1, exceeds, ok, tt.count)
			}
			if exceeds, ok := RecurrenceSetExceedsLimit(tt.raw, tt.count); !ok || exceeds {
				t.Errorf("RecurrenceSetExceedsLimit(limit %d) = %t, %t, want the %d instances within it", tt.count, exceeds, ok, tt.count)
			}
		})
	}
}

// A set holding more starts than the caller will keep halts the listing at the
// last start it could keep. Everything before that is still listed exactly and
// answers its bounds; a bound past it is left to the caller, and asking again
// does not extend the listing, since it would only stop at the same place.
func TestRecurrenceStartListerHaltsAtItsLimit(t *testing.T) {
	dtstart := time.Date(1601, 1, 1, 2, 0, 0, 0, time.UTC)
	const rrule = "FREQ=YEARLY;BYDAY=1SU;BYMONTH=11"
	lister := NewRecurrenceStartLister(dtstart, rrule)
	if lister.ListThrough(time.Date(4050, 1, 1, 0, 0, 0, 0, time.UTC), 1024) {
		t.Fatal("ListThrough() = true for 2,450 yearly starts with a limit of 1024, want false")
	}
	if lister.Len() != 1024 {
		t.Fatalf("kept %d starts, want 1024", lister.Len())
	}
	for _, before := range []time.Time{
		time.Date(1700, 6, 1, 0, 0, 0, 0, time.UTC),
		time.Date(2026, 6, 1, 0, 0, 0, 0, time.UTC),
		lister.starts[1023],
	} {
		want, wantFound, _ := LatestRecurrenceOnOrBefore(dtstart, before, rrule)
		got, found, ok := lister.LatestOnOrBefore(before)
		if !ok || found != wantFound || !got.Equal(want) {
			t.Fatalf("LatestOnOrBefore(%v) = %v, %t, %t; want %v, %t, true", before, got, found, ok, want, wantFound)
		}
	}
	past := time.Date(2700, 1, 1, 0, 0, 0, 0, time.UTC)
	if _, _, ok := lister.LatestOnOrBefore(past); ok {
		t.Fatalf("LatestOnOrBefore(%v) answered past the halted listing", past)
	}
	if lister.ListThrough(past, 4096) || lister.Len() != 1024 {
		t.Fatalf("a halted listing was extended to %d starts", lister.Len())
	}
}

// RFC 4791 §9.9 makes one matching instance enough, so a visitor that has its
// answer ends the expansion there instead of paying for the rest of the range.
func TestVisitRecurrenceInstancesStopsWhenAsked(t *testing.T) {
	raw := "BEGIN:VCALENDAR\r\nBEGIN:VEVENT\r\nUID:daily\r\nDTSTART:20250101T100000Z\r\n" +
		"RRULE:FREQ=DAILY\r\nRDATE:20250101T200000Z\r\nEND:VEVENT\r\nEND:VCALENDAR\r\n"
	dtstart := time.Date(2025, 1, 1, 10, 0, 0, 0, time.UTC)
	rangeStart := time.Date(2025, 6, 1, 0, 0, 0, 0, time.UTC)
	rangeEnd := time.Date(2030, 1, 1, 0, 0, 0, 0, time.UTC)

	visited := 0
	err := VisitRecurrenceInstances(raw, "VEVENT", dtstart, time.Hour, rangeStart, rangeEnd, MaxRecurrenceInstances, nil, func(RecurrenceInstance) bool {
		visited++
		return visited == 3
	})
	if err != nil {
		t.Fatalf("VisitRecurrenceInstances() error = %v, want nil after stopping", err)
	}
	if visited != 3 {
		t.Fatalf("visited %d instances, want 3", visited)
	}

	// A visitor that never stops sees every instance the budget admits, and
	// then the budget error the range runs into.
	visited = 0
	err = VisitRecurrenceInstances(raw, "VEVENT", dtstart, time.Hour, rangeStart, rangeEnd, MaxRecurrenceInstances, nil, func(RecurrenceInstance) bool {
		visited++
		return false
	})
	if !errors.Is(err, ErrRecurrenceExpansionLimit) {
		t.Fatalf("VisitRecurrenceInstances() error = %v, want the expansion limit", err)
	}
	if visited != MaxRecurrenceInstances {
		t.Fatalf("visited %d instances, want %d", visited, MaxRecurrenceInstances)
	}
}

// The period holding the DTSTART begins before it, and a rule whose DTSTART is
// not its first onset of that period generates later onsets inside it. The
// backward search has to reach that period rather than stop at its start.
func TestLatestRecurrenceOnOrBeforeSearchesThePeriodHoldingTheDTStart(t *testing.T) {
	tests := []struct {
		name    string
		dtstart time.Time
		rrule   string
		before  time.Time
		want    time.Time
	}{
		{
			name:    "a later onset in the DTSTART's year",
			dtstart: time.Date(2010, 1, 15, 2, 0, 0, 0, time.UTC),
			rrule:   "FREQ=YEARLY;BYMONTH=3,10;BYDAY=-1SU",
			before:  time.Date(2011, 2, 7, 0, 0, 0, 0, time.UTC),
			want:    time.Date(2010, 10, 31, 2, 0, 0, 0, time.UTC),
		},
		{
			name:    "a DTSTART the rule generates",
			dtstart: time.Date(1996, 10, 27, 3, 0, 0, 0, time.UTC),
			rrule:   "FREQ=YEARLY;BYMONTH=10;BYDAY=-1SU",
			before:  time.Date(1997, 2, 1, 0, 0, 0, 0, time.UTC),
			want:    time.Date(1996, 10, 27, 3, 0, 0, 0, time.UTC),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			latest, found, complete := LatestRecurrenceOnOrBefore(tt.dtstart, tt.before, tt.rrule)
			if !found || !complete || !latest.Equal(tt.want) {
				t.Fatalf("LatestRecurrenceOnOrBefore() = %v, %t, %t, want %v, true, true", latest, found, complete, tt.want)
			}
		})
	}
}

// A counted rule is followed forward from its DTSTART, and the answer is the
// last start reached, so following it holds that one start rather than every
// start before the bound. A rule four times an hour for six years reaches
// about 210,000 of them.
func TestLatestRecurrenceOnOrBeforeOfACountedRuleHoldsOnlyItsLatestStart(t *testing.T) {
	dtstart := time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC)
	rrule := "FREQ=DAILY;COUNT=10000000;BYHOUR=" + enumeratedInts(24) + ";BYMINUTE=0,15,30,45"
	before := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)

	var start, end runtime.MemStats
	runtime.GC()
	runtime.ReadMemStats(&start)
	latest, found, complete := LatestRecurrenceOnOrBefore(dtstart, before, rrule)
	runtime.ReadMemStats(&end)

	if !found || !complete || !latest.Equal(before) {
		t.Fatalf("LatestRecurrenceOnOrBefore() = %v, %t, %t, want %v, true, true", latest, found, complete, before)
	}
	// Generating six years of these periods allocates about 6 MB on its own.
	// Keeping the 210,000 starts as well adds more than 20 MB: the starts and
	// the copies a growing slice leaves behind.
	const allocationBudget = 16 << 20
	if allocated := end.TotalAlloc - start.TotalAlloc; allocated > allocationBudget {
		t.Fatalf("allocated %d bytes, want at most %d: the starts before the latest are being kept", allocated, allocationBudget)
	}
}

// A bound can lie further past the DTSTART than the scan bound counts periods
// -- 274 years of days, 1,917 of weeks, 8,333 of months -- and the backward
// search has to start from the period holding the bound all the same. Starting
// short of it would offer an onset centuries old as the latest, and as a
// complete answer. Each expected start is found by stepping the rule from its
// DTSTART one period at a time.
func TestLatestRecurrenceOnOrBeforeReachesBoundsCenturiesPastTheDTStart(t *testing.T) {
	tests := []struct {
		name    string
		dtstart time.Time
		rrule   string
		before  time.Time
		step    func(time.Time) time.Time
		admits  func(time.Time) bool
	}{
		{
			name:    "daily, once a year",
			dtstart: time.Date(1601, 1, 1, 2, 0, 0, 0, time.UTC),
			rrule:   "FREQ=DAILY;BYMONTH=3;BYMONTHDAY=25",
			before:  time.Date(2026, 6, 1, 0, 0, 0, 0, time.UTC),
			step:    func(t time.Time) time.Time { return t.AddDate(0, 0, 1) },
			admits:  func(t time.Time) bool { return t.Month() == time.March && t.Day() == 25 },
		},
		{
			name:    "every third day",
			dtstart: time.Date(1601, 1, 1, 2, 0, 0, 0, time.UTC),
			rrule:   "FREQ=DAILY;INTERVAL=3",
			before:  time.Date(2026, 6, 1, 0, 0, 0, 0, time.UTC),
			step:    func(t time.Time) time.Time { return t.AddDate(0, 0, 3) },
		},
		{
			name:    "weekly",
			dtstart: time.Date(90, 1, 7, 2, 0, 0, 0, time.UTC),
			rrule:   "FREQ=WEEKLY",
			before:  time.Date(2026, 6, 1, 0, 0, 0, 0, time.UTC),
			step:    func(t time.Time) time.Time { return t.AddDate(0, 0, 7) },
		},
		{
			name:    "monthly",
			dtstart: time.Date(1, 1, 15, 2, 0, 0, 0, time.UTC),
			rrule:   "FREQ=MONTHLY",
			before:  time.Date(9500, 6, 1, 0, 0, 0, 0, time.UTC),
			step:    func(t time.Time) time.Time { return t.AddDate(0, 1, 0) },
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			want := tt.dtstart
			for start := tt.dtstart; !start.After(tt.before); start = tt.step(start) {
				if tt.admits == nil || tt.admits(start) {
					want = start
				}
			}
			latest, found, complete := LatestRecurrenceOnOrBefore(tt.dtstart, tt.before, tt.rrule)
			if !found || !complete || !latest.Equal(want) {
				t.Fatalf("LatestRecurrenceOnOrBefore() = %v, %t, %t, want %v, true, true", latest, found, complete, want)
			}
		})
	}
}

// A COUNT rule is followed forward from its DTSTART. When the work budget runs
// out partway through a period, LatestRecurrenceOnOrBefore answers with the
// latest start it reached, found and incomplete, as it documents, rather than
// with nothing.
func TestLatestRecurrenceOnOrBeforeAnswersWhatItReachedWhenWorkRunsOut(t *testing.T) {
	var hours, minutes []string
	for hour := range 24 {
		hours = append(hours, strconv.Itoa(hour))
	}
	for minute := range 60 {
		minutes = append(minutes, strconv.Itoa(minute))
	}
	// Every minute of every day of each month: a month costs more than a day's
	// candidates, so the budget runs out partway through one.
	rule := "FREQ=MONTHLY;COUNT=10000000;BYDAY=MO,TU,WE,TH,FR,SA,SU;BYHOUR=" + strings.Join(hours, ",") + ";BYMINUTE=" + strings.Join(minutes, ",")
	dtstart := time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC)
	before := time.Date(2030, 1, 1, 0, 0, 0, 0, time.UTC)

	whole := NewRecurrenceStartLister(dtstart, rule)
	if whole.ListThrough(before, math.MaxInt) {
		t.Fatal("the rule was followed to the bound; the case tests nothing")
	}
	lastWhole := whole.starts[len(whole.starts)-1]

	latest, found, complete := LatestRecurrenceOnOrBefore(dtstart, before, rule)
	if !found || complete {
		t.Fatalf("LatestRecurrenceOnOrBefore = %s, found %v, complete %v; want what it reached, incomplete", latest, found, complete)
	}
	if !latest.After(lastWhole) || latest.After(lastWhole.AddDate(0, 1, 0)) || latest.Second() != 0 {
		t.Errorf("latest = %s, want a start reached in the month after %s, the last the whole periods hold", latest, lastWhole)
	}
}

// A listing kept from a later instant holds only the starts from there, with
// the latest start before it, so a rule too dense to list from its DTSTART to
// the instants asked within a listing's limit is still answered from a
// listing. It answers every bound from there as LatestRecurrenceOnOrBefore
// does, and none before it.
func TestRecurrenceStartListerFromAnInstantAgreesWithLatestRecurrenceOnOrBefore(t *testing.T) {
	tests := []struct {
		name    string
		dtstart time.Time
		rrule   string
	}{
		{"Exchange monthly", time.Date(1601, 1, 1, 2, 0, 0, 0, time.UTC), "FREQ=MONTHLY;BYDAY=1SU"},
		{"Exchange standard", time.Date(1601, 1, 1, 2, 0, 0, 0, time.UTC), "FREQ=YEARLY;INTERVAL=1;BYDAY=1SU;BYMONTH=11"},
		{"weekly", time.Date(1601, 1, 1, 2, 0, 0, 0, time.UTC), "FREQ=WEEKLY;BYDAY=SU"},
		{"daily", time.Date(1950, 1, 1, 2, 0, 0, 0, time.UTC), "FREQ=DAILY"},
		{"counted", time.Date(1900, 1, 1, 2, 0, 0, 0, time.UTC), "FREQ=MONTHLY;BYDAY=-1SU;COUNT=1500"},
		{"retired by UNTIL", time.Date(1950, 1, 1, 2, 0, 0, 0, time.UTC), "FREQ=MONTHLY;BYMONTHDAY=15;UNTIL=20100101T000000Z"},
		{"unsynchronized start", time.Date(2010, 1, 15, 2, 0, 0, 0, time.UTC), "FREQ=YEARLY;BYMONTH=3,10;BYDAY=-1SU"},
		{"start before the instant only", time.Date(2010, 1, 15, 2, 0, 0, 0, time.UTC), "FREQ=YEARLY;BYMONTH=2;BYMONTHDAY=30"},
		// The period grid INTERVAL steps and the parts BY rules leave to
		// DTSTART are the DTSTART's, however far from it the listing starts.
		{"every other year by week number", time.Date(2019, 12, 30, 2, 0, 0, 0, time.UTC), "FREQ=YEARLY;INTERVAL=2;BYWEEKNO=1;BYDAY=MO"},
		{"every third month", time.Date(1601, 2, 1, 2, 0, 0, 0, time.UTC), "FREQ=MONTHLY;INTERVAL=3;BYDAY=1SU"},
		{"fortnightly from a Wednesday week", time.Date(1601, 1, 3, 2, 0, 0, 0, time.UTC), "FREQ=WEEKLY;INTERVAL=2;WKST=WE;BYDAY=SU,TU"},
		{"month day from DTSTART, skipping short months", time.Date(1601, 1, 31, 2, 0, 0, 0, time.UTC), "FREQ=MONTHLY"},
		{"29 February", time.Date(1604, 2, 29, 2, 0, 0, 0, time.UTC), "FREQ=YEARLY"},
		{"last weekday by set position", time.Date(1601, 1, 1, 2, 0, 0, 0, time.UTC), "FREQ=MONTHLY;BYDAY=MO,TU,WE,TH,FR;BYSETPOS=-1"},
		{"week number on DTSTART's weekday", time.Date(1601, 1, 4, 2, 0, 0, 0, time.UTC), "FREQ=YEARLY;INTERVAL=2;BYWEEKNO=10,40"},
		{"time of day from DTSTART", time.Date(1601, 1, 1, 1, 30, 15, 0, time.UTC), "FREQ=MONTHLY;BYMONTHDAY=-1,15"},
	}
	through := time.Date(2030, 6, 1, 0, 0, 0, 0, time.UTC)
	for _, tt := range tests {
		for _, from := range []time.Time{time.Date(2025, 6, 1, 0, 0, 0, 0, time.UTC), time.Date(2026, 3, 4, 5, 6, 7, 0, time.UTC)} {
			t.Run(tt.name+"/"+from.Format("2006-01-02"), func(t *testing.T) {
				lister := NewRecurrenceStartListerFrom(tt.dtstart, tt.rrule, from)
				if !lister.ListThrough(through, 2048) {
					t.Fatal("ListThrough() = false, want the window listed")
				}
				for _, start := range lister.starts {
					if start.Before(from) {
						t.Fatalf("kept %v, before %v", start, from)
					}
				}
				for before := from.Add(-48 * time.Hour); before.Before(through); before = before.Add(13*24*time.Hour + 5*time.Hour) {
					got, found, ok := lister.LatestOnOrBefore(before)
					if before.Before(from) {
						if ok {
							t.Fatalf("answered %v, before the listing starts", before)
						}
						continue
					}
					want, wantFound, wantComplete := LatestRecurrenceOnOrBefore(tt.dtstart, before, tt.rrule)
					if !wantComplete || !ok || found != wantFound || (found && !got.Equal(want)) {
						t.Fatalf("at or before %v = %v (found=%t, ok=%t), LatestRecurrenceOnOrBefore = %v (found=%t, complete=%t)",
							before, got, found, ok, want, wantFound, wantComplete)
					}
				}
			})
		}
	}
}

// RRuleParam reads one part of an RRULE as parseRecurrenceRule reads its
// parts: the name in any case, white space around the name, the "=" and the
// value ignored, and a part with no "=" naming nothing. The first part of a
// name answers, as every reader of the rule takes it.
func TestRRuleParam(t *testing.T) {
	tests := []struct {
		rrule, name, want string
		found             bool
	}{
		{"FREQ=DAILY;UNTIL=20260301T000000Z", "UNTIL", "20260301T000000Z", true},
		{"freq=daily;until=20260301", "UNTIL", "20260301", true},
		{"FREQ=DAILY ; Until = 20260301 ", "until", "20260301", true},
		{" COUNT =3;FREQ=DAILY", "COUNT", "3", true},
		{"FREQ=DAILY;COUNT", "COUNT", "", false},
		{"FREQ=DAILY;COUNT=3;COUNT=5", "COUNT", "3", true},
		{"FREQ=DAILY", "UNTIL", "", false},
		{"FREQ=DAILY;UNTIL=", "UNTIL", "", true},
		{"", "FREQ", "", false},
	}
	for _, tt := range tests {
		got, found := RRuleParam(tt.rrule, tt.name)
		if got != tt.want || found != tt.found {
			t.Errorf("RRuleParam(%q, %q) = %q, %v; want %q, %v", tt.rrule, tt.name, got, found, tt.want, tt.found)
		}
	}
	if !SupportedRecurrenceRule("freq = weekly;byday=MO") {
		t.Error("a lowercase FREQ spaced around its = is not supported")
	}
}

// RRuleParts reads every part of an RRULE as RRuleParam reads one, under its
// name in upper case, so each name it holds answers what RRuleParam answers
// for it.
func TestRRuleParts(t *testing.T) {
	tests := []struct {
		rrule string
		want  map[string]string
	}{
		{"FREQ=DAILY;UNTIL=20260301T000000Z", map[string]string{"FREQ": "DAILY", "UNTIL": "20260301T000000Z"}},
		{"freq=daily ; Until = 20260301 ", map[string]string{"FREQ": "daily", "UNTIL": "20260301"}},
		{"FREQ=DAILY;COUNT", map[string]string{"FREQ": "DAILY"}},
		{"FREQ=DAILY;COUNT=3;count=5", map[string]string{"FREQ": "DAILY", "COUNT": "3"}},
		{"FREQ=DAILY;UNTIL=", map[string]string{"FREQ": "DAILY", "UNTIL": ""}},
		{"", map[string]string{}},
	}
	for _, tt := range tests {
		got := RRuleParts(tt.rrule)
		if !maps.Equal(got, tt.want) {
			t.Errorf("RRuleParts(%q) = %v, want %v", tt.rrule, got, tt.want)
		}
		for name, value := range got {
			if param, found := RRuleParam(tt.rrule, name); !found || param != value {
				t.Errorf("RRuleParts(%q)[%s] = %q, RRuleParam = %q, %v", tt.rrule, name, value, param, found)
			}
		}
	}
}
