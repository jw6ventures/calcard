package ical

import (
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
	if !exceeds {
		t.Fatal("RecurrenceSetExceedsLimit() exceeds = false, want true")
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
// BYSETPOS selects. The allocation bound is the assertion -- the defect was not
// that this was slow but that a request could make the server hold gigabytes.
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

// Streaming the period replaced a sort over the whole of it, so the order and
// the selection have to be what that sort produced -- including for a rule
// whose days are generated out of order.
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
			rrule: "FREQ=MONTHLY;BYMONTHDAY=15,1;BYSETPOS=-1;COUNT=3",
			want:  []string{"20240115T000000Z", "20240215T000000Z", "20240315T000000Z"},
		},
		"both ends": {
			rrule: "FREQ=MONTHLY;BYMONTHDAY=15,1;BYSETPOS=1,-1;COUNT=4",
			want:  []string{"20240101T000000Z", "20240115T000000Z", "20240201T000000Z", "20240215T000000Z"},
		},
		"position past the end of the period": {
			rrule: "FREQ=MONTHLY;BYMONTHDAY=15,1;BYSETPOS=3;COUNT=2",
			want:  nil,
		},
		"last weekday of the month": {
			rrule: "FREQ=MONTHLY;BYDAY=MO,TU,WE,TH,FR;BYSETPOS=-1;COUNT=3",
			want:  []string{"20240131T000000Z", "20240229T000000Z", "20240329T000000Z"},
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
