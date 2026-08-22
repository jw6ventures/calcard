package ical

import (
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

	periods := RecurringBusyPeriods(recurringEvent, dtstart, time.Hour, rangeStart, rangeEnd, 1000, nil)

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

	periods := RecurringBusyPeriods(raw, dtstart, time.Hour, rangeStart, rangeEnd, 1000, nil)

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
			got := instanceStarts(RecurrenceInstances(raw, component, dtstart, 0,
				time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC),
				time.Date(2025, 2, 1, 0, 0, 0, 0, time.UTC), 1000, nil))

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

	got := instanceStarts(RecurrenceInstances(raw, "VEVENT", dtstart, 0, boundary, boundary, 1000, nil))
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

	got := instanceStarts(RecurrenceInstances(raw, "VEVENT", dtstart, time.Hour,
		time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC),
		time.Date(2025, 2, 1, 0, 0, 0, 0, time.UTC), 1000, nil))

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

	got := instanceStarts(RecurrenceInstances(raw, "VEVENT", dtstart, 0, scanStart, scanEnd, 1000, behind))
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
	bare := instanceStarts(RecurrenceInstances(raw, "VEVENT", plain, 0, scanStart, scanEnd, 1000, nil))
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

			got := instanceStarts(RecurrenceInstances(raw, "VEVENT", dtstart, time.Hour,
				test.wantStarts[0].Add(-time.Hour), test.wantStarts[len(test.wantStarts)-1].Add(2*time.Hour),
				1000, resolve))
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

	instances := RecurrenceInstances(raw, "VEVENT", dtstart, time.Hour, rangeStart, rangeEnd, 1000, nil)
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
	slots := RecurrenceSlots(raw, "VEVENT", dtstart, time.Hour,
		time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC),
		time.Date(2025, 2, 1, 0, 0, 0, 0, time.UTC), 1000, nil)
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

	instances := RecurrenceInstances(raw, "VEVENT", dtstart, time.Hour, rangeStart, rangeEnd, 1000, nil)
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

	instances := RecurrenceInstances(raw, "VEVENT", dtstart, time.Hour, rangeStart, rangeEnd, 1000, nil)
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
