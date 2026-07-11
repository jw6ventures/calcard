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

func TestUnfoldLines(t *testing.T) {
	raw := "DESCRIPTION:line one\r\n continues here\r\nSUMMARY:next"
	lines := UnfoldLines(raw)
	if len(lines) != 2 {
		t.Fatalf("UnfoldLines() = %d lines, want 2: %#v", len(lines), lines)
	}
	// Continuation whitespace is stripped entirely (existing behavior).
	if lines[0] != "DESCRIPTION:line onecontinues here" {
		t.Fatalf("UnfoldLines() first line = %q", lines[0])
	}
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

	periods := RecurringBusyPeriods(recurringEvent, dtstart, time.Hour, rangeStart, rangeEnd, 1000)

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

	periods := RecurringBusyPeriods(raw, dtstart, time.Hour, rangeStart, rangeEnd, 1000)

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
