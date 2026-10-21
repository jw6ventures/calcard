package ical

import (
	"testing"
	"time"
)

func TestConservativeRecurrenceBounds(t *testing.T) {
	tests := []struct {
		name             string
		raw              string
		wantRecurring    bool
		wantStart        string
		wantStartUnknown bool
		wantUntilAtLeast string
		wantUntilUnknown bool
	}{
		{
			name: "non recurring event",
			raw:  "BEGIN:VCALENDAR\r\nBEGIN:VEVENT\r\nDTSTART:20260101T090000Z\r\nEND:VEVENT\r\nEND:VCALENDAR\r\n",
		},
		{
			name:             "bounded daily event",
			raw:              "BEGIN:VCALENDAR\r\nBEGIN:VEVENT\r\nDTSTART:20260101T090000Z\r\nDTEND:20260101T100000Z\r\nRRULE:FREQ=DAILY;COUNT=3\r\nEND:VEVENT\r\nEND:VCALENDAR\r\n",
			wantRecurring:    true,
			wantStart:        "2026-01-01T09:00:00Z",
			wantUntilAtLeast: "2026-01-03T10:00:00Z",
		},
		{
			name:             "unbounded todo is retained conservatively",
			raw:              "BEGIN:VCALENDAR\r\nBEGIN:VTODO\r\nDTSTART:20260101T090000Z\r\nRRULE:FREQ=WEEKLY\r\nEND:VTODO\r\nEND:VCALENDAR\r\n",
			wantRecurring:    true,
			wantStart:        "2026-01-01T09:00:00Z",
			wantUntilUnknown: true,
		},
		{
			name:             "early rdate extends journal start",
			raw:              "BEGIN:VCALENDAR\r\nBEGIN:VJOURNAL\r\nDTSTART:20260610T090000Z\r\nRDATE:20260601T090000Z\r\nEND:VJOURNAL\r\nEND:VCALENDAR\r\n",
			wantRecurring:    true,
			wantStart:        "2026-06-01T09:00:00Z",
			wantUntilUnknown: true,
		},
		{
			name:             "invalid rdate makes both bounds unknown",
			raw:              "BEGIN:VCALENDAR\r\nBEGIN:VEVENT\r\nRDATE:not-a-date\r\nEND:VEVENT\r\nEND:VCALENDAR\r\n",
			wantRecurring:    true,
			wantStartUnknown: true,
			wantUntilUnknown: true,
		},
		{
			name:             "duplicate dtstart makes both bounds unknown",
			raw:              "BEGIN:VCALENDAR\r\nBEGIN:VEVENT\r\nDTSTART:20260101T090000Z\r\nDTSTART:20270101T090000Z\r\nRRULE:FREQ=DAILY;COUNT=2\r\nEND:VEVENT\r\nEND:VCALENDAR\r\n",
			wantRecurring:    true,
			wantStart:        "2026-01-01T09:00:00Z",
			wantStartUnknown: true,
			wantUntilUnknown: true,
		},
		{
			name:             "duplicate dtend makes upper bound unknown",
			raw:              "BEGIN:VCALENDAR\r\nBEGIN:VEVENT\r\nDTSTART:20260101T090000Z\r\nDTEND:20260101T100000Z\r\nDTEND:20260101T093000Z\r\nRRULE:FREQ=DAILY;COUNT=2\r\nEND:VEVENT\r\nEND:VCALENDAR\r\n",
			wantRecurring:    true,
			wantStart:        "2026-01-01T09:00:00Z",
			wantUntilUnknown: true,
		},
		{
			name:             "duplicate duration makes upper bound unknown",
			raw:              "BEGIN:VCALENDAR\r\nBEGIN:VEVENT\r\nDTSTART:20260101T090000Z\r\nDURATION:PT2H\r\nDURATION:PT1H\r\nRRULE:FREQ=DAILY;COUNT=2\r\nEND:VEVENT\r\nEND:VCALENDAR\r\n",
			wantRecurring:    true,
			wantStart:        "2026-01-01T09:00:00Z",
			wantUntilUnknown: true,
		},
		{
			name:             "empty duplicate rrule preserves recurring classification",
			raw:              "BEGIN:VCALENDAR\r\nBEGIN:VEVENT\r\nDTSTART:20260101T090000Z\r\nDTEND:20260101T100000Z\r\nRRULE:FREQ=DAILY;COUNT=3\r\nRRULE:\r\nEND:VEVENT\r\nEND:VCALENDAR\r\n",
			wantRecurring:    true,
			wantStart:        "2026-01-01T09:00:00Z",
			wantUntilUnknown: true,
		},
		{
			name:             "nonpositive dtend falls back to duration",
			raw:              "BEGIN:VCALENDAR\r\nBEGIN:VEVENT\r\nDTSTART:20260101T090000Z\r\nDTEND:20260101T080000Z\r\nDURATION:PT72H\r\nRRULE:FREQ=DAILY;UNTIL=20260103T090000Z\r\nEND:VEVENT\r\nEND:VCALENDAR\r\n",
			wantRecurring:    true,
			wantStart:        "2026-01-01T09:00:00Z",
			wantUntilAtLeast: "2026-01-06T09:00:00Z",
		},
		{
			name:             "nonpositive all day dtend falls back to one day",
			raw:              "BEGIN:VCALENDAR\r\nBEGIN:VEVENT\r\nDTSTART;VALUE=DATE:20260101\r\nDTEND;VALUE=DATE:20260101\r\nRRULE:FREQ=DAILY;UNTIL=20260103\r\nEND:VEVENT\r\nEND:VCALENDAR\r\n",
			wantRecurring:    true,
			wantStart:        "2026-01-01T00:00:00Z",
			wantUntilAtLeast: "2026-01-04T00:00:00Z",
		},
		{
			name:             "duplicate recurrence id makes both bounds unknown",
			raw:              "BEGIN:VCALENDAR\r\nBEGIN:VEVENT\r\nRECURRENCE-ID;RANGE=THISANDFUTURE:20260102T090000Z\r\nRECURRENCE-ID:20260103T090000Z\r\nDTSTART:20260102T100000Z\r\nDTEND:20260102T110000Z\r\nEND:VEVENT\r\nEND:VCALENDAR\r\n",
			wantRecurring:    true,
			wantStart:        "2026-01-02T10:00:00Z",
			wantStartUnknown: true,
			wantUntilUnknown: true,
		},
		{
			// RFC 5545 §3.8.5.3 makes the DTSTART the first instance even
			// when UNTIL falls before it, so the bound reaches its end.
			name:             "until before dtstart keeps the dtstart instance",
			raw:              "BEGIN:VCALENDAR\r\nBEGIN:VEVENT\r\nDTSTART:20240110T100000Z\r\nDTEND:20240110T110000Z\r\nRRULE:FREQ=DAILY;UNTIL=20240101T000000Z\r\nEND:VEVENT\r\nEND:VCALENDAR\r\n",
			wantRecurring:    true,
			wantStart:        "2024-01-10T10:00:00Z",
			wantUntilAtLeast: "2024-01-10T11:00:00Z",
		},
		{
			name:             "until before dtstart with a duration and a by part",
			raw:              "BEGIN:VCALENDAR\r\nBEGIN:VEVENT\r\nDTSTART:20240110T100000Z\r\nDURATION:PT5H\r\nRRULE:FREQ=WEEKLY;BYDAY=MO;UNTIL=20240101T000000Z\r\nEND:VEVENT\r\nEND:VCALENDAR\r\n",
			wantRecurring:    true,
			wantStart:        "2024-01-10T10:00:00Z",
			wantUntilAtLeast: "2024-01-10T15:00:00Z",
		},
		{
			name:             "until before a dtstart with no end",
			raw:              "BEGIN:VCALENDAR\r\nBEGIN:VEVENT\r\nDTSTART:20240110T100000Z\r\nRRULE:FREQ=MONTHLY;UNTIL=20231231T235959Z\r\nEND:VEVENT\r\nEND:VCALENDAR\r\n",
			wantRecurring:    true,
			wantStart:        "2024-01-10T10:00:00Z",
			wantUntilAtLeast: "2024-01-10T10:00:00Z",
		},
		{
			name:             "date until before an all day dtstart",
			raw:              "BEGIN:VCALENDAR\r\nBEGIN:VEVENT\r\nDTSTART;VALUE=DATE:20240110\r\nRRULE:FREQ=YEARLY;UNTIL=20240101\r\nEND:VEVENT\r\nEND:VCALENDAR\r\n",
			wantRecurring:    true,
			wantStart:        "2024-01-10T00:00:00Z",
			wantUntilAtLeast: "2024-01-11T00:00:00Z",
		},
		{
			// A DATE UNTIL on a timed series is read as the end of its day.
			name:             "date until before a timed dtstart",
			raw:              "BEGIN:VCALENDAR\r\nBEGIN:VEVENT\r\nDTSTART:20240110T100000Z\r\nDTEND:20240110T110000Z\r\nRRULE:FREQ=DAILY;UNTIL=20240101\r\nEND:VEVENT\r\nEND:VCALENDAR\r\n",
			wantRecurring:    true,
			wantStart:        "2024-01-10T10:00:00Z",
			wantUntilAtLeast: "2024-01-10T11:00:00Z",
		},
		{
			name: "until before a dtstart in a zone the object defines",
			raw: "BEGIN:VCALENDAR\r\nBEGIN:VTIMEZONE\r\nTZID:Private/Zone\r\nBEGIN:STANDARD\r\nDTSTART:19700101T000000\r\nTZOFFSETFROM:-0500\r\nTZOFFSETTO:-0500\r\nEND:STANDARD\r\nEND:VTIMEZONE\r\n" +
				"BEGIN:VEVENT\r\nDTSTART;TZID=Private/Zone:20240110T100000\r\nDTEND;TZID=Private/Zone:20240110T113000\r\nRRULE:FREQ=DAILY;UNTIL=20240101T000000Z\r\nEND:VEVENT\r\nEND:VCALENDAR\r\n",
			wantRecurring:    true,
			wantStart:        "2024-01-10T15:00:00Z",
			wantUntilAtLeast: "2024-01-10T16:30:00Z",
		},
		{
			// An hour beside a DATE start spans the whole day it reaches into,
			// as every reader places it, so the bound runs to the day's end.
			name:             "all-day series whose DURATION counts an hour",
			raw:              "BEGIN:VCALENDAR\r\nBEGIN:VEVENT\r\nDTSTART;VALUE=DATE:20260305\r\nDURATION:PT1H\r\nRRULE:FREQ=DAILY;UNTIL=20260310\r\nEND:VEVENT\r\nEND:VCALENDAR\r\n",
			wantRecurring:    true,
			wantStart:        "2026-03-05T00:00:00Z",
			wantUntilAtLeast: "2026-03-11T00:00:00Z",
		},
		{
			name: "all-day override whose DURATION counts an hour",
			raw: "BEGIN:VCALENDAR\r\nBEGIN:VEVENT\r\nRECURRENCE-ID;VALUE=DATE:20260306\r\nDTSTART;VALUE=DATE:20260325\r\n" +
				"DURATION:PT1H\r\nEND:VEVENT\r\nEND:VCALENDAR\r\n",
			wantRecurring:    true,
			wantStart:        "2026-03-25T00:00:00Z",
			wantUntilAtLeast: "2026-03-26T00:00:00Z",
		},
		{
			name: "timezone recurrence rule is ignored",
			raw:  "BEGIN:VCALENDAR\r\nBEGIN:VTIMEZONE\r\nBEGIN:DAYLIGHT\r\nDTSTART:19870405T020000\r\nRRULE:FREQ=YEARLY\r\nEND:DAYLIGHT\r\nEND:VTIMEZONE\r\nBEGIN:VEVENT\r\nDTSTART:20260101T090000Z\r\nEND:VEVENT\r\nEND:VCALENDAR\r\n",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := ConservativeRecurrenceBounds(tc.raw)
			if got.Recurring != tc.wantRecurring {
				t.Fatalf("Recurring = %v, want %v", got.Recurring, tc.wantRecurring)
			}
			if got.StartUnknown != tc.wantStartUnknown {
				t.Fatalf("StartUnknown = %v, want %v", got.StartUnknown, tc.wantStartUnknown)
			}
			if got.UntilUnknown != tc.wantUntilUnknown {
				t.Fatalf("UntilUnknown = %v, want %v", got.UntilUnknown, tc.wantUntilUnknown)
			}
			if tc.wantStart != "" {
				want := mustParseBoundsTime(t, tc.wantStart)
				if got.Start == nil || !got.Start.Equal(want) {
					t.Fatalf("Start = %v, want %v", got.Start, want)
				}
			} else if got.Start != nil {
				t.Fatalf("Start = %v, want nil", got.Start)
			}
			if tc.wantUntilAtLeast != "" {
				minimum := mustParseBoundsTime(t, tc.wantUntilAtLeast)
				if got.Until == nil || got.Until.Before(minimum) {
					t.Fatalf("Until = %v, want at least %v", got.Until, minimum)
				}
			}
		})
	}
}

func mustParseBoundsTime(t *testing.T, value string) time.Time {
	t.Helper()
	parsed, err := time.Parse(time.RFC3339, value)
	if err != nil {
		t.Fatalf("parse time %q: %v", value, err)
	}
	return parsed
}
