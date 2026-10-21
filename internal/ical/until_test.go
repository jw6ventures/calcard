package ical

import (
	"strings"
	"testing"
	"time"
)

func untilEvent(dtstart, rrule string) string {
	return "BEGIN:VCALENDAR\r\nVERSION:2.0\r\nBEGIN:VEVENT\r\nUID:until\r\n" +
		dtstart + "\r\nRRULE:" + rrule + "\r\nEND:VEVENT\r\nEND:VCALENDAR\r\n"
}

func utc(year int, month time.Month, day, hour, minute int) time.Time {
	return time.Date(year, month, day, hour, minute, 0, 0, time.UTC)
}

// A DATE-valued UNTIL on a DATE-TIME DTSTART bounds the set through the end of
// the day it names, in the zone the DTSTART is read in: RFC 5545 §3.3.10 makes
// UNTIL inclusive, and a date names the whole day.
func TestDateUntilOnATimedSeriesIncludesTheWholeDay(t *testing.T) {
	behind := PropertyTimeResolver(func(keyPart, value string) (time.Time, bool) {
		parsed, ok := ParsePropertyDateTimeLocal(keyPart, value)
		return parsed.Add(3 * time.Hour), ok
	})
	tests := []struct {
		name     string
		dtstart  string
		rrule    string
		zones    []string
		resolve  PropertyTimeResolver
		wantLast time.Time
		count    int
	}{
		{
			name:     "UTC weekly",
			dtstart:  "DTSTART:20260105T090000Z",
			rrule:    "FREQ=WEEKLY;UNTIL=20260302",
			wantLast: utc(2026, 3, 2, 9, 0),
			count:    9,
		},
		{
			name:     "UTC daily stops before the next day",
			dtstart:  "DTSTART:20260301T090000Z",
			rrule:    "FREQ=DAILY;UNTIL=20260302",
			wantLast: utc(2026, 3, 2, 9, 0),
			count:    2,
		},
		{
			// 22:00 in New York is already the next day in UTC.
			name:     "zone behind UTC",
			dtstart:  "DTSTART;TZID=America/New_York:20260223T220000",
			rrule:    "FREQ=DAILY;UNTIL=20260302",
			zones:    []string{"America/New_York"},
			wantLast: utc(2026, 3, 3, 3, 0),
			count:    8,
		},
		{
			// 09:00 in Auckland is the previous day in UTC, so the 3 March
			// instance falls on 2 March UTC and is still past the UNTIL day.
			name:     "zone ahead of UTC",
			dtstart:  "DTSTART;TZID=Pacific/Auckland:20260226T090000",
			rrule:    "FREQ=DAILY;UNTIL=20260302",
			zones:    []string{"Pacific/Auckland"},
			wantLast: utc(2026, 3, 1, 20, 0),
			count:    5,
		},
		{
			name:     "floating read as UTC",
			dtstart:  "DTSTART:20260223T220000",
			rrule:    "FREQ=DAILY;UNTIL=20260302",
			wantLast: utc(2026, 3, 2, 22, 0),
			count:    8,
		},
		{
			name:     "floating read in the caller's zone",
			dtstart:  "DTSTART:20260223T220000",
			rrule:    "FREQ=DAILY;UNTIL=20260302",
			resolve:  behind,
			wantLast: utc(2026, 3, 3, 1, 0),
			count:    8,
		},
		{
			// Santiago springs forward at midnight on 6 September 2026, so
			// that day has no 00:00 and 5 September ends at 04:00 UTC.
			name:     "day ending in a spring-forward gap",
			dtstart:  "DTSTART;TZID=America/Santiago:20260901T233000",
			rrule:    "FREQ=DAILY;UNTIL=20260905",
			zones:    []string{"America/Santiago"},
			wantLast: utc(2026, 9, 6, 3, 30),
			count:    5,
		},
		{
			// Santiago falls back at midnight on 5 April 2026, so 23:00-24:00
			// on 4 April is read twice.
			name:     "day ending in a fall-back fold",
			dtstart:  "DTSTART;TZID=America/Santiago:20260401T233000",
			rrule:    "FREQ=DAILY;UNTIL=20260404",
			zones:    []string{"America/Santiago"},
			wantLast: utc(2026, 4, 5, 2, 30),
			count:    4,
		},
		{
			name:     "all-day series",
			dtstart:  "DTSTART;VALUE=DATE:20260301",
			rrule:    "FREQ=DAILY;UNTIL=20260303",
			wantLast: utc(2026, 3, 3, 0, 0),
			count:    3,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			for _, zone := range tt.zones {
				requireHostZone(t, zone)
			}
			raw := untilEvent(tt.dtstart, tt.rrule)
			keyPart, value, _ := strings.Cut(tt.dtstart, ":")
			dtstart, ok := tt.resolve.or(keyPart, value)
			if !ok {
				t.Fatalf("DTSTART %q did not resolve", tt.dtstart)
			}
			rangeStart := dtstart.AddDate(0, -1, 0)
			rangeEnd := tt.wantLast.AddDate(0, 1, 0)
			starts := instanceStarts(requireRecurrenceResult[RecurrenceInstance](t)(
				collectRecurrenceInstances(raw, "VEVENT", dtstart, time.Hour, rangeStart, rangeEnd, 1000, tt.resolve)))
			if len(starts) != tt.count {
				t.Fatalf("instances = %v, want %d ending %v", starts, tt.count, tt.wantLast)
			}
			if last := starts[len(starts)-1]; !last.Equal(tt.wantLast) {
				t.Fatalf("last instance = %v, want %v", last, tt.wantLast)
			}

			if tt.resolve != nil {
				return
			}
			if exceeds, ok := RecurrenceSetExceedsLimit(raw, tt.count); !ok || exceeds {
				t.Errorf("RecurrenceSetExceedsLimit(limit %d) = %v, %v; want false, true", tt.count, exceeds, ok)
			}
			if exceeds, ok := RecurrenceSetExceedsLimit(raw, tt.count-1); !ok || !exceeds {
				t.Errorf("RecurrenceSetExceedsLimit(limit %d) = %v, %v; want true, true", tt.count-1, exceeds, ok)
			}

			bounds := ConservativeRecurrenceBounds(raw)
			if bounds.UntilUnknown || bounds.Until == nil {
				t.Fatalf("bounds = %+v, want a known until", bounds)
			}
			if lastEnd := tt.wantLast.Add(time.Hour); bounds.Until.Before(lastEnd) {
				t.Errorf("bounds until %v is before the last instance ends at %v", bounds.Until, lastEnd)
			}
		})
	}
}

// RFC 5545 §3.3.10 requires a UTC UNTIL beside a DTSTART with a TZID. One
// written as a floating time anyway is read on the DTSTART's wall clock by
// every reader of the series -- the expansion, whatever zone its caller reads
// floating values in, the instance count and the stored bounds -- so all of
// them end the series at the same instance.
func TestFloatingUntilBesideAZonedDTStartIsReadInTheDTStartsZone(t *testing.T) {
	newYork := requireHostZone(t, "America/New_York")
	tokyo := requireHostZone(t, "Asia/Tokyo")
	// 02:30 on 12 March is 06:30Z in New York; read as UTC, or in Tokyo, it is
	// earlier and would end the series an instance sooner. 10 March is the
	// spring change, whose 02:30 is still an instance, read at the offset
	// before the gap (RFC 5545 §3.3.5).
	raw := untilEvent("DTSTART;TZID=America/New_York:20240308T023000", "FREQ=DAILY;UNTIL=20240312T023000")
	want := []time.Time{utc(2024, 3, 8, 7, 30), utc(2024, 3, 9, 7, 30), utc(2024, 3, 10, 7, 30), utc(2024, 3, 11, 6, 30), utc(2024, 3, 12, 6, 30)}
	dtstart := WallClockIn(newYork, utc(2024, 3, 8, 2, 30))
	floatingInTokyo := PropertyTimeResolver(func(keyPart, value string) (time.Time, bool) {
		if _, zoned := PropertyParam(keyPart, "TZID"); zoned || hasZoneSuffix(value) {
			return ParsePropertyDateTimeLocal(keyPart, value)
		}
		wall, err := ParseDateTime(value)
		return WallClockIn(tokyo, wall).UTC(), err == nil
	})
	for name, resolve := range map[string]RecurrenceTimeResolver{"floating read as UTC": nil, "floating read in the caller's zone": floatingInTokyo} {
		starts := instanceStarts(requireRecurrenceResult[RecurrenceInstance](t)(
			collectRecurrenceInstances(raw, "VEVENT", dtstart, time.Hour, dtstart.AddDate(0, 0, -1), dtstart.AddDate(0, 1, 0), 1000, resolve)))
		if !sameInstants(starts, want) {
			t.Errorf("%s: expansion = %v, want %v", name, utcStrings(starts), utcStrings(want))
		}
	}
	if exceeds, ok := RecurrenceSetExceedsLimit(raw, len(want)); !ok || exceeds {
		t.Errorf("RecurrenceSetExceedsLimit(limit %d) = %v, %v; want false, true", len(want), exceeds, ok)
	}
	if exceeds, ok := RecurrenceSetExceedsLimit(raw, len(want)-1); !ok || !exceeds {
		t.Errorf("RecurrenceSetExceedsLimit(limit %d) = %v, %v; want true, true", len(want)-1, exceeds, ok)
	}
	bounds := ConservativeRecurrenceBounds(raw)
	if lastEnd := want[len(want)-1].Add(time.Hour); bounds.UntilUnknown || bounds.Until == nil || bounds.Until.Before(lastEnd) {
		t.Errorf("bounds = %+v, want an until at or after the last instance's end %v", bounds, lastEnd)
	}
}

// An observance's UNTIL is a UTC DATE-TIME (RFC 5545 §3.6.5), never a DATE, so
// reading a DATE as its whole day leaves the observance onsets alone.
func TestObservanceUntilStopsAtTheOnsetItNames(t *testing.T) {
	dtstart := utc(2020, 3, 8, 2, 0)
	latest, found, complete := LatestRecurrenceOnOrBefore(dtstart, utc(2030, 1, 1, 0, 0), "FREQ=YEARLY;BYMONTH=3;BYDAY=2SU;UNTIL=20230312T070000Z")
	if !found || !complete {
		t.Fatalf("LatestRecurrenceOnOrBefore() found = %v, complete = %v", found, complete)
	}
	if want := utc(2023, 3, 12, 2, 0); !latest.Equal(want) {
		t.Fatalf("latest onset = %v, want %v", latest, want)
	}
}
