package ical

import (
	"testing"
	"time"
)

// An override writing neither DTEND nor DURATION takes its series' length only
// when its DTSTART is of its series' value type. Otherwise it lasts as RFC 5545
// §3.6.1 has its DTSTART alone last: a day from a DATE, an instant at a
// DATE-TIME.
func TestOverrideTakesItsSeriesLengthOnlyFromASeriesOfItsValueType(t *testing.T) {
	start := utc(2026, 1, 6, 0, 0)
	midnight := func(date time.Time) time.Time { return date }
	for _, tt := range []struct {
		name                           string
		startIsDate, seriesStartIsDate bool
		seriesLength                   time.Duration
		want                           time.Duration
	}{
		{name: "timed override of a timed series", seriesLength: time.Hour, want: time.Hour},
		{name: "all-day override of an all-day series", startIsDate: true, seriesStartIsDate: true, seriesLength: 48 * time.Hour, want: 48 * time.Hour},
		{name: "all-day override of a timed series", startIsDate: true, seriesLength: time.Hour, want: 24 * time.Hour},
		{name: "timed override of an all-day series", seriesStartIsDate: true, seriesLength: 24 * time.Hour, want: 0},
	} {
		t.Run(tt.name, func(t *testing.T) {
			extent := EventEnd{Start: start, StartIsDate: tt.startIsDate}
			extent.InheritSeriesLength(tt.seriesStartIsDate, tt.seriesLength)
			if got := extent.OccurrenceEnd(midnight).Sub(start); got != tt.want {
				t.Errorf("override lasts %v, want %v", got, tt.want)
			}
		})
	}
}

// Each instance of a series starting on a DATE spans whole days from its own
// date, the nearest whole number of days in the length its caller gives: a
// length measured over a day a transition shortens to 23 hours, or lengthens
// to 25, is one day.
func TestDateSeriesInstancesSpanWholeDaysOfTheirLength(t *testing.T) {
	const raw = "BEGIN:VCALENDAR\r\nVERSION:2.0\r\nBEGIN:VEVENT\r\nUID:days\r\nDTSTART;VALUE=DATE:20260308\r\n" +
		"RRULE:FREQ=DAILY;COUNT=3\r\nEND:VEVENT\r\nEND:VCALENDAR\r\n"
	dtstart := utc(2026, 3, 8, 0, 0)
	for _, length := range []time.Duration{23 * time.Hour, 24 * time.Hour, 25 * time.Hour} {
		periods, err := RecurringBusyPeriods(raw, dtstart, length, dtstart, dtstart.AddDate(0, 0, 3), 10, nil, nil)
		if err != nil {
			t.Fatal(err)
		}
		if len(periods) != 3 {
			t.Fatalf("length %v: periods = %+v, want 3", length, periods)
		}
		for _, period := range periods {
			if period.End.Sub(period.Start) != 24*time.Hour || period.Start.Hour() != 0 {
				t.Errorf("length %v: instance %v-%v, want its whole UTC day", length, period.Start, period.End)
			}
		}
	}
}

// DateOf reads the date a DATE or DATE-TIME value is written on, and only a
// date the calendar has.
func TestDateOfReadsOnlyARealDate(t *testing.T) {
	for value, want := range map[string]string{
		"20260228":         "2026-02-28",
		" 20240229 ":       "2024-02-29",
		"20261101T013000":  "2026-11-01",
		"20261101T013000Z": "2026-11-01",
		"00010101":         "0001-01-01",
		"20250229":         "",
		"20261301":         "",
		"20261100":         "",
		"2026110":          "",
		"2026-11-01":       "",
		"2026110a":         "",
		"+0261101":         "",
	} {
		date, ok := DateOf(value)
		got := ""
		if ok {
			got = date.Format("2006-01-02")
			if date.Location() != time.UTC || date.Hour() != 0 {
				t.Errorf("DateOf(%q) = %v, want a UTC midnight", value, date)
			}
		}
		if got != want {
			t.Errorf("DateOf(%q) = %q, want %q", value, got, want)
		}
	}
}
