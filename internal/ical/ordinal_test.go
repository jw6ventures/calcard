package ical

import (
	"testing"
	"time"
)

func TestNthWeekdayHelpers(t *testing.T) {
	tests := []struct {
		name string
		get  func() (time.Time, bool)
		want string
		ok   bool
	}{
		{name: "second monday in month", get: func() (time.Time, bool) {
			return nthWeekdayInMonth(2026, time.January, weekdaySpecifier{Ordinal: 2, Day: time.Monday}, time.UTC)
		}, want: "2026-01-12", ok: true},
		{name: "last friday in month", get: func() (time.Time, bool) {
			return nthWeekdayInMonth(2026, time.January, weekdaySpecifier{Ordinal: -1, Day: time.Friday}, time.UTC)
		}, want: "2026-01-30", ok: true},
		{name: "fifth monday absent", get: func() (time.Time, bool) {
			return nthWeekdayInMonth(2026, time.February, weekdaySpecifier{Ordinal: 5, Day: time.Monday}, time.UTC)
		}, ok: false},
		{name: "first thursday in year", get: func() (time.Time, bool) {
			return nthWeekdayInYear(2026, weekdaySpecifier{Ordinal: 1, Day: time.Thursday}, time.UTC)
		}, want: "2026-01-01", ok: true},
		{name: "last thursday in year", get: func() (time.Time, bool) {
			return nthWeekdayInYear(2026, weekdaySpecifier{Ordinal: -1, Day: time.Thursday}, time.UTC)
		}, want: "2026-12-31", ok: true},
		{name: "impossible yearly ordinal", get: func() (time.Time, bool) {
			return nthWeekdayInYear(2026, weekdaySpecifier{Ordinal: 54, Day: time.Monday}, time.UTC)
		}, ok: false},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, ok := tc.get()
			if ok != tc.ok {
				t.Fatalf("ok = %v, want %v", ok, tc.ok)
			}
			if tc.ok && got.Format(time.DateOnly) != tc.want {
				t.Fatalf("date = %s, want %s", got.Format(time.DateOnly), tc.want)
			}
		})
	}
}
