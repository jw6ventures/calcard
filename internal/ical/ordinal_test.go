package ical

import (
	"testing"
	"time"
)

func TestDaysByNumberedWeekday(t *testing.T) {
	tests := []struct {
		name string
		get  func() []time.Time
		want string
	}{
		{name: "second monday in month", get: func() []time.Time {
			return daysByWeekdayInMonth(2026, time.January, []weekdaySpecifier{{Ordinal: 2, Day: time.Monday}}, time.UTC)
		}, want: "2026-01-12"},
		{name: "last friday in month", get: func() []time.Time {
			return daysByWeekdayInMonth(2026, time.January, []weekdaySpecifier{{Ordinal: -1, Day: time.Friday}}, time.UTC)
		}, want: "2026-01-30"},
		{name: "fifth monday absent", get: func() []time.Time {
			return daysByWeekdayInMonth(2026, time.February, []weekdaySpecifier{{Ordinal: 5, Day: time.Monday}}, time.UTC)
		}},
		{name: "first thursday in year", get: func() []time.Time {
			return daysByWeekdayInYear(2026, []weekdaySpecifier{{Ordinal: 1, Day: time.Thursday}}, time.UTC)
		}, want: "2026-01-01"},
		{name: "last thursday in year", get: func() []time.Time {
			return daysByWeekdayInYear(2026, []weekdaySpecifier{{Ordinal: -1, Day: time.Thursday}}, time.UTC)
		}, want: "2026-12-31"},
		{name: "fifty-third thursday of a year starting on one", get: func() []time.Time {
			return daysByWeekdayInYear(2026, []weekdaySpecifier{{Ordinal: 53, Day: time.Thursday}}, time.UTC)
		}, want: "2026-12-31"},
		{name: "fifty-third monday absent", get: func() []time.Time {
			return daysByWeekdayInYear(2026, []weekdaySpecifier{{Ordinal: 53, Day: time.Monday}}, time.UTC)
		}},
		{name: "fifty-third last monday absent", get: func() []time.Time {
			return daysByWeekdayInYear(2026, []weekdaySpecifier{{Ordinal: -53, Day: time.Monday}}, time.UTC)
		}},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := tc.get()
			if tc.want == "" {
				if len(got) != 0 {
					t.Fatalf("days = %v, want none", got)
				}
				return
			}
			if len(got) != 1 || got[0].Format(time.DateOnly) != tc.want {
				t.Fatalf("days = %v, want %s", got, tc.want)
			}
		})
	}
}

// The generator computes the day a specifier names and the matcher computes
// which specifiers name a day; the two have to agree on every specifier the
// parser admits, across every month and year shape. Twenty-eight years cover
// each weekday a year or month can start on, in both leap and common years.
func TestWeekdaySetAgreesWithGeneratedDays(t *testing.T) {
	var specifiers []weekdaySpecifier
	for day := time.Sunday; day <= time.Saturday; day++ {
		specifiers = append(specifiers, weekdaySpecifier{Day: day})
		for ordinal := 1; ordinal <= 53; ordinal++ {
			specifiers = append(specifiers, weekdaySpecifier{Ordinal: ordinal, Day: day}, weekdaySpecifier{Ordinal: -ordinal, Day: day})
		}
	}

	for year := 2001; year < 2029; year++ {
		for _, spec := range specifiers {
			set := newWeekdaySet([]weekdaySpecifier{spec})

			named := make(map[string]bool)
			for _, day := range daysByWeekdayInYear(year, []weekdaySpecifier{spec}, time.UTC) {
				named[day.Format(time.DateOnly)] = true
			}
			for dayNumber := 1; dayNumber <= daysInYear(year); dayNumber++ {
				day := time.Date(year, 1, dayNumber, 0, 0, 0, 0, time.UTC)
				if got := set.contains(day.Weekday(), dayNumber, daysInYear(year)); got != named[day.Format(time.DateOnly)] {
					t.Fatalf("year %d, %+v: contains(%s) = %t, generated %t", year, spec, day.Format(time.DateOnly), got, !got)
				}
			}

			for month := time.January; month <= time.December; month++ {
				named := make(map[int]bool)
				for _, day := range daysByWeekdayInMonth(year, month, []weekdaySpecifier{spec}, time.UTC) {
					named[day.Day()] = true
				}
				for dayNumber := 1; dayNumber <= daysInMonth(year, month); dayNumber++ {
					weekday := civilWeekday(year, month, dayNumber)
					if got := set.contains(weekday, dayNumber, daysInMonth(year, month)); got != named[dayNumber] {
						t.Fatalf("%d-%02d, %+v: contains(day %d) = %t, generated %t", year, month, spec, dayNumber, got, !got)
					}
				}
			}
		}
	}
}
