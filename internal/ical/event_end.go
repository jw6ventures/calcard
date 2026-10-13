package ical

import (
	"strings"
	"time"
)

// EventEnd is what a VEVENT writes about where one occurrence of it ends. The
// readers placing a single component -- an override in the recurrence
// expansion here, and the DAV layer's time-range matching, free-busy and
// expansion -- read it through OccurrenceEnd, so none of them can place the
// same occurrence differently. A series' generated instances are placed from
// its length instead, which a DATE series gives in whole days (NominalDays).
type EventEnd struct {
	// Start is the occurrence's DTSTART, and StartIsDate reports a DATE, the
	// date StartDate holds.
	Start       time.Time
	StartIsDate bool
	StartDate   time.Time
	// End is its DTEND, when HasEnd reports one that could be read, and
	// EndDate the date it holds when it is a DATE.
	End     time.Time
	EndDate time.Time
	HasEnd  bool
	// Duration is its DURATION, when HasDuration reports one that could be read.
	Duration    time.Duration
	HasDuration bool
	// InheritsLength reports an override that writes neither DTEND nor
	// DURATION and lasts SeriesLength, the length of an occurrence of its
	// series. InheritSeriesLength decides both.
	InheritsLength bool
	SeriesLength   time.Duration
}

// InheritSeriesLength makes e, an override writing neither DTEND nor DURATION,
// last seriesLength when its DTSTART is of the value type of its series'
// DTSTART, a DATE when seriesStartIsDate. An override of the other type is
// left to RFC 5545 §3.6.1's own reading of its DTSTART: the series' length
// measures an occurrence of a timed event in hours and of an all-day one in
// days, and neither is a length an occurrence of the other kind has.
func (e *EventEnd) InheritSeriesLength(seriesStartIsDate bool, seriesLength time.Duration) {
	e.InheritsLength = e.StartIsDate == seriesStartIsDate
	e.SeriesLength = seriesLength
}

// UsableEnd reports whether the DTEND ends the event. RFC 5545 §3.8.2.2
// requires it later than DTSTART, and calendar applications exporting a one-day
// event write its date as both, so a DTEND that is not later is read as though
// it were not written. A DATE-TIME DTEND equal to its DTSTART is kept: it is
// the instant RFC 4791 §9.9's zero-duration row places.
func (e EventEnd) UsableEnd() bool {
	return e.HasEnd && (e.End.After(e.Start) || e.End.Equal(e.Start) && !e.StartIsDate)
}

// Days is how many days an occurrence starting on a DATE spans: those to its
// DTEND's date when UsableEnd, else the days its DURATION reaches into
// (DurationDays) -- RFC 5545 §3.8.2.5 has it count days or weeks -- else its
// series' NominalDays for an override taking its series' length, else the one
// day RFC 5545 §3.6.1 gives a DATE start alone.
func (e EventEnd) Days() int {
	switch {
	case e.UsableEnd() && !e.EndDate.IsZero():
		return CivilDaysBetween(e.StartDate, e.EndDate)
	case e.UsableEnd():
		return DurationDays(e.End.Sub(e.Start))
	case e.HasDuration:
		return DurationDays(e.Duration)
	case e.InheritsLength:
		return NominalDays(e.SeriesLength)
	default:
		return 1
	}
}

// OccurrenceEnd is where the occurrence ends. One starting on a DATE spans
// the Days from its own date, ending at the midnight that many dates on,
// which only the caller's time zone can place: midnight resolves a date to
// it. Any other ends at its DTEND when UsableEnd, else after its DURATION, a
// negative one read as zero, else, for an override taking its series' length,
// after it -- it changes its instance's start and keeps the rest of it --
// else, as RFC 5545 §3.6.1 has it, at its start. An override whose DTEND or
// DURATION cannot end it takes that last reading, as any VEVENT does.
func (e EventEnd) OccurrenceEnd(midnight func(date time.Time) time.Time) time.Time {
	switch {
	case e.StartIsDate:
		date := e.StartDate
		if date.IsZero() {
			date = CivilDate(e.Start)
		}
		return midnight(date.AddDate(0, 0, e.Days()))
	case e.UsableEnd():
		return e.End
	case e.HasDuration:
		return e.Start.Add(max(e.Duration, 0))
	case e.InheritsLength:
		return e.Start.Add(e.SeriesLength)
	default:
		return e.Start
	}
}

// DurationDays is the number of days a DURATION beside a DATE DTSTART lasts:
// the whole days it reaches into, so an event written to last into a day
// occupies that day, and none for a zero or negative one.
func DurationDays(duration time.Duration) int {
	const day = 24 * time.Hour
	if duration <= 0 {
		return 0
	}
	return int((duration + day - 1) / day)
}

// NominalDays is the number of days in the length of an occurrence of a DATE
// series, which is given as 24 hours a day however many hours the days it
// lands on hold.
func NominalDays(length time.Duration) int {
	const day = 24 * time.Hour
	if length <= 0 {
		return 0
	}
	return int((length + day/2) / day)
}

// DateLayout is the spelling of an iCalendar DATE.
const DateLayout = "20060102"

// DateOf is the date a date-valued property's value names, held as that
// date's UTC midnight: a DATE's own, and a DATE-TIME's on the clock it is
// written in -- its wall clock, or UTC's for a value in UTC.
func DateOf(value string) (time.Time, bool) {
	value = strings.TrimSpace(value)
	if len(value) < len(DateLayout) {
		return time.Time{}, false
	}
	// Read digit by digit rather than through time.Parse: an expansion reads
	// the date of every override of a series, which may number thousands.
	var fields [3]int
	for i, width := range [3]int{4, 2, 2} {
		offset := [3]int{0, 4, 6}[i]
		for _, c := range value[offset : offset+width] {
			if c < '0' || c > '9' {
				return time.Time{}, false
			}
			fields[i] = fields[i]*10 + int(c-'0')
		}
	}
	year, month, day := fields[0], time.Month(fields[1]), fields[2]
	date := time.Date(year, month, day, 0, 0, 0, 0, time.UTC)
	if month < time.January || month > time.December || day < 1 || date.Month() != month {
		return time.Time{}, false
	}
	return date, true
}

// CivilDaysBetween counts the calendar days from one date to another, each
// held as its UTC midnight, which may lie further apart than a time.Duration
// measures.
func CivilDaysBetween(from, to time.Time) int {
	return int((to.Unix() - from.Unix()) / (24 * 60 * 60))
}

// CivilDate is the date of a civil time, one whose fields hold a wall clock or
// a UTC instant, as its UTC midnight.
func CivilDate(t time.Time) time.Time {
	return time.Date(t.Year(), t.Month(), t.Day(), 0, 0, 0, 0, time.UTC)
}
