package ical

import "time"

// EventEnd is what a VEVENT writes about where one occurrence of it ends. Every
// reader placing an occurrence -- the recurrence expansion here, and the DAV
// layer's time-range matching, free-busy and expansion -- reads it through
// OccurrenceEnd, so none of them can place the same occurrence differently.
type EventEnd struct {
	// Start is the occurrence's DTSTART, and StartIsDate reports a DATE.
	Start       time.Time
	StartIsDate bool
	// End is its DTEND, when HasEnd reports one that could be read.
	End    time.Time
	HasEnd bool
	// Duration is its DURATION, when HasDuration reports one that could be read.
	Duration    time.Duration
	HasDuration bool
	// InheritsLength reports an override of an occurrence of a series that
	// writes neither DTEND nor DURATION, which lasts SeriesLength, the length
	// of an occurrence of its series.
	InheritsLength bool
	SeriesLength   time.Duration
}

// UsableEnd reports whether the DTEND ends the event. RFC 5545 §3.8.2.2
// requires it later than DTSTART, and calendar applications exporting a one-day
// event write its date as both, so a DTEND that is not later is read as though
// it were not written. A DATE-TIME DTEND equal to its DTSTART is kept: it is
// the instant RFC 4791 §9.9's zero-duration row places.
func (e EventEnd) UsableEnd() bool {
	return e.HasEnd && (e.End.After(e.Start) || e.End.Equal(e.Start) && !e.StartIsDate)
}

// OccurrenceEnd is where the occurrence ends: at its DTEND when UsableEnd, else
// after its DURATION, a negative one read as zero, else, for an override
// writing neither, after its series' length -- it changes its instance's start
// and keeps the rest of it -- else, as RFC 5545 §3.6.1 has it, at the end of a
// DATE start's day and at a DATE-TIME start itself. An override whose DTEND or
// DURATION cannot end it takes that last reading, as any VEVENT does. dayEnd
// places the end of the day starting at a DATE start, which only the caller's
// time zone can.
func (e EventEnd) OccurrenceEnd(dayEnd func(dayStart time.Time) time.Time) time.Time {
	switch {
	case e.UsableEnd():
		return e.End
	case e.HasDuration:
		return e.Start.Add(max(e.Duration, 0))
	case e.InheritsLength:
		return e.Start.Add(e.SeriesLength)
	case e.StartIsDate:
		return dayEnd(e.Start)
	default:
		return e.Start
	}
}
