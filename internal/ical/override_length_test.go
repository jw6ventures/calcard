package ical

import (
	"testing"
	"time"
)

// An override that states its own length keeps it, zero included, and a
// RANGE=THISANDFUTURE override carries that length to every instance it moves.
// One writing neither DTEND nor DURATION keeps its series' length. One whose
// DTEND or DURATION cannot end it is read as RFC 5545 §3.6.1 reads any VEVENT,
// which makes a DATE-TIME start an instant: the DAV layer's time-range matching
// reads both that way too.
func TestRecurrenceOverrideLengthIsTheOneItStates(t *testing.T) {
	const master = "BEGIN:VCALENDAR\r\nVERSION:2.0\r\n" +
		"BEGIN:VEVENT\r\nUID:weekly\r\nDTSTART:20260105T090000Z\r\nDTEND:20260105T100000Z\r\nRRULE:FREQ=WEEKLY;COUNT=4\r\nEND:VEVENT\r\n"
	override := func(recurrenceID, length string) string {
		return master + "BEGIN:VEVENT\r\nUID:weekly\r\nRECURRENCE-ID" + recurrenceID + "\r\nDTSTART:20260112T110000Z\r\n" +
			length + "END:VEVENT\r\nEND:VCALENDAR\r\n"
	}
	tests := []struct {
		name   string
		length string
		want   time.Duration
	}{
		{name: "DTEND equal to DTSTART", length: "DTEND:20260112T110000Z\r\n", want: 0},
		{name: "zero DURATION", length: "DURATION:PT0S\r\n", want: 0},
		{name: "own DURATION", length: "DURATION:PT30M\r\n", want: 30 * time.Minute},
		{name: "neither", want: time.Hour},
		{name: "DTEND before DTSTART", length: "DTEND:20260112T100000Z\r\n", want: 0},
		{name: "negative DURATION", length: "DURATION:-PT30M\r\n", want: 0},
		{name: "DTEND before DTSTART beside a DURATION", length: "DTEND:20260112T100000Z\r\nDURATION:PT45M\r\n", want: 45 * time.Minute},
	}
	dtstart := utc(2026, 1, 5, 9, 0)
	rangeStart, rangeEnd := utc(2026, 1, 1, 0, 0), utc(2026, 2, 1, 0, 0)
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			slots := requireRecurrenceResult[RecurrenceSlot](t)(RecurrenceSlots(override(";RANGE=THISANDFUTURE:20260112T090000Z", tt.length), "VEVENT", dtstart, time.Hour, rangeStart, rangeEnd, 100, nil, nil))
			if len(slots) != 4 {
				t.Fatalf("slots = %d, want 4: %+v", len(slots), slots)
			}
			for _, slot := range slots[2:] {
				wantStart := slot.RecurrenceID.Add(2 * time.Hour)
				if !slot.Effective.Start.Equal(wantStart) || slot.Effective.End.Sub(slot.Effective.Start) != tt.want {
					t.Errorf("slot %v moved to %v-%v, want %v lasting %v", slot.RecurrenceID, slot.Effective.Start, slot.Effective.End, wantStart, tt.want)
				}
			}

			periods, err := RecurringBusyPeriods(override(":20260112T090000Z", tt.length), dtstart, time.Hour, rangeStart, rangeEnd, 100, nil, nil)
			if err != nil {
				t.Fatal(err)
			}
			found := false
			for _, period := range periods {
				if period.RecurrenceID.Equal(utc(2026, 1, 12, 9, 0)) {
					found = true
					if got := period.End.Sub(period.Start); got != tt.want {
						t.Errorf("override lasts %v, want %v", got, tt.want)
					}
				} else if got := period.End.Sub(period.Start); got != time.Hour {
					t.Errorf("instance %v lasts %v, want the master's hour", period.Start, got)
				}
			}
			if !found {
				t.Fatalf("periods %+v hold no override", periods)
			}
		})
	}
}
