package ical

import (
	"fmt"
	"strings"
	"testing"
	"time"
)

// overridesBeforeTheirMaster is a daily series whose first n slots are each
// replaced by an override writing no end, all written before the master, as
// RFC 5545 lets an object order its components.
func overridesBeforeTheirMaster(n int) string {
	var b strings.Builder
	b.WriteString("BEGIN:VCALENDAR\r\nVERSION:2.0\r\n")
	first := time.Date(2020, 1, 1, 9, 0, 0, 0, time.UTC)
	for i := range n {
		slot := first.AddDate(0, 0, i).Format("20060102T150405Z")
		fmt.Fprintf(&b, "BEGIN:VEVENT\r\nUID:series\r\nRECURRENCE-ID:%s\r\nDTSTART:%s\r\nEND:VEVENT\r\n", slot, slot)
	}
	b.WriteString("BEGIN:VEVENT\r\nUID:series\r\nDTSTART:20200101T090000Z\r\nDTEND:20200101T100000Z\r\nRRULE:FREQ=DAILY\r\nEND:VEVENT\r\nEND:VCALENDAR\r\n")
	return b.String()
}

// Each override writing no end asks whether its series starts on a DATE,
// which is a question about the master alone. A master written after its
// overrides must not make that a walk over them for each override.
func TestRecurringBusyPeriodsScalesWithOverridesWrittenBeforeTheirMaster(t *testing.T) {
	dtstart := time.Date(2020, 1, 1, 9, 0, 0, 0, time.UTC)
	expand := func(t *testing.T, raw string) []BusyPeriod {
		t.Helper()
		periods, err := RecurringBusyPeriods(raw, dtstart, time.Hour, dtstart, dtstart.AddDate(0, 1, 0), MaxRecurrenceInstances, nil)
		if err != nil {
			t.Fatalf("RecurringBusyPeriods() err = %v", err)
		}
		return periods
	}
	if periods := expand(t, overridesBeforeTheirMaster(10)); len(periods) != 31 || periods[0].End.Sub(periods[0].Start) != time.Hour {
		t.Fatalf("periods = %+v, want 31 lasting the series' hour", periods)
	}

	assertScalesLinearly(t, 2_000, func(n int) func() {
		raw := overridesBeforeTheirMaster(n)
		return func() { expand(t, raw) }
	})
}
