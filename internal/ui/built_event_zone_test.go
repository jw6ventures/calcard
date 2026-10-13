package ui

import (
	"strings"
	"testing"
	"time"

	"github.com/jw6ventures/calcard/internal/dav"
	"github.com/jw6ventures/calcard/internal/ical"
	"github.com/jw6ventures/calcard/internal/ui/utils"
)

// The UI and the REST API store an event through the validator, which writes
// the VTIMEZONE for its zone, and a client reads the object by it rather than
// by the host. A weekly series with no end is read out to
// CALDAV:max-date-time, so the definition has to place its instances in every
// later year as the host's zone does, not only in the year the series starts.
func TestBuiltEventDefinesItsZoneForEveryInstanceOfTheSeries(t *testing.T) {
	for _, tzid := range []string{"Europe/Berlin", "America/New_York", "Australia/Sydney"} {
		t.Run(tzid, func(t *testing.T) {
			if _, err := time.LoadLocation(tzid); err != nil {
				t.Skipf("tzdata unavailable: %v", err)
			}
			body, err := utils.BuildEvent("ui-series", "Standup", "2026-01-05T09:00", "2026-01-05T10:00", false, "", "",
				&utils.RecurrenceOptions{Frequency: "WEEKLY"}, &utils.EventOptions{Timezone: tzid})
			if err != nil {
				t.Fatalf("BuildEvent() error = %v", err)
			}
			object, err := dav.ValidateCalendarObject(body, nil)
			if err != nil {
				t.Fatalf("the built event does not pass the validator: %v", err)
			}
			built := object.Body
			begin := strings.Index(built, "BEGIN:VTIMEZONE")
			end := strings.Index(built, "END:VTIMEZONE\r\n")
			if begin < 0 || end < begin {
				t.Fatalf("built event defines no zone:\n%s", built)
			}
			hostOnly := built[:begin] + built[end+len("END:VTIMEZONE\r\n"):]
			assertSameStarts(t, seriesStarts(t, clientReading(built)), seriesStarts(t, hostOnly))
		})
	}
}

// clientReading is body as a client that reads its VTIMEZONEs but not the
// server's marker on them sees it: every definition is the one it reads its
// times by.
func clientReading(body string) string {
	lines := strings.SplitAfter(body, "\n")
	kept := lines[:0]
	for _, line := range lines {
		if !ical.HasPrefixFold(line, ical.HostDefinitionProperty+":") {
			kept = append(kept, line)
		}
	}
	return strings.Join(kept, "")
}

// seriesStarts is where dav.EventInstances, the expander the UI places
// instances with, starts every instance of the recurring VEVENT of body from
// 2026 through CALDAV:max-date-time. One expansion lists at most
// ical.MaxRecurrenceInstances, so the span is read a year at a time.
func seriesStarts(t *testing.T, body string) []time.Time {
	t.Helper()
	_, maxDate := ical.DateLimits()
	var starts []time.Time
	for from := time.Date(2026, time.January, 1, 0, 0, 0, 0, time.UTC); from.Before(maxDate); {
		through := from.AddDate(1, 0, 0)
		if through.After(maxDate) {
			through = maxDate
		}
		expansion := dav.EventInstances(body, from, through)
		if !expansion.Recurs || !expansion.Complete {
			t.Fatalf("expansion from %s to %s: recurs = %t, complete = %t", from, through, expansion.Recurs, expansion.Complete)
		}
		for _, instance := range expansion.Instances {
			if !instance.Start.Before(from) {
				starts = append(starts, instance.Start)
			}
		}
		from = through
	}
	if len(starts) == 0 || maxDate.Sub(starts[len(starts)-1]) > 7*24*time.Hour {
		t.Fatalf("expansion reached %d instances, want the weekly series through max-date-time", len(starts))
	}
	return starts
}

func assertSameStarts(t *testing.T, got, want []time.Time) {
	t.Helper()
	for i := range min(len(got), len(want)) {
		if !got[i].Equal(want[i]) {
			t.Fatalf("instance %d placed at %s, want %s", i, got[i], want[i])
		}
	}
	if len(got) != len(want) {
		t.Fatalf("%d instances, want %d", len(got), len(want))
	}
}
