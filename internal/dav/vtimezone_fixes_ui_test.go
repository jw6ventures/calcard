package dav

import (
	"strings"
	"testing"

	"github.com/jw6ventures/calcard/internal/ui/utils"
)

// The UI and the REST API store an event through the validator, which writes
// the VTIMEZONE for its zone, and a client reads the object by it rather than
// by the host. A weekly series
// with no end is read out to CALDAV:max-date-time, so the definition has to
// place its instances in every later year as the host's zone does, not only in
// the year the series starts.
func TestBuiltEventDefinesItsZoneForEveryInstanceOfTheSeries(t *testing.T) {
	for _, tzid := range []string{"Europe/Berlin", "America/New_York", "Australia/Sydney"} {
		t.Run(tzid, func(t *testing.T) {
			requireHostZones(t, tzid)
			body, err := utils.BuildEvent("ui-series", "Standup", "2026-01-05T09:00", "2026-01-05T10:00", false, "", "",
				&utils.RecurrenceOptions{Frequency: "WEEKLY"}, &utils.EventOptions{Timezone: tzid})
			if err != nil {
				t.Fatalf("BuildEvent() error = %v", err)
			}
			object, err := ValidateCalendarObject(body, nil)
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
