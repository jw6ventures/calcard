package ui

import (
	"strings"
	"testing"

	"github.com/jw6ventures/calcard/internal/dav"
	"github.com/jw6ventures/calcard/internal/ui/utils"
)

// An edit the editor builds from its fields does not write EXDATE itself
// (withUneditedProperties carries it forward from the stored master
// unchanged, restating it only when the series moves), so a stored EXDATE
// naming UTC, or an untrimmed TZID, reaches the validator exactly as stored
// whenever the edit does not move the series. The write path must still
// rewrite it: an edit of one field must not leave the rest of the object as
// broken for DAVx5 as it read it.
func TestEditKeepingUneditedEXDATERewritesItsTZID(t *testing.T) {
	// DTSTART is already the bare "Z" form the editor itself writes for UTC
	// (EventZoneIn resolves UTC to the zero zone), so the edit does not read
	// as moving the series and the EXDATE below -- left in a form an
	// external client or an earlier release gave it -- is carried over unchanged by
	// withUneditedProperties; the write path is what has to fix it.
	previous := []string{
		"UID:series", "DTSTAMP:20260101T000000Z", "SUMMARY:Old title",
		"DTSTART:20260701T100000Z", "DTEND:20260701T110000Z",
		"RRULE:FREQ=WEEKLY;COUNT=4", "EXDATE;TZID=UTC:20260708T100000",
	}
	object := append([]string{"BEGIN:VCALENDAR", "VERSION:2.0", "BEGIN:VEVENT"},
		append(append([]string{}, previous...), "END:VEVENT", "END:VCALENDAR")...)

	form := &utils.EventOptions{Timezone: "UTC"}
	edited, err := utils.BuildEventComponent("series", "New title", "2026-07-01T10:00", "2026-07-01T11:00", false, "", "",
		&utils.RecurrenceOptions{Frequency: "WEEKLY", Count: 4}, "", form)
	if err != nil {
		t.Fatalf("BuildEventComponent() error = %v", err)
	}

	merged := withUneditedProperties(edited, previous, object, form, &utils.RecurrenceOptions{Frequency: "WEEKLY", Count: 4})
	var exdate string
	for _, line := range merged {
		if strings.HasPrefix(strings.ToUpper(line), "EXDATE") {
			exdate = line
		}
	}
	if exdate == "" {
		t.Fatalf("merged lines carry no EXDATE:\n%s", strings.Join(merged, "\n"))
	}
	if exdate != "EXDATE;TZID=UTC:20260708T100000" {
		t.Fatalf("withUneditedProperties changed the unedited EXDATE to %q; the case tests nothing", exdate)
	}

	raw := "BEGIN:VCALENDAR\r\nVERSION:2.0\r\nPRODID:-//Test//EN\r\nBEGIN:VEVENT\r\n" +
		strings.Join(merged, "\r\n") + "\r\nEND:VEVENT\r\nEND:VCALENDAR\r\n"
	stored, err := dav.ValidateCalendarObject(raw, nil)
	if err != nil {
		t.Fatalf("the validator refuses the merged edit: %v\n%s", err, raw)
	}
	if !strings.Contains(stored.Body, "EXDATE:20260708T100000Z") || strings.Contains(stored.Body, "TZID=UTC") {
		t.Errorf("stored body does not rewrite the carried-over EXDATE:\n%s", stored.Body)
	}
}

// An import reads each event in the uploaded file through
// utils.ParseICSFile, and stores what it builds through the same validator
// PUT uses, so a TZID DAVx5 misreads is rewritten on import exactly as it
// would be on a PUT of the same bytes.
func TestImportParsedEventRewritesTZIDsOnStorage(t *testing.T) {
	ics := "BEGIN:VCALENDAR\r\nVERSION:2.0\r\nPRODID:-//Test//EN\r\n" +
		"BEGIN:VEVENT\r\nUID:imported\r\nDTSTAMP:20260101T000000Z\r\n" +
		"DTSTART;TZID=utc:20260701T100000\r\nDTEND;TZID=UTC:20260701T110000\r\n" +
		"END:VEVENT\r\nEND:VCALENDAR\r\n"
	events, leftOut, err := utils.ParseICSFile(ics)
	if err != nil {
		t.Fatalf("ParseICSFile() error = %v", err)
	}
	if leftOut != 0 || len(events) != 1 {
		t.Fatalf("ParseICSFile() = %d events, %d left out, want 1 and 0", len(events), leftOut)
	}
	stored, err := dav.ValidateCalendarObject(events[0], nil)
	if err != nil {
		t.Fatalf("the validator refuses the imported event: %v\n%s", err, events[0])
	}
	want := "DTSTART:20260701T100000Z\r\nDTEND:20260701T110000Z"
	if !strings.Contains(stored.Body, want) {
		t.Errorf("stored body does not rewrite the imported TZID=utc:\n%s\nwant it to hold %q", stored.Body, want)
	}
}
