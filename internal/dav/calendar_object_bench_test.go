package dav

import (
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/jw6ventures/calcard/internal/ical"
	"github.com/jw6ventures/calcard/internal/store"
)

// benchCalendarObject wraps components in the VCALENDAR every write carries.
func benchCalendarObject(components ...string) string {
	return "BEGIN:VCALENDAR\r\nVERSION:2.0\r\nPRODID:-//CalCard//Bench//EN\r\n" +
		strings.Join(components, "") + "END:VCALENDAR\r\n"
}

// benchVEvent builds one VEVENT carrying attendees distinct ATTENDEE lines and
// any extra property lines, an RRULE among them when the caller passes one.
func benchVEvent(attendees int, lines ...string) string {
	var b strings.Builder
	b.WriteString("BEGIN:VEVENT\r\nUID:bench\r\nDTSTAMP:20260301T000000Z\r\nSUMMARY:Planning\r\n")
	for _, line := range lines {
		b.WriteString(line)
		b.WriteString("\r\n")
	}
	for i := range attendees {
		fmt.Fprintf(&b, "ATTENDEE;CN=User %d;PARTSTAT=NEEDS-ACTION:mailto:user%d@example.com\r\n", i, i)
	}
	b.WriteString("END:VEVENT\r\n")
	return b.String()
}

// benchAttendeeObject spreads attendees over a weekly master and as many
// overridden instances as it takes to keep each one within
// CALDAV:max-attendees-per-instance, which is counted per component.
func benchAttendeeObject(attendees int) string {
	master := benchVEvent(min(attendees, ical.MaxAttendeesPerInstance),
		"DTSTART:20260320T100000Z", "DTEND:20260320T110000Z", "RRULE:FREQ=WEEKLY;COUNT=52")
	components := []string{master}
	start := time.Date(2026, 3, 20, 10, 0, 0, 0, time.UTC)
	for remaining, week := attendees-ical.MaxAttendeesPerInstance, 1; remaining > 0; week++ {
		instance := start.AddDate(0, 0, 7*week).Format("20060102T150405Z")
		components = append(components, benchVEvent(min(remaining, ical.MaxAttendeesPerInstance),
			"RECURRENCE-ID:"+instance, "DTSTART:"+instance, "DURATION:PT1H"))
		remaining -= ical.MaxAttendeesPerInstance
	}
	return benchCalendarObject(components...)
}

// benchZonedExdateObject names an IANA zone with no VTIMEZONE to define it, so
// every zoned value resolves through the host's time-zone database.
func benchZonedExdateObject(exdates int) string {
	const zone = "America/New_York"
	location, err := time.LoadLocation(zone)
	if err != nil {
		panic(err)
	}
	start := time.Date(2026, 3, 20, 10, 0, 0, 0, location)
	lines := []string{
		"DTSTART;TZID=" + zone + ":" + start.Format("20060102T150405"),
		"DTEND;TZID=" + zone + ":" + start.Add(time.Hour).Format("20060102T150405"),
		fmt.Sprintf("RRULE:FREQ=DAILY;COUNT=%d", ical.MaxRecurrenceInstances),
	}
	for i := range exdates {
		lines = append(lines, "EXDATE;TZID="+zone+":"+start.AddDate(0, 0, 2*i+1).Format("20060102T150405"))
	}
	return benchCalendarObject(benchVEvent(1, lines...))
}

// BenchmarkValidateCalendarObjectForStorage measures the validator every
// submitted calendar object passes through -- DAV PUT, the REST API and UI
// imports -- across attendee scale, a recurrence set at the instance cap, and
// zoned exceptions resolved without a VTIMEZONE.
func BenchmarkValidateCalendarObjectForStorage(b *testing.B) {
	cal := &store.CalendarAccess{Calendar: store.Calendar{ID: 1, UserID: 1, Name: "Bench"}, Editor: true}
	cases := []struct {
		name string
		body string
	}{
		{name: "attendees=10", body: benchAttendeeObject(10)},
		{name: "attendees=100", body: benchAttendeeObject(100)},
		{name: "attendees=1000_over_10_instances", body: benchAttendeeObject(1000)},
		{name: "daily_recurrence_at_instance_cap", body: benchCalendarObject(benchVEvent(1,
			"DTSTART:20260320T100000Z", "DTEND:20260320T110000Z",
			fmt.Sprintf("RRULE:FREQ=DAILY;COUNT=%d", ical.MaxRecurrenceInstances)))},
		{name: "iana_tzid_without_vtimezone_200_exdates", body: benchZonedExdateObject(200)},
	}
	for _, tc := range cases {
		if _, fault := validateCalendarObjectForStorage(tc.body, cal); fault != nil {
			b.Fatalf("%s: validator refused the object: %d %v", tc.name, fault.status, fault.conditions)
		}
		b.Run(tc.name, func(b *testing.B) {
			b.ReportAllocs()
			for b.Loop() {
				if _, fault := validateCalendarObjectForStorage(tc.body, cal); fault != nil {
					b.Fatalf("validator refused the object: %d %v", fault.status, fault.conditions)
				}
			}
		})
	}
}
