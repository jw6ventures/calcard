package dav

import (
	"fmt"
	"testing"
	"time"

	"github.com/jw6ventures/calcard/internal/store"
)

// benchTimeRangeEvents builds n recurring resources, the shape whose §9.9 test
// costs the most: each one has to be expanded into instances before the table
// can judge any of them.
func benchTimeRangeEvents(n int) []store.Event {
	events := make([]store.Event, 0, n)
	for i := 0; i < n; i++ {
		uid := fmt.Sprintf("event-%d", i)
		events = append(events, store.Event{
			ID:           int64(i + 1),
			UID:          uid,
			ResourceName: uid,
			RawICAL: wrapCalendar(append(
				[]string{
					"BEGIN:VEVENT",
					"UID:" + uid,
					fmt.Sprintf("DTSTART:202401%02dT100000Z", (i%28)+1),
					fmt.Sprintf("DTEND:202401%02dT110000Z", (i%28)+1),
					"SUMMARY:weekly sync",
					"DESCRIPTION:a description long enough to be worth unfolding",
					"RRULE:FREQ=WEEKLY;COUNT=40",
					"EXDATE:20240205T100000Z",
				},
				append(componentLines("VALARM", "ACTION:DISPLAY", "DESCRIPTION:x", "TRIGGER:-PT15M"), "END:VEVENT")...,
			)...),
		})
	}
	return events
}

// BenchmarkCalendarQueryTimeRange measures the in-memory §9.9 pass, which reads
// the resource octets. Recurrence expansion is the dominant cost, so this is the
// gate on how often the payload gets parsed per candidate.
func BenchmarkCalendarQueryTimeRange(b *testing.B) {
	events := benchTimeRangeEvents(200)
	filter := &calFilter{CompFilter: compFilter{
		Name: "VCALENDAR",
		CompFilter: []compFilter{{
			Name:      "VEVENT",
			TimeRange: &timeRange{Start: "20240601T000000Z", End: "20240701T000000Z"},
		}},
	}}

	b.ReportAllocs()
	for b.Loop() {
		applyCalendarFilter(events, filter, floatingZone{})
	}
}

// The same corpus judged through a nested VALARM filter, which expands the
// enclosing event's recurrence set once per candidate and widens the scan by the
// alarm's lead first.
func BenchmarkCalendarQueryTimeRangeNestedAlarm(b *testing.B) {
	events := benchTimeRangeEvents(200)
	filter := &calFilter{CompFilter: compFilter{
		Name: "VCALENDAR",
		CompFilter: []compFilter{{
			Name: "VEVENT",
			CompFilter: []compFilter{{
				Name:      "VALARM",
				TimeRange: &timeRange{Start: "20240601T000000Z", End: "20240701T000000Z"},
			}},
		}},
	}}

	b.ReportAllocs()
	for b.Loop() {
		applyCalendarFilter(events, filter, floatingZone{})
	}
}

// BenchmarkFreeBusyPeriods covers the other expansion caller, which derives the
// published periods from the same parse.
func BenchmarkFreeBusyPeriods(b *testing.B) {
	events := benchTimeRangeEvents(200)
	rangeStart := time.Date(2024, 6, 1, 0, 0, 0, 0, time.UTC)
	rangeEnd := time.Date(2024, 7, 1, 0, 0, 0, 0, time.UTC)

	b.ReportAllocs()
	for b.Loop() {
		for _, candidate := range freeBusyCandidates(events, floatingZone{}) {
			freeBusyIntervals(candidate, rangeStart, rangeEnd, true)
		}
	}
}
