package dav

import (
	"strings"
	"testing"
)

// zeroLengthEventSpellings are the ways a VEVENT can occupy no time: a
// DATE-TIME end equal to its start, or a zero DURATION, for a DATE-TIME and for
// a DATE start. A DATE end equal to a DATE start is the one-day event calendar
// applications export, not an instant.
var zeroLengthEventSpellings = []struct {
	name       string
	properties []string
}{
	{name: "DTEND equal to DTSTART", properties: []string{"DTSTART:20260107T100000Z", "DTEND:20260107T100000Z"}},
	{name: "DURATION PT0S", properties: []string{"DTSTART:20260107T100000Z", "DURATION:PT0S"}},
	{name: "DATE DTSTART with DURATION P0D", properties: []string{"DTSTART;VALUE=DATE:20260107", "DURATION:P0D"}},
}

// zeroLengthRanges place a range around an instant at 2026-01-07: the instant
// is midnight for the DATE spellings and 10:00 for the DATE-TIME ones, so each
// case names both and picks by the spelling.
var zeroLengthRanges = []struct {
	name                string
	dateTime, date      [2]string
	wantEvent, wantTodo bool
}{
	{name: "at range start", dateTime: [2]string{"20260107T100000Z", "20260107T110000Z"}, date: [2]string{"20260107T000000Z", "20260107T010000Z"}, wantEvent: true, wantTodo: true},
	{name: "inside range", dateTime: [2]string{"20260107T090000Z", "20260107T110000Z"}, date: [2]string{"20260106T230000Z", "20260107T010000Z"}, wantEvent: true, wantTodo: true},
	// §9.9's VTODO rows for DTSTART+DUE and DTSTART+DURATION both close the
	// range end (end >= DUE), so a task due at the instant a range ends is in it.
	{name: "at range end", dateTime: [2]string{"20260107T090000Z", "20260107T100000Z"}, date: [2]string{"20260106T230000Z", "20260107T000000Z"}, wantEvent: false, wantTodo: true},
	{name: "before range", dateTime: [2]string{"20260107T100001Z", "20260107T110000Z"}, date: [2]string{"20260107T000001Z", "20260107T010000Z"}, wantEvent: false, wantTodo: false},
}

func zeroLengthRange(properties []string, dateTime, date [2]string) (string, string) {
	if properties[0] == "DTSTART;VALUE=DATE:20260107" {
		return date[0], date[1]
	}
	return dateTime[0], dateTime[1]
}

// An event whose end equals its start is an instant, and §9.9 places it with
// the zero-duration row (start <= DTSTART AND end > DTSTART) however that zero
// length is written.
func TestZeroLengthEventMatchesTheSameRangesInEverySpelling(t *testing.T) {
	for _, spelling := range zeroLengthEventSpellings {
		for _, r := range zeroLengthRanges {
			t.Run(spelling.name+"/"+r.name, func(t *testing.T) {
				start, end := zeroLengthRange(spelling.properties, r.dateTime, r.date)
				raw := wrapCalendar(componentLines("VEVENT", append([]string{"UID:instant"}, spelling.properties...)...)...)
				if got := assertComponentInRange(t, raw, "VEVENT", start, end, floatingZone{}); got != r.wantEvent {
					t.Fatalf("VEVENT %v in [%s, %s) = %v, want %v", spelling.properties, start, end, got, r.wantEvent)
				}
			})
		}
	}
}

// zeroLengthSeries is the same instant as the third occurrence of a daily
// series starting two days earlier.
func zeroLengthSeries(uid string, properties []string) []string {
	series := []string{"UID:" + uid, "RRULE:FREQ=DAILY;COUNT=5"}
	for _, property := range properties {
		series = append(series, strings.ReplaceAll(property, "20260107", "20260105"))
	}
	return series
}

// The same instant generated as one occurrence of a series is matched exactly
// as the lone event is.
func TestZeroLengthRecurrenceInstanceMatchesTheSameRangesInEverySpelling(t *testing.T) {
	for _, spelling := range zeroLengthEventSpellings {
		properties := zeroLengthSeries("series", spelling.properties)
		for _, r := range zeroLengthRanges {
			t.Run(spelling.name+"/"+r.name, func(t *testing.T) {
				start, end := zeroLengthRange(spelling.properties, r.dateTime, r.date)
				raw := wrapCalendar(componentLines("VEVENT", properties...)...)
				if got := assertComponentInRange(t, raw, "VEVENT", start, end, floatingZone{}); got != r.wantEvent {
					t.Fatalf("VEVENT %v in [%s, %s) = %v, want %v", properties, start, end, got, r.wantEvent)
				}
			})
		}
	}
}

// A task due when it starts is placed by §9.9's DTSTART+DUE row, and the same
// task spelled with a zero DURATION by its DTSTART+DURATION row. With DUE equal
// to DTSTART both rows reduce to start <= DTSTART AND end >= DTSTART.
func TestZeroLengthTodoMatchesTheSameRangesInEverySpelling(t *testing.T) {
	spellings := []struct {
		name       string
		properties []string
	}{
		{name: "DUE equal to DTSTART", properties: []string{"DTSTART:20260107T100000Z", "DUE:20260107T100000Z"}},
		{name: "DURATION PT0S", properties: []string{"DTSTART:20260107T100000Z", "DURATION:PT0S"}},
		{name: "DATE DUE equal to DATE DTSTART", properties: []string{"DTSTART;VALUE=DATE:20260107", "DUE;VALUE=DATE:20260107"}},
		{name: "DATE DTSTART with DURATION P0D", properties: []string{"DTSTART;VALUE=DATE:20260107", "DURATION:P0D"}},
	}
	for _, spelling := range spellings {
		for _, r := range zeroLengthRanges {
			t.Run(spelling.name+"/"+r.name, func(t *testing.T) {
				start, end := zeroLengthRange(spelling.properties, r.dateTime, r.date)
				for _, properties := range [][]string{
					append([]string{"UID:task"}, spelling.properties...),
					zeroLengthSeries("tasks", spelling.properties),
				} {
					raw := wrapCalendar(componentLines("VTODO", properties...)...)
					if got := assertComponentInRange(t, raw, "VTODO", start, end, floatingZone{}); got != r.wantTodo {
						t.Fatalf("VTODO %v in [%s, %s) = %v, want %v", properties, start, end, got, r.wantTodo)
					}
				}
			})
		}
	}
}
