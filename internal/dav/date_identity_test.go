package dav

import (
	"strings"
	"testing"
	"time"

	"github.com/jw6ventures/calcard/internal/ical"
)

// zoneObject is a VCALENDAR carrying the VTIMEZONE for the host's zone name
// over [from, to), as a collection's CALDAV:calendar-timezone holds one.
func zoneObject(t *testing.T, name string, from, to time.Time) string {
	t.Helper()
	timezone, ok := ical.VTimezone(name, from, to)
	if !ok {
		t.Skipf("%s is not available", name)
	}
	return "BEGIN:VCALENDAR\r\nVERSION:2.0\r\nPRODID:-//CalCard//EN\r\n" + timezone + "END:VCALENDAR\r\n"
}

func assertZonedFreeBusy(t *testing.T, timezone, raw, start, end string, want []string) {
	t.Helper()
	code, body := zonedReport(t, timezone, raw, freeBusyQuery(start, end))
	if code != 200 {
		t.Fatalf("free-busy status = %d, body %s", code, body)
	}
	assertPublishedFreeBusy(t, parsedFreeBusyLines(t, body), want)
}

func dateSeries(lines ...string) []string {
	return componentLines("VEVENT", append([]string{"UID:instant", "DTSTAMP:20250101T000000Z"}, lines...)...)
}

// In Pacific/Apia 30 December 2011 is skipped, so its midnight is the
// instant of 31 December's, but a series starting on a DATE names its slots
// by their dates: an EXDATE, an override and a THISANDFUTURE override naming
// one of the two dates name that slot alone.
func TestDateSlotsAreExcludedAndOverriddenByTheirDates(t *testing.T) {
	timezone := apiaTimezone(t)
	master := []string{"SUMMARY:Series", "DTSTART;VALUE=DATE:20111228", "DTEND;VALUE=DATE:20111230", "RRULE:FREQ=DAILY;COUNT=6"}
	t.Run("EXDATE", func(t *testing.T) {
		raw := zeroLengthCalendar(dateSeries(append(master, "EXDATE;VALUE=DATE:20111230")...))
		assertZonedExpansion(t, timezone, raw, "20111220T000000Z", "20120110T000000Z",
			[]string{"RECURRENCE-ID;VALUE=DATE:20111231"}, []string{"RECURRENCE-ID;VALUE=DATE:20111230"})
	})
	t.Run("override", func(t *testing.T) {
		raw := zeroLengthCalendar(dateSeries(master...),
			dateSeries("RECURRENCE-ID;VALUE=DATE:20111230", "SUMMARY:Moved", "DTSTART;VALUE=DATE:20111230"))
		assertZonedExpansion(t, timezone, raw, "20111220T000000Z", "20120110T000000Z", []string{
			"SUMMARY:Moved\r\nDTSTART;VALUE=DATE:20111230",
			"SUMMARY:Series\r\nDTSTART;VALUE=DATE:20111231\r\nDTEND;VALUE=DATE:20120102\r\nRECURRENCE-ID;VALUE=DATE:20111231",
		}, nil)
	})
	t.Run("THISANDFUTURE override", func(t *testing.T) {
		raw := zeroLengthCalendar(dateSeries(master...),
			dateSeries("RECURRENCE-ID;RANGE=THISANDFUTURE;VALUE=DATE:20111231", "SUMMARY:Later",
				"DTSTART;VALUE=DATE:20111231", "DTEND;VALUE=DATE:20120101", "TRANSP:TRANSPARENT"))
		assertZonedExpansion(t, timezone, raw, "20111220T000000Z", "20120110T000000Z", []string{
			"SUMMARY:Series\r\nDTSTART;VALUE=DATE:20111230\r\nDTEND;VALUE=DATE:20120101\r\nRECURRENCE-ID;VALUE=DATE:20111230",
			"SUMMARY:Later\r\nDTSTART;VALUE=DATE:20111231\r\nDTEND;VALUE=DATE:20120101",
		}, nil)
		// The instance of the 30th spans its series' two days, the 31st
		// among them, from the midnight both dates share, and is opaque as
		// its series is.
		assertZonedFreeBusy(t, timezone, raw, "20111220T000000Z", "20120110T000000Z",
			[]string{"FREEBUSY:20111228T100000Z/20111231T100000Z"})
		assertZonedQuery(t, timezone, raw, "20111231T000000Z", "20111231T010000Z", true)
	})
	t.Run("THISANDFUTURE override of a one-day series", func(t *testing.T) {
		// The instance of the 31st, which the override describes, is
		// transparent, though it begins at the instant the 30th names.
		raw := zeroLengthCalendar(dateSeries("SUMMARY:Series", "DTSTART;VALUE=DATE:20111228", "RRULE:FREQ=DAILY;COUNT=6"),
			dateSeries("RECURRENCE-ID;RANGE=THISANDFUTURE;VALUE=DATE:20111231", "SUMMARY:Later",
				"DTSTART;VALUE=DATE:20111231", "TRANSP:TRANSPARENT"))
		assertZonedFreeBusy(t, timezone, raw, "20111220T000000Z", "20120110T000000Z",
			[]string{"FREEBUSY:20111228T100000Z/20111230T100000Z"})
	})
	t.Run("THISANDFUTURE override of an excluded date", func(t *testing.T) {
		// The 30th is excluded, so the override naming it names no slot of
		// the set and stands for itself, though the 31st, which the set
		// generates, begins at the same instant.
		raw := zeroLengthCalendar(dateSeries(append(master, "EXDATE;VALUE=DATE:20111230")...),
			dateSeries("RECURRENCE-ID;RANGE=THISANDFUTURE;VALUE=DATE:20111230", "SUMMARY:Later", "DTSTART;VALUE=DATE:20111229"))
		assertZonedExpansion(t, timezone, raw, "20111220T000000Z", "20120110T000000Z", []string{
			"RECURRENCE-ID;VALUE=DATE:20111230\r\nSUMMARY:Later\r\nDTSTART;VALUE=DATE:20111229\r\nDTEND;VALUE=DATE:20111231\r\nEND:VEVENT",
			"SUMMARY:Later\r\nDTSTART;VALUE=DATE:20111230\r\nDTEND;VALUE=DATE:20120101\r\nRECURRENCE-ID;VALUE=DATE:20111231",
		}, nil)
	})
}

// A THISANDFUTURE override starting on a DATE moves the instances it governs
// by the dates between its RECURRENCE-ID and its DTSTART as written. In
// Pacific/Kiritimati, which skipped 31 December 1994, those two dates share a
// midnight, yet the override still moves each instance a date on.
func TestThisAndFutureDateOverrideMovesByTheDatesItIsWrittenWith(t *testing.T) {
	timezone := zoneObject(t, "Pacific/Kiritimati", time.Date(1994, 11, 1, 0, 0, 0, 0, time.UTC), time.Date(1995, 2, 1, 0, 0, 0, 0, time.UTC))
	raw := zeroLengthCalendar(dateSeries("DTSTART;VALUE=DATE:19941228", "RRULE:FREQ=DAILY;COUNT=6"),
		dateSeries("RECURRENCE-ID;RANGE=THISANDFUTURE;VALUE=DATE:19941231", "DTSTART;VALUE=DATE:19950101", "SUMMARY:Later"))
	assertZonedExpansion(t, timezone, raw, "19941220T000000Z", "19950110T000000Z", []string{
		"DTSTART;VALUE=DATE:19941230\r\nRECURRENCE-ID;VALUE=DATE:19941230",
		"DTSTART;VALUE=DATE:19950101\r\nSUMMARY:Later\r\nRECURRENCE-ID;VALUE=DATE:19941231",
		"DTSTART;VALUE=DATE:19950103\r\nSUMMARY:Later\r\nRECURRENCE-ID;VALUE=DATE:19950102",
	}, []string{"DTSTART;VALUE=DATE:19941231"})
	// 28 to 30 December, and 1 to 3 January, Kiritimati's first days at
	// UTC+14, which start where the 30th ends.
	assertZonedFreeBusy(t, timezone, raw, "19941220T000000Z", "19950110T000000Z",
		[]string{"FREEBUSY:19941228T100000Z/19950103T100000Z"})
	// Noon on 2 January, which the slot of the 1st, moved a date on, spans.
	assertZonedQuery(t, timezone, raw, "19950101T220000Z", "19950101T230000Z", true)
}

// An instance a THISANDFUTURE override moves back by dates can land further
// back than the hours between the override's own dates measure: moved back a
// date across the night the clocks go back, it starts 25 hours before its
// slot. A range holding only its first hour still finds it.
func TestThisAndFutureDateOverrideMovedBackAcrossALongNightIsFound(t *testing.T) {
	chicago := chicagoVTimezoneObject()
	raw := zeroLengthCalendar(dateSeries("DTSTART;VALUE=DATE:20261020", "RRULE:FREQ=DAILY;COUNT=20"),
		dateSeries("RECURRENCE-ID;RANGE=THISANDFUTURE;VALUE=DATE:20261025", "DTSTART;VALUE=DATE:20261024", "SUMMARY:Earlier"))
	// The first half hour of Sunday 1 November, CDT, which only the instance
	// of the 2nd, moved back to the 1st, occupies.
	assertZonedQuery(t, chicago, raw, "20261101T050000Z", "20261101T053000Z", true)
	assertZonedExpansion(t, chicago, raw, "20261101T050000Z", "20261101T053000Z",
		[]string{"DTSTART;VALUE=DATE:20261101\r\nSUMMARY:Earlier\r\nRECURRENCE-ID;VALUE=DATE:20261102"}, nil)
}

// A DATE-TIME or PERIOD RDATE of a series starting on a DATE adds the
// instance on the date its start falls on in the zone the series' dates
// resolve in, spanning the whole days its period reaches into.
func TestDateTimeRDateOfADateSeriesFallsOnItsLocalDate(t *testing.T) {
	tokyo := zoneObject(t, "Asia/Tokyo", time.Date(2026, 2, 1, 0, 0, 0, 0, time.UTC), time.Date(2026, 4, 1, 0, 0, 0, 0, time.UTC))
	for _, tt := range []struct {
		name, rdate string
		want        []string
	}{
		{
			name: "DATE-TIME", rdate: "RDATE:20260310T200000Z",
			want: []string{"DTSTART;VALUE=DATE:20260311\r\nRECURRENCE-ID;VALUE=DATE:20260311"},
		},
		{
			name: "PERIOD", rdate: "RDATE;VALUE=PERIOD:20260310T200000Z/PT30H",
			want: []string{"DTSTART;VALUE=DATE:20260311\r\nDTEND;VALUE=DATE:20260313\r\nRECURRENCE-ID;VALUE=DATE:20260311"},
		},
		{
			// The instance the rule generates on Monday the 9th is the one
			// the PERIOD names, and spans its days.
			name: "PERIOD on a date the rule generates", rdate: "RDATE;VALUE=PERIOD:20260308T200000Z/PT30H",
			want: []string{"DTSTART;VALUE=DATE:20260309\r\nDTEND;VALUE=DATE:20260311\r\nRECURRENCE-ID;VALUE=DATE:20260309"},
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			raw := zeroLengthCalendar(dateSeries("DTSTART;VALUE=DATE:20260302", "RRULE:FREQ=WEEKLY;COUNT=2", tt.rdate))
			assertZonedExpansion(t, tokyo, raw, "20260301T000000Z", "20260320T000000Z", tt.want,
				[]string{"VALUE=DATE:20260310"})
		})
	}
}

// A THISANDFUTURE override moving a timed series onto dates moves each
// instance by the date its slot falls on in the series' own zone, an RDATE
// written in another form among them. 23:00 UTC on 20 March is 08:00 on 21
// March in Tokyo, for a series zoned there, read in a New York collection,
// and for one floating in a Tokyo collection; it stays 20 March for a series
// in UTC, read in Tokyo.
func TestThisAndFutureDateOverrideMovesAnRDateByItsDateInTheSeriesZone(t *testing.T) {
	tokyo, ok := ical.VTimezone("Asia/Tokyo", time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC), time.Date(2026, 12, 1, 0, 0, 0, 0, time.UTC))
	if !ok {
		t.Skip("Asia/Tokyo is not available")
	}
	inTokyo := "BEGIN:VCALENDAR\r\nVERSION:2.0\r\nPRODID:-//CalCard//EN\r\n" + tokyo + "END:VCALENDAR\r\n"
	inNewYork := zoneObject(t, "America/New_York", time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC), time.Date(2026, 12, 1, 0, 0, 0, 0, time.UTC))
	for _, tt := range []struct {
		name, collection, start, end, recurrenceID string
		landsOn, notOn                             string
	}{
		{"zoned", inNewYork, "DTSTART;TZID=Asia/Tokyo:20260309T080000", "DTEND;TZID=Asia/Tokyo:20260309T090000",
			"RECURRENCE-ID;RANGE=THISANDFUTURE;TZID=Asia/Tokyo:20260310T080000", "20260321", "20260320"},
		{"floating", inTokyo, "DTSTART:20260309T080000", "DTEND:20260309T090000",
			"RECURRENCE-ID;RANGE=THISANDFUTURE:20260310T080000", "20260321", "20260320"},
		{"UTC", inTokyo, "DTSTART:20260308T230000Z", "DTEND:20260309T000000Z",
			"RECURRENCE-ID;RANGE=THISANDFUTURE:20260309T230000Z", "20260321", "20260322"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			parts := append([]string{"VERSION:2.0", "PRODID:-//CalCard//EN"}, strings.Split(strings.TrimRight(tokyo, "\r\n"), "\r\n")...)
			parts = append(parts, dateSeries(tt.start, tt.end, "RRULE:FREQ=DAILY;COUNT=3", "RDATE:20260320T230000Z")...)
			parts = append(parts, dateSeries(tt.recurrenceID, "DTSTART;VALUE=DATE:20260310", "SUMMARY:Later")...)
			assertZonedExpansion(t, tt.collection, wrapCalendar(parts...), "20260301T000000Z", "20260401T000000Z",
				[]string{"DTSTART;VALUE=DATE:" + tt.landsOn + "\r\nSUMMARY:Later\r\nRECURRENCE-ID"}, []string{"DTSTART;VALUE=DATE:" + tt.notOn})
		})
	}
}

// A DATE written on the day its zone skips keeps that date when expansion
// writes it out, rather than the date its midnight resolves onto.
func TestExpandedDateOnADaySkippedKeepsItsDate(t *testing.T) {
	timezone := apiaTimezone(t)
	raw := zeroLengthCalendar(dateSeries("DTSTART;VALUE=DATE:20111230", "SUMMARY:Skipped"))
	assertZonedExpansion(t, timezone, raw, "20111220T000000Z", "20120110T000000Z",
		[]string{"DTSTART;VALUE=DATE:20111230\r\nSUMMARY:Skipped"}, []string{"20111231"})
}

// A series starting on the day its zone skips spans the days its DTEND
// counts, as every instance does, though its first spans no time.
func TestDateSeriesStartingOnADaySkippedSpansItsDays(t *testing.T) {
	timezone := apiaTimezone(t)
	raw := zeroLengthCalendar(dateSeries("DTSTART;VALUE=DATE:20111230", "DTEND;VALUE=DATE:20111231", "RRULE:FREQ=DAILY;COUNT=3"))
	assertZonedFreeBusy(t, timezone, raw, "20111220T000000Z", "20120110T000000Z",
		[]string{"FREEBUSY:20111230T100000Z/20120101T100000Z"})
}

// A DATE with no end lasts its day, which in Chicago is 23 hours on the day
// the clocks go forward.
func TestDateEventLastsTheHoursOfItsDay(t *testing.T) {
	raw := zeroLengthCalendar(dateSeries("DTSTART;VALUE=DATE:20260308"))
	assertZonedFreeBusy(t, chicagoVTimezoneObject(), raw, "20260301T000000Z", "20260320T000000Z",
		[]string{"FREEBUSY:20260308T060000Z/20260309T050000Z"})
}

// America/Santiago skips its midnight when the clocks go forward, so that
// day starts at 01:00. An all-day series spans it from there, and every
// reader places it there.
func TestDateSeriesSpansADayStartingPastASkippedMidnight(t *testing.T) {
	timezone := zoneObject(t, "America/Santiago", time.Date(2026, 8, 1, 0, 0, 0, 0, time.UTC), time.Date(2026, 10, 1, 0, 0, 0, 0, time.UTC))
	raw := zeroLengthCalendar(dateSeries("DTSTART;VALUE=DATE:20260904", "RRULE:FREQ=DAILY;COUNT=3"))
	assertZonedFreeBusy(t, timezone, raw, "20260901T000000Z", "20260910T000000Z",
		[]string{"FREEBUSY:20260904T040000Z/20260907T030000Z"})
	// The last minute of Saturday 5 September and the first of Sunday the 6th.
	assertZonedQuery(t, timezone, raw, "20260906T035900Z", "20260906T040000Z", true)
	assertZonedQuery(t, timezone, raw, "20260906T040000Z", "20260906T040100Z", true)
	assertZonedQuery(t, timezone, raw, "20260907T030000Z", "20260907T030100Z", false)
	assertZonedExpansion(t, timezone, raw, "20260906T040000Z", "20260906T040100Z",
		[]string{"DTSTART;VALUE=DATE:20260906"}, []string{"DTSTART;VALUE=DATE:20260905"})
}

// assertLimitedOverrides checks that CALDAV:limit-recurrence-set over
// [start, end), read in timezone, keeps the overrides of raw with the
// SUMMARY values kept and drops those with the values dropped.
func assertLimitedOverrides(t *testing.T, timezone, raw, start, end string, kept, dropped []string) {
	t.Helper()
	code, body := zonedReport(t, timezone, raw, calendarDataQuery(`<C:limit-recurrence-set start="`+start+`" end="`+end+`"/>`))
	if code != 207 {
		t.Fatalf("limit-recurrence-set status = %d, body %s", code, body)
	}
	for _, summary := range kept {
		if !strings.Contains(body, "SUMMARY:"+summary) {
			t.Errorf("limit-recurrence-set drops the override %s:\n%s", summary, body)
		}
	}
	for _, summary := range dropped {
		if strings.Contains(body, "SUMMARY:"+summary) {
			t.Errorf("limit-recurrence-set keeps the override %s:\n%s", summary, body)
		}
	}
}

// CALDAV:limit-recurrence-set keeps an override whose slot reaches the range,
// and an all-day slot spans its whole day: the Sunday the clocks go back in
// Chicago holds 25 hours, the last of them past 24 from its midnight.
func TestLimitRecurrenceSetKeepsAnOverrideOfAnAllDaySlotReachingTheRange(t *testing.T) {
	raw := zeroLengthCalendar(dateSeries("DTSTART;VALUE=DATE:20261025", "RRULE:FREQ=WEEKLY;COUNT=4"),
		dateSeries("RECURRENCE-ID;VALUE=DATE:20261101", "DTSTART;VALUE=DATE:20261110", "SUMMARY:Moved"))
	assertLimitedOverrides(t, chicagoVTimezoneObject(), raw, "20261102T053000Z", "20261102T060000Z", []string{"Moved"}, nil)
	assertLimitedOverrides(t, chicagoVTimezoneObject(), raw, "20261102T060000Z", "20261102T063000Z", nil, []string{"Moved"})
}

// CALDAV:limit-recurrence-set keeps a THISANDFUTURE override for the instances
// it governs, which in a series starting on a DATE it names by date: in
// Pacific/Apia the override naming 30 December 2011 governs that slot alone,
// though 31 December's midnight, which the next override names, is its own.
func TestLimitRecurrenceSetKeepsThisAndFutureOverridesForTheDatesTheyGovern(t *testing.T) {
	timezone := apiaTimezone(t)
	raw := zeroLengthCalendar(dateSeries("DTSTART;VALUE=DATE:20111228", "RRULE:FREQ=DAILY;COUNT=6"),
		dateSeries("RECURRENCE-ID;RANGE=THISANDFUTURE;VALUE=DATE:20111230", "DTSTART;VALUE=DATE:20111230", "SUMMARY:Skipped"),
		dateSeries("RECURRENCE-ID;RANGE=THISANDFUTURE;VALUE=DATE:20111231", "DTSTART;VALUE=DATE:20111231", "SUMMARY:Later"))
	// Noon on 2 January 2012, at UTC+14.
	assertLimitedOverrides(t, timezone, raw, "20120101T220000Z", "20120101T230000Z", []string{"Later"}, []string{"Skipped"})
	// Noon on 31 December, whose slot begins at the instant the 30th's does.
	assertLimitedOverrides(t, timezone, raw, "20111230T220000Z", "20111230T230000Z", []string{"Later"}, []string{"Skipped"})
}

// A DATE-TIME RECURRENCE-ID on an override of a series starting on a DATE,
// a shape the validator refuses but a stored object can hold, names the slot
// at the instant it resolves to, whatever its override starts on, and every
// reader matches it so. In New York the 29 December slot begins
// at 05:00 UTC: one naming that instant replaces it, and one naming midnight
// UTC names no slot and stands beside it.
func TestDateTimeRecurrenceIDOfADateSeriesNamesTheSlotAtItsInstant(t *testing.T) {
	newYork := zoneObject(t, "America/New_York", time.Date(2011, 11, 1, 0, 0, 0, 0, time.UTC), time.Date(2012, 2, 1, 0, 0, 0, 0, time.UTC))
	master := dateSeries("SUMMARY:Series", "DTSTART;VALUE=DATE:20111228", "RRULE:FREQ=DAILY;COUNT=3")
	for _, tt := range []struct {
		name, recurrenceID string
		replaced           bool
	}{
		{"the slot's instant", "20111229T050000Z", true},
		{"another instant", "20111229T000000Z", false},
	} {
		t.Run(tt.name, func(t *testing.T) {
			raw := zeroLengthCalendar(master, dateSeries("RECURRENCE-ID:"+tt.recurrenceID, "SUMMARY:Moved", "DTSTART;VALUE=DATE:20120105"))
			slot := []string{"SUMMARY:Series\r\nDTSTART;VALUE=DATE:20111229"}
			var wanted, unwanted []string
			if tt.replaced {
				unwanted = slot
			} else {
				wanted = slot
			}
			assertZonedExpansion(t, newYork, raw, "20111220T000000Z", "20120110T000000Z",
				append(wanted, "SUMMARY:Moved\r\nDTSTART;VALUE=DATE:20120105"), unwanted)
			// Noon on 29 December in New York.
			assertZonedQuery(t, newYork, raw, "20111229T170000Z", "20111229T180000Z", !tt.replaced)
			busy := []string{"FREEBUSY:20111228T050000Z/20111231T050000Z", "FREEBUSY:20120105T050000Z/20120106T050000Z"}
			if tt.replaced {
				busy = []string{"FREEBUSY:20111228T050000Z/20111229T050000Z", "FREEBUSY:20111230T050000Z/20111231T050000Z", "FREEBUSY:20120105T050000Z/20120106T050000Z"}
			}
			assertZonedFreeBusy(t, newYork, raw, "20111220T000000Z", "20120110T000000Z", busy)
		})
	}
}
