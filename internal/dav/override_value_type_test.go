package dav

import (
	"testing"
	"time"

	"github.com/jw6ventures/calcard/internal/calobject"
)

// The value-type mismatches an override of a series can carry, each writing
// neither DTEND nor DURATION: an all-day override of a timed series, which
// occupies its day, and a timed override of an all-day series, which is an
// instant.
var (
	timedDailySeries = componentLines("VEVENT", "UID:instant", "DTSTAMP:20250101T000000Z",
		"DTSTART:20260105T090000Z", "DTEND:20260105T100000Z", "RRULE:FREQ=DAILY;COUNT=3")
	allDayDailySeries = componentLines("VEVENT", "UID:instant", "DTSTAMP:20250101T000000Z",
		"DTSTART;VALUE=DATE:20260105", "RRULE:FREQ=DAILY;COUNT=3")
)

func valueTypeOverride(properties ...string) []string {
	return componentLines("VEVENT", append([]string{"UID:instant", "DTSTAMP:20250101T000000Z"}, properties...)...)
}

// Free-busy publishes an override of another value type than its series for
// the time calendar-query places it in: its day from a DATE, nothing for the
// instant at a DATE-TIME.
func TestFreeBusyPublishesAnOverrideOfAnotherValueTypeAsItsOwnStartHasIt(t *testing.T) {
	for _, tt := range []struct {
		name     string
		raw      string
		wantBusy []string
	}{
		{
			name: "all-day override of a timed series",
			raw: zeroLengthCalendar(timedDailySeries,
				valueTypeOverride("RECURRENCE-ID:20260106T090000Z", "DTSTART;VALUE=DATE:20260106")),
			wantBusy: []string{"FREEBUSY:20260105T090000Z/20260105T100000Z",
				"FREEBUSY:20260106T000000Z/20260107T000000Z", "FREEBUSY:20260107T090000Z/20260107T100000Z"},
		},
		{
			name: "timed override of an all-day series",
			raw: zeroLengthCalendar(allDayDailySeries,
				valueTypeOverride("RECURRENCE-ID;VALUE=DATE:20260106", "DTSTART:20260106T120000Z")),
			wantBusy: []string{"FREEBUSY:20260105T000000Z/20260106T000000Z", "FREEBUSY:20260107T000000Z/20260108T000000Z"},
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			code, body := zeroLengthReport(t, tt.raw, freeBusyQuery("20260105T000000Z", "20260109T000000Z"))
			if code != 200 {
				t.Fatalf("free-busy status = %d, body %s", code, body)
			}
			assertPublishedFreeBusy(t, parsedFreeBusyLines(t, body), tt.wantBusy)
		})
	}
}

// A RANGE=THISANDFUTURE override of another value type than its series, with
// neither DTEND nor DURATION, gives the instances it governs the length its
// own DTSTART has: whole days from a DATE, instants at a DATE-TIME.
// Calendar-query, free-busy, expansion and the views' instances place them
// alike.
func TestThisAndFutureOverrideOfAnotherValueTypeGivesItsInstancesItsOwnLength(t *testing.T) {
	type governed struct {
		recurrenceID, start, end string
	}
	for _, tt := range []struct {
		name          string
		raw           string
		inRange       [2]string
		outOfRange    [2]string
		wantBusy      []string
		unwanted      []string
		wantInstances []governed
	}{
		{
			name: "all-day override of a timed series",
			raw: zeroLengthCalendar(timedDailySeries, valueTypeOverride(
				"RECURRENCE-ID;RANGE=THISANDFUTURE:20260106T090000Z", "DTSTART;VALUE=DATE:20260106", "SUMMARY:Later")),
			inRange:    [2]string{"20260107T120000Z", "20260107T130000Z"},
			outOfRange: [2]string{"20260108T000000Z", "20260108T080000Z"},
			wantBusy:   []string{"FREEBUSY:20260105T090000Z/20260105T100000Z", "FREEBUSY:20260106T000000Z/20260108T000000Z"},
			unwanted:   []string{"DTEND;VALUE=DATE:20260106", "DTEND;VALUE=DATE:20260107"},
			wantInstances: []governed{
				{"20260106T090000Z", "20260106T000000Z", "20260107T000000Z"},
				{"20260107T090000Z", "20260107T000000Z", "20260108T000000Z"},
			},
		},
		{
			name: "timed override of an all-day series",
			raw: zeroLengthCalendar(allDayDailySeries, valueTypeOverride(
				"RECURRENCE-ID;RANGE=THISANDFUTURE;VALUE=DATE:20260106", "DTSTART:20260106T120000Z", "SUMMARY:Later")),
			inRange:    [2]string{"20260107T120000Z", "20260107T123000Z"},
			outOfRange: [2]string{"20260107T130000Z", "20260107T140000Z"},
			wantBusy:   []string{"FREEBUSY:20260105T000000Z/20260106T000000Z"},
			unwanted:   []string{"DTEND"},
			wantInstances: []governed{
				{"20260106T000000Z", "20260106T120000Z", "20260106T120000Z"},
				{"20260107T000000Z", "20260107T120000Z", "20260107T120000Z"},
			},
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			assertOverrideQueried(t, tt.raw, tt.inRange[0], tt.inRange[1], true)
			assertOverrideQueried(t, tt.raw, tt.outOfRange[0], tt.outOfRange[1], false)

			code, body := zeroLengthReport(t, tt.raw, freeBusyQuery("20260105T000000Z", "20260109T000000Z"))
			if code != 200 {
				t.Fatalf("free-busy status = %d, body %s", code, body)
			}
			assertPublishedFreeBusy(t, parsedFreeBusyLines(t, body), tt.wantBusy)

			want := make([]calobject.Instance, 0, len(tt.wantInstances))
			for _, instance := range tt.wantInstances {
				want = append(want, calobject.Instance{RecurrenceID: mustUTC(t, instance.recurrenceID),
					Start: mustUTC(t, instance.start), End: mustUTC(t, instance.end)})
			}
			assertExpandedExtents(t, tt.raw, "20260106T000000Z", "20260108T000000Z", expandedExtents{unwanted: tt.unwanted, want: want})

			override := want[0].RecurrenceID
			got := EventInstances(tt.raw, mustUTC(t, "20260106T000000Z"), mustUTC(t, "20260108T000000Z"))
			if len(got.Instances) != len(want) {
				t.Fatalf("EventInstances() = %v, want %v", got.Instances, want)
			}
			for i, w := range want {
				g := got.Instances[i]
				if !g.RecurrenceID.Equal(w.RecurrenceID) || !g.Start.Equal(w.Start) || !g.End.Equal(w.End) || !g.Override.Equal(override) {
					t.Errorf("EventInstances() instance %d = %v, want %v governed by %s", i, g, w, override.Format(time.RFC3339))
				}
			}
		})
	}
}

// Free-busy publishes an all-day override of an all-day series for the whole
// day it takes, which the collection's zone gives its hours: 25 of them the
// day the clocks go back, 23 the day they go forward.
func TestFreeBusyPublishesADateOverrideForItsSeriesDaysInTheZone(t *testing.T) {
	chicago := chicagoVTimezoneObject()
	for _, tt := range []struct {
		name, day, rangeStart, rangeEnd, want string
	}{
		{"the day the clocks go back", "20261101", "20261101T050000Z", "20261103T000000Z", "FREEBUSY:20261101T050000Z/20261102T060000Z"},
		{"the day the clocks go forward", "20260308", "20260308T060000Z", "20260310T000000Z", "FREEBUSY:20260308T060000Z/20260309T050000Z"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			code, body := zonedReport(t, chicago, dateSeriesEndingOn(tt.day), freeBusyQuery(tt.rangeStart, tt.rangeEnd))
			if code != 200 {
				t.Fatalf("free-busy status = %d, body %s", code, body)
			}
			assertPublishedFreeBusy(t, parsedFreeBusyLines(t, body), []string{tt.want})
		})
	}
}

// An all-day RANGE=THISANDFUTURE override of an all-day series whose DURATION
// counts an hour gives the instances it governs the whole day that series
// lasts. Free-busy, expansion and the views' instances place them alike, and
// expansion writes no DURATION in hours beside their dates.
func TestThisAndFutureDateOverrideOfAnHourDurationSeriesLastsWholeDays(t *testing.T) {
	raw := zeroLengthCalendar(
		componentLines("VEVENT", "UID:instant", "DTSTAMP:20250101T000000Z",
			"DTSTART;VALUE=DATE:20260105", "DURATION:PT1H", "RRULE:FREQ=DAILY;COUNT=4"),
		valueTypeOverride("RECURRENCE-ID;RANGE=THISANDFUTURE;VALUE=DATE:20260106", "DTSTART;VALUE=DATE:20260106", "SUMMARY:Later"))

	code, body := zeroLengthReport(t, raw, freeBusyQuery("20260105T000000Z", "20260110T000000Z"))
	if code != 200 {
		t.Fatalf("free-busy status = %d, body %s", code, body)
	}
	assertPublishedFreeBusy(t, parsedFreeBusyLines(t, body), []string{"FREEBUSY:20260105T000000Z/20260109T000000Z"})

	var want []calobject.Instance
	for _, day := range []string{"20260106", "20260107", "20260108"} {
		start := mustUTC(t, day+"T000000Z")
		want = append(want, calobject.Instance{RecurrenceID: start, Start: start, End: start.AddDate(0, 0, 1)})
	}
	assertExpandedExtents(t, raw, "20260106T000000Z", "20260109T000000Z", expandedExtents{unwanted: []string{"DURATION:PT"}, want: want})
	got := EventInstances(raw, mustUTC(t, "20260106T000000Z"), mustUTC(t, "20260109T000000Z"))
	if len(got.Instances) != len(want) {
		t.Fatalf("EventInstances() = %v, want %v", got.Instances, want)
	}
	for i, w := range want {
		if g := got.Instances[i]; !g.Start.Equal(w.Start) || !g.End.Equal(w.End) {
			t.Errorf("EventInstances() instance %d = %v, want %v", i, g, w)
		}
	}
}
