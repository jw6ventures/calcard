package dav

import (
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/jw6ventures/calcard/internal/calobject"
	"github.com/jw6ventures/calcard/internal/ical"
)

const instancesOutlookEastern = "BEGIN:VTIMEZONE\r\nTZID:Eastern Standard Time\r\n" +
	"BEGIN:STANDARD\r\nDTSTART:16010101T020000\r\nTZOFFSETFROM:-0400\r\nTZOFFSETTO:-0500\r\nRRULE:FREQ=YEARLY;INTERVAL=1;BYDAY=1SU;BYMONTH=11\r\nEND:STANDARD\r\n" +
	"BEGIN:DAYLIGHT\r\nDTSTART:16010101T020000\r\nTZOFFSETFROM:-0500\r\nTZOFFSETTO:-0400\r\nRRULE:FREQ=YEARLY;INTERVAL=1;BYDAY=2SU;BYMONTH=3\r\nEND:DAYLIGHT\r\n" +
	"END:VTIMEZONE\r\n"

// instancesObject wraps components, each a VEVENT's lines between BEGIN and
// END, in a VCALENDAR carrying timezones.
func instancesObject(timezones string, components ...string) string {
	var b strings.Builder
	b.WriteString("BEGIN:VCALENDAR\r\nVERSION:2.0\r\nPRODID:-//Test//EN\r\n" + timezones)
	for _, component := range components {
		b.WriteString("BEGIN:VEVENT\r\nUID:series\r\nDTSTAMP:20240101T000000Z\r\n" + component + "END:VEVENT\r\n")
	}
	b.WriteString("END:VCALENDAR\r\n")
	return b.String()
}

// expandedInstances is what a calendar-query's CALDAV:expand over [start, end)
// returns for raw, read back as instances, less the components whose
// RECURRENCE-ID is one of overridden: those are overrides, which stand for
// themselves rather than for an instance of the series' pattern.
func expandedInstances(t *testing.T, raw string, start, end time.Time, overridden ...string) []calobject.Instance {
	t.Helper()
	got, err := filterICalendarData(raw, newCalendarDataProjection(
		&calendarDataEl{Expand: &calendarRange{Start: start, End: end}}, floatingZone{}))
	if err != nil {
		t.Fatalf("expand: %v", err)
	}
	root, err := parseICalendarObject(got)
	if err != nil {
		t.Fatalf("parse expanded output: %v\n%s", err, got)
	}
	m := newCalendarTimeRangeMatcher(got, root, floatingZone{})
	var instances []calobject.Instance
	for _, child := range root.children {
		if child.name != "VEVENT" {
			continue
		}
		id, ok := m.dateValue(child, "RECURRENCE-ID", noShift)
		if !ok {
			t.Fatalf("an expanded instance carries no RECURRENCE-ID:\n%s", got)
		}
		skip := false
		for _, o := range overridden {
			skip = skip || id.instant.Equal(mustUTC(t, o))
		}
		if skip {
			continue
		}
		dtstart, _ := m.dateValue(child, "DTSTART", noShift)
		instances = append(instances, calobject.Instance{
			RecurrenceID: id.instant,
			Start:        dtstart.instant,
			End:          m.eventOccurrenceEnd(child, dtstart, noShift),
		})
	}
	slices.SortFunc(instances, func(a, b calobject.Instance) int { return a.Start.Compare(b.Start) })
	return instances
}

// The calendar views place a series' instances from EventInstances, so each
// rule shape the views show has to give CalDAV's own expansion of it: ordinal
// and set-position rules, a monthly rule on a day some months lack, RDATE and
// EXDATE, a series started years before the range, instances on a local time
// the zone skips, which COUNT does not count, and zones the object alone
// defines.
func TestEventInstancesAreTheInstancesCalDAVExpands(t *testing.T) {
	october := []time.Time{time.Date(2026, 10, 1, 0, 0, 0, 0, time.UTC), time.Date(2026, 11, 1, 0, 0, 0, 0, time.UTC)}
	tests := []struct {
		name       string
		raw        string
		start, end time.Time
		want       int
		overridden []string
	}{
		{
			name:  "second Monday of each month",
			raw:   instancesObject("", "DTSTART:20260112T090000Z\r\nDTEND:20260112T100000Z\r\nRRULE:FREQ=MONTHLY;BYDAY=2MO\r\n"),
			start: time.Date(2026, 3, 1, 0, 0, 0, 0, time.UTC), end: time.Date(2026, 10, 1, 0, 0, 0, 0, time.UTC),
			want: 7,
		},
		{
			name:  "last weekday of each month",
			raw:   instancesObject("", "DTSTART:20260130T090000Z\r\nDTEND:20260130T100000Z\r\nRRULE:FREQ=MONTHLY;BYDAY=MO,TU,WE,TH,FR;BYSETPOS=-1\r\n"),
			start: october[0], end: october[1], want: 1,
		},
		{
			name:  "the 31st, in months that have one",
			raw:   instancesObject("", "DTSTART:20260131T090000Z\r\nDTEND:20260131T100000Z\r\nRRULE:FREQ=MONTHLY\r\n"),
			start: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC), end: time.Date(2027, 1, 1, 0, 0, 0, 0, time.UTC),
			want: 7,
		},
		{
			name: "RDATE and EXDATE",
			raw: instancesObject("", "DTSTART:20261005T090000Z\r\nDTEND:20261005T100000Z\r\nRRULE:FREQ=WEEKLY;COUNT=4\r\n"+
				"RDATE:20261008T150000Z\r\nEXDATE:20261012T090000Z\r\n"),
			start: october[0], end: october[1], want: 4,
		},
		{
			name:  "a daily series started two years before",
			raw:   instancesObject("", "DTSTART:20240101T090000Z\r\nDTEND:20240101T100000Z\r\nRRULE:FREQ=DAILY\r\n"),
			start: october[0], end: october[1], want: 31,
		},
		{
			name:  "a weekly series started a decade before, in a host zone",
			raw:   instancesObject("", "DTSTART;TZID=America/New_York:20150105T090000\r\nDTEND;TZID=America/New_York:20150105T100000\r\nRRULE:FREQ=WEEKLY\r\n"),
			start: october[0], end: october[1], want: 4,
		},
		{
			name: "a counted series across the skipped hour",
			raw: instancesObject("", "DTSTART;TZID=America/New_York:20260301T023000\r\nDTEND;TZID=America/New_York:20260301T033000\r\n"+
				"RRULE:FREQ=DAILY;COUNT=20\r\n"),
			start: time.Date(2026, 3, 1, 0, 0, 0, 0, time.UTC), end: time.Date(2026, 4, 1, 0, 0, 0, 0, time.UTC),
			want: 20,
		},
		{
			name: "a zone only the object defines",
			raw: instancesObject(instancesOutlookEastern, "DTSTART;TZID=\"Eastern Standard Time\":20240304T090000\r\n"+
				"DTEND;TZID=\"Eastern Standard Time\":20240304T100000\r\nRRULE:FREQ=WEEKLY;UNTIL=20240326T035959Z;BYDAY=MO\r\n"),
			start: time.Date(2024, 3, 1, 0, 0, 0, 0, time.UTC), end: time.Date(2024, 4, 1, 0, 0, 0, 0, time.UTC),
			want: 4,
		},
		{
			name:  "an all-day yearly series",
			raw:   instancesObject("", "DTSTART;VALUE=DATE:20200229\r\nRRULE:FREQ=YEARLY\r\n"),
			start: time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC), end: time.Date(2029, 1, 1, 0, 0, 0, 0, time.UTC),
			want: 2,
		},
		{
			name: "an override replacing one instance",
			raw: instancesObject("", "DTSTART:20261005T090000Z\r\nDTEND:20261005T100000Z\r\nRRULE:FREQ=WEEKLY;COUNT=4\r\n",
				"RECURRENCE-ID:20261012T090000Z\r\nDTSTART:20261013T150000Z\r\nDTEND:20261013T160000Z\r\nSUMMARY:Moved\r\n"),
			start: october[0], end: october[1], want: 3, overridden: []string{"20261012T090000Z"},
		},
		{
			name:  "floating times",
			raw:   instancesObject("", "DTSTART:20261005T090000\r\nDTEND:20261005T100000\r\nRRULE:FREQ=WEEKLY;UNTIL=20261026T090000\r\n"),
			start: october[0], end: october[1], want: 4,
		},
		{
			name: "an all-day series with an override",
			raw: instancesObject("", "DTSTART;VALUE=DATE:20261005\r\nRRULE:FREQ=WEEKLY;COUNT=4\r\n",
				"RECURRENCE-ID;VALUE=DATE:20261012\r\nDTSTART;VALUE=DATE:20261013\r\nSUMMARY:Moved\r\n"),
			start: october[0], end: october[1], want: 3, overridden: []string{"20261012T000000Z"},
		},
		{
			name: "an all-day override of every later instance of a timed series",
			raw: instancesObject("", "DTSTART:20261005T090000Z\r\nDTEND:20261005T100000Z\r\nRRULE:FREQ=WEEKLY;COUNT=4\r\n",
				"RECURRENCE-ID;RANGE=THISANDFUTURE:20261019T090000Z\r\nDTSTART;VALUE=DATE:20261019\r\nDTEND;VALUE=DATE:20261020\r\nSUMMARY:Later\r\n"),
			start: october[0], end: october[1], want: 4,
		},
		{
			name:  "a series lasting a DURATION",
			raw:   instancesObject("", "DTSTART;TZID=Europe/Berlin:20261020T090000\r\nDURATION:PT90M\r\nRRULE:FREQ=DAILY;COUNT=12\r\n"),
			start: october[0], end: october[1], want: 12,
		},
		{
			name:  "an all-day series with no end",
			raw:   instancesObject("", "DTSTART;VALUE=DATE:20261001\r\nRRULE:FREQ=WEEKLY\r\n"),
			start: october[0], end: october[1], want: 5,
		},
		{
			name: "an end in another zone than the start",
			raw: instancesObject("", "DTSTART;TZID=America/New_York:20261020T090000\r\nDTEND;TZID=Europe/Berlin:20261020T160000\r\n"+
				"RRULE:FREQ=DAILY;COUNT=14\r\n"),
			start: october[0], end: october[1], want: 12,
		},
		{
			name: "an override of every later instance that moves and lengthens them",
			raw: instancesObject("", "DTSTART:20261005T090000Z\r\nDURATION:PT1H\r\nRRULE:FREQ=WEEKLY;COUNT=4\r\n",
				"RECURRENCE-ID;RANGE=THISANDFUTURE:20261012T090000Z\r\nDTSTART:20261013T080000Z\r\nDURATION:PT3H\r\nSUMMARY:Later\r\n"),
			start: october[0], end: october[1], want: 4,
		},
		{
			name: "an override of every later instance that writes no end",
			raw: instancesObject("", "DTSTART:20261005T090000Z\r\nDTEND:20261005T100000Z\r\nRRULE:FREQ=WEEKLY;COUNT=4\r\n",
				"RECURRENCE-ID;RANGE=THISANDFUTURE:20261012T090000Z\r\nDTSTART:20261012T130000Z\r\nSUMMARY:Later\r\n"),
			start: october[0], end: october[1], want: 4,
		},
		{
			name: "an override of this and every later instance",
			raw: instancesObject("", "DTSTART:20261005T090000Z\r\nDTEND:20261005T100000Z\r\nRRULE:FREQ=WEEKLY;COUNT=4\r\n",
				"RECURRENCE-ID;RANGE=THISANDFUTURE:20261019T090000Z\r\nDTSTART:20261019T110000Z\r\nDTEND:20261019T130000Z\r\nSUMMARY:Later\r\n"),
			start: october[0], end: october[1], want: 4,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := EventInstances(tt.raw, tt.start, tt.end, nil)
			if !got.Recurs || !got.Complete {
				t.Fatalf("EventInstances(, nil) recurs = %v, complete = %v, want both", got.Recurs, got.Complete)
			}
			want := expandedInstances(t, tt.raw, tt.start, tt.end, tt.overridden...)
			if len(want) != tt.want {
				t.Fatalf("CalDAV expands %d instances, the fixture expects %d: %v", len(want), tt.want, want)
			}
			if len(got.Instances) != len(want) {
				t.Fatalf("EventInstances(, nil) lists %d instances, CalDAV expands %d:\n got %v\nwant %v", len(got.Instances), len(want), got.Instances, want)
			}
			for i := range want {
				g, w := got.Instances[i], want[i]
				if !g.RecurrenceID.Equal(w.RecurrenceID) || !g.Start.Equal(w.Start) || !g.End.Equal(w.End) {
					t.Errorf("instance %d = %v, CalDAV expands %v", i, g, w)
				}
			}
		})
	}
}

// An instance a RANGE=THISANDFUTURE override governs carries that override's
// content, which the views show it with, so it names the override.
func TestEventInstancesNameTheOverrideGoverningThem(t *testing.T) {
	raw := instancesObject("", "DTSTART:20261005T090000Z\r\nDTEND:20261005T100000Z\r\nRRULE:FREQ=WEEKLY;COUNT=4\r\n",
		"RECURRENCE-ID;RANGE=THISANDFUTURE:20261019T090000Z\r\nDTSTART:20261019T110000Z\r\nDTEND:20261019T130000Z\r\nSUMMARY:Later\r\n")
	got := EventInstances(raw, time.Date(2026, 10, 1, 0, 0, 0, 0, time.UTC), time.Date(2026, 11, 1, 0, 0, 0, 0, time.UTC), nil)
	override := mustUTC(t, "20261019T090000Z")
	for _, instance := range got.Instances {
		governed := !instance.RecurrenceID.Before(override)
		if governed != instance.Override.Equal(override) {
			t.Errorf("instance %s names override %v, want governed = %v", instance.RecurrenceID, instance.Override, governed)
		}
	}
}

// A RANGE=THISANDFUTURE override naming a slot its series never generates --
// here one its EXDATE leaves out -- is carried by no instance, so expansion
// writes it as itself and EventInstances places it there; one naming a slot
// the series generates is carried by the instances it governs, and placed as
// none of its own.
func TestEventInstancesPlaceAThisAndFutureOverrideOnlyWhereNoInstanceCarriesIt(t *testing.T) {
	october := []time.Time{time.Date(2026, 10, 1, 0, 0, 0, 0, time.UTC), time.Date(2026, 11, 1, 0, 0, 0, 0, time.UTC)}
	later := "RECURRENCE-ID;RANGE=THISANDFUTURE:20261012T090000Z\r\nDTSTART:20261012T110000Z\r\nDTEND:20261012T130000Z\r\nSUMMARY:Later\r\n"
	for _, tt := range []struct {
		name       string
		exdate     string
		instances  []string
		placed     []string
		overridden []string
	}{
		{
			name: "a slot the series generates", instances: []string{"20261005T090000Z", "20261012T110000Z", "20261019T110000Z"},
		},
		{
			name: "a slot its EXDATE leaves out", exdate: "EXDATE:20261012T090000Z\r\n",
			instances: []string{"20261005T090000Z", "20261019T110000Z"}, placed: []string{"20261012T110000Z"},
			overridden: []string{"20261012T090000Z"},
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			raw := instancesObject("", "DTSTART:20261005T090000Z\r\nDTEND:20261005T100000Z\r\nRRULE:FREQ=WEEKLY;COUNT=3\r\n"+tt.exdate, later)
			got := EventInstances(raw, october[0], october[1], nil)
			var starts, placed []string
			for _, instance := range got.Instances {
				starts = append(starts, instance.Start.Format("20060102T150405Z"))
			}
			for _, override := range got.Overrides {
				if override.End.Sub(override.Start) != 2*time.Hour || !override.RecurrenceID.Equal(mustUTC(t, "20261012T090000Z")) {
					t.Errorf("override placed as %+v, want its own two hours, naming the slot it names", override)
				}
				placed = append(placed, override.Start.Format("20060102T150405Z"))
			}
			if !slices.Equal(starts, tt.instances) || !slices.Equal(placed, tt.placed) {
				t.Errorf("EventInstances(, nil) lists instances at %v and places overrides at %v, want %v and %v", starts, placed, tt.instances, tt.placed)
			}
			expanded := expandedInstances(t, raw, october[0], october[1], tt.overridden...)
			if len(expanded) != len(tt.instances) {
				t.Errorf("CalDAV expands %d instances of the series, EventInstances lists %d", len(expanded), len(tt.instances))
			}
			code, body := zeroLengthReport(t, raw, calendarDataQuery(`<C:expand start="20261001T000000Z" end="20261101T000000Z"/>`))
			if code != 207 {
				t.Fatalf("expand status = %d, body %s", code, body)
			}
			if got, want := strings.Count(body, "DTSTART:20261012T110000Z"), 1; got != want {
				t.Errorf("expansion writes the override's start %d times, want %d:\n%s", got, want, body)
			}
		})
	}
}

// RANGE=THISANDFUTURE overrides a decade apart on a series of every minute
// have millions of slots between them, more than one pass over the series may
// generate, yet each is still found to name a slot the series generates, and
// none stands for itself; one naming no minute stands for itself.
func TestThisAndFutureOverridesFarApartOnADenseSeriesAreEachMatchedToTheirSlot(t *testing.T) {
	raw := instancesObject("", "DTSTART:20200101T000000Z\r\nDURATION:PT30S\r\nRRULE:FREQ=MINUTELY\r\n",
		"RECURRENCE-ID;RANGE=THISANDFUTURE:20200101T001000Z\r\nDTSTART:20200101T001000Z\r\nDURATION:PT30S\r\nSUMMARY:Early\r\n",
		"RECURRENCE-ID;RANGE=THISANDFUTURE:20300101T000000Z\r\nDTSTART:20300101T000000Z\r\nDURATION:PT30S\r\nSUMMARY:Late\r\n",
		"RECURRENCE-ID;RANGE=THISANDFUTURE:20300101T001030Z\r\nDTSTART:20300101T001030Z\r\nDURATION:PT15S\r\nSUMMARY:Between\r\n")
	from, to := time.Date(2030, 1, 1, 0, 0, 0, 0, time.UTC), time.Date(2030, 1, 1, 0, 2, 0, 0, time.UTC)
	got := EventInstances(raw, from, to, nil)
	if len(got.Instances) != 2 || len(got.Overrides) != 0 {
		t.Errorf("EventInstances(, nil) lists %+v and places %+v, want the two minutes alone", got.Instances, got.Overrides)
	}
	between := EventInstances(raw, time.Date(2030, 1, 1, 0, 10, 0, 0, time.UTC), time.Date(2030, 1, 1, 0, 11, 0, 0, time.UTC), nil)
	if len(between.Overrides) != 1 || !between.Overrides[0].Start.Equal(mustUTC(t, "20300101T001030Z")) {
		t.Errorf("EventInstances(, nil) places %+v, want the override naming no minute", between.Overrides)
	}
	code, body := zeroLengthReport(t, raw, calendarDataQuery(`<C:expand start="20300101T000000Z" end="20300101T000200Z"/>`))
	if code != 207 {
		t.Fatalf("expand status = %d, body %s", code, body)
	}
	if strings.Count(body, "BEGIN:VEVENT") != 2 || strings.Count(body, "DTSTART:20300101T000000Z") != 1 {
		t.Errorf("expansion writes other than the two minutes:\n%s", body)
	}
}

// A cancelled RANGE=THISANDFUTURE override removes the slot it names and every
// later one, which expansion leaves out, so it is placed as no instance of
// its own either.
func TestEventInstancesPlaceNoCancelledThisAndFutureOverride(t *testing.T) {
	raw := instancesObject("", "DTSTART:20261005T090000Z\r\nDTEND:20261005T100000Z\r\nRRULE:FREQ=WEEKLY;COUNT=3\r\n",
		"RECURRENCE-ID;RANGE=THISANDFUTURE:20261012T090000Z\r\nDTSTART:20261012T090000Z\r\nDTEND:20261012T100000Z\r\nSTATUS:CANCELLED\r\n")
	october := []time.Time{time.Date(2026, 10, 1, 0, 0, 0, 0, time.UTC), time.Date(2026, 11, 1, 0, 0, 0, 0, time.UTC)}
	got := EventInstances(raw, october[0], october[1], nil)
	if len(got.Instances) != 1 || !got.Instances[0].Start.Equal(mustUTC(t, "20261005T090000Z")) || len(got.Overrides) != 0 {
		t.Errorf("EventInstances(, nil) lists %+v and places %+v, want the first instance alone", got.Instances, got.Overrides)
	}
	code, body := zeroLengthReport(t, raw, calendarDataQuery(`<C:expand start="20261001T000000Z" end="20261101T000000Z"/>`))
	if code != 207 {
		t.Fatalf("expand status = %d, body %s", code, body)
	}
	if strings.Count(body, "BEGIN:VEVENT") != 1 {
		t.Errorf("expansion writes other than the first instance:\n%s", body)
	}
}

// An all-day override repeating its RECURRENCE-ID as its DTSTART and writing
// no end lasts the day its series gives it, as expansion writes it.
func TestEventInstancesPlaceAnAllDayOverrideOverItsDay(t *testing.T) {
	raw := instancesObject("", "DTSTART;VALUE=DATE:20261005\r\nRRULE:FREQ=WEEKLY;COUNT=3\r\n",
		"RECURRENCE-ID;VALUE=DATE:20261012\r\nDTSTART;VALUE=DATE:20261012\r\nSUMMARY:Renamed\r\n")
	got := EventInstances(raw, time.Date(2026, 10, 1, 0, 0, 0, 0, time.UTC), time.Date(2026, 11, 1, 0, 0, 0, 0, time.UTC), nil)
	want := []calobject.Instance{{RecurrenceID: mustUTC(t, "20261012T000000Z"), Start: mustUTC(t, "20261012T000000Z"), End: mustUTC(t, "20261013T000000Z")}}
	if !slices.EqualFunc(got.Overrides, want, func(a, b calobject.Instance) bool {
		return a.RecurrenceID.Equal(b.RecurrenceID) && a.Start.Equal(b.Start) && a.End.Equal(b.End)
	}) {
		t.Errorf("EventInstances(, nil) places overrides at %+v, want %+v", got.Overrides, want)
	}
}

// An override whose DTSTART repeats the wall clock of its RECURRENCE-ID in
// another zone starts at that zone's reading of it.
func TestEventInstancesPlaceAnOverrideInItsOwnStartsZone(t *testing.T) {
	raw := instancesObject("", "DTSTART;TZID=America/New_York:20261005T090000\r\nDTEND;TZID=America/New_York:20261005T100000\r\nRRULE:FREQ=WEEKLY;COUNT=3\r\n",
		"RECURRENCE-ID;TZID=America/New_York:20261012T090000\r\nDTSTART;TZID=Europe/Berlin:20261012T090000\r\nDTEND;TZID=Europe/Berlin:20261012T100000\r\nSUMMARY:Moved\r\n")
	got := EventInstances(raw, time.Date(2026, 10, 1, 0, 0, 0, 0, time.UTC), time.Date(2026, 11, 1, 0, 0, 0, 0, time.UTC), nil)
	want := []calobject.Instance{{RecurrenceID: mustUTC(t, "20261012T130000Z"), Start: mustUTC(t, "20261012T070000Z"), End: mustUTC(t, "20261012T080000Z")}}
	if !slices.EqualFunc(got.Overrides, want, func(a, b calobject.Instance) bool {
		return a.RecurrenceID.Equal(b.RecurrenceID) && a.Start.Equal(b.Start) && a.End.Equal(b.End)
	}) {
		t.Errorf("EventInstances(, nil) places overrides at %+v, want %+v", got.Overrides, want)
	}
}

// A minutely series holds far more instances in a month than one expansion
// returns. The listing stops at the expansion's limit and says it is partial.
func TestEventInstancesStopAtTheExpansionLimit(t *testing.T) {
	raw := instancesObject("", "DTSTART:20261001T000000Z\r\nDTEND:20261001T000030Z\r\nRRULE:FREQ=MINUTELY\r\n")
	got := EventInstances(raw, time.Date(2026, 10, 1, 0, 0, 0, 0, time.UTC), time.Date(2026, 11, 1, 0, 0, 0, 0, time.UTC), nil)
	if !got.Recurs || got.Complete {
		t.Fatalf("EventInstances(, nil) recurs = %v, complete = %v, want a partial listing", got.Recurs, got.Complete)
	}
	if len(got.Instances) != ical.MaxRecurrenceInstances {
		t.Fatalf("EventInstances(, nil) lists %d instances, want %d", len(got.Instances), ical.MaxRecurrenceInstances)
	}
	if first := got.Instances[0].Start; !first.Equal(time.Date(2026, 10, 1, 0, 0, 0, 0, time.UTC)) {
		t.Fatalf("the first listed instance starts %s, want the series' start", first)
	}
}

// Nothing to expand: a single event, and an object that cannot be read.
func TestEventInstancesOfWhatDoesNotRecur(t *testing.T) {
	for name, raw := range map[string]string{
		"single":     instancesObject("", "DTSTART:20261005T090000Z\r\nDTEND:20261005T100000Z\r\n"),
		"unreadable": "not a calendar",
	} {
		if got := EventInstances(raw, time.Date(2026, 10, 1, 0, 0, 0, 0, time.UTC), time.Date(2026, 11, 1, 0, 0, 0, 0, time.UTC), nil); got.Recurs || len(got.Instances) != 0 {
			t.Errorf("%s: EventInstances(, nil) = %+v, want nothing", name, got)
		}
	}
}

// overriddenSeriesObject is a yearly series of two instances and n ordinary
// overrides, one for each day from its first, each starting an hour after the
// slot it names. PUT refuses more than
// 10,000 override components, and objects a 1.1.x release stored may hold more
// than an expansion lists.
func overriddenSeriesObject(n int) string {
	first := time.Date(2026, 1, 1, 9, 0, 0, 0, time.UTC)
	var b strings.Builder
	b.WriteString("BEGIN:VCALENDAR\r\nVERSION:2.0\r\nPRODID:-//Test//EN\r\nBEGIN:VEVENT\r\nUID:overridden\r\nDTSTAMP:20260101T000000Z\r\n" +
		"DTSTART:20260101T090000Z\r\nDTEND:20260101T100000Z\r\nRRULE:FREQ=YEARLY;COUNT=2\r\nEND:VEVENT\r\n")
	for i := range n {
		slot := first.AddDate(0, 0, i)
		b.WriteString("BEGIN:VEVENT\r\nUID:overridden\r\nDTSTAMP:20260101T000000Z\r\nRECURRENCE-ID:" + slot.Format("20060102T150405Z") +
			"\r\nDTSTART:" + slot.Add(time.Hour).Format("20060102T150405Z") + "\r\nDTEND:" + slot.Add(2*time.Hour).Format("20060102T150405Z") + "\r\nEND:VEVENT\r\n")
	}
	b.WriteString("END:VCALENDAR\r\n")
	return b.String()
}

// An expansion lists at most ical.MaxRecurrenceInstances of the instances and
// the overrides placed beside them together, as CALDAV:expand writes at most
// that many components, and is incomplete when it left any out.
func TestEventInstancesCountsOverridesAgainstTheInstanceBudget(t *testing.T) {
	from, to := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC), time.Date(2030, 1, 1, 0, 0, 0, 0, time.UTC)
	t.Run("within the budget", func(t *testing.T) {
		got := EventInstances(overriddenSeriesObject(20), from, to, nil)
		if !got.Recurs || len(got.Overrides) != 20 || !got.Complete {
			t.Errorf("recurs %v, %d overrides placed, complete %v; want all 20 of them, complete", got.Recurs, len(got.Overrides), got.Complete)
		}
	})
	t.Run("past the budget", func(t *testing.T) {
		got := EventInstances(overriddenSeriesObject(ical.MaxRecurrenceInstances+500), from, to, nil)
		if !got.Recurs {
			t.Fatal("the series is not expanded")
		}
		if listed := len(got.Instances) + len(got.Overrides); listed != ical.MaxRecurrenceInstances {
			t.Errorf("%d instances and %d overrides listed, want %d together", len(got.Instances), len(got.Overrides), ical.MaxRecurrenceInstances)
		}
		if got.Complete {
			t.Error("an expansion that left overrides out is complete")
		}
		for i := 1; i < len(got.Overrides); i++ {
			if got.Overrides[i].Start.Before(got.Overrides[i-1].Start) {
				t.Fatal("the overrides kept are not the earliest by start")
			}
		}
	})
}

// Past the budget an expansion keeps the earliest of the instances and the
// overrides together, in time order, not every instance first: an override
// early in the range is listed beside the instances around it, and the later
// instances are what is left out.
func TestEventInstancesKeepsTheEarliestOfInstancesAndOverridesTogether(t *testing.T) {
	raw := "BEGIN:VCALENDAR\r\nVERSION:2.0\r\nPRODID:-//Test//EN\r\n" +
		"BEGIN:VEVENT\r\nUID:long\r\nDTSTAMP:20260101T000000Z\r\nDTSTART:20260101T090000Z\r\nDTEND:20260101T100000Z\r\nRRULE:FREQ=DAILY;COUNT=1500\r\nEND:VEVENT\r\n" +
		"BEGIN:VEVENT\r\nUID:long\r\nDTSTAMP:20260101T000000Z\r\nRECURRENCE-ID:20260103T090000Z\r\nDTSTART:20260103T110000Z\r\nDTEND:20260103T120000Z\r\nEND:VEVENT\r\n" +
		"BEGIN:VEVENT\r\nUID:long\r\nDTSTAMP:20260101T000000Z\r\nRECURRENCE-ID:20260105T090000Z\r\nDTSTART:20260105T110000Z\r\nDTEND:20260105T120000Z\r\nEND:VEVENT\r\n" +
		"END:VCALENDAR\r\n"
	got := EventInstances(raw, time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC), time.Date(2031, 1, 1, 0, 0, 0, 0, time.UTC), nil)
	if !got.Recurs || got.Complete {
		t.Fatalf("recurs %v, complete %v; want a series cut at the budget", got.Recurs, got.Complete)
	}
	if len(got.Overrides) != 2 || len(got.Instances) != ical.MaxRecurrenceInstances-2 {
		t.Fatalf("%d instances and %d overrides listed, want 998 and the 2 overrides, 1000 together", len(got.Instances), len(got.Overrides))
	}
	last := got.Instances[len(got.Instances)-1].Start
	for _, override := range got.Overrides {
		if override.Start.After(last) {
			t.Errorf("override at %v is listed past the last instance kept, %v", override.Start, last)
		}
	}
	// 1,000 entries in time order: the instances of the first 1,000 days less
	// the two an override replaces, and the two overrides.
	if want := time.Date(2026, 1, 1, 9, 0, 0, 0, time.UTC).AddDate(0, 0, 1000-1); !last.Equal(want) {
		t.Errorf("the last instance kept is %v, want %v", last, want)
	}
}
