package dav

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/jw6ventures/calcard/internal/ical"
	"github.com/jw6ventures/calcard/internal/store"
)

// berlinTimezone is the European Union's rules for Central European Time under
// the given TZID, as a client ships them.
func berlinTimezone(tzid string) []string {
	return []string{
		"BEGIN:VTIMEZONE", "TZID:" + tzid,
		"BEGIN:DAYLIGHT", "TZOFFSETFROM:+0100", "TZOFFSETTO:+0200", "TZNAME:CEST",
		"DTSTART:19810329T020000", "RRULE:FREQ=YEARLY;BYMONTH=3;BYDAY=-1SU", "END:DAYLIGHT",
		"BEGIN:STANDARD", "TZOFFSETFROM:+0200", "TZOFFSETTO:+0100", "TZNAME:CET",
		"DTSTART:19961027T030000", "RRULE:FREQ=YEARLY;BYMONTH=10;BYDAY=-1SU", "END:STANDARD",
		"END:VTIMEZONE",
	}
}

// skippedTimeFixture is one way a stored object can name the zone of a series
// at 02:30, the hour Berlin's clocks skip on the last Sunday of March.
type skippedTimeFixture struct {
	name string
	// object builds the resource from the series' VEVENT properties, which
	// carry TZID=@ where the zone goes.
	object func(t *testing.T, properties ...string) string
}

func skippedTimeFixtures() []skippedTimeFixture {
	withZone := func(tzid string, timezone bool) func(*testing.T, ...string) string {
		return func(_ *testing.T, properties ...string) string {
			lines := []string{"VERSION:2.0", "PRODID:-//Client//EN"}
			if timezone {
				lines = append(lines, berlinTimezone(tzid)...)
			}
			for i := range properties {
				properties[i] = strings.ReplaceAll(properties[i], "TZID=@", "TZID="+tzid)
			}
			lines = append(lines, componentLines("VEVENT", properties...)...)
			return wrapCalendar(lines...)
		}
	}
	return []skippedTimeFixture{
		{name: "client VTIMEZONE", object: withZone("Europe/Berlin", true)},
		{name: "zone only the object defines", object: withZone("Private/Berlin", true)},
		// Rows stored before the validator added the host's definition carry
		// none, and are read through the host's zone of that name.
		{name: "no VTIMEZONE", object: withZone("Europe/Berlin", false)},
		{name: "definition the validator stores", object: func(t *testing.T, properties ...string) string {
			t.Helper()
			validated, fault := validateCalendarObjectForStorage(withZone("Europe/Berlin", false)(t, properties...), nil)
			if fault != nil {
				t.Fatalf("validator refused the object: %+v", fault)
			}
			if !strings.Contains(validated.Body, "BEGIN:VTIMEZONE") {
				t.Fatal("the validator stored no definition")
			}
			return validated.Body
		}},
	}
}

// weeklySkippedSeries is a weekly Sunday 02:30 series of four whose third
// Sunday, 29 March 2026, is the day Berlin's clocks jump from 02:00 to 03:00.
// RFC 5545 §3.3.10 leaves that instance out of the set uncounted, so the
// series runs 15 and 22 March and 5 and 12 April.
func weeklySkippedSeries() []string {
	return []string{
		"UID:series", "DTSTAMP:20260101T000000Z",
		"DTSTART;TZID=@:20260315T023000", "DTEND;TZID=@:20260315T033000",
		"RRULE:FREQ=WEEKLY;COUNT=4",
	}
}

func TestCalendarQueryLeavesOutTheInstanceOnASkippedLocalTime(t *testing.T) {
	requireHostZones(t, "Europe/Berlin")
	for _, fixture := range skippedTimeFixtures() {
		t.Run(fixture.name, func(t *testing.T) {
			event := store.Event{UID: "series", RawICAL: fixture.object(t, weeklySkippedSeries()...)}
			for _, tt := range []struct {
				start, end string
				want       bool
			}{
				{"20260322T000000Z", "20260323T000000Z", true},
				{"20260329T000000Z", "20260330T000000Z", false},
				{"20260405T000000Z", "20260406T000000Z", true},
				{"20260412T000000Z", "20260413T000000Z", true},
				{"20260419T000000Z", "20260420T000000Z", false},
			} {
				if got := filterMatches(event, calQueryWithTimeRange(tt.start, tt.end), floatingZone{}); got != tt.want {
					t.Errorf("time-range %s/%s matched = %v, want %v", tt.start, tt.end, got, tt.want)
				}
			}
		})
	}
}

func TestExpandLeavesOutTheInstanceOnASkippedLocalTime(t *testing.T) {
	requireHostZones(t, "Europe/Berlin")
	for _, fixture := range skippedTimeFixtures() {
		t.Run(fixture.name, func(t *testing.T) {
			raw := fixture.object(t, weeklySkippedSeries()...)
			code, body := zeroLengthReport(t, raw, calendarDataQuery(`<C:expand start="20260301T000000Z" end="20260501T000000Z"/>`))
			if code != http.StatusMultiStatus {
				t.Fatalf("status = %d, body %s", code, body)
			}
			for _, want := range []string{
				"RECURRENCE-ID:20260315T013000Z", "RECURRENCE-ID:20260322T013000Z",
				"RECURRENCE-ID:20260405T003000Z", "RECURRENCE-ID:20260412T003000Z",
			} {
				if !strings.Contains(body, want) {
					t.Errorf("expanded output lacks %s:\n%s", want, body)
				}
			}
			if strings.Contains(body, "20260329T") || strings.Count(body, "RECURRENCE-ID") != 4 {
				t.Errorf("expanded output carries an instance on the skipped Sunday:\n%s", body)
			}
		})
	}
}

func TestFreeBusyLeavesOutTheInstanceOnASkippedLocalTime(t *testing.T) {
	requireHostZones(t, "Europe/Berlin")
	calRepo := &fakeCalendarRepo{accessible: []store.CalendarAccess{
		{Calendar: store.Calendar{ID: 1, UserID: 1, Name: "Work"}, Editor: true},
	}}
	body := `<C:free-busy-query xmlns:C="urn:ietf:params:xml:ns:caldav">
  <C:time-range start="20260301T000000Z" end="20260501T000000Z"/>
</C:free-busy-query>`
	want := []string{
		"FREEBUSY:20260315T013000Z/20260315T023000Z",
		"FREEBUSY:20260322T013000Z/20260322T023000Z",
		"FREEBUSY:20260405T003000Z/20260405T013000Z",
		"FREEBUSY:20260412T003000Z/20260412T013000Z",
	}
	for _, fixture := range skippedTimeFixtures() {
		t.Run(fixture.name, func(t *testing.T) {
			h := &DavServer{store: &store.Store{Calendars: calRepo, Events: &fakeEventRepo{
				events: map[string]*store.Event{"1:series": {
					CalendarID: 1, UID: "series", ResourceName: "series", ETag: "e1",
					RawICAL: fixture.object(t, weeklySkippedSeries()...),
				}},
			}}}
			rr := httptest.NewRecorder()
			h.Report(rr, freeBusyRequestFor("/dav/calendars/1/", body, &store.User{ID: 1}))
			if rr.Code != http.StatusOK {
				t.Fatalf("status = %d, want 200: %s", rr.Code, rr.Body.String())
			}
			assertPublishedFreeBusy(t, parsedFreeBusyLines(t, rr.Body.String()), want)
		})
	}
}

// CALDAV:max-instances counts the instances the set has, so the skipped local
// times a daily 02:30 Berlin series passes are not counted against it: 1003
// days from 1 January 2026 through 29 September 2028 hold three of them and
// 1000 instances, the limit, and one more day is over it.
func TestMaxInstancesDoesNotCountSkippedLocalTimes(t *testing.T) {
	requireHostZones(t, "Europe/Berlin")
	series := func(until string) []string {
		return []string{
			"UID:series", "DTSTAMP:20260101T000000Z",
			"DTSTART;TZID=@:20260101T023000", "DTEND;TZID=@:20260101T033000",
			"RRULE:FREQ=DAILY;UNTIL=" + until,
		}
	}
	for _, fixture := range skippedTimeFixtures() {
		if fixture.name == "definition the validator stores" {
			continue
		}
		t.Run(fixture.name, func(t *testing.T) {
			if _, fault := validateCalendarObjectForStorage(fixture.object(t, series("20280929T235959Z")...), nil); fault != nil {
				t.Errorf("a set of 1000 instances was refused: %+v", fault)
			}
			_, fault := validateCalendarObjectForStorage(fixture.object(t, series("20280930T235959Z")...), nil)
			if fault == nil || len(fault.conditions) != 1 || fault.conditions[0] != "max-instances" {
				t.Errorf("a set of 1001 instances got %+v, want max-instances", fault)
			}
		})
	}
}

// An override whose RECURRENCE-ID names the skipped Sunday names no instance of
// the set. It is answered the way any override naming no generated instance
// is: the series' pattern carries no slot for it, while the override itself is
// still a component of the resource -- matched by its own dates, published as
// busy time, and returned by expand.
func TestOverrideOfASkippedInstanceIsKeptAsItsOwnComponent(t *testing.T) {
	requireHostZones(t, "Europe/Berlin")
	raw := wrapCalendar(append(append(append([]string{"VERSION:2.0", "PRODID:-//Client//EN"}, berlinTimezone("Europe/Berlin")...),
		componentLines("VEVENT", "UID:series", "DTSTAMP:20260101T000000Z",
			"DTSTART;TZID=Europe/Berlin:20260322T023000", "DTEND;TZID=Europe/Berlin:20260322T033000",
			"RRULE:FREQ=WEEKLY;COUNT=2")...),
		componentLines("VEVENT", "UID:series", "DTSTAMP:20260101T000000Z",
			"RECURRENCE-ID;TZID=Europe/Berlin:20260329T023000",
			"DTSTART;TZID=Europe/Berlin:20260329T100000", "DTEND;TZID=Europe/Berlin:20260329T110000")...)...)

	event := store.Event{UID: "series", RawICAL: raw}
	if !filterMatches(event, calQueryWithTimeRange("20260329T070000Z", "20260329T090000Z"), floatingZone{}) {
		t.Error("calendar-query missed the override at its own time")
	}
	if !filterMatches(event, calQueryWithTimeRange("20260405T000000Z", "20260406T000000Z"), floatingZone{}) {
		t.Error("calendar-query missed the series' second instance, after the skipped Sunday")
	}

	code, body := zeroLengthReport(t, raw, calendarDataQuery(`<C:expand start="20260301T000000Z" end="20260501T000000Z"/>`))
	if code != http.StatusMultiStatus {
		t.Fatalf("status = %d, body %s", code, body)
	}
	for _, want := range []string{
		"RECURRENCE-ID:20260322T013000Z", "RECURRENCE-ID:20260405T003000Z",
		"RECURRENCE-ID:20260329T013000Z", "DTSTART:20260329T080000Z",
	} {
		if !strings.Contains(body, want) {
			t.Errorf("expanded output lacks %s:\n%s", want, body)
		}
	}
}

// The VTIMEZONE a resource ships is what says which local times its zone
// skips, even under a name the host also defines (RFC 4791 §4.1): a definition
// keeping Central European Time all year skips nothing, so the Sunday 02:30
// the host's Europe/Berlin skips is an instance like any other.
func TestShippedDefinitionDecidesWhichLocalTimesAreSkipped(t *testing.T) {
	requireHostZones(t, "Europe/Berlin")
	properties := weeklySkippedSeries()
	for i := range properties {
		properties[i] = strings.ReplaceAll(properties[i], "TZID=@", "TZID=Europe/Berlin")
	}
	raw := wrapCalendar(append([]string{"VERSION:2.0", "PRODID:-//Client//EN",
		"BEGIN:VTIMEZONE", "TZID:Europe/Berlin",
		"BEGIN:STANDARD", "TZOFFSETFROM:+0100", "TZOFFSETTO:+0100", "DTSTART:19700101T000000", "END:STANDARD",
		"END:VTIMEZONE"}, componentLines("VEVENT", properties...)...)...)
	event := store.Event{UID: "series", RawICAL: raw}
	if !filterMatches(event, calQueryWithTimeRange("20260329T013000Z", "20260329T020000Z"), floatingZone{}) {
		t.Error("calendar-query missed the 29 March instance the shipped definition does not skip")
	}
	if filterMatches(event, calQueryWithTimeRange("20260412T000000Z", "20260413T000000Z"), floatingZone{}) {
		t.Error("calendar-query matched a fifth Sunday of a four-instance series")
	}
}

// A definition whose DAYLIGHT observance states a TZOFFSETFROM of +02:00 after
// a STANDARD one moving to +01:00 contradicts itself about the offset in force
// before the spring onset. Each observance is taken at its word (RFC 5545
// §3.6.5): the one gap the definition states is the DAYLIGHT observance's own,
// from its 02:00 onset for TZOFFSETTO less TZOFFSETFROM, an hour, so the 01:30
// instance on 29 March exists, read in the +01:00 in force before the onset.
// The expansion, the stored bound and the max-instances count all read gaps so,
// and every wall clock outside the gaps the matcher lists exists.
func TestContradictoryDefinitionSkipsOnlyTheGapItsObservanceStates(t *testing.T) {
	timezone := []string{
		"BEGIN:VTIMEZONE", "TZID:Example/Contradictory",
		"BEGIN:STANDARD", "TZOFFSETFROM:+0200", "TZOFFSETTO:+0100",
		"DTSTART:19961027T030000", "RRULE:FREQ=YEARLY;BYMONTH=10;BYDAY=-1SU", "END:STANDARD",
		"BEGIN:DAYLIGHT", "TZOFFSETFROM:+0200", "TZOFFSETTO:+0300",
		"DTSTART:19810329T020000", "RRULE:FREQ=YEARLY;BYMONTH=3;BYDAY=-1SU", "END:DAYLIGHT",
		"END:VTIMEZONE",
	}
	raw := wrapCalendar(append(append([]string{"VERSION:2.0", "PRODID:-//Client//EN"}, timezone...),
		componentLines("VEVENT", "UID:series", "DTSTAMP:20260101T000000Z",
			"DTSTART;TZID=Example/Contradictory:20260327T013000", "DTEND;TZID=Example/Contradictory:20260327T023000",
			"RRULE:FREQ=DAILY;COUNT=4")...)...)
	event := store.Event{UID: "series", RawICAL: raw}
	if !filterMatches(event, calQueryWithTimeRange("20260329T000000Z", "20260329T010000Z"), floatingZone{}) {
		t.Error("calendar-query missed the 01:30 instance on 29 March, outside the stated gap")
	}
	if filterMatches(event, calQueryWithTimeRange("20260330T220000Z", "20260331T030000Z"), floatingZone{}) {
		t.Error("calendar-query matched a 31 March instance the four-instance series does not have")
	}
	if exceeds, ok := ical.RecurrenceSetExceedsLimit(raw, 4); !ok || exceeds {
		t.Errorf("RecurrenceSetExceedsLimit(raw, 4) = %v, %v; want false, true", exceeds, ok)
	}
	if bounds := ical.ConservativeRecurrenceBounds(raw); bounds.UntilUnknown || bounds.Until == nil ||
		bounds.Until.Before(time.Date(2026, 3, 29, 23, 30, 0, 0, time.UTC)) {
		t.Errorf("bounds = %+v, want an until past the last instance's end", bounds)
	}

	root, err := parseStoredICalendarObject(raw)
	if err != nil {
		t.Fatal(err)
	}
	matcher := newCalendarTimeRangeMatcher(raw, root, floatingZone{})
	from, through := time.Date(2026, 3, 28, 0, 0, 0, 0, time.UTC), time.Date(2026, 3, 31, 0, 0, 0, 0, time.UTC)
	gaps, ok := matcher.LocalTimeGaps("Example/Contradictory", from.Add(-48*time.Hour), through)
	if !ok || len(gaps) != 1 || !gaps[0].Start.Equal(time.Date(2026, 3, 29, 2, 0, 0, 0, time.UTC)) || gaps[0].Length != time.Hour {
		t.Fatalf("LocalTimeGaps = %v, %v; want the one hour from 02:00 on 29 March", gaps, ok)
	}
	for wall := from; wall.Before(through); wall = wall.Add(15 * time.Minute) {
		inGap := !wall.Before(gaps[0].Start) && wall.Before(gaps[0].Start.Add(gaps[0].Length))
		if exists := matcher.LocalTimeExists("Example/Contradictory", wall); exists == inGap {
			t.Errorf("LocalTimeExists(%v) = %v, inside the listed gap: %v", wall, exists, inGap)
		}
	}
}

// Expanding a series in a zone the resource defines resolves each instance
// through one walk of the zone's observances. Whether an instance's wall clock
// exists is read off the gaps the zone lists for a year at a time, so it costs
// no walk of its own except for the first instance and those inside a gap.
// Berlin's definition has two rules, so a year of daily instances asks the
// shared listings about two lookups an instance; the work is counted rather
// than timed.
func TestExpansionResolvesEachInstanceThroughOneObservanceLookup(t *testing.T) {
	raw := wrapCalendar(append(append([]string{"VERSION:2.0", "PRODID:-//Client//EN"}, berlinTimezone("Private/Berlin")...),
		componentLines("VEVENT", "UID:series", "DTSTAMP:20260101T000000Z",
			"DTSTART;TZID=Private/Berlin:20260101T023000", "DTEND;TZID=Private/Berlin:20260101T033000",
			"RRULE:FREQ=DAILY")...)...)
	root, err := parseStoredICalendarObject(raw)
	if err != nil {
		t.Fatal(err)
	}
	onsets := useTimezoneOnsets(t, maxHeldOnsets)
	matcher := newCalendarTimeRangeMatcher(raw, root, floatingZone{})
	dtstart, ok := matcher.dateValue(root.children[1], "DTSTART", noShift)
	if !ok {
		t.Fatal("DTSTART did not resolve")
	}
	rangeStart, rangeEnd := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC), time.Date(2027, 1, 1, 0, 0, 0, 0, time.UTC)
	instances := 0
	if err := ical.VisitRecurrenceInstances(raw, "VEVENT", dtstart.instant, time.Hour, rangeStart, rangeEnd, ical.MaxRecurrenceInstances, matcher,
		func(ical.RecurrenceInstance) bool { instances++; return false }); err != nil {
		t.Fatal(err)
	}
	// 2026 has 365 days, one of them the 29 March the zone skips 02:30 on.
	if instances != 364 {
		t.Fatalf("expanded %d instances, want 364", instances)
	}
	if lookups, budget := onsets.lookups.Load(), int64(2*365+16); lookups > budget {
		t.Errorf("asked the observance listings %d times for %d instances, want at most %d", lookups, instances, budget)
	}
}

// periodWalkResolver reads dates as the matcher does but lists no gaps, so an
// expansion through it asks about every candidate and walks a counted
// sub-daily rule one period at a time.
type periodWalkResolver struct {
	ical.RecurrenceTimeResolver
}

// Through the zone a resource defines, jumping a counted sub-daily rule over
// the periods before the range and reading existence off the listed gaps give
// the instances asking about every period does, across a spring gap and a
// fall overlap.
func TestShippedZoneExpansionMatchesAskingEveryPeriod(t *testing.T) {
	newYork, ok := ical.VTimezone("America/New_York", time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC), time.Date(2027, 12, 31, 0, 0, 0, 0, time.UTC))
	if !ok {
		t.Skip("the host does not define America/New_York")
	}
	definitions := map[string][]string{
		"Private/Berlin":   berlinTimezone("Private/Berlin"),
		"America/New_York": strings.Split(strings.TrimSuffix(clientReading(newYork), "\r\n"), "\r\n"),
	}
	for tzid, definition := range definitions {
		for _, series := range []struct{ dtstart, rrule string }{
			{"20260328T224513", "FREQ=SECONDLY;INTERVAL=37;COUNT=3000"},
			{"20260328T224513", "FREQ=MINUTELY;INTERVAL=7;COUNT=300"},
			{"20260328T200000", "FREQ=HOURLY;COUNT=40"},
			{"20260307T200000", "FREQ=MINUTELY;INTERVAL=13;COUNT=200"},
			{"20261024T200000", "FREQ=MINUTELY;INTERVAL=11;COUNT=300"},
			{"20261031T220000", "FREQ=HOURLY;COUNT=40"},
			{"20260301T023000", "FREQ=DAILY;COUNT=60"},
		} {
			t.Run(tzid+"/"+series.dtstart+"/"+series.rrule, func(t *testing.T) {
				raw := wrapCalendar(append(append([]string{"VERSION:2.0", "PRODID:-//Client//EN"}, definition...),
					componentLines("VEVENT", "UID:series", "DTSTAMP:20260101T000000Z",
						"DTSTART;TZID="+tzid+":"+series.dtstart, "DURATION:PT1M", "RRULE:"+series.rrule)...)...)
				root, err := parseStoredICalendarObject(raw)
				if err != nil {
					t.Fatal(err)
				}
				matcher := newCalendarTimeRangeMatcher(raw, root, floatingZone{})
				dtstart, ok := matcher.dateValue(root.children[1], "DTSTART", noShift)
				if !ok {
					t.Fatal("DTSTART did not resolve")
				}
				expand := func(resolve ical.RecurrenceTimeResolver, rangeStart, rangeEnd time.Time) []time.Time {
					var starts []time.Time
					if err := ical.VisitRecurrenceInstances(raw, "VEVENT", dtstart.instant, time.Minute, rangeStart, rangeEnd, 100*ical.MaxRecurrenceInstances, resolve,
						func(instance ical.RecurrenceInstance) bool { starts = append(starts, instance.Start); return false }); err != nil {
						t.Fatal(err)
					}
					return starts
				}
				for _, offset := range []time.Duration{0, 26 * time.Hour, 30 * time.Hour, 50 * time.Hour, 30 * 24 * time.Hour} {
					rangeStart := dtstart.instant.Add(offset)
					rangeEnd := rangeStart.Add(12 * time.Hour)
					listed := expand(matcher, rangeStart, rangeEnd)
					asked := expand(periodWalkResolver{matcher}, rangeStart, rangeEnd)
					if len(listed) != len(asked) {
						t.Fatalf("range from %v: %d instances through the listed gaps, %d asking every period", rangeStart, len(listed), len(asked))
					}
					for i := range listed {
						if !listed[i].Equal(asked[i]) {
							t.Fatalf("range from %v: instance %d is %v through the listed gaps, %v asking every period", rangeStart, i, listed[i], asked[i])
						}
					}
				}
			})
		}
	}
}
