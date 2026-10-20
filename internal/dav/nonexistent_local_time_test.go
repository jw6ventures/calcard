package dav

import (
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/jw6ventures/calcard/internal/calobject"
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
		// Rows stored without the host's definition, as a 1.1.x release
		// stored them, are read through the host's zone of that name.
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
// That instance is read at the offset before the gap (RFC 5545 §3.3.5) and
// counted like any other, so the series runs 15, 22 and 29 March and 5 April.
func weeklySkippedSeries() []string {
	return []string{
		"UID:series", "DTSTAMP:20260101T000000Z",
		"DTSTART;TZID=@:20260315T023000", "DTEND;TZID=@:20260315T033000",
		"RRULE:FREQ=WEEKLY;COUNT=4",
	}
}

func TestCalendarQueryMatchesTheInstanceOnASkippedLocalTime(t *testing.T) {
	requireHostZones(t, "Europe/Berlin")
	for _, fixture := range skippedTimeFixtures() {
		t.Run(fixture.name, func(t *testing.T) {
			event := store.Event{UID: "series", RawICAL: fixture.object(t, weeklySkippedSeries()...)}
			for _, tt := range []struct {
				start, end string
				want       bool
			}{
				{"20260322T000000Z", "20260323T000000Z", true},
				{"20260329T000000Z", "20260330T000000Z", true},
				{"20260405T000000Z", "20260406T000000Z", true},
				{"20260412T000000Z", "20260413T000000Z", false},
				{"20260419T000000Z", "20260420T000000Z", false},
			} {
				if got := filterMatches(t, event, calQueryWithTimeRange(tt.start, tt.end), floatingZone{}); got != tt.want {
					t.Errorf("time-range %s/%s matched = %v, want %v", tt.start, tt.end, got, tt.want)
				}
			}
		})
	}
}

func TestExpandMatchesTheInstanceOnASkippedLocalTime(t *testing.T) {
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
				"RECURRENCE-ID:20260329T013000Z", "RECURRENCE-ID:20260405T003000Z",
			} {
				if !strings.Contains(body, want) {
					t.Errorf("expanded output lacks %s:\n%s", want, body)
				}
			}
			if strings.Contains(body, "20260412T") || strings.Count(body, "RECURRENCE-ID") != 4 {
				t.Errorf("expanded output carries a fifth instance past the series' COUNT:\n%s", body)
			}
		})
	}
}

func TestFreeBusyPublishesTheInstanceOnASkippedLocalTime(t *testing.T) {
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
		"FREEBUSY:20260329T013000Z/20260329T023000Z",
		"FREEBUSY:20260405T003000Z/20260405T013000Z",
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

// CALDAV:max-instances counts the instances the set has, and a daily 02:30
// Berlin series counts a day inside the spring gap like any other, read at
// the offset before it: 1000 days from 1 January 2026 through 26 September
// 2028 hold exactly the limit, and one more day is over it.
func TestMaxInstancesCountsTheSkippedLocalTimeAsAnInstance(t *testing.T) {
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
			if _, fault := validateCalendarObjectForStorage(fixture.object(t, series("20280926T235959Z")...), nil); fault != nil {
				t.Errorf("a set of 1000 instances was refused: %+v", fault)
			}
			_, fault := validateCalendarObjectForStorage(fixture.object(t, series("20280927T235959Z")...), nil)
			if fault == nil || len(fault.conditions) != 1 || fault.conditions[0] != "max-instances" {
				t.Errorf("a set of 1001 instances got %+v, want max-instances", fault)
			}
		})
	}
}

// A series in a zone the host defines is counted toward CALDAV:max-instances
// on the wall clocks its expansion generates, resolved to the instants the
// expansion resolves them to. A series of exactly the limit is stored, an
// override of one of its instances replaces that instance rather than adding
// one, and one instance more is refused -- whether the series starts on a
// local time a spring change skips or runs through one a fall change repeats.
func TestValidateCalendarObjectCountsAZonedSeriesAsItsExpansion(t *testing.T) {
	requireHostZones(t, "America/New_York", "Europe/Berlin")
	tests := []struct {
		name     string
		tzid     string
		dtstart  string
		override string
	}{
		{
			name:     "DTSTART on a skipped local time, one instance moved",
			tzid:     "America/New_York",
			dtstart:  "20240310T023000",
			override: "20240315T023000",
		},
		{
			name:     "an instance on a repeated local time moved",
			tzid:     "Europe/Berlin",
			dtstart:  "20241025T023000",
			override: "20241027T023000",
		},
		{
			name:    "DTSTART on a repeated local time",
			tzid:    "Europe/Berlin",
			dtstart: "20241027T023000",
		},
	}
	object := func(tzid, dtstart, override string, count int) string {
		master := buildComponent("VEVENT", "UID:series", "DTSTAMP:20240101T000000Z",
			"DTSTART;TZID="+tzid+":"+dtstart, "DURATION:PT1H", fmt.Sprintf("RRULE:FREQ=DAILY;COUNT=%d", count))
		if override == "" {
			return buildCalendarObject(master)
		}
		moved := buildComponent("VEVENT", "UID:series", "DTSTAMP:20240101T000000Z",
			"RECURRENCE-ID;TZID="+tzid+":"+override, "DTSTART;TZID="+tzid+":"+override[:9]+"120000", "DURATION:PT1H")
		return buildCalendarObject(master, moved)
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if _, err := ValidateCalendarObject(object(tt.tzid, tt.dtstart, tt.override, ical.MaxRecurrenceInstances), nil); err != nil {
				t.Errorf("a series of %d instances refused: %v", ical.MaxRecurrenceInstances, err)
			}
			_, err := ValidateCalendarObject(object(tt.tzid, tt.dtstart, tt.override, ical.MaxRecurrenceInstances+1), nil)
			want := calobject.Error{Status: http.StatusForbidden, Conditions: []string{"max-instances"}}
			var refusal *calobject.Error
			if !errors.As(err, &refusal) || refusal.Status != want.Status || !reflect.DeepEqual(refusal.Conditions, want.Conditions) {
				t.Errorf("a series of %d instances: error = %v, want %d %v", ical.MaxRecurrenceInstances+1, err, want.Status, want.Conditions)
			}
		})
	}
}

// RFC 5545 §3.3.10 requires a UTC UNTIL beside a DTSTART with a TZID, and the
// validator refuses a floating one, but a series stored with one is still read
// with that UNTIL on the DTSTART's wall clock, whatever zone the request reads
// floating values in, so its last instance is reported and none after it.
func TestStoredFloatingUntilBesideAZonedDTStartEndsOnTheDTStartsWallClock(t *testing.T) {
	requireHostZones(t, "America/New_York", "Asia/Tokyo")
	tokyo, _ := ical.HostLocation("Asia/Tokyo")
	// The last instance, 02:30 on 12 March in New York, is 06:30Z.
	event := store.Event{UID: "series", RawICAL: buildCalendarObject(buildComponent("VEVENT", "UID:series", "DTSTAMP:20240101T000000Z",
		"DTSTART;TZID=America/New_York:20240308T023000", "DURATION:PT1H", "RRULE:FREQ=DAILY;UNTIL=20240312T023000"))}
	for name, zone := range map[string]floatingZone{"no calendar time zone": {}, "calendar time zone Asia/Tokyo": {defined: true, loc: tokyo}} {
		if !filterMatches(t, event, calQueryWithTimeRange("20240312T060000Z", "20240312T070000Z"), zone) {
			t.Errorf("%s: calendar-query missed the series' last instance", name)
		}
		if filterMatches(t, event, calQueryWithTimeRange("20240313T060000Z", "20240313T070000Z"), zone) {
			t.Errorf("%s: calendar-query matched an instance on the day after the series' last", name)
		}
	}
}

// An override whose RECURRENCE-ID names the skipped Sunday names the slot the
// master generates there, read at the offset before the gap, so it
// replaces that instance exactly as an override of an ordinary slot does:
// COUNT=2 is satisfied by 22 and 29 March, with the override moving the
// second rather than adding a third.
func TestOverrideOfASkippedInstanceReplacesTheGeneratedSlot(t *testing.T) {
	requireHostZones(t, "Europe/Berlin")
	raw := wrapCalendar(append(append(append([]string{"VERSION:2.0", "PRODID:-//Client//EN"}, berlinTimezone("Europe/Berlin")...),
		componentLines("VEVENT", "UID:series", "DTSTAMP:20260101T000000Z",
			"DTSTART;TZID=Europe/Berlin:20260322T023000", "DTEND;TZID=Europe/Berlin:20260322T033000",
			"RRULE:FREQ=WEEKLY;COUNT=2")...),
		componentLines("VEVENT", "UID:series", "DTSTAMP:20260101T000000Z",
			"RECURRENCE-ID;TZID=Europe/Berlin:20260329T023000",
			"DTSTART;TZID=Europe/Berlin:20260329T100000", "DTEND;TZID=Europe/Berlin:20260329T110000")...)...)

	event := store.Event{UID: "series", RawICAL: raw}
	if !filterMatches(t, event, calQueryWithTimeRange("20260329T070000Z", "20260329T090000Z"), floatingZone{}) {
		t.Error("calendar-query missed the override at its own time")
	}
	if filterMatches(t, event, calQueryWithTimeRange("20260405T000000Z", "20260406T000000Z"), floatingZone{}) {
		t.Error("calendar-query matched a third instance past the series' COUNT")
	}

	code, body := zeroLengthReport(t, raw, calendarDataQuery(`<C:expand start="20260301T000000Z" end="20260501T000000Z"/>`))
	if code != http.StatusMultiStatus {
		t.Fatalf("status = %d, body %s", code, body)
	}
	for _, want := range []string{
		"RECURRENCE-ID:20260322T013000Z",
		"RECURRENCE-ID:20260329T013000Z", "DTSTART:20260329T080000Z",
	} {
		if !strings.Contains(body, want) {
			t.Errorf("expanded output lacks %s:\n%s", want, body)
		}
	}
	if strings.Contains(body, "20260405T") || strings.Count(body, "RECURRENCE-ID") != 2 {
		t.Errorf("expanded output carries a third instance past the series' COUNT:\n%s", body)
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
	if !filterMatches(t, event, calQueryWithTimeRange("20260329T013000Z", "20260329T020000Z"), floatingZone{}) {
		t.Error("calendar-query missed the 29 March instance the shipped definition does not skip")
	}
	if filterMatches(t, event, calQueryWithTimeRange("20260412T000000Z", "20260413T000000Z"), floatingZone{}) {
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
	if !filterMatches(t, event, calQueryWithTimeRange("20260329T000000Z", "20260329T010000Z"), floatingZone{}) {
		t.Error("calendar-query missed the 01:30 instance on 29 March, outside the stated gap")
	}
	if filterMatches(t, event, calQueryWithTimeRange("20260330T220000Z", "20260331T030000Z"), floatingZone{}) {
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
	// 2026 has 365 days, one of them the 29 March the zone skips 02:30 on,
	// which is still an instance, read at the offset before the gap.
	if instances != 365 {
		t.Fatalf("expanded %d instances, want 365", instances)
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

// A floating timed series whose third occurrence ends on a local time the
// zone's autumn change repeats renders that end moved forward from its own
// written DTSTART, not equal to it: RFC 5545 §3.3.5 reads the DTEND the
// occurrence's shift carries it to at the offset the repeated wall clock's
// first occurrence has, and the spelling written back is civil arithmetic on
// what was written -- the master's own DTEND moved by the occurrence's shift
// -- which no zone transition can turn backward to the DTSTART's own
// spelling. Expand and calendar-query agree on the instance. Santiago's
// autumn change, and Iran's abolished one still in the 2021 tzdata, both
// repeat an hour at a wall clock other than midnight.
func TestExpandRendersAFloatingEndOnARepeatedHourForward(t *testing.T) {
	tests := []struct {
		name                    string
		tzid                    string
		definitionFrom, through time.Time
		dtstart, dtend          string
		count                   int
		wantDTStart, wantDTEnd  string
		probeStart, probeEnd    string
	}{
		{
			name: "Santiago", tzid: "America/Santiago",
			definitionFrom: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC), through: time.Date(2026, 12, 31, 0, 0, 0, 0, time.UTC),
			// 2 April 23:00 is before the autumn change; the third
			// occurrence, 4 April 23:00 to 5 April 00:00, straddles it.
			dtstart: "20260402T230000", dtend: "20260403T000000", count: 4,
			wantDTStart: "20260404T230000", wantDTEnd: "20260405T000000",
			probeStart: "20260405T025900Z", probeEnd: "20260405T030100Z",
		},
		{
			name: "Tehran", tzid: "Asia/Tehran",
			definitionFrom: time.Date(2021, 1, 1, 0, 0, 0, 0, time.UTC), through: time.Date(2021, 12, 31, 0, 0, 0, 0, time.UTC),
			// 19 September 23:00 is before the autumn change; the third
			// occurrence, 21 September 23:00 to 22 September 00:00,
			// straddles it.
			dtstart: "20210919T230000", dtend: "20210920T000000", count: 4,
			wantDTStart: "20210921T230000", wantDTEnd: "20210922T000000",
			probeStart: "20210921T182900Z", probeEnd: "20210921T183100Z",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			requireHostZones(t, tt.tzid)
			definition, ok := ical.VTimezone(tt.tzid, tt.definitionFrom, tt.through)
			if !ok {
				t.Fatalf("no definition of %s", tt.tzid)
			}
			master := componentLines("VEVENT", "UID:fallback", "DTSTAMP:20260101T000000Z",
				"DTSTART:"+tt.dtstart, "DTEND:"+tt.dtend, fmt.Sprintf("RRULE:FREQ=DAILY;COUNT=%d", tt.count))
			raw := wrapCalendar(append([]string{"VERSION:2.0", "PRODID:-//Client//EN"}, master...)...)
			timezone := "<C:timezone>BEGIN:VCALENDAR&#13;\nVERSION:2.0&#13;\nPRODID:-//test//EN&#13;\n" +
				strings.ReplaceAll(clientReading(definition), "\r\n", "&#13;\n") + "END:VCALENDAR&#13;\n</C:timezone>"
			expandBody := `<C:calendar-query xmlns:D="DAV:" xmlns:C="urn:ietf:params:xml:ns:caldav">` +
				`<D:prop><C:calendar-data><C:expand start="` + tt.definitionFrom.Format("20060102T150405Z") + `" end="` + tt.through.Format("20060102T150405Z") + `"/></C:calendar-data></D:prop>` +
				`<C:filter><C:comp-filter name="VCALENDAR"><C:comp-filter name="VEVENT"/></C:comp-filter></C:filter>` +
				timezone + `</C:calendar-query>`
			code, body := zeroLengthReport(t, raw, expandBody)
			if code != http.StatusMultiStatus {
				t.Fatalf("status = %d, body %s", code, body)
			}
			if !strings.Contains(body, "DTSTART:"+tt.wantDTStart) {
				t.Errorf("expanded output lacks the expected DTSTART:\n%s", body)
			}
			if !strings.Contains(body, "DTEND:"+tt.wantDTEnd) {
				t.Errorf("expanded output's DTEND did not move forward past the repeated hour:\n%s", body)
			}
			if strings.Contains(body, "DTEND:"+tt.wantDTStart) {
				t.Errorf("expanded output's DTEND equals its DTSTART:\n%s", body)
			}

			event := store.Event{UID: "fallback", RawICAL: raw}
			zone := floatingZone{defined: true, observances: func() *ical.IndexedTimezoneObservances {
				root, err := parseICalendarObject(wrapCalendar(append([]string{"VERSION:2.0", "PRODID:-//test//EN"},
					strings.Split(strings.TrimSuffix(clientReading(definition), "\r\n"), "\r\n")...)...))
				if err != nil {
					t.Fatal(err)
				}
				observances, ok := submittedTimezoneObservances(root, tt.tzid)
				if !ok {
					t.Fatalf("could not read the generated %s definition", tt.tzid)
				}
				return observances
			}()}
			if !filterMatches(t, event, calQueryWithTimeRange(tt.probeStart, tt.probeEnd), zone) {
				t.Error("calendar-query missed the occurrence straddling the repeated hour")
			}
		})
	}
}

// A cancelled RANGE=THISANDFUTURE override whose RECURRENCE-ID names a local
// time a transition skips removes that slot and every later one exactly as it
// would an ordinary date's: the slot is one the master generates, read at the
// offset before the gap (RFC 5545 §3.3.5), not one no instance stands for, so
// the cancellation suppresses it the way it suppresses any other slot rather
// than leaving it standing alone.
func TestCancelledThisAndFutureOverrideOnASkippedLocalTimeSuppressesFollowingInstances(t *testing.T) {
	requireHostZones(t, "Europe/Berlin")
	raw := instancesObject("",
		"DTSTART;TZID=Europe/Berlin:20260315T023000\r\nDTEND;TZID=Europe/Berlin:20260315T033000\r\nRRULE:FREQ=WEEKLY;COUNT=4\r\n",
		"RECURRENCE-ID;RANGE=THISANDFUTURE;TZID=Europe/Berlin:20260329T023000\r\nSTATUS:CANCELLED\r\n")
	march := []time.Time{time.Date(2026, 3, 1, 0, 0, 0, 0, time.UTC), time.Date(2026, 5, 1, 0, 0, 0, 0, time.UTC)}

	got := EventInstances(raw, march[0], march[1], nil)
	if len(got.Instances) != 2 || len(got.Overrides) != 0 {
		t.Fatalf("EventInstances(, nil) lists %+v and places %+v, want the first two instances alone", got.Instances, got.Overrides)
	}
	if !got.Instances[0].Start.Equal(mustUTC(t, "20260315T013000Z")) || !got.Instances[1].Start.Equal(mustUTC(t, "20260322T013000Z")) {
		t.Errorf("EventInstances(, nil) lists %+v, want 15 and 22 March", got.Instances)
	}

	code, body := zeroLengthReport(t, raw, calendarDataQuery(`<C:expand start="20260301T000000Z" end="20260501T000000Z"/>`))
	if code != http.StatusMultiStatus {
		t.Fatalf("expand status = %d, body %s", code, body)
	}
	if strings.Count(body, "BEGIN:VEVENT") != 2 || strings.Contains(body, "20260329T") || strings.Contains(body, "20260405T") {
		t.Errorf("expansion writes other than the first two instances:\n%s", body)
	}

	event := store.Event{UID: "series", RawICAL: raw}
	if filterMatches(t, event, calQueryWithTimeRange("20260329T000000Z", "20260330T000000Z"), floatingZone{}) {
		t.Error("calendar-query matched the cancelled slot")
	}
	if filterMatches(t, event, calQueryWithTimeRange("20260405T000000Z", "20260406T000000Z"), floatingZone{}) {
		t.Error("calendar-query matched an instance past the cancellation")
	}
}

// A TZID padded with white space inside its quotes resolves, and finds its
// gaps, exactly as the unpadded spelling does: calendar-query, expand and
// free-busy all read the series the same way crossing the same spring gap.
func TestPaddedTZIDCrossesAGapLikeTheUnpaddedSpellingInDAV(t *testing.T) {
	requireHostZones(t, "Europe/Berlin")
	padded := func(_ *testing.T, properties ...string) string {
		lines := append([]string{"VERSION:2.0", "PRODID:-//Client//EN"}, berlinTimezone("Europe/Berlin")...)
		for i := range properties {
			properties[i] = strings.ReplaceAll(properties[i], "TZID=@", `TZID=" Europe/Berlin"`)
		}
		lines = append(lines, componentLines("VEVENT", properties...)...)
		return wrapCalendar(lines...)
	}
	raw := padded(t, weeklySkippedSeries()...)
	event := store.Event{UID: "series", RawICAL: raw}

	for _, tt := range []struct {
		start, end string
		want       bool
	}{
		{"20260322T000000Z", "20260323T000000Z", true},
		{"20260329T000000Z", "20260330T000000Z", true},
		{"20260405T000000Z", "20260406T000000Z", true},
		{"20260412T000000Z", "20260413T000000Z", false},
	} {
		if got := filterMatches(t, event, calQueryWithTimeRange(tt.start, tt.end), floatingZone{}); got != tt.want {
			t.Errorf("padded TZID: time-range %s/%s matched = %v, want %v", tt.start, tt.end, got, tt.want)
		}
	}

	code, body := zeroLengthReport(t, raw, calendarDataQuery(`<C:expand start="20260301T000000Z" end="20260501T000000Z"/>`))
	if code != http.StatusMultiStatus {
		t.Fatalf("padded TZID: expand status = %d, body %s", code, body)
	}
	for _, want := range []string{
		"RECURRENCE-ID:20260315T013000Z", "RECURRENCE-ID:20260322T013000Z",
		"RECURRENCE-ID:20260329T013000Z", "RECURRENCE-ID:20260405T003000Z",
	} {
		if !strings.Contains(body, want) {
			t.Errorf("padded TZID: expanded output lacks %s:\n%s", want, body)
		}
	}

	calRepo := &fakeCalendarRepo{accessible: []store.CalendarAccess{
		{Calendar: store.Calendar{ID: 1, UserID: 1, Name: "Work"}, Editor: true},
	}}
	h := &DavServer{store: &store.Store{Calendars: calRepo, Events: &fakeEventRepo{
		events: map[string]*store.Event{"1:series": {CalendarID: 1, UID: "series", ResourceName: "series", ETag: "e1", RawICAL: raw}},
	}}}
	rr := httptest.NewRecorder()
	h.Report(rr, freeBusyRequestFor("/dav/calendars/1/", `<C:free-busy-query xmlns:C="urn:ietf:params:xml:ns:caldav">
  <C:time-range start="20260301T000000Z" end="20260501T000000Z"/>
</C:free-busy-query>`, &store.User{ID: 1}))
	if rr.Code != http.StatusOK {
		t.Fatalf("padded TZID: free-busy status = %d: %s", rr.Code, rr.Body.String())
	}
	assertPublishedFreeBusy(t, parsedFreeBusyLines(t, rr.Body.String()), []string{
		"FREEBUSY:20260315T013000Z/20260315T023000Z",
		"FREEBUSY:20260322T013000Z/20260322T023000Z",
		"FREEBUSY:20260329T013000Z/20260329T023000Z",
		"FREEBUSY:20260405T003000Z/20260405T013000Z",
	})
}

// A 45-minute MINUTELY interval crossing New York's spring change offers its
// candidates just before, inside and after the gap at 07:00Z, 07:45Z and
// 07:30Z: the one past the gap reads earlier than the one inside it.
// calendar-query's own time-range walk must still find each by its instant,
// not stop early on the assumption that candidates arrive in ascending
// order of wall clock.
func TestCalendarQueryMatchesEachInstantOfAnIntervalReorderedByAGap(t *testing.T) {
	requireHostZones(t, "America/New_York")
	event := store.Event{UID: "series", RawICAL: wrapCalendar(append([]string{"VERSION:2.0", "PRODID:-//Client//EN"},
		componentLines("VEVENT", "UID:series", "DTSTAMP:20260101T000000Z",
			"DTSTART;TZID=America/New_York:20260307T221500", "DURATION:PT1M",
			"RRULE:FREQ=MINUTELY;INTERVAL=45;COUNT=40")...)...)}
	for _, tt := range []struct {
		instant string
		want    bool
	}{
		{"20260308T070000Z", true},
		{"20260308T073000Z", true},
		{"20260308T074500Z", true},
		{"20260308T071500Z", false},
		{"20260308T072000Z", false},
	} {
		start, _ := time.Parse("20060102T150405Z", tt.instant)
		end := start.Add(time.Minute)
		if got := filterMatches(t, event, calQueryWithTimeRange(start.Format("20060102T150405Z"), end.Format("20060102T150405Z")), floatingZone{}); got != tt.want {
			t.Errorf("time-range at %s matched = %v, want %v", tt.instant, got, tt.want)
		}
	}
}

// A candidate inside a gap reads later than the ones just past it, so neither
// UNTIL nor a narrow time-range may end the walk at the first candidate whose
// instant passes it. These 40-minute series cross New York's spring change:
// the first reads 02:20 EST as 07:20Z, past its UNTIL, and 03:00 EDT after it
// as 07:00Z; the second starts inside the gap and ends past the window.
func TestReadersFindTheInstancesAGapReordersAroundUNTILAndAWindow(t *testing.T) {
	requireHostZones(t, "America/New_York")
	untilSeries := store.Event{UID: "series", RawICAL: wrapCalendar(append([]string{"VERSION:2.0", "PRODID:-//Client//EN"},
		componentLines("VEVENT", "UID:series", "DTSTAMP:20260101T000000Z",
			"DTSTART;TZID=America/New_York:20260308T010000", "DURATION:PT1M",
			"RRULE:FREQ=MINUTELY;INTERVAL=40;UNTIL=20260308T071000Z")...)...)}
	countedSeries := store.Event{UID: "series", RawICAL: wrapCalendar(append([]string{"VERSION:2.0", "PRODID:-//Client//EN"},
		componentLines("VEVENT", "UID:series", "DTSTAMP:20260101T000000Z",
			"DTSTART;TZID=America/New_York:20260308T025000", "DURATION:PT1M",
			"RRULE:FREQ=MINUTELY;INTERVAL=40;COUNT=55")...)...)}
	for _, tt := range []struct {
		name         string
		event        store.Event
		start, end   string
		want         bool
		freeBusyLine string
	}{
		{"the instance after the gap that reads before UNTIL", untilSeries, "20260308T065900Z", "20260308T070100Z", true, "FREEBUSY:20260308T070000Z/20260308T070100Z"},
		{"the instance past UNTIL", untilSeries, "20260308T073900Z", "20260308T074100Z", false, ""},
		{"the last instance of a counted series that starts in the gap", countedSeries, "20260309T192000Z", "20260309T200000Z", true, "FREEBUSY:20260309T193000Z/20260309T193100Z"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			if got := filterMatches(t, tt.event, calQueryWithTimeRange(tt.start, tt.end), floatingZone{}); got != tt.want {
				t.Errorf("calendar-query time-range %s/%s matched = %v, want %v", tt.start, tt.end, got, tt.want)
			}
			calRepo := &fakeCalendarRepo{accessible: []store.CalendarAccess{
				{Calendar: store.Calendar{ID: 1, UserID: 1, Name: "Work"}, Editor: true},
			}}
			h := &DavServer{store: &store.Store{Calendars: calRepo, Events: &fakeEventRepo{
				events: map[string]*store.Event{"1:series": {CalendarID: 1, UID: "series", ResourceName: "series", ETag: "e1", RawICAL: tt.event.RawICAL}},
			}}}
			body := `<C:free-busy-query xmlns:C="urn:ietf:params:xml:ns:caldav"><C:time-range start="` + tt.start + `" end="` + tt.end + `"/></C:free-busy-query>`
			rr := httptest.NewRecorder()
			h.Report(rr, freeBusyRequestFor("/dav/calendars/1/", body, &store.User{ID: 1}))
			if rr.Code != http.StatusOK {
				t.Fatalf("status = %d, want 200: %s", rr.Code, rr.Body.String())
			}
			if got := strings.Contains(rr.Body.String(), "FREEBUSY:"); got != tt.want {
				t.Errorf("free-busy published = %v, want %v:\n%s", got, tt.want, rr.Body.String())
			}
			if tt.want && !strings.Contains(rr.Body.String(), tt.freeBusyLine) {
				t.Errorf("free-busy lacks %s:\n%s", tt.freeBusyLine, rr.Body.String())
			}
		})
	}
}

// newYorkTimezone is the United States' rules for Eastern Time since 2007
// under the given TZID, as a client ships them.
func newYorkTimezone(tzid string) []string {
	return []string{
		"BEGIN:VTIMEZONE", "TZID:" + tzid,
		"BEGIN:DAYLIGHT", "TZOFFSETFROM:-0500", "TZOFFSETTO:-0400", "TZNAME:EDT",
		"DTSTART:20070311T020000", "RRULE:FREQ=YEARLY;BYMONTH=3;BYDAY=2SU", "END:DAYLIGHT",
		"BEGIN:STANDARD", "TZOFFSETFROM:-0400", "TZOFFSETTO:-0500", "TZNAME:EST",
		"DTSTART:20071104T020000", "RRULE:FREQ=YEARLY;BYMONTH=11;BYDAY=1SU", "END:STANDARD",
		"END:VTIMEZONE",
	}
}

// A series starting at 03:15 EDT on the New York spring change whose rule
// lists 02:45, which the change skips and which reads 03:45 EDT, and 03:45
// itself: the two name one instant, one instance after the DTSTART (RFC 5545
// §3.8.5.3), which every reader reports at 07:45Z, and the COUNT of six ends on
// 9 March rather than reaching 10 March.
func TestEveryReaderKeepsAnExistingTimeASkippedCandidateMergesInto(t *testing.T) {
	requireHostZones(t, "America/New_York")
	series := []string{
		"UID:series", "DTSTAMP:20260101T000000Z",
		"DTSTART;TZID=America/New_York:20260308T031500", "DTEND;TZID=America/New_York:20260308T032500",
		"RRULE:FREQ=DAILY;BYHOUR=2,3;BYMINUTE=15,45;COUNT=6",
	}
	objects := []struct {
		name string
		raw  func(t *testing.T) string
	}{
		{name: "client VTIMEZONE", raw: func(*testing.T) string {
			lines := append([]string{"VERSION:2.0", "PRODID:-//Client//EN"}, newYorkTimezone("America/New_York")...)
			return wrapCalendar(append(lines, componentLines("VEVENT", series...)...)...)
		}},
		{name: "no VTIMEZONE", raw: func(*testing.T) string {
			return wrapCalendar(append([]string{"VERSION:2.0", "PRODID:-//Client//EN"}, componentLines("VEVENT", series...)...)...)
		}},
		{name: "definition the validator stores", raw: func(t *testing.T) string {
			t.Helper()
			validated, fault := validateCalendarObjectForStorage(
				wrapCalendar(append([]string{"VERSION:2.0", "PRODID:-//Client//EN"}, componentLines("VEVENT", series...)...)...), nil)
			if fault != nil {
				t.Fatalf("validator refused the object: %+v", fault)
			}
			return validated.Body
		}},
	}
	want := []string{
		"2026-03-08T07:15:00Z", "2026-03-08T07:45:00Z",
		"2026-03-09T06:15:00Z", "2026-03-09T06:45:00Z", "2026-03-09T07:15:00Z", "2026-03-09T07:45:00Z",
	}
	for _, object := range objects {
		t.Run(object.name, func(t *testing.T) {
			raw := object.raw(t)

			expansion := EventInstances(raw, time.Date(2026, 3, 1, 0, 0, 0, 0, time.UTC), time.Date(2026, 4, 1, 0, 0, 0, 0, time.UTC), nil)
			var starts []string
			for _, instance := range expansion.Instances {
				starts = append(starts, instance.Start.UTC().Format(time.RFC3339))
			}
			if !reflect.DeepEqual(starts, want) {
				t.Errorf("EventInstances() starts = %v, want %v", starts, want)
			}

			event := store.Event{UID: "series", RawICAL: raw}
			for _, tt := range []struct {
				start, end string
				want       bool
			}{
				{"20260308T074000Z", "20260308T075000Z", true},
				{"20260310T000000Z", "20260311T000000Z", false},
			} {
				if got := filterMatches(t, event, calQueryWithTimeRange(tt.start, tt.end), floatingZone{}); got != tt.want {
					t.Errorf("time-range %s/%s matched = %v, want %v", tt.start, tt.end, got, tt.want)
				}
			}

			code, body := zeroLengthReport(t, raw, calendarDataQuery(`<C:expand start="20260301T000000Z" end="20260401T000000Z"/>`))
			if code != http.StatusMultiStatus {
				t.Fatalf("expand status = %d, body %s", code, body)
			}
			if !strings.Contains(body, "RECURRENCE-ID:20260308T074500Z") {
				t.Errorf("expanded output lacks the 03:45 EDT instance:\n%s", body)
			}
			if strings.Contains(body, "20260310T") || strings.Count(body, "RECURRENCE-ID") != 6 {
				t.Errorf("expanded output carries an instance past the series' COUNT:\n%s", body)
			}
		})
	}
}

// decimals is the comma-separated list of the integers from 0 to count-1,
// excluding skip.
func decimals(count, skip int) string {
	values := make([]string, 0, count)
	for i := range count {
		if i != skip {
			values = append(values, fmt.Sprint(i))
		}
	}
	return strings.Join(values, ",")
}

// Deciding whether a rule generates its DTSTART stops at the first candidate
// that reads an instant after the DTSTART's, not at the end of the next day the
// rule admits: a rule with almost the period's whole work limit in candidates
// on each such day is otherwise refused, in a zone, as exceeding it, although
// the same rule from a UTC DTSTART is accepted with its five instances.
func TestRuleWithADenseDayIsDecidedInAZoneAsInUTC(t *testing.T) {
	requireHostZones(t, "America/New_York")
	for _, tt := range []struct{ name, dtstart, frequency string }{
		{"weekly in a zone", "DTSTART;TZID=America/New_York:20260302T000030", "FREQ=WEEKLY;BYDAY=MO,TU"},
		{"monthly in a zone", "DTSTART;TZID=America/New_York:20260302T000030", "FREQ=MONTHLY;BYMONTHDAY=2,3"},
		{"weekly in UTC", "DTSTART:20260302T050030Z", "FREQ=WEEKLY;BYDAY=MO,TU"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			rule := tt.frequency + ";BYHOUR=" + decimals(24, -1) + ";BYMINUTE=" + decimals(60, -1) + ";BYSECOND=" + decimals(60, 30) + ";COUNT=5"
			raw := wrapCalendar(append([]string{"VERSION:2.0", "PRODID:-//Client//EN"},
				componentLines("VEVENT", "UID:dense", "DTSTAMP:20260101T000000Z", tt.dtstart, "DURATION:PT1S", "RRULE:"+rule)...)...)
			if _, fault := validateCalendarObjectForStorage(raw, nil); fault != nil {
				t.Errorf("validation refused the object: %+v", *fault)
			}
			expansion := EventInstances(raw, time.Date(2026, 3, 1, 0, 0, 0, 0, time.UTC), time.Date(2026, 3, 4, 0, 0, 0, 0, time.UTC), nil)
			if len(expansion.Instances) != 5 || !expansion.Complete {
				t.Errorf("EventInstances() = %d instances, complete %v; want 5, complete", len(expansion.Instances), expansion.Complete)
			}
		})
	}
}
