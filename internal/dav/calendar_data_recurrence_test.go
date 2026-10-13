package dav

import (
	"errors"
	"fmt"
	"math"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/jw6ventures/calcard/internal/ical"
	"github.com/jw6ventures/calcard/internal/store"
)

// thisAndFutureOverridesObject is a daily rule from 1900 carrying count
// RANGE=THISANDFUTURE overrides, one per day from its start, each keeping its
// slot's time. The master carries a VALARM firing alarmLead before every
// instance, and is written after its overrides when masterLast is set, which
// RFC 5545 permits: a VCALENDAR orders its components however it likes.
func thisAndFutureOverridesObject(count int, alarmLead string, masterLast bool) string {
	master := "BEGIN:VEVENT\r\nUID:overridden\r\nDTSTAMP:20240101T000000Z\r\n" +
		"DTSTART:19000101T090000Z\r\nDTEND:19000101T100000Z\r\nRRULE:FREQ=DAILY\r\n" +
		"BEGIN:VALARM\r\nACTION:DISPLAY\r\nDESCRIPTION:reminder\r\nTRIGGER:-" + alarmLead + "\r\nEND:VALARM\r\n" +
		"END:VEVENT\r\n"
	var b strings.Builder
	b.WriteString("BEGIN:VCALENDAR\r\nVERSION:2.0\r\nPRODID:-//Test//EN\r\n")
	if !masterLast {
		b.WriteString(master)
	}
	first := time.Date(1900, 1, 1, 9, 0, 0, 0, time.UTC)
	for i := 0; i < count; i++ {
		slot := first.AddDate(0, 0, i)
		b.WriteString("BEGIN:VEVENT\r\nUID:overridden\r\nDTSTAMP:20240101T000000Z\r\n" +
			"RECURRENCE-ID;RANGE=THISANDFUTURE:" + slot.Format("20060102T150405Z") + "\r\n" +
			"DTSTART:" + slot.Format("20060102T150405Z") + "\r\n" +
			"DTEND:" + slot.Add(time.Hour).Format("20060102T150405Z") + "\r\n" +
			"END:VEVENT\r\n")
	}
	if masterLast {
		b.WriteString(master)
	}
	b.WriteString("END:VCALENDAR\r\n")
	return b.String()
}

// overrideSlotDay is midnight UTC of the day holding slot day of the rule the
// override fixtures above expand.
func overrideSlotDay(day int) time.Time {
	return time.Date(1900, 1, 1, 0, 0, 0, 0, time.UTC).AddDate(0, 0, day)
}

// expandOverDays runs the §9.6.5 expansion of raw over [from, from+days).
func expandOverDays(t *testing.T, raw string, from, days int) string {
	t.Helper()
	got, err := filterICalendarData(raw, newCalendarDataProjection(
		&calendarDataEl{Expand: &calendarRange{Start: overrideSlotDay(from), End: overrideSlotDay(from + days)}}, floatingZone{}))
	if err != nil {
		t.Fatalf("filterICalendarData() err = %v", err)
	}
	return got
}

// Every expanded instance takes its content from the RANGE=THISANDFUTURE
// override governing its slot, and both the instances and the overrides a
// resource carries can number in the thousands. Finding the override by walking
// the resource's components makes the expansion cost their product.
func TestCalendarDataExpandScalesWithManyThisAndFutureOverrides(t *testing.T) {
	const overrides = 250
	got := expandOverDays(t, thisAndFutureOverridesObject(overrides, "PT15M", false), overrides, overrides)
	if instances := strings.Count(got, "BEGIN:VEVENT"); instances != overrides {
		t.Fatalf("expanded %d instances, want %d", instances, overrides)
	}
	// The last override governs every slot of the range, yet each instance
	// names its own slot rather than the override's.
	lastSlot := overrideSlotDay(2*overrides - 1).Add(9 * time.Hour).Format("20060102T150405Z")
	if !strings.Contains(got, "RECURRENCE-ID:"+lastSlot) {
		t.Fatalf("the last instance in the range does not name its own slot %s", lastSlot)
	}

	// The larger run expands 1000 instances, the whole §9.6.5 instance budget.
	assertScalesLinearly(t, overrides, func(n int) func() {
		raw := thisAndFutureOverridesObject(n, "PT15M", false)
		return func() { expandOverDays(t, raw, n, n) }
	})
}

// alarmQueryServer holds the override fixture with n overrides and an alarm
// firing n/2 days before each instance.
func alarmQueryServer(n int) *DavServer {
	calRepo := &fakeCalendarRepo{accessible: []store.CalendarAccess{
		{Calendar: store.Calendar{ID: 1, UserID: 1, Name: "Work"}, Editor: true},
	}}
	return &DavServer{store: &store.Store{Calendars: calRepo, Events: &fakeEventRepo{events: map[string]*store.Event{
		"1:overridden": {
			CalendarID: 1, UID: "overridden", ResourceName: "overridden", ETag: "e1",
			RawICAL: thisAndFutureOverridesObject(n, fmt.Sprintf("P%dD", n/2), false),
		},
	}}}}
}

// alarmQueryMatches asks whether an alarm fires on day n + n/2 of the rule.
// Only the instance on day 2n has one there, and every instance from day n on
// could, so the query visits about n instances before it matches.
func alarmQueryMatches(t *testing.T, h *DavServer, n int) bool {
	t.Helper()
	day := overrideSlotDay(n + n/2)
	body := `<C:calendar-query xmlns:D="DAV:" xmlns:C="urn:ietf:params:xml:ns:caldav">
  <D:prop><D:getetag/></D:prop>
  <C:filter>
    <C:comp-filter name="VCALENDAR">
      <C:comp-filter name="VEVENT">
        <C:comp-filter name="VALARM">
          <C:time-range start="` + day.Format("20060102T150405Z") + `" end="` + day.AddDate(0, 0, 1).Format("20060102T150405Z") + `"/>
        </C:comp-filter>
      </C:comp-filter>
    </C:comp-filter>
  </C:filter>
</C:calendar-query>`
	rr := httptest.NewRecorder()
	h.Report(rr, freeBusyRequestFor("/dav/calendars/1/", body, &store.User{ID: 1}))
	if rr.Code != http.StatusMultiStatus {
		t.Fatalf("status = %d, want 207: %.500s", rr.Code, rr.Body.String())
	}
	return strings.Contains(rr.Body.String(), "overridden")
}

// A VALARM time-range is tested against every instance whose alarm could reach
// the range, and each instance is built from the override governing it. With a
// long trigger lead most of those instances are visited without matching, so
// the lookup is paid once per visited instance.
func TestCalendarQueryAlarmScanScalesWithManyThisAndFutureOverrides(t *testing.T) {
	const overrides = 200
	if !alarmQueryMatches(t, alarmQueryServer(overrides), overrides) {
		t.Fatal("the resource whose alarm fires in the range was not matched")
	}

	// The larger run visits about 800 instances, inside the 1002 the §9.9
	// scan generates before reporting its answer unsettled.
	assertScalesLinearly(t, overrides, func(n int) func() {
		h := alarmQueryServer(n)
		return func() { alarmQueryMatches(t, h, n) }
	})
}

// §9.6.5 expansion leaves out each RANGE=THISANDFUTURE override that has a
// master to carry it, which asks for the master once per override. A master
// written after its overrides must not make that a walk over the resource per
// override.
func TestCalendarDataExpandScalesWithTheMasterAfterItsOverrides(t *testing.T) {
	const overrides = 5_000
	got := expandOverDays(t, thisAndFutureOverridesObject(overrides, "PT15M", true), overrides, 7)
	if instances := strings.Count(got, "BEGIN:VEVENT"); instances != 7 {
		t.Fatalf("expanded %d instances, want 7", instances)
	}

	assertScalesLinearly(t, overrides, func(n int) func() {
		raw := thisAndFutureOverridesObject(n, "PT15M", true)
		return func() { expandOverDays(t, raw, n, 7) }
	})
}

// limitRecurrenceSetOverWeek runs §9.6.6 over the week from the last override
// of an n-override fixture.
func limitRecurrenceSetOverWeek(t *testing.T, raw string, n int) string {
	t.Helper()
	got, err := filterICalendarData(raw, newCalendarDataProjection(
		&calendarDataEl{LimitRecurrenceSet: &calendarRange{Start: overrideSlotDay(n - 1), End: overrideSlotDay(n + 6)}}, floatingZone{}))
	if err != nil {
		t.Fatalf("filterICalendarData() err = %v", err)
	}
	return got
}

// §9.6.6 keeps a RANGE=THISANDFUTURE override when an instance it governs
// reaches the range, which is a question about the master's recurrence slots
// over that range. The slots are the same for every override asking, so a
// resource carrying many overrides must not re-expand the master for each.
func TestCalendarDataLimitRecurrenceSetScalesWithManyThisAndFutureOverrides(t *testing.T) {
	const overrides = 100
	got := limitRecurrenceSetOverWeek(t, thisAndFutureOverridesObject(overrides, "PT15M", false), overrides)
	// The master, and the last override: it governs every slot of the range.
	if components := strings.Count(got, "BEGIN:VEVENT"); components != 2 {
		t.Fatalf("kept %d components, want the master and the governing override", components)
	}

	assertScalesLinearly(t, overrides, func(n int) func() {
		raw := thisAndFutureOverridesObject(n, "PT15M", false)
		return func() { limitRecurrenceSetOverWeek(t, raw, n) }
	})
}

// thisAndFutureDateOverridesObject is an all-day series of every other day
// from 1 January 2020, n/2 of them, carrying a RANGE=THISANDFUTURE override
// for each of the n days from its start: those on the series' days name slots
// it generates, and those between name slots it never does, which stand for
// themselves.
func thisAndFutureDateOverridesObject(n int) string {
	var b strings.Builder
	b.WriteString("BEGIN:VCALENDAR\r\nVERSION:2.0\r\nPRODID:-//Test//EN\r\n")
	fmt.Fprintf(&b, "BEGIN:VEVENT\r\nUID:overridden\r\nDTSTAMP:20240101T000000Z\r\nDTSTART;VALUE=DATE:20200101\r\nRRULE:FREQ=DAILY;INTERVAL=2;COUNT=%d\r\nEND:VEVENT\r\n", n/2)
	first := time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC)
	for i := 0; i < n; i++ {
		day := first.AddDate(0, 0, i).Format("20060102")
		b.WriteString("BEGIN:VEVENT\r\nUID:overridden\r\nDTSTAMP:20240101T000000Z\r\n" +
			"RECURRENCE-ID;RANGE=THISANDFUTURE;VALUE=DATE:" + day + "\r\nDTSTART;VALUE=DATE:" + day + "\r\nSUMMARY:Later\r\nEND:VEVENT\r\n")
	}
	b.WriteString("END:VCALENDAR\r\n")
	return b.String()
}

// Whether a RANGE=THISANDFUTURE override names a slot its series generates
// decides whether expansion writes it as itself, and a resource can carry
// thousands of them. The series is asked about all of them at once, not
// expanded again for each.
func TestThisAndFutureOverridesStandingForThemselvesScaleWithTheirNumber(t *testing.T) {
	const overrides = 250
	from, to := time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC), time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC).AddDate(0, 0, 4*overrides)
	expand := func(t *testing.T, raw string) string {
		t.Helper()
		got, err := filterICalendarData(raw, newCalendarDataProjection(&calendarDataEl{Expand: &calendarRange{Start: from, End: to}}, floatingZone{}))
		if err != nil {
			t.Fatalf("filterICalendarData() err = %v", err)
		}
		return got
	}
	raw := thisAndFutureDateOverridesObject(overrides)
	// Each generated slot is an instance, and each override between them
	// stands for itself.
	if components := strings.Count(expand(t, raw), "BEGIN:VEVENT"); components != overrides {
		t.Fatalf("expanded %d components, want %d", components, overrides)
	}
	if got := EventInstances(raw, from, to); len(got.Instances) != overrides/2 || len(got.Overrides) != overrides/2 {
		t.Fatalf("EventInstances() lists %d instances and places %d overrides, want %d of each", len(got.Instances), len(got.Overrides), overrides/2)
	}

	t.Run("expand", func(t *testing.T) {
		assertScalesLinearly(t, overrides, func(n int) func() {
			raw := thisAndFutureDateOverridesObject(n)
			return func() { expand(t, raw) }
		})
	})
	t.Run("EventInstances", func(t *testing.T) {
		assertScalesLinearly(t, overrides, func(n int) func() {
			raw := thisAndFutureDateOverridesObject(n)
			return func() { EventInstances(raw, from, to) }
		})
	})
	// A day apart on a series of every minute, too many slots lie between
	// the overrides to generate them all, and each override's slot is
	// found on its own.
	t.Run("far apart on a dense series", func(t *testing.T) {
		const farApart = 100
		if got := EventInstances(farApartThisAndFutureOverridesObject(farApart), from, from.AddDate(0, 0, farApart)); len(got.Overrides) != 0 {
			t.Fatalf("EventInstances() places %d overrides, want none: each names a minute of the series", len(got.Overrides))
		}
		assertScalesLinearly(t, farApart, func(n int) func() {
			raw := farApartThisAndFutureOverridesObject(n)
			return func() { EventInstances(raw, from, from.AddDate(0, 0, n)) }
		})
	})
	// Each instance lasting days of a series repeating hourly, scores of
	// instances reach the time each override names.
	t.Run("long instances", func(t *testing.T) {
		const long = 60
		raw := longInstancesThisAndFutureOverridesObject(long)
		from, to := longInstancesFirst, longInstancesFirst.Add(2*long*time.Hour)
		if got := EventInstances(raw, from, to); len(got.Instances) != 2*long || len(got.Overrides) != long {
			t.Fatalf("EventInstances() lists %d instances and places %d overrides, want %d and %d", len(got.Instances), len(got.Overrides), 2*long, long)
		}
		assertScalesLinearly(t, long, func(n int) func() {
			raw := longInstancesThisAndFutureOverridesObject(n)
			return func() { EventInstances(raw, from, longInstancesFirst.Add(2*time.Duration(n)*time.Hour)) }
		})
	})
	// A COUNT rule the expansion cannot jump through is walked from its
	// start whatever the range, and the overrides are answered by that walk,
	// so the series costs what it does without them, over a range holding
	// one of them as over one holding every one.
	t.Run("a COUNT rule walked from its start", func(t *testing.T) {
		const counted = 60
		raw, alone := countedThisAndFutureOverridesObject(counted), countedThisAndFutureOverridesObject(0)
		for _, r := range []struct {
			name                 string
			to                   time.Time
			instances, overrides int
		}{
			{"two months", time.Date(1950, 3, 1, 0, 0, 0, 0, time.UTC), 2, 1},
			{"every instance", time.Date(2040, 1, 1, 0, 0, 0, 0, time.UTC), countedSeriesInstances - counted, counted},
		} {
			t.Run(r.name, func(t *testing.T) {
				from := countedSeriesFirst
				got := EventInstances(raw, from, r.to)
				if len(got.Instances) != r.instances || len(got.Overrides) != r.overrides {
					t.Fatalf("EventInstances() lists %d instances and places %d overrides, want %d and %d", len(got.Instances), len(got.Overrides), r.instances, r.overrides)
				}
				expanded, err := filterICalendarData(raw, newCalendarDataProjection(&calendarDataEl{Expand: &calendarRange{Start: from, End: r.to}}, floatingZone{}))
				if err != nil {
					t.Fatalf("filterICalendarData() err = %v", err)
				}
				if components := strings.Count(expanded, "BEGIN:VEVENT"); components != r.instances+r.overrides {
					t.Fatalf("expanded %d components, want %d", components, r.instances+r.overrides)
				}
				assertCostsAsMuchAsTheSeriesAlone(t,
					func() { EventInstances(raw, from, r.to) },
					func() { EventInstances(alone, from, r.to) })
			})
		}
	})
	// Cancelled overrides moved into the first week of a COUNT rule each name
	// a slot decades on that the series never generates. A cancelled override
	// moves no instance, so the walk over the week passes none of those slots,
	// and each is looked up: the rule is walked from its start once, through
	// the furthest of them, rather than once for each, and every override
	// stands for itself.
	t.Run("cancelled overrides naming slots decades on", func(t *testing.T) {
		const cancelled = 30
		week := []time.Time{time.Date(2000, 1, 1, 0, 0, 0, 0, time.UTC), time.Date(2000, 1, 8, 0, 0, 0, 0, time.UTC)}
		for name, object := range map[string]func(n int) string{
			"a rule picking one start a month": func(n int) string {
				return cancelledFarOverridesObject("", "DTSTART:20000101T000000Z", "FREQ=MONTHLY;BYDAY=MO,TU,WE,TH,FR,SA,SU;BYHOUR="+enumeratedHours()+";BYMINUTE=0,30;BYSETPOS=1;COUNT=999", n)
			},
			"a daily rule in a zone the object defines": func(n int) string {
				return cancelledFarOverridesObject(customEasternTimezone, "DTSTART;TZID=Custom/Eastern:20000101T000000", "FREQ=DAILY;COUNT=30000", n)
			},
		} {
			t.Run(name, func(t *testing.T) {
				raw := object(cancelled)
				got := EventInstances(raw, week[0], week[1])
				if !got.Complete || len(got.Overrides) != cancelled {
					t.Fatalf("EventInstances() places %d overrides, complete = %v, want all %d of a complete listing", len(got.Overrides), got.Complete, cancelled)
				}
				expanded, err := filterICalendarData(raw, newCalendarDataProjection(&calendarDataEl{Expand: &calendarRange{Start: week[0], End: week[1]}}, floatingZone{}))
				if err != nil {
					t.Fatalf("filterICalendarData() err = %v", err)
				}
				if written := strings.Count(expanded, "STATUS:CANCELLED"); written != cancelled {
					t.Fatalf("expansion writes %d cancelled overrides, want %d", written, cancelled)
				}
				assertScalesLinearly(t, cancelled, func(n int) func() {
					raw := object(n)
					return func() { EventInstances(raw, week[0], week[1]) }
				})
			})
		}
	})
}

// customEasternTimezone is a VTIMEZONE an object ships for a zone the host
// does not define, following New York's rules since 2007.
const customEasternTimezone = "BEGIN:VTIMEZONE\r\nTZID:Custom/Eastern\r\n" +
	"BEGIN:DAYLIGHT\r\nTZOFFSETFROM:-0500\r\nTZOFFSETTO:-0400\r\nTZNAME:EDT\r\nDTSTART:19700308T020000\r\nRRULE:FREQ=YEARLY;BYMONTH=3;BYDAY=2SU\r\nEND:DAYLIGHT\r\n" +
	"BEGIN:STANDARD\r\nTZOFFSETFROM:-0400\r\nTZOFFSETTO:-0500\r\nTZNAME:EST\r\nDTSTART:19701101T020000\r\nRRULE:FREQ=YEARLY;BYMONTH=11;BYDAY=1SU\r\nEND:STANDARD\r\n" +
	"END:VTIMEZONE\r\n"

// enumeratedHours is every hour of the day, as a BYHOUR value lists them.
func enumeratedHours() string {
	hours := make([]string, 24)
	for h := range hours {
		hours[h] = fmt.Sprint(h)
	}
	return strings.Join(hours, ",")
}

// cancelledFarOverridesObject is a series starting at dtstart, written in
// the zone timezones defines when it names one, and following rule, which
// carries n STATUS:CANCELLED RANGE=THISANDFUTURE overrides, each starting in
// the series' first week and naming the seventeenth minute of an hour about
// eighty years on, which the series never generates.
func cancelledFarOverridesObject(timezones, dtstart, rule string, n int) string {
	var b strings.Builder
	b.WriteString("BEGIN:VCALENDAR\r\nVERSION:2.0\r\nPRODID:-//Test//EN\r\n" + timezones)
	b.WriteString("BEGIN:VEVENT\r\nUID:overridden\r\nDTSTAMP:20240101T000000Z\r\n" + dtstart + "\r\nDURATION:PT10M\r\nRRULE:" + rule + "\r\nEND:VEVENT\r\n")
	tzid := ""
	if timezones != "" {
		tzid = ";TZID=Custom/Eastern"
	}
	layout := "20060102T150405Z"
	if tzid != "" {
		layout = "20060102T150405"
	}
	for i := 0; i < n; i++ {
		slot := time.Date(2080, 1, 1, 0, 17, 0, 0, time.UTC).Add(-time.Duration(i) * time.Hour).Format(layout)
		start := time.Date(2000, 1, 2, 0, 0, 0, 0, time.UTC).Add(time.Duration(i) * time.Minute).Format(layout)
		b.WriteString("BEGIN:VEVENT\r\nUID:overridden\r\nDTSTAMP:20240101T000000Z\r\n" +
			"RECURRENCE-ID;RANGE=THISANDFUTURE" + tzid + ":" + slot + "\r\nDTSTART" + tzid + ":" + start + "\r\nDURATION:PT5M\r\nSTATUS:CANCELLED\r\nEND:VEVENT\r\n")
	}
	b.WriteString("END:VCALENDAR\r\n")
	return b.String()
}

// A RANGE=THISANDFUTURE override placed in a range names a slot decades on
// that a walk of every minute of the series cannot reach on the expansion's
// budget. Whether the series carries it is then unknown, and an expansion
// owing every instance the range holds cannot say it has them: §9.6.5
// expansion refuses the resource, and a listing of its instances says it is
// partial.
func TestThisAndFutureOverrideWhoseSlotCannotBeLookedUpLeavesTheExpansionUnsettled(t *testing.T) {
	raw := instancesObject("", "DTSTART:20000101T000000Z\r\nDURATION:PT30S\r\nRRULE:FREQ=MINUTELY;BYSECOND=0;COUNT=50000000\r\n",
		"RECURRENCE-ID;RANGE=THISANDFUTURE:20100101T000030Z\r\nDTSTART:20000102T000030Z\r\nDURATION:PT5M\r\nSTATUS:CANCELLED\r\n")
	from, to := time.Date(2000, 1, 2, 0, 0, 0, 0, time.UTC), time.Date(2000, 1, 2, 0, 10, 0, 0, time.UTC)
	if got := EventInstances(raw, from, to); got.Complete || len(got.Overrides) != 0 {
		t.Errorf("EventInstances() places %+v, complete = %v, want a partial listing", got.Overrides, got.Complete)
	}
	if _, err := filterICalendarData(raw, newCalendarDataProjection(&calendarDataEl{Expand: &calendarRange{Start: from, End: to}}, floatingZone{})); !errors.Is(err, ical.ErrRecurrenceExpansionLimit) {
		t.Errorf("filterICalendarData() err = %v, want ErrRecurrenceExpansionLimit", err)
	}
}

// assertCostsAsMuchAsTheSeriesAlone times run, the work over a series and its
// overrides, against alone, the same work over the series without them, and
// fails when run takes more than four times as long. Runs are interleaved and
// the fastest of each kept, and a run under a tenth of a second is too short to
// time against noise.
func assertCostsAsMuchAsTheSeriesAlone(t *testing.T, run, alone func()) {
	t.Helper()
	if raceDetectorEnabled {
		t.Skip("cost is measured without the race detector, which multiplies every run")
	}
	if testing.Short() {
		t.Skip("cost measurement skipped in -short mode")
	}
	fastest := func(work func(), best time.Duration) time.Duration {
		started := time.Now()
		work()
		return min(best, time.Since(started))
	}
	runBest, aloneBest := time.Duration(math.MaxInt64), time.Duration(math.MaxInt64)
	for range linearScalingRuns {
		runBest = fastest(run, runBest)
		aloneBest = fastest(alone, aloneBest)
	}
	t.Logf("with its overrides took %v, the series alone %v", runBest, aloneBest)
	if runBest > 100*time.Millisecond && runBest > 4*aloneBest {
		t.Errorf("the series with its overrides took %v against %v alone", runBest, aloneBest)
	}
}

// countedSeriesFirst and countedSeriesInstances are where the series
// countedThisAndFutureOverridesObject writes starts, and how many instances
// it and its overrides together hold.
var countedSeriesFirst = time.Date(1950, 1, 1, 0, 0, 0, 0, time.UTC)

const countedSeriesInstances = 999

// countedThisAndFutureOverridesObject is the first minute of each month from
// 1950, written as a minutely rule with a COUNT, whose instances only a walk
// from its start can count, carrying n RANGE=THISANDFUTURE overrides spread
// over it, each naming the seventeenth minute of its month, which the series
// never generates. The COUNT leaves the series and its overrides the
// instances countedSeriesInstances says.
func countedThisAndFutureOverridesObject(n int) string {
	count := countedSeriesInstances - n
	var b strings.Builder
	b.WriteString("BEGIN:VCALENDAR\r\nVERSION:2.0\r\nPRODID:-//Test//EN\r\n")
	fmt.Fprintf(&b, "BEGIN:VEVENT\r\nUID:overridden\r\nDTSTAMP:20240101T000000Z\r\nDTSTART:19500101T000000Z\r\nDURATION:PT10M\r\n"+
		"RRULE:FREQ=MINUTELY;BYMONTHDAY=1;BYHOUR=0;BYMINUTE=0;COUNT=%d\r\nEND:VEVENT\r\n", count)
	for i := 0; i < n; i++ {
		slot := countedSeriesFirst.Add(17*time.Minute).AddDate(0, i*(count-1)/n, 0).Format("20060102T150405Z")
		b.WriteString("BEGIN:VEVENT\r\nUID:overridden\r\nDTSTAMP:20240101T000000Z\r\n" +
			"RECURRENCE-ID;RANGE=THISANDFUTURE:" + slot + "\r\nDTSTART:" + slot + "\r\nDURATION:PT5M\r\nSUMMARY:Later\r\nEND:VEVENT\r\n")
	}
	b.WriteString("END:VCALENDAR\r\n")
	return b.String()
}

// longInstancesFirst is where the series longInstancesThisAndFutureOverridesObject
// writes starts.
var longInstancesFirst = time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC)

// longInstancesThisAndFutureOverridesObject is a series of 2n hourly
// instances, each lasting four days, carrying a RANGE=THISANDFUTURE override
// half past each of its first n hours, which the series never generates.
func longInstancesThisAndFutureOverridesObject(n int) string {
	var b strings.Builder
	b.WriteString("BEGIN:VCALENDAR\r\nVERSION:2.0\r\nPRODID:-//Test//EN\r\n")
	fmt.Fprintf(&b, "BEGIN:VEVENT\r\nUID:overridden\r\nDTSTAMP:20240101T000000Z\r\nDTSTART:20200101T000000Z\r\nDURATION:P4D\r\n"+
		"RRULE:FREQ=HOURLY;COUNT=%d\r\nEND:VEVENT\r\n", 2*n)
	for i := 0; i < n; i++ {
		slot := longInstancesFirst.Add(time.Duration(i)*time.Hour + 30*time.Minute).Format("20060102T150405Z")
		b.WriteString("BEGIN:VEVENT\r\nUID:overridden\r\nDTSTAMP:20240101T000000Z\r\n" +
			"RECURRENCE-ID;RANGE=THISANDFUTURE:" + slot + "\r\nDTSTART:" + slot + "\r\nDURATION:PT5M\r\nSUMMARY:Later\r\nEND:VEVENT\r\n")
	}
	b.WriteString("END:VCALENDAR\r\n")
	return b.String()
}

// A RANGE=THISANDFUTURE override naming no slot of its series stands for
// itself where it starts, which a range around the slot it names, holding
// none of it, does not place.
func TestThisAndFutureOverrideNamingNoSlotStandsForItselfWhereItIsPlaced(t *testing.T) {
	raw := instancesObject("", "DTSTART:20260301T090000Z\r\nDURATION:PT1H\r\nRRULE:FREQ=DAILY\r\n",
		"RECURRENCE-ID;RANGE=THISANDFUTURE:20260302T100000Z\r\nDTSTART:20260320T100000Z\r\nDURATION:PT1H\r\nSUMMARY:Moved\r\n")
	for _, r := range []struct {
		name     string
		from, to time.Time
		placed   int
	}{
		{"around the slot it names", time.Date(2026, 3, 1, 0, 0, 0, 0, time.UTC), time.Date(2026, 3, 5, 0, 0, 0, 0, time.UTC), 0},
		{"around where it starts", time.Date(2026, 3, 20, 0, 0, 0, 0, time.UTC), time.Date(2026, 3, 21, 0, 0, 0, 0, time.UTC), 1},
	} {
		t.Run(r.name, func(t *testing.T) {
			if got := EventInstances(raw, r.from, r.to); len(got.Overrides) != r.placed {
				t.Errorf("EventInstances() places %+v, want %d overrides", got.Overrides, r.placed)
			}
			expanded, err := filterICalendarData(raw, newCalendarDataProjection(&calendarDataEl{Expand: &calendarRange{Start: r.from, End: r.to}}, floatingZone{}))
			if err != nil {
				t.Fatalf("filterICalendarData() err = %v", err)
			}
			if naming := strings.Count(expanded, "RECURRENCE-ID:20260302T100000Z\r\n"); naming != r.placed {
				t.Errorf("expansion writes %d components naming the override's slot, want %d:\n%s", naming, r.placed, expanded)
			}
		})
	}
}

// An instance can last longer than many repeats of its series, so scores of
// instances reach the time a RANGE=THISANDFUTURE override names. Whether the
// series has that slot is decided by where each slot starts, and one naming
// none, here a slot an EXDATE leaves out, stands for itself: expansion writes
// it as the one component naming that slot.
func TestThisAndFutureOverrideNamingNoSlotOfALongFrequentSeriesStandsForItself(t *testing.T) {
	from, to := time.Date(2026, 3, 1, 0, 0, 0, 0, time.UTC), time.Date(2027, 3, 1, 0, 0, 0, 0, time.UTC)
	for name, series := range map[string]struct{ master, override, slot string }{
		"two hours of a minutely series": {
			"DTSTART:20260301T000000Z\r\nDURATION:PT2H\r\nRRULE:FREQ=MINUTELY;COUNT=900\r\nEXDATE:20260301T100000Z\r\n",
			"RECURRENCE-ID;RANGE=THISANDFUTURE:20260301T100000Z\r\nDTSTART:20260301T100000Z\r\nDURATION:PT5M\r\n",
			"20260301T100000Z",
		},
		"600 days of a daily series": {
			"DTSTART:20260301T090000Z\r\nDURATION:P600D\r\nRRULE:FREQ=DAILY;COUNT=400\r\nEXDATE:20270101T090000Z\r\n",
			"RECURRENCE-ID;RANGE=THISANDFUTURE:20270101T090000Z\r\nDTSTART:20270101T090000Z\r\nDURATION:PT5M\r\n",
			"20270101T090000Z",
		},
		"100 days of an all-day series": {
			"DTSTART;VALUE=DATE:20260301\r\nDURATION:P100D\r\nRRULE:FREQ=DAILY;COUNT=300\r\nEXDATE;VALUE=DATE:20260901\r\n",
			"RECURRENCE-ID;RANGE=THISANDFUTURE;VALUE=DATE:20260901\r\nDTSTART;VALUE=DATE:20260901\r\n",
			"20260901",
		},
	} {
		t.Run(name, func(t *testing.T) {
			raw := instancesObject("", series.master, series.override+"SUMMARY:Moved\r\n")
			if _, fault := validateCalendarObjectForStorage(raw, nil); fault != nil {
				t.Fatalf("validateCalendarObjectForStorage() = %+v, want the object stored", fault)
			}
			if got := EventInstances(raw, from, to); len(got.Overrides) != 1 {
				t.Errorf("EventInstances() places %+v, want the override", got.Overrides)
			}
			expanded, err := filterICalendarData(raw, newCalendarDataProjection(&calendarDataEl{Expand: &calendarRange{Start: from, End: to}}, floatingZone{}))
			if err != nil {
				t.Fatalf("filterICalendarData() err = %v", err)
			}
			naming := 0
			for _, line := range strings.Split(expanded, "\r\n") {
				if strings.HasPrefix(line, "RECURRENCE-ID") && strings.HasSuffix(line, ":"+series.slot) {
					naming++
				}
			}
			if naming != 1 {
				t.Errorf("expansion writes %d components naming %s, want the override alone:\n%s", naming, series.slot, expanded)
			}
		})
	}
}

// farApartThisAndFutureOverridesObject is a series of every minute from 1
// January 2020 carrying a RANGE=THISANDFUTURE override at the first minute of
// each of its first n days.
func farApartThisAndFutureOverridesObject(n int) string {
	var b strings.Builder
	b.WriteString("BEGIN:VCALENDAR\r\nVERSION:2.0\r\nPRODID:-//Test//EN\r\n")
	b.WriteString("BEGIN:VEVENT\r\nUID:overridden\r\nDTSTAMP:20240101T000000Z\r\nDTSTART:20200101T000000Z\r\nDURATION:PT30S\r\nRRULE:FREQ=MINUTELY\r\nEND:VEVENT\r\n")
	first := time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC)
	for i := 0; i < n; i++ {
		slot := first.AddDate(0, 0, i).Format("20060102T150405Z")
		b.WriteString("BEGIN:VEVENT\r\nUID:overridden\r\nDTSTAMP:20240101T000000Z\r\n" +
			"RECURRENCE-ID;RANGE=THISANDFUTURE:" + slot + "\r\nDTSTART:" + slot + "\r\nDURATION:PT30S\r\nSUMMARY:Later\r\nEND:VEVENT\r\n")
	}
	b.WriteString("END:VCALENDAR\r\n")
	return b.String()
}

func BenchmarkThisAndFutureOverridesStandingForThemselves(b *testing.B) {
	raw := thisAndFutureDateOverridesObject(1000)
	from, to := time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC), time.Date(2023, 1, 1, 0, 0, 0, 0, time.UTC)
	projection := newCalendarDataProjection(&calendarDataEl{Expand: &calendarRange{Start: from, End: to}}, floatingZone{})
	b.Run("expand", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			if _, err := filterICalendarData(raw, projection); err != nil {
				b.Fatal(err)
			}
		}
	})
	b.Run("EventInstances", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			EventInstances(raw, from, to)
		}
	})
}
