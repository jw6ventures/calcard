package dav

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

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
