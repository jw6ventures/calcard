package dav

import (
	"math"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/jw6ventures/calcard/internal/store"
)

// maxLinearScalingRatio separates work linear in its input, which grows about
// fourfold when the input does, from quadratic work, which grows sixteenfold,
// with room on both sides for timing noise.
const maxLinearScalingRatio = 12

// linearScalingFloor is the duration below which the larger run is too short
// to time against noise. The callers size their inputs so a quadratic path
// takes several times longer than this at 4n, so a run finishing under it has
// shown none.
const linearScalingFloor = 200 * time.Millisecond

// linearScalingRuns is how many timed runs of each size are interleaved.
const linearScalingRuns = 5

// assertScalesLinearly times the work prepare builds for n and for 4n and fails
// when the larger took more than maxLinearScalingRatio times the smaller. It
// compares a ratio rather than a wall-clock budget so machine speed and steady
// load cancel out, and it interleaves several runs of each size and keeps the
// fastest so a stall on a loaded machine cannot decide the outcome. Fixtures
// are built by prepare outside the timing.
func assertScalesLinearly(t *testing.T, n int, prepare func(n int) func()) {
	t.Helper()
	if raceDetectorEnabled {
		t.Skip("scaling is measured without the race detector, which multiplies every run")
	}
	if testing.Short() {
		t.Skip("scaling measurement skipped in -short mode")
	}
	small, large := prepare(n), prepare(4*n)
	fastest := func(run func(), best time.Duration) time.Duration {
		started := time.Now()
		run()
		return min(best, time.Since(started))
	}
	smallBest, largeBest := time.Duration(math.MaxInt64), time.Duration(math.MaxInt64)
	for range linearScalingRuns {
		smallBest = fastest(small, smallBest)
		largeBest = fastest(large, largeBest)
	}
	ratio := float64(largeBest) / float64(smallBest)
	t.Logf("n=%d took %v, 4n took %v: ratio %.1f", n, smallBest, largeBest, ratio)
	if largeBest < linearScalingFloor {
		return
	}
	if ratio > maxLinearScalingRatio {
		t.Errorf("work at 4n took %v against %v at n=%d, a ratio of %.1f; linear work stays under %d",
			largeBest, smallBest, n, ratio, maxLinearScalingRatio)
	}
}

// cancelledOverridesServer holds a daily rule from 1900 whose first n slots are
// each replaced by a STATUS:CANCELLED override, followed by a tentative
// RANGE=THISANDFUTURE override at slot n that types every later instance.
func cancelledOverridesServer(n int) *DavServer {
	var raw strings.Builder
	raw.WriteString("BEGIN:VCALENDAR\r\nVERSION:2.0\r\nBEGIN:VEVENT\r\nUID:overridden\r\n" +
		"DTSTART:19000101T090000Z\r\nDTEND:19000101T100000Z\r\nRRULE:FREQ=DAILY\r\nEND:VEVENT\r\n")
	first := time.Date(1900, 1, 1, 9, 0, 0, 0, time.UTC)
	for i := 0; i < n; i++ {
		raw.WriteString("BEGIN:VEVENT\r\nUID:overridden\r\nRECURRENCE-ID:" +
			first.AddDate(0, 0, i).Format("20060102T150405Z") + "\r\nSTATUS:CANCELLED\r\nEND:VEVENT\r\n")
	}
	governing := first.AddDate(0, 0, n).Format("20060102T150405Z")
	raw.WriteString("BEGIN:VEVENT\r\nUID:overridden\r\nRECURRENCE-ID;RANGE=THISANDFUTURE:" + governing + "\r\n" +
		"DTSTART:" + governing + "\r\nSTATUS:TENTATIVE\r\nEND:VEVENT\r\n")
	raw.WriteString("END:VCALENDAR\r\n")

	calRepo := &fakeCalendarRepo{accessible: []store.CalendarAccess{
		{Calendar: store.Calendar{ID: 1, UserID: 1, Name: "Work"}, Editor: true},
	}}
	return &DavServer{store: &store.Store{Calendars: calRepo, Events: &fakeEventRepo{events: map[string]*store.Event{
		"1:overridden": {CalendarID: 1, UID: "overridden", ResourceName: "overridden", ETag: "e1", RawICAL: raw.String()},
	}}}}
}

// freeBusyOverDays asks for the busy time of the first days slots of the rule.
func freeBusyOverDays(t *testing.T, h *DavServer, days int) *httptest.ResponseRecorder {
	t.Helper()
	first := time.Date(1900, 1, 1, 0, 0, 0, 0, time.UTC)
	body := `<C:free-busy-query xmlns:C="urn:ietf:params:xml:ns:caldav">
  <C:time-range start="` + first.Format("20060102T150405Z") + `" end="` +
		first.AddDate(0, 0, days).Format("20060102T150405Z") + `"/>
</C:free-busy-query>`
	rr := httptest.NewRecorder()
	h.Report(rr, freeBusyRequestFor("/dav/calendars/1/", body, &store.User{ID: 1}))
	if rr.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200: %.500s", rr.Code, rr.Body.String())
	}
	return rr
}

// Free-busy types every expanded instance by the component describing its
// slot, and a stored object may carry tens of thousands of overrides inside the
// body limit. Every override is either an exact slot or a governing
// RANGE=THISANDFUTURE one, so the report's work has to grow with the overrides
// plus the instances, not with their product.
func TestFreeBusyScalesWithManyOverrides(t *testing.T) {
	const overrides = 1_000
	rr := freeBusyOverDays(t, cancelledOverridesServer(overrides), 2*overrides)
	periods := parsedFreeBusyPeriods(t, rr.Body.String())
	if len(periods) != overrides {
		t.Fatalf("published %d periods, want %d: every slot but the cancelled ones", len(periods), overrides)
	}
	for _, period := range periods {
		if period.fbType != "BUSY-TENTATIVE" {
			t.Fatalf("period at %v published as %q, want the governing override's BUSY-TENTATIVE", period.start, period.fbType)
		}
	}

	// The larger run spans 64 000 days, inside the storable span.
	assertScalesLinearly(t, 8_000, func(n int) func() {
		h := cancelledOverridesServer(n)
		return func() { freeBusyOverDays(t, h, 2*n) }
	})
}

// The index answers what a walk over the overrides in resource order would: an
// exact override wins, else the latest RANGE=THISANDFUTURE override at or
// before the slot, the first of several naming the same slot, else the master.
func TestRecurrenceOverrideIndexDescribesEachSlot(t *testing.T) {
	raw := "BEGIN:VCALENDAR\r\nVERSION:2.0\r\n" +
		"BEGIN:VEVENT\r\nUID:x\r\nSUMMARY:master\r\nDTSTART:20240101T090000Z\r\nRRULE:FREQ=DAILY\r\nEND:VEVENT\r\n" +
		"BEGIN:VEVENT\r\nUID:x\r\nSUMMARY:future-late\r\nRECURRENCE-ID;RANGE=THISANDFUTURE:20240110T090000Z\r\nEND:VEVENT\r\n" +
		"BEGIN:VEVENT\r\nUID:x\r\nSUMMARY:future-early-first\r\nRECURRENCE-ID;RANGE=THISANDFUTURE:20240105T090000Z\r\nEND:VEVENT\r\n" +
		"BEGIN:VEVENT\r\nUID:x\r\nSUMMARY:future-early-second\r\nRECURRENCE-ID;RANGE=THISANDFUTURE:20240105T090000Z\r\nEND:VEVENT\r\n" +
		"BEGIN:VEVENT\r\nUID:x\r\nSUMMARY:exact\r\nRECURRENCE-ID:20240107T090000Z\r\nEND:VEVENT\r\n" +
		"BEGIN:VEVENT\r\nUID:x\r\nSUMMARY:exact-duplicate\r\nRECURRENCE-ID:20240107T090000Z\r\nEND:VEVENT\r\n" +
		"END:VCALENDAR\r\n"
	root, err := parseICalendarObject(raw)
	if err != nil {
		t.Fatal(err)
	}
	matcher := newCalendarTimeRangeMatcher(raw, root, floatingZone{})
	master := freeBusyMasterComponent(root)
	index := matcher.overrideIndex(root, master)

	slot := func(day int) time.Time { return time.Date(2024, 1, day, 9, 0, 0, 0, time.UTC) }
	tests := []struct {
		slot time.Time
		want string
	}{
		{slot(1), "master"},
		{slot(4), "master"},
		{slot(5), "future-early-first"},
		{slot(6), "future-early-first"},
		{slot(7), "exact"},
		{slot(9), "future-early-first"},
		{slot(10), "future-late"},
		{slot(30), "future-late"},
	}
	for _, tt := range tests {
		if got := index.describing(master, tt.slot).value("SUMMARY"); got != tt.want {
			t.Errorf("slot %v described by %q, want %q", tt.slot, got, tt.want)
		}
	}
}
