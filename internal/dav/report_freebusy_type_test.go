package dav

import (
	"net/http"
	"net/http/httptest"
	"slices"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/jw6ventures/calcard/internal/store"
)

// freeBusyRange is the window every fixture in this file publishes into.
var freeBusyRange = &timeRange{Start: "20240601T000000Z", End: "20240602T000000Z"}

// vEventObject builds a one-VEVENT resource carrying the given extra property
// lines, so a test can vary TRANSP and STATUS and nothing else.
func vEventObject(uid, dtstart, dtend string, extra ...string) store.Event {
	lines := []string{
		"BEGIN:VCALENDAR", "VERSION:2.0", "PRODID:-//CalCard//EN",
		"BEGIN:VEVENT", "UID:" + uid, "DTSTAMP:20240601T080000Z",
		"DTSTART:" + dtstart, "DTEND:" + dtend,
	}
	lines = append(lines, extra...)
	lines = append(lines, "END:VEVENT", "END:VCALENDAR", "")
	return store.Event{UID: uid, ResourceName: uid, RawICAL: strings.Join(lines, "\r\n")}
}

// publishedFreeBusy is every FREEBUSY content line the report writes over the
// single-day freeBusyRange, sorted so a comparison does not depend on candidate
// order.
func publishedFreeBusy(t *testing.T, events ...store.Event) []string {
	t.Helper()
	return publishedFreeBusyOver(t, freeBusyRange, events...)
}

func publishedFreeBusyOver(t *testing.T, tr *timeRange, events ...store.Event) []string {
	t.Helper()
	lines := publishedFreeBusyInEmissionOrder(t, tr, events...)
	sort.Strings(lines)
	return lines
}

// publishedFreeBusyInEmissionOrder keeps the order the report wrote its lines
// in, which RFC 5545 §3.8.2.6 constrains and the sorted view above discards.
func publishedFreeBusyInEmissionOrder(t *testing.T, tr *timeRange, events ...store.Event) []string {
	t.Helper()
	body := mustReportText((&DavServer{}).generateFreeBusy(freeBusyCandidates(events, floatingZone{}), tr))
	return parsedFreeBusyLines(t, body)
}

func parsedFreeBusyLines(t *testing.T, body string) []string {
	t.Helper()
	periods := parsedFreeBusyPeriods(t, body)
	lines := make([]string, 0, len(periods))
	for _, period := range periods {
		lines = append(lines, "FREEBUSY"+freeBusyTypeParameter(period.fbType)+":"+
			period.start.UTC().Format("20060102T150405Z")+"/"+period.end.UTC().Format("20060102T150405Z"))
	}
	return lines
}

func parsedFreeBusyPeriods(t *testing.T, body string) []freeBusyInterval {
	t.Helper()
	root, err := parseICalendarObject(body)
	if err != nil {
		t.Fatalf("free-busy response is not parseable iCalendar: %v; value:\n%s", err, body)
	}
	if fault := validateCalendarObject(root); fault != nil {
		t.Fatalf("free-busy response is not valid iCalendar: %+v; value:\n%s", fault, body)
	}
	if root.childCount("VFREEBUSY") != 1 {
		t.Fatalf("free-busy response carries %d VFREEBUSY components, want exactly 1; value:\n%s",
			root.childCount("VFREEBUSY"), body)
	}
	var component *icalNode
	for _, child := range root.children {
		if child.name == "VFREEBUSY" {
			component = child
			break
		}
	}
	matcher := newCalendarTimeRangeMatcher(body, root, floatingZone{})
	var periods []freeBusyInterval
	for _, property := range component.properties {
		if property.name != "FREEBUSY" {
			continue
		}
		fbType := strings.ToUpper(strings.TrimSpace(property.parameters["FBTYPE"]))
		if fbType == "" {
			fbType = freeBusyBusy
		}
		for _, rawPeriod := range strings.Split(property.value, ",") {
			start, end, ok := matcher.freeBusyPeriod(property, strings.TrimSpace(rawPeriod))
			if !ok || !end.After(start) {
				t.Fatalf("FREEBUSY carries invalid PERIOD %q; value:\n%s", rawPeriod, body)
			}
			periods = append(periods, freeBusyInterval{start: start, end: end, fbType: fbType})
		}
	}
	return periods
}

func assertPublishedFreeBusy(t *testing.T, got, want []string) {
	t.Helper()
	if strings.Join(got, " | ") != strings.Join(want, " | ") {
		t.Fatalf("published busy time = %v, want %v", got, want)
	}
}

// RFC 4791 §7.10 considers only a VEVENT that is absent TRANSP or names OPAQUE.
// A transparent event does not occupy its owner's time, so it publishes nothing.
func TestFreeBusyExcludesTransparentEvents(t *testing.T) {
	tests := map[string]struct {
		transp string
		want   []string
	}{
		"TRANSP absent":       {want: []string{"FREEBUSY:20240601T100000Z/20240601T110000Z"}},
		"TRANSP:OPAQUE":       {transp: "TRANSP:OPAQUE", want: []string{"FREEBUSY:20240601T100000Z/20240601T110000Z"}},
		"TRANSP:TRANSPARENT":  {transp: "TRANSP:TRANSPARENT", want: nil},
		"transp in lowercase": {transp: "TRANSP:transparent", want: nil},
	}

	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			var extra []string
			if test.transp != "" {
				extra = append(extra, test.transp)
			}
			event := vEventObject("e", "20240601T100000Z", "20240601T110000Z", extra...)
			assertPublishedFreeBusy(t, publishedFreeBusy(t, event), test.want)
		})
	}
}

// The RFC 4791 §7.10 table maps TRANSP and STATUS onto the FBTYPE the published
// periods carry. Everything the table sends to FREE publishes nothing at all:
// this report returns busy time only, and free time is inferred from its
// absence. BUSY is written without the parameter, since RFC 5545 §3.2.9 makes
// it the default.
func TestFreeBusyTypeFollowsTheTransparencyAndStatusTable(t *testing.T) {
	tests := map[string]struct {
		properties []string
		want       []string
	}{
		"opaque and confirmed": {
			properties: []string{"TRANSP:OPAQUE", "STATUS:CONFIRMED"},
			want:       []string{"FREEBUSY:20240601T100000Z/20240601T110000Z"},
		},
		"opaque with no status": {
			properties: []string{"TRANSP:OPAQUE"},
			want:       []string{"FREEBUSY:20240601T100000Z/20240601T110000Z"},
		},
		"opaque and tentative": {
			properties: []string{"TRANSP:OPAQUE", "STATUS:TENTATIVE"},
			want:       []string{"FREEBUSY;FBTYPE=BUSY-TENTATIVE:20240601T100000Z/20240601T110000Z"},
		},
		"opaque and cancelled publishes nothing": {
			properties: []string{"TRANSP:OPAQUE", "STATUS:CANCELLED"},
			want:       nil,
		},
		"opaque and an x-name status answers BUSY": {
			properties: []string{"TRANSP:OPAQUE", "STATUS:X-DEFERRED"},
			want:       []string{"FREEBUSY:20240601T100000Z/20240601T110000Z"},
		},
		"transparent and tentative publishes nothing": {
			properties: []string{"TRANSP:TRANSPARENT", "STATUS:TENTATIVE"},
			want:       nil,
		},
		"transparent and confirmed publishes nothing": {
			properties: []string{"TRANSP:TRANSPARENT", "STATUS:CONFIRMED"},
			want:       nil,
		},
	}

	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			event := vEventObject("e", "20240601T100000Z", "20240601T110000Z", test.properties...)
			assertPublishedFreeBusy(t, publishedFreeBusy(t, event), test.want)
		})
	}
}

// §7.10 derives FBTYPE from "the value of the TRANSP and STATUS properties" of
// the VEVENT the period came from. An overridden instance is described by its
// override rather than by the master, so the override's own transparency and
// status decide how that one instance is published.
func TestFreeBusyTypeFollowsTheOverridingInstance(t *testing.T) {
	// recurrenceIDLine is the whole content line, so a test can vary the RANGE
	// parameter without the fixture having to reassemble one.
	recurringWithOverride := func(recurrenceIDLine string, overrideProperties ...string) store.Event {
		lines := []string{
			"BEGIN:VCALENDAR", "VERSION:2.0", "PRODID:-//CalCard//EN",
			"BEGIN:VEVENT", "UID:u", "DTSTAMP:20240601T080000Z",
			"DTSTART:20240601T100000Z", "DTEND:20240601T110000Z",
			"RRULE:FREQ=DAILY;COUNT=3", "END:VEVENT",
			"BEGIN:VEVENT", "UID:u", recurrenceIDLine,
			"DTSTAMP:20240601T080000Z",
			"DTSTART:20240602T100000Z", "DTEND:20240602T110000Z",
		}
		lines = append(lines, overrideProperties...)
		lines = append(lines, "END:VEVENT", "END:VCALENDAR", "")
		return store.Event{UID: "u", ResourceName: "u", RawICAL: strings.Join(lines, "\r\n")}
	}

	const first = "FREEBUSY:20240601T100000Z/20240601T110000Z"
	const third = "FREEBUSY:20240603T100000Z/20240603T110000Z"

	tests := map[string]struct {
		recurrenceIDLine string
		properties       []string
		want             []string
	}{
		"a tentative override types only its own instance": {
			recurrenceIDLine: "RECURRENCE-ID:20240602T100000Z",
			properties:       []string{"STATUS:TENTATIVE"},
			want: []string{
				first,
				"FREEBUSY;FBTYPE=BUSY-TENTATIVE:20240602T100000Z/20240602T110000Z",
				third,
			},
		},
		"a transparent override publishes no busy time for its instance": {
			recurrenceIDLine: "RECURRENCE-ID:20240602T100000Z",
			properties:       []string{"TRANSP:TRANSPARENT"},
			want:             []string{first, third},
		},
		"a cancelled override publishes no busy time for its instance": {
			recurrenceIDLine: "RECURRENCE-ID:20240602T100000Z",
			properties:       []string{"STATUS:CANCELLED"},
			want:             []string{first, third},
		},
		"a confirmed override leaves the whole set busy": {
			recurrenceIDLine: "RECURRENCE-ID:20240602T100000Z",
			properties:       []string{"STATUS:CONFIRMED"},
			want: []string{
				first,
				"FREEBUSY:20240602T100000Z/20240602T110000Z",
				third,
			},
		},
		"a THISANDFUTURE override types its instance and every later one": {
			recurrenceIDLine: "RECURRENCE-ID;RANGE=THISANDFUTURE:20240602T100000Z",
			properties:       []string{"STATUS:TENTATIVE"},
			want: []string{
				first,
				"FREEBUSY;FBTYPE=BUSY-TENTATIVE:20240602T100000Z/20240602T110000Z",
				"FREEBUSY;FBTYPE=BUSY-TENTATIVE:20240603T100000Z/20240603T110000Z",
			},
		},
	}

	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			event := recurringWithOverride(test.recurrenceIDLine, test.properties...)
			// The fixture recurs over three days, so the window has to reach all
			// three for the untouched instances to be part of the comparison.
			window := &timeRange{Start: "20240601T000000Z", End: "20240604T000000Z"}
			assertPublishedFreeBusy(t, publishedFreeBusyOver(t, window, event), sortedCopy(test.want))
		})
	}
}

func TestFreeBusyTypeFollowsTheRecurrenceSlotAfterThisAndFutureMovesIt(t *testing.T) {
	event := store.Event{
		UID:          "shifted",
		ResourceName: "shifted",
		RawICAL: strings.Join([]string{
			"BEGIN:VCALENDAR", "VERSION:2.0", "PRODID:-//CalCard//EN",
			"BEGIN:VEVENT", "UID:shifted", "DTSTAMP:20240601T080000Z",
			"DTSTART:20240601T100000Z", "DTEND:20240601T110000Z",
			"RRULE:FREQ=DAILY;COUNT=3", "END:VEVENT",
			"BEGIN:VEVENT", "UID:shifted",
			"RECURRENCE-ID;RANGE=THISANDFUTURE:20240602T100000Z",
			"DTSTAMP:20240601T080000Z", "DTSTART:20240531T220000Z",
			"DTEND:20240531T230000Z", "STATUS:TENTATIVE", "END:VEVENT",
			"END:VCALENDAR", "",
		}, "\r\n"),
	}
	window := &timeRange{Start: "20240601T000000Z", End: "20240603T000000Z"}

	assertPublishedFreeBusy(t, publishedFreeBusyOver(t, window, event), []string{
		"FREEBUSY:20240601T100000Z/20240601T110000Z",
		"FREEBUSY;FBTYPE=BUSY-TENTATIVE:20240601T220000Z/20240601T230000Z",
	})
}

func sortedCopy(values []string) []string {
	out := append([]string(nil), values...)
	sort.Strings(out)
	return out
}

// RFC 4791 §4.1 permits a calendar object resource made only of overridden
// instances, with no master component. Every one of those carries a
// RECURRENCE-ID, so a reading that publishes only what a master expands into
// would answer for such a resource that its owner is free all day. The
// override describes the instance, so its own TRANSP and STATUS type it.
func TestFreeBusyPublishesAnOverrideOnlyResource(t *testing.T) {
	overrideOnly := func(properties ...string) store.Event {
		lines := []string{
			"BEGIN:VCALENDAR", "VERSION:2.0", "PRODID:-//CalCard//EN",
			"BEGIN:VEVENT", "UID:orphan", "DTSTAMP:20240601T080000Z",
			"RECURRENCE-ID:20240601T090000Z",
			"DTSTART:20240601T100000Z", "DTEND:20240601T110000Z",
		}
		lines = append(lines, properties...)
		lines = append(lines, "END:VEVENT", "END:VCALENDAR", "")
		return store.Event{UID: "orphan", ResourceName: "orphan", RawICAL: strings.Join(lines, "\r\n")}
	}

	tests := map[string]struct {
		properties []string
		want       []string
	}{
		"the instance is published": {
			want: []string{"FREEBUSY:20240601T100000Z/20240601T110000Z"},
		},
		"the override's own status types it": {
			properties: []string{"STATUS:TENTATIVE"},
			want:       []string{"FREEBUSY;FBTYPE=BUSY-TENTATIVE:20240601T100000Z/20240601T110000Z"},
		},
		"a transparent override publishes nothing": {
			properties: []string{"TRANSP:TRANSPARENT"},
			want:       nil,
		},
	}

	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			assertPublishedFreeBusy(t, publishedFreeBusy(t, overrideOnly(test.properties...)), test.want)
		})
	}
}

func TestFreeBusyPublishesEveryOverrideInAMasterlessResource(t *testing.T) {
	event := store.Event{
		UID:          "orphan",
		ResourceName: "orphan",
		RawICAL: strings.Join([]string{
			"BEGIN:VCALENDAR", "VERSION:2.0", "PRODID:-//CalCard//EN",
			"BEGIN:VEVENT", "UID:orphan", "DTSTAMP:20240601T080000Z",
			"RECURRENCE-ID:20240501T090000Z", "DTSTART:20240501T100000Z",
			"DTEND:20240501T110000Z", "END:VEVENT",
			"BEGIN:VEVENT", "UID:orphan", "DTSTAMP:20240601T080000Z",
			"RECURRENCE-ID:20240601T090000Z", "DTSTART:20240601T100000Z",
			"DTEND:20240601T110000Z", "STATUS:TENTATIVE", "END:VEVENT",
			"END:VCALENDAR", "",
		}, "\r\n"),
	}

	assertPublishedFreeBusy(t, publishedFreeBusy(t, event), []string{
		"FREEBUSY;FBTYPE=BUSY-TENTATIVE:20240601T100000Z/20240601T110000Z",
	})
}

// A master carrying no recurrence property at all, beside a sibling that names
// a RECURRENCE-ID. There is no pattern to expand, so there are no slots and no
// instance either component can be said to override: each is a VEVENT that
// occupies its own time, and §7.10 considers every such VEVENT. Publishing only
// the master would drop busy time the resource genuinely occupies, and it is
// also both components that the §9.6.5 expansion returns for these octets.
func TestFreeBusyPublishesOverridesOfAMasterWithNoRecurrencePattern(t *testing.T) {
	event := store.Event{
		UID:          "unpatterned",
		ResourceName: "unpatterned",
		RawICAL: strings.Join([]string{
			"BEGIN:VCALENDAR", "VERSION:2.0", "PRODID:-//CalCard//EN",
			"BEGIN:VEVENT", "UID:unpatterned", "DTSTAMP:20240601T080000Z",
			"DTSTART:20240601T090000Z", "DTEND:20240601T100000Z", "END:VEVENT",
			"BEGIN:VEVENT", "UID:unpatterned", "DTSTAMP:20240601T080000Z",
			"RECURRENCE-ID:20240602T090000Z", "DTSTART:20240602T140000Z",
			"DTEND:20240602T150000Z", "STATUS:TENTATIVE", "END:VEVENT",
			"END:VCALENDAR", "",
		}, "\r\n"),
	}

	twoDays := &timeRange{Start: "20240601T000000Z", End: "20240603T000000Z"}
	assertPublishedFreeBusy(t, publishedFreeBusyOver(t, twoDays, event), []string{
		"FREEBUSY:20240601T090000Z/20240601T100000Z",
		"FREEBUSY;FBTYPE=BUSY-TENTATIVE:20240602T140000Z/20240602T150000Z",
	})
}

// §7.10 considers VFREEBUSY components as well as VEVENTs, which means the
// FREEBUSY periods a stored VFREEBUSY already declares are republished under
// the FBTYPE each one names. RFC 5545 §3.2.9 defaults that to BUSY, and FREE
// names free time, which this report does not return.
func TestFreeBusyConsidersStoredVFreeBusyComponents(t *testing.T) {
	stored := store.Event{
		UID:          "fb",
		ResourceName: "fb",
		RawICAL: strings.Join([]string{
			"BEGIN:VCALENDAR", "VERSION:2.0", "PRODID:-//CalCard//EN",
			"BEGIN:VFREEBUSY", "UID:fb", "DTSTAMP:20240601T080000Z",
			"DTSTART:20240601T000000Z", "DTEND:20240602T000000Z",
			"FREEBUSY:20240601T140000Z/20240601T150000Z",
			"FREEBUSY;FBTYPE=BUSY-TENTATIVE:20240601T160000Z/PT1H",
			"FREEBUSY;FBTYPE=FREE:20240601T180000Z/20240601T190000Z",
			"END:VFREEBUSY", "END:VCALENDAR", "",
		}, "\r\n"),
	}

	assertPublishedFreeBusy(t, publishedFreeBusy(t, stored), []string{
		"FREEBUSY:20240601T140000Z/20240601T150000Z",
		"FREEBUSY;FBTYPE=BUSY-TENTATIVE:20240601T160000Z/20240601T170000Z",
	})
}

// §7.10: duplicate busy periods with the same FBTYPE are not returned. Two
// calendar objects covering the same hour is the ordinary way that happens.
func TestFreeBusyRemovesDuplicatePeriods(t *testing.T) {
	first := vEventObject("a", "20240601T100000Z", "20240601T110000Z")
	second := vEventObject("b", "20240601T100000Z", "20240601T110000Z")

	assertPublishedFreeBusy(t, publishedFreeBusy(t, first, second), []string{
		"FREEBUSY:20240601T100000Z/20240601T110000Z",
	})
}

// §7.10: consecutive or overlapping busy periods of the same type are
// coalesced, while periods of different types may overlap and stay distinct.
func TestFreeBusyCoalescesAdjacentAndOverlappingPeriods(t *testing.T) {
	t.Run("consecutive periods merge", func(t *testing.T) {
		assertPublishedFreeBusy(t, publishedFreeBusy(t,
			vEventObject("a", "20240601T090000Z", "20240601T100000Z"),
			vEventObject("b", "20240601T100000Z", "20240601T110000Z"),
		), []string{"FREEBUSY:20240601T090000Z/20240601T110000Z"})
	})

	t.Run("overlapping periods merge", func(t *testing.T) {
		assertPublishedFreeBusy(t, publishedFreeBusy(t,
			vEventObject("a", "20240601T090000Z", "20240601T103000Z"),
			vEventObject("b", "20240601T100000Z", "20240601T110000Z"),
		), []string{"FREEBUSY:20240601T090000Z/20240601T110000Z"})
	})

	t.Run("a contained period is absorbed", func(t *testing.T) {
		assertPublishedFreeBusy(t, publishedFreeBusy(t,
			vEventObject("a", "20240601T090000Z", "20240601T120000Z"),
			vEventObject("b", "20240601T100000Z", "20240601T110000Z"),
		), []string{"FREEBUSY:20240601T090000Z/20240601T120000Z"})
	})

	t.Run("a gap is preserved", func(t *testing.T) {
		assertPublishedFreeBusy(t, publishedFreeBusy(t,
			vEventObject("a", "20240601T090000Z", "20240601T100000Z"),
			vEventObject("b", "20240601T110000Z", "20240601T120000Z"),
		), []string{
			"FREEBUSY:20240601T090000Z/20240601T100000Z",
			"FREEBUSY:20240601T110000Z/20240601T120000Z",
		})
	})

	t.Run("different types over one interval stay distinct", func(t *testing.T) {
		assertPublishedFreeBusy(t, publishedFreeBusy(t,
			vEventObject("a", "20240601T100000Z", "20240601T110000Z"),
			vEventObject("b", "20240601T100000Z", "20240601T110000Z", "STATUS:TENTATIVE"),
		), []string{
			"FREEBUSY:20240601T100000Z/20240601T110000Z",
			"FREEBUSY;FBTYPE=BUSY-TENTATIVE:20240601T100000Z/20240601T110000Z",
		})
	})
}

// RFC 5545 §3.8.2.6: FREEBUSY properties within a VFREEBUSY "SHOULD be sorted
// in ascending order, based on start time and then end time, with the earliest
// periods first". Grouping by FBTYPE is how the merge finds the periods it may
// coalesce; it is not how the result is published.
func TestFreeBusySortsPublishedPeriodsAscending(t *testing.T) {
	// The tentative period is earlier but sorts later by FBTYPE, so an output
	// grouped by type would put it second.
	tentative := vEventObject("tentative", "20240601T090000Z", "20240601T100000Z", "STATUS:TENTATIVE")
	busy := vEventObject("busy", "20240601T140000Z", "20240601T150000Z")

	assertPublishedFreeBusy(t, publishedFreeBusyInEmissionOrder(t, freeBusyRange, busy, tentative), []string{
		"FREEBUSY;FBTYPE=BUSY-TENTATIVE:20240601T090000Z/20240601T100000Z",
		"FREEBUSY:20240601T140000Z/20240601T150000Z",
	})
}

func TestFreeBusyMergeDropsNonPositivePeriods(t *testing.T) {
	instant := freeBusyInterval{
		start:  mustFreeBusyTime(t, "20240601T110000Z"),
		end:    mustFreeBusyTime(t, "20240601T110000Z"),
		fbType: freeBusyBusy,
	}
	if got := mergeFreeBusyPeriods([]freeBusyInterval{instant}); len(got) != 0 {
		t.Fatalf("single zero-length period survived merge: %+v", got)
	}

	intervals := []freeBusyInterval{
		{start: mustFreeBusyTime(t, "20240601T090000Z"), end: mustFreeBusyTime(t, "20240601T100000Z"), fbType: freeBusyBusy},
		{start: mustFreeBusyTime(t, "20240601T093000Z"), end: mustFreeBusyTime(t, "20240601T093000Z"), fbType: freeBusyBusy},
		{start: mustFreeBusyTime(t, "20240601T110000Z"), end: mustFreeBusyTime(t, "20240601T110000Z"), fbType: freeBusyBusy},
		{start: mustFreeBusyTime(t, "20240601T130000Z"), end: mustFreeBusyTime(t, "20240601T120000Z"), fbType: freeBusyBusy},
	}

	merged := mergeFreeBusyPeriods(intervals)
	if len(merged) != 1 {
		t.Fatalf("merged = %d periods, want 1: %+v", len(merged), merged)
	}
	if !merged[0].start.Equal(mustFreeBusyTime(t, "20240601T090000Z")) ||
		!merged[0].end.Equal(mustFreeBusyTime(t, "20240601T100000Z")) {
		t.Errorf("positive interval changed during merge: %+v", merged[0])
	}
}

func mustFreeBusyTime(t *testing.T, value string) time.Time {
	t.Helper()
	parsed, ok := parseUTCDateTime(value)
	if !ok {
		t.Fatalf("fixture %q is not a date with UTC time", value)
	}
	return parsed
}

// The two routes a collection publishes busy time through -- a VEVENT
// recurrence set and the FREEBUSY periods a stored VFREEBUSY already declares --
// land in one merge, because §7.10 coalescing is a question about the
// collection's time rather than about any one resource.
func TestFreeBusyCoalescesAcrossStoredAndDerivedPeriods(t *testing.T) {
	stored := store.Event{
		UID:          "fb",
		ResourceName: "fb",
		RawICAL: strings.Join([]string{
			"BEGIN:VCALENDAR", "VERSION:2.0", "PRODID:-//CalCard//EN",
			"BEGIN:VFREEBUSY", "UID:fb", "DTSTAMP:20240601T080000Z",
			"DTSTART:20240601T000000Z", "DTEND:20240602T000000Z",
			"FREEBUSY:20240601T100000Z/20240601T110000Z",
			"FREEBUSY;FBTYPE=BUSY-TENTATIVE:20240601T140000Z/20240601T150000Z",
			"END:VFREEBUSY", "END:VCALENDAR", "",
		}, "\r\n"),
	}
	// Recurs hourly, so one instance abuts the stored period and the next
	// overlaps it: both merge into it, while the tentative one stays distinct.
	recurring := store.Event{
		UID:          "ev",
		ResourceName: "ev",
		RawICAL: strings.Join([]string{
			"BEGIN:VCALENDAR", "VERSION:2.0", "PRODID:-//CalCard//EN",
			"BEGIN:VEVENT", "UID:ev", "DTSTAMP:20240601T080000Z",
			"DTSTART:20240601T090000Z", "DTEND:20240601T100000Z",
			"RRULE:FREQ=HOURLY;COUNT=2",
			"END:VEVENT", "END:VCALENDAR", "",
		}, "\r\n"),
	}

	assertPublishedFreeBusy(t, publishedFreeBusy(t, stored, recurring), []string{
		"FREEBUSY:20240601T090000Z/20240601T110000Z",
		"FREEBUSY;FBTYPE=BUSY-TENTATIVE:20240601T140000Z/20240601T150000Z",
	})
}

// --- the whole report over a collection that mixes every contributor shape ---

// heterogeneousFreeBusyServer answers a free-busy-query against one collection
// holding every shape §7.10 has a rule for: a recurring master with a tentative
// override, a resource abutting one of its instances, a transparent one, a
// cancelled one, a stored VFREEBUSY declaring a busy and a free period, and a
// floating value that only the collection's own timezone places correctly.
func heterogeneousFreeBusyServer(t *testing.T) *DavServer {
	t.Helper()
	if _, err := time.LoadLocation("America/Chicago"); err != nil {
		t.Skipf("tzdata unavailable: %v", err)
	}
	chicago := chicagoVTimezone()

	object := func(lines ...string) string {
		all := append([]string{"BEGIN:VCALENDAR", "VERSION:2.0", "PRODID:-//CalCard//EN"}, lines...)
		return strings.Join(append(all, "END:VCALENDAR", ""), "\r\n")
	}
	resources := map[string]string{
		// Daily at 09:00Z, with the second instance overridden as tentative.
		"recurring": object(
			"BEGIN:VEVENT", "UID:recurring", "DTSTAMP:20240601T080000Z",
			"DTSTART:20240601T090000Z", "DTEND:20240601T100000Z",
			"RRULE:FREQ=DAILY;COUNT=3", "END:VEVENT",
			"BEGIN:VEVENT", "UID:recurring", "RECURRENCE-ID:20240602T090000Z",
			"DTSTAMP:20240601T080000Z", "DTSTART:20240602T090000Z",
			"DTEND:20240602T100000Z", "STATUS:TENTATIVE", "END:VEVENT"),
		// Abuts the first instance, from a different resource, so the coalescing
		// §7.10 asks for has to reach across the collection to fire.
		"abutting": object(
			"BEGIN:VEVENT", "UID:abutting", "DTSTAMP:20240601T080000Z",
			"DTSTART:20240601T100000Z", "DTEND:20240601T110000Z", "END:VEVENT"),
		"transparent": object(
			"BEGIN:VEVENT", "UID:transparent", "DTSTAMP:20240601T080000Z",
			"DTSTART:20240601T120000Z", "DTEND:20240601T130000Z",
			"TRANSP:TRANSPARENT", "END:VEVENT"),
		"cancelled": object(
			"BEGIN:VEVENT", "UID:cancelled", "DTSTAMP:20240601T080000Z",
			"DTSTART:20240601T130000Z", "DTEND:20240601T140000Z",
			"STATUS:CANCELLED", "END:VEVENT"),
		"stored": object(
			"BEGIN:VFREEBUSY", "UID:stored", "DTSTAMP:20240601T080000Z",
			"DTSTART:20240601T000000Z", "DTEND:20240602T000000Z",
			"FREEBUSY:20240601T140000Z/20240601T150000Z",
			"FREEBUSY;FBTYPE=FREE:20240601T160000Z/20240601T170000Z",
			"END:VFREEBUSY"),
		// 12:00 floating on the second day: 12:00Z read as UTC, 17:00Z in
		// America/Chicago, and RFC 4791 §7.3 makes the collection property the
		// zone that decides.
		"floating": object(
			"BEGIN:VEVENT", "UID:floating", "DTSTAMP:20240601T080000Z",
			"DTSTART:20240602T120000", "DTEND:20240602T130000", "END:VEVENT"),
		"shifted-range": object(
			"BEGIN:VEVENT", "UID:shifted-range", "DTSTAMP:20240601T080000Z",
			"DTSTART:20240601T180000Z", "DTEND:20240601T190000Z",
			"RRULE:FREQ=DAILY;COUNT=3", "END:VEVENT",
			"BEGIN:VEVENT", "UID:shifted-range",
			"RECURRENCE-ID;RANGE=THISANDFUTURE:20240602T180000Z",
			"DTSTAMP:20240601T080000Z", "DTSTART:20240531T060000Z",
			"DTEND:20240531T070000Z", "STATUS:TENTATIVE", "END:VEVENT"),
		"masterless": object(
			"BEGIN:VEVENT", "UID:masterless", "DTSTAMP:20240601T080000Z",
			"RECURRENCE-ID:20240501T160000Z", "DTSTART:20240501T160000Z",
			"DTEND:20240501T170000Z", "END:VEVENT",
			"BEGIN:VEVENT", "UID:masterless", "DTSTAMP:20240601T080000Z",
			"RECURRENCE-ID:20240601T160000Z", "DTSTART:20240601T160000Z",
			"DTEND:20240601T170000Z", "STATUS:TENTATIVE", "END:VEVENT"),
	}

	events := make(map[string]*store.Event, len(resources))
	for name, raw := range resources {
		events["1:"+name] = &store.Event{
			CalendarID: 1, UID: name, ResourceName: name, ETag: "etag-" + name, RawICAL: raw,
		}
	}
	return &DavServer{store: &store.Store{
		Calendars: &fakeCalendarRepo{accessible: []store.CalendarAccess{
			{Calendar: store.Calendar{ID: 1, UserID: 1, Name: "Work", Timezone: &chicago}, Editor: true},
		}},
		Events: &fakeEventRepo{events: events},
	}}
}

// TestFreeBusyQueryOverAHeterogeneousCollection drives the whole §7.10 report
// through the handler: the store narrowing, the §7.3 zone, the §9.9 candidate
// test, the transparency and status table, and the cross-resource merge all run
// in front of the output. Each of those is pinned on its own elsewhere, over a
// candidate slice built by hand -- which is a different claim from them holding
// together on the request an ordinary client sends.
func TestFreeBusyQueryOverAHeterogeneousCollection(t *testing.T) {
	body := `<C:free-busy-query xmlns:C="urn:ietf:params:xml:ns:caldav">` +
		`<C:time-range start="20240601T000000Z" end="20240604T000000Z"/></C:free-busy-query>`

	rr := httptest.NewRecorder()
	heterogeneousFreeBusyServer(t).Report(rr,
		freeBusyRequestFor("/dav/calendars/1/", body, &store.User{ID: 1}))

	if rr.Code != http.StatusOK {
		t.Fatalf("free-busy-query = %d, want 200: %s", rr.Code, rr.Body.String())
	}
	published := parsedFreeBusyLines(t, rr.Body.String())
	// Emission order, not a sorted view: RFC 5545 §3.8.2.6 asks for ascending
	// start then end, and the tentative instance is what a result grouped by
	// FBTYPE would put last instead of third.
	assertPublishedFreeBusy(t, published, []string{
		// The third shifted-range slot, typed by the override governing its
		// recurrence ID even though its scheduled start moved before that ID.
		"FREEBUSY;FBTYPE=BUSY-TENTATIVE:20240601T060000Z/20240601T070000Z",
		// The first instance and the resource abutting it, merged.
		"FREEBUSY:20240601T090000Z/20240601T110000Z",
		// The stored busy period. Its FBTYPE=FREE sibling publishes nothing, and
		// neither does the transparent or the cancelled resource.
		"FREEBUSY:20240601T140000Z/20240601T150000Z",
		// The in-range component from a resource containing only overrides.
		"FREEBUSY;FBTYPE=BUSY-TENTATIVE:20240601T160000Z/20240601T170000Z",
		"FREEBUSY:20240601T180000Z/20240601T190000Z",
		// The override types its own instance and no other.
		"FREEBUSY;FBTYPE=BUSY-TENTATIVE:20240602T090000Z/20240602T100000Z",
		// 12:00 floating placed in America/Chicago rather than in UTC.
		"FREEBUSY:20240602T170000Z/20240602T180000Z",
		"FREEBUSY:20240603T090000Z/20240603T100000Z",
	})
}

func TestCalendarDataAndFreeBusyComposeAcrossSubmittedTimezoneTransition(t *testing.T) {
	raw := strings.Join([]string{
		"BEGIN:VCALENDAR",
		"VERSION:2.0",
		"PRODID:-//CalCard//DST Test//EN",
		"BEGIN:VTIMEZONE",
		"TZID:Review/Chicago",
		"BEGIN:STANDARD",
		"DTSTART:19701101T020000",
		"TZOFFSETFROM:-0500",
		"TZOFFSETTO:-0600",
		"RRULE:FREQ=YEARLY;BYMONTH=11;BYDAY=1SU",
		"END:STANDARD",
		"BEGIN:DAYLIGHT",
		"DTSTART:19700308T020000",
		"TZOFFSETFROM:-0600",
		"TZOFFSETTO:-0500",
		"RRULE:FREQ=YEARLY;BYMONTH=3;BYDAY=2SU",
		"END:DAYLIGHT",
		"END:VTIMEZONE",
		"BEGIN:VEVENT",
		"UID:dst",
		"DTSTAMP:20240301T080000Z",
		"DTSTART;TZID=Review/Chicago:20240309T090000",
		"DTEND;TZID=Review/Chicago:20240309T100000",
		"RRULE:FREQ=DAILY;COUNT=3",
		"STATUS:TENTATIVE",
		"SUMMARY:Morning review",
		"END:VEVENT",
		"END:VCALENDAR",
		"",
	}, "\r\n")
	start := time.Date(2024, 3, 9, 15, 0, 0, 0, time.UTC)
	end := start.Add(time.Hour)
	server := &DavServer{store: &store.Store{
		Calendars: &fakeCalendarRepo{accessible: []store.CalendarAccess{
			{Calendar: store.Calendar{ID: 1, UserID: 1, Name: "Test"}, Editor: true},
		}},
		Events: &fakeEventRepo{events: map[string]*store.Event{
			"1:dst": {
				CalendarID: 1, UID: "dst", ResourceName: "dst", ETag: "dst-etag",
				RawICAL: raw, DTStart: &start, DTEnd: &end,
			},
		}},
	}}
	user := &store.User{ID: 1}
	wantStarts := []string{"20240309T150000Z", "20240310T140000Z", "20240311T140000Z"}

	t.Run("calendar-data expansion and property selection", func(t *testing.T) {
		body := calendarQueryBody(`<D:prop><D:getetag/><C:calendar-data>` +
			`<C:comp name="VCALENDAR"><C:comp name="VEVENT"><C:prop name="UID"/><C:prop name="DTSTART"/></C:comp></C:comp>` +
			`<C:expand start="20240309T000000Z" end="20240312T000000Z"/>` +
			`</C:calendar-data></D:prop>` +
			`<C:filter><C:comp-filter name="VCALENDAR"><C:comp-filter name="VEVENT"/></C:comp-filter></C:filter>`)
		req := reportRequestFor("/dav/calendars/1/", body, user)
		req.Header.Set("Depth", "1")
		rr := httptest.NewRecorder()
		server.Report(rr, req)
		if rr.Code != http.StatusMultiStatus {
			t.Fatalf("calendar-query = %d, want 207: %s", rr.Code, rr.Body.String())
		}
		value, err := scalarText(decodeMultistatus(t, rr).
			responseForHref(t, "/dav/calendars/1/dst.ics").
			assertPropStatus(t, calQN("calendar-data"), http.StatusOK))
		if err != nil {
			t.Fatal(err)
		}
		properties := icalendarPropertiesIn(t, value, "VCALENDAR", "VEVENT")
		if !slices.Equal(properties["DTSTART"], wantStarts) {
			t.Errorf("expanded DTSTARTs = %v, want %v; value:\n%s", properties["DTSTART"], wantStarts, value)
		}
		if !slices.Equal(properties["RECURRENCE-ID"], wantStarts) {
			t.Errorf("expanded RECURRENCE-IDs = %v, want %v; value:\n%s", properties["RECURRENCE-ID"], wantStarts, value)
		}
		if got := properties["SUMMARY"]; len(got) != 0 {
			t.Errorf("unselected SUMMARY survived: %v; value:\n%s", got, value)
		}
	})

	t.Run("free-busy recurrence and type", func(t *testing.T) {
		body := `<C:free-busy-query xmlns:C="urn:ietf:params:xml:ns:caldav">` +
			`<C:time-range start="20240309T000000Z" end="20240312T000000Z"/></C:free-busy-query>`
		rr := httptest.NewRecorder()
		server.Report(rr, freeBusyRequestFor("/dav/calendars/1/", body, user))
		if rr.Code != http.StatusOK {
			t.Fatalf("free-busy-query = %d, want 200: %s", rr.Code, rr.Body.String())
		}
		assertPublishedFreeBusy(t, parsedFreeBusyLines(t, rr.Body.String()), []string{
			"FREEBUSY;FBTYPE=BUSY-TENTATIVE:20240309T150000Z/20240309T160000Z",
			"FREEBUSY;FBTYPE=BUSY-TENTATIVE:20240310T140000Z/20240310T150000Z",
			"FREEBUSY;FBTYPE=BUSY-TENTATIVE:20240311T140000Z/20240311T150000Z",
		})
	})
}
