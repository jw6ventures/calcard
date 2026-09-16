package dav

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"reflect"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/jw6ventures/calcard/internal/auth"
	"github.com/jw6ventures/calcard/internal/store"
)

// projectFor runs the §9.6 projection over raw with no zone, which is the UTC
// reading every fixture here is written in.
func projectFor(raw string, selection *calendarDataEl) string {
	return mustReportText(filterICalendarData(raw, newCalendarDataProjection(selection, floatingZone{})))
}

// projectForZone is projectFor against the zone RFC 4791 §7.3 gives the report,
// which is what a floating value in the stored octets resolves through.
func projectForZone(raw string, selection *calendarDataEl, zone floatingZone) string {
	return mustReportText(filterICalendarData(raw, newCalendarDataProjection(selection, zone)))
}

// vTimezoneObject is an iCalendar object carrying one VTIMEZONE. The observance
// is a placeholder: an IANA TZID resolves through the host's tzdata, which is
// what a real client sends and what these fixtures depend on.
func vTimezoneObject(tzid string) string {
	return "BEGIN:VCALENDAR\r\nVERSION:2.0\r\nPRODID:-//CalCard//EN\r\n" +
		"BEGIN:VTIMEZONE\r\nTZID:" + tzid + "\r\n" +
		"BEGIN:STANDARD\r\nDTSTART:19700101T000000\r\n" +
		"TZOFFSETFROM:+0000\r\nTZOFFSETTO:+0000\r\nEND:STANDARD\r\n" +
		"END:VTIMEZONE\r\nEND:VCALENDAR\r\n"
}

// chicagoVTimezone is America/Chicago as a client actually submits it: both
// observances with the yearly rules that generate their transitions, so the
// definition describes CST (-06:00) and CDT (-05:00) rather than one fixed
// offset.
//
// A fixture meaning "this zone's offsets" has to carry them. RFC 4791 §7.3
// resolves a floating value against the definition the request supplied, so a
// stub claiming +00:00 under an IANA name describes a zone that is not the one
// it is named after, and any case asserting the named zone's arithmetic would
// only pass by the host's database answering over it.
func chicagoVTimezone() string {
	return strings.Join([]string{
		"BEGIN:VTIMEZONE",
		"TZID:America/Chicago",
		"BEGIN:DAYLIGHT",
		"DTSTART:20070311T020000",
		"TZOFFSETFROM:-0600",
		"TZOFFSETTO:-0500",
		"RRULE:FREQ=YEARLY;BYMONTH=3;BYDAY=2SU",
		"END:DAYLIGHT",
		"BEGIN:STANDARD",
		"DTSTART:20071104T020000",
		"TZOFFSETFROM:-0500",
		"TZOFFSETTO:-0600",
		"RRULE:FREQ=YEARLY;BYMONTH=11;BYDAY=1SU",
		"END:STANDARD",
		"END:VTIMEZONE",
		"",
	}, "\r\n")
}

// chicagoVTimezoneObject is chicagoVTimezone wrapped in the VCALENDAR a
// CALDAV:timezone value carries.
func chicagoVTimezoneObject() string {
	return "BEGIN:VCALENDAR\r\nVERSION:2.0\r\nPRODID:-//CalCard//EN\r\n" +
		chicagoVTimezone() + "END:VCALENDAR\r\n"
}

// componentNames lists the top-level components of a projection. It parses
// rather than analyzes, because RFC 4791 §9.6 permits returned data to be
// invalid per its media type when the request selected less than the media type
// requires -- and a projection naming no scheduled component is exactly that.
func componentNames(t *testing.T, value string) []string {
	t.Helper()
	root, err := parseICalendarObject(value)
	if err != nil {
		t.Fatalf("projection is not a parseable iCalendar object: %v; value:\n%s", err, value)
	}
	var names []string
	for _, child := range root.children {
		names = append(names, child.name)
	}
	return names
}

func propertyNames(t *testing.T, value string, path ...string) []string {
	t.Helper()
	var names []string
	for name := range icalendarPropertiesIn(t, value, path...) {
		names = append(names, name)
	}
	sort.Strings(names)
	return names
}

const projectionFixture = "BEGIN:VCALENDAR\r\n" +
	"VERSION:2.0\r\n" +
	"PRODID:-//CalCard//EN\r\n" +
	"BEGIN:VTIMEZONE\r\n" +
	"TZID:UTC\r\n" +
	"END:VTIMEZONE\r\n" +
	"BEGIN:VEVENT\r\n" +
	"UID:event-1\r\n" +
	"DTSTAMP:20240101T080000Z\r\n" +
	"DTSTART:20240101T090000Z\r\n" +
	"DTEND:20240101T100000Z\r\n" +
	"SUMMARY:Test Event\r\n" +
	"BEGIN:VALARM\r\n" +
	"ACTION:DISPLAY\r\n" +
	"DESCRIPTION:Reminder\r\n" +
	"TRIGGER:-PT15M\r\n" +
	"END:VALARM\r\n" +
	"END:VEVENT\r\n" +
	"END:VCALENDAR\r\n"

// RFC 4791 §9.6 returns a calendar object resource in its entirety when the
// request names no CALDAV:comp, so the stored octets go out exactly as they
// came in.
func TestCalendarDataWithNoCompReturnsTheResourceUnchanged(t *testing.T) {
	version := "2.0"
	for name, selection := range map[string]*calendarDataEl{
		"no selection at all":    nil,
		"attributes only":        {Version: &version},
		"an empty calendar-data": {},
	} {
		t.Run(name, func(t *testing.T) {
			if got := projectFor(projectionFixture, selection); got != projectionFixture {
				t.Fatalf("projection rewrote unselected data:\n got %q\nwant %q", got, projectionFixture)
			}
		})
	}
}

// TestCalendarDataLeavesUnparseableOctetsAlone keeps a resource whose stored
// octets predate the strict validator readable: a projection cannot be derived
// from a tree that was never built, and returning nothing would lose the data.
func TestCalendarDataLeavesUnparseableOctetsAlone(t *testing.T) {
	raw := "not an iCalendar object at all"
	selection := &calendarDataEl{Comp: &calendarComp{Name: "VCALENDAR"}}
	if got := projectFor(raw, selection); got != raw {
		t.Fatalf("projection = %q, want the stored octets %q", got, raw)
	}
}

// The §9.6.1 content model is ((allprop | prop*), (allcomp | comp*)), so a comp
// naming neither alternative matches zero of each: the component alone.
func TestCalendarDataCompSelectsOnlyTheNamedComponents(t *testing.T) {
	tests := map[string]struct {
		selection      *calendarComp
		wantComponents []string
	}{
		"an empty VCALENDAR comp keeps no sub-component": {
			selection:      &calendarComp{Name: "VCALENDAR"},
			wantComponents: nil,
		},
		"a named sub-component excludes its siblings": {
			selection: &calendarComp{Name: "VCALENDAR", Comp: []calendarComp{
				{Name: "VEVENT"},
			}},
			wantComponents: []string{"VEVENT"},
		},
		"the timezone alone": {
			selection: &calendarComp{Name: "VCALENDAR", Comp: []calendarComp{
				{Name: "VTIMEZONE"},
			}},
			wantComponents: []string{"VTIMEZONE"},
		},
		"a comp naming an inner component is read under an implicit VCALENDAR": {
			selection:      &calendarComp{Name: "VEVENT"},
			wantComponents: []string{"VEVENT"},
		},
		"component names match case-insensitively": {
			selection: &calendarComp{Name: "vcalendar", Comp: []calendarComp{
				{Name: "vevent"},
			}},
			wantComponents: []string{"VEVENT"},
		},
	}

	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			got := projectFor(projectionFixture, &calendarDataEl{Comp: test.selection})
			if names := componentNames(t, got); !reflect.DeepEqual(names, test.wantComponents) {
				t.Fatalf("top-level components = %v, want %v; value:\n%s", names, test.wantComponents, got)
			}
			if properties := icalendarPropertiesIn(t, got, "VCALENDAR"); len(properties) != 0 {
				t.Errorf("VCALENDAR carries unrequested properties %v; value:\n%s", properties, got)
			}
		})
	}
}

// The prop* half of the §9.6.1 model, and the exactness §9.6 permits: a
// property the request did not name is absent even when the media type
// requires it.
func TestCalendarDataPropSelectsOnlyTheNamedProperties(t *testing.T) {
	got := projectFor(projectionFixture, &calendarDataEl{Comp: &calendarComp{
		Name: "VCALENDAR",
		Comp: []calendarComp{{
			Name: "VEVENT",
			Prop: []calendarProp{{Name: "UID"}, {Name: "SUMMARY"}},
		}},
	}})
	assertICalendarComponentProperties(t, got, "VEVENT", map[string]string{
		"UID":     "event-1",
		"SUMMARY": "Test Event",
	})
	if properties := icalendarPropertiesIn(t, got, "VCALENDAR"); len(properties) != 0 {
		t.Errorf("VCALENDAR carries unrequested properties %v; value:\n%s", properties, got)
	}
	if names := componentNames(t, got); !reflect.DeepEqual(names, []string{"VEVENT"}) {
		t.Fatalf("top-level components = %v, want [VEVENT]; value:\n%s", names, got)
	}
	if strings.Contains(strings.ToUpper(got), "BEGIN:VALARM") {
		t.Fatalf("a comp naming no sub-component kept the VALARM; value:\n%s", got)
	}
}

// The CalDAV allprop of §9.6.2, rather than the DAV: one that shares its local
// name.
func TestCalendarDataAllPropReturnsEveryPropertyOfTheComponent(t *testing.T) {
	got := projectFor(projectionFixture, &calendarDataEl{Comp: &calendarComp{
		Name: "VCALENDAR",
		Comp: []calendarComp{{Name: "VEVENT", AllProp: true}},
	}})
	assertICalendarComponentProperties(t, got, "VEVENT", map[string]string{
		"UID":     "event-1",
		"DTSTAMP": "20240101T080000Z",
		"DTSTART": "20240101T090000Z",
		"DTEND":   "20240101T100000Z",
		"SUMMARY": "Test Event",
	})
	if strings.Contains(strings.ToUpper(got), "BEGIN:VALARM") {
		t.Fatalf("allprop pulled in a sub-component; value:\n%s", got)
	}
}

// The §9.6.3 allcomp takes the whole subtree, properties and all, not just the
// component names.
func TestCalendarDataAllCompReturnsEverySubComponentInFull(t *testing.T) {
	got := projectFor(projectionFixture, &calendarDataEl{Comp: &calendarComp{
		Name: "VCALENDAR",
		Comp: []calendarComp{{
			Name:    "VEVENT",
			Prop:    []calendarProp{{Name: "UID"}},
			AllComp: true,
		}},
	}})
	alarm := icalendarPropertiesIn(t, got, "VCALENDAR", "VEVENT", "VALARM")
	for name, want := range map[string]string{
		"ACTION":      "DISPLAY",
		"DESCRIPTION": "Reminder",
		"TRIGGER":     "-PT15M",
	} {
		if values := alarm[name]; len(values) != 1 || values[0] != want {
			t.Fatalf("VALARM %s = %v, want [%s]; value:\n%s", name, values, want, got)
		}
	}
	assertICalendarComponentProperties(t, got, "VEVENT", map[string]string{"UID": "event-1"})
}

func TestCalendarDataAllCompAtTheCalendarLevelKeepsEveryComponent(t *testing.T) {
	got := projectFor(projectionFixture, &calendarDataEl{Comp: &calendarComp{
		Name:    "VCALENDAR",
		AllProp: true,
		AllComp: true,
	}})
	if got != projectionFixture {
		t.Fatalf("allprop plus allcomp changed the resource:\n got %q\nwant %q", got, projectionFixture)
	}
}

// §9.6.4 returns the property name, its parameters and a trailing colon, with
// the value data dropped.
func TestCalendarDataNoValueSuppressesOnlyTheValue(t *testing.T) {
	raw := "BEGIN:VCALENDAR\r\n" +
		"VERSION:2.0\r\n" +
		"PRODID:-//CalCard//EN\r\n" +
		"BEGIN:VEVENT\r\n" +
		"UID:event-1\r\n" +
		"SUMMARY;LANGUAGE=en-GB:Test Event\r\n" +
		"END:VEVENT\r\n" +
		"END:VCALENDAR\r\n"

	tests := map[string]struct {
		noValue bool
		want    string
	}{
		"novalue yes drops the value":   {noValue: true, want: "SUMMARY;LANGUAGE=en-GB:\r\n"},
		"the default returns the value": {noValue: false, want: "SUMMARY;LANGUAGE=en-GB:Test Event\r\n"},
	}

	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			got := projectFor(raw, &calendarDataEl{Comp: &calendarComp{
				Name: "VCALENDAR",
				Comp: []calendarComp{{
					Name: "VEVENT",
					Prop: []calendarProp{{Name: "SUMMARY", NoValue: test.noValue}},
				}},
			}})
			if !strings.Contains(got, test.want) {
				t.Fatalf("projection does not carry %q; value:\n%s", test.want, got)
			}
		})
	}
}

// RFC 4791 §7.7 requires non-standard component, property and parameter names
// to be selectable, so the projection matches on the name rather than against a
// list of known ones.
func TestCalendarDataSelectsNonStandardNames(t *testing.T) {
	raw := "BEGIN:VCALENDAR\r\n" +
		"VERSION:2.0\r\n" +
		"PRODID:-//CalCard//EN\r\n" +
		"BEGIN:VEVENT\r\n" +
		"UID:event-1\r\n" +
		"X-ALT-DESC;X-VENDOR-FLAG=on:custom\r\n" +
		"SUMMARY:Test Event\r\n" +
		"BEGIN:X-WOMBAT\r\n" +
		"X-DEPTH:1\r\n" +
		"END:X-WOMBAT\r\n" +
		"END:VEVENT\r\n" +
		"END:VCALENDAR\r\n"

	got := projectFor(raw, &calendarDataEl{Comp: &calendarComp{
		Name: "VCALENDAR",
		Comp: []calendarComp{{
			Name: "VEVENT",
			Prop: []calendarProp{{Name: "x-alt-desc"}},
			Comp: []calendarComp{{Name: "X-WOMBAT", AllProp: true}},
		}},
	}})

	if !strings.Contains(got, "X-ALT-DESC;X-VENDOR-FLAG=on:custom\r\n") {
		t.Fatalf("the non-standard property and its parameter did not survive; value:\n%s", got)
	}
	if strings.Contains(got, "SUMMARY:") {
		t.Fatalf("an unselected property survived; value:\n%s", got)
	}
	if values := icalendarPropertiesIn(t, got, "VCALENDAR", "VEVENT", "X-WOMBAT")["X-DEPTH"]; len(values) != 1 {
		t.Fatalf("the non-standard component was not selected; value:\n%s", got)
	}
}

// TestCalendarDataPropKeepsEveryOccurrence covers a repeating property, where
// selecting the first occurrence alone would silently drop attendees.
func TestCalendarDataPropKeepsEveryOccurrence(t *testing.T) {
	raw := "BEGIN:VCALENDAR\r\n" +
		"VERSION:2.0\r\n" +
		"PRODID:-//CalCard//EN\r\n" +
		"BEGIN:VEVENT\r\n" +
		"UID:event-1\r\n" +
		"ATTENDEE:mailto:a@example.com\r\n" +
		"ATTENDEE:mailto:b@example.com\r\n" +
		"END:VEVENT\r\n" +
		"END:VCALENDAR\r\n"

	got := projectFor(raw, &calendarDataEl{Comp: &calendarComp{
		Name: "VCALENDAR",
		Comp: []calendarComp{{Name: "VEVENT", Prop: []calendarProp{{Name: "ATTENDEE"}}}},
	}})
	if values := icalendarPropertiesIn(t, got, "VCALENDAR", "VEVENT")["ATTENDEE"]; len(values) != 2 {
		t.Fatalf("ATTENDEE occurrences = %v, want both; value:\n%s", values, got)
	}
}

// --- CALDAV:expand (RFC 4791 §9.6.5) ---

const recurringFixture = "BEGIN:VCALENDAR\r\n" +
	"VERSION:2.0\r\n" +
	"PRODID:-//CalCard//EN\r\n" +
	"BEGIN:VEVENT\r\n" +
	"UID:event-1\r\n" +
	"DTSTAMP:20240601T080000Z\r\n" +
	"DTSTART:20240601T100000Z\r\n" +
	"DTEND:20240601T110000Z\r\n" +
	"RRULE:FREQ=DAILY;COUNT=5\r\n" +
	"SUMMARY:Daily\r\n" +
	"END:VEVENT\r\n" +
	"END:VCALENDAR\r\n"

func mustUTC(t *testing.T, value string) time.Time {
	t.Helper()
	parsed, ok := parseUTCDateTime(value)
	if !ok {
		t.Fatalf("fixture %q is not a date with UTC time", value)
	}
	return parsed
}

func expandRange(t *testing.T, from, until string) *calendarRange {
	t.Helper()
	return &calendarRange{Start: mustUTC(t, from), End: mustUTC(t, until)}
}

// §9.6.5 expands the recurrence set into components that define exactly one
// instance each. A resource holding a recurring component beside a one-off is
// the ordinary collection shape, and only the recurring one has a set to
// expand.
func TestCalendarDataExpandReturnsOneComponentPerInstance(t *testing.T) {
	raw := strings.Replace(recurringFixture, "END:VCALENDAR\r\n", strings.Join(componentLines("VEVENT",
		"UID:event-2", "DTSTAMP:20240601T080000Z", "DTSTART:20240602T140000Z", "DTEND:20240602T150000Z",
		"SUMMARY:One off"), "\r\n")+"\r\nEND:VCALENDAR\r\n", 1)

	got := projectFor(raw, &calendarDataEl{
		Expand: expandRange(t, "20240601T000000Z", "20240604T000000Z"),
	})
	starts := icalendarPropertiesIn(t, got, "VCALENDAR", "VEVENT")["DTSTART"]
	want := []string{
		"20240601T100000Z", "20240602T100000Z", "20240603T100000Z",
		"20240602T140000Z",
	}
	if !reflect.DeepEqual(starts, want) {
		t.Fatalf("expanded DTSTARTs = %v, want %v; value:\n%s", starts, want, got)
	}
	if names := componentNames(t, got); len(names) != 4 {
		t.Fatalf("expanded components = %v, want four; value:\n%s", names, got)
	}
	// Three identifiers, not four: the one-off names no slot in any pattern.
	if ids := icalendarPropertiesIn(t, got, "VCALENDAR", "VEVENT")["RECURRENCE-ID"]; len(ids) != 3 {
		t.Errorf("RECURRENCE-IDs = %v, want one per generated instance only; value:\n%s", ids, got)
	}
}

// Intersection is judged by the §9.9 VEVENT table rather than by a bare start
// comparison: an instance ending exactly at the range start does not intersect,
// and one starting exactly at the non-inclusive end does not either.
func TestCalendarDataExpandReturnsOnlyIntersectingInstances(t *testing.T) {
	tests := map[string]struct {
		from, until string
		want        []string
	}{
		"an instance ending exactly at the start is excluded": {
			from: "20240601T110000Z", until: "20240602T100000Z",
			want: nil,
		},
		"an instance starting exactly at the end is excluded": {
			from: "20240601T000000Z", until: "20240601T100000Z",
			want: nil,
		},
		"an instance overlapping by one second is included": {
			from: "20240601T105959Z", until: "20240601T110000Z",
			want: []string{"20240601T100000Z"},
		},
	}

	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			got := projectFor(recurringFixture, &calendarDataEl{
				Expand: expandRange(t, test.from, test.until),
			})
			starts := icalendarPropertiesIn(t, got, "VCALENDAR", "VEVENT")["DTSTART"]
			if !reflect.DeepEqual(starts, test.want) {
				t.Fatalf("expanded DTSTARTs = %v, want %v; value:\n%s", starts, test.want, got)
			}
		})
	}
}

// §9.6.5 requires the RECURRENCE-ID on every instance but the first, and the
// §7.8.6 example puts one on all of them, so every instance carries the one
// naming it.
func TestCalendarDataExpandedInstancesCarryARecurrenceID(t *testing.T) {
	got := projectFor(recurringFixture, &calendarDataEl{
		Expand: expandRange(t, "20240601T000000Z", "20240604T000000Z"),
	})
	properties := icalendarPropertiesIn(t, got, "VCALENDAR", "VEVENT")
	ids := properties["RECURRENCE-ID"]
	if !reflect.DeepEqual(ids, properties["DTSTART"]) {
		t.Fatalf("RECURRENCE-IDs = %v, want them to name each instance %v; value:\n%s",
			ids, properties["DTSTART"], got)
	}
	seen := make(map[string]struct{}, len(ids))
	for _, id := range ids {
		if _, duplicate := seen[id]; duplicate {
			t.Fatalf("two instances share RECURRENCE-ID %s; value:\n%s", id, got)
		}
		seen[id] = struct{}{}
	}
}

// A component carrying no recurrence set already defines exactly one instance,
// so §9.6.5 has no set to expand it into and no slot for a RECURRENCE-ID to
// name. Synthesizing one presents a one-off as an override of a recurrence set
// that does not exist -- which a client resolving overrides against a master
// then has no master for -- and RECURRENCE-ID is not a property RFC 5545 §3.6.4
// gives VFREEBUSY at all.
func TestCalendarDataExpandLeavesANonRecurringComponentAlone(t *testing.T) {
	tests := map[string]struct {
		component   string
		properties  []string
		from, until string
		wantStart   string
	}{
		"a one-off event inside the range": {
			component:  "VEVENT",
			properties: []string{"UID:one-off", "DTSTAMP:20240601T080000Z", "DTSTART:20240601T090000Z", "DTEND:20240601T100000Z"},
			from:       "20240601T000000Z", until: "20240605T000000Z",
			wantStart: "20240601T090000Z",
		},
		"a one-off event outside the range": {
			component:  "VEVENT",
			properties: []string{"UID:one-off", "DTSTAMP:20240601T080000Z", "DTSTART:20240601T090000Z", "DTEND:20240601T100000Z"},
			from:       "20240701T000000Z", until: "20240705T000000Z",
		},
		"an all-day one-off event": {
			component:  "VEVENT",
			properties: []string{"UID:all-day", "DTSTAMP:20240601T080000Z", "DTSTART;VALUE=DATE:20240601"},
			from:       "20240601T000000Z", until: "20240605T000000Z",
			wantStart: "20240601",
		},
		"a stored free-busy component": {
			component: "VFREEBUSY",
			properties: []string{"UID:fb", "DTSTAMP:20240601T080000Z", "DTSTART:20240601T000000Z",
				"DTEND:20240610T000000Z", "FREEBUSY:20240601T100000Z/20240601T110000Z"},
			from: "20240601T000000Z", until: "20240605T000000Z",
			wantStart: "20240601T000000Z",
		},
	}

	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			raw := wrapCalendar(componentLines(test.component, test.properties...)...)
			got := projectFor(raw, &calendarDataEl{Expand: expandRange(t, test.from, test.until)})

			properties := icalendarPropertiesIn(t, got, "VCALENDAR", test.component)
			var wantStarts []string
			if test.wantStart != "" {
				wantStarts = []string{test.wantStart}
			}
			if !reflect.DeepEqual(properties["DTSTART"], wantStarts) {
				t.Fatalf("DTSTARTs = %v, want %v; value:\n%s", properties["DTSTART"], wantStarts, got)
			}
			if ids := properties["RECURRENCE-ID"]; len(ids) != 0 {
				t.Errorf("a component with no recurrence set gained RECURRENCE-ID %v; value:\n%s", ids, got)
			}
		})
	}
}

// TestCalendarDataExpandKeepsTheRecurrenceIDThroughPropertySelection pins the
// interaction between §9.6.5 and §9.6.1, which is the shape a real client
// sends: expand alongside a comp/prop selection that does not name
// RECURRENCE-ID. §9.6.5's requirement is unconditional, and the identifier only
// exists because the server expanded, so the selection cannot decline it.
func TestCalendarDataExpandKeepsTheRecurrenceIDThroughPropertySelection(t *testing.T) {
	got := projectFor(recurringFixture, &calendarDataEl{
		Comp: &calendarComp{Name: "VCALENDAR", Comp: []calendarComp{{
			Name: "VEVENT",
			Prop: []calendarProp{{Name: "UID"}, {Name: "SUMMARY"}},
		}}},
		Expand: expandRange(t, "20240601T000000Z", "20240604T000000Z"),
	})

	properties := icalendarPropertiesIn(t, got, "VCALENDAR", "VEVENT")
	want := []string{"20240601T100000Z", "20240602T100000Z", "20240603T100000Z"}
	if !reflect.DeepEqual(properties["RECURRENCE-ID"], want) {
		t.Fatalf("RECURRENCE-IDs = %v, want %v; value:\n%s", properties["RECURRENCE-ID"], want, got)
	}
	// The selection still governs everything it does name.
	if len(properties["SUMMARY"]) != 3 {
		t.Errorf("SUMMARYs = %v, want one per instance; value:\n%s", properties["SUMMARY"], got)
	}
	if len(properties["DTSTAMP"]) != 0 {
		t.Errorf("an unselected property survived: %v; value:\n%s", properties["DTSTAMP"], got)
	}
}

func TestCalendarDataExpandIANAZoneAcrossDST(t *testing.T) {
	raw := wrapCalendar(componentLines("VEVENT",
		"UID:iana-dst", "DTSTAMP:20240301T080000Z",
		"DTSTART;TZID=America/Chicago:20240309T090000",
		"DTEND;TZID=America/Chicago:20240309T100000",
		"RRULE:FREQ=DAILY;COUNT=3")...)

	got := projectFor(raw, &calendarDataEl{
		Expand: expandRange(t, "20240309T000000Z", "20240312T000000Z"),
	})
	properties := icalendarPropertiesIn(t, got, "VCALENDAR", "VEVENT")
	want := []string{"20240309T150000Z", "20240310T140000Z", "20240311T140000Z"}
	if !reflect.DeepEqual(properties["DTSTART"], want) {
		t.Fatalf("expanded DTSTARTs = %v, want %v; value:\n%s", properties["DTSTART"], want, got)
	}
	if !reflect.DeepEqual(properties["RECURRENCE-ID"], want) {
		t.Fatalf("expanded RECURRENCE-IDs = %v, want %v; value:\n%s", properties["RECURRENCE-ID"], want, got)
	}
}

// §9.6.5: the returned components may use no recurrence property and may not
// refer to or include a VTIMEZONE.
//
// The zone reference is a property-level rule rather than a date-property one.
// RFC 5545 §3.2.19 admits a TZID on any DATE-TIME value, so a non-standard
// property and an alarm's absolute TRIGGER can both carry one, and the
// VTIMEZONE that would resolve them is exactly what this element removes.
func TestCalendarDataExpandDropsRecurrencePropertiesAndVTimezone(t *testing.T) {
	raw := "BEGIN:VCALENDAR\r\n" +
		"VERSION:2.0\r\n" +
		"PRODID:-//CalCard//EN\r\n" +
		"BEGIN:VTIMEZONE\r\n" +
		"TZID:America/New_York\r\n" +
		"BEGIN:STANDARD\r\n" +
		"DTSTART:19701101T020000\r\n" +
		"TZOFFSETFROM:-0400\r\n" +
		"TZOFFSETTO:-0500\r\n" +
		"END:STANDARD\r\n" +
		"END:VTIMEZONE\r\n" +
		"BEGIN:VEVENT\r\n" +
		"UID:event-1\r\n" +
		"DTSTAMP:20240601T080000Z\r\n" +
		"DTSTART;TZID=America/New_York:20240601T100000\r\n" +
		"DTEND;TZID=America/New_York:20240601T110000\r\n" +
		"RRULE:FREQ=DAILY;COUNT=3\r\n" +
		"EXDATE;TZID=America/New_York:20240602T100000\r\n" +
		"X-CALCARD-REMINDER;TZID=America/New_York:20240601T093000\r\n" +
		"BEGIN:VALARM\r\n" +
		"ACTION:DISPLAY\r\n" +
		"DESCRIPTION:Reminder\r\n" +
		"TRIGGER;VALUE=DATE-TIME;TZID=America/New_York:20240601T093000\r\n" +
		"END:VALARM\r\n" +
		"END:VEVENT\r\n" +
		"END:VCALENDAR\r\n"

	got := projectFor(raw, &calendarDataEl{
		Expand: expandRange(t, "20240601T000000Z", "20240604T000000Z"),
	})

	upper := strings.ToUpper(got)
	for _, forbidden := range []string{"RRULE", "RDATE", "EXDATE", "EXRULE", "BEGIN:VTIMEZONE", "TZID="} {
		if strings.Contains(upper, forbidden) {
			t.Fatalf("expanded output still carries %s; value:\n%s", forbidden, got)
		}
	}
	// New York is UTC-5 under this observance, and neither value moves with the
	// instance: only the effective DTSTART, DTEND and DUE do.
	properties := icalendarPropertiesIn(t, got, "VCALENDAR", "VEVENT")
	if values := properties["X-CALCARD-REMINDER"]; len(values) != 2 || values[0] != "20240601T143000Z" {
		t.Errorf("X-CALCARD-REMINDER = %v, want the zoned value rewritten as UTC; value:\n%s", values, got)
	}
	alarms := icalendarPropertiesIn(t, got, "VCALENDAR", "VEVENT", "VALARM")
	if values := alarms["TRIGGER"]; len(values) != 2 || values[0] != "20240601T143000Z" {
		t.Errorf("TRIGGER = %v, want the zoned value rewritten as UTC; value:\n%s", values, got)
	}
}

func TestCalendarDataExpandRewritesCalendarLevelTZIDProperties(t *testing.T) {
	raw := "BEGIN:VCALENDAR\r\n" +
		"VERSION:2.0\r\n" +
		"PRODID:-//CalCard//EN\r\n" +
		"X-CALCARD-WINDOW;TZID=Review/Eastern:20240601T100000\r\n" +
		"BEGIN:VTIMEZONE\r\n" +
		"TZID:Review/Eastern\r\n" +
		"BEGIN:STANDARD\r\n" +
		"DTSTART:19701101T020000\r\n" +
		"TZOFFSETFROM:-0500\r\n" +
		"TZOFFSETTO:-0500\r\n" +
		"END:STANDARD\r\n" +
		"END:VTIMEZONE\r\n" +
		"BEGIN:VEVENT\r\n" +
		"UID:event-1\r\n" +
		"DTSTAMP:20240601T080000Z\r\n" +
		"DTSTART:20240601T100000Z\r\n" +
		"DTEND:20240601T110000Z\r\n" +
		"END:VEVENT\r\n" +
		"END:VCALENDAR\r\n"

	got := projectFor(raw, &calendarDataEl{
		Expand: expandRange(t, "20240601T000000Z", "20240602T000000Z"),
	})
	root, err := parseICalendarObject(got)
	if err != nil {
		t.Fatalf("parse expanded output: %v; value:\n%s", err, got)
	}
	property, ok := firstICalProperty(root, "X-CALCARD-WINDOW")
	if !ok {
		t.Fatalf("calendar-level X-CALCARD-WINDOW is missing; value:\n%s", got)
	}
	if _, present := property.parameters["TZID"]; present {
		t.Errorf("calendar-level property still references a removed VTIMEZONE: %+v; value:\n%s", property, got)
	}
	if property.value != "20240601T150000Z" {
		t.Errorf("calendar-level property value = %q, want 20240601T150000Z; value:\n%s", property.value, got)
	}
	if root.childCount("VTIMEZONE") != 0 {
		t.Errorf("expanded output still carries a VTIMEZONE; value:\n%s", got)
	}
}

// §9.9 defines a time-range test for five component names and RFC 4791 §9.6.5
// asks only for recurrence sets to be expanded, so a component neither reaches
// is returned as it stands. Dropping it would lose data §5.3.3 requires a
// server to preserve, on the strength of a test that never ran.
func TestCalendarDataExpandKeepsComponentsNoTimeRangeTestReaches(t *testing.T) {
	raw := "BEGIN:VCALENDAR\r\n" +
		"VERSION:2.0\r\n" +
		"PRODID:-//CalCard//EN\r\n" +
		"BEGIN:VEVENT\r\n" +
		"UID:event-1\r\n" +
		"DTSTAMP:20240601T080000Z\r\n" +
		"DTSTART:20240601T100000Z\r\n" +
		"DTEND:20240601T110000Z\r\n" +
		"END:VEVENT\r\n" +
		"BEGIN:X-CALCARD-BLOCK\r\n" +
		"X-CALCARD-LABEL:focus\r\n" +
		"END:X-CALCARD-BLOCK\r\n" +
		"END:VCALENDAR\r\n"

	got := projectFor(raw, &calendarDataEl{
		Expand: expandRange(t, "20240601T000000Z", "20240602T000000Z"),
	})

	if names := componentNames(t, got); !reflect.DeepEqual(names, []string{"VEVENT", "X-CALCARD-BLOCK"}) {
		t.Fatalf("components = %v, want the non-standard one kept beside the expanded event; value:\n%s", names, got)
	}
	if values := icalendarPropertiesIn(t, got, "VCALENDAR", "X-CALCARD-BLOCK")["X-CALCARD-LABEL"]; len(values) != 1 {
		t.Errorf("the non-standard component came back without its properties; value:\n%s", got)
	}
}

// §9.6.5 converts a date and local time carrying a time zone reference to a
// date with UTC time.
func TestCalendarDataExpandConvertsZonedValuesToUTC(t *testing.T) {
	if _, err := time.LoadLocation("America/New_York"); err != nil {
		t.Skip("tzdata unavailable")
	}
	raw := "BEGIN:VCALENDAR\r\n" +
		"VERSION:2.0\r\n" +
		"PRODID:-//CalCard//EN\r\n" +
		"BEGIN:VEVENT\r\n" +
		"UID:event-1\r\n" +
		"DTSTAMP:20240601T080000Z\r\n" +
		"DTSTART;TZID=America/New_York:20240601T100000\r\n" +
		"DTEND;TZID=America/New_York:20240601T110000\r\n" +
		"END:VEVENT\r\n" +
		"END:VCALENDAR\r\n"

	got := projectFor(raw, &calendarDataEl{
		Expand: expandRange(t, "20240601T000000Z", "20240602T000000Z"),
	})
	properties := icalendarPropertiesIn(t, got, "VCALENDAR", "VEVENT")
	// New York is UTC-4 on that date.
	if values := properties["DTSTART"]; len(values) != 1 || values[0] != "20240601T140000Z" {
		t.Fatalf("DTSTART = %v, want [20240601T140000Z]; value:\n%s", values, got)
	}
	if values := properties["DTEND"]; len(values) != 1 || values[0] != "20240601T150000Z" {
		t.Fatalf("DTEND = %v, want [20240601T150000Z]; value:\n%s", values, got)
	}
}

// The other side of that conversion: §9.6.5 mandates it only for a value
// carrying a time zone reference, and rewriting an all-day value as an instant
// would destroy the fact that it is all-day.
func TestCalendarDataExpandKeepsAllDayAndFloatingValuesAsWritten(t *testing.T) {
	tests := map[string]struct {
		dtstart string
		want    string
	}{
		"an all-day value stays a DATE": {
			dtstart: "DTSTART;VALUE=DATE:20240601",
			want:    "20240602",
		},
		"a floating value stays floating": {
			dtstart: "DTSTART:20240601T100000",
			want:    "20240602T100000",
		},
	}

	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			raw := "BEGIN:VCALENDAR\r\n" +
				"VERSION:2.0\r\n" +
				"PRODID:-//CalCard//EN\r\n" +
				"BEGIN:VEVENT\r\n" +
				"UID:event-1\r\n" +
				"DTSTAMP:20240601T080000Z\r\n" +
				test.dtstart + "\r\n" +
				"RRULE:FREQ=DAILY;COUNT=3\r\n" +
				"END:VEVENT\r\n" +
				"END:VCALENDAR\r\n"

			got := projectFor(raw, &calendarDataEl{
				Expand: expandRange(t, "20240602T000000Z", "20240603T000000Z"),
			})
			values := icalendarPropertiesIn(t, got, "VCALENDAR", "VEVENT")["DTSTART"]
			if len(values) != 1 || values[0] != test.want {
				t.Fatalf("DTSTART = %v, want [%s]; value:\n%s", values, test.want, got)
			}
		})
	}
}

// A floating DTSTART names a wall clock rather than an instant, so which
// instances an expansion range admits depends on the zone RFC 4791 §7.3 gives
// the report. Reading it as UTC when the request named a zone returns instances
// the client's own range excludes.
func TestCalendarDataExpandResolvesFloatingValuesThroughTheReportTimezone(t *testing.T) {
	if _, err := time.LoadLocation("America/Chicago"); err != nil {
		t.Skip("tzdata unavailable")
	}
	raw := "BEGIN:VCALENDAR\r\n" +
		"VERSION:2.0\r\n" +
		"PRODID:-//CalCard//EN\r\n" +
		"BEGIN:VEVENT\r\n" +
		"UID:event-1\r\n" +
		"DTSTAMP:20240601T080000Z\r\n" +
		"DTSTART:20240601T090000\r\n" +
		"DTEND:20240601T100000\r\n" +
		"RRULE:FREQ=DAILY;COUNT=2\r\n" +
		"END:VEVENT\r\n" +
		"END:VCALENDAR\r\n"

	// The range ends at 12:00Z. Read as UTC the first instance starts at 09:00Z
	// and is inside it; read in Chicago it starts at 14:00Z and is not.
	selection := &calendarDataEl{Expand: expandRange(t, "20240601T000000Z", "20240601T120000Z")}
	chicago := newFloatingZone(chicagoVTimezoneObject())

	utcInstances := len(icalendarPropertiesIn(t, projectFor(raw, selection), "VCALENDAR", "VEVENT")["DTSTART"])
	if utcInstances != 1 {
		t.Fatalf("under UTC the range admits %d instances, want 1", utcInstances)
	}
	zoned := projectForZone(raw, selection, chicago)
	if got := len(icalendarPropertiesIn(t, zoned, "VCALENDAR", "VEVENT")["DTSTART"]); got != 0 {
		t.Errorf("under America/Chicago the range admits %d instances, want 0; value:\n%s", got, zoned)
	}
}

func TestCalendarDataExpandPreservesFloatingWallClockAcrossSubmittedFallBack(t *testing.T) {
	zoneDefinition := strings.Join([]string{
		"BEGIN:VCALENDAR",
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
		"END:VCALENDAR",
		"",
	}, "\r\n")
	raw := "BEGIN:VCALENDAR\r\n" +
		"VERSION:2.0\r\n" +
		"PRODID:-//CalCard//EN\r\n" +
		"BEGIN:VEVENT\r\n" +
		"UID:event-1\r\n" +
		"DTSTAMP:20241101T080000Z\r\n" +
		"DTSTART:20241102T003000\r\n" +
		"DTEND:20241102T013000\r\n" +
		"RRULE:FREQ=DAILY;COUNT=3\r\n" +
		"END:VEVENT\r\n" +
		"END:VCALENDAR\r\n"

	got := projectForZone(raw, &calendarDataEl{
		Expand: expandRange(t, "20241102T000000Z", "20241105T000000Z"),
	}, newFloatingZone(zoneDefinition))
	properties := icalendarPropertiesIn(t, got, "VCALENDAR", "VEVENT")
	want := []string{"20241102T003000", "20241103T003000", "20241104T003000"}
	if starts := properties["DTSTART"]; !reflect.DeepEqual(starts, want) {
		t.Errorf("expanded DTSTARTs = %v, want %v; value:\n%s", starts, want, got)
	}
	if ids := properties["RECURRENCE-ID"]; !reflect.DeepEqual(ids, want) {
		t.Errorf("expanded RECURRENCE-IDs = %v, want %v; value:\n%s", ids, want, got)
	}
}

// TestCalendarDataExpandEmitsOverrideComponentsAsThemselves covers the shape
// §9.6.5 leaves implicit: an overridden instance already defines exactly one
// instance, so it is returned as written rather than regenerated, and the
// generated instance it replaced is not returned as well.
func TestCalendarDataExpandEmitsOverrideComponentsAsThemselves(t *testing.T) {
	raw := "BEGIN:VCALENDAR\r\n" +
		"VERSION:2.0\r\n" +
		"PRODID:-//CalCard//EN\r\n" +
		"BEGIN:VEVENT\r\n" +
		"UID:event-1\r\n" +
		"DTSTAMP:20240601T080000Z\r\n" +
		"DTSTART:20240601T100000Z\r\n" +
		"DTEND:20240601T110000Z\r\n" +
		"RRULE:FREQ=DAILY;COUNT=3\r\n" +
		"SUMMARY:Daily\r\n" +
		"END:VEVENT\r\n" +
		"BEGIN:VEVENT\r\n" +
		"UID:event-1\r\n" +
		"RECURRENCE-ID:20240602T100000Z\r\n" +
		"DTSTAMP:20240601T080000Z\r\n" +
		"DTSTART:20240602T150000Z\r\n" +
		"DTEND:20240602T160000Z\r\n" +
		"SUMMARY:Moved\r\n" +
		"END:VEVENT\r\n" +
		"END:VCALENDAR\r\n"

	got := projectFor(raw, &calendarDataEl{
		Expand: expandRange(t, "20240602T000000Z", "20240603T000000Z"),
	})
	properties := icalendarPropertiesIn(t, got, "VCALENDAR", "VEVENT")
	if starts := properties["DTSTART"]; !reflect.DeepEqual(starts, []string{"20240602T150000Z"}) {
		t.Fatalf("DTSTARTs = %v, want only the override's moved time; value:\n%s", starts, got)
	}
	if summaries := properties["SUMMARY"]; !reflect.DeepEqual(summaries, []string{"Moved"}) {
		t.Fatalf("SUMMARYs = %v, want the override's own; value:\n%s", summaries, got)
	}
	if ids := properties["RECURRENCE-ID"]; !reflect.DeepEqual(ids, []string{"20240602T100000Z"}) {
		t.Fatalf("RECURRENCE-IDs = %v, want the instance the override replaced; value:\n%s", ids, got)
	}
}

// thisAndFutureFixture is a daily master with a RANGE=THISANDFUTURE override at
// the third occurrence that both moves the time and rewrites the content.
const thisAndFutureFixture = "BEGIN:VCALENDAR\r\n" +
	"VERSION:2.0\r\n" +
	"PRODID:-//CalCard//EN\r\n" +
	"BEGIN:VEVENT\r\n" +
	"UID:event-1\r\n" +
	"DTSTAMP:20240601T080000Z\r\n" +
	"DTSTART:20240601T100000Z\r\n" +
	"DTEND:20240601T110000Z\r\n" +
	"RRULE:FREQ=DAILY;COUNT=5\r\n" +
	"SUMMARY:Original\r\n" +
	"LOCATION:Room A\r\n" +
	"END:VEVENT\r\n" +
	"BEGIN:VEVENT\r\n" +
	"UID:event-1\r\n" +
	"RECURRENCE-ID;RANGE=THISANDFUTURE:20240603T100000Z\r\n" +
	"DTSTAMP:20240601T080000Z\r\n" +
	"DTSTART:20240603T150000Z\r\n" +
	"DTEND:20240603T160000Z\r\n" +
	"SUMMARY:Moved\r\n" +
	"LOCATION:Room B\r\n" +
	"END:VEVENT\r\n" +
	"END:VCALENDAR\r\n"

// TestCalendarDataExpandAppliesThisAndFutureOverrides covers the three things a
// RANGE=THISANDFUTURE override changes about an expansion. RFC 5545 §3.8.4.4
// makes it describe the instance it names and every later one, so from that slot
// onward the override is what the instance *is*: the master no longer describes
// it, and the override is not a separate instance of its own.
func TestCalendarDataExpandAppliesThisAndFutureOverrides(t *testing.T) {
	got := projectFor(thisAndFutureFixture, &calendarDataEl{
		Expand: expandRange(t, "20240601T000000Z", "20240607T000000Z"),
	})
	properties := icalendarPropertiesIn(t, got, "VCALENDAR", "VEVENT")

	// One component per instance: the override does not also appear as a
	// component of its own for the slot it names.
	if names := componentNames(t, got); len(names) != 5 {
		t.Fatalf("expanded components = %d, want 5 (one per instance); value:\n%s", len(names), got)
	}

	// The occurrences from the override's slot onward take its time.
	wantStarts := []string{
		"20240601T100000Z", "20240602T100000Z",
		"20240603T150000Z", "20240604T150000Z", "20240605T150000Z",
	}
	if !reflect.DeepEqual(properties["DTSTART"], wantStarts) {
		t.Errorf("DTSTARTs = %v, want %v; value:\n%s", properties["DTSTART"], wantStarts, got)
	}

	// RFC 5545 §3.8.4.4: a RECURRENCE-ID names the slot in the master's
	// pattern, which a THISANDFUTURE shift moves the occurrence away from. It is
	// not the occurrence's own DTSTART.
	wantIDs := []string{
		"20240601T100000Z", "20240602T100000Z",
		"20240603T100000Z", "20240604T100000Z", "20240605T100000Z",
	}
	if !reflect.DeepEqual(properties["RECURRENCE-ID"], wantIDs) {
		t.Errorf("RECURRENCE-IDs = %v, want the master's slots %v; value:\n%s",
			properties["RECURRENCE-ID"], wantIDs, got)
	}

	// The override describes those instances, so its content is theirs.
	wantSummaries := []string{"Original", "Original", "Moved", "Moved", "Moved"}
	if !reflect.DeepEqual(properties["SUMMARY"], wantSummaries) {
		t.Errorf("SUMMARYs = %v, want %v; value:\n%s", properties["SUMMARY"], wantSummaries, got)
	}
	wantLocations := []string{"Room A", "Room A", "Room B", "Room B", "Room B"}
	if !reflect.DeepEqual(properties["LOCATION"], wantLocations) {
		t.Errorf("LOCATIONs = %v, want %v; value:\n%s", properties["LOCATION"], wantLocations, got)
	}

	// Nothing in the output may still say it governs a range of instances.
	if strings.Contains(strings.ToUpper(got), "THISANDFUTURE") {
		t.Errorf("an expanded component still carries RANGE=THISANDFUTURE; value:\n%s", got)
	}
}

func TestCalendarDataExpandKeepsDistinctSlotsAtTheSameScheduledTime(t *testing.T) {
	raw := "BEGIN:VCALENDAR\r\n" +
		"VERSION:2.0\r\n" +
		"PRODID:-//CalCard//EN\r\n" +
		"BEGIN:VEVENT\r\n" +
		"UID:event-1\r\n" +
		"DTSTAMP:20240601T080000Z\r\n" +
		"DTSTART:20240601T100000Z\r\n" +
		"DTEND:20240601T110000Z\r\n" +
		"RRULE:FREQ=DAILY;COUNT=2\r\n" +
		"END:VEVENT\r\n" +
		"BEGIN:VEVENT\r\n" +
		"UID:event-1\r\n" +
		"RECURRENCE-ID;RANGE=THISANDFUTURE:20240602T100000Z\r\n" +
		"DTSTAMP:20240601T080000Z\r\n" +
		"DTSTART:20240601T100000Z\r\n" +
		"DTEND:20240601T110000Z\r\n" +
		"END:VEVENT\r\n" +
		"END:VCALENDAR\r\n"

	got := projectFor(raw, &calendarDataEl{
		Expand: expandRange(t, "20240601T000000Z", "20240603T000000Z"),
	})
	properties := icalendarPropertiesIn(t, got, "VCALENDAR", "VEVENT")
	if starts := properties["DTSTART"]; !reflect.DeepEqual(starts, []string{"20240601T100000Z", "20240601T100000Z"}) {
		t.Fatalf("DTSTARTs = %v, want both colliding instances; value:\n%s", starts, got)
	}
	if ids := properties["RECURRENCE-ID"]; !reflect.DeepEqual(ids, []string{"20240601T100000Z", "20240602T100000Z"}) {
		t.Fatalf("RECURRENCE-IDs = %v, want both distinct slots; value:\n%s", ids, got)
	}
}

// The nearest preceding RANGE=THISANDFUTURE override wins, since a later one
// supersedes an earlier one from its own slot onward.
func TestCalendarDataExpandUsesTheNearestPrecedingThisAndFutureOverride(t *testing.T) {
	raw := strings.Replace(thisAndFutureFixture, "END:VCALENDAR\r\n",
		"BEGIN:VEVENT\r\n"+
			"UID:event-1\r\n"+
			"RECURRENCE-ID;RANGE=THISANDFUTURE:20240605T100000Z\r\n"+
			"DTSTAMP:20240601T080000Z\r\n"+
			"DTSTART:20240605T180000Z\r\n"+
			"DTEND:20240605T190000Z\r\n"+
			"SUMMARY:Moved Again\r\n"+
			"LOCATION:Room C\r\n"+
			"END:VEVENT\r\n"+
			"END:VCALENDAR\r\n", 1)

	got := projectFor(raw, &calendarDataEl{
		Expand: expandRange(t, "20240601T000000Z", "20240607T000000Z"),
	})
	properties := icalendarPropertiesIn(t, got, "VCALENDAR", "VEVENT")

	wantSummaries := []string{"Original", "Original", "Moved", "Moved", "Moved Again"}
	if !reflect.DeepEqual(properties["SUMMARY"], wantSummaries) {
		t.Fatalf("SUMMARYs = %v, want %v; value:\n%s", properties["SUMMARY"], wantSummaries, got)
	}
	if got5 := properties["DTSTART"]; len(got5) != 5 || got5[4] != "20240605T180000Z" {
		t.Errorf("DTSTARTs = %v, want the last one moved to 18:00; value:\n%s", got5, got)
	}
}

// An ordinary override alongside a THISANDFUTURE one still stands for exactly
// its own instance, and takes precedence there over the range-governing one.
func TestCalendarDataExpandKeepsAPlainOverrideBesideAThisAndFutureOne(t *testing.T) {
	raw := strings.Replace(thisAndFutureFixture, "END:VCALENDAR\r\n",
		"BEGIN:VEVENT\r\n"+
			"UID:event-1\r\n"+
			"RECURRENCE-ID:20240604T100000Z\r\n"+
			"DTSTAMP:20240601T080000Z\r\n"+
			"DTSTART:20240604T200000Z\r\n"+
			"DTEND:20240604T210000Z\r\n"+
			"SUMMARY:Just This One\r\n"+
			"END:VEVENT\r\n"+
			"END:VCALENDAR\r\n", 1)

	got := projectFor(raw, &calendarDataEl{
		Expand: expandRange(t, "20240604T000000Z", "20240605T000000Z"),
	})
	properties := icalendarPropertiesIn(t, got, "VCALENDAR", "VEVENT")

	if starts := properties["DTSTART"]; !reflect.DeepEqual(starts, []string{"20240604T200000Z"}) {
		t.Fatalf("DTSTARTs = %v, want only the plain override's time; value:\n%s", starts, got)
	}
	if summaries := properties["SUMMARY"]; !reflect.DeepEqual(summaries, []string{"Just This One"}) {
		t.Errorf("SUMMARYs = %v, want the plain override's own; value:\n%s", summaries, got)
	}
}

// A THISANDFUTURE override is suppressed during expansion because the master's
// instances already carry it, so a resource with no master -- the shape RFC 4791
// §4.1 permits -- has to keep it. It stands for its own instance there, exactly
// as a plain override with no master does, and nothing is left to expand the
// range it would otherwise govern.
func TestCalendarDataExpandKeepsAMasterlessThisAndFutureOverride(t *testing.T) {
	raw := "BEGIN:VCALENDAR\r\n" +
		"VERSION:2.0\r\n" +
		"PRODID:-//CalCard//EN\r\n" +
		"BEGIN:VEVENT\r\n" +
		"UID:event-1\r\n" +
		"RECURRENCE-ID;RANGE=THISANDFUTURE:20240603T100000Z\r\n" +
		"DTSTAMP:20240601T080000Z\r\n" +
		"DTSTART:20240603T150000Z\r\n" +
		"DTEND:20240603T160000Z\r\n" +
		"SUMMARY:Only Instance\r\n" +
		"END:VEVENT\r\n" +
		"END:VCALENDAR\r\n"

	got := projectFor(raw, &calendarDataEl{
		Expand: expandRange(t, "20240601T000000Z", "20240607T000000Z"),
	})
	if names := componentNames(t, got); !reflect.DeepEqual(names, []string{"VEVENT"}) {
		t.Fatalf("components = %v, want the override itself; value:\n%s", names, got)
	}
	properties := icalendarPropertiesIn(t, got, "VCALENDAR", "VEVENT")
	if starts := properties["DTSTART"]; !reflect.DeepEqual(starts, []string{"20240603T150000Z"}) {
		t.Errorf("DTSTARTs = %v, want the override's own time; value:\n%s", starts, got)
	}
	if summaries := properties["SUMMARY"]; !reflect.DeepEqual(summaries, []string{"Only Instance"}) {
		t.Errorf("SUMMARYs = %v, want the override's own; value:\n%s", summaries, got)
	}
	if ids := properties["RECURRENCE-ID"]; !reflect.DeepEqual(ids, []string{"20240603T100000Z"}) {
		t.Errorf("RECURRENCE-IDs = %v, want the slot the override names; value:\n%s", ids, got)
	}
	// The component defines exactly one instance now, so nothing in it may still
	// claim to govern every later one.
	if strings.Contains(strings.ToUpper(got), "THISANDFUTURE") {
		t.Errorf("an expanded component still carries RANGE=THISANDFUTURE; value:\n%s", got)
	}
}

// TestCalendarDataExpandKeepsAnUnexpandableRecurrenceAsOneComponent pins the
// judgement call §9.6.5 does not make: a frequency the server cannot enumerate
// yields no instances, and keeping the RRULE would break the MUST NOT, so the
// master stands for the set once, stripped.
func TestCalendarDataExpandKeepsAnUnexpandableRecurrenceAsOneComponent(t *testing.T) {
	raw := "BEGIN:VCALENDAR\r\n" +
		"VERSION:2.0\r\n" +
		"PRODID:-//CalCard//EN\r\n" +
		"BEGIN:VEVENT\r\n" +
		"UID:event-1\r\n" +
		"DTSTAMP:20240601T080000Z\r\n" +
		"DTSTART:20240601T100000Z\r\n" +
		"DTEND:20240601T110000Z\r\n" +
		"RRULE:FREQ=NOVENA;COUNT=3\r\n" +
		"END:VEVENT\r\n" +
		"END:VCALENDAR\r\n"

	got := projectFor(raw, &calendarDataEl{
		Expand: expandRange(t, "20240601T000000Z", "20240602T000000Z"),
	})
	if names := componentNames(t, got); !reflect.DeepEqual(names, []string{"VEVENT"}) {
		t.Fatalf("components = %v, want one VEVENT; value:\n%s", names, got)
	}
	if strings.Contains(strings.ToUpper(got), "RRULE") {
		t.Fatalf("an unexpandable recurrence kept its RRULE; value:\n%s", got)
	}
}

// --- CALDAV:limit-recurrence-set (RFC 4791 §9.6.6) ---

// §9.6.6 returns the master and the overrides impacting the range. An override
// impacts it through its current scheduled time, its original one, or a RANGE
// parameter, and the master is always returned.
func TestCalendarDataLimitRecurrenceSetKeepsTheMasterAndImpactingOverrides(t *testing.T) {
	master := "BEGIN:VEVENT\r\n" +
		"UID:event-1\r\n" +
		"DTSTAMP:20240601T080000Z\r\n" +
		"DTSTART:20240601T100000Z\r\n" +
		"DTEND:20240601T110000Z\r\n" +
		"RRULE:FREQ=DAILY;COUNT=30\r\n" +
		"END:VEVENT\r\n"
	override := func(recurrenceID, dtstart, dtend string) string {
		return "BEGIN:VEVENT\r\n" +
			"UID:event-1\r\n" +
			"RECURRENCE-ID:" + recurrenceID + "\r\n" +
			"DTSTAMP:20240601T080000Z\r\n" +
			"DTSTART:" + dtstart + "\r\n" +
			"DTEND:" + dtend + "\r\n" +
			"END:VEVENT\r\n"
	}

	tests := map[string]struct {
		override string
		want     bool
	}{
		"inside the range by its current time only": {
			override: override("20240610T100000Z", "20240602T100000Z", "20240602T110000Z"),
			want:     true,
		},
		"inside the range by its original time only": {
			override: override("20240602T100000Z", "20240610T100000Z", "20240610T110000Z"),
			want:     true,
		},
		"outside the range by both times": {
			override: override("20240610T100000Z", "20240611T100000Z", "20240611T110000Z"),
			want:     false,
		},
		"a THISANDFUTURE override before the range governs instances inside it": {
			override: "BEGIN:VEVENT\r\nUID:event-1\r\n" +
				"RECURRENCE-ID;RANGE=THISANDFUTURE:20240601T100000Z\r\n" +
				"DTSTAMP:20240601T080000Z\r\n" +
				"DTSTART:20240601T120000Z\r\n" +
				"DTEND:20240601T130000Z\r\n" +
				"END:VEVENT\r\n",
			want: true,
		},
	}

	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			raw := "BEGIN:VCALENDAR\r\nVERSION:2.0\r\nPRODID:-//CalCard//EN\r\n" +
				master + test.override + "END:VCALENDAR\r\n"

			got := projectFor(raw, &calendarDataEl{
				LimitRecurrenceSet: expandRange(t, "20240602T000000Z", "20240603T000000Z"),
			})

			ids := icalendarPropertiesIn(t, got, "VCALENDAR", "VEVENT")["RECURRENCE-ID"]
			if kept := len(ids) == 1; kept != test.want {
				t.Fatalf("override kept = %v, want %v; value:\n%s", kept, test.want, got)
			}
			// The master is returned whatever happens to the overrides, and it
			// keeps its recurrence rule: this element narrows a set, it does not
			// expand one.
			if !strings.Contains(got, "RRULE:FREQ=DAILY;COUNT=30") {
				t.Fatalf("the master did not survive intact; value:\n%s", got)
			}
		})
	}
}

func TestCalendarDataLimitRecurrenceSetEvaluatesGovernedInstances(t *testing.T) {
	event := func(lines ...string) string {
		return strings.Join(append(append([]string{"BEGIN:VEVENT", "UID:event", "DTSTAMP:20240601T080000Z"}, lines...), "END:VEVENT", ""), "\r\n")
	}
	tests := []struct {
		name      string
		master    string
		overrides []string
		start     string
		end       string
		wantIDs   []string
	}{
		{
			name:   "finite series ended before the range",
			master: event("DTSTART:20240601T100000Z", "DTEND:20240601T110000Z", "RRULE:FREQ=DAILY;COUNT=2"),
			overrides: []string{event(
				"RECURRENCE-ID;RANGE=THISANDFUTURE:20240602T100000Z",
				"DTSTART:20240602T120000Z", "DTEND:20240602T130000Z",
			)},
			start: "20240610T000000Z", end: "20240611T000000Z",
		},
		{
			name:   "until-bounded series ended before the range",
			master: event("DTSTART:20240601T100000Z", "DTEND:20240601T110000Z", "RRULE:FREQ=DAILY;UNTIL=20240602T100000Z"),
			overrides: []string{event(
				"RECURRENCE-ID;RANGE=THISANDFUTURE:20240602T100000Z",
				"DTSTART:20240602T120000Z", "DTEND:20240602T130000Z",
			)},
			start: "20240603T000000Z", end: "20240604T000000Z",
		},
		{
			name:   "range override governs later RDATE slots",
			master: event("DTSTART:20240601T100000Z", "DTEND:20240601T110000Z", "RDATE:20240602T100000Z,20240603T100000Z"),
			overrides: []string{event(
				"RECURRENCE-ID;RANGE=THISANDFUTURE:20240602T100000Z",
				"DTSTART:20240603T100000Z", "DTEND:20240603T110000Z",
			)},
			start: "20240604T000000Z", end: "20240605T000000Z",
			wantIDs: []string{"20240602T100000Z"},
		},
		{
			name:   "excluded future slot does not make the range override relevant",
			master: event("DTSTART:20240601T100000Z", "DTEND:20240601T110000Z", "RRULE:FREQ=DAILY;COUNT=3", "EXDATE:20240603T100000Z"),
			overrides: []string{event(
				"RECURRENCE-ID;RANGE=THISANDFUTURE:20240602T100000Z",
				"DTSTART:20240602T100000Z", "DTEND:20240602T110000Z",
			)},
			start: "20240603T000000Z", end: "20240604T000000Z",
		},
		{
			name:   "negative shift brings a governed instance into the range",
			master: event("DTSTART:20240610T100000Z", "DTEND:20240610T110000Z", "RRULE:FREQ=DAILY;COUNT=3"),
			overrides: []string{event(
				"RECURRENCE-ID;RANGE=THISANDFUTURE:20240611T100000Z",
				"DTSTART:20240602T100000Z", "DTEND:20240602T110000Z",
			)},
			start: "20240603T000000Z", end: "20240604T000000Z",
			wantIDs: []string{"20240611T100000Z"},
		},
		{
			name:   "later range override ends the earlier override's run",
			master: event("DTSTART:20240601T100000Z", "DTEND:20240601T110000Z", "RRULE:FREQ=DAILY;COUNT=5"),
			overrides: []string{
				event("RECURRENCE-ID;RANGE=THISANDFUTURE:20240602T100000Z", "DTSTART:20240602T120000Z", "DTEND:20240602T130000Z"),
				event("RECURRENCE-ID;RANGE=THISANDFUTURE:20240603T100000Z", "DTSTART:20240610T100000Z", "DTEND:20240610T110000Z"),
			},
			start: "20240610T000000Z", end: "20240611T000000Z",
			wantIDs: []string{"20240603T100000Z"},
		},
		{
			name:   "cancelled range override impacts original future slots",
			master: event("DTSTART:20240601T100000Z", "DTEND:20240601T110000Z", "RRULE:FREQ=DAILY;COUNT=3"),
			overrides: []string{event(
				"RECURRENCE-ID;RANGE=THISANDFUTURE:20240602T100000Z", "STATUS:CANCELLED",
			)},
			start: "20240603T000000Z", end: "20240604T000000Z",
			wantIDs: []string{"20240602T100000Z"},
		},
		{
			name:   "ordinary override replaces the only in-range governed slot",
			master: event("DTSTART:20240601T100000Z", "DTEND:20240601T110000Z", "RRULE:FREQ=DAILY;COUNT=3"),
			overrides: []string{
				event("RECURRENCE-ID;RANGE=THISANDFUTURE:20240602T100000Z", "DTSTART:20240602T120000Z", "DTEND:20240602T130000Z"),
				event("RECURRENCE-ID:20240603T100000Z", "DTSTART:20240610T100000Z", "DTEND:20240610T110000Z"),
			},
			start: "20240603T000000Z", end: "20240604T000000Z",
			wantIDs: []string{"20240603T100000Z"},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			raw := "BEGIN:VCALENDAR\r\nVERSION:2.0\r\nPRODID:-//CalCard//EN\r\n" +
				test.master + strings.Join(test.overrides, "") + "END:VCALENDAR\r\n"
			got := projectFor(raw, &calendarDataEl{
				LimitRecurrenceSet: expandRange(t, test.start, test.end),
			})
			gotIDs := icalendarPropertiesIn(t, got, "VCALENDAR", "VEVENT")["RECURRENCE-ID"]
			if !reflect.DeepEqual(gotIDs, test.wantIDs) {
				t.Fatalf("RECURRENCE-IDs = %v, want %v; value:\n%s", gotIDs, test.wantIDs, got)
			}
		})
	}
}

func TestCalendarDataLimitRecurrenceSetKeepsMasterlessRangeOverrideByOriginalTime(t *testing.T) {
	raw := "BEGIN:VCALENDAR\r\n" +
		"VERSION:2.0\r\n" +
		"PRODID:-//CalCard//EN\r\n" +
		"BEGIN:VEVENT\r\n" +
		"UID:event-1\r\n" +
		"RECURRENCE-ID;RANGE=THISANDFUTURE:20240602T100000Z\r\n" +
		"DTSTAMP:20240601T080000Z\r\n" +
		"DTSTART:20240610T100000Z\r\n" +
		"DTEND:20240610T110000Z\r\n" +
		"END:VEVENT\r\n" +
		"END:VCALENDAR\r\n"

	got := projectFor(raw, &calendarDataEl{
		LimitRecurrenceSet: expandRange(t, "20240602T000000Z", "20240603T000000Z"),
	})
	ids := icalendarPropertiesIn(t, got, "VCALENDAR", "VEVENT")["RECURRENCE-ID"]
	want := []string{"20240602T100000Z"}
	if !reflect.DeepEqual(ids, want) {
		t.Fatalf("RECURRENCE-IDs = %v, want %v; value:\n%s", ids, want, got)
	}
}

// --- CALDAV:limit-freebusy-set (RFC 4791 §9.6.7) ---

// §9.6.7 returns only the FREEBUSY periods intersecting the range.
func TestCalendarDataLimitFreeBusySetKeepsOnlyIntersectingPeriods(t *testing.T) {
	raw := "BEGIN:VCALENDAR\r\n" +
		"VERSION:2.0\r\n" +
		"PRODID:-//CalCard//EN\r\n" +
		"BEGIN:VFREEBUSY\r\n" +
		"UID:freebusy-object\r\n" +
		"DTSTAMP:20240601T080000Z\r\n" +
		"DTSTART:20240601T000000Z\r\n" +
		"DTEND:20240610T000000Z\r\n" +
		"FREEBUSY:20240601T100000Z/20240601T110000Z,20240602T100000Z/20240602T110000Z,20240609T100000Z/20240609T110000Z\r\n" +
		"FREEBUSY:20240608T100000Z/PT1H\r\n" +
		"END:VFREEBUSY\r\n" +
		"END:VCALENDAR\r\n"

	got := projectFor(raw, &calendarDataEl{
		LimitFreeBusySet: expandRange(t, "20240601T000000Z", "20240603T000000Z"),
	})

	periods := icalendarPropertiesIn(t, got, "VCALENDAR", "VFREEBUSY")["FREEBUSY"]
	want := []string{"20240601T100000Z/20240601T110000Z,20240602T100000Z/20240602T110000Z"}
	if !reflect.DeepEqual(periods, want) {
		t.Fatalf("FREEBUSY = %v, want %v; value:\n%s", periods, want, got)
	}
	// The component itself is untouched, only its FREEBUSY values are trimmed.
	if names := propertyNames(t, got, "VCALENDAR", "VFREEBUSY"); !reflect.DeepEqual(names,
		[]string{"DTEND", "DTSTAMP", "DTSTART", "FREEBUSY", "UID"}) {
		t.Fatalf("VFREEBUSY properties = %v; value:\n%s", names, got)
	}
}

func TestCalendarDataLimitFreeBusySetLeavesOtherComponentsAlone(t *testing.T) {
	got := projectFor(projectionFixture, &calendarDataEl{
		LimitFreeBusySet: expandRange(t, "20240101T000000Z", "20240102T000000Z"),
	})
	if got != projectionFixture {
		t.Fatalf("limit-freebusy-set rewrote an object carrying no VFREEBUSY:\n got %q\nwant %q", got, projectionFixture)
	}
}

// --- the transforms and the selection together ---

// The §9.6 content model admits a CALDAV:comp beside a recurrence transform and
// a free-busy trim, which is the shape an ordinary client request takes. The
// transforms rewrite the recurrence set and the selection then decides what of
// it is returned, so the selection has to run last: run first it would judge
// components the transform has not produced yet, and trim values the transform
// was going to narrow anyway.
func TestCalendarDataAppliesEveryTransformInContentModelOrder(t *testing.T) {
	t.Run("a trimmed component is then narrowed by the selection", func(t *testing.T) {
		raw := "BEGIN:VCALENDAR\r\n" +
			"VERSION:2.0\r\n" +
			"PRODID:-//CalCard//EN\r\n" +
			"BEGIN:VFREEBUSY\r\n" +
			"UID:freebusy-object\r\n" +
			"DTSTAMP:20240601T080000Z\r\n" +
			"DTSTART:20240601T000000Z\r\n" +
			"DTEND:20240610T000000Z\r\n" +
			"FREEBUSY:20240601T100000Z/20240601T110000Z,20240609T100000Z/20240609T110000Z\r\n" +
			"END:VFREEBUSY\r\n" +
			"END:VCALENDAR\r\n"

		got := projectFor(raw, &calendarDataEl{
			Comp: &calendarComp{Name: "VCALENDAR", Comp: []calendarComp{{
				Name: "VFREEBUSY",
				Prop: []calendarProp{{Name: "UID"}, {Name: "FREEBUSY"}},
			}}},
			LimitFreeBusySet: expandRange(t, "20240601T000000Z", "20240602T000000Z"),
		})

		periods := icalendarPropertiesIn(t, got, "VCALENDAR", "VFREEBUSY")["FREEBUSY"]
		if want := []string{"20240601T100000Z/20240601T110000Z"}; !reflect.DeepEqual(periods, want) {
			t.Errorf("FREEBUSY = %v, want %v; value:\n%s", periods, want, got)
		}
		if names := propertyNames(t, got, "VCALENDAR", "VFREEBUSY"); !reflect.DeepEqual(names, []string{"FREEBUSY", "UID"}) {
			t.Errorf("VFREEBUSY properties = %v, want only the two selected; value:\n%s", names, got)
		}
	})

	t.Run("expansion feeds the selection while an inert trim stands beside it", func(t *testing.T) {
		got := projectFor(recurringFixture, &calendarDataEl{
			Comp: &calendarComp{Name: "VCALENDAR", Comp: []calendarComp{{
				Name: "VEVENT",
				Prop: []calendarProp{{Name: "UID"}, {Name: "DTSTART"}},
			}}},
			Expand:           expandRange(t, "20240601T000000Z", "20240604T000000Z"),
			LimitFreeBusySet: expandRange(t, "20240601T000000Z", "20240604T000000Z"),
		})

		properties := icalendarPropertiesIn(t, got, "VCALENDAR", "VEVENT")
		wantStarts := []string{"20240601T100000Z", "20240602T100000Z", "20240603T100000Z"}
		if !reflect.DeepEqual(properties["DTSTART"], wantStarts) {
			t.Errorf("expanded DTSTARTs = %v, want %v; value:\n%s", properties["DTSTART"], wantStarts, got)
		}
		// The identifier survives a selection that never named it, because
		// §9.6.5 requires it of every expanded instance unconditionally.
		if got := len(properties["RECURRENCE-ID"]); got != len(wantStarts) {
			t.Errorf("RECURRENCE-IDs = %d, want one per instance; value:\n%s", got, properties)
		}
		if got := properties["SUMMARY"]; len(got) != 0 {
			t.Errorf("an unselected property survived expansion: %v", got)
		}
	})
}

// --- The projection through the REPORT handler ---

// heterogeneousCollectionServer answers REPORTs against one calendar holding
// the component shapes a real collection mixes: a recurring master, a one-off
// event, an all-day one-off, a stored free-busy component, and a resource
// carrying the non-standard names §5.3.3 requires a server to preserve.
func heterogeneousCollectionServer() *DavServer {
	framed := func(lines []string) string {
		return wrapCalendar(append([]string{"VERSION:2.0", "PRODID:-//CalCard//EN"}, lines...)...)
	}
	// The two names expansion has no rule of its own for: a component §9.9
	// defines no time-range test for, and a property outside the date-valued set
	// that still references the VTIMEZONE §9.6.5 removes. The observance is
	// carried in the resource rather than left to the host's tzdata, so the
	// fixture resolves the same way everywhere.
	custom := []string{
		"BEGIN:VTIMEZONE", "TZID:America/New_York",
		"BEGIN:STANDARD", "DTSTART:19701101T020000",
		"TZOFFSETFROM:-0400", "TZOFFSETTO:-0500",
		"END:STANDARD", "END:VTIMEZONE",
	}
	custom = append(custom, componentLines("VEVENT",
		"UID:custom", "DTSTAMP:20240601T080000Z",
		"DTSTART;TZID=America/New_York:20240602T080000",
		"DTEND;TZID=America/New_York:20240602T090000",
		"SUMMARY:Retro", "DESCRIPTION:Zoned",
		"X-CALCARD-REMINDER;TZID=America/New_York:20240602T073000")...)
	custom = append(custom, componentLines("X-CALCARD-BLOCK", "X-CALCARD-LABEL:focus")...)
	colliding := componentLines("VEVENT",
		"UID:colliding", "DTSTAMP:20240601T080000Z", "DTSTART:20240601T160000Z",
		"DTEND:20240601T170000Z", "RRULE:FREQ=DAILY;COUNT=2", "SUMMARY:Original")
	colliding = append(colliding, componentLines("VEVENT",
		"UID:colliding", "DTSTAMP:20240601T080000Z",
		"RECURRENCE-ID;RANGE=THISANDFUTURE:20240602T160000Z",
		"DTSTART:20240601T160000Z", "DTEND:20240601T170000Z", "SUMMARY:Shifted")...)

	resources := map[string]string{
		"recurring": framed(componentLines("VEVENT",
			"UID:recurring", "DTSTAMP:20240601T080000Z", "DTSTART:20240601T100000Z",
			"DTEND:20240601T110000Z", "RRULE:FREQ=DAILY;COUNT=5",
			"SUMMARY:Standup", "DESCRIPTION:Every morning")),
		"one-off": framed(componentLines("VEVENT",
			"UID:one-off", "DTSTAMP:20240601T080000Z", "DTSTART:20240602T140000Z",
			"DTEND:20240602T150000Z", "SUMMARY:Review", "DESCRIPTION:Once")),
		"all-day": framed(componentLines("VEVENT",
			"UID:all-day", "DTSTAMP:20240601T080000Z", "DTSTART;VALUE=DATE:20240603",
			"SUMMARY:Offsite", "DESCRIPTION:All day")),
		"freebusy": framed(componentLines("VFREEBUSY",
			"UID:freebusy", "DTSTAMP:20240601T080000Z", "DTSTART:20240601T000000Z",
			"DTEND:20240602T000000Z", "FREEBUSY:20240601T120000Z/20240601T130000Z")),
		"custom":    framed(custom),
		"colliding": framed(colliding),
	}
	events := make(map[string]*store.Event, len(resources))
	for name, raw := range resources {
		events["1:"+name] = &store.Event{
			CalendarID: 1, UID: name, ResourceName: name, ETag: "etag-" + name, RawICAL: raw,
		}
	}
	return &DavServer{store: &store.Store{
		Calendars: &fakeCalendarRepo{accessible: []store.CalendarAccess{
			{Calendar: store.Calendar{ID: 1, UserID: 1, Name: "Test"}, Editor: true},
		}},
		Events: &fakeEventRepo{events: events},
	}}
}

// TestCalendarDataProjectionOverAHeterogeneousCollection drives CALDAV:expand
// together with a CALDAV:comp selection across resources that each need
// different treatment, which is the request an ordinary client sends and the
// one a fixture holding a single recurring master cannot produce. Expansion and
// selection being correct in isolation is not the same claim.
func TestCalendarDataProjectionOverAHeterogeneousCollection(t *testing.T) {
	body := calendarQueryBody(`<D:prop><D:getetag/>` +
		`<C:calendar-data>` +
		`<C:comp name="VCALENDAR"><C:prop name="VERSION"/>` +
		`<C:comp name="VEVENT"><C:prop name="UID"/><C:prop name="DTSTART"/><C:prop name="SUMMARY"/>` +
		`<C:prop name="X-CALCARD-REMINDER"/></C:comp>` +
		`<C:comp name="VFREEBUSY"><C:prop name="UID"/><C:prop name="DTSTART"/><C:prop name="FREEBUSY"/></C:comp>` +
		`<C:comp name="X-CALCARD-BLOCK"><C:allprop/></C:comp>` +
		`</C:comp>` +
		`<C:expand start="20240601T000000Z" end="20240604T000000Z"/>` +
		`</C:calendar-data></D:prop>` +
		`<C:filter><C:comp-filter name="VCALENDAR"/></C:filter>`)

	req := httptest.NewRequest("REPORT", "/dav/calendars/1/", strings.NewReader(body))
	req.Header.Set("Depth", "1")
	req = req.WithContext(auth.WithUser(req.Context(), &store.User{ID: 1}))
	rr := httptest.NewRecorder()
	heterogeneousCollectionServer().Report(rr, req)
	if rr.Code != http.StatusMultiStatus {
		t.Fatalf("REPORT = %d, want 207: %s", rr.Code, rr.Body.String())
	}
	multistatus := decodeMultistatus(t, rr)

	tests := map[string]struct {
		component  string
		wantStarts []string
		wantIDs    int
	}{
		"recurring": {
			component:  "VEVENT",
			wantStarts: []string{"20240601T100000Z", "20240602T100000Z", "20240603T100000Z"},
			wantIDs:    3,
		},
		"one-off": {component: "VEVENT", wantStarts: []string{"20240602T140000Z"}},
		"all-day": {component: "VEVENT", wantStarts: []string{"20240603"}},
		"freebusy": {
			component:  "VFREEBUSY",
			wantStarts: []string{"20240601T000000Z"},
		},
		// -0500 under the observance the resource carries.
		"custom": {component: "VEVENT", wantStarts: []string{"20240602T130000Z"}},
		"colliding": {
			component:  "VEVENT",
			wantStarts: []string{"20240601T160000Z", "20240601T160000Z"},
			wantIDs:    2,
		},
	}

	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			el := multistatus.
				responseForHref(t, "/dav/calendars/1/"+name+".ics").
				assertPropStatus(t, calQN("calendar-data"), http.StatusOK)
			value, err := scalarText(el)
			if err != nil {
				t.Fatal(err)
			}

			// DTSTART is the property that proves the two transforms composed
			// rather than one having run over the other's output: the selection
			// keeps it and expansion has to move it onto each instance, so a
			// value that is merely present says nothing and only the exact set
			// does. The all-day and free-busy resources carry the other half of
			// the claim -- expansion leaves a DATE a DATE, and a component with
			// no recurrence set keeps the value it was stored with.
			properties := icalendarPropertiesIn(t, value, "VCALENDAR", test.component)
			if got := len(properties["UID"]); got != len(test.wantStarts) {
				t.Fatalf("%s components = %d, want %d; value:\n%s", test.component, got, len(test.wantStarts), value)
			}
			if got := properties["DTSTART"]; !reflect.DeepEqual(got, test.wantStarts) {
				t.Errorf("DTSTARTs = %v, want %v; value:\n%s", got, test.wantStarts, value)
			}
			if got := properties["DESCRIPTION"]; len(got) != 0 {
				t.Errorf("an unselected property survived the projection: %v; value:\n%s", got, value)
			}
			if ids := properties["RECURRENCE-ID"]; len(ids) != test.wantIDs {
				t.Errorf("RECURRENCE-IDs = %v, want %d; value:\n%s", ids, test.wantIDs, value)
			}
			if strings.Contains(strings.ToUpper(value), "RRULE") {
				t.Errorf("expanded output still carries a recurrence property; value:\n%s", value)
			}
			// §9.6.5 removes the VTIMEZONE from every projection here, so no
			// resource may come back still naming one.
			if strings.Contains(strings.ToUpper(value), "TZID=") {
				t.Errorf("expanded output still references a VTIMEZONE; value:\n%s", value)
			}
			calendarProperties := icalendarPropertiesIn(t, value, "VCALENDAR")
			if got := calendarProperties["VERSION"]; !reflect.DeepEqual(got, []string{"2.0"}) {
				t.Errorf("VCALENDAR VERSION = %v, want [2.0]; value:\n%s", got, value)
			}
			if got := calendarProperties["PRODID"]; len(got) != 0 {
				t.Errorf("unrequested PRODID survived projection: %v; value:\n%s", got, value)
			}
		})
	}

	// The non-standard names, which the selection asks for and the expansion has
	// no rule of its own for. Both are §5.3.3 data a PUT stored, and expansion is
	// the one transform positioned to lose them.
	t.Run("non-standard names survive the expansion", func(t *testing.T) {
		el := multistatus.
			responseForHref(t, "/dav/calendars/1/custom.ics").
			assertPropStatus(t, calQN("calendar-data"), http.StatusOK)
		value, err := scalarText(el)
		if err != nil {
			t.Fatal(err)
		}
		if got := icalendarPropertiesIn(t, value, "VCALENDAR", "X-CALCARD-BLOCK")["X-CALCARD-LABEL"]; len(got) != 1 {
			t.Errorf("the non-standard component did not survive; value:\n%s", value)
		}
		// 07:30 at -0500, rewritten as UTC because the zone it named is gone.
		got := icalendarPropertiesIn(t, value, "VCALENDAR", "VEVENT")["X-CALCARD-REMINDER"]
		if want := []string{"20240602T123000Z"}; !reflect.DeepEqual(got, want) {
			t.Errorf("X-CALCARD-REMINDER = %v, want %v; value:\n%s", got, want, value)
		}
	})
}

// projectionReportBodies is one CALDAV:calendar-data selector in each REPORT
// body that can carry one. calendar-multiget and sync-collection read their
// selector through the same calendarDataProjection calendar-query does, so the
// three owe the client the same projected octets for the same resource.
func projectionReportBodies(selector string) map[string]string {
	return map[string]string{
		"calendar-query": calendarQueryBody(`<D:prop><D:getetag/>` + selector + `</D:prop>` +
			`<C:filter><C:comp-filter name="VCALENDAR"><C:comp-filter name="VEVENT"/></C:comp-filter></C:filter>`),
		"calendar-multiget": `<C:calendar-multiget xmlns:D="DAV:" xmlns:C="urn:ietf:params:xml:ns:caldav">` +
			`<D:prop><D:getetag/>` + selector + `</D:prop>` +
			`<D:href>/dav/calendars/1/recurring.ics</D:href></C:calendar-multiget>`,
		"sync-collection": `<D:sync-collection xmlns:D="DAV:" xmlns:C="urn:ietf:params:xml:ns:caldav">` +
			`<D:sync-token/><D:sync-level>1</D:sync-level>` +
			`<D:prop><D:getetag/>` + selector + `</D:prop></D:sync-collection>`,
	}
}

// TestCalendarDataProjectionIsTheSameInEveryReportThatCarriesASelector pins the
// wiring rather than the transform: one projection is built per request and
// handed to whichever report is running, so a report that dropped it, or was
// given a different §7.3 zone, would answer the same body with different octets.
// Only calendar-query drives the projection anywhere else in this suite.
func TestCalendarDataProjectionIsTheSameInEveryReportThatCarriesASelector(t *testing.T) {
	selector := `<C:calendar-data>` +
		`<C:comp name="VCALENDAR"><C:comp name="VEVENT">` +
		`<C:prop name="UID"/><C:prop name="DTSTART"/></C:comp></C:comp>` +
		`<C:expand start="20240601T000000Z" end="20240604T000000Z"/>` +
		`</C:calendar-data>`
	wantStarts := []string{"20240601T100000Z", "20240602T100000Z", "20240603T100000Z"}

	for report, body := range projectionReportBodies(selector) {
		t.Run(report, func(t *testing.T) {
			req := httptest.NewRequest("REPORT", "/dav/calendars/1/", strings.NewReader(body))
			req.Header.Set("Depth", "1")
			req = req.WithContext(auth.WithUser(req.Context(), &store.User{ID: 1}))
			rr := httptest.NewRecorder()
			heterogeneousCollectionServer().Report(rr, req)
			if rr.Code != http.StatusMultiStatus {
				t.Fatalf("%s = %d, want 207: %s", report, rr.Code, rr.Body.String())
			}

			value, err := scalarText(decodeMultistatus(t, rr).
				responseForHref(t, "/dav/calendars/1/recurring.ics").
				assertPropStatus(t, calQN("calendar-data"), http.StatusOK))
			if err != nil {
				t.Fatal(err)
			}
			properties := icalendarPropertiesIn(t, value, "VCALENDAR", "VEVENT")
			if got := properties["DTSTART"]; !reflect.DeepEqual(got, wantStarts) {
				t.Errorf("expanded DTSTARTs = %v, want %v; value:\n%s", got, wantStarts, value)
			}
			if got := properties["RECURRENCE-ID"]; !reflect.DeepEqual(got, wantStarts) {
				t.Errorf("RECURRENCE-IDs = %v, want %v; value:\n%s", got, wantStarts, value)
			}
			if got := properties["SUMMARY"]; len(got) != 0 {
				t.Errorf("an unselected property survived the projection: %v; value:\n%s", got, value)
			}
			if strings.Contains(strings.ToUpper(value), "RRULE") {
				t.Errorf("expanded output still carries a recurrence property; value:\n%s", value)
			}
		})
	}
}

// A §9.6 content-model violation is a violation wherever CALDAV:calendar-data is
// spelled. sync-collection is the report CalCard answers whose own body the
// grammar pass does not walk, so without the selector being checked there the
// same selector that is a 400 in a calendar-query would be honoured as a request
// for nothing and answer 207 with every resource unprojected.
func TestRFC4791_CalendarDataContentModelHoldsInEveryReport(t *testing.T) {
	outOfOrder := `<C:calendar-data>` +
		`<C:limit-freebusy-set start="20240601T000000Z" end="20240602T000000Z"/>` +
		`<C:comp name="VCALENDAR"/></C:calendar-data>`

	for report, body := range projectionReportBodies(outOfOrder) {
		t.Run(report, func(t *testing.T) {
			req := httptest.NewRequest("REPORT", "/dav/calendars/1/", strings.NewReader(body))
			req.Header.Set("Depth", "1")
			req = req.WithContext(auth.WithUser(req.Context(), &store.User{ID: 1}))
			rr := httptest.NewRecorder()
			heterogeneousCollectionServer().Report(rr, req)
			if rr.Code != http.StatusBadRequest {
				t.Fatalf("%s = %d, want 400: %s", report, rr.Code, rr.Body.String())
			}
		})
	}
}

// RFC 4791 §11 asks a server to take adequate precautions against a report
// crafted to consume excessive CPU. The §9.6.1 selection is matched against
// every property of every component of every resource the report returns, so a
// selection resolved by scanning would cost the request body multiplied by the
// collection: 350,000 selectors against 2,000 resources measured 10.6 s, under
// a body-size cap that admits 10 MB. Resolving by name makes the per-resource
// cost independent of how many selectors the body carries.
func TestCalendarDataSelectionCostIsIndependentOfSelectorCount(t *testing.T) {
	if testing.Short() {
		t.Skip("cost guard runs a large projection")
	}
	selectors := make([]calendarProp, 0, 200000)
	for i := 0; i < 200000; i++ {
		selectors = append(selectors, calendarProp{Name: fmt.Sprintf("X-ABSENT-%d", i)})
	}
	selectors = append(selectors, calendarProp{Name: "UID"}, calendarProp{Name: "SUMMARY"})
	selection := &calendarDataEl{Comp: &calendarComp{
		Name: "VCALENDAR",
		Comp: []calendarComp{{Name: "VEVENT", Prop: selectors}},
	}}

	projection := newCalendarDataProjection(selection, floatingZone{})
	start := time.Now()
	for i := 0; i < 2000; i++ {
		if got := mustReportText(filterICalendarData(projectionFixture, projection)); got == "" {
			t.Fatal("projection returned nothing")
		}
	}
	// The scan cost this replaces was seconds; the indexed cost is tens of
	// milliseconds. The bound is wide enough that only a return to scanning
	// trips it.
	if elapsed := time.Since(start); elapsed > 2*time.Second {
		t.Fatalf("2,000 resources against %d selectors took %s, want the per-resource cost to be independent of the selector count",
			len(selectors), elapsed)
	}

	assertICalendarComponentProperties(t, mustReportText(filterICalendarData(projectionFixture, projection)), "VEVENT", map[string]string{
		"UID":     "event-1",
		"SUMMARY": "Test Event",
	})
}

// A name repeated among siblings resolves to its first occurrence, which is
// where a scan in document order stopped.
func TestCalendarDataDuplicateSelectorNamesKeepTheFirst(t *testing.T) {
	t.Run("prop", func(t *testing.T) {
		got := projectFor(projectionFixture, &calendarDataEl{Comp: &calendarComp{
			Name: "VCALENDAR",
			Comp: []calendarComp{{Name: "VEVENT", Prop: []calendarProp{
				{Name: "SUMMARY"},
				{Name: "SUMMARY", NoValue: true},
			}}},
		}})
		assertICalendarComponentProperties(t, got, "VEVENT", map[string]string{"SUMMARY": "Test Event"})
	})

	t.Run("comp", func(t *testing.T) {
		got := projectFor(projectionFixture, &calendarDataEl{Comp: &calendarComp{
			Name: "VCALENDAR",
			Comp: []calendarComp{
				{Name: "VEVENT", Prop: []calendarProp{{Name: "UID"}}},
				{Name: "VEVENT", Prop: []calendarProp{{Name: "SUMMARY"}}},
			},
		}})
		assertICalendarComponentProperties(t, got, "VEVENT", map[string]string{"UID": "event-1"})
		if strings.Contains(strings.ToUpper(got), "SUMMARY") {
			t.Fatalf("the second CALDAV:comp of the same name was applied; value:\n%s", got)
		}
	})
}

// The shared index is an optimisation, never a condition of correctness: a
// projection assembled without it projects the same bytes.
func TestCalendarDataProjectionWithoutTheSharedIndexProjectsTheSame(t *testing.T) {
	selection := &calendarDataEl{Comp: &calendarComp{
		Name: "VCALENDAR",
		Comp: []calendarComp{{
			Name: "VEVENT",
			Prop: []calendarProp{{Name: "UID"}, {Name: "SUMMARY"}},
			Comp: []calendarComp{{Name: "VALARM", Prop: []calendarProp{{Name: "ACTION"}}}},
		}},
	}}

	indexed := mustReportText(filterICalendarData(projectionFixture, newCalendarDataProjection(selection, floatingZone{})))
	bare := mustReportText(filterICalendarData(projectionFixture, calendarDataProjection{selection: selection}))
	if indexed != bare {
		t.Fatalf("projection without the shared index differs:\nindexed:\n%s\nbare:\n%s", indexed, bare)
	}
}

func mustReportText(value string, err error) string {
	if err != nil {
		panic(err)
	}
	return value
}
