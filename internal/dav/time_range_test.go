package dav

import (
	"net/http"
	"net/http/httptest"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/jw6ventures/calcard/internal/auth"
	"github.com/jw6ventures/calcard/internal/store"
)

// wrapCalendar puts component lines inside a VCALENDAR so the strict parser
// accepts them.
func wrapCalendar(lines ...string) string {
	return "BEGIN:VCALENDAR\r\n" + strings.Join(lines, "\r\n") + "\r\nEND:VCALENDAR\r\n"
}

func componentLines(name string, properties ...string) []string {
	lines := []string{"BEGIN:" + name}
	lines = append(lines, properties...)
	return append(lines, "END:"+name)
}

// namedComponent finds the first component of the given name anywhere in the
// tree, returning it with the component that encloses it.
func namedComponent(root *icalNode, name string) (*icalNode, *icalNode) {
	if root.name == name {
		return root, nil
	}
	for _, child := range root.children {
		if child.name == name {
			return child, root
		}
		if found, parent := namedComponent(child, name); found != nil {
			return found, parent
		}
	}
	return nil, nil
}

// assertComponentInRange parses raw, locates the named component, and reports
// whether it intersects [start, end) under the §9.9 tables.
func assertComponentInRange(t *testing.T, raw, componentName, start, end string, zone floatingZone) bool {
	t.Helper()
	root, err := parseICalendarObject(raw)
	if err != nil {
		t.Fatalf("parseICalendarObject(%q) = %v", raw, err)
	}
	node, parent := namedComponent(root, componentName)
	if node == nil {
		t.Fatalf("no %s component in %q", componentName, raw)
	}
	rangeStart, rangeEnd, ok := calendarTimeRangeBounds(&timeRange{Start: start, End: end})
	if !ok {
		t.Fatalf("calendarTimeRangeBounds(%q, %q) not usable", start, end)
	}
	matcher := newCalendarTimeRangeMatcher(raw, root, zone)
	return matcher.componentSetInTimeRange(node, parent, rangeStart, rangeEnd)
}

// The VEVENT table of RFC 4791 §9.9, one case per row plus the boundary that
// separates its inclusive bound from its exclusive one.
func TestTimeRangeVEventTable(t *testing.T) {
	tests := []struct {
		name       string
		properties []string
		start      string
		end        string
		want       bool
	}{
		// Row Y|N|N|*: (start < DTEND AND end > DTSTART)
		{name: "DTEND overlapping", properties: []string{"DTSTART:20240601T100000Z", "DTEND:20240601T110000Z"}, start: "20240601T103000Z", end: "20240601T113000Z", want: true},
		{name: "DTEND meeting range start is exclusive", properties: []string{"DTSTART:20240601T090000Z", "DTEND:20240601T100000Z"}, start: "20240601T100000Z", end: "20240601T110000Z", want: false},
		{name: "DTSTART meeting range end is exclusive", properties: []string{"DTSTART:20240601T110000Z", "DTEND:20240601T120000Z"}, start: "20240601T100000Z", end: "20240601T110000Z", want: false},

		// Row N|Y|Y|*: (start < DTSTART+DURATION AND end > DTSTART)
		{name: "positive DURATION overlapping", properties: []string{"DTSTART:20240601T100000Z", "DURATION:PT1H"}, start: "20240601T103000Z", end: "20240601T113000Z", want: true},
		{name: "positive DURATION ending at range start", properties: []string{"DTSTART:20240601T090000Z", "DURATION:PT1H"}, start: "20240601T100000Z", end: "20240601T110000Z", want: false},
		{name: "positive DURATION covers a range start the columns would miss", properties: []string{"DTSTART:20240601T093000Z", "DURATION:PT1H"}, start: "20240601T100000Z", end: "20240601T110000Z", want: true},

		// Row N|Y|N|*: (start <= DTSTART AND end > DTSTART)
		{name: "zero DURATION at range start", properties: []string{"DTSTART:20240601T100000Z", "DURATION:PT0S"}, start: "20240601T100000Z", end: "20240601T110000Z", want: true},
		{name: "zero DURATION at range end", properties: []string{"DTSTART:20240601T110000Z", "DURATION:PT0S"}, start: "20240601T100000Z", end: "20240601T110000Z", want: false},

		// Row N|N|N|Y: DATE-TIME DTSTART alone, (start <= DTSTART AND end > DTSTART)
		{name: "bare DATE-TIME DTSTART at range start", properties: []string{"DTSTART:20240601T100000Z"}, start: "20240601T100000Z", end: "20240601T110000Z", want: true},
		{name: "bare DATE-TIME DTSTART before range", properties: []string{"DTSTART:20240601T095959Z"}, start: "20240601T100000Z", end: "20240601T110000Z", want: false},

		// Row N|N|N|N: DATE DTSTART alone, (start < DTSTART+P1D AND end > DTSTART)
		{name: "DATE DTSTART spans its whole day", properties: []string{"DTSTART;VALUE=DATE:20240601"}, start: "20240601T230000Z", end: "20240602T000000Z", want: true},
		{name: "DATE DTSTART does not reach the next day", properties: []string{"DTSTART;VALUE=DATE:20240601"}, start: "20240602T000000Z", end: "20240603T000000Z", want: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			raw := wrapCalendar(componentLines("VEVENT", append([]string{"UID:e"}, tt.properties...)...)...)
			if got := assertComponentInRange(t, raw, "VEVENT", tt.start, tt.end, floatingZone{}); got != tt.want {
				t.Fatalf("VEVENT in range = %v, want %v for %v", got, tt.want, tt.properties)
			}
		})
	}
}

// All eight rows of the RFC 4791 §9.9 VTODO table.
func TestTimeRangeVTodoTable(t *testing.T) {
	tests := []struct {
		name       string
		properties []string
		start      string
		end        string
		want       bool
	}{
		// Row Y|Y|N: (start <= DTSTART+DURATION) AND ((end > DTSTART) OR (end >= DTSTART+DURATION))
		{name: "DTSTART and DURATION overlapping", properties: []string{"DTSTART:20240601T100000Z", "DURATION:PT2H"}, start: "20240601T110000Z", end: "20240601T130000Z", want: true},
		{name: "DTSTART and DURATION entirely before range", properties: []string{"DTSTART:20240601T100000Z", "DURATION:PT1H"}, start: "20240601T120000Z", end: "20240601T130000Z", want: false},

		// Row Y|N|Y: ((start < DUE) OR (start <= DTSTART)) AND ((end > DTSTART) OR (end >= DUE))
		{name: "DTSTART and DUE overlapping", properties: []string{"DTSTART:20240601T100000Z", "DUE:20240601T120000Z"}, start: "20240601T110000Z", end: "20240601T130000Z", want: true},
		{name: "DTSTART and DUE entirely after range", properties: []string{"DTSTART:20240601T140000Z", "DUE:20240601T150000Z"}, start: "20240601T100000Z", end: "20240601T110000Z", want: false},

		// Row Y|N|N: (start <= DTSTART) AND (end > DTSTART)
		{name: "DTSTART alone at range start", properties: []string{"DTSTART:20240601T100000Z"}, start: "20240601T100000Z", end: "20240601T110000Z", want: true},
		{name: "DTSTART alone at range end", properties: []string{"DTSTART:20240601T110000Z"}, start: "20240601T100000Z", end: "20240601T110000Z", want: false},

		// Row N|N|Y: (start < DUE) AND (end >= DUE)
		{name: "DUE alone with range ending on it", properties: []string{"DUE:20240601T110000Z"}, start: "20240601T100000Z", end: "20240601T110000Z", want: true},
		{name: "DUE alone after the range", properties: []string{"DUE:20240601T120000Z"}, start: "20240601T100000Z", end: "20240601T110000Z", want: false},

		// Row N|N|N|Y|Y: COMPLETED and CREATED
		{name: "COMPLETED and CREATED reaching the range", properties: []string{"CREATED:20240601T090000Z", "COMPLETED:20240601T103000Z"}, start: "20240601T100000Z", end: "20240601T110000Z", want: true},
		{name: "COMPLETED and CREATED after the range", properties: []string{"CREATED:20240601T130000Z", "COMPLETED:20240601T140000Z"}, start: "20240601T100000Z", end: "20240601T110000Z", want: false},

		// Row N|N|N|Y|N: (start <= COMPLETED) AND (end >= COMPLETED)
		{name: "COMPLETED alone inside the range", properties: []string{"COMPLETED:20240601T103000Z"}, start: "20240601T100000Z", end: "20240601T110000Z", want: true},
		{name: "COMPLETED alone before the range", properties: []string{"COMPLETED:20240601T090000Z"}, start: "20240601T100000Z", end: "20240601T110000Z", want: false},

		// Row N|N|N|N|Y: (end > CREATED)
		{name: "CREATED alone before the range end", properties: []string{"CREATED:20240601T090000Z"}, start: "20240601T100000Z", end: "20240601T110000Z", want: true},
		{name: "CREATED alone after the range end", properties: []string{"CREATED:20240601T130000Z"}, start: "20240601T100000Z", end: "20240601T110000Z", want: false},

		// Row N|N|N|N|N: TRUE
		{name: "no scheduling property at all matches every range", properties: []string{"SUMMARY:someday"}, start: "20240601T100000Z", end: "20240601T110000Z", want: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			raw := wrapCalendar(componentLines("VTODO", append([]string{"UID:t"}, tt.properties...)...)...)
			if got := assertComponentInRange(t, raw, "VTODO", tt.start, tt.end, floatingZone{}); got != tt.want {
				t.Fatalf("VTODO in range = %v, want %v for %v", got, tt.want, tt.properties)
			}
		})
	}
}

// The three rows of the RFC 4791 §9.9 VJOURNAL table.
func TestTimeRangeVJournalTable(t *testing.T) {
	tests := []struct {
		name       string
		properties []string
		start      string
		end        string
		want       bool
	}{
		{name: "DATE-TIME DTSTART at range start", properties: []string{"DTSTART:20240601T100000Z"}, start: "20240601T100000Z", end: "20240601T110000Z", want: true},
		{name: "DATE-TIME DTSTART at range end", properties: []string{"DTSTART:20240601T110000Z"}, start: "20240601T100000Z", end: "20240601T110000Z", want: false},
		{name: "DATE DTSTART spans its whole day", properties: []string{"DTSTART;VALUE=DATE:20240601"}, start: "20240601T230000Z", end: "20240602T000000Z", want: true},
		{name: "DATE DTSTART does not reach the next day", properties: []string{"DTSTART;VALUE=DATE:20240601"}, start: "20240602T000000Z", end: "20240603T000000Z", want: false},
		{name: "no DTSTART never matches", properties: []string{"SUMMARY:undated"}, start: "20240601T100000Z", end: "20240601T110000Z", want: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			raw := wrapCalendar(componentLines("VJOURNAL", append([]string{"UID:j"}, tt.properties...)...)...)
			if got := assertComponentInRange(t, raw, "VJOURNAL", tt.start, tt.end, floatingZone{}); got != tt.want {
				t.Fatalf("VJOURNAL in range = %v, want %v for %v", got, tt.want, tt.properties)
			}
		})
	}
}

// The three rows of the RFC 4791 §9.9 VFREEBUSY table.
func TestTimeRangeVFreeBusyTable(t *testing.T) {
	tests := []struct {
		name       string
		properties []string
		start      string
		end        string
		want       bool
	}{
		// Row Y|*: (start <= DTEND) AND (end > DTSTART)
		{name: "DTSTART and DTEND overlapping", properties: []string{"DTSTART:20240601T100000Z", "DTEND:20240601T120000Z"}, start: "20240601T110000Z", end: "20240601T130000Z", want: true},
		{name: "range starting exactly on DTEND still matches", properties: []string{"DTSTART:20240601T100000Z", "DTEND:20240601T120000Z"}, start: "20240601T120000Z", end: "20240601T130000Z", want: true},
		{name: "range starting after DTEND", properties: []string{"DTSTART:20240601T100000Z", "DTEND:20240601T120000Z"}, start: "20240601T120001Z", end: "20240601T130000Z", want: false},

		// Row N|Y: each FREEBUSY period is compared instead
		{name: "FREEBUSY period overlapping", properties: []string{"FREEBUSY:20240601T100000Z/20240601T110000Z"}, start: "20240601T103000Z", end: "20240601T113000Z", want: true},
		{name: "FREEBUSY period outside the range", properties: []string{"FREEBUSY:20240601T100000Z/20240601T110000Z"}, start: "20240601T120000Z", end: "20240601T130000Z", want: false},
		{name: "FREEBUSY period in start/duration form", properties: []string{"FREEBUSY:20240601T100000Z/PT1H"}, start: "20240601T103000Z", end: "20240601T113000Z", want: true},
		{name: "second FREEBUSY period in one value", properties: []string{"FREEBUSY:20240601T100000Z/PT1H,20240601T150000Z/PT1H"}, start: "20240601T153000Z", end: "20240601T163000Z", want: true},

		// Row N|N: FALSE
		{name: "neither bounds nor periods", properties: []string{"ORGANIZER:mailto:a@example.com"}, start: "20240601T100000Z", end: "20240601T110000Z", want: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			raw := wrapCalendar(componentLines("VFREEBUSY", append([]string{"UID:f"}, tt.properties...)...)...)
			if got := assertComponentInRange(t, raw, "VFREEBUSY", tt.start, tt.end, floatingZone{}); got != tt.want {
				t.Fatalf("VFREEBUSY in range = %v, want %v for %v", got, tt.want, tt.properties)
			}
		})
	}
}

// A VALARM overlaps when (start <= trigger-time AND end > trigger-time), for
// the initial trigger and every REPEAT.
func TestTimeRangeVAlarmTriggers(t *testing.T) {
	tests := []struct {
		name   string
		alarm  []string
		start  string
		end    string
		want   bool
		parent []string
	}{
		{
			name:   "absolute trigger inside the range",
			alarm:  []string{"ACTION:DISPLAY", "DESCRIPTION:x", "TRIGGER;VALUE=DATE-TIME:20240601T103000Z"},
			parent: []string{"DTSTART:20240601T120000Z", "DTEND:20240601T130000Z"},
			start:  "20240601T100000Z", end: "20240601T110000Z", want: true,
		},
		{
			name:   "trigger relative to the enclosing DTSTART",
			alarm:  []string{"ACTION:DISPLAY", "DESCRIPTION:x", "TRIGGER:-PT30M"},
			parent: []string{"DTSTART:20240601T120000Z", "DTEND:20240601T130000Z"},
			start:  "20240601T113000Z", end: "20240601T114000Z", want: true,
		},
		{
			name:   "trigger relative to the enclosing DTEND",
			alarm:  []string{"ACTION:DISPLAY", "DESCRIPTION:x", "TRIGGER;RELATED=END:-PT30M"},
			parent: []string{"DTSTART:20240601T120000Z", "DTEND:20240601T130000Z"},
			start:  "20240601T123000Z", end: "20240601T124000Z", want: true,
		},
		{
			name:   "only a repeat reaches the range",
			alarm:  []string{"ACTION:DISPLAY", "DESCRIPTION:x", "TRIGGER;VALUE=DATE-TIME:20240601T080000Z", "REPEAT:3", "DURATION:PT1H"},
			parent: []string{"DTSTART:20240601T120000Z", "DTEND:20240601T130000Z"},
			start:  "20240601T110000Z", end: "20240601T113000Z", want: true,
		},
		{
			name:   "no trigger reaches the range",
			alarm:  []string{"ACTION:DISPLAY", "DESCRIPTION:x", "TRIGGER;VALUE=DATE-TIME:20240601T080000Z", "REPEAT:1", "DURATION:PT1H"},
			parent: []string{"DTSTART:20240601T120000Z", "DTEND:20240601T130000Z"},
			start:  "20240601T110000Z", end: "20240601T113000Z", want: false,
		},
		{
			// RFC 5545 §3.8.6.3: an absolute trigger fires once, at the instant
			// it names, however many times the enclosing component recurs. The
			// weekly series here ends in January, so no instance is generated
			// anywhere near the June range the trigger lands in.
			name:   "absolute trigger outside the recurrence span",
			alarm:  []string{"ACTION:DISPLAY", "DESCRIPTION:x", "TRIGGER;VALUE=DATE-TIME:20240610T100000Z"},
			parent: []string{"DTSTART:20240101T100000Z", "DTEND:20240101T110000Z", "RRULE:FREQ=WEEKLY;COUNT=4"},
			start:  "20240610T095000Z", end: "20240610T101000Z", want: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			event := []string{"BEGIN:VEVENT", "UID:e"}
			event = append(event, tt.parent...)
			event = append(event, componentLines("VALARM", tt.alarm...)...)
			event = append(event, "END:VEVENT")
			raw := wrapCalendar(event...)
			if got := assertComponentInRange(t, raw, "VALARM", tt.start, tt.end, floatingZone{}); got != tt.want {
				t.Fatalf("VALARM in range = %v, want %v", got, tt.want)
			}
		})
	}
}

// An alarm inside a recurring component fires once per instance, and §9.9
// requires every recurrence instance to be considered. Judging the alarm only
// against the master occurrence answers about one trigger out of however many
// the resource actually schedules.
func TestTimeRangeVAlarmFiresForEveryRecurrenceInstance(t *testing.T) {
	// Weekly from 2024-01-01T10:00Z; the 2024-06-10 instance is the 24th, and
	// its alarm fires fifteen minutes ahead of it.
	raw := wrapCalendar(append(
		[]string{"BEGIN:VEVENT", "UID:e", "DTSTART:20240101T100000Z", "DTEND:20240101T110000Z", "RRULE:FREQ=WEEKLY;COUNT=40"},
		append(componentLines("VALARM", "ACTION:DISPLAY", "DESCRIPTION:x", "TRIGGER:-PT15M"), "END:VEVENT")...,
	)...)

	if !assertComponentInRange(t, raw, "VALARM", "20240101T094000Z", "20240101T095000Z", floatingZone{}) {
		t.Fatal("the master occurrence's own alarm did not match")
	}
	if !assertComponentInRange(t, raw, "VALARM", "20240610T094000Z", "20240610T095000Z", floatingZone{}) {
		t.Fatal("the alarm of a later recurrence instance did not match")
	}
	// 2024-06-12 is a Wednesday; no instance of a Monday series falls there.
	if assertComponentInRange(t, raw, "VALARM", "20240612T094000Z", "20240612T095000Z", floatingZone{}) {
		t.Fatal("an alarm matched a day no recurrence instance reaches")
	}
}

// A relative TRIGGER fires outside the occurrence it belongs to, so the
// instance that answers a range can start well beyond that range's own bounds.
// The instance scan has to be widened by the alarm's lead time before the §9.9
// condition judges each candidate.
func TestTimeRangeVAlarmScanReachesInstancesOutsideTheRange(t *testing.T) {
	// Weekly from 2024-01-01T10:00Z with a week of lead time. The alarm that
	// fires on 2024-06-10 belongs to the instance starting 2024-06-17, which
	// sits a week past the end of the range being asked about.
	raw := wrapCalendar(append(
		[]string{"BEGIN:VEVENT", "UID:e", "DTSTART:20240101T100000Z", "DTEND:20240101T110000Z", "RRULE:FREQ=WEEKLY;COUNT=40"},
		append(componentLines("VALARM", "ACTION:DISPLAY", "DESCRIPTION:x", "TRIGGER:-P1W"), "END:VEVENT")...,
	)...)

	if !assertComponentInRange(t, raw, "VALARM", "20240610T095000Z", "20240610T101000Z", floatingZone{}) {
		t.Fatal("an alarm leading its instance by a week was not found")
	}

	// The same shape trailing the occurrence: RELATED=END plus a positive
	// trigger puts the alarm after an instance that starts before the range.
	trailing := wrapCalendar(append(
		[]string{"BEGIN:VEVENT", "UID:e", "DTSTART:20240101T100000Z", "DTEND:20240101T110000Z", "RRULE:FREQ=WEEKLY;COUNT=40"},
		append(componentLines("VALARM", "ACTION:DISPLAY", "DESCRIPTION:x", "TRIGGER;RELATED=END:P1W"), "END:VEVENT")...,
	)...)
	// The 2024-06-03 instance ends at 11:00Z and its alarm fires a week later.
	if !assertComponentInRange(t, trailing, "VALARM", "20240610T105000Z", "20240610T111000Z", floatingZone{}) {
		t.Fatal("an alarm trailing its instance by a week was not found")
	}

	// A REPEAT sequence long enough to overflow the lead arithmetic saturates
	// instead of wrapping, which would have turned the widening into a
	// narrowing and dropped the instance the range does reach.
	huge := wrapCalendar(append(
		[]string{"BEGIN:VEVENT", "UID:e", "DTSTART:20240101T100000Z", "DTEND:20240101T110000Z", "RRULE:FREQ=WEEKLY;COUNT=40"},
		append(componentLines("VALARM", "ACTION:DISPLAY", "DESCRIPTION:x", "TRIGGER:-PT15M", "REPEAT:1000", "DURATION:P9999W"), "END:VEVENT")...,
	)...)
	if !assertComponentInRange(t, huge, "VALARM", "20240610T094000Z", "20240610T095000Z", floatingZone{}) {
		t.Fatal("an overflowing REPEAT sequence narrowed the scan instead of widening it")
	}
}

// §9.9 requires the server to "infer an effective value for DTSTART, DTEND,
// DURATION, and DUE properties for an instance", so a prop-filter time-range on
// one of those three reads each instance's value rather than the master's. The
// other four properties §9.9 names are metadata of the component as a whole and
// carry the same value for every instance.
func TestTimeRangePropFilterConsidersRecurrenceInstances(t *testing.T) {
	raw := wrapCalendar(componentLines("VEVENT",
		"UID:e",
		"CREATED:20231201T080000Z",
		"DTSTAMP:20231201T080000Z",
		"DTSTART:20240101T100000Z",
		"DTEND:20240101T110000Z",
		"RRULE:FREQ=WEEKLY;COUNT=40",
	)...)
	event := store.Event{UID: "e", RawICAL: raw}

	propFilterQuery := func(property, start, end string) *calFilter {
		return &calFilter{CompFilter: compFilter{
			Name: "VCALENDAR",
			CompFilter: []compFilter{{
				Name: "VEVENT",
				PropFilter: []propFilter{{
					Name:      property,
					TimeRange: &timeRange{Start: start, End: end},
				}},
			}},
		}}
	}

	shifted := []struct {
		property string
		start    string
		end      string
	}{
		{property: "DTSTART", start: "20240610T095000Z", end: "20240610T101000Z"},
		{property: "DTEND", start: "20240610T105000Z", end: "20240610T111000Z"},
	}
	for _, tt := range shifted {
		if !eventMatchesFilter(event, propFilterQuery(tt.property, tt.start, tt.end), floatingZone{}) {
			t.Errorf("%s did not take its effective value for the 2024-06-10 instance", tt.property)
		}
		if eventMatchesFilter(event, propFilterQuery(tt.property, "20240612T000000Z", "20240613T000000Z"), floatingZone{}) {
			t.Errorf("%s matched a day no recurrence instance reaches", tt.property)
		}
	}

	// DUE is the VTODO spelling of the same rule.
	todo := store.Event{UID: "t", RawICAL: wrapCalendar(componentLines("VTODO",
		"UID:t", "DTSTART:20240101T100000Z", "DUE:20240101T110000Z", "RRULE:FREQ=WEEKLY;COUNT=40",
	)...)}
	todoQuery := propFilterQuery("DUE", "20240610T105000Z", "20240610T111000Z")
	todoQuery.CompFilter.CompFilter[0].Name = "VTODO"
	if !eventMatchesFilter(todo, todoQuery, floatingZone{}) {
		t.Error("DUE did not take its effective value for the 2024-06-10 instance")
	}

	// CREATED and DTSTAMP describe the component, not an occurrence of it, so
	// they stay where the resource wrote them.
	for _, property := range []string{"CREATED", "DTSTAMP"} {
		if !eventMatchesFilter(event, propFilterQuery(property, "20231201T075000Z", "20231201T081000Z"), floatingZone{}) {
			t.Errorf("%s did not match its own written value", property)
		}
		if eventMatchesFilter(event, propFilterQuery(property, "20240610T075000Z", "20240610T081000Z"), floatingZone{}) {
			t.Errorf("%s was shifted onto a recurrence instance", property)
		}
	}
}

// §9.9 closes with "The semantic of CALDAV:time-range is not defined for any
// other calendar components and properties", so a time-range scoped anywhere
// the tables do not reach matches nothing rather than falling back to whatever
// dates the resource carries.
func TestTimeRangeIsNotEvaluatedOutsideTheDefinedComponents(t *testing.T) {
	raw := wrapCalendar(append(
		componentLines("VTIMEZONE", "TZID:Etc/UTC", "BEGIN:STANDARD", "DTSTART:19700101T000000", "TZOFFSETFROM:+0000", "TZOFFSETTO:+0000", "END:STANDARD"),
		componentLines("VEVENT", "UID:e", "DTSTART:20240601T100000Z", "DTEND:20240601T110000Z")...,
	)...)

	for _, component := range []string{"VCALENDAR", "VTIMEZONE"} {
		t.Run(component, func(t *testing.T) {
			if assertComponentInRange(t, raw, component, "20240601T000000Z", "20240630T000000Z", floatingZone{}) {
				t.Fatalf("a time-range scoped to %s matched; §9.9 defines no semantic for it", component)
			}
		})
	}
}

func TestTimeRangePropertyOverlapIsScopedToTheSevenDefinedProperties(t *testing.T) {
	raw := wrapCalendar(componentLines("VEVENT",
		"UID:e",
		"DTSTART:20240601T100000Z",
		"DTEND:20240601T110000Z",
		"DTSTAMP:20240601T100000Z",
		"CREATED:20240601T100000Z",
		"LAST-MODIFIED:20240601T100000Z",
		"COMPLETED:20240601T100000Z",
		"DUE:20240601T100000Z",
		// A date-valued property §9.9 does not list. It parses fine, and it
		// still must not answer a time-range.
		"RECURRENCE-ID:20240601T100000Z",
	)...)
	root, err := parseICalendarObject(raw)
	if err != nil {
		t.Fatalf("parseICalendarObject() = %v", err)
	}
	event, _ := namedComponent(root, "VEVENT")
	matcher := newCalendarTimeRangeMatcher(raw, root, floatingZone{})
	start := time.Date(2024, 6, 1, 10, 0, 0, 0, time.UTC)
	end := time.Date(2024, 6, 1, 11, 0, 0, 0, time.UTC)

	defined := map[string]bool{
		"COMPLETED": true, "CREATED": true, "DTEND": true, "DTSTAMP": true,
		"DTSTART": true, "DUE": true, "LAST-MODIFIED": true,
	}
	for i := range event.properties {
		property := event.properties[i]
		if property.name == "UID" {
			continue
		}
		got := matcher.propertyInTimeRange(property, event, root, start, end)
		// DTEND is 11:00, which is the exclusive end of the range.
		want := defined[property.name] && property.name != "DTEND"
		if got != want {
			t.Fatalf("propertyInTimeRange(%s) = %v, want %v", property.name, got, want)
		}
	}
}

// §9.9 requires every recurrence instance to be considered, for every component
// type that carries a recurrence pattern -- not only VEVENT.
func TestTimeRangeConsidersRecurrenceInstancesForEveryComponentType(t *testing.T) {
	for _, component := range []string{"VEVENT", "VTODO", "VJOURNAL"} {
		t.Run(component, func(t *testing.T) {
			raw := wrapCalendar(componentLines(component,
				"UID:r",
				"DTSTART:20240601T100000Z",
				"RRULE:FREQ=WEEKLY;COUNT=5",
			)...)

			// The master is well before this range; only the third instance
			// falls inside it.
			if !assertComponentInRange(t, raw, component, "20240615T090000Z", "20240615T110000Z", floatingZone{}) {
				t.Fatalf("%s recurrence instance in range was not matched", component)
			}
			if assertComponentInRange(t, raw, component, "20240617T090000Z", "20240617T110000Z", floatingZone{}) {
				t.Fatalf("%s matched a range no instance reaches", component)
			}
		})
	}
}

// Each §9.9 row has to hold for a generated instance as well as for the master,
// including the rows that reach past DTSTART: a DURATION, the day a DATE-valued
// DTSTART implies, and the VTODO row that keys on DUE alone. None of the three
// is expressible in the denormalized columns, so each is exercised against an
// instance here.
func TestTimeRangeAppliesEveryTableRowToGeneratedInstances(t *testing.T) {
	tests := []struct {
		name       string
		component  string
		properties []string
		inside     [2]string
		outside    [2]string
	}{
		{
			// Row N|Y|Y|*: the range falls strictly inside DTSTART+DURATION of
			// the third instance, touching neither end of it.
			name:       "VEVENT DURATION row",
			component:  "VEVENT",
			properties: []string{"DTSTART:20240601T100000Z", "DURATION:PT2H", "RRULE:FREQ=WEEKLY;COUNT=5"},
			inside:     [2]string{"20240615T110000Z", "20240615T113000Z"},
			outside:    [2]string{"20240615T130000Z", "20240615T140000Z"},
		},
		{
			// Row N|N|N|N: the instance is a DATE, so it spans its whole day.
			name:       "VEVENT DATE row",
			component:  "VEVENT",
			properties: []string{"DTSTART;VALUE=DATE:20240601", "RRULE:FREQ=WEEKLY;COUNT=5"},
			inside:     [2]string{"20240615T230000Z", "20240616T000000Z"},
			outside:    [2]string{"20240616T000000Z", "20240616T010000Z"},
		},
		{
			// Row N|N|Y: (start < DUE) AND (end >= DUE), with DUE shifted onto
			// the instance rather than read off the master.
			name:       "VTODO DUE row",
			component:  "VTODO",
			properties: []string{"DUE:20240601T110000Z", "DTSTART:20240601T100000Z", "RRULE:FREQ=WEEKLY;COUNT=5"},
			inside:     [2]string{"20240615T103000Z", "20240615T110000Z"},
			outside:    [2]string{"20240615T113000Z", "20240615T120000Z"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			raw := wrapCalendar(componentLines(tt.component, append([]string{"UID:r"}, tt.properties...)...)...)
			if !assertComponentInRange(t, raw, tt.component, tt.inside[0], tt.inside[1], floatingZone{}) {
				t.Errorf("the 2024-06-15 instance did not match %v", tt.inside)
			}
			if assertComponentInRange(t, raw, tt.component, tt.outside[0], tt.outside[1], floatingZone{}) {
				t.Errorf("a range no instance reaches matched %v", tt.outside)
			}
		})
	}
}

// RFC 4791 §7.3 resolves a floating value against the request's CALDAV:timezone
// and then the collection's CALDAV:calendar-timezone. The same wall-clock
// reading therefore lands on a different instant depending on the zone in
// force, which is what decides whether it is in range.
func TestTimeRangeResolvesFloatingValuesThroughTheSelectedZone(t *testing.T) {
	if _, err := time.LoadLocation("America/Chicago"); err != nil {
		t.Skipf("tzdata unavailable: %v", err)
	}
	raw := wrapCalendar(componentLines("VEVENT",
		"UID:floating",
		"DTSTART:20240601T100000",
		"DTEND:20240601T110000",
	)...)
	zoneDefinition := wrapCalendar(componentLines("VTIMEZONE",
		"TZID:America/Chicago",
		"BEGIN:STANDARD",
		"DTSTART:19701101T020000",
		"TZOFFSETFROM:-0500",
		"TZOFFSETTO:-0600",
		"END:STANDARD",
	)...)

	// Read as UTC the event is 10:00-11:00Z; in America/Chicago (CDT, -0500 in
	// June) the same wall clock is 15:00-16:00Z.
	utcRange := []string{"20240601T100000Z", "20240601T110000Z"}
	chicagoRange := []string{"20240601T150000Z", "20240601T160000Z"}

	if !assertComponentInRange(t, raw, "VEVENT", utcRange[0], utcRange[1], floatingZone{}) {
		t.Fatal("floating value did not resolve as UTC without a zone")
	}

	zone := newFloatingZone(zoneDefinition)
	if zone.loc == nil {
		t.Fatal("newFloatingZone() did not resolve America/Chicago")
	}
	if assertComponentInRange(t, raw, "VEVENT", utcRange[0], utcRange[1], zone) {
		t.Fatal("floating value still resolved as UTC despite a request timezone")
	}
	if !assertComponentInRange(t, raw, "VEVENT", chicagoRange[0], chicagoRange[1], zone) {
		t.Fatal("floating value did not resolve through the request timezone")
	}
}

// The SQL time-range predicates narrow the candidate rows a report evaluates,
// so they have to be a superset of what the RFC 4791 §9.9 test would keep. A
// row they drop is a false negative nothing downstream can recover from, since
// the in-memory pass never sees it.
//
// The corpus is deliberately weighted toward resources whose derived columns
// are absent or disagree with the extent the component actually occupies: only
// a VEVENT populates dtstart/dtend at all, and dtend only from a literal DTEND,
// so a DURATION, an implied all-day P1D and every VALARM trigger are invisible
// to a predicate reading those two columns.
func TestCalendarQueryPushdownAgreesWithInMemoryFiltering(t *testing.T) {
	corpus := map[string]string{
		"event-in-range":     wrapCalendar(componentLines("VEVENT", "UID:event-in-range", "DTSTART:20240610T100000Z", "DTEND:20240610T110000Z")...),
		"event-out-of-range": wrapCalendar(componentLines("VEVENT", "UID:event-out-of-range", "DTSTART:20240801T100000Z", "DTEND:20240801T110000Z")...),
		"event-duration":     wrapCalendar(componentLines("VEVENT", "UID:event-duration", "DTSTART:20240610T100000Z", "DURATION:PT2H")...),
		"event-all-day":      wrapCalendar(componentLines("VEVENT", "UID:event-all-day", "DTSTART;VALUE=DATE:20240610")...),
		"event-recurring":    wrapCalendar(componentLines("VEVENT", "UID:event-recurring", "DTSTART:20240101T100000Z", "DTEND:20240101T110000Z", "RRULE:FREQ=WEEKLY;COUNT=40")...),
		"event-no-dtstart":   wrapCalendar(componentLines("VEVENT", "UID:event-no-dtstart", "SUMMARY:undated")...),
		"event-early-alarm": wrapCalendar(append(
			[]string{"BEGIN:VEVENT", "UID:event-early-alarm", "DTSTART:20240801T100000Z", "DTEND:20240801T110000Z"},
			append(componentLines("VALARM", "ACTION:DISPLAY", "DESCRIPTION:heads up", "TRIGGER:-P1W"), "END:VEVENT")...,
		)...),
		// A recurring event whose alarm leads each instance by a week: the
		// instance answering an alarm range starts outside that range, and the
		// columns record neither the alarm nor the lead.
		"event-recurring-alarm": wrapCalendar(append(
			[]string{"BEGIN:VEVENT", "UID:event-recurring-alarm", "DTSTART:20240101T100000Z", "DTEND:20240101T110000Z", "RRULE:FREQ=WEEKLY;COUNT=40"},
			append(componentLines("VALARM", "ACTION:DISPLAY", "DESCRIPTION:heads up", "TRIGGER:-P1W"), "END:VEVENT")...,
		)...),
		"todo-in-range":     wrapCalendar(componentLines("VTODO", "UID:todo-in-range", "DTSTART:20240610T100000Z", "DUE:20240610T120000Z")...),
		"todo-out-of-range": wrapCalendar(componentLines("VTODO", "UID:todo-out-of-range", "DTSTART:20240801T100000Z", "DUE:20240801T120000Z")...),
		"todo-bare":         wrapCalendar(componentLines("VTODO", "UID:todo-bare", "SUMMARY:someday")...),
		"todo-recurring":    wrapCalendar(componentLines("VTODO", "UID:todo-recurring", "DTSTART:20240101T100000Z", "DUE:20240101T110000Z", "RRULE:FREQ=WEEKLY;COUNT=40")...),
		"journal-in-range":  wrapCalendar(componentLines("VJOURNAL", "UID:journal-in-range", "DTSTART:20240610T100000Z")...),
		"journal-date":      wrapCalendar(componentLines("VJOURNAL", "UID:journal-date", "DTSTART;VALUE=DATE:20240610")...),
		"freebusy-periods":  wrapCalendar(componentLines("VFREEBUSY", "UID:freebusy-periods", "FREEBUSY:20240610T100000Z/PT1H")...),
		// Floating values sit at the columns' UTC reading but at the zone's
		// instant once §7.3 resolves them, and the EXDATE has to move with them.
		"event-floating-exdate": wrapCalendar(componentLines("VEVENT",
			"UID:event-floating-exdate", "DTSTART:20240610T100000", "DTEND:20240610T110000",
			"RRULE:FREQ=WEEKLY;COUNT=3", "EXDATE:20240610T100000",
		)...),
		// A TZID resolved through the VTIMEZONE the resource ships sits a whole
		// zone offset from the column, which reads the same value as UTC. The
		// wall clock is outside the range on both filters that bound the end;
		// the instant it names is inside.
		"event-tzid-shifted": tzidEventCorpusFixture("event-tzid-shifted",
			"Custom/Plus14", "+1400", "20240701T100000", "20240701T110000"),
	}

	filters := map[string]string{
		"VEVENT":   `<C:comp-filter name="VEVENT"><C:time-range start="20240601T000000Z" end="20240701T000000Z"/></C:comp-filter>`,
		"VTODO":    `<C:comp-filter name="VTODO"><C:time-range start="20240601T000000Z" end="20240701T000000Z"/></C:comp-filter>`,
		"VJOURNAL": `<C:comp-filter name="VJOURNAL"><C:time-range start="20240601T000000Z" end="20240701T000000Z"/></C:comp-filter>`,
		"VFREEBUSY": `<C:comp-filter name="VFREEBUSY">` +
			`<C:time-range start="20240601T000000Z" end="20240701T000000Z"/></C:comp-filter>`,
		"open-ended": `<C:comp-filter name="VEVENT"><C:time-range start="20240601T000000Z"/></C:comp-filter>`,
		"open-start": `<C:comp-filter name="VEVENT"><C:time-range end="20240701T000000Z"/></C:comp-filter>`,

		// A range starting after DTSTART but inside DTSTART+DURATION. dtend is
		// NULL for this row, so a predicate falling back to dtstart measures
		// the event's start against the range start and drops it.
		"inside a DURATION": `<C:comp-filter name="VEVENT"><C:time-range start="20240610T110000Z" end="20240610T120000Z"/></C:comp-filter>`,

		// The same shape for the +P1D a DATE-valued DTSTART implies.
		"late in an all-day": `<C:comp-filter name="VEVENT"><C:time-range start="20240610T230000Z" end="20240611T000000Z"/></C:comp-filter>`,

		// A VALARM range bounds the alarm, not the event that encloses it, so it
		// cannot narrow rows on the enclosing event's own bounds.
		"nested VALARM": `<C:comp-filter name="VEVENT"><C:comp-filter name="VALARM">` +
			`<C:time-range start="20240720T000000Z" end="20240730T000000Z"/></C:comp-filter></C:comp-filter>`,

		// The same, answered by a recurrence instance rather than a master.
		"nested VALARM on a recurring event": `<C:comp-filter name="VEVENT"><C:comp-filter name="VALARM">` +
			`<C:time-range start="20240603T090000Z" end="20240603T110000Z"/></C:comp-filter></C:comp-filter>`,

		// A prop-filter range reads each instance's effective DTSTART, and is
		// never pushed down, so the candidate set has to reach it unnarrowed.
		"DTSTART prop-filter": `<C:comp-filter name="VEVENT">` +
			`<C:prop-filter name="DTSTART"><C:time-range start="20240610T090000Z" end="20240610T110000Z"/>` +
			`</C:prop-filter></C:comp-filter>`,
	}

	for name, componentFilter := range filters {
		t.Run(name, func(t *testing.T) {
			body := `<C:calendar-query xmlns:C="urn:ietf:params:xml:ns:caldav" xmlns:D="DAV:">` +
				`<D:prop><D:getetag/></D:prop><C:filter><C:comp-filter name="VCALENDAR">` +
				componentFilter + `</C:comp-filter></C:filter></C:calendar-query>`

			narrowed := runPushdownCorpusQuery(t, corpus, body, false)
			scanned := runPushdownCorpusQuery(t, corpus, body, true)

			// Agreement on an empty set proves nothing, so a filter matching no
			// corpus member at all is a drifted fixture rather than a pass.
			if len(scanned) == 0 {
				t.Fatalf("filter matched nothing on a full scan; the corpus no longer exercises it")
			}
			if len(narrowed) != len(scanned) {
				t.Fatalf("pushdown returned %v, full scan returned %v", narrowed, scanned)
			}
			for i := range scanned {
				if narrowed[i] != scanned[i] {
					t.Fatalf("pushdown returned %v, full scan returned %v", narrowed, scanned)
				}
			}
		})
	}
}

// The narrowing predicates compare against columns holding a floating value's
// UTC reading, while §7.3 resolves the same value against the request's
// timezone. The row's true instant can therefore sit most of a day from what the
// column records, and a predicate narrowed to the range as spelled would drop it
// before the §9.9 test ever saw it.
func TestCalendarQueryPushdownKeepsFloatingRowsUnderARequestTimezone(t *testing.T) {
	if _, err := time.LoadLocation("Pacific/Kiritimati"); err != nil {
		t.Skipf("tzdata unavailable: %v", err)
	}
	corpus := map[string]string{
		// +14:00, the widest offset any current IANA zone uses: read as UTC this
		// starts 2024-06-10T10:00Z, but the zone puts it at 2024-06-09T20:00Z.
		"floating-far-east": wrapCalendar(componentLines("VEVENT",
			"UID:floating-far-east", "DTSTART:20240610T100000", "DTEND:20240610T110000",
		)...),
		"floating-recurring": wrapCalendar(componentLines("VEVENT",
			"UID:floating-recurring", "DTSTART:20240610T100000", "DTEND:20240610T110000",
			"RRULE:FREQ=WEEKLY;COUNT=3", "EXDATE:20240617T100000",
		)...),
		"utc-anchored": wrapCalendar(componentLines("VEVENT",
			"UID:utc-anchored", "DTSTART:20240610T100000Z", "DTEND:20240610T110000Z",
		)...),
	}
	timezone := "<C:timezone>BEGIN:VCALENDAR&#13;\nVERSION:2.0&#13;\nPRODID:-//test//EN&#13;\n" +
		"BEGIN:VTIMEZONE&#13;\nTZID:Pacific/Kiritimati&#13;\nBEGIN:STANDARD&#13;\n" +
		"DTSTART:19700101T000000&#13;\nTZOFFSETFROM:+1400&#13;\nTZOFFSETTO:+1400&#13;\n" +
		"END:STANDARD&#13;\nEND:VTIMEZONE&#13;\nEND:VCALENDAR&#13;\n</C:timezone>"

	body := `<C:calendar-query xmlns:C="urn:ietf:params:xml:ns:caldav" xmlns:D="DAV:">` +
		`<D:prop><D:getetag/></D:prop><C:filter><C:comp-filter name="VCALENDAR">` +
		`<C:comp-filter name="VEVENT">` +
		`<C:time-range start="20240609T190000Z" end="20240609T210000Z"/>` +
		`</C:comp-filter></C:comp-filter></C:filter>` + timezone + `</C:calendar-query>`

	narrowed := runPushdownCorpusQuery(t, corpus, body, false)
	scanned := runPushdownCorpusQuery(t, corpus, body, true)
	if len(scanned) == 0 {
		t.Fatal("filter matched nothing on a full scan; the corpus no longer exercises it")
	}
	if len(narrowed) != len(scanned) {
		t.Fatalf("pushdown returned %v, full scan returned %v", narrowed, scanned)
	}
	for i := range scanned {
		if narrowed[i] != scanned[i] {
			t.Fatalf("pushdown returned %v, full scan returned %v", narrowed, scanned)
		}
	}
}

// tzidEventCorpusFixture builds a calendar object whose DTSTART and DTEND name
// a TZID the object itself defines, at a fixed offset from UTC.
func tzidEventCorpusFixture(uid, tzid, offset, start, end string) string {
	timezone := componentLines("VTIMEZONE", "TZID:"+tzid,
		"BEGIN:STANDARD", "DTSTART:19700101T000000",
		"TZOFFSETFROM:"+offset, "TZOFFSETTO:"+offset, "END:STANDARD")
	event := componentLines("VEVENT", "UID:"+uid,
		"DTSTART;TZID="+tzid+":"+start, "DTEND;TZID="+tzid+":"+end)
	return wrapCalendar(append(timezone, event...)...)
}

// RFC 4791 §4.1 makes the VTIMEZONE a resource ships authoritative for the TZIDs
// it uses, so that definition is what places the value on the timeline. The
// derived columns the narrowing predicates read resolve a TZID against the
// host's zone database instead, and read one the host does not know as UTC, so
// the two sit a full zone offset apart. The predicates have to keep the row
// anyway: the §9.9 test runs in memory afterwards and can never see a row the
// query did not return.
func TestCalendarQueryPushdownKeepsRowsResolvedThroughASubmittedVTimezone(t *testing.T) {
	corpus := map[string]string{
		// The wall clock reads 2024-06-10T10:00, the instant is 2024-06-09T20:00Z.
		"tzid-unknown-zone": tzidEventCorpusFixture("tzid-unknown-zone",
			"Custom/Plus14", "+1400", "20240610T100000", "20240610T110000"),
		// The shape Outlook and Exchange send: a zone name that is no IANA
		// identifier, so the column reads the value as UTC while the shipped
		// definition puts it an hour earlier.
		"tzid-windows-name": tzidEventCorpusFixture("tzid-windows-name",
			"W. Europe Standard Time", "+0100", "20240610T003000", "20240610T013000"),
		"utc-anchored": wrapCalendar(componentLines("VEVENT",
			"UID:utc-anchored", "DTSTART:20240610T100000Z", "DTEND:20240610T110000Z",
		)...),
	}

	ranges := map[string][2]string{
		"a TZID the host cannot resolve": {"20240609T195000Z", "20240609T205000Z"},
		"a Windows zone name":            {"20240609T232000Z", "20240609T234000Z"},
	}

	for name, bounds := range ranges {
		t.Run(name, func(t *testing.T) {
			body := `<C:calendar-query xmlns:C="urn:ietf:params:xml:ns:caldav" xmlns:D="DAV:">` +
				`<D:prop><D:getetag/></D:prop><C:filter><C:comp-filter name="VCALENDAR">` +
				`<C:comp-filter name="VEVENT">` +
				`<C:time-range start="` + bounds[0] + `" end="` + bounds[1] + `"/>` +
				`</C:comp-filter></C:comp-filter></C:filter></C:calendar-query>`

			narrowed := runPushdownCorpusQuery(t, corpus, body, false)
			scanned := runPushdownCorpusQuery(t, corpus, body, true)
			if len(scanned) == 0 {
				t.Fatal("filter matched nothing on a full scan; the corpus no longer exercises it")
			}
			if len(narrowed) != len(scanned) {
				t.Fatalf("pushdown returned %v, full scan returned %v", narrowed, scanned)
			}
			for i := range scanned {
				if narrowed[i] != scanned[i] {
					t.Fatalf("pushdown returned %v, full scan returned %v", narrowed, scanned)
				}
			}
		})
	}
}

func runPushdownCorpusQuery(t *testing.T, corpus map[string]string, body string, ignoreFilter bool) []string {
	t.Helper()
	events := make(map[string]*store.Event, len(corpus))
	id := int64(0)
	names := make([]string, 0, len(corpus))
	for name := range corpus {
		names = append(names, name)
	}
	sort.Strings(names)
	for _, name := range names {
		id++
		events["1:"+name] = &store.Event{
			ID: id, CalendarID: 1, UID: name, ResourceName: name,
			RawICAL: corpus[name], ETag: "e" + name,
		}
	}

	h := &DavServer{store: &store.Store{
		Calendars: &fakeCalendarRepo{accessible: []store.CalendarAccess{
			{Calendar: store.Calendar{ID: 1, UserID: 1, Name: "Test"}, Editor: true},
		}},
		Events: &fakeEventRepo{events: events, ignoreFilter: ignoreFilter},
	}}

	req := httptest.NewRequest("REPORT", "/dav/calendars/1/", strings.NewReader(body))
	req.Header.Set("Depth", "1")
	req = req.WithContext(auth.WithUser(req.Context(), &store.User{ID: 1}))
	rr := httptest.NewRecorder()
	h.Report(rr, req)

	if rr.Code != http.StatusMultiStatus {
		t.Fatalf("status = %d, want 207; body: %s", rr.Code, rr.Body.String())
	}
	hrefs := decodeMultistatus(t, rr).hrefs()
	sort.Strings(hrefs)
	return hrefs
}

// A recurring component's own DTSTART is its first instance, so an EXDATE
// naming that date removes it. The component must be judged through its
// expanded set alone, or it would match a date the recurrence set no longer
// contains.
func TestTimeRangeHonoursExDateOnTheFirstInstance(t *testing.T) {
	raw := wrapCalendar(componentLines("VEVENT",
		"UID:excluded-first",
		"DTSTART:20240603T100000Z",
		"DTEND:20240603T110000Z",
		"RRULE:FREQ=WEEKLY;COUNT=3",
		"EXDATE:20240603T100000Z",
	)...)

	if assertComponentInRange(t, raw, "VEVENT", "20240603T000000Z", "20240604T000000Z", floatingZone{}) {
		t.Fatal("the EXDATE'd first instance still matched its own day")
	}
	// The instances EXDATE did not remove are still found.
	if !assertComponentInRange(t, raw, "VEVENT", "20240610T000000Z", "20240611T000000Z", floatingZone{}) {
		t.Fatal("a surviving instance was not matched")
	}
}

// One zone answers for the whole recurrence set. A component's DTSTART resolves
// through the §7.3 zone, so the EXDATE that removes an instance and the RDATE
// that adds one have to resolve through it too: read as UTC instead, they name
// instants the generated set never contains, and the exclusion silently stops
// excluding while the addition lands hours from where the client asked.
func TestRecurrenceDatesResolveThroughTheSelectedZone(t *testing.T) {
	if _, err := time.LoadLocation("America/Chicago"); err != nil {
		t.Skipf("tzdata unavailable: %v", err)
	}
	// CDT in June, so a 10:00 wall clock is 15:00Z and a date starts at 05:00Z.
	chicago := newFloatingZone(wrapCalendar(componentLines("VTIMEZONE",
		"TZID:America/Chicago",
		"BEGIN:STANDARD",
		"DTSTART:19701101T020000",
		"TZOFFSETFROM:-0500",
		"TZOFFSETTO:-0600",
		"END:STANDARD",
	)...))
	if chicago.loc == nil {
		t.Fatal("newFloatingZone() did not resolve America/Chicago")
	}
	// A TZID no host knows, resolved by walking the observances the definition
	// ships. That path yields no *time.Location, so the offset has to reach the
	// recurrence dates some other way than through the instant's zone.
	shipped := newFloatingZone(wrapCalendar(componentLines("VTIMEZONE",
		"TZID:Custom/Offset",
		"BEGIN:STANDARD",
		"DTSTART:19700101T000000",
		"TZOFFSETFROM:+0300",
		"TZOFFSETTO:+0300",
		"END:STANDARD",
	)...))
	if shipped.loc != nil || shipped.root == nil {
		t.Fatal("Custom/Offset did not fall back to its shipped observances")
	}

	tests := []struct {
		name       string
		zone       floatingZone
		properties []string
		start      string
		end        string
		want       bool
	}{
		{
			name: "an EXDATE removes the instance it names",
			zone: chicago,
			properties: []string{
				"DTSTART:20240603T100000", "DTEND:20240603T110000",
				"RRULE:FREQ=WEEKLY;COUNT=3", "EXDATE:20240603T100000",
			},
			start: "20240603T150000Z", end: "20240603T160000Z", want: false,
		},
		{
			name: "the instances it does not name survive",
			zone: chicago,
			properties: []string{
				"DTSTART:20240603T100000", "DTEND:20240603T110000",
				"RRULE:FREQ=WEEKLY;COUNT=3", "EXDATE:20240603T100000",
			},
			start: "20240610T150000Z", end: "20240610T160000Z", want: true,
		},
		{
			name: "an RDATE lands on the instant the zone gives it",
			zone: chicago,
			properties: []string{
				"DTSTART:20240603T100000", "DTEND:20240603T110000",
				"RDATE:20240605T100000",
			},
			start: "20240605T150000Z", end: "20240605T160000Z", want: true,
		},
		{
			name: "an RDATE is not left at its UTC reading",
			zone: chicago,
			properties: []string{
				"DTSTART:20240603T100000", "DTEND:20240603T110000",
				"RDATE:20240605T100000",
			},
			start: "20240605T100000Z", end: "20240605T110000Z", want: false,
		},
		{
			// A DATE value is floating by definition, so the same divergence
			// reaches every all-day recurrence carrying an exception.
			name: "a DATE-valued EXDATE removes its whole day",
			zone: chicago,
			properties: []string{
				"DTSTART;VALUE=DATE:20240603",
				"RRULE:FREQ=WEEKLY;COUNT=3", "EXDATE;VALUE=DATE:20240603",
			},
			start: "20240603T050000Z", end: "20240604T050000Z", want: false,
		},
		{
			name: "a DATE-valued EXDATE leaves the following week alone",
			zone: chicago,
			properties: []string{
				"DTSTART;VALUE=DATE:20240603",
				"RRULE:FREQ=WEEKLY;COUNT=3", "EXDATE;VALUE=DATE:20240603",
			},
			start: "20240610T050000Z", end: "20240611T050000Z", want: true,
		},
		{
			name: "a zone read from shipped observances excludes alike",
			zone: shipped,
			properties: []string{
				"DTSTART:20240603T100000", "DTEND:20240603T110000",
				"RRULE:FREQ=WEEKLY;COUNT=3", "EXDATE:20240603T100000",
			},
			start: "20240603T070000Z", end: "20240603T080000Z", want: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			raw := wrapCalendar(componentLines("VEVENT", append([]string{"UID:zoned"}, tt.properties...)...)...)
			if got := assertComponentInRange(t, raw, "VEVENT", tt.start, tt.end, tt.zone); got != tt.want {
				t.Fatalf("VEVENT in range = %v, want %v for %v", got, tt.want, tt.properties)
			}
		})
	}
}

// RFC 4791 §9.9 closes by directing a property-level test at the effective DTEND
// of a VEVENT carrying DURATION rather than one, and at the effective DUE of a
// VTODO in the same shape. The property the filter names is absent from the
// component either way, so the value has to be inferred to answer at all.
func TestTimeRangePropFilterUsesEffectiveDTEndAndDue(t *testing.T) {
	propFilterQuery := func(component, property string, children ...propFilter) *calFilter {
		filter := propFilter{Name: property}
		if len(children) > 0 {
			filter = children[0]
		}
		return &calFilter{CompFilter: compFilter{
			Name:       "VCALENDAR",
			CompFilter: []compFilter{{Name: component, PropFilter: []propFilter{filter}}},
		}}
	}
	rangeOn := func(start, end string) *timeRange {
		return &timeRange{Start: start, End: end}
	}

	event := store.Event{UID: "e", RawICAL: wrapCalendar(componentLines("VEVENT",
		"UID:e", "DTSTART:20240601T100000Z", "DURATION:PT2H",
	)...)}
	todo := store.Event{UID: "t", RawICAL: wrapCalendar(componentLines("VTODO",
		"UID:t", "DTSTART:20240601T100000Z", "DURATION:PT2H",
	)...)}
	recurring := store.Event{UID: "r", RawICAL: wrapCalendar(componentLines("VEVENT",
		"UID:r", "DTSTART:20240101T100000Z", "DURATION:PT2H", "RRULE:FREQ=WEEKLY;COUNT=40",
	)...)}

	cases := []struct {
		name      string
		event     store.Event
		component string
		property  string
		filter    propFilter
		want      bool
	}{
		{
			name: "an effective DTEND answers its own instant", event: event,
			component: "VEVENT", property: "DTEND",
			filter: propFilter{Name: "DTEND", TimeRange: rangeOn("20240601T115000Z", "20240601T121000Z")},
			want:   true,
		},
		{
			name: "an effective DTEND does not answer the DTSTART", event: event,
			component: "VEVENT", property: "DTEND",
			filter: propFilter{Name: "DTEND", TimeRange: rangeOn("20240601T095000Z", "20240601T101000Z")},
			want:   false,
		},
		{
			name: "an effective DUE answers its own instant", event: todo,
			component: "VTODO", property: "DUE",
			filter: propFilter{Name: "DUE", TimeRange: rangeOn("20240601T115000Z", "20240601T121000Z")},
			want:   true,
		},
		{
			// §9.9 requires every recurrence instance to be considered, and the
			// inferred value moves with the instance exactly as a written one does.
			name: "an effective DTEND moves with each recurrence instance", event: recurring,
			component: "VEVENT", property: "DTEND",
			filter: propFilter{Name: "DTEND", TimeRange: rangeOn("20240610T115000Z", "20240610T121000Z")},
			want:   true,
		},
		{
			name: "an effective DTEND reaches no day an instance misses", event: recurring,
			component: "VEVENT", property: "DTEND",
			filter: propFilter{Name: "DTEND", TimeRange: rangeOn("20240612T115000Z", "20240612T121000Z")},
			want:   false,
		},
		{
			// An inferred value carries no parameters, so a param-filter
			// conjoined with the time-range can never be satisfied.
			name: "a param-filter alongside is not satisfied", event: event,
			component: "VEVENT", property: "DTEND",
			filter: propFilter{
				Name:        "DTEND",
				TimeRange:   rangeOn("20240601T115000Z", "20240601T121000Z"),
				ParamFilter: []paramFilter{{Name: "TZID"}},
			},
			want: false,
		},
		{
			// RFC 4791 §9.7.4 asks whether the component defines the property.
			// An inferred value is not one, so the answer stays yes.
			name: "is-not-defined still reports the property absent", event: event,
			component: "VEVENT", property: "DTEND",
			filter: propFilter{Name: "DTEND", IsNotDefined: &struct{}{}},
			want:   true,
		},
	}

	for _, tt := range cases {
		t.Run(tt.name, func(t *testing.T) {
			filter := propFilterQuery(tt.component, tt.property, tt.filter)
			if got := eventMatchesFilter(tt.event, filter, floatingZone{}); got != tt.want {
				t.Fatalf("eventMatchesFilter() = %v, want %v", got, tt.want)
			}
		})
	}
}

// RFC 4791 §7.3 orders the sources a floating value is resolved against: the
// CALDAV:timezone the request carries, then the CALDAV:calendar-timezone the
// targeted collection defines, then UTC. §7.4 gives free-busy only the second,
// since that report has no timezone element of its own.
func TestReportFloatingZoneFollowsTheSectionSevenThreeOrder(t *testing.T) {
	for _, tzid := range []string{"America/Chicago", "Europe/Berlin"} {
		if _, err := time.LoadLocation(tzid); err != nil {
			t.Skipf("tzdata unavailable: %v", err)
		}
	}
	definition := func(tzid string) string {
		return wrapCalendar(componentLines("VTIMEZONE",
			"TZID:"+tzid,
			"BEGIN:STANDARD",
			"DTSTART:19701101T020000",
			"TZOFFSETFROM:+0000",
			"TZOFFSETTO:+0000",
			"END:STANDARD",
		)...)
	}
	requestTimezone := definition("America/Chicago")
	collectionTimezone := definition("Europe/Berlin")

	tests := []struct {
		name       string
		request    string
		collection *string
		want       string
	}{
		{name: "the request timezone wins", request: requestTimezone, collection: &collectionTimezone, want: "America/Chicago"},
		{name: "the collection timezone is next", collection: &collectionTimezone, want: "Europe/Berlin"},
		{name: "UTC is the fallback", want: ""},
		{name: "an undefined collection timezone falls back to UTC", want: ""},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			zone := reportFloatingZone(tt.request, tt.collection)
			got := ""
			if zone.loc != nil {
				got = zone.loc.String()
			}
			if got != tt.want {
				t.Fatalf("reportFloatingZone() resolved %q, want %q", got, tt.want)
			}
		})
	}

	// A collection may hold its CALDAV:calendar-timezone as a bare VTIMEZONE
	// component, so the value is enveloped on the way in exactly as
	// calendarTimezoneValue envelopes it on the way out.
	bare := "BEGIN:VTIMEZONE\r\nTZID:Europe/Berlin\r\nBEGIN:STANDARD\r\nDTSTART:19701101T020000\r\n" +
		"TZOFFSETFROM:+0000\r\nTZOFFSETTO:+0000\r\nEND:STANDARD\r\nEND:VTIMEZONE\r\n"
	if zone := reportFloatingZone("", &bare); zone.loc == nil || zone.loc.String() != "Europe/Berlin" {
		t.Fatalf("a bare stored VTIMEZONE did not resolve: %#v", zone)
	}
}

// The +P1D the §9.9 tables imply for a DATE value is a nominal day, not a fixed
// 24 hours: on the days a zone changes offset the two differ, and adding the
// wrong one puts the end of an all-day event an hour off. America/Chicago
// springs forward on 2024-03-10 (a 23-hour day, ending 05:00Z once the offset
// is -0500) and falls back on 2024-11-03 (a 25-hour one, ending 06:00Z once it
// is -0600). A fixed 24 hours would land an hour out in each direction.
func TestTimeRangeAllDayValueSpansItsNominalDayAcrossAZoneTransition(t *testing.T) {
	loc, err := time.LoadLocation("America/Chicago")
	if err != nil {
		t.Skipf("tzdata unavailable: %v", err)
	}
	zone := newFloatingZone(wrapCalendar(componentLines("VTIMEZONE",
		"TZID:America/Chicago",
		"BEGIN:STANDARD",
		"DTSTART:19701101T020000",
		"TZOFFSETFROM:-0500",
		"TZOFFSETTO:-0600",
		"END:STANDARD",
	)...))
	if zone.loc == nil {
		t.Fatal("newFloatingZone() did not resolve America/Chicago")
	}

	tests := []struct {
		name         string
		date         string
		nominalDay   time.Duration
		lastInDay    string
		firstOutside string
	}{
		{
			name: "spring forward", date: "20240310", nominalDay: 23 * time.Hour,
			lastInDay: "20240311T043000Z", firstOutside: "20240311T050000Z",
		},
		{
			name: "fall back", date: "20241103", nominalDay: 25 * time.Hour,
			lastInDay: "20241104T053000Z", firstOutside: "20241104T060000Z",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			day, err := time.ParseInLocation("20060102", tt.date, loc)
			if err != nil {
				t.Fatalf("ParseInLocation() = %v", err)
			}
			if got := zone.addDays(day.UTC(), 1).Sub(day.UTC()); got != tt.nominalDay {
				t.Fatalf("nominal day = %v, want %v", got, tt.nominalDay)
			}

			raw := wrapCalendar(componentLines("VEVENT", "UID:allday", "DTSTART;VALUE=DATE:"+tt.date)...)
			if !assertComponentInRange(t, raw, "VEVENT", tt.lastInDay, tt.firstOutside, zone) {
				t.Fatalf("the all-day value did not cover the last half hour of its %v day", tt.nominalDay)
			}
			outsideEnd, err := time.Parse("20060102T150405Z", tt.firstOutside)
			if err != nil {
				t.Fatalf("Parse() = %v", err)
			}
			if assertComponentInRange(t, raw, "VEVENT", tt.firstOutside, outsideEnd.Add(30*time.Minute).Format("20060102T150405Z"), zone) {
				t.Fatal("the all-day value spilled past the end of its day")
			}
		})
	}
}

// A TZID naming a zone the host does not know still resolves, through the
// VTIMEZONE the definition ships.
func TestFloatingZoneFallsBackToShippedObservances(t *testing.T) {
	zoneDefinition := wrapCalendar(componentLines("VTIMEZONE",
		"TZID:Custom/Offset",
		"BEGIN:STANDARD",
		"DTSTART:19700101T000000",
		"TZOFFSETFROM:+0300",
		"TZOFFSETTO:+0300",
		"END:STANDARD",
	)...)

	zone := newFloatingZone(zoneDefinition)
	if zone.loc != nil {
		t.Fatal("Custom/Offset unexpectedly resolved as a host location")
	}
	got, ok := zone.resolve("20240601T100000")
	if !ok {
		t.Fatal("floatingZone.resolve() ok = false")
	}
	if want := time.Date(2024, 6, 1, 7, 0, 0, 0, time.UTC); !got.Equal(want) {
		t.Fatalf("floatingZone.resolve() = %v, want %v", got, want)
	}
}

func TestSubmittedTimezoneWallClockUsesAbsoluteTransitions(t *testing.T) {
	zoneDefinition := wrapCalendar(componentLines("VTIMEZONE",
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
	)...)
	zone := newFloatingZone(zoneDefinition)
	if zone.loc != nil || zone.root == nil {
		t.Fatalf("expected a submitted-only timezone, got %#v", zone)
	}

	tests := []struct {
		name    string
		instant time.Time
		want    string
	}{
		{name: "before spring transition", instant: time.Date(2024, 3, 10, 7, 30, 0, 0, time.UTC), want: "20240310T013000"},
		{name: "after spring transition", instant: time.Date(2024, 3, 10, 8, 30, 0, 0, time.UTC), want: "20240310T033000"},
		{name: "before fall transition", instant: time.Date(2024, 11, 3, 6, 30, 0, 0, time.UTC), want: "20241103T013000"},
		{name: "after fall transition", instant: time.Date(2024, 11, 3, 7, 30, 0, 0, time.UTC), want: "20241103T013000"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if got := zone.wallClock(test.instant).Format("20060102T150405"); got != test.want {
				t.Fatalf("wallClock(%v) = %s, want %s", test.instant, got, test.want)
			}
		})
	}
}
