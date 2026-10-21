package ical

import (
	"strings"
	"testing"
	"time"
)

// A VCALENDAR opened inside the open root continues it, as the DAV layer reads
// a stored object, so the components inside it are the object's own.
func TestNestedVCalendarContinuesTheRoot(t *testing.T) {
	raw := "BEGIN:VCALENDAR\r\nVERSION:2.0\r\nBEGIN:VCALENDAR\r\n" +
		"BEGIN:VEVENT\r\nUID:a\r\nDTSTART:20260105T090000Z\r\nDTEND:20260105T100000Z\r\nRRULE:FREQ=WEEKLY;COUNT=3\r\nEND:VEVENT\r\n" +
		"END:VCALENDAR\r\nEND:VCALENDAR\r\n"
	dtstart := utc(2026, 1, 5, 9, 0)
	starts := instanceStarts(requireRecurrenceResult[RecurrenceInstance](t)(
		collectRecurrenceInstances(raw, "VEVENT", dtstart, time.Hour, utc(2026, 1, 1, 0, 0), utc(2026, 2, 1, 0, 0), 100, nil)))
	if len(starts) != 3 || !starts[1].Equal(utc(2026, 1, 12, 9, 0)) {
		t.Fatalf("instances = %v, want the three weekly instances from 5 January", starts)
	}
	if exceeds, ok := RecurrenceSetExceedsLimit(raw, 2); !ok || !exceeds {
		t.Errorf("RecurrenceSetExceedsLimit(limit 2) = %v, %v; want true, true", exceeds, ok)
	}
	if bounds := ConservativeRecurrenceBounds(raw); !bounds.Recurring || bounds.UntilUnknown || bounds.Until == nil {
		t.Errorf("bounds = %+v, want a recurring set with a known until", bounds)
	}
}

func TestComponentReaderTakesOnlyTheObjectsOwnComponents(t *testing.T) {
	raw := "BEGIN:VCALENDAR\r\nVERSION:2.0\r\n" +
		"BEGIN:VTIMEZONE\r\nTZID:Zone\r\nBEGIN:STANDARD\r\nDTSTART:19700101T000000\r\nTZOFFSETFROM:+0000\r\nTZOFFSETTO:+0000\r\nEND:STANDARD\r\nEND:VTIMEZONE\r\n" +
		"BEGIN:VEVENT\r\nUID:a\r\nDTSTART:20260105T090000Z\r\n" +
		"BEGIN:VALARM\r\nACTION:DISPLAY\r\nTRIGGER:-PT15M\r\nEND:VALARM\r\n" +
		"SUMMARY:after the alarm\r\nEND:VEVENT\r\n" +
		"BEGIN:VEVENT\r\nUID:a\r\nRECURRENCE-ID:20260112T090000Z\r\nDTSTART:20260112T100000Z\r\nEND:VEVENT\r\n" +
		"END:VCALENDAR\r\n"
	events := componentsNamed(raw, "VEVENT")
	if len(events) != 2 {
		t.Fatalf("VEVENTs = %d, want 2", len(events))
	}
	if _, ok := ComponentProperty(&events[0], "ACTION"); ok {
		t.Error("the VALARM's ACTION was read as the VEVENT's")
	}
	if summary, ok := ComponentProperty(&events[0], "SUMMARY"); !ok || summary.Value != "after the alarm" {
		t.Errorf("SUMMARY after the VALARM = %+v, %v; want the VEVENT's", summary, ok)
	}
	if _, ok := ComponentProperty(&events[1], "RECURRENCE-ID"); !ok {
		t.Error("the second VEVENT lost its RECURRENCE-ID")
	}
	if standards := componentsNamed(raw, "STANDARD"); len(standards) != 0 {
		t.Errorf("STANDARD read as a top-level component: %d", len(standards))
	}
	if timezones := componentsNamed(raw, "VTIMEZONE"); len(timezones) != 1 {
		t.Errorf("VTIMEZONEs = %d, want 1", len(timezones))
	}
}

// A content line starts with its name (RFC 5545 §3.1), so a line opening with
// whitespace that does not fold it onto the line before is no BEGIN or END:
// the SUMMARY after it is the VEVENT's own, as the DAV layer reads it too.
func TestALineOpeningWithWhitespaceIsNoBeginOrEnd(t *testing.T) {
	raw := "BEGIN:VCALENDAR\r\nVERSION:2.0\r\nBEGIN:VEVENT\r\nUID:a\r\nDTSTART:20260105T090000Z\r\n" +
		"\fBEGIN:VEVENT\r\nSUMMARY:the event's\r\nEND:VEVENT\r\nEND:VCALENDAR\r\n"
	events := componentsNamed(raw, "VEVENT")
	if len(events) != 1 {
		t.Fatalf("VEVENTs = %d, want 1", len(events))
	}
	if summary, ok := ComponentProperty(&events[0], "SUMMARY"); !ok || summary.Value != "the event's" {
		t.Errorf("SUMMARY = %+v, %v; want the VEVENT's own", summary, ok)
	}
}

// The VTIMEZONE a TZID names is the one whose first TZID property it is, as
// the DAV layer reads a definition; a later TZID in the same component names
// nothing, and neither does a later VTIMEZONE repeating a TZID.
func TestTimezoneIsNamedByItsFirstTZID(t *testing.T) {
	lines := UnfoldLines("BEGIN:VCALENDAR\r\n" +
		"BEGIN:VTIMEZONE\r\nTZID:\r\nTZID:Private/Zone\r\n" +
		"BEGIN:STANDARD\r\nDTSTART:19700101T000000\r\nTZOFFSETFROM:+0100\r\nTZOFFSETTO:+0100\r\nEND:STANDARD\r\n" +
		"END:VTIMEZONE\r\n" +
		"BEGIN:VTIMEZONE\r\nTZID:Private/Other\r\n" +
		"BEGIN:STANDARD\r\nDTSTART:19700101T000000\r\nTZOFFSETFROM:+0200\r\nTZOFFSETTO:+0200\r\nEND:STANDARD\r\n" +
		"END:VTIMEZONE\r\n" +
		"BEGIN:VTIMEZONE\r\nTZID:Private/Other\r\n" + HostDefinitionProperty + ":TRUE\r\n" +
		"BEGIN:STANDARD\r\nDTSTART:19700101T000000\r\nTZOFFSETFROM:+0300\r\nTZOFFSETTO:+0300\r\nEND:STANDARD\r\n" +
		"BEGIN:DAYLIGHT\r\nDTSTART:19700601T000000\r\nTZOFFSETFROM:+0300\r\nTZOFFSETTO:+0400\r\nEND:DAYLIGHT\r\n" +
		"END:VTIMEZONE\r\nEND:VCALENDAR\r\n")
	timezones := objectTimezones(lines)
	if _, defined := timezones["Private/Zone"]; defined {
		t.Error("a VTIMEZONE whose first TZID is empty was read as Private/Zone")
	}
	if timezone, defined := timezones["Private/Other"]; !defined || len(timezone.observances) != 1 || timezone.hostGenerated {
		t.Errorf("Private/Other = %+v, %v; want the first definition's one observance", timezone, defined)
	}
}

// Strictly read, every structural defect a stored object is forgiven is an
// error; read leniently, the same lines give the components the stored-object
// rules say.
func TestReadComponentsStrictRefusesWhatLenientRecovers(t *testing.T) {
	const root = "BEGIN:VCALENDAR\r\n"
	for _, raw := range []string{
		root + "BEGIN:VEVENT\r\nEND:VTODO\r\nEND:VEVENT\r\nEND:VCALENDAR\r\n",
		root + "BEGIN:VEVENT\r\nBEGIN:VALARM\r\nEND:VEVENT\r\nEND:VCALENDAR\r\n",
		root + "BEGIN:VEVENT\r\n",
		root + "END:VCALENDAR\r\n" + root + "END:VCALENDAR\r\n",
		root + root + "END:VCALENDAR\r\nEND:VCALENDAR\r\n",
		"X-BEFORE:1\r\n" + root + "END:VCALENDAR\r\n",
		"BEGIN:VEVENT\r\nEND:VEVENT\r\n" + root + "END:VCALENDAR\r\n",
		"END:VEVENT\r\n" + root + "END:VCALENDAR\r\n",
	} {
		count := func(strict bool) (int, error) {
			components := 0
			_, err := ReadComponents(UnfoldLines(raw), strict,
				func(parent *int, name string) (*int, error) { components++; return new(int), nil },
				func(*int, string) error { return nil })
			return components, err
		}
		if _, err := count(true); err == nil {
			t.Errorf("strict reading accepted %q", raw)
		}
		if _, err := count(false); err != nil {
			t.Errorf("lenient reading refused %q: %v", raw, err)
		}
	}
	if _, err := ReadComponents(UnfoldLines("BEGIN:VEVENT\r\nEND:VEVENT\r\n"), false,
		func(*int, string) (*int, error) { return new(int), nil }, func(*int, string) error { return nil }); err == nil {
		t.Error("lenient reading accepted an object with no VCALENDAR")
	}
}

// BEGIN and END are recognised whatever their case, and the component name is
// handed on upper-cased; a line that only resembles one is content.
func TestReadComponentsRecognisesBeginAndEndInAnyCase(t *testing.T) {
	raw := "begin:vcalendar\r\nBegin:VEvent\r\nsummary:lower\r\nBEGINNING:x\r\nEND :VEVENT\r\nend:vevent\r\nEnd:VCalendar\r\n"
	var names, contents []string
	_, err := ReadComponents(UnfoldLines(raw), true,
		func(parent *int, name string) (*int, error) { names = append(names, name); return new(int), nil },
		func(_ *int, line string) error { contents = append(contents, line); return nil })
	if err != nil {
		t.Fatal(err)
	}
	if len(names) != 2 || names[0] != "VCALENDAR" || names[1] != "VEVENT" {
		t.Errorf("components = %q, want [VCALENDAR VEVENT]", names)
	}
	if want := []string{"summary:lower", "BEGINNING:x", "END :VEVENT"}; len(contents) != len(want) ||
		contents[0] != want[0] || contents[1] != want[1] || contents[2] != want[2] {
		t.Errorf("content lines = %q, want %q", contents, want)
	}
}

// nestedComponentLines builds n components opened one inside another beneath
// a VCALENDAR root, so the deepest nests n+1 levels with VCALENDAR as the
// first.
func nestedComponentLines(n int, name string) string {
	var raw strings.Builder
	raw.WriteString("BEGIN:VCALENDAR\r\n")
	for range n {
		raw.WriteString("BEGIN:" + name + "\r\n")
	}
	for range n {
		raw.WriteString("END:" + name + "\r\n")
	}
	raw.WriteString("END:VCALENDAR\r\n")
	return raw.String()
}

func readNested(t *testing.T, raw string, strict bool) ([]string, []string, error) {
	t.Helper()
	var opened, contents []string
	_, err := ReadComponents(UnfoldLines(raw), strict,
		func(_ *int, name string) (*int, error) { opened = append(opened, name); return new(int), nil },
		func(_ *int, line string) error { contents = append(contents, line); return nil })
	return opened, contents, err
}

// Strictly read, a body nesting exactly maxComponentDepth levels, VCALENDAR
// counting as the first, is accepted; one level deeper is refused, since
// real objects nest about four deep and nothing legitimate needs sixteen.
func TestReadComponentsStrictAcceptsTheDepthLimitRefusesOneMore(t *testing.T) {
	if _, _, err := readNested(t, nestedComponentLines(maxComponentDepth-1, "X-DEEP"), true); err != nil {
		t.Fatalf("a body nesting %d levels was refused: %v", maxComponentDepth, err)
	}
	if _, _, err := readNested(t, nestedComponentLines(maxComponentDepth, "X-DEEP"), true); err == nil {
		t.Fatalf("a body nesting %d levels was accepted", maxComponentDepth+1)
	}
}

// Read leniently, as a stored row is, a subtree nested past the limit is
// skipped rather than built: nothing inside it is opened or passed as
// content, its BEGIN and END lines balance by count alone, and the shallow
// components around it -- those before it, and those after its over-deep
// subtree finally closes -- read as if it were not there.
func TestReadComponentsLeniencySkipsPastTheDepthLimitButKeepsSiblings(t *testing.T) {
	const beyond = 5 // overflow levels past the limit, on top of the kept ones
	raw := "BEGIN:VCALENDAR\r\nBEGIN:VEVENT\r\nUID:before\r\nEND:VEVENT\r\n" +
		strings.Repeat("BEGIN:X-DEEP\r\n", maxComponentDepth-1+beyond) +
		"SHOULD-NOT-APPEAR:x\r\n" +
		strings.Repeat("END:X-DEEP\r\n", maxComponentDepth-1+beyond) +
		"BEGIN:VEVENT\r\nUID:after\r\nEND:VEVENT\r\nEND:VCALENDAR\r\n"

	opened, contents, err := readNested(t, raw, false)
	if err != nil {
		t.Fatalf("lenient reading refused a deep body: %v", err)
	}
	deepOpens := 0
	for _, name := range opened {
		if name == "X-DEEP" {
			deepOpens++
		}
	}
	// The "before" VEVENT closes before the X-DEEP chain opens, so the chain
	// starts its own nesting at level 2, beneath VCALENDAR alone, and keeps
	// maxComponentDepth-1 levels of it within the limit.
	if want := maxComponentDepth - 1; deepOpens != want {
		t.Errorf("X-DEEP opens = %d, want %d kept within the limit", deepOpens, want)
	}
	if opened[0] != "VCALENDAR" || opened[len(opened)-1] != "VEVENT" {
		t.Errorf("opened = %q, want VCALENDAR first and the trailing VEVENT last", opened)
	}
	for _, line := range contents {
		if line == "SHOULD-NOT-APPEAR:x" {
			t.Error("a content line inside the skipped subtree was read")
		}
	}
	want := map[string]bool{"UID:before": false, "UID:after": false}
	for _, line := range contents {
		if _, ok := want[line]; ok {
			want[line] = true
		}
	}
	for line, seen := range want {
		if !seen {
			t.Errorf("%q was not read; the siblings around the skipped subtree should still read", line)
		}
	}
}

// A lenient read's cost past the depth limit is bounded by the limit, not by
// how much deeper the body nests: an 8.5 MB body nested hundreds of thousands
// of levels deep allocates no more than one nested to the limit, since nothing
// past the limit is built or has its name upper-cased.
func TestReadComponentsDeeplyNestedBodyCostsNoMoreThanAShallowOne(t *testing.T) {
	read := func(raw string) func() {
		lines := UnfoldLines(raw)
		return func() {
			_, _ = ReadComponents(lines, false,
				func(_ *int, _ string) (*int, error) { return new(int), nil },
				func(_ *int, _ string) error { return nil })
		}
	}
	shallow := read(nestedComponentLines(maxComponentDepth-1, "X-DEEP"))
	deep := read(nestedComponentLines(400000, "X-DEEP"))
	shallowAllocs := testing.AllocsPerRun(20, shallow)
	deepAllocs := testing.AllocsPerRun(5, deep)
	if deepAllocs > shallowAllocs+20 {
		t.Errorf("allocations reading 400,000 nested levels = %.0f, want close to the %d-level body's %.0f",
			deepAllocs, maxComponentDepth-1, shallowAllocs)
	}
}

// Reading the structure of an object costs no copy of each content line, so a
// calendar-query paying for it once per candidate does not pay per line.
func TestReadComponentsDoesNotCopyContentLines(t *testing.T) {
	if raceDetectorEnabled {
		t.Skip("the race detector's instrumentation allocates")
	}
	lines := []string{"BEGIN:VCALENDAR", "BEGIN:VEVENT"}
	for range 200 {
		lines = append(lines, "description;language=en:lower-case text that upper-casing would copy")
	}
	lines = append(lines, "END:VEVENT", "END:VCALENDAR")
	root, child := new(int), new(int)
	allocs := testing.AllocsPerRun(20, func() {
		_, _ = ReadComponents(lines, true,
			func(parent *int, _ string) (*int, error) {
				if parent == nil {
					return root, nil
				}
				return child, nil
			},
			func(*int, string) error { return nil })
	})
	if allocs > 10 {
		t.Errorf("ReadComponents over %d lines allocates %.0f times; want no per-line allocation", len(lines), allocs)
	}
}
