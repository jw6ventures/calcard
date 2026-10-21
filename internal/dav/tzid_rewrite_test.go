package dav

import (
	"reflect"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/jw6ventures/calcard/internal/ical"
	"github.com/jw6ventures/calcard/internal/store"
)

// assertWriteRewrites validates a VEVENT carrying before's lines, with zone,
// a VTIMEZONE's lines or "" for none, ahead of it, so a TZID naming it is
// already defined and the write path generates no host definition for it.
// The stored body must equal zone followed by a VEVENT carrying after's
// lines, and the rewritten body must revalidate -- accepted again, with the
// same metadata the first validation read. VERSION, PRODID and DTSTAMP,
// which buildVEvent and buildCalendarObject add once each, are otherwise
// untouched.
func assertWriteRewrites(t *testing.T, uid, zone string, before, after []string) {
	t.Helper()
	raw := buildCalendarObject(zone, buildVEvent(uid, before...))
	want := buildCalendarObject(zone, buildVEvent(uid, after...))
	validated, fault := validateCalendarObjectForStorage(raw, nil)
	if fault != nil {
		t.Fatalf("%s: refused: %+v\n%s", uid, fault, raw)
	}
	if validated.Body != want {
		t.Errorf("%s: stored\n%q\nwant\n%q", uid, validated.Body, want)
	}
	assertRewriteRevalidates(t, uid, validated)
}

// assertRewriteRevalidates checks that re-running the validator over the
// stored body accepts it and reads the same metadata: dtstart, dtend, all_day
// and the recurrence bounds.
func assertRewriteRevalidates(t *testing.T, uid string, validated *validatedCalendarObject) {
	t.Helper()
	reanalyzed, err := analyzeICalendar(validated.Body)
	if err != nil {
		t.Fatalf("%s: rewritten body does not analyze: %v\n%s", uid, err, validated.Body)
	}
	if !reflect.DeepEqual(reanalyzed.Metadata, validated.Analysis.Metadata) {
		t.Errorf("%s: metadata reading the stored body = %+v, want the metadata the write read: %+v",
			uid, reanalyzed.Metadata, validated.Analysis.Metadata)
	}
	if revalidated, fault := validateCalendarObjectForStorage(validated.Body, nil); fault != nil {
		t.Errorf("%s: rewritten body refused on revalidation: %+v\n%s", uid, fault, validated.Body)
	} else if revalidated.Body != validated.Body {
		t.Errorf("%s: revalidating the stored body changes it again:\n%q\nwant\n%q", uid, revalidated.Body, validated.Body)
	}
}

// Every case DAVx5's relaxed ical4j parsing misreads: a TZID equal to UTC,
// whatever its case and quoting, loses its local time to the device's own
// zone, and white space inside or around another TZID's value keeps it from
// resolving at all. The write path rewrites both; the host lookup these
// cases also exercise needs the zones loaded.
func TestTZIDRewriteUTCAndWhiteSpace(t *testing.T) {
	requireHostZones(t, "Europe/Berlin", "Etc/UTC")
	berlin := buildVTimezone("Europe/Berlin")
	tests := []struct {
		name          string
		zone          string
		before, after []string
	}{
		{name: "UTC uppercase",
			before: []string{"DTSTART;TZID=UTC:20260701T100000", "DTEND;TZID=UTC:20260701T110000"},
			after:  []string{"DTSTART:20260701T100000Z", "DTEND:20260701T110000Z"}},
		{name: "utc lowercase, no VTIMEZONE of its own",
			before: []string{"DTSTART;TZID=utc:20260701T100000", "DTEND:20260701T110000Z"},
			after:  []string{"DTSTART:20260701T100000Z", "DTEND:20260701T110000Z"}},
		{name: "Utc mixed case",
			before: []string{"DTSTART;TZID=Utc:20260701T100000", "DTEND:20260701T110000Z"},
			after:  []string{"DTSTART:20260701T100000Z", "DTEND:20260701T110000Z"}},
		{name: "UTC quoted",
			before: []string{"DTSTART;TZID=\"UTC\":20260701T100000", "DTEND:20260701T110000Z"},
			after:  []string{"DTSTART:20260701T100000Z", "DTEND:20260701T110000Z"}},
		{name: "UTC quoted, untrimmed",
			before: []string{"DTSTART;TZID=\" UTC \":20260701T100000", "DTEND:20260701T110000Z"},
			after:  []string{"DTSTART:20260701T100000Z", "DTEND:20260701T110000Z"}},
		{name: "utc unquoted, untrimmed",
			before: []string{"DTSTART;TZID= utc :20260701T100000", "DTEND:20260701T110000Z"},
			after:  []string{"DTSTART:20260701T100000Z", "DTEND:20260701T110000Z"}},
		{name: "non-UTC zone, quoted and untrimmed", zone: berlin,
			before: []string{"DTSTART;TZID=\" Europe/Berlin\":20260701T100000", "DTEND;TZID=Europe/Berlin:20260701T110000"},
			after:  []string{"DTSTART;TZID=\"Europe/Berlin\":20260701T100000", "DTEND;TZID=Europe/Berlin:20260701T110000"}},
		{name: "non-UTC zone, unquoted and untrimmed", zone: berlin,
			before: []string{"DTSTART;TZID= Europe/Berlin :20260701T100000", "DTEND;TZID=Europe/Berlin:20260701T110000"},
			after:  []string{"DTSTART;TZID=Europe/Berlin:20260701T100000", "DTEND;TZID=Europe/Berlin:20260701T110000"}},
		{name: "non-UTC zone, quoted, white space before the quotes", zone: berlin,
			before: []string{"DTSTART;TZID= \"Europe/Berlin\":20260701T100000", "DTEND;TZID=Europe/Berlin:20260701T110000"},
			after:  []string{"DTSTART;TZID=\"Europe/Berlin\":20260701T100000", "DTEND;TZID=Europe/Berlin:20260701T110000"}},
		{name: "non-UTC zone, quoted, white space after the quotes", zone: berlin,
			before: []string{"DTSTART;TZID=\"Europe/Berlin\" :20260701T100000", "DTEND;TZID=Europe/Berlin:20260701T110000"},
			after:  []string{"DTSTART;TZID=\"Europe/Berlin\":20260701T100000", "DTEND;TZID=Europe/Berlin:20260701T110000"}},
		{name: "non-UTC zone, quoted, white space after the quotes before another parameter", zone: berlin,
			before: []string{"DTSTART;TZID=\"Europe/Berlin\" ;X-A=1:20260701T100000", "DTEND;TZID=Europe/Berlin:20260701T110000"},
			after:  []string{"DTSTART;TZID=\"Europe/Berlin\";X-A=1:20260701T100000", "DTEND;TZID=Europe/Berlin:20260701T110000"}},
		{name: "non-UTC zone, quoted, white space inside and around the quotes", zone: berlin,
			before: []string{"DTSTART;TZID=\t\" Europe/Berlin \" :20260701T100000", "DTEND;TZID=Europe/Berlin:20260701T110000"},
			after:  []string{"DTSTART;TZID=\"Europe/Berlin\":20260701T100000", "DTEND;TZID=Europe/Berlin:20260701T110000"}},
		{name: "UTC quoted, white space around the quotes",
			before: []string{"DTSTART;TZID= \"UTC\" :20260701T100000", "DTEND:20260701T110000Z"},
			after:  []string{"DTSTART:20260701T100000Z", "DTEND:20260701T110000Z"}},
		{name: "Etc/UTC is left as it is: Java knows it, so DAVx5 reads it correctly",
			before: []string{"DTSTART;TZID=Etc/UTC:20260701T100000", "DTEND;TZID=Etc/UTC:20260701T110000"},
			after:  []string{"DTSTART;TZID=Etc/UTC:20260701T100000", "DTEND;TZID=Etc/UTC:20260701T110000"}},
		// readICalProperty admits TZID repeated with the same value
		// (conflictingICalendarParameter refuses only a differing repeat);
		// every repeat is rewritten the same way, or revalidating the stored
		// line would refuse it -- a conflicting repeat, for a trim, or a
		// TZID validICalDateValue does not admit once the value is in UTC
		// form.
		{name: "UTC repeated identically",
			before: []string{"DTSTART;TZID=UTC;TZID=UTC:20260701T100000", "DTEND:20260701T110000Z"},
			after:  []string{"DTSTART:20260701T100000Z", "DTEND:20260701T110000Z"}},
		{name: "non-UTC zone repeated identically, untrimmed and quoted", zone: berlin,
			before: []string{"DTSTART;TZID=\" Europe/Berlin\";TZID=\" Europe/Berlin\":20260701T100000", "DTEND;TZID=Europe/Berlin:20260701T110000"},
			after:  []string{"DTSTART;TZID=\"Europe/Berlin\";TZID=\"Europe/Berlin\":20260701T100000", "DTEND;TZID=Europe/Berlin:20260701T110000"}},
		{name: "non-UTC zone repeated identically, white space around the quotes", zone: berlin,
			before: []string{"DTSTART;TZID= \"Europe/Berlin\";TZID=\"Europe/Berlin\" :20260701T100000", "DTEND;TZID=Europe/Berlin:20260701T110000"},
			after:  []string{"DTSTART;TZID=\"Europe/Berlin\";TZID=\"Europe/Berlin\":20260701T100000", "DTEND;TZID=Europe/Berlin:20260701T110000"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assertWriteRewrites(t, "tzid-"+tt.name, tt.zone, tt.before, tt.after)
		})
	}
}

// A folded DTSTART is rewritten whole: the edit replaces the logical line's
// full span in raw, so the stored line is never folded at all, something the
// SQL repair cannot do without rewriting around the fold.
func TestTZIDRewriteFoldedLine(t *testing.T) {
	assertWriteRewrites(t, "folded", "",
		[]string{"DTSTART;TZID=UT\r\n C:20260701T100000", "DTEND;TZID=UTC:20260701T110000"},
		[]string{"DTSTART:20260701T100000Z", "DTEND:20260701T110000Z"})
}

// EXDATE and RDATE lists get a "Z" on every value they hold, not only the
// first, and an untrimmed TZID on either is trimmed the same as on any other
// property.
func TestTZIDRewriteExdateAndRdateLists(t *testing.T) {
	requireHostZones(t, "Europe/Berlin")
	assertWriteRewrites(t, "exdate-list", "",
		[]string{"DTSTART;TZID=UTC:20260701T100000", "RRULE:FREQ=WEEKLY;COUNT=4",
			"EXDATE;TZID=UTC:20260708T100000,20260715T100000", "DTEND:20260701T110000Z"},
		[]string{"DTSTART:20260701T100000Z", "RRULE:FREQ=WEEKLY;COUNT=4",
			"EXDATE:20260708T100000Z,20260715T100000Z", "DTEND:20260701T110000Z"})
	assertWriteRewrites(t, "rdate-list-untrimmed", buildVTimezone("Europe/Berlin"),
		[]string{"DTSTART:20260701T100000Z", "RDATE;TZID= Europe/Berlin :20260702T100000,20260703T100000", "DTEND:20260701T110000Z"},
		[]string{"DTSTART:20260701T100000Z", "RDATE;TZID=Europe/Berlin:20260702T100000,20260703T100000", "DTEND:20260701T110000Z"})
}

// RDATE PERIOD values rewrite each date-time of a period, keeping a duration
// as it is, since RFC 5545 section 3.3.9 reads neither a Z nor a TZID into one.
func TestTZIDRewriteRdatePeriods(t *testing.T) {
	assertWriteRewrites(t, "rdate-period-duration", "",
		[]string{"DTSTART;TZID=UTC:20260701T100000", "RDATE;VALUE=PERIOD;TZID=UTC:20260720T100000/PT1H", "DTEND:20260701T110000Z"},
		[]string{"DTSTART:20260701T100000Z", "RDATE;VALUE=PERIOD:20260720T100000Z/PT1H", "DTEND:20260701T110000Z"})
	assertWriteRewrites(t, "rdate-period-end", "",
		[]string{"DTSTART;TZID=UTC:20260701T100000", "RDATE;VALUE=PERIOD;TZID=UTC:20260720T100000/20260720T110000", "DTEND:20260701T110000Z"},
		[]string{"DTSTART:20260701T100000Z", "RDATE;VALUE=PERIOD:20260720T100000Z/20260720T110000Z", "DTEND:20260701T110000Z"})
}

// A series and a RANGE=THISANDFUTURE override naming UTC rewrite together:
// validateRecurrenceIdentities holds the override's RECURRENCE-ID to the
// series DTSTART's form and TZID before completion, and the rewrite changes
// both the same way, so the stored result still carries one form and TZID
// between them.
func TestTZIDRewriteOverrideSeries(t *testing.T) {
	raw := buildCalendarObject(
		buildVEvent("series", "DTSTART;TZID=UTC:20260701T100000", "DTEND;TZID=UTC:20260701T110000", "RRULE:FREQ=WEEKLY;COUNT=4"),
		buildVEvent("series", "RECURRENCE-ID;RANGE=THISANDFUTURE;TZID=UTC:20260715T100000",
			"DTSTART;TZID=UTC:20260715T120000", "DTEND;TZID=UTC:20260715T130000"))
	want := buildCalendarObject(
		buildVEvent("series", "DTSTART:20260701T100000Z", "DTEND:20260701T110000Z", "RRULE:FREQ=WEEKLY;COUNT=4"),
		buildVEvent("series", "RECURRENCE-ID;RANGE=THISANDFUTURE:20260715T100000Z",
			"DTSTART:20260715T120000Z", "DTEND:20260715T130000Z"))
	validated, fault := validateCalendarObjectForStorage(raw, nil)
	if fault != nil {
		t.Fatalf("refused: %+v\n%s", fault, raw)
	}
	if validated.Body != want {
		t.Errorf("stored\n%q\nwant\n%q", validated.Body, want)
	}
	assertRewriteRevalidates(t, "series", validated)
}

// A VTODO's DUE rewrites the same way DTSTART, DTEND and RECURRENCE-ID do.
func TestTZIDRewriteVTodoDue(t *testing.T) {
	raw := buildCalendarObject(buildVTodo("task", "DTSTART;TZID=UTC:20260701T090000", "DUE;TZID=UTC:20260701T170000"))
	want := buildCalendarObject(buildVTodo("task", "DTSTART:20260701T090000Z", "DUE:20260701T170000Z"))
	validated, fault := validateCalendarObjectForStorage(raw, nil)
	if fault != nil {
		t.Fatalf("refused: %+v\n%s", fault, raw)
	}
	if validated.Body != want {
		t.Errorf("stored\n%q\nwant\n%q", validated.Body, want)
	}
	assertRewriteRevalidates(t, "task", validated)
}

// An object defining its own zone named UTC away from zero reads a local
// time naming it as another instant than the UTC form would, so the write
// path leaves every time naming it, and the definition itself, as the object
// wrote them.
func TestTZIDRewriteHeldByAPrivateUTCDefinition(t *testing.T) {
	shifted := buildComponent("VTIMEZONE", "TZID: UTC ",
		buildComponent("STANDARD", "DTSTART:19700101T000000", "TZOFFSETFROM:+0100", "TZOFFSETTO:+0100"))
	raw := buildCalendarObject(shifted, buildVEvent("held", "DTSTART;TZID=UTC:20260701T100000", "DTEND;TZID=UTC:20260701T110000"))
	validated, fault := validateCalendarObjectForStorage(raw, nil)
	if fault != nil {
		t.Fatalf("refused: %+v\n%s", fault, raw)
	}
	want := buildCalendarObject(shifted, buildVEvent("held", "DTSTART;TZID=UTC:20260701T100000", "DTEND;TZID=UTC:20260701T110000"))
	if validated.Body != want {
		t.Errorf("stored\n%q\nwant the object as sent, less a write's usual completion\n%q", validated.Body, want)
	}
	assertRewriteRevalidates(t, "held", validated)
}

// A held UTC-named value still trims: it is not rewritten to its UTC form,
// since the hold makes it the private zone's own time, but DAVx5 keeps a
// TZID's white space regardless of what defines the zone, so an untrimmed
// name still resolves to nothing. This is also what the v1.2.0 SQL repair
// does for the same shape, less its own structural limits (see
// internal/store/migration_tzid_repair_postgres_test.go).
func TestTZIDRewriteHeldByAPrivateUTCDefinitionStillTrims(t *testing.T) {
	shifted := buildComponent("VTIMEZONE", "TZID:UTC",
		buildComponent("STANDARD", "DTSTART:19700101T000000", "TZOFFSETFROM:+0100", "TZOFFSETTO:+0100"))
	raw := buildCalendarObject(shifted, buildVEvent("held-padded",
		"DTSTART;TZID=\" UTC\":20260701T100000", "DTEND;TZID=\"UTC \":20260701T110000"))
	validated, fault := validateCalendarObjectForStorage(raw, nil)
	if fault != nil {
		t.Fatalf("refused: %+v\n%s", fault, raw)
	}
	want := buildCalendarObject(shifted, buildVEvent("held-padded",
		"DTSTART;TZID=\"UTC\":20260701T100000", "DTEND;TZID=\"UTC\":20260701T110000"))
	if validated.Body != want {
		t.Errorf("stored\n%q\nwant the padding trimmed, the UTC name and quoting kept\n%q", validated.Body, want)
	}
	assertRewriteRevalidates(t, "held-padded", validated)
}

// Readers look a value's TZID up among the object's definitions by its
// trimmed spelling, and only a name no definition answers is taken for UTC.
// So the hold is of that very spelling: a definition of UTC away from zero
// holds the values naming UTC, not those naming utc, which every reader
// resolves as the UTC it is and which are rewritten; a definition of utc
// holds the values naming utc; and a padded name is held and trimmed like
// the name it trims to.
func TestTZIDRewriteHoldIsOfTheDefinitionsOwnSpelling(t *testing.T) {
	shifted := func(tzid string) string {
		return buildComponent("VTIMEZONE", "TZID:"+tzid,
			buildComponent("STANDARD", "DTSTART:19700101T000000", "TZOFFSETFROM:+0100", "TZOFFSETTO:+0100"))
	}
	tests := []struct {
		name          string
		zone          string
		before, after []string
	}{
		{name: "UTC defined away from zero, value names utc", zone: shifted("UTC"),
			before: []string{"DTSTART;TZID=utc:20260701T100000", "DTEND;TZID=utc:20260701T110000"},
			after:  []string{"DTSTART:20260701T100000Z", "DTEND:20260701T110000Z"}},
		{name: "utc defined away from zero, value names utc", zone: shifted("utc"),
			before: []string{"DTSTART;TZID=utc:20260701T100000", "DTEND;TZID=utc:20260701T110000"},
			after:  []string{"DTSTART;TZID=utc:20260701T100000", "DTEND;TZID=utc:20260701T110000"}},
		{name: "utc defined away from zero, value names UTC", zone: shifted("utc"),
			before: []string{"DTSTART;TZID=UTC:20260701T100000", "DTEND;TZID=UTC:20260701T110000"},
			after:  []string{"DTSTART:20260701T100000Z", "DTEND:20260701T110000Z"}},
		{name: "UTC defined away from zero, padded value is held and trimmed", zone: shifted("UTC"),
			before: []string{"DTSTART;TZID=\" UTC\":20260701T100000", "DTEND;TZID= UTC :20260701T110000"},
			after:  []string{"DTSTART;TZID=\"UTC\":20260701T100000", "DTEND;TZID=UTC:20260701T110000"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assertWriteRewrites(t, "hold-"+tt.name, tt.zone, tt.before, tt.after)
		})
	}
}

// Readers read an offset only from a STANDARD or DAYLIGHT directly inside the
// VTIMEZONE, so one in a component nested in an observance moves no clock
// and holds nothing back.
func TestTZIDRewriteIgnoresAnOffsetNestedInAnObservance(t *testing.T) {
	zone := "BEGIN:VTIMEZONE\r\nTZID:UTC\r\nBEGIN:STANDARD\r\nDTSTART:19700101T000000\r\n" +
		"TZOFFSETFROM:+0000\r\nTZOFFSETTO:+0000\r\nBEGIN:X-NOTE\r\nTZOFFSETTO:+0100\r\nEND:X-NOTE\r\nEND:STANDARD\r\nEND:VTIMEZONE\r\n"
	assertWriteRewrites(t, "nested-offset", zone,
		[]string{"DTSTART;TZID=UTC:20260701T100000", "DTEND;TZID=UTC:20260701T110000"},
		[]string{"DTSTART:20260701T100000Z", "DTEND:20260701T110000Z"})
}

// A zero-offset definition named UTC reads every local time as the UTC
// instant it names, so the times are rewritten; the definition itself is
// kept, since RFC 5545 section 3.6.5 asks for one per TZID used, not that
// every one be used.
func TestTZIDRewriteZeroOffsetUTCDefinitionIsKept(t *testing.T) {
	zero := buildComponent("VTIMEZONE", "TZID:UTC",
		buildComponent("STANDARD", "DTSTART:19700101T000000", "TZOFFSETFROM:+0000", "TZOFFSETTO:+0000"))
	raw := buildCalendarObject(zero, buildVEvent("zero", "DTSTART;TZID=UTC:20260701T100000", "DTEND;TZID=UTC:20260701T110000"))
	validated, fault := validateCalendarObjectForStorage(raw, nil)
	if fault != nil {
		t.Fatalf("refused: %+v\n%s", fault, raw)
	}
	want := buildCalendarObject(zero, buildVEvent("zero", "DTSTART:20260701T100000Z", "DTEND:20260701T110000Z"))
	if validated.Body != want {
		t.Errorf("stored\n%q\nwant\n%q", validated.Body, want)
	}
	assertRewriteRevalidates(t, "zero", validated)
}

// tzidRewriteEdits never overlaps oneDayDateEndEdits or hostTimezoneEdits:
// validICalDateValue refuses a TZID on a DATE, so a DATE DTEND equal to its
// DATE DTSTART, which oneDayDateEndEdits rewrites to the day after, never
// carries one, and hostTimezoneEdits only removes or inserts VTIMEZONE lines,
// never a DTSTART, DTEND, DUE, RECURRENCE-ID, EXDATE or RDATE line.
func TestTZIDRewriteEditsDoNotOverlapOtherEditGenerators(t *testing.T) {
	requireHostZones(t, "America/New_York")
	raw := buildCalendarObject(buildVEvent("mixed",
		"DTSTART;VALUE=DATE:20260701", "DTEND;VALUE=DATE:20260701",
		"EXDATE;TZID=UTC:20260702T100000", "RDATE;TZID=America/New_York:20260703T100000"))
	root, err := parseICalendarObject(raw)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	if fault := validateCalendarObject(root); fault != nil {
		t.Fatalf("refused: %+v\n%s", fault, raw)
	}
	lines := ical.UnfoldLines(raw)
	offsets, ok := logicalLineOffsets(raw, lines)
	if !ok {
		t.Fatal("logicalLineOffsets failed")
	}
	dayEdits := oneDayDateEndEdits(raw, lines, offsets)
	tzidTrims, tzidUTCRewrites := tzidRewriteEdits(raw, lines, offsets, root)
	_, maxDate := ical.DateLimits()
	zoneEdits := hostTimezoneEdits(raw, lines, offsets, root, maxDate, ical.VTimezone)
	if len(dayEdits) == 0 || len(tzidUTCRewrites) == 0 || len(zoneEdits) == 0 {
		t.Fatalf("fixture does not exercise all three generators: day=%d tzid=%d zone=%d",
			len(dayEdits), len(tzidTrims)+len(tzidUTCRewrites), len(zoneEdits))
	}
	all := slices.Concat(dayEdits, tzidTrims, tzidUTCRewrites, zoneEdits)
	assertNoOverlappingEdits(t, all)
	// completeCalendarObject must also apply the three without panicking and
	// leave a body that parses.
	body := completeCalendarObject(raw, lines, root, maxDate, time.Now(), ical.VTimezone, maxDAVBodyBytes)
	if _, err := parseICalendarObject(body); err != nil {
		t.Fatalf("completed body does not parse: %v\n%s", err, body)
	}
}

func assertNoOverlappingEdits(t *testing.T, edits []bodyEdit) {
	t.Helper()
	sorted := slices.Clone(edits)
	slices.SortFunc(sorted, func(a, b bodyEdit) int { return a.start - b.start })
	for i := 1; i < len(sorted); i++ {
		if sorted[i].start < sorted[i-1].end {
			t.Errorf("edits overlap: %+v and %+v", sorted[i-1], sorted[i])
		}
	}
}

// The rewrite shrinks a line, or keeps it the same length, for every shape
// it applies to in practice: a single-valued property such as DTSTART, and
// an EXDATE or RDATE list short enough that removing one whole ";TZID=..."
// parameter covers the "Z" added per value.
func TestTZIDRewriteNeverGrowsTheBodyForOrdinaryShapes(t *testing.T) {
	raw := buildCalendarObject(buildVEvent("shrink",
		"DTSTART;TZID=UTC:20260701T100000", "RRULE:FREQ=DAILY;COUNT=5",
		"EXDATE;TZID=UTC:20260702T100000,20260703T100000,20260704T100000",
		"DTEND:20260701T110000Z"))
	validated, fault := validateCalendarObjectForStorage(raw, nil)
	if fault != nil {
		t.Fatalf("refused: %+v\n%s", fault, raw)
	}
	if len(validated.Body) > len(raw) {
		t.Errorf("rewritten body is %d octets, longer than the %d submitted", len(validated.Body), len(raw))
	}
}

// A long enough EXDATE list naming UTC grows the line when converted: one
// whole ";TZID=UTC" parameter is removed, but a "Z" is added to every value,
// and a list with more values than the parameter held letters comes out
// longer. completeCalendarObject guards exactly this rewrite by limit, as it
// already does for requiredPropertyEdits and hostTimezoneEdits, so a client
// can always write back a body it read at the server's own limit; the trims
// tzidRewriteEdits also makes are applied regardless, since they only shrink.
func TestTZIDRewriteUTCGrowthStaysWithinTheLimit(t *testing.T) {
	values := make([]string, 20)
	base := time.Date(2026, 7, 2, 10, 0, 0, 0, time.UTC)
	for i := range values {
		values[i] = base.AddDate(0, 0, i).Format("20060102T150405")
	}
	raw := buildCalendarObject(buildVEvent("grows",
		"DTSTART:20260701T100000Z", "RRULE:FREQ=DAILY;COUNT=1",
		"EXDATE;TZID=UTC:"+strings.Join(values, ","), "DTEND:20260701T110000Z"))
	root, err := parseICalendarObject(raw)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	if fault := validateCalendarObject(root); fault != nil {
		t.Fatalf("refused: %+v\n%s", fault, raw)
	}
	lines := ical.UnfoldLines(raw)
	complete := func(limit int64) string {
		return completeCalendarObject(raw, lines, root, ical.RecurrenceUntilSentinel, time.Now(), ical.VTimezone, limit)
	}
	converted := complete(int64(len(raw)) + 1000)
	if len(converted) <= len(raw) {
		t.Fatalf("fixture does not grow when converted: submitted %d octets, converted %d", len(raw), len(converted))
	}
	growth := int64(len(converted) - len(raw))

	if got := complete(int64(len(raw)) + growth - 1); got != raw {
		t.Errorf("one octet under the limit, stored\n%q\nwant the object as submitted\n%q", got, raw)
	}
	if got := complete(int64(len(raw)) + growth); got != converted {
		t.Errorf("at the limit, stored\n%q\nwant the converted body\n%q", got, converted)
	}
}

// A stored row can still carry TZID=utc in any case, as the v1.2.0 repair
// leaves a held or skipped value, and the report matcher reads it as UTC too,
// by the same case-insensitive rule the write path and the validator apply:
// zonedWallInstant is the one reader behind icalInstants here and the
// matcher in time_range.go.
func TestStoredUTCTZIDInAnyCaseIsReadAsUTC(t *testing.T) {
	stored := buildCalendarObject(buildVEvent("stored-utc", "DTSTART;TZID=utc:20260701T100000", "DTEND;TZID=Utc:20260701T110000"))
	want := time.Date(2026, 7, 1, 10, 0, 0, 0, time.UTC)
	if got := readsAt(t, stored); !got.Equal(want) {
		t.Errorf("matcher places the event at %s, want %s", got, want)
	}
	tr := &timeRange{Start: "20260701T095000Z", End: "20260701T110500Z"}
	body := mustReportText((&DavServer{}).generateFreeBusy(t.Context(),
		freeBusyCandidates([]store.Event{{UID: "stored-utc", RawICAL: stored}}, floatingZone{}, nil), tr))
	assertPublishedFreeBusy(t, parsedFreeBusyLines(t, body), []string{"FREEBUSY:20260701T100000Z/20260701T110000Z"})
}

// Many EXDATE values on one line, and many separately TZID-qualified
// properties, each cost the rewrite a linear amount of work: one pass over
// the logical lines, without re-splitting or re-unfolding the body.
func TestTZIDRewriteScalesLinearly(t *testing.T) {
	requireHostZones(t, "Europe/Berlin")
	prepare := func(n int) func() {
		exdates := make([]string, n)
		base := time.Date(2026, 7, 2, 10, 0, 0, 0, time.UTC)
		for i := range exdates {
			exdates[i] = base.AddDate(0, 0, i).Format("20060102T150405")
		}
		raw := buildCalendarObject(buildVEvent("scale",
			"DTSTART;TZID=UTC:20260701T100000", "RRULE:FREQ=DAILY;COUNT=1",
			"EXDATE;TZID= Europe/Berlin :"+strings.Join(exdates, ","), "DTEND:20260701T110000Z"))
		return func() {
			if _, fault := validateCalendarObjectForStorage(raw, nil); fault != nil {
				t.Fatalf("refused: %+v", fault)
			}
		}
	}
	assertScalesLinearly(t, 2000, prepare)
}
