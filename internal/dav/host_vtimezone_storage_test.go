package dav

import (
	"fmt"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/jw6ventures/calcard/internal/ical"
	"github.com/jw6ventures/calcard/internal/store"
)

func storedEventByUID(repo *fakeEventRepo, uid string) *store.Event {
	for _, event := range repo.events {
		if event.UID == uid {
			return event
		}
	}
	return nil
}

func requireHostZones(t *testing.T, tzids ...string) {
	t.Helper()
	for _, tzid := range tzids {
		if _, err := time.LoadLocation(tzid); err != nil {
			t.Skipf("tzdata unavailable: %v", err)
		}
	}
}

// hostVTimezone is the definition the validator stores for a zone an object
// names without defining, for an object whose values span the given years.
func hostVTimezone(t *testing.T, tzid string, first, last time.Time) string {
	t.Helper()
	text, ok := ical.VTimezone(tzid, first, last)
	if !ok {
		t.Fatalf("ical.VTimezone(%s) ok = false", tzid)
	}
	return text
}

// RFC 4791 §4.1 has a calendar object resource carry a VTIMEZONE for each TZID
// it uses. An object naming a zone only the host defines is stored with the
// host's definition of it, ahead of its first component, so every client
// reading it back reads its times as the server does.
func TestValidatorStoresTheHostDefinitionOfAZoneTheObjectNames(t *testing.T) {
	requireHostZones(t, "Europe/Berlin", "America/New_York")
	raw := producerObject("VEVENT", "UID:e", "DTSTAMP:20261002T120000Z", berlinEvent+"RRULE:FREQ=WEEKLY;COUNT=4",
		"EXDATE;TZID=Europe/Berlin:20260112T090000", "X-DEADLINE;TZID=\"America/New_York\":20280105T090000")
	validated, fault := validateCalendarObjectForStorage(raw, nil)
	if fault != nil {
		t.Fatalf("refused: %+v", fault)
	}
	_, maxDate := ical.DateLimits()
	componentsAt := strings.Index(raw, "BEGIN:VEVENT")
	want := raw[:componentsAt] +
		hostVTimezone(t, "America/New_York", time.Date(2028, 1, 5, 9, 0, 0, 0, time.UTC), time.Date(2028, 1, 5, 9, 0, 0, 0, time.UTC)) +
		hostVTimezone(t, "Europe/Berlin", time.Date(2026, 1, 5, 9, 0, 0, 0, time.UTC), maxDate) +
		raw[componentsAt:]
	if validated.Body != want {
		t.Fatalf("body to store =\n%s\nwant\n%s", validated.Body, want)
	}

	// What is stored is itself a complete object, which a write of it back
	// stores unchanged.
	root, err := parseICalendarObject(validated.Body)
	if err != nil {
		t.Fatalf("stored body does not parse: %v", err)
	}
	if fault := validateCalendarObject(root); fault != nil {
		t.Fatalf("stored body is refused: %+v", fault)
	}
	again, fault := validateCalendarObjectForStorage(validated.Body, nil)
	if fault != nil || again.Body != validated.Body {
		t.Fatalf("a write of the stored body stores something else: %+v", fault)
	}
}

// A zone whose offset does not change through an object's values is stated by
// the offset it keeps there, even where a change of name alone began the
// period in force, as one did for Warsaw in 1880 and for Buenos Aires in 2000.
// The stored object is complete, so a client can write back what it reads and
// the object can be copied or moved.
func TestValidatorStoresAZoneWhoseNameAloneChangedAsAnObjectThatValidatesAgain(t *testing.T) {
	for _, tt := range []struct{ tzid, dtstart string }{
		{"Europe/Warsaw", "19050601T100000"},
		{"America/Argentina/Buenos_Aires", "20000601T100000"},
	} {
		t.Run(tt.tzid, func(t *testing.T) {
			requireHostZones(t, tt.tzid)
			raw := producerObject("VEVENT", "UID:e", "DTSTAMP:20261002T120000Z", "DTSTART;TZID="+tt.tzid+":"+tt.dtstart, "DURATION:PT1H")
			validated, fault := validateCalendarObjectForStorage(raw, nil)
			if fault != nil {
				t.Fatalf("refused: %+v", fault)
			}
			if !strings.Contains(validated.Body, "TZOFFSETTO:") {
				t.Fatalf("stored a definition with no observance:\n%s", validated.Body)
			}
			again, fault := validateCalendarObjectForStorage(validated.Body, nil)
			if fault != nil || again.Body != validated.Body {
				t.Fatalf("a write of the stored body stores something else: %+v", fault)
			}
			if _, fault := validateTransferredCalendarObject(validated.Body, nil, nil); fault != nil {
				t.Fatalf("the stored body cannot be copied or moved: %+v", fault)
			}
		})
	}
}

// The inserted lines end the way the object's own do, so an object written
// with bare line feeds is not stored with a mix of the two.
func TestValidatorInsertsTheHostDefinitionWithTheObjectsLineEndings(t *testing.T) {
	requireHostZones(t, "Europe/Berlin")
	for name, newline := range map[string]string{"line feeds": "\n", "carriage returns": "\r"} {
		t.Run(name, func(t *testing.T) {
			raw := strings.ReplaceAll(producerObject("VEVENT", "UID:e", berlinEvent), "\r\n", newline)
			validated, fault := validateCalendarObjectForStorage(raw, nil)
			if fault != nil {
				t.Fatalf("refused: %+v", fault)
			}
			if strings.Contains(validated.Body, "\r\n") {
				t.Errorf("body to store mixes line endings:\n%q", validated.Body)
			}
			if !strings.Contains(validated.Body, newline+"BEGIN:VTIMEZONE"+newline+"TZID:Europe/Berlin"+newline) {
				t.Errorf("body to store lacks the Berlin definition:\n%q", validated.Body)
			}
		})
	}
}

// A component's BEGIN line may itself be folded; the definition still goes
// before the component rather than inside it.
func TestValidatorInsertsTheHostDefinitionBeforeAFoldedComponentLine(t *testing.T) {
	requireHostZones(t, "Europe/Berlin")
	raw := strings.Replace(producerObject("VEVENT", "UID:e", berlinEvent), "BEGIN:VEVENT", "BEG\r\n IN:VEVENT", 1)
	validated, fault := validateCalendarObjectForStorage(raw, nil)
	if fault != nil {
		t.Fatalf("refused: %+v", fault)
	}
	if !strings.Contains(validated.Body, "END:VTIMEZONE\r\nBEG\r\n IN:VEVENT\r\n") {
		t.Errorf("definition is not ahead of the folded component line:\n%s", validated.Body)
	}
}

// Every write of calendar data is validated, and a line may be folded after
// every octet. Completing an object with the definitions of its zones reads its
// lines unfolded, which must cost in proportion to the body rather than to the
// square of the folds in one line, or a body within CALDAV:max-resource-size
// holds the server for minutes.
func TestValidatorScalesLinearlyWithTheFoldsInALine(t *testing.T) {
	requireHostZones(t, "Europe/Berlin")
	raw := foldedDescriptionObject(16 << 10)
	validated, fault := validateCalendarObjectForStorage(raw, nil)
	if fault != nil {
		t.Fatalf("refused: %+v", fault)
	}
	componentsAt := strings.Index(raw, "BEGIN:VEVENT")
	want := raw[:componentsAt] +
		hostVTimezone(t, "Europe/Berlin", time.Date(2026, 1, 5, 9, 0, 0, 0, time.UTC), time.Date(2026, 1, 5, 10, 0, 0, 0, time.UTC)) +
		raw[componentsAt:]
	if validated.Body != want {
		t.Fatal("body to store is not the submitted object with the Berlin definition ahead of its event")
	}

	assertScalesLinearly(t, 160<<10, func(size int) func() {
		raw := foldedDescriptionObject(size)
		return func() {
			if _, fault := validateCalendarObjectForStorage(raw, nil); fault != nil {
				t.Fatalf("refused: %+v", fault)
			}
		}
	})
}

// manyDefinedZonesObject is a daily series with zones overridden instances,
// each starting in a zone of its own that the object defines.
func manyDefinedZonesObject(zones int) string {
	components := make([]string, 0, 2*zones+1)
	for i := range zones {
		components = append(components, fixedOffsetVTimezone(fmt.Sprintf("Private/Zone%d", i), "+0100"))
	}
	components = append(components, buildComponent("VEVENT", "UID:zones", "DTSTAMP:20260101T000000Z",
		"DTSTART;TZID=Private/Zone0:20260101T090000", "RRULE:FREQ=DAILY;COUNT=900"))
	first := time.Date(2026, 1, 1, 9, 0, 0, 0, time.UTC)
	for i := range zones {
		wall := first.AddDate(0, 0, i).Format("20060102T150405")
		components = append(components, buildComponent("VEVENT", "UID:zones", "DTSTAMP:20260101T000000Z",
			"RECURRENCE-ID;TZID=Private/Zone0:"+wall, fmt.Sprintf("DTSTART;TZID=Private/Zone%d:%s", i, wall)))
	}
	return buildCalendarObject(components...)
}

// Each zone an object names is read through the definition the object ships
// for it. Finding that definition must not cost a walk over the whole object
// per zone, or an object defining and naming many zones costs the product.
func TestValidatorScalesLinearlyWithTheZonesAnObjectDefines(t *testing.T) {
	validated, fault := validateCalendarObjectForStorage(manyDefinedZonesObject(3), nil)
	if fault != nil {
		t.Fatalf("refused: %+v", fault)
	}
	if got, want := validated.Analysis.Metadata.DTStart, time.Date(2026, 1, 1, 8, 0, 0, 0, time.UTC); got == nil || !got.Equal(want) {
		t.Fatalf("stored dtstart = %v, want %s, read through the object's +0100 definition", got, want)
	}

	assertScalesLinearly(t, 250, func(zones int) func() {
		raw := manyDefinedZonesObject(zones)
		return func() {
			if _, fault := validateCalendarObjectForStorage(raw, nil); fault != nil {
				t.Fatalf("refused: %+v", fault)
			}
		}
	})
}

// longHistoryZoneObject defines one zone by n observances, each stating a
// single onset as a full-history definition does, and excludes n instances of
// a daily series in it.
func longHistoryZoneObject(n int) string {
	observances := make([]string, 0, n)
	for i := range n {
		onset, from, to := "0101", "+0200", "+0100"
		if i%2 == 1 {
			onset, from, to = "0701", "+0100", "+0200"
		}
		observances = append(observances, buildComponent("STANDARD", fmt.Sprintf("DTSTART:%04d%sT000000", 1000+i/2, onset),
			"TZOFFSETFROM:"+from, "TZOFFSETTO:"+to))
	}
	lines := []string{"UID:history", "DTSTAMP:20260101T000000Z", "DTSTART;TZID=Private/History:20260101T090000",
		"RRULE:FREQ=DAILY;COUNT=10"}
	first := time.Date(2026, 1, 1, 9, 0, 0, 0, time.UTC)
	for i := 1; i <= n; i++ {
		lines = append(lines, "EXDATE;TZID=Private/History:"+first.AddDate(0, 0, i).Format("20060102T150405"))
	}
	return buildCalendarObject(buildComponent("VTIMEZONE", append([]string{"TZID:Private/History"}, observances...)...),
		buildComponent("VEVENT", lines...))
}

// Every value an object places in a zone it defines is read through that
// definition. Reading the definition again for each value, or reading each of
// its observances for each, costs the product of the two, which a definition
// listing a zone's whole history makes large.
func TestValidatorScalesLinearlyWithTheObservancesAndValuesOfAZone(t *testing.T) {
	validated, fault := validateCalendarObjectForStorage(longHistoryZoneObject(4), nil)
	if fault != nil {
		t.Fatalf("refused: %+v", fault)
	}
	if got, want := validated.Analysis.Metadata.DTStart, time.Date(2026, 1, 1, 7, 0, 0, 0, time.UTC); got == nil || !got.Equal(want) {
		t.Fatalf("stored dtstart = %v, want %s, read through the last observance's +0200", got, want)
	}

	assertScalesLinearly(t, 500, func(n int) func() {
		raw := longHistoryZoneObject(n)
		return func() {
			if _, fault := validateCalendarObjectForStorage(raw, nil); fault != nil {
				t.Fatalf("refused: %+v", fault)
			}
		}
	})
}

// zoneAfterOverridesObject cancels n instances of a daily series, or moments
// that name none, and defines the series' zone only after them.
func zoneAfterOverridesObject(n int) string {
	components := []string{buildComponent("VEVENT", "UID:late-zone", "DTSTAMP:20260101T000000Z",
		"DTSTART;TZID=Private/Late:20260101T090000", "RRULE:FREQ=DAILY;COUNT=900")}
	first := time.Date(2026, 1, 1, 9, 0, 0, 0, time.UTC)
	for i := 1; i <= n; i++ {
		wall := first.AddDate(0, 0, i%900).Add(time.Duration(i/900) * time.Minute).Format("20060102T150405")
		components = append(components, buildComponent("VEVENT", "UID:late-zone", "DTSTAMP:20260101T000000Z",
			"RECURRENCE-ID;TZID=Private/Late:"+wall, "DTSTART;TZID=Private/Late:"+wall, "DTEND;TZID=Private/Late:"+wall,
			"EXDATE;TZID=Private/Late:"+wall, "STATUS:CANCELLED"))
	}
	return buildCalendarObject(append(components, fixedOffsetVTimezone("Private/Late", "+0100"))...)
}

// Where an object's VTIMEZONE comes is up to the client. Finding it must not
// cost a walk over the object's components for each value placed in it.
func TestValidatorScalesLinearlyWithTheComponentsBeforeAZone(t *testing.T) {
	validated, fault := validateCalendarObjectForStorage(zoneAfterOverridesObject(3), nil)
	if fault != nil {
		t.Fatalf("refused: %+v", fault)
	}
	if got, want := validated.Analysis.Metadata.DTStart, time.Date(2026, 1, 1, 8, 0, 0, 0, time.UTC); got == nil || !got.Equal(want) {
		t.Fatalf("stored dtstart = %v, want %s, read through the object's +0100 definition", got, want)
	}

	assertScalesLinearly(t, 2000, func(n int) func() {
		raw := zoneAfterOverridesObject(n)
		return func() {
			if _, fault := validateCalendarObjectForStorage(raw, nil); fault != nil {
				t.Fatalf("refused: %+v", fault)
			}
		}
	})
}

// An object that defines every zone it names is stored byte for byte, and so
// is one naming no zone at all.
func TestValidatorStoresAnObjectDefiningItsZonesUnchanged(t *testing.T) {
	requireHostZones(t, "America/New_York")
	for name, raw := range map[string]string{
		"Google Calendar export": googleExport,
		"Outlook export":         outlookExport,
		"defined IANA name": buildCalendarObject(buildVTimezone("America/New_York"),
			buildComponent("VEVENT", "UID:e", "DTSTAMP:20250101T000000Z", "DTSTART;TZID=America/New_York:20260105T090000")),
		"UTC only": producerObject("VEVENT", "UID:e", "DTSTAMP:20250101T000000Z", "DTSTART:20260105T090000Z"),
		"floating": producerObject("VEVENT", "UID:e", "DTSTAMP:20250101T000000Z", "DTSTART:20260105T090000"),
	} {
		t.Run(name, func(t *testing.T) {
			validated, fault := validateCalendarObjectForStorage(raw, nil)
			if fault != nil {
				t.Fatalf("refused: %+v", fault)
			}
			if validated.Body != raw {
				t.Errorf("body to store =\n%s\nwant the submitted object", validated.Body)
			}
		})
	}
}

// A definition is needed through the last value the object places, and, for a
// series with no end, through CALDAV:max-date-time. A zone whose transitions no
// yearly rule states shows the difference: its listing stops where the object
// stops needing it.
func TestValidatorDefinesTheZoneThroughTheValuesItPlaces(t *testing.T) {
	requireHostZones(t, "Asia/Gaza")
	gazaEvent := "DTSTART;TZID=Asia/Gaza:20450105T090000\r\nDTEND;TZID=Asia/Gaza:20450105T100000"
	for name, tt := range map[string]struct {
		lines       []string
		wantThrough string
	}{
		"single event":       {[]string{gazaEvent}, ""},
		"series with no end": {[]string{gazaEvent, "RRULE:FREQ=WEEKLY"}, "RDATE:2080"},
		"counted series":     {[]string{gazaEvent, "RRULE:FREQ=YEARLY;COUNT=40"}, "RDATE:2080"},
		"series with UNTIL":  {[]string{gazaEvent, "RRULE:FREQ=YEARLY;UNTIL=20810105T080000Z"}, "RDATE:2080"},
	} {
		t.Run(name, func(t *testing.T) {
			validated, fault := validateCalendarObjectForStorage(producerObject("VEVENT", append([]string{"UID:e"}, tt.lines...)...), nil)
			if fault != nil {
				t.Fatalf("refused: %+v", fault)
			}
			if !strings.Contains(validated.Body, "RDATE:2045") {
				t.Fatalf("definition lacks the 2045 transitions:\n%s", validated.Body)
			}
			if tt.wantThrough == "" {
				if strings.Contains(validated.Body, "RDATE:2047") {
					t.Errorf("definition of a 2045 event lists 2047 transitions:\n%s", validated.Body)
				}
				return
			}
			if !strings.Contains(validated.Body, tt.wantThrough) {
				t.Errorf("definition lacks %s:\n%s", tt.wantThrough, validated.Body)
			}
		})
	}
}

// A stored object may not exceed CALDAV:max-resource-size, or a client could
// not write back what it read. An object the definition would take past the
// limit is stored as submitted.
func TestValidatorDoesNotDefineAZonePastTheMaximumResourceSize(t *testing.T) {
	requireHostZones(t, "Europe/Berlin")
	object := producerObject("VEVENT", "UID:e", "DTSTAMP:20250101T000000Z", berlinEvent+"DESCRIPTION:")
	padding := strings.Repeat("x", int(maxDAVBodyBytes)-len(object)-100)
	raw := strings.Replace(object, "DESCRIPTION:", "DESCRIPTION:"+padding, 1)
	validated, fault := validateCalendarObjectForStorage(raw, nil)
	if fault != nil {
		t.Fatalf("refused: %+v", fault)
	}
	if validated.Body != raw {
		t.Errorf("body to store is %d octets, want the %d submitted", len(validated.Body), len(raw))
	}
}

// RFC 4791 §5.3.4: PUT returns a strong ETag only for octets it stored as
// sent. An object completed with the host's definition of its zone is answered
// without one, while an object stored unchanged is answered with its ETag.
func TestPutWithholdsTheETagOfAnObjectCompletedWithAZoneDefinition(t *testing.T) {
	requireHostZones(t, "Europe/Berlin")
	h, eventRepo := writableCalendarServer()

	raw := producerObject("VEVENT", "UID:completed", berlinEvent)
	rr := putCalendarObject(t, h, "completed.ics", raw)
	if rr.Code != http.StatusCreated {
		t.Fatalf("PUT = %d, want 201; body: %s", rr.Code, rr.Body.String())
	}
	if etag := rr.Header().Get("ETag"); etag != "" {
		t.Errorf("PUT returned ETag %s for octets it did not store as sent", etag)
	}
	stored := storedEventByUID(eventRepo, "completed")
	if stored == nil {
		t.Fatalf("nothing stored under the object's UID: %v", eventRepo.events)
	}
	if !strings.Contains(stored.RawICAL, "BEGIN:VTIMEZONE\r\nTZID:Europe/Berlin\r\n") {
		t.Errorf("stored object lacks the Berlin definition:\n%s", stored.RawICAL)
	}

	complete := buildCalendarObject(buildVTimezone("Europe/Berlin"),
		buildComponent("VEVENT", "UID:complete", "DTSTAMP:20250101T000000Z", "DTSTART;TZID=Europe/Berlin:20260105T090000"))
	rr = putCalendarObject(t, h, "complete.ics", complete)
	if rr.Code != http.StatusCreated {
		t.Fatalf("PUT = %d, want 201; body: %s", rr.Code, rr.Body.String())
	}
	if rr.Header().Get("ETag") == "" {
		t.Error("PUT withheld the ETag of an object it stored as sent")
	}
	if stored := storedEventByUID(eventRepo, "complete"); stored == nil || stored.RawICAL != complete {
		t.Errorf("object defining its zone was not stored as sent")
	}
}
