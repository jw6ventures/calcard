package dav

import (
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/jw6ventures/calcard/internal/auth"
	"github.com/jw6ventures/calcard/internal/ical"
	"github.com/jw6ventures/calcard/internal/store"
)

// clientReading is body as a client that reads its VTIMEZONEs but not the
// server's marker on them sees it: every definition is the one it reads its
// times by.
func clientReading(body string) string {
	lines := strings.SplitAfter(body, "\n")
	kept := lines[:0]
	for _, line := range lines {
		if !ical.HasPrefixFold(line, ical.HostDefinitionProperty+":") {
			kept = append(kept, line)
		}
	}
	return strings.Join(kept, "")
}

// wallClockWhereHostOffsetIs finds 09:00 on the first day of year on which the
// host's zone keeps offset, so a test does not depend on which year's rules a
// tzdata release states.
func wallClockWhereHostOffsetIs(t *testing.T, loc *time.Location, year int, offset time.Duration) time.Time {
	t.Helper()
	for day := time.Date(year, time.January, 1, 9, 0, 0, 0, time.UTC); day.Year() == year; day = day.AddDate(0, 0, 1) {
		if _, got := ical.WallClockIn(loc, day).Zone(); time.Duration(got)*time.Second == offset {
			return day
		}
	}
	t.Skipf("host's %s keeps offset %s on no day of %d", loc, offset, year)
	return time.Time{}
}

// readsAt resolves the DTSTART of body's VEVENT as the report matcher does.
func readsAt(t *testing.T, body string) time.Time {
	t.Helper()
	root, err := parseStoredICalendarObject(body)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	vevent, _ := namedComponent(root, "VEVENT")
	if vevent == nil {
		t.Fatalf("no VEVENT in\n%s", body)
	}
	dtstart, ok := newCalendarTimeRangeMatcher(body, root, floatingZone{}).dateValue(vevent, "DTSTART", noShift)
	if !ok {
		t.Fatalf("DTSTART does not resolve in\n%s", body)
	}
	return dtstart.instant
}

func wallClockValue(wall time.Time) string {
	return wall.Format("20060102T150405")
}

// A client that reads an event back with the definition the server stored for
// its zone, moves it to another year and writes it back, writes a definition
// the server generated for the year it left. Whatever that definition says of
// the new year, the event is the host zone's: calendar-query, free-busy and the
// stored dtstart all place it where the host does, and so does the definition
// stored with it for clients to read.
func TestHostDefinitionWrittenBackFollowsTheEventIntoAnotherYear(t *testing.T) {
	tests := []struct {
		name, tzid string
		from       time.Time
		toYear     int
		// toOffset picks the day the event moves to: one on which the host's
		// zone keeps this offset and the definition written back keeps
		// another.
		toOffset time.Duration
	}{
		// The 2025 definition lists that year's transitions alone; no
		// yearly rule states the zone's suspensions of +01.
		{name: "Casablanca to 2030", tzid: "Africa/Casablanca", from: time.Date(2025, 6, 16, 9, 0, 0, 0, time.UTC), toYear: 2030, toOffset: 0},
		// The 2025 definition's first transition is in November 2024, so a
		// January before it is read in the offset that transition leaves.
		{name: "New York to 2020", tzid: "America/New_York", from: time.Date(2025, 7, 1, 9, 0, 0, 0, time.UTC), toYear: 2020, toOffset: -5 * time.Hour},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			requireHostZones(t, tt.tzid)
			loc, _ := ical.HostLocation(tt.tzid)
			to := wallClockWhereHostOffsetIs(t, loc, tt.toYear, tt.toOffset)
			hostInstant := ical.WallClockIn(loc, to).UTC()

			h, eventRepo := writableCalendarServer()
			event := func(wall time.Time) string {
				return producerObject("VEVENT", "UID:moved", "DTSTAMP:20250101T000000Z",
					"DTSTART;TZID="+tt.tzid+":"+wallClockValue(wall), "DTEND;TZID="+tt.tzid+":"+wallClockValue(wall.Add(time.Hour)))
			}
			if rr := putCalendarObject(t, h, "moved.ics", event(tt.from)); rr.Code != http.StatusCreated {
				t.Fatalf("first PUT = %d: %s", rr.Code, rr.Body.String())
			}
			served := storedEventByUID(eventRepo, "moved").RawICAL
			written := strings.NewReplacer(wallClockValue(tt.from), wallClockValue(to),
				wallClockValue(tt.from.Add(time.Hour)), wallClockValue(to.Add(time.Hour))).Replace(served)
			if stale := readsAt(t, clientReading(written)); stale.Equal(hostInstant) {
				t.Fatalf("the definition written back already places %s where the host does; the case tests nothing", to)
			}

			if rr := putCalendarObject(t, h, "moved.ics", written); rr.Code != http.StatusNoContent && rr.Code != http.StatusCreated {
				t.Fatalf("second PUT = %d: %s", rr.Code, rr.Body.String())
			}
			stored := storedEventByUID(eventRepo, "moved").RawICAL
			if got := readsAt(t, stored); !got.Equal(hostInstant) {
				t.Errorf("matcher places the event at %s, want the host's %s", got, hostInstant)
			}
			if got := readsAt(t, clientReading(stored)); !got.Equal(hostInstant) {
				t.Errorf("the stored definition places the event at %s for a client, want the host's %s:\n%s", got, hostInstant, stored)
			}

			validated, fault := validateCalendarObjectForStorage(stored, nil)
			if fault != nil {
				t.Fatalf("stored object refused: %+v", fault)
			}
			if got := validated.Analysis.Metadata.DTStart; got == nil || !got.Equal(hostInstant) {
				t.Errorf("stored dtstart = %v, want %s", got, hostInstant)
			}
			if validated.Body != stored {
				t.Errorf("a write of the stored object stores something else:\n%s\nwant\n%s", validated.Body, stored)
			}

			rangeStart, rangeEnd := hostInstant.Format("20060102T150405Z"), hostInstant.Add(time.Hour).Format("20060102T150405Z")
			code, body := reportOn(t, h, timeRangeQuery(rangeStart, rangeEnd))
			if code != http.StatusMultiStatus || !strings.Contains(body, "moved.ics") {
				t.Errorf("calendar-query [%s, %s) = %d without the event:\n%s", rangeStart, rangeEnd, code, body)
			}
			before := hostInstant.Add(-2 * time.Hour).Format("20060102T150405Z")
			if code, body := reportOn(t, h, timeRangeQuery(before, rangeStart)); code != http.StatusMultiStatus || strings.Contains(body, "moved.ics") {
				t.Errorf("calendar-query [%s, %s) = %d matching the event:\n%s", before, rangeStart, code, body)
			}
			code, body = reportOn(t, h, freeBusyQuery(hostInstant.Add(-24*time.Hour).Format("20060102T150405Z"), hostInstant.Add(24*time.Hour).Format("20060102T150405Z")))
			if code != http.StatusOK {
				t.Fatalf("free-busy = %d: %s", code, body)
			}
			assertPublishedFreeBusy(t, parsedFreeBusyLines(t, body), []string{"FREEBUSY:" + rangeStart + "/" + rangeEnd})
		})
	}
}

func reportOn(t *testing.T, h *DavServer, body string) (int, string) {
	t.Helper()
	req := httptest.NewRequest("REPORT", "/dav/calendars/1/", strings.NewReader(body))
	req.Header.Set("Depth", "1")
	rr := httptest.NewRecorder()
	h.Report(rr, req.WithContext(auth.WithUser(req.Context(), &store.User{ID: 1})))
	return rr.Code, rr.Body.String()
}

// A definition the client wrote is the one RFC 4791 §4.1 has the object carry,
// and the server reads the object's times by it and stores it as written, even
// for a name the host's database defines differently, and even where it covers
// years the host's zone has since changed.
func TestClientWrittenDefinitionIsHonored(t *testing.T) {
	requireHostZones(t, "America/New_York", "Africa/Casablanca")
	casablanca2025, ok := ical.VTimezone("Africa/Casablanca", time.Date(2025, 6, 16, 9, 0, 0, 0, time.UTC), time.Date(2025, 6, 16, 9, 0, 0, 0, time.UTC))
	if !ok {
		t.Fatal("no Casablanca definition")
	}
	tests := map[string]struct {
		timezone string
		tzid     string
		wall     time.Time
		offset   time.Duration
	}{
		"custom name": {
			timezone: fixedOffsetVTimezone("Custom/Office", "-0300"), tzid: "Custom/Office",
			wall: time.Date(2026, 1, 5, 9, 0, 0, 0, time.UTC), offset: -3 * time.Hour,
		},
		"host name defined otherwise": {
			timezone: fixedOffsetVTimezone("America/New_York", "-0300"), tzid: "America/New_York",
			wall: time.Date(2026, 1, 5, 9, 0, 0, 0, time.UTC), offset: -3 * time.Hour,
		},
		// A definition listing 2025's transitions alone keeps 2025's last
		// offset after them, and a client writing it without the server's
		// marker is taken at its word.
		"host name, unmarked listing of another year": {
			timezone: clientReading(casablanca2025), tzid: "Africa/Casablanca",
			wall: time.Date(2030, 7, 1, 9, 0, 0, 0, time.UTC), offset: time.Hour,
		},
	}
	for name, tt := range tests {
		t.Run(name, func(t *testing.T) {
			h, eventRepo := writableCalendarServer()
			raw := buildCalendarObject(tt.timezone, buildComponent("VEVENT", "UID:client", "DTSTAMP:20250101T000000Z",
				"DTSTART;TZID="+tt.tzid+":"+wallClockValue(tt.wall), "DTEND;TZID="+tt.tzid+":"+wallClockValue(tt.wall.Add(time.Hour))))
			rr := putCalendarObject(t, h, "client.ics", raw)
			if rr.Code != http.StatusCreated {
				t.Fatalf("PUT = %d: %s", rr.Code, rr.Body.String())
			}
			if rr.Header().Get("ETag") == "" {
				t.Error("PUT withheld the ETag of an object it should store as sent")
			}
			stored := storedEventByUID(eventRepo, "client").RawICAL
			if stored != raw {
				t.Fatalf("stored\n%s\nwant the object as sent", stored)
			}
			want := tt.wall.Add(-tt.offset)
			if got := readsAt(t, stored); !got.Equal(want) {
				t.Errorf("matcher places the event at %s, want the client's %s", got, want)
			}
			start, end := want.Format("20060102T150405Z"), want.Add(time.Hour).Format("20060102T150405Z")
			code, body := reportOn(t, h, freeBusyQuery(want.Add(-24*time.Hour).Format("20060102T150405Z"), want.Add(24*time.Hour).Format("20060102T150405Z")))
			if code != http.StatusOK {
				t.Fatalf("free-busy = %d: %s", code, body)
			}
			assertPublishedFreeBusy(t, parsedFreeBusyLines(t, body), []string{"FREEBUSY:" + start + "/" + end})
		})
	}
}

// The marker speaks for the host's database only about a zone the host
// defines: on any other name it is the object's only definition, and is read.
func TestHostDefinitionMarkerOnAZoneTheHostLacksIsReadAsWritten(t *testing.T) {
	timezone := strings.Replace(fixedOffsetVTimezone("Custom/Office", "-0300"),
		"TZID:Custom/Office\r\n", "TZID:Custom/Office\r\n"+ical.HostDefinitionProperty+":TRUE\r\n", 1)
	raw := buildCalendarObject(timezone, buildComponent("VEVENT", "UID:client", "DTSTAMP:20250101T000000Z",
		"DTSTART;TZID=Custom/Office:20260105T090000"))
	validated, fault := validateCalendarObjectForStorage(raw, nil)
	if fault != nil {
		t.Fatalf("refused: %+v", fault)
	}
	if validated.Body != raw {
		t.Errorf("stored\n%s\nwant the object as sent", validated.Body)
	}
	if got, want := readsAt(t, raw), time.Date(2026, 1, 5, 12, 0, 0, 0, time.UTC); !got.Equal(want) {
		t.Errorf("matcher places the event at %s, want %s", got, want)
	}
}

// A stored definition the server generated speaks for the host's database,
// so the server reads the zone from that database even where the definition
// says otherwise, as it does after a tzdata release changes the zone's rules
// for years the stored definition already states.
func TestServerReadsAHostDefinitionByTheHostsDatabase(t *testing.T) {
	requireHostZones(t, "America/New_York")
	generated := hostVTimezone(t, "America/New_York", time.Date(2026, 1, 5, 9, 0, 0, 0, time.UTC), time.Date(2026, 1, 5, 9, 0, 0, 0, time.UTC))
	outdated := strings.NewReplacer("TZOFFSETFROM:-0500", "TZOFFSETFROM:-0600", "TZOFFSETTO:-0500", "TZOFFSETTO:-0600").Replace(generated)
	if outdated == generated {
		t.Fatalf("the New York definition states no -0500 offset:\n%s", generated)
	}
	raw := buildCalendarObject(outdated, buildComponent("VEVENT", "UID:stored", "DTSTAMP:20250101T000000Z",
		"DTSTART;TZID=America/New_York:20260105T090000", "DTEND;TZID=America/New_York:20260105T100000"))
	want := time.Date(2026, 1, 5, 14, 0, 0, 0, time.UTC)
	if got := readsAt(t, clientReading(raw)); got.Equal(want) {
		t.Fatalf("the altered definition places the event where the host does; the case tests nothing")
	}
	if got := readsAt(t, raw); !got.Equal(want) {
		t.Errorf("matcher places the event at %s, want the host's %s", got, want)
	}
	tr := &timeRange{Start: "20260105T000000Z", End: "20260106T000000Z"}
	body := mustReportText((&DavServer{}).generateFreeBusy(t.Context(),
		freeBusyCandidates([]store.Event{{UID: "stored", RawICAL: raw}}, floatingZone{}), tr))
	assertPublishedFreeBusy(t, parsedFreeBusyLines(t, body), []string{"FREEBUSY:20260105T140000Z/20260105T150000Z"})

	// A write of it replaces the definition with the host's.
	validated, fault := validateCalendarObjectForStorage(raw, nil)
	if fault != nil {
		t.Fatalf("refused: %+v", fault)
	}
	if strings.Contains(validated.Body, "-0600") {
		t.Errorf("stored object keeps the outdated definition:\n%s", validated.Body)
	}
	if got := readsAt(t, clientReading(validated.Body)); !got.Equal(want) {
		t.Errorf("the stored definition places the event at %s for a client, want %s", got, want)
	}
}

// Exchange and scripts write UTC times as TZID=UTC. The name places a time
// where a UTC value does for every reader, so the object needs no definition
// for it: it is stored as sent, with its ETag, and read in UTC.
func TestUTCTZIDIsStoredAsSentAndReadInUTC(t *testing.T) {
	for _, tzid := range []string{"UTC", "Etc/UTC"} {
		t.Run(tzid, func(t *testing.T) {
			requireHostZones(t, tzid)
			h, eventRepo := writableCalendarServer()
			raw := producerObject("VEVENT", "UID:utc", "DTSTAMP:20250101T000000Z",
				"DTSTART;TZID="+tzid+":20260105T090000", "DTEND;TZID="+tzid+":20260105T100000")
			rr := putCalendarObject(t, h, "utc.ics", raw)
			if rr.Code != http.StatusCreated {
				t.Fatalf("PUT = %d: %s", rr.Code, rr.Body.String())
			}
			if rr.Header().Get("ETag") == "" {
				t.Error("PUT withheld the ETag of an object it should store as sent")
			}
			stored := storedEventByUID(eventRepo, "utc").RawICAL
			if stored != raw {
				t.Fatalf("stored\n%s\nwant the object as sent", stored)
			}

			want := time.Date(2026, 1, 5, 9, 0, 0, 0, time.UTC)
			if got := readsAt(t, stored); !got.Equal(want) {
				t.Errorf("matcher places the event at %s, want %s", got, want)
			}
			validated, fault := validateCalendarObjectForStorage(stored, nil)
			if fault != nil {
				t.Fatalf("stored object refused: %+v", fault)
			}
			metadata := validated.Analysis.Metadata
			if metadata.DTStart == nil || !metadata.DTStart.Equal(want) {
				t.Errorf("stored dtstart = %v, want %s", metadata.DTStart, want)
			}
			if wantEnd := want.Add(time.Hour); metadata.DTEnd == nil || !metadata.DTEnd.Equal(wantEnd) {
				t.Errorf("stored dtend = %v, want %s", metadata.DTEnd, wantEnd)
			}
			if code, body := reportOn(t, h, timeRangeQuery("20260105T093000Z", "20260105T094500Z")); code != http.StatusMultiStatus || !strings.Contains(body, "utc.ics") {
				t.Errorf("calendar-query inside the event = %d without it:\n%s", code, body)
			}
			if code, body := reportOn(t, h, timeRangeQuery("20260105T100000Z", "20260105T110000Z")); code != http.StatusMultiStatus || strings.Contains(body, "utc.ics") {
				t.Errorf("calendar-query after the event = %d matching it:\n%s", code, body)
			}
		})
	}
}

// storedWithHostDefinition is an object naming Europe/Berlin without defining
// it, as the validator stores it: with the host's definition of the zone,
// carrying the server's marker.
func storedWithHostDefinition(t *testing.T, extra ...string) string {
	t.Helper()
	requireHostZones(t, "Europe/Berlin", "America/New_York")
	properties := append([]string{"UID:marked", "DTSTAMP:20250101T000000Z", "DTSTART;TZID=Europe/Berlin:20260105T090000"}, extra...)
	validated, fault := validateCalendarObjectForStorage(producerObject("VEVENT", properties...), nil)
	if fault != nil {
		t.Fatalf("refused: %+v", fault)
	}
	if !strings.Contains(validated.Body, ical.HostDefinitionProperty) {
		t.Fatalf("no host definition stored:\n%s", validated.Body)
	}
	return validated.Body
}

func failingToGenerate(failing string) func(string, time.Time, time.Time) (string, bool) {
	return func(tzid string, first, last time.Time) (string, bool) {
		if tzid == failing {
			return "", false
		}
		return ical.VTimezone(tzid, first, last)
	}
}

// A definition the server stored for a zone is replaced only by one it can
// generate. Where none can be generated for the zone, the object keeps the one
// it carries, rather than going on naming the zone with no definition of it.
func TestHostDefinitionIsKeptWhereNoReplacementCanBeGenerated(t *testing.T) {
	t.Run("the only zone", func(t *testing.T) {
		stored := storedWithHostDefinition(t)
		root, err := parseICalendarObject(stored)
		if err != nil {
			t.Fatal(err)
		}
		if got := completeCalendarObject(stored, ical.UnfoldLines(stored), root, ical.RecurrenceUntilSentinel, time.Now(), failingToGenerate("Europe/Berlin"), maxDAVBodyBytes); got != stored {
			t.Errorf("stored\n%s\nwant the object as it was", got)
		}
	})
	t.Run("beside a zone that can be generated", func(t *testing.T) {
		stored := storedWithHostDefinition(t, "DTEND;TZID=America/New_York:20260105T040000")
		root, err := parseICalendarObject(stored)
		if err != nil {
			t.Fatal(err)
		}
		// The stored object carries both definitions; with New York's
		// removed, it has to be generated again.
		newYork := hostVTimezone(t, "America/New_York", time.Date(2026, 1, 5, 4, 0, 0, 0, time.UTC), time.Date(2026, 1, 5, 4, 0, 0, 0, time.UTC))
		withoutNewYork := strings.Replace(stored, newYork, "", 1)
		if withoutNewYork == stored {
			t.Fatalf("no New York definition stored:\n%s", stored)
		}
		root, err = parseICalendarObject(withoutNewYork)
		if err != nil {
			t.Fatal(err)
		}
		if got := completeCalendarObject(withoutNewYork, ical.UnfoldLines(withoutNewYork), root, ical.RecurrenceUntilSentinel, time.Now(), failingToGenerate("Europe/Berlin"), maxDAVBodyBytes); got != stored {
			t.Errorf("stored\n%s\nwant\n%s", got, stored)
		}
	})
}

// Definitions that would take the object past CALDAV:max-resource-size are not
// added, and the object is stored as submitted, keeping a definition the server
// stored earlier.
func TestHostDefinitionsPastTheSizeLimitLeaveTheObjectAsSubmitted(t *testing.T) {
	stored := storedWithHostDefinition(t, "DTEND;TZID=America/New_York:20260105T040000")
	newYork := hostVTimezone(t, "America/New_York", time.Date(2026, 1, 5, 4, 0, 0, 0, time.UTC), time.Date(2026, 1, 5, 4, 0, 0, 0, time.UTC))
	submitted := strings.Replace(stored, newYork, "", 1)
	if submitted == stored {
		t.Fatalf("no New York definition stored:\n%s", stored)
	}
	root, err := parseICalendarObject(submitted)
	if err != nil {
		t.Fatal(err)
	}
	if got := completeCalendarObject(submitted, ical.UnfoldLines(submitted), root, ical.RecurrenceUntilSentinel, time.Now(), ical.VTimezone, int64(len(stored)-1)); got != submitted {
		t.Errorf("stored\n%s\nwant the object as submitted", got)
	}
	if got := completeCalendarObject(submitted, ical.UnfoldLines(submitted), root, ical.RecurrenceUntilSentinel, time.Now(), ical.VTimezone, int64(len(stored))); got != stored {
		t.Errorf("within the limit, stored\n%s\nwant\n%s", got, stored)
	}

	// The same through PUT, at the server's limit: the object is stored as
	// sent, and its ETag returned.
	padding := "X-PAD:" + strings.Repeat("a", int(maxDAVBodyBytes)-len(stored)-len("X-PAD:\r\n")+len(newYork)/2)
	submitted = strings.Replace(submitted, "END:VEVENT\r\n", padding+"\r\nEND:VEVENT\r\n", 1)
	if int64(len(submitted)) > maxDAVBodyBytes || int64(len(submitted)+len(newYork)) <= maxDAVBodyBytes {
		t.Fatalf("submitted object of %d octets does not straddle the limit", len(submitted))
	}
	h, eventRepo := writableCalendarServer()
	rr := putCalendarObject(t, h, "marked.ics", submitted)
	if rr.Code != http.StatusCreated {
		t.Fatalf("PUT = %d: %s", rr.Code, rr.Body.String())
	}
	if got := storedEventByUID(eventRepo, "marked").RawICAL; got != submitted {
		t.Errorf("stored object of %d octets is not the %d submitted; definitions kept: %d", len(got), len(submitted), strings.Count(got, "BEGIN:VTIMEZONE"))
	}
	if rr.Header().Get("ETag") == "" {
		t.Error("PUT withheld the ETag of an object it stored as sent")
	}
}

// The validator refuses or accepts an object, and derives what the store
// indexes it by, before it adds the host's definitions: no reading of the
// object depends on them, since a definition the server generates speaks for
// the host's zone, which answers for a TZID the object does not define anyway.
func TestHostDefinitionsChangeNothingTheValidatorReads(t *testing.T) {
	requireHostZones(t, "Europe/Berlin", "America/New_York")
	objects := map[string]string{
		"weekly series with an exception and an override": buildCalendarObject(
			buildComponent("VEVENT", "UID:e", "DTSTAMP:20250101T000000Z", berlinEvent+"RRULE:FREQ=WEEKLY;COUNT=10",
				"EXDATE;TZID=Europe/Berlin:20260112T090000", "X-DEADLINE;TZID=America/New_York:20280105T090000"),
			buildComponent("VEVENT", "UID:e", "DTSTAMP:20250101T000000Z", "RECURRENCE-ID;TZID=Europe/Berlin:20260119T090000",
				"DTSTART;TZID=America/New_York:20260119T090000", "DTEND;TZID=America/New_York:20260119T100000")),
		"series across a daylight-saving change": producerObject("VEVENT", "UID:e", "DTSTAMP:20250101T000000Z",
			"DTSTART;TZID=Europe/Berlin:20260329T023000", "RRULE:FREQ=DAILY;UNTIL=20260405T000000Z"),
		"unbounded series": producerObject("VEVENT", "UID:e", "DTSTAMP:20250101T000000Z", berlinEvent+"RRULE:FREQ=WEEKLY"),
		"task":             producerObject("VTODO", "UID:t", "DTSTAMP:20250101T000000Z", "DUE;TZID=Europe/Berlin:20260105T090000"),
	}
	// A definition the server stored, written back with the event moved to
	// another year, is replaced by one covering the new year.
	objects["server definition written back for another year"] = strings.ReplaceAll(
		storedWithHostDefinition(t, "DTEND;TZID=Europe/Berlin:20260105T100000"), "20260105T", "20300107T")
	for name, raw := range objects {
		t.Run(name, func(t *testing.T) {
			root, err := parseICalendarObject(raw)
			if err != nil {
				t.Fatal(err)
			}
			completed := completeCalendarObject(raw, ical.UnfoldLines(raw), root, ical.RecurrenceUntilSentinel, time.Now(), ical.VTimezone, maxDAVBodyBytes)
			if completed == raw {
				t.Fatal("no definition added; the case tests nothing")
			}
			read := func(body string) (calendarAnalysis, [2]bool) {
				t.Helper()
				lines := ical.UnfoldLines(body)
				times := ical.NewObjectTimesSharingOnsets(lines, sharedTimezoneOnsets)
				analysis, err := analyzeUnfoldedICalendar(body, lines, times)
				if err != nil {
					t.Fatalf("analysis: %v", err)
				}
				exceeds, valid := ical.RecurrenceSetExceedsLimitOf(times, ical.MaxRecurrenceInstances)
				return analysis, [2]bool{exceeds, valid}
			}
			submitted, submittedLimit := read(raw)
			stored, storedLimit := read(completed)
			if submittedLimit != storedLimit {
				t.Errorf("recurrence-limit check = %v as submitted, %v as stored", submittedLimit, storedLimit)
			}
			if !reflect.DeepEqual(submitted.Components, stored.Components) || submitted.MaxAttendees != stored.MaxAttendees ||
				submitted.MaxRRULECount != stored.MaxRRULECount || submitted.HasRRULECount != stored.HasRRULECount {
				t.Errorf("components read differently:\n%+v\n%+v", submitted, stored)
			}
			if !reflect.DeepEqual(submitted.Metadata, stored.Metadata) {
				t.Errorf("metadata as submitted %+v, as stored %+v", submitted.Metadata, stored.Metadata)
			}
		})
	}
}

// A definition the server stored is recognized and replaced however a client
// writes it back: folded anywhere, or with its property names in another case
// or followed by white space, as the validator reads them.
func TestHostDefinitionIsReplacedHoweverItIsWrittenBack(t *testing.T) {
	stored := storedWithHostDefinition(t)
	writtenBack := map[string]*strings.Replacer{
		"folded": strings.NewReplacer("BEGIN:VTIMEZONE\r\n", "BEGIN:VTIME\r\n ZONE\r\n",
			ical.HostDefinitionProperty+":", "X-CALCARD-HO\r\n ST-ZONE:", "TZID:Europe/Berlin\r\n", "TZ\r\n\tID:Europe/Berl\r\n in\r\n"),
		"names in another case, followed by white space": strings.NewReplacer("TZID:Europe/Berlin\r\n", "tzid :Europe/Berlin\r\n",
			ical.HostDefinitionProperty+":", "x-calcard-host-zone :", "BEGIN:VTIMEZONE\r\n", "begin:vtimezone\r\n"),
	}
	for name, replacer := range writtenBack {
		t.Run(name, func(t *testing.T) {
			raw := replacer.Replace(stored)
			if raw == stored {
				t.Fatal("nothing rewritten; the case tests nothing")
			}
			validated, fault := validateCalendarObjectForStorage(raw, nil)
			if fault != nil {
				t.Fatalf("refused: %+v\n%s", fault, raw)
			}
			if validated.Body != stored {
				t.Errorf("stored\n%q\nwant the definition replaced by the host's\n%q", validated.Body, stored)
			}
		})
	}
}

// The VTIMEZONEs ical.TimezoneComponents finds by unfolded line are removed
// from the submitted octets by where each of those lines starts in them,
// whatever line endings and folds the object is written with.
func TestLogicalLineOffsetsLocateEachUnfoldedLine(t *testing.T) {
	for _, raw := range []string{
		"BEGIN:VCALENDAR\r\nX-A:1\r\n  2\r\nEND:VCALENDAR\r\n",
		"BEGIN:VCALENDAR\nX-A:1\n\t2\nEND:VCALENDAR",
		"BEGIN:VCALENDAR\rX-A:1\r 2\r\rEND:VCALENDAR\r",
		"BEGIN:VCALENDAR\r\n\r\n continued\r\nEND:VCALENDAR\n\r",
		"",
	} {
		lines := ical.UnfoldLines(raw)
		offsets, ok := logicalLineOffsets(raw, lines)
		if !ok || len(offsets) != len(lines)+1 || offsets[len(lines)] != len(raw) {
			t.Fatalf("%q: offsets %v, %v for %d lines", raw, offsets, ok, len(lines))
		}
		for i, line := range lines {
			if got := ical.UnfoldLines(raw[offsets[i]:offsets[i+1]]); got[0] != line {
				t.Errorf("%q: line %d starting at %d reads %q, want %q", raw, i, offsets[i], got[0], line)
			}
		}
	}
}

// A definition of a zone the host defines whose rules cannot be followed as
// far as the object's times -- here a rule whose onsets are too sparse for
// any search to reach -- is read past by every reader: the matcher and the
// stored columns read those times through the host's zone instead, and so does
// the validator, which accepts the object from a client and from the store
// alike rather than refusing what it reads the way every reader does.
func TestUnfollowableDefinitionOfAHostZoneIsReadThroughTheHostZone(t *testing.T) {
	requireHostZones(t, "Europe/Berlin")
	timezone := buildComponent("VTIMEZONE", "TZID:Europe/Berlin",
		"BEGIN:STANDARD", "DTSTART:16010101T000000", "TZOFFSETFROM:+0200", "TZOFFSETTO:+0100",
		"RRULE:FREQ=MINUTELY;INTERVAL=7;BYMONTHDAY=29;BYMONTH=2", "END:STANDARD")
	raw := buildCalendarObject(timezone, buildComponent("VEVENT", "UID:unfollowable", "DTSTAMP:20250101T000000Z",
		"DTSTART;TZID=Europe/Berlin:20260105T090000", "DTEND;TZID=Europe/Berlin:20260105T100000",
		"RRULE:FREQ=DAILY;UNTIL=20260110T080000Z", "EXDATE;TZID=Europe/Berlin:20260107T090000"))
	root, err := parseICalendarObject(raw)
	if err != nil {
		t.Fatal(err)
	}
	observances, readable := submittedTimezoneObservances(root, "Europe/Berlin")
	if _, followable := observanceOffsetAtWall(observances, time.Date(2026, 1, 5, 9, 0, 0, 0, time.UTC)); !readable || followable {
		t.Fatalf("definition readable %v, followable %v; want one that is read but cannot be followed", readable, followable)
	}
	want := time.Date(2026, 1, 5, 8, 0, 0, 0, time.UTC)

	if got := readsAt(t, raw); !got.Equal(want) {
		t.Errorf("matcher places the event at %s, want the host's %s", got, want)
	}
	for name, validate := range map[string]func(string, *store.CalendarAccess) (*validatedCalendarObject, *calendarObjectFault){
		"submitted": validateCalendarObjectForStorage, "transferred": validateTransferredCalendarObject,
	} {
		validated, fault := validate(raw, nil)
		if fault != nil {
			t.Fatalf("%s object refused: %+v", name, fault)
		}
		if validated.Body != raw {
			t.Errorf("%s object stored as\n%s", name, validated.Body)
		}
		if got := validated.Analysis.Metadata.DTStart; got == nil || !got.Equal(want) {
			t.Errorf("%s object's dtstart = %v, want %s", name, got, want)
		}
	}

	h, _ := writableCalendarServer()
	if rr := putCalendarObject(t, h, "unfollowable.ics", raw); rr.Code != http.StatusCreated {
		t.Fatalf("PUT = %d: %s", rr.Code, rr.Body.String())
	}
	for _, tt := range []struct {
		start, end string
		want       bool
	}{
		{"20260105T083000Z", "20260105T084500Z", true},
		{"20260107T083000Z", "20260107T084500Z", false},
		{"20260108T083000Z", "20260108T084500Z", true},
	} {
		code, body := reportOn(t, h, timeRangeQuery(tt.start, tt.end))
		if got := strings.Contains(body, "unfollowable.ics"); code != http.StatusMultiStatus || got != tt.want {
			t.Errorf("calendar-query [%s, %s) = %d matching %v, want %v", tt.start, tt.end, code, got, tt.want)
		}
	}
}

// The host's definition stored for a zone covers the times the object places
// in it: a counted series through its last instance, a series that never ends
// through CALDAV:max-date-time, and a date the series adds past its count
// through that date.
func TestHostDefinitionCoversTheSeriesItDefines(t *testing.T) {
	requireHostZones(t, "Africa/Casablanca")
	start := time.Date(2025, 6, 16, 9, 0, 0, 0, time.UTC)
	_, maxDate := ical.DateLimits()
	tests := map[string]struct {
		lines       []string
		first, last time.Time
	}{
		"two days":        {[]string{"RRULE:FREQ=DAILY;COUNT=2"}, start, start.AddDate(0, 0, 1)},
		"never ending":    {[]string{"RRULE:FREQ=DAILY"}, start, maxDate},
		"a date past it":  {[]string{"RRULE:FREQ=DAILY;COUNT=2", "RDATE;TZID=Africa/Casablanca:20300101T090000"}, start, time.Date(2030, 1, 1, 9, 0, 0, 0, time.UTC)},
		"until in a year": {[]string{"RRULE:FREQ=DAILY;UNTIL=20270101T000000Z"}, start, time.Date(2027, 1, 1, 0, 0, 0, 0, time.UTC)},
	}
	for name, tt := range tests {
		t.Run(name, func(t *testing.T) {
			raw := producerObject("VEVENT", append([]string{"UID:e", "DTSTAMP:20250101T000000Z",
				"DTSTART;TZID=Africa/Casablanca:20250616T090000", "DTEND;TZID=Africa/Casablanca:20250616T100000"}, tt.lines...)...)
			validated, fault := validateCalendarObjectForStorage(raw, nil)
			if fault != nil {
				t.Fatalf("refused: %+v", fault)
			}
			want := hostVTimezone(t, "Africa/Casablanca", tt.first, tt.last)
			if !strings.Contains(validated.Body, want) {
				t.Errorf("stored\n%s\nwant the definition through %s:\n%s", validated.Body, tt.last.Format("2006"), want)
			}
		})
	}
}
