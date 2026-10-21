package dav

import (
	"strings"
	"testing"
	"time"

	"github.com/jw6ventures/calcard/internal/store"
)

// unreadableDTEndEvent is a row shaped like one a 1.1.x release stored: its
// RawICAL carries a DATE-TIME that the current, stricter reader refuses as a
// DTEND -- RFC 5545 §3.3.5 names only the compact "20060102T150405Z" form, and
// this line writes the extended form with dashes and colons instead -- while
// its denormalized dtstart and dtend columns keep what that release's own
// reading of the same line resolved it to. The rest of the row is an ordinary
// VEVENT, optionally recurring.
func unreadableDTEndEvent(uid, dtstart string, dtstartColumn, dtendColumn time.Time, extra ...string) store.Event {
	lines := []string{
		"BEGIN:VCALENDAR", "VERSION:2.0", "PRODID:-//CalCard//EN",
		"BEGIN:VEVENT", "UID:" + uid, "DTSTAMP:20250101T080000Z",
		"DTSTART:" + dtstart, "DTEND:2025-03-01T12:00:00Z",
	}
	lines = append(lines, extra...)
	lines = append(lines, "END:VEVENT", "END:VCALENDAR", "")
	return store.Event{
		UID: uid, ResourceName: uid, RawICAL: strings.Join(lines, "\r\n"),
		DTStart: &dtstartColumn, DTEnd: &dtendColumn,
	}
}

// RFC 5545 §3.6.1 ends a DATE-TIME event carrying no DTEND where it starts,
// but a DTEND the component does carry, and that only this stricter reader
// refuses, is not that absence. Dropping to zero length anyway would publish
// a 1.1.x event's busy time as free; the stored dtend column, kept across
// upgrade for exactly such a row, answers instead.
func TestFreeBusyRestoresStoredDTEndForAnUnreadableDTEnd(t *testing.T) {
	start := time.Date(2025, 3, 1, 9, 0, 0, 0, time.UTC)
	end := time.Date(2025, 3, 1, 12, 0, 0, 0, time.UTC)
	tr := &timeRange{Start: "20250301T000000Z", End: "20250302T000000Z"}

	t.Run("standalone event", func(t *testing.T) {
		event := unreadableDTEndEvent("standalone", "20250301T090000Z", start, end)
		got := publishedFreeBusyOver(t, tr, event)
		want := []string{"FREEBUSY:20250301T090000Z/20250301T120000Z"}
		assertPublishedFreeBusy(t, got, want)
	})

	t.Run("no DTEnd column either publishes nothing", func(t *testing.T) {
		event := unreadableDTEndEvent("standalone-no-column", "20250301T090000Z", start, start)
		got := publishedFreeBusyOver(t, tr, event)
		assertPublishedFreeBusy(t, got, nil)
	})

	t.Run("recurring master", func(t *testing.T) {
		longRange := &timeRange{Start: "20250301T000000Z", End: "20250304T000000Z"}
		event := unreadableDTEndEvent("recurring", "20250301T090000Z", start, end, "RRULE:FREQ=DAILY;COUNT=3")
		got := publishedFreeBusyOver(t, longRange, event)
		want := []string{
			"FREEBUSY:20250301T090000Z/20250301T120000Z",
			"FREEBUSY:20250302T090000Z/20250302T120000Z",
			"FREEBUSY:20250303T090000Z/20250303T120000Z",
		}
		assertPublishedFreeBusy(t, got, want)
	})

	t.Run("an explicit DURATION is honored over the column", func(t *testing.T) {
		event := unreadableDTEndEvent("duration", "20250301T090000Z", start, end, "DURATION:PT1H")
		got := publishedFreeBusyOver(t, tr, event)
		want := []string{"FREEBUSY:20250301T090000Z/20250301T100000Z"}
		assertPublishedFreeBusy(t, got, want)
	})
}

// The columns describe one component of the resource -- the first, whose
// DTSTART they hold -- so the fallback applies to a component starting at the
// column's own dtstart and to no other: an override of the same resource with
// its own unreadable DTEND would otherwise be stretched to the first one's
// end.
func TestFreeBusyStoredDTEndFallbackAppliesOnlyToTheComponentTheColumnsDescribe(t *testing.T) {
	start := time.Date(2025, 3, 1, 9, 0, 0, 0, time.UTC)
	end := time.Date(2025, 3, 1, 12, 0, 0, 0, time.UTC)
	override := func(recurrenceID, dtstart, dtend string) []string {
		return []string{
			"BEGIN:VEVENT", "UID:overrides", "DTSTAMP:20250101T080000Z",
			"RECURRENCE-ID:" + recurrenceID, "DTSTART:" + dtstart, "DTEND:" + dtend, "END:VEVENT",
		}
	}
	lines := []string{"BEGIN:VCALENDAR", "VERSION:2.0", "PRODID:-//CalCard//EN"}
	lines = append(lines, override("20250301T090000Z", "20250301T090000Z", "2025-03-01T12:00:00Z")...)
	lines = append(lines, override("20250201T090000Z", "20250201T090000Z", "2025-02-01T10:00:00Z")...)
	lines = append(lines, "END:VCALENDAR", "")
	event := store.Event{
		UID: "overrides", ResourceName: "overrides", RawICAL: strings.Join(lines, "\r\n"),
		DTStart: &start, DTEnd: &end,
	}
	tr := &timeRange{Start: "20250101T000000Z", End: "20250401T000000Z"}
	got := publishedFreeBusyOver(t, tr, event)
	want := []string{"FREEBUSY:20250301T090000Z/20250301T120000Z"}
	assertPublishedFreeBusy(t, got, want)
}

// The columns hold a floating DTSTART as the UTC time with the same fields,
// while free-busy reads it in the collection's zone (RFC 4791 §7.3), and a
// zoned one as the instant it names. The fallback takes the columns' length
// for the component they describe -- the primary VEVENT, chosen as the store
// chooses it -- and places it from the instant this reader resolves for that
// component, so neither reading of the start has to equal the column's.
func TestFreeBusyStoredDTEndFallbackFollowsTheResolvedStart(t *testing.T) {
	berlin, err := time.LoadLocation("Europe/Berlin")
	if err != nil {
		t.Skipf("tzdata unavailable: %v", err)
	}
	zone := floatingZone{defined: true, loc: berlin}
	published := func(tr *timeRange, events ...store.Event) []string {
		t.Helper()
		body := mustReportText((&DavServer{}).generateFreeBusy(t.Context(), freeBusyCandidates(events, zone, nil), tr))
		return parsedFreeBusyLines(t, body)
	}
	tr := &timeRange{Start: "20250301T000000Z", End: "20250302T000000Z"}
	columnStart := time.Date(2025, 3, 1, 9, 0, 0, 0, time.UTC)
	columnEnd := columnStart.Add(time.Hour)

	t.Run("floating in a calendar with a time zone", func(t *testing.T) {
		event := unreadableDTEndEvent("floating", "20250301T090000", columnStart, columnEnd)
		assertPublishedFreeBusy(t, published(tr, event), []string{"FREEBUSY:20250301T080000Z/20250301T090000Z"})
	})

	t.Run("floating recurring master", func(t *testing.T) {
		event := unreadableDTEndEvent("floating-series", "20250301T090000", columnStart, columnEnd, "RRULE:FREQ=DAILY;COUNT=2")
		longRange := &timeRange{Start: "20250301T000000Z", End: "20250303T000000Z"}
		assertPublishedFreeBusy(t, published(longRange, event), []string{
			"FREEBUSY:20250301T080000Z/20250301T090000Z",
			"FREEBUSY:20250302T080000Z/20250302T090000Z",
		})
	})

	t.Run("zoned", func(t *testing.T) {
		event := unreadableDTEndEvent("zoned", "20250301T090000", columnStart.Add(-time.Hour), columnEnd)
		event.RawICAL = strings.Replace(event.RawICAL, "DTSTART:20250301T090000", "DTSTART;TZID=Europe/Berlin:20250301T090000", 1)
		assertPublishedFreeBusy(t, published(tr, event), []string{"FREEBUSY:20250301T080000Z/20250301T100000Z"})
	})

	t.Run("an override of a floating resource without a master takes the length of the first only", func(t *testing.T) {
		override := func(recurrenceID, dtstart, dtend string) []string {
			return []string{
				"BEGIN:VEVENT", "UID:overrides", "DTSTAMP:20250101T080000Z",
				"RECURRENCE-ID:" + recurrenceID, "DTSTART:" + dtstart, "DTEND:" + dtend, "END:VEVENT",
			}
		}
		lines := []string{"BEGIN:VCALENDAR", "VERSION:2.0", "PRODID:-//CalCard//EN"}
		lines = append(lines, override("20250301T090000", "20250301T090000", "2025-03-01T10:00:00Z")...)
		lines = append(lines, override("20250201T090000", "20250201T090000", "2025-02-01T10:00:00Z")...)
		lines = append(lines, "END:VCALENDAR", "")
		event := store.Event{UID: "overrides", ResourceName: "overrides", RawICAL: strings.Join(lines, "\r\n"), DTStart: &columnStart, DTEnd: &columnEnd}
		wide := &timeRange{Start: "20250101T000000Z", End: "20250401T000000Z"}
		assertPublishedFreeBusy(t, published(wide, event), []string{"FREEBUSY:20250301T080000Z/20250301T090000Z"})
	})
}
