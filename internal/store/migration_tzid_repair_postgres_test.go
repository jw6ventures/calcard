package store_test

import (
	"crypto/sha256"
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/lib/pq"

	"github.com/jw6ventures/calcard/internal/calobject"
	"github.com/jw6ventures/calcard/internal/dav"
	"github.com/jw6ventures/calcard/internal/store"
)

// tzidRepairCase is one stored event the TZID repair reads: the body a
// v1.1.x release stored, the body the repair leaves, and how many of its
// content lines the repair reports as left untouched.
type tzidRepairCase struct {
	uid    string
	before string
	after  string
	// reported is the number of content lines the repair leaves that the
	// rewrite applies to.
	reported int
	// refused marks a body the PUT validator refuses before the repair, so
	// nothing is asserted of what it says after it.
	refused bool
}

func tzidRepairEvent(uid, timezones, properties string) string {
	return "BEGIN:VCALENDAR\r\nVERSION:2.0\r\nPRODID:-//CalCard//repair test//EN\r\n" + timezones +
		"BEGIN:VEVENT\r\nUID:" + uid + "\r\nDTSTAMP:20260101T000000Z\r\n" + properties + "END:VEVENT\r\nEND:VCALENDAR\r\n"
}

func tzidRepairCases() []tzidRepairCase {
	utcZero := func(id string) string {
		return "BEGIN:VTIMEZONE\r\nTZID:" + id + "\r\nBEGIN:STANDARD\r\nDTSTART:16010101T000000\r\nTZOFFSETFROM:+0000\r\nTZOFFSETTO:+0000\r\nEND:STANDARD\r\nEND:VTIMEZONE\r\n"
	}
	same := func(uid, timezones, properties string) (string, string) {
		body := tzidRepairEvent(uid, timezones, properties)
		return body, body
	}
	cases := []tzidRepairCase{
		// TZID=UTC on a local DATE-TIME, which ical4j's relaxed parsing drops,
		// becomes the UTC form, on every property that takes the parameter.
		{uid: "utc-start-end",
			before: tzidRepairEvent("utc-start-end", "", "DTSTART;TZID=UTC:20260701T100000\r\nDTEND;TZID=UTC:20260701T110000\r\n"),
			after:  tzidRepairEvent("utc-start-end", "", "DTSTART:20260701T100000Z\r\nDTEND:20260701T110000Z\r\n")},
		// Quoted, untrimmed, and with a tab, it still names UTC.
		{uid: "utc-quoted",
			before: tzidRepairEvent("utc-quoted", "", "DTSTART;TZID=\"UTC\":20260701T100000\r\nDTEND:20260701T110000Z\r\n"),
			after:  tzidRepairEvent("utc-quoted", "", "DTSTART:20260701T100000Z\r\nDTEND:20260701T110000Z\r\n")},
		{uid: "utc-untrimmed-quoted",
			before: tzidRepairEvent("utc-untrimmed-quoted", "", "DTSTART;TZID=\" UTC\":20260701T100000\r\nDTEND;TZID=\"UTC \":20260701T110000\r\n"),
			after:  tzidRepairEvent("utc-untrimmed-quoted", "", "DTSTART:20260701T100000Z\r\nDTEND:20260701T110000Z\r\n")},
		{uid: "utc-untrimmed-unquoted",
			before: tzidRepairEvent("utc-untrimmed-unquoted", "", "DTSTART;TZID=\tUTC :20260701T100000\r\nDTEND;TZID= UTC:20260701T110000\r\n"),
			after:  tzidRepairEvent("utc-untrimmed-unquoted", "", "DTSTART:20260701T100000Z\r\nDTEND:20260701T110000Z\r\n")},
		// Parameters around the TZID stay as written, a quoted one holding a
		// semicolon and a colon included, and the name keeps its case.
		{uid: "utc-other-parameters",
			before: tzidRepairEvent("utc-other-parameters", "", "dtstart;X-A=1;tzid=UTC;VALUE=DATE-TIME;X-B=\"q;r:s\",t:20260701T100000\r\nDTEND:20260701T110000Z\r\n"),
			after:  tzidRepairEvent("utc-other-parameters", "", "dtstart;X-A=1;VALUE=DATE-TIME;X-B=\"q;r:s\",t:20260701T100000Z\r\nDTEND:20260701T110000Z\r\n")},
		// A task's DUE.
		{uid: "utc-due",
			before: "BEGIN:VCALENDAR\r\nVERSION:2.0\r\nPRODID:-//CalCard//repair test//EN\r\nBEGIN:VTODO\r\nUID:utc-due\r\nDTSTAMP:20260101T000000Z\r\nDTSTART;TZID=UTC:20260701T100000\r\nDUE;TZID=UTC:20260702T100000\r\nEND:VTODO\r\nEND:VCALENDAR\r\n",
			after:  "BEGIN:VCALENDAR\r\nVERSION:2.0\r\nPRODID:-//CalCard//repair test//EN\r\nBEGIN:VTODO\r\nUID:utc-due\r\nDTSTAMP:20260101T000000Z\r\nDTSTART:20260701T100000Z\r\nDUE:20260702T100000Z\r\nEND:VTODO\r\nEND:VCALENDAR\r\n"},
		// A series with its override, an EXDATE and an RDATE in UTC, and an
		// RDATE whose zone is untrimmed: both rewrites in one body, which the
		// recurrence repair also bounds. The master's DTSTART and the
		// override's RECURRENCE-ID keep one form and TZID, as PUT holds them.
		{uid: "utc-series",
			before: "BEGIN:VCALENDAR\r\nVERSION:2.0\r\nPRODID:-//CalCard//repair test//EN\r\n" +
				"BEGIN:VEVENT\r\nUID:utc-series\r\nDTSTAMP:20260101T000000Z\r\nDTSTART;TZID=UTC:20260701T100000\r\nDTEND;TZID=UTC:20260701T110000\r\n" +
				"RRULE:FREQ=WEEKLY;COUNT=4\r\nEXDATE;TZID=UTC:20260708T100000\r\nRDATE;TZID=\" Europe/Berlin\":20260801T120000\r\nEND:VEVENT\r\n" +
				"BEGIN:VEVENT\r\nUID:utc-series\r\nDTSTAMP:20260101T000000Z\r\nRECURRENCE-ID;RANGE=THISANDFUTURE;TZID=UTC:20260715T100000\r\n" +
				"DTSTART;TZID=UTC:20260715T120000\r\nDTEND;TZID=UTC:20260715T130000\r\nEND:VEVENT\r\nEND:VCALENDAR\r\n",
			after: "BEGIN:VCALENDAR\r\nVERSION:2.0\r\nPRODID:-//CalCard//repair test//EN\r\n" +
				"BEGIN:VEVENT\r\nUID:utc-series\r\nDTSTAMP:20260101T000000Z\r\nDTSTART:20260701T100000Z\r\nDTEND:20260701T110000Z\r\n" +
				"RRULE:FREQ=WEEKLY;COUNT=4\r\nEXDATE:20260708T100000Z\r\nRDATE;TZID=\"Europe/Berlin\":20260801T120000\r\nEND:VEVENT\r\n" +
				"BEGIN:VEVENT\r\nUID:utc-series\r\nDTSTAMP:20260101T000000Z\r\nRECURRENCE-ID;RANGE=THISANDFUTURE:20260715T100000Z\r\n" +
				"DTSTART:20260715T120000Z\r\nDTEND:20260715T130000Z\r\nEND:VEVENT\r\nEND:VCALENDAR\r\n"},
		// Untrimmed zone names are trimmed, inside quotes or out, white space
		// beyond ASCII included, and keep their quotes.
		{uid: "trim-quoted",
			before: tzidRepairEvent("trim-quoted", "", "DTSTART;TZID=\" Europe/Berlin\":20260701T100000\r\nDTEND;TZID=\"Europe/Berlin\xc2\xa0\":20260701T110000\r\n"),
			after:  tzidRepairEvent("trim-quoted", "", "DTSTART;TZID=\"Europe/Berlin\":20260701T100000\r\nDTEND;TZID=\"Europe/Berlin\":20260701T110000\r\n")},
		{uid: "trim-unquoted",
			before: tzidRepairEvent("trim-unquoted", "", "DTSTART;TZID= Europe/Berlin :20260701T100000\r\nDTEND;TZID=Europe/Berlin:20260701T110000\r\n"),
			after:  tzidRepairEvent("trim-unquoted", "", "DTSTART;TZID=Europe/Berlin:20260701T100000\r\nDTEND;TZID=Europe/Berlin:20260701T110000\r\n")},
		// A definition named UTC whose offsets are all zero reads every local
		// time as the UTC instant it names, so the times are rewritten. The
		// definition stays: RFC 5545 section 3.6.5 asks for one per TZID the
		// object uses, not that every one be used, and the PUT validator leaves
		// an unused one alone. Its observance's own DTSTART is never read.
		{uid: "utc-defined-zero",
			before: tzidRepairEvent("utc-defined-zero", utcZero("UTC"), "DTSTART;TZID=UTC:20260701T100000\r\nDTEND;TZID=UTC:20260701T110000\r\n"),
			after:  tzidRepairEvent("utc-defined-zero", utcZero("UTC"), "DTSTART:20260701T100000Z\r\nDTEND:20260701T110000Z\r\n")},

		// The UTC rewrite applies to lines the repair can only rewrite by
		// re-folding or by rewriting several values; those are reported and
		// left untouched, while the other lines of the body are rewritten.
		{uid: "utc-folded", reported: 1,
			before: tzidRepairEvent("utc-folded", "", "DTSTART;TZID=UT\r\n C:20260701T100000\r\nDTEND;TZID=UTC:20260701T110000\r\n"),
			after:  tzidRepairEvent("utc-folded", "", "DTSTART;TZID=UT\r\n C:20260701T100000\r\nDTEND:20260701T110000Z\r\n")},
		{uid: "utc-exdate-list", reported: 1,
			before: tzidRepairEvent("utc-exdate-list", "", "DTSTART;TZID=UTC:20260701T100000\r\nRRULE:FREQ=WEEKLY;COUNT=4\r\nEXDATE;TZID=UTC:20260708T100000,20260715T100000\r\n"),
			after:  tzidRepairEvent("utc-exdate-list", "", "DTSTART:20260701T100000Z\r\nRRULE:FREQ=WEEKLY;COUNT=4\r\nEXDATE;TZID=UTC:20260708T100000,20260715T100000\r\n")},
		{uid: "utc-rdate-period", reported: 1,
			before: tzidRepairEvent("utc-rdate-period", "", "DTSTART;TZID=UTC:20260701T100000\r\nRDATE;VALUE=PERIOD;TZID=UTC:20260720T100000/PT1H\r\n"),
			after:  tzidRepairEvent("utc-rdate-period", "", "DTSTART:20260701T100000Z\r\nRDATE;VALUE=PERIOD;TZID=UTC:20260720T100000/PT1H\r\n")},
		// A parameter name the server reads with white space around it is
		// still a TZID, in a form the repair does not rewrite.
		{uid: "utc-spaced-name", reported: 1,
			before: tzidRepairEvent("utc-spaced-name", "", "DTSTART; TZID=UTC:20260701T100000\r\nDTEND:20260701T110000Z\r\n"),
			after:  tzidRepairEvent("utc-spaced-name", "", "DTSTART; TZID=UTC:20260701T100000\r\nDTEND:20260701T110000Z\r\n")},
		// The series' DTSTART and its override's RECURRENCE-ID must keep one
		// form, so a RECURRENCE-ID the repair cannot rewrite keeps every
		// DTSTART and RECURRENCE-ID of the body in theirs; the DTENDs are
		// rewritten.
		{uid: "utc-identity-folded", reported: 3,
			before: "BEGIN:VCALENDAR\r\nVERSION:2.0\r\nPRODID:-//CalCard//repair test//EN\r\n" +
				"BEGIN:VEVENT\r\nUID:utc-identity-folded\r\nDTSTAMP:20260101T000000Z\r\nDTSTART;TZID=UTC:20260701T100000\r\nDTEND;TZID=UTC:20260701T110000\r\nRRULE:FREQ=WEEKLY;COUNT=4\r\nEND:VEVENT\r\n" +
				"BEGIN:VEVENT\r\nUID:utc-identity-folded\r\nDTSTAMP:20260101T000000Z\r\nRECURRENCE-ID;TZID=UTC:2026\r\n 0708T100000\r\n" +
				"DTSTART;TZID=UTC:20260708T120000\r\nDTEND;TZID=UTC:20260708T130000\r\nEND:VEVENT\r\nEND:VCALENDAR\r\n",
			after: "BEGIN:VCALENDAR\r\nVERSION:2.0\r\nPRODID:-//CalCard//repair test//EN\r\n" +
				"BEGIN:VEVENT\r\nUID:utc-identity-folded\r\nDTSTAMP:20260101T000000Z\r\nDTSTART;TZID=UTC:20260701T100000\r\nDTEND:20260701T110000Z\r\nRRULE:FREQ=WEEKLY;COUNT=4\r\nEND:VEVENT\r\n" +
				"BEGIN:VEVENT\r\nUID:utc-identity-folded\r\nDTSTAMP:20260101T000000Z\r\nRECURRENCE-ID;TZID=UTC:2026\r\n 0708T100000\r\n" +
				"DTSTART;TZID=UTC:20260708T120000\r\nDTEND:20260708T130000Z\r\nEND:VEVENT\r\nEND:VCALENDAR\r\n"},
		// A definition named UTC that moves its clock reads a local time as
		// another instant than the UTC form names.
		{uid: "utc-defined-shifted", reported: 2,
			before: tzidRepairEvent("utc-defined-shifted",
				"BEGIN:VTIMEZONE\r\nTZID:UTC\r\nBEGIN:STANDARD\r\nDTSTART:16010101T000000\r\nTZOFFSETFROM:+0100\r\nTZOFFSETTO:+0100\r\nEND:STANDARD\r\nEND:VTIMEZONE\r\n",
				"DTSTART;TZID=UTC:20260701T100000\r\nDTEND;TZID=UTC:20260701T110000\r\n")},
		// A definition's name and offsets are its values after the first
		// colon outside quotes, as ical.SplitContentLine reads them, so a
		// quoted parameter holding a colon neither hides that this one is
		// named UTC and moves the clock, nor that the next keeps it at zero.
		{uid: "utc-defined-quoted-colon", reported: 2,
			before: tzidRepairEvent("utc-defined-quoted-colon",
				"BEGIN:VTIMEZONE\r\nTZID;X-A=\"a:b\":UTC\r\nBEGIN:STANDARD\r\nDTSTART:16010101T000000\r\nTZOFFSETFROM:+0100\r\nTZOFFSETTO:+0100\r\nEND:STANDARD\r\nEND:VTIMEZONE\r\n",
				"DTSTART;TZID=UTC:20260701T100000\r\nDTEND;TZID=UTC:20260701T110000\r\n")},
		{uid: "utc-defined-zero-quoted-colon",
			before: tzidRepairEvent("utc-defined-zero-quoted-colon",
				"BEGIN:VTIMEZONE\r\nTZID:UTC\r\nBEGIN:STANDARD\r\nDTSTART:16010101T000000\r\nTZOFFSETFROM;X-A=\"was:+0100\":+0000\r\nTZOFFSETTO;X-A=\"was:+0100\":+0000\r\nEND:STANDARD\r\nEND:VTIMEZONE\r\n",
				"DTSTART;TZID=UTC:20260701T100000\r\n"),
			after: tzidRepairEvent("utc-defined-zero-quoted-colon",
				"BEGIN:VTIMEZONE\r\nTZID:UTC\r\nBEGIN:STANDARD\r\nDTSTART:16010101T000000\r\nTZOFFSETFROM;X-A=\"was:+0100\":+0000\r\nTZOFFSETTO;X-A=\"was:+0100\":+0000\r\nEND:STANDARD\r\nEND:VTIMEZONE\r\n",
				"DTSTART:20260701T100000Z\r\n")},

		// Line breaks are CRLF, bare LF or bare CR, as ical.UnfoldLines reads
		// them, and a fold in a body of either is still reported.
		{uid: "utc-lf", reported: 1,
			before: strings.ReplaceAll(tzidRepairEvent("utc-lf", "", "DTSTART;TZID=UTC:20260701T100000\r\nDTEND;TZID=UT\r\n C:20260701T110000\r\nRDATE;TZID= Europe/Berlin:20260702T100000\r\n"), "\r\n", "\n"),
			after:  strings.ReplaceAll(tzidRepairEvent("utc-lf", "", "DTSTART:20260701T100000Z\r\nDTEND;TZID=UT\r\n C:20260701T110000\r\nRDATE;TZID=Europe/Berlin:20260702T100000\r\n"), "\r\n", "\n")},
		{uid: "utc-cr", reported: 1,
			before: strings.ReplaceAll(tzidRepairEvent("utc-cr", "", "DTSTART;TZID=UTC:20260701T100000\r\nDTEND;TZID=UT\r\n C:20260701T110000\r\nRDATE;TZID= Europe/Berlin:20260702T100000\r\n"), "\r\n", "\r"),
			after:  strings.ReplaceAll(tzidRepairEvent("utc-cr", "", "DTSTART:20260701T100000Z\r\nDTEND;TZID=UT\r\n C:20260701T110000\r\nRDATE;TZID=Europe/Berlin:20260702T100000\r\n"), "\r\n", "\r")},

		// An END closes the innermost open component of its name and every
		// component opened inside it, as ical.ReadComponents reads it, so a
		// VTIMEZONE left open inside a VEVENT ends with the VEVENT, and the
		// override after it is outside every definition.
		{uid: "utc-after-unclosed-timezone", refused: true, reported: 1,
			before: "BEGIN:VCALENDAR\r\nVERSION:2.0\r\nPRODID:-//CalCard//repair test//EN\r\n" +
				"BEGIN:VEVENT\r\nUID:utc-after-unclosed-timezone\r\nDTSTAMP:20260101T000000Z\r\nDTSTART;TZID=UTC:20260701T100000\r\nRRULE:FREQ=DAILY;COUNT=3\r\nBEGIN:VTIMEZONE\r\nEND:VEVENT\r\n" +
				"BEGIN:VEVENT\r\nUID:utc-after-unclosed-timezone\r\nDTSTAMP:20260101T000000Z\r\nRECURRENCE-ID;TZID=UTC:20260702T100000\r\nDTSTART;TZID=UTC:20260702T120000\r\nDTEND;TZID=UT\r\n C:20260702T130000\r\nEND:VEVENT\r\nEND:VCALENDAR\r\n",
			after: "BEGIN:VCALENDAR\r\nVERSION:2.0\r\nPRODID:-//CalCard//repair test//EN\r\n" +
				"BEGIN:VEVENT\r\nUID:utc-after-unclosed-timezone\r\nDTSTAMP:20260101T000000Z\r\nDTSTART:20260701T100000Z\r\nRRULE:FREQ=DAILY;COUNT=3\r\nBEGIN:VTIMEZONE\r\nEND:VEVENT\r\n" +
				"BEGIN:VEVENT\r\nUID:utc-after-unclosed-timezone\r\nDTSTAMP:20260101T000000Z\r\nRECURRENCE-ID:20260702T100000Z\r\nDTSTART:20260702T120000Z\r\nDTEND;TZID=UT\r\n C:20260702T130000\r\nEND:VEVENT\r\nEND:VCALENDAR\r\n"},
	}
	for _, untouched := range []struct {
		uid, timezones, properties string
		refused                    bool
	}{
		// Already in UTC, or a DATE, the value carries no wall clock to float.
		// RFC 5545 section 3.2.19 forbids TZID on either, and PUT refuses both.
		{uid: "utc-already-z", properties: "DTSTART;TZID=UTC:20260701T100000Z\r\nDTEND:20260701T110000Z\r\n", refused: true},
		{uid: "utc-date", properties: "DTSTART;VALUE=DATE;TZID=UTC:20260701\r\nDTEND;VALUE=DATE:20260702\r\n", refused: true},
		// ical4j drops only a parameter whose value is exactly UTC, matched
		// with regard to case; another spelling names another zone.
		{uid: "utc-lowercase", timezones: utcZero("utc"), properties: "DTSTART;TZID=utc:20260701T100000\r\nDTEND;TZID=utc:20260701T110000\r\n"},
		// The text appears only inside another parameter's quoted value.
		{uid: "tzid-in-quoted-value", properties: "DTSTART;X-LABEL=\"TZID=UTC\":20260701T100000Z\r\nDTEND:20260701T110000Z\r\n"},
		// A zone name with white space inside it, which is not trimmed.
		{uid: "spaced-zone-name", timezones: utcZero("Pacific Standard Time"),
			properties: "DTSTART;TZID=\"Pacific Standard Time\":20260701T100000\r\nDTEND;TZID=\"Pacific Standard Time\":20260701T110000\r\n"},
		// An observance's DTSTART is a local time by definition, and ical4j
		// discards any TZID inside a VTIMEZONE. PUT refuses the parameter
		// there, but a 1.1.x release stored it.
		{uid: "utc-in-observance", refused: true, timezones: "BEGIN:VTIMEZONE\r\nTZID:Europe/Berlin\r\nBEGIN:STANDARD\r\nDTSTART;TZID=UTC:16010101T000000\r\nTZOFFSETFROM:+0100\r\nTZOFFSETTO:+0100\r\nEND:STANDARD\r\nEND:VTIMEZONE\r\n",
			properties: "DTSTART;TZID=Europe/Berlin:20260701T100000\r\nDTEND;TZID=Europe/Berlin:20260701T110000\r\n"},
		{uid: "berlin", properties: "DTSTART;TZID=Europe/Berlin:20260701T100000\r\nDTEND;TZID=Europe/Berlin:20260701T110000\r\n"},
		// An END naming no open component closes nothing, so the definition
		// it stands in runs on to its own END, and its observance's DTSTART
		// is still inside it.
		{uid: "utc-in-observance-after-stray-end", refused: true, timezones: "BEGIN:VTIMEZONE\r\nTZID:Europe/Berlin\r\nEND:VALARM\r\nBEGIN:STANDARD\r\nDTSTART;TZID=UTC:16010101T000000\r\nTZOFFSETFROM:+0100\r\nTZOFFSETTO:+0100\r\nEND:STANDARD\r\nEND:VTIMEZONE\r\n",
			properties: "DTSTART;TZID=Europe/Berlin:20260701T100000\r\n"},
	} {
		before, after := same(untouched.uid, untouched.timezones, untouched.properties)
		cases = append(cases, tzidRepairCase{uid: untouched.uid, before: before, after: after, refused: untouched.refused})
	}
	for i := range cases {
		if cases[i].after == "" {
			cases[i].after = cases[i].before
		}
	}
	return cases
}

// DAVx5 reads a TZID through ical4j: under relaxed parsing ical4j deletes a
// parameter whose value is exactly UTC, and it keeps a TZID's white space, so
// the name resolves to nothing. Either way the time floats and shows at the
// device's local time. The upgrade writes such a time in its UTC form and
// trims the zone name, gives each event it changes a new ETag and moves its
// calendar once, and reports the lines it cannot rewrite without re-folding
// them or rewriting several values, leaving them as they are.
func TestPostgres_UpgradeRewritesTZIDsDAVx5Misreads(t *testing.T) {
	pool := store.NewV119ReleasePool(t)
	var mu sync.Mutex
	var warnings []*pq.Error
	noticed := store.OpenWithNotices(t, pool, func(notice *pq.Error) {
		if notice.Severity != "WARNING" {
			return
		}
		mu.Lock()
		defer mu.Unlock()
		warnings = append(warnings, notice)
	})

	var userID, repairedID, unrelatedID int64
	if err := pool.QueryRow(`INSERT INTO users (oauth_subject, primary_email) VALUES ('subject-tzid', 'tzid@example.test') RETURNING id`).Scan(&userID); err != nil {
		t.Fatalf("seed user: %v", err)
	}
	for name, id := range map[string]*int64{"Repaired": &repairedID, "Unrelated": &unrelatedID} {
		if err := pool.QueryRow(`INSERT INTO calendars (user_id, name) VALUES ($1, $2) RETURNING id`, userID, name).Scan(id); err != nil {
			t.Fatalf("seed calendar %s: %v", name, err)
		}
	}

	cases := tzidRepairCases()
	ids := map[string]int64{}
	for _, test := range cases {
		var dtstart, dtend *time.Time
		allDay := false
		if !test.refused {
			before := validateCalendarObject(t, test.uid, test.before)
			dtstart, dtend, allDay = before.Metadata.DTStart, before.Metadata.DTEnd, before.Metadata.AllDay
		} else {
			start := time.Date(2026, 7, 1, 10, 0, 0, 0, time.UTC)
			end := start.Add(time.Hour)
			if test.uid == "utc-date" {
				start, end, allDay = time.Date(2026, 7, 1, 0, 0, 0, 0, time.UTC), time.Date(2026, 7, 2, 0, 0, 0, 0, time.UTC), true
			}
			dtstart, dtend = &start, &end
		}
		var id int64
		if err := pool.QueryRow(`
INSERT INTO events (calendar_id, uid, resource_name, raw_ical, etag, dtstart, dtend, all_day, last_modified)
VALUES ($1, $2, $2 || '.ics', $3, $4, $5, $6, $7, NOW()) RETURNING id`,
			repairedID, test.uid, test.before, "etag-"+test.uid, dtstart, dtend, allDay).Scan(&id); err != nil {
			t.Fatalf("seed %s: %v", test.uid, err)
		}
		ids[test.uid] = id
	}
	if _, err := pool.Exec(`
INSERT INTO events (calendar_id, uid, resource_name, raw_ical, etag, dtstart, dtend, all_day, last_modified)
VALUES ($1, 'unrelated', 'unrelated.ics', $2, 'etag-unrelated', '2026-07-01T08:00:00Z', '2026-07-01T09:00:00Z', FALSE, NOW())`,
		unrelatedID, tzidRepairEvent("unrelated", "", "DTSTART;TZID=Europe/Berlin:20260701T100000\r\nDTEND;TZID=Europe/Berlin:20260701T110000\r\n")); err != nil {
		t.Fatalf("seed unrelated: %v", err)
	}

	// A client that synced after the last of these writes holds this token.
	syncToken := func(calendarID int64) time.Time {
		t.Helper()
		var token time.Time
		if err := pool.QueryRow(`UPDATE calendars SET updated_at = clock_timestamp() WHERE id = $1 RETURNING updated_at`, calendarID).Scan(&token); err != nil {
			t.Fatalf("take sync token: %v", err)
		}
		return token
	}
	tokenBefore := syncToken(repairedID)
	syncToken(unrelatedID)
	ctags := func() (int64, int64) {
		t.Helper()
		var repaired, unrelated int64
		if err := pool.QueryRow(`SELECT (SELECT ctag FROM calendars WHERE id = $1), (SELECT ctag FROM calendars WHERE id = $2)`,
			repairedID, unrelatedID).Scan(&repaired, &unrelated); err != nil {
			t.Fatalf("read ctags: %v", err)
		}
		return repaired, unrelated
	}
	repairedCTag, unrelatedCTag := ctags()

	store.MigrateWithRunner(t, noticed, "../../migrations", "v1.2.0")

	type storedEvent struct {
		body, etag     string
		dtstart, dtend *time.Time
		allDay         bool
	}
	readEvents := func() map[string]storedEvent {
		t.Helper()
		rows, err := pool.Query(`SELECT uid, raw_ical, etag, dtstart, dtend, all_day FROM events WHERE calendar_id = $1`, repairedID)
		if err != nil {
			t.Fatalf("read events: %v", err)
		}
		defer rows.Close()
		events := map[string]storedEvent{}
		for rows.Next() {
			var uid string
			var event storedEvent
			if err := rows.Scan(&uid, &event.body, &event.etag, &event.dtstart, &event.dtend, &event.allDay); err != nil {
				t.Fatalf("scan event: %v", err)
			}
			events[uid] = event
		}
		if err := rows.Err(); err != nil {
			t.Fatalf("read events: %v", err)
		}
		return events
	}
	afterUpgrade := readEvents()

	var changed, reported []string
	reportedLines := 0
	for _, test := range cases {
		got := afterUpgrade[test.uid]
		if got.body != test.after {
			t.Errorf("%s: stored body =\n%q\nwant\n%q", test.uid, got.body, test.after)
			continue
		}
		wantETag := "etag-" + test.uid
		if test.after != test.before {
			changed = append(changed, test.uid)
			wantETag = fmt.Sprintf("%x", sha256.Sum256([]byte(test.after)))
		}
		if got.etag != wantETag {
			t.Errorf("%s: etag = %q, want %q", test.uid, got.etag, wantETag)
		}
		if test.reported > 0 {
			reported = append(reported, test.uid)
			reportedLines += test.reported
		}
		if test.refused {
			continue
		}
		// The rewrite names the instants the body named before: a write of
		// the new body stores the columns a write of the old one did, and
		// the upgrade left the columns as they were.
		before := validateCalendarObject(t, test.uid, test.before)
		after := validateCalendarObject(t, test.uid, test.after)
		if !sameInstant(after.Metadata.DTStart, before.Metadata.DTStart) || !sameInstant(after.Metadata.DTEnd, before.Metadata.DTEnd) || after.Metadata.AllDay != before.Metadata.AllDay {
			t.Errorf("%s: a write of the rewritten body stores %v, %v, %t, want %v, %v, %t", test.uid,
				after.Metadata.DTStart, after.Metadata.DTEnd, after.Metadata.AllDay, before.Metadata.DTStart, before.Metadata.DTEnd, before.Metadata.AllDay)
		}
		if !sameInstant(got.dtstart, before.Metadata.DTStart) || !sameInstant(got.dtend, before.Metadata.DTEnd) || got.allDay != before.Metadata.AllDay {
			t.Errorf("%s: stored columns = %v, %v, %t, want %v, %v, %t", test.uid,
				got.dtstart, got.dtend, got.allDay, before.Metadata.DTStart, before.Metadata.DTEnd, before.Metadata.AllDay)
		}
		if !sameInstant(after.Metadata.RecurrenceStart, before.Metadata.RecurrenceStart) || !sameInstant(after.Metadata.RecurrenceUntil, before.Metadata.RecurrenceUntil) {
			t.Errorf("%s: a write of the rewritten body bounds its recurrence by %v, %v, want %v, %v", test.uid,
				after.Metadata.RecurrenceStart, after.Metadata.RecurrenceUntil, before.Metadata.RecurrenceStart, before.Metadata.RecurrenceUntil)
		}
	}

	// One move of the repaired calendar's CTag and sync token, none of the
	// other's; a client syncing from before the upgrade is told of every
	// rewritten event and of nothing else.
	repairedAfter, unrelatedAfter := ctags()
	if moves := repairedAfter - repairedCTag; moves != 1 {
		t.Errorf("repaired calendar CTag moved %d times, want once", moves)
	}
	if moves := unrelatedAfter - unrelatedCTag; moves != 0 {
		t.Errorf("unrelated calendar CTag moved %d times, want never", moves)
	}
	page, err := store.New(pool).Events.ListChangesAfter(t.Context(), repairedID, store.SyncCursorAt(tokenBefore), true, 1000)
	if err != nil {
		t.Fatalf("ListChangesAfter() error = %v", err)
	}
	var synced []string
	for _, event := range page.Members {
		synced = append(synced, event.UID)
	}
	if slices.Sort(synced); !slices.Equal(synced, sortedCopy(changed)) {
		t.Errorf("sync from the pre-upgrade token reports %v, want the rewritten events %v", synced, sortedCopy(changed))
	}

	// The lines left untouched are reported in the server log, with the
	// events that carry them, and the operator's query finds the same events.
	var wantIDs []int64
	for _, uid := range reported {
		wantIDs = append(wantIDs, ids[uid])
	}
	slices.Sort(wantIDs)
	mu.Lock()
	gotWarnings := slices.Clone(warnings)
	mu.Unlock()
	assertTZIDRepairWarning(t, gotWarnings, len(changed), reportedLines, wantIDs)
	if pending := pendingTZIDRepairs(t, pool); !slices.Equal(pending, wantIDs) {
		t.Errorf("the operator query lists %v, want the reported events %v", pending, wantIDs)
	}

	// A second run finds nothing more to rewrite: no body, ETag, CTag or sync
	// token moves, and the untouched lines are reported again.
	mu.Lock()
	warnings = nil
	mu.Unlock()
	tokenAfter := syncToken(repairedID)
	repairedCTag, _ = ctags()
	store.ApplyV120Migration(t, noticed)
	again := readEvents()
	for uid, first := range afterUpgrade {
		second := again[uid]
		if second.body != first.body || second.etag != first.etag || !sameInstant(second.dtstart, first.dtstart) ||
			!sameInstant(second.dtend, first.dtend) || second.allDay != first.allDay {
			t.Errorf("%s: a second run changed the event from %q (%s) to %q (%s)", uid, first.body, first.etag, second.body, second.etag)
		}
	}
	if again, _ := ctags(); again != repairedCTag {
		t.Errorf("a second run moved the repaired calendar's CTag from %d to %d", repairedCTag, again)
	}
	var token time.Time
	if err := pool.QueryRow(`SELECT updated_at FROM calendars WHERE id = $1`, repairedID).Scan(&token); err != nil {
		t.Fatalf("read sync token: %v", err)
	}
	if !token.Equal(tokenAfter) {
		t.Errorf("a second run moved the sync token from %s to %s", tokenAfter, token)
	}
	mu.Lock()
	gotWarnings = slices.Clone(warnings)
	mu.Unlock()
	assertTZIDRepairWarning(t, gotWarnings, 0, reportedLines, wantIDs)
}

// assertTZIDRepairWarning checks the one warning the repair raises: how many
// events it rewrote, how many lines it left in which events, and the ids of
// those events.
func assertTZIDRepairWarning(t *testing.T, warnings []*pq.Error, rewritten, lines int, ids []int64) {
	t.Helper()
	var repair []*pq.Error
	for _, warning := range warnings {
		if strings.Contains(warning.Message, "TZID") {
			repair = append(repair, warning)
		}
	}
	if len(repair) != 1 {
		t.Fatalf("the upgrade raised %d TZID repair warnings, want one: %v", len(repair), repair)
	}
	wantMessage := fmt.Sprintf("v1.2.0 TZID repair: rewrote %d events, and left %d content lines in %d events as stored", rewritten, lines, len(ids))
	if repair[0].Message != wantMessage {
		t.Errorf("warning = %q, want %q", repair[0].Message, wantMessage)
	}
	idList := make([]string, len(ids))
	for i, id := range ids {
		idList[i] = fmt.Sprint(id)
	}
	if wantDetail := "events.id: " + strings.Join(idList, ", "); repair[0].Detail != wantDetail {
		t.Errorf("warning detail = %q, want %q", repair[0].Detail, wantDetail)
	}
	if repair[0].Hint == "" {
		t.Error("the warning carries no hint")
	}
}

// pendingTZIDRepairs runs the operator's query for the events the TZID repair
// left as stored, read from the file the README and the repair's hint name.
func pendingTZIDRepairs(t *testing.T, pool *sql.DB) []int64 {
	t.Helper()
	query, err := os.ReadFile(filepath.Join("..", "..", "docs", "upgrade-1.2.0", "tzid-repair-pending.sql"))
	if err != nil {
		t.Fatalf("read the operator query: %v", err)
	}
	rows, err := pool.Query(string(query))
	if err != nil {
		t.Fatalf("run the operator query: %v", err)
	}
	defer rows.Close()
	var ids []int64
	for rows.Next() {
		var id, calendarID int64
		var resourceName string
		if err := rows.Scan(&id, &calendarID, &resourceName); err != nil {
			t.Fatalf("scan id: %v", err)
		}
		ids = append(ids, id)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("run the operator query: %v", err)
	}
	return ids
}

func validateCalendarObject(t *testing.T, uid, body string) *calobject.Object {
	t.Helper()
	object, err := dav.ValidateCalendarObject(body, nil)
	if err != nil {
		t.Fatalf("%s: the PUT validator refuses\n%q\n: %v", uid, body, err)
	}
	return object
}

func sameInstant(a, b *time.Time) bool {
	if a == nil || b == nil {
		return a == b
	}
	return a.Equal(*b)
}

func sortedCopy(values []string) []string {
	sorted := slices.Clone(values)
	slices.Sort(sorted)
	return sorted
}

// The repair reports how many events it rewrote on every run, whether or not
// it left any line as stored: a WARNING when it changed or left anything, and
// a NOTICE when it found nothing to do.
func TestPostgres_UpgradeReportsEveryTZIDRepairCount(t *testing.T) {
	pool := store.NewV119ReleasePool(t)
	var mu sync.Mutex
	var notices []*pq.Error
	noticed := store.OpenWithNotices(t, pool, func(notice *pq.Error) {
		if !strings.Contains(notice.Message, "TZID repair") {
			return
		}
		mu.Lock()
		defer mu.Unlock()
		notices = append(notices, notice)
	})
	var calendarID int64
	if err := pool.QueryRow(`
WITH seeded_user AS (
    INSERT INTO users (oauth_subject, primary_email) VALUES ('subject-tzid-count', 'tzid-count@example.test') RETURNING id
)
INSERT INTO calendars (user_id, name) SELECT id, 'Count' FROM seeded_user RETURNING id`).Scan(&calendarID); err != nil {
		t.Fatalf("seed calendar: %v", err)
	}
	if _, err := pool.Exec(`INSERT INTO events (calendar_id, uid, resource_name, raw_ical, etag, last_modified) VALUES ($1, 'count', 'count.ics', $2, 'etag', NOW())`,
		calendarID, tzidRepairEvent("count", "", "DTSTART;TZID=UTC:20260701T100000\r\n")); err != nil {
		t.Fatalf("seed event: %v", err)
	}

	take := func() []string {
		mu.Lock()
		defer mu.Unlock()
		var got []string
		for _, notice := range notices {
			got = append(got, notice.Severity+" "+notice.Message)
		}
		notices = nil
		return got
	}
	store.MigrateWithRunner(t, noticed, "../../migrations", "v1.2.0")
	if got, want := take(), []string{"WARNING v1.2.0 TZID repair: rewrote 1 events, and left 0 content lines in 0 events as stored"}; !slices.Equal(got, want) {
		t.Errorf("the upgrade reported %q, want %q", got, want)
	}
	store.ApplyV120Migration(t, noticed)
	if got, want := take(), []string{"NOTICE v1.2.0 TZID repair: rewrote 0 events, and left 0 content lines in 0 events as stored"}; !slices.Equal(got, want) {
		t.Errorf("a second run reported %q, want %q", got, want)
	}
}
