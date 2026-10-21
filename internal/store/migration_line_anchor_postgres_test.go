package store

import (
	"fmt"
	"strings"
	"testing"
	"time"

	icalpkg "github.com/jw6ventures/calcard/internal/ical"
)

// A component begins and ends only on a content line of its own, as
// ical.ReadComponents reads one: BEGIN:VEVENT or END:VEVENT at the start of a
// line, followed by nothing but white space. Text spelling either inside a
// property value is not one, so a DESCRIPTION holding END:VEVENT must neither
// end the component the recurrence repair reads early nor make text after it
// part of one, and a VTIMEZONE named inside a value must not strip the lines
// after it from the reading that reopens bounds.
func TestPostgres_V120MigrationReadsComponentsFromContentLineStarts(t *testing.T) {
	pool := newV119ReleasePool(t)
	calendarID := seedRepairCalendar(t, pool, "subject-line-anchor")

	const newYork = "BEGIN:VTIMEZONE\r\nTZID:America/New_York\r\n" +
		"BEGIN:DAYLIGHT\r\nDTSTART:20070311T020000\r\nTZOFFSETFROM:-0500\r\nTZOFFSETTO:-0400\r\nRRULE:FREQ=YEARLY;BYMONTH=3;BYDAY=2SU\r\nEND:DAYLIGHT\r\n" +
		"BEGIN:STANDARD\r\nDTSTART:20071104T020000\r\nTZOFFSETFROM:-0400\r\nTZOFFSETTO:-0500\r\nRRULE:FREQ=YEARLY;BYMONTH=11;BYDAY=1SU\r\nEND:STANDARD\r\n" +
		"END:VTIMEZONE\r\n"
	cases := []recurrenceRepairCase{
		// The DESCRIPTION's text closes nothing: the RRULE after it belongs to
		// the VEVENT, which recurs.
		{uid: "description-end-vevent", recurring: true, raw: "BEGIN:VCALENDAR\r\nBEGIN:VEVENT\r\nUID:description-end-vevent\r\nDTSTART:20260115T100000Z\r\n" +
			"DESCRIPTION:paste of BEGIN:VEVENT ... END:VEVENT from a mail\r\nRRULE:FREQ=DAILY\r\nEND:VEVENT\r\nEND:VCALENDAR\r\n"},
		{uid: "description-end-vtodo", recurring: true, raw: "BEGIN:VCALENDAR\r\nBEGIN:VTODO\r\nUID:description-end-vtodo\r\n" +
			"DESCRIPTION:END:VTODO\r\nRDATE:20260615T100000Z\r\nEND:VTODO\r\nEND:VCALENDAR\r\n"},
		// A fold puts the text at the start of a physical line, but the line
		// continues the DESCRIPTION, and unfolding joins it onto it.
		{uid: "folded-description-end-vjournal", recurring: true, raw: "BEGIN:VCALENDAR\r\nBEGIN:VJOURNAL\r\nUID:folded-description-end-vjournal\r\n" +
			"DESCRIPTION:see\r\n END:VJOURNAL\r\nRECURRENCE-ID:20260615T100000Z\r\nEND:VJOURNAL\r\nEND:VCALENDAR\r\n"},
		// The calendar's description names a VEVENT ahead of a VTIMEZONE whose
		// DST rules are RRULEs. No component opens there, so those rules stay
		// outside every VEVENT and the one-off event does not recur.
		{uid: "calendar-text-begin-vevent", raw: "BEGIN:VCALENDAR\r\nX-WR-CALDESC:export starts at BEGIN:VEVENT\r\n" + newYork +
			"BEGIN:VEVENT\r\nUID:calendar-text-begin-vevent\r\nDTSTART;TZID=America/New_York:20260115T100000\r\nEND:VEVENT\r\nEND:VCALENDAR\r\n"},
		// White space after the component name still ends the line as
		// ical.ReadComponents reads it.
		{uid: "trailing-space", recurring: true, raw: "BEGIN:VCALENDAR\r\nBEGIN:VEVENT \r\nUID:trailing-space\r\nRRULE:FREQ=DAILY\r\nEND:VEVENT\t\r\nEND:VCALENDAR\r\n"},
		{uid: "trailing-no-break-space", recurring: true, raw: "BEGIN:VCALENDAR\r\nBEGIN:VEVENT\xc2\xa0\r\nUID:trailing-no-break-space\r\nRRULE:FREQ=DAILY\r\nEND:VEVENT\xe3\x80\x80\r\nEND:VCALENDAR\r\n"},
		// A longer component name is another component.
		{uid: "longer-name", raw: "BEGIN:VCALENDAR\r\nBEGIN:VEVENTX\r\nUID:longer-name\r\nRRULE:FREQ=DAILY\r\nEND:VEVENTX\r\nEND:VCALENDAR\r\n"},
	}
	for _, test := range cases {
		if _, err := pool.Exec(`
INSERT INTO events (calendar_id, uid, resource_name, raw_ical, etag, dtstart, dtend, all_day, last_modified)
VALUES ($1, $2, $2 || '.ics', $3, 'etag', '2026-01-15T10:00:00Z', '2026-01-15T11:00:00Z', FALSE, NOW())`,
			calendarID, test.uid, test.raw); err != nil {
			t.Fatalf("seed %s: %v", test.uid, err)
		}
	}

	// The unstaged predicate is the definition the staged one must agree with,
	// so it is read before the repair writes the bounds it reads.
	tx, err := pool.Begin()
	if err != nil {
		t.Fatalf("begin legacy predicate transaction: %v", err)
	}
	if _, err := tx.Exec(`ALTER TABLE events ADD COLUMN recurrence_start TIMESTAMPTZ, ADD COLUMN recurrence_until TIMESTAMPTZ`); err != nil {
		tx.Rollback()
		t.Fatalf("add recurrence bounds: %v", err)
	}
	unstaged := map[string]bool{}
	rows, err := tx.Query(`SELECT uid FROM events WHERE ` + legacyRecurrenceRepairPredicate)
	if err != nil {
		tx.Rollback()
		t.Fatalf("read unstaged predicate: %v", err)
	}
	for rows.Next() {
		var uid string
		if err := rows.Scan(&uid); err != nil {
			t.Fatalf("scan uid: %v", err)
		}
		unstaged[uid] = true
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("read unstaged predicate: %v", err)
	}
	rows.Close()
	if err := tx.Rollback(); err != nil {
		t.Fatalf("roll back legacy predicate transaction: %v", err)
	}

	migrateWithRunner(t, pool, "../../migrations", "v1.2.0")

	for _, test := range cases {
		var start, until *time.Time
		if err := pool.QueryRow(`SELECT recurrence_start, recurrence_until FROM events WHERE uid = $1`, test.uid).Scan(&start, &until); err != nil {
			t.Fatalf("read %s bounds: %v", test.uid, err)
		}
		repaired := start != nil || until != nil
		if repaired != test.recurring {
			t.Errorf("%s: repaired = %t, want %t", test.uid, repaired, test.recurring)
		}
		if repaired && (!start.UTC().Equal(icalpkg.RecurrenceStartSentinel) || !until.UTC().Equal(icalpkg.RecurrenceUntilSentinel)) {
			t.Errorf("%s: bounds = %s, %s, want the open-ended sentinels", test.uid, start.UTC(), until.UTC())
		}
		if unstaged[test.uid] != test.recurring {
			t.Errorf("%s: the unstaged predicate selects %t, want %t", test.uid, unstaged[test.uid], test.recurring)
		}
	}
}

// The reopening reads DTSTART outside every VTIMEZONE, whose observances
// always carry a time of day. A VTIMEZONE named inside a DESCRIPTION opens no
// definition, so stripping from it would take the series' own DTSTART with it
// and keep a bound a date UNTIL lets the series outlast.
func TestPostgres_V120MigrationReopensBoundsPastTimezoneTextInValues(t *testing.T) {
	pool := newV119ReleasePool(t)
	if _, err := pool.Exec(`ALTER TABLE events ADD COLUMN recurrence_start TIMESTAMPTZ, ADD COLUMN recurrence_until TIMESTAMPTZ`); err != nil {
		t.Fatalf("add recurrence bounds: %v", err)
	}
	calendarID := seedRepairCalendar(t, pool, "subject-timezone-text")

	const newYork = "BEGIN:VTIMEZONE\r\nTZID:America/New_York\r\n" +
		"BEGIN:STANDARD\r\nDTSTART:20071104T020000\r\nTZOFFSETFROM:-0400\r\nTZOFFSETTO:-0500\r\nRRULE:FREQ=YEARLY;BYMONTH=11;BYDAY=1SU\r\nEND:STANDARD\r\n" +
		"END:VTIMEZONE\r\n"
	cases := []struct {
		uid    string
		raw    string
		reopen bool
	}{
		// The DTSTART lies between the DESCRIPTION's text and the real
		// VTIMEZONE's END, and has a time of day.
		{uid: "description-begin-vtimezone", reopen: true, raw: "BEGIN:VCALENDAR\r\nBEGIN:VEVENT\r\nUID:description-begin-vtimezone\r\n" +
			"DESCRIPTION:copied BEGIN:VTIMEZONE from the invite\r\nDTSTART;TZID=America/New_York:20260301T100000\r\n" +
			"RRULE:FREQ=WEEKLY;UNTIL=20260503\r\nEND:VEVENT\r\n" + newYork + "END:VCALENDAR\r\n"},
		// The same series on dates keeps its bound: only the observance
		// DTSTARTs have a time of day, and they are stripped.
		{uid: "description-begin-vtimezone-all-day", raw: "BEGIN:VCALENDAR\r\nBEGIN:VEVENT\r\nUID:description-begin-vtimezone-all-day\r\n" +
			"DESCRIPTION:copied BEGIN:VTIMEZONE from the invite\r\nDTSTART;VALUE=DATE:20260301\r\n" +
			"RRULE:FREQ=WEEKLY;UNTIL=20260503\r\nEND:VEVENT\r\n" + newYork + "END:VCALENDAR\r\n"},
	}
	finiteStart := time.Date(2026, 3, 1, 0, 0, 0, 0, time.UTC)
	finiteUntil := time.Date(2026, 5, 4, 0, 0, 0, 0, time.UTC)
	for _, test := range cases {
		if _, err := pool.Exec(`
INSERT INTO events (calendar_id, uid, resource_name, raw_ical, etag, dtstart, dtend, all_day, last_modified, recurrence_start, recurrence_until)
VALUES ($1, $2, $2 || '.ics', $3, 'etag', '2026-03-01T15:00:00Z', NULL, FALSE, NOW(), $4, $5)`,
			calendarID, test.uid, test.raw, finiteStart, finiteUntil); err != nil {
			t.Fatalf("seed %s: %v", test.uid, err)
		}
	}

	migrateWithRunner(t, pool, "../../migrations", "v1.2.0")

	for _, test := range cases {
		var until time.Time
		if err := pool.QueryRow(`SELECT recurrence_until FROM events WHERE uid = $1`, test.uid).Scan(&until); err != nil {
			t.Fatalf("read %s bound: %v", test.uid, err)
		}
		want := finiteUntil
		if test.reopen {
			want = icalpkg.RecurrenceUntilSentinel
		}
		if !until.UTC().Equal(want) {
			t.Errorf("%s: recurrence_until = %s, want %s", test.uid, until.UTC(), want)
		}
	}
}

// tzidRepairStatement is the TZID repair as the migration runs it, less the
// PL/pgSQL INTO that hands its counts to the warning, so it can be explained.
func tzidRepairStatement(t *testing.T) string {
	t.Helper()
	statement := migrationStatement(t, "WITH pattern AS (")
	into := strings.Index(statement, "\nINTO tzid_")
	if into < 0 {
		t.Fatal("the TZID repair no longer hands its counts over with INTO")
	}
	return statement[:into] + ";"
}

// The TZID repair reads every row of events under ACCESS EXCLUSIVE. Splitting
// a body into its lines and reading each costs far more than the one search
// that finds the bodies the rule can change, so a body is split only when a
// TZID parameter's value is UTC or has white space at an end, inside its
// quotes or out. Zone names holding white space or UTC within them, which
// Exchange and Outlook write for every event, are never split, however many
// zoned times a body carries.
func TestPostgres_V120MigrationSplitsOnlyBodiesTheTZIDRuleReaches(t *testing.T) {
	pool := newV119ReleasePool(t)
	applyV120Migration(t, pool)
	calendarID := seedRepairCalendar(t, pool, "subject-tzid-split")

	const zoned = 50
	if _, err := pool.Exec(`
INSERT INTO events (calendar_id, uid, resource_name, raw_ical, etag, dtstart, dtend, all_day, last_modified)
SELECT $1, 'zoned-' || g, 'zoned-' || g || '.ics',
       'BEGIN:VCALENDAR' || E'\r\n' || 'BEGIN:VEVENT' || E'\r\n' || 'UID:zoned-' || g || E'\r\n' ||
       'DTSTART;TZID=Europe/Berlin:20260701T100000' || E'\r\n' || 'DTEND;TZID="Europe/Berlin":20260701T110000' || E'\r\n' ||
       'SUMMARY:UTC sync; see https://example.test/?a=1' || E'\r\n' ||
       'END:VEVENT' || E'\r\n' || 'END:VCALENDAR' || E'\r\n',
       'etag', '2026-07-01T08:00:00Z', '2026-07-01T09:00:00Z', FALSE, NOW()
FROM generate_series(1, $2) g`, calendarID, zoned); err != nil {
		t.Fatalf("seed zoned events: %v", err)
	}
	for i, zone := range []string{
		`"W. Europe Standard Time"`, `W. Europe Standard Time`, `"(UTC+01:00) Amsterdam, Berlin, Bern, Rome, Stockholm, Vienna"`,
		`Etc/UTC`, `"UTC+01"`, `"Coordinated Universal Time"`, `"UTC Dublin"`,
	} {
		if _, err := pool.Exec(`
INSERT INTO events (calendar_id, uid, resource_name, raw_ical, etag, dtstart, dtend, all_day, last_modified)
VALUES ($1, $2, $2 || '.ics', $3, 'etag', '2026-07-01T08:00:00Z', '2026-07-01T09:00:00Z', FALSE, NOW())`,
			calendarID, fmt.Sprintf("windows-%d", i),
			"BEGIN:VCALENDAR\r\nBEGIN:VEVENT\r\nUID:windows\r\nDTSTART;TZID="+zone+":20260701T100000\r\nDTEND;TZID="+zone+":20260701T110000\r\nEND:VEVENT\r\nEND:VCALENDAR\r\n"); err != nil {
			t.Fatalf("seed Windows-named event: %v", err)
		}
	}
	reached := []string{
		"DTSTART;TZID=UTC:20260701T100000\r\n",
		"DTSTART;TZID=\" Europe/Berlin\":20260701T100000\r\n",
		"DTST\r\n ART;TZ\r\n ID=UTC:20260701T100000\r\n",
		"DTSTART;TZID=\"W. Europe Standard Time \":20260701T100000\r\n",
		"DTSTART;TZID=\"UTC\" :20260701T100000\r\n",
		"DTSTART;X-A=1;TZID=Europe/Berlin\t;VALUE=DATE-TIME:20260701T100000\r\n",
		"DTSTART;TZID=\" UTC \":20260701T100000\r\n",
		"DTSTART;TZID= W. Europe Standard Time:20260701T100000\r\n",
	}
	for i, property := range reached {
		if _, err := pool.Exec(`
INSERT INTO events (calendar_id, uid, resource_name, raw_ical, etag, dtstart, dtend, all_day, last_modified)
VALUES ($1, $2, $2 || '.ics', $3, 'etag', '2026-07-01T10:00:00Z', '2026-07-01T10:00:00Z', FALSE, NOW())`,
			calendarID, fmt.Sprintf("reached-%d", i),
			"BEGIN:VCALENDAR\r\nBEGIN:VEVENT\r\nUID:reached\r\n"+property+"END:VEVENT\r\nEND:VCALENDAR\r\n"); err != nil {
			t.Fatalf("seed reached event: %v", err)
		}
	}

	splits := 0
	walkPlan(explainStatement(t, pool, tzidRepairStatement(t), true), func(node map[string]any) {
		call, _ := node["Function Call"].(string)
		if !strings.HasPrefix(call, "regexp_split_to_table(") {
			return
		}
		splits++
		if loops, _ := node["Actual Loops"].(float64); int(loops) != len(reached) {
			t.Errorf("%s ran %v times, want once per body the rule reaches (%d)", call, loops, len(reached))
		}
	})
	if splits != 1 {
		t.Errorf("the plan splits bodies in %d places, want one", splits)
	}
}

// Where a VTIMEZONE ends depends on which components were open where it
// began, and a series with an override and an alarm per exception carries an
// END line for each. The TZID repair reads each of a body's lines a fixed
// number of times however many of them are END lines, so its cost stays
// linear in the body: no scan of a body's lines runs once per line.
func TestPostgres_V120MigrationScopesTimezonesInOnePassOverALargeSeries(t *testing.T) {
	pool := newV119ReleasePool(t)
	applyV120Migration(t, pool)
	calendarID := seedRepairCalendar(t, pool, "subject-large-series")

	const overrides = 300
	var body strings.Builder
	body.WriteString("BEGIN:VCALENDAR\r\nVERSION:2.0\r\nBEGIN:VTIMEZONE\r\nTZID:UTC\r\nBEGIN:STANDARD\r\nDTSTART:16010101T000000\r\nTZOFFSETFROM:+0000\r\nTZOFFSETTO:+0000\r\nEND:STANDARD\r\nEND:VTIMEZONE\r\n")
	body.WriteString("BEGIN:VEVENT\r\nUID:series\r\nDTSTART;TZID=UTC:20260101T100000\r\nRRULE:FREQ=DAILY\r\nEND:VEVENT\r\n")
	for i := range overrides {
		day := time.Date(2026, 1, 2, 10, 0, 0, 0, time.UTC).AddDate(0, 0, i).Format("20060102T150405")
		fmt.Fprintf(&body, "BEGIN:VEVENT\r\nUID:series\r\nRECURRENCE-ID;TZID=UTC:%s\r\nDTSTART;TZID=UTC:%s\r\nBEGIN:VALARM\r\nTRIGGER:-PT5M\r\nEND:VALARM\r\nEND:VEVENT\r\n", day, day)
	}
	body.WriteString("END:VCALENDAR\r\n")
	if _, err := pool.Exec(`
INSERT INTO events (calendar_id, uid, resource_name, raw_ical, etag, last_modified)
VALUES ($1, 'series', 'series.ics', $2, 'etag', NOW())`, calendarID, body.String()); err != nil {
		t.Fatalf("seed series: %v", err)
	}

	walkPlan(explainStatement(t, pool, tzidRepairStatement(t), true), func(node map[string]any) {
		if loops, _ := node["Actual Loops"].(float64); node["CTE Name"] == "line" && loops > 1 {
			t.Errorf("a scan of the body's lines as %v runs %v times for one %d-override body", node["Alias"], loops, overrides)
		}
	})
}
