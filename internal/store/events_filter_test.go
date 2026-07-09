package store

import (
	"context"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
)

// TestListForCalendarFilteredStartBound asserts the Start lower bound uses the
// indexed recurrence_until coalesce, so recurring events whose stored first
// instance predates the range are not dropped at the database.
func TestListForCalendarFilteredStartBound(t *testing.T) {
	start := time.Date(2026, 6, 1, 0, 0, 0, 0, time.UTC)

	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("failed to create sqlmock: %v", err)
	}
	defer db.Close()

	repo := &eventRepo{pool: db}
	mock.ExpectQuery(`AND COALESCE\(recurrence_until, dtend, dtstart\) >= \$2`).
		WithArgs(int64(1), start).
		WillReturnRows(sqlmock.NewRows([]string{
			"id", "calendar_id", "uid", "resource_name", "raw_ical", "etag",
			"summary", "description", "location", "dtstart", "dtend", "all_day", "last_modified",
		}))

	if _, err := repo.ListForCalendarFiltered(context.Background(), 1, EventFilter{Start: &start}); err != nil {
		t.Fatalf("ListForCalendarFiltered: %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet expectations: %v", err)
	}
}

// TestListForCalendarFilteredEndBound asserts the End upper bound uses the
// indexed recurrence_start coalesce, so recurring events with RDATE/override
// instances before the master dtstart are not dropped at the database.
func TestListForCalendarFilteredEndBound(t *testing.T) {
	end := time.Date(2026, 6, 1, 0, 0, 0, 0, time.UTC)

	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("failed to create sqlmock: %v", err)
	}
	defer db.Close()

	repo := &eventRepo{pool: db}
	mock.ExpectQuery(`AND COALESCE\(recurrence_start, dtstart\) <= \$2`).
		WithArgs(int64(1), end).
		WillReturnRows(sqlmock.NewRows([]string{
			"id", "calendar_id", "uid", "resource_name", "raw_ical", "etag",
			"summary", "description", "location", "dtstart", "dtend", "all_day", "last_modified",
		}))

	if _, err := repo.ListForCalendarFiltered(context.Background(), 1, EventFilter{End: &end}); err != nil {
		t.Fatalf("ListForCalendarFiltered: %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet expectations: %v", err)
	}
}

func TestRecurrenceBackfillScopesToEventLikeComponents(t *testing.T) {
	sql, err := os.ReadFile("../../migrations/v1.1.7.sql")
	if err != nil {
		t.Fatalf("read migration: %v", err)
	}
	migration := string(sql)
	if strings.Contains(migration, "raw_ical ILIKE '%RRULE%'") || strings.Contains(migration, "raw_ical ILIKE '%RDATE%'") {
		t.Fatal("recurrence backfill must not match RRULE/RDATE globally because VTIMEZONE rules would be false positives")
	}
	for _, component := range []string{"BEGIN:VEVENT", "BEGIN:VTODO", "BEGIN:VJOURNAL"} {
		if !strings.Contains(migration, component) {
			t.Fatalf("expected recurrence backfill to inspect %s components", component)
		}
	}
	if !strings.Contains(migration, "regexp_matches(events.raw_ical") {
		t.Fatal("expected recurrence backfill to inspect component bodies in SQL")
	}
	if !strings.Contains(migration, "recurrence_start = '1900-01-01T00:00:00Z'") {
		t.Fatal("expected recurring backfill to use a safe lower start bound")
	}
	if !strings.Contains(migration, "idx_events_recurrence_start") {
		t.Fatal("expected migration to add a recurrence_start index")
	}
}

func mustParseRFC3339(t *testing.T, s string) time.Time {
	t.Helper()
	parsed, err := time.Parse(time.RFC3339, s)
	if err != nil {
		t.Fatalf("parse %q: %v", s, err)
	}
	return parsed
}

func TestRecurrenceUntilFromICal(t *testing.T) {
	base := "BEGIN:VCALENDAR\r\nBEGIN:VEVENT\r\nUID:e1\r\nDTSTART:20260101T090000Z\r\nDTEND:20260101T100000Z\r\n%sEND:VEVENT\r\nEND:VCALENDAR\r\n"

	// isSentinel and notBefore express the safety invariant: a precise bound must
	// be at or after the true last-instance end, and an unbounded/unsafe rule must
	// resolve to the sentinel (never an underestimate).
	isSentinel := func(t *testing.T, got *time.Time) {
		t.Helper()
		if got == nil || !got.Equal(recurrenceUntilSentinel) {
			t.Fatalf("expected sentinel, got %v", got)
		}
	}
	notBefore := func(t *testing.T, got *time.Time, trueEnd time.Time) {
		t.Helper()
		if got == nil {
			t.Fatal("expected a bounded value, got nil")
		}
		if got.Before(trueEnd) {
			t.Fatalf("bound %v underestimates true last-instance end %v", got, trueEnd)
		}
		if got.Equal(recurrenceUntilSentinel) {
			t.Fatalf("expected a precise bound, got sentinel")
		}
	}

	t.Run("non-recurring yields nil", func(t *testing.T) {
		if got := recurrenceUntilFromICal(fmtICal(base, "")); got != nil {
			t.Fatalf("expected nil for non-recurring, got %v", got)
		}
	})

	t.Run("unbounded yields sentinel", func(t *testing.T) {
		isSentinel(t, recurrenceUntilFromICal(fmtICal(base, "RRULE:FREQ=WEEKLY\r\n")))
	})

	t.Run("UNTIL is bounded exactly by the ceiling plus duration", func(t *testing.T) {
		got := recurrenceUntilFromICal(fmtICal(base, "RRULE:FREQ=WEEKLY;UNTIL=20260201T090000Z\r\n"))
		// Last instance starts no later than UNTIL and ends one hour later.
		want := mustParseRFC3339(t, "2026-02-01T10:00:00Z")
		if got == nil || !got.Equal(want) {
			t.Fatalf("want %v, got %v", want, got)
		}
	})

	t.Run("UNTIL without DTEND uses recurring event default duration", func(t *testing.T) {
		ical := "BEGIN:VCALENDAR\r\nBEGIN:VEVENT\r\nUID:e1\r\nDTSTART:20260101T090000Z\r\n" +
			"RRULE:FREQ=DAILY;UNTIL=20260103T090000Z\r\nEND:VEVENT\r\nEND:VCALENDAR\r\n"
		got := recurrenceUntilFromICal(ical)
		want := mustParseRFC3339(t, "2026-01-03T10:00:00Z")
		if got == nil || !got.Equal(want) {
			t.Fatalf("want %v, got %v", want, got)
		}
	})

	t.Run("all-day DTSTART without DTEND uses one-day duration", func(t *testing.T) {
		ical := "BEGIN:VCALENDAR\r\nBEGIN:VEVENT\r\nUID:e1\r\nDTSTART;VALUE=DATE:20260101\r\n" +
			"RRULE:FREQ=DAILY;UNTIL=20260103\r\nEND:VEVENT\r\nEND:VCALENDAR\r\n"
		got := recurrenceUntilFromICal(ical)
		want := mustParseRFC3339(t, "2026-01-04T00:00:00Z")
		if got == nil || !got.Equal(want) {
			t.Fatalf("want %v, got %v", want, got)
		}
	})

	t.Run("UNTIL uses DURATION when DTEND is absent", func(t *testing.T) {
		ical := "BEGIN:VCALENDAR\r\nBEGIN:VEVENT\r\nUID:e1\r\nDTSTART:20260101T090000Z\r\n" +
			"DURATION:PT2H\r\nRRULE:FREQ=DAILY;UNTIL=20260103T090000Z\r\nEND:VEVENT\r\nEND:VCALENDAR\r\n"
		got := recurrenceUntilFromICal(ical)
		want := mustParseRFC3339(t, "2026-01-03T11:00:00Z")
		if got == nil || !got.Equal(want) {
			t.Fatalf("want %v, got %v", want, got)
		}
	})

	t.Run("UNTIL is safe even with BY parts", func(t *testing.T) {
		got := recurrenceUntilFromICal(fmtICal(base, "RRULE:FREQ=MONTHLY;BYMONTHDAY=31;UNTIL=20260601T090000Z\r\n"))
		want := mustParseRFC3339(t, "2026-06-01T10:00:00Z")
		if got == nil || !got.Equal(want) {
			t.Fatalf("want %v, got %v", want, got)
		}
	})

	t.Run("DAILY COUNT is bounded", func(t *testing.T) {
		// 3 daily 1h instances: last starts Jan 3 09:00, ends Jan 3 10:00.
		got := recurrenceUntilFromICal(fmtICal(base, "RRULE:FREQ=DAILY;COUNT=3\r\n"))
		notBefore(t, got, mustParseRFC3339(t, "2026-01-03T10:00:00Z"))
	})

	t.Run("WEEKLY COUNT honors INTERVAL", func(t *testing.T) {
		// 3 instances every 2 weeks: last starts Jan 1 + 28 days = Jan 29 09:00.
		got := recurrenceUntilFromICal(fmtICal(base, "RRULE:FREQ=WEEKLY;COUNT=3;INTERVAL=2\r\n"))
		notBefore(t, got, mustParseRFC3339(t, "2026-01-29T10:00:00Z"))
	})

	t.Run("MONTHLY COUNT falls back to sentinel", func(t *testing.T) {
		// MONTHLY can skip periods (e.g. the 31st), so stepping is unsafe.
		isSentinel(t, recurrenceUntilFromICal(fmtICal(base, "RRULE:FREQ=MONTHLY;COUNT=12\r\n")))
	})

	t.Run("YEARLY COUNT falls back to sentinel", func(t *testing.T) {
		isSentinel(t, recurrenceUntilFromICal(fmtICal(base, "RRULE:FREQ=YEARLY;COUNT=5\r\n")))
	})

	t.Run("BY part on DAILY/WEEKLY falls back to sentinel", func(t *testing.T) {
		// DAILY;BYDAY thins instances so the COUNTth lands later than naive stepping.
		isSentinel(t, recurrenceUntilFromICal(fmtICal(base, "RRULE:FREQ=DAILY;BYDAY=MO;COUNT=5\r\n")))
		isSentinel(t, recurrenceUntilFromICal(fmtICal(base, "RRULE:FREQ=WEEKLY;BYDAY=MO,WE,FR;COUNT=10\r\n")))
	})

	t.Run("sub-daily frequency falls back to sentinel", func(t *testing.T) {
		isSentinel(t, recurrenceUntilFromICal(fmtICal(base, "RRULE:FREQ=HOURLY;COUNT=100\r\n")))
	})

	t.Run("lowercase property name is recognized", func(t *testing.T) {
		got := recurrenceUntilFromICal(fmtICal(base, "rrule:FREQ=DAILY;COUNT=3\r\n"))
		notBefore(t, got, mustParseRFC3339(t, "2026-01-03T10:00:00Z"))
	})

	t.Run("RDATE falls back to sentinel", func(t *testing.T) {
		isSentinel(t, recurrenceUntilFromICal(fmtICal(base, "RDATE:20270101T090000Z\r\n")))
	})

	t.Run("moved override extends upper bound", func(t *testing.T) {
		ical := "BEGIN:VCALENDAR\r\n" +
			"BEGIN:VEVENT\r\nUID:e1\r\nDTSTART:20260101T090000Z\r\nDTEND:20260101T100000Z\r\nRRULE:FREQ=DAILY;COUNT=2\r\nEND:VEVENT\r\n" +
			"BEGIN:VEVENT\r\nUID:e1\r\nRECURRENCE-ID:20260102T090000Z\r\nDTSTART:20260201T150000Z\r\nDTEND:20260201T160000Z\r\nEND:VEVENT\r\n" +
			"END:VCALENDAR\r\n"
		notBefore(t, recurrenceUntilFromICal(ical), mustParseRFC3339(t, "2026-02-01T16:00:00Z"))
	})

	t.Run("this-and-future override uses sentinel upper bound", func(t *testing.T) {
		ical := "BEGIN:VCALENDAR\r\n" +
			"BEGIN:VEVENT\r\nUID:e1\r\nDTSTART:20260101T090000Z\r\nDTEND:20260101T100000Z\r\nRRULE:FREQ=DAILY;COUNT=5\r\nEND:VEVENT\r\n" +
			"BEGIN:VEVENT\r\nUID:e1\r\nRECURRENCE-ID;RANGE=THISANDFUTURE:20260102T090000Z\r\nDTSTART:20260201T150000Z\r\nDTEND:20260201T160000Z\r\nEND:VEVENT\r\n" +
			"END:VCALENDAR\r\n"
		isSentinel(t, recurrenceUntilFromICal(ical))
	})

	t.Run("early RDATE extends lower bound", func(t *testing.T) {
		ical := "BEGIN:VCALENDAR\r\nBEGIN:VEVENT\r\nUID:e1\r\nDTSTART:20260610T090000Z\r\nDTEND:20260610T100000Z\r\nRDATE:20260601T090000Z\r\nEND:VEVENT\r\nEND:VCALENDAR\r\n"
		got := recurrenceStartFromICal(ical)
		want := mustParseRFC3339(t, "2026-06-01T09:00:00Z")
		if got == nil || !got.Equal(want) {
			t.Fatalf("want recurrence_start %v, got %v", want, got)
		}
	})

	t.Run("RRULE in VTIMEZONE before the VEVENT is ignored", func(t *testing.T) {
		// A VTIMEZONE DST rule with a past UNTIL must not be read as the event's
		// recurrence -- the event here is non-recurring, so the result is nil.
		ical := "BEGIN:VCALENDAR\r\n" +
			"BEGIN:VTIMEZONE\r\nTZID:America/New_York\r\n" +
			"BEGIN:DAYLIGHT\r\nRRULE:FREQ=YEARLY;BYMONTH=4;BYDAY=1SU;UNTIL=20060402T070000Z\r\n" +
			"DTSTART:19870405T020000\r\nEND:DAYLIGHT\r\nEND:VTIMEZONE\r\n" +
			"BEGIN:VEVENT\r\nUID:e1\r\nDTSTART:20260101T090000Z\r\nDTEND:20260101T100000Z\r\nEND:VEVENT\r\n" +
			"END:VCALENDAR\r\n"
		if got := recurrenceUntilFromICal(ical); got != nil {
			t.Fatalf("VTIMEZONE RRULE leaked into event recurrence: got %v", got)
		}
	})

	t.Run("RRULE in VTIMEZONE does not shadow the VEVENT RRULE", func(t *testing.T) {
		ical := "BEGIN:VCALENDAR\r\n" +
			"BEGIN:VTIMEZONE\r\nTZID:America/New_York\r\n" +
			"BEGIN:DAYLIGHT\r\nRRULE:FREQ=YEARLY;UNTIL=20060402T070000Z\r\nDTSTART:19870405T020000\r\nEND:DAYLIGHT\r\nEND:VTIMEZONE\r\n" +
			"BEGIN:VEVENT\r\nUID:e1\r\nDTSTART:20260101T090000Z\r\nDTEND:20260101T100000Z\r\n" +
			"RRULE:FREQ=DAILY;COUNT=3\r\nEND:VEVENT\r\n" +
			"END:VCALENDAR\r\n"
		notBefore(t, recurrenceUntilFromICal(ical), mustParseRFC3339(t, "2026-01-03T10:00:00Z"))
	})

	t.Run("RRULE in later VEVENT is considered", func(t *testing.T) {
		ical := "BEGIN:VCALENDAR\r\n" +
			"BEGIN:VEVENT\r\nUID:e1\r\nRECURRENCE-ID:20260102T090000Z\r\nDTSTART:20260102T110000Z\r\nDTEND:20260102T120000Z\r\nEND:VEVENT\r\n" +
			"BEGIN:VEVENT\r\nUID:e1\r\nDTSTART:20260101T090000Z\r\nDTEND:20260101T100000Z\r\nRRULE:FREQ=DAILY;COUNT=3\r\nEND:VEVENT\r\n" +
			"END:VCALENDAR\r\n"
		notBefore(t, recurrenceUntilFromICal(ical), mustParseRFC3339(t, "2026-01-03T10:00:00Z"))
	})
}

func fmtICal(template, rrule string) string {
	return strings.Replace(template, "%s", rrule, 1)
}
