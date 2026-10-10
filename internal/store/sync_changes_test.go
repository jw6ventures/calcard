package store

import (
	"context"
	"fmt"
	"math"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/lib/pq"
)

var eventChangeColumns = []string{"id", "calendar_id", "uid", "resource_name", "raw_ical", "etag", "summary", "description", "location", "dtstart", "dtend", "all_day", "last_modified"}

var tombstoneChangeColumns = []string{"id", "resource_type", "collection_id", "uid", "resource_name", "deleted_at"}

// One page merges the two tables into stream order: members before tombstones
// stamped at the same instant, and each kind by id. A tombstone for a name the
// collection holds again counts toward the page and its cursor without being
// reported, and the row each table reads past the page decides More.
func TestChangeStreamMergesMembersAndTombstonesInStreamOrder(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New() error = %v", err)
	}
	defer db.Close()
	repo := &eventRepo{pool: db}
	t0 := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	t1 := t0.Add(time.Second)
	after := SyncCursor{Stamp: t0, ID: 4}

	mock.ExpectBegin()
	mock.ExpectQuery(regexp.QuoteMeta(eventChangeStream.members)).
		WithArgs(int64(7), t0, int64(4), 5).
		WillReturnRows(sqlmock.NewRows(eventChangeColumns).
			AddRow(int64(5), int64(7), "e5", "e5", "BEGIN:VCALENDAR", "etag", nil, nil, nil, nil, nil, false, t0).
			AddRow(int64(9), int64(7), "e9", "rebound", "BEGIN:VCALENDAR", "etag", nil, nil, nil, nil, nil, false, t1).
			AddRow(int64(2), int64(7), "e2", "e2", "BEGIN:VCALENDAR", "etag", nil, nil, nil, nil, nil, false, t1.Add(time.Second)))
	mock.ExpectQuery(regexp.QuoteMeta(tombstonesAfterQuery)).
		WithArgs("event", int64(7), t0, int64(0), 5).
		WillReturnRows(sqlmock.NewRows(tombstoneChangeColumns).
			AddRow(int64(3), "event", int64(7), "gone", "gone", t0).
			AddRow(int64(8), "event", int64(7), "old", "rebound", t1))
	mock.ExpectQuery(regexp.QuoteMeta(eventChangeStream.liveNames)).
		WithArgs(int64(7), pq.Array([]string{"gone", "rebound"})).
		WillReturnRows(sqlmock.NewRows([]string{"name"}).AddRow("rebound"))
	mock.ExpectCommit()

	page, err := repo.ListChangesAfter(context.Background(), 7, after, true, 4)
	if err != nil {
		t.Fatalf("ListChangesAfter() error = %v", err)
	}
	// Stream order: e5 (t0, member), gone (t0, tombstone), e9 (t1, member),
	// rebound (t1, tombstone) -- the page of four -- then e2.
	if len(page.Members) != 2 || page.Members[0].ID != 5 || page.Members[1].ID != 9 {
		t.Fatalf("Members = %+v, want e5 then e9", page.Members)
	}
	if len(page.Removals) != 1 || page.Removals[0].ID != 3 {
		t.Fatalf("Removals = %+v, want only the tombstone whose name nothing holds", page.Removals)
	}
	if want := (SyncCursor{Stamp: t1, Removal: true, ID: 8}); page.Last != want {
		t.Fatalf("Last = %+v, want %+v", page.Last, want)
	}
	if !page.More {
		t.Fatal("More = false with e2 still unread")
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("sql expectations: %v", err)
	}
}

// A cursor on a tombstone has passed every member stamped at its instant, and
// a first page reads no tombstones at all.
func TestChangeStreamResumesPastATombstoneCursor(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New() error = %v", err)
	}
	defer db.Close()
	repo := &contactRepo{pool: db}
	t0 := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)

	mock.ExpectBegin()
	mock.ExpectQuery(regexp.QuoteMeta(contactChangeStream.members)).
		WithArgs(int64(5), t0, int64(math.MaxInt64), 3).
		WillReturnRows(sqlmock.NewRows([]string{"id", "address_book_id", "uid", "resource_name", "raw_vcard", "etag", "display_name", "primary_email", "birthday", "last_modified"}))
	mock.ExpectQuery(regexp.QuoteMeta(tombstonesAfterQuery)).
		WithArgs("contact", int64(5), t0, int64(12), 3).
		WillReturnRows(sqlmock.NewRows(tombstoneChangeColumns))
	mock.ExpectCommit()
	after := SyncCursor{Stamp: t0, Removal: true, ID: 12}
	page, err := repo.ListChangesAfter(context.Background(), 5, after, true, 2)
	if err != nil {
		t.Fatalf("ListChangesAfter() error = %v", err)
	}
	if page.More || page.Last != after || len(page.Members)+len(page.Removals) != 0 {
		t.Fatalf("empty page = %+v, want no changes and the cursor it was read after", page)
	}

	mock.ExpectBegin()
	mock.ExpectQuery(regexp.QuoteMeta(contactChangeStream.members)).
		WithArgs(int64(5), time.Time{}, int64(0), 3).
		WillReturnRows(sqlmock.NewRows([]string{"id", "address_book_id", "uid", "resource_name", "raw_vcard", "etag", "display_name", "primary_email", "birthday", "last_modified"}))
	mock.ExpectCommit()
	if _, err := repo.ListChangesAfter(context.Background(), 5, SyncCursor{}, false, 2); err != nil {
		t.Fatalf("ListChangesAfter(first page) error = %v", err)
	}

	if page, err := repo.ListChangesAfter(context.Background(), 5, after, true, 0); err != nil || page.Last != after {
		t.Fatalf("ListChangesAfter(limit 0) = %+v, %v; want no read", page, err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("sql expectations: %v", err)
	}
}

// changeStreamPosition is where one change sits in the stream.
func changeStreamPosition(member *Event, tombstone *DeletedResource) SyncCursor {
	if member != nil {
		return SyncCursor{Stamp: member.LastModified, ID: member.ID}
	}
	return SyncCursor{Stamp: tombstone.DeletedAt, Removal: true, ID: tombstone.ID}
}

func changeStreamBefore(a, b SyncCursor) bool {
	if !a.Stamp.Equal(b.Stamp) {
		return a.Stamp.Before(b.Stamp)
	}
	if a.Removal != b.Removal {
		return !a.Removal
	}
	return a.ID < b.ID
}

// Paging the stream a few changes at a time has to visit every change once,
// in stream order, through runs of equal stamps -- the shape a migration or a
// v1.1.x bulk write leaves -- and across the boundary between members and
// tombstones.
func TestPostgres_ChangeStreamPagesThroughEqualStamps(t *testing.T) {
	pool := newPostgresPool(t)
	s := New(pool)
	ctx := context.Background()
	_, calendarID := newPostgresCalendar(t, s, "change-stream")
	_, otherID := newPostgresCalendar(t, s, "change-stream-other")

	for i := 1; i <= 12; i++ {
		name := fmt.Sprintf("event-%02d", i)
		if _, err := s.Events.Upsert(ctx, Event{CalendarID: calendarID, UID: name, ResourceName: name, RawICAL: postgresCalendarObject(name, name), ETag: name}); err != nil {
			t.Fatalf("create %s: %v", name, err)
		}
	}
	if _, err := s.Events.Upsert(ctx, Event{CalendarID: otherID, UID: "elsewhere", ResourceName: "elsewhere", RawICAL: postgresCalendarObject("elsewhere", "x"), ETag: "x"}); err != nil {
		t.Fatalf("create the other calendar's event: %v", err)
	}
	// Eight of the members share one stamp, as a migration that stamps with
	// NOW() leaves them.
	if _, err := pool.ExecContext(ctx, `ALTER TABLE events DISABLE TRIGGER trg_events_touch_last_modified`); err != nil {
		t.Fatalf("disable stamping: %v", err)
	}
	if _, err := pool.ExecContext(ctx, `UPDATE events SET last_modified = '2025-01-01T00:00:00Z' WHERE calendar_id = $1 AND resource_name <= 'event-08'`, calendarID); err != nil {
		t.Fatalf("stamp a run: %v", err)
	}
	if _, err := pool.ExecContext(ctx, `ALTER TABLE events ENABLE TRIGGER trg_events_touch_last_modified`); err != nil {
		t.Fatalf("enable stamping: %v", err)
	}
	for _, uid := range []string{"event-03", "event-10"} {
		if err := s.Events.DeleteByUID(ctx, calendarID, uid); err != nil {
			t.Fatalf("delete %s: %v", uid, err)
		}
	}
	// A name bound again after its deletion is held by the new member, which
	// is the authority over its href.
	if _, err := s.Events.Upsert(ctx, Event{CalendarID: calendarID, UID: "event-10-again", ResourceName: "event-10", RawICAL: postgresCalendarObject("event-10-again", "again"), ETag: "again"}); err != nil {
		t.Fatalf("rebind event-10: %v", err)
	}

	whole, err := s.Events.ListChangesAfter(ctx, calendarID, SyncCursor{}, true, 1000)
	if err != nil || whole.More {
		t.Fatalf("whole stream = %+v, %v", whole, err)
	}
	if len(whole.Members) != 11 || len(whole.Removals) != 1 || whole.Removals[0].UID != "event-03" {
		t.Fatalf("whole stream holds %d members and removals %+v, want 11 members and event-03's removal", len(whole.Members), whole.Removals)
	}

	var visited []SyncCursor
	var removed []string
	after := SyncCursor{}
	for pages := 0; ; pages++ {
		if pages > 20 {
			t.Fatal("paging did not reach the end of the stream")
		}
		page, err := s.Events.ListChangesAfter(ctx, calendarID, after, true, 3)
		if err != nil {
			t.Fatalf("page %d: %v", pages, err)
		}
		var positions []SyncCursor
		for i := range page.Members {
			if page.Members[i].CalendarID != calendarID {
				t.Fatalf("page %d holds another calendar's event %+v", pages, page.Members[i])
			}
			positions = append(positions, changeStreamPosition(&page.Members[i], nil))
		}
		for i := range page.Removals {
			positions = append(positions, changeStreamPosition(nil, &page.Removals[i]))
			removed = append(removed, page.Removals[i].UID)
		}
		for _, position := range positions {
			if !changeStreamBefore(after, position) || changeStreamBefore(page.Last, position) {
				t.Fatalf("page %d holds %+v outside (%+v, %+v]", pages, position, after, page.Last)
			}
		}
		visited = append(visited, positions...)
		if !page.More {
			break
		}
		if !changeStreamBefore(after, page.Last) {
			t.Fatalf("page %d did not move the cursor past %+v", pages, after)
		}
		after = page.Last
	}
	if len(visited) != 12 {
		t.Fatalf("paging visited %d changes, want the 11 members and 1 reported removal", len(visited))
	}
	seen := map[SyncCursor]bool{}
	for _, position := range visited {
		if seen[position] {
			t.Fatalf("paging visited %+v twice", position)
		}
		seen[position] = true
	}
	if strings.Join(removed, ",") != "event-03" {
		t.Fatalf("removals = %v, want only event-03: event-10's name is held again", removed)
	}
}

// The member read is one bound on idx_events_change_stream, which also
// provides its order: a plan that sorted would read the whole calendar to
// answer one page.
func TestPostgres_ChangeStreamReadsAPageFromItsIndex(t *testing.T) {
	pool := newPostgresPool(t)
	s := New(pool)
	ctx := context.Background()
	_, calendarID := newPostgresCalendar(t, s, "change-stream-plan")
	_, otherID := newPostgresCalendar(t, s, "change-stream-plan-other")
	if _, err := pool.ExecContext(ctx, `
INSERT INTO events (calendar_id, uid, resource_name, raw_ical, etag)
SELECT CASE WHEN n % 4 = 0 THEN $1::bigint ELSE $2::bigint END, 'uid-' || n, 'name-' || n, 'BEGIN:VCALENDAR', 'etag'
FROM generate_series(1, 4000) AS n`, calendarID, otherID); err != nil {
		t.Fatalf("seed events: %v", err)
	}
	if _, err := pool.ExecContext(ctx, `ANALYZE events; ANALYZE deleted_resources`); err != nil {
		t.Fatalf("analyze: %v", err)
	}

	literal := strings.NewReplacer(
		"$1", fmt.Sprint(calendarID), "$2", "'2025-01-01T00:00:00Z'::timestamptz", "$3", "0", "$4", "101",
	).Replace(eventChangeStream.members)
	plan := explainStatement(t, pool, literal, false)
	usesIndex := false
	walkPlan(plan, func(node map[string]any) {
		if node["Node Type"] == "Sort" || node["Node Type"] == "Incremental Sort" {
			t.Errorf("the member read sorts: %v", plan)
		}
		if node["Index Name"] == "idx_events_change_stream" {
			usesIndex = true
		}
	})
	if !usesIndex {
		t.Errorf("the member read does not use idx_events_change_stream: %v", plan)
	}
}
