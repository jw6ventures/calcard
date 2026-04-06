package store

import (
	"context"
	"database/sql"
	"errors"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/lib/pq"
)

func TestCalendarRepoCreateAndOwnerScopedMutations(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New() error = %v", err)
	}
	defer db.Close()

	repo := &calendarRepo{pool: db}
	now := time.Now().UTC()
	description := "Work"
	timezone := "America/Chicago"
	color := "#00aa00"

	mock.ExpectQuery(regexp.QuoteMeta(`INSERT INTO calendars (user_id, name, slug, description, timezone, color) VALUES ($1, $2, $3, $4, $5, $6) RETURNING id, user_id, name, slug, description, timezone, color, ctag, created_at, updated_at`)).
		WithArgs(int64(4), "Primary", nil, &description, &timezone, &color).
		WillReturnRows(sqlmock.NewRows([]string{"id", "user_id", "name", "slug", "description", "timezone", "color", "ctag", "created_at", "updated_at"}).
			AddRow(int64(10), int64(4), "Primary", nil, description, timezone, color, int64(3), now, now))

	created, err := repo.Create(context.Background(), Calendar{
		UserID:      4,
		Name:        "Primary",
		Description: &description,
		Timezone:    &timezone,
		Color:       &color,
	})
	if err != nil {
		t.Fatalf("Create() error = %v", err)
	}
	if created.ID != 10 || created.Description == nil || *created.Description != description || created.Color == nil || *created.Color != color {
		t.Fatalf("Create() = %#v", created)
	}

	mock.ExpectExec(regexp.QuoteMeta(`UPDATE calendars SET name=$1, description=$2, timezone=$3, updated_at=NOW() WHERE id=$4 AND user_id=$5`)).
		WithArgs("Renamed", &description, &timezone, int64(10), int64(4)).
		WillReturnResult(sqlmock.NewResult(0, 1))
	if err := repo.Update(context.Background(), 4, 10, "Renamed", &description, &timezone); err != nil {
		t.Fatalf("Update() error = %v", err)
	}

	mock.ExpectExec(regexp.QuoteMeta(`UPDATE calendars SET name=$1, updated_at=NOW() WHERE id=$2 AND user_id=$3`)).
		WithArgs("Renamed Again", int64(99), int64(4)).
		WillReturnResult(sqlmock.NewResult(0, 0))
	if err := repo.Rename(context.Background(), 4, 99, "Renamed Again"); err != ErrNotFound {
		t.Fatalf("Rename() error = %v, want ErrNotFound", err)
	}

	mock.ExpectExec(regexp.QuoteMeta(`DELETE FROM calendars WHERE id=$1 AND user_id=$2`)).
		WithArgs(int64(99), int64(4)).
		WillReturnResult(sqlmock.NewResult(0, 0))
	if err := repo.Delete(context.Background(), 4, 99); err != ErrNotFound {
		t.Fatalf("Delete() error = %v, want ErrNotFound", err)
	}

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("sql expectations: %v", err)
	}
}

func TestCalendarRepoAccessQueriesReturnNilWhenMissing(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New() error = %v", err)
	}
	defer db.Close()

	repo := &calendarRepo{pool: db}

	mock.ExpectQuery(regexp.QuoteMeta(`SELECT id, user_id, name, slug, description, timezone, color, ctag, created_at, updated_at FROM calendars WHERE id=$1`)).
		WithArgs(int64(404)).
		WillReturnError(sql.ErrNoRows)
	got, err := repo.GetByID(context.Background(), 404)
	if err != nil {
		t.Fatalf("GetByID() error = %v", err)
	}
	if got != nil {
		t.Fatalf("GetByID() = %#v, want nil", got)
	}

	mock.ExpectQuery(`(?s)`+
		regexp.QuoteMeta(`SELECT c.id, c.user_id, c.name, c.slug, c.description, c.timezone, c.color, c.ctag, c.created_at, c.updated_at,`)+
		`.*acl_entries.*`+
		regexp.QuoteMeta(`FROM calendars c`)+
		`.*`+
		regexp.QuoteMeta(`WHERE c.id = $1`)).
		WithArgs(int64(12), int64(4)).
		WillReturnError(sql.ErrNoRows)
	accessible, err := repo.GetAccessible(context.Background(), 12, 4)
	if err != nil {
		t.Fatalf("GetAccessible() error = %v", err)
	}
	if accessible != nil {
		t.Fatalf("GetAccessible() = %#v, want nil", accessible)
	}

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("sql expectations: %v", err)
	}
}

func TestCalendarAccessibleReposUseACLs(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New() error = %v", err)
	}
	defer db.Close()

	calendarRepo := &calendarRepo{pool: db}
	now := time.Now().UTC()

	mock.ExpectQuery(`(?s)SELECT c.id, c.user_id, c.name, c.slug, c.description, c.timezone, c.color, c.ctag, c.created_at, c.updated_at,.*FROM calendars c.*acl_entries.*ORDER BY shared, name`).
		WithArgs(int64(4)).
		WillReturnRows(sqlmock.NewRows([]string{"id", "user_id", "name", "slug", "description", "timezone", "color", "ctag", "created_at", "updated_at", "owner_email", "shared", "can_read", "can_read_free_busy", "can_write", "can_write_content", "can_write_properties", "can_bind", "can_unbind"}).
			AddRow(int64(1), int64(4), "Owned", nil, nil, nil, nil, int64(1), now, now, "owner@example.com", false, true, true, true, true, true, true, true).
			AddRow(int64(2), int64(9), "Shared", "shared", "Desc", "UTC", "#123456", int64(3), now, now, "other@example.com", true, true, false, false, false, false, true, false))

	accessible, err := calendarRepo.ListAccessible(context.Background(), 4)
	if err != nil {
		t.Fatalf("ListAccessible() error = %v", err)
	}
	if len(accessible) != 2 || accessible[0].Shared || !accessible[0].Editor || !accessible[1].Shared || accessible[1].Editor {
		t.Fatalf("ListAccessible() = %#v", accessible)
	}
	if accessible[1].Slug == nil || *accessible[1].Slug != "shared" || accessible[1].Color == nil || *accessible[1].Color != "#123456" {
		t.Fatalf("ListAccessible() optional fields = %#v", accessible[1])
	}
	if !accessible[1].Privileges.Read || !accessible[1].Privileges.Bind {
		t.Fatalf("ListAccessible() privileges = %#v, want read+bind", accessible[1].Privileges)
	}
	if accessible[1].Privileges.WriteContent || accessible[1].Privileges.Unbind {
		t.Fatalf("ListAccessible() privileges = %#v, unexpected write-content/unbind", accessible[1].Privileges)
	}

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("sql expectations: %v", err)
	}
}

func TestCalendarAccessibleReposIncludeReadFreeBusyOnlyCalendars(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New() error = %v", err)
	}
	defer db.Close()

	repo := &calendarRepo{pool: db}
	now := time.Now().UTC()

	mock.ExpectQuery(`(?s)SELECT c.id, c.user_id, c.name, c.slug, c.description, c.timezone, c.color, c.ctag, c.created_at, c.updated_at,.*FROM calendars c.*WHERE c.user_id = \$1.*read-free-busy.*ORDER BY shared, name`).
		WithArgs(int64(4)).
		WillReturnRows(sqlmock.NewRows([]string{"id", "user_id", "name", "slug", "description", "timezone", "color", "ctag", "created_at", "updated_at", "owner_email", "shared", "can_read", "can_read_free_busy", "can_write", "can_write_content", "can_write_properties", "can_bind", "can_unbind"}).
			AddRow(int64(7), int64(9), "Busy Only", nil, nil, nil, nil, int64(5), now, now, "owner@example.com", true, false, true, false, false, false, false, false))

	accessible, err := repo.ListAccessible(context.Background(), 4)
	if err != nil {
		t.Fatalf("ListAccessible() error = %v", err)
	}
	if len(accessible) != 1 {
		t.Fatalf("ListAccessible() len = %d, want 1", len(accessible))
	}
	if accessible[0].Privileges.Read {
		t.Fatalf("ListAccessible() read = true, want false for free-busy-only access")
	}
	if !accessible[0].Privileges.ReadFreeBusy {
		t.Fatalf("ListAccessible() readFreeBusy = false, want true")
	}
	if accessible[0].Editor {
		t.Fatalf("ListAccessible() editor = true, want false")
	}

	mock.ExpectQuery(`(?s)SELECT c.id, c.user_id, c.name, c.slug, c.description, c.timezone, c.color, c.ctag, c.created_at, c.updated_at,.*FROM calendars c.*WHERE c.id = \$1.*read-free-busy.*`).
		WithArgs(int64(7), int64(4)).
		WillReturnRows(sqlmock.NewRows([]string{"id", "user_id", "name", "slug", "description", "timezone", "color", "ctag", "created_at", "updated_at", "owner_email", "shared", "can_read", "can_read_free_busy", "can_write", "can_write_content", "can_write_properties", "can_bind", "can_unbind"}).
			AddRow(int64(7), int64(9), "Busy Only", nil, nil, nil, nil, int64(5), now, now, "owner@example.com", true, false, true, false, false, false, false, false))

	got, err := repo.GetAccessible(context.Background(), 7, 4)
	if err != nil {
		t.Fatalf("GetAccessible() error = %v", err)
	}
	if got == nil {
		t.Fatal("GetAccessible() = nil, want calendar access")
	}
	if got.Privileges.Read {
		t.Fatalf("GetAccessible() read = true, want false for free-busy-only access")
	}
	if !got.Privileges.ReadFreeBusy {
		t.Fatalf("GetAccessible() readFreeBusy = false, want true")
	}

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("sql expectations: %v", err)
	}
}

func TestCalendarAccessibleReposIncludeBindOnlyCalendars(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New() error = %v", err)
	}
	defer db.Close()

	repo := &calendarRepo{pool: db}
	now := time.Now().UTC()

	mock.ExpectQuery(`(?s)SELECT c.id, c.user_id, c.name, c.slug, c.description, c.timezone, c.color, c.ctag, c.created_at, c.updated_at,.*FROM calendars c.*WHERE c.user_id = \$1.*bind.*ORDER BY shared, name`).
		WithArgs(int64(4)).
		WillReturnRows(sqlmock.NewRows([]string{"id", "user_id", "name", "slug", "description", "timezone", "color", "ctag", "created_at", "updated_at", "owner_email", "shared", "can_read", "can_read_free_busy", "can_write", "can_write_content", "can_write_properties", "can_bind", "can_unbind"}).
			AddRow(int64(8), int64(9), "Inbox", nil, nil, nil, nil, int64(6), now, now, "owner@example.com", true, false, false, false, false, false, true, false))

	accessible, err := repo.ListAccessible(context.Background(), 4)
	if err != nil {
		t.Fatalf("ListAccessible() error = %v", err)
	}
	if len(accessible) != 1 {
		t.Fatalf("ListAccessible() len = %d, want 1", len(accessible))
	}
	if accessible[0].Privileges.Read || accessible[0].Privileges.ReadFreeBusy {
		t.Fatalf("ListAccessible() read privileges = %#v, want none for bind-only access", accessible[0].Privileges)
	}
	if !accessible[0].Privileges.Bind {
		t.Fatalf("ListAccessible() bind = false, want true")
	}
	if accessible[0].Editor {
		t.Fatalf("ListAccessible() editor = true, want false")
	}

	mock.ExpectQuery(`(?s)SELECT c.id, c.user_id, c.name, c.slug, c.description, c.timezone, c.color, c.ctag, c.created_at, c.updated_at,.*FROM calendars c.*WHERE c.id = \$1.*bind.*`).
		WithArgs(int64(8), int64(4)).
		WillReturnRows(sqlmock.NewRows([]string{"id", "user_id", "name", "slug", "description", "timezone", "color", "ctag", "created_at", "updated_at", "owner_email", "shared", "can_read", "can_read_free_busy", "can_write", "can_write_content", "can_write_properties", "can_bind", "can_unbind"}).
			AddRow(int64(8), int64(9), "Inbox", nil, nil, nil, nil, int64(6), now, now, "owner@example.com", true, false, false, false, false, false, true, false))

	got, err := repo.GetAccessible(context.Background(), 8, 4)
	if err != nil {
		t.Fatalf("GetAccessible() error = %v", err)
	}
	if got == nil {
		t.Fatal("GetAccessible() = nil, want calendar access")
	}
	if got.Privileges.Read || got.Privileges.ReadFreeBusy {
		t.Fatalf("GetAccessible() read privileges = %#v, want none for bind-only access", got.Privileges)
	}
	if !got.Privileges.Bind {
		t.Fatalf("GetAccessible() bind = false, want true")
	}

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("sql expectations: %v", err)
	}
}

func TestCalendarAccessibleReposIncludeObjectGrantedCalendars(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New() error = %v", err)
	}
	defer db.Close()

	repo := &calendarRepo{pool: db}
	now := time.Now().UTC()

	mock.ExpectQuery(`(?s)SELECT c.id, c.user_id, c.name, c.slug, c.description, c.timezone, c.color, c.ctag, c.created_at, c.updated_at,.*FROM calendars c.*events e.*resource_path IN.*ORDER BY shared, name`).
		WithArgs(int64(4)).
		WillReturnRows(sqlmock.NewRows([]string{"id", "user_id", "name", "slug", "description", "timezone", "color", "ctag", "created_at", "updated_at", "owner_email", "shared", "can_read", "can_read_free_busy", "can_write", "can_write_content", "can_write_properties", "can_bind", "can_unbind"}).
			AddRow(int64(12), int64(9), "Object Shared", nil, nil, nil, nil, int64(7), now, now, "owner@example.com", true, false, false, false, false, false, false, false))

	accessible, err := repo.ListAccessible(context.Background(), 4)
	if err != nil {
		t.Fatalf("ListAccessible() error = %v", err)
	}
	if len(accessible) != 1 {
		t.Fatalf("ListAccessible() len = %d, want 1", len(accessible))
	}
	if accessible[0].Privileges.HasAny() {
		t.Fatalf("ListAccessible() privileges = %#v, want no collection privileges for object-only grant", accessible[0].Privileges)
	}

	mock.ExpectQuery(`(?s)SELECT c.id, c.user_id, c.name, c.slug, c.description, c.timezone, c.color, c.ctag, c.created_at, c.updated_at,.*FROM calendars c.*WHERE c.id = \$1.*events e.*resource_path IN`).
		WithArgs(int64(12), int64(4)).
		WillReturnRows(sqlmock.NewRows([]string{"id", "user_id", "name", "slug", "description", "timezone", "color", "ctag", "created_at", "updated_at", "owner_email", "shared", "can_read", "can_read_free_busy", "can_write", "can_write_content", "can_write_properties", "can_bind", "can_unbind"}).
			AddRow(int64(12), int64(9), "Object Shared", nil, nil, nil, nil, int64(7), now, now, "owner@example.com", true, false, false, false, false, false, false, false))

	got, err := repo.GetAccessible(context.Background(), 12, 4)
	if err != nil {
		t.Fatalf("GetAccessible() error = %v", err)
	}
	if got == nil {
		t.Fatal("GetAccessible() = nil, want calendar access")
	}
	if got.Privileges.HasAny() {
		t.Fatalf("GetAccessible() privileges = %#v, want no collection privileges for object-only grant", got.Privileges)
	}

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("sql expectations: %v", err)
	}
}

func TestEventRepoUpsertParsesFieldsAndPagination(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New() error = %v", err)
	}
	defer db.Close()

	repo := &eventRepo{pool: db}
	now := time.Now().UTC()
	dtstart := time.Date(2026, 4, 12, 0, 0, 0, 0, time.UTC)
	dtend := time.Date(2026, 4, 13, 0, 0, 0, 0, time.UTC)

	rawICAL := "BEGIN:VCALENDAR\r\nBEGIN:VEVENT\r\nUID:test-uid\r\nSUMMARY:Planning Day\r\nDTSTART;VALUE=DATE:20260412\r\nDTEND;VALUE=DATE:20260413\r\nEND:VEVENT\r\nEND:VCALENDAR"
	mock.ExpectQuery(regexp.QuoteMeta(`
INSERT INTO events (calendar_id, uid, resource_name, raw_ical, etag, summary, dtstart, dtend, all_day, last_modified)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, NOW())
ON CONFLICT (calendar_id, uid) DO UPDATE SET
        resource_name = EXCLUDED.resource_name,
        raw_ical = EXCLUDED.raw_ical,
        etag = EXCLUDED.etag,
        summary = EXCLUDED.summary,
        dtstart = EXCLUDED.dtstart,
        dtend = EXCLUDED.dtend,
        all_day = EXCLUDED.all_day,
        last_modified = NOW()
RETURNING id, calendar_id, uid, resource_name, raw_ical, etag, summary, dtstart, dtend, all_day, last_modified
`)).
		WithArgs(int64(7), "test-uid", "test-uid", rawICAL, "etag-1", "Planning Day", dtstart, dtend, true).
		WillReturnRows(sqlmock.NewRows([]string{"id", "calendar_id", "uid", "resource_name", "raw_ical", "etag", "summary", "dtstart", "dtend", "all_day", "last_modified"}).
			AddRow(int64(1), int64(7), "test-uid", "test-uid", rawICAL, "etag-1", "Planning Day", dtstart, dtend, true, now))

	created, err := repo.Upsert(context.Background(), Event{
		CalendarID: 7,
		UID:        "test-uid",
		RawICAL:    rawICAL,
		ETag:       "etag-1",
	})
	if err != nil {
		t.Fatalf("Upsert() error = %v", err)
	}
	if created.ResourceName != "test-uid" || created.Summary == nil || *created.Summary != "Planning Day" || !created.AllDay {
		t.Fatalf("Upsert() = %#v", created)
	}

	mock.ExpectQuery(regexp.QuoteMeta(`SELECT COUNT(*) FROM events WHERE calendar_id=$1`)).
		WithArgs(int64(7)).
		WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow(2))
	mock.ExpectQuery(regexp.QuoteMeta(`SELECT id, calendar_id, uid, resource_name, raw_ical, etag, summary, dtstart, dtend, all_day, last_modified FROM events WHERE calendar_id=$1 ORDER BY last_modified DESC LIMIT $2 OFFSET $3`)).
		WithArgs(int64(7), 1, 1).
		WillReturnRows(sqlmock.NewRows([]string{"id", "calendar_id", "uid", "resource_name", "raw_ical", "etag", "summary", "dtstart", "dtend", "all_day", "last_modified"}).
			AddRow(int64(2), int64(7), "other", "other.ics", rawICAL, "etag-2", nil, nil, nil, false, now))

	page, err := repo.ListForCalendarPaginated(context.Background(), 7, 1, 1)
	if err != nil {
		t.Fatalf("ListForCalendarPaginated() error = %v", err)
	}
	if page.TotalCount != 2 || page.Limit != 1 || page.Offset != 1 || len(page.Items) != 1 {
		t.Fatalf("ListForCalendarPaginated() = %#v", page)
	}

	events, err := repo.ListByUIDs(context.Background(), 7, nil)
	if err != nil {
		t.Fatalf("ListByUIDs() error = %v", err)
	}
	if len(events) != 0 {
		t.Fatalf("ListByUIDs() = %#v, want empty", events)
	}

	mock.ExpectQuery(regexp.QuoteMeta(`SELECT COALESCE(MAX(last_modified), '1970-01-01T00:00:00Z') FROM events WHERE calendar_id=$1`)).
		WithArgs(int64(7)).
		WillReturnRows(sqlmock.NewRows([]string{"max"}).AddRow(time.Date(2026, 4, 12, 9, 0, 0, 0, time.FixedZone("CDT", -5*3600))))
	max, err := repo.MaxLastModified(context.Background(), 7)
	if err != nil {
		t.Fatalf("MaxLastModified() error = %v", err)
	}
	if max.Location() != time.UTC || max.Hour() != 14 {
		t.Fatalf("MaxLastModified() = %v", max)
	}

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("sql expectations: %v", err)
	}
}

func TestEventRepoMoveToCalendarRenameWithinSameCalendarCreatesTombstone(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New() error = %v", err)
	}
	defer db.Close()

	repo := &eventRepo{pool: db}

	mock.ExpectBegin()
	mock.ExpectQuery(regexp.QuoteMeta(`SELECT resource_name FROM events WHERE calendar_id=$1 AND uid=$2`)).
		WithArgs(int64(5), "event-1").
		WillReturnRows(sqlmock.NewRows([]string{"resource_name"}).AddRow("old-name"))
	mock.ExpectExec(regexp.QuoteMeta(`DELETE FROM events WHERE calendar_id=$1 AND resource_name=$2 AND uid<>$3`)).
		WithArgs(int64(5), "new-name", "event-1").
		WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectExec(regexp.QuoteMeta(`UPDATE events SET calendar_id=$1, resource_name=$2, last_modified=NOW() WHERE calendar_id=$3 AND uid=$4`)).
		WithArgs(int64(5), "new-name", int64(5), "event-1").
		WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectExec(regexp.QuoteMeta(`INSERT INTO deleted_resources (resource_type, collection_id, uid, resource_name) VALUES ('event', $1, $2, $3)`)).
		WithArgs(int64(5), "event-1", "old-name").
		WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectCommit()

	if err := repo.MoveToCalendar(context.Background(), 5, 5, "event-1", "new-name"); err != nil {
		t.Fatalf("MoveToCalendar() error = %v", err)
	}

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("sql expectations: %v", err)
	}
}

func TestEventRepoMoveToCalendarOverwriteWithinSameCalendarDeletesDestination(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New() error = %v", err)
	}
	defer db.Close()

	repo := &eventRepo{pool: db}

	mock.ExpectBegin()
	mock.ExpectQuery(regexp.QuoteMeta(`SELECT resource_name FROM events WHERE calendar_id=$1 AND uid=$2`)).
		WithArgs(int64(5), "event-1").
		WillReturnRows(sqlmock.NewRows([]string{"resource_name"}).AddRow("old-name"))
	mock.ExpectExec(regexp.QuoteMeta(`DELETE FROM events WHERE calendar_id=$1 AND resource_name=$2 AND uid<>$3`)).
		WithArgs(int64(5), "new-name", "event-1").
		WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectExec(regexp.QuoteMeta(`UPDATE events SET calendar_id=$1, resource_name=$2, last_modified=NOW() WHERE calendar_id=$3 AND uid=$4`)).
		WithArgs(int64(5), "new-name", int64(5), "event-1").
		WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectExec(regexp.QuoteMeta(`INSERT INTO deleted_resources (resource_type, collection_id, uid, resource_name) VALUES ('event', $1, $2, $3)`)).
		WithArgs(int64(5), "event-1", "old-name").
		WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectCommit()

	if err := repo.MoveToCalendar(context.Background(), 5, 5, "event-1", "new-name"); err != nil {
		t.Fatalf("MoveToCalendar() error = %v", err)
	}

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("sql expectations: %v", err)
	}
}

func TestEventRepoMoveToCalendarRejectsDestinationUIDRebindAcrossCalendars(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New() error = %v", err)
	}
	defer db.Close()

	repo := &eventRepo{pool: db}

	mock.ExpectBegin()
	mock.ExpectQuery(regexp.QuoteMeta(`SELECT resource_name FROM events WHERE calendar_id=$1 AND uid=$2`)).
		WithArgs(int64(5), "event-1").
		WillReturnRows(sqlmock.NewRows([]string{"resource_name"}).AddRow("source-name"))
	mock.ExpectQuery(regexp.QuoteMeta(`SELECT resource_name FROM events WHERE calendar_id=$1 AND uid=$2`)).
		WithArgs(int64(9), "event-1").
		WillReturnRows(sqlmock.NewRows([]string{"resource_name"}).AddRow("old-dest-name"))
	mock.ExpectRollback()

	err = repo.MoveToCalendar(context.Background(), 5, 9, "event-1", "new-dest-name")
	if err != ErrConflict {
		t.Fatalf("MoveToCalendar() error = %v, want ErrConflict", err)
	}

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("sql expectations: %v", err)
	}
}

func TestEventRepoCopyToCalendarRejectsDestinationUIDRebindAcrossCalendars(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New() error = %v", err)
	}
	defer db.Close()

	repo := &eventRepo{pool: db}
	now := time.Now().UTC()

	mock.ExpectBegin()
	mock.ExpectQuery(regexp.QuoteMeta(`SELECT id, calendar_id, uid, resource_name, raw_ical, etag, summary, dtstart, dtend, all_day, last_modified FROM events WHERE calendar_id=$1 AND uid=$2`)).
		WithArgs(int64(5), "event-1").
		WillReturnRows(sqlmock.NewRows([]string{"id", "calendar_id", "uid", "resource_name", "raw_ical", "etag", "summary", "dtstart", "dtend", "all_day", "last_modified"}).
			AddRow(int64(1), int64(5), "event-1", "source-name", "BEGIN:VCALENDAR", "etag-src", nil, nil, nil, false, now))
	mock.ExpectQuery(regexp.QuoteMeta(`SELECT resource_name FROM events WHERE calendar_id=$1 AND uid=$2`)).
		WithArgs(int64(9), "event-1").
		WillReturnRows(sqlmock.NewRows([]string{"resource_name"}).AddRow("old-dest-name"))
	mock.ExpectRollback()

	_, err = repo.CopyToCalendar(context.Background(), 5, 9, "event-1", "new-dest-name", "etag-new")
	if err != ErrConflict {
		t.Fatalf("CopyToCalendar() error = %v, want ErrConflict", err)
	}

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("sql expectations: %v", err)
	}
}

func TestEventRepoAndAddressBookRepoReturnNilOrErrNotFound(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New() error = %v", err)
	}
	defer db.Close()

	eventRepo := &eventRepo{pool: db}
	addressBookRepo := &addressBookRepo{pool: db}

	mock.ExpectQuery(regexp.QuoteMeta(`SELECT id, calendar_id, uid, resource_name, raw_ical, etag, summary, dtstart, dtend, all_day, last_modified FROM events WHERE calendar_id=$1 AND uid=$2`)).
		WithArgs(int64(2), "missing").
		WillReturnError(sql.ErrNoRows)
	ev, err := eventRepo.GetByUID(context.Background(), 2, "missing")
	if err != nil {
		t.Fatalf("GetByUID() error = %v", err)
	}
	if ev != nil {
		t.Fatalf("GetByUID() = %#v, want nil", ev)
	}

	mock.ExpectExec(regexp.QuoteMeta(`UPDATE address_books SET name=$1, description=$2, updated_at=NOW() WHERE id=$3 AND user_id=$4`)).
		WithArgs("Contacts", (*string)(nil), int64(12), int64(4)).
		WillReturnResult(sqlmock.NewResult(0, 0))
	if err := addressBookRepo.Update(context.Background(), 4, 12, "Contacts", nil); err != ErrNotFound {
		t.Fatalf("Update() error = %v, want ErrNotFound", err)
	}

	mock.ExpectExec(regexp.QuoteMeta(`DELETE FROM address_books WHERE id=$1 AND user_id=$2`)).
		WithArgs(int64(12), int64(4)).
		WillReturnResult(sqlmock.NewResult(0, 0))
	if err := addressBookRepo.Delete(context.Background(), 4, 12); err != ErrNotFound {
		t.Fatalf("Delete() error = %v, want ErrNotFound", err)
	}

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("sql expectations: %v", err)
	}
}

func TestEventAndAddressBookListQueries(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New() error = %v", err)
	}
	defer db.Close()

	eventRepo := &eventRepo{pool: db}
	bookRepo := &addressBookRepo{pool: db}
	now := time.Now().UTC()

	mock.ExpectQuery(regexp.QuoteMeta(`SELECT id, calendar_id, uid, resource_name, raw_ical, etag, summary, dtstart, dtend, all_day, last_modified FROM events WHERE calendar_id=$1 AND uid = ANY($2)`)).
		WithArgs(int64(7), sqlmock.AnyArg()).
		WillReturnRows(sqlmock.NewRows([]string{"id", "calendar_id", "uid", "resource_name", "raw_ical", "etag", "summary", "dtstart", "dtend", "all_day", "last_modified"}).
			AddRow(int64(1), int64(7), "uid-1", "uid-1.ics", "BEGIN:VCALENDAR", "etag-1", "Meeting", now, now.Add(time.Hour), false, now))
	byUIDs, err := eventRepo.ListByUIDs(context.Background(), 7, []string{"uid-1"})
	if err != nil {
		t.Fatalf("ListByUIDs() error = %v", err)
	}
	if len(byUIDs) != 1 || byUIDs[0].Summary == nil || *byUIDs[0].Summary != "Meeting" {
		t.Fatalf("ListByUIDs() = %#v", byUIDs)
	}

	mock.ExpectQuery(regexp.QuoteMeta(`SELECT id, calendar_id, uid, resource_name, raw_ical, etag, summary, dtstart, dtend, all_day, last_modified FROM events WHERE calendar_id=$1 AND resource_name=$2`)).
		WithArgs(int64(7), "missing.ics").
		WillReturnError(sql.ErrNoRows)
	resource, err := eventRepo.GetByResourceName(context.Background(), 7, "missing.ics")
	if err != nil {
		t.Fatalf("GetByResourceName() error = %v", err)
	}
	if resource != nil {
		t.Fatalf("GetByResourceName() = %#v, want nil", resource)
	}

	since := now.Add(-time.Hour)
	mock.ExpectQuery(regexp.QuoteMeta(`SELECT id, calendar_id, uid, resource_name, raw_ical, etag, summary, dtstart, dtend, all_day, last_modified FROM events WHERE calendar_id=$1 AND last_modified > $2 ORDER BY last_modified DESC`)).
		WithArgs(int64(7), since).
		WillReturnRows(sqlmock.NewRows([]string{"id", "calendar_id", "uid", "resource_name", "raw_ical", "etag", "summary", "dtstart", "dtend", "all_day", "last_modified"}).
			AddRow(int64(2), int64(7), "uid-2", "uid-2.ics", "BEGIN:VCALENDAR", "etag-2", "Recent", nil, nil, true, now))
	modified, err := eventRepo.ListModifiedSince(context.Background(), 7, since)
	if err != nil {
		t.Fatalf("ListModifiedSince() error = %v", err)
	}
	if len(modified) != 1 || !modified[0].AllDay {
		t.Fatalf("ListModifiedSince() = %#v", modified)
	}

	mock.ExpectQuery(`(?s)SELECT e.id, e.calendar_id, e.uid, e.resource_name, e.raw_ical, e.etag, e.summary, e.dtstart, e.dtend, e.all_day, e.last_modified.*FROM events e.*acl_entries.*ORDER BY e.last_modified DESC.*LIMIT \$2`).
		WithArgs(int64(4), 2).
		WillReturnRows(sqlmock.NewRows([]string{"id", "calendar_id", "uid", "resource_name", "raw_ical", "etag", "summary", "dtstart", "dtend", "all_day", "last_modified"}).
			AddRow(int64(3), int64(8), "uid-3", "uid-3.ics", "BEGIN:VCALENDAR", "etag-3", nil, nil, nil, false, now))
	recent, err := eventRepo.ListRecentByUser(context.Background(), 4, 2)
	if err != nil {
		t.Fatalf("ListRecentByUser() error = %v", err)
	}
	if len(recent) != 1 || recent[0].UID != "uid-3" {
		t.Fatalf("ListRecentByUser() = %#v", recent)
	}

	mock.ExpectQuery(`(?s)SELECT e.id, e.calendar_id, e.uid, e.resource_name, e.raw_ical, e.etag, e.summary, e.dtstart, e.dtend, e.all_day, e.last_modified.*resource_path IN.*e.resource_name.*LIMIT \$2`).
		WithArgs(int64(4), 2).
		WillReturnRows(sqlmock.NewRows([]string{"id", "calendar_id", "uid", "resource_name", "raw_ical", "etag", "summary", "dtstart", "dtend", "all_day", "last_modified"}).
			AddRow(int64(6), int64(8), "uid-object", "uid-object", "BEGIN:VCALENDAR", "etag-6", "Direct Grant", nil, nil, false, now))
	recent, err = eventRepo.ListRecentByUser(context.Background(), 4, 2)
	if err != nil {
		t.Fatalf("ListRecentByUser() direct grant error = %v", err)
	}
	if len(recent) != 1 || recent[0].UID != "uid-object" {
		t.Fatalf("ListRecentByUser() direct grant = %#v", recent)
	}

	mock.ExpectQuery(regexp.QuoteMeta(`SELECT id, user_id, name, description, ctag, created_at, updated_at FROM address_books WHERE user_id=$1 ORDER BY created_at`)).
		WithArgs(int64(4)).
		WillReturnRows(sqlmock.NewRows([]string{"id", "user_id", "name", "description", "ctag", "created_at", "updated_at"}).
			AddRow(int64(1), int64(4), "Contacts", nil, int64(1), now, now))
	books, err := bookRepo.ListByUser(context.Background(), 4)
	if err != nil {
		t.Fatalf("ListByUser() error = %v", err)
	}
	if len(books) != 1 || books[0].Description != nil {
		t.Fatalf("ListByUser() = %#v", books)
	}

	mock.ExpectExec(regexp.QuoteMeta(`UPDATE address_books SET name=$1, updated_at=NOW() WHERE id=$2 AND user_id=$3`)).
		WithArgs("Renamed", int64(1), int64(4)).
		WillReturnResult(sqlmock.NewResult(0, 1))
	if err := bookRepo.Rename(context.Background(), 4, 1, "Renamed"); err != nil {
		t.Fatalf("Rename() error = %v", err)
	}

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("sql expectations: %v", err)
	}
}

func TestContactRepoUpsertAndMoveToAddressBook(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New() error = %v", err)
	}
	defer db.Close()

	repo := &contactRepo{pool: db}
	now := time.Now().UTC()
	birthday := time.Date(1990, 5, 15, 0, 0, 0, 0, time.UTC)
	rawVCard := "BEGIN:VCARD\r\nVERSION:3.0\r\nFN:Jane Doe\r\nEMAIL:jane@example.com\r\nBDAY:1990-05-15\r\nEND:VCARD"

	mock.ExpectQuery(regexp.QuoteMeta(`
INSERT INTO contacts (address_book_id, uid, resource_name, raw_vcard, etag, display_name, primary_email, birthday, last_modified)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, NOW())
ON CONFLICT (address_book_id, uid) DO UPDATE SET
        resource_name = EXCLUDED.resource_name,
        raw_vcard = EXCLUDED.raw_vcard,
        etag = EXCLUDED.etag,
        display_name = EXCLUDED.display_name,
        primary_email = EXCLUDED.primary_email,
        birthday = EXCLUDED.birthday,
        last_modified = NOW()
RETURNING id, address_book_id, uid, resource_name, raw_vcard, etag, display_name, primary_email, birthday, last_modified
`)).
		WithArgs(int64(5), "contact-1", "contact-1", rawVCard, "etag-1", "Jane Doe", "jane@example.com", birthday).
		WillReturnRows(sqlmock.NewRows([]string{"id", "address_book_id", "uid", "resource_name", "raw_vcard", "etag", "display_name", "primary_email", "birthday", "last_modified"}).
			AddRow(int64(1), int64(5), "contact-1", "contact-1", rawVCard, "etag-1", "Jane Doe", "jane@example.com", birthday, now))

	created, err := repo.Upsert(context.Background(), Contact{
		AddressBookID: 5,
		UID:           "contact-1",
		RawVCard:      rawVCard,
		ETag:          "etag-1",
	})
	if err != nil {
		t.Fatalf("Upsert() error = %v", err)
	}
	if created.DisplayName == nil || *created.DisplayName != "Jane Doe" || created.PrimaryEmail == nil || *created.PrimaryEmail != "jane@example.com" || created.Birthday == nil {
		t.Fatalf("Upsert() = %#v", created)
	}

	mock.ExpectBegin()
	mock.ExpectQuery(regexp.QuoteMeta(`SELECT resource_name FROM contacts WHERE address_book_id=$1 AND uid=$2`)).
		WithArgs(int64(5), "contact-1").
		WillReturnRows(sqlmock.NewRows([]string{"resource_name"}).AddRow("contact-1"))
	mock.ExpectExec(regexp.QuoteMeta(`DELETE FROM contacts WHERE address_book_id=$1 AND resource_name=$2 AND uid<>$3`)).
		WithArgs(int64(9), "contact-1-copy", "contact-1").
		WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectExec(regexp.QuoteMeta(`DELETE FROM contacts WHERE address_book_id=$1 AND uid=$2`)).
		WithArgs(int64(9), "contact-1").
		WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectExec(regexp.QuoteMeta(`UPDATE contacts SET address_book_id=$1, resource_name=$2, last_modified=NOW() WHERE address_book_id=$3 AND uid=$4`)).
		WithArgs(int64(9), "contact-1-copy", int64(5), "contact-1").
		WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectExec(regexp.QuoteMeta(`INSERT INTO deleted_resources (resource_type, collection_id, uid, resource_name) VALUES ('contact', $1, $2, $3)`)).
		WithArgs(int64(5), "contact-1", "contact-1").
		WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectExec(regexp.QuoteMeta(`UPDATE address_books SET ctag = ctag + 1, updated_at = NOW() WHERE id = $1`)).
		WithArgs(int64(5)).
		WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectCommit()

	if err := repo.MoveToAddressBook(context.Background(), 5, 9, "contact-1", "contact-1-copy"); err != nil {
		t.Fatalf("MoveToAddressBook() error = %v", err)
	}

	mock.ExpectBegin()
	mock.ExpectQuery(regexp.QuoteMeta(`SELECT resource_name FROM contacts WHERE address_book_id=$1 AND uid=$2`)).
		WithArgs(int64(5), "missing").
		WillReturnError(sql.ErrNoRows)
	mock.ExpectRollback()
	if err := repo.MoveToAddressBook(context.Background(), 5, 9, "missing", "missing-copy"); err != ErrNotFound {
		t.Fatalf("MoveToAddressBook() error = %v, want ErrNotFound", err)
	}

	contacts, err := repo.ListByUIDs(context.Background(), 5, []string{})
	if err != nil {
		t.Fatalf("ListByUIDs() error = %v", err)
	}
	if len(contacts) != 0 {
		t.Fatalf("ListByUIDs() = %#v, want empty", contacts)
	}

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("sql expectations: %v", err)
	}
}

func TestContactRepoMoveToAddressBookRenameWithinSameBookCreatesTombstone(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New() error = %v", err)
	}
	defer db.Close()

	repo := &contactRepo{pool: db}

	mock.ExpectBegin()
	mock.ExpectQuery(regexp.QuoteMeta(`SELECT resource_name FROM contacts WHERE address_book_id=$1 AND uid=$2`)).
		WithArgs(int64(5), "contact-1").
		WillReturnRows(sqlmock.NewRows([]string{"resource_name"}).AddRow("legacy-name"))
	mock.ExpectExec(regexp.QuoteMeta(`DELETE FROM contacts WHERE address_book_id=$1 AND resource_name=$2 AND uid<>$3`)).
		WithArgs(int64(5), "renamed-contact", "contact-1").
		WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectExec(regexp.QuoteMeta(`UPDATE contacts SET address_book_id=$1, resource_name=$2, last_modified=NOW() WHERE address_book_id=$3 AND uid=$4`)).
		WithArgs(int64(5), "renamed-contact", int64(5), "contact-1").
		WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectExec(regexp.QuoteMeta(`INSERT INTO deleted_resources (resource_type, collection_id, uid, resource_name) VALUES ('contact', $1, $2, $3)`)).
		WithArgs(int64(5), "contact-1", "legacy-name").
		WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectCommit()

	if err := repo.MoveToAddressBook(context.Background(), 5, 5, "contact-1", "renamed-contact"); err != nil {
		t.Fatalf("MoveToAddressBook() error = %v", err)
	}

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("sql expectations: %v", err)
	}
}

func TestContactRepoMoveToAddressBookOverwriteWithinSameBookDeletesDestination(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New() error = %v", err)
	}
	defer db.Close()

	repo := &contactRepo{pool: db}

	mock.ExpectBegin()
	mock.ExpectQuery(regexp.QuoteMeta(`SELECT resource_name FROM contacts WHERE address_book_id=$1 AND uid=$2`)).
		WithArgs(int64(5), "contact-1").
		WillReturnRows(sqlmock.NewRows([]string{"resource_name"}).AddRow("old-name"))
	mock.ExpectExec(regexp.QuoteMeta(`DELETE FROM contacts WHERE address_book_id=$1 AND resource_name=$2 AND uid<>$3`)).
		WithArgs(int64(5), "new-name", "contact-1").
		WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectExec(regexp.QuoteMeta(`UPDATE contacts SET address_book_id=$1, resource_name=$2, last_modified=NOW() WHERE address_book_id=$3 AND uid=$4`)).
		WithArgs(int64(5), "new-name", int64(5), "contact-1").
		WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectExec(regexp.QuoteMeta(`INSERT INTO deleted_resources (resource_type, collection_id, uid, resource_name) VALUES ('contact', $1, $2, $3)`)).
		WithArgs(int64(5), "contact-1", "old-name").
		WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectCommit()

	if err := repo.MoveToAddressBook(context.Background(), 5, 5, "contact-1", "new-name"); err != nil {
		t.Fatalf("MoveToAddressBook() error = %v", err)
	}

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("sql expectations: %v", err)
	}
}

func TestContactRepoUpsertMapsResourceNameConflictsToErrConflict(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New() error = %v", err)
	}
	defer db.Close()

	repo := &contactRepo{pool: db}
	rawVCard := "BEGIN:VCARD\r\nVERSION:3.0\r\nUID:contact-1\r\nFN:Jane Doe\r\nEND:VCARD\r\n"

	mock.ExpectQuery(regexp.QuoteMeta(`
INSERT INTO contacts (address_book_id, uid, resource_name, raw_vcard, etag, display_name, primary_email, birthday, last_modified)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, NOW())
ON CONFLICT (address_book_id, uid) DO UPDATE SET
        resource_name = EXCLUDED.resource_name,
        raw_vcard = EXCLUDED.raw_vcard,
        etag = EXCLUDED.etag,
        display_name = EXCLUDED.display_name,
        primary_email = EXCLUDED.primary_email,
        birthday = EXCLUDED.birthday,
        last_modified = NOW()
RETURNING id, address_book_id, uid, resource_name, raw_vcard, etag, display_name, primary_email, birthday, last_modified
`)).
		WithArgs(int64(5), "contact-1", "renamed", rawVCard, "etag-1", "Jane Doe", nil, nil).
		WillReturnError(&pq.Error{Code: "23505", Constraint: "idx_contacts_resource_name"})

	_, err = repo.Upsert(context.Background(), Contact{
		AddressBookID: 5,
		UID:           "contact-1",
		ResourceName:  "renamed",
		RawVCard:      rawVCard,
		ETag:          "etag-1",
	})
	if err != ErrConflict {
		t.Fatalf("Upsert() error = %v, want ErrConflict", err)
	}

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("sql expectations: %v", err)
	}
}

func TestContactRepoCopyToAddressBookRenameExistingUIDCreatesDestinationTombstone(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New() error = %v", err)
	}
	defer db.Close()

	repo := &contactRepo{pool: db}
	now := time.Now()
	rawVCard := "BEGIN:VCARD\r\nVERSION:3.0\r\nUID:contact-1\r\nFN:Jane Doe\r\nEND:VCARD\r\n"

	mock.ExpectBegin()
	mock.ExpectQuery(regexp.QuoteMeta(`SELECT id, address_book_id, uid, resource_name, raw_vcard, etag, display_name, primary_email, birthday, last_modified FROM contacts WHERE address_book_id=$1 AND uid=$2`)).
		WithArgs(int64(5), "contact-1").
		WillReturnRows(sqlmock.NewRows([]string{"id", "address_book_id", "uid", "resource_name", "raw_vcard", "etag", "display_name", "primary_email", "birthday", "last_modified"}).
			AddRow(int64(1), int64(5), "contact-1", "source-name", rawVCard, "etag-src", "Jane Doe", nil, nil, now))
	mock.ExpectQuery(regexp.QuoteMeta(`SELECT resource_name FROM contacts WHERE address_book_id=$1 AND uid=$2`)).
		WithArgs(int64(9), "contact-1").
		WillReturnRows(sqlmock.NewRows([]string{"resource_name"}).AddRow("old-dest-name"))
	mock.ExpectExec(regexp.QuoteMeta(`DELETE FROM contacts WHERE address_book_id=$1 AND resource_name=$2 AND uid<>$3`)).
		WithArgs(int64(9), "new-dest-name", "contact-1").
		WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectExec(regexp.QuoteMeta(`INSERT INTO deleted_resources (resource_type, collection_id, uid, resource_name) VALUES ('contact', $1, $2, $3)`)).
		WithArgs(int64(9), "contact-1", "old-dest-name").
		WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectQuery(regexp.QuoteMeta(`
INSERT INTO contacts (address_book_id, uid, resource_name, raw_vcard, etag, display_name, primary_email, birthday, last_modified)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, NOW())
ON CONFLICT (address_book_id, uid) DO UPDATE SET
        resource_name = EXCLUDED.resource_name,
        raw_vcard = EXCLUDED.raw_vcard,
        etag = EXCLUDED.etag,
        display_name = EXCLUDED.display_name,
        primary_email = EXCLUDED.primary_email,
        birthday = EXCLUDED.birthday,
        last_modified = NOW()
RETURNING id, address_book_id, uid, resource_name, raw_vcard, etag, display_name, primary_email, birthday, last_modified
`)).
		WithArgs(int64(9), "contact-1", "new-dest-name", rawVCard, "etag-new", "Jane Doe", nil, nil).
		WillReturnRows(sqlmock.NewRows([]string{"id", "address_book_id", "uid", "resource_name", "raw_vcard", "etag", "display_name", "primary_email", "birthday", "last_modified"}).
			AddRow(int64(2), int64(9), "contact-1", "new-dest-name", rawVCard, "etag-new", "Jane Doe", nil, nil, now))
	mock.ExpectCommit()

	_, err = repo.CopyToAddressBook(context.Background(), 5, 9, "contact-1", "new-dest-name", "etag-new")
	if err != nil {
		t.Fatalf("CopyToAddressBook() error = %v", err)
	}

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("sql expectations: %v", err)
	}
}

func TestStoreDeleteEventAndStateRunsInSingleTransaction(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New() error = %v", err)
	}
	defer db.Close()

	st := New(db)

	mock.ExpectBegin()
	mock.ExpectExec(regexp.QuoteMeta(`DELETE FROM events WHERE calendar_id=$1 AND uid=$2`)).
		WithArgs(int64(7), "event-1").
		WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectExec(regexp.QuoteMeta(`DELETE FROM locks WHERE resource_path=$1`)).
		WithArgs("/dav/calendars/7/renamed").
		WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectExec(regexp.QuoteMeta(`DELETE FROM acl_entries WHERE resource_path=$1`)).
		WithArgs("/dav/calendars/7/renamed").
		WillReturnError(errors.New("acl delete failed"))
	mock.ExpectRollback()

	err = st.DeleteEventAndState(context.Background(), 7, "event-1", "/dav/calendars/7/renamed")
	if err == nil {
		t.Fatal("DeleteEventAndState() error = nil, want error")
	}

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("sql expectations: %v", err)
	}
}

func TestStoreDeleteContactAndStateRunsInSingleTransaction(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New() error = %v", err)
	}
	defer db.Close()

	st := New(db)

	mock.ExpectBegin()
	mock.ExpectExec(regexp.QuoteMeta(`DELETE FROM contacts WHERE address_book_id=$1 AND uid=$2`)).
		WithArgs(int64(5), "contact-1").
		WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectExec(regexp.QuoteMeta(`DELETE FROM locks WHERE resource_path=$1`)).
		WithArgs("/dav/addressbooks/5/contact.v1").
		WillReturnError(errors.New("lock delete failed"))
	mock.ExpectRollback()

	err = st.DeleteContactAndState(context.Background(), 5, "contact-1", "/dav/addressbooks/5/contact.v1")
	if err == nil {
		t.Fatal("DeleteContactAndState() error = nil, want error")
	}

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("sql expectations: %v", err)
	}
}

func TestStoreDeleteContactAndStateRemovesCanonicalAndLegacyPaths(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New() error = %v", err)
	}
	defer db.Close()

	st := New(db)

	mock.ExpectBegin()
	mock.ExpectExec(regexp.QuoteMeta(`DELETE FROM contacts WHERE address_book_id=$1 AND uid=$2`)).
		WithArgs(int64(5), "contact-1").
		WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectExec(regexp.QuoteMeta(`DELETE FROM locks WHERE resource_path=$1`)).
		WithArgs("/dav/addressbooks/5/contact-1").
		WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectExec(regexp.QuoteMeta(`DELETE FROM locks WHERE resource_path=$1`)).
		WithArgs("/dav/addressbooks/5/contact-1.vcf").
		WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectCommit()

	if err := st.DeleteContactAndState(context.Background(), 5, "contact-1", "/dav/addressbooks/5/contact-1"); err != nil {
		t.Fatalf("DeleteContactAndState() error = %v", err)
	}

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("sql expectations: %v", err)
	}
}

func TestStoreDeleteContactAndStatePreservesACLState(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New() error = %v", err)
	}
	defer db.Close()

	st := New(db)

	mock.ExpectBegin()
	mock.ExpectExec(regexp.QuoteMeta(`DELETE FROM contacts WHERE address_book_id=$1 AND uid=$2`)).
		WithArgs(int64(5), "contact-1").
		WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectExec(regexp.QuoteMeta(`DELETE FROM locks WHERE resource_path=$1`)).
		WithArgs("/dav/addressbooks/5/contact-1").
		WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectExec(regexp.QuoteMeta(`DELETE FROM locks WHERE resource_path=$1`)).
		WithArgs("/dav/addressbooks/5/contact-1.vcf").
		WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectCommit()

	if err := st.DeleteContactAndState(context.Background(), 5, "contact-1", "/dav/addressbooks/5/contact-1"); err != nil {
		t.Fatalf("DeleteContactAndState() error = %v", err)
	}

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("sql expectations: %v", err)
	}
}

func TestLockRepoCreateSerializesAncestorAndDescendantPaths(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New() error = %v", err)
	}
	defer db.Close()

	repo := &lockRepo{pool: db}
	expiresAt := time.Now().Add(time.Hour)

	mock.ExpectBegin()
	mock.ExpectExec(regexp.QuoteMeta(`SELECT pg_advisory_xact_lock(hashtext($1))`)).
		WithArgs("/dav/addressbooks/5").
		WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectExec(regexp.QuoteMeta(`SELECT pg_advisory_xact_lock(hashtext($1))`)).
		WithArgs("/dav/addressbooks").
		WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectExec(regexp.QuoteMeta(`SELECT pg_advisory_xact_lock(hashtext($1))`)).
		WithArgs("/dav").
		WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectQuery(regexp.QuoteMeta(`SELECT lock_scope FROM locks WHERE resource_path = $1 AND expires_at > NOW()`)).
		WithArgs("/dav/addressbooks/5").
		WillReturnRows(sqlmock.NewRows([]string{"lock_scope"}))
	mock.ExpectQuery(regexp.QuoteMeta(`SELECT lock_scope FROM locks WHERE resource_path = ANY($1) AND depth = 'infinity' AND expires_at > NOW()`)).
		WithArgs(sqlmock.AnyArg()).
		WillReturnRows(sqlmock.NewRows([]string{"lock_scope"}))
	mock.ExpectQuery(regexp.QuoteMeta(`SELECT lock_scope FROM locks WHERE resource_path LIKE $1 ESCAPE '\' AND expires_at > NOW()`)).
		WithArgs(`/dav/addressbooks/5/%`).
		WillReturnRows(sqlmock.NewRows([]string{"lock_scope"}))
	mock.ExpectQuery(regexp.QuoteMeta(`
INSERT INTO locks (token, resource_path, user_id, lock_scope, lock_type, depth, owner_info, timeout_seconds, expires_at)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
RETURNING id, token, resource_path, user_id, lock_scope, lock_type, depth, owner_info, timeout_seconds, created_at, expires_at
`)).
		WithArgs("opaquelocktoken:test", "/dav/addressbooks/5", int64(1), "exclusive", "write", "infinity", "", 3600, expiresAt).
		WillReturnRows(sqlmock.NewRows([]string{"id", "token", "resource_path", "user_id", "lock_scope", "lock_type", "depth", "owner_info", "timeout_seconds", "created_at", "expires_at"}).
			AddRow(int64(1), "opaquelocktoken:test", "/dav/addressbooks/5", int64(1), "exclusive", "write", "infinity", "", 3600, time.Now(), expiresAt))
	mock.ExpectCommit()

	_, err = repo.Create(context.Background(), Lock{
		Token:          "opaquelocktoken:test",
		ResourcePath:   "/dav/addressbooks/5",
		UserID:         1,
		LockScope:      "exclusive",
		LockType:       "write",
		Depth:          "infinity",
		TimeoutSeconds: 3600,
		ExpiresAt:      expiresAt,
	})
	if err != nil {
		t.Fatalf("Create() error = %v", err)
	}

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("sql expectations: %v", err)
	}
}

func TestLockRepoMoveResourcePath(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New() error = %v", err)
	}
	defer db.Close()

	repo := &lockRepo{pool: db}

	mock.ExpectBegin()
	mock.ExpectExec(regexp.QuoteMeta(`DELETE FROM locks WHERE resource_path=$1 AND expires_at > NOW()`)).
		WithArgs("/dav/addressbooks/5").
		WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectExec(regexp.QuoteMeta(`UPDATE locks SET resource_path=$1 WHERE resource_path=$2 AND expires_at > NOW()`)).
		WithArgs("/dav/addressbooks/5", "/dav/addressbooks/.pending/1/NewBook").
		WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectCommit()

	if err := repo.MoveResourcePath(context.Background(), "/dav/addressbooks/.pending/1/NewBook", "/dav/addressbooks/5"); err != nil {
		t.Fatalf("MoveResourcePath() error = %v", err)
	}

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("sql expectations: %v", err)
	}
}

func TestContactRepoListQueriesAndMoveRollbackOnFailure(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New() error = %v", err)
	}
	defer db.Close()

	repo := &contactRepo{pool: db}
	now := time.Now().UTC()
	birthday := time.Date(1985, 7, 20, 0, 0, 0, 0, time.UTC)
	since := now.Add(-2 * time.Hour)

	mock.ExpectQuery(regexp.QuoteMeta(`SELECT id, address_book_id, uid, resource_name, raw_vcard, etag, display_name, primary_email, birthday, last_modified FROM contacts WHERE address_book_id=$1 AND uid = ANY($2)`)).
		WithArgs(int64(5), sqlmock.AnyArg()).
		WillReturnRows(sqlmock.NewRows([]string{"id", "address_book_id", "uid", "resource_name", "raw_vcard", "etag", "display_name", "primary_email", "birthday", "last_modified"}).
			AddRow(int64(1), int64(5), "uid-1", "uid-1", "BEGIN:VCARD", "etag-1", "Jane Doe", "jane@example.com", birthday, now))
	contacts, err := repo.ListByUIDs(context.Background(), 5, []string{"uid-1"})
	if err != nil {
		t.Fatalf("ListByUIDs() error = %v", err)
	}
	if len(contacts) != 1 || contacts[0].Birthday == nil || !contacts[0].Birthday.Equal(birthday) {
		t.Fatalf("ListByUIDs() = %#v", contacts)
	}

	mock.ExpectQuery(regexp.QuoteMeta(`SELECT id, address_book_id, uid, resource_name, raw_vcard, etag, display_name, primary_email, birthday, last_modified FROM contacts WHERE address_book_id=$1 ORDER BY last_modified DESC`)).
		WithArgs(int64(5)).
		WillReturnRows(sqlmock.NewRows([]string{"id", "address_book_id", "uid", "resource_name", "raw_vcard", "etag", "display_name", "primary_email", "birthday", "last_modified"}).
			AddRow(int64(2), int64(5), "uid-2", "uid-2", "BEGIN:VCARD", "etag-2", nil, nil, nil, now))
	forBook, err := repo.ListForBook(context.Background(), 5)
	if err != nil {
		t.Fatalf("ListForBook() error = %v", err)
	}
	if len(forBook) != 1 || forBook[0].DisplayName != nil {
		t.Fatalf("ListForBook() = %#v", forBook)
	}

	mock.ExpectQuery(regexp.QuoteMeta(`SELECT COUNT(*) FROM contacts WHERE address_book_id=$1`)).
		WithArgs(int64(5)).
		WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow(1))
	mock.ExpectQuery(regexp.QuoteMeta(`SELECT id, address_book_id, uid, resource_name, raw_vcard, etag, display_name, primary_email, birthday, last_modified FROM contacts WHERE address_book_id=$1 ORDER BY LOWER(COALESCE(display_name, '')) ASC, id ASC LIMIT $2 OFFSET $3`)).
		WithArgs(int64(5), 10, 0).
		WillReturnRows(sqlmock.NewRows([]string{"id", "address_book_id", "uid", "resource_name", "raw_vcard", "etag", "display_name", "primary_email", "birthday", "last_modified"}).
			AddRow(int64(3), int64(5), "uid-3", "uid-3", "BEGIN:VCARD", "etag-3", "Alex", nil, nil, now))
	page, err := repo.ListForBookPaginated(context.Background(), 5, 10, 0)
	if err != nil {
		t.Fatalf("ListForBookPaginated() error = %v", err)
	}
	if page.TotalCount != 1 || len(page.Items) != 1 || page.Items[0].DisplayName == nil || *page.Items[0].DisplayName != "Alex" {
		t.Fatalf("ListForBookPaginated() = %#v", page)
	}

	mock.ExpectQuery(regexp.QuoteMeta(`SELECT id, address_book_id, uid, resource_name, raw_vcard, etag, display_name, primary_email, birthday, last_modified FROM contacts WHERE address_book_id=$1 AND last_modified > $2 ORDER BY last_modified DESC`)).
		WithArgs(int64(5), since).
		WillReturnRows(sqlmock.NewRows([]string{"id", "address_book_id", "uid", "resource_name", "raw_vcard", "etag", "display_name", "primary_email", "birthday", "last_modified"}).
			AddRow(int64(4), int64(5), "uid-4", "uid-4", "BEGIN:VCARD", "etag-4", "Chris", "chris@example.com", nil, now))
	modified, err := repo.ListModifiedSince(context.Background(), 5, since)
	if err != nil {
		t.Fatalf("ListModifiedSince() error = %v", err)
	}
	if len(modified) != 1 || modified[0].PrimaryEmail == nil || *modified[0].PrimaryEmail != "chris@example.com" {
		t.Fatalf("ListModifiedSince() = %#v", modified)
	}

	mock.ExpectQuery(regexp.QuoteMeta(`
SELECT c.id, c.address_book_id, c.uid, c.resource_name, c.raw_vcard, c.etag, c.display_name, c.primary_email, c.birthday, c.last_modified
FROM contacts c
JOIN address_books ab ON ab.id = c.address_book_id
WHERE ab.user_id = $1
ORDER BY c.last_modified DESC
LIMIT $2
`)).
		WithArgs(int64(4), 5).
		WillReturnRows(sqlmock.NewRows([]string{"id", "address_book_id", "uid", "resource_name", "raw_vcard", "etag", "display_name", "primary_email", "birthday", "last_modified"}).
			AddRow(int64(5), int64(5), "uid-5", "uid-5", "BEGIN:VCARD", "etag-5", "Recent Contact", nil, nil, now))
	recent, err := repo.ListRecentByUser(context.Background(), 4, 5)
	if err != nil {
		t.Fatalf("ListRecentByUser() error = %v", err)
	}
	if len(recent) != 1 || recent[0].UID != "uid-5" {
		t.Fatalf("ListRecentByUser() = %#v", recent)
	}

	mock.ExpectQuery(regexp.QuoteMeta(`SELECT COALESCE(MAX(last_modified), '1970-01-01T00:00:00Z') FROM contacts WHERE address_book_id=$1`)).
		WithArgs(int64(5)).
		WillReturnRows(sqlmock.NewRows([]string{"max"}).AddRow(time.Date(2026, 4, 12, 9, 0, 0, 0, time.FixedZone("CDT", -5*3600))))
	max, err := repo.MaxLastModified(context.Background(), 5)
	if err != nil {
		t.Fatalf("MaxLastModified() error = %v", err)
	}
	if max.Location() != time.UTC || max.Hour() != 14 {
		t.Fatalf("MaxLastModified() = %v", max)
	}

	mock.ExpectQuery(regexp.QuoteMeta(`
SELECT c.id, c.address_book_id, c.uid, c.resource_name, c.raw_vcard, c.etag, c.display_name, c.primary_email, c.birthday, c.last_modified
FROM contacts c
JOIN address_books ab ON ab.id = c.address_book_id
WHERE ab.user_id = $1 AND c.birthday IS NOT NULL
ORDER BY c.display_name
`)).
		WithArgs(int64(4)).
		WillReturnRows(sqlmock.NewRows([]string{"id", "address_book_id", "uid", "resource_name", "raw_vcard", "etag", "display_name", "primary_email", "birthday", "last_modified"}).
			AddRow(int64(6), int64(5), "uid-6", "uid-6", "BEGIN:VCARD", "etag-6", "Birthday Person", nil, birthday, now))
	withBirthdays, err := repo.ListWithBirthdaysByUser(context.Background(), 4)
	if err != nil {
		t.Fatalf("ListWithBirthdaysByUser() error = %v", err)
	}
	if len(withBirthdays) != 1 || withBirthdays[0].Birthday == nil {
		t.Fatalf("ListWithBirthdaysByUser() = %#v", withBirthdays)
	}

	mock.ExpectBegin()
	mock.ExpectQuery(regexp.QuoteMeta(`SELECT resource_name FROM contacts WHERE address_book_id=$1 AND uid=$2`)).
		WithArgs(int64(5), "uid-rollback").
		WillReturnRows(sqlmock.NewRows([]string{"resource_name"}).AddRow("legacy-name"))
	mock.ExpectExec(regexp.QuoteMeta(`DELETE FROM contacts WHERE address_book_id=$1 AND resource_name=$2 AND uid<>$3`)).
		WithArgs(int64(9), "renamed-contact", "uid-rollback").
		WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectExec(regexp.QuoteMeta(`DELETE FROM contacts WHERE address_book_id=$1 AND uid=$2`)).
		WithArgs(int64(9), "uid-rollback").
		WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectExec(regexp.QuoteMeta(`UPDATE contacts SET address_book_id=$1, resource_name=$2, last_modified=NOW() WHERE address_book_id=$3 AND uid=$4`)).
		WithArgs(int64(9), "renamed-contact", int64(5), "uid-rollback").
		WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectExec(regexp.QuoteMeta(`INSERT INTO deleted_resources (resource_type, collection_id, uid, resource_name) VALUES ('contact', $1, $2, $3)`)).
		WithArgs(int64(5), "uid-rollback", "legacy-name").
		WillReturnError(errors.New("tombstone failed"))
	mock.ExpectRollback()
	if err := repo.MoveToAddressBook(context.Background(), 5, 9, "uid-rollback", "renamed-contact"); err == nil || err.Error() != "tombstone failed" {
		t.Fatalf("MoveToAddressBook() error = %v", err)
	}

	mock.ExpectQuery(regexp.QuoteMeta(`SELECT id, address_book_id, uid, resource_name, raw_vcard, etag, display_name, primary_email, birthday, last_modified FROM contacts WHERE address_book_id=$1 AND uid=$2`)).
		WithArgs(int64(5), "missing").
		WillReturnError(sql.ErrNoRows)
	got, err := repo.GetByUID(context.Background(), 5, "missing")
	if err != nil {
		t.Fatalf("GetByUID() error = %v", err)
	}
	if got != nil {
		t.Fatalf("GetByUID() = %#v, want nil", got)
	}

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("sql expectations: %v", err)
	}
}

func TestACLRepoHasPrivilegeDenyOverridesGrant(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New() error = %v", err)
	}
	defer db.Close()

	repo := &aclRepo{pool: db}

	mock.ExpectQuery(regexp.QuoteMeta(`
SELECT CASE
    WHEN EXISTS (
        SELECT 1 FROM acl_entries
        WHERE resource_path=$1 AND principal_href=$2 AND privilege=$3 AND is_grant=false
    ) THEN FALSE
    WHEN EXISTS (
        SELECT 1 FROM acl_entries
        WHERE resource_path=$1 AND principal_href=$2 AND privilege=$3 AND is_grant=true
    ) THEN TRUE
    ELSE FALSE
END
`)).
		WithArgs("/dav/addressbooks/5/alice.vcf", "/dav/principals/2/", "read").
		WillReturnRows(sqlmock.NewRows([]string{"has_privilege"}).AddRow(false))

	allowed, err := repo.HasPrivilege(context.Background(), "/dav/addressbooks/5/alice.vcf", "/dav/principals/2/", "read")
	if err != nil {
		t.Fatalf("HasPrivilege() error = %v", err)
	}
	if allowed {
		t.Fatal("HasPrivilege() = true, want deny to override grant")
	}

	mock.ExpectQuery(regexp.QuoteMeta(`
SELECT CASE
    WHEN EXISTS (
        SELECT 1 FROM acl_entries
        WHERE resource_path=$1 AND principal_href=$2 AND privilege=$3 AND is_grant=false
    ) THEN FALSE
    WHEN EXISTS (
        SELECT 1 FROM acl_entries
        WHERE resource_path=$1 AND principal_href=$2 AND privilege=$3 AND is_grant=true
    ) THEN TRUE
    ELSE FALSE
END
`)).
		WithArgs("/dav/addressbooks/5/alice.vcf", "/dav/principals/3/", "read").
		WillReturnRows(sqlmock.NewRows([]string{"has_privilege"}).AddRow(true))

	allowed, err = repo.HasPrivilege(context.Background(), "/dav/addressbooks/5/alice.vcf", "/dav/principals/3/", "read")
	if err != nil {
		t.Fatalf("HasPrivilege() error = %v", err)
	}
	if !allowed {
		t.Fatal("HasPrivilege() = false, want true when no deny exists")
	}

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("sql expectations: %v", err)
	}
}

func TestACLRepoSetACLPreservesCreatedAtForUnchangedEntries(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New() error = %v", err)
	}
	defer db.Close()

	repo := &aclRepo{pool: db}
	resourcePath := "/dav/calendars/1"
	createdAt := time.Date(2024, time.June, 1, 12, 0, 0, 0, time.UTC)

	mock.ExpectBegin()
	mock.ExpectQuery(regexp.QuoteMeta(`SELECT id, resource_path, principal_href, is_grant, privilege, created_at FROM acl_entries WHERE resource_path=$1 ORDER BY created_at, id`)).
		WithArgs(resourcePath).
		WillReturnRows(sqlmock.NewRows([]string{"id", "resource_path", "principal_href", "is_grant", "privilege", "created_at"}).
			AddRow(int64(1), resourcePath, "/dav/principals/2/", true, "read", createdAt))
	mock.ExpectExec(regexp.QuoteMeta(`DELETE FROM acl_entries WHERE resource_path=$1`)).
		WithArgs(resourcePath).
		WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectExec(regexp.QuoteMeta(`INSERT INTO acl_entries (resource_path, principal_href, is_grant, privilege, created_at) VALUES ($1, $2, $3, $4, $5)`)).
		WithArgs(resourcePath, "/dav/principals/2/", true, "read", createdAt).
		WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectExec(regexp.QuoteMeta(`INSERT INTO acl_entries (resource_path, principal_href, is_grant, privilege, created_at) VALUES ($1, $2, $3, $4, $5)`)).
		WithArgs(resourcePath, "/dav/principals/2/", true, "write", sqlmock.AnyArg()).
		WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectExec(regexp.QuoteMeta(`UPDATE calendars SET ctag = ctag + 1, updated_at = NOW() WHERE id = $1`)).
		WithArgs(int64(1)).
		WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectExec(regexp.QuoteMeta(`UPDATE events SET last_modified = NOW() WHERE calendar_id = $1`)).
		WithArgs(int64(1)).
		WillReturnResult(sqlmock.NewResult(0, 2))
	mock.ExpectCommit()

	err = repo.SetACL(context.Background(), resourcePath, []ACLEntry{
		{PrincipalHref: "/dav/principals/2/", IsGrant: true, Privilege: "read"},
		{PrincipalHref: "/dav/principals/2/", IsGrant: true, Privilege: "write"},
	})
	if err != nil {
		t.Fatalf("SetACL() error = %v", err)
	}

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("sql expectations: %v", err)
	}
}

func TestACLRepoSetACLTouchesOnlyAffectedCalendarObjectSyncState(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New() error = %v", err)
	}
	defer db.Close()

	repo := &aclRepo{pool: db}
	resourcePath := "/dav/calendars/1/event-1"

	mock.ExpectBegin()
	mock.ExpectQuery(regexp.QuoteMeta(`SELECT id, resource_path, principal_href, is_grant, privilege, created_at FROM acl_entries WHERE resource_path=$1 ORDER BY created_at, id`)).
		WithArgs(resourcePath).
		WillReturnRows(sqlmock.NewRows([]string{"id", "resource_path", "principal_href", "is_grant", "privilege", "created_at"}))
	mock.ExpectExec(regexp.QuoteMeta(`DELETE FROM acl_entries WHERE resource_path=$1`)).
		WithArgs(resourcePath).
		WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectExec(regexp.QuoteMeta(`INSERT INTO acl_entries (resource_path, principal_href, is_grant, privilege, created_at) VALUES ($1, $2, $3, $4, $5)`)).
		WithArgs(resourcePath, "/dav/principals/2/", false, "read", sqlmock.AnyArg()).
		WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectExec(regexp.QuoteMeta(`UPDATE calendars SET ctag = ctag + 1, updated_at = NOW() WHERE id = $1`)).
		WithArgs(int64(1)).
		WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectExec(regexp.QuoteMeta(`UPDATE events SET last_modified = NOW() WHERE calendar_id = $1 AND resource_name IN ($2, $3)`)).
		WithArgs(int64(1), "event-1", "event-1.ics").
		WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectCommit()

	err = repo.SetACL(context.Background(), resourcePath, []ACLEntry{
		{PrincipalHref: "/dav/principals/2/", IsGrant: false, Privilege: "read"},
	})
	if err != nil {
		t.Fatalf("SetACL() error = %v", err)
	}

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("sql expectations: %v", err)
	}
}

func TestACLRepoDeletePrincipalEntriesByResourcePrefixUsesSingleTransaction(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New() error = %v", err)
	}
	defer db.Close()

	repo := &aclRepo{pool: db}
	principalHref := "/dav/principals/2/"
	resourcePrefix := "/dav/calendars/1"
	likePrefix := "/dav/calendars/1/%"

	mock.ExpectBegin()
	mock.ExpectQuery(regexp.QuoteMeta(`SELECT DISTINCT resource_path FROM acl_entries WHERE principal_href=$1 AND (resource_path=$2 OR resource_path LIKE $3 ESCAPE '\') ORDER BY resource_path`)).
		WithArgs(principalHref, resourcePrefix, likePrefix).
		WillReturnRows(sqlmock.NewRows([]string{"resource_path"}).
			AddRow("/dav/calendars/1").
			AddRow("/dav/calendars/1/private-event"))
	mock.ExpectExec(regexp.QuoteMeta(`DELETE FROM acl_entries WHERE principal_href=$1 AND (resource_path=$2 OR resource_path LIKE $3 ESCAPE '\')`)).
		WithArgs(principalHref, resourcePrefix, likePrefix).
		WillReturnResult(sqlmock.NewResult(0, 3))
	mock.ExpectExec(regexp.QuoteMeta(`UPDATE calendars SET ctag = ctag + 1, updated_at = NOW() WHERE id = $1`)).
		WithArgs(int64(1)).
		WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectExec(regexp.QuoteMeta(`UPDATE events SET last_modified = NOW() WHERE calendar_id = $1`)).
		WithArgs(int64(1)).
		WillReturnResult(sqlmock.NewResult(0, 2))
	mock.ExpectExec(regexp.QuoteMeta(`UPDATE calendars SET ctag = ctag + 1, updated_at = NOW() WHERE id = $1`)).
		WithArgs(int64(1)).
		WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectExec(regexp.QuoteMeta(`UPDATE events SET last_modified = NOW() WHERE calendar_id = $1 AND resource_name IN ($2, $3)`)).
		WithArgs(int64(1), "private-event", "private-event.ics").
		WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectCommit()

	err = repo.DeletePrincipalEntriesByResourcePrefix(context.Background(), principalHref, resourcePrefix)
	if err != nil {
		t.Fatalf("DeletePrincipalEntriesByResourcePrefix() error = %v", err)
	}

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("sql expectations: %v", err)
	}
}

func TestLockRepoCreateRejectsDepthInfinityWhenDescendantLocked(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New() error = %v", err)
	}
	defer db.Close()

	repo := &lockRepo{pool: db}
	expiresAt := time.Now().Add(time.Hour)

	mock.ExpectBegin()
	mock.ExpectExec(regexp.QuoteMeta(`SELECT pg_advisory_xact_lock(hashtext($1))`)).
		WithArgs("/dav/addressbooks/5").
		WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectExec(regexp.QuoteMeta(`SELECT pg_advisory_xact_lock(hashtext($1))`)).
		WithArgs("/dav/addressbooks").
		WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectExec(regexp.QuoteMeta(`SELECT pg_advisory_xact_lock(hashtext($1))`)).
		WithArgs("/dav").
		WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectQuery(regexp.QuoteMeta(`SELECT lock_scope FROM locks WHERE resource_path = $1 AND expires_at > NOW()`)).
		WithArgs("/dav/addressbooks/5").
		WillReturnRows(sqlmock.NewRows([]string{"lock_scope"}))
	mock.ExpectQuery(regexp.QuoteMeta(`SELECT lock_scope FROM locks WHERE resource_path = ANY($1) AND depth = 'infinity' AND expires_at > NOW()`)).
		WithArgs(sqlmock.AnyArg()).
		WillReturnRows(sqlmock.NewRows([]string{"lock_scope"}))
	mock.ExpectQuery(regexp.QuoteMeta(`SELECT lock_scope FROM locks WHERE resource_path LIKE $1 ESCAPE '\' AND expires_at > NOW()`)).
		WithArgs(`/dav/addressbooks/5/%`).
		WillReturnRows(sqlmock.NewRows([]string{"lock_scope"}).AddRow("exclusive"))
	mock.ExpectRollback()

	_, err = repo.Create(context.Background(), Lock{
		Token:          "opaquelocktoken:new",
		ResourcePath:   "/dav/addressbooks/5",
		UserID:         1,
		LockScope:      "exclusive",
		LockType:       "write",
		Depth:          "infinity",
		TimeoutSeconds: 3600,
		ExpiresAt:      expiresAt,
	})
	if err != ErrLockConflict {
		t.Fatalf("Create() error = %v, want ErrLockConflict", err)
	}

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("sql expectations: %v", err)
	}
}

func TestLockRepoCreateRejectsInvalidDepth(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New() error = %v", err)
	}
	defer db.Close()

	repo := &lockRepo{pool: db}

	_, err = repo.Create(context.Background(), Lock{
		Token:          "opaquelocktoken:bad-depth",
		ResourcePath:   "/dav/addressbooks/5/alice.vcf",
		UserID:         1,
		LockScope:      "exclusive",
		LockType:       "write",
		Depth:          "1",
		TimeoutSeconds: 3600,
		ExpiresAt:      time.Now().Add(time.Hour),
	})
	if err == nil {
		t.Fatal("Create() error = nil, want invalid depth error")
	}
	if !strings.Contains(err.Error(), "invalid lock depth") {
		t.Fatalf("Create() error = %v, want invalid lock depth", err)
	}

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("sql expectations: %v", err)
	}
}
