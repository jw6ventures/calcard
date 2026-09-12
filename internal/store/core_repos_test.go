package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"regexp"
	"strconv"
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

	mock.ExpectQuery(regexp.QuoteMeta(`INSERT INTO calendars (user_id, name, slug, description, description_lang, timezone, color, supported_components) VALUES ($1, $2, $3, $4, $5, $6, $7, $8) RETURNING id, user_id, name, slug, description, description_lang, timezone, color, supported_components, ctag, created_at, updated_at`)).
		WithArgs(int64(4), "Primary", nil, &description, nil, &timezone, &color, nil).
		WillReturnRows(sqlmock.NewRows([]string{"id", "user_id", "name", "slug", "description", "description_lang", "timezone", "color", "supported_components", "ctag", "created_at", "updated_at"}).
			AddRow(int64(10), int64(4), "Primary", nil, description, nil, timezone, color, nil, int64(3), now, now))

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

	mock.ExpectExec(regexp.QuoteMeta(`UPDATE calendars SET name=$1, description=$2, timezone=$3, color=$4, updated_at=NOW() WHERE id=$5 AND user_id=$6`)).
		WithArgs("Renamed", &description, &timezone, &color, int64(10), int64(4)).
		WillReturnResult(sqlmock.NewResult(0, 1))
	if err := repo.Update(context.Background(), 4, 10, "Renamed", &description, &timezone, &color); err != nil {
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

func TestNormalizeCalendarColorOpaqueAddsAlpha(t *testing.T) {
	got, err := NormalizeCalendarColorOpaque(" #22cc88 ")
	if err != nil {
		t.Fatalf("NormalizeCalendarColorOpaque() error = %v", err)
	}
	if got == nil || *got != "#22CC88FF" {
		t.Fatalf("NormalizeCalendarColorOpaque() = %v, want #22CC88FF", got)
	}

	got, err = NormalizeCalendarColorOpaque("#33669980")
	if err != nil {
		t.Fatalf("NormalizeCalendarColorOpaque() error = %v", err)
	}
	if got == nil || *got != "#33669980" {
		t.Fatalf("NormalizeCalendarColorOpaque() = %v, want #33669980", got)
	}
}

func TestCalendarRepoAccessQueriesReturnNilWhenMissing(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New() error = %v", err)
	}
	defer db.Close()

	repo := &calendarRepo{pool: db}

	mock.ExpectQuery(regexp.QuoteMeta(`SELECT id, user_id, name, slug, description, description_lang, timezone, color, supported_components, ctag, created_at, updated_at FROM calendars WHERE id=$1`)).
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
		regexp.QuoteMeta(`SELECT c.id, c.user_id, c.name, c.slug, c.description, c.description_lang, c.timezone, c.color, c.supported_components, c.ctag, c.created_at, c.updated_at,`)+
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

	mock.ExpectQuery(`(?s)SELECT c.id, c.user_id, c.name, c.slug, c.description, c.description_lang, c.timezone, c.color, c.supported_components, c.ctag, c.created_at, c.updated_at,.*FROM calendars c.*acl_entries.*ORDER BY shared, name`).
		WithArgs(int64(4)).
		WillReturnRows(sqlmock.NewRows([]string{"id", "user_id", "name", "slug", "description", "description_lang", "timezone", "color", "supported_components", "ctag", "created_at", "updated_at", "owner_email", "shared", "can_read", "can_read_free_busy", "can_write", "can_write_content", "can_write_properties", "can_bind", "can_unbind"}).
			AddRow(int64(1), int64(4), "Owned", nil, nil, nil, nil, nil, nil, int64(1), now, now, "owner@example.com", false, true, true, true, true, true, true, true).
			AddRow(int64(2), int64(9), "Shared", "shared", "Desc", nil, "UTC", "#123456", nil, int64(3), now, now, "other@example.com", true, true, false, false, false, false, true, false))

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

func TestACLAccessSQLUsesFirstMatchingACEOrder(t *testing.T) {
	for name, expression := range map[string]string{
		"calendar": calendarACLBooleanExpr("$1", "read", "all"),
		"object":   calendarEventACLAllowsExpr("$1", "read", "all"),
	} {
		t.Run(name, func(t *testing.T) {
			if !strings.Contains(expression, "ORDER BY a.ace_order, a.id") || !strings.Contains(expression, "LIMIT 1") {
				t.Fatalf("ACL SQL does not evaluate the first matching ACE in stored order: %s", expression)
			}
			if strings.Contains(expression, "NOT EXISTS") {
				t.Fatalf("ACL SQL still gives unordered denies global precedence: %s", expression)
			}
		})
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

	mock.ExpectQuery(`(?s)SELECT c.id, c.user_id, c.name, c.slug, c.description, c.description_lang, c.timezone, c.color, c.supported_components, c.ctag, c.created_at, c.updated_at,.*FROM calendars c.*WHERE c.user_id = \$1.*read-free-busy.*ORDER BY shared, name`).
		WithArgs(int64(4)).
		WillReturnRows(sqlmock.NewRows([]string{"id", "user_id", "name", "slug", "description", "description_lang", "timezone", "color", "supported_components", "ctag", "created_at", "updated_at", "owner_email", "shared", "can_read", "can_read_free_busy", "can_write", "can_write_content", "can_write_properties", "can_bind", "can_unbind"}).
			AddRow(int64(7), int64(9), "Busy Only", nil, nil, nil, nil, nil, nil, int64(5), now, now, "owner@example.com", true, false, true, false, false, false, false, false))

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

	mock.ExpectQuery(`(?s)SELECT c.id, c.user_id, c.name, c.slug, c.description, c.description_lang, c.timezone, c.color, c.supported_components, c.ctag, c.created_at, c.updated_at,.*FROM calendars c.*WHERE c.id = \$1.*read-free-busy.*`).
		WithArgs(int64(7), int64(4)).
		WillReturnRows(sqlmock.NewRows([]string{"id", "user_id", "name", "slug", "description", "description_lang", "timezone", "color", "supported_components", "ctag", "created_at", "updated_at", "owner_email", "shared", "can_read", "can_read_free_busy", "can_write", "can_write_content", "can_write_properties", "can_bind", "can_unbind"}).
			AddRow(int64(7), int64(9), "Busy Only", nil, nil, nil, nil, nil, nil, int64(5), now, now, "owner@example.com", true, false, true, false, false, false, false, false))

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

	mock.ExpectQuery(`(?s)SELECT c.id, c.user_id, c.name, c.slug, c.description, c.description_lang, c.timezone, c.color, c.supported_components, c.ctag, c.created_at, c.updated_at,.*FROM calendars c.*WHERE c.user_id = \$1.*bind.*ORDER BY shared, name`).
		WithArgs(int64(4)).
		WillReturnRows(sqlmock.NewRows([]string{"id", "user_id", "name", "slug", "description", "description_lang", "timezone", "color", "supported_components", "ctag", "created_at", "updated_at", "owner_email", "shared", "can_read", "can_read_free_busy", "can_write", "can_write_content", "can_write_properties", "can_bind", "can_unbind"}).
			AddRow(int64(8), int64(9), "Inbox", nil, nil, nil, nil, nil, nil, int64(6), now, now, "owner@example.com", true, false, false, false, false, false, true, false))

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

	mock.ExpectQuery(`(?s)SELECT c.id, c.user_id, c.name, c.slug, c.description, c.description_lang, c.timezone, c.color, c.supported_components, c.ctag, c.created_at, c.updated_at,.*FROM calendars c.*WHERE c.id = \$1.*bind.*`).
		WithArgs(int64(8), int64(4)).
		WillReturnRows(sqlmock.NewRows([]string{"id", "user_id", "name", "slug", "description", "description_lang", "timezone", "color", "supported_components", "ctag", "created_at", "updated_at", "owner_email", "shared", "can_read", "can_read_free_busy", "can_write", "can_write_content", "can_write_properties", "can_bind", "can_unbind"}).
			AddRow(int64(8), int64(9), "Inbox", nil, nil, nil, nil, nil, nil, int64(6), now, now, "owner@example.com", true, false, false, false, false, false, true, false))

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

	mock.ExpectQuery(`(?s)SELECT c.id, c.user_id, c.name, c.slug, c.description, c.description_lang, c.timezone, c.color, c.supported_components, c.ctag, c.created_at, c.updated_at,.*FROM calendars c.*JOIN events e.*e.object_acl_path = g0.resource_path_norm.*ORDER BY shared, name`).
		WithArgs(int64(4)).
		WillReturnRows(sqlmock.NewRows([]string{"id", "user_id", "name", "slug", "description", "description_lang", "timezone", "color", "supported_components", "ctag", "created_at", "updated_at", "owner_email", "shared", "can_read", "can_read_free_busy", "can_write", "can_write_content", "can_write_properties", "can_bind", "can_unbind"}).
			AddRow(int64(12), int64(9), "Object Shared", nil, nil, nil, nil, nil, nil, int64(7), now, now, "owner@example.com", true, false, false, false, false, false, false, false))

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

	mock.ExpectQuery(`(?s)SELECT c.id, c.user_id, c.name, c.slug, c.description, c.description_lang, c.timezone, c.color, c.supported_components, c.ctag, c.created_at, c.updated_at,.*FROM calendars c.*WHERE c.id = \$1.*JOIN events e.*e.object_acl_path = g0.resource_path_norm`).
		WithArgs(int64(12), int64(4)).
		WillReturnRows(sqlmock.NewRows([]string{"id", "user_id", "name", "slug", "description", "description_lang", "timezone", "color", "supported_components", "ctag", "created_at", "updated_at", "owner_email", "shared", "can_read", "can_read_free_busy", "can_write", "can_write_content", "can_write_properties", "can_bind", "can_unbind"}).
			AddRow(int64(12), int64(9), "Object Shared", nil, nil, nil, nil, nil, nil, int64(7), now, now, "owner@example.com", true, false, false, false, false, false, false, false))

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
INSERT INTO events (calendar_id, uid, resource_name, raw_ical, etag, summary, description, location, dtstart, dtend, all_day, recurrence_start, recurrence_until, last_modified)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, NOW())
ON CONFLICT (calendar_id, uid) DO UPDATE SET
        resource_name = EXCLUDED.resource_name,
        raw_ical = EXCLUDED.raw_ical,
        etag = EXCLUDED.etag,
        summary = EXCLUDED.summary,
        description = EXCLUDED.description,
        location = EXCLUDED.location,
        dtstart = EXCLUDED.dtstart,
        dtend = EXCLUDED.dtend,
        all_day = EXCLUDED.all_day,
        recurrence_start = EXCLUDED.recurrence_start,
        recurrence_until = EXCLUDED.recurrence_until,
        last_modified = NOW()
RETURNING id, calendar_id, uid, resource_name, raw_ical, etag, summary, description, location, dtstart, dtend, all_day, last_modified
`)).
		WithArgs(int64(7), "test-uid", "test-uid", rawICAL, "etag-1", "Planning Day", nil, nil, dtstart, dtend, true, nil, nil).
		WillReturnRows(sqlmock.NewRows([]string{"id", "calendar_id", "uid", "resource_name", "raw_ical", "etag", "summary", "description", "location", "dtstart", "dtend", "all_day", "last_modified"}).
			AddRow(int64(1), int64(7), "test-uid", "test-uid", rawICAL, "etag-1", "Planning Day", nil, nil, dtstart, dtend, true, now))

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
	mock.ExpectQuery(regexp.QuoteMeta(`SELECT id, calendar_id, uid, resource_name, raw_ical, etag, summary, description, location, dtstart, dtend, all_day, last_modified FROM events WHERE calendar_id=$1 ORDER BY last_modified DESC LIMIT $2 OFFSET $3`)).
		WithArgs(int64(7), 1, 1).
		WillReturnRows(sqlmock.NewRows([]string{"id", "calendar_id", "uid", "resource_name", "raw_ical", "etag", "summary", "description", "location", "dtstart", "dtend", "all_day", "last_modified"}).
			AddRow(int64(2), int64(7), "other", "other.ics", rawICAL, "etag-2", nil, nil, nil, nil, nil, false, now))

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
	mock.ExpectQuery(regexp.QuoteMeta(`SELECT id, calendar_id, uid, resource_name, raw_ical, etag, summary, description, location, dtstart, dtend, all_day, last_modified FROM events WHERE calendar_id=$1 AND uid=$2`)).
		WithArgs(int64(5), "event-1").
		WillReturnRows(sqlmock.NewRows([]string{"id", "calendar_id", "uid", "resource_name", "raw_ical", "etag", "summary", "description", "location", "dtstart", "dtend", "all_day", "last_modified"}).
			AddRow(int64(1), int64(5), "event-1", "source-name", "BEGIN:VCALENDAR", "etag-src", nil, nil, nil, nil, nil, false, now))
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

	mock.ExpectQuery(regexp.QuoteMeta(`SELECT id, calendar_id, uid, resource_name, raw_ical, etag, summary, description, location, dtstart, dtend, all_day, last_modified FROM events WHERE calendar_id=$1 AND uid=$2`)).
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

	mock.ExpectQuery(regexp.QuoteMeta(`SELECT id, calendar_id, uid, resource_name, raw_ical, etag, summary, description, location, dtstart, dtend, all_day, last_modified FROM events WHERE calendar_id=$1 AND uid = ANY($2)`)).
		WithArgs(int64(7), sqlmock.AnyArg()).
		WillReturnRows(sqlmock.NewRows([]string{"id", "calendar_id", "uid", "resource_name", "raw_ical", "etag", "summary", "description", "location", "dtstart", "dtend", "all_day", "last_modified"}).
			AddRow(int64(1), int64(7), "uid-1", "uid-1.ics", "BEGIN:VCALENDAR", "etag-1", "Meeting", nil, nil, now, now.Add(time.Hour), false, now))
	byUIDs, err := eventRepo.ListByUIDs(context.Background(), 7, []string{"uid-1"})
	if err != nil {
		t.Fatalf("ListByUIDs() error = %v", err)
	}
	if len(byUIDs) != 1 || byUIDs[0].Summary == nil || *byUIDs[0].Summary != "Meeting" {
		t.Fatalf("ListByUIDs() = %#v", byUIDs)
	}

	mock.ExpectQuery(regexp.QuoteMeta(`SELECT id, calendar_id, uid, resource_name, raw_ical, etag, summary, description, location, dtstart, dtend, all_day, last_modified FROM events WHERE calendar_id=$1 AND resource_name=$2`)).
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
	mock.ExpectQuery(regexp.QuoteMeta(`SELECT id, calendar_id, uid, resource_name, raw_ical, etag, summary, description, location, dtstart, dtend, all_day, last_modified FROM events WHERE calendar_id=$1 AND id>$2 AND last_modified > $3 ORDER BY id ASC LIMIT $4`)).
		WithArgs(int64(7), int64(1), since, 256).
		WillReturnRows(sqlmock.NewRows([]string{"id", "calendar_id", "uid", "resource_name", "raw_ical", "etag", "summary", "description", "location", "dtstart", "dtend", "all_day", "last_modified"}).
			AddRow(int64(2), int64(7), "uid-2", "uid-2.ics", "BEGIN:VCALENDAR", "etag-2", "Recent", nil, nil, nil, nil, true, now))
	modified, err := eventRepo.ListModifiedSincePageAfter(context.Background(), 7, 1, since, 256)
	if err != nil {
		t.Fatalf("ListModifiedSincePageAfter() error = %v", err)
	}
	if len(modified) != 1 || !modified[0].AllDay {
		t.Fatalf("ListModifiedSincePageAfter() = %#v", modified)
	}
	empty, err := eventRepo.ListModifiedSincePageAfter(context.Background(), 7, 1, since, 0)
	if err != nil {
		t.Fatalf("ListModifiedSincePageAfter(limit 0) error = %v", err)
	}
	if len(empty) != 0 {
		t.Fatalf("ListModifiedSincePageAfter(limit 0) = %#v", empty)
	}

	// The unfiltered calendar page is the read idx_events_calendar_keyset
	// exists for, and it is the one statement assembled through a builder
	// rather than written as a literal -- so the assembled text is what has to
	// be pinned. A leading column or an ORDER BY that stopped matching the
	// index would drop the read back onto the primary key while staying
	// correct, which no result assertion can see.
	eventColumnNames := []string{"id", "calendar_id", "uid", "resource_name", "raw_ical", "etag", "summary", "description", "location", "dtstart", "dtend", "all_day", "last_modified"}
	mock.ExpectQuery(regexp.QuoteMeta(`SELECT id, calendar_id, uid, resource_name, raw_ical, etag, summary, description, location, dtstart, dtend, all_day, last_modified FROM events WHERE calendar_id=$1 AND id>$2 ORDER BY id ASC LIMIT $3`)).
		WithArgs(int64(7), int64(4), 256).
		WillReturnRows(sqlmock.NewRows(eventColumnNames).
			AddRow(int64(5), int64(7), "uid-5", "uid-5.ics", "BEGIN:VCALENDAR", "etag-5", "Paged", nil, nil, nil, nil, true, now))
	page, err := eventRepo.ListForCalendarPageAfter(context.Background(), 7, 4, 256, EventFilter{})
	if err != nil {
		t.Fatalf("ListForCalendarPageAfter() error = %v", err)
	}
	if len(page) != 1 || page[0].ID != 5 {
		t.Fatalf("ListForCalendarPageAfter() = %#v", page)
	}

	// A time-range narrows the same page. The filter is appended between the
	// keyset predicate and the ORDER BY, so this pins that the range does not
	// displace either of them.
	rangeStart := now.Add(-24 * time.Hour)
	rangeEnd := now.Add(24 * time.Hour)
	mock.ExpectQuery(regexp.QuoteMeta(`SELECT id, calendar_id, uid, resource_name, raw_ical, etag, summary, description, location, dtstart, dtend, all_day, last_modified FROM events WHERE calendar_id=$1 AND id>$2 AND COALESCE(recurrence_until, dtend, 'infinity'::timestamptz) >= $3 AND COALESCE(recurrence_start, dtstart, '-infinity'::timestamptz) <= $4 ORDER BY id ASC LIMIT $5`)).
		WithArgs(int64(7), int64(4), rangeStart.UTC(), rangeEnd.UTC(), 256).
		WillReturnRows(sqlmock.NewRows(eventColumnNames).
			AddRow(int64(6), int64(7), "uid-6", "uid-6.ics", "BEGIN:VCALENDAR", "etag-6", "In range", nil, nil, nil, nil, true, now))
	filtered, err := eventRepo.ListForCalendarPageAfter(context.Background(), 7, 4, 256, EventFilter{Start: &rangeStart, End: &rangeEnd})
	if err != nil {
		t.Fatalf("ListForCalendarPageAfter(filtered) error = %v", err)
	}
	if len(filtered) != 1 || filtered[0].ID != 6 {
		t.Fatalf("ListForCalendarPageAfter(filtered) = %#v", filtered)
	}

	emptyPage, err := eventRepo.ListForCalendarPageAfter(context.Background(), 7, 4, 0, EventFilter{})
	if err != nil {
		t.Fatalf("ListForCalendarPageAfter(limit 0) error = %v", err)
	}
	if len(emptyPage) != 0 {
		t.Fatalf("ListForCalendarPageAfter(limit 0) = %#v", emptyPage)
	}

	mock.ExpectQuery(`(?s)SELECT e.id, e.calendar_id, e.uid, e.resource_name, e.raw_ical, e.etag, e.summary, e.description, e.location, e.dtstart, e.dtend, e.all_day, e.last_modified.*FROM events e.*acl_entries.*ORDER BY e.last_modified DESC.*LIMIT \$2`).
		WithArgs(int64(4), 2).
		WillReturnRows(sqlmock.NewRows([]string{"id", "calendar_id", "uid", "resource_name", "raw_ical", "etag", "summary", "description", "location", "dtstart", "dtend", "all_day", "last_modified"}).
			AddRow(int64(3), int64(8), "uid-3", "uid-3.ics", "BEGIN:VCALENDAR", "etag-3", nil, nil, nil, nil, nil, false, now))
	recent, err := eventRepo.ListRecentByUser(context.Background(), 4, 2)
	if err != nil {
		t.Fatalf("ListRecentByUser() error = %v", err)
	}
	if len(recent) != 1 || recent[0].UID != "uid-3" {
		t.Fatalf("ListRecentByUser() = %#v", recent)
	}

	mock.ExpectQuery(`(?s)SELECT e.id, e.calendar_id, e.uid, e.resource_name, e.raw_ical, e.etag, e.summary, e.description, e.location, e.dtstart, e.dtend, e.all_day, e.last_modified.*resource_path_norm = e.object_acl_path.*LIMIT \$2`).
		WithArgs(int64(4), 2).
		WillReturnRows(sqlmock.NewRows([]string{"id", "calendar_id", "uid", "resource_name", "raw_ical", "etag", "summary", "description", "location", "dtstart", "dtend", "all_day", "last_modified"}).
			AddRow(int64(6), int64(8), "uid-object", "uid-object", "BEGIN:VCALENDAR", "etag-6", "Direct Grant", nil, nil, nil, nil, false, now))
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
	mock.ExpectQuery(regexp.QuoteMeta(`SELECT resource_name FROM contacts WHERE address_book_id=$1 AND uid=$2`)).
		WithArgs(int64(9), "contact-1").
		WillReturnRows(sqlmock.NewRows([]string{"resource_name"}))
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

func TestEventRepoUpsertMapsResourceNameConflictsToErrConflict(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New() error = %v", err)
	}
	defer db.Close()

	repo := &eventRepo{pool: db}
	rawICAL := "BEGIN:VCALENDAR\r\nBEGIN:VEVENT\r\nUID:ev-1\r\nSUMMARY:Meeting\r\nEND:VEVENT\r\nEND:VCALENDAR"

	mock.ExpectQuery(regexp.QuoteMeta(`
INSERT INTO events (calendar_id, uid, resource_name, raw_ical, etag, summary, description, location, dtstart, dtend, all_day, recurrence_start, recurrence_until, last_modified)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, NOW())
ON CONFLICT (calendar_id, uid) DO UPDATE SET
        resource_name = EXCLUDED.resource_name,
        raw_ical = EXCLUDED.raw_ical,
        etag = EXCLUDED.etag,
        summary = EXCLUDED.summary,
        description = EXCLUDED.description,
        location = EXCLUDED.location,
        dtstart = EXCLUDED.dtstart,
        dtend = EXCLUDED.dtend,
        all_day = EXCLUDED.all_day,
        recurrence_start = EXCLUDED.recurrence_start,
        recurrence_until = EXCLUDED.recurrence_until,
        last_modified = NOW()
RETURNING id, calendar_id, uid, resource_name, raw_ical, etag, summary, description, location, dtstart, dtend, all_day, last_modified
`)).
		WillReturnError(&pq.Error{Code: "23505", Constraint: "events_calendar_resource_name_unique"})

	_, err = repo.Upsert(context.Background(), Event{
		CalendarID:   7,
		UID:          "ev-1",
		ResourceName: "renamed",
		RawICAL:      rawICAL,
		ETag:         "etag-1",
	})
	if err != ErrConflict {
		t.Fatalf("Upsert() error = %v, want ErrConflict", err)
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

func TestStoreCreateCalendarAndStateRunsInSingleTransaction(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New() error = %v", err)
	}
	defer db.Close()

	st := New(db)
	now := time.Now().UTC()

	mock.ExpectBegin()
	mock.ExpectQuery(regexp.QuoteMeta(createCalendarQuery)).
		WithArgs(int64(4), "Work", nil, nil, nil, nil, nil, nil).
		WillReturnRows(sqlmock.NewRows([]string{"id", "user_id", "name", "slug", "description", "description_lang", "timezone", "color", "supported_components", "ctag", "created_at", "updated_at"}).
			AddRow(int64(12), int64(4), "Work", nil, nil, nil, nil, nil, nil, int64(1), now, now))
	mock.ExpectExec(regexp.QuoteMeta(`INSERT INTO dav_dead_properties (resource_path, namespace_uri, local_name, inner_xml)`)).
		WithArgs("/dav/calendars/12", "urn:example:custom", "note", "keep").
		WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectExec(regexp.QuoteMeta(`DELETE FROM locks WHERE resource_path=$1 AND expires_at > NOW()`)).
		WithArgs("/dav/calendars/12").
		WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectExec(regexp.QuoteMeta(`UPDATE locks SET resource_path=$1 WHERE resource_path=$2 AND expires_at > NOW()`)).
		WithArgs("/dav/calendars/12", "/dav/calendars/work").
		WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectCommit()

	created, err := st.CreateCalendarAndState(
		context.Background(),
		Calendar{UserID: 4, Name: "Work"},
		[]DeadPropertyMutation{{NamespaceURI: "urn:example:custom", LocalName: "note", InnerXML: "keep"}},
		nil,
		"/dav/calendars/work",
		func(id int64) string { return "/dav/calendars/" + strconv.FormatInt(id, 10) },
	)
	if err != nil {
		t.Fatalf("CreateCalendarAndState() error = %v", err)
	}
	if created.ID != 12 {
		t.Fatalf("CreateCalendarAndState() id = %d, want 12", created.ID)
	}

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("sql expectations: %v", err)
	}
}

func TestStoreCreateCalendarAndStateRechecksLocksInsideTransaction(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New() error = %v", err)
	}
	defer db.Close()

	st := New(db)
	pendingPath := "/dav/calendars/.pending/4/work"
	lookupPaths := []string{pendingPath, "/dav/calendars"}
	preconditions := []LockPrecondition{{
		ResourcePath: pendingPath,
		LookupPaths:  lookupPaths,
	}}

	mock.ExpectBegin()
	for _, resourcePath := range sortedLockSerializationPaths(pendingPath) {
		mock.ExpectExec(regexp.QuoteMeta(`SELECT pg_advisory_xact_lock(hashtext($1))`)).
			WithArgs(resourcePath).
			WillReturnResult(sqlmock.NewResult(0, 1))
	}
	mock.ExpectQuery(regexp.QuoteMeta(`SELECT token, resource_path, depth, expires_at FROM locks WHERE resource_path = ANY($1) AND expires_at > NOW() ORDER BY created_at`)).
		WithArgs(pq.Array(lookupPaths)).
		WillReturnRows(sqlmock.NewRows([]string{"token", "resource_path", "depth", "expires_at"}).
			AddRow("opaquelocktoken:concurrent", pendingPath, "0", time.Now().Add(time.Hour)))
	mock.ExpectRollback()

	_, err = st.CreateCalendarAndState(
		context.Background(),
		Calendar{UserID: 4, Name: "Work"},
		nil,
		preconditions,
		pendingPath,
		func(id int64) string { return "/dav/calendars/" + strconv.FormatInt(id, 10) },
	)
	if !errors.Is(err, ErrLockConflict) {
		t.Fatalf("CreateCalendarAndState() error = %v, want ErrLockConflict", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("sql expectations: %v", err)
	}
}

func TestLockPreconditionsSatisfied(t *testing.T) {
	now := time.Now()
	target := "/dav/calendars/.pending/4/work"
	parent := "/dav/calendars"
	precondition := LockPrecondition{
		ResourcePath: target,
		LookupPaths:  []string{target, parent},
		Tokens:       []string{"opaquelocktoken:allowed"},
	}

	tests := []struct {
		name  string
		locks []Lock
		want  bool
	}{
		{name: "no locks", want: true},
		{
			name:  "matching target token",
			locks: []Lock{{Token: "opaquelocktoken:allowed", ResourcePath: target, Depth: "0", ExpiresAt: now.Add(time.Hour)}},
			want:  true,
		},
		{
			name:  "missing target token",
			locks: []Lock{{Token: "opaquelocktoken:other", ResourcePath: target, Depth: "0", ExpiresAt: now.Add(time.Hour)}},
			want:  false,
		},
		{
			name:  "depth zero ancestor does not apply",
			locks: []Lock{{Token: "opaquelocktoken:other", ResourcePath: parent, Depth: "0", ExpiresAt: now.Add(time.Hour)}},
			want:  true,
		},
		{
			name:  "depth infinity ancestor applies",
			locks: []Lock{{Token: "opaquelocktoken:other", ResourcePath: parent, Depth: "infinity", ExpiresAt: now.Add(time.Hour)}},
			want:  false,
		},
		{
			name:  "expired lock does not apply",
			locks: []Lock{{Token: "opaquelocktoken:other", ResourcePath: target, Depth: "0", ExpiresAt: now.Add(-time.Hour)}},
			want:  true,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if got := LockPreconditionsSatisfied([]LockPrecondition{precondition}, test.locks); got != test.want {
				t.Fatalf("LockPreconditionsSatisfied() = %v, want %v", got, test.want)
			}
		})
	}
}

// A MKCALENDAR that fails after its dead properties are written must not leave
// them behind: dav_dead_properties is keyed by path with no foreign key to the
// calendar, so only the transaction can retract them.
func TestStoreCreateCalendarAndStateRollsBackDeadPropertiesWhenLockRebindFails(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New() error = %v", err)
	}
	defer db.Close()

	st := New(db)
	now := time.Now().UTC()

	mock.ExpectBegin()
	mock.ExpectQuery(regexp.QuoteMeta(createCalendarQuery)).
		WithArgs(int64(4), "Work", nil, nil, nil, nil, nil, nil).
		WillReturnRows(sqlmock.NewRows([]string{"id", "user_id", "name", "slug", "description", "description_lang", "timezone", "color", "supported_components", "ctag", "created_at", "updated_at"}).
			AddRow(int64(12), int64(4), "Work", nil, nil, nil, nil, nil, nil, int64(1), now, now))
	mock.ExpectExec(regexp.QuoteMeta(`INSERT INTO dav_dead_properties (resource_path, namespace_uri, local_name, inner_xml)`)).
		WithArgs("/dav/calendars/12", "urn:example:custom", "note", "keep").
		WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectExec(regexp.QuoteMeta(`DELETE FROM locks WHERE resource_path=$1 AND expires_at > NOW()`)).
		WithArgs("/dav/calendars/12").
		WillReturnError(errors.New("lock rebind failed"))
	mock.ExpectRollback()

	_, err = st.CreateCalendarAndState(
		context.Background(),
		Calendar{UserID: 4, Name: "Work"},
		[]DeadPropertyMutation{{NamespaceURI: "urn:example:custom", LocalName: "note", InnerXML: "keep"}},
		nil,
		"/dav/calendars/work",
		func(id int64) string { return "/dav/calendars/" + strconv.FormatInt(id, 10) },
	)
	if err == nil {
		t.Fatal("CreateCalendarAndState() error = nil, want error")
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
	now := time.Now().UTC()
	eventColumns := []string{"id", "calendar_id", "uid", "resource_name", "raw_ical", "etag", "summary", "description", "location", "dtstart", "dtend", "all_day", "last_modified"}

	mock.ExpectBegin()
	expectDAVObjectIdentityLocks(mock, "calendar-object", 7, "renamed", "event-1")
	mock.ExpectQuery(`SELECT .* FROM events WHERE calendar_id=\$1 AND resource_name=\$2 FOR UPDATE`).
		WithArgs(int64(7), "renamed").
		WillReturnRows(sqlmock.NewRows(eventColumns).AddRow(int64(10), int64(7), "event-1", "renamed", "raw", "etag", nil, nil, nil, nil, nil, false, now))
	mock.ExpectExec(regexp.QuoteMeta(`DELETE FROM events WHERE id=$1`)).
		WithArgs(int64(10)).WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectExec(regexp.QuoteMeta(`DELETE FROM locks WHERE resource_path=$1`)).
		WithArgs("/dav/calendars/7/renamed").
		WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectExec(regexp.QuoteMeta(`DELETE FROM acl_entries WHERE resource_path=$1`)).
		WithArgs("/dav/calendars/7/renamed").
		WillReturnError(errors.New("acl delete failed"))
	mock.ExpectRollback()

	err = st.DeleteEventAndState(context.Background(), 7, DAVResourceState{Exists: true, UID: "event-1", ResourceName: "renamed", ETag: "etag", RawData: "raw"}, "/dav/calendars/7/renamed", nil)
	if err == nil {
		t.Fatal("DeleteEventAndState() error = nil, want error")
	}

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("sql expectations: %v", err)
	}
}

func TestStoreDeleteEventAndStateRechecksLocksInsideTransaction(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New() error = %v", err)
	}
	defer db.Close()

	st := New(db)
	resourcePath := "/dav/calendars/7/event"
	lookupPaths := []string{resourcePath, "/dav/calendars/7"}
	preconditions := []LockPrecondition{{ResourcePath: resourcePath, LookupPaths: lookupPaths}}

	mock.ExpectBegin()
	for _, lockPath := range sortedLockSerializationPaths(resourcePath) {
		mock.ExpectExec(regexp.QuoteMeta(`SELECT pg_advisory_xact_lock(hashtext($1))`)).
			WithArgs(lockPath).
			WillReturnResult(sqlmock.NewResult(0, 1))
	}
	mock.ExpectQuery(regexp.QuoteMeta(`SELECT token, resource_path, depth, expires_at FROM locks WHERE resource_path = ANY($1) AND expires_at > NOW() ORDER BY created_at`)).
		WithArgs(pq.Array(lookupPaths)).
		WillReturnRows(sqlmock.NewRows([]string{"token", "resource_path", "depth", "expires_at"}).
			AddRow("opaquelocktoken:concurrent", resourcePath, "0", time.Now().Add(time.Hour)))
	mock.ExpectRollback()

	err = st.DeleteEventAndState(context.Background(), 7, DAVResourceState{Exists: true, UID: "event", ResourceName: "event"}, resourcePath, preconditions)
	if !errors.Is(err, ErrLockConflict) {
		t.Fatalf("DeleteEventAndState() error = %v, want ErrLockConflict", err)
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
	now := time.Now().UTC()
	contactColumns := []string{"id", "address_book_id", "uid", "resource_name", "raw_vcard", "etag", "display_name", "primary_email", "birthday", "last_modified"}

	mock.ExpectBegin()
	expectDAVObjectIdentityLocks(mock, "contact-object", 5, "contact.v1", "contact-1")
	mock.ExpectQuery(`SELECT .* FROM contacts WHERE address_book_id=\$1 AND resource_name=\$2 FOR UPDATE`).
		WithArgs(int64(5), "contact.v1").
		WillReturnRows(sqlmock.NewRows(contactColumns).AddRow(int64(10), int64(5), "contact-1", "contact.v1", "raw", "etag", nil, nil, nil, now))
	mock.ExpectExec(regexp.QuoteMeta(`DELETE FROM contacts WHERE id=$1`)).
		WithArgs(int64(10)).WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectExec(regexp.QuoteMeta(`DELETE FROM locks WHERE resource_path=$1`)).
		WithArgs("/dav/addressbooks/5/contact.v1").
		WillReturnError(errors.New("lock delete failed"))
	mock.ExpectRollback()

	err = st.DeleteContactAndState(context.Background(), 5, DAVResourceState{Exists: true, UID: "contact-1", ResourceName: "contact.v1", ETag: "etag", RawData: "raw"}, "/dav/addressbooks/5/contact.v1", nil)
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
	now := time.Now().UTC()
	contactColumns := []string{"id", "address_book_id", "uid", "resource_name", "raw_vcard", "etag", "display_name", "primary_email", "birthday", "last_modified"}

	mock.ExpectBegin()
	expectDAVObjectIdentityLocks(mock, "contact-object", 5, "contact-1", "contact-1")
	mock.ExpectQuery(`SELECT .* FROM contacts WHERE address_book_id=\$1 AND resource_name=\$2 FOR UPDATE`).
		WithArgs(int64(5), "contact-1").
		WillReturnRows(sqlmock.NewRows(contactColumns).AddRow(int64(10), int64(5), "contact-1", "contact-1", "raw", "etag", nil, nil, nil, now))
	mock.ExpectExec(regexp.QuoteMeta(`DELETE FROM contacts WHERE id=$1`)).
		WithArgs(int64(10)).WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectExec(regexp.QuoteMeta(`DELETE FROM locks WHERE resource_path=$1`)).
		WithArgs("/dav/addressbooks/5/contact-1").
		WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectExec(regexp.QuoteMeta(`DELETE FROM acl_entries WHERE resource_path=$1`)).
		WithArgs("/dav/addressbooks/5/contact-1").
		WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectExec(regexp.QuoteMeta(`DELETE FROM dav_dead_properties WHERE resource_path=$1`)).
		WithArgs("/dav/addressbooks/5/contact-1").
		WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectExec(regexp.QuoteMeta(`DELETE FROM locks WHERE resource_path=$1`)).
		WithArgs("/dav/addressbooks/5/contact-1.vcf").
		WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectExec(regexp.QuoteMeta(`DELETE FROM acl_entries WHERE resource_path=$1`)).
		WithArgs("/dav/addressbooks/5/contact-1.vcf").
		WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectExec(regexp.QuoteMeta(`DELETE FROM dav_dead_properties WHERE resource_path=$1`)).
		WithArgs("/dav/addressbooks/5/contact-1.vcf").
		WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectCommit()

	if err := st.DeleteContactAndState(context.Background(), 5, DAVResourceState{Exists: true, UID: "contact-1", ResourceName: "contact-1", ETag: "etag", RawData: "raw"}, "/dav/addressbooks/5/contact-1", nil); err != nil {
		t.Fatalf("DeleteContactAndState() error = %v", err)
	}

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("sql expectations: %v", err)
	}
}

func TestStoreDeleteContactAndStateDeletesACLState(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New() error = %v", err)
	}
	defer db.Close()

	st := New(db)
	now := time.Now().UTC()
	contactColumns := []string{"id", "address_book_id", "uid", "resource_name", "raw_vcard", "etag", "display_name", "primary_email", "birthday", "last_modified"}

	mock.ExpectBegin()
	expectDAVObjectIdentityLocks(mock, "contact-object", 5, "contact-1", "contact-1")
	mock.ExpectQuery(`SELECT .* FROM contacts WHERE address_book_id=\$1 AND resource_name=\$2 FOR UPDATE`).
		WithArgs(int64(5), "contact-1").
		WillReturnRows(sqlmock.NewRows(contactColumns).AddRow(int64(10), int64(5), "contact-1", "contact-1", "raw", "etag", nil, nil, nil, now))
	mock.ExpectExec(regexp.QuoteMeta(`DELETE FROM contacts WHERE id=$1`)).
		WithArgs(int64(10)).WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectExec(regexp.QuoteMeta(`DELETE FROM locks WHERE resource_path=$1`)).
		WithArgs("/dav/addressbooks/5/contact-1").
		WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectExec(regexp.QuoteMeta(`DELETE FROM acl_entries WHERE resource_path=$1`)).
		WithArgs("/dav/addressbooks/5/contact-1").
		WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectExec(regexp.QuoteMeta(`DELETE FROM dav_dead_properties WHERE resource_path=$1`)).
		WithArgs("/dav/addressbooks/5/contact-1").
		WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectExec(regexp.QuoteMeta(`DELETE FROM locks WHERE resource_path=$1`)).
		WithArgs("/dav/addressbooks/5/contact-1.vcf").
		WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectExec(regexp.QuoteMeta(`DELETE FROM acl_entries WHERE resource_path=$1`)).
		WithArgs("/dav/addressbooks/5/contact-1.vcf").
		WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectExec(regexp.QuoteMeta(`DELETE FROM dav_dead_properties WHERE resource_path=$1`)).
		WithArgs("/dav/addressbooks/5/contact-1.vcf").
		WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectCommit()

	if err := st.DeleteContactAndState(context.Background(), 5, DAVResourceState{Exists: true, UID: "contact-1", ResourceName: "contact-1", ETag: "etag", RawData: "raw"}, "/dav/addressbooks/5/contact-1", nil); err != nil {
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
	for _, resourcePath := range sortedLockSerializationPaths("/dav/addressbooks/5") {
		mock.ExpectExec(regexp.QuoteMeta(`SELECT pg_advisory_xact_lock(hashtext($1))`)).
			WithArgs(resourcePath).
			WillReturnResult(sqlmock.NewResult(0, 1))
	}
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

func TestLockRepoCreateCanonicalizesPendingCalendarAfterSerialization(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New() error = %v", err)
	}
	defer db.Close()

	repo := &lockRepo{pool: db}
	expiresAt := time.Now().Add(time.Hour)
	pendingPath := "/dav/calendars/.pending/4/work"
	canonicalPath := "/dav/calendars/12"

	mock.ExpectBegin()
	for _, resourcePath := range sortedLockSerializationPaths(pendingPath) {
		mock.ExpectExec(regexp.QuoteMeta(`SELECT pg_advisory_xact_lock(hashtext($1))`)).
			WithArgs(resourcePath).
			WillReturnResult(sqlmock.NewResult(0, 1))
	}
	mock.ExpectQuery(regexp.QuoteMeta(`SELECT id FROM calendars WHERE user_id=$1 AND LOWER(slug)=LOWER($2)`)).
		WithArgs(int64(4), "work").
		WillReturnRows(sqlmock.NewRows([]string{"id"}).AddRow(int64(12)))
	mock.ExpectQuery(regexp.QuoteMeta(`SELECT lock_scope FROM locks WHERE resource_path = $1 AND expires_at > NOW()`)).
		WithArgs(canonicalPath).
		WillReturnRows(sqlmock.NewRows([]string{"lock_scope"}))
	mock.ExpectQuery(regexp.QuoteMeta(`SELECT lock_scope FROM locks WHERE resource_path = ANY($1) AND depth = 'infinity' AND expires_at > NOW()`)).
		WithArgs(pq.Array([]string{"/dav/calendars", "/dav"})).
		WillReturnRows(sqlmock.NewRows([]string{"lock_scope"}))
	mock.ExpectQuery(regexp.QuoteMeta(`
INSERT INTO locks (token, resource_path, user_id, lock_scope, lock_type, depth, owner_info, timeout_seconds, expires_at)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
RETURNING id, token, resource_path, user_id, lock_scope, lock_type, depth, owner_info, timeout_seconds, created_at, expires_at
`)).
		WithArgs("opaquelocktoken:late", canonicalPath, int64(4), "exclusive", "write", "0", "", 3600, expiresAt).
		WillReturnRows(sqlmock.NewRows([]string{"id", "token", "resource_path", "user_id", "lock_scope", "lock_type", "depth", "owner_info", "timeout_seconds", "created_at", "expires_at"}).
			AddRow(int64(9), "opaquelocktoken:late", canonicalPath, int64(4), "exclusive", "write", "0", "", 3600, time.Now(), expiresAt))
	mock.ExpectCommit()

	created, err := repo.Create(context.Background(), Lock{
		Token:          "opaquelocktoken:late",
		ResourcePath:   pendingPath,
		UserID:         4,
		LockScope:      "exclusive",
		LockType:       "write",
		Depth:          "0",
		TimeoutSeconds: 3600,
		ExpiresAt:      expiresAt,
	})
	if err != nil {
		t.Fatalf("Create() error = %v", err)
	}
	if created.ResourcePath != canonicalPath {
		t.Fatalf("Create() resource path = %q, want %q", created.ResourcePath, canonicalPath)
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

	mock.ExpectQuery(regexp.QuoteMeta(`SELECT id, address_book_id, uid, resource_name, raw_vcard, etag, display_name, primary_email, birthday, last_modified FROM contacts WHERE address_book_id=$1 AND id>$2 AND last_modified > $3 ORDER BY id ASC LIMIT $4`)).
		WithArgs(int64(5), int64(0), since, 256).
		WillReturnRows(sqlmock.NewRows([]string{"id", "address_book_id", "uid", "resource_name", "raw_vcard", "etag", "display_name", "primary_email", "birthday", "last_modified"}).
			AddRow(int64(4), int64(5), "uid-4", "uid-4", "BEGIN:VCARD", "etag-4", "Chris", "chris@example.com", nil, now))
	modified, err := repo.ListModifiedSincePageAfter(context.Background(), 5, 0, since, 256)
	if err != nil {
		t.Fatalf("ListModifiedSincePageAfter() error = %v", err)
	}
	if len(modified) != 1 || modified[0].PrimaryEmail == nil || *modified[0].PrimaryEmail != "chris@example.com" {
		t.Fatalf("ListModifiedSincePageAfter() = %#v", modified)
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

	mock.ExpectQuery(regexp.QuoteMeta(`
SELECT c.id, c.address_book_id, c.uid, c.resource_name, c.raw_vcard, c.etag, c.display_name, c.primary_email, c.birthday, c.last_modified
FROM contacts c
JOIN address_books ab ON ab.id = c.address_book_id
WHERE ab.user_id = $1 AND c.birthday IS NOT NULL
ORDER BY c.display_name
LIMIT $2
`)).
		WithArgs(int64(4), 257).
		WillReturnRows(sqlmock.NewRows([]string{"id", "address_book_id", "uid", "resource_name", "raw_vcard", "etag", "display_name", "primary_email", "birthday", "last_modified"}).
			AddRow(int64(7), int64(5), "uid-7", "uid-7", "BEGIN:VCARD", "etag-7", "Later Birthday", nil, birthday, now))
	cappedBirthdays, err := repo.ListWithBirthdaysByUserLimit(context.Background(), 4, 257)
	if err != nil {
		t.Fatalf("ListWithBirthdaysByUserLimit() error = %v", err)
	}
	if len(cappedBirthdays) != 1 || cappedBirthdays[0].UID != "uid-7" {
		t.Fatalf("ListWithBirthdaysByUserLimit() = %#v", cappedBirthdays)
	}
	emptyBirthdays, err := repo.ListWithBirthdaysByUserLimit(context.Background(), 4, 0)
	if err != nil {
		t.Fatalf("ListWithBirthdaysByUserLimit(limit 0) error = %v", err)
	}
	if len(emptyBirthdays) != 0 {
		t.Fatalf("ListWithBirthdaysByUserLimit(limit 0) = %#v", emptyBirthdays)
	}

	mock.ExpectBegin()
	mock.ExpectQuery(regexp.QuoteMeta(`SELECT resource_name FROM contacts WHERE address_book_id=$1 AND uid=$2`)).
		WithArgs(int64(5), "uid-rollback").
		WillReturnRows(sqlmock.NewRows([]string{"resource_name"}).AddRow("legacy-name"))
	mock.ExpectQuery(regexp.QuoteMeta(`SELECT resource_name FROM contacts WHERE address_book_id=$1 AND uid=$2`)).
		WithArgs(int64(9), "uid-rollback").
		WillReturnRows(sqlmock.NewRows([]string{"resource_name"}))
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

func TestACLRepoHasPrivilegeUsesFirstMatchingACE(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New() error = %v", err)
	}
	defer db.Close()

	repo := &aclRepo{pool: db}

	mock.ExpectQuery(regexp.QuoteMeta(`
SELECT COALESCE((
    SELECT is_grant
    FROM acl_entries
    WHERE resource_path=$1 AND principal_href=$2 AND privilege=$3
    ORDER BY ace_order, id
    LIMIT 1
), FALSE)
`)).
		WithArgs("/dav/addressbooks/5/alice.vcf", "/dav/principals/2/", "read").
		WillReturnRows(sqlmock.NewRows([]string{"has_privilege"}).AddRow(false))

	allowed, err := repo.HasPrivilege(context.Background(), "/dav/addressbooks/5/alice.vcf", "/dav/principals/2/", "read")
	if err != nil {
		t.Fatalf("HasPrivilege() error = %v", err)
	}
	if allowed {
		t.Fatal("HasPrivilege() = true, want the first matching ACE decision")
	}

	mock.ExpectQuery(regexp.QuoteMeta(`
SELECT COALESCE((
    SELECT is_grant
    FROM acl_entries
    WHERE resource_path=$1 AND principal_href=$2 AND privilege=$3
    ORDER BY ace_order, id
    LIMIT 1
), FALSE)
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
	statePaths := davStatePaths(resourcePath)

	mock.ExpectBegin()
	for _, lockPath := range sortedLockSerializationPaths(statePaths...) {
		mock.ExpectExec(regexp.QuoteMeta(`SELECT pg_advisory_xact_lock(hashtext($1))`)).
			WithArgs(lockPath).
			WillReturnResult(sqlmock.NewResult(0, 1))
	}
	mock.ExpectQuery(regexp.QuoteMeta(`SELECT id, resource_path, principal_href, is_grant, privilege, ace_order, created_at FROM acl_entries WHERE resource_path = ANY($1) ORDER BY ace_order, resource_path, id`)).
		WithArgs(pq.Array(statePaths)).
		WillReturnRows(sqlmock.NewRows([]string{"id", "resource_path", "principal_href", "is_grant", "privilege", "ace_order", "created_at"}).
			AddRow(int64(1), resourcePath, "/dav/principals/2/", true, "read", 0, createdAt))
	mock.ExpectExec(regexp.QuoteMeta(`DELETE FROM acl_entries WHERE resource_path = ANY($1)`)).
		WithArgs(pq.Array(statePaths)).
		WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectExec(regexp.QuoteMeta(`INSERT INTO acl_entries (resource_path, principal_href, is_grant, privilege, ace_order, created_at) VALUES ($1, $2, $3, $4, $5, $6)`)).
		WithArgs(resourcePath, "/dav/principals/2/", true, "read", 0, createdAt).
		WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectExec(regexp.QuoteMeta(`INSERT INTO acl_entries (resource_path, principal_href, is_grant, privilege, ace_order, created_at) VALUES ($1, $2, $3, $4, $5, $6)`)).
		WithArgs(resourcePath, "/dav/principals/2/", true, "write", 0, sqlmock.AnyArg()).
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
	statePaths := davStatePaths(resourcePath)

	mock.ExpectBegin()
	for _, lockPath := range sortedLockSerializationPaths(statePaths...) {
		mock.ExpectExec(regexp.QuoteMeta(`SELECT pg_advisory_xact_lock(hashtext($1))`)).
			WithArgs(lockPath).
			WillReturnResult(sqlmock.NewResult(0, 1))
	}
	mock.ExpectQuery(regexp.QuoteMeta(`SELECT id, resource_path, principal_href, is_grant, privilege, ace_order, created_at FROM acl_entries WHERE resource_path = ANY($1) ORDER BY ace_order, resource_path, id`)).
		WithArgs(pq.Array(statePaths)).
		WillReturnRows(sqlmock.NewRows([]string{"id", "resource_path", "principal_href", "is_grant", "privilege", "ace_order", "created_at"}))
	mock.ExpectExec(regexp.QuoteMeta(`DELETE FROM acl_entries WHERE resource_path = ANY($1)`)).
		WithArgs(pq.Array(statePaths)).
		WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectExec(regexp.QuoteMeta(`INSERT INTO acl_entries (resource_path, principal_href, is_grant, privilege, ace_order, created_at) VALUES ($1, $2, $3, $4, $5, $6)`)).
		WithArgs(resourcePath, "/dav/principals/2/", false, "read", 0, sqlmock.AnyArg()).
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
	for _, resourcePath := range sortedLockSerializationPaths("/dav/addressbooks/5") {
		mock.ExpectExec(regexp.QuoteMeta(`SELECT pg_advisory_xact_lock(hashtext($1))`)).
			WithArgs(resourcePath).
			WillReturnResult(sqlmock.NewResult(0, 1))
	}
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

func TestStoreMoveEventAndStateRunsInSingleTransaction(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New() error = %v", err)
	}
	defer db.Close()

	st := New(db)

	mock.ExpectBegin()
	mock.ExpectQuery(regexp.QuoteMeta(`SELECT resource_name FROM events WHERE calendar_id=$1 AND uid=$2`)).
		WithArgs(int64(2), "event").
		WillReturnRows(sqlmock.NewRows([]string{"resource_name"}).AddRow("event"))
	mock.ExpectQuery(regexp.QuoteMeta(`SELECT resource_name FROM events WHERE calendar_id=$1 AND uid=$2`)).
		WithArgs(int64(3), "event").
		WillReturnRows(sqlmock.NewRows([]string{"resource_name"}))
	mock.ExpectExec(regexp.QuoteMeta(`DELETE FROM events WHERE calendar_id=$1 AND resource_name=$2 AND uid<>$3`)).
		WithArgs(int64(3), "moved", "event").
		WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectExec(regexp.QuoteMeta(`DELETE FROM events WHERE calendar_id=$1 AND uid=$2`)).
		WithArgs(int64(3), "event").
		WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectExec(regexp.QuoteMeta(`UPDATE events SET calendar_id=$1, resource_name=$2, last_modified=NOW() WHERE calendar_id=$3 AND uid=$4`)).
		WithArgs(int64(3), "moved", int64(2), "event").
		WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectExec(regexp.QuoteMeta(`INSERT INTO deleted_resources (resource_type, collection_id, uid, resource_name) VALUES ('event', $1, $2, $3)`)).
		WithArgs(int64(2), "event", "event").
		WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectExec(regexp.QuoteMeta(`UPDATE calendars SET ctag = ctag + 1, updated_at = NOW() WHERE id = $1`)).
		WithArgs(int64(2)).
		WillReturnResult(sqlmock.NewResult(0, 1))
	for _, paths := range [][2]string{
		{"/dav/calendars/2/event", "/dav/calendars/3/moved"},
		{"/dav/calendars/2/event.ics", "/dav/calendars/3/moved.ics"},
	} {
		expectDAVStateMove(mock, paths[0], paths[1])
	}
	mock.ExpectExec(regexp.QuoteMeta(`DELETE FROM deleted_resources WHERE resource_type=$1 AND collection_id=$2 AND resource_name=$3`)).
		WithArgs("event", int64(3), "moved").
		WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectCommit()

	err = st.MoveEventAndState(context.Background(), 2, 3, "event", "moved", "/dav/calendars/2/event", "/dav/calendars/3/moved", "replaced-uid")
	if err != nil {
		t.Fatalf("MoveEventAndState() error = %v", err)
	}

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("sql expectations: %v", err)
	}
}

func TestStoreMoveEventAndStateRollsBackWhenStateRebindFails(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New() error = %v", err)
	}
	defer db.Close()

	st := New(db)

	mock.ExpectBegin()
	mock.ExpectQuery(regexp.QuoteMeta(`SELECT resource_name FROM events WHERE calendar_id=$1 AND uid=$2`)).
		WithArgs(int64(2), "event").
		WillReturnRows(sqlmock.NewRows([]string{"resource_name"}).AddRow("event"))
	mock.ExpectQuery(regexp.QuoteMeta(`SELECT resource_name FROM events WHERE calendar_id=$1 AND uid=$2`)).
		WithArgs(int64(3), "event").
		WillReturnRows(sqlmock.NewRows([]string{"resource_name"}))
	mock.ExpectExec(regexp.QuoteMeta(`DELETE FROM events WHERE calendar_id=$1 AND resource_name=$2 AND uid<>$3`)).
		WithArgs(int64(3), "moved", "event").
		WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectExec(regexp.QuoteMeta(`DELETE FROM events WHERE calendar_id=$1 AND uid=$2`)).
		WithArgs(int64(3), "event").
		WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectExec(regexp.QuoteMeta(`UPDATE events SET calendar_id=$1, resource_name=$2, last_modified=NOW() WHERE calendar_id=$3 AND uid=$4`)).
		WithArgs(int64(3), "moved", int64(2), "event").
		WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectExec(regexp.QuoteMeta(`INSERT INTO deleted_resources (resource_type, collection_id, uid, resource_name) VALUES ('event', $1, $2, $3)`)).
		WithArgs(int64(2), "event", "event").
		WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectExec(regexp.QuoteMeta(`UPDATE calendars SET ctag = ctag + 1, updated_at = NOW() WHERE id = $1`)).
		WithArgs(int64(2)).
		WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectExec(regexp.QuoteMeta(`DELETE FROM acl_entries WHERE resource_path=$1`)).
		WithArgs("/dav/calendars/3/moved").
		WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectExec(regexp.QuoteMeta(`UPDATE acl_entries SET resource_path=$1 WHERE resource_path=$2`)).
		WithArgs("/dav/calendars/3/moved", "/dav/calendars/2/event").
		WillReturnError(errors.New("acl move failed"))
	mock.ExpectRollback()

	err = st.MoveEventAndState(context.Background(), 2, 3, "event", "moved", "/dav/calendars/2/event", "/dav/calendars/3/moved", "")
	if err == nil {
		t.Fatal("MoveEventAndState() error = nil, want error")
	}

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("sql expectations: %v", err)
	}
}

func TestStoreUpdateAndMoveEventAndStateRunsInSingleTransaction(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New() error = %v", err)
	}
	defer db.Close()

	st := New(db)
	start := time.Date(2026, 7, 24, 10, 0, 0, 0, time.UTC)
	end := start.Add(30 * time.Minute)
	summary := "Updated"
	event := Event{
		CalendarID:   3,
		UID:          "event",
		ResourceName: "custom.ics",
		RawICAL:      "BEGIN:VCALENDAR\r\nBEGIN:VEVENT\r\nUID:event\r\nSUMMARY:Updated\r\nEND:VEVENT\r\nEND:VCALENDAR\r\n",
		ETag:         "new-etag",
		WriteMetadata: &EventWriteMetadata{
			Summary: &summary,
			DTStart: &start,
			DTEnd:   &end,
		},
	}

	mock.ExpectBegin()
	mock.ExpectExec(regexp.QuoteMeta(`UPDATE events SET
calendar_id=$1, resource_name=$2, raw_ical=$3, etag=$4,
summary=$5, description=$6, location=$7, dtstart=$8, dtend=$9,
all_day=$10, recurrence_start=$11, recurrence_until=$12, last_modified=NOW()
WHERE calendar_id=$13 AND uid=$14`)).
		WithArgs(int64(3), "custom.ics", event.RawICAL, "new-etag", &summary, nil, nil, &start, &end, false, nil, nil, int64(2), "event").
		WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectExec(regexp.QuoteMeta(`INSERT INTO deleted_resources (resource_type, collection_id, uid, resource_name) VALUES ('event', $1, $2, $3)`)).
		WithArgs(int64(2), "event", "custom.ics").
		WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectExec(regexp.QuoteMeta(`UPDATE calendars SET ctag = ctag + 1, updated_at = NOW() WHERE id = $1`)).
		WithArgs(int64(2)).
		WillReturnResult(sqlmock.NewResult(0, 1))
	for _, paths := range [][2]string{
		{"/dav/calendars/2/custom.ics", "/dav/calendars/3/custom.ics"},
		{"/dav/calendars/2/custom", "/dav/calendars/3/custom"},
	} {
		expectDAVStateMove(mock, paths[0], paths[1])
	}
	mock.ExpectExec(regexp.QuoteMeta(`DELETE FROM deleted_resources WHERE resource_type=$1 AND collection_id=$2 AND resource_name=$3`)).
		WithArgs("event", int64(3), "custom.ics").
		WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectCommit()

	err = st.UpdateAndMoveEventAndState(
		context.Background(),
		2,
		event,
		"/dav/calendars/2/custom.ics",
		"/dav/calendars/3/custom.ics",
	)
	if err != nil {
		t.Fatalf("UpdateAndMoveEventAndState() error = %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("sql expectations: %v", err)
	}
}

func TestStoreUpdateAndMoveEventAndStateRollsBackOnDAVStateFailure(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New() error = %v", err)
	}
	defer db.Close()

	st := New(db)
	event := Event{
		CalendarID:    3,
		UID:           "event",
		ResourceName:  "event",
		RawICAL:       "BEGIN:VCALENDAR\r\nBEGIN:VEVENT\r\nUID:event\r\nEND:VEVENT\r\nEND:VCALENDAR\r\n",
		ETag:          "new-etag",
		WriteMetadata: &EventWriteMetadata{},
	}

	mock.ExpectBegin()
	mock.ExpectExec(`UPDATE events SET`).
		WithArgs(int64(3), "event", event.RawICAL, "new-etag", nil, nil, nil, nil, nil, false, nil, nil, int64(2), "event").
		WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectExec(`INSERT INTO deleted_resources`).
		WithArgs(int64(2), "event", "event").
		WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectExec(`UPDATE calendars SET ctag`).
		WithArgs(int64(2)).
		WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectExec(`DELETE FROM acl_entries`).WithArgs("/dav/calendars/3/event").WillReturnError(errors.New("state failure"))
	mock.ExpectRollback()

	err = st.UpdateAndMoveEventAndState(
		context.Background(),
		2,
		event,
		"/dav/calendars/2/event",
		"/dav/calendars/3/event",
	)
	if err == nil {
		t.Fatal("UpdateAndMoveEventAndState() error = nil, want rollback error")
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("sql expectations: %v", err)
	}
}

func TestStoreMoveContactAndStateRunsInSingleTransaction(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New() error = %v", err)
	}
	defer db.Close()

	st := New(db)
	now := time.Now().UTC()
	contactColumns := []string{"id", "address_book_id", "uid", "resource_name", "raw_vcard", "etag", "display_name", "primary_email", "birthday", "last_modified"}

	mock.ExpectBegin()
	for _, key := range []string{"contact-object:5:name:alice", "contact-object:5:uid:alice", "contact-object:6:name:moved", "contact-object:6:uid:alice"} {
		mock.ExpectExec(regexp.QuoteMeta(`SELECT pg_advisory_xact_lock(hashtext($1))`)).WithArgs(key).WillReturnResult(sqlmock.NewResult(0, 1))
	}
	mock.ExpectQuery(`SELECT .* FROM contacts WHERE address_book_id=\$1 AND resource_name=\$2 FOR UPDATE`).
		WithArgs(int64(5), "alice").
		WillReturnRows(sqlmock.NewRows(contactColumns).AddRow(int64(10), int64(5), "alice", "alice", "", "", nil, nil, nil, now))
	mock.ExpectQuery(`SELECT .* FROM contacts WHERE address_book_id=\$1 AND resource_name=\$2 FOR UPDATE`).
		WithArgs(int64(6), "moved").WillReturnRows(sqlmock.NewRows(contactColumns))
	mock.ExpectQuery(`SELECT .* FROM contacts WHERE address_book_id=\$1 AND uid=\$2 FOR UPDATE`).
		WithArgs(int64(6), "alice").WillReturnRows(sqlmock.NewRows(contactColumns))
	mock.ExpectQuery(regexp.QuoteMeta(`UPDATE contacts SET address_book_id=$1, resource_name=$2, last_modified=NOW() WHERE id=$3 RETURNING `)+`.*`).
		WithArgs(int64(6), "moved", int64(10)).
		WillReturnRows(sqlmock.NewRows(contactColumns).AddRow(int64(10), int64(6), "alice", "moved", "", "", nil, nil, nil, now))
	mock.ExpectExec(regexp.QuoteMeta(`INSERT INTO deleted_resources (resource_type, collection_id, uid, resource_name) VALUES ('contact', $1, $2, $3)`)).
		WithArgs(int64(5), "alice", "alice").
		WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectExec(regexp.QuoteMeta(`UPDATE address_books SET ctag = ctag + 1, updated_at = NOW() WHERE id = $1`)).
		WithArgs(int64(5)).
		WillReturnResult(sqlmock.NewResult(0, 1))
	for _, paths := range [][2]string{
		{"/dav/addressbooks/5/alice", "/dav/addressbooks/6/moved"},
		{"/dav/addressbooks/5/alice.vcf", "/dav/addressbooks/6/moved.vcf"},
	} {
		expectDAVStateMove(mock, paths[0], paths[1])
	}
	mock.ExpectExec(regexp.QuoteMeta(`DELETE FROM deleted_resources WHERE resource_type=$1 AND collection_id=$2 AND resource_name=$3`)).
		WithArgs("contact", int64(6), "moved").
		WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectCommit()

	err = st.MoveContactAndState(context.Background(), 5, 6, "alice", "moved", "/dav/addressbooks/5/alice", "/dav/addressbooks/6/moved", "", ContactTransferExpectation{Source: DAVResourceState{Exists: true, UID: "alice", ResourceName: "alice"}, Overwrite: true}, nil)
	if err != nil {
		t.Fatalf("MoveContactAndState() error = %v", err)
	}

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("sql expectations: %v", err)
	}
}

func TestStoreMoveContactAndStateRejectsLateDestinationUIDConflict(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New() error = %v", err)
	}
	defer db.Close()

	st := New(db)
	now := time.Now().UTC()
	contactColumns := []string{"id", "address_book_id", "uid", "resource_name", "raw_vcard", "etag", "display_name", "primary_email", "birthday", "last_modified"}
	mock.ExpectBegin()
	for _, key := range []string{"contact-object:5:name:alice", "contact-object:5:uid:alice", "contact-object:6:name:moved", "contact-object:6:uid:alice"} {
		mock.ExpectExec(regexp.QuoteMeta(`SELECT pg_advisory_xact_lock(hashtext($1))`)).WithArgs(key).WillReturnResult(sqlmock.NewResult(0, 1))
	}
	mock.ExpectQuery(`SELECT .* FROM contacts WHERE address_book_id=\$1 AND resource_name=\$2 FOR UPDATE`).
		WithArgs(int64(5), "alice").
		WillReturnRows(sqlmock.NewRows(contactColumns).AddRow(int64(10), int64(5), "alice", "alice", "", "", nil, nil, nil, now))
	mock.ExpectQuery(`SELECT .* FROM contacts WHERE address_book_id=\$1 AND resource_name=\$2 FOR UPDATE`).
		WithArgs(int64(6), "moved").WillReturnRows(sqlmock.NewRows(contactColumns))
	mock.ExpectQuery(`SELECT .* FROM contacts WHERE address_book_id=\$1 AND uid=\$2 FOR UPDATE`).
		WithArgs(int64(6), "alice").
		WillReturnRows(sqlmock.NewRows(contactColumns).AddRow(int64(11), int64(6), "alice", "other-path", "", "", nil, nil, nil, now))
	mock.ExpectRollback()

	err = st.MoveContactAndState(context.Background(), 5, 6, "alice", "moved", "/dav/addressbooks/5/alice", "/dav/addressbooks/6/moved", "", ContactTransferExpectation{Source: DAVResourceState{Exists: true, UID: "alice", ResourceName: "alice"}, Overwrite: true}, nil)
	if err != ErrConflict {
		t.Fatalf("MoveContactAndState() error = %v, want ErrConflict", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("sql expectations: %v", err)
	}
}

func expectDAVStateMove(mock sqlmock.Sqlmock, fromPath, toPath string) {
	mock.ExpectExec(regexp.QuoteMeta(`DELETE FROM acl_entries WHERE resource_path=$1`)).
		WithArgs(toPath).
		WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectExec(regexp.QuoteMeta(`UPDATE acl_entries SET resource_path=$1 WHERE resource_path=$2`)).
		WithArgs(toPath, fromPath).
		WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectExec(regexp.QuoteMeta(`DELETE FROM locks WHERE resource_path=$1`)).
		WithArgs(fromPath).
		WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectExec(regexp.QuoteMeta(`DELETE FROM dav_dead_properties WHERE resource_path=$1`)).
		WithArgs(toPath).
		WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectExec(regexp.QuoteMeta(`UPDATE dav_dead_properties SET resource_path=$1, updated_at=NOW() WHERE resource_path=$2`)).
		WithArgs(toPath, fromPath).
		WillReturnResult(sqlmock.NewResult(0, 1))
}

func expectDAVObjectIdentityLocks(mock sqlmock.Sqlmock, kind string, collectionID int64, resourceName, uid string) {
	for _, key := range []string{
		fmt.Sprintf("%s:%d:name:%s", kind, collectionID, resourceName),
		fmt.Sprintf("%s:%d:uid:%s", kind, collectionID, uid),
	} {
		mock.ExpectExec(regexp.QuoteMeta(`SELECT pg_advisory_xact_lock(hashtext($1))`)).
			WithArgs(key).WillReturnResult(sqlmock.NewResult(0, 1))
	}
}

func TestStoreCopyEventAndStateCopiesDeadPropertiesAndClearsDestinationState(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New() error = %v", err)
	}
	defer db.Close()

	st := New(db)
	now := time.Now().UTC()
	raw := "BEGIN:VCALENDAR\r\nBEGIN:VEVENT\r\nUID:event\r\nEND:VEVENT\r\nEND:VCALENDAR\r\n"
	eventColumns := []string{"id", "calendar_id", "uid", "resource_name", "raw_ical", "etag", "summary", "description", "location", "dtstart", "dtend", "all_day", "last_modified"}

	mock.ExpectBegin()
	mock.ExpectQuery(regexp.QuoteMeta(`SELECT id, calendar_id, uid, resource_name, raw_ical, etag, summary, description, location, dtstart, dtend, all_day, last_modified FROM events WHERE calendar_id=$1 AND uid=$2`)).
		WithArgs(int64(2), "event").
		WillReturnRows(sqlmock.NewRows(eventColumns).AddRow(int64(10), int64(2), "event", "event", raw, "old-etag", nil, nil, nil, nil, nil, false, now))
	mock.ExpectQuery(regexp.QuoteMeta(`SELECT resource_name FROM events WHERE calendar_id=$1 AND uid=$2`)).
		WithArgs(int64(3), "event").
		WillReturnRows(sqlmock.NewRows([]string{"resource_name"}).AddRow("copied"))
	mock.ExpectExec(regexp.QuoteMeta(`DELETE FROM events WHERE calendar_id=$1 AND resource_name=$2 AND uid<>$3`)).
		WithArgs(int64(3), "copied", "event").
		WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectQuery(`INSERT INTO events`).
		WithArgs(int64(3), "event", "copied", raw, "new-etag", nil, nil, nil, nil, nil, false, nil, nil).
		WillReturnRows(sqlmock.NewRows(eventColumns).AddRow(int64(11), int64(3), "event", "copied", raw, "new-etag", nil, nil, nil, nil, nil, false, now))
	for _, statePath := range []string{"/dav/calendars/3/copied", "/dav/calendars/3/copied.ics"} {
		mock.ExpectExec(regexp.QuoteMeta(`DELETE FROM acl_entries WHERE resource_path=$1`)).WithArgs(statePath).WillReturnResult(sqlmock.NewResult(0, 1))
		mock.ExpectExec(regexp.QuoteMeta(`DELETE FROM dav_dead_properties WHERE resource_path=$1`)).WithArgs(statePath).WillReturnResult(sqlmock.NewResult(0, 1))
	}
	mock.ExpectExec(`INSERT INTO dav_dead_properties`).
		WithArgs("/dav/calendars/3/copied", pq.Array(davStatePaths("/dav/calendars/2/event")), "/dav/calendars/2/event").
		WillReturnResult(sqlmock.NewResult(0, 2))
	mock.ExpectExec(regexp.QuoteMeta(`DELETE FROM deleted_resources WHERE resource_type=$1 AND collection_id=$2 AND resource_name=$3`)).
		WithArgs("event", int64(3), "copied").
		WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectCommit()

	copied, err := st.CopyEventAndState(context.Background(), 2, 3, "event", "copied", "new-etag", "/dav/calendars/2/event", "/dav/calendars/3/copied", "")
	if err != nil {
		t.Fatalf("CopyEventAndState() error = %v", err)
	}
	if copied == nil || copied.CalendarID != 3 || copied.ResourceName != "copied" {
		t.Fatalf("CopyEventAndState() = %#v", copied)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("sql expectations: %v", err)
	}
}

func TestStoreCopyEventAndStateRollsBackWhenDestinationStateClearFails(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New() error = %v", err)
	}
	defer db.Close()

	st := New(db)
	now := time.Now().UTC()
	raw := "BEGIN:VCALENDAR\r\nBEGIN:VEVENT\r\nUID:event\r\nEND:VEVENT\r\nEND:VCALENDAR\r\n"
	eventColumns := []string{"id", "calendar_id", "uid", "resource_name", "raw_ical", "etag", "summary", "description", "location", "dtstart", "dtend", "all_day", "last_modified"}

	mock.ExpectBegin()
	mock.ExpectQuery(regexp.QuoteMeta(`SELECT id, calendar_id, uid, resource_name, raw_ical, etag, summary, description, location, dtstart, dtend, all_day, last_modified FROM events WHERE calendar_id=$1 AND uid=$2`)).
		WithArgs(int64(2), "event").
		WillReturnRows(sqlmock.NewRows(eventColumns).AddRow(int64(10), int64(2), "event", "event", raw, "old-etag", nil, nil, nil, nil, nil, false, now))
	mock.ExpectQuery(regexp.QuoteMeta(`SELECT resource_name FROM events WHERE calendar_id=$1 AND uid=$2`)).
		WithArgs(int64(3), "event").
		WillReturnRows(sqlmock.NewRows([]string{"resource_name"}).AddRow("copied"))
	mock.ExpectExec(regexp.QuoteMeta(`DELETE FROM events WHERE calendar_id=$1 AND resource_name=$2 AND uid<>$3`)).
		WithArgs(int64(3), "copied", "event").
		WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectQuery(`INSERT INTO events`).
		WithArgs(int64(3), "event", "copied", raw, "new-etag", nil, nil, nil, nil, nil, false, nil, nil).
		WillReturnRows(sqlmock.NewRows(eventColumns).AddRow(int64(11), int64(3), "event", "copied", raw, "new-etag", nil, nil, nil, nil, nil, false, now))
	mock.ExpectExec(regexp.QuoteMeta(`DELETE FROM acl_entries WHERE resource_path=$1`)).
		WithArgs("/dav/calendars/3/copied").
		WillReturnError(errors.New("ACL delete failed"))
	mock.ExpectRollback()

	_, err = st.CopyEventAndState(context.Background(), 2, 3, "event", "copied", "new-etag", "/dav/calendars/2/event", "/dav/calendars/3/copied", "event")
	if err == nil {
		t.Fatal("CopyEventAndState() error = nil, want rollback error")
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("sql expectations: %v", err)
	}
}

// baselineSchemaVersionRow is the version db.sql seeds, which every migration
// test below asserts against: a fresh install and one upgraded through the
// newest migration have to report the same schema version.
const baselineSchemaVersionRow = "VALUES ('version', 'v1.2.0-rc7')"

// TestCalendarPropertyColumnsMigration pins that the migration and the flattened
// baseline schema both add the columns the calendar live properties are read
// from, so a deployment upgraded by migration and one created from db.sql agree.
func TestCalendarPropertyColumnsMigration(t *testing.T) {
	sources := map[string][]string{
		"../../migrations/v1.1.10.sql": {
			"ALTER TABLE calendars ADD COLUMN IF NOT EXISTS description_lang TEXT",
			"ALTER TABLE calendars ADD COLUMN IF NOT EXISTS supported_components TEXT[]",
			"UPDATE application SET value = 'v1.1.10'",
		},
		"../../db.sql": {
			"ALTER TABLE calendars ADD COLUMN IF NOT EXISTS description_lang TEXT",
			"ALTER TABLE calendars ADD COLUMN IF NOT EXISTS supported_components TEXT[]",
			baselineSchemaVersionRow,
		},
	}
	for path, expected := range sources {
		contents, err := os.ReadFile(path)
		if err != nil {
			t.Fatalf("ReadFile(%s) error = %v", path, err)
		}
		for _, want := range expected {
			if !strings.Contains(string(contents), want) {
				t.Errorf("%s is missing %q", path, want)
			}
		}
	}
}

func TestACLOrderAndDigestCredentialMigrationMatchesBaselineSchema(t *testing.T) {
	sources := map[string][]string{
		"../../migrations/v1.1.11.sql": {
			"ALTER TABLE acl_entries ADD COLUMN IF NOT EXISTS ace_order INTEGER NOT NULL DEFAULT 0",
			"ROW_NUMBER() OVER (PARTITION BY resource_path ORDER BY created_at, id)",
			"DROP INDEX IF EXISTS idx_acl_unique",
			"CREATE INDEX IF NOT EXISTS idx_acl_resource_order ON acl_entries(resource_path, ace_order, id)",
			"ALTER TABLE app_passwords ADD COLUMN IF NOT EXISTS digest_md5_ha1 TEXT",
			"ALTER TABLE app_passwords ADD COLUMN IF NOT EXISTS digest_sha256_ha1 TEXT",
			"UPDATE application SET value = 'v1.1.11'",
		},
		"../../db.sql": {
			"ace_order INTEGER NOT NULL DEFAULT 0",
			"DROP INDEX IF EXISTS idx_acl_unique",
			"CREATE INDEX IF NOT EXISTS idx_acl_resource_order ON acl_entries(resource_path, ace_order, id)",
			"digest_md5_ha1 TEXT NULL",
			"digest_sha256_ha1 TEXT NULL",
			baselineSchemaVersionRow,
		},
	}
	for path, expected := range sources {
		contents, err := os.ReadFile(path)
		if err != nil {
			t.Fatalf("ReadFile(%s) error = %v", path, err)
		}
		for _, want := range expected {
			if !strings.Contains(string(contents), want) {
				t.Errorf("%s is missing %q", path, want)
			}
		}
	}
}

// A PostgreSQL expression index only serves a predicate that spells the
// expression verbatim, so the two COALESCE tails have to read the same in the
// migration, the baseline schema, and ListForCalendarFiltered. Drift costs the
// index silently: the range falls out of the index scan and back into a filter.
func TestTimeRangeIndexMigrationMatchesBaselineSchema(t *testing.T) {
	startIndex := "CREATE INDEX IF NOT EXISTS idx_events_recurrence_start\n    ON events (calendar_id, COALESCE(recurrence_start, dtstart, '-infinity'::timestamptz))"
	untilIndex := "CREATE INDEX IF NOT EXISTS idx_events_recurrence_until\n    ON events (calendar_id, COALESCE(recurrence_until, dtend, 'infinity'::timestamptz))"

	sources := map[string][]string{
		"../../migrations/v1.1.12.sql": {
			"DROP INDEX IF EXISTS idx_events_recurrence_start",
			"DROP INDEX IF EXISTS idx_events_recurrence_until",
			startIndex,
			untilIndex,
			"UPDATE application SET value = 'v1.1.12'",
		},
		"../../db.sql": {
			startIndex,
			untilIndex,
			baselineSchemaVersionRow,
		},
		"postgres.go": {
			"COALESCE(recurrence_start, dtstart, '-infinity'::timestamptz) <= ",
			"COALESCE(recurrence_until, dtend, 'infinity'::timestamptz) >= ",
		},
	}
	for path, expected := range sources {
		contents, err := os.ReadFile(path)
		if err != nil {
			t.Fatalf("ReadFile(%s) error = %v", path, err)
		}
		for _, want := range expected {
			if !strings.Contains(string(contents), want) {
				t.Errorf("%s is missing %q", path, want)
			}
		}
	}
}

// Each keyset index exists for the statements that page a collection by id, so
// the indexes and every one of those statements are pinned together: a leading
// column or an ORDER BY that stopped matching would silently drop the read back
// onto the primary key, which stays correct while costing the whole table.
func TestKeysetIndexMigrationMatchesBaselineSchema(t *testing.T) {
	eventsIndex := "CREATE INDEX IF NOT EXISTS idx_events_calendar_keyset\n    ON events (calendar_id, id)"
	contactsIndex := "CREATE INDEX IF NOT EXISTS idx_contacts_book_keyset\n    ON contacts (address_book_id, id)"
	deletedIndex := "CREATE INDEX IF NOT EXISTS idx_deleted_resources_keyset\n    ON deleted_resources (resource_type, collection_id, id)"

	sources := map[string][]string{
		"../../migrations/v1.2.0-rc7.sql": {
			eventsIndex,
			contactsIndex,
			deletedIndex,
			"DROP INDEX IF EXISTS idx_events_calendar_id",
			"DROP INDEX IF EXISTS idx_contacts_address_book_id",
			"UPDATE application SET value = 'v1.2.0-rc7'",
		},
		"../../db.sql": {
			"CREATE INDEX idx_events_calendar_keyset ON events(calendar_id, id);",
			"CREATE INDEX idx_contacts_book_keyset ON contacts(address_book_id, id);",
			"CREATE INDEX idx_deleted_resources_keyset ON deleted_resources(resource_type, collection_id, id);",
			baselineSchemaVersionRow,
		},
		"postgres.go": {
			"FROM events WHERE calendar_id=$1 AND id>$2 AND last_modified > $3 ORDER BY id ASC LIMIT $4",
			"FROM contacts WHERE address_book_id=$1 AND id>$2 AND last_modified > $3 ORDER BY id ASC LIMIT $4",
			"FROM contacts WHERE address_book_id=$1 AND id>$2 ORDER BY id ASC LIMIT $3",
			"FROM deleted_resources WHERE resource_type=$1 AND collection_id=$2 AND id>$3 AND deleted_at > $4 ORDER BY id ASC LIMIT $5",
		},
	}
	for path, expected := range sources {
		contents, err := os.ReadFile(path)
		if err != nil {
			t.Fatalf("ReadFile(%s) error = %v", path, err)
		}
		for _, want := range expected {
			if !strings.Contains(string(contents), want) {
				t.Errorf("%s is missing %q", path, want)
			}
		}
	}
}

// db.sql must not still create an index the newest migration drops, or a fresh
// install carries one an upgraded deployment does not.
func TestBaselineSchemaDropsSupersededIndexes(t *testing.T) {
	contents, err := os.ReadFile("../../db.sql")
	if err != nil {
		t.Fatalf("ReadFile(db.sql) error = %v", err)
	}
	for _, superseded := range []string{
		"CREATE INDEX idx_events_calendar_id ON events(calendar_id);",
		"CREATE INDEX idx_contacts_address_book_id ON contacts(address_book_id);",
	} {
		if strings.Contains(string(contents), superseded) {
			t.Errorf("db.sql still creates %q, which the keyset migration drops", superseded)
		}
	}
}
