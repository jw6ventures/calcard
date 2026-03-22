package store

import (
	"context"
	"database/sql"
	"errors"
	"regexp"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
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

	mock.ExpectQuery(regexp.QuoteMeta(`
SELECT c.id, c.user_id, c.name, c.slug, c.description, c.timezone, c.color, c.ctag, c.created_at, c.updated_at,
       u.primary_email as owner_email,
       CASE WHEN c.user_id = $2 THEN FALSE ELSE TRUE END as shared,
       COALESCE(cs.editor, TRUE) as editor
FROM calendars c
JOIN users u ON u.id = c.user_id
LEFT JOIN calendar_shares cs ON cs.calendar_id = c.id AND cs.user_id = $2
WHERE c.id = $1 AND (c.user_id = $2 OR cs.user_id = $2)
`)).
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

func TestCalendarAccessibleAndShareRepos(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New() error = %v", err)
	}
	defer db.Close()

	calendarRepo := &calendarRepo{pool: db}
	shareRepo := &calendarShareRepo{pool: db}
	now := time.Now().UTC()

	mock.ExpectQuery(regexp.QuoteMeta(`
SELECT c.id, c.user_id, c.name, c.slug, c.description, c.timezone, c.color, c.ctag, c.created_at, c.updated_at,
       u.primary_email as owner_email, FALSE as shared, TRUE as editor
FROM calendars c
JOIN users u ON u.id = c.user_id
WHERE c.user_id = $1
UNION ALL
SELECT c.id, c.user_id, c.name, c.slug, c.description, c.timezone, c.color, c.ctag, c.created_at, c.updated_at,
       u.primary_email as owner_email, TRUE as shared, cs.editor
FROM calendars c
JOIN calendar_shares cs ON cs.calendar_id = c.id AND cs.user_id = $1
JOIN users u ON u.id = c.user_id
ORDER BY shared, name
`)).
		WithArgs(int64(4)).
		WillReturnRows(sqlmock.NewRows([]string{"id", "user_id", "name", "slug", "description", "timezone", "color", "ctag", "created_at", "updated_at", "owner_email", "shared", "editor"}).
			AddRow(int64(1), int64(4), "Owned", nil, nil, nil, nil, int64(1), now, now, "owner@example.com", false, true).
			AddRow(int64(2), int64(9), "Shared", "shared", "Desc", "UTC", "#123456", int64(3), now, now, "other@example.com", true, false))

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

	mock.ExpectQuery(regexp.QuoteMeta(`SELECT calendar_id, user_id, granted_by, editor, created_at FROM calendar_shares WHERE calendar_id=$1 ORDER BY created_at`)).
		WithArgs(int64(2)).
		WillReturnRows(sqlmock.NewRows([]string{"calendar_id", "user_id", "granted_by", "editor", "created_at"}).
			AddRow(int64(2), int64(4), int64(9), false, now))
	shares, err := shareRepo.ListByCalendar(context.Background(), 2)
	if err != nil {
		t.Fatalf("ListByCalendar() error = %v", err)
	}
	if len(shares) != 1 || shares[0].GrantedBy != 9 {
		t.Fatalf("ListByCalendar() = %#v", shares)
	}

	mock.ExpectExec(regexp.QuoteMeta(`
INSERT INTO calendar_shares (calendar_id, user_id, granted_by, editor)
VALUES ($1, $2, $3, $4)
ON CONFLICT (calendar_id, user_id) DO NOTHING
`)).
		WithArgs(int64(2), int64(4), int64(9), false).
		WillReturnResult(sqlmock.NewResult(0, 1))
	if err := shareRepo.Create(context.Background(), CalendarShare{CalendarID: 2, UserID: 4, GrantedBy: 9, Editor: false}); err != nil {
		t.Fatalf("Create() error = %v", err)
	}

	mock.ExpectExec(regexp.QuoteMeta(`DELETE FROM calendar_shares WHERE calendar_id=$1 AND user_id=$2`)).
		WithArgs(int64(2), int64(4)).
		WillReturnResult(sqlmock.NewResult(0, 1))
	if err := shareRepo.Delete(context.Background(), 2, 4); err != nil {
		t.Fatalf("Delete() error = %v", err)
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

	mock.ExpectQuery(regexp.QuoteMeta(`
SELECT e.id, e.calendar_id, e.uid, e.resource_name, e.raw_ical, e.etag, e.summary, e.dtstart, e.dtend, e.all_day, e.last_modified
FROM events e
JOIN calendars c ON c.id = e.calendar_id
LEFT JOIN calendar_shares cs ON cs.calendar_id = c.id AND cs.user_id = $1
WHERE c.user_id = $1 OR cs.user_id = $1
ORDER BY e.last_modified DESC
LIMIT $2
`)).
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
INSERT INTO contacts (address_book_id, uid, raw_vcard, etag, display_name, primary_email, birthday, last_modified)
VALUES ($1, $2, $3, $4, $5, $6, $7, NOW())
ON CONFLICT (address_book_id, uid) DO UPDATE SET
        raw_vcard = EXCLUDED.raw_vcard,
        etag = EXCLUDED.etag,
        display_name = EXCLUDED.display_name,
        primary_email = EXCLUDED.primary_email,
        birthday = EXCLUDED.birthday,
        last_modified = NOW()
RETURNING id, address_book_id, uid, raw_vcard, etag, display_name, primary_email, birthday, last_modified
`)).
		WithArgs(int64(5), "contact-1", rawVCard, "etag-1", "Jane Doe", "jane@example.com", birthday).
		WillReturnRows(sqlmock.NewRows([]string{"id", "address_book_id", "uid", "raw_vcard", "etag", "display_name", "primary_email", "birthday", "last_modified"}).
			AddRow(int64(1), int64(5), "contact-1", rawVCard, "etag-1", "Jane Doe", "jane@example.com", birthday, now))

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
	mock.ExpectExec(regexp.QuoteMeta(`UPDATE contacts SET address_book_id=$1, last_modified=NOW() WHERE address_book_id=$2 AND uid=$3`)).
		WithArgs(int64(9), int64(5), "contact-1").
		WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectExec(regexp.QuoteMeta(`INSERT INTO deleted_resources (resource_type, collection_id, uid, resource_name) VALUES ('contact', $1, $2, $2)`)).
		WithArgs(int64(5), "contact-1").
		WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectExec(regexp.QuoteMeta(`UPDATE address_books SET ctag = ctag + 1, updated_at = NOW() WHERE id = $1`)).
		WithArgs(int64(5)).
		WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectCommit()

	if err := repo.MoveToAddressBook(context.Background(), 5, 9, "contact-1"); err != nil {
		t.Fatalf("MoveToAddressBook() error = %v", err)
	}

	mock.ExpectBegin()
	mock.ExpectExec(regexp.QuoteMeta(`UPDATE contacts SET address_book_id=$1, last_modified=NOW() WHERE address_book_id=$2 AND uid=$3`)).
		WithArgs(int64(9), int64(5), "missing").
		WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectRollback()
	if err := repo.MoveToAddressBook(context.Background(), 5, 9, "missing"); err != ErrNotFound {
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

	mock.ExpectQuery(regexp.QuoteMeta(`SELECT id, address_book_id, uid, raw_vcard, etag, display_name, primary_email, birthday, last_modified FROM contacts WHERE address_book_id=$1 AND uid = ANY($2)`)).
		WithArgs(int64(5), sqlmock.AnyArg()).
		WillReturnRows(sqlmock.NewRows([]string{"id", "address_book_id", "uid", "raw_vcard", "etag", "display_name", "primary_email", "birthday", "last_modified"}).
			AddRow(int64(1), int64(5), "uid-1", "BEGIN:VCARD", "etag-1", "Jane Doe", "jane@example.com", birthday, now))
	contacts, err := repo.ListByUIDs(context.Background(), 5, []string{"uid-1"})
	if err != nil {
		t.Fatalf("ListByUIDs() error = %v", err)
	}
	if len(contacts) != 1 || contacts[0].Birthday == nil || !contacts[0].Birthday.Equal(birthday) {
		t.Fatalf("ListByUIDs() = %#v", contacts)
	}

	mock.ExpectQuery(regexp.QuoteMeta(`SELECT id, address_book_id, uid, raw_vcard, etag, display_name, primary_email, birthday, last_modified FROM contacts WHERE address_book_id=$1 ORDER BY last_modified DESC`)).
		WithArgs(int64(5)).
		WillReturnRows(sqlmock.NewRows([]string{"id", "address_book_id", "uid", "raw_vcard", "etag", "display_name", "primary_email", "birthday", "last_modified"}).
			AddRow(int64(2), int64(5), "uid-2", "BEGIN:VCARD", "etag-2", nil, nil, nil, now))
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
	mock.ExpectQuery(regexp.QuoteMeta(`SELECT id, address_book_id, uid, raw_vcard, etag, display_name, primary_email, birthday, last_modified FROM contacts WHERE address_book_id=$1 ORDER BY LOWER(COALESCE(display_name, '')) ASC, id ASC LIMIT $2 OFFSET $3`)).
		WithArgs(int64(5), 10, 0).
		WillReturnRows(sqlmock.NewRows([]string{"id", "address_book_id", "uid", "raw_vcard", "etag", "display_name", "primary_email", "birthday", "last_modified"}).
			AddRow(int64(3), int64(5), "uid-3", "BEGIN:VCARD", "etag-3", "Alex", nil, nil, now))
	page, err := repo.ListForBookPaginated(context.Background(), 5, 10, 0)
	if err != nil {
		t.Fatalf("ListForBookPaginated() error = %v", err)
	}
	if page.TotalCount != 1 || len(page.Items) != 1 || page.Items[0].DisplayName == nil || *page.Items[0].DisplayName != "Alex" {
		t.Fatalf("ListForBookPaginated() = %#v", page)
	}

	mock.ExpectQuery(regexp.QuoteMeta(`SELECT id, address_book_id, uid, raw_vcard, etag, display_name, primary_email, birthday, last_modified FROM contacts WHERE address_book_id=$1 AND last_modified > $2 ORDER BY last_modified DESC`)).
		WithArgs(int64(5), since).
		WillReturnRows(sqlmock.NewRows([]string{"id", "address_book_id", "uid", "raw_vcard", "etag", "display_name", "primary_email", "birthday", "last_modified"}).
			AddRow(int64(4), int64(5), "uid-4", "BEGIN:VCARD", "etag-4", "Chris", "chris@example.com", nil, now))
	modified, err := repo.ListModifiedSince(context.Background(), 5, since)
	if err != nil {
		t.Fatalf("ListModifiedSince() error = %v", err)
	}
	if len(modified) != 1 || modified[0].PrimaryEmail == nil || *modified[0].PrimaryEmail != "chris@example.com" {
		t.Fatalf("ListModifiedSince() = %#v", modified)
	}

	mock.ExpectQuery(regexp.QuoteMeta(`
SELECT c.id, c.address_book_id, c.uid, c.raw_vcard, c.etag, c.display_name, c.primary_email, c.birthday, c.last_modified
FROM contacts c
JOIN address_books ab ON ab.id = c.address_book_id
WHERE ab.user_id = $1
ORDER BY c.last_modified DESC
LIMIT $2
`)).
		WithArgs(int64(4), 5).
		WillReturnRows(sqlmock.NewRows([]string{"id", "address_book_id", "uid", "raw_vcard", "etag", "display_name", "primary_email", "birthday", "last_modified"}).
			AddRow(int64(5), int64(5), "uid-5", "BEGIN:VCARD", "etag-5", "Recent Contact", nil, nil, now))
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
SELECT c.id, c.address_book_id, c.uid, c.raw_vcard, c.etag, c.display_name, c.primary_email, c.birthday, c.last_modified
FROM contacts c
JOIN address_books ab ON ab.id = c.address_book_id
WHERE ab.user_id = $1 AND c.birthday IS NOT NULL
ORDER BY c.display_name
`)).
		WithArgs(int64(4)).
		WillReturnRows(sqlmock.NewRows([]string{"id", "address_book_id", "uid", "raw_vcard", "etag", "display_name", "primary_email", "birthday", "last_modified"}).
			AddRow(int64(6), int64(5), "uid-6", "BEGIN:VCARD", "etag-6", "Birthday Person", nil, birthday, now))
	withBirthdays, err := repo.ListWithBirthdaysByUser(context.Background(), 4)
	if err != nil {
		t.Fatalf("ListWithBirthdaysByUser() error = %v", err)
	}
	if len(withBirthdays) != 1 || withBirthdays[0].Birthday == nil {
		t.Fatalf("ListWithBirthdaysByUser() = %#v", withBirthdays)
	}

	mock.ExpectBegin()
	mock.ExpectExec(regexp.QuoteMeta(`UPDATE contacts SET address_book_id=$1, last_modified=NOW() WHERE address_book_id=$2 AND uid=$3`)).
		WithArgs(int64(9), int64(5), "uid-rollback").
		WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectExec(regexp.QuoteMeta(`INSERT INTO deleted_resources (resource_type, collection_id, uid, resource_name) VALUES ('contact', $1, $2, $2)`)).
		WithArgs(int64(5), "uid-rollback").
		WillReturnError(errors.New("tombstone failed"))
	mock.ExpectRollback()
	if err := repo.MoveToAddressBook(context.Background(), 5, 9, "uid-rollback"); err == nil || err.Error() != "tombstone failed" {
		t.Fatalf("MoveToAddressBook() error = %v", err)
	}

	mock.ExpectQuery(regexp.QuoteMeta(`SELECT id, address_book_id, uid, raw_vcard, etag, display_name, primary_email, birthday, last_modified FROM contacts WHERE address_book_id=$1 AND uid=$2`)).
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
