package store

import (
	"context"
	"errors"
	"os"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/lib/pq"
)

func TestDeadPropertyRepositoryListsResourcesInOneQuery(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New() error = %v", err)
	}
	defer db.Close()

	now := time.Now().UTC()
	paths := []string{"/dav/calendars/1", "/dav/calendars/1/event"}
	mock.ExpectQuery(`FROM dav_dead_properties`).
		WithArgs(pq.Array(paths)).
		WillReturnRows(sqlmock.NewRows([]string{"resource_path", "namespace_uri", "local_name", "inner_xml", "created_at", "updated_at"}).
			AddRow(paths[0], "urn:test", "collection", "value", now, now).
			AddRow(paths[1], "urn:test", "object", "<x>value</x>", now, now))

	properties, err := (&deadPropertyRepo{pool: db}).ListByResources(context.Background(), paths)
	if err != nil {
		t.Fatalf("ListByResources() error = %v", err)
	}
	if len(properties) != 2 || properties[1].InnerXML != "<x>value</x>" {
		t.Fatalf("ListByResources() = %#v", properties)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("sql expectations: %v", err)
	}
}

func TestDeadPropertyRepositoryAppliesMutationsInOrder(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New() error = %v", err)
	}
	defer db.Close()

	resourcePath := "/dav/addressbooks/5/alice"
	mock.ExpectBegin()
	mock.ExpectExec(`INSERT INTO dav_dead_properties`).
		WithArgs(resourcePath, "urn:test", "note", "first").
		WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectExec(regexp.QuoteMeta(`DELETE FROM dav_dead_properties WHERE resource_path=$1 AND namespace_uri=$2 AND local_name=$3`)).
		WithArgs(resourcePath, "urn:test", "note").
		WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectExec(`INSERT INTO dav_dead_properties`).
		WithArgs(resourcePath, "urn:test", "note", "last").
		WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectCommit()

	err = (&deadPropertyRepo{pool: db}).Apply(context.Background(), resourcePath, []DeadPropertyMutation{
		{NamespaceURI: "urn:test", LocalName: "note", InnerXML: "first"},
		{NamespaceURI: "urn:test", LocalName: "note", Remove: true},
		{NamespaceURI: "urn:test", LocalName: "note", InnerXML: "last"},
	})
	if err != nil {
		t.Fatalf("Apply() error = %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("sql expectations: %v", err)
	}
}

func TestPatchCalendarPropertiesRollsBackLiveChangeWhenDeadPropertyFails(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New() error = %v", err)
	}
	defer db.Close()

	st := New(db)
	mock.ExpectBegin()
	mock.ExpectExec(regexp.QuoteMeta(`UPDATE calendars SET name=$1, description=$2, timezone=$3, color=$4, updated_at=NOW() WHERE id=$5`)).
		WithArgs("Renamed", nil, nil, nil, int64(5)).
		WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectExec(`INSERT INTO dav_dead_properties`).
		WithArgs("/dav/calendars/5", "urn:test", "note", "value").
		WillReturnError(errors.New("dead property write failed"))
	mock.ExpectRollback()

	err = st.PatchCalendarProperties(context.Background(), 5, "Renamed", nil, nil, nil, "/dav/calendars/5", []DeadPropertyMutation{{NamespaceURI: "urn:test", LocalName: "note", InnerXML: "value"}})
	if err == nil {
		t.Fatal("PatchCalendarProperties() error = nil, want rollback error")
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("sql expectations: %v", err)
	}
}

func TestMigrationV118ContainsDeadPropertyAndScopedACLIndexes(t *testing.T) {
	contents, err := os.ReadFile("../../migrations/v1.1.8.sql")
	if err != nil {
		t.Fatalf("ReadFile() error = %v", err)
	}
	sql := string(contents)
	for _, required := range []string{
		"CREATE TABLE IF NOT EXISTS dav_dead_properties",
		"PRIMARY KEY (resource_path, namespace_uri, local_name)",
		"ON acl_entries (resource_path, principal_href)",
		"ADD COLUMN IF NOT EXISTS object_acl_path",
		"UPDATE application SET value = 'v1.1.8'",
	} {
		if !strings.Contains(sql, required) {
			t.Fatalf("migration missing %q", required)
		}
	}
}

func TestEventRepositoryUsesPrecomputedWriteMetadata(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New() error = %v", err)
	}
	defer db.Close()

	now := time.Now().UTC()
	dtstart := time.Date(2026, 7, 20, 15, 0, 0, 0, time.UTC)
	dtend := dtstart.Add(time.Hour)
	recurrenceUntil := dtend.Add(48 * time.Hour)
	summary := "precomputed summary"
	raw := "not reparsed because DAV already validated it"
	mock.ExpectQuery(`INSERT INTO events`).
		WithArgs(int64(7), "event", "event", raw, "etag", summary, nil, nil, dtstart, dtend, false, dtstart, recurrenceUntil).
		WillReturnRows(sqlmock.NewRows([]string{"id", "calendar_id", "uid", "resource_name", "raw_ical", "etag", "summary", "description", "location", "dtstart", "dtend", "all_day", "last_modified"}).
			AddRow(int64(1), int64(7), "event", "event", raw, "etag", summary, nil, nil, dtstart, dtend, false, now))

	created, err := (&eventRepo{pool: db}).Upsert(context.Background(), Event{
		CalendarID:    7,
		UID:           "event",
		RawICAL:       raw,
		ETag:          "etag",
		WriteMetadata: &EventWriteMetadata{Summary: &summary, DTStart: &dtstart, DTEnd: &dtend, RecurrenceStart: &dtstart, RecurrenceUntil: &recurrenceUntil},
	})
	if err != nil {
		t.Fatalf("Upsert() error = %v", err)
	}
	if created.Summary == nil || *created.Summary != summary {
		t.Fatalf("Upsert() = %#v", created)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("sql expectations: %v", err)
	}
}

func TestACLRepositoryListsOnlyScopedResourcesAndPrincipals(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New() error = %v", err)
	}
	defer db.Close()

	paths := []string{"/dav/calendars/1", "/dav/calendars/1/event"}
	principals := []string{"DAV:all", "/dav/principals/2/"}
	now := time.Now().UTC()
	mock.ExpectQuery(`resource_path = ANY\(\$1\) AND principal_href = ANY\(\$2\)`).
		WithArgs(pq.Array(paths), pq.Array(principals)).
		WillReturnRows(sqlmock.NewRows([]string{"id", "resource_path", "principal_href", "is_grant", "privilege", "created_at"}).
			AddRow(int64(1), paths[1], principals[1], true, "read", now))

	entries, err := (&aclRepo{pool: db}).ListByResourcesAndPrincipals(context.Background(), paths, principals)
	if err != nil {
		t.Fatalf("ListByResourcesAndPrincipals() error = %v", err)
	}
	if len(entries) != 1 || entries[0].ResourcePath != paths[1] {
		t.Fatalf("ListByResourcesAndPrincipals() = %#v", entries)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("sql expectations: %v", err)
	}
}

func TestContactRepositoryListsKeysetPage(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New() error = %v", err)
	}
	defer db.Close()

	now := time.Now().UTC()
	mock.ExpectQuery(`FROM contacts WHERE address_book_id=\$1 AND id>\$2 ORDER BY id ASC LIMIT \$3`).
		WithArgs(int64(5), int64(100), 256).
		WillReturnRows(sqlmock.NewRows([]string{"id", "address_book_id", "uid", "resource_name", "raw_vcard", "etag", "display_name", "primary_email", "birthday", "last_modified"}).
			AddRow(int64(101), int64(5), "alice", "alice", "BEGIN:VCARD\r\nEND:VCARD\r\n", "etag", "Alice", "alice@example.com", nil, now))

	contacts, err := (&contactRepo{pool: db}).ListForBookPageAfter(context.Background(), 5, 100, 256)
	if err != nil {
		t.Fatalf("ListForBookPageAfter() error = %v", err)
	}
	if len(contacts) != 1 || contacts[0].ID != 101 {
		t.Fatalf("ListForBookPageAfter() = %#v", contacts)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("sql expectations: %v", err)
	}
}

func TestAddressBookRepositoryListsCollectionAndObjectAccessibleBooks(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New() error = %v", err)
	}
	defer db.Close()

	now := time.Now().UTC()
	mock.ExpectQuery(`(?s)FROM address_books b.*NOT EXISTS.*d\.is_grant=FALSE.*EXISTS.*g\.is_grant=TRUE.*JOIN contacts c.*c\.object_acl_path=g0\.resource_path_norm.*NOT EXISTS.*d\.is_grant=FALSE`).
		WithArgs(int64(4)).
		WillReturnRows(sqlmock.NewRows([]string{"id", "user_id", "name", "description", "ctag", "created_at", "updated_at"}).
			AddRow(int64(5), int64(9), "Shared", nil, int64(2), now, now))

	books, err := (&addressBookRepo{pool: db}).ListAccessible(context.Background(), 4)
	if err != nil {
		t.Fatalf("ListAccessible() error = %v", err)
	}
	if len(books) != 1 || books[0].ID != 5 {
		t.Fatalf("ListAccessible() = %#v", books)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("sql expectations: %v", err)
	}
}

func TestStoreCopyContactAndStateCopiesDeadPropertiesAndClearsDestinationState(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New() error = %v", err)
	}
	defer db.Close()

	st := New(db)
	now := time.Now().UTC()
	raw := "BEGIN:VCARD\r\nVERSION:3.0\r\nUID:alice\r\nFN:Alice\r\nEND:VCARD\r\n"
	contactColumns := []string{"id", "address_book_id", "uid", "resource_name", "raw_vcard", "etag", "display_name", "primary_email", "birthday", "last_modified"}

	mock.ExpectBegin()
	mock.ExpectQuery(regexp.QuoteMeta(`SELECT id, address_book_id, uid, resource_name, raw_vcard, etag, display_name, primary_email, birthday, last_modified FROM contacts WHERE address_book_id=$1 AND uid=$2`)).
		WithArgs(int64(2), "alice").
		WillReturnRows(sqlmock.NewRows(contactColumns).AddRow(int64(10), int64(2), "alice", "alice", raw, "old-etag", "Alice", "alice@example.com", nil, now))
	mock.ExpectQuery(regexp.QuoteMeta(`SELECT resource_name FROM contacts WHERE address_book_id=$1 AND uid=$2`)).
		WithArgs(int64(3), "alice").
		WillReturnRows(sqlmock.NewRows([]string{"resource_name"}).AddRow("copied"))
	mock.ExpectExec(regexp.QuoteMeta(`DELETE FROM contacts WHERE address_book_id=$1 AND resource_name=$2 AND uid<>$3`)).
		WithArgs(int64(3), "copied", "alice").
		WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectQuery(`INSERT INTO contacts`).
		WithArgs(int64(3), "alice", "copied", raw, "new-etag", "Alice", "alice@example.com", nil).
		WillReturnRows(sqlmock.NewRows(contactColumns).AddRow(int64(11), int64(3), "alice", "copied", raw, "new-etag", "Alice", "alice@example.com", nil, now))
	for _, statePath := range []string{"/dav/addressbooks/3/copied", "/dav/addressbooks/3/copied.vcf"} {
		mock.ExpectExec(regexp.QuoteMeta(`DELETE FROM locks WHERE resource_path=$1`)).WithArgs(statePath).WillReturnResult(sqlmock.NewResult(0, 1))
		mock.ExpectExec(regexp.QuoteMeta(`DELETE FROM acl_entries WHERE resource_path=$1`)).WithArgs(statePath).WillReturnResult(sqlmock.NewResult(0, 1))
		mock.ExpectExec(regexp.QuoteMeta(`DELETE FROM dav_dead_properties WHERE resource_path=$1`)).WithArgs(statePath).WillReturnResult(sqlmock.NewResult(0, 1))
	}
	mock.ExpectExec(`INSERT INTO dav_dead_properties`).
		WithArgs("/dav/addressbooks/3/copied", "/dav/addressbooks/2/alice").
		WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectExec(regexp.QuoteMeta(`DELETE FROM deleted_resources WHERE resource_type=$1 AND collection_id=$2 AND resource_name=$3`)).
		WithArgs("contact", int64(3), "copied").
		WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectCommit()

	copied, err := st.CopyContactAndState(context.Background(), 2, 3, "alice", "copied", "new-etag", "/dav/addressbooks/2/alice", "/dav/addressbooks/3/copied", "alice")
	if err != nil {
		t.Fatalf("CopyContactAndState() error = %v", err)
	}
	if copied == nil || copied.AddressBookID != 3 || copied.ResourceName != "copied" {
		t.Fatalf("CopyContactAndState() = %#v", copied)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("sql expectations: %v", err)
	}
}

func TestStoreCopyContactAndStateRejectsLateDestinationUIDConflict(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New() error = %v", err)
	}
	defer db.Close()

	st := New(db)
	now := time.Now().UTC()
	raw := "BEGIN:VCARD\r\nVERSION:3.0\r\nUID:alice\r\nFN:Alice\r\nEND:VCARD\r\n"
	contactColumns := []string{"id", "address_book_id", "uid", "resource_name", "raw_vcard", "etag", "display_name", "primary_email", "birthday", "last_modified"}

	mock.ExpectBegin()
	mock.ExpectQuery(regexp.QuoteMeta(`SELECT id, address_book_id, uid, resource_name, raw_vcard, etag, display_name, primary_email, birthday, last_modified FROM contacts WHERE address_book_id=$1 AND uid=$2`)).
		WithArgs(int64(2), "alice").
		WillReturnRows(sqlmock.NewRows(contactColumns).AddRow(int64(10), int64(2), "alice", "alice", raw, "old-etag", "Alice", nil, nil, now))
	mock.ExpectQuery(regexp.QuoteMeta(`SELECT resource_name FROM contacts WHERE address_book_id=$1 AND uid=$2`)).
		WithArgs(int64(3), "alice").
		WillReturnRows(sqlmock.NewRows([]string{"resource_name"}).AddRow("other-path"))
	mock.ExpectRollback()

	_, err = st.CopyContactAndState(context.Background(), 2, 3, "alice", "copied", "new-etag", "/dav/addressbooks/2/alice", "/dav/addressbooks/3/copied", "")
	if err != ErrConflict {
		t.Fatalf("CopyContactAndState() error = %v, want ErrConflict", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("sql expectations: %v", err)
	}
}
