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

func TestAppPasswordRepoCRUDAndQueries(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New() error = %v", err)
	}
	defer db.Close()

	repo := &appPasswordRepo{pool: db}
	now := time.Now().UTC()
	expires := now.Add(24 * time.Hour)
	lastUsed := now.Add(time.Hour)

	mock.ExpectQuery(regexp.QuoteMeta(`
INSERT INTO app_passwords (user_id, label, token_hash, expires_at)
VALUES ($1, $2, $3, $4)
RETURNING id, user_id, label, token_hash, created_at, expires_at, revoked_at, last_used_at
`)).
		WithArgs(int64(7), "Laptop", "hash", &expires).
		WillReturnRows(sqlmock.NewRows([]string{"id", "user_id", "label", "token_hash", "created_at", "expires_at", "revoked_at", "last_used_at"}).
			AddRow(int64(1), int64(7), "Laptop", "hash", now, expires, nil, nil))

	created, err := repo.Create(context.Background(), AppPassword{
		UserID:    7,
		Label:     "Laptop",
		TokenHash: "hash",
		ExpiresAt: &expires,
	})
	if err != nil {
		t.Fatalf("Create() error = %v", err)
	}
	if created.ID != 1 || created.ExpiresAt == nil || !created.ExpiresAt.Equal(expires) {
		t.Fatalf("Create() = %#v", created)
	}

	mock.ExpectQuery(regexp.QuoteMeta(`
SELECT id, user_id, label, token_hash, created_at, expires_at, revoked_at, last_used_at
FROM app_passwords
WHERE user_id=$1 AND revoked_at IS NULL AND (expires_at IS NULL OR expires_at > NOW())
ORDER BY created_at DESC
`)).
		WithArgs(int64(7)).
		WillReturnRows(sqlmock.NewRows([]string{"id", "user_id", "label", "token_hash", "created_at", "expires_at", "revoked_at", "last_used_at"}).
			AddRow(int64(1), int64(7), "Laptop", "hash", now, expires, nil, lastUsed))

	found, err := repo.FindValidByUser(context.Background(), 7)
	if err != nil {
		t.Fatalf("FindValidByUser() error = %v", err)
	}
	if len(found) != 1 || found[0].LastUsedAt == nil || !found[0].LastUsedAt.Equal(lastUsed) {
		t.Fatalf("FindValidByUser() = %#v", found)
	}

	mock.ExpectQuery(regexp.QuoteMeta(`SELECT id, user_id, label, token_hash, created_at, expires_at, revoked_at, last_used_at FROM app_passwords WHERE id=$1`)).
		WithArgs(int64(9)).
		WillReturnError(sql.ErrNoRows)

	got, err := repo.GetByID(context.Background(), 9)
	if err != nil {
		t.Fatalf("GetByID() error = %v", err)
	}
	if got != nil {
		t.Fatalf("GetByID() = %#v, want nil", got)
	}

	mock.ExpectExec(regexp.QuoteMeta(`UPDATE app_passwords SET revoked_at = NOW() WHERE id=$1`)).
		WithArgs(int64(1)).
		WillReturnResult(sqlmock.NewResult(0, 1))
	if err := repo.Revoke(context.Background(), 1); err != nil {
		t.Fatalf("Revoke() error = %v", err)
	}

	mock.ExpectExec(regexp.QuoteMeta(`DELETE FROM app_passwords WHERE id=$1 AND revoked_at IS NOT NULL`)).
		WithArgs(int64(1)).
		WillReturnResult(sqlmock.NewResult(0, 1))
	if err := repo.DeleteRevoked(context.Background(), 1); err != nil {
		t.Fatalf("DeleteRevoked() error = %v", err)
	}

	mock.ExpectExec(regexp.QuoteMeta(`UPDATE app_passwords SET last_used_at = NOW() WHERE id=$1`)).
		WithArgs(int64(1)).
		WillReturnResult(sqlmock.NewResult(0, 1))
	if err := repo.TouchLastUsed(context.Background(), 1); err != nil {
		t.Fatalf("TouchLastUsed() error = %v", err)
	}

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("sql expectations: %v", err)
	}
}

func TestDeletedResourceRepoListAndCleanup(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New() error = %v", err)
	}
	defer db.Close()

	repo := &deletedResourceRepo{pool: db}
	since := time.Now().Add(-time.Hour).UTC()
	deletedAt := since.Add(10 * time.Minute)

	mock.ExpectQuery(regexp.QuoteMeta(`SELECT id, resource_type, collection_id, uid, resource_name, deleted_at FROM deleted_resources WHERE resource_type=$1 AND collection_id=$2 AND deleted_at > $3 ORDER BY deleted_at DESC`)).
		WithArgs("event", int64(4), since).
		WillReturnRows(sqlmock.NewRows([]string{"id", "resource_type", "collection_id", "uid", "resource_name", "deleted_at"}).
			AddRow(int64(8), "event", int64(4), "uid-1", "uid-1.ics", deletedAt))

	items, err := repo.ListDeletedSince(context.Background(), "event", 4, since)
	if err != nil {
		t.Fatalf("ListDeletedSince() error = %v", err)
	}
	if len(items) != 1 || items[0].UID != "uid-1" {
		t.Fatalf("ListDeletedSince() = %#v", items)
	}

	mock.ExpectExec(regexp.QuoteMeta(`DELETE FROM deleted_resources WHERE deleted_at < $1`)).
		WithArgs(sqlmock.AnyArg()).
		WillReturnResult(sqlmock.NewResult(0, 3))

	rows, err := repo.Cleanup(context.Background(), 24*time.Hour)
	if err != nil {
		t.Fatalf("Cleanup() error = %v", err)
	}
	if rows != 3 {
		t.Fatalf("Cleanup() rows = %d", rows)
	}

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("sql expectations: %v", err)
	}
}

func TestSessionRepoCRUDQueriesAndNilOnMissing(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New() error = %v", err)
	}
	defer db.Close()

	repo := &sessionRepo{pool: db}
	now := time.Now().UTC()
	expires := now.Add(7 * 24 * time.Hour)
	lastSeen := now.Add(time.Minute)
	userAgent := "CalCard Test"
	ip := "198.51.100.1"

	mock.ExpectQuery(regexp.QuoteMeta(`
INSERT INTO sessions (id, user_id, user_agent, ip_address, expires_at)
VALUES ($1, $2, $3, $4, $5)
RETURNING id, user_id, user_agent, ip_address, created_at, expires_at, last_seen_at
`)).
		WithArgs("session-1", int64(4), &userAgent, &ip, expires).
		WillReturnRows(sqlmock.NewRows([]string{"id", "user_id", "user_agent", "ip_address", "created_at", "expires_at", "last_seen_at"}).
			AddRow("session-1", int64(4), userAgent, ip, now, expires, lastSeen))

	created, err := repo.Create(context.Background(), Session{
		ID:        "session-1",
		UserID:    4,
		UserAgent: &userAgent,
		IPAddress: &ip,
		ExpiresAt: expires,
	})
	if err != nil {
		t.Fatalf("Create() error = %v", err)
	}
	if created.ID != "session-1" || created.UserAgent == nil || *created.UserAgent != userAgent {
		t.Fatalf("Create() = %#v", created)
	}

	mock.ExpectQuery(regexp.QuoteMeta(`SELECT id, user_id, user_agent, ip_address, created_at, expires_at, last_seen_at FROM sessions WHERE id=$1 AND expires_at > NOW()`)).
		WithArgs("missing").
		WillReturnError(sql.ErrNoRows)

	got, err := repo.GetByID(context.Background(), "missing")
	if err != nil {
		t.Fatalf("GetByID() error = %v", err)
	}
	if got != nil {
		t.Fatalf("GetByID() = %#v, want nil", got)
	}

	mock.ExpectQuery(regexp.QuoteMeta(`SELECT id, user_id, user_agent, ip_address, created_at, expires_at, last_seen_at FROM sessions WHERE user_id=$1 AND expires_at > NOW() ORDER BY last_seen_at DESC`)).
		WithArgs(int64(4)).
		WillReturnRows(sqlmock.NewRows([]string{"id", "user_id", "user_agent", "ip_address", "created_at", "expires_at", "last_seen_at"}).
			AddRow("session-1", int64(4), userAgent, ip, now, expires, lastSeen))

	list, err := repo.ListByUser(context.Background(), 4)
	if err != nil {
		t.Fatalf("ListByUser() error = %v", err)
	}
	if len(list) != 1 || list[0].IPAddress == nil || *list[0].IPAddress != ip {
		t.Fatalf("ListByUser() = %#v", list)
	}

	mock.ExpectExec(regexp.QuoteMeta(`UPDATE sessions SET last_seen_at = NOW() WHERE id=$1`)).
		WithArgs("session-1").
		WillReturnResult(sqlmock.NewResult(0, 1))
	if err := repo.TouchLastSeen(context.Background(), "session-1"); err != nil {
		t.Fatalf("TouchLastSeen() error = %v", err)
	}

	mock.ExpectExec(regexp.QuoteMeta(`DELETE FROM sessions WHERE id=$1`)).
		WithArgs("session-1").
		WillReturnResult(sqlmock.NewResult(0, 1))
	if err := repo.Delete(context.Background(), "session-1"); err != nil {
		t.Fatalf("Delete() error = %v", err)
	}

	mock.ExpectExec(regexp.QuoteMeta(`DELETE FROM sessions WHERE user_id=$1`)).
		WithArgs(int64(4)).
		WillReturnResult(sqlmock.NewResult(0, 2))
	if err := repo.DeleteByUser(context.Background(), 4); err != nil {
		t.Fatalf("DeleteByUser() error = %v", err)
	}

	mock.ExpectExec(regexp.QuoteMeta(`DELETE FROM sessions WHERE expires_at < NOW()`)).
		WillReturnResult(sqlmock.NewResult(0, 5))
	rows, err := repo.DeleteExpired(context.Background())
	if err != nil {
		t.Fatalf("DeleteExpired() error = %v", err)
	}
	if rows != 5 {
		t.Fatalf("DeleteExpired() rows = %d", rows)
	}

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("sql expectations: %v", err)
	}
}

func TestScanHelpersHandleNullableFields(t *testing.T) {
	now := time.Now().UTC()

	app, err := scanAppPassword(func(dest ...any) error {
		*(dest[0].(*int64)) = 1
		*(dest[1].(*int64)) = 2
		*(dest[2].(*string)) = "Laptop"
		*(dest[3].(*string)) = "hash"
		*(dest[4].(*time.Time)) = now
		*(dest[5].(*sql.NullTime)) = sql.NullTime{}
		*(dest[6].(*sql.NullTime)) = sql.NullTime{}
		*(dest[7].(*sql.NullTime)) = sql.NullTime{}
		return nil
	})
	if err != nil || app.ExpiresAt != nil || app.RevokedAt != nil || app.LastUsedAt != nil {
		t.Fatalf("scanAppPassword() = %#v, %v", app, err)
	}

	session, err := scanSession(func(dest ...any) error {
		*(dest[0].(*string)) = "session-1"
		*(dest[1].(*int64)) = 2
		*(dest[2].(*sql.NullString)) = sql.NullString{String: "ua", Valid: true}
		*(dest[3].(*sql.NullString)) = sql.NullString{}
		*(dest[4].(*time.Time)) = now
		*(dest[5].(*time.Time)) = now
		*(dest[6].(*time.Time)) = now
		return nil
	})
	if err != nil || session.UserAgent == nil || *session.UserAgent != "ua" || session.IPAddress != nil {
		t.Fatalf("scanSession() = %#v, %v", session, err)
	}

	if _, err := scanSession(func(dest ...any) error { return errors.New("boom") }); err == nil {
		t.Fatal("expected scanSession error")
	}
}
