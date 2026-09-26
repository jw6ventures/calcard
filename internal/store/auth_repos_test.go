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
	md5HA1 := "md5-ha1"
	sha256HA1 := "sha256-ha1"

	mock.ExpectQuery(regexp.QuoteMeta(`
INSERT INTO app_passwords (user_id, label, token_hash, digest_md5_ha1, digest_sha256_ha1, expires_at)
VALUES ($1, $2, $3, $4, $5, $6)
RETURNING id, user_id, label, token_hash, digest_md5_ha1, digest_sha256_ha1, created_at, expires_at, revoked_at, last_used_at
`)).
		WithArgs(int64(7), "Laptop", "hash", &md5HA1, &sha256HA1, &expires).
		WillReturnRows(sqlmock.NewRows([]string{"id", "user_id", "label", "token_hash", "digest_md5_ha1", "digest_sha256_ha1", "created_at", "expires_at", "revoked_at", "last_used_at"}).
			AddRow(int64(1), int64(7), "Laptop", "hash", md5HA1, sha256HA1, now, expires, nil, nil))

	created, err := repo.Create(context.Background(), AppPassword{
		UserID:          7,
		Label:           "Laptop",
		TokenHash:       "hash",
		DigestMD5HA1:    &md5HA1,
		DigestSHA256HA1: &sha256HA1,
		ExpiresAt:       &expires,
	})
	if err != nil {
		t.Fatalf("Create() error = %v", err)
	}
	if created.ID != 1 || created.ExpiresAt == nil || !created.ExpiresAt.Equal(expires) {
		t.Fatalf("Create() = %#v", created)
	}

	mock.ExpectQuery(regexp.QuoteMeta(`
SELECT id, user_id, label, token_hash, digest_md5_ha1, digest_sha256_ha1, created_at, expires_at, revoked_at, last_used_at
FROM app_passwords
WHERE user_id=$1 AND revoked_at IS NULL AND (expires_at IS NULL OR expires_at > NOW())
ORDER BY created_at DESC
`)).
		WithArgs(int64(7)).
		WillReturnRows(sqlmock.NewRows([]string{"id", "user_id", "label", "token_hash", "digest_md5_ha1", "digest_sha256_ha1", "created_at", "expires_at", "revoked_at", "last_used_at"}).
			AddRow(int64(1), int64(7), "Laptop", "hash", md5HA1, sha256HA1, now, expires, nil, lastUsed))

	found, err := repo.FindValidByUser(context.Background(), 7)
	if err != nil {
		t.Fatalf("FindValidByUser() error = %v", err)
	}
	if len(found) != 1 || found[0].LastUsedAt == nil || !found[0].LastUsedAt.Equal(lastUsed) {
		t.Fatalf("FindValidByUser() = %#v", found)
	}

	mock.ExpectQuery(regexp.QuoteMeta(`SELECT id, user_id, label, token_hash, digest_md5_ha1, digest_sha256_ha1, created_at, expires_at, revoked_at, last_used_at FROM app_passwords WHERE id=$1`)).
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

// Purging may never cost a credential its Basic access, which the token hash
// alone decides, and it has to be safe to repeat.
func TestAppPasswordRepoPurgesDigestCredentialsWithoutBreakingBasic(t *testing.T) {
	st := newPostgresStore(t)
	ctx := context.Background()
	user, err := st.Users.UpsertOAuthUser(ctx, "subject-purge", "purge@example.test", "Test User", "Test")
	if err != nil {
		t.Fatalf("UpsertOAuthUser() error = %v", err)
	}

	md5HA1, sha256HA1 := "sealed-md5", "sealed-sha256"
	sealed, err := st.AppPasswords.Create(ctx, AppPassword{
		UserID:          user.ID,
		Label:           "sealed",
		TokenHash:       "bcrypt-hash",
		DigestMD5HA1:    &md5HA1,
		DigestSHA256HA1: &sha256HA1,
	})
	if err != nil {
		t.Fatalf("Create() error = %v", err)
	}
	partial, err := st.AppPasswords.Create(ctx, AppPassword{UserID: user.ID, Label: "partial", TokenHash: "bcrypt-hash", DigestSHA256HA1: &sha256HA1})
	if err != nil {
		t.Fatalf("Create() error = %v", err)
	}
	bare, err := st.AppPasswords.Create(ctx, AppPassword{UserID: user.ID, Label: "bare", TokenHash: "bcrypt-hash"})
	if err != nil {
		t.Fatalf("Create() error = %v", err)
	}

	purged, err := st.AppPasswords.PurgeDigestCredentials(ctx)
	if err != nil {
		t.Fatalf("PurgeDigestCredentials() error = %v", err)
	}
	if purged != 2 {
		t.Fatalf("PurgeDigestCredentials() = %d, want 2 (the rows holding any HA1)", purged)
	}
	for _, id := range []int64{sealed.ID, partial.ID, bare.ID} {
		requireAppPasswordDigestState(t, st, id, nil, nil)
	}

	again, err := st.AppPasswords.PurgeDigestCredentials(ctx)
	if err != nil {
		t.Fatalf("repeated PurgeDigestCredentials() error = %v", err)
	}
	if again != 0 {
		t.Fatalf("repeated PurgeDigestCredentials() = %d, want 0", again)
	}
}

// Replacing an HA1 is one compare-and-swap, so two requests acting on
// different reads of the same row cannot interleave into a state neither
// intended: the one whose read is stale changes nothing and is told so.
func TestAppPasswordRepoReplaceDigestCredentialsIsACompareAndSwap(t *testing.T) {
	st := newPostgresStore(t)
	ctx := context.Background()
	user, err := st.Users.UpsertOAuthUser(ctx, "subject-replace", "replace@example.test", "Test User", "Test")
	if err != nil {
		t.Fatalf("UpsertOAuthUser() error = %v", err)
	}
	ptr := func(value string) *string { return &value }

	for _, tc := range []struct {
		name                 string
		storedMD5, storedSHA *string
		oldMD5, oldSHA       *string
		newMD5, newSHA       *string
		wantSwapped          bool
	}{
		{name: "fills an empty row", newMD5: ptr("a-md5"), newSHA: ptr("a-sha"), wantSwapped: true},
		{name: "replaces what the caller read", storedMD5: ptr("old-md5"), storedSHA: ptr("old-sha"), oldMD5: ptr("old-md5"), oldSHA: ptr("old-sha"), newMD5: ptr("a-md5"), newSHA: ptr("a-sha"), wantSwapped: true},
		{name: "replaces a partial row", storedSHA: ptr("old-sha"), oldSHA: ptr("old-sha"), newMD5: ptr("a-md5"), newSHA: ptr("a-sha"), wantSwapped: true},
		{name: "clears what the caller read", storedMD5: ptr("old-md5"), storedSHA: ptr("old-sha"), oldMD5: ptr("old-md5"), oldSHA: ptr("old-sha"), wantSwapped: true},
		{name: "loses to a concurrent fill", storedMD5: ptr("b-md5"), storedSHA: ptr("b-sha"), newMD5: ptr("a-md5"), newSHA: ptr("a-sha")},
		{name: "loses to a concurrent replacement", storedMD5: ptr("b-md5"), storedSHA: ptr("b-sha"), oldMD5: ptr("old-md5"), oldSHA: ptr("old-sha"), newMD5: ptr("a-md5"), newSHA: ptr("a-sha")},
		{name: "loses to a concurrent clear", oldMD5: ptr("old-md5"), oldSHA: ptr("old-sha"), newMD5: ptr("a-md5"), newSHA: ptr("a-sha")},
		{name: "matches both columns, not one", storedMD5: ptr("old-md5"), storedSHA: ptr("b-sha"), oldMD5: ptr("old-md5"), oldSHA: ptr("old-sha")},
	} {
		t.Run(tc.name, func(t *testing.T) {
			created, err := st.AppPasswords.Create(ctx, AppPassword{UserID: user.ID, Label: tc.name, TokenHash: "bcrypt-hash", DigestMD5HA1: tc.storedMD5, DigestSHA256HA1: tc.storedSHA})
			if err != nil {
				t.Fatalf("Create() error = %v", err)
			}
			swapped, err := st.AppPasswords.ReplaceDigestCredentials(ctx, created.ID, tc.newMD5, tc.newSHA, tc.oldMD5, tc.oldSHA)
			if err != nil {
				t.Fatalf("ReplaceDigestCredentials() error = %v", err)
			}
			if swapped != tc.wantSwapped {
				t.Fatalf("ReplaceDigestCredentials() = %v, want %v", swapped, tc.wantSwapped)
			}
			wantMD5, wantSHA := tc.storedMD5, tc.storedSHA
			if tc.wantSwapped {
				wantMD5, wantSHA = tc.newMD5, tc.newSHA
			}
			requireAppPasswordDigestState(t, st, created.ID, wantMD5, wantSHA)
		})
	}

	swapped, err := st.AppPasswords.ReplaceDigestCredentials(ctx, 1<<40, ptr("a-md5"), ptr("a-sha"), nil, nil)
	if err != nil || swapped {
		t.Fatalf("ReplaceDigestCredentials() on a missing row = %v, %v; want false, nil", swapped, err)
	}
}

func requireAppPasswordDigestState(t *testing.T, st *Store, id int64, wantMD5, wantSHA *string) {
	t.Helper()
	got, err := st.AppPasswords.GetByID(context.Background(), id)
	if err != nil || got == nil {
		t.Fatalf("GetByID(%d) = %#v, %v", id, got, err)
	}
	for column, pair := range map[string][2]*string{"digest_md5_ha1": {got.DigestMD5HA1, wantMD5}, "digest_sha256_ha1": {got.DigestSHA256HA1, wantSHA}} {
		if (pair[0] == nil) != (pair[1] == nil) || pair[0] != nil && *pair[0] != *pair[1] {
			t.Fatalf("app password %d %s = %v, want %v", id, column, derefForLog(pair[0]), derefForLog(pair[1]))
		}
	}
	if got.TokenHash != "bcrypt-hash" {
		t.Fatalf("app password %d token hash = %q, want it untouched", id, got.TokenHash)
	}
}

func derefForLog(value *string) string {
	if value == nil {
		return "<NULL>"
	}
	return *value
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

	mock.ExpectQuery(regexp.QuoteMeta(`SELECT id, resource_type, collection_id, uid, resource_name, deleted_at FROM deleted_resources WHERE resource_type=$1 AND collection_id=$2 AND id>$3 AND deleted_at > $4 ORDER BY id ASC LIMIT $5`)).
		WithArgs("event", int64(4), int64(7), since, 256).
		WillReturnRows(sqlmock.NewRows([]string{"id", "resource_type", "collection_id", "uid", "resource_name", "deleted_at"}).
			AddRow(int64(8), "event", int64(4), "uid-1", "uid-1.ics", deletedAt))

	items, err := repo.ListDeletedSincePageAfter(context.Background(), "event", 4, 7, since, 256)
	if err != nil {
		t.Fatalf("ListDeletedSincePageAfter() error = %v", err)
	}
	if len(items) != 1 || items[0].UID != "uid-1" {
		t.Fatalf("ListDeletedSincePageAfter() = %#v", items)
	}
	empty, err := repo.ListDeletedSincePageAfter(context.Background(), "event", 4, 7, since, 0)
	if err != nil {
		t.Fatalf("ListDeletedSincePageAfter(limit 0) error = %v", err)
	}
	if len(empty) != 0 {
		t.Fatalf("ListDeletedSincePageAfter(limit 0) = %#v", empty)
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
		*(dest[4].(*sql.NullString)) = sql.NullString{String: "md5-ha1", Valid: true}
		*(dest[5].(*sql.NullString)) = sql.NullString{}
		*(dest[6].(*time.Time)) = now
		*(dest[7].(*sql.NullTime)) = sql.NullTime{}
		*(dest[8].(*sql.NullTime)) = sql.NullTime{}
		*(dest[9].(*sql.NullTime)) = sql.NullTime{}
		return nil
	})
	if err != nil || app.DigestMD5HA1 == nil || *app.DigestMD5HA1 != "md5-ha1" || app.DigestSHA256HA1 != nil || app.ExpiresAt != nil || app.RevokedAt != nil || app.LastUsedAt != nil {
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
