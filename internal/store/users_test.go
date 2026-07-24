package store

import (
	"context"
	"os"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
)

func TestUserRepoUpsertOAuthUserSynchronizesProfileNames(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New() error = %v", err)
	}
	defer db.Close()

	repo := &userRepo{pool: db}
	now := time.Now().UTC()
	mock.ExpectQuery(regexp.QuoteMeta(`
INSERT INTO users (oauth_subject, primary_email, full_name, first_name)
VALUES ($1, $2, $3, $4)
ON CONFLICT (oauth_subject) DO UPDATE SET
        primary_email = EXCLUDED.primary_email,
        full_name = EXCLUDED.full_name,
        first_name = EXCLUDED.first_name,
        last_login_at = NOW()
RETURNING id, oauth_subject, primary_email, full_name, first_name, created_at, last_login_at, onboarding_completed_at
`)).
		WithArgs("oauth-subject", "dana@example.com", "Dana Lee", "Dana").
		WillReturnRows(sqlmock.NewRows([]string{"id", "oauth_subject", "primary_email", "full_name", "first_name", "created_at", "last_login_at", "onboarding_completed_at"}).
			AddRow(int64(7), "oauth-subject", "dana@example.com", "Dana Lee", "Dana", now, now, nil))

	user, err := repo.UpsertOAuthUser(context.Background(), "oauth-subject", "dana@example.com", "Dana Lee", "Dana")
	if err != nil {
		t.Fatalf("UpsertOAuthUser() error = %v", err)
	}
	if user.FullName != "Dana Lee" || user.FirstName != "Dana" {
		t.Fatalf("UpsertOAuthUser() = %#v", user)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("sql expectations: %v", err)
	}
}

func TestUserRepoUpsertOAuthUserClearsMissingProfileNames(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New() error = %v", err)
	}
	defer db.Close()

	repo := &userRepo{pool: db}
	now := time.Now().UTC()
	mock.ExpectQuery("INSERT INTO users").
		WithArgs("oauth-subject", "updated@example.com", "", "").
		WillReturnRows(sqlmock.NewRows([]string{"id", "oauth_subject", "primary_email", "full_name", "first_name", "created_at", "last_login_at", "onboarding_completed_at"}).
			AddRow(int64(7), "oauth-subject", "updated@example.com", "", "", now, now, nil))

	user, err := repo.UpsertOAuthUser(context.Background(), "oauth-subject", "updated@example.com", "", "")
	if err != nil {
		t.Fatalf("UpsertOAuthUser() error = %v", err)
	}
	if user.PrimaryEmail != "updated@example.com" || user.FullName != "" || user.FirstName != "" {
		t.Fatalf("UpsertOAuthUser() = %#v", user)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("sql expectations: %v", err)
	}
}

func TestOAuthProfileNamesMigration(t *testing.T) {
	contents, err := os.ReadFile("../../migrations/v1.1.9.sql")
	if err != nil {
		t.Fatalf("ReadFile() error = %v", err)
	}
	sql := string(contents)
	for _, expected := range []string{
		"ALTER TABLE users ADD COLUMN IF NOT EXISTS full_name TEXT NOT NULL DEFAULT ''",
		"ALTER TABLE users ADD COLUMN IF NOT EXISTS first_name TEXT NOT NULL DEFAULT ''",
		"UPDATE application SET value = 'v1.1.9'",
	} {
		if !strings.Contains(sql, expected) {
			t.Errorf("migration missing %q", expected)
		}
	}
}

func TestUserDisplayNamesPreferOAuthProfileAndFallBackToEmail(t *testing.T) {
	tests := []struct {
		name          string
		user          User
		displayName   string
		greetingName  string
		referenceName string
	}{
		{
			name:          "profile names",
			user:          User{PrimaryEmail: "dana@example.com", FullName: "Dana Lee", FirstName: "Dana"},
			displayName:   "Dana Lee",
			greetingName:  "Dana",
			referenceName: "Dana Lee (dana@example.com)",
		},
		{
			name:          "email fallback",
			user:          User{PrimaryEmail: "dana@example.com"},
			displayName:   "dana@example.com",
			greetingName:  "dana@example.com",
			referenceName: "dana@example.com",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.user.DisplayName(); got != tt.displayName {
				t.Errorf("DisplayName() = %q, want %q", got, tt.displayName)
			}
			if got := tt.user.GreetingName(); got != tt.greetingName {
				t.Errorf("GreetingName() = %q, want %q", got, tt.greetingName)
			}
			if got := tt.user.ReferenceName(); got != tt.referenceName {
				t.Errorf("ReferenceName() = %q, want %q", got, tt.referenceName)
			}
		})
	}
}
