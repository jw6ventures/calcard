package store

import (
	"cmp"
	"context"
	"database/sql"
	"errors"
	"hash/fnv"
	"path"
	"slices"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"

	icalpkg "github.com/jw6ventures/calcard/internal/ical"
	"github.com/jw6ventures/calcard/internal/util"
	"github.com/jw6ventures/calcard/internal/vcard"
	"github.com/lib/pq"
)

// userRepo implements UserRepository.
type userRepo struct {
	pool *sql.DB
}

func (r *userRepo) UpsertOAuthUser(ctx context.Context, subject, email, fullName, firstName string) (*User, error) {
	const q = `
WITH previous AS MATERIALIZED (
    SELECT id, primary_email FROM users WHERE oauth_subject = $1
), upserted AS (
INSERT INTO users (oauth_subject, primary_email, full_name, first_name)
VALUES ($1, $2, $3, $4)
ON CONFLICT (oauth_subject) DO UPDATE SET
        primary_email = EXCLUDED.primary_email,
        full_name = EXCLUDED.full_name,
        first_name = EXCLUDED.first_name,
        last_login_at = NOW()
RETURNING id, oauth_subject, primary_email, full_name, first_name, created_at, last_login_at, onboarding_completed_at
), revoked AS (
    UPDATE app_passwords
    SET revoked_at = NOW()
    WHERE user_id = (SELECT id FROM upserted)
      AND revoked_at IS NULL
      AND EXISTS (
          SELECT 1 FROM previous
          WHERE previous.primary_email IS DISTINCT FROM $2
      )
    RETURNING id
)
SELECT id, oauth_subject, primary_email, full_name, first_name, created_at, last_login_at, onboarding_completed_at
FROM upserted
`
	defer observeDB(ctx, "users.upsert_oauth")()
	row := r.pool.QueryRowContext(ctx, q, subject, email, fullName, firstName)
	var u User
	if err := row.Scan(&u.ID, &u.OAuthSubject, &u.PrimaryEmail, &u.FullName, &u.FirstName, &u.CreatedAt, &u.LastLoginAt, &u.OnboardingCompletedAt); err != nil {
		return nil, err
	}
	return &u, nil
}

func (r *userRepo) GetByID(ctx context.Context, id int64) (*User, error) {
	const q = `SELECT id, oauth_subject, primary_email, full_name, first_name, created_at, last_login_at, onboarding_completed_at FROM users WHERE id=$1`
	defer observeDB(ctx, "users.get_by_id")()
	var u User
	if err := r.pool.QueryRowContext(ctx, q, id).Scan(&u.ID, &u.OAuthSubject, &u.PrimaryEmail, &u.FullName, &u.FirstName, &u.CreatedAt, &u.LastLoginAt, &u.OnboardingCompletedAt); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, nil
		}
		return nil, err
	}
	return &u, nil
}

func (r *userRepo) GetByEmail(ctx context.Context, email string) (*User, error) {
	const q = `SELECT id, oauth_subject, primary_email, full_name, first_name, created_at, last_login_at, onboarding_completed_at FROM users WHERE primary_email=$1`
	defer observeDB(ctx, "users.get_by_email")()
	var u User
	if err := r.pool.QueryRowContext(ctx, q, email).Scan(&u.ID, &u.OAuthSubject, &u.PrimaryEmail, &u.FullName, &u.FirstName, &u.CreatedAt, &u.LastLoginAt, &u.OnboardingCompletedAt); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, nil
		}
		return nil, err
	}
	return &u, nil
}

func (r *userRepo) ListActive(ctx context.Context) ([]User, error) {
	const q = `SELECT id, oauth_subject, primary_email, full_name, first_name, created_at, last_login_at, onboarding_completed_at FROM users WHERE last_login_at IS NOT NULL ORDER BY COALESCE(NULLIF(full_name, ''), primary_email), primary_email`
	defer observeDB(ctx, "users.list_active")()
	rows, err := r.pool.QueryContext(ctx, q)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var users []User
	for rows.Next() {
		var u User
		if err := rows.Scan(&u.ID, &u.OAuthSubject, &u.PrimaryEmail, &u.FullName, &u.FirstName, &u.CreatedAt, &u.LastLoginAt, &u.OnboardingCompletedAt); err != nil {
			return nil, err
		}
		users = append(users, u)
	}
	return users, rows.Err()
}

// MarkOnboardingComplete records that the user has finished the welcome tour.
// It is idempotent: a user who has already completed onboarding is left unchanged.
func (r *userRepo) MarkOnboardingComplete(ctx context.Context, userID int64) error {
	const q = `UPDATE users SET onboarding_completed_at = NOW() WHERE id=$1 AND onboarding_completed_at IS NULL`
	defer observeDB(ctx, "users.mark_onboarding")()
	_, err := r.pool.ExecContext(ctx, q, userID)
	return err
}

// calendarRepo implements CalendarRepository.
type calendarRepo struct {
	pool *sql.DB
}

func sqlLiteralList(items ...string) string {
	quoted := make([]string, 0, len(items))
	for _, item := range items {
		quoted = append(quoted, "'"+item+"'")
	}
	return strings.Join(quoted, ", ")
}

func calendarACLBooleanExpr(userParam string, privileges ...string) string {
	return aclOrderedBooleanExpr("a.resource_path = '/dav/calendars/' || c.id::text", userParam, privileges...)
}

func aclPrincipalListExpr(userParam string) string {
	return "('DAV:all', 'DAV:authenticated', '/dav/principals/' || " + userParam + "::text || '/')"
}

// Event ACL paths are matched against the normalized columns
// acl_entries.resource_path_norm and events.object_acl_path (both strip the
// trailing .ics/.vcf so a grant stored either way lines up). The equality is
// index-backed.
func aclOrderedDecisionExpr(resourcePredicate, userParam string, privileges ...string) string {
	privilegeList := sqlLiteralList(privileges...)
	return `(SELECT a.is_grant
           FROM acl_entries a
           WHERE ` + resourcePredicate + `
             AND a.principal_href IN ` + aclPrincipalListExpr(userParam) + `
             AND a.privilege IN (` + privilegeList + `)
           ORDER BY a.ace_order, a.id
           LIMIT 1)`
}

func aclOrderedBooleanExpr(resourcePredicate, userParam string, privileges ...string) string {
	return `COALESCE(` + aclOrderedDecisionExpr(resourcePredicate, userParam, privileges...) + `, FALSE)`
}

func calendarEventACLAllowsExpr(userParam string, privileges ...string) string {
	return aclOrderedBooleanExpr("a.resource_path_norm = e.object_acl_path", userParam, privileges...)
}

func calendarEventACLAllowsWithCollectionFallbackExpr(userParam string, privileges ...string) string {
	return `COALESCE(` + aclOrderedDecisionExpr("a.resource_path_norm = e.object_acl_path", userParam, privileges...) + `, ` + calendarACLBooleanExpr(userParam, privileges...) + `)`
}

func addressBookACLBooleanExpr(userParam string, privileges ...string) string {
	return aclOrderedBooleanExpr("a.resource_path = '/dav/addressbooks/' || b.id::text", userParam, privileges...)
}

func contactACLBooleanExpr(userParam string, privileges ...string) string {
	return aclOrderedBooleanExpr("a.resource_path_norm = c.object_acl_path", userParam, privileges...)
}

func calendarACLAnyAccessExpr(userParam string) string {
	return `(` +
		calendarACLBooleanExpr(userParam, "read", "all") + `
           OR ` + calendarACLBooleanExpr(userParam, "read-free-busy", "read", "all") + `
           OR ` + calendarACLBooleanExpr(userParam, "write", "all") + `
           OR ` + calendarACLBooleanExpr(userParam, "write-content", "write", "all") + `
           OR ` + calendarACLBooleanExpr(userParam, "write-properties", "write", "all") + `
		   OR ` + calendarACLBooleanExpr(userParam, "bind", "write", "all") + `
		   OR ` + calendarACLBooleanExpr(userParam, "unbind", "write", "all") + `
       )`
}

// calendarObjectACLAnyAccessExpr is true when the principal has any access to at
// least one object inside calendar c via an object-level ACL. It drives from the
// principal's grant entries (typically a handful) joined to the matching event,
// rather than scanning every event in the calendar, then re-applies the full
// per-privilege grant/deny check so deny semantics are preserved exactly.
func calendarObjectACLAnyAccessExpr(userParam string) string {
	return `EXISTS (
           SELECT 1
           FROM acl_entries g0
           JOIN events e
             ON e.calendar_id = c.id
            AND e.object_acl_path = g0.resource_path_norm
           WHERE g0.principal_href IN ` + aclPrincipalListExpr(userParam) + `
             AND g0.is_grant = TRUE
             AND (
                 ` + calendarEventACLAllowsExpr(userParam, "read", "all") + `
                 OR ` + calendarEventACLAllowsExpr(userParam, "read-free-busy", "read", "all") + `
                 OR ` + calendarEventACLAllowsExpr(userParam, "write", "all") + `
                 OR ` + calendarEventACLAllowsExpr(userParam, "write-content", "write", "all") + `
                 OR ` + calendarEventACLAllowsExpr(userParam, "write-properties", "write", "all") + `
                 OR ` + calendarEventACLAllowsExpr(userParam, "bind", "write", "all") + `
                 OR ` + calendarEventACLAllowsExpr(userParam, "unbind", "write", "all") + `
             )
       )`
}

func (r *calendarRepo) ListByUser(ctx context.Context, userID int64) ([]Calendar, error) {
	const q = `SELECT id, user_id, name, slug, description, description_lang, timezone, color, supported_components, ctag, created_at, updated_at FROM calendars WHERE user_id=$1 ORDER BY created_at`
	defer observeDB(ctx, "calendars.list_by_user")()
	rows, err := r.pool.QueryContext(ctx, q, userID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var result []Calendar
	for rows.Next() {
		var c Calendar
		var slug, description, descriptionLang, timezone, color sql.NullString
		var components pq.StringArray
		if err := rows.Scan(&c.ID, &c.UserID, &c.Name, &slug, &description, &descriptionLang, &timezone, &color, &components, &c.CTag, &c.CreatedAt, &c.UpdatedAt); err != nil {
			return nil, err
		}
		c.Slug = nullableString(slug)
		c.Description = nullableString(description)
		c.DescriptionLang = nullableString(descriptionLang)
		c.Timezone = nullableString(timezone)
		c.Color = nullableString(color)
		c.SupportedComponents = components
		result = append(result, c)
	}
	return result, rows.Err()
}

func (r *calendarRepo) GetByID(ctx context.Context, id int64) (*Calendar, error) {
	const q = `SELECT id, user_id, name, slug, description, description_lang, timezone, color, supported_components, ctag, created_at, updated_at FROM calendars WHERE id=$1`
	defer observeDB(ctx, "calendars.get_by_id")()
	var c Calendar
	var slug, description, descriptionLang, timezone, color sql.NullString
	var components pq.StringArray
	if err := r.pool.QueryRowContext(ctx, q, id).Scan(&c.ID, &c.UserID, &c.Name, &slug, &description, &descriptionLang, &timezone, &color, &components, &c.CTag, &c.CreatedAt, &c.UpdatedAt); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, nil
		}
		return nil, err
	}
	c.Slug = nullableString(slug)
	c.Description = nullableString(description)
	c.DescriptionLang = nullableString(descriptionLang)
	c.Timezone = nullableString(timezone)
	c.Color = nullableString(color)
	c.SupportedComponents = components
	return &c, nil
}

func (r *calendarRepo) ListAccessible(ctx context.Context, userID int64) ([]CalendarAccess, error) {
	q := `
SELECT c.id, c.user_id, c.name, c.slug, c.description, c.description_lang, c.timezone, c.color, c.supported_components, c.ctag, c.created_at, c.updated_at,
       u.primary_email as owner_email,
       CASE WHEN c.user_id = $1 THEN FALSE ELSE TRUE END as shared,
       CASE WHEN c.user_id = $1 THEN TRUE ELSE ` + calendarACLBooleanExpr("$1", "read", "all") + ` END as can_read,
       CASE WHEN c.user_id = $1 THEN TRUE ELSE ` + calendarACLBooleanExpr("$1", "read-free-busy", "read", "all") + ` END as can_read_free_busy,
       CASE WHEN c.user_id = $1 THEN TRUE ELSE ` + calendarACLBooleanExpr("$1", "write", "all") + ` END as can_write,
       CASE WHEN c.user_id = $1 THEN TRUE ELSE ` + calendarACLBooleanExpr("$1", "write-content", "write", "all") + ` END as can_write_content,
       CASE WHEN c.user_id = $1 THEN TRUE ELSE ` + calendarACLBooleanExpr("$1", "write-properties", "write", "all") + ` END as can_write_properties,
		CASE WHEN c.user_id = $1 THEN TRUE ELSE ` + calendarACLBooleanExpr("$1", "bind", "write", "all") + ` END as can_bind,
		CASE WHEN c.user_id = $1 THEN TRUE ELSE ` + calendarACLBooleanExpr("$1", "unbind", "write", "all") + ` END as can_unbind
FROM calendars c
JOIN users u ON u.id = c.user_id
WHERE c.user_id = $1
   OR (
       c.user_id <> $1
       AND (` + calendarACLAnyAccessExpr("$1") + `
            OR ` + calendarObjectACLAnyAccessExpr("$1") + `)
   )
ORDER BY shared, name
`
	defer observeDB(ctx, "calendars.list_accessible")()
	rows, err := r.pool.QueryContext(ctx, q, userID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var result []CalendarAccess
	for rows.Next() {
		var c CalendarAccess
		var slug, description, descriptionLang, timezone, color sql.NullString
		var components pq.StringArray
		if err := rows.Scan(
			&c.ID, &c.UserID, &c.Name, &slug, &description, &descriptionLang, &timezone, &color, &components, &c.CTag, &c.CreatedAt, &c.UpdatedAt, &c.OwnerEmail, &c.Shared,
			&c.Privileges.Read, &c.Privileges.ReadFreeBusy, &c.Privileges.Write, &c.Privileges.WriteContent, &c.Privileges.WriteProperties, &c.Privileges.Bind, &c.Privileges.Unbind,
		); err != nil {
			return nil, err
		}
		c.Slug = nullableString(slug)
		c.Description = nullableString(description)
		c.DescriptionLang = nullableString(descriptionLang)
		c.Timezone = nullableString(timezone)
		c.Color = nullableString(color)
		c.SupportedComponents = components
		c.PrivilegesResolved = true
		c.Privileges = c.Privileges.Normalized()
		c.Editor = c.Privileges.AllowsEventEditing()
		result = append(result, c)
	}
	return result, rows.Err()
}

func (r *calendarRepo) GetAccessible(ctx context.Context, calendarID, userID int64) (*CalendarAccess, error) {
	q := `
SELECT c.id, c.user_id, c.name, c.slug, c.description, c.description_lang, c.timezone, c.color, c.supported_components, c.ctag, c.created_at, c.updated_at,
       u.primary_email as owner_email,
       CASE WHEN c.user_id = $2 THEN FALSE ELSE TRUE END as shared,
       CASE WHEN c.user_id = $2 THEN TRUE ELSE ` + calendarACLBooleanExpr("$2", "read", "all") + ` END as can_read,
       CASE WHEN c.user_id = $2 THEN TRUE ELSE ` + calendarACLBooleanExpr("$2", "read-free-busy", "read", "all") + ` END as can_read_free_busy,
       CASE WHEN c.user_id = $2 THEN TRUE ELSE ` + calendarACLBooleanExpr("$2", "write", "all") + ` END as can_write,
       CASE WHEN c.user_id = $2 THEN TRUE ELSE ` + calendarACLBooleanExpr("$2", "write-content", "write", "all") + ` END as can_write_content,
       CASE WHEN c.user_id = $2 THEN TRUE ELSE ` + calendarACLBooleanExpr("$2", "write-properties", "write", "all") + ` END as can_write_properties,
		CASE WHEN c.user_id = $2 THEN TRUE ELSE ` + calendarACLBooleanExpr("$2", "bind", "write", "all") + ` END as can_bind,
		CASE WHEN c.user_id = $2 THEN TRUE ELSE ` + calendarACLBooleanExpr("$2", "unbind", "write", "all") + ` END as can_unbind
FROM calendars c
JOIN users u ON u.id = c.user_id
WHERE c.id = $1
  AND (
      c.user_id = $2
      OR (
          c.user_id <> $2
          AND (` + calendarACLAnyAccessExpr("$2") + `
               OR ` + calendarObjectACLAnyAccessExpr("$2") + `)
      )
  )
`
	defer observeDB(ctx, "calendars.get_accessible")()
	var c CalendarAccess
	var slug, description, descriptionLang, timezone, color sql.NullString
	var components pq.StringArray
	if err := r.pool.QueryRowContext(ctx, q, calendarID, userID).Scan(
		&c.ID, &c.UserID, &c.Name, &slug, &description, &descriptionLang, &timezone, &color, &components, &c.CTag, &c.CreatedAt, &c.UpdatedAt, &c.OwnerEmail, &c.Shared,
		&c.Privileges.Read, &c.Privileges.ReadFreeBusy, &c.Privileges.Write, &c.Privileges.WriteContent, &c.Privileges.WriteProperties, &c.Privileges.Bind, &c.Privileges.Unbind,
	); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, nil
		}
		return nil, err
	}
	c.Slug = nullableString(slug)
	c.Description = nullableString(description)
	c.DescriptionLang = nullableString(descriptionLang)
	c.Timezone = nullableString(timezone)
	c.Color = nullableString(color)
	c.SupportedComponents = components
	c.PrivilegesResolved = true
	c.Privileges = c.Privileges.Normalized()
	c.Editor = c.Privileges.AllowsEventEditing()
	return &c, nil
}

const createCalendarQuery = `INSERT INTO calendars (user_id, name, slug, description, description_lang, timezone, color, supported_components) VALUES ($1, $2, $3, $4, $5, $6, $7, $8) RETURNING id, user_id, name, slug, description, description_lang, timezone, color, supported_components, ctag, created_at, updated_at`

func (r *calendarRepo) Create(ctx context.Context, cal Calendar) (*Calendar, error) {
	defer observeDB(ctx, "calendars.create")()
	return createCalendarTx(ctx, r.pool, cal)
}

func createCalendarTx(ctx context.Context, tx queryExecContext, cal Calendar) (*Calendar, error) {
	row := tx.QueryRowContext(ctx, createCalendarQuery, cal.UserID, cal.Name, cal.Slug, cal.Description, cal.DescriptionLang, cal.Timezone, cal.Color, nullableStringArray(cal.SupportedComponents))
	var created Calendar
	var slug, description, descriptionLang, timezone, color sql.NullString
	var components pq.StringArray
	if err := row.Scan(&created.ID, &created.UserID, &created.Name, &slug, &description, &descriptionLang, &timezone, &color, &components, &created.CTag, &created.CreatedAt, &created.UpdatedAt); err != nil {
		return nil, err
	}
	created.Slug = nullableString(slug)
	created.Description = nullableString(description)
	created.DescriptionLang = nullableString(descriptionLang)
	created.Timezone = nullableString(timezone)
	created.Color = nullableString(color)
	created.SupportedComponents = components
	return &created, nil
}

func (r *calendarRepo) Update(ctx context.Context, userID, id int64, name string, description, timezone, color *string) error {
	const q = `UPDATE calendars SET name=$1, description=$2, timezone=$3, color=$4, updated_at=NOW() WHERE id=$5 AND user_id=$6`
	defer observeDB(ctx, "calendars.update")()
	res, err := r.pool.ExecContext(ctx, q, name, description, timezone, color, id, userID)
	if err != nil {
		return err
	}
	rows, err := res.RowsAffected()
	if err != nil {
		return err
	}
	if rows == 0 {
		return ErrNotFound
	}
	return nil
}

func (r *calendarRepo) UpdateProperties(ctx context.Context, id int64, props CalendarProperties) error {
	const q = `UPDATE calendars SET name=$1, description=$2, description_lang=$3, timezone=$4, color=$5, updated_at=NOW() WHERE id=$6`
	defer observeDB(ctx, "calendars.update_properties")()
	res, err := r.pool.ExecContext(ctx, q, props.Name, props.Description, props.DescriptionLang, props.Timezone, props.Color, id)
	if err != nil {
		return err
	}
	rows, err := res.RowsAffected()
	if err != nil {
		return err
	}
	if rows == 0 {
		return ErrNotFound
	}
	return nil
}

func (r *calendarRepo) Rename(ctx context.Context, userID, id int64, name string) error {
	const q = `UPDATE calendars SET name=$1, updated_at=NOW() WHERE id=$2 AND user_id=$3`
	defer observeDB(ctx, "calendars.rename")()
	res, err := r.pool.ExecContext(ctx, q, name, id, userID)
	if err != nil {
		return err
	}
	rows, err := res.RowsAffected()
	if err != nil {
		return err
	}
	if rows == 0 {
		return ErrNotFound
	}
	return nil
}

func (r *calendarRepo) Delete(ctx context.Context, userID, id int64) error {
	const q = `DELETE FROM calendars WHERE id=$1 AND user_id=$2`
	defer observeDB(ctx, "calendars.delete")()
	res, err := r.pool.ExecContext(ctx, q, id, userID)
	if err != nil {
		return err
	}
	rows, err := res.RowsAffected()
	if err != nil {
		return err
	}
	if rows == 0 {
		return ErrNotFound
	}
	return nil
}

// eventRepo implements EventRepository.
type eventRepo struct {
	pool *sql.DB
}

// lockRepositoryCollectionsTx holds the collections a member write that does
// not come through DAV changes, the way a DAV write holds them: each
// collection's DAV path, then its row. The sync-stamp triggers read the
// collection when a member row is written, so the write has to wait out every
// other writer to the collection before it writes anything. A collection that
// does not exist is ErrNotFound.
func lockRepositoryCollectionsTx(ctx context.Context, tx *sql.Tx, table string, ids ...int64) error {
	lockPath := calendarCollectionLockPath
	if table == "address_books" {
		lockPath = addressBookCollectionLockPath
	}
	paths := make([]string, 0, len(ids))
	for _, id := range ids {
		paths = append(paths, lockPath(id))
	}
	if err := acquireDAVPathLocks(ctx, tx, paths...); err != nil {
		return err
	}
	if err := lockCollectionRowsTx(ctx, tx, table, ids...); err != nil {
		if errors.Is(err, ErrResourceStateChanged) {
			return ErrNotFound
		}
		return err
	}
	return nil
}

func (r *eventRepo) Upsert(ctx context.Context, event Event) (*Event, error) {
	var metadata EventWriteMetadata
	if event.WriteMetadata != nil {
		metadata = *event.WriteMetadata
	} else {
		metadata.Summary, metadata.Description, metadata.Location, metadata.DTStart, metadata.DTEnd, metadata.AllDay = parseICalFields(event.RawICAL)
		metadata.RecurrenceStart, metadata.RecurrenceUntil = recurrenceBoundsFromICal(event.RawICAL)
	}
	if event.ResourceName == "" {
		event.ResourceName = event.UID
	}

	const q = `
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
`
	defer observeDB(ctx, "events.upsert")()
	tx, err := r.pool.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()
	if err := lockRepositoryCollectionsTx(ctx, tx, "calendars", event.CalendarID); err != nil {
		return nil, err
	}
	row := tx.QueryRowContext(ctx, q, event.CalendarID, event.UID, event.ResourceName, event.RawICAL, event.ETag, metadata.Summary, metadata.Description, metadata.Location, metadata.DTStart, metadata.DTEnd, metadata.AllDay, metadata.RecurrenceStart, metadata.RecurrenceUntil)
	ev, err := scanEvent(row.Scan)
	if err != nil {
		if isEventResourceNameConflict(err) {
			return nil, ErrConflict
		}
		return nil, err
	}
	if err := tx.Commit(); err != nil {
		return nil, err
	}
	return &ev, nil
}

func (r *eventRepo) DeleteByUID(ctx context.Context, calendarID int64, uid string) error {
	const q = `DELETE FROM events WHERE calendar_id=$1 AND uid=$2`
	defer observeDB(ctx, "events.delete_by_uid")()
	tx, err := r.pool.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if err := lockRepositoryCollectionsTx(ctx, tx, "calendars", calendarID); err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, q, calendarID, uid); err != nil {
		return err
	}
	return tx.Commit()
}

func (r *eventRepo) GetByUID(ctx context.Context, calendarID int64, uid string) (*Event, error) {
	const q = `SELECT id, calendar_id, uid, resource_name, raw_ical, etag, summary, description, location, dtstart, dtend, all_day, last_modified FROM events WHERE calendar_id=$1 AND uid=$2`
	defer observeDB(ctx, "events.get_by_uid")()
	row := r.pool.QueryRowContext(ctx, q, calendarID, uid)
	ev, err := scanEvent(row.Scan)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, nil
		}
		return nil, err
	}
	return &ev, nil
}

func (r *eventRepo) GetByResourceName(ctx context.Context, calendarID int64, resourceName string) (*Event, error) {
	const q = `SELECT id, calendar_id, uid, resource_name, raw_ical, etag, summary, description, location, dtstart, dtend, all_day, last_modified FROM events WHERE calendar_id=$1 AND resource_name=$2`
	defer observeDB(ctx, "events.get_by_resource_name")()
	row := r.pool.QueryRowContext(ctx, q, calendarID, resourceName)
	ev, err := scanEvent(row.Scan)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, nil
		}
		return nil, err
	}
	return &ev, nil
}

func (r *eventRepo) ListByResourceNames(ctx context.Context, calendarID int64, resourceNames []string) ([]Event, error) {
	if len(resourceNames) == 0 {
		return []Event{}, nil
	}
	const q = `SELECT id, calendar_id, uid, resource_name, raw_ical, etag, summary, description, location, dtstart, dtend, all_day, last_modified FROM events WHERE calendar_id=$1 AND resource_name = ANY($2)`
	defer observeDB(ctx, "events.list_by_resource_names")()
	rows, err := r.pool.QueryContext(ctx, q, calendarID, pq.Array(resourceNames))
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var result []Event
	for rows.Next() {
		ev, err := scanEvent(rows.Scan)
		if err != nil {
			return nil, err
		}
		result = append(result, ev)
	}
	return result, rows.Err()
}

func (r *eventRepo) ListByUIDs(ctx context.Context, calendarID int64, uids []string) ([]Event, error) {
	if len(uids) == 0 {
		return []Event{}, nil
	}
	const q = `SELECT id, calendar_id, uid, resource_name, raw_ical, etag, summary, description, location, dtstart, dtend, all_day, last_modified FROM events WHERE calendar_id=$1 AND uid = ANY($2)`
	defer observeDB(ctx, "events.list_by_uids")()
	rows, err := r.pool.QueryContext(ctx, q, calendarID, pq.Array(uids))
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var result []Event
	for rows.Next() {
		ev, err := scanEvent(rows.Scan)
		if err != nil {
			return nil, err
		}
		result = append(result, ev)
	}
	return result, rows.Err()
}

func (r *eventRepo) ListForCalendar(ctx context.Context, calendarID int64) ([]Event, error) {
	const q = `SELECT id, calendar_id, uid, resource_name, raw_ical, etag, summary, description, location, dtstart, dtend, all_day, last_modified FROM events WHERE calendar_id=$1 ORDER BY last_modified DESC`
	defer observeDB(ctx, "events.list_for_calendar")()
	rows, err := r.pool.QueryContext(ctx, q, calendarID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var result []Event
	for rows.Next() {
		ev, err := scanEvent(rows.Scan)
		if err != nil {
			return nil, err
		}
		result = append(result, ev)
	}
	return result, rows.Err()
}

// eventColumns is the canonical select list shared by event queries.
const eventColumns = `id, calendar_id, uid, resource_name, raw_ical, etag, summary, description, location, dtstart, dtend, all_day, last_modified`

// likeEscape escapes characters with special meaning in a LIKE/ILIKE pattern so
// user-supplied search text is matched literally (using the default '\' escape).
func likeEscape(s string) string {
	return strings.NewReplacer(`\`, `\\`, `%`, `\%`, `_`, `\_`).Replace(s)
}

// ListForCalendarFiltered returns calendar events matching f. Every query is
// scoped to a single calendar_id, so it never triggers a full table scan; date
// ranges use recurrence window indexes and text predicates run over that
// narrowed set.
func (r *eventRepo) ListForCalendarFiltered(ctx context.Context, calendarID int64, f EventFilter) ([]Event, error) {
	var sb strings.Builder
	sb.WriteString(`SELECT ` + eventColumns + ` FROM events WHERE calendar_id=$1`)
	args := []any{calendarID}
	placeholder := func(v any) string {
		args = append(args, v)
		return "$" + strconv.Itoa(len(args))
	}

	if f.Start != nil {
		// Keep events whose last instance ends at or after Start, which needs an
		// upper bound on the interval the object occupies. recurrence_until holds
		// that end for every recurring object (a far-future sentinel when it
		// cannot be computed), and dtend holds it for a non-recurring VEVENT that
		// carries a literal DTEND.
		//
		// Nothing else does, so everything else falls to 'infinity' and stays a
		// candidate for the in-memory RFC 4791 §9.9 pass to judge. dtstart is
		// deliberately not in this list: an object's start is no bound on where
		// it ends, so reading it here drops a VEVENT written with DURATION or
		// with a DATE-valued DTSTART -- both occupy time past dtstart that no
		// column records -- along with every VTODO, VJOURNAL and VFREEBUSY,
		// which populate neither column at all.
		sb.WriteString(` AND COALESCE(recurrence_until, dtend, 'infinity'::timestamptz) >= `)
		sb.WriteString(placeholder(f.Start.UTC()))
	}
	if f.End != nil {
		// Keep events whose earliest instance starts at or before End. recurrence_start
		// captures RDATEs and moved overrides that can occur before the master dtstart.
		// '-infinity' keeps an unbounded row for the same reason as above.
		sb.WriteString(` AND COALESCE(recurrence_start, dtstart, '-infinity'::timestamptz) <= `)
		sb.WriteString(placeholder(f.End.UTC()))
	}
	if f.Title != "" {
		sb.WriteString(` AND summary ILIKE `)
		sb.WriteString(placeholder("%" + likeEscape(f.Title) + "%"))
	}
	if f.Description != "" {
		sb.WriteString(` AND description ILIKE `)
		sb.WriteString(placeholder("%" + likeEscape(f.Description) + "%"))
	}
	if f.Location != "" {
		sb.WriteString(` AND location ILIKE `)
		sb.WriteString(placeholder("%" + likeEscape(f.Location) + "%"))
	}
	if f.Query != "" {
		p := placeholder("%" + likeEscape(f.Query) + "%")
		sb.WriteString(` AND (summary ILIKE `)
		sb.WriteString(p)
		sb.WriteString(` OR description ILIKE `)
		sb.WriteString(p)
		sb.WriteString(` OR location ILIKE `)
		sb.WriteString(p)
		sb.WriteString(`)`)
	}
	sb.WriteString(` ORDER BY dtstart ASC NULLS LAST, last_modified DESC`)
	if f.Limit > 0 {
		sb.WriteString(` LIMIT `)
		sb.WriteString(placeholder(f.Limit))
	}
	if f.Offset > 0 {
		sb.WriteString(` OFFSET `)
		sb.WriteString(placeholder(f.Offset))
	}

	defer observeDB(ctx, "events.list_for_calendar_filtered")()
	rows, err := r.pool.QueryContext(ctx, sb.String(), args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var result []Event
	for rows.Next() {
		ev, err := scanEvent(rows.Scan)
		if err != nil {
			return nil, err
		}
		result = append(result, ev)
	}
	return result, rows.Err()
}

func (r *eventRepo) ListForCalendarPageAfter(ctx context.Context, calendarID, afterID int64, limit int, f EventFilter) ([]Event, error) {
	if limit <= 0 {
		return []Event{}, nil
	}
	// The cursor is a row comparison rather than `id>$2`, which is what keeps
	// this page on idx_events_calendar_keyset. Spelled the plain way,
	// events_pkey answers the ORDER BY as well, and the planner costs a pkey
	// scan by assuming the calendar's rows are spread evenly through the id
	// range -- wrong in the expensive direction for a calendar holding a large
	// fraction of the table, where the scan discards every row belonging to
	// another calendar. A row comparison is not an index bound the primary key
	// can use at all.
	//
	// The equality beside it is what scopes the read to one calendar and may not
	// be dropped as a duplicate of the row comparison: `(calendar_id, id) >
	// ($1, $2)` also admits every row of every calendar whose id is higher,
	// which is another user's collection.
	//
	// The two together are one index bound, so the equality costs nothing.
	var sb strings.Builder
	sb.WriteString(`SELECT ` + eventColumns + ` FROM events WHERE calendar_id=$1 AND (calendar_id, id) > ($1, $2)`)
	args := []any{calendarID, afterID}
	placeholder := func(v any) string {
		args = append(args, v)
		return "$" + strconv.Itoa(len(args))
	}
	if f.Start != nil {
		sb.WriteString(` AND COALESCE(recurrence_until, dtend, 'infinity'::timestamptz) >= `)
		sb.WriteString(placeholder(f.Start.UTC()))
	}
	if f.End != nil {
		sb.WriteString(` AND COALESCE(recurrence_start, dtstart, '-infinity'::timestamptz) <= `)
		sb.WriteString(placeholder(f.End.UTC()))
	}
	if f.Title != "" {
		sb.WriteString(` AND summary ILIKE `)
		sb.WriteString(placeholder("%" + likeEscape(f.Title) + "%"))
	}
	if f.Description != "" {
		sb.WriteString(` AND description ILIKE `)
		sb.WriteString(placeholder("%" + likeEscape(f.Description) + "%"))
	}
	if f.Location != "" {
		sb.WriteString(` AND location ILIKE `)
		sb.WriteString(placeholder("%" + likeEscape(f.Location) + "%"))
	}
	if f.Query != "" {
		p := placeholder("%" + likeEscape(f.Query) + "%")
		sb.WriteString(` AND (summary ILIKE `)
		sb.WriteString(p)
		sb.WriteString(` OR description ILIKE `)
		sb.WriteString(p)
		sb.WriteString(` OR location ILIKE `)
		sb.WriteString(p)
		sb.WriteString(`)`)
	}
	// The sort key names the leading column so the index provides the order.
	// With calendar_id fixed by the equality the two spellings select the same
	// rows in the same order.
	sb.WriteString(` ORDER BY calendar_id ASC, id ASC LIMIT `)
	sb.WriteString(placeholder(limit))

	defer observeDB(ctx, "events.list_for_calendar_page_after")()
	rows, err := r.pool.QueryContext(ctx, sb.String(), args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var result []Event
	for rows.Next() {
		event, err := scanEvent(rows.Scan)
		if err != nil {
			return nil, err
		}
		result = append(result, event)
	}
	return result, rows.Err()
}

func (r *eventRepo) ListForCalendarPaginated(ctx context.Context, calendarID int64, limit, offset int) (*PaginatedResult[Event], error) {
	defer observeDB(ctx, "events.list_for_calendar_paginated")()

	// Get total count
	var totalCount int
	countQ := `SELECT COUNT(*) FROM events WHERE calendar_id=$1`
	if err := r.pool.QueryRowContext(ctx, countQ, calendarID).Scan(&totalCount); err != nil {
		return nil, err
	}

	const q = `SELECT id, calendar_id, uid, resource_name, raw_ical, etag, summary, description, location, dtstart, dtend, all_day, last_modified FROM events WHERE calendar_id=$1 ORDER BY last_modified DESC LIMIT $2 OFFSET $3`
	rows, err := r.pool.QueryContext(ctx, q, calendarID, limit, offset)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var items []Event
	for rows.Next() {
		ev, err := scanEvent(rows.Scan)
		if err != nil {
			return nil, err
		}
		items = append(items, ev)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	return &PaginatedResult[Event]{
		Items:      items,
		TotalCount: totalCount,
		Limit:      limit,
		Offset:     offset,
	}, nil
}

// ListModifiedSincePageAfter returns one keyset page of the calendar's rows
// modified after since, ordered by id so a caller can resume from the last id
// it saw. The sync report that reads it bounds how many pages it will take.
func (r *eventRepo) ListModifiedSincePageAfter(ctx context.Context, calendarID, afterID int64, since time.Time, limit int) ([]Event, error) {
	if limit <= 0 {
		return []Event{}, nil
	}
	// The collection equality scopes the read and the row comparison pins the
	// keyset index; ListForCalendarPageAfter carries why neither may be dropped.
	const q = `SELECT ` + eventColumns + ` FROM events WHERE calendar_id=$1 AND (calendar_id, id) > ($1, $2) AND last_modified > $3 ORDER BY calendar_id ASC, id ASC LIMIT $4`
	defer observeDB(ctx, "events.list_modified_since_page_after")()
	rows, err := r.pool.QueryContext(ctx, q, calendarID, afterID, since, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var result []Event
	for rows.Next() {
		ev, err := scanEvent(rows.Scan)
		if err != nil {
			return nil, err
		}
		result = append(result, ev)
	}
	return result, rows.Err()
}

func (r *eventRepo) ListRecentByUser(ctx context.Context, userID int64, limit int) ([]Event, error) {
	q := `
SELECT e.id, e.calendar_id, e.uid, e.resource_name, e.raw_ical, e.etag, e.summary, e.description, e.location, e.dtstart, e.dtend, e.all_day, e.last_modified
FROM events e
JOIN calendars c ON c.id = e.calendar_id
WHERE c.user_id = $1
   OR (
       c.user_id <> $1
       AND ` + calendarEventACLAllowsWithCollectionFallbackExpr("$1", "read", "all") + `
   )
ORDER BY e.last_modified DESC
LIMIT $2
`
	defer observeDB(ctx, "events.list_recent_by_user")()
	rows, err := r.pool.QueryContext(ctx, q, userID, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var result []Event
	for rows.Next() {
		ev, err := scanEvent(rows.Scan)
		if err != nil {
			return nil, err
		}
		result = append(result, ev)
	}
	return result, rows.Err()
}

func (r *eventRepo) MaxLastModified(ctx context.Context, calendarID int64) (time.Time, error) {
	const q = `SELECT COALESCE(MAX(last_modified), '1970-01-01T00:00:00Z') FROM events WHERE calendar_id=$1`
	defer observeDB(ctx, "events.max_last_modified")()
	var ts time.Time
	if err := r.pool.QueryRowContext(ctx, q, calendarID).Scan(&ts); err != nil {
		return time.Time{}, err
	}
	return ts.UTC(), nil
}

func (r *eventRepo) MoveToCalendar(ctx context.Context, fromCalendarID, toCalendarID int64, uid, destResourceName string) error {
	defer observeDB(ctx, "events.move_to_calendar")()

	tx, err := r.pool.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if err := lockRepositoryCollectionsTx(ctx, tx, "calendars", fromCalendarID, toCalendarID); err != nil {
		return err
	}

	if err := moveEventTx(ctx, tx, fromCalendarID, toCalendarID, uid, destResourceName); err != nil {
		return err
	}
	return tx.Commit()
}

func moveEventTx(ctx context.Context, tx queryExecContext, fromCalendarID, toCalendarID int64, uid, destResourceName string) error {
	if destResourceName == "" {
		destResourceName = uid
	}

	const selectQ = `SELECT resource_name FROM events WHERE calendar_id=$1 AND uid=$2`
	var sourceResourceName string
	if err := tx.QueryRowContext(ctx, selectQ, fromCalendarID, uid).Scan(&sourceResourceName); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return ErrNotFound
		}
		return err
	}
	if fromCalendarID != toCalendarID {
		const existingDestQ = `SELECT resource_name FROM events WHERE calendar_id=$1 AND uid=$2`
		var existingDestResourceName string
		switch err := tx.QueryRowContext(ctx, existingDestQ, toCalendarID, uid).Scan(&existingDestResourceName); {
		case err == nil:
			if existingDestResourceName != "" && existingDestResourceName != destResourceName {
				return ErrConflict
			}
		case errors.Is(err, sql.ErrNoRows):
		default:
			return err
		}
	}

	const deleteDestByNameQ = `DELETE FROM events WHERE calendar_id=$1 AND resource_name=$2 AND uid<>$3`
	if _, err := tx.ExecContext(ctx, deleteDestByNameQ, toCalendarID, destResourceName, uid); err != nil {
		return err
	}
	if fromCalendarID != toCalendarID {
		const deleteDestByUIDQ = `DELETE FROM events WHERE calendar_id=$1 AND uid=$2`
		if _, err := tx.ExecContext(ctx, deleteDestByUIDQ, toCalendarID, uid); err != nil {
			return err
		}
	}

	const moveQuery = `UPDATE events SET calendar_id=$1, resource_name=$2, last_modified=NOW() WHERE calendar_id=$3 AND uid=$4`
	result, err := tx.ExecContext(ctx, moveQuery, toCalendarID, destResourceName, fromCalendarID, uid)
	if err != nil {
		return err
	}
	rows, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if rows == 0 {
		return ErrNotFound
	}

	if fromCalendarID == toCalendarID {
		if sourceResourceName != destResourceName {
			const tombstoneQuery = `INSERT INTO deleted_resources (resource_type, collection_id, uid, resource_name) VALUES ('event', $1, $2, $3)`
			if _, err := tx.ExecContext(ctx, tombstoneQuery, fromCalendarID, uid, sourceResourceName); err != nil {
				return err
			}
		}
		return nil
	}

	const tombstoneQuery = `INSERT INTO deleted_resources (resource_type, collection_id, uid, resource_name) VALUES ('event', $1, $2, $3)`
	if _, err := tx.ExecContext(ctx, tombstoneQuery, fromCalendarID, uid, sourceResourceName); err != nil {
		return err
	}

	const incrementCtagQuery = `UPDATE calendars SET ctag = ctag + 1, updated_at = NOW() WHERE id = $1`
	if _, err := tx.ExecContext(ctx, incrementCtagQuery, fromCalendarID); err != nil {
		return err
	}

	return nil
}

func (r *eventRepo) CopyToCalendar(ctx context.Context, fromCalendarID, toCalendarID int64, uid, destResourceName, newETag string) (*Event, error) {
	defer observeDB(ctx, "events.copy_to_calendar")()

	tx, err := r.pool.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()
	if err := lockRepositoryCollectionsTx(ctx, tx, "calendars", fromCalendarID, toCalendarID); err != nil {
		return nil, err
	}

	const selectQ = `SELECT id, calendar_id, uid, resource_name, raw_ical, etag, summary, description, location, dtstart, dtend, all_day, last_modified FROM events WHERE calendar_id=$1 AND uid=$2`
	row := tx.QueryRowContext(ctx, selectQ, fromCalendarID, uid)
	src, err := scanEvent(row.Scan)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, ErrNotFound
		}
		return nil, err
	}

	if destResourceName == "" {
		destResourceName = src.ResourceName
		if destResourceName == "" {
			destResourceName = src.UID
		}
	}

	const existingDestQ = `SELECT resource_name FROM events WHERE calendar_id=$1 AND uid=$2`
	var existingDestResourceName string
	switch err := tx.QueryRowContext(ctx, existingDestQ, toCalendarID, src.UID).Scan(&existingDestResourceName); {
	case err == nil:
		if existingDestResourceName != "" && existingDestResourceName != destResourceName {
			return nil, ErrConflict
		}
	case errors.Is(err, sql.ErrNoRows):
		existingDestResourceName = ""
	default:
		return nil, err
	}

	const deleteDestByNameQ = `DELETE FROM events WHERE calendar_id=$1 AND resource_name=$2 AND uid<>$3`
	if _, err := tx.ExecContext(ctx, deleteDestByNameQ, toCalendarID, destResourceName, src.UID); err != nil {
		return nil, err
	}
	if existingDestResourceName != "" && existingDestResourceName != destResourceName {
		const tombstoneQuery = `INSERT INTO deleted_resources (resource_type, collection_id, uid, resource_name) VALUES ('event', $1, $2, $3)`
		if _, err := tx.ExecContext(ctx, tombstoneQuery, toCalendarID, src.UID, existingDestResourceName); err != nil {
			return nil, err
		}
	}

	const insertQ = `
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
`
	recurrenceStart, recurrenceUntil := recurrenceBoundsFromICal(src.RawICAL)
	insertRow := tx.QueryRowContext(ctx, insertQ, toCalendarID, src.UID, destResourceName, src.RawICAL, newETag, src.Summary, src.Description, src.Location, src.DTStart, src.DTEnd, src.AllDay, recurrenceStart, recurrenceUntil)
	ev, err := scanEvent(insertRow.Scan)
	if err != nil {
		return nil, err
	}

	if err := tx.Commit(); err != nil {
		return nil, err
	}
	return &ev, nil
}

// addressBookRepo implements AddressBookRepository.
type addressBookRepo struct {
	pool *sql.DB
}

// IsDataError reports whether err is PostgreSQL refusing a value it cannot
// store (SQLSTATE class 22) or one past a limit such as the index entry size
// (class 54), as opposed to the database being unavailable.
func IsDataError(err error) bool {
	var pqErr *pq.Error
	return errors.As(err, &pqErr) && (pqErr.Code.Class() == "22" || pqErr.Code.Class() == "54")
}

// isMissingCollection reports whether err is a member insert failing the
// named foreign key to its collection, which means the collection was removed.
func isMissingCollection(err error, constraint string) bool {
	var pqErr *pq.Error
	return errors.As(err, &pqErr) && pqErr.Code == "23503" && pqErr.Constraint == constraint
}

func isAddressBookNameConflict(err error) bool {
	var pqErr *pq.Error
	return errors.As(err, &pqErr) && pqErr.Code == "23505" && pqErr.Constraint == "idx_address_books_user_name_lower"
}

func isContactResourceNameConflict(err error) bool {
	var pqErr *pq.Error
	return errors.As(err, &pqErr) && pqErr.Code == "23505" && pqErr.Constraint == "idx_contacts_resource_name"
}

func isContactIdentityConflict(err error) bool {
	var pqErr *pq.Error
	if !errors.As(err, &pqErr) || pqErr.Code != "23505" {
		return false
	}
	return pqErr.Constraint == "idx_contacts_resource_name" || pqErr.Constraint == "contacts_address_book_id_uid_key"
}

func isEventResourceNameConflict(err error) bool {
	var pqErr *pq.Error
	return errors.As(err, &pqErr) && pqErr.Code == "23505" && pqErr.Constraint == "events_calendar_resource_name_unique"
}

func isEventIdentityConflict(err error) bool {
	var pqErr *pq.Error
	if !errors.As(err, &pqErr) || pqErr.Code != "23505" {
		return false
	}
	return pqErr.Constraint == "events_calendar_resource_name_unique" ||
		pqErr.Constraint == "events_calendar_id_uid_key"
}

func (r *addressBookRepo) ListByUser(ctx context.Context, userID int64) ([]AddressBook, error) {
	const q = `SELECT id, user_id, name, description, ctag, created_at, updated_at FROM address_books WHERE user_id=$1 ORDER BY created_at`
	defer observeDB(ctx, "address_books.list_by_user")()
	rows, err := r.pool.QueryContext(ctx, q, userID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var result []AddressBook
	for rows.Next() {
		var book AddressBook
		var description sql.NullString
		if err := rows.Scan(&book.ID, &book.UserID, &book.Name, &description, &book.CTag, &book.CreatedAt, &book.UpdatedAt); err != nil {
			return nil, err
		}
		book.Description = nullableString(description)
		result = append(result, book)
	}
	return result, rows.Err()
}

func (r *addressBookRepo) ListAccessible(ctx context.Context, userID int64) ([]AddressBook, error) {
	q := `
SELECT b.id, b.user_id, b.name, b.description, b.ctag, b.created_at, b.updated_at
FROM address_books b
WHERE b.user_id=$1
   OR (
       b.user_id<>$1
       AND (
           ` + addressBookACLBooleanExpr("$1", "read", "all") + `
           OR EXISTS (
               SELECT 1
               FROM acl_entries g0
               JOIN contacts c ON c.address_book_id=b.id AND c.object_acl_path=g0.resource_path_norm
               WHERE g0.principal_href IN ('DAV:all', 'DAV:authenticated', '/dav/principals/' || $1::text || '/')
                 AND g0.is_grant=TRUE
                 AND g0.privilege IN ('read', 'all')
                 AND ` + contactACLBooleanExpr("$1", "read", "all") + `
           )
       )
   )
ORDER BY (b.user_id<>$1), b.name`
	defer observeDB(ctx, "address_books.list_accessible")()
	rows, err := r.pool.QueryContext(ctx, q, userID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var result []AddressBook
	for rows.Next() {
		var book AddressBook
		var description sql.NullString
		if err := rows.Scan(&book.ID, &book.UserID, &book.Name, &description, &book.CTag, &book.CreatedAt, &book.UpdatedAt); err != nil {
			return nil, err
		}
		book.Description = nullableString(description)
		result = append(result, book)
	}
	return result, rows.Err()
}

func (r *addressBookRepo) GetByID(ctx context.Context, id int64) (*AddressBook, error) {
	const q = `SELECT id, user_id, name, description, ctag, created_at, updated_at FROM address_books WHERE id=$1`
	defer observeDB(ctx, "address_books.get_by_id")()
	var book AddressBook
	var description sql.NullString
	if err := r.pool.QueryRowContext(ctx, q, id).Scan(&book.ID, &book.UserID, &book.Name, &description, &book.CTag, &book.CreatedAt, &book.UpdatedAt); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, nil
		}
		return nil, err
	}
	book.Description = nullableString(description)
	return &book, nil
}

func (r *addressBookRepo) Create(ctx context.Context, book AddressBook) (*AddressBook, error) {
	const q = `INSERT INTO address_books (user_id, name, description) VALUES ($1, $2, $3) RETURNING id, user_id, name, description, ctag, created_at, updated_at`
	defer observeDB(ctx, "address_books.create")()
	row := r.pool.QueryRowContext(ctx, q, book.UserID, book.Name, book.Description)
	var created AddressBook
	var description sql.NullString
	if err := row.Scan(&created.ID, &created.UserID, &created.Name, &description, &created.CTag, &created.CreatedAt, &created.UpdatedAt); err != nil {
		if isAddressBookNameConflict(err) {
			return nil, ErrConflict
		}
		return nil, err
	}
	created.Description = nullableString(description)
	return &created, nil
}

func (r *addressBookRepo) Update(ctx context.Context, userID, id int64, name string, description *string) error {
	const q = `UPDATE address_books SET name=$1, description=$2, updated_at=NOW() WHERE id=$3 AND user_id=$4`
	defer observeDB(ctx, "address_books.update")()
	res, err := r.pool.ExecContext(ctx, q, name, description, id, userID)
	if err != nil {
		if isAddressBookNameConflict(err) {
			return ErrConflict
		}
		return err
	}
	rows, err := res.RowsAffected()
	if err != nil {
		return err
	}
	if rows == 0 {
		return ErrNotFound
	}
	return nil
}

func (r *addressBookRepo) UpdateProperties(ctx context.Context, id int64, name string, description *string) error {
	const q = `UPDATE address_books SET name=$1, description=$2, updated_at=NOW() WHERE id=$3`
	defer observeDB(ctx, "address_books.update_properties")()
	res, err := r.pool.ExecContext(ctx, q, name, description, id)
	if err != nil {
		if isAddressBookNameConflict(err) {
			return ErrConflict
		}
		return err
	}
	rows, err := res.RowsAffected()
	if err != nil {
		return err
	}
	if rows == 0 {
		return ErrNotFound
	}
	return nil
}

func (r *addressBookRepo) Rename(ctx context.Context, userID, id int64, name string) error {
	const q = `UPDATE address_books SET name=$1, updated_at=NOW() WHERE id=$2 AND user_id=$3`
	defer observeDB(ctx, "address_books.rename")()
	res, err := r.pool.ExecContext(ctx, q, name, id, userID)
	if err != nil {
		if isAddressBookNameConflict(err) {
			return ErrConflict
		}
		return err
	}
	rows, err := res.RowsAffected()
	if err != nil {
		return err
	}
	if rows == 0 {
		return ErrNotFound
	}
	return nil
}

func (r *addressBookRepo) Delete(ctx context.Context, userID, id int64) error {
	const q = `DELETE FROM address_books WHERE id=$1 AND user_id=$2`
	defer observeDB(ctx, "address_books.delete")()
	res, err := r.pool.ExecContext(ctx, q, id, userID)
	if err != nil {
		return err
	}
	rows, err := res.RowsAffected()
	if err != nil {
		return err
	}
	if rows == 0 {
		return ErrNotFound
	}
	return nil
}

// contactRepo implements ContactRepository.
type contactRepo struct {
	pool *sql.DB
}

func (r *contactRepo) Upsert(ctx context.Context, contact Contact) (*Contact, error) {
	// Parse vCard to extract fields
	displayName, primaryEmail, birthday := parseVCardFields(contact.RawVCard)
	if contact.ResourceName == "" {
		contact.ResourceName = contact.UID
	}

	const q = `
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
`
	defer observeDB(ctx, "contacts.upsert")()
	tx, err := r.pool.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()
	if err := lockRepositoryCollectionsTx(ctx, tx, "address_books", contact.AddressBookID); err != nil {
		return nil, err
	}
	row := tx.QueryRowContext(ctx, q, contact.AddressBookID, contact.UID, contact.ResourceName, contact.RawVCard, contact.ETag, displayName, primaryEmail, birthday)
	c, err := scanContact(row.Scan)
	if err != nil {
		if isContactResourceNameConflict(err) {
			return nil, ErrConflict
		}
		return nil, err
	}
	if err := tx.Commit(); err != nil {
		return nil, err
	}
	return &c, nil
}

func (r *contactRepo) DeleteByUID(ctx context.Context, addressBookID int64, uid string) error {
	const q = `DELETE FROM contacts WHERE address_book_id=$1 AND uid=$2`
	defer observeDB(ctx, "contacts.delete_by_uid")()
	tx, err := r.pool.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if err := lockRepositoryCollectionsTx(ctx, tx, "address_books", addressBookID); err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, q, addressBookID, uid); err != nil {
		return err
	}
	return tx.Commit()
}

func (r *contactRepo) MoveToAddressBook(ctx context.Context, fromAddressBookID, toAddressBookID int64, uid, destResourceName string) error {
	defer observeDB(ctx, "contacts.move_to_address_book")()

	tx, err := r.pool.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if err := lockRepositoryCollectionsTx(ctx, tx, "address_books", fromAddressBookID, toAddressBookID); err != nil {
		return err
	}

	if err := moveContactTx(ctx, tx, fromAddressBookID, toAddressBookID, uid, destResourceName); err != nil {
		return err
	}
	return tx.Commit()
}

func moveContactTx(ctx context.Context, tx queryExecContext, fromAddressBookID, toAddressBookID int64, uid, destResourceName string) error {
	if destResourceName == "" {
		destResourceName = uid
	}

	const selectQ = `SELECT resource_name FROM contacts WHERE address_book_id=$1 AND uid=$2`
	var sourceResourceName string
	if err := tx.QueryRowContext(ctx, selectQ, fromAddressBookID, uid).Scan(&sourceResourceName); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return ErrNotFound
		}
		return err
	}
	if fromAddressBookID != toAddressBookID {
		var existingDestResourceName string
		switch err := tx.QueryRowContext(ctx, selectQ, toAddressBookID, uid).Scan(&existingDestResourceName); {
		case err == nil:
			if existingDestResourceName != "" && existingDestResourceName != destResourceName {
				return ErrConflict
			}
		case errors.Is(err, sql.ErrNoRows):
		default:
			return err
		}
	}

	const deleteDestByNameQ = `DELETE FROM contacts WHERE address_book_id=$1 AND resource_name=$2 AND uid<>$3`
	if _, err := tx.ExecContext(ctx, deleteDestByNameQ, toAddressBookID, destResourceName, uid); err != nil {
		return err
	}

	if fromAddressBookID != toAddressBookID {
		const deleteDestByUIDQ = `DELETE FROM contacts WHERE address_book_id=$1 AND uid=$2`
		if _, err := tx.ExecContext(ctx, deleteDestByUIDQ, toAddressBookID, uid); err != nil {
			return err
		}
	}

	const moveQuery = `UPDATE contacts SET address_book_id=$1, resource_name=$2, last_modified=NOW() WHERE address_book_id=$3 AND uid=$4`
	result, err := tx.ExecContext(ctx, moveQuery, toAddressBookID, destResourceName, fromAddressBookID, uid)
	if err != nil {
		return err
	}
	rows, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if rows == 0 {
		return ErrNotFound
	}

	if fromAddressBookID == toAddressBookID {
		if sourceResourceName != destResourceName {
			const tombstoneQuery = `INSERT INTO deleted_resources (resource_type, collection_id, uid, resource_name) VALUES ('contact', $1, $2, $3)`
			if _, err := tx.ExecContext(ctx, tombstoneQuery, fromAddressBookID, uid, sourceResourceName); err != nil {
				return err
			}
		}
		return nil
	}

	const tombstoneQuery = `INSERT INTO deleted_resources (resource_type, collection_id, uid, resource_name) VALUES ('contact', $1, $2, $3)`
	if _, err := tx.ExecContext(ctx, tombstoneQuery, fromAddressBookID, uid, sourceResourceName); err != nil {
		return err
	}

	const incrementCtagQuery = `UPDATE address_books SET ctag = ctag + 1, updated_at = NOW() WHERE id = $1`
	if _, err := tx.ExecContext(ctx, incrementCtagQuery, fromAddressBookID); err != nil {
		return err
	}

	return nil
}

func (r *contactRepo) GetByUID(ctx context.Context, addressBookID int64, uid string) (*Contact, error) {
	const q = `SELECT id, address_book_id, uid, resource_name, raw_vcard, etag, display_name, primary_email, birthday, last_modified FROM contacts WHERE address_book_id=$1 AND uid=$2`
	defer observeDB(ctx, "contacts.get_by_uid")()
	row := r.pool.QueryRowContext(ctx, q, addressBookID, uid)
	c, err := scanContact(row.Scan)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, nil
		}
		return nil, err
	}
	return &c, nil
}

func (r *contactRepo) ListByUIDs(ctx context.Context, addressBookID int64, uids []string) ([]Contact, error) {
	if len(uids) == 0 {
		return []Contact{}, nil
	}
	const q = `SELECT id, address_book_id, uid, resource_name, raw_vcard, etag, display_name, primary_email, birthday, last_modified FROM contacts WHERE address_book_id=$1 AND uid = ANY($2)`
	defer observeDB(ctx, "contacts.list_by_uids")()
	rows, err := r.pool.QueryContext(ctx, q, addressBookID, pq.Array(uids))
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var result []Contact
	for rows.Next() {
		c, err := scanContact(rows.Scan)
		if err != nil {
			return nil, err
		}
		result = append(result, c)
	}
	return result, rows.Err()
}

func (r *contactRepo) ListForBook(ctx context.Context, addressBookID int64) ([]Contact, error) {
	const q = `SELECT id, address_book_id, uid, resource_name, raw_vcard, etag, display_name, primary_email, birthday, last_modified FROM contacts WHERE address_book_id=$1 ORDER BY last_modified DESC`
	defer observeDB(ctx, "contacts.list_for_book")()
	rows, err := r.pool.QueryContext(ctx, q, addressBookID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var result []Contact
	for rows.Next() {
		c, err := scanContact(rows.Scan)
		if err != nil {
			return nil, err
		}
		result = append(result, c)
	}
	return result, rows.Err()
}

func (r *contactRepo) ListForBookPageAfter(ctx context.Context, addressBookID, afterID int64, limit int) ([]Contact, error) {
	if limit <= 0 {
		return []Contact{}, nil
	}
	// The collection equality scopes the read and the row comparison pins the
	// keyset index; ListForCalendarPageAfter carries why neither may be dropped.
	const q = `SELECT ` + contactColumns + ` FROM contacts WHERE address_book_id=$1 AND (address_book_id, id) > ($1, $2) ORDER BY address_book_id ASC, id ASC LIMIT $3`
	defer observeDB(ctx, "contacts.list_for_book_page_after")()
	rows, err := r.pool.QueryContext(ctx, q, addressBookID, afterID, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var result []Contact
	for rows.Next() {
		contact, err := scanContact(rows.Scan)
		if err != nil {
			return nil, err
		}
		result = append(result, contact)
	}
	return result, rows.Err()
}

// contactColumns is the canonical select list shared by contact queries.
const contactColumns = `id, address_book_id, uid, resource_name, raw_vcard, etag, display_name, primary_email, birthday, last_modified`

// ListForBookFiltered returns contacts in an address book matching f. Every
// query is scoped to a single address_book_id (served by the
// (address_book_id, display_name) index), so it never triggers a full table
// scan; text predicates run over that narrowed set.
func (r *contactRepo) ListForBookFiltered(ctx context.Context, addressBookID int64, f ContactFilter) ([]Contact, error) {
	var sb strings.Builder
	sb.WriteString(`SELECT ` + contactColumns + ` FROM contacts WHERE address_book_id=$1`)
	args := []any{addressBookID}
	placeholder := func(v any) string {
		args = append(args, v)
		return "$" + strconv.Itoa(len(args))
	}

	if f.Name != "" {
		sb.WriteString(` AND display_name ILIKE `)
		sb.WriteString(placeholder("%" + likeEscape(f.Name) + "%"))
	}
	if f.Email != "" {
		sb.WriteString(` AND primary_email ILIKE `)
		sb.WriteString(placeholder("%" + likeEscape(f.Email) + "%"))
	}
	if f.Query != "" {
		p := placeholder("%" + likeEscape(f.Query) + "%")
		sb.WriteString(` AND (display_name ILIKE `)
		sb.WriteString(p)
		sb.WriteString(` OR primary_email ILIKE `)
		sb.WriteString(p)
		sb.WriteString(`)`)
	}
	sb.WriteString(` ORDER BY LOWER(COALESCE(display_name, '')) ASC, id ASC`)
	if f.Limit > 0 {
		sb.WriteString(` LIMIT `)
		sb.WriteString(placeholder(f.Limit))
	}
	if f.Offset > 0 {
		sb.WriteString(` OFFSET `)
		sb.WriteString(placeholder(f.Offset))
	}

	defer observeDB(ctx, "contacts.list_for_book_filtered")()
	rows, err := r.pool.QueryContext(ctx, sb.String(), args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var result []Contact
	for rows.Next() {
		c, err := scanContact(rows.Scan)
		if err != nil {
			return nil, err
		}
		result = append(result, c)
	}
	return result, rows.Err()
}

func (r *contactRepo) ListForBookPaginated(ctx context.Context, addressBookID int64, limit, offset int) (*PaginatedResult[Contact], error) {
	defer observeDB(ctx, "contacts.list_for_book_paginated")()

	// Get total count
	var totalCount int
	countQ := `SELECT COUNT(*) FROM contacts WHERE address_book_id=$1`
	if err := r.pool.QueryRowContext(ctx, countQ, addressBookID).Scan(&totalCount); err != nil {
		return nil, err
	}

	const q = `SELECT id, address_book_id, uid, resource_name, raw_vcard, etag, display_name, primary_email, birthday, last_modified FROM contacts WHERE address_book_id=$1 ORDER BY LOWER(COALESCE(display_name, '')) ASC, id ASC LIMIT $2 OFFSET $3`
	rows, err := r.pool.QueryContext(ctx, q, addressBookID, limit, offset)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var items []Contact
	for rows.Next() {
		c, err := scanContact(rows.Scan)
		if err != nil {
			return nil, err
		}
		items = append(items, c)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	return &PaginatedResult[Contact]{
		Items:      items,
		TotalCount: totalCount,
		Limit:      limit,
		Offset:     offset,
	}, nil
}

// ListModifiedSincePageAfter returns one keyset page of the address book's rows
// modified after since, on the same terms the event repository reads a calendar
// with.
func (r *contactRepo) ListModifiedSincePageAfter(ctx context.Context, addressBookID, afterID int64, since time.Time, limit int) ([]Contact, error) {
	if limit <= 0 {
		return []Contact{}, nil
	}
	// The collection equality scopes the read and the row comparison pins the
	// keyset index; ListForCalendarPageAfter carries why neither may be dropped.
	const q = `SELECT ` + contactColumns + ` FROM contacts WHERE address_book_id=$1 AND (address_book_id, id) > ($1, $2) AND last_modified > $3 ORDER BY address_book_id ASC, id ASC LIMIT $4`
	defer observeDB(ctx, "contacts.list_modified_since_page_after")()
	rows, err := r.pool.QueryContext(ctx, q, addressBookID, afterID, since, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var result []Contact
	for rows.Next() {
		c, err := scanContact(rows.Scan)
		if err != nil {
			return nil, err
		}
		result = append(result, c)
	}
	return result, rows.Err()
}

func (r *contactRepo) ListRecentByUser(ctx context.Context, userID int64, limit int) ([]Contact, error) {
	const q = `
SELECT c.id, c.address_book_id, c.uid, c.resource_name, c.raw_vcard, c.etag, c.display_name, c.primary_email, c.birthday, c.last_modified
FROM contacts c
JOIN address_books ab ON ab.id = c.address_book_id
WHERE ab.user_id = $1
ORDER BY c.last_modified DESC
LIMIT $2
`
	defer observeDB(ctx, "contacts.list_recent_by_user")()
	rows, err := r.pool.QueryContext(ctx, q, userID, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var result []Contact
	for rows.Next() {
		c, err := scanContact(rows.Scan)
		if err != nil {
			return nil, err
		}
		result = append(result, c)
	}
	return result, rows.Err()
}

func (r *contactRepo) MaxLastModified(ctx context.Context, addressBookID int64) (time.Time, error) {
	const q = `SELECT COALESCE(MAX(last_modified), '1970-01-01T00:00:00Z') FROM contacts WHERE address_book_id=$1`
	defer observeDB(ctx, "contacts.max_last_modified")()
	var ts time.Time
	if err := r.pool.QueryRowContext(ctx, q, addressBookID).Scan(&ts); err != nil {
		return time.Time{}, err
	}
	return ts.UTC(), nil
}

// ListWithBirthdaysByUserLimit is that set truncated to limit rows. The set
// spans every address book the user owns, so no column orders it within one
// collection and a keyset page would rescan it per page; a caller bounding the
// read asks for one row more than it will accept instead.
func (r *contactRepo) ListWithBirthdaysByUserLimit(ctx context.Context, userID int64, limit int) ([]Contact, error) {
	if limit <= 0 {
		return []Contact{}, nil
	}
	const q = `
SELECT c.id, c.address_book_id, c.uid, c.resource_name, c.raw_vcard, c.etag, c.display_name, c.primary_email, c.birthday, c.last_modified
FROM contacts c
JOIN address_books ab ON ab.id = c.address_book_id
WHERE ab.user_id = $1 AND c.birthday IS NOT NULL
ORDER BY c.display_name
LIMIT $2
`
	defer observeDB(ctx, "contacts.list_with_birthdays_by_user_limit")()
	rows, err := r.pool.QueryContext(ctx, q, userID, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var result []Contact
	for rows.Next() {
		c, err := scanContact(rows.Scan)
		if err != nil {
			return nil, err
		}
		result = append(result, c)
	}
	return result, rows.Err()
}

func (r *contactRepo) GetByResourceName(ctx context.Context, addressBookID int64, resourceName string) (*Contact, error) {
	const q = `SELECT id, address_book_id, uid, resource_name, raw_vcard, etag, display_name, primary_email, birthday, last_modified FROM contacts WHERE address_book_id=$1 AND resource_name=$2`
	defer observeDB(ctx, "contacts.get_by_resource_name")()
	row := r.pool.QueryRowContext(ctx, q, addressBookID, resourceName)
	c, err := scanContact(row.Scan)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, nil
		}
		return nil, err
	}
	return &c, nil
}

func (r *contactRepo) ListByResourceNames(ctx context.Context, addressBookID int64, resourceNames []string) ([]Contact, error) {
	if len(resourceNames) == 0 {
		return []Contact{}, nil
	}
	const q = `SELECT id, address_book_id, uid, resource_name, raw_vcard, etag, display_name, primary_email, birthday, last_modified FROM contacts WHERE address_book_id=$1 AND resource_name = ANY($2)`
	defer observeDB(ctx, "contacts.list_by_resource_names")()
	rows, err := r.pool.QueryContext(ctx, q, addressBookID, pq.Array(resourceNames))
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var result []Contact
	for rows.Next() {
		c, err := scanContact(rows.Scan)
		if err != nil {
			return nil, err
		}
		result = append(result, c)
	}
	return result, rows.Err()
}

func (r *contactRepo) CopyToAddressBook(ctx context.Context, fromAddressBookID, toAddressBookID int64, uid, destResourceName, newETag string) (*Contact, error) {
	defer observeDB(ctx, "contacts.copy_to_address_book")()

	tx, err := r.pool.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()
	if err := lockRepositoryCollectionsTx(ctx, tx, "address_books", fromAddressBookID, toAddressBookID); err != nil {
		return nil, err
	}

	const selectQ = `SELECT id, address_book_id, uid, resource_name, raw_vcard, etag, display_name, primary_email, birthday, last_modified FROM contacts WHERE address_book_id=$1 AND uid=$2`
	row := tx.QueryRowContext(ctx, selectQ, fromAddressBookID, uid)
	src, err := scanContact(row.Scan)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, ErrNotFound
		}
		return nil, err
	}

	if destResourceName == "" {
		destResourceName = src.ResourceName
		if destResourceName == "" {
			destResourceName = src.UID
		}
	}

	const existingDestQ = `SELECT resource_name FROM contacts WHERE address_book_id=$1 AND uid=$2`
	var existingDestResourceName string
	switch err := tx.QueryRowContext(ctx, existingDestQ, toAddressBookID, src.UID).Scan(&existingDestResourceName); {
	case err == nil:
	case errors.Is(err, sql.ErrNoRows):
		existingDestResourceName = ""
	default:
		return nil, err
	}

	const deleteDestByNameQ = `DELETE FROM contacts WHERE address_book_id=$1 AND resource_name=$2 AND uid<>$3`
	if _, err := tx.ExecContext(ctx, deleteDestByNameQ, toAddressBookID, destResourceName, src.UID); err != nil {
		return nil, err
	}
	if existingDestResourceName != "" && existingDestResourceName != destResourceName {
		const tombstoneQuery = `INSERT INTO deleted_resources (resource_type, collection_id, uid, resource_name) VALUES ('contact', $1, $2, $3)`
		if _, err := tx.ExecContext(ctx, tombstoneQuery, toAddressBookID, src.UID, existingDestResourceName); err != nil {
			return nil, err
		}
	}

	const insertQ = `
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
`
	insertRow := tx.QueryRowContext(ctx, insertQ, toAddressBookID, src.UID, destResourceName, src.RawVCard, newETag, src.DisplayName, src.PrimaryEmail, src.Birthday)
	c, err := scanContact(insertRow.Scan)
	if err != nil {
		return nil, err
	}

	if err := tx.Commit(); err != nil {
		return nil, err
	}
	return &c, nil
}

// appPasswordRepo implements AppPasswordRepository.
type appPasswordRepo struct {
	pool *sql.DB
}

func (r *appPasswordRepo) Create(ctx context.Context, token AppPassword) (*AppPassword, error) {
	const q = `
INSERT INTO app_passwords (user_id, label, token_hash, digest_md5_ha1, digest_sha256_ha1, expires_at)
VALUES ($1, $2, $3, $4, $5, $6)
RETURNING id, user_id, label, token_hash, digest_md5_ha1, digest_sha256_ha1, created_at, expires_at, revoked_at, last_used_at
`
	defer observeDB(ctx, "app_passwords.create")()
	row := r.pool.QueryRowContext(ctx, q, token.UserID, token.Label, token.TokenHash, token.DigestMD5HA1, token.DigestSHA256HA1, token.ExpiresAt)
	t, err := scanAppPassword(row.Scan)
	if err != nil {
		return nil, err
	}
	return &t, nil
}

func (r *appPasswordRepo) FindValidByUser(ctx context.Context, userID int64) ([]AppPassword, error) {
	const q = `
SELECT id, user_id, label, token_hash, digest_md5_ha1, digest_sha256_ha1, created_at, expires_at, revoked_at, last_used_at
FROM app_passwords
WHERE user_id=$1 AND revoked_at IS NULL AND (expires_at IS NULL OR expires_at > NOW())
ORDER BY created_at DESC
`
	defer observeDB(ctx, "app_passwords.find_valid_by_user")()
	rows, err := r.pool.QueryContext(ctx, q, userID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var result []AppPassword
	for rows.Next() {
		t, err := scanAppPassword(rows.Scan)
		if err != nil {
			return nil, err
		}
		result = append(result, t)
	}
	return result, rows.Err()
}

func (r *appPasswordRepo) ListByUser(ctx context.Context, userID int64) ([]AppPassword, error) {
	const q = `SELECT id, user_id, label, token_hash, digest_md5_ha1, digest_sha256_ha1, created_at, expires_at, revoked_at, last_used_at FROM app_passwords WHERE user_id=$1 ORDER BY created_at DESC`
	defer observeDB(ctx, "app_passwords.list_by_user")()
	rows, err := r.pool.QueryContext(ctx, q, userID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var result []AppPassword
	for rows.Next() {
		t, err := scanAppPassword(rows.Scan)
		if err != nil {
			return nil, err
		}
		result = append(result, t)
	}
	return result, rows.Err()
}

func (r *appPasswordRepo) GetByID(ctx context.Context, id int64) (*AppPassword, error) {
	const q = `SELECT id, user_id, label, token_hash, digest_md5_ha1, digest_sha256_ha1, created_at, expires_at, revoked_at, last_used_at FROM app_passwords WHERE id=$1`
	defer observeDB(ctx, "app_passwords.get_by_id")()
	row := r.pool.QueryRowContext(ctx, q, id)
	t, err := scanAppPassword(row.Scan)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, nil
		}
		return nil, err
	}
	return &t, nil
}

func (r *appPasswordRepo) Revoke(ctx context.Context, id int64) error {
	const q = `UPDATE app_passwords SET revoked_at = NOW() WHERE id=$1`
	defer observeDB(ctx, "app_passwords.revoke")()
	_, err := r.pool.ExecContext(ctx, q, id)
	return err
}

func (r *appPasswordRepo) DeleteRevoked(ctx context.Context, id int64) error {
	const q = `DELETE FROM app_passwords WHERE id=$1 AND revoked_at IS NOT NULL`
	defer observeDB(ctx, "app_passwords.delete_revoked")()
	_, err := r.pool.ExecContext(ctx, q, id)
	return err
}

func (r *appPasswordRepo) PurgeDigestCredentials(ctx context.Context) (int64, error) {
	const q = `UPDATE app_passwords SET digest_md5_ha1=NULL, digest_sha256_ha1=NULL
        WHERE digest_md5_ha1 IS NOT NULL OR digest_sha256_ha1 IS NOT NULL`
	defer observeDB(ctx, "app_passwords.purge_digest_credentials")()
	result, err := r.pool.ExecContext(ctx, q)
	if err != nil {
		return 0, err
	}
	return result.RowsAffected()
}

func (r *appPasswordRepo) TouchLastUsed(ctx context.Context, id int64) error {
	const q = `UPDATE app_passwords SET last_used_at = NOW() WHERE id=$1`
	defer observeDB(ctx, "app_passwords.touch_last_used")()
	_, err := r.pool.ExecContext(ctx, q, id)
	return err
}

// digestNonceRepo implements DigestNonceRepository.
type digestNonceRepo struct {
	pool *sql.DB
}

// Consume claims the pair by inserting it. The primary key decides the race, so
// two instances handed the same captured Authorization header cannot both see
// an insert: the loser changes no row and reads that as the replay it is.
func (r *digestNonceRepo) Consume(ctx context.Context, tokenID int64, nonce string, nonceCount uint32, expiresAt time.Time) (bool, error) {
	const q = `
INSERT INTO digest_nonce_counts (token_id, nonce, nonce_count, expires_at)
VALUES ($1, $2, $3, $4)
ON CONFLICT (token_id, nonce, nonce_count) DO NOTHING
`
	defer observeDB(ctx, "digest_nonce.consume")()
	res, err := r.pool.ExecContext(ctx, q, tokenID, nonce, int64(nonceCount), expiresAt)
	if err != nil {
		return false, err
	}
	rows, err := res.RowsAffected()
	if err != nil {
		return false, err
	}
	return rows == 1, nil
}

func (r *digestNonceRepo) DeleteExpired(ctx context.Context) (int64, error) {
	const q = `DELETE FROM digest_nonce_counts WHERE expires_at < NOW()`
	defer observeDB(ctx, "digest_nonce.delete_expired")()
	res, err := r.pool.ExecContext(ctx, q)
	if err != nil {
		return 0, err
	}
	rows, err := res.RowsAffected()
	if err != nil {
		return 0, err
	}
	return rows, nil
}

// deletedResourceRepo implements DeletedResourceRepository.
type deletedResourceRepo struct {
	pool *sql.DB
}

// ListDeletedSincePageAfter returns one keyset page of the collection's
// tombstones recorded after since, ordered by id so a caller can resume from
// the last id it saw. Nothing prunes this table, so the set a client's sync
// token selects grows without bound and the sync report that reads it bounds
// how many pages it will take.
func (r *deletedResourceRepo) ListDeletedSincePageAfter(ctx context.Context, resourceType string, collectionID, afterID int64, since time.Time, limit int) ([]DeletedResource, error) {
	if limit <= 0 {
		return []DeletedResource{}, nil
	}
	const q = `SELECT id, resource_type, collection_id, uid, resource_name, deleted_at FROM deleted_resources WHERE resource_type=$1 AND collection_id=$2 AND id>$3 AND deleted_at > $4 ORDER BY id ASC LIMIT $5`
	defer observeDB(ctx, "deleted_resources.list_deleted_since_page_after")()
	rows, err := r.pool.QueryContext(ctx, q, resourceType, collectionID, afterID, since, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var result []DeletedResource
	for rows.Next() {
		var d DeletedResource
		if err := rows.Scan(&d.ID, &d.ResourceType, &d.CollectionID, &d.UID, &d.ResourceName, &d.DeletedAt); err != nil {
			return nil, err
		}
		result = append(result, d)
	}
	return result, rows.Err()
}

func (r *deletedResourceRepo) DeleteByIdentity(ctx context.Context, resourceType string, collectionID int64, uid, resourceName string) error {
	const q = `DELETE FROM deleted_resources WHERE resource_type=$1 AND collection_id=$2 AND uid=$3 AND resource_name=$4`
	defer observeDB(ctx, "deleted_resources.delete_by_identity")()
	_, err := r.pool.ExecContext(ctx, q, resourceType, collectionID, uid, resourceName)
	return err
}

func (r *deletedResourceRepo) Cleanup(ctx context.Context, olderThan time.Duration) (int64, error) {
	const q = `DELETE FROM deleted_resources WHERE deleted_at < $1`
	defer observeDB(ctx, "deleted_resources.cleanup")()
	cutoff := time.Now().Add(-olderThan)
	res, err := r.pool.ExecContext(ctx, q, cutoff)
	if err != nil {
		return 0, err
	}
	rows, err := res.RowsAffected()
	if err != nil {
		return 0, err
	}
	return rows, nil
}

// sessionRepo implements SessionRepository.
type sessionRepo struct {
	pool *sql.DB
}

func (r *sessionRepo) Create(ctx context.Context, session Session) (*Session, error) {
	const q = `
INSERT INTO sessions (id, user_id, user_agent, ip_address, expires_at)
VALUES ($1, $2, $3, $4, $5)
RETURNING id, user_id, user_agent, ip_address, created_at, expires_at, last_seen_at
`
	defer observeDB(ctx, "sessions.create")()
	row := r.pool.QueryRowContext(ctx, q, session.ID, session.UserID, session.UserAgent, session.IPAddress, session.ExpiresAt)
	s, err := scanSession(row.Scan)
	if err != nil {
		return nil, err
	}
	return &s, nil
}

func (r *sessionRepo) GetByID(ctx context.Context, id string) (*Session, error) {
	const q = `SELECT id, user_id, user_agent, ip_address, created_at, expires_at, last_seen_at FROM sessions WHERE id=$1 AND expires_at > NOW()`
	defer observeDB(ctx, "sessions.get_by_id")()
	row := r.pool.QueryRowContext(ctx, q, id)
	s, err := scanSession(row.Scan)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, nil
		}
		return nil, err
	}
	return &s, nil
}

func (r *sessionRepo) ListByUser(ctx context.Context, userID int64) ([]Session, error) {
	const q = `SELECT id, user_id, user_agent, ip_address, created_at, expires_at, last_seen_at FROM sessions WHERE user_id=$1 AND expires_at > NOW() ORDER BY last_seen_at DESC`
	defer observeDB(ctx, "sessions.list_by_user")()
	rows, err := r.pool.QueryContext(ctx, q, userID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var result []Session
	for rows.Next() {
		s, err := scanSession(rows.Scan)
		if err != nil {
			return nil, err
		}
		result = append(result, s)
	}
	return result, rows.Err()
}

func (r *sessionRepo) TouchLastSeen(ctx context.Context, id string) error {
	const q = `UPDATE sessions SET last_seen_at = NOW() WHERE id=$1`
	defer observeDB(ctx, "sessions.touch_last_seen")()
	_, err := r.pool.ExecContext(ctx, q, id)
	return err
}

func (r *sessionRepo) Delete(ctx context.Context, id string) error {
	const q = `DELETE FROM sessions WHERE id=$1`
	defer observeDB(ctx, "sessions.delete")()
	_, err := r.pool.ExecContext(ctx, q, id)
	return err
}

func (r *sessionRepo) DeleteByUser(ctx context.Context, userID int64) error {
	const q = `DELETE FROM sessions WHERE user_id=$1`
	defer observeDB(ctx, "sessions.delete_by_user")()
	_, err := r.pool.ExecContext(ctx, q, userID)
	return err
}

func (r *sessionRepo) DeleteExpired(ctx context.Context) (int64, error) {
	const q = `DELETE FROM sessions WHERE expires_at < NOW()`
	defer observeDB(ctx, "sessions.delete_expired")()
	res, err := r.pool.ExecContext(ctx, q)
	if err != nil {
		return 0, err
	}
	rows, err := res.RowsAffected()
	if err != nil {
		return 0, err
	}
	return rows, nil
}

// lockRepo implements LockRepository.
type lockRepo struct {
	pool *sql.DB
}

func validateLockDepth(depth string) error {
	switch strings.ToLower(strings.TrimSpace(depth)) {
	case "0", "infinity":
		return nil
	default:
		return errors.New("invalid lock depth")
	}
}

func (r *lockRepo) Create(ctx context.Context, lock Lock) (*Lock, error) {
	defer observeDB(ctx, "locks.create")()
	if err := validateLockDepth(lock.Depth); err != nil {
		return nil, err
	}
	tx, err := r.pool.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()
	created, err := createLockTx(ctx, tx, lock, nil)
	if err != nil {
		return nil, err
	}
	if err := tx.Commit(); err != nil {
		return nil, err
	}
	return created, nil
}

// createLockTx records lock after checking, under the DAV path locks of its
// resource, that the target is still in the state the request was authorized
// against and that no existing lock conflicts with it.
func createLockTx(ctx context.Context, tx *sql.Tx, lock Lock, acl *ACLGuard) (*Lock, error) {
	// Serialize concurrent lock creation for the resource and its parent path so
	// parent/child lock requests observe each other before conflict checks run.
	if err := acquireDAVPathLocks(ctx, tx, lock.ResourcePath); err != nil {
		return nil, err
	}
	originalResourcePath := lock.ResourcePath
	if err := canonicalizePendingCalendarLockTx(ctx, tx, &lock); err != nil {
		return nil, err
	}
	if lock.ExpectedTargetExists != nil && !*lock.ExpectedTargetExists && lock.ResourcePath != originalResourcePath {
		return nil, ErrResourceStateChanged
	}
	if err := validateACLGuardTx(ctx, tx, acl); err != nil {
		return nil, err
	}
	if lock.ExpectedCollection != "" {
		if err := validateTargetStateTx(ctx, tx, lock.ExpectedCollection, lock.ExpectedCollectionID, lock.ExpectedResourceState); err != nil {
			return nil, err
		}
	}

	// Check for conflicting locks on the resource itself.
	const conflictQ = `SELECT lock_scope FROM locks WHERE resource_path = $1 AND expires_at > NOW()`
	rows, err := tx.QueryContext(ctx, conflictQ, lock.ResourcePath)
	if err != nil {
		return nil, err
	}
	for rows.Next() {
		var existingScope string
		if err := rows.Scan(&existingScope); err != nil {
			rows.Close()
			return nil, err
		}
		if existingScope == "exclusive" || lock.LockScope == "exclusive" {
			rows.Close()
			return nil, ErrLockConflict
		}
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return nil, err
	}

	// Check for depth-infinity ancestor locks that would conflict.
	ancestors := lockAncestorPaths(lock.ResourcePath)
	if len(ancestors) > 0 {
		const ancestorQ = `SELECT lock_scope FROM locks WHERE resource_path = ANY($1) AND depth = 'infinity' AND expires_at > NOW()`
		aRows, err := tx.QueryContext(ctx, ancestorQ, pq.Array(ancestors))
		if err != nil {
			return nil, err
		}
		for aRows.Next() {
			var existingScope string
			if err := aRows.Scan(&existingScope); err != nil {
				aRows.Close()
				return nil, err
			}
			if existingScope == "exclusive" || lock.LockScope == "exclusive" {
				aRows.Close()
				return nil, ErrLockConflict
			}
		}
		aRows.Close()
		if err := aRows.Err(); err != nil {
			return nil, err
		}
	}

	if lock.Depth == "infinity" {
		const descendantQ = `SELECT lock_scope FROM locks WHERE resource_path LIKE $1 ESCAPE '\' AND expires_at > NOW()`
		descendantPrefix := strings.NewReplacer(`\`, `\\`, `%`, `\%`, `_`, `\_`).Replace(strings.TrimSuffix(lock.ResourcePath, "/") + "/")
		dRows, err := tx.QueryContext(ctx, descendantQ, descendantPrefix+"%")
		if err != nil {
			return nil, err
		}
		for dRows.Next() {
			var existingScope string
			if err := dRows.Scan(&existingScope); err != nil {
				dRows.Close()
				return nil, err
			}
			if existingScope == "exclusive" || lock.LockScope == "exclusive" {
				dRows.Close()
				return nil, ErrLockConflict
			}
		}
		dRows.Close()
		if err := dRows.Err(); err != nil {
			return nil, err
		}
	}

	const insertQ = `
INSERT INTO locks (token, resource_path, user_id, lock_scope, lock_type, depth, owner_info, timeout_seconds, expires_at)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
RETURNING id, token, resource_path, user_id, lock_scope, lock_type, depth, owner_info, timeout_seconds, created_at, expires_at
`
	row := tx.QueryRowContext(ctx, insertQ, lock.Token, lock.ResourcePath, lock.UserID, lock.LockScope, lock.LockType, lock.Depth, lock.OwnerInfo, lock.TimeoutSeconds, lock.ExpiresAt)
	var l Lock
	if err := row.Scan(&l.ID, &l.Token, &l.ResourcePath, &l.UserID, &l.LockScope, &l.LockType, &l.Depth, &l.OwnerInfo, &l.TimeoutSeconds, &l.CreatedAt, &l.ExpiresAt); err != nil {
		return nil, err
	}
	return &l, nil
}

func canonicalizePendingCalendarLockTx(ctx context.Context, tx *sql.Tx, lock *Lock) error {
	if lock == nil {
		return nil
	}
	userID, slug, ok := pendingCalendarLockIdentity(lock.ResourcePath)
	if !ok || userID != lock.UserID {
		return nil
	}
	var calendarID int64
	err := tx.QueryRowContext(ctx, `SELECT id FROM calendars WHERE user_id=$1 AND LOWER(slug)=LOWER($2)`, userID, slug).Scan(&calendarID)
	if errors.Is(err, sql.ErrNoRows) {
		return nil
	}
	if err != nil {
		return err
	}
	lock.ResourcePath = path.Join("/dav/calendars", strconv.FormatInt(calendarID, 10))
	return nil
}

func pendingCalendarLockIdentity(resourcePath string) (int64, string, bool) {
	const prefix = "/dav/calendars/.pending/"
	cleanPath := path.Clean(resourcePath)
	if !strings.HasPrefix(cleanPath, prefix) {
		return 0, "", false
	}
	parts := strings.Split(strings.TrimPrefix(cleanPath, prefix), "/")
	if len(parts) != 2 || parts[1] == "" {
		return 0, "", false
	}
	userID, err := strconv.ParseInt(parts[0], 10, 64)
	if err != nil || userID <= 0 {
		return 0, "", false
	}
	return userID, parts[1], true
}

// davPathLock is one advisory lock a transaction takes on a DAV resource path.
// key is what PostgreSQL actually locks; path is kept for diagnostics.
type davPathLock struct {
	key       int64
	path      string
	exclusive bool
}

// davPathLockQuery takes a whole davPathLocks set in one round trip. unnest
// scans the arrays in the order they were built and the lock call in the target
// list runs as each row is produced, so the set is still acquired in the key
// order that keeps overlapping callers deadlock-free. Splitting the two modes
// into separate statements would acquire them in two disjoint orders instead,
// and an ORDER BY here would not help: the planner is free to evaluate the
// target list below the sort.
const davPathLockQuery = `
SELECT CASE WHEN l.exclusive THEN pg_advisory_xact_lock(l.key)
            ELSE pg_advisory_xact_lock_shared(l.key) END
FROM unnest($1::bigint[], $2::boolean[]) AS l(key, exclusive)`

// davPathLockKey maps a DAV path to its advisory lock key. It uses the whole
// 64-bit key space so that two paths sharing a key, which would silently merge
// their locks, stays practically impossible.
func davPathLockKey(resourcePath string) int64 {
	hash := fnv.New64a()
	hash.Write([]byte(resourcePath))
	return int64(hash.Sum64())
}

// davPathLocks orders the advisory locks a transaction must hold to serialize
// DAV writes to resourcePaths against the operations that can conflict with
// them under RFC 4918.
//
// Each requested path is the caller's own target and is taken exclusively, and
// so is the collection directly containing a target. A member write changes its
// collection's CTag and sync token, and sync-collection reports the changes
// since a token as the rows stamped later than it. Those stamps are taken
// before the write commits, so they only order the way the commits do while
// writers to one collection exclude each other; two sibling writes left to run
// together could commit a stamp older than a token already handed out, and
// that change would never be reported.
//
// Every farther ancestor is only observed and is taken shared, so writes to
// different collections do not wait on each other, while a depth-infinity
// operation, which names the collection itself as a target, still excludes its
// members. The DAV root and the per-kind roots ("/dav", "/dav/calendars") are
// never taken exclusively on a child's behalf: every collection sits directly
// beneath one, and doing so would serialize the whole server.
//
// A path that is both exclusive and shared within the set is taken once,
// exclusively: taking it shared first and upgrading later would let two
// transactions that both hold it shared deadlock on the upgrade. The set is
// sorted by lock key, the value PostgreSQL actually orders waiters on, so every
// transaction walks one total order and overlapping sets cannot deadlock
// against each other. Paths that share a key merge into one entry for the same
// reason.
func davPathLocks(resourcePaths ...string) []davPathLock {
	byKey := make(map[int64]davPathLock, len(resourcePaths)*3)
	add := func(lockPath string, exclusive bool) {
		key := davPathLockKey(lockPath)
		current, seen := byKey[key]
		if seen && (current.exclusive || !exclusive) {
			return
		}
		if seen {
			lockPath = current.path
		}
		byKey[key] = davPathLock{key: key, path: lockPath, exclusive: exclusive}
	}
	for _, resourcePath := range resourcePaths {
		target := path.Clean(resourcePath)
		add(target, true)
		for i, ancestor := range lockAncestorPaths(target) {
			add(ancestor, i == 0 && !isDAVRootPath(ancestor))
		}
	}

	locks := make([]davPathLock, 0, len(byKey))
	for _, lock := range byKey {
		locks = append(locks, lock)
	}
	slices.SortFunc(locks, func(a, b davPathLock) int { return cmp.Compare(a.key, b.key) })
	return locks
}

// isDAVRootPath reports whether p is "/dav" or one of the per-kind roots
// directly beneath it, none of which is a collection a member write changes.
func isDAVRootPath(p string) bool {
	return strings.Count(p, "/") <= 2
}

// acquireDAVPathLocks takes the advisory locks davPathLocks assigns to
// resourcePaths. Callers that need more than one lock set in a single
// transaction must pass every target to one call: two calls decide the
// exclusive set independently and can order or upgrade them against each other.
func acquireDAVPathLocks(ctx context.Context, tx execContext, resourcePaths ...string) error {
	locks := davPathLocks(resourcePaths...)
	if len(locks) == 0 {
		return nil
	}
	keys := make([]int64, len(locks))
	modes := make([]bool, len(locks))
	for i, lock := range locks {
		keys[i] = lock.key
		modes[i] = lock.exclusive
	}
	_, err := tx.ExecContext(ctx, davPathLockQuery, pq.Array(keys), pq.Array(modes))
	return err
}

// lockAncestorPaths returns all parent paths of p, excluding p itself.
func lockAncestorPaths(p string) []string {
	p = strings.TrimSuffix(p, "/")
	var ancestors []string
	for {
		idx := strings.LastIndex(p, "/")
		if idx <= 0 {
			break
		}
		parent := p[:idx]
		if parent == "" {
			break
		}
		ancestors = append(ancestors, parent)
		p = parent
	}
	return ancestors
}

func (r *lockRepo) GetByToken(ctx context.Context, token string) (*Lock, error) {
	const q = `SELECT id, token, resource_path, user_id, lock_scope, lock_type, depth, owner_info, timeout_seconds, created_at, expires_at FROM locks WHERE token=$1 AND expires_at > NOW()`
	defer observeDB(ctx, "locks.get_by_token")()
	var l Lock
	if err := r.pool.QueryRowContext(ctx, q, token).Scan(&l.ID, &l.Token, &l.ResourcePath, &l.UserID, &l.LockScope, &l.LockType, &l.Depth, &l.OwnerInfo, &l.TimeoutSeconds, &l.CreatedAt, &l.ExpiresAt); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, nil
		}
		return nil, err
	}
	return &l, nil
}

func (r *lockRepo) ListByResource(ctx context.Context, resourcePath string) ([]Lock, error) {
	const q = `SELECT id, token, resource_path, user_id, lock_scope, lock_type, depth, owner_info, timeout_seconds, created_at, expires_at FROM locks WHERE resource_path=$1 AND expires_at > NOW() ORDER BY created_at`
	defer observeDB(ctx, "locks.list_by_resource")()
	rows, err := r.pool.QueryContext(ctx, q, resourcePath)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var result []Lock
	for rows.Next() {
		var l Lock
		if err := rows.Scan(&l.ID, &l.Token, &l.ResourcePath, &l.UserID, &l.LockScope, &l.LockType, &l.Depth, &l.OwnerInfo, &l.TimeoutSeconds, &l.CreatedAt, &l.ExpiresAt); err != nil {
			return nil, err
		}
		result = append(result, l)
	}
	return result, rows.Err()
}

func (r *lockRepo) ListByResources(ctx context.Context, paths []string) ([]Lock, error) {
	if len(paths) == 0 {
		return nil, nil
	}
	const q = `SELECT id, token, resource_path, user_id, lock_scope, lock_type, depth, owner_info, timeout_seconds, created_at, expires_at FROM locks WHERE resource_path = ANY($1) AND expires_at > NOW() ORDER BY created_at`
	defer observeDB(ctx, "locks.list_by_resources")()
	rows, err := r.pool.QueryContext(ctx, q, pq.Array(paths))
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var result []Lock
	for rows.Next() {
		var l Lock
		if err := rows.Scan(&l.ID, &l.Token, &l.ResourcePath, &l.UserID, &l.LockScope, &l.LockType, &l.Depth, &l.OwnerInfo, &l.TimeoutSeconds, &l.CreatedAt, &l.ExpiresAt); err != nil {
			return nil, err
		}
		result = append(result, l)
	}
	return result, rows.Err()
}

func (r *lockRepo) ListByResourcePrefix(ctx context.Context, prefix string) ([]Lock, error) {
	const q = `SELECT id, token, resource_path, user_id, lock_scope, lock_type, depth, owner_info, timeout_seconds, created_at, expires_at FROM locks WHERE resource_path LIKE $1 ESCAPE '\' AND expires_at > NOW() ORDER BY created_at`
	defer observeDB(ctx, "locks.list_by_resource_prefix")()
	escaped := strings.NewReplacer(`\`, `\\`, `%`, `\%`, `_`, `\_`).Replace(prefix)
	rows, err := r.pool.QueryContext(ctx, q, escaped+"%")
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var result []Lock
	for rows.Next() {
		var l Lock
		if err := rows.Scan(&l.ID, &l.Token, &l.ResourcePath, &l.UserID, &l.LockScope, &l.LockType, &l.Depth, &l.OwnerInfo, &l.TimeoutSeconds, &l.CreatedAt, &l.ExpiresAt); err != nil {
			return nil, err
		}
		result = append(result, l)
	}
	return result, rows.Err()
}

func (r *lockRepo) MoveResourcePath(ctx context.Context, fromPath, toPath string) error {
	if fromPath == toPath {
		return nil
	}
	defer observeDB(ctx, "locks.move_resource_path")()

	tx, err := r.pool.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	const deleteQ = `DELETE FROM locks WHERE resource_path=$1 AND expires_at > NOW()`
	if _, err := tx.ExecContext(ctx, deleteQ, toPath); err != nil {
		return err
	}

	const moveQ = `UPDATE locks SET resource_path=$1 WHERE resource_path=$2 AND expires_at > NOW()`
	if _, err := tx.ExecContext(ctx, moveQ, toPath, fromPath); err != nil {
		return err
	}

	return tx.Commit()
}

func (r *lockRepo) DeleteByResourcePath(ctx context.Context, resourcePath string) error {
	const q = `DELETE FROM locks WHERE resource_path=$1`
	defer observeDB(ctx, "locks.delete_by_resource_path")()
	_, err := r.pool.ExecContext(ctx, q, resourcePath)
	return err
}

func (r *lockRepo) Delete(ctx context.Context, token string) error {
	const q = `DELETE FROM locks WHERE token=$1`
	defer observeDB(ctx, "locks.delete")()
	_, err := r.pool.ExecContext(ctx, q, token)
	return err
}

func (r *lockRepo) DeleteExpired(ctx context.Context) (int64, error) {
	const q = `DELETE FROM locks WHERE expires_at < NOW()`
	defer observeDB(ctx, "locks.delete_expired")()
	res, err := r.pool.ExecContext(ctx, q)
	if err != nil {
		return 0, err
	}
	rows, err := res.RowsAffected()
	if err != nil {
		return 0, err
	}
	return rows, nil
}

func (r *lockRepo) Refresh(ctx context.Context, token string, newTimeout int, newExpiry time.Time) (*Lock, error) {
	const q = `UPDATE locks SET timeout_seconds=$1, expires_at=$2 WHERE token=$3 AND expires_at > NOW() RETURNING id, token, resource_path, user_id, lock_scope, lock_type, depth, owner_info, timeout_seconds, created_at, expires_at`
	defer observeDB(ctx, "locks.refresh")()
	var l Lock
	if err := r.pool.QueryRowContext(ctx, q, newTimeout, newExpiry, token).Scan(&l.ID, &l.Token, &l.ResourcePath, &l.UserID, &l.LockScope, &l.LockType, &l.Depth, &l.OwnerInfo, &l.TimeoutSeconds, &l.CreatedAt, &l.ExpiresAt); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, ErrNotFound
		}
		return nil, err
	}
	return &l, nil
}

// aclRepo implements ACLRepository.
type aclRepo struct {
	pool *sql.DB
}

func (r *aclRepo) SetACL(ctx context.Context, resourcePath string, entries []ACLEntry) error {
	defer observeDB(ctx, "acl.set_acl")()

	tx, err := r.pool.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if err := lockACLPathsTx(ctx, tx, davStatePaths(resourcePath)...); err != nil {
		return err
	}
	if err := setACLTx(ctx, tx, resourcePath, entries); err != nil {
		return err
	}
	return tx.Commit()
}

// UpdateACL derives the resource's new ACL from its current one without the
// read and the write being able to interleave with another writer's.
func (r *aclRepo) UpdateACL(ctx context.Context, resourcePath string, mutate func([]ACLEntry) ([]ACLEntry, error)) error {
	defer observeDB(ctx, "acl.update_acl")()

	tx, err := r.pool.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	// Locking before the snapshot read is the whole point: mutate derives the new
	// ACL from that read, so a hold that only starts at the write lets a caller
	// write back a stale ACL.
	if err := lockACLPathsTx(ctx, tx, davStatePaths(resourcePath)...); err != nil {
		return err
	}

	// setACLTx replaces every stored spelling of the resource, so mutate has to
	// see the entries of all of them: one it was not shown would be dropped.
	const q = `SELECT id, resource_path, principal_href, is_grant, privilege, ace_order, created_at FROM acl_entries WHERE resource_path = ANY($1) ORDER BY ace_order, resource_path, id`
	rows, err := tx.QueryContext(ctx, q, pq.Array(davStatePaths(resourcePath)))
	if err != nil {
		return err
	}
	current, err := scanACLEntries(rows)
	rows.Close()
	if err != nil {
		return err
	}

	next, err := mutate(current)
	if err != nil {
		return err
	}
	if err := setACLTx(ctx, tx, resourcePath, next); err != nil {
		return err
	}
	return tx.Commit()
}

// RevokePrincipalGrants revokes a share across the whole collection. Member
// grants are evaluated before the collection's, so one left behind would keep
// that resource readable after a collection-only revocation. They carry no
// origin, so every member grant the principal holds goes, including any the
// owner set on a resource directly.
func (r *aclRepo) RevokePrincipalGrants(ctx context.Context, collectionPath, principalHref string, collectionPrivileges []string) error {
	defer observeDB(ctx, "acl.revoke_principal_grants")()

	tx, err := r.pool.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	// Holding the collection exclusively excludes every ACL write and member
	// write beneath it too: each of those holds its collection exclusively.
	if err := lockACLPathsTx(ctx, tx, collectionPath); err != nil {
		return err
	}

	memberPattern := likeEscape(collectionPath) + "/%"
	principals := pq.Array(principalHrefSpellings(principalHref))
	privileges := pq.Array(collectionPrivileges)

	const scope = `is_grant AND principal_href = ANY($1) AND ((resource_path=$2 AND privilege = ANY($4)) OR resource_path LIKE $3 ESCAPE '\')`
	const listQ = `SELECT DISTINCT resource_path FROM acl_entries WHERE ` + scope + ` ORDER BY resource_path`
	rows, err := tx.QueryContext(ctx, listQ, principals, collectionPath, memberPattern, privileges)
	if err != nil {
		return err
	}
	var affected []string
	for rows.Next() {
		var resourcePath string
		if err := rows.Scan(&resourcePath); err != nil {
			rows.Close()
			return err
		}
		affected = append(affected, resourcePath)
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return err
	}
	if err := rows.Close(); err != nil {
		return err
	}
	if len(affected) == 0 {
		return tx.Commit()
	}

	const deleteQ = `DELETE FROM acl_entries WHERE ` + scope
	if _, err := tx.ExecContext(ctx, deleteQ, principals, collectionPath, memberPattern, privileges); err != nil {
		return err
	}

	if err := touchACLDependentStates(ctx, tx, affected); err != nil {
		return err
	}
	return tx.Commit()
}

// principalHrefSpellings returns the stored forms of a principal href that the
// ACL evaluator folds together, so a SQL predicate matches the same rows the
// in-memory comparison does. Only the trailing slash varies in practice: every
// writer canonicalizes the rest before storing it.
func principalHrefSpellings(principalHref string) []string {
	trimmed := strings.TrimSuffix(principalHref, "/")
	if trimmed == "" {
		return []string{principalHref}
	}
	if trimmed == principalHref {
		return []string{principalHref, principalHref + "/"}
	}
	return []string{principalHref, trimmed}
}

// lockACLPathsTx serializes an ACL transaction against the DAV path space. The
// paths are targets, and so is the collection directly containing a member:
// changing a member's ACL bumps that collection's CTag and sync token, and a
// write guarded against the member's ACL holds the collection as well.
func lockACLPathsTx(ctx context.Context, tx *sql.Tx, resourcePaths ...string) error {
	return acquireDAVPathLocks(ctx, tx, resourcePaths...)
}

// setACLTx replaces the ACL of every stored spelling of one resource identity.
// The caller holds the DAV path locks for those paths already: they are part of
// the transaction's exclusive set, which one ordered pass has to settle, and
// anything the caller read to derive these entries needs the hold to have
// started before the read.
func setACLTx(ctx context.Context, tx *sql.Tx, resourcePath string, entries []ACLEntry) error {
	statePaths := davStatePaths(resourcePath)

	type aclIdentity struct {
		principalHref string
		isGrant       bool
		privilege     string
	}

	const existingQ = `SELECT id, resource_path, principal_href, is_grant, privilege, ace_order, created_at FROM acl_entries WHERE resource_path = ANY($1) ORDER BY ace_order, resource_path, id`
	rows, err := tx.QueryContext(ctx, existingQ, pq.Array(statePaths))
	if err != nil {
		return err
	}
	defer rows.Close()

	existingCreatedAt := make(map[aclIdentity]time.Time)
	for rows.Next() {
		var (
			id            int64
			existingPath  string
			principalHref string
			isGrant       bool
			privilege     string
			position      int
			createdAt     time.Time
		)
		if err := rows.Scan(&id, &existingPath, &principalHref, &isGrant, &privilege, &position, &createdAt); err != nil {
			return err
		}
		existingCreatedAt[aclIdentity{
			principalHref: principalHref,
			isGrant:       isGrant,
			privilege:     privilege,
		}] = createdAt
	}
	if err := rows.Err(); err != nil {
		return err
	}

	// Replace every historical spelling of the resource identity so a legacy
	// extension-suffixed ACE cannot survive a revocation on the canonical URL.
	const deleteQ = `DELETE FROM acl_entries WHERE resource_path = ANY($1)`
	if _, err := tx.ExecContext(ctx, deleteQ, pq.Array(statePaths)); err != nil {
		return err
	}

	// Insert the new entries
	const insertQ = `INSERT INTO acl_entries (resource_path, principal_href, is_grant, privilege, ace_order, created_at) VALUES ($1, $2, $3, $4, $5, $6)`
	for _, entry := range entries {
		createdAt := entry.CreatedAt
		if createdAt.IsZero() {
			if preserved, ok := existingCreatedAt[aclIdentity{
				principalHref: entry.PrincipalHref,
				isGrant:       entry.IsGrant,
				privilege:     entry.Privilege,
			}]; ok {
				createdAt = preserved
			} else {
				createdAt = time.Now().UTC()
			}
		}
		if _, err := tx.ExecContext(ctx, insertQ, resourcePath, entry.PrincipalHref, entry.IsGrant, entry.Privilege, entry.Position, createdAt); err != nil {
			return err
		}
	}

	if err := touchACLDependentState(ctx, tx, resourcePath); err != nil {
		return err
	}

	return nil
}

func touchACLDependentState(ctx context.Context, tx *sql.Tx, resourcePath string) error {
	return touchACLDependentStates(ctx, tx, []string{resourcePath})
}

// touchACLDependentStates marks every resource whose ACL changed as modified,
// so CTag and sync-collection clients re-read what they may access. It issues
// one CTag bump and one member update per collection however many of its
// resources changed; a change on the collection itself touches every member.
func touchACLDependentStates(ctx context.Context, tx *sql.Tx, resourcePaths []string) error {
	type collectionKey struct {
		kind string
		id   int64
	}
	type touched struct {
		whole bool
		names []string
	}
	var order []collectionKey
	byCollection := make(map[collectionKey]*touched)
	for _, resourcePath := range resourcePaths {
		kind, id, resourceName, isCollection, ok := aclResourceIdentity(resourcePath)
		if !ok {
			continue
		}
		key := collectionKey{kind: kind, id: id}
		entry, seen := byCollection[key]
		if !seen {
			entry = &touched{}
			byCollection[key] = entry
			order = append(order, key)
		}
		if isCollection {
			entry.whole = true
			continue
		}
		ext := ".ics"
		if kind == "addressbook" {
			ext = ".vcf"
		}
		canonical, alternate := aclResourceNameCandidates(resourceName, ext)
		entry.names = append(entry.names, canonical, alternate)
	}

	for _, key := range order {
		entry := byCollection[key]
		collectionTable, memberTable, memberCollection := "calendars", "events", "calendar_id"
		if key.kind == "addressbook" {
			collectionTable, memberTable, memberCollection = "address_books", "contacts", "address_book_id"
		}
		if _, err := tx.ExecContext(ctx, `UPDATE `+collectionTable+` SET ctag = ctag + 1, updated_at = NOW() WHERE id = $1`, key.id); err != nil {
			return err
		}
		if entry.whole {
			if _, err := tx.ExecContext(ctx, `UPDATE `+memberTable+` SET last_modified = NOW() WHERE `+memberCollection+` = $1`, key.id); err != nil {
				return err
			}
			continue
		}
		slices.Sort(entry.names)
		names := slices.Compact(entry.names)
		if _, err := tx.ExecContext(ctx, `UPDATE `+memberTable+` SET last_modified = NOW() WHERE `+memberCollection+` = $1 AND resource_name = ANY($2)`, key.id, pq.Array(names)); err != nil {
			return err
		}
	}
	return nil
}

func aclResourceIdentity(resourcePath string) (string, int64, string, bool, bool) {
	cleanPath := path.Clean(strings.TrimSpace(resourcePath))
	for _, candidate := range []struct {
		prefix string
		kind   string
	}{
		{prefix: "/dav/calendars/", kind: "calendar"},
		{prefix: "/dav/addressbooks/", kind: "addressbook"},
	} {
		if !strings.HasPrefix(cleanPath, candidate.prefix) {
			continue
		}
		trimmed := strings.TrimPrefix(cleanPath, candidate.prefix)
		segment := strings.Split(trimmed, "/")[0]
		if segment == "" {
			return "", 0, "", false, false
		}
		id, err := strconv.ParseInt(segment, 10, 64)
		if err != nil {
			return "", 0, "", false, false
		}
		if len(strings.Split(trimmed, "/")) == 1 {
			return candidate.kind, id, "", true, true
		}
		resourceName := strings.Split(trimmed, "/")[1]
		if resourceName == "" {
			return "", 0, "", false, false
		}
		return candidate.kind, id, resourceName, false, true
	}
	return "", 0, "", false, false
}

func aclResourceNameCandidates(resourceName, ext string) (string, string) {
	resourceName = strings.TrimSpace(resourceName)
	if strings.EqualFold(path.Ext(resourceName), ext) {
		return resourceName, strings.TrimSuffix(resourceName, path.Ext(resourceName))
	}
	return resourceName, resourceName + ext
}

func (r *aclRepo) ListByResource(ctx context.Context, resourcePath string) ([]ACLEntry, error) {
	const q = `SELECT id, resource_path, principal_href, is_grant, privilege, ace_order, created_at FROM acl_entries WHERE resource_path=$1 ORDER BY ace_order, id`
	defer observeDB(ctx, "acl.list_by_resource")()
	rows, err := r.pool.QueryContext(ctx, q, resourcePath)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var result []ACLEntry
	for rows.Next() {
		var e ACLEntry
		if err := rows.Scan(&e.ID, &e.ResourcePath, &e.PrincipalHref, &e.IsGrant, &e.Privilege, &e.Position, &e.CreatedAt); err != nil {
			return nil, err
		}
		result = append(result, e)
	}
	return result, rows.Err()
}

func (r *aclRepo) ListByResources(ctx context.Context, resourcePaths []string) ([]ACLEntry, error) {
	if len(resourcePaths) == 0 {
		return []ACLEntry{}, nil
	}
	const q = `SELECT id, resource_path, principal_href, is_grant, privilege, ace_order, created_at FROM acl_entries WHERE resource_path = ANY($1) ORDER BY resource_path, ace_order, id`
	defer observeDB(ctx, "acl.list_by_resources")()
	rows, err := r.pool.QueryContext(ctx, q, pq.Array(resourcePaths))
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return scanACLEntries(rows)
}

func (r *aclRepo) ListByResourcesAndPrincipals(ctx context.Context, resourcePaths, principalHrefs []string) ([]ACLEntry, error) {
	if len(resourcePaths) == 0 || len(principalHrefs) == 0 {
		return []ACLEntry{}, nil
	}
	const q = `SELECT id, resource_path, principal_href, is_grant, privilege, ace_order, created_at FROM acl_entries WHERE resource_path = ANY($1) AND principal_href = ANY($2) ORDER BY resource_path, ace_order, id`
	defer observeDB(ctx, "acl.list_by_resources_and_principals")()
	rows, err := r.pool.QueryContext(ctx, q, pq.Array(resourcePaths), pq.Array(principalHrefs))
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return scanACLEntries(rows)
}

func scanACLEntries(rows *sql.Rows) ([]ACLEntry, error) {
	var result []ACLEntry
	for rows.Next() {
		var entry ACLEntry
		if err := rows.Scan(&entry.ID, &entry.ResourcePath, &entry.PrincipalHref, &entry.IsGrant, &entry.Privilege, &entry.Position, &entry.CreatedAt); err != nil {
			return nil, err
		}
		result = append(result, entry)
	}
	return result, rows.Err()
}

func (r *aclRepo) ListByPrincipal(ctx context.Context, principalHref string) ([]ACLEntry, error) {
	const q = `SELECT id, resource_path, principal_href, is_grant, privilege, ace_order, created_at FROM acl_entries WHERE principal_href=$1 ORDER BY resource_path, ace_order, id`
	defer observeDB(ctx, "acl.list_by_principal")()
	rows, err := r.pool.QueryContext(ctx, q, principalHref)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var result []ACLEntry
	for rows.Next() {
		var e ACLEntry
		if err := rows.Scan(&e.ID, &e.ResourcePath, &e.PrincipalHref, &e.IsGrant, &e.Privilege, &e.Position, &e.CreatedAt); err != nil {
			return nil, err
		}
		result = append(result, e)
	}
	return result, rows.Err()
}

func (r *aclRepo) HasPrivilege(ctx context.Context, resourcePath, principalHref, privilege string) (bool, error) {
	const q = `
SELECT COALESCE((
    SELECT is_grant
    FROM acl_entries
    WHERE resource_path=$1 AND principal_href=$2 AND privilege=$3
    ORDER BY ace_order, id
    LIMIT 1
), FALSE)
`
	defer observeDB(ctx, "acl.has_privilege")()
	var exists bool
	if err := r.pool.QueryRowContext(ctx, q, resourcePath, principalHref, privilege).Scan(&exists); err != nil {
		return false, err
	}
	return exists, nil
}

func (r *aclRepo) Delete(ctx context.Context, resourcePath string) error {
	const q = `DELETE FROM acl_entries WHERE resource_path=$1`
	defer observeDB(ctx, "acl.delete")()
	_, err := r.pool.ExecContext(ctx, q, resourcePath)
	return err
}

func (r *aclRepo) MoveResourcePath(ctx context.Context, fromPath, toPath string) error {
	if fromPath == toPath {
		return nil
	}
	defer observeDB(ctx, "acl.move_resource_path")()

	tx, err := r.pool.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	const deleteQ = `DELETE FROM acl_entries WHERE resource_path=$1`
	if _, err := tx.ExecContext(ctx, deleteQ, toPath); err != nil {
		return err
	}

	const moveQ = `UPDATE acl_entries SET resource_path=$1 WHERE resource_path=$2`
	if _, err := tx.ExecContext(ctx, moveQ, toPath, fromPath); err != nil {
		return err
	}

	return tx.Commit()
}

// EnsureDefaultCollections creates baseline calendar and address book when absent.
func (s *Store) EnsureDefaultCollections(ctx context.Context, userID int64) error {
	if err := s.ensureDefaultCalendar(ctx, userID); err != nil {
		return err
	}
	if err := s.ensureDefaultAddressBook(ctx, userID); err != nil {
		return err
	}
	return nil
}

func (s *Store) ensureDefaultCalendar(ctx context.Context, userID int64) error {
	defer observeDB(ctx, "calendars.ensure_default")()

	tx, err := s.pool.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	// Serialize concurrent attempts for the same user
	if _, err := tx.ExecContext(ctx, `SELECT pg_advisory_xact_lock($1)`, userID); err != nil {
		return err
	}

	var exists bool
	const checkQuery = `SELECT EXISTS (SELECT 1 FROM calendars WHERE user_id=$1)`
	if err := tx.QueryRowContext(ctx, checkQuery, userID).Scan(&exists); err != nil {
		return err
	}
	if exists {
		return tx.Commit()
	}

	if _, err := tx.ExecContext(ctx, `INSERT INTO calendars (user_id, name) VALUES ($1, 'Default')`, userID); err != nil {
		return err
	}

	return tx.Commit()
}

func (s *Store) ensureDefaultAddressBook(ctx context.Context, userID int64) error {
	defer observeDB(ctx, "address_books.ensure_default")()

	tx, err := s.pool.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	if _, err := tx.ExecContext(ctx, `SELECT pg_advisory_xact_lock($1)`, userID); err != nil {
		return err
	}

	var exists bool
	const checkQuery = `SELECT EXISTS (SELECT 1 FROM address_books WHERE user_id=$1)`
	if err := tx.QueryRowContext(ctx, checkQuery, userID).Scan(&exists); err != nil {
		return err
	}
	if exists {
		return tx.Commit()
	}

	if _, err := tx.ExecContext(ctx, `INSERT INTO address_books (user_id, name) VALUES ($1, 'Contacts')`, userID); err != nil {
		return err
	}

	return tx.Commit()
}

// Now returns a UTC timestamp to keep updates consistent.
func Now() time.Time {
	return time.Now().UTC()
}

type rowScanner func(dest ...any) error

func nullableString(value sql.NullString) *string {
	if !value.Valid {
		return nil
	}
	v := value.String
	return &v
}

// nullableStringArray keeps the distinction between an unset column and an empty
// list: an empty Go slice would otherwise round-trip as '{}', which reads as a
// collection that accepts nothing rather than one carrying no restriction.
func nullableStringArray(values []string) any {
	if values == nil {
		return nil
	}
	return pq.StringArray(values)
}

func nullableTime(value sql.NullTime) *time.Time {
	if !value.Valid {
		return nil
	}
	v := value.Time
	return &v
}

func scanEvent(scan rowScanner) (Event, error) {
	var ev Event
	var summary sql.NullString
	var description sql.NullString
	var location sql.NullString
	var dtstart sql.NullTime
	var dtend sql.NullTime
	if err := scan(&ev.ID, &ev.CalendarID, &ev.UID, &ev.ResourceName, &ev.RawICAL, &ev.ETag, &summary, &description, &location, &dtstart, &dtend, &ev.AllDay, &ev.LastModified); err != nil {
		return Event{}, err
	}
	ev.Summary = nullableString(summary)
	ev.Description = nullableString(description)
	ev.Location = nullableString(location)
	ev.DTStart = nullableTime(dtstart)
	ev.DTEnd = nullableTime(dtend)
	return ev, nil
}

func scanContact(scan rowScanner) (Contact, error) {
	var c Contact
	var displayName sql.NullString
	var primaryEmail sql.NullString
	var birthday sql.NullTime
	if err := scan(&c.ID, &c.AddressBookID, &c.UID, &c.ResourceName, &c.RawVCard, &c.ETag, &displayName, &primaryEmail, &birthday, &c.LastModified); err != nil {
		return Contact{}, err
	}
	c.DisplayName = nullableString(displayName)
	c.PrimaryEmail = nullableString(primaryEmail)
	c.Birthday = nullableTime(birthday)
	if c.Birthday != nil && c.Birthday.Year() == legacyNoYearBirthdayYear {
		normalized := time.Date(NoYearBirthdayYear, c.Birthday.Month(), c.Birthday.Day(), 0, 0, 0, 0, time.UTC)
		c.Birthday = &normalized
	}
	return c, nil
}

func scanAppPassword(scan rowScanner) (AppPassword, error) {
	var t AppPassword
	var digestMD5HA1 sql.NullString
	var digestSHA256HA1 sql.NullString
	var expiresAt sql.NullTime
	var revokedAt sql.NullTime
	var lastUsedAt sql.NullTime
	if err := scan(&t.ID, &t.UserID, &t.Label, &t.TokenHash, &digestMD5HA1, &digestSHA256HA1, &t.CreatedAt, &expiresAt, &revokedAt, &lastUsedAt); err != nil {
		return AppPassword{}, err
	}
	t.ExpiresAt = nullableTime(expiresAt)
	t.DigestMD5HA1 = nullableString(digestMD5HA1)
	t.DigestSHA256HA1 = nullableString(digestSHA256HA1)
	t.RevokedAt = nullableTime(revokedAt)
	t.LastUsedAt = nullableTime(lastUsedAt)
	return t, nil
}

func scanSession(scan rowScanner) (Session, error) {
	var s Session
	var userAgent sql.NullString
	var ipAddress sql.NullString
	if err := scan(&s.ID, &s.UserID, &userAgent, &ipAddress, &s.CreatedAt, &s.ExpiresAt, &s.LastSeenAt); err != nil {
		return Session{}, err
	}
	s.UserAgent = nullableString(userAgent)
	s.IPAddress = nullableString(ipAddress)
	return s, nil
}

// parseICalFields extracts summary, description, location, dtstart, dtend, and
// all_day from raw iCalendar data.
func parseICalFields(raw string) (summary, description, location *string, dtstart, dtend *time.Time, allDay bool) {
	component := icalpkg.PrimaryVEventComponent(raw)
	if component == nil {
		return nil, nil, nil, nil, nil, false
	}
	if property, ok := icalpkg.ComponentProperty(component, "SUMMARY"); ok {
		summary = util.StrPtr(unescapeICalValue(property.Value))
	}
	if property, ok := icalpkg.ComponentProperty(component, "DESCRIPTION"); ok {
		description = util.StrPtr(unescapeICalValue(property.Value))
	}
	if property, ok := icalpkg.ComponentProperty(component, "LOCATION"); ok {
		location = util.StrPtr(unescapeICalValue(property.Value))
	}
	if property, ok := icalpkg.ComponentProperty(component, "DTSTART"); ok {
		if parsed, ok := icalpkg.ParsePropertyDateTimeLocal(property.KeyPart, property.Value); ok {
			dtstart = &parsed
			allDay = len(strings.TrimSpace(property.Value)) == len("20060102") || icalpkg.PropertyParamEquals(property.KeyPart, "VALUE", "DATE")
		}
	}
	if property, ok := icalpkg.ComponentProperty(component, "DTEND"); ok {
		if parsed, ok := icalpkg.ParsePropertyDateTimeLocal(property.KeyPart, property.Value); ok {
			dtend = &parsed
		}
	}
	return summary, description, location, dtstart, dtend, allDay
}

// recurrenceBoundsFromICal computes the recurrence_start and recurrence_until
// values persisted for an event. The start is the earliest known recurring
// instance start, or a low sentinel when it cannot be computed safely. The until
// is the end of the last recurring instance, a far-future sentinel for unbounded
// or not-confidently-bounded rules, or nil for non-recurring events. Neither
// value may exclude a row that in-memory recurrence expansion would match.
func recurrenceBoundsFromICal(ical string) (*time.Time, *time.Time) {
	bounds := icalpkg.ConservativeRecurrenceBounds(ical)
	if !bounds.Recurring {
		return nil, nil
	}
	start := bounds.Start
	until := bounds.Until
	if bounds.StartUnknown {
		sentinel := icalpkg.RecurrenceStartSentinel
		start = &sentinel
	}
	if bounds.UntilUnknown {
		sentinel := icalpkg.RecurrenceUntilSentinel
		until = &sentinel
	}
	return start, until
}

func unescapeICalValue(s string) string {
	s = strings.ReplaceAll(s, "\\n", "\n")
	s = strings.ReplaceAll(s, "\\N", "\n")
	s = strings.ReplaceAll(s, "\\,", ",")
	s = strings.ReplaceAll(s, "\\;", ";")
	s = strings.ReplaceAll(s, "\\\\", "\\")
	return s
}

// parseVCardFields extracts display_name, primary_email, and birthday from raw
// vCard data. Each comes from the first FN, EMAIL and BDAY, which is what the
// contact form shows and a structured edit rewrites.
func parseVCardFields(raw string) (*string, *string, *time.Time) {
	var displayName, primaryEmail *string
	var birthday *time.Time
	seenBirthday := false

	for _, contentLine := range vcard.ContentLines(raw) {
		line, ok := vcard.ParseLine(contentLine)
		if !ok {
			continue
		}
		switch strings.ToUpper(line.Name) {
		case "FN":
			if displayName == nil {
				displayName = util.StrPtr(truncateUTF8(vcard.UnescapeText(line.Value), maxDisplayNameOctets))
			}
		case "EMAIL":
			if primaryEmail == nil {
				primaryEmail = util.StrPtr(strings.TrimSpace(line.Value))
			}
		case "BDAY":
			if !seenBirthday {
				seenBirthday = true
				birthday = parseVCardBirthday(line)
			}
		}
	}

	return displayName, primaryEmail, birthday
}

// truncateUTF8 cuts s to at most n octets without splitting a character.
func truncateUTF8(s string, n int) string {
	if len(s) <= n {
		return s
	}
	for n > 0 && !utf8.RuneStart(s[n]) {
		n--
	}
	return s[:n]
}

// parseVCardBirthday reads a BDAY line as vcard.ParseDateProperty does. A
// birthday without a year is stored in NoYearBirthdayYear.
func parseVCardBirthday(line vcard.Line) *time.Time {
	date, ok := vcard.ParseDateProperty(line)
	if !ok {
		return nil
	}
	year := date.Year
	if !date.HasYear {
		year = NoYearBirthdayYear
	}
	birthday := time.Date(year, date.Month, date.Day, 0, 0, 0, 0, time.UTC)
	return &birthday
}
