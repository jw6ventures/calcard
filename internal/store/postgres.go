package store

import (
	"context"
	"database/sql"
	"errors"
	"path"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/jw6ventures/calcard/internal/util"
	"github.com/lib/pq"
)

// userRepo implements UserRepository.
type userRepo struct {
	pool *sql.DB
}

func (r *userRepo) UpsertOAuthUser(ctx context.Context, subject, email string) (*User, error) {
	const q = `
INSERT INTO users (oauth_subject, primary_email)
VALUES ($1, $2)
ON CONFLICT (oauth_subject) DO UPDATE SET
        primary_email = EXCLUDED.primary_email,
        last_login_at = NOW()
RETURNING id, oauth_subject, primary_email, created_at, last_login_at
`
	defer observeDB(ctx, "users.upsert_oauth")()
	row := r.pool.QueryRowContext(ctx, q, subject, email)
	var u User
	if err := row.Scan(&u.ID, &u.OAuthSubject, &u.PrimaryEmail, &u.CreatedAt, &u.LastLoginAt); err != nil {
		return nil, err
	}
	return &u, nil
}

func (r *userRepo) GetByID(ctx context.Context, id int64) (*User, error) {
	const q = `SELECT id, oauth_subject, primary_email, created_at, last_login_at FROM users WHERE id=$1`
	defer observeDB(ctx, "users.get_by_id")()
	var u User
	if err := r.pool.QueryRowContext(ctx, q, id).Scan(&u.ID, &u.OAuthSubject, &u.PrimaryEmail, &u.CreatedAt, &u.LastLoginAt); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, nil
		}
		return nil, err
	}
	return &u, nil
}

func (r *userRepo) GetByEmail(ctx context.Context, email string) (*User, error) {
	const q = `SELECT id, oauth_subject, primary_email, created_at, last_login_at FROM users WHERE primary_email=$1`
	defer observeDB(ctx, "users.get_by_email")()
	var u User
	if err := r.pool.QueryRowContext(ctx, q, email).Scan(&u.ID, &u.OAuthSubject, &u.PrimaryEmail, &u.CreatedAt, &u.LastLoginAt); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, nil
		}
		return nil, err
	}
	return &u, nil
}

func (r *userRepo) ListActive(ctx context.Context) ([]User, error) {
	const q = `SELECT id, oauth_subject, primary_email, created_at, last_login_at FROM users WHERE last_login_at IS NOT NULL ORDER BY primary_email`
	defer observeDB(ctx, "users.list_active")()
	rows, err := r.pool.QueryContext(ctx, q)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var users []User
	for rows.Next() {
		var u User
		if err := rows.Scan(&u.ID, &u.OAuthSubject, &u.PrimaryEmail, &u.CreatedAt, &u.LastLoginAt); err != nil {
			return nil, err
		}
		users = append(users, u)
	}
	return users, rows.Err()
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
	privilegeList := sqlLiteralList(privileges...)
	principals := aclPrincipalListExpr(userParam)
	return `(
       NOT EXISTS (
           SELECT 1 FROM acl_entries d
           WHERE d.resource_path = '/dav/calendars/' || c.id::text
             AND d.principal_href IN ` + principals + `
             AND d.is_grant = FALSE
             AND d.privilege IN (` + privilegeList + `)
       )
       AND EXISTS (
           SELECT 1 FROM acl_entries g
           WHERE g.resource_path = '/dav/calendars/' || c.id::text
             AND g.principal_href IN ` + principals + `
             AND g.is_grant = TRUE
             AND g.privilege IN (` + privilegeList + `)
       )
   )`
}

func aclPrincipalListExpr(userParam string) string {
	return "('DAV:all', 'DAV:authenticated', '/dav/principals/' || " + userParam + "::text || '/')"
}

func calendarEventACLPathListExpr() string {
	return `(
            '/dav/calendars/' || c.id::text || '/' || e.resource_name,
            '/dav/calendars/' || c.id::text || '/' || regexp_replace(e.resource_name, '\.ics$', ''),
            '/dav/calendars/' || c.id::text || '/' || regexp_replace(e.resource_name, '\.ics$', '') || '.ics'
        )`
}

func calendarEventACLDenyExpr(userParam string, privileges ...string) string {
	privilegeList := sqlLiteralList(privileges...)
	return `EXISTS (
           SELECT 1 FROM acl_entries d
           WHERE d.resource_path IN ` + calendarEventACLPathListExpr() + `
             AND d.principal_href IN ` + aclPrincipalListExpr(userParam) + `
             AND d.is_grant = FALSE
             AND d.privilege IN (` + privilegeList + `)
       )`
}

func calendarEventACLGrantExpr(userParam string, privileges ...string) string {
	privilegeList := sqlLiteralList(privileges...)
	return `EXISTS (
           SELECT 1 FROM acl_entries g
           WHERE g.resource_path IN ` + calendarEventACLPathListExpr() + `
             AND g.principal_href IN ` + aclPrincipalListExpr(userParam) + `
             AND g.is_grant = TRUE
             AND g.privilege IN (` + privilegeList + `)
       )`
}

func calendarEventACLAllowsExpr(userParam string, privileges ...string) string {
	return `(NOT ` + calendarEventACLDenyExpr(userParam, privileges...) + ` AND ` + calendarEventACLGrantExpr(userParam, privileges...) + `)`
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

func calendarObjectACLAnyAccessExpr(userParam string) string {
	return `EXISTS (
           SELECT 1 FROM events e
           WHERE e.calendar_id = c.id
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
	const q = `SELECT id, user_id, name, slug, description, timezone, color, ctag, created_at, updated_at FROM calendars WHERE user_id=$1 ORDER BY created_at`
	defer observeDB(ctx, "calendars.list_by_user")()
	rows, err := r.pool.QueryContext(ctx, q, userID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var result []Calendar
	for rows.Next() {
		var c Calendar
		var slug, description, timezone, color sql.NullString
		if err := rows.Scan(&c.ID, &c.UserID, &c.Name, &slug, &description, &timezone, &color, &c.CTag, &c.CreatedAt, &c.UpdatedAt); err != nil {
			return nil, err
		}
		c.Slug = nullableString(slug)
		c.Description = nullableString(description)
		c.Timezone = nullableString(timezone)
		c.Color = nullableString(color)
		result = append(result, c)
	}
	return result, rows.Err()
}

func (r *calendarRepo) GetByID(ctx context.Context, id int64) (*Calendar, error) {
	const q = `SELECT id, user_id, name, slug, description, timezone, color, ctag, created_at, updated_at FROM calendars WHERE id=$1`
	defer observeDB(ctx, "calendars.get_by_id")()
	var c Calendar
	var slug, description, timezone, color sql.NullString
	if err := r.pool.QueryRowContext(ctx, q, id).Scan(&c.ID, &c.UserID, &c.Name, &slug, &description, &timezone, &color, &c.CTag, &c.CreatedAt, &c.UpdatedAt); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, nil
		}
		return nil, err
	}
	c.Slug = nullableString(slug)
	c.Description = nullableString(description)
	c.Timezone = nullableString(timezone)
	c.Color = nullableString(color)
	return &c, nil
}

func (r *calendarRepo) ListAccessible(ctx context.Context, userID int64) ([]CalendarAccess, error) {
	q := `
SELECT c.id, c.user_id, c.name, c.slug, c.description, c.timezone, c.color, c.ctag, c.created_at, c.updated_at,
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
		var slug, description, timezone, color sql.NullString
		if err := rows.Scan(
			&c.ID, &c.UserID, &c.Name, &slug, &description, &timezone, &color, &c.CTag, &c.CreatedAt, &c.UpdatedAt, &c.OwnerEmail, &c.Shared,
			&c.Privileges.Read, &c.Privileges.ReadFreeBusy, &c.Privileges.Write, &c.Privileges.WriteContent, &c.Privileges.WriteProperties, &c.Privileges.Bind, &c.Privileges.Unbind,
		); err != nil {
			return nil, err
		}
		c.Slug = nullableString(slug)
		c.Description = nullableString(description)
		c.Timezone = nullableString(timezone)
		c.Color = nullableString(color)
		c.PrivilegesResolved = true
		c.Privileges = c.Privileges.Normalized()
		c.Editor = c.Privileges.AllowsEventEditing()
		result = append(result, c)
	}
	return result, rows.Err()
}

func (r *calendarRepo) GetAccessible(ctx context.Context, calendarID, userID int64) (*CalendarAccess, error) {
	q := `
SELECT c.id, c.user_id, c.name, c.slug, c.description, c.timezone, c.color, c.ctag, c.created_at, c.updated_at,
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
	var slug, description, timezone, color sql.NullString
	if err := r.pool.QueryRowContext(ctx, q, calendarID, userID).Scan(
		&c.ID, &c.UserID, &c.Name, &slug, &description, &timezone, &color, &c.CTag, &c.CreatedAt, &c.UpdatedAt, &c.OwnerEmail, &c.Shared,
		&c.Privileges.Read, &c.Privileges.ReadFreeBusy, &c.Privileges.Write, &c.Privileges.WriteContent, &c.Privileges.WriteProperties, &c.Privileges.Bind, &c.Privileges.Unbind,
	); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, nil
		}
		return nil, err
	}
	c.Slug = nullableString(slug)
	c.Description = nullableString(description)
	c.Timezone = nullableString(timezone)
	c.Color = nullableString(color)
	c.PrivilegesResolved = true
	c.Privileges = c.Privileges.Normalized()
	c.Editor = c.Privileges.AllowsEventEditing()
	return &c, nil
}

func (r *calendarRepo) Create(ctx context.Context, cal Calendar) (*Calendar, error) {
	const q = `INSERT INTO calendars (user_id, name, slug, description, timezone, color) VALUES ($1, $2, $3, $4, $5, $6) RETURNING id, user_id, name, slug, description, timezone, color, ctag, created_at, updated_at`
	defer observeDB(ctx, "calendars.create")()
	row := r.pool.QueryRowContext(ctx, q, cal.UserID, cal.Name, cal.Slug, cal.Description, cal.Timezone, cal.Color)
	var created Calendar
	var slug, description, timezone, color sql.NullString
	if err := row.Scan(&created.ID, &created.UserID, &created.Name, &slug, &description, &timezone, &color, &created.CTag, &created.CreatedAt, &created.UpdatedAt); err != nil {
		return nil, err
	}
	created.Slug = nullableString(slug)
	created.Description = nullableString(description)
	created.Timezone = nullableString(timezone)
	created.Color = nullableString(color)
	return &created, nil
}

func (r *calendarRepo) Update(ctx context.Context, userID, id int64, name string, description, timezone *string) error {
	const q = `UPDATE calendars SET name=$1, description=$2, timezone=$3, updated_at=NOW() WHERE id=$4 AND user_id=$5`
	defer observeDB(ctx, "calendars.update")()
	res, err := r.pool.ExecContext(ctx, q, name, description, timezone, id, userID)
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

func (r *calendarRepo) UpdateProperties(ctx context.Context, id int64, name string, description, timezone *string) error {
	const q = `UPDATE calendars SET name=$1, description=$2, timezone=$3, updated_at=NOW() WHERE id=$4`
	defer observeDB(ctx, "calendars.update_properties")()
	res, err := r.pool.ExecContext(ctx, q, name, description, timezone, id)
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

func (r *eventRepo) Upsert(ctx context.Context, event Event) (*Event, error) {
	// Parse iCal to extract fields
	summary, dtstart, dtend, allDay := parseICalFields(event.RawICAL)
	if event.ResourceName == "" {
		event.ResourceName = event.UID
	}

	const q = `
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
`
	defer observeDB(ctx, "events.upsert")()
	row := r.pool.QueryRowContext(ctx, q, event.CalendarID, event.UID, event.ResourceName, event.RawICAL, event.ETag, summary, dtstart, dtend, allDay)
	ev, err := scanEvent(row.Scan)
	if err != nil {
		return nil, err
	}
	return &ev, nil
}

func (r *eventRepo) DeleteByUID(ctx context.Context, calendarID int64, uid string) error {
	const q = `DELETE FROM events WHERE calendar_id=$1 AND uid=$2`
	defer observeDB(ctx, "events.delete_by_uid")()
	_, err := r.pool.ExecContext(ctx, q, calendarID, uid)
	return err
}

func (r *eventRepo) GetByUID(ctx context.Context, calendarID int64, uid string) (*Event, error) {
	const q = `SELECT id, calendar_id, uid, resource_name, raw_ical, etag, summary, dtstart, dtend, all_day, last_modified FROM events WHERE calendar_id=$1 AND uid=$2`
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
	const q = `SELECT id, calendar_id, uid, resource_name, raw_ical, etag, summary, dtstart, dtend, all_day, last_modified FROM events WHERE calendar_id=$1 AND resource_name=$2`
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

func (r *eventRepo) ListByUIDs(ctx context.Context, calendarID int64, uids []string) ([]Event, error) {
	if len(uids) == 0 {
		return []Event{}, nil
	}
	const q = `SELECT id, calendar_id, uid, resource_name, raw_ical, etag, summary, dtstart, dtend, all_day, last_modified FROM events WHERE calendar_id=$1 AND uid = ANY($2)`
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
	const q = `SELECT id, calendar_id, uid, resource_name, raw_ical, etag, summary, dtstart, dtend, all_day, last_modified FROM events WHERE calendar_id=$1 ORDER BY last_modified DESC`
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

func (r *eventRepo) ListForCalendarPaginated(ctx context.Context, calendarID int64, limit, offset int) (*PaginatedResult[Event], error) {
	defer observeDB(ctx, "events.list_for_calendar_paginated")()

	// Get total count
	var totalCount int
	countQ := `SELECT COUNT(*) FROM events WHERE calendar_id=$1`
	if err := r.pool.QueryRowContext(ctx, countQ, calendarID).Scan(&totalCount); err != nil {
		return nil, err
	}

	const q = `SELECT id, calendar_id, uid, resource_name, raw_ical, etag, summary, dtstart, dtend, all_day, last_modified FROM events WHERE calendar_id=$1 ORDER BY last_modified DESC LIMIT $2 OFFSET $3`
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

func (r *eventRepo) ListModifiedSince(ctx context.Context, calendarID int64, since time.Time) ([]Event, error) {
	const q = `SELECT id, calendar_id, uid, resource_name, raw_ical, etag, summary, dtstart, dtend, all_day, last_modified FROM events WHERE calendar_id=$1 AND last_modified > $2 ORDER BY last_modified DESC`
	defer observeDB(ctx, "events.list_modified_since")()
	rows, err := r.pool.QueryContext(ctx, q, calendarID, since)
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
SELECT e.id, e.calendar_id, e.uid, e.resource_name, e.raw_ical, e.etag, e.summary, e.dtstart, e.dtend, e.all_day, e.last_modified
FROM events e
JOIN calendars c ON c.id = e.calendar_id
WHERE c.user_id = $1
   OR (
       c.user_id <> $1
       AND NOT ` + calendarEventACLDenyExpr("$1", "read", "all") + `
       AND (
           ` + calendarEventACLGrantExpr("$1", "read", "all") + `
           OR ` + calendarACLBooleanExpr("$1", "read", "all") + `
       )
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
		return tx.Commit()
	}

	const tombstoneQuery = `INSERT INTO deleted_resources (resource_type, collection_id, uid, resource_name) VALUES ('event', $1, $2, $3)`
	if _, err := tx.ExecContext(ctx, tombstoneQuery, fromCalendarID, uid, sourceResourceName); err != nil {
		return err
	}

	const incrementCtagQuery = `UPDATE calendars SET ctag = ctag + 1, updated_at = NOW() WHERE id = $1`
	if _, err := tx.ExecContext(ctx, incrementCtagQuery, fromCalendarID); err != nil {
		return err
	}

	return tx.Commit()
}

func (r *eventRepo) CopyToCalendar(ctx context.Context, fromCalendarID, toCalendarID int64, uid, destResourceName, newETag string) (*Event, error) {
	defer observeDB(ctx, "events.copy_to_calendar")()

	tx, err := r.pool.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()

	const selectQ = `SELECT id, calendar_id, uid, resource_name, raw_ical, etag, summary, dtstart, dtend, all_day, last_modified FROM events WHERE calendar_id=$1 AND uid=$2`
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
`
	insertRow := tx.QueryRowContext(ctx, insertQ, toCalendarID, src.UID, destResourceName, src.RawICAL, newETag, src.Summary, src.DTStart, src.DTEnd, src.AllDay)
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

func isAddressBookNameConflict(err error) bool {
	var pqErr *pq.Error
	return errors.As(err, &pqErr) && pqErr.Code == "23505" && pqErr.Constraint == "idx_address_books_user_name_lower"
}

func isContactResourceNameConflict(err error) bool {
	var pqErr *pq.Error
	return errors.As(err, &pqErr) && pqErr.Code == "23505" && pqErr.Constraint == "idx_contacts_resource_name"
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
	row := r.pool.QueryRowContext(ctx, q, contact.AddressBookID, contact.UID, contact.ResourceName, contact.RawVCard, contact.ETag, displayName, primaryEmail, birthday)
	c, err := scanContact(row.Scan)
	if err != nil {
		if isContactResourceNameConflict(err) {
			return nil, ErrConflict
		}
		return nil, err
	}
	return &c, nil
}

func (r *contactRepo) DeleteByUID(ctx context.Context, addressBookID int64, uid string) error {
	const q = `DELETE FROM contacts WHERE address_book_id=$1 AND uid=$2`
	defer observeDB(ctx, "contacts.delete_by_uid")()
	_, err := r.pool.ExecContext(ctx, q, addressBookID, uid)
	return err
}

func (r *contactRepo) MoveToAddressBook(ctx context.Context, fromAddressBookID, toAddressBookID int64, uid, destResourceName string) error {
	defer observeDB(ctx, "contacts.move_to_address_book")()

	tx, err := r.pool.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()

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
		return tx.Commit()
	}

	const tombstoneQuery = `INSERT INTO deleted_resources (resource_type, collection_id, uid, resource_name) VALUES ('contact', $1, $2, $3)`
	if _, err := tx.ExecContext(ctx, tombstoneQuery, fromAddressBookID, uid, sourceResourceName); err != nil {
		return err
	}

	const incrementCtagQuery = `UPDATE address_books SET ctag = ctag + 1, updated_at = NOW() WHERE id = $1`
	if _, err := tx.ExecContext(ctx, incrementCtagQuery, fromAddressBookID); err != nil {
		return err
	}

	return tx.Commit()
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

func (r *contactRepo) ListModifiedSince(ctx context.Context, addressBookID int64, since time.Time) ([]Contact, error) {
	const q = `SELECT id, address_book_id, uid, resource_name, raw_vcard, etag, display_name, primary_email, birthday, last_modified FROM contacts WHERE address_book_id=$1 AND last_modified > $2 ORDER BY last_modified DESC`
	defer observeDB(ctx, "contacts.list_modified_since")()
	rows, err := r.pool.QueryContext(ctx, q, addressBookID, since)
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

func (r *contactRepo) ListWithBirthdaysByUser(ctx context.Context, userID int64) ([]Contact, error) {
	const q = `
SELECT c.id, c.address_book_id, c.uid, c.resource_name, c.raw_vcard, c.etag, c.display_name, c.primary_email, c.birthday, c.last_modified
FROM contacts c
JOIN address_books ab ON ab.id = c.address_book_id
WHERE ab.user_id = $1 AND c.birthday IS NOT NULL
ORDER BY c.display_name
`
	defer observeDB(ctx, "contacts.list_with_birthdays_by_user")()
	rows, err := r.pool.QueryContext(ctx, q, userID)
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

func (r *contactRepo) CopyToAddressBook(ctx context.Context, fromAddressBookID, toAddressBookID int64, uid, destResourceName, newETag string) (*Contact, error) {
	defer observeDB(ctx, "contacts.copy_to_address_book")()

	tx, err := r.pool.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()

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
INSERT INTO app_passwords (user_id, label, token_hash, expires_at)
VALUES ($1, $2, $3, $4)
RETURNING id, user_id, label, token_hash, created_at, expires_at, revoked_at, last_used_at
`
	defer observeDB(ctx, "app_passwords.create")()
	row := r.pool.QueryRowContext(ctx, q, token.UserID, token.Label, token.TokenHash, token.ExpiresAt)
	t, err := scanAppPassword(row.Scan)
	if err != nil {
		return nil, err
	}
	return &t, nil
}

func (r *appPasswordRepo) FindValidByUser(ctx context.Context, userID int64) ([]AppPassword, error) {
	const q = `
SELECT id, user_id, label, token_hash, created_at, expires_at, revoked_at, last_used_at
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
	const q = `SELECT id, user_id, label, token_hash, created_at, expires_at, revoked_at, last_used_at FROM app_passwords WHERE user_id=$1 ORDER BY created_at DESC`
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
	const q = `SELECT id, user_id, label, token_hash, created_at, expires_at, revoked_at, last_used_at FROM app_passwords WHERE id=$1`
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

func (r *appPasswordRepo) TouchLastUsed(ctx context.Context, id int64) error {
	const q = `UPDATE app_passwords SET last_used_at = NOW() WHERE id=$1`
	defer observeDB(ctx, "app_passwords.touch_last_used")()
	_, err := r.pool.ExecContext(ctx, q, id)
	return err
}

// deletedResourceRepo implements DeletedResourceRepository.
type deletedResourceRepo struct {
	pool *sql.DB
}

func (r *deletedResourceRepo) ListDeletedSince(ctx context.Context, resourceType string, collectionID int64, since time.Time) ([]DeletedResource, error) {
	const q = `SELECT id, resource_type, collection_id, uid, resource_name, deleted_at FROM deleted_resources WHERE resource_type=$1 AND collection_id=$2 AND deleted_at > $3 ORDER BY deleted_at DESC`
	defer observeDB(ctx, "deleted_resources.list_deleted_since")()
	rows, err := r.pool.QueryContext(ctx, q, resourceType, collectionID, since)
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

	// Serialize concurrent lock creation for the resource and its parent path so
	// parent/child lock requests observe each other before conflict checks run.
	for _, resourcePath := range lockSerializationPaths(lock.ResourcePath) {
		if _, err := tx.ExecContext(ctx, `SELECT pg_advisory_xact_lock(hashtext($1))`, resourcePath); err != nil {
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

	if err := tx.Commit(); err != nil {
		return nil, err
	}
	return &l, nil
}

func lockSerializationPaths(resourcePath string) []string {
	cleanPath := path.Clean(resourcePath)
	paths := []string{cleanPath}
	paths = append(paths, lockAncestorPaths(cleanPath)...)
	return paths
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

	type aclIdentity struct {
		principalHref string
		isGrant       bool
		privilege     string
	}

	const existingQ = `SELECT id, resource_path, principal_href, is_grant, privilege, created_at FROM acl_entries WHERE resource_path=$1 ORDER BY created_at, id`
	rows, err := tx.QueryContext(ctx, existingQ, resourcePath)
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
			createdAt     time.Time
		)
		if err := rows.Scan(&id, &existingPath, &principalHref, &isGrant, &privilege, &createdAt); err != nil {
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

	// Delete existing entries for this resource
	const deleteQ = `DELETE FROM acl_entries WHERE resource_path=$1`
	if _, err := tx.ExecContext(ctx, deleteQ, resourcePath); err != nil {
		return err
	}

	// Insert the new entries
	const insertQ = `INSERT INTO acl_entries (resource_path, principal_href, is_grant, privilege, created_at) VALUES ($1, $2, $3, $4, $5)`
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
		if _, err := tx.ExecContext(ctx, insertQ, resourcePath, entry.PrincipalHref, entry.IsGrant, entry.Privilege, createdAt); err != nil {
			return err
		}
	}

	if err := touchACLDependentState(ctx, tx, resourcePath); err != nil {
		return err
	}

	return tx.Commit()
}

func touchACLDependentState(ctx context.Context, tx *sql.Tx, resourcePath string) error {
	collectionType, collectionID, resourceName, collectionPath, ok := aclResourceIdentity(resourcePath)
	if !ok {
		return nil
	}

	switch collectionType {
	case "calendar":
		const touchCalendarQ = `UPDATE calendars SET ctag = ctag + 1, updated_at = NOW() WHERE id = $1`
		if _, err := tx.ExecContext(ctx, touchCalendarQ, collectionID); err != nil {
			return err
		}
		if collectionPath {
			const touchEventsQ = `UPDATE events SET last_modified = NOW() WHERE calendar_id = $1`
			if _, err := tx.ExecContext(ctx, touchEventsQ, collectionID); err != nil {
				return err
			}
		} else {
			canonical, alternate := aclResourceNameCandidates(resourceName, ".ics")
			const touchEventQ = `UPDATE events SET last_modified = NOW() WHERE calendar_id = $1 AND resource_name IN ($2, $3)`
			if _, err := tx.ExecContext(ctx, touchEventQ, collectionID, canonical, alternate); err != nil {
				return err
			}
		}
	case "addressbook":
		const touchBookQ = `UPDATE address_books SET ctag = ctag + 1, updated_at = NOW() WHERE id = $1`
		if _, err := tx.ExecContext(ctx, touchBookQ, collectionID); err != nil {
			return err
		}
		if collectionPath {
			const touchContactsQ = `UPDATE contacts SET last_modified = NOW() WHERE address_book_id = $1`
			if _, err := tx.ExecContext(ctx, touchContactsQ, collectionID); err != nil {
				return err
			}
		} else {
			canonical, alternate := aclResourceNameCandidates(resourceName, ".vcf")
			const touchContactQ = `UPDATE contacts SET last_modified = NOW() WHERE address_book_id = $1 AND resource_name IN ($2, $3)`
			if _, err := tx.ExecContext(ctx, touchContactQ, collectionID, canonical, alternate); err != nil {
				return err
			}
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
	const q = `SELECT id, resource_path, principal_href, is_grant, privilege, created_at FROM acl_entries WHERE resource_path=$1 ORDER BY created_at, id`
	defer observeDB(ctx, "acl.list_by_resource")()
	rows, err := r.pool.QueryContext(ctx, q, resourcePath)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var result []ACLEntry
	for rows.Next() {
		var e ACLEntry
		if err := rows.Scan(&e.ID, &e.ResourcePath, &e.PrincipalHref, &e.IsGrant, &e.Privilege, &e.CreatedAt); err != nil {
			return nil, err
		}
		result = append(result, e)
	}
	return result, rows.Err()
}

func (r *aclRepo) ListByPrincipal(ctx context.Context, principalHref string) ([]ACLEntry, error) {
	const q = `SELECT id, resource_path, principal_href, is_grant, privilege, created_at FROM acl_entries WHERE principal_href=$1 ORDER BY created_at`
	defer observeDB(ctx, "acl.list_by_principal")()
	rows, err := r.pool.QueryContext(ctx, q, principalHref)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var result []ACLEntry
	for rows.Next() {
		var e ACLEntry
		if err := rows.Scan(&e.ID, &e.ResourcePath, &e.PrincipalHref, &e.IsGrant, &e.Privilege, &e.CreatedAt); err != nil {
			return nil, err
		}
		result = append(result, e)
	}
	return result, rows.Err()
}

func (r *aclRepo) HasPrivilege(ctx context.Context, resourcePath, principalHref, privilege string) (bool, error) {
	const q = `
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
`
	defer observeDB(ctx, "acl.has_privilege")()
	var exists bool
	if err := r.pool.QueryRowContext(ctx, q, resourcePath, principalHref, privilege).Scan(&exists); err != nil {
		return false, err
	}
	return exists, nil
}

func (r *aclRepo) DeletePrincipalEntriesByResourcePrefix(ctx context.Context, principalHref, resourcePathPrefix string) error {
	defer observeDB(ctx, "acl.delete_principal_entries_by_resource_prefix")()

	tx, err := r.pool.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	escaped := strings.NewReplacer(`\`, `\\`, `%`, `\%`, `_`, `\_`).Replace(resourcePathPrefix)
	likePrefix := escaped + "/%"

	const listQ = `SELECT DISTINCT resource_path FROM acl_entries WHERE principal_href=$1 AND (resource_path=$2 OR resource_path LIKE $3 ESCAPE '\') ORDER BY resource_path`
	rows, err := tx.QueryContext(ctx, listQ, principalHref, resourcePathPrefix, likePrefix)
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

	const deleteQ = `DELETE FROM acl_entries WHERE principal_href=$1 AND (resource_path=$2 OR resource_path LIKE $3 ESCAPE '\')`
	if _, err := tx.ExecContext(ctx, deleteQ, principalHref, resourcePathPrefix, likePrefix); err != nil {
		return err
	}

	for _, resourcePath := range affected {
		if err := touchACLDependentState(ctx, tx, resourcePath); err != nil {
			return err
		}
	}

	return tx.Commit()
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
	var dtstart sql.NullTime
	var dtend sql.NullTime
	if err := scan(&ev.ID, &ev.CalendarID, &ev.UID, &ev.ResourceName, &ev.RawICAL, &ev.ETag, &summary, &dtstart, &dtend, &ev.AllDay, &ev.LastModified); err != nil {
		return Event{}, err
	}
	ev.Summary = nullableString(summary)
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
	return c, nil
}

func scanAppPassword(scan rowScanner) (AppPassword, error) {
	var t AppPassword
	var expiresAt sql.NullTime
	var revokedAt sql.NullTime
	var lastUsedAt sql.NullTime
	if err := scan(&t.ID, &t.UserID, &t.Label, &t.TokenHash, &t.CreatedAt, &expiresAt, &revokedAt, &lastUsedAt); err != nil {
		return AppPassword{}, err
	}
	t.ExpiresAt = nullableTime(expiresAt)
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

// parseICalFields extracts summary, dtstart, dtend, and all_day from raw iCalendar data.
func parseICalFields(ical string) (*string, *time.Time, *time.Time, bool) {
	var summary *string
	var dtstart, dtend *time.Time
	allDay := false

	lines := unfoldICalLines(ical)
	inEvent := false

	for _, line := range lines {
		if line == "BEGIN:VEVENT" {
			inEvent = true
			continue
		}
		if line == "END:VEVENT" {
			break
		}
		if !inEvent {
			continue
		}

		colonIdx := strings.Index(line, ":")
		if colonIdx == -1 {
			continue
		}

		keyPart := line[:colonIdx]
		value := line[colonIdx+1:]

		// Remove parameters (e.g., DTSTART;VALUE=DATE:20231225)
		key := keyPart
		if semiIdx := strings.Index(keyPart, ";"); semiIdx != -1 {
			key = keyPart[:semiIdx]
		}

		switch key {
		case "SUMMARY":
			summary = util.StrPtr(unescapeICalValue(value))
		case "DTSTART":
			t, isAllDay := parseICalDateTime(value, keyPart)
			if t != nil {
				dtstart = t
				allDay = isAllDay
			}
		case "DTEND":
			t, _ := parseICalDateTime(value, keyPart)
			if t != nil {
				dtend = t
			}
		}
	}

	return summary, dtstart, dtend, allDay
}

func unfoldICalLines(ical string) []string {
	// Unfold continuation lines (lines starting with space or tab)
	unfolded := regexp.MustCompile(`\r?\n[ \t]`).ReplaceAllString(ical, "")
	// Normalize line endings and split
	unfolded = strings.ReplaceAll(unfolded, "\r\n", "\n")
	unfolded = strings.ReplaceAll(unfolded, "\r", "\n")
	return strings.Split(unfolded, "\n")
}

func unescapeICalValue(s string) string {
	s = strings.ReplaceAll(s, "\\n", "\n")
	s = strings.ReplaceAll(s, "\\N", "\n")
	s = strings.ReplaceAll(s, "\\,", ",")
	s = strings.ReplaceAll(s, "\\;", ";")
	s = strings.ReplaceAll(s, "\\\\", "\\")
	return s
}

func parseICalDateTime(value, keyPart string) (*time.Time, bool) {
	value = strings.TrimSpace(value)
	isAllDay := false

	// Check for VALUE=DATE parameter (all-day event)
	if strings.Contains(strings.ToUpper(keyPart), "VALUE=DATE") && !strings.Contains(strings.ToUpper(keyPart), "VALUE=DATE-TIME") {
		isAllDay = true
	}

	// Handle timezone identifier parameter (e.g., DTSTART;TZID=America/New_York)
	if tzid := paramValue(keyPart, "TZID"); tzid != "" {
		if loc, err := time.LoadLocation(tzid); err == nil {
			if t, err := time.ParseInLocation("20060102T150405", strings.TrimSuffix(value, "Z"), loc); err == nil {
				utc := t.In(time.UTC)
				return &utc, isAllDay
			}
		}
	}

	// Handle explicit numeric offsets (e.g., 20240201T120000-0500 or 20240201T120000-05:00)
	for _, layout := range []string{"20060102T150405-0700", "20060102T150405-07:00"} {
		if t, err := time.Parse(layout, value); err == nil {
			utc := t.UTC()
			return &utc, isAllDay
		}
	}

	// Remove trailing Z for UTC and parse as basic datetime
	value = strings.TrimSuffix(value, "Z")

	var t time.Time
	var err error

	if len(value) == 8 {
		// All-day: YYYYMMDD
		t, err = time.Parse("20060102", value)
		isAllDay = true
	} else if len(value) == 15 {
		// Date-time: YYYYMMDDTHHmmss
		t, err = time.Parse("20060102T150405", value)
	} else {
		return nil, false
	}

	if err != nil {
		return nil, false
	}
	return &t, isAllDay
}

func paramValue(keyPart, param string) string {
	parts := strings.Split(keyPart, ";")
	if len(parts) < 2 {
		return ""
	}

	paramUpper := strings.ToUpper(param)
	for _, p := range parts[1:] {
		if strings.HasPrefix(strings.ToUpper(p), paramUpper+"=") {
			if pieces := strings.SplitN(p, "=", 2); len(pieces) == 2 {
				return pieces[1]
			}
		}
	}
	return ""
}

// parseVCardFields extracts display_name, primary_email, and birthday from raw vCard data.
func parseVCardFields(vcard string) (*string, *string, *time.Time) {
	var displayName, primaryEmail *string
	var birthday *time.Time

	lines := unfoldVCardLines(vcard)
	for _, line := range lines {
		colonIdx := strings.Index(line, ":")
		if colonIdx == -1 {
			continue
		}

		keyPart := line[:colonIdx]
		value := line[colonIdx+1:]

		// Remove parameters
		key := keyPart
		if semiIdx := strings.Index(keyPart, ";"); semiIdx != -1 {
			key = keyPart[:semiIdx]
		}
		key = strings.ToUpper(key)

		switch key {
		case "FN":
			displayName = util.StrPtr(unescapeVCardValue(value))
		case "EMAIL":
			if primaryEmail == nil {
				primaryEmail = util.StrPtr(strings.TrimSpace(value))
			}
		case "BDAY":
			if bd := parseVCardBirthday(value); bd != nil {
				birthday = bd
			}
		}
	}

	return displayName, primaryEmail, birthday
}

// parseVCardBirthday parses birthday from various vCard formats.
func parseVCardBirthday(value string) *time.Time {
	value = strings.TrimSpace(value)
	if value == "" {
		return nil
	}

	// Try YYYY-MM-DD format
	if t, err := time.Parse("2006-01-02", value); err == nil {
		return &t
	}

	// Try YYYYMMDD format
	if t, err := time.Parse("20060102", value); err == nil {
		return &t
	}

	// Try --MM-DD format (no year)
	if strings.HasPrefix(value, "--") && len(value) >= 7 {
		mmdd := strings.TrimPrefix(value, "--")
		// Use year 1 as placeholder for no-year birthdays
		if t, err := time.Parse("01-02", mmdd); err == nil {
			bd := time.Date(1, t.Month(), t.Day(), 0, 0, 0, 0, time.UTC)
			return &bd
		}
		if t, err := time.Parse("0102", mmdd); err == nil {
			bd := time.Date(1, t.Month(), t.Day(), 0, 0, 0, 0, time.UTC)
			return &bd
		}
	}

	return nil
}

func unfoldVCardLines(vcard string) []string {
	// Unfold continuation lines
	unfolded := regexp.MustCompile(`\r?\n[ \t]`).ReplaceAllString(vcard, "")
	// Normalize line endings and split
	unfolded = strings.ReplaceAll(unfolded, "\r\n", "\n")
	unfolded = strings.ReplaceAll(unfolded, "\r", "\n")
	return strings.Split(unfolded, "\n")
}

func unescapeVCardValue(s string) string {
	s = strings.ReplaceAll(s, "\\n", "\n")
	s = strings.ReplaceAll(s, "\\N", "\n")
	s = strings.ReplaceAll(s, "\\,", ",")
	s = strings.ReplaceAll(s, "\\;", ";")
	s = strings.ReplaceAll(s, "\\\\", "\\")
	return s
}
