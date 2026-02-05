package store

import (
	"context"
	"database/sql"
	"errors"
	"regexp"
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
	const q = `
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
		if err := rows.Scan(&c.ID, &c.UserID, &c.Name, &slug, &description, &timezone, &color, &c.CTag, &c.CreatedAt, &c.UpdatedAt, &c.OwnerEmail, &c.Shared, &c.Editor); err != nil {
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

func (r *calendarRepo) GetAccessible(ctx context.Context, calendarID, userID int64) (*CalendarAccess, error) {
	const q = `
SELECT c.id, c.user_id, c.name, c.slug, c.description, c.timezone, c.color, c.ctag, c.created_at, c.updated_at,
       u.primary_email as owner_email,
       CASE WHEN c.user_id = $2 THEN FALSE ELSE TRUE END as shared,
       COALESCE(cs.editor, TRUE) as editor
FROM calendars c
JOIN users u ON u.id = c.user_id
LEFT JOIN calendar_shares cs ON cs.calendar_id = c.id AND cs.user_id = $2
WHERE c.id = $1 AND (c.user_id = $2 OR cs.user_id = $2)
`
	defer observeDB(ctx, "calendars.get_accessible")()
	var c CalendarAccess
	var slug, description, timezone, color sql.NullString
	if err := r.pool.QueryRowContext(ctx, q, calendarID, userID).Scan(&c.ID, &c.UserID, &c.Name, &slug, &description, &timezone, &color, &c.CTag, &c.CreatedAt, &c.UpdatedAt, &c.OwnerEmail, &c.Shared, &c.Editor); err != nil {
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

// calendarShareRepo implements CalendarShareRepository.
type calendarShareRepo struct {
	pool *sql.DB
}

func (r *calendarShareRepo) ListByCalendar(ctx context.Context, calendarID int64) ([]CalendarShare, error) {
	const q = `SELECT calendar_id, user_id, granted_by, editor, created_at FROM calendar_shares WHERE calendar_id=$1 ORDER BY created_at`
	defer observeDB(ctx, "calendar_shares.list_by_calendar")()
	rows, err := r.pool.QueryContext(ctx, q, calendarID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var shares []CalendarShare
	for rows.Next() {
		var cs CalendarShare
		if err := rows.Scan(&cs.CalendarID, &cs.UserID, &cs.GrantedBy, &cs.Editor, &cs.CreatedAt); err != nil {
			return nil, err
		}
		shares = append(shares, cs)
	}
	return shares, rows.Err()
}

func (r *calendarShareRepo) Create(ctx context.Context, share CalendarShare) error {
	const q = `
INSERT INTO calendar_shares (calendar_id, user_id, granted_by, editor)
VALUES ($1, $2, $3, $4)
ON CONFLICT (calendar_id, user_id) DO NOTHING
`
	defer observeDB(ctx, "calendar_shares.create")()
	_, err := r.pool.ExecContext(ctx, q, share.CalendarID, share.UserID, share.GrantedBy, share.Editor)
	return err
}

func (r *calendarShareRepo) Delete(ctx context.Context, calendarID, userID int64) error {
	const q = `DELETE FROM calendar_shares WHERE calendar_id=$1 AND user_id=$2`
	defer observeDB(ctx, "calendar_shares.delete")()
	_, err := r.pool.ExecContext(ctx, q, calendarID, userID)
	return err
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
	const q = `
SELECT e.id, e.calendar_id, e.uid, e.resource_name, e.raw_ical, e.etag, e.summary, e.dtstart, e.dtend, e.all_day, e.last_modified
FROM events e
JOIN calendars c ON c.id = e.calendar_id
LEFT JOIN calendar_shares cs ON cs.calendar_id = c.id AND cs.user_id = $1
WHERE c.user_id = $1 OR cs.user_id = $1
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

// addressBookRepo implements AddressBookRepository.
type addressBookRepo struct {
	pool *sql.DB
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

	const q = `
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
`
	defer observeDB(ctx, "contacts.upsert")()
	row := r.pool.QueryRowContext(ctx, q, contact.AddressBookID, contact.UID, contact.RawVCard, contact.ETag, displayName, primaryEmail, birthday)
	c, err := scanContact(row.Scan)
	if err != nil {
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

func (r *contactRepo) MoveToAddressBook(ctx context.Context, fromAddressBookID, toAddressBookID int64, uid string) error {
	defer observeDB(ctx, "contacts.move_to_address_book")()

	// Use a transaction to ensure atomicity
	tx, err := r.pool.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	// Move the contact to the new address book
	const moveQuery = `UPDATE contacts SET address_book_id=$1, last_modified=NOW() WHERE address_book_id=$2 AND uid=$3`
	result, err := tx.ExecContext(ctx, moveQuery, toAddressBookID, fromAddressBookID, uid)
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

	// Create a tombstone in the source address book for sync
	const tombstoneQuery = `INSERT INTO deleted_resources (resource_type, collection_id, uid, resource_name) VALUES ('contact', $1, $2, $2)`
	if _, err := tx.ExecContext(ctx, tombstoneQuery, fromAddressBookID, uid); err != nil {
		return err
	}

	// Increment the source address book's ctag so clients know to sync
	const incrementCtagQuery = `UPDATE address_books SET ctag = ctag + 1, updated_at = NOW() WHERE id = $1`
	if _, err := tx.ExecContext(ctx, incrementCtagQuery, fromAddressBookID); err != nil {
		return err
	}

	// The target address book's ctag is automatically incremented by the UPDATE trigger

	return tx.Commit()
}

func (r *contactRepo) GetByUID(ctx context.Context, addressBookID int64, uid string) (*Contact, error) {
	const q = `SELECT id, address_book_id, uid, raw_vcard, etag, display_name, primary_email, birthday, last_modified FROM contacts WHERE address_book_id=$1 AND uid=$2`
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
	const q = `SELECT id, address_book_id, uid, raw_vcard, etag, display_name, primary_email, birthday, last_modified FROM contacts WHERE address_book_id=$1 AND uid = ANY($2)`
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
	const q = `SELECT id, address_book_id, uid, raw_vcard, etag, display_name, primary_email, birthday, last_modified FROM contacts WHERE address_book_id=$1 ORDER BY last_modified DESC`
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

	const q = `SELECT id, address_book_id, uid, raw_vcard, etag, display_name, primary_email, birthday, last_modified FROM contacts WHERE address_book_id=$1 ORDER BY LOWER(COALESCE(display_name, '')) ASC, id ASC LIMIT $2 OFFSET $3`
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
	const q = `SELECT id, address_book_id, uid, raw_vcard, etag, display_name, primary_email, birthday, last_modified FROM contacts WHERE address_book_id=$1 AND last_modified > $2 ORDER BY last_modified DESC`
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
SELECT c.id, c.address_book_id, c.uid, c.raw_vcard, c.etag, c.display_name, c.primary_email, c.birthday, c.last_modified
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
SELECT c.id, c.address_book_id, c.uid, c.raw_vcard, c.etag, c.display_name, c.primary_email, c.birthday, c.last_modified
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
	if err := scan(&c.ID, &c.AddressBookID, &c.UID, &c.RawVCard, &c.ETag, &displayName, &primaryEmail, &birthday, &c.LastModified); err != nil {
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
