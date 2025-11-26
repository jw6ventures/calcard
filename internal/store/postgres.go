package store

import (
	"context"
	"errors"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// userRepo implements UserRepository.
type userRepo struct {
	pool *pgxpool.Pool
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
	row := r.pool.QueryRow(ctx, q, subject, email)
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
	if err := r.pool.QueryRow(ctx, q, id).Scan(&u.ID, &u.OAuthSubject, &u.PrimaryEmail, &u.CreatedAt, &u.LastLoginAt); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
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
	if err := r.pool.QueryRow(ctx, q, email).Scan(&u.ID, &u.OAuthSubject, &u.PrimaryEmail, &u.CreatedAt, &u.LastLoginAt); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, nil
		}
		return nil, err
	}
	return &u, nil
}

// calendarRepo implements CalendarRepository.
type calendarRepo struct {
	pool *pgxpool.Pool
}

func (r *calendarRepo) ListByUser(ctx context.Context, userID int64) ([]Calendar, error) {
	const q = `SELECT id, user_id, name, color, created_at FROM calendars WHERE user_id=$1 ORDER BY created_at`
	defer observeDB(ctx, "calendars.list_by_user")()
	rows, err := r.pool.Query(ctx, q, userID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var result []Calendar
	for rows.Next() {
		var c Calendar
		if err := rows.Scan(&c.ID, &c.UserID, &c.Name, &c.Color, &c.CreatedAt); err != nil {
			return nil, err
		}
		result = append(result, c)
	}
	return result, rows.Err()
}

func (r *calendarRepo) GetByID(ctx context.Context, id int64) (*Calendar, error) {
	const q = `SELECT id, user_id, name, color, created_at FROM calendars WHERE id=$1`
	defer observeDB(ctx, "calendars.get_by_id")()
	var c Calendar
	if err := r.pool.QueryRow(ctx, q, id).Scan(&c.ID, &c.UserID, &c.Name, &c.Color, &c.CreatedAt); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, nil
		}
		return nil, err
	}
	return &c, nil
}

func (r *calendarRepo) Create(ctx context.Context, cal Calendar) (*Calendar, error) {
	const q = `INSERT INTO calendars (user_id, name, color) VALUES ($1, $2, $3) RETURNING id, user_id, name, color, created_at`
	defer observeDB(ctx, "calendars.create")()
	row := r.pool.QueryRow(ctx, q, cal.UserID, cal.Name, cal.Color)
	var created Calendar
	if err := row.Scan(&created.ID, &created.UserID, &created.Name, &created.Color, &created.CreatedAt); err != nil {
		return nil, err
	}
	return &created, nil
}

func (r *calendarRepo) Rename(ctx context.Context, userID, id int64, name string) error {
	const q = `UPDATE calendars SET name=$1 WHERE id=$2 AND user_id=$3`
	defer observeDB(ctx, "calendars.rename")()
	res, err := r.pool.Exec(ctx, q, name, id, userID)
	if err != nil {
		return err
	}
	if res.RowsAffected() == 0 {
		return ErrNotFound
	}
	return nil
}

func (r *calendarRepo) Delete(ctx context.Context, userID, id int64) error {
	const q = `DELETE FROM calendars WHERE id=$1 AND user_id=$2`
	defer observeDB(ctx, "calendars.delete")()
	res, err := r.pool.Exec(ctx, q, id, userID)
	if err != nil {
		return err
	}
	if res.RowsAffected() == 0 {
		return ErrNotFound
	}
	return nil
}

// eventRepo implements EventRepository.
type eventRepo struct {
	pool *pgxpool.Pool
}

func (r *eventRepo) Upsert(ctx context.Context, event Event) (*Event, error) {
	const q = `
INSERT INTO events (calendar_id, uid, raw_ical, etag, last_modified)
VALUES ($1, $2, $3, $4, NOW())
ON CONFLICT (calendar_id, uid) DO UPDATE SET
        raw_ical = EXCLUDED.raw_ical,
        etag = EXCLUDED.etag,
        last_modified = NOW()
RETURNING id, calendar_id, uid, raw_ical, etag, last_modified
`
	defer observeDB(ctx, "events.upsert")()
	row := r.pool.QueryRow(ctx, q, event.CalendarID, event.UID, event.RawICAL, event.ETag)
	var ev Event
	if err := row.Scan(&ev.ID, &ev.CalendarID, &ev.UID, &ev.RawICAL, &ev.ETag, &ev.LastModified); err != nil {
		return nil, err
	}
	return &ev, nil
}

func (r *eventRepo) DeleteByUID(ctx context.Context, calendarID int64, uid string) error {
	const q = `DELETE FROM events WHERE calendar_id=$1 AND uid=$2`
	defer observeDB(ctx, "events.delete_by_uid")()
	_, err := r.pool.Exec(ctx, q, calendarID, uid)
	return err
}

func (r *eventRepo) GetByUID(ctx context.Context, calendarID int64, uid string) (*Event, error) {
	const q = `SELECT id, calendar_id, uid, raw_ical, etag, last_modified FROM events WHERE calendar_id=$1 AND uid=$2`
	defer observeDB(ctx, "events.get_by_uid")()
	var ev Event
	if err := r.pool.QueryRow(ctx, q, calendarID, uid).Scan(&ev.ID, &ev.CalendarID, &ev.UID, &ev.RawICAL, &ev.ETag, &ev.LastModified); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
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
	const q = `SELECT id, calendar_id, uid, raw_ical, etag, last_modified FROM events WHERE calendar_id=$1 AND uid = ANY($2)`
	defer observeDB(ctx, "events.list_by_uids")()
	rows, err := r.pool.Query(ctx, q, calendarID, uids)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var result []Event
	for rows.Next() {
		var ev Event
		if err := rows.Scan(&ev.ID, &ev.CalendarID, &ev.UID, &ev.RawICAL, &ev.ETag, &ev.LastModified); err != nil {
			return nil, err
		}
		result = append(result, ev)
	}
	return result, rows.Err()
}

func (r *eventRepo) ListForCalendar(ctx context.Context, calendarID int64) ([]Event, error) {
	const q = `SELECT id, calendar_id, uid, raw_ical, etag, last_modified FROM events WHERE calendar_id=$1 ORDER BY last_modified DESC`
	defer observeDB(ctx, "events.list_for_calendar")()
	rows, err := r.pool.Query(ctx, q, calendarID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var result []Event
	for rows.Next() {
		var ev Event
		if err := rows.Scan(&ev.ID, &ev.CalendarID, &ev.UID, &ev.RawICAL, &ev.ETag, &ev.LastModified); err != nil {
			return nil, err
		}
		result = append(result, ev)
	}
	return result, rows.Err()
}

// addressBookRepo implements AddressBookRepository.
type addressBookRepo struct {
	pool *pgxpool.Pool
}

func (r *addressBookRepo) ListByUser(ctx context.Context, userID int64) ([]AddressBook, error) {
	const q = `SELECT id, user_id, name, created_at FROM address_books WHERE user_id=$1 ORDER BY created_at`
	defer observeDB(ctx, "address_books.list_by_user")()
	rows, err := r.pool.Query(ctx, q, userID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var result []AddressBook
	for rows.Next() {
		var book AddressBook
		if err := rows.Scan(&book.ID, &book.UserID, &book.Name, &book.CreatedAt); err != nil {
			return nil, err
		}
		result = append(result, book)
	}
	return result, rows.Err()
}

func (r *addressBookRepo) GetByID(ctx context.Context, id int64) (*AddressBook, error) {
	const q = `SELECT id, user_id, name, created_at FROM address_books WHERE id=$1`
	defer observeDB(ctx, "address_books.get_by_id")()
	var book AddressBook
	if err := r.pool.QueryRow(ctx, q, id).Scan(&book.ID, &book.UserID, &book.Name, &book.CreatedAt); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, nil
		}
		return nil, err
	}
	return &book, nil
}

func (r *addressBookRepo) Create(ctx context.Context, book AddressBook) (*AddressBook, error) {
	const q = `INSERT INTO address_books (user_id, name) VALUES ($1, $2) RETURNING id, user_id, name, created_at`
	defer observeDB(ctx, "address_books.create")()
	row := r.pool.QueryRow(ctx, q, book.UserID, book.Name)
	var created AddressBook
	if err := row.Scan(&created.ID, &created.UserID, &created.Name, &created.CreatedAt); err != nil {
		return nil, err
	}
	return &created, nil
}

func (r *addressBookRepo) Rename(ctx context.Context, userID, id int64, name string) error {
	const q = `UPDATE address_books SET name=$1 WHERE id=$2 AND user_id=$3`
	defer observeDB(ctx, "address_books.rename")()
	res, err := r.pool.Exec(ctx, q, name, id, userID)
	if err != nil {
		return err
	}
	if res.RowsAffected() == 0 {
		return ErrNotFound
	}
	return nil
}

func (r *addressBookRepo) Delete(ctx context.Context, userID, id int64) error {
	const q = `DELETE FROM address_books WHERE id=$1 AND user_id=$2`
	defer observeDB(ctx, "address_books.delete")()
	res, err := r.pool.Exec(ctx, q, id, userID)
	if err != nil {
		return err
	}
	if res.RowsAffected() == 0 {
		return ErrNotFound
	}
	return nil
}

// contactRepo implements ContactRepository.
type contactRepo struct {
	pool *pgxpool.Pool
}

func (r *contactRepo) Upsert(ctx context.Context, contact Contact) (*Contact, error) {
	const q = `
INSERT INTO contacts (address_book_id, uid, raw_vcard, etag, last_modified)
VALUES ($1, $2, $3, $4, NOW())
ON CONFLICT (address_book_id, uid) DO UPDATE SET
        raw_vcard = EXCLUDED.raw_vcard,
        etag = EXCLUDED.etag,
        last_modified = NOW()
RETURNING id, address_book_id, uid, raw_vcard, etag, last_modified
`
	defer observeDB(ctx, "contacts.upsert")()
	row := r.pool.QueryRow(ctx, q, contact.AddressBookID, contact.UID, contact.RawVCard, contact.ETag)
	var c Contact
	if err := row.Scan(&c.ID, &c.AddressBookID, &c.UID, &c.RawVCard, &c.ETag, &c.LastModified); err != nil {
		return nil, err
	}
	return &c, nil
}

func (r *contactRepo) DeleteByUID(ctx context.Context, addressBookID int64, uid string) error {
	const q = `DELETE FROM contacts WHERE address_book_id=$1 AND uid=$2`
	defer observeDB(ctx, "contacts.delete_by_uid")()
	_, err := r.pool.Exec(ctx, q, addressBookID, uid)
	return err
}

func (r *contactRepo) GetByUID(ctx context.Context, addressBookID int64, uid string) (*Contact, error) {
	const q = `SELECT id, address_book_id, uid, raw_vcard, etag, last_modified FROM contacts WHERE address_book_id=$1 AND uid=$2`
	defer observeDB(ctx, "contacts.get_by_uid")()
	var c Contact
	if err := r.pool.QueryRow(ctx, q, addressBookID, uid).Scan(&c.ID, &c.AddressBookID, &c.UID, &c.RawVCard, &c.ETag, &c.LastModified); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
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
	const q = `SELECT id, address_book_id, uid, raw_vcard, etag, last_modified FROM contacts WHERE address_book_id=$1 AND uid = ANY($2)`
	defer observeDB(ctx, "contacts.list_by_uids")()
	rows, err := r.pool.Query(ctx, q, addressBookID, uids)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var result []Contact
	for rows.Next() {
		var c Contact
		if err := rows.Scan(&c.ID, &c.AddressBookID, &c.UID, &c.RawVCard, &c.ETag, &c.LastModified); err != nil {
			return nil, err
		}
		result = append(result, c)
	}
	return result, rows.Err()
}

func (r *contactRepo) ListForBook(ctx context.Context, addressBookID int64) ([]Contact, error) {
	const q = `SELECT id, address_book_id, uid, raw_vcard, etag, last_modified FROM contacts WHERE address_book_id=$1 ORDER BY last_modified DESC`
	defer observeDB(ctx, "contacts.list_for_book")()
	rows, err := r.pool.Query(ctx, q, addressBookID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var result []Contact
	for rows.Next() {
		var c Contact
		if err := rows.Scan(&c.ID, &c.AddressBookID, &c.UID, &c.RawVCard, &c.ETag, &c.LastModified); err != nil {
			return nil, err
		}
		result = append(result, c)
	}
	return result, rows.Err()
}

// appPasswordRepo implements AppPasswordRepository.
type appPasswordRepo struct {
	pool *pgxpool.Pool
}

func (r *appPasswordRepo) Create(ctx context.Context, token AppPassword) (*AppPassword, error) {
	const q = `
INSERT INTO app_passwords (user_id, label, token_hash, expires_at)
VALUES ($1, $2, $3, $4)
RETURNING id, user_id, label, token_hash, created_at, expires_at, revoked_at, last_used_at
`
	defer observeDB(ctx, "app_passwords.create")()
	row := r.pool.QueryRow(ctx, q, token.UserID, token.Label, token.TokenHash, token.ExpiresAt)
	var t AppPassword
	if err := row.Scan(&t.ID, &t.UserID, &t.Label, &t.TokenHash, &t.CreatedAt, &t.ExpiresAt, &t.RevokedAt, &t.LastUsedAt); err != nil {
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
	rows, err := r.pool.Query(ctx, q, userID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var result []AppPassword
	for rows.Next() {
		var t AppPassword
		if err := rows.Scan(&t.ID, &t.UserID, &t.Label, &t.TokenHash, &t.CreatedAt, &t.ExpiresAt, &t.RevokedAt, &t.LastUsedAt); err != nil {
			return nil, err
		}
		result = append(result, t)
	}
	return result, rows.Err()
}

func (r *appPasswordRepo) ListByUser(ctx context.Context, userID int64) ([]AppPassword, error) {
	const q = `SELECT id, user_id, label, token_hash, created_at, expires_at, revoked_at, last_used_at FROM app_passwords WHERE user_id=$1 ORDER BY created_at DESC`
	defer observeDB(ctx, "app_passwords.list_by_user")()
	rows, err := r.pool.Query(ctx, q, userID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var result []AppPassword
	for rows.Next() {
		var t AppPassword
		if err := rows.Scan(&t.ID, &t.UserID, &t.Label, &t.TokenHash, &t.CreatedAt, &t.ExpiresAt, &t.RevokedAt, &t.LastUsedAt); err != nil {
			return nil, err
		}
		result = append(result, t)
	}
	return result, rows.Err()
}

func (r *appPasswordRepo) GetByID(ctx context.Context, id int64) (*AppPassword, error) {
	const q = `SELECT id, user_id, label, token_hash, created_at, expires_at, revoked_at, last_used_at FROM app_passwords WHERE id=$1`
	defer observeDB(ctx, "app_passwords.get_by_id")()
	var t AppPassword
	if err := r.pool.QueryRow(ctx, q, id).Scan(&t.ID, &t.UserID, &t.Label, &t.TokenHash, &t.CreatedAt, &t.ExpiresAt, &t.RevokedAt, &t.LastUsedAt); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, nil
		}
		return nil, err
	}
	return &t, nil
}

func (r *appPasswordRepo) Revoke(ctx context.Context, id int64) error {
	const q = `UPDATE app_passwords SET revoked_at = NOW() WHERE id=$1`
	defer observeDB(ctx, "app_passwords.revoke")()
	_, err := r.pool.Exec(ctx, q, id)
	return err
}

func (r *appPasswordRepo) TouchLastUsed(ctx context.Context, id int64) error {
	const q = `UPDATE app_passwords SET last_used_at = NOW() WHERE id=$1`
	defer observeDB(ctx, "app_passwords.touch_last_used")()
	_, err := r.pool.Exec(ctx, q, id)
	return err
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
	const countQuery = `SELECT COUNT(1) FROM calendars WHERE user_id=$1`
	defer observeDB(ctx, "calendars.ensure_default")()
	var count int
	if err := s.pool.QueryRow(ctx, countQuery, userID).Scan(&count); err != nil {
		return err
	}
	if count > 0 {
		return nil
	}
	_, err := s.Calendars.Create(ctx, Calendar{UserID: userID, Name: "Default"})
	return err
}

func (s *Store) ensureDefaultAddressBook(ctx context.Context, userID int64) error {
	const countQuery = `SELECT COUNT(1) FROM address_books WHERE user_id=$1`
	defer observeDB(ctx, "address_books.ensure_default")()
	var count int
	if err := s.pool.QueryRow(ctx, countQuery, userID).Scan(&count); err != nil {
		return err
	}
	if count > 0 {
		return nil
	}
	_, err := s.AddressBooks.Create(ctx, AddressBook{UserID: userID, Name: "Contacts"})
	return err
}

// Now returns a UTC timestamp to keep updates consistent.
func Now() time.Time {
	return time.Now().UTC()
}
