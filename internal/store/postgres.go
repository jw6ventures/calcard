package store

import (
	"context"

	"github.com/jackc/pgx/v5/pgxpool"
)

// userRepo implements UserRepository.
type userRepo struct {
	pool *pgxpool.Pool
}

func (r *userRepo) UpsertOAuthUser(ctx context.Context, subject, email string) (*User, error) {
	// TODO: insert or update user, ensure default calendar/address book creation occurs elsewhere.
	return nil, ErrNotImplemented
}

func (r *userRepo) GetByID(ctx context.Context, id int64) (*User, error) {
	// TODO: query by id.
	return nil, ErrNotImplemented
}

func (r *userRepo) GetByEmail(ctx context.Context, email string) (*User, error) {
	// TODO: query by email.
	return nil, ErrNotImplemented
}

// calendarRepo implements CalendarRepository.
type calendarRepo struct {
	pool *pgxpool.Pool
}

func (r *calendarRepo) ListByUser(ctx context.Context, userID int64) ([]Calendar, error) {
	return nil, ErrNotImplemented
}

func (r *calendarRepo) Create(ctx context.Context, cal Calendar) (*Calendar, error) {
	return nil, ErrNotImplemented
}

func (r *calendarRepo) Rename(ctx context.Context, id int64, name string) error {
	return ErrNotImplemented
}

func (r *calendarRepo) Delete(ctx context.Context, id int64) error {
	return ErrNotImplemented
}

// eventRepo implements EventRepository.
type eventRepo struct {
	pool *pgxpool.Pool
}

func (r *eventRepo) Upsert(ctx context.Context, event Event) (*Event, error) {
	return nil, ErrNotImplemented
}

func (r *eventRepo) DeleteByUID(ctx context.Context, calendarID int64, uid string) error {
	return ErrNotImplemented
}

func (r *eventRepo) GetByUID(ctx context.Context, calendarID int64, uid string) (*Event, error) {
	return nil, ErrNotImplemented
}

func (r *eventRepo) ListForCalendar(ctx context.Context, calendarID int64) ([]Event, error) {
	return nil, ErrNotImplemented
}

// addressBookRepo implements AddressBookRepository.
type addressBookRepo struct {
	pool *pgxpool.Pool
}

func (r *addressBookRepo) ListByUser(ctx context.Context, userID int64) ([]AddressBook, error) {
	return nil, ErrNotImplemented
}

func (r *addressBookRepo) Create(ctx context.Context, book AddressBook) (*AddressBook, error) {
	return nil, ErrNotImplemented
}

func (r *addressBookRepo) Rename(ctx context.Context, id int64, name string) error {
	return ErrNotImplemented
}

func (r *addressBookRepo) Delete(ctx context.Context, id int64) error {
	return ErrNotImplemented
}

// contactRepo implements ContactRepository.
type contactRepo struct {
	pool *pgxpool.Pool
}

func (r *contactRepo) Upsert(ctx context.Context, contact Contact) (*Contact, error) {
	return nil, ErrNotImplemented
}

func (r *contactRepo) DeleteByUID(ctx context.Context, addressBookID int64, uid string) error {
	return ErrNotImplemented
}

func (r *contactRepo) GetByUID(ctx context.Context, addressBookID int64, uid string) (*Contact, error) {
	return nil, ErrNotImplemented
}

func (r *contactRepo) ListForBook(ctx context.Context, addressBookID int64) ([]Contact, error) {
	return nil, ErrNotImplemented
}

// appPasswordRepo implements AppPasswordRepository.
type appPasswordRepo struct {
	pool *pgxpool.Pool
}

func (r *appPasswordRepo) Create(ctx context.Context, token AppPassword) (*AppPassword, error) {
	return nil, ErrNotImplemented
}

func (r *appPasswordRepo) FindActive(ctx context.Context, userID int64, hash string) (*AppPassword, error) {
	return nil, ErrNotImplemented
}

func (r *appPasswordRepo) ListByUser(ctx context.Context, userID int64) ([]AppPassword, error) {
	return nil, ErrNotImplemented
}

func (r *appPasswordRepo) Revoke(ctx context.Context, id int64) error {
	return ErrNotImplemented
}

func (r *appPasswordRepo) TouchLastUsed(ctx context.Context, id int64) error {
	return ErrNotImplemented
}

// ErrNotImplemented is returned for stubbed persistence methods.
var ErrNotImplemented = &NotImplementedError{}

// NotImplementedError is a sentinel error placeholder.
type NotImplementedError struct{}

func (e *NotImplementedError) Error() string { return "not implemented" }
