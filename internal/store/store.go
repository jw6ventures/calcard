package store

import "github.com/jackc/pgx/v5/pgxpool"

// Store aggregates repositories backed by PostgreSQL.
type Store struct {
	pool *pgxpool.Pool

	Users        UserRepository
	Calendars    CalendarRepository
	Events       EventRepository
	AddressBooks AddressBookRepository
	Contacts     ContactRepository
	AppPasswords AppPasswordRepository
}

// New wires concrete repository implementations with shared connection pool.
func New(pool *pgxpool.Pool) *Store {
	return &Store{
		pool:         pool,
		Users:        &userRepo{pool: pool},
		Calendars:    &calendarRepo{pool: pool},
		Events:       &eventRepo{pool: pool},
		AddressBooks: &addressBookRepo{pool: pool},
		Contacts:     &contactRepo{pool: pool},
		AppPasswords: &appPasswordRepo{pool: pool},
	}
}

// TODO: add transactional helpers and connection health checks.
