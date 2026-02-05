package store

import (
	"context"
	"database/sql"
)

type txPool interface {
	BeginTx(ctx context.Context, opts *sql.TxOptions) (*sql.Tx, error)
	PingContext(ctx context.Context) error
}

// Store aggregates repositories backed by PostgreSQL.
type Store struct {
	pool txPool

	Users            UserRepository
	Calendars        CalendarRepository
	CalendarShares   CalendarShareRepository
	Events           EventRepository
	AddressBooks     AddressBookRepository
	Contacts         ContactRepository
	AppPasswords     AppPasswordRepository
	DeletedResources DeletedResourceRepository
	Sessions         SessionRepository
}

// New wires concrete repository implementations with shared connection pool.
func New(pool *sql.DB) *Store {
	return &Store{
		pool:             pool,
		Users:            &userRepo{pool: pool},
		Calendars:        &calendarRepo{pool: pool},
		CalendarShares:   &calendarShareRepo{pool: pool},
		Events:           &eventRepo{pool: pool},
		AddressBooks:     &addressBookRepo{pool: pool},
		Contacts:         &contactRepo{pool: pool},
		AppPasswords:     &appPasswordRepo{pool: pool},
		DeletedResources: &deletedResourceRepo{pool: pool},
		Sessions:         &sessionRepo{pool: pool},
	}
}

// BeginTx starts a transaction with default options.
func (s *Store) BeginTx(ctx context.Context, opts *sql.TxOptions) (*sql.Tx, error) {
	defer observeDB(ctx, "db.begin_tx")()
	return s.pool.BeginTx(ctx, opts)
}

// HealthCheck verifies that the underlying database is reachable.
func (s *Store) HealthCheck(ctx context.Context) error {
	defer observeDB(ctx, "db.healthcheck")()
	return s.pool.PingContext(ctx)
}
