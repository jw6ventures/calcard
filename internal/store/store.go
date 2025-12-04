package store

import (
	"context"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// Store aggregates repositories backed by PostgreSQL.
type Store struct {
	pool *pgxpool.Pool

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
func New(pool *pgxpool.Pool) *Store {
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
func (s *Store) BeginTx(ctx context.Context, opts pgx.TxOptions) (pgx.Tx, error) {
	defer observeDB(ctx, "db.begin_tx")()
	return s.pool.BeginTx(ctx, opts)
}

// HealthCheck verifies that the underlying database is reachable.
func (s *Store) HealthCheck(ctx context.Context) error {
	defer observeDB(ctx, "db.healthcheck")()
	return s.pool.Ping(ctx)
}
