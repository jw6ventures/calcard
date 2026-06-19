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
	Events           EventRepository
	AddressBooks     AddressBookRepository
	Contacts         ContactRepository
	AppPasswords     AppPasswordRepository
	DeletedResources DeletedResourceRepository
	Sessions         SessionRepository
	Locks            LockRepository
	ACLEntries       ACLRepository
}

// New wires concrete repository implementations with shared connection pool.
func New(pool *sql.DB) *Store {
	return &Store{
		pool:             pool,
		Users:            &userRepo{pool: pool},
		Calendars:        &calendarRepo{pool: pool},
		Events:           &eventRepo{pool: pool},
		AddressBooks:     &addressBookRepo{pool: pool},
		Contacts:         &contactRepo{pool: pool},
		AppPasswords:     &appPasswordRepo{pool: pool},
		DeletedResources: &deletedResourceRepo{pool: pool},
		Sessions:         &sessionRepo{pool: pool},
		Locks:            &lockRepo{pool: pool},
		ACLEntries:       &aclRepo{pool: pool},
	}
}

// BeginTx starts a transaction with default options.
func (s *Store) BeginTx(ctx context.Context, opts *sql.TxOptions) (*sql.Tx, error) {
	defer observeDB(ctx, "db.begin_tx")()
	tx, err := s.pool.BeginTx(ctx, opts)
	if err != nil {
		// Always Error here: a failed transaction fails the caller's request
		// regardless of cause. isConnError only refines the message. (The
		// background lock-cleanup sweep instead downgrades non-conn errors to
		// Warn, since a missed sweep is not request-fatal.)
		if isConnError(err) {
			queryLogger.Error("db.begin_tx", "could not start transaction, database appears unreachable: %v", err)
		} else {
			queryLogger.Error("db.begin_tx", "could not start transaction: %v", err)
		}
	}
	return tx, err
}

// HealthCheck verifies that the underlying database is reachable.
func (s *Store) HealthCheck(ctx context.Context) error {
	defer observeDB(ctx, "db.healthcheck")()
	if err := s.pool.PingContext(ctx); err != nil {
		queryLogger.Warn("db.healthcheck", "database ping failed, marking unready: %v", err)
		return err
	}
	return nil
}
