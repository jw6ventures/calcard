package store

import (
	"context"
	"database/sql"
)

type txPool interface {
	BeginTx(ctx context.Context, opts *sql.TxOptions) (*sql.Tx, error)
	ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error)
	Stats() sql.DBStats
}

// Store aggregates repositories backed by PostgreSQL.
type Store struct {
	pool txPool

	Users               UserRepository
	Calendars           CalendarRepository
	CalendarState       CalendarStateCreator
	CalendarObjects     CalendarObjectWriter
	ContactObjects      ContactObjectWriter
	CalendarTransfers   CalendarObjectTransferBackend
	CalendarCollections CalendarCollectionTransferBackend
	Events              EventRepository
	AddressBooks        AddressBookRepository
	Contacts            ContactRepository
	AppPasswords        AppPasswordRepository
	DigestNonces        DigestNonceRepository
	DeletedResources    DeletedResourceRepository
	Sessions            SessionRepository
	Locks               LockRepository
	ACLEntries          ACLRepository
	DeadProperties      DeadPropertyRepository
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
		DigestNonces:     &digestNonceRepo{pool: pool},
		DeletedResources: &deletedResourceRepo{pool: pool},
		Sessions:         &sessionRepo{pool: pool},
		Locks:            &lockRepo{pool: pool},
		ACLEntries:       &aclRepo{pool: pool},
		DeadProperties:   &deadPropertyRepo{pool: pool},
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
//
// After the server restarts, every idle connection in the pool is dead, and a
// check that gives up on the first one keeps an idle replica unready for one
// probe per dead connection. database/sql retries a statement, though not a
// ping, on another connection when the driver reports a bad one; when the dead
// socket instead surfaces as a network error, the driver marks the connection
// bad and the pool discards it. Each failed attempt therefore retires at least
// one dead connection, and one more attempt than the pool holds idle
// connections reaches a fresh one.
func (s *Store) HealthCheck(ctx context.Context) error {
	defer observeDB(ctx, "db.healthcheck")()
	attempts := s.pool.Stats().Idle + 1
	var err error
	for range attempts {
		if _, err = s.pool.ExecContext(ctx, "SELECT 1"); err == nil || !isConnError(err) {
			break
		}
	}
	if err != nil {
		queryLogger.Warn("db.healthcheck", "database check failed, marking unready: %v", err)
		return err
	}
	return nil
}
