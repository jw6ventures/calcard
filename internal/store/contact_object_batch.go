package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
)

// ContactObjectOutcome is what one write of a PutContactObjects batch came to:
// the result and error PutContactObject would have returned for it.
type ContactObjectOutcome struct {
	Result *ContactObjectWriteResult
	Err    error
}

// MaxContactObjectBatch is the most writes PutContactObjects takes. Each write
// that changes a row takes a subtransaction ID, and PostgreSQL caches 64 per
// backend: a transaction with more overflows the cache, and every snapshot taken
// in the database meanwhile has to consult pg_subtrans. A batch stays below it.
const MaxContactObjectBatch = 50

// ErrContactBatchTooLarge refuses a PutContactObjects batch of more than
// MaxContactObjectBatch writes.
var ErrContactBatchTooLarge = errors.New("contact batch exceeds the subtransaction bound")

const (
	contactBatchSavepoint = "SAVEPOINT contact_batch"
	contactBatchRelease   = "RELEASE SAVEPOINT contact_batch"
	contactBatchRollback  = "ROLLBACK TO SAVEPOINT contact_batch"
)

// PutContactObjects writes the contacts of one address book in a single
// transaction, each under a savepoint, so one write the store refuses costs
// that write and not the batch. A batch of more than MaxContactObjectBatch
// writes is refused with ErrContactBatchTooLarge. Every write gets what
// PutContactObject gives it: the lock, ACL and state preconditions checked under lock, and the
// CTag, ETag and sync stamps the database assigns. The identity locks of the
// whole batch are taken in sorted order before the collection row, as a DAV
// write takes its own, so a batch cannot deadlock with one.
//
// A write refused for a reason PutContactObject reports as an error is
// reported in its outcome. An error is returned only when the batch itself
// cannot be run, and then nothing is stored. A store without a connection pool
// has no transaction to hold the batch in: it writes one at a time, and an
// error there leaves the writes before it stored.
func (s *Store) PutContactObjects(ctx context.Context, writes []ContactObjectWrite) ([]ContactObjectOutcome, error) {
	if len(writes) == 0 {
		return nil, nil
	}
	if s == nil || s.Contacts == nil {
		return nil, ErrNotFound
	}
	if len(writes) > MaxContactObjectBatch {
		return nil, fmt.Errorf("%w: %d writes", ErrContactBatchTooLarge, len(writes))
	}
	addressBookID := writes[0].AddressBookID
	batch := make([]ContactObjectWrite, len(writes))
	for i, write := range writes {
		if write.AddressBookID != addressBookID {
			return nil, fmt.Errorf("contact batch spans address books %d and %d", addressBookID, write.AddressBookID)
		}
		if write.ResourceName == "" {
			write.ResourceName = write.UID
		}
		batch[i] = write
	}
	outcomes := make([]ContactObjectOutcome, len(batch))

	if s.pool == nil {
		for i, write := range batch {
			result, err := s.PutContactObject(ctx, write)
			if err != nil && !isContactBatchRefusal(err) {
				return nil, err
			}
			outcomes[i] = ContactObjectOutcome{Result: result, Err: err}
		}
		return outcomes, nil
	}

	tx, err := s.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()

	var preconditions []LockPrecondition
	for _, write := range batch {
		preconditions = append(preconditions, write.LockPreconditions...)
	}
	if err := serializeLockPreconditionsTx(ctx, tx, preconditions, addressBookCollectionLockPath(addressBookID)); err != nil {
		return nil, err
	}
	locks, err := activeLocksForPreconditionsTx(ctx, tx, preconditions)
	if err != nil {
		return nil, err
	}

	var states []DAVResourceState
	var admitted []int
	for i, write := range batch {
		if !LockPreconditionsSatisfied(write.LockPreconditions, locks) {
			outcomes[i].Err = ErrLockConflict
			continue
		}
		if err := validateACLGuardTx(ctx, tx, write.ExpectedState.ACL); err != nil {
			if !errors.Is(err, ErrResourceStateChanged) {
				return nil, err
			}
			outcomes[i].Err = err
			continue
		}
		states = append(states, DAVResourceState{UID: write.UID, ResourceName: write.ResourceName})
		admitted = append(admitted, i)
	}
	if err := lockContactWritesTx(ctx, tx, addressBookID, states...); err != nil {
		return nil, err
	}

	if len(admitted) > 0 {
		if _, err := tx.ExecContext(ctx, contactBatchSavepoint); err != nil {
			return nil, err
		}
	}
	for _, i := range admitted {
		result, err := putContactObjectTx(ctx, tx, batch[i])
		if err == nil {
			outcomes[i] = ContactObjectOutcome{Result: result}
			// Releasing and re-establishing the savepoint in one statement saves a
			// round trip. The released savepoint's subtransaction ID stays with
			// the transaction until it commits, so the batch's size, not this
			// statement, bounds how many a transaction holds.
			if _, err := tx.ExecContext(ctx, contactBatchRelease+"; "+contactBatchSavepoint); err != nil {
				return nil, err
			}
			continue
		}
		if !isContactBatchRefusal(err) {
			return nil, err
		}
		if _, err := tx.ExecContext(ctx, contactBatchRollback); err != nil {
			return nil, err
		}
		outcomes[i] = ContactObjectOutcome{Result: result, Err: err}
	}
	if len(admitted) > 0 {
		if _, err := tx.ExecContext(ctx, contactBatchRelease); err != nil {
			return nil, err
		}
	}
	if err := tx.Commit(); err != nil {
		return nil, err
	}
	return outcomes, nil
}

// lockContactWritesTx takes what a write of the contacts in states holds for
// the rest of its transaction: their identity locks in sorted order, then the
// address book's row. Every contact write takes them in this order, so writes
// to overlapping contacts cannot deadlock.
func lockContactWritesTx(ctx context.Context, tx *sql.Tx, addressBookID int64, states ...DAVResourceState) error {
	if err := acquireDAVObjectIdentityLocks(ctx, tx, "contact-object", addressBookID, states...); err != nil {
		return err
	}
	return lockCollectionRowsTx(ctx, tx, "address_books", addressBookID)
}

// isContactBatchRefusal reports whether err is a write the store refuses, as
// opposed to the database or the batch failing.
func isContactBatchRefusal(err error) bool {
	return errors.Is(err, ErrLockConflict) || errors.Is(err, ErrResourceStateChanged) ||
		errors.Is(err, ErrUIDConflict) || errors.Is(err, ErrConflict) ||
		errors.Is(err, ErrPreconditionFailed) || IsDataError(err)
}
