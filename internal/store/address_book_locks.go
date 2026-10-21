package store

import (
	"context"
	"time"
)

// CheckLocks reports ErrLockConflict when an active lock refuses a write that
// meets lockPreconditions. It is a read ahead of a write, which checks the
// same preconditions again under lock.
func (s *Store) CheckLocks(ctx context.Context, lockPreconditions []LockPrecondition) error {
	if s == nil {
		return nil
	}
	return validateLockPreconditionsFallback(ctx, s.Locks, lockPreconditions)
}

// RenameAddressBookAndState renames the address book of userID as a PROPPATCH of
// its display name does: refused with ErrLockConflict, in the transaction that
// writes the name, when lockPreconditions fail.
func (s *Store) RenameAddressBookAndState(ctx context.Context, userID, addressBookID int64, name string, lockPreconditions []LockPrecondition) error {
	if s == nil || s.AddressBooks == nil {
		return ErrNotFound
	}
	if s.pool == nil {
		if err := validateLockPreconditionsFallback(ctx, s.Locks, lockPreconditions); err != nil {
			return err
		}
		return s.AddressBooks.Rename(ctx, userID, addressBookID, name)
	}
	tx, err := s.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if err := validateLockPreconditionsTx(ctx, tx, lockPreconditions, addressBookCollectionLockPath(addressBookID)); err != nil {
		return err
	}
	result, err := tx.ExecContext(ctx, `UPDATE address_books SET name=$1, updated_at=NOW() WHERE id=$2 AND user_id=$3`, name, addressBookID, userID)
	if err != nil {
		if isAddressBookNameConflict(err) {
			return ErrConflict
		}
		return err
	}
	if err := requireAffectedRow(result); err != nil {
		return err
	}
	return tx.Commit()
}

// UpdateACLAndState is ACLRepository.UpdateACL refused with ErrLockConflict, in
// the transaction that writes the ACL, when lockPreconditions fail, as the ACL
// method is refused on a locked resource, and with ErrNotFound when the
// calendar or address book the resource is or lies in no longer exists, so no
// entries are written for it.
func (s *Store) UpdateACLAndState(ctx context.Context, resourcePath string, lockPreconditions []LockPrecondition, mutate func([]ACLEntry) ([]ACLEntry, error)) error {
	if s == nil || s.ACLEntries == nil {
		return ErrNotFound
	}
	if s.pool == nil {
		if err := validateLockPreconditionsFallback(ctx, s.Locks, lockPreconditions); err != nil {
			return err
		}
		return s.ACLEntries.UpdateACL(ctx, resourcePath, mutate)
	}
	tx, err := s.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	// The locks are taken, and the lock conditions checked, before the ACL is
	// read: mutate derives the new ACL from that read.
	if err := validateLockPreconditionsTx(ctx, tx, lockPreconditions, resourcePath); err != nil {
		return err
	}
	if err := requireCollectionTx(ctx, tx, resourcePath); err != nil {
		return err
	}
	if err := updateACLTx(ctx, tx, resourcePath, mutate); err != nil {
		return err
	}
	return tx.Commit()
}

// DeleteAddressBookAndState deletes the address book of userID as a collection
// DELETE is refused under a lock: with ErrLockConflict, in the deleting
// transaction, when lockPreconditions fail or any active lock lies on a
// resource below the book, a contact or a name no contact holds.
func (s *Store) DeleteAddressBookAndState(ctx context.Context, userID, addressBookID int64, lockPreconditions []LockPrecondition) error {
	if s == nil || s.AddressBooks == nil {
		return ErrNotFound
	}
	collection := addressBookCollectionLockPath(addressBookID)
	if s.pool == nil {
		if err := validateLockPreconditionsFallback(ctx, s.Locks, lockPreconditions); err != nil {
			return err
		}
		if s.Locks != nil {
			below, err := s.Locks.ListByResourcePrefix(ctx, collection+"/")
			if err != nil {
				return err
			}
			now := time.Now()
			for _, lock := range below {
				if !lock.ExpiresAt.Before(now) {
					return ErrLockConflict
				}
			}
		}
		return s.AddressBooks.Delete(ctx, userID, addressBookID)
	}
	tx, err := s.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if err := validateLockPreconditionsTx(ctx, tx, lockPreconditions, collection); err != nil {
		return err
	}
	if err := validateLocksBelowTx(ctx, tx, []string{collection}, nil); err != nil {
		return err
	}
	result, err := tx.ExecContext(ctx, `DELETE FROM address_books WHERE id=$1 AND user_id=$2`, addressBookID, userID)
	if err != nil {
		return err
	}
	if err := requireAffectedRow(result); err != nil {
		return err
	}
	// No active lock lies below the book, so the lock rows left are expired.
	if err := deleteCalendarTreeStateTx(ctx, tx, collection, true); err != nil {
		return err
	}
	return tx.Commit()
}
