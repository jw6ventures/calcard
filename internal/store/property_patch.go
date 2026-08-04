package store

import (
	"context"
	"database/sql"
)

func (s *Store) PatchCalendarProperties(ctx context.Context, calendarID int64, props CalendarProperties, resourcePath string, dead []DeadPropertyMutation, lockPreconditions []LockPrecondition) error {
	if s == nil || s.Calendars == nil {
		return ErrNotFound
	}
	if s.pool == nil {
		if err := validateLockPreconditionsFallback(ctx, s.Locks, lockPreconditions); err != nil {
			return err
		}
		if err := s.Calendars.UpdateProperties(ctx, calendarID, props); err != nil {
			return err
		}
		if s.DeadProperties != nil {
			return s.DeadProperties.Apply(ctx, resourcePath, dead)
		}
		return nil
	}

	tx, err := s.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if err := validateLockPreconditionsTx(ctx, tx, lockPreconditions); err != nil {
		return err
	}
	result, err := tx.ExecContext(ctx, `UPDATE calendars SET name=$1, description=$2, description_lang=$3, timezone=$4, color=$5, updated_at=NOW() WHERE id=$6`, props.Name, props.Description, props.DescriptionLang, props.Timezone, props.Color, calendarID)
	if err != nil {
		return err
	}
	if err := requireAffectedRow(result); err != nil {
		return err
	}
	if err := applyDeadPropertyMutationsTx(ctx, tx, resourcePath, dead); err != nil {
		return err
	}
	return tx.Commit()
}

func (s *Store) PatchAddressBookProperties(ctx context.Context, addressBookID int64, name string, description *string, resourcePath string, dead []DeadPropertyMutation, lockPreconditions []LockPrecondition) error {
	if s == nil || s.AddressBooks == nil {
		return ErrNotFound
	}
	if s.pool == nil {
		if err := validateLockPreconditionsFallback(ctx, s.Locks, lockPreconditions); err != nil {
			return err
		}
		if err := s.AddressBooks.UpdateProperties(ctx, addressBookID, name, description); err != nil {
			return err
		}
		if s.DeadProperties != nil {
			return s.DeadProperties.Apply(ctx, resourcePath, dead)
		}
		return nil
	}

	tx, err := s.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if err := validateLockPreconditionsTx(ctx, tx, lockPreconditions); err != nil {
		return err
	}
	result, err := tx.ExecContext(ctx, `UPDATE address_books SET name=$1, description=$2, updated_at=NOW() WHERE id=$3`, name, description, addressBookID)
	if err != nil {
		if isAddressBookNameConflict(err) {
			return ErrConflict
		}
		return err
	}
	if err := requireAffectedRow(result); err != nil {
		return err
	}
	if err := applyDeadPropertyMutationsTx(ctx, tx, resourcePath, dead); err != nil {
		return err
	}
	return tx.Commit()
}

func (s *Store) PatchObjectDeadProperties(ctx context.Context, kind string, collectionID int64, expected DAVResourceState, resourcePath string, dead []DeadPropertyMutation, lockPreconditions []LockPrecondition) error {
	if len(dead) == 0 {
		return nil
	}
	if s == nil || s.DeadProperties == nil {
		return ErrNotFound
	}
	if s.pool == nil {
		if err := validateLockPreconditionsFallback(ctx, s.Locks, lockPreconditions); err != nil {
			return err
		}
		switch kind {
		case "calendar":
			if s.Events == nil {
				return ErrNotFound
			}
			current, err := s.Events.GetByResourceName(ctx, collectionID, expected.ResourceName)
			if err != nil {
				return err
			}
			if !eventDAVStateMatches(expected, current) {
				return ErrResourceStateChanged
			}
		case "addressbook":
			if s.Contacts == nil {
				return ErrNotFound
			}
			current, err := s.Contacts.GetByResourceName(ctx, collectionID, expected.ResourceName)
			if err != nil {
				return err
			}
			if !contactDAVStateMatches(expected, current) {
				return ErrResourceStateChanged
			}
		default:
			return ErrNotFound
		}
		return s.DeadProperties.Apply(ctx, resourcePath, dead)
	}
	tx, err := s.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if err := validateLockPreconditionsTx(ctx, tx, lockPreconditions); err != nil {
		return err
	}
	switch kind {
	case "calendar":
		if err := validateCollectionCTagsTx(ctx, tx, "calendars",
			collectionCTagExpectation{id: collectionID, ctag: expected.CollectionCTag}); err != nil {
			return err
		}
		current, err := selectEventTx(ctx, tx, `resource_name`, collectionID, expected.ResourceName)
		if err != nil {
			return err
		}
		if !eventDAVStateMatches(expected, current) {
			return ErrResourceStateChanged
		}
	case "addressbook":
		if err := validateCollectionCTagsTx(ctx, tx, "address_books",
			collectionCTagExpectation{id: collectionID, ctag: expected.CollectionCTag}); err != nil {
			return err
		}
		current, err := selectContactTx(ctx, tx, `resource_name`, collectionID, expected.ResourceName)
		if err != nil {
			return err
		}
		if !contactDAVStateMatches(expected, current) {
			return ErrResourceStateChanged
		}
	default:
		return ErrNotFound
	}
	if err := applyDeadPropertyMutationsTx(ctx, tx, resourcePath, dead); err != nil {
		return err
	}
	return tx.Commit()
}

func requireAffectedRow(result sql.Result) error {
	rows, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if rows == 0 {
		return ErrNotFound
	}
	return nil
}
