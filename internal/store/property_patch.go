package store

import (
	"context"
	"database/sql"
)

func (s *Store) PatchCalendarProperties(ctx context.Context, calendarID int64, name string, description, timezone, color *string, resourcePath string, dead []DeadPropertyMutation) error {
	if s == nil || s.Calendars == nil {
		return ErrNotFound
	}
	if s.pool == nil {
		if err := s.Calendars.UpdateProperties(ctx, calendarID, name, description, timezone, color); err != nil {
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
	result, err := tx.ExecContext(ctx, `UPDATE calendars SET name=$1, description=$2, timezone=$3, color=$4, updated_at=NOW() WHERE id=$5`, name, description, timezone, color, calendarID)
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

func (s *Store) PatchAddressBookProperties(ctx context.Context, addressBookID int64, name string, description *string, resourcePath string, dead []DeadPropertyMutation) error {
	if s == nil || s.AddressBooks == nil {
		return ErrNotFound
	}
	if s.pool == nil {
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

func (s *Store) PatchDeadProperties(ctx context.Context, resourcePath string, dead []DeadPropertyMutation) error {
	if len(dead) == 0 {
		return nil
	}
	if s == nil || s.DeadProperties == nil {
		return ErrNotFound
	}
	return s.DeadProperties.Apply(ctx, resourcePath, dead)
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
