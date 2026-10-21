package store

import (
	"context"
	"database/sql"
	"errors"
	"strconv"
	"strings"
)

// requireCollectionTx is ErrNotFound when resourcePath is, or lies in, a
// calendar or address book that no longer exists. The row is held while the
// transaction runs, so a collection deleted meanwhile waits for it. A path in
// neither kind of collection is not checked.
func requireCollectionTx(ctx context.Context, tx *sql.Tx, resourcePath string) error {
	table, id, ok := collectionOfPath(resourcePath)
	if !ok {
		return nil
	}
	var present int
	err := tx.QueryRowContext(ctx, `SELECT 1 FROM `+table+` WHERE id=$1 FOR KEY SHARE`, id).Scan(&present)
	if errors.Is(err, sql.ErrNoRows) {
		return ErrNotFound
	}
	return err
}

// collectionOfPath is the table and ID of the calendar or address book that
// resourcePath is or lies in, and whether it is in either.
func collectionOfPath(resourcePath string) (table string, id int64, ok bool) {
	var rest string
	switch {
	case strings.HasPrefix(resourcePath, "/dav/calendars/"):
		table, rest = "calendars", strings.TrimPrefix(resourcePath, "/dav/calendars/")
	case strings.HasPrefix(resourcePath, "/dav/addressbooks/"):
		table, rest = "address_books", strings.TrimPrefix(resourcePath, "/dav/addressbooks/")
	default:
		return "", 0, false
	}
	segment, _, _ := strings.Cut(rest, "/")
	id, err := strconv.ParseInt(segment, 10, 64)
	if err != nil {
		return "", 0, false
	}
	return table, id, true
}

// RenameCalendarAndState renames the calendar of userID as a PROPPATCH of its
// display name does: only the name, and the colour when setColor is set, are
// written, so a property a client changed meanwhile is kept, and the rename is
// refused with ErrLockConflict, in the transaction that writes it, when
// lockPreconditions fail.
func (s *Store) RenameCalendarAndState(ctx context.Context, userID, calendarID int64, name string, color *string, setColor bool, lockPreconditions []LockPrecondition) error {
	if s == nil || s.Calendars == nil {
		return ErrNotFound
	}
	if s.pool == nil {
		if err := validateLockPreconditionsFallback(ctx, s.Locks, lockPreconditions); err != nil {
			return err
		}
		current, err := s.Calendars.GetByID(ctx, calendarID)
		if err != nil {
			return err
		}
		if current == nil || current.UserID != userID {
			return ErrNotFound
		}
		if !setColor {
			color = current.Color
		}
		return s.Calendars.Update(ctx, userID, calendarID, name, current.Description, current.Timezone, color)
	}
	tx, err := s.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if err := validateLockPreconditionsTx(ctx, tx, lockPreconditions, calendarCollectionLockPath(calendarID)); err != nil {
		return err
	}
	var result sql.Result
	if setColor {
		result, err = tx.ExecContext(ctx, `UPDATE calendars SET name=$1, color=$2, updated_at=NOW() WHERE id=$3 AND user_id=$4`, name, color, calendarID, userID)
	} else {
		result, err = tx.ExecContext(ctx, `UPDATE calendars SET name=$1, updated_at=NOW() WHERE id=$2 AND user_id=$3`, name, calendarID, userID)
	}
	if err != nil {
		return err
	}
	if err := requireAffectedRow(result); err != nil {
		return err
	}
	return tx.Commit()
}
