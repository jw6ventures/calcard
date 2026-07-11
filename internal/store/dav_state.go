package store

import (
	"context"
	"database/sql"
	"path"
	"strings"
)

func (s *Store) DeleteEventAndState(ctx context.Context, calendarID int64, uid, resourcePath string) error {
	if s == nil || s.pool == nil {
		if s == nil || s.Events == nil {
			return ErrNotFound
		}
		if err := s.Events.DeleteByUID(ctx, calendarID, uid); err != nil {
			return err
		}
		return s.deleteDAVStateFallback(ctx, resourcePath, true)
	}

	tx, err := s.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	res, err := tx.ExecContext(ctx, `DELETE FROM events WHERE calendar_id=$1 AND uid=$2`, calendarID, uid)
	if err != nil {
		return err
	}
	rows, err := res.RowsAffected()
	if err != nil {
		return err
	}
	if rows == 0 {
		return ErrNotFound
	}
	if err := deleteDAVStateTx(ctx, tx, resourcePath, true); err != nil {
		return err
	}
	return tx.Commit()
}

func (s *Store) DeleteContactAndState(ctx context.Context, addressBookID int64, uid, resourcePath string) error {
	if s == nil || s.pool == nil {
		if s == nil || s.Contacts == nil {
			return ErrNotFound
		}
		if err := s.Contacts.DeleteByUID(ctx, addressBookID, uid); err != nil {
			return err
		}
		return s.deleteDAVStateFallback(ctx, resourcePath, false)
	}

	tx, err := s.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	res, err := tx.ExecContext(ctx, `DELETE FROM contacts WHERE address_book_id=$1 AND uid=$2`, addressBookID, uid)
	if err != nil {
		return err
	}
	rows, err := res.RowsAffected()
	if err != nil {
		return err
	}
	if rows == 0 {
		return ErrNotFound
	}
	if err := deleteDAVStateTx(ctx, tx, resourcePath, false); err != nil {
		return err
	}
	return tx.Commit()
}

func deleteDAVStateTx(ctx context.Context, tx execContext, resourcePath string, deleteACL bool) error {
	for _, statePath := range davStatePaths(resourcePath) {
		if _, err := tx.ExecContext(ctx, `DELETE FROM locks WHERE resource_path=$1`, statePath); err != nil {
			return err
		}
		if deleteACL {
			if _, err := tx.ExecContext(ctx, `DELETE FROM acl_entries WHERE resource_path=$1`, statePath); err != nil {
				return err
			}
		}
	}
	return nil
}

func (s *Store) deleteDAVStateFallback(ctx context.Context, resourcePath string, deleteACL bool) error {
	if resourcePath == "" || s == nil {
		return nil
	}
	for _, statePath := range davStatePaths(resourcePath) {
		if s.Locks != nil {
			if err := s.Locks.DeleteByResourcePath(ctx, statePath); err != nil {
				return err
			}
		}
		if deleteACL && s.ACLEntries != nil {
			if err := s.ACLEntries.Delete(ctx, statePath); err != nil {
				return err
			}
		}
	}
	return nil
}

func davStatePaths(resourcePath string) []string {
	resourcePath = strings.TrimSpace(resourcePath)
	if resourcePath == "" {
		return nil
	}

	seen := map[string]struct{}{}
	var paths []string
	addPath := func(p string) {
		if p == "" {
			return
		}
		if _, ok := seen[p]; ok {
			return
		}
		seen[p] = struct{}{}
		paths = append(paths, p)
	}

	addPath(resourcePath)
	switch {
	case strings.HasPrefix(resourcePath, "/dav/addressbooks/"):
		base := resourcePath
		if strings.EqualFold(path.Ext(base), ".vcf") {
			base = strings.TrimSuffix(base, path.Ext(base))
			addPath(base)
			addPath(resourcePath)
			return paths
		}
		addPath(base + ".vcf")
	case strings.HasPrefix(resourcePath, "/dav/calendars/"):
		base := resourcePath
		if strings.EqualFold(path.Ext(base), ".ics") {
			base = strings.TrimSuffix(base, path.Ext(base))
			addPath(base)
			addPath(resourcePath)
			return paths
		}
		addPath(base + ".ics")
	}
	return paths
}

type execContext interface {
	ExecContext(context.Context, string, ...any) (sql.Result, error)
}

type queryExecContext interface {
	execContext
	QueryRowContext(context.Context, string, ...any) *sql.Row
}

// MoveEventAndState moves an event between calendars and, in the same
// transaction, rebinds DAV lock/ACL state from fromStatePath to toStatePath
// and clears any tombstone left by a previously deleted resource at the
// destination (replacedUID; empty means nothing was overwritten). Without a
// connection pool (unit-test fakes) it falls back to sequential repository
// calls with no rollback.
func (s *Store) MoveEventAndState(ctx context.Context, fromCalendarID, toCalendarID int64, uid, destResourceName, fromStatePath, toStatePath, replacedUID string) error {
	if s == nil || s.Events == nil {
		return ErrNotFound
	}
	if s.pool == nil {
		if err := s.Events.MoveToCalendar(ctx, fromCalendarID, toCalendarID, uid, destResourceName); err != nil {
			return err
		}
		return s.moveDAVStateFallback(ctx, fromStatePath, toStatePath, "event", toCalendarID, replacedUID, destResourceName)
	}

	tx, err := s.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	if err := moveEventTx(ctx, tx, fromCalendarID, toCalendarID, uid, destResourceName); err != nil {
		return err
	}
	if err := moveDAVStateTx(ctx, tx, fromStatePath, toStatePath); err != nil {
		return err
	}
	if err := clearReplacedTombstoneTx(ctx, tx, "event", toCalendarID, replacedUID, destResourceName); err != nil {
		return err
	}
	return tx.Commit()
}

// MoveContactAndState is the contact counterpart of MoveEventAndState.
func (s *Store) MoveContactAndState(ctx context.Context, fromAddressBookID, toAddressBookID int64, uid, destResourceName, fromStatePath, toStatePath, replacedUID string) error {
	if s == nil || s.Contacts == nil {
		return ErrNotFound
	}
	if s.pool == nil {
		if err := s.Contacts.MoveToAddressBook(ctx, fromAddressBookID, toAddressBookID, uid, destResourceName); err != nil {
			return err
		}
		return s.moveDAVStateFallback(ctx, fromStatePath, toStatePath, "contact", toAddressBookID, replacedUID, destResourceName)
	}

	tx, err := s.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	if err := moveContactTx(ctx, tx, fromAddressBookID, toAddressBookID, uid, destResourceName); err != nil {
		return err
	}
	if err := moveDAVStateTx(ctx, tx, fromStatePath, toStatePath); err != nil {
		return err
	}
	if err := clearReplacedTombstoneTx(ctx, tx, "contact", toAddressBookID, replacedUID, destResourceName); err != nil {
		return err
	}
	return tx.Commit()
}

func moveDAVStateTx(ctx context.Context, tx execContext, fromPath, toPath string) error {
	if fromPath == "" || toPath == "" || fromPath == toPath {
		return nil
	}
	if _, err := tx.ExecContext(ctx, `DELETE FROM acl_entries WHERE resource_path=$1`, toPath); err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, `UPDATE acl_entries SET resource_path=$1 WHERE resource_path=$2`, toPath, fromPath); err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, `DELETE FROM locks WHERE resource_path=$1 AND expires_at > NOW()`, toPath); err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, `UPDATE locks SET resource_path=$1 WHERE resource_path=$2 AND expires_at > NOW()`, toPath, fromPath); err != nil {
		return err
	}
	return nil
}

func clearReplacedTombstoneTx(ctx context.Context, tx execContext, resourceType string, collectionID int64, replacedUID, resourceName string) error {
	if replacedUID == "" {
		return nil
	}
	_, err := tx.ExecContext(ctx, `DELETE FROM deleted_resources WHERE resource_type=$1 AND collection_id=$2 AND uid=$3 AND resource_name=$4`, resourceType, collectionID, replacedUID, resourceName)
	return err
}

func (s *Store) moveDAVStateFallback(ctx context.Context, fromPath, toPath, resourceType string, collectionID int64, replacedUID, resourceName string) error {
	if fromPath != "" && toPath != "" && fromPath != toPath {
		if s.ACLEntries != nil {
			if err := s.ACLEntries.MoveResourcePath(ctx, fromPath, toPath); err != nil {
				return err
			}
		}
		if s.Locks != nil {
			if err := s.Locks.MoveResourcePath(ctx, fromPath, toPath); err != nil {
				return err
			}
		}
	}
	if replacedUID != "" && s.DeletedResources != nil {
		return s.DeletedResources.DeleteByIdentity(ctx, resourceType, collectionID, replacedUID, resourceName)
	}
	return nil
}
