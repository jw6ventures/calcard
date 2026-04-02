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
