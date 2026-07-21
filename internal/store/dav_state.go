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
		return s.deleteDAVStateFallback(ctx, resourcePath, true)
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
	if err := deleteDAVStateTx(ctx, tx, resourcePath, true); err != nil {
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
		if _, err := tx.ExecContext(ctx, `DELETE FROM dav_dead_properties WHERE resource_path=$1`, statePath); err != nil {
			return err
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
		if err := s.clearDeadPropertiesFallback(ctx, statePath); err != nil {
			return err
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
// destination. Without a
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
	if err := clearDestinationTombstonesTx(ctx, tx, "event", toCalendarID, destResourceName); err != nil {
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
	if err := clearDestinationTombstonesTx(ctx, tx, "contact", toAddressBookID, destResourceName); err != nil {
		return err
	}
	return tx.Commit()
}

func moveDAVStateTx(ctx context.Context, tx execContext, fromPath, toPath string) error {
	if fromPath == "" || toPath == "" || fromPath == toPath {
		return nil
	}
	fromPaths := davStatePaths(fromPath)
	toPaths := davStatePaths(toPath)
	for i, sourcePath := range fromPaths {
		destinationPath := toPath
		if i < len(toPaths) {
			destinationPath = toPaths[i]
		}
		if _, err := tx.ExecContext(ctx, `DELETE FROM acl_entries WHERE resource_path=$1`, destinationPath); err != nil {
			return err
		}
		if _, err := tx.ExecContext(ctx, `UPDATE acl_entries SET resource_path=$1 WHERE resource_path=$2`, destinationPath, sourcePath); err != nil {
			return err
		}
		if _, err := tx.ExecContext(ctx, `DELETE FROM locks WHERE resource_path=$1`, destinationPath); err != nil {
			return err
		}
		if _, err := tx.ExecContext(ctx, `UPDATE locks SET resource_path=$1 WHERE resource_path=$2 AND expires_at > NOW()`, destinationPath, sourcePath); err != nil {
			return err
		}
		if _, err := tx.ExecContext(ctx, `DELETE FROM dav_dead_properties WHERE resource_path=$1`, destinationPath); err != nil {
			return err
		}
		if _, err := tx.ExecContext(ctx, `UPDATE dav_dead_properties SET resource_path=$1, updated_at=NOW() WHERE resource_path=$2`, destinationPath, sourcePath); err != nil {
			return err
		}
	}
	return nil
}

func clearDestinationTombstonesTx(ctx context.Context, tx execContext, resourceType string, collectionID int64, resourceName string) error {
	_, err := tx.ExecContext(ctx, `DELETE FROM deleted_resources WHERE resource_type=$1 AND collection_id=$2 AND resource_name=$3`, resourceType, collectionID, resourceName)
	return err
}

func (s *Store) moveDAVStateFallback(ctx context.Context, fromPath, toPath, resourceType string, collectionID int64, replacedUID, resourceName string) error {
	if fromPath != "" && toPath != "" && fromPath != toPath {
		fromPaths := davStatePaths(fromPath)
		toPaths := davStatePaths(toPath)
		for _, destinationPath := range toPaths {
			if s.ACLEntries != nil {
				if err := s.ACLEntries.Delete(ctx, destinationPath); err != nil {
					return err
				}
			}
			if s.Locks != nil {
				if err := s.Locks.DeleteByResourcePath(ctx, destinationPath); err != nil {
					return err
				}
			}
			if err := s.clearDeadPropertiesFallback(ctx, destinationPath); err != nil {
				return err
			}
		}
		for i, sourcePath := range fromPaths {
			destinationPath := toPath
			if i < len(toPaths) {
				destinationPath = toPaths[i]
			}
			if s.ACLEntries != nil {
				if err := s.ACLEntries.MoveResourcePath(ctx, sourcePath, destinationPath); err != nil {
					return err
				}
			}
			if s.Locks != nil {
				if err := s.Locks.MoveResourcePath(ctx, sourcePath, destinationPath); err != nil {
					return err
				}
			}
			if err := s.moveDeadPropertiesFallback(ctx, sourcePath, destinationPath); err != nil {
				return err
			}
		}
	}
	if replacedUID != "" && s.DeletedResources != nil {
		return s.DeletedResources.DeleteByIdentity(ctx, resourceType, collectionID, replacedUID, resourceName)
	}
	return nil
}

func (s *Store) clearDeadPropertiesFallback(ctx context.Context, resourcePath string) error {
	if s == nil || s.DeadProperties == nil || resourcePath == "" {
		return nil
	}
	properties, err := s.DeadProperties.ListByResources(ctx, []string{resourcePath})
	if err != nil {
		return err
	}
	mutations := make([]DeadPropertyMutation, 0, len(properties))
	for _, property := range properties {
		mutations = append(mutations, DeadPropertyMutation{
			NamespaceURI: property.NamespaceURI,
			LocalName:    property.LocalName,
			Remove:       true,
		})
	}
	return s.DeadProperties.Apply(ctx, resourcePath, mutations)
}

func (s *Store) moveDeadPropertiesFallback(ctx context.Context, fromPath, toPath string) error {
	if s == nil || s.DeadProperties == nil || fromPath == "" || toPath == "" || fromPath == toPath {
		return nil
	}
	properties, err := s.DeadProperties.ListByResources(ctx, []string{fromPath, toPath})
	if err != nil {
		return err
	}
	var clearDestination, clearSource, setDestination []DeadPropertyMutation
	for _, property := range properties {
		remove := DeadPropertyMutation{NamespaceURI: property.NamespaceURI, LocalName: property.LocalName, Remove: true}
		switch property.ResourcePath {
		case toPath:
			clearDestination = append(clearDestination, remove)
		case fromPath:
			clearSource = append(clearSource, remove)
			setDestination = append(setDestination, DeadPropertyMutation{
				NamespaceURI: property.NamespaceURI,
				LocalName:    property.LocalName,
				InnerXML:     property.InnerXML,
			})
		}
	}
	if err := s.DeadProperties.Apply(ctx, toPath, clearDestination); err != nil {
		return err
	}
	if err := s.DeadProperties.Apply(ctx, toPath, setDestination); err != nil {
		return err
	}
	return s.DeadProperties.Apply(ctx, fromPath, clearSource)
}

// CopyEventAndState copies an event and its dead properties atomically while
// replacing destination DAV security state rather than inheriting it from the
// source.
func (s *Store) CopyEventAndState(ctx context.Context, fromCalendarID, toCalendarID int64, uid, destResourceName, newETag, fromStatePath, toStatePath, replacedUID string) (*Event, error) {
	if s == nil || s.Events == nil {
		return nil, ErrNotFound
	}
	if s.pool == nil {
		event, err := s.Events.CopyToCalendar(ctx, fromCalendarID, toCalendarID, uid, destResourceName, newETag)
		if err != nil {
			return nil, err
		}
		if err := s.copyDAVStateFallback(ctx, fromStatePath, toStatePath, "event", toCalendarID, replacedUID, destResourceName); err != nil {
			return nil, err
		}
		return event, nil
	}

	tx, err := s.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()
	event, err := copyEventTx(ctx, tx, fromCalendarID, toCalendarID, uid, destResourceName, newETag)
	if err != nil {
		return nil, err
	}
	if err := copyDAVStateTx(ctx, tx, fromStatePath, toStatePath); err != nil {
		return nil, err
	}
	if err := clearDestinationTombstonesTx(ctx, tx, "event", toCalendarID, destResourceName); err != nil {
		return nil, err
	}
	if err := tx.Commit(); err != nil {
		return nil, err
	}
	return event, nil
}

func (s *Store) CopyContactAndState(ctx context.Context, fromAddressBookID, toAddressBookID int64, uid, destResourceName, newETag, fromStatePath, toStatePath, replacedUID string) (*Contact, error) {
	if s == nil || s.Contacts == nil {
		return nil, ErrNotFound
	}
	if s.pool == nil {
		contact, err := s.Contacts.CopyToAddressBook(ctx, fromAddressBookID, toAddressBookID, uid, destResourceName, newETag)
		if err != nil {
			return nil, err
		}
		if err := s.copyDAVStateFallback(ctx, fromStatePath, toStatePath, "contact", toAddressBookID, replacedUID, destResourceName); err != nil {
			return nil, err
		}
		return contact, nil
	}

	tx, err := s.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()
	contact, err := copyContactTx(ctx, tx, fromAddressBookID, toAddressBookID, uid, destResourceName, newETag)
	if err != nil {
		return nil, err
	}
	if err := copyDAVStateTx(ctx, tx, fromStatePath, toStatePath); err != nil {
		return nil, err
	}
	if err := clearDestinationTombstonesTx(ctx, tx, "contact", toAddressBookID, destResourceName); err != nil {
		return nil, err
	}
	if err := tx.Commit(); err != nil {
		return nil, err
	}
	return contact, nil
}

func copyDAVStateTx(ctx context.Context, tx execContext, fromPath, toPath string) error {
	if toPath == "" {
		return nil
	}
	for _, statePath := range davStatePaths(toPath) {
		if _, err := tx.ExecContext(ctx, `DELETE FROM locks WHERE resource_path=$1`, statePath); err != nil {
			return err
		}
		if _, err := tx.ExecContext(ctx, `DELETE FROM acl_entries WHERE resource_path=$1`, statePath); err != nil {
			return err
		}
		if _, err := tx.ExecContext(ctx, `DELETE FROM dav_dead_properties WHERE resource_path=$1`, statePath); err != nil {
			return err
		}
	}
	if fromPath == "" || fromPath == toPath {
		return nil
	}
	_, err := tx.ExecContext(ctx, `
INSERT INTO dav_dead_properties (resource_path, namespace_uri, local_name, inner_xml, created_at, updated_at)
SELECT $1, namespace_uri, local_name, inner_xml, NOW(), NOW()
FROM dav_dead_properties
WHERE resource_path=$2`, toPath, fromPath)
	return err
}

func (s *Store) copyDAVStateFallback(ctx context.Context, fromPath, toPath, resourceType string, collectionID int64, replacedUID, resourceName string) error {
	if toPath != "" {
		for _, statePath := range davStatePaths(toPath) {
			if s.Locks != nil {
				if err := s.Locks.DeleteByResourcePath(ctx, statePath); err != nil {
					return err
				}
			}
			if s.ACLEntries != nil {
				if err := s.ACLEntries.Delete(ctx, statePath); err != nil {
					return err
				}
			}
			if err := s.clearDeadPropertiesFallback(ctx, statePath); err != nil {
				return err
			}
		}
		if err := s.copyDeadPropertiesFallback(ctx, fromPath, toPath); err != nil {
			return err
		}
	}
	if replacedUID != "" && s.DeletedResources != nil {
		return s.DeletedResources.DeleteByIdentity(ctx, resourceType, collectionID, replacedUID, resourceName)
	}
	return nil
}

func (s *Store) copyDeadPropertiesFallback(ctx context.Context, fromPath, toPath string) error {
	if s == nil || s.DeadProperties == nil || toPath == "" || fromPath == toPath {
		return nil
	}
	properties, err := s.DeadProperties.ListByResources(ctx, []string{fromPath, toPath})
	if err != nil {
		return err
	}
	var clearDestination, setDestination []DeadPropertyMutation
	for _, property := range properties {
		if property.ResourcePath == toPath {
			clearDestination = append(clearDestination, DeadPropertyMutation{NamespaceURI: property.NamespaceURI, LocalName: property.LocalName, Remove: true})
		}
		if property.ResourcePath == fromPath {
			setDestination = append(setDestination, DeadPropertyMutation{NamespaceURI: property.NamespaceURI, LocalName: property.LocalName, InnerXML: property.InnerXML})
		}
	}
	if err := s.DeadProperties.Apply(ctx, toPath, clearDestination); err != nil {
		return err
	}
	return s.DeadProperties.Apply(ctx, toPath, setDestination)
}
