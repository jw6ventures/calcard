package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"path"
	"slices"
	"strings"
	"time"

	"github.com/lib/pq"
)

// DAVResourceState identifies the exact object state on which the HTTP layer
// based its conditional and privilege checks.
type DAVResourceState struct {
	Exists         bool
	ID             int64
	UID            string
	ResourceName   string
	ETag           string
	RawData        string
	CollectionCTag *int64
}

type ContactTransferExpectation struct {
	Source                     DAVResourceState
	Destination                DAVResourceState
	SourceAddressBookCTag      *int64
	DestinationAddressBookCTag *int64
	Overwrite                  bool
}

type collectionCTagExpectation struct {
	id   int64
	ctag *int64
}

func validateCollectionCTagsTx(ctx context.Context, tx *sql.Tx, table string, expectations ...collectionCTagExpectation) error {
	expectedByID := make(map[int64]int64, len(expectations))
	for _, expectation := range expectations {
		if expectation.id <= 0 || expectation.ctag == nil {
			continue
		}
		if expected, ok := expectedByID[expectation.id]; ok && expected != *expectation.ctag {
			return ErrResourceStateChanged
		}
		expectedByID[expectation.id] = *expectation.ctag
	}
	ids := make([]int64, 0, len(expectedByID))
	for id := range expectedByID {
		ids = append(ids, id)
	}
	slices.Sort(ids)
	query := `SELECT ctag FROM ` + table + ` WHERE id=$1 FOR UPDATE`
	for _, id := range ids {
		var current int64
		if err := tx.QueryRowContext(ctx, query, id).Scan(&current); err != nil {
			if errors.Is(err, sql.ErrNoRows) {
				return ErrResourceStateChanged
			}
			return err
		}
		if current != expectedByID[id] {
			return ErrResourceStateChanged
		}
	}
	return nil
}

func EventDAVResourceState(event *Event) DAVResourceState {
	if event == nil {
		return DAVResourceState{}
	}
	return DAVResourceState{
		Exists: true, ID: event.ID, UID: event.UID, ResourceName: storedResourceName(*event),
		ETag: event.ETag, RawData: event.RawICAL,
	}
}

func ContactDAVResourceState(contact *Contact) DAVResourceState {
	if contact == nil {
		return DAVResourceState{}
	}
	return DAVResourceState{
		Exists: true, ID: contact.ID, UID: contact.UID, ResourceName: storedContactResourceName(*contact),
		ETag: contact.ETag, RawData: contact.RawVCard,
	}
}

// CreateCalendarAndState creates a calendar collection together with the DAV
// state bound to it at creation time: the dead properties the request set, and
// any lock held on the Request-URI, which moves to the new collection's
// canonical path. The transaction serializes and rechecks the supplied lock
// conditions before writing. RFC 4791 §5.3.1 makes MKCALENDAR all-or-none, so
// every state change shares one transaction; a failure anywhere leaves no
// collection, no dead property rows, and the lock still on its original path.
// resourcePath maps the new calendar's ID to that canonical path, which the
// insert is what determines. A store without a connection pool must provide an
// atomic CalendarState backend whenever dead properties or locks are part of
// the operation; best-effort compensation cannot satisfy the all-or-none rule.
func (s *Store) CreateCalendarAndState(ctx context.Context, cal Calendar, dead []DeadPropertyMutation, lockPreconditions []LockPrecondition, lockPath string, resourcePath func(calendarID int64) string) (*Calendar, error) {
	if s == nil || s.Calendars == nil {
		return nil, ErrNotFound
	}
	if s.pool == nil {
		if s.CalendarState != nil {
			return s.CalendarState.CreateCalendarAndState(ctx, cal, dead, lockPreconditions, lockPath, resourcePath)
		}
		if len(dead) != 0 || len(lockPreconditions) != 0 || s.Locks != nil {
			return nil, ErrAtomicStateUnsupported
		}
		return s.Calendars.Create(ctx, cal)
	}

	tx, err := s.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()

	if err := validateLockPreconditionsTx(ctx, tx, lockPreconditions); err != nil {
		return nil, err
	}
	created, err := createCalendarTx(ctx, tx, cal)
	if err != nil {
		return nil, err
	}
	statePath := resourcePath(created.ID)
	if len(dead) != 0 {
		if err := applyDeadPropertyMutationsTx(ctx, tx, statePath, dead); err != nil {
			return nil, err
		}
	}
	if err := moveLocksTx(ctx, tx, lockPath, statePath); err != nil {
		return nil, err
	}
	if err := tx.Commit(); err != nil {
		return nil, err
	}
	return created, nil
}

func validateLockPreconditionsTx(ctx context.Context, tx *sql.Tx, preconditions []LockPrecondition) error {
	if len(preconditions) == 0 {
		return nil
	}
	var serializationTargets []string
	for _, precondition := range preconditions {
		serializationTargets = append(serializationTargets, precondition.ResourcePath)
	}
	for _, resourcePath := range sortedLockSerializationPaths(serializationTargets...) {
		if _, err := tx.ExecContext(ctx, `SELECT pg_advisory_xact_lock(hashtext($1))`, resourcePath); err != nil {
			return err
		}
	}

	lookupSet := make(map[string]struct{})
	var lookupPaths []string
	for _, precondition := range preconditions {
		for _, resourcePath := range precondition.LookupPaths {
			resourcePath = path.Clean(resourcePath)
			if resourcePath == "." {
				continue
			}
			if _, seen := lookupSet[resourcePath]; seen {
				continue
			}
			lookupSet[resourcePath] = struct{}{}
			lookupPaths = append(lookupPaths, resourcePath)
		}
	}
	if len(lookupPaths) == 0 {
		return nil
	}

	const query = `SELECT token, resource_path, depth, expires_at FROM locks WHERE resource_path = ANY($1) AND expires_at > NOW() ORDER BY created_at`
	rows, err := tx.QueryContext(ctx, query, pq.Array(lookupPaths))
	if err != nil {
		return err
	}
	defer rows.Close()

	var locks []Lock
	for rows.Next() {
		var lock Lock
		if err := rows.Scan(&lock.Token, &lock.ResourcePath, &lock.Depth, &lock.ExpiresAt); err != nil {
			return err
		}
		locks = append(locks, lock)
	}
	if err := rows.Err(); err != nil {
		return err
	}
	if !LockPreconditionsSatisfied(preconditions, locks) {
		return ErrLockConflict
	}
	return nil
}

func validateLockPreconditionsFallback(ctx context.Context, locks LockRepository, preconditions []LockPrecondition) error {
	if len(preconditions) == 0 || locks == nil {
		return nil
	}
	seen := make(map[string]struct{})
	var paths []string
	for _, precondition := range preconditions {
		for _, resourcePath := range precondition.LookupPaths {
			resourcePath = path.Clean(resourcePath)
			if resourcePath == "." {
				continue
			}
			if _, ok := seen[resourcePath]; ok {
				continue
			}
			seen[resourcePath] = struct{}{}
			paths = append(paths, resourcePath)
		}
	}
	active, err := locks.ListByResources(ctx, paths)
	if err != nil {
		return err
	}
	if !LockPreconditionsSatisfied(preconditions, active) {
		return ErrLockConflict
	}
	return nil
}

type ACLResourceExpectation struct {
	CollectionKind string
	CollectionID   int64
	CollectionCTag *int64
	ResourceState  *DAVResourceState
}

func (s *Store) SetACLAndState(ctx context.Context, resourcePath string, entries []ACLEntry, expected ACLResourceExpectation, lockPreconditions []LockPrecondition) error {
	if s == nil || s.ACLEntries == nil {
		return ErrNotFound
	}
	if s.pool == nil {
		if err := validateLockPreconditionsFallback(ctx, s.Locks, lockPreconditions); err != nil {
			return err
		}
		if expected.ResourceState != nil {
			switch expected.CollectionKind {
			case "calendar":
				if s.Events == nil {
					return ErrNotFound
				}
				current, err := s.Events.GetByResourceName(ctx, expected.CollectionID, expected.ResourceState.ResourceName)
				if err != nil {
					return err
				}
				if !eventDAVStateMatches(*expected.ResourceState, current) {
					return ErrResourceStateChanged
				}
			case "addressbook":
				if s.Contacts == nil {
					return ErrNotFound
				}
				current, err := s.Contacts.GetByResourceName(ctx, expected.CollectionID, expected.ResourceState.ResourceName)
				if err != nil {
					return err
				}
				if !contactDAVStateMatches(*expected.ResourceState, current) {
					return ErrResourceStateChanged
				}
			}
		}
		return s.ACLEntries.SetACL(ctx, resourcePath, entries)
	}
	tx, err := s.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if err := validateLockPreconditionsTx(ctx, tx, lockPreconditions); err != nil {
		return err
	}
	switch expected.CollectionKind {
	case "calendar":
		if err := validateCollectionCTagsTx(ctx, tx, "calendars",
			collectionCTagExpectation{id: expected.CollectionID, ctag: expected.CollectionCTag}); err != nil {
			return err
		}
		if expected.ResourceState != nil {
			current, err := selectEventTx(ctx, tx, `resource_name`, expected.CollectionID, expected.ResourceState.ResourceName)
			if err != nil {
				return err
			}
			if !eventDAVStateMatches(*expected.ResourceState, current) {
				return ErrResourceStateChanged
			}
		}
	case "addressbook":
		if err := validateCollectionCTagsTx(ctx, tx, "address_books",
			collectionCTagExpectation{id: expected.CollectionID, ctag: expected.CollectionCTag}); err != nil {
			return err
		}
		if expected.ResourceState != nil {
			current, err := selectContactTx(ctx, tx, `resource_name`, expected.CollectionID, expected.ResourceState.ResourceName)
			if err != nil {
				return err
			}
			if !contactDAVStateMatches(*expected.ResourceState, current) {
				return ErrResourceStateChanged
			}
		}
	}
	if err := setACLTx(ctx, tx, resourcePath, entries); err != nil {
		return err
	}
	return tx.Commit()
}

// LockPreconditionsSatisfied applies the write-lock conditions captured by the
// DAV layer. Atomic non-PostgreSQL CalendarStateCreator implementations use the
// same check immediately before they create the collection.
func LockPreconditionsSatisfied(preconditions []LockPrecondition, locks []Lock) bool {
	now := time.Now()
	for _, precondition := range preconditions {
		lookupPaths := make(map[string]struct{}, len(precondition.LookupPaths))
		for _, resourcePath := range precondition.LookupPaths {
			lookupPaths[path.Clean(resourcePath)] = struct{}{}
		}
		tokens := make(map[string]struct{}, len(precondition.Tokens))
		for _, token := range precondition.Tokens {
			tokens[token] = struct{}{}
		}

		active := false
		satisfied := false
		resourcePath := path.Clean(precondition.ResourcePath)
		for _, lock := range locks {
			if !lock.ExpiresAt.IsZero() && lock.ExpiresAt.Before(now) {
				continue
			}
			lockPath := path.Clean(lock.ResourcePath)
			if _, applies := lookupPaths[lockPath]; !applies {
				continue
			}
			if lockPath != resourcePath && lock.Depth != "infinity" {
				continue
			}
			active = true
			if _, ok := tokens[lock.Token]; ok {
				satisfied = true
				break
			}
		}
		if active && !satisfied {
			return false
		}
	}
	return true
}

func moveLocksTx(ctx context.Context, tx execContext, fromPath, toPath string) error {
	if fromPath == "" || toPath == "" || fromPath == toPath {
		return nil
	}
	if _, err := tx.ExecContext(ctx, `DELETE FROM locks WHERE resource_path=$1 AND expires_at > NOW()`, toPath); err != nil {
		return err
	}
	_, err := tx.ExecContext(ctx, `UPDATE locks SET resource_path=$1 WHERE resource_path=$2 AND expires_at > NOW()`, toPath, fromPath)
	return err
}

func (s *Store) DeleteEventAndState(ctx context.Context, calendarID int64, expected DAVResourceState, resourcePath string, lockPreconditions []LockPrecondition) error {
	if s == nil || s.pool == nil {
		if s == nil || s.Events == nil {
			return ErrNotFound
		}
		if err := validateLockPreconditionsFallback(ctx, s.Locks, lockPreconditions); err != nil {
			return err
		}
		current, err := s.Events.GetByResourceName(ctx, calendarID, expected.ResourceName)
		if err != nil {
			return err
		}
		if !eventDAVStateMatches(expected, current) {
			return ErrResourceStateChanged
		}
		if err := s.Events.DeleteByUID(ctx, calendarID, expected.UID); err != nil {
			return err
		}
		return s.deleteDAVStateFallback(ctx, resourcePath, true)
	}

	tx, err := s.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if err := validateLockPreconditionsTx(ctx, tx, lockPreconditions); err != nil {
		return err
	}
	if err := validateCollectionCTagsTx(ctx, tx, "calendars",
		collectionCTagExpectation{id: calendarID, ctag: expected.CollectionCTag}); err != nil {
		return err
	}
	if err := acquireDAVObjectIdentityLocks(ctx, tx, "calendar-object", calendarID, expected); err != nil {
		return err
	}
	current, err := selectEventTx(ctx, tx, `resource_name`, calendarID, expected.ResourceName)
	if err != nil {
		return err
	}
	if !eventDAVStateMatches(expected, current) {
		return ErrResourceStateChanged
	}
	if _, err := tx.ExecContext(ctx, `DELETE FROM events WHERE id=$1`, current.ID); err != nil {
		return err
	}
	if err := deleteDAVStateTx(ctx, tx, resourcePath, true); err != nil {
		return err
	}
	return tx.Commit()
}

func (s *Store) DeleteContactAndState(ctx context.Context, addressBookID int64, expected DAVResourceState, resourcePath string, lockPreconditions []LockPrecondition) error {
	if s == nil || s.pool == nil {
		if s == nil || s.Contacts == nil {
			return ErrNotFound
		}
		if err := validateLockPreconditionsFallback(ctx, s.Locks, lockPreconditions); err != nil {
			return err
		}
		current, err := s.Contacts.GetByResourceName(ctx, addressBookID, expected.ResourceName)
		if err != nil {
			return err
		}
		if !contactDAVStateMatches(expected, current) {
			return ErrResourceStateChanged
		}
		if err := s.Contacts.DeleteByUID(ctx, addressBookID, expected.UID); err != nil {
			return err
		}
		return s.deleteDAVStateFallback(ctx, resourcePath, true)
	}

	tx, err := s.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if err := validateLockPreconditionsTx(ctx, tx, lockPreconditions); err != nil {
		return err
	}
	if err := validateCollectionCTagsTx(ctx, tx, "address_books",
		collectionCTagExpectation{id: addressBookID, ctag: expected.CollectionCTag}); err != nil {
		return err
	}
	if err := acquireDAVObjectIdentityLocks(ctx, tx, "contact-object", addressBookID, expected); err != nil {
		return err
	}
	current, err := selectContactTx(ctx, tx, `resource_name`, addressBookID, expected.ResourceName)
	if err != nil {
		return err
	}
	if !contactDAVStateMatches(expected, current) {
		return ErrResourceStateChanged
	}
	if _, err := tx.ExecContext(ctx, `DELETE FROM contacts WHERE id=$1`, current.ID); err != nil {
		return err
	}
	if err := deleteDAVStateTx(ctx, tx, resourcePath, true); err != nil {
		return err
	}
	return tx.Commit()
}

func eventDAVStateMatches(expected DAVResourceState, event *Event) bool {
	if expected.Exists != (event != nil) {
		return false
	}
	if event == nil {
		return true
	}
	if expected.ID != 0 && expected.ID != event.ID {
		return false
	}
	return expected.UID == event.UID && expected.ResourceName == storedResourceName(*event) &&
		expected.ETag == event.ETag && expected.RawData == event.RawICAL
}

func contactDAVStateMatches(expected DAVResourceState, contact *Contact) bool {
	if expected.Exists != (contact != nil) {
		return false
	}
	if contact == nil {
		return true
	}
	if expected.ID != 0 && expected.ID != contact.ID {
		return false
	}
	return expected.UID == contact.UID && expected.ResourceName == storedContactResourceName(*contact) &&
		expected.ETag == contact.ETag && expected.RawData == contact.RawVCard
}

func storedContactResourceName(contact Contact) string {
	if contact.ResourceName != "" {
		return contact.ResourceName
	}
	return contact.UID
}

func acquireDAVObjectIdentityLocks(ctx context.Context, tx execContext, kind string, collectionID int64, states ...DAVResourceState) error {
	var keys []string
	for _, state := range states {
		if state.ResourceName != "" {
			keys = append(keys, fmt.Sprintf("%s:%d:name:%s", kind, collectionID, state.ResourceName))
		}
		if state.UID != "" {
			keys = append(keys, fmt.Sprintf("%s:%d:uid:%s", kind, collectionID, state.UID))
		}
	}
	slices.Sort(keys)
	for _, key := range slices.Compact(keys) {
		if _, err := tx.ExecContext(ctx, `SELECT pg_advisory_xact_lock(hashtext($1))`, key); err != nil {
			return err
		}
	}
	return nil
}

const contactDAVColumns = `id, address_book_id, uid, resource_name, raw_vcard, etag, display_name, primary_email, birthday, last_modified`

func selectContactTx(ctx context.Context, tx *sql.Tx, column string, addressBookID int64, value string) (*Contact, error) {
	query := `SELECT ` + contactDAVColumns + ` FROM contacts WHERE address_book_id=$1 AND ` + column + `=$2 FOR UPDATE`
	contact, err := scanContact(tx.QueryRowContext(ctx, query, addressBookID, value).Scan)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, nil
		}
		return nil, err
	}
	return &contact, nil
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
		if !strings.Contains(strings.Trim(strings.TrimPrefix(resourcePath, "/dav/addressbooks/"), "/"), "/") {
			return paths
		}
		base := resourcePath
		if strings.EqualFold(path.Ext(base), ".vcf") {
			base = strings.TrimSuffix(base, path.Ext(base))
			addPath(base)
			addPath(resourcePath)
			return paths
		}
		addPath(base + ".vcf")
	case strings.HasPrefix(resourcePath, "/dav/calendars/"):
		if !strings.Contains(strings.Trim(strings.TrimPrefix(resourcePath, "/dav/calendars/"), "/"), "/") {
			return paths
		}
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

// UpdateAndMoveEventAndState updates an event while moving it to another
// calendar, keeping the event row and DAV resource state in one transaction.
func (s *Store) UpdateAndMoveEventAndState(ctx context.Context, fromCalendarID int64, event Event, fromStatePath, toStatePath string) error {
	if s == nil || s.Events == nil {
		return ErrNotFound
	}
	if event.CalendarID == fromCalendarID {
		_, err := s.Events.Upsert(ctx, event)
		return err
	}
	if event.ResourceName == "" {
		event.ResourceName = event.UID
	}

	var metadata EventWriteMetadata
	if event.WriteMetadata != nil {
		metadata = *event.WriteMetadata
	} else {
		metadata.Summary, metadata.Description, metadata.Location, metadata.DTStart, metadata.DTEnd, metadata.AllDay = parseICalFields(event.RawICAL)
		metadata.RecurrenceStart, metadata.RecurrenceUntil = recurrenceBoundsFromICal(event.RawICAL)
	}

	if s.pool == nil {
		sourceEvent := event
		sourceEvent.CalendarID = fromCalendarID
		if _, err := s.Events.Upsert(ctx, sourceEvent); err != nil {
			return err
		}
		return s.MoveEventAndState(
			ctx,
			fromCalendarID,
			event.CalendarID,
			event.UID,
			event.ResourceName,
			fromStatePath,
			toStatePath,
			"",
		)
	}

	tx, err := s.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	const updateQuery = `UPDATE events SET
calendar_id=$1, resource_name=$2, raw_ical=$3, etag=$4,
summary=$5, description=$6, location=$7, dtstart=$8, dtend=$9,
all_day=$10, recurrence_start=$11, recurrence_until=$12, last_modified=NOW()
WHERE calendar_id=$13 AND uid=$14`
	result, err := tx.ExecContext(
		ctx,
		updateQuery,
		event.CalendarID,
		event.ResourceName,
		event.RawICAL,
		event.ETag,
		metadata.Summary,
		metadata.Description,
		metadata.Location,
		metadata.DTStart,
		metadata.DTEnd,
		metadata.AllDay,
		metadata.RecurrenceStart,
		metadata.RecurrenceUntil,
		fromCalendarID,
		event.UID,
	)
	if err != nil {
		if isEventIdentityConflict(err) {
			return ErrConflict
		}
		return err
	}
	rows, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if rows == 0 {
		return ErrNotFound
	}

	const tombstoneQuery = `INSERT INTO deleted_resources (resource_type, collection_id, uid, resource_name) VALUES ('event', $1, $2, $3)`
	if _, err := tx.ExecContext(ctx, tombstoneQuery, fromCalendarID, event.UID, event.ResourceName); err != nil {
		return err
	}
	const incrementSourceCTagQuery = `UPDATE calendars SET ctag = ctag + 1, updated_at = NOW() WHERE id = $1`
	if _, err := tx.ExecContext(ctx, incrementSourceCTagQuery, fromCalendarID); err != nil {
		return err
	}
	if err := moveDAVStateTx(ctx, tx, fromStatePath, toStatePath); err != nil {
		return err
	}
	if err := clearDestinationTombstonesTx(ctx, tx, "event", event.CalendarID, event.ResourceName); err != nil {
		return err
	}
	return tx.Commit()
}

// MoveContactAndState is the contact counterpart of MoveEventAndState.
func (s *Store) MoveContactAndState(ctx context.Context, fromAddressBookID, toAddressBookID int64, uid, destResourceName, fromStatePath, toStatePath, replacedUID string, expected ContactTransferExpectation, lockPreconditions []LockPrecondition) error {
	if s == nil || s.Contacts == nil {
		return ErrNotFound
	}
	if s.pool == nil {
		if err := validateLockPreconditionsFallback(ctx, s.Locks, lockPreconditions); err != nil {
			return err
		}
		if err := validateContactTransferViaRepository(ctx, s.Contacts, fromAddressBookID, toAddressBookID, destResourceName, expected); err != nil {
			return err
		}
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
	if err := validateLockPreconditionsTx(ctx, tx, lockPreconditions); err != nil {
		return err
	}
	if err := validateCollectionCTagsTx(ctx, tx, "address_books",
		collectionCTagExpectation{id: fromAddressBookID, ctag: expected.SourceAddressBookCTag},
		collectionCTagExpectation{id: toAddressBookID, ctag: expected.DestinationAddressBookCTag}); err != nil {
		return err
	}
	if err := acquireContactTransferIdentityLocks(ctx, tx, fromAddressBookID, toAddressBookID, uid, destResourceName, expected); err != nil {
		return err
	}
	if _, _, err := transferContactTx(ctx, tx, contactTransferMove, fromAddressBookID, toAddressBookID, uid, destResourceName, "", expected); err != nil {
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
		if _, err := tx.ExecContext(ctx, `DELETE FROM locks WHERE resource_path=$1`, sourcePath); err != nil {
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
				if err := s.Locks.DeleteByResourcePath(ctx, sourcePath); err != nil {
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

func (s *Store) CopyContactAndState(ctx context.Context, fromAddressBookID, toAddressBookID int64, uid, destResourceName, newETag, fromStatePath, toStatePath, replacedUID string, expected ContactTransferExpectation, lockPreconditions []LockPrecondition) (*Contact, error) {
	if s == nil || s.Contacts == nil {
		return nil, ErrNotFound
	}
	if s.pool == nil {
		if err := validateLockPreconditionsFallback(ctx, s.Locks, lockPreconditions); err != nil {
			return nil, err
		}
		if err := validateContactTransferViaRepository(ctx, s.Contacts, fromAddressBookID, toAddressBookID, destResourceName, expected); err != nil {
			return nil, err
		}
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
	if err := validateLockPreconditionsTx(ctx, tx, lockPreconditions); err != nil {
		return nil, err
	}
	if err := validateCollectionCTagsTx(ctx, tx, "address_books",
		collectionCTagExpectation{id: fromAddressBookID, ctag: expected.SourceAddressBookCTag},
		collectionCTagExpectation{id: toAddressBookID, ctag: expected.DestinationAddressBookCTag}); err != nil {
		return nil, err
	}
	if err := acquireContactTransferIdentityLocks(ctx, tx, fromAddressBookID, toAddressBookID, uid, destResourceName, expected); err != nil {
		return nil, err
	}
	contact, _, err := transferContactTx(ctx, tx, contactTransferCopy, fromAddressBookID, toAddressBookID, uid, destResourceName, newETag, expected)
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

type contactTransferOperation string

const (
	contactTransferCopy contactTransferOperation = "copy"
	contactTransferMove contactTransferOperation = "move"
)

func validateContactTransferViaRepository(ctx context.Context, contacts ContactRepository, fromAddressBookID, toAddressBookID int64, destResourceName string, expected ContactTransferExpectation) error {
	source, err := contacts.GetByResourceName(ctx, fromAddressBookID, expected.Source.ResourceName)
	if err != nil {
		return err
	}
	if !contactDAVStateMatches(expected.Source, source) {
		return ErrResourceStateChanged
	}
	destination, err := contacts.GetByResourceName(ctx, toAddressBookID, destResourceName)
	if err != nil {
		return err
	}
	if source != nil && destination != nil && source.ID != 0 && source.ID == destination.ID {
		if !expected.Overwrite {
			return ErrPreconditionFailed
		}
		return nil
	}
	if !contactDAVStateMatches(expected.Destination, destination) {
		return ErrResourceStateChanged
	}
	if destination != nil && !expected.Overwrite {
		return ErrPreconditionFailed
	}
	return nil
}

func acquireContactTransferIdentityLocks(ctx context.Context, tx execContext, fromAddressBookID, toAddressBookID int64, uid, destResourceName string, expected ContactTransferExpectation) error {
	keys := []string{
		fmt.Sprintf("contact-object:%d:name:%s", fromAddressBookID, expected.Source.ResourceName),
		fmt.Sprintf("contact-object:%d:uid:%s", fromAddressBookID, uid),
		fmt.Sprintf("contact-object:%d:name:%s", toAddressBookID, destResourceName),
		fmt.Sprintf("contact-object:%d:uid:%s", toAddressBookID, uid),
	}
	if expected.Destination.UID != "" {
		keys = append(keys, fmt.Sprintf("contact-object:%d:uid:%s", toAddressBookID, expected.Destination.UID))
	}
	slices.Sort(keys)
	for _, key := range slices.Compact(keys) {
		if _, err := tx.ExecContext(ctx, `SELECT pg_advisory_xact_lock(hashtext($1))`, key); err != nil {
			return err
		}
	}
	return nil
}

func transferContactTx(ctx context.Context, tx *sql.Tx, operation contactTransferOperation, fromAddressBookID, toAddressBookID int64, uid, destResourceName, newETag string, expected ContactTransferExpectation) (*Contact, bool, error) {
	source, err := selectContactTx(ctx, tx, `resource_name`, fromAddressBookID, expected.Source.ResourceName)
	if err != nil {
		return nil, false, err
	}
	if !contactDAVStateMatches(expected.Source, source) || source.UID != uid {
		return nil, false, ErrResourceStateChanged
	}

	destination, err := selectContactTx(ctx, tx, `resource_name`, toAddressBookID, destResourceName)
	if err != nil {
		return nil, false, err
	}
	sameResource := destination != nil && destination.ID == source.ID
	if sameResource {
		if !expected.Overwrite {
			return nil, false, ErrPreconditionFailed
		}
		return source, false, nil
	}
	if !contactDAVStateMatches(expected.Destination, destination) {
		return nil, false, ErrResourceStateChanged
	}
	if destination != nil && !expected.Overwrite {
		return nil, false, ErrPreconditionFailed
	}

	byUID, err := selectContactTx(ctx, tx, `uid`, toAddressBookID, source.UID)
	if err != nil {
		return nil, false, err
	}
	if byUID != nil && byUID.ID != source.ID && byUID.ID != contactID(destination) && storedContactResourceName(*byUID) != destResourceName {
		return nil, false, ErrConflict
	}
	if operation == contactTransferCopy && fromAddressBookID == toAddressBookID {
		return nil, false, ErrConflict
	}

	created := destination == nil
	if destination != nil {
		if _, err := tx.ExecContext(ctx, `DELETE FROM contacts WHERE id=$1`, destination.ID); err != nil {
			return nil, false, err
		}
	}

	switch operation {
	case contactTransferCopy:
		const insert = `
INSERT INTO contacts (address_book_id, uid, resource_name, raw_vcard, etag, display_name, primary_email, birthday, last_modified)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, NOW())
RETURNING ` + contactDAVColumns
		copied, err := scanContact(tx.QueryRowContext(ctx, insert,
			toAddressBookID, source.UID, destResourceName, source.RawVCard, newETag,
			source.DisplayName, source.PrimaryEmail, source.Birthday,
		).Scan)
		if err != nil {
			if isContactIdentityConflict(err) {
				return nil, false, ErrResourceStateChanged
			}
			return nil, false, err
		}
		return &copied, created, nil
	case contactTransferMove:
		const update = `UPDATE contacts SET address_book_id=$1, resource_name=$2, last_modified=NOW() WHERE id=$3 RETURNING ` + contactDAVColumns
		moved, err := scanContact(tx.QueryRowContext(ctx, update, toAddressBookID, destResourceName, source.ID).Scan)
		if err != nil {
			if isContactIdentityConflict(err) {
				return nil, false, ErrResourceStateChanged
			}
			return nil, false, err
		}
		if storedContactResourceName(*source) != destResourceName || fromAddressBookID != toAddressBookID {
			if _, err := tx.ExecContext(ctx, `INSERT INTO deleted_resources (resource_type, collection_id, uid, resource_name) VALUES ('contact', $1, $2, $3)`, fromAddressBookID, source.UID, storedContactResourceName(*source)); err != nil {
				return nil, false, err
			}
		}
		if fromAddressBookID != toAddressBookID {
			if _, err := tx.ExecContext(ctx, `UPDATE address_books SET ctag = ctag + 1, updated_at = NOW() WHERE id = $1`, fromAddressBookID); err != nil {
				return nil, false, err
			}
		}
		return &moved, created, nil
	default:
		return nil, false, fmt.Errorf("unsupported contact transfer operation %q", operation)
	}
}

func contactID(contact *Contact) int64 {
	if contact == nil {
		return 0
	}
	return contact.ID
}

func copyDAVStateTx(ctx context.Context, tx execContext, fromPath, toPath string) error {
	if toPath == "" {
		return nil
	}
	for _, statePath := range davStatePaths(toPath) {
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
	fromPaths := davStatePaths(fromPath)
	_, err := tx.ExecContext(ctx, `
INSERT INTO dav_dead_properties (resource_path, namespace_uri, local_name, inner_xml, created_at, updated_at)
SELECT DISTINCT ON (namespace_uri, local_name) $1, namespace_uri, local_name, inner_xml, NOW(), NOW()
FROM dav_dead_properties
WHERE resource_path = ANY($2)
ORDER BY namespace_uri, local_name, CASE WHEN resource_path=$3 THEN 0 ELSE 1 END, updated_at DESC`, toPath, pq.Array(fromPaths), fromPath)
	return err
}

func (s *Store) copyDAVStateFallback(ctx context.Context, fromPath, toPath, resourceType string, collectionID int64, replacedUID, resourceName string) error {
	if toPath != "" {
		for _, statePath := range davStatePaths(toPath) {
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
	fromPaths := davStatePaths(fromPath)
	toPaths := davStatePaths(toPath)
	lookupPaths := append(append([]string(nil), fromPaths...), toPaths...)
	properties, err := s.DeadProperties.ListByResources(ctx, lookupPaths)
	if err != nil {
		return err
	}
	propertiesByPath := make(map[string][]DeadProperty)
	for _, property := range properties {
		propertiesByPath[property.ResourcePath] = append(propertiesByPath[property.ResourcePath], property)
	}
	for _, destinationPath := range toPaths {
		var clearDestination []DeadPropertyMutation
		for _, property := range propertiesByPath[destinationPath] {
			clearDestination = append(clearDestination, DeadPropertyMutation{NamespaceURI: property.NamespaceURI, LocalName: property.LocalName, Remove: true})
		}
		if err := s.DeadProperties.Apply(ctx, destinationPath, clearDestination); err != nil {
			return err
		}
	}
	seen := make(map[string]struct{})
	var setDestination []DeadPropertyMutation
	for _, sourcePath := range fromPaths {
		for _, property := range propertiesByPath[sourcePath] {
			key := property.NamespaceURI + "\x00" + property.LocalName
			if _, exists := seen[key]; exists {
				continue
			}
			seen[key] = struct{}{}
			setDestination = append(setDestination, DeadPropertyMutation{NamespaceURI: property.NamespaceURI, LocalName: property.LocalName, InnerXML: property.InnerXML})
		}
	}
	return s.DeadProperties.Apply(ctx, toPath, setDestination)
}
