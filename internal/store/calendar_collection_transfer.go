package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"slices"
	"strings"

	"github.com/lib/pq"
)

// CalendarCollectionTransferOperation names the whole-collection mutations that
// have to commit atomically across the calendar row, its members, and their DAV
// state.
type CalendarCollectionTransferOperation string

const (
	// CalendarCollectionCopy duplicates the collection, and at Depth infinity
	// its members, leaving the source intact.
	CalendarCollectionCopy CalendarCollectionTransferOperation = "copy"
	// CalendarCollectionMove rebinds the collection to a new location.
	CalendarCollectionMove CalendarCollectionTransferOperation = "move"
	// CalendarCollectionDelete removes the collection and everything bound
	// beneath it.
	CalendarCollectionDelete CalendarCollectionTransferOperation = "delete"
)

// CalendarCollectionMember is one calendar object resource the HTTP layer has
// already read, authorized, and validated against the destination's storage
// preconditions. ETag is what the source carried when it was validated and
// NewETag is what the copy will carry; the store rejects the transfer if the
// stored member no longer matches.
type CalendarCollectionMember struct {
	ID           int64
	UID          string
	ResourceName string
	RawICAL      string
	ETag         string
	NewETag      string
	Metadata     *EventWriteMetadata
}

// CalendarCollectionState is the destination collection as the HTTP layer found
// it, carried so the store can refuse a transfer whose overwrite decision was
// made against a collection that has since changed.
type CalendarCollectionState struct {
	Exists bool
	ID     int64
	CTag   int64
}

// CalendarCollectionTransfer is one COPY, MOVE or DELETE of a whole calendar
// collection. Members are preflighted by the caller so that a member which
// cannot be stored at the destination fails the request before anything is
// written, which is what makes the operation all-or-nothing.
type CalendarCollectionTransfer struct {
	Operation CalendarCollectionTransferOperation
	Depth     string
	Overwrite bool

	SourceID           int64
	ExpectedSourceCTag int64
	SourceStatePath    string

	DestinationOwnerID   int64
	DestinationSlug      string
	ExpectedDestination  CalendarCollectionState
	DestinationStatePath string
	DestinationLockPath  string

	Members           []CalendarCollectionMember
	LockPreconditions []LockPrecondition
}

// CalendarCollectionTransferResult reports the resulting collection, if the
// operation produced one, and whether it was newly created rather than an
// overwrite -- the difference between a 201 and a 204.
type CalendarCollectionTransferResult struct {
	Calendar *Calendar
	Created  bool
}

// CalendarCollectionTransferBackend is the seam a non-PostgreSQL store
// implements to answer TransferCalendarCollection itself.
type CalendarCollectionTransferBackend interface {
	TransferCalendarCollection(context.Context, CalendarCollectionTransfer) (*CalendarCollectionTransferResult, error)
}

// TransferCalendarCollection performs one whole-collection COPY, MOVE or DELETE
// in a single transaction. The source is verified against the ctag and member
// set the caller preflighted, so a collection that changed underneath the
// request fails with ErrResourceStateChanged instead of transferring a mixture
// of old and new state.
func (s *Store) TransferCalendarCollection(ctx context.Context, transfer CalendarCollectionTransfer) (*CalendarCollectionTransferResult, error) {
	if s == nil || s.Calendars == nil {
		return nil, ErrNotFound
	}
	if transfer.Operation != CalendarCollectionCopy && transfer.Operation != CalendarCollectionMove && transfer.Operation != CalendarCollectionDelete {
		return nil, fmt.Errorf("unsupported calendar collection transfer operation %q", transfer.Operation)
	}
	if s.pool == nil {
		if s.CalendarCollections == nil {
			return nil, ErrAtomicStateUnsupported
		}
		return s.CalendarCollections.TransferCalendarCollection(ctx, transfer)
	}

	tx, err := s.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()
	if err := validateLockPreconditionsTx(ctx, tx, transfer.LockPreconditions); err != nil {
		return nil, err
	}
	for _, key := range calendarCollectionTransferLockKeys(transfer) {
		if _, err := tx.ExecContext(ctx, `SELECT pg_advisory_xact_lock(hashtext($1))`, key); err != nil {
			return nil, err
		}
	}
	result, err := transferCalendarCollectionTx(ctx, tx, transfer)
	if err != nil {
		return result, err
	}
	if err := tx.Commit(); err != nil {
		return nil, err
	}
	return result, nil
}

func transferCalendarCollectionTx(ctx context.Context, tx *sql.Tx, transfer CalendarCollectionTransfer) (*CalendarCollectionTransferResult, error) {
	source, err := selectCalendarForUpdate(ctx, tx, transfer.SourceID)
	if err != nil {
		return nil, err
	}
	if source == nil {
		return nil, ErrNotFound
	}
	if source.CTag != transfer.ExpectedSourceCTag {
		return nil, ErrResourceStateChanged
	}
	validateExactMemberSet := transfer.Operation != CalendarCollectionCopy || transfer.Depth != "0"
	if err := validateCalendarCollectionMembersTx(ctx, tx, source.ID, transfer.Members, validateExactMemberSet); err != nil {
		return nil, err
	}

	if transfer.Operation == CalendarCollectionDelete {
		if err := deleteCalendarTreeStateTx(ctx, tx, transfer.SourceStatePath, true); err != nil {
			return nil, err
		}
		if _, err := tx.ExecContext(ctx, `DELETE FROM calendars WHERE id=$1`, source.ID); err != nil {
			return nil, err
		}
		if err := deleteCalendarTombstonesTx(ctx, tx, source.ID); err != nil {
			return nil, err
		}
		return &CalendarCollectionTransferResult{}, nil
	}

	var destination *Calendar
	if transfer.ExpectedDestination.Exists {
		destination, err = selectCalendarForUpdate(ctx, tx, transfer.ExpectedDestination.ID)
		if err == nil && destination != nil && (destination.UserID != transfer.DestinationOwnerID || !calendarMatchesSlug(destination, transfer.DestinationSlug)) {
			return nil, ErrResourceStateChanged
		}
	} else {
		destination, err = selectCalendarBySlugForUpdate(ctx, tx, transfer.DestinationOwnerID, transfer.DestinationSlug)
	}
	if err != nil {
		return nil, err
	}
	if !calendarCollectionStateMatches(transfer.ExpectedDestination, destination) {
		return nil, ErrResourceStateChanged
	}
	if destination != nil && destination.ID == source.ID {
		return nil, ErrPreconditionFailed
	}
	if destination != nil && !transfer.Overwrite {
		return nil, ErrPreconditionFailed
	}
	created := destination == nil
	if destination != nil {
		if err := deleteCalendarTreeStateTx(ctx, tx, transfer.DestinationStatePath, false); err != nil {
			return nil, err
		}
		if _, err := tx.ExecContext(ctx, `DELETE FROM calendars WHERE id=$1`, destination.ID); err != nil {
			return nil, err
		}
		if err := deleteCalendarTombstonesTx(ctx, tx, destination.ID); err != nil {
			return nil, err
		}
	}

	slug := strings.ToLower(transfer.DestinationSlug)
	switch transfer.Operation {
	case CalendarCollectionMove:
		// A calendar's numeric ID is itself an advertised binding. Reusing the
		// source row would leave that binding alive after MOVE, so create a new
		// destination identity, rebind the members/state, then remove the source.
		// The display name travels with the collection: only the binding moves,
		// and the sole uniqueness constraint is on the slug the new row carries.
		movedTemplate := *source
		movedTemplate.ID = 0
		movedTemplate.UserID = transfer.DestinationOwnerID
		movedTemplate.Slug = &slug
		moved, err := createCalendarTx(ctx, tx, movedTemplate)
		if err != nil {
			return nil, err
		}
		if _, err := tx.ExecContext(ctx, `UPDATE events SET calendar_id=$1, last_modified=NOW() WHERE calendar_id=$2`, moved.ID, source.ID); err != nil {
			return nil, err
		}
		if err := moveCalendarTreeStateTx(ctx, tx, transfer.SourceStatePath, calendarCollectionStatePath(moved.ID)); err != nil {
			return nil, err
		}
		if _, err := tx.ExecContext(ctx, `UPDATE deleted_resources SET collection_id=$1 WHERE resource_type='event' AND collection_id=$2`, moved.ID, source.ID); err != nil {
			return nil, err
		}
		if err := deleteLockTreeTx(ctx, tx, transfer.SourceStatePath); err != nil {
			return nil, err
		}
		if _, err := tx.ExecContext(ctx, `DELETE FROM calendars WHERE id=$1`, source.ID); err != nil {
			return nil, err
		}
		if err := moveLockTreeTx(ctx, tx, transfer.DestinationLockPath, calendarCollectionStatePath(moved.ID)); err != nil {
			return nil, err
		}
		moved, err = selectCalendarForUpdate(ctx, tx, moved.ID)
		if err != nil {
			return nil, err
		}
		return &CalendarCollectionTransferResult{Calendar: moved, Created: created}, nil
	case CalendarCollectionCopy:
		copy := *source
		copy.ID = 0
		copy.UserID = transfer.DestinationOwnerID
		copy.Slug = &slug
		copied, err := createCalendarTx(ctx, tx, copy)
		if err != nil {
			return nil, err
		}
		if err := copyCalendarDeadPropertiesTx(ctx, tx, transfer.SourceStatePath, calendarCollectionStatePath(copied.ID)); err != nil {
			return nil, err
		}
		if err := moveLockTreeTx(ctx, tx, transfer.DestinationLockPath, calendarCollectionStatePath(copied.ID)); err != nil {
			return nil, err
		}
		if transfer.Depth == "infinity" {
			for _, member := range transfer.Members {
				metadata := calendarCollectionMemberMetadata(member)
				const insert = `
INSERT INTO events (calendar_id, uid, resource_name, raw_ical, etag, summary, description, location, dtstart, dtend, all_day, recurrence_start, recurrence_until, last_modified)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, NOW())`
				if _, err := tx.ExecContext(ctx, insert, copied.ID, member.UID, member.ResourceName, member.RawICAL, member.NewETag,
					metadata.Summary, metadata.Description, metadata.Location, metadata.DTStart, metadata.DTEnd,
					metadata.AllDay, metadata.RecurrenceStart, metadata.RecurrenceUntil); err != nil {
					return nil, err
				}
				if err := copyCalendarDeadPropertiesTx(ctx, tx,
					calendarCollectionMemberStatePath(transfer.SourceStatePath, member.ResourceName),
					calendarCollectionMemberStatePath(calendarCollectionStatePath(copied.ID), member.ResourceName)); err != nil {
					return nil, err
				}
			}
		}
		return &CalendarCollectionTransferResult{Calendar: copied, Created: created}, nil
	}
	return nil, ErrNotFound
}

func selectCalendarForUpdate(ctx context.Context, tx *sql.Tx, id int64) (*Calendar, error) {
	const query = `SELECT id, user_id, name, slug, description, description_lang, timezone, color, supported_components, ctag, created_at, updated_at FROM calendars WHERE id=$1 FOR UPDATE`
	calendar, err := scanCalendar(tx.QueryRowContext(ctx, query, id).Scan)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return &calendar, nil
}

func selectCalendarBySlugForUpdate(ctx context.Context, tx *sql.Tx, userID int64, slug string) (*Calendar, error) {
	const query = `SELECT id, user_id, name, slug, description, description_lang, timezone, color, supported_components, ctag, created_at, updated_at FROM calendars WHERE user_id=$1 AND LOWER(COALESCE(slug, name))=LOWER($2) ORDER BY id FOR UPDATE`
	rows, err := tx.QueryContext(ctx, query, userID, slug)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var match *Calendar
	for rows.Next() {
		calendar, err := scanCalendar(rows.Scan)
		if err != nil {
			return nil, err
		}
		if match != nil {
			return nil, ErrConflict
		}
		match = &calendar
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return match, nil
}

func calendarMatchesSlug(calendar *Calendar, slug string) bool {
	if calendar == nil {
		return false
	}
	if calendar.Slug != nil {
		return strings.EqualFold(*calendar.Slug, slug)
	}
	return strings.EqualFold(calendar.Name, slug)
}

func scanCalendar(scan rowScanner) (Calendar, error) {
	var calendar Calendar
	var slug, description, descriptionLang, timezone, color sql.NullString
	var components pq.StringArray
	if err := scan(&calendar.ID, &calendar.UserID, &calendar.Name, &slug, &description, &descriptionLang, &timezone, &color, &components, &calendar.CTag, &calendar.CreatedAt, &calendar.UpdatedAt); err != nil {
		return Calendar{}, err
	}
	calendar.Slug = nullableString(slug)
	calendar.Description = nullableString(description)
	calendar.DescriptionLang = nullableString(descriptionLang)
	calendar.Timezone = nullableString(timezone)
	calendar.Color = nullableString(color)
	calendar.SupportedComponents = components
	return calendar, nil
}

func validateCalendarCollectionMembersTx(ctx context.Context, tx *sql.Tx, calendarID int64, members []CalendarCollectionMember, exact bool) error {
	query := `SELECT ` + calendarObjectColumns + ` FROM events WHERE calendar_id=$1 ORDER BY id FOR UPDATE`
	rows, err := tx.QueryContext(ctx, query, calendarID)
	if err != nil {
		return err
	}
	defer rows.Close()
	storedByName := make(map[string]Event)
	for rows.Next() {
		event, err := scanEvent(rows.Scan)
		if err != nil {
			return err
		}
		storedByName[storedResourceName(event)] = event
	}
	if err := rows.Err(); err != nil {
		return err
	}
	if exact && len(storedByName) != len(members) {
		return ErrResourceStateChanged
	}
	seen := make(map[string]struct{}, len(members))
	for _, member := range members {
		if _, duplicate := seen[member.ResourceName]; duplicate {
			return ErrResourceStateChanged
		}
		seen[member.ResourceName] = struct{}{}
		stored, exists := storedByName[member.ResourceName]
		if !exists || stored.ID != member.ID || stored.UID != member.UID || stored.ETag != member.ETag || stored.RawICAL != member.RawICAL {
			return ErrResourceStateChanged
		}
	}
	return nil
}

func calendarCollectionStateMatches(expected CalendarCollectionState, calendar *Calendar) bool {
	if expected.Exists != (calendar != nil) {
		return false
	}
	return calendar == nil || expected.ID == calendar.ID && expected.CTag == calendar.CTag
}

func calendarCollectionMemberMetadata(member CalendarCollectionMember) EventWriteMetadata {
	if member.Metadata != nil {
		return *member.Metadata
	}
	var metadata EventWriteMetadata
	metadata.Summary, metadata.Description, metadata.Location, metadata.DTStart, metadata.DTEnd, metadata.AllDay = parseICalFields(member.RawICAL)
	metadata.RecurrenceStart, metadata.RecurrenceUntil = recurrenceBoundsFromICal(member.RawICAL)
	return metadata
}

func calendarCollectionTransferLockKeys(transfer CalendarCollectionTransfer) []string {
	keys := []string{fmt.Sprintf("calendar-collection:id:%d", transfer.SourceID)}
	if transfer.Operation != CalendarCollectionDelete {
		keys = append(keys, fmt.Sprintf("calendar-collection:user:%d:slug:%s", transfer.DestinationOwnerID, strings.ToLower(transfer.DestinationSlug)))
		if transfer.ExpectedDestination.Exists {
			keys = append(keys, fmt.Sprintf("calendar-collection:id:%d", transfer.ExpectedDestination.ID))
		}
	}
	slices.Sort(keys)
	return slices.Compact(keys)
}

func deleteCalendarTreeStateTx(ctx context.Context, tx execContext, root string, deleteLocks bool) error {
	if root == "" {
		return nil
	}
	tables := []string{"acl_entries", "dav_dead_properties"}
	if deleteLocks {
		tables = append(tables, "locks")
	}
	for _, table := range tables {
		query := `DELETE FROM ` + table + ` WHERE resource_path=$1 OR resource_path LIKE $2`
		if _, err := tx.ExecContext(ctx, query, root, root+"/%"); err != nil {
			return err
		}
	}
	return nil
}

func deleteLockTreeTx(ctx context.Context, tx execContext, root string) error {
	if root == "" {
		return nil
	}
	_, err := tx.ExecContext(ctx, `DELETE FROM locks WHERE resource_path=$1 OR resource_path LIKE $2`, root, root+"/%")
	return err
}

func moveCalendarTreeStateTx(ctx context.Context, tx execContext, fromRoot, toRoot string) error {
	if fromRoot == "" || toRoot == "" || fromRoot == toRoot {
		return nil
	}
	for _, table := range []string{"acl_entries", "dav_dead_properties"} {
		query := `UPDATE ` + table + `
SET resource_path = CASE WHEN resource_path=$1 THEN $2 ELSE $2 || SUBSTRING(resource_path FROM LENGTH($1) + 1) END
WHERE resource_path=$1 OR resource_path LIKE $1 || '/%'`
		if _, err := tx.ExecContext(ctx, query, fromRoot, toRoot); err != nil {
			return err
		}
	}
	return nil
}

func moveLockTreeTx(ctx context.Context, tx execContext, fromRoot, toRoot string) error {
	if fromRoot == "" || toRoot == "" || fromRoot == toRoot {
		return nil
	}
	_, err := tx.ExecContext(ctx, `
UPDATE locks
SET resource_path = CASE
    WHEN resource_path=$1 THEN $2
    ELSE $2 || SUBSTRING(resource_path FROM LENGTH($1) + 1)
END
WHERE (resource_path=$1 OR resource_path LIKE $1 || '/%') AND expires_at > NOW()`, fromRoot, toRoot)
	return err
}

func deleteCalendarTombstonesTx(ctx context.Context, tx execContext, calendarID int64) error {
	_, err := tx.ExecContext(ctx, `DELETE FROM deleted_resources WHERE resource_type='event' AND collection_id=$1`, calendarID)
	return err
}

func copyCalendarDeadPropertiesTx(ctx context.Context, tx execContext, fromPath, toPath string) error {
	if fromPath == "" || toPath == "" || fromPath == toPath {
		return nil
	}
	_, err := tx.ExecContext(ctx, `
INSERT INTO dav_dead_properties (resource_path, namespace_uri, local_name, inner_xml, created_at, updated_at)
SELECT DISTINCT ON (namespace_uri, local_name) $1, namespace_uri, local_name, inner_xml, NOW(), NOW()
FROM dav_dead_properties
WHERE resource_path = ANY($2)
ORDER BY namespace_uri, local_name, CASE WHEN resource_path=$3 THEN 0 ELSE 1 END, updated_at DESC`, toPath, pq.Array(davStatePaths(fromPath)), fromPath)
	return err
}

func calendarCollectionStatePath(id int64) string {
	return fmt.Sprintf("/dav/calendars/%d", id)
}

func calendarCollectionMemberStatePath(collectionPath, resourceName string) string {
	return strings.TrimSuffix(collectionPath, "/") + "/" + resourceName
}
