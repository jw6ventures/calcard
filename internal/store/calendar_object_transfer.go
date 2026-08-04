package store

import (
	"context"
	"database/sql"
	"fmt"
	"slices"
)

// CalendarObjectTransferOperation names which of the two RFC 4918 rebinding
// methods a transfer carries out. COPY and MOVE share every precondition and
// differ only in whether the source binding survives.
type CalendarObjectTransferOperation string

const (
	// CalendarObjectCopy leaves the source resource bound where it is.
	CalendarObjectCopy CalendarObjectTransferOperation = "copy"
	// CalendarObjectMove unbinds the source once the destination is written.
	CalendarObjectMove CalendarObjectTransferOperation = "move"
)

// CalendarObjectTransferState is the destination as the HTTP layer found it
// when it decided which privileges the request needed and whether Overwrite
// applied. The store re-reads the destination under lock and refuses the
// transfer when it no longer matches, so an authorization made against a create
// cannot silently be applied to an overwrite.
type CalendarObjectTransferState struct {
	Exists bool
	UID    string
	ETag   string
}

// CalendarObjectTransfer is one COPY or MOVE of a calendar object resource,
// stated so the whole of it commits or none of it does. Every Expected field is
// an optimistic precondition the store verifies under lock; a mismatch is
// ErrResourceStateChanged rather than a write against state nobody authorized.
type CalendarObjectTransfer struct {
	Operation CalendarObjectTransferOperation

	SourceCalendarID   int64
	SourceUID          string
	SourceResourceName string
	ExpectedSourceETag string
	ExpectedSourceRaw  string
	ExpectedSourceCTag *int64

	DestinationCalendarID   int64
	DestinationResourceName string
	ExpectedDestination     CalendarObjectTransferState
	ExpectedDestinationCTag *int64
	Overwrite               bool

	RawICAL  string
	ETag     string
	Metadata *EventWriteMetadata

	SourceStatePath      string
	DestinationStatePath string
	LockPreconditions    []LockPrecondition
}

// CalendarObjectTransferResult reports what the transfer did. Created
// distinguishes the 201 and 204 answers RFC 4918 §9.8.5 requires, and Conflict
// carries the resource already holding the UID so a CALDAV:no-uid-conflict
// response can name it.
type CalendarObjectTransferResult struct {
	Event    *Event
	Created  bool
	Conflict *Event
}

// CalendarObjectTransferBackend is the seam a non-PostgreSQL store implements
// to answer TransferCalendarObject itself.
type CalendarObjectTransferBackend interface {
	TransferCalendarObject(context.Context, CalendarObjectTransfer) (*CalendarObjectTransferResult, error)
}

// TransferCalendarObject performs one COPY or MOVE of a calendar object inside
// a single transaction: lock preconditions, collection ctags, the source and
// destination rows, the UID uniqueness rule, the DAV dead-property state and
// the sync tombstones all move together or not at all.
func (s *Store) TransferCalendarObject(ctx context.Context, transfer CalendarObjectTransfer) (*CalendarObjectTransferResult, error) {
	if s == nil || s.Events == nil {
		return nil, ErrNotFound
	}
	if transfer.Operation != CalendarObjectCopy && transfer.Operation != CalendarObjectMove {
		return nil, fmt.Errorf("unsupported calendar object transfer operation %q", transfer.Operation)
	}
	if transfer.SourceResourceName == "" {
		transfer.SourceResourceName = transfer.SourceUID
	}
	if transfer.DestinationResourceName == "" {
		transfer.DestinationResourceName = transfer.SourceUID
	}
	if s.pool == nil {
		if s.CalendarTransfers == nil {
			return nil, ErrAtomicStateUnsupported
		}
		return s.CalendarTransfers.TransferCalendarObject(ctx, transfer)
	}

	tx, err := s.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()

	if err := validateLockPreconditionsTx(ctx, tx, transfer.LockPreconditions); err != nil {
		return nil, err
	}
	if err := validateCollectionCTagsTx(ctx, tx, "calendars",
		collectionCTagExpectation{id: transfer.SourceCalendarID, ctag: transfer.ExpectedSourceCTag},
		collectionCTagExpectation{id: transfer.DestinationCalendarID, ctag: transfer.ExpectedDestinationCTag}); err != nil {
		return nil, err
	}
	for _, key := range calendarObjectTransferLockKeys(transfer) {
		if _, err := tx.ExecContext(ctx, `SELECT pg_advisory_xact_lock(hashtext($1))`, key); err != nil {
			return nil, err
		}
	}

	result, err := transferCalendarObjectTx(ctx, tx, transfer)
	if err != nil {
		return result, err
	}
	if err := tx.Commit(); err != nil {
		return nil, err
	}
	return result, nil
}

func transferCalendarObjectTx(ctx context.Context, tx *sql.Tx, transfer CalendarObjectTransfer) (*CalendarObjectTransferResult, error) {
	source, err := selectEventTx(ctx, tx, `resource_name`, transfer.SourceCalendarID, transfer.SourceResourceName)
	if err != nil {
		return nil, err
	}
	if source == nil {
		return nil, ErrNotFound
	}
	if source.UID != transfer.SourceUID || source.ETag != transfer.ExpectedSourceETag || source.RawICAL != transfer.ExpectedSourceRaw {
		return nil, ErrResourceStateChanged
	}

	destination, err := selectEventTx(ctx, tx, `resource_name`, transfer.DestinationCalendarID, transfer.DestinationResourceName)
	if err != nil {
		return nil, err
	}
	sameResource := source.ID == eventID(destination)
	if !sameResource && !calendarObjectTransferStateMatches(transfer.ExpectedDestination, destination) {
		return nil, ErrResourceStateChanged
	}
	if sameResource {
		if !transfer.Overwrite {
			return nil, ErrPreconditionFailed
		}
		return &CalendarObjectTransferResult{Event: source}, nil
	}
	if destination != nil && !transfer.Overwrite {
		return nil, ErrPreconditionFailed
	}

	byUID, err := selectEventTx(ctx, tx, `uid`, transfer.DestinationCalendarID, source.UID)
	if err != nil {
		return nil, err
	}
	if byUID != nil && byUID.ID != source.ID && storedResourceName(*byUID) != transfer.DestinationResourceName {
		return &CalendarObjectTransferResult{Conflict: byUID}, ErrUIDConflict
	}
	if transfer.Operation == CalendarObjectCopy && transfer.SourceCalendarID == transfer.DestinationCalendarID {
		return &CalendarObjectTransferResult{Conflict: source}, ErrUIDConflict
	}

	created := destination == nil
	if destination != nil {
		if _, err := tx.ExecContext(ctx, `DELETE FROM events WHERE id=$1`, destination.ID); err != nil {
			return nil, err
		}
	}

	var event *Event
	switch transfer.Operation {
	case CalendarObjectCopy:
		metadata := calendarObjectTransferMetadata(transfer, source)
		const insert = `
INSERT INTO events (calendar_id, uid, resource_name, raw_ical, etag, summary, description, location, dtstart, dtend, all_day, recurrence_start, recurrence_until, last_modified)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, NOW())
RETURNING ` + calendarObjectColumns
		copied, err := scanEvent(tx.QueryRowContext(ctx, insert,
			transfer.DestinationCalendarID, source.UID, transfer.DestinationResourceName,
			transfer.RawICAL, transfer.ETag, metadata.Summary, metadata.Description,
			metadata.Location, metadata.DTStart, metadata.DTEnd, metadata.AllDay,
			metadata.RecurrenceStart, metadata.RecurrenceUntil,
		).Scan)
		if err != nil {
			if isEventIdentityConflict(err) {
				return nil, ErrConflict
			}
			return nil, err
		}
		event = &copied
		if err := copyDAVStateTx(ctx, tx, transfer.SourceStatePath, transfer.DestinationStatePath); err != nil {
			return nil, err
		}
	case CalendarObjectMove:
		const update = `UPDATE events SET calendar_id=$1, resource_name=$2, last_modified=NOW() WHERE id=$3 RETURNING ` + calendarObjectColumns
		moved, err := scanEvent(tx.QueryRowContext(ctx, update, transfer.DestinationCalendarID, transfer.DestinationResourceName, source.ID).Scan)
		if err != nil {
			if isEventIdentityConflict(err) {
				return nil, ErrConflict
			}
			return nil, err
		}
		event = &moved
		if _, err := tx.ExecContext(ctx, `
INSERT INTO deleted_resources (resource_type, collection_id, uid, resource_name)
VALUES ('event', $1, $2, $3)`, source.CalendarID, source.UID, storedResourceName(*source)); err != nil {
			return nil, err
		}
		if source.CalendarID != transfer.DestinationCalendarID {
			if _, err := tx.ExecContext(ctx, `UPDATE calendars SET ctag=ctag+1, updated_at=NOW() WHERE id=$1`, source.CalendarID); err != nil {
				return nil, err
			}
		}
		if err := moveDAVStateTx(ctx, tx, transfer.SourceStatePath, transfer.DestinationStatePath); err != nil {
			return nil, err
		}
	}
	if err := clearDestinationTombstonesTx(ctx, tx, "event", transfer.DestinationCalendarID, transfer.DestinationResourceName); err != nil {
		return nil, err
	}
	return &CalendarObjectTransferResult{Event: event, Created: created}, nil
}

func eventID(event *Event) int64 {
	if event == nil {
		return 0
	}
	return event.ID
}

func calendarObjectTransferStateMatches(expected CalendarObjectTransferState, event *Event) bool {
	if expected.Exists != (event != nil) {
		return false
	}
	if event == nil {
		return true
	}
	return expected.UID == event.UID && expected.ETag == event.ETag
}

func calendarObjectTransferMetadata(transfer CalendarObjectTransfer, source *Event) EventWriteMetadata {
	if transfer.Metadata != nil {
		return *transfer.Metadata
	}
	if source != nil {
		metadata := EventWriteMetadata{
			Summary: source.Summary, Description: source.Description, Location: source.Location,
			DTStart: source.DTStart, DTEnd: source.DTEnd, AllDay: source.AllDay,
		}
		metadata.RecurrenceStart, metadata.RecurrenceUntil = recurrenceBoundsFromICal(source.RawICAL)
		return metadata
	}
	return EventWriteMetadata{}
}

func calendarObjectTransferLockKeys(transfer CalendarObjectTransfer) []string {
	keys := []string{
		fmt.Sprintf("calendar-object:%d:name:%s", transfer.SourceCalendarID, transfer.SourceResourceName),
		fmt.Sprintf("calendar-object:%d:uid:%s", transfer.SourceCalendarID, transfer.SourceUID),
		fmt.Sprintf("calendar-object:%d:name:%s", transfer.DestinationCalendarID, transfer.DestinationResourceName),
		fmt.Sprintf("calendar-object:%d:uid:%s", transfer.DestinationCalendarID, transfer.SourceUID),
	}
	slices.Sort(keys)
	return slices.Compact(keys)
}
