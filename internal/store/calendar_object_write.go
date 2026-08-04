package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"slices"
)

// ErrUIDConflict means the submitted UID is already bound to another resource
// name in the target calendar collection, which RFC 4791 §4.1 forbids.
var ErrUIDConflict = errors.New("calendar object UID conflict")

// ErrPreconditionFailed means a conditional request's If-Match or If-None-Match
// requirement no longer held when the write was attempted.
var ErrPreconditionFailed = errors.New("calendar object precondition failed")

// ErrResourceStateChanged means the target changed between the handler's
// privilege decision and the atomic write. The handler must authorize the new
// create/update shape before retrying it.
var ErrResourceStateChanged = errors.New("calendar object resource state changed")

// ETagCondition is one RFC 7232 conditional header reduced to what the store
// can decide: Any is the "*" form, and ETags lists the entity tags the header
// named that are eligible under the comparison that header uses. An empty list
// with Any false means no stored tag can satisfy it.
type ETagCondition struct {
	Any   bool
	ETags []string
}

// CalendarObjectPrecondition is what a conditional PUT requires of whatever
// already occupies the target resource name. A nil field means the request
// carried no such header. It is stated without HTTP header syntax so the store
// can apply it to the row it has locked, which is the only point at which the
// answer cannot be overtaken by a competing writer.
type CalendarObjectPrecondition struct {
	IfMatch     *ETagCondition
	IfNoneMatch *ETagCondition
}

// CalendarObjectResourceState records the existence class the HTTP layer used
// to choose between DAV:bind and DAV:write-content authorization.
type CalendarObjectResourceState struct {
	Exists bool
}

func (p CalendarObjectPrecondition) satisfiedBy(existing *Event) bool {
	// RFC 7232 §3.1: If-Match fails outright where nothing is mapped.
	if p.IfMatch != nil {
		if existing == nil {
			return false
		}
		if !p.IfMatch.Any && !slices.Contains(p.IfMatch.ETags, existing.ETag) {
			return false
		}
	}
	// §3.2: If-None-Match constrains only what is already there.
	if p.IfNoneMatch != nil && existing != nil {
		if p.IfNoneMatch.Any || slices.Contains(p.IfNoneMatch.ETags, existing.ETag) {
			return false
		}
	}
	return true
}

// CalendarObjectWrite is one PUT of a calendar object resource. Identity, UID
// uniqueness and the conditional-header requirements travel with it so they are
// re-evaluated where the write happens rather than in a separate earlier read.
type CalendarObjectWrite struct {
	CalendarID           int64
	UID                  string
	ResourceName         string
	RawICAL              string
	ETag                 string
	Metadata             *EventWriteMetadata
	Precondition         CalendarObjectPrecondition
	ExpectedState        *CalendarObjectResourceState
	ExpectedCalendarCTag *int64
	LockPreconditions    []LockPrecondition
}

// CalendarObjectWriteResult reports what a write did. Conflict names the
// resource that blocked it, which RFC 4791 §5.3.2.1 asks a CALDAV:no-uid-conflict
// response to identify.
type CalendarObjectWriteResult struct {
	Event    *Event
	Created  bool
	Conflict *Event
}

// CalendarObjectWriter is an atomic calendar-object write backend for stores
// that do not use Store's PostgreSQL transaction. Implementations resolve the
// target resource, the UID owner and the precondition in one indivisible step.
type CalendarObjectWriter interface {
	PutCalendarObject(ctx context.Context, write CalendarObjectWrite) (*CalendarObjectWriteResult, error)
}

// PutCalendarObject stores a calendar object resource, resolving in one step
// what a read followed by an upsert cannot: whether the target resource name is
// already mapped, whether the submitted UID belongs to a different resource in
// the same collection (RFC 4791 §4.1), and whether the request's conditional
// headers still hold. Two concurrent PUTs naming one UID at different resource
// names therefore produce one 201 and one CALDAV:no-uid-conflict rather than two
// creations, the second of which silently renames the first.
//
// A store without a connection pool delegates to its CalendarObjectWriter when
// it has one. Failing that it applies the same rules through the event
// repository, which is all a backend offering no atomic write can do; that path
// exists for in-process fakes, and a real backend that needs the guarantee
// supplies the writer.
func (s *Store) PutCalendarObject(ctx context.Context, write CalendarObjectWrite) (*CalendarObjectWriteResult, error) {
	if s == nil || s.Events == nil {
		return nil, ErrNotFound
	}
	if write.ResourceName == "" {
		write.ResourceName = write.UID
	}
	if s.pool == nil {
		if err := validateLockPreconditionsFallback(ctx, s.Locks, write.LockPreconditions); err != nil {
			return nil, err
		}
		if s.CalendarObjects != nil {
			return s.CalendarObjects.PutCalendarObject(ctx, write)
		}
		return putCalendarObjectViaRepository(ctx, s.Events, write)
	}

	tx, err := s.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()
	if err := validateLockPreconditionsTx(ctx, tx, write.LockPreconditions); err != nil {
		return nil, err
	}
	if err := validateCollectionCTagsTx(ctx, tx, "calendars",
		collectionCTagExpectation{id: write.CalendarID, ctag: write.ExpectedCalendarCTag}); err != nil {
		return nil, err
	}

	result, err := putCalendarObjectTx(ctx, tx, write)
	if err != nil {
		return result, err
	}
	if err := tx.Commit(); err != nil {
		return nil, err
	}
	return result, nil
}

const calendarObjectColumns = `id, calendar_id, uid, resource_name, raw_ical, etag, summary, description, location, dtstart, dtend, all_day, last_modified`

func putCalendarObjectTx(ctx context.Context, tx *sql.Tx, write CalendarObjectWrite) (*CalendarObjectWriteResult, error) {
	// Serialize on the two identities this write can contend for, in a fixed
	// order so two requests holding them in common cannot deadlock against each
	// other. Row locks alone would not do: the rows may not exist yet, and it is
	// exactly the concurrent creation that has to be ordered.
	for _, key := range calendarObjectLockKeys(write) {
		if _, err := tx.ExecContext(ctx, `SELECT pg_advisory_xact_lock(hashtext($1))`, key); err != nil {
			return nil, err
		}
	}

	existing, err := selectEventTx(ctx, tx, `resource_name`, write.CalendarID, write.ResourceName)
	if err != nil {
		return nil, err
	}
	if write.ExpectedState != nil && write.ExpectedState.Exists != (existing != nil) {
		return nil, ErrResourceStateChanged
	}
	if existing != nil && existing.UID != write.UID {
		return &CalendarObjectWriteResult{Conflict: existing}, ErrUIDConflict
	}
	byUID, err := selectEventTx(ctx, tx, `uid`, write.CalendarID, write.UID)
	if err != nil {
		return nil, err
	}

	if conflict := calendarObjectConflict(write, existing, byUID); conflict != nil {
		return &CalendarObjectWriteResult{Conflict: conflict}, ErrUIDConflict
	}
	if !write.Precondition.satisfiedBy(existing) {
		return nil, ErrPreconditionFailed
	}

	metadata := calendarObjectMetadata(write)
	if existing == nil {
		const insert = `
INSERT INTO events (calendar_id, uid, resource_name, raw_ical, etag, summary, description, location, dtstart, dtend, all_day, recurrence_start, recurrence_until, last_modified)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, NOW())
ON CONFLICT DO NOTHING
RETURNING ` + calendarObjectColumns
		row := tx.QueryRowContext(ctx, insert, write.CalendarID, write.UID, write.ResourceName, write.RawICAL, write.ETag,
			metadata.Summary, metadata.Description, metadata.Location, metadata.DTStart, metadata.DTEnd, metadata.AllDay,
			metadata.RecurrenceStart, metadata.RecurrenceUntil)
		created, err := scanEvent(row.Scan)
		if errors.Is(err, sql.ErrNoRows) {
			return calendarObjectConflictAfterWrite(ctx, tx, write)
		}
		if err != nil {
			return nil, err
		}
		return &CalendarObjectWriteResult{Event: &created, Created: true}, nil
	}

	const update = `
UPDATE events SET uid=$2, raw_ical=$3, etag=$4, summary=$5, description=$6, location=$7,
        dtstart=$8, dtend=$9, all_day=$10, recurrence_start=$11, recurrence_until=$12, last_modified=NOW()
WHERE id=$1
RETURNING ` + calendarObjectColumns
	row := tx.QueryRowContext(ctx, update, existing.ID, write.UID, write.RawICAL, write.ETag,
		metadata.Summary, metadata.Description, metadata.Location, metadata.DTStart, metadata.DTEnd, metadata.AllDay,
		metadata.RecurrenceStart, metadata.RecurrenceUntil)
	updated, err := scanEvent(row.Scan)
	if err != nil {
		if isEventIdentityConflict(err) {
			return nil, ErrConflict
		}
		return nil, err
	}
	return &CalendarObjectWriteResult{Event: &updated}, nil
}

func calendarObjectLockKeys(write CalendarObjectWrite) []string {
	keys := []string{
		fmt.Sprintf("calendar-object:%d:name:%s", write.CalendarID, write.ResourceName),
		fmt.Sprintf("calendar-object:%d:uid:%s", write.CalendarID, write.UID),
	}
	slices.Sort(keys)
	return slices.Compact(keys)
}

func selectEventTx(ctx context.Context, tx *sql.Tx, column string, calendarID int64, value string) (*Event, error) {
	query := `SELECT ` + calendarObjectColumns + ` FROM events WHERE calendar_id=$1 AND ` + column + `=$2 FOR UPDATE`
	ev, err := scanEvent(tx.QueryRowContext(ctx, query, calendarID, value).Scan)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, nil
		}
		return nil, err
	}
	return &ev, nil
}

// calendarObjectConflict reports the resource that stops this write on RFC 4791
// §4.1 identity grounds: another resource name already holding the submitted
// UID, or the target resource itself when the request would change the UID it
// was created with.
func calendarObjectConflict(write CalendarObjectWrite, existing, byUID *Event) *Event {
	if existing != nil && existing.UID != write.UID {
		return existing
	}
	if byUID != nil && storedResourceName(*byUID) != write.ResourceName {
		return byUID
	}
	return nil
}

func calendarObjectConflictAfterWrite(ctx context.Context, tx *sql.Tx, write CalendarObjectWrite) (*CalendarObjectWriteResult, error) {
	existing, err := selectEventTx(ctx, tx, `resource_name`, write.CalendarID, write.ResourceName)
	if err != nil {
		return nil, err
	}
	byUID, err := selectEventTx(ctx, tx, `uid`, write.CalendarID, write.UID)
	if err != nil {
		return nil, err
	}
	if conflict := calendarObjectConflict(write, existing, byUID); conflict != nil {
		return &CalendarObjectWriteResult{Conflict: conflict}, ErrUIDConflict
	}
	return nil, ErrConflict
}

// storedResourceName is the resource name a row answers to. Upsert defaults an
// empty one to the UID, so a row written before resource_name existed is
// addressed by its UID and must compare that way here too.
func storedResourceName(event Event) string {
	if event.ResourceName != "" {
		return event.ResourceName
	}
	return event.UID
}

func calendarObjectMetadata(write CalendarObjectWrite) EventWriteMetadata {
	if write.Metadata != nil {
		return *write.Metadata
	}
	var metadata EventWriteMetadata
	metadata.Summary, metadata.Description, metadata.Location, metadata.DTStart, metadata.DTEnd, metadata.AllDay = parseICalFields(write.RawICAL)
	metadata.RecurrenceStart, metadata.RecurrenceUntil = recurrenceBoundsFromICal(write.RawICAL)
	return metadata
}

// putCalendarObjectViaRepository applies the same decisions through the plain
// repository interface, for a store with neither a connection pool nor a
// CalendarObjectWriter.
func putCalendarObjectViaRepository(ctx context.Context, events EventRepository, write CalendarObjectWrite) (*CalendarObjectWriteResult, error) {
	existing, err := events.GetByResourceName(ctx, write.CalendarID, write.ResourceName)
	if err != nil {
		return nil, err
	}
	if write.ExpectedState != nil && write.ExpectedState.Exists != (existing != nil) {
		return nil, ErrResourceStateChanged
	}
	if existing != nil && existing.UID != write.UID {
		return &CalendarObjectWriteResult{Conflict: existing}, ErrUIDConflict
	}
	byUID, err := events.GetByUID(ctx, write.CalendarID, write.UID)
	if err != nil {
		return nil, err
	}
	if conflict := calendarObjectConflict(write, existing, byUID); conflict != nil {
		return &CalendarObjectWriteResult{Conflict: conflict}, ErrUIDConflict
	}
	if !write.Precondition.satisfiedBy(existing) {
		return nil, ErrPreconditionFailed
	}

	stored, err := events.Upsert(ctx, Event{
		CalendarID:    write.CalendarID,
		UID:           write.UID,
		ResourceName:  write.ResourceName,
		RawICAL:       write.RawICAL,
		ETag:          write.ETag,
		WriteMetadata: write.Metadata,
	})
	if err != nil {
		if errors.Is(err, ErrConflict) {
			return repositoryCalendarObjectConflict(ctx, events, write)
		}
		return nil, err
	}
	return &CalendarObjectWriteResult{Event: stored, Created: existing == nil}, nil
}

func repositoryCalendarObjectConflict(ctx context.Context, events EventRepository, write CalendarObjectWrite) (*CalendarObjectWriteResult, error) {
	existing, resourceErr := events.GetByResourceName(ctx, write.CalendarID, write.ResourceName)
	byUID, uidErr := events.GetByUID(ctx, write.CalendarID, write.UID)
	if resourceErr != nil {
		return nil, resourceErr
	}
	if uidErr != nil {
		return nil, uidErr
	}
	if conflict := calendarObjectConflict(write, existing, byUID); conflict != nil {
		return &CalendarObjectWriteResult{Conflict: conflict}, ErrUIDConflict
	}
	return nil, ErrConflict
}
