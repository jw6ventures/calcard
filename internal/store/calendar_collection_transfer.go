package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"slices"
	"strings"
	"time"

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
// preconditions. RawICAL and ETag are the member exactly as it was read, which
// the transfer stores unchanged; the store rejects the transfer if the stored
// member no longer matches them. NewETag is the ETag a COPY gives the member's
// copy, and Metadata the columns its body reads as.
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

	// ExpectedACL pins the ACL entries the source and destination privilege
	// checks read.
	ExpectedACL *ACLGuard
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
	if calendarCollectionTransferStoresMembers(transfer) {
		for _, member := range transfer.Members {
			if member.RawICAL == "" || transfer.Operation == CalendarCollectionCopy && member.NewETag == "" {
				return nil, fmt.Errorf("calendar collection member %q has no body or ETag to store", member.ResourceName)
			}
		}
	}
	if s.pool == nil {
		if s.CalendarCollections == nil {
			return nil, ErrAtomicStateUnsupported
		}
		if err := validateACLGuardFallback(ctx, s.ACLEntries, transfer.ExpectedACL); err != nil {
			return nil, err
		}
		return s.CalendarCollections.TransferCalendarCollection(ctx, transfer)
	}

	tx, err := s.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()
	// The source is held even by a COPY, which leaves it in place: the member
	// set and the ACL the request was checked against must not change before
	// the copy commits. A collection whose members are removed is held too, so
	// no lock appears below it once its locks are checked.
	removedRoots := calendarCollectionTransferRemovedRoots(transfer)
	serialized := append([]string{calendarCollectionLockPath(transfer.SourceID)}, removedRoots...)
	if err := validateLockPreconditionsTx(ctx, tx, transfer.LockPreconditions, serialized...); err != nil {
		return nil, err
	}
	if err := validateLocksBelowTx(ctx, tx, removedRoots, transfer.LockPreconditions); err != nil {
		return nil, err
	}
	if err := validateACLGuardTx(ctx, tx, transfer.ExpectedACL); err != nil {
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
		// Members move with their octets and ETags, and with their columns set
		// to what their bodies read as, as a MOVE of one object sets them. Each
		// row is written once, so its triggers fire as for one write of it.
		if err := deferCalendarCTagTx(ctx, tx, moved.ID, func() error {
			if err := moveEventsWithColumnsTx(ctx, tx, source.ID, moved.ID, transfer.Members); err != nil {
				return err
			}
			_, err := tx.ExecContext(ctx, `UPDATE events SET calendar_id=$1, last_modified=NOW() WHERE calendar_id=$2`, moved.ID, source.ID)
			return err
		}); err != nil {
			return nil, err
		}
		if err := moveTransferredCalendarCTagTx(ctx, tx, moved.ID, len(transfer.Members), destination); err != nil {
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
		written := 0
		if transfer.Depth == "infinity" {
			if err := deferCalendarCTagTx(ctx, tx, copied.ID, func() error {
				return copyEventsTx(ctx, tx, source.ID, copied.ID, transfer.Members)
			}); err != nil {
				return nil, err
			}
			if err := copyCalendarMemberDeadPropertiesTx(ctx, tx, transfer.SourceStatePath, calendarCollectionStatePath(copied.ID), transfer.Members); err != nil {
				return nil, err
			}
			written = len(transfer.Members)
		}
		if err := moveTransferredCalendarCTagTx(ctx, tx, copied.ID, written, destination); err != nil {
			return nil, err
		}
		copied, err = selectCalendarForUpdate(ctx, tx, copied.ID)
		if err != nil {
			return nil, err
		}
		return &CalendarCollectionTransferResult{Calendar: copied, Created: created}, nil
	}
	return nil, ErrNotFound
}

// moveEventsWithColumnsTx moves, in one statement, each of members still in
// from whose indexed columns differ from the metadata its body reads as into
// to, setting those columns. The rows whose columns already agree are left in
// from for the caller to move.
func moveEventsWithColumnsTx(ctx context.Context, tx *sql.Tx, from, to int64, members []CalendarCollectionMember) error {
	if len(members) == 0 {
		return nil
	}
	columns := newMemberColumns(members)
	const update = `
UPDATE events AS e
SET calendar_id = $11, summary = m.summary, description = m.description, location = m.location,
    dtstart = m.dtstart::timestamptz, dtend = m.dtend::timestamptz, all_day = m.all_day,
    recurrence_start = m.recurrence_start::timestamptz, recurrence_until = m.recurrence_until::timestamptz
FROM unnest($2::bigint[], $3::text[], $4::text[], $5::text[], $6::text[], $7::text[], $8::boolean[], $9::text[], $10::text[])
    AS m(id, summary, description, location, dtstart, dtend, all_day, recurrence_start, recurrence_until)
WHERE e.id = m.id AND e.calendar_id = $1
  AND (e.summary IS DISTINCT FROM m.summary OR e.description IS DISTINCT FROM m.description
    OR e.location IS DISTINCT FROM m.location OR e.dtstart IS DISTINCT FROM m.dtstart::timestamptz
    OR e.dtend IS DISTINCT FROM m.dtend::timestamptz OR e.all_day IS DISTINCT FROM m.all_day
    OR e.recurrence_start IS DISTINCT FROM m.recurrence_start::timestamptz
    OR e.recurrence_until IS DISTINCT FROM m.recurrence_until::timestamptz)`
	_, err := tx.ExecContext(ctx, update, from, pq.Array(columns.ids),
		pq.Array(columns.summary), pq.Array(columns.description), pq.Array(columns.location), pq.Array(columns.dtstart), pq.Array(columns.dtend),
		pq.Array(columns.allDay), pq.Array(columns.recurrenceStart), pq.Array(columns.recurrenceUntil), to)
	return err
}

// deferCalendarCTagTx runs write, which writes members of calendar calendarID,
// with their triggers leaving that calendar's CTag alone. The caller moves the
// CTag itself once write is done, which keeps a write of many members of one
// calendar linear in their number.
func deferCalendarCTagTx(ctx context.Context, tx *sql.Tx, calendarID int64, write func() error) error {
	if _, err := tx.ExecContext(ctx, `SELECT set_config('calcard.defer_calendar_ctag', $1::bigint::text, true)`, calendarID); err != nil {
		return err
	}
	if err := write(); err != nil {
		return err
	}
	_, err := tx.ExecContext(ctx, `SELECT set_config('calcard.defer_calendar_ctag', '', true)`)
	return err
}

// moveTransferredCalendarCTagTx moves the CTag of calendar calendarID, which a
// transfer created and wrote written members into, as far as those member
// writes would have one by one, and past the CTag of the calendar it replaced
// at the same location, if any: a client polling that location would otherwise
// be shown a value it already holds. The update also moves the calendar's sync
// token past every member stamp, each of which was read from the clock before
// it.
func moveTransferredCalendarCTagTx(ctx context.Context, tx *sql.Tx, calendarID int64, written int, replaced *Calendar) error {
	floor := int64(0)
	if replaced != nil {
		floor = replaced.CTag + 1
	}
	if written == 0 && floor == 0 {
		return nil
	}
	_, err := tx.ExecContext(ctx, `UPDATE calendars SET ctag = GREATEST(ctag + $2, $3) WHERE id = $1`, calendarID, written, floor)
	return err
}

// copyEventsTx inserts, in one statement, a copy of each of members into
// calendar to, under its new ETag and with its columns set to what its body
// reads as. The body is read from the member's row in from, which the caller
// has locked and checked against the member.
func copyEventsTx(ctx context.Context, tx *sql.Tx, from, to int64, members []CalendarCollectionMember) error {
	if len(members) == 0 {
		return nil
	}
	columns := newMemberColumns(members)
	uids := make([]string, len(members))
	names := make([]string, len(members))
	etags := make([]string, len(members))
	for i, member := range members {
		uids[i], names[i], etags[i] = member.UID, member.ResourceName, member.NewETag
	}
	const insert = `
INSERT INTO events (calendar_id, uid, resource_name, raw_ical, etag, summary, description, location, dtstart, dtend, all_day, recurrence_start, recurrence_until, last_modified)
SELECT $1, m.uid, m.resource_name, e.raw_ical, m.etag, m.summary, m.description, m.location,
       m.dtstart::timestamptz, m.dtend::timestamptz, m.all_day, m.recurrence_start::timestamptz, m.recurrence_until::timestamptz, NOW()
FROM unnest($3::bigint[], $4::text[], $5::text[], $6::text[], $7::text[], $8::text[], $9::text[], $10::text[], $11::text[], $12::boolean[], $13::text[], $14::text[])
    WITH ORDINALITY AS m(id, uid, resource_name, etag, summary, description, location, dtstart, dtend, all_day, recurrence_start, recurrence_until, position)
JOIN events e ON e.id = m.id AND e.calendar_id = $2
ORDER BY m.position`
	result, err := tx.ExecContext(ctx, insert, to, from, pq.Array(columns.ids), pq.Array(uids), pq.Array(names), pq.Array(etags),
		pq.Array(columns.summary), pq.Array(columns.description), pq.Array(columns.location), pq.Array(columns.dtstart), pq.Array(columns.dtend),
		pq.Array(columns.allDay), pq.Array(columns.recurrenceStart), pq.Array(columns.recurrenceUntil))
	if err != nil {
		return err
	}
	copied, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if copied != int64(len(members)) {
		return ErrResourceStateChanged
	}
	return nil
}

// copyCalendarMemberDeadPropertiesTx copies, in one statement, each member's
// dead properties from beneath fromRoot to the same name beneath toRoot, as
// copyCalendarDeadPropertiesTx copies one resource's.
func copyCalendarMemberDeadPropertiesTx(ctx context.Context, tx *sql.Tx, fromRoot, toRoot string, members []CalendarCollectionMember) error {
	var toPaths, fromPaths []string
	for _, member := range members {
		toPaths = append(toPaths, MemberStatePath(toRoot, member.ResourceName))
		fromPaths = append(fromPaths, MemberStatePath(fromRoot, member.ResourceName))
	}
	if len(toPaths) == 0 {
		return nil
	}
	_, err := tx.ExecContext(ctx, `
INSERT INTO dav_dead_properties (resource_path, namespace_uri, local_name, inner_xml, created_at, updated_at)
SELECT m.to_path, d.namespace_uri, d.local_name, d.inner_xml, NOW(), NOW()
FROM unnest($1::text[], $2::text[]) AS m(to_path, from_path)
JOIN dav_dead_properties d ON d.resource_path = m.from_path`,
		pq.Array(toPaths), pq.Array(fromPaths))
	return err
}

// memberColumns holds, one array per column, the row ids of members and the
// columns their bodies read as, for a statement that writes them all at once.
type memberColumns struct {
	ids                              []int64
	allDay                           []bool
	summary, description, location   []sql.NullString
	dtstart, dtend                   []sql.NullString
	recurrenceStart, recurrenceUntil []sql.NullString
}

func newMemberColumns(members []CalendarCollectionMember) memberColumns {
	columns := memberColumns{
		ids:             make([]int64, len(members)),
		allDay:          make([]bool, len(members)),
		summary:         make([]sql.NullString, len(members)),
		description:     make([]sql.NullString, len(members)),
		location:        make([]sql.NullString, len(members)),
		dtstart:         make([]sql.NullString, len(members)),
		dtend:           make([]sql.NullString, len(members)),
		recurrenceStart: make([]sql.NullString, len(members)),
		recurrenceUntil: make([]sql.NullString, len(members)),
	}
	for i, member := range members {
		metadata := calendarCollectionMemberMetadata(member)
		columns.ids[i], columns.allDay[i] = member.ID, metadata.AllDay
		columns.summary[i], columns.description[i], columns.location[i] = nullableText(metadata.Summary), nullableText(metadata.Description), nullableText(metadata.Location)
		columns.dtstart[i], columns.dtend[i] = nullableInstant(metadata.DTStart), nullableInstant(metadata.DTEnd)
		columns.recurrenceStart[i], columns.recurrenceUntil[i] = nullableInstant(metadata.RecurrenceStart), nullableInstant(metadata.RecurrenceUntil)
	}
	return columns
}

func nullableText(value *string) sql.NullString {
	if value == nil {
		return sql.NullString{}
	}
	return sql.NullString{String: *value, Valid: true}
}

func nullableInstant(value *time.Time) sql.NullString {
	if value == nil {
		return sql.NullString{}
	}
	return sql.NullString{String: value.UTC().Format(time.RFC3339Nano), Valid: true}
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

// calendarCollectionTransferStoresMembers reports whether the transfer writes
// its members, as opposed to only guarding on them.
func calendarCollectionTransferStoresMembers(transfer CalendarCollectionTransfer) bool {
	return transfer.Operation == CalendarCollectionMove ||
		transfer.Operation == CalendarCollectionCopy && transfer.Depth == "infinity"
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

// calendarCollectionTransferRemovedRoots names the collections whose members
// transfer removes: the source of a DELETE or MOVE, and the destination a COPY
// or MOVE overwrites.
func calendarCollectionTransferRemovedRoots(transfer CalendarCollectionTransfer) []string {
	var roots []string
	if transfer.Operation != CalendarCollectionCopy {
		roots = append(roots, transfer.SourceStatePath)
	}
	if transfer.Operation != CalendarCollectionDelete && transfer.ExpectedDestination.Exists {
		roots = append(roots, transfer.DestinationStatePath)
	}
	return roots
}

// locksBelowQuery lists the tokens of the active locks whose paths fall in
// any of the byte ranges [$1[i], $2[i]). The ~>=~ and ~<~ operators compare
// bytewise, as the resource path pattern index orders, so each range is read
// from that index.
const locksBelowQuery = `SELECT l.token FROM unnest($1::text[], $2::text[]) AS r(low, high)
JOIN locks l ON l.resource_path ~>=~ r.low AND l.resource_path ~<~ r.high
WHERE l.expires_at > NOW()`

// locksBelowRanges turns each root into the byte range holding exactly the
// paths below it: those starting with root + "/", which sort at or after it
// and before root + "0", "0" being the byte after "/".
func locksBelowRanges(roots []string) (low, high []string) {
	for _, root := range roots {
		if root = strings.TrimSuffix(root, "/"); root != "" {
			low = append(low, root+"/")
			high = append(high, root+"0")
		}
	}
	return low, high
}

// validateLocksBelowTx refuses with ErrLockConflict when an active lock below
// any of roots carries a token no precondition submits. The preconditions name
// the members a request found, while a lock-null resource below a collection
// has no member to be found by, and one locked after the request's own check
// is found only here. The caller holds the roots' path locks, which a LOCK
// below them waits on.
func validateLocksBelowTx(ctx context.Context, tx *sql.Tx, roots []string, preconditions []LockPrecondition) error {
	low, high := locksBelowRanges(roots)
	if len(low) == 0 {
		return nil
	}
	submitted := make(map[string]struct{})
	for _, precondition := range preconditions {
		for _, token := range precondition.Tokens {
			submitted[token] = struct{}{}
		}
	}
	rows, err := tx.QueryContext(ctx, locksBelowQuery, pq.Array(low), pq.Array(high))
	if err != nil {
		return err
	}
	defer rows.Close()
	for rows.Next() {
		var token string
		if err := rows.Scan(&token); err != nil {
			return err
		}
		if _, ok := submitted[token]; !ok {
			return ErrLockConflict
		}
	}
	return rows.Err()
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
SELECT $1, namespace_uri, local_name, inner_xml, NOW(), NOW()
FROM dav_dead_properties
WHERE resource_path = $2`, toPath, fromPath)
	return err
}

func calendarCollectionStatePath(id int64) string {
	return fmt.Sprintf("/dav/calendars/%d", id)
}
