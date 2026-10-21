package store

import (
	"context"
	"database/sql"
	"math"
	"time"

	"github.com/lib/pq"
)

// SyncCursor is a position in a collection's change stream. The stream orders
// member rows by last_modified and tombstones by deleted_at, puts members
// before tombstones stamped at the same instant, and orders rows of one kind
// by id. The zero cursor precedes every change.
//
// Stamps alone cannot mark a position: a migration or a v1.1.x write stamped
// whole batches of rows with one transaction's start time, so one instant can
// hold more changes than a sync page.
type SyncCursor struct {
	Stamp   time.Time
	Removal bool
	ID      int64
}

// SyncCursorAt is the cursor past every change stamped at or before ts, the
// position a sync token naming the instant ts stands for.
func SyncCursorAt(ts time.Time) SyncCursor {
	return SyncCursor{Stamp: ts, Removal: true, ID: math.MaxInt64}
}

// SyncChanges is one page of a collection's change stream.
type SyncChanges[T any] struct {
	// Members are the page's member rows, in stream order.
	Members []T
	// Removals are the page's tombstones whose resource name the collection
	// does not hold in the same snapshot, in stream order. A name freed by a
	// deletion can be bound again, and the member holding it is the authority
	// over its href; its own row is a later change in the stream.
	Removals []DeletedResource
	// Last is the position of the page's last change, a tombstone left out of
	// Removals included, or the cursor the page was read after when the page
	// holds no change.
	Last SyncCursor
	// More reports whether the stream holds changes past Last.
	More bool
}

// changeStream is what one member table contributes to the change-stream read
// both collection kinds share.
type changeStream[T any] struct {
	operation    string
	resourceType string
	// members reads rows after ($2, $3) of collection $1 in stream order,
	// at most $4 of them.
	members string
	// liveNames returns which of the names in $2 collection $1 holds.
	liveNames string
	scan      func(rowScanner) (T, error)
	stamp     func(T) time.Time
	id        func(T) int64
}

const tombstonesAfterQuery = `SELECT id, resource_type, collection_id, uid, resource_name, deleted_at FROM deleted_resources WHERE resource_type=$1 AND collection_id=$2 AND (resource_type, collection_id, deleted_at, id) > ($1, $2, $3, $4) ORDER BY resource_type ASC, collection_id ASC, deleted_at ASC, id ASC LIMIT $5`

var eventChangeStream = changeStream[Event]{
	operation:    "events.list_changes_after",
	resourceType: "event",
	// The row comparison leads with the collection column so the whole
	// cursor is one bound on idx_events_change_stream; the equality beside it
	// keeps the read inside the calendar, since the row comparison alone also
	// admits every row of a calendar with a higher id.
	members:   `SELECT ` + eventColumns + ` FROM events WHERE calendar_id=$1 AND (calendar_id, last_modified, id) > ($1, $2, $3) ORDER BY calendar_id ASC, last_modified ASC, id ASC LIMIT $4`,
	liveNames: `SELECT CASE WHEN resource_name <> '' THEN resource_name ELSE uid END FROM events WHERE calendar_id=$1 AND (resource_name = ANY($2) OR (resource_name = '' AND uid = ANY($2)))`,
	scan:      scanEvent,
	stamp:     func(ev Event) time.Time { return ev.LastModified },
	id:        func(ev Event) int64 { return ev.ID },
}

var contactChangeStream = changeStream[Contact]{
	operation:    "contacts.list_changes_after",
	resourceType: "contact",
	members:      `SELECT ` + contactColumns + ` FROM contacts WHERE address_book_id=$1 AND (address_book_id, last_modified, id) > ($1, $2, $3) ORDER BY address_book_id ASC, last_modified ASC, id ASC LIMIT $4`,
	liveNames:    `SELECT CASE WHEN resource_name <> '' THEN resource_name ELSE uid END FROM contacts WHERE address_book_id=$1 AND (resource_name = ANY($2) OR (resource_name = '' AND uid = ANY($2)))`,
	scan:         scanContact,
	stamp:        func(c Contact) time.Time { return c.LastModified },
	id:           func(c Contact) int64 { return c.ID },
}

// ListChangesAfter reads up to limit changes of the calendar's change stream
// following after; tombstones are read only when removals is set.
func (r *eventRepo) ListChangesAfter(ctx context.Context, calendarID int64, after SyncCursor, removals bool, limit int) (SyncChanges[Event], error) {
	return eventChangeStream.list(ctx, r.pool, calendarID, after, removals, limit)
}

// ListChangesAfter reads up to limit changes of the address book's change
// stream following after; tombstones are read only when removals is set.
func (r *contactRepo) ListChangesAfter(ctx context.Context, addressBookID int64, after SyncCursor, removals bool, limit int) (SyncChanges[Contact], error) {
	return contactChangeStream.list(ctx, r.pool, addressBookID, after, removals, limit)
}

// list reads one page of the stream from a single snapshot. The cursor the
// page ends on is only safe to hand a client if every change it passes was
// visible to the read: member writes commit in stamp order, since each takes
// its collection's row lock and stamps past the collection's updated_at, so a
// change committed after the snapshot is stamped past everything in it. Two
// reads under separate snapshots would not give that, because a member change
// committed between them could fall behind a tombstone the second read
// returned.
func (s changeStream[T]) list(ctx context.Context, pool *sql.DB, collectionID int64, after SyncCursor, removals bool, limit int) (SyncChanges[T], error) {
	page := SyncChanges[T]{Last: after}
	if limit <= 0 {
		return page, nil
	}
	defer observeDB(ctx, s.operation)()
	tx, err := pool.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelRepeatableRead, ReadOnly: true})
	if err != nil {
		return page, err
	}
	defer func() { _ = tx.Rollback() }()

	// One row past the page from each table tells whether the stream goes on.
	fetch := limit
	if fetch < math.MaxInt {
		fetch++
	}
	memberAfterID := after.ID
	if after.Removal {
		memberAfterID = math.MaxInt64
	}
	members, err := queryChangeRows(ctx, tx, s.members, s.scan, collectionID, after.Stamp, memberAfterID, fetch)
	if err != nil {
		return page, err
	}
	var tombstones []DeletedResource
	if removals {
		tombstoneAfterID := int64(0)
		if after.Removal {
			tombstoneAfterID = after.ID
		}
		tombstones, err = queryChangeRows(ctx, tx, tombstonesAfterQuery, scanDeletedResource, s.resourceType, collectionID, after.Stamp, tombstoneAfterID, fetch)
		if err != nil {
			return page, err
		}
	}

	m, t := 0, 0
	var pageTombstones []DeletedResource
	for taken := 0; taken < limit && (m < len(members) || t < len(tombstones)); taken++ {
		if t == len(tombstones) || (m < len(members) && !tombstones[t].DeletedAt.Before(s.stamp(members[m]))) {
			page.Members = append(page.Members, members[m])
			page.Last = SyncCursor{Stamp: s.stamp(members[m]), ID: s.id(members[m])}
			m++
			continue
		}
		pageTombstones = append(pageTombstones, tombstones[t])
		page.Last = SyncCursor{Stamp: tombstones[t].DeletedAt, Removal: true, ID: tombstones[t].ID}
		t++
	}
	page.More = m < len(members) || t < len(tombstones)

	page.Removals, err = s.withoutLiveNames(ctx, tx, collectionID, pageTombstones)
	if err != nil {
		return page, err
	}
	if err := tx.Commit(); err != nil {
		return page, err
	}
	return page, nil
}

func (s changeStream[T]) withoutLiveNames(ctx context.Context, tx *sql.Tx, collectionID int64, tombstones []DeletedResource) ([]DeletedResource, error) {
	if len(tombstones) == 0 {
		return nil, nil
	}
	names := make([]string, len(tombstones))
	for i, tombstone := range tombstones {
		names[i] = tombstoneResourceName(tombstone)
	}
	live, err := queryChangeRows(ctx, tx, s.liveNames, func(scan rowScanner) (string, error) {
		var name string
		err := scan(&name)
		return name, err
	}, collectionID, pq.Array(names))
	if err != nil {
		return nil, err
	}
	if len(live) == 0 {
		return tombstones, nil
	}
	held := make(map[string]struct{}, len(live))
	for _, name := range live {
		held[name] = struct{}{}
	}
	removed := tombstones[:0]
	for _, tombstone := range tombstones {
		if _, ok := held[tombstoneResourceName(tombstone)]; !ok {
			removed = append(removed, tombstone)
		}
	}
	return removed, nil
}

// tombstoneResourceName is the name a tombstone frees: its resource name, or
// its UID when it carries none, as eventResourceName in the dav package reads
// a member's.
func tombstoneResourceName(tombstone DeletedResource) string {
	if tombstone.ResourceName != "" {
		return tombstone.ResourceName
	}
	return tombstone.UID
}

func scanDeletedResource(scan rowScanner) (DeletedResource, error) {
	var d DeletedResource
	err := scan(&d.ID, &d.ResourceType, &d.CollectionID, &d.UID, &d.ResourceName, &d.DeletedAt)
	return d, err
}

func queryChangeRows[T any](ctx context.Context, tx *sql.Tx, query string, scan func(rowScanner) (T, error), args ...any) ([]T, error) {
	rows, err := tx.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var result []T
	for rows.Next() {
		row, err := scan(rows.Scan)
		if err != nil {
			return nil, err
		}
		result = append(result, row)
	}
	return result, rows.Err()
}
