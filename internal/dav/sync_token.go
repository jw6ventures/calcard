package dav

import (
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/jw6ventures/calcard/internal/store"
)

const syncTokenPrefix = "urn:calcard-sync"

type syncTokenInfo struct {
	Kind      string
	ID        int64
	Timestamp time.Time
	// State is the optional trailing segment: the whole state a collection with
	// no change history was in, or the position of a truncated page.
	State string
}

func buildSyncToken(kind string, id int64, ts time.Time) string {
	return fmt.Sprintf("%s:%s:%d:%d", syncTokenPrefix, kind, id, syncTokenNanos(ts))
}

// buildSyncTokenWithState spells a token for a collection that answers a client
// token by comparing whole states rather than by reading changes since an
// instant. The timestamp alone cannot stand in for such a state: it moves only
// with the newest change it can see, so a removal it has no record of would
// leave the token unmoved.
func buildSyncTokenWithState(kind string, id int64, ts time.Time, state string) string {
	return fmt.Sprintf("%s:%s:%d:%d:%s", syncTokenPrefix, kind, id, syncTokenNanos(ts), state)
}

func syncTokenNanos(ts time.Time) int64 {
	if ts.IsZero() {
		return 0
	}
	return ts.UTC().UnixNano()
}

func parseSyncToken(token string) (syncTokenInfo, error) {
	if token == "" || !strings.HasPrefix(token, syncTokenPrefix+":") {
		return syncTokenInfo{}, errInvalidSyncToken
	}
	parts := strings.Split(token[len(syncTokenPrefix)+1:], ":")
	if len(parts) != 3 && len(parts) != 4 {
		return syncTokenInfo{}, errInvalidSyncToken
	}
	id, err := strconv.ParseInt(parts[1], 10, 64)
	if err != nil {
		return syncTokenInfo{}, errInvalidSyncToken
	}
	nanos, err := strconv.ParseInt(parts[2], 10, 64)
	if err != nil {
		return syncTokenInfo{}, errInvalidSyncToken
	}
	info := syncTokenInfo{Kind: parts[0], ID: id}
	if len(parts) == 4 {
		info.State = parts[3]
	}
	if nanos > 0 {
		info.Timestamp = time.Unix(0, nanos).UTC()
	}
	return info, nil
}

// syncTokenAnswerable reports whether the server can still report every change
// since tokenTime. RFC 6578 §3.2 lets a server invalidate a token it cannot
// answer and names this case: a server that "might only be able to maintain up
// to 3 weeks worth of changes to a collection" refuses the token, and the client
// falls back to a full synchronization. CalCard's history is as long as the
// tombstones it keeps, so the retention window is the answer to both questions.
//
// collectionUpdatedAt is the collection's own last-change watermark, and a token
// naming it is answerable however old it is: nothing has changed since, so there
// is no deletion to report and none can have been pruned out from under it.
// Without that case a collection nobody has touched in longer than the window
// would force a full resync on every sync forever, since the token the server
// hands back names that same instant.
//
// Sync handlers check again after all history reads: cleanup can advance its
// cutoff while an accepted request is reading, invalidating its earlier check.
func (h *DavServer) syncTokenAnswerable(tokenTime, collectionUpdatedAt time.Time) bool {
	retention := h.syncHistoryRetention()
	if retention <= 0 {
		// Nothing is pruned, so no token becomes unanswerable through age.
		return true
	}
	if tokenTime.IsZero() {
		// An initial sync reports the collection whole and reads no tombstones.
		return true
	}
	if !time.Now().UTC().Add(-retention).After(tokenTime) {
		return true
	}
	return collectionUpdatedAt.Equal(tokenTime)
}

// syncPosition is where a stored collection's sync-collection report resumes
// its collection's change stream.
type syncPosition struct {
	after store.SyncCursor
	// fresh marks a report with no client token: the first page of a listing,
	// which reads no tombstones because the client holds nothing to remove.
	fresh bool
	// listing marks a report delivering a listing of the collection, fresh or
	// resumed from a truncated page, rather than the changes since a state the
	// client already holds.
	listing bool
	// anchor is the instant the retention window is measured from: the
	// client's token for changes, and the collection's token when a listing
	// began. A listing's first pages end on the oldest stamps in the
	// collection, so measuring from the cursor would refuse the continuation of
	// every listing of a collection older than the window, and the client
	// would start the listing over forever. A tombstone older than the listing
	// names a resource the listing never held.
	anchor time.Time
}

// The two kinds of truncated page a token resumes.
const (
	syncPageListing = "listing"
	syncPageChanges = "changes"
)

// parseStoredSyncPosition reads the client's token for a stored collection of
// the given kind and id. A stored collection never spells a whole-state token,
// so a state segment other than a page position was not issued for it.
func parseStoredSyncPosition(token, kind string, id int64) (syncPosition, error) {
	if token == "" {
		return syncPosition{fresh: true, listing: true}, nil
	}
	info, err := parseSyncToken(token)
	if err != nil || info.Kind != kind || info.ID != id {
		return syncPosition{}, errInvalidSyncToken
	}
	if info.State == "" {
		if info.Timestamp.IsZero() {
			return syncPosition{fresh: true, listing: true}, nil
		}
		return syncPosition{after: store.SyncCursorAt(info.Timestamp), anchor: info.Timestamp}, nil
	}
	parts := strings.Split(info.State, "-")
	if len(parts) != 4 {
		return syncPosition{}, errInvalidSyncToken
	}
	position := syncPosition{anchor: info.Timestamp}
	switch parts[0] {
	case syncPageListing:
		position.listing = true
	case syncPageChanges:
	default:
		return syncPosition{}, errInvalidSyncToken
	}
	switch parts[1] {
	case "m":
	case "t":
		position.after.Removal = true
	default:
		return syncPosition{}, errInvalidSyncToken
	}
	rowID, err := strconv.ParseInt(parts[2], 10, 64)
	if err != nil || rowID <= 0 {
		return syncPosition{}, errInvalidSyncToken
	}
	nanos, err := strconv.ParseInt(parts[3], 10, 64)
	if err != nil || nanos <= 0 {
		return syncPosition{}, errInvalidSyncToken
	}
	position.after.ID = rowID
	position.after.Stamp = time.Unix(0, nanos).UTC()
	return position, nil
}

// buildSyncPageToken spells the token RFC 6578 §3.6 hands back with a
// truncated page: it resumes the change stream after the page's last change,
// and carries the anchor the retention window is measured from.
func buildSyncPageToken(kind string, id int64, position syncPosition, last store.SyncCursor) string {
	phase := syncPageChanges
	if position.listing {
		phase = syncPageListing
	}
	row := "m"
	if last.Removal {
		row = "t"
	}
	state := fmt.Sprintf("%s-%s-%d-%d", phase, row, last.ID, syncTokenNanos(last.Stamp))
	return buildSyncTokenWithState(kind, id, position.anchor, state)
}

func (h *DavServer) syncHistoryRetention() time.Duration {
	if h != nil && h.cfg != nil {
		return h.cfg.DAV.SyncHistoryRetention
	}
	return 0
}

func (h *DavServer) calendarSyncTokenValue(cal *store.CalendarAccess) (string, time.Time) {
	return buildSyncToken("cal", cal.ID, cal.UpdatedAt), cal.UpdatedAt
}

func (h *DavServer) addressBookSyncTokenValue(book *store.AddressBook) (string, time.Time) {
	return buildSyncToken("card", book.ID, book.UpdatedAt), book.UpdatedAt
}
