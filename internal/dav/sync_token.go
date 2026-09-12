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
}

func buildSyncToken(kind string, id int64, ts time.Time) string {
	nanos := int64(0)
	if !ts.IsZero() {
		nanos = ts.UTC().UnixNano()
	}
	return fmt.Sprintf("%s:%s:%d:%d", syncTokenPrefix, kind, id, nanos)
}

func parseSyncToken(token string) (syncTokenInfo, error) {
	if token == "" || !strings.HasPrefix(token, syncTokenPrefix+":") {
		return syncTokenInfo{}, errInvalidSyncToken
	}
	parts := strings.Split(token[len(syncTokenPrefix)+1:], ":")
	if len(parts) != 3 {
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
