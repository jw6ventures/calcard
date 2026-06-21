package dav

import (
	"context"
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

func (h *DavServer) calendarSyncTokenValue(ctx context.Context, cal *store.CalendarAccess) (string, time.Time) {
	return buildSyncToken("cal", cal.ID, cal.UpdatedAt), cal.UpdatedAt
}

func (h *DavServer) addressBookSyncTokenValue(ctx context.Context, book *store.AddressBook) (string, time.Time) {
	return buildSyncToken("card", book.ID, book.UpdatedAt), book.UpdatedAt
}
