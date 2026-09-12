package store

import (
	"context"
	"time"
)

// StartDeletedResourceCleanup periodically prunes the tombstones an incremental
// sync reports as removals. Nothing else does: the rows are written by the ctag
// triggers and only ever read, so without this the table grows for the life of
// the deployment and every sync reads from it.
//
// retention is how far back the server keeps deletion history, and it has a
// correctness floor rather than being a free choice: a tombstone pruned while a
// client's sync token still selects it would lose that deletion silently, the
// client keeping a resource the server no longer has. The DAV layer therefore
// refuses a sync token older than this same window with DAV:valid-sync-token,
// which RFC 6578 §3.2 provides for in as many words -- a server that "might only
// be able to maintain up to 3 weeks worth of changes to a collection" invalidates
// the token and the client falls back to a full synchronization. A retention of
// zero turns pruning off and leaves every token answerable.
//
// The first pass runs immediately rather than after one interval, because a
// process restarted more often than the interval would otherwise never prune.
func StartDeletedResourceCleanup(ctx context.Context, repo DeletedResourceRepository, interval, retention time.Duration) {
	if retention <= 0 {
		return
	}

	prune := func() {
		deleted, err := repo.Cleanup(ctx, retention)
		if err != nil {
			if isConnError(err) {
				queryLogger.Error("tombstone_cleanup", "tombstone cleanup failed, database appears unreachable: %v", err)
			} else {
				queryLogger.Warn("tombstone_cleanup", "tombstone cleanup failed: %v", err)
			}
			return
		}
		if deleted > 0 {
			queryLogger.Debug("tombstone_cleanup", "pruned %d tombstones older than %s", deleted, retention)
		}
	}

	prune()

	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			prune()
		}
	}
}
