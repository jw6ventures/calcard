package store

import (
	"context"
	"time"
)

// StartLockCleanup periodically removes expired locks from the database.
func StartLockCleanup(ctx context.Context, repo LockRepository, interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			deleted, err := repo.DeleteExpired(ctx)
			if err != nil {
				if isConnError(err) {
					queryLogger.Error("lock_cleanup", "expired-lock cleanup failed, database appears unreachable: %v", err)
				} else {
					queryLogger.Warn("lock_cleanup", "expired-lock cleanup failed: %v", err)
				}
				continue
			}
			if deleted > 0 {
				queryLogger.Debug("lock_cleanup", "cleaned up %d expired locks", deleted)
			}
		}
	}
}
