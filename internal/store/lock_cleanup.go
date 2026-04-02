package store

import (
	"context"
	"log"
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
				log.Printf("lock cleanup error: %v", err)
				continue
			}
			if deleted > 0 {
				log.Printf("cleaned up %d expired locks", deleted)
			}
		}
	}
}
