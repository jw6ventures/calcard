package store

import (
	"context"
	"time"
)

// StartDigestNonceCleanup periodically removes spent Digest nonce counts whose
// nonce can no longer be presented. Nothing else deletes them, and every
// authenticated Digest request writes one.
//
// The first pass runs immediately rather than after one interval, because a
// process restarted more often than the interval would otherwise never prune;
// this table takes a write on every authenticated Digest request, a higher
// rate than the other rows this package prunes on a delay.
func StartDigestNonceCleanup(ctx context.Context, repo DigestNonceRepository, interval time.Duration) {
	if repo == nil {
		return
	}

	prune := func() {
		deleted, err := repo.DeleteExpired(ctx)
		if err != nil {
			if isConnError(err) {
				queryLogger.Error("digest_nonce_cleanup", "expired digest nonce cleanup failed, database appears unreachable: %v", err)
			} else {
				queryLogger.Warn("digest_nonce_cleanup", "expired digest nonce cleanup failed: %v", err)
			}
			return
		}
		if deleted > 0 {
			queryLogger.Debug("digest_nonce_cleanup", "cleaned up %d expired digest nonce counts", deleted)
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
