package store

import (
	"context"
	"time"

	"github.com/jw6ventures/calcard/internal/logging"
)

// digestCredentialPurgeTimeout bounds one purge pass, a table-wide UPDATE that
// could otherwise hold a connection indefinitely behind a lock.
const digestCredentialPurgeTimeout = 30 * time.Second

// ReplaceDigestCredentials is a single conditional UPDATE, so the comparison
// and the write cannot be separated by a concurrent writer.
func (r *appPasswordRepo) ReplaceDigestCredentials(ctx context.Context, id int64, newMD5, newSHA256, oldMD5, oldSHA256 *string) (bool, error) {
	const q = `UPDATE app_passwords SET digest_md5_ha1=$2, digest_sha256_ha1=$3
        WHERE id=$1 AND digest_md5_ha1 IS NOT DISTINCT FROM $4 AND digest_sha256_ha1 IS NOT DISTINCT FROM $5`
	defer observeDB(ctx, "app_passwords.replace_digest_credentials")()
	result, err := r.pool.ExecContext(ctx, q, id, newMD5, newSHA256, oldMD5, oldSHA256)
	if err != nil {
		return false, err
	}
	changed, err := result.RowsAffected()
	if err != nil {
		return false, err
	}
	return changed == 1, nil
}

// StartDigestCredentialPurge clears every stored Digest HA1 while the
// deployment has Digest disabled, immediately and then once per interval. The
// repeat is what makes the guarantee hold: a pass that fails is retried rather
// than left for a restart, and rows written by another instance still running
// with Digest on are cleared too.
func StartDigestCredentialPurge(ctx context.Context, repo AppPasswordRepository, digestEnabled bool, interval time.Duration) {
	if repo == nil || digestEnabled {
		return
	}

	purgeDigestCredentials(ctx, repo, queryLogger)

	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			purgeDigestCredentials(ctx, repo, queryLogger)
		}
	}
}

func purgeDigestCredentials(ctx context.Context, repo AppPasswordRepository, log *logging.Logger) {
	ctx, cancel := context.WithTimeout(ctx, digestCredentialPurgeTimeout)
	defer cancel()
	purged, err := repo.PurgeDigestCredentials(ctx)
	if err != nil {
		log.Error("digest_credential_purge", "could not clear stored DAV Digest credentials while APP_DAV_DIGEST_ENABLED is off, will retry: %v", err)
		return
	}
	if purged > 0 {
		log.Warn("digest_credential_purge", "cleared stored DAV Digest credentials from %d app passwords because APP_DAV_DIGEST_ENABLED is off", purged)
	}
}
