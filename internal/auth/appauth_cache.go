package auth

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"time"

	"github.com/jw6ventures/calcard/internal/store"
)

const (
	// authCacheTTL is the longest a validated app-password is trusted before we
	// re-run the full GetByEmail/FindValidByUser/bcrypt validation. An entry is
	// never given more than this and never more than the credential's own
	// remaining validity, whichever is shorter.
	//
	// The cache lives in one process. Revoking a password clears it here
	// immediately, but another replica holding its own entry keeps accepting
	// that password until the entry lapses -- so this value is also the bound
	// on how long a revocation takes to reach every replica, which is why it
	// stays short rather than being tuned up for the bcrypt saving.
	authCacheTTL = 60 * time.Second

	// lastUsedThrottle is the minimum age of last_used_at before we issue
	// another touch_last_used write, keeping that UPDATE off the hot path.
	lastUsedThrottle = 5 * time.Minute
)

type authCacheEntry struct {
	user      *store.User
	tokenID   int64
	expiresAt time.Time
}

// authCacheKey derives a cache key from the credentials without retaining the
// plaintext password in memory.
func authCacheKey(username, password string) string {
	sum := sha256.Sum256([]byte(username + "\x00" + password))
	return hex.EncodeToString(sum[:])
}

func (s *Service) authCacheGet(key string) (*store.User, bool) {
	s.authMu.Lock()
	defer s.authMu.Unlock()
	entry, ok := s.authCache[key]
	if !ok {
		return nil, false
	}
	if time.Now().After(entry.expiresAt) {
		delete(s.authCache, key)
		return nil, false
	}
	return entry.user, true
}

// authCachePut caches a validated credential until authCacheTTL from now or
// until the password's own expiry, whichever comes first. A cache hit answers
// without reading the database, so an entry outliving the password it stands
// for is the password still working after it expired.
func (s *Service) authCachePut(key string, user *store.User, tokenID int64, passwordExpiresAt *time.Time) {
	now := time.Now()
	expiresAt := now.Add(authCacheTTL)
	if passwordExpiresAt != nil && passwordExpiresAt.Before(expiresAt) {
		expiresAt = *passwordExpiresAt
	}
	if !expiresAt.After(now) {
		// Nothing left to cache; the next request has to consult the database
		// anyway, which is where the expiry is enforced.
		return
	}
	s.authMu.Lock()
	defer s.authMu.Unlock()
	if s.authCache == nil {
		s.authCache = make(map[string]authCacheEntry)
	}
	// Opportunistically drop expired entries so the map can't grow unbounded
	// as credentials rotate.
	for k, e := range s.authCache {
		if now.After(e.expiresAt) {
			delete(s.authCache, k)
		}
	}
	s.authCache[key] = authCacheEntry{user: user, tokenID: tokenID, expiresAt: expiresAt}
}

// InvalidateAppPassword drops the cached credential for one app password, so a
// revocation takes effect on this instance at once rather than at the end of
// the entry's lifetime. Other replicas hold their own caches and cannot be
// reached from here; theirs lapse within authCacheTTL.
func (s *Service) InvalidateAppPassword(tokenID int64) {
	if s == nil {
		return
	}
	s.authMu.Lock()
	defer s.authMu.Unlock()
	for key, entry := range s.authCache {
		if entry.tokenID == tokenID {
			delete(s.authCache, key)
		}
	}
}

func (s *Service) authCacheClearUser(userID int64) {
	s.authMu.Lock()
	defer s.authMu.Unlock()
	for key, entry := range s.authCache {
		if entry.user != nil && entry.user.ID == userID {
			delete(s.authCache, key)
		}
	}
}

// touchLastUsedThrottled records app-password usage off the request path. It
// skips the write entirely when last_used_at was updated recently, and runs the
// UPDATE on a detached context so a finished request can't cancel it mid-write.
func (s *Service) touchLastUsedThrottled(t store.AppPassword) {
	if t.LastUsedAt != nil && time.Since(*t.LastUsedAt) < lastUsedThrottle {
		return
	}
	id := t.ID
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = s.store.AppPasswords.TouchLastUsed(ctx, id)
	}()
}
