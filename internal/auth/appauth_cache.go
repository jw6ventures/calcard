package auth

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"time"

	"github.com/jw6ventures/calcard/internal/store"
)

const (
	// authCacheTTL is the longest a bcrypt match is reused before we re-run the
	// full GetByEmail/FindValidByUser/bcrypt validation. An entry is never given
	// more than this and never more than the credential's own remaining
	// validity, whichever is shorter.
	//
	// The cache lives in one process, and replicas cannot clear each other's
	// entries, so a hit is never trusted on its own: every hit re-reads the
	// app_passwords row by primary key (StillValid), and a revocation, expiry
	// or email change made anywhere is refused on the next request everywhere.
	// The TTL therefore bounds only how long one bcrypt result is reused and
	// how stale the cached user's profile fields and last_used_at can get.
	authCacheTTL = 60 * time.Second

	// lastUsedThrottle is the minimum age of last_used_at before we issue
	// another touch_last_used write, keeping that UPDATE off the hot path.
	lastUsedThrottle = 5 * time.Minute
)

type authCacheEntry struct {
	user      *store.User
	tokenID   int64
	tokenHash string
	expiresAt time.Time
}

// authCacheKey derives a cache key from the credentials without retaining the
// plaintext password in memory.
func authCacheKey(username, password string) string {
	sum := sha256.Sum256([]byte(username + "\x00" + password))
	return hex.EncodeToString(sum[:])
}

// authCacheGet returns the bcrypt match cached under key. The caller must still
// confirm the entry's row with StillValid before trusting it.
func (s *Service) authCacheGet(key string) (authCacheEntry, bool) {
	s.authMu.Lock()
	defer s.authMu.Unlock()
	entry, ok := s.authCache[key]
	if !ok {
		return authCacheEntry{}, false
	}
	if time.Now().After(entry.expiresAt) {
		delete(s.authCache, key)
		return authCacheEntry{}, false
	}
	return entry, true
}

func (s *Service) authCacheDelete(key string) {
	s.authMu.Lock()
	defer s.authMu.Unlock()
	delete(s.authCache, key)
}

// authCachePut caches a validated credential until authCacheTTL from now or
// until the password's own expiry, whichever comes first. The row check on
// each hit enforces expiry too; the cap keeps an entry from outliving the
// credential it stands for.
func (s *Service) authCachePut(key string, user *store.User, token store.AppPassword) {
	now := time.Now()
	expiresAt := now.Add(authCacheTTL)
	if token.ExpiresAt != nil && token.ExpiresAt.Before(expiresAt) {
		expiresAt = *token.ExpiresAt
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
	s.authCache[key] = authCacheEntry{user: user, tokenID: token.ID, tokenHash: token.TokenHash, expiresAt: expiresAt}
}

// InvalidateAppPassword drops the cached credential for one app password, so
// the next request on this instance skips straight to full validation. Other
// replicas keep their entries, but the row check on their next hit refuses the
// revoked password there too.
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
