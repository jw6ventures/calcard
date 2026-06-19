package auth

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"time"

	"github.com/jw6ventures/calcard/internal/store"
)

const (
	// authCacheTTL is how long a validated app-password is trusted before we
	// re-run the full GetByEmail/FindValidByUser/bcrypt validation. Kept short
	// so revoked or expired credentials stop working within roughly a minute.
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

func (s *Service) authCachePut(key string, user *store.User, tokenID int64) {
	now := time.Now()
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
	s.authCache[key] = authCacheEntry{user: user, tokenID: tokenID, expiresAt: now.Add(authCacheTTL)}
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
