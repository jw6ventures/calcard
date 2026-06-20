package auth

import (
	"fmt"
	"testing"

	"github.com/jw6ventures/calcard/internal/store"
)

// BenchmarkAuthCacheKey measures the SHA-256 derivation run on every app-password
// request before the cache lookup. It's cheap relative to bcrypt but sits on the
// hot path for every DAV request, so it's worth keeping an eye on.
func BenchmarkAuthCacheKey(b *testing.B) {
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_ = authCacheKey("user@example.com", "correct-horse-battery-staple")
	}
}

// BenchmarkAuthCacheGet_Hit is the fast path the cache exists to protect: a valid
// credential served without touching the DB or bcrypt.
func BenchmarkAuthCacheGet_Hit(b *testing.B) {
	s := &Service{}
	user := &store.User{ID: 1, PrimaryEmail: "user@example.com"}
	key := authCacheKey(user.PrimaryEmail, "secret")
	s.authCachePut(key, user, 1)

	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		if _, ok := s.authCacheGet(key); !ok {
			b.Fatal("expected cache hit")
		}
	}
}

// BenchmarkAuthCachePut exercises the opportunistic expired-entry sweep that runs
// on every insert; with many live entries this scan is the dominant cost.
func BenchmarkAuthCachePut(b *testing.B) {
	for _, n := range []int{1, 100, 1000, 10000} {
		b.Run(fmt.Sprintf("entries=%d", n), func(b *testing.B) {
			s := &Service{}
			for i := 0; i < n; i++ {
				s.authCachePut(authCacheKey(fmt.Sprintf("u%d", i), "p"), &store.User{ID: int64(i)}, int64(i))
			}
			key := authCacheKey("hot@example.com", "secret")
			user := &store.User{ID: 999}

			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				s.authCachePut(key, user, 999)
			}
		})
	}
}
