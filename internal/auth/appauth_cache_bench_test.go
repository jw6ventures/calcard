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
	for b.Loop() {
		_ = authCacheKey("user@example.com", "correct-horse-battery-staple")
	}
}

// BenchmarkAuthCacheGet_Hit is the in-memory half of the fast path the cache
// exists to protect: a valid credential found without bcrypt. The row
// revalidation that follows a hit is a database read and is not measured here.
func BenchmarkAuthCacheGet_Hit(b *testing.B) {
	s := &Service{}
	user := &store.User{ID: 1, PrimaryEmail: "user@example.com"}
	key := authCacheKey(user.PrimaryEmail, "secret")
	s.authCachePut(key, user, store.AppPassword{ID: 1})

	b.ReportAllocs()
	for b.Loop() {
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
				s.authCachePut(authCacheKey(fmt.Sprintf("u%d", i), "p"), &store.User{ID: int64(i)}, store.AppPassword{ID: int64(i)})
			}
			key := authCacheKey("hot@example.com", "secret")
			user := &store.User{ID: 999}

			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				s.authCachePut(key, user, store.AppPassword{ID: 999})
			}
		})
	}
}
