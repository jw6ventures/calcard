package store

import (
	"slices"
	"testing"
)

// A precondition is its own serialization target unless its path is a strict
// ancestor of another precondition's, which serializes it as that path's
// ancestor. Sibling names that only share a prefix are not ancestors.
func TestLockPreconditionTargetsLeaveOutAncestorsOfOtherPreconditions(t *testing.T) {
	var preconditions []LockPrecondition
	for _, resourcePath := range []string{
		"/dav/calendars/1/b",
		"/dav/calendars",
		"/dav/calendars/1/",
		"/dav/calendars/1/a",
		"/dav/calendars/10",
		"/dav/calendars/1x",
		"/dav/addressbooks/2",
	} {
		preconditions = append(preconditions, LockPrecondition{ResourcePath: resourcePath})
	}
	want := []string{"/dav/calendars/1/b", "/dav/calendars/1/a", "/dav/calendars/10", "/dav/calendars/1x", "/dav/addressbooks/2"}
	if got := lockPreconditionTargets(preconditions); !slices.Equal(got, want) {
		t.Fatalf("lockPreconditionTargets() = %q, want %q", got, want)
	}
}
