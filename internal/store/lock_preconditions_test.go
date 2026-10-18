package store

import (
	"fmt"
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
	if got := lockPreconditionTargets(preconditions, nil); !slices.Equal(got, want) {
		t.Fatalf("lockPreconditionTargets() = %q, want %q", got, want)
	}
}

// A precondition below a path the transaction holds exclusively in its own
// right is no target: a collection-wide write holding the collection takes no
// lock per member, while paths outside it, and the collection's ancestors,
// are still targets.
func TestLockPreconditionTargetsLeaveOutPathsBelowOnesHeldExclusively(t *testing.T) {
	var preconditions []LockPrecondition
	for i := range 1000 {
		preconditions = append(preconditions, LockPrecondition{ResourcePath: fmt.Sprintf("/dav/calendars/1/member-%d", i)})
	}
	preconditions = append(preconditions,
		LockPrecondition{ResourcePath: "/dav/calendars/1"},
		LockPrecondition{ResourcePath: "/dav/calendars/1/unmapped/below"},
		LockPrecondition{ResourcePath: "/dav/calendars/10/member"},
		LockPrecondition{ResourcePath: "/dav/calendars/.pending/1/moved"},
	)
	want := []string{"/dav/calendars/10/member", "/dav/calendars/.pending/1/moved"}
	if got := lockPreconditionTargets(preconditions, []string{"/dav/calendars/1"}); !slices.Equal(got, want) {
		t.Fatalf("lockPreconditionTargets() = %q, want %q", got, want)
	}
	if got := len(davPathLocks(append([]string{"/dav/calendars/1"}, want...)...)); got > 8 {
		t.Fatalf("a write to a collection of 1000 members takes %d path locks", got)
	}
}
