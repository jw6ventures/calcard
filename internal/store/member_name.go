package store

import "strings"

// ValidMemberName reports whether name may be stored as the name of a member
// of a collection: one path segment, which path.Clean leaves as it is. The
// name "." or ".." would give the member a state path (MemberStatePath) and an
// href that a reader cleaning paths reads as its collection's or that
// collection's parent's, and a name holding a slash one beneath another
// member, so each would share another resource's ACL entries, locks and dead
// properties.
func ValidMemberName(name string) bool {
	return name != "" && name != "." && name != ".." && !strings.Contains(name, "/")
}
