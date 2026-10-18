package acl

// Holding is what a principal holds on a resource, as a removal of a member
// reads it: some privilege granted, none granted and some refused, or none
// decided either way.
type Holding int

const (
	HoldsNothing Holding = iota
	HoldsDenied
	HoldsSome
)

// Removal is how a request that removes a member from its collection, a
// DELETE or the source side of a move, is answered.
type Removal int

const (
	// RemovalAllowed lets the removal go on.
	RemovalAllowed Removal = iota
	// RemovalRefused refuses it for want of DAV:unbind on the collection, to a
	// principal the answer may tell that the member is there.
	RemovalRefused
	// RemovalDenied refuses it because the collection's ACL refuses the
	// principal every privilege that would let it remove anything.
	RemovalDenied
	// RemovalAbsent answers as for a member that is not there.
	RemovalAbsent
)

// DecideRemoval decides the removal of a member from its collection. RFC 3744
// section 3.10 and Appendix B ask only for DAV:unbind on the collection.
// CalCard also asks for some privilege on the member, so that a removal never
// tells a principal that may know nothing of a member that it exists: a
// principal holding a privilege on the member is answered by whether it holds
// unbind, and any other as below. onMember is what the
// principal holds on the member, the member's own ACL deciding ahead of the
// collection's; onCollection is what it holds on the collection alone; and
// unbind whether it holds DAV:unbind there. exists, whether the member is
// there, is read only when onMember is HoldsDenied, so a caller need not look
// the member up for any other principal.
//
// A principal holding nothing on the member and refused nothing there knows
// no more of it than of a member that is not there, and is answered so. One
// the member's own ACL refuses everything cannot read it either, and would
// learn from a refusal that ACL caused that the member exists, so it is
// answered as a request for a missing member is: refused when the collection
// refuses it, and as absent otherwise.
func DecideRemoval(onMember, onCollection Holding, unbind, exists bool) Removal {
	switch {
	case onMember == HoldsSome && unbind:
		return RemovalAllowed
	case onMember == HoldsSome:
		return RemovalRefused
	case onMember == HoldsNothing:
		return RemovalAbsent
	case !exists:
		return RemovalDenied
	}
	switch {
	case onCollection == HoldsSome && unbind:
		return RemovalAbsent
	case onCollection == HoldsSome:
		return RemovalRefused
	case onCollection == HoldsDenied:
		return RemovalDenied
	default:
		return RemovalAbsent
	}
}
