package acl

import "testing"

// Every combination a removal can meet, with the answer RFC 3744 section 3.10
// and the existence-concealment rules give it. A dimension a row leaves out
// does not change its answer.
func TestDecideRemoval(t *testing.T) {
	holdings := []Holding{HoldsNothing, HoldsDenied, HoldsSome}
	tests := []struct {
		name         string
		onMember     Holding
		onCollection []Holding
		unbind       []bool
		exists       []bool
		want         Removal
	}{
		{
			name:     "some privilege on the member and unbind on the collection",
			onMember: HoldsSome, onCollection: holdings, unbind: []bool{true}, exists: []bool{true, false},
			want: RemovalAllowed,
		},
		{
			name:     "some privilege on the member and no unbind",
			onMember: HoldsSome, onCollection: holdings, unbind: []bool{false}, exists: []bool{true, false},
			want: RemovalRefused,
		},
		{
			name:     "nothing decided on the member",
			onMember: HoldsNothing, onCollection: holdings, unbind: []bool{true, false}, exists: []bool{true, false},
			want: RemovalAbsent,
		},
		{
			name:     "refused on a member that is not there",
			onMember: HoldsDenied, onCollection: holdings, unbind: []bool{true, false}, exists: []bool{false},
			want: RemovalDenied,
		},
		{
			name:     "refused by the member's ACL, unbind on the collection",
			onMember: HoldsDenied, onCollection: []Holding{HoldsSome}, unbind: []bool{true}, exists: []bool{true},
			want: RemovalAbsent,
		},
		{
			name:     "refused by the member's ACL, no unbind on a collection it may see",
			onMember: HoldsDenied, onCollection: []Holding{HoldsSome}, unbind: []bool{false}, exists: []bool{true},
			want: RemovalRefused,
		},
		{
			name:     "refused by the member's ACL and by the collection's",
			onMember: HoldsDenied, onCollection: []Holding{HoldsDenied}, unbind: []bool{true, false}, exists: []bool{true},
			want: RemovalDenied,
		},
		{
			name:     "refused by the member's ACL, nothing decided on the collection",
			onMember: HoldsDenied, onCollection: []Holding{HoldsNothing}, unbind: []bool{true, false}, exists: []bool{true},
			want: RemovalAbsent,
		},
	}
	covered := 0
	for _, tc := range tests {
		for _, onCollection := range tc.onCollection {
			for _, unbind := range tc.unbind {
				for _, exists := range tc.exists {
					covered++
					if got := DecideRemoval(tc.onMember, onCollection, unbind, exists); got != tc.want {
						t.Errorf("%s (collection %d, unbind %t, exists %t): DecideRemoval() = %d, want %d", tc.name, onCollection, unbind, exists, got, tc.want)
					}
				}
			}
		}
	}
	if want := len(holdings) * len(holdings) * 2 * 2; covered != want {
		t.Errorf("the table covers %d combinations, want all %d", covered, want)
	}
}
