package httpcond

import (
	"slices"
	"testing"
)

func TestCheck(t *testing.T) {
	for _, tc := range []struct {
		name        string
		ifMatch     string
		ifNoneMatch string
		etag        string
		exists      bool
		want        bool
	}{
		{name: "no headers on an existing resource", etag: "abc", exists: true, want: true},
		{name: "no headers on an absent resource", want: true},
		{name: "if-match star with existing resource", ifMatch: "*", etag: "abc", exists: true, want: true},
		{name: "if-match star without resource", ifMatch: "*", want: false},
		{name: "if-match tag", ifMatch: `"abc"`, etag: "abc", exists: true, want: true},
		{name: "if-match unquoted tag", ifMatch: `abc`, etag: "abc", exists: true, want: true},
		{name: "if-match other tag", ifMatch: `"zzz"`, etag: "abc", exists: true, want: false},
		{name: "if-match tag without resource", ifMatch: `"abc"`, want: false},
		{name: "if-match list with match", ifMatch: `"zzz", "abc"`, etag: "abc", exists: true, want: true},
		{name: "if-match list without match", ifMatch: `"zzz", "yyy"`, etag: "abc", exists: true, want: false},
		{name: "if-match weak validator", ifMatch: `W/"abc"`, etag: "abc", exists: true, want: false},
		{name: "if-match list with only weak match", ifMatch: `"zzz", W/"abc"`, etag: "abc", exists: true, want: false},
		{name: "if-match list with weak and strong match", ifMatch: `W/"abc", "abc"`, etag: "abc", exists: true, want: true},
		{name: "if-match padded", ifMatch: `  "abc"  `, etag: "abc", exists: true, want: true},
		{name: "if-none-match star with existing resource", ifNoneMatch: "*", etag: "abc", exists: true, want: false},
		{name: "if-none-match star without resource", ifNoneMatch: "*", want: true},
		{name: "if-none-match tag", ifNoneMatch: `"abc"`, etag: "abc", exists: true, want: false},
		{name: "if-none-match other tag", ifNoneMatch: `"zzz"`, etag: "abc", exists: true, want: true},
		{name: "if-none-match tag without resource", ifNoneMatch: `"abc"`, want: true},
		{name: "if-none-match weak validator with match", ifNoneMatch: `W/"abc"`, etag: "abc", exists: true, want: false},
		{name: "if-none-match list with match", ifNoneMatch: `"zzz", "abc"`, etag: "abc", exists: true, want: false},
		{name: "if-none-match list with weak match", ifNoneMatch: `"zzz", W/"abc"`, etag: "abc", exists: true, want: false},
		{name: "if-none-match list without match", ifNoneMatch: `"x", "y"`, etag: "abc", exists: true, want: true},
		{name: "both hold", ifMatch: `"abc"`, ifNoneMatch: `"zzz"`, etag: "abc", exists: true, want: true},
		{name: "if-match fails and if-none-match holds", ifMatch: `"zzz"`, ifNoneMatch: `"yyy"`, etag: "abc", exists: true, want: false},
		{name: "if-match holds and if-none-match fails", ifMatch: `"abc"`, ifNoneMatch: `"abc"`, etag: "abc", exists: true, want: false},
		{name: "if-none-match star fails whatever if-match says", ifMatch: "*", ifNoneMatch: "*", etag: "abc", exists: true, want: false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := Check(tc.ifMatch, tc.ifNoneMatch, tc.etag, tc.exists); got != tc.want {
				t.Fatalf("Check(If-Match=%q, If-None-Match=%q, etag=%q, exists=%t) = %t, want %t",
					tc.ifMatch, tc.ifNoneMatch, tc.etag, tc.exists, got, tc.want)
			}
		})
	}
}

func TestEntityTags(t *testing.T) {
	for _, tc := range []struct {
		value     string
		allowWeak bool
		want      []string
	}{
		{`"a"`, false, []string{"a"}},
		{`"a", "b"`, false, []string{"a", "b"}},
		{`"a",W/"b"`, false, []string{"a"}},
		{`"a",W/"b"`, true, []string{"a", "b"}},
		{`""`, true, nil},
		{`*`, true, []string{"*"}},
	} {
		if got := EntityTags(tc.value, tc.allowWeak); !slices.Equal(got, tc.want) {
			t.Errorf("EntityTags(%q, %t) = %q, want %q", tc.value, tc.allowWeak, got, tc.want)
		}
	}
}
