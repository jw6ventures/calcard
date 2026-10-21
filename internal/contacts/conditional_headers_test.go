package contacts

import (
	"context"
	"errors"
	"testing"
)

// A REST write evaluates If-Match and If-None-Match as RFC 9110 section 13.1
// does, for an update and a delete alike: "*" matches a contact that exists,
// a list matches when any of its tags does, If-Match compares strongly and
// If-None-Match weakly, and the two headers are both evaluated.
func TestRESTWritesEvaluateConditionalHeadersAsRFC9110(t *testing.T) {
	const card = "BEGIN:VCARD\r\nVERSION:3.0\r\nUID:c1\r\nFN:Edited\r\nEND:VCARD\r\n"
	for _, tc := range []struct {
		name        string
		ifMatch     string
		ifNoneMatch string
		held        bool
	}{
		{"If-Match star", "*", "", true},
		{"If-Match tag", `"e1"`, "", true},
		{"If-Match list holding the tag", `"zzz", "e1"`, "", true},
		{"If-Match list without the tag", `"zzz", "yyy"`, "", false},
		{"If-Match weak tag", `W/"e1"`, "", false},
		{"If-None-Match other tag", "", `"zzz"`, true},
		{"If-None-Match tag", "", `"e1"`, false},
		{"If-None-Match weak tag", "", `W/"e1"`, false},
		{"If-None-Match list holding the tag", "", `"zzz", "e1"`, false},
		{"If-None-Match star", "", "*", false},
		{"If-Match tag with If-None-Match tag", `"e1"`, `"e1"`, false},
		{"If-Match tag with If-None-Match other tag", `"e1"`, `"zzz"`, true},
	} {
		t.Run("update/"+tc.name, func(t *testing.T) {
			svc, _ := newTestService()
			_, _, err := svc.UpdateContact(context.Background(), owner, 1, "c1", UpsertInput{RawVCard: card, IfMatch: tc.ifMatch, IfNoneMatch: tc.ifNoneMatch})
			assertConditionalOutcome(t, err, tc.held)
		})
		t.Run("delete/"+tc.name, func(t *testing.T) {
			svc, _ := newTestService()
			assertConditionalOutcome(t, svc.DeleteContact(context.Background(), owner, 1, "c1", tc.ifMatch, tc.ifNoneMatch), tc.held)
		})
	}
}

func assertConditionalOutcome(t *testing.T, err error, held bool) {
	t.Helper()
	switch {
	case held && err != nil:
		t.Fatalf("error = %v, want the write to go ahead", err)
	case !held && !errors.Is(err, ErrPreconditionFailed):
		t.Fatalf("error = %v, want ErrPreconditionFailed", err)
	}
}
