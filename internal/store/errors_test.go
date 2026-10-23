package store

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"net"
	"testing"

	"github.com/lib/pq"
)

func TestIsUnavailableSeparatesAnUnreachableDatabaseFromAFailedStatement(t *testing.T) {
	refused := &net.OpError{Op: "dial", Net: "tcp", Err: errors.New("connect: connection refused")}
	cases := []struct {
		name string
		err  error
		want bool
	}{
		{name: "connection refused", err: refused, want: true},
		{name: "wrapped connection refused", err: fmt.Errorf("get user: %w", refused), want: true},
		{name: "bad connection", err: driver.ErrBadConn, want: true},
		{name: "connection done", err: sql.ErrConnDone, want: true},
		{name: "too many clients", err: &pq.Error{Severity: "FATAL", Code: "53300"}, want: true},
		{name: "terminated by administrator", err: &pq.Error{Severity: "FATAL", Code: "57P01"}, want: true},
		{name: "crash shutdown", err: &pq.Error{Severity: "FATAL", Code: "57P02"}, want: true},
		{name: "starting up", err: &pq.Error{Severity: "FATAL", Code: "57P03"}, want: true},
		{name: "connection failure", err: &pq.Error{Severity: "FATAL", Code: "08006"}, want: true},
		{name: "unique violation", err: &pq.Error{Code: "23505"}, want: false},
		{name: "invalid byte sequence", err: &pq.Error{Code: "22021"}, want: false},
		{name: "statement canceled", err: &pq.Error{Code: "57014"}, want: false},
		{name: "out of memory", err: &pq.Error{Code: "53200"}, want: false},
		{name: "caller canceled", err: context.Canceled, want: false},
		{name: "not found", err: ErrNotFound, want: false},
		{name: "nil", err: nil, want: false},
	}
	for _, tc := range cases {
		if got := IsUnavailable(tc.err); got != tc.want {
			t.Errorf("%s: IsUnavailable(%v) = %v, want %v", tc.name, tc.err, got, tc.want)
		}
	}
}
