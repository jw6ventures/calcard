package store

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"io"
	"net"
)

// ErrNotFound indicates a missing or unauthorized resource lookup.
var ErrNotFound = errors.New("record not found")

// ErrLockConflict indicates that a conflicting lock prevents the operation.
var ErrLockConflict = errors.New("lock conflict")

// ErrConflict indicates the requested change conflicts with an existing record.
var ErrConflict = errors.New("record conflict")

// isConnError reports whether err indicates a database connectivity problem
// (the server is unreachable, the connection was dropped, or the pool is
// closed) as opposed to an ordinary query-level failure such as a constraint
// violation. It deliberately ignores context cancellation/deadline errors,
// which describe the caller giving up rather than the database being down.
func isConnError(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return false
	}
	if errors.Is(err, driver.ErrBadConn) || errors.Is(err, sql.ErrConnDone) || errors.Is(err, io.EOF) {
		return true
	}
	var netErr net.Error
	return errors.As(err, &netErr)
}
