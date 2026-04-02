package store

import "errors"

// ErrNotFound indicates a missing or unauthorized resource lookup.
var ErrNotFound = errors.New("record not found")

// ErrLockConflict indicates that a conflicting lock prevents the operation.
var ErrLockConflict = errors.New("lock conflict")

// ErrConflict indicates the requested change conflicts with an existing record.
var ErrConflict = errors.New("record conflict")
