package store

import "errors"

// ErrNotFound indicates a missing or unauthorized resource lookup.
var ErrNotFound = errors.New("record not found")
