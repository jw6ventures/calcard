package migrations

import "embed"

// Files contains SQL migrations embedded into the binary.
//
// The migrations are stored alongside this package using a flat naming
// convention (e.g., 001_init.sql) so callers can read them directly via the
// returned embed.FS.
//
//go:embed *.sql
var Files embed.FS
