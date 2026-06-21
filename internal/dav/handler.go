package dav

import (
	"errors"
)

var errInvalidSyncToken = errors.New("invalid sync token")
var errInvalidPath = errors.New("invalid path")
var errAmbiguousCalendar = errors.New("ambiguous calendar path")
var errAmbiguousAddressBook = errors.New("ambiguous address book path")
var errForbidden = errors.New("forbidden")

const maxDAVBodyBytes int64 = 10 * 1024 * 1024

// birthdayCalendarID is a special virtual calendar ID for birthdays from contacts.
const birthdayCalendarID int64 = -1
