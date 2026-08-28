package dav

import (
	"errors"
)

var errInvalidSyncToken = errors.New("invalid sync token")
var errUnsupportedReport = errors.New("unsupported report")
var errInvalidPath = errors.New("invalid path")
var errAmbiguousCalendar = errors.New("ambiguous calendar path")
var errAmbiguousAddressBook = errors.New("ambiguous address book path")
var errForbidden = errors.New("forbidden")

// errNumberOfMatchesExceeded carries the DAV:number-of-matches-within-limits
// postcondition of RFC 4791 §7.8 and §7.10 out of a report. It is raised both
// when the matches outnumber what the server will return and when finding them
// would read more stored resources than it will examine, because in neither
// case can the server report the complete match set §7.8 asks it for.
var errNumberOfMatchesExceeded = errors.New("number of matches outside server limits")

// errTooManyHrefs refuses a multiget whose DAV:href count is past what the
// server will answer. RFC 4791 §7.9 owes one DAV:response per href, so the
// report is refused whole rather than answered over a truncated href list.
var errTooManyHrefs = errors.New("multiget href count outside server limits")

// errTooManyCandidateRows refuses a report that would read more stored
// resources than the server will examine. It is the answer for the reports no
// specification gives a postcondition to, so it carries the RFC 4918 capacity
// status; calendar-query and free-busy-query raise
// errNumberOfMatchesExceeded instead, which RFC 4791 §7.8 and §7.10 name.
var errTooManyCandidateRows = errors.New("report candidate rows outside server limits")

const maxDAVBodyBytes int64 = 10 * 1024 * 1024

// birthdayCalendarID is a special virtual calendar ID for birthdays from contacts.
const birthdayCalendarID int64 = -1
