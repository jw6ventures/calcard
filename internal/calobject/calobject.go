// Package calobject is the contract between the CalDAV validator and the other
// writers of calendar data, the REST API and the UI: what a validator accepts
// and how it refuses. It holds no validation itself, so those writers can
// depend on it without depending on the DAV handler that implements it.
package calobject

import (
	"errors"
	"strings"

	"github.com/jw6ventures/calcard/internal/store"
)

// Object is a calendar object resource the CalDAV validator accepted: the
// octets to store, the UID that identifies it, and the metadata the store
// indexes it by.
type Object struct {
	Body     string
	UID      string
	Metadata store.EventWriteMetadata
}

// Validator applies to raw the validation a DAV PUT of it into cal would, so
// calendar data written through the REST API or the UI meets the same bar as
// data a CalDAV client writes. A nil cal stands for a collection with the
// server's default supported components. A refusal is a *Error.
type Validator func(raw string, cal *store.CalendarAccess) (*Object, error)

// ErrUndefinedTimezone is the reason a calendar object is refused when it
// names a TZID that neither one of its VTIMEZONE components nor the host's time
// zone database defines. An *Error carrying it answers
// errors.Is(err, ErrUndefinedTimezone); its conditions are the ones PUT
// answers with.
var ErrUndefinedTimezone = errors.New("calendar object names a time zone it does not define")

// ErrSchedulingMessage is the reason a calendar object is refused when it
// carries METHOD, which makes it an iTIP scheduling message rather than a
// calendar object resource (RFC 4791 §4.1).
var ErrSchedulingMessage = errors.New("calendar object is a scheduling message")

// ErrMultipleUIDs is the reason a calendar object is refused when its
// components carry more than one UID, so it is more than the one resource a
// write stores (RFC 4791 §4.1).
var ErrMultipleUIDs = errors.New("calendar object carries more than one UID")

// Error is a calendar object the CalDAV validator refused: the HTTP status and
// the RFC 4791 §5.3.2.1 precondition elements a PUT of it would be answered
// with.
type Error struct {
	Status     int
	Conditions []string
	cause      error
}

// NewError returns the refusal a PUT would answer with status and conditions.
// cause, when not nil, is the specific reason for the refusal, such as
// ErrUndefinedTimezone.
func NewError(status int, conditions []string, cause error) *Error {
	return &Error{Status: status, Conditions: conditions, cause: cause}
}

func (e *Error) Error() string {
	if e.cause != nil {
		return "invalid calendar object: " + strings.Join(e.Conditions, ", ") + ": " + e.cause.Error()
	}
	return "invalid calendar object: " + strings.Join(e.Conditions, ", ")
}

// Unwrap returns the specific reason for the refusal, such as
// ErrUndefinedTimezone, or nil when the conditions are all there is to say.
func (e *Error) Unwrap() error {
	return e.cause
}
