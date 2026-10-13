// Package calobject is the contract between the CalDAV validator and the other
// writers of calendar data, the REST API and the UI: what a validator accepts
// and how it refuses. It holds no validation itself, so those writers can
// depend on it without depending on the DAV handler that implements it.
package calobject

import (
	"errors"
	"strings"
	"time"

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

// Instance is one instance of a recurring event: the slot of the recurrence
// set it fills, which its RECURRENCE-ID names, and where it falls.
type Instance struct {
	RecurrenceID time.Time
	Start        time.Time
	End          time.Time
	// Override is the RECURRENCE-ID of the RANGE=THISANDFUTURE override whose
	// content the instance carries (RFC 5545 §3.8.4.4), or the zero time
	// when it carries the series' own.
	Override time.Time
}

// Expansion is what an Expander finds of a stored object's recurring event.
type Expansion struct {
	// Recurs is true when the object's VEVENT series has a recurrence set the
	// server can expand. Otherwise its master stands for itself, as a
	// CalDAV expansion returns it.
	Recurs bool
	// Instances are the instances in the range, by start.
	Instances []Instance
	// Overrides are the overrides in the range the expansion writes as
	// themselves -- each replacing one instance, and each RANGE=THISANDFUTURE
	// one naming a slot the series never generates -- by start, placed where
	// the expansion places them, whatever they write of their end. Their
	// Override is the zero time.
	Overrides []Instance
	// Complete is false when the range holds more instances than the
	// expansion lists, so Instances are only some of them.
	Complete bool
}

// Expander lists the instances of the recurring VEVENT of raw, a stored
// calendar object, that RFC 4791 §9.9 places in [start, end): those a CalDAV
// calendar-query with CALDAV:expand over that range returns for it (§9.6.5),
// less each an override replaces, which stands for itself and is placed among
// the expansion's Overrides. Floating times are read in UTC.
type Expander func(raw string, start, end time.Time) Expansion

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

// ErrUIDTooLong is the reason a calendar object is refused when its UID is
// longer than store.MaxIdentifierOctets, the bound the server keeps a UID to
// so the unique index entries it is stored under stay within PostgreSQL's
// limit.
var ErrUIDTooLong = errors.New("calendar object UID is too long")

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
