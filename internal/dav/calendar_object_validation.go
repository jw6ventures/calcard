package dav

import (
	"net/http"
	"time"

	"github.com/jw6ventures/calcard/internal/calobject"
	"github.com/jw6ventures/calcard/internal/ical"
	"github.com/jw6ventures/calcard/internal/store"
)

// validatedCalendarObject is a calendar object resource the validator accepted.
// Body is the octets to store: a transferred object's as they are stored, and a
// submitted object's less a leading byte order mark and completed as
// completeCalendarObject describes.
type validatedCalendarObject struct {
	Body     string
	UID      string
	Analysis calendarAnalysis
}

// validateCalendarObjectForStorage is the validator every write of submitted
// calendar data goes through: DAV PUT, and the REST API and the UI's event
// editor and import through ValidateCalendarObject. COPY and MOVE, which write
// an object the store already holds, go through
// validateTransferredCalendarObject.
func validateCalendarObjectForStorage(raw string, cal *store.CalendarAccess) (*validatedCalendarObject, *calendarObjectFault) {
	return validateCalendarObjectReadAs(raw, cal, readSubmitted)
}

// validateTransferredCalendarObject is validateCalendarObjectForStorage for an
// object the store holds that COPY or MOVE writes again. Its lines are read as
// readTransferred describes: objects stored without parameter checks may hold
// malformed ones, which every reader leaves out, so moving one changes nothing
// any reading of it says. Its octets are kept as they are stored, a leading
// byte order mark included: it was accepted as it is, and changing them would
// change the ETag of every member a collection MOVE carries. Nor is its UID
// held to store.MaxIdentifierOctets: the store already holds it, and a longer
// UID a 1.1.x release stored must still be copyable and movable.
func validateTransferredCalendarObject(raw string, cal *store.CalendarAccess) (*validatedCalendarObject, *calendarObjectFault) {
	return validateCalendarObjectReadAs(raw, cal, readTransferred)
}

func validateCalendarObjectReadAs(raw string, cal *store.CalendarAccess, reading icalReading) (*validatedCalendarObject, *calendarObjectFault) {
	if int64(len(raw)) > maxDAVBodyBytes {
		return nil, &calendarObjectFault{status: http.StatusForbidden, conditions: []string{"max-resource-size"}}
	}
	stored := raw
	raw = ical.TrimByteOrderMark(raw)

	lines := ical.UnfoldLines(raw)
	root, err := parseICalendarTree(lines, reading)
	if err != nil {
		return nil, invalidCalendarData()
	}
	if fault := validateCalendarObject(root); fault != nil {
		return nil, fault
	}
	if reading == readSubmitted {
		if fault := validateSubmittedCalendarProperties(root); fault != nil {
			return nil, fault
		}
	}
	times := ical.NewObjectTimesSharingOnsets(lines, sharedTimezoneOnsets)
	analysis, err := analyzeUnfoldedICalendar(raw, lines, times)
	if err != nil {
		return nil, invalidCalendarData()
	}
	if !calendarAcceptsComponents(cal, analysis.Components) {
		return nil, &calendarObjectFault{status: http.StatusForbidden, conditions: []string{"supported-calendar-component"}}
	}
	if analysis.MaxAttendees > ical.MaxAttendeesPerInstance {
		return nil, &calendarObjectFault{status: http.StatusForbidden, conditions: []string{"max-attendees-per-instance"}}
	}
	exceedsInstances, validRecurrence := ical.RecurrenceSetExceedsLimitOf(times, ical.MaxRecurrenceInstances)
	if !validRecurrence {
		return nil, invalidCalendarData()
	}
	if exceedsInstances {
		return nil, &calendarObjectFault{status: http.StatusForbidden, conditions: []string{"max-instances"}}
	}
	uid, err := analysis.uid()
	if err != nil {
		return nil, invalidCalendarObjectResource()
	}
	if reading == readTransferred {
		return &validatedCalendarObject{Body: stored, UID: uid, Analysis: analysis}, nil
	}
	if len(uid) > store.MaxIdentifierOctets {
		// The UID is a unique index key, whose entries PostgreSQL bounds in
		// size.
		fault := invalidCalendarObjectResource()
		fault.cause = calobject.ErrUIDTooLong
		return nil, fault
	}
	// The completion changes nothing the analysis above read -- a DATE DTEND
	// on its DTSTART is read as the day after, and the definitions speak for
	// the host's zones, through which it already read every TZID they define --
	// so the object is completed only once it is accepted.
	_, maxDate := ical.DateLimits()
	countedEnd := maxDate
	if until := analysis.Metadata.RecurrenceUntil; until != nil && until.Before(maxDate) {
		countedEnd = *until
	}
	body := completeCalendarObject(raw, lines, root, countedEnd, time.Now(), ical.VTimezone, maxDAVBodyBytes)
	return &validatedCalendarObject{Body: body, UID: uid, Analysis: analysis}, nil
}

func writeCalendarObjectFault(w http.ResponseWriter, fault *calendarObjectFault) {
	if fault == nil {
		return
	}
	writeCalDAVErrorMulti(w, fault.status, fault.conditions...)
}

var _ calobject.Validator = ValidateCalendarObject

// ValidateCalendarObject is the calobject.Validator the REST API and the UI
// write calendar data through: it applies to raw the validation a DAV PUT of it
// into cal would, the bound on the UID's length included. A nil cal stands for
// a collection with the server's default supported components. A refusal is a
// *calobject.Error.
func ValidateCalendarObject(raw string, cal *store.CalendarAccess) (*calobject.Object, error) {
	validated, fault := validateCalendarObjectForStorage(raw, cal)
	if fault != nil {
		return nil, calobject.NewError(fault.status, fault.conditions, fault.cause)
	}
	return &calobject.Object{Body: validated.Body, UID: validated.UID, Metadata: validated.Analysis.Metadata}, nil
}
