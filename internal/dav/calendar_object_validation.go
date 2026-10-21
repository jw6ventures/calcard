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
	return validateCalendarObjectReadAs(raw, cal, readSubmitted, nil)
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
//
// pool is the budget the object's recurrence and observance work draws on
// when it is one of many validated for one request, as a collection transfer's
// members are; nil gives the object a pool of its own.
func validateTransferredCalendarObject(raw string, cal *store.CalendarAccess, pool *ical.RequestWork) (*validatedCalendarObject, *calendarObjectFault) {
	return validateCalendarObjectReadAs(raw, cal, readTransferred, pool)
}

// validateCalendarObjectReadAs validates raw as reading reads it. Both of its
// readings of the object, the parsed tree's and the ObjectTimes' that the
// analysis and the recurrence check go through, draw on pool, so what one
// object may cost is bounded as a whole and not once per reading.
//
// A reading that pool ran out on while it followed a zone the object defines
// read that zone as UTC, so what either reading answered may be wrong, the
// metadata it produced included. An object whose pool ran out with either
// reading starved is therefore refused with max-instances, as one whose
// recurrence exceeds the work a validation may spend is, whatever else the
// validation found. A reading that stopped at its own limit with the pool
// still funded is not: it stops there on any pool, so its answer does not
// depend on this one.
func validateCalendarObjectReadAs(raw string, cal *store.CalendarAccess, reading icalReading, pool *ical.RequestWork) (*validatedCalendarObject, *calendarObjectFault) {
	if int64(len(raw)) > maxDAVBodyBytes {
		return nil, &calendarObjectFault{status: http.StatusForbidden, conditions: []string{"max-resource-size"}}
	}
	stored := raw
	raw = ical.TrimByteOrderMark(raw)

	if pool == nil {
		pool = ical.NewRequestWork(nil, ical.ObjectWorkBudget)
	}
	lines := ical.UnfoldLines(raw)
	root, err := parseICalendarTree(lines, reading)
	if err != nil {
		return nil, invalidCalendarData()
	}
	root.drawOn(pool)
	times := ical.NewObjectTimes(lines, sharedTimezoneOnsets, pool)
	validated, fault := validateReadCalendarObject(raw, stored, lines, root, times, cal, reading)
	if pool.Exhausted() && (times.Starved() || root.submittedZones().work.Starved()) {
		return nil, &calendarObjectFault{status: http.StatusForbidden, conditions: []string{"max-instances"}}
	}
	return validated, fault
}

// validateReadCalendarObject is the validation of a parsed calendar object,
// raw less its byte order mark and stored as the store holds it, through the
// tree root and the dates times resolves. It does not know whether either
// reading was starved of the pool; validateCalendarObjectReadAs does.
func validateReadCalendarObject(raw, stored string, lines []string, root *icalNode, times *ical.ObjectTimes, cal *store.CalendarAccess, reading icalReading) (*validatedCalendarObject, *calendarObjectFault) {
	if fault := validateCalendarObject(root); fault != nil {
		return nil, fault
	}
	if reading == readSubmitted {
		if fault := validateSubmittedCalendarProperties(root); fault != nil {
			return nil, fault
		}
	}
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
