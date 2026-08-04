package dav

import (
	"net/http"

	"github.com/jw6ventures/calcard/internal/ical"
	"github.com/jw6ventures/calcard/internal/store"
)

type validatedCalendarObject struct {
	UID      string
	Analysis calendarAnalysis
}

func validateCalendarObjectForStorage(raw string, cal *store.CalendarAccess) (*validatedCalendarObject, *calendarObjectFault) {
	if int64(len(raw)) > maxDAVBodyBytes {
		return nil, &calendarObjectFault{status: http.StatusForbidden, conditions: []string{"max-resource-size"}}
	}

	root, err := parseICalendarObject(raw)
	if err != nil {
		return nil, invalidCalendarData()
	}
	if fault := validateCalendarObject(root); fault != nil {
		return nil, fault
	}
	analysis, err := analyzeICalendar(raw)
	if err != nil {
		return nil, invalidCalendarData()
	}
	if !calendarAcceptsComponents(cal, analysis.Components) {
		return nil, &calendarObjectFault{status: http.StatusForbidden, conditions: []string{"supported-calendar-component"}}
	}
	if analysis.MaxAttendees > caldavMaxAttendees {
		return nil, &calendarObjectFault{status: http.StatusForbidden, conditions: []string{"max-attendees-per-instance"}}
	}
	exceedsInstances, validRecurrence := ical.RecurrenceSetExceedsLimit(raw, caldavMaxInstances)
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
	return &validatedCalendarObject{UID: uid, Analysis: analysis}, nil
}

func writeCalendarObjectFault(w http.ResponseWriter, fault *calendarObjectFault) {
	if fault == nil {
		return
	}
	writeCalDAVErrorMulti(w, fault.status, fault.conditions...)
}
