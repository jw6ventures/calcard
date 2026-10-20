package ui

import (
	"context"
	"net/http"
	"strconv"
	"strings"

	"github.com/jw6ventures/calcard/internal/store"
)

// maxEventWriteAttempts bounds how often an event write is decided afresh when
// another writer changes the event, or the access that decided it, between the
// decision and the write.
const maxEventWriteAttempts = 3

// eventACLScope is the part of the ACL one decision of an event write reads:
// the calendar's, and the event's in it when resourceName is set. calendar is
// the calendar when the caller already has it.
type eventACLScope struct {
	calendarID   int64
	calendar     *store.Calendar
	resourceName string
}

// pinEventACL pins the ACL entries that decide user's access in each scope,
// for the store to check again under the write's locks: an ACL change landing
// between the decision and the write fails the write, and the caller decides
// again on the new ACL. Pin before deciding, so that the guard cannot hold
// entries newer than the decision's. A calendar the user owns adds nothing,
// since an owner's privileges do not come from the ACL; the guard is nil when
// no scope adds anything.
func (h *Handler) pinEventACL(ctx context.Context, user *store.User, scopes ...eventACLScope) (*store.ACLGuard, error) {
	if h.store.ACLEntries == nil || h.store.Calendars == nil {
		return nil, nil
	}
	var paths []string
	for _, scope := range scopes {
		cal := scope.calendar
		if cal == nil {
			var err error
			if cal, err = h.store.Calendars.GetByID(ctx, scope.calendarID); err != nil {
				return nil, err
			}
		}
		if cal == nil || cal.UserID == user.ID {
			continue
		}
		paths = append(paths, calendarACLResourcePath(cal.ID))
		if scope.resourceName != "" {
			paths = append(paths, calendarEventResourcePath(cal.ID, scope.resourceName))
		}
	}
	if len(paths) == 0 {
		return nil, nil
	}
	entries, err := h.store.ACLEntries.ListByResources(ctx, paths)
	if err != nil {
		return nil, err
	}
	return store.NewACLGuard(paths, entries), nil
}

// requestedDestination is the calendar an edit form moves the event to, or
// calendarID when it names none. ok is false when it names one that is not a
// calendar ID.
func requestedDestination(r *http.Request, calendarID int64) (destinationID int64, ok bool) {
	raw := strings.TrimSpace(r.FormValue("destination_calendar_id"))
	if raw == "" {
		return calendarID, true
	}
	destinationID, err := strconv.ParseInt(raw, 10, 64)
	return destinationID, err == nil && destinationID > 0
}

// requireOpenedETag lets a write of existing go on only when the form was
// built from the version of it that is stored. A form that names no version
// cannot show what it edited, and every page the server renders names one, so
// it comes from a stale copy of a page. Either is answered with its reason,
// and nothing is written.
func (h *Handler) requireOpenedETag(w http.ResponseWriter, r *http.Request, calendarID int64, existing *store.Event) bool {
	values, supplied := r.Form["etag"]
	var message string
	switch {
	case !supplied || len(values) == 0:
		message = eventPageOutOfDateMessage
	case strings.TrimSpace(values[0]) != existing.ETag:
		message = eventChangedElsewhereMessage
	default:
		return true
	}
	h.redirectToEventPage(w, r, calendarID, map[string]string{"error": message})
	return false
}

// storeEventEdit replaces existing, as it was read, with event in its calendar.
// The store compares the stored ETag with existing's under the write's lock,
// so an edit built from a version that another client has since replaced is
// refused with store.ErrPreconditionFailed rather than overwriting it, and one
// of an event another client has since removed with
// store.ErrResourceStateChanged rather than bringing it back.
func (h *Handler) storeEventEdit(ctx context.Context, existing *store.Event, event store.Event, guard *store.ACLGuard) error {
	resourcePath := calendarEventResourcePath(event.CalendarID, event.ResourceName)
	_, err := h.store.PutCalendarObject(ctx, store.CalendarObjectWrite{
		CalendarID:    event.CalendarID,
		UID:           event.UID,
		ResourceName:  event.ResourceName,
		RawICAL:       event.RawICAL,
		ETag:          event.ETag,
		Metadata:      event.WriteMetadata,
		Precondition:  store.CalendarObjectPrecondition{IfMatch: &store.ETagCondition{ETags: []string{existing.ETag}}},
		ExpectedState: &store.CalendarObjectResourceState{Exists: true},
		ExpectedACL:   guard,
		// A write over an existing member is held by a lock on the member or by
		// a depth-infinity lock above it, not by a depth-0 lock on its calendar,
		// which covers membership (RFC 4918 section 7.5).
		LockPreconditions: []store.LockPrecondition{store.NewLockPrecondition(resourcePath)},
	})
	return err
}

// storeNewEvent adds event to its calendar. The store checks, in the write,
// that nothing holds the name or the UID, that the ACL guard pins is unchanged,
// and that no lock on the new name or the calendar refuses it.
func (h *Handler) storeNewEvent(ctx context.Context, event store.Event, guard *store.ACLGuard) error {
	_, err := h.store.PutCalendarObject(ctx, store.CalendarObjectWrite{
		CalendarID:        event.CalendarID,
		UID:               event.UID,
		ResourceName:      event.ResourceName,
		RawICAL:           event.RawICAL,
		ETag:              event.ETag,
		Metadata:          event.WriteMetadata,
		ExpectedState:     &store.CalendarObjectResourceState{Exists: false},
		ExpectedACL:       guard,
		LockPreconditions: memberLockPreconditions(event.CalendarID, event.ResourceName),
	})
	return err
}

// memberLockPreconditions are the lock conditions a UI write that submits no
// token meets to add or remove the member resourceName of calendarID: a lock
// on the member or on its calendar, at any depth, refuses it.
func memberLockPreconditions(calendarID int64, resourceName string) []store.LockPrecondition {
	return []store.LockPrecondition{
		store.NewLockPrecondition(calendarEventResourcePath(calendarID, resourceName)),
		store.NewLockPrecondition(calendarACLResourcePath(calendarID)),
	}
}

// moveLockPreconditions are the conditions a move of resourceName from
// fromCalendarID to toCalendarID meets: those of removing it from the one and
// adding it to the other.
func moveLockPreconditions(fromCalendarID, toCalendarID int64, resourceName string) []store.LockPrecondition {
	return append(memberLockPreconditions(fromCalendarID, resourceName), memberLockPreconditions(toCalendarID, resourceName)...)
}

// expectedEventState is the state of existing, with the ACL entries guard
// pins, that a write which replaces or removes it is decided on.
func expectedEventState(existing *store.Event, guard *store.ACLGuard) store.DAVResourceState {
	expected := store.EventDAVResourceState(existing)
	expected.ACL = guard
	return expected
}
