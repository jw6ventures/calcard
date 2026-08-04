package dav

import (
	"encoding/xml"
	"errors"
	"fmt"
	"net/http"
	"path"
	"strconv"
	"strings"

	"github.com/jw6ventures/calcard/internal/store"
)

func (h *DavServer) copyCalendarCollection(w http.ResponseWriter, r *http.Request, user *store.User, sourceSegment, sourcePath, destinationPath, depth string, overwrite bool) {
	h.transferCalendarCollection(w, r, user, sourceSegment, sourcePath, destinationPath, depth, overwrite, store.CalendarCollectionCopy)
}

func (h *DavServer) moveCalendarCollection(w http.ResponseWriter, r *http.Request, user *store.User, sourceSegment, sourcePath, destinationPath string, overwrite bool) {
	h.transferCalendarCollection(w, r, user, sourceSegment, sourcePath, destinationPath, "infinity", overwrite, store.CalendarCollectionMove)
}

func (h *DavServer) transferCalendarCollection(w http.ResponseWriter, r *http.Request, user *store.User, sourceSegment, sourcePath, destinationPath, depth string, overwrite bool, operation store.CalendarCollectionTransferOperation) {
	if normalizeDAVHref(sourcePath) == normalizeDAVHref(destinationPath) {
		writeDAVError(w, http.StatusForbidden, "cannot-copy-move-onto-itself")
		return
	}
	destinationSlug, status, err := h.calendarCollectionTransferDestination(r, user, destinationPath)
	if err != nil {
		http.Error(w, "failed to resolve destination", http.StatusInternalServerError)
		return
	}
	if status != 0 {
		writeCalDAVError(w, status, conditionCalendarLocationOK)
		return
	}

	sourceID, resolved, err := h.resolveCalendarID(r.Context(), user, sourceSegment)
	if err != nil || !resolved {
		if errors.Is(err, errAmbiguousCalendar) {
			http.Error(w, "ambiguous calendar path", http.StatusConflict)
			return
		}
		if err != nil && !errors.Is(err, store.ErrNotFound) {
			http.Error(w, "failed to resolve source calendar", http.StatusInternalServerError)
			return
		}
		http.Error(w, "source calendar not found", http.StatusNotFound)
		return
	}
	var source *store.CalendarAccess
	if operation == store.CalendarCollectionCopy {
		source, err = h.loadCalendarWithPrivilege(r.Context(), user, sourceID, sourcePath, "read")
		if err != nil {
			_ = writePrivilegeRequirementError(w, requirePrivatePrivilegeAt(err, sourcePath, "read"))
			return
		}
	} else {
		calendar, loadErr := h.getCalendar(r.Context(), sourceID)
		if loadErr != nil {
			writeSourceResolutionError(w, loadErr)
			return
		}
		source = &store.CalendarAccess{Calendar: *calendar}
	}
	if operation == store.CalendarCollectionMove {
		if source.UserID != user.ID {
			http.Error(w, "source calendar not found", http.StatusNotFound)
			return
		}
		if err := h.requireACLPrivilege(r.Context(), user, calendarPrefix, "unbind"); err != nil {
			_ = writePrivilegeRequirementError(w, requirePrivilegeAt(err, calendarPrefix, "unbind"))
			return
		}
	}

	destination, err := h.ownedCalendarAtDestination(r, user, destinationSlug)
	if err != nil {
		if errors.Is(err, errAmbiguousCalendar) {
			http.Error(w, "ambiguous destination calendar", http.StatusConflict)
			return
		}
		http.Error(w, "failed to resolve destination calendar", http.StatusInternalServerError)
		return
	}
	if destination != nil && destination.ID == source.ID {
		writeDAVError(w, http.StatusForbidden, "cannot-copy-move-onto-itself")
		return
	}
	if destination != nil && !overwrite {
		http.Error(w, "destination exists", http.StatusPreconditionFailed)
		return
	}
	if err := h.requireACLPrivilege(r.Context(), user, calendarPrefix, "bind"); err != nil {
		_ = writePrivilegeRequirementError(w, requirePrivilegeAt(err, calendarPrefix, "bind"))
		return
	}
	if destination != nil {
		if err := h.requireACLPrivilege(r.Context(), user, calendarPrefix, "unbind"); err != nil {
			_ = writePrivilegeRequirementError(w, requirePrivilegeAt(err, calendarPrefix, "unbind"))
			return
		}
	}
	members, failures, err := h.preflightCalendarCollectionMembers(r, user, source, depth, operation)
	if err != nil {
		http.Error(w, "failed to load source calendar members", http.StatusInternalServerError)
		return
	}
	if len(failures) != 0 {
		writeCalendarCollectionMemberFailures(w, failures)
		return
	}
	lockPaths := []string{destinationPath, path.Dir(destinationPath)}
	if destination != nil {
		destinationMembers, err := h.store.Events.ListForCalendar(r.Context(), destination.ID)
		if err != nil {
			http.Error(w, "failed to load destination calendar members", http.StatusInternalServerError)
			return
		}
		for i := range destinationMembers {
			lockPaths = append(lockPaths, strings.TrimSuffix(destinationPath, "/")+"/"+eventResourceName(destinationMembers[i]))
		}
	}
	if operation == store.CalendarCollectionMove {
		lockPaths = append(lockPaths, sourcePath, path.Dir(sourcePath))
		for i := range members {
			lockPaths = append(lockPaths, strings.TrimSuffix(sourcePath, "/")+"/"+members[i].ResourceName)
		}
	}
	if !h.requireLocks(w, r, "resource is locked", lockPaths...) {
		return
	}

	destinationState := store.CalendarCollectionState{}
	destinationStatePath := ""
	destinationLockPath := ""
	if destination != nil {
		destinationState = store.CalendarCollectionState{Exists: true, ID: destination.ID, CTag: destination.CTag}
		destinationStatePath = calendarStatePath(destination.ID)
		destinationLockPath = destinationStatePath
	} else {
		destinationLockPath, err = h.canonicalDAVPath(r.Context(), user, destinationPath)
		if err != nil {
			http.Error(w, "failed to resolve destination lock path", http.StatusInternalServerError)
			return
		}
	}
	transfer := store.CalendarCollectionTransfer{
		Operation:            operation,
		Depth:                depth,
		Overwrite:            overwrite,
		SourceID:             source.ID,
		ExpectedSourceCTag:   source.CTag,
		SourceStatePath:      calendarStatePath(source.ID),
		DestinationOwnerID:   user.ID,
		DestinationSlug:      destinationSlug,
		ExpectedDestination:  destinationState,
		DestinationStatePath: destinationStatePath,
		DestinationLockPath:  destinationLockPath,
		Members:              members,
		LockPreconditions:    h.lockPreconditions(r, lockPaths...),
	}
	defer invalidateDAVRequestState(r.Context())
	result, err := h.store.TransferCalendarCollection(r.Context(), transfer)
	if err != nil {
		switch {
		case errors.Is(err, store.ErrPreconditionFailed), errors.Is(err, store.ErrResourceStateChanged):
			http.Error(w, "collection changed while transferring", http.StatusPreconditionFailed)
		case errors.Is(err, store.ErrLockConflict):
			http.Error(w, "resource is locked", http.StatusLocked)
		case errors.Is(err, store.ErrConflict):
			http.Error(w, "destination conflict", http.StatusConflict)
		default:
			h.logger().Error("CalendarCollectionTransfer", "%s calendar %d failed: %v", operation, source.ID, err)
			http.Error(w, "failed to transfer calendar collection", http.StatusInternalServerError)
		}
		return
	}
	w.Header().Set("Cache-Control", "no-cache")
	if result != nil && result.Calendar != nil {
		w.Header().Set("Location", calendarStatePath(result.Calendar.ID)+"/")
	}
	if result != nil && !result.Created {
		w.WriteHeader(http.StatusNoContent)
	} else {
		w.WriteHeader(http.StatusCreated)
	}
}

func (h *DavServer) calendarCollectionTransferDestination(r *http.Request, user *store.User, destinationPath string) (string, int, error) {
	target := parsedDAVTarget(r.Context(), destinationPath)
	if !target.Valid || target.Domain != davPathCalendar || target.Resource || target.CollectionSegment == "" {
		return "", http.StatusForbidden, nil
	}
	if _, err := strconv.ParseInt(target.CollectionSegment, 10, 64); err == nil || !isValidCalendarSlug(strings.ToLower(target.CollectionSegment)) {
		return "", http.StatusForbidden, nil
	}
	name, status, err := h.mkcalendarLocation(r.Context(), user, destinationPath)
	if err != nil || status != 0 {
		return "", status, err
	}
	return strings.ToLower(name), 0, nil
}

func (h *DavServer) ownedCalendarAtDestination(r *http.Request, user *store.User, slug string) (*store.Calendar, error) {
	calendars, err := h.store.Calendars.ListByUser(r.Context(), user.ID)
	if err != nil {
		return nil, err
	}
	var match *store.Calendar
	for i := range calendars {
		calendar := &calendars[i]
		if calendar.Slug != nil && strings.EqualFold(*calendar.Slug, slug) || calendar.Slug == nil && strings.EqualFold(calendar.Name, slug) {
			if match != nil {
				return nil, errAmbiguousCalendar
			}
			copy := *calendar
			match = &copy
		}
	}
	return match, nil
}

type calendarCollectionMemberFailure struct {
	href  string
	fault *calendarObjectFault
}

func (h *DavServer) preflightCalendarCollectionMembers(r *http.Request, user *store.User, source *store.CalendarAccess, depth string, operation store.CalendarCollectionTransferOperation) ([]store.CalendarCollectionMember, []calendarCollectionMemberFailure, error) {
	if depth == "0" && operation == store.CalendarCollectionCopy {
		return nil, nil, nil
	}
	events, err := h.store.Events.ListForCalendar(r.Context(), source.ID)
	if err != nil {
		return nil, nil, err
	}
	members := make([]store.CalendarCollectionMember, 0, len(events))
	var failures []calendarCollectionMemberFailure
	for i := range events {
		event := &events[i]
		if operation == store.CalendarCollectionCopy {
			allowed, denied, err := h.calendarPrivilegeDecision(r.Context(), user, &source.Calendar, calendarObjectHref(source.ID, event), "read")
			if err != nil {
				return nil, nil, err
			}
			if !allowed && !denied {
				allowed = source.EffectivePrivileges().Allows("read")
			}
			if !allowed {
				failures = append(failures, calendarCollectionMemberFailure{
					href: calendarObjectHref(source.ID, event), fault: &calendarObjectFault{status: http.StatusNotFound},
				})
				continue
			}
		}
		validated, fault := validateCalendarObjectForStorage(event.RawICAL, source)
		if fault != nil {
			failures = append(failures, calendarCollectionMemberFailure{href: calendarObjectHref(source.ID, event), fault: fault})
			continue
		}
		members = append(members, store.CalendarCollectionMember{
			ID: event.ID, UID: event.UID, ResourceName: eventResourceName(*event), RawICAL: event.RawICAL,
			ETag: event.ETag, NewETag: newCopyETag(event.RawICAL, source.ID), Metadata: &validated.Analysis.Metadata,
		})
	}
	return members, failures, nil
}

// writeCalendarCollectionMemberFailures reports a preflight that found members
// the destination cannot store. RFC 4918 §9.8.5 makes a partial COPY or MOVE a
// 207 naming each failing member, and §1.3 of RFC 4791 puts the precondition
// element under a DAV:error -- here inside each member's own DAV:response,
// because the precondition is a fact about that member rather than the request.
// Nothing has been written when this is reached, so the listed failures are the
// whole outcome rather than a partial result to undo.
func writeCalendarCollectionMemberFailures(w http.ResponseWriter, failures []calendarCollectionMemberFailure) {
	w.Header().Set("Content-Type", "application/xml; charset=utf-8")
	w.WriteHeader(http.StatusMultiStatus)
	_, _ = fmt.Fprint(w, `<?xml version="1.0" encoding="utf-8"?><D:multistatus xmlns:D="DAV:" xmlns:C="urn:ietf:params:xml:ns:caldav">`)
	for _, failure := range failures {
		var href strings.Builder
		_ = xml.EscapeText(&href, []byte(failure.href))
		_, _ = fmt.Fprintf(w, `<D:response><D:href>%s</D:href><D:status>HTTP/1.1 %d %s</D:status><D:error>`, href.String(), failure.fault.status, http.StatusText(failure.fault.status))
		for _, condition := range failure.fault.conditions {
			_, _ = fmt.Fprintf(w, `<C:%s/>`, condition)
		}
		_, _ = fmt.Fprint(w, `</D:error></D:response>`)
	}
	_, _ = fmt.Fprint(w, `</D:multistatus>`)
}

func writeCalendarCollectionAccessError(w http.ResponseWriter, err error) {
	status := http.StatusInternalServerError
	if errors.Is(err, store.ErrNotFound) {
		status = http.StatusNotFound
	} else if errors.Is(err, errForbidden) {
		status = http.StatusForbidden
	}
	http.Error(w, http.StatusText(status), status)
}
