package dav

import (
	"context"
	"encoding/xml"
	"errors"
	"fmt"
	"net/http"
	"path"
	"strconv"
	"strings"

	"github.com/jw6ventures/calcard/internal/acl"
	"github.com/jw6ventures/calcard/internal/ical"
	"github.com/jw6ventures/calcard/internal/store"
)

func (h *DavServer) copyCalendarCollection(w http.ResponseWriter, r *http.Request, user *store.User, sourceSegment, sourcePath, destinationPath, depth string, overwrite bool) {
	h.transferCalendarCollection(w, r, user, sourceSegment, sourcePath, destinationPath, depth, overwrite, store.CalendarCollectionCopy)
}

func (h *DavServer) moveCalendarCollection(w http.ResponseWriter, r *http.Request, user *store.User, sourceSegment, sourcePath, destinationPath string, overwrite bool) {
	h.transferCalendarCollection(w, r, user, sourceSegment, sourcePath, destinationPath, "infinity", overwrite, store.CalendarCollectionMove)
}

func (h *DavServer) transferCalendarCollection(w http.ResponseWriter, r *http.Request, user *store.User, sourceSegment, sourcePath, destinationPath, depth string, overwrite bool, operation store.CalendarCollectionTransferOperation) {
	h.transferCalendarCollectionWithRetry(w, r, user, sourceSegment, sourcePath, destinationPath, depth, overwrite, operation, maxResourceStateRetries)
}

func (h *DavServer) transferCalendarCollectionWithRetry(w http.ResponseWriter, r *http.Request, user *store.User, sourceSegment, sourcePath, destinationPath, depth string, overwrite bool, operation store.CalendarCollectionTransferOperation, retries int) {
	if cleanDAVPath(sourcePath) == cleanDAVPath(destinationPath) {
		writeDAVError(w, http.StatusForbidden, "cannot-copy-move-onto-itself")
		return
	}
	aclGuard, err := h.aclGuard(r.Context(), user, sourcePath, destinationPath)
	if err != nil {
		writeServerFailure(w, err, "failed to evaluate ACL")
		return
	}
	destinationSlug, status, err := h.calendarCollectionTransferDestination(r, user, destinationPath)
	if err != nil {
		writeServerFailure(w, err, "failed to resolve destination")
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
			writeServerFailure(w, err, "failed to resolve source calendar")
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
		writeServerFailure(w, err, "failed to resolve destination calendar")
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
	members, failures, preflight, err := h.preflightCalendarCollectionMembers(r, user, source, depth, operation)
	if err != nil {
		writeServerFailure(w, err, "failed to load source calendar members")
		return
	}
	if len(failures) != 0 {
		writeCalendarCollectionMemberFailures(w, failures)
		return
	}
	lockPaths := []string{destinationPath, path.Dir(destinationPath)}
	var replacedRoots []string
	if destination != nil {
		destinationMembers, err := h.store.Events.ListForCalendar(r.Context(), destination.ID)
		if err != nil {
			writeServerFailure(w, err, "failed to load destination calendar members")
			return
		}
		for i := range destinationMembers {
			lockPaths = append(lockPaths, strings.TrimSuffix(destinationPath, "/")+"/"+eventResourceName(destinationMembers[i])+".ics")
		}
		replacedRoots = append(replacedRoots, calendarStatePath(destination.ID))
	}
	if operation == store.CalendarCollectionMove {
		lockPaths = append(lockPaths, sourcePath, path.Dir(sourcePath))
		for i := range members {
			lockPaths = append(lockPaths, strings.TrimSuffix(sourcePath, "/")+"/"+members[i].ResourceName+".ics")
		}
		replacedRoots = append(replacedRoots, calendarStatePath(source.ID))
	}
	for _, root := range replacedRoots {
		lockedBelow, err := h.lockedPathsBelow(r.Context(), root)
		if err != nil {
			writeLockStateError(w, err)
			return
		}
		lockPaths = append(lockPaths, lockedBelow...)
	}
	if !h.requireLocks(w, r, "resource is locked", lockPaths...) {
		return
	}

	lockPreconditions, ok := h.writeLockPreconditions(w, r, lockPaths...)
	if !ok {
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
			writeServerFailure(w, err, "failed to resolve destination lock path")
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
		LockPreconditions:    lockPreconditions,
		ExpectedACL:          aclGuard,
		RevalidateMembers:    preflight.revalidate,
	}
	defer invalidateDAVRequestState(r.Context())
	result, err := h.store.TransferCalendarCollection(r.Context(), transfer)
	var memberFailures calendarCollectionMemberFailures
	if errors.As(err, &memberFailures) {
		writeCalendarCollectionMemberFailures(w, memberFailures)
		return
	}
	if errors.Is(err, store.ErrResourceStateChanged) && retries > 0 {
		// The collection, its members or the ACL the request was authorized
		// against changed; the next attempt re-reads all of them.
		invalidateDAVRequestState(r.Context())
		h.transferCalendarCollectionWithRetry(w, r, user, sourceSegment, sourcePath, destinationPath, depth, overwrite, operation, retries-1)
		return
	}
	if err != nil {
		switch {
		case errors.Is(err, store.ErrPreconditionFailed), errors.Is(err, store.ErrResourceStateChanged):
			http.Error(w, "collection changed while transferring", http.StatusPreconditionFailed)
		case errors.Is(err, store.ErrLockConflict):
			http.Error(w, "resource is locked", http.StatusLocked)
		case errors.Is(err, store.ErrConflict):
			http.Error(w, "destination conflict", http.StatusConflict)
		case errors.Is(err, store.ErrNotFound):
			http.Error(w, "source calendar not found", http.StatusNotFound)
		default:
			h.logger().Error("CalendarCollectionTransfer", "%s calendar %d failed: %v", operation, source.ID, err)
			writeServerFailure(w, err, "failed to transfer calendar collection")
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

// calendarCollectionMemberFailures is the error a revalidation that finds
// members the destination cannot store fails the transfer with.
type calendarCollectionMemberFailures []calendarCollectionMemberFailure

func (f calendarCollectionMemberFailures) Error() string {
	return fmt.Sprintf("%d calendar collection members cannot be transferred", len(f))
}

// calendarCollectionPreflight checks source's members for a transfer of them
// as preflightCalendarCollectionMembers checked the ones it read.
type calendarCollectionPreflight struct {
	user      *store.User
	source    *store.CalendarAccess
	operation store.CalendarCollectionTransferOperation
	// collectionACL is the source's own ACL as the preflight read it, which
	// the transfer's ACL guard holds in place.
	collectionACL []store.ACLEntry
}

func (h *DavServer) preflightCalendarCollectionMembers(r *http.Request, user *store.User, source *store.CalendarAccess, depth string, operation store.CalendarCollectionTransferOperation) ([]store.CalendarCollectionMember, []calendarCollectionMemberFailure, *calendarCollectionPreflight, error) {
	preflight := &calendarCollectionPreflight{user: user, source: source, operation: operation}
	if depth == "0" && operation == store.CalendarCollectionCopy {
		return nil, nil, preflight, nil
	}
	events, err := h.store.Events.ListForCalendar(r.Context(), source.ID)
	if err != nil {
		return nil, nil, nil, err
	}
	var entries map[string][]store.ACLEntry
	if operation == store.CalendarCollectionCopy {
		if entries, err = h.prefetchCalendarACLEntries(r.Context(), user, source.ID, events); err != nil {
			return nil, nil, nil, err
		}
		preflight.collectionACL = entries[calendarCollectionResourcePath(source.ID)]
	}
	members, failures, err := preflight.check(r.Context(), events, entries)
	if err != nil {
		return nil, nil, nil, err
	}
	return members, failures, preflight, nil
}

// revalidate is the transfer's RevalidateMembers: it checks the members that
// changed after the preflight read them, under the transfer's locks, against
// the ACL entries the transaction read at their paths.
func (p *calendarCollectionPreflight) revalidate(ctx context.Context, changed []store.Event, memberACL []store.ACLEntry) ([]store.CalendarCollectionMember, error) {
	var entries map[string][]store.ACLEntry
	if p.operation == store.CalendarCollectionCopy {
		entries = map[string][]store.ACLEntry{calendarCollectionResourcePath(p.source.ID): p.collectionACL}
		for _, entry := range memberACL {
			entries[entry.ResourcePath] = append(entries[entry.ResourcePath], entry)
		}
		for resourcePath := range entries {
			acl.SortEntries(entries[resourcePath])
		}
	}
	members, failures, err := p.check(ctx, changed, entries)
	if err != nil {
		return nil, err
	}
	if len(failures) != 0 {
		return nil, calendarCollectionMemberFailures(failures)
	}
	return members, nil
}

// check validates events as members of the transfer, deciding a COPY's read
// privilege on each from entries, the ACL stored at the source and at each
// event's path.
func (p *calendarCollectionPreflight) check(ctx context.Context, events []store.Event, entries map[string][]store.ACLEntry) ([]store.CalendarCollectionMember, []calendarCollectionMemberFailure, error) {
	source := p.source
	// One pool serves every member this check reads, so the recurrence and
	// observance work of the whole collection is bounded, not each member's
	// alone.
	pool := ical.NewRequestWork(ctx, transferWorkBudget(len(events)))
	var readDecider *batchedObjectACLDecider
	if p.operation == store.CalendarCollectionCopy {
		readDecider = newBatchedObjectACLDecider(p.user, source.UserID, calendarCollectionResourcePath(source.ID), entries)
	}
	members := make([]store.CalendarCollectionMember, 0, len(events))
	var failures []calendarCollectionMemberFailure
	for i := range events {
		if err := ctx.Err(); err != nil {
			return nil, nil, err
		}
		event := &events[i]
		if p.operation == store.CalendarCollectionCopy {
			allowed, denied := calendarPrivilegeDecisionWithDecider(source, eventResourceName(*event), "read", readDecider)
			if !allowed && !denied {
				allowed = source.EffectivePrivileges().Allows("read")
			}
			if !allowed {
				failures = append(failures, calendarCollectionMemberFailure{
					href: calendarObjectConflictHref(source.ID, event), fault: &calendarObjectFault{status: http.StatusNotFound},
				})
				continue
			}
		}
		validated, fault := validateTransferredCalendarObject(event.RawICAL, source, pool)
		if fault != nil && pool.Exhausted() {
			// A member refused once the transfer's work ran out was not
			// shown invalid; it is one this transfer cannot afford to
			// validate, which max-instances names.
			fault = &calendarObjectFault{status: http.StatusForbidden, conditions: []string{"max-instances"}}
		}
		if fault != nil {
			failures = append(failures, calendarCollectionMemberFailure{href: calendarObjectConflictHref(source.ID, event), fault: fault})
			continue
		}
		// A COPY is a new resource with an ETag of its own; a MOVE keeps the
		// member's.
		newETag := ""
		if p.operation == store.CalendarCollectionCopy {
			newETag = newCopyETag(event.RawICAL, source.ID)
		}
		members = append(members, store.CalendarCollectionMember{
			ID: event.ID, UID: event.UID, ResourceName: eventResourceName(*event),
			RawICAL: event.RawICAL, ETag: event.ETag, NewETag: newETag, Metadata: &validated.Analysis.Metadata,
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
	status := serverFailureStatus(err)
	if errors.Is(err, store.ErrNotFound) {
		status = http.StatusNotFound
	} else if errors.Is(err, errForbidden) {
		status = http.StatusForbidden
	}
	writeStatusText(w, status)
}
