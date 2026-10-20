package dav

import (
	"context"
	"errors"
	"net/http"
	"path"
	"strings"

	"github.com/jw6ventures/calcard/internal/acl"
	"github.com/jw6ventures/calcard/internal/auth"
	"github.com/jw6ventures/calcard/internal/store"
)

func (h *DavServer) delete(w http.ResponseWriter, r *http.Request) {
	h.deleteWithRetry(w, r, maxResourceStateRetries)
}

// memberRemoval decides a DELETE of the member at memberPath in the collection
// at parentPath with acl.DecideRemoval, which needs DAV:unbind on the
// collection (RFC 3744 Appendix B) and, so as not to disclose the member to a
// principal that may know nothing of it, some privilege on the member. It
// answers it: a refusal to answer with, answerAsAbsent=true for the request
// to go on as for a missing member, to 404, or neither for the delete to go
// ahead.
// holding reports what the principal holds at the path it is given: the
// member's own ACL and its collection's at the member's path, the
// collection's alone at the collection's. Whether the member exists is looked
// up only for a principal its ACL refuses, the one case the decision reads
// it, so nothing about a member is read for a principal who may know nothing
// of it.
func memberRemoval(memberPath, parentPath string, exists func() (bool, error), holding func(decisionPath string) (acl.Holding, error), unbind func() (bool, error)) (refusal error, answerAsAbsent bool, err error) {
	onMember, err := holding(memberPath)
	if err != nil {
		return nil, false, err
	}
	onCollection, err := holding(parentPath)
	if err != nil {
		return nil, false, err
	}
	canUnbind, err := unbind()
	if err != nil {
		return nil, false, err
	}
	isThere := false
	if onMember == acl.HoldsDenied {
		if isThere, err = exists(); err != nil {
			return nil, false, err
		}
	}
	switch acl.DecideRemoval(onMember, onCollection, canUnbind, isThere) {
	case acl.RemovalAllowed:
		return nil, false, nil
	case acl.RemovalRefused:
		return requirePrivilegeAt(errPrivilegeNotGranted, parentPath, "unbind"), false, nil
	case acl.RemovalDenied:
		return requirePrivilegeAt(errForbidden, parentPath, "unbind"), false, nil
	default:
		return nil, true, nil
	}
}

// removalHolding turns the answer to whether a principal holds any privilege
// at a path into what acl.DecideRemoval reads: some held, every one refused,
// or nothing decided.
func removalHolding(err error) (acl.Holding, error) {
	switch {
	case err == nil:
		return acl.HoldsSome, nil
	case errors.Is(err, errForbidden):
		return acl.HoldsDenied, nil
	case errors.Is(err, store.ErrNotFound):
		return acl.HoldsNothing, nil
	default:
		return acl.HoldsNothing, err
	}
}

// privilegeHeld turns a privilege check's error into whether the privilege is
// held: a refusal or a missing object is not held, and any other error is
// returned.
func privilegeHeld(err error) (bool, error) {
	switch {
	case err == nil:
		return true, nil
	case isPrivilegeRefusal(err), errors.Is(err, store.ErrNotFound):
		return false, nil
	default:
		return false, err
	}
}

// calendarMemberRemoval is memberRemoval for a member of calendarID.
func (h *DavServer) calendarMemberRemoval(ctx context.Context, user *store.User, calendarID int64, memberPath string, exists func() (bool, error)) (refusal error, answerAsAbsent bool, err error) {
	parentPath := path.Dir(memberPath)
	var cal *store.Calendar
	holding := func(decisionPath string) (acl.Holding, error) {
		held, loaded, err := h.calendarRemovalHolding(ctx, user, calendarID, decisionPath)
		if cal == nil {
			cal = loaded
		}
		return held, err
	}
	unbind := func() (bool, error) {
		if cal == nil {
			loaded, err := h.getCalendar(ctx, calendarID)
			if errors.Is(err, store.ErrNotFound) {
				return false, nil
			}
			if err != nil {
				return false, err
			}
			cal = loaded
		}
		return privilegeHeld(h.requireCalendarPrivilege(ctx, user, cal, parentPath, "unbind"))
	}
	return memberRemoval(memberPath, parentPath, exists, holding, unbind)
}

// addressBookMemberRemoval is memberRemoval for a member of book.
func (h *DavServer) addressBookMemberRemoval(ctx context.Context, user *store.User, book *store.AddressBook, memberPath string, exists func() (bool, error)) (refusal error, answerAsAbsent bool, err error) {
	parentPath := path.Dir(memberPath)
	return memberRemoval(memberPath, parentPath, exists,
		func(decisionPath string) (acl.Holding, error) {
			return removalHolding(h.requireAnyAddressBookPrivilege(ctx, user, book, decisionPath))
		},
		func() (bool, error) {
			return privilegeHeld(h.requireAddressBookPrivilege(ctx, user, book, parentPath, "unbind"))
		})
}

func (h *DavServer) deleteWithRetry(w http.ResponseWriter, r *http.Request, retries int) {
	h.logger().Trace("Delete", "DELETE %s", r.URL.Path)
	user, ok := auth.UserFromContext(r.Context())
	if !ok {
		http.Error(w, "missing user", http.StatusUnauthorized)
		return
	}

	cleanPath := path.Clean(r.URL.Path)
	if target := parsedDAVTarget(r.Context(), cleanPath); target.Valid && target.Domain == davPathCalendar && !target.Resource && target.CollectionSegment != "" {
		h.deleteCalendarCollection(w, r, user, target.CollectionSegment, cleanPath)
		return
	}
	if calendarID, uid, matched, err := h.parseCalendarResourcePath(r.Context(), user, cleanPath); err != nil {
		if errors.Is(err, store.ErrNotFound) {
			http.Error(w, "not found", http.StatusNotFound)
			return
		}
		if errors.Is(err, errAmbiguousCalendar) {
			http.Error(w, "ambiguous calendar path", http.StatusConflict)
			return
		}
		http.Error(w, "failed to load calendar", http.StatusInternalServerError)
		return
	} else if matched {
		aclGuard, err := h.aclGuard(r.Context(), user, cleanPath)
		if err != nil {
			http.Error(w, "failed to evaluate ACL", http.StatusInternalServerError)
			return
		}
		refusal, answerAsAbsent, err := h.calendarMemberRemoval(r.Context(), user, calendarID, cleanPath, func() (bool, error) {
			event, err := h.store.Events.GetByResourceName(r.Context(), calendarID, uid)
			return event != nil, err
		})
		if err != nil {
			http.Error(w, "failed to evaluate ACL", http.StatusInternalServerError)
			return
		}
		if refusal != nil {
			_ = writePrivilegeRequirementError(w, refusal)
			return
		}
		var existing *store.Event
		if !answerAsAbsent {
			existing, err = h.store.Events.GetByResourceName(r.Context(), calendarID, uid)
			if err != nil {
				http.Error(w, "failed to load event", http.StatusInternalServerError)
				return
			}
		}
		// RFC 9110 §13.2.1: preconditions are not evaluated when the request
		// would otherwise fail, so a missing member answers 404 under If-Match.
		if existing == nil {
			http.Error(w, "not found", http.StatusNotFound)
			return
		}
		lockPaths := []string{cleanPath, path.Dir(cleanPath)}
		if !h.requireLocks(w, r, "resource is locked", lockPaths...) {
			return
		}
		if !h.checkConditionalHeaders(r, existing) {
			http.Error(w, "precondition failed", http.StatusPreconditionFailed)
			return
		}
		canonicalPath, err := h.canonicalDAVPath(r.Context(), user, cleanPath)
		if err != nil {
			http.Error(w, "failed to resolve resource state", http.StatusInternalServerError)
			return
		}
		lockPreconditions, ok := h.writeLockPreconditions(w, r, lockPaths...)
		if !ok {
			return
		}
		defer invalidateDAVRequestState(r.Context())
		expected := store.EventDAVResourceState(existing)
		expected.ACL = aclGuard
		if err := h.store.DeleteEventAndState(r.Context(), calendarID, expected, canonicalPath, lockPreconditions); err != nil {
			if errors.Is(err, store.ErrLockConflict) {
				http.Error(w, "resource is locked", http.StatusLocked)
				return
			}
			if errors.Is(err, store.ErrResourceStateChanged) {
				if retries > 0 {
					invalidateDAVRequestState(r.Context())
					h.deleteWithRetry(w, r, retries-1)
					return
				}
				http.Error(w, "resource changed while deleting; retry the request", http.StatusConflict)
				return
			}
			h.logger().Error("Delete", "failed to delete event %q from calendar %d: %v", existing.UID, calendarID, err)
			http.Error(w, "failed to delete", http.StatusInternalServerError)
			return
		}
		h.logger().Info("Delete", "deleted event %q from calendar %d", existing.UID, calendarID)
		w.WriteHeader(http.StatusNoContent)
		return
	}
	if addressBookID, resourceName, matched, err := h.parseAddressBookResourcePath(r.Context(), user, cleanPath); err != nil {
		if errors.Is(err, store.ErrNotFound) {
			http.Error(w, "not found", http.StatusNotFound)
			return
		}
		if errors.Is(err, errAmbiguousAddressBook) {
			http.Error(w, "ambiguous address book path", http.StatusConflict)
			return
		}
		http.Error(w, "failed to load address book", http.StatusInternalServerError)
		return
	} else if matched {
		aclGuard, err := h.aclGuard(r.Context(), user, cleanPath)
		if err != nil {
			http.Error(w, "failed to evaluate ACL", http.StatusInternalServerError)
			return
		}
		book, err := h.getAddressBook(r.Context(), addressBookID)
		if err != nil {
			status := http.StatusInternalServerError
			if errors.Is(err, store.ErrNotFound) {
				status = http.StatusNotFound
			}
			http.Error(w, http.StatusText(status), status)
			return
		}
		existing, err := h.store.Contacts.GetByResourceName(r.Context(), addressBookID, resourceName)
		if err != nil {
			http.Error(w, "failed to load contact", http.StatusInternalServerError)
			return
		}
		refusal, answerAsAbsent, err := h.addressBookMemberRemoval(r.Context(), user, book, cleanPath, func() (bool, error) { return existing != nil, nil })
		if err != nil {
			http.Error(w, "failed to evaluate ACL", http.StatusInternalServerError)
			return
		}
		if refusal != nil {
			_ = writePrivilegeRequirementError(w, refusal)
			return
		}
		if answerAsAbsent {
			existing = nil
		}
		if existing == nil {
			http.Error(w, "not found", http.StatusNotFound)
			return
		}
		lockPaths := []string{cleanPath, path.Dir(cleanPath)}
		if !h.requireLocks(w, r, "resource is locked", lockPaths...) {
			return
		}
		if !h.checkConditionalHeadersContact(r, existing) {
			http.Error(w, "precondition failed", http.StatusPreconditionFailed)
			return
		}
		canonicalPath, err := h.canonicalDAVPath(r.Context(), user, cleanPath)
		if err != nil {
			http.Error(w, "failed to resolve resource state", http.StatusInternalServerError)
			return
		}
		lockPreconditions, ok := h.writeLockPreconditions(w, r, lockPaths...)
		if !ok {
			return
		}
		defer invalidateDAVRequestState(r.Context())
		expected := store.ContactDAVResourceState(existing)
		expected.ACL = aclGuard
		if err := h.store.DeleteContactAndState(r.Context(), addressBookID, expected, canonicalPath, lockPreconditions); err != nil {
			if errors.Is(err, store.ErrLockConflict) {
				http.Error(w, "resource is locked", http.StatusLocked)
				return
			}
			if errors.Is(err, store.ErrResourceStateChanged) {
				if retries > 0 {
					invalidateDAVRequestState(r.Context())
					h.deleteWithRetry(w, r, retries-1)
					return
				}
				http.Error(w, "resource changed while deleting; retry the request", http.StatusConflict)
				return
			}
			h.logger().Error("Delete", "failed to delete contact %q from address book %d: %v", existing.UID, addressBookID, err)
			http.Error(w, "failed to delete", http.StatusInternalServerError)
			return
		}
		h.logger().Info("Delete", "deleted contact %q from address book %d", existing.UID, addressBookID)
		w.WriteHeader(http.StatusNoContent)
		return
	}
	http.Error(w, "unsupported path", http.StatusBadRequest)
}

func (h *DavServer) deleteCalendarCollection(w http.ResponseWriter, r *http.Request, user *store.User, sourceSegment, cleanPath string) {
	sourceID, resolved, err := h.resolveCalendarID(r.Context(), user, sourceSegment)
	if err != nil || !resolved {
		if errors.Is(err, errAmbiguousCalendar) {
			http.Error(w, "ambiguous calendar path", http.StatusConflict)
			return
		}
		if err != nil && !errors.Is(err, store.ErrNotFound) {
			http.Error(w, "failed to resolve calendar", http.StatusInternalServerError)
			return
		}
		http.Error(w, "calendar not found", http.StatusNotFound)
		return
	}
	sourceCalendar, err := h.getCalendar(r.Context(), sourceID)
	if err != nil {
		writeCalendarCollectionAccessError(w, err)
		return
	}
	if sourceCalendar.UserID != user.ID {
		http.Error(w, "calendar not found", http.StatusNotFound)
		return
	}
	if err := h.requireACLPrivilege(r.Context(), user, calendarPrefix, "unbind"); err != nil {
		_ = writePrivilegeRequirementError(w, requirePrivilegeAt(err, calendarPrefix, "unbind"))
		return
	}
	source := &store.CalendarAccess{Calendar: *sourceCalendar}
	events, err := h.store.Events.ListForCalendar(r.Context(), source.ID)
	if err != nil {
		http.Error(w, "failed to load calendar members", http.StatusInternalServerError)
		return
	}
	members := make([]store.CalendarCollectionMember, 0, len(events))
	for i := range events {
		event := &events[i]
		members = append(members, store.CalendarCollectionMember{
			ID: event.ID, UID: event.UID, ResourceName: eventResourceName(*event), RawICAL: event.RawICAL, ETag: event.ETag,
		})
	}
	lockPaths := []string{cleanPath, path.Dir(cleanPath)}
	for i := range members {
		lockPaths = append(lockPaths, strings.TrimSuffix(cleanPath, "/")+"/"+members[i].ResourceName+".ics")
	}
	lockedBelow, err := h.lockedPathsBelow(r.Context(), calendarStatePath(source.ID))
	if err != nil {
		writeLockStateError(w, err)
		return
	}
	lockPaths = append(lockPaths, lockedBelow...)
	if !h.requireLocks(w, r, "resource is locked", lockPaths...) {
		return
	}
	lockPreconditions, ok := h.writeLockPreconditions(w, r, lockPaths...)
	if !ok {
		return
	}
	defer invalidateDAVRequestState(r.Context())
	_, err = h.store.TransferCalendarCollection(r.Context(), store.CalendarCollectionTransfer{
		Operation:          store.CalendarCollectionDelete,
		SourceID:           source.ID,
		ExpectedSourceCTag: source.CTag,
		SourceStatePath:    calendarStatePath(source.ID),
		Members:            members,
		LockPreconditions:  lockPreconditions,
	})
	if err != nil {
		switch {
		case errors.Is(err, store.ErrResourceStateChanged):
			http.Error(w, "calendar changed while deleting", http.StatusConflict)
		case errors.Is(err, store.ErrLockConflict):
			http.Error(w, "resource is locked", http.StatusLocked)
		case errors.Is(err, store.ErrNotFound):
			http.Error(w, "calendar not found", http.StatusNotFound)
		default:
			http.Error(w, "failed to delete calendar collection", http.StatusInternalServerError)
		}
		return
	}
	w.Header().Set("Cache-Control", "no-cache")
	w.WriteHeader(http.StatusNoContent)
}
