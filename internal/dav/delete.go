package dav

import (
	"context"
	"errors"
	"net/http"
	"path"
	"strings"

	"github.com/jw6ventures/calcard/internal/auth"
	"github.com/jw6ventures/calcard/internal/store"
)

func (h *DavServer) delete(w http.ResponseWriter, r *http.Request) {
	h.deleteWithRetry(w, r, maxResourceStateRetries)
}

// memberDeleteDecision decides a DELETE of a member, which needs some
// privilege on the member and DAV:unbind on its collection (RFC 3744 Appendix
// B). decide makes the decision as the ACL entries at the path it is given
// have it: the member's own and its collection's at the member's path, the
// collection's alone, as for a member that is not there, at the collection's.
//
// A principal holding no privilege on an existing member, and so unable to
// read it, would learn from a refusal its own ACL entries caused that it
// exists, so it gets the answer a member that is not there gets: the
// collection's refusal, or answerAsAbsent=true for the request to go on as
// for a missing member, to 404. A principal holding some privilege on
// the member has been told it is there, and is refused as it stands.
func memberDeleteDecision(memberPath, parentPath string, exists func() (bool, error), decide func(decisionPath string) (holdsNone bool, refusal error)) (refusal error, answerAsAbsent bool, err error) {
	holdsNone, refusal := decide(memberPath)
	if refusal == nil || !holdsNone || !refusalNamesPrivilege(refusal) {
		return refusal, false, nil
	}
	existing, err := exists()
	if err != nil || !existing {
		return refusal, false, err
	}
	if _, absentRefusal := decide(parentPath); absentRefusal != nil {
		return absentRefusal, false, nil
	}
	return nil, true, nil
}

// calendarMemberDeleteRefusal is memberDeleteDecision's decide for a member of
// calendarID. holdsNone reports a refusal for want of any privilege on the
// member, as opposed to one for want of DAV:unbind on the collection. A
// principal holding no privilege at all is answered as for a missing
// resource.
func (h *DavServer) calendarMemberDeleteRefusal(ctx context.Context, user *store.User, calendarID int64, decisionPath, parentPath string) (holdsNone bool, refusal error) {
	source, err := h.loadCalendarWithAnyPrivilege(ctx, user, calendarID, decisionPath)
	if err != nil {
		if errors.Is(err, errForbidden) {
			return true, requirePrivilegeAt(err, parentPath, "unbind")
		}
		return true, requirePrivatePrivilegeAt(err, parentPath, "unbind")
	}
	if err := h.requireCalendarPrivilege(ctx, user, &source.Calendar, parentPath, "unbind"); err != nil {
		return false, requirePrivilegeAt(err, parentPath, "unbind")
	}
	return false, nil
}

// addressBookMemberDeleteRefusal is calendarMemberDeleteRefusal for a member
// of book.
func (h *DavServer) addressBookMemberDeleteRefusal(ctx context.Context, user *store.User, book *store.AddressBook, decisionPath, parentPath string) (holdsNone bool, refusal error) {
	if err := h.requireAnyAddressBookPrivilege(ctx, user, book, decisionPath); err != nil {
		if errors.Is(err, errForbidden) {
			return true, requirePrivilegeAt(err, parentPath, "unbind")
		}
		return true, requirePrivatePrivilegeAt(err, parentPath, "unbind")
	}
	if err := h.requireAddressBookPrivilege(ctx, user, book, parentPath, "unbind"); err != nil {
		return false, requirePrivilegeAt(err, parentPath, "unbind")
	}
	return false, nil
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
		privilegePath := path.Dir(cleanPath)
		refusal, answerAsAbsent, err := memberDeleteDecision(cleanPath, privilegePath,
			func() (bool, error) {
				event, err := h.store.Events.GetByResourceName(r.Context(), calendarID, uid)
				return event != nil, err
			},
			func(decisionPath string) (bool, error) {
				return h.calendarMemberDeleteRefusal(r.Context(), user, calendarID, decisionPath, privilegePath)
			})
		if err != nil {
			http.Error(w, "failed to load event", http.StatusInternalServerError)
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
		if !h.checkConditionalHeaders(r, existing) {
			http.Error(w, "precondition failed", http.StatusPreconditionFailed)
			return
		}
		lockPaths := []string{cleanPath, path.Dir(cleanPath)}
		if !h.requireLocks(w, r, "resource is locked", lockPaths...) {
			return
		}
		canonicalPath, err := h.canonicalDAVPath(r.Context(), user, cleanPath)
		if err != nil {
			http.Error(w, "failed to resolve resource state", http.StatusInternalServerError)
			return
		}
		defer invalidateDAVRequestState(r.Context())
		expected := store.EventDAVResourceState(existing)
		expected.ACL = aclGuard
		if err := h.store.DeleteEventAndState(r.Context(), calendarID, expected, canonicalPath, h.lockPreconditions(r, lockPaths...)); err != nil {
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
		privilegePath := path.Dir(cleanPath)
		refusal, answerAsAbsent, err := memberDeleteDecision(cleanPath, privilegePath,
			func() (bool, error) { return existing != nil, nil },
			func(decisionPath string) (bool, error) {
				return h.addressBookMemberDeleteRefusal(r.Context(), user, book, decisionPath, privilegePath)
			})
		if err != nil {
			http.Error(w, "failed to load contact", http.StatusInternalServerError)
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
		defer invalidateDAVRequestState(r.Context())
		expected := store.ContactDAVResourceState(existing)
		expected.ACL = aclGuard
		if err := h.store.DeleteContactAndState(r.Context(), addressBookID, expected, canonicalPath, h.lockPreconditions(r, lockPaths...)); err != nil {
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
		lockPaths = append(lockPaths, strings.TrimSuffix(cleanPath, "/")+"/"+members[i].ResourceName)
	}
	if !h.requireLocks(w, r, "resource is locked", lockPaths...) {
		return
	}
	defer invalidateDAVRequestState(r.Context())
	_, err = h.store.TransferCalendarCollection(r.Context(), store.CalendarCollectionTransfer{
		Operation:          store.CalendarCollectionDelete,
		SourceID:           source.ID,
		ExpectedSourceCTag: source.CTag,
		SourceStatePath:    calendarStatePath(source.ID),
		Members:            members,
		LockPreconditions:  h.lockPreconditions(r, lockPaths...),
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
