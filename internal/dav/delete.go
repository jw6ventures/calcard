package dav

import (
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
		privilegePath := path.Dir(cleanPath)
		source, accessErr := h.loadCalendarWithAnyPrivilege(r.Context(), user, calendarID, cleanPath)
		if accessErr != nil {
			if errors.Is(accessErr, errForbidden) {
				_ = writePrivilegeRequirementError(w, requirePrivilegeAt(accessErr, privilegePath, "unbind"))
			} else {
				_ = writePrivilegeRequirementError(w, requirePrivatePrivilegeAt(accessErr, privilegePath, "unbind"))
			}
			return
		}
		if err := h.requireCalendarPrivilege(r.Context(), user, &source.Calendar, privilegePath, "unbind"); err != nil {
			_ = writePrivilegeRequirementError(w, requirePrivilegeAt(err, privilegePath, "unbind"))
			return
		}
		existing, err := h.store.Events.GetByResourceName(r.Context(), calendarID, uid)
		if err != nil {
			http.Error(w, "failed to load event", http.StatusInternalServerError)
			return
		}
		if !h.checkConditionalHeaders(r, existing) {
			http.Error(w, "precondition failed", http.StatusPreconditionFailed)
			return
		}
		if existing == nil {
			http.Error(w, "not found", http.StatusNotFound)
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
		expected.CollectionCTag = &source.CTag
		if err := h.store.DeleteEventAndState(r.Context(), calendarID, expected, canonicalPath, h.lockPreconditions(r, lockPaths...)); err != nil {
			if errors.Is(err, store.ErrLockConflict) {
				http.Error(w, "resource is locked", http.StatusLocked)
				return
			}
			if errors.Is(err, store.ErrResourceStateChanged) {
				http.Error(w, "precondition failed", http.StatusPreconditionFailed)
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
		if accessErr := h.requireAnyAddressBookPrivilege(r.Context(), user, book, cleanPath); accessErr != nil {
			if errors.Is(accessErr, errForbidden) {
				_ = writePrivilegeRequirementError(w, requirePrivilegeAt(accessErr, privilegePath, "unbind"))
			} else {
				_ = writePrivilegeRequirementError(w, requirePrivatePrivilegeAt(accessErr, privilegePath, "unbind"))
			}
			return
		}
		if err := h.requireAddressBookPrivilege(r.Context(), user, book, privilegePath, "unbind"); err != nil {
			_ = writePrivilegeRequirementError(w, requirePrivilegeAt(err, privilegePath, "unbind"))
			return
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
		expected.CollectionCTag = &book.CTag
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
