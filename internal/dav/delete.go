package dav

import (
	"errors"
	"net/http"
	"path"

	"github.com/jw6ventures/calcard/internal/auth"
	"github.com/jw6ventures/calcard/internal/store"
)

func (h *DavServer) Delete(w http.ResponseWriter, r *http.Request) {
	if h.handleRegisteredMethod(w, r) {
		return
	}
	h.logger().Trace("Delete", "DELETE %s", r.URL.Path)
	user, ok := auth.UserFromContext(r.Context())
	if !ok {
		http.Error(w, "missing user", http.StatusUnauthorized)
		return
	}

	cleanPath := path.Clean(r.URL.Path)
	if !h.requireLock(w, r, cleanPath, "resource is locked") {
		return
	}
	if calendarID, uid, matched, err := h.parseCalendarResourcePath(r.Context(), user, cleanPath); err != nil {
		if err == store.ErrNotFound {
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
		if calendarID == birthdayCalendarID {
			http.Error(w, "birthday calendar is read-only", http.StatusForbidden)
			return
		}

		_, err = h.loadCalendarWithPrivilege(r.Context(), user, calendarID, cleanPath, "unbind")
		if err != nil {
			status := http.StatusInternalServerError
			if err == store.ErrNotFound {
				status = http.StatusNotFound
			}
			if errors.Is(err, errForbidden) {
				status = http.StatusForbidden
			}
			http.Error(w, "not found", status)
			return
		}
		if !h.requireLock(w, r, path.Dir(cleanPath), "resource is locked") {
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
		canonicalPath, err := h.canonicalDAVPath(r.Context(), user, cleanPath)
		if err != nil {
			http.Error(w, "failed to resolve resource state", http.StatusInternalServerError)
			return
		}
		if err := h.store.DeleteEventAndState(r.Context(), calendarID, existing.UID, canonicalPath); err != nil {
			h.logger().Error("Delete", "failed to delete event %q from calendar %d: %v", existing.UID, calendarID, err)
			http.Error(w, "failed to delete", http.StatusInternalServerError)
			return
		}
		h.logger().Info("Delete", "deleted event %q from calendar %d", existing.UID, calendarID)
		w.WriteHeader(http.StatusNoContent)
		return
	}
	if addressBookID, resourceName, matched, err := h.parseAddressBookResourcePath(r.Context(), user, cleanPath); err != nil {
		if err == store.ErrNotFound {
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
			if err == store.ErrNotFound {
				status = http.StatusNotFound
			}
			http.Error(w, "not found", status)
			return
		}
		existing, err := h.store.Contacts.GetByResourceName(r.Context(), addressBookID, resourceName)
		if err != nil {
			http.Error(w, "failed to load contact", http.StatusInternalServerError)
			return
		}
		if err := h.requireAddressBookPrivilege(r.Context(), user, book, cleanPath, "unbind"); err != nil {
			status := http.StatusForbidden
			if err == store.ErrNotFound {
				status = http.StatusNotFound
			}
			http.Error(w, http.StatusText(status), status)
			return
		}
		if !h.requireLock(w, r, path.Dir(cleanPath), "resource is locked") {
			return
		}
		if !h.checkConditionalHeadersContact(r, existing) {
			http.Error(w, "precondition failed", http.StatusPreconditionFailed)
			return
		}
		if existing == nil {
			http.Error(w, "not found", http.StatusNotFound)
			return
		}
		canonicalPath, err := h.canonicalDAVPath(r.Context(), user, cleanPath)
		if err != nil {
			http.Error(w, "failed to resolve resource state", http.StatusInternalServerError)
			return
		}
		if err := h.store.DeleteContactAndState(r.Context(), addressBookID, existing.UID, canonicalPath); err != nil {
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
