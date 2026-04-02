package dav

import (
	"errors"
	"fmt"
	"net/http"
	"path"
	"strings"

	"github.com/jw6ventures/calcard/internal/auth"
	"github.com/jw6ventures/calcard/internal/store"
)

func (h *Handler) Get(w http.ResponseWriter, r *http.Request) {
	cleanPath := path.Clean(r.URL.Path)
	if !strings.HasPrefix(cleanPath, "/dav") {
		http.Error(w, "not found", http.StatusNotFound)
		return
	}
	if strings.HasPrefix(cleanPath, "/dav/addressbooks/") {
		trimmed := strings.Trim(strings.TrimPrefix(cleanPath, "/dav/addressbooks"), "/")
		if trimmed != "" && len(strings.Split(trimmed, "/")) > 2 {
			http.Error(w, "not found", http.StatusNotFound)
			return
		}
	}

	user, ok := auth.UserFromContext(r.Context())
	if !ok {
		http.Error(w, "missing user", http.StatusUnauthorized)
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
			events, err := h.generateBirthdayEvents(r.Context(), user.ID)
			if err != nil {
				http.Error(w, "failed to load birthday events", http.StatusInternalServerError)
				return
			}

			var event *store.Event
			for i := range events {
				if events[i].UID == uid {
					event = &events[i]
					break
				}
			}
			if event == nil {
				http.Error(w, "not found", http.StatusNotFound)
				return
			}
			w.Header().Set("Content-Type", "text/calendar")
			w.Header().Set("ETag", fmt.Sprintf("\"%s\"", event.ETag))
			if !event.LastModified.IsZero() {
				w.Header().Set("Last-Modified", event.LastModified.UTC().Format(http.TimeFormat))
			}
			_, _ = w.Write([]byte(event.RawICAL))
			return
		}

		// Handle regular calendars
		if _, err := h.loadCalendar(r.Context(), user, calendarID); err != nil {
			if err == store.ErrNotFound {
				http.Error(w, "not found", http.StatusNotFound)
				return
			}
			http.Error(w, "failed to load calendar", http.StatusInternalServerError)
			return
		}
		event, err := h.store.Events.GetByResourceName(r.Context(), calendarID, uid)
		if err != nil {
			http.Error(w, "failed to load event", http.StatusInternalServerError)
			return
		}
		if event == nil {
			http.Error(w, "not found", http.StatusNotFound)
			return
		}
		w.Header().Set("Content-Type", "text/calendar")
		w.Header().Set("ETag", fmt.Sprintf("\"%s\"", event.ETag))
		if !event.LastModified.IsZero() {
			w.Header().Set("Last-Modified", event.LastModified.UTC().Format(http.TimeFormat))
		}
		_, _ = w.Write([]byte(event.RawICAL))
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
		if _, err := h.loadAddressBookWithPrivilege(r.Context(), user, addressBookID, cleanPath, "read"); err != nil {
			if err == store.ErrNotFound {
				http.Error(w, "not found", http.StatusNotFound)
				return
			}
			if errors.Is(err, errForbidden) {
				http.Error(w, "forbidden", http.StatusForbidden)
				return
			}
			http.Error(w, "failed to load address book", http.StatusInternalServerError)
			return
		}
		h.writeAddressBookContact(w, r, addressBookID, resourceName)
		return
	}

	w.Header().Set("DAV", davHeaderForPath(cleanPath))
	w.WriteHeader(http.StatusOK)
}

func (h *Handler) writeAddressBookContact(w http.ResponseWriter, r *http.Request, addressBookID int64, resourceName string) {
	contact, err := h.store.Contacts.GetByResourceName(r.Context(), addressBookID, resourceName)
	if err != nil {
		http.Error(w, "failed to load contact", http.StatusInternalServerError)
		return
	}
	if contact == nil {
		http.Error(w, "not found", http.StatusNotFound)
		return
	}
	if !acceptsVCardData(contact.RawVCard, r.Header.Get("Accept")) {
		writeCardDAVPrecondition(w, http.StatusNotAcceptable, "supported-address-data-conversion")
		return
	}
	w.Header().Set("Content-Type", "text/vcard")
	w.Header().Set("ETag", fmt.Sprintf("\"%s\"", contact.ETag))
	if !contact.LastModified.IsZero() {
		w.Header().Set("Last-Modified", contact.LastModified.UTC().Format(http.TimeFormat))
	}
	_, _ = w.Write([]byte(contact.RawVCard))
}
