package dav

import (
	"errors"
	"fmt"
	"net/http"
	"path"
	"strings"

	"gitea.jw6.us/james/calcard/internal/auth"
	"gitea.jw6.us/james/calcard/internal/store"
)

func (h *Handler) Get(w http.ResponseWriter, r *http.Request) {
	cleanPath := path.Clean(r.URL.Path)
	if !strings.HasPrefix(cleanPath, "/dav") {
		http.Error(w, "not found", http.StatusNotFound)
		return
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

	if addressBookID, uid, matched := parseResourcePath(cleanPath, "/dav/addressbooks"); matched {
		if _, err := h.loadAddressBook(r.Context(), user, addressBookID); err != nil {
			if err == store.ErrNotFound {
				http.Error(w, "not found", http.StatusNotFound)
				return
			}
			http.Error(w, "failed to load address book", http.StatusInternalServerError)
			return
		}
		contact, err := h.store.Contacts.GetByUID(r.Context(), addressBookID, uid)
		if err != nil {
			http.Error(w, "failed to load contact", http.StatusInternalServerError)
			return
		}
		if contact == nil {
			http.Error(w, "not found", http.StatusNotFound)
			return
		}
		w.Header().Set("Content-Type", "text/vcard")
		w.Header().Set("ETag", fmt.Sprintf("\"%s\"", contact.ETag))
		if !contact.LastModified.IsZero() {
			w.Header().Set("Last-Modified", contact.LastModified.UTC().Format(http.TimeFormat))
		}
		_, _ = w.Write([]byte(contact.RawVCard))
		return
	}

	w.Header().Set("DAV", "1, 2, calendar-access, addressbook")
	w.WriteHeader(http.StatusOK)
}
