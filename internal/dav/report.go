package dav

import (
	"encoding/xml"
	"errors"
	"fmt"
	"net/http"
	"path"
	"strconv"
	"strings"
	"time"

	"gitea.jw6.us/james/calcard/internal/auth"
	"gitea.jw6.us/james/calcard/internal/store"
)

func (h *Handler) Report(w http.ResponseWriter, r *http.Request) {
	user, ok := auth.UserFromContext(r.Context())
	if !ok {
		http.Error(w, "missing user", http.StatusUnauthorized)
		return
	}

	cleanPath := path.Clean(r.URL.Path)
	ensureCollectionHref := func(p string) string {
		if !strings.HasSuffix(p, "/") {
			return p + "/"
		}
		return p
	}
	body, err := readDAVBody(w, r, maxDAVBodyBytes)
	if err != nil {
		if errors.Is(err, errRequestTooLarge) {
			http.Error(w, "request too large", http.StatusRequestEntityTooLarge)
		} else {
			http.Error(w, "failed to read body", http.StatusBadRequest)
		}
		return
	}
	var report reportRequest
	if err := safeUnmarshalXML(body, &report); err != nil {
		http.Error(w, "invalid REPORT body", http.StatusBadRequest)
		return
	}

	if report.XMLName.Local == "calendar-query" || report.XMLName.Local == "calendar-multiget" {
		if _, _, ok := parseCalendarResourceSegments(cleanPath); ok {
			http.Error(w, "calendar reports not allowed on calendar object resources", http.StatusForbidden)
			return
		}
		if !strings.HasPrefix(cleanPath, "/dav/calendars/") {
			http.Error(w, "calendar reports must target a calendar collection", http.StatusForbidden)
			return
		}
	}

	if report.XMLName.Local == "free-busy-query" {
		if _, _, ok := parseCalendarResourceSegments(cleanPath); ok {
			http.Error(w, "free-busy-query not allowed on calendar object resources", http.StatusForbidden)
			return
		}
	}

	if strings.HasPrefix(cleanPath, "/dav/calendars/") {
		// Reject REPORT requests on resource paths (only allow on collection)
		if _, _, isResource := parseCalendarResourceSegments(cleanPath); isResource {
			http.Error(w, "REPORT not allowed on calendar object resources", http.StatusForbidden)
			return
		}

		rel := strings.Trim(strings.TrimPrefix(cleanPath, "/dav/calendars"), "/")
		parts := strings.Split(rel, "/")
		if len(parts) < 1 || strings.TrimSpace(parts[0]) == "" {
			http.Error(w, "invalid calendar path", http.StatusBadRequest)
			return
		}
		calID, ok, err := h.resolveCalendarID(r.Context(), user, strings.TrimSpace(parts[0]))
		if err != nil {
			if errors.Is(err, errAmbiguousCalendar) {
				http.Error(w, "ambiguous calendar path", http.StatusConflict)
				return
			}
			if err == store.ErrNotFound {
				http.Error(w, "calendar not found", http.StatusNotFound)
				return
			}
			http.Error(w, "failed to resolve calendar", http.StatusInternalServerError)
			return
		}
		if !ok {
			http.Error(w, "invalid calendar id", http.StatusBadRequest)
			return
		}

		// Handle birthday calendar reports
		if calID == birthdayCalendarID {
			if report.XMLName.Local == "expand-property" {
				principalHref := h.principalURL(user)
				href := ensureCollectionHref(path.Join("/dav/calendars", fmt.Sprint(birthdayCalendarID)))
				birthdayName := "Birthdays"
				birthdayDesc := "Contact birthdays from your address books"
				syncToken := buildSyncToken("cal", birthdayCalendarID, time.Unix(0, 0))
				responses := []response{
					calendarCollectionResponse(href, birthdayName, &birthdayDesc, nil, principalHref, syncToken, "0", true),
					principalResponse(ensureCollectionHref(principalHref), user),
				}
				payload := multistatus{
					XMLName:  xml.Name{Space: "DAV:", Local: "multistatus"},
					XmlnsD:   "DAV:",
					XmlnsC:   "urn:ietf:params:xml:ns:caldav",
					XmlnsA:   "urn:ietf:params:xml:ns:carddav",
					XmlnsCS:  "http://calendarserver.org/ns/",
					Response: responses,
				}
				w.Header().Set("Content-Type", "application/xml; charset=utf-8")
				w.WriteHeader(http.StatusMultiStatus)
				_ = xml.NewEncoder(w).Encode(payload)
				return
			}

			if report.XMLName.Local == "free-busy-query" {
				events, err := h.generateBirthdayEvents(r.Context(), user.ID)
				if err != nil {
					http.Error(w, "failed to generate birthday events", http.StatusInternalServerError)
					return
				}
				if report.Filter != nil {
					events = h.applyCalendarFilter(events, report.Filter)
				}
				freeBusyData := h.generateFreeBusy(events, report.Filter)
				w.Header().Set("Content-Type", "text/calendar")
				w.WriteHeader(http.StatusOK)
				_, _ = w.Write([]byte(freeBusyData))
				return
			}

			responses, syncToken, err := h.birthdayCalendarReportResponses(r.Context(), user, h.principalURL(user), cleanPath, report)
			if err != nil {
				http.Error(w, err.Error(), http.StatusInternalServerError)
				return
			}
			payload := multistatus{
				XMLName:   xml.Name{Space: "DAV:", Local: "multistatus"},
				XmlnsD:    "DAV:",
				XmlnsC:    "urn:ietf:params:xml:ns:caldav",
				XmlnsA:    "urn:ietf:params:xml:ns:carddav",
				XmlnsCS:   "http://calendarserver.org/ns/",
				SyncToken: syncToken,
				Response:  responses,
			}
			w.Header().Set("Content-Type", "application/xml; charset=utf-8")
			w.WriteHeader(http.StatusMultiStatus)
			_ = xml.NewEncoder(w).Encode(payload)
			return
		}

		// Handle regular calendar reports
		cal, err := h.loadCalendar(r.Context(), user, calID)
		if err != nil {
			status := http.StatusInternalServerError
			if err == store.ErrNotFound {
				status = http.StatusNotFound
			}
			http.Error(w, "calendar not found", status)
			return
		}
		if cal.Shared && !cal.Editor {
			if report.XMLName.Local == "calendar-query" || report.XMLName.Local == "calendar-multiget" {
				http.Error(w, "forbidden", http.StatusForbidden)
				return
			}
		}
		canonicalPath := path.Join("/dav/calendars", fmt.Sprint(cal.ID))
		if report.XMLName.Local == "expand-property" {
			principalHref := h.principalURL(user)
			href := ensureCollectionHref(canonicalPath)
			ctag := fmt.Sprintf("%d", cal.CTag)
			syncToken := buildSyncToken("cal", cal.ID, cal.UpdatedAt)
			responses := []response{
				calendarCollectionResponse(href, cal.Name, cal.Description, cal.Timezone, principalHref, syncToken, ctag, !cal.Editor),
				principalResponse(ensureCollectionHref(principalHref), user),
			}
			payload := multistatus{
				XMLName:  xml.Name{Space: "DAV:", Local: "multistatus"},
				XmlnsD:   "DAV:",
				XmlnsC:   "urn:ietf:params:xml:ns:caldav",
				XmlnsA:   "urn:ietf:params:xml:ns:carddav",
				XmlnsCS:  "http://calendarserver.org/ns/",
				Response: responses,
			}
			w.Header().Set("Content-Type", "application/xml; charset=utf-8")
			w.WriteHeader(http.StatusMultiStatus)
			_ = xml.NewEncoder(w).Encode(payload)
			return
		}
		if report.XMLName.Local == "free-busy-query" {
			events, err := h.store.Events.ListForCalendar(r.Context(), cal.ID)
			if err != nil {
				http.Error(w, "failed to list events", http.StatusInternalServerError)
				return
			}
			if report.Filter != nil {
				events = h.applyCalendarFilter(events, report.Filter)
			}
			freeBusyData := h.generateFreeBusy(events, report.Filter)
			w.Header().Set("Content-Type", "text/calendar")
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(freeBusyData))
			return
		}
		responses, syncToken, err := h.calendarReportResponses(r.Context(), cal, h.principalURL(user), cleanPath, canonicalPath, report)
		if err != nil {
			if errors.Is(err, errInvalidSyncToken) {
				http.Error(w, "invalid sync token", http.StatusForbidden)
			} else {
				http.Error(w, err.Error(), http.StatusInternalServerError)
			}
			return
		}

		payload := multistatus{
			XMLName:   xml.Name{Space: "DAV:", Local: "multistatus"},
			XmlnsD:    "DAV:",
			XmlnsC:    "urn:ietf:params:xml:ns:caldav",
			XmlnsA:    "urn:ietf:params:xml:ns:carddav",
			XmlnsCS:   "http://calendarserver.org/ns/",
			SyncToken: syncToken,
			Response:  responses,
		}
		w.Header().Set("Content-Type", "application/xml; charset=utf-8")
		w.WriteHeader(http.StatusMultiStatus)
		_ = xml.NewEncoder(w).Encode(payload)
		return
	}

	if strings.HasPrefix(cleanPath, "/dav/addressbooks/") {
		parts := strings.Split(strings.TrimPrefix(cleanPath, "/dav/addressbooks"), "/")
		if len(parts) < 2 || parts[1] == "" {
			http.Error(w, "invalid address book path", http.StatusBadRequest)
			return
		}
		if len(parts) > 2 && parts[2] != "" {
			http.Error(w, "REPORT not allowed on address book object resources", http.StatusForbidden)
			return
		}
		bookID, err := strconv.ParseInt(parts[1], 10, 64)
		if err != nil {
			http.Error(w, "invalid address book id", http.StatusBadRequest)
			return
		}

		book, err := h.loadAddressBook(r.Context(), user, bookID)
		if err != nil {
			status := http.StatusInternalServerError
			if err == store.ErrNotFound {
				status = http.StatusNotFound
			}
			http.Error(w, "address book not found", status)
			return
		}
		responses, syncToken, err := h.addressBookReportResponses(r.Context(), book, h.principalURL(user), cleanPath, report)
		if err != nil {
			if errors.Is(err, errInvalidSyncToken) {
				http.Error(w, "invalid sync token", http.StatusForbidden)
			} else {
				http.Error(w, err.Error(), http.StatusInternalServerError)
			}
			return
		}

		payload := multistatus{
			XMLName:   xml.Name{Space: "DAV:", Local: "multistatus"},
			XmlnsD:    "DAV:",
			XmlnsC:    "urn:ietf:params:xml:ns:caldav",
			XmlnsA:    "urn:ietf:params:xml:ns:carddav",
			XmlnsCS:   "http://calendarserver.org/ns/",
			SyncToken: syncToken,
			Response:  responses,
		}
		w.Header().Set("Content-Type", "application/xml; charset=utf-8")
		w.WriteHeader(http.StatusMultiStatus)
		_ = xml.NewEncoder(w).Encode(payload)
		return
	}

	http.Error(w, "unsupported REPORT path", http.StatusBadRequest)
}
