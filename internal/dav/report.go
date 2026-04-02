package dav

import (
	"encoding/xml"
	"errors"
	"fmt"
	"net/http"
	"path"
	"strings"
	"time"

	"github.com/jw6ventures/calcard/internal/auth"
	"github.com/jw6ventures/calcard/internal/store"
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
	var expandReq *expandPropertyRequest
	if report.XMLName.Local == "expand-property" {
		expandReq, err = parseExpandPropertyRequest(body)
		if err != nil {
			http.Error(w, "invalid REPORT body", http.StatusBadRequest)
			return
		}
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
		_, hasDepth := r.Header["Depth"]
		if report.XMLName.Local == "addressbook-query" && report.CardFilter == nil {
			http.Error(w, "filter required", http.StatusBadRequest)
			return
		}
		if report.XMLName.Local == "addressbook-multiget" {
			if !hasDepth || strings.TrimSpace(r.Header.Get("Depth")) != "0" {
				http.Error(w, "Depth: 0 required", http.StatusBadRequest)
				return
			}
			if len(report.Hrefs) == 0 {
				http.Error(w, "href required", http.StatusBadRequest)
				return
			}
		}
		if err := validateAddressDataRequest(report.AddressData); err != nil {
			writeCardDAVPrecondition(w, http.StatusUnsupportedMediaType, "supported-address-data")
			return
		}
		if report.Prop != nil {
			if err := validateAddressDataRequest(report.Prop.AddressData); err != nil {
				writeCardDAVPrecondition(w, http.StatusUnsupportedMediaType, "supported-address-data")
				return
			}
		}
		if err := validateCardFilter(report.CardFilter); err != nil {
			status := http.StatusBadRequest
			if strings.Contains(err.Error(), "collation") {
				status = http.StatusNotImplemented
				writeCardDAVPrecondition(w, status, "supported-collation")
			} else {
				writeCardDAVPrecondition(w, status, "supported-filter")
			}
			return
		}

		trimmed := strings.Trim(strings.TrimPrefix(cleanPath, "/dav/addressbooks"), "/")
		parts := strings.Split(trimmed, "/")
		if len(parts) == 0 || strings.TrimSpace(parts[0]) == "" {
			http.Error(w, "invalid address book path", http.StatusBadRequest)
			return
		}
		bookID, ok, err := h.resolveAddressBookID(r.Context(), user, strings.TrimSpace(parts[0]))
		if err != nil {
			if errors.Is(err, errAmbiguousAddressBook) {
				http.Error(w, "ambiguous address book path", http.StatusConflict)
				return
			}
			if errors.Is(err, store.ErrNotFound) {
				http.Error(w, "address book not found", http.StatusNotFound)
				return
			}
			http.Error(w, "failed to resolve address book", http.StatusInternalServerError)
			return
		}
		if !ok {
			http.Error(w, "invalid address book id", http.StatusBadRequest)
			return
		}
		if len(parts) > 2 {
			http.Error(w, "invalid address book path", http.StatusBadRequest)
			return
		}
		isResource := len(parts) == 2 && parts[1] != ""
		if isResource {
			switch report.XMLName.Local {
			case "addressbook-query", "addressbook-multiget", "expand-property":
				if !hasDepth {
					http.Error(w, "REPORT not allowed on address book object resources", http.StatusForbidden)
					return
				}
			default:
				http.Error(w, "REPORT not allowed on address book object resources", http.StatusForbidden)
				return
			}
		}
		if report.XMLName.Local == "addressbook-query" && !hasDepth {
			http.Error(w, "Depth header required", http.StatusBadRequest)
			return
		}

		book, err := h.loadAddressBookWithPrivilege(r.Context(), user, bookID, cleanPath, "read")
		if err != nil {
			status := http.StatusInternalServerError
			if errors.Is(err, errForbidden) {
				status = http.StatusForbidden
			}
			if err == store.ErrNotFound {
				status = http.StatusNotFound
			}
			http.Error(w, "address book not found", status)
			return
		}
		// Depth:0 on a collection for addressbook-query means only the collection
		// itself, not its children — return empty multistatus after access checks.
		depth := strings.TrimSpace(r.Header.Get("Depth"))
		if report.XMLName.Local == "addressbook-query" && !isResource && depth == "0" {
			payload := multistatus{
				XMLName: xml.Name{Space: "DAV:", Local: "multistatus"},
				XmlnsD:  "DAV:",
				XmlnsC:  "urn:ietf:params:xml:ns:caldav",
				XmlnsA:  "urn:ietf:params:xml:ns:carddav",
				XmlnsCS: "http://calendarserver.org/ns/",
			}
			w.Header().Set("Content-Type", "application/xml; charset=utf-8")
			w.WriteHeader(http.StatusMultiStatus)
			_ = xml.NewEncoder(w).Encode(payload)
			return
		}
		responses, syncToken, err := h.addressBookReportResponses(r.Context(), user, book, h.principalURL(user), cleanPath, report, expandReq)
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

	// Handle expand-property at the DAV root level
	if report.XMLName.Local == "expand-property" && (cleanPath == "/dav" || cleanPath == "/dav/") {
		rootResp := rootCollectionResponse("/dav/", user, h.principalURL(user))
		selections := expandPropertySelections(expandReq)
		if len(rootResp.Propstat) > 0 {
			expanded := h.expandedPrincipalProp(user, selections)
			if expanded.CurrentUserPrincipal != nil {
				rootResp.Propstat[0].Prop.CurrentUserPrincipal = expanded.CurrentUserPrincipal
			}
			if expanded.PrincipalURL != nil {
				rootResp.Propstat[0].Prop.PrincipalURL = expanded.PrincipalURL
			}
		}
		payload := multistatus{
			XMLName:  xml.Name{Space: "DAV:", Local: "multistatus"},
			XmlnsD:   "DAV:",
			XmlnsC:   "urn:ietf:params:xml:ns:caldav",
			XmlnsA:   "urn:ietf:params:xml:ns:carddav",
			XmlnsCS:  "http://calendarserver.org/ns/",
			Response: []response{rootResp},
		}
		w.Header().Set("Content-Type", "application/xml; charset=utf-8")
		w.WriteHeader(http.StatusMultiStatus)
		_ = xml.NewEncoder(w).Encode(payload)
		return
	}

	http.Error(w, "unsupported REPORT path", http.StatusBadRequest)
}
