package dav

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"net/http"
	"path"
	"strconv"
	"strings"

	"github.com/jw6ventures/calcard/internal/auth"
	"github.com/jw6ventures/calcard/internal/store"
)

func (h *DavServer) report(w http.ResponseWriter, r *http.Request) {
	user, ok := auth.UserFromContext(r.Context())
	if !ok {
		http.Error(w, "missing user", http.StatusUnauthorized)
		return
	}

	cleanPath := path.Clean(r.URL.Path)
	target := parsedDAVTarget(r.Context(), cleanPath)
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
		h.logger().Error("Report", "invalid REPORT body for %s: %v", cleanPath, err)
		http.Error(w, "invalid REPORT body", http.StatusBadRequest)
		return
	}
	h.logger().Trace("Report", "REPORT %s type=%s", cleanPath, report.XMLName.Local)
	var expandReq *expandPropertyRequest
	if report.XMLName.Local == "expand-property" {
		expandReq, err = parseExpandPropertyRequest(body)
		if err != nil {
			http.Error(w, "invalid REPORT body", http.StatusBadRequest)
			return
		}
	}
	if (report.XMLName.Local == "calendar-query" || report.XMLName.Local == "free-busy-query") && !validCalendarFilterTimeRanges(report.Filter) {
		http.Error(w, "invalid time-range", http.StatusBadRequest)
		return
	}
	if report.XMLName.Local == "free-busy-query" && !validTimeRange(report.TimeRange) {
		http.Error(w, "invalid time-range", http.StatusBadRequest)
		return
	}
	if handler, ok := h.davRegistry().reportHandler(cleanPath, report.XMLName.Local); ok {
		r.Body = io.NopCloser(bytes.NewReader(body))
		if handler(w, r, RequestContext{
			Context:    r.Context(),
			User:       user,
			Request:    r,
			Path:       cleanPath,
			Body:       body,
			ReportName: report.XMLName.Local,
		}) {
			return
		}
	}

	if report.XMLName.Local == "calendar-query" || report.XMLName.Local == "calendar-multiget" {
		if target.Domain == davPathCalendar && target.Resource {
			http.Error(w, "calendar reports not allowed on calendar object resources", http.StatusForbidden)
			return
		}
		if !strings.HasPrefix(cleanPath, "/dav/calendars/") {
			http.Error(w, "calendar reports must target a calendar collection", http.StatusForbidden)
			return
		}
	}

	if report.XMLName.Local == "free-busy-query" {
		if target.Domain == davPathCalendar && target.Resource {
			http.Error(w, "free-busy-query not allowed on calendar object resources", http.StatusForbidden)
			return
		}
	}

	if strings.HasPrefix(cleanPath, "/dav/calendars/") {
		h.reportCalendar(w, r, user, cleanPath, report)
		return
	}

	if strings.HasPrefix(cleanPath, "/dav/addressbooks/") {
		h.reportAddressBook(w, r, user, cleanPath, report, expandReq)
		return
	}

	if report.XMLName.Local == "expand-property" && (cleanPath == "/dav" || cleanPath == "/dav/") {
		h.reportRootExpandProperty(w, user, expandReq)
		return
	}

	http.Error(w, "unsupported REPORT path", http.StatusBadRequest)
}

func (h *DavServer) reportCalendar(w http.ResponseWriter, r *http.Request, user *store.User, cleanPath string, report reportRequest) {
	target := parsedDAVTarget(r.Context(), cleanPath)
	// Reject REPORT requests on resource paths (only allow on collection)
	if target.Domain == davPathCalendar && target.Resource {
		http.Error(w, "REPORT not allowed on calendar object resources", http.StatusForbidden)
		return
	}
	if !target.Valid || target.Domain != davPathCalendar || target.CollectionSegment == "" {
		http.Error(w, "invalid calendar path", http.StatusBadRequest)
		return
	}
	calID, ok, err := h.resolveCalendarID(r.Context(), user, target.CollectionSegment)
	if err != nil {
		if errors.Is(err, errAmbiguousCalendar) {
			http.Error(w, "ambiguous calendar path", http.StatusConflict)
			return
		}
		if err == store.ErrNotFound {
			http.Error(w, "calendar not found", http.StatusNotFound)
			return
		}
		h.logger().Error("Report", "failed to resolve calendar for %s: %v", cleanPath, err)
		http.Error(w, "failed to resolve calendar", http.StatusInternalServerError)
		return
	}
	if !ok {
		http.Error(w, "invalid calendar id", http.StatusBadRequest)
		return
	}

	if calID == birthdayCalendarID {
		h.reportBirthdayCalendar(w, r, user, cleanPath, report)
		return
	}

	loadPrivilege := "read"
	if report.XMLName.Local == "free-busy-query" {
		loadPrivilege = "read-free-busy"
	}
	cal, err := h.loadCalendarWithPrivilege(r.Context(), user, calID, cleanPath, loadPrivilege)
	if err != nil {
		status := http.StatusInternalServerError
		if err == store.ErrNotFound {
			status = http.StatusNotFound
		}
		if errors.Is(err, errForbidden) {
			status = http.StatusForbidden
		}
		http.Error(w, "calendar not found", status)
		return
	}
	canonicalPath := path.Join("/dav/calendars", fmt.Sprint(cal.ID))
	if report.XMLName.Local == "expand-property" {
		principalHref := h.principalURL(user)
		href := ensureCollectionHref(canonicalPath)
		ctag := strconv.FormatInt(cal.CTag, 10)
		syncToken := buildSyncToken("cal", cal.ID, cal.UpdatedAt)
		responses := []response{
			calendarCollectionResponseWithPrivileges(href, cal.Name, cal.Description, cal.Timezone, cal.Color, principalHref, syncToken, ctag, cal.EffectivePrivileges()),
			principalResponse(ensureCollectionHref(principalHref), user),
		}
		writeMultiStatus(w, newMultistatus(responses, ""))
		return
	}
	if report.XMLName.Local == "free-busy-query" {
		freeBusyData, err := h.freeBusyQuery(r.Context(), user, cal, report.Filter, report.TimeRange)
		if err != nil {
			http.Error(w, "failed to list events", http.StatusInternalServerError)
			return
		}
		w.Header().Set("Content-Type", "text/calendar")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(freeBusyData))
		return
	}
	responses, syncToken, err := h.calendarReportResponses(r.Context(), user, cal, h.principalURL(user), cleanPath, canonicalPath, report)
	if err != nil {
		if errors.Is(err, errUnsupportedReport) {
			writeDAVError(w, http.StatusForbidden, "supported-report")
		} else if errors.Is(err, errInvalidSyncToken) {
			http.Error(w, "invalid sync token", http.StatusForbidden)
		} else {
			http.Error(w, err.Error(), http.StatusInternalServerError)
		}
		return
	}
	writeMultiStatus(w, newMultistatus(responses, syncToken))
}

func (h *DavServer) reportBirthdayCalendar(w http.ResponseWriter, r *http.Request, user *store.User, cleanPath string, report reportRequest) {
	if report.XMLName.Local == "expand-property" {
		principalHref := h.principalURL(user)
		responses := []response{
			birthdayCalendarCollection(birthdayCalendarHref(), principalHref),
			principalResponse(ensureCollectionHref(principalHref), user),
		}
		writeMultiStatus(w, newMultistatus(responses, ""))
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
		if report.TimeRange != nil {
			events = h.filterCalendarEventsByTimeRange(events, report.TimeRange)
		}
		freeBusyData := h.generateFreeBusy(events, report.Filter, report.TimeRange)
		w.Header().Set("Content-Type", "text/calendar")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(freeBusyData))
		return
	}

	responses, syncToken, err := h.birthdayCalendarReportResponses(r.Context(), user, h.principalURL(user), cleanPath, report)
	if err != nil {
		if errors.Is(err, errUnsupportedReport) {
			writeDAVError(w, http.StatusForbidden, "supported-report")
			return
		}
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	writeMultiStatus(w, newMultistatus(responses, syncToken))
}

func (h *DavServer) reportAddressBook(w http.ResponseWriter, r *http.Request, user *store.User, cleanPath string, report reportRequest, expandReq *expandPropertyRequest) {
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

	target := parsedDAVTarget(r.Context(), cleanPath)
	if !target.Valid || target.Domain != davPathAddressBook || target.CollectionSegment == "" {
		http.Error(w, "invalid address book path", http.StatusBadRequest)
		return
	}
	bookID, ok, err := h.resolveAddressBookID(r.Context(), user, target.CollectionSegment)
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
	if target.Resource {
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
	if report.XMLName.Local == "addressbook-query" && !target.Resource && depth == "0" {
		writeMultiStatus(w, newMultistatus(nil, ""))
		return
	}
	responses, syncToken, err := h.addressBookReportResponses(r.Context(), user, book, h.principalURL(user), cleanPath, report, expandReq)
	if err != nil {
		if errors.Is(err, errUnsupportedReport) {
			writeDAVError(w, http.StatusForbidden, "supported-report")
		} else if errors.Is(err, errInvalidSyncToken) {
			http.Error(w, "invalid sync token", http.StatusForbidden)
		} else {
			http.Error(w, err.Error(), http.StatusInternalServerError)
		}
		return
	}
	writeMultiStatus(w, newMultistatus(responses, syncToken))
}

func (h *DavServer) reportRootExpandProperty(w http.ResponseWriter, user *store.User, expandReq *expandPropertyRequest) {
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
	writeMultiStatus(w, newMultistatus([]response{rootResp}, ""))
}
