package dav

import (
	"bytes"
	"context"
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
	if report.XMLName.Local == "calendar-query" && !validCalendarFilterCollations(report.Filter) {
		// RFC 4791 §7.8.7 and §1.3: no resubmission of the same request can make
		// an unimplemented collation work, so it is a 403.
		writeCalDAVError(w, http.StatusForbidden, "supported-collation")
		return
	}
	if report.XMLName.Local == "free-busy-query" {
		if !validTimeRange(report.TimeRange) {
			http.Error(w, "invalid time-range", http.StatusBadRequest)
			return
		}
		// Checked here, before dispatch, so every calendar type -- including the
		// virtual birthday collection -- is covered by the single guard.
		if !freeBusyHasEffectiveTimeRange(report.Filter, report.TimeRange) {
			http.Error(w, "time-range required", http.StatusBadRequest)
			return
		}
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
	if isACLReport(report.XMLName.Local) {
		h.reportACL(w, r, user, cleanPath, body)
		return
	}

	if report.XMLName.Local == "calendar-query" || report.XMLName.Local == "calendar-multiget" {
		// RFC 4791 §7: both reports are supported on calendar object resources
		// as well as on calendar collections, so only a target outside the
		// calendar namespace is refused here.
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
		h.reportCalendar(w, r, user, cleanPath, report, expandReq)
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

func (h *DavServer) reportCalendar(w http.ResponseWriter, r *http.Request, user *store.User, cleanPath string, report reportRequest, expandReq *expandPropertyRequest) {
	target := parsedDAVTarget(r.Context(), cleanPath)
	// RFC 4791 §7 makes calendar-query and calendar-multiget available on
	// calendar object resources. CalCard also advertises DAV:expand-property
	// there, so those are the three object-target reports served here.
	resourceName := ""
	if target.Domain == davPathCalendar && target.Resource {
		switch report.XMLName.Local {
		case "calendar-query", "calendar-multiget", "expand-property":
			resourceName = target.ResourceName
		default:
			writeDAVError(w, http.StatusForbidden, "supported-report")
			return
		}
	}
	if !target.Valid || target.Domain != davPathCalendar || target.CollectionSegment == "" {
		http.Error(w, "invalid calendar path", http.StatusBadRequest)
		return
	}
	if resourceName != "" && report.XMLName.Local == "calendar-multiget" {
		if len(report.Hrefs) != 1 {
			http.Error(w, "calendar-multiget href must identify the request resource", http.StatusBadRequest)
			return
		}
	}
	calID, ok, err := h.resolveCalendarID(r.Context(), user, target.CollectionSegment)
	if err != nil {
		if errors.Is(err, errAmbiguousCalendar) {
			http.Error(w, "ambiguous calendar path", http.StatusConflict)
			return
		}
		if errors.Is(err, store.ErrNotFound) {
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
	if resourceName != "" && report.XMLName.Local == "calendar-multiget" {
		equivalent, err := h.calendarMultigetHrefIdentifiesResource(r.Context(), user, calID, resourceName, cleanPath, report.Hrefs[0])
		if err != nil {
			http.Error(w, "failed to resolve calendar-multiget href", http.StatusInternalServerError)
			return
		}
		if !equivalent {
			http.Error(w, "calendar-multiget href must identify the request resource", http.StatusBadRequest)
			return
		}
	}

	if calID == birthdayCalendarID {
		h.reportBirthdayCalendar(w, r, user, cleanPath, resourceName, report, expandReq)
		return
	}

	loadPrivilege := "read"
	if report.XMLName.Local == "free-busy-query" {
		loadPrivilege = "read-free-busy"
	}
	cal, err := h.loadCalendarWithPrivilege(r.Context(), user, calID, cleanPath, loadPrivilege)
	if err != nil {
		if errors.Is(err, errForbidden) || isPrivilegeNotGranted(err) {
			writeNeedPrivileges(w, cleanPath, loadPrivilege)
			return
		}
		status := http.StatusInternalServerError
		if errors.Is(err, store.ErrNotFound) {
			status = http.StatusNotFound
		}
		http.Error(w, "calendar not found", status)
		return
	}
	canonicalPath := path.Join("/dav/calendars", fmt.Sprint(cal.ID))
	if report.XMLName.Local == "expand-property" {
		if resourceName != "" {
			event, err := h.store.Events.GetByResourceName(r.Context(), cal.ID, resourceName)
			if err != nil {
				http.Error(w, "failed to fetch event", http.StatusInternalServerError)
				return
			}
			if event == nil {
				http.Error(w, "calendar object not found", http.StatusNotFound)
				return
			}
			resp := buildCalendarObjectExpandPropertyResponse(normalizeDAVHref(cleanPath), *event, expandReq)
			h.writeBoundedMultiStatus(w, newMultistatus([]response{resp}, ""))
			return
		}
		principalHref := h.principalURL(user)
		href := ensureCollectionHref(canonicalPath)
		ctag := strconv.FormatInt(cal.CTag, 10)
		syncToken := buildSyncToken("cal", cal.ID, cal.UpdatedAt)
		responses := []response{
			calendarCollectionResponseWithPrivileges(href, cal.Name, cal.Calendar, principalHref, syncToken, ctag, cal.EffectivePrivileges()),
			principalResponse(ensureCollectionHref(principalHref), user),
		}
		h.writeBoundedMultiStatus(w, newMultistatus(responses, ""))
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
	responses, syncToken, err := h.calendarReportResponses(r.Context(), user, cal, h.principalURL(user), cleanPath, canonicalPath, resourceName, report)
	if err != nil {
		if errors.Is(err, errUnsupportedReport) {
			writeDAVError(w, http.StatusForbidden, "supported-report")
		} else if errors.Is(err, errInvalidSyncToken) {
			http.Error(w, "invalid sync token", http.StatusForbidden)
		} else if errors.Is(err, store.ErrNotFound) {
			http.Error(w, "calendar object not found", http.StatusNotFound)
		} else {
			http.Error(w, err.Error(), http.StatusInternalServerError)
		}
		return
	}
	h.writeBoundedMultiStatus(w, newMultistatus(responses, syncToken))
}

func (h *DavServer) calendarMultigetHrefIdentifiesResource(ctx context.Context, user *store.User, calendarID int64, resourceName, requestPath, href string) (bool, error) {
	resolvedHref := resolveDAVHref(requestPath, href)
	hrefCalendarID, hrefResourceName, matched, err := h.parseCalendarResourcePath(ctx, user, resolvedHref)
	if err != nil {
		if errors.Is(err, store.ErrNotFound) || errors.Is(err, errAmbiguousCalendar) {
			return false, nil
		}
		return false, err
	}
	return matched && hrefCalendarID == calendarID && hrefResourceName == resourceName, nil
}

func (h *DavServer) reportBirthdayCalendar(w http.ResponseWriter, r *http.Request, user *store.User, cleanPath, targetResource string, report reportRequest, expandReq *expandPropertyRequest) {
	if report.XMLName.Local == "expand-property" {
		if targetResource != "" {
			events, err := h.generateBirthdayEvents(r.Context(), user.ID)
			if err != nil {
				http.Error(w, "failed to generate birthday events", http.StatusInternalServerError)
				return
			}
			for _, event := range events {
				if eventResourceName(event) == targetResource {
					resp := buildCalendarObjectExpandPropertyResponse(normalizeDAVHref(cleanPath), event, expandReq)
					h.writeBoundedMultiStatus(w, newMultistatus([]response{resp}, ""))
					return
				}
			}
			http.Error(w, "calendar object not found", http.StatusNotFound)
			return
		}
		principalHref := h.principalURL(user)
		responses := []response{
			birthdayCalendarCollection(birthdayCalendarHref(), principalHref),
			principalResponse(ensureCollectionHref(principalHref), user),
		}
		h.writeBoundedMultiStatus(w, newMultistatus(responses, ""))
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

	responses, syncToken, err := h.birthdayCalendarReportResponses(r.Context(), user, h.principalURL(user), cleanPath, targetResource, report)
	if err != nil {
		if errors.Is(err, errUnsupportedReport) {
			writeDAVError(w, http.StatusForbidden, "supported-report")
			return
		}
		if errors.Is(err, store.ErrNotFound) {
			http.Error(w, "calendar object not found", http.StatusNotFound)
			return
		}
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	h.writeBoundedMultiStatus(w, newMultistatus(responses, syncToken))
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
		if errors.Is(err, errForbidden) || isPrivilegeNotGranted(err) {
			writeNeedPrivileges(w, cleanPath, "read")
			return
		}
		status := http.StatusInternalServerError
		if errors.Is(err, store.ErrNotFound) {
			status = http.StatusNotFound
		}
		http.Error(w, "address book not found", status)
		return
	}
	// Depth:0 on a collection for addressbook-query means only the collection
	// itself, not its children — return empty multistatus after access checks.
	depth := strings.TrimSpace(r.Header.Get("Depth"))
	if report.XMLName.Local == "addressbook-query" && !target.Resource && depth == "0" {
		h.writeBoundedMultiStatus(w, newMultistatus(nil, ""))
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
	h.writeBoundedMultiStatus(w, newMultistatus(responses, syncToken))
}

func (h *DavServer) reportRootExpandProperty(w http.ResponseWriter, user *store.User, expandReq *expandPropertyRequest) {
	rootResp := rootCollectionResponse("/dav/", h.principalURL(user))
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
	h.writeBoundedMultiStatus(w, newMultistatus([]response{rootResp}, ""))
}
