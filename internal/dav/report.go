package dav

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"path"
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
	if fault := h.checkReportBodyLimits(body); fault != nil {
		h.logger().Trace("Report", "rejected oversized body for %s: %v", cleanPath, fault)
		writeReportGrammarFault(w, fault)
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
	// CalDAV REPORT bodies are re-read against their RFC 4791 content models
	// because the permissive decode above drops unknown fields.
	if fault := applyCalendarReportGrammar(&report, body); fault != nil {
		h.logger().Trace("Report", "rejected %s body for %s: %v", report.XMLName.Local, cleanPath, fault)
		writeReportGrammarFault(w, fault)
		return
	}
	if report.XMLName.Local == "calendar-query" || report.XMLName.Local == "calendar-multiget" {
		if !supportedCalendarDataRequest(reportCalendarData(report)) {
			// RFC 4791 §7.8.5 and §7.9.4; §1.3 makes it a 403, because no
			// resubmission of the same request can make the server return a
			// media type it does not serve.
			writeCalDAVError(w, http.StatusForbidden, "supported-calendar-data")
			return
		}
	}
	// RFC 4791 §7.8 and §7.10 both process a request carrying no Depth header as
	// Depth: 0, and RFC 4918 §10.2 fixes the three legal values.
	if report.XMLName.Local == "calendar-query" || report.XMLName.Local == "free-busy-query" {
		if _, ok := reportDepth(r); !ok {
			http.Error(w, "invalid Depth header", http.StatusBadRequest)
			return
		}
	}
	// RFC 4791 §7.8 and §7.9 bound the range a report may ask about by the
	// CALDAV:min-date-time and CALDAV:max-date-time of the collections it
	// targets. §1.3 puts a precondition failure at 403: no resubmission of the
	// same range can bring it inside a limit the server does not move.
	if condition := reportTimeRangeDateLimitFault(report.Filter, report.TimeRange, reportCalendarData(report)); condition != "" {
		writeCalDAVError(w, http.StatusForbidden, condition)
		return
	}
	// The §9.11 grammar already required exactly one CALDAV:time-range, but its
	// bounds still have to be usable. Checked here, before dispatch, so every
	// calendar type -- including the virtual birthday collection -- is covered
	// by the single guard rather than reading a whole collection first.
	if report.XMLName.Local == "free-busy-query" && !freeBusyHasTimeRange(report.TimeRange) {
		http.Error(w, "time-range required", http.StatusBadRequest)
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
	if isACLReport(report.XMLName.Local) {
		h.reportACL(w, r, user, cleanPath, body)
		return
	}

	// RFC 3253 §3.8 defines expand-property over any resource, and its report
	// is the same on all of them: the properties the body names, with each
	// href-valued one expanded. One path serves every target.
	if report.XMLName.Local == "expand-property" {
		h.reportExpandProperty(w, r, user, cleanPath, expandReq)
		return
	}

	if report.XMLName.Local == "calendar-query" || report.XMLName.Local == "calendar-multiget" {
		// RFC 4791 §7: both reports are supported on calendar object resources
		// as well as on calendar collections, so only a target outside the
		// calendar namespace is refused here. RFC 4791 §7.2 leaves them optional
		// on an ordinary collection and RFC 3253 §3.6 spells the decline:
		// DAV:supported-report under a top-level DAV:error.
		if !strings.HasPrefix(cleanPath, "/dav/calendars/") {
			writeDAVError(w, http.StatusForbidden, "supported-report")
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
		h.reportAddressBook(w, r, user, cleanPath, report)
		return
	}

	http.Error(w, "unsupported REPORT path", http.StatusBadRequest)
}

func (h *DavServer) reportCalendar(w http.ResponseWriter, r *http.Request, user *store.User, cleanPath string, report reportRequest) {
	target := parsedDAVTarget(r.Context(), cleanPath)
	// RFC 4791 §7 makes calendar-query and calendar-multiget available on
	// calendar object resources, so those are the object-target reports served
	// here. DAV:expand-property is advertised there too and answered before
	// this path, by the one implementation that serves every resource kind.
	resourceName := ""
	if target.Domain == davPathCalendar && target.Resource {
		switch report.XMLName.Local {
		case "calendar-query", "calendar-multiget":
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
		equivalent, err := h.calendarMultigetHrefIdentifiesResource(r.Context(), user, calID, resourceName, report.Hrefs[0], r)
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
		h.reportBirthdayCalendar(w, r, user, cleanPath, resourceName, report)
		return
	}

	loadPrivilege := "read"
	if report.XMLName.Local == "free-busy-query" {
		loadPrivilege = "read-free-busy"
	}
	cal, err := h.loadCalendarWithPrivilege(r.Context(), user, calID, cleanPath, loadPrivilege)
	if err != nil {
		if report.XMLName.Local == "free-busy-query" {
			_ = writePrivilegeRequirementError(w, requirePrivatePrivilegeAt(err, cleanPath, loadPrivilege))
			return
		}
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
	if calendarQueryExcludedByDepth(r, report, resourceName) {
		h.writeBoundedMultiStatus(w, newMultistatus(nil, ""))
		return
	}
	if report.XMLName.Local == "free-busy-query" {
		// Out of Depth reach is the empty VFREEBUSY §7.10 requires when nothing
		// matches, answered without reading the collection at all.
		var freeBusyData string
		if freeBusyExcludedByDepth(r, report) {
			freeBusyData = h.generateFreeBusy(nil, report.TimeRange)
		} else {
			var err error
			freeBusyData, err = h.freeBusyQuery(r.Context(), user, cal, report.TimeRange)
			if err != nil {
				writeReportError(w, err)
				return
			}
		}
		w.Header().Set("Content-Type", "text/calendar")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(freeBusyData))
		return
	}
	responses, syncToken, err := h.calendarReportResponses(r.Context(), user, cal, h.principalURL(user), canonicalPath, resourceName, report, r)
	if err != nil {
		if errors.Is(err, store.ErrNotFound) {
			http.Error(w, "calendar object not found", http.StatusNotFound)
			return
		}
		writeReportError(w, err)
		return
	}
	h.writeBoundedMultiStatus(w, newMultistatus(responses, syncToken))
}

// writeReportError answers a failed REPORT. The sentinels are shared by the
// calendar, birthday and address book paths, so the mapping from one to its
// status and condition lives in one place; a caller with an answer of its own
// for a given failure handles that failure before delegating here.
func writeReportError(w http.ResponseWriter, err error) {
	switch {
	case errors.Is(err, errNumberOfMatchesExceeded):
		writeNumberOfMatchesWithinLimits(w)
	case errors.Is(err, errTooManyHrefs), errors.Is(err, errTooManyCandidateRows):
		http.Error(w, http.StatusText(http.StatusInsufficientStorage), http.StatusInsufficientStorage)
	case errors.Is(err, errUnsupportedReport):
		writeDAVError(w, http.StatusForbidden, "supported-report")
	case errors.Is(err, errInvalidSyncToken):
		writeDAVError(w, http.StatusForbidden, "valid-sync-token")
	default:
		// The message is fixed rather than taken from err, which carries
		// storage and query detail a client has no business reading.
		http.Error(w, "failed to build report response", http.StatusInternalServerError)
	}
}

func (h *DavServer) calendarMultigetHrefIdentifiesResource(ctx context.Context, user *store.User, calendarID int64, resourceName, href string, r *http.Request) (bool, error) {
	resolved, matched := h.resolveCalendarHrefForRequest(href, r)
	if !matched {
		return false, nil
	}
	hrefCalendarID, matched, err := h.resolveCalendarID(ctx, user, resolved.Segment)
	if err != nil {
		if errors.Is(err, store.ErrNotFound) || errors.Is(err, errAmbiguousCalendar) {
			return false, nil
		}
		return false, err
	}
	return matched && hrefCalendarID == calendarID && resolved.ResourceName == resourceName, nil
}

func (h *DavServer) reportBirthdayCalendar(w http.ResponseWriter, r *http.Request, user *store.User, cleanPath, targetResource string, report reportRequest) {
	if calendarQueryExcludedByDepth(r, report, targetResource) {
		h.writeBoundedMultiStatus(w, newMultistatus(nil, ""))
		return
	}
	if report.XMLName.Local == "free-busy-query" {
		events, err := h.generateBirthdayEvents(r.Context(), user.ID)
		if err != nil {
			http.Error(w, "failed to generate birthday events", http.StatusInternalServerError)
			return
		}
		// Free-busy carries no CALDAV:timezone element of its own, and the
		// generated birthday collection defines no CALDAV:calendar-timezone, so
		// §7.3 leaves UTC as the only source for a floating value.
		var candidates []freeBusyCandidate
		if !freeBusyExcludedByDepth(r, report) {
			candidates = filterFreeBusyCandidatesByTimeRange(freeBusyCandidates(events, floatingZone{}), report.TimeRange)
		}
		freeBusyData := h.generateFreeBusy(candidates, report.TimeRange)
		w.Header().Set("Content-Type", "text/calendar")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(freeBusyData))
		return
	}

	responses, syncToken, err := h.birthdayCalendarReportResponses(r.Context(), user, h.principalURL(user), cleanPath, targetResource, report, r)
	if err != nil {
		if errors.Is(err, store.ErrNotFound) {
			http.Error(w, "calendar object not found", http.StatusNotFound)
			return
		}
		writeReportError(w, err)
		return
	}
	h.writeBoundedMultiStatus(w, newMultistatus(responses, syncToken))
}

func (h *DavServer) reportAddressBook(w http.ResponseWriter, r *http.Request, user *store.User, cleanPath string, report reportRequest) {
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
			// RFC 3253 §3.6 spells the decline of a report a resource does not
			// support: DAV:supported-report under a top-level DAV:error, the
			// same answer the calendar path above gives.
			writeDAVError(w, http.StatusForbidden, "supported-report")
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
	responses, syncToken, err := h.addressBookReportResponses(r.Context(), user, book, h.principalURL(user), cleanPath, report, r)
	if err != nil {
		writeReportError(w, err)
		return
	}
	h.writeBoundedMultiStatus(w, newMultistatus(responses, syncToken))
}
