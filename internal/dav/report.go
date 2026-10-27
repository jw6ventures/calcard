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
	"time"

	"github.com/jw6ventures/calcard/internal/auth"
	"github.com/jw6ventures/calcard/internal/ical"
	"github.com/jw6ventures/calcard/internal/store"
)

func (h *DavServer) report(w http.ResponseWriter, r *http.Request) {
	user, ok := auth.UserFromContext(r.Context())
	if !ok {
		http.Error(w, "missing user", http.StatusUnauthorized)
		return
	}

	// The deadline covers everything the report does: the queries and the
	// recurrence work, which stops when the pool built on this context sees it
	// end, not only between resources.
	if timeout := h.reportTimeout(); timeout > 0 {
		ctx, cancel := context.WithTimeout(r.Context(), timeout)
		defer cancel()
		r = r.WithContext(ctx)
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
		if h.answerInterruptedReport(w, r, report.XMLName.Local, err) {
			return
		}
		h.logger().Error("Report", "failed to resolve calendar for %s: %v", cleanPath, err)
		writeServerFailure(w, err, "failed to resolve calendar")
		return
	}
	if !ok {
		http.Error(w, "invalid calendar id", http.StatusBadRequest)
		return
	}
	if resourceName != "" && report.XMLName.Local == "calendar-multiget" {
		equivalent, err := h.calendarMultigetHrefIdentifiesResource(r.Context(), user, calID, resourceName, report.Hrefs[0], r)
		if err != nil {
			if h.answerInterruptedReport(w, r, report.XMLName.Local, err) {
				return
			}
			writeServerFailure(w, err, "failed to resolve calendar-multiget href")
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
		if !collectionLoadRefused(err) && h.answerInterruptedReport(w, r, report.XMLName.Local, err) {
			return
		}
		if report.XMLName.Local == "free-busy-query" {
			_ = writePrivilegeRequirementError(w, requirePrivatePrivilegeAt(err, cleanPath, loadPrivilege))
			return
		}
		if errors.Is(err, errForbidden) || isPrivilegeNotGranted(err) {
			writeNeedPrivileges(w, cleanPath, loadPrivilege)
			return
		}
		if errors.Is(err, store.ErrNotFound) {
			http.Error(w, "calendar not found", http.StatusNotFound)
			return
		}
		h.logger().Error("Report", "failed to load calendar for %s: %v", cleanPath, err)
		writeServerFailure(w, err, "failed to load calendar")
		return
	}
	canonicalPath := path.Join("/dav/calendars", fmt.Sprint(cal.ID))
	if calendarQueryExcludedByDepth(r, report, resourceName) {
		h.writeReportMultiStatus(w, r, report.XMLName.Local, nil, "")
		return
	}
	if report.XMLName.Local == "free-busy-query" {
		// Out of Depth reach is the empty VFREEBUSY §7.10 requires when nothing
		// matches, answered without reading the collection at all.
		var freeBusyData string
		if freeBusyExcludedByDepth(r, report) {
			freeBusyData, _ = h.generateFreeBusy(r.Context(), nil, report.TimeRange)
		} else {
			var err error
			freeBusyData, err = h.freeBusyQuery(r.Context(), user, cal, report.TimeRange)
			if err != nil {
				h.writeReportFailure(w, r, report.XMLName.Local, err)
				return
			}
		}
		if h.reportDeadlineExpired(w, r, report.XMLName.Local) {
			return
		}
		w.Header().Set("Content-Type", "text/calendar")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(freeBusyData))
		return
	}
	answer, err := h.calendarReportResponses(r.Context(), user, cal, h.principalURL(user), canonicalPath, resourceName, report, r)
	if err != nil {
		if errors.Is(err, store.ErrNotFound) {
			http.Error(w, "calendar object not found", http.StatusNotFound)
			return
		}
		h.writeReportFailure(w, r, report.XMLName.Local, err)
		return
	}
	h.writeReportAnswer(w, r, report.XMLName.Local, answer)
}

// writeReportMultiStatus answers a REPORT and records what it answered with.
// A REPORT that returns fewer responses than the client expected is otherwise
// indistinguishable in the log from one the client mishandled, and the report
// name and response count are what separate the two.
func (h *DavServer) writeReportMultiStatus(w http.ResponseWriter, r *http.Request, reportName string, responses []response, syncToken string) {
	if h.reportDeadlineExpired(w, r, reportName) {
		return
	}
	h.logger().Debug("Report", "%s %s returned %d responses", reportName, r.URL.Path, len(responses))
	h.writeBoundedMultiStatus(w, newMultistatus(responses, syncToken))
}

// reportAnswer is what a collection REPORT answers with. encoded, when set, is
// the multistatus already encoded under the byte ceiling: a sync-collection
// page is encoded to learn whether it fits, and is written from that encoding
// rather than encoded a second time.
type reportAnswer struct {
	responses []response
	syncToken string
	encoded   []byte
}

// writeReportAnswer answers a collection REPORT, from its encoding when it
// carries one.
func (h *DavServer) writeReportAnswer(w http.ResponseWriter, r *http.Request, reportName string, answer reportAnswer) {
	if h.reportDeadlineExpired(w, r, reportName) {
		return
	}
	if answer.encoded == nil {
		h.writeReportMultiStatus(w, r, reportName, answer.responses, answer.syncToken)
		return
	}
	h.logger().Debug("Report", "%s %s returned %d responses", reportName, r.URL.Path, len(answer.responses))
	w.Header().Set("Content-Type", "application/xml; charset=utf-8")
	w.WriteHeader(http.StatusMultiStatus)
	_, _ = w.Write(answer.encoded)
}

// writeReportFailure records a refused REPORT under the same component tag the
// answered ones use, so a client that received no responses and one that
// received a refusal are told apart in the log.
func (h *DavServer) writeReportFailure(w http.ResponseWriter, r *http.Request, reportName string, err error) {
	if h.answerInterruptedReport(w, r, reportName, err) {
		return
	}
	h.logger().Error("Report", "%s failed for %s: %v", reportName, r.URL.Path, err)
	writeReportError(w, err)
}

// reportTimeout is how long one REPORT may run, zero for no limit.
func (h *DavServer) reportTimeout() time.Duration {
	if h == nil || h.cfg == nil {
		return 0
	}
	return h.cfg.DAV.ReportTimeout
}

// answerInterruptedReport answers a REPORT that stopped because its context
// ended, and reports whether it did.
//
// net/http ends a request's context with context.Canceled when the client goes
// away. Nobody reads that answer, so the status only records why the report
// stopped, and only the cancellation itself is not the server's failure: one
// that merely coincided with the client leaving is still logged as the failure
// it is.
//
// The deadline report derives ends it with context.DeadlineExceeded instead.
// The client is still waiting on a report the server will not finish, so it is
// owed the answer the report gives a request it will not compute in full:
// DAV:number-of-matches-within-limits, or 507 for the two multiget reports,
// which answer their href limit so. sync-collection is answered the same way rather
// than as a truncated page (RFC 6578 §3.6): a page cut by time can hold no
// change at all, and a client given a continuation token that makes no
// progress asks again forever.
func (h *DavServer) answerInterruptedReport(w http.ResponseWriter, r *http.Request, reportName string, err error) bool {
	switch cause := r.Context().Err(); {
	case errors.Is(cause, context.Canceled):
		if errors.Is(err, context.Canceled) || store.IsQueryCanceled(err) {
			h.logger().Debug("Report", "%s abandoned by the client for %s: %v", reportName, r.URL.Path, err)
		} else {
			h.logger().Error("Report", "%s failed for %s after the client left: %v", reportName, r.URL.Path, err)
		}
		w.WriteHeader(statusClientClosedRequest)
		return true
	case errors.Is(cause, context.DeadlineExceeded):
		if err != nil && !isDeadlineSymptom(err) {
			return false
		}
		h.logger().Error("Report", "%s for %s stopped at its %s deadline: %v", reportName, r.URL.Path, h.reportTimeout(), cause)
		writeReportDeadlineExceeded(w, reportName)
		return true
	}
	return false
}

// isDeadlineSymptom reports whether err is what a report that ran past its
// deadline meets: the context's own error, the database cancelling a query for
// it, a recurrence expansion refused work by a pool whose context ended, or the
// postcondition error a report that owes the whole range turns that refusal
// into (free-busy-query).
func isDeadlineSymptom(err error) bool {
	return errors.Is(err, context.DeadlineExceeded) || store.IsQueryCanceled(err) ||
		errors.Is(err, ical.ErrRecurrenceExpansionLimit) || errors.Is(err, errNumberOfMatchesExceeded)
}

// writeReportDeadlineExceeded answers a REPORT stopped by its deadline.
func writeReportDeadlineExceeded(w http.ResponseWriter, reportName string) {
	if reportName == "calendar-multiget" || reportName == "addressbook-multiget" {
		writeInsufficientStorage(w)
		return
	}
	writeNumberOfMatchesWithinLimits(w)
}

// reportDeadlineExpired answers a REPORT whose deadline passed while it was
// being answered, whether or not anything it ran reported an error: work the
// deadline stopped is answered as unsettled, which is no error, so the report
// that built its answer from it must not be written as a complete one.
func (h *DavServer) reportDeadlineExpired(w http.ResponseWriter, r *http.Request, reportName string) bool {
	if !errors.Is(r.Context().Err(), context.DeadlineExceeded) {
		return false
	}
	return h.answerInterruptedReport(w, r, reportName, nil)
}

// collectionLoadRefused reports whether loading a report's collection failed
// with an answer for the client -- absent, or not theirs to read -- rather
// than a failure of the server's.
func collectionLoadRefused(err error) bool {
	return errors.Is(err, store.ErrNotFound) || errors.Is(err, errForbidden) || isPrivilegeNotGranted(err)
}

// statusClientClosedRequest is the nonstandard 499 proxies such as nginx log
// for a request the client closed before it was answered.
const statusClientClosedRequest = 499

// writeReportError answers a failed REPORT. The sentinels are shared by the
// calendar, birthday and address book paths, so the mapping from one to its
// status and condition lives in one place; a caller with an answer of its own
// for a given failure handles that failure before delegating here.
func writeReportError(w http.ResponseWriter, err error) {
	switch {
	case errors.Is(err, errNumberOfMatchesExceeded):
		writeNumberOfMatchesWithinLimits(w)
	case errors.Is(err, errTooManyHrefs), errors.Is(err, errTooManyCandidateRows), errors.Is(err, errMultistatusTooLarge):
		writeInsufficientStorage(w)
	case errors.Is(err, errUnsupportedReport):
		writeDAVError(w, http.StatusForbidden, "supported-report")
	case errors.Is(err, errInvalidSyncToken):
		writeDAVError(w, http.StatusForbidden, "valid-sync-token")
	default:
		// The message is fixed rather than taken from err, which carries
		// storage and query detail a client has no business reading.
		writeServerFailure(w, err, "failed to build report response")
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
		h.writeReportMultiStatus(w, r, report.XMLName.Local, nil, "")
		return
	}
	if report.XMLName.Local == "free-busy-query" {
		// Out of Depth reach the report publishes nothing, so the collection is
		// not generated at all -- the stored path answers §7.10's empty
		// VFREEBUSY without a read, and a budget this request cannot spend must
		// not refuse it either.
		var candidates []freeBusyCandidate
		if !freeBusyExcludedByDepth(r, report) {
			events, err := h.generateBirthdayEvents(r.Context(), user.ID)
			if err != nil {
				if errors.Is(err, errTooManyCandidateRows) {
					// §7.10 gives free-busy-query the §7.8 postcondition, which
					// the stored path already answers a refused candidate read
					// with.
					h.writeReportFailure(w, r, report.XMLName.Local, errNumberOfMatchesExceeded)
					return
				}
				h.writeReportFailure(w, r, report.XMLName.Local, fmt.Errorf("failed to generate birthday events: %w", err))
				return
			}
			// Free-busy carries no CALDAV:timezone element of its own, and the
			// generated birthday collection defines no
			// CALDAV:calendar-timezone, so §7.3 leaves UTC as the only source
			// for a floating value.
			candidates, err = filterFreeBusyCandidatesByTimeRange(r.Context(), freeBusyCandidates(events, floatingZone{}, davRequestStateFromContext(r.Context()).requestWork(r.Context(), reportWorkBudget)), report.TimeRange)
			if err != nil {
				h.writeReportFailure(w, r, report.XMLName.Local, freeBusyReportError(err))
				return
			}
		}
		freeBusyData, err := h.generateFreeBusy(r.Context(), candidates, report.TimeRange)
		if err != nil {
			h.writeReportFailure(w, r, report.XMLName.Local, freeBusyReportError(err))
			return
		}
		if h.reportDeadlineExpired(w, r, report.XMLName.Local) {
			return
		}
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
		h.writeReportFailure(w, r, report.XMLName.Local, err)
		return
	}
	h.writeReportMultiStatus(w, r, report.XMLName.Local, responses, syncToken)
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
	prepareCardFilter(report.CardFilter)

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
		if h.answerInterruptedReport(w, r, report.XMLName.Local, err) {
			return
		}
		writeServerFailure(w, err, "failed to resolve address book")
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
		if !collectionLoadRefused(err) && h.answerInterruptedReport(w, r, report.XMLName.Local, err) {
			return
		}
		if errors.Is(err, errForbidden) || isPrivilegeNotGranted(err) {
			writeNeedPrivileges(w, cleanPath, "read")
			return
		}
		if errors.Is(err, store.ErrNotFound) {
			http.Error(w, "address book not found", http.StatusNotFound)
			return
		}
		h.logger().Error("Report", "failed to load address book for %s: %v", cleanPath, err)
		writeServerFailure(w, err, "failed to load address book")
		return
	}
	// Depth:0 on a collection for addressbook-query means only the collection
	// itself, not its children — return empty multistatus after access checks.
	depth := strings.TrimSpace(r.Header.Get("Depth"))
	if report.XMLName.Local == "addressbook-query" && !target.Resource && depth == "0" {
		h.writeReportMultiStatus(w, r, report.XMLName.Local, nil, "")
		return
	}
	answer, err := h.addressBookReportResponses(r.Context(), user, book, h.principalURL(user), cleanPath, report, r)
	if err != nil {
		h.writeReportFailure(w, r, report.XMLName.Local, err)
		return
	}
	h.writeReportAnswer(w, r, report.XMLName.Local, answer)
}
