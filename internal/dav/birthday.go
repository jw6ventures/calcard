package dav

import (
	"context"
	"crypto/sha256"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/jw6ventures/calcard/internal/store"
)

const birthdayCalendarName = "Birthdays"
const birthdayCalendarDescription = "Contact birthdays from your address books"

func birthdayCalendarHref() string {
	return ensureCollectionHref(fmt.Sprintf("/dav/calendars/%d", birthdayCalendarID))
}

func birthdayCalendarSyncToken() string {
	return buildSyncToken("cal", birthdayCalendarID, time.Unix(0, 0))
}

func birthdayCalendarCollection(href, principalHref string) response {
	description := birthdayCalendarDescription
	return calendarCollectionResponse(href, birthdayCalendarName, store.Calendar{Description: &description}, principalHref, birthdayCalendarSyncToken(), "0", true)
}

func isBirthdayCalendarTarget(target davTarget) bool {
	if !target.Valid || target.Domain != davPathCalendar {
		return false
	}
	calendarID, err := strconv.ParseInt(target.CollectionSegment, 10, 64)
	return err == nil && calendarID == birthdayCalendarID
}

func isBirthdayCalendarPath(ctx context.Context, rawPath string) bool {
	return isBirthdayCalendarTarget(parsedDAVTarget(ctx, rawPath))
}

func isBirthdayCalendarMutation(method string) bool {
	switch method {
	case http.MethodPut, http.MethodDelete, "MKCOL", "MKCALENDAR", "COPY", "MOVE", "LOCK", "UNLOCK", "ACL":
		return true
	default:
		return false
	}
}

func rejectBirthdayCalendarMutation(w http.ResponseWriter, r *http.Request) bool {
	if r == nil || !isBirthdayCalendarMutation(r.Method) || !isBirthdayCalendarPath(r.Context(), r.URL.Path) {
		return false
	}
	http.Error(w, "birthday calendar is read-only", http.StatusForbidden)
	return true
}

func (h *DavServer) generateBirthdayEvents(ctx context.Context, userID int64) ([]store.Event, error) {
	contacts, err := h.store.Contacts.ListWithBirthdaysByUser(ctx, userID)
	if err != nil {
		return nil, err
	}

	now := time.Now()
	currentYear := now.Year()
	var events []store.Event

	for _, c := range contacts {
		if c.Birthday == nil {
			continue
		}

		displayName := "Unknown"
		if c.DisplayName != nil {
			displayName = *c.DisplayName
		}

		// Generate UID for this birthday event (based on contact UID to be stable)
		uid := fmt.Sprintf("birthday-%s@calcard", c.UID)

		// No age in the summary: the event recurs yearly, so a baked-in
		// "(turning N)" would be wrong every year after the first.
		summary := fmt.Sprintf("🎂 %s's Birthday", displayName)

		startYear := currentYear
		birthdayThisYear := time.Date(currentYear, c.Birthday.Month(), c.Birthday.Day(), 23, 59, 59, 0, time.UTC)
		if birthdayThisYear.Before(now) {
			startYear = currentYear + 1
		}

		dtstart := time.Date(startYear, c.Birthday.Month(), c.Birthday.Day(), 0, 0, 0, 0, time.UTC)
		dtstartStr := dtstart.Format("20060102")

		// Build the iCal event with yearly recurrence
		var sb strings.Builder
		sb.WriteString("BEGIN:VCALENDAR\r\n")
		sb.WriteString("VERSION:2.0\r\n")
		sb.WriteString("PRODID:-//CalCard//Birthdays//EN\r\n")
		sb.WriteString("BEGIN:VEVENT\r\n")
		sb.WriteString(fmt.Sprintf("UID:%s\r\n", uid))
		// DTSTAMP derives from the contact so the generated iCal (and its
		// ETag) stays stable across requests; time.Now() here would force
		// clients to re-download every birthday on every sync.
		dtstamp := c.LastModified.UTC()
		if c.LastModified.IsZero() {
			dtstamp = time.Unix(0, 0).UTC()
		}
		sb.WriteString(fmt.Sprintf("DTSTAMP:%s\r\n", dtstamp.Format("20060102T150405Z")))
		sb.WriteString(fmt.Sprintf("DTSTART;VALUE=DATE:%s\r\n", dtstartStr))
		sb.WriteString(fmt.Sprintf("SUMMARY:%s\r\n", escapeICalText(summary)))
		sb.WriteString("RRULE:FREQ=YEARLY\r\n")  // Recurring yearly
		sb.WriteString("TRANSP:TRANSPARENT\r\n") // Free/busy: free time
		sb.WriteString("CLASS:PUBLIC\r\n")

		// RFC 5545 §3.8.8.2: a non-standard property the server defines for its
		// own use carries a vendor id, so it cannot collide with another
		// implementation's property of the same purpose.
		sb.WriteString("X-CALCARD-TYPE:BIRTHDAY\r\n")
		sb.WriteString(fmt.Sprintf("X-CALCARD-CONTACT-UID:%s\r\n", c.UID))

		sb.WriteString("END:VEVENT\r\n")
		sb.WriteString("END:VCALENDAR\r\n")

		rawICAL := sb.String()
		etag := fmt.Sprintf("%x", sha256.Sum256([]byte(rawICAL)))

		events = append(events, store.Event{
			ID:           0, // Virtual event, no DB ID
			CalendarID:   birthdayCalendarID,
			UID:          uid,
			RawICAL:      rawICAL,
			ETag:         etag,
			Summary:      &summary,
			DTStart:      &dtstart,
			DTEnd:        nil,
			AllDay:       true,
			LastModified: c.LastModified,
		})
	}

	return events, nil
}

func escapeICalText(s string) string {
	s = strings.ReplaceAll(s, "\\", "\\\\")
	s = strings.ReplaceAll(s, ";", "\\;")
	s = strings.ReplaceAll(s, ",", "\\,")
	s = strings.ReplaceAll(s, "\n", "\\n")
	return s
}

// birthdayCalendarReportResponses runs one REPORT against the virtual birthday
// collection. targetResource names a single generated resource when the
// Request-URI is an object resource rather than the collection.
func (h *DavServer) birthdayCalendarReportResponses(ctx context.Context, user *store.User, principalHref, cleanPath, targetResource string, report reportRequest, request *http.Request) ([]response, string, error) {
	events, err := h.generateBirthdayEvents(ctx, user.ID)
	if err != nil {
		return nil, "", fmt.Errorf("failed to generate birthday events")
	}
	// Response hrefs are built from the collection, so an object-resource
	// Request-URI narrows the candidate set instead of moving the base href.
	collectionPath := cleanPath
	if targetResource != "" {
		events = eventsWithResourceName(events, targetResource)
		if len(events) == 0 {
			// RFC 4791 §7: the Request-URI names no resource here, so there is
			// nothing for the report to run against.
			return nil, "", store.ErrNotFound
		}
		collectionPath = birthdayCalendarHref()
	}

	switch report.XMLName.Local {
	case "calendar-multiget":
		res, err := h.birthdayCalendarMultiGet(ctx, user, events, report.Hrefs, collectionPath, targetResource, report.selector, reportCalendarData(report), request)
		return res, "", err
	case "calendar-query":
		if report.Filter != nil {
			events = h.applyCalendarFilter(events, report.Filter)
		}
		res, err := h.calendarResourceReportResponses(ctx, user, collectionPath, events, report.selector, reportCalendarData(report))
		return res, "", err
	case "free-busy-query":
		if report.Filter != nil {
			events = h.applyCalendarFilter(events, report.Filter)
		}
		if report.TimeRange != nil {
			events = h.filterCalendarEventsByTimeRange(events, report.TimeRange)
		}
		freeBusyData := h.generateFreeBusy(events, report.Filter, report.TimeRange)
		href := strings.TrimSuffix(cleanPath, "/") + "/freebusy.ics"
		etag := fmt.Sprintf("%x", sha256.Sum256([]byte(freeBusyData)))
		return []response{resourceResponse(href, etagProp(etag, freeBusyData, true))}, "", nil
	case "sync-collection":
		if report.SyncToken != "" {
			info, err := parseSyncToken(report.SyncToken)
			if err != nil || info.Kind != "cal" || info.ID != birthdayCalendarID {
				return nil, "", errInvalidSyncToken
			}
		}
		collectionHref := strings.TrimSuffix(cleanPath, "/") + "/"
		// Use a stable sync-token (epoch time) since we always return all events
		syncToken := birthdayCalendarSyncToken()
		calData := reportCalendarData(report)
		responses := []response{
			birthdayCalendarCollection(collectionHref, principalHref),
		}
		resourceResponses := rawCalendarResourceReportResponsesLimit(collectionHref, events, calData, h.multistatusBuildLimit()-len(responses))
		responses = h.appendMultistatusResponses(responses, resourceResponses)
		responses, err = h.finishCalendarReportResponses(ctx, user, responses, propertySelector{Prop: report.Prop}, calData != nil)
		if err != nil {
			return nil, "", err
		}
		return responses, syncToken, nil
	default:
		// RFC 3253 §3.6: unknown report types must be refused, not answered
		// with a full dump of the collection.
		return nil, "", errUnsupportedReport
	}
}

func (h *DavServer) birthdayCalendarMultiGet(ctx context.Context, user *store.User, events []store.Event, hrefs []string, collectionPath, targetResource string, selector propertySelector, calData *calendarDataEl, request *http.Request) ([]response, error) {
	eventsByUID := make(map[string]store.Event)
	for _, ev := range events {
		eventsByUID[ev.UID] = ev
	}

	var responses []response
	for _, href := range hrefs {
		if h.multistatusBuildComplete(responses) {
			break
		}
		resolved, ok := h.resolveCalendarHrefForRequest(href, request)
		uid := resolved.ResourceName
		// The birthday collection is addressed only by its constant ID, never by
		// a slug, so no repository lookup can resolve its segment.
		id, idErr := strconv.ParseInt(resolved.Segment, 10, 64)
		// RFC 4791 §7.9: an unresolvable or out-of-scope href still owes the client a DAV:response.
		if !ok || idErr != nil || id != birthdayCalendarID || !multigetHrefInScope(targetResource, uid) {
			responses = append(responses, response{Href: multiGetFallbackHref(href, resolved.Path, collectionPath), Status: httpStatusNotFound})
			continue
		}
		// An in-scope href names a resource this collection spells one way, so
		// the response carries that spelling rather than the client's.
		responseHref := calendarObjectHref(collectionPath, uid)
		ev, found := eventsByUID[uid]
		if !found {
			responses = append(responses, response{Href: responseHref, Status: httpStatusNotFound})
			continue
		}
		responses = append(responses, rawCalendarResourceReportResponse(responseHref, ev, calData))
	}
	return h.finishCalendarReportResponses(ctx, user, responses, selector, calData != nil)
}
