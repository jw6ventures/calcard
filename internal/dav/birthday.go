package dav

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"math"
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

// listBoundedBirthdayContacts reads the contacts the generated collection is
// built from, under the same row budget a stored collection is read with. The
// set spans every address book the user owns, so it is the one report read
// whose size is not bounded by a single collection -- and the one no keyset
// column orders cheaply, which is why it is read whole under a cap rather than
// a page at a time. One row past the budget is enough to know the report cannot
// answer over the complete set.
func (h *DavServer) listBoundedBirthdayContacts(ctx context.Context, userID int64) ([]store.Contact, error) {
	rowLimit := h.reportCandidateRowLimit()
	// A budget turned off is carried as math.MaxInt, which has no room for the
	// extra row. Asking for the budget itself then reads one row short of
	// knowing the set is complete, which no collection can reach anyway.
	readLimit := rowLimit
	if readLimit < math.MaxInt {
		readLimit++
	}
	contacts, err := h.store.Contacts.ListWithBirthdaysByUserLimit(ctx, userID, readLimit)
	if err != nil {
		return nil, err
	}
	if len(contacts) > rowLimit {
		return nil, errTooManyCandidateRows
	}
	return contacts, nil
}

func (h *DavServer) generateBirthdayEvents(ctx context.Context, userID int64) ([]store.Event, error) {
	contacts, err := h.listBoundedBirthdayContacts(ctx, userID)
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
	switch report.XMLName.Local {
	case "calendar-multiget", "calendar-query", "sync-collection":
	default:
		// RFC 3253 §3.6: unknown report types must be refused, not answered
		// with a full dump of the collection. Refused ahead of the read as
		// well, so an unknown report neither generates the collection nor
		// answers a capacity failure in place of the refusal it is owed.
		return nil, "", errUnsupportedReport
	}
	events, err := h.generateBirthdayEvents(ctx, user.ID)
	if err != nil {
		if errors.Is(err, errTooManyCandidateRows) {
			// The row budget refuses the read this collection is generated
			// from, which is a capacity failure of the whole report. §7.8 gives
			// calendar-query the DAV:number-of-matches-within-limits
			// postcondition for one; §7.9 and RFC 6578 give the other two
			// reports none, so they keep the RFC 4918 capacity status.
			if report.XMLName.Local == "calendar-query" {
				return nil, "", errNumberOfMatchesExceeded
			}
			return nil, "", err
		}
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
		res, err := h.birthdayCalendarMultiGet(ctx, user, events, report.Hrefs, collectionPath, targetResource, report.selector, birthdayCalendarDataProjection(report), request)
		return res, "", err
	case "calendar-query":
		if report.Filter != nil {
			// The collection is generated rather than stored, so it defines no
			// CALDAV:calendar-timezone of its own and §7.3 resolves floating
			// values against the request's CALDAV:timezone, else UTC. Its
			// entries are DTSTART;VALUE=DATE, so the zone decides which instants
			// the implied day covers.
			events = applyCalendarFilter(events, report.Filter, reportFloatingZone(report.Timezone, nil))
		}
		res, err := h.calendarResourceReportResponses(ctx, user, collectionPath, events, report.selector, birthdayCalendarDataProjection(report))
		return res, "", err
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
		projection := birthdayCalendarDataProjection(report)
		responses := []response{
			birthdayCalendarCollection(collectionHref, principalHref),
		}
		resourceResponses := rawCalendarResourceReportResponsesLimit(collectionHref, events, projection, h.multistatusBuildLimit()-len(responses))
		responses = h.appendMultistatusResponses(responses, resourceResponses)
		responses, err = h.finishCalendarReportResponses(ctx, user, responses, propertySelector{Prop: report.Prop}, projection.requested())
		if err != nil {
			return nil, "", err
		}
		return responses, syncToken, nil
	default:
		// Unreachable: the guard above refuses every other report type.
		return nil, "", errUnsupportedReport
	}
}

// birthdayCalendarDataProjection is the §9.6 selection for the generated
// collection. It defines no CALDAV:calendar-timezone of its own, so §7.3 leaves
// the request's CALDAV:timezone as the only source ahead of UTC.
func birthdayCalendarDataProjection(report reportRequest) calendarDataProjection {
	return newCalendarDataProjection(reportCalendarData(report), reportFloatingZone(report.Timezone, nil))
}

func (h *DavServer) birthdayCalendarMultiGet(ctx context.Context, user *store.User, events []store.Event, hrefs []string, collectionPath, targetResource string, selector propertySelector, projection calendarDataProjection, request *http.Request) ([]response, error) {
	// §7.9 owes one DAV:response per href here as it does over a stored
	// collection, so an href list past the limit is refused rather than trimmed.
	if len(hrefs) > h.multigetHrefLimit() {
		return nil, errTooManyHrefs
	}

	eventsByUID := make(map[string]store.Event)
	for _, ev := range events {
		eventsByUID[ev.UID] = ev
	}

	var responses []response
	for _, href := range hrefs {
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
		responses = append(responses, rawCalendarResourceReportResponse(responseHref, ev, projection))
	}
	return h.finishCalendarReportResponses(ctx, user, responses, selector, projection.requested())
}
