package dav

import (
	"context"
	"crypto/sha256"
	"fmt"
	"strings"
	"time"

	"github.com/jw6ventures/calcard/internal/store"
)

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

		var summaryAge string
		if c.Birthday.Year() > 1900 {
			birthdayThisYear := time.Date(currentYear, c.Birthday.Month(), c.Birthday.Day(), 23, 59, 59, 0, time.UTC)
			var ageAtNextBirthday int
			if birthdayThisYear.After(now) {
				ageAtNextBirthday = currentYear - c.Birthday.Year()
			} else {
				ageAtNextBirthday = (currentYear + 1) - c.Birthday.Year()
			}
			summaryAge = fmt.Sprintf(" (turning %d)", ageAtNextBirthday)
		}
		summary := fmt.Sprintf("🎂 %s's Birthday%s", displayName, summaryAge)

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
		sb.WriteString(fmt.Sprintf("DTSTAMP:%s\r\n", time.Now().UTC().Format("20060102T150405Z")))
		sb.WriteString(fmt.Sprintf("DTSTART;VALUE=DATE:%s\r\n", dtstartStr))
		sb.WriteString(fmt.Sprintf("SUMMARY:%s\r\n", escapeICalText(summary)))
		sb.WriteString("RRULE:FREQ=YEARLY\r\n")  // Recurring yearly
		sb.WriteString("TRANSP:TRANSPARENT\r\n") // Free/busy: free time
		sb.WriteString("CLASS:PUBLIC\r\n")

		// Add X-property to mark this as a birthday event
		sb.WriteString("X-CALCARD-TYPE:BIRTHDAY\r\n")
		sb.WriteString(fmt.Sprintf("X-CONTACT-UID:%s\r\n", c.UID))

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

func (h *DavServer) birthdayCalendarReportResponses(ctx context.Context, user *store.User, principalHref, cleanPath string, report reportRequest) ([]response, string, error) {
	events, err := h.generateBirthdayEvents(ctx, user.ID)
	if err != nil {
		return nil, "", fmt.Errorf("failed to generate birthday events")
	}

	switch report.XMLName.Local {
	case "calendar-multiget":
		res, err := h.birthdayCalendarMultiGet(ctx, events, report.Hrefs, cleanPath)
		return res, "", err
	case "calendar-query":
		if report.Filter != nil {
			events = h.applyCalendarFilter(events, report.Filter)
		}
		return calendarResourceResponses(cleanPath, events), "", nil
	case "free-busy-query":
		if report.Filter != nil {
			events = h.applyCalendarFilter(events, report.Filter)
		}
		freeBusyData := h.generateFreeBusy(events, report.Filter)
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
		syncToken := buildSyncToken("cal", birthdayCalendarID, time.Unix(0, 0))
		birthdayName := "Birthdays"
		birthdayDesc := "Contact birthdays from your address books"
		calData := reportCalendarData(report)
		responses := []response{
			calendarCollectionResponse(collectionHref, birthdayName, &birthdayDesc, nil, nil, principalHref, syncToken, "0", true),
		}
		responses = append(responses, calendarResourceResponsesFiltered(collectionHref, events, calData)...)
		return responses, syncToken, nil
	default:
		// Fallback: return all events
		return calendarResourceResponses(cleanPath, events), "", nil
	}
}

func (h *DavServer) birthdayCalendarMultiGet(ctx context.Context, events []store.Event, hrefs []string, cleanPath string) ([]response, error) {
	if len(hrefs) == 0 {
		return calendarResourceResponses(cleanPath, events), nil
	}

	eventsByUID := make(map[string]store.Event)
	for _, ev := range events {
		eventsByUID[ev.UID] = ev
	}

	var responses []response
	for _, href := range hrefs {
		cleanHref := resolveDAVHref(cleanPath, href)
		if cleanHref == "" {
			continue
		}
		// Birthday calendar uses numeric-only parsing (special virtual calendar with constant ID -1)
		id, uid, ok := parseResourcePath(cleanHref, "/dav/calendars")
		if !ok || id != birthdayCalendarID {
			continue
		}
		ev, found := eventsByUID[uid]
		if !found {
			responses = append(responses, response{Href: cleanHref, Status: httpStatusNotFound})
			continue
		}
		responses = append(responses, resourceResponse(cleanHref, etagProp(ev.ETag, ev.RawICAL, true)))
	}
	return responses, nil
}
