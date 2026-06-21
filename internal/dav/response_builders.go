package dav

import (
	"strings"

	"github.com/jw6ventures/calcard/internal/store"
)

func calendarResourceResponses(base string, events []store.Event) []response {
	return calendarResourceResponsesWithData(base, events, true)
}

func eventResourceName(ev store.Event) string {
	if ev.ResourceName != "" {
		return ev.ResourceName
	}
	return ev.UID
}

func contactResourceName(contact store.Contact) string {
	if contact.ResourceName != "" {
		return contact.ResourceName
	}
	return contact.UID
}

func calendarResourceResponsesFiltered(base string, events []store.Event, calData *calendarDataEl) []response {
	baseHref := strings.TrimSuffix(base, "/") + "/"
	var responses []response
	for _, ev := range events {
		href := baseHref + eventResourceName(ev) + ".ics"
		rawData := filterICalendarData(ev.RawICAL, calData)
		responses = append(responses, resourceResponse(href, etagProp(ev.ETag, rawData, true)))
	}
	return responses
}

func calendarResourceResponsesWithData(base string, events []store.Event, includeData bool) []response {
	baseHref := strings.TrimSuffix(base, "/") + "/"
	var responses []response
	for _, ev := range events {
		href := baseHref + eventResourceName(ev) + ".ics"
		responses = append(responses, resourceResponse(href, etagPropWithData(ev.ETag, ev.RawICAL, true, includeData)))
	}
	return responses
}

func addressBookResourceResponses(base string, contacts []store.Contact) []response {
	baseHref := strings.TrimSuffix(base, "/") + "/"
	var responses []response
	for _, c := range contacts {
		href := baseHref + contactResourceName(c) + ".vcf"
		responses = append(responses, resourceResponse(href, etagProp(c.ETag, c.RawVCard, false)))
	}
	return responses
}
