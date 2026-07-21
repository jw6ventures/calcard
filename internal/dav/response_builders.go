package dav

import (
	"context"
	"strings"

	"github.com/jw6ventures/calcard/internal/store"
)

func calendarResourceResponses(base string, events []store.Event) []response {
	return calendarResourceResponsesFilteredLimit(base, events, nil, len(events))
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
	return calendarResourceResponsesFilteredLimit(base, events, calData, len(events))
}

func calendarResourceResponsesFilteredLimit(base string, events []store.Event, calData *calendarDataEl, limit int) []response {
	baseHref := strings.TrimSuffix(base, "/") + "/"
	if limit <= 0 {
		return nil
	}
	if limit > len(events) {
		limit = len(events)
	}
	responses := make([]response, 0, limit)
	for _, ev := range events {
		if len(responses) >= limit {
			break
		}
		href := baseHref + eventResourceName(ev) + ".ics"
		rawData := filterICalendarData(ev.RawICAL, calData)
		responses = append(responses, resourceResponse(href, etagProp(ev.ETag, rawData, true)))
	}
	return responses
}

func (h *DavServer) calendarResourceReportResponses(ctx context.Context, user *store.User, base string, events []store.Event, requested *reportProp, calData *calendarDataEl) ([]response, error) {
	responses := rawCalendarResourceReportResponsesLimit(base, events, requested, calData, h.multistatusBuildLimit())
	return h.finishReportResponses(ctx, user, responses, requested, calData != nil, nil)
}

func rawCalendarResourceReportResponses(base string, events []store.Event, requested *reportProp, calData *calendarDataEl) []response {
	return rawCalendarResourceReportResponsesLimit(base, events, requested, calData, len(events))
}

func rawCalendarResourceReportResponsesLimit(base string, events []store.Event, requested *reportProp, calData *calendarDataEl, limit int) []response {
	baseHref := strings.TrimSuffix(base, "/") + "/"
	if limit <= 0 {
		return nil
	}
	if limit > len(events) {
		limit = len(events)
	}
	responses := make([]response, 0, limit)
	for _, ev := range events {
		if len(responses) >= limit {
			break
		}
		href := baseHref + eventResourceName(ev) + ".ics"
		responses = append(responses, rawCalendarResourceReportResponse(href, ev, requested, calData))
	}
	return responses
}

func rawCalendarResourceReportResponse(href string, event store.Event, requested *reportProp, calData *calendarDataEl) response {
	rawData := filterICalendarData(event.RawICAL, calData)
	propertyStatus := etagProp(event.ETag, rawData, true)
	if requested != nil && requested.SupportedReportSet != nil {
		propertyStatus.Prop.SupportedReportSet = &supportedReportSet{}
	}
	return resourceResponse(href, propertyStatus)
}

func propfindRequestForReport(requested *reportProp, calendarData bool, addressData *addressDataQuery) *propfindRequest {
	if requested == nil {
		return nil
	}
	query := &propfindPropQuery{propertySelection: requested.propertySelection}
	if requested.CalendarData != nil || calendarData {
		query.CalendarData = &struct{}{}
	}
	if requested.AddressData != nil {
		query.AddressData = requested.AddressData
	} else if addressData != nil {
		query.AddressData = addressData
	}
	return &propfindRequest{Prop: query}
}

func (h *DavServer) finishReportResponses(ctx context.Context, user *store.User, responses []response, requested *reportProp, calendarData bool, addressData *addressDataQuery) ([]response, error) {
	responses, limitExceeded := h.capMultistatusResponses(responses)
	if limitExceeded {
		return responses, nil
	}
	request := propfindRequestForReport(requested, calendarData, addressData)
	if request == nil {
		return responses, nil
	}
	if err := h.decoratePropfindResponses(ctx, nil, user, responses, decorationMaskFor(request)); err != nil {
		return nil, err
	}
	for i := range responses {
		if len(responses[i].Propstat) != 0 {
			responses[i] = filterNonPrincipalPropfindResponse(responses[i], request)
		}
	}
	return responses, nil
}

func addressBookResourceResponses(base string, contacts []store.Contact) []response {
	return addressBookResourceResponsesLimit(base, contacts, len(contacts))
}

func addressBookResourceResponsesLimit(base string, contacts []store.Contact, limit int) []response {
	baseHref := strings.TrimSuffix(base, "/") + "/"
	if limit <= 0 {
		return nil
	}
	if limit > len(contacts) {
		limit = len(contacts)
	}
	responses := make([]response, 0, limit)
	for _, c := range contacts {
		if len(responses) >= limit {
			break
		}
		href := baseHref + contactResourceName(c) + ".vcf"
		responses = append(responses, resourceResponse(href, etagProp(c.ETag, c.RawVCard, false)))
	}
	return responses
}
