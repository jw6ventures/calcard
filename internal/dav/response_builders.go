package dav

import (
	"context"
	"net/url"
	"strings"

	"github.com/jw6ventures/calcard/internal/store"
)

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

// calendarObjectHref and addressObjectHref name one object resource inside a
// collection. A resource name comes from the last path segment of the PUT that
// created it, so it can hold a character no URI path segment may carry
// literally; RFC 4918 §8.3 makes DAV:href a URI, so it is percent-encoded here.
// Every PROPFIND and REPORT goes through these two, because a client matches
// the hrefs one of them returns against the hrefs another returns for the same
// resource.
func calendarObjectHref(collectionHref, resourceName string) string {
	return objectHref(collectionHref, resourceName, ".ics")
}

func addressObjectHref(collectionHref, resourceName string) string {
	return objectHref(collectionHref, resourceName, ".vcf")
}

func objectHref(collectionHref, resourceName, extension string) string {
	return strings.TrimSuffix(collectionHref, "/") + "/" + url.PathEscape(resourceName) + extension
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
		href := calendarObjectHref(baseHref, eventResourceName(ev))
		rawData := filterICalendarData(ev.RawICAL, calData)
		responses = append(responses, resourceResponse(href, etagProp(ev.ETag, rawData, true)))
	}
	return responses
}

func (h *DavServer) calendarResourceReportResponses(ctx context.Context, user *store.User, base string, events []store.Event, selector propertySelector, calData *calendarDataEl) ([]response, error) {
	responses := rawCalendarResourceReportResponsesLimit(base, events, calData, h.multistatusBuildLimit())
	return h.finishCalendarReportResponses(ctx, user, responses, selector, calData != nil)
}

func rawCalendarResourceReportResponsesLimit(base string, events []store.Event, calData *calendarDataEl, limit int) []response {
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
		href := calendarObjectHref(baseHref, eventResourceName(ev))
		responses = append(responses, rawCalendarResourceReportResponse(href, ev, calData))
	}
	return responses
}

func rawCalendarResourceReportResponse(href string, event store.Event, calData *calendarDataEl) response {
	rawData := filterICalendarData(event.RawICAL, calData)
	return resourceResponse(href, etagProp(event.ETag, rawData, true))
}

// propfindRequestForReport turns a report's property selector into the
// PROPFIND-shaped request the response filter runs on. RFC 4791 §9.5 and §9.10
// admit DAV:allprop and DAV:propname beside DAV:prop, so all three reach the
// same filter rather than only the one the tag decoder used to model.
func propfindRequestForReport(selector propertySelector, calendarData bool, addressData *addressDataQuery) *propfindRequest {
	switch {
	case selector.AllProp:
		return &propfindRequest{AllProp: &struct{}{}}
	case selector.PropName:
		return &propfindRequest{PropName: &struct{}{}}
	case selector.Prop == nil:
		return nil
	}
	requested := selector.Prop
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

func (h *DavServer) finishReportResponses(ctx context.Context, user *store.User, responses []response, selector propertySelector, calendarData bool, addressData *addressDataQuery) ([]response, error) {
	responses, limitExceeded := h.capMultistatusResponses(responses)
	if limitExceeded {
		return responses, nil
	}
	request := propfindRequestForReport(selector, calendarData, addressData)
	if request == nil {
		return responses, nil
	}
	if err := h.decoratePropfindResponses(ctx, nil, user, responses, decorationMaskFor(request)); err != nil {
		return nil, err
	}
	switch {
	case request.AllProp != nil:
		// RFC 4918 §9.1 allprop, with the CalDAV and CardDAV exclusions the two
		// specifications state for it; CALDAV:calendar-data is a report
		// selector rather than a property, so it goes too.
		stripCalendarAllprop(responses)
		stripAddressBookAllprop(responses)
	case request.PropName != nil:
		for i := range responses {
			if len(responses[i].Propstat) != 0 {
				responses[i] = propnamePropfindResponse(responses[i])
			}
		}
	default:
		for i := range responses {
			if len(responses[i].Propstat) != 0 {
				responses[i] = filterNonPrincipalPropfindResponse(responses[i], request)
			}
		}
	}
	return responses, nil
}

func (h *DavServer) finishCalendarReportResponses(ctx context.Context, user *store.User, responses []response, selector propertySelector, calendarData bool) ([]response, error) {
	if !selector.AllProp && !selector.PropName && selector.Prop == nil {
		for i := range responses {
			if len(responses[i].Propstat) == 0 {
				continue
			}
			responses[i].Propstat = nil
			responses[i].Status = httpStatusOK
		}
	}
	return h.finishReportResponses(ctx, user, responses, selector, calendarData, nil)
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
		href := addressObjectHref(baseHref, contactResourceName(c))
		responses = append(responses, resourceResponse(href, etagProp(c.ETag, c.RawVCard, false)))
	}
	return responses
}
