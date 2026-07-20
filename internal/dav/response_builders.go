package dav

import (
	"context"
	"strings"

	"github.com/jw6ventures/calcard/internal/store"
)

func calendarResourceResponses(base string, events []store.Event) []response {
	return calendarResourceResponsesFiltered(base, events, nil)
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

func (h *DavServer) calendarResourceReportResponses(ctx context.Context, user *store.User, base string, events []store.Event, requested *reportProp, calData *calendarDataEl) ([]response, error) {
	baseHref := strings.TrimSuffix(base, "/") + "/"
	var responses []response
	for _, ev := range events {
		href := baseHref + eventResourceName(ev) + ".ics"
		resp, err := h.calendarResourceReportResponse(ctx, user, href, ev, requested, calData)
		if err != nil {
			return nil, err
		}
		responses = append(responses, resp)
	}
	return responses, nil
}

func (h *DavServer) calendarResourceReportResponse(ctx context.Context, user *store.User, href string, event store.Event, requested *reportProp, calData *calendarDataEl) (response, error) {
	rawData := filterICalendarData(event.RawICAL, calData)
	propertyStatus := etagProp(event.ETag, rawData, true)
	if requested != nil && requested.SupportedReport != nil {
		propertyStatus.Prop.SupportedReportSet = &supportedReportSet{}
	}
	resp := resourceResponse(href, propertyStatus)
	request := propfindRequestForReport(requested, calData != nil, nil)
	if request != nil {
		if err := h.decorateDAVProp(ctx, user, href, &resp.Propstat[0].Prop, decorationMaskFor(request)); err != nil {
			return response{}, err
		}
	}
	return filterPropfindResponseForKind(resp, request, kindCalendarObject), nil
}

func propfindRequestForReport(requested *reportProp, calendarData bool, addressData *addressDataQuery) *propfindRequest {
	if requested == nil {
		return nil
	}
	query := &propfindPropQuery{
		DisplayName:            requested.DisplayName,
		ResourceType:           requested.ResourceType,
		GetETag:                requested.GetETag,
		GetContentType:         requested.GetContentType,
		SupportedReportSet:     requested.SupportedReport,
		LockDiscovery:          requested.LockDiscovery,
		SupportedLock:          requested.SupportedLock,
		ACLProp:                requested.ACLProp,
		SupportedPrivilegeSet:  requested.SupportedPrivilegeSet,
		PrincipalCollectionSet: requested.PrincipalCollectionSet,
		CustomXML:              requested.CustomXML,
	}
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

func addressBookResourceResponses(base string, contacts []store.Contact) []response {
	baseHref := strings.TrimSuffix(base, "/") + "/"
	var responses []response
	for _, c := range contacts {
		href := baseHref + contactResourceName(c) + ".vcf"
		responses = append(responses, resourceResponse(href, etagProp(c.ETag, c.RawVCard, false)))
	}
	return responses
}
