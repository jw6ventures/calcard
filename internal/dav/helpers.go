package dav

import (
	"fmt"
	"strings"
)

// Shared helpers for building DAV responses.

func collectionResponse(href, name string) response {
	return response{
		Href:     href,
		Propstat: []propstat{statusOKProp(name, resourceType{Collection: &struct{}{}})},
	}
}

func calendarCollectionResponse(href, name string, description, timezone *string, principalHref, syncToken, ctag string, readOnly bool) response {
	resp := response{
		Href:     href,
		Propstat: []propstat{statusOKPropWithExtras(name, resourceType{Collection: &struct{}{}, Calendar: &struct{}{}}, principalHref, true, false)},
	}
	p := &resp.Propstat[0].Prop
	if syncToken != "" {
		p.SyncToken = syncToken
	}
	if ctag != "" {
		p.CTag = ctag
	}
	if description != nil && *description != "" {
		p.CalendarDescription = *description
	}
	p.CalendarTimezone = calendarTimezoneValue(timezone)
	p.SupportedCalendarComponentSet = supportedCalendarComponents()
	p.SupportedCalendarData = supportedCalendarDataProp()
	p.ScheduleCalendarTransp = &scheduleCalendarTransp{Opaque: &struct{}{}}
	p.CurrentUserPrivilegeSet = calendarCurrentUserPrivilegeSet()

	p.MaxResourceSize = fmt.Sprintf("%d", maxDAVBodyBytes)
	p.MinDateTime = caldavMinDateTime
	p.MaxDateTime = caldavMaxDateTime
	p.MaxInstances = fmt.Sprintf("%d", caldavMaxInstances)
	p.MaxAttendeesPerInstance = fmt.Sprintf("%d", caldavMaxAttendees)

	if readOnly {
		p.CalendarServerReadOnly = &struct{}{}
	}

	return resp
}

func addressBookCollectionResponse(href, name string, description *string, principalHref, syncToken, ctag string) response {
	resp := response{
		Href:     href,
		Propstat: []propstat{statusOKPropWithExtras(name, resourceType{Collection: &struct{}{}, AddressBook: &struct{}{}}, principalHref, false, true)},
	}
	p := &resp.Propstat[0].Prop
	if syncToken != "" {
		p.SyncToken = syncToken
	}
	if ctag != "" {
		p.CTag = ctag
	}
	if description != nil && *description != "" {
		p.AddressBookDesc = *description
	}
	return resp
}

func statusOKProp(name string, rtype resourceType) propstat {
	return propstat{
		Prop: prop{
			DisplayName:  name,
			ResourceType: rtype,
		},
		Status: httpStatusOK,
	}
}

func statusOKPropWithExtras(name string, rtype resourceType, principalHref string, includeCalendarHome, includeAddressHome bool) propstat {
	p := prop{
		DisplayName:          name,
		ResourceType:         rtype,
		CurrentUserPrincipal: &hrefProp{Href: principalHref},
	}
	if includeCalendarHome {
		p.CalendarHomeSet = &hrefListProp{Href: []string{"/dav/calendars/"}}
		p.SupportedReportSet = calendarSupportedReports()
	}
	if includeAddressHome {
		p.AddressbookHomeSet = &hrefListProp{Href: []string{"/dav/addressbooks/"}}
		p.SupportedReportSet = addressbookSupportedReports()
	}
	if !includeCalendarHome && !includeAddressHome {
		p.SupportedReportSet = combinedSupportedReports()
	}
	return propstat{Prop: p, Status: httpStatusOK}
}

func etagProp(etag, data string, calendar bool) propstat {
	return etagPropWithData(etag, data, calendar, true)
}

func etagPropWithData(etag, data string, calendar bool, includeData bool) propstat {
	propVal := prop{GetETag: "\"" + etag + "\""}
	if includeData {
		if calendar {
			propVal.CalendarData = cdataString(data)
			propVal.GetContentType = "text/calendar; charset=utf-8"
		} else {
			propVal.AddressData = cdataString(data)
			propVal.GetContentType = "text/vcard; charset=utf-8"
		}
	}
	return propstat{Prop: propVal, Status: httpStatusOK}
}

func calendarResourcePropstat(etag, data string, includeData bool) propstat {
	ps := etagPropWithData(etag, data, true, includeData)
	ps.Prop.SupportedReportSet = &supportedReportSet{}
	return ps
}

func resourceResponse(href string, ps propstat) response {
	return response{Href: href, Propstat: []propstat{ps}}
}

func deletedResponse(href string) response {
	return response{Href: href, Status: httpStatusNotFound}
}

func calendarSupportedReports() *supportedReportSet {
	return &supportedReportSet{
		Reports: []supportedReport{
			{Report: reportType{CalendarMultiGet: &struct{}{}}},
			{Report: reportType{CalendarQuery: &struct{}{}}},
			{Report: reportType{FreeBusyQuery: &struct{}{}}},
			{Report: reportType{SyncCollection: &struct{}{}}},
			{Report: reportType{ExpandProperty: &struct{}{}}},
		},
	}
}

func addressbookSupportedReports() *supportedReportSet {
	return &supportedReportSet{
		Reports: []supportedReport{
			{Report: reportType{AddressbookMultiGet: &struct{}{}}},
			{Report: reportType{AddressbookQuery: &struct{}{}}},
			{Report: reportType{SyncCollection: &struct{}{}}},
			{Report: reportType{ExpandProperty: &struct{}{}}},
		},
	}
}

func combinedSupportedReports() *supportedReportSet {
	return &supportedReportSet{
		Reports: []supportedReport{
			{Report: reportType{CalendarMultiGet: &struct{}{}}},
			{Report: reportType{CalendarQuery: &struct{}{}}},
			{Report: reportType{AddressbookMultiGet: &struct{}{}}},
			{Report: reportType{AddressbookQuery: &struct{}{}}},
			{Report: reportType{SyncCollection: &struct{}{}}},
			{Report: reportType{ExpandProperty: &struct{}{}}},
		},
	}
}

func supportedCalendarComponents() *supportedCalendarComponentSet {
	return &supportedCalendarComponentSet{
		Comps: []comp{
			{Name: "VEVENT"},
			{Name: "VTODO"},
			{Name: "VJOURNAL"},
			{Name: "VFREEBUSY"},
		},
	}
}

func supportedCalendarDataProp() *supportedCalendarData {
	return &supportedCalendarData{
		CalendarData: []calendarDataType{
			{ContentType: "text/calendar", Version: "2.0"},
		},
	}
}

func calendarCurrentUserPrivilegeSet() *currentUserPrivilegeSet {
	return &currentUserPrivilegeSet{
		Privileges: []privilege{
			{Read: &readPrivilege{ReadFreeBusy: &struct{}{}}},
			{ReadFreeBusy: &struct{}{}},
		},
	}
}

func calendarTimezoneValue(tz *string) *string {
	if tz == nil || strings.TrimSpace(*tz) == "" {
		defaultTZ := defaultCalendarTimezone
		return &defaultTZ
	}
	return tz
}

const defaultCalendarTimezone = "BEGIN:VTIMEZONE\nTZID:UTC\nBEGIN:STANDARD\nDTSTART:19700101T000000Z\nTZOFFSETFROM:+0000\nTZOFFSETTO:+0000\nTZNAME:UTC\nEND:STANDARD\nEND:VTIMEZONE"
