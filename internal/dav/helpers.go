package dav

import (
	"mime"
	"slices"
	"strconv"
	"strings"

	"github.com/jw6ventures/calcard/internal/ical"
	"github.com/jw6ventures/calcard/internal/store"
)

// Shared helpers for building DAV responses.

func collectionResponse(href, name string) response {
	return response{
		Href:     href,
		Propstat: []propstat{statusOKProp(name, resourceType{Collection: &struct{}{}})},
	}
}

func birthdayCalendarCollectionResponse(href, name string, cal store.Calendar, principalHref, syncToken, ctag string) response {
	resp := calendarCollectionPropstatResponse(href, name, cal, principalHref, syncToken, ctag)
	p := &resp.Propstat[0].Prop
	p.CurrentUserPrivilegeSet = birthdayCalendarCurrentUserPrivilegeSet()
	p.CalendarServerReadOnly = &struct{}{}
	return resp
}

func calendarCollectionResponseWithPrivileges(href, name string, cal store.Calendar, principalHref, syncToken, ctag string, privileges store.CalendarPrivileges) response {
	privileges = privileges.Normalized()
	resp := calendarCollectionPropstatResponse(href, name, cal, principalHref, syncToken, ctag)
	p := &resp.Propstat[0].Prop
	p.CurrentUserPrivilegeSet = calendarCurrentUserPrivilegeSetForCalendar(privileges)
	if !privileges.AllowsAnyWrite() {
		p.CalendarServerReadOnly = &struct{}{}
	}
	return resp
}

// calendarCollectionPropstatResponse builds the live properties every calendar
// collection response carries, leaving the privilege-dependent ones to the
// caller.
func calendarCollectionPropstatResponse(href, name string, cal store.Calendar, principalHref, syncToken, ctag string) response {
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
	// A non-nil description is present even when empty; nil means absent. The
	// filter renders present-empty as an empty element and absent as a 404.
	if cal.Description != nil {
		p.CalendarDescription = langStringPtr(*cal.Description, cal.DescriptionLang)
	}
	if cal.Color != nil && *cal.Color != "" {
		p.CalendarColor = cal.Color
	}
	p.CalendarTimezone = calendarTimezoneValue(cal.Timezone)
	p.SupportedCalendarComponentSet = supportedCalendarComponents(cal.SupportedComponents)
	p.SupportedCalendarData = supportedCalendarDataProp()
	p.CalDAVSupportedCollationSet = caldavSupportedCollationSetProp()
	p.ScheduleCalendarTransp = &scheduleCalendarTransp{Opaque: &struct{}{}}

	p.MaxResourceSize = strconv.FormatInt(maxDAVBodyBytes, 10)
	p.MinDateTime = ical.MinDateTime
	p.MaxDateTime = ical.MaxDateTime
	p.MaxInstances = strconv.Itoa(ical.MaxRecurrenceInstances)
	p.MaxAttendeesPerInstance = strconv.Itoa(ical.MaxAttendeesPerInstance)

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
	if description != nil {
		p.AddressBookDesc = description
	}
	p.SupportedAddressData = supportedAddressDataProp()
	p.AddressBookMaxResourceSize = strconv.FormatInt(maxDAVBodyBytes, 10)
	p.SupportedCollationSet = supportedCollationSetProp()
	return resp
}

func statusOKProp(name string, rtype resourceType) propstat {
	p := prop{ResourceType: &rtype}
	// A plain name string carries no explicit presence bit, so an empty name
	// is treated as absent (nil), not present-empty. Callers that need an
	// explicit present-empty displayname use SetDisplayName.
	if name != "" {
		p.DisplayName = stringPtr(name)
	}
	return propstat{Prop: p, Status: httpStatusOK}
}

func statusOKPropWithExtras(name string, rtype resourceType, principalHref string, includeCalendarHome, includeAddressHome bool) propstat {
	p := prop{
		ResourceType:            &rtype,
		CurrentUserPrincipal:    &hrefProp{Href: principalHref},
		CurrentUserPrincipalURL: &hrefProp{Href: principalHref},
	}
	if name != "" {
		p.DisplayName = stringPtr(name)
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
	// Object resources carry an explicit empty resourcetype so allprop
	// responses keep advertising it (RFC 4918 §15.9).
	propVal := prop{GetETag: "\"" + etag + "\"", ResourceType: &resourceType{}}
	if calendar {
		propVal.CalendarData = cdataString(data)
		propVal.GetContentType = "text/calendar; charset=utf-8"
		// RFC 4791 §7 requires the advertisement on every calendar object
		// resource, however the client reached it, so it is set here rather than
		// only on the Depth: 0 response for the object itself.
		propVal.SupportedReportSet = calendarObjectSupportedReports()
		// §7.5.1: the collations the calendar-query advertised just above
		// honours, which the client needs on the resource it will run it against.
		propVal.CalDAVSupportedCollationSet = caldavSupportedCollationSetProp()
	} else {
		propVal.AddressData = cdataString(data)
		propVal.GetContentType = "text/vcard; charset=utf-8"
	}
	return propstat{Prop: propVal, Status: httpStatusOK}
}

// calendarObjectSupportedReports is the DAV:supported-report-set of a calendar
// object resource. RFC 4791 §7 requires the calendaring reports to be advertised
// there as well as on collections, and the set lists exactly the reports that
// resource serves, including DAV:expand-property. free-busy-query is refused
// on an object resource by §7.10, and sync-collection is a collection report.
func calendarObjectSupportedReports() *supportedReportSet {
	return &supportedReportSet{
		Reports: append([]supportedReport{
			{Report: reportType{CalendarMultiGet: &struct{}{}}},
			{Report: reportType{CalendarQuery: &struct{}{}}},
			{Report: reportType{ExpandProperty: &struct{}{}}},
		}, aclResourceSupportedReports()...),
	}
}

func addressBookResourcePropstat(etag, data string) propstat {
	ps := etagProp(etag, data, false)
	ps.Prop.SupportedReportSet = addressbookObjectSupportedReports()
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
		Reports: append([]supportedReport{
			{Report: reportType{CalendarMultiGet: &struct{}{}}},
			{Report: reportType{CalendarQuery: &struct{}{}}},
			{Report: reportType{FreeBusyQuery: &struct{}{}}},
			{Report: reportType{SyncCollection: &struct{}{}}},
			{Report: reportType{ExpandProperty: &struct{}{}}},
		}, aclCollectionSupportedReports()...),
	}
}

func addressbookSupportedReports() *supportedReportSet {
	return &supportedReportSet{
		Reports: append([]supportedReport{
			{Report: reportType{AddressbookMultiGet: &struct{}{}}},
			{Report: reportType{AddressbookQuery: &struct{}{}}},
			{Report: reportType{SyncCollection: &struct{}{}}},
			{Report: reportType{ExpandProperty: &struct{}{}}},
		}, aclCollectionSupportedReports()...),
	}
}

func addressbookObjectSupportedReports() *supportedReportSet {
	return &supportedReportSet{
		Reports: append([]supportedReport{
			{Report: reportType{AddressbookMultiGet: &struct{}{}}},
			{Report: reportType{AddressbookQuery: &struct{}{}}},
			{Report: reportType{ExpandProperty: &struct{}{}}},
		}, aclResourceSupportedReports()...),
	}
}

func combinedSupportedReports() *supportedReportSet {
	return &supportedReportSet{
		Reports: append([]supportedReport{
			{Report: reportType{CalendarMultiGet: &struct{}{}}},
			{Report: reportType{CalendarQuery: &struct{}{}}},
			{Report: reportType{AddressbookMultiGet: &struct{}{}}},
			{Report: reportType{AddressbookQuery: &struct{}{}}},
			{Report: reportType{SyncCollection: &struct{}{}}},
			{Report: reportType{ExpandProperty: &struct{}{}}},
		}, aclRootCollectionSupportedReports()...),
	}
}

func aclResourceSupportedReports() []supportedReport {
	return []supportedReport{
		{Report: reportType{ACLPrincipalPropSet: &struct{}{}}},
		{Report: reportType{PrincipalPropertySearch: &struct{}{}}},
	}
}

func aclCollectionSupportedReports() []supportedReport {
	return []supportedReport{
		{Report: reportType{ACLPrincipalPropSet: &struct{}{}}},
		{Report: reportType{PrincipalMatch: &struct{}{}}},
		{Report: reportType{PrincipalPropertySearch: &struct{}{}}},
	}
}

func aclPrincipalCollectionSupportedReports() []supportedReport {
	return append(aclCollectionSupportedReports(),
		supportedReport{Report: reportType{PrincipalSearchPropertySet: &struct{}{}}},
	)
}

func aclRootCollectionSupportedReports() []supportedReport {
	return aclCollectionSupportedReports()
}

// calendarSupportedComponents returns the component names a collection accepts.
// The rule lives on the store model because the UI writes into these
// collections too, and a restriction only the DAV handlers enforce is not a
// restriction.
func calendarSupportedComponents(stored []string) []string {
	return store.SupportedComponentsOrDefault(stored)
}

func supportedCalendarComponents(stored []string) *supportedCalendarComponentSet {
	names := calendarSupportedComponents(stored)
	comps := make([]comp, 0, len(names))
	for _, name := range names {
		comps = append(comps, comp{Name: name})
	}
	return &supportedCalendarComponentSet{Comps: comps}
}

// calendarDataVersions and addressDataVersions are the media type versions the
// server advertises in CALDAV:supported-calendar-data (RFC 4791 §5.2.4) and
// CARDDAV:supported-address-data (RFC 6352 §6.2.2). PUT admits exactly what is
// advertised here, so the two cannot drift apart.
var (
	calendarDataVersions = []string{"2.0"}
	addressDataVersions  = []string{"3.0", "4.0"}
)

func supportedCalendarDataProp() *supportedCalendarData {
	types := make([]calendarDataType, 0, len(calendarDataVersions))
	for _, version := range calendarDataVersions {
		types = append(types, calendarDataType{ContentType: "text/calendar", Version: version})
	}
	return &supportedCalendarData{CalendarData: types}
}

func supportedAddressDataProp() *supportedAddressData {
	types := make([]addressDataType, 0, len(addressDataVersions))
	for _, version := range addressDataVersions {
		types = append(types, addressDataType{ContentType: "text/vcard", Version: version})
	}
	return &supportedAddressData{AddressDataType: types}
}

// mediaTypeAdvertised reports whether a request's Content-Type names the media
// type the server advertises for that kind of resource. The header is parsed
// rather than prefix-matched, so "text/calendarjunk" is not read as
// "text/calendar" and "application/ical" is not accepted at all, and a version
// parameter is checked against the advertised versions rather than ignored.
func mediaTypeAdvertised(header, mediaType string, versions []string) bool {
	parsed, params, err := mime.ParseMediaType(header)
	if err != nil {
		return false
	}
	if parsed != mediaType {
		return false
	}
	version, ok := params["version"]
	if !ok {
		return true
	}
	return slices.Contains(versions, strings.TrimSpace(version))
}

func supportedCollationSetProp() *supportedCollationSet {
	return &supportedCollationSet{
		SupportedCollation: []string{"i;ascii-casemap", "i;unicode-casemap"},
	}
}

// supportedCalendarCollations are the collations CalDAV text matching applies,
// which RFC 4791 §7.5 requires to be exactly what
// CALDAV:supported-collation-set advertises. Both do substring matching.
var supportedCalendarCollations = []string{"i;ascii-casemap", "i;octet"}

func caldavSupportedCollationSetProp() *caldavSupportedCollationSet {
	return &caldavSupportedCollationSet{SupportedCollation: supportedCalendarCollations}
}

// calendarCollationSupported reports whether a CALDAV:text-match collation
// attribute names a collation the matcher implements. RFC 4791 §7.5 makes the
// attribute optional and defaults it to i;ascii-casemap, so an absent value is
// supported; "default" is the RFC 4790 alias for the server's default.
func calendarCollationSupported(collation string) bool {
	_, ok := calendarCollationFolder(collation)
	return ok
}

// calendarCollationFolder returns the folding one collation identifier applies
// to both the search string and the value it is matched against. RFC 4791 §7.5
// forbids a wildcard in the identifier, so one is refused before lookup rather
// than expanded.
func calendarCollationFolder(collation string) (func(string) string, bool) {
	normalized := asciiCasemapFold(collation)
	if strings.Contains(normalized, "*") {
		return nil, false
	}
	switch normalized {
	case "", asciiCasemapFold("default"), asciiCasemapFold("i;ascii-casemap"):
		return asciiCasemapFold, true
	case asciiCasemapFold("i;octet"):
		// RFC 4790 §9.3 compares octet by octet, so the value passes through.
		return func(s string) string { return s }, true
	default:
		return nil, false
	}
}

// asciiCasemapFold folds a string the RFC 4790 i;ascii-casemap way: US-ASCII
// letters map to their uppercase form and every other octet is left alone.
// strings.ToUpper cannot stand in for it, because case-folding non-ASCII text
// makes values match that the advertised collation says must not.
func asciiCasemapFold(s string) string {
	if !strings.ContainsFunc(s, isASCIILower) {
		return s
	}
	folded := []byte(s)
	for i, b := range folded {
		if b >= 'a' && b <= 'z' {
			folded[i] = b - ('a' - 'A')
		}
	}
	return string(folded)
}

func isASCIILower(r rune) bool {
	return r >= 'a' && r <= 'z'
}

func birthdayCalendarCurrentUserPrivilegeSet() *currentUserPrivilegeSet {
	return currentUserPrivilegeSetForNames(birthdayCalendarPrivilegeNames)
}

func calendarCurrentUserPrivilegeSetForCalendar(privileges store.CalendarPrivileges) *currentUserPrivilegeSet {
	privileges = privileges.Normalized()
	var privs []privilege
	if privileges.Read {
		privs = append(privs, privilege{Read: &readPrivilege{}})
	}
	if privileges.ReadFreeBusy {
		privs = append(privs, privilege{ReadFreeBusy: &struct{}{}})
	}
	if privileges.Write {
		privs = append(privs, privilege{Write: &struct{}{}})
	}
	if privileges.WriteContent {
		privs = append(privs, privilege{WriteContent: &struct{}{}})
	}
	if privileges.WriteProperties {
		privs = append(privs, privilege{WriteProperties: &struct{}{}})
	}
	if privileges.Bind {
		privs = append(privs, privilege{Bind: &struct{}{}})
	}
	if privileges.Unbind {
		privs = append(privs, privilege{Unbind: &struct{}{}})
	}
	return &currentUserPrivilegeSet{Privileges: privs}
}

// calendarTimezoneValue returns the CALDAV:calendar-timezone value to serve.
// RFC 4791 §5.2.2 makes the value an iCalendar object, so a stored bare
// VTIMEZONE component -- what CalCard wrote before the wrapper was required --
// is enveloped on the way out rather than served as a fragment.
func calendarTimezoneValue(tz *string) *string {
	if tz == nil || strings.TrimSpace(*tz) == "" {
		defaultTZ := defaultCalendarTimezone
		return &defaultTZ
	}
	wrapped := wrapCalendarTimezone(*tz)
	if !validCalendarTimezone(wrapped) {
		defaultTZ := defaultCalendarTimezone
		return &defaultTZ
	}
	return &wrapped
}

// wrapCalendarTimezone envelopes a bare VTIMEZONE component in a VCALENDAR,
// leaving an already-wrapped value untouched.
func wrapCalendarTimezone(value string) string {
	trimmed := strings.TrimSpace(value)
	if trimmed == "" || strings.HasPrefix(strings.ToUpper(trimmed), "BEGIN:VCALENDAR") {
		return value
	}
	separator := "\n"
	if strings.Contains(trimmed, "\r\n") {
		separator = "\r\n"
	}
	return strings.Join([]string{
		"BEGIN:VCALENDAR",
		"VERSION:2.0",
		"PRODID:" + calendarTimezoneProdID,
		trimmed,
		"END:VCALENDAR",
	}, separator)
}

const calendarTimezoneProdID = "-//CalCard//CalCard Calendar Server//EN"

const defaultCalendarTimezone = "BEGIN:VCALENDAR\nVERSION:2.0\nPRODID:" + calendarTimezoneProdID +
	"\nBEGIN:VTIMEZONE\nTZID:UTC\nBEGIN:STANDARD\nDTSTART:19700101T000000\nTZOFFSETFROM:+0000\nTZOFFSETTO:+0000\nTZNAME:UTC\nEND:STANDARD\nEND:VTIMEZONE\nEND:VCALENDAR"
