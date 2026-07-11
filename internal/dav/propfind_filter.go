package dav

import (
	"encoding/xml"
	"strings"
)

// propfindResourceKind classifies a PROPFIND response so the property table
// can decide, per property, whether the resource defines it (200), lacks it
// (404), or ignores the request entirely (legacy behavior for a handful of
// property/kind pairs).
type propfindResourceKind uint8

const (
	kindGenericCollection propfindResourceKind = 1 << iota
	kindPrincipal
	kindCalendarCollection
	kindAddressBookCollection
	kindCalendarObject
	kindAddressObject
)

const (
	kindCollections = kindGenericCollection | kindPrincipal | kindCalendarCollection | kindAddressBookCollection
	kindObjects     = kindCalendarObject | kindAddressObject
	kindAll         = kindCollections | kindObjects
)

// propfindPropertySpec is one row of the property table: how to recognize the
// property in a request, which resource kinds define it, and how to copy its
// value into a response prop.
type propfindPropertySpec struct {
	// emptyName is the prefixed wire name used when rendering the property
	// as an empty element (404 propstats and propname responses).
	emptyName xml.Name
	requested func(q *propfindPropQuery) bool
	// ok lists the kinds that define the property; notFound the kinds that
	// answer 404. Kinds in neither set ignore the request.
	ok       propfindResourceKind
	notFound propfindResourceKind
	// present, when set, gates ok kinds: a requested property whose value is
	// absent from the source moves to the 404 propstat (e.g. calendar-color).
	present   func(src *prop) bool
	copyValue func(dst, src *prop, q *propfindPropQuery)
}

func davName(local string) xml.Name { return xml.Name{Local: local} }

var propfindPropertyTable = []propfindPropertySpec{
	{
		emptyName: davName("d:displayname"),
		requested: func(q *propfindPropQuery) bool { return q.DisplayName != nil },
		ok:        kindCollections, notFound: kindObjects,
		copyValue: func(dst, src *prop, _ *propfindPropQuery) { dst.DisplayName = src.DisplayName },
	},
	{
		emptyName: davName("d:resourcetype"),
		requested: func(q *propfindPropQuery) bool { return q.ResourceType != nil },
		ok:        kindCollections,
		copyValue: func(dst, src *prop, _ *propfindPropQuery) { dst.ResourceType = src.ResourceType },
	},
	{
		emptyName: davName("d:getetag"),
		requested: func(q *propfindPropQuery) bool { return q.GetETag != nil },
		ok:        kindObjects, notFound: kindCollections,
		copyValue: func(dst, src *prop, _ *propfindPropQuery) { dst.GetETag = src.GetETag },
	},
	{
		emptyName: davName("d:getcontenttype"),
		requested: func(q *propfindPropQuery) bool { return q.GetContentType != nil },
		ok:        kindObjects, notFound: kindCollections,
		copyValue: func(dst, src *prop, _ *propfindPropQuery) { dst.GetContentType = src.GetContentType },
	},
	{
		emptyName: davName("cal:calendar-data"),
		requested: func(q *propfindPropQuery) bool { return q.CalendarData != nil },
		ok:        kindCalendarObject, notFound: kindCollections,
		copyValue: func(dst, src *prop, _ *propfindPropQuery) { dst.CalendarData = src.CalendarData },
	},
	{
		emptyName: davName("card:address-data"),
		requested: func(q *propfindPropQuery) bool { return q.AddressData != nil },
		ok:        kindAddressObject, notFound: kindCollections,
		copyValue: func(dst, src *prop, q *propfindPropQuery) {
			dst.AddressData = cdataString(filterVCardData(string(src.AddressData), q.AddressData))
		},
	},
	{
		emptyName: davName("cal:calendar-description"),
		requested: func(q *propfindPropQuery) bool { return q.CalendarDescription != nil },
		ok:        kindCalendarCollection, notFound: kindGenericCollection | kindPrincipal | kindAddressBookCollection,
		copyValue: func(dst, src *prop, _ *propfindPropQuery) { dst.CalendarDescription = src.CalendarDescription },
	},
	{
		emptyName: davName("cal:calendar-timezone"),
		requested: func(q *propfindPropQuery) bool { return q.CalendarTimezone != nil },
		ok:        kindCalendarCollection, notFound: kindGenericCollection | kindPrincipal | kindAddressBookCollection,
		copyValue: func(dst, src *prop, _ *propfindPropQuery) { dst.CalendarTimezone = src.CalendarTimezone },
	},
	{
		emptyName: davName("ical:calendar-color"),
		requested: func(q *propfindPropQuery) bool { return q.CalendarColor != nil },
		ok:        kindCalendarCollection, notFound: kindGenericCollection,
		present:   func(src *prop) bool { return src.CalendarColor != nil },
		copyValue: func(dst, src *prop, _ *propfindPropQuery) { dst.CalendarColor = src.CalendarColor },
	},
	{
		emptyName: davName("card:addressbook-description"),
		requested: func(q *propfindPropQuery) bool { return q.AddressBookDesc != nil },
		ok:        kindAddressBookCollection, notFound: kindGenericCollection | kindPrincipal | kindCalendarCollection,
		copyValue: func(dst, src *prop, _ *propfindPropQuery) { dst.AddressBookDesc = src.AddressBookDesc },
	},
	{
		emptyName: davName("card:supported-address-data"),
		requested: func(q *propfindPropQuery) bool { return q.SupportedAddressData != nil },
		ok:        kindAddressBookCollection, notFound: kindGenericCollection | kindPrincipal | kindCalendarCollection,
		copyValue: func(dst, src *prop, _ *propfindPropQuery) { dst.SupportedAddressData = src.SupportedAddressData },
	},
	{
		emptyName: davName("card:max-resource-size"),
		requested: func(q *propfindPropQuery) bool { return q.AddressBookMaxResourceSize != nil },
		ok:        kindAddressBookCollection, notFound: kindGenericCollection | kindPrincipal | kindCalendarCollection,
		copyValue: func(dst, src *prop, _ *propfindPropQuery) {
			dst.AddressBookMaxResourceSize = src.AddressBookMaxResourceSize
		},
	},
	{
		emptyName: davName("card:supported-collation-set"),
		requested: func(q *propfindPropQuery) bool { return q.SupportedCollationSet != nil },
		ok:        kindAddressBookCollection, notFound: kindGenericCollection | kindPrincipal | kindCalendarCollection,
		copyValue: func(dst, src *prop, _ *propfindPropQuery) { dst.SupportedCollationSet = src.SupportedCollationSet },
	},
	{
		emptyName: davName("d:sync-token"),
		requested: func(q *propfindPropQuery) bool { return q.SyncToken != nil },
		ok:        kindCalendarCollection | kindAddressBookCollection, notFound: kindGenericCollection | kindPrincipal,
		copyValue: func(dst, src *prop, _ *propfindPropQuery) { dst.SyncToken = src.SyncToken },
	},
	{
		emptyName: davName("cs:getctag"),
		requested: func(q *propfindPropQuery) bool { return q.CTag != nil },
		ok:        kindCalendarCollection | kindAddressBookCollection, notFound: kindGenericCollection | kindPrincipal,
		copyValue: func(dst, src *prop, _ *propfindPropQuery) { dst.CTag = src.CTag },
	},
	{
		emptyName: davName("d:current-user-principal"),
		requested: func(q *propfindPropQuery) bool { return q.CurrentUserPrincipal != nil },
		ok:        kindCollections,
		copyValue: func(dst, src *prop, _ *propfindPropQuery) { dst.CurrentUserPrincipal = src.CurrentUserPrincipal },
	},
	{
		emptyName: davName("d:current-user-principal-URL"),
		requested: func(q *propfindPropQuery) bool { return q.CurrentUserPrincipalURL != nil },
		ok:        kindCollections,
		copyValue: func(dst, src *prop, _ *propfindPropQuery) {
			dst.CurrentUserPrincipalURL = src.CurrentUserPrincipalURL
		},
	},
	{
		emptyName: davName("d:principal-URL"),
		requested: func(q *propfindPropQuery) bool { return q.PrincipalURL != nil },
		ok:        kindPrincipal, notFound: kindGenericCollection | kindCalendarCollection | kindAddressBookCollection,
		copyValue: func(dst, src *prop, _ *propfindPropQuery) { dst.PrincipalURL = src.PrincipalURL },
	},
	{
		emptyName: davName("cal:calendar-home-set"),
		requested: func(q *propfindPropQuery) bool { return q.CalendarHomeSet != nil },
		ok:        kindPrincipal, notFound: kindGenericCollection | kindCalendarCollection | kindAddressBookCollection,
		copyValue: func(dst, src *prop, _ *propfindPropQuery) { dst.CalendarHomeSet = src.CalendarHomeSet },
	},
	{
		emptyName: davName("card:addressbook-home-set"),
		requested: func(q *propfindPropQuery) bool { return q.AddressbookHomeSet != nil },
		ok:        kindPrincipal | kindAddressBookCollection, notFound: kindGenericCollection | kindCalendarCollection,
		copyValue: func(dst, src *prop, _ *propfindPropQuery) { dst.AddressbookHomeSet = src.AddressbookHomeSet },
	},
	{
		emptyName: davName("card:principal-address"),
		requested: func(q *propfindPropQuery) bool { return q.PrincipalAddress != nil },
		notFound:  kindCollections,
	},
	{
		emptyName: davName("d:supported-report-set"),
		requested: func(q *propfindPropQuery) bool { return q.SupportedReportSet != nil },
		ok:        kindAll,
		copyValue: func(dst, src *prop, _ *propfindPropQuery) { dst.SupportedReportSet = src.SupportedReportSet },
	},
	{
		emptyName: davName("cal:supported-calendar-component-set"),
		requested: func(q *propfindPropQuery) bool { return q.SupportedCalendarComponentSet != nil },
		ok:        kindCalendarCollection, notFound: kindGenericCollection | kindPrincipal | kindAddressBookCollection,
		copyValue: func(dst, src *prop, _ *propfindPropQuery) {
			dst.SupportedCalendarComponentSet = src.SupportedCalendarComponentSet
		},
	},
	{
		emptyName: davName("cal:max-resource-size"),
		requested: func(q *propfindPropQuery) bool { return q.MaxResourceSize != nil },
		ok:        kindCalendarCollection, notFound: kindGenericCollection | kindPrincipal | kindAddressBookCollection,
		copyValue: func(dst, src *prop, _ *propfindPropQuery) { dst.MaxResourceSize = src.MaxResourceSize },
	},
	{
		emptyName: davName("cal:min-date-time"),
		requested: func(q *propfindPropQuery) bool { return q.MinDateTime != nil },
		ok:        kindCalendarCollection, notFound: kindGenericCollection | kindPrincipal | kindAddressBookCollection,
		copyValue: func(dst, src *prop, _ *propfindPropQuery) { dst.MinDateTime = src.MinDateTime },
	},
	{
		emptyName: davName("cal:max-date-time"),
		requested: func(q *propfindPropQuery) bool { return q.MaxDateTime != nil },
		ok:        kindCalendarCollection, notFound: kindGenericCollection | kindPrincipal | kindAddressBookCollection,
		copyValue: func(dst, src *prop, _ *propfindPropQuery) { dst.MaxDateTime = src.MaxDateTime },
	},
	{
		emptyName: davName("cal:max-instances"),
		requested: func(q *propfindPropQuery) bool { return q.MaxInstances != nil },
		ok:        kindCalendarCollection, notFound: kindGenericCollection | kindPrincipal | kindAddressBookCollection,
		copyValue: func(dst, src *prop, _ *propfindPropQuery) { dst.MaxInstances = src.MaxInstances },
	},
	{
		emptyName: davName("cal:max-attendees-per-instance"),
		requested: func(q *propfindPropQuery) bool { return q.MaxAttendeesPerInstance != nil },
		ok:        kindCalendarCollection, notFound: kindGenericCollection | kindPrincipal | kindAddressBookCollection,
		copyValue: func(dst, src *prop, _ *propfindPropQuery) {
			dst.MaxAttendeesPerInstance = src.MaxAttendeesPerInstance
		},
	},
	{
		emptyName: davName("cal:schedule-calendar-transp"),
		requested: func(q *propfindPropQuery) bool { return q.ScheduleCalendarTransp != nil },
		ok:        kindCalendarCollection, notFound: kindGenericCollection | kindPrincipal | kindAddressBookCollection,
		copyValue: func(dst, src *prop, _ *propfindPropQuery) { dst.ScheduleCalendarTransp = src.ScheduleCalendarTransp },
	},
	{
		emptyName: davName("cal:supported-calendar-data"),
		requested: func(q *propfindPropQuery) bool { return q.SupportedCalendarData != nil },
		ok:        kindCalendarCollection, notFound: kindGenericCollection | kindPrincipal | kindAddressBookCollection,
		copyValue: func(dst, src *prop, _ *propfindPropQuery) { dst.SupportedCalendarData = src.SupportedCalendarData },
	},
	{
		emptyName: davName("cs:read-only"),
		requested: func(q *propfindPropQuery) bool { return q.CalendarServerReadOnly != nil },
		ok:        kindCalendarCollection, notFound: kindGenericCollection | kindPrincipal | kindAddressBookCollection,
		copyValue: func(dst, src *prop, _ *propfindPropQuery) { dst.CalendarServerReadOnly = src.CalendarServerReadOnly },
	},
	{
		emptyName: davName("d:current-user-privilege-set"),
		requested: func(q *propfindPropQuery) bool { return q.CurrentUserPrivilegeSet != nil },
		ok:        kindGenericCollection | kindPrincipal | kindCalendarCollection,
		copyValue: func(dst, src *prop, _ *propfindPropQuery) {
			dst.CurrentUserPrivilegeSet = src.CurrentUserPrivilegeSet
		},
	},
	{
		emptyName: davName("d:lockdiscovery"),
		requested: func(q *propfindPropQuery) bool { return q.LockDiscovery != nil },
		ok:        kindAll,
		copyValue: func(dst, src *prop, _ *propfindPropQuery) { dst.LockDiscovery = src.LockDiscovery },
	},
	{
		emptyName: davName("d:supportedlock"),
		requested: func(q *propfindPropQuery) bool { return q.SupportedLock != nil },
		ok:        kindAll,
		copyValue: func(dst, src *prop, _ *propfindPropQuery) { dst.SupportedLock = src.SupportedLock },
	},
	{
		emptyName: davName("d:owner"),
		requested: func(q *propfindPropQuery) bool { return q.Owner != nil },
		notFound:  kindCollections,
	},
	{
		emptyName: davName("d:acl"),
		requested: func(q *propfindPropQuery) bool { return q.ACLProp != nil },
		ok:        kindAll,
		copyValue: func(dst, src *prop, _ *propfindPropQuery) { dst.ACL = src.ACL },
	},
	{
		emptyName: davName("d:supported-privilege-set"),
		requested: func(q *propfindPropQuery) bool { return q.SupportedPrivilegeSet != nil },
		ok:        kindAll,
		copyValue: func(dst, src *prop, _ *propfindPropQuery) { dst.SupportedPrivilegeSet = src.SupportedPrivilegeSet },
	},
	{
		emptyName: davName("d:principal-collection-set"),
		requested: func(q *propfindPropQuery) bool { return q.PrincipalCollectionSet != nil },
		ok:        kindAll,
		copyValue: func(dst, src *prop, _ *propfindPropQuery) { dst.PrincipalCollectionSet = src.PrincipalCollectionSet },
	},
}

// propfindKindOf classifies a built response by its resourcetype, falling back
// to the href extension for object resources.
func propfindKindOf(resp response) propfindResourceKind {
	src := resp.Propstat[0].Prop
	switch {
	case src.ResourceType.Principal != nil:
		return kindPrincipal
	case src.ResourceType.Calendar != nil:
		return kindCalendarCollection
	case src.ResourceType.AddressBook != nil:
		return kindAddressBookCollection
	case strings.HasSuffix(normalizeDAVHref(resp.Href), ".ics"):
		return kindCalendarObject
	case strings.HasSuffix(normalizeDAVHref(resp.Href), ".vcf"):
		return kindAddressObject
	default:
		return kindGenericCollection
	}
}

// filterPropfindResponseForKind rebuilds the response's propstats from the
// property table: requested properties the kind defines land in the 200
// propstat, requested properties it lacks land in a 404 propstat rendered as
// empty elements per RFC 4918 §9.1.
func filterPropfindResponseForKind(resp response, req *propfindRequest, kind propfindResourceKind) response {
	if req == nil || req.Prop == nil || len(resp.Propstat) == 0 {
		return resp
	}
	src := resp.Propstat[0].Prop
	var okProp prop
	var okSet bool
	var notFoundNames []xml.Name
	for i := range propfindPropertyTable {
		spec := &propfindPropertyTable[i]
		if !spec.requested(req.Prop) {
			continue
		}
		switch {
		case spec.ok&kind != 0 && (spec.present == nil || spec.present(&src)):
			spec.copyValue(&okProp, &src, req.Prop)
			okSet = true
		case spec.notFound&kind != 0 || spec.ok&kind != 0:
			notFoundNames = append(notFoundNames, spec.emptyName)
		}
	}
	for _, name := range req.Prop.CustomXML {
		if name.Local == "" {
			continue
		}
		if property, ok := src.customXMLProperty(name); ok {
			okProp.setCustomXMLProperty(property)
			okSet = true
		} else {
			notFoundNames = append(notFoundNames, name)
		}
	}
	resp.Propstat = nil
	if okSet {
		resp.Propstat = append(resp.Propstat, propstat{Prop: okProp, Status: httpStatusOK})
	}
	if len(notFoundNames) > 0 {
		resp.Propstat = append(resp.Propstat, propstat{PropNames: notFoundNames, Status: httpStatusNotFound})
	}
	if len(resp.Propstat) == 0 {
		resp.Propstat = []propstat{{Prop: prop{}, Status: httpStatusOK}}
	}
	return resp
}

// filterNonPrincipalPropfindResponse routes a response to the property table
// with the right resource kind. (The name predates principal support; it is
// the filter for every response kind.)
func filterNonPrincipalPropfindResponse(resp response, req *propfindRequest) response {
	if req == nil || req.Prop == nil || len(resp.Propstat) == 0 {
		return resp
	}
	kind := propfindKindOf(resp)
	if kind == kindAddressObject {
		return filterAddressObjectPropfindResponse(resp, req)
	}
	return filterPropfindResponseForKind(resp, req, kind)
}

// filterAddressObjectPropfindResponse applies the address-data conversion
// precondition (RFC 6352 §8.7) before the generic property filtering.
func filterAddressObjectPropfindResponse(resp response, req *propfindRequest) response {
	if req == nil || req.Prop == nil || len(resp.Propstat) == 0 {
		return resp
	}
	src := resp.Propstat[0].Prop
	if req.Prop.AddressData != nil && !canServeRequestedAddressData(string(src.AddressData), req.Prop.AddressData) {
		resp.Propstat = nil
		resp.Status = httpStatusNotAcceptable
		resp.Error = &responseError{SupportedAddressDataConversion: &struct{}{}}
		return resp
	}
	resp.Status = ""
	resp.Error = nil
	return filterPropfindResponseForKind(resp, req, kindAddressObject)
}

// propnamePropfindResponse converts a fully built response into an RFC 4918
// §9.1 propname response: the names of the properties defined on the
// resource, without values.
func propnamePropfindResponse(resp response) response {
	if len(resp.Propstat) == 0 {
		return resp
	}
	kind := propfindKindOf(resp)
	src := resp.Propstat[0].Prop
	var names []xml.Name
	for i := range propfindPropertyTable {
		spec := &propfindPropertyTable[i]
		if spec.ok&kind == 0 {
			continue
		}
		if spec.present != nil && !spec.present(&src) {
			continue
		}
		names = append(names, spec.emptyName)
	}
	for _, custom := range src.CustomXML {
		names = append(names, custom.Name)
	}
	resp.Propstat = []propstat{{PropNames: names, Status: httpStatusOK}}
	return resp
}
