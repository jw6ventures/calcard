package dav

import "encoding/xml"

// XML response models and helpers for DAV PROPFIND/REPORT responses.

type multistatus struct {
	XMLName   xml.Name   `xml:"d:multistatus"`
	XmlnsD    string     `xml:"xmlns:d,attr"`
	XmlnsC    string     `xml:"xmlns:cal,attr"`
	XmlnsA    string     `xml:"xmlns:card,attr"`
	XmlnsCS   string     `xml:"xmlns:cs,attr,omitempty"`
	SyncToken string     `xml:"d:sync-token,omitempty"`
	Response  []response `xml:"d:response"`
}

type response struct {
	Href     string     `xml:"d:href"`
	Propstat []propstat `xml:"d:propstat,omitempty"`
	Status   string     `xml:"d:status,omitempty"`
}

type propstat struct {
	Prop   prop   `xml:"d:prop"`
	Status string `xml:"d:status"`
}

type prop struct {
	DisplayName                   string                         `xml:"d:displayname,omitempty"`
	ResourceType                  resourceType                   `xml:"d:resourcetype"`
	GetETag                       string                         `xml:"d:getetag,omitempty"`
	GetContentType                string                         `xml:"d:getcontenttype,omitempty"`
	CalendarData                  cdataString                    `xml:"cal:calendar-data,omitempty"`
	AddressData                   cdataString                    `xml:"card:address-data,omitempty"`
	CalendarDescription           string                         `xml:"cal:calendar-description,omitempty"`
	CalendarTimezone              *string                        `xml:"cal:calendar-timezone,omitempty"`
	AddressBookDesc               string                         `xml:"card:addressbook-description,omitempty"`
	SyncToken                     string                         `xml:"d:sync-token,omitempty"`
	CTag                          string                         `xml:"cs:getctag,omitempty"`
	CurrentUserPrincipal          *hrefProp                      `xml:"d:current-user-principal,omitempty"`
	PrincipalURL                  *hrefProp                      `xml:"d:principal-URL,omitempty"`
	CalendarHomeSet               *hrefListProp                  `xml:"cal:calendar-home-set,omitempty"`
	AddressbookHomeSet            *hrefListProp                  `xml:"card:addressbook-home-set,omitempty"`
	SupportedReportSet            *supportedReportSet            `xml:"d:supported-report-set,omitempty"`
	SupportedCalendarComponentSet *supportedCalendarComponentSet `xml:"cal:supported-calendar-component-set,omitempty"`
	MaxResourceSize               string                         `xml:"cal:max-resource-size,omitempty"`
	MinDateTime                   string                         `xml:"cal:min-date-time,omitempty"`
	MaxDateTime                   string                         `xml:"cal:max-date-time,omitempty"`
	MaxInstances                  string                         `xml:"cal:max-instances,omitempty"`
	MaxAttendeesPerInstance       string                         `xml:"cal:max-attendees-per-instance,omitempty"`
	ScheduleCalendarTransp        *scheduleCalendarTransp        `xml:"cal:schedule-calendar-transp,omitempty"`
	SupportedCalendarData         *supportedCalendarData         `xml:"cal:supported-calendar-data,omitempty"`
	CalendarServerReadOnly        *struct{}                      `xml:"cs:read-only,omitempty"`
	CurrentUserPrivilegeSet       *currentUserPrivilegeSet       `xml:"d:current-user-privilege-set,omitempty"`
}

// cdataString wraps string content in CDATA for raw XML output.
type cdataString string

func (c cdataString) MarshalXML(e *xml.Encoder, start xml.StartElement) error {
	if c == "" {
		return nil
	}
	return e.EncodeElement(struct {
		S string `xml:",cdata"`
	}{S: string(c)}, start)
}

type resourceType struct {
	Collection  *struct{} `xml:"d:collection,omitempty"`
	Calendar    *struct{} `xml:"cal:calendar,omitempty"`
	AddressBook *struct{} `xml:"card:addressbook,omitempty"`
	Principal   *struct{} `xml:"d:principal,omitempty"`
}

type reportRequest struct {
	XMLName      xml.Name
	Hrefs        []string        `xml:"DAV: href"`
	SyncToken    string          `xml:"DAV: sync-token"`
	Filter       *calFilter      `xml:"urn:ietf:params:xml:ns:caldav filter"`
	CalendarData *calendarDataEl `xml:"urn:ietf:params:xml:ns:caldav calendar-data"`
	Prop         *reportProp     `xml:"DAV: prop"`
}

// reportProp captures the prop element in reports for partial retrieval
type reportProp struct {
	CalendarData *calendarDataEl `xml:"urn:ietf:params:xml:ns:caldav calendar-data"`
}

// calendarDataEl specifies what calendar data to return (RFC 4791 Section 9.6)
type calendarDataEl struct {
	Expand *expandEl      `xml:"urn:ietf:params:xml:ns:caldav expand"`
	Comp   []calendarComp `xml:"urn:ietf:params:xml:ns:caldav comp"`
	Prop   []calendarProp `xml:"urn:ietf:params:xml:ns:caldav prop"`
}

// calendarComp describes component selection within calendar-data.
type calendarComp struct {
	Name string         `xml:"name,attr"`
	Comp []calendarComp `xml:"urn:ietf:params:xml:ns:caldav comp"`
	Prop []calendarProp `xml:"urn:ietf:params:xml:ns:caldav prop"`
}

// calendarProp describes property selection within calendar-data.
type calendarProp struct {
	Name string `xml:"name,attr"`
}

// expandEl specifies recurrence expansion parameters
type expandEl struct {
	Start string `xml:"start,attr"`
	End   string `xml:"end,attr"`
}

// propfindRequest represents a PROPFIND request body (RFC 4918 Section 9.1)
type propfindRequest struct {
	XMLName  xml.Name
	AllProp  *struct{}          `xml:"DAV: allprop"`
	PropName *struct{}          `xml:"DAV: propname"`
	Prop     *propfindPropQuery `xml:"DAV: prop"`
}

// propfindPropQuery lists specific properties requested
type propfindPropQuery struct {
	DisplayName                   *struct{} `xml:"DAV: displayname"`
	ResourceType                  *struct{} `xml:"DAV: resourcetype"`
	GetETag                       *struct{} `xml:"DAV: getetag"`
	GetContentType                *struct{} `xml:"DAV: getcontenttype"`
	CalendarData                  *struct{} `xml:"urn:ietf:params:xml:ns:caldav calendar-data"`
	AddressData                   *struct{} `xml:"urn:ietf:params:xml:ns:carddav address-data"`
	CalendarDescription           *struct{} `xml:"urn:ietf:params:xml:ns:caldav calendar-description"`
	CalendarTimezone              *struct{} `xml:"urn:ietf:params:xml:ns:caldav calendar-timezone"`
	AddressBookDesc               *struct{} `xml:"urn:ietf:params:xml:ns:carddav addressbook-description"`
	SyncToken                     *struct{} `xml:"DAV: sync-token"`
	CTag                          *struct{} `xml:"http://calendarserver.org/ns/ getctag"`
	CurrentUserPrincipal          *struct{} `xml:"DAV: current-user-principal"`
	PrincipalURL                  *struct{} `xml:"DAV: principal-URL"`
	CalendarHomeSet               *struct{} `xml:"urn:ietf:params:xml:ns:caldav calendar-home-set"`
	AddressbookHomeSet            *struct{} `xml:"urn:ietf:params:xml:ns:carddav addressbook-home-set"`
	SupportedReportSet            *struct{} `xml:"DAV: supported-report-set"`
	SupportedCalendarComponentSet *struct{} `xml:"urn:ietf:params:xml:ns:caldav supported-calendar-component-set"`
	SupportedCalendarData         *struct{} `xml:"urn:ietf:params:xml:ns:caldav supported-calendar-data"`
}

// calFilter represents a CalDAV calendar-query filter (RFC 4791 Section 9.7)
type calFilter struct {
	CompFilter compFilter `xml:"urn:ietf:params:xml:ns:caldav comp-filter"`
}

// compFilter filters by component type and optionally by time-range
type compFilter struct {
	Name       string       `xml:"name,attr"`
	TimeRange  *timeRange   `xml:"urn:ietf:params:xml:ns:caldav time-range"`
	CompFilter []compFilter `xml:"urn:ietf:params:xml:ns:caldav comp-filter"`
	PropFilter []propFilter `xml:"urn:ietf:params:xml:ns:caldav prop-filter"`
	TextMatch  *textMatch   `xml:"urn:ietf:params:xml:ns:caldav text-match"`
}

// propFilter filters by property presence and optionally by text-match
type propFilter struct {
	Name         string     `xml:"name,attr"`
	IsNotDefined *struct{}  `xml:"urn:ietf:params:xml:ns:caldav is-not-defined"`
	TextMatch    *textMatch `xml:"urn:ietf:params:xml:ns:caldav text-match"`
}

// textMatch filters by text content
type textMatch struct {
	Text            string `xml:",chardata"`
	Collation       string `xml:"collation,attr,omitempty"`
	NegateCondition string `xml:"negate-condition,attr,omitempty"`
}

type timeRange struct {
	Start string `xml:"start,attr"`
	End   string `xml:"end,attr"`
}

type proppatchRequest struct {
	XMLName xml.Name
	Set     *proppatchSet    `xml:"DAV: set"`
	Remove  *proppatchRemove `xml:"DAV: remove"`
}

type mkcalendarRequest struct {
	XMLName xml.Name
	Set     *mkcalendarSet `xml:"DAV: set"`
}

type mkcalendarSet struct {
	Prop proppatchProp `xml:"DAV: prop"`
}

type proppatchSet struct {
	Prop proppatchProp `xml:"DAV: prop"`
}

type proppatchRemove struct {
	Prop proppatchProp `xml:"DAV: prop"`
}

type proppatchProp struct {
	DisplayName         *string `xml:"DAV: displayname"`
	CalendarDescription *string `xml:"urn:ietf:params:xml:ns:caldav calendar-description"`
	CalendarTimezone    *string `xml:"urn:ietf:params:xml:ns:caldav calendar-timezone"`
	AddressBookDesc     *string `xml:"urn:ietf:params:xml:ns:carddav addressbook-description"`
}

type hrefProp struct {
	Href string `xml:"d:href"`
}

type hrefListProp struct {
	Href []string `xml:"d:href"`
}

type supportedReportSet struct {
	Reports []supportedReport `xml:"d:supported-report"`
}

type supportedReport struct {
	Report reportType `xml:"d:report"`
}

type reportType struct {
	CalendarMultiGet    *struct{} `xml:"cal:calendar-multiget,omitempty"`
	CalendarQuery       *struct{} `xml:"cal:calendar-query,omitempty"`
	FreeBusyQuery       *struct{} `xml:"cal:free-busy-query,omitempty"`
	AddressbookMultiGet *struct{} `xml:"card:addressbook-multiget,omitempty"`
	AddressbookQuery    *struct{} `xml:"card:addressbook-query,omitempty"`
	SyncCollection      *struct{} `xml:"d:sync-collection,omitempty"`
	ExpandProperty      *struct{} `xml:"d:expand-property,omitempty"`
}

type supportedCalendarComponentSet struct {
	Comps []comp `xml:"cal:comp"`
}

type comp struct {
	Name string `xml:"name,attr"`
}

type scheduleCalendarTransp struct {
	Opaque      *struct{} `xml:"cal:opaque,omitempty"`
	Transparent *struct{} `xml:"cal:transparent,omitempty"`
}

type supportedCalendarData struct {
	CalendarData []calendarDataType `xml:"cal:calendar-data"`
}

type calendarDataType struct {
	ContentType string `xml:"content-type,attr"`
	Version     string `xml:"version,attr,omitempty"`
}

type currentUserPrivilegeSet struct {
	Privileges []privilege `xml:"d:privilege"`
}

type privilege struct {
	Read         *readPrivilege `xml:"d:read,omitempty"`
	ReadFreeBusy *struct{}      `xml:"cal:read-free-busy,omitempty"`
}

type readPrivilege struct {
	ReadFreeBusy *struct{} `xml:"cal:read-free-busy,omitempty"`
}
