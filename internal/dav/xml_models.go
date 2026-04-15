package dav

import (
	"encoding/xml"
	"fmt"
	"io"
	"strings"
)

// XML response models and helpers for DAV PROPFIND/REPORT responses.

type multistatus struct {
	XMLName   xml.Name   `xml:"d:multistatus"`
	XmlnsD    string     `xml:"xmlns:d,attr"`
	XmlnsC    string     `xml:"xmlns:cal,attr"`
	XmlnsA    string     `xml:"xmlns:card,attr"`
	XmlnsCS   string     `xml:"xmlns:cs,attr,omitempty"`
	XmlnsICAL string     `xml:"xmlns:ical,attr,omitempty"`
	SyncToken string     `xml:"d:sync-token,omitempty"`
	Response  []response `xml:"d:response"`
}

type response struct {
	Href     string         `xml:"d:href"`
	Propstat []propstat     `xml:"d:propstat,omitempty"`
	Status   string         `xml:"d:status,omitempty"`
	Error    *responseError `xml:"d:error,omitempty"`
}

type responseError struct {
	NumberOfMatchesWithinLimits    *struct{} `xml:"d:number-of-matches-within-limits,omitempty"`
	SupportedAddressDataConversion *struct{} `xml:"card:supported-address-data-conversion,omitempty"`
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
	CalendarColor                 *string                        `xml:"ical:calendar-color,omitempty"`
	AddressBookDesc               string                         `xml:"card:addressbook-description,omitempty"`
	SupportedAddressData          *supportedAddressData          `xml:"card:supported-address-data,omitempty"`
	AddressBookMaxResourceSize    string                         `xml:"card:max-resource-size,omitempty"`
	SupportedCollationSet         *supportedCollationSet         `xml:"card:supported-collation-set,omitempty"`
	SyncToken                     string                         `xml:"d:sync-token,omitempty"`
	CTag                          string                         `xml:"cs:getctag,omitempty"`
	CurrentUserPrincipal          *expandableHrefProp            `xml:"d:current-user-principal,omitempty"`
	CurrentUserPrincipalURL       *hrefProp                      `xml:"d:current-user-principal-URL,omitempty"`
	PrincipalURL                  *expandableHrefProp            `xml:"d:principal-URL,omitempty"`
	CalendarHomeSet               *hrefListProp                  `xml:"cal:calendar-home-set,omitempty"`
	AddressbookHomeSet            *hrefListProp                  `xml:"card:addressbook-home-set,omitempty"`
	PrincipalAddress              *hrefProp                      `xml:"card:principal-address,omitempty"`
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
	LockDiscovery                 *lockDiscoveryProp             `xml:"d:lockdiscovery,omitempty"`
	SupportedLock                 *supportedLockProp             `xml:"d:supportedlock,omitempty"`
	Owner                         *hrefProp                      `xml:"d:owner,omitempty"`
	ACL                           *aclProp                       `xml:"d:acl,omitempty"`
	SupportedPrivilegeSet         *supportedPrivilegeSetProp     `xml:"d:supported-privilege-set,omitempty"`
	PrincipalCollectionSet        *hrefListProp                  `xml:"d:principal-collection-set,omitempty"`
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
	Hrefs        []string          `xml:"DAV: href"`
	SyncToken    string            `xml:"DAV: sync-token"`
	Filter       *calFilter        `xml:"urn:ietf:params:xml:ns:caldav filter"`
	CardFilter   *cardFilter       `xml:"urn:ietf:params:xml:ns:carddav filter"`
	CalendarData *calendarDataEl   `xml:"urn:ietf:params:xml:ns:caldav calendar-data"`
	AddressData  *addressDataQuery `xml:"urn:ietf:params:xml:ns:carddav address-data"`
	Prop         *reportProp       `xml:"DAV: prop"`
	Limit        *addressbookLimit `xml:"urn:ietf:params:xml:ns:carddav limit"`
}

// reportProp captures the prop element in reports for partial retrieval
type reportProp struct {
	DisplayName     *struct{}         `xml:"DAV: displayname"`
	GetETag         *struct{}         `xml:"DAV: getetag"`
	GetContentType  *struct{}         `xml:"DAV: getcontenttype"`
	SupportedReport *struct{}         `xml:"DAV: supported-report-set"`
	CalendarData    *calendarDataEl   `xml:"urn:ietf:params:xml:ns:caldav calendar-data"`
	AddressData     *addressDataQuery `xml:"urn:ietf:params:xml:ns:carddav address-data"`
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
	DisplayName                   *struct{}         `xml:"DAV: displayname"`
	ResourceType                  *struct{}         `xml:"DAV: resourcetype"`
	GetETag                       *struct{}         `xml:"DAV: getetag"`
	GetContentType                *struct{}         `xml:"DAV: getcontenttype"`
	CalendarData                  *struct{}         `xml:"urn:ietf:params:xml:ns:caldav calendar-data"`
	AddressData                   *addressDataQuery `xml:"urn:ietf:params:xml:ns:carddav address-data"`
	CalendarDescription           *struct{}         `xml:"urn:ietf:params:xml:ns:caldav calendar-description"`
	CalendarTimezone              *struct{}         `xml:"urn:ietf:params:xml:ns:caldav calendar-timezone"`
	CalendarColor                 *struct{}         `xml:"http://apple.com/ns/ical/ calendar-color"`
	AddressBookDesc               *struct{}         `xml:"urn:ietf:params:xml:ns:carddav addressbook-description"`
	SupportedAddressData          *struct{}         `xml:"urn:ietf:params:xml:ns:carddav supported-address-data"`
	AddressBookMaxResourceSize    *struct{}         `xml:"urn:ietf:params:xml:ns:carddav max-resource-size"`
	SupportedCollationSet         *struct{}         `xml:"urn:ietf:params:xml:ns:carddav supported-collation-set"`
	SyncToken                     *struct{}         `xml:"DAV: sync-token"`
	CTag                          *struct{}         `xml:"http://calendarserver.org/ns/ getctag"`
	CurrentUserPrincipal          *struct{}         `xml:"DAV: current-user-principal"`
	CurrentUserPrincipalURL       *struct{}         `xml:"DAV: current-user-principal-URL"`
	PrincipalURL                  *struct{}         `xml:"DAV: principal-URL"`
	CalendarHomeSet               *struct{}         `xml:"urn:ietf:params:xml:ns:caldav calendar-home-set"`
	AddressbookHomeSet            *struct{}         `xml:"urn:ietf:params:xml:ns:carddav addressbook-home-set"`
	PrincipalAddress              *struct{}         `xml:"urn:ietf:params:xml:ns:carddav principal-address"`
	SupportedReportSet            *struct{}         `xml:"DAV: supported-report-set"`
	SupportedCalendarComponentSet *struct{}         `xml:"urn:ietf:params:xml:ns:caldav supported-calendar-component-set"`
	MaxResourceSize               *struct{}         `xml:"urn:ietf:params:xml:ns:caldav max-resource-size"`
	MinDateTime                   *struct{}         `xml:"urn:ietf:params:xml:ns:caldav min-date-time"`
	MaxDateTime                   *struct{}         `xml:"urn:ietf:params:xml:ns:caldav max-date-time"`
	MaxInstances                  *struct{}         `xml:"urn:ietf:params:xml:ns:caldav max-instances"`
	MaxAttendeesPerInstance       *struct{}         `xml:"urn:ietf:params:xml:ns:caldav max-attendees-per-instance"`
	ScheduleCalendarTransp        *struct{}         `xml:"urn:ietf:params:xml:ns:caldav schedule-calendar-transp"`
	SupportedCalendarData         *struct{}         `xml:"urn:ietf:params:xml:ns:caldav supported-calendar-data"`
	CalendarServerReadOnly        *struct{}         `xml:"http://calendarserver.org/ns/ read-only"`
	CurrentUserPrivilegeSet       *struct{}         `xml:"DAV: current-user-privilege-set"`
	LockDiscovery                 *struct{}         `xml:"DAV: lockdiscovery"`
	SupportedLock                 *struct{}         `xml:"DAV: supportedlock"`
	Owner                         *struct{}         `xml:"DAV: owner"`
	ACLProp                       *struct{}         `xml:"DAV: acl"`
	SupportedPrivilegeSet         *struct{}         `xml:"DAV: supported-privilege-set"`
	PrincipalCollectionSet        *struct{}         `xml:"DAV: principal-collection-set"`
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
	MatchType       string `xml:"match-type,attr,omitempty"`
	NegateCondition string `xml:"negate-condition,attr,omitempty"`
}

type timeRange struct {
	Start string `xml:"start,attr"`
	End   string `xml:"end,attr"`
}

type cardFilter struct {
	Test       string           `xml:"test,attr,omitempty"`
	PropFilter []cardPropFilter `xml:"urn:ietf:params:xml:ns:carddav prop-filter"`
}

type cardPropFilter struct {
	Test         string            `xml:"test,attr,omitempty"`
	Name         string            `xml:"name,attr"`
	IsNotDefined *struct{}         `xml:"urn:ietf:params:xml:ns:carddav is-not-defined"`
	TextMatch    *textMatch        `xml:"urn:ietf:params:xml:ns:carddav text-match"`
	ParamFilter  []cardParamFilter `xml:"urn:ietf:params:xml:ns:carddav param-filter"`
}

type cardParamFilter struct {
	Name         string     `xml:"name,attr"`
	IsNotDefined *struct{}  `xml:"urn:ietf:params:xml:ns:carddav is-not-defined"`
	TextMatch    *textMatch `xml:"urn:ietf:params:xml:ns:carddav text-match"`
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
	DisplayName                *string                `xml:"DAV: displayname"`
	ResourceType               *resourceType          `xml:"DAV: resourcetype"`
	CalendarDescription        *string                `xml:"urn:ietf:params:xml:ns:caldav calendar-description"`
	CalendarTimezone           *string                `xml:"urn:ietf:params:xml:ns:caldav calendar-timezone"`
	CalendarColor              *string                `xml:"http://apple.com/ns/ical/ calendar-color"`
	AddressBookDesc            *string                `xml:"urn:ietf:params:xml:ns:carddav addressbook-description"`
	SupportedAddressData       *supportedAddressData  `xml:"urn:ietf:params:xml:ns:carddav supported-address-data"`
	AddressBookMaxResourceSize *string                `xml:"urn:ietf:params:xml:ns:carddav max-resource-size"`
	SupportedCollationSet      *supportedCollationSet `xml:"urn:ietf:params:xml:ns:carddav supported-collation-set"`
}

type hrefProp struct {
	Href string `xml:"d:href"`
}

type expandableHrefProp struct {
	Href     string
	Response []response
}

func (p *expandableHrefProp) MarshalXML(enc *xml.Encoder, start xml.StartElement) error {
	if err := enc.EncodeToken(start); err != nil {
		return err
	}
	if p != nil {
		if p.Href != "" {
			if err := enc.EncodeElement(p.Href, xml.StartElement{Name: xml.Name{Local: "d:href"}}); err != nil {
				return err
			}
		}
		for _, resp := range p.Response {
			if err := enc.EncodeElement(resp, xml.StartElement{Name: xml.Name{Local: "d:response"}}); err != nil {
				return err
			}
		}
	}
	return enc.EncodeToken(start.End())
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

type supportedAddressData struct {
	AddressDataType []addressDataType `xml:"card:address-data-type"`
}

type addressDataType struct {
	ContentType string `xml:"content-type,attr"`
	Version     string `xml:"version,attr,omitempty"`
}

type supportedCollationSet struct {
	SupportedCollation []string `xml:"card:supported-collation"`
}

type addressDataQuery struct {
	ContentType string            `xml:"content-type,attr,omitempty"`
	Version     string            `xml:"version,attr,omitempty"`
	AllProp     *struct{}         `xml:"urn:ietf:params:xml:ns:carddav allprop"`
	Prop        []addressDataProp `xml:"urn:ietf:params:xml:ns:carddav prop"`
}

type addressDataProp struct {
	Name    string `xml:"name,attr"`
	NoValue string `xml:"novalue,attr,omitempty"`
}

type addressbookLimit struct {
	NResults int `xml:"urn:ietf:params:xml:ns:carddav nresults"`
}

type expandPropertyRequest struct {
	XMLName  xml.Name                `xml:"DAV: expand-property"`
	Prop     expandPropertyTarget    `xml:"DAV: prop"`
	Property []expandPropertyElement `xml:"DAV: property"`
}

type expandPropertyTarget struct {
	CurrentUserPrincipal *expandPropertySpec `xml:"DAV: current-user-principal"`
	PrincipalURL         *expandPropertySpec `xml:"DAV: principal-URL"`
}

type expandPropertySpec struct {
	Prop *propfindPropQuery `xml:"DAV: prop"`
}

type expandPropertyElement struct {
	Name      string                  `xml:"name,attr"`
	Namespace string                  `xml:"namespace,attr"`
	Property  []expandPropertyElement `xml:"DAV: property"`
}

func (e *expandPropertyElement) UnmarshalXML(dec *xml.Decoder, start xml.StartElement) error {
	*e = expandPropertyElement{}
	for _, attr := range start.Attr {
		switch attr.Name.Local {
		case "name":
			e.Name = attr.Value
		case "namespace":
			e.Namespace = attr.Value
		}
	}

	for {
		tok, err := dec.Token()
		if err != nil {
			if err == io.EOF {
				return nil
			}
			return err
		}
		switch tok := tok.(type) {
		case xml.StartElement:
			if tok.Name.Space == "DAV:" && tok.Name.Local == "property" {
				var child expandPropertyElement
				if err := dec.DecodeElement(&child, &tok); err != nil {
					return err
				}
				e.Property = append(e.Property, child)
				continue
			}
			if err := dec.Skip(); err != nil {
				return err
			}
		case xml.EndElement:
			if tok.Name == start.Name {
				return nil
			}
		}
	}
}

type currentUserPrivilegeSet struct {
	Privileges []privilege `xml:"d:privilege"`
}

type privilege struct {
	Read            *readPrivilege `xml:"d:read,omitempty"`
	ReadFreeBusy    *struct{}      `xml:"cal:read-free-busy,omitempty"`
	Write           *struct{}      `xml:"d:write,omitempty"`
	WriteContent    *struct{}      `xml:"d:write-content,omitempty"`
	WriteProperties *struct{}      `xml:"d:write-properties,omitempty"`
	Bind            *struct{}      `xml:"d:bind,omitempty"`
	Unbind          *struct{}      `xml:"d:unbind,omitempty"`
}

type readPrivilege struct {
	ReadFreeBusy *struct{} `xml:"cal:read-free-busy,omitempty"`
}

// Lock XML models (RFC 4918)

type lockInfo struct {
	XMLName   xml.Name   `xml:"DAV: lockinfo"`
	LockScope lockScope  `xml:"DAV: lockscope"`
	LockType  lockType   `xml:"DAV: locktype"`
	Owner     *lockOwner `xml:"DAV: owner"`
}

type lockScope struct {
	Exclusive *struct{} `xml:"DAV: exclusive"`
	Shared    *struct{} `xml:"DAV: shared"`
}

type lockType struct {
	Write *struct{} `xml:"DAV: write"`
}

type lockOwner struct {
	Href string `xml:"DAV: href,omitempty"`
	Text string `xml:",chardata"`
}

type lockDiscoveryProp struct {
	ActiveLocks []activeLock `xml:"d:activelock"`
}

type activeLock struct {
	LockScope activeLockScope `xml:"d:lockscope"`
	LockType  activeLockType  `xml:"d:locktype"`
	Depth     string          `xml:"d:depth"`
	Owner     *lockOwnerResp  `xml:"d:owner,omitempty"`
	Timeout   string          `xml:"d:timeout"`
	LockToken *lockTokenProp  `xml:"d:locktoken"`
	LockRoot  *hrefProp       `xml:"d:lockroot"`
}

type activeLockScope struct {
	Exclusive *struct{} `xml:"d:exclusive,omitempty"`
	Shared    *struct{} `xml:"d:shared,omitempty"`
}

type activeLockType struct {
	Write *struct{} `xml:"d:write,omitempty"`
}

type lockOwnerResp struct {
	Href string `xml:"d:href,omitempty"`
	Text string `xml:",chardata"`
}

type lockTokenProp struct {
	Href string `xml:"d:href"`
}

type supportedLockProp struct {
	LockEntries []lockEntry `xml:"d:lockentry"`
}

type lockEntry struct {
	LockScope activeLockScope `xml:"d:lockscope"`
	LockType  activeLockType  `xml:"d:locktype"`
}

// ACL XML models (RFC 3744)

type aclRequest struct {
	XMLName xml.Name `xml:"DAV: acl"`
	ACE     []ace    `xml:"DAV: ace"`
}

type ace struct {
	Principal acePrincipal `xml:"DAV: principal"`
	Grant     *aceGrant    `xml:"DAV: grant"`
	Deny      *aceDeny     `xml:"DAV: deny"`
}

type acePrincipal struct {
	Href          string    `xml:"DAV: href,omitempty"`
	All           *struct{} `xml:"DAV: all,omitempty"`
	Authenticated *struct{} `xml:"DAV: authenticated,omitempty"`
	Self          *struct{} `xml:"DAV: self,omitempty"`
}

type aceGrant struct {
	Privileges []acePrivilege `xml:"DAV: privilege"`
}

type aceDeny struct {
	Privileges []acePrivilege `xml:"DAV: privilege"`
}

type acePrivilege struct {
	Read            *emptyElement          `xml:"DAV: read,omitempty"`
	Write           *emptyElement          `xml:"DAV: write,omitempty"`
	WriteContent    *emptyElement          `xml:"DAV: write-content,omitempty"`
	WriteProperties *emptyElement          `xml:"DAV: write-properties,omitempty"`
	ReadACL         *emptyElement          `xml:"DAV: read-acl,omitempty"`
	WriteACL        *emptyElement          `xml:"DAV: write-acl,omitempty"`
	Bind            *emptyElement          `xml:"DAV: bind,omitempty"`
	Unbind          *emptyElement          `xml:"DAV: unbind,omitempty"`
	All             *emptyElement          `xml:"DAV: all,omitempty"`
	Unknown         []unsupportedPrivilege `xml:",any"`
}

type emptyElement struct{}

func (e *emptyElement) UnmarshalXML(dec *xml.Decoder, start xml.StartElement) error {
	for {
		tok, err := dec.Token()
		if err != nil {
			return err
		}
		switch tok := tok.(type) {
		case xml.StartElement:
			return fmt.Errorf("unexpected nested element %q", xmlNameString(tok.Name))
		case xml.CharData:
			if strings.TrimSpace(string(tok)) != "" {
				return fmt.Errorf("unexpected character data in %q", xmlNameString(start.Name))
			}
		case xml.EndElement:
			if tok.Name == start.Name {
				return nil
			}
		}
	}
}

type unsupportedPrivilege struct {
	XMLName xml.Name
}

func xmlNameString(name xml.Name) string {
	if name.Space == "" {
		return name.Local
	}
	return name.Space + " " + name.Local
}

// ACL response properties

type aclProp struct {
	ACE []aceResp `xml:"d:ace"`
}

type aceResp struct {
	Principal acePrincipalResp `xml:"d:principal"`
	Grant     *aceGrantResp    `xml:"d:grant,omitempty"`
	Deny      *aceDenyResp     `xml:"d:deny,omitempty"`
}

type acePrincipalResp struct {
	Href          string    `xml:"d:href,omitempty"`
	All           *struct{} `xml:"d:all,omitempty"`
	Authenticated *struct{} `xml:"d:authenticated,omitempty"`
	Self          *struct{} `xml:"d:self,omitempty"`
}

type aceGrantResp struct {
	Privileges []acePrivilegeResp `xml:"d:privilege"`
}

type aceDenyResp struct {
	Privileges []acePrivilegeResp `xml:"d:privilege"`
}

type acePrivilegeResp struct {
	Read            *struct{} `xml:"d:read,omitempty"`
	Write           *struct{} `xml:"d:write,omitempty"`
	WriteContent    *struct{} `xml:"d:write-content,omitempty"`
	WriteProperties *struct{} `xml:"d:write-properties,omitempty"`
	ReadACL         *struct{} `xml:"d:read-acl,omitempty"`
	WriteACL        *struct{} `xml:"d:write-acl,omitempty"`
	Bind            *struct{} `xml:"d:bind,omitempty"`
	Unbind          *struct{} `xml:"d:unbind,omitempty"`
	All             *struct{} `xml:"d:all,omitempty"`
}

type supportedPrivilegeSetProp struct {
	SupportedPrivileges []supportedPrivilege `xml:"d:supported-privilege"`
}

type supportedPrivilege struct {
	Privilege   supportedPrivilegeType `xml:"d:privilege"`
	Description string                 `xml:"d:description"`
	SubPrivs    []supportedPrivilege   `xml:"d:supported-privilege,omitempty"`
}

type supportedPrivilegeType struct {
	Read            *struct{} `xml:"d:read,omitempty"`
	Write           *struct{} `xml:"d:write,omitempty"`
	WriteContent    *struct{} `xml:"d:write-content,omitempty"`
	WriteProperties *struct{} `xml:"d:write-properties,omitempty"`
	ReadACL         *struct{} `xml:"d:read-acl,omitempty"`
	WriteACL        *struct{} `xml:"d:write-acl,omitempty"`
	Bind            *struct{} `xml:"d:bind,omitempty"`
	Unbind          *struct{} `xml:"d:unbind,omitempty"`
	All             *struct{} `xml:"d:all,omitempty"`
	ReadFreeBusy    *struct{} `xml:"cal:read-free-busy,omitempty"`
}
