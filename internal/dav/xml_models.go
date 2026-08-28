package dav

import (
	"encoding/xml"
	"fmt"
	"io"
	"strings"
	"time"
)

// XML response models and helpers for DAV PROPFIND/REPORT responses.

const (
	namespaceDAV     = "DAV:"
	namespaceCalDAV  = "urn:ietf:params:xml:ns:caldav"
	namespaceCardDAV = "urn:ietf:params:xml:ns:carddav"
)

type multistatus struct {
	XMLName   xml.Name   `xml:"d:multistatus"`
	XmlnsD    string     `xml:"xmlns:d,attr"`
	XmlnsC    string     `xml:"xmlns:cal,attr"`
	XmlnsA    string     `xml:"xmlns:card,attr"`
	XmlnsCS   string     `xml:"xmlns:cs,attr,omitempty"`
	XmlnsICAL string     `xml:"xmlns:ical,attr,omitempty"`
	Response  []response `xml:"d:response"`
	// SyncToken trails the responses because encoding/xml emits fields in
	// declaration order and RFC 6578 §6.4 sequences the element model as
	// (response*, responsedescription?, sync-token?).
	SyncToken string `xml:"d:sync-token,omitempty"`
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

// propstatError is the RFC 4918 §16 DAV:error where a condition applies to
// particular properties rather than to the resource: in a 207 the element goes
// inside the propstat carrying those properties, since a multistatus has no
// top-level error to put it in.
type propstatError struct {
	CannotModifyProtectedProperty *struct{} `xml:"d:cannot-modify-protected-property,omitempty"`
}

// propstat follows the RFC 4918 §14.22 content model
// (prop, status, error?, responsedescription?), so the field order is the
// element order on the wire.
type propstat struct {
	Prop prop `xml:"d:prop"`
	// PropNames renders the prop element as a list of empty property
	// elements instead of Prop — used for 404 propstats and propname
	// responses, where RFC 4918 requires names without values.
	PropNames []xml.Name     `xml:"-"`
	Status    string         `xml:"d:status"`
	Error     *propstatError `xml:"d:error,omitempty"`
}

func (p propstat) MarshalXML(e *xml.Encoder, start xml.StartElement) error {
	if len(p.PropNames) == 0 {
		type propstatNoMarshal propstat
		return e.EncodeElement(propstatNoMarshal(p), start)
	}
	if err := e.EncodeToken(start); err != nil {
		return err
	}
	propStart := xml.StartElement{Name: xml.Name{Local: "d:prop"}}
	if err := e.EncodeToken(propStart); err != nil {
		return err
	}
	for _, name := range p.PropNames {
		el := xml.StartElement{Name: wirePropertyName(name)}
		if err := e.EncodeToken(el); err != nil {
			return err
		}
		if err := e.EncodeToken(el.End()); err != nil {
			return err
		}
	}
	if err := e.EncodeToken(propStart.End()); err != nil {
		return err
	}
	if err := e.EncodeElement(p.Status, xml.StartElement{Name: xml.Name{Local: "d:status"}}); err != nil {
		return err
	}
	if p.Error != nil {
		if err := e.EncodeElement(p.Error, xml.StartElement{Name: xml.Name{Local: "d:error"}}); err != nil {
			return err
		}
	}
	return e.EncodeToken(start.End())
}

func wirePropertyName(name xml.Name) xml.Name {
	prefixes := map[string]string{
		"DAV:":                           "d",
		"urn:ietf:params:xml:ns:caldav":  "cal",
		"urn:ietf:params:xml:ns:carddav": "card",
		"http://calendarserver.org/ns/":  "cs",
		"http://apple.com/ns/ical/":      "ical",
	}
	if prefix := prefixes[name.Space]; prefix != "" {
		return xml.Name{Local: prefix + ":" + name.Local}
	}
	return name
}

type prop struct {
	// DisplayName, CalendarDescription, and AddressBookDesc are pointers so the
	// property filter can distinguish three states explicitly (RFC 4918 §9.1):
	// nil = absent (404), a pointer to "" = present-empty (200 empty element),
	// and a pointer to a non-empty value = present (200). Presence must not be
	// inferred from a Go zero value.
	DisplayName                    *string                        `xml:"d:displayname,omitempty"`
	ResourceType                   *resourceType                  `xml:"d:resourcetype,omitempty"`
	GetETag                        string                         `xml:"d:getetag,omitempty"`
	GetContentType                 string                         `xml:"d:getcontenttype,omitempty"`
	CalendarData                   cdataString                    `xml:"cal:calendar-data,omitempty"`
	AddressData                    cdataString                    `xml:"card:address-data,omitempty"`
	CalendarDescription            *langString                    `xml:"cal:calendar-description,omitempty"`
	CalendarTimezone               *string                        `xml:"cal:calendar-timezone,omitempty"`
	CalendarColor                  *string                        `xml:"ical:calendar-color,omitempty"`
	AddressBookDesc                *string                        `xml:"card:addressbook-description,omitempty"`
	SupportedAddressData           *supportedAddressData          `xml:"card:supported-address-data,omitempty"`
	AddressBookMaxResourceSize     string                         `xml:"card:max-resource-size,omitempty"`
	SupportedCollationSet          *supportedCollationSet         `xml:"card:supported-collation-set,omitempty"`
	CalDAVSupportedCollationSet    *caldavSupportedCollationSet   `xml:"cal:supported-collation-set,omitempty"`
	SyncToken                      string                         `xml:"d:sync-token,omitempty"`
	CTag                           string                         `xml:"cs:getctag,omitempty"`
	CurrentUserPrincipal           *hrefProp                      `xml:"d:current-user-principal,omitempty"`
	CurrentUserPrincipalURL        *hrefProp                      `xml:"d:current-user-principal-URL,omitempty"`
	PrincipalURL                   *hrefProp                      `xml:"d:principal-URL,omitempty"`
	AlternateURISet                *hrefListProp                  `xml:"d:alternate-URI-set,omitempty"`
	GroupMembership                *hrefListProp                  `xml:"d:group-membership,omitempty"`
	CalendarHomeSet                *hrefListProp                  `xml:"cal:calendar-home-set,omitempty"`
	AddressbookHomeSet             *hrefListProp                  `xml:"card:addressbook-home-set,omitempty"`
	PrincipalAddress               *hrefProp                      `xml:"card:principal-address,omitempty"`
	SupportedReportSet             *supportedReportSet            `xml:"d:supported-report-set,omitempty"`
	SupportedCalendarComponentSet  *supportedCalendarComponentSet `xml:"cal:supported-calendar-component-set,omitempty"`
	MaxResourceSize                string                         `xml:"cal:max-resource-size,omitempty"`
	MinDateTime                    string                         `xml:"cal:min-date-time,omitempty"`
	MaxDateTime                    string                         `xml:"cal:max-date-time,omitempty"`
	MaxInstances                   string                         `xml:"cal:max-instances,omitempty"`
	MaxAttendeesPerInstance        string                         `xml:"cal:max-attendees-per-instance,omitempty"`
	ScheduleCalendarTransp         *scheduleCalendarTransp        `xml:"cal:schedule-calendar-transp,omitempty"`
	SupportedCalendarData          *supportedCalendarData         `xml:"cal:supported-calendar-data,omitempty"`
	CalendarServerReadOnly         *struct{}                      `xml:"cs:read-only,omitempty"`
	CurrentUserPrivilegeSet        *currentUserPrivilegeSet       `xml:"d:current-user-privilege-set,omitempty"`
	LockDiscovery                  *lockDiscoveryProp             `xml:"d:lockdiscovery,omitempty"`
	SupportedLock                  *supportedLockProp             `xml:"d:supportedlock,omitempty"`
	Owner                          *hrefProp                      `xml:"d:owner,omitempty"`
	Group                          *hrefProp                      `xml:"d:group,omitempty"`
	ACL                            *aclProp                       `xml:"d:acl,omitempty"`
	ACLRestrictions                *aclRestrictionsProp           `xml:"d:acl-restrictions,omitempty"`
	InheritedACLSet                *hrefListProp                  `xml:"d:inherited-acl-set,omitempty"`
	SupportedPrivilegeSet          *supportedPrivilegeSetProp     `xml:"d:supported-privilege-set,omitempty"`
	PrincipalCollectionSet         *hrefListProp                  `xml:"d:principal-collection-set,omitempty"`
	CustomXML                      []XMLProperty                  `xml:",any,omitempty"`
	aclForbidden                   bool
	currentUserPrivilegesForbidden bool
}

func (p *prop) setCustomXMLProperty(property XMLProperty) {
	for i := range p.CustomXML {
		if p.CustomXML[i].Name == property.Name {
			p.CustomXML[i] = property
			return
		}
	}
	p.CustomXML = append(p.CustomXML, property)
}

func (p prop) customXMLProperty(name xml.Name) (XMLProperty, bool) {
	for _, property := range p.CustomXML {
		if property.Name == name {
			return property, true
		}
	}
	return XMLProperty{}, false
}

func (p XMLProperty) MarshalXML(enc *xml.Encoder, start xml.StartElement) error {
	start.Name = p.Name
	if p.Value == nil {
		if err := enc.EncodeToken(start); err != nil {
			return err
		}
		return enc.EncodeToken(start.End())
	}
	return enc.EncodeElement(p.Value, start)
}

type rawXMLValue string

func (r rawXMLValue) MarshalXML(enc *xml.Encoder, start xml.StartElement) error {
	if err := enc.EncodeToken(start); err != nil {
		return err
	}
	wrapped := "<dead-property>" + string(r) + "</dead-property>"
	dec := xml.NewDecoder(strings.NewReader(wrapped))
	depth := 0
	for {
		token, err := dec.Token()
		if err != nil {
			if err == io.EOF {
				break
			}
			return err
		}
		switch token := token.(type) {
		case xml.StartElement:
			depth++
			if depth == 1 {
				continue
			}
			if err := enc.EncodeToken(token); err != nil {
				return err
			}
		case xml.EndElement:
			if depth == 1 {
				depth--
				continue
			}
			if err := enc.EncodeToken(token); err != nil {
				return err
			}
			depth--
		default:
			if depth > 0 {
				if err := enc.EncodeToken(token); err != nil {
					return err
				}
			}
		}
	}
	return enc.EncodeToken(start.End())
}

// langString is a text property value that carries the xml:lang attribute it
// was stored with. RFC 4918 §4.3 requires a server to return the language of a
// property value that was set with one, so the attribute travels with the value
// instead of being reconstructed from the request.
//
// The attribute name is spelled as a prefixed local name, matching the rest of
// this file: encoding/xml writes it verbatim, whereas an xml.Name carrying the
// "xml" namespace would make the encoder invent an xmlns declaration for it.
type langString struct {
	Value string
	Lang  string
}

func (l langString) MarshalXML(enc *xml.Encoder, start xml.StartElement) error {
	if l.Lang != "" {
		start.Attr = append(start.Attr, xml.Attr{Name: xml.Name{Local: "xml:lang"}, Value: l.Lang})
	}
	return enc.EncodeElement(l.Value, start)
}

// langStringPtr builds a present property value; a nil lang means the value
// carries no language tag.
func langStringPtr(value string, lang *string) *langString {
	result := langString{Value: value}
	if lang != nil {
		result.Lang = *lang
	}
	return &result
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
	TimeRange    *timeRange        `xml:"urn:ietf:params:xml:ns:caldav time-range"`
	CardFilter   *cardFilter       `xml:"urn:ietf:params:xml:ns:carddav filter"`
	CalendarData *calendarDataEl   `xml:"urn:ietf:params:xml:ns:caldav calendar-data"`
	AddressData  *addressDataQuery `xml:"urn:ietf:params:xml:ns:carddav address-data"`
	Prop         *reportProp       `xml:"DAV: prop"`
	Limit        *addressbookLimit `xml:"urn:ietf:params:xml:ns:carddav limit"`

	// selector and Timezone are filled by the RFC 4791 §9 grammar pass rather
	// than by struct tags: the three property selectors are alternatives the
	// tag decoder cannot distinguish, and CALDAV:timezone has a position in the
	// content model as well as a value. The element is carried here rather than
	// parsed away so the time-range evaluator can validate and use it.
	selector propertySelector
	Timezone string `xml:"-"`
}

// propertySelection is the shared live/dead property model used by PROPFIND
// and REPORT. The two methods add only their method-specific data selectors.
type propertySelection struct {
	DisplayName                   *struct{}  `xml:"DAV: displayname"`
	ResourceType                  *struct{}  `xml:"DAV: resourcetype"`
	GetETag                       *struct{}  `xml:"DAV: getetag"`
	GetContentType                *struct{}  `xml:"DAV: getcontenttype"`
	CalendarDescription           *struct{}  `xml:"urn:ietf:params:xml:ns:caldav calendar-description"`
	CalendarTimezone              *struct{}  `xml:"urn:ietf:params:xml:ns:caldav calendar-timezone"`
	CalendarColor                 *struct{}  `xml:"http://apple.com/ns/ical/ calendar-color"`
	AddressBookDesc               *struct{}  `xml:"urn:ietf:params:xml:ns:carddav addressbook-description"`
	SupportedAddressData          *struct{}  `xml:"urn:ietf:params:xml:ns:carddav supported-address-data"`
	AddressBookMaxResourceSize    *struct{}  `xml:"urn:ietf:params:xml:ns:carddav max-resource-size"`
	SupportedCollationSet         *struct{}  `xml:"urn:ietf:params:xml:ns:carddav supported-collation-set"`
	CalDAVSupportedCollationSet   *struct{}  `xml:"urn:ietf:params:xml:ns:caldav supported-collation-set"`
	SyncToken                     *struct{}  `xml:"DAV: sync-token"`
	CTag                          *struct{}  `xml:"http://calendarserver.org/ns/ getctag"`
	CurrentUserPrincipal          *struct{}  `xml:"DAV: current-user-principal"`
	CurrentUserPrincipalURL       *struct{}  `xml:"DAV: current-user-principal-URL"`
	PrincipalURL                  *struct{}  `xml:"DAV: principal-URL"`
	AlternateURISet               *struct{}  `xml:"DAV: alternate-URI-set"`
	GroupMembership               *struct{}  `xml:"DAV: group-membership"`
	CalendarHomeSet               *struct{}  `xml:"urn:ietf:params:xml:ns:caldav calendar-home-set"`
	AddressbookHomeSet            *struct{}  `xml:"urn:ietf:params:xml:ns:carddav addressbook-home-set"`
	PrincipalAddress              *struct{}  `xml:"urn:ietf:params:xml:ns:carddav principal-address"`
	SupportedReportSet            *struct{}  `xml:"DAV: supported-report-set"`
	SupportedCalendarComponentSet *struct{}  `xml:"urn:ietf:params:xml:ns:caldav supported-calendar-component-set"`
	MaxResourceSize               *struct{}  `xml:"urn:ietf:params:xml:ns:caldav max-resource-size"`
	MinDateTime                   *struct{}  `xml:"urn:ietf:params:xml:ns:caldav min-date-time"`
	MaxDateTime                   *struct{}  `xml:"urn:ietf:params:xml:ns:caldav max-date-time"`
	MaxInstances                  *struct{}  `xml:"urn:ietf:params:xml:ns:caldav max-instances"`
	MaxAttendeesPerInstance       *struct{}  `xml:"urn:ietf:params:xml:ns:caldav max-attendees-per-instance"`
	ScheduleCalendarTransp        *struct{}  `xml:"urn:ietf:params:xml:ns:caldav schedule-calendar-transp"`
	SupportedCalendarData         *struct{}  `xml:"urn:ietf:params:xml:ns:caldav supported-calendar-data"`
	CalendarServerReadOnly        *struct{}  `xml:"http://calendarserver.org/ns/ read-only"`
	CurrentUserPrivilegeSet       *struct{}  `xml:"DAV: current-user-privilege-set"`
	LockDiscovery                 *struct{}  `xml:"DAV: lockdiscovery"`
	SupportedLock                 *struct{}  `xml:"DAV: supportedlock"`
	Owner                         *struct{}  `xml:"DAV: owner"`
	Group                         *struct{}  `xml:"DAV: group"`
	ACLProp                       *struct{}  `xml:"DAV: acl"`
	ACLRestrictions               *struct{}  `xml:"DAV: acl-restrictions"`
	InheritedACLSet               *struct{}  `xml:"DAV: inherited-acl-set"`
	SupportedPrivilegeSet         *struct{}  `xml:"DAV: supported-privilege-set"`
	PrincipalCollectionSet        *struct{}  `xml:"DAV: principal-collection-set"`
	CustomXML                     []xml.Name `xml:",any"`
}

// reportProp captures the prop element in reports for partial retrieval.
type reportProp struct {
	propertySelection
	CalendarData *calendarDataEl   `xml:"urn:ietf:params:xml:ns:caldav calendar-data"`
	AddressData  *addressDataQuery `xml:"urn:ietf:params:xml:ns:carddav address-data"`
}

// calendarRange is the resolved start/end pair CALDAV:expand,
// CALDAV:limit-recurrence-set and CALDAV:limit-freebusy-set share (RFC 4791
// §9.6.5–§9.6.7): an inclusive start, a non-inclusive end, both required.
type calendarRange struct {
	Start time.Time
	End   time.Time
}

// calendarDataEl specifies what calendar data to return (RFC 4791 §9.6). Its
// ATTLIST defaults content-type to text/calendar and version to 2.0, so an
// absent attribute names the same pair an explicit one would. The children are
// read by UnmarshalXML against the §9.6 content model rather than by struct
// tags, which cannot express the sequence or the expand/limit-recurrence-set
// alternation.
type calendarDataEl struct {
	ContentType        *string
	Version            *string
	Comp               *calendarComp
	Expand             *calendarRange
	LimitRecurrenceSet *calendarRange
	LimitFreeBusySet   *calendarRange
	// fault carries a content-model violation out of UnmarshalXML. The decode in
	// report() runs permissively over every REPORT body, so only the §9 grammar
	// pass is entitled to turn one into a rejection.
	fault *reportGrammarFault
}

// calendarComp is the CALDAV:comp selection of RFC 4791 §9.6.1,
// ((allprop | prop*), (allcomp | comp*)).
type calendarComp struct {
	Name    string
	AllProp bool
	Prop    []calendarProp
	AllComp bool
	Comp    []calendarComp
}

// calendarProp is the CALDAV:prop selection of RFC 4791 §9.6.4. NoValue asks
// for the property name, its parameters and a trailing colon with the value
// suppressed.
type calendarProp struct {
	Name    string
	NoValue bool
}

// propfindRequest represents a PROPFIND request body (RFC 4918 Section 9.1). It
// also carries the property selection of a DAV:expand-property REPORT, so both
// run through one property filter.
type propfindRequest struct {
	XMLName      xml.Name
	AllProp      *struct{}          `xml:"DAV: allprop"`
	PropName     *struct{}          `xml:"DAV: propname"`
	Prop         *propfindPropQuery `xml:"DAV: prop"`
	suppressData bool
	// expand and expandXML are set only by DAV:expand-property; expandErr carries
	// the first failure back out, because the filter itself returns no error.
	expand    expandPropertyExpander
	expandXML expandXMLPropertyExpander
	expandErr error
}

// propfindPropQuery lists specific properties requested
type propfindPropQuery struct {
	propertySelection
	CalendarData *struct{}         `xml:"urn:ietf:params:xml:ns:caldav calendar-data"`
	AddressData  *addressDataQuery `xml:"urn:ietf:params:xml:ns:carddav address-data"`
}

// calFilter represents a CalDAV calendar-query filter (RFC 4791 Section 9.7)
type calFilter struct {
	CompFilter compFilter `xml:"urn:ietf:params:xml:ns:caldav comp-filter"`
}

// compFilter filters by component type and optionally by time-range. RFC 4791
// §9.7.1 admits no text-match here: text matching is scoped to a property or a
// parameter, never to a whole component.
type compFilter struct {
	Name         string       `xml:"name,attr"`
	IsNotDefined *struct{}    `xml:"urn:ietf:params:xml:ns:caldav is-not-defined"`
	TimeRange    *timeRange   `xml:"urn:ietf:params:xml:ns:caldav time-range"`
	CompFilter   []compFilter `xml:"urn:ietf:params:xml:ns:caldav comp-filter"`
	PropFilter   []propFilter `xml:"urn:ietf:params:xml:ns:caldav prop-filter"`
}

// propFilter filters by property presence and optionally by time-range or
// text-match, each conjoined with every param-filter (RFC 4791 §9.7.2).
type propFilter struct {
	Name         string        `xml:"name,attr"`
	IsNotDefined *struct{}     `xml:"urn:ietf:params:xml:ns:caldav is-not-defined"`
	TimeRange    *timeRange    `xml:"urn:ietf:params:xml:ns:caldav time-range"`
	TextMatch    *textMatch    `xml:"urn:ietf:params:xml:ns:caldav text-match"`
	ParamFilter  []paramFilter `xml:"urn:ietf:params:xml:ns:caldav param-filter"`
}

// paramFilter filters by parameter presence and optionally by text-match
// against that parameter's value (RFC 4791 §9.7.3).
type paramFilter struct {
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
	XMLName      xml.Name
	Instructions []proppatchInstruction
}

type proppatchInstruction struct {
	Remove     bool
	Properties []proppatchProperty
}

type proppatchProperty struct {
	Name     xml.Name
	InnerXML string
	Text     string
	// Lang is the in-scope xml:lang for this property's value, empty when none
	// applies. RFC 4918 §4.3 scopes the attribute in the normal XML way, so a
	// declaration on an ancestor of the property element reaches it.
	Lang       string
	HasElement bool
}

// xmlLangNamespace is the namespace the XML specification reserves for the
// "xml" prefix; encoding/xml resolves xml:lang to it.
const xmlLangNamespace = "http://www.w3.org/XML/1998/namespace"

// inScopeLang returns the xml:lang declared on start, or inherited when start
// declares none.
func inScopeLang(start xml.StartElement, inherited string) string {
	for _, attr := range start.Attr {
		if attr.Name.Local != "lang" {
			continue
		}
		if attr.Name.Space == xmlLangNamespace || attr.Name.Space == "xml" {
			return strings.TrimSpace(attr.Value)
		}
	}
	return inherited
}

func (r *proppatchRequest) UnmarshalXML(dec *xml.Decoder, start xml.StartElement) error {
	if start.Name.Space != "DAV:" || start.Name.Local != "propertyupdate" {
		return fmt.Errorf("unexpected PROPPATCH root %q", xmlNameString(start.Name))
	}
	*r = proppatchRequest{XMLName: start.Name}
	rootLang := inScopeLang(start, "")
	for {
		token, err := dec.Token()
		if err != nil {
			return err
		}
		switch token := token.(type) {
		case xml.StartElement:
			if token.Name.Space != "DAV:" || (token.Name.Local != "set" && token.Name.Local != "remove") {
				return fmt.Errorf("unexpected PROPPATCH instruction %q", xmlNameString(token.Name))
			}
			instruction, err := decodeProppatchInstruction(dec, token, token.Name.Local == "remove", rootLang)
			if err != nil {
				return err
			}
			r.Instructions = append(r.Instructions, instruction)
		case xml.EndElement:
			if token.Name == start.Name {
				return nil
			}
		}
	}
}

func decodeProppatchInstruction(dec *xml.Decoder, start xml.StartElement, remove bool, inheritedLang string) (proppatchInstruction, error) {
	instruction := proppatchInstruction{Remove: remove}
	instructionLang := inScopeLang(start, inheritedLang)
	seenProp := false
	for {
		token, err := dec.Token()
		if err != nil {
			return instruction, err
		}
		switch token := token.(type) {
		case xml.StartElement:
			if seenProp || token.Name.Space != "DAV:" || token.Name.Local != "prop" {
				return instruction, fmt.Errorf("PROPPATCH instruction must contain one DAV:prop")
			}
			seenProp = true
			properties, err := decodeProppatchProperties(dec, token, instructionLang)
			if err != nil {
				return instruction, err
			}
			instruction.Properties = properties
		case xml.EndElement:
			if token.Name == start.Name {
				if !seenProp {
					return instruction, fmt.Errorf("PROPPATCH instruction is missing DAV:prop")
				}
				if len(instruction.Properties) == 0 {
					return instruction, fmt.Errorf("PROPPATCH DAV:prop is empty")
				}
				return instruction, nil
			}
		}
	}
}

func decodeProppatchProperties(dec *xml.Decoder, start xml.StartElement, inheritedLang string) ([]proppatchProperty, error) {
	var properties []proppatchProperty
	propLang := inScopeLang(start, inheritedLang)
	for {
		token, err := dec.Token()
		if err != nil {
			return nil, err
		}
		switch token := token.(type) {
		case xml.StartElement:
			property, err := decodeProppatchProperty(dec, token, propLang)
			if err != nil {
				return nil, err
			}
			properties = append(properties, property)
		case xml.EndElement:
			if token.Name == start.Name {
				return properties, nil
			}
		}
	}
}

func decodeProppatchProperty(dec *xml.Decoder, start xml.StartElement, inheritedLang string) (proppatchProperty, error) {
	var inner strings.Builder
	enc := xml.NewEncoder(&inner)
	var text strings.Builder
	property := proppatchProperty{Name: start.Name, Lang: inScopeLang(start, inheritedLang)}
	for {
		token, err := dec.Token()
		if err != nil {
			return property, err
		}
		switch token := token.(type) {
		case xml.StartElement:
			property.HasElement = true
			if err := enc.EncodeToken(token); err != nil {
				return property, err
			}
		case xml.EndElement:
			if token.Name == start.Name {
				if err := enc.Flush(); err != nil {
					return property, err
				}
				property.InnerXML = inner.String()
				property.Text = strings.TrimSpace(text.String())
				return property, nil
			}
			if err := enc.EncodeToken(token); err != nil {
				return property, err
			}
		case xml.CharData:
			text.Write([]byte(token))
			if err := enc.EncodeToken(token); err != nil {
				return property, err
			}
		case xml.Comment, xml.Directive, xml.ProcInst:
			if err := enc.EncodeToken(token); err != nil {
				return property, err
			}
		}
	}
}

// mkcolRequest is the extended MKCOL body (RFC 5689). MKCALENDAR has its own
// stricter grammar; see mkcalendarRequest.
type mkcolRequest struct {
	XMLName xml.Name
	Set     *mkcolSet `xml:"DAV: set"`
}

type mkcolSet struct {
	Prop proppatchProp `xml:"DAV: prop"`
}

// mkcalendarRequest is the CALDAV:mkcalendar request body (RFC 4791 §9.2). The
// content model is <!ELEMENT mkcalendar (DAV:set)>: no other root element, and
// exactly one DAV:set. §5.3.1 requires the property instructions it carries to
// be processed in document order, so they are decoded into an ordered list
// rather than a struct of named fields.
type mkcalendarRequest struct {
	XMLName      xml.Name
	Instructions []proppatchInstruction
}

func (r *mkcalendarRequest) UnmarshalXML(dec *xml.Decoder, start xml.StartElement) error {
	if start.Name.Space != "urn:ietf:params:xml:ns:caldav" || start.Name.Local != "mkcalendar" {
		return fmt.Errorf("unexpected MKCALENDAR root %q", xmlNameString(start.Name))
	}
	*r = mkcalendarRequest{XMLName: start.Name}
	rootLang := inScopeLang(start, "")
	for {
		token, err := dec.Token()
		if err != nil {
			return err
		}
		switch token := token.(type) {
		case xml.StartElement:
			if token.Name.Space != "DAV:" || token.Name.Local != "set" {
				return fmt.Errorf("unexpected MKCALENDAR instruction %q", xmlNameString(token.Name))
			}
			if len(r.Instructions) != 0 {
				return fmt.Errorf("MKCALENDAR carries more than one DAV:set")
			}
			instruction, err := decodeProppatchInstruction(dec, token, false, rootLang)
			if err != nil {
				return err
			}
			r.Instructions = append(r.Instructions, instruction)
		case xml.CharData:
			// The content model is element-only, so anything but whitespace
			// between the instructions is outside the grammar.
			if strings.TrimSpace(string(token)) != "" {
				return fmt.Errorf("MKCALENDAR carries character data")
			}
		case xml.EndElement:
			if token.Name == start.Name {
				if len(r.Instructions) != 1 {
					return fmt.Errorf("MKCALENDAR carries %d DAV:set elements, want exactly 1", len(r.Instructions))
				}
				return nil
			}
		}
	}
}

// mkcalendarResponse is the CALDAV:mkcalendar-response body RFC 4791 §5.3.1
// requires of a MKCALENDAR response that carries one. §9.3 declares the element
// ANY; CalCard reports the properties it applied, mirroring a PROPPATCH
// propstat so a client can see which instructions took effect.
type mkcalendarResponse struct {
	XMLName   xml.Name   `xml:"cal:mkcalendar-response"`
	XmlnsD    string     `xml:"xmlns:d,attr"`
	XmlnsCal  string     `xml:"xmlns:cal,attr"`
	XmlnsCard string     `xml:"xmlns:card,attr"`
	XmlnsCS   string     `xml:"xmlns:cs,attr"`
	XmlnsICAL string     `xml:"xmlns:ical,attr"`
	Propstat  []propstat `xml:"d:propstat,omitempty"`
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
	CustomXML                  []xml.Name             `xml:",any"`
}

// hrefProp is a property whose value is a single DAV:href. Where the property
// table marks one expandable, DAV:expand-property substitutes a DAV:response
// per referenced resource (RFC 3253 §3.8); that rendering belongs to the
// property filter, so this type carries only the plain value.
type hrefProp struct {
	Href string `xml:"d:href,omitempty"`
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
	CalendarMultiGet           *struct{} `xml:"cal:calendar-multiget,omitempty"`
	CalendarQuery              *struct{} `xml:"cal:calendar-query,omitempty"`
	FreeBusyQuery              *struct{} `xml:"cal:free-busy-query,omitempty"`
	AddressbookMultiGet        *struct{} `xml:"card:addressbook-multiget,omitempty"`
	AddressbookQuery           *struct{} `xml:"card:addressbook-query,omitempty"`
	SyncCollection             *struct{} `xml:"d:sync-collection,omitempty"`
	ExpandProperty             *struct{} `xml:"d:expand-property,omitempty"`
	ACLPrincipalPropSet        *struct{} `xml:"d:acl-principal-prop-set,omitempty"`
	PrincipalMatch             *struct{} `xml:"d:principal-match,omitempty"`
	PrincipalPropertySearch    *struct{} `xml:"d:principal-property-search,omitempty"`
	PrincipalSearchPropertySet *struct{} `xml:"d:principal-search-property-set,omitempty"`
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

// caldavSupportedCollationSet is the CalDAV CALDAV:supported-collation-set
// (RFC 4791 §9.4). It shares its local name with the CardDAV property but not
// its namespace, so the two carry separate children.
type caldavSupportedCollationSet struct {
	SupportedCollation []string `xml:"cal:supported-collation"`
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
	XMLName  xml.Name
	Property []expandPropertyElement `xml:"DAV: property"`
}

type expandPropertyElement struct {
	Name      string                  `xml:"name,attr"`
	Namespace string                  `xml:"namespace,attr"`
	Property  []expandPropertyElement `xml:"DAV: property"`
}

type currentUserPrivilegeSet struct {
	Privileges []privilege `xml:"d:privilege"`
}

type privilege struct {
	All                         *struct{}      `xml:"d:all,omitempty"`
	Read                        *readPrivilege `xml:"d:read,omitempty"`
	ReadFreeBusy                *struct{}      `xml:"cal:read-free-busy,omitempty"`
	Write                       *struct{}      `xml:"d:write,omitempty"`
	WriteContent                *struct{}      `xml:"d:write-content,omitempty"`
	WriteProperties             *struct{}      `xml:"d:write-properties,omitempty"`
	Bind                        *struct{}      `xml:"d:bind,omitempty"`
	Unbind                      *struct{}      `xml:"d:unbind,omitempty"`
	ReadACL                     *struct{}      `xml:"d:read-acl,omitempty"`
	ReadCurrentUserPrivilegeSet *struct{}      `xml:"d:read-current-user-privilege-set,omitempty"`
	WriteACL                    *struct{}      `xml:"d:write-acl,omitempty"`
	Unlock                      *struct{}      `xml:"d:unlock,omitempty"`
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
	XMLName xml.Name               `xml:"DAV: acl"`
	ACE     []ace                  `xml:"DAV: ace"`
	Unknown []unsupportedPrivilege `xml:",any"`
}

func (r *aclRequest) UnmarshalXML(dec *xml.Decoder, start xml.StartElement) error {
	if start.Name != (xml.Name{Space: "DAV:", Local: "acl"}) {
		return fmt.Errorf("unexpected ACL root %q", xmlNameString(start.Name))
	}
	*r = aclRequest{XMLName: start.Name}
	return decodeElementOnly(dec, start, func(child xml.StartElement) error {
		switch child.Name {
		case (xml.Name{Space: "DAV:", Local: "ace"}):
			var value ace
			if err := dec.DecodeElement(&value, &child); err != nil {
				return err
			}
			r.ACE = append(r.ACE, value)
		default:
			var value unsupportedPrivilege
			if err := dec.DecodeElement(&value, &child); err != nil {
				return err
			}
			r.Unknown = append(r.Unknown, value)
		}
		return nil
	})
}

type ace struct {
	Principal []acePrincipal         `xml:"DAV: principal"`
	Invert    []aceInvert            `xml:"DAV: invert"`
	Grant     []aceGrant             `xml:"DAV: grant"`
	Deny      []aceDeny              `xml:"DAV: deny"`
	Protected []emptyElement         `xml:"DAV: protected"`
	Inherited []aceInherited         `xml:"DAV: inherited"`
	Unknown   []unsupportedPrivilege `xml:",any"`
	Sequence  []xml.Name             `xml:"-"`
}

func (a *ace) UnmarshalXML(dec *xml.Decoder, start xml.StartElement) error {
	*a = ace{}
	for {
		token, err := dec.Token()
		if err != nil {
			return err
		}
		switch token := token.(type) {
		case xml.StartElement:
			a.Sequence = append(a.Sequence, token.Name)
			switch token.Name {
			case (xml.Name{Space: "DAV:", Local: "principal"}):
				var value acePrincipal
				if err := dec.DecodeElement(&value, &token); err != nil {
					return err
				}
				a.Principal = append(a.Principal, value)
			case (xml.Name{Space: "DAV:", Local: "invert"}):
				var value aceInvert
				if err := dec.DecodeElement(&value, &token); err != nil {
					return err
				}
				a.Invert = append(a.Invert, value)
			case (xml.Name{Space: "DAV:", Local: "grant"}):
				var value aceGrant
				if err := dec.DecodeElement(&value, &token); err != nil {
					return err
				}
				a.Grant = append(a.Grant, value)
			case (xml.Name{Space: "DAV:", Local: "deny"}):
				var value aceDeny
				if err := dec.DecodeElement(&value, &token); err != nil {
					return err
				}
				a.Deny = append(a.Deny, value)
			case (xml.Name{Space: "DAV:", Local: "protected"}):
				var value emptyElement
				if err := dec.DecodeElement(&value, &token); err != nil {
					return err
				}
				a.Protected = append(a.Protected, value)
			case (xml.Name{Space: "DAV:", Local: "inherited"}):
				var value aceInherited
				if err := dec.DecodeElement(&value, &token); err != nil {
					return err
				}
				a.Inherited = append(a.Inherited, value)
			default:
				var value unsupportedPrivilege
				if err := dec.DecodeElement(&value, &token); err != nil {
					return err
				}
				a.Unknown = append(a.Unknown, value)
			}
		case xml.CharData:
			if strings.TrimSpace(string(token)) != "" {
				return fmt.Errorf("unexpected character data in DAV:ace")
			}
		case xml.EndElement:
			if token.Name == start.Name {
				return nil
			}
		}
	}
}

type acePrincipal struct {
	Href            []string               `xml:"DAV: href"`
	All             []emptyElement         `xml:"DAV: all"`
	Authenticated   []emptyElement         `xml:"DAV: authenticated"`
	Unauthenticated []emptyElement         `xml:"DAV: unauthenticated"`
	Property        []acePrincipalProperty `xml:"DAV: property"`
	Self            []emptyElement         `xml:"DAV: self"`
	Unknown         []unsupportedPrivilege `xml:",any"`
}

func (p *acePrincipal) UnmarshalXML(dec *xml.Decoder, start xml.StartElement) error {
	*p = acePrincipal{}
	return decodeElementOnly(dec, start, func(child xml.StartElement) error {
		switch child.Name {
		case (xml.Name{Space: "DAV:", Local: "href"}):
			value, err := decodeTextOnly(dec, child)
			if err != nil {
				return err
			}
			p.Href = append(p.Href, value)
		case (xml.Name{Space: "DAV:", Local: "all"}):
			return decodeEmptyElement(dec, child, &p.All)
		case (xml.Name{Space: "DAV:", Local: "authenticated"}):
			return decodeEmptyElement(dec, child, &p.Authenticated)
		case (xml.Name{Space: "DAV:", Local: "unauthenticated"}):
			return decodeEmptyElement(dec, child, &p.Unauthenticated)
		case (xml.Name{Space: "DAV:", Local: "property"}):
			var value acePrincipalProperty
			if err := dec.DecodeElement(&value, &child); err != nil {
				return err
			}
			p.Property = append(p.Property, value)
		case (xml.Name{Space: "DAV:", Local: "self"}):
			return decodeEmptyElement(dec, child, &p.Self)
		default:
			return decodeUnsupportedElement(dec, child, &p.Unknown)
		}
		return nil
	})
}

type aceInvert struct {
	Principal []acePrincipal         `xml:"DAV: principal"`
	Unknown   []unsupportedPrivilege `xml:",any"`
}

func (i *aceInvert) UnmarshalXML(dec *xml.Decoder, start xml.StartElement) error {
	*i = aceInvert{}
	return decodeElementOnly(dec, start, func(child xml.StartElement) error {
		if child.Name == (xml.Name{Space: "DAV:", Local: "principal"}) {
			var value acePrincipal
			if err := dec.DecodeElement(&value, &child); err != nil {
				return err
			}
			i.Principal = append(i.Principal, value)
			return nil
		}
		return decodeUnsupportedElement(dec, child, &i.Unknown)
	})
}

type acePrincipalProperty struct {
	Owner   []emptyElement         `xml:"DAV: owner"`
	Group   []emptyElement         `xml:"DAV: group"`
	Unknown []unsupportedPrivilege `xml:",any"`
}

func (p *acePrincipalProperty) UnmarshalXML(dec *xml.Decoder, start xml.StartElement) error {
	*p = acePrincipalProperty{}
	return decodeElementOnly(dec, start, func(child xml.StartElement) error {
		switch child.Name {
		case (xml.Name{Space: "DAV:", Local: "owner"}):
			return decodeEmptyElement(dec, child, &p.Owner)
		case (xml.Name{Space: "DAV:", Local: "group"}):
			return decodeEmptyElement(dec, child, &p.Group)
		default:
			return decodeUnsupportedElement(dec, child, &p.Unknown)
		}
	})
}

type aceInherited struct {
	Href    []string               `xml:"DAV: href"`
	Unknown []unsupportedPrivilege `xml:",any"`
}

func (i *aceInherited) UnmarshalXML(dec *xml.Decoder, start xml.StartElement) error {
	*i = aceInherited{}
	return decodeElementOnly(dec, start, func(child xml.StartElement) error {
		if child.Name == (xml.Name{Space: "DAV:", Local: "href"}) {
			value, err := decodeTextOnly(dec, child)
			if err != nil {
				return err
			}
			i.Href = append(i.Href, value)
			return nil
		}
		return decodeUnsupportedElement(dec, child, &i.Unknown)
	})
}

type aceGrant struct {
	Privileges []acePrivilege         `xml:"DAV: privilege"`
	Unknown    []unsupportedPrivilege `xml:",any"`
}

func (g *aceGrant) UnmarshalXML(dec *xml.Decoder, start xml.StartElement) error {
	*g = aceGrant{}
	return decodeElementOnly(dec, start, func(child xml.StartElement) error {
		if child.Name == (xml.Name{Space: "DAV:", Local: "privilege"}) {
			var value acePrivilege
			if err := dec.DecodeElement(&value, &child); err != nil {
				return err
			}
			g.Privileges = append(g.Privileges, value)
			return nil
		}
		return decodeUnsupportedElement(dec, child, &g.Unknown)
	})
}

type aceDeny struct {
	Privileges []acePrivilege         `xml:"DAV: privilege"`
	Unknown    []unsupportedPrivilege `xml:",any"`
}

func (d *aceDeny) UnmarshalXML(dec *xml.Decoder, start xml.StartElement) error {
	*d = aceDeny{}
	return decodeElementOnly(dec, start, func(child xml.StartElement) error {
		if child.Name == (xml.Name{Space: "DAV:", Local: "privilege"}) {
			var value acePrivilege
			if err := dec.DecodeElement(&value, &child); err != nil {
				return err
			}
			d.Privileges = append(d.Privileges, value)
			return nil
		}
		return decodeUnsupportedElement(dec, child, &d.Unknown)
	})
}

type acePrivilege struct {
	Read                        *emptyElement          `xml:"DAV: read,omitempty"`
	Write                       *emptyElement          `xml:"DAV: write,omitempty"`
	WriteContent                *emptyElement          `xml:"DAV: write-content,omitempty"`
	WriteProperties             *emptyElement          `xml:"DAV: write-properties,omitempty"`
	ReadACL                     *emptyElement          `xml:"DAV: read-acl,omitempty"`
	ReadCurrentUserPrivilegeSet *emptyElement          `xml:"DAV: read-current-user-privilege-set,omitempty"`
	WriteACL                    *emptyElement          `xml:"DAV: write-acl,omitempty"`
	Unlock                      *emptyElement          `xml:"DAV: unlock,omitempty"`
	Bind                        *emptyElement          `xml:"DAV: bind,omitempty"`
	Unbind                      *emptyElement          `xml:"DAV: unbind,omitempty"`
	ReadFreeBusy                *emptyElement          `xml:"urn:ietf:params:xml:ns:caldav read-free-busy,omitempty"`
	All                         *emptyElement          `xml:"DAV: all,omitempty"`
	Unknown                     []unsupportedPrivilege `xml:",any"`
	Elements                    int                    `xml:"-"`
}

func (p *acePrivilege) UnmarshalXML(dec *xml.Decoder, start xml.StartElement) error {
	*p = acePrivilege{}
	return decodeElementOnly(dec, start, func(child xml.StartElement) error {
		p.Elements++
		switch child.Name {
		case (xml.Name{Space: "DAV:", Local: "read"}):
			return decodeEmptyElementPointer(dec, child, &p.Read)
		case (xml.Name{Space: "DAV:", Local: "write"}):
			return decodeEmptyElementPointer(dec, child, &p.Write)
		case (xml.Name{Space: "DAV:", Local: "write-content"}):
			return decodeEmptyElementPointer(dec, child, &p.WriteContent)
		case (xml.Name{Space: "DAV:", Local: "write-properties"}):
			return decodeEmptyElementPointer(dec, child, &p.WriteProperties)
		case (xml.Name{Space: "DAV:", Local: "read-acl"}):
			return decodeEmptyElementPointer(dec, child, &p.ReadACL)
		case (xml.Name{Space: "DAV:", Local: "read-current-user-privilege-set"}):
			return decodeEmptyElementPointer(dec, child, &p.ReadCurrentUserPrivilegeSet)
		case (xml.Name{Space: "DAV:", Local: "write-acl"}):
			return decodeEmptyElementPointer(dec, child, &p.WriteACL)
		case (xml.Name{Space: "DAV:", Local: "unlock"}):
			return decodeEmptyElementPointer(dec, child, &p.Unlock)
		case (xml.Name{Space: "DAV:", Local: "bind"}):
			return decodeEmptyElementPointer(dec, child, &p.Bind)
		case (xml.Name{Space: "DAV:", Local: "unbind"}):
			return decodeEmptyElementPointer(dec, child, &p.Unbind)
		case (xml.Name{Space: "urn:ietf:params:xml:ns:caldav", Local: "read-free-busy"}):
			return decodeEmptyElementPointer(dec, child, &p.ReadFreeBusy)
		case (xml.Name{Space: "DAV:", Local: "all"}):
			return decodeEmptyElementPointer(dec, child, &p.All)
		default:
			return decodeUnsupportedElement(dec, child, &p.Unknown)
		}
	})
}

func decodeElementOnly(dec *xml.Decoder, start xml.StartElement, decodeChild func(xml.StartElement) error) error {
	for {
		token, err := dec.Token()
		if err != nil {
			return err
		}
		switch token := token.(type) {
		case xml.StartElement:
			if err := decodeChild(token); err != nil {
				return err
			}
		case xml.CharData:
			if strings.TrimSpace(string(token)) != "" {
				return fmt.Errorf("unexpected character data in %q", xmlNameString(start.Name))
			}
		case xml.EndElement:
			if token.Name == start.Name {
				return nil
			}
		}
	}
}

func decodeTextOnly(dec *xml.Decoder, start xml.StartElement) (string, error) {
	var value strings.Builder
	for {
		token, err := dec.Token()
		if err != nil {
			return "", err
		}
		switch token := token.(type) {
		case xml.StartElement:
			return "", fmt.Errorf("unexpected nested element %q", xmlNameString(token.Name))
		case xml.CharData:
			value.Write(token)
		case xml.EndElement:
			if token.Name == start.Name {
				return value.String(), nil
			}
		}
	}
}

func decodeEmptyElement(dec *xml.Decoder, start xml.StartElement, values *[]emptyElement) error {
	var value emptyElement
	if err := dec.DecodeElement(&value, &start); err != nil {
		return err
	}
	*values = append(*values, value)
	return nil
}

func decodeEmptyElementPointer(dec *xml.Decoder, start xml.StartElement, value **emptyElement) error {
	var decoded emptyElement
	if err := dec.DecodeElement(&decoded, &start); err != nil {
		return err
	}
	*value = &decoded
	return nil
}

func decodeUnsupportedElement(dec *xml.Decoder, start xml.StartElement, values *[]unsupportedPrivilege) error {
	var value unsupportedPrivilege
	if err := dec.DecodeElement(&value, &start); err != nil {
		return err
	}
	*values = append(*values, value)
	return nil
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
	Principal acePrincipalResp  `xml:"d:principal"`
	Grant     *aceGrantResp     `xml:"d:grant,omitempty"`
	Deny      *aceDenyResp      `xml:"d:deny,omitempty"`
	Protected *struct{}         `xml:"d:protected,omitempty"`
	Inherited *aceInheritedResp `xml:"d:inherited,omitempty"`
}

type aceInheritedResp struct {
	Href string `xml:"d:href"`
}

type acePrincipalResp struct {
	Href            string                    `xml:"d:href,omitempty"`
	All             *struct{}                 `xml:"d:all,omitempty"`
	Authenticated   *struct{}                 `xml:"d:authenticated,omitempty"`
	Unauthenticated *struct{}                 `xml:"d:unauthenticated,omitempty"`
	Property        *acePrincipalPropertyResp `xml:"d:property,omitempty"`
	Self            *struct{}                 `xml:"d:self,omitempty"`
}

type acePrincipalPropertyResp struct {
	Owner *struct{} `xml:"d:owner,omitempty"`
	Group *struct{} `xml:"d:group,omitempty"`
}

type aceGrantResp struct {
	Privileges []acePrivilegeResp `xml:"d:privilege"`
}

type aceDenyResp struct {
	Privileges []acePrivilegeResp `xml:"d:privilege"`
}

type acePrivilegeResp struct {
	Read                        *struct{} `xml:"d:read,omitempty"`
	Write                       *struct{} `xml:"d:write,omitempty"`
	WriteContent                *struct{} `xml:"d:write-content,omitempty"`
	WriteProperties             *struct{} `xml:"d:write-properties,omitempty"`
	ReadACL                     *struct{} `xml:"d:read-acl,omitempty"`
	ReadCurrentUserPrivilegeSet *struct{} `xml:"d:read-current-user-privilege-set,omitempty"`
	WriteACL                    *struct{} `xml:"d:write-acl,omitempty"`
	Unlock                      *struct{} `xml:"d:unlock,omitempty"`
	Bind                        *struct{} `xml:"d:bind,omitempty"`
	Unbind                      *struct{} `xml:"d:unbind,omitempty"`
	ReadFreeBusy                *struct{} `xml:"cal:read-free-busy,omitempty"`
	All                         *struct{} `xml:"d:all,omitempty"`
}

type supportedPrivilegeSetProp struct {
	SupportedPrivileges []supportedPrivilege `xml:"d:supported-privilege"`
}

type supportedPrivilege struct {
	Privilege   supportedPrivilegeType `xml:"d:privilege"`
	Description langString             `xml:"d:description"`
	SubPrivs    []supportedPrivilege   `xml:"d:supported-privilege,omitempty"`
}

type supportedPrivilegeType struct {
	Read                        *struct{} `xml:"d:read,omitempty"`
	Write                       *struct{} `xml:"d:write,omitempty"`
	WriteContent                *struct{} `xml:"d:write-content,omitempty"`
	WriteProperties             *struct{} `xml:"d:write-properties,omitempty"`
	ReadACL                     *struct{} `xml:"d:read-acl,omitempty"`
	ReadCurrentUserPrivilegeSet *struct{} `xml:"d:read-current-user-privilege-set,omitempty"`
	WriteACL                    *struct{} `xml:"d:write-acl,omitempty"`
	Unlock                      *struct{} `xml:"d:unlock,omitempty"`
	Bind                        *struct{} `xml:"d:bind,omitempty"`
	Unbind                      *struct{} `xml:"d:unbind,omitempty"`
	All                         *struct{} `xml:"d:all,omitempty"`
	ReadFreeBusy                *struct{} `xml:"cal:read-free-busy,omitempty"`
}

type aclRestrictionsProp struct {
	NoInvert *struct{} `xml:"d:no-invert,omitempty"`
}
