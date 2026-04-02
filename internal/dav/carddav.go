package dav

import (
	"encoding/xml"
	"fmt"
	"net/http"
	"strconv"
	"strings"

	"github.com/jw6ventures/calcard/internal/store"
	"golang.org/x/text/cases"
)

var supportedCardDAVCollations = map[string]struct{}{
	"":                  {},
	"default":           {},
	"i;ascii-casemap":   {},
	"i;unicode-casemap": {},
}

var supportedCardDAVFilterProps = map[string]struct{}{
	"ADR":          {},
	"ANNIVERSARY":  {},
	"BDAY":         {},
	"CATEGORIES":   {},
	"EMAIL":        {},
	"FBURL":        {},
	"FN":           {},
	"GENDER":       {},
	"GEO":          {},
	"IMPP":         {},
	"KIND":         {},
	"KEY":          {},
	"LANG":         {},
	"LOGO":         {},
	"MEMBER":       {},
	"N":            {},
	"NICKNAME":     {},
	"NOTE":         {},
	"ORG":          {},
	"PHOTO":        {},
	"PRODID":       {},
	"RELATED":      {},
	"REV":          {},
	"ROLE":         {},
	"SOUND":        {},
	"SOURCE":       {},
	"TEL":          {},
	"TITLE":        {},
	"TZ":           {},
	"UID":          {},
	"URL":          {},
	"XML":          {},
	"CALADRURI":    {},
	"CALURI":       {},
	"CLIENTPIDMAP": {},
}

type vcardProperty struct {
	Name   string
	Params map[string][]string
	Value  string
	Raw    string
}

func parseAddressBookReportPath(cleanPath string) (bookID int64, resourceUID string, isResource bool, err error) {
	trimmed := strings.Trim(strings.TrimPrefix(cleanPath, "/dav/addressbooks"), "/")
	if trimmed == "" {
		return 0, "", false, errInvalidPath
	}
	parts := strings.Split(trimmed, "/")
	if len(parts) == 0 || strings.TrimSpace(parts[0]) == "" {
		return 0, "", false, errInvalidPath
	}
	bookID, err = parseCollectionID(parts[0])
	if err != nil {
		return 0, "", false, err
	}
	if len(parts) == 1 {
		return bookID, "", false, nil
	}
	if len(parts) == 2 && parts[1] != "" {
		return bookID, strings.TrimSuffix(parts[1], pathExt(parts[1])), true, nil
	}
	return 0, "", false, errInvalidPath
}

func parseCollectionID(raw string) (int64, error) {
	id, err := strconv.ParseInt(raw, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("%w: invalid address book id", errInvalidPath)
	}
	return id, nil
}

func pathExt(raw string) string {
	if idx := strings.LastIndexByte(raw, '.'); idx >= 0 {
		return raw[idx:]
	}
	return ""
}

func validateAddressDataRequest(query *addressDataQuery) error {
	if query == nil {
		return nil
	}
	contentType := strings.ToLower(strings.TrimSpace(query.ContentType))
	version := strings.TrimSpace(query.Version)
	if contentType != "" && contentType != "text/vcard" {
		return fmt.Errorf("unsupported address-data type")
	}
	if version != "" && version != "3.0" && version != "4.0" {
		return fmt.Errorf("unsupported address-data version")
	}
	return nil
}

func extractVCardVersion(raw string) (string, error) {
	lines := unfoldICalLines(raw)
	version := ""
	versionCount := 0
	for _, line := range lines {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}
		upper := strings.ToUpper(line)
		if !strings.HasPrefix(upper, "VERSION:") {
			continue
		}
		versionCount++
		version = strings.TrimSpace(line[len("VERSION:"):])
	}
	switch versionCount {
	case 0:
		return "", fmt.Errorf("no VERSION property found in vCard data")
	case 1:
		if version == "" {
			return "", fmt.Errorf("empty VERSION property")
		}
		return version, nil
	default:
		return "", fmt.Errorf("multiple VERSION properties found in vCard data")
	}
}

func canServeRequestedAddressData(raw string, query *addressDataQuery) bool {
	if query == nil || strings.TrimSpace(query.Version) == "" {
		return true
	}
	version, err := extractVCardVersion(raw)
	if err != nil {
		return false
	}
	return version == strings.TrimSpace(query.Version)
}

func acceptsVCardData(rawVCard, acceptHeader string) bool {
	acceptHeader = strings.TrimSpace(acceptHeader)
	if acceptHeader == "" {
		return true
	}

	rawVersion, err := extractVCardVersion(rawVCard)
	if err != nil {
		return false
	}

	for _, rawRange := range strings.Split(acceptHeader, ",") {
		parts := strings.Split(rawRange, ";")
		mediaType := strings.ToLower(strings.TrimSpace(parts[0]))
		quality := 1.0
		switch mediaType {
		case "*/*", "text/*", "text/vcard":
		default:
			continue
		}

		requestedVersion := ""
		for _, part := range parts[1:] {
			param := strings.SplitN(strings.TrimSpace(part), "=", 2)
			if len(param) != 2 {
				continue
			}
			name := strings.TrimSpace(param[0])
			value := strings.Trim(strings.TrimSpace(param[1]), `"`)
			if strings.EqualFold(name, "q") {
				if parsed, err := strconv.ParseFloat(value, 64); err == nil {
					quality = parsed
				}
				continue
			}
			if strings.EqualFold(name, "version") {
				requestedVersion = value
				break
			}
		}
		if quality <= 0 {
			continue
		}
		if requestedVersion == "" || requestedVersion == rawVersion {
			return true
		}
	}

	return false
}

func validateCardFilter(filter *cardFilter) error {
	if filter == nil {
		return nil
	}
	for _, propFilter := range filter.PropFilter {
		name := strings.ToUpper(strings.TrimSpace(propFilter.Name))
		baseName := vcardPropertyBaseName(name)
		// Allow standard vCard properties from the allowlist and any X- extension
		// properties (RFC 6352 Section 8.5: servers MAY support non-standard properties).
		if _, ok := supportedCardDAVFilterProps[baseName]; !ok && !strings.HasPrefix(baseName, "X-") {
			return fmt.Errorf("unsupported filter")
		}
		if propFilter.TextMatch != nil {
			if _, ok := supportedCardDAVCollations[strings.ToLower(strings.TrimSpace(propFilter.TextMatch.Collation))]; !ok {
				return fmt.Errorf("unsupported collation")
			}
		}
		for _, paramFilter := range propFilter.ParamFilter {
			paramName := strings.ToUpper(strings.TrimSpace(paramFilter.Name))
			if _, ok := supportedCardDAVFilterParams[paramName]; !ok {
				return fmt.Errorf("unsupported filter")
			}
			if paramFilter.TextMatch == nil {
				continue
			}
			if _, ok := supportedCardDAVCollations[strings.ToLower(strings.TrimSpace(paramFilter.TextMatch.Collation))]; !ok {
				return fmt.Errorf("unsupported collation")
			}
		}
	}
	return nil
}

var supportedCardDAVFilterParams = map[string]struct{}{
	"TYPE":      {},
	"VALUE":     {},
	"PREF":      {},
	"LANGUAGE":  {},
	"ALTID":     {},
	"PID":       {},
	"MEDIATYPE": {},
	"CALSCALE":  {},
	"SORT-AS":   {},
	"GEO":       {},
	"TZ":        {},
}

func parseExpandPropertyRequest(body []byte) (*expandPropertyRequest, error) {
	if len(body) == 0 {
		return nil, nil
	}
	var req expandPropertyRequest
	if err := safeUnmarshalXML(body, &req); err != nil {
		return nil, err
	}
	return &req, nil
}

type expandPropertySelection struct {
	CurrentUserPrincipal *propfindRequest
	PrincipalURL         *propfindRequest
}

func expandPropertySelections(req *expandPropertyRequest) expandPropertySelection {
	var selection expandPropertySelection
	if req == nil {
		return selection
	}
	if req.Prop.CurrentUserPrincipal != nil {
		selection.CurrentUserPrincipal = &propfindRequest{Prop: req.Prop.CurrentUserPrincipal.Prop}
	}
	if req.Prop.PrincipalURL != nil {
		selection.PrincipalURL = &propfindRequest{Prop: req.Prop.PrincipalURL.Prop}
	}
	for _, property := range req.Property {
		propReq := &propfindRequest{Prop: propfindQueryFromExpandProperties(property.Property)}
		switch {
		case property.Namespace == "DAV:" && property.Name == "current-user-principal":
			selection.CurrentUserPrincipal = propReq
		case property.Namespace == "DAV:" && property.Name == "principal-URL":
			selection.PrincipalURL = propReq
		}
	}
	return selection
}

func propfindQueryFromExpandProperties(properties []expandPropertyElement) *propfindPropQuery {
	if len(properties) == 0 {
		return nil
	}
	query := &propfindPropQuery{}
	for _, property := range properties {
		switch {
		case property.Namespace == "DAV:" && property.Name == "displayname":
			query.DisplayName = &struct{}{}
		case property.Namespace == "DAV:" && property.Name == "resourcetype":
			query.ResourceType = &struct{}{}
		case property.Namespace == "DAV:" && property.Name == "current-user-principal":
			query.CurrentUserPrincipal = &struct{}{}
		case property.Namespace == "DAV:" && property.Name == "current-user-principal-URL":
			query.CurrentUserPrincipalURL = &struct{}{}
		case property.Namespace == "DAV:" && property.Name == "principal-URL":
			query.PrincipalURL = &struct{}{}
		case property.Namespace == "DAV:" && property.Name == "supported-report-set":
			query.SupportedReportSet = &struct{}{}
		case property.Namespace == "DAV:" && property.Name == "lockdiscovery":
			query.LockDiscovery = &struct{}{}
		case property.Namespace == "DAV:" && property.Name == "supportedlock":
			query.SupportedLock = &struct{}{}
		case property.Namespace == "DAV:" && property.Name == "acl":
			query.ACLProp = &struct{}{}
		case property.Namespace == "DAV:" && property.Name == "supported-privilege-set":
			query.SupportedPrivilegeSet = &struct{}{}
		case property.Namespace == "DAV:" && property.Name == "principal-collection-set":
			query.PrincipalCollectionSet = &struct{}{}
		case property.Namespace == "DAV:" && property.Name == "current-user-privilege-set":
			query.CurrentUserPrivilegeSet = &struct{}{}
		case property.Namespace == "urn:ietf:params:xml:ns:caldav" && property.Name == "calendar-home-set":
			query.CalendarHomeSet = &struct{}{}
		case property.Namespace == "urn:ietf:params:xml:ns:carddav" && property.Name == "addressbook-home-set":
			query.AddressbookHomeSet = &struct{}{}
		case property.Namespace == "urn:ietf:params:xml:ns:carddav" && property.Name == "principal-address":
			query.PrincipalAddress = &struct{}{}
		}
	}
	return query
}

func parseVCardProperties(raw string) []vcardProperty {
	lines := unfoldICalLines(raw)
	props := make([]vcardProperty, 0, len(lines))
	for _, line := range lines {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}
		upper := strings.ToUpper(line)
		if upper == "BEGIN:VCARD" || upper == "END:VCARD" {
			continue
		}
		colonIdx := strings.IndexByte(line, ':')
		if colonIdx == -1 {
			continue
		}
		head := line[:colonIdx]
		value := line[colonIdx+1:]
		parts := strings.Split(head, ";")
		if len(parts) == 0 {
			continue
		}
		name := strings.ToUpper(strings.TrimSpace(parts[0]))
		params := make(map[string][]string)
		for _, rawParam := range parts[1:] {
			if rawParam == "" {
				continue
			}
			paramParts := strings.SplitN(rawParam, "=", 2)
			paramName := strings.ToUpper(strings.TrimSpace(paramParts[0]))
			if len(paramParts) == 1 {
				params[paramName] = append(params[paramName], "")
				continue
			}
			for _, valuePart := range strings.Split(paramParts[1], ",") {
				params[paramName] = append(params[paramName], strings.TrimSpace(valuePart))
			}
		}
		props = append(props, vcardProperty{
			Name:   name,
			Params: params,
			Value:  value,
			Raw:    line,
		})
	}
	return props
}

// vcardPropertyBaseName returns the base property name stripping any group
// prefix. For example "X-ABC.TEL" returns "TEL", "TEL" returns "TEL".
func vcardPropertyBaseName(name string) string {
	if dot := strings.LastIndexByte(name, '.'); dot >= 0 {
		return name[dot+1:]
	}
	return name
}

// vcardNameMatches checks whether a vCard property full name (potentially
// group-prefixed) matches a requested name using RFC 6352 Section 10.4.2/10.5.1
// semantics: an ungrouped request matches both ungrouped and any group-prefixed
// variant; a grouped request matches only that exact group.
func vcardNameMatches(fullName, requested string) bool {
	if fullName == requested {
		return true
	}
	// If the requested name is grouped (contains a dot), only exact match.
	if strings.ContainsRune(requested, '.') {
		return false
	}
	// Ungrouped request: also match if the base name (after dot) equals it.
	return vcardPropertyBaseName(fullName) == requested
}

func filterVCardData(raw string, query *addressDataQuery) string {
	if query == nil || query.AllProp != nil || len(query.Prop) == 0 {
		return raw
	}

	type selEntry struct {
		name    string
		noValue string
	}
	entries := make([]selEntry, 0, len(query.Prop))
	for _, prop := range query.Prop {
		entries = append(entries, selEntry{
			name:    strings.ToUpper(strings.TrimSpace(prop.Name)),
			noValue: strings.ToLower(strings.TrimSpace(prop.NoValue)),
		})
	}

	lines := unfoldICalLines(raw)
	out := make([]string, 0, len(lines))
	for _, line := range lines {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}
		upper := strings.ToUpper(line)
		if upper == "BEGIN:VCARD" || upper == "END:VCARD" || strings.HasPrefix(upper, "VERSION:") {
			out = append(out, line)
			continue
		}
		colonIdx := strings.IndexByte(line, ':')
		if colonIdx == -1 {
			continue
		}
		head := line[:colonIdx]
		name := strings.ToUpper(head)
		if semi := strings.IndexByte(name, ';'); semi >= 0 {
			name = name[:semi]
		}
		matched := false
		noValue := ""
		for _, entry := range entries {
			if vcardNameMatches(name, entry.name) {
				matched = true
				noValue = entry.noValue
				break
			}
		}
		if !matched {
			continue
		}
		if noValue == "yes" {
			out = append(out, head+":")
			continue
		}
		out = append(out, line)
	}
	if len(out) == 0 {
		return raw
	}
	return strings.Join(out, "\r\n") + "\r\n"
}

// collationFold applies the appropriate case folding for the given collation.
// For "i;ascii-casemap" it uses ASCII-only uppercasing per RFC 4790.
// For "i;unicode-casemap" (or empty/default) it uses Unicode case folding per RFC 5051.
func collationFold(s, collation string) string {
	switch strings.ToLower(strings.TrimSpace(collation)) {
	case "i;ascii-casemap":
		return strings.ToUpper(s)
	default:
		return cases.Fold().String(s)
	}
}

func matchTextValue(value string, textMatch *textMatch) bool {
	if textMatch == nil {
		return true
	}
	collation := ""
	if textMatch.Collation != "" {
		collation = textMatch.Collation
	}
	candidate := collationFold(value, collation)
	needle := collationFold(strings.TrimSpace(textMatch.Text), collation)
	matchType := strings.ToLower(strings.TrimSpace(textMatch.MatchType))
	matches := false
	switch matchType {
	case "", "contains":
		matches = strings.Contains(candidate, needle)
	case "equals":
		matches = candidate == needle
	case "starts-with":
		matches = strings.HasPrefix(candidate, needle)
	case "ends-with":
		matches = strings.HasSuffix(candidate, needle)
	default:
		matches = strings.Contains(candidate, needle)
	}
	if strings.EqualFold(strings.TrimSpace(textMatch.NegateCondition), "yes") {
		return !matches
	}
	return matches
}

func contactMatchesCardFilter(contact store.Contact, filter *cardFilter) bool {
	if filter == nil || len(filter.PropFilter) == 0 {
		return true
	}
	props := parseVCardProperties(contact.RawVCard)
	matches := make([]bool, 0, len(filter.PropFilter))
	for _, propFilter := range filter.PropFilter {
		matches = append(matches, matchesCardPropFilter(props, propFilter))
	}
	if strings.EqualFold(strings.TrimSpace(filter.Test), "allof") {
		for _, matched := range matches {
			if !matched {
				return false
			}
		}
		return true
	}
	for _, matched := range matches {
		if matched {
			return true
		}
	}
	return false
}

func matchesCardPropFilter(props []vcardProperty, filter cardPropFilter) bool {
	target := strings.ToUpper(strings.TrimSpace(filter.Name))
	candidates := make([]vcardProperty, 0, len(props))
	for _, prop := range props {
		if vcardNameMatches(prop.Name, target) {
			candidates = append(candidates, prop)
		}
	}
	if filter.IsNotDefined != nil {
		return len(candidates) == 0
	}
	if len(candidates) == 0 {
		return false
	}
	for _, prop := range candidates {
		if matchesCardProp(prop, filter) {
			return true
		}
	}
	return false
}

func matchesCardProp(prop vcardProperty, filter cardPropFilter) bool {
	var checks []bool
	if filter.TextMatch != nil {
		checks = append(checks, matchTextValue(prop.Value, filter.TextMatch))
	}
	for _, paramFilter := range filter.ParamFilter {
		checks = append(checks, matchesCardParamFilter(prop, paramFilter))
	}
	if len(checks) == 0 {
		return true
	}
	if strings.EqualFold(strings.TrimSpace(filter.Test), "allof") {
		for _, matched := range checks {
			if !matched {
				return false
			}
		}
		return true
	}
	for _, matched := range checks {
		if matched {
			return true
		}
	}
	return false
}

func matchesCardParamFilter(prop vcardProperty, filter cardParamFilter) bool {
	name := strings.ToUpper(strings.TrimSpace(filter.Name))
	values := prop.Params[name]
	if filter.IsNotDefined != nil {
		return len(values) == 0
	}
	if len(values) == 0 {
		return false
	}
	if filter.TextMatch == nil {
		return true
	}
	for _, value := range values {
		if matchTextValue(value, filter.TextMatch) {
			return true
		}
	}
	return false
}

func reportAddressData(report reportRequest) *addressDataQuery {
	if report.Prop != nil && report.Prop.AddressData != nil {
		return report.Prop.AddressData
	}
	return report.AddressData
}

func hasAddressObjectRequestedProps(req *reportProp, topLevelAddressData *addressDataQuery) bool {
	return topLevelAddressData != nil || (req != nil && (req.GetETag != nil || req.GetContentType != nil || req.AddressData != nil || req.DisplayName != nil || req.SupportedReport != nil))
}

func effectiveAddressDataRequest(req *reportProp, topLevelAddressData *addressDataQuery) *addressDataQuery {
	if req != nil && req.AddressData != nil {
		return req.AddressData
	}
	return topLevelAddressData
}

func buildAddressObjectReportResponse(href string, contact store.Contact, req *reportProp, topLevelAddressData *addressDataQuery) response {
	addressDataReq := effectiveAddressDataRequest(req, topLevelAddressData)
	if addressDataReq != nil && !canServeRequestedAddressData(contact.RawVCard, addressDataReq) {
		return response{
			Href:   href,
			Status: httpStatusNotAcceptable,
			Error:  &responseError{SupportedAddressDataConversion: &struct{}{}},
		}
	}

	if !hasAddressObjectRequestedProps(req, topLevelAddressData) {
		return resourceResponse(href, etagProp(contact.ETag, contact.RawVCard, false))
	}
	if req == nil {
		return resourceResponse(href, etagProp(contact.ETag, filterVCardData(contact.RawVCard, addressDataReq), false))
	}

	var okProp prop
	var okSet bool
	var notFoundProp prop
	var notFoundSet bool

	if req.GetETag != nil {
		okProp.GetETag = `"` + contact.ETag + `"`
		okSet = true
	}
	if req.GetContentType != nil {
		okProp.GetContentType = "text/vcard; charset=utf-8"
		okSet = true
	}
	if addressDataReq != nil {
		okProp.AddressData = cdataString(filterVCardData(contact.RawVCard, addressDataReq))
		okSet = true
	}
	if req.SupportedReport != nil {
		okProp.SupportedReportSet = addressbookSupportedReports()
		okSet = true
	}
	if req.DisplayName != nil {
		notFoundProp.DisplayName = "displayname"
		notFoundSet = true
	}

	resp := response{Href: href}
	if okSet {
		resp.Propstat = append(resp.Propstat, propstat{Prop: okProp, Status: httpStatusOK})
	}
	if notFoundSet {
		resp.Propstat = append(resp.Propstat, propstat{Prop: notFoundProp, Status: httpStatusNotFound})
	}
	return resp
}

func buildAddressObjectExpandPropertyResponse(href string, contact store.Contact, req *expandPropertyRequest) response {
	resp := resourceResponse(href, addressBookResourcePropstat(contact.ETag, contact.RawVCard, true))
	if req == nil {
		return resp
	}

	var notFoundProp prop
	var notFoundSet bool
	if req.Prop.CurrentUserPrincipal != nil {
		notFoundProp.CurrentUserPrincipal = &expandableHrefProp{}
		notFoundSet = true
	}
	if req.Prop.PrincipalURL != nil {
		notFoundProp.PrincipalURL = &expandableHrefProp{}
		notFoundSet = true
	}
	if notFoundSet {
		resp.Propstat = append(resp.Propstat, propstat{Prop: notFoundProp, Status: httpStatusNotFound})
	}
	return resp
}

func propstatForAddressObjectPropfind(req *propfindPropQuery, src prop) []propstat {
	if req == nil {
		return []propstat{{Prop: src, Status: httpStatusOK}}
	}
	var okProp prop
	var okSet bool
	var notFoundProp prop
	var notFoundSet bool

	if req.GetETag != nil {
		okProp.GetETag = src.GetETag
		okSet = true
	}
	if req.GetContentType != nil {
		okProp.GetContentType = src.GetContentType
		okSet = true
	}
	if req.SupportedReportSet != nil {
		okProp.SupportedReportSet = src.SupportedReportSet
		okSet = true
	}
	if req.AddressData != nil {
		okProp.AddressData = cdataString(filterVCardData(string(src.AddressData), req.AddressData))
		okSet = true
	}
	if req.LockDiscovery != nil {
		okProp.LockDiscovery = src.LockDiscovery
		okSet = true
	}
	if req.SupportedLock != nil {
		okProp.SupportedLock = src.SupportedLock
		okSet = true
	}
	if req.ACLProp != nil {
		okProp.ACL = src.ACL
		okSet = true
	}
	if req.SupportedPrivilegeSet != nil {
		okProp.SupportedPrivilegeSet = src.SupportedPrivilegeSet
		okSet = true
	}
	if req.PrincipalCollectionSet != nil {
		okProp.PrincipalCollectionSet = src.PrincipalCollectionSet
		okSet = true
	}
	if req.DisplayName != nil {
		notFoundProp.DisplayName = "displayname"
		notFoundSet = true
	}

	var stats []propstat
	if okSet {
		stats = append(stats, propstat{Prop: okProp, Status: httpStatusOK})
	}
	if notFoundSet {
		stats = append(stats, propstat{Prop: notFoundProp, Status: httpStatusNotFound})
	}
	if len(stats) == 0 {
		stats = append(stats, propstat{Prop: prop{}, Status: httpStatusOK})
	}
	return stats
}

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
	resp.Propstat = propstatForAddressObjectPropfind(req.Prop, src)
	return resp
}

func filterPrincipalPropfindResponse(resp response, req *propfindRequest) response {
	if req == nil || req.Prop == nil {
		return resp
	}
	src := resp.Propstat[0].Prop
	var okProp prop
	var okSet bool
	var notFound prop
	var notFoundSet bool

	if req.Prop.DisplayName != nil {
		okProp.DisplayName = src.DisplayName
		okSet = true
	}
	if req.Prop.ResourceType != nil {
		okProp.ResourceType = src.ResourceType
		okSet = true
	}
	if req.Prop.CurrentUserPrincipal != nil {
		okProp.CurrentUserPrincipal = src.CurrentUserPrincipal
		okSet = true
	}
	if req.Prop.CurrentUserPrincipalURL != nil {
		okProp.CurrentUserPrincipalURL = src.CurrentUserPrincipalURL
		okSet = true
	}
	if req.Prop.PrincipalURL != nil {
		okProp.PrincipalURL = src.PrincipalURL
		okSet = true
	}
	if req.Prop.CalendarHomeSet != nil {
		okProp.CalendarHomeSet = src.CalendarHomeSet
		okSet = true
	}
	if req.Prop.AddressbookHomeSet != nil {
		okProp.AddressbookHomeSet = src.AddressbookHomeSet
		okSet = true
	}
	if req.Prop.SupportedReportSet != nil {
		okProp.SupportedReportSet = src.SupportedReportSet
		okSet = true
	}
	if req.Prop.LockDiscovery != nil {
		okProp.LockDiscovery = src.LockDiscovery
		okSet = true
	}
	if req.Prop.SupportedLock != nil {
		okProp.SupportedLock = src.SupportedLock
		okSet = true
	}
	if req.Prop.ACLProp != nil {
		okProp.ACL = src.ACL
		okSet = true
	}
	if req.Prop.SupportedPrivilegeSet != nil {
		okProp.SupportedPrivilegeSet = src.SupportedPrivilegeSet
		okSet = true
	}
	if req.Prop.PrincipalCollectionSet != nil {
		okProp.PrincipalCollectionSet = src.PrincipalCollectionSet
		okSet = true
	}
	if req.Prop.CurrentUserPrivilegeSet != nil {
		okProp.CurrentUserPrivilegeSet = src.CurrentUserPrivilegeSet
		okSet = true
	}
	if req.Prop.GetETag != nil {
		notFound.GetETag = "getetag"
		notFoundSet = true
	}
	if req.Prop.GetContentType != nil {
		notFound.GetContentType = "getcontenttype"
		notFoundSet = true
	}
	if req.Prop.CalendarData != nil {
		notFound.CalendarData = cdataString("calendar-data")
		notFoundSet = true
	}
	if req.Prop.AddressData != nil {
		notFound.AddressData = cdataString("address-data")
		notFoundSet = true
	}
	if req.Prop.CalendarDescription != nil {
		notFound.CalendarDescription = "calendar-description"
		notFoundSet = true
	}
	if req.Prop.CalendarTimezone != nil {
		notFound.CalendarTimezone = stringPtr("calendar-timezone")
		notFoundSet = true
	}
	if req.Prop.AddressBookDesc != nil {
		notFound.AddressBookDesc = "addressbook-description"
		notFoundSet = true
	}
	if req.Prop.SupportedAddressData != nil {
		notFound.SupportedAddressData = &supportedAddressData{}
		notFoundSet = true
	}
	if req.Prop.AddressBookMaxResourceSize != nil {
		notFound.AddressBookMaxResourceSize = "max-resource-size"
		notFoundSet = true
	}
	if req.Prop.SupportedCollationSet != nil {
		notFound.SupportedCollationSet = &supportedCollationSet{}
		notFoundSet = true
	}
	if req.Prop.SyncToken != nil {
		notFound.SyncToken = "sync-token"
		notFoundSet = true
	}
	if req.Prop.CTag != nil {
		notFound.CTag = "getctag"
		notFoundSet = true
	}
	if req.Prop.PrincipalAddress != nil {
		notFound.PrincipalAddress = &hrefProp{}
		notFoundSet = true
	}
	if req.Prop.SupportedCalendarComponentSet != nil {
		notFound.SupportedCalendarComponentSet = &supportedCalendarComponentSet{}
		notFoundSet = true
	}
	if req.Prop.MaxResourceSize != nil {
		notFound.MaxResourceSize = "max-resource-size"
		notFoundSet = true
	}
	if req.Prop.MinDateTime != nil {
		notFound.MinDateTime = "min-date-time"
		notFoundSet = true
	}
	if req.Prop.MaxDateTime != nil {
		notFound.MaxDateTime = "max-date-time"
		notFoundSet = true
	}
	if req.Prop.MaxInstances != nil {
		notFound.MaxInstances = "max-instances"
		notFoundSet = true
	}
	if req.Prop.MaxAttendeesPerInstance != nil {
		notFound.MaxAttendeesPerInstance = "max-attendees-per-instance"
		notFoundSet = true
	}
	if req.Prop.ScheduleCalendarTransp != nil {
		notFound.ScheduleCalendarTransp = &scheduleCalendarTransp{}
		notFoundSet = true
	}
	if req.Prop.SupportedCalendarData != nil {
		notFound.SupportedCalendarData = &supportedCalendarData{}
		notFoundSet = true
	}
	if req.Prop.CalendarServerReadOnly != nil {
		notFound.CalendarServerReadOnly = &struct{}{}
		notFoundSet = true
	}
	if req.Prop.Owner != nil {
		notFound.Owner = &hrefProp{}
		notFoundSet = true
	}

	resp.Propstat = nil
	if okSet {
		resp.Propstat = append(resp.Propstat, propstat{Prop: okProp, Status: httpStatusOK})
	}
	if notFoundSet {
		resp.Propstat = append(resp.Propstat, propstat{Prop: notFound, Status: httpStatusNotFound})
	}
	if len(resp.Propstat) == 0 {
		resp.Propstat = []propstat{{Prop: prop{}, Status: httpStatusOK}}
	}
	return resp
}

func filterAddressBookCollectionPropfindResponse(resp response, req *propfindRequest) response {
	if req == nil || req.Prop == nil {
		return resp
	}
	src := resp.Propstat[0].Prop
	var okProp prop
	var okSet bool
	var notFound prop
	var notFoundSet bool
	if req.Prop.DisplayName != nil {
		okProp.DisplayName = src.DisplayName
		okSet = true
	}
	if req.Prop.ResourceType != nil {
		okProp.ResourceType = src.ResourceType
		okSet = true
	}
	if req.Prop.AddressBookDesc != nil {
		okProp.AddressBookDesc = src.AddressBookDesc
		okSet = true
	}
	if req.Prop.SupportedAddressData != nil {
		okProp.SupportedAddressData = src.SupportedAddressData
		okSet = true
	}
	if req.Prop.AddressBookMaxResourceSize != nil {
		okProp.AddressBookMaxResourceSize = src.AddressBookMaxResourceSize
		okSet = true
	}
	if req.Prop.SupportedCollationSet != nil {
		okProp.SupportedCollationSet = src.SupportedCollationSet
		okSet = true
	}
	if req.Prop.SyncToken != nil {
		okProp.SyncToken = src.SyncToken
		okSet = true
	}
	if req.Prop.CTag != nil {
		okProp.CTag = src.CTag
		okSet = true
	}
	if req.Prop.CurrentUserPrincipal != nil {
		okProp.CurrentUserPrincipal = src.CurrentUserPrincipal
		okSet = true
	}
	if req.Prop.CurrentUserPrincipalURL != nil {
		okProp.CurrentUserPrincipalURL = src.CurrentUserPrincipalURL
		okSet = true
	}
	if req.Prop.AddressbookHomeSet != nil {
		okProp.AddressbookHomeSet = src.AddressbookHomeSet
		okSet = true
	}
	if req.Prop.SupportedReportSet != nil {
		okProp.SupportedReportSet = src.SupportedReportSet
		okSet = true
	}
	if req.Prop.LockDiscovery != nil {
		okProp.LockDiscovery = src.LockDiscovery
		okSet = true
	}
	if req.Prop.SupportedLock != nil {
		okProp.SupportedLock = src.SupportedLock
		okSet = true
	}
	if req.Prop.ACLProp != nil {
		okProp.ACL = src.ACL
		okSet = true
	}
	if req.Prop.SupportedPrivilegeSet != nil {
		okProp.SupportedPrivilegeSet = src.SupportedPrivilegeSet
		okSet = true
	}
	if req.Prop.PrincipalCollectionSet != nil {
		okProp.PrincipalCollectionSet = src.PrincipalCollectionSet
		okSet = true
	}
	if req.Prop.GetETag != nil {
		notFound.GetETag = "getetag"
		notFoundSet = true
	}
	if req.Prop.GetContentType != nil {
		notFound.GetContentType = "getcontenttype"
		notFoundSet = true
	}
	if req.Prop.CalendarData != nil {
		notFound.CalendarData = cdataString("calendar-data")
		notFoundSet = true
	}
	if req.Prop.AddressData != nil {
		notFound.AddressData = cdataString("address-data")
		notFoundSet = true
	}
	if req.Prop.CalendarDescription != nil {
		notFound.CalendarDescription = "calendar-description"
		notFoundSet = true
	}
	if req.Prop.CalendarTimezone != nil {
		notFound.CalendarTimezone = stringPtr("calendar-timezone")
		notFoundSet = true
	}
	if req.Prop.PrincipalURL != nil {
		notFound.PrincipalURL = &expandableHrefProp{}
		notFoundSet = true
	}
	if req.Prop.CalendarHomeSet != nil {
		notFound.CalendarHomeSet = &hrefListProp{}
		notFoundSet = true
	}
	if req.Prop.PrincipalAddress != nil {
		notFound.PrincipalAddress = &hrefProp{}
		notFoundSet = true
	}
	if req.Prop.SupportedCalendarComponentSet != nil {
		notFound.SupportedCalendarComponentSet = &supportedCalendarComponentSet{}
		notFoundSet = true
	}
	if req.Prop.MaxResourceSize != nil {
		notFound.MaxResourceSize = "max-resource-size"
		notFoundSet = true
	}
	if req.Prop.MinDateTime != nil {
		notFound.MinDateTime = "min-date-time"
		notFoundSet = true
	}
	if req.Prop.MaxDateTime != nil {
		notFound.MaxDateTime = "max-date-time"
		notFoundSet = true
	}
	if req.Prop.MaxInstances != nil {
		notFound.MaxInstances = "max-instances"
		notFoundSet = true
	}
	if req.Prop.MaxAttendeesPerInstance != nil {
		notFound.MaxAttendeesPerInstance = "max-attendees-per-instance"
		notFoundSet = true
	}
	if req.Prop.ScheduleCalendarTransp != nil {
		notFound.ScheduleCalendarTransp = &scheduleCalendarTransp{}
		notFoundSet = true
	}
	if req.Prop.SupportedCalendarData != nil {
		notFound.SupportedCalendarData = &supportedCalendarData{}
		notFoundSet = true
	}
	if req.Prop.CalendarServerReadOnly != nil {
		notFound.CalendarServerReadOnly = &struct{}{}
		notFoundSet = true
	}
	if req.Prop.Owner != nil {
		notFound.Owner = &hrefProp{}
		notFoundSet = true
	}
	resp.Propstat = nil
	if okSet {
		resp.Propstat = append(resp.Propstat, propstat{Prop: okProp, Status: httpStatusOK})
	}
	if notFoundSet {
		resp.Propstat = append(resp.Propstat, propstat{Prop: notFound, Status: httpStatusNotFound})
	}
	if len(resp.Propstat) == 0 {
		resp.Propstat = []propstat{{Prop: prop{}, Status: httpStatusOK}}
	}
	return resp
}

func stripAddressBookAllprop(responses []response) {
	for i := range responses {
		for j := range responses[i].Propstat {
			prop := &responses[i].Propstat[j].Prop
			if prop.ResourceType.AddressBook == nil {
				continue
			}
			prop.AddressBookDesc = ""
			prop.SupportedAddressData = nil
			prop.AddressBookMaxResourceSize = ""
			prop.SupportedCollationSet = nil
		}
	}
}

func stripPrincipalAllprop(responses []response) {
	for i := range responses {
		for j := range responses[i].Propstat {
			prop := &responses[i].Propstat[j].Prop
			if prop.ResourceType.Principal == nil {
				continue
			}
			prop.CalendarHomeSet = nil
			prop.AddressbookHomeSet = nil
		}
	}
}

func writeCardDAVPrecondition(w http.ResponseWriter, status int, condition string) {
	w.Header().Set("Content-Type", "application/xml; charset=utf-8")
	w.WriteHeader(status)
	fmt.Fprintf(w, `<?xml version="1.0" encoding="utf-8"?><D:error xmlns:D="DAV:" xmlns:C="urn:ietf:params:xml:ns:carddav"><C:%s/></D:error>`, condition)
}

// writeCardDAVUIDConflict writes a no-uid-conflict error response including the
// href of the conflicting resource per RFC 6352 §6.3.2.1.
func writeCardDAVUIDConflict(w http.ResponseWriter, conflictHref string) {
	w.Header().Set("Content-Type", "application/xml; charset=utf-8")
	w.WriteHeader(http.StatusConflict)
	var escaped strings.Builder
	if err := xml.EscapeText(&escaped, []byte(conflictHref)); err != nil {
		conflictHref = ""
	} else {
		conflictHref = escaped.String()
	}
	fmt.Fprintf(w, `<?xml version="1.0" encoding="utf-8"?><D:error xmlns:D="DAV:" xmlns:C="urn:ietf:params:xml:ns:carddav"><C:no-uid-conflict><D:href>%s</D:href></C:no-uid-conflict></D:error>`, conflictHref)
}
