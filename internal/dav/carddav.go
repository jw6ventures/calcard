package dav

import (
	"context"
	"encoding/xml"
	"fmt"
	"net/http"
	"strconv"
	"strings"

	"github.com/jw6ventures/calcard/internal/ical"
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
	lines := ical.UnfoldLines(raw)
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
	lines := ical.UnfoldLines(raw)
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

	lines := ical.UnfoldLines(raw)
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

func effectiveAddressDataRequest(req *reportProp, topLevelAddressData *addressDataQuery) *addressDataQuery {
	if req != nil && req.AddressData != nil {
		return req.AddressData
	}
	return topLevelAddressData
}

func (h *DavServer) buildAddressObjectReportResponse(ctx context.Context, user *store.User, href string, contact store.Contact, req *reportProp, topLevelAddressData *addressDataQuery) (response, error) {
	addressDataReq := effectiveAddressDataRequest(req, topLevelAddressData)
	if addressDataReq != nil && !canServeRequestedAddressData(contact.RawVCard, addressDataReq) {
		return response{
			Href:   href,
			Status: httpStatusNotAcceptable,
			Error:  &responseError{SupportedAddressDataConversion: &struct{}{}},
		}, nil
	}

	rawData := contact.RawVCard
	if addressDataReq != nil {
		rawData = filterVCardData(rawData, addressDataReq)
	}
	propertyStatus := etagProp(contact.ETag, rawData, false)
	if req != nil && req.SupportedReportSet != nil {
		propertyStatus.Prop.SupportedReportSet = addressbookSupportedReports()
	}
	resp := resourceResponse(href, propertyStatus)
	return resp, nil
}

func buildAddressObjectExpandPropertyResponse(href string, contact store.Contact, req *expandPropertyRequest) response {
	resp := resourceResponse(href, addressBookResourcePropstat(contact.ETag, contact.RawVCard))
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

func stripAddressBookAllprop(responses []response) {
	for i := range responses {
		for j := range responses[i].Propstat {
			prop := &responses[i].Propstat[j].Prop
			prop.AddressData = ""
			if prop.ResourceType == nil || prop.ResourceType.AddressBook == nil {
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
			if prop.ResourceType == nil || prop.ResourceType.Principal == nil {
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
