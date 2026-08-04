package dav

import (
	"bytes"
	"context"
	"encoding/xml"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"path"
	"strconv"
	"strings"

	"github.com/jw6ventures/calcard/internal/store"
)

func isACLReport(name string) bool {
	switch name {
	case "acl-principal-prop-set", "principal-match", "principal-property-search", "principal-search-property-set":
		return true
	default:
		return false
	}
}

func davReportName(local string) xml.Name {
	return xml.Name{Space: "DAV:", Local: local}
}

type aclReportPropertyName struct {
	XMLName xml.Name
}

type aclPrincipalPropSetRequest struct {
	XMLName xml.Name    `xml:"DAV: acl-principal-prop-set"`
	Prop    *reportProp `xml:"DAV: prop"`
}

type principalPropertyElement struct {
	Properties []aclReportPropertyName `xml:",any"`
}

func (p *principalPropertyElement) UnmarshalXML(decoder *xml.Decoder, start xml.StartElement) error {
	for {
		token, err := decoder.Token()
		if err != nil {
			return err
		}
		switch token := token.(type) {
		case xml.StartElement:
			if err := decodeEmptyXMLElement(decoder, token); err != nil {
				return err
			}
			p.Properties = append(p.Properties, aclReportPropertyName{XMLName: token.Name})
		case xml.CharData:
			if strings.TrimSpace(string(token)) != "" {
				return fmt.Errorf("unexpected text in %s", start.Name.Local)
			}
		case xml.EndElement:
			if token.Name == start.Name {
				return nil
			}
		}
	}
}

func decodeEmptyXMLElement(decoder *xml.Decoder, start xml.StartElement) error {
	for {
		token, err := decoder.Token()
		if err != nil {
			return err
		}
		switch token := token.(type) {
		case xml.StartElement:
			return fmt.Errorf("nested element in %s", start.Name.Local)
		case xml.CharData:
			if strings.TrimSpace(string(token)) != "" {
				return fmt.Errorf("unexpected text in %s", start.Name.Local)
			}
		case xml.EndElement:
			if token.Name == start.Name {
				return nil
			}
		}
	}
}

type principalMatchRequest struct {
	XMLName           xml.Name                  `xml:"DAV: principal-match"`
	PrincipalProperty *principalPropertyElement `xml:"DAV: principal-property"`
	Self              *struct{}                 `xml:"DAV: self"`
	Prop              *reportProp               `xml:"DAV: prop"`
}

type principalPropertySearchElement struct {
	Prop  principalPropertyElement `xml:"DAV: prop"`
	Match string                   `xml:"DAV: match"`
}

func (s *principalPropertySearchElement) UnmarshalXML(decoder *xml.Decoder, start xml.StartElement) error {
	if start.Name != davReportName("property-search") {
		return fmt.Errorf("invalid property-search element")
	}
	var children []xml.Name
	for {
		token, err := decoder.Token()
		if err != nil {
			return err
		}
		switch token := token.(type) {
		case xml.StartElement:
			children = append(children, token.Name)
			switch len(children) {
			case 1:
				if token.Name != davReportName("prop") {
					return fmt.Errorf("property-search must begin with prop")
				}
				if err := decoder.DecodeElement(&s.Prop, &token); err != nil {
					return err
				}
			case 2:
				if token.Name != davReportName("match") {
					return fmt.Errorf("property-search must end with match")
				}
				match, err := decodeStrictXMLText(decoder, token)
				if err != nil {
					return err
				}
				s.Match = match
			default:
				return fmt.Errorf("too many property-search children")
			}
		case xml.CharData:
			if strings.TrimSpace(string(token)) != "" {
				return fmt.Errorf("unexpected property-search text")
			}
		case xml.EndElement:
			if token.Name == start.Name {
				if len(children) != 2 || len(s.Prop.Properties) == 0 || strings.TrimSpace(s.Match) == "" {
					return fmt.Errorf("invalid property-search content")
				}
				return nil
			}
		}
	}
}

func decodeStrictXMLText(decoder *xml.Decoder, start xml.StartElement) (string, error) {
	var value strings.Builder
	for {
		token, err := decoder.Token()
		if err != nil {
			return "", err
		}
		switch token := token.(type) {
		case xml.CharData:
			value.Write(token)
		case xml.StartElement:
			return "", fmt.Errorf("nested element in %s", start.Name.Local)
		case xml.EndElement:
			if token.Name == start.Name {
				return value.String(), nil
			}
		}
	}
}

type principalPropertySearchRequest struct {
	XMLName                       xml.Name                         `xml:"DAV: principal-property-search"`
	Searches                      []principalPropertySearchElement `xml:"DAV: property-search"`
	Prop                          *reportProp                      `xml:"DAV: prop"`
	ApplyToPrincipalCollectionSet *struct{}                        `xml:"DAV: apply-to-principal-collection-set"`
}

// reportACL dispatches the four RFC 3744 principal reports. All four are
// defined on a single resource rather than over a collection's members, so a
// Depth other than 0 is a malformed request rather than a deeper traversal.
func (h *DavServer) reportACL(w http.ResponseWriter, r *http.Request, user *store.User, cleanPath string, body []byte) {
	if depth := strings.TrimSpace(r.Header.Get("Depth")); depth != "" && depth != "0" {
		http.Error(w, "ACL reports require Depth: 0", http.StatusBadRequest)
		return
	}
	switch reportName, err := xmlRootName(body); {
	case err != nil:
		http.Error(w, "invalid ACL REPORT body", http.StatusBadRequest)
	case reportName == davReportName("acl-principal-prop-set"):
		h.reportACLPrincipalPropSet(w, r, user, cleanPath, body)
	case reportName == davReportName("principal-match"):
		h.reportPrincipalMatch(w, r, user, cleanPath, body)
	case reportName == davReportName("principal-property-search"):
		h.reportPrincipalPropertySearch(w, r, user, cleanPath, body)
	case reportName == davReportName("principal-search-property-set"):
		h.reportPrincipalSearchPropertySet(w, cleanPath, body)
	default:
		http.Error(w, "unsupported ACL REPORT", http.StatusBadRequest)
	}
}

func (h *DavServer) reportACLPrincipalPropSet(w http.ResponseWriter, r *http.Request, user *store.User, cleanPath string, body []byte) {
	children, err := directXMLChildNames(body)
	if err != nil || len(children) == 0 || countXMLName(children, davReportName("prop")) > 1 {
		http.Error(w, "invalid acl-principal-prop-set body", http.StatusBadRequest)
		return
	}
	var request aclPrincipalPropSetRequest
	if err := safeUnmarshalXML(body, &request); err != nil {
		http.Error(w, "invalid acl-principal-prop-set body", http.StatusBadRequest)
		return
	}
	if _, err := h.aclResourceExpectation(r.Context(), user, cleanPath); err != nil {
		if errors.Is(err, store.ErrNotFound) {
			http.Error(w, "not found", http.StatusNotFound)
		} else {
			http.Error(w, "failed to resolve resource", http.StatusInternalServerError)
		}
		return
	}
	canonicalPath, err := h.canonicalDAVPath(r.Context(), user, cleanPath)
	if err != nil {
		if errors.Is(err, store.ErrNotFound) {
			http.Error(w, "not found", http.StatusNotFound)
		} else {
			http.Error(w, "failed to resolve resource", http.StatusInternalServerError)
		}
		return
	}
	allowed, err := h.checkACLPrivilege(r.Context(), user, canonicalPath, "read-acl")
	if err != nil {
		http.Error(w, "failed to evaluate ACL", http.StatusInternalServerError)
		return
	}
	if !allowed {
		writeNeedPrivileges(w, canonicalPath, "read-acl")
		return
	}

	var principalHrefs []string
	owner, err := h.ownerPrincipalForPath(r.Context(), user, canonicalPath)
	if err != nil {
		http.Error(w, "failed to resolve resource owner", http.StatusInternalServerError)
		return
	}
	if owner != "" {
		principalHrefs = append(principalHrefs, owner)
	}
	if h != nil && h.store != nil && h.store.ACLEntries != nil {
		entries, err := h.aclEntriesForResource(r.Context(), canonicalPath)
		if err != nil {
			http.Error(w, "failed to load ACL", http.StatusInternalServerError)
			return
		}
		for _, entry := range entries {
			if isHTTPPrincipalHref(entry.PrincipalHref) {
				principalHrefs = append(principalHrefs, entry.PrincipalHref)
			}
		}
		if inheritedPath := inheritedACLSourcePath(canonicalPath); inheritedPath != "" {
			inherited, err := h.aclEntriesForResource(r.Context(), inheritedPath)
			if err != nil {
				http.Error(w, "failed to load inherited ACL", http.StatusInternalServerError)
				return
			}
			for _, entry := range inherited {
				if isHTTPPrincipalHref(entry.PrincipalHref) {
					principalHrefs = append(principalHrefs, entry.PrincipalHref)
				}
			}
		}
	}
	principalHrefs = uniqueNormalizedHrefs(principalHrefs)
	responses := make([]response, 0, len(principalHrefs))
	for _, href := range principalHrefs {
		allowed, err := h.checkACLPrivilege(r.Context(), user, href, "read")
		if err != nil {
			http.Error(w, "failed to evaluate principal access", http.StatusInternalServerError)
			return
		}
		if !allowed {
			continue
		}
		if request.Prop == nil {
			responses = append(responses, response{Href: ensureCollectionHref(href), Status: httpStatusOK})
			continue
		}
		principalResponse, err := h.aclPrincipalResponse(r.Context(), user, href)
		if err != nil {
			http.Error(w, "failed to load principal", http.StatusInternalServerError)
			return
		}
		responses = append(responses, principalResponse)
	}
	responses, err = h.selectACLReportProperties(r, user, responses, request.Prop)
	if err != nil {
		http.Error(w, "failed to build ACL principal response", http.StatusInternalServerError)
		return
	}
	h.writeBoundedMultiStatus(w, newMultistatus(responses, ""))
}

func (h *DavServer) aclPrincipalResponse(ctx context.Context, current *store.User, href string) (response, error) {
	id, local := localPrincipalID(href)
	if !local {
		return response{Href: href, Status: httpStatusNotFound}, nil
	}
	var principal *store.User
	if current != nil && current.ID == id {
		copy := *current
		principal = &copy
	} else if h != nil && h.store != nil && h.store.Users != nil {
		var err error
		principal, err = h.store.Users.GetByID(ctx, id)
		if err != nil {
			return response{}, err
		}
	}
	if principal == nil {
		return response{Href: ensureCollectionHref(href), Status: httpStatusNotFound}, nil
	}
	return principalResponse(ensureCollectionHref(href), principal), nil
}

func (h *DavServer) reportPrincipalMatch(w http.ResponseWriter, r *http.Request, user *store.User, cleanPath string, body []byte) {
	children, err := directXMLChildNames(body)
	if err != nil || !validPrincipalMatchChildren(children) {
		http.Error(w, "invalid principal-match body", http.StatusBadRequest)
		return
	}
	var request principalMatchRequest
	if err := safeUnmarshalXML(body, &request); err != nil || (request.Self == nil) == (request.PrincipalProperty == nil) {
		http.Error(w, "invalid principal-match body", http.StatusBadRequest)
		return
	}
	if request.PrincipalProperty != nil && len(request.PrincipalProperty.Properties) != 1 {
		http.Error(w, "principal-property must name exactly one property", http.StatusBadRequest)
		return
	}
	if !h.isDAVCollection(r.Context(), user, cleanPath) {
		http.Error(w, "principal-match target must be a collection", http.StatusBadRequest)
		return
	}
	candidates, err := h.buildPropfindResponses(r.Context(), nil, cleanPath, "infinity", user, nil)
	if err != nil {
		if errors.Is(err, store.ErrNotFound) || errors.Is(err, errForbidden) {
			http.Error(w, "not found", http.StatusNotFound)
		} else {
			http.Error(w, "failed to enumerate collection", http.StatusInternalServerError)
		}
		return
	}
	requestHref := ensureCollectionHref(normalizeDAVHref(cleanPath))
	currentPrincipal := ensureCollectionHref(h.principalURL(user))
	responses := make([]response, 0, len(candidates))
	for _, candidate := range candidates {
		if ensureCollectionHref(normalizeDAVHref(candidate.Href)) == requestHref {
			continue
		}
		matched := false
		if request.Self != nil {
			matched = propfindKindOf(candidate) == kindPrincipal && ensureCollectionHref(normalizeDAVHref(candidate.Href)) == currentPrincipal
		} else {
			principalHref, err := h.principalPropertyHref(r.Context(), user, candidate, request.PrincipalProperty.Properties[0].XMLName)
			if err != nil {
				http.Error(w, "failed to resolve principal property", http.StatusInternalServerError)
				return
			}
			matched = ensureCollectionHref(normalizeDAVHref(principalHref)) == currentPrincipal
		}
		if !matched {
			continue
		}
		if request.Prop == nil {
			responses = append(responses, response{Href: candidate.Href, Status: httpStatusOK})
		} else {
			responses = append(responses, candidate)
		}
	}
	responses, err = h.selectACLReportProperties(r, user, responses, request.Prop)
	if err != nil {
		http.Error(w, "failed to build principal-match response", http.StatusInternalServerError)
		return
	}
	h.writeBoundedMultiStatus(w, newMultistatus(responses, ""))
}

func (h *DavServer) principalPropertyHref(ctx context.Context, user *store.User, candidate response, property xml.Name) (string, error) {
	switch property {
	case davReportName("owner"):
		return h.ownerPrincipalForPath(ctx, user, candidate.Href)
	case davReportName("principal-URL"):
		if propfindKindOf(candidate) == kindPrincipal {
			return candidate.Href, nil
		}
	}
	return "", nil
}

func (h *DavServer) reportPrincipalPropertySearch(w http.ResponseWriter, r *http.Request, user *store.User, cleanPath string, body []byte) {
	children, err := directXMLChildNames(body)
	if err != nil || !validPrincipalPropertySearchChildren(children) {
		http.Error(w, "invalid principal-property-search body", http.StatusBadRequest)
		return
	}
	var request principalPropertySearchRequest
	if err := safeUnmarshalXML(body, &request); err != nil || len(request.Searches) == 0 {
		http.Error(w, "invalid principal-property-search body", http.StatusBadRequest)
		return
	}
	// RFC 3744 §9.4 runs the search over the members of the collection the
	// report is applied to, and only DAV:apply-to-principal-collection-set
	// redirects it to the principal collections. CalCard exposes exactly one
	// principal collection, so any other Request-URI has no member set to
	// search.
	if request.ApplyToPrincipalCollectionSet == nil && normalizeDAVHref(cleanPath) != "/dav/principals" {
		http.Error(w, "principal-property-search target must be a principal collection", http.StatusBadRequest)
		return
	}
	if request.ApplyToPrincipalCollectionSet != nil {
		responses, err := h.buildPropfindResponses(r.Context(), r, cleanPath, "0", user, nil)
		if err != nil || len(responses) == 0 {
			if errors.Is(err, store.ErrNotFound) || errors.Is(err, errForbidden) || len(responses) == 0 {
				http.Error(w, "not found", http.StatusNotFound)
			} else {
				http.Error(w, "failed to resolve principal collection set", http.StatusInternalServerError)
			}
			return
		}
	}
	if h == nil || h.store == nil || h.store.Users == nil {
		http.Error(w, "principal repository unavailable", http.StatusInternalServerError)
		return
	}
	principals, err := h.store.Users.ListActive(r.Context())
	if err != nil {
		http.Error(w, "failed to search principals", http.StatusInternalServerError)
		return
	}
	responses := make([]response, 0, len(principals))
	for i := range principals {
		principal := &principals[i]
		allowed, err := h.checkACLPrivilege(r.Context(), user, h.principalURL(principal), "read")
		if err != nil {
			http.Error(w, "failed to evaluate principal access", http.StatusInternalServerError)
			return
		}
		if !allowed {
			continue
		}
		if !principalMatchesSearches(principal, request.Searches) {
			continue
		}
		if request.Prop == nil {
			responses = append(responses, response{Href: h.principalURL(principal), Status: httpStatusOK})
		} else {
			responses = append(responses, principalResponse(h.principalURL(principal), principal))
		}
	}
	responses, err = h.selectACLReportProperties(r, user, responses, request.Prop)
	if err != nil {
		http.Error(w, "failed to build principal search response", http.StatusInternalServerError)
		return
	}
	h.writeBoundedMultiStatus(w, newMultistatus(responses, ""))
}

// principalMatchesSearches applies the RFC 3744 §9.4 conjunction: a principal
// matches only if every DAV:property-search matches it. DAV:displayname is the
// one searchable property CalCard advertises through
// principal-search-property-set, and §9.4 leaves a search over any other
// property matching nothing rather than erroring.
func principalMatchesSearches(principal *store.User, searches []principalPropertySearchElement) bool {
	for _, search := range searches {
		if len(search.Prop.Properties) == 0 {
			return false
		}
		needle := strings.ToLower(search.Match)
		for _, property := range search.Prop.Properties {
			if property.XMLName != davReportName("displayname") || !strings.Contains(strings.ToLower(principalDisplayName(principal)), needle) {
				return false
			}
		}
	}
	return true
}

// reportPrincipalSearchPropertySet answers with the properties
// principal-property-search will actually search, which RFC 3744 §9.5 requires
// to be exactly the set that report honours. The request body is defined as an
// empty element, so any content makes it malformed.
func (h *DavServer) reportPrincipalSearchPropertySet(w http.ResponseWriter, cleanPath string, body []byte) {
	children, err := directXMLChildNames(body)
	if err != nil || len(children) != 0 || normalizeDAVHref(cleanPath) != "/dav/principals" {
		http.Error(w, "invalid principal-search-property-set request", http.StatusBadRequest)
		return
	}
	w.Header().Set("Content-Type", "application/xml; charset=utf-8")
	w.WriteHeader(http.StatusOK)
	_, _ = fmt.Fprint(w, `<?xml version="1.0" encoding="utf-8"?><D:principal-search-property-set xmlns:D="DAV:"><D:principal-search-property><D:prop><D:displayname/></D:prop><D:description xml:lang="en">Display name</D:description></D:principal-search-property></D:principal-search-property-set>`)
}

func (h *DavServer) selectACLReportProperties(r *http.Request, user *store.User, responses []response, requested *reportProp) ([]response, error) {
	if requested == nil {
		return responses, nil
	}
	propfindRequest := &propfindRequest{Prop: &propfindPropQuery{propertySelection: requested.propertySelection}}
	if err := h.decoratePropfindResponses(r.Context(), r, user, responses, decorationMaskFor(propfindRequest)); err != nil {
		return nil, err
	}
	for i := range responses {
		if len(responses[i].Propstat) != 0 {
			responses[i] = filterNonPrincipalPropfindResponse(responses[i], propfindRequest)
		}
	}
	return responses, nil
}

func (h *DavServer) isDAVCollection(ctx context.Context, user *store.User, cleanPath string) bool {
	cleanPath = normalizeDAVHref(cleanPath)
	if cleanPath == "/dav" || cleanPath == "/dav/calendars" || cleanPath == "/dav/addressbooks" || cleanPath == "/dav/principals" {
		return true
	}
	target := parsedDAVTarget(ctx, cleanPath)
	if target.Valid && !target.Resource && target.CollectionSegment != "" {
		return true
	}
	_, ok := h.davRegistry().registeredExtensionCollection(cleanPath)
	return ok
}

func xmlRootName(body []byte) (xml.Name, error) {
	decoder := xml.NewDecoder(bytes.NewReader(body))
	for {
		token, err := decoder.Token()
		if err != nil {
			return xml.Name{}, err
		}
		if start, ok := token.(xml.StartElement); ok {
			return start.Name, nil
		}
	}
}

func directXMLChildNames(body []byte) ([]xml.Name, error) {
	decoder := xml.NewDecoder(bytes.NewReader(body))
	depth := 0
	var names []xml.Name
	for {
		token, err := decoder.Token()
		if err != nil {
			if errors.Is(err, io.EOF) {
				return names, nil
			}
			return nil, err
		}
		switch token := token.(type) {
		case xml.StartElement:
			if depth == 1 {
				names = append(names, token.Name)
			}
			depth++
		case xml.EndElement:
			depth--
		}
	}
}

func countXMLName(names []xml.Name, want xml.Name) int {
	count := 0
	for _, name := range names {
		if name == want {
			count++
		}
	}
	return count
}

func validPrincipalMatchChildren(children []xml.Name) bool {
	if len(children) < 1 || len(children) > 2 || children[0] != davReportName("self") && children[0] != davReportName("principal-property") {
		return false
	}
	return len(children) == 1 || children[1] == davReportName("prop")
}

func validPrincipalPropertySearchChildren(children []xml.Name) bool {
	index := 0
	for index < len(children) && children[index] == davReportName("property-search") {
		index++
	}
	if index == 0 {
		return false
	}
	if index < len(children) && children[index] == davReportName("prop") {
		index++
	}
	if index < len(children) && children[index] == davReportName("apply-to-principal-collection-set") {
		index++
	}
	return index == len(children)
}

func isHTTPPrincipalHref(href string) bool {
	normalized := strings.TrimSpace(href)
	if strings.HasPrefix(normalized, "/") {
		return true
	}
	parsed, err := url.Parse(normalized)
	return err == nil && (parsed.Scheme == "http" || parsed.Scheme == "https") && parsed.Host != ""
}

func uniqueNormalizedHrefs(hrefs []string) []string {
	seen := make(map[string]struct{}, len(hrefs))
	result := make([]string, 0, len(hrefs))
	for _, href := range hrefs {
		normalized := ensureCollectionHref(normalizeDAVHref(href))
		if _, exists := seen[normalized]; exists {
			continue
		}
		seen[normalized] = struct{}{}
		result = append(result, href)
	}
	return result
}

func localPrincipalID(href string) (int64, bool) {
	parsed, err := url.Parse(strings.TrimSpace(href))
	if err != nil || parsed.RawQuery != "" || parsed.Fragment != "" {
		return 0, false
	}
	principalPath := path.Clean(parsed.Path)
	segments := strings.Split(strings.Trim(strings.TrimPrefix(principalPath, "/dav/principals"), "/"), "/")
	if !strings.HasPrefix(principalPath, "/dav/principals/") || len(segments) != 1 {
		return 0, false
	}
	id, err := strconv.ParseInt(segments[0], 10, 64)
	return id, err == nil && id > 0
}
