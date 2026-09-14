package dav

import (
	"bytes"
	"context"
	"encoding/xml"
	"errors"
	"io"
	"net/http"
	"strings"

	"github.com/jw6ventures/calcard/internal/store"
)

// DAV:expand-property (RFC 3253 §3.8) reports the properties its DAV:property
// elements name. Nesting DAV:property children under an href-valued property
// replaces its DAV:href children with DAV:response elements carrying that
// nested selection; a property without nested selections keeps its own value.

// expandedHrefProperty is the expanded value of one href-valued property: the
// DAV:response elements standing in for its DAV:href children.
type expandedHrefProperty struct {
	Response []response `xml:"d:response"`
}

// expandPropertyExpander turns one property's hrefs into the responses that
// replace them. It reports expanded=false when the request names the property
// without nesting anything under it, which RFC 3253 §3.8 answers with the
// property's own value. It is nil on a request that is not an expand-property
// report, leaving the property filter behaving exactly as PROPFIND needs.
type expandPropertyExpander func(spec *propfindPropertySpec, hrefs []string) (responses []response, expanded bool, err error)

type expandXMLPropertyExpander func(property XMLProperty) (expanded XMLProperty, ok bool, err error)

type expandedXMLPropertyItem struct {
	token    xml.Token
	response *response
}

type expandedXMLPropertyValue struct {
	items []expandedXMLPropertyItem
}

func (v expandedXMLPropertyValue) MarshalXML(enc *xml.Encoder, start xml.StartElement) error {
	if err := enc.EncodeToken(start); err != nil {
		return err
	}
	for _, item := range v.items {
		if item.response != nil {
			if err := enc.EncodeElement(*item.response, xml.StartElement{Name: xml.Name{Local: "d:response"}}); err != nil {
				return err
			}
			continue
		}
		if err := enc.EncodeToken(item.token); err != nil {
			return err
		}
	}
	return enc.EncodeToken(start.End())
}

type expandPropertyBudget struct {
	remaining int
}

func (b *expandPropertyBudget) reserve(count int) error {
	if b == nil || count < 0 || count > b.remaining {
		return errMultistatusTooLarge
	}
	b.remaining -= count
	return nil
}

// expandProperty applies the request's expander to one property. It sits on
// propfindRequest so one property-filter path serves PROPFIND and
// expand-property alike, rather than either growing a second copy of the
// present, absent and forbidden rules the table encodes.
func (r *propfindRequest) expandProperty(spec *propfindPropertySpec, src *prop) (XMLProperty, bool) {
	if r == nil || r.expand == nil {
		return XMLProperty{}, false
	}
	if spec.expandValue != nil {
		expanded, ok := r.expandCustomXMLProperty(XMLProperty{Name: spec.qname(), Value: spec.expandValue(src)})
		if !ok {
			return XMLProperty{}, false
		}
		expanded.Name = spec.emptyName
		return expanded, true
	}
	if spec.hrefs == nil {
		return XMLProperty{}, false
	}
	responses, expanded, err := r.expand(spec, spec.hrefs(src))
	if err != nil && r.expandErr == nil {
		r.expandErr = err
	}
	if !expanded {
		return XMLProperty{}, false
	}
	return XMLProperty{Name: spec.emptyName, Value: expandedHrefProperty{Response: responses}}, true
}

func (r *propfindRequest) expandCustomXMLProperty(property XMLProperty) (XMLProperty, bool) {
	if r == nil || r.expandXML == nil {
		return XMLProperty{}, false
	}
	expanded, ok, err := r.expandXML(property)
	if err != nil && r.expandErr == nil {
		r.expandErr = err
	}
	return expanded, ok
}

// expandPropertyMaxDepth bounds how many referenced-resource edges one report
// may traverse. The request parser rejects a body that would exceed it.
const expandPropertyMaxDepth = 5

func parseExpandPropertyRequest(body []byte) (*expandPropertyRequest, error) {
	decoder, start, fault := openReportBody(body, davQName("expand-property"))
	if fault != nil {
		return nil, fault
	}
	if fault := checkAttributes(start, "DAV:expand-property"); fault != nil {
		return nil, fault
	}

	request := &expandPropertyRequest{XMLName: start.Name}
	if fault := walkElementChildren(decoder, start, "DAV:expand-property", malformedReport, func(dec *xml.Decoder, child xml.StartElement) *reportGrammarFault {
		if child.Name != davQName("property") {
			return malformedReport("DAV:expand-property carries unexpected %s", xmlNameString(child.Name))
		}
		property, fault := decodeExpandPropertyElement(dec, child, 1)
		if fault != nil {
			return fault
		}
		request.Property = append(request.Property, property)
		return nil
	}); fault != nil {
		return nil, fault
	}
	if err := expectXMLDocumentEnd(decoder); err != nil {
		return nil, err
	}
	return request, nil
}

func decodeExpandPropertyElement(decoder *xml.Decoder, start xml.StartElement, level int) (expandPropertyElement, *reportGrammarFault) {
	if level > expandPropertyMaxDepth+1 {
		return expandPropertyElement{}, malformedReport("DAV:expand-property nesting exceeds the supported depth")
	}
	if fault := checkAttributes(start, "DAV:property", "name", "namespace"); fault != nil {
		return expandPropertyElement{}, fault
	}
	name, present := attributeValue(start, "name")
	if !present || !validXMLNameToken(name) {
		return expandPropertyElement{}, malformedReport("DAV:property carries no valid name attribute")
	}
	namespace, present := attributeValue(start, "namespace")
	if !present {
		namespace = namespaceDAV
	}

	property := expandPropertyElement{Name: name, Namespace: namespace}
	if fault := walkElementChildren(decoder, start, "DAV:property", malformedReport, func(dec *xml.Decoder, child xml.StartElement) *reportGrammarFault {
		if child.Name != davQName("property") {
			return malformedReport("DAV:property carries unexpected %s", xmlNameString(child.Name))
		}
		nested, fault := decodeExpandPropertyElement(dec, child, level+1)
		if fault != nil {
			return fault
		}
		property.Property = append(property.Property, nested)
		return nil
	}); fault != nil {
		return expandPropertyElement{}, fault
	}
	return property, nil
}

// reportExpandProperty answers a DAV:expand-property REPORT against any
// resource the PROPFIND builders can describe, which is every resource CalCard
// exposes. Routing the target through those builders is what makes the reported
// property set the requested one on calendar collections and calendar object
// resources as well as on the DAV root.
func (h *DavServer) reportExpandProperty(w http.ResponseWriter, r *http.Request, user *store.User, targetPath string, req *expandPropertyRequest) {
	maxResponses, _ := h.multistatusLimits()
	if maxResponses <= 0 {
		maxResponses = defaultMaxMultistatusResponses
	}
	budget := &expandPropertyBudget{remaining: maxResponses}
	if err := budget.reserve(1); err != nil {
		http.Error(w, http.StatusText(http.StatusInsufficientStorage), http.StatusInsufficientStorage)
		return
	}
	responses, err := h.expandPropertyResponses(r.Context(), r, user, targetPath, expandPropertyList(req), expandPropertyMaxDepth, budget)
	if err != nil {
		// The target is resolved by the PROPFIND builders, so a failure to
		// resolve it means here what it means there.
		if errors.Is(err, errForbidden) || isPrivilegeNotGranted(err) {
			writeNeedPrivileges(w, targetPath, "read")
			return
		}
		status := http.StatusBadRequest
		switch {
		case errors.Is(err, errMultistatusTooLarge):
			status = http.StatusInsufficientStorage
		case errors.Is(err, errAmbiguousCalendar) || errors.Is(err, errAmbiguousAddressBook):
			status = http.StatusConflict
		case errors.Is(err, store.ErrNotFound) || errors.Is(err, http.ErrNotSupported):
			status = http.StatusNotFound
		}
		h.logger().Error("Report", "expand-property failed for %s (status %d): %v", targetPath, status, err)
		http.Error(w, http.StatusText(status), status)
		return
	}
	h.writeReportMultiStatus(w, r, "expand-property", responses, "")
}

// expandPropertyResponses builds the DAV:response for one resource with each
// named href-valued property expanded. depth bounds the recursion the request's
// own nesting drives.
func (h *DavServer) expandPropertyResponses(ctx context.Context, r *http.Request, user *store.User, targetPath string, properties []expandPropertyElement, depth int, budget *expandPropertyBudget) ([]response, error) {
	request := &propfindRequest{Prop: propfindQueryFromExpandProperties(properties)}
	if request.Prop == nil {
		// RFC 3253 §3.8 names the reported properties in the body; a body
		// naming none reports the resource with no properties at all.
		request.Prop = &propfindPropQuery{}
	}
	if depth > 0 {
		referenceBase := davHrefReferenceBase(targetPath)
		request.expand = func(spec *propfindPropertySpec, hrefs []string) ([]response, bool, error) {
			nested, ok := nestedExpandProperties(properties, spec.qname())
			if !ok {
				return nil, false, nil
			}
			responses, err := h.expandReferencedResources(ctx, r, user, referenceBase, hrefs, nested, depth-1, budget)
			return responses, true, err
		}
		request.expandXML = func(property XMLProperty) (XMLProperty, bool, error) {
			nested, ok := nestedExpandProperties(properties, property.Name)
			if !ok {
				return XMLProperty{}, false, nil
			}
			value, expanded, err := h.expandXMLPropertyValue(ctx, r, user, referenceBase, property, nested, depth-1, budget)
			if err != nil || !expanded {
				return XMLProperty{}, expanded, err
			}
			return XMLProperty{Name: property.Name, Value: value}, true, nil
		}
	}
	responses, err := h.buildPropfindResponses(ctx, r, targetPath, "0", user, request)
	if err != nil {
		return nil, err
	}
	if request.expandErr != nil {
		return nil, request.expandErr
	}
	return responses, nil
}

// expandReferencedResources reads the resources one property's hrefs name.
// Unreadable references become 404 responses so every href is replaced without
// revealing whether the resource is absent or merely inaccessible.
func (h *DavServer) expandReferencedResources(ctx context.Context, r *http.Request, user *store.User, sourcePath string, hrefs []string, nested []expandPropertyElement, depth int, budget *expandPropertyBudget) ([]response, error) {
	var expanded []response
	for _, href := range hrefs {
		if err := budget.reserve(1); err != nil {
			return nil, err
		}
		resolved, ok := h.resolveDAVHrefReference(href, sourcePath, r)
		if !ok {
			expanded = append(expanded, response{Href: strings.TrimSpace(href), Status: httpStatusNotFound})
			continue
		}
		referenced, err := h.expandPropertyResponses(ctx, r, user, normalizeDAVHref(resolved.Path), nested, depth, budget)
		if err != nil {
			if errors.Is(err, store.ErrNotFound) || errors.Is(err, errForbidden) ||
				isPrivilegeNotGranted(err) || errors.Is(err, http.ErrNotSupported) {
				expanded = append(expanded, response{Href: strings.TrimSpace(href), Status: httpStatusNotFound})
				continue
			}
			return nil, err
		}
		if len(referenced) > 1 {
			if err := budget.reserve(len(referenced) - 1); err != nil {
				return nil, err
			}
		}
		expanded = append(expanded, referenced...)
	}
	return expanded, nil
}

func (h *DavServer) expandXMLPropertyValue(ctx context.Context, r *http.Request, user *store.User, sourcePath string, property XMLProperty, nested []expandPropertyElement, depth int, budget *expandPropertyBudget) (expandedXMLPropertyValue, bool, error) {
	encoded, err := marshalXMLPropertyForExpansion(property)
	if err != nil {
		return expandedXMLPropertyValue{}, false, err
	}
	decoder := xml.NewDecoder(bytes.NewReader(encoded))
	var root xml.StartElement
	for {
		token, err := decoder.Token()
		if err != nil {
			return expandedXMLPropertyValue{}, false, err
		}
		if start, ok := token.(xml.StartElement); ok {
			root = start
			break
		}
	}

	var value expandedXMLPropertyValue
	expanded := false
	for {
		token, err := decoder.Token()
		if err != nil {
			if errors.Is(err, io.EOF) {
				return expandedXMLPropertyValue{}, false, io.ErrUnexpectedEOF
			}
			return expandedXMLPropertyValue{}, false, err
		}
		switch token := token.(type) {
		case xml.StartElement:
			if token.Name == davQName("href") {
				var href string
				if err := decoder.DecodeElement(&href, &token); err != nil {
					return expandedXMLPropertyValue{}, false, err
				}
				responses, err := h.expandReferencedResources(ctx, r, user, sourcePath, []string{href}, nested, depth, budget)
				if err != nil {
					return expandedXMLPropertyValue{}, false, err
				}
				for i := range responses {
					response := responses[i]
					value.items = append(value.items, expandedXMLPropertyItem{response: &response})
				}
				expanded = true
				continue
			}
			value.items = append(value.items, expandedXMLPropertyItem{token: cleanExpandedXMLToken(token)})
		case xml.EndElement:
			if token.Name == root.Name {
				return value, expanded, nil
			}
			value.items = append(value.items, expandedXMLPropertyItem{token: xml.CopyToken(token)})
		default:
			value.items = append(value.items, expandedXMLPropertyItem{token: xml.CopyToken(token)})
		}
	}
}

func marshalXMLPropertyForExpansion(property XMLProperty) ([]byte, error) {
	var body bytes.Buffer
	encoder := xml.NewEncoder(&body)
	start := xml.StartElement{
		Name: property.Name,
		Attr: []xml.Attr{{Name: xml.Name{Local: "xmlns:d"}, Value: namespaceDAV}},
	}
	if property.Value == nil {
		if err := encoder.EncodeToken(start); err != nil {
			return nil, err
		}
		if err := encoder.EncodeToken(start.End()); err != nil {
			return nil, err
		}
	} else if err := encoder.EncodeElement(property.Value, start); err != nil {
		return nil, err
	}
	if err := encoder.Flush(); err != nil {
		return nil, err
	}
	return body.Bytes(), nil
}

func cleanExpandedXMLToken(start xml.StartElement) xml.Token {
	clean := xml.StartElement{Name: start.Name}
	for _, attr := range start.Attr {
		if attr.Name.Local == "xmlns" || attr.Name.Space == "xmlns" {
			continue
		}
		clean.Attr = append(clean.Attr, attr)
	}
	return clean
}

func expandPropertyList(req *expandPropertyRequest) []expandPropertyElement {
	if req == nil {
		return nil
	}
	return req.Property
}

// nestedExpandProperties returns the DAV:property children the request nests
// under one property, which select the properties its substituted responses
// carry. ok is false when the request names the property without nesting
// anything: RFC 3253 §3.8 makes nesting the request to expand, so a bare name
// asks for the property's own value.
func nestedExpandProperties(properties []expandPropertyElement, name xml.Name) ([]expandPropertyElement, bool) {
	for _, property := range properties {
		if property.Namespace != name.Space || property.Name != name.Local {
			continue
		}
		if len(property.Property) == 0 {
			return nil, false
		}
		return property.Property, true
	}
	return nil, false
}

// propfindQueryFromExpandProperties builds the property selection an
// expand-property body names. The names are re-emitted as a DAV:prop document
// and decoded through the same struct tags a PROPFIND body goes through, so the
// two methods cannot disagree about what a property name selects and a name the
// server does not define lands in CustomXML for its 404 exactly as it would
// under PROPFIND.
func propfindQueryFromExpandProperties(properties []expandPropertyElement) *propfindPropQuery {
	if len(properties) == 0 {
		return nil
	}
	var body bytes.Buffer
	encoder := xml.NewEncoder(&body)
	root := xml.StartElement{Name: xml.Name{Space: namespaceDAV, Local: "prop"}}
	if err := encoder.EncodeToken(root); err != nil {
		return nil
	}
	for _, property := range properties {
		name := xml.Name{Space: property.Namespace, Local: property.Name}
		if name.Local == "" {
			continue
		}
		if name.Space == "" {
			name.Space = namespaceDAV
		}
		start := xml.StartElement{Name: name}
		if err := encoder.EncodeToken(start); err != nil {
			return nil
		}
		if err := encoder.EncodeToken(start.End()); err != nil {
			return nil
		}
	}
	if err := encoder.EncodeToken(root.End()); err != nil {
		return nil
	}
	if err := encoder.Flush(); err != nil {
		return nil
	}
	var query propfindPropQuery
	if err := safeUnmarshalXML(body.Bytes(), &query); err != nil {
		return nil
	}
	return &query
}
