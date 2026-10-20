package dav

import (
	"bytes"
	"encoding/xml"
	"math"
)

const (
	// defaultMaxRequestedProperties sits an order of magnitude above the few
	// dozen properties the most thorough client discovery asks for at once.
	defaultMaxRequestedProperties = 1000
	// minPropertyElementBytes is the length of the shortest element that can
	// name a property, "<a/>".
	minPropertyElementBytes = 4
)

func (h *DavServer) requestedPropertyLimit() int {
	if h != nil && h.cfg != nil && h.cfg.DAV.MaxRequestedProperties > 0 {
		return h.cfg.DAV.MaxRequestedProperties
	}
	return defaultMaxRequestedProperties
}

// requestedPropertyCounter counts the distinct property names a request body
// selects: the children of each DAV:prop (RFC 4918 §14.18) and each
// DAV:property of DAV:expand-property (RFC 3253 §3.8). Every selected name is
// answered in every DAV:response, so the count is bounded from the raw body,
// before a decoder materializes the list. A name repeated within the body
// counts once, because it is answered once. With the limit turned off it
// records nothing, since no count can exceed it.
type requestedPropertyCounter struct {
	limit     int
	propDepth int
	names     map[xml.Name]struct{}
}

// start records one start element at the given document depth and reports
// whether the body now names more distinct properties than the limit.
func (c *requestedPropertyCounter) start(element xml.StartElement, depth int) bool {
	if c.limit == math.MaxInt {
		return false
	}
	var name xml.Name
	switch {
	case c.propDepth > 0:
		if depth != c.propDepth+1 {
			return false
		}
		name = element.Name
	case element.Name == davQName("prop"):
		c.propDepth = depth
		return false
	case element.Name == davQName("property"):
		local, ok := attributeValue(element, "name")
		if !ok {
			return false
		}
		namespace, ok := attributeValue(element, "namespace")
		if !ok {
			namespace = namespaceDAV
		}
		name = xml.Name{Space: namespace, Local: local}
	default:
		return false
	}
	if c.names == nil {
		c.names = make(map[xml.Name]struct{})
	}
	c.names[name] = struct{}{}
	return len(c.names) > c.limit
}

// end leaves the DAV:prop scope when the element at its depth closes.
func (c *requestedPropertyCounter) end(depth int) {
	if depth == c.propDepth {
		c.propDepth = 0
	}
}

// propfindBodyExceedsPropertyLimit reports whether a PROPFIND body names more
// distinct properties than this server answers per resource. It reads the raw
// body for the reason checkReportBodyLimits does: the struct decode would
// otherwise pay for every name before the limit applied. A body it cannot
// tokenize is left to that decode, whose report of a malformed body is the
// more specific one.
func (h *DavServer) propfindBodyExceedsPropertyLimit(body []byte) bool {
	limit := h.requestedPropertyLimit()
	if limit == math.MaxInt {
		return false
	}
	// A body too short to hold limit+1 property elements is within the limit
	// whatever it names, which spares the common small PROPFIND a second read.
	if limit < math.MaxInt/minPropertyElementBytes-1 && len(body) < minPropertyElementBytes*(limit+1) {
		return false
	}
	counter := requestedPropertyCounter{limit: limit}
	decoder := xml.NewDecoder(bytes.NewReader(body))
	decoder.Entity = xml.HTMLEntity
	depth := 0
	for {
		token, err := decoder.Token()
		if err != nil {
			return false
		}
		switch token := token.(type) {
		case xml.StartElement:
			depth++
			if counter.start(token, depth) {
				return true
			}
		case xml.EndElement:
			counter.end(depth)
			depth--
		}
	}
}
