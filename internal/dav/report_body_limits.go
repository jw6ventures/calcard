package dav

import (
	"bytes"
	"encoding/xml"
	"errors"
	"fmt"
	"io"
)

const (
	defaultMaxFilterElements        = 100
	defaultMaxReportElementDepth    = 20
	defaultMaxCardDAVQueryBytes     = 65536
	defaultMaxAddressDataProperties = 100
)

// maxRecursiveGrammarDepth is the ceiling the recursive decoders hold
// themselves to. checkReportBodyLimits normally refuses a deep body before they
// run, but an operator can set that budget to unlimited, and unbounded
// recursion would then grow the goroutine stack until the runtime kills the
// process -- a failure mode no configuration value should be able to produce.
// It sits far above any nesting RFC 4791 defines, so it never decides the
// answer to a body the configured limit would have accepted.
const maxRecursiveGrammarDepth = 1000

// checkReportBodyLimits refuses a REPORT body whose element nesting or filter
// size is past what this server will evaluate. RFC 4791 §11 asks a server to
// take adequate precautions against a report crafted to spend its CPU and
// memory: both recursive REPORT grammars -- CALDAV:comp-filter and the
// CALDAV:comp of CALDAV:calendar-data -- descend once per nested element, and
// each filter element, CalDAV or CardDAV, is evaluated against every candidate
// resource.
//
// It measures the body once, iteratively, ahead of every decode, because the
// permissive struct decode recurses through the same body the grammar pass
// later re-reads: a bound applied only in the grammar pass would be applied
// after the cost had already been paid.
//
// Depth is measured over the whole document, since every recursive decoder
// descends with it. The element count is scoped to the two filter grammars,
// where a legitimate body carries a handful of elements and each one multiplies
// the collection scan. CardDAV filter and address-data subtrees also share a
// byte budget, and address-data selectors have a count limit: their names and
// text are work repeated per contact even when the filter itself is small.
func (h *DavServer) checkReportBodyLimits(body []byte) *reportGrammarFault {
	maxDepth := h.reportElementDepthLimit()
	maxFilterElements := h.filterElementLimit()
	maxQueryBytes, maxAddressProperties := h.cardDAVQueryLimits()

	decoder := xml.NewDecoder(bytes.NewReader(body))
	decoder.Entity = xml.HTMLEntity
	depth := 0
	filterDepth := 0
	filterSpace := ""
	filterElements := 0
	queryDepth, addressDataDepth, addressProperties := 0, 0, 0
	queryBytes := int64(0)
	for {
		startOffset := decoder.InputOffset()
		token, err := decoder.Token()
		if errors.Is(err, io.EOF) {
			return nil
		}
		if err != nil {
			// A body this pass cannot tokenize is left to the decoders, whose
			// reporting of a malformed body is the more specific one.
			return nil
		}
		switch token := token.(type) {
		case xml.StartElement:
			depth++
			if queryDepth == 0 && (token.Name == cardDAVQName("filter") || token.Name == cardDAVQName("address-data")) {
				queryDepth = depth
			}
			if addressDataDepth == 0 && token.Name == cardDAVQName("address-data") {
				addressDataDepth = depth
			}
			if addressDataDepth > 0 && token.Name == cardDAVQName("prop") {
				addressProperties++
				if addressProperties > maxAddressProperties {
					return malformedReport("CARDDAV:address-data carries more than the %d properties this server selects", maxAddressProperties)
				}
			}
			if filterDepth == 0 && isReportFilterName(token.Name) {
				filterDepth = depth
				filterSpace = token.Name.Space
			}
			if filterDepth > 0 {
				filterElements++
				if filterElements > maxFilterElements {
					return filterBudgetFault(filterSpace,
						fmt.Sprintf("carries more than the %d elements this server evaluates", maxFilterElements))
				}
			}
			if depth > maxDepth {
				if filterDepth > 0 {
					return filterBudgetFault(filterSpace,
						fmt.Sprintf("nests deeper than the %d elements this server evaluates", maxDepth))
				}
				return malformedReport("REPORT body nests deeper than the %d elements this server reads", maxDepth)
			}
		case xml.EndElement:
			if filterDepth > 0 && depth == filterDepth {
				filterDepth = 0
				filterSpace = ""
			}
			depth--
		}
		if queryDepth > 0 {
			// Count wire bytes, including attributes and markup, so long names
			// cannot replace long text as work repeated for every contact.
			queryBytes += decoder.InputOffset() - startOffset
			if queryBytes > int64(maxQueryBytes) {
				return malformedReport("CardDAV query metadata exceeds %d bytes", maxQueryBytes)
			}
			if depth < queryDepth {
				queryDepth = 0
			}
		}
		if addressDataDepth > depth {
			addressDataDepth = 0
		}
	}
}

func (h *DavServer) cardDAVQueryLimits() (int, int) {
	queryBytes, properties := defaultMaxCardDAVQueryBytes, defaultMaxAddressDataProperties
	if h != nil && h.cfg != nil {
		if h.cfg.DAV.MaxCardDAVQueryBytes > 0 {
			queryBytes = h.cfg.DAV.MaxCardDAVQueryBytes
		}
		if h.cfg.DAV.MaxAddressDataProperties > 0 {
			properties = h.cfg.DAV.MaxAddressDataProperties
		}
	}
	return queryBytes, properties
}

// isReportFilterName reports whether name is one of the two REPORT filter
// elements whose size is bounded: CALDAV:filter (RFC 4791 §9.7) and
// CARDDAV:filter (RFC 6352 §10.5).
func isReportFilterName(name xml.Name) bool {
	return name == calDAVQName("filter") || name == cardDAVQName("filter")
}

// filterBudgetFault names the failure a filter past the server's element or
// nesting budget raises, which differs by grammar because the two
// specifications do. RFC 4791 §7.8.7 defines CALDAV:valid-filter, broad enough
// to carry a validity bound of the server's own. RFC 6352 defines no
// counterpart, and §8.5 and §8.6 scope CARDDAV:supported-filter to a filter
// naming a vCard property or parameter the server cannot query -- which a
// filter of well-formed elements does not do -- so a CardDAV filter past the
// budget violates no named precondition and answers 400, the general rule for a
// body no precondition reaches.
func filterBudgetFault(namespace, reason string) *reportGrammarFault {
	if namespace == namespaceCardDAV {
		return malformedReport("CARDDAV:filter %s", reason)
	}
	return invalidFilter("CALDAV:filter %s", reason)
}

func (h *DavServer) reportElementDepthLimit() int {
	if h != nil && h.cfg != nil && h.cfg.DAV.MaxReportElementDepth > 0 {
		return h.cfg.DAV.MaxReportElementDepth
	}
	return defaultMaxReportElementDepth
}

func (h *DavServer) filterElementLimit() int {
	if h != nil && h.cfg != nil && h.cfg.DAV.MaxFilterElements > 0 {
		return h.cfg.DAV.MaxFilterElements
	}
	return defaultMaxFilterElements
}
