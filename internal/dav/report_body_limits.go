package dav

import (
	"bytes"
	"encoding/xml"
	"errors"
	"io"
)

const (
	defaultMaxFilterElements     = 100
	defaultMaxReportElementDepth = 20
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
// each CalDAV filter element is evaluated against every candidate resource.
//
// It measures the body once, iteratively, ahead of every decode, because the
// permissive struct decode recurses through the same body the grammar pass
// later re-reads: a bound applied only in the grammar pass would be applied
// after the cost had already been paid.
//
// Depth is measured over the whole document, since every recursive decoder
// descends with it. The element count is scoped to CALDAV:filter, where a
// legitimate body carries a handful of elements and each one multiplies the
// collection scan; the rest of a body is bounded by its element depth and by
// the request size cap.
func (h *DavServer) checkReportBodyLimits(body []byte) *reportGrammarFault {
	maxDepth := h.reportElementDepthLimit()
	maxFilterElements := h.filterElementLimit()

	decoder := xml.NewDecoder(bytes.NewReader(body))
	decoder.Entity = xml.HTMLEntity
	depth := 0
	filterDepth := 0
	filterElements := 0
	for {
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
			inFilter := filterDepth > 0
			if !inFilter && token.Name == calDAVQName("filter") {
				filterDepth = depth
				inFilter = true
			}
			if inFilter {
				filterElements++
				if filterElements > maxFilterElements {
					return invalidFilter("CALDAV:filter carries more than the %d elements this server evaluates", maxFilterElements)
				}
			}
			if depth > maxDepth {
				if inFilter {
					return invalidFilter("CALDAV:filter nests deeper than the %d elements this server evaluates", maxDepth)
				}
				return malformedReport("REPORT body nests deeper than the %d elements this server reads", maxDepth)
			}
		case xml.EndElement:
			if filterDepth > 0 && depth == filterDepth {
				filterDepth = 0
			}
			depth--
		}
	}
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
