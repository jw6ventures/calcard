package dav

import (
	"encoding/xml"
	"errors"
	"fmt"
)

// syncPageLimit is how many changes one sync-collection page reports: what the
// response ceiling leaves beside the one DAV:response naming the collection,
// within the candidate-row budget, and within the client's DAV:limit.
func (h *DavServer) syncPageLimit(limit *syncCollectionLimit) (int, error) {
	pageLimit := min(h.maxReportResponses()-1, h.reportCandidateRowLimit())
	if limit != nil {
		// RFC 6578 §3.7: a server that cannot truncate at or below the count
		// the client asked for fails with DAV:number-of-matches-within-limits.
		// A page of no changes cannot move the client's token forward, so no
		// count below one can be honored.
		if limit.NResults < 1 {
			return 0, errNumberOfMatchesExceeded
		}
		pageLimit = min(pageLimit, limit.NResults)
	}
	if pageLimit < 1 {
		return 0, errMultistatusTooLarge
	}
	return pageLimit, nil
}

// syncTruncationMarker is the DAV:response RFC 6578 §3.6 places on the
// Request-URI of a truncated sync-collection: 507 with the
// DAV:number-of-matches-within-limits condition. It stands in for the
// collection's own response, so the collection is named once.
func syncTruncationMarker(collectionHref string) response {
	return response{
		Href:   collectionHref,
		Status: "HTTP/1.1 507 Insufficient Storage",
		Error:  &responseError{NumberOfMatchesWithinLimits: &struct{}{}},
	}
}

// fitSyncPage builds a sync-collection page small enough for the configured
// multistatus byte ceiling. The response count is bounded before the page is
// read, but its size depends on the properties and data the client asked for,
// so a page that encodes past the ceiling is read again with fewer changes,
// scaled by how many of its responses fitted. Only a page of one change that
// still does not fit is refused outright. The page is encoded once to learn
// whether it fits, and the answer carries that encoding to be written.
func (h *DavServer) fitSyncPage(limit int, build func(limit int) ([]response, string, error)) (reportAnswer, error) {
	maxResponses, maxBytes := h.multistatusLimits()
	if maxResponses <= 0 {
		maxResponses = defaultMaxMultistatusResponses
	}
	if maxBytes <= 0 {
		maxBytes = defaultMaxMultistatusBytes
	}
	for {
		responses, token, err := build(limit)
		if err != nil {
			return reportAnswer{}, err
		}
		if len(responses) > maxResponses {
			return reportAnswer{}, errMultistatusTooLarge
		}
		buf := &boundedXMLBuffer{limit: maxBytes}
		err = xml.NewEncoder(buf).Encode(newMultistatus(responses, token))
		if err == nil {
			return reportAnswer{responses: responses, syncToken: token, encoded: buf.Bytes()}, nil
		}
		if !errors.Is(err, errMultistatusTooLarge) {
			return reportAnswer{}, fmt.Errorf("encode sync page: %w", err)
		}
		if limit == 1 {
			return reportAnswer{}, errMultistatusTooLarge
		}
		fitted := multistatusResponsesWithin(responses, token, maxBytes)
		limit = max(1, min(limit-1, fitted*9/10))
	}
}

// multistatusResponsesWithin reports how many of the leading responses fit
// within maxBytes in the multistatus carrying them and token. It counts the
// encoding rather than keeping it, and stops at the first response past the
// ceiling.
func multistatusResponsesWithin(responses []response, token string, maxBytes int) int {
	counter := &byteCounter{}
	encoder := xml.NewEncoder(counter)
	if err := encoder.Encode(newMultistatus(nil, token)); err != nil {
		return 0
	}
	start := xml.StartElement{Name: xml.Name{Local: "d:response"}}
	for i := range responses {
		if err := encoder.EncodeElement(responses[i], start); err != nil {
			return i
		}
		if err := encoder.Flush(); err != nil || counter.n > maxBytes {
			return i
		}
	}
	return len(responses)
}

type byteCounter struct{ n int }

func (c *byteCounter) Write(p []byte) (int, error) {
	c.n += len(p)
	return len(p), nil
}
