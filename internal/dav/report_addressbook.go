package dav

import (
	"context"
	"fmt"
	"net/http"
	"strconv"
	"strings"

	"github.com/jw6ventures/calcard/internal/store"
)

func (h *DavServer) addressBookReportResponses(ctx context.Context, user *store.User, book *store.AddressBook, principalHref, cleanPath string, report reportRequest, request *http.Request) (reportAnswer, error) {
	addressDataReq := reportAddressData(report)
	switch report.XMLName.Local {
	case "addressbook-multiget":
		res, err := h.addressBookMultiGetReport(ctx, user, book, report.Hrefs, cleanPath, report.Prop, addressDataReq, request)
		return reportAnswer{responses: res}, err
	case "addressbook-query":
		res, err := h.addressBookQuery(ctx, user, book, cleanPath, report.CardFilter, report.Prop, addressDataReq, report.Limit)
		return reportAnswer{responses: res}, err
	case "sync-collection":
		return h.addressBookSyncCollection(ctx, user, book, principalHref, cleanPath, report)
	default:
		// RFC 3253 §3.6: unknown report types must be refused, not answered
		// with a full dump of the collection.
		return reportAnswer{}, errUnsupportedReport
	}
}

func (h *DavServer) addressBookQuery(ctx context.Context, user *store.User, book *store.AddressBook, cleanPath string, filter *cardFilter, reqProp *reportProp, addressDataReq *addressDataQuery, limit *addressbookLimit) ([]response, error) {
	if target := parsedDAVTarget(ctx, cleanPath); target.Valid && target.Domain == davPathAddressBook && target.Resource {
		baseHref := strings.TrimSuffix(strings.TrimSuffix(cleanPath, "/"), "/"+target.ResourceName+".vcf") + "/"
		return h.addressBookObjectQuery(ctx, user, book, baseHref, target.ResourceName, filter, reqProp, addressDataReq)
	}
	baseHref := ensureCollectionHref(cleanPath)
	// The §8.6.2 truncation marker names the Request-URI, which is this
	// collection: cleanPath has had any trailing slash cleaned off it, and a
	// collection href carries one.
	requestHref := baseHref
	stopAfter := h.multistatusBuildLimit()
	clientLimit := 0
	if limit != nil && limit.NResults > 0 {
		clientLimit = limit.NResults
		if clientLimit < stopAfter-1 {
			stopAfter = clientLimit + 1
		}
	}
	rowLimit := h.reportCandidateRowLimit()
	truncated := false
	scanned := 0
	var responses []response
	afterID := int64(0)
	for len(responses) < stopAfter {
		contacts, err := h.store.Contacts.ListForBookPageAfter(ctx, book.ID, afterID, multistatusPageSize)
		if err != nil {
			return nil, fmt.Errorf("failed to list contacts: %w", err)
		}
		if len(contacts) == 0 {
			break
		}
		// A filter matching nothing costs the same parse per contact as one
		// matching everything, so the row budget is what bounds a query whose
		// response count never grows. A page reaching past the budget is trimmed
		// to what is left of it rather than dropped, so no candidate row the
		// budget allows goes unexamined -- dropping it would answer nothing at
		// all for a budget shorter than one page. The query then stops reading
		// and reports what it has under the §8.6.2 truncation marker.
		scanned += len(contacts)
		if scanned > rowLimit {
			contacts = contacts[:len(contacts)-(scanned-rowLimit)]
			truncated = true
			if len(contacts) == 0 {
				break
			}
		}
		resourceNames := make([]string, 0, len(contacts))
		for _, contact := range contacts {
			resourceNames = append(resourceNames, contactResourceName(contact))
		}
		entriesByPath, err := h.prefetchAddressBookACLEntries(ctx, user, book.ID, resourceNames)
		if err != nil {
			return nil, err
		}
		decider := newBatchedObjectACLDecider(user, book.UserID, addressBookCollectionResourcePath(book.ID), entriesByPath)
		for _, contact := range contacts {
			resourceName := contactResourceName(contact)
			if !canReadAddressBookContactWithDecider(resourceName, decider) || !contactMatchesCardFilter(contact, filter) {
				continue
			}
			href := addressObjectHref(baseHref, resourceName)
			resp, err := h.buildAddressObjectReportResponse(href, contact, reqProp, addressDataReq)
			if err != nil {
				return nil, err
			}
			responses = append(responses, resp)
			if len(responses) >= stopAfter {
				break
			}
		}
		if truncated {
			break
		}
		lastID := contacts[len(contacts)-1].ID
		if lastID <= afterID || len(contacts) < multistatusPageSize {
			break
		}
		afterID = lastID
	}
	// RFC 6352 §8.6.2 covers a limit the client asked for and a limit the server
	// imposes "to limit the amount of work expended in processing a query" with
	// one answer: a 207 whose DAV:response for the Request-URI carries 507 and
	// the DAV:number-of-matches-within-limits precondition, beside the partial
	// results. §8.6.2 excludes that marker from a client-requested count, so the
	// client limit trims to itself; the server's own response ceiling counts
	// every DAV:response it returns, so the matches are capped one slot short of
	// it to leave the marker room.
	if clientLimit > 0 && len(responses) > clientLimit {
		responses = responses[:clientLimit]
		truncated = true
	}
	if maxResponses := h.maxReportResponses(); len(responses) >= maxResponses {
		if len(responses) > maxResponses {
			truncated = true
		}
		if truncated {
			responses = responses[:maxResponses-1]
		}
	}
	if truncated {
		responses = append(responses, response{
			Href:   requestHref,
			Status: "HTTP/1.1 507 Insufficient Storage",
			Error:  &responseError{NumberOfMatchesWithinLimits: &struct{}{}},
		})
	}
	return h.finishReportResponses(ctx, user, responses, propertySelector{Prop: reqProp}, false, addressDataReq)
}

// addressBookObjectQuery answers the report RFC 6352 §8.6 serves on an address
// object resource, which is one read of the resource the Request-URI names. The
// collection path above cannot serve it: paging the whole book to find one
// contact spends the candidate-row budget on rows the report can never answer
// over, so a book past that budget would answer the §8.6.2 truncation marker
// instead of the resource asked for. It is the CardDAV counterpart of
// calendarObjectQuery.
func (h *DavServer) addressBookObjectQuery(ctx context.Context, user *store.User, book *store.AddressBook, baseHref, resourceName string, filter *cardFilter, reqProp *reportProp, addressDataReq *addressDataQuery) ([]response, error) {
	contact, err := h.store.Contacts.GetByResourceName(ctx, book.ID, resourceName)
	if err != nil {
		return nil, fmt.Errorf("failed to fetch contact: %w", err)
	}
	var responses []response
	if contact != nil && contactMatchesCardFilter(*contact, filter) {
		visible, err := h.filterReadableAddressBookContacts(ctx, user, book, []store.Contact{*contact})
		if err != nil {
			return nil, err
		}
		for _, readable := range visible {
			href := addressObjectHref(baseHref, contactResourceName(readable))
			resp, err := h.buildAddressObjectReportResponse(href, readable, reqProp, addressDataReq)
			if err != nil {
				return nil, err
			}
			responses = append(responses, resp)
		}
	}
	return h.finishReportResponses(ctx, user, responses, propertySelector{Prop: reqProp}, false, addressDataReq)
}

func (h *DavServer) addressBookMultiGetReport(ctx context.Context, user *store.User, book *store.AddressBook, hrefs []string, cleanPath string, reqProp *reportProp, addressDataReq *addressDataQuery, request *http.Request) ([]response, error) {
	if len(hrefs) == 0 {
		return nil, fmt.Errorf("href required")
	}
	// RFC 6352 §8.7 owes one DAV:response per DAV:href and states no truncation
	// rule of its own -- §8.6.2 is scoped to addressbook-query -- so a list past
	// what the server will answer is refused with the RFC 4918 capacity status
	// rather than trimmed, which would present the leading hrefs as the whole
	// answer.
	if len(hrefs) > h.multigetHrefLimit() {
		return nil, errTooManyHrefs
	}
	bookID := book.ID
	targetResourceName := ""
	if target := parsedDAVTarget(ctx, cleanPath); target.Valid && target.Domain == davPathAddressBook && target.Resource {
		targetResourceName = target.ResourceName
	}
	resourceNames := make([]string, 0, len(hrefs))
	seenNames := make(map[string]struct{}, len(hrefs))
	for _, href := range hrefs {
		resolved, ok := h.resolveAddressBookHrefForRequest(href, request)
		if !ok {
			continue
		}
		if _, seen := seenNames[resolved.ResourceName]; seen {
			continue
		}
		seenNames[resolved.ResourceName] = struct{}{}
		resourceNames = append(resourceNames, resolved.ResourceName)
	}
	entriesByPath, err := h.prefetchAddressBookACLEntries(ctx, user, bookID, resourceNames)
	if err != nil {
		return nil, err
	}
	decider := newBatchedObjectACLDecider(user, book.UserID, addressBookCollectionResourcePath(book.ID), entriesByPath)
	// Batch-fetch the contacts and memoize collection-segment resolution: a
	// multiget of hundreds of hrefs otherwise issues one contact query and one
	// segment lookup per href.
	contacts, err := h.store.Contacts.ListByResourceNames(ctx, bookID, resourceNames)
	if err != nil {
		return nil, fmt.Errorf("failed to fetch contacts: %w", err)
	}
	contactsByName := make(map[string]*store.Contact, len(contacts))
	for i := range contacts {
		contactsByName[contactResourceName(contacts[i])] = &contacts[i]
	}
	type segmentResolution struct {
		id int64
		ok bool
	}
	segmentIDs := make(map[string]segmentResolution)
	resolveSegment := func(segment string) (int64, bool) {
		if resolution, ok := segmentIDs[segment]; ok {
			return resolution.id, resolution.ok
		}
		id, ok, err := h.resolveAddressBookID(ctx, user, segment)
		resolution := segmentResolution{id: id, ok: ok && err == nil}
		segmentIDs[segment] = resolution
		return resolution.id, resolution.ok
	}
	var responses []response
	for _, href := range hrefs {
		resolved, ok := h.resolveAddressBookHrefForRequest(href, request)
		resourceName := resolved.ResourceName
		responseHref := multiGetFallbackHref(href, resolved.Path, cleanPath)
		// RFC 6352 §8.7: every requested href needs a DAV:response, so an
		// unresolvable or out-of-scope one reports 404 under the best href the
		// request gives us instead of being dropped.
		if !ok {
			responses = append(responses, response{Href: responseHref, Status: httpStatusNotFound})
			continue
		}
		id, ok := resolveSegment(resolved.Segment)
		if !ok || id != bookID {
			responses = append(responses, response{Href: responseHref, Status: httpStatusNotFound})
			continue
		}
		c := contactsByName[resourceName]
		if c == nil {
			responses = append(responses, response{Href: responseHref, Status: httpStatusNotFound})
			continue
		}
		if targetResourceName != "" && resourceName != targetResourceName {
			responses = append(responses, response{Href: responseHref, Status: httpStatusNotFound})
			continue
		}
		if !canReadAddressBookContactWithDecider(resourceName, decider) {
			responses = append(responses, response{Href: responseHref, Status: httpStatusNotFound})
			continue
		}
		resp, err := h.buildAddressObjectReportResponse(responseHref, *c, reqProp, addressDataReq)
		if err != nil {
			return nil, err
		}
		responses = append(responses, resp)
	}
	return h.finishReportResponses(ctx, user, responses, propertySelector{Prop: reqProp}, false, addressDataReq)
}

func (h *DavServer) addressBookSyncCollection(ctx context.Context, user *store.User, book *store.AddressBook, principalHref, cleanPath string, report reportRequest) (reportAnswer, error) {
	position, err := parseStoredSyncPosition(report.SyncToken, "card", book.ID)
	if err != nil {
		return reportAnswer{}, err
	}
	if position.fresh {
		position.anchor = book.UpdatedAt
	}
	if !h.syncTokenAnswerable(position.anchor, book.UpdatedAt) {
		return reportAnswer{}, errInvalidSyncToken
	}
	limit, err := h.syncPageLimit(report.SyncLimit)
	if err != nil {
		return reportAnswer{}, err
	}
	answer, err := h.fitSyncPage(limit, func(limit int) ([]response, string, error) {
		return h.addressBookSyncPage(ctx, user, book, principalHref, cleanPath, report, position, limit)
	})
	if err != nil {
		return reportAnswer{}, err
	}
	if !h.syncTokenAnswerable(position.anchor, book.UpdatedAt) {
		return reportAnswer{}, errInvalidSyncToken
	}
	return answer, nil
}

// addressBookSyncPage reports up to limit changes of the address book's change
// stream after position, on the terms calendarSyncPage reports a calendar's.
//
// A contact the requester cannot read is never named, not even as removed:
// reporting the change since a client's token refuses the token instead, and
// the client's full resynchronization reconciles what it lost. A listing
// leaves such contacts out, as its first page does, since a refusal there
// would restart a listing that reaches the same contact again.
func (h *DavServer) addressBookSyncPage(ctx context.Context, user *store.User, book *store.AddressBook, principalHref, cleanPath string, report reportRequest, position syncPosition, limit int) ([]response, string, error) {
	page, err := h.store.Contacts.ListChangesAfter(ctx, book.ID, position.after, !position.fresh, limit)
	if err != nil {
		return nil, "", fmt.Errorf("failed to list contact changes: %w", err)
	}
	contacts, err := h.filterReadableAddressBookContacts(ctx, user, book, page.Members)
	if err != nil {
		return nil, "", err
	}
	if !position.listing && len(contacts) != len(page.Members) {
		return nil, "", errInvalidSyncToken
	}

	collectionHref := strings.TrimSuffix(cleanPath, "/") + "/"
	syncToken, _ := h.addressBookSyncTokenValue(book)
	responses := make([]response, 0, len(contacts)+len(page.Removals)+1)
	if !page.More {
		responses = append(responses, addressBookCollectionResponse(collectionHref, book.Name, book.Description, principalHref, syncToken, strconv.FormatInt(book.CTag, 10)))
	}
	addressDataReq := reportAddressData(report)
	for _, contact := range contacts {
		href := addressObjectHref(collectionHref, contactResourceName(contact))
		resp, err := h.buildAddressObjectReportResponse(href, contact, report.Prop, addressDataReq)
		if err != nil {
			return nil, "", err
		}
		responses = append(responses, resp)
	}

	if len(page.Removals) > 0 {
		names := make([]string, len(page.Removals))
		for i, tombstone := range page.Removals {
			names[i] = deletedResourceName(tombstone)
		}
		entriesByPath, err := h.prefetchAddressBookACLEntries(ctx, user, book.ID, names)
		if err != nil {
			return nil, "", err
		}
		decider := newBatchedObjectACLDecider(user, book.UserID, addressBookCollectionResourcePath(book.ID), entriesByPath)
		removed := make(map[string]struct{}, len(names))
		for _, name := range names {
			if !canReadAddressBookContactWithDecider(name, decider) {
				if position.listing {
					continue
				}
				return nil, "", errInvalidSyncToken
			}
			href := addressObjectHref(collectionHref, name)
			if _, ok := removed[href]; ok {
				continue
			}
			removed[href] = struct{}{}
			responses = append(responses, deletedResponse(href))
		}
	}

	responses, err = h.finishReportResponses(ctx, user, responses, propertySelector{Prop: report.Prop}, false, addressDataReq)
	if err != nil {
		return nil, "", err
	}
	if page.More {
		responses = append(responses, syncTruncationMarker(collectionHref))
		syncToken = buildSyncPageToken("card", book.ID, position, page.Last)
	}
	return responses, syncToken, nil
}
