package dav

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/jw6ventures/calcard/internal/store"
)

// listBoundedAddressBookContacts reads an address book in keyset pages rather
// than whole, on the same terms listBoundedCalendarEvents reads a calendar.
func (h *DavServer) listBoundedAddressBookContacts(ctx context.Context, bookID int64) ([]store.Contact, error) {
	return collectBoundedPages(ctx, h.reportCandidateRowLimit(),
		func(ctx context.Context, afterID int64) ([]store.Contact, error) {
			return h.store.Contacts.ListForBookPageAfter(ctx, bookID, afterID, multistatusPageSize)
		}, contactID)
}

// listBoundedModifiedAddressBookContacts is that read narrowed to the rows an
// incremental sync reports on, which the client's sync token decides and so
// bounds nothing on its own.
func (h *DavServer) listBoundedModifiedAddressBookContacts(ctx context.Context, bookID int64, since time.Time) ([]store.Contact, error) {
	return collectBoundedPages(ctx, h.reportCandidateRowLimit(),
		func(ctx context.Context, afterID int64) ([]store.Contact, error) {
			return h.store.Contacts.ListModifiedSincePageAfter(ctx, bookID, afterID, since, multistatusPageSize)
		}, contactID)
}

func (h *DavServer) addressBookReportResponses(ctx context.Context, user *store.User, book *store.AddressBook, principalHref, cleanPath string, report reportRequest, request *http.Request) ([]response, string, error) {
	addressDataReq := reportAddressData(report)
	switch report.XMLName.Local {
	case "addressbook-multiget":
		res, err := h.addressBookMultiGetReport(ctx, user, book, report.Hrefs, cleanPath, report.Prop, addressDataReq, request)
		return res, "", err
	case "addressbook-query":
		res, err := h.addressBookQuery(ctx, user, book, cleanPath, report.CardFilter, report.Prop, addressDataReq, report.Limit)
		return res, "", err
	case "sync-collection":
		return h.addressBookSyncCollection(ctx, user, book, principalHref, cleanPath, report)
	default:
		// RFC 3253 §3.6: unknown report types must be refused, not answered
		// with a full dump of the collection.
		return nil, "", errUnsupportedReport
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
			return nil, fmt.Errorf("failed to list contacts")
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
		return nil, fmt.Errorf("failed to fetch contact")
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
		return nil, fmt.Errorf("failed to fetch contact")
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

func (h *DavServer) addressBookSyncCollection(ctx context.Context, user *store.User, book *store.AddressBook, principalHref, cleanPath string, report reportRequest) ([]response, string, error) {
	syncToken, _ := h.addressBookSyncTokenValue(book)
	collectionHref := strings.TrimSuffix(cleanPath, "/") + "/"

	var since time.Time
	if report.SyncToken != "" {
		info, err := parseSyncToken(report.SyncToken)
		if err != nil || info.Kind != "card" || info.ID != book.ID {
			return nil, "", errInvalidSyncToken
		}
		if !h.syncTokenAnswerable(info.Timestamp, book.UpdatedAt) {
			return nil, "", errInvalidSyncToken
		}
		since = info.Timestamp
	}

	var contacts []store.Contact
	var err error
	if since.IsZero() {
		contacts, err = h.listBoundedAddressBookContacts(ctx, book.ID)
	} else {
		contacts, err = h.listBoundedModifiedAddressBookContacts(ctx, book.ID, since)
	}
	if err != nil {
		if errors.Is(err, errTooManyCandidateRows) {
			return nil, "", err
		}
		return nil, "", errors.New("failed to list contacts")
	}
	contacts, err = h.filterReadableAddressBookContacts(ctx, user, book, contacts)
	if err != nil {
		return nil, "", err
	}

	responses := []response{
		addressBookCollectionResponse(collectionHref, book.Name, book.Description, principalHref, syncToken, strconv.FormatInt(book.CTag, 10)),
	}
	addressDataReq := reportAddressData(report)
	for _, contact := range contacts {
		if h.multistatusBuildComplete(responses) {
			break
		}
		href := addressObjectHref(collectionHref, contactResourceName(contact))
		resp, err := h.buildAddressObjectReportResponse(href, contact, report.Prop, addressDataReq)
		if err != nil {
			return nil, "", err
		}
		responses = h.appendMultistatusResponses(responses, []response{resp})
	}

	// Include deleted resources if this is an incremental sync
	if !since.IsZero() && !h.multistatusBuildComplete(responses) {
		deleted, err := h.listBoundedDeletedResources(ctx, "contact", book.ID, since)
		if err != nil {
			if errors.Is(err, errTooManyCandidateRows) {
				return nil, "", err
			}
			return nil, "", fmt.Errorf("failed to list deleted contacts")
		}
		deletedNames := make([]string, 0, len(deleted))
		for _, d := range deleted {
			resourceName := d.ResourceName
			if resourceName == "" {
				resourceName = d.UID
			}
			deletedNames = append(deletedNames, resourceName)
		}
		entriesByPath, err := h.prefetchAddressBookACLEntries(ctx, user, book.ID, deletedNames)
		if err != nil {
			return nil, "", err
		}
		decider := newBatchedObjectACLDecider(user, book.UserID, addressBookCollectionResourcePath(book.ID), entriesByPath)
		for _, resourceName := range deletedNames {
			if h.multistatusBuildComplete(responses) {
				break
			}
			if !canReadAddressBookContactWithDecider(resourceName, decider) {
				continue
			}
			href := addressObjectHref(collectionHref, resourceName)
			responses = h.appendMultistatusResponses(responses, []response{deletedResponse(href)})
		}
	}

	responses, err = h.finishReportResponses(ctx, user, responses, propertySelector{Prop: report.Prop}, false, addressDataReq)
	if err != nil {
		return nil, "", err
	}
	if !h.syncTokenAnswerable(since, book.UpdatedAt) {
		return nil, "", errInvalidSyncToken
	}
	return responses, syncToken, nil
}
