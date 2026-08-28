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
	rowLimit := h.reportCandidateRowLimit()
	var contacts []store.Contact
	afterID := int64(0)
	for {
		page, err := h.store.Contacts.ListForBookPageAfter(ctx, bookID, afterID, multistatusPageSize)
		if err != nil {
			return nil, err
		}
		if len(page) == 0 {
			return contacts, nil
		}
		if len(contacts)+len(page) > rowLimit {
			return nil, errTooManyCandidateRows
		}
		contacts = append(contacts, page...)

		lastID := page[len(page)-1].ID
		if lastID <= afterID || len(page) < multistatusPageSize {
			return contacts, nil
		}
		afterID = lastID
	}
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
	targetResourceName := ""
	if target := parsedDAVTarget(ctx, cleanPath); target.Valid && target.Domain == davPathAddressBook && target.Resource {
		targetResourceName = target.ResourceName
	}
	baseHref := strings.TrimSuffix(cleanPath, "/") + "/"
	if targetResourceName != "" {
		baseHref = strings.TrimSuffix(strings.TrimSuffix(cleanPath, "/"), "/"+targetResourceName+".vcf") + "/"
	}
	stopAfter := h.multistatusBuildLimit()
	clientLimit := 0
	if limit != nil && limit.NResults > 0 {
		clientLimit = limit.NResults
		if clientLimit < stopAfter-1 {
			stopAfter = clientLimit + 1
		}
	}
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
			if targetResourceName != "" && resourceName != targetResourceName {
				continue
			}
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
		lastID := contacts[len(contacts)-1].ID
		if lastID <= afterID || len(contacts) < multistatusPageSize {
			break
		}
		afterID = lastID
	}
	if clientLimit > 0 && len(responses) > clientLimit {
		responses = responses[:clientLimit]
		responses = append(responses, response{
			Href:   cleanPath,
			Status: "HTTP/1.1 507 Insufficient Storage",
			Error:  &responseError{NumberOfMatchesWithinLimits: &struct{}{}},
		})
	}
	return h.finishReportResponses(ctx, user, responses, propertySelector{Prop: reqProp}, false, addressDataReq)
}

func (h *DavServer) addressBookMultiGetReport(ctx context.Context, user *store.User, book *store.AddressBook, hrefs []string, cleanPath string, reqProp *reportProp, addressDataReq *addressDataQuery, request *http.Request) ([]response, error) {
	if len(hrefs) == 0 {
		return nil, fmt.Errorf("href required")
	}
	if buildLimit := h.multistatusBuildLimit(); len(hrefs) > buildLimit {
		hrefs = hrefs[:buildLimit]
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
		since = info.Timestamp
	}

	var contacts []store.Contact
	var err error
	if since.IsZero() {
		contacts, err = h.listBoundedAddressBookContacts(ctx, book.ID)
	} else {
		contacts, err = h.store.Contacts.ListModifiedSince(ctx, book.ID, since)
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
		deleted, err := h.store.DeletedResources.ListDeletedSince(ctx, "contact", book.ID, since)
		if err != nil {
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
	return responses, syncToken, nil
}
