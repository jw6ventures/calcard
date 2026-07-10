package dav

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/jw6ventures/calcard/internal/store"
)

func (h *DavServer) addressBookReportResponses(ctx context.Context, user *store.User, book *store.AddressBook, principalHref, cleanPath string, report reportRequest, expandReq *expandPropertyRequest) ([]response, string, error) {
	targetResourceName := ""
	addressDataReq := reportAddressData(report)
	if _, resourceName, matched := parseAddressBookResourceSegments(cleanPath); matched {
		targetResourceName = resourceName
	}
	switch report.XMLName.Local {
	case "addressbook-multiget":
		res, err := h.addressBookMultiGetReport(ctx, user, book, report.Hrefs, cleanPath, report.Prop, addressDataReq)
		return res, "", err
	case "addressbook-query":
		res, err := h.addressBookQuery(ctx, user, book, cleanPath, report.CardFilter, report.Prop, addressDataReq, report.Limit)
		return res, "", err
	case "expand-property":
		collectionHref := strings.TrimSuffix(cleanPath, "/")
		if targetResourceName == "" {
			collectionHref += "/"
		}
		if targetResourceName != "" {
			contact, err := h.store.Contacts.GetByResourceName(ctx, book.ID, targetResourceName)
			if err != nil {
				return nil, "", fmt.Errorf("failed to fetch contact")
			}
			if contact == nil {
				return []response{{Href: collectionHref, Status: httpStatusNotFound}}, "", nil
			}
			return []response{buildAddressObjectExpandPropertyResponse(collectionHref, *contact, expandReq)}, "", nil
		}
		resp := addressBookCollectionResponse(collectionHref, book.Name, book.Description, principalHref, buildSyncToken("card", book.ID, book.UpdatedAt), strconv.FormatInt(book.CTag, 10))
		selections := expandPropertySelections(expandReq)
		if len(resp.Propstat) > 0 {
			expanded := h.expandedPrincipalProp(user, selections)
			if expanded.CurrentUserPrincipal != nil {
				resp.Propstat[0].Prop.CurrentUserPrincipal = expanded.CurrentUserPrincipal
			}
			if expanded.PrincipalURL != nil {
				resp.Propstat[0].Prop.PrincipalURL = expanded.PrincipalURL
			}
		}
		return []response{resp}, "", nil
	case "sync-collection":
		return h.addressBookSyncCollection(ctx, user, book, principalHref, cleanPath, report)
	default:
		// RFC 3253 §3.6: unknown report types must be refused, not answered
		// with a full dump of the collection.
		return nil, "", errUnsupportedReport
	}
}

func (h *DavServer) addressBookQuery(ctx context.Context, user *store.User, book *store.AddressBook, cleanPath string, filter *cardFilter, reqProp *reportProp, addressDataReq *addressDataQuery, limit *addressbookLimit) ([]response, error) {
	contacts, err := h.store.Contacts.ListForBook(ctx, book.ID)
	if err != nil {
		return nil, fmt.Errorf("failed to list contacts")
	}
	targetResourceName := ""
	if _, resourceName, matched := parseAddressBookResourceSegments(cleanPath); matched {
		targetResourceName = resourceName
	}
	baseHref := strings.TrimSuffix(cleanPath, "/") + "/"
	if targetResourceName != "" {
		baseHref = strings.TrimSuffix(strings.TrimSuffix(cleanPath, "/"), "/"+targetResourceName+".vcf") + "/"
	}
	resourceNames := make([]string, 0, len(contacts))
	for _, contact := range contacts {
		resourceName := contactResourceName(contact)
		if targetResourceName != "" && resourceName != targetResourceName {
			continue
		}
		resourceNames = append(resourceNames, resourceName)
	}
	entriesByPath, err := h.prefetchAddressBookACLEntries(ctx, user, book.ID, resourceNames)
	if err != nil {
		return nil, err
	}
	var responses []response
	for _, contact := range contacts {
		resourceName := contactResourceName(contact)
		if targetResourceName != "" && resourceName != targetResourceName {
			continue
		}
		if !canReadAddressBookContactWithEntries(user, book, resourceName, entriesByPath) {
			continue
		}
		if !contactMatchesCardFilter(contact, filter) {
			continue
		}
		href := baseHref + resourceName + ".vcf"
		responses = append(responses, buildAddressObjectReportResponse(href, contact, reqProp, addressDataReq))
	}
	if limit != nil && limit.NResults > 0 && len(responses) > limit.NResults {
		responses = responses[:limit.NResults]
		responses = append(responses, response{
			Href:   cleanPath,
			Status: "HTTP/1.1 507 Insufficient Storage",
			Error:  &responseError{NumberOfMatchesWithinLimits: &struct{}{}},
		})
	}
	return responses, nil
}

func (h *DavServer) addressBookMultiGetReport(ctx context.Context, user *store.User, book *store.AddressBook, hrefs []string, cleanPath string, reqProp *reportProp, addressDataReq *addressDataQuery) ([]response, error) {
	if len(hrefs) == 0 {
		return nil, fmt.Errorf("href required")
	}
	bookID := book.ID
	targetResourceName := ""
	if _, resourceName, matched := parseAddressBookResourceSegments(cleanPath); matched {
		targetResourceName = resourceName
	}
	resourceNames := make([]string, 0, len(hrefs))
	seenNames := make(map[string]struct{}, len(hrefs))
	for _, href := range hrefs {
		cleanHref := resolveDAVHref(cleanPath, href)
		if cleanHref == "" {
			continue
		}
		if _, resourceName, ok := parseAddressBookResourceSegments(cleanHref); ok {
			if _, ok := seenNames[resourceName]; ok {
				continue
			}
			seenNames[resourceName] = struct{}{}
			resourceNames = append(resourceNames, resourceName)
		}
	}
	entriesByPath, err := h.prefetchAddressBookACLEntries(ctx, user, bookID, resourceNames)
	if err != nil {
		return nil, err
	}
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
		cleanHref := resolveDAVHref(cleanPath, href)
		responseHref := cleanHref
		if responseHref == "" {
			responseHref = strings.TrimSpace(href)
		}
		if responseHref == "" {
			responseHref = cleanPath
		}
		if cleanHref == "" {
			responses = append(responses, response{Href: responseHref, Status: httpStatusNotFound})
			continue
		}
		segment, resourceName, ok := parseAddressBookResourceSegments(cleanHref)
		if !ok {
			responses = append(responses, response{Href: responseHref, Status: httpStatusNotFound})
			continue
		}
		id, ok := resolveSegment(segment)
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
		if !canReadAddressBookContactWithEntries(user, book, resourceName, entriesByPath) {
			responses = append(responses, response{Href: responseHref, Status: httpStatusNotFound})
			continue
		}
		responses = append(responses, buildAddressObjectReportResponse(responseHref, *c, reqProp, addressDataReq))
	}
	return responses, nil
}

func (h *DavServer) addressBookSyncCollection(ctx context.Context, user *store.User, book *store.AddressBook, principalHref, cleanPath string, report reportRequest) ([]response, string, error) {
	syncToken, _ := h.addressBookSyncTokenValue(ctx, book)
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
		contacts, err = h.store.Contacts.ListForBook(ctx, book.ID)
	} else {
		contacts, err = h.store.Contacts.ListModifiedSince(ctx, book.ID, since)
	}
	if err != nil {
		return nil, "", fmt.Errorf("failed to list contacts")
	}
	contacts, err = h.filterReadableAddressBookContacts(ctx, user, book, contacts)
	if err != nil {
		return nil, "", err
	}

	responses := []response{
		addressBookCollectionResponse(collectionHref, book.Name, book.Description, principalHref, syncToken, strconv.FormatInt(book.CTag, 10)),
	}
	responses = append(responses, addressBookResourceResponses(collectionHref, contacts)...)

	// Include deleted resources if this is an incremental sync
	if !since.IsZero() {
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
		for _, resourceName := range deletedNames {
			if !canReadAddressBookContactWithEntries(user, book, resourceName, entriesByPath) {
				continue
			}
			href := collectionHref + resourceName + ".vcf"
			responses = append(responses, deletedResponse(href))
		}
	}

	return responses, syncToken, nil
}
