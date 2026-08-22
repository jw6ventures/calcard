package dav

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"path"
	"strconv"
	"strings"
	"time"

	"github.com/jw6ventures/calcard/internal/store"
)

// depthIncludesChildren reports whether a PROPFIND depth asks for a
// collection's members. Depth: infinity includes them at every level; the
// builders that own multi-level hierarchies additionally recurse when the
// depth is "infinity".
func depthIncludesChildren(depth string) bool {
	return depth == "1" || depth == "infinity"
}

func (h *DavServer) buildPropfindResponses(ctx context.Context, r *http.Request, reqPath, depth string, user *store.User, propfindReq *propfindRequest) ([]response, error) {
	cleanPath := path.Clean(reqPath)
	if !strings.HasPrefix(cleanPath, "/dav") {
		return nil, http.ErrNotSupported
	}

	switch {
	case cleanPath == "/dav" || cleanPath == "/dav/":
		href := ensureCollectionHref(cleanPath)
		principalHref := h.principalURL(user)
		res := []response{rootCollectionResponse(href, principalHref)}
		switch depth {
		case "1":
			res = h.appendMultistatusResponses(res, []response{
				collectionResponse(ensureCollectionHref("/dav/calendars"), "Calendars"),
				collectionResponse(ensureCollectionHref("/dav/addressbooks"), "Address Books"),
				principalResponse(ensureCollectionHref(principalHref), user),
			})
		case "infinity":
			// Recurse into each home set; their builders emit the collection
			// response followed by every member.
			calendarRes, err := h.calendarResponses(ctx, "/dav/calendars", depth, user)
			if err != nil {
				return nil, err
			}
			calendarRes, err = h.appendLockNullMembers(ctx, user, "/dav/calendars", depth, calendarRes)
			if err != nil {
				return nil, err
			}
			res = h.appendMultistatusResponses(res, calendarRes)
			if !h.multistatusBuildComplete(res) {
				addressBookRes, err := h.addressBookResponses(ctx, "/dav/addressbooks", depth, user)
				if err != nil {
					return nil, err
				}
				addressBookRes, err = h.appendLockNullMembers(ctx, user, "/dav/addressbooks", depth, addressBookRes)
				if err != nil {
					return nil, err
				}
				res = h.appendMultistatusResponses(res, addressBookRes)
			}
			if !h.multistatusBuildComplete(res) {
				res = h.appendMultistatusResponses(res, []response{principalResponse(ensureCollectionHref(principalHref), user)})
			}
		}
		res, err := h.appendCollectionContributors(ctx, r, user, cleanPath, depth, res)
		if err != nil {
			return nil, err
		}
		if err := h.decoratePropfindResponses(ctx, r, user, res, decorationMaskFor(propfindReq)); err != nil {
			return nil, err
		}
		if propfindReq != nil && propfindReq.AllProp != nil {
			stripCalendarAllprop(res)
			stripAddressBookAllprop(res)
			stripPrincipalAllprop(res)
		}
		if propfindReq != nil && propfindReq.Prop != nil {
			for i := range res {
				res[i] = filterNonPrincipalPropfindResponse(res[i], propfindReq)
			}
		}
		return res, nil
	case strings.HasPrefix(cleanPath, "/dav/principals"):
		responses, err := h.principalResponses(cleanPath, depth, user)
		if err != nil {
			return nil, err
		}
		responses, err = h.appendCollectionContributors(ctx, r, user, cleanPath, depth, responses)
		if err != nil {
			return nil, err
		}
		if err := h.decoratePropfindResponses(ctx, r, user, responses, decorationMaskFor(propfindReq)); err != nil {
			return nil, err
		}
		if propfindReq != nil && propfindReq.AllProp != nil {
			stripPrincipalAllprop(responses)
		}
		if propfindReq != nil && propfindReq.Prop != nil {
			for i := range responses {
				responses[i] = filterNonPrincipalPropfindResponse(responses[i], propfindReq)
			}
		}
		return responses, nil
	case strings.HasPrefix(cleanPath, "/dav/calendars"):
		responses, err := h.calendarResponses(ctx, cleanPath, depth, user)
		if err != nil {
			lockNull, lockErr := h.lockNullResourceResponse(ctx, user, cleanPath)
			if lockErr != nil {
				return nil, lockErr
			}
			if lockNull == nil {
				return nil, err
			}
			responses = []response{*lockNull}
		} else if len(responses) == 1 && responses[0].Status == httpStatusNotFound {
			lockNull, lockErr := h.lockNullResourceResponse(ctx, user, cleanPath)
			if lockErr != nil {
				return nil, lockErr
			}
			if lockNull != nil {
				responses = []response{*lockNull}
			}
		}
		responses, err = h.appendLockNullMembers(ctx, user, cleanPath, depth, responses)
		if err != nil {
			return nil, err
		}
		responses, err = h.appendCollectionContributors(ctx, r, user, cleanPath, depth, responses)
		if err != nil {
			return nil, err
		}
		if err := h.decoratePropfindResponses(ctx, r, user, responses, decorationMaskFor(propfindReq)); err != nil {
			return nil, err
		}
		if propfindReq != nil && propfindReq.AllProp != nil {
			stripCalendarAllprop(responses)
		}
		if propfindReq != nil && propfindReq.Prop != nil {
			for i := range responses {
				responses[i] = filterNonPrincipalPropfindResponse(responses[i], propfindReq)
			}
		}
		return responses, nil
	case strings.HasPrefix(cleanPath, "/dav/addressbooks"):
		responses, err := h.addressBookResponses(ctx, cleanPath, depth, user)
		if err != nil {
			lockNull, lockErr := h.lockNullResourceResponse(ctx, user, cleanPath)
			if lockErr != nil {
				return nil, lockErr
			}
			if lockNull == nil {
				return nil, err
			}
			responses = []response{*lockNull}
		} else if len(responses) == 1 && responses[0].Status == httpStatusNotFound {
			lockNull, lockErr := h.lockNullResourceResponse(ctx, user, cleanPath)
			if lockErr != nil {
				return nil, lockErr
			}
			if lockNull != nil {
				responses = []response{*lockNull}
			}
		}
		responses, err = h.appendLockNullMembers(ctx, user, cleanPath, depth, responses)
		if err != nil {
			return nil, err
		}
		responses, err = h.appendCollectionContributors(ctx, r, user, cleanPath, depth, responses)
		if err != nil {
			return nil, err
		}
		if err := h.decoratePropfindResponses(ctx, r, user, responses, decorationMaskFor(propfindReq)); err != nil {
			return nil, err
		}
		if propfindReq != nil && propfindReq.AllProp != nil {
			stripAddressBookAllprop(responses)
		}
		if propfindReq != nil && propfindReq.Prop != nil {
			for i := range responses {
				responses[i] = filterNonPrincipalPropfindResponse(responses[i], propfindReq)
			}
		}
		return responses, nil
	default:
		collection, ok := h.davRegistry().registeredExtensionCollection(cleanPath)
		if !ok {
			return nil, http.ErrNotSupported
		}
		href := normalizeDAVHref(collection.Href)
		if !strings.HasSuffix(href, "/") {
			href += "/"
		}
		if collection.Name == "" {
			collection.Name = path.Base(strings.TrimSuffix(href, "/"))
		}
		responses := []response{collectionResponse(href, collection.Name)}
		var err error
		responses, err = h.appendCollectionContributors(ctx, r, user, cleanPath, depth, responses)
		if err != nil {
			return nil, err
		}
		if err := h.decoratePropfindResponses(ctx, r, user, responses, decorationMaskFor(propfindReq)); err != nil {
			return nil, err
		}
		if propfindReq != nil && propfindReq.Prop != nil {
			for i := range responses {
				responses[i] = filterNonPrincipalPropfindResponse(responses[i], propfindReq)
			}
		}
		return responses, nil
	}
}

func (h *DavServer) lockNullResourceResponse(ctx context.Context, user *store.User, cleanPath string) (*response, error) {
	if h == nil || h.store == nil || h.store.Locks == nil || user == nil {
		return nil, nil
	}
	canonicalPath, err := h.canonicalDAVPath(ctx, user, cleanPath)
	if err != nil {
		return nil, nil
	}
	locks, err := h.store.Locks.ListByResource(ctx, canonicalPath)
	if err != nil {
		return nil, err
	}
	now := time.Now()
	for i := range locks {
		lock := &locks[i]
		if lock.ExpiresAt.Before(now) {
			continue
		}
		publicPath := publicDAVLockRoot(lock.ResourcePath)
		allowed, err := h.checkACLPrivilege(ctx, user, path.Dir(publicPath), "read")
		if err != nil {
			return nil, err
		}
		if !allowed {
			return nil, nil
		}
		result := resourceResponse(publicPath, statusOKProp(path.Base(publicPath), resourceType{}))
		return &result, nil
	}
	return nil, nil
}

func (h *DavServer) appendLockNullMembers(ctx context.Context, user *store.User, cleanPath, depth string, responses []response) ([]response, error) {
	if !depthIncludesChildren(depth) || h == nil || h.store == nil || h.store.Locks == nil || user == nil {
		return responses, nil
	}
	parentPath := normalizeDAVHref(cleanPath)
	locks, err := h.store.Locks.ListByResourcePrefix(ctx, strings.TrimSuffix(parentPath, "/")+"/")
	if err != nil {
		return nil, err
	}
	seen := make(map[string]struct{}, len(responses))
	for i := range responses {
		seen[normalizeDAVHref(responses[i].Href)] = struct{}{}
	}
	now := time.Now()
	for i := range locks {
		lock := &locks[i]
		if lock.ExpiresAt.Before(now) {
			continue
		}
		if _, pending := publicPendingCollectionPath(lock.ResourcePath); pending && lock.UserID != user.ID {
			continue
		}
		publicPath := publicDAVLockRoot(lock.ResourcePath)
		candidatePath := normalizeDAVHref(publicPath)
		if depth == "1" {
			if path.Dir(candidatePath) != parentPath {
				continue
			}
		} else if !strings.HasPrefix(candidatePath, strings.TrimSuffix(parentPath, "/")+"/") {
			continue
		}
		if _, ok := seen[candidatePath]; ok {
			continue
		}
		exists, err := h.lockTargetExists(ctx, user, publicPath)
		if err != nil {
			return nil, err
		}
		if exists {
			continue
		}
		allowed, err := h.checkACLPrivilege(ctx, user, path.Dir(publicPath), "read")
		if err != nil {
			return nil, err
		}
		if !allowed {
			continue
		}
		responses = h.appendMultistatusResponses(responses, []response{
			resourceResponse(publicPath, statusOKProp(path.Base(publicPath), resourceType{})),
		})
		seen[candidatePath] = struct{}{}
	}
	return responses, nil
}

// stripCalendarAllprop removes the CalDAV properties RFC 4791 keeps out of a
// DAV:allprop response. §5.2.1, §5.2.2, §5.2.3, §5.2.4, §5.2.5 through §5.2.9,
// §6.2.1 and §7.5.1 each carry a SHOULD NOT for their property, and the rule is
// absence rather than a 404: allprop returns the properties the server chooses
// to expose that way, so an excluded one simply is not there.
func stripCalendarAllprop(responses []response) {
	for i := range responses {
		for j := range responses[i].Propstat {
			prop := &responses[i].Propstat[j].Prop
			prop.CalendarData = ""
			// §7.5.1 applies wherever the property is defined, and it is defined
			// on calendar object resources as well as on their collections.
			prop.CalDAVSupportedCollationSet = nil
			if prop.ResourceType == nil || prop.ResourceType.Calendar == nil {
				continue
			}
			prop.CalendarDescription = nil
			prop.CalendarTimezone = nil
			prop.SupportedCalendarComponentSet = nil
			prop.SupportedCalendarData = nil
			prop.CalendarHomeSet = nil
			prop.MaxResourceSize = ""
			prop.MinDateTime = ""
			prop.MaxDateTime = ""
			prop.MaxInstances = ""
			prop.MaxAttendeesPerInstance = ""
		}
	}
}

func (h *DavServer) appendCollectionContributors(ctx context.Context, r *http.Request, user *store.User, cleanPath, depth string, responses []response) ([]response, error) {
	if !depthIncludesChildren(depth) || h.multistatusBuildComplete(responses) {
		return responses, nil
	}
	collections, err := h.davRegistry().contributeCollections(RequestContext{
		Context: ctx,
		User:    user,
		Request: r,
		Path:    cleanPath,
		Depth:   depth,
	})
	if err != nil {
		return nil, err
	}
	for _, c := range collections {
		href := normalizeDAVHref(c.Href)
		if strings.HasSuffix(c.Href, "/") && !strings.HasSuffix(href, "/") {
			href += "/"
		}
		if href == "." || href == "" {
			continue
		}
		if c.Name == "" {
			c.Name = path.Base(strings.TrimSuffix(href, "/"))
		}
		responses = h.appendMultistatusResponses(responses, []response{collectionResponse(href, c.Name)})
		if h.multistatusBuildComplete(responses) {
			break
		}
	}
	return responses, nil
}

func (h *DavServer) calendarResponses(ctx context.Context, cleanPath, depth string, user *store.User) ([]response, error) {
	relPath := strings.Trim(strings.TrimPrefix(cleanPath, "/dav/calendars"), "/")
	if relPath == "" {
		base := ensureCollectionHref("/dav/calendars")
		res := []response{collectionResponse(base, "Calendars")}
		if depthIncludesChildren(depth) {
			cals, err := h.accessibleCalendars(ctx, user)
			if err != nil {
				return nil, err
			}
			principalHref := h.principalURL(user)

			// Add the virtual birthday calendar first
			birthdayHref := birthdayCalendarHref()
			res = h.appendMultistatusResponses(res, []response{birthdayCalendarCollection(birthdayHref, principalHref)})
			if depth == "infinity" && !h.multistatusBuildComplete(res) && h.store != nil && h.store.Contacts != nil {
				events, err := h.generateBirthdayEvents(ctx, user.ID)
				if err != nil {
					return nil, err
				}
				res = h.appendMultistatusResponses(res, calendarResourceResponsesFilteredLimit(birthdayHref, events, calendarDataProjection{}, h.multistatusBuildLimit()-len(res)))
			}

			// Add regular calendars
			for i := range cals {
				if h.multistatusBuildComplete(res) {
					break
				}
				c := &cals[i]
				readable, err := h.canReadCalendarCollection(ctx, user, c)
				if err != nil {
					return nil, err
				}
				if !readable {
					continue
				}
				href := ensureCollectionHref(path.Join("/dav/calendars", fmt.Sprint(c.ID)))
				ctag := strconv.FormatInt(c.CTag, 10)
				syncToken := buildSyncToken("cal", c.ID, c.UpdatedAt)
				res = h.appendMultistatusResponses(res, []response{calendarCollectionResponseWithPrivileges(href, c.Name, c.Calendar, principalHref, syncToken, ctag, c.EffectivePrivileges())})
				if depth == "infinity" && !h.multistatusBuildComplete(res) {
					res, err = h.appendCalendarPropfindPages(ctx, user, c, href, res)
					if err != nil {
						return nil, err
					}
				}
			}
		}
		return res, nil
	}

	segments := strings.Split(relPath, "/")
	if len(segments) > 2 {
		return nil, http.ErrNotSupported
	}
	calID, err := strconv.ParseInt(segments[0], 10, 64)
	if calID == birthdayCalendarID {
		href := birthdayCalendarHref()
		principalHref := h.principalURL(user)
		res := []response{birthdayCalendarCollection(href, principalHref)}

		if depthIncludesChildren(depth) {
			events, err := h.generateBirthdayEvents(ctx, user.ID)
			if err != nil {
				return nil, err
			}
			base := ensureCollectionHref(href)
			res = h.appendMultistatusResponses(res, calendarResourceResponsesFilteredLimit(base, events, calendarDataProjection{}, h.multistatusBuildLimit()-len(res)))
		}
		return res, nil
	}

	var cal *store.CalendarAccess
	if err != nil {
		cal, err = h.loadCalendarByName(ctx, user, segments[0])
		if err != nil {
			if errors.Is(err, errAmbiguousCalendar) {
				return nil, errAmbiguousCalendar
			}
			return nil, http.ErrNotSupported
		}
	} else {
		cal, err = h.loadDiscoverableCalendar(ctx, user, calID)
		if err != nil {
			return nil, err
		}
	}

	if len(segments) == 2 {
		resourceName := strings.TrimSuffix(segments[1], path.Ext(segments[1]))
		if resourceName == "" {
			return nil, http.ErrNotSupported
		}
		href := ensureCollectionHref(path.Join("/dav/calendars", fmt.Sprint(cal.ID)))
		resourceHref := calendarObjectHref(href, resourceName)
		allowed, err := h.canReadCalendarObject(ctx, user, cal, resourceName)
		if err != nil {
			return nil, err
		}
		if !allowed {
			return []response{{Href: resourceHref, Status: httpStatusNotFound}}, nil
		}
		event, err := h.store.Events.GetByResourceName(ctx, cal.ID, resourceName)
		if err != nil {
			return nil, err
		}
		if event == nil {
			return []response{{Href: resourceHref, Status: httpStatusNotFound}}, nil
		}
		return []response{resourceResponse(resourceHref, etagProp(event.ETag, event.RawICAL, true))}, nil
	}
	readable, err := h.canReadCalendarCollection(ctx, user, cal)
	if err != nil {
		return nil, err
	}
	if !readable {
		return nil, store.ErrNotFound
	}

	href := ensureCollectionHref(path.Join("/dav/calendars", fmt.Sprint(cal.ID)))
	ctag := strconv.FormatInt(cal.CTag, 10)
	syncToken := buildSyncToken("cal", cal.ID, cal.UpdatedAt)
	principalHref := h.principalURL(user)
	res := []response{calendarCollectionResponseWithPrivileges(href, cal.Name, cal.Calendar, principalHref, syncToken, ctag, cal.EffectivePrivileges())}
	if depthIncludesChildren(depth) {
		res, err = h.appendCalendarPropfindPages(ctx, user, cal, ensureCollectionHref(href), res)
		if err != nil {
			return nil, err
		}
	}
	return res, nil
}

func (h *DavServer) canReadCalendarCollection(ctx context.Context, user *store.User, cal *store.CalendarAccess) (bool, error) {
	if cal == nil {
		return false, nil
	}
	allowed, denied, err := h.calendarPrivilegeDecision(ctx, user, &cal.Calendar, calendarCollectionResourcePath(cal.ID), "read")
	if err != nil || allowed || denied {
		return allowed, err
	}
	return cal.EffectivePrivileges().Allows("read"), nil
}

func (h *DavServer) loadDiscoverableCalendar(ctx context.Context, user *store.User, calendarID int64) (*store.CalendarAccess, error) {
	cal, err := h.loadCalendar(ctx, user, calendarID)
	if err == nil {
		return cal, nil
	}
	if !errors.Is(err, store.ErrNotFound) && !errors.Is(err, errForbidden) {
		return nil, err
	}

	calendars, err := h.accessibleCalendars(ctx, user)
	if err != nil {
		return nil, err
	}
	for _, candidate := range calendars {
		if candidate.ID == calendarID {
			copy := candidate
			return &copy, nil
		}
	}
	return nil, store.ErrNotFound
}

func (h *DavServer) addressBookResponses(ctx context.Context, cleanPath, depth string, user *store.User) ([]response, error) {
	relPath := strings.Trim(strings.TrimPrefix(cleanPath, "/dav/addressbooks"), "/")
	if relPath == "" {
		base := ensureCollectionHref("/dav/addressbooks")
		res := []response{collectionResponse(base, "Address Books")}
		if depthIncludesChildren(depth) {
			books, err := h.accessibleAddressBooks(ctx, user)
			if err != nil {
				return nil, err
			}
			principalHref := h.principalURL(user)
			for i := range books {
				if h.multistatusBuildComplete(res) {
					break
				}
				b := &books[i]
				readable, err := h.canReadAddressBookCollection(ctx, user, b)
				if err != nil {
					return nil, err
				}
				if !readable {
					continue
				}
				href := ensureCollectionHref(path.Join("/dav/addressbooks", fmt.Sprint(b.ID)))
				ctag := strconv.FormatInt(b.CTag, 10)
				syncToken := buildSyncToken("card", b.ID, b.UpdatedAt)
				res = h.appendMultistatusResponses(res, []response{addressBookCollectionResponse(href, b.Name, b.Description, principalHref, syncToken, ctag)})
				if depth == "infinity" && !h.multistatusBuildComplete(res) {
					res, err = h.appendAddressBookPropfindPages(ctx, user, b, href, res)
					if err != nil {
						return nil, err
					}
				}
			}
		}
		return res, nil
	}

	segments := strings.Split(relPath, "/")
	if len(segments) > 2 {
		return nil, http.ErrNotSupported
	}
	bookID, ok, err := h.resolveAddressBookID(ctx, user, strings.TrimSpace(segments[0]))
	if err != nil {
		if errors.Is(err, errAmbiguousAddressBook) {
			return nil, errAmbiguousAddressBook
		}
		return nil, http.ErrNotSupported
	}
	if !ok {
		return nil, http.ErrNotSupported
	}
	book, err := h.loadAddressBookWithPrivilege(ctx, user, bookID, cleanPath, "read")
	if err != nil {
		return nil, err
	}
	collectionHref := ensureCollectionHref(cleanPath)
	if len(segments) == 2 {
		collectionHref = ensureCollectionHref(strings.TrimSuffix(cleanPath, "/"+segments[1]))
	}
	if len(segments) == 2 {
		resourceName := strings.TrimSuffix(segments[1], path.Ext(segments[1]))
		if resourceName == "" {
			return nil, http.ErrNotSupported
		}
		contact, err := h.store.Contacts.GetByResourceName(ctx, book.ID, resourceName)
		if err != nil {
			return nil, err
		}
		href := addressObjectHref(collectionHref, resourceName)
		if contact == nil {
			return []response{{Href: href, Status: httpStatusNotFound}}, nil
		}
		return []response{resourceResponse(href, addressBookResourcePropstat(contact.ETag, contact.RawVCard))}, nil
	}
	href := collectionHref
	ctag := strconv.FormatInt(book.CTag, 10)
	syncToken := buildSyncToken("card", book.ID, book.UpdatedAt)
	principalHref := h.principalURL(user)
	res := []response{addressBookCollectionResponse(href, book.Name, book.Description, principalHref, syncToken, ctag)}
	if depthIncludesChildren(depth) {
		res, err = h.appendAddressBookPropfindPages(ctx, user, book, ensureCollectionHref(href), res)
		if err != nil {
			return nil, err
		}
	}
	return res, nil
}

func (h *DavServer) canReadAddressBookCollection(ctx context.Context, user *store.User, book *store.AddressBook) (bool, error) {
	if book == nil {
		return false, nil
	}
	allowed, denied, err := h.addressBookPrivilegeDecision(ctx, user, book, addressBookCollectionResourcePath(book.ID), "read")
	if err != nil || allowed || denied {
		return allowed, err
	}
	return h == nil || h.store == nil || h.store.ACLEntries == nil, nil
}

func (h *DavServer) appendCalendarPropfindPages(ctx context.Context, user *store.User, cal *store.CalendarAccess, baseHref string, responses []response) ([]response, error) {
	afterID := int64(0)
	for !h.multistatusBuildComplete(responses) {
		events, err := h.store.Events.ListForCalendarPageAfter(ctx, cal.ID, afterID, multistatusPageSize, store.EventFilter{})
		if err != nil {
			return nil, err
		}
		if len(events) == 0 {
			break
		}
		visible, err := h.filterReadableCalendarEvents(ctx, user, cal, events)
		if err != nil {
			return nil, err
		}
		responses = h.appendMultistatusResponses(responses, calendarResourceResponsesFilteredLimit(baseHref, visible, calendarDataProjection{}, h.multistatusBuildLimit()-len(responses)))
		lastID := events[len(events)-1].ID
		if lastID <= afterID || len(events) < multistatusPageSize {
			break
		}
		afterID = lastID
	}
	return responses, nil
}

func (h *DavServer) appendAddressBookPropfindPages(ctx context.Context, user *store.User, book *store.AddressBook, baseHref string, responses []response) ([]response, error) {
	afterID := int64(0)
	for !h.multistatusBuildComplete(responses) {
		contacts, err := h.store.Contacts.ListForBookPageAfter(ctx, book.ID, afterID, multistatusPageSize)
		if err != nil {
			return nil, err
		}
		if len(contacts) == 0 {
			break
		}
		visible, err := h.filterReadableAddressBookContacts(ctx, user, book, contacts)
		if err != nil {
			return nil, err
		}
		responses = h.appendMultistatusResponses(responses, addressBookResourceResponsesLimit(baseHref, visible, h.multistatusBuildLimit()-len(responses)))
		lastID := contacts[len(contacts)-1].ID
		if lastID <= afterID || len(contacts) < multistatusPageSize {
			break
		}
		afterID = lastID
	}
	return responses, nil
}

func (h *DavServer) principalURL(user *store.User) string {
	return fmt.Sprintf("/dav/principals/%d/", user.ID)
}

func (h *DavServer) principalResponses(cleanPath, depth string, user *store.User) ([]response, error) {
	relPath := strings.Trim(strings.TrimPrefix(cleanPath, "/dav/principals"), "/")
	principalHref := ensureCollectionHref(h.principalURL(user))

	// Only the authenticated user's principal is exposed.
	if relPath == "" {
		collection := collectionResponse(ensureCollectionHref("/dav/principals"), "Principals")
		collection.Propstat[0].Prop.SupportedReportSet = &supportedReportSet{Reports: aclPrincipalCollectionSupportedReports()}
		res := []response{collection}
		if depthIncludesChildren(depth) {
			res = append(res, principalResponse(principalHref, user))
		}
		return res, nil
	}

	if relPath != fmt.Sprint(user.ID) && relPath != fmt.Sprint(user.ID)+"/" {
		return nil, store.ErrNotFound
	}

	return []response{principalResponse(principalHref, user)}, nil
}

// principalDisplayName returns a guaranteed non-empty display name for a
// principal. RFC 3744 §4 requires DAV:displayname on a principal to be a
// non-empty human-readable name, so fall back to the principal identifier when
// the user has neither a full name nor a login email.
func principalDisplayName(user *store.User) string {
	if name := strings.TrimSpace(user.DisplayName()); name != "" {
		return name
	}
	return fmt.Sprintf("Principal %d", user.ID)
}

func principalResponse(href string, user *store.User) response {
	p := prop{
		DisplayName:             stringPtr(principalDisplayName(user)),
		ResourceType:            &resourceType{Principal: &struct{}{}},
		PrincipalURL:            &hrefProp{Href: href},
		CurrentUserPrincipal:    &hrefProp{Href: href},
		CurrentUserPrincipalURL: &hrefProp{Href: href},
		CalendarHomeSet:         &hrefListProp{Href: []string{"/dav/calendars/"}},
		AddressbookHomeSet:      &hrefListProp{Href: []string{"/dav/addressbooks/"}},
		SupportedReportSet:      &supportedReportSet{Reports: aclResourceSupportedReports()},
	}
	return response{Href: href, Propstat: []propstat{{Prop: p, Status: httpStatusOK}}}
}

func rootCollectionResponse(href string, principalHref string) response {
	p := prop{
		DisplayName:             stringPtr("CalCard DAV"),
		ResourceType:            &resourceType{Collection: &struct{}{}},
		CurrentUserPrincipal:    &hrefProp{Href: principalHref},
		CurrentUserPrincipalURL: &hrefProp{Href: principalHref},
		SupportedReportSet:      combinedSupportedReports(),
	}
	return response{Href: href, Propstat: []propstat{{Prop: p, Status: httpStatusOK}}}
}
