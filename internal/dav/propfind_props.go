package dav

import (
	"context"
	"strconv"
	"strings"
	"time"

	"github.com/jw6ventures/calcard/internal/auth"
	"github.com/jw6ventures/calcard/internal/store"
)

func (h *Handler) decoratePropfindResponses(ctx context.Context, user *store.User, responses []response) error {
	for i := range responses {
		if len(responses[i].Propstat) == 0 {
			continue
		}
		resourcePath := normalizeDAVHref(responses[i].Href)
		for j := range responses[i].Propstat {
			if responses[i].Propstat[j].Status != httpStatusOK {
				continue
			}
			if err := h.decorateDAVProp(ctx, user, resourcePath, &responses[i].Propstat[j].Prop); err != nil {
				return err
			}
		}
	}
	return nil
}

func (h *Handler) decorateDAVProp(ctx context.Context, user *store.User, resourcePath string, p *prop) error {
	if p == nil || resourcePath == "" || !strings.HasPrefix(resourcePath, "/dav") {
		return nil
	}

	p.SupportedLock = defaultSupportedLock()
	p.SupportedPrivilegeSet = defaultSupportedPrivilegeSet()
	p.PrincipalCollectionSet = &hrefListProp{Href: []string{"/dav/principals/"}}

	lockDiscovery, err := h.lockDiscoveryForPath(ctx, resourcePath)
	if err != nil {
		return err
	}
	p.LockDiscovery = lockDiscovery

	if h != nil && h.store != nil && h.store.ACLEntries != nil {
		entries, err := h.aclEntriesForResource(ctx, resourcePath)
		if err != nil {
			return err
		}
		p.ACL = buildACLPropFromEntries(entries)
	}

	if user != nil && p.CurrentUserPrincipal == nil {
		principalHref := h.principalURL(user)
		p.CurrentUserPrincipal = &expandableHrefProp{Href: principalHref}
		p.CurrentUserPrincipalURL = &hrefProp{Href: principalHref}
	}
	if user != nil && p.CurrentUserPrivilegeSet == nil {
		p.CurrentUserPrivilegeSet = h.currentUserPrivilegeSetForPath(ctx, user, resourcePath)
	}

	return nil
}

func (h *Handler) currentUserPrivilegeSetForPath(ctx context.Context, user *store.User, resourcePath string) *currentUserPrivilegeSet {
	if user == nil {
		return nil
	}

	cleanPath := normalizeDAVHref(resourcePath)
	if strings.HasPrefix(cleanPath, "/dav/calendars/") {
		segment := singleCollectionSegment(cleanPath, "/dav/calendars/")
		if segment == "" {
			if parsedSegment, _, ok := parseCalendarResourceSegments(cleanPath); ok {
				segment = parsedSegment
			}
		}
		if segment == "" {
			return nil
		}

		calendarID, ok, err := h.resolveCalendarID(ctx, user, segment)
		if err != nil || !ok {
			return nil
		}
		cal, err := h.getCalendar(ctx, calendarID)
		if err != nil || cal == nil {
			return nil
		}

		privileges := make([]privilege, 0, 7)
		if err := h.requireCalendarPrivilege(ctx, user, cal, cleanPath, "read"); err == nil {
			privileges = append(privileges, privilege{Read: &readPrivilege{}})
		}
		if err := h.requireCalendarPrivilege(ctx, user, cal, cleanPath, "read-free-busy"); err == nil {
			privileges = append(privileges, privilege{ReadFreeBusy: &struct{}{}})
		}
		for _, name := range []string{"write", "write-content", "write-properties", "bind", "unbind"} {
			if err := h.requireCalendarPrivilege(ctx, user, cal, cleanPath, name); err != nil {
				continue
			}
			switch name {
			case "write":
				privileges = append(privileges, privilege{Write: &struct{}{}})
			case "write-content":
				privileges = append(privileges, privilege{WriteContent: &struct{}{}})
			case "write-properties":
				privileges = append(privileges, privilege{WriteProperties: &struct{}{}})
			case "bind":
				privileges = append(privileges, privilege{Bind: &struct{}{}})
			case "unbind":
				privileges = append(privileges, privilege{Unbind: &struct{}{}})
			}
		}
		if len(privileges) == 0 {
			return nil
		}
		return &currentUserPrivilegeSet{Privileges: privileges}
	}

	if !strings.HasPrefix(cleanPath, "/dav/addressbooks/") {
		return nil
	}

	segment := singleCollectionSegment(cleanPath, "/dav/addressbooks/")
	if segment == "" {
		if parsedSegment, _, ok := parseAddressBookResourceSegments(cleanPath); ok {
			segment = parsedSegment
		}
	}
	if segment == "" {
		return nil
	}

	bookID, ok, err := h.resolveAddressBookID(ctx, user, segment)
	if err != nil || !ok {
		return nil
	}
	book, err := h.getAddressBook(ctx, bookID)
	if err != nil || book == nil {
		return nil
	}

	privileges := make([]privilege, 0, 6)
	for _, name := range []string{"read", "write", "write-content", "write-properties", "bind", "unbind"} {
		if err := h.requireAddressBookPrivilege(ctx, user, book, cleanPath, name); err != nil {
			continue
		}
		switch name {
		case "read":
			privileges = append(privileges, privilege{Read: &readPrivilege{}})
		case "write":
			privileges = append(privileges, privilege{Write: &struct{}{}})
		case "write-content":
			privileges = append(privileges, privilege{WriteContent: &struct{}{}})
		case "write-properties":
			privileges = append(privileges, privilege{WriteProperties: &struct{}{}})
		case "bind":
			privileges = append(privileges, privilege{Bind: &struct{}{}})
		case "unbind":
			privileges = append(privileges, privilege{Unbind: &struct{}{}})
		}
	}
	if len(privileges) == 0 {
		return nil
	}
	return &currentUserPrivilegeSet{Privileges: privileges}
}

func (h *Handler) lockDiscoveryForPath(ctx context.Context, resourcePath string) (*lockDiscoveryProp, error) {
	if h == nil || h.store == nil || h.store.Locks == nil {
		return &lockDiscoveryProp{}, nil
	}
	if user, ok := auth.UserFromContext(ctx); ok {
		if canonicalPath, err := h.canonicalDAVPath(ctx, user, resourcePath); err == nil && canonicalPath != "" {
			resourcePath = canonicalPath
		}
	}

	paths := lockLookupPaths(resourcePath)
	locks, err := h.store.Locks.ListByResources(ctx, paths)
	if err != nil {
		return nil, err
	}

	now := time.Now()
	activeLocks := make([]activeLock, 0, len(locks))
	for i := range locks {
		lock := locks[i]
		if lock.ExpiresAt.Before(now) {
			continue
		}
		lockPath := normalizeDAVResourceIdentity(lock.ResourcePath)
		if lockPath != resourcePath && lock.Depth != "infinity" {
			continue
		}
		activeLocks = append(activeLocks, activeLockFromStoreLock(&lock))
	}

	return &lockDiscoveryProp{ActiveLocks: activeLocks}, nil
}

func (h *Handler) accessibleAddressBooks(ctx context.Context, user *store.User) ([]store.AddressBook, error) {
	owned, err := h.store.AddressBooks.ListByUser(ctx, user.ID)
	if err != nil {
		return nil, err
	}
	if h == nil || h.store == nil || h.store.ACLEntries == nil {
		return owned, nil
	}

	seen := make(map[int64]struct{}, len(owned))
	for _, book := range owned {
		seen[book.ID] = struct{}{}
	}

	principals := []string{"DAV:all", "DAV:authenticated", h.principalURL(user)}
	for _, principal := range principals {
		entries, err := h.store.ACLEntries.ListByPrincipal(ctx, principal)
		if err != nil {
			return nil, err
		}
		for _, entry := range entries {
			collectionPath := addressBookCollectionPath(entry.ResourcePath)
			if collectionPath == "/dav/addressbooks" || !strings.HasPrefix(collectionPath, "/dav/addressbooks/") {
				continue
			}
			granted, err := h.checkACLPrivilege(ctx, user, collectionPath, "read")
			if err != nil {
				return nil, err
			}
			if !granted {
				continue
			}

			segment := strings.Trim(strings.TrimPrefix(collectionPath, "/dav/addressbooks/"), "/")
			if segment == "" || strings.Contains(segment, "/") {
				continue
			}

			bookID, err := strconv.ParseInt(segment, 10, 64)
			if err != nil {
				continue
			}
			if _, ok := seen[bookID]; ok {
				continue
			}
			book, err := h.getAddressBook(ctx, bookID)
			if err != nil {
				if err == store.ErrNotFound {
					continue
				}
				return nil, err
			}
			seen[bookID] = struct{}{}
			owned = append(owned, *book)
		}
	}

	return owned, nil
}

func filterGenericCollectionPropfindResponse(resp response, req *propfindRequest) response {
	if req == nil || req.Prop == nil || len(resp.Propstat) == 0 {
		return resp
	}
	src := resp.Propstat[0].Prop
	var okProp prop
	var okSet bool
	var notFoundProp prop
	var notFoundSet bool
	if req.Prop.DisplayName != nil {
		okProp.DisplayName = src.DisplayName
		okSet = true
	}
	if req.Prop.ResourceType != nil {
		okProp.ResourceType = src.ResourceType
		okSet = true
	}
	if req.Prop.CurrentUserPrincipal != nil {
		okProp.CurrentUserPrincipal = src.CurrentUserPrincipal
		okSet = true
	}
	if req.Prop.CurrentUserPrincipalURL != nil {
		okProp.CurrentUserPrincipalURL = src.CurrentUserPrincipalURL
		okSet = true
	}
	if req.Prop.SupportedReportSet != nil {
		okProp.SupportedReportSet = src.SupportedReportSet
		okSet = true
	}
	if req.Prop.LockDiscovery != nil {
		okProp.LockDiscovery = src.LockDiscovery
		okSet = true
	}
	if req.Prop.SupportedLock != nil {
		okProp.SupportedLock = src.SupportedLock
		okSet = true
	}
	if req.Prop.ACLProp != nil {
		okProp.ACL = src.ACL
		okSet = true
	}
	if req.Prop.SupportedPrivilegeSet != nil {
		okProp.SupportedPrivilegeSet = src.SupportedPrivilegeSet
		okSet = true
	}
	if req.Prop.PrincipalCollectionSet != nil {
		okProp.PrincipalCollectionSet = src.PrincipalCollectionSet
		okSet = true
	}
	if req.Prop.CurrentUserPrivilegeSet != nil {
		okProp.CurrentUserPrivilegeSet = src.CurrentUserPrivilegeSet
		okSet = true
	}
	if req.Prop.GetETag != nil {
		notFoundProp.GetETag = "getetag"
		notFoundSet = true
	}
	if req.Prop.GetContentType != nil {
		notFoundProp.GetContentType = "getcontenttype"
		notFoundSet = true
	}
	if req.Prop.CalendarData != nil {
		notFoundProp.CalendarData = cdataString("calendar-data")
		notFoundSet = true
	}
	if req.Prop.AddressData != nil {
		notFoundProp.AddressData = cdataString("address-data")
		notFoundSet = true
	}
	if req.Prop.CalendarDescription != nil {
		notFoundProp.CalendarDescription = "calendar-description"
		notFoundSet = true
	}
	if req.Prop.CalendarTimezone != nil {
		notFoundProp.CalendarTimezone = stringPtr("calendar-timezone")
		notFoundSet = true
	}
	if req.Prop.AddressBookDesc != nil {
		notFoundProp.AddressBookDesc = "addressbook-description"
		notFoundSet = true
	}
	if req.Prop.SupportedAddressData != nil {
		notFoundProp.SupportedAddressData = &supportedAddressData{}
		notFoundSet = true
	}
	if req.Prop.AddressBookMaxResourceSize != nil {
		notFoundProp.AddressBookMaxResourceSize = "max-resource-size"
		notFoundSet = true
	}
	if req.Prop.SupportedCollationSet != nil {
		notFoundProp.SupportedCollationSet = &supportedCollationSet{}
		notFoundSet = true
	}
	if req.Prop.SyncToken != nil {
		notFoundProp.SyncToken = "sync-token"
		notFoundSet = true
	}
	if req.Prop.CTag != nil {
		notFoundProp.CTag = "getctag"
		notFoundSet = true
	}
	if req.Prop.PrincipalURL != nil {
		notFoundProp.PrincipalURL = &expandableHrefProp{}
		notFoundSet = true
	}
	if req.Prop.CalendarHomeSet != nil {
		notFoundProp.CalendarHomeSet = &hrefListProp{}
		notFoundSet = true
	}
	if req.Prop.AddressbookHomeSet != nil {
		notFoundProp.AddressbookHomeSet = &hrefListProp{}
		notFoundSet = true
	}
	if req.Prop.PrincipalAddress != nil {
		notFoundProp.PrincipalAddress = &hrefProp{}
		notFoundSet = true
	}
	if req.Prop.SupportedCalendarComponentSet != nil {
		notFoundProp.SupportedCalendarComponentSet = &supportedCalendarComponentSet{}
		notFoundSet = true
	}
	if req.Prop.MaxResourceSize != nil {
		notFoundProp.MaxResourceSize = "max-resource-size"
		notFoundSet = true
	}
	if req.Prop.MinDateTime != nil {
		notFoundProp.MinDateTime = "min-date-time"
		notFoundSet = true
	}
	if req.Prop.MaxDateTime != nil {
		notFoundProp.MaxDateTime = "max-date-time"
		notFoundSet = true
	}
	if req.Prop.MaxInstances != nil {
		notFoundProp.MaxInstances = "max-instances"
		notFoundSet = true
	}
	if req.Prop.MaxAttendeesPerInstance != nil {
		notFoundProp.MaxAttendeesPerInstance = "max-attendees-per-instance"
		notFoundSet = true
	}
	if req.Prop.ScheduleCalendarTransp != nil {
		notFoundProp.ScheduleCalendarTransp = &scheduleCalendarTransp{}
		notFoundSet = true
	}
	if req.Prop.SupportedCalendarData != nil {
		notFoundProp.SupportedCalendarData = &supportedCalendarData{}
		notFoundSet = true
	}
	if req.Prop.CalendarServerReadOnly != nil {
		notFoundProp.CalendarServerReadOnly = &struct{}{}
		notFoundSet = true
	}
	if req.Prop.Owner != nil {
		notFoundProp.Owner = &hrefProp{}
		notFoundSet = true
	}
	resp.Propstat = nil
	if okSet {
		resp.Propstat = append(resp.Propstat, propstat{Prop: okProp, Status: httpStatusOK})
	}
	if notFoundSet {
		resp.Propstat = append(resp.Propstat, propstat{Prop: notFoundProp, Status: httpStatusNotFound})
	}
	if len(resp.Propstat) == 0 {
		resp.Propstat = []propstat{{Prop: prop{}, Status: httpStatusOK}}
	}
	return resp
}

func stringPtr(v string) *string {
	return &v
}

func filterCalendarCollectionPropfindResponse(resp response, req *propfindRequest) response {
	if req == nil || req.Prop == nil || len(resp.Propstat) == 0 {
		return resp
	}
	src := resp.Propstat[0].Prop
	var okProp prop
	var okSet bool
	var notFoundProp prop
	var notFoundSet bool
	if req.Prop.DisplayName != nil {
		okProp.DisplayName = src.DisplayName
		okSet = true
	}
	if req.Prop.ResourceType != nil {
		okProp.ResourceType = src.ResourceType
		okSet = true
	}
	if req.Prop.CalendarDescription != nil {
		okProp.CalendarDescription = src.CalendarDescription
		okSet = true
	}
	if req.Prop.CalendarTimezone != nil {
		okProp.CalendarTimezone = src.CalendarTimezone
		okSet = true
	}
	if req.Prop.SyncToken != nil {
		okProp.SyncToken = src.SyncToken
		okSet = true
	}
	if req.Prop.CTag != nil {
		okProp.CTag = src.CTag
		okSet = true
	}
	if req.Prop.CurrentUserPrincipal != nil {
		okProp.CurrentUserPrincipal = src.CurrentUserPrincipal
		okSet = true
	}
	if req.Prop.CurrentUserPrincipalURL != nil {
		okProp.CurrentUserPrincipalURL = src.CurrentUserPrincipalURL
		okSet = true
	}
	if req.Prop.SupportedReportSet != nil {
		okProp.SupportedReportSet = src.SupportedReportSet
		okSet = true
	}
	if req.Prop.SupportedCalendarComponentSet != nil {
		okProp.SupportedCalendarComponentSet = src.SupportedCalendarComponentSet
		okSet = true
	}
	if req.Prop.MaxResourceSize != nil {
		okProp.MaxResourceSize = src.MaxResourceSize
		okSet = true
	}
	if req.Prop.MinDateTime != nil {
		okProp.MinDateTime = src.MinDateTime
		okSet = true
	}
	if req.Prop.MaxDateTime != nil {
		okProp.MaxDateTime = src.MaxDateTime
		okSet = true
	}
	if req.Prop.MaxInstances != nil {
		okProp.MaxInstances = src.MaxInstances
		okSet = true
	}
	if req.Prop.MaxAttendeesPerInstance != nil {
		okProp.MaxAttendeesPerInstance = src.MaxAttendeesPerInstance
		okSet = true
	}
	if req.Prop.ScheduleCalendarTransp != nil {
		okProp.ScheduleCalendarTransp = src.ScheduleCalendarTransp
		okSet = true
	}
	if req.Prop.SupportedCalendarData != nil {
		okProp.SupportedCalendarData = src.SupportedCalendarData
		okSet = true
	}
	if req.Prop.CalendarServerReadOnly != nil {
		okProp.CalendarServerReadOnly = src.CalendarServerReadOnly
		okSet = true
	}
	if req.Prop.CurrentUserPrivilegeSet != nil {
		okProp.CurrentUserPrivilegeSet = src.CurrentUserPrivilegeSet
		okSet = true
	}
	if req.Prop.LockDiscovery != nil {
		okProp.LockDiscovery = src.LockDiscovery
		okSet = true
	}
	if req.Prop.SupportedLock != nil {
		okProp.SupportedLock = src.SupportedLock
		okSet = true
	}
	if req.Prop.ACLProp != nil {
		okProp.ACL = src.ACL
		okSet = true
	}
	if req.Prop.SupportedPrivilegeSet != nil {
		okProp.SupportedPrivilegeSet = src.SupportedPrivilegeSet
		okSet = true
	}
	if req.Prop.PrincipalCollectionSet != nil {
		okProp.PrincipalCollectionSet = src.PrincipalCollectionSet
		okSet = true
	}
	if req.Prop.GetETag != nil {
		notFoundProp.GetETag = "getetag"
		notFoundSet = true
	}
	if req.Prop.GetContentType != nil {
		notFoundProp.GetContentType = "getcontenttype"
		notFoundSet = true
	}
	if req.Prop.CalendarData != nil {
		notFoundProp.CalendarData = cdataString("calendar-data")
		notFoundSet = true
	}
	if req.Prop.AddressData != nil {
		notFoundProp.AddressData = cdataString("address-data")
		notFoundSet = true
	}
	if req.Prop.AddressBookDesc != nil {
		notFoundProp.AddressBookDesc = "addressbook-description"
		notFoundSet = true
	}
	if req.Prop.SupportedAddressData != nil {
		notFoundProp.SupportedAddressData = &supportedAddressData{}
		notFoundSet = true
	}
	if req.Prop.AddressBookMaxResourceSize != nil {
		notFoundProp.AddressBookMaxResourceSize = "max-resource-size"
		notFoundSet = true
	}
	if req.Prop.SupportedCollationSet != nil {
		notFoundProp.SupportedCollationSet = &supportedCollationSet{}
		notFoundSet = true
	}
	if req.Prop.PrincipalURL != nil {
		notFoundProp.PrincipalURL = &expandableHrefProp{}
		notFoundSet = true
	}
	if req.Prop.CalendarHomeSet != nil {
		notFoundProp.CalendarHomeSet = &hrefListProp{}
		notFoundSet = true
	}
	if req.Prop.AddressbookHomeSet != nil {
		notFoundProp.AddressbookHomeSet = &hrefListProp{}
		notFoundSet = true
	}
	if req.Prop.PrincipalAddress != nil {
		notFoundProp.PrincipalAddress = &hrefProp{}
		notFoundSet = true
	}
	if req.Prop.Owner != nil {
		notFoundProp.Owner = &hrefProp{}
		notFoundSet = true
	}
	resp.Propstat = nil
	if okSet {
		resp.Propstat = append(resp.Propstat, propstat{Prop: okProp, Status: httpStatusOK})
	}
	if notFoundSet {
		resp.Propstat = append(resp.Propstat, propstat{Prop: notFoundProp, Status: httpStatusNotFound})
	}
	if len(resp.Propstat) == 0 {
		resp.Propstat = []propstat{{Prop: prop{}, Status: httpStatusOK}}
	}
	return resp
}

func propstatForCalendarObjectPropfind(req *propfindPropQuery, src prop) []propstat {
	if req == nil {
		return []propstat{{Prop: src, Status: httpStatusOK}}
	}
	var okProp prop
	var okSet bool
	var notFoundProp prop
	var notFoundSet bool
	if req.GetETag != nil {
		okProp.GetETag = src.GetETag
		okSet = true
	}
	if req.GetContentType != nil {
		okProp.GetContentType = src.GetContentType
		okSet = true
	}
	if req.CalendarData != nil {
		okProp.CalendarData = src.CalendarData
		okSet = true
	}
	if req.SupportedReportSet != nil {
		okProp.SupportedReportSet = src.SupportedReportSet
		okSet = true
	}
	if req.LockDiscovery != nil {
		okProp.LockDiscovery = src.LockDiscovery
		okSet = true
	}
	if req.SupportedLock != nil {
		okProp.SupportedLock = src.SupportedLock
		okSet = true
	}
	if req.ACLProp != nil {
		okProp.ACL = src.ACL
		okSet = true
	}
	if req.SupportedPrivilegeSet != nil {
		okProp.SupportedPrivilegeSet = src.SupportedPrivilegeSet
		okSet = true
	}
	if req.PrincipalCollectionSet != nil {
		okProp.PrincipalCollectionSet = src.PrincipalCollectionSet
		okSet = true
	}
	if req.DisplayName != nil {
		notFoundProp.DisplayName = "displayname"
		notFoundSet = true
	}

	var stats []propstat
	if okSet {
		stats = append(stats, propstat{Prop: okProp, Status: httpStatusOK})
	}
	if notFoundSet {
		stats = append(stats, propstat{Prop: notFoundProp, Status: httpStatusNotFound})
	}
	if len(stats) == 0 {
		stats = append(stats, propstat{Prop: prop{}, Status: httpStatusOK})
	}
	return stats
}

func filterNonPrincipalPropfindResponse(resp response, req *propfindRequest) response {
	if req == nil || req.Prop == nil || len(resp.Propstat) == 0 {
		return resp
	}
	src := resp.Propstat[0].Prop
	switch {
	case src.ResourceType.Principal != nil:
		return filterPrincipalPropfindResponse(resp, req)
	case src.ResourceType.Calendar != nil:
		return filterCalendarCollectionPropfindResponse(resp, req)
	case src.ResourceType.AddressBook != nil:
		return filterAddressBookCollectionPropfindResponse(resp, req)
	case strings.HasSuffix(normalizeDAVHref(resp.Href), ".ics"):
		resp.Propstat = propstatForCalendarObjectPropfind(req.Prop, src)
		return resp
	case strings.HasSuffix(normalizeDAVHref(resp.Href), ".vcf"):
		return filterAddressObjectPropfindResponse(resp, req)
	default:
		return filterGenericCollectionPropfindResponse(resp, req)
	}
}
