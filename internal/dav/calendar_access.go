package dav

import (
	"context"
	"errors"
	"fmt"
	"path"
	"strconv"
	"strings"

	"github.com/jw6ventures/calcard/internal/store"
)

func calendarCollectionPath(cleanPath string) string {
	cleanPath = normalizeDAVHref(cleanPath)
	if !strings.HasPrefix(cleanPath, "/dav/calendars/") {
		return cleanPath
	}
	trimmed := strings.TrimPrefix(cleanPath, "/dav/calendars/")
	parts := strings.Split(trimmed, "/")
	if len(parts) == 0 || strings.TrimSpace(parts[0]) == "" {
		return "/dav/calendars"
	}
	return path.Join("/dav/calendars", parts[0])
}

func (h *Handler) getCalendar(ctx context.Context, id int64) (*store.Calendar, error) {
	if h == nil || h.store == nil || h.store.Calendars == nil {
		return nil, store.ErrNotFound
	}
	cal, err := h.store.Calendars.GetByID(ctx, id)
	if err != nil {
		return nil, err
	}
	if cal == nil {
		return nil, store.ErrNotFound
	}
	return cal, nil
}

func (h *Handler) legacyLoadCalendarByName(ctx context.Context, user *store.User, name string) (*store.CalendarAccess, error) {
	normalizedName := strings.ToLower(name)
	accessible, err := h.store.Calendars.ListAccessible(ctx, user.ID)
	if err != nil {
		return nil, err
	}
	var match *store.CalendarAccess
	for _, c := range accessible {
		if (c.Slug != nil && *c.Slug == normalizedName) || c.Name == name {
			if match != nil {
				return nil, errAmbiguousCalendar
			}
			copy := c
			match = &copy
		}
	}
	if match != nil {
		return match, nil
	}

	owned, err := h.store.Calendars.ListByUser(ctx, user.ID)
	if err != nil {
		return nil, err
	}
	var ownedMatch *store.CalendarAccess
	for _, c := range owned {
		if (c.Slug != nil && *c.Slug == normalizedName) || c.Name == name {
			if ownedMatch != nil {
				return nil, errAmbiguousCalendar
			}
			cal := store.CalendarAccess{Calendar: c, Editor: true, Privileges: store.FullCalendarPrivileges()}
			ownedMatch = &cal
		}
	}
	if ownedMatch != nil {
		return ownedMatch, nil
	}

	return nil, store.ErrNotFound
}

func (h *Handler) accessibleCalendars(ctx context.Context, user *store.User) ([]store.CalendarAccess, error) {
	if h == nil || h.store == nil || h.store.Calendars == nil || user == nil {
		return nil, nil
	}

	legacy, err := h.store.Calendars.ListAccessible(ctx, user.ID)
	if err != nil {
		return nil, err
	}

	seen := make(map[int64]struct{}, len(legacy))
	result := make([]store.CalendarAccess, 0, len(legacy))
	for _, cal := range legacy {
		effective, err := h.loadCalendar(ctx, user, cal.ID)
		if err != nil {
			if err == store.ErrNotFound || errors.Is(err, errForbidden) {
				if cal.PrivilegesResolved && !cal.Privileges.HasAny() {
					result = append(result, cal)
					seen[cal.ID] = struct{}{}
				}
				continue
			}
			return nil, err
		}
		effective.OwnerEmail = cal.OwnerEmail
		result = append(result, *effective)
		seen[effective.ID] = struct{}{}
	}

	if h.store.ACLEntries == nil {
		return result, nil
	}

	for _, principal := range []string{"DAV:all", "DAV:authenticated", h.principalURL(user)} {
		entries, err := h.store.ACLEntries.ListByPrincipal(ctx, principal)
		if err != nil {
			return nil, err
		}
		for _, entry := range entries {
			collectionPath := calendarCollectionPath(entry.ResourcePath)
			if collectionPath == "/dav/calendars" || !strings.HasPrefix(collectionPath, "/dav/calendars/") {
				continue
			}

			segment := strings.Trim(strings.TrimPrefix(collectionPath, "/dav/calendars/"), "/")
			if segment == "" || strings.Contains(segment, "/") {
				continue
			}

			calID, err := strconv.ParseInt(segment, 10, 64)
			if err != nil {
				continue
			}
			if _, ok := seen[calID]; ok {
				continue
			}

			effective, err := h.loadCalendar(ctx, user, calID)
			if err != nil {
				if err == store.ErrNotFound || errors.Is(err, errForbidden) {
					if normalizeDAVHref(entry.ResourcePath) != collectionPath {
						discoverable, err := h.hasDiscoverableCalendarObjectGrant(ctx, user, entry.ResourcePath)
						if err != nil {
							return nil, err
						}
						if !discoverable {
							continue
						}
						cal, err := h.getCalendar(ctx, calID)
						if err == nil {
							result = append(result, store.CalendarAccess{
								Calendar:           *cal,
								Shared:             user == nil || cal.UserID != user.ID,
								PrivilegesResolved: true,
							})
							seen[calID] = struct{}{}
						} else if err != store.ErrNotFound {
							return nil, err
						}
					}
					continue
				}
				return nil, err
			}
			result = append(result, *effective)
			seen[effective.ID] = struct{}{}
		}
	}

	return result, nil
}

func (h *Handler) hasDiscoverableCalendarObjectGrant(ctx context.Context, user *store.User, resourcePath string) (bool, error) {
	entries, err := h.aclEntriesForResource(ctx, resourcePath)
	if err != nil || len(entries) == 0 {
		return false, err
	}

	applicablePrincipals := applicableACLPrincipals(user)
	for _, privilege := range []string{"read", "read-free-busy", "write", "write-content", "write-properties", "bind", "unbind"} {
		granted, applicable := aclDecisionForPrivilege(entries, applicablePrincipals, privilege)
		if applicable && granted {
			return true, nil
		}
	}
	return false, nil
}

func (h *Handler) calendarPrivilegeDecision(ctx context.Context, user *store.User, cal *store.Calendar, cleanPath, privilege string) (bool, bool, error) {
	if cal == nil {
		return false, false, nil
	}
	if canonicalPath, err := h.canonicalDAVPath(ctx, user, cleanPath); err == nil && canonicalPath != "" {
		cleanPath = canonicalPath
	} else if err != nil {
		return false, false, err
	}
	if user != nil && cal.UserID == user.ID {
		return true, false, nil
	}

	hasApplicable, err := h.aclHasApplicablePrincipal(ctx, user, cleanPath)
	if err != nil {
		return false, false, err
	}
	if granted, applicable, err := h.aclDecision(ctx, user, cleanPath, privilege); err != nil {
		return false, false, err
	} else if applicable {
		return granted, !granted, nil
	}

	collectionPath := calendarCollectionPath(cleanPath)
	if collectionPath != cleanPath {
		collectionApplicable, err := h.aclHasApplicablePrincipal(ctx, user, collectionPath)
		if err != nil {
			return false, false, err
		}
		if granted, applicable, err := h.aclDecision(ctx, user, collectionPath, privilege); err != nil {
			return false, false, err
		} else if applicable {
			return granted, !granted, nil
		}
		return false, hasApplicable || collectionApplicable, nil
	}

	return false, hasApplicable, nil
}

func (h *Handler) requireCalendarPrivilege(ctx context.Context, user *store.User, cal *store.Calendar, cleanPath, privilege string) error {
	allowed, denied, err := h.calendarPrivilegeDecision(ctx, user, cal, cleanPath, privilege)
	if err != nil {
		return err
	}
	if allowed {
		return nil
	}
	if denied {
		return errForbidden
	}
	return store.ErrNotFound
}

func (h *Handler) calendarAccessForPath(ctx context.Context, user *store.User, cal *store.Calendar, cleanPath string) (*store.CalendarAccess, error) {
	privileges := store.CalendarPrivileges{}
	for _, candidate := range []struct {
		name string
		set  func()
	}{
		{name: "read", set: func() { privileges.Read = true }},
		{name: "read-free-busy", set: func() { privileges.ReadFreeBusy = true }},
		{name: "write", set: func() { privileges.Write = true }},
		{name: "write-content", set: func() { privileges.WriteContent = true }},
		{name: "write-properties", set: func() { privileges.WriteProperties = true }},
		{name: "bind", set: func() { privileges.Bind = true }},
		{name: "unbind", set: func() { privileges.Unbind = true }},
	} {
		allowed, denied, err := h.calendarPrivilegeDecision(ctx, user, cal, cleanPath, candidate.name)
		if err != nil {
			return nil, err
		}
		if allowed {
			candidate.set()
		}
		if denied {
			continue
		}
	}
	privileges = privileges.Normalized()

	return &store.CalendarAccess{
		Calendar:           *cal,
		Shared:             user == nil || cal.UserID != user.ID,
		Editor:             privileges.AllowsEventEditing(),
		Privileges:         privileges,
		PrivilegesResolved: true,
	}, nil
}

func (h *Handler) aclHasApplicablePrincipal(ctx context.Context, user *store.User, resourcePath string) (bool, error) {
	if h == nil || h.store == nil || h.store.ACLEntries == nil {
		return false, nil
	}

	entries, err := h.aclEntriesForResource(ctx, resourcePath)
	if err != nil || len(entries) == 0 {
		return false, err
	}

	applicablePrincipals := applicableACLPrincipals(user)
	for _, entry := range entries {
		if _, ok := applicablePrincipals[normalizeACLPrincipalHref(entry.PrincipalHref)]; ok {
			return true, nil
		}
	}
	return false, nil
}

func (h *Handler) loadCalendarWithPrivilege(ctx context.Context, user *store.User, id int64, cleanPath, privilege string) (*store.CalendarAccess, error) {
	var legacy *store.CalendarAccess
	if user != nil && h != nil && h.store != nil && h.store.Calendars != nil {
		legacyAccess, legacyErr := h.store.Calendars.GetAccessible(ctx, id, user.ID)
		if legacyErr != nil {
			return nil, legacyErr
		}
		legacy = legacyAccess
	}
	cal, err := h.getCalendar(ctx, id)
	if err != nil {
		if err != store.ErrNotFound || legacy == nil {
			return nil, err
		}
		cal = &legacy.Calendar
	}
	if err := h.requireCalendarPrivilege(ctx, user, cal, cleanPath, privilege); err != nil {
		if !errors.Is(err, store.ErrNotFound) || legacy == nil || !legacy.EffectivePrivileges().Allows(privilege) {
			return nil, err
		}
	}
	access, err := h.calendarAccessForPath(ctx, user, cal, cleanPath)
	if err != nil {
		return nil, err
	}
	mergeCalendarAccessWithLegacy(access, legacy)
	return access, nil
}

func (h *Handler) canAccessCalendarObject(ctx context.Context, user *store.User, cal *store.CalendarAccess, resourceName, privilege string) (bool, error) {
	if cal == nil {
		return false, nil
	}
	resourcePath := path.Join("/dav/calendars", fmt.Sprint(cal.ID), resourceName)
	if err := h.requireCalendarPrivilege(ctx, user, &cal.Calendar, resourcePath, privilege); err != nil {
		if err == store.ErrNotFound || errors.Is(err, errForbidden) {
			return false, nil
		}
		return false, err
	}
	return true, nil
}

func (h *Handler) canReadCalendarObject(ctx context.Context, user *store.User, cal *store.CalendarAccess, resourceName string) (bool, error) {
	return h.canAccessCalendarObject(ctx, user, cal, resourceName, "read")
}

func (h *Handler) prefetchCalendarACLEntries(ctx context.Context, user *store.User, calendarID int64, events []store.Event) (map[string][]store.ACLEntry, error) {
	if h == nil || h.store == nil || h.store.ACLEntries == nil || user == nil {
		return nil, nil
	}

	relevantPaths := map[string]struct{}{
		calendarCollectionResourcePath(calendarID): {},
	}
	for _, event := range events {
		for _, resourcePath := range calendarObjectACLPaths(calendarID, eventResourceName(event)) {
			relevantPaths[resourcePath] = struct{}{}
		}
	}

	result := make(map[string][]store.ACLEntry, len(relevantPaths))
	for _, principalHref := range aclPrincipalHrefs(user) {
		entries, err := h.store.ACLEntries.ListByPrincipal(ctx, principalHref)
		if err != nil {
			return nil, err
		}
		for _, entry := range entries {
			resourcePath := normalizeDAVHref(entry.ResourcePath)
			if _, ok := relevantPaths[resourcePath]; !ok {
				continue
			}
			result[resourcePath] = append(result[resourcePath], entry)
		}
	}
	return result, nil
}

func (h *Handler) canAccessCalendarObjectWithEntries(user *store.User, cal *store.CalendarAccess, resourceName, privilege string, entriesByPath map[string][]store.ACLEntry) (bool, error) {
	allowed, denied := calendarPrivilegeDecisionFromEntries(user, cal, resourceName, privilege, entriesByPath)
	if allowed {
		return true, nil
	}
	if denied {
		return false, nil
	}
	return cal != nil && cal.EffectivePrivileges().Allows(privilege), nil
}

func (h *Handler) filterCalendarEventsByPrivilege(ctx context.Context, user *store.User, cal *store.CalendarAccess, events []store.Event, privilege string) ([]store.Event, error) {
	prefetchedACLEntries, err := h.prefetchCalendarACLEntries(ctx, user, cal.ID, events)
	if err != nil {
		return nil, err
	}
	visible := make([]store.Event, 0, len(events))
	for _, event := range events {
		allowed, err := h.canAccessCalendarObjectWithEntries(user, cal, eventResourceName(event), privilege, prefetchedACLEntries)
		if err != nil {
			return nil, err
		}
		if allowed {
			visible = append(visible, event)
		}
	}
	return visible, nil
}

func (h *Handler) filterReadableCalendarEvents(ctx context.Context, user *store.User, cal *store.CalendarAccess, events []store.Event) ([]store.Event, error) {
	return h.filterCalendarEventsByPrivilege(ctx, user, cal, events, "read")
}

func (h *Handler) loadCalendarWithAnyPrivilege(ctx context.Context, user *store.User, id int64, cleanPath string) (*store.CalendarAccess, error) {
	var legacy *store.CalendarAccess
	if user != nil && h != nil && h.store != nil && h.store.Calendars != nil {
		legacyAccess, legacyErr := h.store.Calendars.GetAccessible(ctx, id, user.ID)
		if legacyErr != nil {
			return nil, legacyErr
		}
		legacy = legacyAccess
	}

	cal, err := h.getCalendar(ctx, id)
	if err != nil {
		if err != store.ErrNotFound || legacy == nil {
			return nil, err
		}
		cal = &legacy.Calendar
	}

	if err := h.requireAnyCalendarPrivilege(ctx, user, cal, cleanPath); err != nil {
		if !errors.Is(err, store.ErrNotFound) || legacy == nil || !legacy.EffectivePrivileges().HasAny() {
			return nil, err
		}
	}

	access, err := h.calendarAccessForPath(ctx, user, cal, cleanPath)
	if err != nil {
		return nil, err
	}
	mergeCalendarAccessWithLegacy(access, legacy)
	if !access.Privileges.HasAny() {
		return nil, store.ErrNotFound
	}
	return access, nil
}

func (h *Handler) requireAnyCalendarPrivilege(ctx context.Context, user *store.User, cal *store.Calendar, cleanPath string) error {
	sawForbidden := false
	for _, privilege := range []string{"read", "read-free-busy", "write", "write-content", "write-properties", "bind", "unbind"} {
		err := h.requireCalendarPrivilege(ctx, user, cal, cleanPath, privilege)
		switch {
		case err == nil:
			return nil
		case err == store.ErrNotFound:
			continue
		case errors.Is(err, errForbidden):
			sawForbidden = true
		default:
			return err
		}
	}
	if sawForbidden {
		return errForbidden
	}
	return store.ErrNotFound
}

func mergeCalendarAccessWithLegacy(access, legacy *store.CalendarAccess) {
	if access == nil {
		return
	}
	if legacy != nil {
		if legacy.Shared {
			access.Shared = true
		}
		if access.OwnerEmail == "" {
			access.OwnerEmail = legacy.OwnerEmail
		}
		if !access.Privileges.HasAny() {
			access.Privileges = legacy.EffectivePrivileges()
			access.PrivilegesResolved = legacy.PrivilegesResolved || legacy.Privileges.HasAny()
		}
	}
	access.Privileges = access.Privileges.Normalized()
	access.Editor = access.Privileges.AllowsEventEditing()
}

func calendarCollectionResourcePath(calendarID int64) string {
	return path.Join("/dav/calendars", fmt.Sprint(calendarID))
}

func calendarObjectACLPaths(calendarID int64, resourceName string) []string {
	resourceName = strings.TrimSpace(resourceName)
	if resourceName == "" {
		return nil
	}
	base := path.Join("/dav/calendars", fmt.Sprint(calendarID), resourceName)
	paths := []string{base}
	if strings.EqualFold(path.Ext(resourceName), ".ics") {
		paths = append(paths, strings.TrimSuffix(base, path.Ext(resourceName)))
	} else {
		paths = append(paths, base+".ics")
	}
	return paths
}

func aclPrincipalHrefs(user *store.User) []string {
	principals := []string{"DAV:all"}
	if user != nil {
		principals = append(principals, "DAV:authenticated", fmt.Sprintf("/dav/principals/%d/", user.ID))
	}
	return principals
}

func calendarPrivilegeDecisionFromEntries(user *store.User, cal *store.CalendarAccess, resourceName, privilege string, entriesByPath map[string][]store.ACLEntry) (bool, bool) {
	if cal == nil || user == nil {
		return false, false
	}
	if cal.UserID == user.ID {
		return true, false
	}

	resourcePaths := calendarObjectACLPaths(cal.ID, resourceName)
	if granted, applicable := aclDecisionForResourcePaths(entriesByPath, user, resourcePaths, privilege); applicable {
		return granted, !granted
	}

	collectionPaths := []string{calendarCollectionResourcePath(cal.ID)}
	if granted, applicable := aclDecisionForResourcePaths(entriesByPath, user, collectionPaths, privilege); applicable {
		return granted, !granted
	}

	return false, aclHasApplicablePrincipalForPaths(entriesByPath, user, resourcePaths) || aclHasApplicablePrincipalForPaths(entriesByPath, user, collectionPaths)
}

func aclDecisionForResourcePaths(entriesByPath map[string][]store.ACLEntry, user *store.User, resourcePaths []string, privilege string) (bool, bool) {
	if len(entriesByPath) == 0 {
		return false, false
	}
	entries := make([]store.ACLEntry, 0, len(resourcePaths))
	for _, resourcePath := range resourcePaths {
		entries = append(entries, entriesByPath[normalizeDAVHref(resourcePath)]...)
	}
	return aclDecisionForPrivilege(entries, applicableACLPrincipals(user), privilege)
}

func aclHasApplicablePrincipalForPaths(entriesByPath map[string][]store.ACLEntry, user *store.User, resourcePaths []string) bool {
	if len(entriesByPath) == 0 {
		return false
	}
	applicablePrincipals := applicableACLPrincipals(user)
	for _, resourcePath := range resourcePaths {
		for _, entry := range entriesByPath[normalizeDAVHref(resourcePath)] {
			if _, ok := applicablePrincipals[normalizeACLPrincipalHref(entry.PrincipalHref)]; ok {
				return true
			}
		}
	}
	return false
}
