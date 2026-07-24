package dav

import (
	"context"
	"errors"
	"path"
	"strings"

	"github.com/jw6ventures/calcard/internal/acl"
	"github.com/jw6ventures/calcard/internal/store"
)

func calendarCollectionPath(cleanPath string) string {
	return collectionPathForPrefix(cleanPath, calendarPrefix)
}

func (h *DavServer) getCalendar(ctx context.Context, id int64) (*store.Calendar, error) {
	if h == nil || h.store == nil || h.store.Calendars == nil {
		return nil, store.ErrNotFound
	}
	if state := davRequestStateFromContext(ctx); state != nil {
		if calendar, ok := state.calendar(id); ok {
			return calendar, nil
		}
	}
	cal, err := h.store.Calendars.GetByID(ctx, id)
	if err != nil {
		return nil, err
	}
	if cal == nil {
		return nil, store.ErrNotFound
	}
	if state := davRequestStateFromContext(ctx); state != nil {
		state.putCalendar(cal)
	}
	return cal, nil
}

func (h *DavServer) accessibleCalendars(ctx context.Context, user *store.User) ([]store.CalendarAccess, error) {
	if h == nil || h.store == nil || h.store.Calendars == nil || user == nil {
		return nil, nil
	}

	calendars, err := h.store.Calendars.ListAccessible(ctx, user.ID)
	if err != nil {
		return nil, err
	}
	if state := davRequestStateFromContext(ctx); state != nil {
		for i := range calendars {
			state.putCalendar(&calendars[i].Calendar)
		}
	}
	return calendars, nil
}

// calendarPrivilegeNames is every privilege a calendar access decision can
// resolve, in DAV:acl document order.
var calendarPrivilegeNames = []string{"read", "read-free-busy", "write", "write-content", "write-properties", "bind", "unbind"}

// calendarPrivilegeContext carries one resolution of everything a calendar
// privilege decision depends on — canonical path, ownership, and the ACL
// entries for the resource and its collection — so callers evaluating several
// privileges (privilege sets, any-privilege probes) resolve the path and read
// the entries once instead of once per privilege.
type calendarPrivilegeContext struct {
	owner                bool
	principals           map[string]struct{}
	resourceEntries      []store.ACLEntry
	collectionEntries    []store.ACLEntry
	sameCollection       bool
	hasApplicable        bool
	collectionApplicable bool
}

func (h *DavServer) calendarPrivilegeContextFor(ctx context.Context, user *store.User, cal *store.Calendar, cleanPath string) (*calendarPrivilegeContext, error) {
	if cal == nil {
		return nil, nil
	}
	if canonicalPath, err := h.canonicalDAVPath(ctx, user, cleanPath); err == nil && canonicalPath != "" {
		cleanPath = canonicalPath
	} else if err != nil {
		return nil, err
	}
	pc := &calendarPrivilegeContext{principals: acl.ApplicablePrincipals(user)}
	if user != nil && cal.UserID == user.ID {
		pc.owner = true
		return pc, nil
	}

	hasACLStore := h != nil && h.store != nil && h.store.ACLEntries != nil
	if hasACLStore {
		entries, err := h.aclEntriesForResource(ctx, cleanPath)
		if err != nil {
			return nil, err
		}
		pc.resourceEntries = entries
		pc.hasApplicable = acl.HasApplicablePrincipal(entries, pc.principals)
	}

	collectionPath := calendarCollectionPath(cleanPath)
	pc.sameCollection = collectionPath == cleanPath
	if !pc.sameCollection && hasACLStore {
		entries, err := h.aclEntriesForResource(ctx, collectionPath)
		if err != nil {
			return nil, err
		}
		pc.collectionEntries = entries
		pc.collectionApplicable = acl.HasApplicablePrincipal(entries, pc.principals)
	}
	return pc, nil
}

// decide evaluates one privilege against the resolved context. The second
// result reports an applicable deny: object entries win over collection
// entries, and with no applicable decision the caller's fallback sees denied
// only when some entry named an applicable principal without granting.
func (pc *calendarPrivilegeContext) decide(privilege string) (allowed, denied bool) {
	if pc.owner {
		return true, false
	}
	if granted, applicable := acl.DecisionForPrivilege(pc.resourceEntries, pc.principals, privilege); applicable {
		return granted, !granted
	}
	if !pc.sameCollection {
		if granted, applicable := acl.DecisionForPrivilege(pc.collectionEntries, pc.principals, privilege); applicable {
			return granted, !granted
		}
		return false, pc.hasApplicable || pc.collectionApplicable
	}
	return false, pc.hasApplicable
}

func (h *DavServer) calendarPrivilegeDecision(ctx context.Context, user *store.User, cal *store.Calendar, cleanPath, privilege string) (bool, bool, error) {
	pc, err := h.calendarPrivilegeContextFor(ctx, user, cal, cleanPath)
	if err != nil || pc == nil {
		return false, false, err
	}
	allowed, denied := pc.decide(privilege)
	return allowed, denied, nil
}

func (h *DavServer) requireCalendarPrivilege(ctx context.Context, user *store.User, cal *store.Calendar, cleanPath, privilege string) error {
	return requirePrivilegeDecision(h.calendarPrivilegeDecision(ctx, user, cal, cleanPath, privilege))
}

func setCalendarPrivilege(privileges *store.CalendarPrivileges, name string) {
	switch name {
	case "read":
		privileges.Read = true
	case "read-free-busy":
		privileges.ReadFreeBusy = true
	case "write":
		privileges.Write = true
	case "write-content":
		privileges.WriteContent = true
	case "write-properties":
		privileges.WriteProperties = true
	case "bind":
		privileges.Bind = true
	case "unbind":
		privileges.Unbind = true
	}
}

func (h *DavServer) calendarAccessForPath(ctx context.Context, user *store.User, cal *store.Calendar, cleanPath string) (*store.CalendarAccess, error) {
	pc, err := h.calendarPrivilegeContextFor(ctx, user, cal, cleanPath)
	if err != nil {
		return nil, err
	}
	privileges := store.CalendarPrivileges{}
	if pc != nil {
		for _, name := range calendarPrivilegeNames {
			if allowed, _ := pc.decide(name); allowed {
				setCalendarPrivilege(&privileges, name)
			}
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

func (h *DavServer) loadCalendarWithPrivilege(ctx context.Context, user *store.User, id int64, cleanPath, privilege string) (*store.CalendarAccess, error) {
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

func (h *DavServer) canAccessCalendarObject(ctx context.Context, user *store.User, cal *store.CalendarAccess, resourceName, privilege string) (bool, error) {
	if cal == nil {
		return false, nil
	}
	resourcePath := objectResourcePath(calendarPrefix, cal.ID, resourceName)
	if err := h.requireCalendarPrivilege(ctx, user, &cal.Calendar, resourcePath, privilege); err != nil {
		if err == store.ErrNotFound || errors.Is(err, errForbidden) {
			return false, nil
		}
		return false, err
	}
	return true, nil
}

func (h *DavServer) canReadCalendarObject(ctx context.Context, user *store.User, cal *store.CalendarAccess, resourceName string) (bool, error) {
	return h.canAccessCalendarObject(ctx, user, cal, resourceName, "read")
}

func (h *DavServer) prefetchCalendarACLEntries(ctx context.Context, user *store.User, calendarID int64, events []store.Event) (map[string][]store.ACLEntry, error) {
	collectionPath := calendarCollectionResourcePath(calendarID)
	relevantPaths := make([]string, 0, 1+2*len(events))
	relevantPaths = append(relevantPaths, collectionPath)
	for _, event := range events {
		relevantPaths = appendObjectACLPaths(relevantPaths, collectionPath, eventResourceName(event), ".ics")
	}
	return h.prefetchACLEntries(ctx, user, relevantPaths)
}

func (h *DavServer) filterCalendarEventsByPrivilege(ctx context.Context, user *store.User, cal *store.CalendarAccess, events []store.Event, privilege string) ([]store.Event, error) {
	prefetchedACLEntries, err := h.prefetchCalendarACLEntries(ctx, user, cal.ID, events)
	if err != nil {
		return nil, err
	}
	decider := newBatchedObjectACLDecider(user, cal.UserID, calendarCollectionResourcePath(cal.ID), prefetchedACLEntries)
	visible := make([]store.Event, 0, len(events))
	for _, event := range events {
		allowed, denied := calendarPrivilegeDecisionWithDecider(cal, eventResourceName(event), privilege, decider)
		if !allowed && !denied {
			allowed = cal.EffectivePrivileges().Allows(privilege)
		}
		if allowed {
			visible = append(visible, event)
		}
	}
	return visible, nil
}

func (h *DavServer) filterReadableCalendarEvents(ctx context.Context, user *store.User, cal *store.CalendarAccess, events []store.Event) ([]store.Event, error) {
	return h.filterCalendarEventsByPrivilege(ctx, user, cal, events, "read")
}

func (h *DavServer) loadCalendarWithAnyPrivilege(ctx context.Context, user *store.User, id int64, cleanPath string) (*store.CalendarAccess, error) {
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

func (h *DavServer) requireAnyCalendarPrivilege(ctx context.Context, user *store.User, cal *store.Calendar, cleanPath string) error {
	pc, err := h.calendarPrivilegeContextFor(ctx, user, cal, cleanPath)
	if err != nil {
		return err
	}
	if pc == nil {
		return store.ErrNotFound
	}
	sawForbidden := false
	for _, privilege := range calendarPrivilegeNames {
		allowed, denied := pc.decide(privilege)
		if allowed {
			return nil
		}
		if denied {
			sawForbidden = true
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
	return collectionResourcePath(calendarPrefix, calendarID)
}

func calendarPrivilegeDecisionWithDecider(cal *store.CalendarAccess, resourceName, privilege string, decider *batchedObjectACLDecider) (bool, bool) {
	if cal == nil || decider == nil {
		return false, false
	}
	if granted, denied, decided := decider.decide(resourceName, ".ics", privilege); decided {
		return granted, denied
	}
	return false, decider.hasApplicable(resourceName, ".ics")
}

// prefetchACLEntries loads the request's relevant resource/principal pairs in
// one scoped query and keys the result by normalized resource path.
func (h *DavServer) prefetchACLEntries(ctx context.Context, user *store.User, relevantPaths []string) (map[string][]store.ACLEntry, error) {
	if h == nil || h.store == nil || h.store.ACLEntries == nil || user == nil {
		return nil, nil
	}
	entries, err := h.store.ACLEntries.ListByResourcesAndPrincipals(ctx, relevantPaths, acl.PrincipalHrefs(user))
	if err != nil {
		return nil, err
	}
	result := make(map[string][]store.ACLEntry, len(relevantPaths))
	for _, entry := range entries {
		resourcePath := normalizeDAVHref(entry.ResourcePath)
		result[resourcePath] = append(result[resourcePath], entry)
	}
	return result, nil
}

type batchedObjectACLDecider struct {
	owner                   bool
	principals              map[string]struct{}
	entriesByPath           map[string][]store.ACLEntry
	collectionPath          string
	collectionEntries       []store.ACLEntry
	collectionDecisions     [7]batchedACLDecision
	collectionHasApplicable bool
	hasObjectEntries        bool
}

type batchedACLDecision struct {
	granted    bool
	applicable bool
}

func newBatchedObjectACLDecider(user *store.User, ownerID int64, collectionPath string, entriesByPath map[string][]store.ACLEntry) *batchedObjectACLDecider {
	decider := &batchedObjectACLDecider{
		owner:          user != nil && ownerID == user.ID,
		principals:     acl.ApplicablePrincipals(user),
		entriesByPath:  entriesByPath,
		collectionPath: normalizeDAVHref(collectionPath),
	}
	decider.collectionEntries = entriesByPath[decider.collectionPath]
	decider.collectionHasApplicable = acl.HasApplicablePrincipal(decider.collectionEntries, decider.principals)
	for i, privilege := range calendarPrivilegeNames {
		granted, applicable := acl.DecisionForPrivilege(decider.collectionEntries, decider.principals, privilege)
		decider.collectionDecisions[i] = batchedACLDecision{granted: granted, applicable: applicable}
	}
	for resourcePath, entries := range entriesByPath {
		if resourcePath != decider.collectionPath && len(entries) != 0 {
			decider.hasObjectEntries = true
			break
		}
	}
	return decider
}

func (d *batchedObjectACLDecider) decide(resourceName, extension, privilege string) (granted, denied, decided bool) {
	if d == nil {
		return false, false, false
	}
	if d.owner {
		return true, false, true
	}
	if d.hasObjectEntries {
		primary, alternate := d.objectEntries(resourceName, extension)
		if granted, applicable := decisionForEntrySets(primary, alternate, d.principals, privilege); applicable {
			return granted, !granted, true
		}
	}
	decision, ok := d.collectionDecision(privilege)
	if !ok {
		decision.granted, decision.applicable = acl.DecisionForPrivilege(d.collectionEntries, d.principals, privilege)
	}
	if decision.applicable {
		return decision.granted, !decision.granted, true
	}
	return false, false, false
}

func (d *batchedObjectACLDecider) hasApplicable(resourceName, extension string) bool {
	if d == nil {
		return false
	}
	if !d.hasObjectEntries {
		return d.collectionHasApplicable
	}
	primary, alternate := d.objectEntries(resourceName, extension)
	return acl.HasApplicablePrincipal(primary, d.principals) ||
		acl.HasApplicablePrincipal(alternate, d.principals) ||
		d.collectionHasApplicable
}

func (d *batchedObjectACLDecider) collectionDecision(privilege string) (batchedACLDecision, bool) {
	var index int
	switch privilege {
	case "read":
		index = 0
	case "read-free-busy":
		index = 1
	case "write":
		index = 2
	case "write-content":
		index = 3
	case "write-properties":
		index = 4
	case "bind":
		index = 5
	case "unbind":
		index = 6
	default:
		return batchedACLDecision{}, false
	}
	return d.collectionDecisions[index], true
}

func (d *batchedObjectACLDecider) objectEntries(resourceName, extension string) ([]store.ACLEntry, []store.ACLEntry) {
	resourceName = strings.TrimSpace(resourceName)
	if d == nil || resourceName == "" {
		return nil, nil
	}
	primaryPath := strings.TrimSuffix(d.collectionPath, "/") + "/" + resourceName
	alternatePath := primaryPath + extension
	if strings.EqualFold(path.Ext(resourceName), extension) {
		alternatePath = strings.TrimSuffix(primaryPath, path.Ext(resourceName))
	}
	return d.entriesByPath[primaryPath], d.entriesByPath[alternatePath]
}

func decisionForEntrySets(first, second []store.ACLEntry, principals map[string]struct{}, privilege string) (bool, bool) {
	if privilege == "write" {
		applicable := false
		for _, child := range []string{"write-content", "write-properties", "bind", "unbind"} {
			granted, decided := decideEntrySets(first, second, principals, child)
			applicable = applicable || decided
			if !granted {
				return false, applicable
			}
		}
		return applicable, applicable
	}
	return decideEntrySets(first, second, principals, privilege)
}

func decideEntrySets(first, second []store.ACLEntry, principals map[string]struct{}, privilege string) (bool, bool) {
	hasGrant := false
	for _, entries := range [][]store.ACLEntry{first, second} {
		for _, entry := range entries {
			if _, ok := principals[acl.NormalizePrincipalHref(entry.PrincipalHref)]; !ok || !acl.PrivilegeMatches(entry.Privilege, privilege) {
				continue
			}
			if !entry.IsGrant {
				return false, true
			}
			hasGrant = true
		}
	}
	return hasGrant, hasGrant
}

func (h *DavServer) loadCalendar(ctx context.Context, user *store.User, id int64) (*store.CalendarAccess, error) {
	return h.loadCalendarWithAnyPrivilege(ctx, user, id, collectionResourcePath(calendarPrefix, id))
}

func (h *DavServer) loadCalendarByName(ctx context.Context, user *store.User, name string) (*store.CalendarAccess, error) {
	accessible, err := h.accessibleCalendars(ctx, user)
	if err != nil {
		return nil, err
	}
	var match *store.CalendarAccess
	for _, c := range accessible {
		if (c.Slug != nil && *c.Slug == strings.ToLower(name)) || c.Name == name {
			if match != nil {
				return nil, errAmbiguousCalendar
			}
			copy := c
			match = &copy
		}
	}
	if match == nil {
		return nil, store.ErrNotFound
	}
	return match, nil
}
