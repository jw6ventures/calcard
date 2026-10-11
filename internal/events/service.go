package events

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/jw6ventures/calcard/internal/acl"
	"github.com/jw6ventures/calcard/internal/calobject"
	"github.com/jw6ventures/calcard/internal/store"
	"github.com/jw6ventures/calcard/internal/ui/utils"
)

const MaxBodyBytes int64 = 10 * 1024 * 1024

var (
	ErrNotFound             = errors.New("not found")
	ErrForbidden            = errors.New("forbidden")
	ErrBadRequest           = errors.New("bad request")
	ErrConflict             = errors.New("conflict")
	ErrPreconditionFailed   = errors.New("precondition failed")
	ErrUnsupportedMediaType = errors.New("unsupported media type")
)

type Service struct {
	store    *store.Store
	validate calobject.Validator
}

// NewService returns a Service that stores calendar data in st once validate
// accepts it. validate is required: a Service without one would store data a
// CalDAV client could not have written, so a nil validate panics.
func NewService(st *store.Store, validate calobject.Validator) *Service {
	if validate == nil {
		panic("events: NewService requires a calendar object validator")
	}
	return &Service{store: st, validate: validate}
}

type StructuredRecurrence struct {
	Frequency  string   `json:"frequency"`
	Interval   int      `json:"interval"`
	Count      int      `json:"count"`
	Until      string   `json:"until"`
	ByDay      []string `json:"byDay"`
	ByMonth    int      `json:"byMonth"`
	ByMonthDay int      `json:"byMonthDay"`
}

type StructuredInput struct {
	UID          string                `json:"uid"`
	Summary      string                `json:"summary"`
	DTStart      string                `json:"dtstart"`
	DTEnd        string                `json:"dtend"`
	AllDay       bool                  `json:"allDay"`
	Location     string                `json:"location"`
	Description  string                `json:"description"`
	Timezone     string                `json:"timezone"`
	URL          string                `json:"url"`
	Status       string                `json:"status"`
	Categories   []string              `json:"categories"`
	Class        string                `json:"class"`
	Transparency string                `json:"transparency"`
	Organizer    string                `json:"organizer"`
	Attendees    []string              `json:"attendees"`
	Attachments  []string              `json:"attachments"`
	Reminders    []int                 `json:"reminders"`
	Recurrence   *StructuredRecurrence `json:"recurrence"`
}

type UpsertInput struct {
	Structured  *StructuredInput
	RawICS      string
	ContentType string
	IfMatch     string
	IfNoneMatch string
}

func (s *Service) ListCalendars(ctx context.Context, user *store.User) ([]store.CalendarAccess, error) {
	return s.store.Calendars.ListAccessible(ctx, user.ID)
}

func (s *Service) GetCalendar(ctx context.Context, user *store.User, calendarID int64) (*store.CalendarAccess, error) {
	cal, err := s.store.Calendars.GetAccessible(ctx, calendarID, user.ID)
	if err != nil {
		return nil, err
	}
	if cal == nil {
		return nil, ErrNotFound
	}
	return cal, nil
}

func (s *Service) loadCalendarForResource(ctx context.Context, user *store.User, calendarID int64, resourceName, privilege string) (*store.CalendarAccess, error) {
	access, err := s.calendarForPrincipal(ctx, user, calendarID)
	if err != nil {
		return nil, err
	}
	if err := s.requireCalendarPrivilege(ctx, user, access, resourceName, privilege); err != nil {
		return nil, err
	}
	return access, nil
}

// calendarForPrincipal is the calendar calendarID names, carrying the
// privileges user holds on it.
func (s *Service) calendarForPrincipal(ctx context.Context, user *store.User, calendarID int64) (*store.CalendarAccess, error) {
	var legacy *store.CalendarAccess
	if s != nil && s.store != nil && s.store.Calendars != nil && user != nil {
		cal, err := s.store.Calendars.GetAccessible(ctx, calendarID, user.ID)
		if err != nil {
			return nil, err
		}
		legacy = cal
	}

	if s == nil || s.store == nil || s.store.Calendars == nil {
		return nil, ErrNotFound
	}
	cal, err := s.store.Calendars.GetByID(ctx, calendarID)
	if err != nil {
		return nil, err
	}
	if cal == nil {
		if legacy == nil {
			return nil, ErrNotFound
		}
		cal = &legacy.Calendar
	}

	access := &store.CalendarAccess{
		Calendar: *cal,
		Shared:   user == nil || cal.UserID != user.ID,
	}
	if legacy != nil {
		access.OwnerEmail = legacy.OwnerEmail
		access.Shared = legacy.Shared
		access.Privileges = legacy.EffectivePrivileges()
		access.PrivilegesResolved = legacy.PrivilegesResolved || legacy.Privileges.HasAny()
		access.Editor = access.Privileges.AllowsEventEditing()
	} else {
		// The user holds nothing on the calendar or any of its events, so
		// only an ACE can grant a privilege. Left unresolved, the empty
		// privileges would read as a read-only share.
		access.PrivilegesResolved = true
	}
	return access, nil
}

func (s *Service) ListEvents(ctx context.Context, user *store.User, calendarID int64, filter store.EventFilter) ([]store.Event, error) {
	cal, err := s.GetCalendar(ctx, user, calendarID)
	if err != nil {
		return nil, err
	}

	// A single principal sweep tells us whether event visibility is uniform
	// across the calendar (owner, or a non-owner whose grants are all
	// collection-level) and yields the ACL entries the slow path needs. When it
	// is uniform we decide once and let SQL apply LIMIT/OFFSET, instead of
	// fetching every row and filtering (and paginating) in Go.
	uniform, allowed, entriesByPath, err := s.calendarReadPlan(ctx, user, cal)
	if err != nil {
		return nil, err
	}
	if uniform {
		if !allowed {
			return []store.Event{}, nil
		}
		return s.listEventsFromStore(ctx, calendarID, filter)
	}

	// Slow path: per-resource ACL grants exist, so visibility varies per event.
	// Date and text predicates are still pushed into SQL (calendar-scoped,
	// indexed), but pagination is applied in Go after per-event ACL filtering so
	// page sizes stay correct.
	limit, offset := filter.Limit, filter.Offset
	dbFilter := filter
	dbFilter.Limit = 0
	dbFilter.Offset = 0

	events, err := s.listEventsFromStore(ctx, calendarID, dbFilter)
	if err != nil {
		return nil, err
	}

	visible := make([]store.Event, 0, len(events))
	for _, event := range events {
		allowed, err := s.canReadCalendarResourceWithEntries(user, cal, eventResourceName(event), entriesByPath)
		if err != nil {
			return nil, err
		}
		if allowed {
			visible = append(visible, event)
		}
	}

	if offset > 0 {
		if offset >= len(visible) {
			return []store.Event{}, nil
		}
		visible = visible[offset:]
	}
	if limit > 0 && limit < len(visible) {
		visible = visible[:limit]
	}
	return visible, nil
}

// listEventsFromStore fetches events for a calendar, applying any date/text
// predicates and LIMIT/OFFSET in the filter. An empty filter returns every
// event.
func (s *Service) listEventsFromStore(ctx context.Context, calendarID int64, filter store.EventFilter) ([]store.Event, error) {
	if filter.IsZero() {
		return s.store.Events.ListForCalendar(ctx, calendarID)
	}
	if !filter.HasPredicate() {
		return s.listPaginatedFromStore(ctx, calendarID, filter)
	}
	return s.store.Events.ListForCalendarFiltered(ctx, calendarID, filter)
}

// listPaginatedFromStore returns a pagination-only slice of a calendar's events
// in last_modified DESC order. A positive limit is pushed into SQL via
// ListForCalendarPaginated. An offset without a limit has no upper bound to push
// into a SQL LIMIT, so it falls back to fetching in order and skipping in Go.
// EXPENSIVE
func (s *Service) listPaginatedFromStore(ctx context.Context, calendarID int64, filter store.EventFilter) ([]store.Event, error) {
	if filter.Limit > 0 {
		page, err := s.store.Events.ListForCalendarPaginated(ctx, calendarID, filter.Limit, filter.Offset)
		if err != nil {
			return nil, err
		}
		if page == nil {
			return nil, nil
		}
		return page.Items, nil
	}

	events, err := s.store.Events.ListForCalendar(ctx, calendarID)
	if err != nil {
		return nil, err
	}
	if filter.Offset > 0 {
		if filter.Offset >= len(events) {
			return []store.Event{}, nil
		}
		events = events[filter.Offset:]
	}
	return events, nil
}

// calendarReadPlan decides, in a single ACL principal sweep, how ListEvents
// should read a calendar. uniform reports whether every event resolves to the
// same read decision (so the caller can skip per-event filtering and push
// pagination into SQL); allowed is that decision when uniform. When per-resource
// grants exist, uniform is false and entriesByPath carries the user's
// calendar-scoped ACL entries for the per-event slow path.
func (s *Service) calendarReadPlan(ctx context.Context, user *store.User, cal *store.CalendarAccess) (uniform, allowed bool, entriesByPath map[string][]store.ACLEntry, err error) {
	if cal != nil && user != nil && cal.UserID == user.ID {
		return true, true, nil, nil
	}
	if s == nil || s.store == nil || s.store.ACLEntries == nil || user == nil || cal == nil {
		// No ACL store (or no user/calendar to scope to): every event resolves to
		// the same effective-privilege decision.
		return true, cal.EffectivePrivileges().Allows("read"), nil, nil
	}

	entriesByPath, hasResourceEntries, err := s.collectCalendarACLEntries(ctx, user, cal.ID)
	if err != nil {
		return false, false, nil, err
	}
	if hasResourceEntries {
		return false, false, entriesByPath, nil
	}

	// No per-resource entries: every event resolves to the collection-level
	// decision. An empty resourceName collapses to the collection paths.
	allowed, _ = s.canReadCalendarResourceWithEntries(user, cal, "", entriesByPath)
	return true, allowed, entriesByPath, nil
}

// collectCalendarACLEntries returns, in one principal sweep, the ACL entries
// applicable to the user that target this calendar's collection or a resource
// beneath it, keyed by resource path. hasResourceEntries reports whether any
// entry targets a per-resource path (beneath the collection), which means event
// visibility can vary. The cost is bounded by the user's ACL entry count,
// independent of the event count.
func (s *Service) collectCalendarACLEntries(ctx context.Context, user *store.User, calendarID int64) (map[string][]store.ACLEntry, bool, error) {
	collectionPath := calendarACLCollectionPath(calendarID)
	resourcePrefix := collectionPath + "/"
	entriesByPath := make(map[string][]store.ACLEntry)
	hasResourceEntries := false
	for _, principalHref := range acl.PrincipalHrefs(user) {
		entries, err := s.store.ACLEntries.ListByPrincipal(ctx, principalHref)
		if err != nil {
			return nil, false, err
		}
		for _, entry := range entries {
			path := strings.TrimSpace(entry.ResourcePath)
			switch {
			case path == collectionPath:
				entriesByPath[path] = append(entriesByPath[path], entry)
			case strings.HasPrefix(path, resourcePrefix):
				entriesByPath[path] = append(entriesByPath[path], entry)
				hasResourceEntries = true
			}
		}
	}
	for resourcePath := range entriesByPath {
		acl.SortEntries(entriesByPath[resourcePath])
	}
	return entriesByPath, hasResourceEntries, nil
}

func (s *Service) GetEvent(ctx context.Context, user *store.User, calendarID int64, uid string) (*store.Event, error) {
	ev, err := s.store.Events.GetByUID(ctx, calendarID, uid)
	if err != nil {
		return nil, err
	}
	if ev == nil {
		return nil, ErrNotFound
	}

	cal, err := s.loadCalendarForResource(ctx, user, calendarID, eventResourceName(*ev), "read")
	if err != nil {
		if errors.Is(err, ErrForbidden) {
			return nil, ErrNotFound
		}
		return nil, err
	}
	allowed, err := s.canReadCalendarResource(ctx, user, cal, eventResourceName(*ev))
	if err != nil {
		return nil, err
	}
	if !allowed {
		return nil, ErrNotFound
	}
	return ev, nil
}

func (s *Service) CreateEvent(ctx context.Context, user *store.User, calendarID int64, input UpsertInput) (*store.Event, bool, error) {
	cal, err := s.GetCalendar(ctx, user, calendarID)
	if err != nil {
		return nil, false, err
	}
	// DAV:bind is a privilege on the calendar (RFC 3744 §3.9), checked before
	// the body is built and validated, as a DAV PUT checks it: a principal who
	// may not add to the calendar cannot make the server spend that work. A
	// create never lands on a stored object, so no object ACE bears on it.
	if err := s.requireCalendarPrivilege(ctx, user, cal, "", "bind"); err != nil {
		return nil, false, err
	}

	object, err := s.normalizeEventPayload(input, "", cal)
	if err != nil {
		return nil, false, err
	}
	uid := object.UID
	// The UID is also the resource name; both are unique index keys, whose
	// entries PostgreSQL bounds in size.
	if len(uid) > store.MaxIdentifierOctets {
		return nil, false, fmt.Errorf("%w: uid must be at most %d octets", ErrBadRequest, store.MaxIdentifierOctets)
	}
	existing, err := s.store.Events.GetByUID(ctx, calendarID, uid)
	if err != nil {
		return nil, false, err
	}
	if !checkConditionalHeaders(input.IfMatch, input.IfNoneMatch, existing) {
		return nil, false, ErrPreconditionFailed
	}
	if existing != nil {
		return nil, false, ErrConflict
	}

	// The store re-checks, inside its write transaction, that nothing holds
	// the resource name or the UID, as it does for a DAV PUT that creates: a
	// create that loses a race to another is refused rather than overwriting
	// the event the other stored, which needs DAV:write-content.
	write := store.CalendarObjectWrite{
		CalendarID:    calendarID,
		UID:           uid,
		ResourceName:  uid,
		RawICAL:       object.Body,
		ETag:          fmt.Sprintf("%x", sha256.Sum256([]byte(object.Body))),
		Metadata:      &object.Metadata,
		ExpectedState: &store.CalendarObjectResourceState{Exists: false},
	}
	result, err := s.store.PutCalendarObject(ctx, write)
	switch {
	case errors.Is(err, store.ErrResourceStateChanged):
		if input.IfNoneMatch == "*" {
			return nil, false, ErrPreconditionFailed
		}
		return nil, false, ErrConflict
	case errors.Is(err, store.ErrUIDConflict), errors.Is(err, store.ErrConflict):
		return nil, false, ErrConflict
	case err != nil:
		return nil, false, err
	case result == nil || result.Event == nil:
		return nil, false, fmt.Errorf("calendar object write returned no event for %q in calendar %d", uid, calendarID)
	}
	return result.Event, true, nil
}

func (s *Service) UpdateEvent(ctx context.Context, user *store.User, calendarID int64, uid string, input UpsertInput) (*store.Event, bool, error) {
	existing, err := s.store.Events.GetByUID(ctx, calendarID, uid)
	if err != nil {
		return nil, false, err
	}
	cal, existing, err := s.authorizeEventWrite(ctx, user, calendarID, existing, "write-content")
	if err != nil {
		return nil, false, err
	}
	if existing == nil {
		return nil, false, ErrNotFound
	}
	if !checkConditionalHeaders(input.IfMatch, input.IfNoneMatch, existing) {
		return nil, false, ErrPreconditionFailed
	}

	object, err := s.normalizeEventPayload(input, uid, cal)
	if err != nil {
		return nil, false, err
	}
	event, created, err := s.saveEvent(ctx, calendarID, uid, eventResourceName(*existing), object, input.IfMatch, input.IfNoneMatch)
	return event, created, err
}

func (s *Service) DeleteEvent(ctx context.Context, user *store.User, calendarID int64, uid, ifMatch, ifNoneMatch string) error {
	existing, err := s.store.Events.GetByUID(ctx, calendarID, uid)
	if err != nil {
		return err
	}
	if existing, err = s.authorizeEventDelete(ctx, user, calendarID, existing); err != nil {
		return err
	}
	// RFC 9110 §13.2.1: a precondition is not evaluated where the request
	// would otherwise answer neither 2xx nor 412, as it does for an event
	// that is not there.
	if existing == nil {
		return ErrNotFound
	}
	if !checkConditionalHeaders(ifMatch, ifNoneMatch, existing) {
		return ErrPreconditionFailed
	}
	resourcePaths := calendarACLResourcePaths(calendarID, eventResourceName(*existing))
	if len(resourcePaths) == 0 {
		return ErrNotFound
	}
	err = s.store.DeleteEventAndState(ctx, calendarID, store.EventDAVResourceState(existing), resourcePaths[0], nil)
	if errors.Is(err, store.ErrResourceStateChanged) {
		if ifMatch != "" || ifNoneMatch != "" {
			return ErrPreconditionFailed
		}
		return ErrConflict
	}
	return err
}

// authorizeEventDelete decides a delete of existing as DAV DELETE decides it
// (internal/dav/delete.go), but for one difference: the principal needs some
// privilege on the event and DAV:unbind on the calendar, a privilege of the
// collection a member leaves (RFC 3744 §3.10). A principal holding no
// privilege on the event, an event-level deny of every privilege included, is
// answered as for a UID that names nothing, and gets existing back as nil. For
// such a UID, a principal holding some privilege on the calendar but not
// DAV:unbind gets ErrForbidden, and one holding none gets ErrNotFound. The
// difference: where a principal's ACEs on the calendar grant none of its
// privileges, or deny every one, DAV answers 403, which confirms the calendar
// exists; this answers ErrNotFound, concealing the calendar from a principal
// who holds nothing on it, as GetEvent does.
func (s *Service) authorizeEventDelete(ctx context.Context, user *store.User, calendarID int64, existing *store.Event) (*store.Event, error) {
	cal, err := s.calendarForPrincipal(ctx, user, calendarID)
	if err != nil {
		return nil, err
	}
	resourceName := ""
	if existing != nil {
		resourceName = eventResourceName(*existing)
	}
	entriesByPath, err := s.calendarACLEntries(ctx, cal.ID, resourceName)
	if err != nil {
		return nil, err
	}
	if existing != nil && !holdsAnyCalendarPrivilege(user, cal, resourceName, entriesByPath) {
		existing = nil
	}
	if existing == nil && !holdsAnyCalendarPrivilege(user, cal, "", entriesByPath) {
		return nil, ErrNotFound
	}
	if !calendarPrivilegeAllowed(user, cal, "", "unbind", entriesByPath) {
		return nil, ErrForbidden
	}
	return existing, nil
}

// calendarACLEntries are the ACL entries on calendarID and, unless
// resourceName is empty, on the event it names, keyed by resource path.
func (s *Service) calendarACLEntries(ctx context.Context, calendarID int64, resourceName string) (map[string][]store.ACLEntry, error) {
	entriesByPath := map[string][]store.ACLEntry{}
	if s.store.ACLEntries == nil {
		return entriesByPath, nil
	}
	for _, resourcePath := range append(calendarACLResourcePaths(calendarID, resourceName), calendarACLCollectionPath(calendarID)) {
		entries, err := s.store.ACLEntries.ListByResource(ctx, resourcePath)
		if err != nil {
			return nil, err
		}
		entriesByPath[resourcePath] = entries
	}
	return entriesByPath, nil
}

// calendarPrivilegeAllowed is requireCalendarPrivilege's decision, made from
// entriesByPath.
func calendarPrivilegeAllowed(user *store.User, cal *store.CalendarAccess, resourceName, privilege string, entriesByPath map[string][]store.ACLEntry) bool {
	allowed, denied := calendarPrivilegeDecisionFromEntries(user, cal, resourceName, privilege, entriesByPath)
	if allowed || denied {
		return allowed
	}
	return cal != nil && cal.EffectivePrivileges().Allows(privilege)
}

// holdsAnyCalendarPrivilege reports whether user holds any of
// acl.CalendarPrivilegeNames on the event resourceName names, or on the
// calendar when it is empty, each decided from entriesByPath as
// requireCalendarPrivilege decides it. As in DAV's check, the calendar's
// resolved privileges count only when no ACE applicable to the user decides
// any of them.
func holdsAnyCalendarPrivilege(user *store.User, cal *store.CalendarAccess, resourceName string, entriesByPath map[string][]store.ACLEntry) bool {
	if cal == nil || user == nil {
		return false
	}
	denied := false
	for _, privilege := range acl.CalendarPrivilegeNames {
		allowed, privilegeDenied := calendarPrivilegeDecisionFromEntries(user, cal, resourceName, privilege, entriesByPath)
		if allowed {
			return true
		}
		denied = denied || privilegeDenied
	}
	return !denied && cal.EffectivePrivileges().HasAny()
}

// authorizeEventWrite checks privilege on the target of a write to existing:
// the event, or the calendar when the UID names none. It runs before anything
// about the event is answered. A principal who can neither read the event nor
// hold privilege on it is treated as for a UID that names nothing, and gets
// existing back as nil, so the answer cannot reveal that the event exists.
// For such a UID, a principal without privilege on the calendar gets
// ErrForbidden when they read the calendar and ErrNotFound otherwise, as
// GetEvent answers a principal who cannot read; one with it is authorized,
// and the caller answers as for a missing event.
func (s *Service) authorizeEventWrite(ctx context.Context, user *store.User, calendarID int64, existing *store.Event, privilege string) (*store.CalendarAccess, *store.Event, error) {
	cal, err := s.calendarForPrincipal(ctx, user, calendarID)
	if err != nil {
		return nil, nil, err
	}
	if existing != nil {
		visible, err := s.authorizeCalendarResource(ctx, user, cal, eventResourceName(*existing), privilege)
		if err != nil {
			return nil, nil, err
		}
		if visible {
			return cal, existing, nil
		}
	}
	visible, err := s.authorizeCalendarResource(ctx, user, cal, "", privilege)
	switch {
	case err != nil:
		return nil, nil, err
	case !visible:
		return nil, nil, ErrNotFound
	}
	return cal, nil, nil
}

// authorizeCalendarResource checks that user holds privilege on the event
// resourceName names, or on the calendar when it is empty. visible reports
// whether user may know of the resource at all, by reading it or by holding
// privilege on it; a user who may but lacks privilege gets ErrForbidden.
func (s *Service) authorizeCalendarResource(ctx context.Context, user *store.User, cal *store.CalendarAccess, resourceName, privilege string) (visible bool, err error) {
	err = s.requireCalendarPrivilege(ctx, user, cal, resourceName, privilege)
	if !errors.Is(err, ErrForbidden) {
		return err == nil, err
	}
	readable, err := s.canReadCalendarResource(ctx, user, cal, resourceName)
	if err != nil || !readable {
		return false, err
	}
	return true, ErrForbidden
}

func (s *Service) requireCalendarPrivilege(ctx context.Context, user *store.User, cal *store.CalendarAccess, resourceName, privilege string) error {
	allowed, denied, err := s.calendarPrivilegeDecision(ctx, user, cal, resourceName, privilege)
	if err != nil {
		return err
	}
	if allowed {
		return nil
	}
	if denied {
		return ErrForbidden
	}
	if cal != nil && cal.EffectivePrivileges().Allows(privilege) {
		return nil
	}
	return ErrForbidden
}

func (s *Service) canReadCalendarResource(ctx context.Context, user *store.User, cal *store.CalendarAccess, resourceName string) (bool, error) {
	allowed, denied, err := s.calendarPrivilegeDecision(ctx, user, cal, resourceName, "read")
	if err != nil {
		return false, err
	}
	if allowed {
		return true, nil
	}
	if denied {
		return false, nil
	}
	return cal != nil && cal.EffectivePrivileges().Allows("read"), nil
}

func (s *Service) canReadCalendarResourceWithEntries(user *store.User, cal *store.CalendarAccess, resourceName string, entriesByPath map[string][]store.ACLEntry) (bool, error) {
	allowed, denied := calendarPrivilegeDecisionFromEntries(user, cal, resourceName, "read", entriesByPath)
	if allowed {
		return true, nil
	}
	if denied {
		return false, nil
	}
	return cal != nil && cal.EffectivePrivileges().Allows("read"), nil
}

func (s *Service) calendarPrivilegeDecision(ctx context.Context, user *store.User, cal *store.CalendarAccess, resourceName, privilege string) (bool, bool, error) {
	if cal == nil || user == nil {
		return false, false, nil
	}
	if cal.UserID == user.ID {
		return true, false, nil
	}
	if s == nil || s.store == nil || s.store.ACLEntries == nil {
		return false, false, nil
	}

	resourcePaths := calendarACLResourcePaths(cal.ID, resourceName)
	resourceApplicable, err := s.aclHasApplicablePrincipal(ctx, user, cal.UserID, resourcePaths)
	if err != nil {
		return false, false, err
	}
	if granted, applicable, err := s.aclDecision(ctx, user, cal.UserID, resourcePaths, privilege); err != nil {
		return false, false, err
	} else if applicable {
		return granted, !granted, nil
	}

	collectionPaths := []string{calendarACLCollectionPath(cal.ID)}
	collectionApplicable, err := s.aclHasApplicablePrincipal(ctx, user, cal.UserID, collectionPaths)
	if err != nil {
		return false, false, err
	}
	if granted, applicable, err := s.aclDecision(ctx, user, cal.UserID, collectionPaths, privilege); err != nil {
		return false, false, err
	} else if applicable {
		return granted, !granted, nil
	}

	return false, resourceApplicable || collectionApplicable, nil
}

func (s *Service) aclDecision(ctx context.Context, user *store.User, ownerID int64, resourcePaths []string, privilege string) (bool, bool, error) {
	entries, err := s.aclEntriesForPaths(ctx, resourcePaths)
	if err != nil {
		return false, false, err
	}

	granted, applicable := acl.DecisionForPrivilege(entries, calendarApplicablePrincipals(user, ownerID), privilege)
	return granted, applicable, nil
}

func (s *Service) aclHasApplicablePrincipal(ctx context.Context, user *store.User, ownerID int64, resourcePaths []string) (bool, error) {
	entries, err := s.aclEntriesForPaths(ctx, resourcePaths)
	if err != nil {
		return false, err
	}
	if len(entries) == 0 {
		return false, nil
	}

	return acl.HasApplicablePrincipal(entries, calendarApplicablePrincipals(user, ownerID)), nil
}

// calendarApplicablePrincipals names the principals an ACE on a calendar or one
// of its objects can apply to. A calendar is never a principal resource, so
// DAV:self cannot resolve against it; DAV:property:owner resolves to the row's
// owning user.
func calendarApplicablePrincipals(user *store.User, ownerID int64) map[string]struct{} {
	return acl.ApplicablePrincipalsFor(user, acl.ResourcePrincipals{OwnerHref: acl.PrincipalHref(ownerID)})
}

func (s *Service) aclEntriesForPaths(ctx context.Context, resourcePaths []string) ([]store.ACLEntry, error) {
	if s == nil || s.store == nil || s.store.ACLEntries == nil {
		return nil, nil
	}
	seen := make(map[string]struct{}, len(resourcePaths))
	var result []store.ACLEntry
	for _, resourcePath := range resourcePaths {
		if resourcePath == "" {
			continue
		}
		if _, ok := seen[resourcePath]; ok {
			continue
		}
		seen[resourcePath] = struct{}{}
		entries, err := s.store.ACLEntries.ListByResource(ctx, resourcePath)
		if err != nil {
			return nil, err
		}
		result = append(result, entries...)
	}
	acl.SortEntries(result)
	return result, nil
}

func (s *Service) prefetchCalendarACLEntries(ctx context.Context, user *store.User, calendarID int64, events []store.Event) (map[string][]store.ACLEntry, error) {
	if s == nil || s.store == nil || s.store.ACLEntries == nil || user == nil {
		return nil, nil
	}

	relevantPaths := map[string]struct{}{
		calendarACLCollectionPath(calendarID): {},
	}
	for _, event := range events {
		for _, resourcePath := range calendarACLResourcePaths(calendarID, eventResourceName(event)) {
			relevantPaths[resourcePath] = struct{}{}
		}
	}

	result := make(map[string][]store.ACLEntry, len(relevantPaths))
	for _, principalHref := range acl.PrincipalHrefs(user) {
		entries, err := s.store.ACLEntries.ListByPrincipal(ctx, principalHref)
		if err != nil {
			return nil, err
		}
		for _, entry := range entries {
			resourcePath := strings.TrimSpace(entry.ResourcePath)
			if _, ok := relevantPaths[resourcePath]; !ok {
				continue
			}
			result[resourcePath] = append(result[resourcePath], entry)
		}
	}
	for resourcePath := range result {
		acl.SortEntries(result[resourcePath])
	}
	return result, nil
}

func calendarACLCollectionPath(calendarID int64) string {
	return fmt.Sprintf("/dav/calendars/%d", calendarID)
}

func calendarACLResourcePaths(calendarID int64, resourceName string) []string {
	resourceName = strings.TrimSpace(resourceName)
	if resourceName == "" {
		return nil
	}
	base := calendarACLCollectionPath(calendarID) + "/" + resourceName
	paths := []string{base}
	if strings.EqualFold(pathExt(resourceName), ".ics") {
		paths = append(paths, strings.TrimSuffix(base, pathExt(resourceName)))
	} else {
		paths = append(paths, base+".ics")
	}
	return paths
}

func eventResourceName(event store.Event) string {
	if event.ResourceName != "" {
		return event.ResourceName
	}
	return event.UID
}

func calendarPrivilegeDecisionFromEntries(user *store.User, cal *store.CalendarAccess, resourceName, privilege string, entriesByPath map[string][]store.ACLEntry) (bool, bool) {
	if cal == nil || user == nil {
		return false, false
	}
	if cal.UserID == user.ID {
		return true, false
	}

	resourcePaths := calendarACLResourcePaths(cal.ID, resourceName)
	if granted, applicable := aclDecisionForResourcePaths(entriesByPath, user, cal.UserID, resourcePaths, privilege); applicable {
		return granted, !granted
	}

	collectionPaths := []string{calendarACLCollectionPath(cal.ID)}
	if granted, applicable := aclDecisionForResourcePaths(entriesByPath, user, cal.UserID, collectionPaths, privilege); applicable {
		return granted, !granted
	}

	return false, aclHasApplicablePrincipalForPaths(entriesByPath, user, cal.UserID, resourcePaths) ||
		aclHasApplicablePrincipalForPaths(entriesByPath, user, cal.UserID, collectionPaths)
}

func aclDecisionForResourcePaths(entriesByPath map[string][]store.ACLEntry, user *store.User, ownerID int64, resourcePaths []string, privilege string) (bool, bool) {
	if len(entriesByPath) == 0 {
		return false, false
	}
	entries := make([]store.ACLEntry, 0, len(resourcePaths))
	for _, resourcePath := range resourcePaths {
		entries = append(entries, entriesByPath[resourcePath]...)
	}
	acl.SortEntries(entries)
	return acl.DecisionForPrivilege(entries, calendarApplicablePrincipals(user, ownerID), privilege)
}

func aclHasApplicablePrincipalForPaths(entriesByPath map[string][]store.ACLEntry, user *store.User, ownerID int64, resourcePaths []string) bool {
	if len(entriesByPath) == 0 {
		return false
	}
	applicablePrincipals := calendarApplicablePrincipals(user, ownerID)
	for _, resourcePath := range resourcePaths {
		if acl.HasApplicablePrincipal(entriesByPath[resourcePath], applicablePrincipals) {
			return true
		}
	}
	return false
}

func pathExt(resourceName string) string {
	idx := strings.LastIndex(resourceName, ".")
	if idx < 0 {
		return ""
	}
	return resourceName[idx:]
}

// normalizeEventPayload builds the calendar object a request describes and puts
// it through the validator a DAV PUT into cal applies, so the API stores nothing
// a CalDAV client could not have written. expectedUID, when set, is the UID the
// request path names.
func (s *Service) normalizeEventPayload(input UpsertInput, expectedUID string, cal *store.CalendarAccess) (*calobject.Object, error) {
	var body string
	switch {
	case strings.TrimSpace(input.RawICS) != "":
		if err := validateCalendarContentType(input.ContentType); err != nil {
			return nil, err
		}
		body = ensureCRLF(strings.TrimSpace(input.RawICS))
	case input.Structured != nil:
		built, _, err := buildStructuredEvent(input.Structured, expectedUID)
		if err != nil {
			return nil, err
		}
		body = built
	default:
		return nil, fmt.Errorf("%w: missing event body", ErrBadRequest)
	}

	object, err := s.validate(body, cal)
	if err != nil {
		return nil, calendarObjectError(err)
	}
	if expectedUID != "" && object.UID != expectedUID {
		return nil, fmt.Errorf("%w: path uid does not match calendar data uid", ErrBadRequest)
	}
	return object, nil
}

// calendarObjectError maps a refusal from the CalDAV validator onto this API's
// errors. A body that is a scheduling message, or that names more than one
// UID, cannot be the one resource the request writes, which is a conflict;
// every other refusal is a bad request the caller fixes by sending other data.
// The message names the RFC 4791 precondition or the reason for the refusal,
// never the validator's own error text.
func calendarObjectError(err error) error {
	var refusal *calobject.Error
	if !errors.As(err, &refusal) {
		return err
	}
	conditions := strings.Join(refusal.Conditions, ", ")
	for _, reason := range []error{calobject.ErrSchedulingMessage, calobject.ErrMultipleUIDs} {
		if errors.Is(refusal, reason) {
			return fmt.Errorf("%w: %s: %w", ErrConflict, conditions, reason)
		}
	}
	if errors.Is(refusal, calobject.ErrUndefinedTimezone) {
		return fmt.Errorf("%w: %w", ErrBadRequest, calobject.ErrUndefinedTimezone)
	}
	return fmt.Errorf("%w: %s", ErrBadRequest, conditions)
}

func (s *Service) saveEvent(ctx context.Context, calendarID int64, uid, resourceName string, object *calobject.Object, ifMatch, ifNoneMatch string) (*store.Event, bool, error) {
	existingByResource, err := s.store.Events.GetByResourceName(ctx, calendarID, resourceName)
	if err != nil {
		return nil, false, err
	}
	if existingByResource != nil && existingByResource.UID != uid {
		return nil, false, ErrConflict
	}

	existing, err := s.store.Events.GetByUID(ctx, calendarID, uid)
	if err != nil {
		return nil, false, err
	}
	if existing != nil && existing.ResourceName != "" && existing.ResourceName != resourceName {
		return nil, false, ErrConflict
	}
	if !checkConditionalHeaders(ifMatch, ifNoneMatch, existing) {
		return nil, false, ErrPreconditionFailed
	}

	etag := fmt.Sprintf("%x", sha256.Sum256([]byte(object.Body)))
	created := existing == nil
	ev, err := s.store.Events.Upsert(ctx, store.Event{
		CalendarID:    calendarID,
		UID:           uid,
		ResourceName:  resourceName,
		RawICAL:       object.Body,
		ETag:          etag,
		WriteMetadata: &object.Metadata,
	})
	if err != nil {
		return nil, false, err
	}
	return ev, created, nil
}

func buildStructuredEvent(input *StructuredInput, expectedUID string) (string, string, error) {
	summary := strings.TrimSpace(input.Summary)
	if summary == "" {
		return "", "", fmt.Errorf("%w: summary is required", ErrBadRequest)
	}
	dtstart := strings.TrimSpace(input.DTStart)
	dtend := strings.TrimSpace(input.DTEnd)
	if dtstart == "" || dtend == "" {
		return "", "", fmt.Errorf("%w: start and end are required", ErrBadRequest)
	}
	// A request carries no VTIMEZONE, so only the host can define the zone.
	timezone := strings.TrimSpace(input.Timezone)
	zone, known := utils.ResolveEventZone(timezone, nil)
	if !known {
		return "", "", fmt.Errorf("%w: timezone is not a zone the server's time zone database defines", ErrBadRequest)
	}
	if err := utils.ValidateEventDates(dtstart, dtend, input.AllDay, zone); err != nil {
		return "", "", fmt.Errorf("%w: %v", ErrBadRequest, err)
	}

	uid := strings.TrimSpace(input.UID)
	if expectedUID != "" {
		if uid != "" && uid != expectedUID {
			return "", "", fmt.Errorf("%w: path uid does not match payload uid", ErrBadRequest)
		}
		uid = expectedUID
	}
	if uid == "" {
		uid = utils.GenerateUID()
	}

	var recurrence *utils.RecurrenceOptions
	if input.Recurrence != nil {
		until := strings.TrimSpace(input.Recurrence.Until)
		if until != "" {
			if _, err := time.Parse(time.DateOnly, until); err != nil {
				return "", "", fmt.Errorf("%w: recurrence until must be a YYYY-MM-DD date", ErrBadRequest)
			}
		}
		recurrence = &utils.RecurrenceOptions{
			Frequency: strings.TrimSpace(input.Recurrence.Frequency),
			Interval:  input.Recurrence.Interval,
			Count:     input.Recurrence.Count,
			Until:     until,
			ByDay:     input.Recurrence.ByDay,
			ByMonth:   input.Recurrence.ByMonth,
		}
		if input.Recurrence.ByMonthDay > 0 {
			recurrence.ByMonthDay = []int{input.Recurrence.ByMonthDay}
		}
	}

	opts := &utils.EventOptions{
		Timezone:     timezone,
		URL:          strings.TrimSpace(input.URL),
		Status:       strings.TrimSpace(input.Status),
		Categories:   input.Categories,
		Class:        strings.TrimSpace(input.Class),
		Transparency: strings.TrimSpace(input.Transparency),
		Organizer:    strings.TrimSpace(input.Organizer),
		Attendees:    input.Attendees,
		Attachments:  input.Attachments,
		Reminders:    input.Reminders,
	}

	body, err := utils.BuildEvent(
		uid,
		summary,
		dtstart,
		dtend,
		input.AllDay,
		strings.TrimSpace(input.Location),
		strings.TrimSpace(input.Description),
		recurrence,
		opts,
	)
	if err != nil {
		return "", "", fmt.Errorf("%w: %v", ErrBadRequest, err)
	}
	return body, uid, nil
}

func validateCalendarContentType(contentType string) error {
	contentType = strings.ToLower(strings.TrimSpace(contentType))
	if contentType == "" {
		return fmt.Errorf("%w: missing content type", ErrUnsupportedMediaType)
	}
	if strings.HasPrefix(contentType, "text/calendar") || strings.HasPrefix(contentType, "application/ical") || strings.HasPrefix(contentType, "application/ics") {
		return nil
	}
	return fmt.Errorf("%w: unsupported content type", ErrUnsupportedMediaType)
}

func checkConditionalHeaders(ifMatch, ifNoneMatch string, existing *store.Event) bool {
	if ifNoneMatch == "*" {
		return existing == nil
	}
	if ifMatch != "" {
		if existing == nil {
			return false
		}
		return strings.Trim(ifMatch, "\"") == existing.ETag
	}
	if ifNoneMatch != "" {
		if existing == nil {
			return true
		}
		return strings.Trim(ifNoneMatch, "\"") != existing.ETag
	}
	return true
}

func ensureCRLF(body string) string {
	lines := utils.UnfoldLines(body)
	return strings.Join(lines, "\r\n") + "\r\n"
}

func StatusCode(err error) int {
	switch {
	case err == nil:
		return http.StatusOK
	case errors.Is(err, ErrNotFound):
		return http.StatusNotFound
	case errors.Is(err, ErrForbidden):
		return http.StatusForbidden
	case errors.Is(err, ErrConflict):
		return http.StatusConflict
	case errors.Is(err, ErrPreconditionFailed):
		return http.StatusPreconditionFailed
	case errors.Is(err, ErrUnsupportedMediaType):
		return http.StatusUnsupportedMediaType
	case errors.Is(err, ErrBadRequest):
		return http.StatusBadRequest
	default:
		return http.StatusInternalServerError
	}
}
