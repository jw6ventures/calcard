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
	"github.com/jw6ventures/calcard/internal/httpcond"
	"github.com/jw6ventures/calcard/internal/store"
	"github.com/jw6ventures/calcard/internal/ui/utils"
)

const MaxBodyBytes int64 = 10 * 1024 * 1024

var (
	ErrNotFound           = errors.New("not found")
	ErrForbidden          = errors.New("forbidden")
	ErrBadRequest         = errors.New("bad request")
	ErrConflict           = errors.New("conflict")
	ErrPreconditionFailed = errors.New("precondition failed")

	// ErrLocked refuses a write to an event, or to a calendar that is to hold
	// it, that a WebDAV lock covers. The API submits no lock token, so it
	// cannot write under one; the lock's holder writes over CalDAV.
	ErrLocked = errors.New("locked")

	// ErrChangedConcurrently is a write given up because the event or the
	// access that decided it kept changing while the write was retried. It is
	// ErrConflict.
	ErrChangedConcurrently = fmt.Errorf("%w: the event or its access kept changing", ErrConflict)

	// ErrUnwritableName refuses a write that would keep an event under a name
	// no href carries, such as "." as a 1.1.x release could store it. Such an
	// event can be read, deleted, or moved over CalDAV to another name. It is
	// ErrBadRequest.
	ErrUnwritableName = fmt.Errorf("%w: the event is stored under a name that cannot be kept; it can be read, deleted or moved to another name", ErrBadRequest)
)

// maxWriteAttempts bounds how often a write is read and decided afresh when
// another writer changes the event, or the access that decided it, between the
// decision and the write.
const maxWriteAttempts = 3

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
	entries, err := s.store.ACLEntries.ListByPrincipals(ctx, acl.PrincipalHrefs(user))
	if err != nil {
		return nil, false, err
	}
	for _, entry := range entries {
		path := entry.ResourcePath
		switch {
		case path == collectionPath:
			entriesByPath[path] = append(entriesByPath[path], entry)
		case strings.HasPrefix(path, resourcePrefix):
			entriesByPath[path] = append(entriesByPath[path], entry)
			hasResourceEntries = true
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

// CreateEvent stores a new event under its UID. The write is conditional on
// nothing holding the UID or the resource name, so an event another client
// stores concurrently is not overwritten, and it fails with ErrLocked where a
// WebDAV lock covers the new name or the calendar. A write that loses to a
// concurrent change of the calendar's events or of the ACL is decided again,
// which reports the event another client stored as a conflict and a withdrawn
// grant as forbidden.
func (s *Service) CreateEvent(ctx context.Context, user *store.User, calendarID int64, input UpsertInput) (*store.Event, error) {
	for attempt := 1; ; attempt++ {
		event, err := s.createEventOnce(ctx, user, calendarID, input)
		if !writeLost(err) {
			return event, err
		}
		if attempt == maxWriteAttempts {
			return nil, ErrChangedConcurrently
		}
	}
}

func (s *Service) createEventOnce(ctx context.Context, user *store.User, calendarID int64, input UpsertInput) (*store.Event, error) {
	guard, err := s.pinACL(ctx, user, calendarID, "")
	if err != nil {
		return nil, err
	}
	cal, err := s.GetCalendar(ctx, user, calendarID)
	if err != nil {
		return nil, err
	}
	// DAV:bind is a privilege on the calendar (RFC 3744 §3.9), checked before
	// the body is built and validated, as a DAV PUT checks it: a principal who
	// may not add to the calendar cannot make the server spend that work. A
	// create never lands on a stored object, so no object ACE bears on it.
	if err := s.requireCalendarPrivilege(ctx, user, cal, "", "bind"); err != nil {
		return nil, err
	}

	object, err := s.normalizeEventPayload(input, "", cal)
	if err != nil {
		return nil, err
	}
	uid := object.UID
	// The event is stored under its UID as its resource name, which must be
	// one an href can name.
	if !store.ValidMemberName(uid) {
		return nil, fmt.Errorf(`%w: uid must be one path segment, neither "." nor "..", without '/'`, ErrBadRequest)
	}
	collectionPath := calendarACLCollectionPath(calendarID)
	lockPreconditions := []store.LockPrecondition{
		store.NewLockPrecondition(store.MemberStatePath(collectionPath, uid)),
		store.NewLockPrecondition(collectionPath),
	}
	if input.IfMatch != "" || input.IfNoneMatch != "" {
		if err := writeFailure(s.store.CheckLocks(ctx, lockPreconditions)); err != nil {
			return nil, err
		}
	}
	existing, err := s.store.Events.GetByUID(ctx, calendarID, uid)
	if err != nil {
		return nil, err
	}
	if !checkConditionalHeaders(input.IfMatch, input.IfNoneMatch, existing) {
		return nil, ErrPreconditionFailed
	}
	if existing != nil {
		return nil, ErrConflict
	}

	// The store re-checks, inside its write transaction, that nothing holds
	// the resource name or the UID, as it does for a DAV PUT that creates: a
	// create that loses a race to another is refused rather than overwriting
	// the event the other stored, which needs DAV:write-content.
	result, err := s.store.PutCalendarObject(ctx, store.CalendarObjectWrite{
		CalendarID:        calendarID,
		UID:               uid,
		ResourceName:      uid,
		RawICAL:           object.Body,
		ETag:              fmt.Sprintf("%x", sha256.Sum256([]byte(object.Body))),
		Metadata:          &object.Metadata,
		ExpectedState:     &store.CalendarObjectResourceState{Exists: false},
		ExpectedACL:       guard,
		LockPreconditions: lockPreconditions,
	})
	switch {
	case err != nil:
		return nil, writeFailure(err)
	case result == nil || result.Event == nil:
		return nil, fmt.Errorf("calendar object write returned no event for %q in calendar %d", uid, calendarID)
	}
	return result.Event, nil
}

// UpdateEvent replaces the event uid names. The write is conditional on the
// version the request was decided on: a request's If-Match or If-None-Match
// is checked against the version each attempt reads, so it fails only when
// the event itself changed, not when only the ACL did. An event removed
// between the decision and the write is not stored again. A write that loses
// to a concurrent change of the event or of the ACL is decided again, and
// fails with ErrLocked where a WebDAV lock covers the event.
func (s *Service) UpdateEvent(ctx context.Context, user *store.User, calendarID int64, uid string, input UpsertInput) (*store.Event, bool, error) {
	for attempt := 1; ; attempt++ {
		event, err := s.updateEventOnce(ctx, user, calendarID, uid, input)
		if !writeLost(err) {
			return event, false, err
		}
		if attempt == maxWriteAttempts {
			return nil, false, ErrChangedConcurrently
		}
	}
}

func (s *Service) updateEventOnce(ctx context.Context, user *store.User, calendarID int64, uid string, input UpsertInput) (*store.Event, error) {
	existing, err := s.store.Events.GetByUID(ctx, calendarID, uid)
	if err != nil {
		return nil, err
	}
	resourceName := ""
	if existing != nil {
		resourceName = eventResourceName(*existing)
	}
	guard, err := s.pinACL(ctx, user, calendarID, resourceName)
	if err != nil {
		return nil, err
	}
	cal, existing, err := s.AuthorizeEventWrite(ctx, user, calendarID, existing, "write-content")
	if err != nil {
		return nil, err
	}
	if existing == nil {
		return nil, ErrNotFound
	}
	if err := writableEventName(*existing); err != nil {
		return nil, err
	}
	lockPreconditions := []store.LockPrecondition{store.NewLockPrecondition(store.MemberStatePath(calendarACLCollectionPath(calendarID), resourceName))}
	if input.IfMatch != "" || input.IfNoneMatch != "" {
		if err := writeFailure(s.store.CheckLocks(ctx, lockPreconditions)); err != nil {
			return nil, err
		}
	}
	if !checkConditionalHeaders(input.IfMatch, input.IfNoneMatch, existing) {
		return nil, ErrPreconditionFailed
	}

	object, err := s.normalizeEventPayload(input, uid, cal)
	if err != nil {
		return nil, err
	}
	result, err := s.store.PutCalendarObject(ctx, store.CalendarObjectWrite{
		CalendarID:   calendarID,
		UID:          uid,
		ResourceName: resourceName,
		RawICAL:      object.Body,
		ETag:         fmt.Sprintf("%x", sha256.Sum256([]byte(object.Body))),
		Metadata:     &object.Metadata,
		// The version read is the one the request's own conditions were checked
		// against, so holding the write to it holds them too.
		Precondition:      store.CalendarObjectPrecondition{IfMatch: &store.ETagCondition{ETags: []string{existing.ETag}}},
		ExpectedState:     &store.CalendarObjectResourceState{Exists: true},
		ExpectedACL:       guard,
		LockPreconditions: lockPreconditions,
	})
	switch {
	case err != nil:
		return nil, writeFailure(err)
	case result == nil || result.Event == nil:
		return nil, fmt.Errorf("calendar object write returned no event for %q in calendar %d", uid, calendarID)
	}
	return result.Event, nil
}

// DeleteEvent removes the event uid names, honouring If-Match and
// If-None-Match. It is decided as a DAV DELETE of the event is: see
// AuthorizeEventRemoval, and it fails with ErrLocked where a WebDAV lock
// covers the event or the calendar. An event that is not there, or that the
// user is answered about as if it were not, is ErrNotFound whatever the
// preconditions, which RFC 9110 section 13.2.1 has a server ignore when its
// answer without them would be neither 2xx nor 412. A delete that loses to a
// concurrent change of the event or of the ACL is decided again, and the
// caller's preconditions are checked again against the event as it then reads.
func (s *Service) DeleteEvent(ctx context.Context, user *store.User, calendarID int64, uid, ifMatch, ifNoneMatch string) error {
	for attempt := 1; ; attempt++ {
		err := s.deleteEventOnce(ctx, user, calendarID, uid, ifMatch, ifNoneMatch)
		if !writeLost(err) {
			return err
		}
		if attempt == maxWriteAttempts {
			return ErrChangedConcurrently
		}
	}
}

func (s *Service) deleteEventOnce(ctx context.Context, user *store.User, calendarID int64, uid, ifMatch, ifNoneMatch string) error {
	existing, err := s.store.Events.GetByUID(ctx, calendarID, uid)
	if err != nil {
		return err
	}
	resourceName := ""
	if existing != nil {
		resourceName = eventResourceName(*existing)
	}
	guard, err := s.pinACL(ctx, user, calendarID, resourceName)
	if err != nil {
		return err
	}
	if existing, err = s.AuthorizeEventRemoval(ctx, user, calendarID, existing); err != nil {
		return err
	}
	if existing == nil {
		return ErrNotFound
	}
	resourcePaths := calendarACLResourcePaths(calendarID, resourceName)
	if len(resourcePaths) == 0 {
		return ErrNotFound
	}
	lockPreconditions := []store.LockPrecondition{
		store.NewLockPrecondition(resourcePaths[0]),
		store.NewLockPrecondition(calendarACLCollectionPath(calendarID)),
	}
	if ifMatch != "" || ifNoneMatch != "" {
		if err := writeFailure(s.store.CheckLocks(ctx, lockPreconditions)); err != nil {
			return err
		}
	}
	if !checkConditionalHeaders(ifMatch, ifNoneMatch, existing) {
		return ErrPreconditionFailed
	}
	expected := store.EventDAVResourceState(existing)
	expected.ACL = guard
	err = s.store.DeleteEventAndState(ctx, calendarID, expected, resourcePaths[0], lockPreconditions)
	return writeFailure(err)
}

// writeLost reports whether err means the write found the event, or the ACL
// it was decided on, other than the decision read them, so the caller reads
// and decides again.
func writeLost(err error) bool {
	return errors.Is(err, store.ErrResourceStateChanged) || errors.Is(err, store.ErrPreconditionFailed)
}

// writeFailure maps the store's refusal of an event write onto this API's
// errors, leaving a write that lost to a concurrent change for the caller to
// decide again.
func writeFailure(err error) error {
	switch {
	case errors.Is(err, store.ErrLockConflict):
		return ErrLocked
	case errors.Is(err, store.ErrUIDConflict), errors.Is(err, store.ErrConflict):
		return ErrConflict
	}
	return err
}

// pinACL pins the ACL entries that decide user's access to calendarID and, when
// resourceName is set, to the event it names, for the store to check again
// under the write's locks: an ACL change landing between the decision and the
// write fails the write with store.ErrResourceStateChanged, and the caller
// decides again on the new ACL. Pin before deciding, so that the guard cannot
// hold entries newer than the decision's. A calendar the user owns has none,
// since an owner's privileges do not come from the ACL, and the guard is nil.
func (s *Service) pinACL(ctx context.Context, user *store.User, calendarID int64, resourceName string) (*store.ACLGuard, error) {
	if s.store.ACLEntries == nil || s.store.Calendars == nil {
		return nil, nil
	}
	cal, err := s.store.Calendars.GetByID(ctx, calendarID)
	if err != nil {
		return nil, err
	}
	if cal == nil || (user != nil && cal.UserID == user.ID) {
		return nil, nil
	}
	paths := append(calendarACLResourcePaths(calendarID, resourceName), calendarACLCollectionPath(calendarID))
	entries, err := s.store.ACLEntries.ListByResources(ctx, paths)
	if err != nil {
		return nil, err
	}
	return store.NewACLGuard(paths, entries), nil
}

// AuthorizeEventRemoval decides taking existing out of calendarID, by deleting
// it or moving it to another calendar, as DAV DELETE decides it
// (internal/dav/delete.go), but for one difference: the principal needs some
// privilege on the event and DAV:unbind on the calendar, a privilege of the
// collection a member leaves (RFC 3744 §3.10). A principal holding no
// privilege on the event, an event-level deny of every privilege included, is
// answered as for a UID that names nothing, and gets existing back as nil. For
// such a UID, a principal holding some privilege on the calendar but not
// DAV:unbind gets ErrForbidden, and one holding none gets ErrNotFound. The
// difference: where a principal's ACEs on the calendar deny every one of its
// privileges, DAV answers 403, which confirms the calendar exists; this
// answers ErrNotFound, concealing the calendar from a principal who holds
// nothing on it, as GetEvent does.
func (s *Service) AuthorizeEventRemoval(ctx context.Context, user *store.User, calendarID int64, existing *store.Event) (*store.Event, error) {
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
	switch acl.DecideRemoval(calendarHolding(user, cal, resourceName, entriesByPath), calendarHolding(user, cal, "", entriesByPath),
		calendarPrivilegeAllowed(user, cal, "", "unbind", entriesByPath), existing != nil) {
	case acl.RemovalAllowed:
		return existing, nil
	case acl.RemovalRefused:
		return nil, ErrForbidden
	default:
		return nil, ErrNotFound
	}
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

// calendarHolding is what user holds of acl.CalendarPrivilegeNames on the
// event resourceName names, or on the calendar when it is empty, each decided
// from entriesByPath as DAV's removal decides it. An entry that names the user
// but decides none of them refuses nothing, and the calendar's resolved
// privileges count only when the entries decide none of them.
func calendarHolding(user *store.User, cal *store.CalendarAccess, resourceName string, entriesByPath map[string][]store.ACLEntry) acl.Holding {
	if cal == nil || user == nil {
		return acl.HoldsNothing
	}
	if cal.UserID == user.ID {
		return acl.HoldsSome
	}
	principals := calendarApplicablePrincipals(user, cal.UserID)
	resourceEntries := calendarResourceEntries(cal.ID, resourceName, entriesByPath)
	collectionEntries := entriesByPath[calendarACLCollectionPath(cal.ID)]
	denied := false
	for _, privilege := range acl.CalendarPrivilegeNames {
		granted, decided := acl.DecisionThroughLayers(principals, privilege, resourceEntries, collectionEntries)
		if granted {
			return acl.HoldsSome
		}
		denied = denied || decided
	}
	switch {
	case denied:
		return acl.HoldsDenied
	case cal.EffectivePrivileges().HasAny():
		return acl.HoldsSome
	default:
		return acl.HoldsNothing
	}
}

// AuthorizeEventWrite checks privilege on the target of a write to existing:
// the event, or the calendar when the UID names none. It runs before anything
// about the event is answered. A principal who can neither read the event nor
// hold privilege on it is treated as for a UID that names nothing, and gets
// existing back as nil, so the answer cannot reveal that the event exists.
// For such a UID, a principal without privilege on the calendar gets
// ErrForbidden when they read the calendar and ErrNotFound otherwise, as
// GetEvent answers a principal who cannot read; one with it is authorized,
// and the caller answers as for a missing event.
func (s *Service) AuthorizeEventWrite(ctx context.Context, user *store.User, calendarID int64, existing *store.Event, privilege string) (*store.CalendarAccess, *store.Event, error) {
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

	resourceEntries, err := s.aclEntriesForPaths(ctx, calendarACLResourcePaths(cal.ID, resourceName))
	if err != nil {
		return false, false, err
	}
	collectionEntries, err := s.aclEntriesForPaths(ctx, []string{calendarACLCollectionPath(cal.ID)})
	if err != nil {
		return false, false, err
	}
	allowed, denied := calendarDecisionFromLayers(user, cal.UserID, privilege, resourceEntries, collectionEntries)
	return allowed, denied, nil
}

// calendarDecisionFromLayers decides privilege from the ACL of an event, when
// one is named, ahead of its calendar's. With no decision, denied reports
// whether any entry names a principal the user counts as.
func calendarDecisionFromLayers(user *store.User, ownerID int64, privilege string, resourceEntries, collectionEntries []store.ACLEntry) (allowed, denied bool) {
	principals := calendarApplicablePrincipals(user, ownerID)
	if granted, decided := acl.DecisionThroughLayers(principals, privilege, resourceEntries, collectionEntries); decided {
		return granted, !granted
	}
	return false, acl.HasApplicablePrincipal(resourceEntries, principals) || acl.HasApplicablePrincipal(collectionEntries, principals)
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

	entries, err := s.store.ACLEntries.ListByPrincipals(ctx, acl.PrincipalHrefs(user))
	if err != nil {
		return nil, err
	}
	result := make(map[string][]store.ACLEntry, len(relevantPaths))
	for _, entry := range entries {
		resourcePath := entry.ResourcePath
		if _, ok := relevantPaths[resourcePath]; !ok {
			continue
		}
		result[resourcePath] = append(result[resourcePath], entry)
	}
	for resourcePath := range result {
		acl.SortEntries(result[resourcePath])
	}
	return result, nil
}

func calendarACLCollectionPath(calendarID int64) string {
	return fmt.Sprintf("/dav/calendars/%d", calendarID)
}

// calendarACLResourcePaths lists the paths whose ACL entries make up the ACL
// of the event resourceName names: its state path, which is where DAV reads
// and writes it, or none for an empty name.
func calendarACLResourcePaths(calendarID int64, resourceName string) []string {
	if resourceName == "" {
		return nil
	}
	return []string{store.MemberStatePath(calendarACLCollectionPath(calendarID), resourceName)}
}

// writableEventName refuses a write in place to event when it is stored under
// a name store.ValidMemberName refuses, as a 1.1.x release could store one:
// its state path names the calendar, or the calendar's parent, once cleaned.
func writableEventName(event store.Event) error {
	if name := eventResourceName(event); !store.ValidMemberName(name) {
		return fmt.Errorf("%w: %q", ErrUnwritableName, name)
	}
	return nil
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

	return calendarDecisionFromLayers(user, cal.UserID, privilege, calendarResourceEntries(cal.ID, resourceName, entriesByPath), entriesByPath[calendarACLCollectionPath(cal.ID)])
}

// calendarResourceEntries is the ACL of the event resourceName names in
// calendarID from entriesByPath, or none for an empty name.
func calendarResourceEntries(calendarID int64, resourceName string, entriesByPath map[string][]store.ACLEntry) []store.ACLEntry {
	if resourceName == "" {
		return nil
	}
	return entriesByPath[store.MemberStatePath(calendarACLCollectionPath(calendarID), resourceName)]
}

// normalizeEventPayload builds the calendar object a request describes and puts
// it through the validator a DAV PUT into cal applies, so the API stores nothing
// a CalDAV client could not have written. expectedUID, when set, is the UID the
// request path names.
func (s *Service) normalizeEventPayload(input UpsertInput, expectedUID string, cal *store.CalendarAccess) (*calobject.Object, error) {
	var body string
	switch {
	case strings.TrimSpace(input.RawICS) != "":
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
	if errors.Is(refusal, calobject.ErrUIDTooLong) {
		return fmt.Errorf("%w: uid must be at most %d octets", ErrBadRequest, store.MaxIdentifierOctets)
	}
	return fmt.Errorf("%w: %s", ErrBadRequest, conditions)
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

// checkConditionalHeaders evaluates a request's If-Match and If-None-Match
// against the event it writes, which is nil where there is none.
func checkConditionalHeaders(ifMatch, ifNoneMatch string, existing *store.Event) bool {
	etag := ""
	if existing != nil {
		etag = existing.ETag
	}
	return httpcond.Check(ifMatch, ifNoneMatch, etag, existing != nil)
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
	case errors.Is(err, ErrLocked):
		return http.StatusLocked
	case errors.Is(err, ErrConflict):
		return http.StatusConflict
	case errors.Is(err, ErrPreconditionFailed):
		return http.StatusPreconditionFailed
	case errors.Is(err, ErrBadRequest):
		return http.StatusBadRequest
	default:
		return http.StatusInternalServerError
	}
}
