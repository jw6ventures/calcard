package dav

import (
	"context"
	"sync"

	"github.com/jw6ventures/calcard/internal/ical"
	"github.com/jw6ventures/calcard/internal/store"
)

// reportWorkBudget bounds the recurrence and observance work every evaluation
// and reading of one REPORT may spend together, on top of each evaluation's own
// per-call cap inside ical (2,000,000 units, a few tenths of a second at its
// worst). Measured on thirty resources whose zones each cost several million
// units to follow, a calendar-query, an expansion and a free-busy report each
// end after about 2 seconds with it, where without it a calendar-query takes 9
// seconds and a free-busy report 21. A calendar of a thousand ordinary
// recurring resources, which share their zone's rules, is answered exactly,
// so no ordinary REPORT is touched. The UI's events listing
// (eventsJSONWorkBudget) is sized the same.
const reportWorkBudget = 20_000_000

// transferMemberWork is the recurrence and observance work each member of a
// collection COPY or MOVE adds to the pool its validation draws on. An ordinary
// member costs the pool between nothing and about 2,200 units however its series
// recurs, measured with the zone's rules already listed: a daily series of two
// years or a weekday series of 900 instances in America/New_York or an
// Outlook-style zone take 2,200 and 2,000, an unbounded daily one 1,100, and an
// hourly series of 1,000 instances 2,000. 10,000 leaves the costliest of them
// more than four times its cost.
const transferMemberWork = 10_000

// transferWorkBudget is the pool one attempt of a transfer of members members
// validates them on: what a single object may cost, ical.ObjectWorkBudget, so
// that any one member PUT accepted can be transferred however cold the zone
// listings are, and the allowance of every member besides. A collection of
// ordinary members therefore never runs the pool dry, whatever its size, and one
// of hostile members costs the attempt that many allowances and one object's
// worth more. Each attempt validates on a pool of its own, and revalidates the
// members that changed after it read them on another sized to those members
// alone; a transfer the store sends back for another attempt makes at most
// 1 + maxResourceStateRetries attempts.
func transferWorkBudget(members int) int {
	return ical.ObjectWorkBudget + members*transferMemberWork
}

type davRequestState struct {
	aclEntries *aclEntryCache

	mu                    sync.Mutex
	lockIndex             *lockBatchIndex
	collectionResolutions map[collectionResolutionKey]collectionResolutionResult
	calendars             map[int64]*store.Calendar
	addressBooks          map[int64]*store.AddressBook
	calendarAccess        map[calendarAccessKey]*store.CalendarAccess
	primaryTarget         *davTarget
	ifHeader              *ifHeaderState
	// work is the recurrence and observance budget shared by every
	// evaluation and reading that answers this request, created the first
	// time one is asked for and kept for the rest of the request.
	work *ical.RequestWork
}

// calendarAccessKey names one store-side resolution of a user's access to a
// calendar, which depends on nothing else.
type calendarAccessKey struct {
	calendarID int64
	userID     int64
}

type collectionResolutionKey struct {
	userID  int64
	prefix  string
	segment string
}

type collectionResolutionResult struct {
	id  int64
	ok  bool
	err error
}

type davRequestStateKeyType struct{}

var davRequestStateKey = davRequestStateKeyType{}

func withDAVRequestState(ctx context.Context) context.Context {
	if davRequestStateFromContext(ctx) != nil {
		return ctx
	}
	return context.WithValue(ctx, davRequestStateKey, &davRequestState{
		aclEntries:            &aclEntryCache{entries: make(map[string][]store.ACLEntry)},
		collectionResolutions: make(map[collectionResolutionKey]collectionResolutionResult),
		calendars:             make(map[int64]*store.Calendar),
		addressBooks:          make(map[int64]*store.AddressBook),
		calendarAccess:        make(map[calendarAccessKey]*store.CalendarAccess),
	})
}

func (s *davRequestState) calendar(id int64) (*store.Calendar, bool) {
	if s == nil {
		return nil, false
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	calendar, ok := s.calendars[id]
	return calendar, ok
}

func (s *davRequestState) putCalendar(calendar *store.Calendar) {
	if s == nil || calendar == nil {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	copy := *calendar
	s.calendars[calendar.ID] = &copy
}

// memoizedCalendarAccess returns the memoized resolution for key; a nil access
// with ok set records that the user has none.
func (s *davRequestState) memoizedCalendarAccess(key calendarAccessKey) (*store.CalendarAccess, bool) {
	if s == nil {
		return nil, false
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	access, ok := s.calendarAccess[key]
	if access == nil {
		return nil, ok
	}
	copy := *access
	return &copy, ok
}

func (s *davRequestState) memoizeCalendarAccess(key calendarAccessKey, access *store.CalendarAccess) {
	if s == nil {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if access == nil {
		s.calendarAccess[key] = nil
		return
	}
	copy := *access
	s.calendarAccess[key] = &copy
}

func (s *davRequestState) addressBook(id int64) (*store.AddressBook, bool) {
	if s == nil {
		return nil, false
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	book, ok := s.addressBooks[id]
	return book, ok
}

func (s *davRequestState) putAddressBook(book *store.AddressBook) {
	if s == nil || book == nil {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	copy := *book
	s.addressBooks[book.ID] = &copy
}

func davRequestStateFromContext(ctx context.Context) *davRequestState {
	state, _ := ctx.Value(davRequestStateKey).(*davRequestState)
	return state
}

func (s *davRequestState) setLockIndex(index *lockBatchIndex) {
	if s == nil {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.lockIndex = index
}

func (s *davRequestState) currentLockIndex() *lockBatchIndex {
	if s == nil {
		return nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.lockIndex
}

func (s *davRequestState) collectionResolution(key collectionResolutionKey) (collectionResolutionResult, bool) {
	if s == nil {
		return collectionResolutionResult{}, false
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	result, ok := s.collectionResolutions[key]
	return result, ok
}

func (s *davRequestState) putCollectionResolution(key collectionResolutionKey, result collectionResolutionResult) {
	if s == nil {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.collectionResolutions[key] = result
}

func (s *davRequestState) invalidateCollectionResolutions() {
	if s == nil {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.collectionResolutions = make(map[collectionResolutionKey]collectionResolutionResult)
}

// parsedIfHeader returns the request's If header as parseIfHeader read it, or
// nil before it has. The header does not change during a request, so this
// survives invalidateDAVRequestState.
func (s *davRequestState) parsedIfHeader() *ifHeaderState {
	if s == nil {
		return nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.ifHeader
}

func (s *davRequestState) setParsedIfHeader(state *ifHeaderState) {
	if s == nil {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.ifHeader = state
}

func (s *davRequestState) setPrimaryDAVTarget(target davTarget) {
	if s == nil {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	copy := target
	s.primaryTarget = &copy
}

func (s *davRequestState) primaryDAVTarget(cleanPath string) (davTarget, bool) {
	if s == nil {
		return davTarget{}, false
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.primaryTarget == nil || s.primaryTarget.CleanPath != cleanPath {
		return davTarget{}, false
	}
	return *s.primaryTarget, true
}

// requestWork returns the request's recurrence and observance budget,
// creating it on ctx with units the first time it is asked for and returning
// that same pool on every later call this request makes, whatever ctx or
// units they pass: a request answers one REPORT, so only its first caller's
// sizing applies. A nil state (possible
// only outside a DAV request, such as a test building its own tree) answers
// nil, which every pool-aware entry point treats as "no pool".
func (s *davRequestState) requestWork(ctx context.Context, units int) *ical.RequestWork {
	if s == nil {
		return nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.work == nil {
		s.work = ical.NewRequestWork(ctx, units)
	}
	return s.work
}

func invalidateDAVRequestState(ctx context.Context) {
	state := davRequestStateFromContext(ctx)
	if state == nil {
		return
	}
	state.aclEntries.invalidate()
	state.invalidateCollectionResolutions()
	state.mu.Lock()
	state.calendars = make(map[int64]*store.Calendar)
	state.addressBooks = make(map[int64]*store.AddressBook)
	state.calendarAccess = make(map[calendarAccessKey]*store.CalendarAccess)
	state.mu.Unlock()
	if index := state.currentLockIndex(); index != nil {
		index.markStale()
	}
}
