package ui

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/go-chi/chi/v5"

	"github.com/jw6ventures/calcard/internal/acl"
	"github.com/jw6ventures/calcard/internal/auth"
	"github.com/jw6ventures/calcard/internal/store"
)

// serveCalendar has the sharee post form to handle with the route parameters
// params.
func (f *uiEventFixture) serveCalendar(handle http.HandlerFunc, params map[string]string, form url.Values) *httptest.ResponseRecorder {
	f.t.Helper()
	return f.serveCalendarAs(f.sharee, handle, params, form)
}

// serveCalendarAs is serveCalendar for a post by user.
func (f *uiEventFixture) serveCalendarAs(user *store.User, handle http.HandlerFunc, params map[string]string, form url.Values) *httptest.ResponseRecorder {
	f.t.Helper()
	req := httptest.NewRequest(http.MethodPost, "/calendars/x", strings.NewReader(form.Encode()))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	routeContext := chi.NewRouteContext()
	for key, value := range params {
		routeContext.URLParams.Add(key, value)
	}
	req = req.WithContext(context.WithValue(auth.WithUser(req.Context(), user), chi.RouteCtxKey, routeContext))
	rr := httptest.NewRecorder()
	handle(rr, req)
	return rr
}

// calendarWrite is a calendar-level UI write with the CalDAV request making
// the same change.
type calendarWrite struct {
	name string
	// refusedBy lists the lock layouts that refuse it, as DAV refuses its
	// counterpart.
	refusedBy []string
	flash     string
	run       func(f *uiEventFixture, calendarID int64) *httptest.ResponseRecorder
	dav       func(f *uiEventFixture, calendarID int64) *httptest.ResponseRecorder
	// changed reports whether the write took effect.
	changed func(f *uiEventFixture, calendarID int64) bool
}

func calendarWrites(other *store.User) []calendarWrite {
	calendarParams := func(id int64) map[string]string { return map[string]string{"id": strconv.FormatInt(id, 10)} }
	aclBody := func(privilege string) string {
		return `<?xml version="1.0"?><d:acl xmlns:d="DAV:"><d:ace><d:principal><d:href>` + acl.PrincipalHref(other.ID) +
			`</d:href></d:principal><d:grant><d:privilege><d:` + privilege + `/></d:privilege></d:grant></d:ace></d:acl>`
	}
	shared := func(f *uiEventFixture, calendarID int64) bool {
		entries, err := f.database.ACLEntries.ListByResource(f.t.Context(), shareeCalendarPath(calendarID))
		if err != nil {
			f.t.Fatal(err)
		}
		for _, entry := range entries {
			if acl.NormalizePrincipalHref(entry.PrincipalHref) == acl.PrincipalHref(other.ID) {
				return true
			}
		}
		return false
	}
	return []calendarWrite{
		{
			name: "delete", refusedBy: []string{"the calendar, depth 0", "the calendar, depth infinity", "an event in it"}, flash: calendarDeleteLockedFlash,
			run: func(f *uiEventFixture, id int64) *httptest.ResponseRecorder {
				return f.serveCalendar(f.handler.DeleteCalendar, calendarParams(id), nil)
			},
			dav: func(f *uiEventFixture, id int64) *httptest.ResponseRecorder {
				return f.davRequest(f.sharee, http.MethodDelete, calendarHref(id), nil, "")
			},
			changed: func(f *uiEventFixture, id int64) bool {
				cal, err := f.database.Calendars.GetByID(f.t.Context(), id)
				if err != nil {
					f.t.Fatal(err)
				}
				return cal == nil
			},
		},
		{
			name: "rename", refusedBy: []string{"the calendar, depth 0", "the calendar, depth infinity"}, flash: calendarLockedFlash,
			run: func(f *uiEventFixture, id int64) *httptest.ResponseRecorder {
				return f.serveCalendar(f.handler.RenameCalendar, calendarParams(id), url.Values{"name": {"Renamed"}, "color": {"#112233"}})
			},
			dav: func(f *uiEventFixture, id int64) *httptest.ResponseRecorder {
				return f.davRequest(f.sharee, "PROPPATCH", calendarHref(id), nil,
					`<?xml version="1.0"?><d:propertyupdate xmlns:d="DAV:"><d:set><d:prop><d:displayname>Renamed</d:displayname></d:prop></d:set></d:propertyupdate>`)
			},
			changed: func(f *uiEventFixture, id int64) bool {
				cal, err := f.database.Calendars.GetByID(f.t.Context(), id)
				if err != nil || cal == nil {
					f.t.Fatalf("calendar %d: %v, %v", id, cal, err)
				}
				return cal.Name == "Renamed"
			},
		},
		{
			name: "share", refusedBy: []string{"the calendar, depth 0", "the calendar, depth infinity"}, flash: calendarLockedFlash,
			run: func(f *uiEventFixture, id int64) *httptest.ResponseRecorder {
				return f.serveCalendar(f.handler.ShareCalendar, calendarParams(id), url.Values{"user_id": {strconv.FormatInt(other.ID, 10)}})
			},
			dav: func(f *uiEventFixture, id int64) *httptest.ResponseRecorder {
				return f.davRequest(f.sharee, "ACL", calendarHref(id), nil, aclBody("read"))
			},
			changed: shared,
		},
	}
}

// A calendar-level write from the UI is refused by the locks that refuse its
// CalDAV counterpart (DELETE, PROPPATCH, ACL), without a lock token, and the
// refusal leaves the calendar and the lock as they were. Unsharing and leaving
// only remove grants and are not refused (see TestPostgres_UICalendarRevocationIgnoresLocks).
func TestPostgres_UICalendarWritesRefuseWhatDAVRefusesForWantOfALockToken(t *testing.T) {
	f := newUIEventFixture(t)
	layouts := []struct {
		name string
		lock func(f *uiEventFixture, calendarID int64) string
	}{
		{"the calendar, depth 0", func(f *uiEventFixture, id int64) string { return f.davLock(f.sharee, calendarHref(id), "0", "") }},
		{"the calendar, depth infinity", func(f *uiEventFixture, id int64) string { return f.davLock(f.sharee, calendarHref(id), "infinity", "") }},
		{"an event in it", func(f *uiEventFixture, id int64) string {
			f.putEvent(id, "member", eventObject(singleEvent, "member"))
			return f.davLock(f.sharee, eventHref(id, "member"), "0", "")
		}},
	}
	for _, write := range calendarWrites(f.owner) {
		for _, layout := range layouts {
			t.Run(write.name+": "+layout.name, func(t *testing.T) {
				setup := func(role string) (int64, string) {
					calendar := f.calendarOf(f.sharee, slug("Locked "+write.name+layout.name+role))
					return calendar.ID, layout.lock(f, calendar.ID)
				}
				davID, _ := setup("dav")
				refused := write.dav(f, davID).Code == http.StatusLocked
				want := false
				for _, name := range write.refusedBy {
					want = want || name == layout.name
				}
				if refused != want {
					t.Fatalf("the CalDAV request answered locked = %t, want %t: the table does not describe DAV", refused, want)
				}

				id, token := setup("ui")
				rr := write.run(f, id)
				if refused {
					if got := flashedError(t, rr); got != write.flash {
						t.Errorf("flashed error = %q, want %q", got, write.flash)
					}
					if write.changed(f, id) {
						t.Errorf("the write was applied")
					}
				} else if write.changed(f, id) == false || flashedErrorOf(rr) != "" {
					t.Errorf("the write was refused: %q", flashedErrorOf(rr))
				}
				if !refused {
					return
				}
				if !f.lockHeld(token) {
					t.Errorf("the refused write dropped the lock")
				}
			})
		}
	}
}

// An expired lock refuses a calendar-level write no more than it refuses a
// CalDAV request.
func TestPostgres_UICalendarWritesIgnoreAnExpiredLock(t *testing.T) {
	f := newUIEventFixture(t)
	for _, write := range calendarWrites(f.owner) {
		t.Run(write.name, func(t *testing.T) {
			calendar := f.calendarOf(f.sharee, "Expired "+write.name)
			f.davLock(f.sharee, calendarHref(calendar.ID), "0", "Second-1")
			time.Sleep(1200 * time.Millisecond)

			rr := write.run(f, calendar.ID)

			if got := flashedErrorOf(rr); got != "" {
				t.Errorf("an expired lock refused the write: %q", got)
			}
			if !write.changed(f, calendar.ID) {
				t.Errorf("the write was not applied")
			}
		})
	}
}

// A lock taken after the UI decided a calendar-level write and before it is
// stored refuses the write.
func TestPostgres_UICalendarWriteRefusesALockTakenBeforeIt(t *testing.T) {
	f := newUIEventFixture(t)
	for _, write := range calendarWrites(f.owner) {
		t.Run(write.name, func(t *testing.T) {
			calendar := f.calendarOf(f.sharee, "Racing "+write.name)
			var locked *httptest.ResponseRecorder
			f.database.Calendars = &afterGetCalendars{CalendarRepository: f.database.Calendars, after: func() {
				locked = f.davRequest(f.sharee, "LOCK", calendarHref(calendar.ID), map[string]string{"Depth": "0"}, exclusiveWriteLock)
			}}
			t.Cleanup(func() { f.database.Calendars = f.database.Calendars.(*afterGetCalendars).CalendarRepository })

			rr := write.run(f, calendar.ID)

			if got := flashedError(t, rr); got != write.flash {
				t.Errorf("flashed error = %q, want %q", got, write.flash)
			}
			if write.changed(f, calendar.ID) {
				t.Errorf("the write was applied")
			}
			if locked == nil || locked.Code != http.StatusOK {
				t.Fatalf("the lock was not taken: %+v", locked)
			}
			if !f.lockHeld(strings.Trim(locked.Header().Get("Lock-Token"), "<>")) {
				t.Errorf("the refused write dropped the lock")
			}
		})
	}
}

// afterGetCalendars runs after once the handler has read the calendar it is
// about to write.
type afterGetCalendars struct {
	store.CalendarRepository
	after func()
}

func (c *afterGetCalendars) GetByID(ctx context.Context, id int64) (*store.Calendar, error) {
	cal, err := c.CalendarRepository.GetByID(ctx, id)
	if c.after != nil {
		after := c.after
		c.after = nil
		after()
	}
	return cal, err
}

func (c *afterGetCalendars) GetAccessible(ctx context.Context, calendarID, userID int64) (*store.CalendarAccess, error) {
	cal, err := c.CalendarRepository.GetAccessible(ctx, calendarID, userID)
	if c.after != nil {
		after := c.after
		c.after = nil
		after()
	}
	return cal, err
}

func shareeCalendarPath(id int64) string { return fmt.Sprintf("/dav/calendars/%d", id) }

// Deleting a calendar removes the DAV state bound beneath it -- the ACL entries
// of the calendar and its events, and the locks and dead properties -- as a
// CalDAV DELETE of the collection does.
func TestPostgres_UICalendarDeleteRemovesTheStateBeneathIt(t *testing.T) {
	f := newUIEventFixture(t)
	calendar := f.calendarOf(f.sharee, "Doomed")
	f.putEvent(calendar.ID, "member", eventObject(singleEvent, "member"))
	grant := []store.ACLEntry{{PrincipalHref: acl.PrincipalHref(f.owner.ID), IsGrant: true, Privilege: "read"}}
	f.setACL(shareeCalendarPath(calendar.ID), grant)
	f.setACL(store.MemberStatePath(shareeCalendarPath(calendar.ID), "member"), grant)

	rr := f.serveCalendar(f.handler.DeleteCalendar, map[string]string{"id": strconv.FormatInt(calendar.ID, 10)}, nil)

	if got := flashedErrorOf(rr); got != "" {
		t.Fatalf("the delete was refused: %q", got)
	}
	for _, resourcePath := range []string{shareeCalendarPath(calendar.ID), store.MemberStatePath(shareeCalendarPath(calendar.ID), "member")} {
		if entries, err := f.database.ACLEntries.ListByResource(t.Context(), resourcePath); err != nil || len(entries) != 0 {
			t.Errorf("ACL entries left at %s: %v, %v", resourcePath, entries, err)
		}
	}
}

// Deleting a calendar that CalDAV clients on other replicas keep writing to
// holds the calendar while it removes it, so the writers queue behind the
// delete instead of making it give up, and nothing they wrote outlives it.
func TestPostgres_UICalendarDeleteUnderContinuousMemberWrites(t *testing.T) {
	const writers = 4
	f := newUIEventFixture(t)
	calendar := f.calendarOf(f.sharee, "Doomed")
	root := shareeCalendarPath(calendar.ID)
	var ops atomic.Int64
	var mu sync.Mutex
	var serverErrors []string
	stop := make(chan struct{})
	var wg sync.WaitGroup
	for g := 0; g < writers; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			header := map[string]string{"Content-Type": "text/calendar"}
			for k := 0; ; k++ {
				select {
				case <-stop:
					return
				default:
				}
				uid := fmt.Sprintf("w%d-%d", g, k)
				href := root + "/" + uid + ".ics"
				for _, rr := range []*httptest.ResponseRecorder{
					f.davRequest(f.sharee, http.MethodPut, href, header, eventObject(singleEvent, uid)),
					f.davRequest(f.sharee, http.MethodDelete, href, nil, ""),
				} {
					if rr.Code >= 500 {
						mu.Lock()
						serverErrors = append(serverErrors, fmt.Sprintf("%s = %d %s", uid, rr.Code, rr.Body.String()))
						mu.Unlock()
					}
				}
				f.davRequest(f.sharee, http.MethodPut, root+"/"+uid+"-kept.ics", header, eventObject(singleEvent, uid+"-kept"))
				ops.Add(1)
			}
		}(g)
	}
	waitForOps := func(n int64) {
		target := ops.Load() + n
		deadline := time.Now().Add(30 * time.Second)
		for ops.Load() < target {
			if time.Now().After(deadline) {
				t.Fatalf("writers made %d operations, want %d", ops.Load(), target)
			}
			time.Sleep(time.Millisecond)
		}
	}
	waitForOps(writers * 10)
	rr := f.serveCalendar(f.handler.DeleteCalendar, map[string]string{"id": strconv.FormatInt(calendar.ID, 10)}, nil)
	waitForOps(writers * 10)
	close(stop)
	wg.Wait()

	if got := flashedErrorOf(rr); got != "" {
		t.Fatalf("the delete was refused: %q", got)
	}
	for _, failure := range serverErrors {
		t.Errorf("member write failed on the server: %s", failure)
	}
	if remains, err := f.database.Calendars.GetByID(t.Context(), calendar.ID); err == nil && remains != nil {
		t.Fatalf("calendar remains after its delete: %#v", remains)
	}
	if events, err := f.database.Events.ListForCalendar(t.Context(), calendar.ID); err != nil || len(events) != 0 {
		t.Fatalf("%d members outlive their calendar: %v", len(events), err)
	}
}

// Removing a principal's access only removes grants, so no lock refuses it. An
// owner who unshares a sharee who has locked the calendar also drops that
// sharee's lock, which they have no access to keep, and keeps their own; a
// sharee leaving a calendar somebody else locked leaves that lock where it is.
func TestPostgres_UICalendarRevocationIgnoresLocks(t *testing.T) {
	f := newUIEventFixture(t)
	newShared := func(uid string) int64 {
		return f.calendarWithEvent(uid, eventObject(singleEvent, uid), f.shareeACE(true, "read", "write"), nil)
	}

	t.Run("the owner revokes a sharee who locked the calendar", func(t *testing.T) {
		calendarID := newShared("revoked-sharee-lock")
		ownerLock := f.davLock(f.owner, eventHref(calendarID, "revoked-sharee-lock"), "0", "")
		shareeLock := f.davLock(f.sharee, calendarHref(calendarID), "0", "Second-604800")
		params := map[string]string{"id": fmt.Sprint(calendarID), "userId": fmt.Sprint(f.sharee.ID)}

		rr := f.serveCalendarAs(f.owner, f.handler.UnshareCalendar, params, nil)

		if got := flashedErrorOf(rr); got != "" {
			t.Errorf("the revocation was refused: %q", got)
		}
		if entries, err := f.database.ACLEntries.ListByResource(t.Context(), shareeCalendarPath(calendarID)); err != nil || len(entries) != 0 {
			t.Errorf("the sharee's grants remain: %v, %v", entries, err)
		}
		if f.lockHeld(shareeLock) {
			t.Error("the revoked sharee's lock was kept")
		}
		if !f.lockHeld(ownerLock) {
			t.Error("the owner's lock was dropped")
		}
	})

	t.Run("a sharee leaves a calendar the owner locked", func(t *testing.T) {
		calendarID := newShared("left-owner-lock")
		ownerLock := f.davLock(f.owner, calendarHref(calendarID), "0", "")
		params := map[string]string{"id": fmt.Sprint(calendarID), "userId": fmt.Sprint(f.sharee.ID)}

		rr := f.serveCalendarAs(f.sharee, f.handler.UnshareCalendar, params, nil)

		if got := flashedErrorOf(rr); got != "" {
			t.Errorf("leaving was refused: %q", got)
		}
		if entries, err := f.database.ACLEntries.ListByResource(t.Context(), shareeCalendarPath(calendarID)); err != nil || len(entries) != 0 {
			t.Errorf("the sharee's grants remain: %v, %v", entries, err)
		}
		if !f.lockHeld(ownerLock) {
			t.Error("the owner's lock was dropped")
		}
	})
}

// A rename writes the name and the colour and nothing else: a property a client
// set after the form was read is kept.
func TestPostgres_UICalendarRenameKeepsAPropertySetMeanwhile(t *testing.T) {
	f := newUIEventFixture(t)
	calendar := f.calendarOf(f.sharee, "Racing rename")
	inner := f.database.Calendars
	f.database.Calendars = &afterGetCalendars{CalendarRepository: inner, after: func() {
		description := "set by a client"
		timezone := "Europe/Berlin"
		if err := inner.UpdateProperties(f.t.Context(), calendar.ID, store.CalendarProperties{Name: calendar.Name, Description: &description, Timezone: &timezone}); err != nil {
			f.t.Error(err)
		}
	}}
	t.Cleanup(func() { f.database.Calendars = inner })

	rr := f.serveCalendar(f.handler.RenameCalendar, map[string]string{"id": fmt.Sprint(calendar.ID)}, url.Values{"name": {"Renamed"}})

	if got := flashedErrorOf(rr); got != "" {
		t.Fatalf("the rename was refused: %q", got)
	}
	got, err := inner.GetByID(t.Context(), calendar.ID)
	if err != nil || got == nil {
		t.Fatalf("calendar: %v, %v", got, err)
	}
	if got.Name != "Renamed" {
		t.Errorf("name = %q, want Renamed", got.Name)
	}
	if got.Description == nil || *got.Description != "set by a client" || got.Timezone == nil || *got.Timezone != "Europe/Berlin" {
		t.Errorf("description %v and timezone %v after the rename, want the ones set meanwhile", got.Description, got.Timezone)
	}
}

// A share that lands after the calendar was deleted is answered as for a
// calendar that is not there, and writes no ACL entries.
func TestPostgres_UICalendarShareAfterDeleteLeavesNoACL(t *testing.T) {
	f := newUIEventFixture(t)
	calendar := f.calendarOf(f.sharee, "Doomed")
	inner := f.database.Calendars
	f.database.Calendars = &afterGetCalendars{CalendarRepository: inner, after: func() {
		if err := inner.Delete(f.t.Context(), f.sharee.ID, calendar.ID); err != nil {
			f.t.Error(err)
		}
	}}
	t.Cleanup(func() { f.database.Calendars = inner })

	rr := f.serveCalendar(f.handler.ShareCalendar, map[string]string{"id": fmt.Sprint(calendar.ID)}, url.Values{"user_id": {fmt.Sprint(f.owner.ID)}})

	if got := flashedError(t, rr); got != "not found" {
		t.Errorf("flashed error = %q, want the answer for a calendar that is not there", got)
	}
	if entries, err := f.database.ACLEntries.ListByResource(t.Context(), shareeCalendarPath(calendar.ID)); err != nil || len(entries) != 0 {
		t.Errorf("%d ACL entries remain for the deleted calendar: %v", len(entries), err)
	}
}
