package events

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jw6ventures/calcard/internal/acl"
	"github.com/jw6ventures/calcard/internal/auth"
	"github.com/jw6ventures/calcard/internal/calobject"
	"github.com/jw6ventures/calcard/internal/dav"
	"github.com/jw6ventures/calcard/internal/store"
	"github.com/jw6ventures/calcard/internal/ui/utils"
)

// writeSafetyFixture is a calendar of the owner's on a PostgreSQL store, with a
// sharee whose access the tests grant and withdraw.
type writeSafetyFixture struct {
	t          *testing.T
	database   *store.Store
	owner      *store.User
	sharee     *store.User
	calendar   *store.Calendar
	collection string
}

func newWriteSafetyFixture(t *testing.T) *writeSafetyFixture {
	t.Helper()
	database := newEventsPostgresStore(t)
	newUser := func(name string) *store.User {
		t.Helper()
		user, err := database.Users.UpsertOAuthUser(t.Context(), name, name+"@example.test", name, name)
		if err != nil {
			t.Fatalf("create user %s: %v", name, err)
		}
		return user
	}
	f := &writeSafetyFixture{t: t, database: database, owner: newUser("owner"), sharee: newUser("sharee")}
	cal, err := database.Calendars.Create(t.Context(), store.Calendar{UserID: f.owner.ID, Name: "Shared"})
	if err != nil {
		t.Fatalf("create calendar: %v", err)
	}
	f.calendar = cal
	f.collection = fmt.Sprintf("/dav/calendars/%d", cal.ID)
	return f
}

func (f *writeSafetyFixture) shareeACL(grant bool, privileges ...string) []store.ACLEntry {
	entries := make([]store.ACLEntry, 0, len(privileges))
	for i, privilege := range privileges {
		entries = append(entries, store.ACLEntry{PrincipalHref: acl.PrincipalHref(f.sharee.ID), IsGrant: grant, Privilege: privilege, Position: i})
	}
	return entries
}

func (f *writeSafetyFixture) setACL(resourcePath string, entries []store.ACLEntry) {
	f.t.Helper()
	if err := f.database.ACLEntries.SetACL(f.t.Context(), resourcePath, entries); err != nil {
		f.t.Fatalf("set the ACL of %s: %v", resourcePath, err)
	}
}

// putEvent stores body as the event uid the way a CalDAV PUT does.
func (f *writeSafetyFixture) putEvent(uid, body string) *store.Event {
	f.t.Helper()
	event, err := f.database.Events.Upsert(f.t.Context(), store.Event{
		CalendarID: f.calendar.ID, UID: uid, ResourceName: uid, RawICAL: body, ETag: utils.GenerateETag(body),
	})
	if err != nil {
		f.t.Fatalf("store event %s: %v", uid, err)
	}
	return event
}

func (f *writeSafetyFixture) stored(uid string) *store.Event {
	f.t.Helper()
	event, err := f.database.Events.GetByUID(f.t.Context(), f.calendar.ID, uid)
	if err != nil {
		f.t.Fatalf("load event %s: %v", uid, err)
	}
	return event
}

func (f *writeSafetyFixture) remove(uid string) {
	f.t.Helper()
	if err := f.database.Events.DeleteByUID(f.t.Context(), f.calendar.ID, uid); err != nil {
		f.t.Fatalf("remove event %s: %v", uid, err)
	}
}

func summaryICS(uid, summary string) string {
	return strings.Replace(validICS(uid), "SUMMARY:Planning", "SUMMARY:"+summary, 1)
}

// serviceRunning is a service whose validator, which every create and update
// passes the event through after deciding it and before storing it, first runs
// before: the moment another client's write lands in the gap between the two.
func (f *writeSafetyFixture) serviceRunning(before func()) *Service {
	var once sync.Once
	return NewService(f.database, func(raw string, cal *store.CalendarAccess) (*calobject.Object, error) {
		once.Do(before)
		return dav.ValidateCalendarObject(raw, cal)
	})
}

// decisionHookACL runs after once, right after the decision to remove an event
// reads the calendar's own ACL, which it does last.
type decisionHookACL struct {
	store.ACLRepository
	collectionPath string
	after          func()
}

func (a *decisionHookACL) ListByResource(ctx context.Context, resourcePath string) ([]store.ACLEntry, error) {
	entries, err := a.ACLRepository.ListByResource(ctx, resourcePath)
	if resourcePath == a.collectionPath && a.after != nil {
		after := a.after
		a.after = nil
		after()
	}
	return entries, err
}

// serviceRunningAfterDecision is a service whose removal of an event runs after
// once its decision has read the calendar's ACL.
func (f *writeSafetyFixture) serviceRunningAfterDecision(after func()) *Service {
	f.database.ACLEntries = &decisionHookACL{ACLRepository: f.database.ACLEntries, collectionPath: f.collection, after: after}
	return NewService(f.database, dav.ValidateCalendarObject)
}

// An update replaces the event it read. An event another client removes
// before the write is not brought back, and the update is answered as for an
// event that is not there.
func TestPostgres_RESTUpdateDoesNotRecreateARemovedEvent(t *testing.T) {
	f := newWriteSafetyFixture(t)
	f.putEvent("gone", validICS("gone"))
	svc := f.serviceRunning(func() { f.remove("gone") })

	_, _, err := svc.UpdateEvent(t.Context(), f.owner, f.calendar.ID, "gone", UpsertInput{RawICS: summaryICS("gone", "Revived")})
	if !errors.Is(err, ErrNotFound) {
		t.Fatalf("UpdateEvent() of an event removed meanwhile error = %v, want ErrNotFound", err)
	}
	if f.stored("gone") != nil {
		t.Fatal("the update brought the removed event back")
	}
}

// The write is checked under the ACL it was decided on: access withdrawn
// between the decision and the write fails the write, and the decision made
// again on the new ACL refuses it.
func TestPostgres_RESTWritesFailWhenAccessIsWithdrawnAfterTheDecision(t *testing.T) {
	grantWrite := func(f *writeSafetyFixture) {
		f.setACL(f.collection, f.shareeACL(true, "read", "write-content", "bind", "unbind"))
	}
	withdraw := func(f *writeSafetyFixture) {
		f.setACL(f.collection, f.shareeACL(true, "read"))
	}

	t.Run("update", func(t *testing.T) {
		f := newWriteSafetyFixture(t)
		before := f.putEvent("shared", validICS("shared"))
		grantWrite(f)
		svc := f.serviceRunning(func() { withdraw(f) })

		_, _, err := svc.UpdateEvent(t.Context(), f.sharee, f.calendar.ID, "shared", UpsertInput{RawICS: summaryICS("shared", "Edited")})
		if !errors.Is(err, ErrForbidden) {
			t.Fatalf("UpdateEvent() after the grant was withdrawn error = %v, want ErrForbidden", err)
		}
		if got := f.stored("shared"); got == nil || got.ETag != before.ETag {
			t.Fatalf("the event was written after the grant was withdrawn: %+v", got)
		}
	})

	t.Run("event-level grant", func(t *testing.T) {
		f := newWriteSafetyFixture(t)
		before := f.putEvent("shared", validICS("shared"))
		f.setACL(store.MemberStatePath(f.collection, "shared"), f.shareeACL(true, "read", "write-content"))
		svc := f.serviceRunning(func() { f.setACL(store.MemberStatePath(f.collection, "shared"), f.shareeACL(true, "read")) })

		_, _, err := svc.UpdateEvent(t.Context(), f.sharee, f.calendar.ID, "shared", UpsertInput{RawICS: summaryICS("shared", "Edited")})
		if !errors.Is(err, ErrForbidden) {
			t.Fatalf("UpdateEvent() after the event's grant was withdrawn error = %v, want ErrForbidden", err)
		}
		if got := f.stored("shared"); got == nil || got.ETag != before.ETag {
			t.Fatalf("the event was written after the grant was withdrawn: %+v", got)
		}
	})

	t.Run("delete", func(t *testing.T) {
		f := newWriteSafetyFixture(t)
		f.putEvent("shared", validICS("shared"))
		grantWrite(f)
		svc := f.serviceRunningAfterDecision(func() { withdraw(f) })

		err := svc.DeleteEvent(t.Context(), f.sharee, f.calendar.ID, "shared", "", "")
		if !errors.Is(err, ErrForbidden) {
			t.Fatalf("DeleteEvent() after the grant was withdrawn error = %v, want ErrForbidden", err)
		}
		if f.stored("shared") == nil {
			t.Fatal("the event was removed after the grant was withdrawn")
		}
	})

	t.Run("create", func(t *testing.T) {
		f := newWriteSafetyFixture(t)
		grantWrite(f)
		svc := f.serviceRunning(func() { withdraw(f) })

		_, err := svc.CreateEvent(t.Context(), f.sharee, f.calendar.ID, UpsertInput{RawICS: validICS("fresh")})
		if !errors.Is(err, ErrForbidden) {
			t.Fatalf("CreateEvent() after the grant was withdrawn error = %v, want ErrForbidden", err)
		}
		if f.stored("fresh") != nil {
			t.Fatal("the event was created after the grant was withdrawn")
		}
	})
}

// A request's If-Match is checked against the version stored when the write
// lands. A version another client stored in between fails it, and the other
// client's version is kept.
func TestPostgres_RESTUpdateIfMatchHoldsUnderTheWrite(t *testing.T) {
	f := newWriteSafetyFixture(t)
	opened := f.putEvent("contested", validICS("contested"))
	theirs := summaryICS("contested", "Theirs")
	svc := f.serviceRunning(func() { f.putEvent("contested", theirs) })

	_, _, err := svc.UpdateEvent(t.Context(), f.owner, f.calendar.ID, "contested", UpsertInput{
		RawICS: summaryICS("contested", "Mine"), IfMatch: `"` + opened.ETag + `"`,
	})
	if !errors.Is(err, ErrPreconditionFailed) {
		t.Fatalf("UpdateEvent() with a stale If-Match error = %v, want ErrPreconditionFailed", err)
	}
	if got := f.stored("contested"); got == nil || got.RawICAL != theirs {
		t.Fatalf("the other client's version was overwritten: %+v", got)
	}
}

// An update that names no version replaces whichever one is stored when it is
// decided, so one another client stored in between is read and replaced, not
// failed.
func TestPostgres_RESTUnconditionalUpdateIsDecidedAgainOnAConcurrentWrite(t *testing.T) {
	f := newWriteSafetyFixture(t)
	f.putEvent("contested", validICS("contested"))
	svc := f.serviceRunning(func() { f.putEvent("contested", summaryICS("contested", "Theirs")) })

	mine := summaryICS("contested", "Mine")
	event, _, err := svc.UpdateEvent(t.Context(), f.owner, f.calendar.ID, "contested", UpsertInput{RawICS: mine})
	if err != nil {
		t.Fatalf("UpdateEvent() error = %v, want it to replace the version stored meanwhile", err)
	}
	if got := f.stored("contested"); got == nil || got.ETag != event.ETag || !strings.Contains(got.RawICAL, "SUMMARY:Mine") {
		t.Fatalf("stored %+v, want the update's version", got)
	}
}

// A delete removes the event it read. An event already removed is answered as
// one that is not there; one edited in between is removed only when the
// request named no version.
func TestPostgres_RESTDeleteFollowsConcurrentChanges(t *testing.T) {
	t.Run("removed meanwhile", func(t *testing.T) {
		f := newWriteSafetyFixture(t)
		f.putEvent("twice", validICS("twice"))
		svc := f.serviceRunningAfterDecision(func() { f.remove("twice") })

		if err := svc.DeleteEvent(t.Context(), f.owner, f.calendar.ID, "twice", "", ""); !errors.Is(err, ErrNotFound) {
			t.Fatalf("DeleteEvent() of an event removed meanwhile error = %v, want ErrNotFound", err)
		}
	})

	t.Run("edited meanwhile without If-Match", func(t *testing.T) {
		f := newWriteSafetyFixture(t)
		f.putEvent("edited", validICS("edited"))
		svc := f.serviceRunningAfterDecision(func() { f.putEvent("edited", summaryICS("edited", "Theirs")) })

		if err := svc.DeleteEvent(t.Context(), f.owner, f.calendar.ID, "edited", "", ""); err != nil {
			t.Fatalf("DeleteEvent() error = %v, want the event removed", err)
		}
		if f.stored("edited") != nil {
			t.Fatal("the event is still stored")
		}
	})

	t.Run("edited meanwhile with If-Match", func(t *testing.T) {
		f := newWriteSafetyFixture(t)
		opened := f.putEvent("edited", validICS("edited"))
		theirs := summaryICS("edited", "Theirs")
		svc := f.serviceRunningAfterDecision(func() { f.putEvent("edited", theirs) })

		err := svc.DeleteEvent(t.Context(), f.owner, f.calendar.ID, "edited", `"`+opened.ETag+`"`, "")
		if !errors.Is(err, ErrPreconditionFailed) {
			t.Fatalf("DeleteEvent() with a stale If-Match error = %v, want ErrPreconditionFailed", err)
		}
		if got := f.stored("edited"); got == nil || got.RawICAL != theirs {
			t.Fatalf("the other client's version was removed: %+v", got)
		}
	})
}

// A create loses to an event another client stores under the UID between the
// decision and the write, and is refused as a conflict, or as a failed
// If-None-Match when it sent one.
func TestPostgres_RESTCreateLosesToAConcurrentCreate(t *testing.T) {
	for _, tc := range []struct {
		name        string
		ifNoneMatch string
		want        error
	}{
		{"no precondition", "", ErrConflict},
		{"If-None-Match", "*", ErrPreconditionFailed},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newWriteSafetyFixture(t)
			theirs := summaryICS("raced", "Theirs")
			svc := f.serviceRunning(func() { f.putEvent("raced", theirs) })

			_, err := svc.CreateEvent(t.Context(), f.owner, f.calendar.ID, UpsertInput{RawICS: validICS("raced"), IfNoneMatch: tc.ifNoneMatch})
			if !errors.Is(err, tc.want) {
				t.Fatalf("CreateEvent() error = %v, want %v", err, tc.want)
			}
			if got := f.stored("raced"); got == nil || got.RawICAL != theirs {
				t.Fatalf("the other client's event was overwritten: %+v", got)
			}
		})
	}
}

// A write that keeps losing to another is given up on with a conflict instead
// of being retried for ever, and writes nothing.
func TestPostgres_RESTWritesGiveUpWhenTheEventKeepsChanging(t *testing.T) {
	f := newWriteSafetyFixture(t)
	f.putEvent("busy", validICS("busy"))
	var changes int
	svc := NewService(f.database, func(raw string, cal *store.CalendarAccess) (*calobject.Object, error) {
		changes++
		f.putEvent("busy", summaryICS("busy", fmt.Sprintf("Change %d", changes)))
		return dav.ValidateCalendarObject(raw, cal)
	})

	_, _, err := svc.UpdateEvent(t.Context(), f.owner, f.calendar.ID, "busy", UpsertInput{RawICS: summaryICS("busy", "Mine")})
	if !errors.Is(err, ErrChangedConcurrently) || !errors.Is(err, ErrConflict) {
		t.Fatalf("UpdateEvent() error = %v, want ErrChangedConcurrently, a conflict", err)
	}
	if StatusCode(err) != http.StatusConflict {
		t.Fatalf("UpdateEvent() answers %d, want 409", StatusCode(err))
	}
	if changes != maxWriteAttempts {
		t.Fatalf("the update was decided %d times, want %d", changes, maxWriteAttempts)
	}
	if got := f.stored("busy"); got == nil || strings.Contains(got.RawICAL, "SUMMARY:Mine") {
		t.Fatalf("a write that was given up on changed the event: %+v", got)
	}
}

func TestStatusCodeAnswersLockedAndChangedConcurrently(t *testing.T) {
	if got := StatusCode(ErrLocked); got != http.StatusLocked {
		t.Errorf("StatusCode(ErrLocked) = %d, want 423", got)
	}
	if got := StatusCode(fmt.Errorf("event: %w", ErrChangedConcurrently)); got != http.StatusConflict {
		t.Errorf("StatusCode(ErrChangedConcurrently) = %d, want 409", got)
	}
}

const exclusiveWriteLock = `<?xml version="1.0" encoding="utf-8"?><D:lockinfo xmlns:D="DAV:"><D:lockscope><D:exclusive/></D:lockscope><D:locktype><D:write/></D:locktype></D:lockinfo>`

// lock takes an exclusive write lock on requestURI as the owner does over
// CalDAV and returns its token.
func (f *writeSafetyFixture) lock(requestURI, depth string) string {
	f.t.Helper()
	req := httptest.NewRequest("LOCK", requestURI, strings.NewReader(exclusiveWriteLock))
	req.Header.Set("Depth", depth)
	req.Header.Set("Timeout", "Second-600")
	req = req.WithContext(auth.WithUser(req.Context(), f.owner))
	rec := httptest.NewRecorder()
	dav.NewDavServer(dav.Options{Store: f.database}).ServeHTTP(rec, req)
	if rec.Code != http.StatusOK && rec.Code != http.StatusCreated {
		f.t.Fatalf("LOCK %s Depth %s = %d: %s", requestURI, depth, rec.Code, rec.Body.String())
	}
	token := strings.Trim(rec.Header().Get("Lock-Token"), "<>")
	if token == "" {
		f.t.Fatalf("LOCK %s answered no Lock-Token", requestURI)
	}
	return token
}

// expire makes the lock token names run out, as it does when its timeout
// passes.
func (f *writeSafetyFixture) expire(token string) {
	f.t.Helper()
	if _, err := f.database.Locks.Refresh(f.t.Context(), token, 1, time.Now().Add(-time.Minute)); err != nil {
		f.t.Fatalf("expire lock %s: %v", token, err)
	}
}

// A REST write submits no lock token, so it is refused with 423 wherever a
// CalDAV write without one would be: a lock on the event, a lock on the
// calendar for writes that add or remove a member, and a depth-infinity lock
// on the calendar for every write. A lock that has expired refuses nothing.
func TestPostgres_RESTEventWritesHonourWebDAVLocks(t *testing.T) {
	type operation struct {
		name string
		run  func(f *writeSafetyFixture, svc *Service) error
	}
	update := operation{"update", func(f *writeSafetyFixture, svc *Service) error {
		_, _, err := svc.UpdateEvent(t.Context(), f.owner, f.calendar.ID, "target", UpsertInput{RawICS: summaryICS("target", "Edited")})
		return err
	}}
	remove := operation{"delete", func(f *writeSafetyFixture, svc *Service) error {
		return svc.DeleteEvent(t.Context(), f.owner, f.calendar.ID, "target", "", "")
	}}
	create := operation{"create", func(f *writeSafetyFixture, svc *Service) error {
		_, err := svc.CreateEvent(t.Context(), f.owner, f.calendar.ID, UpsertInput{RawICS: validICS("fresh")})
		return err
	}}

	for _, tc := range []struct {
		name    string
		onEvent bool
		depth   string
		expired bool
		locked  map[string]bool
	}{
		{name: "lock on the event", onEvent: true, depth: "0", locked: map[string]bool{"update": true, "delete": true}},
		{name: "depth 0 lock on the calendar", depth: "0", locked: map[string]bool{"delete": true, "create": true}},
		{name: "depth infinity lock on the calendar", depth: "infinity", locked: map[string]bool{"update": true, "delete": true, "create": true}},
		{name: "expired lock on the event", onEvent: true, depth: "0", expired: true},
		{name: "expired lock on the calendar", depth: "infinity", expired: true},
	} {
		for _, op := range []operation{update, remove, create} {
			t.Run(tc.name+"/"+op.name, func(t *testing.T) {
				f := newWriteSafetyFixture(t)
				before := f.putEvent("target", validICS("target"))
				requestURI := f.collection + "/"
				if tc.onEvent {
					requestURI = f.collection + "/target.ics"
				}
				token := f.lock(requestURI, tc.depth)
				if tc.expired {
					f.expire(token)
				}

				err := op.run(f, NewService(f.database, dav.ValidateCalendarObject))
				if tc.locked[op.name] {
					if !errors.Is(err, ErrLocked) {
						t.Fatalf("%s error = %v, want ErrLocked", op.name, err)
					}
					if StatusCode(err) != http.StatusLocked {
						t.Fatalf("%s answers %d, want 423", op.name, StatusCode(err))
					}
					if got := f.stored("target"); got == nil || got.ETag != before.ETag {
						t.Fatalf("a refused %s changed the event: %+v", op.name, got)
					}
					if op.name == "create" && f.stored("fresh") != nil {
						t.Fatal("a refused create stored the event")
					}
					return
				}
				if err != nil {
					t.Fatalf("%s error = %v, want it written", op.name, err)
				}
			})
		}
	}
}

// A WebDAV lock is answered before a conditional header is evaluated, as DAV
// answers it: a request with a stale If-Match, or an If-None-Match: * on an
// event that exists, gets 423 from a lock that covers it, and 412 from a lock
// that does not or that has expired.
func TestPostgres_RESTLockedWritesAreAnswered423BeforeAFailedPrecondition(t *testing.T) {
	type request struct {
		name string
		run  func(f *writeSafetyFixture, svc *Service) error
	}
	staleUpdate := request{"update with a stale If-Match", func(f *writeSafetyFixture, svc *Service) error {
		_, _, err := svc.UpdateEvent(t.Context(), f.owner, f.calendar.ID, "target", UpsertInput{RawICS: summaryICS("target", "Edited"), IfMatch: `"stale"`})
		return err
	}}
	staleDelete := request{"delete with a stale If-Match", func(f *writeSafetyFixture, svc *Service) error {
		return svc.DeleteEvent(t.Context(), f.owner, f.calendar.ID, "target", `"stale"`, "")
	}}
	createExisting := request{"create with If-None-Match: *", func(f *writeSafetyFixture, svc *Service) error {
		_, err := svc.CreateEvent(t.Context(), f.owner, f.calendar.ID, UpsertInput{RawICS: validICS("target"), IfNoneMatch: "*"})
		return err
	}}
	for _, tc := range []struct {
		name    string
		onEvent bool
		depth   string
		lock    bool
		expired bool
		want    map[string]error
	}{
		{name: "lock on the event", onEvent: true, depth: "0", lock: true, want: map[string]error{
			staleUpdate.name: ErrLocked, staleDelete.name: ErrLocked, createExisting.name: ErrLocked}},
		{name: "depth 0 lock on the calendar", depth: "0", lock: true, want: map[string]error{
			staleUpdate.name: ErrPreconditionFailed, staleDelete.name: ErrLocked, createExisting.name: ErrLocked}},
		{name: "expired lock", onEvent: true, depth: "0", lock: true, expired: true, want: map[string]error{
			staleUpdate.name: ErrPreconditionFailed, staleDelete.name: ErrPreconditionFailed, createExisting.name: ErrPreconditionFailed}},
		{name: "no lock", want: map[string]error{
			staleUpdate.name: ErrPreconditionFailed, staleDelete.name: ErrPreconditionFailed, createExisting.name: ErrPreconditionFailed}},
	} {
		for _, req := range []request{staleUpdate, staleDelete, createExisting} {
			t.Run(tc.name+"/"+req.name, func(t *testing.T) {
				f := newWriteSafetyFixture(t)
				before := f.putEvent("target", validICS("target"))
				if tc.lock {
					requestURI := f.collection + "/"
					if tc.onEvent {
						requestURI = f.collection + "/target.ics"
					}
					token := f.lock(requestURI, tc.depth)
					if tc.expired {
						f.expire(token)
					}
				}
				err := req.run(f, NewService(f.database, dav.ValidateCalendarObject))
				if !errors.Is(err, tc.want[req.name]) {
					t.Fatalf("error = %v, want %v", err, tc.want[req.name])
				}
				if got := f.stored("target"); got == nil || got.ETag != before.ETag {
					t.Fatalf("a refused request changed the event: %+v", got)
				}
			})
		}
	}
}

// If-Match: * holds where the event exists (RFC 9110 section 13.1.1), at the
// decision and when the write lands: an event removed in between is answered
// as one that is not there.
func TestPostgres_RESTIfMatchAnyHoldsWhereTheEventExists(t *testing.T) {
	t.Run("update", func(t *testing.T) {
		f := newWriteSafetyFixture(t)
		f.putEvent("here", validICS("here"))
		svc := NewService(f.database, dav.ValidateCalendarObject)
		if _, _, err := svc.UpdateEvent(t.Context(), f.owner, f.calendar.ID, "here", UpsertInput{RawICS: summaryICS("here", "Edited"), IfMatch: "*"}); err != nil {
			t.Fatalf("UpdateEvent() with If-Match: * of an event that exists error = %v", err)
		}
		if got := f.stored("here"); got == nil || !strings.Contains(got.RawICAL, "SUMMARY:Edited") {
			t.Fatalf("the update was not stored: %+v", got)
		}
		if _, _, err := svc.UpdateEvent(t.Context(), f.owner, f.calendar.ID, "absent", UpsertInput{RawICS: validICS("absent"), IfMatch: "*"}); !errors.Is(err, ErrNotFound) {
			t.Fatalf("UpdateEvent() with If-Match: * of an event that is not there error = %v, want ErrNotFound", err)
		}
	})

	t.Run("delete", func(t *testing.T) {
		f := newWriteSafetyFixture(t)
		f.putEvent("here", validICS("here"))
		svc := NewService(f.database, dav.ValidateCalendarObject)
		if err := svc.DeleteEvent(t.Context(), f.owner, f.calendar.ID, "here", "*", ""); err != nil {
			t.Fatalf("DeleteEvent() with If-Match: * of an event that exists error = %v", err)
		}
		if f.stored("here") != nil {
			t.Fatal("the event is still stored")
		}
		if err := svc.DeleteEvent(t.Context(), f.owner, f.calendar.ID, "here", "*", ""); !errors.Is(err, ErrNotFound) {
			t.Fatalf("DeleteEvent() with If-Match: * of an event that is not there error = %v, want ErrNotFound", err)
		}
	})

	t.Run("event removed after the decision", func(t *testing.T) {
		f := newWriteSafetyFixture(t)
		f.putEvent("gone", validICS("gone"))
		svc := f.serviceRunning(func() { f.remove("gone") })
		_, _, err := svc.UpdateEvent(t.Context(), f.owner, f.calendar.ID, "gone", UpsertInput{RawICS: validICS("gone"), IfMatch: "*"})
		if !errors.Is(err, ErrNotFound) {
			t.Fatalf("UpdateEvent() error = %v, want ErrNotFound", err)
		}
		if f.stored("gone") != nil {
			t.Fatal("the update brought the removed event back")
		}
	})
}
