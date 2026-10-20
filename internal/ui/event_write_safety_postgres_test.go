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
	"testing"

	"github.com/jw6ventures/calcard/internal/acl"
	"github.com/jw6ventures/calcard/internal/calobject"
	"github.com/jw6ventures/calcard/internal/config"
	"github.com/jw6ventures/calcard/internal/dav"
	"github.com/jw6ventures/calcard/internal/store"
	"github.com/jw6ventures/calcard/internal/ui/utils"
)

const (
	changedElsewhereFlash = "This event was changed elsewhere since you opened it. Nothing was changed. Reload the page to see the current version, then try again."
	outOfDateFlash        = "This page is out of date, so the event was not changed. Reload the page and try again."
)

func keptChangingFlash(action string) string {
	return "This event kept changing elsewhere and could not be " + action + ". Reload the page and try again."
}

// putEvent stores body as the event uid of calendarID the way a CalDAV PUT
// does, under an ETag that names it.
func (f *uiEventFixture) putEvent(calendarID int64, uid, body string) *store.Event {
	f.t.Helper()
	event, err := f.database.Events.Upsert(f.t.Context(), store.Event{
		CalendarID: calendarID, UID: uid, ResourceName: uid, RawICAL: body, ETag: utils.GenerateETag(body),
	})
	if err != nil {
		f.t.Fatal(err)
	}
	return event
}

// putEventNamed is putEvent for an event stored under resourceName.
func (f *uiEventFixture) putEventNamed(calendarID int64, uid, resourceName, body string) *store.Event {
	f.t.Helper()
	event, err := f.database.Events.Upsert(f.t.Context(), store.Event{
		CalendarID: calendarID, UID: uid, ResourceName: resourceName, RawICAL: body, ETag: utils.GenerateETag(body),
	})
	if err != nil {
		f.t.Error(err)
	}
	return event
}

func (f *uiEventFixture) storedEvent(calendarID int64, uid string) *store.Event {
	f.t.Helper()
	event, err := f.database.Events.GetByUID(f.t.Context(), calendarID, uid)
	if err != nil {
		f.t.Fatal(err)
	}
	return event
}

// calendarOf creates a calendar of user's.
func (f *uiEventFixture) calendarOf(user *store.User, name string) *store.Calendar {
	f.t.Helper()
	cal, err := f.database.Calendars.Create(f.t.Context(), store.Calendar{UserID: user.ID, Name: name})
	if err != nil {
		f.t.Fatal(err)
	}
	return cal
}

func (f *uiEventFixture) setACL(resourcePath string, entries []store.ACLEntry) {
	f.t.Helper()
	if err := f.database.ACLEntries.SetACL(f.t.Context(), resourcePath, entries); err != nil {
		f.t.Fatal(err)
	}
}

// handlerRunning is a handler whose validator, which every event write passes
// the event through after deciding it and before storing it, first runs
// before: the moment another client's write lands in the gap between the two.
func (f *uiEventFixture) handlerRunning(before func()) *Handler {
	var once sync.Once
	validate := func(raw string, cal *store.CalendarAccess) (*calobject.Object, error) {
		once.Do(before)
		return dav.ValidateCalendarObject(raw, cal)
	}
	return NewHandler(&config.Config{}, f.database, nil, validate, dav.EventInstances)
}

// aclWrapper is an ACL repository standing in front of another.
type aclWrapper interface {
	store.ACLRepository
	wrap(store.ACLRepository)
}

// wrapACL puts wrapper in front of the store's ACL repository until the test
// ends.
func (f *uiEventFixture) wrapACL(wrapper aclWrapper) {
	f.t.Helper()
	inner := f.database.ACLEntries
	wrapper.wrap(inner)
	f.database.ACLEntries = wrapper
	f.t.Cleanup(func() { f.database.ACLEntries = inner })
}

// handlerRunningOn is a handler whose validator runs each on every event it
// validates, before it validates it.
func (f *uiEventFixture) handlerRunningOn(each func()) *Handler {
	validate := func(raw string, cal *store.CalendarAccess) (*calobject.Object, error) {
		each()
		return dav.ValidateCalendarObject(raw, cal)
	}
	return NewHandler(&config.Config{}, f.database, nil, validate, dav.EventInstances)
}

// decisionHookACL runs after once, as the decision to remove an event reads
// the calendar's own ACL, which it does last.
type decisionHookACL struct {
	store.ACLRepository
	collectionPath string
	after          func()
}

func (a *decisionHookACL) wrap(inner store.ACLRepository) { a.ACLRepository = inner }

func (a *decisionHookACL) ListByResource(ctx context.Context, resourcePath string) ([]store.ACLEntry, error) {
	entries, err := a.ACLRepository.ListByResource(ctx, resourcePath)
	if resourcePath == a.collectionPath && a.after != nil {
		after := a.after
		a.after = nil
		after()
	}
	return entries, err
}

// handlerRunningAfterDecision is a handler whose removal of an event runs
// after once its decision has read the calendar's ACL.
func (f *uiEventFixture) handlerRunningAfterDecision(calendarID int64, after func()) *Handler {
	f.wrapACL(&decisionHookACL{collectionPath: fmt.Sprintf("/dav/calendars/%d", calendarID), after: after})
	return NewHandler(&config.Config{}, f.database, nil, dav.ValidateCalendarObject, dav.EventInstances)
}

const (
	singleEvent = iota
	seriesEvent
)

const (
	editTime = "2026-01-16T10:00"
	editEnd  = "2026-01-16T11:00"
)

// eventWrite is one write of an event through the UI.
type eventWrite struct {
	name   string
	object int
	handle func(*Handler) http.HandlerFunc
	form   func(destination int64) url.Values
	// decided reports whether the handler, run by a hook, reaches a
	// validator; a removal of the whole event does not.
	validates bool
	// moves reports whether the write takes the event to the destination.
	moves bool
}

func eventWrites() []eventWrite {
	edit := func(extra ...string) func(int64) url.Values {
		return func(destination int64) url.Values {
			form := url.Values{"summary": {"edited"}, "dtstart": {editTime}, "dtend": {editEnd}, "timezone": {"UTC"}}
			for i := 0; i < len(extra); i += 2 {
				form.Set(extra[i], extra[i+1])
			}
			if destination != 0 {
				form.Set("destination_calendar_id", strconv.FormatInt(destination, 10))
			}
			return form
		}
	}
	occurrence := []string{"edit_scope", "occurrence", "recurrence_id", "2026-01-16T10:00", "recurrence_timezone", "UTC"}
	return []eventWrite{
		{name: "edit", object: singleEvent, handle: updateHandler, form: edit(), validates: true},
		{name: "occurrence edit", object: seriesEvent, handle: updateHandler, form: edit(occurrence...), validates: true},
		{name: "occurrence delete", object: seriesEvent, handle: deleteHandler, form: edit(occurrence...), validates: true},
		{name: "series delete", object: seriesEvent, handle: deleteHandler, form: edit()},
		{name: "move", object: singleEvent, handle: updateHandler, form: edit(), validates: true, moves: true},
	}
}

func updateHandler(h *Handler) http.HandlerFunc { return h.UpdateEvent }
func deleteHandler(h *Handler) http.HandlerFunc { return h.DeleteEvent }

func eventObject(kind int, uid string) string {
	if kind == seriesEvent {
		return uiEventBody(uid, "DTSTART:20260115T100000Z", "DTEND:20260115T110000Z", "RRULE:FREQ=DAILY;COUNT=3")
	}
	return uiEventBody(uid, "DTSTART:20260115T100000Z", "DTEND:20260115T110000Z")
}

func slug(s string) string { return strings.ReplaceAll(s, " ", "-") }

// A write takes effect only on the version of the event the page was built
// from: its ETag travels with the form. A form built before another client
// changed the event, or one that does not say what it was built from, writes
// nothing and tells the person why.
func TestPostgres_UIEventWritesNeedTheETagTheEventWasOpenedOn(t *testing.T) {
	f := newUIEventFixture(t)
	elsewhere := f.calendarOf(f.sharee, "Elsewhere")
	for _, write := range eventWrites() {
		for _, tc := range []struct {
			name      string
			etag      func(*store.Event) []string
			wantFlash string
		}{
			{"a stale ETag", func(*store.Event) []string { return []string{"stale"} }, changedElsewhereFlash},
			{"no ETag", func(*store.Event) []string { return nil }, outOfDateFlash},
			{"the current ETag", func(e *store.Event) []string { return []string{e.ETag} }, ""},
		} {
			t.Run(write.name+": "+tc.name, func(t *testing.T) {
				uid := "etag-" + slug(write.name+"-"+tc.name)
				before := f.putEvent(f.mine.ID, uid, eventObject(write.object, uid))
				destination := int64(0)
				if write.moves {
					destination = elsewhere.ID
				}
				form := write.form(destination)
				if etag := tc.etag(before); etag != nil {
					form["etag"] = etag
				}
				rr := f.serve(write.handle(f.handler), f.mine.ID, uid, form)

				after := f.storedEvent(f.mine.ID, uid)
				if tc.wantFlash == "" {
					if got := flashedErrorOf(rr); got != "" {
						t.Fatalf("the current ETag was refused: %q", got)
					}
					if after != nil && after.ETag == before.ETag {
						t.Errorf("the write was not applied")
					}
					return
				}
				if got := flashedError(t, rr); got != tc.wantFlash {
					t.Errorf("flashed error = %q, want %q", got, tc.wantFlash)
				}
				if after == nil || after.ETag != before.ETag || after.RawICAL != before.RawICAL {
					t.Errorf("the event was written: %+v", after)
				}
				if f.eventStored(elsewhere.ID, uid) {
					t.Errorf("the event was moved")
				}
			})
		}
	}
}

// flashedErrorOf is the error a redirect carries, or "" when it has none.
func flashedErrorOf(rr *httptest.ResponseRecorder) string {
	location, err := url.Parse(rr.Header().Get("Location"))
	if err != nil {
		return ""
	}
	return location.Query().Get("error")
}

// A CalDAV client's write that lands after the page built the edit and before
// the edit is stored is not overwritten: the comparison with the version the
// page was opened on happens in the write itself.
func TestPostgres_UIEventWriteKeepsAWriteThatLandsBeforeIt(t *testing.T) {
	f := newUIEventFixture(t)
	elsewhere := f.calendarOf(f.sharee, "Elsewhere")
	for _, write := range eventWrites() {
		t.Run(write.name, func(t *testing.T) {
			uid := "landed-" + slug(write.name)
			before := f.putEvent(f.mine.ID, uid, eventObject(write.object, uid))
			davBody := strings.Replace(before.RawICAL, "SUMMARY:x", "SUMMARY:from a CalDAV client", 1)
			landed := func() { f.putEvent(f.mine.ID, uid, davBody) }
			var handler *Handler
			if write.validates {
				handler = f.handlerRunning(landed)
			} else {
				handler = f.handlerRunningAfterDecision(f.mine.ID, landed)
			}
			destination := int64(0)
			if write.moves {
				destination = elsewhere.ID
			}
			form := write.form(destination)
			form.Set("etag", before.ETag)
			rr := f.serve(write.handle(handler), f.mine.ID, uid, form)

			if got := flashedError(t, rr); got != changedElsewhereFlash {
				t.Errorf("flashed error = %q, want %q", got, changedElsewhereFlash)
			}
			after := f.storedEvent(f.mine.ID, uid)
			if after == nil || after.RawICAL != davBody {
				t.Errorf("the CalDAV client's write was lost: %+v", after)
			}
			if f.eventStored(elsewhere.ID, uid) {
				t.Errorf("the event was moved")
			}
		})
	}
}

// An event another client deletes after the page decided the write is not
// brought back by it.
func TestPostgres_UIEventWriteDoesNotRestoreAnEventRemovedBeforeIt(t *testing.T) {
	f := newUIEventFixture(t)
	elsewhere := f.calendarOf(f.sharee, "Elsewhere")
	for _, write := range eventWrites() {
		t.Run(write.name, func(t *testing.T) {
			uid := "removed-" + slug(write.name)
			before := f.putEvent(f.mine.ID, uid, eventObject(write.object, uid))
			removed := func() {
				if err := f.database.Events.DeleteByUID(f.t.Context(), f.mine.ID, uid); err != nil {
					f.t.Error(err)
				}
			}
			var handler *Handler
			if write.validates {
				handler = f.handlerRunning(removed)
			} else {
				handler = f.handlerRunningAfterDecision(f.mine.ID, removed)
			}
			destination := int64(0)
			if write.moves {
				destination = elsewhere.ID
			}
			form := write.form(destination)
			form.Set("etag", before.ETag)
			rr := f.serve(write.handle(handler), f.mine.ID, uid, form)

			if rr.Code != http.StatusNotFound {
				t.Errorf("status = %d, want %d for an event that is gone (%s)", rr.Code, http.StatusNotFound, rr.Header().Get("Location"))
			}
			if f.eventStored(f.mine.ID, uid) || f.eventStored(elsewhere.ID, uid) {
				t.Errorf("the removed event is stored again")
			}
		})
	}
}

// A sharee's grant withdrawn between the page deciding a write and the write
// stops it: the decision is made again on the new ACL.
func TestPostgres_UIEventWriteRechecksAnACLChangedBeforeIt(t *testing.T) {
	f := newUIEventFixture(t)
	mine := f.calendarOf(f.sharee, "Receiving")
	for _, write := range eventWrites() {
		t.Run(write.name, func(t *testing.T) {
			uid := "revoked-" + slug(write.name)
			calendarID := f.calendarWithEvent(uid, eventObject(write.object, uid), f.shareeACE(true, "read", "write"), nil)
			before := f.storedEvent(calendarID, uid)
			revoke := func() { f.setACL(fmt.Sprintf("/dav/calendars/%d", calendarID), f.shareeACE(true, "read")) }
			var handler *Handler
			if write.validates {
				handler = f.handlerRunning(revoke)
			} else {
				handler = f.handlerRunningAfterDecision(calendarID, revoke)
			}
			destination := int64(0)
			if write.moves {
				destination = mine.ID
			}
			form := write.form(destination)
			form.Set("etag", before.ETag)
			rr := f.serve(write.handle(handler), calendarID, uid, form)

			if rr.Code != http.StatusForbidden {
				t.Errorf("status = %d, want %d once the grant is withdrawn (%s)", rr.Code, http.StatusForbidden, rr.Header().Get("Location"))
			}
			after := f.storedEvent(calendarID, uid)
			if after == nil || after.ETag != before.ETag || after.RawICAL != before.RawICAL {
				t.Errorf("the event was changed through a withdrawn grant: %+v", after)
			}
			if f.eventStored(mine.ID, uid) {
				t.Errorf("the event was moved through a withdrawn grant")
			}
		})
	}
	t.Run("create", func(t *testing.T) {
		cal := f.calendarWithEvent("existing-create", eventObject(singleEvent, "existing-create"), f.shareeACE(true, "read", "write"), nil)
		revoke := func() { f.setACL(fmt.Sprintf("/dav/calendars/%d", cal), f.shareeACE(true, "read")) }
		rr := f.serve(f.handlerRunning(revoke).CreateEvent, cal, "", url.Values{
			"summary": {"created"}, "dtstart": {editTime}, "dtend": {editEnd}, "timezone": {"UTC"},
		})
		if rr.Code != http.StatusForbidden {
			t.Errorf("status = %d, want %d once the grant is withdrawn (%s)", rr.Code, http.StatusForbidden, rr.Header().Get("Location"))
		}
		events, err := f.database.Events.ListForCalendar(f.t.Context(), cal)
		if err != nil {
			t.Fatal(err)
		}
		if len(events) != 1 {
			t.Errorf("the calendar holds %d events, want only the one it began with", len(events))
		}
	})
}

// A move needs DAV:bind on the destination calendar, whose ACL can change
// while the move runs as the source's can.
func TestPostgres_UIMoveRechecksTheDestinationACLChangedBeforeIt(t *testing.T) {
	f := newUIEventFixture(t)
	uid := "destination-revoked"
	before := f.putEvent(f.mine.ID, uid, eventObject(singleEvent, uid))
	destination := f.calendarOf(f.owner, "Destination")
	destinationPath := fmt.Sprintf("/dav/calendars/%d", destination.ID)
	f.setACL(destinationPath, f.shareeACE(true, "read", "bind"))
	handler := f.handlerRunning(func() { f.setACL(destinationPath, f.shareeACE(true, "read")) })

	form := url.Values{
		"summary": {"moved"}, "dtstart": {editTime}, "dtend": {editEnd}, "timezone": {"UTC"},
		"destination_calendar_id": {strconv.FormatInt(destination.ID, 10)}, "etag": {before.ETag},
	}
	rr := f.serve(handler.UpdateEvent, f.mine.ID, uid, form)

	if rr.Code != http.StatusForbidden {
		t.Errorf("status = %d, want %d once bind is withdrawn (%s)", rr.Code, http.StatusForbidden, rr.Header().Get("Location"))
	}
	if after := f.storedEvent(f.mine.ID, uid); after == nil || after.ETag != before.ETag {
		t.Errorf("the event left its calendar: %+v", after)
	}
	if f.eventStored(destination.ID, uid) {
		t.Errorf("the event reached a calendar the sharee may no longer add to")
	}
}

// churningACL adds a grant to an unrelated principal to the calendar each time
// its entries are read to be pinned, for the first changes reads. The grant
// decides nothing the write depends on, but the entries the write pinned have
// changed by the time the store checks them.
type churningACL struct {
	store.ACLRepository
	collectionPath string
	changes        int
}

func (c *churningACL) wrap(inner store.ACLRepository) { c.ACLRepository = inner }

func (c *churningACL) ListByResources(ctx context.Context, paths []string) ([]store.ACLEntry, error) {
	entries, err := c.ACLRepository.ListByResources(ctx, paths)
	if err == nil && c.changes > 0 {
		c.changes--
		current, listErr := c.ACLRepository.ListByResource(ctx, c.collectionPath)
		if listErr != nil {
			return nil, listErr
		}
		current = append(current, store.ACLEntry{
			PrincipalHref: acl.PrincipalHref(int64(9000 + len(current))), IsGrant: true, Privilege: "read", Position: 100 + len(current),
		})
		if err := c.ACLRepository.SetACL(ctx, c.collectionPath, current); err != nil {
			return nil, err
		}
	}
	return entries, err
}

// A write whose ACL changed in a way that does not matter to it is decided
// again and goes through; one whose ACL keeps changing gives up, naming the
// reason, and leaves the event as it was.
func TestPostgres_UIEventWriteGivesUpOnlyWhenTheACLKeepsChanging(t *testing.T) {
	f := newUIEventFixture(t)
	mine := f.calendarOf(f.sharee, "Receiving")
	for _, write := range eventWrites() {
		for _, tc := range []struct {
			name    string
			changes int
			applied bool
		}{
			{"one unrelated change", 1, true},
			{"a change on every attempt", 100, false},
		} {
			t.Run(write.name+": "+tc.name, func(t *testing.T) {
				uid := "churn-" + slug(write.name+"-"+tc.name)
				calendarID := f.calendarWithEvent(uid, eventObject(write.object, uid), f.shareeACE(true, "read", "write"), nil)
				before := f.storedEvent(calendarID, uid)
				collectionPath := fmt.Sprintf("/dav/calendars/%d", calendarID)
				f.wrapACL(&churningACL{collectionPath: collectionPath, changes: tc.changes})
				destination := int64(0)
				if write.moves {
					destination = mine.ID
				}
				form := write.form(destination)
				form.Set("etag", before.ETag)
				rr := f.serve(write.handle(f.handler), calendarID, uid, form)

				after := f.storedEvent(calendarID, uid)
				applied := after == nil || after.ETag != before.ETag
				if applied != tc.applied {
					t.Errorf("write applied = %t, want %t (%s)", applied, tc.applied, rr.Header().Get("Location"))
				}
				if !tc.applied {
					action := "saved"
					if strings.HasSuffix(write.name, "delete") {
						action = "deleted"
					}
					if got := flashedError(t, rr); got != keptChangingFlash(action) {
						t.Errorf("flashed error = %q, want %q", got, keptChangingFlash(action))
					}
				}
			})
		}
	}
}

// A create whose calendar's ACL changes without mattering to it is decided
// again and goes through; one whose ACL keeps changing gives up.
func TestPostgres_UIEventCreateGivesUpOnlyWhenTheACLKeepsChanging(t *testing.T) {
	f := newUIEventFixture(t)
	for _, tc := range []struct {
		name    string
		changes int
		created bool
	}{
		{"one unrelated change", 1, true},
		{"a change on every attempt", 100, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			existing := "churn-existing-" + slug(tc.name)
			calendarID := f.calendarWithEvent(existing, eventObject(singleEvent, existing), f.shareeACE(true, "read", "write"), nil)
			f.wrapACL(&churningACL{collectionPath: fmt.Sprintf("/dav/calendars/%d", calendarID), changes: tc.changes})
			rr := f.serve(f.handler.CreateEvent, calendarID, "", url.Values{
				"summary": {"created"}, "dtstart": {editTime}, "dtend": {editEnd}, "timezone": {"UTC"},
			})
			events, err := f.database.Events.ListForCalendar(f.t.Context(), calendarID)
			if err != nil {
				t.Fatal(err)
			}
			if created := len(events) == 2; created != tc.created {
				t.Errorf("event created = %t, want %t (%s)", created, tc.created, rr.Header().Get("Location"))
			}
			if !tc.created {
				if got := flashedError(t, rr); got != keptChangingFlash("saved") {
					t.Errorf("flashed error = %q, want %q", got, keptChangingFlash("saved"))
				}
			}
		})
	}
}

// The privileges the UI decides on come from the ACL entries at the calendar's
// path and the event's, and nowhere else: a grant withdrawn after the handler
// loaded the calendar, whose privileges that load reports, authorizes nothing.
func TestPostgres_UIEventWriteIgnoresAGrantWithdrawnAfterTheCalendarWasLoaded(t *testing.T) {
	f := newUIEventFixture(t)
	for _, write := range eventWrites() {
		if write.moves {
			continue
		}
		t.Run(write.name, func(t *testing.T) {
			uid := "stale-load-" + slug(write.name)
			calendarID := f.calendarWithEvent(uid, eventObject(write.object, uid), f.shareeACE(true, "read", "write"), nil)
			before := f.storedEvent(calendarID, uid)
			inner := f.database.Calendars
			f.database.Calendars = &afterGetCalendars{CalendarRepository: inner, after: func() {
				f.setACL(fmt.Sprintf("/dav/calendars/%d", calendarID), f.shareeACE(true, "read"))
			}}
			t.Cleanup(func() { f.database.Calendars = inner })

			form := write.form(0)
			form.Set("etag", before.ETag)
			rr := f.serve(write.handle(f.handler), calendarID, uid, form)

			if rr.Code != http.StatusForbidden {
				t.Errorf("status = %d, want %d (%s)", rr.Code, http.StatusForbidden, rr.Header().Get("Location"))
			}
			if after := f.storedEvent(calendarID, uid); after == nil || after.ETag != before.ETag {
				t.Errorf("the event was changed through a withdrawn grant: %+v", after)
			}
		})
	}
}
