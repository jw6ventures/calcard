package ui

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/jw6ventures/calcard/internal/auth"
	"github.com/jw6ventures/calcard/internal/config"
	"github.com/jw6ventures/calcard/internal/dav"
	"github.com/jw6ventures/calcard/internal/store"
)

const (
	eventLockedFlash          = "This event or its calendar is locked by a CalDAV client. Try again once it has been unlocked."
	eventMoveLockedFlash      = "This event, its calendar or the destination calendar is locked by a CalDAV client. Try again once it has been unlocked."
	calendarLockedFlash       = "This calendar is locked by a CalDAV client. Try again once it has been unlocked."
	calendarDeleteLockedFlash = "This calendar or an event in it is locked by a CalDAV client. Try again once it has been unlocked."

	exclusiveWriteLock = `<?xml version="1.0" encoding="utf-8"?><d:lockinfo xmlns:d="DAV:"><d:lockscope><d:exclusive/></d:lockscope><d:locktype><d:write/></d:locktype></d:lockinfo>`
)

// davRequest sends a request to the CalDAV server as user and returns the
// response.
func (f *uiEventFixture) davRequest(user *store.User, method, target string, header map[string]string, body string) *httptest.ResponseRecorder {
	f.t.Helper()
	req := httptest.NewRequest(method, target, strings.NewReader(body))
	for key, value := range header {
		req.Header.Set(key, value)
	}
	req = req.WithContext(auth.WithUser(req.Context(), user))
	rr := httptest.NewRecorder()
	dav.NewDavServer(dav.Options{Config: &config.Config{}, Store: f.database}).ServeHTTP(rr, req)
	return rr
}

// davLock takes a write lock on href with a CalDAV LOCK as user and returns its
// token.
func (f *uiEventFixture) davLock(user *store.User, href, depth, timeout string) string {
	f.t.Helper()
	header := map[string]string{"Depth": depth}
	if timeout != "" {
		header["Timeout"] = timeout
	}
	rr := f.davRequest(user, "LOCK", href, header, exclusiveWriteLock)
	if rr.Code != http.StatusOK && rr.Code != http.StatusCreated {
		f.t.Fatalf("LOCK %s = %d: %s", href, rr.Code, rr.Body.String())
	}
	return strings.Trim(rr.Header().Get("Lock-Token"), "<>")
}

func (f *uiEventFixture) lockHeld(token string) bool {
	f.t.Helper()
	held, err := f.database.Locks.GetByToken(f.t.Context(), token)
	if err != nil {
		f.t.Fatal(err)
	}
	return held != nil
}

func calendarHref(calendarID int64) string { return fmt.Sprintf("/dav/calendars/%d/", calendarID) }

func eventHref(calendarID int64, uid string) string {
	return fmt.Sprintf("/dav/calendars/%d/%s.ics", calendarID, uid)
}

// lockedWrite is a UI write with the CalDAV request that makes the same change.
type lockedWrite struct {
	eventWrite
	creates bool
	// dav is the CalDAV request making the same change, as the user holding
	// the locks, without a lock token.
	dav func(f *uiEventFixture, calendarID, destinationID int64, uid string) *httptest.ResponseRecorder
}

func lockedWrites() []lockedWrite {
	put := func(f *uiEventFixture, calendarID, _ int64, uid string) *httptest.ResponseRecorder {
		return f.davRequest(f.sharee, http.MethodPut, eventHref(calendarID, uid), map[string]string{"Content-Type": "text/calendar"}, eventObject(singleEvent, uid))
	}
	byName := map[string]eventWrite{}
	for _, write := range eventWrites() {
		byName[write.name] = write
	}
	return []lockedWrite{
		{eventWrite: byName["edit"], dav: put},
		{eventWrite: byName["occurrence edit"], dav: put},
		{eventWrite: byName["occurrence delete"], dav: put},
		{eventWrite: byName["series delete"], dav: func(f *uiEventFixture, calendarID, _ int64, uid string) *httptest.ResponseRecorder {
			return f.davRequest(f.sharee, http.MethodDelete, eventHref(calendarID, uid), nil, "")
		}},
		{eventWrite: byName["move"], dav: func(f *uiEventFixture, calendarID, destinationID int64, uid string) *httptest.ResponseRecorder {
			return f.davRequest(f.sharee, "MOVE", eventHref(calendarID, uid), map[string]string{"Destination": eventHref(destinationID, uid)}, "")
		}},
		{eventWrite: eventWrite{name: "create", object: singleEvent, handle: func(h *Handler) http.HandlerFunc { return h.CreateEvent }, form: byName["edit"].form}, creates: true,
			dav: func(f *uiEventFixture, calendarID, _ int64, _ string) *httptest.ResponseRecorder {
				return f.davRequest(f.sharee, http.MethodPut, eventHref(calendarID, "created-by-dav"), map[string]string{"Content-Type": "text/calendar"}, eventObject(singleEvent, "created-by-dav"))
			}},
	}
}

// What a lock on one resource keeps from the UI is what it keeps from a CalDAV
// client that submits no lock token: a lock on the event, or on a calendar it
// is bound to or leaves, refuses the write at any depth where DAV does, and
// the refusal leaves the event and the lock as they were. The UI submits no
// token, so the holder's own lock refuses it too.
func TestPostgres_UIEventWritesRefuseWhatDAVRefusesForWantOfALockToken(t *testing.T) {
	f := newUIEventFixture(t)
	layouts := []struct {
		name string
		// lock takes the lock and returns its token.
		lock func(calendarID, destinationID int64, uid string) string
		// refuses reports whether a write of that name is refused.
		refuses func(write string) bool
	}{
		{"the event", func(c, _ int64, uid string) string { return f.davLock(f.sharee, eventHref(c, uid), "0", "") },
			func(w string) bool { return w != "create" }},
		{"its calendar, depth 0", func(c, _ int64, _ string) string { return f.davLock(f.sharee, calendarHref(c), "0", "") },
			func(w string) bool { return w == "series delete" || w == "move" || w == "create" }},
		{"its calendar, depth infinity", func(c, _ int64, _ string) string { return f.davLock(f.sharee, calendarHref(c), "infinity", "") },
			func(string) bool { return true }},
		{"the destination calendar", func(_, d int64, _ string) string { return f.davLock(f.sharee, calendarHref(d), "0", "") },
			func(w string) bool { return w == "move" }},
		{"the destination name", func(_, d int64, uid string) string { return f.davLock(f.sharee, eventHref(d, uid), "0", "") },
			func(w string) bool { return w == "move" }},
	}
	for _, write := range lockedWrites() {
		for _, layout := range layouts {
			t.Run(write.name+": "+layout.name, func(t *testing.T) {
				// setup makes an event, a destination and the lock; the CalDAV
				// request is made against one set and the UI write against another.
				setup := func(role string) (calendar, destination *store.Calendar, uid string, event *store.Event, token string) {
					name := slug(write.name + "-" + layout.name + "-" + role)
					calendar = f.calendarOf(f.sharee, "Locked "+name)
					destination = f.calendarOf(f.sharee, "Destination "+name)
					uid = "locked-" + name
					event = f.putEvent(calendar.ID, uid, eventObject(write.object, uid))
					return calendar, destination, uid, event, layout.lock(calendar.ID, destination.ID, uid)
				}
				davCalendar, davDestination, davUID, _, _ := setup("dav")
				davRefuses := write.dav(f, davCalendar.ID, davDestination.ID, davUID).Code == http.StatusLocked
				if want := layout.refuses(write.name); davRefuses != want {
					t.Fatalf("the CalDAV request answered locked = %t, want %t: the table does not describe DAV", davRefuses, want)
				}

				calendar, destination, uid, before, token := setup("ui")
				destinationID := int64(0)
				if write.moves {
					destinationID = destination.ID
				}
				form := write.form(destinationID)
				form.Set("etag", before.ETag)
				rr := f.serve(write.handle(f.handler), calendar.ID, uid, form)

				after := f.storedEvent(calendar.ID, uid)
				applied := after == nil || after.ETag != before.ETag
				if write.creates {
					events, err := f.database.Events.ListForCalendar(t.Context(), calendar.ID)
					if err != nil {
						t.Fatal(err)
					}
					applied = len(events) > 1
				}
				if davRefuses {
					want := eventLockedFlash
					if write.moves {
						want = eventMoveLockedFlash
					}
					if got := flashedError(t, rr); got != want {
						t.Errorf("flashed error = %q, want %q", got, want)
					}
					if applied {
						t.Errorf("the write was applied")
					}
					if f.eventStored(destination.ID, uid) {
						t.Errorf("the event was moved")
					}
				} else if !applied || flashedErrorOf(rr) != "" {
					t.Errorf("the write was refused: %q", flashedErrorOf(rr))
				}
				if !f.lockHeld(token) {
					t.Errorf("the write dropped the lock")
				}
			})
		}
	}
}

// A lock another user holds on a calendar shared for editing refuses the
// sharee's UI write as it refuses their CalDAV request, and the holder's
// lock stays.
func TestPostgres_UIEventWriteRefusesALockAnotherUserHolds(t *testing.T) {
	f := newUIEventFixture(t)
	uid := "locked-by-owner"
	calendarID := f.calendarWithEvent(uid, eventObject(singleEvent, uid), f.shareeACE(true, "read", "write"), nil)
	token := f.davLock(f.owner, eventHref(calendarID, uid), "0", "")
	before := f.storedEvent(calendarID, uid)

	rr := f.serve(f.handler.DeleteEvent, calendarID, uid, url.Values{"etag": {before.ETag}})

	if got := flashedError(t, rr); got != eventLockedFlash {
		t.Errorf("flashed error = %q, want %q", got, eventLockedFlash)
	}
	if !f.eventStored(calendarID, uid) || !f.lockHeld(token) {
		t.Errorf("the refused delete removed the event or the lock")
	}
}

// An expired lock refuses nothing.
func TestPostgres_UIEventWriteIgnoresAnExpiredLock(t *testing.T) {
	f := newUIEventFixture(t)
	uid := "lock-expired"
	before := f.putEvent(f.mine.ID, uid, eventObject(singleEvent, uid))
	f.davLock(f.sharee, eventHref(f.mine.ID, uid), "0", "Second-1")
	time.Sleep(1200 * time.Millisecond)

	rr := f.serve(f.handler.UpdateEvent, f.mine.ID, uid, url.Values{
		"summary": {"edited"}, "dtstart": {editTime}, "dtend": {editEnd}, "timezone": {"UTC"}, "etag": {before.ETag},
	})

	if got := flashedErrorOf(rr); got != "" {
		t.Errorf("an expired lock refused the edit: %q", got)
	}
	if after := f.storedEvent(f.mine.ID, uid); after == nil || after.ETag == before.ETag {
		t.Errorf("the edit was not applied")
	}
}

// A lock taken after the UI decided a write and before it stores it refuses
// the write: the check is made inside the write.
func TestPostgres_UIEventWriteRefusesALockTakenBeforeIt(t *testing.T) {
	f := newUIEventFixture(t)
	destination := f.calendarOf(f.sharee, "Destination")
	for _, write := range lockedWrites() {
		if write.name == "occurrence edit" || write.name == "occurrence delete" {
			continue
		}
		t.Run(write.name, func(t *testing.T) {
			uid := "lock-race-" + slug(write.name)
			before := f.putEvent(f.mine.ID, uid, eventObject(write.object, uid))
			lock := func() { f.davLock(f.sharee, eventHref(f.mine.ID, uid), "0", "") }
			if write.creates {
				lock = func() { f.davLock(f.sharee, calendarHref(f.mine.ID), "0", "") }
			}
			var handler *Handler
			if write.validates || write.creates {
				handler = f.handlerRunning(lock)
			} else {
				handler = f.handlerRunningAfterDecision(f.mine.ID, lock)
			}
			destinationID := int64(0)
			if write.moves {
				destinationID = destination.ID
			}
			form := write.form(destinationID)
			form.Set("etag", before.ETag)
			rr := f.serve(write.handle(handler), f.mine.ID, uid, form)

			want := eventLockedFlash
			if write.moves {
				want = eventMoveLockedFlash
			}
			if got := flashedError(t, rr); got != want {
				t.Errorf("flashed error = %q, want %q", got, want)
			}
			if after := f.storedEvent(f.mine.ID, uid); after == nil || after.ETag != before.ETag {
				t.Errorf("the write was applied")
			}
			if f.eventStored(destination.ID, uid) {
				t.Errorf("the event was moved")
			}
		})
	}
}
