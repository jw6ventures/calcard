package ui

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"

	"github.com/jw6ventures/calcard/internal/auth"
	"github.com/jw6ventures/calcard/internal/store"
)

// importFile is an iCalendar file holding one single event per uid.
func importFile(uids ...string) string {
	var sb strings.Builder
	sb.WriteString("BEGIN:VCALENDAR\r\nVERSION:2.0\r\nPRODID:-//Test//EN\r\n")
	for _, uid := range uids {
		sb.WriteString("BEGIN:VEVENT\r\nUID:" + uid + "\r\nDTSTAMP:20260101T000000Z\r\nSUMMARY:imported " + uid +
			"\r\nDTSTART:20260320T100000Z\r\nDTEND:20260320T110000Z\r\nEND:VEVENT\r\n")
	}
	sb.WriteString("END:VCALENDAR\r\n")
	return sb.String()
}

// importFlash is the status or error the import redirect carries.
func importFlash(t *testing.T, rr *httptest.ResponseRecorder) string {
	t.Helper()
	if rr.Code != http.StatusFound {
		t.Fatalf("status = %d: %s", rr.Code, rr.Body.String())
	}
	location, err := url.Parse(rr.Header().Get("Location"))
	if err != nil {
		t.Fatal(err)
	}
	if message := location.Query().Get("error"); message != "" {
		return message
	}
	return location.Query().Get("status")
}

// runImport has the sharee import file into calendarID through handler.
func (f *uiEventFixture) runImport(handler *Handler, calendarID int64, file string) *httptest.ResponseRecorder {
	f.t.Helper()
	req := newICSImportRequest(f.t, "/calendars/x/import", "events.ics", file)
	req = withRouteID(req, fmt.Sprint(calendarID))
	req = req.WithContext(auth.WithUser(req.Context(), f.sharee))
	rr := httptest.NewRecorder()
	handler.ImportCalendar(rr, req)
	return rr
}

func (f *uiEventFixture) summaryOf(calendarID int64, uid string) string {
	f.t.Helper()
	event := f.storedEvent(calendarID, uid)
	if event == nil || event.Summary == nil {
		return ""
	}
	return *event.Summary
}

// An import writes each event as a UI create or edit does. A lock on an event
// refuses its replacement, and a lock on the calendar refuses a new event, as
// either refuses a CalDAV PUT that submits no token; the events that are not
// refused are imported, and the flash says how many were left out and why.
func TestPostgres_ImportSkipsTheEventsALockHolds(t *testing.T) {
	f := newUIEventFixture(t)
	setup := func(name string) int64 {
		calendar := f.calendarOf(f.sharee, name)
		f.putEvent(calendar.ID, "replaced", eventObject(singleEvent, "replaced"))
		f.putEvent(calendar.ID, "held", eventObject(singleEvent, "held"))
		return calendar.ID
	}

	t.Run("a locked event", func(t *testing.T) {
		calendarID := setup("Event lock")
		token := f.davLock(f.sharee, eventHref(calendarID, "held"), "0", "")
		heldBefore := f.storedEvent(calendarID, "held")

		flash := importFlash(t, f.runImport(f.handler, calendarID, importFile("replaced", "held", "added")))

		if !strings.Contains(flash, "Imported 2 event(s)") || !strings.Contains(flash, "skipped 1 held by a lock of a CalDAV client") {
			t.Errorf("flash = %q", flash)
		}
		if after := f.storedEvent(calendarID, "held"); after.ETag != heldBefore.ETag {
			t.Errorf("the locked event was replaced")
		}
		if f.summaryOf(calendarID, "replaced") != "imported replaced" || f.summaryOf(calendarID, "added") != "imported added" {
			t.Errorf("the events that are not locked were not imported")
		}
		if !f.lockHeld(token) {
			t.Errorf("the import dropped the lock")
		}
	})

	t.Run("a locked calendar", func(t *testing.T) {
		calendarID := setup("Calendar lock")
		token := f.davLock(f.sharee, calendarHref(calendarID), "0", "")

		flash := importFlash(t, f.runImport(f.handler, calendarID, importFile("replaced", "added")))

		if !strings.Contains(flash, "Imported 1 event(s)") || !strings.Contains(flash, "skipped 1 held by a lock of a CalDAV client") {
			t.Errorf("flash = %q", flash)
		}
		if f.eventStored(calendarID, "added") {
			t.Errorf("an event was added to a locked calendar")
		}
		if f.summaryOf(calendarID, "replaced") != "imported replaced" {
			t.Errorf("a depth-0 lock on the calendar refused the replacement of an event, which it does not hold")
		}
		if !f.lockHeld(token) {
			t.Errorf("the import dropped the lock")
		}
	})

	t.Run("nothing left to import", func(t *testing.T) {
		calendarID := setup("All locked")
		f.davLock(f.sharee, calendarHref(calendarID), "infinity", "")

		flash := importFlash(t, f.runImport(f.handler, calendarID, importFile("replaced", "added")))

		if !strings.HasPrefix(flash, "failed to import events") || !strings.Contains(flash, "skipped 2 held by a lock of a CalDAV client") {
			t.Errorf("flash = %q", flash)
		}
	})
}

// An import replaces the version of an event it decided on and no other: a
// CalDAV client's write landing between the decision and the write is kept, and
// the event is left out with a reason; the other events are imported.
func TestPostgres_ImportKeepsAnEventChangedBeforeItIsWritten(t *testing.T) {
	f := newUIEventFixture(t)
	calendar := f.calendarOf(f.sharee, "Changing")
	before := f.putEvent(calendar.ID, "changing", eventObject(singleEvent, "changing"))
	davBody := strings.Replace(before.RawICAL, "SUMMARY:x", "SUMMARY:from a CalDAV client", 1)
	calls := 0
	handler := f.handlerRunningOn(func() {
		// The second event is validated after the first was decided.
		calls++
		if calls == 2 {
			f.putEvent(calendar.ID, "changing", davBody)
		}
	})

	flash := importFlash(t, f.runImport(handler, calendar.ID, importFile("changing", "added")))

	if !strings.Contains(flash, "Imported 1 event(s)") || !strings.Contains(flash, "skipped 1 changed during the import") {
		t.Errorf("flash = %q", flash)
	}
	if after := f.storedEvent(calendar.ID, "changing"); after == nil || after.RawICAL != davBody {
		t.Errorf("the CalDAV client's write was lost: %+v", after)
	}
	if f.summaryOf(calendar.ID, "added") != "imported added" {
		t.Errorf("the other event was not imported")
	}
}

// A grant withdrawn between the decision on an event and its write is decided
// again, and the import writes no event through it.
func TestPostgres_ImportRechecksAnACLChangedBeforeAnEventIsWritten(t *testing.T) {
	f := newUIEventFixture(t)
	calendarID := f.calendarWithEvent("existing", eventObject(singleEvent, "existing"), f.shareeACE(true, "read", "write"), nil)
	before := f.storedEvent(calendarID, "existing")
	// The name of the new event is chosen last of everything an import decides.
	inner := f.database.Events
	f.database.Events = &afterNameLookupEvents{EventRepository: inner, after: func() {
		f.setACL(fmt.Sprintf("/dav/calendars/%d", calendarID), f.shareeACE(true, "read"))
	}}
	t.Cleanup(func() { f.database.Events = inner })

	flash := importFlash(t, f.runImport(f.handler, calendarID, importFile("existing", "added")))

	if !strings.Contains(flash, "skipped 2 changed during the import") {
		t.Errorf("flash = %q", flash)
	}
	if after := f.storedEvent(calendarID, "existing"); after.ETag != before.ETag || f.eventStored(calendarID, "added") {
		t.Errorf("the import wrote through a withdrawn grant")
	}
}

// An ACL change that does not matter to the import is decided again once, and
// the import goes through.
func TestPostgres_ImportSurvivesAnUnrelatedACLChange(t *testing.T) {
	f := newUIEventFixture(t)
	calendarID := f.calendarWithEvent("existing", eventObject(singleEvent, "existing"), f.shareeACE(true, "read", "write"), nil)
	f.wrapACL(&churningACL{collectionPath: fmt.Sprintf("/dav/calendars/%d", calendarID), changes: 1})

	flash := importFlash(t, f.runImport(f.handler, calendarID, importFile("existing", "added")))

	if !strings.Contains(flash, "Imported 2 event(s)") {
		t.Errorf("flash = %q", flash)
	}
	if f.summaryOf(calendarID, "existing") != "imported existing" || !f.eventStored(calendarID, "added") {
		t.Errorf("the events were not imported")
	}
}

// An event stored under a name no href carries is left alone, as an edit
// leaves it, and the import goes on.
func TestPostgres_ImportLeavesAnEventStoredUnderAnUnwritableName(t *testing.T) {
	f := newUIEventFixture(t)
	calendar := f.calendarOf(f.sharee, "Legacy")
	if _, err := f.database.Events.Upsert(t.Context(), store.Event{
		CalendarID: calendar.ID, UID: "legacy", ResourceName: ".", RawICAL: eventObject(singleEvent, "legacy"), ETag: "legacy-etag",
	}); err != nil {
		t.Fatal(err)
	}

	flash := importFlash(t, f.runImport(f.handler, calendar.ID, importFile("legacy", "added")))

	if !strings.Contains(flash, "Imported 1 event(s)") {
		t.Errorf("flash = %q", flash)
	}
	if after := f.storedEvent(calendar.ID, "legacy"); after.ETag != "legacy-etag" || after.ResourceName != "." {
		t.Errorf("the legacy event was written: %+v", after)
	}
}

// afterNameLookupEvents runs after once, after the first lookup of an event by
// its resource name.
type afterNameLookupEvents struct {
	store.EventRepository
	after func()
}

func (e *afterNameLookupEvents) GetByResourceName(ctx context.Context, calendarID int64, resourceName string) (*store.Event, error) {
	event, err := e.EventRepository.GetByResourceName(ctx, calendarID, resourceName)
	if e.after != nil {
		after := e.after
		e.after = nil
		after()
	}
	return event, err
}

// An event a CalDAV client creates under the same UID while the import runs is
// a change the import lost to, not a failed save.
func TestPostgres_ImportReportsAConcurrentSameUIDCreateAsChanged(t *testing.T) {
	f := newUIEventFixture(t)
	calendar := f.calendarOf(f.sharee, "Racing")
	calls := 0
	handler := f.handlerRunningOn(func() {
		calls++
		if calls == 2 {
			f.putEventNamed(calendar.ID, "raced", "other-name", eventObject(singleEvent, "raced"))
		}
	})

	flash := importFlash(t, f.runImport(handler, calendar.ID, importFile("raced", "added")))

	if !strings.Contains(flash, "skipped 1 changed during the import") || strings.Contains(flash, "could not be saved") {
		t.Errorf("flash = %q", flash)
	}
	if f.summaryOf(calendar.ID, "added") != "imported added" {
		t.Errorf("the other event was not imported")
	}
}
