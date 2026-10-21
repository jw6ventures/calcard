package ui

import (
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"

	"github.com/jw6ventures/calcard/internal/auth"
	"github.com/jw6ventures/calcard/internal/config"
	"github.com/jw6ventures/calcard/internal/dav"
	"github.com/jw6ventures/calcard/internal/store"
)

// An import names a new event after its UID, and distinct UIDs can give one
// name: "a@b" and "a_b" both give a_b, and "." and ".." give none an href can
// carry. Each event is still imported, under a name of its own that a CalDAV
// client can address, whether the name is taken by an event stored before or
// by one earlier in the same upload.
func TestPostgres_ImportGivesEveryEventANameOfItsOwn(t *testing.T) {
	database := newUIPostgresStore(t)
	ctx := t.Context()
	owner, err := database.Users.UpsertOAuthUser(ctx, "owner", "owner@example.test", "Owner", "Owner")
	if err != nil {
		t.Fatal(err)
	}
	cal, err := database.Calendars.Create(ctx, store.Calendar{UserID: owner.ID, Name: "Import"})
	if err != nil {
		t.Fatal(err)
	}
	handler := NewHandler(&config.Config{}, database, nil, dav.ValidateCalendarObject, dav.EventInstances)
	event := func(uid string) string {
		return "BEGIN:VEVENT\r\nUID:" + uid + "\r\nDTSTAMP:20260101T000000Z\r\nSUMMARY:" + uid + "\r\nDTSTART:20260320T100000Z\r\nEND:VEVENT\r\n"
	}
	importICS := func(uids ...string) {
		t.Helper()
		var body strings.Builder
		body.WriteString("BEGIN:VCALENDAR\r\nVERSION:2.0\r\nPRODID:-//Test//EN\r\n")
		for _, uid := range uids {
			body.WriteString(event(uid))
		}
		body.WriteString("END:VCALENDAR\r\n")
		req := newICSImportRequest(t, "/calendars/x/import", "events.ics", body.String())
		req = withRouteID(req, strconv.FormatInt(cal.ID, 10))
		req = req.WithContext(auth.WithUser(req.Context(), owner))
		rr := httptest.NewRecorder()
		handler.ImportCalendar(rr, req)
		if rr.Code != http.StatusFound || strings.Contains(rr.Header().Get("Location"), "error") || strings.Contains(rr.Header().Get("Location"), "could+not") {
			t.Fatalf("import of %q: status %d location %s", uids, rr.Code, rr.Header().Get("Location"))
		}
	}
	importICS("a@b")
	importICS("a_b", ".", "..")

	names := map[string]string{}
	for _, uid := range []string{"a@b", "a_b", ".", ".."} {
		stored, err := database.Events.GetByUID(ctx, cal.ID, uid)
		if err != nil {
			t.Fatal(err)
		}
		if stored == nil {
			t.Errorf("event %q was not imported", uid)
			continue
		}
		if !store.ValidMemberName(stored.ResourceName) {
			t.Errorf("event %q is stored as %q, a name no href carries", uid, stored.ResourceName)
		}
		if other, taken := names[stored.ResourceName]; taken {
			t.Errorf("events %q and %q are both stored as %q", other, uid, stored.ResourceName)
		}
		names[stored.ResourceName] = uid
	}
	if stored, err := database.Events.GetByUID(ctx, cal.ID, "a@b"); err != nil || stored == nil || stored.ResourceName != "a_b" {
		t.Errorf("the first event to claim a_b lost it: %+v, %v", stored, err)
	}
}

// DAV:bind is a privilege of the calendar a new event joins (RFC 3744 section
// 3.9), so an import adds an event only for a user the calendar grants it to,
// whatever ACL is stored at the path the new event would take: entries the
// upgrade repair leaves at a path no event holds included.
func TestPostgres_ImportAddsAnEventOnlyWithBindOnTheCalendar(t *testing.T) {
	f := newUIEventFixture(t)
	ctx := t.Context()
	uid := "orphaned-name"
	calendarID := f.calendarWithEvent("writable", uiEventBody("writable", "DTSTART:20260115T100000Z"), f.shareeACE(true, "read"), f.shareeACE(true, "write"))
	if err := f.database.ACLEntries.SetACL(ctx, store.MemberStatePath("/dav/calendars/"+strconv.FormatInt(calendarID, 10), uid), f.shareeACE(true, "all")); err != nil {
		t.Fatal(err)
	}
	body := "BEGIN:VCALENDAR\r\nVERSION:2.0\r\nPRODID:-//Test//EN\r\n" +
		"BEGIN:VEVENT\r\nUID:" + uid + "\r\nDTSTAMP:20260101T000000Z\r\nSUMMARY:new\r\nDTSTART:20260320T100000Z\r\nEND:VEVENT\r\n" +
		"END:VCALENDAR\r\n"
	req := newICSImportRequest(t, "/calendars/x/import", "events.ics", body)
	req = withRouteID(req, strconv.FormatInt(calendarID, 10))
	req = req.WithContext(auth.WithUser(req.Context(), f.sharee))
	rr := httptest.NewRecorder()
	f.handler.ImportCalendar(rr, req)
	if f.eventStored(calendarID, uid) {
		t.Errorf("a sharee without bind on the calendar imported a new event: status %d location %s", rr.Code, rr.Header().Get("Location"))
	}
}
