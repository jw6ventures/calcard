package ui

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strconv"
	"strings"
	"testing"

	"github.com/go-chi/chi/v5"

	"github.com/jw6ventures/calcard/internal/acl"
	"github.com/jw6ventures/calcard/internal/auth"
	"github.com/jw6ventures/calcard/internal/config"
	"github.com/jw6ventures/calcard/internal/dav"
	"github.com/jw6ventures/calcard/internal/store"
)

// uiEventFixture is an owner's calendar holding one event, the ACLs on both,
// and a sharee who acts on them through the UI handlers.
type uiEventFixture struct {
	t        *testing.T
	database *store.Store
	handler  *Handler
	owner    *store.User
	sharee   *store.User
	mine     *store.Calendar
}

func newUIEventFixture(t *testing.T) *uiEventFixture {
	t.Helper()
	database := newUIPostgresStore(t)
	newUser := func(name string) *store.User {
		t.Helper()
		user, err := database.Users.UpsertOAuthUser(t.Context(), name, name+"@example.test", name, name)
		if err != nil {
			t.Fatal(err)
		}
		return user
	}
	f := &uiEventFixture{
		t:        t,
		database: database,
		handler:  NewHandler(&config.Config{}, database, nil, dav.ValidateCalendarObject, dav.EventInstances),
		owner:    newUser("owner"),
		sharee:   newUser("sharee"),
	}
	mine, err := database.Calendars.Create(t.Context(), store.Calendar{UserID: f.sharee.ID, Name: "Mine"})
	if err != nil {
		t.Fatal(err)
	}
	f.mine = mine
	return f
}

func (f *uiEventFixture) shareeACE(grant bool, privileges ...string) []store.ACLEntry {
	entries := make([]store.ACLEntry, 0, len(privileges))
	for i, privilege := range privileges {
		entries = append(entries, store.ACLEntry{PrincipalHref: acl.PrincipalHref(f.sharee.ID), IsGrant: grant, Privilege: privilege, Position: i})
	}
	return entries
}

// calendarWithEvent stores body as the event uid in a new calendar of the
// owner's, with calendarACL on the calendar and eventACL on the event.
func (f *uiEventFixture) calendarWithEvent(uid, body string, calendarACL, eventACL []store.ACLEntry) int64 {
	f.t.Helper()
	ctx := f.t.Context()
	cal, err := f.database.Calendars.Create(ctx, store.Calendar{UserID: f.owner.ID, Name: "Owner's " + uid})
	if err != nil {
		f.t.Fatal(err)
	}
	collectionPath := fmt.Sprintf("/dav/calendars/%d", cal.ID)
	if _, err := f.database.Events.Upsert(ctx, store.Event{CalendarID: cal.ID, UID: uid, ResourceName: uid, RawICAL: body, ETag: uid}); err != nil {
		f.t.Fatal(err)
	}
	if calendarACL != nil {
		if err := f.database.ACLEntries.SetACL(ctx, collectionPath, calendarACL); err != nil {
			f.t.Fatal(err)
		}
	}
	if eventACL != nil {
		if err := f.database.ACLEntries.SetACL(ctx, store.MemberStatePath(collectionPath, uid), eventACL); err != nil {
			f.t.Fatal(err)
		}
	}
	return cal.ID
}

func (f *uiEventFixture) eventStored(calendarID int64, uid string) bool {
	f.t.Helper()
	event, err := f.database.Events.GetByUID(f.t.Context(), calendarID, uid)
	if err != nil {
		f.t.Fatal(err)
	}
	return event != nil
}

// serve has the sharee post form to handle for the event uid of calendarID.
func (f *uiEventFixture) serve(handle http.HandlerFunc, calendarID int64, uid string, form url.Values) *httptest.ResponseRecorder {
	f.t.Helper()
	req := httptest.NewRequest(http.MethodPost, "/calendars/x/events/y", strings.NewReader(form.Encode()))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	routeContext := chi.NewRouteContext()
	routeContext.URLParams.Add("id", strconv.FormatInt(calendarID, 10))
	routeContext.URLParams.Add("uid", uid)
	req = req.WithContext(context.WithValue(auth.WithUser(req.Context(), f.sharee), chi.RouteCtxKey, routeContext))
	rr := httptest.NewRecorder()
	handle(rr, req)
	return rr
}

func uiEventBody(uid string, lines ...string) string {
	return "BEGIN:VCALENDAR\r\nVERSION:2.0\r\nPRODID:-//Test//EN\r\nBEGIN:VEVENT\r\nUID:" + uid + "\r\nDTSTAMP:20260101T000000Z\r\n" +
		strings.Join(lines, "\r\n") + "\r\nSUMMARY:x\r\nEND:VEVENT\r\nEND:VCALENDAR\r\n"
}

// The UI takes an event out of its calendar, by deleting it or moving it to
// another, only as a DAV DELETE or MOVE of it would: with DAV:unbind on the
// calendar, which RFC 3744 section 3.10 makes a privilege of the collection a
// member leaves. A grant on the event alone, of unbind or of everything,
// does not let a sharee take it out of the owner's calendar.
func TestPostgres_UIRemovesAnEventOnlyWithUnbindOnItsCalendar(t *testing.T) {
	f := newUIEventFixture(t)
	single := func(uid string) string {
		return uiEventBody(uid, "DTSTART:20260115T100000Z", "DTEND:20260115T110000Z")
	}
	tests := []struct {
		name                  string
		calendarACL, eventACL []store.ACLEntry
		wantRemoved           bool
		wantStatus            int
	}{
		{name: "write on the event alone", eventACL: f.shareeACE(true, "write"), wantStatus: http.StatusForbidden},
		{name: "all on the event alone", eventACL: f.shareeACE(true, "all"), wantStatus: http.StatusForbidden},
		{name: "unbind on the event alone", eventACL: f.shareeACE(true, "unbind"), wantStatus: http.StatusForbidden},
		{name: "unbind on the calendar, denied on the event", calendarACL: f.shareeACE(true, "read", "write"), eventACL: f.shareeACE(false, "unbind"), wantRemoved: true, wantStatus: http.StatusFound},
	}
	for _, tc := range tests {
		t.Run("series delete: "+tc.name, func(t *testing.T) {
			uid := "delete-" + strings.ReplaceAll(tc.name, " ", "-")
			calendarID := f.calendarWithEvent(uid, single(uid), tc.calendarACL, tc.eventACL)
			rr := f.serve(f.handler.DeleteEvent, calendarID, uid, nil)
			if rr.Code != tc.wantStatus {
				t.Errorf("DeleteEvent = %d, want %d", rr.Code, tc.wantStatus)
			}
			if removed := !f.eventStored(calendarID, uid); removed != tc.wantRemoved {
				t.Errorf("event removed = %t, want %t", removed, tc.wantRemoved)
			}
		})
	}
	for _, tc := range tests {
		// A move out also needs write-content on the event, which an unbind
		// grant does not give; a sharee who may not read the event either is
		// answered as for a UID the calendar does not hold.
		if tc.name == "unbind on the event alone" {
			tc.wantStatus = http.StatusNotFound
		}
		t.Run("move out: "+tc.name, func(t *testing.T) {
			uid := "move-" + strings.ReplaceAll(tc.name, " ", "-")
			calendarID := f.calendarWithEvent(uid, single(uid), tc.calendarACL, tc.eventACL)
			rr := f.serve(f.handler.UpdateEvent, calendarID, uid, url.Values{
				"summary": {"moved"}, "dtstart": {"2026-01-15T10:00"}, "dtend": {"2026-01-15T11:00"},
				"destination_calendar_id": {strconv.FormatInt(f.mine.ID, 10)},
			})
			if rr.Code != tc.wantStatus {
				t.Errorf("UpdateEvent moving out = %d, want %d (%s)", rr.Code, tc.wantStatus, rr.Header().Get("Location"))
			}
			if removed := !f.eventStored(calendarID, uid); removed != tc.wantRemoved {
				t.Errorf("event removed from the owner's calendar = %t, want %t", removed, tc.wantRemoved)
			}
			if moved := f.eventStored(f.mine.ID, uid); moved != tc.wantRemoved {
				t.Errorf("event in the sharee's calendar = %t, want %t", moved, tc.wantRemoved)
			}
		})
	}

	// Deleting the only occurrence an object holds, an override with no
	// series beside it, removes the whole object, so it is decided as that
	// removal is.
	t.Run("occurrence delete of an object holding no series", func(t *testing.T) {
		for _, tc := range []struct {
			name        string
			calendarACL []store.ACLEntry
			wantRemoved bool
			wantStatus  int
		}{
			{name: "write on the event alone", wantStatus: http.StatusForbidden},
			{name: "unbind on the calendar", calendarACL: f.shareeACE(true, "read", "unbind"), wantRemoved: true, wantStatus: http.StatusFound},
		} {
			uid := "override-" + strings.ReplaceAll(tc.name, " ", "-")
			body := uiEventBody(uid, "RECURRENCE-ID:20260115T100000Z", "DTSTART:20260115T100000Z", "DTEND:20260115T110000Z")
			calendarID := f.calendarWithEvent(uid, body, tc.calendarACL, f.shareeACE(true, "write"))
			rr := f.serve(f.handler.DeleteEvent, calendarID, uid, url.Values{"edit_scope": {"occurrence"}, "recurrence_id": {"2026-01-15T10:00"}, "timezone": {"UTC"}})
			if rr.Code != tc.wantStatus {
				t.Errorf("%s: DeleteEvent of the occurrence = %d, want %d (%s)", tc.name, rr.Code, tc.wantStatus, rr.Header().Get("Location"))
			}
			if removed := !f.eventStored(calendarID, uid); removed != tc.wantRemoved {
				t.Errorf("%s: event removed = %t, want %t", tc.name, removed, tc.wantRemoved)
			}
		}
	})
}

// The UI answers an edit or delete of an event the sharee may not read
// exactly as it answers one naming a UID the calendar does not hold, so the
// answer does not tell the sharee that the event exists.
func TestPostgres_UIAnswersAnUnreadableEventAsAMissingOne(t *testing.T) {
	f := newUIEventFixture(t)
	series := func(uid string) string {
		return uiEventBody(uid, "DTSTART:20260115T100000Z", "DTEND:20260115T110000Z", "RRULE:FREQ=DAILY;COUNT=3")
	}
	edit := url.Values{"summary": {"edited"}, "dtstart": {"2026-01-15T10:00"}, "dtend": {"2026-01-15T11:00"}}
	occurrence := url.Values{"edit_scope": {"occurrence"}, "recurrence_id": {"2026-01-15T10:00"}, "timezone": {"UTC"}}
	requests := []struct {
		name   string
		handle func(*Handler) http.HandlerFunc
		form   url.Values
	}{
		{"edit", func(h *Handler) http.HandlerFunc { return h.UpdateEvent }, edit},
		{"series delete", func(h *Handler) http.HandlerFunc { return h.DeleteEvent }, nil},
		{"occurrence delete", func(h *Handler) http.HandlerFunc { return h.DeleteEvent }, occurrence},
	}
	calendars := []struct {
		name        string
		calendarACL []store.ACLEntry
	}{
		{"calendar shared read-only", f.shareeACE(true, "read")},
		{"calendar shared for editing", f.shareeACE(true, "read", "write")},
	}
	for _, cal := range calendars {
		for _, request := range requests {
			t.Run(cal.name+": "+request.name, func(t *testing.T) {
				uid := strings.ReplaceAll(cal.name+"-"+request.name, " ", "-")
				calendarID := f.calendarWithEvent(uid, series(uid), cal.calendarACL, f.shareeACE(false, "all"))
				existing := f.serve(request.handle(f.handler), calendarID, uid, request.form)
				missing := f.serve(request.handle(f.handler), calendarID, uid+"-missing", request.form)
				if existing.Code != missing.Code || existing.Body.String() != missing.Body.String() || existing.Header().Get("Location") != missing.Header().Get("Location") {
					t.Errorf("unreadable event answered %d %q %q, a missing one %d %q %q",
						existing.Code, existing.Body.String(), existing.Header().Get("Location"),
						missing.Code, missing.Body.String(), missing.Header().Get("Location"))
				}
				stored, err := f.database.Events.GetByUID(t.Context(), calendarID, uid)
				if err != nil {
					t.Fatal(err)
				}
				if stored == nil || stored.ETag != uid {
					t.Errorf("the unreadable event was changed: %+v", stored)
				}
			})
		}
	}
}

// The address book pages answer an edit, delete or move of a contact the
// sharee may not read as they answer one naming a UID the book does not hold.
func TestPostgres_UIAnswersAnUnreadableContactAsAMissingOne(t *testing.T) {
	f := newUIEventFixture(t)
	ctx := t.Context()
	target, err := f.database.AddressBooks.Create(ctx, store.AddressBook{UserID: f.sharee.ID, Name: "Mine"})
	if err != nil {
		t.Fatal(err)
	}
	requests := []struct {
		name   string
		handle func(*Handler) http.HandlerFunc
		form   url.Values
	}{
		{"edit", func(h *Handler) http.HandlerFunc { return h.UpdateContact }, url.Values{"display_name": {"Edited"}, "email": {"not an address"}}},
		{"delete", func(h *Handler) http.HandlerFunc { return h.DeleteContact }, nil},
		{"move", func(h *Handler) http.HandlerFunc { return h.MoveContact }, url.Values{"target_address_book_id": {strconv.FormatInt(target.ID, 10)}}},
	}
	for _, request := range requests {
		t.Run(request.name, func(t *testing.T) {
			book, err := f.database.AddressBooks.Create(ctx, store.AddressBook{UserID: f.owner.ID, Name: "Owner's " + request.name})
			if err != nil {
				t.Fatal(err)
			}
			bookPath := fmt.Sprintf("/dav/addressbooks/%d", book.ID)
			uid := "contact-" + request.name
			card := "BEGIN:VCARD\r\nVERSION:3.0\r\nUID:" + uid + "\r\nFN:Hidden\r\nEMAIL:not an address\r\nEND:VCARD\r\n"
			if _, err := f.database.Contacts.Upsert(ctx, store.Contact{AddressBookID: book.ID, UID: uid, ResourceName: uid, RawVCard: card, ETag: uid}); err != nil {
				t.Fatal(err)
			}
			if err := f.database.ACLEntries.SetACL(ctx, bookPath, f.shareeACE(true, "read", "write")); err != nil {
				t.Fatal(err)
			}
			if err := f.database.ACLEntries.SetACL(ctx, store.MemberStatePath(bookPath, uid), f.shareeACE(false, "all")); err != nil {
				t.Fatal(err)
			}
			existing := f.serve(request.handle(f.handler), book.ID, uid, request.form)
			missing := f.serve(request.handle(f.handler), book.ID, uid+"-missing", request.form)
			if existing.Code != missing.Code || existing.Body.String() != missing.Body.String() || existing.Header().Get("Location") != missing.Header().Get("Location") {
				t.Errorf("unreadable contact answered %d %q %q, a missing one %d %q %q",
					existing.Code, existing.Body.String(), existing.Header().Get("Location"),
					missing.Code, missing.Body.String(), missing.Header().Get("Location"))
			}
			stored, err := f.database.Contacts.GetByUID(ctx, book.ID, uid)
			if err != nil {
				t.Fatal(err)
			}
			if stored == nil || stored.ETag != uid {
				t.Errorf("the unreadable contact was changed: %+v", stored)
			}
		})
	}
}

// A 1.1.x release could store an event or contact as "." or "..", which name
// the collection or its parent once a path is cleaned. The UI may still show
// or delete one, but writes none in place and moves none under that name.
func TestPostgres_UIWritesNoItemStoredUnderANameNoHrefCarries(t *testing.T) {
	f := newUIEventFixture(t)
	ctx := t.Context()
	for _, name := range []string{".", ".."} {
		t.Run("event "+name, func(t *testing.T) {
			cal, err := f.database.Calendars.Create(ctx, store.Calendar{UserID: f.owner.ID, Name: "Legacy " + name})
			if err != nil {
				t.Fatal(err)
			}
			collectionPath := fmt.Sprintf("/dav/calendars/%d", cal.ID)
			if err := f.database.ACLEntries.SetACL(ctx, collectionPath, f.shareeACE(true, "read", "write")); err != nil {
				t.Fatal(err)
			}
			uid := "legacy-event"
			body := uiEventBody(uid, "DTSTART:20260115T100000Z", "DTEND:20260115T110000Z", "RRULE:FREQ=DAILY;COUNT=3")
			if _, err := f.database.Events.Upsert(ctx, store.Event{CalendarID: cal.ID, UID: uid, ResourceName: name, RawICAL: body, ETag: uid}); err != nil {
				t.Fatal(err)
			}
			edit := url.Values{"summary": {"edited"}, "dtstart": {"2026-01-15T10:00"}, "dtend": {"2026-01-15T11:00"}}
			move := url.Values{"summary": {"moved"}, "dtstart": {"2026-01-15T10:00"}, "dtend": {"2026-01-15T11:00"}, "destination_calendar_id": {strconv.FormatInt(f.mine.ID, 10)}}
			occurrence := url.Values{"edit_scope": {"occurrence"}, "recurrence_id": {"2026-01-16T10:00"}, "timezone": {"UTC"}}
			for _, request := range []struct {
				label  string
				handle http.HandlerFunc
				form   url.Values
			}{
				{"edit", f.handler.UpdateEvent, edit},
				{"move", f.handler.UpdateEvent, move},
				{"occurrence delete", f.handler.DeleteEvent, occurrence},
			} {
				rr := f.serve(request.handle, cal.ID, uid, request.form)
				if rr.Code != http.StatusFound || !strings.Contains(rr.Header().Get("Location"), "error=") {
					t.Errorf("%s: status %d location %q, want a redirect naming the error", request.label, rr.Code, rr.Header().Get("Location"))
				}
				stored, err := f.database.Events.GetByUID(ctx, cal.ID, uid)
				if err != nil {
					t.Fatal(err)
				}
				if stored == nil || stored.ETag != uid || stored.ResourceName != name {
					t.Errorf("%s wrote the event stored as %q: %+v", request.label, name, stored)
				}
				if f.eventStored(f.mine.ID, uid) {
					t.Errorf("%s moved the event stored as %q", request.label, name)
				}
			}
			if rr := f.serve(f.handler.DeleteEvent, cal.ID, uid, nil); rr.Code != http.StatusFound || f.eventStored(cal.ID, uid) {
				t.Errorf("delete of the event stored as %q: status %d location %q, stored %t", name, rr.Code, rr.Header().Get("Location"), f.eventStored(cal.ID, uid))
			}
		})
		t.Run("contact "+name, func(t *testing.T) {
			book, err := f.database.AddressBooks.Create(ctx, store.AddressBook{UserID: f.owner.ID, Name: "Legacy " + name})
			if err != nil {
				t.Fatal(err)
			}
			target, err := f.database.AddressBooks.Create(ctx, store.AddressBook{UserID: f.sharee.ID, Name: "Target " + name})
			if err != nil {
				t.Fatal(err)
			}
			if err := f.database.ACLEntries.SetACL(ctx, fmt.Sprintf("/dav/addressbooks/%d", book.ID), f.shareeACE(true, "read", "write")); err != nil {
				t.Fatal(err)
			}
			uid := "legacy-contact"
			card := "BEGIN:VCARD\r\nVERSION:3.0\r\nUID:" + uid + "\r\nFN:Legacy\r\nEND:VCARD\r\n"
			if _, err := f.database.Contacts.Upsert(ctx, store.Contact{AddressBookID: book.ID, UID: uid, ResourceName: name, RawVCard: card, ETag: uid}); err != nil {
				t.Fatal(err)
			}
			for _, request := range []struct {
				label  string
				handle http.HandlerFunc
				form   url.Values
			}{
				{"edit", f.handler.UpdateContact, url.Values{"display_name": {"Edited"}}},
				{"move", f.handler.MoveContact, url.Values{"target_address_book_id": {strconv.FormatInt(target.ID, 10)}}},
			} {
				rr := f.serve(request.handle, book.ID, uid, request.form)
				if rr.Code != http.StatusFound || !strings.Contains(rr.Header().Get("Location"), "error=") {
					t.Errorf("%s: status %d location %q, want a redirect naming the error", request.label, rr.Code, rr.Header().Get("Location"))
				}
				stored, err := f.database.Contacts.GetByUID(ctx, book.ID, uid)
				if err != nil {
					t.Fatal(err)
				}
				if stored == nil || stored.ETag != uid || stored.ResourceName != name {
					t.Errorf("%s wrote the contact stored as %q: %+v", request.label, name, stored)
				}
				if moved, err := f.database.Contacts.GetByUID(ctx, target.ID, uid); err != nil || moved != nil {
					t.Errorf("%s moved the contact stored as %q: %+v, %v", request.label, name, moved, err)
				}
			}
		})
	}
}

// A move adds the event to the destination calendar, which needs DAV:bind
// there, a privilege of the calendar (RFC 3744 section 3.9): an ACL stored at
// the path the event would take in it neither grants nor refuses it.
func TestPostgres_UIMoveDecidesBindOnTheDestinationCalendar(t *testing.T) {
	f := newUIEventFixture(t)
	ctx := t.Context()
	for _, tc := range []struct {
		name                   string
		destinationACL, member []store.ACLEntry
		wantMoved              bool
	}{
		{"bind granted on the destination's member path only", f.shareeACE(true, "read"), f.shareeACE(true, "all"), false},
		{"bind granted on the destination, refused on its member path", f.shareeACE(true, "read", "bind"), f.shareeACE(false, "bind"), true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			uid := "moving-" + strings.ReplaceAll(tc.name, " ", "-")
			source := f.calendarWithEvent(uid, uiEventBody(uid, "DTSTART:20260115T100000Z", "DTEND:20260115T110000Z"), f.shareeACE(true, "read", "write"), nil)
			destination, err := f.database.Calendars.Create(ctx, store.Calendar{UserID: f.owner.ID, Name: "Destination " + uid})
			if err != nil {
				t.Fatal(err)
			}
			destinationPath := fmt.Sprintf("/dav/calendars/%d", destination.ID)
			if err := f.database.ACLEntries.SetACL(ctx, destinationPath, tc.destinationACL); err != nil {
				t.Fatal(err)
			}
			if err := f.database.ACLEntries.SetACL(ctx, store.MemberStatePath(destinationPath, uid), tc.member); err != nil {
				t.Fatal(err)
			}
			rr := f.serve(f.handler.UpdateEvent, source, uid, url.Values{
				"summary": {"moved"}, "dtstart": {"2026-01-15T10:00"}, "dtend": {"2026-01-15T11:00"},
				"destination_calendar_id": {strconv.FormatInt(destination.ID, 10)},
			})
			if moved := f.eventStored(destination.ID, uid); moved != tc.wantMoved {
				t.Errorf("event moved = %t, want %t (status %d location %q)", moved, tc.wantMoved, rr.Code, rr.Header().Get("Location"))
			}
		})
	}
}
