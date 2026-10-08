package dav

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/jw6ventures/calcard/internal/auth"
	"github.com/jw6ventures/calcard/internal/ical"
	"github.com/jw6ventures/calcard/internal/store"
)

// A stored resource may carry a byte order mark, which PUT strips. COPY and
// MOVE, of the resource or of its whole collection, store the octets as they
// are stored, mark included, so a MOVE keeps the resource's ETag; a COPY is a
// new resource with an ETag of its own.
func TestPostgres_HTTPTransfersStoreValidatedBodies(t *testing.T) {
	body := buildCalendarObject(buildVEvent("bom-member", "SUMMARY:Stored with a BOM"))
	tests := []struct {
		name        string
		method      string
		source      func(calendarID int64) string
		destination string
		target      func(destinationCalendarID int64) string
		collection  bool
	}{
		{name: "object COPY", method: "COPY", source: bomMemberPath, destination: "/dav/calendars/%d/copied.ics", target: func(id int64) string { return fmt.Sprintf("/dav/calendars/%d/copied.ics", id) }},
		{name: "object MOVE", method: "MOVE", source: bomMemberPath, destination: "/dav/calendars/%d/moved.ics", target: func(id int64) string { return fmt.Sprintf("/dav/calendars/%d/moved.ics", id) }},
		{name: "collection COPY", method: "COPY", source: bomCollectionPath, destination: "/dav/calendars/copied-calendar", collection: true},
		{name: "collection MOVE", method: "MOVE", source: bomCollectionPath, destination: "/dav/calendars/moved-calendar", collection: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			database := newDAVPostgresStore(t)
			ctx := t.Context()
			user, err := database.Users.UpsertOAuthUser(ctx, "bom-transfer", "bom-transfer@example.test", "BOM Transfer", "Test")
			if err != nil {
				t.Fatalf("create user: %v", err)
			}
			source, err := database.Calendars.Create(ctx, store.Calendar{UserID: user.ID, Name: "Source"})
			if err != nil {
				t.Fatalf("create source calendar: %v", err)
			}
			destination, err := database.Calendars.Create(ctx, store.Calendar{UserID: user.ID, Name: "Destination"})
			if err != nil {
				t.Fatalf("create destination calendar: %v", err)
			}
			if _, err := database.PutCalendarObject(ctx, store.CalendarObjectWrite{
				CalendarID: source.ID, UID: "bom-member", ResourceName: "bom-member",
				RawICAL: ical.ByteOrderMark + body, ETag: "bom-etag",
			}); err != nil {
				t.Fatalf("seed BOM member: %v", err)
			}
			h := NewDavServer(Options{Store: database})

			destinationHeader := tt.destination
			if !tt.collection {
				destinationHeader = fmt.Sprintf(tt.destination, destination.ID)
			}
			req := httptest.NewRequest(tt.method, tt.source(source.ID), nil)
			req.Header.Set("Destination", destinationHeader)
			req = req.WithContext(auth.WithUser(req.Context(), user))
			rr := httptest.NewRecorder()
			h.ServeHTTP(rr, req)
			if rr.Code != http.StatusCreated {
				t.Fatalf("%s status = %d, want 201: %s", tt.method, rr.Code, rr.Body.String())
			}

			target := ""
			if tt.collection {
				location := rr.Header().Get("Location")
				if location == "" {
					t.Fatal("collection transfer returned no Location")
				}
				target = strings.TrimSuffix(location, "/") + "/bom-member.ics"
			} else {
				target = tt.target(destination.ID)
			}
			get := httptest.NewRequest(http.MethodGet, target, nil)
			get = get.WithContext(auth.WithUser(get.Context(), user))
			got := httptest.NewRecorder()
			h.ServeHTTP(got, get)
			if got.Code != http.StatusOK {
				t.Fatalf("GET %s = %d: %s", target, got.Code, got.Body.String())
			}
			if got.Body.String() != ical.ByteOrderMark+body {
				t.Errorf("transferred body = %q, want the stored octets, mark included", got.Body.String())
			}
			etag := strings.Trim(got.Header().Get("ETag"), `"`)
			if wantKept := tt.method == "MOVE"; etag == "" || (etag == "bom-etag") != wantKept {
				t.Errorf("%s ETag = %q, want the stored ETag kept only by a MOVE", tt.method, etag)
			}
		})
	}
}

func bomMemberPath(calendarID int64) string {
	return fmt.Sprintf("/dav/calendars/%d/bom-member.ics", calendarID)
}

func bomCollectionPath(calendarID int64) string {
	return fmt.Sprintf("/dav/calendars/%d/", calendarID)
}

// A member whose columns were read otherwise than the validator reads its body
// -- here an Outlook-zone event stored with its wall clocks as UTC and no end
// -- moves with its body and ETag, and with the columns a write of that body
// stores: a collection MOVE leaves the same columns a MOVE of the object alone
// does.
func TestPostgres_MovesStoreTheColumnsTheValidatorReads(t *testing.T) {
	body := outlookEasternEvent()
	wallAsUTC := utcInstant(t, "20240304T090000Z")
	wantStart, wantEnd := utcInstant(t, "20240304T140000Z"), utcInstant(t, "20240304T150000Z")
	for _, collection := range []bool{false, true} {
		name := "object MOVE"
		if collection {
			name = "collection MOVE"
		}
		t.Run(name, func(t *testing.T) {
			database := newDAVPostgresStore(t)
			ctx := t.Context()
			user, err := database.Users.UpsertOAuthUser(ctx, "column-move", "column-move@example.test", "Column Move", "Test")
			if err != nil {
				t.Fatalf("create user: %v", err)
			}
			source, err := database.Calendars.Create(ctx, store.Calendar{UserID: user.ID, Name: "Source"})
			if err != nil {
				t.Fatalf("create source calendar: %v", err)
			}
			destination, err := database.Calendars.Create(ctx, store.Calendar{UserID: user.ID, Name: "Destination"})
			if err != nil {
				t.Fatalf("create destination calendar: %v", err)
			}
			if _, err := database.PutCalendarObject(ctx, store.CalendarObjectWrite{
				CalendarID: source.ID, UID: "outlook", ResourceName: "outlook", RawICAL: body, ETag: "stored-etag",
				Metadata: &store.EventWriteMetadata{DTStart: &wallAsUTC},
			}); err != nil {
				t.Fatalf("seed member: %v", err)
			}
			h := NewDavServer(Options{Store: database})
			path, header := fmt.Sprintf("/dav/calendars/%d/outlook.ics", source.ID), fmt.Sprintf("/dav/calendars/%d/outlook.ics", destination.ID)
			if collection {
				path, header = fmt.Sprintf("/dav/calendars/%d/", source.ID), "/dav/calendars/moved-calendar"
			}
			req := httptest.NewRequest("MOVE", path, nil)
			req.Header.Set("Destination", header)
			rr := httptest.NewRecorder()
			h.ServeHTTP(rr, req.WithContext(auth.WithUser(req.Context(), user)))
			if rr.Code != http.StatusCreated {
				t.Fatalf("MOVE = %d: %s", rr.Code, rr.Body.String())
			}

			calendars, err := database.Calendars.ListAccessible(ctx, user.ID)
			if err != nil {
				t.Fatal(err)
			}
			var moved *store.Event
			for _, calendar := range calendars {
				if event, err := database.Events.GetByUID(ctx, calendar.ID, "outlook"); err == nil && event != nil {
					moved = event
				}
			}
			if moved == nil {
				t.Fatal("the member is nowhere after the MOVE")
			}
			if moved.RawICAL != body || moved.ETag != "stored-etag" {
				t.Errorf("moved body or ETag changed: %q, %q", moved.ETag, moved.RawICAL)
			}
			if moved.DTStart == nil || !moved.DTStart.Equal(wantStart) || moved.DTEnd == nil || !moved.DTEnd.Equal(wantEnd) {
				t.Errorf("moved dtstart, dtend = %v, %v; want %v, %v", moved.DTStart, moved.DTEnd, wantStart, wantEnd)
			}
		})
	}
}
