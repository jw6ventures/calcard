package dav

import (
	"net/http/httptest"
	"slices"
	"strconv"
	"strings"
	"testing"

	"github.com/jw6ventures/calcard/internal/auth"
	"github.com/jw6ventures/calcard/internal/store"
)

// The §9.6 projection and the §7.10 report are driven end to end here: a real
// PUT stores the octets through the real store, and the REPORT reads them back
// out of PostgreSQL. The in-memory suites pin the semantics; this pins that the
// projection survives a round trip through storage, which is where the raw
// octets it works from actually come from.
func TestPostgres_CalendarDataProjectionAndFreeBusyEndToEnd(t *testing.T) {
	database := newDAVPostgresStore(t)
	ctx := t.Context()
	user, err := database.Users.UpsertOAuthUser(ctx, "dav-projection", "dav-projection@example.test", "Projection", "Test")
	if err != nil {
		t.Fatalf("create user: %v", err)
	}
	calendar, err := database.Calendars.Create(ctx, store.Calendar{UserID: user.ID, Name: "Projection"})
	if err != nil {
		t.Fatalf("create calendar: %v", err)
	}
	h := NewDavServer(Options{Store: database})

	object := strings.Join([]string{
		"BEGIN:VCALENDAR",
		"VERSION:2.0",
		"PRODID:-//CalCard//Test//EN",
		"BEGIN:VTIMEZONE",
		"TZID:Review/Chicago",
		"BEGIN:STANDARD",
		"DTSTART:19701101T020000",
		"TZOFFSETFROM:-0500",
		"TZOFFSETTO:-0600",
		"RRULE:FREQ=YEARLY;BYMONTH=11;BYDAY=1SU",
		"END:STANDARD",
		"BEGIN:DAYLIGHT",
		"DTSTART:19700308T020000",
		"TZOFFSETFROM:-0600",
		"TZOFFSETTO:-0500",
		"RRULE:FREQ=YEARLY;BYMONTH=3;BYDAY=2SU",
		"END:DAYLIGHT",
		"END:VTIMEZONE",
		"BEGIN:VEVENT",
		"UID:daily-standup",
		"DTSTAMP:20240301T080000Z",
		"DTSTART;TZID=Review/Chicago:20240309T090000",
		"DTEND;TZID=Review/Chicago:20240309T093000",
		"RRULE:FREQ=DAILY;COUNT=5",
		"STATUS:TENTATIVE",
		"SUMMARY:Standup",
		"DESCRIPTION:Every morning",
		"END:VEVENT",
		"END:VCALENDAR",
		"",
	}, "\r\n")

	put := newCalendarPutRequest(objectPath(calendar.ID, "standup"), strings.NewReader(object))
	put = put.WithContext(auth.WithUser(put.Context(), user))
	putRR := httptest.NewRecorder()
	h.Put(putRR, put)
	if putRR.Code != 201 {
		t.Fatalf("PUT = %d, want 201: %s", putRR.Code, putRR.Body.String())
	}

	t.Run("expand returns one component per instance", func(t *testing.T) {
		value := reportCalendarDataValue(t, h, user, calendar.ID, `<C:calendar-data>
			<C:comp name="VCALENDAR"><C:comp name="VEVENT"><C:prop name="UID"/><C:prop name="DTSTART"/><C:prop name="SUMMARY"/></C:comp></C:comp>
			<C:expand start="20240309T000000Z" end="20240312T000000Z"/>
		</C:calendar-data>`)

		properties := icalendarPropertiesIn(t, value, "VCALENDAR", "VEVENT")
		if got := properties["RECURRENCE-ID"]; len(got) != 3 {
			t.Fatalf("expanded instances = %d, want 3; value:\n%s", len(got), value)
		}
		wantStarts := []string{"20240309T150000Z", "20240310T140000Z", "20240311T140000Z"}
		if got := properties["RECURRENCE-ID"]; !slices.Equal(got, wantStarts) {
			t.Errorf("RECURRENCE-IDs = %v, want %v; value:\n%s", got, wantStarts, value)
		}
		if got := properties["DTSTART"]; !slices.Equal(got, wantStarts) {
			t.Errorf("DTSTARTs = %v, want %v; value:\n%s", got, wantStarts, value)
		}
		if got := properties["SUMMARY"]; len(got) != 3 || got[0] != "Standup" {
			t.Errorf("SUMMARYs = %v, want one per instance; value:\n%s", got, value)
		}
		if got := properties["DESCRIPTION"]; len(got) != 0 {
			t.Errorf("an unselected property survived the projection: %v; value:\n%s", got, value)
		}
		if strings.Contains(strings.ToUpper(value), "RRULE") {
			t.Errorf("expanded output still carries a recurrence property; value:\n%s", value)
		}
	})

	t.Run("no selection returns the stored octets", func(t *testing.T) {
		value := reportCalendarDataValue(t, h, user, calendar.ID, `<C:calendar-data/>`)
		// §9.6 anticipates the one difference a round trip introduces: an XML
		// parser normalizes CRLF to a single LF, and the CR "MAY be omitted".
		// Everything else must be the octets that were stored.
		if want := strings.ReplaceAll(object, "\r\n", "\n"); value != want {
			t.Fatalf("unprojected calendar-data changed the stored octets:\n got %q\nwant %q", value, want)
		}
	})

	t.Run("free-busy honours Depth", func(t *testing.T) {
		body := `<C:free-busy-query xmlns:C="urn:ietf:params:xml:ns:caldav">` +
			`<C:time-range start="20240309T000000Z" end="20240312T000000Z"/></C:free-busy-query>`
		wantPeriods := []string{
			"FREEBUSY;FBTYPE=BUSY-TENTATIVE:20240309T150000Z/20240309T153000Z",
			"FREEBUSY;FBTYPE=BUSY-TENTATIVE:20240310T140000Z/20240310T143000Z",
			"FREEBUSY;FBTYPE=BUSY-TENTATIVE:20240311T140000Z/20240311T143000Z",
		}

		for depth, wantBusy := range map[string]bool{"": false, "0": false, "1": true} {
			req := reportRequestFor(collectionPath(calendar.ID), body, user)
			if depth != "" {
				req.Header.Set("Depth", depth)
			}
			rr := httptest.NewRecorder()
			h.Report(rr, req)

			if rr.Code != 200 {
				t.Fatalf("Depth %q: status = %d, want 200: %s", depth, rr.Code, rr.Body.String())
			}
			periods := parsedFreeBusyLines(t, rr.Body.String())
			if busy := len(periods) != 0; busy != wantBusy {
				t.Errorf("Depth %q: published periods = %v, want busy = %v", depth, periods, wantBusy)
			}
			if wantBusy && !slices.Equal(periods, wantPeriods) {
				t.Errorf("Depth %q: published periods = %v, want %v", depth, periods, wantPeriods)
			}
		}
	})
}

func collectionPath(calendarID int64) string {
	return "/dav/calendars/" + strconv.FormatInt(calendarID, 10) + "/"
}

func objectPath(calendarID int64, name string) string {
	return collectionPath(calendarID) + name + ".ics"
}

// reportCalendarDataValue runs a calendar-query carrying the given
// CALDAV:calendar-data selector and returns the value it served.
func reportCalendarDataValue(t *testing.T, h *DavServer, user *store.User, calendarID int64, calendarData string) string {
	t.Helper()
	body := `<?xml version="1.0" encoding="utf-8"?>
<C:calendar-query xmlns:D="DAV:" xmlns:C="urn:ietf:params:xml:ns:caldav">
  <D:prop><D:getetag/>` + calendarData + `</D:prop>
  <C:filter><C:comp-filter name="VCALENDAR"><C:comp-filter name="VEVENT"/></C:comp-filter></C:filter>
</C:calendar-query>`

	req := reportRequestFor(collectionPath(calendarID), body, user)
	req.Header.Set("Depth", "1")
	rr := httptest.NewRecorder()
	h.Report(rr, req)
	if rr.Code != 207 {
		t.Fatalf("REPORT = %d, want 207: %s", rr.Code, rr.Body.String())
	}

	el := decodeMultistatus(t, rr).
		responseForHref(t, objectPath(calendarID, "standup")).
		assertPropStatus(t, calQN("calendar-data"), 200)
	value, err := scalarText(el)
	if err != nil {
		t.Fatal(err)
	}
	return value
}

// syncCollectionAgainst runs one RFC 6578 sync-collection over the collection
// and returns the multistatus with the token the server answered.
func syncCollectionAgainst(t *testing.T, h *DavServer, user *store.User, calendarID int64, syncToken string) davMultistatus {
	t.Helper()
	body := `<?xml version="1.0" encoding="utf-8"?>
<D:sync-collection xmlns:D="DAV:"><D:sync-token>` + syncToken +
		`</D:sync-token><D:prop><D:getetag/></D:prop></D:sync-collection>`
	req := reportRequestFor(collectionPath(calendarID), body, user)
	req.Header.Set("Depth", "1")
	rr := httptest.NewRecorder()
	h.Report(rr, req)
	return decodeMultistatus(t, rr)
}

// The incremental sync reads two statements the in-memory suites only pin as
// SQL text: the keyset page of modified rows, and the tombstone page behind it.
// Both are driven here through real storage, so a query the fakes accept but
// PostgreSQL rejects -- or one whose keyset resume is wrong -- fails.
func TestPostgres_SyncCollectionReadsChangesAndRemovalsThroughStorage(t *testing.T) {
	database := newDAVPostgresStore(t)
	ctx := t.Context()
	user, err := database.Users.UpsertOAuthUser(ctx, "dav-sync", "dav-sync@example.test", "Sync", "Test")
	if err != nil {
		t.Fatalf("create user: %v", err)
	}
	calendar, err := database.Calendars.Create(ctx, store.Calendar{UserID: user.ID, Name: "Sync"})
	if err != nil {
		t.Fatalf("create calendar: %v", err)
	}
	h := NewDavServer(Options{Store: database})

	object := func(uid string) string {
		return strings.Join([]string{
			"BEGIN:VCALENDAR", "VERSION:2.0", "PRODID:-//CalCard//Test//EN",
			"BEGIN:VEVENT", "UID:" + uid, "DTSTAMP:20240301T080000Z",
			"DTSTART:20240309T090000Z", "DTEND:20240309T093000Z", "SUMMARY:" + uid,
			"END:VEVENT", "END:VCALENDAR", "",
		}, "\r\n")
	}
	put := func(name string) {
		t.Helper()
		req := newCalendarPutRequest(objectPath(calendar.ID, name), strings.NewReader(object(name)))
		req = req.WithContext(auth.WithUser(req.Context(), user))
		rr := httptest.NewRecorder()
		h.Put(rr, req)
		if rr.Code != 201 && rr.Code != 204 {
			t.Fatalf("PUT %s = %d: %s", name, rr.Code, rr.Body.String())
		}
	}
	put("kept")
	put("removed")

	initial := syncCollectionAgainst(t, h, user, calendar.ID, "")
	if initial.SyncToken == "" {
		t.Fatal("initial sync returned no sync-token")
	}
	// The collection and both resources.
	if len(initial.Responses) != 3 {
		t.Fatalf("initial sync responses = %d, want 3", len(initial.Responses))
	}

	put("kept")
	del := httptest.NewRequest("DELETE", objectPath(calendar.ID, "removed"), nil)
	del = del.WithContext(auth.WithUser(del.Context(), user))
	delRR := httptest.NewRecorder()
	h.Delete(delRR, del)
	if delRR.Code != 204 {
		t.Fatalf("DELETE = %d: %s", delRR.Code, delRR.Body.String())
	}

	incremental := syncCollectionAgainst(t, h, user, calendar.ID, initial.SyncToken)
	changed := map[string]string{}
	for _, response := range incremental.Responses {
		for _, href := range response.Hrefs {
			changed[href] = response.Status
		}
	}
	keptHref := objectPath(calendar.ID, "kept")
	removedHref := objectPath(calendar.ID, "removed")
	if _, ok := changed[keptHref]; !ok {
		t.Fatalf("incremental sync omitted the modified resource; got %v", changed)
	}
	if status, ok := changed[removedHref]; !ok || !strings.Contains(status, "404") {
		t.Fatalf("incremental sync did not report the removal as 404; got %q for %s in %v", status, removedHref, changed)
	}
}
