package dav

import (
	"fmt"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/jw6ventures/calcard/internal/auth"
	"github.com/jw6ventures/calcard/internal/store"
)

// A client following truncated pages has to end holding the collection as it
// is, whatever changes while it pages: a change to a resource it already holds
// is reported again, a removal of one is reported, and a resource added or
// removed before its page arrives is reported or left out accordingly. Each
// page is read from one snapshot of storage, which is what lets its token pass
// the changes it holds without passing one committed beside them.
func TestPostgres_SyncCollectionPagesThroughAChangingCalendar(t *testing.T) {
	database := newDAVPostgresStore(t)
	ctx := t.Context()
	user, err := database.Users.UpsertOAuthUser(ctx, "sync-pages", "sync-pages@example.test", "Sync", "Pages")
	if err != nil {
		t.Fatal(err)
	}
	cal, err := database.Calendars.Create(ctx, store.Calendar{UserID: user.ID, Name: "Pages"})
	if err != nil {
		t.Fatal(err)
	}
	put := func(uid, name, summary string) {
		t.Helper()
		raw := "BEGIN:VCALENDAR\r\nVERSION:2.0\r\nPRODID:-//CalCard//Test//EN\r\nBEGIN:VEVENT\r\nUID:" + uid +
			"\r\nDTSTAMP:20260101T000000Z\r\nDTSTART:20260105T100000Z\r\nSUMMARY:" + summary + "\r\nEND:VEVENT\r\nEND:VCALENDAR\r\n"
		if _, err := database.Events.Upsert(ctx, store.Event{CalendarID: cal.ID, UID: uid, ResourceName: name, RawICAL: raw, ETag: uid + "-" + summary}); err != nil {
			t.Fatalf("put %s: %v", uid, err)
		}
	}
	remove := func(uid string) {
		t.Helper()
		if err := database.Events.DeleteByUID(ctx, cal.ID, uid); err != nil {
			t.Fatalf("delete %s: %v", uid, err)
		}
	}
	for i := 1; i <= 15; i++ {
		name := fmt.Sprintf("event-%02d", i)
		put(name, name, "v1")
	}

	h := NewDavServer(Options{Store: database})
	collectionHref := fmt.Sprintf("/dav/calendars/%d/", cal.ID)
	report := func(token string) *httptest.ResponseRecorder {
		req := httptest.NewRequest("REPORT", collectionHref, strings.NewReader(syncCollectionLimitBody(token, 4)))
		req.Header.Set("Depth", "1")
		req = req.WithContext(auth.WithUser(req.Context(), user))
		rr := httptest.NewRecorder()
		h.Report(rr, req)
		return rr
	}
	href := func(name string) string { return collectionHref + name + ".ics" }

	walk := walkSyncChanging(t, report, collectionHref, "", 20, func(page int) {
		switch page {
		case 1:
			// The first page reported event-01 to event-04.
			remove("event-02")
			put("event-03", "event-03", "v2")
			put("event-16", "event-16", "v1")
			remove("event-09")
		case 2:
			// A name freed after its first report is bound again.
			put("event-02-again", "event-02", "v1")
		}
	})

	live, err := database.Events.ListForCalendar(ctx, cal.ID)
	if err != nil {
		t.Fatal(err)
	}
	want := map[string]bool{}
	for _, event := range live {
		want[href(eventResourceName(event))] = true
	}
	for h, present := range walk.state {
		if present && !want[h] {
			t.Errorf("the client holds %s, which the calendar does not", h)
		}
	}
	for h := range want {
		if !walk.state[h] {
			t.Errorf("the client does not hold %s, which the calendar does", h)
		}
	}
	if walk.reported[href("event-03")] < 2 {
		t.Errorf("event-03 changed after its first report but was reported %d times", walk.reported[href("event-03")])
	}

	// The final token names the state the client now holds.
	again := walkSync(t, report, collectionHref, walk.token, 2)
	if len(again.state) != 0 {
		t.Fatalf("a sync from the final token reported %v, want nothing", again.state)
	}
}
