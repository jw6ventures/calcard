package dav

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jw6ventures/calcard/internal/acl"
	"github.com/jw6ventures/calcard/internal/auth"
	"github.com/jw6ventures/calcard/internal/store"
)

// independentDAVWriters is how many clients the independent-write tests hold at
// the barrier before releasing them at one collection. Bulk sync from Apple
// Calendar and Thunderbird runs at least this wide, and it is above
// maxResourceStateRetries, so a member write gated on collection-wide state
// runs out of retries here instead of passing by luck.
const independentDAVWriters = 20

type calendarReadBarrier struct {
	store.CalendarRepository
	bookID  int64
	reads   atomic.Int32
	ready   sync.WaitGroup
	release chan struct{}
}

func (b *calendarReadBarrier) GetByID(ctx context.Context, id int64) (*store.Calendar, error) {
	book, err := b.CalendarRepository.GetByID(ctx, id)
	if id == b.bookID && b.reads.Add(1) <= independentDAVWriters {
		b.ready.Done()
		<-b.release
	}
	return book, err
}
func TestPostgresIndependentCalendarWrites(t *testing.T) {
	for _, method := range []string{"create", "update", "DELETE", "COPY", "MOVE"} {
		t.Run(method, func(t *testing.T) {
			db := newDAVPostgresStore(t)
			user, err := db.Users.UpsertOAuthUser(t.Context(), "concurrent", "concurrent@example.test", "Concurrent", "Concurrent")
			if err != nil {
				t.Fatal(err)
			}
			book, err := db.Calendars.Create(t.Context(), store.Calendar{UserID: user.ID, Name: "Source"})
			if err != nil {
				t.Fatal(err)
			}
			dest, err := db.Calendars.Create(t.Context(), store.Calendar{UserID: user.ID, Name: "Destination"})
			if err != nil {
				t.Fatal(err)
			}
			h := NewDavServer(Options{Store: db})
			bodies := make([]string, independentDAVWriters)
			etags := make([]string, independentDAVWriters)
			for i := range bodies {
				bodies[i] = fmt.Sprintf("BEGIN:VCALENDAR\r\nVERSION:2.0\r\nPRODID:-//Review//EN\r\nBEGIN:VEVENT\r\nUID:c%d\r\nSUMMARY:Event %d\r\nDTSTAMP:20260101T000000Z\r\nDTSTART:20260101T100000Z\r\nDTEND:20260101T110000Z\r\nX-REVIEW:%d\r\nEND:VEVENT\r\nEND:VCALENDAR\r\n", i, i, i)
				if method != "create" {
					req := httptest.NewRequest(http.MethodPut, fmt.Sprintf("/dav/calendars/%d/c%d.ics", book.ID, i), strings.NewReader(bodies[i]))
					req.Header.Set("Content-Type", "text/calendar")
					rr := httptest.NewRecorder()
					h.ServeHTTP(rr, req.WithContext(auth.WithUser(req.Context(), user)))
					if rr.Code != http.StatusCreated {
						t.Fatalf("seed: %d %s", rr.Code, rr.Body.String())
					}
					etags[i] = rr.Header().Get("ETag")
				}
			}
			barrier := &calendarReadBarrier{CalendarRepository: db.Calendars, bookID: book.ID, release: make(chan struct{})}
			if method == "COPY" || method == "MOVE" {
				barrier.bookID = dest.ID
			}
			barrier.ready.Add(independentDAVWriters)
			db.Calendars = barrier
			results := make(chan *httptest.ResponseRecorder, independentDAVWriters)
			for i := 0; i < independentDAVWriters; i++ {
				go func(i int) {
					verb := method
					if method == "create" || method == "update" {
						verb = http.MethodPut
					}
					req := httptest.NewRequest(verb, fmt.Sprintf("/dav/calendars/%d/c%d.ics", book.ID, i), strings.NewReader(strings.ReplaceAll(bodies[i], "SUMMARY:Event", "SUMMARY:Updated")))
					req.Header.Set("Content-Type", "text/calendar")
					if etags[i] != "" {
						req.Header.Set("If-Match", etags[i])
					}
					if method == "COPY" || method == "MOVE" {
						req.Header.Set("Destination", fmt.Sprintf("/dav/calendars/%d/c%d.ics", dest.ID, i))
					}
					rr := httptest.NewRecorder()
					h.ServeHTTP(rr, req.WithContext(auth.WithUser(req.Context(), user)))
					results <- rr
				}(i)
			}
			barrier.ready.Wait()
			close(barrier.release)
			want := http.StatusCreated
			if method == "update" || method == "DELETE" {
				want = http.StatusNoContent
			}
			for i := 0; i < independentDAVWriters; i++ {
				rr := <-results
				if rr.Code != want {
					t.Errorf("independent %s = %d, want %d: %s", method, rr.Code, want, rr.Body.String())
				}
			}
		})
	}
}

// TestPostgresBulkConcurrentCalendarPuts is the bulk-upload shape: one client
// pushing a calendar's worth of distinct objects over parallel connections.
// None of the writes touches another's resource, so every one of them owes the
// client a 201 no matter how wide the fan-out runs.
func TestPostgresBulkConcurrentCalendarPuts(t *testing.T) {
	for _, writers := range []int{2, 5, 10, 20, 40} {
		t.Run(fmt.Sprintf("writers=%d", writers), func(t *testing.T) {
			db := newDAVPostgresStore(t)
			user, err := db.Users.UpsertOAuthUser(t.Context(), "bulk", "bulk@example.test", "Bulk", "Bulk")
			if err != nil {
				t.Fatal(err)
			}
			calendar, err := db.Calendars.Create(t.Context(), store.Calendar{UserID: user.ID, Name: "Bulk"})
			if err != nil {
				t.Fatal(err)
			}
			h := NewDavServer(Options{Store: db})

			var wg sync.WaitGroup
			codes := make([]int, writers)
			responses := make([]string, writers)
			start := make(chan struct{})
			for i := 0; i < writers; i++ {
				wg.Add(1)
				go func(i int) {
					defer wg.Done()
					body := fmt.Sprintf("BEGIN:VCALENDAR\r\nVERSION:2.0\r\nPRODID:-//Bulk//EN\r\nBEGIN:VEVENT\r\nUID:bulk%d\r\nSUMMARY:Event %d\r\nDTSTAMP:20260101T000000Z\r\nDTSTART:20260101T100000Z\r\nDTEND:20260101T110000Z\r\nEND:VEVENT\r\nEND:VCALENDAR\r\n", i, i)
					req := httptest.NewRequest(http.MethodPut, fmt.Sprintf("/dav/calendars/%d/bulk%d.ics", calendar.ID, i), strings.NewReader(body))
					req.Header.Set("Content-Type", "text/calendar")
					rr := httptest.NewRecorder()
					<-start
					h.ServeHTTP(rr, req.WithContext(auth.WithUser(req.Context(), user)))
					codes[i] = rr.Code
					responses[i] = strings.TrimSpace(rr.Body.String())
				}(i)
			}
			close(start)
			wg.Wait()

			for i, code := range codes {
				if code != http.StatusCreated {
					t.Errorf("writer %d = %d, want 201: %s", i, code, responses[i])
				}
			}
			stored, err := db.Events.ListForCalendar(t.Context(), calendar.ID)
			if err != nil {
				t.Fatal(err)
			}
			if len(stored) != writers {
				t.Errorf("calendar holds %d objects, want %d", len(stored), writers)
			}
		})
	}
}

type eventReadMutation struct {
	store.EventRepository
	mutate func(*store.Event) error
	calls  int
}

func (r *eventReadMutation) GetByResourceName(ctx context.Context, calendarID int64, name string) (*store.Event, error) {
	event, err := r.EventRepository.GetByResourceName(ctx, calendarID, name)
	if err == nil && event != nil {
		r.calls++
		if err := r.mutate(event); err != nil {
			return nil, err
		}
	}
	return event, err
}

func TestPostgresCalendarDeleteRetryChecksCurrentState(t *testing.T) {
	for _, kind := range []string{"etag", "authorization", "retry limit"} {
		t.Run(kind, func(t *testing.T) {
			db := newDAVPostgresStore(t)
			ctx := t.Context()
			owner, err := db.Users.UpsertOAuthUser(ctx, "owner", "owner@example.test", "", "")
			if err != nil {
				t.Fatal(err)
			}
			guest, err := db.Users.UpsertOAuthUser(ctx, "guest", "guest@example.test", "", "")
			if err != nil {
				t.Fatal(err)
			}
			calendar, err := db.Calendars.Create(ctx, store.Calendar{UserID: owner.ID, Name: "Calendar"})
			if err != nil {
				t.Fatal(err)
			}
			root := fmt.Sprintf("/dav/calendars/%d", calendar.ID)
			if err := db.ACLEntries.SetACL(ctx, root, []store.ACLEntry{{PrincipalHref: acl.PrincipalHref(guest.ID), IsGrant: true, Privilege: "all"}}); err != nil {
				t.Fatal(err)
			}
			event := store.Event{CalendarID: calendar.ID, UID: "target", ResourceName: "target", ETag: "original", RawICAL: "BEGIN:VCALENDAR\r\nVERSION:2.0\r\nBEGIN:VEVENT\r\nUID:target\r\nDTSTART:20260101T090000Z\r\nEND:VEVENT\r\nEND:VCALENDAR\r\n"}
			if _, err := db.Events.Upsert(ctx, event); err != nil {
				t.Fatal(err)
			}
			repo := db.Events
			wrapper := &eventReadMutation{EventRepository: repo}
			// Each change lands after the handler's privilege check and before its
			// write. The authorization case changes only the ACL and leaves the
			// target untouched, so the write's own ACL re-check is all that can
			// catch it.
			wrapper.mutate = func(current *store.Event) error {
				if kind == "retry limit" {
					churned := *current
					churned.ETag = fmt.Sprint(wrapper.calls)
					_, err := repo.Upsert(ctx, churned)
					return err
				}
				if wrapper.calls > 1 {
					return nil
				}
				if kind == "authorization" {
					return db.ACLEntries.SetACL(ctx, root, []store.ACLEntry{{PrincipalHref: acl.PrincipalHref(guest.ID), IsGrant: true, Privilege: "read"}})
				}
				updated := *current
				updated.ETag = "changed"
				_, err := repo.Upsert(ctx, updated)
				return err
			}
			db.Events = wrapper
			h := NewDavServer(Options{Store: db})
			req := httptest.NewRequest(http.MethodDelete, root+"/target.ics", nil)
			if kind != "retry limit" {
				// The retry-limit case must reach the retry bound, so it carries no
				// conditional header that a refreshed read could answer 412 instead.
				req.Header.Set("If-Match", `"original"`)
			}
			rr := httptest.NewRecorder()
			h.ServeHTTP(rr, req.WithContext(auth.WithUser(ctx, guest)))
			want := http.StatusPreconditionFailed
			if kind == "authorization" {
				want = http.StatusForbidden
			}
			if kind == "retry limit" {
				want = http.StatusConflict
			}
			if rr.Code != want {
				t.Fatalf("status %d, want %d: %s", rr.Code, want, rr.Body.String())
			}
			retained, err := repo.GetByResourceName(ctx, calendar.ID, "target")
			if err != nil || retained == nil {
				t.Fatalf("target removed: %v", err)
			}
			if kind == "retry limit" && wrapper.calls != maxResourceStateRetries+1 {
				t.Fatalf("attempts=%d", wrapper.calls)
			}
		})
	}
}

// A LOCK can land on the target after the handler's own lock check passed. The
// write transaction still finds it, and the client is told the resource is
// locked, as a contact PUT tells it, rather than that the server failed.
func TestPostgresCalendarPutReportsALockTakenDuringTheRequest(t *testing.T) {
	for _, tt := range []struct{ name, suffix string }{
		{name: "canonical spelling"},
		// Earlier releases recorded object locks with the extension.
		{name: "legacy spelling", suffix: ".ics"},
	} {
		t.Run(tt.name, func(t *testing.T) { testCalendarPutReportsALockTakenDuringTheRequest(t, tt.suffix) })
	}
}

func testCalendarPutReportsALockTakenDuringTheRequest(t *testing.T, lockSuffix string) {
	db := newDAVPostgresStore(t)
	ctx := t.Context()
	owner, err := db.Users.UpsertOAuthUser(ctx, "owner", "owner@example.test", "", "")
	if err != nil {
		t.Fatal(err)
	}
	calendar, err := db.Calendars.Create(ctx, store.Calendar{UserID: owner.ID, Name: "Calendar"})
	if err != nil {
		t.Fatal(err)
	}
	body := func(summary string) string {
		return "BEGIN:VCALENDAR\r\nVERSION:2.0\r\nPRODID:-//Lock//EN\r\nBEGIN:VEVENT\r\nUID:target\r\nDTSTAMP:20260101T000000Z\r\nDTSTART:20260101T100000Z\r\nSUMMARY:" + summary + "\r\nEND:VEVENT\r\nEND:VCALENDAR\r\n"
	}
	h := NewDavServer(Options{Store: db})
	target := fmt.Sprintf("/dav/calendars/%d/target.ics", calendar.ID)
	put := func() *httptest.ResponseRecorder {
		req := httptest.NewRequest(http.MethodPut, target, strings.NewReader(body("changed")))
		req.Header.Set("Content-Type", "text/calendar")
		rr := httptest.NewRecorder()
		h.ServeHTTP(rr, req.WithContext(auth.WithUser(ctx, owner)))
		return rr
	}
	if rr := put(); rr.Code != http.StatusCreated {
		t.Fatalf("seed: %d %s", rr.Code, rr.Body.String())
	}

	repo := db.Events
	wrapper := &eventReadMutation{EventRepository: repo}
	wrapper.mutate = func(*store.Event) error {
		if wrapper.calls > 1 {
			return nil
		}
		_, err := db.Locks.Create(ctx, store.Lock{
			Token: "opaquelocktoken:concurrent", ResourcePath: strings.TrimSuffix(target, ".ics") + lockSuffix, UserID: owner.ID,
			LockScope: "exclusive", LockType: "write", Depth: "0", TimeoutSeconds: 300, ExpiresAt: time.Now().Add(5 * time.Minute),
		})
		return err
	}
	db.Events = wrapper

	if rr := put(); rr.Code != http.StatusLocked {
		t.Fatalf("PUT after a concurrent LOCK = %d, want 423: %s", rr.Code, rr.Body.String())
	}
}

// aclReadMutation runs mutate once, right after the first ACL read of path,
// which is the read the request pins its ACL on.
type aclReadMutation struct {
	store.ACLRepository
	path   string
	mutate func() error
	done   bool
}

func (r *aclReadMutation) ListByResource(ctx context.Context, resourcePath string) ([]store.ACLEntry, error) {
	entries, err := r.ACLRepository.ListByResource(ctx, resourcePath)
	if err == nil && resourcePath == r.path && !r.done {
		r.done = true
		if err := r.mutate(); err != nil {
			return nil, err
		}
	}
	return entries, err
}

// A PROPPATCH on a collection is authorized against the collection's ACL like a
// write to a member. A downgrade landing after the privilege check has to stop
// the write, and the retry answers from the new ACL.
func TestPostgresCollectionProppatchRechecksAChangedACL(t *testing.T) {
	db := newDAVPostgresStore(t)
	ctx := t.Context()
	owner, err := db.Users.UpsertOAuthUser(ctx, "owner", "owner@example.test", "", "")
	if err != nil {
		t.Fatal(err)
	}
	guest, err := db.Users.UpsertOAuthUser(ctx, "guest", "guest@example.test", "", "")
	if err != nil {
		t.Fatal(err)
	}
	calendar, err := db.Calendars.Create(ctx, store.Calendar{UserID: owner.ID, Name: "Original"})
	if err != nil {
		t.Fatal(err)
	}
	root := fmt.Sprintf("/dav/calendars/%d", calendar.ID)
	grant := func(privilege string) error {
		return db.ACLEntries.SetACL(ctx, root, []store.ACLEntry{{PrincipalHref: acl.PrincipalHref(guest.ID), IsGrant: true, Privilege: privilege}})
	}
	if err := grant("all"); err != nil {
		t.Fatal(err)
	}
	repo := db.ACLEntries
	db.ACLEntries = &aclReadMutation{ACLRepository: repo, path: root, mutate: func() error { return grant("read") }}
	h := NewDavServer(Options{Store: db})

	body := `<?xml version="1.0" encoding="utf-8"?><D:propertyupdate xmlns:D="DAV:"><D:set><D:prop><D:displayname>Renamed</D:displayname></D:prop></D:set></D:propertyupdate>`
	req := httptest.NewRequest("PROPPATCH", root+"/", strings.NewReader(body))
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, req.WithContext(auth.WithUser(ctx, guest)))
	if rr.Code != http.StatusForbidden {
		t.Fatalf("PROPPATCH after the guest lost write-properties = %d, want 403: %s", rr.Code, rr.Body.String())
	}
	stored, err := db.Calendars.GetByID(ctx, calendar.ID)
	if err != nil {
		t.Fatal(err)
	}
	if stored.Name != "Original" {
		t.Fatalf("calendar renamed to %q by a request authorized against a revoked grant", stored.Name)
	}
}

// A whole-collection COPY by a sharee is authorized against the source's ACL.
// A revocation landing after the privilege check has to stop the copy, and
// the retry answers from the new ACL.
func TestPostgresCalendarCollectionCopyRechecksAChangedACL(t *testing.T) {
	db := newDAVPostgresStore(t)
	ctx := t.Context()
	owner, err := db.Users.UpsertOAuthUser(ctx, "owner", "owner@example.test", "", "")
	if err != nil {
		t.Fatal(err)
	}
	guest, err := db.Users.UpsertOAuthUser(ctx, "guest", "guest@example.test", "", "")
	if err != nil {
		t.Fatal(err)
	}
	calendar, err := db.Calendars.Create(ctx, store.Calendar{UserID: owner.ID, Name: "Shared"})
	if err != nil {
		t.Fatal(err)
	}
	root := fmt.Sprintf("/dav/calendars/%d", calendar.ID)
	if err := db.ACLEntries.SetACL(ctx, root, []store.ACLEntry{{PrincipalHref: acl.PrincipalHref(guest.ID), IsGrant: true, Privilege: "read"}}); err != nil {
		t.Fatal(err)
	}
	repo := db.ACLEntries
	db.ACLEntries = &aclReadMutation{ACLRepository: repo, path: root, mutate: func() error { return repo.SetACL(ctx, root, nil) }}
	h := NewDavServer(Options{Store: db})

	req := httptest.NewRequest("COPY", root+"/", nil)
	req.Header.Set("Destination", "/dav/calendars/guest-copy/")
	req.Header.Set("Depth", "infinity")
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, req.WithContext(auth.WithUser(ctx, guest)))
	if rr.Code != http.StatusForbidden && rr.Code != http.StatusNotFound {
		t.Fatalf("COPY after the guest lost read = %d, want 403 or 404: %s", rr.Code, rr.Body.String())
	}
	copies, err := db.Calendars.ListByUser(ctx, guest.ID)
	if err != nil {
		t.Fatal(err)
	}
	if len(copies) != 0 {
		t.Fatalf("guest holds %d calendars copied under a revoked grant", len(copies))
	}
}
