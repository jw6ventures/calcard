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

	"github.com/jw6ventures/calcard/internal/acl"
	"github.com/jw6ventures/calcard/internal/auth"
	"github.com/jw6ventures/calcard/internal/store"
)

type calendarReadBarrier struct {
	store.CalendarRepository
	bookID  int64
	reads   atomic.Int32
	ready   sync.WaitGroup
	release chan struct{}
}

func (b *calendarReadBarrier) GetByID(ctx context.Context, id int64) (*store.Calendar, error) {
	book, err := b.CalendarRepository.GetByID(ctx, id)
	if id == b.bookID && b.reads.Add(1) <= 2 {
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
			bodies := make([]string, 2)
			etags := make([]string, 2)
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
			barrier.ready.Add(2)
			db.Calendars = barrier
			results := make(chan *httptest.ResponseRecorder, 2)
			for i := 0; i < 2; i++ {
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
			for i := 0; i < 2; i++ {
				rr := <-results
				if rr.Code != want {
					t.Errorf("independent %s = %d, want %d: %s", method, rr.Code, want, rr.Body.String())
				}
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
			wrapper.mutate = func(current *store.Event) error {
				if kind == "retry limit" {
					other := event
					other.UID = "other"
					other.ResourceName = "other"
					other.RawICAL = strings.ReplaceAll(event.RawICAL, "UID:target", "UID:other")
					other.ETag = fmt.Sprint(wrapper.calls)
					_, err := repo.Upsert(ctx, other)
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
			req.Header.Set("If-Match", `"original"`)
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
