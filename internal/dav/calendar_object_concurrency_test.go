package dav

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	"github.com/jw6ventures/calcard/internal/auth"
	"github.com/jw6ventures/calcard/internal/store"
)

type concurrentCalendarObjectBackend struct {
	*fakeEventRepo
	mu     sync.Mutex
	events map[string]*store.Event
}

type immutableCalendarRepo struct {
	*fakeCalendarRepo
	calendar store.Calendar
}

func (r *immutableCalendarRepo) GetByID(_ context.Context, id int64) (*store.Calendar, error) {
	if id != r.calendar.ID {
		return nil, nil
	}
	copy := r.calendar
	return &copy, nil
}

func (r *immutableCalendarRepo) GetAccessible(_ context.Context, calendarID, userID int64) (*store.CalendarAccess, error) {
	if calendarID != r.calendar.ID || userID != r.calendar.UserID {
		return nil, nil
	}
	return &store.CalendarAccess{Calendar: r.calendar, Editor: true}, nil
}

func newConcurrentCalendarObjectBackend() *concurrentCalendarObjectBackend {
	return &concurrentCalendarObjectBackend{
		fakeEventRepo: &fakeEventRepo{},
		events:        make(map[string]*store.Event),
	}
}

func (b *concurrentCalendarObjectBackend) GetByUID(_ context.Context, calendarID int64, uid string) (*store.Event, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return cloneEvent(b.events[fmt.Sprintf("%d:%s", calendarID, uid)]), nil
}

func (b *concurrentCalendarObjectBackend) GetByResourceName(_ context.Context, calendarID int64, resourceName string) (*store.Event, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return cloneEvent(eventByResourceName(b.events, calendarID, resourceName)), nil
}

func (b *concurrentCalendarObjectBackend) PutCalendarObject(_ context.Context, write store.CalendarObjectWrite) (*store.CalendarObjectWriteResult, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	existing := eventByResourceName(b.events, write.CalendarID, write.ResourceName)
	if write.ExpectedState != nil && write.ExpectedState.Exists != (existing != nil) {
		return nil, store.ErrResourceStateChanged
	}
	byUID := b.events[fmt.Sprintf("%d:%s", write.CalendarID, write.UID)]
	if existing != nil && existing.UID != write.UID {
		return &store.CalendarObjectWriteResult{Conflict: cloneEvent(existing)}, store.ErrUIDConflict
	}
	if byUID != nil && eventResourceName(*byUID) != write.ResourceName {
		return &store.CalendarObjectWriteResult{Conflict: cloneEvent(byUID)}, store.ErrUIDConflict
	}

	event := &store.Event{
		ID:           int64(len(b.events) + 1),
		CalendarID:   write.CalendarID,
		UID:          write.UID,
		ResourceName: write.ResourceName,
		RawICAL:      write.RawICAL,
		ETag:         write.ETag,
	}
	created := existing == nil
	b.events[fmt.Sprintf("%d:%s", write.CalendarID, write.UID)] = event
	return &store.CalendarObjectWriteResult{Event: cloneEvent(event), Created: created}, nil
}

func eventByResourceName(events map[string]*store.Event, calendarID int64, resourceName string) *store.Event {
	for _, event := range events {
		if event.CalendarID == calendarID && eventResourceName(*event) == resourceName {
			return event
		}
	}
	return nil
}

func cloneEvent(event *store.Event) *store.Event {
	if event == nil {
		return nil
	}
	copy := *event
	return &copy
}

func TestRFC4791_ConcurrentHTTPPutsReturnOneCreatedAndUIDConflicts(t *testing.T) {
	backend := newConcurrentCalendarObjectBackend()
	calRepo := &immutableCalendarRepo{
		fakeCalendarRepo: &fakeCalendarRepo{},
		calendar:         store.Calendar{ID: 1, UserID: 1, Name: "Test"},
	}
	h := NewDavServer(Options{Store: &store.Store{
		Calendars:       calRepo,
		Events:          backend,
		CalendarObjects: backend,
	}})
	user := &store.User{ID: 1}

	const writers = 8
	type response struct {
		name string
		rr   *httptest.ResponseRecorder
	}
	responses := make([]response, writers)
	start := make(chan struct{})
	var wg sync.WaitGroup
	for i := 0; i < writers; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			<-start
			name := fmt.Sprintf("resource-%d", i)
			body := buildCalendarObject(buildVEvent("shared-uid", fmt.Sprintf("SUMMARY:Writer %d", i)))
			req := newCalendarPutRequest("/dav/calendars/1/"+name+".ics", strings.NewReader(body))
			req = req.WithContext(auth.WithUser(req.Context(), user))
			rr := httptest.NewRecorder()
			h.Put(rr, req)
			responses[i] = response{name: name, rr: rr}
		}(i)
	}
	close(start)
	wg.Wait()

	created := ""
	for _, response := range responses {
		switch response.rr.Code {
		case http.StatusCreated:
			if created != "" {
				t.Fatalf("both %q and %q were created for one UID", created, response.name)
			}
			created = response.name
		case http.StatusConflict:
		default:
			t.Fatalf("PUT %q = %d, want 201 or 409: %s", response.name, response.rr.Code, response.rr.Body.String())
		}
	}
	if created == "" {
		t.Fatal("no competing PUT created the calendar object")
	}
	wantHref := "/dav/calendars/1/" + created + ".ics"
	for _, response := range responses {
		if response.name != created {
			assertUIDConflict(t, response.rr, wantHref)
		}
	}
}
