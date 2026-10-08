package events

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"slices"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/jw6ventures/calcard/internal/acl"
	"github.com/jw6ventures/calcard/internal/calobject"
	"github.com/jw6ventures/calcard/internal/dav"
	"github.com/jw6ventures/calcard/internal/ical"
	"github.com/jw6ventures/calcard/internal/store"
	"github.com/jw6ventures/calcard/internal/ui/utils"
)

func TestNewServiceRequiresAValidator(t *testing.T) {
	defer func() {
		if recover() == nil {
			t.Fatal("NewService with a nil validator did not panic; the service would store calendar data unvalidated")
		}
	}()
	NewService(&store.Store{}, nil)
}

// Every write goes through the validator the service was built with, and its
// refusal reaches the caller as this API's error.
func TestServiceWritesThroughItsValidator(t *testing.T) {
	var validated []string
	refuse := func(raw string, cal *store.CalendarAccess) (*calobject.Object, error) {
		validated = append(validated, raw)
		return nil, calobject.NewError(http.StatusForbidden, []string{"max-instances"}, nil)
	}
	repo := &fakeEventRepo{events: map[string]store.Event{}}
	svc := NewService(&store.Store{
		Calendars: &fakeCalendarRepo{calendars: map[int64]*store.CalendarAccess{
			1: {Calendar: store.Calendar{ID: 1, UserID: 1, Name: "Work"}, Editor: true},
		}},
		Events: repo,
	}, refuse)

	_, _, err := svc.CreateEvent(context.Background(), &store.User{ID: 1}, 1, UpsertInput{RawICS: validICS("refused"), ContentType: "text/calendar"})
	if !errors.Is(err, ErrBadRequest) || !strings.Contains(err.Error(), "max-instances") {
		t.Fatalf("CreateEvent() error = %v, want the validator's max-instances refusal as ErrBadRequest", err)
	}
	if len(validated) != 1 || validated[0] != validICS("refused") {
		t.Fatalf("validator saw %q, want the submitted object once", validated)
	}
	if len(repo.events) != 0 {
		t.Fatalf("stored %d events after a refusal, want none", len(repo.events))
	}
}

func TestServiceCRUDAndValidation(t *testing.T) {
	user := &store.User{ID: 1}

	t.Run("list and get calendar", func(t *testing.T) {
		svc := NewService(&store.Store{
			Calendars: &fakeCalendarRepo{calendars: map[int64]*store.CalendarAccess{
				1: {Calendar: store.Calendar{ID: 1, UserID: 1, Name: "Work"}, Editor: true},
			}},
			Events: &fakeEventRepo{events: map[string]store.Event{}},
		}, dav.ValidateCalendarObject)

		cals, err := svc.ListCalendars(context.Background(), user)
		if err != nil || len(cals) != 1 {
			t.Fatalf("ListCalendars() err=%v len=%d", err, len(cals))
		}
		cal, err := svc.GetCalendar(context.Background(), user, 1)
		if err != nil || cal == nil {
			t.Fatalf("GetCalendar() err=%v cal=%v", err, cal)
		}
		events, err := svc.ListEvents(context.Background(), user, 1, store.EventFilter{})
		if err != nil || len(events) != 0 {
			t.Fatalf("ListEvents() err=%v len=%d", err, len(events))
		}
	})

	t.Run("get calendar not found", func(t *testing.T) {
		svc := NewService(&store.Store{Calendars: &fakeCalendarRepo{calendars: map[int64]*store.CalendarAccess{}}}, dav.ValidateCalendarObject)
		_, err := svc.GetCalendar(context.Background(), user, 1)
		if !errors.Is(err, ErrNotFound) {
			t.Fatalf("expected ErrNotFound, got %v", err)
		}
	})

	t.Run("create raw ics success", func(t *testing.T) {
		repo := &fakeEventRepo{events: map[string]store.Event{}}
		svc := newServiceWithRepos(true, repo)
		ev, created, err := svc.CreateEvent(context.Background(), user, 1, UpsertInput{
			RawICS:      validICS("raw-1"),
			ContentType: "text/calendar",
		})
		if err != nil || !created || ev == nil || ev.UID != "raw-1" {
			t.Fatalf("CreateEvent() err=%v created=%v ev=%+v", err, created, ev)
		}
		got, err := svc.GetEvent(context.Background(), user, 1, "raw-1")
		if err != nil || got == nil {
			t.Fatalf("GetEvent() err=%v got=%v", err, got)
		}
	})

	t.Run("create forbidden", func(t *testing.T) {
		svc := NewService(&store.Store{
			Calendars: &fakeCalendarRepo{calendars: map[int64]*store.CalendarAccess{
				1: {Calendar: store.Calendar{ID: 1, UserID: 9, Name: "Shared"}, Shared: true, Editor: false},
			}},
			Events: &fakeEventRepo{events: map[string]store.Event{}},
		}, dav.ValidateCalendarObject)

		_, _, err := svc.CreateEvent(context.Background(), user, 1, UpsertInput{
			Structured: &StructuredInput{Summary: "x", DTStart: "2026-03-20T10:00", DTEnd: "2026-03-20T11:00"},
		})
		if !errors.Is(err, ErrForbidden) {
			t.Fatalf("expected ErrForbidden, got %v", err)
		}
	})

	t.Run("create conflict existing uid", func(t *testing.T) {
		repo := &fakeEventRepo{events: map[string]store.Event{"1:dup": {CalendarID: 1, UID: "dup", ResourceName: "dup", ETag: "e1"}}}
		svc := newServiceWithRepos(true, repo)
		_, _, err := svc.CreateEvent(context.Background(), user, 1, UpsertInput{
			RawICS:      validICS("dup"),
			ContentType: "text/calendar",
		})
		if !errors.Is(err, ErrConflict) {
			t.Fatalf("expected ErrConflict, got %v", err)
		}
	})

	t.Run("create if none match fails", func(t *testing.T) {
		repo := &fakeEventRepo{events: map[string]store.Event{"1:dup": {CalendarID: 1, UID: "dup", ResourceName: "dup", ETag: "e1"}}}
		svc := newServiceWithRepos(true, repo)
		_, _, err := svc.CreateEvent(context.Background(), user, 1, UpsertInput{
			RawICS:      validICS("dup"),
			ContentType: "text/calendar",
			IfNoneMatch: "*",
		})
		if !errors.Is(err, ErrPreconditionFailed) {
			t.Fatalf("expected ErrPreconditionFailed, got %v", err)
		}
	})

	t.Run("update not found", func(t *testing.T) {
		svc := newServiceWithRepos(true, &fakeEventRepo{events: map[string]store.Event{}})
		_, _, err := svc.UpdateEvent(context.Background(), user, 1, "missing", UpsertInput{
			Structured: &StructuredInput{Summary: "x", DTStart: "2026-03-20T10:00", DTEnd: "2026-03-20T11:00"},
		})
		if !errors.Is(err, ErrNotFound) {
			t.Fatalf("expected ErrNotFound, got %v", err)
		}
	})

	t.Run("update raw ics success preserves resource", func(t *testing.T) {
		repo := &fakeEventRepo{events: map[string]store.Event{
			"1:uid-1": {CalendarID: 1, UID: "uid-1", ResourceName: "resource-a", ETag: "etag-old"},
		}}
		svc := newServiceWithRepos(true, repo)
		ev, _, err := svc.UpdateEvent(context.Background(), user, 1, "uid-1", UpsertInput{
			RawICS:      validICS("uid-1"),
			ContentType: "text/calendar",
			IfMatch:     `"etag-old"`,
		})
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if ev.ResourceName != "resource-a" {
			t.Fatalf("expected preserved resource name, got %s", ev.ResourceName)
		}
	})

	t.Run("update payload uid mismatch", func(t *testing.T) {
		repo := &fakeEventRepo{events: map[string]store.Event{
			"1:uid-1": {CalendarID: 1, UID: "uid-1", ResourceName: "uid-1", ETag: "etag-old"},
		}}
		svc := newServiceWithRepos(true, repo)
		_, _, err := svc.UpdateEvent(context.Background(), user, 1, "uid-1", UpsertInput{
			Structured: &StructuredInput{UID: "other", Summary: "x", DTStart: "2026-03-20T10:00", DTEnd: "2026-03-20T11:00"},
		})
		if !errors.Is(err, ErrBadRequest) {
			t.Fatalf("expected ErrBadRequest, got %v", err)
		}
	})

	t.Run("update resource conflict", func(t *testing.T) {
		repo := &fakeEventRepo{events: map[string]store.Event{
			"1:uid-1": {CalendarID: 1, UID: "uid-1", ResourceName: "resource-a", ETag: "etag-old"},
			"1:uid-2": {CalendarID: 1, UID: "uid-2", ResourceName: "resource-b", ETag: "etag-2"},
		}}
		svc := newServiceWithRepos(true, repo)
		_, _, err := svc.saveEvent(context.Background(), 1, "uid-1", "resource-b", mustCalendarObject(t, validICS("uid-1")), "", "")
		if !errors.Is(err, ErrConflict) {
			t.Fatalf("expected ErrConflict, got %v", err)
		}
	})

	t.Run("save event precondition failed", func(t *testing.T) {
		repo := &fakeEventRepo{events: map[string]store.Event{
			"1:uid-1": {CalendarID: 1, UID: "uid-1", ResourceName: "resource-a", ETag: "etag-old"},
		}}
		svc := newServiceWithRepos(true, repo)
		_, _, err := svc.saveEvent(context.Background(), 1, "uid-1", "resource-a", mustCalendarObject(t, validICS("uid-1")), `"wrong"`, "")
		if !errors.Is(err, ErrPreconditionFailed) {
			t.Fatalf("expected ErrPreconditionFailed, got %v", err)
		}
	})

	t.Run("delete success", func(t *testing.T) {
		repo := &fakeEventRepo{events: map[string]store.Event{
			"1:uid-1": {CalendarID: 1, UID: "uid-1", ResourceName: "uid-1", ETag: "etag-1"},
		}}
		dead := &fakeDeadPropertyRepo{properties: []store.DeadProperty{{ResourcePath: "/dav/calendars/1/uid-1", NamespaceURI: "urn:test", LocalName: "note"}}}
		svc := NewService(&store.Store{
			Calendars: &fakeCalendarRepo{calendars: map[int64]*store.CalendarAccess{1: {Calendar: store.Calendar{ID: 1, UserID: 1, Name: "Work"}, Editor: true}}},
			Events:    repo, DeadProperties: dead,
		}, dav.ValidateCalendarObject)

		err := svc.DeleteEvent(context.Background(), user, 1, "uid-1", `"etag-1"`, "")
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if len(dead.properties) != 0 {
			t.Fatalf("DeleteEvent retained dead properties: %#v", dead.properties)
		}
	})

	t.Run("delete precondition failed", func(t *testing.T) {
		repo := &fakeEventRepo{events: map[string]store.Event{
			"1:uid-1": {CalendarID: 1, UID: "uid-1", ResourceName: "uid-1", ETag: "etag-1"},
		}}
		svc := newServiceWithRepos(true, repo)
		err := svc.DeleteEvent(context.Background(), user, 1, "uid-1", `"wrong"`, "")
		if !errors.Is(err, ErrPreconditionFailed) {
			t.Fatalf("expected ErrPreconditionFailed, got %v", err)
		}
	})

	t.Run("delete forbidden and not found", func(t *testing.T) {
		svc := NewService(&store.Store{
			Calendars: &fakeCalendarRepo{calendars: map[int64]*store.CalendarAccess{
				1: {Calendar: store.Calendar{ID: 1, UserID: 9, Name: "Shared"}, Shared: true, Editor: false},
			}},
			Events: &fakeEventRepo{events: map[string]store.Event{
				"1:uid-1": {CalendarID: 1, UID: "uid-1", ResourceName: "uid-1", ETag: "etag-1"},
			}},
		}, dav.ValidateCalendarObject)

		if err := svc.DeleteEvent(context.Background(), user, 1, "uid-1", "", ""); !errors.Is(err, ErrForbidden) {
			t.Fatalf("expected ErrForbidden, got %v", err)
		}
		svc = newServiceWithRepos(true, &fakeEventRepo{events: map[string]store.Event{}})
		if err := svc.DeleteEvent(context.Background(), user, 1, "missing", "", ""); !errors.Is(err, ErrNotFound) {
			t.Fatalf("expected ErrNotFound, got %v", err)
		}
	})

	t.Run("normalize payload branches", func(t *testing.T) {
		svc := newServiceWithRepos(true, &fakeEventRepo{events: map[string]store.Event{}})
		if _, err := svc.normalizeEventPayload(UpsertInput{}, "", nil); !errors.Is(err, ErrBadRequest) {
			t.Fatalf("expected ErrBadRequest, got %v", err)
		}
		if _, err := svc.normalizeEventPayload(UpsertInput{RawICS: validICS("u1"), ContentType: ""}, "", nil); !errors.Is(err, ErrUnsupportedMediaType) {
			t.Fatalf("expected ErrUnsupportedMediaType, got %v", err)
		}
		if _, err := svc.normalizeEventPayload(UpsertInput{RawICS: validICS("u1"), ContentType: "text/calendar"}, "other", nil); !errors.Is(err, ErrBadRequest) {
			t.Fatalf("expected ErrBadRequest, got %v", err)
		}
	})
}

func TestServiceEnforcesCalendarObjectACLs(t *testing.T) {
	delegate := &store.User{ID: 2}
	summaryVisible := "Visible"
	summaryHidden := "Hidden"
	start := time.Date(2026, 3, 20, 10, 0, 0, 0, time.UTC)
	end := start.Add(time.Hour)

	svc := NewService(&store.Store{
		Calendars: &fakeCalendarRepo{calendars: map[int64]*store.CalendarAccess{
			1: {Calendar: store.Calendar{ID: 1, UserID: 1, Name: "Shared"}, Shared: true, Editor: true},
		}},
		Events: &fakeEventRepo{events: map[string]store.Event{
			"1:visible": {CalendarID: 1, UID: "visible", ResourceName: "visible", ETag: "etag-visible", Summary: &summaryVisible, DTStart: &start, DTEnd: &end},
			"1:hidden":  {CalendarID: 1, UID: "hidden", ResourceName: "hidden", ETag: "etag-hidden", Summary: &summaryHidden, DTStart: &start, DTEnd: &end},
		}},
		ACLEntries: &fakeACLRepo{entries: []store.ACLEntry{
			{ResourcePath: "/dav/calendars/1", PrincipalHref: "/dav/principals/2/", IsGrant: true, Privilege: "read"},
			{ResourcePath: "/dav/calendars/1/hidden", PrincipalHref: "/dav/principals/2/", IsGrant: false, Privilege: "read"},
		}},
	}, dav.ValidateCalendarObject)

	events, err := svc.ListEvents(context.Background(), delegate, 1, store.EventFilter{})
	if err != nil {
		t.Fatalf("ListEvents() error = %v", err)
	}
	if len(events) != 1 || events[0].UID != "visible" {
		t.Fatalf("ListEvents() = %#v, want only visible event", events)
	}

	got, err := svc.GetEvent(context.Background(), delegate, 1, "hidden")
	if !errors.Is(err, ErrNotFound) {
		t.Fatalf("GetEvent() error = %v, want ErrNotFound", err)
	}
	if got != nil {
		t.Fatalf("GetEvent() = %#v, want nil", got)
	}
}

func TestServiceAllowsDirectObjectAccessWithoutCollectionAccess(t *testing.T) {
	delegate := &store.User{ID: 2}
	repo := &fakeEventRepo{events: map[string]store.Event{
		"1:event-1": {CalendarID: 1, UID: "event-1", ResourceName: "event-1", RawICAL: validICS("event-1"), ETag: "etag-1"},
	}}
	svc := NewService(&store.Store{
		Calendars: &fakeCalendarRepo{calendars: map[int64]*store.CalendarAccess{
			1: {Calendar: store.Calendar{ID: 1, UserID: 1, Name: "Shared"}, Shared: true, Editor: false},
		}},
		Events: repo,
		ACLEntries: &fakeACLRepo{entries: []store.ACLEntry{
			{ResourcePath: "/dav/calendars/1/event-1", PrincipalHref: "/dav/principals/2/", IsGrant: true, Privilege: "read"},
			{ResourcePath: "/dav/calendars/1/event-1", PrincipalHref: "/dav/principals/2/", IsGrant: true, Privilege: "write-content"},
			{ResourcePath: "/dav/calendars/1/event-1", PrincipalHref: "/dav/principals/2/", IsGrant: true, Privilege: "unbind"},
		}},
	}, dav.ValidateCalendarObject)

	got, err := svc.GetEvent(context.Background(), delegate, 1, "event-1")
	if err != nil || got == nil || got.UID != "event-1" {
		t.Fatalf("GetEvent() err=%v got=%#v", err, got)
	}

	updated, _, err := svc.UpdateEvent(context.Background(), delegate, 1, "event-1", UpsertInput{
		RawICS:      validICS("event-1"),
		ContentType: "text/calendar",
		IfMatch:     `"etag-1"`,
	})
	if err != nil || updated == nil || updated.UID != "event-1" {
		t.Fatalf("UpdateEvent() err=%v updated=%#v", err, updated)
	}

	if err := svc.DeleteEvent(context.Background(), delegate, 1, "event-1", `"`+updated.ETag+`"`, ""); err != nil {
		t.Fatalf("DeleteEvent() error = %v", err)
	}
	if got, _ := repo.GetByUID(context.Background(), 1, "event-1"); got != nil {
		t.Fatalf("expected event deletion, got %#v", got)
	}
}

func TestServiceRequiresSpecificCalendarACLPrivileges(t *testing.T) {
	delegate := &store.User{ID: 2}

	t.Run("create requires bind", func(t *testing.T) {
		repo := &fakeEventRepo{events: map[string]store.Event{}}
		svc := NewService(&store.Store{
			Calendars: &fakeCalendarRepo{calendars: map[int64]*store.CalendarAccess{
				1: {Calendar: store.Calendar{ID: 1, UserID: 1, Name: "Shared"}, Shared: true, Editor: true},
			}},
			Events: repo,
			ACLEntries: &fakeACLRepo{entries: []store.ACLEntry{
				{ResourcePath: "/dav/calendars/1", PrincipalHref: "/dav/principals/2/", IsGrant: true, Privilege: "read"},
				{ResourcePath: "/dav/calendars/1", PrincipalHref: "/dav/principals/2/", IsGrant: true, Privilege: "write-content"},
			}},
		}, dav.ValidateCalendarObject)

		_, _, err := svc.CreateEvent(context.Background(), delegate, 1, UpsertInput{
			Structured: &StructuredInput{Summary: "Create", DTStart: "2026-03-20T10:00", DTEnd: "2026-03-20T11:00"},
		})
		if !errors.Is(err, ErrForbidden) {
			t.Fatalf("CreateEvent() error = %v, want ErrForbidden", err)
		}
		if len(repo.events) != 0 {
			t.Fatalf("expected no stored events, got %#v", repo.events)
		}
	})

	t.Run("update requires write-content", func(t *testing.T) {
		repo := &fakeEventRepo{events: map[string]store.Event{
			"1:event-1": {CalendarID: 1, UID: "event-1", ResourceName: "event-1", ETag: "etag-1"},
		}}
		svc := NewService(&store.Store{
			Calendars: &fakeCalendarRepo{calendars: map[int64]*store.CalendarAccess{
				1: {Calendar: store.Calendar{ID: 1, UserID: 1, Name: "Shared"}, Shared: true, Editor: true},
			}},
			Events: repo,
			ACLEntries: &fakeACLRepo{entries: []store.ACLEntry{
				{ResourcePath: "/dav/calendars/1", PrincipalHref: "/dav/principals/2/", IsGrant: true, Privilege: "read"},
				{ResourcePath: "/dav/calendars/1", PrincipalHref: "/dav/principals/2/", IsGrant: true, Privilege: "bind"},
			}},
		}, dav.ValidateCalendarObject)

		_, _, err := svc.UpdateEvent(context.Background(), delegate, 1, "event-1", UpsertInput{
			RawICS:      validICS("event-1"),
			ContentType: "text/calendar",
			IfMatch:     `"etag-1"`,
		})
		if !errors.Is(err, ErrForbidden) {
			t.Fatalf("UpdateEvent() error = %v, want ErrForbidden", err)
		}
	})

	t.Run("update honors object deny", func(t *testing.T) {
		repo := &fakeEventRepo{events: map[string]store.Event{
			"1:event-1": {CalendarID: 1, UID: "event-1", ResourceName: "event-1.ics", ETag: "etag-1"},
		}}
		svc := NewService(&store.Store{
			Calendars: &fakeCalendarRepo{calendars: map[int64]*store.CalendarAccess{
				1: {Calendar: store.Calendar{ID: 1, UserID: 1, Name: "Shared"}, Shared: true, Editor: true},
			}},
			Events: repo,
			ACLEntries: &fakeACLRepo{entries: []store.ACLEntry{
				{ResourcePath: "/dav/calendars/1", PrincipalHref: "/dav/principals/2/", IsGrant: true, Privilege: "read"},
				{ResourcePath: "/dav/calendars/1", PrincipalHref: "/dav/principals/2/", IsGrant: true, Privilege: "write-content"},
				{ResourcePath: "/dav/calendars/1/event-1", PrincipalHref: "/dav/principals/2/", IsGrant: false, Privilege: "write-content"},
			}},
		}, dav.ValidateCalendarObject)

		_, _, err := svc.UpdateEvent(context.Background(), delegate, 1, "event-1", UpsertInput{
			RawICS:      validICS("event-1"),
			ContentType: "text/calendar",
			IfMatch:     `"etag-1"`,
		})
		if !errors.Is(err, ErrForbidden) {
			t.Fatalf("UpdateEvent() error = %v, want ErrForbidden", err)
		}
	})

	t.Run("delete requires unbind", func(t *testing.T) {
		repo := &fakeEventRepo{events: map[string]store.Event{
			"1:event-1": {CalendarID: 1, UID: "event-1", ResourceName: "event-1", ETag: "etag-1"},
		}}
		svc := NewService(&store.Store{
			Calendars: &fakeCalendarRepo{calendars: map[int64]*store.CalendarAccess{
				1: {Calendar: store.Calendar{ID: 1, UserID: 1, Name: "Shared"}, Shared: true, Editor: true},
			}},
			Events: repo,
			ACLEntries: &fakeACLRepo{entries: []store.ACLEntry{
				{ResourcePath: "/dav/calendars/1", PrincipalHref: "/dav/principals/2/", IsGrant: true, Privilege: "read"},
				{ResourcePath: "/dav/calendars/1", PrincipalHref: "/dav/principals/2/", IsGrant: true, Privilege: "write-content"},
			}},
		}, dav.ValidateCalendarObject)

		err := svc.DeleteEvent(context.Background(), delegate, 1, "event-1", `"etag-1"`, "")
		if !errors.Is(err, ErrForbidden) {
			t.Fatalf("DeleteEvent() error = %v, want ErrForbidden", err)
		}
	})
}

func TestServiceListEventsBatchesACLLookups(t *testing.T) {
	delegate := &store.User{ID: 2}
	aclRepo := &fakeACLRepo{
		entries: []store.ACLEntry{
			{ResourcePath: "/dav/calendars/1", PrincipalHref: "/dav/principals/2/", IsGrant: true, Privilege: "read"},
			{ResourcePath: "/dav/calendars/1/hidden-2", PrincipalHref: "/dav/principals/2/", IsGrant: false, Privilege: "read"},
		},
	}
	svc := NewService(&store.Store{
		Calendars: &fakeCalendarRepo{calendars: map[int64]*store.CalendarAccess{
			1: {Calendar: store.Calendar{ID: 1, UserID: 1, Name: "Shared"}, Shared: true, PrivilegesResolved: true, Privileges: store.CalendarPrivileges{Read: true}},
		}},
		Events: &fakeEventRepo{events: map[string]store.Event{
			"1:visible-1": {CalendarID: 1, UID: "visible-1", ResourceName: "visible-1"},
			"1:hidden-2":  {CalendarID: 1, UID: "hidden-2", ResourceName: "hidden-2"},
			"1:visible-3": {CalendarID: 1, UID: "visible-3", ResourceName: "visible-3"},
		}},
		ACLEntries: aclRepo,
	}, dav.ValidateCalendarObject)

	events, err := svc.ListEvents(context.Background(), delegate, 1, store.EventFilter{})
	if err != nil {
		t.Fatalf("ListEvents() error = %v", err)
	}
	if len(events) != 2 {
		t.Fatalf("ListEvents() len = %d, want 2 visible events", len(events))
	}
	if aclRepo.listByResourceCalls != 0 {
		t.Fatalf("expected batched ACL lookup without per-resource calls, got %d ListByResource calls", aclRepo.listByResourceCalls)
	}
	if aclRepo.listByPrincipalCalls == 0 || aclRepo.listByPrincipalCalls > 3 {
		t.Fatalf("expected batched ListByPrincipal calls, got %d", aclRepo.listByPrincipalCalls)
	}
}

func TestServiceListEventsHonorsACEOrderAcrossPrincipalQueries(t *testing.T) {
	delegate := &store.User{ID: 2}
	tests := []struct {
		name      string
		entries   []store.ACLEntry
		wantCount int
	}{
		{
			name: "specific deny precedes broad grant",
			entries: []store.ACLEntry{
				{ID: 2, ResourcePath: "/dav/calendars/1/event-1", PrincipalHref: "DAV:all", IsGrant: true, Privilege: "read", Position: 1},
				{ID: 1, ResourcePath: "/dav/calendars/1/event-1", PrincipalHref: "/dav/principals/2/", IsGrant: false, Privilege: "read", Position: 0},
			},
			wantCount: 0,
		},
		{
			name: "broad grant precedes specific deny",
			entries: []store.ACLEntry{
				{ID: 1, ResourcePath: "/dav/calendars/1/event-1", PrincipalHref: "DAV:all", IsGrant: true, Privilege: "read", Position: 0},
				{ID: 2, ResourcePath: "/dav/calendars/1/event-1", PrincipalHref: "/dav/principals/2/", IsGrant: false, Privilege: "read", Position: 1},
			},
			wantCount: 1,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			svc := NewService(&store.Store{
				Calendars: &fakeCalendarRepo{calendars: map[int64]*store.CalendarAccess{
					1: {Calendar: store.Calendar{ID: 1, UserID: 1, Name: "Shared"}, Shared: true, PrivilegesResolved: true, Privileges: store.CalendarPrivileges{Read: true}},
				}},
				Events: &fakeEventRepo{events: map[string]store.Event{
					"1:event-1": {CalendarID: 1, UID: "event-1", ResourceName: "event-1"},
				}},
				ACLEntries: &fakeACLRepo{entries: tc.entries},
			}, dav.ValidateCalendarObject)

			events, err := svc.ListEvents(context.Background(), delegate, 1, store.EventFilter{})
			if err != nil {
				t.Fatalf("ListEvents() error = %v", err)
			}
			if len(events) != tc.wantCount {
				t.Fatalf("ListEvents() len = %d, want %d", len(events), tc.wantCount)
			}
		})
	}
}

func TestCalendarPrivilegeDecisionHonorsACEOrderAcrossResourceAliases(t *testing.T) {
	user := &store.User{ID: 2}
	cal := &store.CalendarAccess{Calendar: store.Calendar{ID: 1, UserID: 1}}
	entries := map[string][]store.ACLEntry{
		"/dav/calendars/1/event": {
			{ID: 2, ResourcePath: "/dav/calendars/1/event", PrincipalHref: "DAV:all", IsGrant: true, Privilege: "read", Position: 1},
		},
		"/dav/calendars/1/event.ics": {
			{ID: 1, ResourcePath: "/dav/calendars/1/event.ics", PrincipalHref: "/dav/principals/2/", IsGrant: false, Privilege: "read", Position: 0},
		},
	}

	granted, denied := calendarPrivilegeDecisionFromEntries(user, cal, "event", "read", entries)
	if granted || !denied {
		t.Fatalf("calendarPrivilegeDecisionFromEntries() = (%v, %v), want denied", granted, denied)
	}
}

func TestServiceListEventsPaginationOnlyPreservesLastModifiedOrder(t *testing.T) {
	owner := &store.User{ID: 1}

	t1 := time.Date(2026, 6, 1, 10, 0, 0, 0, time.UTC) // oldest modification
	t2 := time.Date(2026, 6, 2, 10, 0, 0, 0, time.UTC)
	t3 := time.Date(2026, 6, 3, 10, 0, 0, 0, time.UTC) // newest modification
	janStart := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	febStart := time.Date(2026, 2, 1, 0, 0, 0, 0, time.UTC)
	marStart := time.Date(2026, 3, 1, 0, 0, 0, 0, time.UTC)

	svc := NewService(&store.Store{
		Calendars: &fakeCalendarRepo{calendars: map[int64]*store.CalendarAccess{
			1: {Calendar: store.Calendar{ID: 1, UserID: 1, Name: "Mine"}},
		}},
		Events: &fakeEventRepo{events: map[string]store.Event{
			"1:early": {CalendarID: 1, UID: "early", ResourceName: "early", DTStart: &janStart, LastModified: t1},
			"1:mid":   {CalendarID: 1, UID: "mid", ResourceName: "mid", DTStart: &febStart, LastModified: t3},
			"1:late":  {CalendarID: 1, UID: "late", ResourceName: "late", DTStart: &marStart, LastModified: t2},
		}},
	}, dav.ValidateCalendarObject)

	events, err := svc.ListEvents(context.Background(), owner, 1, store.EventFilter{Limit: 2})
	if err != nil {
		t.Fatalf("ListEvents() error = %v", err)
	}

	gotUIDs := make([]string, len(events))
	for i, ev := range events {
		gotUIDs[i] = ev.UID
	}
	// A pagination-only filter must preserve ListForCalendar's last_modified DESC
	// ordering. If it regressed to ListForCalendarFiltered (dtstart ASC), the
	// first page would start with "early".
	if got, want := strings.Join(gotUIDs, ","), "mid,late"; got != want {
		t.Fatalf("ListEvents(limit=2) order = %q, want %q (last_modified DESC, not dtstart)", got, want)
	}
}

func TestHelpersAndValidators(t *testing.T) {
	t.Run("build structured event branches", func(t *testing.T) {
		if _, _, err := buildStructuredEvent(&StructuredInput{}, ""); !errors.Is(err, ErrBadRequest) {
			t.Fatalf("expected ErrBadRequest, got %v", err)
		}
		if _, _, err := buildStructuredEvent(&StructuredInput{Summary: "x"}, ""); !errors.Is(err, ErrBadRequest) {
			t.Fatalf("expected ErrBadRequest, got %v", err)
		}
		if _, _, err := buildStructuredEvent(&StructuredInput{Summary: "x", DTStart: "bad", DTEnd: "2026-03-20"}, ""); !errors.Is(err, ErrBadRequest) {
			t.Fatalf("expected ErrBadRequest, got %v", err)
		}
		if _, _, err := buildStructuredEvent(&StructuredInput{UID: "wrong", Summary: "x", DTStart: "2026-03-20T10:00", DTEnd: "2026-03-20T11:00"}, "right"); !errors.Is(err, ErrBadRequest) {
			t.Fatalf("expected ErrBadRequest, got %v", err)
		}
		body, uid, err := buildStructuredEvent(&StructuredInput{
			Summary: "x", DTStart: "2026-03-20T10:00", DTEnd: "2026-03-20T11:00",
			Recurrence: &StructuredRecurrence{Frequency: "DAILY", Count: 2},
		}, "")
		if err != nil || uid == "" || !strings.Contains(body, "RRULE:FREQ=DAILY;COUNT=2") {
			t.Fatalf("unexpected result err=%v uid=%q body=%s", err, uid, body)
		}
	})

	t.Run("structured times are read as the calendar object stores them", func(t *testing.T) {
		if _, err := time.LoadLocation("America/New_York"); err != nil {
			t.Skipf("tzdata unavailable: %v", err)
		}
		for name, input := range map[string]*StructuredInput{
			// 02:30 falls in the 10 March gap and names 03:30 EDT (RFC 5545
			// §3.3.5), after the 03:15 EDT end.
			"end before a skipped start": {Summary: "x", Timezone: "America/New_York", DTStart: "2024-03-10T02:30", DTEnd: "2024-03-10T03:15"},
			// The builder writes local date-times only.
			"start with a UTC designator": {Summary: "x", DTStart: "2026-03-20T10:00:00Z", DTEnd: "2026-03-20T11:00"},
			"end with an offset":          {Summary: "x", DTStart: "2026-03-20T10:00", DTEnd: "2026-03-20T11:00:00-05:00"},
		} {
			if _, _, err := buildStructuredEvent(input, ""); !errors.Is(err, ErrBadRequest) {
				t.Errorf("%s: buildStructuredEvent() error = %v, want ErrBadRequest", name, err)
			}
		}
		body, _, err := buildStructuredEvent(&StructuredInput{Summary: "x", Timezone: "America/New_York", DTStart: "2024-03-10T02:30", DTEnd: "2024-03-10T03:30"}, "")
		if err != nil || !strings.Contains(body, "DTSTART;TZID=America/New_York:20240310T023000\r\n") {
			t.Fatalf("buildStructuredEvent() = %v\n%s, want the entered start", err, body)
		}
	})

	t.Run("content type validation", func(t *testing.T) {
		if !errors.Is(validateCalendarContentType(""), ErrUnsupportedMediaType) {
			t.Fatal("expected unsupported media type")
		}
		if !errors.Is(validateCalendarContentType("application/json"), ErrUnsupportedMediaType) {
			t.Fatal("expected unsupported media type")
		}
		if err := validateCalendarContentType("text/calendar; charset=utf-8"); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
	})

	t.Run("conditional headers", func(t *testing.T) {
		ev := &store.Event{ETag: "current"}
		if !checkConditionalHeaders("", "", nil) {
			t.Fatal("expected true")
		}
		if !checkConditionalHeaders("", "*", nil) {
			t.Fatal("expected true")
		}
		if checkConditionalHeaders("", "*", ev) {
			t.Fatal("expected false")
		}
		if !checkConditionalHeaders(`"current"`, "", ev) {
			t.Fatal("expected true")
		}
		if checkConditionalHeaders(`"wrong"`, "", ev) {
			t.Fatal("expected false")
		}
		if !checkConditionalHeaders("", `"wrong"`, ev) {
			t.Fatal("expected true")
		}
		if checkConditionalHeaders("", `"current"`, ev) {
			t.Fatal("expected false")
		}
	})

	t.Run("misc helpers", func(t *testing.T) {
		if err := utils.ValidateEventDates("2026-03-20T10:00", "2026-03-20T10:00", false, utils.EventZone{}); err != nil {
			t.Fatalf("a zero-length event was refused: %v", err)
		}
		if err := utils.ValidateEventDates("2026-03-20T10:00", "2026-03-20T09:00", false, utils.EventZone{}); err == nil {
			t.Fatal("expected an end before the start to be refused")
		}
		if got := ensureCRLF("A\nB"); got != "A\r\nB\r\n" {
			t.Fatalf("unexpected CRLF result %q", got)
		}
		minDate, maxDate := ical.DateLimits()
		if !minDate.Before(maxDate) {
			t.Fatal("expected valid date limits")
		}
		if StatusCode(nil) != http.StatusOK ||
			StatusCode(ErrNotFound) != http.StatusNotFound ||
			StatusCode(ErrForbidden) != http.StatusForbidden ||
			StatusCode(ErrConflict) != http.StatusConflict ||
			StatusCode(ErrPreconditionFailed) != http.StatusPreconditionFailed ||
			StatusCode(ErrUnsupportedMediaType) != http.StatusUnsupportedMediaType ||
			StatusCode(ErrBadRequest) != http.StatusBadRequest ||
			StatusCode(errors.New("x")) != http.StatusInternalServerError {
			t.Fatal("unexpected status code mapping")
		}
	})
}

func newServiceWithRepos(editor bool, repo *fakeEventRepo) *Service {
	return NewService(&store.Store{
		Calendars: &fakeCalendarRepo{calendars: map[int64]*store.CalendarAccess{
			1: {Calendar: store.Calendar{ID: 1, UserID: 1, Name: "Work"}, Editor: editor},
		}},
		Events: repo,
	}, dav.ValidateCalendarObject)

}

func validICS(uid string) string {
	return "BEGIN:VCALENDAR\r\nVERSION:2.0\r\nPRODID:-//Test//EN\r\nBEGIN:VEVENT\r\nUID:" + uid + "\r\nDTSTAMP:20260101T000000Z\r\nSUMMARY:Planning\r\nDTSTART:20260320T100000Z\r\nDTEND:20260320T110000Z\r\nEND:VEVENT\r\nEND:VCALENDAR\r\n"
}

func methodICS(uid string) string {
	return "BEGIN:VCALENDAR\r\nVERSION:2.0\r\nMETHOD:REQUEST\r\nBEGIN:VEVENT\r\nUID:" + uid + "\r\nSUMMARY:Planning\r\nDTSTART:20260320T100000Z\r\nDTEND:20260320T110000Z\r\nEND:VEVENT\r\nEND:VCALENDAR\r\n"
}

type fakeCalendarRepo struct {
	calendars map[int64]*store.CalendarAccess
}

type fakeDeadPropertyRepo struct {
	properties []store.DeadProperty
}

func (f *fakeDeadPropertyRepo) ListByResources(_ context.Context, paths []string) ([]store.DeadProperty, error) {
	wanted := make(map[string]struct{}, len(paths))
	for _, resourcePath := range paths {
		wanted[resourcePath] = struct{}{}
	}
	var result []store.DeadProperty
	for _, property := range f.properties {
		if _, ok := wanted[property.ResourcePath]; ok {
			result = append(result, property)
		}
	}
	return result, nil
}

func (f *fakeDeadPropertyRepo) Apply(_ context.Context, resourcePath string, mutations []store.DeadPropertyMutation) error {
	remove := make(map[string]struct{}, len(mutations))
	for _, mutation := range mutations {
		if mutation.Remove {
			remove[mutation.NamespaceURI+"\x00"+mutation.LocalName] = struct{}{}
		}
	}
	kept := f.properties[:0]
	for _, property := range f.properties {
		if property.ResourcePath == resourcePath {
			if _, ok := remove[property.NamespaceURI+"\x00"+property.LocalName]; ok {
				continue
			}
		}
		kept = append(kept, property)
	}
	f.properties = kept
	return nil
}

func (f *fakeCalendarRepo) GetByID(ctx context.Context, id int64) (*store.Calendar, error) {
	if cal, ok := f.calendars[id]; ok {
		copy := cal.Calendar
		return &copy, nil
	}
	return nil, nil
}
func (f *fakeCalendarRepo) ListByUser(ctx context.Context, userID int64) ([]store.Calendar, error) {
	return nil, nil
}
func (f *fakeCalendarRepo) ListAccessible(ctx context.Context, userID int64) ([]store.CalendarAccess, error) {
	var out []store.CalendarAccess
	for _, cal := range f.calendars {
		if cal.UserID == userID || cal.Shared {
			out = append(out, *cal)
		}
	}
	return out, nil
}
func (f *fakeCalendarRepo) GetAccessible(ctx context.Context, calendarID, userID int64) (*store.CalendarAccess, error) {
	if cal, ok := f.calendars[calendarID]; ok && (cal.UserID == userID || cal.Shared) {
		copy := *cal
		return &copy, nil
	}
	return nil, nil
}
func (f *fakeCalendarRepo) Create(ctx context.Context, cal store.Calendar) (*store.Calendar, error) {
	return nil, nil
}
func (f *fakeCalendarRepo) Update(ctx context.Context, userID, id int64, name string, description, timezone, color *string) error {
	return nil
}
func (f *fakeCalendarRepo) UpdateProperties(ctx context.Context, id int64, props store.CalendarProperties) error {
	return nil
}
func (f *fakeCalendarRepo) Rename(ctx context.Context, userID, id int64, name string) error {
	return nil
}
func (f *fakeCalendarRepo) Delete(ctx context.Context, userID, id int64) error { return nil }

type fakeEventRepo struct {
	events map[string]store.Event
}

func (f *fakeEventRepo) Upsert(ctx context.Context, event store.Event) (*store.Event, error) {
	event.LastModified = time.Now().UTC()
	if event.ResourceName == "" {
		event.ResourceName = event.UID
	}
	f.events[key(event.CalendarID, event.UID)] = event
	copy := event
	return &copy, nil
}
func (f *fakeEventRepo) DeleteByUID(ctx context.Context, calendarID int64, uid string) error {
	delete(f.events, key(calendarID, uid))
	return nil
}
func (f *fakeEventRepo) GetByUID(ctx context.Context, calendarID int64, uid string) (*store.Event, error) {
	ev, ok := f.events[key(calendarID, uid)]
	if !ok {
		return nil, nil
	}
	copy := ev
	return &copy, nil
}
func (f *fakeEventRepo) GetByResourceName(ctx context.Context, calendarID int64, resourceName string) (*store.Event, error) {
	for _, ev := range f.events {
		if ev.CalendarID == calendarID && ev.ResourceName == resourceName {
			copy := ev
			return &copy, nil
		}
	}
	return nil, nil
}
func (f *fakeEventRepo) ListByResourceNames(ctx context.Context, calendarID int64, resourceNames []string) ([]store.Event, error) {
	var out []store.Event
	for _, name := range resourceNames {
		ev, err := f.GetByResourceName(ctx, calendarID, name)
		if err != nil {
			return nil, err
		}
		if ev != nil {
			out = append(out, *ev)
		}
	}
	return out, nil
}
func (f *fakeEventRepo) ListForCalendar(ctx context.Context, calendarID int64) ([]store.Event, error) {
	var out []store.Event
	for _, ev := range f.events {
		if ev.CalendarID == calendarID {
			out = append(out, ev)
		}
	}
	// Mirror the store's "ORDER BY last_modified DESC" so tests can rely on it.
	sort.SliceStable(out, func(i, j int) bool {
		return out[i].LastModified.After(out[j].LastModified)
	})
	return out, nil
}
func (f *fakeEventRepo) ListForCalendarFiltered(ctx context.Context, calendarID int64, filter store.EventFilter) ([]store.Event, error) {
	out, err := f.ListForCalendar(ctx, calendarID)
	if err != nil {
		return nil, err
	}
	// Mirror the store's "ORDER BY dtstart ASC NULLS LAST, last_modified DESC".
	sort.SliceStable(out, func(i, j int) bool {
		di, dj := out[i].DTStart, out[j].DTStart
		switch {
		case di == nil && dj == nil:
			return out[i].LastModified.After(out[j].LastModified)
		case di == nil:
			return false // NULLS LAST
		case dj == nil:
			return true
		case di.Equal(*dj):
			return out[i].LastModified.After(out[j].LastModified)
		default:
			return di.Before(*dj)
		}
	})
	// Model the LIMIT/OFFSET pushdown the real store applies in SQL so that
	// callers relying on store-side pagination behave the same here.
	if filter.Offset > 0 {
		if filter.Offset >= len(out) {
			return []store.Event{}, nil
		}
		out = out[filter.Offset:]
	}
	if filter.Limit > 0 && filter.Limit < len(out) {
		out = out[:filter.Limit]
	}
	return out, nil
}

func (f *fakeEventRepo) ListForCalendarPageAfter(ctx context.Context, calendarID, afterID int64, limit int, filter store.EventFilter) ([]store.Event, error) {
	events, err := f.ListForCalendarFiltered(ctx, calendarID, filter)
	if err != nil {
		return nil, err
	}
	var result []store.Event
	for _, event := range events {
		if event.ID > afterID {
			result = append(result, event)
		}
	}
	if limit >= 0 && len(result) > limit {
		result = result[:limit]
	}
	return result, nil
}
func (f *fakeEventRepo) ListForCalendarPaginated(ctx context.Context, calendarID int64, limit, offset int) (*store.PaginatedResult[store.Event], error) {
	// Mirror the store: "ORDER BY last_modified DESC LIMIT/OFFSET".
	all, err := f.ListForCalendar(ctx, calendarID)
	if err != nil {
		return nil, err
	}
	total := len(all)
	items := all
	if offset > 0 {
		if offset >= len(items) {
			items = nil
		} else {
			items = items[offset:]
		}
	}
	if limit > 0 && limit < len(items) {
		items = items[:limit]
	}
	return &store.PaginatedResult[store.Event]{
		Items:      items,
		TotalCount: total,
		Limit:      limit,
		Offset:     offset,
	}, nil
}
func (f *fakeEventRepo) ListByUIDs(ctx context.Context, calendarID int64, uids []string) ([]store.Event, error) {
	return nil, nil
}
func (f *fakeEventRepo) ListModifiedSincePageAfter(ctx context.Context, calendarID, afterID int64, since time.Time, limit int) ([]store.Event, error) {
	return nil, nil
}
func (f *fakeEventRepo) ListRecentByUser(ctx context.Context, userID int64, limit int) ([]store.Event, error) {
	return nil, nil
}
func (f *fakeEventRepo) MaxLastModified(ctx context.Context, calendarID int64) (time.Time, error) {
	return time.Time{}, nil
}
func (f *fakeEventRepo) MoveToCalendar(ctx context.Context, fromCalendarID, toCalendarID int64, uid, destResourceName string) error {
	return nil
}
func (f *fakeEventRepo) CopyToCalendar(ctx context.Context, fromCalendarID, toCalendarID int64, uid, destResourceName, newETag string) (*store.Event, error) {
	return nil, nil
}

type fakeACLRepo struct {
	entries              []store.ACLEntry
	listByResourceCalls  int
	listByPrincipalCalls int
}

func (f *fakeACLRepo) SetACL(ctx context.Context, resourcePath string, entries []store.ACLEntry) error {
	return nil
}

func (f *fakeACLRepo) ListByResource(ctx context.Context, resourcePath string) ([]store.ACLEntry, error) {
	f.listByResourceCalls++
	var result []store.ACLEntry
	for _, entry := range f.entries {
		if entry.ResourcePath == resourcePath {
			result = append(result, entry)
		}
	}
	return result, nil
}

func (f *fakeACLRepo) UpdateACL(ctx context.Context, resourcePath string, mutate func([]store.ACLEntry) ([]store.ACLEntry, error)) error {
	current, err := f.ListByResource(ctx, resourcePath)
	if err != nil {
		return err
	}
	next, err := mutate(current)
	if err != nil {
		return err
	}
	return f.SetACL(ctx, resourcePath, next)
}

// RevokePrincipalGrants mirrors the repository contract: take back the named
// collection privileges plus every grant beneath the collection, keeping denies.
func (f *fakeACLRepo) RevokePrincipalGrants(_ context.Context, collectionPath, principalHref string, collectionPrivileges []string) error {
	kept := f.entries[:0:0]
	for _, entry := range f.entries {
		if !entry.IsGrant || acl.NormalizePrincipalHref(entry.PrincipalHref) != acl.NormalizePrincipalHref(principalHref) {
			kept = append(kept, entry)
			continue
		}
		if entry.ResourcePath == collectionPath && slices.Contains(collectionPrivileges, entry.Privilege) {
			continue
		}
		if strings.HasPrefix(entry.ResourcePath, collectionPath+"/") {
			continue
		}
		kept = append(kept, entry)
	}
	f.entries = kept
	return nil
}

func (f *fakeACLRepo) ListByPrincipal(ctx context.Context, principalHref string) ([]store.ACLEntry, error) {
	f.listByPrincipalCalls++
	var result []store.ACLEntry
	for _, entry := range f.entries {
		if entry.PrincipalHref == principalHref {
			result = append(result, entry)
		}
	}
	return result, nil
}

func (f *fakeACLRepo) HasPrivilege(ctx context.Context, resourcePath, principalHref, privilege string) (bool, error) {
	return false, nil
}

func (f *fakeACLRepo) MoveResourcePath(ctx context.Context, fromPath, toPath string) error {
	return nil
}

func (f *fakeACLRepo) Delete(ctx context.Context, resourcePath string) error {
	return nil
}

func key(calendarID int64, uid string) string {
	return strconv.FormatInt(calendarID, 10) + ":" + uid
}

// The API stores what a DAV PUT would and refuses what it would, answering a
// refusal with the API's own errors: a calendar object resource RFC 4791 §4.1
// rules out is a conflict, and anything else -- a TZID defined nowhere
// included -- is a bad request. The limits the CalDAV collection properties
// advertise are among them, so a payload the DAV handlers refuse cannot be
// stored here and then served over CalDAV.
func TestEventWritesApplyTheCalDAVValidator(t *testing.T) {
	user := &store.User{ID: 1}
	event := func(lines ...string) string {
		return "BEGIN:VCALENDAR\r\nVERSION:2.0\r\nBEGIN:VEVENT\r\nUID:x\r\n" + strings.Join(lines, "\r\n") + "\r\nEND:VEVENT\r\nEND:VCALENDAR\r\n"
	}
	vtimezone := "BEGIN:VTIMEZONE\r\nTZID:Office\r\nBEGIN:STANDARD\r\nDTSTART:19700101T000000\r\nTZOFFSETFROM:+0000\r\nTZOFFSETTO:+0000\r\nEND:STANDARD\r\nEND:VTIMEZONE\r\n"
	tests := map[string]struct {
		ics string
		// zone is an IANA zone the case needs the host's time zone database for.
		zone string
		want error
		// message is text the error returned to the API client must carry.
		message string
	}{
		"not iCalendar":                   {ics: "bad", want: ErrBadRequest},
		"unsupported component":           {ics: "BEGIN:VCALENDAR\r\nVERSION:2.0\r\nBEGIN:VAVAILABILITY\r\nUID:x\r\nEND:VAVAILABILITY\r\nEND:VCALENDAR\r\n", want: ErrBadRequest},
		"METHOD":                          {ics: methodICS("x"), want: ErrConflict, message: "valid-calendar-object-resource"},
		"two UIDs":                        {ics: "BEGIN:VCALENDAR\r\nVERSION:2.0\r\nBEGIN:VEVENT\r\nUID:a\r\nDTSTART:20260320T100000Z\r\nEND:VEVENT\r\nBEGIN:VEVENT\r\nUID:b\r\nDTSTART:20260321T100000Z\r\nEND:VEVENT\r\nEND:VCALENDAR\r\n", want: ErrConflict, message: "valid-calendar-object-resource"},
		"missing UID":                     {ics: "BEGIN:VCALENDAR\r\nVERSION:2.0\r\nBEGIN:VEVENT\r\nDTSTART:20260320T100000Z\r\nEND:VEVENT\r\nEND:VCALENDAR\r\n", want: ErrBadRequest},
		"zone defined twice":              {ics: "BEGIN:VCALENDAR\r\nVERSION:2.0\r\n" + vtimezone + vtimezone + "BEGIN:VEVENT\r\nUID:x\r\nDTSTART;TZID=Office:20260320T100000\r\nEND:VEVENT\r\nEND:VCALENDAR\r\n", want: ErrBadRequest},
		"zone defined nowhere":            {ics: event("DTSTART;TZID=Nowhere/Zone:20260320T100000"), want: ErrBadRequest, message: "time zone it does not define"},
		"end before start":                {ics: event("DTSTART:20260320T100000Z", "DTEND:20260320T090000Z"), want: ErrBadRequest},
		"past max-date-time":              {ics: event("DTSTART:22010101T100000Z", "DTEND:22010101T110000Z"), want: ErrBadRequest},
		"past max-instances":              {ics: event("DTSTART:20240601T100000Z", fmt.Sprintf("RRULE:FREQ=DAILY;COUNT=%d", ical.MaxRecurrenceInstances+1)), want: ErrBadRequest},
		"past max-attendees-per-instance": {ics: event("DTSTART:20240601T100000Z", strings.TrimSuffix(strings.Repeat("ATTENDEE:mailto:a@example.com\r\n", ical.MaxAttendeesPerInstance+1), "\r\n")), want: ErrBadRequest},
		"zero-length event":               {ics: event("DTSTART:20260320T100000Z", "DTEND:20260320T100000Z")},
		"zone defined by the object":      {ics: "BEGIN:VCALENDAR\r\nVERSION:2.0\r\n" + vtimezone + "BEGIN:VEVENT\r\nUID:x\r\nDTSTART;TZID=Office:20260320T100000\r\nEND:VEVENT\r\nEND:VCALENDAR\r\n"},
		"IANA zone without a VTIMEZONE":   {ics: event("DTSTART;TZID=Europe/Berlin:20260320T100000"), zone: "Europe/Berlin"},
	}
	for name, tt := range tests {
		t.Run(name, func(t *testing.T) {
			if tt.zone != "" {
				if _, err := time.LoadLocation(tt.zone); err != nil {
					t.Skipf("tzdata unavailable: %v", err)
				}
			}
			repo := &fakeEventRepo{events: map[string]store.Event{}}
			svc := newServiceWithRepos(true, repo)
			_, _, err := svc.CreateEvent(context.Background(), user, 1, UpsertInput{RawICS: tt.ics, ContentType: "text/calendar"})
			if tt.want == nil && err != nil {
				t.Fatalf("CreateEvent() error = %v, want it stored", err)
			}
			for _, sentinel := range []error{ErrBadRequest, ErrConflict} {
				if errors.Is(err, sentinel) != (sentinel == tt.want) {
					t.Fatalf("CreateEvent() error = %v, want %v", err, tt.want)
				}
			}
			if tt.message != "" && !strings.Contains(err.Error(), tt.message) {
				t.Errorf("CreateEvent() error = %q, want it to say %q", err, tt.message)
			}
			if stored := len(repo.events) == 1; stored != (tt.want == nil) {
				t.Fatalf("stored = %v", stored)
			}
		})
	}
}

// What the API stores is what the validator hands back -- the object less a
// byte order mark -- indexed by the metadata the validator derived, and a
// collection's supported components bound it as they bound a PUT.
func TestEventWritesStoreWhatTheValidatorAccepted(t *testing.T) {
	user := &store.User{ID: 1}
	repo := &fakeEventRepo{events: map[string]store.Event{}}
	svc := newServiceWithRepos(true, repo)
	if _, _, err := svc.CreateEvent(context.Background(), user, 1, UpsertInput{RawICS: "\xef\xbb\xbf" + validICS("bom"), ContentType: "text/calendar"}); err != nil {
		t.Fatalf("CreateEvent() error = %v", err)
	}
	stored := repo.events[key(1, "bom")]
	if stored.RawICAL != validICS("bom") {
		t.Errorf("stored %q, want the object without its byte order mark", stored.RawICAL)
	}
	if stored.WriteMetadata == nil || stored.WriteMetadata.Summary == nil || *stored.WriteMetadata.Summary != "Planning" {
		t.Errorf("stored metadata = %+v, want the validator's", stored.WriteMetadata)
	}

	todoOnly := NewService(&store.Store{
		Calendars: &fakeCalendarRepo{calendars: map[int64]*store.CalendarAccess{
			1: {Calendar: store.Calendar{ID: 1, UserID: 1, Name: "Tasks", SupportedComponents: []string{"VTODO"}}, Editor: true},
		}},
		Events: &fakeEventRepo{events: map[string]store.Event{}},
	}, dav.ValidateCalendarObject)

	_, _, err := todoOnly.CreateEvent(context.Background(), user, 1, UpsertInput{RawICS: validICS("event"), ContentType: "text/calendar"})
	if !errors.Is(err, ErrBadRequest) || !strings.Contains(err.Error(), "supported-calendar-component") {
		t.Fatalf("CreateEvent() into a VTODO collection error = %v, want supported-calendar-component", err)
	}
}

func mustCalendarObject(t *testing.T, raw string) *calobject.Object {
	t.Helper()
	object, err := dav.ValidateCalendarObject(raw, nil)
	if err != nil {
		t.Fatalf("fixture refused: %v", err)
	}
	return object
}

type changedDeleteEventRepo struct{ *fakeEventRepo }

func (r *changedDeleteEventRepo) GetByResourceName(ctx context.Context, collectionID int64, name string) (*store.Event, error) {
	item, err := r.fakeEventRepo.GetByResourceName(ctx, collectionID, name)
	if item != nil {
		item.RawICAL = "updated data"
	}
	return item, err
}
func TestDeleteEventConcurrentChange(t *testing.T) {
	for _, conditional := range []bool{false, true} {
		t.Run(fmt.Sprint(conditional), func(t *testing.T) {
			base := &fakeEventRepo{events: map[string]store.Event{"1:changed": {CalendarID: 1, UID: "changed", ResourceName: "changed", ETag: "old-etag"}}}
			svc := newServiceWithRepos(true, base)
			svc.store.Events = &changedDeleteEventRepo{base}
			match, want := "", ErrConflict
			if conditional {
				match, want = "old-etag", ErrPreconditionFailed
			}
			err := svc.DeleteEvent(context.Background(), &store.User{ID: 1}, 1, "changed", match, "")
			if !errors.Is(err, want) {
				t.Fatalf("error = %v, want %v", err, want)
			}
			if len(base.events) != 1 {
				t.Fatal("concurrent update was deleted")
			}
		})
	}
}

// The UID names the event's resource, and both are unique index keys whose
// entries PostgreSQL bounds in size, so one past the limit is the caller's
// error rather than a failed write.
func TestCreateEventBoundsTheUID(t *testing.T) {
	user := &store.User{ID: 1}
	repo := &fakeEventRepo{events: map[string]store.Event{}}
	svc := newServiceWithRepos(true, repo)
	_, _, err := svc.CreateEvent(context.Background(), user, 1, UpsertInput{
		RawICS:      validICS(strings.Repeat("u", store.MaxIdentifierOctets+1)),
		ContentType: "text/calendar",
	})
	if !errors.Is(err, ErrBadRequest) {
		t.Fatalf("CreateEvent() error = %v, want ErrBadRequest", err)
	}
	if len(repo.events) != 0 {
		t.Fatal("the event was stored")
	}
	if _, created, err := svc.CreateEvent(context.Background(), user, 1, UpsertInput{
		RawICS:      validICS(strings.Repeat("u", store.MaxIdentifierOctets)),
		ContentType: "text/calendar",
	}); err != nil || !created {
		t.Fatalf("UID at the limit: created=%v err=%v", created, err)
	}
}

const appleEventUID = "6C4E3B5F-2A7D-4E3C-9F1B-8D2A6E7C4B10"

// appleAlarm is a VALARM as Apple Calendar writes it: RFC 9074 §4 gives the
// alarm its own UID, mirrored in X-WR-ALARMUID.
func appleAlarm(uid string) string {
	return "BEGIN:VALARM\r\nACTION:DISPLAY\r\nDESCRIPTION:Reminder\r\nTRIGGER:-PT15M\r\nUID:" + uid + "\r\nX-WR-ALARMUID:" + uid + "\r\nEND:VALARM\r\n"
}

func appleCalendar(components ...string) string {
	return "BEGIN:VCALENDAR\r\nVERSION:2.0\r\nPRODID:-//Apple Inc.//macOS 15.0//EN\r\n" + strings.Join(components, "") + "END:VCALENDAR\r\n"
}

func TestRawICSIdentifiesTheObjectByTopLevelComponentUIDs(t *testing.T) {
	user := &store.User{ID: 1}
	accepted := []struct {
		name string
		ics  string
	}{
		{
			name: "alarm after the event UID",
			ics: appleCalendar("BEGIN:VEVENT\r\nUID:" + appleEventUID + "\r\nSUMMARY:Planning\r\nDTSTART:20260320T100000Z\r\nDTEND:20260320T110000Z\r\n" +
				appleAlarm("D1B2C3E4-F5A6-4B7C-8D9E-0F1A2B3C4D5E") + "END:VEVENT\r\n"),
		},
		{
			name: "alarm before the event UID",
			ics: appleCalendar("BEGIN:VEVENT\r\n" + appleAlarm("D1B2C3E4-F5A6-4B7C-8D9E-0F1A2B3C4D5E") +
				"UID:" + appleEventUID + "\r\nSUMMARY:Planning\r\nDTSTART:20260320T100000Z\r\nDTEND:20260320T110000Z\r\nEND:VEVENT\r\n"),
		},
		{
			name: "recurring master and override with distinct alarm UIDs",
			ics: appleCalendar(
				"BEGIN:VEVENT\r\nUID:"+appleEventUID+"\r\nSUMMARY:Standup\r\nDTSTART:20260320T100000Z\r\nDTEND:20260320T103000Z\r\nRRULE:FREQ=WEEKLY;COUNT=10\r\n"+
					appleAlarm("A1A1A1A1-0000-4000-8000-000000000001")+"END:VEVENT\r\n",
				"BEGIN:VEVENT\r\n"+appleAlarm("B2B2B2B2-0000-4000-8000-000000000002")+
					"UID:"+appleEventUID+"\r\nRECURRENCE-ID:20260327T100000Z\r\nSUMMARY:Standup (moved)\r\nDTSTART:20260327T110000Z\r\nDTEND:20260327T113000Z\r\nEND:VEVENT\r\n",
			),
		},
	}
	for _, tt := range accepted {
		t.Run(tt.name, func(t *testing.T) {
			repo := &fakeEventRepo{events: map[string]store.Event{}}
			svc := newServiceWithRepos(true, repo)
			ev, created, err := svc.CreateEvent(context.Background(), user, 1, UpsertInput{RawICS: tt.ics, ContentType: "text/calendar"})
			if err != nil || !created || ev == nil || ev.UID != appleEventUID {
				t.Fatalf("CreateEvent() ev=%+v created=%v err=%v", ev, created, err)
			}
			ev, created, err = svc.UpdateEvent(context.Background(), user, 1, appleEventUID, UpsertInput{RawICS: tt.ics, ContentType: "text/calendar"})
			if err != nil || created || ev == nil || ev.UID != appleEventUID {
				t.Fatalf("UpdateEvent() ev=%+v created=%v err=%v", ev, created, err)
			}
		})
	}

	rejected := []struct {
		name string
		ics  string
		want error
	}{
		{
			name: "UID only on the alarm",
			ics: appleCalendar("BEGIN:VEVENT\r\nSUMMARY:Planning\r\nDTSTART:20260320T100000Z\r\nDTEND:20260320T110000Z\r\n" +
				appleAlarm("D1B2C3E4-F5A6-4B7C-8D9E-0F1A2B3C4D5E") + "END:VEVENT\r\n"),
			want: ErrBadRequest,
		},
		{
			name: "different UIDs on two top-level events",
			ics: appleCalendar(
				"BEGIN:VEVENT\r\nUID:"+appleEventUID+"\r\nDTSTART:20260320T100000Z\r\n"+appleAlarm("A1A1A1A1-0000-4000-8000-000000000001")+"END:VEVENT\r\n",
				"BEGIN:VEVENT\r\nUID:other-event\r\nDTSTART:20260321T100000Z\r\n"+appleAlarm("A1A1A1A1-0000-4000-8000-000000000001")+"END:VEVENT\r\n",
			),
			want: ErrConflict,
		},
	}
	for _, tt := range rejected {
		t.Run(tt.name, func(t *testing.T) {
			repo := &fakeEventRepo{events: map[string]store.Event{}}
			svc := newServiceWithRepos(true, repo)
			_, _, err := svc.CreateEvent(context.Background(), user, 1, UpsertInput{RawICS: tt.ics, ContentType: "text/calendar"})
			if !errors.Is(err, tt.want) {
				t.Fatalf("CreateEvent() error = %v, want %v", err, tt.want)
			}
			if len(repo.events) != 0 {
				t.Fatal("the event was stored")
			}
		})
	}
}

func TestRawICSReadsUIDWithParameters(t *testing.T) {
	user := &store.User{ID: 1}
	event := func(uidLine string) string {
		return "BEGIN:VEVENT\r\n" + uidLine + "\r\nSUMMARY:Planning\r\nDTSTART:20260320T100000Z\r\nDTEND:20260320T110000Z\r\n" +
			"BEGIN:VALARM\r\nACTION:DISPLAY\r\nTRIGGER:-PT15M\r\nUID;X-SOURCE=alarm:D1B2C3E4-F5A6-4B7C-8D9E-0F1A2B3C4D5E\r\nEND:VALARM\r\nEND:VEVENT\r\n"
	}
	accepted := []struct {
		name string
		ics  string
	}{
		{name: "parameter", ics: appleCalendar(event("UID;X-SOURCE=import:" + appleEventUID))},
		{name: "lowercase name", ics: appleCalendar(event("uid;x-source=import:" + appleEventUID))},
		{name: "quoted parameter holding a colon", ics: appleCalendar(event(`UID;X-SOURCE="https://example.com:8443/feed":` + appleEventUID))},
		{name: "plain and parameterized on master and override", ics: appleCalendar(
			event("UID:"+appleEventUID),
			"BEGIN:VEVENT\r\nUID;X-SOURCE=import:"+appleEventUID+"\r\nRECURRENCE-ID:20260327T100000Z\r\nDTSTART:20260327T110000Z\r\nEND:VEVENT\r\n",
		)},
	}
	for _, tt := range accepted {
		t.Run(tt.name, func(t *testing.T) {
			repo := &fakeEventRepo{events: map[string]store.Event{}}
			svc := newServiceWithRepos(true, repo)
			ev, created, err := svc.CreateEvent(context.Background(), user, 1, UpsertInput{RawICS: tt.ics, ContentType: "text/calendar"})
			if err != nil || !created || ev == nil || ev.UID != appleEventUID {
				t.Fatalf("CreateEvent() ev=%+v created=%v err=%v", ev, created, err)
			}
			if _, _, err := svc.UpdateEvent(context.Background(), user, 1, appleEventUID, UpsertInput{RawICS: tt.ics, ContentType: "text/calendar"}); err != nil {
				t.Fatalf("UpdateEvent() err=%v", err)
			}
		})
	}

	rejected := []struct {
		name string
		ics  string
		want error
	}{
		{name: "property name merely starting with UID", ics: appleCalendar(event("UIDX:" + appleEventUID)), want: ErrBadRequest},
		{name: "empty parameterized UID", ics: appleCalendar(event("UID;X-SOURCE=import:")), want: ErrBadRequest},
		{name: "parameterized UID differing from another event's UID", ics: appleCalendar(
			event("UID:"+appleEventUID),
			event("UID;X-SOURCE=import:other-event"),
		), want: ErrConflict},
	}
	for _, tt := range rejected {
		t.Run(tt.name, func(t *testing.T) {
			repo := &fakeEventRepo{events: map[string]store.Event{}}
			svc := newServiceWithRepos(true, repo)
			_, _, err := svc.CreateEvent(context.Background(), user, 1, UpsertInput{RawICS: tt.ics, ContentType: "text/calendar"})
			if !errors.Is(err, tt.want) {
				t.Fatalf("CreateEvent() error = %v, want %v", err, tt.want)
			}
			if len(repo.events) != 0 {
				t.Fatal("the event was stored")
			}
		})
	}
}
