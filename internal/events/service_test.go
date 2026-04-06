package events

import (
	"context"
	"errors"
	"net/http"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/jw6ventures/calcard/internal/store"
)

func TestServiceCRUDAndValidation(t *testing.T) {
	user := &store.User{ID: 1}

	t.Run("list and get calendar", func(t *testing.T) {
		svc := NewService(&store.Store{
			Calendars: &fakeCalendarRepo{calendars: map[int64]*store.CalendarAccess{
				1: {Calendar: store.Calendar{ID: 1, UserID: 1, Name: "Work"}, Editor: true},
			}},
			Events: &fakeEventRepo{events: map[string]store.Event{}},
		})
		cals, err := svc.ListCalendars(context.Background(), user)
		if err != nil || len(cals) != 1 {
			t.Fatalf("ListCalendars() err=%v len=%d", err, len(cals))
		}
		cal, err := svc.GetCalendar(context.Background(), user, 1)
		if err != nil || cal == nil {
			t.Fatalf("GetCalendar() err=%v cal=%v", err, cal)
		}
		events, err := svc.ListEvents(context.Background(), user, 1)
		if err != nil || len(events) != 0 {
			t.Fatalf("ListEvents() err=%v len=%d", err, len(events))
		}
	})

	t.Run("get calendar not found", func(t *testing.T) {
		svc := NewService(&store.Store{Calendars: &fakeCalendarRepo{calendars: map[int64]*store.CalendarAccess{}}})
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
		})
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
		_, _, err := svc.saveEvent(context.Background(), 1, "uid-1", "resource-b", validICS("uid-1"), "", "")
		if !errors.Is(err, ErrConflict) {
			t.Fatalf("expected ErrConflict, got %v", err)
		}
	})

	t.Run("save event precondition failed", func(t *testing.T) {
		repo := &fakeEventRepo{events: map[string]store.Event{
			"1:uid-1": {CalendarID: 1, UID: "uid-1", ResourceName: "resource-a", ETag: "etag-old"},
		}}
		svc := newServiceWithRepos(true, repo)
		_, _, err := svc.saveEvent(context.Background(), 1, "uid-1", "resource-a", validICS("uid-1"), `"wrong"`, "")
		if !errors.Is(err, ErrPreconditionFailed) {
			t.Fatalf("expected ErrPreconditionFailed, got %v", err)
		}
	})

	t.Run("delete success", func(t *testing.T) {
		repo := &fakeEventRepo{events: map[string]store.Event{
			"1:uid-1": {CalendarID: 1, UID: "uid-1", ResourceName: "uid-1", ETag: "etag-1"},
		}}
		svc := newServiceWithRepos(true, repo)
		err := svc.DeleteEvent(context.Background(), user, 1, "uid-1", `"etag-1"`, "")
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
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
		})
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
		if _, _, err := svc.normalizeEventPayload(UpsertInput{}, ""); !errors.Is(err, ErrBadRequest) {
			t.Fatalf("expected ErrBadRequest, got %v", err)
		}
		if _, _, err := svc.normalizeEventPayload(UpsertInput{RawICS: validICS("u1"), ContentType: ""}, ""); !errors.Is(err, ErrUnsupportedMediaType) {
			t.Fatalf("expected ErrUnsupportedMediaType, got %v", err)
		}
		if _, _, err := svc.normalizeEventPayload(UpsertInput{RawICS: validICS("u1"), ContentType: "text/calendar"}, "other"); !errors.Is(err, ErrBadRequest) {
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
	})

	events, err := svc.ListEvents(context.Background(), delegate, 1)
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
	})

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
		})

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
		})

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
		})

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
		})

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
	})

	events, err := svc.ListEvents(context.Background(), delegate, 1)
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

	t.Run("strict calendar validation branches", func(t *testing.T) {
		if !errors.Is(validateStrictICalendar("bad"), ErrBadRequest) {
			t.Fatal("expected bad request")
		}
		unsupported := "BEGIN:VCALENDAR\r\nBEGIN:VAVAILABILITY\r\nUID:x\r\nEND:VAVAILABILITY\r\nEND:VCALENDAR\r\n"
		if !errors.Is(validateStrictICalendar(unsupported), ErrBadRequest) {
			t.Fatal("expected bad request")
		}
		if !errors.Is(validateStrictICalendar(methodICS("x")), ErrConflict) {
			t.Fatal("expected conflict")
		}
		multiUID := "BEGIN:VCALENDAR\r\nBEGIN:VEVENT\r\nUID:a\r\nEND:VEVENT\r\nBEGIN:VEVENT\r\nUID:b\r\nEND:VEVENT\r\nEND:VCALENDAR\r\n"
		if !errors.Is(validateStrictICalendar(multiUID), ErrConflict) {
			t.Fatal("expected conflict")
		}
		tooMany := "BEGIN:VCALENDAR\r\nBEGIN:VEVENT\r\nUID:x\r\nDTSTART:20260320T100000Z\r\nDTEND:20260320T110000Z\r\nRRULE:FREQ=DAILY;COUNT=2001\r\nEND:VEVENT\r\nEND:VCALENDAR\r\n"
		if !errors.Is(validateStrictICalendar(tooMany), ErrBadRequest) {
			t.Fatal("expected bad request")
		}
		farFuture := "BEGIN:VCALENDAR\r\nBEGIN:VEVENT\r\nUID:x\r\nDTSTART:22010101T100000Z\r\nDTEND:22010101T110000Z\r\nEND:VEVENT\r\nEND:VCALENDAR\r\n"
		if !errors.Is(validateStrictICalendar(farFuture), ErrBadRequest) {
			t.Fatal("expected bad request")
		}
		var b strings.Builder
		b.WriteString("BEGIN:VCALENDAR\r\nBEGIN:VEVENT\r\nUID:x\r\nDTSTART:20260320T100000Z\r\nDTEND:20260320T110000Z\r\n")
		for i := 0; i < caldavMaxAttendees+1; i++ {
			b.WriteString("ATTENDEE:mailto:user@example.com\r\n")
		}
		b.WriteString("END:VEVENT\r\nEND:VCALENDAR\r\n")
		if !errors.Is(validateStrictICalendar(b.String()), ErrBadRequest) {
			t.Fatal("expected bad request")
		}
		if err := validateStrictICalendar(validICS("ok")); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
	})

	t.Run("ical parsing helpers", func(t *testing.T) {
		if _, err := extractUIDFromICalendar("BEGIN:VCALENDAR\r\nEND:VCALENDAR\r\n"); err == nil {
			t.Fatal("expected missing uid error")
		}
		if _, err := extractUIDFromICalendar("BEGIN:VCALENDAR\r\nBEGIN:VEVENT\r\nUID:\r\nEND:VEVENT\r\nEND:VCALENDAR\r\n"); err == nil {
			t.Fatal("expected empty uid error")
		}
		if !hasMultipleDifferentUIDs("BEGIN:VCALENDAR\r\nBEGIN:VEVENT\r\nUID:a\r\nEND:VEVENT\r\nBEGIN:VEVENT\r\nUID:b\r\nEND:VEVENT\r\nEND:VCALENDAR\r\n") {
			t.Fatal("expected multiple uids")
		}
		if !containsICalMethodProperty(methodICS("x")) {
			t.Fatal("expected method property")
		}
		if _, err := parseICalDateTime("not-a-date"); err == nil {
			t.Fatal("expected parse error")
		}
		if _, err := parseICalDateTime("20260320T100000Z"); err != nil {
			t.Fatalf("unexpected parse error: %v", err)
		}
		if got := extractICalComponentTypes(validICS("ok")); len(got) == 0 {
			t.Fatal("expected components")
		}
		if count, ok := extractICalRRULECount("RRULE:FREQ=DAILY;COUNT=2"); !ok || count != 2 {
			t.Fatalf("unexpected RRULE count result ok=%v count=%d", ok, count)
		}
		if count, ok := extractICalRRULECount("RRULE:FREQ=DAILY;COUNT=abc"); ok || count != 0 {
			t.Fatal("expected invalid count handling")
		}
		if count := countICalAttendees("ATTENDEE:a\r\nATTENDEE:b\r\n"); count != 2 {
			t.Fatalf("expected 2 attendees, got %d", count)
		}
		if conds := validateCalendarObjectResource("BEGIN:VCALENDAR\r\nBEGIN:VEVENT\r\nEND:VEVENT\r\nEND:VCALENDAR\r\n"); len(conds) == 0 {
			t.Fatal("expected validation conditions")
		}
		if vals := extractICalDateTimes(validICS("ok")); len(vals) < 2 {
			t.Fatalf("expected date times, got %d", len(vals))
		}
	})

	t.Run("misc helpers", func(t *testing.T) {
		if err := validateEventDates("2026-03-20", "2026-03-20"); err == nil {
			t.Fatal("expected end after start error")
		}
		if got := ensureCRLF("A\nB"); got != "A\r\nB\r\n" {
			t.Fatalf("unexpected CRLF result %q", got)
		}
		minDate, maxDate := caldavDateLimits()
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
	})
}

func validICS(uid string) string {
	return "BEGIN:VCALENDAR\r\nVERSION:2.0\r\nBEGIN:VEVENT\r\nUID:" + uid + "\r\nSUMMARY:Planning\r\nDTSTART:20260320T100000Z\r\nDTEND:20260320T110000Z\r\nEND:VEVENT\r\nEND:VCALENDAR\r\n"
}

func methodICS(uid string) string {
	return "BEGIN:VCALENDAR\r\nVERSION:2.0\r\nMETHOD:REQUEST\r\nBEGIN:VEVENT\r\nUID:" + uid + "\r\nSUMMARY:Planning\r\nDTSTART:20260320T100000Z\r\nDTEND:20260320T110000Z\r\nEND:VEVENT\r\nEND:VCALENDAR\r\n"
}

type fakeCalendarRepo struct {
	calendars map[int64]*store.CalendarAccess
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
func (f *fakeCalendarRepo) Update(ctx context.Context, userID, id int64, name string, description, timezone *string) error {
	return nil
}
func (f *fakeCalendarRepo) UpdateProperties(ctx context.Context, id int64, name string, description, timezone *string) error {
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
func (f *fakeEventRepo) ListForCalendar(ctx context.Context, calendarID int64) ([]store.Event, error) {
	var out []store.Event
	for _, ev := range f.events {
		if ev.CalendarID == calendarID {
			out = append(out, ev)
		}
	}
	return out, nil
}
func (f *fakeEventRepo) ListForCalendarPaginated(ctx context.Context, calendarID int64, limit, offset int) (*store.PaginatedResult[store.Event], error) {
	return nil, nil
}
func (f *fakeEventRepo) ListByUIDs(ctx context.Context, calendarID int64, uids []string) ([]store.Event, error) {
	return nil, nil
}
func (f *fakeEventRepo) ListModifiedSince(ctx context.Context, calendarID int64, since time.Time) ([]store.Event, error) {
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

func (f *fakeACLRepo) DeletePrincipalEntriesByResourcePrefix(ctx context.Context, principalHref, resourcePathPrefix string) error {
	return nil
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
