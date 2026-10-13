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

	_, err := svc.CreateEvent(context.Background(), &store.User{ID: 1}, 1, UpsertInput{RawICS: validICS("refused")})
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
		ev, err := svc.CreateEvent(context.Background(), user, 1, UpsertInput{
			RawICS: validICS("raw-1"),
		})
		if err != nil || ev == nil || ev.UID != "raw-1" {
			t.Fatalf("CreateEvent() err=%v ev=%+v", err, ev)
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

		_, err := svc.CreateEvent(context.Background(), user, 1, UpsertInput{
			Structured: &StructuredInput{Summary: "x", DTStart: "2026-03-20T10:00", DTEnd: "2026-03-20T11:00"},
		})
		if !errors.Is(err, ErrForbidden) {
			t.Fatalf("expected ErrForbidden, got %v", err)
		}
	})

	t.Run("create conflict existing uid", func(t *testing.T) {
		repo := &fakeEventRepo{events: map[string]store.Event{"1:dup": {CalendarID: 1, UID: "dup", ResourceName: "dup", ETag: "e1"}}}
		svc := newServiceWithRepos(true, repo)
		_, err := svc.CreateEvent(context.Background(), user, 1, UpsertInput{
			RawICS: validICS("dup"),
		})
		if !errors.Is(err, ErrConflict) {
			t.Fatalf("expected ErrConflict, got %v", err)
		}
	})

	t.Run("create if none match fails", func(t *testing.T) {
		repo := &fakeEventRepo{events: map[string]store.Event{"1:dup": {CalendarID: 1, UID: "dup", ResourceName: "dup", ETag: "e1"}}}
		svc := newServiceWithRepos(true, repo)
		_, err := svc.CreateEvent(context.Background(), user, 1, UpsertInput{
			RawICS:      validICS("dup"),
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
			RawICS:  validICS("uid-1"),
			IfMatch: `"etag-old"`,
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
		if _, err := svc.normalizeEventPayload(UpsertInput{RawICS: validICS("u1")}, "other", nil); !errors.Is(err, ErrBadRequest) {
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

// Grants on one event let a principal with no access to its calendar read and
// change that event, but not remove it: DAV:unbind is a privilege of the
// calendar the event leaves (RFC 3744 §3.10), as DAV DELETE holds it.
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
		RawICS:  validICS("event-1"),
		IfMatch: `"etag-1"`,
	})
	if err != nil || updated == nil || updated.UID != "event-1" {
		t.Fatalf("UpdateEvent() err=%v updated=%#v", err, updated)
	}

	if err := svc.DeleteEvent(context.Background(), delegate, 1, "event-1", `"`+updated.ETag+`"`, ""); !errors.Is(err, ErrForbidden) {
		t.Fatalf("DeleteEvent() error = %v, want ErrForbidden for want of DAV:unbind on the calendar", err)
	}
	if got, _ := repo.GetByUID(context.Background(), 1, "event-1"); got == nil {
		t.Fatal("DeleteEvent() removed the event without DAV:unbind on the calendar")
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

		_, err := svc.CreateEvent(context.Background(), delegate, 1, UpsertInput{
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
			RawICS:  validICS("event-1"),
			IfMatch: `"etag-1"`,
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
			RawICS:  validICS("event-1"),
			IfMatch: `"etag-1"`,
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

// countingValidator is dav.ValidateCalendarObject, counting its calls.
func countingValidator(calls *int) calobject.Validator {
	return func(raw string, cal *store.CalendarAccess) (*calobject.Object, error) {
		*calls++
		return dav.ValidateCalendarObject(raw, cal)
	}
}

// A create is refused for want of DAV:bind on the calendar before the event is
// built from its body or validated, as a DAV PUT is: building and validating a calendar object spends the
// recurrence and VTIMEZONE work a principal who may not add to the calendar
// must not be able to make the server do, and whether the UID is taken or the
// preconditions hold is no answer to give them either.
func TestCreateEventRefusesAPrincipalWithoutBindBeforeBuildingTheEvent(t *testing.T) {
	delegate := &store.User{ID: 2}
	readOnly := []store.ACLEntry{
		{ResourcePath: "/dav/calendars/1", PrincipalHref: "/dav/principals/2/", IsGrant: true, Privilege: "read"},
		{ResourcePath: "/dav/calendars/1", PrincipalHref: "/dav/principals/2/", IsGrant: true, Privilege: "write-content"},
	}
	canBind := append(slices.Clone(readOnly), store.ACLEntry{ResourcePath: "/dav/calendars/1", PrincipalHref: "/dav/principals/2/", IsGrant: true, Privilege: "bind"})
	structured := &StructuredInput{Summary: "Create", DTStart: "2026-03-20T10:00", DTEnd: "2026-03-20T11:00"}
	tests := map[string]struct {
		acl            []store.ACLEntry
		input          UpsertInput
		want           error
		wantValidation int
	}{
		"structured":                                    {acl: readOnly, input: UpsertInput{Structured: structured}, want: ErrForbidden},
		"structured the builder refuses":                {acl: readOnly, input: UpsertInput{Structured: &StructuredInput{DTStart: "2026-03-20T10:00", DTEnd: "2026-03-20T11:00"}}, want: ErrForbidden},
		"structured in a zone the server does not know": {acl: readOnly, input: UpsertInput{Structured: &StructuredInput{Summary: "Create", DTStart: "2026-03-20T10:00", DTEnd: "2026-03-20T11:00", Timezone: "Nowhere/Zone"}}, want: ErrForbidden},
		"raw iCalendar":                                 {acl: readOnly, input: UpsertInput{RawICS: validICS("new")}, want: ErrForbidden},
		"raw iCalendar the validator fails":             {acl: readOnly, input: UpsertInput{RawICS: methodICS("new")}, want: ErrForbidden},
		"UID already stored":                            {acl: readOnly, input: UpsertInput{RawICS: validICS("taken")}, want: ErrForbidden},
		"If-Match on a new UID":                         {acl: readOnly, input: UpsertInput{RawICS: validICS("new"), IfMatch: `"etag"`}, want: ErrForbidden},
		"If-None-Match * on a stored UID":               {acl: readOnly, input: UpsertInput{RawICS: validICS("taken"), IfNoneMatch: "*"}, want: ErrForbidden},
		"with bind":                                     {acl: canBind, input: UpsertInput{Structured: structured}, wantValidation: 1},
		"with bind, UID already stored":                 {acl: canBind, input: UpsertInput{RawICS: validICS("taken")}, want: ErrConflict, wantValidation: 1},
	}
	for name, tt := range tests {
		t.Run(name, func(t *testing.T) {
			validations := 0
			repo := &fakeEventRepo{events: map[string]store.Event{
				"1:taken": {CalendarID: 1, UID: "taken", ResourceName: "taken", RawICAL: validICS("taken"), ETag: "etag"},
			}}
			svc := NewService(&store.Store{
				Calendars:  calendarSharedThrough(t, tt.acl),
				Events:     repo,
				ACLEntries: &fakeACLRepo{entries: tt.acl},
			}, countingValidator(&validations))

			_, err := svc.CreateEvent(context.Background(), delegate, 1, tt.input)
			if !errors.Is(err, tt.want) {
				t.Fatalf("CreateEvent() error = %v, want %v", err, tt.want)
			}
			if validations != tt.wantValidation {
				t.Fatalf("validator ran %d times, want %d", validations, tt.wantValidation)
			}
		})
	}
}

// racingEventRepo stores competitor the first time GetByUID finds its UID
// free, as a create running alongside would between that look and the write.
type racingEventRepo struct {
	*fakeEventRepo
	competitor store.Event
	raced      bool
}

func (r *racingEventRepo) GetByUID(ctx context.Context, calendarID int64, uid string) (*store.Event, error) {
	ev, err := r.fakeEventRepo.GetByUID(ctx, calendarID, uid)
	if ev == nil && err == nil && !r.raced && uid == r.competitor.UID {
		r.raced = true
		r.events[key(r.competitor.CalendarID, r.competitor.UID)] = r.competitor
	}
	return ev, err
}

// A create only ever adds an event. One that finds the UID free but loses the
// race to store it is refused rather than overwriting the winner, so a
// principal holding DAV:bind but not DAV:write-content cannot replace an event
// by racing its creator.
func TestCreateEventDoesNotOverwriteAnEventStoredWhileItRan(t *testing.T) {
	bindOnly := []store.ACLEntry{
		{ResourcePath: "/dav/calendars/1", PrincipalHref: "/dav/principals/2/", IsGrant: true, Privilege: "read"},
		{ResourcePath: "/dav/calendars/1", PrincipalHref: "/dav/principals/2/", IsGrant: true, Privilege: "bind"},
	}
	competitor := store.Event{CalendarID: 1, UID: "raced", ResourceName: "raced", RawICAL: validICS("raced"), ETag: "theirs"}
	tests := map[string]struct {
		ifNoneMatch string
		want        error
	}{
		"unconditional":   {want: ErrConflict},
		"If-None-Match *": {ifNoneMatch: "*", want: ErrPreconditionFailed},
	}
	for name, tt := range tests {
		t.Run(name, func(t *testing.T) {
			repo := &racingEventRepo{fakeEventRepo: &fakeEventRepo{events: map[string]store.Event{}}, competitor: competitor}
			svc := NewService(&store.Store{
				Calendars:  calendarSharedThrough(t, bindOnly),
				Events:     repo,
				ACLEntries: &fakeACLRepo{entries: bindOnly},
			}, dav.ValidateCalendarObject)

			body := strings.Replace(validICS("raced"), "SUMMARY:Planning", "SUMMARY:Mine", 1)
			_, err := svc.CreateEvent(context.Background(), &store.User{ID: 2}, 1, UpsertInput{RawICS: body, IfNoneMatch: tt.ifNoneMatch})
			if !repo.raced {
				t.Fatal("the competing create never ran")
			}
			if !errors.Is(err, tt.want) {
				t.Fatalf("CreateEvent() error = %v, want %v", err, tt.want)
			}
			if stored := repo.events[key(1, "raced")]; stored.ETag != "theirs" || stored.RawICAL != competitor.RawICAL {
				t.Fatalf("the competing event was overwritten: %#v", stored)
			}
		})
	}
}

// DAV:bind is a privilege on the collection (RFC 3744 §3.9), so a create needs
// it on the calendar. An ACE on the path the new object would take belongs to
// the object already stored there, under another UID, and neither grants nor
// denies adding one.
func TestCreateEventNeedsBindOnTheCalendarWhateverAnObjectACLSays(t *testing.T) {
	delegate := &store.User{ID: 2}
	collectionRead := store.ACLEntry{ResourcePath: "/dav/calendars/1", PrincipalHref: "/dav/principals/2/", IsGrant: true, Privilege: "read"}
	collectionBind := store.ACLEntry{ResourcePath: "/dav/calendars/1", PrincipalHref: "/dav/principals/2/", IsGrant: true, Privilege: "bind"}
	objectGrant := store.ACLEntry{ResourcePath: "/dav/calendars/1/new.ics", PrincipalHref: "/dav/principals/2/", IsGrant: true, Privilege: "all"}
	objectDeny := store.ACLEntry{ResourcePath: "/dav/calendars/1/new.ics", PrincipalHref: "/dav/principals/2/", IsGrant: false, Privilege: "all"}
	tests := map[string]struct {
		storedName string
		acl        []store.ACLEntry
		want       error
	}{
		"object grant without calendar bind":          {storedName: "new", acl: []store.ACLEntry{collectionRead, objectGrant}, want: ErrForbidden},
		"object deny with calendar bind":              {storedName: "new", acl: []store.ACLEntry{collectionRead, collectionBind, objectDeny}, want: ErrConflict},
		"grant on an object named with its extension": {storedName: "new.ics", acl: []store.ACLEntry{collectionRead, objectGrant}, want: ErrForbidden},
	}
	for name, tt := range tests {
		t.Run(name, func(t *testing.T) {
			repo := &fakeEventRepo{events: map[string]store.Event{
				"1:other": {CalendarID: 1, UID: "other", ResourceName: tt.storedName, RawICAL: validICS("other"), ETag: "etag"},
			}}
			svc := NewService(&store.Store{
				Calendars:  calendarSharedThrough(t, tt.acl),
				Events:     repo,
				ACLEntries: &fakeACLRepo{entries: tt.acl},
			}, dav.ValidateCalendarObject)

			_, err := svc.CreateEvent(context.Background(), delegate, 1, UpsertInput{RawICS: validICS("new")})
			if !errors.Is(err, tt.want) {
				t.Fatalf("CreateEvent() error = %v, want %v", err, tt.want)
			}
			if _, stored := repo.events[key(1, "new")]; stored {
				t.Fatal("stored the new event")
			}
		})
	}
}

// An update is refused for want of DAV:write-content on the event before the
// event is built from its body or validated, as a DAV PUT to a stored object
// is.
func TestUpdateEventRefusesAPrincipalWithoutWriteContentBeforeBuildingTheEvent(t *testing.T) {
	delegate := &store.User{ID: 2}
	cannotWrite := []store.ACLEntry{
		{ResourcePath: "/dav/calendars/1", PrincipalHref: "/dav/principals/2/", IsGrant: true, Privilege: "read"},
		{ResourcePath: "/dav/calendars/1", PrincipalHref: "/dav/principals/2/", IsGrant: true, Privilege: "bind"},
	}
	canWrite := append(slices.Clone(cannotWrite), store.ACLEntry{ResourcePath: "/dav/calendars/1/event-1", PrincipalHref: "/dav/principals/2/", IsGrant: true, Privilege: "write-content"})
	tests := map[string]struct {
		acl            []store.ACLEntry
		input          UpsertInput
		want           error
		wantValidation int
	}{
		"structured":                            {acl: cannotWrite, input: UpsertInput{Structured: &StructuredInput{Summary: "Update", DTStart: "2026-03-20T10:00", DTEnd: "2026-03-20T11:00"}}, want: ErrForbidden},
		"structured the builder refuses":        {acl: cannotWrite, input: UpsertInput{Structured: &StructuredInput{DTStart: "2026-03-20T10:00", DTEnd: "2026-03-20T11:00"}}, want: ErrForbidden},
		"structured with a malformed until":     {acl: cannotWrite, input: UpsertInput{Structured: &StructuredInput{Summary: "Update", DTStart: "2026-03-20T10:00", DTEnd: "2026-03-20T11:00", Recurrence: &StructuredRecurrence{Frequency: "DAILY", Until: "June"}}}, want: ErrForbidden},
		"raw iCalendar":                         {acl: cannotWrite, input: UpsertInput{RawICS: validICS("event-1")}, want: ErrForbidden},
		"raw iCalendar the validator fails":     {acl: cannotWrite, input: UpsertInput{RawICS: methodICS("event-1")}, want: ErrForbidden},
		"with write-content":                    {acl: canWrite, input: UpsertInput{RawICS: validICS("event-1")}, wantValidation: 1},
		"with write-content, validator refuses": {acl: canWrite, input: UpsertInput{RawICS: methodICS("event-1")}, want: ErrConflict, wantValidation: 1},
	}
	for name, tt := range tests {
		t.Run(name, func(t *testing.T) {
			validations := 0
			svc := NewService(&store.Store{
				Calendars: calendarSharedThrough(t, tt.acl),
				Events: &fakeEventRepo{events: map[string]store.Event{
					"1:event-1": {CalendarID: 1, UID: "event-1", ResourceName: "event-1", RawICAL: validICS("event-1"), ETag: "etag-1"},
				}},
				ACLEntries: &fakeACLRepo{entries: tt.acl},
			}, countingValidator(&validations))

			_, _, err := svc.UpdateEvent(context.Background(), delegate, 1, "event-1", tt.input)
			if !errors.Is(err, tt.want) {
				t.Fatalf("UpdateEvent() error = %v, want %v", err, tt.want)
			}
			if validations != tt.wantValidation {
				t.Fatalf("validator ran %d times, want %d", validations, tt.wantValidation)
			}
		})
	}
}

// targetOutcome is the error a request answers with when its UID names the
// stored event and when it names none.
type targetOutcome struct{ stored, missing error }

// aceSpec is one ACE a principal holds, on the calendar or on the event
// "stored".
type aceSpec struct {
	onEvent   bool
	privilege string
	deny      bool
}

func onCalendar(privilege string) aceSpec { return aceSpec{privilege: privilege} }
func onEvent(privilege string) aceSpec    { return aceSpec{onEvent: true, privilege: privilege} }
func deniedOnEvent(privilege string) aceSpec {
	return aceSpec{onEvent: true, privilege: privilege, deny: true}
}
func deniedOnCalendar(privilege string) aceSpec { return aceSpec{privilege: privilege, deny: true} }

// eventWriteAccessCase is a principal of a calendar holding the event
// "stored", and what an update and a delete answer them with no conditional
// header, with If-Match naming the stored ETag, and with If-None-Match: *.
type eventWriteAccessCase struct {
	principal string
	aces      []aceSpec
	// concealed principals get the same answer whether or not the UID exists.
	concealed bool
	update    [3]targetOutcome
	delete    [3]targetOutcome
	// davDelete, when set, is what a DAV DELETE answers where it differs from
	// delete: a principal holding no privilege on the calendar, only ACEs
	// granting none or denying every one, is answered 403 by DAV, while REST
	// answers 404 so as not to confirm the calendar exists, as GetEvent does.
	davDelete *[3]targetOutcome
}

// eventWriteAccessCases pins the order of the checks. An update needs
// DAV:write-content on the event; a principal who can neither read it nor
// hold that is answered as for a UID that names nothing. A delete follows DAV
// DELETE (internal/dav/delete.go): it needs DAV:unbind on the calendar (RFC
// 3744 §3.10) and some privilege on the event, so an event-level deny of
// every privilege refuses it, and a principal holding no privilege on the
// event is answered as for a UID that names nothing. For such a UID, a
// principal holding no privilege on the calendar gets 404, as GetEvent
// answers them, and one holding some but not the privilege the method needs
// on the calendar gets 403. Where DAV answers 403 instead of that 404, the
// case says so in davDelete. The precondition comes after the privileges,
// and for a delete after the 404 too (RFC 9110 §13.2.1).
// The HTTP test in internal/api/handler_test.go,
// TestEventUpdatesAndDeletesAnswerAPrincipalWhoCannotSeeTheEventAlikeWhetherOrNotItExists,
// repeats these cases; test packages cannot share them, so a change to one
// belongs in both.
var eventWriteAccessCases = []eventWriteAccessCase{
	{
		principal: "no access", concealed: true,
		update: [3]targetOutcome{{ErrNotFound, ErrNotFound}, {ErrNotFound, ErrNotFound}, {ErrNotFound, ErrNotFound}},
		delete: [3]targetOutcome{{ErrNotFound, ErrNotFound}, {ErrNotFound, ErrNotFound}, {ErrNotFound, ErrNotFound}},
	},
	{
		principal: "read-only", aces: []aceSpec{onCalendar("read")},
		update: [3]targetOutcome{{ErrForbidden, ErrForbidden}, {ErrForbidden, ErrForbidden}, {ErrForbidden, ErrForbidden}},
		delete: [3]targetOutcome{{ErrForbidden, ErrForbidden}, {ErrForbidden, ErrForbidden}, {ErrForbidden, ErrForbidden}},
	},
	{
		principal: "calendar reader the event is hidden from", concealed: true,
		aces:   []aceSpec{deniedOnEvent("all"), onCalendar("read")},
		update: [3]targetOutcome{{ErrForbidden, ErrForbidden}, {ErrForbidden, ErrForbidden}, {ErrForbidden, ErrForbidden}},
		delete: [3]targetOutcome{{ErrForbidden, ErrForbidden}, {ErrForbidden, ErrForbidden}, {ErrForbidden, ErrForbidden}},
	},
	{
		principal: "calendar writer the event is hidden from", concealed: true,
		aces:   []aceSpec{deniedOnEvent("all"), onCalendar("read"), onCalendar("write-content"), onCalendar("unbind")},
		update: [3]targetOutcome{{ErrNotFound, ErrNotFound}, {ErrNotFound, ErrNotFound}, {ErrNotFound, ErrNotFound}},
		delete: [3]targetOutcome{{ErrNotFound, ErrNotFound}, {ErrNotFound, ErrNotFound}, {ErrNotFound, ErrNotFound}},
	},
	{
		principal: "event grant", aces: []aceSpec{onEvent("read"), onEvent("write-content"), onEvent("unbind")},
		update: [3]targetOutcome{{nil, ErrNotFound}, {nil, ErrNotFound}, {ErrPreconditionFailed, ErrNotFound}},
		delete: [3]targetOutcome{{ErrForbidden, ErrNotFound}, {ErrForbidden, ErrNotFound}, {ErrForbidden, ErrNotFound}},
	},
	{
		principal: "write", aces: []aceSpec{onCalendar("read"), onCalendar("write-content"), onCalendar("unbind")},
		update: [3]targetOutcome{{nil, ErrNotFound}, {nil, ErrNotFound}, {ErrPreconditionFailed, ErrNotFound}},
		delete: [3]targetOutcome{{nil, ErrNotFound}, {nil, ErrNotFound}, {ErrPreconditionFailed, ErrNotFound}},
	},
	{
		principal: "calendar all, event unbind denied", aces: []aceSpec{deniedOnEvent("unbind"), onCalendar("all")},
		update: [3]targetOutcome{{nil, ErrNotFound}, {nil, ErrNotFound}, {ErrPreconditionFailed, ErrNotFound}},
		delete: [3]targetOutcome{{nil, ErrNotFound}, {nil, ErrNotFound}, {ErrPreconditionFailed, ErrNotFound}},
	},
	{
		principal: "calendar deny all", aces: []aceSpec{deniedOnCalendar("all")}, concealed: true,
		update:    [3]targetOutcome{{ErrNotFound, ErrNotFound}, {ErrNotFound, ErrNotFound}, {ErrNotFound, ErrNotFound}},
		delete:    [3]targetOutcome{{ErrNotFound, ErrNotFound}, {ErrNotFound, ErrNotFound}, {ErrNotFound, ErrNotFound}},
		davDelete: &[3]targetOutcome{{ErrForbidden, ErrForbidden}, {ErrForbidden, ErrForbidden}, {ErrForbidden, ErrForbidden}},
	},
	{
		principal: "calendar read-acl only", aces: []aceSpec{onCalendar("read-acl")}, concealed: true,
		update:    [3]targetOutcome{{ErrNotFound, ErrNotFound}, {ErrNotFound, ErrNotFound}, {ErrNotFound, ErrNotFound}},
		delete:    [3]targetOutcome{{ErrNotFound, ErrNotFound}, {ErrNotFound, ErrNotFound}, {ErrNotFound, ErrNotFound}},
		davDelete: &[3]targetOutcome{{ErrForbidden, ErrForbidden}, {ErrForbidden, ErrForbidden}, {ErrForbidden, ErrForbidden}},
	},
	{
		principal: "event read, calendar deny all", aces: []aceSpec{onEvent("read"), deniedOnCalendar("all")},
		update:    [3]targetOutcome{{ErrForbidden, ErrNotFound}, {ErrForbidden, ErrNotFound}, {ErrForbidden, ErrNotFound}},
		delete:    [3]targetOutcome{{ErrForbidden, ErrNotFound}, {ErrForbidden, ErrNotFound}, {ErrForbidden, ErrNotFound}},
		davDelete: &[3]targetOutcome{{ErrForbidden, ErrForbidden}, {ErrForbidden, ErrForbidden}, {ErrForbidden, ErrForbidden}},
	},
}

// eventWriteAccessHeaders are the conditional headers eventWriteAccessCases
// lists outcomes for, in order.
var eventWriteAccessHeaders = []struct{ name, ifMatch, ifNoneMatch string }{
	{name: "unconditional"},
	{name: "If-Match", ifMatch: `"etag"`},
	{name: "If-None-Match *", ifNoneMatch: "*"},
}

// eventWriteAccessEntries are the ACEs the principal userID holds as
// eventWriteAccessCases describes them, keyed by resource path.
func eventWriteAccessEntries(calendarID, userID int64, aces []aceSpec) map[string][]store.ACLEntry {
	collection := fmt.Sprintf("/dav/calendars/%d", calendarID)
	entries := map[string][]store.ACLEntry{}
	for _, ace := range aces {
		path := collection
		if ace.onEvent {
			path += "/stored.ics"
		}
		entries[path] = append(entries[path], store.ACLEntry{ResourcePath: path, PrincipalHref: fmt.Sprintf("/dav/principals/%d/", userID), IsGrant: !ace.deny, Privilege: ace.privilege})
	}
	return entries
}

// getAccessiblePrivileges lists each privilege GetAccessible resolves and the
// ACE privileges that decide it, the first applicable such ACE deciding, as
// internal/store's calendar access SQL lists them.
var getAccessiblePrivileges = []struct {
	name      string
	decidedBy []string
}{
	{"read", []string{"read", "all"}},
	{"read-free-busy", []string{"read-free-busy", "read", "all"}},
	{"write", []string{"write", "all"}},
	{"write-content", []string{"write-content", "write", "all"}},
	{"write-properties", []string{"write-properties", "write", "all"}},
	{"bind", []string{"bind", "write", "all"}},
	{"unbind", []string{"unbind", "write", "all"}},
}

// privilegesGetAccessibleIgnores are ACE privileges no calendar access
// decision reads.
var privilegesGetAccessibleIgnores = []string{"read-acl", "write-acl", "read-current-user-privilege-set", "unlock"}

// accessibleAs is the calendar as GetAccessible returns it to a principal
// whose ACEs are entries, keyed by path: nil when no ACE of theirs on the
// calendar, nor on an event of it they hold a grant on, allows any of
// getAccessiblePrivileges; otherwise shared, with the calendar's own ACEs
// resolved. It fails on an ACE it does not model.
func accessibleAs(t *testing.T, cal store.Calendar, entries map[string][]store.ACLEntry) *store.CalendarAccess {
	t.Helper()
	collection := fmt.Sprintf("/dav/calendars/%d", cal.ID)
	for path, pathEntries := range entries {
		if path != collection && !strings.HasPrefix(path, collection+"/") {
			t.Fatalf("accessibleAs does not model an ACE on %s, outside calendar %d", path, cal.ID)
		}
		for _, entry := range pathEntries {
			known := slices.Contains(privilegesGetAccessibleIgnores, entry.Privilege)
			for _, privilege := range getAccessiblePrivileges {
				known = known || slices.Contains(privilege.decidedBy, entry.Privilege)
			}
			if !known {
				t.Fatalf("accessibleAs does not model the privilege %q", entry.Privilege)
			}
		}
	}
	allows := func(path string, decidedBy []string) bool {
		for _, entry := range entries[path] {
			if slices.Contains(decidedBy, entry.Privilege) {
				return entry.IsGrant
			}
		}
		return false
	}
	resolved := map[string]bool{}
	reachable := false
	for _, privilege := range getAccessiblePrivileges {
		resolved[privilege.name] = allows(collection, privilege.decidedBy)
		reachable = reachable || resolved[privilege.name]
	}
	for path, pathEntries := range entries {
		if path == collection || !slices.ContainsFunc(pathEntries, func(entry store.ACLEntry) bool { return entry.IsGrant }) {
			continue
		}
		for _, privilege := range getAccessiblePrivileges {
			reachable = reachable || allows(path, privilege.decidedBy)
		}
	}
	if !reachable {
		return nil
	}
	privileges := store.CalendarPrivileges{
		Read: resolved["read"], ReadFreeBusy: resolved["read-free-busy"], Write: resolved["write"],
		WriteContent: resolved["write-content"], WriteProperties: resolved["write-properties"],
		Bind: resolved["bind"], Unbind: resolved["unbind"],
	}.Normalized()
	return &store.CalendarAccess{Calendar: cal, Shared: true, PrivilegesResolved: true, Privileges: privileges, Editor: privileges.AllowsEventEditing()}
}

// eventWriteAccessUserID is the user eventWriteAccessStore gives the case at
// index i; user 1 owns the calendar.
func eventWriteAccessUserID(i int) int64 { return int64(i) + 2 }

// eventWriteAccessStore is a calendar of user 1's holding the event "stored"
// and shared with no one but through the ACEs eventWriteAccessCases names.
func eventWriteAccessStore(t *testing.T) (*store.Store, *fakeEventRepo) {
	t.Helper()
	repo := &fakeEventRepo{events: map[string]store.Event{
		"1:stored": {CalendarID: 1, UID: "stored", ResourceName: "stored", RawICAL: validICS("stored"), ETag: "etag"},
	}}
	acls := &fakeACLRepo{}
	for i, tc := range eventWriteAccessCases {
		for _, pathEntries := range eventWriteAccessEntries(1, eventWriteAccessUserID(i), tc.aces) {
			acls.entries = append(acls.entries, pathEntries...)
		}
	}
	return &store.Store{Calendars: calendarSharedThrough(t, acls.entries), Events: repo, ACLEntries: acls}, repo
}

// calendarSharedThrough is calendar 1, user 1's, as GetAccessible returns it
// to each principal the ACEs entries name.
func calendarSharedThrough(t *testing.T, entries []store.ACLEntry) *fakeCalendarRepo {
	t.Helper()
	cal := store.Calendar{ID: 1, UserID: 1, Name: "Shared"}
	byUser := map[int64]map[string][]store.ACLEntry{}
	for _, entry := range entries {
		var userID int64
		if _, err := fmt.Sscanf(entry.PrincipalHref, "/dav/principals/%d/", &userID); err != nil {
			t.Fatalf("calendarSharedThrough does not model the principal %q", entry.PrincipalHref)
		}
		if byUser[userID] == nil {
			byUser[userID] = map[string][]store.ACLEntry{}
		}
		byUser[userID][entry.ResourcePath] = append(byUser[userID][entry.ResourcePath], entry)
	}
	repo := &fakeCalendarRepo{
		calendars:  map[int64]*store.CalendarAccess{1: {Calendar: cal}},
		accessible: map[int64]*store.CalendarAccess{1: {Calendar: cal, PrivilegesResolved: true, Privileges: store.FullCalendarPrivileges()}},
	}
	for userID, userEntries := range byUser {
		if access := accessibleAs(t, cal, userEntries); access != nil {
			repo.accessible[userID] = access
		}
	}
	return repo
}

func TestEventUpdatesAndDeletesRevealNothingToAPrincipalWhoCannotSeeTheEvent(t *testing.T) {
	for i, tc := range eventWriteAccessCases {
		user := &store.User{ID: eventWriteAccessUserID(i)}
		for _, op := range []string{"update", "delete"} {
			outcomes := tc.update
			if op == "delete" {
				outcomes = tc.delete
			}
			for h, header := range eventWriteAccessHeaders {
				t.Run(tc.principal+"/"+op+"/"+header.name, func(t *testing.T) {
					got := map[string]error{}
					for _, uid := range []string{"stored", "missing"} {
						st, repo := eventWriteAccessStore(t)
						svc := NewService(st, dav.ValidateCalendarObject)
						var err error
						if op == "update" {
							_, _, err = svc.UpdateEvent(context.Background(), user, 1, uid, UpsertInput{
								RawICS: validICS(uid), IfMatch: header.ifMatch, IfNoneMatch: header.ifNoneMatch,
							})
						} else {
							err = svc.DeleteEvent(context.Background(), user, 1, uid, header.ifMatch, header.ifNoneMatch)
						}
						got[uid] = err
						if err != nil {
							if stored, ok := repo.events["1:stored"]; !ok || stored.ETag != "etag" || len(repo.events) != 1 {
								t.Errorf("%s of %q answered %v but changed the calendar: %#v", op, uid, err, repo.events)
							}
						}
					}
					assertEventWriteOutcome(t, tc, outcomes[h], got)
				})
			}
		}
	}
}

func assertEventWriteOutcome(t *testing.T, tc eventWriteAccessCase, want targetOutcome, got map[string]error) {
	t.Helper()
	if !errors.Is(got["stored"], want.stored) || !errors.Is(got["missing"], want.missing) {
		t.Fatalf("stored UID: %v, missing UID: %v; want %v and %v", got["stored"], got["missing"], want.stored, want.missing)
	}
	if tc.concealed && fmt.Sprint(got["stored"]) != fmt.Sprint(got["missing"]) {
		t.Fatalf("answers differ by existence: %v for the stored UID, %v for a missing one", got["stored"], got["missing"])
	}
}

// A principal with no access to a calendar reads none of its events, even
// knowing a UID, as an attendee of an invitation does.
func TestGetEventHidesAnEventFromAPrincipalWithNoAccess(t *testing.T) {
	st, _ := eventWriteAccessStore(t)
	svc := NewService(st, dav.ValidateCalendarObject)
	for i, tc := range eventWriteAccessCases {
		user := &store.User{ID: eventWriteAccessUserID(i)}
		switch tc.principal {
		case "no access":
			for _, uid := range []string{"stored", "missing"} {
				if ev, err := svc.GetEvent(context.Background(), user, 1, uid); !errors.Is(err, ErrNotFound) || ev != nil {
					t.Fatalf("GetEvent(%q) = %v, %v; want ErrNotFound", uid, ev != nil, err)
				}
			}
		case "read-only", "event grant":
			if _, err := svc.GetEvent(context.Background(), user, 1, "stored"); err != nil {
				t.Fatalf("GetEvent() by the %s principal error = %v, want the event", tc.principal, err)
			}
		}
	}
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
	// accessible, when set, is what GetAccessible returns to each user,
	// keyed by user ID, in place of the calendar for its owner or a share.
	accessible map[int64]*store.CalendarAccess
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
	if f.accessible != nil {
		if cal, ok := f.accessible[userID]; ok && cal.ID == calendarID {
			copy := *cal
			return &copy, nil
		}
		return nil, nil
	}
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
func (f *fakeEventRepo) ListChangesAfter(ctx context.Context, collectionID int64, after store.SyncCursor, removals bool, limit int) (store.SyncChanges[store.Event], error) {
	return store.SyncChanges[store.Event]{Last: after}, nil
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
			_, err := svc.CreateEvent(context.Background(), user, 1, UpsertInput{RawICS: tt.ics})
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
	if _, err := svc.CreateEvent(context.Background(), user, 1, UpsertInput{RawICS: "\xef\xbb\xbf" + validICS("bom")}); err != nil {
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

	_, err := todoOnly.CreateEvent(context.Background(), user, 1, UpsertInput{RawICS: validICS("event")})
	if !errors.Is(err, ErrBadRequest) || !strings.Contains(err.Error(), "supported-calendar-component") {
		t.Fatalf("CreateEvent() into a VTODO collection error = %v, want supported-calendar-component", err)
	}
}

// A structured event's timezone and recurrence until are refused when the
// server cannot read them, rather than read as UTC or as no end: an unknown
// zone would move the event by its offset from UTC, and an unreadable until
// would leave the series without an end.
func TestStructuredEventsRefuseAZoneOrUntilTheServerCannotRead(t *testing.T) {
	user := &store.User{ID: 1}
	timed := func(timezone string, recurrence *StructuredRecurrence) *StructuredInput {
		return &StructuredInput{Summary: "x", DTStart: "2026-03-20T10:00", DTEnd: "2026-03-20T11:00", Timezone: timezone, Recurrence: recurrence}
	}
	daily := func(until string, count int) *StructuredRecurrence {
		return &StructuredRecurrence{Frequency: "DAILY", Until: until, Count: count}
	}
	tests := map[string]struct {
		input *StructuredInput
		// zone is an IANA zone the case needs the host's time zone database for.
		zone string
		want error
		// stored is a line the stored object must hold.
		stored string
	}{
		"zone the database lacks":             {input: timed("Nowhere/Zone", nil), want: ErrBadRequest},
		"Windows zone name":                   {input: timed("Eastern Standard Time", nil), want: ErrBadRequest},
		"the server's own zone":               {input: timed("Local", nil), want: ErrBadRequest},
		"zone with a quote":                   {input: timed(`America/"Chicago`, nil), want: ErrBadRequest},
		"unknown zone on all-day event":       {input: &StructuredInput{Summary: "x", DTStart: "2026-03-20", DTEnd: "2026-03-20", AllDay: true, Timezone: "Nowhere/Zone"}, want: ErrBadRequest},
		"no zone":                             {input: timed("", nil), stored: "DTSTART:20260320T100000Z"},
		"blank zone":                          {input: timed("  ", nil), stored: "DTSTART:20260320T100000Z"},
		"UTC":                                 {input: timed("UTC", nil), stored: "DTSTART:20260320T100000Z"},
		"IANA zone":                           {input: timed("America/Chicago", nil), zone: "America/Chicago", stored: "DTSTART;TZID=America/Chicago:20260320T100000"},
		"until not a date":                    {input: timed("", daily("June", 0)), want: ErrBadRequest},
		"until in basic format":               {input: timed("", daily("20260630", 0)), want: ErrBadRequest},
		"until with a time":                   {input: timed("", daily("2026-06-30T10:00", 0)), want: ErrBadRequest},
		"until on no calendar day":            {input: timed("", daily("2026-02-30", 0)), want: ErrBadRequest},
		"malformed until beside count":        {input: timed("", daily("June", 3)), want: ErrBadRequest},
		"malformed until without a frequency": {input: timed("", &StructuredRecurrence{Until: "June"}), want: ErrBadRequest},
		"until":                               {input: timed("", daily("2026-06-30", 0)), stored: "RRULE:FREQ=DAILY;UNTIL=20260630T235959Z"},
		"no until":                            {input: timed("", daily("", 0)), stored: "RRULE:FREQ=DAILY\r\n"},
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
			created, err := svc.CreateEvent(context.Background(), user, 1, UpsertInput{Structured: tt.input})
			if !errors.Is(err, tt.want) {
				t.Fatalf("CreateEvent() error = %v, want %v", err, tt.want)
			}
			if tt.want != nil {
				if len(repo.events) != 0 {
					t.Fatalf("stored %d events after a refusal, want none", len(repo.events))
				}
				repo.events["1:stored"] = store.Event{CalendarID: 1, UID: "stored", ResourceName: "stored", RawICAL: validICS("stored"), ETag: "etag"}
				if _, _, err := svc.UpdateEvent(context.Background(), user, 1, "stored", UpsertInput{Structured: tt.input}); !errors.Is(err, tt.want) {
					t.Fatalf("UpdateEvent() error = %v, want %v", err, tt.want)
				}
				if repo.events["1:stored"].ETag != "etag" {
					t.Fatal("UpdateEvent() rewrote the stored event after a refusal")
				}
				return
			}
			if !strings.Contains(created.RawICAL, tt.stored) {
				t.Fatalf("stored\n%s\nwant it to hold %q", created.RawICAL, tt.stored)
			}
		})
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
	_, err := svc.CreateEvent(context.Background(), user, 1, UpsertInput{
		RawICS: validICS(strings.Repeat("u", store.MaxIdentifierOctets+1)),
	})
	if !errors.Is(err, ErrBadRequest) || !strings.Contains(err.Error(), fmt.Sprintf("uid must be at most %d octets", store.MaxIdentifierOctets)) {
		t.Fatalf("CreateEvent() error = %v, want ErrBadRequest naming the UID bound", err)
	}
	if len(repo.events) != 0 {
		t.Fatal("the event was stored")
	}
	if _, err := svc.CreateEvent(context.Background(), user, 1, UpsertInput{
		RawICS: validICS(strings.Repeat("u", store.MaxIdentifierOctets)),
	}); err != nil {
		t.Fatalf("UID at the limit: err=%v", err)
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
			ev, err := svc.CreateEvent(context.Background(), user, 1, UpsertInput{RawICS: tt.ics})
			if err != nil || ev == nil || ev.UID != appleEventUID {
				t.Fatalf("CreateEvent() ev=%+v err=%v", ev, err)
			}
			ev, created, err := svc.UpdateEvent(context.Background(), user, 1, appleEventUID, UpsertInput{RawICS: tt.ics})
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
			_, err := svc.CreateEvent(context.Background(), user, 1, UpsertInput{RawICS: tt.ics})
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
			ev, err := svc.CreateEvent(context.Background(), user, 1, UpsertInput{RawICS: tt.ics})
			if err != nil || ev == nil || ev.UID != appleEventUID {
				t.Fatalf("CreateEvent() ev=%+v err=%v", ev, err)
			}
			if _, _, err := svc.UpdateEvent(context.Background(), user, 1, appleEventUID, UpsertInput{RawICS: tt.ics}); err != nil {
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
			_, err := svc.CreateEvent(context.Background(), user, 1, UpsertInput{RawICS: tt.ics})
			if !errors.Is(err, tt.want) {
				t.Fatalf("CreateEvent() error = %v, want %v", err, tt.want)
			}
			if len(repo.events) != 0 {
				t.Fatal("the event was stored")
			}
		})
	}
}
