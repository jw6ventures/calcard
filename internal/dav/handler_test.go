package dav

import (
	"context"
	"encoding/xml"
	"fmt"
	"strings"
	"testing"
	"time"

	"gitea.jw6.us/james/calcard/internal/store"
)

func TestParseResourcePathAcceptsAbsoluteHref(t *testing.T) {
	id, uid, ok := parseResourcePath("https://cal.example.com/dav/calendars/42/test-event.ics", "/dav/calendars")
	if !ok {
		t.Fatalf("expected absolute href to be parsed")
	}
	if id != 42 {
		t.Fatalf("expected id 42, got %d", id)
	}
	if uid != "test-event" {
		t.Fatalf("expected uid test-event, got %q", uid)
	}
}

func TestCalendarMultiGetHandlesAbsoluteHref(t *testing.T) {
	repo := &fakeEventRepo{
		events: map[string]*store.Event{
			"2:test-event": {
				CalendarID: 2,
				UID:        "test-event",
				RawICAL:    "BEGIN:VCALENDAR\r\nBEGIN:VEVENT\r\nUID:test-event\r\nEND:VEVENT\r\nEND:VCALENDAR\r\n",
				ETag:       "abc123",
			},
		},
	}
	h := &Handler{store: &store.Store{Events: repo, DeletedResources: &fakeDeletedResourceRepo{}}}

	hrefs := []string{"https://cal.example.com/dav/calendars/2/test-event.ics"}
	responses, err := h.calendarMultiGet(context.Background(), 2, hrefs, "/dav/calendars/2/")
	if err != nil {
		t.Fatalf("calendarMultiGet returned error: %v", err)
	}
	if len(responses) != 1 {
		t.Fatalf("expected 1 response, got %d", len(responses))
	}
	if responses[0].Href != "/dav/calendars/2/test-event.ics" {
		t.Fatalf("unexpected href %q", responses[0].Href)
	}
}

func TestCalendarMultiGetHandlesRelativeHref(t *testing.T) {
	repo := &fakeEventRepo{
		events: map[string]*store.Event{
			"2:test-event": {
				CalendarID: 2,
				UID:        "test-event",
				RawICAL:    "BEGIN:VCALENDAR\r\nBEGIN:VEVENT\r\nUID:test-event\r\nEND:VEVENT\r\nEND:VCALENDAR\r\n",
				ETag:       "abc123",
			},
		},
	}
	h := &Handler{store: &store.Store{Events: repo, DeletedResources: &fakeDeletedResourceRepo{}}}

	hrefs := []string{"test-event.ics"}
	responses, err := h.calendarMultiGet(context.Background(), 2, hrefs, "/dav/calendars/2/")
	if err != nil {
		t.Fatalf("calendarMultiGet returned error: %v", err)
	}
	if len(responses) != 1 {
		t.Fatalf("expected 1 response, got %d", len(responses))
	}
	if responses[0].Href != "/dav/calendars/2/test-event.ics" {
		t.Fatalf("unexpected href %q", responses[0].Href)
	}
}

func TestCalendarReportSyncCollectionReturnsToken(t *testing.T) {
	now := store.Now()
	repo := &fakeEventRepo{
		events: map[string]*store.Event{
			"2:test-event": {
				CalendarID:   2,
				UID:          "test-event",
				RawICAL:      "BEGIN:VCALENDAR\r\nBEGIN:VEVENT\r\nUID:test-event\r\nEND:VEVENT\r\nEND:VCALENDAR\r\n",
				ETag:         "abc123",
				LastModified: now,
			},
		},
	}
	h := &Handler{store: &store.Store{Events: repo, DeletedResources: &fakeDeletedResourceRepo{}}}

	report := reportRequest{XMLName: xml.Name{Local: "sync-collection"}}
	cal := &store.CalendarAccess{Calendar: store.Calendar{ID: 2, Name: "Test", CTag: 1, UpdatedAt: now}, Editor: true}
	responses, token, err := h.calendarReportResponses(context.Background(), cal, "/dav/principals/1/", "/dav/calendars/2/", report)
	if err != nil {
		t.Fatalf("calendarReportResponses returned error: %v", err)
	}
	if token == "" {
		t.Fatalf("expected sync token, got empty string")
	}
	if len(responses) != 2 {
		t.Fatalf("expected 2 responses, got %d", len(responses))
	}
	if responses[0].Href != "/dav/calendars/2/" {
		t.Fatalf("expected collection response first, got %s", responses[0].Href)
	}
}

func TestCalendarSyncCollectionIncludesDeletedResources(t *testing.T) {
	now := store.Now()
	repo := &fakeEventRepo{
		events: map[string]*store.Event{},
	}
	deletedRepo := &fakeDeletedResourceRepo{
		deleted: []store.DeletedResource{
			{ID: 1, ResourceType: "event", CollectionID: 2, UID: "deleted-event", DeletedAt: now},
		},
	}
	h := &Handler{store: &store.Store{Events: repo, DeletedResources: deletedRepo}}

	report := reportRequest{
		XMLName:   xml.Name{Local: "sync-collection"},
		SyncToken: buildSyncToken("cal", 2, now.Add(-time.Hour)),
	}
	cal := &store.CalendarAccess{Calendar: store.Calendar{ID: 2, Name: "Test", CTag: 2, UpdatedAt: now}, Editor: true}
	responses, _, err := h.calendarReportResponses(context.Background(), cal, "/dav/principals/1/", "/dav/calendars/2/", report)
	if err != nil {
		t.Fatalf("calendarReportResponses returned error: %v", err)
	}
	// Should have collection response + deleted event
	if len(responses) != 2 {
		t.Fatalf("expected 2 responses, got %d", len(responses))
	}
	// Check that deleted event has 404 status
	found := false
	for _, r := range responses {
		if strings.Contains(r.Href, "deleted-event") {
			if r.Status != "HTTP/1.1 404 Not Found" {
				t.Errorf("expected 404 status for deleted event, got %q", r.Status)
			}
			found = true
		}
	}
	if !found {
		t.Error("deleted event response not found")
	}
}

type fakeEventRepo struct {
	events map[string]*store.Event
}

func (f *fakeEventRepo) key(calendarID int64, uid string) string {
	return fmt.Sprintf("%d:%s", calendarID, uid)
}

func (f *fakeEventRepo) Upsert(ctx context.Context, event store.Event) (*store.Event, error) {
	return nil, nil
}

func (f *fakeEventRepo) DeleteByUID(ctx context.Context, calendarID int64, uid string) error {
	return nil
}

func (f *fakeEventRepo) GetByUID(ctx context.Context, calendarID int64, uid string) (*store.Event, error) {
	if ev, ok := f.events[f.key(calendarID, uid)]; ok {
		copy := *ev
		return &copy, nil
	}
	return nil, nil
}

func (f *fakeEventRepo) ListForCalendar(ctx context.Context, calendarID int64) ([]store.Event, error) {
	var result []store.Event
	for _, ev := range f.events {
		if ev.CalendarID != calendarID {
			continue
		}
		copy := *ev
		result = append(result, copy)
	}
	return result, nil
}

func (f *fakeEventRepo) ListForCalendarPaginated(ctx context.Context, calendarID int64, limit, offset int) (*store.PaginatedResult[store.Event], error) {
	events, _ := f.ListForCalendar(ctx, calendarID)
	return &store.PaginatedResult[store.Event]{
		Items:      events,
		TotalCount: len(events),
		Limit:      limit,
		Offset:     offset,
	}, nil
}

func (f *fakeEventRepo) ListByUIDs(ctx context.Context, calendarID int64, uids []string) ([]store.Event, error) {
	return nil, nil
}

func (f *fakeEventRepo) ListModifiedSince(ctx context.Context, calendarID int64, since time.Time) ([]store.Event, error) {
	var result []store.Event
	for _, ev := range f.events {
		if ev.CalendarID != calendarID {
			continue
		}
		if ev.LastModified.After(since) {
			copy := *ev
			result = append(result, copy)
		}
	}
	return result, nil
}

func (f *fakeEventRepo) ListRecentByUser(ctx context.Context, userID int64, limit int) ([]store.Event, error) {
	return nil, nil
}

func (f *fakeEventRepo) MaxLastModified(ctx context.Context, calendarID int64) (time.Time, error) {
	var max time.Time
	for _, ev := range f.events {
		if ev.CalendarID != calendarID {
			continue
		}
		if ev.LastModified.After(max) {
			max = ev.LastModified
		}
	}
	return max, nil
}

type fakeDeletedResourceRepo struct {
	deleted []store.DeletedResource
}

func (f *fakeDeletedResourceRepo) ListDeletedSince(ctx context.Context, resourceType string, collectionID int64, since time.Time) ([]store.DeletedResource, error) {
	var result []store.DeletedResource
	for _, d := range f.deleted {
		if d.ResourceType == resourceType && d.CollectionID == collectionID && d.DeletedAt.After(since) {
			result = append(result, d)
		}
	}
	return result, nil
}

func (f *fakeDeletedResourceRepo) Cleanup(ctx context.Context, olderThan time.Duration) (int64, error) {
	return 0, nil
}

func TestCalendarDataUseCDATA(t *testing.T) {
	ps := etagProp("abc123", "BEGIN:VCALENDAR\r\nEND:VCALENDAR\r\n", true)
	resp := response{Href: "/test.ics", Propstat: []propstat{ps}}
	payload := multistatus{
		XmlnsD:   "DAV:",
		XmlnsC:   "urn:ietf:params:xml:ns:caldav",
		XmlnsA:   "urn:ietf:params:xml:ns:carddav",
		Response: []response{resp},
	}
	out, err := xml.Marshal(payload)
	if err != nil {
		t.Fatalf("failed to marshal: %v", err)
	}
	xmlStr := string(out)
	// Check that calendar data is wrapped in CDATA and contains raw CRLF
	if !strings.Contains(xmlStr, "<![CDATA[BEGIN:VCALENDAR") {
		t.Errorf("expected CDATA wrapper, got: %s", xmlStr)
	}
	// Check that CRLF is not escaped
	if strings.Contains(xmlStr, "&#xD;") || strings.Contains(xmlStr, "&#xA;") {
		t.Errorf("CRLF should not be escaped, got: %s", xmlStr)
	}
}

func TestReportRequestParsesDifferentNamespacePrefixes(t *testing.T) {
	tests := []struct {
		name     string
		body     string
		wantType string
		wantHref string
	}{
		{
			name: "lowercase d prefix",
			body: `<?xml version="1.0" encoding="UTF-8"?>
<c:calendar-multiget xmlns:d="DAV:" xmlns:c="urn:ietf:params:xml:ns:caldav">
  <d:href>/dav/calendars/1/event1.ics</d:href>
</c:calendar-multiget>`,
			wantType: "calendar-multiget",
			wantHref: "/dav/calendars/1/event1.ics",
		},
		{
			name: "uppercase D prefix (Thunderbird style)",
			body: `<?xml version="1.0" encoding="UTF-8"?>
<C:calendar-multiget xmlns:D="DAV:" xmlns:C="urn:ietf:params:xml:ns:caldav">
  <D:href>/dav/calendars/1/event2.ics</D:href>
</C:calendar-multiget>`,
			wantType: "calendar-multiget",
			wantHref: "/dav/calendars/1/event2.ics",
		},
		{
			name: "sync-collection with sync-token",
			body: `<?xml version="1.0" encoding="UTF-8"?>
<D:sync-collection xmlns:D="DAV:">
  <D:sync-token>urn:calcard-sync:cal:1:0</D:sync-token>
</D:sync-collection>`,
			wantType: "sync-collection",
			wantHref: "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var report reportRequest
			if err := xml.Unmarshal([]byte(tt.body), &report); err != nil {
				t.Fatalf("failed to unmarshal: %v", err)
			}
			if report.XMLName.Local != tt.wantType {
				t.Errorf("XMLName.Local = %q, want %q", report.XMLName.Local, tt.wantType)
			}
			if tt.wantHref != "" {
				if len(report.Hrefs) == 0 {
					t.Fatalf("expected hrefs, got none")
				}
				if report.Hrefs[0] != tt.wantHref {
					t.Errorf("Hrefs[0] = %q, want %q", report.Hrefs[0], tt.wantHref)
				}
			}
		})
	}
}
