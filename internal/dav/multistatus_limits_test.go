package dav

import (
	"bytes"
	"context"
	"encoding/xml"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/jw6ventures/calcard/internal/config"
	"github.com/jw6ventures/calcard/internal/store"
)

func TestWriteBoundedMultiStatusRejectsResponseCountBeforeHeaders(t *testing.T) {
	cfg := &config.Config{}
	cfg.DAV.MaxMultistatusResponses = 1
	cfg.DAV.MaxMultistatusBytes = 1024
	h := NewDavServer(Options{Config: cfg})
	rr := httptest.NewRecorder()

	h.writeBoundedMultiStatus(rr, newMultistatus([]response{{Href: "/one"}, {Href: "/two"}}, ""))

	if rr.Code != http.StatusInsufficientStorage {
		t.Fatalf("status = %d, want 507", rr.Code)
	}
	if strings.Contains(rr.Body.String(), "multistatus") {
		t.Fatalf("hard response limit returned a partial multistatus: %s", rr.Body.String())
	}
}

func TestWriteBoundedMultiStatusRejectsEncodedBytesBeforeHeaders(t *testing.T) {
	cfg := &config.Config{}
	cfg.DAV.MaxMultistatusResponses = 10
	cfg.DAV.MaxMultistatusBytes = 64
	h := NewDavServer(Options{Config: cfg})
	rr := httptest.NewRecorder()

	h.writeBoundedMultiStatus(rr, newMultistatus([]response{{Href: "/a-very-long-resource-name"}}, ""))

	if rr.Code != http.StatusInsufficientStorage {
		t.Fatalf("status = %d, want 507", rr.Code)
	}
	if strings.Contains(rr.Body.String(), "multistatus") {
		t.Fatalf("hard byte limit returned a partial multistatus: %s", rr.Body.String())
	}
}

func TestWriteBoundedMultiStatusEncodedByteBoundary(t *testing.T) {
	payload := newMultistatus([]response{{Href: "/exact-boundary"}}, "")
	var encoded bytes.Buffer
	if err := xml.NewEncoder(&encoded).Encode(payload); err != nil {
		t.Fatalf("Encode() error = %v", err)
	}

	for _, tt := range []struct {
		name       string
		limit      int
		wantStatus int
	}{
		{name: "exact", limit: encoded.Len(), wantStatus: http.StatusMultiStatus},
		{name: "one byte short", limit: encoded.Len() - 1, wantStatus: http.StatusInsufficientStorage},
	} {
		t.Run(tt.name, func(t *testing.T) {
			cfg := &config.Config{}
			cfg.DAV.MaxMultistatusResponses = 1
			cfg.DAV.MaxMultistatusBytes = tt.limit
			h := NewDavServer(Options{Config: cfg})
			rr := httptest.NewRecorder()

			h.writeBoundedMultiStatus(rr, payload)

			if rr.Code != tt.wantStatus {
				t.Fatalf("status = %d, want %d", rr.Code, tt.wantStatus)
			}
		})
	}
}

func TestWriteBoundedMultiStatusDefaultResponseBoundary(t *testing.T) {
	h := NewDavServer(Options{})
	responses := make([]response, defaultMaxMultistatusResponses)
	for i := range responses {
		responses[i].Href = "/r"
	}

	rr := httptest.NewRecorder()
	h.writeBoundedMultiStatus(rr, newMultistatus(responses, ""))
	if rr.Code != http.StatusMultiStatus {
		t.Fatalf("10,000 response status = %d, want 207", rr.Code)
	}

	rr = httptest.NewRecorder()
	responses = append(responses, response{Href: "/overflow"})
	h.writeBoundedMultiStatus(rr, newMultistatus(responses, ""))
	if rr.Code != http.StatusInsufficientStorage {
		t.Fatalf("10,001 response status = %d, want 507", rr.Code)
	}
	if strings.Contains(rr.Body.String(), "multistatus") {
		t.Fatalf("hard response limit returned a partial multistatus: %s", rr.Body.String())
	}
}

func TestCalendarReportStopsBuildingAtOverflowSentinel(t *testing.T) {
	cfg := &config.Config{}
	cfg.DAV.MaxMultistatusResponses = 1
	cfg.DAV.MaxMultistatusBytes = defaultMaxMultistatusBytes
	dead := &fakeDeadPropertyRepo{}
	h := NewDavServer(Options{Config: cfg, Store: &store.Store{DeadProperties: dead}})
	events := []store.Event{
		{UID: "one", ResourceName: "one"},
		{UID: "two", ResourceName: "two"},
		{UID: "three", ResourceName: "three"},
	}

	responses, err := h.calendarResourceReportResponses(context.Background(), &store.User{ID: 1}, "/dav/calendars/1/", events, &reportProp{}, nil)
	if err != nil {
		t.Fatalf("calendarResourceReportResponses() error = %v", err)
	}
	if len(responses) != 2 {
		t.Fatalf("built responses = %d, want max+1 sentinel", len(responses))
	}
	if dead.listCalls != 0 {
		t.Fatalf("overflow response performed dead-property decoration queries: %d", dead.listCalls)
	}
}

func TestCalendarQueryUsesKeysetPageAndStopsAtOverflowSentinel(t *testing.T) {
	cfg := &config.Config{}
	cfg.DAV.MaxMultistatusResponses = 1
	cfg.DAV.MaxMultistatusBytes = defaultMaxMultistatusBytes
	eventRepo := &fakeEventRepo{events: make(map[string]*store.Event)}
	for id := int64(1); id <= 600; id++ {
		uid := fmt.Sprintf("event-%d", id)
		eventRepo.events[uid] = &store.Event{
			ID:           id,
			CalendarID:   1,
			UID:          uid,
			ResourceName: uid,
			RawICAL:      "BEGIN:VCALENDAR\r\nBEGIN:VEVENT\r\nUID:" + uid + "\r\nEND:VEVENT\r\nEND:VCALENDAR\r\n",
		}
	}
	h := NewDavServer(Options{Config: cfg, Store: &store.Store{Events: eventRepo}})
	cal := &store.CalendarAccess{Calendar: store.Calendar{ID: 1, UserID: 1}}

	responses, err := h.calendarQuery(context.Background(), &store.User{ID: 1}, cal, "/dav/calendars/1/", nil, nil, nil)
	if err != nil {
		t.Fatalf("calendarQuery() error = %v", err)
	}
	if len(responses) != 2 {
		t.Fatalf("calendarQuery() responses = %d, want max+1 sentinel", len(responses))
	}
	if eventRepo.pageLookupCount != 1 {
		t.Fatalf("keyset page queries = %d, want 1", eventRepo.pageLookupCount)
	}
	if eventRepo.listForCalendarCalls != 0 {
		t.Fatalf("unbounded calendar queries = %d, want 0", eventRepo.listForCalendarCalls)
	}
}

func TestCalendarQueryContinuesPagingPastNonmatchingRows(t *testing.T) {
	eventRepo := &fakeEventRepo{events: make(map[string]*store.Event)}
	for id := int64(1); id <= 300; id++ {
		uid := fmt.Sprintf("event-%d", id)
		summary := "ordinary"
		if id == 300 {
			summary = "needle"
		}
		eventRepo.events[uid] = &store.Event{
			ID:           id,
			CalendarID:   1,
			UID:          uid,
			ResourceName: uid,
			RawICAL:      "BEGIN:VCALENDAR\r\nBEGIN:VEVENT\r\nUID:" + uid + "\r\nSUMMARY:" + summary + "\r\nEND:VEVENT\r\nEND:VCALENDAR\r\n",
		}
	}
	h := NewDavServer(Options{Store: &store.Store{Events: eventRepo}})
	cal := &store.CalendarAccess{Calendar: store.Calendar{ID: 1, UserID: 1}}
	filter := &calFilter{CompFilter: compFilter{
		Name: "VCALENDAR",
		CompFilter: []compFilter{{
			Name: "VEVENT",
			PropFilter: []propFilter{{
				Name:      "SUMMARY",
				TextMatch: &textMatch{Text: "needle"},
			}},
		}},
	}}

	responses, err := h.calendarQuery(context.Background(), &store.User{ID: 1}, cal, "/dav/calendars/1/", filter, nil, nil)
	if err != nil {
		t.Fatalf("calendarQuery() error = %v", err)
	}
	if len(responses) != 1 || responses[0].Href != "/dav/calendars/1/event-300.ics" {
		t.Fatalf("calendarQuery() responses = %#v, want event-300", responses)
	}
	if eventRepo.pageLookupCount != 2 {
		t.Fatalf("keyset page queries = %d, want 2", eventRepo.pageLookupCount)
	}
	if eventRepo.listForCalendarCalls != 0 {
		t.Fatalf("unbounded calendar queries = %d, want 0", eventRepo.listForCalendarCalls)
	}
}

func TestCalendarPropfindUsesKeysetPageAndStopsAtOverflowSentinel(t *testing.T) {
	cfg := &config.Config{}
	cfg.DAV.MaxMultistatusResponses = 1
	cfg.DAV.MaxMultistatusBytes = defaultMaxMultistatusBytes
	eventRepo := &fakeEventRepo{events: make(map[string]*store.Event)}
	for id := int64(1); id <= 600; id++ {
		uid := fmt.Sprintf("event-%d", id)
		eventRepo.events[uid] = &store.Event{ID: id, CalendarID: 1, UID: uid, ResourceName: uid}
	}
	h := NewDavServer(Options{Config: cfg, Store: &store.Store{Events: eventRepo}})
	cal := &store.CalendarAccess{Calendar: store.Calendar{ID: 1, UserID: 1}}

	responses, err := h.appendCalendarPropfindPages(
		context.Background(),
		&store.User{ID: 1},
		cal,
		"/dav/calendars/1/",
		[]response{{Href: "/dav/calendars/1/"}},
	)
	if err != nil {
		t.Fatalf("appendCalendarPropfindPages() error = %v", err)
	}
	if len(responses) != 2 {
		t.Fatalf("appendCalendarPropfindPages() responses = %d, want max+1 sentinel", len(responses))
	}
	if eventRepo.pageLookupCount != 1 {
		t.Fatalf("keyset page queries = %d, want 1", eventRepo.pageLookupCount)
	}
	if eventRepo.listForCalendarCalls != 0 {
		t.Fatalf("unbounded calendar queries = %d, want 0", eventRepo.listForCalendarCalls)
	}
}
