package dav

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/jw6ventures/calcard/internal/auth"
	"github.com/jw6ventures/calcard/internal/store"
)

func TestPutSuppliesPrecomputedEventWriteMetadata(t *testing.T) {
	calRepo := &fakeCalendarRepo{accessible: []store.CalendarAccess{{
		Calendar: store.Calendar{ID: 2, UserID: 1, Name: "Work"},
		Editor:   true,
	}}}
	eventRepo := &fakeEventRepo{events: map[string]*store.Event{}}
	h := &DavServer{store: &store.Store{Calendars: calRepo, Events: eventRepo}}

	raw := "BEGIN:VCALENDAR\r\n" +
		"VERSION:2.0\r\n" +
		"BEGIN:VEVENT\r\n" +
		"UID:event\r\n" +
		"SUMMARY:Planning\\, Review\r\n" +
		"DESCRIPTION:Outer description\r\n" +
		"DTSTART:20260720T150000Z\r\n" +
		"DTEND:20260720T160000Z\r\n" +
		"RRULE:FREQ=DAILY;COUNT=3\r\n" +
		"BEGIN:VALARM\r\n" +
		"DESCRIPTION:Nested alarm must not replace event metadata\r\n" +
		"END:VALARM\r\n" +
		"END:VEVENT\r\n" +
		"END:VCALENDAR\r\n"
	req := newCalendarPutRequest("/dav/calendars/2/event.ics", strings.NewReader(raw))
	req = req.WithContext(auth.WithUser(req.Context(), &store.User{ID: 1}))
	rr := httptest.NewRecorder()

	h.Put(rr, req)

	if rr.Code != http.StatusCreated {
		t.Fatalf("PUT status = %d: %s", rr.Code, rr.Body.String())
	}
	event := eventRepo.events[eventRepo.key(2, "event")]
	if event == nil || event.WriteMetadata == nil {
		t.Fatalf("stored event metadata = %#v", event)
	}
	metadata := event.WriteMetadata
	if metadata.Summary == nil || *metadata.Summary != "Planning, Review" {
		t.Fatalf("summary metadata = %#v", metadata.Summary)
	}
	if metadata.Description == nil || *metadata.Description != "Outer description" {
		t.Fatalf("description metadata = %#v", metadata.Description)
	}
	wantStart := time.Date(2026, 7, 20, 15, 0, 0, 0, time.UTC)
	if metadata.DTStart == nil || !metadata.DTStart.Equal(wantStart) {
		t.Fatalf("DTSTART metadata = %#v, want %s", metadata.DTStart, wantStart)
	}
	if metadata.RecurrenceStart == nil || metadata.RecurrenceUntil == nil {
		t.Fatalf("recurrence metadata = start %#v until %#v", metadata.RecurrenceStart, metadata.RecurrenceUntil)
	}
}

func TestCalendarAnalysisUsesMasterEventMetadataAndIgnoresTimezoneRecurrenceLimit(t *testing.T) {
	raw := "BEGIN:VCALENDAR\r\n" +
		"VERSION:2.0\r\n" +
		"BEGIN:VTIMEZONE\r\n" +
		"TZID:Example/Zone\r\n" +
		"BEGIN:STANDARD\r\n" +
		"DTSTART:19700101T000000\r\n" +
		"RRULE:FREQ=YEARLY;COUNT=5000\r\n" +
		"END:STANDARD\r\n" +
		"END:VTIMEZONE\r\n" +
		"BEGIN:VEVENT\r\n" +
		"UID:event\r\n" +
		"RECURRENCE-ID:20260721T150000Z\r\n" +
		"SUMMARY:Override\r\n" +
		"DTSTART:20260721T170000Z\r\n" +
		"END:VEVENT\r\n" +
		"BEGIN:VEVENT\r\n" +
		"UID:event\r\n" +
		"SUMMARY:Master\r\n" +
		"DTSTART:20260720T150000Z\r\n" +
		"RRULE:FREQ=DAILY;COUNT=3\r\n" +
		"END:VEVENT\r\n" +
		"END:VCALENDAR\r\n"

	analysis, err := analyzeICalendar(raw)
	if err != nil {
		t.Fatalf("analyzeICalendar() error = %v", err)
	}
	if analysis.Metadata.Summary == nil || *analysis.Metadata.Summary != "Master" {
		t.Fatalf("summary metadata = %#v, want master event", analysis.Metadata.Summary)
	}
	wantStart := time.Date(2026, 7, 20, 15, 0, 0, 0, time.UTC)
	if analysis.Metadata.DTStart == nil || !analysis.Metadata.DTStart.Equal(wantStart) {
		t.Fatalf("DTSTART metadata = %#v, want %s", analysis.Metadata.DTStart, wantStart)
	}
	if !analysis.HasRRULECount || analysis.MaxRRULECount != 3 {
		t.Fatalf("RRULE count = (%d, %v), want (3, true)", analysis.MaxRRULECount, analysis.HasRRULECount)
	}
}

func TestCalendarAnalysisRejectsMultipleCalendarRoots(t *testing.T) {
	raw := "BEGIN:VCALENDAR\r\nBEGIN:VEVENT\r\nUID:one\r\nEND:VEVENT\r\nEND:VCALENDAR\r\n" +
		"BEGIN:VCALENDAR\r\nBEGIN:VEVENT\r\nUID:two\r\nEND:VEVENT\r\nEND:VCALENDAR\r\n"
	if _, err := analyzeICalendar(raw); err == nil {
		t.Fatal("analyzeICalendar() error = nil for multiple VCALENDAR roots")
	}
}
