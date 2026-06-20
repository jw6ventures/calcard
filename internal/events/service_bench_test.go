package events

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/jw6ventures/calcard/internal/store"
)

// eventScales is the event-count ladder shared by the ListEvents benchmarks. It
// brackets realistic calendars (hundreds) through pathological ones (tens of
// thousands) so the O(events) work in ListEvents shows up clearly.
var eventScales = []int{100, 1000, 10000, 50000}

// benchEvents builds n events for calendar 1, keyed the same way fakeEventRepo
// expects. Each carries a valid ICS body so the rows resemble production data.
func benchEvents(n int) map[string]store.Event {
	events := make(map[string]store.Event, n)
	for i := 0; i < n; i++ {
		uid := fmt.Sprintf("event-%d", i)
		events[key(1, uid)] = store.Event{
			CalendarID:   1,
			UID:          uid,
			ResourceName: uid + ".ics",
			RawICAL:      validICS(uid),
		}
	}
	return events
}

// BenchmarkListEvents_Owner measures the owner fast path: every event short-
// circuits on cal.UserID == user.ID, so this isolates the per-event loop and the
// ACL prefetch overhead from actual ACL matching.
func BenchmarkListEvents_Owner(b *testing.B) {
	user := &store.User{ID: 1}
	for _, n := range eventScales {
		svc := NewService(&store.Store{
			Calendars: &fakeCalendarRepo{calendars: map[int64]*store.CalendarAccess{
				1: {Calendar: store.Calendar{ID: 1, UserID: 1, Name: "Work"}},
			}},
			Events:     &fakeEventRepo{events: benchEvents(n)},
			ACLEntries: &fakeACLRepo{},
		})
		b.Run(fmt.Sprintf("events=%d", n), func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				if _, err := svc.ListEvents(context.Background(), user, 1, store.EventFilter{}); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

// BenchmarkListEvents_Shared is the realistic worst case: a non-owner with only
// a collection-level grant, so every event falls through to the ACL decision
// path. This is the scaling behavior the ACL prefetch work targeted.
func BenchmarkListEvents_Shared(b *testing.B) {
	user := &store.User{ID: 2}
	for _, n := range eventScales {
		svc := newSharedListService(benchEvents(n))
		b.Run(fmt.Sprintf("events=%d", n), func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				if _, err := svc.ListEvents(context.Background(), user, 1, store.EventFilter{}); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

// BenchmarkListEvents_Paginated covers a small page over a collection-level
// share. Because visibility is uniform, ListEvents now skips per-event ACL
// filtering and pushes LIMIT/OFFSET into the store, so the per-event allocation
// cost seen in Shared/events=50000 disappears. (The residual cost here is the
// in-memory fake materializing every row from its map; a real SQL store returns
// only the page.)
func BenchmarkListEvents_Paginated(b *testing.B) {
	user := &store.User{ID: 2}
	const n = 50000
	svc := newSharedListService(benchEvents(n))
	filter := store.EventFilter{Limit: 50}
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		page, err := svc.ListEvents(context.Background(), user, 1, filter)
		if err != nil {
			b.Fatal(err)
		}
		if len(page) != 50 {
			b.Fatalf("expected 50 events, got %d", len(page))
		}
	}
}

// newSharedListService wires a calendar shared with user 2 via a collection-level
// read grant, the setup used by the shared/paginated ListEvents benchmarks.
func newSharedListService(events map[string]store.Event) *Service {
	return NewService(&store.Store{
		Calendars: &fakeCalendarRepo{calendars: map[int64]*store.CalendarAccess{
			1: {
				Calendar:           store.Calendar{ID: 1, UserID: 1, Name: "Shared"},
				Shared:             true,
				PrivilegesResolved: true,
				Privileges:         store.CalendarPrivileges{Read: true},
			},
		}},
		Events: &fakeEventRepo{events: events},
		ACLEntries: &fakeACLRepo{entries: []store.ACLEntry{
			{ResourcePath: "/dav/calendars/1", PrincipalHref: "/dav/principals/2/", IsGrant: true, Privilege: "read"},
		}},
	})
}

// BenchmarkPrefetchCalendarACLEntries isolates the prefetch pass, which builds a
// relevant-paths set sized to all events (two paths each) and scans the user's
// principal entries. It scales event count against per-principal entry count.
func BenchmarkPrefetchCalendarACLEntries(b *testing.B) {
	user := &store.User{ID: 2}
	for _, n := range []int{100, 1000, 10000} {
		for _, m := range []int{8, 128} {
			events := make([]store.Event, 0, n)
			for i := 0; i < n; i++ {
				uid := fmt.Sprintf("event-%d", i)
				events = append(events, store.Event{CalendarID: 1, UID: uid, ResourceName: uid + ".ics"})
			}
			entries := []store.ACLEntry{
				{ResourcePath: "/dav/calendars/1", PrincipalHref: "/dav/principals/2/", IsGrant: true, Privilege: "read"},
			}
			for j := 0; j < m; j++ {
				entries = append(entries, store.ACLEntry{
					ResourcePath:  fmt.Sprintf("/dav/calendars/%d", 100+j),
					PrincipalHref: "/dav/principals/2/",
					IsGrant:       true,
					Privilege:     "read",
				})
			}
			svc := NewService(&store.Store{ACLEntries: &fakeACLRepo{entries: entries}})
			b.Run(fmt.Sprintf("events=%d/entries=%d", n, m), func(b *testing.B) {
				b.ReportAllocs()
				for i := 0; i < b.N; i++ {
					if _, err := svc.prefetchCalendarACLEntries(context.Background(), user, 1, events); err != nil {
						b.Fatal(err)
					}
				}
			})
		}
	}
}

// benchICSWithAttendees builds a valid VEVENT carrying the given number of
// ATTENDEE lines, exercising the multiple line-by-line passes in validation.
func benchICSWithAttendees(uid string, attendees int) string {
	var b strings.Builder
	b.WriteString("BEGIN:VCALENDAR\r\nVERSION:2.0\r\nBEGIN:VEVENT\r\nUID:")
	b.WriteString(uid)
	b.WriteString("\r\nSUMMARY:Planning\r\nDTSTART:20260320T100000Z\r\nDTEND:20260320T110000Z\r\n")
	for i := 0; i < attendees; i++ {
		b.WriteString("ATTENDEE:mailto:user@example.com\r\n")
	}
	b.WriteString("END:VEVENT\r\nEND:VCALENDAR\r\n")
	return b.String()
}

// BenchmarkValidateStrictICalendar measures the per-event write-path validation,
// which makes several full passes over the unfolded lines. It scales attendee
// count and includes a recurrence near the instance cap.
func BenchmarkValidateStrictICalendar(b *testing.B) {
	for _, attendees := range []int{10, 100, 1000} {
		body := benchICSWithAttendees("bench", attendees)
		b.Run(fmt.Sprintf("attendees=%d", attendees), func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				if err := validateStrictICalendar(body); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
	b.Run("recurrence", func(b *testing.B) {
		body := "BEGIN:VCALENDAR\r\nVERSION:2.0\r\nBEGIN:VEVENT\r\nUID:bench\r\n" +
			"SUMMARY:Planning\r\nDTSTART:20260320T100000Z\r\nDTEND:20260320T110000Z\r\n" +
			"RRULE:FREQ=DAILY;COUNT=1999\r\nEND:VEVENT\r\nEND:VCALENDAR\r\n"
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			if err := validateStrictICalendar(body); err != nil {
				b.Fatal(err)
			}
		}
	})
}

// BenchmarkNormalizeEventPayload covers the full save-time normalization for both
// supported inputs: a raw ICS body and a structured payload built from scratch.
func BenchmarkNormalizeEventPayload(b *testing.B) {
	svc := &Service{}
	b.Run("raw_ics", func(b *testing.B) {
		input := UpsertInput{RawICS: benchICSWithAttendees("bench", 100), ContentType: "text/calendar"}
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			if _, _, err := svc.normalizeEventPayload(input, ""); err != nil {
				b.Fatal(err)
			}
		}
	})
	b.Run("structured", func(b *testing.B) {
		input := UpsertInput{Structured: &StructuredInput{
			Summary:     "Planning",
			DTStart:     "2026-03-20T10:00",
			DTEnd:       "2026-03-20T11:00",
			Location:    "HQ",
			Description: "Quarterly planning",
		}}
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			if _, _, err := svc.normalizeEventPayload(input, ""); err != nil {
				b.Fatal(err)
			}
		}
	})
}
