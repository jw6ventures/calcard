package events

import (
	"context"
	"crypto/sha256"
	"fmt"
	"strings"
	"testing"

	"github.com/jw6ventures/calcard/internal/store"
)

// rawICSWithEvent wraps one VEVENT's lines, each already carrying its own
// line ending, in the minimal VCALENDAR a raw_ics create or update sends.
func rawICSWithEvent(uid string, lines ...string) string {
	return "BEGIN:VCALENDAR\r\nVERSION:2.0\r\nPRODID:-//Test//EN\r\nBEGIN:VEVENT\r\nUID:" + uid +
		"\r\nDTSTAMP:20260101T000000Z\r\n" + strings.Join(lines, "\r\n") + "\r\nEND:VEVENT\r\nEND:VCALENDAR\r\n"
}

// rawICSWithZoneAndEvent is rawICSWithEvent with a fixed-offset VTIMEZONE of
// the object's own ahead of the VEVENT, so a case naming a non-UTC zone
// exercises only the trim, not the unrelated host-generated definition a
// name with no VTIMEZONE of its own gets.
func rawICSWithZoneAndEvent(tzid, uid string, lines ...string) string {
	zone := "BEGIN:VTIMEZONE\r\nTZID:" + tzid + "\r\nBEGIN:STANDARD\r\nDTSTART:19700101T000000\r\n" +
		"TZOFFSETFROM:+0000\r\nTZOFFSETTO:+0000\r\nTZNAME:X\r\nEND:STANDARD\r\nEND:VTIMEZONE\r\n"
	return "BEGIN:VCALENDAR\r\nVERSION:2.0\r\nPRODID:-//Test//EN\r\n" + zone + "BEGIN:VEVENT\r\nUID:" + uid +
		"\r\nDTSTAMP:20260101T000000Z\r\n" + strings.Join(lines, "\r\n") + "\r\nEND:VEVENT\r\nEND:VCALENDAR\r\n"
}

func sha256ETag(body string) string {
	return fmt.Sprintf("%x", sha256.Sum256([]byte(body)))
}

// REST create (raw_ics) goes through the same validator, and the same write
// path, PUT does: a TZID naming UTC in any case is rewritten to that value's
// UTC form, and an untrimmed one is trimmed. The stored body and the
// returned ETag must be the one the write actually read back, not the one
// the request sent.
func TestRawICSCreateRewritesTZIDs(t *testing.T) {
	user := &store.User{ID: 1}
	tests := []struct {
		name string
		sent string
		want string
	}{
		{"UTC uppercase",
			rawICSWithEvent("utc-upper", "DTSTART;TZID=UTC:20260701T100000", "DTEND;TZID=UTC:20260701T110000"),
			rawICSWithEvent("utc-upper", "DTSTART:20260701T100000Z", "DTEND:20260701T110000Z")},
		{"utc lowercase",
			rawICSWithEvent("utc-lower", "DTSTART;TZID=utc:20260701T100000", "DTEND:20260701T110000Z"),
			rawICSWithEvent("utc-lower", "DTSTART:20260701T100000Z", "DTEND:20260701T110000Z")},
		{"padded non-UTC zone",
			rawICSWithZoneAndEvent("Custom/Zone", "padded-zone", "DTSTART;TZID= Custom/Zone :20260701T100000", "DTEND;TZID=Custom/Zone:20260701T110000"),
			rawICSWithZoneAndEvent("Custom/Zone", "padded-zone", "DTSTART;TZID=Custom/Zone:20260701T100000", "DTEND;TZID=Custom/Zone:20260701T110000")},
		{"quoted non-UTC zone with white space around the quotes",
			rawICSWithZoneAndEvent("Custom/Zone", "quoted-zone", "DTSTART;TZID= \"Custom/Zone\" ;X-A=1:20260701T100000", "DTEND;TZID=Custom/Zone:20260701T110000"),
			rawICSWithZoneAndEvent("Custom/Zone", "quoted-zone", "DTSTART;TZID=\"Custom/Zone\";X-A=1:20260701T100000", "DTEND;TZID=Custom/Zone:20260701T110000")},
		{"EXDATE list naming UTC",
			rawICSWithEvent("exdate-utc", "DTSTART;TZID=UTC:20260701T100000", "RRULE:FREQ=DAILY;COUNT=3",
				"EXDATE;TZID=UTC:20260702T100000,20260703T100000", "DTEND:20260701T110000Z"),
			rawICSWithEvent("exdate-utc", "DTSTART:20260701T100000Z", "RRULE:FREQ=DAILY;COUNT=3",
				"EXDATE:20260702T100000Z,20260703T100000Z", "DTEND:20260701T110000Z")},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			repo := &fakeEventRepo{events: map[string]store.Event{}}
			svc := newServiceWithRepos(true, repo)
			ev, err := svc.CreateEvent(context.Background(), user, 1, UpsertInput{RawICS: tt.sent})
			if err != nil {
				t.Fatalf("CreateEvent() error = %v", err)
			}
			if ev.RawICAL != tt.want {
				t.Errorf("stored\n%q\nwant\n%q", ev.RawICAL, tt.want)
			}
			if want := sha256ETag(tt.want); ev.ETag != want {
				t.Errorf("ETag = %q, want the stored body's %q", ev.ETag, want)
			}
			got, err := svc.GetEvent(context.Background(), user, 1, ev.UID)
			if err != nil {
				t.Fatalf("GetEvent() error = %v", err)
			}
			if got.RawICAL != tt.want || got.ETag != ev.ETag {
				t.Errorf("GetEvent() = %q (%s), want %q (%s)", got.RawICAL, got.ETag, tt.want, ev.ETag)
			}
		})
	}
}

// REST update (raw_ics) rewrites the same way a create does, for a stored
// event whose own body carries what the rewrite fixes -- a row the v1.2.0
// repair reported, or one an earlier release stored.
func TestRawICSUpdateRewritesTZIDs(t *testing.T) {
	user := &store.User{ID: 1}
	sent := rawICSWithEvent("updated", "DTSTART;TZID=\" UTC\":20260701T100000", "DTEND;TZID=UTC:20260701T110000")
	repo := &fakeEventRepo{events: map[string]store.Event{
		"1:updated": {CalendarID: 1, UID: "updated", ResourceName: "updated", RawICAL: rawICSWithEvent("updated", "DTSTART:20260601T100000Z", "DTEND:20260601T110000Z"), ETag: "etag-old"},
	}}
	svc := newServiceWithRepos(true, repo)
	ev, _, err := svc.UpdateEvent(context.Background(), user, 1, "updated", UpsertInput{RawICS: sent})
	if err != nil {
		t.Fatalf("UpdateEvent() error = %v", err)
	}
	want := rawICSWithEvent("updated", "DTSTART:20260701T100000Z", "DTEND:20260701T110000Z")
	if ev.RawICAL != want {
		t.Errorf("stored\n%q\nwant\n%q", ev.RawICAL, want)
	}
	if wantETag := sha256ETag(want); ev.ETag != wantETag {
		t.Errorf("ETag = %q, want the stored body's %q", ev.ETag, wantETag)
	}
}
