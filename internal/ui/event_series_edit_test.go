package ui

import (
	"maps"
	"net/http"
	"net/http/httptest"
	"net/url"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/jw6ventures/calcard/internal/config"
	"github.com/jw6ventures/calcard/internal/dav"
	"github.com/jw6ventures/calcard/internal/store"
	"github.com/jw6ventures/calcard/internal/ui/utils"
)

// storedMaster is the content lines of the stored series' master VEVENT.
func storedMaster(t *testing.T, raw string) []string {
	t.Helper()
	_, components, _ := utils.SplitComponents(raw)
	for _, component := range components {
		if utils.RecurrenceIDValue(component) == "" {
			return component
		}
	}
	t.Fatalf("stored object has no master:\n%s", raw)
	return nil
}

// hourPeriods are the free-busy periods of hour-long instances starting at
// each of walls, wall clocks in loc.
func hourPeriods(t *testing.T, loc *time.Location, walls ...string) []string {
	t.Helper()
	var periods []string
	for _, value := range walls {
		wall, err := time.ParseInLocation("2006-01-02T15:04", value, loc)
		if err != nil {
			t.Fatalf("bad wall clock %q: %v", value, err)
		}
		periods = append(periods, wall.UTC().Format("20060102T150405Z")+"/"+wall.Add(time.Hour).UTC().Format("20060102T150405Z"))
	}
	slices.Sort(periods)
	return periods
}

// An occurrence deleted from a series stays deleted when the series is edited
// afterwards. When the edit moves the series -- to another time of day, into
// another zone, or to whole days -- the exclusion moves with the occurrence
// it removed, since an exclusion names an instance by its start.
func TestSeriesEditKeepsDeletedOccurrencesDeleted(t *testing.T) {
	newYork, err := time.LoadLocation("America/New_York")
	if err != nil {
		t.Skipf("America/New_York is not available: %v", err)
	}
	losAngeles, err := time.LoadLocation("America/Los_Angeles")
	if err != nil {
		t.Skipf("America/Los_Angeles is not available: %v", err)
	}
	series := url.Values{
		"dtstart": {"2026-06-15T10:00"}, "dtend": {"2026-06-15T11:00"},
		"recurrence": {"WEEKLY"}, "recurrence_byday": {"MO"},
		"recurrence_end_type": {"after"}, "recurrence_count": {"4"},
	}
	tests := []struct {
		name string
		zone string
		edit url.Values
		// exdate is the exclusion the stored master carries.
		exdate string
		want   func(t *testing.T) []string
	}{
		{
			name:   "renamed",
			zone:   "America/New_York",
			edit:   url.Values{"summary": {"Renamed"}},
			exdate: "EXDATE;TZID=America/New_York:20260622T100000",
			want: func(t *testing.T) []string {
				return hourPeriods(t, newYork, "2026-06-15T10:00", "2026-06-29T10:00", "2026-07-06T10:00")
			},
		},
		{
			name:   "moved an hour later",
			zone:   "America/New_York",
			edit:   url.Values{"dtstart": {"2026-06-15T11:00"}, "dtend": {"2026-06-15T12:00"}},
			exdate: "EXDATE;TZID=America/New_York:20260622T110000",
			want: func(t *testing.T) []string {
				return hourPeriods(t, newYork, "2026-06-15T11:00", "2026-06-29T11:00", "2026-07-06T11:00")
			},
		},
		{
			name:   "moved into another zone",
			zone:   "America/Los_Angeles",
			edit:   url.Values{},
			exdate: "EXDATE;TZID=America/Los_Angeles:20260622T100000",
			want: func(t *testing.T) []string {
				return hourPeriods(t, losAngeles, "2026-06-15T10:00", "2026-06-29T10:00", "2026-07-06T10:00")
			},
		},
		{
			name:   "made all-day",
			zone:   "America/New_York",
			edit:   url.Values{"all_day": {"on"}, "dtstart": {"2026-06-15"}, "dtend": {"2026-06-15"}},
			exdate: "EXDATE;VALUE=DATE:20260622",
			want: func(t *testing.T) []string {
				var periods []string
				for _, day := range []string{"20260615", "20260629", "20260706"} {
					start, _ := time.Parse("20060102", day)
					periods = append(periods, start.Format("20060102T150405Z")+"/"+start.AddDate(0, 0, 1).Format("20060102T150405Z"))
				}
				return periods
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			calRepo := &fakeCalendarRepo{calendars: map[int64]*store.Calendar{1: {ID: 1, UserID: 100, Name: "Work"}}}
			eventRepo := &fakeEventRepoWithUpsert{fakeEventRepo: fakeEventRepo{events: make(map[string]*store.Event)}}
			s := &store.Store{Calendars: calRepo, Events: eventRepo}
			handler := NewHandler(&config.Config{}, s, nil, dav.ValidateCalendarObject, dav.EventInstances)
			user := &store.User{ID: 100}

			w := httptest.NewRecorder()
			handler.CreateEvent(w, eventFormRequest(t, "/calendars/1/events", "", user, "America/New_York", series))
			if location := w.Header().Get("Location"); w.Code != http.StatusFound || !strings.Contains(location, "status=event_created") {
				t.Fatalf("CreateEvent() status = %d location = %s", w.Code, location)
			}
			var uid string
			for _, created := range eventRepo.events {
				uid = created.UID
			}

			w = httptest.NewRecorder()
			handler.DeleteEvent(w, deleteOccurrenceRequest(t, uid, url.Values{
				"recurrence_id": {"2026-06-22T10:00"}, "recurrence_timezone": {"America/New_York"},
			}))
			if location := w.Header().Get("Location"); w.Code != http.StatusFound || !strings.Contains(location, "status=occurrence_deleted") {
				t.Fatalf("DeleteEvent() status = %d location = %s", w.Code, location)
			}

			edit := maps.Clone(series)
			maps.Copy(edit, tt.edit)
			w = httptest.NewRecorder()
			handler.UpdateEvent(w, eventFormRequest(t, "/calendars/1/events/"+uid, uid, user, tt.zone, edit))
			if location := w.Header().Get("Location"); w.Code != http.StatusFound || !strings.Contains(location, "status=event_updated") {
				t.Fatalf("UpdateEvent() status = %d location = %s", w.Code, location)
			}
			stored := eventRepo.events["1:"+uid]
			// The fake store assigns no ID, and the DAV server pages rows by it.
			stored.ID = 1

			if master := storedMaster(t, stored.RawICAL); !slices.Contains(master, tt.exdate) {
				t.Errorf("stored master lacks %q:\n%s", tt.exdate, stored.RawICAL)
			}
			if got, want := davFreeBusyPeriods(t, s, user, "20260601T000000Z", "20260801T000000Z"), tt.want(t); !slices.Equal(got, want) {
				t.Errorf("CalDAV free-busy = %v, want %v, without the deleted occurrence\n%s", got, want, stored.RawICAL)
			}
		})
	}
}

// Turning a series into a single event leaves nothing for its exclusions to
// exclude but the event itself, which one naming its start would remove.
func TestSeriesEditEndingTheSeriesDropsItsExclusions(t *testing.T) {
	newYork, err := time.LoadLocation("America/New_York")
	if err != nil {
		t.Skipf("America/New_York is not available: %v", err)
	}
	calRepo := &fakeCalendarRepo{calendars: map[int64]*store.Calendar{1: {ID: 1, UserID: 100, Name: "Work"}}}
	eventRepo := &fakeEventRepoWithUpsert{fakeEventRepo: fakeEventRepo{events: make(map[string]*store.Event)}}
	s := &store.Store{Calendars: calRepo, Events: eventRepo}
	handler := NewHandler(&config.Config{}, s, nil, dav.ValidateCalendarObject, dav.EventInstances)
	user := &store.User{ID: 100}

	w := httptest.NewRecorder()
	handler.CreateEvent(w, eventFormRequest(t, "/calendars/1/events", "", user, "America/New_York", url.Values{
		"dtstart": {"2026-06-15T10:00"}, "dtend": {"2026-06-15T11:00"},
		"recurrence": {"WEEKLY"}, "recurrence_end_type": {"after"}, "recurrence_count": {"4"},
	}))
	if w.Code != http.StatusFound {
		t.Fatalf("CreateEvent() status = %d", w.Code)
	}
	var uid string
	for _, created := range eventRepo.events {
		uid = created.UID
	}
	w = httptest.NewRecorder()
	handler.DeleteEvent(w, deleteOccurrenceRequest(t, uid, url.Values{
		"recurrence_id": {"2026-06-15T10:00"}, "recurrence_timezone": {"America/New_York"},
	}))
	if location := w.Header().Get("Location"); !strings.Contains(location, "status=occurrence_deleted") {
		t.Fatalf("DeleteEvent() location = %s", location)
	}

	w = httptest.NewRecorder()
	handler.UpdateEvent(w, eventFormRequest(t, "/calendars/1/events/"+uid, uid, user, "America/New_York", url.Values{
		"dtstart": {"2026-06-15T10:00"}, "dtend": {"2026-06-15T11:00"},
	}))
	if location := w.Header().Get("Location"); w.Code != http.StatusFound || !strings.Contains(location, "status=event_updated") {
		t.Fatalf("UpdateEvent() status = %d location = %s", w.Code, location)
	}
	stored := eventRepo.events["1:"+uid]
	stored.ID = 1
	if strings.Contains(stored.RawICAL, "EXDATE") {
		t.Errorf("the single event kept its series' exclusions:\n%s", stored.RawICAL)
	}
	if got, want := davFreeBusyPeriods(t, s, user, "20260601T000000Z", "20260801T000000Z"), hourPeriods(t, newYork, "2026-06-15T10:00"); !slices.Equal(got, want) {
		t.Errorf("CalDAV free-busy = %v, want %v\n%s", got, want, stored.RawICAL)
	}
}

// clientSeries is a series a CalDAV client stored, carrying properties and
// alarms the event editor neither shows nor writes.
const clientSeries = "BEGIN:VCALENDAR\r\nVERSION:2.0\r\nPRODID:-//Client//EN\r\n" +
	"BEGIN:VEVENT\r\nUID:client-series\r\nDTSTAMP:20260101T000000Z\r\n" +
	"CREATED:20251201T120000Z\r\nLAST-MODIFIED:20251215T120000Z\r\nSEQUENCE:3\r\n" +
	"SUMMARY:Planning\r\nDESCRIPTION:Agenda\r\nX-ALT-DESC;FMTTYPE=text/html:<p>Agenda</p>\r\n" +
	"DTSTART;TZID=America/New_York:20260615T100000\r\nDTEND;TZID=America/New_York:20260615T110000\r\n" +
	"RRULE:FREQ=WEEKLY;COUNT=4\r\n" +
	"EXDATE:20260622T140000Z\r\n" +
	"RDATE;TZID=America/New_York:20260625T150000\r\n" +
	"PRIORITY:1\r\nGEO:40.7;-74.0\r\nCOMMENT:Bring laptops\r\nCONTACT:Front desk\r\nRESOURCES:Projector\r\n" +
	"RELATED-TO:parent-uid\r\nX-APPLE-TRAVEL-ADVISORY-BEHAVIOR:AUTOMATIC\r\n" +
	"ORGANIZER;CN=Owner:mailto:owner@example.com\r\nATTENDEE;PARTSTAT=ACCEPTED;CN=Ann:mailto:ann@example.com\r\n" +
	"BEGIN:VALARM\r\nACTION:DISPLAY\r\nDESCRIPTION:Reminder\r\nTRIGGER:-PT15M\r\nEND:VALARM\r\n" +
	"BEGIN:VALARM\r\nACTION:AUDIO\r\nTRIGGER;VALUE=DATE-TIME:20260615T130000Z\r\nEND:VALARM\r\n" +
	"END:VEVENT\r\nEND:VCALENDAR\r\n"

// What the editor shows of clientSeries, which it submits back.
var clientSeriesForm = url.Values{
	"summary": {"Planning"}, "description": {"Agenda"},
	"dtstart": {"2026-06-15T10:00"}, "dtend": {"2026-06-15T11:00"},
	"recurrence": {"WEEKLY"}, "recurrence_end_type": {"after"}, "recurrence_count": {"4"},
	"organizer": {"Owner <owner@example.com>"}, "attendees": {"Ann <ann@example.com>"},
	"reminder_minutes": {"15"},
}

// A series edit rewrites what the editor shows from its fields and keeps the
// rest of what a client stored: properties the editor has no field for, the
// HTML rendering of a description left unchanged, and alarms the editor
// cannot show. LAST-MODIFIED is the time of the edit.
func TestSeriesEditKeepsWhatTheEditorDoesNotEdit(t *testing.T) {
	newYork, err := time.LoadLocation("America/New_York")
	if err != nil {
		t.Skipf("America/New_York is not available: %v", err)
	}
	validated, err := dav.ValidateCalendarObject(clientSeries, nil)
	if err != nil {
		t.Fatalf("the client's series should pass the validator: %v", err)
	}
	kept := []string{
		"CREATED:20251201T120000Z", "SEQUENCE:3", "PRIORITY:1", "GEO:40.7;-74.0",
		"COMMENT:Bring laptops", "CONTACT:Front desk", "RESOURCES:Projector", "RELATED-TO:parent-uid",
		"X-APPLE-TRAVEL-ADVISORY-BEHAVIOR:AUTOMATIC",
	}
	audioAlarm := "BEGIN:VALARM\r\nACTION:AUDIO\r\nTRIGGER;VALUE=DATE-TIME:20260615T130000Z\r\nEND:VALARM\r\n"
	tests := []struct {
		name string
		edit url.Values
		// present and absent are master lines the stored object has, and
		// has not.
		present, absent []string
		walls           []string
	}{
		{
			name:    "renamed",
			edit:    url.Values{"summary": {"Planning (moved rooms)"}},
			present: append([]string{"SUMMARY:Planning (moved rooms)", "EXDATE:20260622T140000Z", "RDATE;TZID=America/New_York:20260625T150000", "X-ALT-DESC;FMTTYPE=text/html:<p>Agenda</p>"}, kept...),
			absent:  []string{"LAST-MODIFIED:20251215T120000Z", "SUMMARY:Planning"},
			walls:   []string{"2026-06-15T10:00", "2026-06-25T15:00", "2026-06-29T10:00", "2026-07-06T10:00"},
		},
		{
			name: "moved an hour later with a new description",
			edit: url.Values{"dtstart": {"2026-06-15T11:00"}, "dtend": {"2026-06-15T12:00"}, "description": {"New agenda"}},
			present: append([]string{
				"EXDATE;TZID=America/New_York:20260622T110000", "RDATE;TZID=America/New_York:20260625T160000",
			}, kept...),
			absent: []string{"X-ALT-DESC;FMTTYPE=text/html:<p>Agenda</p>", "EXDATE:20260622T140000Z"},
			walls:  []string{"2026-06-15T11:00", "2026-06-25T16:00", "2026-06-29T11:00", "2026-07-06T11:00"},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			calRepo := &fakeCalendarRepo{calendars: map[int64]*store.Calendar{1: {ID: 1, UserID: 100, Name: "Work"}}}
			eventRepo := &fakeEventRepoWithUpsert{fakeEventRepo: fakeEventRepo{events: map[string]*store.Event{
				"1:client-series": {ID: 1, CalendarID: 1, UID: "client-series", ResourceName: "client-series.ics", RawICAL: validated.Body, ETag: "e"},
			}}}
			s := &store.Store{Calendars: calRepo, Events: eventRepo}
			handler := NewHandler(&config.Config{}, s, nil, dav.ValidateCalendarObject, dav.EventInstances)
			user := &store.User{ID: 100}

			edit := maps.Clone(clientSeriesForm)
			maps.Copy(edit, tt.edit)
			w := httptest.NewRecorder()
			handler.UpdateEvent(w, eventFormRequest(t, "/calendars/1/events/client-series", "client-series", user, "America/New_York", edit))
			if location := w.Header().Get("Location"); w.Code != http.StatusFound || !strings.Contains(location, "status=event_updated") {
				t.Fatalf("UpdateEvent() status = %d location = %s", w.Code, location)
			}
			stored := eventRepo.events["1:client-series"]
			stored.ID = 1
			master := storedMaster(t, stored.RawICAL)
			for _, line := range tt.present {
				if !slices.Contains(master, line) {
					t.Errorf("stored master lacks %q:\n%s", line, stored.RawICAL)
				}
			}
			for _, line := range tt.absent {
				if slices.Contains(master, line) {
					t.Errorf("stored master still has %q:\n%s", line, stored.RawICAL)
				}
			}
			lastModified := slices.IndexFunc(master, func(line string) bool { return strings.HasPrefix(line, "LAST-MODIFIED:") })
			dtstamp := slices.IndexFunc(master, func(line string) bool { return strings.HasPrefix(line, "DTSTAMP:") })
			if lastModified < 0 || master[lastModified][len("LAST-MODIFIED:"):] != master[dtstamp][len("DTSTAMP:"):] {
				t.Errorf("stored master's LAST-MODIFIED is not the time of the edit:\n%s", stored.RawICAL)
			}
			if got := strings.Count(stored.RawICAL, "BEGIN:VALARM"); got != 2 || !strings.Contains(stored.RawICAL, audioAlarm) || !strings.Contains(stored.RawICAL, "TRIGGER:-PT15M\r\n") {
				t.Errorf("stored %d alarms, want the editor's reminder and the client's audio alarm:\n%s", got, stored.RawICAL)
			}
			if firstAlarm := slices.Index(master, "BEGIN:VALARM"); firstAlarm >= 0 {
				for _, line := range master[firstAlarm:] {
					if !strings.HasPrefix(line, "BEGIN:") && !strings.HasPrefix(line, "END:") && slices.Contains(kept, line) {
						t.Errorf("property %q follows the master's alarms:\n%s", line, stored.RawICAL)
					}
				}
			}
			if got, want := davFreeBusyPeriods(t, s, user, "20260601T000000Z", "20260801T000000Z"), hourPeriods(t, newYork, tt.walls...); !slices.Equal(got, want) {
				t.Errorf("CalDAV free-busy = %v, want %v\n%s", got, want, stored.RawICAL)
			}
		})
	}
}
