package ui

import (
	"context"
	"encoding/json"
	"maps"
	"net/http"
	"net/http/httptest"
	"net/url"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/go-chi/chi/v5"

	"github.com/jw6ventures/calcard/internal/auth"
	"github.com/jw6ventures/calcard/internal/config"
	"github.com/jw6ventures/calcard/internal/dav"
	"github.com/jw6ventures/calcard/internal/store"
	"github.com/jw6ventures/calcard/internal/ui/utils"
)

// Alarms of fieldedSeries: the editor shows the first two and the last as
// reminders, and cannot show the others as minutes before the start.
const (
	appleReminder = "BEGIN:VALARM\r\nX-WR-ALARMUID:5A5B6C\r\nUID:5A5B6C\r\nTRIGGER:-PT15M\r\nDESCRIPTION:Reminder\r\n" +
		"ACTION:DISPLAY\r\nACKNOWLEDGED:20260615T135000Z\r\nEND:VALARM\r\n"
	emailAlarm = "BEGIN:VALARM\r\nACTION:EMAIL\r\nTRIGGER:-PT1H\r\nSUMMARY:Planning soon\r\n" +
		"DESCRIPTION:Planning starts in an hour\r\nATTENDEE:mailto:owner@example.com\r\nEND:VALARM\r\n"
	afterStartAlarm = "BEGIN:VALARM\r\nACTION:DISPLAY\r\nDESCRIPTION:Started\r\nTRIGGER:PT15M\r\nEND:VALARM\r\n"
	endAlarm        = "BEGIN:VALARM\r\nACTION:DISPLAY\r\nDESCRIPTION:Wrap up\r\nTRIGGER;RELATED=END:-PT5M\r\nEND:VALARM\r\n"
	weekAlarm       = "BEGIN:VALARM\r\nACTION:DISPLAY\r\nDESCRIPTION:Next week\r\nTRIGGER:-P1W\r\nEND:VALARM\r\n"
)

// fieldedSeriesMaster is the master of a series a CalDAV client stored with
// parameters on the properties the editor shows, values the editor cannot
// show, and alarms of every kind.
const fieldedSeriesMaster = "BEGIN:VEVENT\r\nUID:fielded\r\nDTSTAMP:20260101T000000Z\r\nSEQUENCE:2\r\n" +
	"SUMMARY;LANGUAGE=en:Planning\r\nDESCRIPTION;ALTREP=\"cid:agenda@example.com\":Agenda\r\nLOCATION;LANGUAGE=en:Room 1\r\n" +
	"DTSTART;TZID=America/New_York:20260615T100000\r\nDTEND;TZID=America/New_York:20260615T110000\r\n" +
	"RRULE:FREQ=WEEKLY;COUNT=4\r\nSTATUS:confirmed\r\nCLASS:x-team-only\r\nTRANSP:OPAQUE\r\n" +
	"URL;VALUE=URI:https://example.com/planning\r\n" +
	"CATEGORIES:Work\r\nCATEGORIES;LANGUAGE=en:Planning\\, Q3\r\n" +
	"ORGANIZER;CN=Owner;SENT-BY=\"mailto:assistant@example.com\":mailto:owner@example.com\r\n" +
	"ATTENDEE;CN=\"Doe, John\";PARTSTAT=ACCEPTED;ROLE=REQ-PARTICIPANT;RSVP=TRUE;X-NUM-GUESTS=0:mailto:john@example.com\r\n" +
	"ATTENDEE;CUTYPE=ROOM;PARTSTAT=ACCEPTED;CN=Room 1:urn:uuid:room-1\r\n" +
	"ATTENDEE;PARTSTAT=TENTATIVE;CN=Ann:mailto:ann@example.com\r\n" +
	"ATTACH;FMTTYPE=application/pdf:https://example.com/agenda.pdf\r\n" +
	"ATTACH;FMTTYPE=text/html:https://example.com/doc?ids=1,2\r\n" +
	"ATTACH;ENCODING=BASE64;VALUE=BINARY;FMTTYPE=text/plain:SGVsbG8=\r\n" +
	appleReminder + emailAlarm + afterStartAlarm + endAlarm + weekAlarm +
	"END:VEVENT\r\n"

func calendarObject(components ...string) string {
	return "BEGIN:VCALENDAR\r\nVERSION:2.0\r\nPRODID:-//Client//EN\r\n" + strings.Join(components, "") + "END:VCALENDAR\r\n"
}

// fieldedSeriesForm is what the editor submits for fieldedSeries' series
// when nothing in it is changed, given the fields the view model shows.
func fieldedSeriesForm(view calendarEventView) url.Values {
	form := editorForm(view)
	form.Set("dtstart", "2026-06-15T10:00")
	form.Set("dtend", "2026-06-15T11:00")
	form.Set("recurrence", "WEEKLY")
	form.Set("recurrence_end_type", "after")
	form.Set("recurrence_count", "4")
	return form
}

// editorForm is what the event editor submits for an event it shows as view
// when none of the fields below is changed: lists are joined as the editor
// fills its inputs, and each reminder is a row of its own.
func editorForm(view calendarEventView) url.Values {
	form := url.Values{
		"summary": {view.Summary}, "description": {view.Description}, "location": {view.Location},
		"status": {view.Status}, "url": {view.URL}, "class": {view.Class}, "transparency": {view.Transparency},
		"categories": {strings.Join(view.Categories, ", ")},
		"organizer":  {view.Organizer},
		"attendees":  {strings.Join(view.Attendees, "\n")}, "attachments": {strings.Join(view.Attachments, "\n")},
	}
	for _, minutes := range view.Reminders {
		form.Add("reminder_minutes", strconv.Itoa(minutes))
	}
	return form
}

// storedEventHandler is a handler over calendar 1 holding raw as the event
// uid, which must pass the validator.
func storedEventHandler(t *testing.T, uid, raw string) (*Handler, *store.Store, *fakeEventRepoWithUpsert, *store.User) {
	t.Helper()
	validated, err := dav.ValidateCalendarObject(raw, nil)
	if err != nil {
		t.Fatalf("the stored object should pass the validator: %v\n%s", err, raw)
	}
	calRepo := &fakeCalendarRepo{calendars: map[int64]*store.Calendar{1: {ID: 1, UserID: 100, Name: "Work"}}}
	eventRepo := &fakeEventRepoWithUpsert{fakeEventRepo: fakeEventRepo{events: map[string]*store.Event{
		"1:" + uid: {ID: 1, CalendarID: 1, UID: uid, ResourceName: uid + ".ics", RawICAL: validated.Body, ETag: "e"},
	}}}
	s := &store.Store{Calendars: calRepo, Events: eventRepo}
	return NewHandler(&config.Config{}, s, nil, dav.ValidateCalendarObject, dav.EventInstances), s, eventRepo, &store.User{ID: 100}
}

// updateEvent submits form as an edit of uid in calendar 1, which must be
// stored, and returns the stored object.
func updateEvent(t *testing.T, handler *Handler, events *fakeEventRepoWithUpsert, user *store.User, uid, zone string, form url.Values) string {
	t.Helper()
	w := httptest.NewRecorder()
	handler.UpdateEvent(w, eventFormRequest(t, "/calendars/1/events/"+uid, uid, user, zone, form))
	if location := w.Header().Get("Location"); w.Code != http.StatusFound || !strings.Contains(location, "status=event_updated") {
		t.Fatalf("UpdateEvent() status = %d location = %s", w.Code, location)
	}
	stored := events.events["1:"+uid]
	// The fake store assigns no ID, and the DAV server pages rows by it.
	stored.ID = 1
	return stored.RawICAL
}

// storedOverride is the content lines of the stored override whose
// RECURRENCE-ID line is recurrenceID.
func storedOverride(t *testing.T, raw, recurrenceID string) []string {
	t.Helper()
	_, components, _ := utils.SplitComponents(raw)
	for _, component := range components {
		if slices.Contains(component, recurrenceID) {
			return component
		}
	}
	t.Fatalf("stored object has no override %q:\n%s", recurrenceID, raw)
	return nil
}

// alarmLines splits alarms, VALARMs written one after another, into their
// content lines.
func alarmLines(alarms ...string) []string {
	var lines []string
	for _, alarm := range alarms {
		lines = append(lines, strings.Split(strings.TrimSuffix(alarm, "\r\n"), "\r\n")...)
	}
	return lines
}

// containsRun reports whether lines holds want as consecutive lines.
func containsRun(lines, want []string) bool {
	for i := range lines {
		if i+len(want) <= len(lines) && slices.Equal(lines[i:i+len(want)], want) {
			return true
		}
	}
	return false
}

// The editor shows a reminder for each alarm a duration before the start,
// whatever its action, and none for one it cannot show as minutes before the
// start. Every category is shown, an escaped comma inside one included, and
// an attachment only when it is a URI.
func TestEditorShowsWhatItCanWriteBack(t *testing.T) {
	handler, _, _, user := storedEventHandler(t, "fielded", calendarObject(fieldedSeriesMaster))
	view := calendarViewModel(t, handler, user, "2026-06-01", "2026-06-16")

	if want := []int{15, 60, 7 * 24 * 60}; !slices.Equal(view.Reminders, want) {
		t.Errorf("reminders = %v, want %v: only the alarms at or before the start", view.Reminders, want)
	}
	if want := []string{"Work", "Planning, Q3"}; !slices.Equal(view.Categories, want) {
		t.Errorf("categories = %q, want %q", view.Categories, want)
	}
	if want := []string{"https://example.com/agenda.pdf", "https://example.com/doc?ids=1,2"}; !slices.Equal(view.Attachments, want) {
		t.Errorf("attachments = %q, want %q", view.Attachments, want)
	}
	if want := []string{"Doe, John <john@example.com>", "Room 1 <urn:uuid:room-1>", "Ann <ann@example.com>"}; !slices.Equal(view.Attendees, want) {
		t.Errorf("attendees = %q, want %q", view.Attendees, want)
	}
	if view.Status != "CONFIRMED" || view.Class != "X-TEAM-ONLY" || view.Transparency != "OPAQUE" {
		t.Errorf("status, class, transparency = %q, %q, %q, want them upper-cased as the editor's selects hold them", view.Status, view.Class, view.Transparency)
	}
}

// What the editor submits as it showed it is kept as the client wrote it:
// parameters the editor has no field for -- PARTSTAT, ROLE, RSVP, SENT-BY,
// LANGUAGE, ALTREP, FMTTYPE -- an attendee that is not a mail address, a
// binary attachment, categories over several lines, and every alarm, the
// properties RFC 9074 gives an alarm included.
func TestSeriesEditKeepsTheFieldsItDidNotChange(t *testing.T) {
	handler, _, events, user := storedEventHandler(t, "fielded", calendarObject(fieldedSeriesMaster))
	form := fieldedSeriesForm(calendarViewModel(t, handler, user, "2026-06-01", "2026-06-16"))
	form.Set("summary", "Planning (moved rooms)")

	raw := updateEvent(t, handler, events, user, "fielded", "America/New_York", form)
	master := storedMaster(t, raw)
	for _, want := range []string{
		"SUMMARY:Planning (moved rooms)",
		`DESCRIPTION;ALTREP="cid:agenda@example.com":Agenda`, "LOCATION;LANGUAGE=en:Room 1", "STATUS:confirmed",
		"CLASS:x-team-only", "TRANSP:OPAQUE", "URL;VALUE=URI:https://example.com/planning",
		"ATTACH;FMTTYPE=text/html:https://example.com/doc?ids=1,2",
		"CATEGORIES:Work", `CATEGORIES;LANGUAGE=en:Planning\, Q3`,
		`ORGANIZER;CN=Owner;SENT-BY="mailto:assistant@example.com":mailto:owner@example.com`,
		`ATTENDEE;CN="Doe, John";PARTSTAT=ACCEPTED;ROLE=REQ-PARTICIPANT;RSVP=TRUE;X-NUM-GUESTS=0:mailto:john@example.com`,
		"ATTENDEE;CUTYPE=ROOM;PARTSTAT=ACCEPTED;CN=Room 1:urn:uuid:room-1",
		"ATTENDEE;PARTSTAT=TENTATIVE;CN=Ann:mailto:ann@example.com",
		"ATTACH;FMTTYPE=application/pdf:https://example.com/agenda.pdf",
		"ATTACH;ENCODING=BASE64;VALUE=BINARY;FMTTYPE=text/plain:SGVsbG8=",
		"SEQUENCE:2",
	} {
		if !slices.Contains(master, want) {
			t.Errorf("stored master lacks %q:\n%s", want, raw)
		}
	}
	for _, property := range []string{"SUMMARY", "ORGANIZER", "ATTENDEE", "ATTACH", "CATEGORIES", "DESCRIPTION", "LOCATION", "STATUS", "CLASS", "TRANSP", "URL"} {
		want := strings.Count(fieldedSeriesMaster, "\r\n"+property+":") + strings.Count(fieldedSeriesMaster, "\r\n"+property+";")
		if got := countProperty(master, property); got != want {
			t.Errorf("stored master has %d %s lines, want %d:\n%s", got, property, want, raw)
		}
	}
	for _, alarm := range []string{appleReminder, emailAlarm, afterStartAlarm, endAlarm, weekAlarm} {
		if !containsRun(master, alarmLines(alarm)) {
			t.Errorf("stored master lacks the alarm\n%s\nin\n%s", alarm, raw)
		}
	}
	if got := countProperty(master, "BEGIN:VALARM"); got != 5 {
		t.Errorf("stored master has %d alarms, want 5:\n%s", got, raw)
	}
}

// countProperty counts the lines of a property, or the BEGIN lines of a
// component given as "BEGIN:NAME", among component's lines.
func countProperty(component []string, name string) int {
	count := 0
	for _, line := range component {
		if strings.HasPrefix(name, "BEGIN:") {
			if strings.EqualFold(line, name) {
				count++
			}
			continue
		}
		if keyPart, _, ok := strings.Cut(line, ":"); ok && strings.EqualFold(strings.SplitN(keyPart, ";", 2)[0], name) {
			count++
		}
	}
	return count
}

// A field the edit changes is written from the form; what the client wrote
// beside it that the editor has no field for stays with it.
func TestSeriesEditWritesTheFieldsItChanged(t *testing.T) {
	tests := []struct {
		name string
		edit func(form url.Values)
		// present and absent are lines the stored master has, and has not.
		present, absent []string
		// alarms are the alarms the stored master has, in order.
		alarms []string
	}{
		{
			name: "attendee renamed, one removed, one added",
			edit: func(form url.Values) {
				form.Set("attendees", "Room 1 <urn:uuid:room-1>\nAnn Lee <ann@example.com>\nNew Person <new@example.com>")
			},
			present: []string{
				"ATTENDEE;CUTYPE=ROOM;PARTSTAT=ACCEPTED;CN=Room 1:urn:uuid:room-1",
				"ATTENDEE;PARTSTAT=TENTATIVE;CN=Ann Lee:mailto:ann@example.com",
				"ATTENDEE;CN=New Person:mailto:new@example.com",
			},
			absent: []string{"ATTENDEE;PARTSTAT=TENTATIVE;CN=Ann:mailto:ann@example.com"},
			alarms: []string{appleReminder, emailAlarm, afterStartAlarm, endAlarm, weekAlarm},
		},
		{
			name: "name with brackets given to an attendee",
			edit: func(form url.Values) {
				form.Set("attendees", "Doe, John <john@example.com>\nRoom 1 <urn:uuid:room-1>\n"+`"Ann <ops> Lee" <ann@example.com>`)
			},
			present: []string{"ATTENDEE;PARTSTAT=TENTATIVE;CN=Ann <ops> Lee:mailto:ann@example.com"},
			alarms:  []string{appleReminder, emailAlarm, afterStartAlarm, endAlarm, weekAlarm},
		},
		{
			name: "room renamed",
			edit: func(form url.Values) {
				form.Set("attendees", "Doe, John <john@example.com>\nRoom One <urn:uuid:room-1>\nAnn <ann@example.com>")
			},
			present: []string{"ATTENDEE;CUTYPE=ROOM;PARTSTAT=ACCEPTED;CN=Room One:urn:uuid:room-1"},
			absent:  []string{"ATTENDEE;CUTYPE=ROOM;PARTSTAT=ACCEPTED;CN=Room 1:urn:uuid:room-1"},
			alarms:  []string{appleReminder, emailAlarm, afterStartAlarm, endAlarm, weekAlarm},
		},
		{
			name: "organizer's name cleared",
			edit: func(form url.Values) { form.Set("organizer", "owner@example.com") },
			present: []string{
				`ORGANIZER;SENT-BY="mailto:assistant@example.com":mailto:owner@example.com`,
			},
			alarms: []string{appleReminder, emailAlarm, afterStartAlarm, endAlarm, weekAlarm},
		},
		{
			name:    "categories changed",
			edit:    func(form url.Values) { form.Set("categories", "Work, Review") },
			present: []string{"CATEGORIES:Work,Review"},
			absent:  []string{"CATEGORIES:Work", `CATEGORIES;LANGUAGE=en:Planning\, Q3`},
			alarms:  []string{appleReminder, emailAlarm, afterStartAlarm, endAlarm, weekAlarm},
		},
		{
			name:    "attachment replaced",
			edit:    func(form url.Values) { form.Set("attachments", "https://example.com/minutes.pdf") },
			present: []string{"ATTACH:https://example.com/minutes.pdf", "ATTACH;ENCODING=BASE64;VALUE=BINARY;FMTTYPE=text/plain:SGVsbG8="},
			absent:  []string{"ATTACH;FMTTYPE=application/pdf:https://example.com/agenda.pdf", "ATTACH;FMTTYPE=text/html:https://example.com/doc?ids=1,2"},
			alarms:  []string{appleReminder, emailAlarm, afterStartAlarm, endAlarm, weekAlarm},
		},
		{
			name: "reminder added",
			edit: func(form url.Values) { form.Add("reminder_minutes", "30") },
			alarms: []string{
				appleReminder, emailAlarm, afterStartAlarm, endAlarm, weekAlarm,
				"BEGIN:VALARM\r\nACTION:DISPLAY\r\nDESCRIPTION:Reminder\r\nTRIGGER:-PT30M\r\nEND:VALARM\r\n",
			},
		},
		{
			name:   "reminders removed",
			edit:   func(form url.Values) { form.Del("reminder_minutes") },
			alarms: []string{afterStartAlarm, endAlarm},
		},
		{
			name:    "class changed",
			edit:    func(form url.Values) { form.Set("class", "PRIVATE") },
			present: []string{"CLASS:PRIVATE"},
			absent:  []string{"CLASS:x-team-only"},
			alarms:  []string{appleReminder, emailAlarm, afterStartAlarm, endAlarm, weekAlarm},
		},
		{
			name: "summary and location changed",
			edit: func(form url.Values) {
				form.Set("summary", "Review")
				form.Set("location", "Room 2")
			},
			present: []string{"SUMMARY:Review", "LOCATION:Room 2"},
			absent:  []string{"LOCATION;LANGUAGE=en:Room 1"},
			alarms:  []string{appleReminder, emailAlarm, afterStartAlarm, endAlarm, weekAlarm},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			handler, _, events, user := storedEventHandler(t, "fielded", calendarObject(fieldedSeriesMaster))
			form := fieldedSeriesForm(calendarViewModel(t, handler, user, "2026-06-01", "2026-06-16"))
			tt.edit(form)

			raw := updateEvent(t, handler, events, user, "fielded", "America/New_York", form)
			master := storedMaster(t, raw)
			for _, want := range tt.present {
				if !slices.Contains(master, want) {
					t.Errorf("stored master lacks %q:\n%s", want, raw)
				}
			}
			for _, unwanted := range tt.absent {
				if slices.Contains(master, unwanted) {
					t.Errorf("stored master still has %q:\n%s", unwanted, raw)
				}
			}
			firstAlarm := slices.Index(master, "BEGIN:VALARM")
			if firstAlarm < 0 {
				firstAlarm = len(master)
			}
			if got, want := master[firstAlarm:], alarmLines(tt.alarms...); !slices.Equal(got, want) {
				t.Errorf("stored alarms =\n%s\nwant\n%s", strings.Join(got, "\n"), strings.Join(want, "\n"))
			}
		})
	}
}

// fieldedOverride is an override of fieldedSeries' second instance, moved to
// the afternoon, carrying data of its own the editor has no field for.
const fieldedOverride = "BEGIN:VEVENT\r\nUID:fielded\r\nDTSTAMP:20260101T000000Z\r\nSEQUENCE:1\r\n" +
	"RECURRENCE-ID;TZID=America/New_York:20260622T100000\r\nSUMMARY:Planning (afternoon)\r\n" +
	"DTSTART;TZID=America/New_York:20260622T150000\r\nDTEND;TZID=America/New_York:20260622T160000\r\n" +
	"X-MICROSOFT-CDO-BUSYSTATUS:TENTATIVE\r\nCATEGORIES:Work\r\nCATEGORIES:Moved\r\n" +
	"ATTENDEE;PARTSTAT=DECLINED;CN=Ann:mailto:ann@example.com\r\n" +
	emailAlarm +
	"END:VEVENT\r\n"

// occurrenceEditorForm is what the editor submits, unchanged, for the
// override at index of the one event calendar 1 holds from start through the
// day before end. The browser shows an override as its series with the
// override's own fields over it (calendar_events_js expandRecurringEvents),
// so a field the view model left out of the override would show the series'.
func occurrenceEditorForm(t *testing.T, handler *Handler, user *store.User, start, end string, index int) url.Values {
	t.Helper()
	req := httptest.NewRequest(http.MethodGet, "/calendars/1/events.json?start="+start+"&end="+end, nil)
	rctx := chi.NewRouteContext()
	rctx.URLParams.Add("id", "1")
	req = req.WithContext(context.WithValue(auth.WithUser(req.Context(), user), chi.RouteCtxKey, rctx))
	w := httptest.NewRecorder()
	handler.GetCalendarEventsJSON(w, req)
	var events []map[string]json.RawMessage
	if err := json.Unmarshal(w.Body.Bytes(), &events); err != nil || len(events) != 1 {
		t.Fatalf("events JSON = %d %s (%v), want one event", w.Code, w.Body.String(), err)
	}
	var overrides []map[string]json.RawMessage
	if err := json.Unmarshal(events[0]["overrides"], &overrides); err != nil || index >= len(overrides) {
		t.Fatalf("event has overrides %s, want one at %d", events[0]["overrides"], index)
	}
	shown := maps.Clone(events[0])
	maps.Copy(shown, overrides[index])
	encoded, err := json.Marshal(shown)
	if err != nil {
		t.Fatal(err)
	}
	var view calendarEventView
	if err := json.Unmarshal(encoded, &view); err != nil {
		t.Fatal(err)
	}
	return editorForm(view)
}

// Editing an occurrence that already has an override edits that override: what
// the editor does not show of it is kept, and what it leaves out that the
// series has is not added.
func TestOccurrenceEditKeepsTheOverridesUneditedData(t *testing.T) {
	newYork, err := time.LoadLocation("America/New_York")
	if err != nil {
		t.Skipf("America/New_York is not available: %v", err)
	}
	handler, s, events, user := storedEventHandler(t, "fielded", calendarObject(fieldedSeriesMaster, fieldedOverride))
	form := occurrenceEditorForm(t, handler, user, "2026-06-01", "2026-06-16", 0)
	form.Set("summary", "Planning (late afternoon)")
	form.Set("dtstart", "2026-06-22T16:00")
	form.Set("dtend", "2026-06-22T17:00")
	form.Set("edit_scope", "occurrence")
	form.Set("recurrence_id", "2026-06-22T10:00")
	form.Set("recurrence_timezone", "America/New_York")

	raw := updateEvent(t, handler, events, user, "fielded", "America/New_York", form)
	override := storedOverride(t, raw, "RECURRENCE-ID;TZID=America/New_York:20260622T100000")
	for _, want := range []string{
		"SUMMARY:Planning (late afternoon)", "SEQUENCE:1", "X-MICROSOFT-CDO-BUSYSTATUS:TENTATIVE",
		"CATEGORIES:Work", "CATEGORIES:Moved", "ATTENDEE;PARTSTAT=DECLINED;CN=Ann:mailto:ann@example.com",
	} {
		if !slices.Contains(override, want) {
			t.Errorf("stored override lacks %q:\n%s", want, raw)
		}
	}
	if !containsRun(override, alarmLines(emailAlarm)) || countProperty(override, "BEGIN:VALARM") != 1 {
		t.Errorf("stored override does not hold just its email alarm:\n%s", raw)
	}
	// What the override leaves out stays out, though the series has it.
	for property, want := range map[string]int{
		"LOCATION": 0, "DESCRIPTION": 1, "URL": 0, "CLASS": 0, "TRANSP": 0, "STATUS": 0, "ORGANIZER": 0, "ATTACH": 0,
		"ATTENDEE": 2, "CATEGORIES": 2,
	} {
		if got := countProperty(override, property); got != want {
			t.Errorf("stored override has %d %s lines, want %d:\n%s", got, property, want, raw)
		}
	}
	if got, want := davFreeBusyPeriods(t, s, user, "20260601T000000Z", "20260801T000000Z"),
		hourPeriods(t, newYork, "2026-06-15T10:00", "2026-06-22T16:00", "2026-06-29T10:00", "2026-07-06T10:00"); !slices.Equal(got, want) {
		t.Errorf("CalDAV free-busy = %v, want %v\n%s", got, want, raw)
	}
}

// An occurrence edited for the first time becomes an override holding what the
// series holds that the editor does not show, as a client copies the master
// into the override: attendees' parameters, alarms, and properties of the
// client's own. Only what makes the series a series is left out.
func TestOccurrenceEditStartsFromTheSeries(t *testing.T) {
	handler, _, events, user := storedEventHandler(t, "fielded", calendarObject(fieldedSeriesMaster))
	form := editorForm(calendarViewModel(t, handler, user, "2026-06-01", "2026-06-16"))
	form.Set("summary", "Planning (once)")
	form.Set("dtstart", "2026-06-29T10:00")
	form.Set("dtend", "2026-06-29T11:00")
	form.Set("edit_scope", "occurrence")
	form.Set("recurrence_id", "2026-06-29T10:00")
	form.Set("recurrence_timezone", "America/New_York")

	raw := updateEvent(t, handler, events, user, "fielded", "America/New_York", form)
	override := storedOverride(t, raw, "RECURRENCE-ID;TZID=America/New_York:20260629T100000")
	for _, want := range []string{
		"SUMMARY:Planning (once)", "SEQUENCE:2",
		`ATTENDEE;CN="Doe, John";PARTSTAT=ACCEPTED;ROLE=REQ-PARTICIPANT;RSVP=TRUE;X-NUM-GUESTS=0:mailto:john@example.com`,
		"ATTACH;ENCODING=BASE64;VALUE=BINARY;FMTTYPE=text/plain:SGVsbG8=",
	} {
		if !slices.Contains(override, want) {
			t.Errorf("stored override lacks %q:\n%s", want, raw)
		}
	}
	for _, alarm := range []string{appleReminder, emailAlarm, afterStartAlarm, endAlarm, weekAlarm} {
		if !containsRun(override, alarmLines(alarm)) {
			t.Errorf("stored override lacks the alarm\n%s\nin\n%s", alarm, raw)
		}
	}
	for _, property := range []string{"RRULE", "RDATE", "EXDATE", "EXRULE"} {
		if countProperty(override, property) != 0 {
			t.Errorf("stored override has the series' %s:\n%s", property, raw)
		}
	}
}

// overriddenSeries is a weekly series of hour-long instances at 10:00 in New
// York with three overrides: one moving its second instance to the afternoon,
// one only retitling its third, and one lengthening its fourth to 30 hours.
const overriddenSeries = "BEGIN:VEVENT\r\nUID:overridden\r\nDTSTAMP:20260101T000000Z\r\nSUMMARY:Standup\r\n" +
	"DTSTART;TZID=America/New_York:20260615T100000\r\nDTEND;TZID=America/New_York:20260615T110000\r\n" +
	"RRULE:FREQ=WEEKLY;COUNT=4\r\nEND:VEVENT\r\n" +
	"BEGIN:VEVENT\r\nUID:overridden\r\nDTSTAMP:20260101T000000Z\r\nSUMMARY:Moved standup\r\n" +
	"RECURRENCE-ID;TZID=America/New_York:20260622T100000\r\n" +
	"DTSTART;TZID=America/New_York:20260622T150000\r\nDTEND;TZID=America/New_York:20260622T160000\r\nEND:VEVENT\r\n" +
	"BEGIN:VEVENT\r\nUID:overridden\r\nDTSTAMP:20260101T000000Z\r\nSUMMARY:Retitled standup\r\n" +
	"RECURRENCE-ID;TZID=America/New_York:20260629T100000\r\n" +
	"DTSTART;TZID=America/New_York:20260629T100000\r\nDTEND;TZID=America/New_York:20260629T110000\r\nEND:VEVENT\r\n" +
	"BEGIN:VEVENT\r\nUID:overridden\r\nDTSTAMP:20260101T000000Z\r\nSUMMARY:Long standup\r\n" +
	"RECURRENCE-ID;TZID=America/New_York:20260706T100000\r\n" +
	"DTSTART;TZID=America/New_York:20260706T100000\r\nDURATION:PT30H\r\nEND:VEVENT\r\n"

// An override names the instance it replaces by that instance's start, so when
// a series edit moves the series' instances, each override is moved to name
// the same day's instance of the moved series, and an override that left its
// instance's time as it was moves with it. When the edit ends the series, no
// instance is left to replace, and the overrides go.
func TestSeriesEditTakesItsOverridesAlong(t *testing.T) {
	newYork, err := time.LoadLocation("America/New_York")
	if err != nil {
		t.Skipf("America/New_York is not available: %v", err)
	}
	losAngeles, err := time.LoadLocation("America/Los_Angeles")
	if err != nil {
		t.Skipf("America/Los_Angeles is not available: %v", err)
	}
	day := func(date string) string {
		start, _ := time.Parse("20060102", date)
		return start.Format("20060102T150405Z") + "/" + start.AddDate(0, 0, 1).Format("20060102T150405Z")
	}
	// long is the period of a 30-hour instance starting at wall in loc.
	long := func(loc *time.Location, wall string) string {
		start, err := time.ParseInLocation("2006-01-02T15:04", wall, loc)
		if err != nil {
			t.Fatalf("bad wall clock %q: %v", wall, err)
		}
		return start.UTC().Format("20060102T150405Z") + "/" + start.Add(30*time.Hour).UTC().Format("20060102T150405Z")
	}
	series := url.Values{
		"summary": {"Standup"}, "dtstart": {"2026-06-15T10:00"}, "dtend": {"2026-06-15T11:00"},
		"recurrence": {"WEEKLY"}, "recurrence_end_type": {"after"}, "recurrence_count": {"4"},
	}
	tests := []struct {
		name string
		// zone is the editor's zone, New York when empty.
		zone string
		edit url.Values
		// overrides are the stored overrides' RECURRENCE-ID lines, each
		// with the lines the override must also hold.
		overrides map[string][]string
		want      func(t *testing.T) []string
	}{
		{
			name: "moved an hour later",
			edit: url.Values{"dtstart": {"2026-06-15T11:00"}, "dtend": {"2026-06-15T12:00"}},
			overrides: map[string][]string{
				"RECURRENCE-ID;TZID=America/New_York:20260622T110000": {"DTSTART;TZID=America/New_York:20260622T150000"},
				"RECURRENCE-ID;TZID=America/New_York:20260629T110000": {
					"DTSTART;TZID=America/New_York:20260629T110000", "DTEND;TZID=America/New_York:20260629T120000",
				},
				"RECURRENCE-ID;TZID=America/New_York:20260706T110000": {"DTSTART;TZID=America/New_York:20260706T110000", "DURATION:PT30H"},
			},
			want: func(t *testing.T) []string {
				periods := append(hourPeriods(t, newYork, "2026-06-15T11:00", "2026-06-22T15:00", "2026-06-29T11:00"), long(newYork, "2026-07-06T11:00"))
				slices.Sort(periods)
				return periods
			},
		},
		{
			// RFC 4791 §4.1 has an override name its instance in the
			// zone of the series' DTSTART, so an edit that left the
			// overrides naming New York times would not be stored.
			name: "moved into another zone",
			zone: "America/Los_Angeles",
			overrides: map[string][]string{
				"RECURRENCE-ID;TZID=America/Los_Angeles:20260622T100000": {"DTSTART;TZID=America/New_York:20260622T150000"},
				"RECURRENCE-ID;TZID=America/Los_Angeles:20260629T100000": {
					"DTSTART;TZID=America/Los_Angeles:20260629T100000", "DTEND;TZID=America/Los_Angeles:20260629T110000",
				},
				"RECURRENCE-ID;TZID=America/Los_Angeles:20260706T100000": {"DTSTART;TZID=America/Los_Angeles:20260706T100000", "DURATION:PT30H"},
			},
			want: func(t *testing.T) []string {
				periods := append(hourPeriods(t, losAngeles, "2026-06-15T10:00", "2026-06-29T10:00"),
					hourPeriods(t, newYork, "2026-06-22T15:00")[0], long(losAngeles, "2026-07-06T10:00"))
				slices.Sort(periods)
				return periods
			},
		},
		{
			name: "made all-day",
			edit: url.Values{"all_day": {"on"}, "dtstart": {"2026-06-15"}, "dtend": {"2026-06-15"}},
			overrides: map[string][]string{
				"RECURRENCE-ID;VALUE=DATE:20260622": {"DTSTART;TZID=America/New_York:20260622T150000"},
				"RECURRENCE-ID;VALUE=DATE:20260629": {"DTSTART;VALUE=DATE:20260629", "DTEND;VALUE=DATE:20260630"},
				// The override was longer than the series' instances, and
				// keeps its length rounded up to whole days.
				"RECURRENCE-ID;VALUE=DATE:20260706": {"DTSTART;VALUE=DATE:20260706", "DTEND;VALUE=DATE:20260708"},
			},
			want: func(t *testing.T) []string {
				periods := append(hourPeriods(t, newYork, "2026-06-22T15:00"), day("20260615"), day("20260629"), "20260706T000000Z/20260708T000000Z")
				slices.Sort(periods)
				return periods
			},
		},
		{
			name:      "made a single event",
			edit:      url.Values{"recurrence": {""}},
			overrides: map[string][]string{},
			want: func(t *testing.T) []string {
				return hourPeriods(t, newYork, "2026-06-15T10:00")
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			handler, s, events, user := storedEventHandler(t, "overridden", calendarObject(overriddenSeries))
			form := maps.Clone(series)
			maps.Copy(form, tt.edit)

			zone := tt.zone
			if zone == "" {
				zone = "America/New_York"
			}
			raw := updateEvent(t, handler, events, user, "overridden", zone, form)
			_, components, _ := utils.SplitComponents(raw)
			if got := len(components) - 1; got != len(tt.overrides) {
				t.Errorf("stored %d overrides, want %d:\n%s", got, len(tt.overrides), raw)
			}
			for recurrenceID, lines := range tt.overrides {
				override := storedOverride(t, raw, recurrenceID)
				for _, want := range lines {
					if !slices.Contains(override, want) {
						t.Errorf("stored override %q lacks %q:\n%s", recurrenceID, want, raw)
					}
				}
			}
			if got, want := davFreeBusyPeriods(t, s, user, "20260601T000000Z", "20260801T000000Z"), tt.want(t); !slices.Equal(got, want) {
				t.Errorf("CalDAV free-busy = %v, want %v\n%s", got, want, raw)
			}
		})
	}
}

// A display name holding a comma, semicolon or colon is written quoted and
// shown as written, so submitting what the editor shows stores the same
// lines again. Several addresses on one line of the attendee field are split
// at the commas between them, not at a comma inside a name.
func TestAttendeeNamesRoundTripThroughTheEditor(t *testing.T) {
	calRepo := &fakeCalendarRepo{calendars: map[int64]*store.Calendar{1: {ID: 1, UserID: 100, Name: "Work"}}}
	eventRepo := &fakeEventRepoWithUpsert{fakeEventRepo: fakeEventRepo{events: make(map[string]*store.Event)}}
	s := &store.Store{Calendars: calRepo, Events: eventRepo}
	handler := NewHandler(&config.Config{}, s, nil, dav.ValidateCalendarObject, dav.EventInstances)
	user := &store.User{ID: 100}

	w := httptest.NewRecorder()
	handler.CreateEvent(w, eventFormRequest(t, "/calendars/1/events", "", user, "America/New_York", url.Values{
		"dtstart": {"2026-06-15T10:00"}, "dtend": {"2026-06-15T11:00"},
		"organizer": {"Doe, Jane <jane@example.com>"},
		"attendees": {"Doe, John <john@example.com>\nDoe; John <john2@example.com>\nDr: Who <who@example.com>, plain@example.com\n" +
			`"bob@example.com, Bob" <bob@example.com>` + "\nself@example.com <self@example.com>"},
	}))
	if location := w.Header().Get("Location"); w.Code != http.StatusFound || !strings.Contains(location, "status=event_created") {
		t.Fatalf("CreateEvent() status = %d location = %s", w.Code, location)
	}
	var uid string
	for _, created := range eventRepo.events {
		uid = created.UID
	}
	created := eventRepo.events["1:"+uid].RawICAL
	wantLines := []string{
		`ORGANIZER;CN="Doe, Jane":mailto:jane@example.com`,
		`ATTENDEE;CN="Doe, John":mailto:john@example.com`,
		`ATTENDEE;CN="Doe; John":mailto:john2@example.com`,
		`ATTENDEE;CN="Dr: Who":mailto:who@example.com`,
		`ATTENDEE:mailto:plain@example.com`,
		`ATTENDEE;CN="bob@example.com, Bob":mailto:bob@example.com`,
		`ATTENDEE;CN=self@example.com:mailto:self@example.com`,
	}
	master := storedMaster(t, created)
	for _, want := range wantLines {
		if !slices.Contains(master, want) {
			t.Errorf("stored event lacks %q:\n%s", want, created)
		}
	}

	view := calendarViewModel(t, handler, user, "2026-06-01", "2026-06-16")
	if want := "Doe, Jane <jane@example.com>"; view.Organizer != want {
		t.Errorf("organizer = %q, want %q", view.Organizer, want)
	}
	if want := []string{
		"Doe, John <john@example.com>", "Doe; John <john2@example.com>", "Dr: Who <who@example.com>", "plain@example.com",
		`"bob@example.com, Bob" <bob@example.com>`, "self@example.com",
	}; !slices.Equal(view.Attendees, want) {
		t.Errorf("attendees = %q, want %q", view.Attendees, want)
	}

	form := editorForm(view)
	form.Set("dtstart", "2026-06-15T10:00")
	form.Set("dtend", "2026-06-15T11:00")
	edited := storedMaster(t, updateEvent(t, handler, eventRepo, user, uid, "America/New_York", form))
	for _, want := range wantLines {
		if !slices.Contains(edited, want) {
			t.Errorf("edited event lacks %q:\n%s", want, strings.Join(edited, "\n"))
		}
	}
	if got := countProperty(edited, "ATTENDEE"); got != 6 {
		t.Errorf("edited event has %d attendees, want 6:\n%s", got, strings.Join(edited, "\n"))
	}
}

func TestSplitAddressList(t *testing.T) {
	tests := []struct {
		value string
		want  []string
	}{
		{"Ann <ann@example.com>\nBob <bob@example.com>", []string{"Ann <ann@example.com>", "Bob <bob@example.com>"}},
		{"ann@example.com, bob@example.com", []string{"ann@example.com", "bob@example.com"}},
		{"Doe, John <john@example.com>, Ann <ann@example.com>", []string{"Doe, John <john@example.com>", "Ann <ann@example.com>"}},
		{`"Doe, Jane" <jane@example.com>,bob@example.com`, []string{`"Doe, Jane" <jane@example.com>`, "bob@example.com"}},
		{"Room <urn:uuid:a,b>, ann@example.com", []string{"Room <urn:uuid:a,b>", "ann@example.com"}},
		{`"Ops <team>" <ops@example.com>, ann@example.com`, []string{`"Ops <team>" <ops@example.com>`, "ann@example.com"}},
		{"\r\n , \n", nil},
		{`"bob@example.com, Bob" <bob@example.com>, ann@example.com`, []string{`"bob@example.com, Bob" <bob@example.com>`, "ann@example.com"}},
	}
	for _, tt := range tests {
		if got := splitAddressList(tt.value); !slices.Equal(got, tt.want) {
			t.Errorf("splitAddressList(%q) = %q, want %q", tt.value, got, tt.want)
		}
	}
}

// A form value that would write a content line of its own is refused, and
// nothing is stored.
func TestEventFormRefusesContentLineInjection(t *testing.T) {
	for _, field := range []string{"status", "class", "transparency", "recurrence"} {
		t.Run(field, func(t *testing.T) {
			calRepo := &fakeCalendarRepo{calendars: map[int64]*store.Calendar{1: {ID: 1, UserID: 100, Name: "Work"}}}
			eventRepo := &fakeEventRepoWithUpsert{fakeEventRepo: fakeEventRepo{events: make(map[string]*store.Event)}}
			handler := NewHandler(&config.Config{}, &store.Store{Calendars: calRepo, Events: eventRepo}, nil, dav.ValidateCalendarObject, dav.EventInstances)
			user := &store.User{ID: 100}

			w := httptest.NewRecorder()
			handler.CreateEvent(w, eventFormRequest(t, "/calendars/1/events", "", user, "America/New_York", url.Values{
				"dtstart": {"2026-06-15T10:00"}, "dtend": {"2026-06-15T11:00"},
				field: {"WEEKLY\r\nATTENDEE;CN=Evil:mailto:evil@example.com"},
			}))
			if location := w.Header().Get("Location"); w.Code != http.StatusFound || !strings.Contains(location, "error=") {
				t.Errorf("CreateEvent() status = %d location = %s, want an error", w.Code, location)
			}
			if len(eventRepo.events) != 0 {
				t.Errorf("stored %d events, want none", len(eventRepo.events))
			}
		})
	}
}

func TestParseTriggerMinutes(t *testing.T) {
	tests := []struct {
		value   string
		minutes int
		ok      bool
	}{
		{"-PT15M", 15, true},
		{"-PT1H30M", 90, true},
		{"-P1D", 1440, true},
		{"-P1W", 10080, true},
		{"-P1DT2H", 1560, true},
		{"-PT45S", 1, true},
		{"PT0M", 0, true},
		{"+PT0S", 0, true},
		{"PT15M", 0, false},
		{"+PT15M", 0, false},
		{"-PT", 0, false},
		{"-P", 0, false},
		{"-PT15", 0, false},
		{"-P15M", 0, false},
		{"-PT1D", 0, false},
		{"20260615T130000Z", 0, false},
		{"-+PT5M", 0, false},
		{"-P1DT", 0, false},
		{"-PT999999999999H", 0, false},
	}
	for _, tt := range tests {
		if minutes, ok := parseTriggerMinutes(tt.value); minutes != tt.minutes || ok != tt.ok {
			t.Errorf("parseTriggerMinutes(%q) = %d, %v, want %d, %v", tt.value, minutes, ok, tt.minutes, tt.ok)
		}
	}
}

// The editor's repeat select offers daily, weekly, monthly and yearly rules,
// and shows any other frequency the series has as an option of its own
// (selectStoredValue), so a series edit that leaves it alone submits the rule
// back, and the overrides of its instances are kept. "Does not repeat" ends
// the series.
func TestSeriesEditKeepsAnHourlyRule(t *testing.T) {
	newYork, err := time.LoadLocation("America/New_York")
	if err != nil {
		t.Skipf("America/New_York is not available: %v", err)
	}
	const rule = "RRULE:FREQ=HOURLY;INTERVAL=2;COUNT=3"
	series := "BEGIN:VEVENT\r\nUID:hourly\r\nDTSTAMP:20260101T000000Z\r\nSUMMARY:Rounds\r\n" +
		"DTSTART;TZID=America/New_York:20260615T100000\r\nDTEND;TZID=America/New_York:20260615T110000\r\n" + rule + "\r\nEND:VEVENT\r\n" +
		"BEGIN:VEVENT\r\nUID:hourly\r\nDTSTAMP:20260101T000000Z\r\nSUMMARY:Late rounds\r\n" +
		"RECURRENCE-ID;TZID=America/New_York:20260615T120000\r\n" +
		"DTSTART;TZID=America/New_York:20260615T130000\r\nDTEND;TZID=America/New_York:20260615T140000\r\nEND:VEVENT\r\n"
	handler, s, events, user := storedEventHandler(t, "hourly", calendarObject(series))
	raw := updateEvent(t, handler, events, user, "hourly", "America/New_York", url.Values{
		"summary": {"Ward rounds"}, "dtstart": {"2026-06-15T10:00"}, "dtend": {"2026-06-15T11:00"},
		"recurrence": {"HOURLY"}, "recurrence_interval": {"2"}, "recurrence_end_type": {"after"}, "recurrence_count": {"3"},
	})
	if master := storedMaster(t, raw); !slices.Contains(master, rule) {
		t.Errorf("stored master lacks %q:\n%s", rule, raw)
	}
	storedOverride(t, raw, "RECURRENCE-ID;TZID=America/New_York:20260615T120000")
	// The override's hour at 13:00 and the last instance's at 14:00 are
	// published as one period.
	adjacent := hourPeriods(t, newYork, "2026-06-15T13:00", "2026-06-15T14:00")
	merged := adjacent[0][:strings.Index(adjacent[0], "/")] + adjacent[1][strings.Index(adjacent[1], "/"):]
	if got, want := davFreeBusyPeriods(t, s, user, "20260601T000000Z", "20260801T000000Z"),
		append(hourPeriods(t, newYork, "2026-06-15T10:00"), merged); !slices.Equal(got, want) {
		t.Errorf("CalDAV free-busy = %v, want %v\n%s", got, want, raw)
	}
}

// Rule part values are case-insensitive (RFC 5545 §3.3.10), and the editors'
// selects hold them upper-cased: a series stored with "FREQ=weekly" shows as
// weekly, not as "Does not repeat", which an edit would take for ending it.
func TestViewModelShowsTheRuleUpperCased(t *testing.T) {
	series := "BEGIN:VEVENT\r\nUID:lower\r\nDTSTAMP:20260101T000000Z\r\nSUMMARY:Standup\r\n" +
		"DTSTART;TZID=America/New_York:20260615T100000\r\nDTEND;TZID=America/New_York:20260615T110000\r\n" +
		"RRULE:FREQ=weekly;byday=mo;COUNT=4\r\nEND:VEVENT\r\n"
	handler, _, _, user := storedEventHandler(t, "lower", calendarObject(series))
	view := calendarViewModel(t, handler, user, "2026-06-01", "2026-06-16")
	if view.RRuleParsed["FREQ"] != "WEEKLY" || view.RRuleParsed["BYDAY"] != "MO" {
		t.Errorf("rruleParsed = %v, want FREQ=WEEKLY and BYDAY=MO", view.RRuleParsed)
	}
}

// An override of an instance and every later one (RFC 5545 §3.8.4.4
// RANGE=THISANDFUTURE) stays one when the occurrence it names is edited.
func TestOccurrenceEditKeepsTheOverridesRange(t *testing.T) {
	newYork, err := time.LoadLocation("America/New_York")
	if err != nil {
		t.Skipf("America/New_York is not available: %v", err)
	}
	series := "BEGIN:VEVENT\r\nUID:ranged\r\nDTSTAMP:20260101T000000Z\r\nSUMMARY:Standup\r\n" +
		"DTSTART;TZID=America/New_York:20260615T100000\r\nDTEND;TZID=America/New_York:20260615T110000\r\n" +
		"RRULE:FREQ=WEEKLY;COUNT=4\r\nEND:VEVENT\r\n" +
		"BEGIN:VEVENT\r\nUID:ranged\r\nDTSTAMP:20260101T000000Z\r\nSUMMARY:Later standup\r\n" +
		"RECURRENCE-ID;RANGE=THISANDFUTURE;TZID=America/New_York:20260622T100000\r\n" +
		"DTSTART;TZID=America/New_York:20260622T110000\r\nDTEND;TZID=America/New_York:20260622T120000\r\nEND:VEVENT\r\n"
	handler, s, events, user := storedEventHandler(t, "ranged", calendarObject(series))
	form := occurrenceEditorForm(t, handler, user, "2026-06-01", "2026-06-16", 0)
	form.Set("summary", "Later standup, renamed")
	form.Set("dtstart", "2026-06-22T11:00")
	form.Set("dtend", "2026-06-22T12:00")
	form.Set("edit_scope", "occurrence")
	form.Set("recurrence_id", "2026-06-22T10:00")
	form.Set("recurrence_timezone", "America/New_York")

	raw := updateEvent(t, handler, events, user, "ranged", "America/New_York", form)
	storedOverride(t, raw, "RECURRENCE-ID;TZID=America/New_York;RANGE=THISANDFUTURE:20260622T100000")
	if got, want := davFreeBusyPeriods(t, s, user, "20260601T000000Z", "20260801T000000Z"),
		hourPeriods(t, newYork, "2026-06-15T10:00", "2026-06-22T11:00", "2026-06-29T11:00", "2026-07-06T11:00"); !slices.Equal(got, want) {
		t.Errorf("CalDAV free-busy = %v, want %v\n%s", got, want, raw)
	}
}

func TestSplitAttachmentList(t *testing.T) {
	tests := []struct {
		value string
		want  []string
	}{
		{"https://example.com/a\nhttps://example.com/b", []string{"https://example.com/a", "https://example.com/b"}},
		{"https://example.com/a, https://example.com/b", []string{"https://example.com/a", "https://example.com/b"}},
		{"https://example.com/doc?ids=1,2", []string{"https://example.com/doc?ids=1,2"}},
		{"https://example.com/doc?ids=1,2,\r\n", []string{"https://example.com/doc?ids=1,2"}},
		{" \n , \n", nil},
	}
	for _, tt := range tests {
		if got := splitAttachmentList(tt.value); !slices.Equal(got, tt.want) {
			t.Errorf("splitAttachmentList(%q) = %q, want %q", tt.value, got, tt.want)
		}
	}
}

func TestSplitICalTextList(t *testing.T) {
	tests := []struct {
		value string
		want  []string
	}{
		{`Work,Planning\, Q3`, []string{"Work", "Planning, Q3"}},
		{`a\\,b`, []string{`a\`, "b"}},
		{`a,b\`, []string{"a", `b\`}},
		{`, ,a`, []string{"a"}},
	}
	for _, tt := range tests {
		if got := splitICalTextList(tt.value); !slices.Equal(got, tt.want) {
			t.Errorf("splitICalTextList(%q) = %q, want %q", tt.value, got, tt.want)
		}
	}
}

// The attendee field is a form value of up to the form's size, and a stored
// event may name tens of thousands of attendees, so reading the field and
// matching it to the stored attendees cost time linear in their number.
func TestAttendeeFieldScalesLinearly(t *testing.T) {
	t.Run("split", func(t *testing.T) {
		assertScalesLinearly(t, 50_000, func(n int) func() {
			field := strings.Repeat("a,", n)
			return func() {
				if got := splitAddressList(field); len(got) != 1 {
					t.Fatalf("split into %d entries, want 1", len(got))
				}
			}
		})
	})
	t.Run("match", func(t *testing.T) {
		assertScalesLinearly(t, 2_000, func(n int) func() {
			previous := make([]string, n)
			submitted := make([]string, n)
			for i := range n {
				previous[i] = "ATTENDEE;PARTSTAT=ACCEPTED;CN=Person " + strconv.Itoa(i) + ":mailto:p" + strconv.Itoa(i) + "@example.com"
				submitted[i] = "Person " + strconv.Itoa(n-1-i) + " <p" + strconv.Itoa(n-1-i) + "@example.com>"
			}
			return func() {
				if got := keptAddresses("ATTENDEE", submitted, previous); len(got) != n || got[0] != previous[n-1] {
					t.Fatalf("kept %d attendees, the first %q", len(got), got[0])
				}
			}
		})
	})
}
