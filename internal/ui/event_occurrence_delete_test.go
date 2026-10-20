package ui

import (
	"net/http"
	"net/http/httptest"
	"net/url"
	"slices"
	"strings"
	"testing"
)

// deleteOccurrence submits the occurrence delete of uid in calendar 1 with
// fields over the version stored now, and returns the stored object.
func deleteOccurrence(t *testing.T, handler *Handler, events *fakeEventRepoWithUpsert, uid string, fields url.Values) string {
	t.Helper()
	fields.Set("etag", storedETag(t, events, uid))
	w := httptest.NewRecorder()
	handler.DeleteEvent(w, deleteOccurrenceRequest(t, uid, fields))
	if location := w.Header().Get("Location"); w.Code != http.StatusFound || !strings.Contains(location, "status=occurrence_deleted") {
		t.Fatalf("DeleteEvent() status = %d location = %s", w.Code, location)
	}
	stored := events.events["1:"+uid]
	// The fake store assigns no ID, and the DAV server pages rows by it.
	stored.ID = 1
	return stored.RawICAL
}

func countLines(raw, prefix string) int {
	count := 0
	for _, line := range strings.Split(raw, "\r\n") {
		if strings.HasPrefix(line, prefix) {
			count++
		}
	}
	return count
}

// A floating series' editor says the occurrence is floating with a marker of
// its own, so a zone typed into the zone field, which is the zone of the
// occurrence's times, never becomes the zone of the RECURRENCE-ID or EXDATE
// naming the series' instance.
func TestFloatingOccurrenceKeepsItsIdentityWhateverZoneIsTyped(t *testing.T) {
	t.Run("delete", func(t *testing.T) {
		handler, s, events, user := storedEventHandler(t, "float", calendarObject(floatingSeries))
		raw := deleteOccurrence(t, handler, events, "float", url.Values{
			"recurrence_id": {"2026-01-07T09:00"}, "recurrence_all_day": {"false"},
			"recurrence_timezone": {""}, "recurrence_floating": {"1"}, "timezone": {"Europe/Berlin"},
		})
		requireFloating(t, raw)
		want := floatingPeriods("05 09", "06 09", "08 09", "09 09")
		if got := davFreeBusyPeriods(t, s, user, "20260101T000000Z", "20260201T000000Z"); !slices.Equal(got, want) {
			t.Errorf("CalDAV free-busy = %v, want %v, without the deleted 7th\n%s", got, want, raw)
		}
	})
	t.Run("edit", func(t *testing.T) {
		handler, s, events, user := storedEventHandler(t, "float", calendarObject(floatingSeries))
		raw := updateEvent(t, handler, events, user, "float", "Europe/Berlin", url.Values{
			"summary": {"Moved"}, "dtstart": {"2026-01-07T11:00"}, "dtend": {"2026-01-07T12:00"},
			"edit_scope": {"occurrence"}, "recurrence_id": {"2026-01-07T09:00"}, "recurrence_all_day": {"false"},
			"recurrence_timezone": {""}, "recurrence_floating": {"1"},
		})
		if !strings.Contains(raw, "RECURRENCE-ID:20260107T090000\r\n") || !strings.Contains(raw, "DTSTART;TZID=Europe/Berlin:20260107T110000\r\n") {
			t.Errorf("the override is not the floating instance's, started in the typed zone:\n%s", raw)
		}
		want := floatingPeriods("05 09", "06 09", "08 09", "09 09")
		want = append(want, "20260107T100000Z/20260107T110000Z")
		slices.Sort(want)
		if got := davFreeBusyPeriods(t, s, user, "20260101T000000Z", "20260201T000000Z"); !slices.Equal(got, want) {
			t.Errorf("CalDAV free-busy = %v, want %v\n%s", got, want, raw)
		}
	})
	t.Run("the marker wins over a zone named beside it", func(t *testing.T) {
		handler, _, events, _ := storedEventHandler(t, "float", calendarObject(floatingSeries))
		raw := deleteOccurrence(t, handler, events, "float", url.Values{
			"recurrence_id": {"2026-01-07T09:00"}, "recurrence_timezone": {"Europe/Berlin"}, "recurrence_floating": {"1"},
		})
		requireFloating(t, raw)
	})
	t.Run("a form that names no occurrence zone uses the form's zone", func(t *testing.T) {
		const zoned = "BEGIN:VEVENT\r\nUID:zoned\r\nDTSTAMP:20260101T000000Z\r\nSUMMARY:Zoned\r\n" +
			"DTSTART;TZID=Europe/Berlin:20260105T090000\r\nDTEND;TZID=Europe/Berlin:20260105T100000\r\nRRULE:FREQ=DAILY;COUNT=5\r\nEND:VEVENT\r\n"
		handler, _, events, _ := storedEventHandler(t, "zoned", calendarObject(zoned))
		raw := deleteOccurrence(t, handler, events, "zoned", url.Values{
			"recurrence_id": {"2026-01-07T09:00"}, "timezone": {"Europe/Berlin"},
		})
		if !strings.Contains(raw, "EXDATE;TZID=Europe/Berlin:20260107T090000\r\n") {
			t.Errorf("the occurrence is not excluded in the form's zone:\n%s", raw)
		}
	})
}

// The slot a RANGE=THISANDFUTURE override is anchored at is deleted by
// excluding it from the series, and the override, whose own instance is the
// slot, moves to the next instance it governs: later instances keep the
// override's content.
func TestDeletingTheSlotOfAThisAndFutureOverrideKeepsItsLaterInstances(t *testing.T) {
	const rule = "RRULE:FREQ=WEEKLY;BYDAY=MO;COUNT=4\r\n"
	object := func(start, rule, override string) string {
		return calendarObject("BEGIN:VEVENT\r\nUID:series\r\nDTSTAMP:20260101T000000Z\r\nSUMMARY:Series\r\nSEQUENCE:2\r\n" +
			start + rule + "END:VEVENT\r\n" +
			"BEGIN:VEVENT\r\nUID:series\r\nDTSTAMP:20260101T000000Z\r\nSUMMARY:Later\r\nSEQUENCE:5\r\n" + override + "END:VEVENT\r\n")
	}
	tests := []struct {
		name   string
		raw    string
		fields url.Values
		// stored lines, and the free-busy periods afterwards.
		lines []string
		want  []string
	}{
		{
			name: "UTC",
			raw: object("DTSTART:20260105T090000Z\r\nDTEND:20260105T100000Z\r\n", rule,
				"RECURRENCE-ID;RANGE=THISANDFUTURE:20260112T090000Z\r\nDTSTART:20260112T110000Z\r\nDTEND:20260112T120000Z\r\n"),
			fields: url.Values{"recurrence_id": {"2026-01-12T09:00"}, "recurrence_timezone": {"UTC"}},
			lines: []string{"EXDATE:20260112T090000Z", "RECURRENCE-ID;RANGE=THISANDFUTURE:20260119T090000Z",
				"DTSTART:20260119T110000Z", "DTEND:20260119T120000Z", "SEQUENCE:3", "SEQUENCE:5"},
			want: []string{"20260105T090000Z/20260105T100000Z", "20260119T110000Z/20260119T120000Z", "20260126T110000Z/20260126T120000Z"},
		},
		{
			name: "floating",
			raw: object("DTSTART:20260105T090000\r\nDTEND:20260105T100000\r\n", rule,
				"RECURRENCE-ID;RANGE=THISANDFUTURE:20260112T090000\r\nDTSTART:20260112T110000\r\nDTEND:20260112T120000\r\n"),
			fields: url.Values{"recurrence_id": {"2026-01-12T09:00"}, "recurrence_timezone": {""}, "recurrence_floating": {"1"}},
			lines: []string{"EXDATE:20260112T090000", "RECURRENCE-ID;RANGE=THISANDFUTURE:20260119T090000",
				"DTSTART:20260119T110000", "DTEND:20260119T120000"},
			want: []string{"20260105T090000Z/20260105T100000Z", "20260119T110000Z/20260119T120000Z", "20260126T110000Z/20260126T120000Z"},
		},
		{
			name: "zoned across a change of offset",
			raw: object("DTSTART;TZID=America/New_York:20260302T090000\r\nDTEND;TZID=America/New_York:20260302T100000\r\n",
				"RRULE:FREQ=WEEKLY;BYDAY=MO;COUNT=4\r\n",
				"RECURRENCE-ID;RANGE=THISANDFUTURE;TZID=America/New_York:20260309T090000\r\nDTSTART;TZID=America/New_York:20260309T110000\r\nDTEND;TZID=America/New_York:20260309T120000\r\n"),
			fields: url.Values{"recurrence_id": {"2026-03-09T09:00"}, "recurrence_timezone": {"America/New_York"}},
			lines: []string{"EXDATE;TZID=America/New_York:20260309T090000", "RECURRENCE-ID;RANGE=THISANDFUTURE;TZID=America/New_York:20260316T090000",
				"DTSTART;TZID=America/New_York:20260316T110000", "DTEND;TZID=America/New_York:20260316T120000"},
			want: []string{"20260302T140000Z/20260302T150000Z", "20260316T150000Z/20260316T160000Z", "20260323T150000Z/20260323T160000Z"},
		},
		{
			name: "an instance replaced by an override of its own is skipped",
			raw: object("DTSTART:20260105T090000Z\r\nDTEND:20260105T100000Z\r\n", rule,
				"RECURRENCE-ID;RANGE=THISANDFUTURE:20260112T090000Z\r\nDTSTART:20260112T110000Z\r\nDTEND:20260112T120000Z\r\nEND:VEVENT\r\n"+
					"BEGIN:VEVENT\r\nUID:series\r\nDTSTAMP:20260101T000000Z\r\nSUMMARY:Own\r\nRECURRENCE-ID:20260119T090000Z\r\nDTSTART:20260119T130000Z\r\nDTEND:20260119T140000Z\r\n"),
			fields: url.Values{"recurrence_id": {"2026-01-12T09:00"}, "recurrence_timezone": {"UTC"}},
			lines:  []string{"RECURRENCE-ID;RANGE=THISANDFUTURE:20260126T090000Z", "RECURRENCE-ID:20260119T090000Z"},
			want:   []string{"20260105T090000Z/20260105T100000Z", "20260119T130000Z/20260119T140000Z", "20260126T110000Z/20260126T120000Z"},
		},
		{
			name: "no later instance",
			raw: object("DTSTART:20260105T090000Z\r\nDTEND:20260105T100000Z\r\n", "RRULE:FREQ=WEEKLY;BYDAY=MO;COUNT=2\r\n",
				"RECURRENCE-ID;RANGE=THISANDFUTURE:20260112T090000Z\r\nDTSTART:20260112T110000Z\r\nDTEND:20260112T120000Z\r\n"),
			fields: url.Values{"recurrence_id": {"2026-01-12T09:00"}, "recurrence_timezone": {"UTC"}},
			lines:  []string{"EXDATE:20260112T090000Z"},
			want:   []string{"20260105T090000Z/20260105T100000Z"},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			handler, s, events, user := storedEventHandler(t, "series", tt.raw)
			raw := deleteOccurrence(t, handler, events, "series", tt.fields)
			for _, line := range tt.lines {
				if !strings.Contains(raw, line+"\r\n") {
					t.Errorf("stored object lacks %q:\n%s", line, raw)
				}
			}
			if got := davFreeBusyPeriods(t, s, user, "20260101T000000Z", "20260401T000000Z"); !slices.Equal(got, tt.want) {
				t.Errorf("CalDAV free-busy = %v, want %v\n%s", got, tt.want, raw)
			}
			if n := countLines(raw, "RECURRENCE-ID"); strings.Contains(tt.name, "no later") && n != 0 {
				t.Errorf("an override with nothing left to govern was kept:\n%s", raw)
			}
		})
	}
}

// An instance an EXDATE already excludes, whatever form that EXDATE takes, is
// not excluded twice, and the series is not revised by a delete that changes
// nothing about it.
func TestOccurrenceDeleteDoesNotExcludeAnInstanceTwice(t *testing.T) {
	series := func(extra string) string {
		return calendarObject("BEGIN:VEVENT\r\nUID:series\r\nDTSTAMP:20260101T000000Z\r\nSUMMARY:Series\r\nSEQUENCE:2\r\n" +
			extra + "RRULE:FREQ=DAILY;COUNT=5\r\nEND:VEVENT\r\n")
	}
	tests := []struct {
		name   string
		raw    string
		fields url.Values
		// exdates is the number of EXDATE lines afterwards, and sequence
		// the series' SEQUENCE.
		exdates  int
		sequence string
	}{
		{
			name:   "in a list",
			raw:    series("DTSTART:20260105T090000Z\r\nDTEND:20260105T100000Z\r\nEXDATE:20260106T090000Z,20260107T090000Z\r\n"),
			fields: url.Values{"recurrence_id": {"2026-01-07T09:00"}, "recurrence_timezone": {"UTC"}}, exdates: 1, sequence: "2",
		},
		{
			name:   "in the zone's other spelling",
			raw:    series("DTSTART;TZID=Europe/Berlin:20260105T100000\r\nDTEND;TZID=Europe/Berlin:20260105T110000\r\nEXDATE;VALUE=DATE-TIME;TZID=\"Europe/Berlin\":20260107T100000\r\n"),
			fields: url.Values{"recurrence_id": {"2026-01-07T09:00"}, "recurrence_timezone": {"UTC"}}, exdates: 1, sequence: "2",
		},
		{
			name:   "floating",
			raw:    series("DTSTART:20260105T090000\r\nDTEND:20260105T100000\r\nEXDATE:20260106T090000,20260107T090000\r\n"),
			fields: url.Values{"recurrence_id": {"2026-01-07T09:00"}, "recurrence_timezone": {""}, "recurrence_floating": {"1"}}, exdates: 1, sequence: "2",
		},
		{
			name:   "by a UTC EXDATE a floating series does not read as the instance",
			raw:    series("DTSTART:20260105T090000\r\nDTEND:20260105T100000\r\nEXDATE:20260107T090000Z\r\n"),
			fields: url.Values{"recurrence_id": {"2026-01-07T09:00"}, "recurrence_timezone": {""}, "recurrence_floating": {"1"}}, exdates: 2, sequence: "3",
		},
		{
			name:   "an instance not yet excluded",
			raw:    series("DTSTART:20260105T090000Z\r\nDTEND:20260105T100000Z\r\nEXDATE:20260106T090000Z,20260108T090000Z\r\n"),
			fields: url.Values{"recurrence_id": {"2026-01-07T09:00"}, "recurrence_timezone": {"UTC"}}, exdates: 2, sequence: "3",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			handler, _, events, _ := storedEventHandler(t, "series", tt.raw)
			raw := deleteOccurrence(t, handler, events, "series", tt.fields)
			if got := countLines(raw, "EXDATE"); got != tt.exdates {
				t.Errorf("stored %d EXDATE lines, want %d:\n%s", got, tt.exdates, raw)
			}
			requireSequences(t, raw, tt.sequence)
		})
	}
}
