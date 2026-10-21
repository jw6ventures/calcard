package ui

import (
	"maps"
	"net/http"
	"net/http/httptest"
	"net/url"
	"slices"
	"strings"
	"testing"

	"github.com/jw6ventures/calcard/internal/ui/utils"
)

// sequenceSeries is a weekly Monday 09:00 UTC series of four, with the lines
// extra among its properties.
func sequenceSeries(extra string) string {
	return calendarObject("BEGIN:VEVENT\r\nUID:series\r\nDTSTAMP:20260101T000000Z\r\nSUMMARY:Series\r\n" + extra +
		"DTSTART:20260105T090000Z\r\nDTEND:20260105T100000Z\r\nRRULE:FREQ=WEEKLY;BYDAY=MO;COUNT=4\r\nEND:VEVENT\r\n")
}

// seriesForm is what the editor submits for sequenceSeries' series, with
// changes.
func seriesForm(changes url.Values) url.Values {
	form := url.Values{
		"summary": {"Series"}, "dtstart": {"2026-01-05T09:00"}, "dtend": {"2026-01-05T10:00"},
		"recurrence": {"WEEKLY"}, "recurrence_byday": {"MO"}, "recurrence_end_type": {"after"}, "recurrence_count": {"4"},
	}
	for key, values := range changes {
		form[key] = values
	}
	return form
}

// storedSequences are the SEQUENCE values of the stored object's components in
// order, "" for a component with none.
func storedSequences(t *testing.T, raw string) []string {
	t.Helper()
	_, components, _ := utils.SplitComponents(raw)
	var sequences []string
	for _, component := range components {
		sequence := ""
		depth := 0
		for _, line := range component {
			switch {
			case strings.HasPrefix(line, "BEGIN:"):
				depth++
			case strings.HasPrefix(line, "END:"):
				depth--
			case depth == 0 && strings.HasPrefix(line, "SEQUENCE"):
				sequence = line
			}
		}
		sequences = append(sequences, strings.TrimPrefix(sequence, "SEQUENCE:"))
	}
	return sequences
}

func requireSequences(t *testing.T, raw string, want ...string) {
	t.Helper()
	got := storedSequences(t, raw)
	if strings.Join(got, ",") != strings.Join(want, ",") {
		t.Errorf("SEQUENCE of each component = %q, want %q\n%s", got, want, raw)
	}
}

// The organizer's SEQUENCE increases when the schedule of the event is revised
// (RFC 5545 §3.8.7.4): its start, its end or duration, or its repeat rule. A
// missing SEQUENCE counts as zero. An edit that changes nothing of the
// schedule, or restates it in another form that names the same times, keeps it.
func TestSeriesEditIncrementsSequenceOnAScheduleChange(t *testing.T) {
	tests := []struct {
		name   string
		extra  string
		edit   url.Values
		before string
		want   string
	}{
		{"renamed", "SEQUENCE:2\r\n", url.Values{"summary": {"Renamed"}}, "2", "2"},
		{"description changed", "SEQUENCE:2\r\n", url.Values{"description": {"Notes"}}, "2", "2"},
		{"nothing changed and no SEQUENCE", "", url.Values{}, "", ""},
		{"start moved", "SEQUENCE:2\r\n", url.Values{"dtstart": {"2026-01-05T10:00"}, "dtend": {"2026-01-05T11:00"}}, "2", "3"},
		{"start moved and no SEQUENCE", "", url.Values{"dtstart": {"2026-01-06T09:00"}, "dtend": {"2026-01-06T10:00"}}, "", "1"},
		{"end moved", "SEQUENCE:2\r\n", url.Values{"dtend": {"2026-01-05T10:30"}}, "2", "3"},
		{"rule changed", "SEQUENCE:2\r\n", url.Values{"recurrence_count": {"5"}}, "2", "3"},
		{"series ended", "SEQUENCE:2\r\n", url.Values{"recurrence": {""}}, "2", "3"},
		{"made all-day", "SEQUENCE:2\r\n", url.Values{"all_day": {"on"}, "dtstart": {"2026-01-05"}, "dtend": {"2026-01-05"}}, "2", "3"},
		{"moved into another zone", "SEQUENCE:2\r\n", url.Values{"timezone": {"Europe/Berlin"}}, "2", "3"},
		{"SEQUENCE with a parameter", "SEQUENCE;X-A=b:7\r\n", url.Values{"dtend": {"2026-01-05T10:30"}}, "", "8"},
		{"SEQUENCE not a number", "SEQUENCE:x\r\n", url.Values{"dtend": {"2026-01-05T10:30"}}, "x", "1"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			handler, _, events, user := storedEventHandler(t, "series", sequenceSeries(tt.extra))
			zone := "UTC"
			if z := tt.edit.Get("timezone"); z != "" {
				zone = z
			}
			updateEvent(t, handler, events, user, "series", zone, seriesForm(tt.edit))
			requireSequences(t, events.events["1:series"].RawICAL, tt.want)
		})
	}
}

// A schedule the edit restates in another form, naming the same times, is not
// a revision: a DURATION the editor writes as a DTEND, a zone written in
// another spelling, and a floating series that stays floating.
func TestSeriesEditKeepsSequenceWhenTheScheduleIsRestated(t *testing.T) {
	tests := []struct {
		name string
		raw  string
		zone string
		edit url.Values
	}{
		{
			name: "duration written as an end",
			raw: calendarObject("BEGIN:VEVENT\r\nUID:series\r\nDTSTAMP:20260101T000000Z\r\nSUMMARY:Series\r\nSEQUENCE:2\r\n" +
				"DTSTART:20260105T090000Z\r\nDURATION:PT1H\r\nRRULE:FREQ=WEEKLY;BYDAY=MO;COUNT=4\r\nEND:VEVENT\r\n"),
			zone: "UTC", edit: url.Values{},
		},
		{
			name: "zone with a parameter",
			raw: calendarObject("BEGIN:VEVENT\r\nUID:series\r\nDTSTAMP:20260101T000000Z\r\nSUMMARY:Series\r\nSEQUENCE:2\r\n" +
				"DTSTART;VALUE=DATE-TIME;TZID=\"Europe/Berlin\":20260105T090000\r\nDTEND;TZID=Europe/Berlin:20260105T100000\r\n" +
				"RRULE:FREQ=WEEKLY;BYDAY=MO;COUNT=4\r\nEND:VEVENT\r\n"),
			zone: "Europe/Berlin", edit: url.Values{},
		},
		{
			name: "floating series",
			raw: calendarObject("BEGIN:VEVENT\r\nUID:series\r\nDTSTAMP:20260101T000000Z\r\nSUMMARY:Series\r\nSEQUENCE:2\r\n" +
				"DTSTART:20260105T090000\r\nDTEND:20260105T100000\r\nRRULE:FREQ=WEEKLY;BYDAY=MO;COUNT=4\r\nEND:VEVENT\r\n"),
			zone: "", edit: url.Values{"summary": {"Renamed"}},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			handler, _, events, user := storedEventHandler(t, "series", tt.raw)
			updateEvent(t, handler, events, user, "series", tt.zone, seriesForm(tt.edit))
			requireSequences(t, events.events["1:series"].RawICAL, "2")
		})
	}
}

// An occurrence edit revises the override alone: its own SEQUENCE, which
// starts at the series' and increases when the edit moves the occurrence or
// changes its length.
func TestOccurrenceEditIncrementsTheOverridesSequence(t *testing.T) {
	handler, _, events, user := storedEventHandler(t, "series", sequenceSeries("SEQUENCE:2\r\n"))
	occurrence := func(changes url.Values) url.Values {
		form := url.Values{
			"summary": {"Series"}, "dtstart": {"2026-01-12T09:00"}, "dtend": {"2026-01-12T10:00"},
			"edit_scope": {"occurrence"}, "recurrence_id": {"2026-01-12T09:00"}, "recurrence_timezone": {"UTC"},
		}
		for key, values := range changes {
			form[key] = values
		}
		return form
	}
	edit := func(changes url.Values) string {
		return updateEvent(t, handler, events, user, "series", "UTC", occurrence(changes))
	}

	requireSequences(t, edit(url.Values{"summary": {"Renamed"}}), "2", "2")
	requireSequences(t, edit(url.Values{"summary": {"Renamed again"}}), "2", "2")
	requireSequences(t, edit(url.Values{"dtstart": {"2026-01-12T11:00"}, "dtend": {"2026-01-12T12:00"}}), "2", "3")
	requireSequences(t, edit(url.Values{"dtstart": {"2026-01-12T11:00"}, "dtend": {"2026-01-12T12:00"}, "description": {"Notes"}}), "2", "3")
	requireSequences(t, edit(url.Values{"dtstart": {"2026-01-12T11:00"}, "dtend": {"2026-01-12T12:30"}}), "2", "4")
	requireSequences(t, edit(url.Values{"dtstart": {"2026-01-12T11:00"}, "dtend": {"2026-01-12T12:30"}, "summary": {"Last"}}), "2", "4")
}

// An occurrence moved on its first edit starts from the series' SEQUENCE, and
// from zero when the series has none; one only renamed keeps what it copied.
func TestFirstOccurrenceEditStartsFromTheSeriesSequence(t *testing.T) {
	tests := []struct {
		name    string
		extra   string
		changes url.Values
		want    []string
	}{
		{"moved", "SEQUENCE:5\r\n", url.Values{"dtstart": {"2026-01-12T08:00"}, "dtend": {"2026-01-12T09:00"}}, []string{"5", "6"}},
		{"moved without a SEQUENCE", "", url.Values{"dtstart": {"2026-01-12T08:00"}, "dtend": {"2026-01-12T09:00"}}, []string{"", "1"}},
		{"lengthened", "SEQUENCE:5\r\n", url.Values{"dtend": {"2026-01-12T11:00"}}, []string{"5", "6"}},
		{"renamed", "SEQUENCE:5\r\n", url.Values{"summary": {"Renamed"}}, []string{"5", "5"}},
		{"renamed without a SEQUENCE", "", url.Values{"summary": {"Renamed"}}, []string{"", ""}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			handler, _, events, user := storedEventHandler(t, "series", sequenceSeries(tt.extra))
			form := url.Values{
				"summary": {"Series"}, "dtstart": {"2026-01-12T09:00"}, "dtend": {"2026-01-12T10:00"},
				"edit_scope": {"occurrence"}, "recurrence_id": {"2026-01-12T09:00"}, "recurrence_timezone": {"UTC"},
			}
			for key, values := range tt.changes {
				form[key] = values
			}
			requireSequences(t, updateEvent(t, handler, events, user, "series", "UTC", form), tt.want...)
		})
	}
}

// Deleting an occurrence adds an EXDATE to the series, which removes an
// instance from its schedule: a revision of the series.
func TestOccurrenceDeleteIncrementsTheSeriesSequence(t *testing.T) {
	for _, tt := range []struct{ name, extra, want string }{
		{"with a SEQUENCE", "SEQUENCE:2\r\n", "3"},
		{"without a SEQUENCE", "", "1"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			handler, _, events, _ := storedEventHandler(t, "series", sequenceSeries(tt.extra))
			w := httptest.NewRecorder()
			handler.DeleteEvent(w, deleteOccurrenceRequest(t, "series", url.Values{
				"etag": {storedETag(t, events, "series")}, "recurrence_id": {"2026-01-12T09:00"}, "recurrence_timezone": {"UTC"},
			}))
			if location := w.Header().Get("Location"); w.Code != http.StatusFound || !strings.Contains(location, "status=occurrence_deleted") {
				t.Fatalf("DeleteEvent() status = %d location = %s", w.Code, location)
			}
			requireSequences(t, events.events["1:series"].RawICAL, tt.want)
		})
	}
}

// Only the organizer's calendar user increments SEQUENCE (RFC 5545 §3.8.7.4):
// an attendee moving their copy, or deleting an occurrence of it, leaves the
// organizer's SEQUENCE, and the organizer, or the editor of an event that
// names no organizer, increments it.
func TestSequenceIsIncrementedByTheOrganizerAlone(t *testing.T) {
	series := func(organizer string) string {
		return calendarObject("BEGIN:VEVENT\r\nUID:series\r\nDTSTAMP:20260101T000000Z\r\nSUMMARY:Meeting\r\nSEQUENCE:3\r\n" + organizer +
			"ATTENDEE;PARTSTAT=ACCEPTED:mailto:owner@example.com\r\n" +
			"DTSTART:20260105T090000Z\r\nDTEND:20260105T100000Z\r\nRRULE:FREQ=WEEKLY;BYDAY=MO;COUNT=4\r\nEND:VEVENT\r\n")
	}
	tests := []struct {
		name      string
		organizer string
		sequence  string
	}{
		{"organizer is another user", "ORGANIZER;CN=Boss:mailto:boss@example.com\r\n", "3"},
		{"organizer is the editor", "ORGANIZER:MAILTO:Owner@Example.com\r\n", "4"},
		{"no organizer", "", "4"},
	}
	for _, tt := range tests {
		form := seriesForm(url.Values{
			"dtstart": {"2026-01-05T10:00"}, "dtend": {"2026-01-05T11:00"},
			"attendees": {"owner@example.com"},
		})
		switch {
		case strings.Contains(tt.organizer, "boss"):
			form.Set("organizer", "Boss <boss@example.com>")
		case tt.organizer != "":
			form.Set("organizer", "owner@example.com")
		}
		t.Run(tt.name+"/series edit", func(t *testing.T) {
			handler, _, events, user := storedEventHandler(t, "series", series(tt.organizer))
			requireSequences(t, updateEvent(t, handler, events, user, "series", "UTC", form), tt.sequence)
		})
		t.Run(tt.name+"/occurrence edit", func(t *testing.T) {
			handler, _, events, user := storedEventHandler(t, "series", series(tt.organizer))
			occurrence := maps.Clone(form)
			occurrence.Set("dtstart", "2026-01-12T11:00")
			occurrence.Set("dtend", "2026-01-12T12:00")
			occurrence.Set("edit_scope", "occurrence")
			occurrence.Set("recurrence_id", "2026-01-12T09:00")
			occurrence.Set("recurrence_timezone", "UTC")
			requireSequences(t, updateEvent(t, handler, events, user, "series", "UTC", occurrence), "3", tt.sequence)
		})
		t.Run(tt.name+"/occurrence delete", func(t *testing.T) {
			handler, _, events, _ := storedEventHandler(t, "series", series(tt.organizer))
			requireSequences(t, deleteOccurrence(t, handler, events, "series", url.Values{
				"recurrence_id": {"2026-01-12T09:00"}, "recurrence_timezone": {"UTC"},
			}), tt.sequence)
		})
	}
}

// An edit of an instance an override of this and every later instance
// governs starts from that override, which gave the instance its data and its
// SEQUENCE, rather than the series' master: the new override of the instance is
// never below the SEQUENCE the instance carried, a rename keeps it, and
// a move increments it from the schedule the instance had.
func TestEditOfAnInstanceTheThisAndFutureOverrideGovernsStartsFromThatOverride(t *testing.T) {
	object := func(masterSequence string) string {
		return calendarObject("BEGIN:VEVENT\r\nUID:series\r\nDTSTAMP:20260101T000000Z\r\nSUMMARY:Series\r\nSEQUENCE:" + masterSequence + "\r\n" +
			"DTSTART:20260105T090000Z\r\nDTEND:20260105T100000Z\r\nRRULE:FREQ=WEEKLY;BYDAY=MO;COUNT=6\r\nEND:VEVENT\r\n" +
			"BEGIN:VEVENT\r\nUID:series\r\nDTSTAMP:20260101T000000Z\r\nSUMMARY:Later\r\nSEQUENCE:5\r\nDESCRIPTION:Kept\r\n" +
			"RECURRENCE-ID;RANGE=THISANDFUTURE:20260112T090000Z\r\nDTSTART:20260112T110000Z\r\nDTEND:20260112T120000Z\r\nEND:VEVENT\r\n")
	}
	edit := func(changes url.Values) url.Values {
		form := url.Values{
			"summary": {"Later"}, "description": {"Kept"}, "dtstart": {"2026-01-19T11:00"}, "dtend": {"2026-01-19T12:00"},
			"edit_scope": {"occurrence"}, "recurrence_id": {"2026-01-19T09:00"}, "recurrence_timezone": {"UTC"},
		}
		for key, values := range changes {
			form[key] = values
		}
		return form
	}
	tests := []struct {
		name           string
		masterSequence string
		changes        url.Values
		sequence       string
	}{
		{"nothing changed", "2", url.Values{}, "5"},
		{"renamed", "2", url.Values{"summary": {"Renamed"}}, "5"},
		{"moved", "2", url.Values{"dtstart": {"2026-01-19T13:00"}, "dtend": {"2026-01-19T14:00"}}, "6"},
		{"lengthened", "2", url.Values{"dtend": {"2026-01-19T12:30"}}, "6"},
		{"series further revised", "7", url.Values{"summary": {"Renamed"}}, "7"},
		{"series further revised and moved", "7", url.Values{"dtstart": {"2026-01-19T13:00"}, "dtend": {"2026-01-19T14:00"}}, "8"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			handler, _, events, user := storedEventHandler(t, "series", object(tt.masterSequence))
			raw := updateEvent(t, handler, events, user, "series", "UTC", edit(tt.changes))
			got := storedSequences(t, raw)
			if len(got) != 3 || got[0] != tt.masterSequence || got[1] != "5" || got[2] != tt.sequence {
				t.Errorf("SEQUENCE of each component = %q, want the master's %s, the governing override's 5 and %s\n%s", got, tt.masterSequence, tt.sequence, raw)
			}
			_, components, _ := utils.SplitComponents(raw)
			if instance := components[2]; !slices.Contains(instance, "DESCRIPTION:Kept") || slices.ContainsFunc(instance, func(line string) bool { return strings.Contains(line, "RANGE=") }) {
				t.Errorf("the instance's override is not a plain one carrying the governing override's data:\n%s", raw)
			}
		})
	}
}
