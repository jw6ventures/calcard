package ui

import (
	"encoding/json"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"slices"
	"strconv"
	"strings"
	"testing"

	"github.com/jw6ventures/calcard/internal/ical"
	"github.com/jw6ventures/calcard/internal/ui/utils"
)

// repeatFields is what the editor's repeat fields submit when it fills them
// from form and none of them is changed: the weekdays are checkboxes, in the
// page's order.
func repeatFields(form *recurrenceForm) url.Values {
	fields := url.Values{
		"recurrence":            {form.Frequency},
		"recurrence_interval":   {strconv.Itoa(form.Interval)},
		"recurrence_end_type":   {form.End},
		"recurrence_count":      {"10"},
		"recurrence_until":      {form.Until},
		"recurrence_bymonthday": {form.ByMonthDay},
		"recurrence_bymonth":    {""},
	}
	if form.End == "after" {
		fields.Set("recurrence_count", strconv.Itoa(form.Count))
	}
	if form.ByMonth != 0 {
		fields.Set("recurrence_bymonth", strconv.Itoa(form.ByMonth))
	}
	for _, day := range []string{"MO", "TU", "WE", "TH", "FR", "SA", "SU"} {
		if slices.Contains(form.ByDay, day) {
			fields.Add("recurrence_byday", day)
		}
	}
	return fields
}

// ruledSeries is a Monday 10:00 New York series repeating by rule.
func ruledSeries(rule string) string {
	return calendarObject("BEGIN:VEVENT\r\nUID:ruled\r\nDTSTAMP:20260101T000000Z\r\nSUMMARY:Planning\r\n" +
		"DTSTART;TZID=America/New_York:20260615T100000\r\nDTEND;TZID=America/New_York:20260615T110000\r\n" +
		"RRULE:" + rule + "\r\nEND:VEVENT\r\n")
}

// editRuledSeries opens the editor on the stored ruledSeries(rule) and
// submits it with its repeat fields as the editor fills them, changed by
// change, and the title changed. It returns the view model the editor was
// filled from and the stored RRULE.
func editRuledSeries(t *testing.T, rule string, change func(url.Values)) (calendarEventView, string) {
	t.Helper()
	handler, _, events, user := storedEventHandler(t, "ruled", ruledSeries(rule))
	view := calendarViewModel(t, handler, user, "2026-06-01", "2026-07-01")
	if view.RecurrenceForm == nil {
		t.Fatalf("the view model of a series repeating by %s holds no repeat fields", rule)
	}
	form := editorForm(view)
	form.Set("summary", "Renamed")
	form.Set("dtstart", "2026-06-15T10:00")
	form.Set("dtend", "2026-06-15T11:00")
	for key, values := range repeatFields(view.RecurrenceForm) {
		form[key] = values
	}
	if change != nil {
		change(form)
	}
	raw := updateEvent(t, handler, events, user, "ruled", view.Timezone, form)
	property, _ := ical.ComponentProperty(ical.PrimaryVEventComponent(raw), "RRULE")
	return view, property.Value
}

// The repeat fields show a frequency, an interval, an end, weekdays, days of
// the month and one month, and a series' rule may hold more: an ordinal
// weekday, a set position, a week start, a list of months. A title-only edit
// of such a series stores the rule as written, so it falls on the dates it
// did, rather than the rule the fields can write.
func TestSeriesEditKeepsTheRuleItsFieldsShow(t *testing.T) {
	for _, rule := range []string{
		"FREQ=MONTHLY;COUNT=6;BYDAY=2MO",
		"FREQ=MONTHLY;BYDAY=MO,TU,WE,TH,FR;BYSETPOS=-1",
		"FREQ=MONTHLY;BYMONTHDAY=-1",
		"FREQ=MONTHLY;BYMONTHDAY=1,15",
		"FREQ=YEARLY;BYMONTH=3,9;BYMONTHDAY=1",
		"FREQ=YEARLY;BYWEEKNO=20;BYDAY=MO",
		"FREQ=YEARLY;BYYEARDAY=100,200",
		"FREQ=DAILY;BYHOUR=10,17",
		"FREQ=WEEKLY;WKST=SU;INTERVAL=2;BYDAY=MO,WE",
		"FREQ=WEEKLY;INTERVAL=1;BYDAY=MO",
		"freq=weekly;byday=mo,fr",
		"FREQ=HOURLY;INTERVAL=6;COUNT=8",
		// A client's UNTIL at the last instance, and a DATE UNTIL.
		"FREQ=WEEKLY;UNTIL=20260706T140000Z",
		"FREQ=MONTHLY;BYDAY=-1FR;UNTIL=20261231",
	} {
		t.Run(rule, func(t *testing.T) {
			_, stored := editRuledSeries(t, rule, nil)
			if stored != rule {
				t.Errorf("a title-only edit stored RRULE:%s, want RRULE:%s", stored, rule)
			}
		})
	}
}

// A field the edit changes writes its part of the rule, and leaves the parts
// of the rule the fields do not show as they were. A weekday unchecked drops
// its entries and one checked is added plain. A changed frequency writes the
// rule the fields hold.
func TestSeriesEditWritesTheRulePartsItChanged(t *testing.T) {
	tests := []struct {
		name, rule string
		change     func(url.Values)
		want       string
	}{
		{"a changed count", "FREQ=MONTHLY;BYDAY=2MO;COUNT=6", func(f url.Values) { f.Set("recurrence_count", "10") },
			"FREQ=MONTHLY;BYDAY=2MO;COUNT=10"},
		{"a changed interval", "FREQ=MONTHLY;BYDAY=MO,TU,WE,TH,FR;BYSETPOS=-1", func(f url.Values) { f.Set("recurrence_interval", "2") },
			"FREQ=MONTHLY;BYDAY=MO,TU,WE,TH,FR;BYSETPOS=-1;INTERVAL=2"},
		{"an interval back to one", "FREQ=WEEKLY;INTERVAL=2;WKST=SU", func(f url.Values) { f.Set("recurrence_interval", "1") },
			"FREQ=WEEKLY;WKST=SU"},
		{"a weekday unchecked", "FREQ=WEEKLY;BYDAY=MO,WE,FR;WKST=SU", func(f url.Values) { f["recurrence_byday"] = []string{"MO", "FR"} },
			"FREQ=WEEKLY;BYDAY=MO,FR;WKST=SU"},
		{"a weekday checked beside an ordinal one", "FREQ=MONTHLY;BYDAY=2MO", func(f url.Values) { f["recurrence_byday"] = []string{"MO", "TU"} },
			"FREQ=MONTHLY;BYDAY=2MO,TU"},
		{"every weekday unchecked", "FREQ=MONTHLY;BYDAY=2MO;COUNT=3", func(f url.Values) { delete(f, "recurrence_byday") },
			"FREQ=MONTHLY;COUNT=3"},
		{"days of the month changed", "FREQ=MONTHLY;BYMONTHDAY=1,15", func(f url.Values) { f.Set("recurrence_bymonthday", "1, 15, -1") },
			"FREQ=MONTHLY;BYMONTHDAY=1,15,-1"},
		{"a month picked for a list of them", "FREQ=YEARLY;BYMONTH=3,9;BYMONTHDAY=1", func(f url.Values) { f.Set("recurrence_bymonth", "6") },
			"FREQ=YEARLY;BYMONTH=6;BYMONTHDAY=1"},
		{"a count turned into an end date", "FREQ=WEEKLY;COUNT=4;WKST=SU", func(f url.Values) {
			f.Set("recurrence_end_type", "on")
			f.Set("recurrence_until", "2026-07-31")
		}, "FREQ=WEEKLY;WKST=SU;UNTIL=20260801T035959Z"},
		{
			// A set position with nothing left to select from is no rule
			// the server can expand, so the fields write theirs.
			"every weekday of a set position unchecked", "FREQ=MONTHLY;BYDAY=MO,TU,WE,TH,FR;BYSETPOS=-1", func(f url.Values) { delete(f, "recurrence_byday") },
			"FREQ=MONTHLY",
		},
		{"every day of a set position cleared", "FREQ=MONTHLY;BYMONTHDAY=1,15;BYSETPOS=1", func(f url.Values) { f.Set("recurrence_bymonthday", "") },
			"FREQ=MONTHLY"},
		{
			// The weekly fields hide the days of the month, which a weekly
			// rule cannot hold, while the form still submits them.
			"a monthly rule turned weekly", "FREQ=MONTHLY;BYMONTHDAY=1,15", func(f url.Values) {
				f.Set("recurrence", "WEEKLY")
				f["recurrence_byday"] = []string{"MO"}
			}, "FREQ=WEEKLY;BYDAY=MO",
		},
		{"a changed frequency", "FREQ=MONTHLY;BYDAY=2MO;BYSETPOS=1", func(f url.Values) {
			f.Set("recurrence", "WEEKLY")
			f["recurrence_byday"] = []string{"MO"}
		}, "FREQ=WEEKLY;BYDAY=MO"},
		{
			// Moved an hour later, the series' last instance is at 15:00Z on
			// 6 July, after the UNTIL the client wrote at the old one, so the
			// UNTIL is restated as the end of the day the fields show.
			"a series moved past its UNTIL", "FREQ=WEEKLY;UNTIL=20260706T140000Z;WKST=SU", func(f url.Values) {
				f.Set("dtstart", "2026-06-15T11:00")
				f.Set("dtend", "2026-06-15T12:00")
			}, "FREQ=WEEKLY;UNTIL=20260707T035959Z;WKST=SU",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, stored := editRuledSeries(t, tt.rule, tt.change)
			if stored != tt.want {
				t.Errorf("stored RRULE:%s, want RRULE:%s", stored, tt.want)
			}
		})
	}
}

// The view model gives the repeat fields the server compares an edit with,
// which both editors fill their fields from.
func TestViewModelShowsTheRuleInTheEditorsFields(t *testing.T) {
	tests := []struct {
		rule string
		want recurrenceForm
	}{
		{"FREQ=WEEKLY;BYDAY=MO,WE;UNTIL=20260801T035959Z",
			recurrenceForm{Frequency: "WEEKLY", Interval: 1, ByDay: []string{"MO", "WE"}, End: "on", Until: "2026-07-31"}},
		{"FREQ=MONTHLY;COUNT=6;BYDAY=2MO",
			recurrenceForm{Frequency: "MONTHLY", Interval: 1, ByDay: []string{"MO"}, End: "after", Count: 6, Partial: true}},
		{"FREQ=MONTHLY;INTERVAL=3;BYMONTHDAY=-1,15",
			recurrenceForm{Frequency: "MONTHLY", Interval: 3, ByDay: []string{}, ByMonthDay: "-1,15", End: "never"}},
		{"FREQ=YEARLY;BYMONTH=3;BYMONTHDAY=1",
			recurrenceForm{Frequency: "YEARLY", Interval: 1, ByDay: []string{}, ByMonthDay: "1", ByMonth: 3, End: "never"}},
		{"FREQ=YEARLY;BYMONTH=3,9",
			recurrenceForm{Frequency: "YEARLY", Interval: 1, ByDay: []string{}, End: "never", Partial: true}},
		{"freq=hourly;count=8;byminute=0,30",
			recurrenceForm{Frequency: "HOURLY", Interval: 1, ByDay: []string{}, End: "after", Count: 8, Partial: true}},
		{"FREQ=DAILY;BYDAY=MO,TU,WE,TH,FR",
			recurrenceForm{Frequency: "DAILY", Interval: 1, ByDay: []string{"MO", "TU", "WE", "TH", "FR"}, End: "never", Partial: true}},
		{"FREQ=MONTHLY;BYMONTH=3,6;BYMONTHDAY=1",
			recurrenceForm{Frequency: "MONTHLY", Interval: 1, ByDay: []string{}, ByMonthDay: "1", End: "never", Partial: true}},
		{"FREQ=WEEKLY;WKST=SU;INTERVAL=2;BYDAY=SU",
			recurrenceForm{Frequency: "WEEKLY", Interval: 2, ByDay: []string{"SU"}, End: "never", Partial: true}},
		{"FREQ=WEEKLY;WKST=SU;BYDAY=MO,WE",
			recurrenceForm{Frequency: "WEEKLY", Interval: 1, ByDay: []string{"MO", "WE"}, End: "never"}},
		{"FREQ=WEEKLY;WKST=SU;INTERVAL=2;BYDAY=MO,WE",
			recurrenceForm{Frequency: "WEEKLY", Interval: 2, ByDay: []string{"MO", "WE"}, End: "never", Partial: true}},
	}
	for _, tt := range tests {
		t.Run(tt.rule, func(t *testing.T) {
			handler, _, _, user := storedEventHandler(t, "ruled", ruledSeries(tt.rule))
			view := calendarViewModel(t, handler, user, "2026-06-01", "2026-07-01")
			if view.RecurrenceForm == nil || !reflect.DeepEqual(*view.RecurrenceForm, tt.want) {
				t.Errorf("repeat fields = %+v, want %+v", view.RecurrenceForm, tt.want)
			}
		})
	}
}

// The end date field shows the day an UNTIL ends the series on in the
// series' zone, and that day, submitted unchanged into the zone the editor
// submits, writes the UNTIL the editor wrote for it: the day's last second in
// the zone, in UTC, for a timed series, and the DATE for an all-day one.
func TestShownRecurrenceEndsOnTheDayTheEditorWrites(t *testing.T) {
	tests := []struct {
		name, start, until, zone, object string
		allDay                           bool
	}{
		{name: "New York", start: "DTSTART;TZID=America/New_York:20250724T090000", until: "20250801T035959Z", zone: "America/New_York"},
		{name: "New York in winter", start: "DTSTART;TZID=America/New_York:20250309T013000", until: "20250801T035959Z", zone: "America/New_York"},
		{name: "Kolkata", start: "DTSTART;TZID=Asia/Kolkata:20250724T090000", until: "20250731T182959Z", zone: "Asia/Kolkata"},
		{name: "UTC", start: "DTSTART:20250724T090000Z", until: "20250731T235959Z"},
		{name: "a zone nothing defines", start: "DTSTART;TZID=Not/AZone:20250724T233000", until: "20250731T235959Z", zone: "Not/AZone"},
		{name: "a zone the object defines", start: "DTSTART;TZID=\"Eastern Standard Time\":20250724T090000", until: "20250801T035959Z",
			zone: "Eastern Standard Time", object: outlookEastern},
		{name: "all-day", start: "DTSTART;VALUE=DATE:20250724", until: "20250731", allDay: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			master := []string{tt.start, "RRULE:FREQ=WEEKLY;UNTIL=" + tt.until}
			object := utils.UnfoldLines(calendarObject(tt.object + "BEGIN:VEVENT\r\n" + strings.Join(master, "\r\n") + "\r\nEND:VEVENT\r\n"))
			shown, partial, ok := shownRecurrence(master, object)
			if !ok || partial || shown.Until != "2025-07-31" {
				t.Fatalf("shownRecurrence() = %+v, %v, %v, want an end on 2025-07-31", shown, partial, ok)
			}
			start, end := "2025-07-24T09:00", "2025-07-24T10:00"
			if tt.allDay {
				start, end = "2025-07-24", "2025-07-24"
			}
			component, err := utils.BuildEventComponent("ruled", "Ruled", start, end, tt.allDay, "", "",
				&utils.RecurrenceOptions{Frequency: "WEEKLY", Interval: 1, Until: shown.Until}, "",
				&utils.EventOptions{Timezone: tt.zone, Object: object})
			if err != nil {
				t.Fatal(err)
			}
			if want := "RRULE:FREQ=WEEKLY;UNTIL=" + tt.until; !slices.Contains(component, want) {
				t.Errorf("the day shown writes %q, want %s", component, want)
			}
		})
	}
}

// Each editor fills its repeat fields from the view model's, so that an edit
// leaving them as filled submits them as the server shows them
// (repeatFields), and shows a rule it shows only in part beside them.
func TestCalendarViewsFillTheRepeatFieldsTheServerShows(t *testing.T) {
	node := requireNode(t)
	forms := []recurrenceForm{
		{Frequency: "MONTHLY", Interval: 1, ByDay: []string{"MO"}, End: "after", Count: 6, Partial: true},
		{Frequency: "WEEKLY", Interval: 2, ByDay: []string{"MO", "WE", "SU"}, End: "on", Until: "2026-07-31"},
		{Frequency: "MONTHLY", Interval: 1, ByDay: []string{}, ByMonthDay: "-1,15", End: "never"},
		{Frequency: "YEARLY", Interval: 1, ByDay: []string{}, ByMonthDay: "1", ByMonth: 3, End: "never"},
		{Frequency: "HOURLY", Interval: 6, ByDay: []string{}, End: "after", Count: 8},
	}
	encoded, err := json.Marshal(forms)
	if err != nil {
		t.Fatal(err)
	}
	formsFile := filepath.Join(t.TempDir(), "forms.json")
	if err := os.WriteFile(formsFile, encoded, 0o600); err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"calendar_view.html", "all_calendars_view.html"} {
		t.Run(name, func(t *testing.T) {
			cmd := exec.Command(node, filepath.Join("testdata", "recurrence_prefill.mjs"), renderedPage(t, name), formsFile)
			output, err := cmd.CombinedOutput()
			if err != nil {
				t.Fatalf("%v\n%s", err, output)
			}
			var filled []struct {
				Fields  url.Values
				Partial string
			}
			if err := json.Unmarshal(output, &filled); err != nil || len(filled) != len(forms) {
				t.Fatalf("%v: %s", err, output)
			}
			for i, form := range forms {
				want := repeatFields(&form)
				if !reflect.DeepEqual(filled[i].Fields, want) {
					t.Errorf("filled from %+v, the editor submits %v, want %v", form, filled[i].Fields, want)
				}
				if shown := filled[i].Partial != ""; shown != form.Partial || form.Partial && !strings.Contains(filled[i].Partial, "FREQ=TEST") {
					t.Errorf("filled from %+v, the editor notes %q", form, filled[i].Partial)
				}
			}
		})
	}
}
