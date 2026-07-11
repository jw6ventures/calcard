package dav

import (
	"strings"
	"testing"
	"time"

	"github.com/jw6ventures/calcard/internal/ical"
	"github.com/jw6ventures/calcard/internal/store"
)

func calQueryWithTimeRange(start, end string) *calFilter {
	return &calFilter{
		CompFilter: compFilter{
			Name: "VCALENDAR",
			CompFilter: []compFilter{
				{
					Name:      "VEVENT",
					TimeRange: &timeRange{Start: start, End: end},
				},
			},
		},
	}
}

func TestEffectiveTimeRangeWalksNestedCompFilter(t *testing.T) {
	tr := effectiveTimeRange(calQueryWithTimeRange("20260601T000000Z", "20260701T000000Z"))
	if tr == nil {
		t.Fatal("expected nested VEVENT time-range, got nil")
	}
	if tr.Start != "20260601T000000Z" || tr.End != "20260701T000000Z" {
		t.Fatalf("unexpected bounds: %+v", tr)
	}

	if got := effectiveTimeRange(nil); got != nil {
		t.Fatalf("nil filter should yield nil, got %+v", got)
	}
	noRange := &calFilter{CompFilter: compFilter{Name: "VCALENDAR"}}
	if got := effectiveTimeRange(noRange); got != nil {
		t.Fatalf("filter without time-range should yield nil, got %+v", got)
	}
}

func TestEventFilterFromCalFilter(t *testing.T) {
	t.Run("valid range sets both bounds", func(t *testing.T) {
		ef, ok := eventFilterFromCalFilter(calQueryWithTimeRange("20260601T000000Z", "20260701T000000Z"))
		if !ok {
			t.Fatal("expected ok for valid time-range")
		}
		if ef.Start == nil || ef.End == nil {
			t.Fatalf("expected both bounds set, got %+v", ef)
		}
		if !ef.End.After(*ef.Start) {
			t.Errorf("end %v should be after start %v", ef.End, ef.Start)
		}
	})

	t.Run("open-ended start-only range is usable", func(t *testing.T) {
		ef, ok := eventFilterFromCalFilter(calQueryWithTimeRange("20260601T000000Z", ""))
		if !ok {
			t.Fatal("expected ok for start-only range")
		}
		if ef.Start == nil {
			t.Error("expected Start to be set")
		}
	})

	t.Run("no time-range is not usable", func(t *testing.T) {
		if _, ok := eventFilterFromCalFilter(&calFilter{CompFilter: compFilter{Name: "VCALENDAR"}}); ok {
			t.Error("expected ok=false when no time-range is present")
		}
		if _, ok := eventFilterFromCalFilter(nil); ok {
			t.Error("expected ok=false for nil filter")
		}
	})

	t.Run("invalid range is not usable", func(t *testing.T) {
		if _, ok := eventFilterFromCalFilter(calQueryWithTimeRange("not-a-date", "")); ok {
			t.Error("expected ok=false for unparseable time-range")
		}
	})

	t.Run("non-VEVENT range is not pushed to SQL", func(t *testing.T) {
		filter := &calFilter{
			CompFilter: compFilter{
				Name: "VCALENDAR",
				CompFilter: []compFilter{
					{
						Name:      "VTODO",
						TimeRange: &timeRange{Start: "20260601T000000Z", End: "20260701T000000Z"},
					},
				},
			},
		}
		if _, ok := eventFilterFromCalFilter(filter); ok {
			t.Error("expected ok=false for VTODO time-range pushdown")
		}
	})
}

func TestGenerateFreeBusyExpandsRecurringEventsInRequestedRange(t *testing.T) {
	start := time.Date(2024, 6, 3, 10, 0, 0, 0, time.UTC)
	end := time.Date(2024, 6, 3, 11, 0, 0, 0, time.UTC)
	event := store.Event{
		UID:     "weekly",
		RawICAL: "BEGIN:VCALENDAR\r\nBEGIN:VEVENT\r\nUID:weekly\r\nDTSTART:20240603T100000Z\r\nDTEND:20240603T110000Z\r\nRRULE:FREQ=WEEKLY;COUNT=4\r\nEND:VEVENT\r\nEND:VCALENDAR\r\n",
		DTStart: &start,
		DTEnd:   &end,
	}
	tr := &timeRange{Start: "20240617T000000Z", End: "20240618T000000Z"}

	body := (&DavServer{}).generateFreeBusy([]store.Event{event}, nil, tr)

	if !strings.Contains(body, "FREEBUSY:20240617T100000Z/20240617T110000Z") {
		t.Fatalf("expected recurring busy period inside requested range, got %s", body)
	}
	if strings.Contains(body, "FREEBUSY:20240603T100000Z/20240603T110000Z") {
		t.Fatalf("expected master instance outside requested range to be omitted, got %s", body)
	}
}

func TestGenerateFreeBusyExpandsRRuleByParts(t *testing.T) {
	tests := []struct {
		name     string
		dtstart  string
		dtend    string
		rrule    string
		rangeTR  *timeRange
		wantBusy string
		unwanted string
	}{
		{
			name:     "weekly byday multiple weekdays",
			dtstart:  "20240603T090000Z",
			dtend:    "20240603T100000Z",
			rrule:    "FREQ=WEEKLY;COUNT=6;BYDAY=MO,WE,FR",
			rangeTR:  &timeRange{Start: "20240605T000000Z", End: "20240606T000000Z"},
			wantBusy: "FREEBUSY:20240605T090000Z/20240605T100000Z",
			unwanted: "FREEBUSY:20240610T090000Z/20240610T100000Z",
		},
		{
			name:     "monthly bymonthday skips missing month day",
			dtstart:  "20240131T090000Z",
			dtend:    "20240131T100000Z",
			rrule:    "FREQ=MONTHLY;COUNT=3;BYMONTHDAY=31",
			rangeTR:  &timeRange{Start: "20240331T000000Z", End: "20240401T000000Z"},
			wantBusy: "FREEBUSY:20240331T090000Z/20240331T100000Z",
			unwanted: "FREEBUSY:20240229T090000Z/20240229T100000Z",
		},
		{
			name:     "yearly ordinal byday in month",
			dtstart:  "20240331T090000Z",
			dtend:    "20240331T100000Z",
			rrule:    "FREQ=YEARLY;COUNT=3;BYMONTH=3;BYDAY=-1SU",
			rangeTR:  &timeRange{Start: "20250330T000000Z", End: "20250331T000000Z"},
			wantBusy: "FREEBUSY:20250330T090000Z/20250330T100000Z",
			unwanted: "FREEBUSY:20250331T090000Z/20250331T100000Z",
		},
		{
			name:     "yearly bymonthday without bymonth expands every month",
			dtstart:  "20240110T090000Z",
			dtend:    "20240110T100000Z",
			rrule:    "FREQ=YEARLY;COUNT=3;BYMONTHDAY=10",
			rangeTR:  &timeRange{Start: "20240210T000000Z", End: "20240211T000000Z"},
			wantBusy: "FREEBUSY:20240210T090000Z/20240210T100000Z",
			unwanted: "FREEBUSY:20240410T090000Z/20240410T100000Z",
		},
		{
			name:     "yearly ordinal byday without bymonth applies to year",
			dtstart:  "20241229T090000Z",
			dtend:    "20241229T100000Z",
			rrule:    "FREQ=YEARLY;COUNT=2;BYDAY=-1SU",
			rangeTR:  &timeRange{Start: "20251228T000000Z", End: "20251229T000000Z"},
			wantBusy: "FREEBUSY:20251228T090000Z/20251228T100000Z",
			unwanted: "FREEBUSY:20251221T090000Z/20251221T100000Z",
		},
		{
			name:     "monthly bysetpos",
			dtstart:  "20240607T090000Z",
			dtend:    "20240607T100000Z",
			rrule:    "FREQ=MONTHLY;COUNT=3;BYDAY=FR;BYSETPOS=1",
			rangeTR:  &timeRange{Start: "20240705T000000Z", End: "20240706T000000Z"},
			wantBusy: "FREEBUSY:20240705T090000Z/20240705T100000Z",
			unwanted: "FREEBUSY:20240712T090000Z/20240712T100000Z",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			start, err := ical.ParseDateTime(tt.dtstart)
			if err != nil {
				t.Fatalf("parse dtstart: %v", err)
			}
			end, err := ical.ParseDateTime(tt.dtend)
			if err != nil {
				t.Fatalf("parse dtend: %v", err)
			}
			event := store.Event{
				UID:     tt.name,
				RawICAL: "BEGIN:VCALENDAR\r\nBEGIN:VEVENT\r\nUID:" + tt.name + "\r\nDTSTART:" + tt.dtstart + "\r\nDTEND:" + tt.dtend + "\r\nRRULE:" + tt.rrule + "\r\nEND:VEVENT\r\nEND:VCALENDAR\r\n",
				DTStart: &start,
				DTEnd:   &end,
			}

			h := &DavServer{}
			if !h.eventInTimeRange(event, tt.rangeTR) {
				t.Fatalf("expected event to match range %+v", tt.rangeTR)
			}
			body := h.generateFreeBusy([]store.Event{event}, nil, tt.rangeTR)
			if !strings.Contains(body, tt.wantBusy) {
				t.Fatalf("expected busy period %q, got %s", tt.wantBusy, body)
			}
			if tt.unwanted != "" && strings.Contains(body, tt.unwanted) {
				t.Fatalf("expected unwanted busy period %q to be omitted, got %s", tt.unwanted, body)
			}
		})
	}
}

func TestGenerateFreeBusyExpandsSubDailyRecurringEvents(t *testing.T) {
	tests := []struct {
		name     string
		rrule    string
		rangeTR  *timeRange
		wantBusy string
		unwanted string
		start    time.Time
		end      time.Time
	}{
		{
			name:     "hourly",
			rrule:    "FREQ=HOURLY;INTERVAL=2;COUNT=6",
			rangeTR:  &timeRange{Start: "20240601T040000Z", End: "20240601T050000Z"},
			wantBusy: "FREEBUSY:20240601T040000Z/20240601T041500Z",
			unwanted: "FREEBUSY:20240601T000000Z/20240601T001500Z",
			start:    time.Date(2024, 6, 1, 0, 0, 0, 0, time.UTC),
			end:      time.Date(2024, 6, 1, 0, 15, 0, 0, time.UTC),
		},
		{
			name:     "minutely",
			rrule:    "FREQ=MINUTELY;INTERVAL=30;COUNT=6",
			rangeTR:  &timeRange{Start: "20240601T010000Z", End: "20240601T013000Z"},
			wantBusy: "FREEBUSY:20240601T010000Z/20240601T011000Z",
			unwanted: "FREEBUSY:20240601T000000Z/20240601T001000Z",
			start:    time.Date(2024, 6, 1, 0, 0, 0, 0, time.UTC),
			end:      time.Date(2024, 6, 1, 0, 10, 0, 0, time.UTC),
		},
		{
			name:     "secondly",
			rrule:    "FREQ=SECONDLY;INTERVAL=30;COUNT=6",
			rangeTR:  &timeRange{Start: "20240601T000100Z", End: "20240601T000130Z"},
			wantBusy: "FREEBUSY:20240601T000100Z/20240601T000110Z",
			unwanted: "FREEBUSY:20240601T000000Z/20240601T000010Z",
			start:    time.Date(2024, 6, 1, 0, 0, 0, 0, time.UTC),
			end:      time.Date(2024, 6, 1, 0, 0, 10, 0, time.UTC),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			event := store.Event{
				UID:     tt.name,
				RawICAL: "BEGIN:VCALENDAR\r\nBEGIN:VEVENT\r\nUID:" + tt.name + "\r\nDTSTART:" + tt.start.Format("20060102T150405Z") + "\r\nDTEND:" + tt.end.Format("20060102T150405Z") + "\r\nRRULE:" + tt.rrule + "\r\nEND:VEVENT\r\nEND:VCALENDAR\r\n",
				DTStart: &tt.start,
				DTEnd:   &tt.end,
			}

			body := (&DavServer{}).generateFreeBusy([]store.Event{event}, nil, tt.rangeTR)

			if !strings.Contains(body, tt.wantBusy) {
				t.Fatalf("expected busy period %q, got %s", tt.wantBusy, body)
			}
			if strings.Contains(body, tt.unwanted) {
				t.Fatalf("expected out-of-range master period to be omitted, got %s", body)
			}
		})
	}
}

func TestGenerateFreeBusyExpandsCountedSubDailyBeyondScanLimit(t *testing.T) {
	start := time.Date(2024, 6, 1, 0, 0, 0, 0, time.UTC)
	end := start.Add(10 * time.Second)
	target := start.Add(150000 * time.Second)
	event := store.Event{
		UID:     "counted-secondly-long",
		RawICAL: "BEGIN:VCALENDAR\r\nBEGIN:VEVENT\r\nUID:counted-secondly-long\r\nDTSTART:20240601T000000Z\r\nDTEND:20240601T000010Z\r\nRRULE:FREQ=SECONDLY;COUNT=200000\r\nEND:VEVENT\r\nEND:VCALENDAR\r\n",
		DTStart: &start,
		DTEnd:   &end,
	}
	tr := &timeRange{
		Start: target.Format("20060102T150405Z"),
		End:   target.Add(time.Second).Format("20060102T150405Z"),
	}

	h := &DavServer{}
	if !h.eventInTimeRange(event, tr) {
		t.Fatal("expected counted secondly recurrence beyond scan limit to match requested range")
	}
	body := h.generateFreeBusy([]store.Event{event}, nil, tr)
	want := "FREEBUSY:" + target.Format("20060102T150405Z") + "/" + target.Add(10*time.Second).Format("20060102T150405Z")
	if !strings.Contains(body, want) {
		t.Fatalf("expected busy period %q, got %s", want, body)
	}
}

func TestGenerateFreeBusyExpandsSubDailyByParts(t *testing.T) {
	tests := []struct {
		name     string
		start    time.Time
		end      time.Time
		rrule    string
		rangeTR  *timeRange
		wantBusy string
	}{
		{
			name:     "minutely bysecond",
			start:    time.Date(2024, 6, 1, 0, 0, 0, 0, time.UTC),
			end:      time.Date(2024, 6, 1, 0, 0, 10, 0, time.UTC),
			rrule:    "FREQ=MINUTELY;COUNT=4;BYSECOND=0,30",
			rangeTR:  &timeRange{Start: "20240601T000030Z", End: "20240601T000040Z"},
			wantBusy: "FREEBUSY:20240601T000030Z/20240601T000040Z",
		},
		{
			name:     "hourly byminute",
			start:    time.Date(2024, 6, 1, 0, 0, 0, 0, time.UTC),
			end:      time.Date(2024, 6, 1, 0, 15, 0, 0, time.UTC),
			rrule:    "FREQ=HOURLY;COUNT=4;BYMINUTE=0,30",
			rangeTR:  &timeRange{Start: "20240601T003000Z", End: "20240601T004500Z"},
			wantBusy: "FREEBUSY:20240601T003000Z/20240601T004500Z",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			event := store.Event{
				UID:     tt.name,
				RawICAL: "BEGIN:VCALENDAR\r\nBEGIN:VEVENT\r\nUID:" + tt.name + "\r\nDTSTART:" + tt.start.Format("20060102T150405Z") + "\r\nDTEND:" + tt.end.Format("20060102T150405Z") + "\r\nRRULE:" + tt.rrule + "\r\nEND:VEVENT\r\nEND:VCALENDAR\r\n",
				DTStart: &tt.start,
				DTEnd:   &tt.end,
			}

			h := &DavServer{}
			if !h.eventInTimeRange(event, tt.rangeTR) {
				t.Fatalf("expected event to match range %+v", tt.rangeTR)
			}
			body := h.generateFreeBusy([]store.Event{event}, nil, tt.rangeTR)
			if !strings.Contains(body, tt.wantBusy) {
				t.Fatalf("expected busy period %q, got %s", tt.wantBusy, body)
			}
		})
	}
}

func TestRecurringOverrideMovesInstanceIntoRange(t *testing.T) {
	start := time.Date(2024, 6, 1, 10, 0, 0, 0, time.UTC)
	end := time.Date(2024, 6, 1, 11, 0, 0, 0, time.UTC)
	event := store.Event{
		UID: "override",
		RawICAL: "BEGIN:VCALENDAR\r\nBEGIN:VEVENT\r\nUID:override\r\nDTSTART:20240601T100000Z\r\nDTEND:20240601T110000Z\r\nRRULE:FREQ=DAILY;COUNT=2\r\nEND:VEVENT\r\n" +
			"BEGIN:VEVENT\r\nUID:override\r\nRECURRENCE-ID:20240602T100000Z\r\nDTSTART:20240602T150000Z\r\nDTEND:20240602T160000Z\r\nEND:VEVENT\r\nEND:VCALENDAR\r\n",
		DTStart: &start,
		DTEnd:   &end,
	}

	h := &DavServer{}
	movedRange := &timeRange{Start: "20240602T150000Z", End: "20240602T160000Z"}
	if !h.eventInTimeRange(event, movedRange) {
		t.Fatal("expected override instance to match its moved range")
	}
	body := h.generateFreeBusy([]store.Event{event}, nil, movedRange)
	if !strings.Contains(body, "FREEBUSY:20240602T150000Z/20240602T160000Z") {
		t.Fatalf("expected moved override busy period, got %s", body)
	}

	originalRange := &timeRange{Start: "20240602T100000Z", End: "20240602T110000Z"}
	if h.eventInTimeRange(event, originalRange) {
		t.Fatal("expected overridden original instance to be suppressed")
	}
	originalBody := h.generateFreeBusy([]store.Event{event}, nil, originalRange)
	if strings.Contains(originalBody, "FREEBUSY:20240602T100000Z/20240602T110000Z") {
		t.Fatalf("expected generated original instance to be suppressed, got %s", originalBody)
	}
}

func TestCancelledRecurrenceOverrideSuppressesBusyPeriod(t *testing.T) {
	start := time.Date(2024, 6, 1, 10, 0, 0, 0, time.UTC)
	end := time.Date(2024, 6, 1, 11, 0, 0, 0, time.UTC)
	event := store.Event{
		UID: "cancelled-override",
		RawICAL: "BEGIN:VCALENDAR\r\nBEGIN:VEVENT\r\nUID:cancelled-override\r\nDTSTART:20240601T100000Z\r\nDTEND:20240601T110000Z\r\nRRULE:FREQ=DAILY;COUNT=2\r\nEND:VEVENT\r\n" +
			"BEGIN:VEVENT\r\nUID:cancelled-override\r\nRECURRENCE-ID:20240602T100000Z\r\nSTATUS:CANCELLED\r\nEND:VEVENT\r\nEND:VCALENDAR\r\n",
		DTStart: &start,
		DTEnd:   &end,
	}

	tr := &timeRange{Start: "20240602T000000Z", End: "20240603T000000Z"}
	h := &DavServer{}
	if h.eventInTimeRange(event, tr) {
		t.Fatal("expected cancelled recurrence override not to match range")
	}
	body := h.generateFreeBusy([]store.Event{event}, nil, tr)
	if strings.Contains(body, "FREEBUSY:20240602T100000Z/20240602T110000Z") {
		t.Fatalf("expected cancelled recurrence override to be omitted, got %s", body)
	}
}

func TestRecurringTimeRangeHonorsRDateAndExDate(t *testing.T) {
	start := time.Date(2024, 6, 1, 9, 0, 0, 0, time.UTC)
	end := time.Date(2024, 6, 1, 10, 0, 0, 0, time.UTC)
	rdateEvent := store.Event{
		UID:     "rdate",
		RawICAL: "BEGIN:VCALENDAR\r\nBEGIN:VEVENT\r\nUID:rdate\r\nDTSTART:20240601T090000Z\r\nDTEND:20240601T100000Z\r\nRDATE:20240605T090000Z\r\nEND:VEVENT\r\nEND:VCALENDAR\r\n",
		DTStart: &start,
		DTEnd:   &end,
	}
	tr := &timeRange{Start: "20240605T000000Z", End: "20240606T000000Z"}

	h := &DavServer{}
	if !h.eventInTimeRange(rdateEvent, tr) {
		t.Fatal("expected RDATE instance to match requested range")
	}
	body := h.generateFreeBusy([]store.Event{rdateEvent}, nil, tr)
	if !strings.Contains(body, "FREEBUSY:20240605T090000Z/20240605T100000Z") {
		t.Fatalf("expected RDATE busy period, got %s", body)
	}

	exdateEvent := store.Event{
		UID:     "exdate",
		RawICAL: "BEGIN:VCALENDAR\r\nBEGIN:VEVENT\r\nUID:exdate\r\nDTSTART:20240601T090000Z\r\nDTEND:20240601T100000Z\r\nRRULE:FREQ=DAILY;COUNT=3\r\nEXDATE:20240602T090000Z\r\nEND:VEVENT\r\nEND:VCALENDAR\r\n",
		DTStart: &start,
		DTEnd:   &end,
	}
	exdateBody := h.generateFreeBusy([]store.Event{exdateEvent}, nil, &timeRange{Start: "20240602T000000Z", End: "20240603T000000Z"})
	if strings.Contains(exdateBody, "FREEBUSY:20240602T090000Z/20240602T100000Z") {
		t.Fatalf("expected EXDATE instance to be omitted, got %s", exdateBody)
	}
}

func TestGenerateFreeBusyExpandsYearlyByWeekNo(t *testing.T) {
	start := time.Date(2024, 1, 1, 9, 0, 0, 0, time.UTC)
	end := time.Date(2024, 1, 1, 10, 0, 0, 0, time.UTC)
	event := store.Event{
		UID:     "byweekno",
		RawICAL: "BEGIN:VCALENDAR\r\nBEGIN:VEVENT\r\nUID:byweekno\r\nDTSTART:20240101T090000Z\r\nDTEND:20240101T100000Z\r\nRRULE:FREQ=YEARLY;COUNT=2;BYWEEKNO=20;BYDAY=MO\r\nEND:VEVENT\r\nEND:VCALENDAR\r\n",
		DTStart: &start,
		DTEnd:   &end,
	}

	h := &DavServer{}
	tr := &timeRange{Start: "20240513T000000Z", End: "20240514T000000Z"}
	if !h.eventInTimeRange(event, tr) {
		t.Fatal("expected BYWEEKNO recurrence to match requested week")
	}
	body := h.generateFreeBusy([]store.Event{event}, nil, tr)
	if !strings.Contains(body, "FREEBUSY:20240513T090000Z/20240513T100000Z") {
		t.Fatalf("expected BYWEEKNO busy period, got %s", body)
	}
	if strings.Contains(body, "FREEBUSY:20240101T090000Z/20240101T100000Z") {
		t.Fatalf("expected DTSTART outside requested BYWEEKNO range to be omitted, got %s", body)
	}
}

func TestGenerateFreeBusyExpandsMonthlyAndYearlyRecurringEvents(t *testing.T) {
	tests := []struct {
		name     string
		rrule    string
		rangeTR  *timeRange
		wantBusy string
		start    time.Time
		end      time.Time
	}{
		{
			name:     "monthly interval",
			rrule:    "FREQ=MONTHLY;INTERVAL=2;COUNT=4",
			rangeTR:  &timeRange{Start: "20240501T000000Z", End: "20240502T000000Z"},
			wantBusy: "FREEBUSY:20240501T090000Z/20240501T100000Z",
			start:    time.Date(2024, 1, 1, 9, 0, 0, 0, time.UTC),
			end:      time.Date(2024, 1, 1, 10, 0, 0, 0, time.UTC),
		},
		{
			name:     "yearly interval",
			rrule:    "FREQ=YEARLY;INTERVAL=2;COUNT=3",
			rangeTR:  &timeRange{Start: "20280601T000000Z", End: "20280602T000000Z"},
			wantBusy: "FREEBUSY:20280601T090000Z/20280601T100000Z",
			start:    time.Date(2024, 6, 1, 9, 0, 0, 0, time.UTC),
			end:      time.Date(2024, 6, 1, 10, 0, 0, 0, time.UTC),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			event := store.Event{
				UID:     tt.name,
				RawICAL: "BEGIN:VCALENDAR\r\nBEGIN:VEVENT\r\nUID:" + tt.name + "\r\nDTSTART:" + tt.start.Format("20060102T150405Z") + "\r\nDTEND:" + tt.end.Format("20060102T150405Z") + "\r\nRRULE:" + tt.rrule + "\r\nEND:VEVENT\r\nEND:VCALENDAR\r\n",
				DTStart: &tt.start,
				DTEnd:   &tt.end,
			}

			body := (&DavServer{}).generateFreeBusy([]store.Event{event}, nil, tt.rangeTR)
			if !strings.Contains(body, tt.wantBusy) {
				t.Fatalf("expected busy period %q, got %s", tt.wantBusy, body)
			}
		})
	}
}

func TestGenerateFreeBusyHonorsRecurrenceUntil(t *testing.T) {
	start := time.Date(2024, 6, 1, 9, 0, 0, 0, time.UTC)
	end := time.Date(2024, 6, 1, 10, 0, 0, 0, time.UTC)
	event := store.Event{
		UID:     "until",
		RawICAL: "BEGIN:VCALENDAR\r\nBEGIN:VEVENT\r\nUID:until\r\nDTSTART:20240601T090000Z\r\nDTEND:20240601T100000Z\r\nRRULE:FREQ=DAILY;UNTIL=20240603T090000Z\r\nEND:VEVENT\r\nEND:VCALENDAR\r\n",
		DTStart: &start,
		DTEnd:   &end,
	}

	inRangeBody := (&DavServer{}).generateFreeBusy([]store.Event{event}, nil, &timeRange{Start: "20240603T000000Z", End: "20240604T000000Z"})
	if !strings.Contains(inRangeBody, "FREEBUSY:20240603T090000Z/20240603T100000Z") {
		t.Fatalf("expected last UNTIL instance, got %s", inRangeBody)
	}

	afterUntilBody := (&DavServer{}).generateFreeBusy([]store.Event{event}, nil, &timeRange{Start: "20240604T000000Z", End: "20240605T000000Z"})
	if strings.Contains(afterUntilBody, "FREEBUSY:") {
		t.Fatalf("expected no instances after UNTIL, got %s", afterUntilBody)
	}
}

func TestUnsupportedRecurrenceFrequencyIsPermissiveForFiltering(t *testing.T) {
	start := time.Date(2024, 6, 1, 9, 0, 0, 0, time.UTC)
	end := time.Date(2024, 6, 1, 10, 0, 0, 0, time.UTC)
	event := store.Event{
		UID:     "unsupported",
		RawICAL: "BEGIN:VCALENDAR\r\nBEGIN:VEVENT\r\nUID:unsupported\r\nDTSTART:20240601T090000Z\r\nDTEND:20240601T100000Z\r\nRRULE:FREQ=NOPE;COUNT=2\r\nEND:VEVENT\r\nEND:VCALENDAR\r\n",
		DTStart: &start,
		DTEnd:   &end,
	}

	if !(&DavServer{}).eventInTimeRange(event, &timeRange{Start: "20250101T000000Z", End: "20250102T000000Z"}) {
		t.Fatal("expected unsupported recurrence frequency to be permissively included by filtering")
	}
	if got := (&DavServer{}).generateFreeBusy([]store.Event{event}, nil, &timeRange{Start: "20250101T000000Z", End: "20250102T000000Z"}); strings.Contains(got, "FREEBUSY:") {
		t.Fatalf("expected unsupported recurrence expansion to produce no invented periods, got %s", got)
	}
}

func TestMalformedRRuleFallsBackToMasterInstance(t *testing.T) {
	start := time.Date(2024, 6, 1, 9, 0, 0, 0, time.UTC)
	end := time.Date(2024, 6, 1, 10, 0, 0, 0, time.UTC)
	event := store.Event{
		UID:     "malformed",
		RawICAL: "BEGIN:VCALENDAR\r\nBEGIN:VEVENT\r\nUID:malformed\r\nDTSTART:20240601T090000Z\r\nDTEND:20240601T100000Z\r\nRRULE:FREQ=DAILY;COUNT=bogus\r\nEND:VEVENT\r\nEND:VCALENDAR\r\n",
		DTStart: &start,
		DTEnd:   &end,
	}

	h := &DavServer{}
	masterRange := &timeRange{Start: "20240601T093000Z", End: "20240601T094500Z"}
	if !h.eventInTimeRange(event, masterRange) {
		t.Fatal("expected malformed recurrence to fall back to the master instance")
	}
	body := h.generateFreeBusy([]store.Event{event}, nil, masterRange)
	if !strings.Contains(body, "FREEBUSY:20240601T090000Z/20240601T100000Z") {
		t.Fatalf("expected master busy period for malformed RRULE, got %s", body)
	}

	laterRange := &timeRange{Start: "20240602T093000Z", End: "20240602T094500Z"}
	if h.eventInTimeRange(event, laterRange) {
		t.Fatal("expected malformed recurrence not to invent future instances")
	}
}

func TestRecurringDurationDefinesBusyPeriodEnd(t *testing.T) {
	start := time.Date(2024, 6, 1, 9, 0, 0, 0, time.UTC)
	event := store.Event{
		UID:     "duration",
		RawICAL: "BEGIN:VCALENDAR\r\nBEGIN:VEVENT\r\nUID:duration\r\nDTSTART:20240601T090000Z\r\nDURATION:PT2H\r\nRRULE:FREQ=DAILY;COUNT=2\r\nEND:VEVENT\r\nEND:VCALENDAR\r\n",
		DTStart: &start,
	}

	h := &DavServer{}
	tr := &timeRange{Start: "20240602T103000Z", End: "20240602T104500Z"}
	if !h.eventInTimeRange(event, tr) {
		t.Fatal("expected DURATION-backed recurrence to overlap the requested range")
	}
	body := h.generateFreeBusy([]store.Event{event}, nil, tr)
	if !strings.Contains(body, "FREEBUSY:20240602T090000Z/20240602T110000Z") {
		t.Fatalf("expected DURATION to set recurring busy period end, got %s", body)
	}
}

func TestRDatePeriodWithExplicitEnd(t *testing.T) {
	start := time.Date(2024, 6, 1, 9, 0, 0, 0, time.UTC)
	event := store.Event{
		UID:     "rdate-period",
		RawICAL: "BEGIN:VCALENDAR\r\nBEGIN:VEVENT\r\nUID:rdate-period\r\nDTSTART:20240601T090000Z\r\nRDATE;VALUE=PERIOD:20240605T090000Z/20240605T113000Z\r\nEND:VEVENT\r\nEND:VCALENDAR\r\n",
		DTStart: &start,
	}

	body := (&DavServer{}).generateFreeBusy([]store.Event{event}, nil, &timeRange{Start: "20240605T000000Z", End: "20240606T000000Z"})
	if !strings.Contains(body, "FREEBUSY:20240605T090000Z/20240605T113000Z") {
		t.Fatalf("expected RDATE period explicit end, got %s", body)
	}
}

func TestRDatePeriodWithDuration(t *testing.T) {
	start := time.Date(2024, 6, 1, 9, 0, 0, 0, time.UTC)
	event := store.Event{
		UID:     "rdate-duration",
		RawICAL: "BEGIN:VCALENDAR\r\nBEGIN:VEVENT\r\nUID:rdate-duration\r\nDTSTART:20240601T090000Z\r\nRDATE;VALUE=PERIOD:20240605T090000Z/PT2H30M\r\nEND:VEVENT\r\nEND:VCALENDAR\r\n",
		DTStart: &start,
	}

	body := (&DavServer{}).generateFreeBusy([]store.Event{event}, nil, &timeRange{Start: "20240605T110000Z", End: "20240605T120000Z"})
	if !strings.Contains(body, "FREEBUSY:20240605T090000Z/20240605T113000Z") {
		t.Fatalf("expected RDATE duration period to be expanded to an absolute end, got %s", body)
	}
}

func TestRDateAndExDateHonorTZID(t *testing.T) {
	start := time.Date(2024, 6, 1, 13, 0, 0, 0, time.UTC)
	end := time.Date(2024, 6, 1, 14, 0, 0, 0, time.UTC)
	rdateEvent := store.Event{
		UID:     "rdate-tzid",
		RawICAL: "BEGIN:VCALENDAR\r\nBEGIN:VEVENT\r\nUID:rdate-tzid\r\nDTSTART;TZID=America/New_York:20240601T090000\r\nDTEND;TZID=America/New_York:20240601T100000\r\nRDATE;TZID=America/New_York:20240605T090000\r\nEND:VEVENT\r\nEND:VCALENDAR\r\n",
		DTStart: &start,
		DTEnd:   &end,
	}

	body := (&DavServer{}).generateFreeBusy([]store.Event{rdateEvent}, nil, &timeRange{Start: "20240605T000000Z", End: "20240606T000000Z"})
	if !strings.Contains(body, "FREEBUSY:20240605T130000Z/20240605T140000Z") {
		t.Fatalf("expected TZID RDATE converted to UTC, got %s", body)
	}

	exdateEvent := store.Event{
		UID:     "exdate-tzid",
		RawICAL: "BEGIN:VCALENDAR\r\nBEGIN:VEVENT\r\nUID:exdate-tzid\r\nDTSTART;TZID=America/New_York:20240601T090000\r\nDTEND;TZID=America/New_York:20240601T100000\r\nRRULE:FREQ=DAILY;COUNT=2\r\nEXDATE;TZID=America/New_York:20240602T090000\r\nEND:VEVENT\r\nEND:VCALENDAR\r\n",
		DTStart: &start,
		DTEnd:   &end,
	}
	exdateBody := (&DavServer{}).generateFreeBusy([]store.Event{exdateEvent}, nil, &timeRange{Start: "20240602T000000Z", End: "20240603T000000Z"})
	if strings.Contains(exdateBody, "FREEBUSY:20240602T130000Z/20240602T140000Z") {
		t.Fatalf("expected TZID EXDATE instance to be omitted, got %s", exdateBody)
	}
}

func TestRecurringAllDayWithoutDTEndUsesOneDayDuration(t *testing.T) {
	start := time.Date(2024, 6, 1, 0, 0, 0, 0, time.UTC)
	event := store.Event{
		UID:     "all-day",
		RawICAL: "BEGIN:VCALENDAR\r\nBEGIN:VEVENT\r\nUID:all-day\r\nDTSTART;VALUE=DATE:20240601\r\nRRULE:FREQ=DAILY;COUNT=2\r\nEND:VEVENT\r\nEND:VCALENDAR\r\n",
		DTStart: &start,
		AllDay:  true,
	}

	h := &DavServer{}
	tr := &timeRange{Start: "20240602T120000Z", End: "20240602T130000Z"}
	if !h.eventInTimeRange(event, tr) {
		t.Fatal("expected recurring all-day event to overlap the afternoon of the generated day")
	}
	body := h.generateFreeBusy([]store.Event{event}, nil, tr)
	if !strings.Contains(body, "FREEBUSY:20240602T000000Z/20240603T000000Z") {
		t.Fatalf("expected all-day recurrence to use a one-day busy period, got %s", body)
	}
}

func TestThisAndFutureOverrideShiftsFollowingInstances(t *testing.T) {
	start := time.Date(2024, 6, 1, 9, 0, 0, 0, time.UTC)
	end := time.Date(2024, 6, 1, 10, 0, 0, 0, time.UTC)
	event := store.Event{
		UID: "this-and-future",
		RawICAL: "BEGIN:VCALENDAR\r\nBEGIN:VEVENT\r\nUID:this-and-future\r\nDTSTART:20240601T090000Z\r\nDTEND:20240601T100000Z\r\nRRULE:FREQ=DAILY;COUNT=4\r\nEND:VEVENT\r\n" +
			"BEGIN:VEVENT\r\nUID:this-and-future\r\nRECURRENCE-ID;RANGE=THISANDFUTURE:20240602T090000Z\r\nDTSTART:20240602T150000Z\r\nDTEND:20240602T160000Z\r\nEND:VEVENT\r\nEND:VCALENDAR\r\n",
		DTStart: &start,
		DTEnd:   &end,
	}

	h := &DavServer{}
	tr := &timeRange{Start: "20240604T150000Z", End: "20240604T160000Z"}
	if !h.eventInTimeRange(event, tr) {
		t.Fatal("expected RANGE=THISANDFUTURE override to shift later generated instances")
	}
	body := h.generateFreeBusy([]store.Event{event}, nil, tr)
	if !strings.Contains(body, "FREEBUSY:20240604T150000Z/20240604T160000Z") {
		t.Fatalf("expected shifted future busy period, got %s", body)
	}
	if strings.Contains(body, "FREEBUSY:20240604T090000Z/20240604T100000Z") {
		t.Fatalf("expected original future instance to be suppressed, got %s", body)
	}
}

func TestCancelledThisAndFutureOverrideSuppressesFollowingInstances(t *testing.T) {
	start := time.Date(2024, 6, 1, 9, 0, 0, 0, time.UTC)
	end := time.Date(2024, 6, 1, 10, 0, 0, 0, time.UTC)
	event := store.Event{
		UID: "cancel-this-and-future",
		RawICAL: "BEGIN:VCALENDAR\r\nBEGIN:VEVENT\r\nUID:cancel-this-and-future\r\nDTSTART:20240601T090000Z\r\nDTEND:20240601T100000Z\r\nRRULE:FREQ=DAILY;COUNT=4\r\nEND:VEVENT\r\n" +
			"BEGIN:VEVENT\r\nUID:cancel-this-and-future\r\nRECURRENCE-ID;RANGE=THISANDFUTURE:20240602T090000Z\r\nSTATUS:CANCELLED\r\nEND:VEVENT\r\nEND:VCALENDAR\r\n",
		DTStart: &start,
		DTEnd:   &end,
	}

	h := &DavServer{}
	tr := &timeRange{Start: "20240603T090000Z", End: "20240603T100000Z"}
	if h.eventInTimeRange(event, tr) {
		t.Fatal("expected RANGE=THISANDFUTURE cancellation to suppress following generated instances")
	}
	body := h.generateFreeBusy([]store.Event{event}, nil, tr)
	if strings.Contains(body, "FREEBUSY:20240603T090000Z/20240603T100000Z") {
		t.Fatalf("expected cancelled future busy period to be omitted, got %s", body)
	}
}

func TestLowercaseRRuleIsExpanded(t *testing.T) {
	start := time.Date(2024, 6, 1, 9, 0, 0, 0, time.UTC)
	end := time.Date(2024, 6, 1, 10, 0, 0, 0, time.UTC)
	event := store.Event{
		UID:     "lowercase",
		RawICAL: "BEGIN:VCALENDAR\r\nBEGIN:VEVENT\r\nUID:lowercase\r\nDTSTART:20240601T090000Z\r\nDTEND:20240601T100000Z\r\nrrule:FREQ=DAILY;COUNT=2\r\nEND:VEVENT\r\nEND:VCALENDAR\r\n",
		DTStart: &start,
		DTEnd:   &end,
	}

	body := (&DavServer{}).generateFreeBusy([]store.Event{event}, nil, &timeRange{Start: "20240602T000000Z", End: "20240603T000000Z"})
	if !strings.Contains(body, "FREEBUSY:20240602T090000Z/20240602T100000Z") {
		t.Fatalf("expected lowercase rrule to be expanded, got %s", body)
	}
}

func TestRecurrenceParsingIsScopedToVEvent(t *testing.T) {
	start := time.Date(2024, 6, 1, 9, 0, 0, 0, time.UTC)
	end := time.Date(2024, 6, 1, 10, 0, 0, 0, time.UTC)
	event := store.Event{
		UID: "timezone-rule",
		RawICAL: "BEGIN:VCALENDAR\r\nBEGIN:VTIMEZONE\r\nTZID:America/New_York\r\n" +
			"BEGIN:DAYLIGHT\r\nDTSTART:19870405T020000\r\nRRULE:FREQ=YEARLY;BYMONTH=4;BYDAY=1SU\r\nEND:DAYLIGHT\r\nEND:VTIMEZONE\r\n" +
			"BEGIN:VEVENT\r\nUID:timezone-rule\r\nDTSTART:20240601T090000Z\r\nDTEND:20240601T100000Z\r\nEND:VEVENT\r\nEND:VCALENDAR\r\n",
		DTStart: &start,
		DTEnd:   &end,
	}

	h := &DavServer{}
	tr := &timeRange{Start: "20250601T000000Z", End: "20250602T000000Z"}
	if h.eventInTimeRange(event, tr) {
		t.Fatal("expected VTIMEZONE RRULE not to make a non-recurring VEVENT match")
	}
	body := h.generateFreeBusy([]store.Event{event}, nil, tr)
	if strings.Contains(body, "FREEBUSY:20250601T090000Z/20250601T100000Z") {
		t.Fatalf("expected no invented busy period from VTIMEZONE RRULE, got %s", body)
	}
}
