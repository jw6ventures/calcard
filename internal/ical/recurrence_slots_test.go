package ical

import (
	"testing"
	"time"
)

// VisitRecurrenceInstancesAndSlots answers whether a set generates each slot
// asked about from where the slot starts: the walk the range asks for answers
// those it passes, and those before or past it are looked up, each on its own
// for a rule the walk can start part way through and in one walk from the
// rule's start otherwise, on the work budget the walk was given.
func TestVisitRecurrenceInstancesAndSlotsMatchesSlotsByWhereTheyStart(t *testing.T) {
	at := func(value string) time.Time {
		parsed, err := time.Parse("20060102T150405Z", value)
		if err != nil {
			t.Fatal(err)
		}
		return parsed
	}
	instant := func(value string) RecurrenceSlotID { return RecurrenceSlotID{Instant: at(value)} }
	date := func(value string) RecurrenceSlotID {
		day := at(value + "T000000Z")
		return RecurrenceSlotID{Instant: day, Date: day}
	}
	const (
		yes        = RecurrenceSlotGenerated
		no         = RecurrenceSlotNotGenerated
		unanswered = RecurrenceSlotUnanswered
		notLooked  = RecurrenceSlotNotLookedUp
	)
	dense := "FREQ=YEARLY;BYDAY=MO,TU,WE,TH,FR,SA,SU;BYHOUR=" + enumeratedInts(24) +
		";BYMINUTE=" + enumeratedInts(60) + ";BYSECOND=" + enumeratedInts(60) + ";BYSETPOS=-1"
	for _, tt := range []struct {
		name       string
		master     string
		duration   time.Duration
		from, to   string
		ids        []RecurrenceSlotID
		want       []RecurrenceSlotAnswer
		stopsFirst bool
		// unneeded are the ids not to be looked up when the walk does not
		// pass them.
		unneeded map[int]bool
	}{
		{
			// Each instance lasts four days, so scores of them reach the
			// half hours between their starts, which name no slot.
			name:     "a COUNT rule of long instances",
			master:   "DTSTART:20260301T000000Z\r\nRRULE:FREQ=HOURLY;COUNT=48\r\nEXDATE:20260301T060000Z\r\n",
			duration: 96 * time.Hour,
			from:     "20260301T000000Z", to: "20260302T000000Z",
			ids: []RecurrenceSlotID{
				instant("20260301T050000Z"), instant("20260301T053000Z"), instant("20260301T060000Z"),
				instant("20260302T230000Z"), instant("20260302T233000Z"), instant("20260305T010000Z"),
			},
			want: []RecurrenceSlotAnswer{yes, no, no, yes, no, no},
		},
		{
			name:     "a rule walked from part way through it",
			master:   "DTSTART:20200101T090000Z\r\nRRULE:FREQ=DAILY\r\n",
			duration: time.Hour,
			from:     "20260301T000000Z", to: "20260302T000000Z",
			ids:  []RecurrenceSlotID{instant("20210601T090000Z"), instant("20210601T100000Z"), instant("20260301T090000Z"), instant("20300101T093000Z")},
			want: []RecurrenceSlotAnswer{yes, no, yes, no},
		},
		{
			// The rule is walked from its start for the range, and once more
			// for the slots past it, decades on, whichever it has.
			name:     "a COUNT rule looked up past the range",
			master:   "DTSTART:20000101T000000Z\r\nRRULE:FREQ=MONTHLY;BYDAY=MO,TU,WE,TH,FR,SA,SU;BYSETPOS=1;COUNT=999\r\n",
			duration: time.Hour,
			from:     "20000101T000000Z", to: "20000108T000000Z",
			ids:  []RecurrenceSlotID{instant("20800101T000000Z"), instant("20800101T001700Z"), instant("20500201T000000Z"), instant("20500202T000000Z")},
			want: []RecurrenceSlotAnswer{yes, no, yes, no},
		},
		{
			name:     "a slot not to be looked up",
			master:   "DTSTART:20200101T090000Z\r\nRRULE:FREQ=DAILY\r\n",
			duration: time.Hour,
			from:     "20260301T000000Z", to: "20260302T000000Z",
			ids:      []RecurrenceSlotID{instant("20210601T100000Z"), instant("20210601T110000Z")},
			want:     []RecurrenceSlotAnswer{notLooked, no},
			unneeded: map[int]bool{0: true},
		},
		{
			name:     "a walk the visit stops",
			master:   "DTSTART:20200101T090000Z\r\nRRULE:FREQ=DAILY\r\n",
			duration: time.Hour,
			from:     "20200101T000000Z", to: "20200201T000000Z",
			ids:        []RecurrenceSlotID{instant("20200120T090000Z"), instant("20200120T100000Z")},
			want:       []RecurrenceSlotAnswer{yes, no},
			stopsFirst: true,
		},
		{
			// The rule offers a candidate once a month of minutes, and is
			// followed day by day once its first periods are spent; the
			// visit stops there, at the first instance of the range.
			name:     "a walk the visit stops part way through the rule",
			master:   "DTSTART:19500101T000000Z\r\nRRULE:FREQ=MINUTELY;BYMONTHDAY=1;BYHOUR=0;BYMINUTE=0;COUNT=900\r\n",
			duration: 10 * time.Minute,
			from:     "19510101T000000Z", to: "20000101T000000Z",
			ids:        []RecurrenceSlotID{instant("19520101T000000Z"), instant("19520101T001700Z")},
			want:       []RecurrenceSlotAnswer{yes, no},
			stopsFirst: true,
		},
		{
			name:     "a series starting on a DATE",
			master:   "DTSTART;VALUE=DATE:20260301\r\nRRULE:FREQ=DAILY;INTERVAL=2\r\n",
			duration: 24 * time.Hour,
			from:     "20260301T000000Z", to: "20260310T000000Z",
			ids:  []RecurrenceSlotID{date("20260305"), date("20260306"), date("20270305"), date("20270306")},
			want: []RecurrenceSlotAnswer{yes, no, no, yes},
		},
		{
			name:     "a set of RDATEs",
			master:   "DTSTART:20260301T090000Z\r\nRDATE:20260310T090000Z\r\n",
			duration: time.Hour,
			from:     "20260301T000000Z", to: "20260302T000000Z",
			ids:  []RecurrenceSlotID{instant("20260301T090000Z"), instant("20260310T090000Z"), instant("20260311T090000Z")},
			want: []RecurrenceSlotAnswer{yes, yes, no},
		},
		{
			// The rule is too costly to follow to any slot past its start,
			// so the one asked about is left unanswered.
			name:     "a rule the budget cannot follow",
			master:   "DTSTART:20240101T000000Z\r\nRRULE:" + dense + "\r\n",
			duration: time.Second,
			from:     "20240101T000000Z", to: "20250101T000000Z",
			ids:  []RecurrenceSlotID{instant("20240101T000000Z"), instant("20240101T000001Z")},
			want: []RecurrenceSlotAnswer{yes, unanswered},
		},
		{
			// The week the range asks for is walked, but a walk of every
			// minute of the decade to the slots past it outlasts the budget:
			// the one before where it ran out is answered, the other not.
			name:     "lookups the budget cannot follow",
			master:   "DTSTART:20000101T000000Z\r\nRRULE:FREQ=MINUTELY;BYSECOND=0;COUNT=50000000\r\n",
			duration: 30 * time.Second,
			from:     "20000101T000000Z", to: "20000108T000000Z",
			ids:  []RecurrenceSlotID{instant("20000110T000030Z"), instant("20100101T000000Z")},
			want: []RecurrenceSlotAnswer{no, unanswered},
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			raw := "BEGIN:VCALENDAR\r\nBEGIN:VEVENT\r\nUID:slots\r\n" + tt.master + "END:VEVENT\r\nEND:VCALENDAR\r\n"
			start, ok := ComponentProperty(PrimaryVEventComponent(raw), "DTSTART")
			if !ok {
				t.Fatal("no DTSTART")
			}
			resolved, ok := ParsePropertyDateTimeLocal(start.KeyPart, start.Value)
			if !ok {
				t.Fatal("unreadable DTSTART")
			}
			got, _ := VisitRecurrenceInstancesAndSlots(raw, "VEVENT", resolved, tt.duration, at(tt.from), at(tt.to),
				MaxRecurrenceInstances, nil, tt.ids, func(i int) bool { return !tt.unneeded[i] }, nil, func(RecurrenceInstance) bool { return tt.stopsFirst })
			if len(got) != len(tt.ids) {
				t.Fatalf("answered %d slots, want %d", len(got), len(tt.ids))
			}
			for i, id := range tt.ids {
				if got[i] != tt.want[i] {
					t.Errorf("slot %s answered %d, want %d", id.Instant.Format("20060102T150405Z"), got[i], tt.want[i])
				}
			}
		})
	}
}
