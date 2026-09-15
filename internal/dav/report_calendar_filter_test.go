package dav

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/jw6ventures/calcard/internal/auth"
	"github.com/jw6ventures/calcard/internal/ical"
	"github.com/jw6ventures/calcard/internal/store"
)

// resourceInTimeRange drives matcherInTimeRange from stored octets, so a case
// can state the resource it is about rather than the parse the report path
// carries between its passes.
func resourceInTimeRange(event store.Event, tr *timeRange, zone floatingZone) bool {
	matcher, _ := newEventTimeRangeMatcher(event, zone)
	return matcherInTimeRange(matcher, tr)
}

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

func calQueryWithPropFilter(pf propFilter) *calFilter {
	return &calFilter{
		CompFilter: compFilter{
			Name: "VCALENDAR",
			CompFilter: []compFilter{
				{
					Name:       "VEVENT",
					PropFilter: []propFilter{pf},
				},
			},
		},
	}
}

// RFC 4791 §9.7.2 matches a prop-filter against the property name only. A
// substring search for "NAME:" misses parameterized lines and lets a prefixed
// extension property satisfy the wrong filter.
func TestPropFilterMatchesExactPropertyName(t *testing.T) {
	tests := []struct {
		name       string
		body       string
		propFilter propFilter
		want       bool
	}{
		{
			name:       "parameterized property matches",
			body:       "DTSTART;TZID=America/New_York:20240601T090000\r\n",
			propFilter: propFilter{Name: "DTSTART"},
			want:       true,
		},
		{
			name:       "plain property matches",
			body:       "SUMMARY:Standup\r\n",
			propFilter: propFilter{Name: "SUMMARY"},
			want:       true,
		},
		{
			name:       "prefixed extension property does not match",
			body:       "X-SUMMARY:Standup\r\n",
			propFilter: propFilter{Name: "SUMMARY"},
			want:       false,
		},
		{
			name:       "suffixed extension property does not match",
			body:       "SUMMARY-ALT:Standup\r\n",
			propFilter: propFilter{Name: "SUMMARY"},
			want:       false,
		},
		{
			name:       "filter name is case-insensitive",
			body:       "SUMMARY:Standup\r\n",
			propFilter: propFilter{Name: "summary"},
			want:       true,
		},
		{
			name:       "property name is case-insensitive",
			body:       "summary:Standup\r\n",
			propFilter: propFilter{Name: "SUMMARY"},
			want:       true,
		},
		{
			name:       "folded property name matches once unfolded",
			body:       "X-CALCARD-LONG-PROPERTY-NAM\r\n E:value\r\n",
			propFilter: propFilter{Name: "X-CALCARD-LONG-PROPERTY-NAME"},
			want:       true,
		},
		{
			name:       "property-looking text inside a folded value does not match",
			body:       "DESCRIPTION:agenda\r\n SUMMARY:not a property\r\n",
			propFilter: propFilter{Name: "SUMMARY"},
			want:       false,
		},
		{
			name:       "property-looking text inside a value does not match",
			body:       "DESCRIPTION:see SUMMARY:elsewhere\r\n",
			propFilter: propFilter{Name: "SUMMARY"},
			want:       false,
		},
		{
			name:       "is-not-defined is true when only a prefixed property exists",
			body:       "X-SUMMARY:Standup\r\n",
			propFilter: propFilter{Name: "SUMMARY", IsNotDefined: &struct{}{}},
			want:       true,
		},
		{
			name:       "is-not-defined is false when the property exists with parameters",
			body:       "SUMMARY;LANGUAGE=en:Standup\r\n",
			propFilter: propFilter{Name: "SUMMARY", IsNotDefined: &struct{}{}},
			want:       false,
		},
		{
			name:       "text-match still applies to a defined property",
			body:       "SUMMARY:Standup\r\n",
			propFilter: propFilter{Name: "SUMMARY", TextMatch: &textMatch{Text: "standup"}},
			want:       true,
		},
		{
			name:       "text-match miss rejects a defined property",
			body:       "SUMMARY:Standup\r\n",
			propFilter: propFilter{Name: "SUMMARY", TextMatch: &textMatch{Text: "retro"}},
			want:       false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			event := store.Event{
				UID:     "prop-filter",
				RawICAL: "BEGIN:VCALENDAR\r\nBEGIN:VEVENT\r\nUID:prop-filter\r\n" + tt.body + "END:VEVENT\r\nEND:VCALENDAR\r\n",
			}
			if got := eventMatchesFilter(event, calQueryWithPropFilter(tt.propFilter), floatingZone{}); got != tt.want {
				t.Fatalf("eventMatchesFilter = %v, want %v for body %q", got, tt.want, tt.body)
			}
		})
	}
}

// RFC 4790 §9.2: i;ascii-casemap folds US-ASCII letters and nothing else. The
// server advertises that collation, so text matching must not fold the
// non-ASCII text a Unicode-aware uppercase would -- a client asking for "é"
// would otherwise be handed events carrying "É".
func TestTextMatchFoldsOnlyASCIICase(t *testing.T) {
	tests := []struct {
		name  string
		body  string
		match string
		want  bool
	}{
		{
			name:  "ASCII case is folded",
			body:  "SUMMARY:Standup\r\n",
			match: "sTaNdUp",
			want:  true,
		},
		{
			name:  "non-ASCII case is not folded",
			body:  "SUMMARY:CAFÉ\r\n",
			match: "café",
			want:  false,
		},
		{
			name:  "non-ASCII matches itself exactly",
			body:  "SUMMARY:CAFÉ\r\n",
			match: "CAFÉ",
			want:  true,
		},
		{
			name:  "ASCII letters around non-ASCII still fold",
			body:  "SUMMARY:Café Meeting\r\n",
			match: "Café MEETING",
			want:  true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			event := store.Event{
				UID:     "collation",
				RawICAL: "BEGIN:VCALENDAR\r\nBEGIN:VEVENT\r\nUID:collation\r\n" + tt.body + "END:VEVENT\r\nEND:VCALENDAR\r\n",
			}
			filter := calQueryWithPropFilter(propFilter{Name: "SUMMARY", TextMatch: &textMatch{Text: tt.match}})
			if got := eventMatchesFilter(event, filter, floatingZone{}); got != tt.want {
				t.Fatalf("eventMatchesFilter = %v, want %v matching %q against %q", got, tt.want, tt.match, tt.body)
			}
		})
	}
}

// RFC 4791 §7.5 admits only the collations CALDAV:supported-collation-set
// advertises, and §7.8.7 answers anything else with CALDAV:supported-collation.
// The rule reaches every CALDAV:text-match a body carries, so the identifier is
// exercised at each depth the §9.7 grammar admits one: a prop-filter directly
// under the filter, a prop-filter nested two comp-filters down, and a
// param-filter inside a prop-filter.
func TestCalendarQueryCollationsAreValidatedAtEveryFilterDepth(t *testing.T) {
	tests := map[string]struct {
		collation string
		supported bool
	}{
		"absent attribute defaults to i;ascii-casemap": {collation: "", supported: true},
		"the advertised collation":                     {collation: "i;ascii-casemap", supported: true},
		"identifiers are case-insensitive":             {collation: "I;ASCII-CASEMAP", supported: true},
		"the default alias":                            {collation: "default", supported: true},
		"the required octet collation":                 {collation: "i;octet", supported: true},
		"an unadvertised collation":                    {collation: "i;unicode-casemap"},
		"an unknown identifier":                        {collation: "i;made-up"},
		// RFC 4791 §7.5 forbids a wildcard in a collation identifier, so one is
		// refused rather than expanded against the supported set.
		"a wildcard identifier":       {collation: "i;ascii-*"},
		"a bare wildcard":             {collation: "*"},
		"a wildcard on a real prefix": {collation: "i;*"},
	}

	for name, tt := range tests {
		t.Run(name, func(t *testing.T) {
			match := `<C:text-match collation="` + tt.collation + `">standup</C:text-match>`
			depths := map[string]string{
				"prop-filter": `<C:comp-filter name="VCALENDAR"><C:comp-filter name="VEVENT">` +
					`<C:prop-filter name="SUMMARY">` + match + `</C:prop-filter>` +
					`</C:comp-filter></C:comp-filter>`,
				"nested comp-filter": `<C:comp-filter name="VCALENDAR"><C:comp-filter name="VEVENT">` +
					`<C:comp-filter name="VALARM"><C:prop-filter name="DESCRIPTION">` + match + `</C:prop-filter></C:comp-filter>` +
					`</C:comp-filter></C:comp-filter>`,
				"param-filter": `<C:comp-filter name="VCALENDAR"><C:comp-filter name="VEVENT">` +
					`<C:prop-filter name="ATTENDEE"><C:param-filter name="PARTSTAT">` + match + `</C:param-filter></C:prop-filter>` +
					`</C:comp-filter></C:comp-filter>`,
			}

			for depth, filter := range depths {
				t.Run(depth, func(t *testing.T) {
					rr := runCalendarReport(t, calendarFilterBody(filter))
					if tt.supported {
						if rr.Code != http.StatusMultiStatus {
							t.Fatalf("collation %q = %d, want 207; body: %s", tt.collation, rr.Code, rr.Body.String())
						}
						return
					}
					assertErrorConditions(t, rr, http.StatusForbidden, calQN("supported-collation"))
				})
			}
		})
	}

	t.Run("a filter naming no collation at all", func(t *testing.T) {
		rr := runCalendarReport(t, calendarFilterBody(grammarVEventFilter))
		if rr.Code != http.StatusMultiStatus {
			t.Fatalf("status = %d, want 207; body: %s", rr.Code, rr.Body.String())
		}
	})
}

func TestEventFilterFromCalFilter(t *testing.T) {
	// Both bounds carry the skew unconditionally: the columns the predicates
	// read and the instant the §9.9 test judges resolve the same property
	// against different zones, and the report cannot tell per row which of them
	// a candidate needs.
	t.Run("valid range sets both bounds, each carrying the skew", func(t *testing.T) {
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
		wantStart := time.Date(2026, 6, 1, 0, 0, 0, 0, time.UTC).Add(-maxStoredInstantSkew)
		wantEnd := time.Date(2026, 7, 1, 0, 0, 0, 0, time.UTC).Add(maxStoredInstantSkew)
		if !ef.Start.Equal(wantStart) {
			t.Errorf("start = %v, want %v", ef.Start, wantStart)
		}
		if !ef.End.Equal(wantEnd) {
			t.Errorf("end = %v, want %v", ef.End, wantEnd)
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
		if ef.End != nil {
			t.Errorf("an omitted end bounded the read at %v", ef.End)
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

// RFC 4791 §9.9 matches a zero-duration event with (start <= DTSTART && end >
// DTSTART); the ordinary overlap expression wrongly excludes one sitting
// exactly on range_start.
func TestEventInTimeRangeBoundaryOverlap(t *testing.T) {
	rangeTR := &timeRange{Start: "20240601T100000Z", End: "20240601T110000Z"}

	tests := []struct {
		name     string
		dtstart  string
		dtend    string
		duration string
		want     bool
	}{
		{name: "zero-duration at range start", dtstart: "20240601T100000Z", want: true},
		// A DTEND equal to DTSTART is not a legal VEVENT -- RFC 5545 §3.6.1
		// requires DTEND to be later than DTSTART -- and the §9.9 row for a
		// VEVENT carrying DTEND is (start < DTEND AND end > DTSTART), which the
		// degenerate spelling cannot satisfy at the range start. A zero-duration
		// occurrence is written with DURATION:PT0S or with DTSTART alone.
		{name: "explicit zero-duration DTEND at range start", dtstart: "20240601T100000Z", dtend: "20240601T100000Z", want: false},
		{name: "zero DURATION at range start", dtstart: "20240601T100000Z", duration: "PT0S", want: true},
		{name: "zero-duration inside range", dtstart: "20240601T103000Z", want: true},
		{name: "zero-duration just before range start", dtstart: "20240601T095900Z", want: false},
		{name: "zero-duration at range end", dtstart: "20240601T110000Z", want: false},
		{name: "duration ending at range start", dtstart: "20240601T090000Z", dtend: "20240601T100000Z", want: false},
		{name: "duration starting at range end", dtstart: "20240601T110000Z", dtend: "20240601T120000Z", want: false},
		{name: "duration overlapping range start", dtstart: "20240601T093000Z", dtend: "20240601T103000Z", want: true},
		{name: "duration spanning the range", dtstart: "20240601T090000Z", dtend: "20240601T120000Z", want: true},
		{name: "all-day covering the range", dtstart: ";VALUE=DATE:20240601", want: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dtstart := tt.dtstart
			if !strings.HasPrefix(dtstart, ";") {
				dtstart = ":" + dtstart
			}
			raw := "BEGIN:VCALENDAR\r\nBEGIN:VEVENT\r\nUID:boundary\r\nDTSTART" + dtstart + "\r\n"
			if tt.dtend != "" {
				raw += "DTEND:" + tt.dtend + "\r\n"
			}
			if tt.duration != "" {
				raw += "DURATION:" + tt.duration + "\r\n"
			}
			raw += "END:VEVENT\r\nEND:VCALENDAR\r\n"

			if got := resourceInTimeRange(store.Event{UID: "boundary", RawICAL: raw}, rangeTR, floatingZone{}); got != tt.want {
				t.Fatalf("eventInTimeRange = %v, want %v for %q", got, tt.want, raw)
			}
		})
	}
}

func TestFreeBusyOmitsZeroDurationEventAtRangeStart(t *testing.T) {
	start := time.Date(2024, 6, 1, 10, 0, 0, 0, time.UTC)
	event := store.Event{
		UID:     "zero-duration",
		RawICAL: "BEGIN:VCALENDAR\r\nBEGIN:VEVENT\r\nUID:zero-duration\r\nDTSTART:20240601T100000Z\r\nEND:VEVENT\r\nEND:VCALENDAR\r\n",
		DTStart: &start,
	}

	h := &DavServer{}
	body := h.generateFreeBusy(freeBusyCandidates([]store.Event{event}, floatingZone{}), &timeRange{Start: "20240601T100000Z", End: "20240601T110000Z"})
	if periods := parsedFreeBusyLines(t, body); len(periods) != 0 {
		t.Fatalf("zero-duration event published periods %v", periods)
	}

	before := h.generateFreeBusy(freeBusyCandidates([]store.Event{event}, floatingZone{}), &timeRange{Start: "20240601T110000Z", End: "20240601T120000Z"})
	if periods := parsedFreeBusyLines(t, before); len(periods) != 0 {
		t.Fatalf("zero-duration event before the range published periods %v", periods)
	}
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

	body := (&DavServer{}).generateFreeBusy(freeBusyCandidates([]store.Event{event}, floatingZone{}), tr)

	assertPublishedFreeBusy(t, parsedFreeBusyLines(t, body), []string{
		"FREEBUSY:20240617T100000Z/20240617T110000Z",
	})
}

// Each range spans the single day its rule should place one instance on, so the
// exact published set is the assertion: it pins the instance the rule has to
// generate and rules out every instance it must not, without the fixture having
// to name the wrong ones one at a time.
func TestGenerateFreeBusyExpandsRRuleByParts(t *testing.T) {
	tests := []struct {
		name     string
		dtstart  string
		dtend    string
		rrule    string
		rangeTR  *timeRange
		wantBusy string
	}{
		{
			name:     "weekly byday multiple weekdays",
			dtstart:  "20240603T090000Z",
			dtend:    "20240603T100000Z",
			rrule:    "FREQ=WEEKLY;COUNT=6;BYDAY=MO,WE,FR",
			rangeTR:  &timeRange{Start: "20240605T000000Z", End: "20240606T000000Z"},
			wantBusy: "FREEBUSY:20240605T090000Z/20240605T100000Z",
		},
		{
			name:     "monthly bymonthday skips missing month day",
			dtstart:  "20240131T090000Z",
			dtend:    "20240131T100000Z",
			rrule:    "FREQ=MONTHLY;COUNT=3;BYMONTHDAY=31",
			rangeTR:  &timeRange{Start: "20240331T000000Z", End: "20240401T000000Z"},
			wantBusy: "FREEBUSY:20240331T090000Z/20240331T100000Z",
		},
		{
			name:     "yearly ordinal byday in month",
			dtstart:  "20240331T090000Z",
			dtend:    "20240331T100000Z",
			rrule:    "FREQ=YEARLY;COUNT=3;BYMONTH=3;BYDAY=-1SU",
			rangeTR:  &timeRange{Start: "20250330T000000Z", End: "20250331T000000Z"},
			wantBusy: "FREEBUSY:20250330T090000Z/20250330T100000Z",
		},
		{
			name:     "yearly bymonthday without bymonth expands every month",
			dtstart:  "20240110T090000Z",
			dtend:    "20240110T100000Z",
			rrule:    "FREQ=YEARLY;COUNT=3;BYMONTHDAY=10",
			rangeTR:  &timeRange{Start: "20240210T000000Z", End: "20240211T000000Z"},
			wantBusy: "FREEBUSY:20240210T090000Z/20240210T100000Z",
		},
		{
			name:     "yearly ordinal byday without bymonth applies to year",
			dtstart:  "20241229T090000Z",
			dtend:    "20241229T100000Z",
			rrule:    "FREQ=YEARLY;COUNT=2;BYDAY=-1SU",
			rangeTR:  &timeRange{Start: "20251228T000000Z", End: "20251229T000000Z"},
			wantBusy: "FREEBUSY:20251228T090000Z/20251228T100000Z",
		},
		{
			name:     "monthly bysetpos",
			dtstart:  "20240607T090000Z",
			dtend:    "20240607T100000Z",
			rrule:    "FREQ=MONTHLY;COUNT=3;BYDAY=FR;BYSETPOS=1",
			rangeTR:  &timeRange{Start: "20240705T000000Z", End: "20240706T000000Z"},
			wantBusy: "FREEBUSY:20240705T090000Z/20240705T100000Z",
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
			if !resourceInTimeRange(event, tt.rangeTR, floatingZone{}) {
				t.Fatalf("expected event to match range %+v", tt.rangeTR)
			}
			body := h.generateFreeBusy(freeBusyCandidates([]store.Event{event}, floatingZone{}), tt.rangeTR)
			assertPublishedFreeBusy(t, parsedFreeBusyLines(t, body), []string{tt.wantBusy})
		})
	}
}

// Each range is narrow enough to admit one occurrence, so the exact published
// set pins both the instance the rule generates and the master instance outside
// the range that it must not.
func TestGenerateFreeBusyExpandsSubDailyRecurringEvents(t *testing.T) {
	tests := []struct {
		name     string
		rrule    string
		rangeTR  *timeRange
		wantBusy string
		start    time.Time
		end      time.Time
	}{
		{
			name:     "hourly",
			rrule:    "FREQ=HOURLY;INTERVAL=2;COUNT=6",
			rangeTR:  &timeRange{Start: "20240601T040000Z", End: "20240601T050000Z"},
			wantBusy: "FREEBUSY:20240601T040000Z/20240601T041500Z",
			start:    time.Date(2024, 6, 1, 0, 0, 0, 0, time.UTC),
			end:      time.Date(2024, 6, 1, 0, 15, 0, 0, time.UTC),
		},
		{
			name:     "minutely",
			rrule:    "FREQ=MINUTELY;INTERVAL=30;COUNT=6",
			rangeTR:  &timeRange{Start: "20240601T010000Z", End: "20240601T013000Z"},
			wantBusy: "FREEBUSY:20240601T010000Z/20240601T011000Z",
			start:    time.Date(2024, 6, 1, 0, 0, 0, 0, time.UTC),
			end:      time.Date(2024, 6, 1, 0, 10, 0, 0, time.UTC),
		},
		{
			name:     "secondly",
			rrule:    "FREQ=SECONDLY;INTERVAL=30;COUNT=6",
			rangeTR:  &timeRange{Start: "20240601T000100Z", End: "20240601T000130Z"},
			wantBusy: "FREEBUSY:20240601T000100Z/20240601T000110Z",
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

			body := (&DavServer{}).generateFreeBusy(freeBusyCandidates([]store.Event{event}, floatingZone{}), tt.rangeTR)
			assertPublishedFreeBusy(t, parsedFreeBusyLines(t, body), []string{tt.wantBusy})
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
	if !resourceInTimeRange(event, tr, floatingZone{}) {
		t.Fatal("expected counted secondly recurrence beyond scan limit to match requested range")
	}
	body := h.generateFreeBusy(freeBusyCandidates([]store.Event{event}, floatingZone{}), tr)
	// A secondly recurrence of ten-second occurrences overlaps itself, and
	// RFC 4791 §7.10 has the server coalesce consecutive or overlapping busy
	// periods of the same type. The instance is therefore published inside one
	// merged span rather than as a period of its own.
	assertFreeBusyCovers(t, body, target, target.Add(10*time.Second))
}

// assertFreeBusyCovers asserts some published busy period spans [from, to].
// Coalescing makes the exact period boundaries a function of the whole result
// set, so what a single occurrence owes the client is coverage rather than a
// line of its own.
func assertFreeBusyCovers(t *testing.T, body string, from, to time.Time) {
	t.Helper()
	for _, period := range parsedFreeBusyPeriods(t, body) {
		if !period.start.After(from) && !period.end.Before(to) {
			return
		}
	}
	t.Fatalf("no published busy period covers %s/%s; got %s",
		from.Format("20060102T150405Z"), to.Format("20060102T150405Z"), body)
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
			if !resourceInTimeRange(event, tt.rangeTR, floatingZone{}) {
				t.Fatalf("expected event to match range %+v", tt.rangeTR)
			}
			body := h.generateFreeBusy(freeBusyCandidates([]store.Event{event}, floatingZone{}), tt.rangeTR)
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
	if !resourceInTimeRange(event, movedRange, floatingZone{}) {
		t.Fatal("expected override instance to match its moved range")
	}
	body := h.generateFreeBusy(freeBusyCandidates([]store.Event{event}, floatingZone{}), movedRange)
	assertPublishedFreeBusy(t, parsedFreeBusyLines(t, body), []string{
		"FREEBUSY:20240602T150000Z/20240602T160000Z",
	})

	originalRange := &timeRange{Start: "20240602T100000Z", End: "20240602T110000Z"}
	if resourceInTimeRange(event, originalRange, floatingZone{}) {
		t.Fatal("expected overridden original instance to be suppressed")
	}
	originalBody := h.generateFreeBusy(freeBusyCandidates([]store.Event{event}, floatingZone{}), originalRange)
	assertPublishedFreeBusy(t, parsedFreeBusyLines(t, originalBody), nil)
}

// A cancelled override publishes no busy time for the slot it replaces. Which
// cancellation shape is under test decides what the assertion can prove: an
// override carrying no DTSTART occupies no interval, so it publishes nothing
// whatever the cancellation did, and only the slot-occupying shape tells the
// two apart. Both shapes are legal, so both are covered.
func TestCancelledRecurrenceOverrideSuppressesBusyPeriod(t *testing.T) {
	const master = "BEGIN:VCALENDAR\r\nBEGIN:VEVENT\r\nUID:cancelled-override\r\n" +
		"DTSTART:20240601T100000Z\r\nDTEND:20240601T110000Z\r\nRRULE:FREQ=DAILY;COUNT=2\r\nEND:VEVENT\r\n"
	start := time.Date(2024, 6, 1, 10, 0, 0, 0, time.UTC)
	end := time.Date(2024, 6, 1, 11, 0, 0, 0, time.UTC)
	tr := &timeRange{Start: "20240602T000000Z", End: "20240603T000000Z"}

	eventWithOverride := func(override string) store.Event {
		return store.Event{
			UID:     "cancelled-override",
			RawICAL: master + override + "END:VCALENDAR\r\n",
			DTStart: &start,
			DTEnd:   &end,
		}
	}

	t.Run("the override occupies the slot it cancels", func(t *testing.T) {
		event := eventWithOverride("BEGIN:VEVENT\r\nUID:cancelled-override\r\n" +
			"RECURRENCE-ID:20240602T100000Z\r\nDTSTART:20240602T100000Z\r\nDTEND:20240602T110000Z\r\n" +
			"STATUS:CANCELLED\r\nEND:VEVENT\r\n")
		// §9.9 judges intersection and says nothing about STATUS, so the filter
		// matches the interval the override occupies. Whether that interval is
		// busy is the separate question §7.10 answers, by sending CANCELLED to
		// FREE.
		if !resourceInTimeRange(event, tr, floatingZone{}) {
			t.Fatal("expected the cancelled override to intersect the range it occupies")
		}
		body := (&DavServer{}).generateFreeBusy(freeBusyCandidates([]store.Event{event}, floatingZone{}), tr)
		assertPublishedFreeBusy(t, parsedFreeBusyLines(t, body), nil)
	})

	t.Run("the override carries no times of its own", func(t *testing.T) {
		event := eventWithOverride("BEGIN:VEVENT\r\nUID:cancelled-override\r\n" +
			"RECURRENCE-ID:20240602T100000Z\r\nSTATUS:CANCELLED\r\nEND:VEVENT\r\n")
		// The master's generated slot is judged by the override replacing it,
		// and this one names no interval, so nothing reaches the range at all.
		if resourceInTimeRange(event, tr, floatingZone{}) {
			t.Fatal("expected the overridden slot to be judged by the override")
		}
		body := (&DavServer{}).generateFreeBusy(freeBusyCandidates([]store.Event{event}, floatingZone{}), tr)
		assertPublishedFreeBusy(t, parsedFreeBusyLines(t, body), nil)
	})
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
	if !resourceInTimeRange(rdateEvent, tr, floatingZone{}) {
		t.Fatal("expected RDATE instance to match requested range")
	}
	body := h.generateFreeBusy(freeBusyCandidates([]store.Event{rdateEvent}, floatingZone{}), tr)
	assertPublishedFreeBusy(t, parsedFreeBusyLines(t, body), []string{
		"FREEBUSY:20240605T090000Z/20240605T100000Z",
	})

	exdateEvent := store.Event{
		UID:     "exdate",
		RawICAL: "BEGIN:VCALENDAR\r\nBEGIN:VEVENT\r\nUID:exdate\r\nDTSTART:20240601T090000Z\r\nDTEND:20240601T100000Z\r\nRRULE:FREQ=DAILY;COUNT=3\r\nEXDATE:20240602T090000Z\r\nEND:VEVENT\r\nEND:VCALENDAR\r\n",
		DTStart: &start,
		DTEnd:   &end,
	}
	exdateBody := h.generateFreeBusy(freeBusyCandidates([]store.Event{exdateEvent}, floatingZone{}), &timeRange{Start: "20240602T000000Z", End: "20240603T000000Z"})
	assertPublishedFreeBusy(t, parsedFreeBusyLines(t, exdateBody), nil)
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
	if !resourceInTimeRange(event, tr, floatingZone{}) {
		t.Fatal("expected BYWEEKNO recurrence to match requested week")
	}
	body := h.generateFreeBusy(freeBusyCandidates([]store.Event{event}, floatingZone{}), tr)
	assertPublishedFreeBusy(t, parsedFreeBusyLines(t, body), []string{
		"FREEBUSY:20240513T090000Z/20240513T100000Z",
	})
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

			body := (&DavServer{}).generateFreeBusy(freeBusyCandidates([]store.Event{event}, floatingZone{}), tt.rangeTR)
			assertPublishedFreeBusy(t, parsedFreeBusyLines(t, body), []string{tt.wantBusy})
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

	inRangeBody := (&DavServer{}).generateFreeBusy(freeBusyCandidates([]store.Event{event}, floatingZone{}), &timeRange{Start: "20240603T000000Z", End: "20240604T000000Z"})
	assertPublishedFreeBusy(t, parsedFreeBusyLines(t, inRangeBody), []string{
		"FREEBUSY:20240603T090000Z/20240603T100000Z",
	})

	afterUntilBody := (&DavServer{}).generateFreeBusy(freeBusyCandidates([]store.Event{event}, floatingZone{}), &timeRange{Start: "20240604T000000Z", End: "20240605T000000Z"})
	assertPublishedFreeBusy(t, parsedFreeBusyLines(t, afterUntilBody), nil)
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

	if !resourceInTimeRange(event, &timeRange{Start: "20250101T000000Z", End: "20250102T000000Z"}, floatingZone{}) {
		t.Fatal("expected unsupported recurrence frequency to be permissively included by filtering")
	}
	got := (&DavServer{}).generateFreeBusy(freeBusyCandidates([]store.Event{event}, floatingZone{}), &timeRange{Start: "20250101T000000Z", End: "20250102T000000Z"})
	assertPublishedFreeBusy(t, parsedFreeBusyLines(t, got), nil)
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
	if !resourceInTimeRange(event, masterRange, floatingZone{}) {
		t.Fatal("expected malformed recurrence to fall back to the master instance")
	}
	body := h.generateFreeBusy(freeBusyCandidates([]store.Event{event}, floatingZone{}), masterRange)
	assertPublishedFreeBusy(t, parsedFreeBusyLines(t, body), []string{
		"FREEBUSY:20240601T090000Z/20240601T100000Z",
	})

	laterRange := &timeRange{Start: "20240602T093000Z", End: "20240602T094500Z"}
	if resourceInTimeRange(event, laterRange, floatingZone{}) {
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
	if !resourceInTimeRange(event, tr, floatingZone{}) {
		t.Fatal("expected DURATION-backed recurrence to overlap the requested range")
	}
	body := h.generateFreeBusy(freeBusyCandidates([]store.Event{event}, floatingZone{}), tr)
	assertPublishedFreeBusy(t, parsedFreeBusyLines(t, body), []string{
		"FREEBUSY:20240602T090000Z/20240602T110000Z",
	})
}

func TestRDatePeriodWithExplicitEnd(t *testing.T) {
	start := time.Date(2024, 6, 1, 9, 0, 0, 0, time.UTC)
	event := store.Event{
		UID:     "rdate-period",
		RawICAL: "BEGIN:VCALENDAR\r\nBEGIN:VEVENT\r\nUID:rdate-period\r\nDTSTART:20240601T090000Z\r\nRDATE;VALUE=PERIOD:20240605T090000Z/20240605T113000Z\r\nEND:VEVENT\r\nEND:VCALENDAR\r\n",
		DTStart: &start,
	}

	body := (&DavServer{}).generateFreeBusy(freeBusyCandidates([]store.Event{event}, floatingZone{}), &timeRange{Start: "20240605T000000Z", End: "20240606T000000Z"})
	assertPublishedFreeBusy(t, parsedFreeBusyLines(t, body), []string{
		"FREEBUSY:20240605T090000Z/20240605T113000Z",
	})
}

func TestRDatePeriodWithDuration(t *testing.T) {
	start := time.Date(2024, 6, 1, 9, 0, 0, 0, time.UTC)
	event := store.Event{
		UID:     "rdate-duration",
		RawICAL: "BEGIN:VCALENDAR\r\nBEGIN:VEVENT\r\nUID:rdate-duration\r\nDTSTART:20240601T090000Z\r\nRDATE;VALUE=PERIOD:20240605T090000Z/PT2H30M\r\nEND:VEVENT\r\nEND:VCALENDAR\r\n",
		DTStart: &start,
	}

	body := (&DavServer{}).generateFreeBusy(freeBusyCandidates([]store.Event{event}, floatingZone{}), &timeRange{Start: "20240605T110000Z", End: "20240605T120000Z"})
	assertPublishedFreeBusy(t, parsedFreeBusyLines(t, body), []string{
		"FREEBUSY:20240605T090000Z/20240605T113000Z",
	})
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

	body := (&DavServer{}).generateFreeBusy(freeBusyCandidates([]store.Event{rdateEvent}, floatingZone{}), &timeRange{Start: "20240605T000000Z", End: "20240606T000000Z"})
	assertPublishedFreeBusy(t, parsedFreeBusyLines(t, body), []string{
		"FREEBUSY:20240605T130000Z/20240605T140000Z",
	})

	exdateEvent := store.Event{
		UID:     "exdate-tzid",
		RawICAL: "BEGIN:VCALENDAR\r\nBEGIN:VEVENT\r\nUID:exdate-tzid\r\nDTSTART;TZID=America/New_York:20240601T090000\r\nDTEND;TZID=America/New_York:20240601T100000\r\nRRULE:FREQ=DAILY;COUNT=2\r\nEXDATE;TZID=America/New_York:20240602T090000\r\nEND:VEVENT\r\nEND:VCALENDAR\r\n",
		DTStart: &start,
		DTEnd:   &end,
	}
	exdateBody := (&DavServer{}).generateFreeBusy(freeBusyCandidates([]store.Event{exdateEvent}, floatingZone{}), &timeRange{Start: "20240602T000000Z", End: "20240603T000000Z"})
	assertPublishedFreeBusy(t, parsedFreeBusyLines(t, exdateBody), nil)
}

// RFC 4791 §7.3 gives free-busy the collection's CALDAV:calendar-timezone as
// the zone a floating value resolves against, and §7.10 makes the response body
// the report's answer. Resolving the zone only when choosing which events to
// consider, and then deriving the busy periods as though the same values were
// UTC, answers with a period the request did not ask about -- or, as here, with
// none at all, reporting the user free while they are busy.
func TestFreeBusyPeriodsResolveFloatingValuesThroughTheCollectionTimezone(t *testing.T) {
	if _, err := time.LoadLocation("America/Chicago"); err != nil {
		t.Skipf("tzdata unavailable: %v", err)
	}
	// 10:00 floating. Read as UTC that is 10:00Z; in America/Chicago (CDT in
	// June) it is 15:00Z, and only the latter is the instant §7.3 names.
	columnStart := time.Date(2024, 6, 1, 10, 0, 0, 0, time.UTC)
	columnEnd := time.Date(2024, 6, 1, 11, 0, 0, 0, time.UTC)
	chicago := chicagoVTimezone()

	calRepo := &fakeCalendarRepo{
		accessible: []store.CalendarAccess{
			{Calendar: store.Calendar{ID: 1, UserID: 1, Name: "Test", Timezone: &chicago}, Editor: true},
		},
	}
	eventRepo := &fakeEventRepo{
		events: map[string]*store.Event{
			"1:floating": {
				CalendarID: 1,
				UID:        "floating",
				RawICAL: "BEGIN:VCALENDAR\r\nBEGIN:VEVENT\r\nUID:floating\r\n" +
					"DTSTART:20240601T100000\r\nDTEND:20240601T110000\r\nEND:VEVENT\r\nEND:VCALENDAR\r\n",
				ETag:    "e",
				DTStart: &columnStart,
				DTEnd:   &columnEnd,
			},
		},
	}
	h := &DavServer{store: &store.Store{Calendars: calRepo, Events: eventRepo}}

	body := `<?xml version="1.0" encoding="utf-8" ?>
<C:free-busy-query xmlns:C="urn:ietf:params:xml:ns:caldav">
  <C:time-range start="20240601T150000Z" end="20240601T160000Z"/>
</C:free-busy-query>`
	req := httptest.NewRequest("REPORT", "/dav/calendars/1/", strings.NewReader(body))
	req.Header.Set("Depth", "1")
	req = req.WithContext(auth.WithUser(req.Context(), &store.User{ID: 1}))
	rr := httptest.NewRecorder()

	h.Report(rr, req)

	if rr.Code != http.StatusOK {
		t.Fatalf("free-busy-query = %d, want 200; body: %s", rr.Code, rr.Body.String())
	}
	assertPublishedFreeBusy(t, parsedFreeBusyLines(t, rr.Body.String()), []string{
		"FREEBUSY:20240601T150000Z/20240601T160000Z",
	})
}

// The published periods come from a recurrence set the collection's timezone
// resolved, so the EXDATE that removes an occurrence has to be read in that same
// zone. Left at its UTC reading it matches no generated instance, and the
// response reports the user busy through an occurrence they cancelled.
func TestFreeBusyPeriodsHonourExDateUnderTheCollectionTimezone(t *testing.T) {
	if _, err := time.LoadLocation("America/Chicago"); err != nil {
		t.Skipf("tzdata unavailable: %v", err)
	}
	columnStart := time.Date(2024, 6, 1, 10, 0, 0, 0, time.UTC)
	columnEnd := time.Date(2024, 6, 1, 11, 0, 0, 0, time.UTC)
	chicago := chicagoVTimezone()

	calRepo := &fakeCalendarRepo{
		accessible: []store.CalendarAccess{
			{Calendar: store.Calendar{ID: 1, UserID: 1, Name: "Test", Timezone: &chicago}, Editor: true},
		},
	}
	eventRepo := &fakeEventRepo{
		events: map[string]*store.Event{
			"1:daily": {
				CalendarID: 1,
				UID:        "daily",
				RawICAL: "BEGIN:VCALENDAR\r\nBEGIN:VEVENT\r\nUID:daily\r\n" +
					"DTSTART:20240601T100000\r\nDTEND:20240601T110000\r\n" +
					"RRULE:FREQ=DAILY;COUNT=3\r\nEXDATE:20240602T100000\r\n" +
					"END:VEVENT\r\nEND:VCALENDAR\r\n",
				ETag:    "e",
				DTStart: &columnStart,
				DTEnd:   &columnEnd,
			},
		},
	}
	h := &DavServer{store: &store.Store{Calendars: calRepo, Events: eventRepo}}

	body := `<?xml version="1.0" encoding="utf-8" ?>
<C:free-busy-query xmlns:C="urn:ietf:params:xml:ns:caldav">
  <C:time-range start="20240601T000000Z" end="20240605T000000Z"/>
</C:free-busy-query>`
	req := httptest.NewRequest("REPORT", "/dav/calendars/1/", strings.NewReader(body))
	req.Header.Set("Depth", "1")
	req = req.WithContext(auth.WithUser(req.Context(), &store.User{ID: 1}))
	rr := httptest.NewRecorder()

	h.Report(rr, req)

	if rr.Code != http.StatusOK {
		t.Fatalf("free-busy-query = %d, want 200; body: %s", rr.Code, rr.Body.String())
	}
	assertPublishedFreeBusy(t, parsedFreeBusyLines(t, rr.Body.String()), []string{
		"FREEBUSY:20240601T150000Z/20240601T160000Z",
		"FREEBUSY:20240603T150000Z/20240603T160000Z",
	})
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
	if !resourceInTimeRange(event, tr, floatingZone{}) {
		t.Fatal("expected recurring all-day event to overlap the afternoon of the generated day")
	}
	body := h.generateFreeBusy(freeBusyCandidates([]store.Event{event}, floatingZone{}), tr)
	assertPublishedFreeBusy(t, parsedFreeBusyLines(t, body), []string{
		"FREEBUSY:20240602T000000Z/20240603T000000Z",
	})
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
	if !resourceInTimeRange(event, tr, floatingZone{}) {
		t.Fatal("expected RANGE=THISANDFUTURE override to shift later generated instances")
	}
	body := h.generateFreeBusy(freeBusyCandidates([]store.Event{event}, floatingZone{}), tr)
	assertPublishedFreeBusy(t, parsedFreeBusyLines(t, body), []string{
		"FREEBUSY:20240604T150000Z/20240604T160000Z",
	})
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
	if resourceInTimeRange(event, tr, floatingZone{}) {
		t.Fatal("expected RANGE=THISANDFUTURE cancellation to suppress following generated instances")
	}
	body := h.generateFreeBusy(freeBusyCandidates([]store.Event{event}, floatingZone{}), tr)
	assertPublishedFreeBusy(t, parsedFreeBusyLines(t, body), nil)
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

	body := (&DavServer{}).generateFreeBusy(freeBusyCandidates([]store.Event{event}, floatingZone{}), &timeRange{Start: "20240602T000000Z", End: "20240603T000000Z"})
	assertPublishedFreeBusy(t, parsedFreeBusyLines(t, body), []string{
		"FREEBUSY:20240602T090000Z/20240602T100000Z",
	})
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
	if resourceInTimeRange(event, tr, floatingZone{}) {
		t.Fatal("expected VTIMEZONE RRULE not to make a non-recurring VEVENT match")
	}
	body := h.generateFreeBusy(freeBusyCandidates([]store.Event{event}, floatingZone{}), tr)
	assertPublishedFreeBusy(t, parsedFreeBusyLines(t, body), nil)
}

// calFilterOverVEvent wraps VEVENT-scoped children in the VCALENDAR comp-filter
// RFC 4791 §9.7.1 scopes to the calendar object resource itself.
func calFilterOverVEvent(vevent compFilter) *calFilter {
	vevent.Name = "VEVENT"
	return &calFilter{CompFilter: compFilter{Name: "VCALENDAR", CompFilter: []compFilter{vevent}}}
}

const filterEventICAL = "BEGIN:VCALENDAR\r\n" +
	"VERSION:2.0\r\n" +
	"BEGIN:VEVENT\r\n" +
	"UID:scoped\r\n" +
	"DTSTART:20240601T090000Z\r\n" +
	"SUMMARY:Standup\r\n" +
	"DESCRIPTION:agenda mentions Retro\r\n" +
	"ATTENDEE;PARTSTAT=ACCEPTED;CN=Dana:mailto:dana@example.com\r\n" +
	"BEGIN:VALARM\r\n" +
	"ACTION:DISPLAY\r\n" +
	"DESCRIPTION:Reminder\r\n" +
	"TRIGGER:-PT15M\r\n" +
	"END:VALARM\r\n" +
	"END:VEVENT\r\n" +
	"END:VCALENDAR\r\n"

// RFC 4791 §9.7.2 scopes a prop-filter's text-match to the named property's
// value. Matching the whole object instead lets an unrelated property satisfy
// the filter, which is the defect this pins closed.
func TestTextMatchIsScopedToTheNamedProperty(t *testing.T) {
	tests := []struct {
		name     string
		property string
		text     string
		want     bool
	}{
		{name: "the named property's own value matches", property: "SUMMARY", text: "Standup", want: true},
		{name: "another property's value does not", property: "SUMMARY", text: "Retro", want: false},
		{name: "that other property matches under its own filter", property: "DESCRIPTION", text: "Retro", want: true},
		{name: "a component name is not a property value", property: "SUMMARY", text: "VEVENT", want: false},
		{name: "a parameter is not part of the property value", property: "ATTENDEE", text: "ACCEPTED", want: false},
		{name: "the property value itself still matches", property: "ATTENDEE", text: "dana@example.com", want: true},
	}

	event := store.Event{UID: "scoped", RawICAL: filterEventICAL}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			filter := calFilterOverVEvent(compFilter{PropFilter: []propFilter{{
				Name:      tt.property,
				TextMatch: &textMatch{Text: tt.text},
			}}})
			if got := eventMatchesFilter(event, filter, floatingZone{}); got != tt.want {
				t.Fatalf("text-match %q on %s = %v, want %v", tt.text, tt.property, got, tt.want)
			}
		})
	}
}

// RFC 4791 §9.7.3: a param-filter is scoped to the named parameter of the
// enclosing property. An empty filter matches its existence, is-not-defined
// matches its absence, and a text-match matches that parameter's value.
func TestParamFilterIsScopedToTheNamedParameter(t *testing.T) {
	tests := []struct {
		name  string
		param paramFilter
		want  bool
	}{
		{name: "a defined parameter matches an empty filter", param: paramFilter{Name: "PARTSTAT"}, want: true},
		{name: "an absent parameter does not", param: paramFilter{Name: "ROLE"}, want: false},
		{name: "is-not-defined matches an absent parameter", param: paramFilter{Name: "ROLE", IsNotDefined: &struct{}{}}, want: true},
		{name: "is-not-defined rejects a defined parameter", param: paramFilter{Name: "PARTSTAT", IsNotDefined: &struct{}{}}, want: false},
		{name: "the parameter value matches", param: paramFilter{Name: "PARTSTAT", TextMatch: &textMatch{Text: "accepted"}}, want: true},
		{name: "another parameter's value does not", param: paramFilter{Name: "PARTSTAT", TextMatch: &textMatch{Text: "Dana"}}, want: false},
		{name: "the property value is not the parameter value", param: paramFilter{Name: "PARTSTAT", TextMatch: &textMatch{Text: "mailto:"}}, want: false},
		{name: "parameter names are case-insensitive", param: paramFilter{Name: "partstat"}, want: true},
		{name: "a negated parameter text-match inverts", param: paramFilter{Name: "PARTSTAT", TextMatch: &textMatch{Text: "DECLINED", NegateCondition: "yes"}}, want: true},
	}

	event := store.Event{UID: "scoped", RawICAL: filterEventICAL}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			filter := calFilterOverVEvent(compFilter{PropFilter: []propFilter{{
				Name:        "ATTENDEE",
				ParamFilter: []paramFilter{tt.param},
			}}})
			if got := eventMatchesFilter(event, filter, floatingZone{}); got != tt.want {
				t.Fatalf("param-filter %+v = %v, want %v", tt.param, got, tt.want)
			}
		})
	}
}

// RFC 4791 §9.7.1 and §9.7.4: a comp-filter is scoped to the calendar object at
// the filter root and to the enclosing component when nested, an empty filter
// matches existence, and is-not-defined matches an absent component.
func TestCompFilterScopingAndIsNotDefined(t *testing.T) {
	tests := []struct {
		name   string
		filter *calFilter
		want   bool
	}{
		{
			name:   "the root comp-filter names the calendar object",
			filter: &calFilter{CompFilter: compFilter{Name: "VCALENDAR"}},
			want:   true,
		},
		{
			name:   "a root comp-filter naming a contained component matches nothing",
			filter: &calFilter{CompFilter: compFilter{Name: "VEVENT"}},
			want:   false,
		},
		{
			name:   "a nested component is found under its parent",
			filter: calFilterOverVEvent(compFilter{}),
			want:   true,
		},
		{
			name:   "a component nested two deep is found",
			filter: calFilterOverVEvent(compFilter{CompFilter: []compFilter{{Name: "VALARM"}}}),
			want:   true,
		},
		{
			name:   "a component absent from the parent does not match",
			filter: calFilterOverVEvent(compFilter{CompFilter: []compFilter{{Name: "VTODO"}}}),
			want:   false,
		},
		{
			name:   "a VALARM outside its VEVENT parent does not match",
			filter: &calFilter{CompFilter: compFilter{Name: "VCALENDAR", CompFilter: []compFilter{{Name: "VALARM"}}}},
			want:   false,
		},
		{
			name:   "is-not-defined matches an absent component",
			filter: calFilterOverVEvent(compFilter{CompFilter: []compFilter{{Name: "VTODO", IsNotDefined: &struct{}{}}}}),
			want:   true,
		},
		{
			name:   "is-not-defined rejects a present component",
			filter: calFilterOverVEvent(compFilter{CompFilter: []compFilter{{Name: "VALARM", IsNotDefined: &struct{}{}}}}),
			want:   false,
		},
		{
			name: "every child filter is conjunctive",
			filter: calFilterOverVEvent(compFilter{
				PropFilter: []propFilter{{Name: "SUMMARY"}, {Name: "LOCATION"}},
			}),
			want: false,
		},
		{
			name: "a prop-filter is scoped to its own component",
			filter: calFilterOverVEvent(compFilter{CompFilter: []compFilter{{
				Name:       "VALARM",
				PropFilter: []propFilter{{Name: "SUMMARY"}},
			}}}),
			want: false,
		},
		{
			name: "the alarm's own property is in scope there",
			filter: calFilterOverVEvent(compFilter{CompFilter: []compFilter{{
				Name:       "VALARM",
				PropFilter: []propFilter{{Name: "TRIGGER"}},
			}}}),
			want: true,
		},
	}

	event := store.Event{UID: "scoped", RawICAL: filterEventICAL}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := eventMatchesFilter(event, tt.filter, floatingZone{}); got != tt.want {
				t.Fatalf("eventMatchesFilter = %v, want %v", got, tt.want)
			}
		})
	}
}

// RFC 4791 §7.5 requires both i;ascii-casemap and i;octet, and §9.7.5 defaults
// the attribute to i;ascii-casemap. i;octet compares octet by octet (RFC 4790
// §9.3), so it is the case-sensitive one.
func TestTextMatchHonoursTheNamedCollation(t *testing.T) {
	tests := []struct {
		name      string
		collation string
		text      string
		want      bool
	}{
		{name: "absent collation folds ASCII case", collation: "", text: "standup", want: true},
		{name: "the default alias folds ASCII case", collation: "default", text: "standup", want: true},
		{name: "i;ascii-casemap folds ASCII case", collation: "i;ascii-casemap", text: "sTaNdUp", want: true},
		{name: "i;octet is case-sensitive", collation: "i;octet", text: "standup", want: false},
		{name: "i;octet matches the exact octets", collation: "i;octet", text: "Standup", want: true},
		{name: "identifiers are case-insensitive", collation: "I;OCTET", text: "Standup", want: true},
	}

	event := store.Event{UID: "scoped", RawICAL: filterEventICAL}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			filter := calFilterOverVEvent(compFilter{PropFilter: []propFilter{{
				Name:      "SUMMARY",
				TextMatch: &textMatch{Text: tt.text, Collation: tt.collation},
			}}})
			if got := eventMatchesFilter(event, filter, floatingZone{}); got != tt.want {
				t.Fatalf("collation %q matching %q = %v, want %v", tt.collation, tt.text, got, tt.want)
			}
		})
	}
}

func TestTextMatchUsesTheLogicalPropertyValueWithoutTrimmingTheNeedle(t *testing.T) {
	tests := []struct {
		name    string
		rawICAL string
		text    string
		want    bool
	}{
		{
			name: "an escaped comma is matched as the TEXT value it represents",
			rawICAL: "BEGIN:VCALENDAR\r\nVERSION:2.0\r\nBEGIN:VEVENT\r\nUID:escaped\r\n" +
				`SUMMARY:Planning\, review` + "\r\nEND:VEVENT\r\nEND:VCALENDAR\r\n",
			text: "Planning, review",
			want: true,
		},
		{
			name: "octet matching preserves whitespace in the search text",
			rawICAL: "BEGIN:VCALENDAR\r\nVERSION:2.0\r\nBEGIN:VEVENT\r\nUID:spaces\r\n" +
				"SUMMARY:Standup\r\nEND:VEVENT\r\nEND:VCALENDAR\r\n",
			text: " Standup ",
			want: false,
		},
		{
			name: "octet matching preserves trailing whitespace in the property value",
			rawICAL: "BEGIN:VCALENDAR\r\nVERSION:2.0\r\nBEGIN:VEVENT\r\nUID:value-spaces\r\n" +
				"SUMMARY:Standup \r\nEND:VEVENT\r\nEND:VCALENDAR\r\n",
			text: "Standup ",
			want: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			filter := calFilterOverVEvent(compFilter{PropFilter: []propFilter{{
				Name: "SUMMARY",
				TextMatch: &textMatch{
					Text:      tt.text,
					Collation: "i;octet",
				},
			}}})
			if got := eventMatchesFilter(store.Event{UID: "match", RawICAL: tt.rawICAL}, filter, floatingZone{}); got != tt.want {
				t.Fatalf("eventMatchesFilter() = %v, want %v", got, tt.want)
			}
		})
	}
}

// A calendar object whose stored octets do not parse matches nothing rather
// than failing the report for every other resource in the collection.
func TestFilterTreatsUnparseableStoredDataAsNoMatch(t *testing.T) {
	filter := &calFilter{CompFilter: compFilter{Name: "VCALENDAR"}}
	for _, raw := range []string{"", "ICAL", "BEGIN:VCALENDAR\r\nBEGIN:VEVENT\r\n"} {
		if eventMatchesFilter(store.Event{UID: "broken", RawICAL: raw}, filter, floatingZone{}) {
			t.Errorf("unparseable data %q matched", raw)
		}
	}
}

// The virtual birthday collection is the only calendar data CalCard authors, so
// it never passes through PUT validation. Filtering it goes through the same
// parsed-tree matcher as stored data, which this pins: a generated event has to
// stay parseable and its properties addressable by name.
func TestBirthdayCalendarEventsMatchScopedFilters(t *testing.T) {
	birthday := time.Date(1990, 6, 15, 0, 0, 0, 0, time.UTC)
	displayName := "Dana Lee"
	h := &DavServer{store: &store.Store{
		Contacts: &fakeContactRepo{contacts: map[string]*store.Contact{
			"1:dana": {ID: 1, AddressBookID: 1, UID: "dana", DisplayName: &displayName, Birthday: &birthday},
		}},
	}}

	events, err := h.generateBirthdayEvents(context.Background(), 1)
	if err != nil {
		t.Fatalf("generateBirthdayEvents() error = %v", err)
	}
	if len(events) != 1 {
		t.Fatalf("generateBirthdayEvents() = %d events, want 1", len(events))
	}

	tests := []struct {
		name   string
		filter *calFilter
		want   bool
	}{
		{name: "the generated VEVENT is found", filter: calFilterOverVEvent(compFilter{}), want: true},
		{
			name: "its SUMMARY is addressable by name",
			filter: calFilterOverVEvent(compFilter{PropFilter: []propFilter{{
				Name: "SUMMARY", TextMatch: &textMatch{Text: "Dana Lee"},
			}}}),
			want: true,
		},
		{
			name: "the server-authored extension property is addressable too",
			filter: calFilterOverVEvent(compFilter{PropFilter: []propFilter{{
				Name: "X-CALCARD-TYPE", TextMatch: &textMatch{Text: "BIRTHDAY"},
			}}}),
			want: true,
		},
		{
			name: "a DTSTART parameter is addressable",
			filter: calFilterOverVEvent(compFilter{PropFilter: []propFilter{{
				Name:        "DTSTART",
				ParamFilter: []paramFilter{{Name: "VALUE", TextMatch: &textMatch{Text: "DATE"}}},
			}}}),
			want: true,
		},
		{
			name:   "a component it does not carry does not match",
			filter: calFilterOverVEvent(compFilter{CompFilter: []compFilter{{Name: "VALARM"}}}),
			want:   false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := eventMatchesFilter(events[0], tt.filter, floatingZone{}); got != tt.want {
				t.Fatalf("eventMatchesFilter = %v, want %v for %s", got, tt.want, events[0].RawICAL)
			}
		})
	}
}

// RFC 4791 §7.3 resolves a floating value against the CALDAV:timezone the
// request carries before anything else, and that source is available to every
// collection. The birthday collection defines no CALDAV:calendar-timezone of its
// own, but its entries are DTSTART;VALUE=DATE -- floating values whose implied
// day sits at a different instant in every zone -- so ignoring the request
// element answers with a different day than the client asked about.
func TestBirthdayCalendarQueryResolvesDatesThroughTheRequestTimezone(t *testing.T) {
	if _, err := time.LoadLocation("America/Chicago"); err != nil {
		t.Skipf("tzdata unavailable: %v", err)
	}
	birthday := time.Date(1990, 6, 15, 0, 0, 0, 0, time.UTC)
	displayName := "Dana Lee"
	h := &DavServer{store: &store.Store{
		Contacts: &fakeContactRepo{contacts: map[string]*store.Contact{
			"1:dana": {ID: 1, AddressBookID: 1, UID: "dana", DisplayName: &displayName, Birthday: &birthday},
		}},
	}}

	events, err := h.generateBirthdayEvents(context.Background(), 1)
	if err != nil {
		t.Fatalf("generateBirthdayEvents() error = %v", err)
	}
	// The generated DTSTART rolls to next year once this year's birthday has
	// passed, so the range is built from the value the collection actually
	// carries rather than from a fixed date.
	root, err := parseICalendarObject(events[0].RawICAL)
	if err != nil {
		t.Fatalf("parseICalendarObject() = %v", err)
	}
	event, _ := namedComponent(root, "VEVENT")
	day, err := time.Parse("20060102", strings.TrimSpace(event.value("DTSTART")))
	if err != nil {
		t.Fatalf("generated DTSTART is not a DATE: %v", err)
	}

	// Read as UTC the birthday occupies [day, day+1); in America/Chicago (CDT,
	// -0500 in June) the same DATE is [day+05:00Z, day+1+05:00Z). This range
	// falls in the second and not the first.
	rangeStart := day.AddDate(0, 0, 1).UTC()
	timeRangeXML := fmt.Sprintf(`<C:time-range start="%s" end="%s"/>`,
		rangeStart.Format("20060102T150405Z"),
		rangeStart.Add(3*time.Hour).Format("20060102T150405Z"))

	query := func(timezone string) davMultistatus {
		t.Helper()
		body := calendarQueryBody(`<D:prop><D:getetag/></D:prop><C:filter>` +
			`<C:comp-filter name="VCALENDAR"><C:comp-filter name="VEVENT">` +
			timeRangeXML +
			`</C:comp-filter></C:comp-filter></C:filter>` + timezone)
		req := httptest.NewRequest("REPORT", birthdayCalendarHref(), strings.NewReader(body))
		req.Header.Set("Depth", "1")
		req = req.WithContext(auth.WithUser(req.Context(), &store.User{ID: 1}))
		rr := httptest.NewRecorder()
		h.Report(rr, req)
		return decodeMultistatus(t, rr)
	}

	if got := query(""); len(got.Responses) != 0 {
		t.Fatalf("without a request timezone the range is outside the UTC day, got %d responses", len(got.Responses))
	}

	chicago := `<C:timezone>` + grammarChicagoVTimezone() + `</C:timezone>`
	if got := query(chicago); len(got.Responses) != 1 {
		t.Fatalf("calendar-query responses = %d, want 1; the request timezone was not applied", len(got.Responses))
	}
}

// A client matches the hrefs one report returns against another's, so the
// generated collection has to answer a multiget with the spelling its
// calendar-query returns rather than echoing back the href the request used.
func TestBirthdayCalendarReportsAgreeOnTheResourceHref(t *testing.T) {
	birthday := time.Date(1990, 6, 15, 0, 0, 0, 0, time.UTC)
	displayName := "Dana Lee"
	h := &DavServer{store: &store.Store{
		Contacts: &fakeContactRepo{contacts: map[string]*store.Contact{
			"1:dana": {ID: 1, AddressBookID: 1, UID: "dana", DisplayName: &displayName, Birthday: &birthday},
		}},
	}}

	events, err := h.generateBirthdayEvents(context.Background(), 1)
	if err != nil {
		t.Fatalf("generateBirthdayEvents() error = %v", err)
	}
	resourceName := eventResourceName(events[0])
	collectionHref := birthdayCalendarHref()

	runReport := func(t *testing.T, body string) davMultistatus {
		t.Helper()
		req := httptest.NewRequest("REPORT", collectionHref, strings.NewReader(body))
		req.Header.Set("Depth", "1")
		req = req.WithContext(auth.WithUser(req.Context(), &store.User{ID: 1}))
		rr := httptest.NewRecorder()
		h.Report(rr, req)
		return decodeMultistatus(t, rr)
	}

	queried := runReport(t, calendarQueryBody(
		`<D:prop><D:getetag/></D:prop><C:filter>`+grammarVEventFilter+`</C:filter>`))
	if len(queried.Responses) != 1 {
		t.Fatalf("calendar-query responses = %d, want 1", len(queried.Responses))
	}
	if len(queried.Responses[0].Hrefs) != 1 {
		t.Fatalf("calendar-query response carries %d hrefs, want 1", len(queried.Responses[0].Hrefs))
	}
	want := queried.Responses[0].Hrefs[0]

	// The extension is spelled the other way round from the canonical href, so
	// an echoed response href would not match what calendar-query returned.
	fetched := runReport(t, `<?xml version="1.0" encoding="utf-8"?>
<C:calendar-multiget xmlns:D="DAV:" xmlns:C="urn:ietf:params:xml:ns:caldav">
  <D:prop><D:getetag/></D:prop>
  <D:href>`+collectionHref+resourceName+`.ICS</D:href>
</C:calendar-multiget>`)
	if len(fetched.Responses) != 1 {
		t.Fatalf("calendar-multiget responses = %d, want 1", len(fetched.Responses))
	}
	fetched.Responses[0].assertHref(t, want)
	fetched.Responses[0].assertPropStatus(t, davQN("getetag"), http.StatusOK)
}
