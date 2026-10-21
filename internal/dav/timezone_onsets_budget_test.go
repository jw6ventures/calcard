package dav

import (
	"testing"
	"time"

	"github.com/jw6ventures/calcard/internal/ical"
	"github.com/jw6ventures/calcard/internal/store"
)

// smallObservanceRule is a rule listed from 1981, a few dozen onsets to the
// present, beside the four centuries Exchange's 1601 DTSTART lists.
var smallObservanceRule = struct {
	dtstart time.Time
	rrule   string
}{time.Date(1981, 3, 29, 2, 0, 0, 0, time.UTC), "FREQ=YEARLY;BYMONTH=3;BYDAY=-1SU"}

// requireCacheWithinBudget checks that the cache holds no more than its budget,
// and counts exactly what its listings hold, in the room they have for starts
// (ical.RecurrenceStartLister.Held) rather than the starts they list: the array
// a listing grows into is memory the cache holds, and one it dropped starts
// from does not keep it.
func requireCacheWithinBudget(t *testing.T, onsets *timezoneOnsets) {
	t.Helper()
	held := 0
	onsets.rules.Range(func(_ observanceRuleKey, rule *listedRule) bool {
		held += ruleCost(rule.key.rrule, rule.listed())
		return true
	})
	if counted := onsets.rules.Held(); counted != held {
		t.Fatalf("the cache counts %d units, its listings hold %d", counted, held)
	}
	if held > onsets.rules.Budget() {
		t.Fatalf("holding %d units, want at most %d", held, onsets.rules.Budget())
	}
}

// readOnset answers a lookup as a reader of the cache does: from a listing
// where one reaches before, and otherwise by walking the rule, which the
// reader does on the work budget of the object it is reading.
func readOnset(onsets *timezoneOnsets, dtstart, before time.Time, rrule string) (time.Time, bool, bool) {
	if latest, found, listed := onsets.LatestOnOrBefore(dtstart, before, rrule, nil); listed {
		return latest, found, true
	}
	return ical.LatestRecurrenceOnOrBefore(dtstart, before, rrule)
}

// rulesByRecency lists the cached rules, most recently used first.
func rulesByRecency(onsets *timezoneOnsets) []*listedRule {
	var rules []*listedRule
	onsets.rules.Range(func(_ observanceRuleKey, rule *listedRule) bool {
		rules = append(rules, rule)
		return true
	})
	return rules
}

// A rule whose listing out to the instant asked would outgrow the whole budget
// lists only what the budget holds beside its text. The instant is answered by
// walking the rule, and a listing that stops before the present is not kept,
// so the cache stays within its budget whichever rules it held before.
func TestTimezoneOnsetsListNoMoreOfARuleThanTheBudgetHolds(t *testing.T) {
	const budget = 256
	before := time.Date(2026, 6, 1, 0, 0, 0, 0, time.UTC)
	rule := exchangeStandardRule
	if want := ruleCost(rule.rrule, 2026-1601); want <= budget {
		t.Fatalf("the rule's listing to %v costs %d units, want it to outgrow the %d-unit budget", before, want, budget)
	}
	onsets := newTimezoneOnsets(budget)
	ask := func(dtstart time.Time, rrule string) {
		t.Helper()
		want, wantFound, wantComplete := ical.LatestRecurrenceOnOrBefore(dtstart, before, rrule)
		got, found, complete := readOnset(onsets, dtstart, before, rrule)
		if !got.Equal(want) || found != wantFound || complete != wantComplete {
			t.Fatalf("latest(%v) = %v, %t, %t, want %v, %t, %t", dtstart, got, found, complete, want, wantFound, wantComplete)
		}
		requireCacheWithinBudget(t, onsets)
	}

	ask(rule.dtstart, rule.rrule)
	ask(smallObservanceRule.dtstart, smallObservanceRule.rrule)
	ask(rule.dtstart, rule.rrule)
	ask(rule.dtstart, rule.rrule)

	key := observanceRuleKey{seconds: rule.dtstart.Unix(), rrule: rule.rrule}
	cached, ok := onsets.rules.Get(key)
	if !ok {
		t.Fatal("the rule was not cached")
	}
	if !cached.walked || cached.listed() != 0 {
		t.Fatalf("the rule holds %d onsets, walked %v; want a listing that stopped centuries short of the instant dropped", cached.listed(), cached.walked)
	}
	if walks := onsets.walks.Load(); walks != 3 {
		t.Fatalf("walked the rule %d times, want once for each lookup past what it lists", walks)
	}
}

// A lookup marks its rule most recently used before listing it, but other
// lookups can mark theirs after it, so the rule a listing grows can be the
// least recently used when its growth is charged. The rules used since are
// evicted for it, rather than the cache stopping at the rule it is charging
// while still over its budget.
func TestTimezoneOnsetsChargeAGrowingRuleThatOthersWereUsedSince(t *testing.T) {
	const budget = 256
	onsets := newTimezoneOnsets(budget)
	before := time.Date(2026, 6, 1, 0, 0, 0, 0, time.UTC)
	rule := exchangeStandardRule

	growing := onsets.rule(observanceRuleKey{seconds: rule.dtstart.Unix(), rrule: rule.rrule})
	onsets.rule(observanceRuleKey{seconds: smallObservanceRule.dtstart.Unix(), rrule: smallObservanceRule.rrule})
	if rules := rulesByRecency(onsets); len(rules) != 2 || rules[1] != growing {
		t.Fatal("the growing rule is not the least recently used")
	}
	growing.latest(onsets, rule.dtstart, before, nil)
	requireCacheWithinBudget(t, onsets)
	if rules := rulesByRecency(onsets); len(rules) == 0 || rules[0] != growing {
		t.Fatal("the rule just listed is not the most recently used")
	}
}

// A rule that can select no onset of its own has its DTSTART as its only onset
// from the moment it is read, before any listing is asked for, and the cache
// counts that onset like any other.
func TestTimezoneOnsetsCountTheOnsetARuleStartsWith(t *testing.T) {
	onsets := newTimezoneOnsets(maxHeldOnsets)
	dtstart := time.Date(1996, 3, 29, 2, 0, 0, 0, time.UTC)
	const selectsNothing = "FREQ=YEARLY;BYMONTH=3;BYMONTHDAY=29;BYSETPOS=5"
	before := time.Date(2026, 6, 1, 0, 0, 0, 0, time.UTC)
	got, found, listed := onsets.LatestOnOrBefore(dtstart, before, selectsNothing, nil)
	if !found || !listed || !got.Equal(dtstart) {
		t.Fatalf("latest = %v, %t, %t, want the DTSTART alone, listed", got, found, listed)
	}
	if cached := onsets.rules.Len(); cached != 1 {
		t.Fatalf("cached %d rules, want the one asked about", cached)
	}
	requireCacheWithinBudget(t, onsets)
}

// A rule denser than yearly, written from Exchange's 1601 DTSTART, holds more
// onsets before the present than one rule may list, so its listing from the
// DTSTART stops centuries short of the instants asked.
//
// Without COUNT, the rule is answered by searching back from each instant, in
// a few microseconds, so that listing is dropped and the rule held at the cost
// of its text. A counted rule can only be followed forward from its DTSTART,
// so walking it costs centuries of it per instant: its instants are answered
// from a window listed from shortly before the first, which goes on to answer
// those after it.
func TestTimezoneOnsetsAnswerADenseRulesInstantsWithoutListingItFromItsDTSTART(t *testing.T) {
	dtstart := time.Date(1601, 1, 1, 2, 0, 0, 0, time.UTC)
	start := time.Date(1900, 1, 1, 0, 0, 0, 0, time.UTC)
	resolve := func(t *testing.T, onsets *timezoneOnsets, rrule string) {
		t.Helper()
		for i := range 1000 {
			before := start.AddDate(0, 6*i, 0).Add(time.Duration(i) * 7 * time.Hour)
			want, wantFound, wantComplete := ical.LatestRecurrenceOnOrBefore(dtstart, before, rrule)
			got, found, complete := readOnset(onsets, dtstart, before, rrule)
			if !got.Equal(want) || found != wantFound || complete != wantComplete {
				t.Fatalf("latest(%v) = %v, %t, %t, want %v, %t, %t", before, got, found, complete, want, wantFound, wantComplete)
			}
		}
		requireCacheWithinBudget(t, onsets)
	}

	t.Run("without COUNT", func(t *testing.T) {
		const monthly = "FREQ=MONTHLY;BYDAY=1SU"
		onsets := newTimezoneOnsets(maxHeldOnsets)
		resolve(t, onsets, monthly)
		rule, ok := onsets.rules.Get(observanceRuleKey{seconds: dtstart.Unix(), rrule: monthly})
		if !ok || !rule.walked || rule.listed() != 0 {
			t.Errorf("cached %+v, want the rule held without a listing", rule)
		}
		if relists := onsets.relists.Load(); relists != 0 {
			t.Errorf("listed the rule afresh %d times", relists)
		}
	})
	t.Run("counted", func(t *testing.T) {
		const monthly = "FREQ=MONTHLY;BYDAY=1SU;COUNT=100000"
		onsets := newTimezoneOnsets(maxHeldOnsets)
		resolve(t, onsets, monthly)
		if walks := onsets.walks.Load(); walks > 2 {
			t.Errorf("walked the rule %d times for 1000 lookups, want them answered from listings", walks)
		}
		// Each window reaches maxListedOnsetsPerRule months, some 170 years,
		// past the instant it was listed for.
		if relists := onsets.relists.Load(); relists > 8 {
			t.Errorf("listed the rule afresh %d times over 500 years", relists)
		}
	})
}

// A counted rule so dense that even a window kept from shortly before the
// instant asked cannot reach it within a listing's limit and work keeps no
// window, and is not listed again for every lookup: the instants past its
// listing from the DTSTART are walked, as LatestRecurrenceOnOrBefore walks
// them.
func TestTimezoneOnsetsWalkARuleNoListingCanReach(t *testing.T) {
	onsets := newTimezoneOnsets(maxHeldOnsets)
	dtstart := time.Date(1601, 1, 1, 2, 0, 0, 0, time.UTC)
	const hourly = "FREQ=HOURLY;COUNT=100000000"
	for year := 2000; year < 2005; year++ {
		before := time.Date(year, 6, 1, 0, 0, 0, 0, time.UTC)
		want, wantFound, wantComplete := ical.LatestRecurrenceOnOrBefore(dtstart, before, hourly)
		got, found, complete := readOnset(onsets, dtstart, before, hourly)
		if !got.Equal(want) || found != wantFound || complete != wantComplete {
			t.Fatalf("latest(%v) = %v, %t, %t, want %v, %t, %t", before, got, found, complete, want, wantFound, wantComplete)
		}
	}
	if relists := onsets.relists.Load(); relists > 1 {
		t.Errorf("listed the rule afresh %d times, want at most once before giving up on listing it", relists)
	}
	rule, ok := onsets.rules.Get(observanceRuleKey{seconds: dtstart.Unix(), rrule: hourly})
	if !ok || rule.window != nil || !rule.windowless {
		t.Errorf("cached %+v, want the rule held without a window", rule)
	}
	requireCacheWithinBudget(t, onsets)
}

// A lookup that starves partway through listing a rule leaves the shared cache
// able to answer exactly as an unstarved one does: the rule's next lookup, on
// work of its own, finishes the listing without counting again the starts the
// starved one had listed. The rule is counted and holds several starts a period,
// which the over-count of a period listed twice would shift.
func TestTimezoneOnsetsStarvedPartwayAnswerAsAnUnstarvedCacheDoes(t *testing.T) {
	dtstart := time.Date(2000, 3, 5, 2, 0, 0, 0, time.UTC)
	const rrule = "FREQ=YEARLY;BYMONTH=3,10;BYDAY=1SU,-1SU;COUNT=12"
	before := time.Date(2002, 12, 1, 0, 0, 0, 0, time.UTC)
	want := time.Date(2002, 10, 27, 2, 0, 0, 0, time.UTC)

	for _, units := range []int{0, 3, 6, 9, 12, 20, 40} {
		onsets := newTimezoneOnsets(maxHeldOnsets)
		starved := ical.NewObservanceWork(ical.NewRequestWork(nil, units))
		onsets.LatestOnOrBefore(dtstart, before, rrule, starved)
		for _, work := range []*ical.ObservanceWork{ical.NewObservanceWork(nil), nil} {
			got, found, listed := onsets.LatestOnOrBefore(dtstart, before, rrule, work)
			if !listed || !found || !got.Equal(want) {
				t.Fatalf("after a lookup on %d units, latest = %v, %t, %t; want %v", units, got, found, listed, want)
			}
		}
		requireCacheWithinBudget(t, onsets)
	}
}

// A window listed afresh that the reading cannot afford leaves the window the
// rule already holds in place, rather than discarding a listing that is still
// right for the lookups it answers. Over pools from nothing to enough, some
// starve the new window after the old one has reached its limit, and the rule
// never ends up without a window it held, unless the listing was found
// hopeless.
func TestTimezoneOnsetsKeepTheWindowARelistingCouldNotAfford(t *testing.T) {
	dtstart := time.Date(1601, 1, 1, 2, 0, 0, 0, time.UTC)
	const monthly = "FREQ=MONTHLY;BYDAY=1SU;COUNT=100000"
	key := observanceRuleKey{seconds: dtstart.Unix(), rrule: monthly}
	kept := 0
	for units := 0; units <= 60_000; units += 250 {
		onsets := newTimezoneOnsets(maxHeldOnsets)
		if _, _, listed := onsets.LatestOnOrBefore(dtstart, time.Date(1900, 6, 1, 0, 0, 0, 0, time.UTC), monthly, ical.NewObservanceWork(nil)); !listed {
			t.Fatal("the first lookup was not answered from a window")
		}
		rule, _ := onsets.rules.Get(key)
		window := rule.window
		if window == nil {
			t.Fatal("the rule holds no window to keep")
		}
		poor := ical.NewObservanceWork(ical.NewRequestWork(nil, units))
		onsets.LatestOnOrBefore(dtstart, time.Date(2100, 6, 1, 0, 0, 0, 0, time.UTC), monthly, poor)
		if rule.window == nil && !rule.windowless {
			t.Fatalf("a lookup on %d units left the rule without the window it held", units)
		}
		if rule.window == window && onsets.relists.Load() == 2 {
			kept++
		}
		requireCacheWithinBudget(t, onsets)
	}
	if kept == 0 {
		t.Error("no pool starved a new window with the old one still held; the test no longer reaches the case")
	}
}

// weekNumberedZoneObject is an event at start in a zone whose onsets are
// numbered by ISO week: +01:00 from the Monday of week 1, whose first day
// falls in the year before in 2025 (29 December), back to +00:00 on the
// Friday of week 2.
func weekNumberedZoneObject(start, end string) string {
	return wrapCalendar(
		"VERSION:2.0", "PRODID:-//Client//EN",
		"BEGIN:VTIMEZONE", "TZID:Week Zone",
		"BEGIN:DAYLIGHT", "DTSTART:20200106T020000", "RRULE:FREQ=YEARLY;BYWEEKNO=1;BYDAY=MO;WKST=MO",
		"TZOFFSETFROM:+0000", "TZOFFSETTO:+0100", "END:DAYLIGHT",
		"BEGIN:STANDARD", "DTSTART:20200110T020000", "RRULE:FREQ=YEARLY;BYWEEKNO=2;BYDAY=FR;WKST=MO",
		"TZOFFSETFROM:+0100", "TZOFFSETTO:+0000", "END:STANDARD",
		"END:VTIMEZONE",
		"BEGIN:VEVENT", "UID:week", "DTSTAMP:20250101T000000Z",
		"DTSTART;TZID=Week Zone:"+start, "DTEND;TZID=Week Zone:"+end,
		"END:VEVENT",
	)
}

// A reading that runs out of its request's pool while listing a week-numbered
// zone leaves the shared cache answering every later request as a cold cache
// does, including for an onset that falls in the days of a period's first
// week that lie before the period: the 29 December 2025 onset puts 12:00 on
// 30 December at 11:00Z, whichever pool ran dry before.
func TestTimezoneOnsetsStarvedOnAWeekNumberedZoneAnswerAsAColdCacheDoes(t *testing.T) {
	early := ical.UnfoldLines(weekNumberedZoneObject("20210601T120000", "20210601T130000"))
	target := ical.UnfoldLines(weekNumberedZoneObject("20251230T120000", "20251230T130000"))
	const key, value = "DTSTART;TZID=Week Zone", "20251230T120000"
	want := time.Date(2025, 12, 30, 11, 0, 0, 0, time.UTC)
	if got, _ := ical.NewObjectTimes(target, newTimezoneOnsets(maxHeldOnsets), ical.NewRequestWork(nil, ical.ObjectWorkBudget)).ResolvePropertyTime(key, value); !got.Equal(want) {
		t.Fatalf("cold cache resolves %s:%s to %v, want %v", key, value, got, want)
	}

	starvedAny := false
	for units := 1; units < 2000; units++ {
		onsets := newTimezoneOnsets(maxHeldOnsets)
		starved := ical.NewRequestWork(nil, units)
		ical.NewObjectTimes(early, onsets, starved).ResolvePropertyTime(key, "20210601T120000")
		if !starved.Exhausted() {
			continue
		}
		starvedAny = true
		times := ical.NewObjectTimes(target, onsets, ical.NewRequestWork(nil, ical.ObjectWorkBudget))
		if got, _ := times.ResolvePropertyTime(key, value); !got.Equal(want) {
			t.Fatalf("after a reading starved on %d units, a funded reading resolves %s:%s to %v, want %v", units, key, value, got, want)
		}
		requireCacheWithinBudget(t, onsets)
	}
	if !starvedAny {
		t.Fatal("no pool starved the early reading; the test no longer reaches the case")
	}
}

// The same through a calendar-query REPORT on a pool of its own: an earlier
// request starved on the zone does not make the event miss the hour it
// occupies.
func TestCalendarQueryAfterAStarvedWeekNumberedZoneReadingMatchesAsOnAColdCache(t *testing.T) {
	event := &store.Event{ID: 1, CalendarID: 1, UID: "week", ResourceName: "week", ETag: "e",
		RawICAL: weekNumberedZoneObject("20251230T120000", "20251230T130000")}
	body := queryHead + `<D:getetag/></D:prop><C:filter><C:comp-filter name="VCALENDAR"><C:comp-filter name="VEVENT">` +
		`<C:time-range start="20251230T110000Z" end="20251230T115900Z"/></C:comp-filter></C:comp-filter></C:filter></C:calendar-query>`
	matches := func() int {
		h := hostileRecurrenceServer()
		h.store.Events = &fakeEventRepo{ignoreFilter: true, events: map[string]*store.Event{"1:week": event}}
		rr, _ := reportOver(t, h, body)
		return len(decodeMultistatus(t, rr).Responses)
	}

	useTimezoneOnsets(t, maxHeldOnsets)
	if got := matches(); got != 1 {
		t.Fatalf("on a cold cache the query matches %d resources, want 1", got)
	}
	onsets := useTimezoneOnsets(t, maxHeldOnsets)
	starved := ical.NewRequestWork(nil, 63)
	ical.NewObjectTimes(ical.UnfoldLines(weekNumberedZoneObject("20210601T120000", "20210601T130000")), onsets, starved).
		ResolvePropertyTime("DTSTART;TZID=Week Zone", "20210601T120000")
	if !starved.Exhausted() {
		t.Fatal("the earlier reading was not starved; the test no longer reaches the case")
	}
	if got := matches(); got != 1 {
		t.Errorf("after a starved reading of the zone the query matches %d resources, want 1", got)
	}
}
