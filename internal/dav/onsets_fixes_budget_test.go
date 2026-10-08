package dav

import (
	"testing"
	"time"

	"github.com/jw6ventures/calcard/internal/ical"
)

// smallObservanceRule is a rule listed from 1981, a few dozen onsets to the
// present, beside the four centuries Exchange's 1601 DTSTART lists.
var smallObservanceRule = struct {
	dtstart time.Time
	rrule   string
}{time.Date(1981, 3, 29, 2, 0, 0, 0, time.UTC), "FREQ=YEARLY;BYMONTH=3;BYDAY=-1SU"}

// requireCacheWithinBudget checks that the cache holds no more than its budget,
// and counts exactly what its listings hold.
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
		got, found, complete := onsets.LatestOnOrBefore(dtstart, before, rrule)
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
	growing.latest(onsets, rule.dtstart, before)
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
	got, found, complete := onsets.LatestOnOrBefore(dtstart, before, selectsNothing)
	if !found || !complete || !got.Equal(dtstart) {
		t.Fatalf("latest = %v, %t, %t, want the DTSTART alone", got, found, complete)
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
			got, found, complete := onsets.LatestOnOrBefore(dtstart, before, rrule)
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
		got, found, complete := onsets.LatestOnOrBefore(dtstart, before, hourly)
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
