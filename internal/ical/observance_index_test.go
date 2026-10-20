package ical

import (
	"math/rand/v2"
	"strconv"
	"strings"
	"testing"
	"time"
)

func observanceAt(start time.Time, from, to time.Duration, rdates []time.Time, rrules ...string) TimezoneObservance {
	observance := TimezoneObservance{Start: start, OffsetFrom: from, OffsetTo: to, RDates: rdates}
	for _, rule := range rrules {
		observance.RRules = append(observance.RRules, observanceRuleOnWallClock(rule, from))
	}
	return observance
}

func wallAt(year int, month time.Month, day, hour int) time.Time {
	return time.Date(year, month, day, hour, 0, 0, 0, time.UTC)
}

// indexEquivalenceDefinitions are definitions whose lookups the index must
// answer as the scan does: ordinary rule pairs, histories stated by RDATE,
// observances that contradict one another at the same onset, a rule too dense
// to follow, transitions at and before the zero time, and many stated onsets
// in no particular order.
func indexEquivalenceDefinitions() map[string]TimezoneObservances {
	const hour = time.Hour
	// Every second of half of each year: more than one period may generate.
	numbers := func(from, through int) string {
		parts := make([]string, 0, through-from+1)
		for n := from; n <= through; n++ {
			parts = append(parts, strconv.Itoa(n))
		}
		return strings.Join(parts, ",")
	}
	dense := "FREQ=YEARLY;BYMONTH=1,2,3,4,5,6;BYMONTHDAY=" + numbers(1, 31) +
		";BYHOUR=" + numbers(0, 23) + ";BYMINUTE=" + numbers(0, 59) + ";BYSECOND=" + numbers(0, 59)
	definitions := map[string]TimezoneObservances{
		"empty": nil,
		"rule pair": {
			observanceAt(wallAt(2007, 3, 11, 2), -5*hour, -4*hour, nil, "FREQ=YEARLY;BYMONTH=3;BYDAY=2SU"),
			observanceAt(wallAt(2007, 11, 4, 2), -4*hour, -5*hour, nil, "FREQ=YEARLY;BYMONTH=11;BYDAY=1SU"),
		},
		"rules ending by UNTIL beside later rules": {
			observanceAt(wallAt(1967, 10, 29, 2), -4*hour, -5*hour, nil, "FREQ=YEARLY;BYMONTH=10;BYDAY=-1SU;UNTIL=20061029T060000Z"),
			observanceAt(wallAt(1987, 4, 5, 2), -5*hour, -4*hour, nil, "FREQ=YEARLY;BYMONTH=4;BYDAY=1SU;UNTIL=20060402T070000Z"),
			observanceAt(wallAt(2007, 3, 11, 2), -5*hour, -4*hour, nil, "FREQ=YEARLY;BYMONTH=3;BYDAY=2SU"),
			observanceAt(wallAt(2007, 11, 4, 2), -4*hour, -5*hour, nil, "FREQ=YEARLY;BYMONTH=11;BYDAY=1SU"),
		},
		"history by RDATE": {
			observanceAt(wallAt(1883, 11, 18, 12), -5*hour+4*time.Minute, -5*hour, nil),
			observanceAt(wallAt(1918, 3, 31, 2), -5*hour, -4*hour, []time.Time{wallAt(1919, 3, 30, 2), wallAt(1920, 3, 28, 2), wallAt(1974, 1, 6, 2)}),
			observanceAt(wallAt(1918, 10, 27, 2), -4*hour, -5*hour, []time.Time{wallAt(1919, 10, 26, 2), wallAt(1920, 10, 31, 2), wallAt(1974, 10, 27, 2)}),
		},
		"contradicting observances at one onset": {
			observanceAt(wallAt(2000, 1, 1, 0), 1*hour, 2*hour, []time.Time{wallAt(2010, 6, 1, 0)}, "FREQ=YEARLY;BYMONTH=3;BYDAY=-1SU"),
			observanceAt(wallAt(2000, 1, 1, 0), 2*hour, 3*hour, []time.Time{wallAt(2010, 6, 1, 0)}),
			observanceAt(wallAt(2000, 3, 26, 0), 3*hour, 1*hour, nil, "FREQ=YEARLY;BYMONTH=3;BYDAY=-1SU"),
			observanceAt(wallAt(2001, 3, 25, 0), 1*hour, 4*hour, nil),
		},
		"rule onset meeting a later RDATE": {
			observanceAt(wallAt(2001, 3, 25, 1), 0, 1*hour, nil, "FREQ=YEARLY;BYMONTH=3;BYDAY=-1SU"),
			observanceAt(wallAt(1999, 1, 1, 0), 1*hour, 0, []time.Time{wallAt(2003, 3, 30, 1), wallAt(2005, 3, 27, 1)}),
		},
		"rule too dense to follow": {
			observanceAt(wallAt(1900, 1, 1, 0), 0, 1*hour, nil, "FREQ=SECONDLY"),
			observanceAt(wallAt(2020, 1, 1, 0), -4*hour, -5*hour, nil, dense),
			observanceAt(wallAt(1950, 1, 1, 0), 1*hour, 0, []time.Time{wallAt(1960, 1, 1, 0)}),
		},
		"transitions at and before the zero time": {
			observanceAt(time.Time{}, 1*hour, -1*hour, nil),
			observanceAt(wallAt(0, 6, 1, 0), 2*hour, 3*hour, nil),
			observanceAt(wallAt(1990, 1, 1, 0), 1*hour, 2*hour, []time.Time{time.Time{}.Add(-time.Hour)}),
		},
		"rule from before the zero time": {
			observanceAt(wallAt(-1, 12, 31, 23), 1*hour, 2*hour, nil, "FREQ=YEARLY"),
			observanceAt(wallAt(0, 6, 1, 0), 3*hour, 4*hour, nil, "FREQ=YEARLY"),
		},
	}
	random := rand.New(rand.NewPCG(1, 2))
	var scattered TimezoneObservances
	for range 300 {
		start := wallAt(1600+random.IntN(600), time.Month(1+random.IntN(12)), 1+random.IntN(28), random.IntN(24))
		from := time.Duration(random.IntN(5)-2) * hour
		to := time.Duration(random.IntN(5)-2) * hour
		var rdates []time.Time
		for range random.IntN(3) {
			rdates = append(rdates, start.AddDate(random.IntN(200)-100, 0, 0))
		}
		var rules []string
		if random.IntN(20) == 0 {
			rules = append(rules, "FREQ=YEARLY;BYMONTH=10;BYDAY=-1SU")
		}
		scattered = append(scattered, observanceAt(start, from, to, rdates, rules...))
	}
	// A repeated onset with another offset, so ties among stated onsets are
	// decided by the order the observances are listed in.
	scattered = append(scattered, observanceAt(scattered[7].Start, 3*hour, -3*hour, nil))
	definitions["scattered"] = scattered
	return definitions
}

// The index answers every lookup the scan answers, with the same offset,
// whichever onsets the rules are read through.
func TestIndexedTimezoneObservancesAnswerAsTheScanDoes(t *testing.T) {
	random := rand.New(rand.NewPCG(3, 4))
	for name, observances := range indexEquivalenceDefinitions() {
		t.Run(name, func(t *testing.T) {
			var bounds []time.Time
			for _, observance := range observances {
				for _, onset := range append([]time.Time{observance.Start}, observance.RDates...) {
					for _, shift := range []time.Duration{0, wallScaleShift(observance), instantScaleShift(observance)} {
						at := onset.Add(shift)
						bounds = append(bounds, at, at.Add(-time.Nanosecond), at.Add(time.Nanosecond))
					}
				}
			}
			bounds = append(bounds, time.Time{}, time.Time{}.Add(-time.Hour), wallAt(1, 1, 1, 1), wallAt(9999, 12, 31, 23))
			for range 2000 {
				bounds = append(bounds, wallAt(1500, 1, 1, 0).Add(time.Duration(random.Int64N(int64(700*365*24)))*time.Hour+time.Duration(random.IntN(3600))*time.Second))
			}
			for _, onsets := range []struct {
				name             string
				scanned, indexed ObservanceOnsets
			}{
				{name: "walked"},
				{name: "listed", scanned: onsetListings{}, indexed: onsetListings{}},
			} {
				indexed := IndexTimezoneObservances(observances, nil)
				for _, bound := range bounds {
					wantOffset, wantOK := observances.OffsetAtWall(bound, onsets.scanned)
					gotOffset, gotOK := indexed.OffsetAtWall(bound, onsets.indexed)
					if gotOffset != wantOffset || gotOK != wantOK {
						t.Fatalf("%s OffsetAtWall(%v) = %v, %t; the scan answers %v, %t", onsets.name, bound, gotOffset, gotOK, wantOffset, wantOK)
					}
					wantOffset, wantOK = observances.OffsetAtInstant(bound, onsets.scanned)
					gotOffset, gotOK = indexed.OffsetAtInstant(bound, onsets.indexed)
					if gotOffset != wantOffset || gotOK != wantOK {
						t.Fatalf("%s OffsetAtInstant(%v) = %v, %t; the scan answers %v, %t", onsets.name, bound, gotOffset, gotOK, wantOffset, wantOK)
					}
				}
			}
		})
	}
}

// A definition stating its history onset by onset is searched rather than
// read whole for every lookup.
func TestIndexedTimezoneObservancesSearchTheOnsetsTheyState(t *testing.T) {
	var observances TimezoneObservances
	for year := 1000; year < 3000; year++ {
		observances = append(observances, observanceAt(wallAt(year, 3, 1, 2), time.Hour, 2*time.Hour, nil),
			observanceAt(wallAt(year, 10, 1, 3), 2*time.Hour, time.Hour, nil))
	}
	indexed := IndexTimezoneObservances(observances, nil)
	if _, ok := indexed.OffsetAtWall(wallAt(2026, 6, 1, 0), nil); !ok {
		t.Fatal("the definition answers no offset")
	}
	if !indexed.wall.indexed || len(indexed.wall.stated) != len(observances) {
		t.Fatalf("the wall-clock scale lists %d stated onsets, indexed %t; want all %d", len(indexed.wall.stated), indexed.wall.indexed, len(observances))
	}
	allocations := testing.AllocsPerRun(100, func() {
		indexed.OffsetAtWall(wallAt(2026, 6, 1, 0), nil)
	})
	if allocations != 0 {
		t.Errorf("a lookup allocates %v times, want none", allocations)
	}
}

// Every lookup through the definitions one reading of an object holds walks a
// rule no listing answers on the reading's one budget for that rule. A zone
// that moves its clock forward at every second costs a day of candidates for
// each walk, and a series asks for one per instance, so walks on budgets of
// their own would cost a budget per instance. The reading answers lookups as
// each walk on its own budget answers them until the rule's budget is spent,
// and every lookup of that rule after, through any of the object's zones, is
// incomplete without walking again.
func TestAReadingWalksItsObservanceRulesOnOneBudgetEach(t *testing.T) {
	every := "FREQ=DAILY;BYHOUR=" + everyValue(0, 23) + ";BYMINUTE=" + everyValue(0, 59) + ";BYSECOND=" + everyValue(0, 59)
	observances := TimezoneObservances{
		observanceAt(wallAt(1601, 1, 1, 0), 0, 0, nil),
		observanceAt(wallAt(1601, 1, 1, 2), 0, time.Second, nil, every),
	}
	work := NewObservanceWork(nil)
	reading := IndexTimezoneObservances(observances, work)
	answered := 0
	for day := range 1000 {
		wall := time.Date(2024, 1, 5, 9, 30, 0, 0, time.UTC).AddDate(0, 0, day)
		got, ok := reading.OffsetAtWall(wall, nil)
		if !ok {
			continue
		}
		if answered < day {
			t.Fatalf("answered %v after a lookup the spent budget refused", wall)
		}
		if want, wantOK := observances.OffsetAtWall(wall, nil); !wantOK || got != want {
			t.Fatalf("OffsetAtWall(%v) = %v, walking it on its own gives %v, %t", wall, got, want, wantOK)
		}
		answered++
	}
	if answered == 0 || answered >= 1000 {
		t.Fatalf("answered %d of 1000 lookups, want the reading's budget spent partway", answered)
	}
	if spent := work.ruleWalks[ruleKey(observances[1].Start, every)]; spent < recurrenceWorkLimit-dayOfCandidates || spent > recurrenceWorkLimit {
		t.Fatalf("the rule's walks spent %d units, want what one evaluation may spend, %d", spent, recurrenceWorkLimit)
	}
	if _, known := reading.Skips(time.Date(2030, 1, 1, 9, 30, 0, 0, time.UTC), nil); known {
		t.Error("Skips walked the rule on a spent budget")
	}
	if _, ok := reading.Gaps(time.Date(2030, 1, 1, 0, 0, 0, 0, time.UTC), time.Date(2030, 1, 1, 1, 0, 0, 0, time.UTC), nil); ok {
		t.Error("Gaps listed the rule on a spent budget")
	}
	otherZone := IndexTimezoneObservances(observances, work)
	if _, ok := otherZone.OffsetAtWall(time.Date(2024, 1, 5, 9, 30, 0, 0, time.UTC), nil); ok {
		t.Error("another zone of the same reading walked the rule on the spent budget")
	}
}

// dayOfCandidates is the most one walk of the rule above spends past the last
// unit the rule's budget holds: a day of its candidates, 86,400 and the day
// list.
const dayOfCandidates = 2 * 86_401

// A rule that has spent its budget leaves the reading's walks to the others: a
// definition of many observances is followed rule by rule, and one dense rule
// among them ends only its own lookups.
func TestADenseObservanceRuleSpendsOnlyItsOwnWalks(t *testing.T) {
	every := "FREQ=DAILY;BYHOUR=" + everyValue(0, 23) + ";BYMINUTE=" + everyValue(0, 59) + ";BYSECOND=" + everyValue(0, 59)
	observances := TimezoneObservances{
		observanceAt(wallAt(1601, 1, 1, 0), 0, 0, nil),
		observanceAt(wallAt(1601, 1, 1, 2), 0, time.Second, nil, every),
		observanceAt(wallAt(1601, 1, 1, 3), 0, time.Hour, nil, "FREQ=YEARLY;BYMONTH=3;BYDAY=2SU"),
	}
	reading := IndexTimezoneObservances(observances, NewObservanceWork(nil))
	for day := range 100 {
		reading.OffsetAtWall(time.Date(2024, 1, 5, 9, 30, 0, 0, time.UTC).AddDate(0, 0, day), nil)
	}
	if _, known := reading.Skips(time.Date(2030, 1, 1, 9, 30, 0, 0, time.UTC), nil); known {
		t.Error("the dense rule was walked on after its budget was spent")
	}
	other := TimezoneObservances{observances[0], observanceAt(wallAt(1601, 1, 1, 4), 0, time.Hour, nil, "FREQ=YEARLY;BYMONTH=10;BYDAY=-1SU")}
	if _, known := IndexTimezoneObservances(other, reading.work).Skips(time.Date(2030, 1, 1, 9, 30, 0, 0, time.UTC), nil); !known {
		t.Error("a rule that had spent nothing was refused for the dense one's spending")
	}
}
