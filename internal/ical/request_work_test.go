package ical

import (
	"context"
	"fmt"
	"math/rand/v2"
	"sync"
	"testing"
	"time"
)

// spendAll spends step units at a time from each of works in turn until every
// one has been refused, and returns what each spent.
func spendAll(works []*recurrenceWork, step int) []int {
	spent := make([]int, len(works))
	refused := make([]bool, len(works))
	for open := len(works); open > 0; {
		for i, work := range works {
			if refused[i] {
				continue
			}
			if work.spend(step) {
				spent[i] += step
				continue
			}
			refused[i] = true
			open--
		}
	}
	return spent
}

// Evaluations drawing on one pool spend no more than the pool holds between
// them, however they interleave, and use all of it.
func TestRequestWorkBoundsEvaluationsTogether(t *testing.T) {
	pool := NewRequestWork(nil, 3*requestWorkChunk)
	spent := spendAll([]*recurrenceWork{newRecurrenceWork(pool), newRecurrenceWork(pool)}, 1000)
	if total := spent[0] + spent[1]; total != 3*requestWorkChunk {
		t.Fatalf("evaluations spent %d and %d, want %d between them", spent[0], spent[1], 3*requestWorkChunk)
	}
	if !pool.Exhausted() {
		t.Error("a pool that refused an evaluation is not exhausted")
	}
}

// An evaluation is still capped at recurrenceWorkLimit when the pool holds far
// more, and a refusal for that reason is not the pool's.
func TestRequestWorkKeepsEachEvaluationsOwnCap(t *testing.T) {
	pool := NewRequestWork(nil, 100*recurrenceWorkLimit)
	work := newRecurrenceWork(pool)
	if spent := spendAll([]*recurrenceWork{work}, 1000)[0]; spent != recurrenceWorkLimit {
		t.Fatalf("one evaluation spent %d, want its cap %d", spent, recurrenceWorkLimit)
	}
	if work.poolStarved() {
		t.Error("an evaluation that reached its own cap reports the pool starved it")
	}
	if pool.Exhausted() {
		t.Error("a refusal for the evaluation's own cap exhausted the pool")
	}
}

// A pool drawn down to exactly nothing by work that was all granted has refused
// nobody.
func TestRequestWorkDrawnDownByGrantedWorkIsNotExhausted(t *testing.T) {
	pool := NewRequestWork(nil, requestWorkChunk)
	work := newRecurrenceWork(pool)
	if !work.spend(requestWorkChunk) {
		t.Fatal("the pool's whole content was refused")
	}
	if pool.Exhausted() {
		t.Error("a pool whose content was all granted is exhausted")
	}
	if work.spend(1) || !work.poolStarved() || !pool.Exhausted() {
		t.Error("the unit past the pool's content was granted, or its refusal was not the pool's")
	}
}

// A pool stops granting when the request's context ends, so a walk in progress
// stops at its next chunk rather than running on to its cap.
func TestRequestWorkEndsWithItsContext(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	pool := NewRequestWork(ctx, 100*recurrenceWorkLimit)
	work := newRecurrenceWork(pool)
	if !work.spend(1) {
		t.Fatal("a live pool refused work")
	}
	if pool.Exhausted() {
		t.Fatal("a live pool is exhausted")
	}
	cancel()
	if !pool.Exhausted() {
		t.Error("a pool whose context ended is not exhausted")
	}
	// The unit held from the chunk already drawn is still spent; the walk stops
	// when it next needs more.
	spent := 1
	for work.spend(1000) {
		spent += 1000
	}
	if spent > requestWorkChunk {
		t.Errorf("spent %d after the context ended, want at most the chunk already drawn", spent)
	}
}

// The pool is shared by evaluations on any goroutine: what they spend between
// them is the pool, no more and no less.
func TestRequestWorkIsSafeForConcurrentUse(t *testing.T) {
	const workers = 8
	pool := NewRequestWork(nil, 40*requestWorkChunk)
	var wg sync.WaitGroup
	spent := make([]int, workers)
	for i := range workers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			spent[i] = spendAll([]*recurrenceWork{newRecurrenceWork(pool)}, 100)[0]
		}()
	}
	wg.Wait()
	total := 0
	for _, n := range spent {
		total += n
	}
	if total != 40*requestWorkChunk {
		t.Fatalf("workers spent %d, want the pool's %d", total, 40*requestWorkChunk)
	}
}

// Units an evaluation drew and did not spend go back to its source when it
// ends, so a reading's many small lookups are charged what they spent, not a
// chunk apiece.
func TestEvaluationReleasesWhatItDidNotSpend(t *testing.T) {
	pool := NewRequestWork(nil, 10*requestWorkChunk)
	reading := newRecurrenceWork(pool)
	for range 100 {
		evaluation := reading.evaluation(recurrenceWorkLimit)
		if !evaluation.spend(10) {
			t.Fatal("an evaluation within the reading's budget was refused")
		}
		evaluation.release()
	}
	reading.release()
	if left := pool.remaining.Load(); left != 10*requestWorkChunk-1000 {
		t.Fatalf("pool holds %d, want %d after 1000 units were spent", left, 10*requestWorkChunk-1000)
	}
}

// listedRule is a rule whose listing from a distant DTSTART is long enough to
// take several chunks of work, and listedStart the DTSTART it is listed from.
const (
	listedRule  = "FREQ=YEARLY;BYMONTH=3;BYDAY=2SU"
	listedStart = "16010101T020000"
)

func listedRuleStart(t *testing.T) time.Time {
	t.Helper()
	dtstart, err := ParseDateTime(listedStart)
	if err != nil {
		t.Fatal(err)
	}
	return dtstart
}

// starveRules are rules a listing is starved partway through: one start a
// period, several starts a period, a COUNT that ends them, an UNTIL, and a
// week-numbered rule whose period's first week can begin in the year before
// it, so a period's first start can precede the period itself.
var starveRules = []struct {
	name, rrule string
	dtstart     time.Time
	through     time.Time
}{
	{"yearly", listedRule, time.Date(1601, 1, 1, 2, 0, 0, 0, time.UTC), time.Date(2040, 1, 1, 0, 0, 0, 0, time.UTC)},
	{"counted, 52 starts a period", "FREQ=YEARLY;BYDAY=MO;COUNT=200", time.Date(2000, 1, 3, 9, 0, 0, 0, time.UTC), time.Date(2040, 1, 1, 0, 0, 0, 0, time.UTC)},
	{"counted, four starts a period", "FREQ=YEARLY;BYMONTH=3,10;BYDAY=1SU,-1SU;COUNT=12", time.Date(2000, 3, 5, 2, 0, 0, 0, time.UTC), time.Date(2040, 1, 1, 0, 0, 0, 0, time.UTC)},
	{"until, weekly", "FREQ=WEEKLY;BYDAY=MO,WE,FR;UNTIL=20010601T000000Z", time.Date(2000, 1, 3, 9, 0, 0, 0, time.UTC), time.Date(2040, 1, 1, 0, 0, 0, 0, time.UTC)},
	{"monthly with several starts", "FREQ=MONTHLY;BYDAY=1SU,3SU;COUNT=60", time.Date(2000, 1, 2, 9, 0, 0, 0, time.UTC), time.Date(2040, 1, 1, 0, 0, 0, 0, time.UTC)},
	{"yearly by week number", "FREQ=YEARLY;BYWEEKNO=1;BYDAY=MO;WKST=MO", time.Date(2020, 1, 6, 2, 0, 0, 0, time.UTC), time.Date(2040, 1, 1, 0, 0, 0, 0, time.UTC)},
}

func sameStarts(t *testing.T, label string, got, want *RecurrenceStartLister) {
	t.Helper()
	if len(got.starts) != len(want.starts) {
		var last, wantLast time.Time
		if n := len(got.starts); n > 0 {
			last = got.starts[n-1]
		}
		if n := len(want.starts); n > 0 {
			wantLast = want.starts[n-1]
		}
		t.Fatalf("%s: %d starts ending %v, an uninterrupted listing holds %d ending %v", label, len(got.starts), last, len(want.starts), wantLast)
	}
	for i := range want.starts {
		if !got.starts[i].Equal(want.starts[i]) {
			t.Fatalf("%s: start %d is %v, want %v", label, i, got.starts[i], want.starts[i])
		}
	}
}

// A listing built or extended for a reading is charged to that reading, and one
// the reading cannot afford is not marked unable to grow: a later reading with
// budget of its own lists it, to the same starts an uninterrupted listing
// holds, whether the rule ends by COUNT or UNTIL and however many starts a
// period holds.
func TestListingTheReadingCannotAffordIsNotHeldAgainstTheRule(t *testing.T) {
	poor := NewObservanceWork(NewRequestWork(nil, 0))
	for _, rule := range starveRules {
		t.Run(rule.name, func(t *testing.T) {
			if lister := NewRecurrenceStartLister(rule.dtstart, rule.rrule, poor); lister != nil {
				t.Fatalf("a listing begun on an empty pool = %v, want none", lister)
			}
			rich := NewObservanceWork(nil)
			lister := NewRecurrenceStartLister(rule.dtstart, rule.rrule, rich)
			if lister == nil {
				t.Fatal("a reading with budget could not begin a listing")
			}
			// Enough for part of the listing, not all of it.
			short := NewObservanceWork(NewRequestWork(nil, 12))
			if ok := lister.ListThrough(rule.through, maxListedObservanceOnsets, short); ok || !lister.Starved() {
				t.Fatalf("ListThrough on a nearly empty pool = %v, starved %v; want unfinished, starved", ok, lister.Starved())
			}
			if lister.halted {
				t.Fatal("a listing the reading could not afford is marked unable to grow")
			}
			if ok := lister.ListThrough(rule.through, maxListedObservanceOnsets, rich); !ok || lister.Starved() {
				t.Fatalf("ListThrough on a reading with budget = %v, starved %v; want complete", ok, lister.Starved())
			}
			whole := NewRecurrenceStartLister(rule.dtstart, rule.rrule, nil)
			if !whole.ListThrough(rule.through, maxListedObservanceOnsets, nil) {
				t.Fatal("the uninterrupted listing did not complete")
			}
			sameStarts(t, "grown in steps", lister, whole)
		})
	}
}

// However often, and wherever in a period, a listing is starved, finishing it
// on work of its own gives the starts an uninterrupted listing holds. The pools
// are drawn at random, from a few units to enough for most of the listing.
func TestListingStarvedAnywhereFinishesAsAnUnstarvedOneDoes(t *testing.T) {
	random := rand.New(rand.NewPCG(7, 11))
	for _, rule := range starveRules {
		t.Run(rule.name, func(t *testing.T) {
			whole := NewRecurrenceStartLister(rule.dtstart, rule.rrule, nil)
			whole.ListThrough(rule.through, maxListedObservanceOnsets, nil)
			for trial := range 400 {
				lister := NewRecurrenceStartLister(rule.dtstart, rule.rrule, NewObservanceWork(nil))
				for range 1 + random.IntN(3) {
					pool := NewRequestWork(nil, 1+random.IntN(1+trial*4))
					lister.ListThrough(rule.through, maxListedObservanceOnsets, NewObservanceWork(pool))
					if lister.halted {
						t.Fatalf("trial %d: starving a listing halted it", trial)
					}
				}
				if !lister.ListThrough(rule.through, maxListedObservanceOnsets, NewObservanceWork(nil)) {
					t.Fatalf("trial %d: the listing could not be finished", trial)
				}
				sameStarts(t, fmt.Sprintf("trial %d", trial), lister, whole)
			}
		})
	}
}

// A rule that needs more than one evaluation may spend is halted for good, on
// whoever's budget: it is the rule that is too large, not the reading.
func TestListingOfARuleBeyondOneEvaluationStaysHalted(t *testing.T) {
	dtstart := listedRuleStart(t)
	const dense = "FREQ=DAILY;BYHOUR=0,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23;" +
		"BYMINUTE=0,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33,34,35,36,37,38,39,40,41,42,43,44,45,46,47,48,49,50,51,52,53,54,55,56,57,58,59;" +
		"BYSECOND=0,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33,34,35,36,37,38,39,40,41,42,43,44,45,46,47,48,49,50,51,52,53,54,55,56,57,58,59"
	work := NewObservanceWork(nil)
	lister := NewRecurrenceStartLister(dtstart, dense, work)
	if lister == nil {
		t.Fatal("a reading with budget could not begin a listing")
	}
	if ok := lister.ListThrough(time.Date(2030, 1, 1, 0, 0, 0, 0, time.UTC), maxListedObservanceOnsets, work); ok || lister.Starved() {
		t.Fatalf("ListThrough = %v, starved %v; want unfinished, not starved", ok, lister.Starved())
	}
	if !lister.halted {
		t.Error("a listing that outgrew one evaluation is not halted")
	}
	if ok := lister.ListThrough(time.Date(2030, 1, 1, 0, 0, 0, 0, time.UTC), maxListedObservanceOnsets, NewObservanceWork(nil)); ok {
		t.Error("a halted listing was extended")
	}
}

// What a listing holds is what it has room for: starts dropped by a halt or by
// dropping those before an instant release their array, so a cache counting
// Held counts memory.
func TestListingHoldsNoRoomForStartsItDropped(t *testing.T) {
	dtstart := listedRuleStart(t)
	grown := func() *RecurrenceStartLister {
		lister := &RecurrenceStartLister{dtstart: dtstart}
		for i := range 3000 {
			lister.starts = append(lister.starts, dtstart.AddDate(0, 0, i))
		}
		return lister
	}
	roomFor := func(lister *RecurrenceStartLister) bool {
		return lister.Held() <= len(lister.starts)+len(lister.starts)/4+8
	}

	halted := grown()
	halted.covered = dtstart.AddDate(0, 0, 99)
	halted.halt(halted.covered, true)
	if len(halted.starts) != 100 || !roomFor(halted) {
		t.Errorf("a halted listing lists %d starts and holds room for %d, want 100 and no more", len(halted.starts), halted.Held())
	}

	dropped := grown()
	dropped.keepFrom = dtstart.AddDate(0, 0, 2900)
	dropped.dropBefore(len(dropped.starts))
	if len(dropped.starts) != 100 || !roomFor(dropped) {
		t.Errorf("a listing kept from an instant lists %d starts and holds room for %d, want 100 and no more", len(dropped.starts), dropped.Held())
	}
}

// A request of many cheap evaluations is charged what they spent, not a chunk
// apiece: two thousand expansions of an ordinary series leave nearly all of a
// pool that a chunk each would have emptied five times over.
func TestRequestWorkIsChargedForWorkDoneNotChunksDrawn(t *testing.T) {
	const size = 20_000_000
	pool := NewRequestWork(nil, size)
	raw := "BEGIN:VCALENDAR\r\nBEGIN:VEVENT\r\nUID:x\r\nDTSTART:20260101T090000Z\r\nRRULE:FREQ=WEEKLY;COUNT=10\r\nEND:VEVENT\r\nEND:VCALENDAR\r\n"
	dtstart := time.Date(2026, 1, 1, 9, 0, 0, 0, time.UTC)
	for range 2000 {
		periods, err := RecurringBusyPeriods(raw, dtstart, time.Hour, dtstart, dtstart.AddDate(0, 6, 0), MaxRecurrenceInstances, nil, pool)
		if err != nil || len(periods) != 10 {
			t.Fatalf("expanded %d periods, %v; want the series' 10", len(periods), err)
		}
	}
	if pool.Exhausted() {
		t.Fatal("2000 ordinary expansions exhausted the pool")
	}
	if spent := size - int(pool.remaining.Load()); spent > size/10 {
		t.Errorf("2000 ordinary expansions spent %d units of %d, want a small part of it", spent, size)
	}
}

// A draw that is granted in full has refused nobody, however many times its
// first look at the pool found less than it wanted: a refund landing between
// the look and the exchange makes the look stale, and the retry finds enough.
func TestRequestWorkDrawGrantedInFullIsNeverADenial(t *testing.T) {
	const need = 1000
	pool := NewRequestWork(nil, need)
	stop := make(chan struct{})
	var churning sync.WaitGroup
	churning.Add(1)
	go func() {
		defer churning.Done()
		for {
			select {
			case <-stop:
				return
			default:
			}
			// Hold all of the pool, then give it back in two steps, so the
			// pool is seen at less than the draw wants while it is on its way
			// back.
			if got := pool.draw(need, 0); got > 0 {
				pool.refund(got * 4 / 10)
				pool.refund(got - got*4/10)
			}
		}
	}()
	full := 0
	for range 200_000 {
		pool.denied.Store(false)
		got := pool.draw(need, need)
		if got == need {
			full++
			if pool.denied.Load() {
				close(stop)
				churning.Wait()
				t.Fatalf("a draw granted in full left the pool marked as having refused one (after %d full draws)", full)
			}
		}
		pool.refund(got)
	}
	close(stop)
	churning.Wait()
	if full == 0 {
		t.Fatal("no draw was granted in full; the test saw nothing to check")
	}
}

// RecurrenceSlots, like the other expansions, stops when its pool is spent.
func TestRecurrenceSlotsStopsOnASpentPool(t *testing.T) {
	raw := "BEGIN:VCALENDAR\r\nBEGIN:VEVENT\r\nUID:x\r\nDTSTART:20260101T090000Z\r\nRRULE:FREQ=DAILY;COUNT=500\r\nEND:VEVENT\r\nEND:VCALENDAR\r\n"
	dtstart := time.Date(2026, 1, 1, 9, 0, 0, 0, time.UTC)
	until := dtstart.AddDate(2, 0, 0)
	if slots, err := RecurrenceSlots(raw, "VEVENT", dtstart, time.Hour, dtstart, until, MaxRecurrenceInstances, nil, NewRequestWork(nil, 1_000_000)); err != nil || len(slots) != 500 {
		t.Fatalf("on a pool that holds the walk: %d slots, %v; want the series' 500", len(slots), err)
	}
	spent := NewRequestWork(nil, 10)
	if _, err := RecurrenceSlots(raw, "VEVENT", dtstart, time.Hour, dtstart, until, MaxRecurrenceInstances, nil, spent); err == nil || !spent.Exhausted() {
		t.Errorf("on a pool of ten units: %v, exhausted %v; want the expansion refused for the pool", err, spent.Exhausted())
	}
}

// A walk refused by the reading's own walk limit while the request's pool still
// has work is not a starved one: the reading stops there on any pool, so what
// it answered does not depend on this one. One refused because the pool ran out
// first is.
func TestObservanceWorkIsStarvedOnlyByTheRequestPool(t *testing.T) {
	spendWalks := func(work *ObservanceWork, rules int) (refused bool) {
		for i := range rules {
			key := onsetListingKey{seconds: int64(i), rrule: "FREQ=DAILY"}
			work.useWalk(key, func(evaluation *recurrenceWork) {
				if !evaluation.spend(recurrenceWorkLimit) {
					refused = true
				}
			})
		}
		return refused
	}
	walkRules := observanceWalkLimit/recurrenceWorkLimit + 1

	t.Run("the reading's walk limit", func(t *testing.T) {
		pool := NewRequestWork(nil, 1<<40)
		work := NewObservanceWork(pool)
		if !spendWalks(work, walkRules) {
			t.Fatal("no walk was refused")
		}
		if work.Starved() || pool.Exhausted() {
			t.Errorf("Starved() = %v, pool exhausted %v; want neither", work.Starved(), pool.Exhausted())
		}
	})
	t.Run("one rule's allowance", func(t *testing.T) {
		pool := NewRequestWork(nil, 1<<40)
		work := NewObservanceWork(pool)
		work.useWalk(onsetListingKey{rrule: "FREQ=DAILY"}, func(evaluation *recurrenceWork) {
			evaluation.spend(recurrenceWorkLimit)
			if evaluation.spend(1) {
				t.Error("a walk past its rule's allowance was granted")
			}
		})
		if work.Starved() {
			t.Error("Starved() = true, want false")
		}
	})
	t.Run("the request's pool", func(t *testing.T) {
		pool := NewRequestWork(nil, observanceWalkLimit/2)
		work := NewObservanceWork(pool)
		if !spendWalks(work, walkRules) {
			t.Fatal("no walk was refused")
		}
		if !work.Starved() || !pool.Exhausted() {
			t.Errorf("Starved() = %v, pool exhausted %v; want both", work.Starved(), pool.Exhausted())
		}
	})
}

// Reading an object's recurrence bounds resolves no date of a component that
// does not recur, which would walk the rules of its zone for nothing.
func TestRecurrenceBoundsLeaveTheDatesOfAComponentThatDoesNotRecurUnresolved(t *testing.T) {
	zone := []string{"BEGIN:VTIMEZONE", "TZID:Test/Zone", "BEGIN:STANDARD", "DTSTART:19700101T000000", "TZOFFSETFROM:+0100", "TZOFFSETTO:+0100", "END:STANDARD", "END:VTIMEZONE"}
	object := func(extra ...string) []string {
		lines := append([]string{"BEGIN:VCALENDAR", "VERSION:2.0", "PRODID:-//Test//EN"}, zone...)
		lines = append(lines, "BEGIN:VEVENT", "UID:bounds", "DTSTAMP:20260101T000000Z",
			"DTSTART;TZID=Test/Zone:20260105T090000", "DTEND;TZID=Test/Zone:20260105T100000")
		lines = append(lines, extra...)
		return append(lines, "END:VEVENT", "END:VCALENDAR")
	}

	plain := NewObjectTimes(object(), nil, nil)
	if bounds := ConservativeRecurrenceBoundsOf(plain); bounds != (RecurrenceBounds{}) {
		t.Errorf("bounds of an event that does not recur = %+v, want none", bounds)
	}
	if len(plain.zones) != 0 {
		t.Errorf("the bounds of an event that does not recur read %d zones, want none", len(plain.zones))
	}

	for _, extra := range []string{"RRULE:FREQ=DAILY;COUNT=3", "RDATE;TZID=Test/Zone:20260107T090000", "RECURRENCE-ID;TZID=Test/Zone:20260105T090000"} {
		recurring := NewObjectTimes(object(extra), nil, nil)
		if bounds := ConservativeRecurrenceBoundsOf(recurring); !bounds.Recurring || bounds.Start == nil {
			t.Errorf("%s: bounds = %+v, want a recurring object with a start", extra, bounds)
		}
		if len(recurring.zones) == 0 {
			t.Errorf("%s: the bounds read no zone", extra)
		}
	}
}
